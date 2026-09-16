// Project:   dfe-transform-vrl
// File:      tests/e2e/cli_service.rs
// Purpose:   End-to-end test of cli::run_transform_service with live Kafka
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Drives the full `pipeline::run` path with a real Kafka broker.
//! Live broker via `.env` (KAFKA_BROKERS) is preferred; falls back to
//! testcontainers Apache Kafka if no live broker is reachable.
//!
//! This test is the only thing that exercises `pipeline::run` (which
//! constructs `KafkaTransport` directly and drives the governed engine), so it
//! accounts for a significant chunk of pipeline.rs coverage. The governor is
//! `None` here (self-regulation off) so the path is the whole-batch loop.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::Bytes;
use dfe_transform_vrl::config::Config;
use dfe_transform_vrl::config::hot::HotConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::metrics::TransformMetrics;
use dfe_transform_vrl::pipeline;
use scalo::config::shared::SharedConfig;
use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::transport::kafka::{KafkaConfig, KafkaProfile, KafkaTransport};
use scalo::transport::{TransportBase, TransportSender};
use scalo::worker::engine::BatchProcessingConfig;
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, WorkerPoolConfig};
use tokio_util::sync::CancellationToken;

use super::common::{self, ensure_kafka_or_skip};

/// Build a Config that points at the test's Kafka broker, exercising
/// pipeline::run() end-to-end.
fn build_test_config(
    kf: &common::KafkaTestConfig,
    source_topic: &str,
    sink_topic: &str,
    group: &str,
) -> Config {
    let sasl = dfe_transform_vrl::config::SaslConfig {
        enabled: kf.has_sasl(),
        mechanism: kf.sasl_mechanism.clone().unwrap_or_default(),
        username: kf.sasl_user.clone().unwrap_or_default(),
        password: kf
            .sasl_password
            .clone()
            .map(scalo::SensitiveString::from)
            .unwrap_or_default(),
    };
    let tls = dfe_transform_vrl::config::TlsConfig {
        enabled: kf.security_protocol.contains("SSL"),
        skip_verify: true,
        ..Default::default()
    };

    let mut config = Config::default();
    config.pipeline.name = "cli-e2e-test".to_string();
    config.pipeline.batch_size = 5;
    config.pipeline.batch_timeout_ms = 1000;

    config.source.brokers = kf.brokers.split(',').map(String::from).collect();
    config.source.group_id = group.to_string();
    config.source.topics = vec![source_topic.to_string()];
    config.source.format = "json".to_string();
    config.source.auto_offset_reset = "earliest".to_string();
    config.source.sasl = sasl.clone();
    config.source.tls = tls.clone();

    config.sink.brokers = kf.brokers.split(',').map(String::from).collect();
    config.sink.topic = sink_topic.to_string();
    config.sink.key_field = ".id".to_string();
    config.sink.compression = "none".to_string();
    config.sink.sasl = sasl;
    config.sink.tls = tls;

    config
}

/// Build a producer KafkaConfig matching the test broker, used to seed events.
fn seed_producer_config(kf: &common::KafkaTestConfig, topic: &str) -> KafkaConfig {
    let mut config = KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: kf.brokers.split(',').map(String::from).collect(),
        group: format!("dfe-cli-seed-{topic}"),
        client_id: "dfe-cli-e2e-seed".to_string(),
        topics: vec![topic.to_string()],
        ..KafkaConfig::devtest()
    };
    config.security_protocol = kf.security_protocol.clone();
    if kf.has_sasl() {
        config.sasl_mechanism = kf.sasl_mechanism.clone();
        config.sasl_username = kf.sasl_user.clone();
        config.sasl_password = kf.sasl_password.clone().map(scalo::SensitiveString::from);
    }
    config
}

#[tokio::test]
#[ignore = "requires live Kafka broker or testcontainers. \
    Run explicitly with `cargo nextest run -- --ignored`. \
    NEVER run by default in CI — that's the silent-internal-broker-touch bug."]
async fn test_pipeline_run_end_to_end_with_live_kafka() {
    let env = ensure_kafka_or_skip!("pipeline-run-end-to-end");
    let kf = env.config();

    let source_topic = common::test_topic("cli-src");
    let sink_topic = common::test_topic("cli-sink");
    let group = common::test_topic("cli-grp");

    eprintln!(
        "Test: source={source_topic} sink={sink_topic} group={group} brokers={}",
        kf.brokers
    );

    // 1. Seed the source topic with a few events
    let seed_cfg = seed_producer_config(kf, &source_topic);
    let seed = tokio::time::timeout(Duration::from_secs(15), KafkaTransport::new(&seed_cfg))
        .await
        .expect("seed producer create timed out")
        .expect("seed producer create failed");

    for i in 0..3u32 {
        let payload = serde_json::to_vec(&serde_json::json!({
            "id": format!("e2e-{i}"),
            "value": i * 10,
        }))
        .unwrap();
        let _ = tokio::time::timeout(
            Duration::from_secs(10),
            seed.send(&format!("e2e-{i}"), Bytes::from(payload)),
        )
        .await
        .expect("seed send timed out");
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), seed.close()).await;
    eprintln!("Seeded 3 events to {source_topic}");

    // 2. Build the full pipeline config and compile a VRL program
    let config = build_test_config(kf, &source_topic, &sink_topic, &group);
    let program = Arc::new(
        compile_vrl(r#".tag = "cli-e2e-pass""#, None)
            .unwrap()
            .program,
    );

    // 3. Drive pipeline::run() — exercises full Kafka init, the governed engine
    //    driver, metrics. This is the path that's previously been untested
    //    without Kafka. Governor is None (self-regulation off) -> whole-batch
    //    loop; engine carries no byte budget.
    let metrics = Arc::new(TransformMetrics::default());
    let hot_config = SharedConfig::new(HotConfig::from_config(&config));
    let ready_flag = Arc::new(AtomicBool::new(false));
    // Explicit 1-thread pool (valid on any core count); `BatchEngine::new`
    // derives the pool bounds from `available_parallelism` and panics on a
    // 1-core CI sandbox.
    let pool = Arc::new(AdaptiveWorkerPool::new(WorkerPoolConfig {
        min_threads: 1,
        max_threads: 1,
        ..Default::default()
    }));
    let engine = Arc::new(BatchEngine::with_pool(
        pool,
        BatchProcessingConfig::default(),
    ));
    let shutdown = CancellationToken::new();

    let pipeline_config = config.clone();
    let pipeline_metrics = Arc::clone(&metrics);
    let pipeline_shutdown = shutdown.clone();
    let pipeline_handle = tokio::spawn(async move {
        pipeline::run(
            &pipeline_config,
            program,
            hot_config,
            pipeline_metrics,
            ready_flag,
            pipeline_shutdown,
            None,
            engine,
            None,
            None,
            Arc::new(MemoryGuard::new(MemoryGuardConfig::default())),
        )
        .await
    });

    // 4. Let the pipeline drain the seeded events
    tokio::time::sleep(Duration::from_secs(8)).await;

    // 5. Verify by consuming sink topic
    let verify_cfg = seed_producer_config(kf, &sink_topic);
    // Use a consumer config (for recv); reuse the helper but change group
    let verify_kafka = KafkaConfig {
        group: format!("dfe-cli-verify-{sink_topic}"),
        topics: vec![sink_topic.clone()],
        auto_offset_reset: "earliest".to_string(),
        ..verify_cfg
    };
    let verifier =
        tokio::time::timeout(Duration::from_secs(15), KafkaTransport::new(&verify_kafka))
            .await
            .expect("verifier create timed out")
            .expect("verifier create failed");

    use scalo::transport::TransportReceiver;
    let batch = tokio::time::timeout(Duration::from_secs(15), verifier.recv(10))
        .await
        .expect("verifier recv timed out")
        .expect("verifier recv error");

    eprintln!(
        "Verifier received {} records from sink",
        batch.records.len()
    );

    // 6. Shutdown pipeline cleanly
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), pipeline_handle).await;
    let _ = verifier.close().await;

    // We seeded 3 events; some may have been delivered (depends on partition
    // assignment timing). Assert at least one round-tripped to validate
    // that pipeline::run actually wired everything correctly.
    assert!(
        !batch.is_empty(),
        "expected at least one transformed event in {sink_topic}"
    );

    // Verify VRL transform actually ran
    for record in &batch.records {
        let val: serde_json::Value = serde_json::from_slice(&record.payload).unwrap();
        assert_eq!(val["tag"], "cli-e2e-pass", "VRL .tag should be set");
    }
}
