// Project:   dfe-transform-vrl
// File:      tests/e2e/kafka.rs
// Purpose:   Kafka end-to-end tests — real produce/consume through VRL transforms
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka end-to-end tests using real Kafka (docker-local or remote).
//!
//! Drives the WorkBatch governed engine driver
//! ([`pipeline::run_governed_pipeline`]) with a stand-alone [`BatchEngine`]
//! (no byte budget wired -> `run_governed` delegates to the whole-batch
//! `run_workbatch` loop) and a [`CancellationToken`] for shutdown.
//!
//! Run explicitly: `TEST_MODE=docker cargo nextest run -- --ignored`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dfe_transform_vrl::config::hot::HotConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::metrics::TransformMetrics;
use dfe_transform_vrl::pipeline;
use scalo::config::shared::SharedConfig;
use scalo::transport::kafka::{KafkaConfig, KafkaProfile, KafkaTransport};
use scalo::transport::{PayloadFormat, TransportBase, TransportReceiver, TransportSender};
use scalo::worker::engine::BatchProcessingConfig;
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, WorkerPoolConfig};
use tokio_util::sync::CancellationToken;

use super::common::{self, KafkaTestConfig, ensure_kafka_or_skip};

fn consumer_kafka_config(kf: &KafkaTestConfig, topics: &[String], group_id: &str) -> KafkaConfig {
    let mut config = KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: kf.brokers.split(',').map(String::from).collect(),
        group: group_id.to_string(),
        client_id: "dfe-transform-vrl-test-consumer".to_string(),
        topics: topics.to_vec(),
        auto_offset_reset: "earliest".to_string(),
        ..KafkaConfig::devtest()
    };
    apply_sasl(&mut config, kf);
    config
}

fn producer_kafka_config(kf: &KafkaTestConfig, topic: &str) -> KafkaConfig {
    let mut config = KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: kf.brokers.split(',').map(String::from).collect(),
        group: format!("dfe-test-producer-{topic}"),
        client_id: "dfe-transform-vrl-test-producer".to_string(),
        topics: vec![topic.to_string()],
        ..KafkaConfig::devtest()
    };
    apply_sasl(&mut config, kf);
    config
}

fn apply_sasl(config: &mut KafkaConfig, kf: &KafkaTestConfig) {
    config.security_protocol = kf.security_protocol.clone();
    if kf.has_sasl() {
        config.sasl_mechanism = kf.sasl_mechanism.clone();
        config.sasl_username = kf.sasl_user.clone();
        config.sasl_password = kf.sasl_password.clone().map(scalo::SensitiveString::from);
    }
}

fn default_hot_config() -> SharedConfig<HotConfig> {
    SharedConfig::new(HotConfig {
        batch_size: 10,
        batch_timeout_ms: 5000,
        key_field: String::new(),
    })
}

/// A stand-alone batch engine with no byte budget wired -- `run_governed`
/// delegates to the whole-batch `run_workbatch` loop (byte-identical to the
/// pre-governor data path), which is what these broker round-trips exercise.
///
/// Pool built with an EXPLICIT 1-thread config (valid on any core count); the
/// `BatchEngine::new` default derives bounds from `available_parallelism` and
/// panics on a 1-core CI sandbox.
fn default_engine() -> Arc<BatchEngine> {
    let pool = Arc::new(AdaptiveWorkerPool::new(WorkerPoolConfig {
        min_threads: 1,
        max_threads: 1,
        ..Default::default()
    }));
    Arc::new(BatchEngine::with_pool(
        pool,
        BatchProcessingConfig::default(),
    ))
}

#[tokio::test]
#[ignore = "requires live Kafka broker or testcontainers. \
    Run explicitly with `cargo nextest run -- --ignored`. \
    NEVER run by default in CI — that's the silent-internal-broker-touch bug."]
async fn test_produce_consume_json_transform() {
    let env = ensure_kafka_or_skip!("produce-consume-json-transform");
    let kf = env.config();
    let source_topic = common::test_topic("json-src");
    let sink_topic = common::test_topic("json-sink");
    let group = common::test_topic("json-cg");

    let seed_config = producer_kafka_config(&kf, &source_topic);
    let seed_producer = KafkaTransport::new(&seed_config).await.unwrap();

    for i in 0..5 {
        let payload = serde_json::to_vec(&serde_json::json!({
            "seq": i,
            "message": format!("event-{i}"),
            "level": "info"
        }))
        .unwrap();
        seed_producer
            .send(&format!("key-{i}"), Bytes::from(payload))
            .await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = seed_producer.close().await;

    let consumer_config = consumer_kafka_config(&kf, &[source_topic.clone()], &group);
    let consumer = KafkaTransport::new(&consumer_config).await.unwrap();

    let producer_config = producer_kafka_config(&kf, &sink_topic);
    let producer = KafkaTransport::new(&producer_config).await.unwrap();

    let program = Arc::new(
        compile_vrl(
            r#".transformed = true
.level = upcase!(.level)"#,
            None,
        )
        .unwrap()
        .program,
    );

    let hot = default_hot_config();
    let metrics = Arc::new(TransformMetrics::default());
    let ready_flag = Arc::new(AtomicBool::new(false));
    let engine = default_engine();
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn({
        let program = Arc::clone(&program);
        let hot = hot.clone();
        let ready = Arc::clone(&ready_flag);
        let metrics = Arc::clone(&metrics);
        let engine = Arc::clone(&engine);
        let shutdown = shutdown.clone();
        let sink_topic = sink_topic.clone();
        async move {
            pipeline::run_governed_pipeline(
                &engine,
                &consumer,
                &producer,
                program,
                hot,
                PayloadFormat::Json,
                &metrics,
                ready,
                shutdown,
                None,
                sink_topic,
                Arc::new(AtomicBool::new(false)),
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        ready_flag.load(Ordering::Relaxed),
        "pipeline should be ready"
    );

    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok(), "pipeline should shut down cleanly");

    let verify_config =
        consumer_kafka_config(&kf, &[sink_topic.clone()], &common::test_topic("verify-cg"));
    let verifier = KafkaTransport::new(&verify_config).await.unwrap();

    let batch = tokio::time::timeout(Duration::from_secs(5), verifier.recv(10))
        .await
        .unwrap()
        .unwrap();

    assert!(
        !batch.is_empty(),
        "should have received transformed events in sink topic"
    );

    for record in &batch.records {
        let value: serde_json::Value = serde_json::from_slice(&record.payload).unwrap();
        assert_eq!(value["transformed"], true);
        assert_eq!(value["level"], "INFO");
    }

    let _ = verifier.close().await;
}

#[tokio::test]
#[ignore = "requires live Kafka broker or testcontainers. \
    Run explicitly with `cargo nextest run -- --ignored`. \
    NEVER run by default in CI — that's the silent-internal-broker-touch bug."]
async fn test_produce_consume_msgpack_transform() {
    let env = ensure_kafka_or_skip!("produce-consume-msgpack-transform");
    let kf = env.config();
    let source_topic = common::test_topic("mp-src");
    let sink_topic = common::test_topic("mp-sink");
    let group = common::test_topic("mp-cg");

    let seed_config = producer_kafka_config(&kf, &source_topic);
    let seed_producer = KafkaTransport::new(&seed_config).await.unwrap();

    for i in 0..3 {
        let payload = rmp_serde::to_vec(&serde_json::json!({
            "seq": i,
            "data": "msgpack-test"
        }))
        .unwrap();
        seed_producer
            .send(&format!("key-{i}"), Bytes::from(payload))
            .await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = seed_producer.close().await;

    let consumer_config = consumer_kafka_config(&kf, &[source_topic.clone()], &group);
    let consumer = KafkaTransport::new(&consumer_config).await.unwrap();

    let producer_config = producer_kafka_config(&kf, &sink_topic);
    let producer = KafkaTransport::new(&producer_config).await.unwrap();

    let program = Arc::new(compile_vrl(r#".format = "msgpack""#, None).unwrap().program);

    let hot = default_hot_config();
    let metrics = Arc::new(TransformMetrics::default());
    let ready_flag = Arc::new(AtomicBool::new(false));
    let engine = default_engine();
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn({
        let program = Arc::clone(&program);
        let hot = hot.clone();
        let ready = Arc::clone(&ready_flag);
        let metrics = Arc::clone(&metrics);
        let engine = Arc::clone(&engine);
        let shutdown = shutdown.clone();
        let sink_topic = sink_topic.clone();
        async move {
            pipeline::run_governed_pipeline(
                &engine,
                &consumer,
                &producer,
                program,
                hot,
                PayloadFormat::Auto,
                &metrics,
                ready,
                shutdown,
                None,
                sink_topic,
                Arc::new(AtomicBool::new(false)),
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok());

    let verify_config = consumer_kafka_config(
        &kf,
        &[sink_topic.clone()],
        &common::test_topic("mp-verify-cg"),
    );
    let verifier = KafkaTransport::new(&verify_config).await.unwrap();

    let batch = tokio::time::timeout(Duration::from_secs(5), verifier.recv(10))
        .await
        .unwrap()
        .unwrap();

    assert!(!batch.is_empty(), "should have transformed msgpack events");
    let _ = verifier.close().await;
}

#[tokio::test]
#[ignore = "requires live Kafka broker or testcontainers. \
    Run explicitly with `cargo nextest run -- --ignored`. \
    NEVER run by default in CI — that's the silent-internal-broker-touch bug."]
async fn test_vrl_abort_drops_events() {
    let env = ensure_kafka_or_skip!("vrl-abort-drops-events");
    let kf = env.config();
    let source_topic = common::test_topic("abort-src");
    let sink_topic = common::test_topic("abort-sink");
    let group = common::test_topic("abort-cg");

    let seed_config = producer_kafka_config(&kf, &source_topic);
    let seed_producer = KafkaTransport::new(&seed_config).await.unwrap();

    for i in 0..4 {
        let payload = serde_json::to_vec(&serde_json::json!({
            "seq": i,
            "keep": i % 2 == 0
        }))
        .unwrap();
        seed_producer
            .send(&format!("key-{i}"), Bytes::from(payload))
            .await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = seed_producer.close().await;

    let consumer_config = consumer_kafka_config(&kf, &[source_topic.clone()], &group);
    let consumer = KafkaTransport::new(&consumer_config).await.unwrap();

    let producer_config = producer_kafka_config(&kf, &sink_topic);
    let producer = KafkaTransport::new(&producer_config).await.unwrap();

    // `!.keep` does not compile: a path resolves to `any`, and VRL refuses to
    // negate a non-boolean. Comparing against `true` carries the same intent
    // (drop anything not explicitly kept) for any incoming type.
    let program = Arc::new(
        compile_vrl(r#"if .keep != true { abort }"#, None)
            .unwrap()
            .program,
    );

    let hot = default_hot_config();
    let metrics = Arc::new(TransformMetrics::default());
    let ready_flag = Arc::new(AtomicBool::new(false));
    let engine = default_engine();
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn({
        let program = Arc::clone(&program);
        let hot = hot.clone();
        let ready = Arc::clone(&ready_flag);
        let metrics = Arc::clone(&metrics);
        let engine = Arc::clone(&engine);
        let shutdown = shutdown.clone();
        let sink_topic = sink_topic.clone();
        async move {
            pipeline::run_governed_pipeline(
                &engine,
                &consumer,
                &producer,
                program,
                hot,
                PayloadFormat::Json,
                &metrics,
                ready,
                shutdown,
                None,
                sink_topic,
                Arc::new(AtomicBool::new(false)),
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let verify_config = consumer_kafka_config(
        &kf,
        &[sink_topic.clone()],
        &common::test_topic("abort-verify-cg"),
    );
    let verifier = KafkaTransport::new(&verify_config).await.unwrap();

    let batch = tokio::time::timeout(Duration::from_secs(5), verifier.recv(10))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        batch.records.len(),
        2,
        "only keep=true events should pass through"
    );

    for record in &batch.records {
        let value: serde_json::Value = serde_json::from_slice(&record.payload).unwrap();
        assert_eq!(value["keep"], true);
    }

    let _ = verifier.close().await;
}
