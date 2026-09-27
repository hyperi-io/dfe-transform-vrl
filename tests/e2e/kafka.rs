// Project:   dfe-transform-vrl
// File:      tests/e2e/kafka.rs
// Purpose:   Kafka end-to-end tests — real produce/consume through VRL transforms
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka end-to-end tests using real Kafka (docker-local or remote).
//!
//! Drives the pipeline loop ([`pipeline::run_governed_pipeline`]) with a
//! stand-alone [`BatchEngine`] (no byte budget wired, so each block is sent
//! whole) and a [`CancellationToken`] for shutdown. The source's offsets are
//! committed only once the sink has delivered the block.
//!
//! Run explicitly: `TEST_MODE=docker cargo nextest run -- --ignored`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dfe_transform_vrl::config::hot::HotConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::metrics::TransformMetrics;
use dfe_transform_vrl::pipeline;
use scalo::config::shared::SharedConfig;
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaProfile, KafkaTransport};
use scalo::transport::{TransportBase, TransportReceiver, TransportSender};
use scalo::worker::engine::BatchProcessingConfig;
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, WorkerPoolConfig};
use tokio_util::sync::CancellationToken;

use super::common::{self, KafkaTestConfig, ensure_kafka_or_skip};

/// How long a round trip gets to put every expected record on the sink topic,
/// consumer group join included.
const ROUND_TRIP_TIMEOUT: Duration = Duration::from_secs(60);

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

/// A stand-alone batch engine with no byte budget wired, so the pipeline loop
/// sends each block whole.
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

/// Read `topic` from the start until `want` records have arrived or `timeout`
/// passes.
async fn read_topic(
    kf: &KafkaTestConfig,
    topic: &str,
    want: usize,
    timeout: Duration,
) -> Vec<Bytes> {
    let verify_config =
        consumer_kafka_config(kf, &[topic.to_string()], &common::test_topic("verify-cg"));
    let verifier = KafkaTransport::new(&verify_config).await.unwrap();
    let mut payloads = Vec::new();
    let deadline = Instant::now() + timeout;
    while payloads.len() < want && Instant::now() < deadline {
        if let Ok(Ok(batch)) =
            tokio::time::timeout(Duration::from_secs(5), verifier.recv(want)).await
        {
            payloads.extend(batch.records.into_iter().map(|r| r.payload));
        }
    }
    let _ = verifier.close().await;
    payloads
}

/// Seed `inputs` onto a fresh source topic, run the pipeline over it with the
/// VRL program `vrl`, and return what reached the sink topic once `want`
/// records have, or [`ROUND_TRIP_TIMEOUT`] passes.
async fn round_trip(
    kf: &KafkaTestConfig,
    name: &str,
    inputs: Vec<Vec<u8>>,
    vrl: &str,
    want: usize,
) -> Vec<Bytes> {
    let source_topic = common::test_topic(&format!("{name}-src"));
    let sink_topic = common::test_topic(&format!("{name}-sink"));
    let group = common::test_topic(&format!("{name}-cg"));

    // Both topics exist before anything reads them: a consumer subscribed to a
    // missing topic fails its first poll.
    KafkaAdmin::new(&consumer_kafka_config(
        kf,
        &[source_topic.clone()],
        &format!("{name}-admin"),
    ))
    .unwrap()
    .create_topics(&[(&source_topic, 1, 1), (&sink_topic, 1, 1)])
    .await
    .unwrap();

    let seed_producer = KafkaTransport::new(&producer_kafka_config(kf, &source_topic))
        .await
        .unwrap();
    for payload in inputs {
        // `send`'s first argument is the destination topic, not a key.
        let seeded = seed_producer
            .send(&source_topic, Bytes::from(payload))
            .await;
        assert!(seeded.is_ok(), "seeding {source_topic} failed: {seeded:?}");
    }
    let _ = seed_producer.close().await;

    let consumer = KafkaTransport::new(&consumer_kafka_config(kf, &[source_topic], &group))
        .await
        .unwrap();
    let producer = KafkaTransport::new(&producer_kafka_config(kf, &sink_topic))
        .await
        .unwrap();
    let program = Arc::new(compile_vrl(vrl, None).unwrap().program);
    let ready = Arc::new(AtomicBool::new(false));
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn({
        let ready = Arc::clone(&ready);
        let shutdown = shutdown.clone();
        let sink_topic = sink_topic.clone();
        async move {
            pipeline::run_governed_pipeline(
                &default_engine(),
                &consumer,
                &producer,
                program,
                default_hot_config(),
                &Arc::new(TransformMetrics::default()),
                ready,
                shutdown,
                None,
                sink_topic,
                Arc::new(AtomicBool::new(false)),
            )
            .await
        }
    });

    let received = read_topic(kf, &sink_topic, want, ROUND_TRIP_TIMEOUT).await;

    assert!(
        ready.load(Ordering::Acquire),
        "the pipeline must still be running and ready"
    );
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_ok(),
        "pipeline should shut down cleanly: {result:?}"
    );
    received
}

#[tokio::test]
#[ignore = "requires live Kafka broker or testcontainers. \
    Run explicitly with `cargo nextest run -- --ignored`. \
    NEVER run by default in CI — that's the silent-internal-broker-touch bug."]
async fn test_produce_consume_json_transform() {
    let env = ensure_kafka_or_skip!("produce-consume-json-transform");
    let kf = env.config();

    let inputs = (0..5)
        .map(|i| {
            serde_json::to_vec(&serde_json::json!({
                "seq": i,
                "message": format!("event-{i}"),
                "level": "info"
            }))
            .unwrap()
        })
        .collect();
    let received = round_trip(
        kf,
        "json",
        inputs,
        ".transformed = true\n.level = upcase!(.level)",
        5,
    )
    .await;

    assert_eq!(received.len(), 5, "every seeded event reaches the sink");
    for payload in &received {
        let value: serde_json::Value = serde_json::from_slice(payload).unwrap();
        assert_eq!(value["transformed"], true);
        assert_eq!(value["level"], "INFO");
    }
}

#[tokio::test]
#[ignore = "requires live Kafka broker or testcontainers. \
    Run explicitly with `cargo nextest run -- --ignored`. \
    NEVER run by default in CI — that's the silent-internal-broker-touch bug."]
async fn test_vrl_abort_drops_events() {
    let env = ensure_kafka_or_skip!("vrl-abort-drops-events");
    let kf = env.config();

    let inputs = (0..4)
        .map(|i| {
            serde_json::to_vec(&serde_json::json!({
                "seq": i,
                "keep": i % 2 == 0
            }))
            .unwrap()
        })
        .collect();
    // `!.keep` does not compile: a path resolves to `any`, and VRL refuses to
    // negate a non-boolean. Comparing against `true` carries the same intent
    // (drop anything not explicitly kept) for any incoming type.
    let received = round_trip(kf, "abort", inputs, "if .keep != true { abort }", 2).await;

    assert_eq!(
        received.len(),
        2,
        "only keep=true events should pass through"
    );
    for payload in &received {
        let value: serde_json::Value = serde_json::from_slice(payload).unwrap();
        assert_eq!(value["keep"], true);
    }
}
