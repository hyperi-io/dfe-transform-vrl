// Project:   dfe-transform-vrl
// File:      tests/integration_kafka.rs
// Purpose:   Kafka integration tests — real produce/consume through VRL transforms
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka integration tests using real Kafka (docker-local or remote).
//!
//! These tests require a running Kafka broker. They use `skip_if_no_kafka!()`
//! to skip cleanly when Kafka is unavailable.
//!
//! Run explicitly: `TEST_MODE=docker cargo nextest run --test integration_kafka`

mod common;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use dfe_transform_vrl::config::hot::HotConfig;
    use dfe_transform_vrl::engine::compiler::compile_vrl;
    use dfe_transform_vrl::metrics::TransformMetrics;
    use dfe_transform_vrl::pipeline;
    use hyperi_rustlib::config::shared::SharedConfig;
    use hyperi_rustlib::memory::{MemoryGuard, MemoryGuardConfig};
    use hyperi_rustlib::transport::kafka::{KafkaConfig, KafkaProfile, KafkaTransport};
    use hyperi_rustlib::transport::{PayloadFormat, Transport};

    use crate::common::{self, KafkaTestConfig, skip_if_no_kafka};

    /// Build a `KafkaConfig` from test config for consumer role.
    fn consumer_kafka_config(
        kf: &KafkaTestConfig,
        topics: &[String],
        group_id: &str,
    ) -> KafkaConfig {
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

    /// Build a `KafkaConfig` from test config for producer role.
    ///
    /// KafkaTransport creates both consumer + producer internally, so we
    /// need a valid group_id even for producer-only use.
    fn producer_kafka_config(kf: &KafkaTestConfig, topic: &str) -> KafkaConfig {
        let mut config = KafkaConfig {
            profile: KafkaProfile::DevTest,
            brokers: kf.brokers.split(',').map(String::from).collect(),
            group: format!("dfe-test-producer-{}", topic),
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
            config.sasl_password = kf.sasl_password.clone();
        }
    }

    // =========================================================================
    // Produce JSON → consume → VRL transform → verify output
    // =========================================================================

    #[tokio::test]
    async fn test_produce_consume_json_transform() {
        skip_if_no_kafka!();

        let kf = common::kafka_test_config();
        let source_topic = common::test_topic("json-src");
        let sink_topic = common::test_topic("json-sink");
        let group = common::test_topic("json-cg");

        // Create producer to seed source topic
        let seed_config = producer_kafka_config(&kf, &source_topic);
        let seed_producer = KafkaTransport::new(&seed_config).await.unwrap();

        // Produce 5 JSON events
        for i in 0..5 {
            let payload = serde_json::to_vec(&serde_json::json!({
                "seq": i,
                "message": format!("event-{i}"),
                "level": "info"
            }))
            .unwrap();
            seed_producer.send(&format!("key-{i}"), &payload).await;
        }
        // Allow Kafka to flush
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = seed_producer.close().await;

        // Create consumer and output producer via KafkaTransport
        let consumer_config = consumer_kafka_config(&kf, &[source_topic.clone()], &group);
        let consumer = KafkaTransport::new(&consumer_config).await.unwrap();

        let producer_config = producer_kafka_config(&kf, &sink_topic);
        let producer = KafkaTransport::new(&producer_config).await.unwrap();

        // Compile a simple VRL transform
        let program = Arc::new(
            compile_vrl(
                r#".transformed = true
.level = upcase!(.level)"#,
                None,
            )
            .unwrap()
            .program,
        );

        let hot = SharedConfig::new(HotConfig {
            batch_size: 10,
            batch_timeout_ms: 5000,
            key_field: String::new(),
            scaling_pressure_threshold: 0.8,
        });

        let metrics = TransformMetrics::default();
        let ready_flag = Arc::new(AtomicBool::new(false));
        let memory_guard = Arc::new(MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 100_000_000,
            ..Default::default()
        }));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Run pipeline briefly then shut down
        let pipeline_consumer = consumer;
        let pipeline_producer = producer;
        let handle = tokio::spawn({
            let program = Arc::clone(&program);
            let hot = hot.clone();
            let ready = Arc::clone(&ready_flag);
            let guard = Arc::clone(&memory_guard);
            async move {
                pipeline::run_with_transport(
                    &pipeline_consumer,
                    &pipeline_producer,
                    program,
                    hot,
                    PayloadFormat::Json,
                    &metrics,
                    ready,
                    guard,
                    shutdown_rx,
                )
                .await
            }
        });

        // Wait for pipeline to become ready and process
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            ready_flag.load(Ordering::Relaxed),
            "pipeline should be ready"
        );

        // Signal shutdown
        let _ = shutdown_tx.send(true);
        let result = tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok(), "pipeline should shut down cleanly");

        // Consume from sink topic to verify transformed events
        let verify_config =
            consumer_kafka_config(&kf, &[sink_topic.clone()], &common::test_topic("verify-cg"));
        let verifier = KafkaTransport::new(&verify_config).await.unwrap();

        let messages = tokio::time::timeout(Duration::from_secs(5), verifier.recv(10))
            .await
            .unwrap()
            .unwrap();

        assert!(
            !messages.is_empty(),
            "should have received transformed events in sink topic"
        );

        for msg in &messages {
            let value: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
            assert_eq!(value["transformed"], true);
            assert_eq!(value["level"], "INFO");
        }

        let _ = verifier.close().await;
    }

    // =========================================================================
    // Produce msgpack → consume → VRL transform → verify output
    // =========================================================================

    #[tokio::test]
    async fn test_produce_consume_msgpack_transform() {
        skip_if_no_kafka!();

        let kf = common::kafka_test_config();
        let source_topic = common::test_topic("mp-src");
        let sink_topic = common::test_topic("mp-sink");
        let group = common::test_topic("mp-cg");

        // Produce msgpack events
        let seed_config = producer_kafka_config(&kf, &source_topic);
        let seed_producer = KafkaTransport::new(&seed_config).await.unwrap();

        for i in 0..3 {
            let payload = rmp_serde::to_vec(&serde_json::json!({
                "seq": i,
                "data": "msgpack-test"
            }))
            .unwrap();
            seed_producer.send(&format!("key-{i}"), &payload).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = seed_producer.close().await;

        // Run pipeline
        let consumer_config = consumer_kafka_config(&kf, &[source_topic.clone()], &group);
        let consumer = KafkaTransport::new(&consumer_config).await.unwrap();

        let producer_config = producer_kafka_config(&kf, &sink_topic);
        let producer = KafkaTransport::new(&producer_config).await.unwrap();

        let program = Arc::new(compile_vrl(r#".format = "msgpack""#, None).unwrap().program);

        let hot = SharedConfig::new(HotConfig {
            batch_size: 10,
            batch_timeout_ms: 5000,
            key_field: String::new(),
            scaling_pressure_threshold: 0.8,
        });

        let metrics = TransformMetrics::default();
        let ready_flag = Arc::new(AtomicBool::new(false));
        let memory_guard = Arc::new(MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 100_000_000,
            ..Default::default()
        }));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn({
            let program = Arc::clone(&program);
            let hot = hot.clone();
            let ready = Arc::clone(&ready_flag);
            let guard = Arc::clone(&memory_guard);
            async move {
                pipeline::run_with_transport(
                    &consumer,
                    &producer,
                    program,
                    hot,
                    PayloadFormat::Auto,
                    &metrics,
                    ready,
                    guard,
                    shutdown_rx,
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = shutdown_tx.send(true);
        let result = tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok());

        // Verify — output should be auto-detected format (msgpack in, msgpack out)
        let verify_config = consumer_kafka_config(
            &kf,
            &[sink_topic.clone()],
            &common::test_topic("mp-verify-cg"),
        );
        let verifier = KafkaTransport::new(&verify_config).await.unwrap();

        let messages = tokio::time::timeout(Duration::from_secs(5), verifier.recv(10))
            .await
            .unwrap()
            .unwrap();

        assert!(
            !messages.is_empty(),
            "should have transformed msgpack events"
        );
        let _ = verifier.close().await;
    }

    // =========================================================================
    // VRL abort drops events (not forwarded to sink)
    // =========================================================================

    #[tokio::test]
    async fn test_vrl_abort_drops_events() {
        skip_if_no_kafka!();

        let kf = common::kafka_test_config();
        let source_topic = common::test_topic("abort-src");
        let sink_topic = common::test_topic("abort-sink");
        let group = common::test_topic("abort-cg");

        // Produce events: seq 0-4, VRL will abort odd ones
        let seed_config = producer_kafka_config(&kf, &source_topic);
        let seed_producer = KafkaTransport::new(&seed_config).await.unwrap();

        for i in 0..4 {
            let payload = serde_json::to_vec(&serde_json::json!({
                "seq": i,
                "keep": i % 2 == 0
            }))
            .unwrap();
            seed_producer.send(&format!("key-{i}"), &payload).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = seed_producer.close().await;

        let consumer_config = consumer_kafka_config(&kf, &[source_topic.clone()], &group);
        let consumer = KafkaTransport::new(&consumer_config).await.unwrap();

        let producer_config = producer_kafka_config(&kf, &sink_topic);
        let producer = KafkaTransport::new(&producer_config).await.unwrap();

        // VRL: abort events where .keep is false
        let program = Arc::new(compile_vrl(r#"if !.keep { abort }"#, None).unwrap().program);

        let hot = SharedConfig::new(HotConfig {
            batch_size: 10,
            batch_timeout_ms: 5000,
            key_field: String::new(),
            scaling_pressure_threshold: 0.8,
        });

        let metrics = TransformMetrics::default();
        let ready_flag = Arc::new(AtomicBool::new(false));
        let memory_guard = Arc::new(MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 100_000_000,
            ..Default::default()
        }));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn({
            let program = Arc::clone(&program);
            let hot = hot.clone();
            let ready = Arc::clone(&ready_flag);
            let guard = Arc::clone(&memory_guard);
            async move {
                pipeline::run_with_transport(
                    &consumer,
                    &producer,
                    program,
                    hot,
                    PayloadFormat::Json,
                    &metrics,
                    ready,
                    guard,
                    shutdown_rx,
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

        // Only even-numbered events (keep=true) should make it to sink
        let verify_config = consumer_kafka_config(
            &kf,
            &[sink_topic.clone()],
            &common::test_topic("abort-verify-cg"),
        );
        let verifier = KafkaTransport::new(&verify_config).await.unwrap();

        let messages = tokio::time::timeout(Duration::from_secs(5), verifier.recv(10))
            .await
            .unwrap()
            .unwrap();

        // Should have 2 events (seq 0 and seq 2, where keep=true)
        assert_eq!(
            messages.len(),
            2,
            "only keep=true events should pass through"
        );

        for msg in &messages {
            let value: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
            assert_eq!(value["keep"], true);
        }

        let _ = verifier.close().await;
    }
}
