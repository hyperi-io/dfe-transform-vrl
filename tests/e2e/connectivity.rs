// Project:   dfe-transform-vrl
// File:      tests/e2e/connectivity.rs
// Purpose:   Smoke test live Kafka connectivity (creds + SASL handshake)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

//! Quick smoke test that verifies the live Kafka credentials in `.env`
//! actually work. Helpful for catching stale creds before running long
//! pipeline tests.

use std::time::Duration;

use scalo::transport::kafka::{KafkaConfig, KafkaProfile, KafkaTransport};
use scalo::transport::{TransportBase, TransportSender};

use super::common;

#[tokio::test]
#[ignore = "requires live Kafka broker (KAFKA_BROKERS env). \
    Run explicitly with `cargo nextest run -- --ignored`. \
    NEVER run by default in CI — that's the silent-internal-broker-touch bug."]
async fn test_live_kafka_connectivity_smoke() {
    let kf = common::kafka_test_config();

    if !kf.is_reachable() {
        eprintln!("SKIP: live Kafka at {} not reachable (TCP)", kf.brokers);
        return;
    }
    eprintln!("Live Kafka TCP OK at {}", kf.brokers);

    let topic = common::test_topic("conn-smoke");
    let mut config = KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: kf.brokers.split(',').map(String::from).collect(),
        group: format!("dfe-conn-smoke-{topic}"),
        client_id: "dfe-transform-vrl-conn-smoke".to_string(),
        topics: vec![topic.clone()],
        ..KafkaConfig::devtest()
    };
    config.security_protocol = kf.security_protocol.clone();
    if kf.has_sasl() {
        config.sasl_mechanism = kf.sasl_mechanism.clone();
        config.sasl_username = kf.sasl_user.clone();
        config.sasl_password = kf.sasl_password.clone().map(scalo::SensitiveString::from);
    }

    let result = tokio::time::timeout(Duration::from_secs(20), KafkaTransport::new(&config)).await;
    match result {
        Ok(Ok(transport)) => {
            eprintln!("KafkaTransport::new OK");
            // Send with timeout to detect hangs early
            let send_result = tokio::time::timeout(
                Duration::from_secs(15),
                transport.send(&topic, bytes::Bytes::from_static(b"hello")),
            )
            .await;
            match send_result {
                Ok(_) => eprintln!("Send OK"),
                Err(_) => panic!("Send TIMED OUT after 15s"),
            }
            let _ = tokio::time::timeout(Duration::from_secs(5), transport.close()).await;
            eprintln!("Close OK");
        }
        Ok(Err(e)) => {
            panic!("KafkaTransport::new failed (probably bad creds): {e}");
        }
        Err(_) => {
            panic!("KafkaTransport::new TIMED OUT after 20s — likely SASL/TLS handshake hung");
        }
    }
}
