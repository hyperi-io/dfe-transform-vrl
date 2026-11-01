// Project:   dfe-transform-vrl
// File:      tests/e2e/kafka_tls.rs
// Purpose:   The source and sink Kafka clients over SASL_SSL, trusting the CA SSL_CERT_FILE names
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The consumer and producer this service builds from its own `source` and
//! `sink` config, against a real broker with a private CA and SCRAM-SHA-512.
//!
//! The config names no CA file, as the chart deploys it: the CA arrives in the
//! bundle `SSL_CERT_FILE` names. A bundle holding another CA must be refused.
//! The broker is one this test holds alone, from scalo's `TlsBroker`.

use std::time::{Duration, Instant};

use bytes::Bytes;
use dfe_transform_vrl::config::{SaslConfig, SinkConfig, SourceConfig, TlsConfig};
use dfe_transform_vrl::kafka::{
    build_consumer_config, build_producer_config, create_consumer, create_producer,
};
use scalo::transport::kafka::KafkaAdmin;
use scalo::transport::kafka::test_support::{
    SCRAM_USER, TlsBroker, require_docker, scram_password, skip_or_fail,
};
use scalo::transport::{TransportBase, TransportReceiver, TransportSender};

use crate::common::SUITE;

/// Group join plus the first fetch, on a single-node broker busy with a JVM start.
const ROUND_TRIP_WITHIN: Duration = Duration::from_secs(90);

/// The endpoints as a deployment's config writes them, with no CA file named.
fn endpoints(broker: &TlsBroker, topic: &str) -> (SourceConfig, SinkConfig) {
    let sasl = SaslConfig {
        enabled: true,
        mechanism: "scram_sha_512".into(),
        username: SCRAM_USER.into(),
        password: scram_password().into(),
    };
    let tls = TlsConfig {
        enabled: true,
        ..TlsConfig::default()
    };
    let source = SourceConfig {
        brokers: vec![broker.sasl_ssl.clone()],
        topics: vec![topic.to_string()],
        group_id: format!("group-{topic}"),
        auto_offset_reset: "earliest".into(),
        sasl: sasl.clone(),
        tls: tls.clone(),
        ..SourceConfig::default()
    };
    let sink = SinkConfig {
        brokers: vec![broker.sasl_ssl.clone()],
        topic: topic.to_string(),
        message_timeout_ms: 8_000,
        sasl,
        tls,
        ..SinkConfig::default()
    };
    (source, sink)
}

/// Create `topic`, send one record through the sink and read it back through
/// the source.
async fn round_trip(broker: &TlsBroker, topic: &str) -> Result<(), String> {
    let (source, sink) = endpoints(broker, topic);
    let producer_config = build_producer_config(&sink, "tls");
    KafkaAdmin::new(&producer_config)
        .map_err(|e| format!("admin client: {e}"))?
        .create_topics(&[(topic, 1, 1)])
        .await
        .map_err(|e| format!("create topic: {e}"))?;

    let producer = create_producer(&producer_config)
        .await
        .map_err(|e| format!("producer: {e}"))?;
    let payload = format!("{{\"topic\":\"{topic}\"}}");
    let sent = producer.send(topic, Bytes::from(payload.clone())).await;
    if !sent.is_ok() {
        return Err(format!("send was not confirmed: {sent:?}"));
    }

    let consumer = create_consumer(&build_consumer_config(&source))
        .await
        .map_err(|e| format!("consumer: {e}"))?;
    let deadline = Instant::now() + ROUND_TRIP_WITHIN;
    while Instant::now() < deadline {
        let batch = consumer.recv(10).await.map_err(|e| format!("recv: {e}"))?;
        if batch
            .records
            .iter()
            .any(|record| record.payload.as_ref() == payload.as_bytes())
        {
            let _ = consumer.close().await;
            let _ = producer.close().await;
            return Ok(());
        }
    }
    Err(format!(
        "record never came back within {ROUND_TRIP_WITHIN:?}"
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_kafka_clients_trust_the_ca_ssl_cert_file_names() {
    if !require_docker() {
        return;
    }
    let broker = match TlsBroker::start(&SUITE, "kafka-tls-ssl-cert-file").await {
        Ok(broker) => broker,
        Err(e) => {
            skip_or_fail(&format!("the TLS broker would not start ({e})"));
            return;
        }
    };

    let mut failures = Vec::new();
    for (bundle, trusted) in [("ca.crt", true), ("rogue-ca.crt", false)] {
        let path = broker.pki.file(bundle);
        let topic = format!("vrl-tls-{}", bundle.replace('.', "-"));
        let outcome = temp_env::async_with_vars(
            [
                ("SSL_CERT_FILE", Some(path.as_str())),
                ("SSL_CERT_DIR", None),
            ],
            round_trip(&broker, &topic),
        )
        .await;
        match (trusted, outcome) {
            (true, Ok(())) | (false, Err(_)) => {}
            (true, Err(why)) => failures.push(format!("SSL_CERT_FILE={bundle}: {why}")),
            (false, Ok(())) => failures.push(format!(
                "SSL_CERT_FILE={bundle}: a bundle holding another CA was trusted"
            )),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
