// Project:   dfe-transform-vrl
// File:      src/kafka/mod.rs
// Purpose:   Kafka consumer and producer via rustlib transport-kafka
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka transport layer using hyperi-rustlib `transport-kafka`.
//!
//! Wraps rustlib's `KafkaTransport` to provide the consume/produce/commit
//! cycle with format detection via rustlib's `FormatDetector`.

use std::collections::HashMap;

use hyperi_rustlib::transport::PayloadFormat;
use hyperi_rustlib::transport::kafka::{KafkaConfig, KafkaProfile, KafkaTransport};

use crate::config;

/// Build a `KafkaConfig` for the consumer from our source config.
pub fn build_consumer_config(source: &config::SourceConfig) -> KafkaConfig {
    let mut kafka_config = KafkaConfig {
        profile: KafkaProfile::Production,
        brokers: source.brokers.clone(),
        group: source.group_id.clone(),
        client_id: "dfe-transform-vrl-consumer".to_string(),
        topics: source.topics.clone(),
        auto_offset_reset: source.auto_offset_reset.clone(),
        session_timeout_ms: source.session_timeout_ms,
        fetch_max_bytes: i32::try_from(source.max_buffer_bytes).unwrap_or(i32::MAX),
        ..KafkaConfig::production()
    };

    apply_sasl_tls(&mut kafka_config, &source.sasl, &source.tls);

    for (k, v) in &source.librdkafka_options {
        kafka_config
            .librdkafka_overrides
            .insert(k.clone(), v.clone());
    }

    kafka_config
}

/// Build a `KafkaConfig` for the producer from our sink config.
///
/// The producer uses the same `KafkaTransport` but only the `send()` path.
/// We configure it as a separate transport instance with sink brokers/auth.
pub fn build_producer_config(sink: &config::SinkConfig, pipeline_name: &str) -> KafkaConfig {
    let mut kafka_config = KafkaConfig {
        profile: KafkaProfile::Production,
        brokers: sink.brokers.clone(),
        group: String::new(),
        client_id: format!("dfe-transform-vrl-producer-{pipeline_name}"),
        topics: vec![sink.topic.clone()],
        ..KafkaConfig::production()
    };

    apply_sasl_tls(&mut kafka_config, &sink.sasl, &sink.tls);

    let mut overrides = HashMap::new();
    overrides.insert("compression.type".to_string(), sink.compression.clone());
    overrides.insert(
        "message.timeout.ms".to_string(),
        sink.message_timeout_ms.to_string(),
    );
    overrides.insert(
        "queue.buffering.max.kbytes".to_string(),
        (sink.max_buffer_bytes / 1024).to_string(),
    );

    for (k, v) in &sink.librdkafka_options {
        overrides.insert(k.clone(), v.clone());
    }

    kafka_config.librdkafka_overrides = overrides;
    kafka_config
}

/// Create a `KafkaTransport` from config (consumer side).
pub async fn create_consumer(config: &KafkaConfig) -> crate::Result<KafkaTransport> {
    KafkaTransport::new(config)
        .await
        .map_err(|e| crate::Error::Kafka(format!("failed to create consumer: {e}")))
}

/// Create a `KafkaTransport` from config (producer side).
pub async fn create_producer(config: &KafkaConfig) -> crate::Result<KafkaTransport> {
    KafkaTransport::new(config)
        .await
        .map_err(|e| crate::Error::Kafka(format!("failed to create producer: {e}")))
}

/// Map our source format config to rustlib's `PayloadFormat`.
pub fn parse_format(format_str: &str) -> PayloadFormat {
    match format_str {
        "json" => PayloadFormat::Json,
        "msgpack" => PayloadFormat::MsgPack,
        _ => PayloadFormat::Auto,
    }
}

fn apply_sasl_tls(
    kafka_config: &mut KafkaConfig,
    sasl: &config::SaslConfig,
    tls: &config::TlsConfig,
) {
    if sasl.enabled {
        let mechanism = match sasl.mechanism.as_str() {
            "plain" => "PLAIN",
            "scram_sha_256" => "SCRAM-SHA-256",
            "scram_sha_512" => "SCRAM-SHA-512",
            other => other,
        };
        kafka_config.sasl_mechanism = Some(mechanism.to_string());
        kafka_config.sasl_username = Some(sasl.username.clone());
        kafka_config.sasl_password = Some(sasl.password.clone());

        if tls.enabled {
            kafka_config.security_protocol = "sasl_ssl".to_string();
        } else {
            kafka_config.security_protocol = "sasl_plaintext".to_string();
        }
    } else if tls.enabled {
        kafka_config.security_protocol = "ssl".to_string();
    }

    if tls.enabled {
        kafka_config.ssl_ca_location.clone_from(&tls.ca_cert_file);
        kafka_config
            .ssl_certificate_location
            .clone_from(&tls.cert_file);
        kafka_config.ssl_key_location.clone_from(&tls.key_file);
        kafka_config.ssl_skip_verify = tls.skip_verify;
    }
}
