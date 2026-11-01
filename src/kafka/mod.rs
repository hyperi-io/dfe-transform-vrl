// Project:   dfe-transform-vrl
// File:      src/kafka/mod.rs
// Purpose:   Kafka consumer and producer via scalo transport-kafka
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka transport layer using scalo `transport-kafka`.
//!
//! Wraps scalo's `KafkaTransport` to provide the consume/produce/commit
//! cycle.

use std::collections::HashMap;

use scalo::kafka_config::{KafkaSource, ServiceRole};
use scalo::transport::kafka::{KafkaConfig, KafkaProfile, KafkaTransport};

use crate::config;

/// Build a `KafkaConfig` for the consumer from our source config.
pub fn build_consumer_config(source: &config::SourceConfig) -> KafkaConfig {
    // Consumer profile: group + topics set (subscribes). scalo 2.9 dropped the
    // explicit KafkaRole -- a non-empty group + topics is the consumer shape.
    let mut kafka_config = KafkaConfig {
        profile: KafkaProfile::Production,
        brokers: source.brokers.clone(),
        group: source.group_id.clone(),
        client_id: "dfe-transform-vrl-consumer".to_string(),
        topics: source.topics.clone(),
        auto_offset_reset: source.auto_offset_reset.clone(),
        session_timeout_ms: source.session_timeout_ms,
        ..KafkaConfig::production()
    };

    apply_sasl_tls(&mut kafka_config, &source.sasl, &source.tls);

    // Fetch sizes come from scalo's sizing surface, which overwrites the
    // named fetch fields, so the buffer cap is the prefetch queue itself.
    kafka_config.librdkafka_overrides.insert(
        "queued.max.messages.kbytes".to_string(),
        prefetch_queue_kib(source.max_buffer_bytes).to_string(),
    );

    if source.statistics_interval_ms > 0 {
        kafka_config.librdkafka_overrides.insert(
            "statistics.interval.ms".to_string(),
            source.statistics_interval_ms.to_string(),
        );
    }

    for (k, v) in &source.librdkafka_options {
        kafka_config
            .librdkafka_overrides
            .insert(k.clone(), v.clone());
    }

    kafka_config
}

/// `max_buffer_bytes` in KiB, inside librdkafka's `1..=2097151` range for
/// `queued.max.messages.kbytes`.
fn prefetch_queue_kib(max_buffer_bytes: u64) -> u64 {
    (max_buffer_bytes / 1024).clamp(1, 2_097_151)
}

/// Build a `KafkaConfig` for the producer from our sink config.
///
/// The producer uses the same `KafkaTransport` but only the `send()` path.
/// We configure it as a separate transport instance with sink brokers/auth.
pub fn build_producer_config(sink: &config::SinkConfig, pipeline_name: &str) -> KafkaConfig {
    // Producer profile: built through `KafkaTransport::sender`, so no consumer
    // and no subscription. The `topics` list is the produce destination.
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
///
/// # Errors
///
/// Returns an error when the Kafka consumer cannot be created from `config`.
pub async fn create_consumer(config: &KafkaConfig) -> crate::Result<KafkaTransport> {
    KafkaTransport::receiver(config)
        .await
        .map_err(|e| crate::Error::Kafka(format!("failed to create consumer: {e}")))
}

/// Create a `KafkaTransport` from config (producer side).
///
/// # Errors
///
/// Returns an error when the Kafka producer cannot be created from `config`.
pub async fn create_producer(config: &KafkaConfig) -> crate::Result<KafkaTransport> {
    KafkaTransport::sender(config)
        .await
        .map_err(|e| crate::Error::Kafka(format!("failed to create producer: {e}")))
}

/// Derive a `KafkaSource` from the first configured source topic.
///
/// If the topic follows DFE naming convention (`{source}_land`), extracts
/// the source name. Otherwise returns `None`. Derived groups carry the `dfe-`
/// prefix the broker ACL grants and committed offsets are stored under.
pub fn derive_dfe_source(source: &config::SourceConfig) -> Option<KafkaSource> {
    source
        .topics
        .first()
        .and_then(|topic| KafkaSource::source_from_topic(topic))
        .map(|n| KafkaSource::new(n).with_group_prefix("dfe-"))
}

/// Derive a consumer group ID using DFE naming conventions.
///
/// Uses `KafkaSource` if a source can be derived from the topic name,
/// falling back to the explicit `group_id` from config. The explicit
/// config always takes precedence (it may be set by dfe-engine).
pub fn derive_consumer_group(source: &config::SourceConfig, pipeline_name: &str) -> String {
    // Explicit config wins — dfe-engine sets this via env var
    if !source.group_id.is_empty() {
        return source.group_id.clone();
    }

    // Try KafkaSource convention
    if let Some(dfe_source) = derive_dfe_source(source)
        && let Ok(cg) = dfe_source.consumer_group(
            "transform-vrl",
            ServiceRole::Transform,
            Some(pipeline_name),
            None,
        )
    {
        return cg;
    }

    // Ultimate fallback
    format!("dfe-transform-vrl-{pipeline_name}")
}

fn apply_sasl_tls(
    kafka_config: &mut KafkaConfig,
    sasl: &config::SaslConfig,
    tls: &config::TlsConfig,
) {
    if sasl.enabled {
        let mechanism = config::SaslMechanism::parse(&sasl.mechanism)
            .map_or(sasl.mechanism.as_str(), |known| known.as_librdkafka());
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::{SaslConfig, SinkConfig, SourceConfig, TlsConfig};

    fn default_source() -> SourceConfig {
        SourceConfig {
            brokers: vec!["broker-1:9092".into(), "broker-2:9092".into()],
            topics: vec!["input-topic".into()],
            group_id: "test-group".into(),
            ..SourceConfig::default()
        }
    }

    fn default_sink() -> SinkConfig {
        SinkConfig {
            brokers: vec!["broker-1:9092".into()],
            topic: "output-topic".into(),
            compression: "zstd".into(),
            ..SinkConfig::default()
        }
    }

    #[test]
    fn test_build_consumer_config_basic() {
        let source = default_source();
        let config = build_consumer_config(&source);
        assert_eq!(config.brokers, vec!["broker-1:9092", "broker-2:9092"]);
        assert_eq!(config.group, "test-group");
        assert_eq!(config.topics, vec!["input-topic"]);
        assert_eq!(config.client_id, "dfe-transform-vrl-consumer");
    }

    #[test]
    fn test_build_consumer_config_with_sasl() {
        let mut source = default_source();
        source.sasl = SaslConfig {
            enabled: true,
            mechanism: "scram_sha_512".into(),
            username: "user".into(),
            password: "pass".into(),
        };
        source.tls = TlsConfig {
            enabled: true,
            ..TlsConfig::default()
        };
        let config = build_consumer_config(&source);
        assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-512".into()));
        assert_eq!(config.sasl_username, Some("user".into()));
        assert_eq!(config.sasl_password, Some("pass".into()));
        assert_eq!(config.security_protocol, "sasl_ssl");
    }

    #[test]
    fn test_build_consumer_config_sasl_without_tls() {
        let mut source = default_source();
        source.sasl = SaslConfig {
            enabled: true,
            mechanism: "plain".into(),
            username: "u".into(),
            password: "p".into(),
        };
        let config = build_consumer_config(&source);
        assert_eq!(config.sasl_mechanism, Some("PLAIN".into()));
        assert_eq!(config.security_protocol, "sasl_plaintext");
    }

    #[test]
    fn test_build_consumer_config_tls_only() {
        let mut source = default_source();
        source.tls = TlsConfig {
            enabled: true,
            ca_cert_file: Some("/ca.pem".into()),
            cert_file: Some("/cert.pem".into()),
            key_file: Some("/key.pem".into()),
            skip_verify: true,
        };
        let config = build_consumer_config(&source);
        assert_eq!(config.security_protocol, "ssl");
        assert_eq!(config.ssl_ca_location, Some("/ca.pem".into()));
        assert_eq!(config.ssl_certificate_location, Some("/cert.pem".into()));
        assert_eq!(config.ssl_key_location, Some("/key.pem".into()));
        assert!(config.ssl_skip_verify);
    }

    /// `max_buffer_bytes` bounds librdkafka's prefetch queue. The override
    /// layer is the one the transport applies last, so the queue cap must be
    /// there, and fetch sizing is left to scalo's sizing surface.
    #[test]
    fn max_buffer_bytes_caps_the_consumer_prefetch_queue() {
        for (bytes, kib) in [
            (67_108_864_u64, "65536"),
            (16_777_216, "16384"),
            (1023, "1"),
        ] {
            let mut source = default_source();
            source.max_buffer_bytes = bytes;
            let config = build_consumer_config(&source);
            assert_eq!(
                config
                    .librdkafka_overrides
                    .get("queued.max.messages.kbytes")
                    .map(String::as_str),
                Some(kib),
                "max_buffer_bytes {bytes}"
            );
            assert!(
                !config.librdkafka_overrides.contains_key("fetch.max.bytes"),
                "fetch sizing stays with the sizing surface"
            );
        }
    }

    /// An operator's own `queued.max.messages.kbytes` wins over the derived cap.
    #[test]
    fn a_librdkafka_option_overrides_the_derived_queue_cap() {
        let mut source = default_source();
        source
            .librdkafka_options
            .insert("queued.max.messages.kbytes".into(), "2048".into());
        let config = build_consumer_config(&source);
        assert_eq!(
            config
                .librdkafka_overrides
                .get("queued.max.messages.kbytes")
                .map(String::as_str),
            Some("2048")
        );
    }

    /// `/readyz` reads scalo's health registry, so each Kafka transport this
    /// service builds must register there: a refused credential, a missing
    /// topic or a fenced consumer then takes the pod out of service.
    #[tokio::test]
    async fn the_kafka_transports_register_with_readiness() {
        let count = || {
            scalo::health::HealthRegistry::components()
                .iter()
                .filter(|(name, _)| name == "transport:kafka")
                .count()
        };
        let before = count();
        let mut source = default_source();
        source.brokers = vec!["127.0.0.1:1".into()];
        let consumer = create_consumer(&build_consumer_config(&source))
            .await
            .unwrap();
        let mut sink = default_sink();
        sink.brokers = vec!["127.0.0.1:1".into()];
        let producer = create_producer(&build_producer_config(&sink, "p"))
            .await
            .unwrap();
        assert_eq!(count(), before + 2);
        drop((consumer, producer));
    }

    #[test]
    fn test_build_consumer_config_librdkafka_overrides() {
        let mut source = default_source();
        source
            .librdkafka_options
            .insert("fetch.min.bytes".into(), "1024".into());
        let config = build_consumer_config(&source);
        assert_eq!(
            config.librdkafka_overrides.get("fetch.min.bytes"),
            Some(&"1024".to_string())
        );
    }

    /// A private broker CA travels as PEM text, so a pod with no certificate
    /// file mounted still verifies the broker, and production posture accepts it.
    #[test]
    fn a_private_ca_reaches_librdkafka_as_pem_without_a_mounted_file() {
        const PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBprivateca\n-----END CERTIFICATE-----\n";
        let sasl = SaslConfig {
            enabled: true,
            mechanism: "scram_sha_512".into(),
            username: "u".into(),
            password: "p".into(),
        };
        let tls = TlsConfig {
            enabled: true,
            ..TlsConfig::default()
        };

        let mut source = default_source();
        source.sasl = sasl.clone();
        source.tls = tls.clone();
        source
            .librdkafka_options
            .insert("ssl.ca.pem".into(), PEM.into());
        let consumer = build_consumer_config(&source);
        assert_eq!(consumer.security_protocol, "sasl_ssl");
        assert_eq!(consumer.ssl_ca_location, None);
        assert_eq!(
            consumer
                .librdkafka_overrides
                .get("ssl.ca.pem")
                .map(String::as_str),
            Some(PEM)
        );
        consumer.validate(true).unwrap();

        let mut sink = default_sink();
        sink.sasl = sasl;
        sink.tls = tls;
        sink.librdkafka_options
            .insert("ssl.ca.pem".into(), PEM.into());
        let producer = build_producer_config(&sink, "p");
        producer.validate(true).unwrap();
        let client = scalo::transport::kafka::producer_client_config(&producer, &[]);
        assert_eq!(client.get("ssl.ca.pem"), Some(PEM));
        assert_eq!(client.get("security.protocol"), Some("sasl_ssl"));
    }

    #[test]
    fn test_build_producer_config_basic() {
        let sink = default_sink();
        let config = build_producer_config(&sink, "test-pipeline");
        assert_eq!(config.brokers, vec!["broker-1:9092"]);
        assert_eq!(config.topics, vec!["output-topic"]);
        assert_eq!(config.client_id, "dfe-transform-vrl-producer-test-pipeline");
        assert_eq!(config.group, "");
    }

    #[test]
    fn test_build_producer_config_overrides() {
        let sink = default_sink();
        let config = build_producer_config(&sink, "p");
        assert_eq!(
            config.librdkafka_overrides.get("compression.type"),
            Some(&"zstd".to_string())
        );
        assert_eq!(
            config.librdkafka_overrides.get("message.timeout.ms"),
            Some(&sink.message_timeout_ms.to_string())
        );
    }

    #[test]
    fn test_build_producer_config_custom_librdkafka() {
        let mut sink = default_sink();
        sink.librdkafka_options
            .insert("linger.ms".into(), "5".into());
        let config = build_producer_config(&sink, "p");
        assert_eq!(
            config.librdkafka_overrides.get("linger.ms"),
            Some(&"5".to_string())
        );
    }

    #[test]
    fn test_derive_dfe_source_from_land_topic() {
        let source = SourceConfig {
            topics: vec!["syslog_land".into()],
            ..SourceConfig::default()
        };
        let dfe = derive_dfe_source(&source).unwrap();
        assert_eq!(dfe.name(), "syslog");
        assert_eq!(dfe.input_topic(), "syslog_land");
        assert_eq!(dfe.output_topic(), "syslog_load");
    }

    #[test]
    fn test_derive_dfe_source_no_convention() {
        let source = SourceConfig {
            topics: vec!["custom-topic".into()],
            ..SourceConfig::default()
        };
        assert!(derive_dfe_source(&source).is_none());
    }

    #[test]
    fn test_derive_consumer_group_explicit_wins() {
        let source = SourceConfig {
            group_id: "explicit-cg".into(),
            topics: vec!["syslog_land".into()],
            ..SourceConfig::default()
        };
        assert_eq!(derive_consumer_group(&source, "my-pipeline"), "explicit-cg");
    }

    #[test]
    fn test_derive_consumer_group_from_source() {
        let source = SourceConfig {
            group_id: String::new(),
            topics: vec!["syslog_land".into()],
            ..SourceConfig::default()
        };
        assert_eq!(
            derive_consumer_group(&source, "my-pipeline"),
            "dfe-transform-vrl-my-pipeline"
        );
    }

    /// Committed offsets and the broker ACL are keyed on these exact names, so
    /// the source-derived group must stay `dfe-transform-vrl-<pipeline>`.
    #[test]
    fn derived_consumer_group_keeps_the_dfe_prefix_the_broker_acl_grants() {
        let source = SourceConfig {
            group_id: String::new(),
            topics: vec!["syslog_land".into()],
            ..SourceConfig::default()
        };
        let derived = derive_dfe_source(&source).unwrap();
        assert_eq!(derived.group_prefix(), "dfe-");
        assert_eq!(
            derived
                .consumer_group(
                    "transform-vrl",
                    ServiceRole::Transform,
                    Some("my-pipeline"),
                    None
                )
                .unwrap(),
            "dfe-transform-vrl-my-pipeline"
        );
        assert_eq!(
            derived
                .consumer_group("transform-vrl", ServiceRole::Transform, None, None)
                .unwrap(),
            "dfe-transform-vrl-syslog"
        );
        assert_eq!(
            derive_consumer_group(&source, "my-pipeline"),
            "dfe-transform-vrl-my-pipeline"
        );
        assert_eq!(derive_consumer_group(&source, ""), "dfe-transform-vrl-");
    }

    #[test]
    fn test_derive_consumer_group_fallback() {
        let source = SourceConfig {
            group_id: String::new(),
            topics: vec!["custom-topic".into()],
            ..SourceConfig::default()
        };
        assert_eq!(
            derive_consumer_group(&source, "my-pipeline"),
            "dfe-transform-vrl-my-pipeline"
        );
    }

    #[test]
    fn test_sasl_mechanism_mapping() {
        let mut source = default_source();
        source.sasl.enabled = true;

        source.sasl.mechanism = "scram_sha_256".into();
        let config = build_consumer_config(&source);
        assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-256".into()));

        source.sasl.mechanism = "scram_sha_512".into();
        let config = build_consumer_config(&source);
        assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-512".into()));

        source.sasl.mechanism = "plain".into();
        let config = build_consumer_config(&source);
        assert_eq!(config.sasl_mechanism, Some("PLAIN".into()));
    }

    /// The Kafka Secret's spelling reaches librdkafka as librdkafka's own name.
    #[test]
    fn librdkafka_mechanism_spellings_reach_the_client_unchanged() {
        for (given, sent) in [
            ("PLAIN", "PLAIN"),
            ("SCRAM-SHA-256", "SCRAM-SHA-256"),
            ("SCRAM-SHA-512", "SCRAM-SHA-512"),
            ("scram-sha-512", "SCRAM-SHA-512"),
        ] {
            let mut source = default_source();
            source.sasl.enabled = true;
            source.sasl.mechanism = given.into();
            assert_eq!(
                build_consumer_config(&source).sasl_mechanism.as_deref(),
                Some(sent),
                "consumer, {given}"
            );

            let mut sink = default_sink();
            sink.sasl.enabled = true;
            sink.sasl.mechanism = given.into();
            assert_eq!(
                build_producer_config(&sink, "p").sasl_mechanism.as_deref(),
                Some(sent),
                "producer, {given}"
            );
        }
    }
}
