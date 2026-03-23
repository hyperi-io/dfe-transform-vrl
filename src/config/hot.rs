// Project:   dfe-transform-vrl
// File:      src/config/hot.rs
// Purpose:   Hot-reloadable config subset
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Hot-reloadable configuration subset.
//!
//! Only fields that can safely change at runtime without restarting the process
//! live here. The pipeline reads these via `SharedConfig<HotConfig>` each batch.
//!
//! ## Hot-reloaded (takes effect on next batch):
//!
//! - `batch_size` — events per transform batch
//! - `batch_timeout_ms` — max wait for a full batch before flushing partial
//! - `key_field` — sink partition key path (e.g. `.org_id`, `.host.name`)
//! - `scaling_pressure_threshold` — KEDA pressure threshold
//!
//! ## Requires pod restart (bound at startup):
//!
//! - `pipeline.name` — baked into Kafka `group_id`, metrics labels, tracing spans
//! - `source.*` — rdkafka consumer connection, subscription, auth, TLS, buffer sizes
//! - `sink.brokers` — rdkafka producer connection, auth, TLS
//! - `sink.topic` — output topic (changing mid-stream risks data loss)
//! - `sink.compression` — rdkafka compression.type set at producer creation
//! - `sink.sasl.*` / `sink.tls.*` — security protocol set at producer creation
//! - `sink.max_buffer_bytes` — rdkafka queue.buffering.max.kbytes at creation
//! - `sink.librdkafka_options` — passed to `ClientConfig` at creation
//! - `transforms.*` — VRL programs compiled at startup (immutable for process lifetime)
//! - `health.address` — HTTP server binds to socket at startup
//! - `metrics.address` — metrics server binds to socket at startup
//! - `logging.*` — tracing subscriber configured at startup

use serde::{Deserialize, Serialize};

use super::loader::Config;

/// Configuration fields that are safe to change at runtime.
///
/// Read via `SharedConfig<HotConfig>` by the pipeline on each batch iteration.
/// Updated by `ConfigReloader` when the config file changes (or on SIGHUP).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotConfig {
    /// Events per transform batch.
    pub batch_size: usize,

    /// Max wait (ms) for a full batch before processing a partial batch.
    pub batch_timeout_ms: u64,

    /// Event field path for Kafka partition key (e.g. `.org_id`, `.host.name`).
    pub key_field: String,

    /// KEDA scaling pressure threshold (0.0–1.0).
    pub scaling_pressure_threshold: f64,
}

impl HotConfig {
    /// Extract hot-reloadable fields from the full config.
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            batch_size: config.pipeline.batch_size,
            batch_timeout_ms: config.pipeline.batch_timeout_ms,
            key_field: config.sink.key_field.clone(),
            scaling_pressure_threshold: config.scaling.pressure_threshold,
        }
    }
}

impl Default for HotConfig {
    fn default() -> Self {
        let config = Config::default();
        Self::from_config(&config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_extracts_correct_fields() {
        let mut config = Config::default();
        config.pipeline.batch_size = 5000;
        config.pipeline.batch_timeout_ms = 250;
        config.sink.key_field = ".tenant_id".to_string();
        config.scaling.pressure_threshold = 0.6;

        let hot = HotConfig::from_config(&config);
        assert_eq!(hot.batch_size, 5000);
        assert_eq!(hot.batch_timeout_ms, 250);
        assert_eq!(hot.key_field, ".tenant_id");
        assert!((hot.scaling_pressure_threshold - 0.6).abs() < f64::EPSILON);
    }

    #[test]
    fn default_matches_config_defaults() {
        let hot_default = HotConfig::default();
        let config_default = Config::default();

        assert_eq!(hot_default.batch_size, config_default.pipeline.batch_size);
        assert_eq!(
            hot_default.batch_timeout_ms,
            config_default.pipeline.batch_timeout_ms
        );
        assert_eq!(hot_default.key_field, config_default.sink.key_field);
    }

    #[test]
    fn partial_eq_works() {
        let a = HotConfig::default();
        let b = HotConfig::default();
        assert_eq!(a, b);

        let mut c = HotConfig::default();
        c.batch_size = 999;
        assert_ne!(a, c);
    }

    #[test]
    fn clone_produces_independent_copy() {
        let mut a = HotConfig::default();
        let b = a.clone();
        a.batch_size = 42;
        assert_ne!(a.batch_size, b.batch_size);
    }

    #[test]
    fn serde_roundtrip() {
        let hot = HotConfig {
            batch_size: 2000,
            batch_timeout_ms: 500,
            key_field: ".org_id".to_string(),
            scaling_pressure_threshold: 0.9,
        };
        let yaml = serde_yaml_ng::to_string(&hot).unwrap();
        let deserialized: HotConfig = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(hot, deserialized);
    }
}
