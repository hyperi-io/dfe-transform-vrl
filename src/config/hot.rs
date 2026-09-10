// Project:   dfe-transform-vrl
// File:      src/config/hot.rs
// Purpose:   Hot-reloadable config subset
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Reloadable configuration subset.
//!
//! The fields that could safely change at runtime without restarting the
//! process. `ConfigReloader` re-reads and re-validates the file on a change or
//! a SIGHUP and swaps this struct through `SharedConfig<HotConfig>`.
//!
//! ## Carried here
//!
//! - `batch_size` — events per transform batch
//! - `batch_timeout_ms` — max wait for a full batch before flushing partial
//! - `key_field` — sink partition key path (e.g. `.org_id`, `.host.name`)
//!
//! All three are on [`crate::config::INERT_SETTINGS`]: the pipeline holds this
//! struct but reads none of them yet, so a reload today re-validates the file
//! and changes no behaviour. The wrapper warns at startup for each one a
//! deployment has set rather than implying otherwise.
//!
//! ## Requires pod restart (bound at startup)
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
//! - `metrics.address` — scalo binds the metrics server, which also serves the
//!   probes, before `run_service`
//! - `logger.*` — tracing subscriber built once, before the runtime

use serde::{Deserialize, Serialize};

use super::loader::Config;

/// Configuration fields that are safe to change at runtime.
///
/// Read via `SharedConfig<HotConfig>` by the pipeline on each batch iteration.
/// Updated by `ConfigReloader` when the config file changes (or on SIGHUP).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotConfig {
    /// Events per transform batch.
    pub batch_size: usize,

    /// Max wait (ms) for a full batch before processing a partial batch.
    pub batch_timeout_ms: u64,

    /// Event field path for Kafka partition key (e.g. `.org_id`, `.host.name`).
    pub key_field: String,
}

impl HotConfig {
    /// Extract hot-reloadable fields from the full config.
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            batch_size: config.pipeline.batch_size,
            batch_timeout_ms: config.pipeline.batch_timeout_ms,
            key_field: config.sink.key_field.clone(),
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn from_config_extracts_correct_fields() {
        let mut config = Config::default();
        config.pipeline.batch_size = 5000;
        config.pipeline.batch_timeout_ms = 250;
        config.sink.key_field = ".tenant_id".to_string();

        let hot = HotConfig::from_config(&config);
        assert_eq!(hot.batch_size, 5000);
        assert_eq!(hot.batch_timeout_ms, 250);
        assert_eq!(hot.key_field, ".tenant_id");
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

        let c = HotConfig {
            batch_size: 999,
            ..HotConfig::default()
        };
        assert_ne!(a, c);
    }

    #[test]
    fn clone_produces_independent_copy() {
        let a = HotConfig {
            batch_size: 42,
            ..HotConfig::default()
        };
        let b = HotConfig::default();
        assert_ne!(a.batch_size, b.batch_size);
    }

    #[test]
    fn serde_roundtrip() {
        let hot = HotConfig {
            batch_size: 2000,
            batch_timeout_ms: 500,
            key_field: ".org_id".to_string(),
        };
        let yaml = serde_yaml_ng::to_string(&hot).unwrap();
        let deserialized: HotConfig = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(hot, deserialized);
    }
}
