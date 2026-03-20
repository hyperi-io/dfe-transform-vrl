// Project:   dfe-transform-vrl
// File:      src/config/loader.rs
// Purpose:   Configuration structures and loading
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration structures and loading.
//!
//! Big-dial config schema for Kafka source/sink (wrapper-controlled),
//! VRL transform files, health/metrics endpoints, and scaling pressure.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Result;

/// SASL authentication for Kafka.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SaslConfig {
    pub enabled: bool,
    /// SASL mechanism: plain, `scram_sha_256`, `scram_sha_512`.
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanism: "scram_sha_512".to_string(),
            username: String::new(),
            password: String::new(),
        }
    }
}

/// TLS configuration for Kafka.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_cert_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub skip_verify: bool,
}

/// Main configuration.
///
/// ## Hot-reload classification
///
/// **Hot-reloaded** (takes effect on next batch, via `SharedConfig<HotConfig>`):
/// - `pipeline.batch_size`
/// - `pipeline.batch_timeout_ms`
/// - `sink.key_field`
/// - `scaling.pressure_threshold`
///
/// **Requires pod restart** (bound at startup):
/// - `pipeline.name` — baked into Kafka `group_id`, metrics labels, tracing spans
/// - `source.*` — rdkafka consumer: connection, subscription, auth, TLS, buffers
/// - `sink.brokers` — rdkafka producer connection established at startup
/// - `sink.topic` — output topic (changing mid-stream risks data loss)
/// - `sink.compression` — rdkafka `compression.type` set at producer creation
/// - `sink.sasl.*` / `sink.tls.*` — security protocol set at producer creation
/// - `sink.max_buffer_bytes` — rdkafka `queue.buffering.max.kbytes` at creation
/// - `sink.librdkafka_options` — passed to `ClientConfig` at creation
/// - `transforms.*` — VRL programs compiled at startup, immutable for process lifetime
/// - `health.address` — HTTP server binds to socket at startup
/// - `metrics.address` — metrics server binds to socket at startup
/// - `logging.*` — tracing subscriber configured at startup
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub pipeline: PipelineConfig,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub transforms: TransformConfig,
    #[serde(default)]
    pub enrichment_tables: Vec<EnrichmentTableConfig>,
    pub health: HealthConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
    pub scaling: ScalingConfig,
}

/// Enrichment table file reference.
///
/// Tables are loaded at startup into `HashMap<Key, Row>` for O(1) lookups.
/// Immutable for the process lifetime — restart the pod to update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrichmentTableConfig {
    /// Table name used in VRL: `get_enrichment_table_record("name", ...)`.
    pub name: String,
    /// Path to the enrichment data file (.csv or .json).
    pub path: String,
    /// Column(s) used as the lookup key. Multiple columns are concatenated.
    pub key_columns: Vec<String>,
}

/// Pipeline identity and processing settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PipelineConfig {
    /// Pipeline name (used in metrics labels, Kafka `group_id`, logging).
    pub name: String,
    /// Events per transform batch.
    pub batch_size: usize,
    /// Max wait (ms) for a full batch before processing a partial batch.
    pub batch_timeout_ms: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            batch_size: 1000,
            batch_timeout_ms: 100,
        }
    }
}

/// Kafka source configuration (wrapper-controlled consumer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SourceConfig {
    pub brokers: Vec<String>,
    pub topics: Vec<String>,
    pub group_id: String,
    /// Payload format: auto, json, msgpack.
    pub format: String,
    /// Maximum consumer buffer size in bytes.
    pub max_buffer_bytes: u64,
    pub sasl: SaslConfig,
    pub tls: TlsConfig,
    pub auto_offset_reset: String,
    pub session_timeout_ms: u32,
    pub commit_interval_ms: u32,
    /// Extra librdkafka options.
    pub librdkafka_options: BTreeMap<String, String>,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["events".to_string()],
            group_id: "dfe-transform-vrl".to_string(),
            format: "auto".to_string(),
            max_buffer_bytes: 67_108_864, // 64 MiB
            sasl: SaslConfig::default(),
            tls: TlsConfig::default(),
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 30_000,
            commit_interval_ms: 5_000,
            librdkafka_options: BTreeMap::new(),
        }
    }
}

/// Kafka sink configuration (wrapper-controlled producer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SinkConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    /// Event field path for Kafka partition key (e.g., ".`org_id`").
    pub key_field: String,
    /// Compression: none, gzip, lz4, snappy, zstd.
    pub compression: String,
    /// Maximum producer buffer size in bytes.
    pub max_buffer_bytes: u64,
    pub sasl: SaslConfig,
    pub tls: TlsConfig,
    /// Producer delivery timeout (ms).
    pub message_timeout_ms: u32,
    /// Extra librdkafka options.
    pub librdkafka_options: BTreeMap<String, String>,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topic: String::new(),
            key_field: String::new(),
            compression: "zstd".to_string(),
            max_buffer_bytes: 67_108_864, // 64 MiB
            sasl: SaslConfig::default(),
            tls: TlsConfig::default(),
            message_timeout_ms: 300_000,
            librdkafka_options: BTreeMap::new(),
        }
    }
}

/// VRL transform file configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TransformConfig {
    /// Directory to load all .vrl transform files from (sorted by filename).
    pub dir: Option<String>,
    /// Explicit list of .vrl file paths (loaded in order).
    pub files: Option<Vec<String>>,
}

/// Health endpoint configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    pub address: String,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:9000".to_string(),
        }
    }
}

/// Metrics endpoint configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:9090".to_string(),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "json".to_string(),
        }
    }
}

/// KEDA scaling pressure configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ScalingConfig {
    pub pressure_threshold: f64,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            pressure_threshold: 0.8,
        }
    }
}

// =============================================================================
// Config loading cascade — uses rustlib flat_env helpers
// =============================================================================

use hyperi_rustlib::config::flat_env::{
    ApplyFlatEnv, Normalize, flat_env_list, flat_env_string, flat_env_string_sensitive,
};

const ENV_PREFIX: &str = "DFE_TRANSFORM";

/// Flat env overrides for K8s-friendly single-underscore env vars.
///
/// Env var names are the contract with dfe-engine — do not rename.
/// Uses rustlib `flat_env_*` helpers for consistent parsing and logging.
impl ApplyFlatEnv for Config {
    fn apply_flat_env(&mut self, prefix: &str) {
        // Pipeline
        if let Some(v) = flat_env_string(prefix, "PIPELINE_NAME") {
            self.pipeline.name = v;
        }
        // Source
        if let Some(v) = flat_env_list(prefix, "SOURCE_BROKERS") {
            self.source.brokers = v;
        }
        if let Some(v) = flat_env_list(prefix, "SOURCE_TOPICS") {
            self.source.topics = v;
        }
        if let Some(v) = flat_env_string(prefix, "SOURCE_GROUP_ID") {
            self.source.group_id = v;
        }
        if let Some(v) = flat_env_string(prefix, "SOURCE_FORMAT") {
            self.source.format = v;
        }
        if let Some(v) = flat_env_string(prefix, "SOURCE_SASL_USERNAME") {
            self.source.sasl.username = v;
        }
        if let Some(v) = flat_env_string_sensitive(prefix, "SOURCE_SASL_PASSWORD") {
            self.source.sasl.password = v;
        }
        // Sink
        if let Some(v) = flat_env_list(prefix, "SINK_BROKERS") {
            self.sink.brokers = v;
        }
        if let Some(v) = flat_env_string(prefix, "SINK_TOPIC") {
            self.sink.topic = v;
        }
        if let Some(v) = flat_env_string(prefix, "SINK_KEY_FIELD") {
            self.sink.key_field = v;
        }
        if let Some(v) = flat_env_string(prefix, "SINK_COMPRESSION") {
            self.sink.compression = v;
        }
        if let Some(v) = flat_env_string(prefix, "SINK_SASL_USERNAME") {
            self.sink.sasl.username = v;
        }
        if let Some(v) = flat_env_string_sensitive(prefix, "SINK_SASL_PASSWORD") {
            self.sink.sasl.password = v;
        }
        // Transforms
        if let Some(v) = flat_env_string(prefix, "TRANSFORMS_DIR") {
            self.transforms.dir = Some(v);
        }
        // Infra
        if let Some(v) = flat_env_string(prefix, "HEALTH_ADDRESS") {
            self.health.address = v;
        }
        if let Some(v) = flat_env_string(prefix, "METRICS_ADDRESS") {
            self.metrics.address = v;
        }
    }
}

/// Normalise config after all sources merge.
/// Infers implied settings regardless of how values arrived.
impl Normalize for Config {
    fn normalize(&mut self) {
        // Credentials present → enable SASL auth
        if !self.source.sasl.username.is_empty() {
            self.source.sasl.enabled = true;
        }
        if !self.sink.sasl.username.is_empty() {
            self.sink.sasl.enabled = true;
        }
        // TLS cert present → enable TLS
        if self.source.tls.ca_cert_file.is_some() {
            self.source.tls.enabled = true;
        }
        if self.sink.tls.ca_cert_file.is_some() {
            self.sink.tls.enabled = true;
        }
    }
}

impl Config {
    /// Load configuration with full cascade.
    ///
    /// Priority (highest to lowest):
    ///   1. CLI args (handled by caller)
    ///   2. Flat env overrides (`DFE_TRANSFORM_SOURCE_BROKERS`, etc.)
    ///   3. Figment env vars with `__` nesting (`DFE_TRANSFORM_SOURCE__BROKERS`)
    ///   4. `.env` file (via dotenvy)
    ///   5. Config YAML file
    ///   6. Hard-coded defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        let _ = dotenvy::dotenv();
        let mut config = Self::default();

        if let Some(path) = config_path {
            if Path::new(path).exists() {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
                config = serde_yaml_ng::from_str(&content)?;
                debug!(path, "loaded configuration file");
            }
        } else {
            for path in &["config.yaml", "config.yml"] {
                if Path::new(path).exists() {
                    let content = std::fs::read_to_string(path)
                        .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
                    config = serde_yaml_ng::from_str(&content)?;
                    debug!(path, "loaded configuration file");
                    break;
                }
            }
        }

        // Figment env (double-underscore nesting)
        {
            use figment::Figment;
            use figment::providers::{Env, Serialized};

            let figment = Figment::from(Serialized::defaults(&config))
                .merge(Env::prefixed(&format!("{ENV_PREFIX}_")).split("__"));

            config = figment
                .extract()
                .map_err(|e| crate::Error::Config(e.to_string()))?;
        }

        // Flat env overrides (single-underscore, K8s-friendly)
        config.apply_flat_env(ENV_PREFIX);

        // Normalise (infer implied settings)
        config.normalize();

        Ok(config)
    }
}
