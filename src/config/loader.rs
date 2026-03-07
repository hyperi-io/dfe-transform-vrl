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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SaslConfig {
    pub enabled: bool,
    /// SASL mechanism: plain, scram_sha_256, scram_sha_512.
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_cert_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub skip_verify: bool,
}

/// Main configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub pipeline: PipelineConfig,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub transforms: TransformConfig,
    pub health: HealthConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
    pub scaling: ScalingConfig,
}

/// Pipeline identity and processing settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PipelineConfig {
    /// Pipeline name (used in metrics labels, Kafka group_id, logging).
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SinkConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    /// Event field path for Kafka partition key (e.g., ".org_id").
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TransformConfig {
    /// Directory to load all .vrl transform files from (sorted by filename).
    pub dir: Option<String>,
    /// Explicit list of .vrl file paths (loaded in order).
    pub files: Option<Vec<String>>,
}

/// Health endpoint configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
// Config loading cascade
// =============================================================================

const ENV_PREFIX: &str = "DFE_TRANSFORM";

fn env_var(name: &str) -> Option<String> {
    std::env::var(format!("{ENV_PREFIX}_{name}")).ok()
}

fn env_var_list(name: &str) -> Option<Vec<String>> {
    env_var(name).map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
}

fn apply_figment_env(config: &mut Config) -> Result<()> {
    use figment::Figment;
    use figment::providers::{Env, Serialized};

    let figment = Figment::from(Serialized::defaults(&*config))
        .merge(Env::prefixed(&format!("{ENV_PREFIX}_")).split("__"));

    *config = figment
        .extract()
        .map_err(|e| crate::Error::Config(e.to_string()))?;
    Ok(())
}

fn apply_env_overrides(config: &mut Config) {
    if let Some(v) = env_var("PIPELINE_NAME") {
        config.pipeline.name = v;
        debug!("override: pipeline.name from env");
    }
    if let Some(v) = env_var_list("SOURCE_BROKERS") {
        config.source.brokers = v;
        debug!("override: source.brokers from env");
    }
    if let Some(v) = env_var_list("SOURCE_TOPICS") {
        config.source.topics = v;
        debug!("override: source.topics from env");
    }
    if let Some(v) = env_var("SOURCE_GROUP_ID") {
        config.source.group_id = v;
        debug!("override: source.group_id from env");
    }
    if let Some(v) = env_var("SOURCE_FORMAT") {
        config.source.format = v;
        debug!("override: source.format from env");
    }
    if let Some(v) = env_var("SOURCE_SASL_USERNAME") {
        config.source.sasl.enabled = true;
        config.source.sasl.username = v;
        debug!("override: source.sasl.username from env");
    }
    if let Some(v) = env_var("SOURCE_SASL_PASSWORD") {
        config.source.sasl.enabled = true;
        config.source.sasl.password = v;
        debug!("override: source.sasl.password from env");
    }
    if let Some(v) = env_var_list("SINK_BROKERS") {
        config.sink.brokers = v;
        debug!("override: sink.brokers from env");
    }
    if let Some(v) = env_var("SINK_TOPIC") {
        config.sink.topic = v;
        debug!("override: sink.topic from env");
    }
    if let Some(v) = env_var("SINK_KEY_FIELD") {
        config.sink.key_field = v;
        debug!("override: sink.key_field from env");
    }
    if let Some(v) = env_var("SINK_COMPRESSION") {
        config.sink.compression = v;
        debug!("override: sink.compression from env");
    }
    if let Some(v) = env_var("SINK_SASL_USERNAME") {
        config.sink.sasl.enabled = true;
        config.sink.sasl.username = v;
        debug!("override: sink.sasl.username from env");
    }
    if let Some(v) = env_var("SINK_SASL_PASSWORD") {
        config.sink.sasl.enabled = true;
        config.sink.sasl.password = v;
        debug!("override: sink.sasl.password from env");
    }
    if let Some(v) = env_var("TRANSFORMS_DIR") {
        config.transforms.dir = Some(v);
        debug!("override: transforms.dir from env");
    }
    if let Some(v) = env_var("HEALTH_ADDRESS") {
        config.health.address = v;
        debug!("override: health.address from env");
    }
    if let Some(v) = env_var("METRICS_ADDRESS") {
        config.metrics.address = v;
        debug!("override: metrics.address from env");
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
        let mut config = Config::default();

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

        apply_figment_env(&mut config)?;
        apply_env_overrides(&mut config);

        Ok(config)
    }
}
