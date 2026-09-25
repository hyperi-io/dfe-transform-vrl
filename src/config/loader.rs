// Project:   dfe-transform-vrl
// File:      src/config/loader.rs
// Purpose:   Configuration structures and loading
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration structures and loading.
//!
//! Big-dial config schema for Kafka source/sink (wrapper-controlled), VRL
//! transform files and enrichment tables. scalo's own sections are not in here
//! -- see [`SCALO_CASCADE_SECTIONS`].

use std::collections::BTreeMap;
use std::path::Path;

use scalo::SensitiveString;
use scalo::transport::AcknowledgementsConfig;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Result;

/// SASL authentication for Kafka.
///
/// `password` is a [`SensitiveString`] so every serialise path redacts it by
/// type rather than by a reader's field-name heuristic, and the emitted config
/// schema carries `x-dfe-secret` + `writeOnly` on the field by construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SaslConfig {
    pub enabled: bool,
    /// SASL mechanism: plain, `scram_sha_256`, `scram_sha_512`.
    pub mechanism: String,
    pub username: String,
    // Skipping an unset password keeps the redaction constant out of the
    // emitted schema: a secret's default is absent, not a placeholder.
    #[serde(skip_serializing_if = "SensitiveString::is_empty")]
    pub password: SensitiveString,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanism: "scram_sha_512".to_string(),
            username: String::new(),
            password: SensitiveString::default(),
        }
    }
}

/// TLS configuration for Kafka.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
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
/// ## This struct is the whole of the `--config` file
///
/// It owns `pipeline`, `source`, `sink`, `transforms` and `enrichment_tables`,
/// and nothing else belongs in that file. scalo's own sections --
/// `metrics`, `logger`, `scaling`, `worker_pool`, `batch_processing`,
/// `self_regulation`, `version_check` -- resolve from scalo's cascade, which
/// discovers files by fixed base name (`settings.yaml`, `defaults.yaml`) and so
/// never reads a file named `config.yaml`. They are set through the env layer
/// instead; [`SCALO_CASCADE_SECTIONS`] carries the mapping and
/// [`warn_unreachable_scalo_settings`] says so when one turns up here.
///
/// ## Requires pod restart (bound at startup)
///
/// - `pipeline.name` — baked into Kafka `group_id`, metrics labels, tracing spans
/// - `source.*` — rdkafka consumer: connection, subscription, auth, TLS, buffers
/// - `sink.brokers` — rdkafka producer connection established at startup
/// - `sink.topic` — output topic (changing mid-stream risks data loss)
/// - `sink.compression` — rdkafka `compression.type` set at producer creation
/// - `sink.sasl.*` / `sink.tls.*` — security protocol set at producer creation
/// - `sink.max_buffer_bytes` — rdkafka `queue.buffering.max.kbytes` at creation
/// - `sink.librdkafka_options` — passed to `ClientConfig` at creation
/// - `transforms.*` — VRL programs compiled at startup, immutable for process lifetime
///
/// ## Accepted but not applied
///
/// [`Config::inert_settings`] is the list, and the wrapper warns at startup for
/// each one a deployment has actually set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct Config {
    pub pipeline: PipelineConfig,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub transforms: TransformConfig,
    #[serde(default)]
    pub enrichment_tables: Vec<EnrichmentTableConfig>,
}

/// Enrichment table configuration.
///
/// Supports two config formats:
/// - **New (tagged):** `source` field with type-tagged enum (`file`, `mmdb`, `stix`, `sqlite`)
/// - **Legacy (flat):** `path` + `key_columns` only (treated as `File` with auto format detection)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, schemars::JsonSchema)]
#[serde(default)]
pub struct EnrichmentTableConfig {
    /// Table name used in VRL: `get_enrichment_table_record("name", ...)`.
    pub name: String,
    /// Path to enrichment data file (legacy flat format — kept for backwards compat).
    #[serde(default)]
    pub path: String,
    /// Lookup key column(s). Multiple columns are concatenated with `\x00`.
    #[serde(default)]
    pub key_columns: Vec<String>,
    /// Source definition (new tagged format). Takes precedence over `path`.
    #[serde(default)]
    pub source: Option<EnrichmentSourceConfig>,
    /// Per-column type coercion, matching Vector's file enrichment table
    /// `schema`. Without an entry a CSV cell stays a string, so
    /// `status_code: integer` is what makes `{"status_code": 1}` match, and a
    /// `timestamp` column is what makes a `{"from": ..., "to": ...}` date
    /// range able to match at all. Accepted values: `asis`, `bytes`,
    /// `string`, `int`, `integer`, `float`, `bool`, `boolean`, `date`,
    /// `date|<format>`, `timestamp`, `timestamp|<format>`. `asis` and
    /// `bytes` leave the cell alone.
    #[serde(default)]
    pub schema: BTreeMap<String, String>,
    /// Optional periodic refresh.
    #[serde(default)]
    pub refresh: Option<RefreshConfig>,
    /// Maximum materialised table size in bytes. Fail-fast at load if exceeded.
    /// When set, the loaded table's memory is also registered with `MemoryGuard`
    /// so enrichment memory counts against the pod memory budget.
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

impl EnrichmentTableConfig {
    /// Resolve the effective source config.
    ///
    /// If `source` is set (new format), use it directly.
    /// If only `path` is set (legacy format), treat as `File` with auto format detection.
    pub fn resolved_source(&self) -> crate::Result<EnrichmentSourceConfig> {
        if let Some(ref source) = self.source {
            return Ok(source.clone());
        }
        if !self.path.is_empty() {
            return Ok(EnrichmentSourceConfig::File {
                path: self.path.clone(),
                format: None,
            });
        }
        Err(crate::Error::Enrichment(format!(
            "enrichment table '{}': no source configured (set 'source' or 'path')",
            self.name
        )))
    }
}

/// Source definition for an enrichment table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum EnrichmentSourceConfig {
    File {
        path: String,
        #[serde(default)]
        format: Option<FileFormat>,
    },
    Mmdb {
        path: String,
    },
    Stix {
        #[serde(default)]
        path: Option<String>,
        #[serde(default)]
        url: Option<String>,
        #[serde(default)]
        collection: Option<String>,
        #[serde(default)]
        auth: Option<StixAuthConfig>,
    },
    Sqlite {
        path: String,
        query: String,
    },
}

/// File format for enrichment table loading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Csv,
    Json,
    Yaml,
    Auto,
}

/// Authentication for STIX/TAXII sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StixAuthConfig {
    /// Auth type: `bearer`, `basic`, or `api_key`.
    #[serde(rename = "type")]
    pub auth_type: String,
    #[serde(default)]
    pub token_env: Option<String>,
    #[serde(default)]
    pub username_env: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
}

/// Periodic refresh configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RefreshConfig {
    /// Refresh interval in seconds. Minimum enforced: 60.
    pub interval_secs: u64,
}

/// Pipeline identity and processing settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
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

/// Which transport a stage uses.
///
/// One deployment runs one of them: `bus` is a broker between the stages,
/// `direct` is gRPC between them and needs no broker at all. The record and
/// the transform are identical either way -- only who hands the record over
/// changes.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// Kafka topics.
    #[default]
    Bus,
    /// A scalo Push listener (source) or client (sink).
    Direct,
}

impl Transport {
    /// Whether this stage is on the direct transport.
    #[must_use]
    pub const fn is_direct(self) -> bool {
        matches!(self, Self::Direct)
    }
}

/// Source configuration: the bus topics to consume, or the Push listener to
/// accept records on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SourceConfig {
    /// `bus` consumes `topics`; `direct` accepts pushes on `listen`.
    pub transport: Transport,
    /// Address the Push listener binds on the direct transport.
    pub listen: String,
    pub brokers: Vec<String>,
    pub topics: Vec<String>,
    pub group_id: String,
    /// Maximum consumer buffer size in bytes.
    pub max_buffer_bytes: u64,
    pub sasl: SaslConfig,
    pub tls: TlsConfig,
    pub auto_offset_reset: String,
    pub session_timeout_ms: u32,
    pub commit_interval_ms: u32,
    /// librdkafka statistics emission interval (ms). 0 = disabled.
    /// When enabled (and scalo supports `StatsContext` in `KafkaTransport`),
    /// rdkafka broker RTT, consumer lag, and queue depth metrics auto-emit
    /// to the Prometheus `/metrics` endpoint.
    pub statistics_interval_ms: u32,
    /// Extra librdkafka options.
    pub librdkafka_options: BTreeMap<String, String>,
    /// On (the default), the source is acknowledged -- a Kafka offset commit, a
    /// Push answered OK -- only once the transformed records are delivered to
    /// the sink, so a crash or a failed send re-delivers rather than loses them.
    /// Off, it is acknowledged at receipt.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            transport: Transport::default(),
            listen: "0.0.0.0:6000".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["events".to_string()],
            group_id: "dfe-transform-vrl".to_string(),
            max_buffer_bytes: 67_108_864, // 64 MiB
            sasl: SaslConfig::default(),
            tls: TlsConfig::default(),
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 30_000,
            commit_interval_ms: 5_000,
            statistics_interval_ms: 5_000,
            librdkafka_options: BTreeMap::new(),
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// Sink configuration: the bus topic to produce to, or the Push listener to
/// send the transformed records on to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SinkConfig {
    /// `bus` produces to `topic`; `direct` pushes to `endpoint`.
    pub transport: Transport,
    /// Downstream Push listener on the direct transport, e.g. the loader.
    pub endpoint: String,
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
    /// Producer delivery timeout (ms). Capped at 15 s while a direct source
    /// holds its acknowledgements, so a report arrives inside the hold;
    /// `librdkafka_options.message.timeout.ms` overrides the cap.
    pub message_timeout_ms: u32,
    /// Extra librdkafka options.
    pub librdkafka_options: BTreeMap<String, String>,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            transport: Transport::default(),
            endpoint: "http://dfe-loader:6000".to_string(),
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct TransformConfig {
    /// Directory to load all .vrl transform files from (sorted by filename).
    pub dir: Option<String>,
    /// Explicit list of .vrl file paths (loaded in order).
    pub files: Option<Vec<String>>,
}

// =============================================================================
// scalo's own config sections
// =============================================================================

/// Top-level sections that belong to scalo's cascade, not to [`Config`].
///
/// `scalo::config` discovers files by fixed base name -- `defaults.yaml`,
/// `settings.yaml`, `settings.{env}.yaml` -- in the working directory,
/// `config/`, `/config/`, `~/.config/{app}/` and any extra directory. There is
/// no way to point it at a file called `config.yaml`, so a section listed here
/// reaches nothing when it is written into the `--config` file. The env layer
/// does reach it, hence the paired variable.
pub const SCALO_CASCADE_SECTIONS: &[(&str, &str)] = &[
    ("metrics", "METRICS_ADDR, or DFE_TRANSFORM_METRICS__*"),
    (
        "logger",
        "LOG_LEVEL / LOG_FORMAT, or DFE_TRANSFORM_LOGGER__*",
    ),
    ("scaling", "DFE_TRANSFORM_SCALING__*"),
    ("worker_pool", "DFE_TRANSFORM_WORKER_POOL__*"),
    ("batch_processing", "DFE_TRANSFORM_BATCH_PROCESSING__*"),
    ("self_regulation", "DFE_TRANSFORM_SELF_REGULATION__*"),
    ("version_check", "DFE_TRANSFORM_VERSION_CHECK__*"),
];

/// Warn for each [`SCALO_CASCADE_SECTIONS`] entry a deployment has aimed at
/// and missed.
///
/// The two ways to miss are writing the section into the `--config` file, and
/// spelling its env var with one underscore where the cascade splits on two.
/// Re-reads the file rather than threading the raw YAML through [`Config`]:
/// once, at startup, and only to report.
pub fn warn_unreachable_scalo_settings(config_path: Option<&str>) {
    if let Some(path) = config_path
        && let Ok(content) = std::fs::read_to_string(path)
        && let Ok(serde_yaml_ng::Value::Mapping(map)) = serde_yaml_ng::from_str(&content)
    {
        for (section, instead) in SCALO_CASCADE_SECTIONS {
            if map.contains_key(serde_yaml_ng::Value::String((*section).to_string())) {
                tracing::warn!(
                    section = section,
                    instead = instead,
                    path = path,
                    "config section belongs to the scalo cascade, which cannot \
                     read this file -- the values in it are not applied"
                );
            }
        }
    }

    // `Env::prefixed("DFE_TRANSFORM_").split("__")` strips the prefix and then
    // needs a DOUBLE underscore to nest, so DFE_TRANSFORM_METRICS_ADDRESS
    // becomes the flat key `metrics_address` and matches no section.
    for (name, _) in std::env::vars() {
        for (section, instead) in SCALO_CASCADE_SECTIONS {
            let single = format!("{ENV_PREFIX}_{}_", section.to_uppercase());
            let double = format!("{single}_");
            if name.starts_with(&single) && !name.starts_with(&double) {
                tracing::warn!(
                    env_var = name,
                    instead = instead,
                    "env var needs a double underscore after the section to \
                     nest into the scalo cascade -- as spelled it is not applied"
                );
            }
        }
    }
}

// =============================================================================
// Settings the wrapper accepts but does not apply
// =============================================================================

/// A setting the config schema accepts, that parses, and that reaches nothing.
///
/// An entry here makes the wrapper warn at startup when a deployment sets it,
/// and gives `every_inert_setting_is_detectable` a surface to walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InertSetting {
    /// Dotted key, spelled as a deployment writes it.
    pub key: &'static str,
    /// What it fails to reach.
    pub why: &'static str,
    /// The key that does the job, where one exists.
    pub instead: Option<&'static str>,
}

/// Every setting in that state. See [`Config::inert_settings`].
pub const INERT_SETTINGS: &[InertSetting] = &[
    InertSetting {
        key: "pipeline.batch_size",
        why: "the batch engine is built by the scalo runtime before run_service, \
              and takes its chunk size from batch_processing.max_chunk_size",
        instead: Some("batch_processing.max_chunk_size"),
    },
    InertSetting {
        key: "pipeline.batch_timeout_ms",
        why: "the governed engine driver has no partial-batch timer -- it blocks \
              on the consumer and processes whatever the fetch returned",
        instead: None,
    },
    InertSetting {
        key: "sink.key_field",
        why: "the producer's key argument carries the destination topic, not a \
              partition key, so no key is derived from the event (scalo-rs#37)",
        instead: None,
    },
    InertSetting {
        key: "source.commit_interval_ms",
        why: "the production profile disables librdkafka auto-commit -- the \
              engine commits at the at-least-once barrier after each sink ack",
        instead: None,
    },
];

// =============================================================================
// Config loading cascade — uses scalo flat_env helpers
// =============================================================================

use scalo::config::flat_env::{
    ApplyFlatEnv, Normalize, flat_env_list, flat_env_string, flat_env_string_sensitive,
};

const ENV_PREFIX: &str = "DFE_TRANSFORM";

/// Flat env overrides for K8s-friendly single-underscore env vars.
///
/// `deployment::contract()` declares the SASL names separately for the chart it
/// generates, so a rename here without one there stops the credential arriving.
/// Uses scalo `flat_env_*` helpers for consistent parsing and logging.
impl ApplyFlatEnv for Config {
    fn apply_flat_env(&mut self, prefix: &str) {
        // Pipeline
        if let Some(v) = flat_env_string(prefix, "PIPELINE_NAME") {
            self.pipeline.name = v;
        }
        // Shared Kafka credentials, applied before the per-endpoint names so
        // those override them. One cluster with one SCRAM principal is the
        // default shape, so a single Secret feeds both endpoints.
        if let Some(v) = flat_env_string(prefix, "KAFKA_SASL_USERNAME") {
            self.source.sasl.username.clone_from(&v);
            self.sink.sasl.username = v;
        }
        if let Some(v) = flat_env_string_sensitive(prefix, "KAFKA_SASL_PASSWORD") {
            self.source.sasl.password = SensitiveString::new(v.clone());
            self.sink.sasl.password = SensitiveString::new(v);
        }
        // Source
        if let Some(v) = flat_env_string(prefix, "SOURCE_TRANSPORT") {
            self.source.transport = parse_transport(&v, self.source.transport);
        }
        if let Some(v) = flat_env_string(prefix, "SOURCE_LISTEN") {
            self.source.listen = v;
        }
        if let Some(v) = flat_env_list(prefix, "SOURCE_BROKERS") {
            self.source.brokers = v;
        }
        if let Some(v) = flat_env_list(prefix, "SOURCE_TOPICS") {
            self.source.topics = v;
        }
        if let Some(v) = flat_env_string(prefix, "SOURCE_GROUP_ID") {
            self.source.group_id = v;
        }
        if let Some(v) = flat_env_string(prefix, "SOURCE_SASL_USERNAME") {
            self.source.sasl.username = v;
        }
        if let Some(v) = flat_env_string_sensitive(prefix, "SOURCE_SASL_PASSWORD") {
            self.source.sasl.password = SensitiveString::new(v);
        }
        // Sink
        if let Some(v) = flat_env_string(prefix, "SINK_TRANSPORT") {
            self.sink.transport = parse_transport(&v, self.sink.transport);
        }
        if let Some(v) = flat_env_string(prefix, "SINK_ENDPOINT") {
            self.sink.endpoint = v;
        }
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
            self.sink.sasl.password = SensitiveString::new(v);
        }
        // Transforms
        if let Some(v) = flat_env_string(prefix, "TRANSFORMS_DIR") {
            self.transforms.dir = Some(v);
        }
    }
}

/// Parse a transport name from the environment, keeping `current` on anything
/// unrecognised so a typo cannot silently move a deployment off its transport.
fn parse_transport(value: &str, current: Transport) -> Transport {
    match value.trim().to_ascii_lowercase().as_str() {
        "bus" | "kafka" => Transport::Bus,
        "direct" | "grpc" => Transport::Direct,
        other => {
            tracing::warn!(
                value = other,
                "unknown transport, keeping the configured one"
            );
            current
        }
    }
}

/// Normalise config after all sources merge.
/// Infers implied settings regardless of how values arrived.
impl Normalize for Config {
    fn normalize(&mut self) {
        // Either half of the credential enables SASL -- keying on the username
        // alone leaves a password-only config on scalo's plaintext default.
        if !self.source.sasl.username.is_empty() || !self.source.sasl.password.is_empty() {
            self.source.sasl.enabled = true;
        }
        if !self.sink.sasl.username.is_empty() || !self.sink.sasl.password.is_empty() {
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
            // Explicit `--config <path>` MUST exist. The previous behaviour
            // (silent fallback to defaults) hid mount-path typos and surfaced
            // as misleading "sink.topic must not be empty" validation
            // errors several stages later. See GH issue #9.
            if !Path::new(path).exists() {
                return Err(crate::Error::Config(format!(
                    "config file not found: {path}"
                )));
            }
            let content = std::fs::read_to_string(path)
                .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
            config = serde_yaml_ng::from_str(&content)?;
            debug!(path, "loaded configuration file");
        } else {
            // No --config: lenient fallback search in CWD.
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

        // Figment env (double-underscore nesting). The round-trip serialises the
        // file-sourced config, so without `expose_during` a SensitiveString
        // password is replaced by the redaction constant here.
        config = scalo::expose_during(|| {
            use figment::Figment;
            use figment::providers::{Env, Serialized};

            Figment::from(Serialized::defaults(&config))
                .merge(Env::prefixed(&format!("{ENV_PREFIX}_")).split("__"))
                .extract::<Self>()
                .map_err(|e| crate::Error::Config(e.to_string()))
        })?;

        // Flat env overrides (single-underscore, K8s-friendly)
        config.apply_flat_env(ENV_PREFIX);

        // Normalise (infer implied settings)
        config.normalize();

        // Register in global config registry (enables /config endpoint dump with redaction)
        config.register_sections();

        Ok(config)
    }

    /// The [`INERT_SETTINGS`] this config has actually moved off their defaults.
    ///
    /// A deployment that leaves one alone is not told about it; one that has
    /// turned the dial is.
    #[must_use]
    pub fn inert_settings(&self) -> Vec<InertSetting> {
        let default = Self::default();
        INERT_SETTINGS
            .iter()
            .copied()
            .filter(|setting| match setting.key {
                "pipeline.batch_size" => self.pipeline.batch_size != default.pipeline.batch_size,
                "pipeline.batch_timeout_ms" => {
                    self.pipeline.batch_timeout_ms != default.pipeline.batch_timeout_ms
                }
                "sink.key_field" => !self.sink.key_field.is_empty(),
                "source.commit_interval_ms" => {
                    self.source.commit_interval_ms != default.source.commit_interval_ms
                }
                // An entry with no arm here is unreportable;
                // `every_inert_setting_is_detectable` fails on it at test time.
                _ => false,
            })
            .collect()
    }

    /// Warn once per inert setting a deployment has set.
    pub fn warn_inert_settings(&self) {
        for setting in self.inert_settings() {
            tracing::warn!(
                key = setting.key,
                instead = setting
                    .instead
                    .unwrap_or("nothing -- the capability is absent"),
                "config setting is accepted but not applied: {}",
                setting.why
            );
        }
    }

    /// Register all config sections in the global config registry.
    ///
    /// Enables redacted config dump via `registry::dump_effective()` and
    /// change notifications via `registry::on_change()`. Called after load
    /// and after each hot-reload.
    pub fn register_sections(&self) {
        use scalo::config::registry;
        registry::register("pipeline", &self.pipeline);
        registry::register("source", &self.source);
        registry::register("sink", &self.sink);
        registry::register("transforms", &self.transforms);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// Turn each dial in turn and confirm the setting is reported.
    ///
    /// Walks [`INERT_SETTINGS`] rather than naming the keys, so an entry added
    /// without a matching arm in [`Config::inert_settings`] fails here rather
    /// than going unreported at runtime.
    #[test]
    fn every_inert_setting_is_detectable() {
        for setting in INERT_SETTINGS {
            let mut config = Config::default();
            match setting.key {
                "pipeline.batch_size" => config.pipeline.batch_size += 1,
                "pipeline.batch_timeout_ms" => config.pipeline.batch_timeout_ms += 1,
                "sink.key_field" => config.sink.key_field = ".org_id".to_string(),
                "source.commit_interval_ms" => config.source.commit_interval_ms += 1,
                other => panic!(
                    "INERT_SETTINGS carries `{other}` but this test does not know \
                     how to set it -- add an arm here and in Config::inert_settings"
                ),
            }
            let reported = config.inert_settings();
            assert!(
                reported.contains(setting),
                "setting `{}` was turned but inert_settings() did not report it; \
                 reported: {:?}",
                setting.key,
                reported.iter().map(|s| s.key).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn inert_settings_are_silent_on_an_untouched_config() {
        assert!(
            Config::default().inert_settings().is_empty(),
            "a deployment that has not turned an inert dial must not be warned"
        );
    }

    /// No [`SCALO_CASCADE_SECTIONS`] name may also be a [`Config`] field.
    ///
    /// The two are read by different cascades. A name in both puts the
    /// wrapper's copy in the derived schema while scalo resolves its own, so
    /// the documented field and the effective one drift apart unnoticed.
    #[test]
    fn scalo_sections_do_not_collide_with_config_fields() {
        let config = serde_json::to_value(Config::default()).unwrap();
        let fields = config.as_object().unwrap();
        for (section, _) in SCALO_CASCADE_SECTIONS {
            assert!(
                !fields.contains_key(*section),
                "`{section}` is both a Config field and a scalo cascade section"
            );
        }
    }

    #[test]
    fn default_config_has_sensible_defaults() {
        let config = Config::default();
        assert_eq!(config.pipeline.name, "default");
        assert_eq!(config.pipeline.batch_size, 1000);
        assert_eq!(config.pipeline.batch_timeout_ms, 100);
        assert_eq!(config.sink.compression, "zstd");
        assert_eq!(config.source.transport, Transport::Bus);
        assert_eq!(config.sink.transport, Transport::Bus);
    }

    #[test]
    fn source_acknowledgements_are_on_unless_the_file_turns_them_off() {
        assert!(Config::default().source.acknowledgements.enabled);

        let untouched: Config = serde_yaml_ng::from_str("source:\n  topics: [events]\n").unwrap();
        assert!(
            untouched.source.acknowledgements.enabled,
            "a source section without the key keeps the default"
        );

        let off: Config =
            serde_yaml_ng::from_str("source:\n  acknowledgements:\n    enabled: false\n").unwrap();
        assert!(!off.source.acknowledgements.enabled);
    }

    #[test]
    fn load_from_yaml_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.yaml");

        std::fs::write(
            &path,
            r#"
pipeline:
  name: "test-pipeline"
  batch_size: 5000
source:
  brokers: ["kafka-1:9092", "kafka-2:9092"]
  topics: ["events"]
  group_id: "test-group"
sink:
  brokers: ["kafka-1:9092"]
  topic: "output"
  key_field: ".tenant_id"
"#,
        )
        .unwrap();

        let config = Config::load(Some(path.to_str().unwrap())).unwrap();
        assert_eq!(config.pipeline.name, "test-pipeline");
        assert_eq!(config.pipeline.batch_size, 5000);
        assert_eq!(config.source.brokers, vec!["kafka-1:9092", "kafka-2:9092"]);
        assert_eq!(config.source.topics, vec!["events"]);
        assert_eq!(config.sink.topic, "output");
        assert_eq!(config.sink.key_field, ".tenant_id");
    }

    #[test]
    fn load_missing_explicit_path_returns_file_not_found() {
        // GH issue #9: explicit `--config <path>` MUST fail-fast with a
        // clear "config file not found" error. The previous behaviour
        // (silent fallback to defaults) hid mount-path typos and surfaced
        // as misleading "sink.topic must not be empty" validation errors
        // several stages downstream.
        let err = Config::load(Some("/nonexistent/path.yaml")).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("config file not found"),
            "error must say 'config file not found', got: {msg}"
        );
        assert!(
            msg.contains("/nonexistent/path.yaml"),
            "error must include the actual missing path, got: {msg}"
        );
    }

    #[test]
    fn load_no_path_with_no_cwd_config_returns_defaults() {
        // No --config arg AND no config.yaml/.yml in CWD → defaults.
        // This path stays lenient (only explicit --config requires
        // existence per GH#9).
        let tmp = tempfile::tempdir().unwrap();
        let prev_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();
        // `load` reads the process environment, so it runs under temp_env's
        // lock -- otherwise a flat-env test's override window supplies the
        // name this asserts is a default.
        let config = temp_env::with_var("DFE_TRANSFORM_PIPELINE_NAME", None::<&str>, || {
            Config::load(None)
        });
        std::env::set_current_dir(prev_cwd).unwrap();
        assert_eq!(config.unwrap().pipeline.name, "default");
    }

    #[test]
    fn normalize_enables_sasl_when_username_set() {
        let mut config = Config::default();
        assert!(!config.source.sasl.enabled);

        config.source.sasl.username = "alice".to_string();
        config.normalize();

        assert!(config.source.sasl.enabled);
    }

    #[test]
    fn normalize_enables_tls_when_ca_cert_set() {
        let mut config = Config::default();
        assert!(!config.source.tls.enabled);

        config.source.tls.ca_cert_file = Some("/etc/ssl/ca.crt".to_string());
        config.normalize();

        assert!(config.source.tls.enabled);
    }

    #[test]
    fn normalize_does_not_enable_sasl_without_username() {
        let mut config = Config::default();
        config.normalize();
        assert!(!config.source.sasl.enabled);
        assert!(!config.sink.sasl.enabled);
    }

    #[test]
    fn serde_roundtrip() {
        let config = Config::default();
        let yaml = serde_yaml_ng::to_string(&config).unwrap();
        let deserialized: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(config.pipeline.name, deserialized.pipeline.name);
        assert_eq!(config.pipeline.batch_size, deserialized.pipeline.batch_size);
        assert_eq!(config.source.group_id, deserialized.source.group_id);
    }

    #[test]
    fn a_source_from_an_older_render_still_loads() {
        // An engine render from before JSON-only still carries source.format.
        let yaml = "source:\n  format: auto\n  group_id: cg\n";
        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("the removed key is ignored, not refused");
        assert_eq!(config.source.group_id, "cg");
    }

    #[test]
    fn sasl_config_defaults() {
        let sasl = SaslConfig::default();
        assert!(!sasl.enabled);
        assert_eq!(sasl.mechanism, "scram_sha_512");
        assert!(sasl.username.is_empty());
        assert!(sasl.password.is_empty());
    }

    #[test]
    fn tls_config_defaults() {
        let tls = TlsConfig::default();
        assert!(!tls.enabled);
        assert!(tls.ca_cert_file.is_none());
    }

    #[test]
    fn enrichment_table_config_empty_by_default() {
        let config = Config::default();
        assert!(config.enrichment_tables.is_empty());
    }

    #[test]
    fn enrichment_config_legacy_flat_format() {
        let yaml = r#"
enrichment_tables:
  - name: "services"
    path: "/data/services.csv"
    key_columns: ["service_id"]
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(config.enrichment_tables.len(), 1);
        let table = &config.enrichment_tables[0];
        assert_eq!(table.name, "services");
        assert_eq!(table.path, "/data/services.csv");
        let source = table.resolved_source().unwrap();
        assert!(matches!(source, EnrichmentSourceConfig::File { .. }));
    }

    #[test]
    fn enrichment_config_schema_column_types() {
        let yaml = r#"
enrichment_tables:
  - name: "services"
    path: "/data/services.csv"
    key_columns: ["service_id"]
    schema:
      status_code: "integer"
      commissioned: "date|%d/%m/%Y"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let table = &config.enrichment_tables[0];
        assert_eq!(
            table.schema.get("status_code").map(String::as_str),
            Some("integer")
        );
        assert_eq!(
            table.schema.get("commissioned").map(String::as_str),
            Some("date|%d/%m/%Y")
        );
    }

    #[test]
    fn enrichment_config_schema_absent_is_empty() {
        let yaml = r#"
enrichment_tables:
  - name: "services"
    path: "/data/services.csv"
    key_columns: ["service_id"]
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(config.enrichment_tables[0].schema.is_empty());
    }

    #[test]
    fn enrichment_config_new_tagged_mmdb() {
        let yaml = r#"
enrichment_tables:
  - name: "geoip"
    source:
      type: mmdb
      path: "/data/GeoLite2-City.mmdb"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let source = config.enrichment_tables[0].resolved_source().unwrap();
        assert!(matches!(source, EnrichmentSourceConfig::Mmdb { .. }));
    }

    #[test]
    fn enrichment_config_stix_with_url() {
        let yaml = r#"
enrichment_tables:
  - name: "threats"
    source:
      type: stix
      url: "https://taxii.example.com/objects"
    key_columns: ["indicator"]
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let source = config.enrichment_tables[0].resolved_source().unwrap();
        let EnrichmentSourceConfig::Stix { url, .. } = source else {
            unreachable!("expected Stix");
        };
        assert_eq!(url.as_deref(), Some("https://taxii.example.com/objects"));
    }

    #[test]
    fn enrichment_config_stix_with_file() {
        let yaml = r#"
enrichment_tables:
  - name: "threats"
    source:
      type: stix
      path: "/data/stix-bundle.json"
    key_columns: ["indicator"]
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let source = config.enrichment_tables[0].resolved_source().unwrap();
        let EnrichmentSourceConfig::Stix { path, .. } = source else {
            unreachable!("expected Stix");
        };
        assert_eq!(path.as_deref(), Some("/data/stix-bundle.json"));
    }

    #[test]
    fn enrichment_config_sqlite() {
        let yaml = r#"
enrichment_tables:
  - name: "subscribers"
    source:
      type: sqlite
      path: "/data/subs.db"
      query: "SELECT msisdn, plan FROM subscribers"
    key_columns: ["msisdn"]
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let source = config.enrichment_tables[0].resolved_source().unwrap();
        assert!(matches!(source, EnrichmentSourceConfig::Sqlite { .. }));
    }

    #[test]
    fn enrichment_config_resolved_source_error_when_empty() {
        let config = EnrichmentTableConfig::default();
        assert!(config.resolved_source().is_err());
    }

    #[test]
    fn enrichment_config_with_refresh() {
        let yaml = r#"
enrichment_tables:
  - name: "threats"
    source:
      type: stix
      url: "https://example.com/stix"
    key_columns: ["indicator"]
    refresh:
      interval_secs: 3600
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let refresh = config.enrichment_tables[0].refresh.as_ref().unwrap();
        assert_eq!(refresh.interval_secs, 3600);
    }

    // ---------------------------------------------------------------------
    // Flat env overrides
    //
    // These drive `apply_flat_env` on a `Config::default()` directly, never
    // `Config::load()` -- load touches dotenvy, the CWD and the global config
    // registry, so it is order-dependent under a parallel harness.
    // ---------------------------------------------------------------------

    /// Every name `apply_flat_env` reads must land on its field. A name that is
    /// written but never read is the defect class this closes, so the shared
    /// SASL pair gets its own pass -- the per-endpoint names shadow it.
    #[test]
    fn flat_env_applies_every_supported_name() {
        let vars: Vec<(&str, Option<&str>)> = vec![
            ("DFE_TRANSFORM_PIPELINE_NAME", Some("edge")),
            ("DFE_TRANSFORM_SOURCE_BROKERS", Some("b1:9092,b2:9092")),
            ("DFE_TRANSFORM_SOURCE_TOPICS", Some("t1,t2")),
            ("DFE_TRANSFORM_SOURCE_GROUP_ID", Some("cg")),
            ("DFE_TRANSFORM_SOURCE_SASL_USERNAME", Some("src-user")),
            ("DFE_TRANSFORM_SOURCE_SASL_PASSWORD", Some("src-pass")),
            ("DFE_TRANSFORM_SINK_BROKERS", Some("b3:9092")),
            ("DFE_TRANSFORM_SINK_TOPIC", Some("out")),
            ("DFE_TRANSFORM_SINK_KEY_FIELD", Some(".tenant_id")),
            ("DFE_TRANSFORM_SINK_COMPRESSION", Some("lz4")),
            ("DFE_TRANSFORM_SINK_SASL_USERNAME", Some("sink-user")),
            ("DFE_TRANSFORM_SINK_SASL_PASSWORD", Some("sink-pass")),
            ("DFE_TRANSFORM_TRANSFORMS_DIR", Some("/etc/vrl")),
        ];

        let config = temp_env::with_vars(vars, || {
            let mut config = Config::default();
            config.apply_flat_env(ENV_PREFIX);
            config
        });

        assert_eq!(config.pipeline.name, "edge");
        assert_eq!(config.source.brokers, vec!["b1:9092", "b2:9092"]);
        assert_eq!(config.source.topics, vec!["t1", "t2"]);
        assert_eq!(config.source.group_id, "cg");
        assert_eq!(config.source.sasl.username, "src-user");
        assert_eq!(config.source.sasl.password.expose(), "src-pass");
        assert_eq!(config.sink.brokers, vec!["b3:9092"]);
        assert_eq!(config.sink.topic, "out");
        assert_eq!(config.sink.key_field, ".tenant_id");
        assert_eq!(config.sink.compression, "lz4");
        assert_eq!(config.sink.sasl.username, "sink-user");
        assert_eq!(config.sink.sasl.password.expose(), "sink-pass");
        assert_eq!(config.transforms.dir.as_deref(), Some("/etc/vrl"));

        let shared = temp_env::with_vars(
            [
                ("DFE_TRANSFORM_KAFKA_SASL_USERNAME", Some("shared-user")),
                ("DFE_TRANSFORM_KAFKA_SASL_PASSWORD", Some("shared-pass")),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                config
            },
        );

        assert_eq!(shared.source.sasl.username, "shared-user");
        assert_eq!(shared.source.sasl.password.expose(), "shared-pass");
    }

    /// One cluster with one SCRAM principal is the default shape, so the shared
    /// pair the chart injects must reach both endpoints from a single Secret.
    #[test]
    fn flat_env_kafka_sasl_fans_out_to_source_and_sink() {
        let config = temp_env::with_vars(
            [
                ("DFE_TRANSFORM_KAFKA_SASL_USERNAME", Some("scram-user")),
                ("DFE_TRANSFORM_KAFKA_SASL_PASSWORD", Some("scram-pass")),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                config
            },
        );

        assert_eq!(config.source.sasl.username, "scram-user");
        assert_eq!(config.source.sasl.password.expose(), "scram-pass");
        assert_eq!(config.sink.sasl.username, "scram-user");
        assert_eq!(config.sink.sasl.password.expose(), "scram-pass");
    }

    /// Two clusters: the endpoint-specific name overrides the shared one, and
    /// the endpoint left unset still gets the shared credential. Both
    /// directions, because the shared block only sits above BOTH per-endpoint
    /// blocks -- checking one direction lets it slide between them unnoticed.
    #[test]
    fn flat_env_endpoint_sasl_overrides_shared() {
        let sink_specific = temp_env::with_vars(
            [
                ("DFE_TRANSFORM_KAFKA_SASL_USERNAME", Some("shared-user")),
                ("DFE_TRANSFORM_KAFKA_SASL_PASSWORD", Some("shared-pass")),
                ("DFE_TRANSFORM_SINK_SASL_USERNAME", Some("sink-user")),
                ("DFE_TRANSFORM_SINK_SASL_PASSWORD", Some("sink-pass")),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                config
            },
        );

        assert_eq!(sink_specific.source.sasl.username, "shared-user");
        assert_eq!(sink_specific.source.sasl.password.expose(), "shared-pass");
        assert_eq!(sink_specific.sink.sasl.username, "sink-user");
        assert_eq!(sink_specific.sink.sasl.password.expose(), "sink-pass");

        let source_specific = temp_env::with_vars(
            [
                ("DFE_TRANSFORM_KAFKA_SASL_USERNAME", Some("shared-user")),
                ("DFE_TRANSFORM_KAFKA_SASL_PASSWORD", Some("shared-pass")),
                ("DFE_TRANSFORM_SOURCE_SASL_USERNAME", Some("src-user")),
                ("DFE_TRANSFORM_SOURCE_SASL_PASSWORD", Some("src-pass")),
            ],
            || {
                let mut config = Config::default();
                config.apply_flat_env(ENV_PREFIX);
                config
            },
        );

        assert_eq!(source_specific.source.sasl.username, "src-user");
        assert_eq!(source_specific.source.sasl.password.expose(), "src-pass");
        assert_eq!(source_specific.sink.sasl.username, "shared-user");
        assert_eq!(source_specific.sink.sasl.password.expose(), "shared-pass");
    }

    /// The config registry serialises the whole section into a process-global
    /// and redacts on the way out by field name; the type must redact instead.
    #[test]
    fn a_serialised_sasl_section_never_carries_the_password() {
        let sasl = SaslConfig {
            enabled: true,
            username: "scram-user".to_string(),
            password: SensitiveString::new("hunter2"),
            ..SaslConfig::default()
        };

        let rendered = serde_json::to_string(&sasl).unwrap();

        assert!(
            !rendered.contains("hunter2"),
            "the password reached a serialised output: {rendered}"
        );
        assert!(
            format!("{sasl:?}").contains("***REDACTED***"),
            "Debug must redact the password, got: {sasl:?}"
        );
    }

    /// The figment round-trip serialises the file-sourced config, so without
    /// `expose_during` the password arrives at the broker as the constant.
    #[test]
    fn a_password_from_the_config_file_survives_the_figment_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("with-password.yaml");

        std::fs::write(
            &path,
            r"
source:
  sasl:
    enabled: true
    username: scram-user
    password: from-the-file
",
        )
        .unwrap();

        let config = Config::load(Some(path.to_str().unwrap())).unwrap();

        assert_eq!(config.source.sasl.password.expose(), "from-the-file");
    }

    /// The engine serves this schema to the console, where a marked field
    /// renders as set-or-not and an unmarked one renders its value.
    #[test]
    fn the_emitted_schema_marks_the_password_and_gives_it_no_default() {
        let schema = serde_json::to_value(schemars::schema_for!(Config)).unwrap();
        let password = &schema["$defs"]["SaslConfig"]["properties"]["password"];

        assert_eq!(password["x-dfe-secret"], true, "schema was: {schema}");
        assert_eq!(password["writeOnly"], true);
        assert!(
            password.get("default").is_none(),
            "a secret's default is absent, not a placeholder: {password}"
        );
        assert!(
            !schema.to_string().contains("REDACTED"),
            "the redaction constant must not reach the emitted schema"
        );
    }

    /// A password with no username must not leave the transport on scalo's
    /// plaintext default while the operator believes a credential was supplied.
    #[test]
    fn normalize_enables_sasl_on_a_password_with_no_username() {
        let mut config = Config::default();
        config.source.sasl.password = SensitiveString::new("scram-pass");
        config.sink.sasl.password = SensitiveString::new("scram-pass");

        config.normalize();

        assert!(config.source.sasl.enabled);
        assert!(config.sink.sasl.enabled);
    }
}
