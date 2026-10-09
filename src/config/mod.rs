// Project:   dfe-transform-vrl
// File:      src/config/mod.rs
// Purpose:   Configuration module — loading and validation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration loading and validation.
//!
//! Configuration cascade (highest to lowest priority):
//!   1. CLI args (--config, --log-level, etc.)
//!   2. Environment variables (`DFE_TRANSFORM_*`)
//!   3. `./.env` in the working directory, never a parent's
//!   4. Config file specified by `--config`
//!   5. Hard-coded defaults
//!
//! That cascade covers [`Config`] only. scalo resolves its own sections
//! ([`SCALO_CASCADE_SECTIONS`]) from a second cascade, which [`crate::cli`]
//! seeds at startup with the `--config` file as its settings layer, so both
//! read the same file.

pub mod hot;
pub mod loader;
pub mod validate;

pub use hot::HotConfig;
pub use loader::{
    Config, EnrichmentSourceConfig, EnrichmentTableConfig, FileFormat, INERT_SETTINGS,
    InertSetting, PipelineConfig, RefreshConfig, SCALO_CASCADE_SECTIONS, SaslConfig, SinkConfig,
    SourceConfig, StixAuthConfig, TlsConfig, TransformConfig, Transport,
    warn_unreachable_scalo_settings, working_dir_config_file,
};
