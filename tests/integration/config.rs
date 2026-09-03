// Project:   dfe-transform-vrl
// File:      tests/integration/config.rs
// Purpose:   Integration tests — config loading and cascade
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for configuration loading, YAML fixtures, and validation.

use dfe_transform_vrl::config::Config;

fn fixture_path(name: &str) -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    format!("{manifest}/tests/fixtures/configs/{name}")
}

#[test]
fn test_load_minimal_config() {
    let config = Config::load(Some(&fixture_path("minimal.yaml"))).unwrap();
    assert_eq!(config.pipeline.name, "test-pipeline");
    assert_eq!(config.pipeline.batch_size, 100);
    assert_eq!(config.pipeline.batch_timeout_ms, 50);
    assert_eq!(config.source.brokers, vec!["localhost:9092"]);
    assert_eq!(config.source.topics, vec!["test-input"]);
    assert_eq!(config.source.group_id, "test-group");
    assert_eq!(config.source.format, "auto");
    assert_eq!(config.sink.topic, "test-output");
}

#[test]
fn test_load_sasl_config() {
    let config = Config::load(Some(&fixture_path("with_sasl.yaml"))).unwrap();
    assert_eq!(config.pipeline.name, "prod-pipeline");
    assert_eq!(config.pipeline.batch_size, 5000);
    assert!(config.source.sasl.enabled);
    assert_eq!(config.source.sasl.mechanism, "scram_sha_512");
    assert!(config.source.tls.enabled);
    assert!(config.sink.sasl.enabled);
    assert!(config.sink.tls.enabled);
    assert_eq!(config.sink.compression, "zstd");
    assert_eq!(config.sink.key_field, ".org_id");
}

#[test]
fn test_minimal_config_validates() {
    let config = Config::load(Some(&fixture_path("minimal.yaml"))).unwrap();
    assert!(config.validate().is_ok());
}

#[test]
fn test_sasl_config_validates() {
    let config = Config::load(Some(&fixture_path("with_sasl.yaml"))).unwrap();
    assert!(config.validate().is_ok());
}

#[test]
fn test_default_config_fields() {
    let config = Config::load(Some(&fixture_path("minimal.yaml"))).unwrap();
    assert_eq!(config.health.address, "0.0.0.0:9000");
    assert!(!config.source.sasl.enabled);
    assert!(!config.source.tls.enabled);
}

#[test]
fn test_nonexistent_explicit_config_returns_file_not_found() {
    // GH issue #9: explicit `--config <path>` MUST fail-fast with a
    // clear "config file not found" error. The previous behaviour
    // (silent fallback to defaults) hid mount-path typos and surfaced
    // as misleading "sink.topic must not be empty" validation errors
    // several stages later.
    let err = Config::load(Some("/nonexistent/config.yaml")).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("config file not found"),
        "expected 'config file not found', got: {msg}"
    );
    assert!(
        msg.contains("/nonexistent/config.yaml"),
        "error must include the actual missing path, got: {msg}"
    );
}

#[test]
fn test_env_override_pipeline_name() {
    let config = Config::load(Some(&fixture_path("minimal.yaml"))).unwrap();
    assert_eq!(config.pipeline.name, "test-pipeline");
}
