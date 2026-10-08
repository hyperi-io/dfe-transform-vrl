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
    assert_eq!(config.sink.topic, "test-output");
}

#[test]
fn test_load_sasl_config() {
    let config = Config::load(Some(&fixture_path("with_sasl.yaml"))).unwrap();
    assert_eq!(config.pipeline.name, "prod-pipeline");
    assert_eq!(config.pipeline.batch_size, 5000);
    assert!(config.source.sasl.enabled);
    assert_eq!(config.source.sasl.mechanism, "scram_sha_512");
    // The file-sourced password survives the figment round-trip rather than
    // arriving as the redaction constant.
    assert_eq!(
        config.source.sasl.password.expose(),
        "placeholder-not-a-secret"
    );
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

/// `config-check` run by the binary from `dir`, returning what it printed.
fn config_check_in(dir: &std::path::Path) -> String {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vrl"))
        .arg("config-check")
        .current_dir(dir)
        .env_remove("DFE_TRANSFORM_PIPELINE_NAME")
        .env_remove("DFE_TRANSFORM_SINK_TOPIC")
        .output()
        .expect("the binary runs");
    let printed = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "config-check failed:\n{printed}");
    printed
}

/// The binary reads the `.env` in its working directory and no other.
///
/// A `.env` in a parent directory belongs to whatever project sits above, so a
/// search up the tree loads another project's settings and credentials.
#[test]
fn a_dotenv_in_a_parent_directory_is_not_loaded() {
    let root = tempfile::TempDir::new().expect("tempdir");
    let project = root.path().join("project");
    std::fs::create_dir(&project).expect("project dir");
    std::fs::write(
        project.join("config.yaml"),
        "sink:\n  topic: from_the_file\ntransforms:\n  dir: /etc/dfe-transform-vrl/transforms\n",
    )
    .expect("project config");
    std::fs::write(
        root.path().join(".env"),
        "DFE_TRANSFORM_PIPELINE_NAME=from_parent_dotenv\n",
    )
    .expect("parent .env");

    let printed = config_check_in(&project);
    assert!(
        !printed.contains("from_parent_dotenv"),
        "a .env in the parent directory reached the config"
    );

    // The project's own .env still loads.
    std::fs::write(
        project.join(".env"),
        "DFE_TRANSFORM_SINK_TOPIC=from_project_dotenv\n",
    )
    .expect("project .env");
    let printed = config_check_in(&project);
    assert!(
        printed.contains("from_project_dotenv"),
        "the project's own .env did not reach the config"
    );
    assert!(
        !printed.contains("from_parent_dotenv"),
        "a .env in the parent directory reached the config"
    );
}

/// Every memory-guard variable `.env.example` documents is one the runtime
/// reads, under the prefix the runtime reads it with.
#[test]
fn env_example_memory_guard_vars_reach_the_guard() {
    use clap::Parser;
    use scalo::cli::ServiceApp;
    use scalo::memory::MemoryGuardConfig;

    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let example = std::fs::read_to_string(format!("{manifest}/.env.example")).unwrap();
    let documented: Vec<String> = example
        .lines()
        .filter_map(|line| line.trim_start_matches('#').trim().split_once('='))
        .map(|(name, _)| name.to_string())
        .filter(|name| name.contains("_MEMORY_"))
        .collect();
    assert_eq!(
        documented.len(),
        3,
        "the example documents the three memory-guard settings: {documented:?}"
    );

    // The ServiceRuntime builds its guard with `MemoryGuardConfig::from_env(env_prefix)`.
    let prefix = dfe_transform_vrl::cli::App::parse_from(["dfe-transform-vrl"])
        .env_prefix()
        .to_string();

    for name in documented {
        let (sentinel, read): (&str, fn(&MemoryGuardConfig) -> String) =
            match name.rsplit_once("_MEMORY_").map(|(_, setting)| setting) {
                Some("LIMIT_BYTES") => ("123456789", |c| c.limit_bytes.to_string()),
                Some("PRESSURE_THRESHOLD") => ("0.5", |c| c.pressure_threshold.to_string()),
                Some("CGROUP_HEADROOM") => ("0.5", |c| c.cgroup_headroom.to_string()),
                other => panic!("{name}: the memory guard has no setting {other:?}"),
            };
        let config = temp_env::with_var(&name, Some(sentinel), || {
            MemoryGuardConfig::from_env(&prefix)
        });
        assert_eq!(
            read(&config),
            sentinel,
            "{name} is documented in .env.example but the memory guard never reads it"
        );
    }
}
