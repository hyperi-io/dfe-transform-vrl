// Project:   dfe-transform-vrl
// File:      tests/smoke.rs
// Purpose:   Startup smoke tests to catch init panics, missing deps, bad feature combos
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Smoke tests that exercise the compiled binary.
//!
//! These catch:
//! - Init panics from static initialisers or bad feature combos
//! - Missing native library dependencies (librdkafka, libssl)
//! - Broken CLI argument parsing
//! - Broken emit subcommands (Dockerfile, Helm, Compose, Contract)

use std::process::Command;

fn binary_path() -> String {
    // cargo test sets this for integration tests
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    format!("{target_dir}/debug/dfe-transform-vrl")
}

#[test]
fn binary_help_exits_zero() {
    let output = Command::new(binary_path())
        .arg("--help")
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "--help should exit 0, got {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("dfe-transform-vrl"),
        "help output should contain binary name"
    );
}

#[test]
fn binary_version_exits_zero() {
    let output = Command::new(binary_path())
        .arg("--version")
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "--version should exit 0, got {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn config_check_without_config_fails() {
    let output = Command::new(binary_path())
        .arg("config-check")
        .output()
        .expect("failed to execute binary");

    // Should fail because no config file exists at the default path
    assert!(
        !output.status.success(),
        "config-check without config should fail"
    );
}

#[test]
fn config_check_with_valid_config_exits_zero() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let config_path = dir.path().join("config.yaml");

    // Write a minimal valid config
    std::fs::write(
        &config_path,
        r#"
pipeline:
  name: smoke-test
source:
  brokers: ["localhost:9092"]
  topics: ["test"]
  group_id: "smoke-test"
transforms:
  files:
    - "/dev/null"
sink:
  brokers: ["localhost:9092"]
  topic: "out"
"#,
    )
    .expect("write config");

    // Also need a transform file — /dev/null is empty, VRL accepts empty
    let output = Command::new(binary_path())
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .arg("config-check")
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "config-check with valid config should exit 0\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn emit_dockerfile_outputs_from() {
    let output = Command::new(binary_path())
        .arg("emit-dockerfile")
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "emit-dockerfile should exit 0\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("FROM"),
        "Dockerfile should contain FROM directive"
    );
}

#[test]
fn emit_compose_outputs_yaml() {
    let output = Command::new(binary_path())
        .arg("emit-compose")
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "emit-compose should exit 0\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("image:") || stdout.contains("services:"),
        "compose output should contain Docker Compose structure"
    );
}

#[test]
fn emit_contract_outputs_json() {
    let output = Command::new(binary_path())
        .arg("emit-contract")
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "emit-contract should exit 0\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Contract is JSON — should parse
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(&stdout);
    assert!(
        parsed.is_ok(),
        "emit-contract should produce valid JSON: {}",
        stdout
    );
}

#[test]
fn emit_chart_creates_directory() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let chart_dir = dir.path().join("chart-output");

    let output = Command::new(binary_path())
        .arg("emit-chart")
        .arg(chart_dir.to_str().unwrap())
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "emit-chart should exit 0\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Chart directory should contain Chart.yaml
    assert!(
        chart_dir.join("Chart.yaml").exists(),
        "emit-chart should create Chart.yaml in output dir"
    );
}
