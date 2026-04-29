// Project:   dfe-transform-vrl
// File:      tests/smoke.rs
// Purpose:   Startup smoke tests to catch init panics, missing deps, bad feature combos
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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
fn config_check_with_checked_in_fixture_exits_zero() {
    // Use the checked-in minimal fixture so this test doubles as drift
    // detection: if the loader schema changes and breaks minimal.yaml,
    // both this smoke test AND every other test using the fixture fail
    // together — one consistent signal instead of inline-drift.
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/configs/minimal.yaml");

    assert!(
        fixture.exists(),
        "checked-in fixture missing: {}",
        fixture.display()
    );

    let output = Command::new(binary_path())
        .arg("--config")
        .arg(fixture.to_str().expect("fixture path utf8"))
        .arg("config-check")
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "config-check on tests/fixtures/configs/minimal.yaml should exit 0\n\
         stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn example_config_yaml_parses_as_yaml() {
    // The example config users will copy MUST at minimum be valid YAML.
    // Most of it is intentionally commented out (users uncomment + fill in),
    // so we can't run config-check against it — but a YAML syntax check
    // catches the most likely regression: someone breaks the example while
    // editing it.
    let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.yaml");

    let body = std::fs::read_to_string(&example)
        .unwrap_or_else(|e| panic!("read {}: {e}", example.display()));

    serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&body)
        .unwrap_or_else(|e| panic!("config.example.yaml is not valid YAML: {e}"));
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
        "emit-contract should produce valid JSON: {stdout}",
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
