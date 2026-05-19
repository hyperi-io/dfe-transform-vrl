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

/// GH issue #11 regression: the wrapper used to construct a SECOND
/// `MetricsManager` that tried to bind the same `:9090` as rustlib's
/// auto-started one, causing every startup to fail with EADDRINUSE.
/// Spawn the actual binary, give it time to clear the metrics-server
/// bind path, and assert it doesn't die with that error before we
/// terminate it. Uses minimal.yaml whose `transforms.dir` doesn't
/// exist on disk; the wrapper crashes downstream on Kafka connect or
/// transforms-load — but the metrics server step happens *before*
/// either, so EADDRINUSE would surface here.
#[test]
fn service_startup_does_not_crash_with_eaddrinuse() {
    use std::io::Read;
    use std::time::{Duration, Instant};

    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/configs/minimal.yaml");
    assert!(fixture.exists(), "fixture missing");

    // Bind to random unused ports so we don't conflict with anything on
    // the host running the test suite (the rustlib metrics server uses
    // METRICS_ADDR; we set both health/metrics via env to ephemeral high
    // ports).
    let mut child = std::process::Command::new(binary_path())
        .arg("--config")
        .arg(&fixture)
        .arg("run")
        .env("METRICS_ADDR", "127.0.0.1:0")
        .env("DFE_TRANSFORM_HEALTH__ADDRESS", "127.0.0.1:0")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn binary");

    // Give the wrapper ~2s to run all startup steps (health bind, metrics
    // bind, VRL compile, kafka init begin). EADDRINUSE shows up well
    // before then.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Ok(Some(_)) = child.try_wait() {
            // Process exited before 2s — collect stderr and assert it
            // wasn't EADDRINUSE.
            let mut stderr = String::new();
            if let Some(mut s) = child.stderr.take() {
                let _ = s.read_to_string(&mut stderr);
            }
            assert!(
                !stderr.contains("Address already in use"),
                "GH#11 regressed: wrapper crashed with EADDRINUSE on startup.\nstderr:\n{stderr}"
            );
            // Any other early exit is fine for this test's scope — minimal.yaml
            // fails on transforms-load (`/etc/dfe-transform-vrl/transforms`
            // doesn't exist) which we accept.
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Still running after 2s — startup succeeded past the metrics bind.
    // Terminate cleanly.
    let _ = child.kill();
    let _ = child.wait();
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
