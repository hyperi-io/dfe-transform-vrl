// Project:   dfe-transform-vrl
// File:      tests/smoke.rs
// Purpose:   Startup smoke tests to catch init panics, missing deps, bad feature combos
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Smoke tests that exercise the compiled binary.
//!
//! These catch:
//! - Init panics from static initialisers or bad feature combos
//! - Missing native library dependencies (librdkafka, libssl)
//! - Broken CLI argument parsing
//! - Broken emit subcommands (Dockerfile, Helm, Compose, Contract)

// Shared with the integration and e2e binaries, so every spawn of the binary
// has its telemetry switched off by the one definition.
#[path = "common/offline.rs"]
mod offline;

use offline::binary;

#[test]
fn binary_help_exits_zero() {
    let output = binary()
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
    let output = binary()
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
    let output = binary()
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

    let output = binary()
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

/// `config-check` prints the settings it will actually run with. Return the
/// value it reports for `key`, from the report -- NOT from the config dump
/// below it, which prints what was parsed rather than what took effect.
fn reported_setting(stderr: &str, key: &str) -> String {
    stderr
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(key))
        .map_or_else(
            || panic!("config-check reported no `{key}` line:\n{stderr}"),
            |rest| rest.trim().to_string(),
        )
}

/// The scalo-owned settings must reach the runtime through the env layer.
///
/// Walks `SCALO_CASCADE_SECTIONS`' two reportable members end to end, against
/// the real binary, with values that differ from every default -- so it fails
/// if the app stops seeding scalo's cascade, which is the state that left the
/// metrics address, the log level and the log format unsettable by any means.
#[test]
fn scalo_cascade_env_reaches_the_runtime() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/configs/minimal.yaml");

    let output = binary()
        .arg("--config")
        .arg(fixture.to_str().expect("fixture path utf8"))
        .arg("config-check")
        .env("DFE_TRANSFORM_LOGGER__LEVEL", "warn")
        .env("DFE_TRANSFORM_LOGGER__FORMAT", "json")
        .env("DFE_TRANSFORM_METRICS__ADDRESS", "127.0.0.1:19099")
        // Cleared so the assertions cannot pass on a higher-priority source.
        .env_remove("LOG_LEVEL")
        .env_remove("LOG_FORMAT")
        .env_remove("METRICS_ADDR")
        .output()
        .expect("failed to execute binary");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "config-check should exit 0\nstderr: {stderr}"
    );

    // Each value differs from the default it would fall back to -- info, the
    // otel-derived text, and 0.0.0.0:9090.
    assert_eq!(reported_setting(&stderr, "log_level"), "warn");
    assert_eq!(reported_setting(&stderr, "log_format"), "json");
    assert_eq!(reported_setting(&stderr, "metrics_addr"), "127.0.0.1:19099");
}

/// scalo's own sections take effect from the `--config` file.
///
/// scalo reads that file as its settings layer, so `logger` and `metrics`
/// written there reach the runtime with no env var set. It runs in an empty
/// directory, so no `.env` or `settings.yaml` can supply the values instead.
#[test]
fn scalo_sections_in_the_config_file_reach_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/configs/minimal.yaml");
    let config = dir.path().join("config.yaml");
    std::fs::write(
        &config,
        format!(
            "{}\nlogger:\n  level: warn\nmetrics:\n  address: \"127.0.0.1:19098\"\n",
            std::fs::read_to_string(&fixture).unwrap()
        ),
    )
    .unwrap();

    let output = binary()
        .current_dir(dir.path())
        .arg("--config")
        .arg(config.to_str().expect("config path utf8"))
        .arg("config-check")
        // Cleared so the assertions cannot pass on a higher-priority source.
        .env_remove("LOG_LEVEL")
        .env_remove("METRICS_ADDR")
        .env_remove("DFE_TRANSFORM_LOGGER__LEVEL")
        .env_remove("DFE_TRANSFORM_METRICS__ADDRESS")
        .output()
        .expect("failed to execute binary");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "config-check should exit 0\nstderr: {stderr}"
    );

    // Each value differs from the default it would fall back to -- info and
    // 0.0.0.0:9090.
    assert_eq!(reported_setting(&stderr, "log_level"), "warn");
    assert_eq!(reported_setting(&stderr, "metrics_addr"), "127.0.0.1:19098");
}

/// Write a config carrying a `scaling:` section into `dir`, with a transform
/// program on disk, and return its path.
fn write_scaling_probe(dir: &std::path::Path) -> std::path::PathBuf {
    // A program on disk takes the run past scalo's idle gate, which sits
    // before the startup warnings (scalo-rs #69); the broker is never there.
    let transforms = dir.join("transforms");
    std::fs::create_dir(&transforms).unwrap();
    std::fs::write(transforms.join("100_probe.vrl"), ".marked = true\n").unwrap();
    let config = dir.join("config.yaml");
    std::fs::write(
        &config,
        format!(
            "pipeline:\n  name: warn-probe\n\
             source:\n  brokers: [\"127.0.0.1:9092\"]\n  topics: [\"in\"]\n  group_id: \"g\"\n\
             sink:\n  brokers: [\"127.0.0.1:9092\"]\n  topic: \"out\"\n\
             transforms:\n  dir: \"{}\"\n\
             scaling:\n  enabled: false\n",
            transforms.display()
        ),
    )
    .unwrap();
    config
}

/// Start `run` from `dir` with `args`, and read its stderr until a line
/// satisfies `stop`, the process exits, or 30 s pass.
///
/// Returns whether `stop` matched, and every line read, including those that
/// arrive in the 200 ms after a match. The run never exits on its own, so it is
/// killed either way.
fn run_until(dir: &std::path::Path, args: &[&str], stop: impl Fn(&str) -> bool) -> (bool, String) {
    use std::io::{BufRead, BufReader};
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::{Duration, Instant};

    let mut child = binary()
        .current_dir(dir)
        .args(args)
        .arg("run")
        .env("METRICS_ADDR", "127.0.0.1:0")
        // The topology line is logged at info, which a stricter level would hide.
        .env("LOG_LEVEL", "info")
        .env_remove("RUST_LOG")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn binary");
    let stderr = child.stderr.take().expect("stderr is piped");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = String::new();
    let mut matched = false;
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(line) => {
                matched = stop(&line);
                seen.push_str(&line);
                seen.push('\n');
                if matched {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    break;
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    // A task racing the one that logged the matching line writes its own a few
    // milliseconds either side, so the lines in flight are read before the kill.
    if matched {
        let settled = Instant::now() + Duration::from_millis(200);
        while let Ok(line) = rx.recv_timeout(settled.saturating_duration_since(Instant::now())) {
            seen.push_str(&line);
            seen.push('\n');
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    (matched, seen)
}

/// The startup warning for a scalo section the wrapper's file cannot deliver.
fn warns_for_scaling(line: &str) -> bool {
    line.contains("belongs to the scalo cascade") && line.contains("scaling")
}

/// The same warning for the `otel_tracing` section.
fn warns_for_otel_tracing(line: &str) -> bool {
    line.contains("belongs to the scalo cascade") && line.contains("otel_tracing")
}

/// A scalo section in the `--config` file is applied, so it draws no warning.
#[test]
fn scalo_section_in_the_config_file_does_not_warn() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_scaling_probe(dir.path());

    // The warnings are logged before the topology line, in the same function.
    let (reached, seen) = run_until(
        dir.path(),
        &["--config", config.to_str().unwrap()],
        |line| line.contains("DFE topology"),
    );

    assert!(reached, "the run never logged its topology\nstderr: {seen}");
    assert!(
        !seen.lines().any(warns_for_scaling),
        "a `scaling:` section in the --config file reaches scalo and must not \
         warn\nstderr: {seen}"
    );
}

/// `otel_tracing`, the span-export switch, is a scalo section like `scaling`:
/// in a working-directory `config.yaml` it is reported, not obeyed.
#[test]
fn otel_tracing_section_in_a_working_directory_config_warns() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_scaling_probe(dir.path());
    let mut body = std::fs::read_to_string(&config).unwrap();
    body.push_str("otel_tracing:\n  enabled: false\n");
    std::fs::write(&config, body).unwrap();

    // The warnings are logged before the topology line, in the same function.
    let (reached, seen) = run_until(dir.path(), &[], |line| line.contains("DFE topology"));

    assert!(reached, "the run never logged its topology\nstderr: {seen}");
    assert!(
        seen.lines().any(warns_for_otel_tracing),
        "an `otel_tracing:` section in a working-directory config.yaml must warn \
         that it is not applied\nstderr: {seen}"
    );
}

/// A `config.yaml` found in the working directory is read by the wrapper
/// alone, so a scalo section in it is reported, not obeyed.
#[test]
fn scalo_section_in_a_working_directory_config_warns() {
    let dir = tempfile::tempdir().unwrap();
    write_scaling_probe(dir.path());

    let (warned, seen) = run_until(dir.path(), &[], warns_for_scaling);

    assert!(
        warned,
        "a `scaling:` section in a working-directory config.yaml must warn that \
         it is not applied\nstderr: {seen}"
    );
}

/// The settings `binary()` applies take effect in the running service.
///
/// A scalo release that renames one of the keys turns it into a no-op, and the
/// tests would go back to phoning home with nothing failing. The two OTLP
/// lines are logged when scalo reads its switches, ahead of the topology line.
#[test]
fn binary_helper_switches_telemetry_off() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_scaling_probe(dir.path());

    let (reached, seen) = run_until(
        dir.path(),
        &["--config", config.to_str().unwrap()],
        |line| line.contains("DFE topology"),
    );

    assert!(reached, "the run never logged its topology\nstderr: {seen}");
    for line in ["OTLP span export disabled", "OTLP metric push disabled"] {
        assert!(
            seen.contains(line),
            "expected `{line}` in the startup log\nstderr: {seen}"
        );
    }
    assert!(
        !seen.contains("version check ON"),
        "the version check announced itself\nstderr: {seen}"
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
/// `MetricsManager` that tried to bind the same `:9090` as scalo's
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
    // the host running the test suite (the scalo metrics server uses
    // METRICS_ADDR; we set both health/metrics via env to ephemeral high
    // ports).
    let mut child = binary()
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

/// The committed Dockerfile is `emit-dockerfile` output, byte for byte, so a
/// contract change that was never regenerated cannot ship a stale image.
#[test]
fn checked_in_dockerfile_matches_emit_dockerfile() {
    let output = binary()
        .arg("emit-dockerfile")
        .output()
        .expect("failed to execute binary");

    assert!(
        output.status.success(),
        "emit-dockerfile should exit 0\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Dockerfile");
    let on_disk = std::fs::read_to_string(&path).expect("read the committed Dockerfile");
    let emitted = String::from_utf8(output.stdout).expect("emit-dockerfile prints UTF-8");
    assert_eq!(
        on_disk, emitted,
        "the committed Dockerfile does not match emit-dockerfile -- regenerate with: \
         `dfe-transform-vrl emit-dockerfile > Dockerfile`"
    );
}

#[test]
fn emit_compose_outputs_yaml() {
    let output = binary()
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
    let output = binary()
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

    let output = binary()
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
