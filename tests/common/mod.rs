// Project:   dfe-transform-vrl
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure — dual-mode (remote/docker) config helpers
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared test helpers for integration and e2e tests.
//!
//! Supports two test backends via `TEST_MODE` in `.env`:
//! - `remote` (default) — devex cluster endpoints from env vars
//! - `docker` — dfe-docker infra profile on localhost (no auth, no TLS)

use std::env;

/// Test backend mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    Remote,
    Docker,
}

impl TestMode {
    pub fn detect() -> Self {
        load_dotenv();
        match env::var("TEST_MODE").unwrap_or_default().as_str() {
            "docker" => Self::Docker,
            _ => Self::Remote,
        }
    }
}

pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

// =============================================================================
// Kafka config
// =============================================================================

/// Kafka connection config for the active test mode.
pub struct KafkaTestConfig {
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_user: Option<String>,
    pub sasl_password: Option<String>,
}

/// Returns Kafka connection config for the active test mode.
///
/// Docker mode: `localhost:19092`, PLAINTEXT, no SASL.
/// Remote mode: from env vars (`KAFKA_BROKERS`, `_SASL_MECHANISM`, etc.)
pub fn kafka_test_config() -> KafkaTestConfig {
    load_dotenv();
    match TestMode::detect() {
        TestMode::Docker => KafkaTestConfig {
            brokers: "localhost:19092".into(),
            security_protocol: "PLAINTEXT".into(),
            sasl_mechanism: None,
            sasl_user: None,
            sasl_password: None,
        },
        TestMode::Remote => KafkaTestConfig {
            brokers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
            security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "SASL_PLAINTEXT".into()),
            sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
            sasl_user: env::var("KAFKA_SASL_USER").ok(),
            sasl_password: env::var("KAFKA_SASL_PASSWORD").ok(),
        },
    }
}

impl KafkaTestConfig {
    pub const fn has_sasl(&self) -> bool {
        self.sasl_mechanism.is_some() && self.sasl_user.is_some()
    }

    /// Check if broker is reachable via TCP.
    pub fn is_reachable(&self) -> bool {
        use std::net::ToSocketAddrs;
        let first = self.brokers.split(',').next().unwrap_or(&self.brokers);
        first
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .is_some_and(|a| {
                std::net::TcpStream::connect_timeout(&a, std::time::Duration::from_secs(3)).is_ok()
            })
    }
}

// =============================================================================
// Test topic naming
// =============================================================================

/// Generate a unique test topic name to avoid collisions.
pub fn test_topic(suffix: &str) -> String {
    load_dotenv();
    let prefix = env::var("TEST_TOPIC_PREFIX").unwrap_or_else(|_| "dfe-transform-vrl-test".into());
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{prefix}-{suffix}-{ts}")
}

// =============================================================================
// Docker lifecycle helper
// =============================================================================

/// Start dfe-docker infra profile if `TEST_MODE=docker` and containers aren't running.
///
/// Looks for dfe-docker at:
///   1. `DFE_DOCKER_PATH` env var
///   2. `../dfe-docker` (sibling directory convention)
///   3. `/projects/dfe-docker` (absolute path)
///
/// Returns `Ok(true)` if containers were started, `Ok(false)` if already running or not docker mode.
#[allow(dead_code)]
pub fn ensure_docker_infra() -> Result<bool, String> {
    if TestMode::detect() != TestMode::Docker {
        return Ok(false);
    }

    // Check if already running (look for the Kafka container from dfe-docker)
    let output = std::process::Command::new("docker")
        .args(["ps", "--filter", "name=dfe-kafka", "--format", "{{.Names}}"])
        .output()
        .map_err(|e| format!("docker not found: {e}"))?;

    if String::from_utf8_lossy(&output.stdout).contains("dfe-kafka") {
        return Ok(false);
    }

    let docker_path = env::var("DFE_DOCKER_PATH").unwrap_or_else(|_| {
        if std::path::Path::new("/projects/dfe-docker/docker-compose.yml").exists() {
            "/projects/dfe-docker".into()
        } else {
            "../dfe-docker".into()
        }
    });

    if !std::path::Path::new(&docker_path)
        .join("docker-compose.yml")
        .exists()
    {
        return Err(format!(
            "dfe-docker not found at {docker_path}. Set DFE_DOCKER_PATH or clone dfe-docker."
        ));
    }

    let status = std::process::Command::new("docker")
        .args(["compose", "--profile", "infra", "up", "-d"])
        .current_dir(&docker_path)
        .status()
        .map_err(|e| format!("docker compose failed: {e}"))?;

    if !status.success() {
        return Err("docker compose --profile infra up -d failed".into());
    }

    // Wait for Kafka to become reachable
    for _ in 0..30 {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let kf = kafka_test_config();
        if kf.is_reachable() {
            return Ok(true);
        }
    }

    Err("Kafka did not become reachable within 30s".into())
}

// =============================================================================
// Skip macros
// =============================================================================

/// Skip test if Kafka is not available in the current test mode.
macro_rules! skip_if_no_kafka {
    () => {
        let kf = crate::common::kafka_test_config();
        if !kf.is_reachable() {
            eprintln!(
                "Skipping: Kafka not reachable at {} (TEST_MODE={:?})",
                kf.brokers,
                crate::common::TestMode::detect()
            );
            return;
        }
    };
}

pub(crate) use skip_if_no_kafka;
