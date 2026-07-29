// Project:   dfe-transform-vrl
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure — dual-mode (remote/docker) config helpers
// Language:  Rust
//
// License:   BUSL-1.1
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
#[allow(unused_macros)]
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

#[allow(unused_imports)]
pub(crate) use skip_if_no_kafka;

// =============================================================================
// Live-or-Testcontainers Kafka helper
// =============================================================================

/// RAII test environment for Kafka tests.
///
/// Resolution order:
/// 1. `$KAFKA_BROKERS` (or `TEST_MODE=docker` localhost:19092) → use it (no container spawned)
/// 2. Otherwise → spawn an Apache Kafka container via testcontainers (`KRaft` mode, no Zookeeper)
///
/// **Cleanup:** When this struct drops, any spawned container is automatically
/// stopped and removed by the testcontainers `Drop` impl. Tests do not need
/// explicit teardown — just let the binding go out of scope at end of test.
///
/// Usage:
/// ```ignore
/// let env = KafkaTestEnv::ensure().await;
/// let kf = env.config();
/// // ...use kf.brokers...
/// // env drops here, container stops if one was spawned
/// ```
#[allow(dead_code)]
pub struct KafkaTestEnv {
    config: KafkaTestConfig,
    // Kept alive for RAII; dropped (and container stopped) when KafkaTestEnv drops.
    _container:
        Option<testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>>,
}

#[allow(dead_code)]
impl KafkaTestEnv {
    /// Ensure a Kafka broker is available — either live or via testcontainers.
    ///
    /// If Docker isn't available and no live broker is configured, returns `None`
    /// so callers can `return` (skip) the test.
    pub async fn ensure() -> Option<Self> {
        let live = kafka_test_config();
        if live.is_reachable() {
            eprintln!("Using live Kafka at {}", live.brokers);
            return Some(Self {
                config: live,
                _container: None,
            });
        }

        // No live broker — try to spawn a testcontainers Kafka.
        eprintln!("No live Kafka reachable — attempting to spawn testcontainers Kafka...");
        match Self::spawn_container().await {
            Ok(env) => {
                eprintln!("Spawned testcontainers Kafka at {}", env.config.brokers);
                Some(env)
            }
            Err(e) => {
                eprintln!("Could not spawn testcontainers Kafka: {e}");
                None
            }
        }
    }

    async fn spawn_container() -> Result<Self, String> {
        use testcontainers::ImageExt;
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

        // Pinned here, not left to the module default of 3.8.0. A tag baked
        // into a dependency's source is invisible to dependency review:
        // Renovate reads Cargo.toml, correctly reports the crate current, and
        // never sees the image.
        // renovate: datasource=docker depName=apache/kafka-native
        const KAFKA_TAG: &str = "4.3.1";

        let container = Kafka::default()
            .with_tag(KAFKA_TAG)
            .start()
            .await
            .map_err(|e| format!("start Kafka container: {e}"))?;

        let host = container
            .get_host()
            .await
            .map_err(|e| format!("get host: {e}"))?;
        let port = container
            .get_host_port_ipv4(KAFKA_PORT)
            .await
            .map_err(|e| format!("get port: {e}"))?;
        let brokers = format!("{host}:{port}");

        Ok(Self {
            config: KafkaTestConfig {
                brokers,
                security_protocol: "PLAINTEXT".into(),
                sasl_mechanism: None,
                sasl_user: None,
                sasl_password: None,
            },
            _container: Some(container),
        })
    }

    pub const fn config(&self) -> &KafkaTestConfig {
        &self.config
    }
}

/// Panic if NEITHER a live broker nor Docker is available while running in CI.
///
/// Scoped to "no path at all", not to "the live broker is absent". CI is not
/// promised an external Kafka, but it does provide a container runtime, so
/// `KafkaTestEnv::ensure()` should always find one of the two. Finding neither
/// means the test would pass VACUOUSLY -- green while exercising nothing.
///
/// The live-only probe (`skip_if_no_kafka!`) stays a plain skip for the same
/// reason: failing on it would assert an environment nobody agreed to provide.
pub fn require_service_in_ci(what: &str, detail: &str) {
    assert!(
        std::env::var_os("CI").is_none(),
        "{what} unavailable in CI ({detail}) -- integration tests must RUN here, \
         not skip. Skipping would report green while testing nothing."
    );
}

/// Skip the test (with eprintln explanation) if no Kafka can be made available.
/// This is used when neither a live broker nor Docker is reachable in the env.
#[macro_export]
macro_rules! ensure_kafka_or_skip {
    () => {{
        match $crate::common::KafkaTestEnv::ensure().await {
            Some(env) => env,
            None => {
                $crate::common::require_service_in_ci("Kafka", "no live broker and no Docker");
                eprintln!(
                    "SKIP: no live Kafka and Docker/testcontainers unavailable. \
                     Set $KAFKA_BROKERS or run Docker."
                );
                return;
            }
        }
    }};
}

#[allow(unused_imports)]
pub(crate) use ensure_kafka_or_skip;
