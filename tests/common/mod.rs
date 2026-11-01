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
//! - `remote` (default) — remote cluster endpoints from env vars
//! - `docker` — dfe-docker infra profile on localhost (no auth, no TLS)

use std::env;

use scalo::deployment::test_support::shared_service::{
    OwnedContainer, SharedContainer, Suite, container_name, docker, start_alone,
};
use scalo::transport::kafka::test_support::{KAFKA_PORT, docker_answers, kafka_spec, shared_kafka};

pub mod offline;
pub mod ports;

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

/// Load this repo's own `.env`: `dotenvy::dotenv()` would load the first `.env` in any parent.
pub fn load_dotenv() {
    let _ = dotenvy::from_path(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".env"));
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
///   2. `../dfe-docker` beside this repo (sibling directory convention)
///
/// Returns `Ok(true)` if containers were started, `Ok(false)` if already running, not docker mode,
/// or dfe-docker is not checked out (the skip reason goes to stderr).
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
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../dfe-docker")
            .to_string_lossy()
            .into_owned()
    });

    if !std::path::Path::new(&docker_path)
        .join("docker-compose.yml")
        .exists()
    {
        eprintln!(
            "Skipping: dfe-docker not found at {docker_path}. \
             Set DFE_DOCKER_PATH or clone dfe-docker beside this repo."
        );
        return Ok(false);
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
// Broker containers
// =============================================================================
//
// scalo's `shared_service` starts and names every container: one PLAINTEXT
// broker per test run, shared by every nextest process, and a broker a test
// holds alone when it pauses it. Names start `dfe-transform-vrl-test-integration-`
// (or `HYPERI_TEST_CONTAINER_PREFIX`), the `io.hyperi.test.suite` label is
// `dfe-transform-vrl-integration`, and a watcher removes them once the run ends.

/// This suite, for container names, labels and the `HYPERI_TEST_*` lookup.
pub const SUITE: Suite = Suite {
    repo: "dfe-transform-vrl",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

/// Where a [`KafkaTestEnv`]'s broker comes from.
enum Broker {
    /// A broker the environment names, never paused.
    Live,
    /// The run's shared broker.
    Shared(SharedContainer),
    /// A broker this test holds alone, removed when the env drops.
    Owned(OwnedContainer),
}

/// A Kafka broker for one test.
///
/// Resolution order for [`ensure`](Self::ensure):
/// 1. `$KAFKA_BROKERS` (or `TEST_MODE=docker` localhost:19092), when it answers.
/// 2. Otherwise the run's shared broker container.
///
/// Usage:
/// ```ignore
/// let env = KafkaTestEnv::ensure("my-test-name").await;
/// let kf = env.config();
/// ```
#[allow(dead_code)]
pub struct KafkaTestEnv {
    config: KafkaTestConfig,
    broker: Broker,
}

#[allow(dead_code)]
impl KafkaTestEnv {
    /// A live broker when one answers, else the run's shared broker.
    ///
    /// `None` when there is neither, so the caller can skip.
    pub async fn ensure(test: &str) -> Option<Self> {
        let live = kafka_test_config();
        if live.is_reachable() {
            eprintln!("Using live Kafka at {}", live.brokers);
            return Some(Self {
                config: live,
                broker: Broker::Live,
            });
        }
        Self::hermetic(test).await
    }

    /// The run's shared broker container, never a live one.
    ///
    /// `ensure` prefers a live broker, which is wrong for a test that uses
    /// the real DFE topic names: `filebeat_land` on a shared live broker is
    /// somebody else's data. `None` when Docker does not answer.
    pub async fn hermetic(_test: &str) -> Option<Self> {
        if !docker_answers() {
            return None;
        }
        let container = shared_kafka(SUITE).await;
        Some(Self {
            config: plaintext(container.addr(KAFKA_PORT)),
            broker: Broker::Shared(container),
        })
    }

    /// A broker this test holds alone, published on a host port below 10240
    /// from [`ports::free_port`], which [`pause`](Self::pause) may freeze.
    ///
    /// Retries on a fresh port when another process binds the one picked
    /// before Docker does.
    pub async fn hermetic_on_low_port(test: &str) -> Option<Self> {
        if !docker_answers() {
            return None;
        }
        let mut last = String::new();
        for attempt in 0..5 {
            let name = container_name(&SUITE, Some(&format!("{test}-{attempt}")), "kafka");
            let spec = kafka_spec(Some(ports::free_port()));
            match tokio::task::spawn_blocking(move || start_alone(&SUITE, &spec, &name)).await {
                Ok(owned) => {
                    return Some(Self {
                        config: plaintext(owned.container.addr(KAFKA_PORT)),
                        broker: Broker::Owned(owned),
                    });
                }
                Err(e) => last = e.to_string(),
            }
        }
        eprintln!("Could not start a Kafka container: {last}");
        None
    }

    /// Freeze the broker container this test holds. Its clients keep their
    /// connections and get no answers, as with a stalled broker.
    pub async fn pause(&self) -> Result<(), String> {
        self.docker_on_owned("pause").await
    }

    /// Resume the broker container [`pause`](Self::pause) froze.
    pub async fn unpause(&self) -> Result<(), String> {
        self.docker_on_owned("unpause").await
    }

    async fn docker_on_owned(&self, verb: &'static str) -> Result<(), String> {
        let Broker::Owned(owned) = &self.broker else {
            return Err("only a broker this test holds alone may be paused".into());
        };
        let name = owned.container.name.clone();
        let out = tokio::task::spawn_blocking(move || docker(&[verb, &name]))
            .await
            .map_err(|e| format!("docker {verb}: {e}"))?;
        if out.status.success() {
            Ok(())
        } else {
            Err(format!(
                "docker {verb}: {}",
                String::from_utf8_lossy(&out.stderr)
            ))
        }
    }

    pub const fn config(&self) -> &KafkaTestConfig {
        &self.config
    }

    /// The broker container's name, or `None` for a live broker.
    pub fn container_name(&self) -> Option<&str> {
        match &self.broker {
            Broker::Live => None,
            Broker::Shared(container) => Some(&container.name),
            Broker::Owned(owned) => Some(&owned.container.name),
        }
    }
}

/// A PLAINTEXT client config for a broker container at `brokers`.
fn plaintext(brokers: String) -> KafkaTestConfig {
    KafkaTestConfig {
        brokers,
        security_protocol: "PLAINTEXT".into(),
        sasl_mechanism: None,
        sasl_user: None,
        sasl_password: None,
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
///
/// Takes the calling test's name, which [`KafkaTestEnv::ensure`] takes.
#[macro_export]
macro_rules! ensure_kafka_or_skip {
    ($test:expr) => {{
        match $crate::common::KafkaTestEnv::ensure($test).await {
            Some(env) => env,
            None => {
                $crate::common::require_service_in_ci("Kafka", "no live broker and no Docker");
                eprintln!("SKIP: no live Kafka and no Docker. Set $KAFKA_BROKERS or run Docker.");
                return;
            }
        }
    }};
}

#[allow(unused_imports)]
pub(crate) use ensure_kafka_or_skip;
