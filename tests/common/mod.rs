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
// Container naming and cleanup
// =============================================================================
//
// Every container this suite starts carries a name that says which repo, which
// suite and which backing service it is, so an operator looking at `docker ps`
// can tell what left it behind. testcontainers' default is a random hex name,
// which is untraceable the moment one survives.
//
// Naming: `dfe-transform-vrl-test-integration-<test>-<service>`, because every
// container here is owned by exactly ONE test. nextest runs each test in its own
// process, so nothing is shared even when it looks like it should be -- three
// tests calling `ensure_kafka_or_skip!()` start three brokers. That was already
// true with testcontainers' random names; the only thing a single shared name
// would add is a collision, where the first test wins and the rest fail with
// "name is already in use" and skip. `container_name` still takes `None` for a
// container started once for a whole binary, but no suite does that today.
//
// Cleanup is belt AND braces, because `Drop` alone is not enough:
//
//   - Normal completion and a panic both unwind, so `Drop` stops the container.
//   - A SIGKILL, an abort, or Ctrl-C on the test run does NOT. `Drop` never
//     runs and the container survives.
//
// testcontainers-rs 0.27 has no resource reaper (no Ryuk), so the second case
// is the one that leaves crap behind. A deterministic name would then make it
// WORSE than a random one -- the leaked container holds the name and every
// later run fails with "name already in use". `reap_stale` closes that: remove
// any container already holding the name before starting, so a leak costs the
// next run nothing and self-heals.
//
// The label goes on as well, so a sweep can find these regardless of name:
//   docker rm -f $(docker ps -aq --filter label=io.hyperi.test.suite=dfe-transform-vrl-integration)

/// Label marking every container this suite starts, for bulk cleanup.
pub const TEST_SUITE_LABEL: (&str, &str) =
    ("io.hyperi.test.suite", "dfe-transform-vrl-integration");

/// Labels for a container this suite starts: what it is, and whose run owns it.
///
/// The name says what and why; these say WHO, which is what you need when
/// several runs share a machine and one has left something behind. The pid is
/// the owning test process -- `ps -p <pid>` answers "is that run still alive, or
/// is this rubbish I can remove?".
fn test_labels(service: &str) -> Vec<(String, String)> {
    vec![
        (
            TEST_SUITE_LABEL.0.to_string(),
            TEST_SUITE_LABEL.1.to_string(),
        ),
        (
            "io.hyperi.test.repo".to_string(),
            "dfe-transform-vrl".to_string(),
        ),
        ("io.hyperi.test.service".to_string(), service.to_string()),
        (
            "io.hyperi.test.owner-pid".to_string(),
            std::process::id().to_string(),
        ),
    ]
}

/// Container name for a backing service in this suite.
///
/// Pass `Some(test)` -- the owning test -- for anything a test starts for itself,
/// which is everything here. `None` is for a container started once for a whole
/// test binary; nothing does that today, and using it from several tests would
/// make them collide on the name rather than share the container.
///
/// Names are lowercased and non-alphanumerics collapse to `-`, because Docker
/// only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`, and a Rust test path
/// (`kafka::test_roundtrip`) has colons in it.
#[must_use]
pub fn container_name(test: Option<&str>, service: &str) -> String {
    let slug = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
    };
    test.map_or_else(
        || format!("dfe-transform-vrl-test-integration-{}", slug(service)),
        |t| {
            format!(
                "dfe-transform-vrl-test-integration-{}-{}",
                slug(t),
                slug(service)
            )
        },
    )
}

/// Remove a DEAD container holding `name`, so a leak from a killed run cannot
/// block this one.
///
/// Never touches a RUNNING container. Two concurrent runs of this suite on one
/// machine share these names, and force-removing a live one would sabotage the
/// other run -- a confusing mid-test failure in a process that did nothing
/// wrong. Leaving it means the start below fails with "name is already in use",
/// which says what actually happened.
///
/// Best-effort otherwise: no Docker, nothing to remove, or an already-gone
/// container are all fine. A failure here must not fail the test -- the start
/// that follows reports the real problem.
pub fn reap_stale(name: &str) {
    let running = std::process::Command::new("docker")
        .args(["ps", "--quiet", "--filter", &format!("name=^{name}$")])
        .output();
    // Non-empty stdout means a container by this name is up. Leave it alone.
    if let Ok(out) = &running
        && !out.stdout.is_empty()
    {
        return;
    }
    let _ = std::process::Command::new("docker")
        .args(["rm", "--force", "--volumes", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

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
/// let env = KafkaTestEnv::ensure("my-test-name").await;
/// let kf = env.config();
/// // ...use kf.brokers...
/// // env drops here, container stops if one was spawned
/// ```
#[allow(dead_code)]
pub struct KafkaTestEnv {
    config: KafkaTestConfig,
    // Kept alive for RAII; dropped (and container stopped) when KafkaTestEnv
    // drops. Also read by `manages_container`, so it carries no underscore --
    // that prefix would claim nothing looks at it.
    container: Option<testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>>,
}

#[allow(dead_code)]
impl KafkaTestEnv {
    /// Ensure a Kafka broker is available — either live or via testcontainers.
    ///
    /// `test` names the calling test and goes into the container name, so
    /// concurrent tests do not collide on it.
    ///
    /// If Docker isn't available and no live broker is configured, returns `None`
    /// so callers can `return` (skip) the test.
    pub async fn ensure(test: &str) -> Option<Self> {
        let live = kafka_test_config();
        if live.is_reachable() {
            eprintln!("Using live Kafka at {}", live.brokers);
            return Some(Self {
                config: live,
                container: None,
            });
        }

        // No live broker — try to spawn a testcontainers Kafka.
        eprintln!("No live Kafka reachable — attempting to spawn testcontainers Kafka...");
        match Self::spawn_container(test, None).await {
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

    /// Ensure a Kafka broker this test OWNS — always a fresh container,
    /// never a broker that was already there.
    ///
    /// `ensure` prefers a live broker, which is wrong for a test that uses
    /// the real DFE topic names: `filebeat_land` on a shared broker is
    /// somebody else's data. Returns `None` when no container can be
    /// started, so callers can skip.
    pub async fn hermetic(test: &str) -> Option<Self> {
        match Self::spawn_container(test, None).await {
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

    /// As [`hermetic`](Self::hermetic), with the broker published on a host
    /// port below 10240 from [`ports::free_port`].
    ///
    /// Retries on a fresh port when another process binds the one picked
    /// before Docker does.
    pub async fn hermetic_on_low_port(test: &str) -> Option<Self> {
        let mut last = String::new();
        for _ in 0..5 {
            match Self::spawn_container(test, Some(ports::free_port())).await {
                Ok(env) => {
                    eprintln!("Spawned testcontainers Kafka at {}", env.config.brokers);
                    return Some(env);
                }
                Err(e) => last = e,
            }
        }
        eprintln!("Could not spawn testcontainers Kafka: {last}");
        None
    }

    /// Freeze the broker container this env started. Its clients keep their
    /// connections and get no answers, as with a stalled broker.
    pub async fn pause(&self) -> Result<(), String> {
        self.container
            .as_ref()
            .ok_or("this env points at a live broker, which a test must not pause")?
            .pause()
            .await
            .map_err(|e| format!("pause the broker: {e}"))
    }

    /// Resume the broker container [`pause`](Self::pause) froze.
    pub async fn unpause(&self) -> Result<(), String> {
        self.container
            .as_ref()
            .ok_or("this env points at a live broker, which a test must not pause")?
            .unpause()
            .await
            .map_err(|e| format!("unpause the broker: {e}"))
    }

    async fn spawn_container(test: &str, host_port: Option<u16>) -> Result<Self, String> {
        use testcontainers::ImageExt;
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

        // Pinned here, not left to the module default of 3.8.0. A tag baked
        // into a dependency's source is invisible to dependency review:
        // Renovate reads Cargo.toml, correctly reports the crate current, and
        // never sees the image.
        // JVM image: `apache/kafka-native` before 4.4.0 segfaults in `getpwuid` on ~2% of starts.
        // renovate: datasource=docker depName=apache/kafka
        const KAFKA_TAG: &str = "4.3.1";
        // Digest of `KAFKA_TAG`, apart from it because the Renovate regex stops at a colon.
        const KAFKA_DIGEST: &str =
            "sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

        // A JVM broker takes 5-12 s to become ready, more on a busy runner, so 60 s is too tight.
        const KAFKA_STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

        let name = container_name(Some(test), "kafka");
        reap_stale(&name);
        let request = Kafka::default()
            .with_jvm_image()
            .with_tag(format!("{KAFKA_TAG}@{KAFKA_DIGEST}"))
            .with_container_name(&name)
            .with_labels(test_labels("kafka"))
            .with_startup_timeout(KAFKA_STARTUP_TIMEOUT);
        let request = match host_port {
            Some(port) => request.with_mapped_port(port, KAFKA_PORT),
            None => request,
        };
        let container = request
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
            container: Some(container),
        })
    }

    pub const fn config(&self) -> &KafkaTestConfig {
        &self.config
    }

    /// Whether THIS env started a container, as opposed to pointing at a live
    /// broker. The hygiene test has nothing to inspect in the live case.
    pub const fn manages_container(&self) -> bool {
        self.container.is_some()
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
/// Takes the calling test's name, which names the container it may start. It is
/// passed rather than derived because Rust has no way to read the current test's
/// name, and a shared name would make concurrent tests collide.
#[macro_export]
macro_rules! ensure_kafka_or_skip {
    ($test:expr) => {{
        match $crate::common::KafkaTestEnv::ensure($test).await {
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
