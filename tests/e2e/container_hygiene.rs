// Project:   dfe-transform-vrl
// File:      tests/e2e/container_hygiene.rs
// Purpose:   Pin the name and label this suite's broker containers carry
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! scalo's `shared_service` names and labels every container from this
//! suite's [`SUITE`](super::common::SUITE), so `docker ps` says which repo
//! started one and a sweep can find them all by label.

/// A started broker carries the suite's name prefix and label.
#[tokio::test]
#[ignore = "starts a real Kafka container -- run: cargo nextest run --run-ignored all --ignore-default-filter"]
async fn a_started_broker_carries_the_suite_name_and_label() {
    let Some(env) = super::common::KafkaTestEnv::ensure("container-hygiene").await else {
        eprintln!("skipping: no Docker and no live Kafka");
        return;
    };
    let Some(name) = env.container_name() else {
        eprintln!("skipping: a live Kafka is configured, so no container was started");
        return;
    };
    assert!(
        name.starts_with("dfe-transform-vrl-test-integration-"),
        "{name} does not carry the suite prefix"
    );

    let label = std::process::Command::new("docker")
        .args([
            "inspect",
            name,
            "--format",
            r#"{{index .Config.Labels "io.hyperi.test.suite"}}"#,
        ])
        .output()
        .expect("run docker inspect");
    assert_eq!(
        String::from_utf8_lossy(&label.stdout).trim(),
        "dfe-transform-vrl-integration"
    );
}
