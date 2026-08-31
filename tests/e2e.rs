// Project:   dfe-transform-vrl
// File:      tests/e2e.rs
// Purpose:   End-to-end tests requiring real infrastructure
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used)]

//! End-to-end tests requiring real infrastructure (Kafka, etc.).
//!
//! Tests that may reach a broker somebody else owns are `#[ignore]` by
//! default. Run with:
//!   `cargo nextest run -- --ignored`
//!   `TEST_MODE=docker cargo nextest run -- --ignored`
//!
//! `filebeat_kafka` is not among them: it starts and drops its own broker
//! container, so it runs by default like any other test.

#[path = "common/mod.rs"]
mod common;

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "common/filebeat.rs"]
mod filebeat_corpus;

#[allow(clippy::expect_used, clippy::panic)]
#[path = "e2e/filebeat_kafka.rs"]
mod filebeat_kafka;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/kafka.rs"]
mod kafka;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/connectivity.rs"]
mod connectivity;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/cli_service.rs"]
mod cli_service;

// A leak check has to fail the test when the container is still there, and the
// poll loop it sits after cannot express that as an assert.
#[allow(clippy::panic)]
#[path = "e2e/container_hygiene.rs"]
mod container_hygiene;
