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
//! `filebeat_kafka`, `held_acks` and `kafka_tls` are not among them: they use a broker
//! container scalo's test support starts, the run's shared one or one the
//! test holds alone, so they run by default like any other test.

#[path = "common/mod.rs"]
mod common;

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "common/filebeat.rs"]
mod filebeat_corpus;

#[allow(clippy::expect_used, clippy::panic)]
#[path = "e2e/filebeat_kafka.rs"]
mod filebeat_kafka;

#[allow(clippy::expect_used, clippy::panic)]
#[path = "e2e/held_acks.rs"]
mod held_acks;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/kafka.rs"]
mod kafka;

#[path = "e2e/kafka_tls.rs"]
mod kafka_tls;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/connectivity.rs"]
mod connectivity;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/cli_service.rs"]
mod cli_service;

#[path = "e2e/container_hygiene.rs"]
mod container_hygiene;
