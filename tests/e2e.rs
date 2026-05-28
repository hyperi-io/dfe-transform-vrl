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
//! All tests are `#[ignore]` by default. Run with:
//!   `cargo nextest run -- --ignored`
//!   `TEST_MODE=docker cargo nextest run -- --ignored`

#[path = "common/mod.rs"]
mod common;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/kafka.rs"]
mod kafka;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/connectivity.rs"]
mod connectivity;

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#[path = "e2e/cli_service.rs"]
mod cli_service;
