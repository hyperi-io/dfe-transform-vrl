// Project:   dfe-transform-vrl
// File:      tests/integration.rs
// Purpose:   Single-binary integration test entry point
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests — single binary with submodules.
//!
//! Consolidates all integration tests into one compilation unit for ~3x
//! faster link times vs separate `tests/*.rs` files.

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "integration/config.rs"]
mod config;

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "integration/enrichment.rs"]
mod enrichment;

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "integration/vrl_edge_cases.rs"]
mod vrl_edge_cases;

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "integration/vrl_realworld.rs"]
mod vrl_realworld;

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "integration/vrl_transforms.rs"]
mod vrl_transforms;
