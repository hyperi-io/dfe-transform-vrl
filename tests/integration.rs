// Project:   dfe-transform-vrl
// File:      tests/integration.rs
// Purpose:   Single-binary integration test entry point
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::needless_raw_string_hashes,
    clippy::manual_is_multiple_of,
    clippy::redundant_clone,
    clippy::single_range_in_vec_init,
    clippy::missing_docs_in_private_items,
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::missing_const_for_fn
)]

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

#[cfg(feature = "transport-memory")]
#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "integration/pipeline_memory.rs"]
mod pipeline_memory;

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "integration/vrl_deep.rs"]
mod vrl_deep;
