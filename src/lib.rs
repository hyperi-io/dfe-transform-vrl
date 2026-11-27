// Project:   dfe-transform-vrl
// File:      src/lib.rs
// Purpose:   Library root -- module declarations and public API
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-transform-vrl: in-process VRL transforms over JSON records, between a
//! Kafka or direct gRPC source and sink.
//!
//! Lints live in `Cargo.toml` `[lints]`, which covers the lib and the bins.

pub mod cli;
pub mod config;
pub mod deployment;
pub mod engine;
pub mod enrichment;
pub mod error;
pub mod kafka;
pub mod metrics;
pub mod pipeline;

pub use error::{Error, Result};
