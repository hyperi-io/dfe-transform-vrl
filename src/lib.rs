// Project:   dfe-transform-vrl
// File:      src/lib.rs
// Purpose:   Library root — module declarations and public API
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-transform-vrl: Embedded VRL transform engine for Kafka-to-Kafka pipelines.
//!
//! Wrapper-controlled Kafka source/sink with in-process VRL transforms over
//! JSON records, and bounded memory.
//!
//! Lints (`forbid(unsafe_code)`, `warn(clippy::pedantic)`, `warn(missing_docs)`,
//! `warn(rustdoc::*)`, `deny(clippy::unwrap_used/expect_used/panic/dbg_macro)`)
//! are configured in `Cargo.toml` `[lints]` — the modern Cargo 1.74+ canonical
//! form, applies to both lib and bin from one place. Don't duplicate them here
//! as `#![...]` attributes: that's the two-file-source-of-truth footgun (one
//! copy will rot).

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
