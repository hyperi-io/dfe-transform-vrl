// Project:   dfe-transform-vrl
// File:      src/lib.rs
// Purpose:   Library root — module declarations and public API
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-transform-vrl: Embedded VRL transform engine for Kafka-to-Kafka pipelines.
//!
//! Wrapper-controlled Kafka source/sink with in-process VRL transforms,
//! native msgpack support, and bounded memory.

pub mod cli;
pub mod config;
pub mod deployment;
pub mod engine;
pub mod error;
pub mod health;
pub mod kafka;
pub mod metrics;
pub mod pipeline;

pub use error::{Error, Result};
