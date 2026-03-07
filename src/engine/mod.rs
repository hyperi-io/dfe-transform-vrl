// Project:   dfe-transform-vrl
// File:      src/engine/mod.rs
// Purpose:   VRL transform engine — compilation and execution
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! VRL transform engine.
//!
//! Compiles VRL source code at startup and executes compiled programs
//! against events in-process. No Vector subprocess needed.

pub mod compiler;
pub mod runner;
