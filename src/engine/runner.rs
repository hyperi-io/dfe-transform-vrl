// Project:   dfe-transform-vrl
// File:      src/engine/runner.rs
// Purpose:   VRL program execution against events
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! VRL program execution.
//!
//! Runs compiled VRL programs against event Values in-process.
//! Handles runtime errors with configurable behaviour (skip, DLQ).

// VRL execution will be implemented once we verify the vrl crate API
// compiles correctly. The key types are:
//
// - vrl::compiler::compile() — compile VRL source to Program
// - vrl::compiler::runtime::Runtime — execute Program against TargetValueRef
// - vrl::compiler::TargetValueRef — wraps &mut Value + metadata + secrets
// - vrl::value::Value — the event data type (serde-compatible)
