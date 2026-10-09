// Project:   dfe-transform-vrl
// File:      tests/common/offline.rs
// Purpose:   The binary as a Command that sends no telemetry off the host
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The one place a test builds a `Command` for the compiled binary.
//!
//! The smoke, integration and e2e binaries all declare this file as a module,
//! so a spawn anywhere in `tests/` gets the same settings.

use std::process::Command;

/// The binary as a `Command`, with its telemetry switched off.
///
/// The version check and the OTLP span and metric export all default to on:
/// `run` posts to the release server, and the logger and the runtime dial the
/// OTLP endpoint (`localhost:4317` unless the caller's environment names one).
/// A test run must reach neither, so every spawn goes through here.
pub fn binary() -> Command {
    // Cargo resolves this at compile time to the binary it just built, so it
    // still points at one when coverage redirects the build with --target-dir.
    let mut command = Command::new(env!("CARGO_BIN_EXE_dfe-transform-vrl"));
    command
        .env("DFE_TRANSFORM_VERSION_CHECK__ENABLED", "false")
        .env("DFE_TRANSFORM_OTEL_TRACING__ENABLED", "false")
        .env("DFE_TRANSFORM_METRICS__OTEL__ENABLED", "false");
    command
}
