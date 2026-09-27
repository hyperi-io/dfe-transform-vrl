// Project:   dfe-transform-vrl
// File:      tests/common/ports.rs
// Purpose:   Host ports below 10240 for test listeners and container mappings
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Host ports for the listeners and container port mappings a test starts.
//!
//! They come from below 10240, outside the range the OS assigns ephemeral
//! ports from, so an outbound connection elsewhere on the host cannot take a
//! port between the probe here and the test's own bind.

use std::sync::atomic::{AtomicU32, Ordering};

/// The lowest port handed out.
const FIRST: u16 = 7000;

/// One past the highest port handed out.
const END: u16 = 10240;

/// Ports each process starts its scans within, one per call.
const PER_PROCESS: u32 = 8;

/// Calls made so far in this process, so successive calls start their scan
/// at different ports.
static CALLS: AtomicU32 = AtomicU32::new(0);

/// A host port below 10240 that nothing holds right now.
///
/// Each process starts its scans in its own run of [`PER_PROCESS`] ports,
/// taken from its pid, so concurrent nextest processes do not race for one
/// port, and moves on by one per call, so two calls before either port is
/// bound never return the same one. Another process can still take the port
/// first, so the caller binds it and retries on a fresh one when that fails.
pub fn free_port() -> u16 {
    let span = u32::from(END - FIRST);
    let start = std::process::id()
        .wrapping_mul(PER_PROCESS)
        .wrapping_add(CALLS.fetch_add(1, Ordering::Relaxed))
        % span;
    (0..span)
        .map(|i| FIRST + u16::try_from((start + i) % span).expect("offset fits the span"))
        .find(|port| std::net::TcpListener::bind(("0.0.0.0", *port)).is_ok())
        .expect("no free host port below 10240")
}
