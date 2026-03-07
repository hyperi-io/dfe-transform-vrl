// Project:   dfe-transform-vrl
// File:      src/metrics.rs
// Purpose:   Prometheus metrics server
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics server.
//!
//! Exposes `/metrics` on the configured address (default :9090).
//! Includes:
//! - Events processed (counter, by outcome: success/error/filtered)
//! - Transform latency (histogram)
//! - Consumer lag (gauge, per partition)
//! - Producer queue depth (gauge)
//! - Scaling pressure (gauge, KEDA-compatible)

// Metrics implementation follows the same pattern as dfe-transform-vector.
