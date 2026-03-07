// Project:   dfe-transform-vrl
// File:      src/health.rs
// Purpose:   Health endpoint HTTP server
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Health endpoint server.
//!
//! Serves `/health/live` and `/health/ready` endpoints on the configured
//! address (default :9000). Same contract as dfe-transform-vector and
//! all other DFE services.

// Health server implementation follows the same pattern as dfe-transform-vector.
// Will reuse hyper-based HTTP server or migrate to hyperi-rustlib http-server module.
