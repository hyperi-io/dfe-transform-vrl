// Project:   dfe-transform-vrl
// File:      src/health.rs
// Purpose:   Health endpoint HTTP server via rustlib
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Health endpoint server using hyperi-rustlib `http-server` module.
//!
//! Serves `/health/live` and `/health/ready` endpoints on the configured
//! address (default :9000). The rustlib `HttpServer` provides these endpoints
//! automatically when `enable_health_endpoints` is true.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use hyperi_rustlib::http_server::{HttpServer, HttpServerConfig, Router};
use tracing::info;

/// Start the health server on the given address.
///
/// Returns the ready flag so the pipeline can signal readiness,
/// and a future that runs the server until shutdown.
#[allow(clippy::unused_async)]
pub async fn start_health_server(
    address: &str,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> crate::Result<Arc<AtomicBool>> {
    let config = HttpServerConfig {
        bind_address: address.to_string(),
        enable_health_endpoints: true,
        enable_metrics_endpoint: false,
        ..HttpServerConfig::new(address)
    };

    let server = HttpServer::new(config);
    let ready_flag = server.ready_flag();

    let app = Router::new();

    info!(address, "starting health server");

    let ready_flag_clone = Arc::clone(&ready_flag);
    tokio::spawn(async move {
        let mut shutdown_rx = shutdown;
        server
            .serve_with_shutdown(app, async move {
                let _ = shutdown_rx.changed().await;
            })
            .await
            .ok();
    });

    Ok(ready_flag_clone)
}
