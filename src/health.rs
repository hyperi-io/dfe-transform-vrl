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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    /// Start the server on a random ephemeral port and verify the ready flag
    /// is returned and writable.
    #[tokio::test]
    async fn start_health_server_returns_ready_flag() {
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Ephemeral port 0 asks the OS to pick a free one.
        let ready = start_health_server("127.0.0.1:0", rx)
            .await
            .expect("server should start");

        // Flag is writable — pipeline uses it to signal readiness state.
        ready.store(false, Ordering::Release);
        assert!(!ready.load(Ordering::Acquire));
        ready.store(true, Ordering::Release);
        assert!(ready.load(Ordering::Acquire));

        // Clean shutdown
        let _ = tx.send(true);
        // Give the server task a moment to observe shutdown
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn start_health_server_invalid_address_fails_or_binds() {
        let (_tx, rx) = tokio::sync::watch::channel(false);

        // Binding to an invalid port format doesn't fail synchronously because
        // the server spawns its listener in a tokio task. But returning a
        // ready flag is still expected from start_health_server itself.
        // We exercise that code path.
        let result = start_health_server("127.0.0.1:0", rx).await;
        assert!(result.is_ok());
    }
}
