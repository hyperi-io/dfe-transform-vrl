// Project:   dfe-transform-vrl
// File:      src/metrics.rs
// Purpose:   Prometheus metrics via rustlib MetricsManager
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics using hyperi-rustlib `metrics` module.
//!
//! Exposes `/metrics` on the configured address (default :9090).
//! All counters, gauges, and histograms are created via `MetricsManager`.

use hyperi_rustlib::metrics::MetricsManager;
use tracing::info;

/// Application metrics for the transform pipeline.
pub struct TransformMetrics {
    pub events_received: metrics::Counter,
    pub events_produced: metrics::Counter,
    pub events_failed: metrics::Counter,
    pub events_filtered: metrics::Counter,
    pub transform_duration: metrics::Histogram,
    pub batch_size: metrics::Histogram,
    pub scaling_pressure: metrics::Gauge,
}

impl TransformMetrics {
    /// Create all metrics via the rustlib `MetricsManager`.
    pub fn new(manager: &MetricsManager) -> Self {
        Self {
            events_received: manager
                .counter("events_received_total", "Total events consumed from source"),
            events_produced: manager
                .counter("events_produced_total", "Total events produced to sink"),
            events_failed: manager.counter(
                "events_failed_total",
                "Total events that failed VRL transform",
            ),
            events_filtered: manager.counter(
                "events_filtered_total",
                "Total events filtered (dropped) by VRL transform",
            ),
            transform_duration: manager.histogram(
                "transform_duration_seconds",
                "Time spent executing VRL transforms per batch",
            ),
            batch_size: manager
                .histogram("batch_size_events", "Number of events per transform batch"),
            scaling_pressure: manager.gauge(
                "scaling_pressure",
                "KEDA-compatible scaling pressure (0-100)",
            ),
        }
    }
}

/// Start the metrics server on the given address.
pub async fn start_metrics_server(
    manager: &mut MetricsManager,
    address: &str,
) -> crate::Result<()> {
    info!(address, "starting metrics server");
    manager
        .start_server(address)
        .await
        .map_err(|e| crate::Error::Health(format!("metrics server failed: {e}")))
}
