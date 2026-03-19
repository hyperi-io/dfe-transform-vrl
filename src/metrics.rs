// Project:   dfe-transform-vrl
// File:      src/metrics.rs
// Purpose:   Prometheus metrics via rustlib MetricsManager + DfeMetrics
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics using hyperi-rustlib `metrics` module.
//!
//! Dual-emit: existing `transform_vrl_*` metrics (project-specific) alongside
//! standard `dfe_*` metrics (platform-wide via `DfeMetrics`). The `dfe_*` names
//! will eventually replace the project-specific names once all dashboards migrate.

use hyperi_rustlib::metrics::{DfeMetrics, MetricsManager};
use tracing::info;

/// Application metrics for the transform pipeline.
///
/// Dual-emits both project-specific metrics (`transform_vrl_*` via `MetricsManager`)
/// and platform-standard metrics (`dfe_*` via `DfeMetrics`). Keep both during
/// transition — remove project-specific names once dashboards are migrated.
pub struct TransformMetrics {
    // Project-specific (existing — do not remove yet)
    pub events_received: metrics::Counter,
    pub events_produced: metrics::Counter,
    pub events_failed: metrics::Counter,
    pub events_filtered: metrics::Counter,
    pub transform_duration: metrics::Histogram,
    pub batch_size: metrics::Histogram,
    pub scaling_pressure: metrics::Gauge,

    // Platform-standard (new — dual-emit alongside existing)
    pub dfe: Option<DfeMetrics>,
}

impl TransformMetrics {
    /// Create all metrics via the rustlib `MetricsManager`.
    ///
    /// In production, `DfeMetrics::register()` is called after the `MetricsManager`
    /// installs the global recorder. In tests, `dfe` is `None`.
    pub fn new(manager: &MetricsManager) -> Self {
        let dfe = DfeMetrics::register();

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
            dfe: Some(dfe),
        }
    }
}

impl Default for TransformMetrics {
    /// Default for tests — no `DfeMetrics` (no global recorder installed).
    fn default() -> Self {
        Self {
            events_received: metrics::counter!("events_received_total"),
            events_produced: metrics::counter!("events_produced_total"),
            events_failed: metrics::counter!("events_failed_total"),
            events_filtered: metrics::counter!("events_filtered_total"),
            transform_duration: metrics::histogram!("transform_duration_seconds"),
            batch_size: metrics::histogram!("batch_size_events"),
            scaling_pressure: metrics::gauge!("scaling_pressure"),
            dfe: None,
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
