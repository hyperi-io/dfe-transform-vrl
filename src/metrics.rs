// Project:   dfe-transform-vrl
// File:      src/metrics.rs
// Purpose:   Standardised DFE metrics via rustlib MetricsManager + dfe_groups
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics using the DFE metrics standard.
//!
//! Three layers:
//! - **Layer 1 (DfeMetrics):** Platform-wide `dfe_*` metrics (records, transport, scaling)
//! - **Layer 2 (`dfe_groups`):** Common metric groups (`AppMetrics`, `ConsumerMetrics`, etc.)
//! - **Layer 3 (app-specific):** `dfe_transform_vrl_*` metrics unique to this service
//!
//! Namespace: `dfe_transform_vrl` (set on `MetricsManager`).

use hyperi_rustlib::metrics::dfe_groups::{
    AppMetrics, BackpressureMetrics, ConsumerMetrics, EnrichmentMetrics, SinkMetrics,
};
use hyperi_rustlib::metrics::{DfeMetrics, MetricsManager};

/// All metrics for the transform pipeline, organised by layer.
pub struct TransformMetrics {
    // Layer 1: Platform standard (dfe_*)
    pub dfe: Option<DfeMetrics>,

    // Layer 2: Common metric groups (dfe_transform_vrl_*)
    pub app: Option<AppMetrics>,
    pub consumer: Option<ConsumerMetrics>,
    pub sink: Option<SinkMetrics>,
    pub backpressure: Option<BackpressureMetrics>,
    pub enrichment: Option<EnrichmentMetrics>,

    // Layer 3: App-specific (dfe_transform_vrl_*)
    pub execute_duration: metrics::Histogram,
    pub deserialise_duration: metrics::Histogram,
    pub serialise_duration: metrics::Histogram,
    pub batch_duration: metrics::Histogram,
    pub records_error: metrics::Counter,
    pub records_format: metrics::Counter,
    pub programs_loaded: metrics::Gauge,
    pub abort_total: metrics::Counter,
    pub batch_size: metrics::Histogram,
    pub enrichment_table_rows: metrics::Gauge,
    pub events_per_second: metrics::Gauge,
    pub enrichment_reload_total: metrics::Counter,
    pub enrichment_reload_duration: metrics::Histogram,
    pub enrichment_last_reload_timestamp: metrics::Gauge,
}

impl TransformMetrics {
    /// Create all metrics via the rustlib `MetricsManager`.
    ///
    /// The `MetricsManager` must be created with namespace `"dfe_transform_vrl"`
    /// so all registered metrics are prefixed correctly.
    pub fn new(manager: &MetricsManager, version: &str, commit: &str) -> Self {
        let dfe = DfeMetrics::register(manager);

        let app = AppMetrics::new(manager, version, commit);
        let consumer = ConsumerMetrics::new(manager);
        let sink = SinkMetrics::new(manager);
        let backpressure = BackpressureMetrics::new(manager);
        let enrichment = EnrichmentMetrics::new(manager);

        Self {
            dfe: Some(dfe),
            app: Some(app),
            consumer: Some(consumer),
            sink: Some(sink),
            backpressure: Some(backpressure),
            enrichment: Some(enrichment),

            execute_duration: manager.histogram(
                "execute_duration_seconds",
                "VRL execution time per batch (excluding deser/ser)",
            ),
            deserialise_duration: manager.histogram(
                "deserialise_duration_seconds",
                "Deserialisation time per batch",
            ),
            serialise_duration: manager
                .histogram("serialise_duration_seconds", "Serialisation time per batch"),
            batch_duration: manager.histogram(
                "batch_duration_seconds",
                "End-to-end batch latency (consume to commit)",
            ),
            records_error: manager.counter(
                "records_error_total",
                "Records that failed processing, by stage",
            ),
            records_format: manager.counter(
                "records_format_total",
                "Records received by detected format",
            ),
            programs_loaded: manager.gauge("programs_loaded", "Active VRL program count"),
            abort_total: manager.counter("abort_total", "Events dropped by VRL abort"),
            batch_size: manager.histogram("batch_size", "Events per transform batch"),
            enrichment_table_rows: manager
                .gauge("enrichment_table_rows", "Rows loaded per enrichment table"),
            events_per_second: manager.gauge(
                "events_per_second",
                "Instantaneous throughput (produced events / batch duration)",
            ),
            enrichment_reload_total: manager.counter(
                "enrichment_reload_total",
                "Enrichment table reload attempts",
            ),
            enrichment_reload_duration: manager.histogram(
                "enrichment_reload_duration_seconds",
                "Enrichment table reload latency",
            ),
            enrichment_last_reload_timestamp: manager.gauge(
                "enrichment_table_last_reload_timestamp",
                "Unix timestamp of last successful enrichment table reload",
            ),
        }
    }

    /// Record a deserialise error.
    #[inline]
    pub fn record_deser_error(&self) {
        self.records_error.increment(1);
        // Labelled counter for stage breakdown
        metrics::counter!(
            "dfe_transform_vrl_records_error_total",
            "stage" => "deserialise"
        )
        .increment(1);
    }

    /// Record a VRL transform error.
    #[inline]
    pub fn record_transform_error(&self) {
        self.records_error.increment(1);
        metrics::counter!(
            "dfe_transform_vrl_records_error_total",
            "stage" => "transform"
        )
        .increment(1);
    }

    /// Record a produce error.
    #[inline]
    pub fn record_produce_error(&self) {
        self.records_error.increment(1);
        metrics::counter!(
            "dfe_transform_vrl_records_error_total",
            "stage" => "produce"
        )
        .increment(1);
    }

    /// Record the detected format for a batch of records.
    #[inline]
    pub fn record_format(&self, format: &str, count: u64) {
        metrics::counter!(
            "dfe_transform_vrl_records_format_total",
            "format" => format.to_string()
        )
        .increment(count);
    }

    /// Set enrichment table row count for a specific table.
    #[inline]
    #[allow(clippy::cast_precision_loss)]
    pub fn set_enrichment_rows(&self, table: &str, rows: usize) {
        metrics::gauge!(
            "dfe_transform_vrl_enrichment_table_rows",
            "table" => table.to_string()
        )
        .set(rows as f64);
    }

    /// Record an enrichment table reload attempt (success or failure).
    #[inline]
    pub fn record_enrichment_reload(&self, table: &str, duration_secs: f64, success: bool) {
        let result = if success { "success" } else { "error" };
        metrics::counter!(
            "dfe_transform_vrl_enrichment_reload_total",
            "table" => table.to_string(),
            "result" => result
        )
        .increment(1);
        metrics::histogram!(
            "dfe_transform_vrl_enrichment_reload_duration_seconds",
            "table" => table.to_string()
        )
        .record(duration_secs);
        if success {
            metrics::gauge!(
                "dfe_transform_vrl_enrichment_table_last_reload_timestamp",
                "table" => table.to_string()
            )
            .set(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64(),
            );
        }
    }
}

impl Default for TransformMetrics {
    /// Default for tests — no `DfeMetrics` or groups (no global recorder installed).
    fn default() -> Self {
        Self {
            dfe: None,
            app: None,
            consumer: None,
            sink: None,
            backpressure: None,
            enrichment: None,
            execute_duration: metrics::histogram!("dfe_transform_vrl_execute_duration_seconds"),
            deserialise_duration: metrics::histogram!(
                "dfe_transform_vrl_deserialise_duration_seconds"
            ),
            serialise_duration: metrics::histogram!("dfe_transform_vrl_serialise_duration_seconds"),
            batch_duration: metrics::histogram!("dfe_transform_vrl_batch_duration_seconds"),
            records_error: metrics::counter!("dfe_transform_vrl_records_error_total"),
            records_format: metrics::counter!("dfe_transform_vrl_records_format_total"),
            programs_loaded: metrics::gauge!("dfe_transform_vrl_programs_loaded"),
            abort_total: metrics::counter!("dfe_transform_vrl_abort_total"),
            batch_size: metrics::histogram!("dfe_transform_vrl_batch_size"),
            enrichment_table_rows: metrics::gauge!("dfe_transform_vrl_enrichment_table_rows"),
            events_per_second: metrics::gauge!("dfe_transform_vrl_events_per_second"),
            enrichment_reload_total: metrics::counter!("dfe_transform_vrl_enrichment_reload_total"),
            enrichment_reload_duration: metrics::histogram!(
                "dfe_transform_vrl_enrichment_reload_duration_seconds"
            ),
            enrichment_last_reload_timestamp: metrics::gauge!(
                "dfe_transform_vrl_enrichment_table_last_reload_timestamp"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_does_not_panic() {
        // Verifies TransformMetrics::default() works without a global recorder.
        // This is the test code path — no MetricsManager installed.
        let m = TransformMetrics::default();
        assert!(m.dfe.is_none());
        assert!(m.app.is_none());
        assert!(m.consumer.is_none());
        assert!(m.sink.is_none());
        assert!(m.backpressure.is_none());
        assert!(m.enrichment.is_none());
    }

    #[test]
    fn record_deser_error_does_not_panic() {
        let m = TransformMetrics::default();
        m.record_deser_error();
    }

    #[test]
    fn record_transform_error_does_not_panic() {
        let m = TransformMetrics::default();
        m.record_transform_error();
    }

    #[test]
    fn record_produce_error_does_not_panic() {
        let m = TransformMetrics::default();
        m.record_produce_error();
    }

    #[test]
    fn record_format_does_not_panic() {
        let m = TransformMetrics::default();
        m.record_format("json", 10);
        m.record_format("msgpack", 5);
    }

    #[test]
    fn set_enrichment_rows_does_not_panic() {
        let m = TransformMetrics::default();
        m.set_enrichment_rows("geo", 1000);
        m.set_enrichment_rows("services", 0);
    }

    #[test]
    fn histogram_and_gauge_recording_does_not_panic() {
        let m = TransformMetrics::default();
        m.execute_duration.record(0.001);
        m.deserialise_duration.record(0.0005);
        m.serialise_duration.record(0.0003);
        m.batch_duration.record(0.05);
        m.batch_size.record(500.0);
        m.programs_loaded.set(3.0);
        m.abort_total.increment(1);
        m.events_per_second.set(12345.0);
    }
}
