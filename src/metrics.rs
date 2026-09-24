// Project:   dfe-transform-vrl
// File:      src/metrics.rs
// Purpose:   Standardised DFE metrics via scalo MetricsManager + metric groups
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics using the DFE metrics standard.
//!
//! Three layers:
//! - **Layer 1 (`ServiceMetrics`):** Platform-wide `dfe_*` metrics (records, transport, scaling)
//! - **Layer 2 (`metrics::groups`):** Common metric groups (`AppMetrics`, `ConsumerMetrics`, etc.)
//! - **Layer 3 (app-specific):** metrics unique to this service
//!
//! Metric names are emitted BARE -- the `MetricsManager` namespace (the app
//! name `dfe-transform-vrl` -> `dfe_transform_vrl_`) prepends the prefix ONCE
//! via the global recorder's prefix layer. App code MUST pass bare segment
//! names (e.g. `records_error_total`, not `dfe_transform_vrl_records_error_total`)
//! or the prefix would double up. Per-app differentiation in the platform is by
//! LABEL (Prometheus job/pod), never the metric name.

use std::sync::Arc;
use std::time::Duration;

use scalo::memory::MemoryGuard;
use scalo::metrics::groups::{
    AppMetrics, BackpressureMetrics, ConsumerMetrics, EnrichmentMetrics, SinkMetrics,
};
use scalo::metrics::{MetricsManager, ServiceMetrics};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// How often the memory gauges are refreshed from the memory guard.
const MEMORY_GAUGE_INTERVAL: Duration = Duration::from_secs(1);

/// All metrics for the transform pipeline, organised by layer.
pub struct TransformMetrics {
    // Layer 1: Platform standard (dfe_*)
    pub dfe: Option<ServiceMetrics>,

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
    /// Create all metrics via the scalo `MetricsManager`.
    ///
    /// The `MetricsManager` namespace is the app name (`dfe-transform-vrl` ->
    /// `dfe_transform_vrl_`); the prefix layer prepends it once to every bare
    /// name registered here.
    pub fn new(manager: &MetricsManager, version: &str, commit: &str) -> Self {
        let dfe = ServiceMetrics::register(manager);

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
    ///
    /// Each error lands in one `stage` series only. An unlabelled increment as
    /// well would be a second series under the same name, and a `sum()` across
    /// labels would count the error twice.
    #[inline]
    pub fn record_deser_error(&self) {
        // Bare name -- the namespace prefix layer prepends `dfe_transform_vrl_` once.
        metrics::counter!(
            "records_error_total",
            "stage" => "deserialise"
        )
        .increment(1);
    }

    /// Record a VRL transform error.
    #[inline]
    pub fn record_transform_error(&self) {
        metrics::counter!(
            "records_error_total",
            "stage" => "transform"
        )
        .increment(1);
    }

    /// Record a produce error.
    #[inline]
    pub fn record_produce_error(&self) {
        metrics::counter!(
            "records_error_total",
            "stage" => "produce"
        )
        .increment(1);
    }

    /// Record the detected format for a batch of records.
    #[inline]
    pub fn record_format(&self, format: &str, count: u64) {
        metrics::counter!(
            "records_format_total",
            "format" => format.to_string()
        )
        .increment(count);
    }

    /// Set enrichment table row count for a specific table.
    #[inline]
    #[allow(clippy::cast_precision_loss)]
    pub fn set_enrichment_rows(&self, table: &str, rows: usize) {
        metrics::gauge!(
            "enrichment_table_rows",
            "table" => table.to_string()
        )
        .set(rows as f64);
    }

    /// Record an enrichment table reload attempt (success or failure).
    #[inline]
    pub fn record_enrichment_reload(&self, table: &str, duration_secs: f64, success: bool) {
        let result = if success { "success" } else { "error" };
        metrics::counter!(
            "enrichment_reload_total",
            "table" => table.to_string(),
            "result" => result
        )
        .increment(1);
        metrics::histogram!(
            "enrichment_reload_duration_seconds",
            "table" => table.to_string()
        )
        .record(duration_secs);
        if success {
            metrics::gauge!(
                "enrichment_table_last_reload_timestamp",
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

    /// Copy the memory guard's usage and limit into the `memory_used_bytes`
    /// and `memory_limit_bytes` gauges.
    #[inline]
    pub fn record_memory(&self, guard: &MemoryGuard) {
        if let Some(ref app) = self.app {
            app.set_memory(guard.current_bytes(), guard.limit_bytes());
        }
    }
}

/// Refresh the memory gauges from `guard` once a second until `shutdown` is
/// cancelled.
///
/// A task of its own because the pipeline's scaling ticker runs only on the
/// bus transport with scaling enabled, and these gauges are wanted on both.
pub fn spawn_memory_gauge_task(
    transform_metrics: Arc<TransformMetrics>,
    guard: Arc<MemoryGuard>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(MEMORY_GAUGE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = tick.tick() => transform_metrics.record_memory(&guard),
            }
        }
    })
}

impl Default for TransformMetrics {
    /// Default for tests — no `ServiceMetrics` or groups (no global recorder
    /// installed). Bare names match the `new()` registration path; the
    /// namespace prefix layer (absent in tests) is what would prepend
    /// `dfe_transform_vrl_` in production.
    fn default() -> Self {
        Self {
            dfe: None,
            app: None,
            consumer: None,
            sink: None,
            backpressure: None,
            enrichment: None,
            execute_duration: metrics::histogram!("execute_duration_seconds"),
            deserialise_duration: metrics::histogram!("deserialise_duration_seconds"),
            serialise_duration: metrics::histogram!("serialise_duration_seconds"),
            batch_duration: metrics::histogram!("batch_duration_seconds"),
            records_format: metrics::counter!("records_format_total"),
            programs_loaded: metrics::gauge!("programs_loaded"),
            abort_total: metrics::counter!("abort_total"),
            batch_size: metrics::histogram!("batch_size"),
            enrichment_table_rows: metrics::gauge!("enrichment_table_rows"),
            events_per_second: metrics::gauge!("events_per_second"),
            enrichment_reload_total: metrics::counter!("enrichment_reload_total"),
            enrichment_reload_duration: metrics::histogram!("enrichment_reload_duration_seconds"),
            enrichment_last_reload_timestamp: metrics::gauge!(
                "enrichment_table_last_reload_timestamp"
            ),
        }
    }
}

/// A recorder for tests that reads back what production code wrote, which
/// scalo's write-only metric handles cannot.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
pub(crate) mod capture {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    /// Every counter by its name and labels, as a Prometheus series is keyed,
    /// every gauge's last value by name, and every histogram key registered.
    #[derive(Default)]
    pub struct Capture {
        counters: Mutex<HashMap<metrics::Key, Arc<AtomicU64>>>,
        gauges: Mutex<HashMap<String, Arc<AtomicU64>>>,
        histograms: Mutex<Vec<metrics::Key>>,
    }

    /// The labels of `key` as owned pairs, sorted.
    fn labels_of(key: &metrics::Key) -> Vec<(String, String)> {
        let mut labels: Vec<(String, String)> = key
            .labels()
            .map(|l| (l.key().to_string(), l.value().to_string()))
            .collect();
        labels.sort();
        labels
    }

    /// Whether `key` is `name` carrying exactly `labels`.
    fn is_series(key: &metrics::Key, name: &str, labels: &[(&str, &str)]) -> bool {
        let mut want: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        want.sort();
        key.name() == name && labels_of(key) == want
    }

    impl Capture {
        /// Last value of the gauge `name`.
        pub fn gauge(&self, name: &str) -> Option<f64> {
            self.gauges
                .lock()
                .unwrap()
                .get(name)
                .map(|g| f64::from_bits(g.load(Ordering::Acquire)))
        }

        /// Value of the counter series `name` with exactly `labels`.
        pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
            self.counters
                .lock()
                .unwrap()
                .iter()
                .find(|(key, _)| is_series(key, name, labels))
                .map(|(_, cell)| cell.load(Ordering::Acquire))
        }

        /// Whether the histogram series `name` with exactly `labels` was registered.
        pub fn has_histogram(&self, name: &str, labels: &[(&str, &str)]) -> bool {
            self.histograms
                .lock()
                .unwrap()
                .iter()
                .any(|key| is_series(key, name, labels))
        }

        /// Every counter and histogram series carrying `label` = `value`.
        pub fn series_labelled(&self, label: &str, value: &str) -> Vec<String> {
            let counters = self.counters.lock().unwrap();
            let histograms = self.histograms.lock().unwrap();
            counters
                .keys()
                .chain(histograms.iter())
                .filter(|key| labels_of(key).iter().any(|(k, v)| k == label && v == value))
                .map(|key| key.name().to_string())
                .collect()
        }
    }

    impl metrics::Recorder for Capture {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            let cell = Arc::clone(
                self.counters
                    .lock()
                    .unwrap()
                    .entry(key.clone())
                    .or_default(),
            );
            metrics::Counter::from_arc(cell)
        }

        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            let cell = Arc::clone(
                self.gauges
                    .lock()
                    .unwrap()
                    .entry(key.name().to_string())
                    .or_default(),
            );
            metrics::Gauge::from_arc(cell)
        }

        fn register_histogram(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            self.histograms.lock().unwrap().push(key.clone());
            metrics::Histogram::noop()
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::capture::Capture;
    use super::*;

    use scalo::memory::{MemoryGuardConfig, UsageSource};

    const FIXTURE_USED_BYTES: u64 = 123_456_789;
    const FIXTURE_LIMIT_BYTES: u64 = 1 << 30;

    /// Transform metrics whose gauges land in `capture`, plus a guard reading a
    /// fixture cgroup that reports [`FIXTURE_USED_BYTES`] in use.
    fn captured_metrics_and_guard(
        capture: &Capture,
        namespace: &str,
    ) -> (TransformMetrics, MemoryGuard, tempfile::TempDir) {
        let manager = MetricsManager::new(namespace);
        let m = metrics::with_local_recorder(capture, || {
            TransformMetrics::new(&manager, "0.1.0", "eeee")
        });
        let cgroup = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            cgroup.path().join("memory.current"),
            format!("{FIXTURE_USED_BYTES}\n"),
        )
        .expect("write memory.current");
        let guard = MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: FIXTURE_LIMIT_BYTES,
                ..MemoryGuardConfig::default()
            },
            UsageSource::CgroupV2(cgroup.path().to_path_buf()),
        );
        (m, guard, cgroup)
    }

    #[test]
    fn record_memory_writes_the_guard_reading_to_the_app_gauges() {
        let capture = Capture::default();
        let (m, guard, _cgroup) = captured_metrics_and_guard(&capture, "test_record_memory");
        assert_eq!(capture.gauge("memory_used_bytes"), Some(0.0));

        m.record_memory(&guard);

        assert_eq!(
            capture.gauge("memory_used_bytes"),
            Some(123_456_789.0),
            "memory_used_bytes must carry the guard's current_bytes"
        );
        assert_eq!(
            capture.gauge("memory_limit_bytes"),
            Some(1_073_741_824.0),
            "memory_limit_bytes must carry the guard's limit_bytes"
        );
    }

    #[tokio::test]
    async fn memory_gauge_task_writes_the_gauges_and_stops_on_shutdown() {
        let capture = Capture::default();
        let (m, guard, _cgroup) = captured_metrics_and_guard(&capture, "test_memory_gauge_task");
        let shutdown = CancellationToken::new();
        let task = spawn_memory_gauge_task(Arc::new(m), Arc::new(guard), shutdown.clone());

        // The interval's first tick fires at once; allow a loaded host 5s.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while capture.gauge("memory_used_bytes") != Some(123_456_789.0) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the task never wrote memory_used_bytes"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the task must stop once shutdown is cancelled")
            .expect("the task must not panic");
    }

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

    #[test]
    fn new_wires_layer1_and_layer2_groups() {
        // Construct a MetricsManager — exercises the full registration path
        // through Layer 1 (ServiceMetrics), Layer 2 groups, and Layer 3 histograms.
        let manager = MetricsManager::new("test_dfe_transform_vrl");
        let m = TransformMetrics::new(&manager, "0.1.0", "abc1234");

        assert!(m.dfe.is_some());
        assert!(m.app.is_some());
        assert!(m.consumer.is_some());
        assert!(m.sink.is_some());
        assert!(m.backpressure.is_some());
        assert!(m.enrichment.is_some());

        // Layer 3 histograms/counters/gauges must be usable
        m.execute_duration.record(0.01);
        m.deserialise_duration.record(0.005);
        m.serialise_duration.record(0.003);
        m.batch_duration.record(0.1);
        m.batch_size.record(1000.0);
        m.programs_loaded.set(1.0);
        m.abort_total.increment(1);
        m.events_per_second.set(42_000.0);
        m.enrichment_table_rows.set(500.0);
        m.enrichment_reload_total.increment(1);
        m.enrichment_reload_duration.record(0.5);
        m.enrichment_last_reload_timestamp.set(1_234_567_890.0);
    }

    #[test]
    fn enrichment_reload_records_success_and_failure() {
        let manager = MetricsManager::new("test_reload_metrics");
        let m = TransformMetrics::new(&manager, "0.1.0", "def5678");

        // Exercises the timestamp-recording branch (success path)
        m.record_enrichment_reload("table_a", 0.042, true);
        // Exercises the failure branch
        m.record_enrichment_reload("table_b", 0.999, false);
    }

    #[test]
    fn set_enrichment_rows_covers_gauge_label_path() {
        let manager = MetricsManager::new("test_set_rows");
        let m = TransformMetrics::new(&manager, "0.1.0", "aaaa");
        m.set_enrichment_rows("geo", 10_000);
        m.set_enrichment_rows("services", 50);
        m.set_enrichment_rows("empty", 0);
    }

    /// Counts one named counter across every label set, as a `sum()` over the
    /// name reads it.
    struct CountingRecorder {
        name: &'static str,
        hits: Arc<AtomicU64>,
    }

    impl metrics::Recorder for CountingRecorder {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            if key.name() == self.name {
                metrics::Counter::from_arc(Arc::clone(&self.hits))
            } else {
                metrics::Counter::noop()
            }
        }

        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// Run `f` with a thread-local recorder counting `name`.
    fn counted(name: &'static str, f: impl FnOnce()) -> u64 {
        let hits = Arc::new(AtomicU64::new(0));
        let recorder = CountingRecorder {
            name,
            hits: Arc::clone(&hits),
        };
        metrics::with_local_recorder(&recorder, f);
        hits.load(Ordering::Acquire)
    }

    #[test]
    fn each_error_counts_once_across_records_error_total() {
        let manager = MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        let hits = counted("records_error_total", || {
            let m = TransformMetrics::new(&manager, "0.1.0", "eeee");
            m.record_deser_error();
            m.record_transform_error();
            m.record_produce_error();
        });
        assert_eq!(hits, 3, "three errors read as three across every stage");
    }

    #[test]
    fn stage_error_counters_all_three_stages() {
        let manager = MetricsManager::new("test_stage_errors");
        let m = TransformMetrics::new(&manager, "0.1.0", "bbbb");
        m.record_deser_error();
        m.record_transform_error();
        m.record_produce_error();
    }

    #[test]
    fn record_format_covers_all_formats() {
        let manager = MetricsManager::new("test_formats");
        let m = TransformMetrics::new(&manager, "0.1.0", "cccc");
        m.record_format("json", 100);
        m.record_format("msgpack", 50);
        m.record_format("auto", 0);
    }
}
