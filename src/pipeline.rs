// Project:   dfe-transform-vrl
// File:      src/pipeline.rs
// Purpose:   Event processing pipeline — consume, transform, produce
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Event processing pipeline.
//!
//! Orchestrates the data flow:
//! 1. Consume batch from source transport
//! 2. Deserialise to VRL Value (auto-sensing format via rustlib)
//! 3. Run VRL transforms in-process
//! 4. Serialise back to original format
//! 5. Produce to sink transport
//! 6. Commit consumer offsets after delivery confirmation
//!
//! The pipeline is generic over the `Transport` trait, allowing:
//! - `KafkaTransport` in production
//! - `MemoryTransport` in unit tests (no Kafka broker needed)
//!
//! Hot-reloadable config (`batch_size`, `batch_timeout_ms`, `key_field`,
//! `scaling_pressure_threshold`) is read from `SharedConfig<HotConfig>`
//! at the start of each batch. See [`crate::config::hot`] for the full
//! classification of hot vs restart-required fields.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hyperi_rustlib::config::shared::SharedConfig;
use hyperi_rustlib::logger::{log_debounced, log_sampled, log_state_change, security};
use hyperi_rustlib::memory::MemoryGuard;
use hyperi_rustlib::transport::{PayloadFormat, SendResult, Transport};
use tracing::{debug, error, info, warn};

// Per-site log spam guards
static DESER_ERRORS: AtomicU64 = AtomicU64::new(0);
static VRL_ERRORS: AtomicU64 = AtomicU64::new(0);
static PRODUCE_ERRORS: AtomicU64 = AtomicU64::new(0);
static BACKPRESSURE_ACTIVE: AtomicBool = AtomicBool::new(false);
static BATCH_ERROR_TS: AtomicU64 = AtomicU64::new(0);
static MEMORY_PRESSURE_ACTIVE: AtomicBool = AtomicBool::new(false);
use vrl::compiler::Program;
use vrl::value::Value;

use crate::config::Config;
use crate::config::hot::HotConfig;
use crate::engine::runner::run_vrl;
use crate::kafka;
use crate::metrics::TransformMetrics;

/// Run the transform pipeline with Kafka transports (production entry point).
pub async fn run(
    config: &Config,
    program: Arc<Program>,
    hot_config: SharedConfig<HotConfig>,
    transform_metrics: &TransformMetrics,
    ready_flag: Arc<AtomicBool>,
    memory_guard: Arc<MemoryGuard>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    worker_pool: Option<Arc<hyperi_rustlib::worker::AdaptiveWorkerPool>>,
) -> crate::Result<()> {
    let consumer_config = kafka::build_consumer_config(&config.source);
    let producer_config = kafka::build_producer_config(&config.sink, &config.pipeline.name);
    let payload_format = kafka::parse_format(&config.source.format);

    info!(
        pipeline = %config.pipeline.name,
        source_topics = ?config.source.topics,
        sink_topic = %config.sink.topic,
        format = %config.source.format,
        batch_size = config.pipeline.batch_size,
        "initialising pipeline"
    );

    let consumer = kafka::create_consumer(&consumer_config).await?;
    let producer = kafka::create_producer(&producer_config).await?;

    run_with_transport(
        &consumer,
        &producer,
        program,
        hot_config,
        payload_format,
        transform_metrics,
        ready_flag,
        memory_guard,
        shutdown_rx,
        worker_pool,
    )
    .await
}

/// Run the transform pipeline with any `Transport` implementation.
///
/// Generic over `T: Transport` so the same pipeline logic works with
/// `KafkaTransport` (production) and `MemoryTransport` (tests).
///
/// Hot-reloadable fields (`batch_size`, `batch_timeout_ms`, `key_field`)
/// are read from `SharedConfig<HotConfig>` at the start of each batch.
#[allow(clippy::too_many_arguments)]
pub async fn run_with_transport<T: Transport>(
    consumer: &T,
    producer: &T,
    program: Arc<Program>,
    hot_config: SharedConfig<HotConfig>,
    payload_format: PayloadFormat,
    transform_metrics: &TransformMetrics,
    ready_flag: Arc<AtomicBool>,
    memory_guard: Arc<MemoryGuard>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    worker_pool: Option<Arc<hyperi_rustlib::worker::AdaptiveWorkerPool>>,
) -> crate::Result<()> {
    ready_flag.store(true, Ordering::Release);
    if let Some(ref dfe) = transform_metrics.dfe {
        dfe.pipeline_ready(true);
    }
    info!("pipeline ready — entering event loop");

    loop {
        // Memory pressure check — stall consuming until pressure drops
        if memory_guard.under_pressure() {
            if log_state_change(&MEMORY_PRESSURE_ACTIVE, true) {
                warn!(
                    current_bytes = memory_guard.current_bytes(),
                    limit_bytes = memory_guard.limit_bytes(),
                    "memory pressure HIGH — pausing consumer"
                );
            }
            ready_flag.store(false, Ordering::Release);
            if let Some(ref dfe) = transform_metrics.dfe {
                dfe.pipeline_ready(false);
                dfe.scaling_memory_pressure(memory_guard.pressure_ratio());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if log_state_change(&MEMORY_PRESSURE_ACTIVE, false) {
            info!("memory pressure recovered — resuming consumer");
            ready_flag.store(true, Ordering::Release);
            if let Some(ref dfe) = transform_metrics.dfe {
                dfe.pipeline_ready(true);
                dfe.scaling_memory_pressure(memory_guard.pressure_ratio());
            }
        }

        // Read hot config each iteration — picks up runtime changes
        let hot = hot_config.get();
        let batch_timeout = Duration::from_millis(hot.batch_timeout_ms);

        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("shutdown signal received, draining pipeline");
                break;
            }
            result = process_batch(
                consumer,
                producer,
                &program,
                &hot.key_field,
                hot.batch_size,
                batch_timeout,
                payload_format,
                transform_metrics,
                &memory_guard,
                &worker_pool,
            ) => {
                if let Err(e) = result
                    && log_debounced(&BATCH_ERROR_TS, 5000)
                {
                    error!(error = %e, "batch processing error (max 1/5s)");
                }
            }
        }
    }

    ready_flag.store(false, Ordering::Release);
    if let Some(ref dfe) = transform_metrics.dfe {
        dfe.pipeline_ready(false);
    }
    info!("closing transports");
    let _ = consumer.close().await;
    let _ = producer.close().await;

    Ok(())
}

/// Process a single batch: consume → transform → produce → commit.
///
/// Uses `batch_timeout` to bound how long we wait for a full batch. If the
/// timeout fires before `batch_size` messages arrive, we process what we have.
/// This prevents latency spikes at low volume.
#[allow(
    clippy::too_many_arguments,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]
async fn process_batch<T: Transport>(
    consumer: &T,
    producer: &T,
    program: &Program,
    key_field: &str,
    batch_size: usize,
    batch_timeout: Duration,
    payload_format: PayloadFormat,
    transform_metrics: &TransformMetrics,
    memory_guard: &MemoryGuard,
    worker_pool: &Option<Arc<hyperi_rustlib::worker::AdaptiveWorkerPool>>,
) -> crate::Result<()> {
    let messages = match tokio::time::timeout(batch_timeout, consumer.recv(batch_size)).await {
        Ok(result) => result.map_err(|e| crate::Error::Kafka(format!("consume error: {e}")))?,
        Err(_elapsed) => {
            // Timeout — no messages arrived within the batch window
            return Ok(());
        }
    };

    if messages.is_empty() {
        return Ok(());
    }

    // Track batch memory in the guard
    let batch_bytes: u64 = messages.iter().map(|m| m.payload.len() as u64).sum();
    memory_guard.add_bytes(batch_bytes);

    let batch_start = Instant::now();
    let batch_len = messages.len();

    // Layer 1: DfeMetrics (platform)
    if let Some(ref dfe) = transform_metrics.dfe {
        dfe.records_received(batch_len as u64);
    }
    // Layer 2: AppMetrics (common group)
    if let Some(ref app) = transform_metrics.app {
        app.record_received(batch_len as u64);
        app.record_bytes_received(batch_bytes);
    }
    // Layer 3: App-specific
    transform_metrics.batch_size.record(batch_len as f64);
    debug!(count = batch_len, "consumed batch");

    let mut commit_tokens = Vec::with_capacity(batch_len);
    let mut produced_count: u64 = 0;
    let mut produced_bytes: u64 = 0;
    let mut json_count: u64 = 0;
    let mut msgpack_count: u64 = 0;

    // Phase 1: Deserialise all events — parallel via rayon when worker pool available
    let deser_start = Instant::now();

    // Prepare indexed messages for parallel deser
    let indexed_msgs: Vec<(usize, PayloadFormat, &[u8])> = messages
        .iter()
        .enumerate()
        .map(|(idx, msg)| {
            let format = if payload_format == PayloadFormat::Auto {
                msg.format
            } else {
                payload_format
            };
            (idx, format, msg.payload.as_slice())
        })
        .collect();

    // Parallel deserialisation (CPU-bound parsing)
    let deser_results: Vec<Result<(Value, PayloadFormat, usize), (usize, PayloadFormat, String)>> =
        if let Some(pool) = worker_pool {
            pool.process_batch(
                &indexed_msgs,
                |(idx, format, payload)| match deserialize_event(payload, *format) {
                    Ok(v) => Ok((v, *format, *idx)),
                    Err(e) => Err((*idx, *format, e.to_string())),
                },
            )
        } else {
            indexed_msgs
                .iter()
                .map(
                    |(idx, format, payload)| match deserialize_event(payload, *format) {
                        Ok(v) => Ok((v, *format, *idx)),
                        Err(e) => Err((*idx, *format, e.to_string())),
                    },
                )
                .collect()
        };

    // Sequential: separate successes from errors, track metrics + commit tokens
    let mut events: Vec<(Value, PayloadFormat, usize)> = Vec::with_capacity(batch_len);
    for result in deser_results {
        match result {
            Ok(item) => {
                match item.1 {
                    PayloadFormat::Json => json_count += 1,
                    PayloadFormat::MsgPack => msgpack_count += 1,
                    PayloadFormat::Auto => {}
                }
                events.push(item);
            }
            Err((idx, _format, e)) => {
                if log_sampled(&DESER_ERRORS, 1000) {
                    warn!(error = %e, total = DESER_ERRORS.load(Ordering::Relaxed), "deserialise failure (sampled 1/1000)");
                }
                security::input_validation_failure("deserialise", &e, None);
                transform_metrics.record_deser_error();
                commit_tokens.push(messages[idx].token.clone());
            }
        }
    }
    let deser_elapsed = deser_start.elapsed();
    transform_metrics
        .deserialise_duration
        .record(deser_elapsed.as_secs_f64());

    // Record format distribution
    if json_count > 0 {
        transform_metrics.record_format("json", json_count);
    }
    if msgpack_count > 0 {
        transform_metrics.record_format("msgpack", msgpack_count);
    }

    // Phase 2: VRL transform — parallel via rayon when worker pool available
    let vrl_start = Instant::now();

    // Run VRL evaluation in parallel (CPU-bound, program is Sync)
    let vrl_results: Vec<Result<(Value, PayloadFormat, usize), (usize, crate::Error)>> =
        if let Some(pool) = worker_pool {
            pool.process_batch(&events, |(value, format, idx)| {
                let mut value = value.clone();
                match run_vrl(program, &mut value) {
                    Ok(_) => Ok((value, *format, *idx)),
                    Err(e) => Err((*idx, e)),
                }
            })
        } else {
            // Sequential fallback
            events
                .into_iter()
                .map(
                    |(mut value, format, idx)| match run_vrl(program, &mut value) {
                        Ok(_) => Ok((value, format, idx)),
                        Err(e) => Err((idx, e)),
                    },
                )
                .collect()
        };

    // Separate successes from failures (sequential — metrics + commit tokens)
    let mut transformed: Vec<(Value, PayloadFormat, usize)> = Vec::with_capacity(vrl_results.len());
    for result in vrl_results {
        match result {
            Ok(item) => transformed.push(item),
            Err((idx, crate::Error::VrlAbort(ref reason))) => {
                debug!(reason = %reason, "event dropped by VRL abort");
                transform_metrics.abort_total.increment(1);
                if let Some(ref dfe) = transform_metrics.dfe {
                    dfe.records_filtered(1);
                }
                commit_tokens.push(messages[idx].token.clone());
            }
            Err((idx, e)) => {
                if log_sampled(&VRL_ERRORS, 1000) {
                    warn!(error = %e, total = VRL_ERRORS.load(Ordering::Relaxed), "VRL transform error (sampled 1/1000)");
                }
                security::input_validation_failure("vrl_transform", &e.to_string(), None);
                transform_metrics.record_transform_error();
                commit_tokens.push(messages[idx].token.clone());
            }
        }
    }
    let vrl_elapsed = vrl_start.elapsed();
    transform_metrics
        .execute_duration
        .record(vrl_elapsed.as_secs_f64());

    // Phase 3: Serialise + Produce
    let ser_start = Instant::now();
    for (value, format, idx) in &transformed {
        let serialized = serialize_event(value, *format)?;
        let key = extract_key(value, key_field);
        let key_str = key.as_deref().unwrap_or("");
        let ser_bytes = serialized.len() as u64;

        match producer.send(key_str, &serialized).await {
            SendResult::Ok => {
                produced_count += 1;
                produced_bytes += ser_bytes;
                if log_state_change(&BACKPRESSURE_ACTIVE, false) {
                    info!("producer backpressure cleared");
                }
                if let Some(ref dfe) = transform_metrics.dfe {
                    dfe.transport_sent("kafka", 1);
                }
            }
            SendResult::Backpressured => {
                if log_state_change(&BACKPRESSURE_ACTIVE, true) {
                    warn!("producer backpressure active");
                }
                if let Some(ref dfe) = transform_metrics.dfe {
                    dfe.transport_backpressured("kafka", 1);
                }
                if let Some(ref bp) = transform_metrics.backpressure {
                    bp.record_event();
                }
                tokio::task::yield_now().await;
                match producer.send(key_str, &serialized).await {
                    SendResult::Ok => {
                        produced_count += 1;
                        produced_bytes += ser_bytes;
                        if let Some(ref dfe) = transform_metrics.dfe {
                            dfe.transport_sent("kafka", 1);
                        }
                    }
                    other => {
                        if log_sampled(&PRODUCE_ERRORS, 1000) {
                            error!(result = ?other, total = PRODUCE_ERRORS.load(Ordering::Relaxed), "produce failed (sampled 1/1000)");
                        }
                        transform_metrics.record_produce_error();
                        if let Some(ref dfe) = transform_metrics.dfe {
                            dfe.transport_send_errors("kafka", 1);
                        }
                    }
                }
            }
            SendResult::Fatal(e) => {
                error!(error = %e, "fatal produce error");
                transform_metrics.record_produce_error();
                if let Some(ref dfe) = transform_metrics.dfe {
                    dfe.transport_send_errors("kafka", 1);
                }
                return Err(crate::Error::Kafka(format!("produce failed: {e}")));
            }
        }

        commit_tokens.push(messages[*idx].token.clone());
    }
    let ser_elapsed = ser_start.elapsed();
    transform_metrics
        .serialise_duration
        .record(ser_elapsed.as_secs_f64());

    // Layer 1: DfeMetrics
    if let Some(ref dfe) = transform_metrics.dfe {
        dfe.records_delivered(produced_count);
        dfe.transport_send_duration("kafka", ser_elapsed.as_secs_f64());
        let pressure = memory_guard.pressure_ratio();
        dfe.scaling_pressure(pressure * 100.0);
        dfe.scaling_memory_pressure(pressure);
    }

    // Layer 2: AppMetrics + SinkMetrics
    if let Some(ref app) = transform_metrics.app {
        app.record_processed(produced_count);
        app.record_bytes_written(produced_bytes);
        app.set_memory(memory_guard.current_bytes(), memory_guard.limit_bytes());
    }
    if let Some(ref sink) = transform_metrics.sink {
        sink.record_duration("kafka", ser_elapsed.as_secs_f64());
    }

    // Commit offsets
    if !commit_tokens.is_empty() {
        consumer
            .commit(&commit_tokens)
            .await
            .map_err(|e| crate::Error::Kafka(format!("offset commit error: {e}")))?;
        if let Some(ref consumer_metrics) = transform_metrics.consumer {
            consumer_metrics.record_offsets_committed(1);
        }
    }

    // Release tracked memory after batch is fully committed
    memory_guard.release(batch_bytes);

    // End-to-end batch duration + EPS
    let batch_elapsed = batch_start.elapsed();
    transform_metrics
        .batch_duration
        .record(batch_elapsed.as_secs_f64());

    let batch_secs = batch_elapsed.as_secs_f64();
    if batch_secs > 0.0 {
        transform_metrics
            .events_per_second
            .set(produced_count as f64 / batch_secs);
    }

    debug!(
        produced = produced_count,
        elapsed_ms = batch_elapsed.as_millis(),
        deser_ms = deser_elapsed.as_millis(),
        vrl_ms = vrl_elapsed.as_millis(),
        ser_ms = ser_elapsed.as_millis(),
        "batch complete"
    );

    Ok(())
}

/// Deserialise raw bytes to VRL Value using the detected format.
fn deserialize_event(payload: &[u8], format: PayloadFormat) -> crate::Result<Value> {
    match format {
        PayloadFormat::Json => serde_json::from_slice(payload)
            .map_err(|e| crate::Error::Serialisation(format!("JSON deserialise: {e}"))),
        PayloadFormat::MsgPack => rmp_serde::from_slice(payload)
            .map_err(|e| crate::Error::Serialisation(format!("msgpack deserialise: {e}"))),
        PayloadFormat::Auto => {
            let detected = PayloadFormat::detect(payload);
            deserialize_event(payload, detected)
        }
    }
}

/// Serialise VRL Value back to the original format.
fn serialize_event(value: &Value, format: PayloadFormat) -> crate::Result<Vec<u8>> {
    match format {
        PayloadFormat::Json | PayloadFormat::Auto => serde_json::to_vec(value)
            .map_err(|e| crate::Error::Serialisation(format!("JSON serialise: {e}"))),
        PayloadFormat::MsgPack => rmp_serde::to_vec(value)
            .map_err(|e| crate::Error::Serialisation(format!("msgpack serialise: {e}"))),
    }
}

/// Extract a key from the event for Kafka partition routing.
///
/// Supports dot-separated paths (e.g., `.org_id`, `.host.name`, `.meta.tenant_id`).
/// Walks the nested object tree following each path segment.
fn extract_key(value: &Value, key_field: &str) -> Option<String> {
    if key_field.is_empty() {
        return None;
    }

    let path = key_field.strip_prefix('.').unwrap_or(key_field);
    let segments: Vec<&str> = path.split('.').collect();

    let mut current = value;
    for segment in &segments {
        current = current.as_object()?.get(*segment)?;
    }

    Some(match current {
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        other => format!("{other}"),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_json() {
        let json = br#"{"key": "value"}"#;
        let value = deserialize_event(json, PayloadFormat::Json).unwrap();
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("key"), Some(&Value::from("value")));
    }

    #[test]
    fn test_deserialize_msgpack() {
        let data = rmp_serde::to_vec(&serde_json::json!({"x": 42})).unwrap();
        let value = deserialize_event(&data, PayloadFormat::MsgPack).unwrap();
        let obj = value.as_object().unwrap();
        assert!(obj.get("x").is_some());
    }

    #[test]
    fn test_serialize_roundtrip_json() {
        let original = Value::from(serde_json::json!({"a": 1, "b": "hello"}));
        let bytes = serialize_event(&original, PayloadFormat::Json).unwrap();
        let recovered = deserialize_event(&bytes, PayloadFormat::Json).unwrap();
        assert_eq!(
            original.as_object().unwrap().get("a"),
            recovered.as_object().unwrap().get("a"),
        );
    }

    #[test]
    fn test_serialize_roundtrip_msgpack() {
        let original = Value::from(serde_json::json!({"x": 99}));
        let bytes = serialize_event(&original, PayloadFormat::MsgPack).unwrap();
        let recovered = deserialize_event(&bytes, PayloadFormat::MsgPack).unwrap();
        assert!(recovered.as_object().unwrap().get("x").is_some());
    }

    #[test]
    fn test_extract_key() {
        let value = Value::from(serde_json::json!({"org_id": "abc123"}));
        assert_eq!(extract_key(&value, ".org_id"), Some("abc123".to_string()));
        assert_eq!(extract_key(&value, "org_id"), Some("abc123".to_string()));
    }

    #[test]
    fn test_extract_key_empty() {
        let value = Value::from(serde_json::json!({"x": 1}));
        assert_eq!(extract_key(&value, ""), None);
    }

    #[test]
    fn test_extract_key_missing() {
        let value = Value::from(serde_json::json!({"x": 1}));
        assert_eq!(extract_key(&value, ".missing"), None);
    }

    #[test]
    fn test_extract_key_nested() {
        let value = Value::from(serde_json::json!({"host": {"name": "prod-web-01"}}));
        assert_eq!(
            extract_key(&value, ".host.name"),
            Some("prod-web-01".to_string())
        );
    }

    #[test]
    fn test_extract_key_deeply_nested() {
        let value = Value::from(serde_json::json!({"a": {"b": {"c": "deep"}}}));
        assert_eq!(extract_key(&value, ".a.b.c"), Some("deep".to_string()));
    }

    #[test]
    fn test_extract_key_nested_missing() {
        let value = Value::from(serde_json::json!({"host": {"ip": "1.2.3.4"}}));
        assert_eq!(extract_key(&value, ".host.name"), None);
    }

    #[test]
    fn test_auto_detect_json() {
        let json = br#"{"key": "value"}"#;
        let value = deserialize_event(json, PayloadFormat::Auto).unwrap();
        assert!(value.as_object().is_some());
    }

    #[test]
    fn test_auto_detect_msgpack() {
        let data = rmp_serde::to_vec(&serde_json::json!({"y": true})).unwrap();
        let value = deserialize_event(&data, PayloadFormat::Auto).unwrap();
        assert!(value.as_object().is_some());
    }

    /// Prove VRL evaluation runs on multiple threads via the worker pool.
    #[test]
    fn test_parallel_vrl_uses_multiple_threads() {
        use std::sync::Arc;

        let pool_config = hyperi_rustlib::worker::WorkerPoolConfig {
            min_threads: 4,
            max_threads: 4,
            ..Default::default()
        };
        let pool = Arc::new(hyperi_rustlib::worker::AdaptiveWorkerPool::new(pool_config));

        // Compile a simple VRL program
        let fns = vrl::stdlib::all();
        let program = vrl::compiler::compile(r#".processed = true"#, &fns)
            .expect("VRL compile failed")
            .program;

        // Create 40 events
        let events: Vec<(Value, PayloadFormat, usize)> = (0..40)
            .map(|i| {
                let v = Value::from(serde_json::json!({"id": i, "data": "test"}));
                (v, PayloadFormat::Json, i)
            })
            .collect();

        let thread_ids = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        let tids = thread_ids.clone();

        let results: Vec<Result<(Value, PayloadFormat, usize), (usize, crate::Error)>> = pool
            .process_batch(&events, |(value, format, idx)| {
                tids.lock().unwrap().insert(std::thread::current().id());
                let mut value = value.clone();
                // Simulate CPU work
                std::thread::sleep(std::time::Duration::from_millis(1));
                match crate::engine::runner::run_vrl(&program, &mut value) {
                    Ok(_) => Ok((value, *format, *idx)),
                    Err(e) => Err((*idx, e)),
                }
            });

        assert_eq!(results.len(), 40);
        assert!(
            results.iter().all(Result::is_ok),
            "all VRL evaluations should succeed"
        );

        let unique_threads = thread_ids.lock().unwrap().len();
        assert!(
            unique_threads > 1,
            "Expected multiple threads for VRL eval, got {unique_threads}"
        );
    }

    /// Prove mixed VRL success/failure (abort + error) are handled correctly in parallel.
    #[test]
    fn test_parallel_vrl_mixed_success_abort_error() {
        use std::sync::Arc;

        let pool_config = hyperi_rustlib::worker::WorkerPoolConfig {
            min_threads: 2,
            max_threads: 2,
            ..Default::default()
        };
        let pool = Arc::new(hyperi_rustlib::worker::AdaptiveWorkerPool::new(pool_config));

        // VRL program that aborts when .drop == true
        let fns = vrl::stdlib::all();
        let program = vrl::compiler::compile(
            r#"if .drop == true { abort } else { .processed = true }"#,
            &fns,
        )
        .expect("VRL compile failed")
        .program;

        let events: Vec<(Value, PayloadFormat, usize)> = (0..10)
            .map(|i| {
                let should_drop = i % 3 == 0; // items 0, 3, 6, 9 abort
                let v = Value::from(serde_json::json!({"id": i, "drop": should_drop}));
                (v, PayloadFormat::Json, i)
            })
            .collect();

        let results: Vec<Result<(Value, PayloadFormat, usize), (usize, crate::Error)>> = pool
            .process_batch(&events, |(value, format, idx)| {
                let mut value = value.clone();
                match crate::engine::runner::run_vrl(&program, &mut value) {
                    Ok(_) => Ok((value, *format, *idx)),
                    Err(e) => Err((*idx, e)),
                }
            });

        assert_eq!(results.len(), 10);

        let successes: Vec<_> = results.iter().filter(|r| r.is_ok()).collect();
        let aborts: Vec<_> = results
            .iter()
            .filter(|r| matches!(r, Err((_, crate::Error::VrlAbort(_)))))
            .collect();

        // Items 1,2,4,5,7,8 succeed (6 items)
        assert_eq!(successes.len(), 6, "expected 6 successes");
        // Items 0,3,6,9 abort (4 items)
        assert_eq!(aborts.len(), 4, "expected 4 aborts");
    }
}
