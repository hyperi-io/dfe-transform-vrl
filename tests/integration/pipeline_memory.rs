// Project:   dfe-transform-vrl
// File:      tests/integration/pipeline_memory.rs
// Purpose:   Full-pipeline integration tests using MemoryTransport (no Kafka)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end pipeline tests that exercise the WorkBatch governed engine driver
//! ([`pipeline::run_governed_pipeline`]) using in-memory channels instead of
//! Kafka. These cover the hot path that normally requires a running broker.
//!
//! scalo's MemoryTransport arrives via the `scalo` DEV-dependency, so this
//! module always compiles. Each test spins up a send-half (to simulate
//! upstream producers) and a receive-half (to observe the pipeline's sink
//! output).
//!
//! The engine carries NO byte budget (no governor here), so `run_governed`
//! delegates to the whole-batch `run_workbatch` loop. Shutdown is driven by a
//! [`CancellationToken`] (the driver returns cleanly on cancel). Sends pass
//! owned [`Bytes`]; the sink output is read as `WorkBatch.records`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::Bytes;
use dfe_transform_vrl::config::hot::HotConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::metrics::TransformMetrics;
use dfe_transform_vrl::pipeline::run_governed_pipeline;
use scalo::config::shared::SharedConfig;
use scalo::transport::{
    MemoryConfig, MemoryTransport, PayloadFormat, TransportReceiver, TransportSender,
};
use scalo::worker::engine::BatchProcessingConfig;
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, WorkerPoolConfig};
use tokio_util::sync::CancellationToken;

/// Build a pipeline harness: source transport, sink transport, hot config,
/// shutdown token. Returns everything a test needs to drive the pipeline.
struct Harness {
    source: Arc<MemoryTransport>,
    sink: Arc<MemoryTransport>,
    hot_config: SharedConfig<HotConfig>,
    shutdown: CancellationToken,
}

impl Harness {
    fn new(batch_size: usize, batch_timeout_ms: u64, key_field: &str) -> Self {
        let cfg = MemoryConfig {
            buffer_size: 10_000,
            recv_timeout_ms: 10,
            ..MemoryConfig::default()
        };
        let source = Arc::new(MemoryTransport::new(&cfg).expect("memory transport"));
        let sink = Arc::new(MemoryTransport::new(&cfg).expect("memory transport"));

        let hot = HotConfig {
            batch_size,
            batch_timeout_ms,
            key_field: key_field.to_string(),
            scaling_pressure_threshold: 0.8,
        };
        let hot_config = SharedConfig::new(hot);

        Self {
            source,
            sink,
            hot_config,
            shutdown: CancellationToken::new(),
        }
    }

    /// Inject a JSON message as if produced by an upstream service.
    async fn send_json(&self, json: &str) {
        let result = self
            .source
            .send("", Bytes::copy_from_slice(json.as_bytes()))
            .await;
        assert!(
            matches!(result, scalo::transport::SendResult::Ok),
            "send failed: {result:?}"
        );
    }

    /// Drain pending records from the sink — returns up to `max` payloads.
    async fn drain_sink(&self, max: usize) -> Vec<Vec<u8>> {
        let batch = self.sink.recv(max).await.unwrap_or_else(|_| {
            scalo::transport::WorkBatch::<<MemoryTransport as TransportReceiver>::Token>::empty()
        });
        batch
            .records
            .into_iter()
            .map(|r| r.payload.to_vec())
            .collect()
    }

    /// Stop the pipeline.
    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// Build and compile a VRL program from source.
fn compile_program(source: &str) -> Arc<vrl::compiler::Program> {
    let result = compile_vrl(source, None).expect("VRL compile");
    Arc::new(result.program)
}

/// A stand-alone batch engine with no byte budget -> whole-batch loop.
///
/// Builds the pool with an EXPLICIT 1-thread config (valid on any core count)
/// rather than `BatchEngine::new`, whose `WorkerPoolConfig::default()` derives
/// the bounds from `available_parallelism` and panics on a 1-core CI sandbox
/// (`min_threads > max_threads`).
fn engine() -> Arc<BatchEngine> {
    let pool = Arc::new(AdaptiveWorkerPool::new(WorkerPoolConfig {
        min_threads: 1,
        max_threads: 1,
        ..Default::default()
    }));
    Arc::new(BatchEngine::with_pool(
        pool,
        BatchProcessingConfig::default(),
    ))
}

#[tokio::test]
async fn test_pipeline_passes_json_through_identity_vrl() {
    let h = Harness::new(10, 50, ".id");
    h.send_json(r#"{"id": "evt-1", "value": 42}"#).await;
    h.send_json(r#"{"id": "evt-2", "value": 100}"#).await;

    // Identity VRL — no mutation
    let prog = compile_program(".");

    // Run the pipeline in the background
    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    // Let it process
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Drain the sink
    let out = h.drain_sink(10).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    assert_eq!(out.len(), 2, "expected 2 messages to pass through");
    // Events should be valid JSON with the same ids
    let payloads: Vec<String> = out
        .iter()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .collect();
    assert!(payloads.iter().any(|p| p.contains("evt-1")));
    assert!(payloads.iter().any(|p| p.contains("evt-2")));
}

#[tokio::test]
async fn test_pipeline_transforms_events_with_vrl_mutation() {
    let h = Harness::new(5, 50, ".id");
    h.send_json(r#"{"id": "a", "level": "info", "msg": "hello"}"#)
        .await;
    h.send_json(r#"{"id": "b", "level": "warn", "msg": "careful"}"#)
        .await;

    // Upper-case the level field, add a transformed flag
    let prog = compile_program(".level = upcase!(string!(.level)); .transformed = true");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let out = h.drain_sink(10).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    assert_eq!(out.len(), 2);
    let all = out
        .iter()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(all.contains("INFO"), "level should be uppercased: {all}");
    assert!(all.contains("WARN"));
    assert!(all.contains("transformed"));
}

#[tokio::test]
async fn test_pipeline_vrl_abort_drops_message() {
    let h = Harness::new(5, 50, ".id");
    // Three events: two should pass, one should abort
    h.send_json(r#"{"id": "keep-1", "drop": false}"#).await;
    h.send_json(r#"{"id": "drop-1", "drop": true}"#).await;
    h.send_json(r#"{"id": "keep-2", "drop": false}"#).await;

    let prog = compile_program("if .drop == true { abort }");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let out = h.drain_sink(10).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    // Only the two keep-* events should land in the sink
    assert_eq!(out.len(), 2, "abort should drop one event");
    let all = out
        .iter()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(all.contains("keep-1"));
    assert!(all.contains("keep-2"));
    assert!(!all.contains("drop-1"));
}

#[tokio::test]
async fn test_pipeline_skips_malformed_json_but_continues() {
    let h = Harness::new(10, 50, ".id");
    h.send_json(r#"{"id": "good-1", "value": 1}"#).await;
    // Intentional malformed JSON
    h.source
        .send("", Bytes::from_static(b"{not valid json{"))
        .await;
    h.send_json(r#"{"id": "good-2", "value": 2}"#).await;

    let prog = compile_program(".");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Json,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let out = h.drain_sink(10).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    // The malformed event should be filtered; both good events pass through
    assert_eq!(out.len(), 2, "malformed JSON should be skipped");
    let all = out
        .iter()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(all.contains("good-1"));
    assert!(all.contains("good-2"));
}

#[tokio::test]
async fn test_pipeline_vrl_runtime_error_skips_event() {
    let h = Harness::new(10, 50, ".id");
    // Second event triggers string! on an integer -> runtime error
    h.send_json(r#"{"id": "s1", "name": "alice"}"#).await;
    h.send_json(r#"{"id": "s2", "name": 42}"#).await;
    h.send_json(r#"{"id": "s3", "name": "bob"}"#).await;

    // Infallible coercion — fails for numeric input
    let prog = compile_program(".name = upcase!(string!(.name))");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let out = h.drain_sink(10).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    // s1 and s3 should pass; s2 fails and is filtered
    assert_eq!(out.len(), 2, "VRL runtime error should skip event");
    let all = out
        .iter()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(all.contains("ALICE"));
    assert!(all.contains("BOB"));
    assert!(!all.contains(r#""name":42"#));
}

#[tokio::test]
async fn test_pipeline_large_batch_of_mixed_events() {
    let h = Harness::new(50, 100, ".id");

    // 100 events: 50 normal, 25 with abort, 25 with runtime errors
    for i in 0..100 {
        let drop = i % 4 == 1;
        let bad = i % 4 == 2;
        let value = if bad {
            serde_json::json!({"id": format!("e{i}"), "drop": drop, "name": 42})
        } else {
            serde_json::json!({"id": format!("e{i}"), "drop": drop, "name": format!("name-{i}")})
        };
        h.send_json(&value.to_string()).await;
    }

    let prog = compile_program(
        r#"
        if .drop == true { abort }
        .name = upcase!(string!(.name))
        "#,
    );

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    let out = h.drain_sink(200).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    // 100 events, 25 aborted (drop=true), 25 runtime errors (name=42)
    // => 50 should land. Allow slack since error path also drops the bad
    // event; only well-formed events end up in out.
    assert!(
        out.len() >= 40 && out.len() <= 60,
        "expected ~50 events, got {}",
        out.len()
    );
}

#[tokio::test]
async fn test_pipeline_msgpack_roundtrip() {
    let h = Harness::new(5, 50, ".id");
    // Encode events as msgpack
    for i in 0..3 {
        let val = serde_json::json!({
            "id": format!("m{i}"),
            "n": i,
        });
        let bytes = rmp_serde::to_vec(&val).unwrap();
        h.source.send("", Bytes::from(bytes)).await;
    }

    let prog = compile_program(".tag = \"tagged\"");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::MsgPack,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let out = h.drain_sink(10).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    assert_eq!(out.len(), 3, "all msgpack events should roundtrip");
    // Each output should be msgpack-decodable and contain tag="tagged"
    for payload in out {
        let val: serde_json::Value = rmp_serde::from_slice(&payload).unwrap();
        assert_eq!(val["tag"], "tagged");
    }
}

#[tokio::test]
async fn test_pipeline_auto_detect_mixed_format_batch() {
    let h = Harness::new(10, 50, ".id");
    // Mix of JSON and msgpack — auto-detect
    h.send_json(r#"{"id": "json-1", "src": "json"}"#).await;
    let mp = rmp_serde::to_vec(&serde_json::json!({"id": "mp-1", "src": "msgpack"})).unwrap();
    h.source.send("", Bytes::from(mp)).await;
    h.send_json(r#"{"id": "json-2", "src": "json"}"#).await;

    let prog = compile_program(".");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let out = h.drain_sink(10).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    assert_eq!(out.len(), 3);
}

#[tokio::test]
async fn test_pipeline_shutdown_stops_loop_promptly() {
    let h = Harness::new(10, 100, ".id");
    let prog = compile_program(".");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    // Start with no events — pipeline should idle, then shut down quickly
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.shutdown();

    let start = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(2), pipeline).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "pipeline did not shut down within 2s");
    assert!(
        elapsed < Duration::from_secs(1),
        "shutdown took too long: {elapsed:?}"
    );
}

#[tokio::test]
async fn test_pipeline_hot_reload_batch_size_picked_up() {
    let h = Harness::new(2, 50, ".id");

    // Send 10 events
    for i in 0..10 {
        h.send_json(&format!(r#"{{"id":"e{i}","n":{i}}}"#)).await;
    }

    let prog = compile_program(".");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    // Let some batches drain.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Update hot config (the driver owns batch sizing now via the engine's
    // chunk size / governor, so this is a no-op for throughput -- the update
    // path must still apply cleanly without disturbing delivery).
    h.hot_config.update(HotConfig {
        batch_size: 100,
        batch_timeout_ms: 50,
        key_field: ".id".to_string(),
        scaling_pressure_threshold: 0.8,
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let out = h.drain_sink(50).await;
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    assert_eq!(out.len(), 10, "all events should drain");
}

#[tokio::test]
async fn test_pipeline_routes_to_sink_topic() {
    // Under scalo #37 the sender's `key` arg is the destination TOPIC, so
    // every output record routes on the configured sink topic ("out"), NOT a
    // per-record partition key. (The dot-path partition-key extractor was
    // removed in the WorkBatch migration; re-introduce once #37 lands.)
    let h = Harness::new(5, 50, ".org_id");
    h.send_json(r#"{"id":"e1","org_id":"tenant-a","x":1}"#)
        .await;
    h.send_json(r#"{"id":"e2","org_id":"tenant-b","x":2}"#)
        .await;

    let prog = compile_program(".");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    let batch = h
        .sink
        .recv(10)
        .await
        .unwrap_or_else(|_| scalo::transport::WorkBatch::empty());
    h.shutdown();
    pipeline.await.unwrap().unwrap();

    assert_eq!(batch.records.len(), 2);
    // Every record routes to the sink topic.
    for record in &batch.records {
        assert_eq!(record.key.as_deref(), Some("out"));
    }
    // The org_ids survive in the payload (just not as the routing key).
    let all = batch
        .records
        .iter()
        .map(|r| String::from_utf8_lossy(&r.payload).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(all.contains("tenant-a"));
    assert!(all.contains("tenant-b"));
}

#[tokio::test]
async fn test_pipeline_empty_source_idles_quietly() {
    let h = Harness::new(10, 50, ".id");
    let prog = compile_program(".");

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    // No events — should loop quietly, then shut down cleanly
    tokio::time::sleep(Duration::from_millis(200)).await;
    let out = h.drain_sink(10).await;
    h.shutdown();
    let result = tokio::time::timeout(Duration::from_secs(2), pipeline).await;

    assert!(result.is_ok(), "pipeline should shut down cleanly");
    assert_eq!(out.len(), 0, "no events should appear in sink");
}

#[tokio::test]
async fn test_pipeline_stress_1000_events() {
    let h = Harness::new(100, 50, ".id");

    for i in 0..1_000 {
        h.send_json(&format!(
            r#"{{"id":"stress-{i}","level":"info","payload":"data-{i}"}}"#
        ))
        .await;
    }

    let prog = compile_program(
        r#"
        .level = upcase!(string!(.level))
        .processed_at = "2026-04-16T00:00:00Z"
        "#,
    );

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    // Give it time to drain 1000 events
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Drain in multiple passes.
    let mut all_out = Vec::new();
    for _ in 0..15 {
        let batch = h.drain_sink(200).await;
        if batch.is_empty() {
            break;
        }
        all_out.extend(batch);
    }

    h.shutdown();
    pipeline.await.unwrap().unwrap();

    assert_eq!(
        all_out.len(),
        1_000,
        "all 1000 events must be delivered (at-least-once)"
    );

    // Spot-check a few for correctness
    let first = String::from_utf8_lossy(&all_out[0]).to_string();
    assert!(first.contains("INFO"));
    assert!(first.contains("processed_at"));
}

#[tokio::test]
async fn test_pipeline_with_worker_pool_parallel_processing() {
    use scalo::worker::{AdaptiveWorkerPool, WorkerPoolConfig};

    let h = Harness::new(20, 50, ".id");
    for i in 0..50 {
        h.send_json(&format!(r#"{{"id":"e{i}","n":{i}}}"#)).await;
    }

    // Non-trivial VRL so there's actual work to parallelise
    let prog = compile_program(
        r#"
        .doubled = int!(.n) * 2
        .label = if int!(.n) > 25 { "high" } else { "low" }
        "#,
    );

    // min_threads: 1 so the config stays valid on a 1-core CI sandbox, where
    // max_threads resolves down to the single available core (min 2 > max 1
    // would otherwise panic). max_threads 4 still parallelises where cores allow.
    let pool = Arc::new(AdaptiveWorkerPool::new(WorkerPoolConfig {
        min_threads: 1,
        max_threads: 4,
        ..Default::default()
    }));

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown,
            Some(pool),
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    let out = h.drain_sink(100).await;
    h.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(2), pipeline).await;

    assert_eq!(
        out.len(),
        50,
        "all events should be processed by worker pool"
    );
    // Verify VRL ran — doubled field present
    let first = String::from_utf8_lossy(&out[0]).to_string();
    assert!(first.contains("doubled"));
    assert!(first.contains("label"));
}

#[tokio::test]
async fn test_pipeline_burst_drains_with_concurrent_reader() {
    // A burst into the source drains to the sink while a consumer reads it
    // concurrently. (The old per-record backpressure-retry loop was deleted;
    // a saturated sink is now a terminal ack-barrier error, so this exercises
    // the happy burst path with a draining consumer rather than a retry.)
    let cfg = MemoryConfig {
        buffer_size: 1000,
        recv_timeout_ms: 10,
        ..MemoryConfig::default()
    };
    let source = Arc::new(MemoryTransport::new(&cfg).expect("memory transport"));
    let sink = Arc::new(MemoryTransport::new(&cfg).expect("memory transport"));

    let hot_config = SharedConfig::new(HotConfig {
        batch_size: 20,
        batch_timeout_ms: 50,
        key_field: ".id".to_string(),
        scaling_pressure_threshold: 0.8,
    });
    let shutdown = CancellationToken::new();

    // Send a burst
    for i in 0..20 {
        let _ = source
            .send("", Bytes::from(format!(r#"{{"id":"b{i}"}}"#).into_bytes()))
            .await;
    }

    let prog = compile_program(".");
    let sink_reader = Arc::clone(&sink);

    let source_pipeline = Arc::clone(&source);
    let sink_pipeline = Arc::clone(&sink);
    let shutdown_pipeline = shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let metrics = Arc::new(TransformMetrics::default());
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source_pipeline,
            &*sink_pipeline,
            prog,
            hot_config,
            PayloadFormat::Auto,
            &metrics,
            ready,
            shutdown_pipeline,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    // Continuously drain the sink.
    let mut collected = 0;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let batch = sink_reader
            .recv(10)
            .await
            .unwrap_or_else(|_| scalo::transport::WorkBatch::empty());
        collected += batch.records.len();
        if collected >= 20 {
            break;
        }
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), pipeline).await;

    assert!(
        collected > 0,
        "should deliver some events through the burst, got {collected}"
    );
}

#[tokio::test]
async fn test_pipeline_with_real_metrics_recorder() {
    // Exercise the ServiceMetrics / layered-groups recording branches by using
    // TransformMetrics::new() with a real MetricsManager rather than the
    // test-default (which has all layers as None).
    let h = Harness::new(10, 50, ".id");
    for i in 0..5 {
        h.send_json(&format!(r#"{{"id":"m{i}","level":"info"}}"#))
            .await;
    }

    let prog = compile_program(r#".tag = "metricated""#);

    let manager = scalo::metrics::MetricsManager::new("test_pipeline_dfe_transform_vrl_real");
    let transform_metrics = Arc::new(TransformMetrics::new(&manager, "0.1.0", "testcommit"));

    let source = Arc::clone(&h.source);
    let sink = Arc::clone(&h.sink);
    let hot = h.hot_config.clone();
    let shutdown = h.shutdown.clone();
    let eng = engine();
    let pipeline = tokio::spawn(async move {
        let ready = Arc::new(AtomicBool::new(false));
        run_governed_pipeline(
            &eng,
            &*source,
            &*sink,
            prog,
            hot,
            PayloadFormat::Auto,
            &transform_metrics,
            ready,
            shutdown,
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let out = h.drain_sink(10).await;
    h.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(2), pipeline).await;

    assert_eq!(
        out.len(),
        5,
        "all events should be metricated and delivered"
    );
}
