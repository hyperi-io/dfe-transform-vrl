// Project:   dfe-transform-vrl
// File:      src/pipeline.rs
// Purpose:   Event processing pipeline — consume, transform, produce
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Event processing pipeline.
//!
//! The mid-tier transform stage (consume -> VRL transform -> produce ->
//! release) is driven by scalo's `WorkBatch` pipeline loop
//! ([`BatchEngine::pipeline`]). The loop owns `recv -> process -> send ->
//! release` with full self-regulation (inbound gate + AIMD byte-budget
//! streaming), and holds each block's source acknowledgement until the sink
//! has delivered it. This crate supplies only the VRL-specific `process`
//! closure and the produce `sink`.
//!
//! Data flow per block:
//! 1. The loop receives a [`WorkBatch`] of [`Record`]s from the source: the
//!    governed Kafka consumer on the bus transport (intake pauses under memory
//!    pressure -- the member stays in the group, no rebalance), or the Push
//!    listener on the direct transport (pushes are refused `UNAVAILABLE` under
//!    pressure).
//! 2. `process` parses each record's JSON into a VRL `Value`, runs the
//!    compiled VRL program in parallel on the worker pool, and serialises the
//!    surviving events back to JSON. Records that are not JSON, or that a VRL
//!    `abort` or a transform error drops, are removed from the block, which
//!    then releases `Dropped`; the block's `commit_tokens`
//!    (the source acks) flow through untouched, so a fan-in NEVER under-acks
//!    the source.
//! 3. The loop sends the whole out-batch via the sender's
//!    [`TransportSender::send_batch`].
//! 4. Only once the send returns `Ok` does the loop release the block's
//!    source: Kafka commits the offsets, the Push listener answers its sender
//!    OK. A failed send leaves it unreleased -- Kafka re-reads the block after
//!    a restart, and a Push sender is answered `UNAVAILABLE` and retries.
//!    Batch-level at-least-once, not per record. With
//!    `source.acknowledgements.enabled: false` the source is released at
//!    receipt instead.
//!
//! Self-regulation is default-ON (opt out via `self_regulation.enabled =
//! false`). The byte-budget lever is wired into the engine by the
//! `ServiceRuntime`; the inbound gate is attached here, to the Kafka consumer
//! via [`SelfRegulationGovernor::attach_kafka_gate`] and to the Push listener
//! through its builder. The OUTBOUND producer drain is NEVER gated -- gating
//! the sink would deadlock the pipeline (scalo `docs/backpressure.md`).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use scalo::SelfRegulationGovernor;
use scalo::config::shared::SharedConfig;
use scalo::logger::{log_sampled, security};
use scalo::memory::MemoryGuard;
use scalo::scaling::ScalingPressure;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::kafka::{KafkaConfig, KafkaTransport, total_consumer_lag};
use scalo::transport::{
    AnySender, DeliveryStatus, PayloadFormat, PieceFinalizer, Record, RecordMeta, SendResult,
    TransportSender, WorkBatch,
};
use scalo::worker::AdaptiveWorkerPool;
use scalo::worker::BatchEngine;
use scalo::worker::engine::{BlockPieces, EngineError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

/// Cadence for pushing per-pod scaling signals (assigned Kafka lag + outbound
/// circuit state + memory ratio) into the runtime's unified [`ScalingPressure`]
/// (the engine served at `/scaling/pressure` to KEDA). Fresher than the worker
/// pool's own scaling tick so the inbound pressure term never lags the
/// autoscaler poll, and cheap (one `librdkafka` stats read per tick). Mirrors
/// the loader's 5s flush-tick push cadence.
const SCALING_SIGNAL_INTERVAL_SECS: u64 = 5;

/// Longest a Push is held for delivery: inside the stage in front's 20 s send
/// deadline, so it is answered before that sender gives up.
pub const PUSH_MAX_HOLD: Duration = Duration::from_secs(18);

/// Send deadline to the next hop -- a Push listener, or a Kafka delivery
/// report while a Push is held: inside this stage's own [`PUSH_MAX_HOLD`], so
/// a slow hop is retried before the hold runs out.
pub const NEXT_HOP_SEND_TIMEOUT_MS: u64 = 15_000;

// Per-site log spam guards
static DESER_ERRORS: AtomicU64 = AtomicU64::new(0);
static VRL_ERRORS: AtomicU64 = AtomicU64::new(0);
static FILTERED_BLOCKS: AtomicU64 = AtomicU64::new(0);
use vrl::compiler::Program;
use vrl::value::Value;

/// Per-record deserialise outcome: `(value, index)` on success, or
/// `(index, error)` on failure.
type DeserResult = Result<(Value, usize), (usize, String)>;

/// Per-record VRL outcome: `(value, index)` on success, or `(index, error)` on
/// failure (abort vs runtime error discriminated by the [`crate::Error`]
/// variant).
type VrlResult = Result<(Value, usize), (usize, crate::Error)>;

use crate::config::Config;
use crate::config::hot::HotConfig;
use crate::engine::runner::run_vrl;
use crate::kafka;
use crate::metrics::TransformMetrics;

/// Run the transform pipeline (production entry point).
///
/// Builds the source -- the governed Kafka consumer (inbound pause-partitions
/// gate attached when self-regulation is on) or the armed Push listener -- and
/// the sink, then hands the `recv -> process -> send -> release` loop to
/// [`BatchEngine::pipeline`].
#[allow(clippy::too_many_arguments)]
pub async fn run(
    config: &Config,
    program: Arc<Program>,
    hot_config: SharedConfig<HotConfig>,
    transform_metrics: Arc<TransformMetrics>,
    ready_flag: Arc<AtomicBool>,
    shutdown: CancellationToken,
    worker_pool: Option<Arc<AdaptiveWorkerPool>>,
    engine: Arc<BatchEngine>,
    governor: Option<SelfRegulationGovernor>,
    scaling: Option<Arc<ScalingPressure>>,
    memory_guard: Arc<MemoryGuard>,
) -> crate::Result<()> {
    info!(
        pipeline = %config.pipeline.name,
        source_transport = ?config.source.transport,
        source_topics = ?config.source.topics,
        sink_transport = ?config.sink.transport,
        sink_topic = %config.sink.topic,
        self_regulation = governor.is_some(),
        "initialising pipeline"
    );

    // Sink: the bus producer, or a client to the downstream Push listener.
    // Never gated -- gating the outbound drain deadlocks the pipeline.
    let producer = build_sender(config).await?;

    // The direct transport has no consumer group and no lag, so the Kafka gate
    // and the lag ticker belong to the bus form alone.
    if config.source.transport.is_direct() {
        let listener = start_push_listener(config, governor.as_ref(), &memory_guard).await?;
        info!(listen = %config.source.listen, "Push listener started");
        return run_governed_pipeline(
            &engine,
            &listener,
            &producer,
            program,
            hot_config,
            &transform_metrics,
            ready_flag,
            shutdown,
            worker_pool,
            config.sink.topic.clone(),
            Arc::new(AtomicBool::new(false)),
        )
        .await;
    }

    let consumer_config = kafka::build_consumer_config(&config.source);

    // Consumer: attach the self-regulation inbound gate so intake pauses the
    // ASSIGNED partitions under memory pressure (member stays in the group,
    // consumer lag rises, KEDA scales up). When the governor is off the plain
    // consumer is used (byte-identical to the pre-governor data path).
    let consumer = KafkaTransport::new(&consumer_config)
        .await
        .map_err(|e| crate::Error::Kafka(format!("failed to create consumer: {e}")))?
        .with_acknowledgements(config.source.acknowledgements);
    let consumer = match governor.as_ref() {
        Some(gov) => gov.attach_kafka_gate(consumer),
        None => consumer,
    };
    // Share the consumer with the scaling-signal ticker (below) without taking
    // it away from the engine's recv loop. `KafkaTransport` is not `Clone`, and
    // every method we use takes `&self`, so an `Arc` serves both readers.
    let consumer = Arc::new(consumer);

    // Outbound circuit latch. The sink closure opens it on a fatal produce
    // failure and closes it on the next successful send; the scaling-signal
    // ticker reads it and pushes `set_circuit_open`. A dead sink gates the
    // composite pressure to 0 (more pods cannot relieve an unreachable broker).
    let circuit_open = Arc::new(AtomicBool::new(false));

    // Per-pod scaling-signal ticker (scalo 2.9 unified engine). The batch
    // engine owns the recv loop, so there is no app-side poll tick to piggy-back
    // on; a lightweight background task feeds the runtime's shared
    // `ScalingPressure` -- the engine served at `/scaling/pressure` to KEDA --
    // on a fixed cadence. It pushes:
    //   - the `kafka_lag` component (the assigned-partition lag, the primary
    //     KEDA driver, registered via `ServiceApp::scaling_components`);
    //   - the outbound circuit latch (open -> pressure gated to 0: more pods
    //     cannot relieve a dead broker);
    //   - the memory ratio (the never-OOM HARD gate: usage past the
    //     memory_gate_threshold forces pressure to 100 -> immediate scale-up).
    // These replace the old per-pod `ScalingSignalsCell` push, which fed the
    // SAME source values into a now-removed second engine. Stops on shutdown.
    // Only spawned when the runtime built the engine (`scaling.enabled`);
    // absent it, `set_component`/`set_memory` would have no engine to feed.
    let signal_task = scaling.map(|scaling| {
        let consumer = Arc::clone(&consumer);
        let circuit_open = Arc::clone(&circuit_open);
        let memory_guard = Arc::clone(&memory_guard);
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(std::time::Duration::from_secs(SCALING_SIGNAL_INTERVAL_SECS));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = tick.tick() => {
                        // assigned_lag = lag summed over THIS pod's ASSIGNED
                        // partitions (inherently per-pod, scale-invariant).
                        // Requires librdkafka statistics (statistics.interval.ms
                        // > 0, set in build_consumer_config); 0 if disabled.
                        let lag = total_consumer_lag(&consumer.stats()).max(0);
                        #[allow(clippy::cast_precision_loss)]
                        scaling.set_component("kafka_lag", lag as f64);
                        scaling.set_circuit_open(circuit_open.load(Ordering::Acquire));
                        // Feed the memory HARD gate from the shared cgroup-aware
                        // guard (the SAME guard the governor's inbound brake
                        // uses) so /scaling/pressure scales up before OOM.
                        scaling
                            .set_memory(memory_guard.current_bytes(), memory_guard.limit_bytes());
                    }
                }
            }
        })
    });

    let result = run_governed_pipeline(
        &engine,
        consumer.as_ref(),
        &producer,
        program,
        hot_config,
        &transform_metrics,
        ready_flag,
        shutdown,
        worker_pool,
        config.sink.topic.clone(),
        circuit_open,
    )
    .await;

    if let Some(task) = signal_task {
        task.abort();
    }

    result
}

/// Start the direct transport's Push listener, armed from its first request.
///
/// Armed, a Push is answered only once its records are released, so no push
/// is acknowledged before the pipeline loop is running to deliver it. While
/// the governor's pressure holds the listener refuses pushes `UNAVAILABLE`,
/// and the responses it holds are leased on the memory guard.
///
/// # Errors
///
/// The listen address is invalid or cannot be bound.
pub async fn start_push_listener(
    config: &Config,
    governor: Option<&SelfRegulationGovernor>,
    memory_guard: &Arc<MemoryGuard>,
) -> crate::Result<GrpcTransport> {
    let grpc = GrpcConfig::server(&config.source.listen);
    let builder = GrpcTransport::builder(&grpc)
        .acknowledgements(config.source.acknowledgements)
        .armed(true)
        .max_hold(PUSH_MAX_HOLD)
        .memory_guard(Arc::clone(memory_guard));
    let builder = match governor {
        Some(governor) => builder.pressure(governor.pressure()),
        None => builder,
    };
    builder
        .start()
        .await
        .map_err(|e| crate::Error::Kafka(format!("failed to start Push listener: {e}")))
}

/// Build the sink for this deployment's transport.
///
/// `AnySender` keeps the driver one code path across both, and delegates
/// `send_batch` to the backend's native batch RPC.
///
/// # Errors
///
/// The sink transport cannot be created from `config.sink`.
pub async fn build_sender(config: &Config) -> crate::Result<AnySender> {
    if config.sink.transport.is_direct() {
        let mut grpc = GrpcConfig::client(&config.sink.endpoint);
        grpc.send_timeout_ms = NEXT_HOP_SEND_TIMEOUT_MS;
        let transport = GrpcTransport::new(&grpc)
            .await
            .map_err(|e| crate::Error::Kafka(format!("failed to create gRPC sink: {e}")))?;
        info!(endpoint = %config.sink.endpoint, "gRPC sink initialised");
        return Ok(AnySender::Grpc(transport));
    }

    let transport = KafkaTransport::new(&producer_config(config))
        .await
        .map_err(|e| crate::Error::Kafka(format!("failed to create producer: {e}")))?;
    Ok(AnySender::Kafka(transport))
}

/// The bus sink's producer config.
///
/// While a direct source holds its acknowledgements, a delivery report that
/// comes after the hold is spent is a duplicate in waiting: the sender was
/// already told to retry. So `message.timeout.ms` is capped at
/// [`NEXT_HOP_SEND_TIMEOUT_MS`], unless `sink.librdkafka_options` sets it.
fn producer_config(config: &Config) -> KafkaConfig {
    let mut producer = kafka::build_producer_config(&config.sink, &config.pipeline.name);
    let holds = config.source.transport.is_direct() && config.source.acknowledgements.enabled;
    if holds
        && !config
            .sink
            .librdkafka_options
            .contains_key("message.timeout.ms")
    {
        let capped = u64::from(config.sink.message_timeout_ms).min(NEXT_HOP_SEND_TIMEOUT_MS);
        producer
            .librdkafka_overrides
            .insert("message.timeout.ms".to_string(), capped.to_string());
    }
    producer
}

/// Drive the mid-tier transform through [`BatchEngine::pipeline`].
///
/// `receiver` is the source (the optionally gated Kafka consumer, or the Push
/// listener); `sender` is the sink. The VRL transform is the `process` closure
/// and the produce is the `sink` closure. The loop owns batching, streaming
/// sub-blocks under pressure, and holding each block's source acknowledgement
/// until the sink has delivered it. Records `sender` would dead-letter rather
/// than send are taken out of the block before the sink is called.
///
/// Exposed `pub` so the e2e integration tests can drive a real Kafka round-trip
/// with pre-built transports + a stand-alone engine, without going through the
/// full `ServiceRuntime` bootstrap.
#[allow(clippy::too_many_arguments)]
pub async fn run_governed_pipeline<R, S>(
    engine: &BatchEngine,
    receiver: &R,
    sender: &S,
    program: Arc<Program>,
    hot_config: SharedConfig<HotConfig>,
    transform_metrics: &Arc<TransformMetrics>,
    ready_flag: Arc<AtomicBool>,
    shutdown: CancellationToken,
    worker_pool: Option<Arc<AdaptiveWorkerPool>>,
    sink_topic: String,
    circuit_open: Arc<AtomicBool>,
) -> crate::Result<()>
where
    R: scalo::transport::TransportReceiver,
    S: TransportSender,
{
    ready_flag.store(true, Ordering::Release);
    if let Some(ref dfe) = transform_metrics.dfe {
        dfe.pipeline_ready(true);
    }
    info!("pipeline ready — entering governed event loop");

    // ---- process: bytes -> VRL Value -> bytes, in parallel on the pool -------
    //
    // Runs the compiled VRL program across the block. Records dropped by a VRL
    // `abort` or a transform/(de)serialise error are removed from the block and
    // counted in a marker record the sink takes back out; the commit_tokens flow
    // through untouched via `map_records`, so a fan-in never under-acks the
    // source offsets (at-least-once on the surviving set).
    // `hot_config` is retained for its other live fields (validated + reloaded)
    // even though the partition-key path is dormant until scalo #37 lands.
    let _ = &hot_config;
    let process = {
        let program = Arc::clone(&program);
        let transform_metrics = Arc::clone(transform_metrics);
        let worker_pool = worker_pool.clone();
        let sink_topic = Arc::<str>::from(sink_topic.as_str());
        move |batch: WorkBatch<R::Token>| -> Result<WorkBatch<R::Token>, EngineError> {
            Ok(batch.map_records(|records| {
                transform_block(
                    records,
                    &program,
                    &sink_topic,
                    &transform_metrics,
                    worker_pool.as_ref(),
                )
            }))
        }
    };

    // ---- sink: send the whole out-batch via the producer ---------------------
    //
    // `send_batch` routes each record to its `key` (the Kafka destination topic
    // -- set to the configured sink topic in `transform_records`). The loop
    // releases the block's source only on `Ok`; see `settle_send` for how every
    // other result holds it.
    //
    // The sender counts its own `transport_*` series under its own transport
    // label, so the sink records only the metrics no transport emits.
    let sink_backend = sender.name();
    let sink = {
        let transform_metrics = Arc::clone(transform_metrics);
        let circuit_open = Arc::clone(&circuit_open);
        move |out: &WorkBatch<R::Token>, pieces: &BlockPieces<'_>| {
            let transform_metrics = Arc::clone(&transform_metrics);
            let circuit_open = Arc::clone(&circuit_open);
            // The engine's `Sink` bound returns an OWNED future (it cannot borrow
            // `out`), so clone the records into the async block. This is cheap:
            // `Record` is `Bytes` (refcount bump) + `Arc<str>` key -- N refcount
            // bumps, NOT N payload copies. Zero-copy on the wire is preserved.
            let (records, dropped) = split_dropped(&out.records);
            if dropped > 0 {
                // Records `process` removed: the block releases `Dropped`.
                pieces.piece().report(DeliveryStatus::Dropped);
            }
            // A piece per attempt, so a block the sender filtered out rather
            // than sent is released `Dropped`, never `Delivered`.
            let filtered = (!records.is_empty()).then(|| pieces.piece());
            async move {
                let Some(filtered) = filtered else {
                    return Ok(());
                };
                let start = Instant::now();
                let result = sender.send_batch(&records).await;
                settle_send(
                    result,
                    records.len() as u64,
                    start.elapsed().as_secs_f64(),
                    sink_backend,
                    &transform_metrics,
                    &circuit_open,
                    filtered,
                )
            }
        }
    };

    // No ticker: the produce is synchronous per block (no buffered sink to
    // flush on a timer), so there is nothing to fire between blocks.
    let run_result = engine
        .pipeline(receiver)
        .shutdown(shutdown)
        .sender(sender)
        .run_with_pieces(process, sink)
        .await;

    ready_flag.store(false, Ordering::Release);
    if let Some(ref dfe) = transform_metrics.dfe {
        dfe.pipeline_ready(false);
    }

    info!("closing transports");
    let _ = receiver.close().await;
    let _ = sender.close().await;

    run_result.map_err(|e| crate::Error::Kafka(format!("pipeline engine error: {e}")))
}

/// Meter one sink call and turn its [`SendResult`] into the loop's verdict.
///
/// A failure the sender calls recoverable is returned transient, so the loop
/// holds the block and sends it again after a backoff -- or, for a Push
/// source, answers `UNAVAILABLE` at the hold deadline so the sender retries.
/// Only a permanent failure stops the loop.
///
/// `filtered` reports `Dropped` when the sender filtered the block out
/// instead of sending it, which releases the source without calling the
/// records delivered. Otherwise it reports `Delivered`, the floor of the
/// merge, and leaves the block's status to the loop's own piece.
fn settle_send(
    result: SendResult,
    record_count: u64,
    elapsed_secs: f64,
    sink_backend: &'static str,
    transform_metrics: &TransformMetrics,
    circuit_open: &AtomicBool,
    filtered: PieceFinalizer,
) -> Result<(), EngineError> {
    filtered.report(if matches!(result, SendResult::FilteredDlq) {
        DeliveryStatus::Dropped
    } else {
        DeliveryStatus::Delivered
    });
    match result {
        SendResult::Ok => {
            // Sink reachable -> clear the outbound circuit latch (the scaling
            // ticker reads it for `set_circuit_open`).
            circuit_open.store(false, Ordering::Release);
            if let Some(ref dfe) = transform_metrics.dfe {
                dfe.records_delivered(record_count);
            }
            if let Some(ref app) = transform_metrics.app {
                app.record_processed(record_count);
            }
            if let Some(ref sm) = transform_metrics.sink {
                sm.record_duration(sink_backend, elapsed_secs);
            }
            trace!(records = record_count, "produced batch");
            Ok(())
        }
        SendResult::FilteredDlq => {
            // Records the screen should have taken out of the block: the same
            // bytes are refused on every retry, so they are dropped, not sent.
            if log_sampled(&FILTERED_BLOCKS, 100) {
                warn!(
                    records = record_count,
                    backend = sink_backend,
                    total = FILTERED_BLOCKS.load(Ordering::Relaxed),
                    "the sink filtered out a whole block instead of sending it; its \
                     records are dropped (sampled 1/100)"
                );
            }
            if let Some(ref dfe) = transform_metrics.dfe {
                dfe.records_filtered(record_count);
            }
            Ok(())
        }
        SendResult::Backpressured => {
            if let Some(ref bp) = transform_metrics.backpressure {
                bp.record_event();
            }
            Err(scalo::TransportError::Backpressure.into())
        }
        SendResult::Fatal(e) if e.is_recoverable() => {
            if let Some(ref bp) = transform_metrics.backpressure {
                bp.record_event();
            }
            Err(EngineError::Transport(e))
        }
        SendResult::Fatal(e) => {
            // Sink unreachable -> open the outbound circuit latch so the
            // scaling composite gates to 0 (more pods cannot relieve a dead
            // broker; the circuit is the gate).
            circuit_open.store(true, Ordering::Release);
            transform_metrics.record_produce_error();
            Err(EngineError::Sink(format!("produce failed: {e}")))
        }
    }
}

/// Header of the marker record [`transform_block`] appends, carrying how many
/// records the transform removed from the block.
const DROPPED_HEADER: &str = "x-dfe-transform-vrl-dropped";

/// [`transform_records`] over one block, with a marker record appended when
/// the transform removed any.
///
/// `process` has no way to report a status of its own, so the count rides in
/// the block to the sink, which takes the marker out and releases the block
/// `Dropped`. A block whose every record was removed still reaches the sink
/// that way.
fn transform_block(
    records: Vec<Record>,
    program: &Program,
    sink_topic: &Arc<str>,
    transform_metrics: &TransformMetrics,
    worker_pool: Option<&Arc<AdaptiveWorkerPool>>,
) -> Vec<Record> {
    let received = records.len();
    let mut out = transform_records(records, program, sink_topic, transform_metrics, worker_pool);
    let dropped = received.saturating_sub(out.len());
    if dropped > 0 {
        out.push(Record {
            payload: Bytes::new(),
            key: None,
            headers: vec![(DROPPED_HEADER.to_string(), dropped.to_string().into_bytes())],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        });
    }
    out
}

/// The records of a block to send, and how many [`transform_block`] removed.
fn split_dropped(records: &[Record]) -> (Vec<Record>, u64) {
    let mut dropped = 0_u64;
    let send = records
        .iter()
        .filter(|record| {
            let Some((_, count)) = record.headers.iter().find(|(k, _)| k == DROPPED_HEADER) else {
                return true;
            };
            dropped += std::str::from_utf8(count)
                .ok()
                .and_then(|count| count.parse::<u64>().ok())
                .unwrap_or(1);
            false
        })
        .cloned()
        .collect();
    (send, dropped)
}

/// Transform a block of [`Record`]s through the VRL program.
///
/// JSON parse -> VRL eval (parallel on the worker pool when available) ->
/// JSON serialise. Returns ONLY the surviving records; a record that is not
/// JSON, or that a VRL `abort`, a transform error or a serialise error drops,
/// is removed (and metered). Each surviving record's `key` is set to the sink topic so the
/// Kafka producer routes it correctly (scalo #37: `send`'s key arg IS the
/// destination topic).
#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::option_if_let_else
)]
fn transform_records(
    records: Vec<Record>,
    program: &Program,
    sink_topic: &Arc<str>,
    transform_metrics: &TransformMetrics,
    worker_pool: Option<&Arc<AdaptiveWorkerPool>>,
) -> Vec<Record> {
    let batch_len = records.len();
    if batch_len == 0 {
        return records;
    }

    let batch_bytes: u64 = records.iter().map(|r| r.payload.len() as u64).sum();

    // Layer 1: ServiceMetrics (platform)
    if let Some(ref dfe) = transform_metrics.dfe {
        dfe.records_received(batch_len as u64);
    }
    // Layer 2: AppMetrics (common group). Its `records_received_total` is the
    // platform series above, so counting it here as well doubles every record.
    if let Some(ref app) = transform_metrics.app {
        app.record_bytes_received(batch_bytes);
    }
    // Layer 3: app-specific
    transform_metrics.batch_size.record(batch_len as f64);

    // Phase 1: parse JSON (CPU-bound) -- parallel via the pool.
    let deser_start = Instant::now();
    let indexed: Vec<(usize, Bytes)> = records
        .iter()
        .enumerate()
        .map(|(idx, rec)| (idx, rec.payload.clone()))
        .collect();

    let deser = |(idx, payload): &(usize, Bytes)| -> DeserResult {
        match deserialize_event(payload) {
            Ok(v) => Ok((v, *idx)),
            Err(e) => Err((*idx, e.to_string())),
        }
    };
    let deser_results: Vec<DeserResult> = match worker_pool {
        Some(pool) => pool.process_batch(&indexed, deser),
        None => indexed.iter().map(deser).collect(),
    };

    let mut events: Vec<(Value, usize)> = Vec::with_capacity(batch_len);
    for result in deser_results {
        match result {
            Ok(item) => events.push(item),
            Err((_idx, e)) => {
                if log_sampled(&DESER_ERRORS, 1000) {
                    warn!(error = %e, total = DESER_ERRORS.load(Ordering::Relaxed), "deserialise failure (sampled 1/1000)");
                }
                security::input_validation_failure("deserialise", &e, None);
                transform_metrics.record_deser_error();
            }
        }
    }
    transform_metrics
        .deserialise_duration
        .record(deser_start.elapsed().as_secs_f64());
    if !events.is_empty() {
        transform_metrics.record_format("json", events.len() as u64);
    }

    // Phase 2: VRL transform (CPU-bound) -- parallel via the pool.
    let vrl_start = Instant::now();
    let vrl = |(value, idx): &(Value, usize)| -> VrlResult {
        let mut value = value.clone();
        match run_vrl(program, &mut value) {
            Ok(_) => Ok((value, *idx)),
            Err(e) => Err((*idx, e)),
        }
    };
    let vrl_results: Vec<VrlResult> = match worker_pool {
        Some(pool) => pool.process_batch(&events, vrl),
        None => events.iter().map(vrl).collect(),
    };

    let mut transformed: Vec<(Value, usize)> = Vec::with_capacity(vrl_results.len());
    for result in vrl_results {
        match result {
            Ok(item) => transformed.push(item),
            Err((idx, crate::Error::VrlAbort(ref reason))) => {
                debug!(reason = %reason, index = idx, "event dropped by VRL abort");
                transform_metrics.abort_total.increment(1);
                if let Some(ref dfe) = transform_metrics.dfe {
                    dfe.records_filtered(1);
                }
            }
            Err((idx, e)) => {
                if log_sampled(&VRL_ERRORS, 1000) {
                    warn!(error = %e, total = VRL_ERRORS.load(Ordering::Relaxed), "VRL transform error (sampled 1/1000)");
                }
                trace!(stage = "vrl_transform", error = %e, index = idx, "message error routing");
                security::input_validation_failure("vrl_transform", &e.to_string(), None);
                transform_metrics.record_transform_error();
            }
        }
    }
    transform_metrics
        .execute_duration
        .record(vrl_start.elapsed().as_secs_f64());

    // Phase 3: serialise the surviving events back to JSON and build the
    // output records. The Kafka producer routes each record to its
    // `key` (= the sink topic). Partition keying via the routing field is NOT
    // settable through the sender trait (scalo #37: `send`'s key arg IS the
    // destination topic, so there is no slot for a partition key); the routing
    // field stays a config surface only until #37 lands.
    let ser_start = Instant::now();
    let mut out_records: Vec<Record> = Vec::with_capacity(transformed.len());
    for (value, _idx) in &transformed {
        match serialize_event(value) {
            Ok(serialized) => {
                out_records.push(Record {
                    payload: Bytes::from(serialized),
                    key: Some(Arc::clone(sink_topic)),
                    headers: Vec::new(),
                    metadata: RecordMeta {
                        timestamp_ms: None,
                        format: PayloadFormat::Json,
                    },
                });
            }
            Err(e) => {
                if log_sampled(&VRL_ERRORS, 1000) {
                    warn!(error = %e, "serialise failure (sampled 1/1000)");
                }
                transform_metrics.record_produce_error();
            }
        }
    }
    transform_metrics
        .serialise_duration
        .record(ser_start.elapsed().as_secs_f64());

    out_records
}

/// Parse a JSON payload into a VRL Value.
///
/// Uses `sonic_rs` (SIMD-accelerated, 2-4x faster than `serde_json`). Its serde
/// path refuses nesting past 254 levels, which a 2 MiB release worker stack
/// holds, so this needs no depth pre-check.
fn deserialize_event(payload: &[u8]) -> crate::Result<Value> {
    sonic_rs::from_slice(payload)
        .map_err(|e| crate::Error::Serialisation(format!("payload is not JSON: {e}")))
}

/// Serialise a VRL Value to JSON.
///
/// Uses `sonic_rs` (SIMD-accelerated, matching the parse path).
fn serialize_event(value: &Value) -> crate::Result<Vec<u8>> {
    sonic_rs::to_vec(value).map_err(|e| crate::Error::Serialisation(format!("JSON serialise: {e}")))
}

// NB: the dot-path partition-key extractor (`extract_key`) was removed in the
// WorkBatch migration. scalo #37 means the Kafka sender's `key` arg IS the
// destination topic (no slot for a partition key via `send`/`send_batch`), so
// the produce path keys on the sink topic and there is nothing for an extractor
// to feed. Re-introduce a per-record partition key once #37 lands.

// The integration and e2e tests' port picker, so a listener here also binds below 10240.
#[cfg(test)]
#[allow(clippy::expect_used)]
#[path = "../tests/common/ports.rs"]
mod test_ports;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::needless_raw_string_hashes
)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_json() {
        let json = br#"{"key": "value"}"#;
        let value = deserialize_event(json).unwrap();
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("key"), Some(&Value::from("value")));
    }

    #[test]
    fn a_payload_that_is_not_json_is_refused_by_name() {
        let err = deserialize_event(b"\x00\x00\x02\x00").unwrap_err();
        assert!(
            err.to_string().contains("payload is not JSON"),
            "the refusal must say why, got: {err}"
        );
    }

    #[test]
    fn test_serialize_roundtrip_json() {
        let original = Value::from(serde_json::json!({"a": 1, "b": "hello"}));
        let bytes = serialize_event(&original).unwrap();
        let recovered = deserialize_event(&bytes).unwrap();
        assert_eq!(
            original.as_object().unwrap().get("a"),
            recovered.as_object().unwrap().get("a"),
        );
    }

    /// The `WorkBatch` process path: deserialise -> VRL -> serialise across a
    /// block of records, with commit tokens carried through untouched. Proves
    /// the headline migration contract -- a fan-in (abort drops records) does
    /// NOT disturb the source acks.
    #[test]
    fn test_transform_records_preserves_tokens_on_fan_in() {
        use scalo::transport::CommitToken;

        #[derive(Debug, Clone, PartialEq, Eq)]
        struct TestToken(u64);
        impl std::fmt::Display for TestToken {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "tok-{}", self.0)
            }
        }
        impl CommitToken for TestToken {}

        let fns = vrl::stdlib::all();
        let program = vrl::compiler::compile(
            r#"if .drop == true { abort } else { .processed = true }"#,
            &fns,
        )
        .expect("VRL compile failed")
        .program;

        // 4 records, 2 marked to abort (fan-in 4 -> 2).
        let records: Vec<Record> = (0..4)
            .map(|i| {
                let drop = i % 2 == 0;
                let payload = format!(r#"{{"id":{i},"drop":{drop}}}"#);
                Record {
                    payload: Bytes::from(payload.into_bytes()),
                    key: None,
                    headers: Vec::new(),
                    metadata: RecordMeta {
                        timestamp_ms: None,
                        format: PayloadFormat::Json,
                    },
                }
            })
            .collect();

        let batch: WorkBatch<TestToken> =
            WorkBatch::new(records, vec![TestToken(10), TestToken(11)]);
        let metrics = TransformMetrics::default();
        let sink_topic: Arc<str> = Arc::from("out");

        let out = batch
            .map_records(|recs| transform_records(recs, &program, &sink_topic, &metrics, None));

        // 2 survived the abort fan-in...
        assert_eq!(out.records.len(), 2, "two records dropped by abort");
        // ...but the source acks are untouched.
        assert_eq!(out.commit_tokens, vec![TestToken(10), TestToken(11)]);
        // surviving records route to the sink topic.
        for r in &out.records {
            assert_eq!(r.key.as_deref(), Some("out"));
            let v = deserialize_event(&r.payload).unwrap();
            assert_eq!(
                v.as_object().unwrap().get("processed"),
                Some(&Value::Boolean(true))
            );
        }
    }

    /// A deserialise failure drops the bad record but keeps the good ones; the
    /// run never aborts the whole block on one bad payload.
    #[test]
    fn test_transform_records_drops_unparseable() {
        let fns = vrl::stdlib::all();
        let program = vrl::compiler::compile(r#".ok = true"#, &fns)
            .expect("VRL compile failed")
            .program;

        let records = vec![
            Record {
                payload: Bytes::from_static(br#"{"a":1}"#),
                key: None,
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            },
            Record {
                payload: Bytes::from_static(b"{not json"),
                key: None,
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            },
        ];

        let metrics = TransformMetrics::default();
        let sink_topic: Arc<str> = Arc::from("out");
        let out = transform_records(records, &program, &sink_topic, &metrics, None);
        assert_eq!(out.len(), 1, "only the valid record survives");
    }

    /// scalo's loop retries an `EngineError::Transport` whose error is
    /// recoverable and stops on anything else, so this is the line between a
    /// held block and a stopped pipeline.
    #[test]
    fn a_recoverable_send_failure_holds_the_block_for_a_retry() {
        let metrics = TransformMetrics::default();
        let circuit_open = AtomicBool::new(false);
        let settle = |result| {
            let (piece, _merged) = one_piece();
            settle_send(result, 3, 0.1, "grpc", &metrics, &circuit_open, piece)
        };

        for verdict in [
            settle(SendResult::Backpressured),
            settle(SendResult::Fatal(scalo::TransportError::Timeout)),
            settle(SendResult::Fatal(scalo::TransportError::Backpressure)),
        ] {
            assert!(
                matches!(verdict, Err(EngineError::Transport(ref e)) if e.is_recoverable()),
                "a recoverable failure must come back retryable, got {verdict:?}"
            );
        }
        assert!(
            !circuit_open.load(Ordering::Acquire),
            "a failure worth retrying leaves the outbound circuit closed"
        );
    }

    #[test]
    fn a_permanent_send_failure_stops_the_loop_and_opens_the_circuit() {
        let metrics = TransformMetrics::default();
        let circuit_open = AtomicBool::new(false);

        for permanent in [
            scalo::TransportError::Send("topic authorisation failed".into()),
            scalo::TransportError::Closed,
        ] {
            let (piece, _merged) = one_piece();
            let verdict = settle_send(
                SendResult::Fatal(permanent),
                3,
                0.1,
                "kafka",
                &metrics,
                &circuit_open,
                piece,
            );
            assert!(matches!(verdict, Err(EngineError::Sink(_))), "{verdict:?}");
            assert!(circuit_open.load(Ordering::Acquire));
        }

        let (piece, _merged) = one_piece();
        let delivered = settle_send(
            SendResult::Ok,
            3,
            0.1,
            "kafka",
            &metrics,
            &circuit_open,
            piece,
        );
        assert!(delivered.is_ok());
        assert!(
            !circuit_open.load(Ordering::Acquire),
            "a delivered block closes the circuit again"
        );
    }

    /// The piece a sink call reports into, sealed as the loop seals it, and
    /// the status the block is released with.
    fn one_piece() -> (PieceFinalizer, std::sync::mpsc::Receiver<DeliveryStatus>) {
        let (released, merged) = std::sync::mpsc::channel();
        let block = scalo::transport::BatchFinalizer::new(move |status| {
            let _ = released.send(status);
        });
        let piece = block.piece();
        block.seal();
        (piece, merged)
    }

    /// A block the sender filtered out still releases its source, so it is not
    /// sent again, but as dropped: none of its records was delivered.
    #[test]
    fn a_filtered_block_is_released_dropped_not_delivered() {
        let metrics = TransformMetrics::default();
        let circuit_open = AtomicBool::new(false);
        let released = |result| {
            let (piece, merged) = one_piece();
            let verdict = settle_send(result, 3, 0.1, "grpc", &metrics, &circuit_open, piece);
            (verdict, merged.try_recv().expect("the piece reported"))
        };

        let (verdict, status) = released(SendResult::FilteredDlq);
        assert!(
            verdict.is_ok(),
            "a filtered block is not retried: {verdict:?}"
        );
        assert_eq!(status, DeliveryStatus::Dropped);

        // Any other result leaves the block's status to the loop's own piece.
        for result in [
            SendResult::Ok,
            SendResult::Backpressured,
            SendResult::Fatal(scalo::TransportError::Closed),
        ] {
            let (_, status) = released(result);
            assert_eq!(status, DeliveryStatus::Delivered);
        }
    }

    /// The `message.timeout.ms` the bus sink's producer is built with.
    fn delivery_timeout(config: &Config) -> Option<String> {
        producer_config(config)
            .librdkafka_overrides
            .get("message.timeout.ms")
            .cloned()
    }

    /// A delivery report after the hold is spent comes back to a sender that
    /// was already told to retry, so a held Push caps the producer's timeout.
    #[test]
    fn a_held_push_caps_the_producer_delivery_timeout_inside_the_hold() {
        let mut config = Config::default();
        config.sink.topic = "out".to_string();
        assert_eq!(
            delivery_timeout(&config).as_deref(),
            Some("300000"),
            "the bus source holds no Push, so the configured timeout stands"
        );

        config.source.transport = crate::config::Transport::Direct;
        assert_eq!(delivery_timeout(&config).as_deref(), Some("15000"));
        assert!(u128::from(NEXT_HOP_SEND_TIMEOUT_MS) < PUSH_MAX_HOLD.as_millis());

        config.sink.message_timeout_ms = 5_000;
        assert_eq!(
            delivery_timeout(&config).as_deref(),
            Some("5000"),
            "a shorter configured timeout is kept"
        );

        config.sink.message_timeout_ms = 300_000;
        config.source.acknowledgements = scalo::transport::AcknowledgementsConfig::new(false);
        assert_eq!(
            delivery_timeout(&config).as_deref(),
            Some("300000"),
            "a source answered at receipt holds nothing"
        );

        config.source.acknowledgements = scalo::transport::AcknowledgementsConfig::new(true);
        config
            .sink
            .librdkafka_options
            .insert("message.timeout.ms".to_string(), "60000".to_string());
        assert_eq!(
            delivery_timeout(&config).as_deref(),
            Some("60000"),
            "an explicit librdkafka setting is the operator's"
        );
    }

    use std::time::Duration;

    use scalo::metrics::MetricsManager;
    use scalo::transport::{MemoryConfig, MemoryTransport};
    use scalo::worker::WorkerPoolConfig;
    use scalo::worker::engine::BatchProcessingConfig;

    use crate::metrics::capture::Capture;

    /// `n` JSON records a passthrough program keeps.
    fn json_records(n: usize) -> Vec<Record> {
        (0..n)
            .map(|i| Record {
                payload: Bytes::from(format!(r#"{{"id":{i}}}"#)),
                key: None,
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            })
            .collect()
    }

    /// The platform and app metric groups both name `records_received_total`,
    /// and one name with no labels is one series.
    #[test]
    fn a_received_record_is_counted_once() {
        let capture = Capture::default();
        let manager = MetricsManager::new("test_received_once");
        let program = crate::engine::compiler::compile_vrl(".", None)
            .expect("VRL compile")
            .program;
        let sink_topic: Arc<str> = Arc::from("out");

        let out = metrics::with_local_recorder(&capture, || {
            let m = TransformMetrics::new(&manager, "0.1.0", "ffff");
            transform_records(json_records(3), &program, &sink_topic, &m, None)
        });

        assert_eq!(out.len(), 3);
        assert_eq!(
            capture.counter("records_received_total", &[]),
            Some(3),
            "three records received must count three"
        );
    }

    /// JSON records carrying `keep`, for a program that aborts the rest.
    fn keep_records(keep: &[bool]) -> Vec<Record> {
        keep.iter()
            .enumerate()
            .map(|(i, keep)| Record {
                payload: Bytes::from(format!(r#"{{"id":{i},"keep":{keep}}}"#)),
                key: None,
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            })
            .collect()
    }

    /// A program that aborts every record not marked `keep`.
    fn abort_unkept() -> Program {
        crate::engine::compiler::compile_vrl("if .keep != true { abort }", None)
            .expect("VRL compile")
            .program
    }

    /// The marker carries exactly the records the transform removed, and never
    /// reaches the records the sink sends.
    #[test]
    fn a_block_counts_the_records_the_transform_removed() {
        let program = abort_unkept();
        let metrics = TransformMetrics::default();
        let sink_topic: Arc<str> = Arc::from("out");
        let block = |keep: &[bool]| {
            let out = transform_block(keep_records(keep), &program, &sink_topic, &metrics, None);
            (out.len(), split_dropped(&out))
        };

        let (len, (send, dropped)) = block(&[true, false, true, false]);
        assert_eq!((len, send.len(), dropped), (3, 2, 2));
        assert!(
            send.iter()
                .all(|r| r.headers.is_empty() && !r.payload.is_empty())
        );

        let (len, (send, dropped)) = block(&[false, false]);
        assert_eq!(
            (len, send.len(), dropped),
            (1, 0, 2),
            "an all-dropped block still carries its marker to the sink"
        );

        let (len, (send, dropped)) = block(&[true, true]);
        assert_eq!(
            (len, send.len(), dropped),
            (2, 2, 0),
            "nothing removed, no marker"
        );
    }

    use super::test_ports as ports;

    /// Records VRL removes release their Push `Dropped`, never `Delivered`,
    /// whether some of the request survives or none of it does.
    #[test]
    fn records_vrl_removes_release_their_push_dropped() {
        let capture = Capture::default();
        let sent = metrics::with_local_recorder(&capture, || {
            this_thread_runtime().block_on(async {
                let memory_guard =
                    Arc::new(MemoryGuard::new(scalo::memory::MemoryGuardConfig::default()));
                let mut bound = None;
                for _ in 0..20 {
                    let port = ports::free_port();
                    let mut config = Config::default();
                    config.source.transport = crate::config::Transport::Direct;
                    config.source.listen = format!("127.0.0.1:{port}");
                    if let Ok(listener) = start_push_listener(&config, None, &memory_guard).await {
                        bound = Some((port, listener));
                        break;
                    }
                }
                let (port, listener) = bound.expect("a Push listener on a free port");
                let pusher =
                    GrpcTransport::new(&GrpcConfig::client(&format!("http://127.0.0.1:{port}")))
                        .await
                        .expect("push client");
                let sink = MemoryTransport::new(&MemoryConfig {
                    buffer_size: 16,
                    recv_timeout_ms: 10,
                    ..MemoryConfig::default()
                })
                .expect("memory sink");
                let pool = Arc::new(AdaptiveWorkerPool::new(WorkerPoolConfig {
                    min_threads: 1,
                    max_threads: 1,
                    ..Default::default()
                }));
                let engine = BatchEngine::with_pool(pool, BatchProcessingConfig::default());
                let shutdown = CancellationToken::new();
                let transform_metrics = Arc::new(TransformMetrics::default());

                let run = run_governed_pipeline(
                    &engine,
                    &listener,
                    &sink,
                    Arc::new(abort_unkept()),
                    SharedConfig::new(HotConfig {
                        batch_size: 10,
                        batch_timeout_ms: 50,
                        key_field: String::new(),
                    }),
                    &transform_metrics,
                    Arc::new(AtomicBool::new(false)),
                    shutdown.clone(),
                    None,
                    "out".to_string(),
                    Arc::new(AtomicBool::new(false)),
                );
                let push = async {
                    // Each request is answered only once its records are released.
                    for keep in [&[true, false][..], &[false, false], &[true]] {
                        let answer = pusher.send_batch(&keep_records(keep)).await;
                        assert!(matches!(answer, SendResult::Ok), "{keep:?}: {answer:?}");
                    }
                    let sent = scalo::transport::TransportReceiver::recv(&sink, 16)
                        .await
                        .expect("sink recv")
                        .records;
                    shutdown.cancel();
                    sent
                };
                let (result, sent) = tokio::join!(run, push);
                result.expect("pipeline");
                sent
            })
        });

        let released = |outcome| {
            capture.counter(
                "transport_ack_released_total",
                &[("transport", "grpc"), ("outcome", outcome)],
            )
        };
        assert_eq!(
            released("dropped"),
            Some(2),
            "the two requests VRL cut into"
        );
        assert_eq!(released("delivered"), Some(1), "the request VRL kept whole");
        assert_eq!(sent.len(), 2, "only the kept records are sent");
        assert!(
            sent.iter()
                .all(|r| r.headers.is_empty() && !r.payload.is_empty()),
            "the marker never reaches the sink's wire"
        );
    }

    /// A runtime whose tasks all run on this thread, so a local recorder sees them.
    fn this_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    }

    /// Run three records from a memory source through the pipeline into `sink`,
    /// returning once the sink has delivered all three.
    async fn deliver_three<S: TransportSender>(
        manager: &MetricsManager,
        capture: &Capture,
        sink: &S,
    ) {
        let source = MemoryTransport::new(&MemoryConfig {
            buffer_size: 16,
            recv_timeout_ms: 10,
            ..MemoryConfig::default()
        })
        .expect("memory source");
        for record in json_records(3) {
            let sent = source.send("in", record.payload).await;
            assert!(matches!(sent, SendResult::Ok), "{sent:?}");
        }

        let pool = Arc::new(AdaptiveWorkerPool::new(WorkerPoolConfig {
            min_threads: 1,
            max_threads: 1,
            ..Default::default()
        }));
        let engine = BatchEngine::with_pool(pool, BatchProcessingConfig::default());
        let program = Arc::new(
            crate::engine::compiler::compile_vrl(".", None)
                .expect("VRL compile")
                .program,
        );
        let hot_config = SharedConfig::new(HotConfig {
            batch_size: 10,
            batch_timeout_ms: 50,
            key_field: ".id".to_string(),
        });
        let metrics = Arc::new(TransformMetrics::new(manager, "0.1.0", "ffff"));
        let shutdown = CancellationToken::new();

        let run = run_governed_pipeline(
            &engine,
            &source,
            sink,
            program,
            hot_config,
            &metrics,
            Arc::new(AtomicBool::new(false)),
            shutdown.clone(),
            None,
            "out".to_string(),
            Arc::new(AtomicBool::new(false)),
        );
        let watch = async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while capture.counter("records_delivered_total", &[]) != Some(3) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the sink never delivered the three records"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            shutdown.cancel();
        };
        let (result, ()) = tokio::join!(run, watch);
        result.expect("pipeline");
    }

    /// The sink's metrics name the transport the records went over, and no
    /// series claims one they did not.
    fn assert_sink_metered_as(capture: &Capture, backend: &str) {
        for label in ["transport", "backend"] {
            assert_eq!(
                capture.series_labelled(label, "kafka"),
                Vec::<String>::new(),
                "no record went over Kafka, so no series may say {label}=kafka"
            );
        }
        assert!(
            capture.has_histogram("sink_duration_seconds", &[("backend", backend)]),
            "sink_duration_seconds must carry backend={backend}"
        );
    }

    #[test]
    fn a_grpc_sink_is_metered_as_grpc() {
        let capture = Capture::default();
        let manager = MetricsManager::new("test_grpc_sink");

        metrics::with_local_recorder(&capture, || {
            this_thread_runtime().block_on(async {
                // The downstream stage, the loader on a real deployment.
                let downstream = GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0"))
                    .await
                    .expect("downstream listener");
                let addr = downstream.local_addr().expect("bound address");
                let sink = AnySender::Grpc(
                    GrpcTransport::new(&GrpcConfig::client(&format!("http://{addr}")))
                        .await
                        .expect("sink client"),
                );
                deliver_three(&manager, &capture, &sink).await;
            });
        });

        assert_sink_metered_as(&capture, "grpc");
    }

    #[test]
    fn a_memory_sink_is_metered_as_memory() {
        let capture = Capture::default();
        let manager = MetricsManager::new("test_memory_sink");

        metrics::with_local_recorder(&capture, || {
            this_thread_runtime().block_on(async {
                let sink = MemoryTransport::new(&MemoryConfig {
                    buffer_size: 16,
                    ..MemoryConfig::default()
                })
                .expect("memory sink");
                deliver_three(&manager, &capture, &sink).await;
            });
        });

        assert_sink_metered_as(&capture, "memory");
    }

    // ---- nesting depth ----

    /// The stack a Tokio worker or worker-pool thread gets by default.
    const WORKER_STACK: usize = 2 * 1024 * 1024;

    /// A stack that holds the 254 levels the parse accepts, whose frames
    /// overflow 2 MiB near 128 levels in a debug build.
    const BOUND_STACK: usize = if cfg!(debug_assertions) {
        32 * 1024 * 1024
    } else {
        WORKER_STACK
    };

    /// Run `test` on a thread with `stack` bytes of stack, and fail unless it returns.
    fn on_stack(stack: usize, test: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(stack)
            .spawn(test)
            .expect("spawn the transform thread")
            .join()
            .expect("the transform thread must return");
    }

    fn nested_array(depth: usize) -> Vec<u8> {
        format!("{}1{}", "[".repeat(depth), "]".repeat(depth)).into_bytes()
    }

    fn nested_object(depth: usize) -> Vec<u8> {
        format!("{}1{}", "{\"a\":".repeat(depth), "}".repeat(depth)).into_bytes()
    }

    /// sonic-rs's serde path refuses nesting past 254 levels with an error, so
    /// a record nested far deeper is refused as not JSON, never recursed into.
    #[test]
    fn a_deeply_nested_payload_is_refused_as_not_json() {
        on_stack(BOUND_STACK, || {
            for depth in [20_000, 100_000] {
                for payload in [nested_array(depth), nested_object(depth)] {
                    let refused = deserialize_event(&payload)
                        .expect_err("a record nested this deep must be refused");
                    let message = refused.to_string();
                    assert!(
                        message.contains("payload is not JSON") && message.contains("nesting"),
                        "depth {depth}: {message}"
                    );
                }
            }
        });
    }

    #[test]
    fn the_parse_takes_254_levels_and_refuses_255() {
        on_stack(BOUND_STACK, || {
            for nested in [nested_array, nested_object] {
                assert!(deserialize_event(&nested(254)).is_ok());
                assert!(deserialize_event(&nested(255)).is_err());
            }
        });
    }

    /// A deep record is counted on the deserialise error and removed, and the
    /// rest of its block is transformed.
    #[test]
    fn a_deeply_nested_record_is_dropped_and_the_rest_transformed() {
        on_stack(BOUND_STACK, || {
            let capture = Capture::default();
            let program = crate::engine::compiler::compile_vrl(".ok = true", None)
                .expect("VRL compile")
                .program;
            let sink_topic: Arc<str> = Arc::from("out");
            let mut records = json_records(2);
            for payload in [nested_object(20_000), nested_array(100_000)] {
                records.insert(
                    1,
                    Record {
                        payload: Bytes::from(payload),
                        key: None,
                        headers: Vec::new(),
                        metadata: RecordMeta {
                            timestamp_ms: None,
                            format: PayloadFormat::Json,
                        },
                    },
                );
            }

            let out = metrics::with_local_recorder(&capture, || {
                transform_records(
                    records,
                    &program,
                    &sink_topic,
                    &TransformMetrics::default(),
                    None,
                )
            });

            let ids: Vec<Value> = out
                .iter()
                .map(|r| deserialize_event(&r.payload).unwrap().as_object().unwrap()["id"].clone())
                .collect();
            assert_eq!(ids, vec![Value::from(0), Value::from(1)]);
            assert_eq!(
                capture.counter("records_error_total", &[("stage", "deserialise")]),
                Some(2)
            );
        });
    }
}
