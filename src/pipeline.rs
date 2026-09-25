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
//! 2. `process` deserialises each record to a VRL `Value` (auto-sensing format),
//!    runs the compiled VRL program in parallel on the worker pool, and
//!    re-serialises the surviving events back to their original wire format.
//!    Records dropped by a VRL `abort` or a transform error are removed from
//!    the block; the block's `commit_tokens` (the source acks) flow through
//!    untouched, so a fan-in NEVER under-acks the source.
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
use scalo::transport::kafka::{KafkaTransport, total_consumer_lag};
use scalo::transport::{
    AnySender, PayloadFormat, Record, RecordMeta, SendResult, TransportSender, WorkBatch,
};
use scalo::worker::AdaptiveWorkerPool;
use scalo::worker::BatchEngine;
use scalo::worker::engine::EngineError;
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

/// Send deadline to the next hop's Push listener: inside this stage's own
/// [`PUSH_MAX_HOLD`], so a slow hop is retried before the hold runs out.
pub const NEXT_HOP_SEND_TIMEOUT_MS: u64 = 15_000;

// Per-site log spam guards
static DESER_ERRORS: AtomicU64 = AtomicU64::new(0);
static VRL_ERRORS: AtomicU64 = AtomicU64::new(0);
use vrl::compiler::Program;
use vrl::value::Value;

/// Per-record deserialise outcome: `(value, format, index)` on success, or
/// `(index, format, error)` on failure (the index/format keep the failed
/// record traceable for metrics).
type DeserResult = Result<(Value, PayloadFormat, usize), (usize, PayloadFormat, String)>;

/// Per-record VRL outcome: `(value, format, index)` on success, or
/// `(index, error)` on failure (abort vs runtime error discriminated by the
/// [`crate::Error`] variant).
type VrlResult = Result<(Value, PayloadFormat, usize), (usize, crate::Error)>;

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
    let payload_format = kafka::parse_format(&config.source.format);

    info!(
        pipeline = %config.pipeline.name,
        source_transport = ?config.source.transport,
        source_topics = ?config.source.topics,
        sink_transport = ?config.sink.transport,
        sink_topic = %config.sink.topic,
        format = %config.source.format,
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
            payload_format,
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
        payload_format,
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

    let producer_config = kafka::build_producer_config(&config.sink, &config.pipeline.name);
    let transport = KafkaTransport::new(&producer_config)
        .await
        .map_err(|e| crate::Error::Kafka(format!("failed to create producer: {e}")))?;
    Ok(AnySender::Kafka(transport))
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
    payload_format: PayloadFormat,
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
    // `abort` or a transform/(de)serialise error are removed from the block; the
    // commit_tokens flow through untouched via `map_records`, so a fan-in never
    // under-acks the source offsets (at-least-once on the surviving set).
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
                transform_records(
                    records,
                    &program,
                    payload_format,
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
        move |out: &WorkBatch<R::Token>| {
            let transform_metrics = Arc::clone(&transform_metrics);
            let circuit_open = Arc::clone(&circuit_open);
            // The engine's `Sink` bound returns an OWNED future (it cannot borrow
            // `out`), so clone the records into the async block. This is cheap:
            // `Record` is `Bytes` (refcount bump) + `Arc<str>` key -- N refcount
            // bumps, NOT N payload copies. Zero-copy on the wire is preserved.
            let records: Vec<Record> = out.records.clone();
            async move {
                let start = Instant::now();
                let result = sender.send_batch(&records).await;
                settle_send(
                    result,
                    records.len() as u64,
                    start.elapsed().as_secs_f64(),
                    sink_backend,
                    &transform_metrics,
                    &circuit_open,
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
        .run(process, sink)
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
fn settle_send(
    result: SendResult,
    record_count: u64,
    elapsed_secs: f64,
    sink_backend: &'static str,
    transform_metrics: &TransformMetrics,
    circuit_open: &AtomicBool,
) -> Result<(), EngineError> {
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
            // The whole block was outbound-filtered to DLQ. Treat as delivered
            // for release purposes (the records left the sender via the
            // outbound filter), but surface it.
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

/// Transform a block of [`Record`]s through the VRL program.
///
/// Deserialise -> VRL eval (parallel on the worker pool when available) ->
/// re-serialise. Returns ONLY the surviving records; records dropped by a VRL
/// `abort`, a transform error, or a (de)serialise error are removed (and
/// metered). Each surviving record's `key` is set to the sink topic so the
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
    payload_format: PayloadFormat,
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

    // Phase 1: deserialise (CPU-bound parsing) -- parallel via the pool.
    let deser_start = Instant::now();
    let indexed: Vec<(usize, PayloadFormat, Bytes)> = records
        .iter()
        .enumerate()
        .map(|(idx, rec)| {
            let format = if payload_format == PayloadFormat::Auto {
                rec.metadata.format
            } else {
                payload_format
            };
            (idx, format, rec.payload.clone())
        })
        .collect();

    let deser = |(idx, format, payload): &(usize, PayloadFormat, Bytes)| -> DeserResult {
        match deserialize_event(payload, *format) {
            Ok(v) => Ok((v, *format, *idx)),
            Err(e) => Err((*idx, *format, e.to_string())),
        }
    };
    let deser_results: Vec<DeserResult> = match worker_pool {
        Some(pool) => pool.process_batch(&indexed, deser),
        None => indexed.iter().map(deser).collect(),
    };

    let mut events: Vec<(Value, PayloadFormat, usize)> = Vec::with_capacity(batch_len);
    let mut json_count: u64 = 0;
    let mut msgpack_count: u64 = 0;
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
            Err((_idx, _format, e)) => {
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
    if json_count > 0 {
        transform_metrics.record_format("json", json_count);
    }
    if msgpack_count > 0 {
        transform_metrics.record_format("msgpack", msgpack_count);
    }

    // Phase 2: VRL transform (CPU-bound) -- parallel via the pool.
    let vrl_start = Instant::now();
    let vrl = |(value, format, idx): &(Value, PayloadFormat, usize)| -> VrlResult {
        let mut value = value.clone();
        match run_vrl(program, &mut value) {
            Ok(_) => Ok((value, *format, *idx)),
            Err(e) => Err((*idx, e)),
        }
    };
    let vrl_results: Vec<VrlResult> = match worker_pool {
        Some(pool) => pool.process_batch(&events, vrl),
        None => events.iter().map(vrl).collect(),
    };

    let mut transformed: Vec<(Value, PayloadFormat, usize)> = Vec::with_capacity(vrl_results.len());
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

    // Phase 3: serialise the surviving events back to their wire format and
    // build the output records. The Kafka producer routes each record to its
    // `key` (= the sink topic). Partition keying via the routing field is NOT
    // settable through the sender trait (scalo #37: `send`'s key arg IS the
    // destination topic, so there is no slot for a partition key); the routing
    // field stays a config surface only until #37 lands.
    let ser_start = Instant::now();
    let mut out_records: Vec<Record> = Vec::with_capacity(transformed.len());
    for (value, format, _idx) in &transformed {
        match serialize_event(value, *format) {
            Ok(serialized) => {
                out_records.push(Record {
                    payload: Bytes::from(serialized),
                    key: Some(Arc::clone(sink_topic)),
                    headers: Vec::new(),
                    metadata: RecordMeta {
                        timestamp_ms: None,
                        format: *format,
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

/// Deserialise raw bytes to VRL Value using the detected format.
///
/// Uses `sonic_rs` for JSON (SIMD-accelerated, 2-4x faster than `serde_json`).
/// Both produce the same `vrl::value::Value` via serde `Deserialize`.
fn deserialize_event(payload: &[u8], format: PayloadFormat) -> crate::Result<Value> {
    match format {
        PayloadFormat::Json => sonic_rs::from_slice(payload)
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
///
/// Uses `sonic_rs` for JSON (SIMD-accelerated, matching the deserialise path).
fn serialize_event(value: &Value, format: PayloadFormat) -> crate::Result<Vec<u8>> {
    match format {
        PayloadFormat::Json | PayloadFormat::Auto => sonic_rs::to_vec(value)
            .map_err(|e| crate::Error::Serialisation(format!("JSON serialise: {e}"))),
        PayloadFormat::MsgPack => rmp_serde::to_vec(value)
            .map_err(|e| crate::Error::Serialisation(format!("msgpack serialise: {e}"))),
    }
}

// NB: the dot-path partition-key extractor (`extract_key`) was removed in the
// WorkBatch migration. scalo #37 means the Kafka sender's `key` arg IS the
// destination topic (no slot for a partition key via `send`/`send_batch`), so
// the produce path keys on the sink topic and there is nothing for an extractor
// to feed. Re-introduce a per-record partition key once #37 lands.

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

        let out = batch.map_records(|recs| {
            transform_records(
                recs,
                &program,
                PayloadFormat::Json,
                &sink_topic,
                &metrics,
                None,
            )
        });

        // 2 survived the abort fan-in...
        assert_eq!(out.records.len(), 2, "two records dropped by abort");
        // ...but the source acks are untouched.
        assert_eq!(out.commit_tokens, vec![TestToken(10), TestToken(11)]);
        // surviving records route to the sink topic.
        for r in &out.records {
            assert_eq!(r.key.as_deref(), Some("out"));
            let v = deserialize_event(&r.payload, PayloadFormat::Json).unwrap();
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
        let out = transform_records(
            records,
            &program,
            PayloadFormat::Json,
            &sink_topic,
            &metrics,
            None,
        );
        assert_eq!(out.len(), 1, "only the valid record survives");
    }

    /// scalo's loop retries an `EngineError::Transport` whose error is
    /// recoverable and stops on anything else, so this is the line between a
    /// held block and a stopped pipeline.
    #[test]
    fn a_recoverable_send_failure_holds_the_block_for_a_retry() {
        let metrics = TransformMetrics::default();
        let circuit_open = AtomicBool::new(false);
        let settle = |result| settle_send(result, 3, 0.1, "grpc", &metrics, &circuit_open);

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
            let verdict = settle_send(
                SendResult::Fatal(permanent),
                3,
                0.1,
                "kafka",
                &metrics,
                &circuit_open,
            );
            assert!(matches!(verdict, Err(EngineError::Sink(_))), "{verdict:?}");
            assert!(circuit_open.load(Ordering::Acquire));
        }

        let delivered = settle_send(SendResult::Ok, 3, 0.1, "kafka", &metrics, &circuit_open);
        assert!(delivered.is_ok());
        assert!(
            !circuit_open.load(Ordering::Acquire),
            "a delivered block closes the circuit again"
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
            transform_records(
                json_records(3),
                &program,
                PayloadFormat::Json,
                &sink_topic,
                &m,
                None,
            )
        });

        assert_eq!(out.len(), 3);
        assert_eq!(
            capture.counter("records_received_total", &[]),
            Some(3),
            "three records received must count three"
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
            PayloadFormat::Auto,
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
}
