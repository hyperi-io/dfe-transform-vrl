// Project:   dfe-transform-vrl
// File:      tests/e2e/held_acks.rs
// Purpose:   A Push is answered only once Kafka confirms the transformed record
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! gRPC in, Kafka out: the sender of a Push is answered only once the broker
//! has confirmed delivery of what the transform made of it.
//!
//! The broker is frozen with `docker pause` between two pushes. The transform
//! under test keeps the second Push unanswered until the broker is resumed and
//! the record lands. A second transform on the same broker, with
//! `source.acknowledgements.enabled: false`, is the control: it answers at
//! receipt, broker or not, so a green result cannot come from a broker that
//! was never really stalled.
//!
//! The broker is a container this test starts and drops, never a live one: a
//! shared broker must not be paused.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dfe_transform_vrl::config::hot::HotConfig;
use dfe_transform_vrl::config::{Config, Transport};
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::metrics::TransformMetrics;
use dfe_transform_vrl::pipeline;
use scalo::config::shared::SharedConfig;
use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaProfile, KafkaTransport};
use scalo::transport::{
    AcknowledgementsConfig, PayloadFormat, SendResult, TransportBase, TransportReceiver,
    TransportSender,
};
use scalo::worker::engine::BatchProcessingConfig;
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, WorkerPoolConfig};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::common::{self, KafkaTestConfig, ports};

/// The topic both transforms produce to.
const SINK_TOPIC: &str = "held_acks_load";

/// The Push sender's deadline. The transform holds a Push for at most 18 s
/// inside it, which leaves the broker pause below room to recover.
const PUSH_DEADLINE_MS: u64 = 30_000;

/// How long the broker stays frozen while the held Push is watched.
const PAUSE: Duration = Duration::from_secs(3);

fn kafka_config(kf: &KafkaTestConfig, topics: &[&str], group: &str) -> KafkaConfig {
    KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: kf.brokers.split(',').map(String::from).collect(),
        group: group.to_string(),
        client_id: format!("dfe-transform-vrl-held-acks-{group}"),
        topics: topics.iter().map(|t| (*t).to_string()).collect(),
        auto_offset_reset: "earliest".to_string(),
        security_protocol: kf.security_protocol.clone(),
        ..KafkaConfig::devtest()
    }
}

/// Poll until the port accepts a TCP connection.
async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..300 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nothing listening on {addr} within 15s");
}

/// One transform on the direct source and the bus sink, wired as
/// `pipeline::run` wires it -- the Push listener from `start_push_listener`,
/// the producer from `build_sender` -- plus a client that pushes into it.
struct Transform {
    pusher: Arc<GrpcTransport>,
    shutdown: CancellationToken,
    task: JoinHandle<dfe_transform_vrl::Result<()>>,
}

impl Transform {
    async fn start(kf: &KafkaTestConfig, acknowledgements: bool) -> Self {
        let mut config = Config::default();
        config.pipeline.name = format!("held-acks-{acknowledgements}");
        config.source.transport = Transport::Direct;
        config.source.format = "json".to_string();
        config.source.acknowledgements = AcknowledgementsConfig::new(acknowledgements);
        config.sink.brokers = kf.brokers.split(',').map(String::from).collect();
        config.sink.topic = SINK_TOPIC.to_string();
        config.sink.compression = "none".to_string();

        // Bound here rather than inside the pipeline task, so a port another
        // process took first is retried instead of pushed into.
        let memory_guard = Arc::new(MemoryGuard::new(MemoryGuardConfig::default()));
        let mut bound = None;
        for _ in 0..20 {
            let port = ports::free_port();
            config.source.listen = format!("127.0.0.1:{port}");
            if let Ok(listener) = pipeline::start_push_listener(&config, None, &memory_guard).await
            {
                bound = Some((port, listener));
                break;
            }
        }
        let (port, listener) = bound.expect("a Push listener on a free port");
        wait_for_port(port).await;
        let sender = pipeline::build_sender(&config).await.expect("Kafka sink");

        let program = Arc::new(
            compile_vrl(".transformed = true", None)
                .expect("VRL compile")
                .program,
        );
        let pool = Arc::new(AdaptiveWorkerPool::new(WorkerPoolConfig {
            min_threads: 1,
            max_threads: 1,
            ..Default::default()
        }));
        let engine = Arc::new(BatchEngine::with_pool(
            pool,
            BatchProcessingConfig::default(),
        ));
        let hot_config = SharedConfig::new(HotConfig::from_config(&config));
        let shutdown = CancellationToken::new();

        let task = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                pipeline::run_governed_pipeline(
                    &engine,
                    &listener,
                    &sender,
                    program,
                    hot_config,
                    PayloadFormat::Json,
                    &Arc::new(TransformMetrics::default()),
                    Arc::new(AtomicBool::new(false)),
                    shutdown,
                    None,
                    SINK_TOPIC.to_string(),
                    Arc::new(AtomicBool::new(false)),
                )
                .await
            }
        });

        let mut client = GrpcConfig::client(&format!("http://127.0.0.1:{port}"));
        client.send_timeout_ms = PUSH_DEADLINE_MS;
        let pusher = Arc::new(GrpcTransport::new(&client).await.expect("push client"));
        Self {
            pusher,
            shutdown,
            task,
        }
    }

    /// Push one record, returning the answer and when it arrived.
    fn push(&self, id: &str) -> JoinHandle<(SendResult, Instant)> {
        let pusher = Arc::clone(&self.pusher);
        let payload = Bytes::from(format!(r#"{{"id":"{id}"}}"#));
        tokio::spawn(async move {
            let result = pusher.send("held_acks_land", payload).await;
            (result, Instant::now())
        })
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let stopped = tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("the transform stops within 30s of shutdown")
            .expect("the transform task joins");
        assert!(stopped.is_ok(), "the transform stopped with {stopped:?}");
    }
}

/// Read `topic` until every id in `want` has arrived, or `timeout` passes.
async fn ids_on(
    kf: &KafkaTestConfig,
    topic: &str,
    want: &[&str],
    timeout: Duration,
) -> Vec<String> {
    let consumer = KafkaTransport::new(&kafka_config(kf, &[topic], "held-acks-verify"))
        .await
        .expect("verify consumer");
    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + timeout;
    while !want.iter().all(|id| seen.iter().any(|s| s == id)) && Instant::now() < deadline {
        let batch = consumer.recv(100).await.expect("recv on the sink topic");
        for record in &batch.records {
            let event: serde_json::Value =
                serde_json::from_slice(&record.payload).expect("sink payload is JSON");
            assert_eq!(event["transformed"], true, "the VRL program ran: {event}");
            if let Some(id) = event["id"].as_str() {
                seen.push(id.to_string());
            }
        }
    }
    let _ = consumer.close().await;
    seen
}

#[tokio::test]
async fn a_push_is_answered_only_after_kafka_confirms_its_delivery() {
    let Some(env) =
        common::KafkaTestEnv::hermetic_on_low_port("push-held-until-kafka-delivers").await
    else {
        common::require_service_in_ci("Kafka", "no Docker to start a broker container");
        eprintln!("SKIP: no Docker, so this test cannot own a broker.");
        return;
    };
    let kf = env.config();

    KafkaAdmin::new(&kafka_config(kf, &[SINK_TOPIC], "held-acks-admin"))
        .expect("admin client")
        .create_topics(&[(SINK_TOPIC, 1, 1)])
        .await
        .expect("create the sink topic");

    let held = Transform::start(kf, true).await;
    let control = Transform::start(kf, false).await;

    // Both producers connect and deliver while the broker is answering.
    for (transform, id) in [(&held, "warm-held"), (&control, "warm-control")] {
        let (result, _) = tokio::time::timeout(Duration::from_secs(30), transform.push(id))
            .await
            .expect("a push to a healthy pipeline is answered")
            .expect("push task");
        assert!(matches!(result, SendResult::Ok), "{id}: {result:?}");
    }

    env.pause().await.expect("pause the broker");

    // Observed while frozen; asserted only after the broker is resumed, so a
    // failure never leaves a paused container behind.
    let control_while_paused =
        tokio::time::timeout(Duration::from_secs(5), control.push("paused-control")).await;
    let held_push = held.push("paused-held");
    tokio::time::sleep(PAUSE).await;
    let held_answered_while_paused = held_push.is_finished();

    // Taken before the call: the broker is running again before it returns.
    let resuming_at = Instant::now();
    env.unpause().await.expect("unpause the broker");

    let (control_result, _) = control_while_paused
        .expect("with acknowledgements off, a push is answered at receipt, broker or not")
        .expect("push task");
    assert!(
        matches!(control_result, SendResult::Ok),
        "{control_result:?}"
    );

    assert!(
        !held_answered_while_paused,
        "the push was answered while the broker could not confirm delivery"
    );
    let (held_result, answered_at) = tokio::time::timeout(Duration::from_secs(15), held_push)
        .await
        .expect("the held push is answered once the broker is back")
        .expect("push task");
    assert!(
        matches!(held_result, SendResult::Ok),
        "the held push must be answered OK once delivered: {held_result:?}"
    );
    assert!(
        answered_at >= resuming_at,
        "the held push was answered while the broker was frozen"
    );

    let want = ["warm-held", "warm-control", "paused-control", "paused-held"];
    let seen = ids_on(kf, SINK_TOPIC, &want, Duration::from_secs(30)).await;
    for id in want {
        assert!(
            seen.iter().any(|s| s == id),
            "{id} never reached {SINK_TOPIC}; saw {seen:?}"
        );
    }

    held.stop().await;
    control.stop().await;
}
