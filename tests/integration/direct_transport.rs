// Project:   dfe-transform-vrl
// File:      tests/integration/direct_transport.rs
// Purpose:   Push listener in, gRPC sink out, with no broker in the path
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The transform on the direct transport.
//!
//! A batch pushed at the transform's Push listener comes out of its gRPC sink
//! transformed, with no Kafka anywhere. The downstream listener stands in for
//! the loader. Its sender is answered only once the downstream has confirmed
//! delivery, and a downstream refusal reaches it as `UNAVAILABLE`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dfe_transform_vrl::config::hot::HotConfig;
use dfe_transform_vrl::config::{Config, Transport};
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::metrics::TransformMetrics;
use dfe_transform_vrl::pipeline::{self, run_governed_pipeline};
use scalo::SelfRegulationConfig;
use scalo::SelfRegulationGovernor;
use scalo::config::shared::SharedConfig;
use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
use scalo::transport::grpc::proto::transport_client::TransportClient;
use scalo::transport::grpc::proto::{Format, PushRequest};
use scalo::transport::grpc::{GrpcConfig, GrpcToken, GrpcTransport};
use scalo::transport::{
    AnySender, DeliveryStatus, PayloadFormat, SendResult, TransportReceiver, TransportSender,
};
use scalo::worker::engine::BatchProcessingConfig;
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, WorkerPoolConfig};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::ports;

/// Poll until the port accepts a TCP connection, or fail after 15s.
async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..300 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nothing listening on 127.0.0.1:{port} within 15s");
}

/// Start a scalo Push listener, armed or not, and return its endpoint plus the
/// transport.
async fn start_listener(armed: bool) -> (String, GrpcTransport) {
    // The port is free when picked but can be taken before the bind lands under
    // parallel CI load, so retry on a fresh one.
    let mut last_err = String::new();
    for _ in 0..20 {
        let port = ports::free_port();
        let config = GrpcConfig::server(&format!("127.0.0.1:{port}"));
        match GrpcTransport::builder(&config).armed(armed).start().await {
            Ok(transport) => {
                wait_for_port(port).await;
                return (format!("http://127.0.0.1:{port}"), transport);
            }
            Err(e) => {
                last_err = e.to_string();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("listener failed to start after 20 attempts: {last_err}");
}

/// Start the transform's own Push listener the way `pipeline::run` does, with
/// no pipeline reading it, and return its endpoint plus the transport.
async fn start_transform_listener(
    governor: Option<&SelfRegulationGovernor>,
    memory_guard: &Arc<MemoryGuard>,
) -> (String, GrpcTransport) {
    let mut last_err = String::new();
    for _ in 0..20 {
        let port = ports::free_port();
        let mut config = Config::default();
        config.source.transport = Transport::Direct;
        config.source.listen = format!("127.0.0.1:{port}");
        match pipeline::start_push_listener(&config, governor, memory_guard).await {
            Ok(transport) => {
                wait_for_port(port).await;
                return (format!("http://127.0.0.1:{port}"), transport);
            }
            Err(e) => last_err = e.to_string(),
        }
    }
    panic!("Push listener failed to start after 20 attempts: {last_err}");
}

/// Push one record with a raw client under `deadline`, returning the status
/// and how long the answer took.
async fn raw_push(
    endpoint: String,
    id: &str,
    deadline: Duration,
) -> (Result<(), tonic::Status>, Duration) {
    let mut client = TransportClient::connect(endpoint)
        .await
        .expect("push client");
    let mut request = tonic::Request::new(PushRequest {
        payload: Bytes::from(format!(r#"{{"id":"{id}"}}"#)),
        format: Format::Json.into(),
        metadata: HashMap::new(),
    });
    request.set_timeout(deadline);
    let pushed_at = Instant::now();
    let answer = client.push(request).await.map(|_| ());
    (answer, pushed_at.elapsed())
}

/// A stand-alone batch engine with an explicit 1-thread pool, valid on any
/// core count.
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

/// The transform on the direct transport both ways, wired as `pipeline::run`
/// wires it: the Push listener from `start_push_listener` and the sink from
/// `build_sender`. Returns its Push endpoint, the token that stops it, and its
/// task.
async fn start_transform(
    loader_endpoint: &str,
) -> (
    String,
    CancellationToken,
    JoinHandle<dfe_transform_vrl::Result<()>>,
) {
    let memory_guard = Arc::new(MemoryGuard::new(MemoryGuardConfig::default()));
    let (endpoint, listener) = start_transform_listener(None, &memory_guard).await;
    let mut config = Config::default();
    config.sink.transport = Transport::Direct;
    config.sink.endpoint = loader_endpoint.to_string();
    let sender = pipeline::build_sender(&config).await.expect("gRPC sink");

    let program = Arc::new(
        compile_vrl(".transformed = true", None)
            .expect("VRL compile")
            .program,
    );
    let hot_config = SharedConfig::new(HotConfig::from_config(&config));
    let shutdown = CancellationToken::new();
    let task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            run_governed_pipeline(
                &engine(),
                &listener,
                &sender,
                program,
                hot_config,
                PayloadFormat::Auto,
                &Arc::new(TransformMetrics::default()),
                Arc::new(AtomicBool::new(false)),
                shutdown,
                None,
                "orders_load".to_string(),
                Arc::new(AtomicBool::new(false)),
            )
            .await
        }
    });
    (endpoint, shutdown, task)
}

/// Take what reaches the downstream listener until `want` records have, or
/// `timeout` passes, keeping the tokens it would release them with.
async fn take_from(
    loader: &GrpcTransport,
    want: usize,
    timeout: Duration,
) -> (Vec<Bytes>, Vec<GrpcToken>) {
    let mut payloads = Vec::new();
    let mut tokens = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while payloads.len() < want && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(10).await {
            payloads.extend(batch.records.into_iter().map(|r| r.payload));
            tokens.extend(batch.commit_tokens);
        }
    }
    (payloads, tokens)
}

#[tokio::test]
async fn a_batch_pushed_over_grpc_comes_out_the_grpc_sink_transformed() {
    // The downstream stage (the loader, on a real deployment).
    let (loader_endpoint, loader) = start_listener(false).await;

    // The transform's own Push listener, and a client that pushes into it.
    let (transform_endpoint, transform_listener) = start_listener(false).await;
    let pusher = GrpcTransport::new(&GrpcConfig::client(&transform_endpoint))
        .await
        .expect("push client");

    let sink = AnySender::Grpc(
        GrpcTransport::new(&GrpcConfig::client(&loader_endpoint))
            .await
            .expect("sink client"),
    );

    let program = Arc::new(
        compile_vrl(
            ".level = upcase!(string!(.level)); .transformed = true",
            None,
        )
        .expect("VRL compile")
        .program,
    );
    let hot_config = SharedConfig::new(HotConfig {
        batch_size: 10,
        batch_timeout_ms: 50,
        key_field: ".id".to_string(),
    });
    let shutdown = CancellationToken::new();

    let pipeline_shutdown = shutdown.clone();
    let pipeline = tokio::spawn(async move {
        run_governed_pipeline(
            &engine(),
            &transform_listener,
            &sink,
            program,
            hot_config,
            PayloadFormat::Auto,
            &Arc::new(TransformMetrics::default()),
            Arc::new(AtomicBool::new(false)),
            pipeline_shutdown,
            None,
            "orders_load".to_string(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    });

    for id in 0..3 {
        let result = pusher
            .send(
                "orders_land",
                Bytes::from(format!(r#"{{"id":"e{id}","level":"info"}}"#)),
            )
            .await;
        assert!(result.is_ok(), "push {id} failed: {result:?}");
    }

    // Collect what reached the downstream listener.
    let (received, _) = take_from(&loader, 3, Duration::from_secs(10)).await;

    shutdown.cancel();
    let _ = pipeline.await.expect("pipeline task");

    assert_eq!(
        received.len(),
        3,
        "every pushed record must reach the downstream listener"
    );
    for payload in &received {
        let record: serde_json::Value = serde_json::from_slice(payload).expect("valid JSON");
        assert_eq!(record["level"], "INFO", "the VRL program ran");
        assert_eq!(record["transformed"], true);
    }
}

/// Nothing reads the listener, so nothing can release the push: a listener
/// built armed holds it to its hold budget, where an unarmed one would have
/// answered it OK on arrival with no loop to deliver it.
#[tokio::test]
async fn the_push_listener_holds_from_its_first_push() {
    let memory_guard = Arc::new(MemoryGuard::new(MemoryGuardConfig::default()));
    let (endpoint, listener) = start_transform_listener(None, &memory_guard).await;

    // A 3 s deadline leaves the listener a 2 s hold.
    let (answer, took) = raw_push(endpoint, "unread", Duration::from_secs(3)).await;
    let status = answer.expect_err("a push nothing released must not be answered OK");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    assert!(
        status.metadata().get("scalo-hold-expired").is_some(),
        "the push must have been held to its budget: {status:?}"
    );
    assert!(
        took >= Duration::from_secs(1),
        "answered after only {took:?}"
    );

    let queued = listener.recv(10).await.expect("recv");
    assert_eq!(
        queued.records.len(),
        1,
        "the held push stays queued, for the retry to duplicate rather than lose"
    );
}

/// Under memory pressure the listener refuses a push at the door rather than
/// queueing it.
#[tokio::test]
async fn the_push_listener_refuses_while_the_governor_holds() {
    let memory_guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: 1024 * 1024,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    let governor = SelfRegulationConfig::default()
        .build(Arc::clone(&memory_guard))
        .expect("self-regulation is on by default");
    let (endpoint, listener) = start_transform_listener(Some(&governor), &memory_guard).await;

    memory_guard.add_bytes(1024 * 1024);
    let (answer, took) = raw_push(endpoint, "shed", Duration::from_secs(10)).await;
    let status = answer.expect_err("a push under memory pressure must be refused");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    assert!(
        status.metadata().get("scalo-hold-expired").is_none(),
        "refused at admission, not held: {status:?}"
    );
    assert!(took < Duration::from_secs(5), "refused only after {took:?}");

    let queued = listener.recv(10).await.expect("recv");
    assert!(
        queued.records.is_empty(),
        "a refused push must not be queued"
    );
}

#[tokio::test]
async fn a_push_is_answered_only_once_the_downstream_confirms() {
    // Armed, the downstream answers the transform only when the test releases.
    let (loader_endpoint, loader) = start_listener(true).await;
    let (transform_endpoint, shutdown, transform) = start_transform(&loader_endpoint).await;
    let pusher = Arc::new(
        GrpcTransport::new(&GrpcConfig::client(&transform_endpoint))
            .await
            .expect("push client"),
    );

    let push = tokio::spawn({
        let pusher = Arc::clone(&pusher);
        async move {
            let result = pusher
                .send("orders_land", Bytes::from_static(br#"{"id":"held"}"#))
                .await;
            (result, Instant::now())
        }
    });

    let (received, tokens) = take_from(&loader, 1, Duration::from_secs(10)).await;
    assert_eq!(received.len(), 1, "the record must reach the downstream");
    let record: serde_json::Value = serde_json::from_slice(&received[0]).expect("valid JSON");
    assert_eq!(record["transformed"], true, "the VRL program ran");

    // The record is downstream but not yet confirmed there.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !push.is_finished(),
        "the push was answered before the downstream confirmed delivery"
    );

    let confirmed_at = Instant::now();
    loader
        .release(&tokens, DeliveryStatus::Delivered)
        .await
        .expect("release downstream");
    let (result, answered_at) = tokio::time::timeout(Duration::from_secs(5), push)
        .await
        .expect("the push is answered once the downstream confirms")
        .expect("push task");
    assert!(matches!(result, SendResult::Ok), "{result:?}");
    assert!(answered_at >= confirmed_at);

    shutdown.cancel();
    let stopped = transform.await.expect("transform task");
    assert!(stopped.is_ok(), "{stopped:?}");
}

#[tokio::test]
async fn a_downstream_refusal_answers_the_upstream_unavailable() {
    let (loader_endpoint, loader) = start_listener(true).await;
    let (transform_endpoint, shutdown, transform) = start_transform(&loader_endpoint).await;

    // A raw client, because scalo's folds every retryable code into one
    // `SendResult`. Its 4 s deadline gives the transform a hold of 3 s.
    let deadline = Duration::from_secs(4);
    let pushed_at = Instant::now();
    let push = tokio::spawn(raw_push(transform_endpoint, "refused", deadline));

    // The downstream refuses every copy the transform sends it.
    let mut refused = 0_usize;
    while !push.is_finished() && pushed_at.elapsed() < deadline * 2 {
        let (received, tokens) = take_from(&loader, 1, Duration::from_millis(200)).await;
        if !tokens.is_empty() {
            refused += received.len();
            loader
                .release(&tokens, DeliveryStatus::Errored)
                .await
                .expect("refuse downstream");
        }
    }

    let (answer, answered_after) = push.await.expect("push task");
    let status = answer.expect_err("a refused record must not be answered OK");
    assert!(refused > 0, "the record never reached the downstream");
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "the sender must be told to retry: {status:?}"
    );
    assert!(
        answered_after < deadline,
        "answered after {answered_after:?}, not before the sender's own {deadline:?} deadline"
    );

    shutdown.cancel();
    let stopped = transform.await.expect("transform task");
    assert!(stopped.is_ok(), "{stopped:?}");
}
