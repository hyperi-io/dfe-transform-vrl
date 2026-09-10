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
//! the loader.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::Bytes;
use dfe_transform_vrl::config::hot::HotConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::metrics::TransformMetrics;
use dfe_transform_vrl::pipeline::run_governed_pipeline;
use scalo::config::shared::SharedConfig;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{AnySender, PayloadFormat, TransportReceiver, TransportSender};
use scalo::worker::engine::BatchProcessingConfig;
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, WorkerPoolConfig};
use tokio_util::sync::CancellationToken;

/// Allocate a free loopback port.
fn random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

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

/// Start a scalo Push listener and return its endpoint plus the transport.
async fn start_listener() -> (String, GrpcTransport) {
    // The port is free when picked but can be taken before the bind lands under
    // parallel CI load, so retry on a fresh one.
    let mut last_err = String::new();
    for _ in 0..20 {
        let port = random_port();
        let config = GrpcConfig::server(&format!("127.0.0.1:{port}"));
        match GrpcTransport::new(&config).await {
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

#[tokio::test]
async fn a_batch_pushed_over_grpc_comes_out_the_grpc_sink_transformed() {
    // The downstream stage (the loader, on a real deployment).
    let (loader_endpoint, loader) = start_listener().await;

    // The transform's own Push listener, and a client that pushes into it.
    let (transform_endpoint, transform_listener) = start_listener().await;
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
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while received.len() < 3 && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(10).await {
            received.extend(batch.records.into_iter().map(|r| r.payload));
        }
    }

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
