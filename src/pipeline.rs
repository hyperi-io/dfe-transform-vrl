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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hyperi_rustlib::transport::{PayloadFormat, SendResult, Transport};
use tracing::{debug, error, info, warn};
use vrl::compiler::Program;
use vrl::value::Value;

use crate::config::Config;
use crate::engine::runner::run_vrl;
use crate::kafka;
use crate::metrics::TransformMetrics;

/// Run the transform pipeline with Kafka transports (production entry point).
pub async fn run(
    config: &Config,
    program: Arc<Program>,
    transform_metrics: &TransformMetrics,
    ready_flag: Arc<AtomicBool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
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
        &config.sink.key_field,
        config.pipeline.batch_size,
        config.pipeline.batch_timeout_ms,
        payload_format,
        transform_metrics,
        ready_flag,
        shutdown_rx,
    )
    .await
}

/// Run the transform pipeline with any `Transport` implementation.
///
/// Generic over `T: Transport` so the same pipeline logic works with
/// `KafkaTransport` (production) and `MemoryTransport` (tests).
#[allow(clippy::too_many_arguments)]
pub async fn run_with_transport<T: Transport>(
    consumer: &T,
    producer: &T,
    program: Arc<Program>,
    key_field: &str,
    batch_size: usize,
    batch_timeout_ms: u64,
    payload_format: PayloadFormat,
    transform_metrics: &TransformMetrics,
    ready_flag: Arc<AtomicBool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> crate::Result<()> {
    let batch_timeout = Duration::from_millis(batch_timeout_ms);

    ready_flag.store(true, Ordering::Release);
    info!("pipeline ready — entering event loop");

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("shutdown signal received, draining pipeline");
                break;
            }
            result = process_batch(
                consumer,
                producer,
                &program,
                key_field,
                batch_size,
                batch_timeout,
                payload_format,
                transform_metrics,
            ) => {
                if let Err(e) = result {
                    error!(error = %e, "batch processing error");
                }
            }
        }
    }

    ready_flag.store(false, Ordering::Release);
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
#[allow(clippy::too_many_arguments, clippy::cast_precision_loss)]
async fn process_batch<T: Transport>(
    consumer: &T,
    producer: &T,
    program: &Program,
    key_field: &str,
    batch_size: usize,
    batch_timeout: Duration,
    payload_format: PayloadFormat,
    transform_metrics: &TransformMetrics,
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

    let batch_len = messages.len();
    transform_metrics
        .events_received
        .increment(batch_len as u64);
    transform_metrics.batch_size.record(batch_len as f64);
    debug!(count = batch_len, "consumed batch");

    let timer = Instant::now();

    let mut commit_tokens = Vec::with_capacity(batch_len);
    let mut produced_count: u64 = 0;
    let mut failed_count: u64 = 0;

    for msg in &messages {
        let format = if payload_format == PayloadFormat::Auto {
            msg.format
        } else {
            payload_format
        };

        let deser_result = deserialize_event(&msg.payload, format);
        let mut value = match deser_result {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "failed to deserialise event, skipping");
                failed_count += 1;
                commit_tokens.push(msg.token.clone());
                continue;
            }
        };

        match run_vrl(program, &mut value) {
            Ok(_) => {}
            Err(crate::Error::VrlAbort(ref reason)) => {
                debug!(reason = %reason, "event dropped by VRL abort");
                transform_metrics.events_filtered.increment(1);
                commit_tokens.push(msg.token.clone());
                continue;
            }
            Err(e) => {
                warn!(error = %e, "VRL transform error, skipping event");
                failed_count += 1;
                commit_tokens.push(msg.token.clone());
                continue;
            }
        }

        let serialized = serialize_event(&value, format)?;

        let key = extract_key(&value, key_field);
        let key_str = key.as_deref().unwrap_or("");

        match producer.send(key_str, &serialized).await {
            SendResult::Ok => {
                produced_count += 1;
            }
            SendResult::Backpressured => {
                warn!("producer backpressure, retrying after yield");
                tokio::task::yield_now().await;
                match producer.send(key_str, &serialized).await {
                    SendResult::Ok => produced_count += 1,
                    other => {
                        error!(result = ?other, "produce failed after retry");
                        failed_count += 1;
                    }
                }
            }
            SendResult::Fatal(e) => {
                error!(error = %e, "fatal produce error");
                return Err(crate::Error::Kafka(format!("produce failed: {e}")));
            }
        }

        commit_tokens.push(msg.token.clone());
    }

    let elapsed = timer.elapsed();
    transform_metrics
        .transform_duration
        .record(elapsed.as_secs_f64());
    transform_metrics.events_produced.increment(produced_count);
    transform_metrics.events_failed.increment(failed_count);

    if !commit_tokens.is_empty() {
        consumer
            .commit(&commit_tokens)
            .await
            .map_err(|e| crate::Error::Kafka(format!("offset commit error: {e}")))?;
    }

    debug!(
        produced = produced_count,
        failed = failed_count,
        elapsed_ms = elapsed.as_millis(),
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
}
