// Project:   dfe-transform-vrl
// File:      src/kafka/mod.rs
// Purpose:   Kafka consumer and producer with offset tracking
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Wrapper-controlled Kafka consumer and producer.
//!
//! Unlike dfe-transform-vector where Vector owns the Kafka source/sink,
//! this module manages rdkafka directly with bounded buffers and
//! watermark-based offset commit (at-least-once guarantee).

// Kafka consumer/producer implementation will follow once the VRL engine
// compiles. The key types from rdkafka:
//
// - StreamConsumer — async consumer with configurable fetch/buffer sizes
// - FutureProducer — async producer with delivery confirmation futures
// - TopicPartitionList — offset commit tracking
