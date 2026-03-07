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
//! 1. Consume batch from Kafka (msgpack or JSON)
//! 2. Deserialise to VRL Value (auto-sensing format)
//! 3. Run VRL transforms in-process
//! 4. Serialise back to original format
//! 5. Produce to sink Kafka topic
//! 6. Commit consumer offsets after delivery confirmation

// Pipeline implementation will follow once VRL engine and Kafka layer compile.
