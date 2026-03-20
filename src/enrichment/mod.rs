// Project:   dfe-transform-vrl
// File:      src/enrichment/mod.rs
// Purpose:   Enrichment table loading and VRL function integration
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Enrichment tables for VRL transforms.
//!
//! Loads CSV and JSON flat-file enrichment tables at startup into
//! `HashMap<String, ObjectMap>` for O(1) lookups at runtime. Tables are
//! immutable for the process lifetime (K8s restarts on `ConfigMap` change).
//!
//! Custom VRL functions (`get_enrichment_table_record`,
//! `find_enrichment_table_records`) are registered alongside the VRL stdlib,
//! backed by an `Arc<EnrichmentRegistry>` captured at compile time.

pub mod registry;
pub mod vrl_functions;

pub use registry::{EnrichmentRegistry, EnrichmentTable};
