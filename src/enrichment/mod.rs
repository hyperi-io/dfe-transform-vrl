// Project:   dfe-transform-vrl
// File:      src/enrichment/mod.rs
// Purpose:   Enrichment table loading and VRL function integration
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Multi-source enrichment engine for VRL transforms.
//!
//! Loads enrichment data from CSV, JSON, YAML, MMDB, STIX, and `SQLite` sources
//! into `FxHashMap`-backed tables for O(1) lookups at PB/hr scale. Tables
//! support atomic hot-reload via `ArcSwap`.
//!
//! Custom VRL functions (`get_enrichment_table_record`,
//! `find_enrichment_table_records`) are registered alongside the VRL stdlib,
//! backed by an `Arc<EnrichmentRegistry>` captured at compile time.

pub mod loader;
pub mod refresh;
pub mod registry;
pub mod stix;
pub mod table;
pub mod vrl_functions;

pub use registry::EnrichmentRegistry;
pub use table::EnrichmentTable;
