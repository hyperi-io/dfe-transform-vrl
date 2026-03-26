//! Project:   dfe-transform-vrl
//! File:      src/enrichment/table.rs
//! Purpose:   High-performance enrichment table backed by `FxHashMap` + `ArcSwap`
//! Language:  Rust
//!
//! License:   FSL-1.1-ALv2
//! Copyright: (c) 2026 HYPERI PTY LIMITED

use arc_swap::ArcSwap;
use rustc_hash::FxHashMap;
use std::sync::Arc;
use vrl::value::{KeyString, ObjectMap, Value};

// ---------------------------------------------------------------------------
// CompactKey
// ---------------------------------------------------------------------------

/// Compact binary key for enrichment table lookups.
///
/// Column values are joined with a `\x00` null-byte separator so that
/// multi-column keys remain unambiguous. `Value::Bytes(b)` contributes its
/// raw bytes; all other `Value` variants contribute `format!("{v}")`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompactKey(Box<[u8]>);

impl CompactKey {
    /// Build a key from a VRL event row using the given key columns.
    ///
    /// Returns `None` if any key column is absent from `condition`.
    /// Intended for lookup-side calls where a missing column means "no match".
    pub fn from_condition(condition: &ObjectMap, key_columns: &[String]) -> Option<Self> {
        let mut buf: Vec<u8> = Vec::new();
        let mut first = true;

        for col in key_columns {
            let key = KeyString::from(col.as_str());
            let value = condition.get(&key)?;

            if !first {
                buf.push(b'\x00');
            }
            first = false;

            match value {
                Value::Bytes(b) => buf.extend_from_slice(b),
                other => buf.extend_from_slice(format!("{other}").as_bytes()),
            }
        }

        Some(Self(buf.into_boxed_slice()))
    }

    /// Build a key from a data row using the given key columns.
    ///
    /// Infallible: columns absent from `row` contribute empty bytes.
    /// Intended for index-build time when loading table rows.
    pub fn from_row(row: &ObjectMap, key_columns: &[String]) -> Self {
        let mut buf: Vec<u8> = Vec::new();
        let mut first = true;

        for col in key_columns {
            let key = KeyString::from(col.as_str());

            if !first {
                buf.push(b'\x00');
            }
            first = false;

            if let Some(value) = row.get(&key) {
                match value {
                    Value::Bytes(b) => buf.extend_from_slice(b),
                    other => buf.extend_from_slice(format!("{other}").as_bytes()),
                }
            }
            // Missing column → contributes empty bytes (separator still written)
        }

        Self(buf.into_boxed_slice())
    }

    /// Raw bytes of this key (for memory estimation).
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// TableData
// ---------------------------------------------------------------------------

/// Storage backend for an enrichment table.
pub enum TableData {
    /// Lock-free hash-map backed by `ArcSwap` for zero-downtime hot-reload.
    HashMap(ArcSwap<FxHashMap<CompactKey, Arc<ObjectMap>>>),

    /// `GeoIP2` `MaxMind` database reader (requires `enrichment-mmdb` feature).
    #[cfg(feature = "enrichment-mmdb")]
    Mmdb(Arc<maxminddb::Reader<Vec<u8>>>),
}

impl TableData {
    /// Look up a single record by exact key match.
    ///
    /// Returns `None` when no matching record exists.
    pub fn get_record(
        &self,
        condition: &ObjectMap,
        key_columns: &[String],
    ) -> Option<Arc<ObjectMap>> {
        match self {
            Self::HashMap(swap) => {
                let key = CompactKey::from_condition(condition, key_columns)?;
                let guard = swap.load();
                guard.get(&key).map(Arc::clone)
            }

            #[cfg(feature = "enrichment-mmdb")]
            Self::Mmdb(_reader) => {
                // MMDB lookup requires validated maxminddb API integration.
                // Tracked in TODO — will be implemented with proper API research.
                None
            }
        }
    }

    /// Return all records whose fields match every entry in `condition`.
    ///
    /// For the `HashMap` variant this performs an O(n) scan.
    /// For the `Mmdb` variant at most one record is returned.
    pub fn find_records(&self, condition: &ObjectMap) -> Vec<Arc<ObjectMap>> {
        match self {
            Self::HashMap(swap) => {
                let guard = swap.load();
                guard
                    .values()
                    .filter(|row| record_matches_condition(row, condition))
                    .map(Arc::clone)
                    .collect()
            }

            #[cfg(feature = "enrichment-mmdb")]
            Self::Mmdb(_) => Vec::new(),
        }
    }

    /// Number of records in the table (approximate for MMDB).
    pub fn len(&self) -> usize {
        match self {
            Self::HashMap(swap) => swap.load().len(),
            #[cfg(feature = "enrichment-mmdb")]
            Self::Mmdb(_) => 0, // MMDB has no simple record count API
        }
    }

    /// Returns `true` if the table contains no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// EnrichmentTable
// ---------------------------------------------------------------------------

/// Named enrichment table with an associated storage backend.
///
/// Stores its source config and refresh config so the refresh loop can
/// reload without external state (per spec thread-safety model).
pub struct EnrichmentTable {
    name: Arc<str>,
    data: TableData,
    /// Columns that form the composite lookup key.  `None` for backends (e.g.
    /// MMDB) where the lookup key is implicit (an IP address in `condition`).
    key_columns: Option<Arc<[String]>>,
    /// Source config (for refresh loop reloading).
    source_config: Option<crate::config::EnrichmentSourceConfig>,
    /// Refresh config (interval, etc.).
    refresh_config: Option<crate::config::RefreshConfig>,
}

impl EnrichmentTable {
    /// Construct a hash-map-backed table with the given key columns.
    pub fn new_hashmap(
        name: &str,
        rows: FxHashMap<CompactKey, Arc<ObjectMap>>,
        key_columns: Vec<String>,
        source_config: Option<crate::config::EnrichmentSourceConfig>,
        refresh_config: Option<crate::config::RefreshConfig>,
    ) -> Self {
        Self {
            name: Arc::from(name),
            data: TableData::HashMap(ArcSwap::from_pointee(rows)),
            key_columns: Some(key_columns.into()),
            source_config,
            refresh_config,
        }
    }

    /// Construct an MMDB-backed table.
    #[cfg(feature = "enrichment-mmdb")]
    pub fn new_mmdb(
        name: &str,
        reader: maxminddb::Reader<Vec<u8>>,
        source_config: Option<crate::config::EnrichmentSourceConfig>,
        refresh_config: Option<crate::config::RefreshConfig>,
    ) -> Self {
        Self {
            name: Arc::from(name),
            data: TableData::Mmdb(Arc::new(reader)),
            key_columns: None,
            source_config,
            refresh_config,
        }
    }

    /// Look up a single record by exact key match.
    pub fn get_record(&self, condition: &ObjectMap) -> Option<Arc<ObjectMap>> {
        let cols = self.key_columns.as_deref().unwrap_or(&[]);
        self.data.get_record(condition, cols)
    }

    /// Return all records whose fields match every entry in `condition`.
    pub fn find_records(&self, condition: &ObjectMap) -> Vec<Arc<ObjectMap>> {
        self.data.find_records(condition)
    }

    /// The table name as configured.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Number of records in the table.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the table contains no records.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Key columns used for composite lookup keys.
    pub fn key_columns(&self) -> Option<&[String]> {
        self.key_columns.as_deref()
    }

    /// Source config (for refresh loop).
    pub const fn source_config(&self) -> Option<&crate::config::EnrichmentSourceConfig> {
        self.source_config.as_ref()
    }

    /// Refresh config (for refresh loop).
    pub const fn refresh_config(&self) -> Option<&crate::config::RefreshConfig> {
        self.refresh_config.as_ref()
    }

    /// Estimate the memory footprint of this table in bytes.
    pub fn estimated_bytes(&self) -> u64 {
        match &self.data {
            TableData::HashMap(swap) => {
                crate::enrichment::loader::estimate_table_bytes(&swap.load())
            }
            #[cfg(feature = "enrichment-mmdb")]
            TableData::Mmdb(_) => 0, // MMDB size not easily estimated
        }
    }

    /// Atomically replace the inner hash-map.
    ///
    /// Readers holding a `load()` guard continue to see the old map until
    /// they drop it; all subsequent loads see the new map.
    ///
    /// Returns `Err` if this table is not hash-map-backed.
    pub fn swap_hashmap(
        &self,
        new_rows: FxHashMap<CompactKey, Arc<ObjectMap>>,
    ) -> Result<(), SwapError> {
        match &self.data {
            TableData::HashMap(swap) => {
                swap.store(Arc::new(new_rows));
                Ok(())
            }
            #[cfg(feature = "enrichment-mmdb")]
            TableData::Mmdb(_) => Err(SwapError::NotHashMap),
        }
    }
}

// ---------------------------------------------------------------------------
// SwapError
// ---------------------------------------------------------------------------

/// Errors returned by [`EnrichmentTable::swap_hashmap`].
#[derive(Debug, thiserror::Error)]
pub enum SwapError {
    #[error("table is not hash-map-backed; swap is not supported")]
    NotHashMap,
}

impl std::fmt::Debug for EnrichmentTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrichmentTable")
            .field("name", &self.name)
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Returns `true` if every key-value pair in `condition` appears in `row`.
fn record_matches_condition(row: &ObjectMap, condition: &ObjectMap) -> bool {
    condition.iter().all(|(k, v)| row.get(k) == Some(v))
}

// MMDB helper functions (mmdb_ip_from_condition, mmdb_to_object_map) are
// deferred until maxminddb API is properly researched and a test .mmdb
***REMOVED***
// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use vrl::value::{ObjectMap, Value};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn bytes_value(s: &str) -> Value {
        Value::Bytes(s.as_bytes().to_vec().into())
    }

    fn make_row(pairs: &[(&str, Value)]) -> ObjectMap {
        let mut map = ObjectMap::new();
        for (k, v) in pairs {
            map.insert(KeyString::from(*k), v.clone());
        }
        map
    }

    fn make_hashmap_table(
        name: &str,
        key_columns: Vec<String>,
        rows: Vec<ObjectMap>,
    ) -> EnrichmentTable {
        let mut hm: FxHashMap<CompactKey, Arc<ObjectMap>> = FxHashMap::default();
        for row in rows {
            let key = CompactKey::from_row(&row, &key_columns);
            hm.insert(key, Arc::new(row));
        }
        EnrichmentTable::new_hashmap(name, hm, key_columns, None, None)
    }

    // -----------------------------------------------------------------------
    // CompactKey::from_condition
    // -----------------------------------------------------------------------

    #[test]
    fn compact_key_from_condition_single_column_present() {
        let row = make_row(&[("country", bytes_value("AU"))]);
        let key = CompactKey::from_condition(&row, &["country".to_string()]);
        assert!(key.is_some());
        assert_eq!(key.unwrap().0.as_ref(), b"AU");
    }

    #[test]
    fn compact_key_from_condition_missing_column_returns_none() {
        let row = make_row(&[("city", bytes_value("Sydney"))]);
        let key = CompactKey::from_condition(&row, &["country".to_string()]);
        assert!(key.is_none());
    }

    #[test]
    fn compact_key_from_condition_multi_column_separator() {
        let row = make_row(&[
            ("country", bytes_value("AU")),
            ("city", bytes_value("Sydney")),
        ]);
        let key =
            CompactKey::from_condition(&row, &["country".to_string(), "city".to_string()]).unwrap();
        assert_eq!(key.0.as_ref(), b"AU\x00Sydney");
    }

    #[test]
    fn compact_key_from_condition_one_column_missing_in_multi() {
        let row = make_row(&[("country", bytes_value("AU"))]);
        let key = CompactKey::from_condition(&row, &["country".to_string(), "city".to_string()]);
        assert!(key.is_none());
    }

    #[test]
    fn compact_key_from_condition_non_bytes_value() {
        let mut row = ObjectMap::new();
        row.insert(KeyString::from("code"), Value::Integer(42));
        let key = CompactKey::from_condition(&row, &["code".to_string()]).unwrap();
        assert_eq!(key.0.as_ref(), b"42");
    }

    #[test]
    fn compact_key_from_condition_empty_columns_list() {
        let row = make_row(&[("x", bytes_value("1"))]);
        let key = CompactKey::from_condition(&row, &[]).unwrap();
        assert_eq!(key.0.as_ref(), b"");
    }

    // -----------------------------------------------------------------------
    // CompactKey::from_row
    // -----------------------------------------------------------------------

    #[test]
    fn compact_key_from_row_single_column() {
        let row = make_row(&[("country", bytes_value("US"))]);
        let key = CompactKey::from_row(&row, &["country".to_string()]);
        assert_eq!(key.0.as_ref(), b"US");
    }

    #[test]
    fn compact_key_from_row_missing_column_produces_empty_segment() {
        let row = make_row(&[("city", bytes_value("NY"))]);
        let key = CompactKey::from_row(&row, &["country".to_string(), "city".to_string()]);
        // country missing → empty segment, separator, then "NY"
        assert_eq!(key.0.as_ref(), b"\x00NY");
    }

    #[test]
    fn compact_key_from_row_multi_column_all_present() {
        let row = make_row(&[
            ("country", bytes_value("DE")),
            ("city", bytes_value("Berlin")),
        ]);
        let key = CompactKey::from_row(&row, &["country".to_string(), "city".to_string()]);
        assert_eq!(key.0.as_ref(), b"DE\x00Berlin");
    }

    #[test]
    fn compact_key_from_row_integer_value() {
        let mut row = ObjectMap::new();
        row.insert(KeyString::from("code"), Value::Integer(7));
        let key = CompactKey::from_row(&row, &["code".to_string()]);
        assert_eq!(key.0.as_ref(), b"7");
    }

    #[test]
    fn compact_key_from_row_empty_columns_list() {
        let row = make_row(&[("x", bytes_value("ignored"))]);
        let key = CompactKey::from_row(&row, &[]);
        assert_eq!(key.0.as_ref(), b"");
    }

    // -----------------------------------------------------------------------
    // CompactKey equality and hashing (critical for FxHashMap use)
    // -----------------------------------------------------------------------

    #[test]
    fn compact_key_equality_same_input() {
        let row = make_row(&[("country", bytes_value("JP"))]);
        let a = CompactKey::from_row(&row, &["country".to_string()]);
        let b = CompactKey::from_condition(&row, &["country".to_string()]).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn compact_key_inequality_different_values() {
        let r1 = make_row(&[("country", bytes_value("AU"))]);
        let r2 = make_row(&[("country", bytes_value("US"))]);
        let k1 = CompactKey::from_row(&r1, &["country".to_string()]);
        let k2 = CompactKey::from_row(&r2, &["country".to_string()]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn compact_key_in_fxhashmap() {
        let row = make_row(&[("k", bytes_value("v"))]);
        let key = CompactKey::from_row(&row, &["k".to_string()]);
        let mut hm: FxHashMap<CompactKey, u32> = FxHashMap::default();
        hm.insert(key, 99);
        let lookup = CompactKey::from_condition(&row, &["k".to_string()]).unwrap();
        assert_eq!(hm.get(&lookup), Some(&99));
    }

    // -----------------------------------------------------------------------
    // EnrichmentTable — construction and basic metadata
    // -----------------------------------------------------------------------

    #[test]
    fn table_name_returned_correctly() {
        let table = make_hashmap_table("geoip", vec!["country".to_string()], vec![]);
        assert_eq!(table.name(), "geoip");
    }

    #[test]
    fn table_len_empty() {
        let table = make_hashmap_table("t", vec!["k".to_string()], vec![]);
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn table_len_with_rows() {
        let rows = vec![
            make_row(&[("country", bytes_value("AU"))]),
            make_row(&[("country", bytes_value("US"))]),
        ];
        let table = make_hashmap_table("t", vec!["country".to_string()], rows);
        assert_eq!(table.len(), 2);
        assert!(!table.is_empty());
    }

    // -----------------------------------------------------------------------
    // EnrichmentTable::get_record
    // -----------------------------------------------------------------------

    #[test]
    fn get_record_hit() {
        let row = make_row(&[
            ("country", bytes_value("AU")),
            ("capital", bytes_value("Canberra")),
        ]);
        let table = make_hashmap_table("t", vec!["country".to_string()], vec![row]);

        let condition = make_row(&[("country", bytes_value("AU"))]);
        let result = table.get_record(&condition);
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(
            record.get(&KeyString::from("capital")),
            Some(&bytes_value("Canberra"))
        );
    }

    #[test]
    fn get_record_miss() {
        let row = make_row(&[("country", bytes_value("AU"))]);
        let table = make_hashmap_table("t", vec!["country".to_string()], vec![row]);

        let condition = make_row(&[("country", bytes_value("NZ"))]);
        assert!(table.get_record(&condition).is_none());
    }

    #[test]
    fn get_record_missing_key_column_in_condition() {
        let row = make_row(&[("country", bytes_value("AU"))]);
        let table = make_hashmap_table("t", vec!["country".to_string()], vec![row]);

        let condition = make_row(&[("city", bytes_value("Brisbane"))]);
        assert!(table.get_record(&condition).is_none());
    }

    #[test]
    fn get_record_multi_column_key_hit() {
        let row = make_row(&[
            ("country", bytes_value("AU")),
            ("city", bytes_value("Sydney")),
            ("pop", bytes_value("5_000_000")),
        ]);
        let table = make_hashmap_table(
            "t",
            vec!["country".to_string(), "city".to_string()],
            vec![row],
        );

        let condition = make_row(&[
            ("country", bytes_value("AU")),
            ("city", bytes_value("Sydney")),
        ]);
        let record = table.get_record(&condition).unwrap();
        assert_eq!(
            record.get(&KeyString::from("pop")),
            Some(&bytes_value("5_000_000"))
        );
    }

    #[test]
    fn get_record_multi_column_key_partial_miss() {
        let row = make_row(&[
            ("country", bytes_value("AU")),
            ("city", bytes_value("Sydney")),
        ]);
        let table = make_hashmap_table(
            "t",
            vec!["country".to_string(), "city".to_string()],
            vec![row],
        );

        // Wrong city
        let condition = make_row(&[
            ("country", bytes_value("AU")),
            ("city", bytes_value("Melbourne")),
        ]);
        assert!(table.get_record(&condition).is_none());
    }

    // -----------------------------------------------------------------------
    // EnrichmentTable::find_records
    // -----------------------------------------------------------------------

    #[test]
    fn find_records_all_match() {
        let r1 = make_row(&[("type", bytes_value("A")), ("val", bytes_value("x"))]);
        let r2 = make_row(&[("type", bytes_value("A")), ("val", bytes_value("y"))]);
        let r3 = make_row(&[("type", bytes_value("B")), ("val", bytes_value("z"))]);
        let table = make_hashmap_table("t", vec!["val".to_string()], vec![r1, r2, r3]);

        let condition = make_row(&[("type", bytes_value("A"))]);
        let mut results = table.find_records(&condition);
        results.sort_by(|a, b| {
            let av = a.get(&KeyString::from("val")).unwrap();
            let bv = b.get(&KeyString::from("val")).unwrap();
            format!("{av}").cmp(&format!("{bv}"))
        });
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].get(&KeyString::from("val")),
            Some(&bytes_value("x"))
        );
        assert_eq!(
            results[1].get(&KeyString::from("val")),
            Some(&bytes_value("y"))
        );
    }

    #[test]
    fn find_records_no_match() {
        let row = make_row(&[("country", bytes_value("AU"))]);
        let table = make_hashmap_table("t", vec!["country".to_string()], vec![row]);

        let condition = make_row(&[("country", bytes_value("NZ"))]);
        assert!(table.find_records(&condition).is_empty());
    }

    #[test]
    fn find_records_empty_condition_matches_all() {
        let r1 = make_row(&[("x", bytes_value("1"))]);
        let r2 = make_row(&[("x", bytes_value("2"))]);
        let table = make_hashmap_table("t", vec!["x".to_string()], vec![r1, r2]);

        let condition = ObjectMap::new();
        let results = table.find_records(&condition);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn find_records_empty_table() {
        let table = make_hashmap_table("t", vec!["x".to_string()], vec![]);
        let condition = make_row(&[("x", bytes_value("1"))]);
        assert!(table.find_records(&condition).is_empty());
    }

    // -----------------------------------------------------------------------
    // EnrichmentTable::swap_hashmap
    // -----------------------------------------------------------------------

    #[test]
    fn swap_hashmap_replaces_data_atomically() {
        let old_row = make_row(&[("k", bytes_value("old"))]);
        let table = make_hashmap_table("t", vec!["k".to_string()], vec![old_row]);

        let condition = make_row(&[("k", bytes_value("old"))]);
        assert!(table.get_record(&condition).is_some());

        // Build new map without the old row
        let new_hm: FxHashMap<CompactKey, Arc<ObjectMap>> = FxHashMap::default();
        table.swap_hashmap(new_hm).unwrap();

        // Old lookup now returns None
        assert!(table.get_record(&condition).is_none());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn swap_hashmap_with_new_rows() {
        let table = make_hashmap_table("t", vec!["k".to_string()], vec![]);

        let new_row = make_row(&[("k", bytes_value("fresh")), ("v", bytes_value("data"))]);
        let key = CompactKey::from_row(&new_row, &["k".to_string()]);
        let mut new_hm: FxHashMap<CompactKey, Arc<ObjectMap>> = FxHashMap::default();
        new_hm.insert(key, Arc::new(new_row));

        table.swap_hashmap(new_hm).unwrap();

        let condition = make_row(&[("k", bytes_value("fresh"))]);
        let record = table.get_record(&condition).unwrap();
        assert_eq!(
            record.get(&KeyString::from("v")),
            Some(&bytes_value("data"))
        );
    }

    // -----------------------------------------------------------------------
    // record_matches_condition helper
    // -----------------------------------------------------------------------

    #[test]
    fn record_matches_condition_all_match() {
        let row = make_row(&[("a", bytes_value("1")), ("b", bytes_value("2"))]);
        let cond = make_row(&[("a", bytes_value("1"))]);
        assert!(record_matches_condition(&row, &cond));
    }

    #[test]
    fn record_matches_condition_no_match() {
        let row = make_row(&[("a", bytes_value("1"))]);
        let cond = make_row(&[("a", bytes_value("X"))]);
        assert!(!record_matches_condition(&row, &cond));
    }

    #[test]
    fn record_matches_condition_empty_condition() {
        let row = make_row(&[("a", bytes_value("1"))]);
        let cond = ObjectMap::new();
        assert!(record_matches_condition(&row, &cond));
    }

    #[test]
    fn record_matches_condition_missing_field() {
        let row = make_row(&[("b", bytes_value("2"))]);
        let cond = make_row(&[("a", bytes_value("1"))]);
        assert!(!record_matches_condition(&row, &cond));
    }

    // -----------------------------------------------------------------------
    // Boundary: null-byte separator disambiguates keys
    // -----------------------------------------------------------------------

    #[test]
    fn compact_key_separator_prevents_collision() {
        // "AB" + "C" vs "A" + "BC" — these must NOT be equal
        let r1 = make_row(&[("col1", bytes_value("AB")), ("col2", bytes_value("C"))]);
        let r2 = make_row(&[("col1", bytes_value("A")), ("col2", bytes_value("BC"))]);
        let cols = vec!["col1".to_string(), "col2".to_string()];
        let k1 = CompactKey::from_row(&r1, &cols);
        let k2 = CompactKey::from_row(&r2, &cols);
        assert_ne!(k1, k2);
    }
}
