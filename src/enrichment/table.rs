//! Project:   dfe-transform-vrl
//! File:      src/enrichment/table.rs
//! Purpose:   High-performance enrichment table backed by `FxHashMap` + `ArcSwap`
//! Language:  Rust
//!
//! License:   BUSL-1.1
//! Copyright: (c) 2026 HYPERI PTY LIMITED

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;
use std::sync::Arc;
use vrl::value::{KeyString, ObjectMap, Value};

/// Materialised rows of a hash-map-backed table.
///
/// A key maps to *every* row carrying it, not just the last one loaded --
/// duplicate key values are legitimate data (the shipped `timezones.csv` has
/// 12 duplicated abbreviations) and Vector reports them as an ambiguous match
/// rather than silently picking one.
pub type RowMap = FxHashMap<CompactKey, Vec<Arc<ObjectMap>>>;

// ---------------------------------------------------------------------------
// Condition
// ---------------------------------------------------------------------------

/// One field-matching rule from a VRL enrichment condition, resolved for a
/// single event.
///
/// Mirrors Vector's `enrichment::Condition`: a condition object entry whose
/// value is `{"from": t'...'}`, `{"to": t'...'}` or both becomes a date
/// comparison; everything else is an equality test.
#[derive(Debug, Clone)]
pub enum Condition {
    /// The field must equal `value` (or the wildcard, when one is supplied).
    Equals {
        /// Row field to compare.
        field: KeyString,
        /// Value the field must equal.
        value: Value,
    },
    /// The timestamp field must fall inside `[from, to]`, both inclusive.
    BetweenDates {
        /// Row field to compare.
        field: KeyString,
        /// Earliest accepted timestamp.
        from: DateTime<Utc>,
        /// Latest accepted timestamp.
        to: DateTime<Utc>,
    },
    /// The timestamp field must be at or after `from`.
    FromDate {
        /// Row field to compare.
        field: KeyString,
        /// Earliest accepted timestamp.
        from: DateTime<Utc>,
    },
    /// The timestamp field must be at or before `to`.
    ToDate {
        /// Row field to compare.
        field: KeyString,
        /// Latest accepted timestamp.
        to: DateTime<Utc>,
    },
}

impl Condition {
    /// The row field this condition tests.
    pub const fn field(&self) -> &KeyString {
        match self {
            Self::Equals { field, .. }
            | Self::BetweenDates { field, .. }
            | Self::FromDate { field, .. }
            | Self::ToDate { field, .. } => field,
        }
    }

    /// The compared value when this is an equality test, else `None`.
    ///
    /// Only equality conditions can use the `CompactKey` index; a date range
    /// has to be evaluated against every candidate row.
    pub const fn equality_value(&self) -> Option<&Value> {
        match self {
            Self::Equals { value, .. } => Some(value),
            _ => None,
        }
    }
}

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
    /// Build an index key from resolved lookup conditions.
    ///
    /// Returns `None` unless every key column is present as an *equality*
    /// condition, which is the only case the index can answer. Any other
    /// condition shape falls back to a scan.
    pub fn from_conditions(conditions: &[Condition], key_columns: &[String]) -> Option<Self> {
        if key_columns.is_empty() {
            return None;
        }

        let mut buf: Vec<u8> = Vec::new();
        let mut first = true;

        for col in key_columns {
            let value = conditions
                .iter()
                .find(|c| c.field().as_str() == col.as_str())
                .and_then(Condition::equality_value)?;

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
    HashMap(ArcSwap<RowMap>),

    /// `GeoIP2` `MaxMind` database reader (requires `enrichment-mmdb` feature).
    #[cfg(feature = "enrichment-mmdb")]
    Mmdb(Arc<maxminddb::Reader<Vec<u8>>>),
}

impl TableData {
    /// Return every row matching *all* `conditions`.
    ///
    /// `case_sensitive` applies to string comparison only. `wildcard`, when
    /// set, additionally matches a row whose cell equals the wildcard value.
    /// `select` restricts the returned fields; a selected field the row does
    /// not carry is silently omitted, matching Vector.
    ///
    /// The `HashMap` backend uses the `CompactKey` index when the conditions
    /// pin every key column by equality and the comparison is case sensitive,
    /// and otherwise scans. The `Mmdb` backend takes exactly one condition,
    /// whose value is the IP address to look up.
    pub fn find_rows(
        &self,
        conditions: &[Condition],
        case_sensitive: bool,
        wildcard: Option<&Value>,
        select: Option<&[String]>,
        key_columns: &[String],
    ) -> Result<Vec<ObjectMap>, String> {
        match self {
            Self::HashMap(swap) => {
                let guard = swap.load();
                // The index is keyed on exact bytes, so it can only answer a
                // case-sensitive lookup with no wildcard: a case fold or a
                // wildcard value would land in a different bucket.
                let indexed = (case_sensitive && wildcard.is_none())
                    .then(|| CompactKey::from_conditions(conditions, key_columns))
                    .flatten();

                // The index narrows to one bucket; the bucket's rows still go
                // through the full condition test, because a condition outside
                // the key columns is not encoded in the key.
                let matched: Vec<ObjectMap> = indexed.map_or_else(
                    || {
                        Self::collect_matches(
                            guard.values().flatten(),
                            conditions,
                            case_sensitive,
                            wildcard,
                        )
                    },
                    |key| {
                        guard.get(&key).map_or_else(Vec::new, |rows| {
                            Self::collect_matches(rows.iter(), conditions, case_sensitive, wildcard)
                        })
                    },
                );

                Ok(matched
                    .into_iter()
                    .map(|row| apply_select(row, select))
                    .collect())
            }

            #[cfg(feature = "enrichment-mmdb")]
            Self::Mmdb(reader) => mmdb_find_rows(reader, conditions, select),
        }
    }

    /// Clone out every row in `rows` that satisfies all `conditions`.
    fn collect_matches<'a, I>(
        rows: I,
        conditions: &[Condition],
        case_sensitive: bool,
        wildcard: Option<&Value>,
    ) -> Vec<ObjectMap>
    where
        I: Iterator<Item = &'a Arc<ObjectMap>>,
    {
        rows.filter(|row| row_matches(row, conditions, case_sensitive, wildcard))
            .map(|row| (**row).clone())
            .collect()
    }

    /// Number of records in the table (approximate for MMDB).
    pub fn len(&self) -> usize {
        match self {
            Self::HashMap(swap) => swap.load().values().map(Vec::len).sum(),
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
    /// Compiled column type coercions, reused by the refresh loop.
    schema: Arc<crate::enrichment::loader::ColumnSchema>,
    /// Column names the source declared, for compile-time condition checking.
    ///
    /// Empty means "not knowable" rather than "no columns" -- an MMDB table
    /// has no column list at all, and a JSON or STIX source with no rows
    /// declares none. The check is skipped in that case rather than rejecting
    /// every condition.
    columns: Arc<[KeyString]>,
}

impl EnrichmentTable {
    /// Construct a hash-map-backed table with the given key columns.
    pub fn new_hashmap(
        name: &str,
        rows: RowMap,
        key_columns: Vec<String>,
        source_config: Option<crate::config::EnrichmentSourceConfig>,
        refresh_config: Option<crate::config::RefreshConfig>,
        schema: Arc<crate::enrichment::loader::ColumnSchema>,
        columns: Arc<[KeyString]>,
    ) -> Self {
        Self {
            name: Arc::from(name),
            data: TableData::HashMap(ArcSwap::from_pointee(rows)),
            key_columns: Some(key_columns.into()),
            source_config,
            refresh_config,
            schema,
            columns,
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
            schema: Arc::default(),
            // A geo database has no column list to check a condition against;
            // arity is enforced instead, in `mmdb_find_rows`.
            columns: Arc::from(Vec::new()),
        }
    }

    /// Return every row matching *all* `conditions`.
    ///
    /// See [`TableData::find_rows`] for the matching rules.
    pub fn find_rows(
        &self,
        conditions: &[Condition],
        case_sensitive: bool,
        wildcard: Option<&Value>,
        select: Option<&[String]>,
    ) -> Result<Vec<ObjectMap>, String> {
        let cols = self.key_columns.as_deref().unwrap_or(&[]);
        self.data
            .find_rows(conditions, case_sensitive, wildcard, select, cols)
    }

    /// Column names the source declared, empty when not knowable.
    pub fn columns(&self) -> &[KeyString] {
        &self.columns
    }

    /// Reject condition fields naming a column this table does not have.
    ///
    /// Vector does this at compile time: `add_index` hands every non-date
    /// condition field to the table, and the file table's
    /// `normalize_index_fields` raises `MissingDatasetFields` when one is not
    /// a header. Without it a typo in a condition is not an error at all, it
    /// is a lookup that silently never matches for the life of the process --
    /// the same failure this function's caller already prevents for the table
    /// name itself.
    ///
    /// `fields` should exclude date-range conditions, which Vector also
    /// excludes because they are not indexed. Returns the offending names,
    /// sorted, so the diagnostic is stable.
    pub fn missing_columns(&self, fields: &[&str]) -> Vec<String> {
        // No declared columns means the source could not tell us what it has,
        // so there is nothing to check against and every field is allowed.
        if self.columns.is_empty() {
            return Vec::new();
        }

        let mut missing: Vec<String> = fields
            .iter()
            .filter(|field| !self.columns.iter().any(|col| col.as_str() == **field))
            .map(|field| (*field).to_string())
            .collect();
        missing.sort_unstable();
        missing.dedup();
        missing
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

    /// Compiled column type coercions (for the refresh loop).
    pub const fn schema(&self) -> &Arc<crate::enrichment::loader::ColumnSchema> {
        &self.schema
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
    pub fn swap_hashmap(&self, new_rows: RowMap) -> Result<(), SwapError> {
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

/// Returns `true` if `row` satisfies every condition.
///
/// A condition naming a field this row does not carry never matches. That is
/// a per-row test, not the dataset check: a condition on a column the *table*
/// does not have is rejected at compile time (see
/// `EnrichmentTable::validate_condition_fields`), so reaching here with an
/// unknown field means the column exists but this row omits it, which is
/// possible for JSON and STIX sources whose rows need not agree.
fn row_matches(
    row: &ObjectMap,
    conditions: &[Condition],
    case_sensitive: bool,
    wildcard: Option<&Value>,
) -> bool {
    conditions.iter().all(|condition| match condition {
        Condition::Equals { field, value } => row.get(field.as_str()).is_some_and(|cell| {
            values_equal(cell, value, case_sensitive)
                || wildcard.is_some_and(|w| values_equal(cell, w, case_sensitive))
        }),
        Condition::BetweenDates { field, from, to } => {
            matches!(row.get(field.as_str()), Some(Value::Timestamp(ts)) if from <= ts && ts <= to)
        }
        Condition::FromDate { field, from } => {
            matches!(row.get(field.as_str()), Some(Value::Timestamp(ts)) if from <= ts)
        }
        Condition::ToDate { field, to } => {
            matches!(row.get(field.as_str()), Some(Value::Timestamp(ts)) if ts <= to)
        }
    })
}

/// Compare a table cell against a condition value.
///
/// Case insensitivity applies to UTF-8 byte strings only; every other pair of
/// `Value` variants is compared for exact equality either way.
fn values_equal(cell: &Value, expected: &Value, case_sensitive: bool) -> bool {
    if case_sensitive {
        return cell == expected;
    }
    match (cell, expected) {
        (Value::Bytes(a), Value::Bytes(b)) => {
            match (std::str::from_utf8(a), std::str::from_utf8(b)) {
                (Ok(a), Ok(b)) => a.to_lowercase() == b.to_lowercase(),
                (Err(_), Err(_)) => a == b,
                _ => false,
            }
        }
        _ => cell == expected,
    }
}

/// Keep only the selected fields of a row.
///
/// A selected field the row does not carry is dropped rather than raised as an
/// error, matching Vector's `add_columns`.
fn apply_select(row: ObjectMap, select: Option<&[String]>) -> ObjectMap {
    let Some(select) = select else {
        return row;
    };
    row.into_iter()
        .filter(|(key, _)| select.iter().any(|s| s.as_str() == key.as_str()))
        .collect()
}

/// Look one IP address up in a `MaxMind` database.
///
/// A geo table has no columns to filter on, so exactly one condition is
/// accepted and its value is the address. A miss yields no rows, which
/// `get_enrichment_table_record` then reports as "No rows found".
#[cfg(feature = "enrichment-mmdb")]
fn mmdb_find_rows(
    reader: &maxminddb::Reader<Vec<u8>>,
    conditions: &[Condition],
    select: Option<&[String]>,
) -> Result<Vec<ObjectMap>, String> {
    let [Condition::Equals { field, value }] = conditions else {
        return Err(
            "an mmdb enrichment table takes exactly one condition, the IP address".to_string(),
        );
    };

    let Value::Bytes(bytes) = value else {
        return Err(format!(
            "condition field {field:?} must be a string IP address"
        ));
    };
    let text = String::from_utf8_lossy(bytes);
    let address: std::net::IpAddr = text
        .parse()
        .map_err(|_| format!("condition field {field:?} is not an IP address: {text}"))?;

    let lookup = reader
        .lookup(address)
        .map_err(|e| format!("mmdb lookup failed for {text}: {e}"))?;
    let record = lookup
        .decode::<serde_json::Value>()
        .map_err(|e| format!("cannot decode mmdb record for {text}: {e}"))?;

    let Some(record) = record else {
        return Ok(Vec::new());
    };
    match Value::from(record) {
        Value::Object(row) => Ok(vec![apply_select(row, select)]),
        _ => Err(format!("mmdb record for {text} is not an object")),
    }
}

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

    /// Equality conditions, the common shape in these tests.
    fn eq(pairs: &[(&str, Value)]) -> Vec<Condition> {
        pairs
            .iter()
            .map(|(k, v)| Condition::Equals {
                field: KeyString::from(*k),
                value: v.clone(),
            })
            .collect()
    }

    fn make_hashmap_table(
        name: &str,
        key_columns: Vec<String>,
        rows: Vec<ObjectMap>,
    ) -> EnrichmentTable {
        let mut hm = RowMap::default();
        let mut columns: Vec<KeyString> = Vec::new();
        for row in rows {
            crate::enrichment::loader::union_columns(&mut columns, &row);
            let key = CompactKey::from_row(&row, &key_columns);
            hm.entry(key).or_default().push(Arc::new(row));
        }
        EnrichmentTable::new_hashmap(
            name,
            hm,
            key_columns,
            None,
            None,
            Arc::default(),
            Arc::from(columns),
        )
    }

    /// All rows matching `conditions`, case sensitive, no wildcard, no select.
    fn find(table: &EnrichmentTable, conditions: &[Condition]) -> Vec<ObjectMap> {
        table.find_rows(conditions, true, None, None).unwrap()
    }

    fn field(row: &ObjectMap, key: &str) -> Option<Value> {
        row.get(key).cloned()
    }

    // -----------------------------------------------------------------------
    // CompactKey::from_conditions
    // -----------------------------------------------------------------------

    #[test]
    fn compact_key_from_conditions_single_column_present() {
        let key = CompactKey::from_conditions(
            &eq(&[("country", bytes_value("AU"))]),
            &["country".to_string()],
        );
        assert!(key.is_some());
        assert_eq!(key.unwrap().0.as_ref(), b"AU");
    }

    #[test]
    fn compact_key_from_conditions_missing_column_returns_none() {
        let key = CompactKey::from_conditions(
            &eq(&[("city", bytes_value("Sydney"))]),
            &["country".to_string()],
        );
        assert!(key.is_none());
    }

    #[test]
    fn compact_key_from_conditions_multi_column_separator() {
        let key = CompactKey::from_conditions(
            &eq(&[
                ("country", bytes_value("AU")),
                ("city", bytes_value("Sydney")),
            ]),
            &["country".to_string(), "city".to_string()],
        )
        .unwrap();
        assert_eq!(key.0.as_ref(), b"AU\x00Sydney");
    }

    #[test]
    fn compact_key_from_conditions_one_column_missing_in_multi() {
        let key = CompactKey::from_conditions(
            &eq(&[("country", bytes_value("AU"))]),
            &["country".to_string(), "city".to_string()],
        );
        assert!(key.is_none());
    }

    #[test]
    fn compact_key_from_conditions_non_bytes_value() {
        let key = CompactKey::from_conditions(
            &eq(&[("code", Value::Integer(42))]),
            &["code".to_string()],
        )
        .unwrap();
        assert_eq!(key.0.as_ref(), b"42");
    }

    #[test]
    fn compact_key_from_conditions_empty_columns_list() {
        // No key columns means no index to consult.
        let key = CompactKey::from_conditions(&eq(&[("x", bytes_value("1"))]), &[]);
        assert!(key.is_none());
    }

    #[test]
    fn compact_key_from_conditions_date_range_is_not_indexable() {
        let conditions = vec![Condition::FromDate {
            field: KeyString::from("dob"),
            from: timestamp("1980-01-01T00:00:00Z"),
        }];
        assert!(CompactKey::from_conditions(&conditions, &["dob".to_string()]).is_none());
    }

    /// Parse an RFC 3339 instant for the date-range tests.
    fn timestamp(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text).unwrap().into()
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
        // country missing -> empty segment, separator, then "NY"
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
        let b = CompactKey::from_conditions(
            &eq(&[("country", bytes_value("JP"))]),
            &["country".to_string()],
        )
        .unwrap();
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
        let lookup =
            CompactKey::from_conditions(&eq(&[("k", bytes_value("v"))]), &["k".to_string()])
                .unwrap();
        assert_eq!(hm.get(&lookup), Some(&99));
    }

    #[test]
    fn compact_key_separator_prevents_collision() {
        // "AB" + "C" vs "A" + "BC" -- these must NOT be equal
        let r1 = make_row(&[("col1", bytes_value("AB")), ("col2", bytes_value("C"))]);
        let r2 = make_row(&[("col1", bytes_value("A")), ("col2", bytes_value("BC"))]);
        let cols = vec!["col1".to_string(), "col2".to_string()];
        let k1 = CompactKey::from_row(&r1, &cols);
        let k2 = CompactKey::from_row(&r2, &cols);
        assert_ne!(k1, k2);
    }

    // -----------------------------------------------------------------------
    // EnrichmentTable -- construction and basic metadata
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

    #[test]
    fn table_len_counts_rows_not_keys() {
        // Two rows share a key value; both are kept, so len is 2.
        let rows = vec![
            make_row(&[
                ("abbr", bytes_value("CST")),
                ("offset", bytes_value("-06:00")),
            ]),
            make_row(&[
                ("abbr", bytes_value("CST")),
                ("offset", bytes_value("+08:00")),
            ]),
        ];
        let table = make_hashmap_table("tz", vec!["abbr".to_string()], rows);
        assert_eq!(table.len(), 2);
    }

    // -----------------------------------------------------------------------
    // find_rows -- indexed path
    // -----------------------------------------------------------------------

    #[test]
    fn find_rows_hit() {
        let row = make_row(&[
            ("country", bytes_value("AU")),
            ("capital", bytes_value("Canberra")),
        ]);
        let table = make_hashmap_table("t", vec!["country".to_string()], vec![row]);

        let rows = find(&table, &eq(&[("country", bytes_value("AU"))]));
        assert_eq!(rows.len(), 1);
        assert_eq!(field(&rows[0], "capital"), Some(bytes_value("Canberra")));
    }

    #[test]
    fn find_rows_miss() {
        let row = make_row(&[("country", bytes_value("AU"))]);
        let table = make_hashmap_table("t", vec!["country".to_string()], vec![row]);
        assert!(find(&table, &eq(&[("country", bytes_value("NZ"))])).is_empty());
    }

    #[test]
    fn find_rows_missing_key_column_falls_back_to_scan() {
        let row = make_row(&[("country", bytes_value("AU"))]);
        let table = make_hashmap_table("t", vec!["country".to_string()], vec![row]);
        // Reached only when the row omits a column the table declares; the
        // VRL path rejects an undeclared column at compile time instead.
        assert!(find(&table, &eq(&[("city", bytes_value("Brisbane"))])).is_empty());
    }

    // -----------------------------------------------------------------------
    // missing_columns -- the compile-time dataset check
    // -----------------------------------------------------------------------

    #[test]
    fn missing_columns_reports_only_undeclared_fields() {
        let row = make_row(&[("id", bytes_value("1")), ("name", bytes_value("Bob"))]);
        let table = make_hashmap_table("t", vec!["id".to_string()], vec![row]);
        assert!(table.missing_columns(&["id", "name"]).is_empty());
        assert_eq!(table.missing_columns(&["nope"]), vec!["nope".to_string()]);
    }

    #[test]
    fn missing_columns_sorts_and_dedups() {
        let row = make_row(&[("id", bytes_value("1"))]);
        let table = make_hashmap_table("t", vec!["id".to_string()], vec![row]);
        assert_eq!(
            table.missing_columns(&["zeta", "alpha", "zeta"]),
            vec!["alpha".to_string(), "zeta".to_string()]
        );
    }

    #[test]
    fn missing_columns_allows_everything_when_columns_are_unknown() {
        // An MMDB table declares no columns, so there is nothing to check
        // against and a condition on any field must be allowed through.
        let table = make_hashmap_table("t", vec!["id".to_string()], vec![]);
        assert!(table.columns().is_empty());
        assert!(table.missing_columns(&["anything"]).is_empty());
    }

    #[test]
    fn find_rows_multi_column_key_hit() {
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

        let rows = find(
            &table,
            &eq(&[
                ("country", bytes_value("AU")),
                ("city", bytes_value("Sydney")),
            ]),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(field(&rows[0], "pop"), Some(bytes_value("5_000_000")));
    }

    #[test]
    fn find_rows_multi_column_key_partial_miss() {
        let row = make_row(&[
            ("country", bytes_value("AU")),
            ("city", bytes_value("Sydney")),
        ]);
        let table = make_hashmap_table(
            "t",
            vec!["country".to_string(), "city".to_string()],
            vec![row],
        );

        let rows = find(
            &table,
            &eq(&[
                ("country", bytes_value("AU")),
                ("city", bytes_value("Melbourne")),
            ]),
        );
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // find_rows -- conditions outside the key columns
    // -----------------------------------------------------------------------

    #[test]
    fn find_rows_filters_on_non_key_condition_fields() {
        // The bug this guards: keying on "id" and discarding "status" returned
        // the row regardless of its status.
        let row = make_row(&[
            ("id", bytes_value("1")),
            ("name", bytes_value("Bob")),
            ("status", bytes_value("active")),
        ]);
        let table = make_hashmap_table("users", vec!["id".to_string()], vec![row]);

        let rows = find(
            &table,
            &eq(&[
                ("id", bytes_value("1")),
                ("status", bytes_value("disabled")),
            ]),
        );
        assert!(rows.is_empty(), "status must be part of the match");

        let rows = find(
            &table,
            &eq(&[("id", bytes_value("1")), ("status", bytes_value("active"))]),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(field(&rows[0], "name"), Some(bytes_value("Bob")));
    }

    #[test]
    fn find_rows_all_match_on_scan() {
        let r1 = make_row(&[("type", bytes_value("A")), ("val", bytes_value("x"))]);
        let r2 = make_row(&[("type", bytes_value("A")), ("val", bytes_value("y"))]);
        let r3 = make_row(&[("type", bytes_value("B")), ("val", bytes_value("z"))]);
        let table = make_hashmap_table("t", vec!["val".to_string()], vec![r1, r2, r3]);

        let mut rows = find(&table, &eq(&[("type", bytes_value("A"))]));
        rows.sort_by_key(|r| format!("{:?}", field(r, "val")));
        assert_eq!(rows.len(), 2);
        assert_eq!(field(&rows[0], "val"), Some(bytes_value("x")));
        assert_eq!(field(&rows[1], "val"), Some(bytes_value("y")));
    }

    #[test]
    fn find_rows_empty_condition_matches_all() {
        let r1 = make_row(&[("x", bytes_value("1"))]);
        let r2 = make_row(&[("x", bytes_value("2"))]);
        let table = make_hashmap_table("t", vec!["x".to_string()], vec![r1, r2]);
        assert_eq!(find(&table, &[]).len(), 2);
    }

    #[test]
    fn find_rows_empty_table() {
        let table = make_hashmap_table("t", vec!["x".to_string()], vec![]);
        assert!(find(&table, &eq(&[("x", bytes_value("1"))])).is_empty());
    }

    #[test]
    fn find_rows_returns_every_row_sharing_a_key() {
        let r1 = make_row(&[
            ("abbr", bytes_value("CST")),
            ("name", bytes_value("Central")),
        ]);
        let r2 = make_row(&[("abbr", bytes_value("CST")), ("name", bytes_value("China"))]);
        let table = make_hashmap_table("tz", vec!["abbr".to_string()], vec![r1, r2]);

        let rows = find(&table, &eq(&[("abbr", bytes_value("CST"))]));
        assert_eq!(rows.len(), 2, "duplicate keys must not be collapsed");
    }

    // -----------------------------------------------------------------------
    // find_rows -- case sensitivity, wildcard, select
    // -----------------------------------------------------------------------

    #[test]
    fn find_rows_is_case_sensitive_by_default() {
        let row = make_row(&[("name", bytes_value("Bob"))]);
        let table = make_hashmap_table("t", vec!["name".to_string()], vec![row]);
        assert!(find(&table, &eq(&[("name", bytes_value("bob"))])).is_empty());
    }

    #[test]
    fn find_rows_case_insensitive_matches() {
        let row = make_row(&[("name", bytes_value("Bob")), ("id", bytes_value("1"))]);
        let table = make_hashmap_table("t", vec!["name".to_string()], vec![row]);

        let rows = table
            .find_rows(&eq(&[("name", bytes_value("bob"))]), false, None, None)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(field(&rows[0], "id"), Some(bytes_value("1")));
    }

    #[test]
    fn find_rows_wildcard_matches_rows_the_condition_misses() {
        let r1 = make_row(&[("id", bytes_value("1")), ("status", bytes_value("active"))]);
        let r2 = make_row(&[("id", bytes_value("2")), ("status", bytes_value("banned"))]);
        let table = make_hashmap_table("t", vec!["id".to_string()], vec![r1, r2]);

        let wildcard = bytes_value("active");
        let rows = table
            .find_rows(
                &eq(&[("status", bytes_value("nope"))]),
                true,
                Some(&wildcard),
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(field(&rows[0], "id"), Some(bytes_value("1")));
    }

    #[test]
    fn find_rows_wildcard_reaches_rows_the_index_would_hide() {
        // The condition pins the key column, so the index would answer with
        // one bucket and miss every wildcard match outside it.
        let r1 = make_row(&[("id", bytes_value("1")), ("name", bytes_value("Bob"))]);
        let r2 = make_row(&[("id", bytes_value("*")), ("name", bytes_value("Anyone"))]);
        let table = make_hashmap_table("t", vec!["id".to_string()], vec![r1, r2]);

        let wildcard = bytes_value("*");
        let rows = table
            .find_rows(
                &eq(&[("id", bytes_value("999"))]),
                true,
                Some(&wildcard),
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(field(&rows[0], "name"), Some(bytes_value("Anyone")));
    }

    #[test]
    fn find_rows_select_limits_the_returned_fields() {
        let row = make_row(&[
            ("id", bytes_value("1")),
            ("name", bytes_value("Bob")),
            ("status", bytes_value("active")),
        ]);
        let table = make_hashmap_table("t", vec!["id".to_string()], vec![row]);

        let select = vec!["name".to_string()];
        let rows = table
            .find_rows(&eq(&[("id", bytes_value("1"))]), true, None, Some(&select))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(field(&rows[0], "name"), Some(bytes_value("Bob")));
        assert!(field(&rows[0], "status").is_none());
    }

    #[test]
    fn find_rows_select_drops_fields_the_row_lacks() {
        // Vector omits an unknown selected field rather than erroring.
        let row = make_row(&[("id", bytes_value("1")), ("name", bytes_value("Bob"))]);
        let table = make_hashmap_table("t", vec!["id".to_string()], vec![row]);

        let select = vec!["name".to_string(), "nosuchcol".to_string()];
        let rows = table
            .find_rows(&eq(&[("id", bytes_value("1"))]), true, None, Some(&select))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
        assert_eq!(field(&rows[0], "name"), Some(bytes_value("Bob")));
    }

    // -----------------------------------------------------------------------
    // find_rows -- date ranges
    // -----------------------------------------------------------------------

    #[test]
    fn find_rows_between_dates() {
        let rows = vec![
            make_row(&[
                ("id", bytes_value("1")),
                ("dob", Value::Timestamp(timestamp("1985-06-15T00:00:00Z"))),
            ]),
            make_row(&[
                ("id", bytes_value("2")),
                ("dob", Value::Timestamp(timestamp("1990-01-01T00:00:00Z"))),
            ]),
        ];
        let table = make_hashmap_table("people", vec!["id".to_string()], rows);

        let conditions = vec![Condition::BetweenDates {
            field: KeyString::from("dob"),
            from: timestamp("1980-01-01T00:00:00Z"),
            to: timestamp("1989-12-31T00:00:00Z"),
        }];
        let found = find(&table, &conditions);
        assert_eq!(found.len(), 1);
        assert_eq!(field(&found[0], "id"), Some(bytes_value("1")));
    }

    #[test]
    fn find_rows_from_and_to_dates() {
        let rows = vec![
            make_row(&[
                ("id", bytes_value("1")),
                ("dob", Value::Timestamp(timestamp("1985-06-15T00:00:00Z"))),
            ]),
            make_row(&[
                ("id", bytes_value("2")),
                ("dob", Value::Timestamp(timestamp("1990-01-01T00:00:00Z"))),
            ]),
        ];
        let table = make_hashmap_table("people", vec!["id".to_string()], rows);

        let from = find(
            &table,
            &[Condition::FromDate {
                field: KeyString::from("dob"),
                from: timestamp("1986-01-01T00:00:00Z"),
            }],
        );
        assert_eq!(from.len(), 1);
        assert_eq!(field(&from[0], "id"), Some(bytes_value("2")));

        let to = find(
            &table,
            &[Condition::ToDate {
                field: KeyString::from("dob"),
                to: timestamp("1986-01-01T00:00:00Z"),
            }],
        );
        assert_eq!(to.len(), 1);
        assert_eq!(field(&to[0], "id"), Some(bytes_value("1")));
    }

    #[test]
    fn find_rows_date_condition_on_a_string_column_never_matches() {
        // A column loaded without a `schema` entry is a string, and Vector
        // only compares Timestamp cells against a date range.
        let row = make_row(&[
            ("id", bytes_value("1")),
            ("dob", bytes_value("1985-06-15T00:00:00Z")),
        ]);
        let table = make_hashmap_table("people", vec!["id".to_string()], vec![row]);

        let found = find(
            &table,
            &[Condition::FromDate {
                field: KeyString::from("dob"),
                from: timestamp("1980-01-01T00:00:00Z"),
            }],
        );
        assert!(found.is_empty());
    }

    // -----------------------------------------------------------------------
    // EnrichmentTable::swap_hashmap
    // -----------------------------------------------------------------------

    #[test]
    fn swap_hashmap_replaces_data_atomically() {
        let old_row = make_row(&[("k", bytes_value("old"))]);
        let table = make_hashmap_table("t", vec!["k".to_string()], vec![old_row]);
        assert_eq!(find(&table, &eq(&[("k", bytes_value("old"))])).len(), 1);

        table.swap_hashmap(RowMap::default()).unwrap();

        assert!(find(&table, &eq(&[("k", bytes_value("old"))])).is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn swap_hashmap_with_new_rows() {
        let table = make_hashmap_table("t", vec!["k".to_string()], vec![]);

        let new_row = make_row(&[("k", bytes_value("fresh")), ("v", bytes_value("data"))]);
        let key = CompactKey::from_row(&new_row, &["k".to_string()]);
        let mut new_hm = RowMap::default();
        new_hm.insert(key, vec![Arc::new(new_row)]);

        table.swap_hashmap(new_hm).unwrap();

        let rows = find(&table, &eq(&[("k", bytes_value("fresh"))]));
        assert_eq!(rows.len(), 1);
        assert_eq!(field(&rows[0], "v"), Some(bytes_value("data")));
    }

    // -----------------------------------------------------------------------
    // row_matches helper
    // -----------------------------------------------------------------------

    #[test]
    fn row_matches_all_match() {
        let row = make_row(&[("a", bytes_value("1")), ("b", bytes_value("2"))]);
        assert!(row_matches(
            &row,
            &eq(&[("a", bytes_value("1"))]),
            true,
            None
        ));
    }

    #[test]
    fn row_matches_no_match() {
        let row = make_row(&[("a", bytes_value("1"))]);
        assert!(!row_matches(
            &row,
            &eq(&[("a", bytes_value("X"))]),
            true,
            None
        ));
    }

    #[test]
    fn row_matches_empty_condition() {
        let row = make_row(&[("a", bytes_value("1"))]);
        assert!(row_matches(&row, &[], true, None));
    }

    #[test]
    fn row_matches_missing_field() {
        let row = make_row(&[("b", bytes_value("2"))]);
        assert!(!row_matches(
            &row,
            &eq(&[("a", bytes_value("1"))]),
            true,
            None
        ));
    }
}
