//! Project:   dfe-transform-vrl
//! File:      src/enrichment/loader.rs
//! Purpose:   Enrichment table loaders for all source types
//! Language:  Rust
//!
//! License:   BUSL-1.1
//! Copyright: (c) 2026 HYPERI PTY LIMITED

//! Load enrichment data from various sources into `FxHashMap<CompactKey, Arc<ObjectMap>>`.
//!
//! Every source type (except MMDB) materialises into the same `FxHashMap` structure.
//! The `load_from_source()` dispatcher is the single entry point used by both
//! `EnrichmentRegistry::load()` (startup) and the refresh loop (runtime).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use vrl::compiler::TimeZone;
use vrl::compiler::conversion::Conversion;
use vrl::value::{KeyString, ObjectMap, Value};

use crate::config::{EnrichmentSourceConfig, FileFormat};
use crate::enrichment::table::{CompactKey, RowMap};

// ---------------------------------------------------------------------------
// Column schema
// ---------------------------------------------------------------------------

/// Per-column type coercion for text-valued sources.
///
/// Mirrors Vector's file enrichment table `schema`: without it every CSV cell
/// loads as a string, so a `status_code` column compares as `"1"` and never
/// as `1`, and a date-range condition can never match because the cell is not
/// a timestamp.
///
/// Conversion names come from VRL's own `Conversion`, so they are exactly
/// Vector's: `asis`/`bytes`/`string` (all no-ops), `int`/`integer`, `float`,
/// `bool`/`boolean`, `timestamp`, `timestamp|<format>`. `date` and
/// `date|<format>` are handled here, matching Vector's file table, and parse
/// to midnight UTC.
#[derive(Debug, Clone, Default)]
pub struct ColumnSchema {
    /// Column name -> how its text is converted.
    columns: BTreeMap<String, ColumnType>,
}

/// A single column's declared conversion.
#[derive(Debug, Clone)]
enum ColumnType {
    /// A date with no time part, parsed at midnight UTC with this format.
    Date(String),
    /// Any conversion VRL's own `Conversion` understands.
    Vrl(Conversion),
}

impl ColumnSchema {
    /// Compile a `column -> type name` map, failing on an unknown type name.
    pub fn compile(spec: &BTreeMap<String, String>, table_name: &str) -> crate::Result<Self> {
        let mut columns = BTreeMap::new();
        for (column, type_name) in spec {
            let mut parts = type_name.splitn(2, '|').map(str::trim);
            let column_type = match (parts.next(), parts.next()) {
                (Some("date"), None) => ColumnType::Date("%Y-%m-%d".to_string()),
                (Some("date"), Some(format)) => ColumnType::Date(format.to_string()),
                _ => {
                    ColumnType::Vrl(Conversion::parse(type_name, TimeZone::Local).map_err(|e| {
                        crate::Error::Enrichment(format!(
                            "table '{table_name}': column '{column}': {e}"
                        ))
                    })?)
                }
            };
            columns.insert(column.clone(), column_type);
        }
        Ok(Self { columns })
    }

    /// Whether any column declares a conversion.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Convert one cell, returning it unchanged when the column is undeclared.
    pub fn coerce(&self, table_name: &str, column: &str, raw: &str) -> crate::Result<Value> {
        let Some(column_type) = self.columns.get(column) else {
            return Ok(Value::from(raw));
        };
        match column_type {
            ColumnType::Date(format) => {
                let date = chrono::NaiveDate::parse_from_str(raw, format).map_err(|e| {
                    crate::Error::Enrichment(format!(
                        "table '{table_name}': column '{column}': cannot parse date '{raw}': {e}"
                    ))
                })?;
                Ok(Value::Timestamp(
                    date.and_time(chrono::NaiveTime::MIN).and_utc(),
                ))
            }
            ColumnType::Vrl(conversion) => conversion
                .convert::<Value>(bytes::Bytes::copy_from_slice(raw.as_bytes()))
                .map_err(|e| {
                    crate::Error::Enrichment(format!(
                        "table '{table_name}': column '{column}': cannot convert '{raw}': {e}"
                    ))
                }),
        }
    }

    /// Convert a already-typed value, coercing only strings.
    ///
    /// JSON and YAML sources carry their own types, so only a string cell is a
    /// candidate for a declared conversion.
    fn coerce_value(&self, table_name: &str, column: &str, value: Value) -> crate::Result<Value> {
        if self.is_empty() {
            return Ok(value);
        }
        match &value {
            Value::Bytes(bytes) => {
                let raw = String::from_utf8_lossy(bytes).into_owned();
                self.coerce(table_name, column, &raw)
            }
            _ => Ok(value),
        }
    }
}

// ---------------------------------------------------------------------------
// Loaded table
// ---------------------------------------------------------------------------

/// The rows of a table plus the column names the source declares.
///
/// The columns are what makes a condition on a column the table does not have
/// a compile-time error rather than a lookup that silently never matches, so
/// they have to come from the source rather than from the rows: a CSV with
/// headers and no data rows still has columns.
///
/// Column order is the source's own (CSV header order, SQLite projection
/// order, first-seen key order for JSON and YAML), so a diagnostic listing
/// them reads the way the file does.
#[derive(Debug, Default)]
pub struct LoadedTable {
    /// Rows, bucketed by key.
    pub rows: RowMap,
    /// Column names the source declares.
    pub columns: Vec<KeyString>,
}

/// Collect column names in first-seen order, for sources that declare none.
///
/// JSON, YAML and STIX rows carry their own keys and need not agree, so the
/// column set is their union. An empty source therefore yields no columns,
/// which is indistinguishable from "unknown" and disables the check.
pub(crate) fn union_columns(seen: &mut Vec<KeyString>, row: &ObjectMap) {
    for key in row.keys() {
        if !seen.iter().any(|k| k == key) {
            seen.push(key.clone());
        }
    }
}

/// Dispatch to the correct loader based on source config.
///
/// Used by both `EnrichmentRegistry::load()` (startup) and the refresh loop.
/// STIX and MMDB are handled separately (STIX is async, MMDB uses `TableData::Mmdb`).
pub fn load_from_source(
    source: &EnrichmentSourceConfig,
    table_name: &str,
    key_columns: &[String],
    schema: &ColumnSchema,
) -> crate::Result<LoadedTable> {
    match source {
        EnrichmentSourceConfig::File { path, format } => {
            let detected = format.unwrap_or_else(|| detect_format(path));
            match detected {
                FileFormat::Csv => load_csv(Path::new(path), table_name, key_columns, schema),
                FileFormat::Json => load_json(Path::new(path), table_name, key_columns, schema),
                FileFormat::Yaml => load_yaml(Path::new(path), table_name, key_columns, schema),
                FileFormat::Auto => {
                    let ext_format = detect_format(path);
                    if ext_format == FileFormat::Auto {
                        // Unknown extension — try JSON first, then CSV
                        load_json(Path::new(path), table_name, key_columns, schema)
                            .or_else(|_| load_csv(Path::new(path), table_name, key_columns, schema))
                    } else {
                        load_from_source(
                            &EnrichmentSourceConfig::File {
                                path: path.clone(),
                                format: Some(ext_format),
                            },
                            table_name,
                            key_columns,
                            schema,
                        )
                    }
                }
            }
        }
        #[cfg(feature = "enrichment-sqlite")]
        EnrichmentSourceConfig::Sqlite { path, query } => {
            load_sqlite(Path::new(path), query, table_name, key_columns)
        }
        #[cfg(not(feature = "enrichment-sqlite"))]
        EnrichmentSourceConfig::Sqlite { .. } => Err(crate::Error::Enrichment(
            "SQLite enrichment support not compiled (enable 'enrichment-sqlite' feature)".into(),
        )),
        EnrichmentSourceConfig::Stix { .. } => Err(crate::Error::Enrichment(
            "use load_stix for STIX sources (async)".into(),
        )),
        EnrichmentSourceConfig::Mmdb { .. } => Err(crate::Error::Enrichment(
            "MMDB uses TableData::Mmdb, not HashMap — handled by registry directly".into(),
        )),
    }
}

/// Detect file format from extension.
pub fn detect_format(path: &str) -> FileFormat {
    Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map_or(FileFormat::Auto, |ext| {
            match ext.to_ascii_lowercase().as_str() {
                "csv" => FileFormat::Csv,
                "json" => FileFormat::Json,
                "yaml" | "yml" => FileFormat::Yaml,
                _ => FileFormat::Auto,
            }
        })
}

// ---------------------------------------------------------------------------
// CSV loader
// ---------------------------------------------------------------------------

/// Load a CSV file into an `FxHashMap` keyed by the key columns.
///
/// Uses the `csv` crate for RFC 4180 compliant parsing (handles quoted fields,
/// embedded commas, and escaped quotes).
pub fn load_csv(
    path: &Path,
    table_name: &str,
    key_columns: &[String],
    schema: &ColumnSchema,
) -> crate::Result<LoadedTable> {
    if !path.is_file() {
        return Err(crate::Error::Enrichment(format!(
            "table '{table_name}': file not found: {}",
            path.display()
        )));
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_path(path)
        .map_err(|e| {
            crate::Error::Enrichment(format!(
                "table '{table_name}': read {}: {e}",
                path.display()
            ))
        })?;

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| {
            crate::Error::Enrichment(format!("table '{table_name}': read CSV headers: {e}"))
        })?
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    validate_key_columns(&headers, key_columns, table_name)?;

    let mut rows = RowMap::default();

    for (row_idx, result) in reader.records().enumerate() {
        let record = result.map_err(|e| {
            crate::Error::Enrichment(format!("table '{table_name}': row {}: {e}", row_idx + 2))
        })?;

        if record.len() != headers.len() {
            return Err(crate::Error::Enrichment(format!(
                "table '{table_name}': row {} has {} columns, expected {}",
                row_idx + 2,
                record.len(),
                headers.len()
            )));
        }

        let mut row = ObjectMap::new();
        for (header, value) in headers.iter().zip(record.iter()) {
            let cell = schema.coerce(table_name, header, value)?;
            row.insert(KeyString::from(header.as_str()), cell);
        }

        let key = CompactKey::from_row(&row, key_columns);
        rows.entry(key).or_default().push(Arc::new(row));
    }

    Ok(LoadedTable {
        rows,
        columns: headers.iter().map(|h| KeyString::from(h.as_str())).collect(),
    })
}

// ---------------------------------------------------------------------------
// JSON loader
// ---------------------------------------------------------------------------

/// Load a JSON array file into an `FxHashMap` keyed by the key columns.
pub fn load_json(
    path: &Path,
    table_name: &str,
    key_columns: &[String],
    schema: &ColumnSchema,
) -> crate::Result<LoadedTable> {
    if !path.is_file() {
        return Err(crate::Error::Enrichment(format!(
            "table '{table_name}': file not found: {}",
            path.display()
        )));
    }

    let content = std::fs::read_to_string(path).map_err(|e| {
        crate::Error::Enrichment(format!(
            "table '{table_name}': read {}: {e}",
            path.display()
        ))
    })?;

    let json_array: Vec<serde_json::Value> = serde_json::from_str(&content).map_err(|e| {
        crate::Error::Enrichment(format!("table '{table_name}': invalid JSON: {e}"))
    })?;

    json_objects_to_map(&json_array, table_name, key_columns, schema)
}

// ---------------------------------------------------------------------------
// YAML loader
// ---------------------------------------------------------------------------

/// Load a YAML array file into an `FxHashMap` keyed by the key columns.
pub fn load_yaml(
    path: &Path,
    table_name: &str,
    key_columns: &[String],
    schema: &ColumnSchema,
) -> crate::Result<LoadedTable> {
    if !path.is_file() {
        return Err(crate::Error::Enrichment(format!(
            "table '{table_name}': file not found: {}",
            path.display()
        )));
    }

    let content = std::fs::read_to_string(path).map_err(|e| {
        crate::Error::Enrichment(format!(
            "table '{table_name}': read {}: {e}",
            path.display()
        ))
    })?;

    // Parse YAML to JSON Value array (serde_yaml_ng -> serde_json interop)
    let yaml_array: Vec<serde_json::Value> = serde_yaml_ng::from_str(&content).map_err(|e| {
        crate::Error::Enrichment(format!("table '{table_name}': invalid YAML: {e}"))
    })?;

    json_objects_to_map(&yaml_array, table_name, key_columns, schema)
}

// ---------------------------------------------------------------------------
// SQLite loader (feature-gated)
// ---------------------------------------------------------------------------

/// Load rows from a `SQLite` query into a hash map.
#[cfg(feature = "enrichment-sqlite")]
pub fn load_sqlite(
    path: &Path,
    query: &str,
    table_name: &str,
    key_columns: &[String],
) -> crate::Result<LoadedTable> {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| {
        crate::Error::Enrichment(format!(
            "table '{table_name}': open SQLite {}: {e}",
            path.display()
        ))
    })?;

    let mut stmt = conn.prepare(query).map_err(|e| {
        crate::Error::Enrichment(format!("table '{table_name}': prepare query: {e}"))
    })?;

    let column_names: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|s| (*s).to_string())
        .collect();

    validate_key_columns(&column_names, key_columns, table_name)?;

    let mut rows = RowMap::default();

    let row_iter = stmt
        .query_map([], |row| {
            let mut obj = ObjectMap::new();
            for (i, col_name) in column_names.iter().enumerate() {
                let val = sqlite_value_to_vrl(row, i);
                obj.insert(KeyString::from(col_name.as_str()), val);
            }
            Ok(obj)
        })
        .map_err(|e| {
            crate::Error::Enrichment(format!("table '{table_name}': execute query: {e}"))
        })?;

    for result in row_iter {
        let obj = result.map_err(|e| {
            crate::Error::Enrichment(format!("table '{table_name}': read row: {e}"))
        })?;
        let key = CompactKey::from_row(&obj, key_columns);
        rows.entry(key).or_default().push(Arc::new(obj));
    }

    Ok(LoadedTable {
        rows,
        columns: column_names
            .iter()
            .map(|c| KeyString::from(c.as_str()))
            .collect(),
    })
}

/// Convert a `SQLite` column value to VRL `Value`.
#[cfg(feature = "enrichment-sqlite")]
fn sqlite_value_to_vrl(row: &rusqlite::Row<'_>, idx: usize) -> Value {
    use rusqlite::types::ValueRef;
    match row.get_ref(idx) {
        Ok(ValueRef::Integer(i)) => Value::Integer(i),
        Ok(ValueRef::Real(f)) => {
            // Convert via serde_json to get proper VRL Float wrapping
            Value::from(serde_json::Value::from(f))
        }
        Ok(ValueRef::Text(t)) => Value::from(String::from_utf8_lossy(t).as_ref()),
        Ok(ValueRef::Blob(b)) => Value::from(b),
        Ok(ValueRef::Null) | Err(_) => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// MMDB loader (feature-gated)
// ---------------------------------------------------------------------------

/// Load a `MaxMind` MMDB file. Returns the reader directly (not a hash map).
#[cfg(feature = "enrichment-mmdb")]
pub fn load_mmdb(path: &Path, table_name: &str) -> crate::Result<maxminddb::Reader<Vec<u8>>> {
    maxminddb::Reader::open_readfile(path).map_err(|e| {
        crate::Error::Enrichment(format!(
            "table '{table_name}': open MMDB {}: {e}",
            path.display()
        ))
    })
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Convert a JSON array of objects into an `FxHashMap<CompactKey, Arc<ObjectMap>>`.
///
/// Shared by JSON and YAML loaders (YAML is parsed to JSON Values first).
fn json_objects_to_map(
    items: &[serde_json::Value],
    table_name: &str,
    key_columns: &[String],
    schema: &ColumnSchema,
) -> crate::Result<LoadedTable> {
    let mut rows = RowMap::default();
    let mut columns: Vec<KeyString> = Vec::new();

    for (idx, item) in items.iter().enumerate() {
        let obj = item.as_object().ok_or_else(|| {
            crate::Error::Enrichment(format!(
                "table '{table_name}': element {idx} is not an object"
            ))
        })?;

        for kc in key_columns {
            if !obj.contains_key(kc) {
                return Err(crate::Error::Enrichment(format!(
                    "table '{table_name}': element {idx} missing key column '{kc}'"
                )));
            }
        }

        let mut row = ObjectMap::new();
        for (k, v) in obj {
            let cell = schema.coerce_value(table_name, k, json_to_vrl_value(v))?;
            row.insert(KeyString::from(k.as_str()), cell);
        }

        union_columns(&mut columns, &row);

        let key = CompactKey::from_row(&row, key_columns);
        rows.entry(key).or_default().push(Arc::new(row));
    }

    Ok(LoadedTable { rows, columns })
}

/// Validate that all key columns exist in the header/column list.
fn validate_key_columns(
    headers: &[String],
    key_columns: &[String],
    table_name: &str,
) -> crate::Result<()> {
    for kc in key_columns {
        if !headers.iter().any(|h| h == kc) {
            return Err(crate::Error::Enrichment(format!(
                "table '{table_name}': key column '{kc}' not found in columns: {headers:?}"
            )));
        }
    }
    Ok(())
}

/// Convert a `serde_json::Value` to a VRL `Value`.
fn json_to_vrl_value(v: &serde_json::Value) -> Value {
    Value::from(v.clone())
}

/// Estimate the memory footprint of a materialised enrichment table.
///
/// Counts key bytes + approximate `ObjectMap` overhead per entry.
/// Used for `max_bytes` enforcement.
#[allow(clippy::cast_precision_loss, clippy::implicit_hasher)]
pub fn estimate_table_bytes(map: &RowMap) -> u64 {
    let mut total: u64 = 0;
    // FxHashMap bucket overhead: ~48 bytes per entry
    total += (map.len() as u64) * 48;
    for (key, bucket) in map {
        // CompactKey: Box<[u8]> pointer + data
        total += 16 + key.as_bytes().len() as u64;
        for value in bucket {
            // Arc<ObjectMap>: pointer + refcount + map overhead
            total += 16;
            for (k, v) in value.as_ref() {
                total += k.len() as u64 + estimate_value_bytes(v);
            }
        }
    }
    total
}

/// Rough estimate of VRL `Value` size in bytes.
#[allow(clippy::cast_precision_loss)]
fn estimate_value_bytes(v: &Value) -> u64 {
    match v {
        Value::Bytes(b) => 24 + b.len() as u64,
        Value::Integer(_) | Value::Float(_) | Value::Boolean(_) | Value::Null => 16,
        Value::Object(obj) => {
            let mut size: u64 = 48;
            for (k, inner) in obj {
                size += k.len() as u64 + estimate_value_bytes(inner);
            }
            size
        }
        Value::Array(arr) => {
            let mut size: u64 = 24;
            for inner in arr {
                size += estimate_value_bytes(inner);
            }
            size
        }
        _ => 32,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::LazyLock;

    /// Most loader tests declare no column types.
    static NO_SCHEMA: LazyLock<ColumnSchema> = LazyLock::new(ColumnSchema::default);

    fn write_file(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        fs::write(&path, content).unwrap();
        path
    }

    // -----------------------------------------------------------------------
    // CSV tests
    // -----------------------------------------------------------------------

    #[test]
    fn load_csv_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(
            dir.path(),
            "services.csv",
            "service_id,name,tier\nsvc-001,auth,critical\nsvc-002,web,standard\n",
        );

        let rows = load_csv(&path, "services", &["service_id".into()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);

        let mut cond = ObjectMap::new();
        cond.insert("service_id".into(), Value::from("svc-001"));
        let key = CompactKey::from_row(&cond, &["service_id".into()]);
        let row = &rows.get(&key).unwrap()[0];
        assert_eq!(row.get("name"), Some(&Value::from("auth")));
    }

    #[test]
    fn load_csv_multi_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(
            dir.path(),
            "geo.csv",
            "country,city,tz\nAU,Sydney,AEST\nAU,Melbourne,AEST\nUS,NYC,EST\n",
        );

        let keys = vec!["country".into(), "city".into()];
        let rows = load_csv(&path, "geo", &keys, &NO_SCHEMA).unwrap().rows;
        assert_eq!(rows.len(), 3);

        let mut cond = ObjectMap::new();
        cond.insert("country".into(), Value::from("AU"));
        cond.insert("city".into(), Value::from("Sydney"));
        let key = CompactKey::from_row(&cond, &keys);
        let row = &rows.get(&key).unwrap()[0];
        assert_eq!(row.get("tz"), Some(&Value::from("AEST")));
    }

    #[test]
    fn load_csv_missing_key_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "bad.csv", "a,b\n1,2\n");
        assert!(load_csv(&path, "bad", &["missing".into()], &NO_SCHEMA).is_err());
    }

    #[test]
    fn load_csv_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "empty.csv", "");
        assert!(load_csv(&path, "empty", &["id".into()], &NO_SCHEMA).is_err());
    }

    #[test]
    fn load_csv_mismatched_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "bad.csv", "a,b,c\n1,2\n");
        assert!(load_csv(&path, "bad", &["a".into()], &NO_SCHEMA).is_err());
    }

    #[test]
    fn load_csv_file_not_found() {
        assert!(
            load_csv(
                Path::new("/nonexistent.csv"),
                "t",
                &["id".into()],
                &NO_SCHEMA
            )
            .is_err()
        );
    }

    // -----------------------------------------------------------------------
    // JSON tests
    // -----------------------------------------------------------------------

    #[test]
    fn load_json_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(
            dir.path(),
            "services.json",
            r#"[
                {"service_id": "svc-001", "name": "auth"},
                {"service_id": "svc-002", "name": "web"}
            ]"#,
        );

        let rows = load_json(&path, "services", &["service_id".into()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn load_json_missing_key_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "bad.json", r#"[{"a": 1}]"#);
        assert!(load_json(&path, "bad", &["missing".into()], &NO_SCHEMA).is_err());
    }

    #[test]
    fn load_json_not_array() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "bad.json", r#"{"not": "array"}"#);
        assert!(load_json(&path, "bad", &["id".into()], &NO_SCHEMA).is_err());
    }

    #[test]
    fn load_json_element_not_object() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "bad.json", "[1, 2, 3]");
        assert!(load_json(&path, "bad", &["id".into()], &NO_SCHEMA).is_err());
    }

    // -----------------------------------------------------------------------
    // YAML tests
    // -----------------------------------------------------------------------

    #[test]
    fn load_yaml_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(
            dir.path(),
            "routes.yaml",
            r#"- route_id: "SYD-MEL"
  origin: "YSSY"
  destination: "YMML"
- route_id: "SYD-BNE"
  origin: "YSSY"
  destination: "YBBN"
"#,
        );

        let rows = load_yaml(&path, "routes", &["route_id".into()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);

        let mut cond = ObjectMap::new();
        cond.insert("route_id".into(), Value::from("SYD-MEL"));
        let key = CompactKey::from_row(&cond, &["route_id".into()]);
        let row = &rows.get(&key).unwrap()[0];
        assert_eq!(row.get("origin"), Some(&Value::from("YSSY")));
    }

    #[test]
    fn load_yaml_missing_key_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "bad.yaml", "- a: 1\n");
        assert!(load_yaml(&path, "bad", &["missing".into()], &NO_SCHEMA).is_err());
    }

    // -----------------------------------------------------------------------
    // SQLite tests (feature-gated)
    // -----------------------------------------------------------------------

    #[cfg(feature = "enrichment-sqlite")]
    #[test]
    fn load_sqlite_basic() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create test DB
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE subscribers (msisdn TEXT, plan TEXT, region TEXT);
             INSERT INTO subscribers VALUES ('61400000001', 'premium', 'AU');
             INSERT INTO subscribers VALUES ('61400000002', 'basic', 'NZ');",
        )
        .unwrap();
        drop(conn);

        let rows = load_sqlite(
            &db_path,
            "SELECT msisdn, plan, region FROM subscribers",
            "subscribers",
            &["msisdn".into()],
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 2);

        let mut condition = ObjectMap::new();
        condition.insert("msisdn".into(), Value::from("61400000001"));
        let key = CompactKey::from_row(&condition, &["msisdn".to_string()]);
        let row = &rows.get(&key).unwrap()[0];
        assert_eq!(row.get("plan"), Some(&Value::from("premium")));
    }

    #[cfg(feature = "enrichment-sqlite")]
    #[test]
    fn load_sqlite_missing_key_column() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE t (a TEXT); INSERT INTO t VALUES ('x');")
            .unwrap();
        drop(conn);

        assert!(load_sqlite(&db_path, "SELECT a FROM t", "t", &["missing".into()]).is_err());
    }

    // -----------------------------------------------------------------------
    // Format detection tests
    // -----------------------------------------------------------------------

    #[test]
    fn detect_format_csv() {
        assert_eq!(detect_format("/data/table.csv"), FileFormat::Csv);
        assert_eq!(detect_format("/data/TABLE.CSV"), FileFormat::Csv);
    }

    #[test]
    fn load_from_source_auto_with_json_extension_works() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        std::fs::write(
            &path,
            r#"[{"ip":"1.2.3.4","country":"AU"},{"ip":"5.6.7.8","country":"NZ"}]"#,
        )
        .unwrap();
        let src = EnrichmentSourceConfig::File {
            path: path.to_string_lossy().to_string(),
            format: None,
        };
        let rows = load_from_source(&src, "geo", &["ip".to_string()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn load_from_source_auto_with_csv_extension_works() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.csv");
        std::fs::write(&path, "k,v\nfoo,1\nbar,2\n").unwrap();
        let src = EnrichmentSourceConfig::File {
            path: path.to_string_lossy().to_string(),
            format: None,
        };
        let rows = load_from_source(&src, "table", &["k".to_string()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn load_from_source_auto_unknown_extension_json_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.dat");
        std::fs::write(&path, r#"[{"k":"a","v":"1"}]"#).unwrap();
        let src = EnrichmentSourceConfig::File {
            path: path.to_string_lossy().to_string(),
            format: None,
        };
        let rows = load_from_source(&src, "ext", &["k".to_string()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn load_from_source_auto_unknown_extension_csv_fallback() {
        let dir = tempfile::tempdir().unwrap();
        // JSON parse fails, CSV parse should succeed.
        let path = dir.path().join("data.dat");
        std::fs::write(&path, "k,v\nhello,world\n").unwrap();
        let src = EnrichmentSourceConfig::File {
            path: path.to_string_lossy().to_string(),
            format: None,
        };
        let rows = load_from_source(&src, "ext", &["k".to_string()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn load_from_source_explicit_format_overrides_extension() {
        let dir = tempfile::tempdir().unwrap();
        // File is CSV content but named .json — explicit format CSV should win.
        let path = dir.path().join("tricky.json");
        std::fs::write(&path, "k,v\nx,1\n").unwrap();
        let src = EnrichmentSourceConfig::File {
            path: path.to_string_lossy().to_string(),
            format: Some(FileFormat::Csv),
        };
        let rows = load_from_source(&src, "t", &["k".to_string()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn load_from_source_yaml_format_via_extension() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.yaml");
        std::fs::write(&path, "- k: hello\n  v: world\n- k: foo\n  v: bar\n").unwrap();
        let src = EnrichmentSourceConfig::File {
            path: path.to_string_lossy().to_string(),
            format: None,
        };
        let rows = load_from_source(&src, "y", &["k".to_string()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn detect_format_json() {
        assert_eq!(detect_format("/data/table.json"), FileFormat::Json);
    }

    #[test]
    fn detect_format_yaml() {
        assert_eq!(detect_format("/data/table.yaml"), FileFormat::Yaml);
        assert_eq!(detect_format("/data/table.yml"), FileFormat::Yaml);
    }

    #[test]
    fn detect_format_unknown() {
        assert_eq!(detect_format("/data/table.toml"), FileFormat::Auto);
    }

    // -----------------------------------------------------------------------
    // Dispatcher tests
    // -----------------------------------------------------------------------

    #[test]
    fn load_from_source_file_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "svc.csv", "id,name\n1,auth\n");

        let source = EnrichmentSourceConfig::File {
            path: path.to_string_lossy().to_string(),
            format: Some(FileFormat::Csv),
        };
        let rows = load_from_source(&source, "svc", &["id".into()], &NO_SCHEMA)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn load_from_source_stix_returns_error() {
        let source = EnrichmentSourceConfig::Stix {
            path: None,
            url: Some("https://example.com".into()),
            collection: None,
            auth: None,
        };
        assert!(load_from_source(&source, "t", &["id".into()], &NO_SCHEMA).is_err());
    }

    #[test]
    fn load_from_source_mmdb_returns_error() {
        let source = EnrichmentSourceConfig::Mmdb {
            path: "/data/geo.mmdb".into(),
        };
        assert!(load_from_source(&source, "t", &["id".into()], &NO_SCHEMA).is_err());
    }

    // -----------------------------------------------------------------------
    // Memory estimation tests
    // -----------------------------------------------------------------------

    #[test]
    fn estimate_table_bytes_empty() {
        let map = RowMap::default();
        assert_eq!(estimate_table_bytes(&map), 0);
    }

    #[test]
    fn estimate_table_bytes_nonzero() {
        let mut map = RowMap::default();
        let mut row = ObjectMap::new();
        row.insert("id".into(), Value::from("test"));
        let key = CompactKey::from_row(&row, &["id".into()]);
        map.insert(key, vec![Arc::new(row)]);
        assert!(estimate_table_bytes(&map) > 0);
    }
}
