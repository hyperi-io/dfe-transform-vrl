// Project:   dfe-transform-vrl
// File:      src/enrichment/registry.rs
// Purpose:   Enrichment table loading from CSV/JSON files
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Enrichment table registry — loads CSV/JSON files into HashMap-backed tables.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tracing::info;
use vrl::value::{KeyString, ObjectMap, Value};

use crate::config::EnrichmentTableConfig;

/// A single enrichment table backed by a `HashMap<String, ObjectMap>`.
///
/// Key is the concatenation of key column values (joined with `\x00`).
/// Value is the full row as a VRL-compatible `ObjectMap`.
#[derive(Debug, Clone)]
pub struct EnrichmentTable {
    name: String,
    rows: HashMap<String, ObjectMap>,
    key_columns: Vec<String>,
}

impl EnrichmentTable {
    /// Look up a single record by key values.
    ///
    /// `condition` is an object mapping key column names to expected values.
    /// All key columns must match for a hit.
    pub fn get_record(&self, condition: &ObjectMap) -> Option<&ObjectMap> {
        let key = self.build_key(condition)?;
        self.rows.get(&key)
    }

    /// Find all records matching partial conditions.
    ///
    /// Unlike `get_record` which requires all key columns, this scans
    /// for rows where all provided condition fields match.
    pub fn find_records(&self, condition: &ObjectMap) -> Vec<&ObjectMap> {
        if condition.is_empty() {
            return Vec::new();
        }

        self.rows
            .values()
            .filter(|row| {
                condition
                    .iter()
                    .all(|(k, v)| row.get(k.as_str()).is_some_and(|row_v| row_v == v))
            })
            .collect()
    }

    /// Table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn build_key(&self, condition: &ObjectMap) -> Option<String> {
        let mut parts = Vec::with_capacity(self.key_columns.len());
        for col in &self.key_columns {
            let val = condition.get(col.as_str())?;
            parts.push(value_to_key_string(val));
        }
        Some(parts.join("\x00"))
    }
}

/// Collection of named enrichment tables.
#[derive(Debug, Clone, Default)]
pub struct EnrichmentRegistry {
    tables: HashMap<String, EnrichmentTable>,
}

impl EnrichmentRegistry {
    /// Load all enrichment tables from config. Fails fast on any error.
    pub fn load(configs: &[EnrichmentTableConfig]) -> crate::Result<Self> {
        let mut tables = HashMap::with_capacity(configs.len());

        for config in configs {
            let table = load_table(config)?;
            info!(
                table = %table.name,
                rows = table.len(),
                key_columns = ?config.key_columns,
                "loaded enrichment table"
            );
            tables.insert(config.name.clone(), table);
        }

        Ok(Self { tables })
    }

    /// Look up a table by name.
    pub fn get_table(&self, name: &str) -> Option<&EnrichmentTable> {
        self.tables.get(name)
    }

    /// Check if a table exists (used at compile time for validation).
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Table names (for error messages).
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(String::as_str).collect()
    }

    /// Number of tables.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Wrap in Arc for sharing between compiler and runtime.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

/// Load a single enrichment table from file.
fn load_table(config: &EnrichmentTableConfig) -> crate::Result<EnrichmentTable> {
    let path = Path::new(&config.path);
    if !path.is_file() {
        return Err(crate::Error::Config(format!(
            "enrichment table '{}': file not found: {}",
            config.name, config.path
        )));
    }

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    let rows = match ext.as_str() {
        "csv" => load_csv(path, &config.name, &config.key_columns)?,
        "json" => load_json(path, &config.name, &config.key_columns)?,
        other => {
            return Err(crate::Error::Config(format!(
                "enrichment table '{}': unsupported format '.{other}' (supported: .csv, .json)",
                config.name
            )));
        }
    };

    Ok(EnrichmentTable {
        name: config.name.clone(),
        rows,
        key_columns: config.key_columns.clone(),
    })
}

/// Load a CSV file into a `HashMap` keyed by the key columns.
fn load_csv(
    path: &Path,
    table_name: &str,
    key_columns: &[String],
) -> crate::Result<HashMap<String, ObjectMap>> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        crate::Error::Config(format!(
            "enrichment table '{table_name}': failed to read {}: {e}",
            path.display()
        ))
    })?;

    let mut rows = HashMap::new();
    let mut lines = content.lines();

    let header_line = lines.next().ok_or_else(|| {
        crate::Error::Config(format!(
            "enrichment table '{table_name}': CSV file is empty"
        ))
    })?;

    let headers: Vec<&str> = header_line.split(',').map(str::trim).collect();

    // Validate key columns exist in headers
    for kc in key_columns {
        if !headers.contains(&kc.as_str()) {
            return Err(crate::Error::Config(format!(
                "enrichment table '{table_name}': key column '{kc}' not found in CSV headers: {headers:?}"
            )));
        }
    }

    for (line_num, line) in lines.enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let values: Vec<&str> = line.split(',').map(str::trim).collect();
        if values.len() != headers.len() {
            return Err(crate::Error::Config(format!(
                "enrichment table '{table_name}': row {} has {} columns, expected {} (header count)",
                line_num + 2,
                values.len(),
                headers.len()
            )));
        }

        let mut row = ObjectMap::new();
        for (header, value) in headers.iter().zip(values.iter()) {
            row.insert(KeyString::from(*header), Value::from(*value));
        }

        let key = build_row_key(&row, key_columns);
        rows.insert(key, row);
    }

    Ok(rows)
}

/// Load a JSON array file into a `HashMap` keyed by the key columns.
fn load_json(
    path: &Path,
    table_name: &str,
    key_columns: &[String],
) -> crate::Result<HashMap<String, ObjectMap>> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        crate::Error::Config(format!(
            "enrichment table '{table_name}': failed to read {}: {e}",
            path.display()
        ))
    })?;

    let json_array: Vec<serde_json::Value> = serde_json::from_str(&content).map_err(|e| {
        crate::Error::Config(format!(
            "enrichment table '{table_name}': invalid JSON: {e}"
        ))
    })?;

    let mut rows = HashMap::with_capacity(json_array.len());

    for (idx, item) in json_array.iter().enumerate() {
        let obj = item.as_object().ok_or_else(|| {
            crate::Error::Config(format!(
                "enrichment table '{table_name}': element {idx} is not a JSON object"
            ))
        })?;

        // Validate key columns exist
        for kc in key_columns {
            if !obj.contains_key(kc) {
                return Err(crate::Error::Config(format!(
                    "enrichment table '{table_name}': element {idx} missing key column '{kc}'"
                )));
            }
        }

        let row: ObjectMap = obj
            .iter()
            .map(|(k, v)| (KeyString::from(k.as_str()), json_to_vrl_value(v)))
            .collect();

        let key = build_row_key(&row, key_columns);
        rows.insert(key, row);
    }

    Ok(rows)
}

/// Build a lookup key from row values for the given key columns.
fn build_row_key(row: &ObjectMap, key_columns: &[String]) -> String {
    key_columns
        .iter()
        .map(|col| {
            row.get(col.as_str())
                .map(value_to_key_string)
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join("\x00")
}

/// Convert a VRL Value to a string for key building.
fn value_to_key_string(v: &Value) -> String {
    match v {
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        other => format!("{other}"),
    }
}

/// Convert a `serde_json::Value` to a VRL `Value`.
fn json_to_vrl_value(v: &serde_json::Value) -> Value {
    Value::from(v.clone())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs;

    fn write_csv(dir: &Path, name: &str, content: &str) -> String {
        let path = dir.join(name);
        fs::write(&path, content).unwrap();
        path.to_string_lossy().to_string()
    }

    fn write_json(dir: &Path, name: &str, content: &str) -> String {
        let path = dir.join(name);
        fs::write(&path, content).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn test_load_csv_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_csv(
            dir.path(),
            "services.csv",
            "service_id,name,tier\nsvc-001,auth,critical\nsvc-002,web,standard\n",
        );

        let config = EnrichmentTableConfig {
            name: "services".into(),
            path,
            key_columns: vec!["service_id".into()],
        };

        let table = load_table(&config).unwrap();
        assert_eq!(table.len(), 2);

        let mut cond = ObjectMap::new();
        cond.insert("service_id".into(), Value::from("svc-001"));
        let row = table.get_record(&cond).unwrap();
        assert_eq!(row.get("name"), Some(&Value::from("auth")));
        assert_eq!(row.get("tier"), Some(&Value::from("critical")));
    }

    #[test]
    fn test_load_csv_multi_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_csv(
            dir.path(),
            "geo.csv",
            "country,city,timezone\nAU,Sydney,AEST\nAU,Melbourne,AEST\nUS,NYC,EST\n",
        );

        let config = EnrichmentTableConfig {
            name: "geo".into(),
            path,
            key_columns: vec!["country".into(), "city".into()],
        };

        let table = load_table(&config).unwrap();
        assert_eq!(table.len(), 3);

        let mut cond = ObjectMap::new();
        cond.insert("country".into(), Value::from("AU"));
        cond.insert("city".into(), Value::from("Sydney"));
        let row = table.get_record(&cond).unwrap();
        assert_eq!(row.get("timezone"), Some(&Value::from("AEST")));
    }

    #[test]
    fn test_load_csv_missing_key_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_csv(dir.path(), "bad.csv", "a,b\n1,2\n");

        let config = EnrichmentTableConfig {
            name: "bad".into(),
            path,
            key_columns: vec!["missing_col".into()],
        };

        assert!(load_table(&config).is_err());
    }

    #[test]
    fn test_load_csv_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_csv(dir.path(), "empty.csv", "");

        let config = EnrichmentTableConfig {
            name: "empty".into(),
            path,
            key_columns: vec!["id".into()],
        };

        assert!(load_table(&config).is_err());
    }

    #[test]
    fn test_load_csv_mismatched_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_csv(dir.path(), "bad.csv", "a,b,c\n1,2\n");

        let config = EnrichmentTableConfig {
            name: "bad".into(),
            path,
            key_columns: vec!["a".into()],
        };

        assert!(load_table(&config).is_err());
    }

    #[test]
    fn test_load_json_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_json(
            dir.path(),
            "services.json",
            r#"[
                {"service_id": "svc-001", "name": "auth", "tier": "critical"},
                {"service_id": "svc-002", "name": "web", "tier": "standard"}
            ]"#,
        );

        let config = EnrichmentTableConfig {
            name: "services".into(),
            path,
            key_columns: vec!["service_id".into()],
        };

        let table = load_table(&config).unwrap();
        assert_eq!(table.len(), 2);

        let mut cond = ObjectMap::new();
        cond.insert("service_id".into(), Value::from("svc-002"));
        let row = table.get_record(&cond).unwrap();
        assert_eq!(row.get("name"), Some(&Value::from("web")));
    }

    #[test]
    fn test_load_json_missing_key_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_json(dir.path(), "bad.json", r#"[{"a": 1, "b": 2}]"#);

        let config = EnrichmentTableConfig {
            name: "bad".into(),
            path,
            key_columns: vec!["missing".into()],
        };

        assert!(load_table(&config).is_err());
    }

    #[test]
    fn test_load_json_not_array() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_json(dir.path(), "bad.json", r#"{"not": "array"}"#);

        let config = EnrichmentTableConfig {
            name: "bad".into(),
            path,
            key_columns: vec!["id".into()],
        };

        assert!(load_table(&config).is_err());
    }

    #[test]
    fn test_load_json_element_not_object() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_json(dir.path(), "bad.json", "[1, 2, 3]");

        let config = EnrichmentTableConfig {
            name: "bad".into(),
            path,
            key_columns: vec!["id".into()],
        };

        assert!(load_table(&config).is_err());
    }

    #[test]
    fn test_missing_file() {
        let config = EnrichmentTableConfig {
            name: "missing".into(),
            path: "/nonexistent/file.csv".into(),
            key_columns: vec!["id".into()],
        };

        assert!(load_table(&config).is_err());
    }

    #[test]
    fn test_unsupported_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.toml");
        fs::write(&path, "key = 'value'\n").unwrap();

        let config = EnrichmentTableConfig {
            name: "bad".into(),
            path: path.to_string_lossy().to_string(),
            key_columns: vec!["id".into()],
        };

        assert!(load_table(&config).is_err());
    }

    #[test]
    fn test_registry_load() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = write_csv(dir.path(), "svc.csv", "id,name\n1,auth\n");
        let json_path = write_json(
            dir.path(),
            "geo.json",
            r#"[{"cc": "AU", "name": "Australia"}]"#,
        );

        let configs = vec![
            EnrichmentTableConfig {
                name: "services".into(),
                path: csv_path,
                key_columns: vec!["id".into()],
            },
            EnrichmentTableConfig {
                name: "geo".into(),
                path: json_path,
                key_columns: vec!["cc".into()],
            },
        ];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        assert_eq!(registry.len(), 2);
        assert!(registry.has_table("services"));
        assert!(registry.has_table("geo"));
        assert!(!registry.has_table("missing"));
    }

    #[test]
    fn test_find_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_json(
            dir.path(),
            "data.json",
            r#"[
                {"country": "AU", "city": "Sydney", "pop": 5000000},
                {"country": "AU", "city": "Melbourne", "pop": 4500000},
                {"country": "US", "city": "NYC", "pop": 8000000}
            ]"#,
        );

        let config = EnrichmentTableConfig {
            name: "cities".into(),
            path,
            key_columns: vec!["country".into(), "city".into()],
        };

        let table = load_table(&config).unwrap();

        let mut cond = ObjectMap::new();
        cond.insert("country".into(), Value::from("AU"));
        let matches = table.find_records(&cond);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_find_records_empty_condition() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_json(dir.path(), "data.json", r#"[{"id": "1", "name": "a"}]"#);

        let config = EnrichmentTableConfig {
            name: "t".into(),
            path,
            key_columns: vec!["id".into()],
        };

        let table = load_table(&config).unwrap();
        let empty = ObjectMap::new();
        assert!(table.find_records(&empty).is_empty());
    }
}
