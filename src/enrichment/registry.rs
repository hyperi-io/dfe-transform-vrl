// Project:   dfe-transform-vrl
// File:      src/enrichment/registry.rs
// Purpose:   Enrichment table registry — multi-source loading and dispatch
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Enrichment table registry — loads tables from multiple source types into
//! `FxHashMap`-backed `EnrichmentTable` instances for O(1) lookups.

use std::sync::Arc;

use rustc_hash::FxHashMap;
use tracing::info;

use crate::config::{EnrichmentSourceConfig, EnrichmentTableConfig};
use crate::enrichment::loader;
use crate::enrichment::table::EnrichmentTable;

/// Collection of named enrichment tables.
///
/// Created at startup via `load()`. Individual tables support atomic refresh
/// via `ArcSwap` (see `EnrichmentTable::swap_hashmap`).
#[derive(Debug)]
pub struct EnrichmentRegistry {
    tables: FxHashMap<String, EnrichmentTable>,
}

impl EnrichmentRegistry {
    /// Load all enrichment tables from config. Fails fast on any error.
    ///
    /// Dispatches on `resolved_source()` to call the appropriate loader.
    /// STIX HTTP sources are not supported here (require async context).
    /// STIX file sources and all other types are loaded synchronously.
    pub fn load(configs: &[EnrichmentTableConfig]) -> crate::Result<Self> {
        let mut tables = FxHashMap::default();

        for config in configs {
            let source = config.resolved_source()?;
            let table = load_table_from_source(config, &source)?;

            // Enforce max_bytes if configured
            if let Some(max_bytes) = config.max_bytes {
                let estimated = table.estimated_bytes();
                if estimated > max_bytes {
                    return Err(crate::Error::Enrichment(format!(
                        "table '{}': materialised size ({estimated} bytes) exceeds max_bytes ({max_bytes})",
                        config.name
                    )));
                }
            }

            info!(
                table = %table.name(),
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

    /// Check if a table exists (used at VRL compile time for validation).
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

    /// Iterator over all tables (for refresh task to find refreshable tables).
    pub fn tables(&self) -> impl Iterator<Item = &EnrichmentTable> {
        self.tables.values()
    }

    /// Wrap in Arc for sharing between VRL compiler and runtime.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

/// Load a single enrichment table from a resolved source config.
fn load_table_from_source(
    config: &EnrichmentTableConfig,
    source: &EnrichmentSourceConfig,
) -> crate::Result<EnrichmentTable> {
    match source {
        EnrichmentSourceConfig::Mmdb { path: mmdb_path } => {
            #[cfg(feature = "enrichment-mmdb")]
            {
                let reader = loader::load_mmdb(std::path::Path::new(mmdb_path), &config.name)?;
                Ok(EnrichmentTable::new_mmdb(
                    &config.name,
                    reader,
                    config.source.clone(),
                    config.refresh.clone(),
                ))
            }
            #[cfg(not(feature = "enrichment-mmdb"))]
            {
                let _ = mmdb_path;
                Err(crate::Error::Enrichment(
                    "MMDB enrichment support not compiled (enable 'enrichment-mmdb' feature)"
                        .into(),
                ))
            }
        }
        EnrichmentSourceConfig::Stix {
            path,
            url: _,
            collection: _,
            auth: _,
        } => {
            // File-based STIX: load synchronously
            if let Some(file_path) = path {
                let map = crate::enrichment::stix::load_stix(
                    Some(file_path.as_str()),
                    None,
                    None,
                    &config.name,
                    &config.key_columns,
                )?;
                Ok(EnrichmentTable::new_hashmap(
                    &config.name,
                    map,
                    config.key_columns.clone(),
                    config.source.clone(),
                    config.refresh.clone(),
                ))
            } else {
                // URL-based STIX requires async — cannot load here
                Err(crate::Error::Enrichment(format!(
                    "table '{}': STIX HTTP sources require async loading (wire via refresh task or provide a file path)",
                    config.name
                )))
            }
        }
        _ => {
            // File, SQLite — all dispatched through load_from_source
            let map = loader::load_from_source(source, &config.name, &config.key_columns)?;
            Ok(EnrichmentTable::new_hashmap(
                &config.name,
                map,
                config.key_columns.clone(),
                config.source.clone(),
                config.refresh.clone(),
            ))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use vrl::value::{ObjectMap, Value};

    fn write_file(dir: &Path, name: &str, content: &str) -> String {
        let path = dir.join(name);
        fs::write(&path, content).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn registry_load_csv_legacy_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(
            dir.path(),
            "services.csv",
            "service_id,name,tier\nsvc-001,auth,critical\nsvc-002,web,standard\n",
        );

        let configs = vec![EnrichmentTableConfig {
            name: "services".into(),
            path,
            key_columns: vec!["service_id".into()],
            ..Default::default()
        }];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        assert_eq!(registry.len(), 1);
        assert!(registry.has_table("services"));

        let table = registry.get_table("services").unwrap();
        assert_eq!(table.len(), 2);

        let mut cond = ObjectMap::new();
        cond.insert("service_id".into(), Value::from("svc-001"));
        let row = table.get_record(&cond).unwrap();
        assert_eq!(row.get("name"), Some(&Value::from("auth")));
    }

    #[test]
    fn registry_load_json_legacy_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(
            dir.path(),
            "geo.json",
            r#"[{"cc": "AU", "name": "Australia"}, {"cc": "NZ", "name": "New Zealand"}]"#,
        );

        let configs = vec![EnrichmentTableConfig {
            name: "geo".into(),
            path,
            key_columns: vec!["cc".into()],
            ..Default::default()
        }];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        assert_eq!(registry.len(), 1);

        let table = registry.get_table("geo").unwrap();
        let mut cond = ObjectMap::new();
        cond.insert("cc".into(), Value::from("AU"));
        let row = table.get_record(&cond).unwrap();
        assert_eq!(row.get("name"), Some(&Value::from("Australia")));
    }

    #[test]
    fn registry_load_new_format_file() {
        let dir = tempfile::tempdir().unwrap();
        let _path = write_file(dir.path(), "svc.csv", "id,name\n1,auth\n");

        let configs = vec![EnrichmentTableConfig {
            name: "svc".into(),
            path: String::new(),
            key_columns: vec!["id".into()],
            source: Some(EnrichmentSourceConfig::File {
                path: dir.path().join("svc.csv").to_string_lossy().to_string(),
                format: Some(crate::config::FileFormat::Csv),
            }),
            ..Default::default()
        }];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn registry_load_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let _path = write_file(
            dir.path(),
            "routes.yaml",
            "- route_id: SYD-MEL\n  origin: YSSY\n- route_id: SYD-BNE\n  origin: YSSY\n",
        );

        let configs = vec![EnrichmentTableConfig {
            name: "routes".into(),
            path: String::new(),
            key_columns: vec!["route_id".into()],
            source: Some(EnrichmentSourceConfig::File {
                path: dir.path().join("routes.yaml").to_string_lossy().to_string(),
                format: Some(crate::config::FileFormat::Yaml),
            }),
            ..Default::default()
        }];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        let table = registry.get_table("routes").unwrap();
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn registry_load_stix_file() {
        let dir = tempfile::tempdir().unwrap();
        let stix = r#"{
            "type": "bundle",
            "objects": [
                {
                    "type": "indicator",
                    "pattern": "[ipv4-addr:value = '10.0.0.1']",
                    "confidence": 90,
                    "name": "Test IOC"
                }
            ]
        }"#;
        let _path = write_file(dir.path(), "stix.json", stix);

        let configs = vec![EnrichmentTableConfig {
            name: "threats".into(),
            path: String::new(),
            key_columns: vec!["indicator".into()],
            source: Some(EnrichmentSourceConfig::Stix {
                path: Some(dir.path().join("stix.json").to_string_lossy().to_string()),
                url: None,
                collection: None,
                auth: None,
            }),
            ..Default::default()
        }];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        let table = registry.get_table("threats").unwrap();
        assert_eq!(table.len(), 1);

        let mut cond = ObjectMap::new();
        cond.insert("indicator".into(), Value::from("10.0.0.1"));
        let row = table.get_record(&cond).unwrap();
        assert_eq!(row.get("name"), Some(&Value::from("Test IOC")));
    }

    #[test]
    fn registry_load_multiple_tables() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = write_file(dir.path(), "svc.csv", "id,name\n1,auth\n");
        let json_path = write_file(
            dir.path(),
            "geo.json",
            r#"[{"cc": "AU", "name": "Australia"}]"#,
        );

        let configs = vec![
            EnrichmentTableConfig {
                name: "services".into(),
                path: csv_path,
                key_columns: vec!["id".into()],
                ..Default::default()
            },
            EnrichmentTableConfig {
                name: "geo".into(),
                path: json_path,
                key_columns: vec!["cc".into()],
                ..Default::default()
            },
        ];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        assert_eq!(registry.len(), 2);
        assert!(registry.has_table("services"));
        assert!(registry.has_table("geo"));
        assert!(!registry.has_table("missing"));
    }

    #[test]
    fn registry_table_names() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "t.csv", "id\n1\n");

        let configs = vec![EnrichmentTableConfig {
            name: "test".into(),
            path,
            key_columns: vec!["id".into()],
            ..Default::default()
        }];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        let names = registry.table_names();
        assert!(names.contains(&"test"));
    }

    #[test]
    fn registry_missing_file_fails() {
        let configs = vec![EnrichmentTableConfig {
            name: "missing".into(),
            path: "/nonexistent.csv".into(),
            key_columns: vec!["id".into()],
            ..Default::default()
        }];

        assert!(EnrichmentRegistry::load(&configs).is_err());
    }

    #[test]
    fn registry_no_source_configured_fails() {
        let configs = vec![EnrichmentTableConfig {
            name: "empty".into(),
            ..Default::default()
        }];

        assert!(EnrichmentRegistry::load(&configs).is_err());
    }

    #[test]
    fn registry_max_bytes_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "big.csv", "id,data\n1,aaaa\n2,bbbb\n");

        let configs = vec![EnrichmentTableConfig {
            name: "big".into(),
            path,
            key_columns: vec!["id".into()],
            max_bytes: Some(1), // 1 byte — will always exceed
            ..Default::default()
        }];

        let result = EnrichmentRegistry::load(&configs);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("exceeds max_bytes"));
    }

    #[test]
    fn registry_find_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(
            dir.path(),
            "data.json",
            r#"[
                {"country": "AU", "city": "Sydney"},
                {"country": "AU", "city": "Melbourne"},
                {"country": "US", "city": "NYC"}
            ]"#,
        );

        let configs = vec![EnrichmentTableConfig {
            name: "cities".into(),
            path,
            key_columns: vec!["country".into(), "city".into()],
            ..Default::default()
        }];

        let registry = EnrichmentRegistry::load(&configs).unwrap();
        let table = registry.get_table("cities").unwrap();

        let mut cond = ObjectMap::new();
        cond.insert("country".into(), Value::from("AU"));
        let matches = table.find_records(&cond);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn registry_into_arc() {
        let registry = EnrichmentRegistry::load(&[]).unwrap();
        let arc = registry.into_arc();
        assert!(arc.is_empty());
    }
}
