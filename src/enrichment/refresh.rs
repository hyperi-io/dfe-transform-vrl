//! Project:   dfe-transform-vrl
//! File:      src/enrichment/refresh.rs
//! Purpose:   Background refresh for enrichment tables with periodic reload
//! Language:  Rust
//!
//! License:   FSL-1.1-ALv2
//! Copyright: (c) 2026 HYPERI PTY LIMITED

//! Spawns per-table tokio tasks that periodically reload enrichment data
//! and atomically swap the inner `FxHashMap` via `ArcSwap`.
//!
//! Fail-safe: on reload failure, keeps existing data and logs the error.
//! Minimum interval enforced: 60 seconds (clamped at runtime).

use std::sync::Arc;

use tracing::{error, info};

use crate::config::EnrichmentSourceConfig;
use crate::enrichment::loader;
use crate::enrichment::registry::EnrichmentRegistry;
use crate::metrics::TransformMetrics;

/// Spawn background refresh tasks for all tables that have refresh config.
///
/// Each refreshable table gets its own tokio task that sleeps for
/// `interval_secs` then reloads and swaps. Tasks shut down when the
/// shutdown signal fires.
pub fn start_refresh_tasks(
    registry: &Arc<EnrichmentRegistry>,
    metrics: &Arc<TransformMetrics>,
    shutdown_rx: &tokio::sync::watch::Receiver<bool>,
) {
    for table in registry.tables() {
        let Some(refresh) = table.refresh_config() else {
            continue;
        };
        let Some(source) = table.source_config() else {
            continue;
        };

        // Enforce minimum 60s interval
        let interval_secs = refresh.interval_secs.max(60);
        let table_name = table.name().to_string();
        let source = source.clone();
        let key_columns: Vec<String> = table
            .key_columns()
            .map_or_else(Vec::new, <[String]>::to_vec);
        let reg = Arc::clone(registry);
        let m = Arc::clone(metrics);
        let mut rx = shutdown_rx.clone();

        tokio::spawn(async move {
            let mut timer = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            // Skip the first immediate tick (data was just loaded at startup)
            timer.tick().await;

            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        info!(table = %table_name, "refresh task shutting down");
                        break;
                    }
                    _ = timer.tick() => {
                        let reg = Arc::clone(&reg);
                        let name = table_name.clone();
                        let src = source.clone();
                        let cols = key_columns.clone();
                        let metrics = Arc::clone(&m);
                        let _ = tokio::task::spawn_blocking(move || {
                            reload_table(&reg, &name, &src, &cols, &metrics);
                        }).await;
                    }
                }
            }
        });

        info!(
            table = %table.name(),
            interval_secs,
            "started enrichment refresh task"
        );
    }
}

/// Reload a single table from its source config and swap the data.
fn reload_table(
    registry: &EnrichmentRegistry,
    table_name: &str,
    source: &EnrichmentSourceConfig,
    key_columns: &[String],
    metrics: &TransformMetrics,
) {
    let start = std::time::Instant::now();

    let result = match source {
        EnrichmentSourceConfig::Stix {
            path,
            url: _,
            collection: _,
            auth: _,
        } => {
            // File-based STIX only for sync refresh (HTTP needs async wiring)
            path.as_ref().map_or_else(
                || {
                    Err(crate::Error::Enrichment(format!(
                        "table '{table_name}': STIX HTTP refresh not yet supported"
                    )))
                },
                |file_path| {
                    crate::enrichment::stix::load_stix(
                        Some(file_path.as_str()),
                        None,
                        None,
                        table_name,
                        key_columns,
                    )
                },
            )
        }
        EnrichmentSourceConfig::Mmdb { .. } => {
            // MMDB refresh would need to swap the Reader, not the HashMap
            Err(crate::Error::Enrichment(format!(
                "table '{table_name}': MMDB refresh not yet supported"
            )))
        }
        _ => loader::load_from_source(source, table_name, key_columns),
    };

    let elapsed = start.elapsed().as_secs_f64();

    match result {
        Ok(new_data) => {
            if let Some(table) = registry.get_table(table_name) {
                if let Err(e) = table.swap_hashmap(new_data) {
                    error!(table = %table_name, error = %e, "enrichment swap failed");
                    metrics.record_enrichment_reload(table_name, elapsed, false);
                    return;
                }
                metrics.record_enrichment_reload(table_name, elapsed, true);
                metrics.set_enrichment_rows(table_name, table.len());
                info!(table = %table_name, elapsed_ms = %(elapsed * 1000.0), "enrichment table refreshed");
            }
        }
        Err(e) => {
            error!(
                table = %table_name,
                error = %e,
                "enrichment refresh failed, keeping existing data"
            );
            metrics.record_enrichment_reload(table_name, elapsed, false);
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::cloned_ref_to_slice_refs,
    clippy::redundant_clone,
    clippy::missing_docs_in_private_items,
    clippy::doc_markdown
)]
mod tests {
    use super::*;
    use crate::config::loader::{EnrichmentTableConfig, RefreshConfig};
    use std::io::Write;

    /// Build a temp CSV file we can reload against.
    fn write_csv(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, "{content}").unwrap();
        path
    }

    /// Standard no-op metrics used by reload_table.
    fn make_metrics() -> Arc<TransformMetrics> {
        Arc::new(TransformMetrics::default())
    }

    #[test]
    fn reload_table_succeeds_with_csv_source() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_csv(
            dir.path(),
            "geo.csv",
            "ip,country\n1.2.3.4,AU\n5.6.7.8,NZ\n",
        );

        // Build a registry with a single CSV-backed table
        let table_cfg = EnrichmentTableConfig {
            name: "geo".to_string(),
            source: Some(EnrichmentSourceConfig::File {
                path: csv.to_string_lossy().to_string(),
                format: None,
            }),
            key_columns: vec!["ip".to_string()],
            max_bytes: None,
            refresh: Some(RefreshConfig { interval_secs: 60 }),
            ..Default::default()
        };
        let registry = crate::enrichment::EnrichmentRegistry::load(&[table_cfg.clone()]).unwrap();
        let reg_arc = registry.into_arc();

        let metrics = make_metrics();

        // Trigger a reload
        reload_table(
            &reg_arc,
            "geo",
            &table_cfg.source.clone().unwrap(),
            &table_cfg.key_columns,
            &metrics,
        );

        // Table should still exist and be populated
        let table = reg_arc.get_table("geo").unwrap();
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn reload_table_mmdb_source_returns_error_gracefully() {
        // Build a registry with ANY loadable table so the registry isn't empty.
        let dir = tempfile::tempdir().unwrap();
        let csv = write_csv(dir.path(), "t.csv", "k,v\na,1\n");
        let table_cfg = EnrichmentTableConfig {
            name: "trap".to_string(),
            source: Some(EnrichmentSourceConfig::File {
                path: csv.to_string_lossy().to_string(),
                format: None,
            }),
            key_columns: vec!["k".to_string()],
            ..Default::default()
        };
        let registry = crate::enrichment::EnrichmentRegistry::load(&[table_cfg]).unwrap();
        let reg_arc = registry.into_arc();
        let metrics = make_metrics();

        // Mmdb source path — reload_table should fail gracefully and NOT
        // swap the existing data.
        let mmdb_source = EnrichmentSourceConfig::Mmdb {
            path: "/nonexistent/geo.mmdb".to_string(),
        };
        reload_table(&reg_arc, "trap", &mmdb_source, &[], &metrics);

        // Table is still there with its original CSV data
        let table = reg_arc.get_table("trap").unwrap();
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn reload_table_stix_http_without_path_errors() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_csv(dir.path(), "t.csv", "k,v\na,1\n");
        let table_cfg = EnrichmentTableConfig {
            name: "stix".to_string(),
            source: Some(EnrichmentSourceConfig::File {
                path: csv.to_string_lossy().to_string(),
                format: None,
            }),
            key_columns: vec!["k".to_string()],
            ..Default::default()
        };
        let registry = crate::enrichment::EnrichmentRegistry::load(&[table_cfg]).unwrap();
        let reg_arc = registry.into_arc();
        let metrics = make_metrics();

        // STIX with no path, no URL — should hit the unsupported branch
        let stix_no_path = EnrichmentSourceConfig::Stix {
            path: None,
            url: Some("https://example.invalid/taxii".to_string()),
            collection: None,
            auth: None,
        };
        reload_table(&reg_arc, "stix", &stix_no_path, &[], &metrics);

        // Existing CSV data preserved
        let table = reg_arc.get_table("stix").unwrap();
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn reload_table_missing_file_preserves_existing_data() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_csv(dir.path(), "ok.csv", "k,v\nhello,world\n");
        let table_cfg = EnrichmentTableConfig {
            name: "data".to_string(),
            source: Some(EnrichmentSourceConfig::File {
                path: csv.to_string_lossy().to_string(),
                format: None,
            }),
            key_columns: vec!["k".to_string()],
            ..Default::default()
        };
        let registry = crate::enrichment::EnrichmentRegistry::load(&[table_cfg]).unwrap();
        let reg_arc = registry.into_arc();
        let metrics = make_metrics();

        // Reload from a source that doesn't exist — fail-safe should
        // keep the old row.
        let bad_source = EnrichmentSourceConfig::File {
            path: "/nonexistent/path/data.csv".to_string(),
            format: None,
        };
        reload_table(&reg_arc, "data", &bad_source, &["k".to_string()], &metrics);

        let table = reg_arc.get_table("data").unwrap();
        assert_eq!(table.len(), 1, "reload failure must not wipe existing data");
    }

    #[test]
    fn reload_table_swaps_to_new_data_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_csv(dir.path(), "v1.csv", "k,v\nx,1\n");
        let table_cfg = EnrichmentTableConfig {
            name: "swap".to_string(),
            source: Some(EnrichmentSourceConfig::File {
                path: csv.to_string_lossy().to_string(),
                format: None,
            }),
            key_columns: vec!["k".to_string()],
            ..Default::default()
        };
        let registry = crate::enrichment::EnrichmentRegistry::load(&[table_cfg.clone()]).unwrap();
        let reg_arc = registry.into_arc();
        let metrics = make_metrics();

        // Assert initial state
        let table = reg_arc.get_table("swap").unwrap();
        assert_eq!(table.len(), 1);

        // Now write a new CSV with more rows and reload
        let csv2 = write_csv(dir.path(), "v2.csv", "k,v\nx,1\ny,2\nz,3\n");
        let new_source = EnrichmentSourceConfig::File {
            path: csv2.to_string_lossy().to_string(),
            format: None,
        };
        reload_table(&reg_arc, "swap", &new_source, &["k".to_string()], &metrics);

        let table = reg_arc.get_table("swap").unwrap();
        assert_eq!(table.len(), 3, "table should reflect new rows post-reload");
    }

    #[test]
    fn start_refresh_tasks_skips_tables_without_refresh_config() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_csv(dir.path(), "static.csv", "k,v\na,1\n");
        let table_cfg = EnrichmentTableConfig {
            name: "static_table".to_string(),
            source: Some(EnrichmentSourceConfig::File {
                path: csv.to_string_lossy().to_string(),
                format: None,
            }),
            key_columns: vec!["k".to_string()],
            refresh: None, // No refresh config
            ..Default::default()
        };
        let registry = crate::enrichment::EnrichmentRegistry::load(&[table_cfg]).unwrap();
        let reg_arc = registry.into_arc();
        let metrics = make_metrics();
        let (_tx, rx) = tokio::sync::watch::channel(false);

        // Should not spawn any task (table has no refresh config)
        start_refresh_tasks(&reg_arc, &metrics, &rx);
    }

    #[tokio::test]
    async fn start_refresh_tasks_spawns_and_shuts_down_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_csv(dir.path(), "refreshable.csv", "k,v\na,1\n");
        let table_cfg = EnrichmentTableConfig {
            name: "refreshable".to_string(),
            source: Some(EnrichmentSourceConfig::File {
                path: csv.to_string_lossy().to_string(),
                format: None,
            }),
            key_columns: vec!["k".to_string()],
            refresh: Some(RefreshConfig { interval_secs: 60 }),
            ..Default::default()
        };
        let registry = crate::enrichment::EnrichmentRegistry::load(&[table_cfg]).unwrap();
        let reg_arc = registry.into_arc();
        let metrics = make_metrics();
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Spawn the refresh task
        start_refresh_tasks(&reg_arc, &metrics, &rx);

        // Signal shutdown immediately
        let _ = tx.send(true);

        // Give the task time to observe the shutdown signal and exit
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
