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
