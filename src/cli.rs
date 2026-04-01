// Project:   dfe-transform-vrl
// File:      src/cli.rs
// Purpose:   CLI definition and service orchestrator
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CLI definition and service lifecycle orchestrator.
//!
//! Implements the `DfeApp` trait from rustlib, wiring together config loading,
//! VRL compilation, health/metrics servers, pipeline, and graceful shutdown.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo};
use hyperi_rustlib::config::reloader::{ConfigReloader, ReloaderConfig};
use hyperi_rustlib::config::shared::SharedConfig;
use hyperi_rustlib::deployment::{generate_chart, generate_compose_fragment, generate_dockerfile};
use hyperi_rustlib::memory::{MemoryGuard, MemoryGuardConfig};
use hyperi_rustlib::metrics::MetricsManager;
use tracing::{error, info};

use crate::config::Config;
use crate::config::hot::HotConfig;
use crate::engine::compiler;
use crate::{deployment, health, metrics, pipeline};

#[derive(Parser, Debug)]
#[command(name = "dfe-transform-vrl")]
#[command(version, about, long_about = None)]
pub struct App {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<AppCommand>,
}

#[derive(Subcommand, Clone, Debug)]
enum AppCommand {
    Run,
    Version,
    #[command(name = "config-check")]
    ConfigCheck,
    #[command(name = "emit-dockerfile")]
    EmitDockerfile,
    #[command(name = "emit-chart")]
    EmitChart {
        dir: String,
    },
    #[command(name = "emit-compose")]
    EmitCompose,
    #[command(name = "emit-contract")]
    EmitContract,
}

impl DfeApp for App {
    type Config = Config;

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "dfe-transform-vrl"
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn env_prefix(&self) -> &str {
        "DFE_TRANSFORM"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("dfe-transform-vrl", env!("CARGO_PKG_VERSION"))
    }

    fn common_args(&self) -> &CommonArgs {
        &self.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        match &self.command {
            Some(AppCommand::Version) => {
                static VERSION: StandardCommand = StandardCommand::Version;
                Some(&VERSION)
            }
            Some(AppCommand::ConfigCheck) => {
                static CONFIG_CHECK: StandardCommand = StandardCommand::ConfigCheck;
                Some(&CONFIG_CHECK)
            }
            _ => None,
        }
    }

    fn load_config(&self, path: Option<&str>) -> Result<Config, CliError> {
        let config =
            Config::load(path).map_err(|e| CliError::Config(format!("failed to load: {e}")))?;
        config
            .validate()
            .map_err(|e| CliError::Config(format!("validation failed: {e}")))?;
        Ok(config)
    }

    async fn run_service(
        &self,
        config: Config,
        runtime: hyperi_rustlib::cli::ServiceRuntime,
    ) -> Result<(), CliError> {
        run_transform_service(config, self.common.config.clone(), runtime)
            .await
            .map_err(|e| CliError::Service(e.to_string()))
    }
}

/// Handle emit subcommands that bypass the normal `DfeApp` lifecycle.
/// Returns `Some(())` if handled, `None` if not an emit command.
pub fn handle_emit_command(app: &App) -> Option<()> {
    let cmd = app.command.as_ref()?;
    match cmd {
        AppCommand::EmitDockerfile => {
            let contract = deployment::contract();
            println!("{}", generate_dockerfile(&contract));
            Some(())
        }
        AppCommand::EmitChart { dir } => {
            let contract = deployment::contract();
            if let Err(e) = generate_chart(&contract, dir) {
                eprintln!("error: failed to generate Helm chart: {e}");
                std::process::exit(1);
            }
            eprintln!("Helm chart generated in {dir}/");
            Some(())
        }
        AppCommand::EmitCompose => {
            let contract = deployment::contract();
            println!("{}", generate_compose_fragment(&contract));
            Some(())
        }
        AppCommand::EmitContract => {
            let contract = deployment::contract();
            println!("{}", contract.to_json());
            Some(())
        }
        _ => None,
    }
}

async fn run_transform_service(
    config: Config,
    config_path: Option<String>,
    runtime: hyperi_rustlib::cli::ServiceRuntime,
) -> anyhow::Result<()> {
    info!(
        pipeline = %config.pipeline.name,
        version = env!("CARGO_PKG_VERSION"),
        "starting dfe-transform-vrl"
    );

    // Compile VRL programs (fail-fast before any async work)
    // Compile VRL programs and load enrichment tables
    let vrl_source = compiler::load_vrl_source(&config.transforms)
        .map_err(|e| anyhow::anyhow!("VRL source loading failed: {e}"))?;

    let enrichment_registry = if config.enrichment_tables.is_empty() {
        None
    } else {
        let registry = crate::enrichment::EnrichmentRegistry::load(&config.enrichment_tables)
            .map_err(|e| anyhow::anyhow!("enrichment table loading failed: {e}"))?;
        info!(tables = registry.len(), "enrichment tables loaded");
        Some(registry.into_arc())
    };

    let program = {
        let compilation = compiler::compile_vrl(&vrl_source, enrichment_registry.clone())
            .map_err(|e| anyhow::anyhow!("VRL compilation failed: {e}"))?;
        Arc::new(compilation.program)
    };
    info!("VRL program compiled");

    // Shutdown coordination
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Memory guard — cgroup-aware backpressure (Pattern B: pause consumer)
    // Created early so readiness check can reference it.
    let memory_guard = Arc::new(MemoryGuard::new(MemoryGuardConfig::from_env(
        "DFE_TRANSFORM_VRL",
    )));
    info!(
        limit_bytes = memory_guard.limit_bytes(),
        "memory guard initialised"
    );

    // Health server
    let ready_flag = health::start_health_server(&config.health.address, shutdown_rx.clone())
        .await
        .map_err(|e| anyhow::anyhow!("health server failed: {e}"))?;

    // Metrics server
    let mut metrics_manager = MetricsManager::new("dfe_transform_vrl");
    let commit_hash = option_env!("GIT_COMMIT").unwrap_or("unknown");
    let transform_metrics =
        metrics::TransformMetrics::new(&metrics_manager, env!("CARGO_PKG_VERSION"), commit_hash);

    // Wire readiness check into metrics manager
    let readiness_flag = Arc::clone(&ready_flag);
    let readiness_guard = Arc::clone(&memory_guard);
    metrics_manager.set_readiness_check(move || {
        readiness_flag.load(std::sync::atomic::Ordering::Acquire)
            && !readiness_guard.under_pressure()
    });

    info!(address = %config.metrics.address, "starting metrics server");
    metrics_manager
        .start_server(&config.metrics.address)
        .await
        .map_err(|e| anyhow::anyhow!("metrics server failed: {e}"))?;

    // Hot-reloadable config subset (read by pipeline each batch)
    let hot_config = SharedConfig::new(HotConfig::from_config(&config));
    info!(
        batch_size = config.pipeline.batch_size,
        batch_timeout_ms = config.pipeline.batch_timeout_ms,
        key_field = %config.sink.key_field,
        "hot-reloadable config initialised"
    );

    // Config reloader: file polling + SIGHUP → reload hot-config subset
    let resolved_config_path = config_path.map(PathBuf::from).or_else(|| {
        ["config.yaml", "config.yml"]
            .iter()
            .map(PathBuf::from)
            .find(|p| p.exists())
    });

    let _reloader_handle = {
        let reload_path = resolved_config_path.clone();
        let reloader = ConfigReloader::new(
            ReloaderConfig {
                config_path: resolved_config_path,
                poll_interval: Duration::from_secs(5),
                debounce: Duration::from_millis(500),
                enable_sighup: true,
                ..Default::default()
            },
            hot_config.clone(),
            move || {
                let full = Config::load(reload_path.as_ref().and_then(|p| p.to_str()))
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
                full.validate()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
                Ok(HotConfig::from_config(&full))
            },
            |hot| {
                if hot.batch_size == 0 {
                    return Err("batch_size must be > 0".into());
                }
                Ok(())
            },
        )
        .with_post_reload_hook(|_hot| {
            hyperi_rustlib::logger::security::config_changed(
                "config_reload",
                "system",
                "pipeline config reloaded",
            );
        });
        reloader.start()
    };

    // Enrichment refresh tasks (per-table background reload)
    let transform_metrics = Arc::new(transform_metrics);
    if let Some(ref reg) = enrichment_registry {
        crate::enrichment::refresh::start_refresh_tasks(reg, &transform_metrics, &shutdown_rx);
    }

    // Pipeline
    let pipeline_shutdown_rx = shutdown_rx.clone();
    let pipeline_hot_config = hot_config.clone();
    let pipeline_memory_guard = Arc::clone(&memory_guard);
    let pipeline_worker_pool = runtime.worker_pool.clone();
    let pipeline_handle = tokio::spawn(async move {
        pipeline::run(
            &config,
            program,
            pipeline_hot_config,
            &transform_metrics,
            ready_flag,
            pipeline_memory_guard,
            pipeline_shutdown_rx,
            pipeline_worker_pool,
        )
        .await
    });

    // Wait for SIGTERM (K8s) or SIGINT (Ctrl+C)
    wait_for_shutdown_signal().await?;
    info!("received shutdown signal");
    let _ = shutdown_tx.send(true);

    // Wait for pipeline to drain
    match pipeline_handle.await {
        Ok(Ok(())) => info!("pipeline shutdown complete"),
        Ok(Err(e)) => error!(error = %e, "pipeline shutdown with error"),
        Err(e) => error!(error = %e, "pipeline task panicked"),
    }

    info!("shutdown complete");
    Ok(())
}

/// Wait for either SIGTERM or SIGINT.
///
/// K8s sends SIGTERM before killing pods. Ctrl+C sends SIGINT for local dev.
async fn wait_for_shutdown_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| anyhow::anyhow!("SIGTERM handler: {e}"))?;

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.map_err(|e| anyhow::anyhow!("SIGINT handler: {e}"))
        }
        _ = sigterm.recv() => Ok(()),
    }
}
