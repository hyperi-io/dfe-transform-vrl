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

use std::sync::Arc;

use clap::{Parser, Subcommand};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo};
use hyperi_rustlib::deployment::{generate_chart, generate_compose_fragment, generate_dockerfile};
use hyperi_rustlib::metrics::MetricsManager;
use tracing::{error, info};

use crate::config::Config;
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

    async fn run_service(&self, config: Config) -> Result<(), CliError> {
        run_transform_service(config)
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

async fn run_transform_service(config: Config) -> anyhow::Result<()> {
    info!(
        pipeline = %config.pipeline.name,
        version = env!("CARGO_PKG_VERSION"),
        "starting dfe-transform-vrl"
    );

    // Compile VRL programs (fail-fast before any async work)
    let program = {
        let vrl_source = compiler::load_vrl_source(&config.transforms)
            .map_err(|e| anyhow::anyhow!("VRL source loading failed: {e}"))?;
        let compilation = compiler::compile_vrl(&vrl_source)
            .map_err(|e| anyhow::anyhow!("VRL compilation failed: {e}"))?;
        Arc::new(compilation.program)
    };
    info!("VRL program compiled");

    // Shutdown coordination
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Health server
    let ready_flag = health::start_health_server(&config.health.address, shutdown_rx.clone())
        .await
        .map_err(|e| anyhow::anyhow!("health server failed: {e}"))?;

    // Metrics server
    let mut metrics_manager = MetricsManager::new("transform_vrl");
    let transform_metrics = metrics::TransformMetrics::new(&metrics_manager);
    metrics::start_metrics_server(&mut metrics_manager, &config.metrics.address)
        .await
        .map_err(|e| anyhow::anyhow!("metrics server failed: {e}"))?;

    // Pipeline
    let pipeline_shutdown_rx = shutdown_rx.clone();
    let pipeline_handle = tokio::spawn(async move {
        pipeline::run(
            &config,
            program,
            &transform_metrics,
            ready_flag,
            pipeline_shutdown_rx,
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
