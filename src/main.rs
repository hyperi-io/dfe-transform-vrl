// Project:   dfe-transform-vrl
// File:      src/main.rs
// Purpose:   CLI entry point and orchestrator
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CLI entry point for dfe-transform-vrl.

use std::sync::Arc;

use clap::{Parser, Subcommand};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo};
use hyperi_rustlib::deployment::{generate_chart, generate_compose_fragment, generate_dockerfile};
use hyperi_rustlib::metrics::MetricsManager;
use tracing::{error, info};

use dfe_transform_vrl::config::Config;
use dfe_transform_vrl::engine::compiler;
use dfe_transform_vrl::{deployment, health, metrics, pipeline};

#[derive(Parser, Debug)]
#[command(name = "dfe-transform-vrl")]
#[command(version, about, long_about = None)]
struct App {
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

#[tokio::main]
async fn main() {
    let app = App::parse();

    if let Some(ref cmd) = app.command {
        match cmd {
            AppCommand::EmitDockerfile => {
                let contract = deployment::contract();
                println!("{}", generate_dockerfile(&contract));
                return;
            }
            AppCommand::EmitChart { dir } => {
                let contract = deployment::contract();
                if let Err(e) = generate_chart(&contract, dir) {
                    eprintln!("error: failed to generate Helm chart: {e}");
                    std::process::exit(1);
                }
                eprintln!("Helm chart generated in {dir}/");
                return;
            }
            AppCommand::EmitCompose => {
                let contract = deployment::contract();
                println!("{}", generate_compose_fragment(&contract));
                return;
            }
            AppCommand::EmitContract => {
                let contract = deployment::contract();
                println!("{}", contract.to_json());
                return;
            }
            _ => {}
        }
    }

    if let Err(e) = hyperi_rustlib::cli::run_app(app).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

async fn run_transform_service(config: Config) -> anyhow::Result<()> {
    info!(
        pipeline = %config.pipeline.name,
        version = env!("CARGO_PKG_VERSION"),
        "starting dfe-transform-vrl"
    );

    // 1. Load and compile VRL programs (extract program before any .await)
    let program = {
        let vrl_source = compiler::load_vrl_source(&config.transforms)
            .map_err(|e| anyhow::anyhow!("VRL source loading failed: {e}"))?;
        let compilation = compiler::compile_vrl(&vrl_source)
            .map_err(|e| anyhow::anyhow!("VRL compilation failed: {e}"))?;
        Arc::new(compilation.program)
    };
    info!("VRL program compiled");

    // 2. Shutdown coordination
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // 3. Start health server (rustlib http-server)
    let ready_flag = health::start_health_server(&config.health.address, shutdown_rx.clone())
        .await
        .map_err(|e| anyhow::anyhow!("health server failed: {e}"))?;

    // 4. Start metrics server (rustlib MetricsManager)
    let mut metrics_manager = MetricsManager::new("transform_vrl");
    let transform_metrics = metrics::TransformMetrics::new(&metrics_manager);
    metrics::start_metrics_server(&mut metrics_manager, &config.metrics.address)
        .await
        .map_err(|e| anyhow::anyhow!("metrics server failed: {e}"))?;

    // 5. Run pipeline (consume -> transform -> produce -> commit)
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

    // 6. Wait for SIGTERM/SIGINT
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| anyhow::anyhow!("signal handler error: {e}"))?;
    info!("received shutdown signal");
    let _ = shutdown_tx.send(true);

    // 7. Wait for pipeline to drain
    match pipeline_handle.await {
        Ok(Ok(())) => info!("pipeline shutdown complete"),
        Ok(Err(e)) => error!(error = %e, "pipeline shutdown with error"),
        Err(e) => error!(error = %e, "pipeline task panicked"),
    }

    info!("shutdown complete");
    Ok(())
}
