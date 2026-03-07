// Project:   dfe-transform-vrl
// File:      src/main.rs
// Purpose:   CLI entry point and orchestrator
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CLI entry point for dfe-transform-vrl.
//!
//! Uses hyperi-rustlib CLI module for standard arguments and subcommands.
//! Implements the [`DfeApp`] trait for the standard DFE service lifecycle.

use clap::{Parser, Subcommand};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo};
use hyperi_rustlib::deployment::{generate_chart, generate_compose_fragment, generate_dockerfile};
use tracing::info;

use dfe_transform_vrl::config::Config;
use dfe_transform_vrl::deployment;

/// dfe-transform-vrl: Kafka-to-Kafka transform pipelines with embedded VRL engine.
#[derive(Parser, Debug)]
#[command(name = "dfe-transform-vrl")]
#[command(version, about, long_about = None)]
struct App {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<AppCommand>,
}

/// Application subcommands.
#[derive(Subcommand, Clone, Debug)]
enum AppCommand {
    /// Start the service (default if no subcommand given).
    Run,

    /// Print version information and exit.
    Version,

    /// Validate configuration and exit.
    #[command(name = "config-check")]
    ConfigCheck,

    /// Generate Dockerfile to stdout.
    #[command(name = "emit-dockerfile")]
    EmitDockerfile,

    /// Generate Helm chart to the given directory.
    #[command(name = "emit-chart")]
    EmitChart {
        /// Output directory for the chart.
        dir: String,
    },

    /// Generate Docker Compose fragment to stdout.
    #[command(name = "emit-compose")]
    EmitCompose,

    /// Print deployment contract as JSON to stdout.
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

/// Main service loop.
async fn run_transform_service(config: Config) -> anyhow::Result<()> {
    info!(
        pipeline = %config.pipeline.name,
        version = env!("CARGO_PKG_VERSION"),
        "starting dfe-transform-vrl"
    );

    // Lifecycle:
    // 1. Load and compile VRL programs
    // 2. Start health + metrics servers
    // 3. Create Kafka consumer + producer
    // 4. Run event loop (consume → transform → produce → commit)
    // 5. Handle SIGTERM for graceful shutdown

    info!("shutdown complete");
    Ok(())
}
