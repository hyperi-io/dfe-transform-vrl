// Project:   dfe-transform-vrl
// File:      src/cli.rs
// Purpose:   CLI definition and service orchestrator
// Language:  Rust
//
// License:   BUSL-1.1
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
use tracing::{debug, error, info};

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
    /// Standard rustlib commands (run, version, config-check, generate-artefacts, metrics-manifest).
    #[command(flatten)]
    Standard(StandardCommand),
    #[command(name = "emit-dockerfile")]
    EmitDockerfile,
    #[command(name = "emit-chart")]
    EmitChart { dir: String },
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
            Some(AppCommand::Standard(cmd)) => Some(cmd),
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

    fn deployment_contract(&self) -> Option<hyperi_rustlib::deployment::DeploymentContract> {
        Some(crate::deployment::contract())
    }
}

/// Handle emit subcommands that bypass the normal `DfeApp` lifecycle.
/// Returns `Some(())` if handled, `None` if not an emit command.
pub fn handle_emit_command(app: &App) -> Option<()> {
    let cmd = app.command.as_ref()?;
    match cmd {
        AppCommand::EmitDockerfile => {
            let contract = deployment::contract();
            println!("{}", generate_dockerfile(&contract, None));
            Some(())
        }
        AppCommand::EmitChart { dir } => {
            let contract = deployment::contract();
            if let Err(e) = generate_chart(&contract, dir, None) {
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
        AppCommand::Standard(_) => None,
    }
}

#[allow(clippy::too_many_lines)]
async fn run_transform_service(
    config: Config,
    config_path: Option<String>,
    mut runtime: hyperi_rustlib::cli::ServiceRuntime,
) -> anyhow::Result<()> {
    info!(
        pipeline = %config.pipeline.name,
        version = env!("CARGO_PKG_VERSION"),
        "starting dfe-transform-vrl"
    );

    // Log derived DFE topology — operators rely on this to confirm the
    // wrapper joined the right consumer group and resolves the right
    // source name from the topic naming convention. Pattern matches
    // dfe-transform-wasm; useful when debugging consumer-group surprises.
    let derived_source = crate::kafka::derive_dfe_source(&config.source);
    let consumer_group = crate::kafka::derive_consumer_group(&config.source, &config.pipeline.name);
    info!(
        source_topics = ?config.source.topics,
        sink_topic = %config.sink.topic,
        consumer_group = %consumer_group,
        dfe_source = ?derived_source.as_ref().map(hyperi_rustlib::DfeSource::name),
        dfe_input_topic = ?derived_source.as_ref().map(hyperi_rustlib::DfeSource::input_topic),
        dfe_output_topic = ?derived_source.as_ref().map(hyperi_rustlib::DfeSource::output_topic),
        "DFE topology"
    );

    debug!(
        brokers = ?config.source.brokers,
        group_id = %config.source.group_id,
        topics = ?config.source.topics,
        input_format = %config.source.format,
        sink_brokers = ?config.sink.brokers,
        sink_topic = %config.sink.topic,
        key_field = %config.sink.key_field,
        batch_size = config.pipeline.batch_size,
        batch_timeout_ms = config.pipeline.batch_timeout_ms,
        transform_dir = ?config.transforms.dir,
        transform_files = ?config.transforms.files,
        enrichment_tables = config.enrichment_tables.len(),
        "startup config"
    );

    // Compile VRL programs (fail-fast before any async work)
    // Compile VRL programs and load enrichment tables
    let vrl_source = compiler::load_vrl_source(&config.transforms)
        .map_err(|e| anyhow::anyhow!("VRL source loading failed: {e}"))?;

    let enrichment_registry = if config.enrichment_tables.is_empty() {
        None
    } else {
        for table_cfg in &config.enrichment_tables {
            debug!(
                table = %table_cfg.name,
                key_columns = ?table_cfg.key_columns,
                max_bytes = ?table_cfg.max_bytes,
                refresh = table_cfg.refresh.as_ref().map(|r| r.interval_secs),
                "loading enrichment table"
            );
        }
        let registry = crate::enrichment::EnrichmentRegistry::load(&config.enrichment_tables)
            .map_err(|e| anyhow::anyhow!("enrichment table loading failed: {e}"))?;
        for table in registry.tables() {
            debug!(
                table = %table.name(),
                rows = table.len(),
                "enrichment table loaded"
            );
        }
        info!(tables = registry.len(), "enrichment tables loaded");
        Some(registry.into_arc())
    };

    let program = {
        let compilation = compiler::compile_vrl(&vrl_source, enrichment_registry.clone())
            .map_err(|e| anyhow::anyhow!("VRL compilation failed: {e}"))?;
        Arc::new(compilation.program)
    };
    // VRL compiles all source files into a single program; log program count (always 1)
    debug!(
        source_bytes = vrl_source.len(),
        programs_loaded = 1,
        "VRL program compiled"
    );
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

    // Metrics: reuse the MetricsManager that rustlib's DfeApp framework has
    // already constructed and started for us (`runtime.metrics`). NEVER
    // construct a new MetricsManager here — that creates a second server on
    // the same port and the wrapper crashes with EADDRINUSE on startup (GH
    // issue #11). The framework already exposes everything we register on
    // the global recorder; we just wire our app-specific metrics + readiness
    // into the running manager.
    //
    // `config.metrics.address` is intentionally ignored — rustlib's
    // `--metrics-addr` (env `METRICS_ADDR`, default `0.0.0.0:9090`) is the
    // single source of truth. Charts / deployments override that env var,
    // not the YAML field, to relocate the endpoint.
    let commit_hash = option_env!("GIT_COMMIT").unwrap_or("unknown");
    let transform_metrics =
        metrics::TransformMetrics::new(&runtime.metrics, env!("CARGO_PKG_VERSION"), commit_hash);

    // Wire readiness check into the running manager.
    let readiness_flag = Arc::clone(&ready_flag);
    let readiness_guard = Arc::clone(&memory_guard);
    runtime.metrics.set_readiness_check(move || {
        readiness_flag.load(std::sync::atomic::Ordering::Acquire)
            && !readiness_guard.under_pressure()
    });
    info!("readiness check wired into rustlib metrics server");

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
        .with_post_reload_hook(|hot| {
            debug!(
                batch_size = hot.batch_size,
                batch_timeout_ms = hot.batch_timeout_ms,
                key_field = %hot.key_field,
                "config reload applied"
            );
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc
)]
mod tests {
    use super::*;
    use clap::Parser;

    /// Parse a fake argv so we can exercise `App` without a real process.
    fn parse(args: &[&str]) -> App {
        let mut owned: Vec<String> = vec!["dfe-transform-vrl".to_string()];
        owned.extend(args.iter().map(|s| (*s).to_string()));
        App::parse_from(owned)
    }

    #[test]
    fn app_parses_run_command() {
        let app = parse(&["run"]);
        assert!(matches!(
            app.command,
            Some(AppCommand::Standard(StandardCommand::Run))
        ));
    }

    #[test]
    fn app_parses_version_command() {
        let app = parse(&["version"]);
        assert!(matches!(
            app.command,
            Some(AppCommand::Standard(StandardCommand::Version))
        ));
    }

    #[test]
    fn app_parses_config_check_command() {
        let app = parse(&["config-check"]);
        assert!(matches!(
            app.command,
            Some(AppCommand::Standard(StandardCommand::ConfigCheck))
        ));
    }

    #[test]
    fn app_parses_emit_dockerfile_command() {
        let app = parse(&["emit-dockerfile"]);
        assert!(matches!(app.command, Some(AppCommand::EmitDockerfile)));
    }

    #[test]
    fn app_parses_emit_chart_with_dir() {
        let app = parse(&["emit-chart", "/tmp/chart-out"]);
        match &app.command {
            Some(AppCommand::EmitChart { dir }) => assert_eq!(dir, "/tmp/chart-out"),
            other => panic!("expected EmitChart, got {other:?}"),
        }
    }

    #[test]
    fn app_parses_emit_compose_command() {
        let app = parse(&["emit-compose"]);
        assert!(matches!(app.command, Some(AppCommand::EmitCompose)));
    }

    #[test]
    fn app_parses_emit_contract_command() {
        let app = parse(&["emit-contract"]);
        assert!(matches!(app.command, Some(AppCommand::EmitContract)));
    }

    #[test]
    fn app_parses_no_subcommand() {
        let app = parse(&[]);
        assert!(app.command.is_none());
    }

    #[test]
    fn dfe_app_identity() {
        let app = parse(&["run"]);
        assert_eq!(app.name(), "dfe-transform-vrl");
        assert_eq!(app.env_prefix(), "DFE_TRANSFORM");
        let version = app.version_info();
        assert_eq!(version.name, "dfe-transform-vrl");
        assert!(!version.version.is_empty());
    }

    #[test]
    fn dfe_app_common_args_accessible() {
        let app = parse(&["run"]);
        // Just verifies the accessor compiles and returns a reference.
        let _args = app.common_args();
    }

    #[test]
    fn command_maps_standard_variants() {
        // After the StandardCommand flatten, every standard subcommand
        // (run, version, config-check, generate-artefacts, metrics-manifest)
        // returns Some via the trait. Only emit-* / non-standard variants
        // return None.
        let version_app = parse(&["version"]);
        assert!(version_app.command().is_some());
        let config_check_app = parse(&["config-check"]);
        assert!(config_check_app.command().is_some());
        let run_app = parse(&["run"]);
        assert!(run_app.command().is_some());
        let emit_app = parse(&["emit-dockerfile"]);
        assert!(emit_app.command().is_none());
    }

    #[test]
    fn load_config_missing_path_returns_file_not_found() {
        // GH issue #9: explicit `--config <path>` MUST fail-fast with a
        // clear "file not found" error when the file doesn't exist. The
        // previous behaviour (silent fallback to defaults) hid mount-path
        // typos and surfaced as misleading
        // "sink.topic must not be empty" several stages later.
        let app = parse(&["run"]);
        let result = app.load_config(Some("/nonexistent/path/config.yaml"));
        let err = result.expect_err("missing --config path must error");
        let msg = err.to_string();
        assert!(
            msg.contains("config file not found"),
            "error must say 'config file not found', got: {msg}"
        );
        assert!(
            msg.contains("/nonexistent/path/config.yaml"),
            "error must include the actual missing path, got: {msg}"
        );
        // Must NOT fall through to validation errors like "sink.topic must
        // not be empty" — those mislead operators into thinking the config
        // is loaded but partially wrong.
        assert!(
            !msg.contains("sink.topic"),
            "must not leak validation error from default config, got: {msg}"
        );
    }

    #[test]
    fn load_config_rejects_invalid_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.yaml");
        std::fs::write(&path, "not valid yaml: [unclosed").unwrap();

        let app = parse(&["run"]);
        let result = app.load_config(Some(path.to_str().unwrap()));
        assert!(result.is_err(), "malformed YAML should error");
    }

    #[test]
    fn load_config_accepts_valid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ok.yaml");
        let yaml = r#"
pipeline:
  name: "test-pipeline"
  batch_size: 100
source:
  brokers: ["localhost:9092"]
  group_id: "test-group"
  topics: ["in"]
  format: "json"
sink:
  brokers: ["localhost:9092"]
  topic: "out"
  key_field: ".id"
  compression: "none"
transforms:
  files: ["transform.vrl"]
"#;
        std::fs::write(&path, yaml).unwrap();

        let app = parse(&["run"]);
        let config = app.load_config(Some(path.to_str().unwrap())).unwrap();
        assert_eq!(config.pipeline.name, "test-pipeline");
        assert_eq!(config.pipeline.batch_size, 100);
    }

    #[test]
    fn handle_emit_command_dockerfile_returns_some() {
        let app = parse(&["emit-dockerfile"]);
        // Note: this prints to stdout, but that's fine for tests.
        // We just verify the code path executes and returns Some(()).
        let result = handle_emit_command(&app);
        assert_eq!(result, Some(()));
    }

    #[test]
    fn handle_emit_command_compose_returns_some() {
        let app = parse(&["emit-compose"]);
        let result = handle_emit_command(&app);
        assert_eq!(result, Some(()));
    }

    #[test]
    fn handle_emit_command_contract_returns_some() {
        let app = parse(&["emit-contract"]);
        let result = handle_emit_command(&app);
        assert_eq!(result, Some(()));
    }

    #[test]
    fn handle_emit_command_chart_generates_files() {
        let dir = tempfile::tempdir().unwrap();
        let app = parse(&["emit-chart", dir.path().to_str().unwrap()]);
        let result = handle_emit_command(&app);
        assert_eq!(result, Some(()));
        // Verify chart dir is non-empty
        let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(!entries.is_empty(), "chart dir should contain files");
    }

    #[test]
    fn handle_emit_command_returns_none_for_non_emit() {
        let app = parse(&["run"]);
        assert_eq!(handle_emit_command(&app), None);
        let app = parse(&["version"]);
        assert_eq!(handle_emit_command(&app), None);
        let app = parse(&["config-check"]);
        assert_eq!(handle_emit_command(&app), None);
    }

    #[test]
    fn handle_emit_command_no_subcommand_returns_none() {
        let app = parse(&[]);
        assert_eq!(handle_emit_command(&app), None);
    }
}
