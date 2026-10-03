// Project:   dfe-transform-vrl
// File:      src/cli.rs
// Purpose:   CLI definition and service orchestrator
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CLI definition and service lifecycle orchestrator.
//!
//! Implements the `ServiceApp` trait from scalo, wiring together config
//! loading, VRL compilation, the metrics server that also serves the probes,
//! pipeline, and graceful shutdown.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use clap::{Parser, Subcommand};
use scalo::cli::{CliError, CommonArgs, ServiceApp, StandardCommand, VersionInfo};
use scalo::config::reloader::{ConfigReloader, ReloaderConfig};
use scalo::config::shared::SharedConfig;
use scalo::deployment::{generate_chart, generate_compose_fragment, generate_dockerfile};
use scalo::scaling::ScalingComponent;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::config::hot::HotConfig;
use crate::engine::{budget, compiler};
use crate::{deployment, metrics, pipeline};

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
    /// Standard scalo commands (run, version, config-check, generate-artefacts, metrics-manifest).
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

impl ServiceApp for App {
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
        // Seed scalo's cascade. While `try_get()` is None every runtime
        // `from_cascade()` -- metrics.address, logger.*, scaling.*,
        // worker_pool.*, batch_processing.*, self_regulation.*,
        // version_check.* -- resolves to its hard-coded default, and no env
        // var moves it. Must run before the logger and the ServiceRuntime.
        if let Err(e) = scalo::config::setup(self.common.to_config_options(self.env_prefix())) {
            // The cascade is a OnceLock; a second load keeps the first seed.
            debug!(error = %e, "scalo config cascade already seeded");
        }

        let config =
            Config::load(path).map_err(|e| CliError::Config(format!("failed to load: {e}")))?;
        config
            .validate()
            .map_err(|e| CliError::Config(format!("validation failed: {e}")))?;
        Ok(config)
    }

    fn work_state(&self, config: &Config) -> scalo::lifecycle::WorkState {
        config.work_state()
    }

    async fn run_service(
        &self,
        config: Config,
        runtime: scalo::cli::ServiceRuntime,
    ) -> Result<(), CliError> {
        run_transform_service(config, self.common.config.clone(), runtime)
            .await
            .map_err(|e| CliError::Service(e.to_string()))
    }

    fn scaling_components(&self, _config: &Config) -> Vec<ScalingComponent> {
        // Register this app's weighted KEDA components on the runtime's unified
        // `ScalingPressure` (the engine served at `/scaling/pressure`). The
        // pipeline's per-pod ticker drives `kafka_lag`; the worker-pool scaler
        // drives `worker_pool_saturation` (set_component is a no-op for an
        // unregistered name, so both MUST be registered here to count).
        // `memory` is the never-OOM HARD gate, fed via `set_memory`. The lag
        // term is the dominant KEDA driver (consumer-group lag); the pool
        // saturation term reflects CPU-bound transform pressure.
        vec![
            ScalingComponent::new("kafka_lag", 0.70, 100_000.0),
            ScalingComponent::new("worker_pool_saturation", 0.30, 1.0),
        ]
    }

    fn register_metrics(&self, manager: &scalo::metrics::MetricsManager) {
        // `metrics-manifest` and `generate-artefacts` read the registry without
        // starting the service, so the catalogue stays empty unless the
        // transform's metrics are built against the manager they hand in.
        let _ = metrics::TransformMetrics::new(
            manager,
            env!("CARGO_PKG_VERSION"),
            option_env!("GIT_COMMIT").unwrap_or("unknown"),
        );
    }

    fn deployment_contract(&self) -> Option<scalo::deployment::DeploymentContract> {
        Some(crate::deployment::contract())
    }

    fn version_check_defaults(&self) -> scalo::version_check::VersionCheckConfig {
        // The runtime overlays the version_check cascade keys on this, so a
        // deployment's explicit enabled: false always wins.
        scalo::version_check::VersionCheckConfig {
            api_url: "https://releases.hyperi.io/api/v1/check".into(),
            ..Default::default()
        }
    }
}

/// Handle emit subcommands that bypass the normal `ServiceApp` lifecycle.
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
    mut runtime: scalo::cli::ServiceRuntime,
) -> anyhow::Result<()> {
    info!(
        pipeline = %config.pipeline.name,
        version = env!("CARGO_PKG_VERSION"),
        "starting dfe-transform-vrl"
    );

    // Say so for every dial this deployment has turned that reaches nothing.
    config.warn_inert_settings();
    crate::config::warn_unreachable_scalo_settings(config_path.as_deref());

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
        dfe_source = ?derived_source.as_ref().map(scalo::KafkaSource::name),
        dfe_input_topic = ?derived_source.as_ref().map(scalo::KafkaSource::input_topic),
        dfe_output_topic = ?derived_source.as_ref().map(scalo::KafkaSource::output_topic),
        "DFE topology"
    );

    debug!(
        brokers = ?config.source.brokers,
        group_id = %config.source.group_id,
        topics = ?config.source.topics,
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

    // Compiling runs before anything back-pressures it, so too small a limit
    // has to be a refusal here rather than an exit-137 restart loop.
    budget::check_compile_budget(vrl_source.len() as u64, scalo::detect_memory_limit())
        .map_err(|e| anyhow::anyhow!("{e}"))?;

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
    budget::log_compiled(vrl_source.len() as u64);

    // Shutdown coordination. The runtime installs the signal handler and
    // cancels `runtime.shutdown` on SIGTERM (K8s) / SIGINT (Ctrl+C). That token
    // is the single source of truth: the engine driver stops on cancel, and we
    // bridge it to a local `watch` channel for the enrichment refresh tasks,
    // which still take a watch receiver -- no second signal handler.
    let shutdown_token = runtime.shutdown.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let bridge_token = shutdown_token.clone();
        tokio::spawn(async move {
            bridge_token.cancelled().await;
            let _ = shutdown_tx.send(true);
        });
    }

    // Memory guard — the runtime's shared, cgroup-aware guard. It is the SAME
    // guard that feeds the self-regulation governor (the inbound pause-partitions
    // brake) and the worker pool, so accounting is unified. No stand-alone guard.
    let memory_guard = Arc::clone(&runtime.memory_guard);
    info!(
        limit_bytes = memory_guard.limit_bytes(),
        "memory guard initialised (runtime shared)"
    );

    // Readiness flag. The probes are served by the runtime's metrics server on
    // the metrics port, which is what the chart probes; this service starts no
    // second HTTP listener for them.
    let ready_flag = Arc::new(AtomicBool::new(false));

    // Metrics: reuse the MetricsManager that scalo's ServiceApp framework has
    // already constructed and started for us (`runtime.metrics`). NEVER
    // construct a new MetricsManager here — that creates a second server on
    // the same port and the wrapper crashes with EADDRINUSE on startup (GH
    // issue #11). The framework already exposes everything we register on
    // the global recorder; we just wire our app-specific metrics + readiness
    // into the running manager.
    //
    // The listener is already bound by then, on the address `load_config` fed
    // the cascade: `--metrics-addr`/`METRICS_ADDR` first, else `metrics.address`
    // from the config file, else `0.0.0.0:9090`.
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
    info!("readiness check wired into scalo metrics server");

    // Reloadable config subset. Every field in it is on `INERT_SETTINGS`, so a
    // reload re-validates the file and changes no pipeline behaviour. Logged at
    // debug: `warn_inert_settings` above is what an operator needs to see.
    let hot_config = SharedConfig::new(HotConfig::from_config(&config));
    debug!(
        batch_size = config.pipeline.batch_size,
        batch_timeout_ms = config.pipeline.batch_timeout_ms,
        key_field = %config.sink.key_field,
        "reloadable config subset initialised"
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
            scalo::logger::security::config_changed(
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

    // Batch engine: the runtime built it (governed byte-budget lever already
    // wired when self-regulation is on). The mid-tier transform requires it.
    let engine = runtime
        .batch_engine
        .clone()
        .ok_or_else(|| anyhow::anyhow!("batch engine unavailable (worker pool not configured)"))?;

    // Self-regulation governor (default-ON, opt-out). Cloned into the pipeline
    // so the Kafka pause-partitions inbound gate can be attached to the consumer.
    // The governor owns the inbound brake + AIMD byte budget (already wired into
    // the batch engine by ServiceRuntime) over the shared memory guard.
    let governor = runtime.governor.clone();
    let worker_pool = runtime.worker_pool.clone();

    // Unified scaling-pressure engine (scalo 2.9). The runtime built ONE
    // `ScalingPressure` from `ScalingPressureConfig::from_cascade()` + the
    // components this app registered via `scaling_components`, and already wired
    // it into the MetricsManager (served at `/scaling/pressure` to KEDA) and the
    // worker pool (which feeds `worker_pool_saturation`). The pipeline's per-pod
    // ticker feeds the `kafka_lag` component + the outbound circuit latch + the
    // memory HARD gate. This collapses the old dual-engine model (the separate
    // runtime `ScalingSignalsCell` is gone). `None` when `scaling.enabled =
    // false` -- the ticker is then not spawned.
    let scaling = runtime.scaling.clone();

    // Stops on `shutdown_token`; aborted below as well, for a pipeline that
    // fails without a shutdown.
    let memory_gauge_task = metrics::spawn_memory_gauge_task(
        Arc::clone(&transform_metrics),
        Arc::clone(&memory_guard),
        shutdown_token.clone(),
    );

    // Pipeline -- runs the governed engine driver until `shutdown_token` is
    // cancelled (the driver returns cleanly on cancel). `run_service` is awaited
    // by `run_app`, so this blocks here for the process lifetime; no separate
    // spawn + signal-wait is needed (the runtime owns signal handling).
    let result = pipeline::run(
        &config,
        program,
        hot_config.clone(),
        Arc::clone(&transform_metrics),
        ready_flag,
        shutdown_token,
        worker_pool,
        engine,
        governor,
        scaling,
        memory_guard,
    )
    .await;
    memory_gauge_task.abort();

    match result {
        Ok(()) => info!("pipeline shutdown complete"),
        Err(ref e) => error!(error = %e, "pipeline shutdown with error"),
    }

    info!("shutdown complete");
    result.map_err(|e| anyhow::anyhow!("pipeline failed: {e}"))
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
        assert_ne!(version.version, "");
    }

    #[test]
    fn dfe_app_common_args_accessible() {
        let app = parse(&["run"]);
        // Just verifies the accessor compiles and returns a reference.
        let _args = app.common_args();
    }

    /// `metrics-manifest` builds an offline manager, calls `register_metrics`
    /// and prints the registry, so an app that leaves scalo's no-op default in
    /// place prints an empty catalogue.
    #[test]
    fn register_metrics_fills_the_manifest() {
        let app = parse(&["metrics-manifest"]);
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline(app.name()),
        );

        app.register_metrics(&manager);

        let names: Vec<String> = manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .map(|m| m.name)
            .collect();
        assert!(!names.is_empty(), "metrics-manifest catalogue is empty");
        // Suffix match: the manifest's namespace prefix is scalo's to settle.
        assert!(
            names
                .iter()
                .any(|n| n.ends_with("execute_duration_seconds")),
            "metrics-manifest catalogue is missing the transform's own metrics: {names:?}"
        );
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
