// Project:   dfe-transform-vrl
// File:      src/deployment.rs
// Purpose:   Deployment contract for artefact generation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for Dockerfile, Helm chart, and compose fragment generation.
//!
//! Unlike dfe-transform-vector, this service does NOT need the Vector binary
//! in its container image. The container is just the Rust binary.

use scalo::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KafkaLagTrigger, KedaConfig, KedaContract,
    NativeDepsContract, PortContract, SecretEnvContract, SecretGroupContract,
    base_image_from_cascade,
};

/// The `source.transport` values that bind the Push listener: the name
/// `Transport::Direct` serialises to, and the alias dfe-engine renders.
const PUSH_TRANSPORTS: [&str; 2] = ["direct", "grpc"];

/// Build the deployment contract for dfe-transform-vrl.
#[must_use]
pub fn contract() -> DeploymentContract {
    // One cascade-resolved base image drives BOTH the runtime FROM and the
    // native-deps codename (so the Confluent librdkafka repo matches the base).
    let base_image = base_image_from_cascade();
    DeploymentContract {
        app_name: "dfe-transform-vrl".into(),
        binary_name: "dfe-transform-vrl".into(),
        description: "Embedded VRL transform engine — Kafka-to-Kafka pipelines".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/livez".into(),
            readiness_path: "/readyz".into(),
            metrics_path: "/metrics".into(),
        },
        env_prefix: "DFE_TRANSFORM".into(),
        metric_prefix: "transform_vrl".into(),
        config_mount_path: "/etc/dfe-transform-vrl/config.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        base_image: base_image.clone(),
        // The Push listener binds only on the direct transport, and the probes use the metrics port.
        extra_ports: vec![
            PortContract::tcp("push", 6000)
                .when_one_of("config.source.transport", PUSH_TRANSPORTS)
                .bound_from("source.listen"),
        ],
        unbound_listen_paths: vec![],
        entrypoint_args: vec![
            "--config".into(),
            "/etc/dfe-transform-vrl/config.yaml".into(),
        ],
        // One entry per credential -- the generator renders `key_name` into
        // values.yaml, so a second entry reusing it emits a duplicate YAML key.
        secrets: vec![SecretGroupContract {
            group_name: "kafka".into(),
            env_vars: vec![
                SecretEnvContract {
                    env_var: "DFE_TRANSFORM_KAFKA_SASL_USERNAME".into(),
                    key_name: "username".into(),
                    secret_key: "kafka-username".into(),
                },
                SecretEnvContract {
                    env_var: "DFE_TRANSFORM_KAFKA_SASL_PASSWORD".into(),
                    key_name: "password".into(),
                    secret_key: "kafka-password".into(),
                },
            ],
        }],
        default_config: Some(serde_json::json!({
            "pipeline": {
                "name": "default",
                "batch_size": 1000,
                "batch_timeout_ms": 100
            },
            "source": {
                "transport": "bus",
                "listen": "0.0.0.0:6000",
                "brokers": ["kafka:9092"],
                "topics": ["raw_events"],
                "group_id": "dfe-transform-vrl-default",
                "sasl": { "enabled": true, "mechanism": "scram_sha_512" },
                "tls": { "enabled": false },
                "acknowledgements": { "enabled": true }
            },
            "sink": {
                "transport": "bus",
                "endpoint": "http://dfe-loader:6000",
                "brokers": ["kafka:9092"],
                "topic": "enriched_events",
                "key_field": ".org_id",
                "compression": "zstd",
                "sasl": { "enabled": true, "mechanism": "scram_sha_512" },
                "tls": { "enabled": false }
            },
            // No `metrics`, `logger` or `scaling` section: those are scalo's,
            // and scalo's cascade discovers files by fixed base name, so it
            // never reads this one. Shipping them here put values in front of
            // operators that the process could not act on. They are set through
            // the env layer -- see `config::SCALO_CASCADE_SECTIONS`, and the
            // startup warning that fires when one turns up in the file anyway.
            // Readiness is served by the metrics listener, so there is no
            // `health` section either.
            "transforms": {
                "dir": "/etc/dfe-transform-vrl/transforms"
            }
        })),
        depends_on: vec!["kafka".into()],
        native_deps: NativeDepsContract::for_scalo_features(&["transport-kafka"], &base_image),
        image_profile: ImageProfile::Production,
        keda: Some(
            KedaContract::from_config(&KedaConfig {
                enabled: true,
                min_replicas: 1,
                max_replicas: 10,
                polling_interval: 15,
                cooldown_period: 300,
                cpu_enabled: true,
                cpu_threshold: 80,
                ..Default::default()
            })
            // Raw consumer-group lag rises when a downstream stage breaks, so it never scales this app.
            .with_kafka_trigger(KafkaLagTrigger::disabled()),
        ),
        schema_version: 3,
        oci_labels: scalo::deployment::OciLabels {
            licenses: "BUSL-1.1".into(),
            ..Default::default()
        },
        // Reflectable config (scalo-rs#6): derived JSON Schema of the full
        // Config + the capability catalog (the VRL transform + the enrichment-
        // table source types the schema cannot self-describe as friendly forms).
        config_schema: Some(scalo::deployment::config_schema_json::<crate::config::Config>()),
        capabilities: capabilities(),
    }
}

/// Capability catalog for dfe-transform-vrl: the VRL transform engine plus the
/// enrichment-table source types (grounded in `config::loader`). See
/// `docs/reflectable-config-shape.md` in scalo-rs for the shape.
fn capabilities() -> Vec<scalo::deployment::Capability> {
    use scalo::deployment::{Capability, FieldSpec};
    vec![
        Capability::new("transform", "vrl")
            .description("Vector Remap Language transform engine: applies ordered .vrl programs to each event.")
            .maturity("stable")
            .field(FieldSpec::string("dir").description("Directory of .vrl files, applied sorted by filename."))
            .field(FieldSpec::list("files").description("Explicit ordered list of .vrl file paths.")),
        Capability::source("enrichment")
            .description("Enrichment tables queried from VRL via get_enrichment_table_record().")
            .maturity("stable")
            .children(vec![
                Capability::service("file")
                    .description("CSV/JSON/YAML file (format auto-detected when unset).")
                    .field(FieldSpec::string("path").required().description("Path to the data file."))
                    .field(
                        FieldSpec::enumeration("format", ["csv", "json", "yaml", "auto"])
                            .description("Explicit file format (default auto)."),
                    ),
                Capability::service("mmdb")
                    .description("MaxMind DB (e.g. GeoIP) lookups.")
                    .field(FieldSpec::string("path").required().description("Path to the .mmdb file.")),
                Capability::service("stix")
                    .description("STIX/TAXII threat-intel indicators (file or TAXII URL).")
                    .field(FieldSpec::string("path").description("Local STIX bundle path."))
                    .field(FieldSpec::string("url").description("TAXII collection URL."))
                    .field(FieldSpec::string("collection").description("TAXII collection id."))
                    .field(FieldSpec::string("auth").description("Auth spec (type + env-var refs for bearer/basic/api_key).")),
                Capability::service("sqlite")
                    .description("SQLite query result materialised as a lookup table.")
                    .field(FieldSpec::string("path").required().description("Path to the SQLite database."))
                    .field(FieldSpec::string("query").required().description("SELECT query; first column is the lookup key.")),
            ]),
    ]
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_identity() {
        let c = contract();
        assert_eq!(c.app_name, "dfe-transform-vrl");
        assert_eq!(c.binary_name, "dfe-transform-vrl");
        assert_eq!(c.env_prefix, "DFE_TRANSFORM");
        assert_eq!(c.metric_prefix, "transform_vrl");
    }

    #[test]
    fn test_contract_carries_reflectable_config() {
        let c = contract();
        assert!(c.config_schema.is_some());
        assert_eq!(c.schema_version, 3);
        assert!(!c.capabilities.is_empty());
        // The VRL transform capability + the enrichment source family.
        assert!(c.capabilities.iter().any(|cap| cap.name == "vrl"));
        let enrich = c
            .capabilities
            .iter()
            .find(|cap| cap.name == "enrichment")
            .expect("enrichment capability");
        let kinds: Vec<&str> = enrich.children.iter().map(|s| s.name.as_str()).collect();
        assert!(kinds.contains(&"stix") && kinds.contains(&"sqlite"));
    }

    /// The committed reflectable artefacts under docs/ must not drift from a
    /// fresh regen. Regenerate with `dfe-transform-vrl config-schema --dir docs`.
    #[test]
    fn test_config_artifacts_do_not_drift() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs");
        scalo::deployment::assert_no_config_artifact_drift(&contract(), dir);
    }

    #[test]
    fn test_contract_health_paths() {
        let c = contract();
        assert_eq!(c.health.liveness_path, "/livez");
        assert_eq!(c.health.readiness_path, "/readyz");
        assert_eq!(c.health.metrics_path, "/metrics");
    }

    #[test]
    fn test_contract_ports() {
        let c = contract();
        assert_eq!(c.metrics_port, 9090);
        assert_eq!(c.extra_ports.len(), 1);
        assert_eq!(c.extra_ports[0].port, 6000);
        assert_eq!(c.extra_ports[0].name, "push");
        assert_eq!(c.extra_ports[0].protocol, "TCP");
        assert_eq!(
            c.extra_ports[0].bound_from.as_deref(),
            Some("source.listen")
        );
    }

    /// The gate compares the chart's string of `source.transport`, so it has to
    /// hold for every name the config reads as `Transport::Direct` and for no
    /// other.
    #[test]
    fn test_push_port_listens_only_on_the_direct_transport() {
        let c = contract();
        let gate = c.extra_ports[0].when.as_ref().expect("push port is gated");
        let on = |name: &str| {
            let mut config = c.default_config.clone().expect("default_config present");
            config["source"]["transport"] = serde_json::Value::from(name);
            gate.holds_in(&config)
        };

        for name in ["direct", "grpc", "bus", "kafka"] {
            let transport: crate::config::Transport =
                serde_json::from_value(serde_json::Value::from(name)).expect("a transport name");
            assert_eq!(on(name), Some(transport.is_direct()), "transport {name}");
        }
        assert_eq!(on("tcp"), Some(false));
        assert_eq!(
            gate.holds_in(c.default_config.as_ref().expect("default_config present")),
            Some(false),
            "the shipped default is the bus transport, which binds no Push listener"
        );
    }

    /// `generate-artefacts` and `generate_chart` write nothing for a contract
    /// that fails either check.
    #[test]
    fn test_contract_passes_the_generate_artefacts_checks() {
        let c = contract();
        c.validate()
            .expect("every generator must accept the contract");
        scalo::deployment::assert_listeners_declared(&c);
        assert!(
            c.unresolved_values_paths().is_empty(),
            "the chart reads values default_config never sets: {:?}",
            c.unresolved_values_paths()
        );
    }

    /// GH issue #10 regression: config mount path must follow the
    /// `/etc/<component-name>/config.yaml` convention used by every other
    /// DFE component (loader, receiver, fetcher, archiver). The previous
    /// path `/etc/dfe/config.yaml` forced dfe-docker compose authors to
    /// special-case this one service.
    #[test]
    fn test_contract_config_mount_path_follows_dfe_convention() {
        let c = contract();
        assert_eq!(
            c.config_mount_path, "/etc/dfe-transform-vrl/config.yaml",
            "config mount path must be /etc/<component>/config.yaml — matches \
             loader/receiver/fetcher/archiver convention"
        );
        // The wrapper's --config arg must point at the same path.
        let config_arg_idx = c
            .entrypoint_args
            .iter()
            .position(|a| a == "--config")
            .expect("entrypoint_args must contain --config");
        let config_arg_value = c
            .entrypoint_args
            .get(config_arg_idx + 1)
            .expect("--config must have a path arg after it");
        assert_eq!(
            config_arg_value, "/etc/dfe-transform-vrl/config.yaml",
            "--config arg must match config_mount_path; drift between the two \
             will surface as 'config file not found' (GH#9) at startup"
        );
    }

    /// KEDA stays on and scales on CPU alone, because raw consumer-group lag
    /// must never reach the `ScaledObject`.
    #[test]
    fn test_contract_keda_scales_on_cpu_without_a_lag_trigger() {
        let c = contract();
        let keda = c.keda.as_ref().unwrap();
        assert!(keda.enabled);
        assert!(
            !keda.kafka_trigger.enabled,
            "a consumer-lag trigger scales out when a downstream stage is broken"
        );
        assert!(keda.cpu_enabled);
        assert_eq!(keda.cpu_threshold, 80);
        assert_eq!(keda.min_replicas, 1);
        assert_eq!(keda.max_replicas, 10);
        assert_eq!(keda.polling_interval, 15);
        assert_eq!(keda.cooldown_period, 300);
    }

    #[test]
    fn test_contract_secrets() {
        let c = contract();
        assert_eq!(c.secrets.len(), 1);
        assert_eq!(c.secrets[0].group_name, "kafka");
        assert_eq!(c.secrets[0].env_vars.len(), 2);
    }

    /// The generated chart injects each `SecretEnvContract.env_var` verbatim,
    /// so a name the config cascade never reads mounts the Secret and drops it.
    #[test]
    fn test_every_contract_secret_env_var_reaches_the_config() {
        use scalo::config::flat_env::ApplyFlatEnv;

        let c = contract();
        let prefix = c.env_prefix.clone();

        for group in &c.secrets {
            for env in &group.env_vars {
                assert!(
                    env.env_var.starts_with(&format!("{prefix}_")),
                    "{} must carry the {prefix} prefix the config cascade reads",
                    env.env_var
                );

                let sentinel = format!("sentinel-{}", env.key_name);
                let config = temp_env::with_var(&env.env_var, Some(&sentinel), || {
                    let mut config = crate::config::Config::default();
                    config.apply_flat_env(&prefix);
                    config
                });

                // A credential field redacts on every other serialise path, so
                // the walk has to expose to see where the value landed.
                let rendered = scalo::expose_during(|| {
                    serde_json::to_string(&config).expect("config serialises")
                });
                assert!(
                    rendered.contains(&sentinel),
                    "{} is injected by the chart but never lands in the config",
                    env.env_var
                );
            }
        }
    }

    /// `chart/` is `emit-chart` output, so a hand edit there is reverted by the
    /// next regen -- which is how the Kafka SASL env names shipped broken. A
    /// hand fix the generator cannot yet make goes in as a pinned `ChartPatch`,
    /// never as an exempt file.
    #[test]
    fn test_committed_chart_matches_the_generator() {
        let chart = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("chart");
        scalo::deployment::assert_no_chart_drift(&contract(), &chart, &[]);
    }

    /// True when this path answers `helm version`.
    fn helm_runs(bin: &std::path::Path) -> bool {
        std::process::Command::new(bin)
            .arg("version")
            .output()
            .is_ok_and(|out| out.status.success())
    }

    /// Download a pinned helm into the gitignored cache and return its path.
    ///
    /// The script's own progress lines are replayed so a cold fetch is visible
    /// in the test output.
    fn fetch_helm() -> Result<std::path::PathBuf, String> {
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/fetch-helm.sh");
        let out = std::process::Command::new("bash")
            .arg(&script)
            .output()
            .map_err(|err| format!("{} did not run: {err}", script.display()))?;
        let log = String::from_utf8_lossy(&out.stderr);
        if !out.status.success() {
            return Err(format!("{} failed:\n{log}", script.display()));
        }
        eprint!("{log}");

        let stdout = String::from_utf8_lossy(&out.stdout);
        let printed = stdout
            .trim_end()
            .lines()
            .next_back()
            .ok_or_else(|| format!("{} printed no helm path", script.display()))?;
        let bin = std::path::PathBuf::from(printed);
        if !helm_runs(&bin) {
            return Err(format!("{} is not a working helm", bin.display()));
        }
        Ok(bin)
    }

    /// A usable helm, or the reason this host has none.
    ///
    /// The render check below is the only proof the chart's `ScaledObject`
    /// carries no lag trigger, so a runner without helm fetches one instead of
    /// letting the gate disappear with its environment.
    fn helm_binary() -> Result<&'static std::path::PathBuf, &'static str> {
        static HELM: std::sync::OnceLock<Result<std::path::PathBuf, String>> =
            std::sync::OnceLock::new();
        HELM.get_or_init(|| {
            if helm_runs(std::path::Path::new("helm")) {
                return Ok(std::path::PathBuf::from("helm"));
            }
            fetch_helm()
        })
        .as_ref()
        .map_err(String::as_str)
    }

    /// Render the committed chart under `--set` overrides, returning helm's
    /// stderr when the render is refused.
    fn render_chart(helm_bin: &std::path::Path, overrides: &[&str]) -> Result<String, String> {
        let chart = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("chart");
        let mut helm = std::process::Command::new(helm_bin);
        helm.arg("template").arg("guard").arg(&chart);
        for set in overrides {
            helm.arg("--set").arg(set);
        }
        let out = helm.output().expect("helm runs");
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).into_owned())
        }
    }

    /// A usable helm, or `None` off CI when none could be had.
    fn helm_or_skip() -> Option<&'static std::path::PathBuf> {
        match helm_binary() {
            Ok(bin) => Some(bin),
            Err(reason) => {
                assert!(
                    std::env::var_os("CI").is_none(),
                    "helm is missing on a CI runner and could not be fetched, so \
                     the chart render goes unchecked: {reason}"
                );
                eprintln!("skipping chart render checks: {reason}");
                None
            }
        }
    }

    /// Consumer-group lag rises when a downstream stage breaks and scaling out
    /// fixes nothing, so the rendered `ScaledObject` carries the CPU trigger alone.
    #[test]
    fn test_keda_scaled_object_carries_no_lag_trigger() {
        let Some(helm_bin) = helm_or_skip() else {
            return;
        };

        let rendered = render_chart(helm_bin, &[])
            .unwrap_or_else(|err| panic!("helm template failed:\n{err}"));
        assert!(
            rendered.contains("kind: ScaledObject"),
            "KEDA is on by default, so the ScaledObject must render:\n{rendered}"
        );
        assert!(
            !rendered.contains("type: kafka"),
            "the ScaledObject must carry no consumer-lag trigger:\n{rendered}"
        );
        assert!(
            !rendered.contains("kind: TriggerAuthentication"),
            "with no lag trigger there is nothing for a TriggerAuthentication to serve:\n{rendered}"
        );
        assert!(
            rendered.contains("type: cpu") && rendered.contains("value: \"80\""),
            "the CPU trigger must render at the contract's 80% threshold:\n{rendered}"
        );
    }

    /// The Service and Deployment publish the Push port only where the app
    /// binds it, which is the direct transport under either of its names --
    /// dfe-engine renders `grpc`.
    #[test]
    fn test_push_port_renders_only_on_the_direct_transport() {
        let Some(helm_bin) = helm_or_skip() else {
            return;
        };

        let bus = render_chart(helm_bin, &[])
            .unwrap_or_else(|err| panic!("helm template failed:\n{err}"));
        assert!(
            !bus.contains("containerPort: 6000") && !bus.contains("port: 6000"),
            "the bus transport binds no Push listener, so no port 6000 may render:\n{bus}"
        );

        for name in ["direct", "grpc"] {
            let set = format!("config.source.transport={name}");
            let direct = render_chart(helm_bin, &[&set])
                .unwrap_or_else(|err| panic!("helm template failed:\n{err}"));
            assert!(
                direct.contains("containerPort: 6000") && direct.contains("port: 6000"),
                "transport {name} binds the Push listener, so port 6000 must render:\n{direct}"
            );
        }

        let kafka = render_chart(helm_bin, &["config.source.transport=kafka"])
            .unwrap_or_else(|err| panic!("helm template failed:\n{err}"));
        assert!(
            !kafka.contains("containerPort: 6000") && !kafka.contains("port: 6000"),
            "transport kafka is the bus, so no port 6000 may render:\n{kafka}"
        );
    }

    #[test]
    fn test_contract_default_config_present() {
        let c = contract();
        let cfg = c.default_config.unwrap();
        assert!(cfg.get("pipeline").is_some());
        assert!(cfg.get("source").is_some());
        assert!(cfg.get("sink").is_some());
    }

    /// The shipped `--config` must carry no section that only scalo's cascade
    /// could read.
    ///
    /// `scalo::config` finds files by fixed base name (`settings.yaml`,
    /// `defaults.yaml`, `settings.{env}.yaml`), so it never reads the
    /// `config.yaml` this contract mounts. A `scaling:` or `metrics:` block in
    /// here parses, renders into the `ConfigMap`, and changes nothing, so
    /// asserting that such a block deserialises into scalo's type proves only
    /// its shape -- this asserts it is not shipped at all.
    #[test]
    fn test_default_config_carries_no_scalo_cascade_section() {
        let cfg = contract().default_config.expect("default_config present");
        let map = cfg.as_object().expect("default_config is an object");

        for (section, instead) in crate::config::SCALO_CASCADE_SECTIONS {
            assert!(
                !map.contains_key(*section),
                "default_config ships a `{section}` section, which scalo's \
                 cascade cannot read from this file -- set it via {instead}"
            );
        }
    }

    /// Every section the contract DOES ship must be one the wrapper reads,
    /// which is to say a field of `Config`.
    #[test]
    fn test_default_config_sections_are_all_read_by_the_wrapper() {
        let cfg = contract().default_config.expect("default_config present");
        let map = cfg.as_object().expect("default_config is an object");

        let known = serde_json::to_value(crate::config::Config::default())
            .expect("Config serialises")
            .as_object()
            .expect("Config is an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        for section in map.keys() {
            assert!(
                known.contains(section),
                "default_config ships a `{section}` section that is not a field \
                 of Config, so nothing deserialises it"
            );
        }
    }
}
