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
    CONTRACT_SCHEMA_VERSION, DeploymentContract, HealthContract, ImageProfile, KafkaLagTrigger,
    KedaConfig, KedaContract, NativeDepsContract, PortContract, ResourceList, ResourcesContract,
    SecretEnvContract, SecretGroupContract, SecurityContract, base_image_from_cascade,
};

/// The `source.transport` values that bind the Push listener: the name
/// `Transport::Direct` serialises to, and the alias dfe-engine renders.
const PUSH_TRANSPORTS: [&str; 2] = ["direct", "grpc"];

/// The chart description and the OCI image description, kept as one string so
/// the two cannot drift.
const DESCRIPTION: &str = "Embedded VRL transform engine -- Kafka-to-Kafka pipelines";

/// Build the deployment contract for dfe-transform-vrl.
#[must_use]
pub fn contract() -> DeploymentContract {
    // One cascade-resolved base image drives BOTH the runtime FROM and the
    // native-deps codename (so the Confluent librdkafka repo matches the base).
    let base_image = base_image_from_cascade();
    DeploymentContract {
        app_name: "dfe-transform-vrl".into(),
        binary_name: "dfe-transform-vrl".into(),
        description: DESCRIPTION.into(),
        metrics_port: 9090,
        health: HealthContract {
            startup_budget_seconds: 120,
            ..HealthContract::default()
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
                .bound_from("source.listen")
                .app_protocol("kubernetes.io/h2c"),
        ],
        unbound_listen_paths: vec![],
        entrypoint_args: vec![
            "--config".into(),
            "/etc/dfe-transform-vrl/config.yaml".into(),
        ],
        secrets: secrets(),
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
            // No `metrics`, `logger` or `scaling` section: scalo's own defaults
            // apply without one, and `config_schema` below describes `Config`
            // alone. A deployment moves one in the rendered file or the env
            // layer -- see `config::SCALO_CASCADE_SECTIONS`. Readiness is served
            // by the metrics listener, so there is no `health` section either.
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
        schema_version: CONTRACT_SCHEMA_VERSION,
        // scalo writes no vendor, licence or copyright of its own, so the labels
        // and the generated Dockerfile header carry exactly these.
        oci_labels: scalo::deployment::OciLabels {
            title: "dfe-transform-vrl".into(),
            description: DESCRIPTION.into(),
            vendor: "HYPERI PTY LIMITED".into(),
            label_namespace: "io.hyperi".into(),
            licenses: "BUSL-1.1".into(),
            copyright: "(c) 2026 HYPERI PTY LIMITED".into(),
        },
        // Reflectable config (scalo-rs#6): derived JSON Schema of the full
        // Config + the capability catalog (the VRL transform + the enrichment-
        // table source types the schema cannot self-describe as friendly forms).
        config_schema: Some(scalo::deployment::config_schema_json::<crate::config::Config>()),
        capabilities: capabilities(),
        // The app writes no file at run time, so the root filesystem stays read-only.
        writable_paths: vec![],
        termination_grace_seconds: 45,
        resources: ResourcesContract {
            requests: ResourceList {
                cpu: "100m".into(),
                memory: "128Mi".into(),
            },
            limits: ResourceList {
                cpu: "500m".into(),
                memory: "512Mi".into(),
            },
        },
        security: SecurityContract::default(),
        singleton: false,
    }
}

/// The Kafka Secret the chart mounts into each endpoint's SASL env vars.
///
/// `Config::apply_flat_env` reads these per-endpoint names. Each `key_name` is
/// distinct because the chart renders it as a values key.
fn secrets() -> Vec<SecretGroupContract> {
    let env = |env_var: &str, key_name: &str, secret_key: &str| SecretEnvContract {
        env_var: env_var.into(),
        key_name: key_name.into(),
        secret_key: secret_key.into(),
    };
    vec![SecretGroupContract::new(
        "kafka",
        vec![
            env(
                "DFE_TRANSFORM_SOURCE_SASL_USERNAME",
                "source-username",
                "username",
            ),
            env(
                "DFE_TRANSFORM_SOURCE_SASL_PASSWORD",
                "source-password",
                "password",
            ),
            env(
                "DFE_TRANSFORM_SINK_SASL_USERNAME",
                "sink-username",
                "username",
            ),
            env(
                "DFE_TRANSFORM_SINK_SASL_PASSWORD",
                "sink-password",
                "password",
            ),
        ],
    )]
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

    /// The OCI title and description feed the image labels and the registry
    /// package page, and scalo leaves both empty unless the app sets them.
    #[test]
    fn test_oci_title_and_description_are_set() {
        let c = contract();
        assert_eq!(c.oci_labels.title, c.app_name);
        assert_eq!(c.oci_labels.description, c.description);
        assert_ne!(c.oci_labels.description, "");
    }

    #[test]
    fn test_contract_carries_reflectable_config() {
        let c = contract();
        assert!(c.config_schema.is_some());
        assert_eq!(c.schema_version, CONTRACT_SCHEMA_VERSION);
        assert_ne!(c.capabilities, [] as [scalo::Capability; 0]);
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

    /// Every enrichment source the catalogue offers is compiled into the build
    /// that ships and the build that is tested, so a table the console offers
    /// cannot stop the transform at startup.
    #[test]
    fn every_catalogued_enrichment_source_is_in_the_shipped_build() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".hyperi-ci.yaml");
        let ci: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&std::fs::read_to_string(&path).expect("read .hyperi-ci.yaml"))
                .expect(".hyperi-ci.yaml parses");
        let features = |stage: &str| -> Vec<String> {
            ci[stage]["rust"]["features"]
                .as_str()
                .unwrap_or_else(|| panic!("{stage}.rust.features is one comma-separated string"))
                .split(',')
                .map(|f| f.trim().to_string())
                .collect()
        };
        let (build, test) = (features("build"), features("test"));

        let enrichment = contract()
            .capabilities
            .into_iter()
            .find(|cap| cap.name == "enrichment")
            .expect("enrichment capability");
        for source in &enrichment.children {
            let feature = match source.name.as_str() {
                "file" | "stix" => continue,
                "mmdb" => "enrichment-mmdb",
                "sqlite" => "enrichment-sqlite",
                other => panic!("the catalogue offers `{other}`, which maps to no known feature"),
            };
            for (stage, set) in [("build", &build), ("test", &test)] {
                assert!(
                    set.iter().any(|f| f == feature),
                    "the catalogue offers `{}` but {stage}.rust.features lacks `{feature}`: {set:?}",
                    source.name
                );
            }
        }
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
        // The Push listener is cleartext gRPC, so a proxy in front of it must speak h2c.
        assert_eq!(c.extra_ports[0].app_protocol, "kubernetes.io/h2c");
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
        assert!(!c.secrets[0].optional);
        assert_eq!(c.secrets[0].env_vars.len(), 4);
    }

    /// The chart mounts a Secret under every declared name, so a name the
    /// config never reads leaves the credential silently unused.
    ///
    /// Each name spells the field it fills -- `DFE_TRANSFORM_SINK_SASL_PASSWORD`
    /// is `sink.sasl.password` -- so a name read into the other endpoint fails
    /// here too.
    #[test]
    fn test_every_contract_secret_env_var_reaches_the_config() {
        use scalo::config::flat_env::ApplyFlatEnv;

        let c = contract();
        let prefix = c.env_prefix.clone();

        for group in &c.secrets {
            for env in &group.env_vars {
                let field = env
                    .env_var
                    .strip_prefix(&format!("{prefix}_"))
                    .unwrap_or_else(|| panic!("{} lacks the {prefix} prefix", env.env_var));
                let pointer = format!("/{}", field.to_ascii_lowercase().replace('_', "/"));

                let sentinel = format!("sentinel-{}", env.key_name);
                let config = temp_env::with_var(&env.env_var, Some(&sentinel), || {
                    let mut config = crate::config::Config::default();
                    config.apply_flat_env(&prefix);
                    config
                });

                // A credential field redacts on every other serialise path.
                let applied = scalo::expose_during(|| {
                    serde_json::to_value(&config).expect("config serialises")
                });
                let reached = applied
                    .pointer(&pointer)
                    .and_then(serde_json::Value::as_str)
                    == Some(sentinel.as_str());
                // The message carries the env var and group names only, never a value.
                assert!(
                    reached,
                    "{} ({}) was set and the config field its name spells did not read it",
                    env.env_var, group.group_name
                );
            }
        }
    }

    #[test]
    fn test_contract_default_config_present() {
        let c = contract();
        let cfg = c.default_config.unwrap();
        assert!(cfg.get("pipeline").is_some());
        assert!(cfg.get("source").is_some());
        assert!(cfg.get("sink").is_some());
    }

    /// The shipped `--config` carries no section only scalo's cascade reads.
    ///
    /// Such a block would pin a copy of scalo's own default, and the contract's
    /// `config_schema` describes `Config` alone.
    #[test]
    fn test_default_config_carries_no_scalo_cascade_section() {
        let cfg = contract().default_config.expect("default_config present");
        let map = cfg.as_object().expect("default_config is an object");

        for (section, instead) in crate::config::SCALO_CASCADE_SECTIONS {
            assert!(
                !map.contains_key(*section),
                "default_config ships a `{section}` section, which pins scalo's \
                 default and is absent from the config schema -- set it via {instead}"
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
                 of Config, so the wrapper does not read it"
            );
        }
    }
}
