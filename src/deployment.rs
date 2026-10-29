// Project:   dfe-transform-vrl
// File:      src/deployment.rs
// Purpose:   Deployment contract for artefact generation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for Dockerfile and compose fragment generation, and
//! for the thin chart the release assembles.
//!
//! Unlike dfe-transform-vector, this service does NOT need the Vector binary
//! in its container image. The container is just the Rust binary.

use scalo::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KafkaLagTrigger, KedaConfig, KedaContract,
    NativeDepsContract, PortContract, ResourceList, ResourcesContract, SecretEnvContract,
    SecretGroupContract, base_image_from_cascade,
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
        hot_paths: vec![],
        entrypoint_args: vec![
            "--config".into(),
            "/etc/dfe-transform-vrl/config.yaml".into(),
        ],
        secrets: secrets(),
        default_config: default_config(),
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
        ..DeploymentContract::default()
    }
}

/// The config keys the contract advertises as the deployment default, read
/// from `Config::default()` so the two cannot drift.
const DEFAULT_CONFIG_KEYS: [&str; 12] = [
    "/pipeline/name",
    "/pipeline/batch_size",
    "/pipeline/batch_timeout_ms",
    "/source/transport",
    "/source/listen",
    "/source/tls/enabled",
    "/source/acknowledgements/enabled",
    "/sink/transport",
    "/sink/endpoint",
    "/sink/key_field",
    "/sink/compression",
    "/sink/tls/enabled",
];

/// The values a deployment decides, which the app has no default for: the
/// in-cluster broker and topics, the SCRAM credential the chart's Secret
/// fills in, and the directory the chart mounts the transforms at.
fn deployment_values() -> [(&'static str, serde_json::Value); 10] {
    use serde_json::json;
    [
        ("/source/brokers", json!(["kafka:9092"])),
        ("/source/topics", json!(["raw_events"])),
        ("/source/group_id", json!("dfe-transform-vrl-default")),
        ("/source/sasl/enabled", json!(true)),
        ("/source/sasl/mechanism", json!("scram_sha_512")),
        ("/sink/brokers", json!(["kafka:9092"])),
        ("/sink/topic", json!("enriched_events")),
        ("/sink/sasl/enabled", json!(true)),
        ("/sink/sasl/mechanism", json!("scram_sha_512")),
        (
            "/transforms/dir",
            json!("/etc/dfe-transform-vrl/transforms"),
        ),
    ]
}

/// The deployment default config: [`DEFAULT_CONFIG_KEYS`] from
/// `Config::default()`, then [`deployment_values`].
///
/// `None` when `Config::default()` does not serialise or a listed key is not in
/// it, which the contract tests refuse.
fn default_config() -> Option<serde_json::Value> {
    let defaults = serde_json::to_value(crate::config::Config::default()).ok()?;
    let mut config = serde_json::Value::Object(serde_json::Map::new());
    for pointer in DEFAULT_CONFIG_KEYS {
        set_at(&mut config, pointer, defaults.pointer(pointer)?.clone());
    }
    for (pointer, value) in deployment_values() {
        set_at(&mut config, pointer, value);
    }
    Some(config)
}

/// Set `value` at the JSON pointer `pointer` in `root`, creating the objects on the way.
fn set_at(root: &mut serde_json::Value, pointer: &str, value: serde_json::Value) {
    let mut node = root;
    let mut keys = pointer.trim_start_matches('/').split('/').peekable();
    while let Some(key) = keys.next() {
        let Some(object) = node.as_object_mut() else {
            return;
        };
        if keys.peek().is_none() {
            object.insert(key.to_owned(), value);
            return;
        }
        node = object
            .entry(key.to_owned())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    }
}

/// The Kafka Secret the chart mounts into each endpoint's SASL env vars.
///
/// `Config::apply_flat_env` reads these names: a credential per endpoint, and
/// one mechanism for both, since the Secret names its broker's. Each
/// `key_name` is distinct because the chart renders it as a values key.
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
            env(
                "DFE_TRANSFORM_KAFKA_SASL_MECHANISM",
                "mechanism",
                "sasl.mechanism",
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
        assert_eq!(c.schema_version, scalo::deployment::CONTRACT_SCHEMA_VERSION);
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
        let ci: serde_json::Value = scalo::yaml::from_path(&path, scalo::yaml::Profile::Config)
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

    /// `generate-artefacts` writes nothing for a contract that fails either
    /// check.
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
        assert_eq!(c.secrets[0].env_vars.len(), 5);
    }

    /// The broker's mechanism is the `sasl.mechanism` key beside the credential
    /// in the Kafka Secret, so a client never falls back to its own default.
    #[test]
    fn test_contract_mounts_the_mechanism_from_the_kafka_secret() {
        let c = contract();
        let mechanism = c.secrets[0]
            .env_vars
            .iter()
            .find(|env| env.env_var == "DFE_TRANSFORM_KAFKA_SASL_MECHANISM")
            .expect("the kafka group mounts the mechanism");
        assert_eq!(mechanism.secret_key, "sasl.mechanism");
    }

    /// The chart mounts a Secret under every declared name, so a name the
    /// config never reads leaves the credential silently unused.
    ///
    /// Each name spells the field it fills -- `DFE_TRANSFORM_SINK_SASL_PASSWORD`
    /// is `sink.sasl.password` -- so a name read into the other endpoint fails
    /// here too. A shared `KAFKA_SASL_*` name spells the field on both.
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
                let path = |field: &str| field.to_ascii_lowercase().replace('_', "/");
                let pointers: Vec<String> = field.strip_prefix("KAFKA_").map_or_else(
                    || vec![format!("/{}", path(field))],
                    |shared| {
                        ["source", "sink"]
                            .iter()
                            .map(|endpoint| format!("/{endpoint}/{}", path(shared)))
                            .collect()
                    },
                );

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
                let reached = pointers.iter().all(|pointer| {
                    applied.pointer(pointer).and_then(serde_json::Value::as_str)
                        == Some(sentinel.as_str())
                });
                // The message carries the env var and group names only, never a value.
                assert!(
                    reached,
                    "{} ({}) was set and the config field its name spells did not read it",
                    env.env_var, group.group_name
                );
            }
        }
    }

    /// The advertised default is the app's own default plus the values a
    /// deployment decides, and it loads and validates as a config file. A key
    /// the contract hand-wrote would differ from `Config::default()` here.
    #[test]
    fn the_default_config_is_the_apps_default_and_loads() {
        let advertised = contract().default_config.expect("default_config");
        let defaults = serde_json::to_value(crate::config::Config::default()).expect("config");

        let mut differing = Vec::new();
        let mut stack = vec![(String::new(), &advertised)];
        while let Some((path, node)) = stack.pop() {
            match node {
                serde_json::Value::Object(map) => {
                    stack.extend(map.iter().map(|(k, v)| (format!("{path}/{k}"), v)));
                }
                leaf if defaults.pointer(&path) != Some(leaf) => differing.push(path),
                _ => {}
            }
        }
        differing.sort();
        let mut decided: Vec<String> = deployment_values()
            .iter()
            .filter(|(pointer, value)| defaults.pointer(pointer) != Some(value))
            .map(|(pointer, _)| (*pointer).to_owned())
            .collect();
        decided.sort();
        assert_eq!(
            differing, decided,
            "only a deployment-decided value differs"
        );

        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, serde_json::to_string(&advertised).expect("json")).expect("write");
        let mut config = crate::config::Config::load(Some(file.to_str().expect("utf-8")))
            .expect("the default config loads");
        // The chart's Secret supplies the SCRAM credential through the env.
        for sasl in [&mut config.source.sasl, &mut config.sink.sasl] {
            sasl.username = "from-the-secret".to_owned();
            sasl.password = scalo::SensitiveString::new("from-the-secret");
        }
        config.validate().expect("the default config validates");
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
