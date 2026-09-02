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
    DeploymentContract, HealthContract, ImageProfile, KedaConfig, KedaContract, NativeDepsContract,
    PortContract, SecretEnvContract, SecretGroupContract, base_image_from_cascade,
};

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
        extra_ports: vec![PortContract {
            name: "health".into(),
            port: 9000,
            protocol: "TCP".into(),
        }],
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
                "brokers": ["kafka:9092"],
                "topics": ["raw_events"],
                "group_id": "dfe-transform-vrl-default",
                "format": "auto",
                "sasl": { "enabled": true, "mechanism": "scram_sha_512" },
                "tls": { "enabled": false }
            },
            "sink": {
                "brokers": ["kafka:9092"],
                "topic": "enriched_events",
                "key_field": ".org_id",
                "compression": "zstd",
                "sasl": { "enabled": true, "mechanism": "scram_sha_512" },
                "tls": { "enabled": false }
            },
            "transforms": {
                "dir": "/etc/dfe-transform-vrl/transforms"
            },
            "health": { "address": "0.0.0.0:9000" },
            "metrics": { "address": "0.0.0.0:9090" },
            // Unified scaling-pressure engine (scalo 2.9). The CEL weighted
            // engine collapsed into ONE ScalingPressure served at
            // /scaling/pressure to KEDA: base config holds only the gate
            // thresholds; the weighted components (kafka_lag, memory) are
            // registered in code via `ServiceApp::scaling_components`. The
            // memory gate forces pressure to 100 once usage crosses
            // memory_gate_threshold (scale before OOM).
            "scaling": {
                "enabled": true,
                "memory_gate_threshold": 0.8
            }
        })),
        depends_on: vec!["kafka".into()],
        native_deps: NativeDepsContract::for_scalo_features(&["transport-kafka"], &base_image),
        image_profile: ImageProfile::Production,
        // KedaContract is #[non_exhaustive] (scalo) -- build it from a
        // KedaConfig holding this app's real KEDA values and convert. The
        // scaling_pressure_* trigger fields then come from KedaConfig defaults
        // (trigger OFF -- the Prometheus serverAddress is cluster-specific), and
        // ..Default::default() future-proofs any later contract-field additions.
        keda: Some(KedaContract::from_config(&KedaConfig {
            min_replicas: 1,
            max_replicas: 10,
            polling_interval: 15,
            cooldown_period: 300,
            kafka_lag_threshold: 1000,
            activation_lag_threshold: 0,
            cpu_enabled: true,
            cpu_threshold: 80,
            ..Default::default()
        })),
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
        assert_eq!(c.extra_ports[0].port, 9000);
        assert_eq!(c.extra_ports[0].name, "health");
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

    #[test]
    fn test_contract_keda_enabled() {
        let c = contract();
        let keda = c.keda.as_ref().unwrap();
        assert_eq!(keda.min_replicas, 1);
        assert_eq!(keda.max_replicas, 10);
        assert_eq!(keda.kafka_lag_threshold, 1000);
        assert!(keda.cpu_enabled);
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

                let rendered = serde_json::to_string(&config).expect("config serialises");
                assert!(
                    rendered.contains(&sentinel),
                    "{} is injected by the chart but never lands in the config",
                    env.env_var
                );
            }
        }
    }

    /// `chart/` is `emit-chart` output, so a hand edit there is reverted by the
    /// next regen -- which is how the Kafka SASL env names shipped broken.
    /// `templates/keda-scaledobject.yaml` is the one deliberate exception: the
    /// generator emits a `.Values.config.kafka.*` path this app's values do not
    /// have, so a regenerated copy will not render at all.
    #[test]
    fn test_committed_chart_matches_the_generator() {
        const HAND_FIXED: &[&str] = &["templates/keda-scaledobject.yaml"];

        let generated = tempfile::tempdir().expect("temp dir");
        scalo::deployment::generate_chart(&contract(), generated.path(), None)
            .expect("chart generates");

        let want = read_chart(generated.path());
        let got = read_chart(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("chart"));

        assert_eq!(
            want.keys().collect::<Vec<_>>(),
            got.keys().collect::<Vec<_>>(),
            "chart/ holds a different set of files from `emit-chart`"
        );

        for (rel, from_generator) in &want {
            if HAND_FIXED.contains(&rel.as_str()) {
                assert_ne!(
                    got[rel], *from_generator,
                    "chart/{rel} is listed as hand-fixed but now matches the \
                     generator -- drop it from HAND_FIXED"
                );
                continue;
            }
            assert_eq!(
                got[rel], *from_generator,
                "chart/{rel} has drifted from `emit-chart` -- fix contract() and \
                 regenerate, never hand-edit the output"
            );
        }
    }

    /// Relative path -> contents for a chart directory (root files plus
    /// `templates/`, which is the whole shape the generator emits).
    fn read_chart(dir: &std::path::Path) -> std::collections::BTreeMap<String, String> {
        let mut out = std::collections::BTreeMap::new();
        for sub in [None, Some("templates")] {
            let here = sub.map_or_else(|| dir.to_path_buf(), |s| dir.join(s));
            for entry in std::fs::read_dir(&here).expect("chart directory readable") {
                let entry = entry.expect("chart directory entry");
                if !entry.file_type().expect("file type").is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                let rel = sub.map_or_else(|| name.clone(), |s| format!("{s}/{name}"));
                let body = std::fs::read_to_string(entry.path()).expect("chart file readable");
                out.insert(rel, body);
            }
        }
        out
    }

    #[test]
    fn test_contract_default_config_present() {
        let c = contract();
        let cfg = c.default_config.unwrap();
        assert!(cfg.get("pipeline").is_some());
        assert!(cfg.get("source").is_some());
        assert!(cfg.get("sink").is_some());
    }

    /// Cascade-applied proof (scalo 2.9): the `scaling` section the contract
    /// ships in the deployed `--config` deserialises into scalo's OWN
    /// `ScalingPressureConfig` -- the exact type `from_cascade()` unmarshals
    /// from the `scaling` key once `run_app` populates the cascade from the
    /// file. This guards the shape (key names / kinds) the engine honours, so a
    /// rename here can't silently leave the engine on its defaults. The 2.9
    /// unified engine config holds only the gate thresholds; the weighted KEDA
    /// components are registered in code (`ServiceApp::scaling_components`), not
    /// in the config.
    #[test]
    fn test_contract_scaling_section_matches_scalo_pressure_config() {
        use scalo::scaling::ScalingPressureConfig;

        let cfg = contract().default_config.expect("default_config present");
        let scaling = cfg.get("scaling").expect("scaling section present");

        let pressure: ScalingPressureConfig = serde_json::from_value(scaling.clone())
            .expect("scaling section must deser as scalo ScalingPressureConfig");

        assert!(pressure.enabled, "scaling engine must be enabled");
        assert!(
            (pressure.memory_gate_threshold - 0.8).abs() < f64::EPSILON,
            "memory_gate_threshold must be 0.8 (scale before OOM)"
        );
    }
}
