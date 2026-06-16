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

use hyperi_rustlib::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KedaConfig, KedaContract, NativeDepsContract,
    PortContract, SecretEnvContract, SecretGroupContract,
};

/// Build the deployment contract for dfe-transform-vrl.
#[must_use]
pub fn contract() -> DeploymentContract {
    DeploymentContract {
        app_name: "dfe-transform-vrl".into(),
        binary_name: "dfe-transform-vrl".into(),
        description: "Embedded VRL transform engine — Kafka-to-Kafka pipelines".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/health/live".into(),
            readiness_path: "/health/ready".into(),
            metrics_path: "/metrics".into(),
        },
        env_prefix: "DFE_TRANSFORM".into(),
        metric_prefix: "transform_vrl".into(),
        config_mount_path: "/etc/dfe-transform-vrl/config.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        base_image: "ubuntu:24.04".into(),
        extra_ports: vec![PortContract {
            name: "health".into(),
            port: 9000,
            protocol: "TCP".into(),
        }],
        entrypoint_args: vec![
            "--config".into(),
            "/etc/dfe-transform-vrl/config.yaml".into(),
        ],
        secrets: vec![SecretGroupContract {
            group_name: "kafka".into(),
            env_vars: vec![
                SecretEnvContract {
                    env_var: "KAFKA_SASL_USERNAME".into(),
                    key_name: "username".into(),
                    secret_key: "kafka-username".into(),
                },
                SecretEnvContract {
                    env_var: "KAFKA_SASL_PASSWORD".into(),
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
            // Horizontal scaling-pressure engine (rustlib 2.8.11). Read from the
            // global cascade by ScalingEngineConfig (requires the `expression`
            // feature). Kafka in/out -> inbound + outbound = kafka. lag_target is
            // intentionally omitted (TUNE-ME: no measured per-pod throughput);
            // the lag term then contributes 0 rather than mis-scaling.
            "scaling": {
                "enabled": true,
                "interval_secs": 15,
                "transport": { "inbound": "kafka", "outbound": "kafka" },
                "params": { "cpu_target": 0.70 },
                "pressures": []
            }
        })),
        depends_on: vec!["kafka".into()],
        native_deps: NativeDepsContract::for_rustlib_features(&["transport-kafka"], "ubuntu:24.04"),
        image_profile: ImageProfile::Production,
        // KedaContract is #[non_exhaustive] (rustlib 2.8.13) -- build it from a
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
        schema_version: 2,
        oci_labels: hyperi_rustlib::deployment::OciLabels::default(),
    }
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
    fn test_contract_health_paths() {
        let c = contract();
        assert_eq!(c.health.liveness_path, "/health/live");
        assert_eq!(c.health.readiness_path, "/health/ready");
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

    #[test]
    fn test_contract_default_config_present() {
        let c = contract();
        let cfg = c.default_config.unwrap();
        assert!(cfg.get("pipeline").is_some());
        assert!(cfg.get("source").is_some());
        assert!(cfg.get("sink").is_some());
    }

    /// Cascade-applied proof (rustlib 2.8.11): the `scaling` section the contract
    /// ships in the deployed `--config` deserialises into rustlib's OWN
    /// `ScalingEngineConfig` -- the exact type `from_cascade()` unmarshals from
    /// the `scaling` key once `run_app` populates the cascade from the file. This
    /// guards the shape (key names / kinds) the engine actually honours, so a
    /// rename here can't silently leave the engine on its defaults.
    #[test]
    fn test_contract_scaling_section_matches_rustlib_engine_config() {
        use hyperi_rustlib::scaling::ScalingEngineConfig;

        let cfg = contract().default_config.expect("default_config present");
        let scaling = cfg.get("scaling").expect("scaling section present");

        let engine: ScalingEngineConfig = serde_json::from_value(scaling.clone())
            .expect("scaling section must deser as rustlib ScalingEngineConfig");

        assert!(engine.enabled, "scaling engine must be enabled");
        assert_eq!(engine.interval_secs, 15);
        assert_eq!(engine.transport.inbound.as_deref(), Some("kafka"));
        assert_eq!(engine.transport.outbound.as_deref(), Some("kafka"));
        assert!(
            (engine.cpu_target() - 0.70).abs() < f64::EPSILON,
            "cpu_target must be 0.70"
        );
        // lag_target is intentionally omitted (TUNE-ME: no measured per-pod
        // throughput) so the lag term contributes 0 rather than mis-scaling.
        assert!(
            !engine.params.contains_key("lag_target"),
            "lag_target must stay UNSET until a real per-pod throughput is measured"
        );
        // Empty pressures => rustlib composes the context-aware smart default.
        assert!(engine.pressures.is_empty());
    }
}
