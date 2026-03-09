// Project:   dfe-transform-vrl
// File:      src/deployment.rs
// Purpose:   Deployment contract for artefact generation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for Dockerfile, Helm chart, and compose fragment generation.
//!
//! Unlike dfe-transform-vector, this service does NOT need the Vector binary
//! in its container image. The container is just the Rust binary.

use hyperi_rustlib::deployment::{
    DeploymentContract, HealthContract, KedaContract, PortContract, SecretEnvContract,
    SecretGroupContract,
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
        config_mount_path: "/etc/dfe/config.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        base_image: "ubuntu:24.04".into(),
        extra_ports: vec![PortContract {
            name: "health".into(),
            port: 9000,
            protocol: "TCP".into(),
        }],
        entrypoint_args: vec!["--config".into(), "/etc/dfe/config.yaml".into()],
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
                "dir": "/etc/dfe/transforms"
            },
            "health": { "address": "0.0.0.0:9000" },
            "metrics": { "address": "0.0.0.0:9090" }
        })),
        depends_on: vec!["kafka".into()],
        keda: Some(KedaContract {
            min_replicas: 1,
            max_replicas: 10,
            polling_interval: 15,
            cooldown_period: 300,
            kafka_lag_threshold: 1000,
            activation_lag_threshold: 0,
            cpu_enabled: true,
            cpu_threshold: 80,
        }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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
}
