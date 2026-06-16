// Project:   dfe-transform-vrl
// File:      src/error.rs
// Purpose:   Error types for the application
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Error types for dfe-transform-vrl.

use thiserror::Error;

/// Main error type.
#[derive(Error, Debug)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("VRL compilation error: {0}")]
    VrlCompile(String),

    #[error("VRL runtime error: {0}")]
    VrlRuntime(String),

    #[error("VRL abort: {0}")]
    VrlAbort(String),

    #[error("Kafka error: {0}")]
    Kafka(String),

    #[error("serialisation error: {0}")]
    Serialisation(String),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("enrichment error: {0}")]
    Enrichment(String),

    #[error("health check error: {0}")]
    Health(String),

    #[error("shutdown requested")]
    Shutdown,
}

/// Result type alias using our Error.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn display_config_error() {
        let e = Error::Config("missing field".to_string());
        assert_eq!(e.to_string(), "configuration error: missing field");
    }

    #[test]
    fn display_vrl_compile_error() {
        let e = Error::VrlCompile("unexpected token".to_string());
        assert_eq!(e.to_string(), "VRL compilation error: unexpected token");
    }

    #[test]
    fn display_vrl_runtime_error() {
        let e = Error::VrlRuntime("type mismatch".to_string());
        assert_eq!(e.to_string(), "VRL runtime error: type mismatch");
    }

    #[test]
    fn display_vrl_abort() {
        let e = Error::VrlAbort("event filtered".to_string());
        assert_eq!(e.to_string(), "VRL abort: event filtered");
    }

    #[test]
    fn display_kafka_error() {
        let e = Error::Kafka("broker unreachable".to_string());
        assert_eq!(e.to_string(), "Kafka error: broker unreachable");
    }

    #[test]
    fn display_enrichment_error() {
        let e = Error::Enrichment("table not found".to_string());
        assert_eq!(e.to_string(), "enrichment error: table not found");
    }

    #[test]
    fn display_shutdown() {
        let e = Error::Shutdown;
        assert_eq!(e.to_string(), "shutdown requested");
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let e: Error = io_err.into();
        assert!(matches!(e, Error::Io(_)));
        assert!(e.to_string().contains("gone"));
    }

    #[test]
    fn from_yaml_error() {
        let yaml_err = serde_yaml_ng::from_str::<serde_yaml_ng::Value>("{{invalid").unwrap_err();
        let e: Error = yaml_err.into();
        assert!(matches!(e, Error::Yaml(_)));
    }

    #[test]
    fn error_is_debug() {
        let e = Error::Config("test".to_string());
        let debug = format!("{e:?}");
        assert!(debug.contains("Config"));
    }
}
