// Project:   dfe-transform-vrl
// File:      src/config/validate.rs
// Purpose:   Configuration validation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration validation.

use crate::Result;
use crate::config::loader::{Config, SaslConfig};

impl Config {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.pipeline.name.is_empty() {
            return Err(crate::Error::Validation(
                "pipeline.name must not be empty".into(),
            ));
        }
        if self.pipeline.batch_size == 0 {
            return Err(crate::Error::Validation(
                "pipeline.batch_size must be > 0".into(),
            ));
        }
        // Floor of 10ms — anything lower spins the consumer recv loop into a
        // busy-poll burning CPU without meaningfully reducing batch latency.
        if self.pipeline.batch_timeout_ms < 10 {
            return Err(crate::Error::Validation(format!(
                "pipeline.batch_timeout_ms must be >= 10 (got {}); shorter \
                 timeouts produce a busy-poll loop without meaningful latency \
                 reduction",
                self.pipeline.batch_timeout_ms
            )));
        }

        // Source
        if self.source.brokers.is_empty() {
            return Err(crate::Error::Validation(
                "source.brokers must have at least one broker".into(),
            ));
        }
        if self.source.topics.is_empty() {
            return Err(crate::Error::Validation(
                "source.topics must have at least one topic".into(),
            ));
        }
        if self.source.group_id.is_empty() {
            return Err(crate::Error::Validation(
                "source.group_id must not be empty".into(),
            ));
        }
        validate_format(&self.source.format)?;
        validate_sasl("source.sasl", &self.source.sasl)?;

        // Sink
        if self.sink.brokers.is_empty() {
            return Err(crate::Error::Validation(
                "sink.brokers must have at least one broker".into(),
            ));
        }
        if self.sink.topic.is_empty() {
            return Err(crate::Error::Validation(
                "sink.topic must not be empty".into(),
            ));
        }
        validate_sasl("sink.sasl", &self.sink.sasl)?;

        // Transforms — must have at least dir or files
        if self.transforms.dir.is_none() && self.transforms.files.is_none() {
            return Err(crate::Error::Validation(
                "transforms.dir or transforms.files must be specified".into(),
            ));
        }

        Ok(())
    }
}

fn validate_format(format: &str) -> Result<()> {
    let valid = ["auto", "json", "msgpack"];
    if !valid.contains(&format) {
        return Err(crate::Error::Validation(format!(
            "source.format must be one of: {}",
            valid.join(", ")
        )));
    }
    Ok(())
}

fn validate_sasl(prefix: &str, sasl: &SaslConfig) -> Result<()> {
    if !sasl.enabled {
        return Ok(());
    }
    let valid_mechanisms = ["plain", "scram_sha_256", "scram_sha_512"];
    if !valid_mechanisms.contains(&sasl.mechanism.as_str()) {
        return Err(crate::Error::Validation(format!(
            "{prefix}.mechanism must be one of: {}",
            valid_mechanisms.join(", ")
        )));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn minimal_config() -> Config {
        Config {
            pipeline: crate::config::PipelineConfig {
                name: "test".to_string(),
                ..Default::default()
            },
            source: crate::config::SourceConfig {
                brokers: vec!["localhost:9092".to_string()],
                topics: vec!["input".to_string()],
                group_id: "test-group".to_string(),
                ..Default::default()
            },
            sink: crate::config::SinkConfig {
                brokers: vec!["localhost:9092".to_string()],
                topic: "output".to_string(),
                ..Default::default()
            },
            transforms: crate::config::TransformConfig {
                dir: Some("/etc/dfe-transform-vrl/transforms".to_string()),
                files: None,
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_valid_config() {
        let config = minimal_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_empty_pipeline_name() {
        let mut config = minimal_config();
        config.pipeline.name = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_source_brokers() {
        let mut config = minimal_config();
        config.source.brokers.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_sink_topic() {
        let mut config = minimal_config();
        config.sink.topic = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_no_transforms() {
        let mut config = minimal_config();
        config.transforms.dir = None;
        config.transforms.files = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_format() {
        let mut config = minimal_config();
        config.source.format = "xml".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_valid_formats() {
        for format in &["auto", "json", "msgpack"] {
            let mut config = minimal_config();
            config.source.format = format.to_string();
            assert!(config.validate().is_ok(), "format {format} should be valid");
        }
    }

    #[test]
    fn test_invalid_sasl_mechanism() {
        let mut config = minimal_config();
        config.source.sasl.enabled = true;
        config.source.sasl.mechanism = "SCRAM-SHA-512".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_zero_batch_size() {
        let mut config = minimal_config();
        config.pipeline.batch_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_batch_timeout_below_floor_rejected() {
        for too_small in [0_u64, 1, 5, 9] {
            let mut config = minimal_config();
            config.pipeline.batch_timeout_ms = too_small;
            let err = config.validate().unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("batch_timeout_ms"),
                "expected batch_timeout_ms in error for value {too_small}, got: {msg}"
            );
        }
    }

    #[test]
    fn test_batch_timeout_at_floor_accepted() {
        let mut config = minimal_config();
        config.pipeline.batch_timeout_ms = 10;
        assert!(config.validate().is_ok());
    }
}
