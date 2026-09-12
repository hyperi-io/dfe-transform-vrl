// Project:   dfe-transform-vrl
// File:      src/config/validate.rs
// Purpose:   Configuration validation
// Language:  Rust
//
// License:   BUSL-1.1
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

        // Source. Structural problems refuse; a valid config with no topics is
        // a transform whose source has not been written yet, which idles
        // instead (see `work_state`).
        if self.source.transport.is_direct() {
            if self.source.listen.parse::<std::net::SocketAddr>().is_err() {
                return Err(crate::Error::Validation(format!(
                    "source.listen must be a bind address on the direct transport (got '{}')",
                    self.source.listen
                )));
            }
        } else {
            if self.source.brokers.is_empty() {
                return Err(crate::Error::Validation(
                    "source.brokers must have at least one broker".into(),
                ));
            }
            if self.source.group_id.is_empty() {
                return Err(crate::Error::Validation(
                    "source.group_id must not be empty".into(),
                ));
            }
        }
        validate_format(&self.source.format)?;
        validate_sasl("source.sasl", &self.source.sasl)?;

        // Sink
        if self.sink.transport.is_direct() {
            if self.sink.endpoint.is_empty() {
                return Err(crate::Error::Validation(
                    "sink.endpoint must not be empty on the direct transport".into(),
                ));
            }
        } else {
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

    /// Does this configuration give the transform work?
    ///
    /// Two ways to have none, and both are the same answer: start, stay Ready,
    /// hold no consumer group, and pick up the first config that gives the app
    /// work. On the bus a transform with no topics has nothing to consume; on
    /// the direct transport the listener IS the work. Either way a transform
    /// with no program can only pass records through, which is not what the
    /// source that named it asked for -- and the program arrives after the
    /// instance does, so refusing to boot would crash-loop the wait.
    #[must_use]
    pub fn work_state(&self) -> scalo::lifecycle::WorkState {
        if !self.source.transport.is_direct() && self.source.topics.is_empty() {
            return scalo::lifecycle::WorkState::idle("no source topics configured");
        }
        scalo::lifecycle::WorkState::idle_if(
            crate::engine::compiler::program_count(&self.transforms) == 0,
            "no VRL program in the transforms this config names",
        )
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

    /// A config with topics and one program on disk, which is a working transform.
    fn working_config(dir: &std::path::Path) -> Config {
        std::fs::write(dir.join("100_transform.vrl"), ".marked = true\n").unwrap();
        let mut config = minimal_config();
        config.transforms.dir = Some(dir.display().to_string());
        config
    }

    #[test]
    fn a_config_with_topics_and_a_program_is_active() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            working_config(dir.path()).work_state(),
            scalo::lifecycle::WorkState::Active
        );
    }

    #[test]
    fn no_topics_idles_on_the_topics_reason() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = working_config(dir.path());
        config.source.topics.clear();
        assert_eq!(
            config.work_state().reason(),
            Some("no source topics configured")
        );
    }

    #[test]
    fn an_empty_transforms_directory_idles_rather_than_refusing() {
        // The program arrives after the instance does, so an empty directory is
        // no work rather than a bad config.
        let dir = tempfile::tempdir().unwrap();
        let mut config = minimal_config();
        config.transforms.dir = Some(dir.path().display().to_string());

        assert!(config.validate().is_ok());
        assert_eq!(
            config.work_state().reason(),
            Some("no VRL program in the transforms this config names")
        );
    }

    #[test]
    fn a_transforms_directory_that_is_not_there_yet_idles_too() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = minimal_config();
        config.transforms.dir = Some(dir.path().join("not-written-yet").display().to_string());

        assert!(config.work_state().is_idle());
    }

    #[test]
    fn a_program_written_into_the_directory_turns_the_gate() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = minimal_config();
        config.transforms.dir = Some(dir.path().display().to_string());
        assert!(config.work_state().is_idle());

        std::fs::write(dir.path().join("100_transform.vrl"), ".marked = true\n").unwrap();

        assert_eq!(config.work_state(), scalo::lifecycle::WorkState::Active);
    }

    #[test]
    fn a_direct_transform_with_no_program_still_idles() {
        // The listener is the work on direct, but a pass-through is not what the
        // source that named a transform asked for.
        let dir = tempfile::tempdir().unwrap();
        let mut config = minimal_config();
        config.source.transport = crate::config::Transport::Direct;
        config.source.listen = "0.0.0.0:6000".to_string();
        config.sink.transport = crate::config::Transport::Direct;
        config.sink.endpoint = "http://dfe-loader:6000".to_string();
        config.transforms.dir = Some(dir.path().display().to_string());

        assert!(config.work_state().is_idle());
    }
}
