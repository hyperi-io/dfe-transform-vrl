// Project:   dfe-transform-vrl
// File:      tests/integration_vrl_transforms.rs
// Purpose:   Integration tests — VRL transforms against fixture files
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for VRL transforms against fixture files.
//!
//! Tests load real .vrl files from the fixtures directory, compile them,
//! and run them against sample events to verify end-to-end transform behaviour.

#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use dfe_transform_vrl::config::TransformConfig;
    use dfe_transform_vrl::engine::compiler::{compile_vrl, load_vrl_source};
    use dfe_transform_vrl::engine::runner::{run_vrl, run_vrl_batch};
    use vrl::value::Value;

    fn fixtures_dir() -> String {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        format!("{manifest}/tests/fixtures/transforms")
    }

    fn load_and_compile(config: &TransformConfig) -> vrl::compiler::Program {
        let source = load_vrl_source(config).unwrap();
        compile_vrl(&source).unwrap().program
    }

    #[test]
    fn test_all_fixture_transforms_compile() {
        let config = TransformConfig {
            dir: Some(fixtures_dir()),
            files: None,
        };
        let source = load_vrl_source(&config).unwrap();
        let result = compile_vrl(&source);
        assert!(
            result.is_ok(),
            "fixture transforms should compile: {}",
            result.err().map_or_else(String::new, |e| e.to_string())
        );
    }

    #[test]
    fn test_enrich_metadata_adds_fields() {
        let config = TransformConfig {
            dir: None,
            files: Some(vec![format!("{}/02_enrich_metadata.vrl", fixtures_dir())]),
        };
        let program = load_and_compile(&config);
        let mut event = Value::from(serde_json::json!({"message": "hello"}));
        let result = run_vrl(&program, &mut event);
        assert!(result.is_ok());

        let obj = event.as_object().unwrap();
        let meta = obj.get("meta").unwrap().as_object().unwrap();
        assert_eq!(meta.get("processed"), Some(&Value::Boolean(true)));
        assert_eq!(meta.get("pipeline"), Some(&Value::from("test-pipeline")));
        assert_eq!(meta.get("version"), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_normalize_fields_downcases_level() {
        let config = TransformConfig {
            dir: None,
            files: Some(vec![format!("{}/03_normalize_fields.vrl", fixtures_dir())]),
        };
        let program = load_and_compile(&config);
        let mut event = Value::from(serde_json::json!({
            "message": "  spaced message  ",
            "level": "ERROR"
        }));
        let result = run_vrl(&program, &mut event);
        assert!(result.is_ok());

        let obj = event.as_object().unwrap();
        assert_eq!(obj.get("message"), Some(&Value::from("spaced message")));
        assert_eq!(obj.get("level"), Some(&Value::from("error")));
    }

    #[test]
    fn test_filter_fields_removes_internal() {
        let config = TransformConfig {
            dir: None,
            files: Some(vec![format!("{}/04_filter_fields.vrl", fixtures_dir())]),
        };
        let program = load_and_compile(&config);
        let mut event = Value::from(serde_json::json!({
            "message": "keep this",
            "internal_id": "abc123",
            "debug_info": {"trace": true}
        }));
        let result = run_vrl(&program, &mut event);
        assert!(result.is_ok());

        let obj = event.as_object().unwrap();
        assert!(obj.get("message").is_some());
        assert!(obj.get("internal_id").is_none());
        assert!(obj.get("debug_info").is_none());
    }

    #[test]
    fn test_full_pipeline_all_transforms() {
        let config = TransformConfig {
            dir: Some(fixtures_dir()),
            files: None,
        };
        let program = load_and_compile(&config);

        let mut event = Value::from(serde_json::json!({
            "message": "  test event  ",
            "level": "WARNING",
            "internal_id": "int-001",
            "debug_info": {"verbose": true}
        }));

        let result = run_vrl(&program, &mut event);
        assert!(result.is_ok());

        let obj = event.as_object().unwrap();
        // 02_enrich_metadata should add .meta
        assert!(obj.get("meta").is_some());
        // 03_normalize_fields should strip whitespace and downcase
        assert_eq!(obj.get("message"), Some(&Value::from("test event")));
        assert_eq!(obj.get("level"), Some(&Value::from("warning")));
        // 04_filter_fields should remove internal fields
        assert!(obj.get("internal_id").is_none());
        assert!(obj.get("debug_info").is_none());
    }

    #[test]
    fn test_batch_transform_all_fixtures() {
        let config = TransformConfig {
            dir: Some(fixtures_dir()),
            files: None,
        };
        let program = load_and_compile(&config);

        let mut events: Vec<Value> = (0..10)
            .map(|i| {
                Value::from(serde_json::json!({
                    "message": format!("  event {i}  "),
                    "level": "INFO",
                    "internal_id": format!("id-{i}"),
                }))
            })
            .collect();

        let results = run_vrl_batch(&program, &mut events);
        assert_eq!(results.len(), 10);
        for (_, result) in &results {
            assert!(result.is_ok());
        }

        for (i, event) in events.iter().enumerate() {
            let obj = event.as_object().unwrap();
            assert_eq!(obj.get("message"), Some(&Value::from(format!("event {i}"))));
            assert_eq!(obj.get("level"), Some(&Value::from("info")));
            assert!(obj.get("internal_id").is_none());
            assert!(obj.get("meta").is_some());
        }
    }

    #[test]
    fn test_individual_file_loading() {
        let dir = fixtures_dir();
        let config = TransformConfig {
            dir: None,
            files: Some(vec![
                format!("{dir}/04_filter_fields.vrl"),
                format!("{dir}/02_enrich_metadata.vrl"),
            ]),
        };
        let source = load_vrl_source(&config).unwrap();
        assert!(source.contains("del(.internal_id)"));
        assert!(source.contains(".meta.processed = true"));

        let result = compile_vrl(&source);
        assert!(result.is_ok());
    }
}
