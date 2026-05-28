// Project:   dfe-transform-vrl
// File:      tests/integration/vrl_realworld.rs
// Purpose:   Real-world VRL transform patterns
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

use dfe_transform_vrl::config::TransformConfig;
use dfe_transform_vrl::engine::compiler::{compile_vrl, load_vrl_source};
use dfe_transform_vrl::engine::runner::{run_vrl, run_vrl_batch};
use vrl::value::Value;

fn realworld_dir() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    format!("{manifest}/tests/fixtures/transforms_realworld")
}

fn load_single(filename: &str) -> vrl::compiler::Program {
    let config = TransformConfig {
        dir: None,
        files: Some(vec![format!("{}/{filename}", realworld_dir())]),
    };
    let source = load_vrl_source(&config).unwrap();
    compile_vrl(&source, None).unwrap().program
}

// =========================================================================
// 01_conditional_routing.vrl
// =========================================================================

mod conditional_routing {
    use super::*;

    #[test]
    fn test_info_event_routed_normal() {
        let program = load_single("01_conditional_routing.vrl");
        let mut event = Value::from(serde_json::json!({
            "level": "INFO",
            "message": "app started"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        assert_eq!(obj.get("level"), Some(&Value::from("info")));
        let routing = obj.get("routing").unwrap().as_object().unwrap();
        assert_eq!(routing.get("priority"), Some(&Value::from("normal")));
        assert_eq!(routing.get("alert"), Some(&Value::Boolean(false)));
    }

    #[test]
    fn test_error_event_routed_high() {
        let program = load_single("01_conditional_routing.vrl");
        let mut event = Value::from(serde_json::json!({
            "level": "ERROR",
            "message": "connection failed"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let routing = event
            .as_object()
            .unwrap()
            .get("routing")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(routing.get("priority"), Some(&Value::from("high")));
        assert_eq!(routing.get("alert"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_critical_event_routed_high() {
        let program = load_single("01_conditional_routing.vrl");
        let mut event = Value::from(serde_json::json!({
            "level": "CRITICAL",
            "message": "disk full"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let routing = event
            .as_object()
            .unwrap()
            .get("routing")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(routing.get("priority"), Some(&Value::from("high")));
        assert_eq!(routing.get("alert"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_debug_event_dropped() {
        let program = load_single("01_conditional_routing.vrl");
        let mut event = Value::from(serde_json::json!({
            "level": "DEBUG",
            "message": "trace detail"
        }));
        assert!(
            run_vrl(&program, &mut event).is_err(),
            "debug events should be aborted"
        );
    }

    #[test]
    fn test_missing_level_gets_unknown() {
        let program = load_single("01_conditional_routing.vrl");
        let mut event = Value::from(serde_json::json!({"message": "no level field"}));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert_eq!(
            event.as_object().unwrap().get("level"),
            Some(&Value::from("unknown"))
        );
    }

    #[test]
    fn test_batch_routing_with_drops() {
        let program = load_single("01_conditional_routing.vrl");
        let mut events = vec![
            Value::from(serde_json::json!({"level": "INFO"})),
            Value::from(serde_json::json!({"level": "DEBUG"})),
            Value::from(serde_json::json!({"level": "ERROR"})),
            Value::from(serde_json::json!({"level": "DEBUG"})),
            Value::from(serde_json::json!({"level": "WARNING"})),
        ];

        let results = run_vrl_batch(&program, &mut events);
        assert!(results[0].1.is_ok(), "INFO should pass");
        assert!(results[1].1.is_err(), "DEBUG should drop");
        assert!(results[2].1.is_ok(), "ERROR should pass");
        assert!(results[3].1.is_err(), "DEBUG should drop");
        assert!(results[4].1.is_ok(), "WARNING should pass");

        // Count: 3 pass, 2 drop
        let pass_count = results.iter().filter(|(_, r)| r.is_ok()).count();
        let drop_count = results.iter().filter(|(_, r)| r.is_err()).count();
        assert_eq!(pass_count, 3);
        assert_eq!(drop_count, 2);
    }
}

// =========================================================================
// 02_kv_extract.vrl
// =========================================================================

mod kv_extract {
    use super::*;

    #[test]
    fn test_basic_kv_extraction() {
        let program = load_single("02_kv_extract.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "user=alice action=login status=success"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let extracted = event
            .as_object()
            .unwrap()
            .get("extracted")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(extracted.get("user"), Some(&Value::from("alice")));
        assert_eq!(extracted.get("action"), Some(&Value::from("login")));
        assert_eq!(extracted.get("status"), Some(&Value::from("success")));
    }

    #[test]
    fn test_kv_with_duration_ms() {
        let program = load_single("02_kv_extract.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "action=query duration=42ms"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let extracted = event
            .as_object()
            .unwrap()
            .get("extracted")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(extracted.get("action"), Some(&Value::from("query")));
        assert_eq!(extracted.get("duration_ms"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_kv_no_message_field() {
        let program = load_single("02_kv_extract.vrl");
        let mut event = Value::from(serde_json::json!({"other": "data"}));
        assert!(run_vrl(&program, &mut event).is_ok());
        // Should not have extracted anything
        assert!(event.as_object().unwrap().get("extracted").is_none());
    }
}

// =========================================================================
// 03_json_unflatten.vrl
// =========================================================================

mod json_unflatten {
    use super::*;

    #[test]
    fn test_embedded_json_extracted() {
        let program = load_single("03_json_unflatten.vrl");
        let inner = serde_json::json!({
            "timestamp": "2026-01-01T00:00:00Z",
            "level": "info",
            "service": "api",
            "data": {"key": "value"}
        });
        let mut event = Value::from(serde_json::json!({
            "message": inner.to_string()
        }));
        assert!(run_vrl(&program, &mut event).is_ok());

        let obj = event.as_object().unwrap();
        assert_eq!(
            obj.get("event_time"),
            Some(&Value::from("2026-01-01T00:00:00Z"))
        );
        assert_eq!(obj.get("level"), Some(&Value::from("info")));
        assert_eq!(obj.get("service"), Some(&Value::from("api")));
        // Remaining fields stay in .payload
        let payload = obj.get("payload").unwrap().as_object().unwrap();
        assert!(payload.get("data").is_some());
        // Promoted fields removed from payload
        assert!(payload.get("timestamp").is_none());
        assert!(payload.get("level").is_none());
    }

    #[test]
    fn test_non_json_message_untouched() {
        let program = load_single("03_json_unflatten.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "plain text log line"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        // Should not have .payload
        assert!(event.as_object().unwrap().get("payload").is_none());
        assert_eq!(
            event.as_object().unwrap().get("message"),
            Some(&Value::from("plain text log line"))
        );
    }

    #[test]
    fn test_json_array_message_untouched() {
        let program = load_single("03_json_unflatten.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "[1,2,3]"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        // Array is not an object, so should not be extracted
        assert!(event.as_object().unwrap().get("payload").is_none());
    }
}

// =========================================================================
// 04_key_rename_sanitize.vrl
// =========================================================================

mod key_rename {
    use super::*;

    #[test]
    fn test_at_timestamp_renamed() {
        let program = load_single("04_key_rename_sanitize.vrl");
        let mut event = Value::from(serde_json::json!({
            "@timestamp": "2026-01-01T00:00:00Z",
            "message": "test"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        assert!(obj.get("@timestamp").is_none());
        assert_eq!(
            obj.get("event_time"),
            Some(&Value::from("2026-01-01T00:00:00Z"))
        );
    }

    #[test]
    fn test_at_message_renamed() {
        let program = load_single("04_key_rename_sanitize.vrl");
        let mut event = Value::from(serde_json::json!({
            "@message": "hello world"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        assert!(obj.get("@message").is_none());
        assert_eq!(obj.get("message"), Some(&Value::from("hello world")));
    }

    #[test]
    fn test_host_name_flattened() {
        let program = load_single("04_key_rename_sanitize.vrl");
        let mut event = Value::from(serde_json::json!({
            "host": {"name": "prod-web-01"}
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        assert_eq!(obj.get("hostname"), Some(&Value::from("prod-web-01")));
        // Empty host object should be cleaned up
        assert!(obj.get("host").is_none());
    }

    #[test]
    fn test_missing_org_id_gets_default() {
        let program = load_single("04_key_rename_sanitize.vrl");
        let mut event = Value::from(serde_json::json!({"message": "no org"}));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert_eq!(
            event.as_object().unwrap().get("org_id"),
            Some(&Value::from("unknown"))
        );
    }

    #[test]
    fn test_existing_org_id_preserved() {
        let program = load_single("04_key_rename_sanitize.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "with org",
            "org_id": "acme-corp"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert_eq!(
            event.as_object().unwrap().get("org_id"),
            Some(&Value::from("acme-corp"))
        );
    }

    #[test]
    fn test_full_elastic_style_event() {
        let program = load_single("04_key_rename_sanitize.vrl");
        let mut event = Value::from(serde_json::json!({
            "@timestamp": "2026-03-18T12:00:00Z",
            "@message": "request completed",
            "host": {"name": "api-3"},
            "status": 200
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        assert_eq!(
            obj.get("event_time"),
            Some(&Value::from("2026-03-18T12:00:00Z"))
        );
        assert_eq!(obj.get("message"), Some(&Value::from("request completed")));
        assert_eq!(obj.get("hostname"), Some(&Value::from("api-3")));
        assert_eq!(obj.get("org_id"), Some(&Value::from("unknown")));
        assert!(obj.get("@timestamp").is_none());
        assert!(obj.get("@message").is_none());
    }
}

// =========================================================================
// 05_ip_enrichment.vrl
// =========================================================================

mod ip_enrichment {
    use super::*;

    #[test]
    fn test_private_ipv4_10() {
        let program = load_single("05_ip_enrichment.vrl");
        let mut event = Value::from(serde_json::json!({"source_ip": "10.0.1.5"}));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        assert_eq!(obj.get("ip_valid"), Some(&Value::Boolean(true)));
        assert_eq!(obj.get("ip_class"), Some(&Value::from("private")));
    }

    #[test]
    fn test_private_ipv4_192() {
        let program = load_single("05_ip_enrichment.vrl");
        let mut event = Value::from(serde_json::json!({"source_ip": "192.168.1.1"}));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert_eq!(
            event.as_object().unwrap().get("ip_class"),
            Some(&Value::from("private"))
        );
    }

    #[test]
    fn test_loopback() {
        let program = load_single("05_ip_enrichment.vrl");
        let mut event = Value::from(serde_json::json!({"source_ip": "127.0.0.1"}));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert_eq!(
            event.as_object().unwrap().get("ip_class"),
            Some(&Value::from("private"))
        );
    }

    #[test]
    fn test_public_ip() {
        let program = load_single("05_ip_enrichment.vrl");
        let mut event = Value::from(serde_json::json!({"source_ip": "8.8.8.8"}));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        assert_eq!(obj.get("ip_valid"), Some(&Value::Boolean(true)));
        assert_eq!(obj.get("ip_class"), Some(&Value::from("public")));
    }

    #[test]
    fn test_no_source_ip_field() {
        let program = load_single("05_ip_enrichment.vrl");
        let mut event = Value::from(serde_json::json!({"other": "data"}));
        assert!(run_vrl(&program, &mut event).is_ok());
        // No ip_valid or ip_class fields should be set
        let obj = event.as_object().unwrap();
        assert!(obj.get("ip_valid").is_none());
        assert!(obj.get("ip_class").is_none());
    }
}

// =========================================================================
// 06_multiline_complex.vrl
// =========================================================================

mod complex_pipeline {
    use super::*;

    #[test]
    fn test_success_200() {
        let program = load_single("06_multiline_complex.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "request completed",
            "status_code": 200,
            "response_time_ms": 42.5,
            "internal_trace_id": "abc",
            "_metadata": {"debug": true}
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        assert_eq!(obj.get("status_code"), Some(&Value::Integer(200)));
        assert_eq!(obj.get("status_category"), Some(&Value::from("success")));
        assert_eq!(obj.get("pipeline"), Some(&Value::from("realworld-test")));
        assert!(obj.get("processed_at").is_some());
        assert!(obj.get("internal_trace_id").is_none());
        assert!(obj.get("_metadata").is_none());
    }

    #[test]
    fn test_client_error_404() {
        let program = load_single("06_multiline_complex.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "not found",
            "status_code": 404
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert_eq!(
            event.as_object().unwrap().get("status_category"),
            Some(&Value::from("client_error"))
        );
    }

    #[test]
    fn test_server_error_500() {
        let program = load_single("06_multiline_complex.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "internal error",
            "status_code": 500
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert_eq!(
            event.as_object().unwrap().get("status_category"),
            Some(&Value::from("server_error"))
        );
    }

    #[test]
    fn test_string_status_code_coerced() {
        let program = load_single("06_multiline_complex.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "with string code",
            "status_code": "200"
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert_eq!(
            event.as_object().unwrap().get("status_code"),
            Some(&Value::Integer(200))
        );
        assert_eq!(
            event.as_object().unwrap().get("status_category"),
            Some(&Value::from("success"))
        );
    }

    #[test]
    fn test_missing_message_aborts() {
        let program = load_single("06_multiline_complex.vrl");
        let mut event = Value::from(serde_json::json!({
            "status_code": 200
        }));
        assert!(
            run_vrl(&program, &mut event).is_err(),
            "missing .message should abort"
        );
    }

    #[test]
    fn test_long_message_truncated() {
        let program = load_single("06_multiline_complex.vrl");
        let long_msg = "x".repeat(2048);
        let mut event = Value::from(serde_json::json!({
            "message": long_msg,
            "status_code": 200
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        let obj = event.as_object().unwrap();
        let msg = obj.get("message").unwrap().as_str().unwrap();
        assert_eq!(msg.len(), 1024);
        assert_eq!(obj.get("message_truncated"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_short_message_not_truncated() {
        let program = load_single("06_multiline_complex.vrl");
        let mut event = Value::from(serde_json::json!({
            "message": "short",
            "status_code": 200
        }));
        assert!(run_vrl(&program, &mut event).is_ok());
        assert!(
            event
                .as_object()
                .unwrap()
                .get("message_truncated")
                .is_none()
        );
    }
}

// =========================================================================
// All realworld fixtures compile
// =========================================================================

#[test]
fn test_all_realworld_fixtures_compile_individually() {
    let dir = realworld_dir();
    let entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "vrl"))
        .collect();

    assert!(
        !entries.is_empty(),
        "should have realworld fixture .vrl files"
    );

    for entry in entries {
        let path = entry.path();
        let source = std::fs::read_to_string(&path).unwrap();
        let result = compile_vrl(&source, None);
        assert!(
            result.is_ok(),
            "fixture {} should compile: {}",
            path.display(),
            result.err().map_or_else(String::new, |e| e.to_string())
        );
    }
}

// =========================================================================
// Multi-file chaining — verify transform ordering matters
// =========================================================================

#[test]
fn test_transform_order_matters() {
    // File A: set .level = downcase(.level)
    // File B: check .level == "error" → add tag
    // Order A then B should work; B without A might not (if level is uppercase)
    let program_ab = compile_vrl(
        r#"
        .level = downcase!(string!(.level))
        if .level == "error" { .is_error = true }
    "#,
        None,
    )
    .unwrap()
    .program;

    let mut event = Value::from(serde_json::json!({"level": "ERROR"}));
    assert!(run_vrl(&program_ab, &mut event).is_ok());
    assert_eq!(
        event.as_object().unwrap().get("is_error"),
        Some(&Value::Boolean(true))
    );

    // Without normalisation first, uppercase "ERROR" != "error"
    let program_b_only = compile_vrl(
        r#"
        if .level == "error" { .is_error = true }
    "#,
        None,
    )
    .unwrap()
    .program;

    let mut event2 = Value::from(serde_json::json!({"level": "ERROR"}));
    assert!(run_vrl(&program_b_only, &mut event2).is_ok());
    // .is_error should NOT be set because "ERROR" != "error"
    assert!(event2.as_object().unwrap().get("is_error").is_none());
}

// =========================================================================
// Large batch stress test
// =========================================================================

#[test]
fn test_large_batch_1000_events() {
    let program = load_single("06_multiline_complex.vrl");
    let mut events: Vec<Value> = (0..1000)
        .map(|i| {
            Value::from(serde_json::json!({
                "message": format!("event {i}"),
                "status_code": if i % 10 == 0 { 500 } else { 200 },
                "response_time_ms": f64::from(i) * 0.5,
                "internal_trace_id": format!("trace-{i}"),
            }))
        })
        .collect();

    let results = run_vrl_batch(&program, &mut events);
    assert_eq!(results.len(), 1000);

    let successes = results.iter().filter(|(_, r)| r.is_ok()).count();
    assert_eq!(successes, 1000, "all 1000 events should succeed");

    // Spot check: internal_trace_id should be removed from all
    for event in &events {
        assert!(
            event
                .as_object()
                .unwrap()
                .get("internal_trace_id")
                .is_none()
        );
    }

    // Spot check: status categories
    let server_errors = events
        .iter()
        .filter(|e| {
            e.as_object()
                .unwrap()
                .get("status_category")
                .is_some_and(|v| v == &Value::from("server_error"))
        })
        .count();
    assert_eq!(
        server_errors, 100,
        "every 10th event should be server_error"
    );
}
