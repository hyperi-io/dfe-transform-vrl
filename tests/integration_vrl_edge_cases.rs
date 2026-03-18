// Project:   dfe-transform-vrl
// File:      tests/integration_vrl_edge_cases.rs
// Purpose:   Edge case and failure mode tests for VRL transforms
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Edge case tests: type coercion failures, abort semantics, null handling,
//! partial batch failures, empty/malformed inputs, and known-should-fail patterns.

#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use dfe_transform_vrl::engine::compiler::compile_vrl;
    use dfe_transform_vrl::engine::runner::{run_vrl, run_vrl_batch};
    use vrl::value::Value;

    fn compile(source: &str) -> vrl::compiler::Program {
        compile_vrl(source).unwrap().program
    }

    // =========================================================================
    // Known-should-fail: compilation errors
    // =========================================================================

    #[test]
    fn test_compile_fails_on_syntax_error() {
        assert!(compile_vrl("if { broken").is_err());
    }

    #[test]
    fn test_compile_fails_on_unknown_function() {
        assert!(compile_vrl(".x = this_function_does_not_exist()").is_err());
    }

    #[test]
    fn test_compile_fails_on_unclosed_string() {
        assert!(compile_vrl(r#".x = "unclosed"#).is_err());
    }

    #[test]
    fn test_compile_empty_source_fails() {
        let result = compile_vrl("");
        if let Ok(cr) = result {
            let mut value = Value::from(serde_json::json!({"a": 1}));
            let _ = run_vrl(&cr.program, &mut value);
            assert_eq!(
                value.as_object().unwrap().get("a"),
                Some(&Value::Integer(1))
            );
        }
    }

    // =========================================================================
    // Known-should-fail: runtime errors
    // =========================================================================

    #[test]
    fn test_runtime_fail_parse_json_invalid() {
        let program = compile(r".parsed = parse_json!(.raw)");
        let mut value = Value::from(serde_json::json!({"raw": "{{not json}}"}));
        assert!(run_vrl(&program, &mut value).is_err());
    }

    #[test]
    fn test_runtime_fail_to_int_non_numeric() {
        let program = compile(r".num = int!(.val)");
        let mut value = Value::from(serde_json::json!({"val": "not_a_number"}));
        assert!(run_vrl(&program, &mut value).is_err());
    }

    #[test]
    fn test_runtime_fail_array_bang_on_object() {
        let program = compile(r".out = array!(.data)");
        let mut value = Value::from(serde_json::json!({"data": {"key": "val"}}));
        assert!(run_vrl(&program, &mut value).is_err());
    }

    #[test]
    fn test_runtime_divide_by_zero_captured_as_err() {
        let program = compile(
            r"
            divisor = int!(.divisor)
            .result, .div_err = 10 / divisor
        ",
        );

        // Non-zero: should succeed (VRL division returns float)
        let mut ok = Value::from(serde_json::json!({"divisor": 2}));
        assert!(run_vrl(&program, &mut ok).is_ok());
        let result = ok
            .as_object()
            .unwrap()
            .get("result")
            .unwrap()
            .as_float()
            .unwrap();
        assert!((result - 5.0).abs() < f64::EPSILON);

        // Zero: err should be populated
        let mut zero = Value::from(serde_json::json!({"divisor": 0}));
        assert!(run_vrl(&program, &mut zero).is_ok());
        assert!(
            zero.as_object().unwrap().get("div_err").is_some(),
            "divide by zero should set error value"
        );
    }

    // =========================================================================
    // Null and missing field handling
    // =========================================================================

    #[test]
    fn test_null_value_field() {
        let program = compile(
            r#"
            if .field == null {
                .field = "default_value"
            }
        "#,
        );
        let mut value = Value::from(serde_json::json!({"field": null}));
        assert!(run_vrl(&program, &mut value).is_ok());
        assert_eq!(
            value.as_object().unwrap().get("field"),
            Some(&Value::from("default_value"))
        );
    }

    #[test]
    fn test_missing_field_is_null() {
        let program = compile(
            r"
            .present = exists(.there)
            .absent = exists(.not_there)
        ",
        );
        let mut value = Value::from(serde_json::json!({"there": 1}));
        assert!(run_vrl(&program, &mut value).is_ok());
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("present"), Some(&Value::Boolean(true)));
        assert_eq!(obj.get("absent"), Some(&Value::Boolean(false)));
    }

    #[test]
    fn test_nested_missing_field() {
        let program = compile(
            r"
            .deep_exists = exists(.a.b.c.d)
        ",
        );
        let mut value = Value::from(serde_json::json!({"a": {"b": {}}}));
        assert!(run_vrl(&program, &mut value).is_ok());
        assert_eq!(
            value.as_object().unwrap().get("deep_exists"),
            Some(&Value::Boolean(false))
        );
    }

    #[test]
    fn test_all_null_fields() {
        let program = compile(
            r#"
            if .a == null { .a = "default_a" }
            if .b == null { .b = "default_b" }
            if .c == null { .c = "default_c" }
        "#,
        );
        let mut value = Value::from(serde_json::json!({"a": null, "b": null}));
        assert!(run_vrl(&program, &mut value).is_ok());
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("a"), Some(&Value::from("default_a")));
        assert_eq!(obj.get("b"), Some(&Value::from("default_b")));
        assert_eq!(obj.get("c"), Some(&Value::from("default_c")));
    }

    // =========================================================================
    // Abort semantics
    // =========================================================================

    #[test]
    fn test_abort_plain() {
        let program = compile("abort");
        let mut value = Value::from(serde_json::json!({"keep": true}));
        assert!(run_vrl(&program, &mut value).is_err());
    }

    #[test]
    fn test_abort_with_message() {
        let program = compile(r#"abort "this event should be dropped""#);
        let mut value = Value::from(serde_json::json!({"a": 1}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_err());
    }

    #[test]
    fn test_abort_conditional_pass_through() {
        let program = compile(
            r#"
            if .internal == true {
                abort "internal events dropped"
            }
            .external = true
        "#,
        );

        let mut internal = Value::from(serde_json::json!({"internal": true, "msg": "test"}));
        assert!(run_vrl(&program, &mut internal).is_err());

        let mut external = Value::from(serde_json::json!({"internal": false, "msg": "test"}));
        assert!(run_vrl(&program, &mut external).is_ok());
        assert_eq!(
            external.as_object().unwrap().get("external"),
            Some(&Value::Boolean(true))
        );
    }

    // =========================================================================
    // Batch with mixed success/failure/abort
    // =========================================================================

    #[test]
    fn test_batch_mixed_abort_error_success() {
        let program = compile(
            r#"
            if .action == "drop" {
                abort "dropped by policy"
            }
            .parsed = parse_json!(.data)
        "#,
        );

        let mut events = vec![
            Value::from(serde_json::json!({"action": "keep", "data": r#"{"ok":1}"#})),
            Value::from(serde_json::json!({"action": "drop", "data": r#"{"ok":2}"#})),
            Value::from(serde_json::json!({"action": "keep", "data": "not json"})),
            Value::from(serde_json::json!({"action": "keep", "data": r#"{"ok":3}"#})),
        ];

        let results = run_vrl_batch(&program, &mut events);
        assert_eq!(results.len(), 4);
        assert!(results[0].1.is_ok(), "event 0 should succeed");
        assert!(results[1].1.is_err(), "event 1 should abort");
        assert!(results[2].1.is_err(), "event 2 should fail parse");
        assert!(results[3].1.is_ok(), "event 3 should succeed");
    }

    #[test]
    fn test_batch_all_fail() {
        let program = compile("abort");
        let mut events = vec![
            Value::from(serde_json::json!({"id": 1})),
            Value::from(serde_json::json!({"id": 2})),
            Value::from(serde_json::json!({"id": 3})),
        ];

        let results = run_vrl_batch(&program, &mut events);
        assert!(results.iter().all(|(_, r)| r.is_err()));
    }

    #[test]
    fn test_batch_empty() {
        let program = compile(".x = 1");
        let mut events: Vec<Value> = vec![];
        let results = run_vrl_batch(&program, &mut events);
        assert!(results.is_empty());
    }

    // =========================================================================
    // Edge data types
    // =========================================================================

    #[test]
    fn test_boolean_field_manipulation() {
        let program = compile(
            r#"
            .negated = !bool!(.flag)
            .as_string = to_string(.flag) ?? "unknown"
        "#,
        );
        let mut value = Value::from(serde_json::json!({"flag": true}));
        assert!(run_vrl(&program, &mut value).is_ok());
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("negated"), Some(&Value::Boolean(false)));
        assert_eq!(obj.get("as_string"), Some(&Value::from("true")));
    }

    #[test]
    fn test_float_precision() {
        let program = compile(".result = float!(.a) + float!(.b)");
        let mut value = Value::from(serde_json::json!({"a": 0.1, "b": 0.2}));
        assert!(run_vrl(&program, &mut value).is_ok());
        let result = value
            .as_object()
            .unwrap()
            .get("result")
            .unwrap()
            .as_float()
            .unwrap();
        assert!((result - 0.3).abs() < 1e-10);
    }

    #[test]
    fn test_large_integer() {
        let program = compile(".big = int!(.val) * 2");
        let mut value = Value::from(serde_json::json!({"val": 9_007_199_254_740_992_i64}));
        assert!(run_vrl(&program, &mut value).is_ok());
        assert_eq!(
            value.as_object().unwrap().get("big").unwrap().as_integer(),
            Some(18_014_398_509_481_984_i64)
        );
    }

    #[test]
    fn test_nested_array_of_objects() {
        let program = compile(
            r#"
            .count = length!(array!(.items))
            .first_name = get!(array!(.items)[0], ["name"])
        "#,
        );
        let mut value = Value::from(serde_json::json!({
            "items": [
                {"name": "alice", "age": 30},
                {"name": "bob", "age": 25}
            ]
        }));
        assert!(run_vrl(&program, &mut value).is_ok());
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("count"), Some(&Value::Integer(2)));
        assert_eq!(obj.get("first_name"), Some(&Value::from("alice")));
    }

    #[test]
    fn test_empty_string_handling() {
        let program = compile(
            r#"
            .is_empty = (string!(.message) == "")
            .has_length = length!(string!(.message)) > 0
        "#,
        );
        let mut value = Value::from(serde_json::json!({"message": ""}));
        assert!(run_vrl(&program, &mut value).is_ok());
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("is_empty"), Some(&Value::Boolean(true)));
        assert_eq!(obj.get("has_length"), Some(&Value::Boolean(false)));
    }

    // =========================================================================
    // Fallible vs infallible function variants
    // =========================================================================

    #[test]
    fn test_fallible_function_with_error_handling() {
        let program = compile(
            r"
            parsed, err = parse_json(.raw)
            if err != null {
                .parse_failed = true
            } else {
                .parsed = parsed
            }
        ",
        );

        let mut good = Value::from(serde_json::json!({"raw": r#"{"ok":true}"#}));
        assert!(run_vrl(&program, &mut good).is_ok());
        assert!(good.as_object().unwrap().get("parsed").is_some());
        assert!(good.as_object().unwrap().get("parse_failed").is_none());

        let mut bad = Value::from(serde_json::json!({"raw": "broken{json"}));
        assert!(run_vrl(&program, &mut bad).is_ok());
        assert_eq!(
            bad.as_object().unwrap().get("parse_failed"),
            Some(&Value::Boolean(true))
        );
    }

    #[test]
    fn test_infallible_function_aborts_on_error() {
        let program = compile(r".parsed = parse_json!(.raw)");
        let mut value = Value::from(serde_json::json!({"raw": "not json"}));
        assert!(
            run_vrl(&program, &mut value).is_err(),
            "infallible parse_json! should abort on bad input"
        );
    }

    // =========================================================================
    // Idempotency — running same transform twice
    // =========================================================================

    #[test]
    fn test_idempotent_transform() {
        let program = compile(
            r"
            .level = downcase(.level) ?? .level
            .processed = true
            del(.temp)
        ",
        );

        let mut value = Value::from(serde_json::json!({
            "level": "ERROR",
            "temp": "remove_me"
        }));

        assert!(run_vrl(&program, &mut value).is_ok());
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("level"), Some(&Value::from("error")));
        assert_eq!(obj.get("processed"), Some(&Value::Boolean(true)));
        assert!(obj.get("temp").is_none());

        assert!(run_vrl(&program, &mut value).is_ok());
        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("level"), Some(&Value::from("error")));
        assert_eq!(obj.get("processed"), Some(&Value::Boolean(true)));
    }
}
