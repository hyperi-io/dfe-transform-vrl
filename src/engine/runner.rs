// Project:   dfe-transform-vrl
// File:      src/engine/runner.rs
// Purpose:   VRL program execution against events
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! VRL program execution.
//!
//! Runs compiled VRL programs against event Values in-process.
//! Each event is wrapped in a `TargetValueRef` and passed to the VRL runtime.

use vrl::compiler::{Context, ExpressionError, Program, TargetValueRef, TimeZone};
use vrl::prelude::state::RuntimeState;
use vrl::value::{Secrets, Value};

use crate::Result;

/// Run a compiled VRL program against a single event value.
///
/// Mutates the value in place. Returns the VRL program's return value
/// (which is typically the last expression result, not the event itself).
#[inline]
pub fn run_vrl(program: &Program, value: &mut Value) -> Result<Value> {
    let mut metadata = Value::Object(std::collections::BTreeMap::default());
    let mut secrets = Secrets::default();
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();

    let mut target = TargetValueRef {
        value,
        metadata: &mut metadata,
        secrets: &mut secrets,
    };

    let mut ctx = Context::new(&mut target, &mut state, &timezone);

    program.resolve(&mut ctx).map_err(|e| match e {
        ExpressionError::Abort { message, .. } => {
            crate::Error::VrlAbort(message.unwrap_or_else(|| "aborted".into()))
        }
        other => crate::Error::VrlRuntime(format!("{other}")),
    })
}

/// Transform a batch of events through a compiled VRL program.
///
/// Returns a vec of (index, result) pairs. Failed events are collected
/// as errors rather than stopping the batch — the caller decides whether
/// to skip, DLQ, or abort.
pub fn run_vrl_batch(
    program: &Program,
    events: &mut [Value],
) -> Vec<(usize, std::result::Result<Value, crate::Error>)> {
    let mut results = Vec::with_capacity(events.len());

    for (idx, event) in events.iter_mut().enumerate() {
        let result = run_vrl(program, event);
        results.push((idx, result));
    }

    results
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::default_trait_access)]
mod tests {
    use super::*;
    use crate::engine::compiler::compile_vrl;

    fn compile_test_program(source: &str) -> Program {
        compile_vrl(source, None).unwrap().program
    }

    #[test]
    fn test_run_simple_assignment() {
        let program = compile_test_program(".processed = true");
        let mut value = Value::Object(Default::default());
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());

        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("processed"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_run_field_manipulation() {
        let program = compile_test_program(
            r"
            .upper = upcase!(.name)
            del(.name)
        ",
        );
        let mut value = Value::from(serde_json::json!({"name": "hello"}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());

        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("upper"), Some(&Value::from("HELLO")));
        assert!(obj.get("name").is_none());
    }

    #[test]
    fn test_run_batch() {
        let program = compile_test_program(".count = int!(.count) + 1");
        let mut events: Vec<Value> = (0..5)
            .map(|i| Value::from(serde_json::json!({"count": i})))
            .collect();

        let results = run_vrl_batch(&program, &mut events);
        assert_eq!(results.len(), 5);
        for (_, result) in &results {
            assert!(result.is_ok());
        }

        for (i, event) in events.iter().enumerate() {
            let count = event
                .as_object()
                .and_then(|o| o.get("count"))
                .and_then(vrl::value::Value::as_integer);
            assert_eq!(count, Some(i64::try_from(i + 1).unwrap()));
        }
    }

    #[test]
    fn test_run_vrl_with_stdlib() {
        let program = compile_test_program(".ts = now()");
        let mut value = Value::Object(Default::default());
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());
        assert!(value.as_object().unwrap().get("ts").is_some());
    }

    #[test]
    fn test_run_vrl_runtime_error() {
        let program = compile_test_program(
            r"
            .parsed = parse_json!(.raw)
        ",
        );
        let mut value = Value::from(serde_json::json!({"raw": "not json {{{"}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_err());
    }

    // --- Edge case: empty object ---

    #[test]
    fn test_run_empty_object() {
        let program = compile_test_program(".processed = true");
        let mut value = Value::Object(Default::default());
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());
        assert_eq!(
            value.as_object().unwrap().get("processed"),
            Some(&Value::Boolean(true))
        );
    }

    // --- Edge case: null field access ---

    #[test]
    fn test_run_null_field_access() {
        let program = compile_test_program(
            r"
            if .missing == null {
                .was_null = true
            }
        ",
        );
        let mut value = Value::from(serde_json::json!({"other": 1}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());
        assert_eq!(
            value.as_object().unwrap().get("was_null"),
            Some(&Value::Boolean(true))
        );
    }

    // --- Edge case: deeply nested object ---

    #[test]
    fn test_run_deeply_nested_object() {
        let program = compile_test_program(".a.b.c.d.e = 42");
        let mut value = Value::Object(Default::default());
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());

        let nested = value
            .as_object()
            .unwrap()
            .get("a")
            .unwrap()
            .as_object()
            .unwrap()
            .get("b")
            .unwrap()
            .as_object()
            .unwrap()
            .get("c")
            .unwrap()
            .as_object()
            .unwrap()
            .get("d")
            .unwrap()
            .as_object()
            .unwrap()
            .get("e");
        assert_eq!(nested, Some(&Value::Integer(42)));
    }

    // --- Edge case: array manipulation ---

    #[test]
    fn test_run_array_push() {
        let program = compile_test_program(
            r#"
            .tags = push!(.tags, "new_tag")
        "#,
        );
        let mut value = Value::from(serde_json::json!({"tags": ["a", "b"]}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());

        let tags = value
            .as_object()
            .unwrap()
            .get("tags")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[2], Value::from("new_tag"));
    }

    #[test]
    fn test_run_array_filter() {
        let program = compile_test_program(
            r#"
            items = array!(.items)
            .items = filter(items) -> |_index, value| { value != "remove_me" }
        "#,
        );
        let mut value =
            Value::from(serde_json::json!({"items": ["keep", "remove_me", "also_keep"]}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());

        let items = value
            .as_object()
            .unwrap()
            .get("items")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(items.len(), 2);
        assert!(!items.contains(&Value::from("remove_me")));
    }

    // --- Edge case: unicode fields ---

    #[test]
    fn test_run_unicode_values() {
        let program = compile_test_program(".upper = upcase!(.name)");
        let mut value = Value::from(serde_json::json!({"name": "héllo wörld"}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());
        assert_eq!(
            value.as_object().unwrap().get("upper"),
            Some(&Value::from("HÉLLO WÖRLD"))
        );
    }

    #[test]
    fn test_run_unicode_keys() {
        let program = compile_test_program(r#".output = get!(., ["日本語"])"#);
        let mut value = Value::from(serde_json::json!({"日本語": "value"}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());
        assert_eq!(
            value.as_object().unwrap().get("output"),
            Some(&Value::from("value"))
        );
    }

    // --- Edge case: type coercion failures ---

    #[test]
    fn test_run_string_bang_on_integer_fails() {
        let program = compile_test_program(".out = string!(.count)");
        let mut value = Value::from(serde_json::json!({"count": 42}));
        let result = run_vrl(&program, &mut value);
        assert!(
            result.is_err(),
            "string!() on integer should fail at runtime"
        );
    }

    #[test]
    fn test_run_int_bang_on_string_fails() {
        let program = compile_test_program(".out = int!(.name)");
        let mut value = Value::from(serde_json::json!({"name": "not_a_number"}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_err(), "int!() on string should fail at runtime");
    }

    #[test]
    fn test_run_type_coercion_with_fallback() {
        let program = compile_test_program(
            r#"
            .out = to_string(.count) ?? "unknown"
        "#,
        );
        let mut value = Value::from(serde_json::json!({"count": 42}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());
        assert_eq!(
            value.as_object().unwrap().get("out"),
            Some(&Value::from("42"))
        );
    }

    // --- Edge case: abort semantics ---

    #[test]
    fn test_run_abort_produces_error() {
        let program = compile_test_program(r#"abort "event rejected""#);
        let mut value = Value::from(serde_json::json!({"message": "test"}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_err(), "abort should produce a runtime error");
    }

    #[test]
    fn test_run_conditional_abort() {
        let program = compile_test_program(
            r#"
            if .level == "FATAL" {
                abort "fatal events dropped"
            }
            .processed = true
        "#,
        );

        // Non-fatal: should succeed
        let mut ok_event = Value::from(serde_json::json!({"level": "INFO"}));
        assert!(run_vrl(&program, &mut ok_event).is_ok());
        assert_eq!(
            ok_event.as_object().unwrap().get("processed"),
            Some(&Value::Boolean(true))
        );

        // Fatal: should abort
        let mut fatal_event = Value::from(serde_json::json!({"level": "FATAL"}));
        assert!(
            run_vrl(&program, &mut fatal_event).is_err(),
            "FATAL level should trigger abort"
        );
    }

    // --- Edge case: batch with partial failures ---

    #[test]
    fn test_batch_partial_failures() {
        let program = compile_test_program(
            r"
            .parsed = parse_json!(.raw)
        ",
        );
        let mut events = vec![
            Value::from(serde_json::json!({"raw": r#"{"valid": true}"#})),
            Value::from(serde_json::json!({"raw": "not json"})),
            Value::from(serde_json::json!({"raw": r#"{"also": "valid"}"#})),
            Value::from(serde_json::json!({"raw": "{broken"})),
            Value::from(serde_json::json!({"raw": r#"{"ok": 1}"#})),
        ];

        let results = run_vrl_batch(&program, &mut events);
        assert_eq!(results.len(), 5);

        assert!(results[0].1.is_ok(), "valid JSON should pass");
        assert!(results[1].1.is_err(), "invalid JSON should fail");
        assert!(results[2].1.is_ok(), "valid JSON should pass");
        assert!(results[3].1.is_err(), "broken JSON should fail");
        assert!(results[4].1.is_ok(), "valid JSON should pass");

        // Verify successful events were mutated
        assert!(events[0].as_object().unwrap().get("parsed").is_some());
        assert!(events[2].as_object().unwrap().get("parsed").is_some());
        assert!(events[4].as_object().unwrap().get("parsed").is_some());
    }

    #[test]
    fn test_batch_abort_does_not_stop_others() {
        let program = compile_test_program(
            r#"
            if .drop == true { abort "dropped" }
            .kept = true
        "#,
        );
        let mut events = vec![
            Value::from(serde_json::json!({"drop": false, "id": 1})),
            Value::from(serde_json::json!({"drop": true, "id": 2})),
            Value::from(serde_json::json!({"drop": false, "id": 3})),
        ];

        let results = run_vrl_batch(&program, &mut events);
        assert!(results[0].1.is_ok());
        assert!(results[1].1.is_err(), "abort event should error");
        assert!(results[2].1.is_ok());

        assert_eq!(
            events[0].as_object().unwrap().get("kept"),
            Some(&Value::Boolean(true))
        );
        assert_eq!(
            events[2].as_object().unwrap().get("kept"),
            Some(&Value::Boolean(true))
        );
    }

    // --- Edge case: large event ---

    #[test]
    fn test_run_large_event() {
        let program = compile_test_program(".field_count = length(.)");
        let mut obj = serde_json::Map::new();
        for i in 0..500 {
            obj.insert(format!("field_{i}"), serde_json::Value::from(i));
        }
        let mut value = Value::from(serde_json::Value::Object(obj));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());

        let count = value
            .as_object()
            .unwrap()
            .get("field_count")
            .unwrap()
            .as_integer()
            .unwrap();
        // length(.) is evaluated before assignment, so it counts the 500 original fields
        assert_eq!(count, 500);
    }

    // --- Edge case: event is not an object ---

    #[test]
    fn test_run_non_object_value() {
        let program = compile_test_program(". = to_string(.) ?? \"fallback\"");
        let mut value = Value::Integer(42);
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());
    }

    // --- Edge case: del on missing field is no-op ---

    #[test]
    fn test_run_del_missing_field_is_noop() {
        let program = compile_test_program(
            r"
            del(.nonexistent)
            .survived = true
        ",
        );
        let mut value = Value::from(serde_json::json!({"message": "hello"}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());
        assert_eq!(
            value.as_object().unwrap().get("survived"),
            Some(&Value::Boolean(true))
        );
        assert_eq!(
            value.as_object().unwrap().get("message"),
            Some(&Value::from("hello"))
        );
    }

    // --- Edge case: multiple transforms chained ---

    #[test]
    fn test_run_chained_transforms() {
        let program = compile_test_program(
            r#"
            .step1 = "done"
            .message = downcase!(string!(.message))
            .step2 = "done"
            .message = replace(.message, "world", "vrl")
            .step3 = "done"
        "#,
        );
        let mut value = Value::from(serde_json::json!({"message": "Hello WORLD"}));
        let result = run_vrl(&program, &mut value);
        assert!(result.is_ok());

        let obj = value.as_object().unwrap();
        assert_eq!(obj.get("message"), Some(&Value::from("hello vrl")));
        assert_eq!(obj.get("step1"), Some(&Value::from("done")));
        assert_eq!(obj.get("step2"), Some(&Value::from("done")));
        assert_eq!(obj.get("step3"), Some(&Value::from("done")));
    }
}
