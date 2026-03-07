// Project:   dfe-transform-vrl
// File:      src/engine/runner.rs
// Purpose:   VRL program execution against events
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! VRL program execution.
//!
//! Runs compiled VRL programs against event Values in-process.
//! Each event is wrapped in a `TargetValueRef` and passed to the VRL runtime.

use vrl::compiler::{Context, Program, TargetValueRef, TimeZone};
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

    program
        .resolve(&mut ctx)
        .map_err(|e| crate::Error::VrlRuntime(format!("{e}")))
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
        compile_vrl(source).unwrap().program
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
}
