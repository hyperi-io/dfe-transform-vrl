// Project:   dfe-transform-vrl
// File:      tests/integration/vrl_deep.rs
// Purpose:   Deep VRL execution tests — complex programs, error paths, fuzz
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deep tests of VRL program execution against real event data.
//! Covers:
//! - Complex stdlib chains (parse_json, to_int, upcase, contains, etc.)
//! - Type coercion success and failure paths
//! - Conditional routing, filtering, aborts with and without messages
//! - Nested object traversal, array manipulation
//! - Runtime errors (divide by zero, bad regex, etc.)
//! - Batch execution with mixed success/failure
//! - Randomised fuzz inputs — programs must fail safely, never panic

use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::engine::runner::{run_vrl, run_vrl_batch};
use vrl::compiler::Program;
use vrl::value::Value;

fn compile(source: &str) -> Program {
    compile_vrl(source, None)
        .unwrap_or_else(|e| panic!("VRL compile failed for {source:?}: {e}"))
        .program
}

// ==========================================================================
// String manipulation — upcase / downcase / contains / split / replace
// ==========================================================================

#[test]
fn vrl_upcase_and_downcase_full_path() {
    let p = compile(".up = upcase!(.msg); .dn = downcase!(.msg)");
    let mut v = Value::from(serde_json::json!({"msg": "Hello World"}));
    run_vrl(&p, &mut v).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.get("up").unwrap(), &Value::from("HELLO WORLD"));
    assert_eq!(obj.get("dn").unwrap(), &Value::from("hello world"));
}

#[test]
fn vrl_contains_string_returns_true_and_false() {
    let p = compile(r#".hit = contains(string!(.msg), "world")"#);
    let mut v = Value::from(serde_json::json!({"msg": "hello world"}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("hit").unwrap(),
        &Value::Boolean(true)
    );

    let mut v2 = Value::from(serde_json::json!({"msg": "hello"}));
    run_vrl(&p, &mut v2).unwrap();
    assert_eq!(
        v2.as_object().unwrap().get("hit").unwrap(),
        &Value::Boolean(false)
    );
}

#[test]
fn vrl_split_and_array_indexing() {
    let p = compile(r#".parts = split(string!(.path), "/")"#);
    let mut v = Value::from(serde_json::json!({"path": "a/b/c/d"}));
    run_vrl(&p, &mut v).unwrap();
    let arr = v
        .as_object()
        .unwrap()
        .get("parts")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0], Value::from("a"));
    assert_eq!(arr[3], Value::from("d"));
}

#[test]
fn vrl_replace_substring() {
    let p = compile(r#".out = replace(string!(.msg), "foo", "bar")"#);
    let mut v = Value::from(serde_json::json!({"msg": "foo baz foo"}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("out").unwrap(),
        &Value::from("bar baz bar")
    );
}

#[test]
fn vrl_length_on_string_and_array() {
    let p = compile(".sl = length(string!(.s)); .al = length(array!(.a))");
    let mut v = Value::from(serde_json::json!({"s": "abcdef", "a": [1,2,3]}));
    run_vrl(&p, &mut v).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.get("sl").unwrap(), &Value::Integer(6));
    assert_eq!(obj.get("al").unwrap(), &Value::Integer(3));
}

// ==========================================================================
// Type coercion — to_int / to_float / to_bool / to_string
// ==========================================================================

#[test]
fn vrl_to_int_from_string_succeeds() {
    let p = compile(".n = to_int!(.s)");
    let mut v = Value::from(serde_json::json!({"s": "42"}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("n").unwrap(),
        &Value::Integer(42)
    );
}

#[test]
fn vrl_to_int_from_non_numeric_string_fails() {
    let p = compile(".n = to_int!(.s)");
    let mut v = Value::from(serde_json::json!({"s": "not a number"}));
    let err = run_vrl(&p, &mut v).unwrap_err();
    assert!(matches!(err, dfe_transform_vrl::Error::VrlRuntime(_)));
}

#[test]
fn vrl_to_int_with_fallback_recovers() {
    let p = compile(".n = to_int(.s) ?? 0");
    let mut v = Value::from(serde_json::json!({"s": "bad"}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(v.as_object().unwrap().get("n").unwrap(), &Value::Integer(0));
}

#[test]
fn vrl_to_float_succeeds_and_fails() {
    let p = compile(".f = to_float!(.v)");
    let mut ok = Value::from(serde_json::json!({"v": "3.14"}));
    run_vrl(&p, &mut ok).unwrap();
    assert!(ok.as_object().unwrap().get("f").unwrap().is_float());

    let mut bad = Value::from(serde_json::json!({"v": "abc"}));
    let err = run_vrl(&p, &mut bad).unwrap_err();
    assert!(matches!(err, dfe_transform_vrl::Error::VrlRuntime(_)));
}

#[test]
fn vrl_to_bool_coerces_common_strings() {
    let p = compile(".b = to_bool!(.v)");
    for (input, expected) in [("true", true), ("false", false), ("1", true), ("0", false)] {
        let mut v = Value::from(serde_json::json!({"v": input}));
        run_vrl(&p, &mut v).unwrap();
        assert_eq!(
            v.as_object().unwrap().get("b").unwrap(),
            &Value::Boolean(expected),
            "input {input} -> {expected}"
        );
    }
}

#[test]
fn vrl_to_string_on_various_types() {
    let p = compile(".s = to_string!(.n)");
    let mut int_v = Value::from(serde_json::json!({"n": 42}));
    run_vrl(&p, &mut int_v).unwrap();
    assert_eq!(
        int_v.as_object().unwrap().get("s").unwrap(),
        &Value::from("42")
    );

    let mut bool_v = Value::from(serde_json::json!({"n": true}));
    run_vrl(&p, &mut bool_v).unwrap();
    assert_eq!(
        bool_v.as_object().unwrap().get("s").unwrap(),
        &Value::from("true")
    );
}

// ==========================================================================
// Nested object traversal and path operations
// ==========================================================================

#[test]
fn vrl_deep_nested_path_read() {
    let p = compile(".name = .user.profile.name");
    let mut v = Value::from(serde_json::json!({
        "user": {"profile": {"name": "alice", "age": 30}}
    }));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("name").unwrap(),
        &Value::from("alice")
    );
}

#[test]
fn vrl_missing_nested_path_returns_null() {
    let p = compile(".x = .a.b.c");
    let mut v = Value::from(serde_json::json!({"a": {"x": 1}}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(v.as_object().unwrap().get("x").unwrap(), &Value::Null);
}

#[test]
fn vrl_set_deep_path_creates_nested_objects() {
    let p = compile(".a.b.c = \"deep\"");
    let mut v = Value::from(serde_json::json!({}));
    run_vrl(&p, &mut v).unwrap();
    let obj = v.as_object().unwrap();
    let a = obj.get("a").unwrap().as_object().unwrap();
    let b = a.get("b").unwrap().as_object().unwrap();
    assert_eq!(b.get("c").unwrap(), &Value::from("deep"));
}

#[test]
fn vrl_delete_field() {
    let p = compile("del(.secret)");
    let mut v = Value::from(serde_json::json!({"name": "bob", "secret": "password123"}));
    run_vrl(&p, &mut v).unwrap();
    let obj = v.as_object().unwrap();
    assert!(!obj.contains_key("secret"));
    assert!(obj.contains_key("name"));
}

#[test]
fn vrl_exists_check() {
    let p = compile(".has_x = exists(.x); .has_y = exists(.y)");
    let mut v = Value::from(serde_json::json!({"x": 1}));
    run_vrl(&p, &mut v).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.get("has_x").unwrap(), &Value::Boolean(true));
    assert_eq!(obj.get("has_y").unwrap(), &Value::Boolean(false));
}

// ==========================================================================
// Array operations
// ==========================================================================

#[test]
fn vrl_push_onto_array() {
    let p = compile(".tags = push(array!(.tags), \"new\")");
    let mut v = Value::from(serde_json::json!({"tags": ["a", "b"]}));
    run_vrl(&p, &mut v).unwrap();
    let arr = v
        .as_object()
        .unwrap()
        .get("tags")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[2], Value::from("new"));
}

#[test]
fn vrl_array_filter_keeps_matching() {
    let p = compile(
        r"
        .filtered = filter(array!(.items)) -> |_idx, item| {
            int!(item) > 5
        }
        ",
    );
    let mut v = Value::from(serde_json::json!({"items": [1, 5, 10, 3, 20]}));
    run_vrl(&p, &mut v).unwrap();
    let filtered = v
        .as_object()
        .unwrap()
        .get("filtered")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0], Value::Integer(10));
    assert_eq!(filtered[1], Value::Integer(20));
}

#[test]
fn vrl_array_map_transforms_each() {
    let p = compile(
        r"
        .doubled = map_values(array!(.nums)) -> |n| {
            int!(n) * 2
        }
        ",
    );
    let mut v = Value::from(serde_json::json!({"nums": [1, 2, 3]}));
    run_vrl(&p, &mut v).unwrap();
    let doubled = v
        .as_object()
        .unwrap()
        .get("doubled")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(
        doubled,
        &vec![Value::Integer(2), Value::Integer(4), Value::Integer(6)]
    );
}

// ==========================================================================
// Parse functions — parse_json, parse_syslog, parse_key_value
// ==========================================================================

#[test]
fn vrl_parse_json_from_string() {
    let p = compile(".data = parse_json!(string!(.raw))");
    let mut v = Value::from(serde_json::json!({
        "raw": r#"{"user": "alice", "count": 42}"#
    }));
    run_vrl(&p, &mut v).unwrap();
    let data = v
        .as_object()
        .unwrap()
        .get("data")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(data.get("user").unwrap(), &Value::from("alice"));
    assert_eq!(data.get("count").unwrap(), &Value::Integer(42));
}

#[test]
fn vrl_parse_json_invalid_fails_with_bang() {
    let p = compile(".data = parse_json!(string!(.raw))");
    let mut v = Value::from(serde_json::json!({"raw": "{not valid}"}));
    let err = run_vrl(&p, &mut v).unwrap_err();
    assert!(matches!(err, dfe_transform_vrl::Error::VrlRuntime(_)));
}

#[test]
fn vrl_parse_json_invalid_recovers_with_fallback() {
    let p = compile(r#".data = parse_json(string!(.raw)) ?? {"fallback": true}"#);
    let mut v = Value::from(serde_json::json!({"raw": "{broken"}));
    run_vrl(&p, &mut v).unwrap();
    let data = v
        .as_object()
        .unwrap()
        .get("data")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(data.get("fallback").unwrap(), &Value::Boolean(true));
}

#[test]
fn vrl_parse_key_value_standard_format() {
    let p = compile(".kv = parse_key_value!(string!(.line))");
    let mut v = Value::from(serde_json::json!({
        "line": r#"user=alice host=prod-01 code=200"#
    }));
    run_vrl(&p, &mut v).unwrap();
    let kv = v
        .as_object()
        .unwrap()
        .get("kv")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(kv.get("user").unwrap(), &Value::from("alice"));
    assert_eq!(kv.get("host").unwrap(), &Value::from("prod-01"));
}

// ==========================================================================
// Regex matching
// ==========================================================================

#[test]
fn vrl_regex_match_returns_bool() {
    let p = compile(r".is_ipv4 = match(string!(.ip), r'^\d+\.\d+\.\d+\.\d+$')");
    let mut v = Value::from(serde_json::json!({"ip": "192.168.1.1"}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("is_ipv4").unwrap(),
        &Value::Boolean(true)
    );

    let mut v2 = Value::from(serde_json::json!({"ip": "not an ip"}));
    run_vrl(&p, &mut v2).unwrap();
    assert_eq!(
        v2.as_object().unwrap().get("is_ipv4").unwrap(),
        &Value::Boolean(false)
    );
}

#[test]
fn vrl_regex_parse_captures_named_groups() {
    let p = compile(
        r"
        parsed = parse_regex!(string!(.line), r'^(?P<level>\w+) (?P<msg>.*)$')
        .level = parsed.level
        .msg = parsed.msg
        ",
    );
    let mut v = Value::from(serde_json::json!({"line": "ERROR something broke"}));
    run_vrl(&p, &mut v).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.get("level").unwrap(), &Value::from("ERROR"));
    assert_eq!(obj.get("msg").unwrap(), &Value::from("something broke"));
}

// ==========================================================================
// Conditional logic and abort
// ==========================================================================

#[test]
fn vrl_if_else_branches() {
    let p = compile(
        r#"
        if int!(.n) > 10 {
            .tier = "high"
        } else if int!(.n) > 5 {
            .tier = "medium"
        } else {
            .tier = "low"
        }
        "#,
    );
    for (n, expected) in [(1, "low"), (7, "medium"), (50, "high")] {
        let mut v = Value::from(serde_json::json!({"n": n}));
        run_vrl(&p, &mut v).unwrap();
        assert_eq!(
            v.as_object().unwrap().get("tier").unwrap(),
            &Value::from(expected),
            "n={n}"
        );
    }
}

#[test]
fn vrl_abort_with_reason() {
    let p = compile(r#"abort "this event is blocklisted""#);
    let mut v = Value::from(serde_json::json!({}));
    let err = run_vrl(&p, &mut v).unwrap_err();
    match err {
        dfe_transform_vrl::Error::VrlAbort(msg) => {
            assert!(msg.contains("blocklisted"), "abort message lost: {msg}");
        }
        other => panic!("expected VrlAbort, got {other:?}"),
    }
}

#[test]
fn vrl_conditional_abort() {
    let p = compile(r#"if string!(.level) == "debug" { abort "dropping debug" }"#);
    let mut debug = Value::from(serde_json::json!({"level": "debug"}));
    assert!(matches!(
        run_vrl(&p, &mut debug).unwrap_err(),
        dfe_transform_vrl::Error::VrlAbort(_)
    ));

    let mut info = Value::from(serde_json::json!({"level": "info"}));
    run_vrl(&p, &mut info).unwrap();
}

// ==========================================================================
// Numeric operations and edge cases
// ==========================================================================

#[test]
fn vrl_integer_arithmetic() {
    let p = compile(".sum = int!(.a) + int!(.b); .prod = int!(.a) * int!(.b)");
    let mut v = Value::from(serde_json::json!({"a": 3, "b": 4}));
    run_vrl(&p, &mut v).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.get("sum").unwrap(), &Value::Integer(7));
    assert_eq!(obj.get("prod").unwrap(), &Value::Integer(12));
}

#[test]
fn vrl_float_arithmetic() {
    let p = compile(".avg = (float!(.a) + float!(.b)) / 2.0");
    let mut v = Value::from(serde_json::json!({"a": 1.5, "b": 2.5}));
    run_vrl(&p, &mut v).unwrap();
    let avg = v
        .as_object()
        .unwrap()
        .get("avg")
        .unwrap()
        .as_float()
        .unwrap();
    assert!((avg - 2.0).abs() < 1e-9);
}

#[test]
fn vrl_modulo_operation() {
    let p = compile(".mod = mod(int!(.n), 3)");
    let mut v = Value::from(serde_json::json!({"n": 10}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("mod").unwrap(),
        &Value::Integer(1)
    );
}

#[test]
fn vrl_large_integer_does_not_overflow_silently() {
    let p = compile(".big = int!(.a) + int!(.b)");
    let mut v = Value::from(serde_json::json!({"a": i64::MAX, "b": 0}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("big").unwrap(),
        &Value::Integer(i64::MAX)
    );
}

// ==========================================================================
// Encoding / decoding
// ==========================================================================

#[test]
fn vrl_encode_base64() {
    let p = compile(".b64 = encode_base64(string!(.s))");
    let mut v = Value::from(serde_json::json!({"s": "hello"}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("b64").unwrap(),
        &Value::from("aGVsbG8=")
    );
}

#[test]
fn vrl_decode_base64_roundtrip() {
    let p = compile(".out = decode_base64!(string!(.in))");
    let mut v = Value::from(serde_json::json!({"in": "aGVsbG8="}));
    run_vrl(&p, &mut v).unwrap();
    // decode_base64 returns Bytes
    let out = v.as_object().unwrap().get("out").unwrap();
    let bytes = out.as_bytes().unwrap();
    assert_eq!(&bytes[..], b"hello");
}

#[test]
fn vrl_encode_json_and_parse_back() {
    let p = compile(
        r#"
        encoded = encode_json({"a": 1, "b": "two"})
        .decoded = parse_json!(encoded)
        "#,
    );
    let mut v = Value::from(serde_json::json!({}));
    run_vrl(&p, &mut v).unwrap();
    let decoded = v
        .as_object()
        .unwrap()
        .get("decoded")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(decoded.get("a").unwrap(), &Value::Integer(1));
    assert_eq!(decoded.get("b").unwrap(), &Value::from("two"));
}

// ==========================================================================
// Batch execution — mixed success/failure/abort semantics
// ==========================================================================

#[test]
fn vrl_batch_all_succeed() {
    let p = compile(".seen = true");
    let mut events: Vec<Value> = (0..20)
        .map(|i| Value::from(serde_json::json!({"id": i})))
        .collect();

    let results = run_vrl_batch(&p, &mut events);
    assert_eq!(results.len(), 20);
    assert!(results.iter().all(|(_, r)| r.is_ok()));
    assert!(
        events
            .iter()
            .all(|e| e.as_object().unwrap().get("seen") == Some(&Value::Boolean(true)))
    );
}

#[test]
fn vrl_batch_mixed_abort_error_success() {
    let p = compile(
        r#"
        if .kind == "drop" {
            abort
        }
        .name = upcase!(string!(.name))
        "#,
    );
    let mut events: Vec<Value> = vec![
        serde_json::json!({"kind": "keep", "name": "alice"}).into(),
        serde_json::json!({"kind": "drop", "name": "bob"}).into(),
        serde_json::json!({"kind": "keep", "name": 42}).into(), // runtime error
        serde_json::json!({"kind": "keep", "name": "carol"}).into(),
    ];

    let results = run_vrl_batch(&p, &mut events);
    assert_eq!(results.len(), 4);

    // Event 0 and 3: success
    assert!(results[0].1.is_ok());
    assert!(results[3].1.is_ok());

    // Event 1: abort
    assert!(matches!(
        &results[1].1,
        Err(dfe_transform_vrl::Error::VrlAbort(_))
    ));

    // Event 2: runtime error (string! on int)
    assert!(matches!(
        &results[2].1,
        Err(dfe_transform_vrl::Error::VrlRuntime(_))
    ));
}

#[test]
fn vrl_batch_empty_returns_empty() {
    let p = compile(".seen = true");
    let mut events: Vec<Value> = Vec::new();
    let results = run_vrl_batch(&p, &mut events);
    assert!(results.is_empty());
}

// ==========================================================================
// Unicode and edge-case strings
// ==========================================================================

#[test]
fn vrl_handles_unicode_values() {
    // VRL paths must be ASCII but values support full Unicode.
    let p = compile(r#".greeting_upper = upcase!(.greeting)"#);
    let mut v = Value::from(serde_json::json!({"greeting": "héllo wörld"}));
    run_vrl(&p, &mut v).unwrap();
    assert_eq!(
        v.as_object().unwrap().get("greeting_upper").unwrap(),
        &Value::from("HÉLLO WÖRLD")
    );
}

#[test]
fn vrl_empty_string_operations() {
    let p = compile(".len = length(string!(.s)); .up = upcase!(.s)");
    let mut v = Value::from(serde_json::json!({"s": ""}));
    run_vrl(&p, &mut v).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.get("len").unwrap(), &Value::Integer(0));
    assert_eq!(obj.get("up").unwrap(), &Value::from(""));
}

// ==========================================================================
// Random / fuzz-like inputs — must not panic
// ==========================================================================

/// A deterministic xorshift PRNG so tests are reproducible.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Generate a random event for fuzz testing.
fn fuzz_event(seed: &mut u64) -> Value {
    let kind = xorshift(seed) % 6;
    match kind {
        0 => Value::from(serde_json::json!({})),
        1 => Value::from(serde_json::json!({"n": xorshift(seed) as i64})),
        2 => Value::from(serde_json::json!({"s": format!("str-{}", xorshift(seed))})),
        3 => Value::from(serde_json::json!({"arr": [1, 2, 3, xorshift(seed) as i64]})),
        4 => Value::from(serde_json::json!({
            "nested": {"a": {"b": {"c": xorshift(seed) as i64}}}
        })),
        _ => Value::from(serde_json::json!({
            "mixed": xorshift(seed) as i64,
            "flag": xorshift(seed) % 2 == 0,
            "arr": [xorshift(seed) as i64, xorshift(seed) as i64]
        })),
    }
}

#[test]
fn vrl_fuzz_runtime_never_panics_with_fallible_program() {
    // Program touches many fields — most will fail with bang ops,
    // so we use ?? fallbacks everywhere to ensure we exercise both
    // success and failure paths without aborting.
    let p = compile(
        r#"
        .n_squared = (to_int(.n) ?? 0) * (to_int(.n) ?? 0)
        .s_upper = upcase(to_string(.s) ?? "")
        .arr_len = length(array(.arr) ?? [])
        .deep = to_int(.nested.a.b.c) ?? -1
        .flag_inverted = !(to_bool(.flag) ?? false)
        "#,
    );
    let mut seed: u64 = 0xdead_beef_cafe_babe;

    for _ in 0..500 {
        let mut event = fuzz_event(&mut seed);
        // Must never panic regardless of input shape
        let _ = run_vrl(&p, &mut event);
    }
}

#[test]
fn vrl_fuzz_with_nested_random_garbage() {
    // Program with chained fallible operations.
    let p = compile(
        r#"
        parts = split(to_string(.msg) ?? "", ",")
        .first = parts[0]
        .last = parts[-1]
        .count = length(parts)
        first_val = parts[0]
        .upper = if is_string(first_val) { upcase(string!(first_val)) } else { "" }
        "#,
    );
    let mut seed: u64 = 42;

    for _ in 0..200 {
        let r = xorshift(&mut seed);
        // Mix of values the program may or may not handle gracefully
        let mut event = match r % 5 {
            0 => Value::from(serde_json::json!({"msg": "a,b,c"})),
            1 => Value::from(serde_json::json!({"msg": ""})),
            2 => Value::from(serde_json::json!({"msg": 42})), // type mismatch but fallback
            3 => Value::from(serde_json::json!({"msg": null})),
            _ => Value::from(serde_json::json!({"other": r as i64})),
        };
        let _ = run_vrl(&p, &mut event);
    }
}

#[test]
fn vrl_fuzz_abort_is_caught_as_abort_error() {
    // Program that aborts for half the inputs
    let p = compile(
        r#"
        if (to_int(.n) ?? 0) > 100 {
            abort "too large"
        }
        .ok = true
        "#,
    );
    let mut seed: u64 = 99;
    let mut abort_count = 0u32;
    let mut ok_count = 0u32;

    for _ in 0..300 {
        let n = (xorshift(&mut seed) % 200) as i64;
        let mut event = Value::from(serde_json::json!({"n": n}));
        match run_vrl(&p, &mut event) {
            Ok(_) => ok_count += 1,
            Err(dfe_transform_vrl::Error::VrlAbort(_)) => abort_count += 1,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    assert!(abort_count > 0, "expected some aborts");
    assert!(ok_count > 0, "expected some passes");
}

// ==========================================================================
// Complex real-world chains
// ==========================================================================

#[test]
fn vrl_log_normalisation_pipeline() {
    // Simulates a realistic log-normalisation pipeline:
    // 1. parse JSON from raw message
    // 2. normalise level to uppercase
    // 3. drop debug events
    // 4. add correlation ID
    let p = compile(
        r#"
        parsed = parse_json!(string!(.raw))
        . = merge!(., parsed)
        .level = upcase(to_string(.level) ?? "UNKNOWN")
        if .level == "DEBUG" {
            abort "debug filtered"
        }
        .correlation_id = "trace-" + to_string!(int!(.timestamp))
        del(.raw)
        "#,
    );

    let mut event = Value::from(serde_json::json!({
        "raw": r#"{"timestamp": 1700000000, "level": "info", "msg": "hello"}"#
    }));
    run_vrl(&p, &mut event).unwrap();
    let obj = event.as_object().unwrap();
    assert_eq!(obj.get("level").unwrap(), &Value::from("INFO"));
    assert_eq!(obj.get("msg").unwrap(), &Value::from("hello"));
    assert!(
        obj.get("correlation_id")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("1700000000")
    );
    assert!(!obj.contains_key("raw"));

    // Debug event is dropped
    let mut debug = Value::from(serde_json::json!({
        "raw": r#"{"timestamp": 1, "level": "debug", "msg": "noisy"}"#
    }));
    assert!(matches!(
        run_vrl(&p, &mut debug).unwrap_err(),
        dfe_transform_vrl::Error::VrlAbort(_)
    ));
}

#[test]
fn vrl_metric_extraction_pipeline() {
    // Extract metrics from structured log: method, path, status, latency
    let p = compile(
        r#"
        .method = upcase(to_string(.http.method) ?? "UNKNOWN")
        .path = to_string(.http.path) ?? "/"
        .status = to_int(.http.status) ?? 0
        .latency_ms = to_float(.http.latency) ?? 0.0
        .is_error = .status >= 400
        .is_slow = .latency_ms > 1000.0
        "#,
    );

    let mut ok = Value::from(serde_json::json!({
        "http": {"method": "get", "path": "/api/users", "status": 200, "latency": 45}
    }));
    run_vrl(&p, &mut ok).unwrap();
    let obj = ok.as_object().unwrap();
    assert_eq!(obj.get("method").unwrap(), &Value::from("GET"));
    assert_eq!(obj.get("status").unwrap(), &Value::Integer(200));
    assert_eq!(obj.get("is_error").unwrap(), &Value::Boolean(false));
    assert_eq!(obj.get("is_slow").unwrap(), &Value::Boolean(false));

    let mut err = Value::from(serde_json::json!({
        "http": {"method": "post", "path": "/api/upload", "status": 503, "latency": 5000}
    }));
    run_vrl(&p, &mut err).unwrap();
    let obj = err.as_object().unwrap();
    assert_eq!(obj.get("is_error").unwrap(), &Value::Boolean(true));
    assert_eq!(obj.get("is_slow").unwrap(), &Value::Boolean(true));
}

#[test]
fn vrl_pii_masking_pipeline() {
    // Mask email addresses; replace card with last-4 notation
    let p = compile(
        r#"
        if exists(.email) {
            .email = "***MASKED***"
        }
        if exists(.card) {
            card_str = to_string!(.card)
            card_len = length(card_str)
            if card_len >= 4 {
                last4 = slice!(card_str, card_len - 4, card_len)
                .card = "****-****-****-" + last4
            } else {
                .card = "****"
            }
        }
        "#,
    );

    let mut event = Value::from(serde_json::json!({
        "user_id": 42,
        "email": "alice@example.com",
        "card": "1234567890123456"
    }));
    run_vrl(&p, &mut event).unwrap();
    let obj = event.as_object().unwrap();
    assert_eq!(obj.get("email").unwrap(), &Value::from("***MASKED***"));
    assert_eq!(obj.get("user_id").unwrap(), &Value::Integer(42));
    let card = obj.get("card").unwrap().as_str().unwrap();
    assert!(card.starts_with("****-****-****-"), "card: {card}");
    assert!(
        card.ends_with("3456"),
        "card should end with last 4: {card}"
    );
}

#[test]
fn vrl_chained_transformations_large_event() {
    // Exercise a longer pipeline on a realistic event
    let p = compile(
        r#"
        .processed_at = "2026-04-16T12:00:00Z"
        .host.name = downcase(to_string(.host.name) ?? "unknown")
        if !exists(.tags) {
            .tags = []
        }
        .tags = push(array!(.tags), "normalised")
        .tags = push(array!(.tags), "v2")
        errors = to_int(.counters.errors) ?? 0
        if errors > 5 {
            .alert = true
        } else {
            .alert = false
        }
        "#,
    );

    let mut event = Value::from(serde_json::json!({
        "host": {"name": "PROD-WEB-01"},
        "tags": ["existing"],
        "counters": {"errors": 10}
    }));
    run_vrl(&p, &mut event).unwrap();
    let obj = event.as_object().unwrap();
    assert_eq!(obj.get("alert").unwrap(), &Value::Boolean(true));
    let host = obj.get("host").unwrap().as_object().unwrap();
    assert_eq!(host.get("name").unwrap(), &Value::from("prod-web-01"));
    let tags = obj.get("tags").unwrap().as_array().unwrap();
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0], Value::from("existing"));
    assert_eq!(tags[1], Value::from("normalised"));
    assert_eq!(tags[2], Value::from("v2"));
}

// ==========================================================================
// Compilation failure paths (expected fails)
// ==========================================================================

#[test]
fn vrl_compile_fails_for_syntax_error() {
    let result = compile_vrl(".x = ((", None);
    assert!(result.is_err());
}

#[test]
fn vrl_compile_fails_for_unknown_function() {
    let result = compile_vrl(".x = definitely_not_a_real_function(1)", None);
    assert!(result.is_err());
}

#[test]
fn vrl_compile_fails_for_unclosed_block() {
    let result = compile_vrl("if .x == 1 { .y = 2", None);
    assert!(result.is_err());
}

#[test]
fn vrl_compile_fails_for_invalid_regex() {
    let result = compile_vrl(r".match = match(string!(.s), r'[unclosed')", None);
    assert!(result.is_err());
}

#[test]
fn vrl_compile_fails_for_bad_assignment() {
    let result = compile_vrl("123 = .x", None);
    assert!(result.is_err());
}

// ==========================================================================
// Scale — large event, large batch
// ==========================================================================

#[test]
fn vrl_large_event_2000_fields() {
    let p = compile(".big.processed = true");
    let mut obj = serde_json::Map::new();
    for i in 0..2000 {
        obj.insert(format!("field_{i}"), serde_json::Value::from(i));
    }
    let big = serde_json::json!({"big": serde_json::Value::Object(obj)});
    let mut value = Value::from(big);
    run_vrl(&p, &mut value).unwrap();

    let big_obj = value
        .as_object()
        .unwrap()
        .get("big")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(big_obj.get("processed").unwrap(), &Value::Boolean(true));
    // All original fields preserved
    assert!(big_obj.contains_key("field_0"));
    assert!(big_obj.contains_key("field_1999"));
}

#[test]
fn vrl_batch_5000_events_performance() {
    let p = compile(
        r#"
        .level = upcase(to_string(.level) ?? "INFO")
        .processed = true
        "#,
    );
    let mut events: Vec<Value> = (0..5000)
        .map(|i| {
            Value::from(serde_json::json!({
                "id": i,
                "level": if i % 3 == 0 { "error" } else { "info" }
            }))
        })
        .collect();

    let start = std::time::Instant::now();
    let results = run_vrl_batch(&p, &mut events);
    let elapsed = start.elapsed();

    assert_eq!(results.len(), 5000);
    assert!(results.iter().all(|(_, r)| r.is_ok()));
    // Sanity: should complete in a reasonable time
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "5000 events took too long: {elapsed:?}"
    );
}
