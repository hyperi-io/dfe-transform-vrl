// Project:   dfe-transform-vrl
// File:      tests/integration/filebeat_pipeline.rs
// Purpose:   Integration tests for the bundled filebeat-compat pipeline
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the bundled INTERIM filebeat-compat pipeline
//! (`pipelines/filebeat/filebeat.vrl`), ported from the DFE 2.1 Vector
//! templates (dfe-vector-templates 14x family).
//!
//! The case tests drive the pipeline with the synthetic lines in
//! `tests/fixtures/filebeat/cases.yaml` and check each output against the
//! subset of fields its case names.
//!
//! This runs the pipeline IN PROCESS. `tests/e2e/filebeat_kafka.rs` runs the
//! same cases through the app binary and a real broker; both grade with the
//! shared comparison in `tests/common/filebeat.rs`.

use std::sync::OnceLock;

use dfe_transform_vrl::config::EnrichmentTableConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::engine::runner::run_vrl;
use dfe_transform_vrl::enrichment::EnrichmentRegistry;
use vrl::compiler::Program;
use vrl::value::Value;

use crate::filebeat_cases as fb;

/// Compile the bundled pipeline with the bundled timezones table.
///
/// Compiled once per process (the 5.5K-line program takes ~1min in debug)
/// and shared by every test in this module.
fn compile_filebeat() -> &'static Program {
    static PROGRAM: OnceLock<Program> = OnceLock::new();
    PROGRAM.get_or_init(|| {
        let source = std::fs::read_to_string(fb::repo_path("pipelines/filebeat/filebeat.vrl"))
            .expect("bundled filebeat.vrl must exist");
        let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
            name: "timezones".into(),
            path: fb::repo_path("pipelines/filebeat/timezones.csv")
                .to_string_lossy()
                .into_owned(),
            key_columns: vec!["abbreviation".into()],
            ..Default::default()
        }])
        .expect("bundled timezones.csv must load")
        .into_arc();

        match compile_vrl(&source, Some(registry)) {
            Ok(result) => result.program,
            Err(e) => panic!("bundled filebeat.vrl failed to compile:\n{e}"),
        }
    })
}

fn event_with_conf(message: &str, conf: Option<&serde_json::Value>) -> Value {
    Value::from(fb::input_event(message, conf))
}

fn event(message: &str, tz_offset: Option<&str>) -> Value {
    let conf = tz_offset.map(|tz| serde_json::json!({ "tz_offset": tz }));
    event_with_conf(message, conf.as_ref())
}

fn field<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = value;
    for part in path.split('.') {
        cur = cur.as_object()?.get(part)?;
    }
    Some(cur)
}

fn str_field(value: &Value, path: &str) -> Option<String> {
    field(value, path).map(|v| match v {
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => other.to_string(),
    })
}

// =========================================================================
// Compile
// =========================================================================

#[test]
fn bundled_filebeat_pipeline_compiles() {
    let _program = compile_filebeat();
}

// =========================================================================
// Module detection smoke tests (one line per module)
// =========================================================================

fn meraki_line() -> &'static str {
    &fb::case("meraki-flows-allow").line
}

fn ios_line() -> &'static str {
    &fb::case("ios-acl-host-header-tcp").line
}

fn umbrella_line() -> &'static str {
    &fb::case("umbrella-dns-allowed").line
}

const UNMATCHED_LINE: &str = "completely unrecognisable payload";

#[test]
fn detects_and_parses_meraki_flows_line() {
    let program = compile_filebeat();
    let mut value = event(meraki_line(), Some("UTC"));
    run_vrl(program, &mut value).expect("meraki flows line must transform");
    assert_eq!(
        str_field(&value, "cisco_meraki.event_type").as_deref(),
        Some("flows"),
        "meraki branch must run: {value:?}"
    );
    assert_eq!(
        str_field(&value, "event.action").as_deref(),
        Some("layer3-firewall-allowed-flow"),
        "meraki flows mapping must apply: {value:?}"
    );
}

#[test]
fn detects_and_parses_cisco_ios_line() {
    let program = compile_filebeat();
    let mut value = event(ios_line(), Some("UTC"));
    run_vrl(program, &mut value).expect("cisco ios line must transform");
    assert_eq!(
        str_field(&value, "observer.product").as_deref(),
        Some("IOS"),
        "ios branch must run: {value:?}"
    );
}

#[test]
fn detects_and_parses_umbrella_dns_line() {
    let program = compile_filebeat();
    let mut value = event(umbrella_line(), None);
    run_vrl(program, &mut value).expect("umbrella dns line must transform");
    assert_eq!(
        str_field(&value, "observer.product").as_deref(),
        Some("Umbrella"),
        "umbrella branch must run: {value:?}"
    );
    assert_eq!(
        str_field(&value, "observer.type").as_deref(),
        Some("dns"),
        "dns log type must be detected: {value:?}"
    );
}

#[test]
fn unmatched_event_is_tagged_and_passed_through() {
    let program = compile_filebeat();
    let mut value = event(UNMATCHED_LINE, None);
    run_vrl(program, &mut value).expect("unmatched event must pass through");
    let tags = field(&value, "tags").expect("tags must exist");
    let tags = tags.as_array().expect("tags must be an array");
    assert!(
        tags.contains(&Value::from("filebeat_unmatched")),
        "unmatched events must be tagged: {tags:?}"
    );
    assert_eq!(
        str_field(&value, "message").as_deref(),
        Some(UNMATCHED_LINE),
        "unmatched events must keep their payload: {value:?}"
    );
}

// =========================================================================
// Event carriage
// =========================================================================

/// One line per branch, for tests that care which branch ran rather than
/// what it produced.
fn branch_lines() -> [(&'static str, &'static str); 4] {
    [
        ("meraki", meraki_line()),
        ("ios", ios_line()),
        ("umbrella", umbrella_line()),
        ("unmatched", UNMATCHED_LINE),
    ]
}

#[test]
fn a_root_field_survives_every_branch_but_a_tag_does_not() {
    // A case replay marks its own events so a shared cluster's older rows
    // are not counted as this run's. `tags` cannot carry that marker: every
    // module branch assigns `.tags` outright before it does anything else, so
    // a root field is the only carriage that comes out the far side.
    let program = compile_filebeat();
    for (branch, line) in branch_lines() {
        let mut input = fb::input_event(line, Some(&serde_json::json!({ "tz_offset": "UTC" })));
        input["_e2e"] = serde_json::json!({ "run": "ws21" });
        input["tags"] = serde_json::json!(["ws21"]);
        let mut value = Value::from(input);
        run_vrl(program, &mut value).expect("the line must transform");

        assert_eq!(
            str_field(&value, "_e2e.run").as_deref(),
            Some("ws21"),
            "the {branch} branch dropped the root marker: {value:?}"
        );
        if branch != "unmatched" {
            let kept_the_tag = field(&value, "tags")
                .and_then(Value::as_array)
                .is_some_and(|tags| tags.contains(&Value::from("ws21")));
            assert!(
                !kept_the_tag,
                "the {branch} branch kept an input tag, so this test proves nothing: {value:?}"
            );
        }
    }
}

// =========================================================================
// Synthetic cases (tests/fixtures/filebeat/cases.yaml)
// =========================================================================

/// Run every case of one module through the pipeline and compare against
/// what each names. Returns human-readable problem strings (empty = pass).
fn run_cases(module: fb::Module) -> Vec<String> {
    let program = compile_filebeat();
    let mut problems = Vec::new();

    for case in fb::cases_for(module) {
        let mut value = Value::from(fb::case_event(case));
        if let Err(e) = run_vrl(program, &mut value) {
            problems.push(format!("{}: pipeline failed: {e}", case.name));
            continue;
        }
        let ours = serde_json::to_value(&value).expect("event serialises");
        if fb::is_unmatched(&ours) {
            problems.push(format!("{}: routed to filebeat_unmatched", case.name));
            continue;
        }
        problems.extend(
            fb::problems_for(case, &ours)
                .into_iter()
                .map(|p| format!("{}: {p}", case.name)),
        );
    }
    problems
}

#[test]
fn cases_cover_each_module_and_say_what_they_expect() {
    for module in [fb::Module::Meraki, fb::Module::Ios, fb::Module::Umbrella] {
        assert!(!fb::cases_for(module).is_empty(), "no cases for {module:?}");
    }
    for case in fb::cases() {
        assert!(
            case.expect.as_object().is_some_and(|o| !o.is_empty()),
            "case {} expects nothing, so it can never fail",
            case.name
        );
    }
}

fn assert_cases_clean(module: fb::Module) {
    let problems = run_cases(module);
    assert!(
        problems.is_empty(),
        "case mismatches ({}):\n{}",
        problems.len(),
        problems.join("\n")
    );
}

#[test]
fn cases_umbrella() {
    assert_cases_clean(fb::Module::Umbrella);
}

#[test]
fn cases_cisco_ios() {
    assert_cases_clean(fb::Module::Ios);
}

#[test]
fn cases_cisco_meraki() {
    assert_cases_clean(fb::Module::Meraki);
}
