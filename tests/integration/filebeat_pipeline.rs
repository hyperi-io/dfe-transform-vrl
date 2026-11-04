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
//! The corpus tests drive the pipeline with REAL filebeat test data from
//! elastic/integrations (pinned SHA, see
//! `tests/fixtures/filebeat/README.md`), read in memory from the committed
//! tar.gz at test time.
//!
//! This runs the pipeline IN PROCESS. `tests/e2e/filebeat_kafka.rs` runs the
//! same corpus through the app binary and a real broker; both grade against
//! the shared comparison in `tests/common/filebeat.rs`.

use std::sync::OnceLock;

use dfe_transform_vrl::config::EnrichmentTableConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::engine::runner::run_vrl;
use dfe_transform_vrl::enrichment::EnrichmentRegistry;
use vrl::compiler::Program;
use vrl::value::Value;

use crate::filebeat_corpus as fb;

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
// Module detection smoke tests (single real lines per module)
// =========================================================================

const MERAKI_LINE: &str = "<134>1 1647478988.289402144 MX84_4 flows allow src=10.0.2.170 \
     dst=10.0.0.34 mac=00:7C:2D:BD:76:F2 protocol=udp sport=54841 dport=15600";

const IOS_LINE: &str = "Feb  8 04:00:48 192.168.100.2 585917: Feb  8 04:00:47.272: \
     %SEC-6-IPACCESSLOGRP: list 177 denied igmp 192.168.100.197 -> 224.0.0.22, 1 packet";

const UMBRELLA_LINE: &str = r#""2020-07-23 23:49:54","elasticuser","elasticuser,Elastic Machine","192.168.1.1","81.2.69.144","Allowed","1 (A)","NOERROR","www.elastic.co.","Software/Technology,Business Services,Application","Test Policy Name","AD Users, Roaming Computers","""#;

const UNMATCHED_LINE: &str = "completely unrecognisable payload";

#[test]
fn detects_and_parses_meraki_flows_line() {
    let program = compile_filebeat();
    let mut value = event(MERAKI_LINE, Some("UTC"));
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
    let mut value = event(IOS_LINE, Some("UTC"));
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
    let mut value = event(UMBRELLA_LINE, None);
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

/// One real line per branch, for tests that care which branch ran rather than
/// what it produced.
const BRANCH_LINES: &[(&str, &str)] = &[
    ("meraki", MERAKI_LINE),
    ("ios", IOS_LINE),
    ("umbrella", UMBRELLA_LINE),
    ("unmatched", UNMATCHED_LINE),
];

#[test]
fn a_root_field_survives_every_branch_but_a_tag_does_not() {
    // A corpus replay marks its own events so a shared cluster's older rows
    // are not counted as this run's. `tags` cannot carry that marker: every
    // module branch assigns `.tags` outright before it does anything else, so
    // a root field is the only carriage that comes out the far side.
    let program = compile_filebeat();
    for (branch, line) in BRANCH_LINES {
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
        if *branch != "unmatched" {
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
// Real-data corpus (elastic/integrations pipeline test data, pinned; see
// tests/fixtures/filebeat/README.md)
// =========================================================================

/// Run every line of a corpus log through the pipeline and compare against
/// the goldens. Returns human-readable problem strings (empty = pass).
fn run_corpus_file(log_name: &str) -> Vec<String> {
    let program = compile_filebeat();
    let lines = fb::corpus_lines(log_name);
    let expected = fb::corpus_expected(&format!("{log_name}-expected.json"));
    let conf = fb::corpus_conf(log_name);
    let mut problems = Vec::new();

    if lines.len() != expected.len() {
        problems.push(format!(
            "{log_name}: {} input lines but {} golden events",
            lines.len(),
            expected.len()
        ));
    }

    for (idx, (line, exp)) in lines.iter().zip(&expected).enumerate() {
        let mut value = event_with_conf(line, conf.as_ref());
        if let Err(e) = run_vrl(program, &mut value) {
            problems.push(format!("{log_name}[{idx}]: pipeline failed: {e}"));
            continue;
        }
        let ours = serde_json::to_value(&value).expect("event serialises");
        if fb::is_unmatched(&ours) {
            problems.push(format!("{log_name}[{idx}]: routed to filebeat_unmatched"));
            continue;
        }
        problems.extend(
            fb::problems_against_golden(log_name, idx, exp, &ours)
                .into_iter()
                .map(|p| format!("{log_name}[{idx}]: {p}")),
        );
    }
    problems
}

fn assert_corpus_clean(log_names: &[&str]) {
    let mut problems = Vec::new();
    for name in log_names {
        problems.extend(run_corpus_file(name));
    }
    assert!(
        problems.is_empty(),
        "corpus mismatches ({}):\n{}",
        problems.len(),
        problems.join("\n")
    );
}

#[test]
fn corpus_umbrella_dnslogs() {
    assert_corpus_clean(&["cisco_umbrella/log/test-umbrella-dnslogs.log"]);
}

#[test]
fn corpus_umbrella_proxylogs() {
    assert_corpus_clean(&["cisco_umbrella/log/test-umbrella-proxylogs.log"]);
}

#[test]
fn corpus_umbrella_auditlogs() {
    assert_corpus_clean(&["cisco_umbrella/log/test-umbrella-auditlogs.log"]);
}

#[test]
fn corpus_umbrella_iplogs() {
    assert_corpus_clean(&["cisco_umbrella/log/test-umbrella-iplogs.log"]);
}

#[test]
fn corpus_umbrella_dlplogs() {
    assert_corpus_clean(&["cisco_umbrella/log/test-umbrella-dlplogs.log"]);
}

#[test]
fn corpus_umbrella_intrusionlogs() {
    assert_corpus_clean(&["cisco_umbrella/log/test-umbrella-intrusionlogs.log"]);
}

#[test]
fn corpus_umbrella_cloudfirewalllogs() {
    assert_corpus_clean(&["cisco_umbrella/log/test-umbrella-cloudfirewalllogs.log"]);
}

#[test]
fn corpus_cisco_ios() {
    assert_corpus_clean(fb::IOS_LOGS);
}

#[test]
fn corpus_cisco_meraki() {
    assert_corpus_clean(fb::MERAKI_LOGS);
}
