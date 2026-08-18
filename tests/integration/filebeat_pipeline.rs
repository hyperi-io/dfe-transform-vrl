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
//! `tests/fixtures/filebeat/README.md`), unpacked from the committed
//! tar.gz at test time.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use dfe_transform_vrl::config::EnrichmentTableConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::engine::runner::run_vrl;
use dfe_transform_vrl::enrichment::EnrichmentRegistry;
use vrl::compiler::Program;
use vrl::value::Value;

fn repo_path(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

/// Compile the bundled pipeline with the bundled timezones table.
///
/// Compiled once per process (the 5.5K-line program takes ~1min in debug)
/// and shared by every test in this module.
fn compile_filebeat() -> &'static Program {
    static PROGRAM: OnceLock<Program> = OnceLock::new();
    PROGRAM.get_or_init(|| {
        let source = std::fs::read_to_string(repo_path("pipelines/filebeat/filebeat.vrl"))
            .expect("bundled filebeat.vrl must exist");
        let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
            name: "timezones".into(),
            path: repo_path("pipelines/filebeat/timezones.csv")
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

/// Build a DFE 2.1-shaped input event: `{message, tags, timestamp}` plus
/// the `_conf` object the 2.1 deployment injected (tz_offset, tz_map).
fn event_with_conf(message: &str, conf: Option<&serde_json::Value>) -> Value {
    let mut obj = serde_json::json!({
        "message": message,
        "tags": [],
        "timestamp": "2026-01-01T00:00:00Z",
    });
    if let Some(conf) = conf {
        obj["_conf"] = conf.clone();
    }
    Value::from(obj)
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

#[test]
fn detects_and_parses_meraki_flows_line() {
    let program = compile_filebeat();
    let mut value = event(
        "<134>1 1647478988.289402144 MX84_4 flows allow src=10.0.2.170 \
         dst=10.0.0.34 mac=00:7C:2D:BD:76:F2 protocol=udp sport=54841 dport=15600",
        Some("UTC"),
    );
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
    let mut value = event(
        "Feb  8 04:00:48 192.168.100.2 585917: Feb  8 04:00:47.272: \
         %SEC-6-IPACCESSLOGRP: list 177 denied igmp 192.168.100.197 -> 224.0.0.22, 1 packet",
        Some("UTC"),
    );
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
    let mut value = event(
        r#""2020-07-23 23:49:54","elasticuser","elasticuser,Elastic Machine","192.168.1.1","81.2.69.144","Allowed","1 (A)","NOERROR","www.elastic.co.","Software/Technology,Business Services,Application","Test Policy Name","AD Users, Roaming Computers","""#,
        None,
    );
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
    let mut value = event("completely unrecognisable payload", None);
    run_vrl(program, &mut value).expect("unmatched event must pass through");
    let tags = field(&value, "tags").expect("tags must exist");
    let tags = tags.as_array().expect("tags must be an array");
    assert!(
        tags.contains(&Value::from("filebeat_unmatched")),
        "unmatched events must be tagged: {tags:?}"
    );
    assert_eq!(
        str_field(&value, "message").as_deref(),
        Some("completely unrecognisable payload"),
        "unmatched events must keep their payload: {value:?}"
    );
}

// =========================================================================
// Real-data corpus (elastic/integrations pipeline test data, pinned; see
// tests/fixtures/filebeat/README.md)
// =========================================================================

use std::collections::BTreeMap;

/// The committed corpus archive, read once and held in memory.
fn corpus() -> &'static BTreeMap<String, Vec<u8>> {
    static DATA: OnceLock<BTreeMap<String, Vec<u8>>> = OnceLock::new();
    DATA.get_or_init(|| {
        let path = repo_path("tests/fixtures/filebeat/filebeat-testdata.tar.gz");
        let file = std::fs::File::open(&path).expect("fixtures archive must exist");
        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
        let mut map = BTreeMap::new();
        for entry in archive.entries().expect("read fixtures tar") {
            let mut entry = entry.expect("fixtures tar entry");
            if !entry.header().entry_type().is_file() {
                continue;
            }
            let name = entry
                .path()
                .expect("entry path")
                .to_string_lossy()
                .trim_start_matches("./")
                .to_string();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut buf).expect("read entry");
            map.insert(name, buf);
        }
        assert!(!map.is_empty(), "fixtures archive is empty");
        map
    })
}

fn corpus_text(name: &str) -> String {
    let bytes = corpus()
        .get(name)
        .unwrap_or_else(|| panic!("fixture {name} missing from archive"));
    String::from_utf8_lossy(bytes).into_owned()
}

fn corpus_lines(name: &str) -> Vec<String> {
    corpus_text(name)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_string)
        .collect()
}

fn corpus_expected(name: &str) -> Vec<serde_json::Value> {
    let doc: serde_json::Value =
        serde_json::from_str(&corpus_text(name)).expect("expected.json parses");
    doc.get("expected")
        .and_then(|e| e.as_array())
        .expect("expected array")
        .clone()
}

/// The `fields._conf` object for a corpus file (tz_offset, tz_map, ...):
/// `<file>-config.yml` wins, else the module's `test-common-config.yml`
/// (mirrors the elastic test harness).
fn corpus_conf(log_name: &str) -> Option<serde_json::Value> {
    let per_file = format!("{log_name}-config.yml");
    let dir = log_name.rsplit_once('/').map_or("", |(d, _)| d);
    let common = format!("{dir}/test-common-config.yml");
    for candidate in [per_file, common] {
        if let Some(bytes) = corpus().get(&candidate) {
            let doc: serde_json::Value =
                serde_yaml_ng::from_slice(bytes).expect("config yml parses");
            if let Some(conf) = doc.get("fields").and_then(|f| f.get("_conf")) {
                return Some(conf.clone());
            }
        }
    }
    None
}

/// Paths where the port deliberately diverges from the elastic goldens.
/// geoip enrichment is stripped (dfe-loader owns geoip), event.ingested
/// is elastic-harness-dynamic, log.file.path carries our routing seed.
const IGNORED_PATHS: &[&str] = &[
    "source.geo",
    "source.as",
    "destination.geo",
    "destination.as",
    "client.geo",
    "client.as",
    "event.ingested",
    "log.file.path",
    // VRL's parse_user_agent (woothee/ua-parser) classifies differently
    // from elastic's user_agent processor ("Mac OSX" vs "Mac OS X",
    // device "pc" vs "Mac"). Engine-library parity, not a port defect;
    // user_agent.original stays asserted.
    "user_agent.name",
    "user_agent.os",
    "user_agent.device",
    "user_agent.version",
    "user_agent.major",
    "user_agent.minor",
    "user_agent.patch",
];

fn ignored(path: &str) -> bool {
    IGNORED_PATHS
        .iter()
        .any(|p| path == *p || path.starts_with(&format!("{p}.")))
}

/// Narrow per-line divergences from the goldens, each with a cause that is
/// library parity or an elastic-integration feature the 2.1 templates
/// never had (see pipelines/filebeat/README.md, Limitations).
const ACCEPTED_MISMATCHES: &[(&str, usize, &str)] = &[
    // VRL's public-suffix data includes the PSL private section
    // (*.servicebus.windows.net); elastic's registered_domain processor
    // uses the ICANN section only.
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        6,
        "dns.question.registered_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        6,
        "dns.question.subdomain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        6,
        "dns.question.top_level_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        9,
        "dns.question.registered_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        9,
        "dns.question.subdomain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        9,
        "dns.question.top_level_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        10,
        "dns.question.registered_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        10,
        "dns.question.subdomain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        10,
        "dns.question.top_level_domain",
    ),
    // VRL parse_url normalises an empty path to "/"; elastic keeps "".
    (
        "cisco_umbrella/log/test-umbrella-dlplogs.log",
        0,
        "url.path",
    ),
    // VRL is_nullish(" ") is true, so the template skips user_agent
    // entirely for whitespace-only UA strings; elastic still emits
    // user_agent.original " " with name "Other". Library semantics.
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        7,
        "user_agent",
    ),
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        8,
        "user_agent",
    ),
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        9,
        "user_agent",
    ),
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        10,
        "user_agent",
    ),
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        11,
        "user_agent",
    ),
];

fn accepted_mismatch(file: &str, idx: usize, path: &str) -> bool {
    ACCEPTED_MISMATCHES
        .iter()
        .any(|(f, i, p)| *f == file && *i == idx && *p == path)
}

/// Timestamps match at millisecond precision. With `allow_year_swap`
/// (cisco_ios only - its classic "Feb  8 04:00:48" format carries no
/// year, so the parser infers the CURRENT year while the goldens embed
/// their generation year) a mismatch in the year alone is tolerated.
fn timestamps_match(ours: &str, exp: &str, allow_year_swap: bool) -> bool {
    use chrono::{DateTime, Datelike, SubsecRound};
    let (Ok(a), Ok(b)) = (
        DateTime::parse_from_rfc3339(ours),
        DateTime::parse_from_rfc3339(exp),
    ) else {
        return false;
    };
    let a = a.trunc_subsecs(3);
    let b = b.trunc_subsecs(3);
    if a == b {
        return true;
    }
    if !allow_year_swap {
        return false;
    }
    let (Some(a), Some(b)) = (a.with_year(2000), b.with_year(2000)) else {
        return false;
    };
    a == b
}

/// Canonical query-string compare: same key -> multiset of values,
/// independent of parameter order (the 2.1 template rebuilds url.query
/// from a VRL object, which alphabetises; elastic keeps raw order).
fn query_params(q: &str) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for pair in q.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        map.entry(k.to_string()).or_default().push(v.to_string());
    }
    for values in map.values_mut() {
        values.sort();
    }
    map
}

/// Compare one transformed event against its elastic golden: every leaf in
/// `expected` (bar the documented ignores) must be present and equal in
/// ours. Extra fields on our side (DFE 2.1 event carriage like `_conf`,
/// `timestamp`) are fine. The root `tags` array compares as a set (the
/// 2.1 template seeds tags in a different order).
fn diff_expected(
    path: &str,
    exp: &serde_json::Value,
    ours: &serde_json::Value,
    allow_year_swap: bool,
    problems: &mut Vec<String>,
) {
    if ignored(path) {
        return;
    }
    match exp {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                match ours.get(k) {
                    Some(o) => diff_expected(&child, v, o, allow_year_swap, problems),
                    None => {
                        if !ignored(&child) {
                            problems.push(format!("{child}: missing (expected {v})"));
                        }
                    }
                }
            }
        }
        serde_json::Value::Array(items) if path == "tags" => {
            let ours_set: Vec<&serde_json::Value> = ours
                .as_array()
                .map(|a| a.iter().collect())
                .unwrap_or_default();
            for item in items {
                if !ours_set.contains(&item) {
                    problems.push(format!("tags: missing {item}"));
                }
            }
        }
        serde_json::Value::Array(items) => match ours.as_array() {
            Some(ours_arr) if ours_arr.len() == items.len() => {
                for (i, (e, o)) in items.iter().zip(ours_arr).enumerate() {
                    diff_expected(&format!("{path}[{i}]"), e, o, allow_year_swap, problems);
                }
            }
            _ => problems.push(format!("{path}: expected array {exp}, got {ours}")),
        },
        leaf => {
            if path == "@timestamp" || path.ends_with(".@timestamp") {
                let ours_s = ours.as_str().unwrap_or_default();
                let exp_s = leaf.as_str().unwrap_or_default();
                if !timestamps_match(ours_s, exp_s, allow_year_swap) {
                    problems.push(format!("{path}: expected {exp_s}, got {ours_s}"));
                }
            } else if path == "url.query" {
                let ours_s = ours.as_str().unwrap_or_default();
                let exp_s = leaf.as_str().unwrap_or_default();
                if query_params(ours_s) != query_params(exp_s) {
                    problems.push(format!(
                        "{path}: params differ, expected {exp_s}, got {ours_s}"
                    ));
                }
            } else if ours != leaf {
                problems.push(format!("{path}: expected {leaf}, got {ours}"));
            }
        }
    }
}

/// Run every line of a corpus log through the pipeline and compare against
/// the goldens. Returns human-readable problem strings (empty = pass).
fn run_corpus_file(log_name: &str) -> Vec<String> {
    let program = compile_filebeat();
    let lines = corpus_lines(log_name);
    let expected = corpus_expected(&format!("{log_name}-expected.json"));
    let conf = corpus_conf(log_name);
    let allow_year_swap = log_name.starts_with("cisco_ios/");
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
        // Every corpus line belongs to a known module - the unmatched
        // catch-all must never fire on it.
        if ours
            .get("tags")
            .and_then(|t| t.as_array())
            .is_some_and(|t| t.contains(&serde_json::json!("filebeat_unmatched")))
        {
            problems.push(format!("{log_name}[{idx}]: routed to filebeat_unmatched"));
            continue;
        }
        let mut event_problems = Vec::new();
        diff_expected("", exp, &ours, allow_year_swap, &mut event_problems);
        problems.extend(
            event_problems
                .into_iter()
                .filter(|p| {
                    let path = p.split(':').next().unwrap_or_default();
                    !accepted_mismatch(log_name, idx, path)
                })
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
    assert_corpus_clean(&[
        "cisco_ios/log/test-asr920.log",
        "cisco_ios/log/test-badauth.log",
        "cisco_ios/log/test-cisco-ios.log",
        "cisco_ios/log/test-date-format-tzoffset.log",
        "cisco_ios/log/test-date-format.log",
        "cisco_ios/log/test-syslog-header.log",
        "cisco_ios/log/test-syslog.log",
        "cisco_ios/log/test-tzoffset.log",
    ]);
}

#[test]
fn corpus_cisco_meraki() {
    assert_corpus_clean(&[
        "cisco_meraki/log/test-airmarshal-events.log",
        "cisco_meraki/log/test-events.log",
        "cisco_meraki/log/test-flows.log",
        "cisco_meraki/log/test-ip-flow.log",
        "cisco_meraki/log/test-security-events.log",
        "cisco_meraki/log/test-urls.log",
    ]);
}
