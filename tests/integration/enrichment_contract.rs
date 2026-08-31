// Project:   dfe-transform-vrl
// File:      tests/integration/enrichment_contract.rs
// Purpose:   Enrichment behaviour pinned against Vector's documented contract
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Every expectation here was taken from `vector 0.58.0` run as an oracle,
//! not from the docs: a stdin -> remap -> console config over the same CSV,
//! checked with `vector validate` for the compile-time cases and a real event
//! for the runtime ones. See hyperi-io/dfe-transform-vrl#38.
//!
//! No mocks: real CSV files in temp dirs, and a real MaxMind database built
//! by `mmdb_fixture` for the MMDB cases.

use std::sync::Arc;

use dfe_transform_vrl::config::EnrichmentTableConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::engine::runner::run_vrl;
use dfe_transform_vrl::enrichment::EnrichmentRegistry;
use vrl::value::Value;

/// `id,name,status` with one disabled user and two active ones.
const USERS_CSV: &str = "id,name,status\n1,Bob,active\n2,Fred,disabled\n3,Alice,active\n";

fn write_file(dir: &std::path::Path, name: &str, content: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path.to_string_lossy().to_string()
}

/// A registry holding a single `users` table keyed on `id`.
fn users_registry(dir: &std::path::Path) -> Arc<EnrichmentRegistry> {
    registry_for(EnrichmentTableConfig {
        name: "users".into(),
        path: write_file(dir, "users.csv", USERS_CSV),
        key_columns: vec!["id".into()],
        ..Default::default()
    })
}

fn registry_for(config: EnrichmentTableConfig) -> Arc<EnrichmentRegistry> {
    EnrichmentRegistry::load(&[config]).unwrap().into_arc()
}

/// Run `source` over a seed event, returning the transformed event.
fn run(source: &str, registry: Arc<EnrichmentRegistry>) -> Result<Value, String> {
    let program = compile_vrl(source, Some(registry))
        .map_err(|e| e.to_string())?
        .program;
    let mut value = Value::from(serde_json::json!({"message": "seed"}));
    run_vrl(&program, &mut value).map_err(|e| e.to_string())?;
    Ok(value)
}

/// The compile diagnostics for `source`, which must not compile.
fn compile_error(source: &str, registry: Option<Arc<EnrichmentRegistry>>) -> String {
    compile_vrl(source, registry)
        .err()
        .expect("program must not compile")
        .to_string()
}

fn field(event: &Value, key: &str) -> Option<Value> {
    event.as_object().unwrap().get(key).cloned()
}

/// A `column -> type name` schema map.
fn schema_of(pairs: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

// =========================================================================
// Bug 2 -- non-key condition fields must filter, not be discarded
// =========================================================================

#[test]
fn condition_fields_outside_the_key_columns_are_matched() {
    // vector 0.58.0, same CSV and condition: "No rows found".
    let dir = tempfile::tempdir().unwrap();
    let err = run(
        r#". |= get_enrichment_table_record!("users", {"id": "1", "status": "disabled"})"#,
        users_registry(dir.path()),
    )
    .expect_err("Bob is active, so this condition matches nothing");
    assert!(err.contains("No rows found"), "unexpected error: {err}");
}

#[test]
fn a_condition_that_does_match_every_field_returns_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let event = run(
        r#". |= get_enrichment_table_record!("users", {"id": "1", "status": "active"})"#,
        users_registry(dir.path()),
    )
    .unwrap();
    assert_eq!(field(&event, "name"), Some(Value::from("Bob")));
}

// =========================================================================
// Bug 3 -- a miss errors, an ambiguous match errors
// =========================================================================

#[test]
fn a_miss_errors_rather_than_returning_null() {
    let dir = tempfile::tempdir().unwrap();
    let err = run(
        r#".user = get_enrichment_table_record!("users", {"id": "99"})"#,
        users_registry(dir.path()),
    )
    .expect_err("a miss must abort the program");
    assert!(err.contains("No rows found"), "unexpected error: {err}");
}

#[test]
fn an_ambiguous_match_errors() {
    // vector 0.58.0 on {"status": "active"}: "More than one row found".
    let dir = tempfile::tempdir().unwrap();
    let err = run(
        r#".user = get_enrichment_table_record!("users", {"status": "active"})"#,
        users_registry(dir.path()),
    )
    .expect_err("two active users must abort the program");
    assert!(
        err.contains("More than one row found"),
        "unexpected error: {err}"
    );
}

#[test]
fn a_miss_is_recoverable_with_the_infallible_form() {
    // The replacement for the old null return: handle it explicitly.
    let dir = tempfile::tempdir().unwrap();
    let event = run(
        r#".user = get_enrichment_table_record("users", {"id": "99"}) ?? {"name": "unknown"}"#,
        users_registry(dir.path()),
    )
    .unwrap();
    let user = field(&event, "user").unwrap();
    assert_eq!(
        user.as_object().unwrap().get("name"),
        Some(&Value::from("unknown"))
    );
}

#[test]
fn find_returns_an_empty_array_on_a_miss() {
    let dir = tempfile::tempdir().unwrap();
    let event = run(
        r#".rows = find_enrichment_table_records!("users", {"id": "99"})"#,
        users_registry(dir.path()),
    )
    .unwrap();
    assert_eq!(field(&event, "rows"), Some(Value::Array(vec![])));
}

#[test]
fn find_returns_every_matching_row() {
    let dir = tempfile::tempdir().unwrap();
    let event = run(
        r#".rows = find_enrichment_table_records!("users", {"status": "active"})"#,
        users_registry(dir.path()),
    )
    .unwrap();
    let rows = field(&event, "rows").unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
}

#[test]
fn duplicate_key_values_are_all_kept() {
    // pipelines/filebeat/timezones.csv has 12 duplicated abbreviations, and
    // the filebeat pipeline branches on `length(records) > 1`. Collapsing
    // them at load time made that branch unreachable.
    let dir = tempfile::tempdir().unwrap();
    let registry = registry_for(EnrichmentTableConfig {
        name: "tz".into(),
        path: write_file(
            dir.path(),
            "tz.csv",
            "abbreviation,name,offset\nCST,Central Standard Time,-06:00\nCST,China Standard Time,+08:00\n",
        ),
        key_columns: vec!["abbreviation".into()],
        ..Default::default()
    });

    let event = run(
        r#".rows = find_enrichment_table_records!("tz", {"abbreviation": "CST"})"#,
        registry,
    )
    .unwrap();
    let rows = field(&event, "rows").unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
}

// =========================================================================
// Bug 4 -- select, case_sensitive and wildcard
// =========================================================================

#[test]
fn select_limits_the_returned_fields() {
    let dir = tempfile::tempdir().unwrap();
    let event = run(
        r#".user = get_enrichment_table_record!("users", {"id": "1"}, select: ["name"])"#,
        users_registry(dir.path()),
    )
    .unwrap();
    let user = field(&event, "user").unwrap();
    let user = user.as_object().unwrap();
    assert_eq!(user.get("name"), Some(&Value::from("Bob")));
    assert!(user.get("status").is_none());
}

#[test]
fn select_drops_a_field_the_row_does_not_carry() {
    // vector 0.58.0 returns just {"name": "Bob"} here rather than erroring.
    let dir = tempfile::tempdir().unwrap();
    let event = run(
        r#".user = get_enrichment_table_record!("users", {"id": "1"}, select: ["name", "nosuchcol"])"#,
        users_registry(dir.path()),
    )
    .unwrap();
    let user = field(&event, "user").unwrap();
    assert_eq!(user.as_object().unwrap().len(), 1);
}

#[test]
fn case_sensitive_defaults_to_true_and_can_be_turned_off() {
    let dir = tempfile::tempdir().unwrap();
    let registry = users_registry(dir.path());

    let err = run(
        r#".user = get_enrichment_table_record!("users", {"name": "bob"})"#,
        Arc::clone(&registry),
    )
    .expect_err("case sensitive by default");
    assert!(err.contains("No rows found"), "unexpected error: {err}");

    let event = run(
        r#".user = get_enrichment_table_record!("users", {"name": "bob"}, case_sensitive: false)"#,
        registry,
    )
    .unwrap();
    let user = field(&event, "user").unwrap();
    assert_eq!(user.as_object().unwrap().get("id"), Some(&Value::from("1")));
}

#[test]
fn wildcard_matches_rows_the_condition_value_does_not() {
    // vector 0.58.0 returns both active rows for this call.
    let dir = tempfile::tempdir().unwrap();
    let event = run(
        r#".rows = find_enrichment_table_records!("users", {"status": "nope"}, wildcard: "active")"#,
        users_registry(dir.path()),
    )
    .unwrap();
    let rows = field(&event, "rows").unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
}

#[test]
fn wildcard_reaches_rows_the_key_index_would_hide() {
    // vector 0.58.0 over id,name of 1,Bob and *,Anyone with the same call
    // returns [{"id":"*","name":"Anyone"}] -- the wildcard is not confined to
    // the bucket the condition selects.
    let dir = tempfile::tempdir().unwrap();
    let registry = registry_for(EnrichmentTableConfig {
        name: "wild".into(),
        path: write_file(dir.path(), "wild.csv", "id,name\n1,Bob\n*,Anyone\n"),
        key_columns: vec!["id".into()],
        ..Default::default()
    });

    let event = run(
        r#".rows = find_enrichment_table_records!("wild", {"id": "999"}, wildcard: "*")"#,
        registry,
    )
    .unwrap();
    let rows = field(&event, "rows").unwrap();
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].as_object().unwrap().get("name"),
        Some(&Value::from("Anyone"))
    );
}

// =========================================================================
// Bug 5 -- the merge idiom from the CSV enrichment guide
// =========================================================================

#[test]
fn the_merge_idiom_compiles_and_runs() {
    // `. |= get_enrichment_table_record!(...)` is what the guide documents.
    let dir = tempfile::tempdir().unwrap();
    let event = run(
        r#". |= get_enrichment_table_record!("users", {"id": "2"})"#,
        users_registry(dir.path()),
    )
    .unwrap();
    assert_eq!(field(&event, "name"), Some(Value::from("Fred")));
    assert_eq!(field(&event, "status"), Some(Value::from("disabled")));
    assert_eq!(field(&event, "message"), Some(Value::from("seed")));
}

// =========================================================================
// Bug 6 -- compile-time table validation
// =========================================================================

#[test]
fn an_unknown_table_is_a_compile_error_naming_the_registered_tables() {
    let dir = tempfile::tempdir().unwrap();
    let err = compile_error(
        r#".x = get_enrichment_table_record!("no_such_table", {"id": "1"})"#,
        Some(users_registry(dir.path())),
    );
    assert!(
        err.contains("invalid enum variant"),
        "unexpected diagnostic: {err}"
    );
    assert!(
        err.contains("users"),
        "the diagnostic must list what IS registered: {err}"
    );
}

#[test]
fn a_computed_table_name_is_a_compile_error() {
    // The security property: VRL must never be able to compute a table name.
    let dir = tempfile::tempdir().unwrap();
    let err = compile_error(
        r#"t = string!(.message)
           .x = get_enrichment_table_record!(t, {"id": "1"})"#,
        Some(users_registry(dir.path())),
    );
    assert!(
        err.contains("expected") && err.contains("literal"),
        "unexpected diagnostic: {err}"
    );
}

#[test]
fn find_records_validates_the_table_name_too() {
    let dir = tempfile::tempdir().unwrap();
    let err = compile_error(
        r#".x = find_enrichment_table_records!("no_such_table", {"id": "1"})"#,
        Some(users_registry(dir.path())),
    );
    assert!(
        err.contains("invalid enum variant"),
        "unexpected diagnostic: {err}"
    );
}

#[test]
fn no_registry_is_a_compile_error() {
    let err = compile_error(
        r#".x = get_enrichment_table_record!("users", {"id": "1"})"#,
        None,
    );
    assert!(
        err.contains("enrichment tables not loaded"),
        "unexpected diagnostic: {err}"
    );
}

// =========================================================================
// Bug 7 -- schema coercion and date-range conditions
// =========================================================================

#[test]
fn schema_coerces_a_column_to_an_integer() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry_for(EnrichmentTableConfig {
        name: "codes".into(),
        path: write_file(dir.path(), "codes.csv", "id,status_code\na,404\nb,500\n"),
        key_columns: vec!["id".into()],
        schema: schema_of(&[("status_code", "integer")]),
        ..Default::default()
    });

    let event = run(
        r#". |= get_enrichment_table_record!("codes", {"status_code": 404})"#,
        registry,
    )
    .unwrap();
    assert_eq!(field(&event, "id"), Some(Value::from("a")));
    assert_eq!(field(&event, "status_code"), Some(Value::Integer(404)));
}

#[test]
fn a_bad_schema_type_name_fails_the_table_load() {
    let dir = tempfile::tempdir().unwrap();
    let result = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "codes".into(),
        path: write_file(dir.path(), "codes.csv", "id,status_code\na,404\n"),
        key_columns: vec!["id".into()],
        schema: schema_of(&[("status_code", "quantum")]),
        ..Default::default()
    }]);
    assert!(result.is_err());
}

#[test]
fn date_range_conditions_select_by_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry_for(EnrichmentTableConfig {
        name: "people".into(),
        path: write_file(
            dir.path(),
            "people.csv",
            "id,name,dob\n1,Bob,1985-06-15T00:00:00Z\n2,Fred,1990-01-01T00:00:00Z\n3,Alice,1975-03-20T00:00:00Z\n",
        ),
        key_columns: vec!["id".into()],
        schema: schema_of(&[("dob", "timestamp")]),
        ..Default::default()
    });

    let event = run(
        r#"
        .between = find_enrichment_table_records!("people", {"dob": {"from": t'1980-01-01T00:00:00Z', "to": t'1989-12-31T00:00:00Z'}})
        .from = find_enrichment_table_records!("people", {"dob": {"from": t'1986-01-01T00:00:00Z'}})
        .to = find_enrichment_table_records!("people", {"dob": {"to": t'1980-01-01T00:00:00Z'}})
        "#,
        registry,
    )
    .unwrap();

    let names = |key: &str| -> Vec<String> {
        field(&event, key)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row.as_object().unwrap().get("name").unwrap().to_string())
            .collect()
    };
    assert_eq!(names("between"), vec![r#""Bob""#]);
    assert_eq!(names("from"), vec![r#""Fred""#]);
    assert_eq!(names("to"), vec![r#""Alice""#]);
}

#[test]
fn a_date_column_parses_at_midnight_utc() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry_for(EnrichmentTableConfig {
        name: "people".into(),
        path: write_file(dir.path(), "people.csv", "id,dob\n1,1985-06-15\n"),
        key_columns: vec!["id".into()],
        schema: schema_of(&[("dob", "date")]),
        ..Default::default()
    });

    let event = run(
        r#".rows = find_enrichment_table_records!("people", {"dob": {"from": t'1985-06-15T00:00:00Z', "to": t'1985-06-15T00:00:00Z'}})"#,
        registry,
    )
    .unwrap();
    assert_eq!(field(&event, "rows").unwrap().as_array().unwrap().len(), 1);
}

// =========================================================================
// Bug 1 -- MMDB lookups
// =========================================================================

#[cfg(feature = "enrichment-mmdb")]
mod mmdb {
    use super::{compile_error, field, registry_for, run};
    use dfe_transform_vrl::config::{EnrichmentSourceConfig, EnrichmentTableConfig};
    use vrl::value::Value;

    use crate::mmdb_fixture::asn_fixture;

    fn asn_registry(
        dir: &std::path::Path,
    ) -> std::sync::Arc<dfe_transform_vrl::enrichment::EnrichmentRegistry> {
        let path = asn_fixture(dir);
        registry_for(EnrichmentTableConfig {
            name: "asn".into(),
            source: Some(EnrichmentSourceConfig::Mmdb {
                path: path.to_string_lossy().to_string(),
            }),
            ..Default::default()
        })
    }

    #[test]
    fn a_hit_returns_the_decoded_record() {
        let dir = tempfile::tempdir().unwrap();
        let event = run(
            r#". |= get_enrichment_table_record!("asn", {"ip": "1.128.0.5"})"#,
            asn_registry(dir.path()),
        )
        .unwrap();
        assert_eq!(
            field(&event, "autonomous_system_number"),
            Some(Value::Integer(1221))
        );
        assert_eq!(
            field(&event, "autonomous_system_organization"),
            Some(Value::from("Telstra Pty Ltd"))
        );
    }

    #[test]
    fn a_second_network_resolves_independently() {
        let dir = tempfile::tempdir().unwrap();
        let event = run(
            r#". |= get_enrichment_table_record!("asn", {"ip": "12.81.92.200"})"#,
            asn_registry(dir.path()),
        )
        .unwrap();
        assert_eq!(
            field(&event, "autonomous_system_number"),
            Some(Value::Integer(7018))
        );
    }

    #[test]
    fn a_miss_errors_like_any_other_table() {
        let dir = tempfile::tempdir().unwrap();
        let err = run(
            r#".x = get_enrichment_table_record!("asn", {"ip": "203.0.113.1"})"#,
            asn_registry(dir.path()),
        )
        .expect_err("an address outside the fixture must miss");
        assert!(err.contains("No rows found"), "unexpected error: {err}");
    }

    #[test]
    fn find_returns_the_record_as_a_single_element_array() {
        let dir = tempfile::tempdir().unwrap();
        let event = run(
            r#".rows = find_enrichment_table_records!("asn", {"ip": "1.128.0.5"})"#,
            asn_registry(dir.path()),
        )
        .unwrap();
        assert_eq!(field(&event, "rows").unwrap().as_array().unwrap().len(), 1);
    }

    #[test]
    fn select_applies_to_an_mmdb_record() {
        let dir = tempfile::tempdir().unwrap();
        let event = run(
            r#".x = get_enrichment_table_record!("asn", {"ip": "1.128.0.5"}, select: ["autonomous_system_number"])"#,
            asn_registry(dir.path()),
        )
        .unwrap();
        let record = field(&event, "x").unwrap();
        assert_eq!(record.as_object().unwrap().len(), 1);
    }

    #[test]
    fn a_non_address_condition_value_errors() {
        let dir = tempfile::tempdir().unwrap();
        let err = run(
            r#".x = get_enrichment_table_record!("asn", {"ip": "not-an-ip"})"#,
            asn_registry(dir.path()),
        )
        .expect_err("a non-address must abort");
        assert!(err.contains("not an IP address"), "unexpected error: {err}");
    }

    #[test]
    fn two_conditions_error_because_a_geo_table_has_one_key() {
        let dir = tempfile::tempdir().unwrap();
        let err = run(
            r#".x = get_enrichment_table_record!("asn", {"ip": "1.128.0.5", "other": "x"})"#,
            asn_registry(dir.path()),
        )
        .expect_err("an mmdb table takes exactly one condition");
        assert!(
            err.contains("exactly one condition"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn an_mmdb_table_name_is_still_validated_at_compile_time() {
        let dir = tempfile::tempdir().unwrap();
        let err = compile_error(
            r#".x = get_enrichment_table_record!("nope", {"ip": "1.128.0.5"})"#,
            Some(asn_registry(dir.path())),
        );
        assert!(err.contains("invalid enum variant"), "unexpected: {err}");
    }
}
