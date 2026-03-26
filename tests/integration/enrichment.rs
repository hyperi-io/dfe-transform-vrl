// Project:   dfe-transform-vrl
// File:      tests/integration/enrichment.rs
// Purpose:   Integration tests for enrichment table VRL functions
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;

use dfe_transform_vrl::config::EnrichmentTableConfig;
use dfe_transform_vrl::engine::compiler::compile_vrl;
use dfe_transform_vrl::engine::runner::run_vrl;
use dfe_transform_vrl::enrichment::EnrichmentRegistry;
use vrl::compiler::Program;
use vrl::value::Value;

fn compile_with_registry(source: &str, registry: Arc<EnrichmentRegistry>) -> Program {
    compile_vrl(source, Some(registry)).unwrap().program
}

fn run_transform(program: &Program, event: serde_json::Value) -> Value {
    let mut value = Value::from(event);
    run_vrl(program, &mut value).unwrap();
    value
}

fn write_file(dir: &std::path::Path, name: &str, content: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path.to_string_lossy().to_string()
}

// =========================================================================
// CSV enrichment
// =========================================================================

#[test]
fn test_csv_lookup_hit() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = write_file(
        dir.path(),
        "services.csv",
        "service_id,name,tier\nsvc-001,auth,critical\nsvc-002,web,standard\n",
    );

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "services".into(),
        path: csv_path,
        key_columns: vec!["service_id".into()],
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    let program = compile_with_registry(
        r#".service = get_enrichment_table_record!("services", {"service_id": .service_id})"#,
        registry,
    );

    let result = run_transform(
        &program,
        serde_json::json!({"service_id": "svc-001", "message": "hello"}),
    );

    let obj = result.as_object().unwrap();
    let service = obj.get("service").unwrap().as_object().unwrap();
    assert_eq!(service.get("name"), Some(&Value::from("auth")));
    assert_eq!(service.get("tier"), Some(&Value::from("critical")));
}

#[test]
fn test_csv_lookup_miss_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = write_file(
        dir.path(),
        "services.csv",
        "service_id,name\nsvc-001,auth\n",
    );

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "services".into(),
        path: csv_path,
        key_columns: vec!["service_id".into()],
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    let program = compile_with_registry(
        r#".service = get_enrichment_table_record!("services", {"service_id": .service_id})"#,
        registry,
    );

    let result = run_transform(&program, serde_json::json!({"service_id": "svc-999"}));

    let obj = result.as_object().unwrap();
    assert_eq!(obj.get("service"), Some(&Value::Null));
}

// =========================================================================
// JSON enrichment
// =========================================================================

#[test]
fn test_json_lookup_hit() {
    let dir = tempfile::tempdir().unwrap();
    let json_path = write_file(
        dir.path(),
        "geo.json",
        r#"[
            {"country_code": "AU", "country_name": "Australia", "region": "APAC"},
            {"country_code": "US", "country_name": "United States", "region": "NA"}
        ]"#,
    );

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "geo".into(),
        path: json_path,
        key_columns: vec!["country_code".into()],
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    let program = compile_with_registry(
        r#".geo = get_enrichment_table_record!("geo", {"country_code": .cc})"#,
        registry,
    );

    let result = run_transform(&program, serde_json::json!({"cc": "AU"}));

    let geo = result
        .as_object()
        .unwrap()
        .get("geo")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(geo.get("country_name"), Some(&Value::from("Australia")));
    assert_eq!(geo.get("region"), Some(&Value::from("APAC")));
}

// =========================================================================
// find_enrichment_table_records
// =========================================================================

#[test]
fn test_find_records_multiple_matches() {
    let dir = tempfile::tempdir().unwrap();
    let json_path = write_file(
        dir.path(),
        "cities.json",
        r#"[
            {"country": "AU", "city": "Sydney"},
            {"country": "AU", "city": "Melbourne"},
            {"country": "US", "city": "NYC"}
        ]"#,
    );

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "cities".into(),
        path: json_path,
        key_columns: vec!["country".into(), "city".into()],
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    let program = compile_with_registry(
        r#".au_cities = find_enrichment_table_records!("cities", {"country": "AU"})"#,
        registry,
    );

    let result = run_transform(&program, serde_json::json!({"event": "test"}));

    let cities = result
        .as_object()
        .unwrap()
        .get("au_cities")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(cities.len(), 2);
}

#[test]
fn test_find_records_no_matches() {
    let dir = tempfile::tempdir().unwrap();
    let json_path = write_file(
        dir.path(),
        "data.json",
        r#"[{"country": "AU", "city": "Sydney"}]"#,
    );

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "data".into(),
        path: json_path,
        key_columns: vec!["country".into()],
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    let program = compile_with_registry(
        r#".results = find_enrichment_table_records!("data", {"country": "XX"})"#,
        registry,
    );

    let result = run_transform(&program, serde_json::json!({"event": "test"}));

    let results = result
        .as_object()
        .unwrap()
        .get("results")
        .unwrap()
        .as_array()
        .unwrap();
    assert!(results.is_empty());
}

// =========================================================================
// Multi-key lookup
// =========================================================================

#[test]
fn test_multi_key_csv_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = write_file(
        dir.path(),
        "geo.csv",
        "country,city,timezone\nAU,Sydney,AEST\nAU,Melbourne,AEST\nUS,NYC,EST\n",
    );

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "geo".into(),
        path: csv_path,
        key_columns: vec!["country".into(), "city".into()],
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    let program = compile_with_registry(
        r#".tz_info = get_enrichment_table_record!("geo", {"country": .country, "city": .city})"#,
        registry,
    );

    let result = run_transform(
        &program,
        serde_json::json!({"country": "AU", "city": "Sydney"}),
    );

    let tz_info = result
        .as_object()
        .unwrap()
        .get("tz_info")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(tz_info.get("timezone"), Some(&Value::from("AEST")));
}

// =========================================================================
// Multiple tables
// =========================================================================

#[test]
fn test_multiple_tables_in_single_vrl() {
    let dir = tempfile::tempdir().unwrap();
    let svc_path = write_file(
        dir.path(),
        "services.csv",
        "service_id,name\nsvc-001,auth\n",
    );
    let geo_path = write_file(
        dir.path(),
        "geo.json",
        r#"[{"cc": "AU", "name": "Australia"}]"#,
    );

    let registry = EnrichmentRegistry::load(&[
        EnrichmentTableConfig {
            name: "services".into(),
            path: svc_path,
            key_columns: vec!["service_id".into()],
            ..Default::default()
        },
        EnrichmentTableConfig {
            name: "geo".into(),
            path: geo_path,
            key_columns: vec!["cc".into()],
            ..Default::default()
        },
    ])
    .unwrap()
    .into_arc();

    let source = r#"
        .svc = get_enrichment_table_record!("services", {"service_id": .service_id})
        .geo = get_enrichment_table_record!("geo", {"cc": .country})
    "#;
    let program = compile_with_registry(source, registry);

    let result = run_transform(
        &program,
        serde_json::json!({"service_id": "svc-001", "country": "AU"}),
    );

    let obj = result.as_object().unwrap();
    let svc = obj.get("svc").unwrap().as_object().unwrap();
    assert_eq!(svc.get("name"), Some(&Value::from("auth")));

    let geo = obj.get("geo").unwrap().as_object().unwrap();
    assert_eq!(geo.get("name"), Some(&Value::from("Australia")));
}

// =========================================================================
// Compile-time failure cases
// =========================================================================

#[test]
fn test_enrichment_without_registry_fails_compilation() {
    let result = compile_vrl(
        r#".x = get_enrichment_table_record!("foo", {"id": .id})"#,
        None,
    );
    assert!(result.is_err());
}

// =========================================================================
// Startup failure: missing enrichment file
// =========================================================================

#[test]
fn test_missing_enrichment_file_fails_load() {
    let result = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "missing".into(),
        path: "/nonexistent/file.csv".into(),
        key_columns: vec!["id".into()],
        ..Default::default()
    }]);
    assert!(result.is_err());
}

// =========================================================================
// Null/missing field handling
// =========================================================================

#[test]
fn test_lookup_with_null_field_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = write_file(dir.path(), "svc.csv", "service_id,name\nsvc-001,auth\n");

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "svc".into(),
        path: csv_path,
        key_columns: vec!["service_id".into()],
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    // When .service_id is null, the condition object has a null value
    // which won't match any key — should return null
    let program = compile_with_registry(
        r#".result = get_enrichment_table_record!("svc", {"service_id": .service_id})"#,
        registry,
    );

    let result = run_transform(&program, serde_json::json!({"other_field": "value"}));

    let obj = result.as_object().unwrap();
    assert_eq!(obj.get("result"), Some(&Value::Null));
}

// =========================================================================
// YAML enrichment
// =========================================================================

#[test]
fn test_yaml_lookup_hit() {
    let dir = tempfile::tempdir().unwrap();
    let _path = write_file(
        dir.path(),
        "routes.yaml",
        "- route_id: SYD-MEL\n  origin: YSSY\n  destination: YMML\n- route_id: SYD-BNE\n  origin: YSSY\n  destination: YBBN\n",
    );

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "routes".into(),
        path: String::new(),
        key_columns: vec!["route_id".into()],
        source: Some(dfe_transform_vrl::config::EnrichmentSourceConfig::File {
            path: dir.path().join("routes.yaml").to_string_lossy().to_string(),
            format: Some(dfe_transform_vrl::config::FileFormat::Yaml),
        }),
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    let program = compile_with_registry(
        r#".route = get_enrichment_table_record!("routes", {"route_id": .route_id})"#,
        registry,
    );

    let result = run_transform(&program, serde_json::json!({"route_id": "SYD-MEL"}));

    let route = result
        .as_object()
        .unwrap()
        .get("route")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(route.get("origin"), Some(&Value::from("YSSY")));
    assert_eq!(route.get("destination"), Some(&Value::from("YMML")));
}

// =========================================================================
// STIX enrichment (file-based)
// =========================================================================

#[test]
fn test_stix_file_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let stix_content = r#"{
        "type": "bundle",
        "objects": [
            {
                "type": "indicator",
                "pattern": "[ipv4-addr:value = '203.0.113.50']",
                "confidence": 85,
                "name": "Known C2 server"
            },
            {
                "type": "indicator",
                "pattern": "[domain-name:value = 'evil.example.com']",
                "confidence": 70,
                "name": "Phishing domain"
            }
        ]
    }"#;
    let _path = write_file(dir.path(), "stix.json", stix_content);

    let registry = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "threats".into(),
        path: String::new(),
        key_columns: vec!["indicator".into()],
        source: Some(dfe_transform_vrl::config::EnrichmentSourceConfig::Stix {
            path: Some(dir.path().join("stix.json").to_string_lossy().to_string()),
            url: None,
            collection: None,
            auth: None,
        }),
        ..Default::default()
    }])
    .unwrap()
    .into_arc();

    let program = compile_with_registry(
        r#".threat = get_enrichment_table_record!("threats", {"indicator": .src_ip})"#,
        registry,
    );

    let result = run_transform(&program, serde_json::json!({"src_ip": "203.0.113.50"}));

    let threat = result
        .as_object()
        .unwrap()
        .get("threat")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(threat.get("name"), Some(&Value::from("Known C2 server")));
    assert_eq!(threat.get("confidence"), Some(&Value::Integer(85)));
    assert_eq!(threat.get("indicator_type"), Some(&Value::from("ipv4")));
}

// =========================================================================
// max_bytes enforcement
// =========================================================================

#[test]
fn test_max_bytes_enforcement() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = write_file(dir.path(), "big.csv", "id,data\n1,aaaa\n2,bbbb\n3,cccc\n");

    let result = EnrichmentRegistry::load(&[EnrichmentTableConfig {
        name: "big".into(),
        path: csv_path,
        key_columns: vec!["id".into()],
        max_bytes: Some(1), // 1 byte — will always exceed
        ..Default::default()
    }]);

    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("exceeds max_bytes"));
}
