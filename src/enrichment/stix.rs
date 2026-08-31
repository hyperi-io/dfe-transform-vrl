//! Project:   dfe-transform-vrl
//! File:      src/enrichment/stix.rs
//! Purpose:   STIX 2.1 indicator parsing and materialisation into `FxHashMap`
//! Language:  Rust
//!
//! License:   BUSL-1.1
//! Copyright: (c) 2026 HYPERI PTY LIMITED

//! Parse STIX 2.1 bundles (file or HTTP) and materialise indicators into
//! `FxHashMap<CompactKey, Arc<ObjectMap>>` for O(1) enrichment lookups.
//!
//! Supports both file-based STIX bundles (mounted as `ConfigMap`) and
//! HTTP-fetched TAXII feeds. No dedicated STIX crate — STIX 2.1 is JSON
//! with a known schema, parsed via `serde_json`.

use std::sync::Arc;

use serde::Deserialize;
use vrl::value::{KeyString, ObjectMap, Value};

use crate::config::StixAuthConfig;
use crate::enrichment::table::{CompactKey, RowMap};

// ---------------------------------------------------------------------------
// STIX 2.1 model (minimal — just what we need for indicator extraction)
// ---------------------------------------------------------------------------

/// STIX 2.1 bundle envelope.
#[derive(Debug, Deserialize)]
struct StixBundle {
    #[serde(default)]
    objects: Vec<StixObject>,
}

/// STIX 2.1 object (we only extract indicators).
#[derive(Debug, Deserialize)]
struct StixObject {
    #[serde(rename = "type")]
    object_type: String,
    #[serde(default)]
    pattern: Option<String>,
    #[serde(default)]
    confidence: Option<u32>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    created: Option<String>,
    #[serde(default)]
    valid_from: Option<String>,
    #[serde(default)]
    valid_until: Option<String>,
    #[serde(default)]
    labels: Option<Vec<String>>,
    #[serde(default)]
    object_marking_refs: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Load a STIX 2.1 bundle from file or HTTP and materialise indicators.
///
/// - If `path` is set, reads from local file (sync).
/// - If `url` is set, fetches via HTTP (requires tokio runtime).
/// - `path` takes precedence if both are set.
///
/// Returns `FxHashMap` keyed by indicator value (IP, domain, hash, etc.).
pub fn load_stix(
    path: Option<&str>,
    url: Option<&str>,
    _auth: Option<&StixAuthConfig>,
    table_name: &str,
    key_columns: &[String],
) -> crate::Result<RowMap> {
    let json_bytes = if let Some(path) = path {
        std::fs::read(path).map_err(|e| {
            crate::Error::Enrichment(format!("table '{table_name}': read STIX file {path}: {e}"))
        })?
    } else if let Some(_url) = url {
        // HTTP fetch will be wired via scalo HttpClient in the async refresh path.
        // For now, return a clear error if only URL is provided without async context.
        return Err(crate::Error::Enrichment(format!(
            "table '{table_name}': STIX HTTP fetch requires async context (use refresh task or load from file)"
        )));
    } else {
        return Err(crate::Error::Enrichment(format!(
            "table '{table_name}': STIX source needs 'path' or 'url'"
        )));
    };

    materialise_stix(&json_bytes, table_name, key_columns)
}

/// Parse STIX 2.1 JSON and materialise indicators into an `FxHashMap`.
///
/// Extracts indicator objects, parses their STIX patterns to get the
/// indicator value (IP, domain, hash), and builds a lookup table.
pub fn materialise_stix(
    json_bytes: &[u8],
    table_name: &str,
    key_columns: &[String],
) -> crate::Result<RowMap> {
    let bundle: StixBundle = serde_json::from_slice(json_bytes).map_err(|e| {
        crate::Error::Enrichment(format!("table '{table_name}': invalid STIX JSON: {e}"))
    })?;

    let mut rows = RowMap::default();

    for obj in &bundle.objects {
        if obj.object_type != "indicator" {
            continue;
        }

        let Some(ref pattern) = obj.pattern else {
            continue;
        };

        // Extract indicator value from STIX pattern
        let Some(indicator_value) = extract_indicator_from_pattern(pattern) else {
            continue;
        };

        let mut row = ObjectMap::new();
        row.insert(
            KeyString::from("indicator"),
            Value::from(indicator_value.as_str()),
        );
        row.insert(
            KeyString::from("indicator_type"),
            Value::from(indicator_type_from_pattern(pattern)),
        );

        if let Some(ref name) = obj.name {
            row.insert(KeyString::from("name"), Value::from(name.as_str()));
        }
        if let Some(ref desc) = obj.description {
            row.insert(KeyString::from("description"), Value::from(desc.as_str()));
        }
        if let Some(confidence) = obj.confidence {
            row.insert(
                KeyString::from("confidence"),
                Value::Integer(i64::from(confidence)),
            );
        }
        if let Some(ref id) = obj.id {
            row.insert(KeyString::from("stix_id"), Value::from(id.as_str()));
        }
        if let Some(ref created) = obj.created {
            row.insert(KeyString::from("created"), Value::from(created.as_str()));
        }
        if let Some(ref valid_from) = obj.valid_from {
            row.insert(
                KeyString::from("valid_from"),
                Value::from(valid_from.as_str()),
            );
        }
        if let Some(ref valid_until) = obj.valid_until {
            row.insert(
                KeyString::from("valid_until"),
                Value::from(valid_until.as_str()),
            );
        }
        if let Some(ref labels) = obj.labels {
            let label_values: Vec<Value> = labels.iter().map(|l| Value::from(l.as_str())).collect();
            row.insert(KeyString::from("labels"), Value::Array(label_values));
        }

        // Extract TLP from marking refs (simplified — looks for TLP marking IDs)
        if let Some(ref markings) = obj.object_marking_refs
            && let Some(tlp) = extract_tlp_from_markings(markings)
        {
            row.insert(KeyString::from("tlp"), Value::from(tlp));
        }

        let key = CompactKey::from_row(&row, key_columns);
        rows.entry(key).or_default().push(Arc::new(row));
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// STIX pattern parsing
// ---------------------------------------------------------------------------

/// Extract the indicator value from a STIX 2.1 pattern string.
///
/// Handles common patterns:
/// - `[ipv4-addr:value = '1.2.3.4']`
/// - `[ipv6-addr:value = '2001:db8::1']`
/// - `[domain-name:value = 'evil.example.com']`
/// - `[file:hashes.'SHA-256' = 'abc123...']`
/// - `[url:value = 'http://evil.example.com/malware']`
/// - `[email-addr:value = 'phish@evil.com']`
fn extract_indicator_from_pattern(pattern: &str) -> Option<String> {
    // Find the value between single quotes after '='
    let eq_pos = pattern.find('=')?;
    let after_eq = &pattern[eq_pos + 1..];
    let open_quote = after_eq.find('\'')?;
    let value_start = open_quote + 1;
    let close_quote = after_eq[value_start..].find('\'')?;
    let value = &after_eq[value_start..value_start + close_quote];

    if value.is_empty() {
        return None;
    }

    Some(value.to_string())
}

/// Determine the indicator type from a STIX pattern prefix.
fn indicator_type_from_pattern(pattern: &str) -> &'static str {
    if pattern.contains("ipv4-addr") {
        "ipv4"
    } else if pattern.contains("ipv6-addr") {
        "ipv6"
    } else if pattern.contains("domain-name") {
        "domain"
    } else if pattern.contains("file:hashes") {
        "hash"
    } else if pattern.contains("url:") {
        "url"
    } else if pattern.contains("email-addr") {
        "email"
    } else {
        "unknown"
    }
}

/// Extract TLP level from STIX marking definition references.
///
/// Standard TLP marking definition IDs:
/// - `marking-definition--613f2e26-407d-48c7-9eca-b8e91df99dc9` = TLP:WHITE
/// - `marking-definition--34098fce-860f-48ae-8e50-ebd3cc5e41da` = TLP:GREEN
/// - `marking-definition--f88d31f6-486f-44da-b317-01333bde0b82` = TLP:AMBER
/// - `marking-definition--5e57c739-391a-4eb3-b6be-7d15ca92d5ed` = TLP:RED
fn extract_tlp_from_markings(markings: &[String]) -> Option<&'static str> {
    for marking in markings {
        if marking.contains("613f2e26") {
            return Some("white");
        }
        if marking.contains("34098fce") {
            return Some("green");
        }
        if marking.contains("f88d31f6") {
            return Some("amber");
        }
        if marking.contains("5e57c739") {
            return Some("red");
        }
    }
    None
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The single row materialised for `indicator`.
    fn indicator_row(rows: &RowMap, indicator: &str) -> Arc<ObjectMap> {
        let mut probe = ObjectMap::new();
        probe.insert("indicator".into(), Value::from(indicator));
        let key = CompactKey::from_row(&probe, &["indicator".to_string()]);
        let bucket = rows.get(&key).unwrap();
        assert_eq!(bucket.len(), 1);
        Arc::clone(&bucket[0])
    }

    fn sample_stix_bundle() -> &'static str {
        r#"{
            "type": "bundle",
            "id": "bundle--test-001",
            "objects": [
                {
                    "type": "indicator",
                    "id": "indicator--001",
                    "pattern": "[ipv4-addr:value = '203.0.113.50']",
                    "confidence": 85,
                    "name": "Malicious IP",
                    "description": "Known C2 server",
                    "created": "2026-01-01T00:00:00Z",
                    "valid_from": "2026-01-01T00:00:00Z",
                    "labels": ["malicious-activity"],
                    "object_marking_refs": ["marking-definition--f88d31f6-486f-44da-b317-01333bde0b82"]
                },
                {
                    "type": "indicator",
                    "id": "indicator--002",
                    "pattern": "[domain-name:value = 'evil.example.com']",
                    "confidence": 70,
                    "name": "Phishing domain"
                },
                {
                    "type": "indicator",
                    "id": "indicator--003",
                    "pattern": "[file:hashes.'SHA-256' = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855']",
                    "confidence": 95,
                    "name": "Known malware hash"
                },
                {
                    "type": "malware",
                    "id": "malware--001",
                    "name": "BadBot"
                }
            ]
        }"#
    }

    #[test]
    fn materialise_stix_extracts_indicators() {
        let rows = materialise_stix(
            sample_stix_bundle().as_bytes(),
            "threats",
            &["indicator".into()],
        )
        .unwrap();

        // 3 indicators (malware object is skipped)
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn materialise_stix_ip_lookup() {
        let rows = materialise_stix(
            sample_stix_bundle().as_bytes(),
            "threats",
            &["indicator".into()],
        )
        .unwrap();

        let row = indicator_row(&rows, "203.0.113.50");

        assert_eq!(row.get("name"), Some(&Value::from("Malicious IP")));
        assert_eq!(row.get("confidence"), Some(&Value::Integer(85)));
        assert_eq!(row.get("indicator_type"), Some(&Value::from("ipv4")));
        assert_eq!(row.get("tlp"), Some(&Value::from("amber")));
    }

    #[test]
    fn materialise_stix_domain_lookup() {
        let rows = materialise_stix(
            sample_stix_bundle().as_bytes(),
            "threats",
            &["indicator".into()],
        )
        .unwrap();

        let row = indicator_row(&rows, "evil.example.com");

        assert_eq!(row.get("indicator_type"), Some(&Value::from("domain")));
    }

    #[test]
    fn materialise_stix_hash_lookup() {
        let rows = materialise_stix(
            sample_stix_bundle().as_bytes(),
            "threats",
            &["indicator".into()],
        )
        .unwrap();

        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let row = indicator_row(&rows, hash);

        assert_eq!(row.get("indicator_type"), Some(&Value::from("hash")));
        assert_eq!(row.get("confidence"), Some(&Value::Integer(95)));
    }

    #[test]
    fn materialise_stix_invalid_json() {
        assert!(materialise_stix(b"not json", "t", &["indicator".into()]).is_err());
    }

    #[test]
    fn materialise_stix_empty_bundle() {
        let bundle = r#"{"type": "bundle", "objects": []}"#;
        let rows = materialise_stix(bundle.as_bytes(), "t", &["indicator".into()]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn materialise_stix_no_indicators() {
        let bundle = r#"{"type": "bundle", "objects": [{"type": "malware", "name": "x"}]}"#;
        let rows = materialise_stix(bundle.as_bytes(), "t", &["indicator".into()]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn materialise_stix_indicator_without_pattern_skipped() {
        let bundle =
            r#"{"type": "bundle", "objects": [{"type": "indicator", "name": "no pattern"}]}"#;
        let rows = materialise_stix(bundle.as_bytes(), "t", &["indicator".into()]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn extract_indicator_ipv4() {
        let pattern = "[ipv4-addr:value = '1.2.3.4']";
        assert_eq!(
            extract_indicator_from_pattern(pattern),
            Some("1.2.3.4".into())
        );
    }

    #[test]
    fn extract_indicator_domain() {
        let pattern = "[domain-name:value = 'evil.com']";
        assert_eq!(
            extract_indicator_from_pattern(pattern),
            Some("evil.com".into())
        );
    }

    #[test]
    fn extract_indicator_no_quotes() {
        let pattern = "[ipv4-addr:value = ]";
        assert!(extract_indicator_from_pattern(pattern).is_none());
    }

    #[test]
    fn extract_indicator_empty_value() {
        let pattern = "[ipv4-addr:value = '']";
        assert!(extract_indicator_from_pattern(pattern).is_none());
    }

    #[test]
    fn indicator_type_detection() {
        assert_eq!(
            indicator_type_from_pattern("[ipv4-addr:value = '1.2.3.4']"),
            "ipv4"
        );
        assert_eq!(
            indicator_type_from_pattern("[ipv6-addr:value = '::1']"),
            "ipv6"
        );
        assert_eq!(
            indicator_type_from_pattern("[domain-name:value = 'x.com']"),
            "domain"
        );
        assert_eq!(
            indicator_type_from_pattern("[file:hashes.'SHA-256' = 'abc']"),
            "hash"
        );
        assert_eq!(
            indicator_type_from_pattern("[url:value = 'http://x']"),
            "url"
        );
        assert_eq!(
            indicator_type_from_pattern("[email-addr:value = 'x@y']"),
            "email"
        );
        assert_eq!(
            indicator_type_from_pattern("[unknown:value = 'x']"),
            "unknown"
        );
    }

    #[test]
    fn tlp_extraction() {
        let markings = vec!["marking-definition--f88d31f6-486f-44da-b317-01333bde0b82".into()];
        assert_eq!(extract_tlp_from_markings(&markings), Some("amber"));

        let markings = vec!["marking-definition--5e57c739-391a-4eb3-b6be-7d15ca92d5ed".into()];
        assert_eq!(extract_tlp_from_markings(&markings), Some("red"));

        let markings: Vec<String> = vec![];
        assert_eq!(extract_tlp_from_markings(&markings), None);
    }

    #[test]
    fn load_stix_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stix.json");
        std::fs::write(&path, sample_stix_bundle()).unwrap();

        let rows = load_stix(
            Some(path.to_str().unwrap()),
            None,
            None,
            "threats",
            &["indicator".into()],
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn load_stix_no_source_errors() {
        assert!(load_stix(None, None, None, "t", &["indicator".into()]).is_err());
    }

    #[test]
    fn load_stix_missing_file_errors() {
        assert!(
            load_stix(
                Some("/nonexistent/stix.json"),
                None,
                None,
                "t",
                &["indicator".into()]
            )
            .is_err()
        );
    }
}
