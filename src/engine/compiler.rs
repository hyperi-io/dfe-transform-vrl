// Project:   dfe-transform-vrl
// File:      src/engine/compiler.rs
// Purpose:   VRL program compilation from transform files
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! VRL program compilation.
//!
//! Loads VRL source from transform files, compiles into executable programs.

use std::path::Path;
use std::sync::Arc;

use tracing::{debug, info};
use vrl::compiler::{CompilationResult, CompileConfig, compile_with_external, state};

use crate::Result;
use crate::config::TransformConfig;
use crate::enrichment::EnrichmentRegistry;
use crate::enrichment::vrl_functions;

/// How many VRL programs this configuration currently names on disk.
///
/// A config can legitimately name a directory that is empty, or not there yet:
/// the deployment writes the program after the instance exists, so a transform
/// is deployed before it has one. That is no work rather than a bad config, so
/// this counts rather than refusing and the idle gate decides
/// (`Config::work_state`).
#[must_use]
pub fn program_count(config: &TransformConfig) -> usize {
    let in_dir = config
        .dir
        .as_ref()
        .and_then(|dir| std::fs::read_dir(Path::new(dir)).ok())
        .map_or(0, |entries| {
            entries
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "vrl"))
                .count()
        });
    let named = config.files.as_ref().map_or(0, |files| {
        files.iter().filter(|f| Path::new(f).is_file()).count()
    });
    in_dir + named
}

/// Load VRL source code from transform configuration.
///
/// If `dir` is specified, loads all `.vrl` files sorted by filename.
/// If `files` is specified, loads files in the given order.
/// Files are concatenated with newlines between them.
pub fn load_vrl_source(config: &TransformConfig) -> Result<String> {
    let mut sources = Vec::new();

    if let Some(ref dir) = config.dir {
        let dir_path = Path::new(dir);
        if !dir_path.is_dir() {
            return Err(crate::Error::Config(format!(
                "transforms.dir does not exist or is not a directory: {dir}"
            )));
        }

        let mut entries: Vec<_> = std::fs::read_dir(dir_path)
            .map_err(|e| crate::Error::Config(format!("failed to read transforms dir: {e}")))?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "vrl"))
            .collect();

        entries.sort_by_key(std::fs::DirEntry::file_name);

        for entry in entries {
            let path = entry.path();
            let content = std::fs::read_to_string(&path).map_err(|e| {
                crate::Error::Config(format!(
                    "failed to read transform file {}: {e}",
                    path.display()
                ))
            })?;
            debug!(file = %path.display(), "loaded VRL transform file");
            sources.push(content);
        }
    }

    if let Some(ref files) = config.files {
        for file_path in files {
            let path = Path::new(file_path);
            if !path.is_file() {
                return Err(crate::Error::Config(format!(
                    "transform file does not exist: {file_path}"
                )));
            }
            let content = std::fs::read_to_string(path).map_err(|e| {
                crate::Error::Config(format!("failed to read transform file {file_path}: {e}"))
            })?;
            debug!(file = file_path, "loaded VRL transform file");
            sources.push(content);
        }
    }

    if sources.is_empty() {
        return Err(crate::Error::Config("no VRL transform files found".into()));
    }

    let combined = sources.join("\n\n");
    // programs_loaded is always 1 — VRL concatenates all files into a single program
    debug!(
        file_count = sources.len(),
        total_bytes = combined.len(),
        programs_loaded = 1,
        "VRL source loaded, ready for compilation"
    );
    info!(
        file_count = sources.len(),
        total_bytes = combined.len(),
        "loaded VRL source"
    );
    Ok(combined)
}

/// Compile VRL source code into an executable program.
///
/// Uses the full VRL stdlib plus custom enrichment functions.
///
/// The `EnrichmentRegistry` is injected into the compile config, and the
/// enrichment functions use it to resolve the table named in each call. The
/// table name must be a compile-time literal naming a registered table, so a
/// typo fails here rather than on the millionth event, and a VRL program can
/// never compute a table name. Passing `None` therefore makes any call to
/// `get_enrichment_table_record` or `find_enrichment_table_records` a
/// compile error.
///
/// The compiled `Program` is reused for every event — zero per-event
/// compilation cost.
pub fn compile_vrl(
    source: &str,
    registry: Option<Arc<EnrichmentRegistry>>,
) -> Result<CompilationResult> {
    let mut fns = vrl::stdlib::all();
    fns.extend(vrl_functions::enrichment_functions());

    let mut config = CompileConfig::default();
    if let Some(reg) = registry {
        config.set_custom(reg);
    }

    let external = state::ExternalEnv::default();
    compile_with_external(source, &fns, &external, config).map_err(|diagnostics| {
        let messages: Vec<String> = diagnostics.into_iter().map(|d| format!("{d:?}")).collect();
        crate::Error::VrlCompile(messages.join("\n"))
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_load_from_dir() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("01_first.vrl"), ".a = 1\n").unwrap();
        fs::write(dir.path().join("02_second.vrl"), ".b = 2\n").unwrap();
        fs::write(dir.path().join("readme.txt"), "not a vrl file").unwrap();

        let config = TransformConfig {
            dir: Some(dir.path().to_string_lossy().to_string()),
            files: None,
        };

        let source = load_vrl_source(&config).unwrap();
        assert!(source.contains(".a = 1"));
        assert!(source.contains(".b = 2"));
        assert!(!source.contains("not a vrl file"));
    }

    #[test]
    fn test_load_from_files() {
        let dir = tempfile::tempdir().unwrap();
        let file_a = dir.path().join("a.vrl");
        let file_b = dir.path().join("b.vrl");
        fs::write(&file_a, ".x = true\n").unwrap();
        fs::write(&file_b, ".y = false\n").unwrap();

        let config = TransformConfig {
            dir: None,
            files: Some(vec![
                file_a.to_string_lossy().to_string(),
                file_b.to_string_lossy().to_string(),
            ]),
        };

        let source = load_vrl_source(&config).unwrap();
        assert!(source.contains(".x = true"));
        assert!(source.contains(".y = false"));
    }

    #[test]
    fn test_missing_dir() {
        let config = TransformConfig {
            dir: Some("/nonexistent/path".to_string()),
            files: None,
        };
        assert!(load_vrl_source(&config).is_err());
    }

    #[test]
    fn test_missing_file() {
        let config = TransformConfig {
            dir: None,
            files: Some(vec!["/nonexistent/file.vrl".to_string()]),
        };
        assert!(load_vrl_source(&config).is_err());
    }

    #[test]
    fn test_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let config = TransformConfig {
            dir: Some(dir.path().to_string_lossy().to_string()),
            files: None,
        };
        assert!(load_vrl_source(&config).is_err());
    }

    #[test]
    fn test_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("03_third.vrl"), "THIRD").unwrap();
        fs::write(dir.path().join("01_first.vrl"), "FIRST").unwrap();
        fs::write(dir.path().join("02_second.vrl"), "SECOND").unwrap();

        let config = TransformConfig {
            dir: Some(dir.path().to_string_lossy().to_string()),
            files: None,
        };

        let source = load_vrl_source(&config).unwrap();
        let first_pos = source.find("FIRST").unwrap();
        let second_pos = source.find("SECOND").unwrap();
        let third_pos = source.find("THIRD").unwrap();
        assert!(first_pos < second_pos);
        assert!(second_pos < third_pos);
    }

    #[test]
    fn test_compile_valid_vrl() {
        let result = compile_vrl(".processed = true\n.timestamp = now()", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compile_invalid_vrl() {
        let result = compile_vrl("this is not valid VRL !!!{{{", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_compile_with_stdlib_functions() {
        let source = r#"
            .parsed = parse_json!("{\"key\": \"value\"}")
            .upper = upcase!("hello")
            .ts = now()
        "#;
        let result = compile_vrl(source, None);
        assert!(result.is_ok());
    }
}
