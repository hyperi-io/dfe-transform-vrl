// Project:   dfe-transform-vrl
// File:      src/engine/compiler.rs
// Purpose:   VRL program compilation from transform files
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! VRL program compilation.
//!
//! Loads VRL source from transform files, compiles into executable programs.

use std::path::Path;

use tracing::{debug, info};

use crate::Result;
use crate::config::TransformConfig;

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
            .filter_map(|entry| entry.ok())
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
    info!(
        file_count = sources.len(),
        total_bytes = combined.len(),
        "loaded VRL source"
    );
    Ok(combined)
}

#[cfg(test)]
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
}
