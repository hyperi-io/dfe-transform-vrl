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
use vrl::compiler::function::{
    self, ArgumentList, Compiled, Example, FunctionCompileContext, Parameter,
};
use vrl::compiler::{CompilationResult, CompileConfig, Function, compile_with_external, state};
use vrl::stdlib::{ParseEtld, ParseGroks};
use vrl::value::Value;

use crate::Result;
use crate::config::TransformConfig;
use crate::enrichment::EnrichmentRegistry;
use crate::enrichment::vrl_functions;

/// How many VRL programs this configuration names on disk.
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
///
/// # Errors
///
/// Returns an error when a VRL file or directory cannot be read.
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
    info!(
        file_count = sources.len(),
        total_bytes = combined.len(),
        "loaded VRL source"
    );
    Ok(combined)
}

/// vrl functions a transform may not call, though the stdlib features we build
/// still carry them: they read the host's name or a file the program names, or
/// decompress a record with no bound on the output.
const REFUSED_FUNCTIONS: &[&str] = &[
    "get_hostname",
    "parse_proto",
    "encode_proto",
    "validate_json_schema",
    "decode_gzip",
    "decode_zlib",
    "decode_zstd",
    "decode_lz4",
    "decode_snappy",
];

/// A vrl function that refuses its file argument, a path the program names and
/// the pod reads when the program compiles: `parse_etld`'s `psl` and
/// `parse_groks`' `alias_sources`. The rest of the function is unchanged.
#[derive(Clone, Copy, Debug)]
struct WithoutFileArgument<F> {
    inner: F,
    file_argument: &'static str,
}

/// The stdlib functions that take a file argument, each without it.
const WITHOUT_FILE_ARGUMENT: [WithoutFileArgument<&'static dyn Function>; 2] = [
    WithoutFileArgument {
        inner: &ParseEtld,
        file_argument: "psl",
    },
    WithoutFileArgument {
        inner: &ParseGroks,
        file_argument: "alias_sources",
    },
];

impl Function for WithoutFileArgument<&'static dyn Function> {
    fn identifier(&self) -> &'static str {
        self.inner.identifier()
    }

    fn summary(&self) -> &'static str {
        self.inner.summary()
    }

    fn usage(&self) -> &'static str {
        self.inner.usage()
    }

    fn category(&self) -> &'static str {
        self.inner.category()
    }

    fn internal_failure_reasons(&self) -> &'static [&'static str] {
        self.inner.internal_failure_reasons()
    }

    fn return_kind(&self) -> u16 {
        self.inner.return_kind()
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }

    fn compile(
        &self,
        state: &state::TypeState,
        ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        if arguments.optional(self.file_argument).is_some() {
            return Err(function::Error::InvalidArgument {
                keyword: self.file_argument,
                value: Value::Null,
                error: "a transform may not name a file for the pod to read",
            }
            .into());
        }
        self.inner.compile(state, ctx, arguments)
    }

    fn parameters(&self) -> &'static [Parameter] {
        self.inner.parameters()
    }
}

/// Compile VRL source code into an executable program.
///
/// Uses the VRL stdlib, less any function that reads the host, a file the
/// program names, or decompresses without a bound, plus custom enrichment
/// functions. A refused function is an undefined one.
///
/// The `EnrichmentRegistry` is injected into the compile config, and the
/// enrichment functions use it to resolve the table named in each call. The
/// table name must be a compile-time literal naming a registered table, so a
/// typo fails here rather than on the millionth event, and a VRL program can
/// never compute a table name. Passing `None` therefore makes any call to
/// `get_enrichment_table_record` or `find_enrichment_table_records` a
/// compile error.
///
/// The compiled `Program` is reused for every event -- zero per-event
/// compilation cost.
///
/// # Errors
///
/// Returns an error when the VRL source fails to compile, or calls an enrichment function it may not.
pub fn compile_vrl(
    source: &str,
    registry: Option<Arc<EnrichmentRegistry>>,
) -> Result<CompilationResult> {
    let mut fns = vrl::stdlib::all();
    fns.retain(|f| {
        !REFUSED_FUNCTIONS.contains(&f.identifier())
            && !WITHOUT_FILE_ARGUMENT
                .iter()
                .any(|w| w.identifier() == f.identifier())
    });
    fns.extend(
        WITHOUT_FILE_ARGUMENT
            .iter()
            .map(|w| Box::new(*w) as Box<dyn Function>),
    );
    fns.extend(vrl_functions::enrichment_functions());

    let mut config = CompileConfig::default();
    if let Some(reg) = registry {
        config.set_custom(reg);
    }

    let external = state::ExternalEnv::default();
    // Rendered against the source, so the error quotes the call that failed.
    compile_with_external(source, &fns, &external, config).map_err(|diagnostics| {
        crate::Error::VrlCompile(vrl::diagnostic::Formatter::new(source, diagnostics).to_string())
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

    /// A vrl release that renames a file argument would let it through, so each
    /// name must still be one its function takes.
    #[test]
    fn each_refused_file_argument_is_one_the_function_takes() {
        for wrapper in WITHOUT_FILE_ARGUMENT {
            assert!(
                wrapper
                    .parameters()
                    .iter()
                    .any(|p| p.keyword == wrapper.file_argument),
                "{} has no `{}` argument",
                wrapper.identifier(),
                wrapper.file_argument
            );
        }
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
