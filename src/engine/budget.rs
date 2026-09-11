// Project:   dfe-transform-vrl
// File:      src/engine/budget.rs
// Purpose:   Memory floor the VRL compiler needs, checked before it runs
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Memory floor for VRL compilation, checked before the compiler runs.
//!
//! Compiling is the one startup step whose cost scales with the deployment's
//! own data: a per-source transform instance gets whatever VRL its source
//! implies, and the bundled filebeat pipeline is 212 KB of it. Below a certain
//! container limit the kernel kills the process mid-compile, which surfaces as
//! an exit-137 restart loop with nothing in the log to say what was too small.
//! Reading the limit first turns that into one refusal that names the numbers.

use tracing::info;

use crate::Result;

/// Resident bytes the compiler needs per byte of VRL source.
///
/// Measured on the shipped image (2 CPUs, jemalloc) against the bundled
/// filebeat pipeline: 212,218 bytes compiles under a 40 MiB container limit
/// and is OOM-killed under 32 MiB; the same file concatenated twice (424,438
/// bytes) compiles under 56 MiB and is killed under 48 MiB. That is 79 bytes
/// per source byte between the two points, rounded up for headroom.
const COMPILE_BYTES_PER_SOURCE_BYTE: u64 = 96;

/// Resident bytes the process holds before compiling anything.
///
/// The same measurement with a 30-byte program compiles under a 16 MiB limit.
/// 24 MiB is the intercept the two filebeat points extrapolate to.
const RUNTIME_RESERVE_BYTES: u64 = 24 * 1024 * 1024;

/// Smallest container memory limit that can compile `program_bytes` of VRL.
///
/// This covers COMPILATION only. A running pipeline holds consumer and
/// producer buffers on top of it and needs several times this much -- the
/// figure to size a deployment from is the steady-state one, not this.
#[must_use]
pub const fn compile_floor_bytes(program_bytes: u64) -> u64 {
    RUNTIME_RESERVE_BYTES + program_bytes * COMPILE_BYTES_PER_SOURCE_BYTE
}

/// Refuse to start when the container limit cannot compile this program.
///
/// Logs the limit, the program size and the floor either way, so an operator
/// sizing the instance sees all three whether or not the check trips.
pub fn check_compile_budget(program_bytes: u64, limit_bytes: u64) -> Result<()> {
    let floor_bytes = compile_floor_bytes(program_bytes);
    info!(
        program_bytes,
        limit_bytes, floor_bytes, "VRL compile budget"
    );

    if limit_bytes < floor_bytes {
        return Err(crate::Error::Config(format!(
            "memory limit too small to compile the VRL program: {program_bytes} bytes of VRL \
             needs at least {floor_bytes} bytes ({} MiB) of container memory to compile, and \
             this container's limit is {limit_bytes} bytes ({} MiB). Raise the limit, or point \
             transforms at a smaller program.",
            floor_bytes / (1024 * 1024),
            limit_bytes / (1024 * 1024),
        )));
    }
    Ok(())
}

/// Report what the compile actually cost, next to what was budgeted for it.
pub fn log_compiled(program_bytes: u64) {
    info!(
        program_bytes,
        resident_bytes = resident_bytes(),
        floor_bytes = compile_floor_bytes(program_bytes),
        "VRL program compiled"
    );
}

/// Page size the `statm` counts are in; the only targets are Linux x86-64 and
/// aarch64, both 4 KiB.
const PAGE_BYTES: u64 = 4096;

/// This process's resident set size in bytes, 0 when it cannot be read.
///
/// `/proc/self/statm` field 2 is resident pages.
fn resident_bytes() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    let Some(pages) = statm.split_whitespace().nth(1) else {
        return 0;
    };
    let Ok(pages) = pages.parse::<u64>() else {
        return 0;
    };
    pages * PAGE_BYTES
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::subscriber::with_default;

    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn floor_is_the_reserve_for_an_empty_program() {
        assert_eq!(compile_floor_bytes(0), RUNTIME_RESERVE_BYTES);
    }

    #[test]
    fn floor_grows_with_the_program() {
        let small = compile_floor_bytes(1_000);
        let large = compile_floor_bytes(10_000);
        assert!(large > small, "a bigger program must imply a bigger floor");
        assert_eq!(large - small, 9_000 * COMPILE_BYTES_PER_SOURCE_BYTE);
    }

    #[test]
    fn floor_for_the_bundled_filebeat_program_sits_above_the_measured_cliff() {
        // 212,218 bytes is OOM-killed under a 32 MiB limit and compiles under
        // 40 MiB on the shipped image, so the floor must fall in between: high
        // enough to refuse 32 MiB, low enough not to refuse a real deployment.
        let floor = compile_floor_bytes(212_218);
        assert!(floor > 32 * MIB, "must refuse the limit that OOM-kills");
        assert!(floor < 64 * MIB, "must not refuse a limit that works");
    }

    #[test]
    fn budget_check_passes_when_the_limit_clears_the_floor() {
        assert!(check_compile_budget(212_218, 512 * MIB).is_ok());
    }

    #[test]
    fn budget_check_refuses_a_limit_below_the_floor() {
        let err = check_compile_budget(212_218, 32 * MIB)
            .expect_err("32 MiB cannot compile the filebeat program");
        let msg = err.to_string();
        assert!(msg.contains("212218"), "must name the program bytes: {msg}");
        assert!(
            msg.contains(&compile_floor_bytes(212_218).to_string()),
            "must name the floor: {msg}"
        );
        assert!(
            msg.contains(&(32 * MIB).to_string()),
            "must name the limit: {msg}"
        );
    }

    #[test]
    fn budget_check_passes_at_exactly_the_floor() {
        let floor = compile_floor_bytes(212_218);
        assert!(check_compile_budget(212_218, floor).is_ok());
        assert!(check_compile_budget(212_218, floor - 1).is_err());
    }

    #[test]
    fn budget_log_names_the_limit_the_program_and_the_floor() {
        let events = capture(|| {
            check_compile_budget(212_218, 512 * MIB).unwrap();
        });
        let budget = events
            .iter()
            .find(|e| e.message == "VRL compile budget")
            .expect("the budget line must be logged before the compiler runs");
        assert_eq!(budget.field("program_bytes"), Some(212_218));
        assert_eq!(budget.field("limit_bytes"), Some(i128::from(512 * MIB)));
        assert_eq!(
            budget.field("floor_bytes"),
            Some(i128::from(compile_floor_bytes(212_218)))
        );
    }

    #[test]
    fn compiled_log_reports_the_resident_size() {
        let events = capture(|| log_compiled(212_218));
        let compiled = events
            .iter()
            .find(|e| e.message == "VRL program compiled")
            .expect("the compiled line must be logged after the compiler runs");
        assert_eq!(compiled.field("program_bytes"), Some(212_218));
        let resident = compiled
            .field("resident_bytes")
            .expect("resident_bytes must be on the line");
        assert!(resident > 0, "a running test process is resident: {resident}");
    }

    /// One captured event: its message and its integer fields.
    struct Event {
        message: String,
        fields: Vec<(String, i128)>,
    }

    impl Event {
        fn field(&self, name: &str) -> Option<i128> {
            self.fields
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| *value)
        }
    }

    /// Run `body` with a subscriber that records the events it emits.
    ///
    /// Hand-rolled on `tracing` alone: pulling tracing-subscriber in as a
    /// dev-dependency for two assertions is not worth the graph.
    fn capture(body: impl FnOnce()) -> Vec<Event> {
        let collected = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Collector {
            events: Arc::clone(&collected),
        };
        with_default(subscriber, body);
        Arc::try_unwrap(collected).map_or_else(
            |shared| shared.lock().unwrap().drain(..).collect(),
            |cell| cell.into_inner().unwrap(),
        )
    }

    struct Collector {
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl tracing::Subscriber for Collector {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::Id {
            tracing::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::Id, _follows: &tracing::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut recorder = Recorder {
                message: String::new(),
                fields: Vec::new(),
            };
            event.record(&mut recorder);
            self.events.lock().unwrap().push(Event {
                message: recorder.message,
                fields: recorder.fields,
            });
        }

        fn enter(&self, _span: &tracing::Id) {}

        fn exit(&self, _span: &tracing::Id) {}
    }

    struct Recorder {
        message: String,
        fields: Vec<(String, i128)>,
    }

    impl Visit for Recorder {
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields.push((field.name().to_string(), i128::from(value)));
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.fields.push((field.name().to_string(), i128::from(value)));
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{value:?}");
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "message" {
                self.message = value.to_string();
            }
        }
    }
}
