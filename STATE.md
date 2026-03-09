## CI UNAVAILABLE — Commit with [skip ci]

The CI system is being completely rewritten. Until migration instructions are provided:

- **All commits MUST include `[skip ci]` in the commit message**
- Do not trigger CI runs or rely on CI for validation
- Run `./ci/local-build.sh` for local validation if it exists

---

# Project Context

**Project:** dfe-transform-vrl
**Purpose:** Embedded VRL (Vector Remap Language) transform engine with wrapper-controlled Kafka source/sink. Replaces Vector subprocess for VRL-only pipelines, giving full memory control and native msgpack support.

> **Note:** The `hyperi-ai/` submodule provides standards and configuration - not code
> to import. Your project never imports or links to it.

---

## DO NOT ADD TO THIS FILE

**The following belong elsewhere:**

| Data | Correct Location |
|------|------------------|
| Version numbers | `VERSION` file, `git describe --tags` |
| Tasks/Progress | `TODO.md` |
| Session history | Git log (`git log --oneline -10`) |
| Changelog | `CHANGELOG.md` (semantic-release) |
| Dates | Git commit timestamps |

**This file is for static project context only.**

---

## Project Overview

### Architecture

A Rust binary (`dfe-transform-vrl`) runs as PID 1 in a K8s pod. It:
1. Loads "big dial" config (pipeline name, Kafka source/sink settings, VRL transform file paths)
2. Compiles VRL programs from user-supplied transform files at startup
3. Runs an rdkafka consumer, consuming batches from source Kafka topic(s)
4. Auto-senses payload format (msgpack or JSON) using `FormatDetector`
5. Deserialises each event to VRL `Value`, runs compiled VRL transforms in-process
6. Serialises output back to the detected format (msgpack or JSON)
7. Produces output events via rdkafka producer to sink Kafka topic
8. Commits consumer offsets only after producer delivery confirmation (at-least-once)
9. Provides health (`/health/live`, `/health/ready`) and metrics (`/metrics`) endpoints
10. Supports bounded memory via configurable consumer/producer buffer sizes

### Why This Exists (vs dfe-transform-vector)

Vector.dev has no memory cap — you cannot limit its internal buffer memory usage.
In K8s autoscaling this means 2-4x memory waste per pod compared to DFE services
that control their own buffers. Additionally, Vector has no msgpack codec support,
and the DFE platform is moving to msgpack as primary wire format.

This project solves both problems by:
- **Owning the Kafka source/sink** — rdkafka consumer/producer in the wrapper, with
  configurable buffer sizes that align to pod memory limits
- **Embedding VRL directly** — the `vrl` crate runs transforms in-process on `Value`,
  eliminating the need for JSON format conversion through a pipe
- **Native msgpack support** — `rmp-serde` deserialises msgpack directly to `Value`,
  VRL transforms operate on `Value`, then `rmp-serde` serialises back to msgpack.
  Zero format conversion overhead.

### Key Components

1. **Config Engine** (`src/config/`) — Big-dial config loading (7-layer cascade via hyperi-rustlib), VRL program compilation, validation
2. **VRL Engine** (`src/engine/`) — VRL program compilation, batch execution against events, custom DFE functions
3. **Kafka Layer** (`src/kafka/`) — rdkafka consumer with offset tracking, producer with delivery confirmation, watermark-based offset commit
4. **Pipeline** (`src/pipeline.rs`) — Event loop: consume batch → deserialise → transform → serialise → produce → commit
5. **Observability** (`src/health.rs`, `src/metrics.rs`) — HTTP server with health probes and Prometheus metrics
6. **Deployment** (`src/deployment.rs`) — DeploymentContract for Dockerfile, Helm chart, compose fragment generation

### Tech Stack

- **Language:** Rust (edition 2024)
- **Async runtime:** tokio
- **Transform engine:** VRL crate (embedded, no Vector subprocess)
- **Shared lib:** hyperi-rustlib (from JFrog `hyperi` registry)
  - `cli` — DfeApp trait, CommonArgs, CLI framework
  - `deployment` — DeploymentContract, Dockerfile/Helm/Compose generation
  - `logger` — Structured logging with masking (tracing-based)
  - `http-server` — Axum HTTP server with built-in `/health/live`, `/health/ready`
  - `metrics` — MetricsManager, Prometheus counters/gauges/histograms + server
  - `transport-kafka` — KafkaTransport (rdkafka), KafkaConfig, offset commit
  - `transport` — FormatDetector, PayloadFormat, serialize/parse payload helpers
  - `scaling` — Scaling pressure calculation for KEDA
- **Deployment:** Helm + Argo CD (via dfe-engine HelmValuesCompiler)

---

## Key Decisions

### VRL Crate Embedded (not Vector Subprocess)

**Decision:** Embed the `vrl` crate directly for VRL-only transform pipelines.
**Rationale:** VRL is published as a standalone crate with full compiler + runtime + stdlib.
Embedding it gives: zero format conversion (msgpack→Value→VRL→Value→msgpack), full memory
control (no Vector internal buffers), simpler process model (no subprocess management),
and smaller container image (no Vector binary).
**Trade-off:** Cannot use Vector-native transforms (lua, aggregate, dedupe, throttle).
Pipelines needing those use `dfe-transform-vector` instead.

### Wrapper-Controlled Kafka Source/Sink

**Decision:** rdkafka consumer/producer owned by the wrapper, not Vector.
**Rationale:** Vector has no memory cap. In K8s autoscaling, pod memory must be set to
the max Vector might ever use, causing 2-4x waste. With wrapper-controlled buffers,
memory is bounded and predictable, matching dfe-loader/dfe-receiver patterns.

### Watermark-Based Offset Commit

**Decision:** Commit consumer offsets only after producer delivery confirmation.
**Rationale:** At-least-once guarantee — never release a consumer group offset until
the transformed event is confirmed written to the sink topic. Track per-partition
watermarks: when offset N is confirmed produced, all offsets ≤ N on that partition
are safe to commit.

### Native msgpack Support

**Decision:** Auto-sense msgpack vs JSON using FormatDetector, process natively.
**Rationale:** DFE platform uses msgpack as primary wire format. msgpack→Value is
faster than JSON→Value and uses less memory. The `vrl` crate's `Value` type is
serde-compatible, so `rmp-serde` can deserialise msgpack directly to VRL Value.

### Same Management Interface

**Decision:** Identical dfe-engine ServicePlugin contract as dfe-transform-vector.
**Rationale:** dfe-engine must manage both transform services identically — same config
registry, same Helm compilation, same KEDA wiring. The only difference is the service
name and the absence of Vector subprocess config.

### Health Endpoint Paths

**Decision:** `/health/live` and `/health/ready` (matching dfe-engine contract).

### Crates-Backed Modular Architecture

**Decision:** Code is organised into focused modules that map to well-defined crate
responsibilities. Each module owns a single concern and depends on established crates
rather than hand-rolling functionality.
**Rationale:** Maintainability, testability, and leverage. Each module can be reasoned
about independently, and crate-backed implementations get upstream bug fixes for free.
**Module map:**
- `config/` — Config schema and loading (`figment`, `serde_yaml_ng`, `dotenvy`)
- `engine/` — VRL compilation and execution (`vrl` crate)
- `kafka/` — Kafka transport wiring (`hyperi-rustlib` transport-kafka)
- `pipeline.rs` — Event loop orchestrating consume → transform → produce
- `health.rs` — Health endpoints (`hyperi-rustlib` http-server)
- `metrics.rs` — Prometheus metrics (`hyperi-rustlib` metrics, `metrics` crate)
- `deployment.rs` — Container/chart generation (`hyperi-rustlib` deployment)
- `error.rs` — Unified error types (`thiserror`)

### Flat Single-Crate Structure (No Workspace)

**Decision:** Keep dfe-transform-vrl as a single crate, not a Cargo workspace.
**Rationale:** Reviewed against dfe-transform-wasm's `crates/` workspace pattern.
The wasm project needs separate crates because it ships an SDK to external users
(host, sdk, wit, test-harness — each serves a different consumer). dfe-transform-vrl
has one consumer (the binary itself), no external API, and is ~2.1k lines. Splitting
would add Cargo.toml overhead, workspace dependency management, and feature flag
complexity with no benefit. Revisit if an external-facing VRL function SDK is added.

### hyperi-rustlib First (No Bespoke Duplicates)

**Decision:** Use hyperi-rustlib for everything it provides. No bespoke code that
duplicates rustlib functionality.
**Rationale:** Consistency across DFE services, reduced maintenance, shared bug fixes.
**Applies to:** HTTP server, health endpoints, metrics, Kafka transport, format detection,
config cascade, CLI framework, logging, deployment contracts, scaling pressure.
**Source:** JFrog `hyperi` Cargo registry (`hypersec.jfrog.io`), not path dependencies.

---

## External Dependencies

- **VRL crate** — Transform engine (compiler + runtime + stdlib)
- **hyperi-rustlib** — Shared Rust library from JFrog `hyperi` registry (CLI, deployment,
  logger, http-server, metrics, transport-kafka, scaling). Kafka/rdkafka access is via
  rustlib's transport-kafka feature — not a direct rdkafka dependency.
- **dfe-engine** — Python orchestrator (ServicePlugin registration, config registry)
- **Apache Kafka** — Source and sink for all pipelines
- **KEDA** — Autoscaling based on Kafka consumer lag
- **Prometheus** — Metrics scraping via PodMonitor

---

## Sibling Projects

- `/projects/dfe-transform-vector` — Vector subprocess wrapper (for non-VRL transforms)
- `/projects/dfe-loader` — Kafka→ClickHouse loader (Rust, same big-dial pattern)
- `/projects/dfe-receiver` — Inbound data receiver (Rust)
- `/projects/dfe-engine` — Python orchestrator (manages all DFE services)

---

## Resources

**Documentation:**

- [docs/DESIGN.md](docs/DESIGN.md) — Full architecture and design
- [TODO.md](TODO.md) — Work breakdown structure

**Sibling Reference:**

- [dfe-transform-vector](https://github.com/hyperi-io/dfe-transform-vector) — Vector subprocess wrapper (patterns to follow)

**External:**

- [VRL crate](https://crates.io/crates/vrl) — Embedded transform engine
- [VRL function reference](https://vector.dev/docs/reference/vrl/functions/) — All stdlib functions
- [rdkafka docs](https://docs.rs/rdkafka/latest/rdkafka/) — Kafka client
- [rmp-serde docs](https://docs.rs/rmp-serde/latest/rmp_serde/) — MessagePack serde

---

## Notes for AI Assistants

This file contains **static project context only**.

**DO NOT add:** version numbers, progress, dates, session history.
**DO add:** architecture decisions, key components, how things work.

When in doubt, ask: "Will this be true next week?" If no, it doesn't belong here.
