# dfe-transform-vrl — Work Breakdown

## Phase 1: MVP — Config + VRL Engine + Pipeline

### 1.1 Project Scaffold
- [x] 1.1.1 Create repo, Cargo.toml, config files, submodules
- [x] 1.1.2 Write CLAUDE.md, TODO.md, docs/DESIGN.md
- [x] 1.1.3 Error types, lib.rs module structure
- [x] 1.1.4 Project compiles with `cargo check`

### 1.2 Config Engine
- [x] 1.2.1 Config schema (loader.rs) — pipeline, source, sink, transforms, health, metrics, scaling
- [x] 1.2.2 Config validation (validate.rs) — 9 unit tests
- [x] 1.2.3 Env var cascade (figment + flat overrides, same pattern as transform-vector)
- [x] 1.2.4 Config example YAML

### 1.3 VRL Engine
- [x] 1.3.1 VRL program loading — read .vrl files from dir or file list, 6 unit tests
- [x] 1.3.2 VRL compilation — parse + compile VRL programs at startup
- [x] 1.3.3 VRL execution — run compiled program against a single VRL Value
- [x] 1.3.4 Batch execution — process a batch of events through the VRL pipeline
- [x] 1.3.5 Error handling — VRL runtime errors → DLQ or skip with metrics
- [x] 1.3.6 Unit tests for VRL compilation and execution

### 1.4 Kafka Layer
- [x] 1.4.1 Consumer — via rustlib KafkaTransport, configurable buffer/fetch sizes
- [x] 1.4.2 Producer — via rustlib KafkaTransport, delivery confirmation tracking
- [x] 1.4.3 Offset tracking — commit on delivery confirmation via rustlib commit()
- [x] 1.4.4 Format detection — auto-sense msgpack vs JSON via rustlib PayloadFormat
- [x] 1.4.5 Serialisation — msgpack↔Value and JSON↔Value conversion
- [x] 1.4.6 SASL/TLS configuration — same big-dial pattern as transform-vector
- [x] 1.4.7 librdkafka profile support — KafkaConfig with rustlib KafkaProfile

### 1.5 Pipeline
- [x] 1.5.1 Event loop — consume batch → deserialise → transform → serialise → produce
- [x] 1.5.2 Backpressure — producer backpressure with yield + retry
- [x] 1.5.3 Graceful shutdown — drain in-flight events, commit final offsets, SIGTERM handling
- [x] 1.5.4 Memory budget — configurable consumer/producer buffer sizes via big-dial config

### 1.6 Health & Metrics
- [x] 1.6.1 Health server — /health/live, /health/ready via rustlib HttpServer
- [x] 1.6.2 Metrics server — Prometheus metrics via rustlib MetricsManager
- [x] 1.6.3 Scaling pressure metric — KEDA-compatible weighted signal

### 1.7 CLI & Main
- [x] 1.7.1 DfeApp implementation — standard CLI pattern (run, version, config-check)
- [x] 1.7.2 Emit commands — emit-dockerfile, emit-chart, emit-compose, emit-contract
- [x] 1.7.3 Main orchestrator — startup, lifecycle, signal handling

## Phase 2: Deployment

### 2.1 Docker
- [x] 2.1.1 DeploymentContract — container image definition (no Vector binary needed)
- [x] 2.1.2 Dockerfile generation via emit-dockerfile

### 2.2 Helm Chart
- [x] 2.2.1 Chart generation via emit-chart (Deployment, not StatefulSet)
- [x] 2.2.2 KEDA ScaledObject for Kafka consumer lag autoscaling
- [x] 2.2.3 ConfigMap and Secret templates

### 2.3 CI/CD
- [x] 2.3.1 Migrate to hyperi-ci (replaces legacy ci submodule)
- [ ] 2.3.2 GitHub Actions workflows (build, test, release) — blocked on hyperi-ci rewrite
- [ ] 2.3.3 Container image build and push
- [ ] 2.3.4 Helm chart packaging

## Phase 3: Testing & Hardening

### 3.1 Unit Tests
- [x] 3.1.1 Config loading and validation
- [x] 3.1.2 VRL file loading
- [x] 3.1.3 VRL compilation and execution
- [x] 3.1.4 Format detection and serialisation
- [x] 3.1.5 Offset tracking logic (delegated to rustlib KafkaTransport — no custom code to test)

### 3.2 Integration Tests
- [x] 3.2.1 VRL transforms against fixture files
- [x] 3.2.2 Config cascade (YAML + env vars)
- [x] 3.2.3 VRL edge cases — type coercion, abort, nulls, unicode, nested, arrays
- [x] 3.2.4 VRL real-world patterns — syslog, JSON manipulation, conditional routing
- [x] 3.2.5 Known-should-fail transforms — runtime errors, invalid field access

### 3.3 E2E Tests
- [ ] 3.3.1 Kafka testcontainers — produce msgpack → transform → consume transformed
- [ ] 3.3.2 Kafka testcontainers — produce JSON → transform → consume transformed
- [ ] 3.3.3 At-least-once guarantee — crash recovery, offset commit verification

## Phase 4: dfe-engine Integration

### 4.1 ServicePlugin
- [x] 4.1.1 Python ServicePlugin (ServiceDescriptor, Pydantic config model)
- [x] 4.1.2 HelmValuesCompiler integration
- [ ] 4.1.3 dfe-core ApplicationSet and common values

## Phase 5: VRL Enrichment Tables

Deliberate subset of Vector.dev enrichment tables — just VRL + enrich, no
Vector runtime. Fail-fast on startup if enrichment files are missing or malformed.

### 5.1 Enrichment Table Loading
- [ ] 5.1.1 Config schema — `enrichment_tables` section: name, path, key_columns
- [ ] 5.1.2 CSV file loader — read CSV to `HashMap<Key, Row>` at startup
- [ ] 5.1.3 JSON file loader — read JSON array to `HashMap<Key, Row>` at startup
- [ ] 5.1.4 Fail-fast validation — missing file, malformed data, duplicate keys → abort startup
- [ ] 5.1.5 Unit tests for CSV/JSON loading, missing file, malformed data

### 5.2 VRL TableRegistry Integration
- [ ] 5.2.1 Implement `vrl::enrichment::TableRegistry` trait backed by `HashMap`
- [ ] 5.2.2 Pass populated registry to VRL compiler and runtime context
- [ ] 5.2.3 VRL programs can use `get_enrichment_table_record("name", {"key": .field})`
- [ ] 5.2.4 Table name not found at compile time → compilation error (caught at startup)
- [ ] 5.2.5 Unit tests for registry lookup, missing table, missing key

### 5.3 Integration Tests
- [ ] 5.3.1 End-to-end: CSV enrichment table + VRL transform using get_enrichment_table_record
- [ ] 5.3.2 End-to-end: JSON enrichment table + VRL transform
- [ ] 5.3.3 Startup failure: missing enrichment file
- [ ] 5.3.4 Startup failure: VRL references non-existent table name

### Design Decisions

**What we implement:**
- CSV and JSON flat-file enrichment tables loaded at startup
- `HashMap<Key, Row>` backing — O(1) lookup, no disk I/O at runtime
- Tables immutable for process lifetime (K8s restarts on ConfigMap change)
- Standard VRL `get_enrichment_table_record()` / `find_enrichment_table_records()` syntax

**What we deliberately skip:**
- No hot-reload of enrichment files (restart the pod — K8s way)
- No `file_regex` / glob patterns (explicit file paths only)
- No GeoIP `.mmdb` support (just CSV/JSON flat tables)
- No `type: grok_pattern` tables (use VRL `parse_groks` stdlib instead)

**Fail-on-start behaviour:**
- File not found → startup error, pod CrashLoopBackOff
- File not parseable → startup error
- VRL program references a table name that doesn't exist → compilation error (caught at startup)

**Config example:**
```yaml
enrichment_tables:
  - name: "geo_lookup"
    path: "/etc/dfe/enrichment/geo.csv"
    key_columns: ["ip_range"]
  - name: "service_map"
    path: "/etc/dfe/enrichment/services.json"
    key_columns: ["service_id"]
```

## Phase 6: Hardening & Observability (Completed)

### 6.1 Code Review Fixes
- [x] 6.1.1 SIGTERM handling alongside SIGINT
- [x] 6.1.2 Batch timeout in pipeline recv (batch_timeout_ms)
- [x] 6.1.3 Separate events_filtered (abort) from events_failed (error) metrics
- [x] 6.1.4 Nested dot-path support in extract_key
- [x] 6.1.5 Clippy clean, profiling profile, deny.toml fix

### 6.2 Hot-Reload
- [x] 6.2.1 SharedConfig<HotConfig> for batch_size, batch_timeout_ms, retry, scaling
- [x] 6.2.2 Pipeline reads hot fields each batch cycle
- [x] 6.2.3 Config struct documented with hot-reload vs restart-required classification

### 6.3 Rustlib 1.16.3 Remediation
- [x] 6.3.1 Migrate apply_env_overrides() to ApplyFlatEnv trait
- [x] 6.3.2 Add DfeMetrics dual-emit alongside existing metrics
- [x] 6.3.3 Wire security events into auth/TLS/config reload sites
- [x] 6.3.4 Fix log spam sites

### 6.4 Infrastructure
- [x] 6.4.1 Migrate to hyperi-ci (replace legacy ci submodule)
- [x] 6.4.2 Update hyperi-ai submodule
- [x] 6.4.3 Add Renovate config
- [x] 6.4.4 Uniform Transport trait usage for Kafka layer

## Open Items

- [ ] CI workflows — blocked on hyperi-ci rewrite completing
- [ ] E2E Kafka testcontainers tests (3.3.x)
- [ ] dfe-core ApplicationSet integration (4.1.3)
- [ ] VRL enrichment tables (Phase 5)
- [ ] FlatEnvOverrides derive macro — spec written at `/projects/dfe-receiver/docs/superpowers/specs/2026-03-19-flat-env-overrides-derive.md`
