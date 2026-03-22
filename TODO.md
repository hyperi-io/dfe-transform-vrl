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
- [x] 3.3.1 Kafka produce JSON → VRL transform → consume transformed (integration_kafka.rs)
- [x] 3.3.2 Kafka produce msgpack → VRL transform → consume transformed (integration_kafka.rs)
- [x] 3.3.3 VRL abort drops events — only keep=true events forwarded to sink
- [ ] 3.3.4 At-least-once guarantee — crash recovery, offset commit verification

## Phase 4: dfe-engine Integration

### 4.1 ServicePlugin
- [x] 4.1.1 Python ServicePlugin (ServiceDescriptor, Pydantic config model)
- [x] 4.1.2 HelmValuesCompiler integration
- [ ] 4.1.3 dfe-core ApplicationSet and common values

## Phase 5: VRL Enrichment Tables

Deliberate subset of Vector.dev enrichment tables — just VRL + enrich, no
Vector runtime. Fail-fast on startup if enrichment files are missing or malformed.

### 5.1 Enrichment Table Loading
- [x] 5.1.1 Config schema — `enrichment_tables` section: name, path, key_columns
- [x] 5.1.2 CSV file loader — read CSV to `HashMap<Key, Row>` at startup
- [x] 5.1.3 JSON file loader — read JSON array to `HashMap<Key, Row>` at startup
- [x] 5.1.4 Fail-fast validation — missing file, malformed data, duplicate keys → abort startup
- [x] 5.1.5 Unit tests for CSV/JSON loading, missing file, malformed data

### 5.2 VRL TableRegistry Integration
- [x] 5.2.1 Custom VRL functions (get_enrichment_table_record, find_enrichment_table_records)
- [x] 5.2.2 Pass populated registry to VRL compiler via Arc<EnrichmentRegistry>
- [x] 5.2.3 VRL programs can use `get_enrichment_table_record("name", {"key": .field})`
- [x] 5.2.4 Table name not found at runtime → VRL error (caught at startup via test compile)
- [x] 5.2.5 Unit tests for registry lookup, missing table, missing key

### 5.3 Integration Tests
- [x] 5.3.1 End-to-end: CSV enrichment table + VRL transform
- [x] 5.3.2 End-to-end: JSON enrichment table + VRL transform
- [x] 5.3.3 Startup failure: missing enrichment file
- [x] 5.3.4 Startup failure: VRL references non-existent table name

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

### 6.5 Rustlib 1.16.5 — MemoryGuard + DfeSource
- [x] 6.5.1 Bump rustlib to >=1.16.5, add `memory` feature
- [x] 6.5.2 Add MemoryGuard (Pattern B — pause consumer under memory pressure)
- [x] 6.5.3 Wire under_pressure() into readiness probe (ready=false during pressure)
- [x] 6.5.4 Add memory_used_bytes / memory_limit_bytes Prometheus gauges
- [x] 6.5.5 Add DfeSource topic naming helpers (derive_dfe_source, derive_consumer_group)

### 6.6 Rustlib 1.16.7 — DfeMetrics Wiring + Capability Audit
- [x] 6.6.1 Bump rustlib to >=1.16.7
- [x] 6.6.2 Wire DfeMetrics::pipeline_ready() on ready/unready/shutdown transitions
- [x] 6.6.3 Wire DfeMetrics::scaling_pressure() + scaling_memory_pressure() each batch
- [x] 6.6.4 Wire MetricsManager::set_readiness_check() with ready_flag + memory_guard
- [x] 6.6.5 Set scaling_pressure gauge from memory_guard.pressure_ratio()
- [x] 6.6.6 Full capability audit — no bespoke code duplicating rustlib found

### 6.7 CI + Release
- [x] 6.7.1 Remove [skip ci] blanket — CI live via hyperi-ci
- [x] 6.7.2 PR #1 merged main → release — first release cut

### 6.8 Metrics Standard Migration (v1.0.1)
- [x] 6.8.1 Bump rustlib to >=1.18.0, add `metrics-dfe` feature
- [x] 6.8.2 Fix MetricsManager namespace: `transform_vrl` → `dfe_transform_vrl`
- [x] 6.8.3 Wire Layer 2 groups: AppMetrics, ConsumerMetrics, SinkMetrics, BackpressureMetrics, EnrichmentMetrics
- [x] 6.8.4 Split batch timing: deser/VRL/ser/end-to-end histograms
- [x] 6.8.5 Per-stage error counter (stage label: deserialise/transform/produce)
- [x] 6.8.6 Format detection counter (format label: json/msgpack)
- [x] 6.8.7 VRL-specific metrics: programs_loaded, abort_total, enrichment_table_rows
- [x] 6.8.8 Remove legacy unprefixed metrics
- [x] 6.8.9 Patch 3 security vulnerabilities (aws-lc-sys, rustls-webpki)

## Open Items

- [ ] Update hyperi-ai submodule (new standards/rules landed)
- [ ] Documentation review using /doco skill — verify docs match code post-metrics migration
- [ ] Re-build and re-test with updated hyperi-ci (prod/test change separation)
- [ ] At-least-once guarantee E2E test (3.3.4) — crash recovery, offset commit verification
- [ ] dfe-core ApplicationSet integration (4.1.3)
- [ ] FlatEnvOverrides derive macro — spec written at `/projects/dfe-receiver/docs/superpowers/specs/2026-03-19-flat-env-overrides-derive.md`
