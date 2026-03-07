# dfe-transform-vrl — Work Breakdown

## Phase 1: MVP — Config + VRL Engine + Pipeline

### 1.1 Project Scaffold
- [x] 1.1.1 Create repo, Cargo.toml, config files, submodules
- [x] 1.1.2 Write CLAUDE.md, TODO.md, docs/DESIGN.md
- [ ] 1.1.3 Implement error types, lib.rs module structure
- [ ] 1.1.4 Get project compiling with `cargo check`

### 1.2 Config Engine
- [ ] 1.2.1 Config schema (loader.rs) — pipeline, source, sink, transforms, health, metrics, scaling
- [ ] 1.2.2 Config validation (validate.rs)
- [ ] 1.2.3 Env var cascade (figment + flat overrides, same pattern as transform-vector)
- [ ] 1.2.4 Config example YAML
- [ ] 1.2.5 Unit tests for config loading and validation

### 1.3 VRL Engine
- [ ] 1.3.1 VRL program loading — read transform files, concatenate VRL source
- [ ] 1.3.2 VRL compilation — parse + compile VRL programs at startup
- [ ] 1.3.3 VRL execution — run compiled program against a single VRL Value
- [ ] 1.3.4 Batch execution — process a batch of events through the VRL pipeline
- [ ] 1.3.5 Error handling — VRL runtime errors → DLQ or skip with metrics
- [ ] 1.3.6 Unit tests for VRL compilation and execution

### 1.4 Kafka Layer
- [ ] 1.4.1 Consumer — rdkafka StreamConsumer, configurable buffer/fetch sizes
- [ ] 1.4.2 Producer — rdkafka FutureProducer, delivery confirmation tracking
- [ ] 1.4.3 Offset tracking — per-partition watermark, commit on delivery confirmation
- [ ] 1.4.4 Format detection — auto-sense msgpack vs JSON on first message per partition
- [ ] 1.4.5 Serialisation — msgpack↔Value and JSON↔Value conversion
- [ ] 1.4.6 SASL/TLS configuration — same big-dial pattern as transform-vector
- [ ] 1.4.7 librdkafka profile support — kafka_defaults module (central config, rustlib fallback)

### 1.5 Pipeline
- [ ] 1.5.1 Event loop — consume batch → deserialise → transform → serialise → produce
- [ ] 1.5.2 Backpressure — bounded channels between consumer and transform, transform and producer
- [ ] 1.5.3 Graceful shutdown — drain in-flight events, commit final offsets, SIGTERM handling
- [ ] 1.5.4 Memory budget — configurable limits for consumer prefetch, transform batch, producer queue

### 1.6 Health & Metrics
- [ ] 1.6.1 Health server — /health/live, /health/ready HTTP endpoints
- [ ] 1.6.2 Metrics server — Prometheus metrics (events processed, errors, latency, consumer lag)
- [ ] 1.6.3 Scaling pressure metric — KEDA-compatible weighted signal

### 1.7 CLI & Main
- [ ] 1.7.1 DfeApp implementation — standard CLI pattern (run, version, config-check)
- [ ] 1.7.2 Emit commands — emit-dockerfile, emit-chart, emit-compose, emit-contract
- [ ] 1.7.3 Main orchestrator — startup, lifecycle, signal handling

## Phase 2: Deployment

### 2.1 Docker
- [ ] 2.1.1 DeploymentContract — container image definition (no Vector binary needed)
- [ ] 2.1.2 Dockerfile generation via emit-dockerfile

### 2.2 Helm Chart
- [ ] 2.2.1 Chart generation via emit-chart (Deployment, not StatefulSet)
- [ ] 2.2.2 KEDA ScaledObject for Kafka consumer lag autoscaling
- [ ] 2.2.3 ConfigMap and Secret templates

### 2.3 CI/CD
- [ ] 2.3.1 GitHub Actions workflows (build, test, release)
- [ ] 2.3.2 Container image build and push
- [ ] 2.3.3 Helm chart packaging

## Phase 3: Testing & Hardening

### 3.1 Unit Tests
- [ ] 3.1.1 Config loading and validation
- [ ] 3.1.2 VRL compilation and execution
- [ ] 3.1.3 Format detection and serialisation
- [ ] 3.1.4 Offset tracking logic

### 3.2 Integration Tests
- [ ] 3.2.1 VRL transforms against fixture files
- [ ] 3.2.2 Config cascade (YAML + env vars)

### 3.3 E2E Tests
- [ ] 3.3.1 Kafka testcontainers — produce msgpack → transform → consume transformed
- [ ] 3.3.2 Kafka testcontainers — produce JSON → transform → consume transformed
- [ ] 3.3.3 At-least-once guarantee — crash recovery, offset commit verification

## Phase 4: dfe-engine Integration

### 4.1 ServicePlugin
- [ ] 4.1.1 Python ServicePlugin (ServiceDescriptor, Pydantic config model)
- [ ] 4.1.2 HelmValuesCompiler integration
- [ ] 4.1.3 dfe-core ApplicationSet and common values
