# dfe-transform-vrl -- Design Document

## Overview

`dfe-transform-vrl` is an embedded VRL transform engine for Kafka-to-Kafka
pipelines. Unlike `dfe-transform-vector` (which runs Vector as a subprocess),
this service embeds the VRL crate directly and owns the Kafka consumer/producer,
giving full control over memory, backpressure, and wire format.

## Problem Statement

### Memory Control

Vector.dev has no configurable memory cap. Its internal buffers grow unbounded
based on throughput. In K8s autoscaling, pod memory limits must be set to the
maximum Vector might ever use, resulting in 2-4x memory waste compared to DFE
services that control their own buffers.

## Architecture

```mermaid
flowchart TB
    subgraph DP["Data path"]
        direction LR
        KS["Kafka source (scalo)<br/>JSON"] -->|consume| VRL["VRL engine<br/>Value in/out"]
        VRL --> KP["Kafka sink (scalo)<br/>JSON"]
        KP -.->|offset commit after delivery, at-least-once| KS
    end
    subgraph OPS["Operational endpoints (same process)"]
        direction LR
        MS["Metrics :9090<br/>/metrics, /livez, /readyz"]
    end
```

## Data Flow

### Per-Event Processing

```text
Kafka partition message (raw bytes)
  │
  ├─ Parse: sonic_rs::from_slice::<Value>() -- a record that is not JSON is dropped
  │
  ├─ Run VRL program(s) on Value
  │   └─ VRL operates directly on Value — no format conversion
  │
  ├─ Serialise: sonic_rs::to_vec(&value)
  │
  └─ KafkaTransport::send() (scalo) → delivery future
```

### Offset Commit Strategy (At-Least-Once)

Release is batch-level. scalo's batch engine hands a block of records to the
transform, sends the result, and releases the block's source offsets once the
batch send returns Ok. Under that, scalo's Kafka transport holds each source
offset it handed out and commits a partition only up to its lowest unreleased
one, so the cumulative commit never passes a record not yet released.

If the process crashes between send and release, the whole block is re-consumed
and re-processed (at-least-once, not exactly-once). VRL transforms must be
idempotent.

### Payload Format

JSON is the only payload format. MessagePack, supported in DFE/XDR 2.0 and 2.1, is deprecated in DFE 2.2 and no longer accepted: the JSON path (SIMD parsing with sonic-rs, zstd on the wire) is fast enough that MessagePack gave no CPU saving.

A record that does not parse as JSON is removed from its block, counted in
`records_error_total{stage="deserialise"}`, and logged (sampled) with the parse
error. The block's source is then released `Dropped`, so the record is not
redelivered.

## VRL Integration

### Compilation

VRL programs are compiled once at startup:

```rust
let program = vrl::compiler::compile(source, &vrl::stdlib::all())
    .map_err(|diagnostics| /* format errors */)?;
```

The compiled `Program` is reused for every event -- zero per-event compilation cost.

### Execution

Each event is wrapped in a `TargetValueRef` and passed to the VRL runtime:

```rust
let mut target = TargetValueRef {
    value: &mut event_value,
    metadata: &mut metadata,
    secrets: &mut secrets,
};
runtime.resolve(&mut target, &program)?;
```

VRL's `Value` type is serde-compatible, so `sonic_rs` parses JSON bytes
directly into it.

### Transform File Format

Transform files contain raw VRL source code (not Vector YAML):

```vrl
# transforms/01_parse.vrl
.parsed = parse_json!(.message)
del(.message)

# transforms/02_enrich.vrl
.environment = .labels.env ?? "unknown"
.processed_at = now()
```

Files are loaded in sorted order (by filename) and concatenated into a single
VRL program. This matches Vector's remap transform behaviour where multiple
VRL statements execute sequentially.

vrl is built without its `enable_env_functions` and `enable_network_functions`
features, and `compile_vrl` drops `get_hostname` from the system functions it
keeps for `parse_etld`. A program calling `get_env_var`, `get_hostname`,
`http_request`, `dns_lookup` or `reverse_dns` fails to compile as a call to an
undefined function.

### Bundled Pipeline: filebeat-compat (INTERIM)

`pipelines/filebeat/` ships a pre-canned, opt-in port of the DFE 2.1 Vector
filebeat templates (Cisco Meraki logs, Cisco IOS, Cisco Umbrella) as one
pure-VRL file plus its `timezones.csv` enrichment table. It is convenience
data, not engine capability: opting in means pointing the standard
`transforms.dir` and `enrichment_tables` config at those files. The engine
carries no filebeat-specific code, and the integration tests
(`tests/integration/filebeat_pipeline.rs`) drive the bundle with the real
elastic/integrations pipeline test corpus, which doubles as a full-engine
workload.

INTERIM: elastic compatibility is being replaced by `dfe-transform-elastic`
(Rust-native, in beta). Wiring, routing behaviour, known limitations, and
regeneration tooling (`scripts/filebeat/`) are documented in
[pipelines/filebeat/README.md](../pipelines/filebeat/README.md).

## Memory Budget

The wrapper controls all memory allocation:

| Component | Configuration | Default |
|-----------|--------------|---------|
| Consumer prefetch | `source.max_buffer_bytes` | 64 MiB |
| Transform chunk | `batch_processing.max_chunk_size` (env only) | 10 000 events |
| Producer queue | `sink.max_buffer_bytes` | 64 MiB |
| Total pod memory | K8s resource limit | 512 MiB (128 MiB request) |

The sum of consumer + producer buffers must fit within the pod memory limit
minus overhead (binary, runtime, VRL compiled programs). The wrapper enforces
backpressure: if the producer queue is full, the consumer pauses fetching.

## Configuration

Same big-dial pattern as `dfe-transform-vector`, minus Vector subprocess config:

```yaml
pipeline:
  name: "my-pipeline"

source:
  brokers: ["kafka:9092"]
  topics: ["raw_events"]
  group_id: "dfe-transform-vrl-my-pipeline"
  max_buffer_bytes: 67108864        # 64 MiB consumer buffer

transforms:
  dir: "/etc/dfe-transform-vrl/transforms/"       # directory of .vrl files

sink:
  brokers: ["kafka:9092"]
  topic: "enriched_events"
  max_buffer_bytes: 67108864        # 64 MiB producer buffer
```

That is the wrapper's whole schema. `metrics`, `logger`, `scaling`,
`worker_pool`, `batch_processing`, `self_regulation`, `version_check` and
`otel_tracing` belong to scalo's cascade, which reads this file as its settings
layer when it is passed with `--config`, so they may sit beside these sections.
The env layer (`METRICS_ADDR`, `LOG_LEVEL`, `DFE_TRANSFORM_VRL_SCALING__*`, ...)
outranks the file.

## Comparison: dfe-transform-vrl vs dfe-transform-vector

| Aspect | dfe-transform-vrl | dfe-transform-vector |
|--------|-------------------|---------------------|
| Transform engine | VRL crate (in-process) | Vector subprocess |
| Kafka source/sink | rdkafka (wrapper-controlled) | Vector-managed |
| Memory control | Full (bounded buffers) | None (Vector unbounded) |
| Supported transforms | VRL only | All Vector transforms |
| Container image size | ~20 MiB (Rust binary) | ~170 MiB (Rust + Vector) |
| Process model | Single process | Wrapper + subprocess |
| Startup time | Fast (compile VRL) | Slower (spawn + healthcheck Vector) |
| Crash recovery | Restart binary | Restart subprocess + binary |

### When to Use Which

- **dfe-transform-vrl**: VRL-only transforms (the common case),
  memory-constrained pods, high-density deployments
- **dfe-transform-vector**: Pipelines needing Vector-native transforms (`lua`,
  `aggregate`, `dedupe`, `throttle`, `sample`), or complex multi-source/sink routing

## Reload

### Carried in the reloadable subset

`ConfigReloader` (file polling + SIGHUP) re-reads and re-validates the file and
swaps `SharedConfig<HotConfig>`, so a bad edit is caught without a restart.

| Field | What it would control | State |
|-------|----------------------|-------|
| `pipeline.batch_size` | Events per transform chunk | not applied -- see below |
| `pipeline.batch_timeout_ms` | Max wait before flushing partial batch | not applied -- no such timer |
| `sink.key_field` | Kafka partition key path | not applied -- scalo-rs#37 |

### Accepted but not applied

The pipeline holds the reloadable subset but reads none of it yet, so a reload
changes no pipeline behaviour today. `config::INERT_SETTINGS` is the list, and
the wrapper warns at startup for each one a deployment has set. The dial that
does size a chunk is `batch_processing.max_chunk_size`, set via the env layer.

### Requires pod restart

These fields are bound to connections, compiled programs, or server sockets
established at startup. Changing them requires a pod restart (which is the
standard K8s pattern -- ConfigMap changes trigger rolling restart via the
`checksum/config` annotation in the Deployment template).

| Field | Why |
|-------|-----|
| `pipeline.name` | Baked into Kafka group_id, metrics labels, tracing spans |
| `source.*` | rdkafka consumer: connection, subscription, auth, TLS, buffers |
| `sink.brokers` | rdkafka producer connection |
| `sink.topic` | Output topic (changing mid-stream risks data loss) |
| `sink.compression` | rdkafka `compression.type` |
| `sink.sasl.*` / `sink.tls.*` | Security protocol |
| `sink.max_buffer_bytes` | rdkafka `queue.buffering.max.kbytes` |
| `sink.librdkafka_options` | librdkafka `ClientConfig` |
| `transforms.*` | VRL programs compiled at startup |

## Operating notes

### Environment variable overrides

The big dials have flat env var overrides for K8s. This is the whole list --
anything not here is not read:

| Env Var | Config Field |
|---------|-------------|
| `DFE_TRANSFORM_VRL_PIPELINE_NAME` | `pipeline.name` |
| `DFE_TRANSFORM_VRL_KAFKA_SASL_USERNAME` | `source.sasl.username` + `sink.sasl.username` |
| `DFE_TRANSFORM_VRL_KAFKA_SASL_PASSWORD` | `source.sasl.password` + `sink.sasl.password` |
| `DFE_TRANSFORM_VRL_KAFKA_SASL_MECHANISM` | `source.sasl.mechanism` + `sink.sasl.mechanism` |
| `DFE_TRANSFORM_VRL_KAFKA_SECURITY_PROTOCOL` | `source.tls.enabled` + `sink.tls.enabled`, set true when the value contains `SSL`, never set false |
| `DFE_TRANSFORM_VRL_SOURCE_BROKERS` | `source.brokers` |
| `DFE_TRANSFORM_VRL_SOURCE_TOPICS` | `source.topics` |
| `DFE_TRANSFORM_VRL_SOURCE_GROUP_ID` | `source.group_id` |
| `DFE_TRANSFORM_VRL_SOURCE_SASL_USERNAME` | `source.sasl.username` |
| `DFE_TRANSFORM_VRL_SOURCE_SASL_PASSWORD` | `source.sasl.password` |
| `DFE_TRANSFORM_VRL_SINK_BROKERS` | `sink.brokers` |
| `DFE_TRANSFORM_VRL_SINK_TOPIC` | `sink.topic` |
| `DFE_TRANSFORM_VRL_SINK_KEY_FIELD` | `sink.key_field` |
| `DFE_TRANSFORM_VRL_SINK_COMPRESSION` | `sink.compression` |
| `DFE_TRANSFORM_VRL_SINK_SASL_USERNAME` | `sink.sasl.username` |
| `DFE_TRANSFORM_VRL_SINK_SASL_PASSWORD` | `sink.sasl.password` |
| `DFE_TRANSFORM_VRL_TRANSFORMS_DIR` | `transforms.dir` |

The chart mounts the Kafka Secret's `username` and `password` into both endpoint pairs, and its `sasl.mechanism` (`PLAIN`, `SCRAM-SHA-512`) into `KAFKA_SASL_MECHANISM`. Outside the chart the shared `KAFKA_SASL_*` names reach both endpoints, and the per-endpoint names override them.

Either half turns SASL on, an enabled block still missing one once the env
layer has run refuses to start, and the password is redacted on every output
path (`x-scalo-secret` + `writeOnly` in the emitted schema).

### Kafka over TLS with a private CA

`tls.ca_cert_file` is a path, and the chart mounts no certificate file. Put the
CA's PEM text in the rendered config instead:

```yaml
config:
  source:
    tls:
      enabled: true
    librdkafka_options:
      ssl.ca.pem: |
        -----BEGIN CERTIFICATE-----
        ...
        -----END CERTIFICATE-----
```

The same two keys go under `sink`. A CA is public, so a ConfigMap can hold it.
Mutual TLS is not supported through the chart, which mounts no client key. KEDA
scales on CPU and opens no Kafka connection, so it needs no CA.

### scalo's own settings share the config file

`metrics`, `logger`, `scaling`, `worker_pool`, `batch_processing`,
`self_regulation`, `version_check` and `otel_tracing` are resolved by scalo, not
by the wrapper.
scalo reads the file passed with `--config` as its settings layer, once at
startup, so those sections take effect from the mounted `config.yaml` beside the
wrapper's own:

```yaml
scaling:
  memory_gate_threshold: 0.8
batch_processing:
  max_chunk_size: 10000
```

A `config.yaml` picked up from the working directory with no `--config` is read
by the wrapper alone, so a scalo section in it changes nothing and the wrapper
warns at startup. Pass the file with `--config`.

The env layer outranks the file, and there the section nests on a **double**
underscore:

```bash
METRICS_ADDR=0.0.0.0:9090          # or DFE_TRANSFORM_VRL_METRICS__ADDRESS
LOG_LEVEL=debug                    # or DFE_TRANSFORM_VRL_LOGGER__LEVEL
LOG_FORMAT=json                    # or DFE_TRANSFORM_VRL_LOGGER__FORMAT
DFE_TRANSFORM_VRL_SCALING__MEMORY_GATE_THRESHOLD=0.8
DFE_TRANSFORM_VRL_BATCH_PROCESSING__MAX_CHUNK_SIZE=10000
DFE_TRANSFORM_VRL_OTEL_TRACING__ENABLED=false    # OTLP span export, on by default
DFE_TRANSFORM_VRL_METRICS__OTEL__ENABLED=false   # OTLP metric push, on by default
```

A single underscore (`DFE_TRANSFORM_VRL_METRICS_ADDRESS`) produces a flat key that
matches no section and is ignored, and that spelling also warns.

### Helm chart

No chart is committed here. At release, hyperi-ci runs the binary's `generate-artefacts` for the deployment contract and assembles a thin chart from it on the scalo-service library chart, at the version `release.helm.library` names in `.hyperi-ci.yaml`. Keep that version at the scalo version in `Cargo.toml`: a library renders only the contract version its scalo release writes.

To see the chart a release would ship, build the binary and run `hyperi-ci chart assemble --binary target/debug/dfe-transform-vrl --image ghcr.io/hyperi-io/dfe-transform-vrl:<tag>@sha256:<digest> --version <version>`. It prints the chart directory it wrote.

The ScaledObject scales on CPU alone, because `contract()` sets `KafkaLagTrigger::disabled()`. Consumer-group lag rises when a downstream stage breaks, and more replicas cannot fix that. A deployment adds a trigger with the library's `keda.extraTriggers` value.
