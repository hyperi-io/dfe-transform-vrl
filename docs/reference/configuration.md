# Configuration

How dfe-transform-vrl reads its config, what each setting needs to change, and the chart a release ships. Every key is in [config-schema.yaml](../config-schema.yaml).

## Operating notes

The transform's sections are `pipeline`, `source`, `sink`, `transforms`, `enrichment_tables` and `dlq`, and [config-schema.yaml](../config-schema.yaml) lists every key in them. `--config` is the one way to name the file: `DFE_TRANSFORM_VRL_CONFIG` reaches neither the wrapper nor scalo, and with no `--config` the wrapper reads `config.yaml` or `config.yml` from the working directory.

The big dials have flat env var overrides for K8s. These are every flat name `apply_flat_env` in `src/config/loader.rs` reads:

| Env Var | Config Field |
|---------|-------------|
| `DFE_TRANSFORM_VRL_PIPELINE_NAME` | `pipeline.name` |
| `DFE_TRANSFORM_VRL_KAFKA_SASL_USERNAME` | `source.sasl.username` + `sink.sasl.username` |
| `DFE_TRANSFORM_VRL_KAFKA_SASL_PASSWORD` | `source.sasl.password` + `sink.sasl.password` |
| `DFE_TRANSFORM_VRL_KAFKA_SASL_MECHANISM` | `source.sasl.mechanism` + `sink.sasl.mechanism` |
| `DFE_TRANSFORM_VRL_KAFKA_SECURITY_PROTOCOL` | `source.tls.enabled` + `sink.tls.enabled`, set true when the value contains `SSL`, never set false |
| `DFE_TRANSFORM_VRL_SOURCE_TRANSPORT` | `source.transport` (`bus`, `kafka`, `direct` or `grpc`) |
| `DFE_TRANSFORM_VRL_SOURCE_LISTEN` | `source.listen` |
| `DFE_TRANSFORM_VRL_SOURCE_BROKERS` | `source.brokers` |
| `DFE_TRANSFORM_VRL_SOURCE_TOPICS` | `source.topics` |
| `DFE_TRANSFORM_VRL_SOURCE_GROUP_ID` | `source.group_id` |
| `DFE_TRANSFORM_VRL_SOURCE_SASL_USERNAME` | `source.sasl.username` |
| `DFE_TRANSFORM_VRL_SOURCE_SASL_PASSWORD` | `source.sasl.password` |
| `DFE_TRANSFORM_VRL_SINK_TRANSPORT` | `sink.transport` (`bus`, `kafka`, `direct` or `grpc`) |
| `DFE_TRANSFORM_VRL_SINK_ENDPOINT` | `sink.endpoint` |
| `DFE_TRANSFORM_VRL_SINK_BROKERS` | `sink.brokers` |
| `DFE_TRANSFORM_VRL_SINK_TOPIC` | `sink.topic` |
| `DFE_TRANSFORM_VRL_SINK_KEY_FIELD` | `sink.key_field` |
| `DFE_TRANSFORM_VRL_SINK_COMPRESSION` | `sink.compression` |
| `DFE_TRANSFORM_VRL_SINK_SASL_USERNAME` | `sink.sasl.username` |
| `DFE_TRANSFORM_VRL_SINK_SASL_PASSWORD` | `sink.sasl.password` |
| `DFE_TRANSFORM_VRL_TRANSFORMS_DIR` | `transforms.dir` |

An empty value reads as unset. Every other key of the same sections takes the double-underscore form, `DFE_TRANSFORM_VRL_SINK__TOPIC` for `sink.topic`, and a flat name outranks it.

The chart mounts the Kafka Secret's `username` and `password` into both endpoint pairs, and its `sasl.mechanism` (`PLAIN`, `SCRAM-SHA-512`) into `KAFKA_SASL_MECHANISM`. Outside the chart the shared `KAFKA_SASL_*` names reach both endpoints, and the per-endpoint names override them.

Either half turns SASL on, an enabled block still missing one once the env layer has run stops startup, and the password is redacted on every output path (`x-scalo-secret` + `writeOnly` in the emitted schema).

For Kafka over TLS with a private CA: `tls.ca_paths` takes paths, and the chart mounts no certificate file. Put the CA's PEM text in the rendered config instead:

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

The same two keys go under `sink`. A CA is public, so a ConfigMap can hold it. Mutual TLS is not supported through the chart, which mounts no client key. KEDA scales on CPU and opens no Kafka connection, so it needs no CA.

scalo's settings share the config file. `metrics`, `logger`, `memory`, `scaling`, `worker_pool`, `batch_processing`, `self_regulation`, `version_check` and `otel_tracing` are resolved by scalo, not by the wrapper. scalo reads the file given with `--config` as its settings layer, once at startup, so those sections take effect from the mounted `config.yaml` beside the wrapper's:

```yaml
scaling:
  memory_gate_threshold: 0.8
batch_processing:
  max_chunk_size: 10000
```

A `config.yaml` picked up from the working directory with no `--config` is read by the wrapper alone, so a scalo section in it changes nothing. The wrapper warns at startup for each section `config::SCALO_CASCADE_SECTIONS` lists, `memory` included. Pass the file with `--config`.

The env layer outranks the file, and there the section nests on a **double** underscore:

```bash
METRICS_ADDR=0.0.0.0:9090          # or DFE_TRANSFORM_VRL_METRICS__ADDRESS
LOG_LEVEL=debug                    # or DFE_TRANSFORM_VRL_LOGGER__LEVEL
LOG_FORMAT=json                    # or DFE_TRANSFORM_VRL_LOGGER__FORMAT
DFE_TRANSFORM_VRL_SCALING__MEMORY_GATE_THRESHOLD=0.8
DFE_TRANSFORM_VRL_BATCH_PROCESSING__MAX_CHUNK_SIZE=10000
DFE_TRANSFORM_VRL_OTEL_TRACING__ENABLED=false    # OTLP span export, on by default
DFE_TRANSFORM_VRL_METRICS__OTEL__ENABLED=false   # OTLP metric push, on by default
```

A single underscore (`DFE_TRANSFORM_VRL_METRICS_ADDRESS`) produces a flat key that matches no section and is ignored, and that spelling warns as well. The memory guard is the exception: scalo reads `DFE_TRANSFORM_VRL_MEMORY_LIMIT_BYTES`, `DFE_TRANSFORM_VRL_MEMORY_PRESSURE_THRESHOLD` and `DFE_TRANSFORM_VRL_MEMORY_CGROUP_HEADROOM` on a single underscore.

## Reload

`ConfigReloader` (file polling + SIGHUP) re-reads and re-validates the file and swaps `SharedConfig<HotConfig>`. A bad edit logs `Config reload failed, keeping current config` and counts in `config_reloads_total{result="error"}`, and the service keeps running. The reloadable subset has three fields, and the pipeline reads none of them:

| Field | What it would control | State |
|-------|----------------------|-------|
| `pipeline.batch_size` | Events per transform chunk | not applied -- the chunk size is `batch_processing.max_chunk_size`, a scalo section |
| `pipeline.batch_timeout_ms` | Max wait before flushing partial batch | not applied -- no such timer |
| `sink.key_field` | Kafka partition key path | not applied -- scalo-rs#37 |

`config::INERT_SETTINGS` lists those three and `source.commit_interval_ms`, which the production Kafka profile leaves unread because librdkafka auto-commit is off and the engine commits after each sink ack. The wrapper warns at startup for each one a deployment has moved off its default.

The fields below are bound to connections, compiled programs, or server sockets set up at startup. Changing them needs a pod restart, and the chart rolls the pods: a config change alters the `checksum/config` annotation on the pod template.

| Field | Why |
|-------|-----|
| `pipeline.name` | The producer's `client.id` and the log fields. The consumer group is `source.group_id` |
| `source.*` | rdkafka consumer or Push listener: connection, subscription, auth, TLS, buffers |
| `sink.transport`, `sink.endpoint` | The sink built at startup |
| `sink.brokers` | rdkafka producer connection |
| `sink.topic` | Output topic (changing mid-stream risks data loss) |
| `sink.compression` | rdkafka `compression.type` |
| `sink.sasl.*` / `sink.tls.*` | Security protocol |
| `sink.max_buffer_bytes` | rdkafka `queue.buffering.max.kbytes` |
| `sink.librdkafka_options` | librdkafka `ClientConfig` |
| `transforms.*` | VRL programs compiled at startup |
| `enrichment_tables` | Tables loaded at startup, and the compiled program checks its lookups against them |
| `dlq.*` | The dead-letter queue spawned at startup |

## Helm chart

No chart is committed here. At release, hyperi-ci runs the binary's `generate-artefacts` for the deployment contract and assembles a thin chart from it on the scalo-service library chart, at the version `release.helm.library` names in `.hyperi-ci.yaml`. Keep that version at the scalo version in `Cargo.toml`: a library renders the contract version its scalo release writes and no other.

To see the chart a release would ship, build the binary and run `hyperi-ci chart assemble --binary target/debug/dfe-transform-vrl --image ghcr.io/hyperi-io/dfe-transform-vrl:<tag>@sha256:<digest> --version <version>`. It prints the chart directory it wrote.

The ScaledObject scales on CPU alone, because `contract()` sets `KafkaLagTrigger::disabled()`. Consumer-group lag rises when a downstream stage breaks, and more replicas cannot fix that. A deployment adds a trigger with the library's `keda.extraTriggers` value.

Rotating the Kafka credential does not roll the pods by default, because the env it lands in resolves at pod start. The library chart's `reload.enabled` and `reload.secrets` values add the Stakater Reloader annotations that roll them, on a cluster that runs Reloader.
