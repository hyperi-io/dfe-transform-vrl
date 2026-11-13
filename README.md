# dfe-transform-vrl

Runs your VRL transforms over a DFE deployment's event stream, in-process.

[![Build Status](https://github.com/hyperi-io/dfe-transform-vrl/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-transform-vrl/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](LICENSE)

## What it does

- Sits between the land topic and dfe-loader, and runs the VRL (Vector Remap Language) files in a directory, in filename order, against each event. The VRL engine is compiled in, so a transform is a file you change, not a release.
- Kafka in and out, or a direct gRPC push on a deployment with no broker. The transform you write is the same on both.
- At-least-once by default: the source is acknowledged only once the sink has the transformed records. `source.acknowledgements.enabled: false` acknowledges at receipt instead.
- Enrichment tables from CSV, JSON, YAML, MMDB, STIX and SQLite, read from VRL. A table with a `refresh` setting reloads while the service runs. MMDB loads once at start.
- The host and network functions (`get_env_var`, `get_hostname`, `http_request`, `dns_lookup`, `reverse_dns`) do not compile, so a transform cannot reach outside the process.

JSON is the only payload format. A record that is not JSON is dropped, counted in `records_error_total{stage="deserialise"}`, and not redelivered. Need one of Vector's own transforms beyond `remap`? That is [dfe-transform-vector](https://github.com/hyperi-io/dfe-transform-vector).

## Quick start

```bash
cargo build --release
./target/release/dfe-transform-vrl config-check --config config.example.yaml
./target/release/dfe-transform-vrl --config config.example.yaml
```

The smallest config names the source, the transform directory and the sink:

```yaml
pipeline:
  name: my-pipeline

source:
  brokers: ["kafka:9092"]
  topics: ["raw_events"]
  group_id: dfe-transform-vrl-my-pipeline

transforms:
  dir: /etc/dfe/transforms/

sink:
  brokers: ["kafka:9092"]
  topic: enriched_events
```

A transform file is plain VRL:

```vrl
# transforms/01_parse.vrl
.parsed = parse_json!(.message)
del(.message)
```

Health and metrics answer on port 9090: `/livez`, `/readyz` (503 until the pipeline runs, while memory is under pressure, and while a transport reports itself unhealthy) and `/metrics`.

## Configuration

[config.example.yaml](config.example.yaml) is the annotated reference, and [docs/config-schema.yaml](docs/config-schema.yaml) lists every key with its type and default. The big dials have flat environment overrides for Kubernetes, `DFE_TRANSFORM_VRL_SOURCE_BROKERS`, `DFE_TRANSFORM_VRL_SINK_TOPIC`, `DFE_TRANSFORM_VRL_TRANSFORMS_DIR` and the SASL pair among them. The whole list, the scalo sections that share the file on a double underscore, Kafka over TLS with a private CA, and the settings that parse but reach nothing are in [docs/internals.md](docs/internals.md).

`pipelines/filebeat/` ships a pure-VRL port of the DFE 2.1 filebeat templates (Cisco Meraki, IOS, Umbrella) with its `timezones.csv` table. Opt in by pointing `transforms.dir` and `enrichment_tables` at it. It is interim: dfe-transform-elastic replaces it.

## Documentation

- [docs/architecture.md](docs/architecture.md): the shape, who owns the loop, the delivery contract and the two config authorities.
- [docs/internals.md](docs/internals.md): data flow, VRL integration, memory budget, configuration, reload, operating notes.
- [pipelines/filebeat/README.md](pipelines/filebeat/README.md): wiring and limits of the filebeat bundle.
- [tests/TESTING.md](tests/TESTING.md): the test tiers.

The suite's published docs are at https://dfe.docs.hyperi.io.

## Help and contributing

How to work in this repo, the commands that prove a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in a new issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-transform-vrl/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, see [LICENSE](LICENSE).
