# dfe-transform-vrl

Runs your VRL transforms over a DFE deployment's event stream, in-process.

[![Build Status](https://github.com/hyperi-io/dfe-transform-vrl/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-transform-vrl/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](LICENSE)

## What it does

- Sits between the land topic and dfe-loader, and runs the VRL (Vector Remap Language) files in a directory, in filename order, against each event. The VRL engine is compiled in, so a transform is a file you change, not a release.
- Kafka in and out, or a direct gRPC push on a deployment with no broker. The transform you write is the same on both.
- At-least-once by default: the source is acknowledged after the sink has the transformed records. `source.acknowledgements.enabled: false` acknowledges at receipt instead.
- Enrichment tables from CSV, JSON, YAML and STIX, read from VRL, plus MMDB and SQLite with the `enrichment-mmdb` and `enrichment-sqlite` build features. A table with a `refresh` setting reloads as the service runs. MMDB loads once at start.
- The host, network, file and decompression functions (`get_env_var`, `get_hostname`, `http_request`, `dns_lookup`, `reverse_dns`, `parse_proto`, `encode_proto`, `validate_json_schema`, `decode_gzip`, `decode_zlib`, `decode_zstd`, `decode_lz4`, `decode_snappy`) and the file arguments `parse_etld`'s `psl` and `parse_groks`' `alias_sources` do not compile, so a transform cannot read the host's environment or files, reach the network, or inflate a record without a bound.
- Built on [scalo](https://github.com/hyperi-io/scalo-rs), so config, logs, metrics, health probes and scaling signals work the way they do in every DFE Rust service.

JSON is the one payload format. A record that is not JSON is dropped, counted in `records_error_total{stage="deserialise"}`, and not redelivered. For Vector's transforms beyond `remap`, use [dfe-transform-vector](https://github.com/hyperi-io/dfe-transform-vector).

## Quick start

On Linux the build needs `protoc`, a C toolchain, `pkg-config` and librdkafka. scalo compiles its gRPC `.proto` files, `aws-lc-sys` compiles C, and `rdkafka-sys` links librdkafka through pkg-config. It rejects a librdkafka older than the version after the `+` in its `Cargo.lock` entry, and a Debian or Ubuntu package can be older than that, so there [build librdkafka from source](https://github.com/confluentinc/librdkafka#build-from-source) first.

```bash
sudo apt-get install -y protobuf-compiler build-essential pkg-config
cargo build --release
cp config.example.yaml config.yaml      # uncomment source, sink and transforms, then set the brokers, topics and transforms.dir
./target/release/dfe-transform-vrl --config config.yaml config-check
./target/release/dfe-transform-vrl --config config.yaml
```

`--config` goes before the subcommand, or `config-check` rejects it as an unexpected argument.

The smallest config names the source, a directory holding at least one `.vrl` file, and the sink:

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

Health and metrics answer on port 9090: `/livez`, `/readyz` (503 when memory is under pressure or a transport reports itself unhealthy, and 200 once the pipeline runs, an idle one included) and `/metrics`.

Outside the image, set `dlq.file.path` to a writable directory. The default, `/var/spool/scalo/dlq`, is one the image creates, and without a writable one the service logs that the DLQ has no backend and holds any dead letter unacknowledged at its source.

## Configuration

[config.example.yaml](config.example.yaml) is the annotated reference, and [docs/config-schema.yaml](docs/config-schema.yaml) lists every key of the transform's sections with its type and default. The big dials have flat environment overrides for Kubernetes, `DFE_TRANSFORM_VRL_SOURCE_BROKERS`, `DFE_TRANSFORM_VRL_SINK_TOPIC`, `DFE_TRANSFORM_VRL_TRANSFORMS_DIR` and the SASL pair among them. The whole list, Kafka over TLS with a private CA and the scalo sections that share the file on a double underscore are under [Operating notes](docs/reference/configuration.md#operating-notes) in docs/reference/configuration.md, and the settings that parse but reach nothing under [Reload](docs/reference/configuration.md#reload).

`pipelines/filebeat/` ships a legacy pure-VRL port of the DFE 2.1 filebeat templates (Cisco Meraki, IOS, Umbrella) with its `timezones.csv` table, kept for DFE/XDR 2.0 and 2.1 filebeat feeds. Opt in by pointing `transforms.dir` and `enrichment_tables` at it. Any other feed uses dfe-transform-elastic, which has the same sources as compiled Rust.

## Documentation

- [docs/architecture.md](docs/architecture.md): the parts, how data flows, the delivery contract and why it is built this way.
- [docs/internals.md](docs/internals.md): the process closure, VRL integration and the memory budget.
- [docs/reference/configuration.md](docs/reference/configuration.md): env overrides, Kafka TLS, reload and the chart.
- [pipelines/filebeat/README.md](pipelines/filebeat/README.md): wiring and limits of the filebeat bundle.
- [tests/TESTING.md](tests/TESTING.md): the test tiers.

The suite's published docs are at <https://dfe.docs.hyperi.io>, and this repository's pages at <https://dfe.docs.hyperi.io/dfe-transform-vrl/>.

## Help and contributing

How to work in this repo, the commands that test a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in an issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-transform-vrl/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, see [LICENSE](LICENSE).
