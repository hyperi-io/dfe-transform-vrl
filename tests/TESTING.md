# Testing

## Tiers

Unit tests run without any external infrastructure, embedded in the source files (`#[cfg(test)]`):

```bash
cargo nextest run --lib
```

The integration tests in `tests/` run without a broker: VRL compilation, transforms, config, enrichment, the filebeat cases, the direct transport on local ports, and the pipeline over scalo's in-memory transport.

The core tier is every test that is not `#[ignore]`: unit, integration, smoke, and the e2e tests that use a broker container scalo starts (`filebeat_kafka`, `held_acks`, `kafka_tls`). PR and push runs use it, and so does `hyperi-ci check`.

```bash
cargo nextest run --all-features
```

The full tier adds the `#[ignore]` e2e tests. `kafka`, `cli_service` and `container_hygiene` prefer a live broker when `KAFKA_BROKERS` answers, else the run's shared container, and `connectivity` checks a live broker only and skips when none answers. A release runs the full tier (`test.full.required_for_release`), and so does the nightly CI run.

```bash
cargo nextest run --all-features --run-ignored all --ignore-default-filter
hyperi-ci check --tier full
```

## Broker containers

scalo's test support starts every broker, from the image `scalo::transport::kafka::test_support::KAFKA_IMAGE` pins. Locally and in CI the same way: the first test process that needs one starts it through Docker, and nothing has to be running beforehand.

- One PLAINTEXT broker serves the whole run (`shared_kafka`). Every nextest process joins it, and a watcher removes it when the run ends.
- `held_acks` holds a broker alone, because it pauses it, and removes it when the test ends.
- `kafka_tls` holds a TLS, mTLS and SASL_SSL broker alone (`TlsBroker`), with certificates from the openssl image scalo's test support pins. Its clients name no CA file and trust the bundle `SSL_CERT_FILE` names.

Containers are named `dfe-transform-vrl-test-integration-*` (or `HYPERI_TEST_CONTAINER_PREFIX`) and labelled `io.hyperi.test.suite=dfe-transform-vrl-integration`. A core-tier test that needs a broker skips without Docker locally and fails under `CI`.

## A live broker

`TEST_MODE`, read from the environment or this repo's own `.env`, picks where a test looks for a live broker. Unset or `remote`, it reads `KAFKA_BROKERS` (default `localhost:9092`) with `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USER` and `KAFKA_SASL_PASSWORD`. `docker` means `localhost:19092`, PLAINTEXT with no SASL, the host port dfe-docker publishes its broker on. A test that finds no live broker there falls back to the run's shared container, except `connectivity`, which skips.

```bash
TEST_MODE=docker cargo nextest run --all-features --run-ignored all --ignore-default-filter
```

`.env` is gitignored and holds a developer's own values. `.env.example`, committed, lists the names the harness and the app read, with placeholders.

## Shared helpers

`tests/common/mod.rs` holds the helpers the integration and e2e tests share:

- `TestMode::detect()` -- reads `TEST_MODE` once this repo's `.env` is loaded
- `kafka_test_config()` -- the live-broker config for the active mode
- `test_topic("suffix")` -- a unique topic name with a timestamp, prefixed by `TEST_TOPIC_PREFIX`
- `KafkaTestEnv::ensure()` -- a live broker when one answers, else the run's shared broker container. `hermetic()` is the shared container alone
- `KafkaTestEnv::hermetic_on_low_port()` -- a broker container the test holds alone, published below port 10240. `pause()` and `unpause()` freeze and resume it
- `ensure_kafka_or_skip!()` -- `ensure()`, skipping when it finds neither and failing under `CI`
- `ports::free_port()` -- a host port below 10240 for a test listener, outside the range the OS hands out ephemeral ports from
