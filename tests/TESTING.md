# Testing

## Test Modes

Tests support two backends, controlled by `TEST_MODE` in `.env`:

### Remote (default)

Uses the remote cluster endpoints from `.env`. Tests skip if endpoints are unreachable.

```bash
TEST_MODE=remote cargo nextest run --all-features
```

### Docker-local

Uses `dfe-docker` infra profile (Kafka on localhost, no auth, no TLS).

```bash
# Start infrastructure (once, stays running)
cd ../dfe-docker
docker compose --profile infra up -d

# Run tests
TEST_MODE=docker cargo nextest run --all-features

# Tear down (when done)
docker compose --profile infra down
```

Docker-local Kafka: `localhost:19092` (PLAINTEXT, no SASL)

## Test Categories

### Unit Tests

Run without any external infrastructure. Embedded in source files (`#[cfg(test)]`).

```bash
cargo nextest run --lib
```

### Integration Tests

In `tests/` directory. Most run without Kafka (VRL compilation, transforms, config).
Kafka-dependent tests use `skip_if_no_kafka!()` and skip cleanly when Kafka is unavailable.

```bash
cargo nextest run --all-features
```

### Core and full tiers

The core tier is every test that is not `#[ignore]`: unit, integration, smoke, and the e2e tests that use a broker container scalo starts (`filebeat_kafka`, `held_acks`, `kafka_tls`). PR and push runs use it, and so does `hyperi-ci check`.

```bash
cargo nextest run --all-features
```

The full tier adds the `#[ignore]` e2e tests (`kafka`, `cli_service`, `container_hygiene`), which prefer a live broker when `KAFKA_BROKERS` answers. A release runs it (`test.full.required_for_release`), and so does the nightly CI run.

```bash
cargo nextest run --all-features --run-ignored all --ignore-default-filter
hyperi-ci check --tier full
```

### Broker containers

scalo's test support starts every broker, from `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (`scalo::transport::kafka::test_support::KAFKA_IMAGE`). Locally and in CI the same way: the first test process that needs one starts it through Docker, and nothing has to be running beforehand.

- One PLAINTEXT broker serves the whole run (`shared_kafka`). Every nextest process joins it, and a watcher removes it when the run ends.
- `held_acks` holds a broker alone, because it pauses it, and removes it when the test ends.
- `kafka_tls` holds a TLS, mTLS and SASL_SSL broker alone (`TlsBroker`), with certificates from `alpine/openssl:3.5.9`. Its clients name no CA file and trust the bundle `SSL_CERT_FILE` names.

Containers are named `dfe-transform-vrl-test-integration-*` (or `HYPERI_TEST_CONTAINER_PREFIX`) and labelled `io.hyperi.test.suite=dfe-transform-vrl-integration`. A test that needs a broker skips without Docker locally and fails under `CI`.

The testing standard these tiers follow: `standards/universal/testing.md` in hyperi-ai.

## Test Infrastructure

### `tests/common/mod.rs`

Shared helpers used by integration and e2e tests:

- `TestMode::detect()` — reads `TEST_MODE` from `.env`
- `kafka_test_config()` — returns correct broker/auth config for the active mode
- `test_topic("suffix")` — generates unique topic names with timestamp
- `skip_if_no_kafka!()` — skips test if Kafka is unreachable
- `ensure_docker_infra()` — starts dfe-docker containers if needed
- `KafkaTestEnv::ensure()` -- a live broker when one answers, else the run's shared broker container. `hermetic()` is the shared container alone
- `KafkaTestEnv::hermetic_on_low_port()` -- a broker container the test holds alone, published below port 10240. `pause()` and `unpause()` freeze and resume it
- `ports::free_port()` -- a host port below 10240 for a test listener, outside the range the OS hands out ephemeral ports from

### `.env` and `.env.example`

`.env` contains real credentials (gitignored). `.env.example` has the same structure
with placeholder values (committed).
