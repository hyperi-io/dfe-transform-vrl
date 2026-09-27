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

### E2E Tests (Kafka)

Require a running Kafka broker. Marked with `#[ignore]` — run explicitly:

```bash
cargo nextest run --all-features --run-ignored
```

`filebeat_kafka` and `held_acks` are not ignored: each starts and drops a broker
container of its own, so they run wherever Docker is available. `held_acks`
freezes its broker with `docker pause`, which is why it never uses a live one.

## Test Infrastructure

### `tests/common/mod.rs`

Shared helpers used by integration and e2e tests:

- `TestMode::detect()` — reads `TEST_MODE` from `.env`
- `kafka_test_config()` — returns correct broker/auth config for the active mode
- `test_topic("suffix")` — generates unique topic names with timestamp
- `skip_if_no_kafka!()` — skips test if Kafka is unreachable
- `ensure_docker_infra()` — starts dfe-docker containers if needed
- `KafkaTestEnv::hermetic_on_low_port()` -- a broker container the test owns, published below port 10240. `pause()` and `unpause()` freeze and resume it
- `ports::free_port()` -- a host port below 10240 for a test listener, outside the range the OS hands out ephemeral ports from

### `.env` and `.env.example`

`.env` contains real credentials (gitignored). `.env.example` has the same structure
with placeholder values (committed).
