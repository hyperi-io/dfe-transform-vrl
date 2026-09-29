#!/usr/bin/env bash
# Project:   dfe-transform-vrl
# File:      scripts/pgo-workload.sh
# Purpose:   PGO workload orchestrator -- Kafka + wrapper + producer
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   scripts/pgo-workload.sh <path-to-dfe-transform-vrl-binary>
#
# Runs the shipped filebeat pipeline (pipelines/filebeat) over the committed
# elastic/integrations corpus, so a PGO-instrumented binary profiles the hot
# path it ships with: Kafka consume -> JSON parse -> VRL (parse_groks,
# parse_timestamp, the timezones lookup) -> JSON serialise -> Kafka produce.
#
# Environment variables (all optional):
#   PGO_WORKLOAD_DURATION_SECS   Duration of load (default 300, floor 60)
#   PGO_WORKLOAD_KAFKA_IMAGE     Override the broker image
#   PGO_WORKLOAD_KAFKA_PORT      Host port the broker listens on (default 19092)
#   PGO_WORKLOAD_METRICS_PORT    Host port for the wrapper's metrics (default 9090)
#   PGO_WORKLOAD_KEEP            Set to 1 to skip cleanup (debug)
#   PGO_DRIVER_PATH              Override pgo-driver binary path
#   PGO_DRIVER_RPS               Records per second (default 5000)
#
# Preconditions:
#   - Docker daemon running, user has access
#   - $1 is the wrapper binary built with --features jemalloc
#   - pgo-driver binary built with --features pgo-driver (auto-built if missing)
#
# Behaviour:
#   - Starts single-node Redpanda (Kafka API) and creates the two topics
#   - Writes an ephemeral wrapper config pointing at pipelines/filebeat
#   - Starts the passed-in wrapper binary in background
#   - Waits for the wrapper's /readyz to return 200
#   - Runs pgo-driver, which produces the corpus for the configured duration
#     and exits non-zero unless the wrapper's /metrics shows delivered records
#     and fewer errors than deliveries -- so a workload that transformed
#     nothing fails here instead of profiling an idle binary
#   - Cleans up (traps EXIT): kills wrapper, removes container

set -euo pipefail

# ----------------------------------------------------------------------------
# Args + env
# ----------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <path-to-dfe-transform-vrl-binary>" >&2
    exit 1
fi

WRAPPER_BIN="$1"
if [[ ! -x "$WRAPPER_BIN" ]]; then
    echo "error: $WRAPPER_BIN is not executable" >&2
    exit 1
fi

DURATION="${PGO_WORKLOAD_DURATION_SECS:-300}"
# Held equal to the suite stack's broker (dfe-infra versions.yaml, services.redpanda-version).
KAFKA_IMAGE="${PGO_WORKLOAD_KAFKA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:v26.2.2@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96}"
KAFKA_PORT="${PGO_WORKLOAD_KAFKA_PORT:-19092}"
METRICS_PORT="${PGO_WORKLOAD_METRICS_PORT:-9090}"
KEEP="${PGO_WORKLOAD_KEEP:-0}"

# Floor of 60s -- shorter workloads produce bad PGO profiles
if [[ "$DURATION" -lt 60 ]]; then
    echo "error: PGO_WORKLOAD_DURATION_SECS must be >= 60 (got $DURATION)" >&2
    echo "  short workloads produce NEGATIVE PGO gains by biasing the" >&2
    echo "  compiler toward startup paths instead of hot paths" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PIPELINE_DIR="$PROJECT_ROOT/pipelines/filebeat"
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_ROOT/target}"

# Locate pgo-driver binary; build on demand if missing.
PGO_DRIVER_PATH="${PGO_DRIVER_PATH:-}"
if [[ -z "$PGO_DRIVER_PATH" ]]; then
    for candidate in \
        "$TARGET_DIR/release/pgo-driver" \
        "$TARGET_DIR/debug/pgo-driver"; do
        if [[ -x "$candidate" ]]; then
            PGO_DRIVER_PATH="$candidate"
            break
        fi
    done
fi
if [[ -z "$PGO_DRIVER_PATH" || ! -x "$PGO_DRIVER_PATH" ]]; then
    echo "pgo-workload: pgo-driver not found, building..." >&2
    (cd "$PROJECT_ROOT" && cargo build --release --features pgo-driver --bin pgo-driver) \
        || { echo "error: failed to build pgo-driver" >&2; exit 1; }
    PGO_DRIVER_PATH="$TARGET_DIR/release/pgo-driver"
    if [[ ! -x "$PGO_DRIVER_PATH" ]]; then
        echo "error: pgo-driver still missing after build at $PGO_DRIVER_PATH" >&2
        exit 1
    fi
fi

# A listener already on the metrics port would answer /readyz for a wrapper
# that never started.
if curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$METRICS_PORT/"; then
    echo "error: 127.0.0.1:$METRICS_PORT is already serving; set PGO_WORKLOAD_METRICS_PORT" >&2
    exit 1
fi

# ----------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------

WRAPPER_PID=""
KAFKA_CID=""
WORK_DIR=""

cleanup() {
    local rc=$?
    if [[ "$KEEP" == "1" ]]; then
        echo "PGO_WORKLOAD_KEEP=1 -- skipping cleanup" >&2
        echo "  wrapper PID: $WRAPPER_PID" >&2
        echo "  kafka CID:   $KAFKA_CID" >&2
        echo "  work dir:    $WORK_DIR" >&2
        return $rc
    fi
    echo "pgo-workload: cleanup" >&2
    if [[ -n "$WRAPPER_PID" ]] && kill -0 "$WRAPPER_PID" 2>/dev/null; then
        # The profile is written on a clean exit, so the wrapper gets time to drain first.
        kill -TERM "$WRAPPER_PID" 2>/dev/null || true
        for _ in $(seq 1 30); do
            if ! kill -0 "$WRAPPER_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill -KILL "$WRAPPER_PID" 2>/dev/null || true
    fi
    if [[ -n "$KAFKA_CID" ]]; then
        docker rm -f "$KAFKA_CID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$WORK_DIR" && -d "$WORK_DIR" ]]; then
        rm -rf "$WORK_DIR"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# Start Redpanda (Kafka-API broker -- lightweight, fits 4GB CI runners)
# ----------------------------------------------------------------------------

echo "pgo-workload: starting Redpanda ($KAFKA_IMAGE)"
# Redpanda (Kafka-API, C++/Seastar) replaces the Kafka JVM: the JVM's 1.5-2GB
# heap starves the PGO-instrumented binary on 4GB CI runners (arm64 ARC + the
# free OSS runners we target post-OSS). dev-container mode bundles
# --overprovisioned, --reserve-memory 0M, --check=false, --unsafe-bypass-fsync
# and auto-creates topics; the explicit --memory cap leaves headroom for the
# instrumented binary + load driver. Same Kafka wire protocol.
KAFKA_CID=$(docker run -d --rm \
    -p "$KAFKA_PORT:9092" \
    "$KAFKA_IMAGE" \
    redpanda start \
        --mode dev-container \
        --smp 1 \
        --memory 512M \
        --kafka-addr PLAINTEXT://0.0.0.0:9092 \
        --advertise-kafka-addr "PLAINTEXT://localhost:$KAFKA_PORT")
echo "pgo-workload: Redpanda CID: $KAFKA_CID"

# Real protocol readiness via the admin API (rpk), not a bare TCP-open probe:
# only reports healthy once the broker is actually serving.
for attempt in $(seq 1 60); do
    if docker exec "$KAFKA_CID" rpk cluster health 2>/dev/null | grep -q "Healthy:.*true"; then
        echo "pgo-workload: Redpanda ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: Redpanda did not become ready in 120s" >&2
        docker logs --tail 50 "$KAFKA_CID" >&2
        exit 1
    fi
    sleep 2
done

# Pre-create the topics. Redpanda auto-creates on PRODUCE but not on a consumer
# SUBSCRIBE, so the wrapper (which consumes pgo_source) would never find the
# topic and never reach ready -- and the load driver only starts after the
# wrapper is ready, so nothing ever produces it. Created from a --network host
# client so it reaches the advertised localhost listener.
for t in pgo_source pgo_sink; do
    if ! docker run --rm --network host "$KAFKA_IMAGE" \
        topic create "$t" -p 3 -X "brokers=localhost:$KAFKA_PORT"; then
        echo "error: could not create topic $t" >&2
        exit 1
    fi
done
echo "pgo-workload: created topics pgo_source, pgo_sink"

# ----------------------------------------------------------------------------
# Write the ephemeral wrapper config -- the shipped filebeat pipeline and its
# timezones table, the same two knobs a chart mounts
# ----------------------------------------------------------------------------

WORK_DIR=$(mktemp -d -t pgo-workload-XXXXXX)
CONFIG_FILE="$WORK_DIR/config.yaml"

# Only sections the wrapper reads: scalo's own settings (log level, metrics
# address) reach it through the env layer and the CLI below, never this file.
cat > "$CONFIG_FILE" <<YAML
pipeline:
  name: "pgo-workload"

source:
  brokers:
    - "localhost:$KAFKA_PORT"
  topics:
    - "pgo_source"
  group_id: "pgo-workload"
  auto_offset_reset: "earliest"

sink:
  brokers:
    - "localhost:$KAFKA_PORT"
  topic: "pgo_sink"
  compression: "zstd"

transforms:
  dir: "$PIPELINE_DIR"

enrichment_tables:
  - name: "timezones"
    path: "$PIPELINE_DIR/timezones.csv"
    key_columns: ["abbreviation"]
YAML

# ----------------------------------------------------------------------------
# Start wrapper
# ----------------------------------------------------------------------------

echo "pgo-workload: starting wrapper: $WRAPPER_BIN"
echo "pgo-workload: config: $CONFIG_FILE"
echo "pgo-workload: transforms: $PIPELINE_DIR"

# PGO profiles go here by default with cargo-pgo
export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE:-$TARGET_DIR/pgo-profiles/pgo-%p_%m.profraw}"
mkdir -p "$(dirname "$LLVM_PROFILE_FILE")"

LOG_LEVEL=warn LOG_FORMAT=json "$WRAPPER_BIN" \
    --config "$CONFIG_FILE" \
    --metrics-addr "127.0.0.1:$METRICS_PORT" \
    run \
    >"$WORK_DIR/wrapper.log" 2>&1 &
WRAPPER_PID=$!
echo "pgo-workload: wrapper PID: $WRAPPER_PID"

# An instrumented binary compiles the 212 KB filebeat program slowly on a small runner.
for attempt in $(seq 1 180); do
    if ! kill -0 "$WRAPPER_PID" 2>/dev/null; then
        echo "error: wrapper died during startup" >&2
        tail -100 "$WORK_DIR/wrapper.log" >&2
        exit 1
    fi
    # The probes are served by the metrics server; there is no separate
    # health listener.
    if curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:$METRICS_PORT/readyz"; then
        echo "pgo-workload: wrapper ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 180 ]]; then
        echo "error: wrapper did not become ready in 180s" >&2
        tail -100 "$WORK_DIR/wrapper.log" >&2
        exit 1
    fi
    sleep 1
done

# Extra settle so the consumer group is fully joined before we start producing
sleep 2

# ----------------------------------------------------------------------------
# Run load driver -- it fails the workload unless the wrapper delivered
# ----------------------------------------------------------------------------

echo "pgo-workload: driving load for ${DURATION}s via $PGO_DRIVER_PATH"

if ! PGO_DRIVER_DURATION_SECS="$DURATION" \
    PGO_DRIVER_BROKERS="127.0.0.1:$KAFKA_PORT" \
    PGO_DRIVER_TOPIC="pgo_source" \
    PGO_DRIVER_RPS="${PGO_DRIVER_RPS:-5000}" \
    PGO_DRIVER_METRICS_ADDR="127.0.0.1:$METRICS_PORT" \
    "$PGO_DRIVER_PATH"; then
    echo "error: the workload did not transform and deliver the corpus" >&2
    tail -100 "$WORK_DIR/wrapper.log" >&2
    exit 1
fi

echo "pgo-workload: done"
