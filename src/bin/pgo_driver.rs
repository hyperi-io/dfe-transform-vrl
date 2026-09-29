// Project:   dfe-transform-vrl
// File:      src/bin/pgo-driver.rs
// Purpose:   PGO workload driver — Kafka producer for VRL hot-path instrumentation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! PGO workload driver for `dfe-transform-vrl`.
//!
//! Replays the elastic/integrations filebeat corpus onto the source topic, as
//! the input events the bundled filebeat pipeline reads, so a PGO-instrumented
//! `dfe-transform-vrl` profiles the pipeline it ships with: Kafka consume ->
//! JSON parse -> `parse_groks`, `parse_timestamp` and the timezones lookup ->
//! JSON serialise -> Kafka produce. It then reads the transform's `/metrics`
//! and fails unless the records were transformed and delivered.
//!
//! Invoked by `scripts/pgo-workload.sh`, which owns the broker container, the
//! transform process and its config.
//!
//! Built only with `--features pgo-driver`. Main wrapper binary unaffected.
//!
//! Configuration via environment variables:
//! - `PGO_DRIVER_DURATION_SECS` (default 300) -- how long to produce
//! - `PGO_DRIVER_BROKERS` (default `127.0.0.1:19092`)
//! - `PGO_DRIVER_TOPIC` (default `pgo_source`) -- source topic the wrapper consumes
//! - `PGO_DRIVER_RPS` (default 5000) -- records per second
//! - `PGO_DRIVER_BATCH_LINGER_MS` (default 10) -- librdkafka batching
//! - `PGO_DRIVER_METRICS_ADDR` (default `127.0.0.1:9090`) -- the wrapper's metrics listener
//! - `PGO_DRIVER_SETTLE_SECS` (default 60) -- longest wait for deliveries to stop rising
//!
//! Exit codes:
//! - 0: produced for the full duration, and the wrapper delivered records
//!   with fewer errors than deliveries
//! - 1: setup failed, or the wrapper did not transform and deliver the load

#![allow(clippy::expect_used)]
// workload driver, not library code
// Throughput-rate maths and array indexing on a monotonically-increasing
// counter -- precision loss / 32-bit truncation are irrelevant to a load
// generator and never reached on the 64-bit targets we build.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

// The same corpus reader and input-event shape the filebeat test suites use.
#[path = "../../tests/common/filebeat.rs"]
mod filebeat_corpus;

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use tokio::time::{MissedTickBehavior, interval};

use crate::filebeat_corpus as fb;

/// How often the producer tops the sent count up to the configured rate.
const PRODUCE_TICK: Duration = Duration::from_millis(10);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cfg = Config::from_env();
    println!("pgo-driver starting: {cfg:#?}");

    let events = corpus_events();
    println!(
        "pgo-driver: replaying {} filebeat corpus events from {} logs",
        events.len(),
        fb::all_logs().len()
    );

    let producer: FutureProducer = match ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("client.id", "dfe-transform-vrl-pgo-driver")
        .set("linger.ms", cfg.batch_linger_ms.to_string())
        .set("compression.type", "zstd")
        .set("acks", "1")
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "262144")
        .set("message.timeout.ms", "30000")
        .create()
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("pgo-driver: producer init failed: {e}");
            std::process::exit(1);
        }
    };

    let stats = Arc::new(Stats::new());
    let deadline = Instant::now() + Duration::from_secs(cfg.duration_secs);

    let reporter_stats = Arc::clone(&stats);
    let reporter = tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(15));
        tick.tick().await; // skip immediate
        while Instant::now() < deadline {
            tick.tick().await;
            reporter_stats.report();
        }
    });

    produce(&producer, &cfg, &stats, &events, deadline).await;
    reporter.abort();

    if let Err(e) = producer.flush(Duration::from_secs(10)) {
        eprintln!("pgo-driver: flush error: {e}");
    }
    stats.report();

    if stats.sent.load(Ordering::Relaxed) == 0 {
        eprintln!("pgo-driver: FAIL -- no record was queued to {}", cfg.topic);
        std::process::exit(1);
    }

    match settle(&cfg.metrics_addr, Duration::from_secs(cfg.settle_secs)).await {
        Ok(counts) => match counts.verdict() {
            Ok(()) => println!("pgo-driver: complete -- {counts}"),
            Err(reason) => {
                eprintln!("pgo-driver: FAIL -- {reason} ({counts})");
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!(
                "pgo-driver: FAIL -- could not read {}/metrics: {e}",
                cfg.metrics_addr
            );
            std::process::exit(1);
        }
    }
}

// ===========================================================================
// Config
// ===========================================================================

#[derive(Clone, Debug)]
struct Config {
    duration_secs: u64,
    brokers: String,
    topic: String,
    rps: u32,
    batch_linger_ms: u32,
    metrics_addr: String,
    settle_secs: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            duration_secs: env_parsed("PGO_DRIVER_DURATION_SECS", 300),
            brokers: env_str("PGO_DRIVER_BROKERS", "127.0.0.1:19092"),
            topic: env_str("PGO_DRIVER_TOPIC", "pgo_source"),
            rps: env_parsed("PGO_DRIVER_RPS", 5000),
            batch_linger_ms: env_parsed("PGO_DRIVER_BATCH_LINGER_MS", 10),
            metrics_addr: env_str("PGO_DRIVER_METRICS_ADDR", "127.0.0.1:9090"),
            settle_secs: env_parsed("PGO_DRIVER_SETTLE_SECS", 60),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parsed<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ===========================================================================
// Corpus
// ===========================================================================

/// Every corpus line as the input event the bundled pipeline reads.
fn corpus_events() -> Vec<Vec<u8>> {
    let mut events = Vec::new();
    for log in fb::all_logs() {
        let conf = fb::corpus_conf(log);
        for line in fb::corpus_lines(log) {
            let event = fb::input_event(&line, conf.as_ref());
            events.push(serde_json::to_vec(&event).expect("a corpus event serialises"));
        }
    }
    assert!(!events.is_empty(), "the filebeat corpus has no events");
    events
}

// ===========================================================================
// Producer
// ===========================================================================

struct Stats {
    sent: AtomicU64,
    enqueue_errors: AtomicU64,
    deliver_errors: AtomicU64,
    start: Instant,
}

impl Stats {
    fn new() -> Self {
        Self {
            sent: AtomicU64::new(0),
            enqueue_errors: AtomicU64::new(0),
            deliver_errors: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    fn report(&self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let sent = self.sent.load(Ordering::Relaxed);
        let enq = self.enqueue_errors.load(Ordering::Relaxed);
        let del = self.deliver_errors.load(Ordering::Relaxed);
        println!(
            "pgo-driver [{elapsed:>6.1}s] sent={sent:>9} enq_err={enq} deliver_err={del} rate={:.0}/s",
            (sent as f64) / elapsed.max(1.0)
        );
    }
}

/// Queue corpus events at `cfg.rps` until `deadline`, cycling the corpus.
async fn produce(
    producer: &FutureProducer,
    cfg: &Config,
    stats: &Arc<Stats>,
    events: &[Vec<u8>],
    deadline: Instant,
) {
    let mut tick = interval(PRODUCE_TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let start = Instant::now();
    let mut queued = 0_u64;

    while Instant::now() < deadline {
        tick.tick().await;
        let due = (start.elapsed().as_secs_f64() * f64::from(cfg.rps)) as u64;
        while queued < due {
            let payload = &events[(queued as usize) % events.len()];
            let record: FutureRecord<'_, (), [u8]> =
                FutureRecord::to(&cfg.topic).payload(payload.as_slice());
            // Fire-and-forget: queueing returns at once, and a full librdkafka
            // queue leaves the rest of this tick's quota for the next tick.
            let Ok(delivery) = producer.send_result(record) else {
                stats.enqueue_errors.fetch_add(1, Ordering::Relaxed);
                break;
            };
            queued += 1;
            stats.sent.fetch_add(1, Ordering::Relaxed);
            let stats = Arc::clone(stats);
            tokio::spawn(async move {
                if let Ok(Err((_e, _msg))) = delivery.await {
                    stats.deliver_errors.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    }
}

// ===========================================================================
// Verdict from the wrapper's own metrics
// ===========================================================================

/// The wrapper's record counters, summed across their label sets.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counts {
    received: u64,
    delivered: u64,
    filtered: u64,
    errors: u64,
}

impl Counts {
    /// Parse the counters out of a Prometheus text exposition.
    fn parse(exposition: &str) -> Self {
        let mut counts = Self::default();
        for line in exposition.lines().filter(|l| !l.starts_with('#')) {
            let Some((series, value)) = line.rsplit_once(' ') else {
                continue;
            };
            let name = series.split('{').next().unwrap_or_default();
            let Ok(value) = value.parse::<f64>() else {
                continue;
            };
            let value = value as u64;
            match name {
                "records_received_total" => counts.received += value,
                "records_delivered_total" => counts.delivered += value,
                "records_filtered_total" => counts.filtered += value,
                "records_error_total" => counts.errors += value,
                _ => {}
            }
        }
        counts
    }

    /// Whether the wrapper did the work the profile is meant to capture.
    const fn verdict(self) -> Result<(), &'static str> {
        if self.delivered == 0 {
            return Err("the transform delivered no records");
        }
        if self.errors >= self.delivered {
            return Err("transform errors dominate the delivered records");
        }
        Ok(())
    }
}

impl std::fmt::Display for Counts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "received={} delivered={} filtered={} errors={}",
            self.received, self.delivered, self.filtered, self.errors
        )
    }
}

/// GET `/metrics` from `addr` and return the response body.
async fn scrape(addr: &str) -> std::io::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let request = async {
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        let head = format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n");
        stream.write_all(head.as_bytes()).await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        let (status, body) = response
            .split_once("\r\n\r\n")
            .unwrap_or((response.as_str(), ""));
        if !status.starts_with("HTTP/1.1 200") && !status.starts_with("HTTP/1.0 200") {
            let line = status.lines().next().unwrap_or_default();
            return Err(std::io::Error::other(format!("answered {line}")));
        }
        Ok(body.to_string())
    };
    tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .map_err(|_| std::io::Error::other("timed out"))?
}

/// Scrape until deliveries stop rising or `limit` passes, and return the last counts.
async fn settle(addr: &str, limit: Duration) -> std::io::Result<Counts> {
    let deadline = Instant::now() + limit;
    let mut last = Counts::parse(&scrape(addr).await?);
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let now = Counts::parse(&scrape(addr).await?);
        if now.delivered == last.delivered || Instant::now() >= deadline {
            return Ok(now);
        }
        last = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPOSITION: &str = "\
# HELP records_delivered_total Records delivered
# TYPE records_delivered_total counter
records_delivered_total 120
records_received_total 130
records_filtered_total 4
records_error_total{stage=\"deserialise\"} 1
records_error_total{stage=\"transform\"} 5
transport_sent_total{transport=\"kafka\"} 120
";

    #[test]
    fn counts_sum_every_stage_of_the_error_counter() {
        let counts = Counts::parse(EXPOSITION);
        assert_eq!(
            counts,
            Counts {
                received: 130,
                delivered: 120,
                filtered: 4,
                errors: 6
            }
        );
        assert_eq!(counts.verdict(), Ok(()));
    }

    #[test]
    fn nothing_delivered_or_errors_dominating_fails_the_workload() {
        assert!(Counts::default().verdict().is_err());
        let dominated = Counts {
            delivered: 10,
            errors: 10,
            ..Counts::default()
        };
        assert!(dominated.verdict().is_err());
    }
}
