// Project:   dfe-transform-vrl
// File:      tests/e2e/filebeat_kafka.rs
// Purpose:   WS21 acceptance: the filebeat corpus through the app and a broker
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The transform hop of the WS21 chain, proved on real data.
//!
//! ```text
//! filebeat corpus -> filebeat_land -> dfe-transform-vrl -> filebeat_load
//! ```
//!
//! What makes this an acceptance test rather than another unit test:
//!
//! - The transform is the SHIPPED BINARY, started from a config file with
//!   `transforms.dir` and `enrichment_tables` pointed at the bundled
//!   pipeline -- the same two knobs a chart mounts.
//! - The topics are the real source-bound names. `filebeat_land` in and
//!   `filebeat_load` out is what a `filebeat` Source compiles to, and the
//!   app derives the source name back out of the input topic.
//! - The data is the elastic/integrations corpus, and the output is graded
//!   against its golden events by the same comparison the in-process suite
//!   uses (`tests/common/filebeat.rs`).
//!
//! The broker is a container this test starts and drops, never a live one:
//! `filebeat_land` on a shared broker is somebody else's data.
//!
//! NOT covered here, because it needs a cluster: the receiver hop that picks
//! the source, and the loader hop that lands rows in `ClickHouse`.

use std::time::{Duration, Instant};

use bytes::Bytes;
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaProfile, KafkaTransport};
use scalo::transport::{TransportBase, TransportReceiver, TransportSender};

use crate::common::{self, KafkaTestConfig};
use crate::filebeat_corpus as fb;

/// The source this instance is bound to, and the topics that follow from it.
const SOURCE: &str = "filebeat";
const LAND_TOPIC: &str = "filebeat_land";
const LOAD_TOPIC: &str = "filebeat_load";

/// A line no filebeat module claims, carried through to prove the
/// shape-unmatched catch-all still passes an event on rather than dropping it.
const UNMATCHED_PROBE: &str = "ws21 probe: a line no filebeat module claims";

/// How long the app gets to compile the 5.5K-line pipeline and join the
/// consumer group. Debug builds spend most of it in the VRL compiler.
const READY_TIMEOUT: Duration = Duration::from_secs(240);

/// How long the corpus gets to arrive on the sink topic once the app is ready.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(180);

/// One golden event, and the corpus line it grades.
struct Golden {
    log: &'static str,
    idx: usize,
    event: serde_json::Value,
}

/// Every corpus line as the app will read it off `filebeat_land`, paired with
/// the goldens they must produce.
fn corpus_workload() -> (Vec<serde_json::Value>, Vec<Golden>) {
    let mut inputs = Vec::new();
    let mut goldens = Vec::new();

    for log in fb::all_logs() {
        let lines = fb::corpus_lines(log);
        let expected = fb::corpus_expected(&format!("{log}-expected.json"));
        assert_eq!(
            lines.len(),
            expected.len(),
            "{log}: {} input lines but {} golden events",
            lines.len(),
            expected.len()
        );
        let conf = fb::corpus_conf(log);
        for (idx, (line, event)) in lines.iter().zip(expected).enumerate() {
            inputs.push(fb::input_event(line, conf.as_ref()));
            goldens.push(Golden { log, idx, event });
        }
    }

    inputs.push(fb::input_event(UNMATCHED_PROBE, None));
    (inputs, goldens)
}

fn kafka_config(kf: &KafkaTestConfig, topics: &[&str], group: &str) -> KafkaConfig {
    KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: kf.brokers.split(',').map(String::from).collect(),
        group: group.to_string(),
        client_id: format!("dfe-transform-vrl-ws21-{group}"),
        topics: topics.iter().map(|t| (*t).to_string()).collect(),
        auto_offset_reset: "earliest".to_string(),
        security_protocol: kf.security_protocol.clone(),
        ..KafkaConfig::devtest()
    }
}

/// A port the OS has just confirmed free. Racy in principle; the window
/// between the probe and the app's bind is microseconds on a test host.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let port = listener
        .local_addr()
        .expect("read the bound address")
        .port();
    drop(listener);
    port
}

/// The config file the app runs from: the bundled pipeline, its timezones
/// table, and the source-bound topics.
fn write_config(dir: &std::path::Path, brokers: &str) -> std::path::PathBuf {
    let pipeline_dir = fb::repo_path("pipelines/filebeat");
    // JSON is valid YAML 1.2, so serialising sidesteps quoting the paths.
    let config = serde_json::json!({
        "pipeline": {
            "name": SOURCE,
            "batch_size": 50,
            "batch_timeout_ms": 500,
        },
        "source": {
            "brokers": [brokers],
            "topics": [LAND_TOPIC],
            "group_id": format!("dfe-transform-vrl-{SOURCE}"),
            "format": "json",
            "auto_offset_reset": "earliest",
        },
        "sink": {
            "brokers": [brokers],
            "topic": LOAD_TOPIC,
            "compression": "none",
        },
        "transforms": { "dir": pipeline_dir.to_string_lossy() },
        "enrichment_tables": [{
            "name": "timezones",
            "path": pipeline_dir.join("timezones.csv").to_string_lossy(),
            "key_columns": ["abbreviation"],
        }],
        "logging": { "level": "info", "format": "text" },
        "scaling": { "enabled": false },
    });
    let path = dir.join("config.yaml");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&config).expect("config serialises"),
    )
    .expect("write config");
    path
}

/// GET a URL and return its status, or `None` if the request did not answer
/// within a couple of seconds. Small enough not to justify an HTTP client
/// dependency.
async fn http_status(host_port: &str, path: &str) -> Option<u16> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let request = async {
        let mut stream = tokio::net::TcpStream::connect(host_port).await.ok()?;
        let head = format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
        stream.write_all(head.as_bytes()).await.ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await.ok()?;
        response
            .lines()
            .next()?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()
    };
    tokio::time::timeout(Duration::from_secs(3), request)
        .await
        .ok()
        .flatten()
}

/// Read up to `want` JSON events off `topic`, stopping early once they arrive.
async fn drain(
    kf: &KafkaTestConfig,
    topic: &str,
    group: &str,
    want: usize,
    timeout: Duration,
) -> Vec<serde_json::Value> {
    let consumer = KafkaTransport::new(&kafka_config(kf, &[topic], group))
        .await
        .unwrap_or_else(|e| panic!("consumer on {topic}: {e}"));
    let mut out = Vec::with_capacity(want);
    let deadline = Instant::now() + timeout;
    while out.len() < want && Instant::now() < deadline {
        match consumer.recv(want).await {
            Ok(batch) => {
                for record in &batch.records {
                    match serde_json::from_slice(&record.payload) {
                        Ok(value) => out.push(value),
                        Err(e) => panic!("payload on {topic} is not JSON: {e}"),
                    }
                }
                let _ = consumer.commit(&batch.commit_tokens).await;
            }
            Err(e) => panic!("recv on {topic} failed: {e}"),
        }
    }
    let _ = consumer.close().await;
    out
}

/// Match every golden to an output event, consuming each output once.
///
/// The sink topic has one partition, so the output usually arrives in the
/// order it was produced; that position is tried first and the scan is the
/// fallback. Returns the goldens that nothing produced, each with the
/// closest candidate's problems.
fn match_goldens(goldens: &[Golden], outputs: &[serde_json::Value]) -> Vec<String> {
    let mut used = vec![false; outputs.len()];
    let mut unmatched = Vec::new();

    for (position, golden) in goldens.iter().enumerate() {
        let mut order: Vec<usize> = Vec::with_capacity(outputs.len());
        if position < outputs.len() {
            order.push(position);
        }
        order.extend((0..outputs.len()).filter(|i| *i != position));

        let mut closest: Option<Vec<String>> = None;
        let mut matched = false;
        for candidate in order {
            if used[candidate] {
                continue;
            }
            let problems = fb::problems_against_golden(
                golden.log,
                golden.idx,
                &golden.event,
                &outputs[candidate],
            );
            if problems.is_empty() {
                used[candidate] = true;
                matched = true;
                break;
            }
            if closest.as_ref().is_none_or(|c| problems.len() < c.len()) {
                closest = Some(problems);
            }
        }

        if !matched {
            let detail = closest.map_or_else(
                || "no output event was left to compare against".to_string(),
                |problems| problems.join("; "),
            );
            unmatched.push(format!("{}[{}]: {detail}", golden.log, golden.idx));
        }
    }
    unmatched
}

#[tokio::test]
async fn filebeat_corpus_round_trips_through_kafka() {
    let Some(env) = common::KafkaTestEnv::hermetic("filebeat-corpus-round-trip").await else {
        common::require_service_in_ci("Kafka", "no Docker to start a broker container");
        eprintln!("SKIP: no Docker, so this test cannot own a broker.");
        return;
    };
    let kf = env.config();

    let (inputs, goldens) = corpus_workload();
    eprintln!(
        "WS21: replaying {} corpus lines from {} logs through {LAND_TOPIC} -> {LOAD_TOPIC}",
        inputs.len(),
        fb::all_logs().len()
    );

    let admin =
        KafkaAdmin::new(&kafka_config(kf, &[LAND_TOPIC], "ws21-admin")).expect("admin client");
    admin
        .create_topics(&[(LAND_TOPIC, 1, 1), (LOAD_TOPIC, 1, 1)])
        .await
        .expect("create the source-bound topics");

    let producer = KafkaTransport::new(&kafka_config(kf, &[LAND_TOPIC], "ws21-seed"))
        .await
        .expect("seed producer");
    // `send`'s first argument is the DESTINATION, which for Kafka is the topic
    // name rather than a partition key.
    for (i, event) in inputs.iter().enumerate() {
        let payload = serde_json::to_vec(event).expect("event serialises");
        let sent = producer.send(LAND_TOPIC, Bytes::from(payload)).await;
        assert!(
            matches!(
                sent,
                scalo::transport::SendResult::Ok | scalo::transport::SendResult::Backpressured
            ),
            "seeding {LAND_TOPIC} failed at event {i}: {sent:?}"
        );
    }
    let _ = producer.close().await;

    // The seed has to be on the topic before the app is asked to read it, or a
    // later empty sink says nothing about the transform.
    let seeded = drain(
        kf,
        LAND_TOPIC,
        "ws21-seed-check",
        inputs.len(),
        Duration::from_secs(60),
    )
    .await;
    assert_eq!(
        seeded.len(),
        inputs.len(),
        "seeding {LAND_TOPIC} put {} of {} events on the topic",
        seeded.len(),
        inputs.len()
    );

    let work = tempfile::tempdir().expect("work dir");
    let config_path = write_config(work.path(), &kf.brokers);
    // The probes are served by the metrics server, so readiness is polled there.
    let metrics = format!("127.0.0.1:{}", free_port());

    // kill_on_drop reaps the app on every exit path, assertion failures
    // included.
    let mut app = tokio::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vrl"))
        .arg("--config")
        .arg(&config_path)
        .arg("--metrics-addr")
        .arg(&metrics)
        .arg("run")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn dfe-transform-vrl");

    let deadline = Instant::now() + READY_TIMEOUT;
    let mut ready = false;
    while Instant::now() < deadline {
        if let Ok(Some(status)) = app.try_wait() {
            panic!("dfe-transform-vrl exited before becoming ready: {status}");
        }
        if http_status(&metrics, "/readyz").await == Some(200) {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        ready,
        "dfe-transform-vrl did not report ready on {metrics} within {READY_TIMEOUT:?}"
    );

    let outputs = drain(kf, LOAD_TOPIC, "ws21-verify", inputs.len(), DRAIN_TIMEOUT).await;
    let _ = app.start_kill();

    // Assertion 1 -- everything that went in came out. A transform that
    // erred an event dropped it, which is the failure this catches first.
    assert_eq!(
        outputs.len(),
        inputs.len(),
        "seeded {} events on {LAND_TOPIC} but {LOAD_TOPIC} carried {}",
        inputs.len(),
        outputs.len()
    );

    // Assertion 2 -- the shape-unmatched catch-all passes an event through
    // tagged, rather than dropping it.
    let probe_at = outputs
        .iter()
        .position(|e| e.get("message").and_then(serde_json::Value::as_str) == Some(UNMATCHED_PROBE))
        .expect("the unmatched probe must arrive on the sink topic");
    assert!(
        fb::is_unmatched(&outputs[probe_at]),
        "an unclaimed line must carry filebeat_unmatched: {}",
        outputs[probe_at]
    );

    // Assertion 3 -- every corpus line agrees with its elastic golden, after
    // the port's documented divergences.
    let mut corpus_outputs = outputs;
    corpus_outputs.remove(probe_at);
    let unmatched = match_goldens(&goldens, &corpus_outputs);
    assert!(
        unmatched.is_empty(),
        "{} of {} corpus events did not match their golden:\n{}",
        unmatched.len(),
        goldens.len(),
        unmatched.join("\n")
    );

    // Assertion 4 -- all three module branches actually ran. A pipeline that
    // detected nothing would still satisfy the count above by passing
    // everything through unmatched.
    let branch_ran = |module: &str, probe: &dyn Fn(&serde_json::Value) -> bool| {
        assert!(
            corpus_outputs.iter().any(probe),
            "no output event came from the {module} branch"
        );
    };
    let product_is = |event: &serde_json::Value, want: &str| {
        event
            .get("observer")
            .and_then(|o| o.get("product"))
            .and_then(serde_json::Value::as_str)
            == Some(want)
    };
    branch_ran("umbrella", &|e| product_is(e, "Umbrella"));
    branch_ran("ios", &|e| product_is(e, "IOS"));
    branch_ran("meraki", &|e| e.get("cisco_meraki").is_some());
}
