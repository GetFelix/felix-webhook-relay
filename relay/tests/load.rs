//! Intake under concurrency, and the performance measurements. Needs
//! `dev/up.sh`; see `delivery.rs`.
//!
//! The `measure_` tests print the numbers in `docs/performance.md`. They run
//! only with `RELAY_TEST_PERF=1`, in release mode and alone:
//! `RELAY_TEST_PERF=1 cargo test --release --test load -- --include-ignored --nocapture measure`.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use common::{Endpoint, Relay, TENANT, felix, unique};
use felix_client::StartPosition;
use felix_relay_core::Envelope;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Send `count` webhooks with `concurrency` in flight; returns each one's
/// body, offset and time to `202`, in sending order.
async fn blast(
    relay: &Arc<Relay>,
    path: &str,
    count: usize,
    concurrency: usize,
    body: impl Fn(usize) -> Vec<u8>,
) -> (Vec<(Vec<u8>, u64, Duration)>, Duration) {
    let gate = Arc::new(Semaphore::new(concurrency));
    let mut tasks = JoinSet::new();
    let started = Instant::now();
    for n in 0..count {
        let permit = Arc::clone(&gate).acquire_owned().await.unwrap();
        let (relay, path, body) = (Arc::clone(relay), path.to_string(), body(n));
        tasks.spawn(async move {
            let sent = Instant::now();
            let response = relay
                .http
                .post(relay.url(&path))
                .body(body.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED);
            let took = sent.elapsed();
            let answer: Value = response.json().await.unwrap();
            drop(permit);
            (n, body, answer["offset"].as_u64().unwrap(), took)
        });
    }
    let mut results = Vec::new();
    while let Some(done) = tasks.join_next().await {
        results.push(done.unwrap());
    }
    let elapsed = started.elapsed();
    results.sort_by_key(|r| r.0);
    (
        results.into_iter().map(|(_, b, o, t)| (b, o, t)).collect(),
        elapsed,
    )
}

async fn token_source(relay: &Relay, id: &str) -> String {
    let token = relay
        .create_source(id, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    let path = format!("/in/{TENANT}/{id}/{token}");
    // Waits out the config watch, then the first webhook is the warm-up.
    assert_eq!(
        relay.send(&path, &[], b"{}").await.status(),
        StatusCode::ACCEPTED
    );
    path
}

/// The records `path`'s source holds.
async fn sent_total(relay: &Relay, path: &str) -> u64 {
    let response = relay.send(path, &[], b"{}").await;
    response.json::<Value>().await.unwrap()["offset"]
        .as_u64()
        .unwrap()
        + 1
}

fn percentile(mut values: Vec<Duration>, p: usize) -> Duration {
    values.sort();
    values[(values.len() * p).div_ceil(100).max(1) - 1]
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn concurrent_webhooks_land_at_the_offsets_intake_reports() {
    let run = unique("batch");
    let relay = Arc::new(Relay::start("intake,admin", &[]).await);
    let path = token_source(&relay, &run).await;
    let (sent, _) = blast(&relay, &path, 400, 64, |n| {
        format!("{{\"n\":{n}}}").into_bytes()
    })
    .await;

    let client = felix().await;
    let first = sent.iter().map(|s| s.1).min().unwrap();
    let mut subscription = client
        .subscribe_from(
            "relay",
            TENANT,
            &format!("src.{run}"),
            Some(StartPosition::Offset(first)),
        )
        .await
        .unwrap();
    let mut stored = std::collections::HashMap::new();
    while stored.len() < sent.len() {
        let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        stored.insert(
            event.offset.unwrap(),
            Envelope::decode(&event.payload).unwrap().body,
        );
    }
    for (body, offset, _) in &sent {
        assert_eq!(stored.get(offset), Some(body), "offset {offset}");
    }
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn measure_the_performance_targets() {
    if std::env::var("RELAY_TEST_PERF").as_deref() != Ok("1") {
        eprintln!("skipped: a measurement; set RELAY_TEST_PERF=1");
        return;
    }
    let run = unique("perf");
    let body = vec![b'x'; 1024];
    let relay = Arc::new(
        Relay::start(
            "intake,deliver,admin",
            &[("RELAY_ENDPOINT_PREFIXES", &run), ("RUST_LOG", "warn")],
        )
        .await,
    );
    let path = token_source(&relay, &run).await;

    println!("| Measure | Concurrency | Webhooks | Rate (/s) | p50 | p99 |");
    println!("|---|---|---|---|---|---|");
    for (concurrency, count) in [(1, 2000), (8, 5000), (32, 10_000), (128, 20_000)] {
        let (sent, elapsed) = blast(&relay, &path, count, concurrency, |_| body.clone()).await;
        let latencies: Vec<Duration> = sent.iter().map(|s| s.2).collect();
        println!(
            "| Intake, request to 202 | {concurrency} | {count} | {:.0} | {:.1} ms | {:.1} ms |",
            count as f64 / elapsed.as_secs_f64(),
            percentile(latencies.clone(), 50).as_secs_f64() * 1000.0,
            percentile(latencies, 99).as_secs_f64() * 1000.0,
        );
    }

    // A new endpoint on that source walks its history first, acknowledging
    // each record unsent (Felix cannot start a group at an offset).
    let history = sent_total(&relay, &path).await;
    let walker = Endpoint::start().await;
    let walked = format!("{run}-walk");
    let started = Instant::now();
    relay.create_endpoint(&walked, &run, &walker.url()).await;
    relay.send(&path, &[], b"{}").await;
    walker.wait_for(1).await;
    let walk = started.elapsed();
    println!(
        "| New endpoint's walk past a source's history | | {history} | {:.0} | {:.1} s total | |",
        history as f64 / walk.as_secs_f64(),
        walk.as_secs_f64(),
    );

    // Send to endpoint receipt on a fresh source, one sender at a steady 50
    // per second.
    let fresh = format!("{run}-fresh");
    let path = token_source(&relay, &fresh).await;
    let endpoint = Endpoint::start().await;
    relay.create_endpoint(&fresh, &fresh, &endpoint.url()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut sent_at = std::collections::HashMap::new();
    let count = 1500;
    for n in 0..count {
        let started = Instant::now();
        let response = relay
            .send(&path, &[], format!("{{\"n\":{n}}}").as_bytes())
            .await;
        let id: Value = response.json().await.unwrap();
        sent_at.insert(id["id"].as_str().unwrap().to_string(), started);
        tokio::time::sleep(Duration::from_millis(20).saturating_sub(started.elapsed())).await;
    }
    let received = endpoint.wait_for(count).await;
    let latencies: Vec<Duration> = received
        .iter()
        .filter_map(|r| sent_at.get(r.id()).map(|at| r.at - *at))
        .collect();
    println!(
        "| Send to endpoint receipt, healthy endpoint | 1 at 50/s | {count} | 50 | {:.1} ms | {:.1} ms |",
        percentile(latencies.clone(), 50).as_secs_f64() * 1000.0,
        percentile(latencies, 99).as_secs_f64() * 1000.0,
    );
}

/// The relay process's resident memory, from `/proc`.
fn resident_mib(relay: &Relay) -> f64 {
    let status =
        std::fs::read_to_string(format!("/proc/{}/status", relay.child.id())).unwrap_or_default();
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|kb| kb.trim().trim_end_matches(" kB").trim().parse::<f64>().ok())
        .map_or(0.0, |kb| kb / 1024.0)
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn measure_endpoints_per_process() {
    if std::env::var("RELAY_TEST_PERF").as_deref() != Ok("1") {
        eprintln!("skipped: a measurement; set RELAY_TEST_PERF=1");
        return;
    }
    let idle: usize =
        std::env::var("RELAY_TEST_IDLE_ENDPOINTS").map_or(1000, |n| n.parse().unwrap());
    let active: usize =
        std::env::var("RELAY_TEST_ACTIVE_ENDPOINTS").map_or(200, |n| n.parse().unwrap());
    let rate: u64 = std::env::var("RELAY_TEST_FLEET_RATE").map_or(2, |n| n.parse().unwrap());
    let run = unique("fleet");
    let admin = Arc::new(Relay::start("intake,admin", &[]).await);
    let quiet = format!("{run}-quiet");
    let busy = format!("{run}-busy");
    token_source(&admin, &quiet).await;
    let path = token_source(&admin, &busy).await;
    let target = Endpoint::start().await;
    for n in 0..idle {
        admin
            .create_endpoint(&format!("{run}-idle-{n}"), &quiet, &target.url())
            .await;
    }
    for n in 0..active {
        let url = format!("{}?e={n}", target.url());
        admin
            .create_endpoint(&format!("{run}-active-{n}"), &busy, &url)
            .await;
    }
    let worker = Relay::start(
        "deliver",
        &[("RELAY_ENDPOINT_PREFIXES", &run), ("RUST_LOG", "warn")],
    )
    .await;
    // Every task has to be past its first poll before the timing means anything.
    tokio::time::sleep(Duration::from_secs(15)).await;
    let idle_mib = resident_mib(&worker);

    let webhooks = 50;
    let mut sent_at = std::collections::HashMap::new();
    for n in 0..webhooks {
        let started = Instant::now();
        let response = admin
            .send(&path, &[], format!("{{\"n\":{n}}}").as_bytes())
            .await;
        let id: Value = response.json().await.unwrap();
        sent_at.insert(id["id"].as_str().unwrap().to_string(), started);
        tokio::time::sleep(Duration::from_millis(1000 / rate).saturating_sub(started.elapsed()))
            .await;
    }
    let received = common::within(Duration::from_secs(120), "every delivery", async || {
        let received = target.received();
        (received.len() >= webhooks * active).then_some(received)
    })
    .await;
    let latencies: Vec<Duration> = received
        .iter()
        .filter_map(|r| sent_at.get(r.id()).map(|at| r.at - *at))
        .collect();
    println!(
        "| {idle} idle and {active} active endpoints, one process, {rate} webhooks/s to the active ones' source | {} deliveries | p50 {:.1} ms | p99 {:.1} ms | {idle_mib:.0} MiB idle, {:.0} MiB after |",
        latencies.len(),
        percentile(latencies.clone(), 50).as_secs_f64() * 1000.0,
        percentile(latencies, 99).as_secs_f64() * 1000.0,
        resident_mib(&worker),
    );
}
