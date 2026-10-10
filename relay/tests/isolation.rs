//! Demonstration 2: one endpoint that answers after 10 s does not delay the
//! others on its source. In a file of its own so that it runs alone: other
//! tests sharing the broker would be noise in a millisecond comparison.
//! Needs `dev/up.sh`; see `delivery.rs`.
//!
//! Sends `RELAY_TEST_ISOLATION_WEBHOOKS` webhooks per run (default 100).

mod common;

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use common::{Endpoint, Relay, TENANT, unique};
use serde_json::{Value, json};

/// A token source; returns its intake path.
async fn source(admin: &Relay, id: &str) -> String {
    let token = admin
        .create_source(id, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    format!("/in/{TENANT}/{id}/{token}")
}

async fn endpoint(admin: &Relay, id: &str, config: Value) {
    admin
        .admin(reqwest::Method::PUT, &format!("/endpoints/{id}"), config)
        .await;
}

/// Send one webhook; returns its event id.
async fn send(intake: &Relay, path: &str, body: &str) -> String {
    let response = intake.send(path, &[], body.as_bytes()).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let answer: Value = response.json().await.unwrap();
    answer["id"].as_str().unwrap().to_string()
}

fn p99(mut latencies: Vec<Duration>) -> Duration {
    latencies.sort();
    latencies[(latencies.len() * 99).div_ceil(100) - 1]
}

/// One source with `fast` endpoints and `slow` ones beside them; returns its
/// intake path and the fast endpoints' server.
async fn setup(relay: &Relay, id: &str, fast: usize, slow: usize) -> (String, Endpoint) {
    let path = source(relay, id).await;
    let quick = Endpoint::start().await;
    let stalled =
        Endpoint::replying_after(Duration::from_secs(10), |_, _| StatusCode::NO_CONTENT).await;
    for n in 0..fast {
        let url = format!("{}?e={n}", quick.url());
        endpoint(
            relay,
            &format!("{id}-fast-{n}"),
            json!({ "source": id, "url": url }),
        )
        .await;
    }
    for n in 0..slow {
        let url = stalled.url();
        endpoint(
            relay,
            &format!("{id}-slow-{n}"),
            json!({ "source": id, "url": url }),
        )
        .await;
    }
    (path, quick)
}

/// From sending each webhook to each fast endpoint receiving it.
async fn latencies(
    quick: &Endpoint,
    sent: &HashMap<String, Instant>,
    count: usize,
) -> Vec<Duration> {
    quick
        .wait_for(count)
        .await
        .iter()
        .map(|r| r.at - sent[r.id()])
        .collect()
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_slow_endpoint_does_not_delay_the_others() {
    let webhooks: usize = std::env::var("RELAY_TEST_ISOLATION_WEBHOOKS")
        .map(|n| n.parse().unwrap())
        .unwrap_or(100);
    let run = unique("isolation");
    let (alone_id, beside_id) = (format!("{run}-alone"), format!("{run}-beside"));
    // A relay each, as two separate runs would have: one shared relay would
    // hide a slow endpoint that holds up the whole process.
    let alone_relay = Relay::start(
        "intake,deliver,admin",
        &[("RELAY_ENDPOINT_PREFIXES", &alone_id)],
    )
    .await;
    let beside_relay = Relay::start(
        "intake,deliver,admin",
        &[("RELAY_ENDPOINT_PREFIXES", &beside_id)],
    )
    .await;
    let (alone_path, alone_quick) = setup(&alone_relay, &alone_id, 3, 0).await;
    let (beside_path, beside_quick) = setup(&beside_relay, &beside_id, 3, 1).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The two sources take turns, so a stall on a shared CI runner lands on
    // both measurements rather than on whichever ran during it.
    let mut alone_sent = HashMap::new();
    let mut beside_sent = HashMap::new();
    for n in 0..webhooks {
        let body = format!("{{\"n\":{n}}}");
        let started = Instant::now();
        alone_sent.insert(send(&alone_relay, &alone_path, &body).await, started);
        let started = Instant::now();
        beside_sent.insert(send(&beside_relay, &beside_path, &body).await, started);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let alone = p99(latencies(&alone_quick, &alone_sent, webhooks * 3).await);
    let beside = p99(latencies(&beside_quick, &beside_sent, webhooks * 3).await);
    // Straight to stderr rather than println!, which the test harness
    // swallows for a passing test: passing runs are the baseline.
    let _ = writeln!(
        std::io::stderr(),
        "p99 to the fast endpoints: {alone:?} alone, {beside:?} beside a 10 s endpoint"
    );
    // 10% is the target; the 10 ms floor keeps scheduler noise on a shared
    // CI runner from failing a millisecond-scale comparison.
    assert!(
        beside <= alone.mul_f64(1.1) + Duration::from_millis(10),
        "{beside:?} beside the slow endpoint against {alone:?} without it"
    );
}
