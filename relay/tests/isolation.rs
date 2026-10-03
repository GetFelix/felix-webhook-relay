//! Demonstration 2: one endpoint that answers after 10 s does not delay the
//! others on its source. In a file of its own so that it runs alone: other
//! tests sharing the broker would be noise in a millisecond comparison.
//! Needs `dev/up.sh`; see `delivery.rs`.
//!
//! Sends `RELAY_TEST_ISOLATION_WEBHOOKS` webhooks per run (default 100).

mod common;

use std::collections::HashMap;
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

/// Delivery latency to `fast` endpoints, with `slow` ones beside them on the
/// same source: from sending each webhook to each fast endpoint receiving it.
async fn latencies(run: &str, fast: usize, slow: usize) -> Vec<Duration> {
    let webhooks: usize = std::env::var("RELAY_TEST_ISOLATION_WEBHOOKS")
        .map(|n| n.parse().unwrap())
        .unwrap_or(100);
    let relay = Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT_PREFIXES", run)]).await;
    let path = source(&relay, run).await;
    let quick = Endpoint::start().await;
    let stalled =
        Endpoint::replying_after(Duration::from_secs(10), |_, _| StatusCode::NO_CONTENT).await;
    for n in 0..fast {
        let url = format!("{}?e={n}", quick.url());
        endpoint(
            &relay,
            &format!("{run}-fast-{n}"),
            json!({ "source": run, "url": url }),
        )
        .await;
    }
    for n in 0..slow {
        let url = stalled.url();
        endpoint(
            &relay,
            &format!("{run}-slow-{n}"),
            json!({ "source": run, "url": url }),
        )
        .await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut sent = HashMap::new();
    for n in 0..webhooks {
        let started = Instant::now();
        let id = send(&relay, &path, &format!("{{\"n\":{n}}}")).await;
        sent.insert(id, started);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    quick
        .wait_for(webhooks * fast)
        .await
        .iter()
        .map(|r| r.at - sent[r.id()])
        .collect()
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_slow_endpoint_does_not_delay_the_others() {
    let alone = p99(latencies(&unique("alone"), 3, 0).await);
    let beside = p99(latencies(&unique("beside"), 3, 1).await);
    println!("p99 to the fast endpoints: {alone:?} alone, {beside:?} beside a 10 s endpoint");
    // 10% is the target; the 10 ms floor keeps scheduler noise on a shared
    // CI runner from failing a millisecond-scale comparison.
    assert!(
        beside <= alone.mul_f64(1.1) + Duration::from_millis(10),
        "{beside:?} beside the slow endpoint against {alone:?} without it"
    );
}
