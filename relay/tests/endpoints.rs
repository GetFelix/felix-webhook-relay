//! Many endpoints from config: start offsets, filters, unordered windows,
//! static assignment and isolation, against a real Felix broker. Needs
//! `dev/up.sh`; see `delivery.rs`.
//!
//! The isolation demonstration sends `RELAY_TEST_ISOLATION_WEBHOOKS`
//! webhooks per run (default 100).

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use common::{
    Endpoint, Received, Relay, TENANT, eventually, metric, read_stream, state_entry, unique,
};
use felix_relay_core::catalog::owner;
use felix_relay_core::records::DeadLetter;
use serde_json::{Value, json};

/// A token source; returns its intake path.
async fn source(admin: &Relay, id: &str, extra: Value) -> String {
    let mut config = json!({ "scheme": { "type": "token" } });
    config
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let token = admin.create_source(id, config).await.unwrap();
    format!("/in/{TENANT}/{id}/{token}")
}

async fn endpoint(admin: &Relay, id: &str, config: Value) {
    admin
        .admin(reqwest::Method::PUT, &format!("/endpoints/{id}"), config)
        .await;
}

/// Send each body in turn; returns the event ids.
async fn send(
    intake: &Relay,
    path: &str,
    bodies: &[&str],
    headers: &[(&str, &str)],
) -> Vec<String> {
    let mut ids = Vec::new();
    for body in bodies {
        let response = intake.send(path, headers, body.as_bytes()).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let answer: Value = response.json().await.unwrap();
        ids.push(answer["id"].as_str().unwrap().to_string());
    }
    ids
}

fn ids(received: &[Received]) -> Vec<String> {
    received.iter().map(|r| r.id().to_string()).collect()
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn endpoints_added_and_deleted_in_config_start_and_stop() {
    let run = unique("config");
    let target = Endpoint::start().await;
    let relay = Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT_PREFIXES", &run)]).await;
    let path = source(&relay, &run, json!({})).await;

    endpoint(&relay, &run, json!({ "source": run, "url": target.url() })).await;
    let sent = send(&relay, &path, &["{}"], &[]).await;
    assert_eq!(
        ids(&target.wait_for(1).await),
        sent,
        "started without a restart"
    );

    relay
        .admin(
            reqwest::Method::DELETE,
            &format!("/endpoints/{run}"),
            Value::Null,
        )
        .await;
    // A task blocked in a poll holds it open for up to the poll wait.
    tokio::time::sleep(Duration::from_secs(11)).await;
    let polls = metric(&relay, "relay_group_polls_total").await;
    send(&relay, &path, &["{}"], &[]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        metric(&relay, "relay_group_polls_total").await,
        polls,
        "no polling"
    );
    assert_eq!(
        target.received().len(),
        1,
        "nothing delivered after the delete"
    );
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_new_endpoint_starts_at_the_tail_unless_it_backfills() {
    let run = unique("start");
    let (fresh, backfilled) = (Endpoint::start().await, Endpoint::start().await);
    let relay = Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT_PREFIXES", &run)]).await;
    let path = source(&relay, &run, json!({})).await;
    let history = send(&relay, &path, &["{\"n\":0}", "{\"n\":1}", "{\"n\":2}"], &[]).await;

    endpoint(
        &relay,
        &format!("{run}-fresh"),
        json!({ "source": run, "url": fresh.url() }),
    )
    .await;
    endpoint(
        &relay,
        &format!("{run}-backfill"),
        json!({ "source": run, "url": backfilled.url(), "backfill": true }),
    )
    .await;
    let new = send(&relay, &path, &["{\"n\":3}", "{\"n\":4}"], &[]).await;

    assert_eq!(ids(&fresh.wait_for(2).await), new);
    let everything: Vec<String> = history.into_iter().chain(new).collect();
    assert_eq!(ids(&backfilled.wait_for(5).await), everything);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(fresh.received().len(), 2, "no history for the new endpoint");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_filtered_endpoint_gets_its_types_and_its_cursor_moves_on() {
    let run = unique("filter");
    let target = Endpoint::start().await;
    let relay = Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT_PREFIXES", &run)]).await;
    let path = source(&relay, &run, json!({ "event_type_header": "x-event-type" })).await;
    endpoint(
        &relay,
        &run,
        json!({ "source": run, "url": target.url(), "event_types": ["invoice.paid"] }),
    )
    .await;

    let mut wanted = Vec::new();
    let mut last = String::new();
    for kind in [
        "invoice.paid",
        "invoice.created",
        "invoice.paid",
        "invoice.created",
    ] {
        let sent = send(&relay, &path, &["{}"], &[("x-event-type", kind)]).await;
        if kind == "invoice.paid" {
            wanted.extend(sent.clone());
        }
        last = sent[0].clone();
    }
    assert_eq!(ids(&target.wait_for(2).await), wanted);
    let last: u64 = last.rsplit(':').next().unwrap().parse().unwrap();
    eventually("the cursor past the filtered records", async || {
        let health = state_entry(&format!("health/{run}")).await?;
        (health["last_acked"] == json!(last)).then_some(())
    })
    .await;
    assert_eq!(target.received().len(), 2);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn an_unordered_endpoint_keeps_its_window_in_flight() {
    let run = unique("window");
    let in_flight = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(std::sync::Mutex::new(HashSet::new()));
    let router = axum::Router::new().route(
        "/hook",
        axum::routing::post({
            let (in_flight, most, delivered) = (
                Arc::clone(&in_flight),
                Arc::clone(&most),
                Arc::clone(&delivered),
            );
            move |headers: axum::http::HeaderMap, body: axum::body::Bytes| async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                if body.starts_with(b"poison") {
                    return StatusCode::SERVICE_UNAVAILABLE;
                }
                let id = headers["webhook-id"].to_str().unwrap().to_string();
                delivered.lock().unwrap().insert(id);
                StatusCode::NO_CONTENT
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await });

    let relay = Relay::start(
        "intake,deliver,admin",
        &[
            ("RELAY_ENDPOINT_PREFIXES", &run),
            ("RELAY_REFUSED_RETRIES", "100ms,100ms"),
        ],
    )
    .await;
    let path = source(&relay, &run, json!({})).await;
    endpoint(
        &relay,
        &run,
        json!({ "source": run, "url": url, "mode": "unordered" }),
    )
    .await;
    let mut bodies: Vec<String> = (0..48).map(|n| format!("{{\"n\":{n}}}")).collect();
    bodies[5] = "poison".to_string();
    let bodies: Vec<&str> = bodies.iter().map(String::as_str).collect();
    let sent = send(&relay, &path, &bodies, &[]).await;

    let expected: HashSet<String> = sent
        .iter()
        .enumerate()
        .filter(|(n, _)| *n != 5)
        .map(|(_, id)| id.clone())
        .collect();
    eventually("every good webhook", async || {
        (*delivered.lock().unwrap() == expected).then_some(())
    })
    .await;
    assert_eq!(most.load(Ordering::SeqCst), 16, "the window, and no more");

    // The record that kept failing while others got through is set aside
    // rather than pausing the endpoint.
    let dead = read_stream("dead", 1, |bytes| {
        DeadLetter::decode(bytes).ok().filter(|d| d.endpoint == run)
    })
    .await;
    assert_eq!(dead[0].envelope.body, b"poison");
    assert_eq!(dead[0].last_status, Some(503));
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn two_processes_split_a_hundred_endpoints() {
    let run = unique("split");
    let target = Endpoint::start().await;
    let admin = Relay::start("intake,admin", &[]).await;
    let path = source(&admin, &run, json!({})).await;
    let endpoints: Vec<String> = (0..100).map(|n| format!("{run}-{n}")).collect();
    for id in &endpoints {
        let url = format!("{}?e={id}", target.url());
        endpoint(&admin, id, json!({ "source": run, "url": url })).await;
    }
    let mut workers = Vec::new();
    for index in ["0", "1"] {
        let name = format!("worker {index}");
        let env = [
            ("RELAY_ENDPOINT_PREFIXES", run.as_str()),
            ("RELAY_WORKER_COUNT", "2"),
            ("RELAY_WORKER_INDEX", index),
            ("RELAY_WORKER_NAME", name.as_str()),
        ];
        workers.push(Relay::start("deliver", &env).await);
    }
    // Every task has its group polling before the webhook goes in.
    tokio::time::sleep(Duration::from_secs(2)).await;
    send(&admin, &path, &["{}"], &[]).await;

    target.wait_for(100).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut per_endpoint: HashMap<String, usize> = HashMap::new();
    for received in target.received() {
        let query = received.uri.query().unwrap().to_string();
        *per_endpoint.entry(query[2..].to_string()).or_default() += 1;
    }
    assert_eq!(per_endpoint.len(), 100);
    assert!(
        per_endpoint.values().all(|&n| n == 1),
        "an endpoint was delivered twice"
    );

    let mut split = [0, 0];
    for id in &endpoints {
        let index = owner(id, 2);
        split[index as usize] += 1;
        let reporter = eventually("a health entry", async || {
            state_entry(&format!("health/{id}")).await
        })
        .await["reporter"]
            .clone();
        assert_eq!(reporter, json!(format!("worker {index}")), "{id}");
    }
    assert!(split[0] > 25 && split[1] > 25, "{split:?}");
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
    let path = source(&relay, run, json!({})).await;
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
        let id = send(&relay, &path, &[&format!("{{\"n\":{n}}}")], &[])
            .await
            .remove(0);
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
