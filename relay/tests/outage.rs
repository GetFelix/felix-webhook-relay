//! Retries, pausing, dead letters, the claim wait and disabling, against a
//! real Felix broker. Needs `dev/up.sh`; see `delivery.rs`.
//!
//! The outage demonstration reads its size from the environment, so CI runs
//! a short one and a long one with the same code:
//! `RELAY_TEST_WEBHOOKS` (default 300), `RELAY_TEST_OUTAGE` (default `8s`,
//! longer than the dev broker's 5 s claim timeout) and `RELAY_TEST_BACKOFF`
//! (default `200ms,500ms,1s`; the relay's own default is `5s,15s,1m,2m,5m`).

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use common::{
    Endpoint, Received, Relay, TENANT, eventually, metric, read_stream, state_entry, unique, within,
};
use felix_relay_core::health::{parse_duration, parse_durations};
use felix_relay_core::records::{Attempt, DeadLetter};
use serde_json::{Value, json};
use tokio::task::JoinSet;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// A token source and an endpoint on it; returns the intake path.
async fn setup(admin: &Relay, id: &str, url: &str) -> String {
    let token = admin
        .create_source(id, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    admin.create_endpoint(id, id, url).await;
    format!("/in/{TENANT}/{id}/{token}")
}

/// Send `bodies` with up to 16 in flight; returns `(offset, id)` in intake order.
async fn send_all(intake: &Arc<Relay>, path: &str, bodies: Vec<String>) -> Vec<(u64, String)> {
    let mut accepted = Vec::new();
    let mut tasks = JoinSet::new();
    for body in bodies {
        if tasks.len() >= 16 {
            accepted.push(tasks.join_next().await.unwrap().unwrap());
        }
        let intake = Arc::clone(intake);
        let path = path.to_string();
        tasks.spawn(async move {
            let response = intake.send(&path, &[], body.as_bytes()).await;
            assert_eq!(response.status(), StatusCode::ACCEPTED);
            let answer: Value = response.json().await.unwrap();
            let offset = answer["offset"].as_u64().unwrap();
            (offset, answer["id"].as_str().unwrap().to_string())
        });
    }
    while let Some(done) = tasks.join_next().await {
        accepted.push(done.unwrap());
    }
    accepted.sort();
    accepted
}

/// The ids the endpoint took, in the order it took them.
fn delivered(received: &[Received]) -> Vec<String> {
    received
        .iter()
        .filter(|r| r.status.is_success())
        .map(|r| r.id().to_string())
        .collect()
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn an_endpoint_that_was_down_gets_everything_in_order() {
    let webhooks: usize = env_or("RELAY_TEST_WEBHOOKS", "300").parse().unwrap();
    let outage = parse_duration(&env_or("RELAY_TEST_OUTAGE", "8s")).unwrap();
    let backoff = env_or("RELAY_TEST_BACKOFF", "200ms,500ms,1s");
    let longest_wait = *parse_durations(&backoff).unwrap().iter().max().unwrap();

    let run = unique("outage");
    let down = Arc::new(AtomicBool::new(true));
    let endpoint = Endpoint::replying({
        let down = Arc::clone(&down);
        move |_, _| {
            if down.load(Ordering::SeqCst) {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::NO_CONTENT
            }
        }
    })
    .await;
    let relay = Arc::new(
        Relay::start(
            "intake,deliver,admin",
            &[
                ("RELAY_ENDPOINT_PREFIXES", &run),
                ("RELAY_BACKOFF", &backoff),
            ],
        )
        .await,
    );
    let path = setup(&relay, &run, &endpoint.url()).await;

    let outage_started = Instant::now();
    let bodies = (0..webhooks).map(|n| format!("{{\"n\":{n}}}")).collect();
    let accepted = send_all(&relay, &path, bodies).await;
    endpoint.wait_for(1).await;
    let health = eventually("a paused health entry", async || {
        let health = state_entry(&format!("health/{run}")).await?;
        (health["state"] == "paused").then_some(health)
    })
    .await;
    assert!(health["last_error"].as_str().unwrap().starts_with("503"));

    // A paused endpoint polls nothing: its cursor holds still in the log.
    let polls = metric(&relay, "relay_group_polls_total").await;
    tokio::time::sleep(outage.saturating_sub(outage_started.elapsed())).await;
    assert_eq!(
        metric(&relay, "relay_group_polls_total").await,
        polls,
        "no polls during the outage"
    );

    down.store(false, Ordering::SeqCst);
    // Within one backoff interval of coming back, plus its jitter.
    let limit = longest_wait.mul_f64(1.2) + Duration::from_secs(1);
    within(limit, "the first delivery after the outage", async || {
        endpoint
            .received()
            .iter()
            .any(|r| r.status.is_success())
            .then_some(())
    })
    .await;
    let drain = Duration::from_secs(60) + Duration::from_millis(webhooks as u64 * 20);
    let all = within(drain, "every webhook delivered", async || {
        let received = endpoint.received();
        (delivered(&received).len() >= webhooks).then_some(received)
    })
    .await;
    let expected: Vec<String> = accepted.iter().map(|(_, id)| id.clone()).collect();
    assert_eq!(delivered(&all), expected, "each once, in intake order");

    // The trail matches what the endpoint saw.
    let refused = all.iter().filter(|r| !r.status.is_success()).count();
    let attempts = read_stream("attempts", all.len(), |bytes| {
        Attempt::decode(bytes).ok().filter(|a| a.endpoint == run)
    })
    .await;
    assert_eq!(
        attempts.iter().filter(|a| a.status == Some(503)).count(),
        refused
    );
    assert_eq!(
        attempts.iter().filter(|a| a.status == Some(204)).count(),
        webhooks
    );
    let last = accepted.last().unwrap().0;
    eventually("a health entry that has caught up", async || {
        let health = state_entry(&format!("health/{run}")).await?;
        (health["state"] == "active" && health["last_acked"] == json!(last)).then_some(())
    })
    .await;
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_refused_record_is_dead_lettered_and_the_rest_arrive() {
    let run = unique("dead");
    let endpoint = Endpoint::replying(|_, received| {
        if received.body.starts_with(b"poison") {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::NO_CONTENT
        }
    })
    .await;
    let relay = Arc::new(
        Relay::start(
            "intake,deliver,admin",
            &[
                ("RELAY_ENDPOINT_PREFIXES", &run),
                ("RELAY_REFUSED_RETRIES", "100ms,200ms"),
            ],
        )
        .await,
    );
    let path = setup(&relay, &run, &endpoint.url()).await;
    let mut accepted = Vec::new();
    for body in ["one", "poison two", "three", "four"] {
        accepted.extend(send_all(&relay, &path, vec![body.to_string()]).await);
    }

    let all = eventually("the records after the refused one", async || {
        let received = endpoint.received();
        (delivered(&received).len() >= 3).then_some(received)
    })
    .await;
    let ids =
        |picks: &[usize]| -> Vec<String> { picks.iter().map(|&i| accepted[i].1.clone()).collect() };
    assert_eq!(delivered(&all), ids(&[0, 2, 3]));
    let poisoned: Vec<&Received> = all.iter().filter(|r| r.id() == accepted[1].1).collect();
    assert_eq!(poisoned.len(), 3, "tried three times");

    let dead = read_stream("dead", 1, |bytes| {
        DeadLetter::decode(bytes).ok().filter(|d| d.endpoint == run)
    })
    .await;
    assert_eq!(dead[0].offset, accepted[1].0);
    assert_eq!(dead[0].source, run);
    assert_eq!(dead[0].attempts, 3);
    assert_eq!(dead[0].last_status, Some(400));
    assert_eq!(dead[0].envelope.body, b"poison two");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_restarted_worker_resumes_in_order() {
    let run = unique("restart");
    let endpoint =
        Endpoint::replying_after(Duration::from_millis(100), |_, _| StatusCode::NO_CONTENT).await;
    let intake = Arc::new(Relay::start("intake,admin", &[]).await);
    let path = setup(&intake, &run, &endpoint.url()).await;
    let bodies = (0..40).map(|n| format!("{{\"n\":{n}}}")).collect();
    let accepted = send_all(&intake, &path, bodies).await;

    // The claim wait must cover the dev broker's 5 s visibility timeout.
    let worker = [
        ("RELAY_ENDPOINT_PREFIXES", run.as_str()),
        ("RELAY_CLAIM_WAIT_MS", "5000"),
    ];
    let first = Relay::start("deliver", &worker).await;
    endpoint.wait_for(10).await;
    // Dropping kills it with SIGKILL, mid-batch.
    drop(first);
    let before = endpoint.received().len();
    let _second = Relay::start("deliver", &worker).await;

    let all = eventually("every webhook after the restart", async || {
        let received = endpoint.received();
        let ids: HashSet<&str> = received.iter().map(|r| r.id()).collect();
        (ids.len() >= accepted.len()).then_some(received)
    })
    .await;
    let order: Vec<u64> = all
        .iter()
        .map(|r| accepted.iter().find(|(_, id)| id == r.id()).unwrap().0)
        .collect();
    assert!(
        order.windows(2).all(|pair| pair[0] <= pair[1]),
        "delivered out of order across the restart: {order:?}"
    );
    assert!(
        all.len() - accepted.len() <= 1,
        "at most the record in flight repeats; {before} arrived before the kill"
    );
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn gone_disables_the_endpoint_until_it_is_enabled() {
    let run = unique("gone");
    let endpoint = Endpoint::replying(|before, _| match before {
        0 => StatusCode::GONE,
        _ => StatusCode::NO_CONTENT,
    })
    .await;
    let relay =
        Arc::new(Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT_PREFIXES", &run)]).await);
    let path = setup(&relay, &run, &endpoint.url()).await;
    let bodies = (0..3).map(|n| format!("{{\"n\":{n}}}")).collect();
    let accepted = send_all(&relay, &path, bodies).await;

    endpoint.wait_for(1).await;
    let shown = eventually("the endpoint disabled in config", async || {
        let shown = relay
            .admin(
                reqwest::Method::GET,
                &format!("/endpoints/{run}"),
                Value::Null,
            )
            .await;
        shown.get("disabled").is_some().then_some(shown)
    })
    .await;
    assert!(
        shown["disabled"]["reason"]
            .as_str()
            .unwrap()
            .contains("410")
    );
    eventually("a disabled health entry", async || {
        let health = state_entry(&format!("health/{run}")).await?;
        (health["state"] == "disabled").then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(endpoint.received().len(), 1, "nothing sent while disabled");

    relay
        .admin(
            reqwest::Method::POST,
            &format!("/endpoints/{run}/enable"),
            json!({}),
        )
        .await;
    let all = endpoint.wait_for(4).await;
    let expected: Vec<String> = accepted.iter().map(|(_, id)| id.clone()).collect();
    assert_eq!(delivered(&all), expected, "resumed from the cursor");
}
