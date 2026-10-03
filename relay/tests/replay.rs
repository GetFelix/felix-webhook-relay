//! Offset for a time, replays and redrives, against a real Felix broker.
//! Needs `dev/up.sh`; see `delivery.rs`.
//!
//! The search test writes `RELAY_TEST_SEARCH_RECORDS` records (default
//! 100,000; the demonstrations job uses 10 million).

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use common::{Endpoint, Received, Relay, TENANT, create_stream, eventually, felix, unique, within};
use felix_relay_core::Envelope;
use serde_json::{Value, json};

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// A token source with an endpoint on it; returns the intake path.
async fn setup(admin: &Relay, id: &str, url: &str) -> String {
    let token = admin
        .create_source(id, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    admin.create_endpoint(id, id, url).await;
    format!("/in/{TENANT}/{id}/{token}")
}

async fn send(intake: &Relay, path: &str, count: usize) -> Vec<String> {
    let mut ids = Vec::new();
    for n in 0..count {
        let response = intake
            .send(path, &[], format!("{{\"n\":{n}}}").as_bytes())
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let answer: Value = response.json().await.unwrap();
        ids.push(answer["id"].as_str().unwrap().to_string());
    }
    ids
}

fn replayed<'a>(received: &'a [Received], job: &str) -> Vec<&'a Received> {
    received
        .iter()
        .filter(|r| r.header("webhook-replay") == Some(job))
        .collect()
}

async fn job(relay: &Relay, id: &str) -> Value {
    relay
        .admin(reqwest::Method::GET, &format!("/jobs/{id}"), Value::Null)
        .await
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn the_offset_for_a_time_is_found_by_bisecting_the_log() {
    let records: u64 = std::env::var("RELAY_TEST_SEARCH_RECORDS")
        .map(|n| n.parse().unwrap())
        .unwrap_or(100_000);
    let source = unique("search");
    let stream = format!("src.{source}");
    create_stream(&stream).await;
    // Envelopes 10 ms apart, written straight to the stream in big batches.
    let base = 1_700_000_000_000;
    let client = felix().await;
    let producer = client.idempotent_producer().await.unwrap();
    let batch = 10_000;
    for start in (0..records).step_by(batch as usize) {
        let payloads = (start..(start + batch).min(records))
            .map(|i| {
                Envelope {
                    id: None,
                    received_at: base + i * 10,
                    event_type: None,
                    content_type: None,
                    headers: Vec::new(),
                    body: Vec::new(),
                }
                .encode()
            })
            .collect();
        producer
            .publish_batch("relay", TENANT, &stream, payloads)
            .await
            .unwrap();
    }

    let relay = Relay::start("admin", &[]).await;
    for (at, expected) in [
        (base - 1, 0),
        (base, 0),
        (base + 1, 1),
        (base + 10 * (records / 3), records / 3),
        (base + 10 * (records - 1), records - 1),
        (base + 10 * records, records),
    ] {
        let answer = relay
            .admin(
                reqwest::Method::GET,
                &format!("/sources/{source}/offset?at={at}"),
                Value::Null,
            )
            .await;
        assert_eq!(answer["offset"], json!(expected), "at {at}");
        let millis = answer["millis"].as_u64().unwrap();
        println!(
            "{records} records: offset for a time in {millis} ms, {} reads",
            answer["reads"]
        );
        assert!(millis < 1000, "{millis} ms for {records} records");
    }
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_replay_sends_exactly_its_window_beside_live_delivery() {
    let run = unique("replay");
    let endpoint = Endpoint::start().await;
    let relay = Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT_PREFIXES", &run)]).await;
    let path = setup(&relay, &run, &endpoint.url()).await;

    let before = send(&relay, &path, 5).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let since = now_millis();
    let inside = send(&relay, &path, 5).await;
    let until = now_millis();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = send(&relay, &path, 5).await;
    endpoint.wait_for(15).await;

    let started = relay
        .admin(
            reqwest::Method::POST,
            &format!("/endpoints/{run}/replays"),
            json!({ "since": since, "until": until }),
        )
        .await;
    let job_id = started["id"].as_str().unwrap().to_string();
    let during = send(&relay, &path, 5).await;

    let received = eventually("the replay and the live webhooks", async || {
        let received = endpoint.received();
        (replayed(&received, &job_id).len() >= 5 && received.len() >= 25).then_some(received)
    })
    .await;
    let replay: Vec<&str> = replayed(&received, &job_id)
        .iter()
        .map(|r| r.id())
        .collect();
    assert_eq!(replay, inside, "exactly the window, in intake order");
    let live: Vec<&str> = received
        .iter()
        .filter(|r| r.header("webhook-replay").is_none())
        .map(|r| r.id())
        .collect();
    let expected: Vec<String> = [before, inside, after, during].concat();
    assert_eq!(live, expected, "live delivery carried on");

    let done = eventually("the job to finish", async || {
        let job = job(&relay, &job_id).await;
        (job["status"] == "done").then_some(job)
    })
    .await;
    assert_eq!(done["sent"], json!(5));
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_killed_worker_resumes_a_replay_from_its_checkpoint() {
    let run = unique("resume");
    let endpoint =
        Endpoint::replying_after(Duration::from_millis(20), |_, _| StatusCode::NO_CONTENT).await;
    let admin = Relay::start("intake,admin", &[]).await;
    let token = admin
        .create_source(&run, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    let history = send(&admin, &format!("/in/{TENANT}/{run}/{token}"), 300).await;
    // Created after the history, so live delivery has nothing to send.
    admin.create_endpoint(&run, &run, &endpoint.url()).await;
    let worker = [("RELAY_ENDPOINT_PREFIXES", run.as_str())];
    let first = Relay::start("deliver", &worker).await;
    let started = admin
        .admin(
            reqwest::Method::POST,
            &format!("/endpoints/{run}/replays"),
            json!({}),
        )
        .await;
    let job_id = started["id"].as_str().unwrap().to_string();

    endpoint.wait_for(150).await;
    drop(first);
    let before = endpoint.received().len();
    let _second = Relay::start("deliver", &worker).await;

    let received = within(
        Duration::from_secs(120),
        "every record replayed",
        async || {
            let received = endpoint.received();
            let ids: HashSet<&str> = received.iter().map(|r| r.id()).collect();
            (ids.len() >= history.len()).then_some(received)
        },
    )
    .await;
    let mut seen = HashSet::new();
    let first_seen: Vec<&str> = received
        .iter()
        .map(|r| r.id())
        .filter(|id| seen.insert(*id))
        .collect();
    assert_eq!(first_seen, history, "in intake order");
    let repeats = received.len() - history.len();
    assert!(
        repeats <= 100,
        "{repeats} repeats; at most a checkpoint's worth after {before} before the kill"
    );
    eventually("the job to finish", async || {
        (job(&admin, &job_id).await["status"] == "done").then_some(())
    })
    .await;
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_dead_letter_redriven_after_the_fix_arrives_once() {
    let run = unique("redrive");
    let fixed = Arc::new(AtomicBool::new(false));
    let endpoint = Endpoint::replying({
        let fixed = Arc::clone(&fixed);
        move |_, received| {
            let poison = received.body.starts_with(b"poison") && !fixed.load(Ordering::SeqCst);
            if poison || received.body.starts_with(b"toxic") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::NO_CONTENT
            }
        }
    })
    .await;
    let relay = Relay::start(
        "intake,deliver,admin",
        &[
            ("RELAY_ENDPOINT_PREFIXES", &run),
            ("RELAY_REFUSED_RETRIES", "100ms,100ms"),
        ],
    )
    .await;
    let path = setup(&relay, &run, &endpoint.url()).await;
    for body in ["poison", "fine", "toxic"] {
        assert_eq!(
            relay.send(&path, &[], body.as_bytes()).await.status(),
            StatusCode::ACCEPTED
        );
    }

    let dead = eventually("two dead letters", async || {
        let listed = relay
            .admin(reqwest::Method::GET, "/dead", Value::Null)
            .await;
        let ours: Vec<Value> = listed["relay"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|d| d["endpoint"] == json!(run))
            .cloned()
            .collect();
        (ours.len() == 2).then_some(ours)
    })
    .await;
    // Newest first.
    let (toxic, poison) = (&dead[0], &dead[1]);
    assert!(poison["mark"].is_null());
    assert_eq!(poison["last_status"], json!(400));

    fixed.store(true, Ordering::SeqCst);
    let poison_id = poison["event_id"].as_str().unwrap().to_string();
    let offset = poison["offset"].as_u64().unwrap();
    relay
        .admin(
            reqwest::Method::POST,
            &format!("/dead/{offset}/redrive"),
            Value::Null,
        )
        .await;
    let offset = toxic["offset"].as_u64().unwrap();
    relay
        .admin(
            reqwest::Method::POST,
            &format!("/dead/{offset}/discard"),
            Value::Null,
        )
        .await;

    eventually("the redriven record", async || {
        endpoint
            .received()
            .iter()
            .any(|r| r.id() == poison_id && r.status.is_success())
            .then_some(())
    })
    .await;
    let marks = eventually("both marked", async || {
        let listed = relay
            .admin(reqwest::Method::GET, "/dead", Value::Null)
            .await;
        let marks: Vec<Value> = listed["relay"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|d| d["endpoint"] == json!(run))
            .map(|d| d["mark"]["status"].clone())
            .collect();
        (marks == [json!("discarded"), json!("redriven")]).then_some(marks)
    })
    .await;
    assert_eq!(marks.len(), 2);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let received = endpoint.received();
    let poison: Vec<&Received> = received.iter().filter(|r| r.id() == poison_id).collect();
    assert_eq!(
        poison.iter().filter(|r| r.status.is_success()).count(),
        1,
        "arrives once"
    );
    assert!(poison.last().unwrap().header("webhook-replay").is_some());
    assert_eq!(
        received
            .iter()
            .filter(|r| r.body.starts_with(b"toxic"))
            .count(),
        3
    );
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn felix_dead_letters_are_listed_and_redriven() {
    let run = unique("brokerdead");
    let endpoint = Endpoint::start().await;
    let admin = Relay::start("intake,admin", &[]).await;
    let path = setup(&admin, &run, &endpoint.url()).await;
    let sent = send(&admin, &path, 1).await;

    // A worker that keeps dying on a record: claim it and never acknowledge,
    // until Felix gives up on it after FELIX_GROUP_MAX_ATTEMPTS claims.
    let client = felix().await;
    let (stream, group) = (format!("src.{run}"), format!("ep.{run}"));
    let offset = within(
        Duration::from_secs(90),
        "Felix to dead-letter the record",
        async || {
            let _ = client
                .group_poll("relay", TENANT, &stream, 0, &group, 1)
                .await;
            let dead = client
                .group_dead_letters("relay", TENANT, &stream, 0, &group)
                .await
                .unwrap();
            if dead.is_empty() {
                // The dev broker's claims lapse after 5 s.
                tokio::time::sleep(Duration::from_millis(5200)).await;
            }
            dead.first().copied()
        },
    )
    .await;

    let listed = admin
        .admin(reqwest::Method::GET, "/dead", Value::Null)
        .await;
    assert!(
        listed["broker"]
            .as_array()
            .unwrap()
            .contains(&json!({ "endpoint": run, "source": run, "offset": offset }))
    );
    let _worker = Relay::start("deliver", &[("RELAY_ENDPOINT_PREFIXES", &run)]).await;
    admin
        .admin(
            reqwest::Method::POST,
            &format!("/endpoints/{run}/broker-dead/{offset}/redrive"),
            Value::Null,
        )
        .await;
    let received = endpoint.wait_for(1).await;
    assert_eq!(received[0].id(), sent[0]);
}
