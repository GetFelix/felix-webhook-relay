//! The relay binary against a real Felix broker. Ignored by default because
//! they need the development stack; start it with `dev/up.sh`, export the two
//! variables it prints, and run `cargo test -- --include-ignored`.

mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use common::{Endpoint, Relay, TENANT, WAIT, felix, unique};
use felix_client::StartPosition;
use felix_relay_core::Envelope;
use serde_json::{Value, json};

const WEBHOOKS: usize = 10;

/// A webhook intake accepted: the id and offset from its `202`, and what was sent.
struct Accepted {
    id: String,
    offset: u64,
    body: Vec<u8>,
}

/// A source with a URL token, which needs no signing in the test.
async fn token_source(admin: &Relay, source: &str) -> String {
    let token = admin
        .create_source(
            source,
            json!({
                "scheme": { "type": "token" },
                "event_type_header": "X-Event-Type",
                "keep_headers": ["X-Test-Run"]
            }),
        )
        .await
        .expect("a generated token");
    format!("/in/{TENANT}/{source}/{token}")
}

async fn send_webhooks(intake: &Relay, path: &str, source: &str) -> Vec<Accepted> {
    let mut accepted = Vec::new();
    for n in 0..WEBHOOKS {
        let body = format!("{{\"source\":\"{source}\",\"n\":{n}}}").into_bytes();
        let response = intake
            .send(
                path,
                &[
                    ("content-type", "application/json"),
                    ("x-event-type", "test.ping"),
                    ("x-test-run", source),
                    ("x-not-kept", "dropped"),
                ],
                &body,
            )
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let answer: Value = response.json().await.unwrap();
        let offset = answer["offset"].as_u64().expect("an offset");
        let id = answer["id"].as_str().expect("an id").to_string();
        assert_eq!(id, format!("{source}:{offset}"));
        accepted.push(Accepted { id, offset, body });
    }
    accepted
}

/// Wait until every accepted webhook reached the endpoint, then check each
/// arrived once, in intake order, with its body and content type intact.
async fn assert_delivered_in_order(endpoint: &Endpoint, accepted: &[Accepted]) {
    let received = endpoint.wait_for(accepted.len()).await;
    let ids: Vec<&str> = received.iter().map(|r| r.id()).collect();
    let expected: Vec<&str> = accepted.iter().map(|a| a.id.as_str()).collect();
    assert_eq!(ids, expected, "each webhook once, in intake order");
    for (got, sent) in received.iter().zip(accepted) {
        assert_eq!(got.body, sent.body);
        assert_eq!(got.header("content-type"), Some("application/json"));
    }
    // Long enough for a duplicate sent right after the last one to arrive.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(endpoint.received().len(), accepted.len());
}

/// The record Felix holds at `offset` in `stream`.
async fn record_at(stream: &str, offset: u64) -> Envelope {
    let felix = felix().await;
    let mut subscription = felix
        .subscribe_from("relay", TENANT, stream, Some(StartPosition::Offset(offset)))
        .await
        .unwrap();
    let event = tokio::time::timeout(WAIT, subscription.next_event())
        .await
        .expect("a record at the offset")
        .unwrap()
        .expect("an open subscription");
    assert_eq!(event.offset, Some(offset));
    Envelope::decode(&event.payload).unwrap()
}

fn histogram_count(metrics: &str, name: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name}_count ")))
        .and_then(|count| count.parse().ok())
        .unwrap_or_else(|| panic!("no {name} in /metrics"))
}

async fn metrics(relay: &Relay) -> String {
    reqwest::get(relay.url("/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn one_process_delivers_in_order() {
    let run = unique("one");
    let endpoint = Endpoint::start().await;
    let relay = Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT_PREFIXES", &run)]).await;
    let path = token_source(&relay, &run).await;
    relay.create_endpoint(&run, &run, &endpoint.url()).await;

    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let accepted = send_webhooks(&relay, &path, &run).await;
    assert_delivered_in_order(&endpoint, &accepted).await;

    let stored = record_at(&format!("src.{run}"), accepted[0].offset).await;
    assert_eq!(stored.id, None);
    assert!(stored.received_at >= before);
    assert_eq!(stored.event_type.as_deref(), Some("test.ping"));
    assert_eq!(stored.content_type.as_deref(), Some("application/json"));
    assert_eq!(stored.headers, [("x-test-run".to_string(), run.clone())]);
    assert_eq!(stored.body, accepted[0].body);

    let metrics = metrics(&relay).await;
    assert_eq!(
        histogram_count(&metrics, "relay_intake_ack_seconds"),
        WEBHOOKS as u64
    );
    assert!(histogram_count(&metrics, "relay_poll_wakeup_seconds") >= WEBHOOKS as u64);
    assert!(histogram_count(&metrics, "relay_outbound_request_seconds") >= WEBHOOKS as u64);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn split_intake_and_delivery_deliver_in_order() {
    let run = unique("split");
    let endpoint = Endpoint::start().await;
    let admin = Relay::start("admin", &[]).await;
    let path = token_source(&admin, &run).await;
    admin.create_endpoint(&run, &run, &endpoint.url()).await;
    let intake = Relay::start("intake", &[]).await;
    let deliver = Relay::start("deliver", &[("RELAY_ENDPOINT_PREFIXES", &run)]).await;

    let accepted = send_webhooks(&intake, &path, &run).await;
    assert_delivered_in_order(&endpoint, &accepted).await;

    let intake_metrics = metrics(&intake).await;
    assert_eq!(
        histogram_count(&intake_metrics, "relay_intake_ack_seconds"),
        WEBHOOKS as u64
    );
    assert_eq!(
        histogram_count(&intake_metrics, "relay_outbound_request_seconds"),
        0
    );
    let deliver_metrics = metrics(&deliver).await;
    assert_eq!(
        histogram_count(&deliver_metrics, "relay_intake_ack_seconds"),
        0
    );
    assert!(histogram_count(&deliver_metrics, "relay_outbound_request_seconds") >= WEBHOOKS as u64);
    let status = deliver
        .http
        .post(deliver.url(&path))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, StatusCode::NOT_FOUND);
    let status = intake
        .http
        .get(intake.url(&format!("/api/{TENANT}/sources/{run}")))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn intake_refuses_oversized_bodies_and_unknown_sources() {
    let run = unique("limits");
    let intake = Relay::start("intake,admin", &[]).await;
    let path = token_source(&intake, &run).await;

    let too_big = intake.send(&path, &[], &vec![b'x'; 1024 * 1024 + 1]).await;
    assert_eq!(too_big.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let at_limit = intake.send(&path, &[], &vec![b'x'; 1024 * 1024]).await;
    assert_eq!(at_limit.status(), StatusCode::ACCEPTED);

    let token = path.rsplit('/').next().unwrap();
    for path in [
        format!("/in/{TENANT}/other/{token}"),
        format!("/in/other/{run}/{token}"),
    ] {
        let status = intake
            .http
            .post(intake.url(&path))
            .body("{}")
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn config_changes_reach_a_running_relay() {
    let run = unique("watch");
    let relay = Relay::start("intake,admin", &[]).await;
    let path = token_source(&relay, &run).await;
    assert_eq!(
        relay.send(&path, &[], b"{}").await.status(),
        StatusCode::ACCEPTED
    );

    relay
        .admin(
            reqwest::Method::DELETE,
            &format!("/sources/{run}"),
            Value::Null,
        )
        .await;
    common::eventually("the deleted source to be refused", async || {
        let status = relay
            .http
            .post(relay.url(&path))
            .body("{}")
            .send()
            .await
            .unwrap()
            .status();
        (status == StatusCode::NOT_FOUND).then_some(())
    })
    .await;
}
