//! The relay binary against a real Felix broker. Ignored by default because
//! they need the development stack; start it with `dev/up.sh`, export the two
//! variables it prints, and run `cargo test -- --include-ignored`.

use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use felix_client::{ClientConfig, ClusterClient, StartPosition};
use felix_relay_core::Envelope;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use serde_json::Value;

const WAIT: Duration = Duration::from_secs(60);
const WEBHOOKS: usize = 10;

/// A request the test endpoint received.
#[derive(Clone)]
struct Received {
    id: String,
    content_type: Option<String>,
    body: Bytes,
}

type Inbox = Arc<Mutex<Vec<Received>>>;

/// An HTTP endpoint that records every request and answers `204`.
async fn start_endpoint() -> (SocketAddr, Inbox) {
    async fn receive(State(inbox): State<Inbox>, headers: HeaderMap, body: Bytes) -> StatusCode {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };
        inbox.lock().unwrap().push(Received {
            id: header("webhook-id").unwrap_or_default(),
            content_type: header("content-type"),
            body,
        });
        StatusCode::NO_CONTENT
    }
    let inbox = Inbox::default();
    let router = axum::Router::new()
        .route("/hook", post(receive))
        .with_state(Arc::clone(&inbox));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });
    (addr, inbox)
}

/// A `felix-relay` process, killed when dropped.
struct Relay {
    child: Child,
    addr: SocketAddr,
}

impl Relay {
    async fn start(roles: &str, endpoint: &str, endpoint_url: &str) -> Self {
        let addr = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_felix-relay"))
            .env("RELAY_LISTEN", addr.to_string())
            .env("RELAY_ROLES", roles)
            .env("RELAY_ENDPOINT", endpoint)
            .env("RELAY_ENDPOINT_URL", endpoint_url)
            .env("RELAY_EVENT_TYPE_HEADER", "x-event-type")
            .env("RELAY_KEEP_HEADERS", "x-test-run")
            .spawn()
            .expect("start felix-relay");
        let mut relay = Self { child, addr };
        let started = Instant::now();
        while reqwest::get(relay.url("/healthz")).await.is_err() {
            if let Some(status) = relay.child.try_wait().unwrap() {
                panic!("felix-relay exited with {status}");
            }
            assert!(started.elapsed() < WAIT, "felix-relay did not start");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        relay
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A tag unique to one test run. The source stream outlives a test, so the
/// endpoint's new group also sees what earlier tests appended.
fn run_tag(test: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{test}-{nanos}")
}

/// A webhook intake accepted: the id and offset from its `202`, and what was sent.
struct Accepted {
    id: String,
    offset: u64,
    body: Vec<u8>,
}

async fn send_webhooks(intake: &Relay, run: &str) -> Vec<Accepted> {
    let http = reqwest::Client::new();
    let mut accepted = Vec::new();
    for n in 0..WEBHOOKS {
        let body = format!("{{\"run\":\"{run}\",\"n\":{n}}}").into_bytes();
        let response = http
            .post(intake.url("/in/acme/demo"))
            .header("content-type", "application/json")
            .header("x-event-type", "test.ping")
            .header("x-test-run", run)
            .header("x-not-kept", "dropped")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let answer: Value = response.json().await.unwrap();
        let offset = answer["offset"].as_u64().expect("an offset");
        let id = answer["id"].as_str().expect("an id").to_string();
        assert_eq!(id, format!("demo:{offset}"));
        accepted.push(Accepted { id, offset, body });
    }
    accepted
}

/// Wait until every accepted webhook reached the endpoint, then check each
/// arrived once, in intake order, with its body and content type intact.
async fn assert_delivered_in_order(inbox: &Inbox, accepted: &[Accepted]) {
    let started = Instant::now();
    let received = loop {
        let received = inbox.lock().unwrap().clone();
        let ours: Vec<Received> = received
            .into_iter()
            .filter(|r| accepted.iter().any(|a| a.id == r.id))
            .collect();
        if ours.len() >= accepted.len() {
            break ours;
        }
        assert!(
            started.elapsed() < WAIT,
            "only {} of {} webhooks arrived",
            ours.len(),
            accepted.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let ids: Vec<&str> = received.iter().map(|r| r.id.as_str()).collect();
    let expected: Vec<&str> = accepted.iter().map(|a| a.id.as_str()).collect();
    assert_eq!(ids, expected, "each webhook once, in intake order");
    for (got, sent) in received.iter().zip(accepted) {
        assert_eq!(got.body, sent.body);
        assert_eq!(got.content_type.as_deref(), Some("application/json"));
    }
    // Long enough for a duplicate sent right after the last one to arrive.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let all = inbox.lock().unwrap().clone();
    for sent in accepted {
        let copies = all.iter().filter(|r| r.id == sent.id).count();
        assert_eq!(copies, 1, "{} arrived {copies} times", sent.id);
    }
}

async fn felix() -> ClusterClient {
    let ca = std::env::var("RELAY_FELIX_CA_FILE").expect("RELAY_FELIX_CA_FILE");
    let token = std::env::var("RELAY_FELIX_TOKEN_FILE").expect("RELAY_FELIX_TOKEN_FILE");
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&ca).unwrap() {
        roots.add(cert.unwrap()).unwrap();
    }
    let quic = felix_client::quic_client_config(Some(Arc::new(roots)), true).unwrap();
    let mut config = ClientConfig::optimized_defaults(quic);
    config.auth_tenant_id = Some("relay".to_string());
    config.auth_token = Some(std::fs::read_to_string(token).unwrap().trim().to_string());
    let brokers = ["127.0.0.1:5000".parse().unwrap()];
    ClusterClient::connect(&brokers, "localhost", config)
        .await
        .unwrap()
}

/// The record Felix holds at `offset` in the demo source.
async fn record_at(offset: u64) -> Envelope {
    let felix = Arc::new(felix().await);
    let mut subscription = felix
        .subscribe_from(
            "relay",
            "acme",
            "src.demo",
            Some(StartPosition::Offset(offset)),
        )
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

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn one_process_delivers_in_order() {
    let run = run_tag("one");
    let (endpoint, inbox) = start_endpoint().await;
    let relay = Relay::start(
        "intake,deliver,admin",
        &run,
        &format!("http://{endpoint}/hook"),
    )
    .await;

    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let accepted = send_webhooks(&relay, &run).await;
    assert_delivered_in_order(&inbox, &accepted).await;

    let stored = record_at(accepted[0].offset).await;
    assert_eq!(stored.id, None);
    assert!(stored.received_at >= before);
    assert_eq!(stored.event_type.as_deref(), Some("test.ping"));
    assert_eq!(stored.content_type.as_deref(), Some("application/json"));
    assert_eq!(stored.headers, [("x-test-run".to_string(), run.clone())]);
    assert_eq!(stored.body, accepted[0].body);

    let metrics = reqwest::get(relay.url("/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
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
    let run = run_tag("split");
    let (endpoint, inbox) = start_endpoint().await;
    let url = format!("http://{endpoint}/hook");
    let intake = Relay::start("intake", &run, &url).await;
    let deliver = Relay::start("deliver", &run, &url).await;

    let accepted = send_webhooks(&intake, &run).await;
    assert_delivered_in_order(&inbox, &accepted).await;

    let intake_metrics = reqwest::get(intake.url("/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(
        histogram_count(&intake_metrics, "relay_intake_ack_seconds"),
        WEBHOOKS as u64
    );
    assert_eq!(
        histogram_count(&intake_metrics, "relay_outbound_request_seconds"),
        0
    );
    let deliver_metrics = reqwest::get(deliver.url("/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(
        histogram_count(&deliver_metrics, "relay_intake_ack_seconds"),
        0
    );
    assert!(histogram_count(&deliver_metrics, "relay_outbound_request_seconds") >= WEBHOOKS as u64);
    assert_eq!(
        reqwest::get(deliver.url("/in/acme/demo"))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn intake_refuses_oversized_bodies_and_unknown_sources() {
    let run = run_tag("limits");
    let intake = Relay::start("intake", &run, "http://127.0.0.1:9/unused").await;
    let http = reqwest::Client::new();

    let too_big = http
        .post(intake.url("/in/acme/demo"))
        .body(vec![b'x'; 1024 * 1024 + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(too_big.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let at_limit = http
        .post(intake.url("/in/acme/demo"))
        .body(vec![b'x'; 1024 * 1024])
        .send()
        .await
        .unwrap();
    assert_eq!(at_limit.status(), StatusCode::ACCEPTED);

    for path in ["/in/acme/other", "/in/other/demo"] {
        let status = http
            .post(intake.url(path))
            .body("{}")
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
}
