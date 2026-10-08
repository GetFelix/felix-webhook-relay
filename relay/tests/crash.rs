//! Demonstration 5: under load, kill -9 intake, a delivery worker, and each
//! broker of a three-broker cluster in turn, so whichever owns the source is
//! killed. Nothing that got a `2xx` from intake may be lost, and repeats must
//! stay within the bound in the design's "Delivery semantics".
//!
//! Needs the three-broker stack, `dev/up.sh --cluster`, and
//! `RELAY_TEST_CLUSTER=1`; without that it does nothing.

mod common;

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Endpoint, Relay, TENANT, unique, within};
use serde_json::{Value, json};
use standardwebhooks::Webhook;

const BROKERS: &str = "127.0.0.1:5000,127.0.0.1:5010,127.0.0.1:5020";
const SENDERS: usize = 4;
const SECRET: &str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";

fn free_port() -> String {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    addr.to_string()
}

/// The engine `dev/up.sh` picks: `CONTAINER_ENGINE`, else Docker when its
/// daemon answers, else Podman.
fn container_engine() -> String {
    if let Ok(engine) = std::env::var("CONTAINER_ENGINE") {
        return engine;
    }
    let docker_up = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if docker_up { "docker" } else { "podman" }.to_string()
}

fn container(engine: &str, args: &[&str]) {
    let status = Command::new(engine).args(args).status().unwrap();
    assert!(status.success(), "{engine} {args:?}");
}

/// A relay role on a fixed port, so a replacement takes over its address.
struct Role {
    roles: &'static str,
    env: Vec<(String, String)>,
}

impl Role {
    async fn start(&self) -> Relay {
        let env: Vec<(&str, &str)> = self
            .env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        Relay::start(self.roles, &env).await
    }
}

#[tokio::test]
#[ignore = "needs the three-broker stack"]
async fn nothing_acknowledged_is_lost_when_anything_is_killed() {
    if std::env::var("RELAY_TEST_CLUSTER").as_deref() != Ok("1") {
        eprintln!("skipped: start dev/up.sh --cluster and set RELAY_TEST_CLUSTER=1");
        return;
    }
    let run = unique("crash");
    let shared = [
        ("RELAY_FELIX_BROKERS", BROKERS.to_string()),
        ("RELAY_STREAM_REPLICAS", "3".to_string()),
        ("RELAY_ENDPOINT_PREFIXES", run.clone()),
        ("RELAY_CLAIM_WAIT_MS", "5000".to_string()),
        ("RUST_LOG", "warn".to_string()),
    ];
    let with = |port: String| -> Vec<(String, String)> {
        let mut env: Vec<(String, String)> = shared
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        env.push(("RELAY_LISTEN".to_string(), port));
        env
    };
    let intake = Role {
        roles: "intake",
        env: with(free_port()),
    };
    let deliver = Role {
        roles: "deliver",
        env: with(free_port()),
    };
    let admin = Role {
        roles: "admin",
        env: with(free_port()),
    }
    .start()
    .await;

    let endpoint = Endpoint::start().await;
    admin
        .create_source(
            &run,
            json!({
                "scheme": { "type": "standard-webhooks" },
                "secret": SECRET,
                "event_id": { "header": "webhook-id" }
            }),
        )
        .await;
    admin.create_endpoint(&run, &run, &endpoint.url()).await;

    let intake_url = format!("http://{}/in/{TENANT}/{run}", intake.env.last().unwrap().1);
    let mut intake_relay = Some(intake.start().await);
    let mut deliver_relay = Some(deliver.start().await);

    // Each sender retries a webhook until intake answers 2xx, the way real
    // senders do, and records which ids were acknowledged and at what offset.
    let accepted: Arc<Mutex<HashMap<String, u64>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let mut senders = Vec::new();
    for sender in 0..SENDERS {
        let (accepted, stop, url) = (Arc::clone(&accepted), Arc::clone(&stop), intake_url.clone());
        senders.push(tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap();
            let signer = Webhook::new(SECRET).unwrap();
            let mut n = 0;
            while !stop.load(Ordering::SeqCst) {
                let id = format!("msg_{sender}_{n}");
                let body = format!("{{\"sender\":{sender},\"n\":{n}}}");
                loop {
                    let now = common::now_secs() as i64;
                    let signature = signer.sign(&id, now, body.as_bytes()).unwrap();
                    let sent = http
                        .post(&url)
                        .header("webhook-id", &id)
                        .header("webhook-timestamp", now.to_string())
                        .header("webhook-signature", signature)
                        .body(body.clone())
                        .send()
                        .await;
                    if let Ok(response) = sent
                        && response.status().is_success()
                    {
                        let answer: Value = response.json().await.unwrap();
                        let offset = answer["offset"].as_u64().unwrap();
                        accepted.lock().unwrap().insert(id.clone(), offset);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                n += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    let pause = |secs| tokio::time::sleep(Duration::from_secs(secs));
    pause(5).await;
    eprintln!("kill -9 intake");
    drop(intake_relay.take());
    intake_relay = Some(intake.start().await);
    pause(5).await;
    eprintln!("kill -9 the delivery worker");
    drop(deliver_relay.take());
    deliver_relay = Some(deliver.start().await);
    let engine = container_engine();
    for broker in ["broker-1", "broker-2-1", "broker-3-1"] {
        pause(8).await;
        let name = format!("felix-webhook-relay-{broker}");
        eprintln!("kill -9 {name}");
        container(&engine, &["kill", "--signal", "KILL", &name]);
        pause(8).await;
        container(&engine, &["start", &name]);
    }
    pause(10).await;
    stop.store(true, Ordering::SeqCst);
    for sender in senders {
        sender.await.unwrap();
    }

    let accepted = accepted.lock().unwrap().clone();
    let received = within(
        Duration::from_secs(300),
        "every acknowledged webhook",
        async || {
            let received = endpoint.received();
            let ids: HashSet<&str> = received.iter().map(|r| r.id()).collect();
            accepted
                .keys()
                .all(|id| ids.contains(id.as_str()))
                .then_some(received)
        },
    )
    .await;
    drop((intake_relay, deliver_relay));

    let mut copies: HashMap<&str, usize> = HashMap::new();
    for request in &received {
        *copies.entry(request.id()).or_default() += 1;
    }
    let repeats: usize = copies.values().map(|n| n - 1).sum();
    // Order: the first time each id arrives follows the offsets intake gave.
    let mut seen = HashSet::new();
    let first: Vec<u64> = received
        .iter()
        .filter(|r| seen.insert(r.id()))
        .filter_map(|r| accepted.get(r.id()).copied())
        .collect();
    let out_of_order = first.windows(2).filter(|w| w[0] > w[1]).count();
    eprintln!(
        "{} acknowledged, all received; {repeats} repeats; {out_of_order} out of order",
        accepted.len()
    );
    // The design's bound: an intake killed mid-request can store a copy of
    // each webhook it held (one per sender), a worker killed after a request
    // and before its ack repeats one record, and each broker failover repeats
    // at most the endpoint's window, one for an ordered endpoint.
    let bound = SENDERS + 1 + 3;
    assert!(repeats <= bound, "{repeats} repeats, more than {bound}");
    assert!(accepted.len() > 1000, "too little load: {}", accepted.len());
}
