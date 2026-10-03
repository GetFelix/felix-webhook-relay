//! Signatures both ways, stable event ids, idempotency keys and sealed
//! secrets, against a real Felix broker. Needs `dev/up.sh`; see `delivery.rs`.

mod common;

use std::process::{Command, Stdio};
use std::time::Instant;

use axum::http::StatusCode;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use common::{Endpoint, Relay, TENANT, WAIT, eventually, felix, now_secs, tail, unique};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use standardwebhooks::Webhook;

fn hmac_sha256(secret: &str, parts: &[&[u8]]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().to_vec()
}

/// One scheme's way of signing a request: headers for a body at a time.
type Signer = fn(&str, &[u8], u64) -> Vec<(String, String)>;

fn standard(secret: &str, body: &[u8], at: u64) -> Vec<(String, String)> {
    let id = format!("msg_{}", unique("sw"));
    let signature = Webhook::new(secret)
        .unwrap()
        .sign(&id, at as i64, body)
        .unwrap();
    vec![
        ("webhook-id".into(), id),
        ("webhook-timestamp".into(), at.to_string()),
        ("webhook-signature".into(), signature),
    ]
}

fn github(secret: &str, body: &[u8], _: u64) -> Vec<(String, String)> {
    let digest = hex::encode(hmac_sha256(secret, &[body]));
    vec![("x-hub-signature-256".into(), format!("sha256={digest}"))]
}

fn stripe(secret: &str, body: &[u8], at: u64) -> Vec<(String, String)> {
    let at = at.to_string();
    let digest = hex::encode(hmac_sha256(secret, &[at.as_bytes(), b".", body]));
    vec![("stripe-signature".into(), format!("t={at},v1={digest}"))]
}

fn generic(secret: &str, body: &[u8], _: u64) -> Vec<(String, String)> {
    let digest = BASE64.encode(hmac_sha256(secret, &[body]));
    vec![("x-acme-signature".into(), digest)]
}

async fn post(
    relay: &Relay,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> (StatusCode, Value) {
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    let response = relay.send(path, &headers, body).await;
    let status = response.status();
    (status, response.json().await.unwrap_or(Value::Null))
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn every_scheme_refuses_bad_and_stale_signatures_before_the_log() {
    let relay = Relay::start("intake,admin", &[]).await;
    let cases: [(&str, Value, &str, Signer, bool); 4] = [
        (
            "sw",
            json!({ "type": "standard-webhooks" }),
            "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
            standard,
            true,
        ),
        (
            "github",
            json!({ "type": "github" }),
            "gh secret",
            github,
            false,
        ),
        (
            "stripe",
            json!({ "type": "stripe" }),
            "whsec_stripe",
            stripe,
            true,
        ),
        (
            "hmac",
            json!({ "type": "hmac", "header": "X-Acme-Signature", "encoding": "base64" }),
            "acme secret",
            generic,
            false,
        ),
    ];
    for (name, scheme, secret, sign, timed) in cases {
        let source = unique(name);
        relay
            .create_source(&source, json!({ "scheme": scheme, "secret": secret }))
            .await;
        let path = format!("/in/{TENANT}/{source}");
        let body = br#"{"n":1}"#;

        let (status, first) = post(&relay, &path, &sign(secret, body, now_secs()), body).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{name}: a good signature");
        let first = first["offset"].as_u64().unwrap();

        let mut refused = vec![
            // Another secret, in a form every scheme accepts.
            (
                "bad signature",
                sign("whsec_YW5vdGhlciBzZWNyZXQ=", body, now_secs()),
                body.as_slice(),
            ),
            (
                "changed body",
                sign(secret, body, now_secs()),
                br#"{"n":2}"#.as_slice(),
            ),
            ("no signature", Vec::new(), body.as_slice()),
        ];
        if timed {
            refused.push(("stale", sign(secret, body, now_secs() - 301), body));
            refused.push(("future", sign(secret, body, now_secs() + 301), body));
        }
        for (what, headers, body) in refused {
            let (status, _) = post(&relay, &path, &headers, body).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{name}: {what}");
        }

        let (status, second) = post(&relay, &path, &sign(secret, body, now_secs()), body).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            second["offset"].as_u64(),
            Some(first + 1),
            "{name}: nothing refused reached the log"
        );
    }

    let source = unique("token");
    let token = relay
        .create_source(&source, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    assert!(token.len() >= 64, "a long random token");
    let good = relay
        .send(&format!("/in/{TENANT}/{source}/{token}"), &[], b"{}")
        .await;
    assert_eq!(good.status(), StatusCode::ACCEPTED);
    for path in [
        format!("/in/{TENANT}/{source}/wrong"),
        format!("/in/{TENANT}/{source}"),
    ] {
        let status = relay
            .http
            .post(relay.url(&path))
            .body("{}")
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}");
    }
    assert_eq!(tail(&format!("src.{source}")).await, 1);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn deliveries_verify_with_the_standard_webhooks_library() {
    let run = unique("signed");
    let endpoint = Endpoint::start().await;
    let relay = Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT", &run)]).await;
    let token = relay
        .create_source(&run, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    let path = format!("/in/{TENANT}/{run}/{token}");
    let first_secret = relay.create_endpoint(&run, &run, &endpoint.url()).await;
    assert!(first_secret.starts_with("whsec_"));

    for n in 0..3 {
        let body = format!("{{\"before\":{n}}}");
        assert_eq!(
            relay.send(&path, &[], body.as_bytes()).await.status(),
            StatusCode::ACCEPTED
        );
    }
    let first = Webhook::new(&first_secret).unwrap();
    for received in endpoint.wait_for(3).await {
        first.verify(&received.body, &received.headers).unwrap();
        assert!(
            first
                .verify(b"{\"tampered\":1}", &received.headers)
                .is_err()
        );
    }

    let rotated = relay
        .admin(
            reqwest::Method::POST,
            &format!("/endpoints/{run}/secret"),
            json!({}),
        )
        .await;
    let second_secret = rotated["secret"].as_str().unwrap();
    let second = Webhook::new(second_secret).unwrap();
    // Keep sending until a delivery carries both signatures, which is when
    // this relay's config watch has seen the rotation.
    let rotated_at = eventually("a delivery signed with both secrets", async || {
        let sent = endpoint.received().len();
        relay.send(&path, &[], b"{\"after\":1}").await;
        let received = endpoint.wait_for(sent + 1).await;
        let last = received.last().unwrap();
        (last.header("webhook-signature")?.split(' ').count() == 2).then_some(sent)
    })
    .await;
    let received = endpoint.received();
    for request in &received {
        first.verify(&request.body, &request.headers).unwrap();
    }
    for request in &received[rotated_at..] {
        second.verify(&request.body, &request.headers).unwrap();
    }
    assert!(
        second
            .verify(&received[0].body, &received[0].headers)
            .is_err()
    );

    let shown = relay
        .admin(
            reqwest::Method::GET,
            &format!("/endpoints/{run}"),
            Value::Null,
        )
        .await;
    assert!(shown.get("secret").is_none());
    assert!(shown["rotating_until"].as_u64().unwrap() > now_secs() * 1000);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_redelivered_record_keeps_its_first_id() {
    let run = unique("ids");
    let endpoint = Endpoint::replying(|before, _| match before {
        0 => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::NO_CONTENT,
    })
    .await;
    let relay = Relay::start("intake,deliver,admin", &[("RELAY_ENDPOINT", &run)]).await;
    let secret = "gh secret";
    relay
        .create_source(
            &run,
            json!({
                "scheme": { "type": "github" },
                "secret": secret,
                "event_id": { "header": "x-github-delivery" }
            }),
        )
        .await;
    relay.create_endpoint(&run, &run, &endpoint.url()).await;

    let delivery = format!("72d3162e-{run}");
    let body = br#"{"action":"opened"}"#;
    let mut headers = github(secret, body, 0);
    headers.push(("x-github-delivery".into(), delivery.clone()));
    let (status, answer) = post(&relay, &format!("/in/{TENANT}/{run}"), &headers, body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(answer["id"], json!(delivery));

    let received = endpoint.wait_for(2).await;
    assert_eq!(received[0].id(), delivery, "the first attempt");
    assert_eq!(received[1].id(), delivery, "the retry");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_retried_webhook_is_stored_once() {
    let source = unique("idem");
    let relay = Relay::start("intake,admin", &[]).await;
    let secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    relay
        .create_source(
            &source,
            json!({
                "scheme": { "type": "standard-webhooks" },
                "secret": secret,
                "event_id": { "header": "webhook-id" }
            }),
        )
        .await;
    let path = format!("/in/{TENANT}/{source}");
    let body = br#"{"type":"invoice.paid"}"#;
    let headers = standard(secret, body, now_secs());
    let id = headers[0].1.clone();

    let (status, first) = post(&relay, &path, &headers, body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(first["id"], json!(id));
    // The sender timed out waiting and sends the same webhook again.
    let (status, again) = post(&relay, &path, &headers, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again, first);

    let (status, other) = post(&relay, &path, &standard(secret, body, now_secs()), body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let first = first["offset"].as_u64().unwrap();
    assert_eq!(other["offset"].as_u64(), Some(first + 1));
    assert_eq!(tail(&format!("src.{source}")).await, first + 2);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn secrets_are_sealed_in_felix() {
    let run = unique("sealed");
    let relay = Relay::start("admin", &[]).await;
    let source_secret = format!("github-secret-{run}");
    relay
        .create_source(
            &run,
            json!({ "scheme": { "type": "github" }, "secret": source_secret }),
        )
        .await;
    let endpoint_secret = "whsec_c2VhbGVkIGVuZHBvaW50IHNlY3JldCBmb3IgdGVzdHM=";
    relay
        .admin(
            reqwest::Method::PUT,
            &format!("/endpoints/{run}"),
            json!({ "source": run, "url": "http://127.0.0.1:9/hook", "secret": endpoint_secret }),
        )
        .await;

    let client = felix().await.client().await;
    for (key, secret, raw) in [
        (
            format!("source/{run}"),
            source_secret.as_str(),
            source_secret.as_bytes().to_vec(),
        ),
        (
            format!("endpoint/{run}"),
            endpoint_secret,
            BASE64.decode(&endpoint_secret[6..]).unwrap(),
        ),
    ] {
        let stored = client
            .cache_get("relay", TENANT, "config", &key)
            .await
            .unwrap()
            .expect("the entry");
        let text = String::from_utf8_lossy(&stored);
        assert!(!text.contains(secret), "{key} holds its secret: {text}");
        assert!(!stored.windows(raw.len()).any(|w| w == raw.as_slice()));
        let shown = relay
            .admin(
                reqwest::Method::GET,
                &format!("/{}s/{run}", key.split('/').next().unwrap()),
                Value::Null,
            )
            .await;
        assert!(!shown.to_string().contains(secret));
    }
}

#[tokio::test]
async fn the_relay_refuses_to_start_without_its_key() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_felix-relay"))
        .env_remove("RELAY_SECRET_KEY")
        .env("RELAY_FELIX_TOKEN_FILE", "/nonexistent")
        .env("RELAY_LISTEN", "127.0.0.1:0")
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(started.elapsed() < WAIT, "the relay kept running");
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    assert!(!status.success());
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr).unwrap();
    assert!(stderr.contains("RELAY_SECRET_KEY"), "{stderr}");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_url_token_never_reaches_the_log() {
    let source = unique("redact");
    let log_path = std::env::temp_dir().join(format!("{source}.log"));
    let log = std::fs::File::create(&log_path).unwrap();
    let relay =
        Relay::start_logging("intake,admin", &[("RUST_LOG", "trace")], Stdio::from(log)).await;
    let token = relay
        .create_source(&source, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    let good = relay
        .send(&format!("/in/{TENANT}/{source}/{token}"), &[], b"{}")
        .await;
    assert_eq!(good.status(), StatusCode::ACCEPTED);
    let wrong = format!("{}x", &token[..token.len() - 1]);
    let path = format!("/in/{TENANT}/{source}/{wrong}");
    let refused = relay
        .http
        .post(relay.url(&path))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::UNAUTHORIZED);
    let metrics = reqwest::get(relay.url("/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    drop(relay);

    let log = std::fs::read_to_string(&log_path).unwrap();
    assert!(log.contains(&source), "the log was captured");
    for secret in [&token, &wrong] {
        assert!(!log.contains(secret.as_str()), "a token reached the log");
        assert!(
            !metrics.contains(secret.as_str()),
            "a token reached /metrics"
        );
    }
    let _ = std::fs::remove_file(log_path);
}
