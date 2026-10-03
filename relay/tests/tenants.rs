//! Tenants as namespaces, narrowed tokens, the admin API, page and sign-in,
//! against a real Felix broker, control plane and Dex. Needs `dev/up.sh`;
//! see `delivery.rs`.

mod common;

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use common::{ALICE, DEX, Endpoint, Relay, SECRET_KEY, TENANT, eventually, id_token, unique};
use felix_client::{ClientConfig, ClusterClient, StartPosition};
use reqwest::Method;
use reqwest::header::{COOKIE, LOCATION, SET_COOKIE};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use serde_json::{Value, json};

const BOB: &str = "bob@example.com";
const CAROL: &str = "carol@example.com";
const GLOBEX: &str = "globex";

fn state_file(name: &str) -> String {
    let ca = std::env::var("RELAY_FELIX_CA_FILE").unwrap();
    std::path::Path::new(&ca)
        .with_file_name(name)
        .display()
        .to_string()
}

/// A client on the dev broker that connects with `token`.
async fn felix_with(token: &str) -> Arc<ClusterClient> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(state_file("broker-cert.pem")).unwrap() {
        roots.add(cert.unwrap()).unwrap();
    }
    let quic = felix_client::quic_client_config(Some(Arc::new(roots)), true).unwrap();
    let mut config = ClientConfig::optimized_defaults(quic);
    config.auth_tenant_id = Some("relay".to_string());
    config.auth_token = Some(token.to_string());
    let brokers = ["127.0.0.1:5000".parse().unwrap()];
    Arc::new(
        ClusterClient::connect(&brokers, "localhost", config)
            .await
            .unwrap(),
    )
}

/// Everything a relay process does with Felix, tried on one tenant's
/// objects. Each entry is the call and whether it worked.
async fn attempt_everything(
    client: &Arc<ClusterClient>,
    tenant: &str,
) -> Vec<(&'static str, bool)> {
    let cache = client.client().await;
    vec![
        (
            "publish",
            client
                .publish(
                    "relay",
                    tenant,
                    "attempts",
                    b"probe".to_vec(),
                    felix_wire::AckMode::PerMessage,
                )
                .await
                .is_ok(),
        ),
        (
            "subscribe",
            client
                .subscribe_from("relay", tenant, "dead", Some(StartPosition::Latest))
                .await
                .is_ok(),
        ),
        (
            "poll",
            client
                .group_poll("relay", tenant, "dead", 0, "ep.probe", 1)
                .await
                .is_ok(),
        ),
        (
            "cache read",
            cache
                .cache_get("relay", tenant, "config", "probe")
                .await
                .is_ok(),
        ),
        (
            "cache write",
            cache
                .cache_put(
                    "relay",
                    tenant,
                    "state",
                    "probe",
                    b"x".to_vec().into(),
                    Some(1000),
                )
                .await
                .is_ok(),
        ),
        (
            "counter",
            cache
                .counter_add("relay", tenant, "stats", "probe", 1)
                .await
                .is_ok(),
        ),
    ]
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_relay_token_is_refused_on_every_other_tenant() {
    // The token the relay itself would connect to acme with.
    let output = Command::new(env!("CARGO_BIN_EXE_felix-relay"))
        .args(["token", TENANT])
        .env("RELAY_ROLES", "intake,deliver")
        .env("RELAY_SECRET_KEY", SECRET_KEY)
        .env("RELAY_IDP_TOKEN_FILE", state_file("relay-idp.token"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let token = String::from_utf8(output.stdout).unwrap();
    let client = felix_with(token.trim()).await;

    for (call, worked) in attempt_everything(&client, TENANT).await {
        assert!(worked, "{call} on its own tenant");
    }
    for (call, worked) in attempt_everything(&client, GLOBEX).await {
        assert!(!worked, "{call} on another tenant was allowed");
    }
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn one_process_serves_two_tenants() {
    let run = unique("tenants");
    let relay = Relay::start(
        "intake,deliver,admin",
        &[
            ("RELAY_TENANTS", "acme,globex"),
            ("RELAY_ENDPOINT_PREFIXES", &run),
        ],
    )
    .await;
    let mut targets = Vec::new();
    for (who, tenant) in [(ALICE, TENANT), (BOB, GLOBEX)] {
        let target = Endpoint::start().await;
        let source = relay
            .admin_as(
                who,
                tenant,
                Method::PUT,
                &format!("/sources/{run}"),
                json!({ "scheme": { "type": "token" } }),
            )
            .await;
        let token = source["secret"].as_str().unwrap().to_string();
        relay
            .admin_as(
                who,
                tenant,
                Method::PUT,
                &format!("/endpoints/{run}"),
                json!({ "source": run, "url": target.url() }),
            )
            .await;
        let path = format!("/in/{tenant}/{run}/{token}");
        let response = relay.send(&path, &[], tenant.as_bytes()).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        targets.push((tenant, target));
    }
    for (tenant, target) in targets {
        let received = target.wait_for(1).await;
        assert_eq!(
            received[0].body,
            tenant.as_bytes(),
            "{tenant}'s own webhook"
        );
    }

    // Each admin is refused on the other's tenant, by the control plane.
    for (who, tenant) in [(ALICE, GLOBEX), (BOB, TENANT), (CAROL, TENANT)] {
        let response = relay
            .call_as(
                who,
                tenant,
                Method::GET,
                &format!("/sources/{run}"),
                Value::Null,
            )
            .await;
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{who} on {tenant}"
        );
        let body = response.text().await.unwrap();
        assert!(body.contains("control plane refused"), "{body}");
    }
    let anonymous = relay
        .http
        .get(relay.url(&format!("/api/{TENANT}/sources/{run}")))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn every_admin_route_works_and_the_page_reaches_it() {
    let run = unique("routes");
    let target = Endpoint::replying({
        let calls = std::sync::atomic::AtomicUsize::new(0);
        move |_, _| match calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            0 => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::NO_CONTENT,
        }
    })
    .await;
    // A minute's backoff, which only "retry now" can cut short in time.
    let relay = Relay::start(
        "intake,deliver,admin",
        &[("RELAY_ENDPOINT_PREFIXES", &run), ("RELAY_BACKOFF", "60s")],
    )
    .await;
    let token = relay
        .create_source(&run, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    let shown = relay
        .admin(Method::GET, &format!("/sources/{run}"), Value::Null)
        .await;
    assert_eq!(shown["scheme"]["type"], "token");
    relay.create_endpoint(&run, &run, &target.url()).await;
    let sent = relay
        .send(&format!("/in/{TENANT}/{run}/{token}"), &[], b"{\"n\":1}")
        .await;
    let offset = sent.json::<Value>().await.unwrap()["offset"]
        .as_u64()
        .unwrap();

    target.wait_for(1).await;
    eventually("the endpoint to pause", async || {
        let health = common::state_entry(&format!("health/{run}")).await?;
        (health["state"] == "paused").then_some(())
    })
    .await;
    relay
        .admin(
            Method::POST,
            &format!("/endpoints/{run}/retry"),
            Value::Null,
        )
        .await;
    common::within(Duration::from_secs(20), "the retry", async || {
        (target.received().len() >= 2).then_some(())
    })
    .await;

    let event = eventually("the event with its attempts", async || {
        let event = relay
            .admin(Method::GET, &format!("/events/{run}/{offset}"), Value::Null)
            .await;
        (event["attempts"].as_array()?.len() >= 2).then_some(event)
    })
    .await;
    assert_eq!(event["body"], "{\"n\":1}");
    let statuses: Vec<&Value> = event["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| &a["status"])
        .collect();
    assert!(statuses.contains(&&json!(503)) && statuses.contains(&&json!(204)));

    // The page offers every action the API has.
    let page = relay
        .http
        .get(relay.url(&format!("/admin/{TENANT}")))
        .bearer_auth(id_token(ALICE).await)
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let html = page.text().await.unwrap();
    for action in [
        format!("/api/{TENANT}/sources/{{id}}\" data-method=\"PUT\""),
        format!("/api/{TENANT}/endpoints/{{id}}\" data-method=\"PUT\""),
        format!("/api/{TENANT}/endpoints/{run}/retry\""),
        format!("/api/{TENANT}/endpoints/{run}/secret\""),
        format!("/api/{TENANT}/endpoints/{run}/replays\""),
        format!("/api/{TENANT}/endpoints/{run}\" data-method=\"DELETE\""),
    ] {
        assert!(html.contains(&action), "the page has no {action}");
    }
    assert!(html.contains("Signed in as alice@example.com"));

    relay
        .admin(Method::DELETE, &format!("/endpoints/{run}"), Value::Null)
        .await;
    relay
        .admin(Method::DELETE, &format!("/sources/{run}"), Value::Null)
        .await;
    let gone = relay
        .call_as(
            ALICE,
            TENANT,
            Method::GET,
            &format!("/sources/{run}"),
            Value::Null,
        )
        .await;
    assert_eq!(gone.status(), StatusCode::NOT_FOUND);
}

/// Follow redirects by hand from `url`, through Dex's login form as `email`,
/// until the relay's own callback, and return the relay's session cookie and
/// where it sent the browser.
async fn sign_in(relay: &Relay, email: &str, path: &str) -> (String, String) {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let first = http.get(relay.url(path)).send().await.unwrap();
    assert_eq!(
        first.status(),
        StatusCode::SEE_OTHER,
        "signed out goes to sign-in"
    );
    let mut url = relay.url(first.headers()[LOCATION].to_str().unwrap());
    let mut login_cookie = String::new();
    for _ in 0..20 {
        let response = http
            .get(&url)
            .header(COOKIE, &login_cookie)
            .send()
            .await
            .unwrap();
        for cookie in response.headers().get_all(SET_COOKIE) {
            let cookie = cookie.to_str().unwrap();
            if cookie.starts_with("relay_session=") {
                let session = cookie.split(';').next().unwrap().to_string();
                let to = response.headers()[LOCATION].to_str().unwrap().to_string();
                return (session, to);
            }
            if cookie.starts_with("relay_login=") {
                login_cookie = cookie.split(';').next().unwrap().to_string();
            }
        }
        let next = match response.headers().get(LOCATION) {
            Some(location) => location.to_str().unwrap().to_string(),
            None => {
                // Dex's login form: post the password to the page it came from.
                assert!(
                    url.starts_with(DEX),
                    "stopped at {url}: {}",
                    response.status()
                );
                let posted = http
                    .post(&url)
                    .form(&[("login", email), ("password", "password")])
                    .send()
                    .await
                    .unwrap();
                posted.headers()[LOCATION].to_str().unwrap().to_string()
            }
        };
        url = if next.starts_with("http") {
            next
        } else if next.starts_with("/dex") {
            format!("http://127.0.0.1:5556{next}")
        } else {
            relay.url(&next)
        };
    }
    panic!("the sign-in did not end at the relay");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn admins_sign_in_and_others_are_refused_by_the_control_plane() {
    // Dex only sends people back to a callback it knows, on this port.
    let relay = Relay::start(
        "admin",
        &[
            ("RELAY_LISTEN", "127.0.0.1:18090"),
            ("RELAY_PUBLIC_URL", "http://127.0.0.1:18090"),
        ],
    )
    .await;
    let page = format!("/admin/{TENANT}");

    let (session, to) = sign_in(&relay, ALICE, &page).await;
    assert_eq!(to, page);
    let response = relay
        .http
        .get(relay.url(&page))
        .header(COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("Signed in as alice@example.com")
    );

    let (session, _) = sign_in(&relay, CAROL, &page).await;
    let response = relay
        .http
        .get(relay.url(&page))
        .header(COOKIE, &session)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let html = response.text().await.unwrap();
    assert!(
        html.contains("carol@example.com cannot administer"),
        "{html}"
    );
    assert!(html.contains("control plane refused"), "{html}");

    // A cookie the relay did not seal is no session at all.
    let forged = relay
        .http
        .get(relay.url(&format!("/api/{TENANT}/dead")))
        .header(COOKIE, "relay_session=AAAA")
        .send()
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::UNAUTHORIZED);
}

/// `token` with its payload claims changed by `edit`, the signature left as
/// it was, as someone forging a token would.
fn tampered(token: &str, edit: impl FnOnce(&mut Value)) -> String {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let parts: Vec<&str> = token.split('.').collect();
    let mut claims: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    edit(&mut claims);
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    format!("{}.{payload}.{}", parts[0], parts[2])
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_tampered_token_is_refused_and_acts_on_nothing() {
    let run = unique("forged");
    let relay = Relay::start("admin", &[("RELAY_TENANTS", "acme,globex")]).await;
    let alice = id_token(ALICE).await;
    // Alice's real token is in use, so a cache keyed loosely would hand a
    // forgery her connection.
    let real = relay
        .call_as(
            ALICE,
            TENANT,
            Method::GET,
            &format!("/sources/{run}"),
            Value::Null,
        )
        .await;
    assert_eq!(real.status(), StatusCode::NOT_FOUND);

    let forgeries = [
        (
            "as bob, for globex",
            tampered(&alice, |c| c["email"] = json!(BOB)),
            GLOBEX,
        ),
        (
            "as bob, for acme",
            tampered(&alice, |c| c["email"] = json!(BOB)),
            TENANT,
        ),
        (
            "alice, longer lived",
            tampered(&alice, |c| c["exp"] = json!(4_000_000_000u64)),
            TENANT,
        ),
        (
            "alice, another audience",
            tampered(&alice, |c| c["aud"] = json!("felix-webhook-relay")),
            TENANT,
        ),
    ];
    for (what, token, tenant) in forgeries {
        for (method, path) in [
            (Method::GET, format!("/api/{tenant}/sources/{run}")),
            (Method::PUT, format!("/api/{tenant}/sources/{run}")),
        ] {
            let response = relay
                .http
                .request(method.clone(), relay.url(&path))
                .bearer_auth(&token)
                .json(&json!({ "scheme": { "type": "token" } }))
                .send()
                .await
                .unwrap();
            let status = response.status();
            assert!(
                status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN,
                "{what}: {method} {path} -> {status}"
            );
        }
        let page = relay
            .http
            .get(relay.url(&format!("/admin/{tenant}")))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(!page.contains("Signed in as"), "{what}: the page opened");
    }
    // Nothing was created in either tenant.
    for (who, tenant) in [(ALICE, TENANT), (BOB, GLOBEX)] {
        let shown = relay
            .call_as(
                who,
                tenant,
                Method::GET,
                &format!("/sources/{run}"),
                Value::Null,
            )
            .await;
        assert_eq!(shown.status(), StatusCode::NOT_FOUND, "{tenant}");
    }
}
