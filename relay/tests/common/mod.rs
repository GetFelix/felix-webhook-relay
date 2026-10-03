//! What the integration tests share: a relay process, a recording endpoint,
//! and direct access to the development Felix stack started by `dev/up.sh`.
// Each test binary uses a different part of this.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::routing::post;
use felix_client::{ClientConfig, ClusterClient, StartPosition};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use serde_json::{Value, json};

pub const WAIT: Duration = Duration::from_secs(60);
/// The `RELAY_SECRET_KEY` every test relay runs with.
pub const SECRET_KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
pub const TENANT: &str = "acme";
/// The dev stack's Dex, where the tests' admins sign in.
pub const DEX: &str = "http://127.0.0.1:5556/dex";
/// Administers `acme`; `bob@example.com` administers `globex`, and
/// `carol@example.com` nothing.
pub const ALICE: &str = "alice@example.com";

/// An ID token for a Dex user, by password, as a script would get one.
/// Kept per user for the run, so the relay sees one admin session each.
pub async fn id_token(email: &str) -> String {
    static TOKENS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);
    if let Some(token) = TOKENS.lock().unwrap().get_or_insert_default().get(email) {
        return token.clone();
    }
    let token = fresh_id_token(email).await;
    TOKENS
        .lock()
        .unwrap()
        .get_or_insert_default()
        .insert(email.to_string(), token.clone());
    token
}

async fn fresh_id_token(email: &str) -> String {
    let answer: Value = reqwest::Client::new()
        .post(format!("{DEX}/token"))
        .basic_auth("relay-admin", Some("dev-admin-secret"))
        .form(&[
            ("grant_type", "password"),
            ("username", email),
            ("password", "password"),
            ("scope", "openid email profile"),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    answer["id_token"]
        .as_str()
        .unwrap_or_else(|| panic!("no ID token for {email}: {answer}"))
        .to_string()
}
const CONTROL_PLANE: &str = "http://127.0.0.1:8443";

/// An id unique to one test run. Streams and groups outlive a test, so
/// every test names its own.
pub fn unique(test: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{test}-{nanos}")
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Poll `check` until it returns something, or fail after [`WAIT`].
pub async fn eventually<T>(what: &str, check: impl AsyncFnMut() -> Option<T>) -> T {
    within(WAIT, what, check).await
}

/// Poll `check` until it returns something, or fail after `limit`.
pub async fn within<T>(
    limit: Duration,
    what: &str,
    mut check: impl AsyncFnMut() -> Option<T>,
) -> T {
    let started = Instant::now();
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(started.elapsed() < limit, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A `felix-relay` process, killed when dropped.
pub struct Relay {
    pub child: Child,
    pub addr: SocketAddr,
    pub http: reqwest::Client,
}

impl Relay {
    pub async fn start(roles: &str, env: &[(&str, &str)]) -> Self {
        Self::start_logging(roles, env, Stdio::inherit()).await
    }

    /// Like [`Relay::start`], with its log written to `log`.
    pub async fn start_logging(roles: &str, env: &[(&str, &str)], log: Stdio) -> Self {
        // Tests share one tenant, so a delivery process must keep to its own
        // test's endpoints.
        assert!(
            !roles.contains("deliver") || env.iter().any(|(n, _)| *n == "RELAY_ENDPOINT_PREFIXES"),
            "a delivery process in a test needs RELAY_ENDPOINT_PREFIXES"
        );
        let addr = match env.iter().find(|(name, _)| *name == "RELAY_LISTEN") {
            Some((_, listen)) => listen.parse().unwrap(),
            None => TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap(),
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_felix-relay"));
        command
            .env("RELAY_LISTEN", addr.to_string())
            .env("RELAY_ROLES", roles)
            .env("RELAY_SECRET_KEY", SECRET_KEY)
            .env("RELAY_IDP_TOKEN_FILE", state_dir().join("relay-idp.token"))
            .env("RELAY_OIDC_ISSUER", DEX)
            .env("RELAY_OIDC_CLIENT_ID", "relay-admin")
            .env("RELAY_OIDC_CLIENT_SECRET", "dev-admin-secret")
            // Every test's group is new, so there are no earlier claims to wait out.
            .env("RELAY_CLAIM_WAIT_MS", "0");
        for (name, value) in env {
            command.env(name, value);
        }
        let child = command.stdout(log).spawn().expect("start felix-relay");
        let mut relay = Self {
            child,
            addr,
            http: reqwest::Client::new(),
        };
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

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// An admin API call on `acme` as alice that must succeed; returns its
    /// JSON answer.
    pub async fn admin(&self, method: reqwest::Method, path: &str, body: Value) -> Value {
        self.admin_as(ALICE, TENANT, method, path, body).await
    }

    pub async fn admin_as(
        &self,
        who: &str,
        tenant: &str,
        method: reqwest::Method,
        path: &str,
        body: Value,
    ) -> Value {
        let response = self.call_as(who, tenant, method.clone(), path, body).await;
        let status = response.status();
        let text = response.text().await.unwrap();
        assert!(status.is_success(), "{method} {path} -> {status}: {text}");
        serde_json::from_str(&text).unwrap_or(Value::Null)
    }

    /// An admin API call as `who` on `tenant`, whatever its answer.
    pub async fn call_as(
        &self,
        who: &str,
        tenant: &str,
        method: reqwest::Method,
        path: &str,
        body: Value,
    ) -> reqwest::Response {
        self.http
            .request(method, self.url(&format!("/api/{tenant}{path}")))
            .bearer_auth(id_token(who).await)
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    /// Create a source, which creates its stream; returns its secret when the
    /// relay made one up.
    pub async fn create_source(&self, id: &str, config: Value) -> Option<String> {
        let answer = self
            .admin(reqwest::Method::PUT, &format!("/sources/{id}"), config)
            .await;
        answer["secret"].as_str().map(str::to_string)
    }

    /// Create an endpoint; returns its signing secret.
    pub async fn create_endpoint(&self, id: &str, source: &str, url: &str) -> String {
        let answer = self
            .admin(
                reqwest::Method::PUT,
                &format!("/endpoints/{id}"),
                json!({ "source": source, "url": url }),
            )
            .await;
        answer["secret"].as_str().unwrap().to_string()
    }

    /// Post a webhook, waiting out the moment between a source being
    /// written and this relay's config watch seeing it.
    pub async fn send(
        &self,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> reqwest::Response {
        let started = Instant::now();
        loop {
            let mut request = self.http.post(self.url(path)).body(body.to_vec());
            for (name, value) in headers {
                request = request.header(*name, *value);
            }
            let response = request.send().await.unwrap();
            if response.status() != StatusCode::NOT_FOUND || started.elapsed() > WAIT {
                return response;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A request the test endpoint received.
#[derive(Clone, Debug)]
pub struct Received {
    pub headers: HeaderMap,
    pub body: Bytes,
    /// What the endpoint answered.
    pub status: StatusCode,
    pub uri: Uri,
    pub at: Instant,
}

impl Received {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    pub fn id(&self) -> &str {
        self.header("webhook-id").unwrap_or_default()
    }
}

type Reply = Arc<dyn Fn(usize, &Received) -> StatusCode + Send + Sync>;

/// An HTTP endpoint that records every request.
#[derive(Clone)]
pub struct Endpoint {
    pub addr: SocketAddr,
    inbox: Arc<Mutex<Vec<Received>>>,
}

impl Endpoint {
    /// Answers `204` to everything.
    pub async fn start() -> Self {
        Self::replying(|_, _| StatusCode::NO_CONTENT).await
    }

    /// Answers with `reply(requests received before this one, headers)`.
    pub async fn replying(
        reply: impl Fn(usize, &Received) -> StatusCode + Send + Sync + 'static,
    ) -> Self {
        Self::replying_after(Duration::ZERO, reply).await
    }

    /// Like [`Endpoint::replying`], taking `delay` over every answer.
    pub async fn replying_after(
        delay: Duration,
        reply: impl Fn(usize, &Received) -> StatusCode + Send + Sync + 'static,
    ) -> Self {
        type Shared = (Arc<Mutex<Vec<Received>>>, Reply, Duration);
        async fn receive(
            State((inbox, reply, delay)): State<Shared>,
            uri: Uri,
            headers: HeaderMap,
            body: Bytes,
        ) -> StatusCode {
            let status = {
                let mut inbox = inbox.lock().unwrap();
                let mut received = Received {
                    headers,
                    body,
                    status: StatusCode::OK,
                    uri,
                    at: Instant::now(),
                };
                received.status = reply(inbox.len(), &received);
                inbox.push(received.clone());
                received.status
            };
            tokio::time::sleep(delay).await;
            status
        }
        let inbox = Arc::new(Mutex::new(Vec::new()));
        let shared: Shared = (Arc::clone(&inbox), Arc::new(reply), delay);
        let router = axum::Router::new()
            .route("/hook", post(receive))
            .with_state(shared);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await });
        Self { addr, inbox }
    }

    pub fn url(&self) -> String {
        format!("http://{}/hook", self.addr)
    }

    pub fn received(&self) -> Vec<Received> {
        self.inbox.lock().unwrap().clone()
    }

    /// Wait until at least `count` requests have arrived; returns them all.
    pub async fn wait_for(&self, count: usize) -> Vec<Received> {
        eventually(&format!("{count} requests at the endpoint"), async || {
            let received = self.received();
            (received.len() >= count).then_some(received)
        })
        .await
    }
}

fn state_dir() -> PathBuf {
    let ca = std::env::var("RELAY_FELIX_CA_FILE").expect("RELAY_FELIX_CA_FILE");
    PathBuf::from(ca).parent().unwrap().to_path_buf()
}

/// A client on the development broker with the relay's token.
pub async fn felix() -> Arc<ClusterClient> {
    let ca = std::env::var("RELAY_FELIX_CA_FILE").expect("RELAY_FELIX_CA_FILE");
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&ca).unwrap() {
        roots.add(cert.unwrap()).unwrap();
    }
    let quic = felix_client::quic_client_config(Some(Arc::new(roots)), true).unwrap();
    let mut config = ClientConfig::optimized_defaults(quic);
    config.auth_tenant_id = Some("relay".to_string());
    let token = std::fs::read_to_string(state_dir().join("relay.token")).unwrap();
    config.auth_token = Some(token.trim().to_string());
    let brokers = ["127.0.0.1:5000".parse().unwrap()];
    Arc::new(
        ClusterClient::connect(&brokers, "localhost", config)
            .await
            .unwrap(),
    )
}

/// Create a stream in the test tenant through the control plane, as an
/// operator would, and wait for the broker to learn of it.
pub async fn create_stream(stream: &str) {
    let admin = std::fs::read_to_string(state_dir().join("admin.token")).unwrap();
    let response = reqwest::Client::new()
        .post(format!(
            "{CONTROL_PLANE}/v1/tenants/relay/namespaces/{TENANT}/streams"
        ))
        .bearer_auth(admin.trim())
        .json(&json!({
            "stream": stream,
            "kind": "Stream",
            "shards": 1,
            "replication_factor": 1,
            "retention": { "max_age_seconds": null, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": true
        }))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "create {stream}: {}",
        response.status()
    );
    let felix = felix().await;
    eventually(&format!("the broker to serve {stream}"), async || {
        felix
            .subscribe_from("relay", TENANT, stream, Some(StartPosition::Latest))
            .await
            .ok()
    })
    .await;
}

/// The offset the next record appended to `stream` will get.
pub async fn tail(stream: &str) -> u64 {
    let felix = felix().await;
    let subscription = felix
        .subscribe_from("relay", TENANT, stream, Some(StartPosition::Latest))
        .await
        .unwrap();
    subscription.live_offset().expect("a live offset")
}

/// Every record in `stream` from the start that `keep` returns something
/// for, once there are at least `count` of them.
pub async fn read_stream<T>(
    stream: &str,
    count: usize,
    keep: impl Fn(&[u8]) -> Option<T>,
) -> Vec<T> {
    let felix = felix().await;
    let mut subscription = felix
        .subscribe_from("relay", TENANT, stream, Some(StartPosition::Earliest))
        .await
        .unwrap();
    let mut kept = Vec::new();
    while kept.len() < count {
        let event = tokio::time::timeout(WAIT, subscription.next_event())
            .await
            .unwrap_or_else(|_| panic!("only {} matching records in {stream}", kept.len()))
            .unwrap()
            .expect("an open subscription");
        kept.extend(keep(&event.payload));
    }
    kept
}

/// A JSON entry in the tenant's `state` cache.
pub async fn state_entry(key: &str) -> Option<Value> {
    let client = felix().await.client().await;
    let value = client
        .cache_get("relay", TENANT, "state", key)
        .await
        .unwrap()?;
    Some(serde_json::from_slice(&value).unwrap())
}

/// The value of a counter on a relay's `/metrics`.
pub async fn metric(relay: &Relay, name: &str) -> u64 {
    let metrics = reqwest::get(relay.url("/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("no {name} in /metrics"))
}
