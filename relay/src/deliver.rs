//! The delivery task for one endpoint: poll its group, send each record in
//! order, signed, and settle it. A record is settled when the endpoint takes
//! it or when it is dead-lettered after the endpoint refused it for good.
//!
//! The task never polls while it holds an unsettled record. Felix hands out
//! lapsed claims first, so holding one while polling again would let a later
//! record overtake it, and Felix cannot extend a claim or delay a nack. So a
//! record that is still being retried after its claim lapsed simply stays in
//! hand; the late acknowledgement settles it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_client::ClusterClient;
use felix_relay_core::Envelope;
use felix_relay_core::catalog::{Disabled, Endpoint, endpoint_key};
use felix_relay_core::health::{Decision, Health, Outcome, State, parse_retry_after};
use felix_relay_core::records::{Attempt, DeadLetter, RESPONSE_SNIPPET_BYTES};
use felix_relay_core::signature::standard_signature;
use felix_wire::GroupRecord;
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};

use crate::catalog::CONFIG;
use crate::report::Reporter;
use crate::{App, unix_millis};

/// Records taken per poll.
const BATCH: u32 = 16;
/// How long the broker may hold a poll open waiting for work.
const POLL_WAIT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// The pause before retrying a failed call to Felix.
const FELIX_RETRY: Duration = Duration::from_secs(1);

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    let endpoint = app.config.endpoint.clone();
    let mut task = Task {
        source: String::new(),
        group: format!("ep.{endpoint}"),
        reporter: Reporter::start(
            Arc::clone(&app.felix),
            &endpoint,
            app.config.reporter.clone(),
        ),
        http: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .build()?,
        health: Health::default(),
        disabling: false,
        last_offset: None,
        endpoint,
        app,
    };
    task.source = task.enabled().await?.source;
    task.run().await
}

/// The answer to one request, as much of it as the relay keeps.
struct Answer {
    status: Option<u16>,
    retry_after: Option<Duration>,
    detail: String,
    at: u64,
    millis: u64,
}

struct Task {
    app: Arc<App>,
    endpoint: String,
    source: String,
    group: String,
    reporter: Reporter,
    http: reqwest::Client,
    health: Health,
    /// Set between deciding to disable the endpoint and seeing that in config.
    disabling: bool,
    last_offset: Option<u64>,
}

impl Task {
    fn felix(&self) -> &'static ClusterClient {
        self.app.felix.client()
    }

    async fn run(&mut self) -> Result<()> {
        let stream = format!("src.{}", self.source);
        tracing::info!(%stream, group = %self.group, "delivering");
        // Claims carry no consumer identity (felix#962), so the claims of a
        // run before this one come back only once they lapse. Polling before
        // then would hand out newer records ahead of them.
        tokio::time::sleep(self.app.config.claim_wait).await;
        let felix = Arc::clone(&self.app.felix);
        loop {
            let polled = self
                .felix()
                .group_poll_wait(
                    &felix.tenant,
                    &felix.namespace,
                    &stream,
                    0,
                    &self.group,
                    BATCH,
                    POLL_WAIT,
                )
                .await;
            self.app
                .metrics
                .polls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let records = match polled {
                Ok(records) => records,
                Err(err) => {
                    tracing::warn!(group = %self.group, "poll failed: {err:#}");
                    tokio::time::sleep(FELIX_RETRY).await;
                    continue;
                }
            };
            let polled_at = unix_millis();
            for record in records {
                self.settle(&record, polled_at).await?;
                self.ack(&stream, record.offset).await;
            }
        }
    }

    async fn settle(&mut self, record: &GroupRecord, polled_at: u64) -> Result<()> {
        if let Some(last) = self.last_offset
            && record.offset > last + 1
        {
            self.reporter.gap(last + 1, record.offset - 1);
        }
        self.last_offset = Some(record.offset);
        let envelope = match Envelope::decode(&record.payload) {
            Ok(envelope) => envelope,
            Err(err) => {
                // Nothing can deliver it, and holding it would stop the endpoint.
                tracing::error!(offset = record.offset, "skipping: {err}");
                return Ok(());
            }
        };
        let age = polled_at.saturating_sub(envelope.received_at);
        self.app
            .metrics
            .poll_wakeup
            .record(Duration::from_millis(age));
        let id = envelope.event_id(&self.source, record.offset);
        let mut tries = 0;
        loop {
            let endpoint = self.enabled().await?;
            let answer = self.send(&endpoint, &id, &envelope).await;
            tries += 1;
            self.log_attempt(record.offset, &id, &answer);
            let outcome = Outcome::classify(answer.status, answer.retry_after);
            let jitter = f64::from(unix_millis() as u32 % 1000) / 1000.0;
            let before = self.health.state;
            let decision = self
                .health
                .decide(outcome, answer.at, jitter, &self.app.config.policy);
            let state = self.health.state;
            let failing_since = self.health.failing_since;
            self.reporter.update(state != before, |report| {
                report.state = state;
                report.failing_since = failing_since;
                if outcome != Outcome::Delivered {
                    report.last_error = Some(match answer.status {
                        Some(status) => format!("{status} {}", answer.detail),
                        None => answer.detail.clone(),
                    });
                }
            });
            match decision {
                Decision::Ack => return Ok(()),
                Decision::Retry(wait) | Decision::Pause(wait) => tokio::time::sleep(wait).await,
                Decision::DeadLetter => {
                    self.dead_letter(record.offset, envelope, tries, answer)
                        .await;
                    return Ok(());
                }
                Decision::Disable(reason) => {
                    tracing::warn!(endpoint = %self.endpoint, "disabling: {reason}");
                    self.disable(reason).await;
                }
            }
        }
    }

    /// The endpoint's config once it exists and is enabled. While it is
    /// disabled the task waits here, holding whatever it had claimed.
    async fn enabled(&mut self) -> Result<Endpoint> {
        let mut catalog = self.app.catalog.clone();
        loop {
            let endpoint = catalog
                .borrow_and_update()
                .endpoints
                .get(&self.endpoint)
                .cloned();
            match endpoint {
                Some(endpoint) if endpoint.disabled.is_some() => {
                    self.disabling = false;
                    if self.health.state != State::Disabled {
                        self.health.state = State::Disabled;
                        self.reporter
                            .update(true, |report| report.state = State::Disabled);
                    }
                }
                // Until this task's own write shows up, an enabled entry is stale.
                Some(endpoint) if !self.disabling => {
                    if self.health.state == State::Disabled {
                        tracing::info!(endpoint = %self.endpoint, "enabled again");
                        self.health.enable();
                        self.reporter.update(true, |report| {
                            report.state = State::Active;
                            report.failing_since = None;
                        });
                    }
                    return Ok(endpoint);
                }
                _ => {}
            }
            catalog
                .changed()
                .await
                .context("the config watch stopped")?;
        }
    }

    /// Mark the endpoint disabled in config, where an operator sees it and
    /// can enable it again.
    async fn disable(&mut self, reason: String) {
        let felix = &self.app.felix;
        let key = endpoint_key(&self.endpoint);
        loop {
            let written = async {
                let bytes = felix.cache_get(CONFIG, &key).await?.context("no config")?;
                let mut endpoint: Endpoint = serde_json::from_slice(&bytes)?;
                endpoint.disabled = Some(Disabled {
                    reason: reason.clone(),
                    at: unix_millis(),
                });
                felix
                    .cache_put(CONFIG, &key, serde_json::to_vec(&endpoint)?, None)
                    .await
            };
            match written.await {
                Ok(()) => break,
                Err(err) => {
                    tracing::warn!(endpoint = %self.endpoint, "could not disable: {err:#}");
                    tokio::time::sleep(FELIX_RETRY).await;
                }
            }
        }
        self.disabling = true;
    }

    /// One signed request. The caller reads the endpoint's config afresh for
    /// every attempt, so a URL change or a secret rotation applies to the next.
    async fn send(&self, endpoint: &Endpoint, id: &str, envelope: &Envelope) -> Answer {
        let at = unix_millis();
        let started = Instant::now();
        let result = self.request(endpoint, id, envelope, at).await;
        let elapsed = started.elapsed();
        self.app.metrics.outbound.record(elapsed);
        let millis = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        match result {
            Ok(mut response) => {
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_retry_after);
                let mut snippet = Vec::new();
                while snippet.len() < RESPONSE_SNIPPET_BYTES {
                    match response.chunk().await {
                        Ok(Some(chunk)) => snippet.extend_from_slice(&chunk),
                        _ => break,
                    }
                }
                snippet.truncate(RESPONSE_SNIPPET_BYTES);
                Answer {
                    status: Some(response.status().as_u16()),
                    retry_after,
                    detail: String::from_utf8_lossy(&snippet).into_owned(),
                    at,
                    millis,
                }
            }
            Err(err) => Answer {
                status: None,
                retry_after: None,
                detail: format!("{err:#}"),
                at,
                millis,
            },
        }
    }

    async fn request(
        &self,
        endpoint: &Endpoint,
        id: &str,
        envelope: &Envelope,
        now: u64,
    ) -> Result<reqwest::Response> {
        let timestamp = now / 1000;
        let key = &self.app.config.secret_key;
        let name = endpoint_key(&self.endpoint);
        let mut secrets = vec![key.open(&name, &endpoint.secret)?];
        if let Some(previous) = endpoint.previous_secret.as_ref().filter(|p| p.until > now) {
            secrets.push(key.open(&name, &previous.secret)?);
        }
        let signatures = secrets
            .iter()
            .map(|secret| standard_signature(secret, id, timestamp, &envelope.body))
            .collect::<Result<Vec<_>, _>>()?
            .join(" ");
        let mut request = self
            .http
            .post(&endpoint.url)
            .header("webhook-id", id)
            .header("webhook-timestamp", timestamp.to_string())
            .header("webhook-signature", signatures)
            .body(envelope.body.clone());
        if let Some(content_type) = &envelope.content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        Ok(request.send().await?)
    }

    /// The trail of attempts is not a source of truth, so it is published
    /// without waiting for the broker.
    fn log_attempt(&self, offset: u64, id: &str, answer: &Answer) {
        let attempt = Attempt {
            endpoint: self.endpoint.clone(),
            offset,
            event_id: id.to_string(),
            at: answer.at,
            millis: answer.millis,
            status: answer.status,
            detail: answer.detail.clone(),
        };
        let felix = Arc::clone(&self.app.felix);
        tokio::spawn(async move {
            if let Err(err) = felix.publish_unacked("attempts", attempt.encode()).await {
                tracing::debug!("attempt not logged: {err:#}");
            }
        });
    }

    /// Store the record in the `dead` stream. Only once it is stored may the
    /// record be acknowledged, or a crash in between would lose it.
    async fn dead_letter(&self, offset: u64, envelope: Envelope, attempts: u32, answer: Answer) {
        let dead = DeadLetter {
            endpoint: self.endpoint.clone(),
            source: self.source.clone(),
            offset,
            at: unix_millis(),
            attempts,
            last_status: answer.status,
            last_response: answer.detail,
            envelope,
        };
        tracing::warn!(endpoint = %self.endpoint, offset, "dead-lettered");
        while let Err(err) = self
            .app
            .felix
            .append("dead".to_string(), dead.encode())
            .await
        {
            tracing::warn!(offset, "could not store a dead letter: {err:#}");
            tokio::time::sleep(FELIX_RETRY).await;
        }
    }

    async fn ack(&self, stream: &str, offset: u64) {
        let felix = &self.app.felix;
        while let Err(err) = self
            .felix()
            .group_ack(
                &felix.tenant,
                &felix.namespace,
                stream,
                0,
                &self.group,
                offset,
            )
            .await
        {
            tracing::warn!(offset, "ack failed: {err:#}");
            tokio::time::sleep(FELIX_RETRY).await;
        }
        self.reporter
            .update(false, |report| report.last_acked = Some(offset));
    }
}
