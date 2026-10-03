//! One endpoint's delivery task: poll its group, send what the endpoint
//! wants, signed, and settle each record. A record is settled when the
//! endpoint takes it, when it is dead-lettered after the endpoint refused it
//! for good, or when the endpoint does not want it at all.
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
use felix_relay_core::catalog::{Disabled, Endpoint, Mode, endpoint_key};
use felix_relay_core::health::{Decision, Health, Outcome, State, parse_retry_after};
use felix_relay_core::records::{Attempt, DeadLetter, RESPONSE_SNIPPET_BYTES};
use felix_relay_core::signature::standard_signature;
use futures_util::future::join_all;
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};

use crate::catalog::CONFIG;
use crate::report::Reporter;
use crate::{App, unix_millis};

/// Records an ordered endpoint takes per poll.
const ORDERED_BATCH: u32 = 16;
/// How long the broker may hold a poll open waiting for work.
const POLL_WAIT: Duration = Duration::from_secs(10);
/// The pause before retrying a failed call to Felix.
const FELIX_RETRY: Duration = Duration::from_secs(1);

/// The answer to one request, as much of it as the relay keeps.
struct Answer {
    status: Option<u16>,
    retry_after: Option<Duration>,
    detail: String,
    at: u64,
    millis: u64,
}

/// A record in hand, not yet settled.
struct Held {
    offset: u64,
    id: String,
    envelope: Envelope,
    tries: u32,
    refusals: usize,
}

pub(super) struct Task {
    app: Arc<App>,
    endpoint: String,
    source: String,
    stream: String,
    group: String,
    mode: Mode,
    window: u32,
    reporter: Reporter,
    http: reqwest::Client,
    health: Health,
    /// Set between deciding to disable the endpoint and seeing that in config.
    disabling: bool,
    last_offset: Option<u64>,
}

impl Task {
    pub(super) fn new(app: Arc<App>, id: &str, endpoint: &Endpoint, http: reqwest::Client) -> Self {
        Self {
            reporter: Reporter::start(Arc::clone(&app.felix), id, app.config.reporter.clone()),
            endpoint: id.to_string(),
            source: endpoint.source.clone(),
            stream: format!("src.{}", endpoint.source),
            group: format!("ep.{id}"),
            mode: endpoint.mode,
            window: endpoint.in_flight(),
            http,
            health: Health::default(),
            disabling: false,
            last_offset: None,
            app,
        }
    }

    fn felix(&self) -> &'static ClusterClient {
        self.app.felix.client()
    }

    pub(super) async fn run(mut self) -> Result<()> {
        self.enabled().await?;
        tracing::info!(stream = %self.stream, group = %self.group, "delivering");
        // Claims carry no consumer identity (felix#962), so the claims of a
        // run before this one come back only once they lapse. Polling before
        // then would hand out newer records ahead of them.
        tokio::time::sleep(self.app.config.claim_wait).await;
        let felix = Arc::clone(&self.app.felix);
        let batch = match self.mode {
            Mode::Ordered => ORDERED_BATCH,
            Mode::Unordered => self.window,
        };
        loop {
            let polled = self
                .felix()
                .group_poll_wait(
                    &felix.tenant,
                    &felix.namespace,
                    &self.stream,
                    0,
                    &self.group,
                    batch,
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
            let mut wanted = Vec::new();
            for record in records {
                match self.take(record.offset, &record.payload, polled_at).await? {
                    Some(held) => wanted.push(held),
                    None => self.ack(record.offset).await,
                }
            }
            match self.mode {
                Mode::Ordered => {
                    for held in wanted {
                        self.settle(vec![held]).await?;
                    }
                }
                Mode::Unordered => self.settle(wanted).await?,
            }
        }
    }

    /// A polled record to send, or `None` for one to acknowledge unsent.
    async fn take(&mut self, offset: u64, payload: &[u8], polled_at: u64) -> Result<Option<Held>> {
        if let Some(last) = self.last_offset
            && offset > last + 1
        {
            self.reporter.gap(last + 1, offset - 1);
        }
        self.last_offset = Some(offset);
        let envelope = match Envelope::decode(payload) {
            Ok(envelope) => envelope,
            Err(err) => {
                // Nothing can deliver it, and holding it would stop the endpoint.
                tracing::error!(offset, "skipping: {err}");
                return Ok(None);
            }
        };
        let endpoint = self.enabled().await?;
        if !endpoint.wants(offset, envelope.event_type.as_deref()) {
            return Ok(None);
        }
        let age = polled_at.saturating_sub(envelope.received_at);
        self.app
            .metrics
            .poll_wakeup
            .record(Duration::from_millis(age));
        Ok(Some(Held {
            offset,
            id: envelope.event_id(&self.source, offset),
            envelope,
            tries: 0,
            refusals: 0,
        }))
    }

    /// Send every held record, at once, until each is settled. An ordered
    /// endpoint calls this with one record at a time.
    async fn settle(&mut self, mut held: Vec<Held>) -> Result<()> {
        while !held.is_empty() {
            let endpoint = self.enabled().await?;
            let answers = join_all(
                held.iter()
                    .map(|record| self.send(&endpoint, &record.id, &record.envelope)),
            )
            .await;
            let outcomes: Vec<Outcome> = answers
                .iter()
                .map(|answer| Outcome::classify(answer.status, answer.retry_after))
                .collect();
            let mut refusals: Vec<usize> = held.iter().map(|record| record.refusals).collect();
            let jitter = f64::from(unix_millis() as u32 % 1000) / 1000.0;
            let before = self.health.state;
            let decisions = self.health.decide_window(
                &outcomes,
                &mut refusals,
                unix_millis(),
                jitter,
                &self.app.config.policy,
            );
            self.report(before, &outcomes, &answers);

            let mut wait = Duration::ZERO;
            let mut disable = None;
            let mut kept = Vec::new();
            for (((mut record, answer), decision), refusals) in
                held.into_iter().zip(answers).zip(decisions).zip(refusals)
            {
                record.tries += 1;
                record.refusals = refusals;
                self.log_attempt(record.offset, &record.id, &answer);
                match decision {
                    Decision::Ack => self.ack(record.offset).await,
                    Decision::DeadLetter => {
                        let offset = record.offset;
                        self.dead_letter(offset, record.envelope, record.tries, answer)
                            .await;
                        self.ack(offset).await;
                    }
                    Decision::Retry(after) | Decision::Pause(after) => {
                        wait = wait.max(after);
                        kept.push(record);
                    }
                    Decision::Disable(reason) => {
                        disable = Some(reason);
                        kept.push(record);
                    }
                }
            }
            held = kept;
            match disable {
                Some(reason) => {
                    tracing::warn!(endpoint = %self.endpoint, "disabling: {reason}");
                    self.disable(reason).await;
                }
                None => tokio::time::sleep(wait).await,
            }
        }
        Ok(())
    }

    fn report(&self, before: State, outcomes: &[Outcome], answers: &[Answer]) {
        let state = self.health.state;
        let failing_since = self.health.failing_since;
        let failed = outcomes
            .iter()
            .zip(answers)
            .find(|(outcome, _)| **outcome != Outcome::Delivered);
        self.reporter.update(state != before, |report| {
            report.state = state;
            report.failing_since = failing_since;
            if let Some((_, answer)) = failed {
                report.last_error = Some(match answer.status {
                    Some(status) => format!("{status} {}", answer.detail),
                    None => answer.detail.clone(),
                });
            }
        });
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

    async fn ack(&self, offset: u64) {
        let felix = &self.app.felix;
        while let Err(err) = self
            .felix()
            .group_ack(
                &felix.tenant,
                &felix.namespace,
                &self.stream,
                0,
                &self.group,
                offset,
            )
            .await
        {
            tracing::warn!(offset, "ack failed: {err:#}");
            tokio::time::sleep(FELIX_RETRY).await;
        }
        self.reporter.update(false, |report| {
            report.last_acked = report.last_acked.max(Some(offset))
        });
    }
}
