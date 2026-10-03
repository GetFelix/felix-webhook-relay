//! One endpoint's requests: signing, sending, and the records a request
//! leaves behind. Live delivery and jobs both send through this.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_relay_core::Envelope;
use felix_relay_core::catalog::{Endpoint, endpoint_key};
use felix_relay_core::health::parse_retry_after;
use felix_relay_core::records::{Attempt, DeadLetter, RESPONSE_SNIPPET_BYTES};
use felix_relay_core::signature::standard_signature;
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};

use crate::{App, unix_millis};

/// The pause before retrying a failed call to Felix.
pub(super) const FELIX_RETRY: Duration = Duration::from_secs(1);

/// A number in `[0, 1)` to spread backoff waits. It only has to differ
/// between endpoints, so the clock's low bits do.
pub(super) fn jitter() -> f64 {
    f64::from(unix_millis() as u32 % 1000) / 1000.0
}

/// The answer to one request, as much of it as the relay keeps.
pub(super) struct Answer {
    pub(super) status: Option<u16>,
    pub(super) retry_after: Option<Duration>,
    pub(super) detail: String,
    pub(super) at: u64,
    pub(super) millis: u64,
}

#[derive(Clone)]
pub(super) struct Sender {
    pub(super) app: Arc<App>,
    pub(super) endpoint: String,
    pub(super) source: String,
    pub(super) http: reqwest::Client,
}

impl Sender {
    /// The endpoint's config as it stands.
    pub(super) fn endpoint(&self) -> Result<Endpoint> {
        self.app
            .catalog
            .borrow()
            .endpoints
            .get(&self.endpoint)
            .cloned()
            .with_context(|| format!("endpoint {} is not configured", self.endpoint))
    }

    /// One signed request. The caller reads the endpoint's config afresh for
    /// every attempt, so a URL change or a secret rotation applies to the next.
    pub(super) async fn send(
        &self,
        endpoint: &Endpoint,
        id: &str,
        envelope: &Envelope,
        job: Option<&str>,
    ) -> Answer {
        let at = unix_millis();
        let started = Instant::now();
        let result = self.request(endpoint, id, envelope, job, at).await;
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
        job: Option<&str>,
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
        if let Some(job) = job {
            request = request.header("webhook-replay", job);
        }
        Ok(request.send().await?)
    }

    /// The trail of attempts is not a source of truth, so it is published
    /// without waiting for the broker.
    pub(super) fn log_attempt(&self, offset: u64, id: &str, answer: &Answer) {
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
    pub(super) async fn dead_letter(
        &self,
        offset: u64,
        envelope: Envelope,
        attempts: u32,
        answer: Answer,
    ) {
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
}
