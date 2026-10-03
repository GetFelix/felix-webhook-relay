//! The delivery task for one endpoint: poll its group, send each record in
//! order, signed, and acknowledge it once the endpoint answers `2xx`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_relay_core::Envelope;
use felix_relay_core::catalog::{Endpoint, endpoint_key};
use felix_relay_core::signature::standard_signature;
use reqwest::header::CONTENT_TYPE;

use crate::{App, unix_millis};

/// Records taken per poll.
const BATCH: u32 = 16;
/// How long the broker may hold a poll open waiting for work.
const POLL_WAIT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// The pause before sending a refused record again, or retrying a failed
/// call to Felix.
const RETRY_DELAY: Duration = Duration::from_secs(1);

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    let felix = &app.felix;
    let endpoint_id = &app.config.endpoint;
    let source = current(&app, endpoint_id).await?.source;
    let stream = format!("src.{source}");
    let group = format!("ep.{endpoint_id}");
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    tracing::info!(%stream, %group, "delivering");

    loop {
        // A new poll only once every record from the last one is settled.
        // Felix hands out lapsed claims first, so holding a record while
        // polling again would let a later one overtake it.
        let records = match felix
            .client()
            .group_poll_wait(
                &felix.tenant,
                &felix.namespace,
                &stream,
                0,
                &group,
                BATCH,
                POLL_WAIT,
            )
            .await
        {
            Ok(records) => records,
            Err(err) => {
                tracing::warn!(%group, "poll failed: {err:#}");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };
        let polled_at = unix_millis();

        for record in records {
            match Envelope::decode(&record.payload) {
                Ok(envelope) => {
                    let age = polled_at.saturating_sub(envelope.received_at);
                    app.metrics.poll_wakeup.record(Duration::from_millis(age));
                    let id = envelope.event_id(&source, record.offset);
                    send_until_accepted(&app, &http, endpoint_id, &id, &envelope).await;
                }
                // Nothing can deliver it, and holding it would stop the endpoint.
                Err(err) => tracing::error!(offset = record.offset, "skipping: {err}"),
            }
            loop {
                match felix
                    .client()
                    .group_ack(
                        &felix.tenant,
                        &felix.namespace,
                        &stream,
                        0,
                        &group,
                        record.offset,
                    )
                    .await
                {
                    Ok(()) => break,
                    Err(err) => {
                        tracing::warn!(offset = record.offset, "ack failed: {err:#}");
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }
        }
    }
}

/// The endpoint's config as it stands, waiting for it to exist.
async fn current(app: &App, id: &str) -> Result<Endpoint> {
    let mut catalog = app.catalog.clone();
    loop {
        if let Some(endpoint) = catalog.borrow_and_update().endpoints.get(id) {
            return Ok(endpoint.clone());
        }
        tracing::info!(endpoint = %id, "waiting for the endpoint to be configured");
        catalog
            .changed()
            .await
            .context("the config watch stopped")?;
    }
}

/// Send one event until the endpoint answers `2xx`. Retries are a fixed
/// pause for now; backoff and dead letters replace them.
async fn send_until_accepted(
    app: &App,
    http: &reqwest::Client,
    endpoint_id: &str,
    id: &str,
    envelope: &Envelope,
) {
    loop {
        match send(app, http, endpoint_id, id, envelope).await {
            Ok(status) if status.is_success() => return,
            Ok(status) => tracing::warn!(%id, %status, "endpoint refused"),
            Err(err) => tracing::warn!(%id, "request failed: {err:#}"),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// One signed request, with the endpoint's config read afresh so a URL
/// change or a secret rotation applies to the next attempt.
async fn send(
    app: &App,
    http: &reqwest::Client,
    endpoint_id: &str,
    id: &str,
    envelope: &Envelope,
) -> Result<reqwest::StatusCode> {
    let endpoint = current(app, endpoint_id).await?;
    let now = unix_millis();
    let timestamp = now / 1000;
    let key = &app.config.secret_key;
    let name = endpoint_key(endpoint_id);
    let mut secrets = vec![key.open(&name, &endpoint.secret)?];
    if let Some(previous) = endpoint.previous_secret.as_ref().filter(|p| p.until > now) {
        secrets.push(key.open(&name, &previous.secret)?);
    }
    let signatures = secrets
        .iter()
        .map(|secret| standard_signature(secret, id, timestamp, &envelope.body))
        .collect::<Result<Vec<_>, _>>()?
        .join(" ");

    let mut request = http
        .post(&endpoint.url)
        .header("webhook-id", id)
        .header("webhook-timestamp", timestamp.to_string())
        .header("webhook-signature", signatures)
        .body(envelope.body.clone());
    if let Some(content_type) = &envelope.content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }
    let started = Instant::now();
    let result = request.send().await;
    app.metrics.outbound.record(started.elapsed());
    Ok(result?.status())
}
