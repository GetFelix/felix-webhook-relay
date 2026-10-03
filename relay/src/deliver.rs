//! The delivery task for one endpoint: poll its group, send each record in
//! order, acknowledge it once the endpoint answers `2xx`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_relay_core::Envelope;
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
    let felix = app.felix.as_ref().context("deliver runs with Felix")?;
    let config = &app.config;
    let url = config.endpoint_url.as_deref().context("no endpoint URL")?;
    let stream = config.source_stream();
    let group = config.endpoint_group();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    tracing::info!(%stream, %group, %url, "delivering");

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
                    let id = envelope.event_id(&config.source, record.offset);
                    send_until_accepted(&app, &http, url, &id, &envelope).await;
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

/// Send one event until the endpoint answers `2xx`. Retries are a fixed
/// pause for now; backoff and dead letters replace them.
async fn send_until_accepted(
    app: &App,
    http: &reqwest::Client,
    url: &str,
    id: &str,
    envelope: &Envelope,
) {
    loop {
        let mut request = http
            .post(url)
            .header("webhook-id", id)
            .body(envelope.body.clone());
        if let Some(content_type) = &envelope.content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        let started = Instant::now();
        let result = request.send().await;
        app.metrics.outbound.record(started.elapsed());
        match result {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => tracing::warn!(%id, status = %response.status(), "endpoint refused"),
            Err(err) => tracing::warn!(%id, "request failed: {err}"),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}
