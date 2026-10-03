//! `POST /in/{tenant}/{source}`: verify the request, wrap it in an envelope,
//! append it, and answer only once Felix has acknowledged it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use felix_relay_core::Envelope;
use felix_relay_core::catalog::source_key;
use serde_json::json;

use crate::{App, unix_millis};

/// Larger bodies are refused with `413`. Well above what the common senders
/// send, and far below Felix's 16 MiB frame limit.
const MAX_BODY_BYTES: usize = 1024 * 1024;
const IDEM: &str = "idem";
/// How long intake remembers a sender's event id. Senders give up retrying
/// well within a day.
const IDEM_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub(crate) fn routes() -> axum::Router<Arc<App>> {
    axum::Router::new()
        .route("/in/{tenant}/{source}", post(signed))
        // The token is a secret. Nothing in the relay logs request paths, and
        // nothing may start to: the tests check that it never reaches the log.
        .route("/in/{tenant}/{source}/{token}", post(with_token))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
}

async fn signed(
    State(app): State<Arc<App>>,
    Path((tenant, source)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    accept(app, tenant, source, None, headers, body).await
}

async fn with_token(
    State(app): State<Arc<App>>,
    Path((tenant, source, token)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    accept(app, tenant, source, Some(token), headers, body).await
}

async fn accept(
    app: Arc<App>,
    tenant: String,
    source_id: String,
    token: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some((tenant, source)) = app.tenants.get(&tenant).and_then(|tenant| {
        let source = tenant.catalog.borrow().sources.get(&source_id).cloned()?;
        Some((tenant, source))
    }) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let felix = &tenant.felix;
    let header_value = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());

    let secret = match app
        .config
        .secret_key
        .open(&source_key(&source_id), &source.secret)
    {
        Ok(secret) => secret,
        Err(err) => {
            tracing::error!(source = %source_id, "{err}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let now = unix_millis();
    if let Err(refusal) =
        source
            .scheme
            .verify(&secret, header_value, token.as_deref(), &body, now / 1000)
    {
        tracing::info!(tenant = %tenant.name, source = %source_id, "refused: {refusal}");
        return (StatusCode::UNAUTHORIZED, refusal.to_string()).into_response();
    }

    let envelope = Envelope {
        id: source
            .event_id
            .as_ref()
            .and_then(|from| from.extract(header_value, &body)),
        received_at: now,
        event_type: source
            .event_type_header
            .as_deref()
            .and_then(header_value)
            .map(str::to_string),
        content_type: header_value(header::CONTENT_TYPE.as_str()).map(str::to_string),
        headers: headers
            .iter()
            .filter(|(name, _)| source.keep_headers.iter().any(|keep| keep == name.as_str()))
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect(),
        body: body.to_vec(),
    };

    // No conditional put in Felix, so two copies arriving at once can both
    // miss here and both be stored. They carry the same id, which the
    // receiver dedupes on; the check is for the common case, a retry after a
    // timeout.
    let idem_key = envelope.id.as_ref().map(|id| format!("{source_id}/{id}"));
    if let Some(key) = &idem_key {
        match felix.cache_get(IDEM, key).await {
            Ok(Some(offset)) => {
                let offset = String::from_utf8_lossy(&offset).parse::<u64>().ok();
                return (
                    StatusCode::OK,
                    Json(json!({ "id": envelope.id, "offset": offset })),
                )
                    .into_response();
            }
            Ok(None) => {}
            Err(err) => tracing::warn!("idempotency check skipped: {err:#}"),
        }
    }

    let started = Instant::now();
    let offset = match felix
        .append(format!("src.{source_id}"), envelope.encode())
        .await
    {
        Ok(offset) => offset,
        Err(err) => {
            tracing::error!(tenant = %tenant.name, source = %source_id, "could not store a webhook: {err:#}");
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    app.metrics.intake_ack.record(started.elapsed());
    felix.count(format!("received/{source_id}"));
    if let Some(key) = &idem_key
        && let Err(err) = felix
            .cache_put(IDEM, key, offset.to_string().into_bytes(), Some(IDEM_TTL))
            .await
    {
        tracing::warn!("could not record an idempotency key: {err:#}");
    }
    let id = envelope.event_id(&source_id, offset);
    (
        StatusCode::ACCEPTED,
        Json(json!({ "id": id, "offset": offset })),
    )
        .into_response()
}
