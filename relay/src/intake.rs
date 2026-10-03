//! `POST /in/{tenant}/{source}`: wrap the request in an envelope, append it,
//! and answer only once Felix has acknowledged it.

use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use felix_relay_core::Envelope;
use serde_json::json;

use crate::{App, unix_millis};

/// Larger bodies are refused with `413`. Well above what the common senders
/// send, and far below Felix's 16 MiB frame limit.
const MAX_BODY_BYTES: usize = 1024 * 1024;

pub(crate) fn routes() -> axum::Router<Arc<App>> {
    axum::Router::new()
        .route("/in/{tenant}/{source}", post(accept))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
}

async fn accept(
    State(app): State<Arc<App>>,
    Path((tenant, source)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let config = &app.config;
    if tenant != config.tenant || source != config.source {
        return StatusCode::NOT_FOUND.into_response();
    }
    let felix = app.felix.as_ref().expect("intake runs with Felix");

    let header_value = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    let envelope = Envelope {
        id: None,
        received_at: unix_millis(),
        event_type: config.event_type_header.as_deref().and_then(header_value),
        content_type: header_value(header::CONTENT_TYPE.as_str()),
        headers: headers
            .iter()
            .filter(|(name, _)| config.keep_headers.iter().any(|keep| keep == name.as_str()))
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect(),
        body: body.to_vec(),
    };

    let started = Instant::now();
    match felix
        .append(config.source_stream(), envelope.encode())
        .await
    {
        Ok(offset) => {
            app.metrics.intake_ack.record(started.elapsed());
            let id = envelope.event_id(&config.source, offset);
            (
                StatusCode::ACCEPTED,
                Json(json!({ "id": id, "offset": offset })),
            )
                .into_response()
        }
        Err(err) => {
            tracing::error!(%source, "could not store a webhook: {err:#}");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}
