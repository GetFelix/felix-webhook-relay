//! The admin JSON API: sources and endpoints in the `config` cache, with
//! their secrets sealed before they are written.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use felix_relay_core::catalog::{
    Endpoint, EventIdFrom, PreviousSecret, ROTATION_OVERLAP_MS, Source, endpoint_key, source_key,
    valid_id,
};
use felix_relay_core::secret::random_bytes;
use felix_relay_core::signature::{Scheme, new_standard_secret};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::catalog::CONFIG;
use crate::{App, unix_millis};

pub(crate) fn routes() -> axum::Router<Arc<App>> {
    axum::Router::new()
        .route(
            "/api/{tenant}/sources/{id}",
            put(put_source).get(get_source).delete(delete_source),
        )
        .route(
            "/api/{tenant}/endpoints/{id}",
            put(put_endpoint).get(get_endpoint).delete(delete_endpoint),
        )
        .route("/api/{tenant}/endpoints/{id}/secret", post(rotate_secret))
        .route("/api/{tenant}/endpoints/{id}/enable", post(enable_endpoint))
}

/// A failed admin request: a status and a message for the operator.
pub(crate) struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        tracing::error!("admin request failed: {err:#}");
        Self(StatusCode::SERVICE_UNAVAILABLE, format!("{err:#}"))
    }
}

type ApiResult = Result<Response, ApiError>;

fn bad_request(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.into())
}

fn not_found() -> ApiError {
    ApiError(StatusCode::NOT_FOUND, "not found".to_string())
}

/// Checks the tenant and the id, and returns the entry's cache key.
fn entry_key(
    app: &App,
    tenant: &str,
    id: &str,
    key: fn(&str) -> String,
) -> Result<String, ApiError> {
    if tenant != app.config.tenant {
        return Err(not_found());
    }
    if !valid_id(id) {
        return Err(bad_request(
            "ids are 1 to 64 lowercase letters, digits, - and _",
        ));
    }
    Ok(key(id))
}

async fn read<T: DeserializeOwned>(app: &App, key: &str) -> Result<Option<T>, ApiError> {
    let Some(bytes) = app.felix.cache_get(CONFIG, key).await? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(&bytes)
        .map_err(|err| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{key}: {err}")))?;
    Ok(Some(value))
}

async fn write(app: &App, key: &str, value: &impl serde::Serialize) -> Result<(), ApiError> {
    let bytes = serde_json::to_vec(value).expect("config serializes");
    app.felix.cache_put(CONFIG, key, bytes, None).await?;
    Ok(())
}

/// An entry as the API shows it: everything but its secrets.
fn shown(id: &str, entry: &impl serde::Serialize) -> Value {
    let mut value = serde_json::to_value(entry).expect("config serializes");
    let object = value.as_object_mut().expect("entries are objects");
    object.remove("secret");
    if let Some(previous) = object.remove("previous_secret") {
        object.insert("rotating_until".to_string(), previous["until"].clone());
    }
    object.insert("id".to_string(), json!(id));
    value
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceInput {
    scheme: Scheme,
    /// Kept from the existing source when absent, or made up for a new one.
    secret: Option<String>,
    event_id: Option<EventIdFrom>,
    event_type_header: Option<String>,
    #[serde(default)]
    keep_headers: Vec<String>,
}

async fn put_source(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
    Json(input): Json<SourceInput>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, source_key)?;
    let existing: Option<Source> = read(&app, &key).await?;
    let (secret, generated) = match (input.secret, existing) {
        (Some(secret), _) => (app.config.secret_key.seal(&key, &secret), None),
        (None, Some(existing)) => (existing.secret, None),
        (None, None) => {
            let secret = match input.scheme {
                Scheme::StandardWebhooks => new_standard_secret(random_bytes()),
                _ => hex::encode(random_bytes()),
            };
            (app.config.secret_key.seal(&key, &secret), Some(secret))
        }
    };
    let plain = app
        .config
        .secret_key
        .open(&key, &secret)
        .map_err(anyhow::Error::from)?;
    input
        .scheme
        .check_secret(&plain)
        .map_err(|err| bad_request(err.to_string()))?;
    let source = Source {
        scheme: input.scheme,
        secret,
        event_id: input.event_id,
        event_type_header: input.event_type_header.map(|h| h.to_ascii_lowercase()),
        keep_headers: input
            .keep_headers
            .iter()
            .map(|h| h.to_ascii_lowercase())
            .collect(),
    };
    write(&app, &key, &source).await?;
    let mut shown = shown(&id, &source);
    if let Some(secret) = generated {
        shown["secret"] = json!(secret);
    }
    Ok(Json(shown).into_response())
}

async fn get_source(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, source_key)?;
    let source: Source = read(&app, &key).await?.ok_or_else(not_found)?;
    Ok(Json(shown(&id, &source)).into_response())
}

async fn delete_source(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, source_key)?;
    app.felix.cache_delete(CONFIG, &key).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointInput {
    source: String,
    url: String,
    /// Kept from the existing endpoint when absent, or made up for a new one.
    secret: Option<String>,
}

async fn put_endpoint(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
    Json(input): Json<EndpointInput>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, endpoint_key)?;
    if !valid_id(&input.source) {
        return Err(bad_request("source is not a valid id"));
    }
    match reqwest::Url::parse(&input.url) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => {}
        _ => return Err(bad_request("url must be an http or https URL")),
    }
    let existing: Option<Endpoint> = read(&app, &key).await?;
    let disabled = existing.as_ref().and_then(|e| e.disabled.clone());
    let mut generated = None;
    let (secret, previous_secret) = match (input.secret, existing) {
        (Some(secret), existing) => {
            Scheme::StandardWebhooks
                .check_secret(&secret)
                .map_err(|err| bad_request(err.to_string()))?;
            let previous = existing.and_then(|e| e.previous_secret);
            (app.config.secret_key.seal(&key, &secret), previous)
        }
        (None, Some(existing)) => (existing.secret, existing.previous_secret),
        (None, None) => {
            let secret = new_standard_secret(random_bytes());
            let sealed = app.config.secret_key.seal(&key, &secret);
            generated = Some(secret);
            (sealed, None)
        }
    };
    let endpoint = Endpoint {
        source: input.source,
        url: input.url,
        secret,
        previous_secret,
        disabled,
    };
    write(&app, &key, &endpoint).await?;
    let mut shown = shown(&id, &endpoint);
    if let Some(secret) = generated {
        shown["secret"] = json!(secret);
    }
    Ok(Json(shown).into_response())
}

async fn get_endpoint(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, endpoint_key)?;
    let endpoint: Endpoint = read(&app, &key).await?.ok_or_else(not_found)?;
    Ok(Json(shown(&id, &endpoint)).into_response())
}

async fn delete_endpoint(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, endpoint_key)?;
    app.felix.cache_delete(CONFIG, &key).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SecretInput {
    secret: Option<String>,
}

/// Replace an endpoint's signing secret. The old one keeps signing beside
/// it for a day, which Standard Webhooks allows, so receivers can switch
/// over without rejecting anything.
async fn rotate_secret(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
    input: Option<Json<SecretInput>>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, endpoint_key)?;
    let mut endpoint: Endpoint = read(&app, &key).await?.ok_or_else(not_found)?;
    let secret = match input.and_then(|Json(input)| input.secret) {
        Some(secret) => {
            Scheme::StandardWebhooks
                .check_secret(&secret)
                .map_err(|err| bad_request(err.to_string()))?;
            secret
        }
        None => new_standard_secret(random_bytes()),
    };
    let until = unix_millis() + ROTATION_OVERLAP_MS;
    endpoint.previous_secret = Some(PreviousSecret {
        secret: std::mem::replace(
            &mut endpoint.secret,
            app.config.secret_key.seal(&key, &secret),
        ),
        until,
    });
    write(&app, &key, &endpoint).await?;
    Ok(Json(json!({ "id": id, "secret": secret, "rotating_until": until })).into_response())
}

/// Clear a disabled endpoint. Its worker resumes from the group's cursor.
async fn enable_endpoint(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, endpoint_key)?;
    let mut endpoint: Endpoint = read(&app, &key).await?.ok_or_else(not_found)?;
    endpoint.disabled = None;
    write(&app, &key, &endpoint).await?;
    Ok(Json(shown(&id, &endpoint)).into_response())
}
