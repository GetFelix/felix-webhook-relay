//! The admin JSON API: sources and endpoints in the `config` cache, with
//! their secrets sealed before they are written.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use felix_relay_core::catalog::{
    DEFAULT_WINDOW, Endpoint, EventIdFrom, Mode, PreviousSecret, ROTATION_OVERLAP_MS, Source,
    endpoint_key, source_key, valid_id,
};
use felix_relay_core::secret::random_bytes;
use felix_relay_core::signature::{Scheme, new_standard_secret};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::auth::{self, MANAGE, Refused};
use crate::catalog::CONFIG;
use crate::session::{self, Denied, Identity};
use crate::tenant::Tenant;
use crate::{App, unix_millis};

mod jobs;
mod page;

pub(crate) fn routes() -> axum::Router<Arc<App>> {
    axum::Router::new()
        .merge(jobs::routes())
        .merge(page::routes())
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

/// The tenant an admin request is about, from the `{tenant}` in its path,
/// opened with the admin's own token, and who the admin is.
pub(crate) struct Admin(pub(crate) Arc<Tenant>, pub(crate) Identity);

impl FromRequestParts<Arc<App>> for Admin {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &Arc<App>) -> Result<Self, ApiError> {
        let Path(params) = Path::<HashMap<String, String>>::from_request_parts(parts, app)
            .await
            .map_err(|_| not_found())?;
        let tenant = params.get("tenant").ok_or_else(not_found)?;
        match session::admin_tenant(app, &parts.headers, tenant).await {
            Ok((tenant, who)) => Ok(Admin(tenant, who)),
            Err(Denied::SignedOut) => Err(ApiError(
                StatusCode::UNAUTHORIZED,
                "sign in at /auth/login, or send an ID token as a bearer token".to_string(),
            )),
            Err(Denied::Forbidden(reason)) => Err(ApiError(StatusCode::FORBIDDEN, reason)),
            Err(Denied::Unavailable(err)) => Err(err.into()),
        }
    }
}

/// Checks the id, and returns the entry's cache key.
fn entry_key(id: &str, key: fn(&str) -> String) -> Result<String, ApiError> {
    if !valid_id(id) {
        return Err(bad_request(
            "ids are 1 to 64 lowercase letters, digits, - and _",
        ));
    }
    Ok(key(id))
}

async fn read<T: DeserializeOwned>(tenant: &Tenant, key: &str) -> Result<Option<T>, ApiError> {
    read_from(tenant, CONFIG, key).await
}

async fn read_from<T: DeserializeOwned>(
    tenant: &Tenant,
    cache: &str,
    key: &str,
) -> Result<Option<T>, ApiError> {
    let Some(bytes) = tenant.felix.cache_get(cache, key).await? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(&bytes)
        .map_err(|err| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{key}: {err}")))?;
    Ok(Some(value))
}

async fn write(tenant: &Tenant, key: &str, value: &impl serde::Serialize) -> Result<(), ApiError> {
    let bytes = serde_json::to_vec(value).expect("config serializes");
    tenant.felix.cache_put(CONFIG, key, bytes, None).await?;
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
    Admin(tenant, who): Admin,
    Path((_, id)): Path<(String, String)>,
    Json(input): Json<SourceInput>,
) -> ApiResult {
    let key = entry_key(&id, source_key)?;
    let existing: Option<Source> = read(&tenant, &key).await?;
    if existing.is_none() {
        create_stream(&app, &who, &tenant, &format!("src.{id}")).await?;
    }
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
    write(&tenant, &key, &source).await?;
    let mut shown = shown(&id, &source);
    if let Some(secret) = generated {
        shown["secret"] = json!(secret);
    }
    Ok(Json(shown).into_response())
}

/// Create a source's stream through the control plane, with the admin's
/// token, and wait until the broker serves it. Felix creates streams only
/// there; a client cannot.
async fn create_stream(
    app: &App,
    who: &Identity,
    tenant: &Tenant,
    stream: &str,
) -> Result<(), ApiError> {
    let config = &app.config;
    let http = reqwest::Client::new();
    let token = auth::exchange(
        &http,
        config,
        &who.id_token,
        &tenant.name,
        &MANAGE,
        "felix-controlplane",
    )
    .await
    .map_err(|err| match err.downcast::<Refused>() {
        Ok(refused) => ApiError(StatusCode::FORBIDDEN, refused.to_string()),
        Err(err) => err.into(),
    })?;
    let url = format!(
        "{}/v1/tenants/{}/namespaces/{}/streams",
        config.control_plane, config.felix_tenant, tenant.name
    );
    let response = http
        .post(&url)
        .bearer_auth(token)
        .json(&json!({
            "stream": stream,
            "kind": "Stream",
            "shards": 1,
            "replication_factor": config.stream_replicas,
            "retention": { "max_age_seconds": null, "max_size_bytes": null },
            "consistency": if config.stream_replicas > 1 { "Quorum" } else { "Leader" },
            "delivery": "AtLeastOnce",
            "durable": true
        }))
        .send()
        .await
        .map_err(anyhow::Error::from)?;
    let status = response.status();
    if !status.is_success() && status != StatusCode::CONFLICT {
        let body = response.text().await.unwrap_or_default();
        return Err(ApiError(status, format!("create {stream}: {body}")));
    }
    // Brokers learn of new streams on their next sync with the control plane.
    for _ in 0..100 {
        if tenant.felix.bounds(stream).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err(ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        format!("{stream} was created but no broker serves it yet"),
    ))
}

async fn get_source(Admin(tenant, _): Admin, Path((_, id)): Path<(String, String)>) -> ApiResult {
    let key = entry_key(&id, source_key)?;
    let source: Source = read(&tenant, &key).await?.ok_or_else(not_found)?;
    Ok(Json(shown(&id, &source)).into_response())
}

async fn delete_source(
    Admin(tenant, _): Admin,
    Path((_, id)): Path<(String, String)>,
) -> ApiResult {
    let key = entry_key(&id, source_key)?;
    tenant.felix.cache_delete(CONFIG, &key).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointInput {
    source: String,
    url: String,
    /// Kept from the existing endpoint when absent, or made up for a new one.
    secret: Option<String>,
    #[serde(default)]
    mode: Mode,
    window: Option<u32>,
    #[serde(default)]
    event_types: Vec<String>,
    /// For a new endpoint: start from the beginning of the source's log
    /// rather than from its tail.
    #[serde(default)]
    backfill: bool,
}

async fn put_endpoint(
    State(app): State<Arc<App>>,
    Admin(tenant, _): Admin,
    Path((_, id)): Path<(String, String)>,
    Json(input): Json<EndpointInput>,
) -> ApiResult {
    let key = entry_key(&id, endpoint_key)?;
    if !valid_id(&input.source) {
        return Err(bad_request("source is not a valid id"));
    }
    match reqwest::Url::parse(&input.url) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => {}
        _ => return Err(bad_request("url must be an http or https URL")),
    }
    let existing: Option<Endpoint> = read(&tenant, &key).await?;
    let disabled = existing.as_ref().and_then(|e| e.disabled.clone());
    let start_offset = match &existing {
        Some(existing) if existing.source == input.source => existing.start_offset,
        _ if input.backfill => 0,
        _ => source_tail(&tenant, &input.source).await?,
    };
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
        mode: input.mode,
        window: input.window.unwrap_or(DEFAULT_WINDOW).clamp(1, 256),
        event_types: input.event_types,
        start_offset,
        disabled,
    };
    write(&tenant, &key, &endpoint).await?;
    let mut shown = shown(&id, &endpoint);
    if let Some(secret) = generated {
        shown["secret"] = json!(secret);
    }
    Ok(Json(shown).into_response())
}

/// The offset the next webhook to the source will get. A new endpoint starts
/// there, because a new Felix group starts at the beginning of the log and
/// cannot be told otherwise.
async fn source_tail(tenant: &Tenant, source: &str) -> Result<u64, ApiError> {
    let (_, tail) = tenant
        .felix
        .bounds(&format!("src.{source}"))
        .await
        .map_err(|err| bad_request(format!("source {source} has no stream: {err:#}")))?;
    Ok(tail)
}

async fn get_endpoint(Admin(tenant, _): Admin, Path((_, id)): Path<(String, String)>) -> ApiResult {
    let key = entry_key(&id, endpoint_key)?;
    let endpoint: Endpoint = read(&tenant, &key).await?.ok_or_else(not_found)?;
    Ok(Json(shown(&id, &endpoint)).into_response())
}

async fn delete_endpoint(
    Admin(tenant, _): Admin,
    Path((_, id)): Path<(String, String)>,
) -> ApiResult {
    let key = entry_key(&id, endpoint_key)?;
    tenant.felix.cache_delete(CONFIG, &key).await?;
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
    Admin(tenant, _): Admin,
    Path((_, id)): Path<(String, String)>,
    input: Option<Json<SecretInput>>,
) -> ApiResult {
    let key = entry_key(&id, endpoint_key)?;
    let mut endpoint: Endpoint = read(&tenant, &key).await?.ok_or_else(not_found)?;
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
    write(&tenant, &key, &endpoint).await?;
    Ok(Json(json!({ "id": id, "secret": secret, "rotating_until": until })).into_response())
}

/// Clear a disabled endpoint. Its worker resumes from the group's cursor.
async fn enable_endpoint(
    Admin(tenant, _): Admin,
    Path((_, id)): Path<(String, String)>,
) -> ApiResult {
    let key = entry_key(&id, endpoint_key)?;
    let mut endpoint: Endpoint = read(&tenant, &key).await?.ok_or_else(not_found)?;
    endpoint.disabled = None;
    write(&tenant, &key, &endpoint).await?;
    Ok(Json(shown(&id, &endpoint)).into_response())
}
