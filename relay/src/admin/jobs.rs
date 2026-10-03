//! Replays, dead letters and redrives, and finding an offset by time.

use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use felix_relay_core::Envelope;
use felix_relay_core::catalog::{Endpoint, endpoint_key, source_key};
use felix_relay_core::jobs::{
    DeadMark, DeadStatus, Job, JobKind, JobStatus, OffsetSearch, SEARCH_MARGIN_MS,
};
use felix_relay_core::records::{Attempt, DeadLetter};
use felix_relay_core::secret::random_bytes;
use serde::Deserialize;
use serde_json::json;

use super::{Admin, ApiError, ApiResult, bad_request, entry_key, not_found, read};
use crate::catalog::STATE;
use crate::deliver::read_dead_letter;
use crate::felix::Felix;
use crate::tenant::Tenant;
use crate::{App, unix_millis};

/// How many of the newest dead letters the API lists.
const DEAD_LISTED: u64 = 200;

pub(super) fn routes() -> axum::Router<Arc<App>> {
    axum::Router::new()
        .route("/api/{tenant}/sources/{id}/offset", get(offset_at))
        .route("/api/{tenant}/endpoints/{id}/replays", post(start_replay))
        .route("/api/{tenant}/jobs/{id}", get(get_job))
        .route("/api/{tenant}/endpoints/{id}/retry", post(retry_now))
        .route("/api/{tenant}/events/{source}/{offset}", get(event))
        .route("/api/{tenant}/dead", get(list_dead))
        .route("/api/{tenant}/dead/{offset}/{action}", post(act_on_dead))
        .route(
            "/api/{tenant}/endpoints/{id}/broker-dead/{offset}/{action}",
            post(act_on_broker_dead),
        )
}

/// The first offset of `stream` received at or after `target` (Unix
/// milliseconds), found by bisecting the log on `received_at`.
async fn offset_for_time(felix: &Felix, stream: &str, target: u64) -> anyhow::Result<OffsetSearch> {
    let (oldest, tail) = felix.bounds(stream).await?;
    let mut search = OffsetSearch::new(oldest, tail, target);
    while let Some(probe) = search.next() {
        let found = felix
            .read(stream, probe, tail, 1)
            .await?
            .into_iter()
            .next()
            .and_then(|(offset, payload)| {
                Some((offset, Envelope::decode(&payload).ok()?.received_at))
            });
        search.observe(probe, found);
    }
    Ok(search)
}

#[derive(Deserialize)]
struct At {
    /// Unix milliseconds.
    at: u64,
}

async fn offset_at(
    Admin(tenant, _): Admin,
    Path((_, id)): Path<(String, String)>,
    Query(At { at }): Query<At>,
) -> ApiResult {
    entry_key(&id, source_key)?;
    let started = Instant::now();
    let search = offset_for_time(&tenant.felix, &format!("src.{id}"), at).await?;
    Ok(Json(json!({
        "offset": search.result(),
        "reads": search.probes,
        "millis": started.elapsed().as_millis(),
    }))
    .into_response())
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ReplayInput {
    /// Unix milliseconds; records received at or after it.
    since: Option<u64>,
    /// Unix milliseconds; records received before it.
    until: Option<u64>,
    /// Offsets, instead of or as well as times.
    from: Option<u64>,
    to: Option<u64>,
    #[serde(default)]
    pause_live: bool,
}

fn new_job_id(kind: &str) -> String {
    format!(
        "{kind}-{}-{}",
        unix_millis(),
        hex::encode(&random_bytes()[..4])
    )
}

async fn save_job(tenant: &Tenant, id: &str, job: &Job) -> Result<(), ApiError> {
    let json = serde_json::to_vec(job).expect("a job serializes");
    tenant
        .felix
        .cache_put(STATE, &format!("job/{id}"), json, None)
        .await?;
    Ok(())
}

async fn start_replay(
    Admin(tenant, _): Admin,
    Path((_, id)): Path<(String, String)>,
    input: Option<Json<ReplayInput>>,
) -> ApiResult {
    let key = entry_key(&id, endpoint_key)?;
    let endpoint: Endpoint = read(&tenant, &key).await?.ok_or_else(not_found)?;
    let input = input.map(|Json(input)| input).unwrap_or_default();
    let stream = format!("src.{}", endpoint.source);
    let (oldest, tail) = tenant.felix.bounds(&stream).await?;
    // Aim early and late by the margin; the worker filters by `received_at`.
    let from = match (input.from, input.since) {
        (Some(from), _) => from,
        (None, Some(since)) => {
            let target = since.saturating_sub(SEARCH_MARGIN_MS);
            offset_for_time(&tenant.felix, &stream, target)
                .await?
                .result()
        }
        (None, None) => oldest,
    };
    let to = match (input.to, input.until) {
        (Some(to), _) => to,
        (None, Some(until)) => {
            let target = until.saturating_add(SEARCH_MARGIN_MS);
            offset_for_time(&tenant.felix, &stream, target)
                .await?
                .result()
        }
        (None, None) => tail,
    };
    if from < oldest {
        return Err(bad_request(format!(
            "offset {from} is below the oldest record the source still holds, {oldest}"
        )));
    }
    let now = unix_millis();
    let job = Job {
        endpoint: id,
        kind: JobKind::Replay {
            from,
            to,
            next: from,
            since: input.since,
            until: input.until,
            pause_live: input.pause_live,
        },
        status: JobStatus::Pending,
        sent: 0,
        error: None,
        created_at: now,
        updated_at: now,
    };
    let job_id = new_job_id("replay");
    save_job(&tenant, &job_id, &job).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "id": job_id, "from": from, "to": to })),
    )
        .into_response())
}

/// Ask a paused endpoint's worker to skip the rest of its backoff wait.
async fn retry_now(Admin(tenant, _): Admin, Path((_, id)): Path<(String, String)>) -> ApiResult {
    let key = entry_key(&id, endpoint_key)?;
    let _: Endpoint = read(&tenant, &key).await?.ok_or_else(not_found)?;
    let now = unix_millis();
    let job = Job {
        endpoint: id,
        kind: JobKind::Retry,
        status: JobStatus::Pending,
        sent: 0,
        error: None,
        created_at: now,
        updated_at: now,
    };
    let job_id = new_job_id("retry");
    save_job(&tenant, &job_id, &job).await?;
    Ok((StatusCode::ACCEPTED, Json(json!({ "id": job_id }))).into_response())
}

/// How far back the event view looks for attempts. The trail has no index,
/// so this is a scan of its newest records.
const ATTEMPTS_SCANNED: u64 = 2_000;

/// One event: its envelope as stored, and its recent attempts.
async fn event(
    Admin(tenant, _): Admin,
    Path((_, source, offset)): Path<(String, String, u64)>,
) -> ApiResult {
    entry_key(&source, source_key)?;
    let felix = &tenant.felix;
    let (_, payload) = felix
        .read(&format!("src.{source}"), offset, offset + 1, 1)
        .await?
        .pop()
        .ok_or_else(not_found)?;
    let envelope = Envelope::decode(&payload).map_err(|err| bad_request(err.to_string()))?;
    let endpoints: Vec<String> = tenant
        .catalog
        .borrow()
        .endpoints
        .iter()
        .filter(|(_, endpoint)| endpoint.source == source)
        .map(|(id, _)| id.clone())
        .collect();
    let (oldest, tail) = felix.bounds("attempts").await?;
    let from = oldest.max(tail.saturating_sub(ATTEMPTS_SCANNED));
    let attempts: Vec<Attempt> = felix
        .read("attempts", from, tail, ATTEMPTS_SCANNED as usize)
        .await?
        .into_iter()
        .filter_map(|(_, payload)| Attempt::decode(&payload).ok())
        .filter(|a| a.offset == offset && endpoints.contains(&a.endpoint))
        .collect();
    Ok(Json(json!({
        "source": source,
        "offset": offset,
        "id": envelope.event_id(&source, offset),
        "received_at": envelope.received_at,
        "event_type": envelope.event_type,
        "content_type": envelope.content_type,
        "headers": envelope.headers,
        "body": String::from_utf8_lossy(&envelope.body),
        "attempts": attempts.iter().map(|a| json!({
            "endpoint": a.endpoint,
            "at": a.at,
            "millis": a.millis,
            "status": a.status,
            "detail": a.detail,
        })).collect::<Vec<_>>(),
    }))
    .into_response())
}

async fn get_job(Admin(tenant, _): Admin, Path((_, id)): Path<(String, String)>) -> ApiResult {
    let job: Job = super::read_from(&tenant, STATE, &format!("job/{id}"))
        .await?
        .ok_or_else(not_found)?;
    Ok(Json(job).into_response())
}

/// Both kinds of dead letter: the relay's own, newest first, with what has
/// become of each, and the ones Felix holds for each endpoint's group.
async fn list_dead(Admin(tenant, _): Admin) -> ApiResult {
    Ok(Json(dead_letters(&tenant).await?).into_response())
}

/// `{"relay": [...], "broker": [...]}`: the newest relay dead letters with
/// their marks, and Felix's group dead letters for every endpoint.
pub(super) async fn dead_letters(tenant: &Tenant) -> Result<serde_json::Value, ApiError> {
    let felix = &tenant.felix;
    let (oldest, tail) = felix.bounds("dead").await?;
    let mut relay = Vec::new();
    for (offset, payload) in felix
        .read(
            "dead",
            oldest.max(tail.saturating_sub(DEAD_LISTED)),
            tail,
            DEAD_LISTED as usize,
        )
        .await?
        .into_iter()
        .rev()
    {
        let Ok(dead) = DeadLetter::decode(&payload) else {
            continue;
        };
        let mark: Option<DeadMark> =
            super::read_from(tenant, STATE, &format!("dead/{offset}")).await?;
        relay.push(json!({
            "offset": offset,
            "endpoint": dead.endpoint,
            "source": dead.source,
            "source_offset": dead.offset,
            "event_id": dead.envelope.event_id(&dead.source, dead.offset),
            "at": dead.at,
            "attempts": dead.attempts,
            "last_status": dead.last_status,
            "last_response": dead.last_response,
            "mark": mark,
        }));
    }
    let endpoints: Vec<(String, Endpoint)> = tenant
        .catalog
        .borrow()
        .endpoints
        .iter()
        .map(|(id, endpoint)| (id.clone(), endpoint.clone()))
        .collect();
    let mut broker = Vec::new();
    for (id, endpoint) in endpoints {
        let offsets = felix
            .client()
            .group_dead_letters(
                &felix.tenant,
                &felix.namespace,
                &format!("src.{}", endpoint.source),
                0,
                &format!("ep.{id}"),
            )
            .await;
        // A group that has never been polled is not an error worth failing
        // the whole list over.
        for offset in offsets.unwrap_or_default() {
            broker.push(json!({ "endpoint": id, "source": endpoint.source, "offset": offset }));
        }
    }
    Ok(json!({ "relay": relay, "broker": broker }))
}

async fn act_on_dead(
    Admin(tenant, _): Admin,
    Path((_, offset, action)): Path<(String, u64, String)>,
) -> ApiResult {
    let dead = read_dead_letter(&tenant.felix, offset)
        .await
        .map_err(|_| not_found())?;
    let mut answer = json!({ "offset": offset });
    let mark = match action.as_str() {
        "redrive" => {
            let now = unix_millis();
            let job = Job {
                endpoint: dead.endpoint,
                kind: JobKind::Redrive {
                    dead_offset: offset,
                },
                status: JobStatus::Pending,
                sent: 0,
                error: None,
                created_at: now,
                updated_at: now,
            };
            let job_id = new_job_id("redrive");
            save_job(&tenant, &job_id, &job).await?;
            answer["job"] = json!(job_id);
            DeadMark {
                status: DeadStatus::Redriving,
                at: now,
                job: Some(job_id),
            }
        }
        "discard" => DeadMark {
            status: DeadStatus::Discarded,
            at: unix_millis(),
            job: None,
        },
        _ => return Err(not_found()),
    };
    let json = serde_json::to_vec(&mark).expect("a mark serializes");
    tenant
        .felix
        .cache_put(STATE, &format!("dead/{offset}"), json, None)
        .await?;
    answer["mark"] = json!(mark);
    Ok(Json(answer).into_response())
}

/// A record Felix dead-lettered for an endpoint's group after it was claimed
/// too many times: put it back in play, or drop it.
async fn act_on_broker_dead(
    Admin(tenant, _): Admin,
    Path((_, id, offset, action)): Path<(String, String, u64, String)>,
) -> ApiResult {
    let key = entry_key(&id, endpoint_key)?;
    let endpoint: Endpoint = read(&tenant, &key).await?.ok_or_else(not_found)?;
    let felix = &tenant.felix;
    let stream = format!("src.{}", endpoint.source);
    let group = format!("ep.{id}");
    let client = felix.client();
    let done = match action.as_str() {
        "redrive" => {
            client
                .group_redrive(&felix.tenant, &felix.namespace, &stream, 0, &group, offset)
                .await
        }
        "discard" => {
            client
                .group_discard(&felix.tenant, &felix.namespace, &stream, 0, &group, offset)
                .await
        }
        _ => return Err(not_found()),
    };
    done.map_err(|err| ApiError(StatusCode::CONFLICT, format!("{err:#}")))?;
    Ok(Json(json!({ "endpoint": id, "offset": offset, "done": action })).into_response())
}
