//! Replays, dead letters and redrives, and finding an offset by time.

use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use felix_relay_core::Envelope;
use felix_relay_core::catalog::{Endpoint, endpoint_key, source_key};
use felix_relay_core::jobs::{
    DeadMark, DeadStatus, Job, JobKind, JobStatus, OffsetSearch, SEARCH_MARGIN_MS,
};
use felix_relay_core::records::DeadLetter;
use felix_relay_core::secret::random_bytes;
use serde::Deserialize;
use serde_json::json;

use super::{ApiError, ApiResult, bad_request, entry_key, not_found, read};
use crate::catalog::STATE;
use crate::deliver::read_dead_letter;
use crate::felix::Felix;
use crate::{App, unix_millis};

/// How many of the newest dead letters the API lists.
const DEAD_LISTED: u64 = 200;

pub(super) fn routes() -> axum::Router<Arc<App>> {
    axum::Router::new()
        .route("/api/{tenant}/sources/{id}/offset", get(offset_at))
        .route("/api/{tenant}/endpoints/{id}/replays", post(start_replay))
        .route("/api/{tenant}/jobs/{id}", get(get_job))
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
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
    Query(At { at }): Query<At>,
) -> ApiResult {
    entry_key(&app, &tenant, &id, source_key)?;
    let started = Instant::now();
    let search = offset_for_time(&app.felix, &format!("src.{id}"), at).await?;
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

async fn save_job(app: &App, id: &str, job: &Job) -> Result<(), ApiError> {
    let json = serde_json::to_vec(job).expect("a job serializes");
    app.felix
        .cache_put(STATE, &format!("job/{id}"), json, None)
        .await?;
    Ok(())
}

async fn start_replay(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
    input: Option<Json<ReplayInput>>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, endpoint_key)?;
    let endpoint: Endpoint = read(&app, &key).await?.ok_or_else(not_found)?;
    let input = input.map(|Json(input)| input).unwrap_or_default();
    let stream = format!("src.{}", endpoint.source);
    let (oldest, tail) = app.felix.bounds(&stream).await?;
    // Aim early and late by the margin; the worker filters by `received_at`.
    let from = match (input.from, input.since) {
        (Some(from), _) => from,
        (None, Some(since)) => {
            let target = since.saturating_sub(SEARCH_MARGIN_MS);
            offset_for_time(&app.felix, &stream, target).await?.result()
        }
        (None, None) => oldest,
    };
    let to = match (input.to, input.until) {
        (Some(to), _) => to,
        (None, Some(until)) => {
            let target = until.saturating_add(SEARCH_MARGIN_MS);
            offset_for_time(&app.felix, &stream, target).await?.result()
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
    save_job(&app, &job_id, &job).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "id": job_id, "from": from, "to": to })),
    )
        .into_response())
}

async fn get_job(
    State(app): State<Arc<App>>,
    Path((tenant, id)): Path<(String, String)>,
) -> ApiResult {
    if tenant != app.config.tenant {
        return Err(not_found());
    }
    let job: Job = super::read_from(&app, STATE, &format!("job/{id}"))
        .await?
        .ok_or_else(not_found)?;
    Ok(Json(job).into_response())
}

/// Both kinds of dead letter: the relay's own, newest first, with what has
/// become of each, and the ones Felix holds for each endpoint's group.
async fn list_dead(State(app): State<Arc<App>>, Path(tenant): Path<String>) -> ApiResult {
    if tenant != app.config.tenant {
        return Err(not_found());
    }
    let felix = &app.felix;
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
            super::read_from(&app, STATE, &format!("dead/{offset}")).await?;
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
    let endpoints: Vec<(String, Endpoint)> = app
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
    Ok(Json(json!({ "relay": relay, "broker": broker })).into_response())
}

async fn act_on_dead(
    State(app): State<Arc<App>>,
    Path((tenant, offset, action)): Path<(String, u64, String)>,
) -> ApiResult {
    if tenant != app.config.tenant {
        return Err(not_found());
    }
    let dead = read_dead_letter(&app.felix, offset)
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
            save_job(&app, &job_id, &job).await?;
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
    app.felix
        .cache_put(STATE, &format!("dead/{offset}"), json, None)
        .await?;
    answer["mark"] = json!(mark);
    Ok(Json(answer).into_response())
}

/// A record Felix dead-lettered for an endpoint's group after it was claimed
/// too many times: put it back in play, or drop it.
async fn act_on_broker_dead(
    State(app): State<Arc<App>>,
    Path((tenant, id, offset, action)): Path<(String, String, u64, String)>,
) -> ApiResult {
    let key = entry_key(&app, &tenant, &id, endpoint_key)?;
    let endpoint: Endpoint = read(&app, &key).await?.ok_or_else(not_found)?;
    let felix = &app.felix;
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
