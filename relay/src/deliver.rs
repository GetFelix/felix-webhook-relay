//! Delivery: one task per endpoint this process owns, started, restarted and
//! stopped as the endpoints in config change, and the replay and redrive
//! jobs for those endpoints.

mod job;
mod send;
mod task;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_relay_core::catalog::{Endpoint, owner};
use felix_relay_core::jobs::JobKind;
use tokio::task::JoinHandle;

use crate::App;
use crate::tenant::Tenant;
pub(crate) use job::read_dead_letter;
use send::Sender;
use task::Task;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// How often a task that ended on an error is started again.
const RESTART_EVERY: Duration = Duration::from_secs(5);

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    let tenants = app
        .tenants
        .values()
        .map(|tenant| supervise(Arc::clone(&app), Arc::clone(tenant), http.clone()));
    futures_util::future::try_join_all(tenants).await?;
    Ok(())
}

/// Run the tasks and jobs of one tenant's endpoints that this process owns.
async fn supervise(app: Arc<App>, tenant: Arc<Tenant>, http: reqwest::Client) -> Result<()> {
    let config = &app.config;
    let mut catalog = tenant.catalog.clone();
    let mut jobs = tenant.jobs.clone();
    let mut running: HashMap<String, (Endpoint, JoinHandle<()>)> = HashMap::new();
    let mut running_jobs: HashMap<String, JoinHandle<()>> = HashMap::new();
    let owns = |id: &str| {
        (config.endpoint_prefixes.is_empty()
            || config.endpoint_prefixes.iter().any(|p| id.starts_with(p)))
            && owner(id, config.worker_count) == config.worker_index
    };
    loop {
        let owned: HashMap<String, Endpoint> = catalog
            .borrow_and_update()
            .endpoints
            .iter()
            .filter(|(id, _)| owns(id))
            .map(|(id, endpoint)| (id.clone(), endpoint.clone()))
            .collect();

        running.retain(|id, (started, handle)| {
            let keep =
                !handle.is_finished() && owned.get(id).is_some_and(|now| same_task(started, now));
            if !keep {
                handle.abort();
            }
            keep
        });
        for (id, endpoint) in &owned {
            if running.contains_key(id) {
                continue;
            }
            let task = Task::new(
                Arc::clone(&app),
                Arc::clone(&tenant),
                id,
                endpoint,
                http.clone(),
            );
            let handle = tokio::spawn(async move {
                if let Err(err) = task.run().await {
                    tracing::error!("a delivery task stopped: {err:#}");
                }
            });
            running.insert(id.clone(), (endpoint.clone(), handle));
        }

        running_jobs.retain(|_, handle| !handle.is_finished());
        for (id, job) in &jobs.borrow_and_update().0 {
            let Some(endpoint) = owned.get(&job.endpoint) else {
                continue;
            };
            if !job.active() || job.kind == JobKind::Retry || running_jobs.contains_key(id) {
                continue;
            }
            let sender = Sender {
                app: Arc::clone(&app),
                tenant: Arc::clone(&tenant),
                endpoint: job.endpoint.clone(),
                source: endpoint.source.clone(),
                http: http.clone(),
            };
            let handle = tokio::spawn(job::run(sender, id.clone(), job.clone()));
            running_jobs.insert(id.clone(), handle);
        }

        tokio::select! {
            changed = catalog.changed() => changed.context("the config watch stopped")?,
            changed = jobs.changed() => changed.context("the job watch stopped")?,
            () = tokio::time::sleep(RESTART_EVERY) => {}
        }
    }
}

/// Whether a running task can carry on after a config change. Everything
/// else, the URL, secrets, filters and the disabled flag, it reads afresh.
fn same_task(started: &Endpoint, now: &Endpoint) -> bool {
    started.source == now.source
        && started.mode == now.mode
        && started.in_flight() == now.in_flight()
}
