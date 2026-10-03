//! Delivery: one task per endpoint this process owns, started, restarted and
//! stopped as the endpoints in config change.

mod task;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_relay_core::catalog::{Endpoint, owner};
use tokio::task::JoinHandle;

use crate::App;
use task::Task;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// How often a task that ended on an error is started again.
const RESTART_EVERY: Duration = Duration::from_secs(5);

pub(crate) async fn run(app: Arc<App>) -> Result<()> {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    let config = &app.config;
    let mut catalog = app.catalog.clone();
    let mut running: HashMap<String, (Endpoint, JoinHandle<()>)> = HashMap::new();
    loop {
        let owned: HashMap<String, Endpoint> = catalog
            .borrow_and_update()
            .endpoints
            .iter()
            .filter(|(id, _)| {
                (config.endpoint_prefixes.is_empty()
                    || config.endpoint_prefixes.iter().any(|p| id.starts_with(p)))
                    && owner(id, config.worker_count) == config.worker_index
            })
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
        for (id, endpoint) in owned {
            if running.contains_key(&id) {
                continue;
            }
            let task = Task::new(Arc::clone(&app), &id, &endpoint, http.clone());
            let handle = tokio::spawn(async move {
                if let Err(err) = task.run().await {
                    tracing::error!("a delivery task stopped: {err:#}");
                }
            });
            running.insert(id, (endpoint, handle));
        }

        tokio::select! {
            changed = catalog.changed() => changed.context("the config watch stopped")?,
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
