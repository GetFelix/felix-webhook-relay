//! Every source and endpoint, and every job, kept current from retained
//! watches on the `config` and `state` caches: the current entries first,
//! then each change.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{CacheWatchFilter, CacheWatchItem};
use felix_relay_core::catalog::{Endpoint, Source};
use felix_relay_core::jobs::Job;
use tokio::sync::watch;

use crate::felix::Felix;

pub(crate) const CONFIG: &str = "config";
pub(crate) const STATE: &str = "state";

/// What a watch keeps up to date.
pub(crate) trait Entries: Default + Clone + Send + Sync + 'static {
    const CACHE: &'static str;
    const PREFIX: &'static str;
    fn apply(&mut self, key: &str, value: Option<&[u8]>);
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Catalog {
    pub(crate) sources: HashMap<String, Source>,
    pub(crate) endpoints: HashMap<String, Endpoint>,
}

impl Entries for Catalog {
    const CACHE: &'static str = CONFIG;
    const PREFIX: &'static str = "";

    fn apply(&mut self, key: &str, value: Option<&[u8]>) {
        let parsed = if let Some(id) = key.strip_prefix("source/") {
            apply_one(&mut self.sources, id, value)
        } else if let Some(id) = key.strip_prefix("endpoint/") {
            apply_one(&mut self.endpoints, id, value)
        } else {
            Ok(())
        };
        if let Err(err) = parsed {
            tracing::warn!(%key, "ignoring a config entry: {err}");
        }
    }
}

/// Replay and redrive jobs, by id.
#[derive(Debug, Default, Clone)]
pub(crate) struct Jobs(pub(crate) HashMap<String, Job>);

impl Entries for Jobs {
    const CACHE: &'static str = STATE;
    const PREFIX: &'static str = "job/";

    fn apply(&mut self, key: &str, value: Option<&[u8]>) {
        let id = key.strip_prefix(Self::PREFIX).unwrap_or(key);
        if let Err(err) = apply_one(&mut self.0, id, value) {
            tracing::warn!(%key, "ignoring a job: {err}");
        }
    }
}

fn apply_one<T: serde::de::DeserializeOwned>(
    map: &mut HashMap<String, T>,
    id: &str,
    value: Option<&[u8]>,
) -> serde_json::Result<()> {
    match value {
        Some(bytes) => {
            map.insert(id.to_string(), serde_json::from_slice(bytes)?);
        }
        None => {
            map.remove(id);
        }
    }
    Ok(())
}

/// Start following a cache. Returns once the current entries are loaded;
/// the receiver then sees every later change.
pub(crate) async fn follow<T: Entries>(felix: Arc<Felix>) -> Result<watch::Receiver<Arc<T>>> {
    let (tx, rx) = watch::channel(Arc::new(T::default()));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let mut ready_tx = Some(ready_tx);
    tokio::spawn(async move {
        loop {
            match follow_once(&felix, &tx, &mut ready_tx).await {
                Ok(()) => tracing::warn!(cache = T::CACHE, "a watch ended; watching again"),
                Err(err) => tracing::warn!(cache = T::CACHE, "a watch failed: {err:#}"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    ready_rx
        .await
        .with_context(|| format!("load the {} cache", T::CACHE))?;
    Ok(rx)
}

/// One retained watch, from a fresh snapshot to its end. A watch that ends
/// is replaced by a new retained one, which rebuilds everything, so nothing
/// that changed in between is missed.
async fn follow_once<T: Entries>(
    felix: &Felix,
    tx: &watch::Sender<Arc<T>>,
    ready: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<()> {
    let mut watch = felix
        .client()
        .watch_cache_retained(
            &felix.tenant,
            &felix.namespace,
            T::CACHE,
            CacheWatchFilter::Prefix(T::PREFIX.to_string()),
        )
        .await?;
    let mut snapshot = watch.retained_count().unwrap_or(0);
    let mut catalog = T::default();
    loop {
        if snapshot == 0 {
            tx.send_replace(Arc::new(catalog.clone()));
            if let Some(ready) = ready.take() {
                let _ = ready.send(());
            }
        }
        match watch.recv().await {
            Some(CacheWatchItem::Change(change)) => {
                catalog.apply(&change.key, change.value.as_deref());
                snapshot = snapshot.saturating_sub(1);
            }
            Some(CacheWatchItem::ShardMoved(_)) => {}
            Some(CacheWatchItem::Lagged { .. }) | None => return Ok(()),
        }
    }
}
