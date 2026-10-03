//! Every source and endpoint, kept current from a retained watch on the
//! `config` cache: the current entries first, then each change.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{CacheWatchFilter, CacheWatchItem};
use felix_relay_core::catalog::{Endpoint, Source};
use tokio::sync::watch;

use crate::felix::Felix;

pub(crate) const CONFIG: &str = "config";

#[derive(Debug, Default, Clone)]
pub(crate) struct Catalog {
    pub(crate) sources: HashMap<String, Source>,
    pub(crate) endpoints: HashMap<String, Endpoint>,
}

impl Catalog {
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

/// Start following the `config` cache. Returns once the current entries are
/// loaded; the receiver then sees every later change.
pub(crate) async fn follow(felix: Arc<Felix>) -> Result<watch::Receiver<Arc<Catalog>>> {
    let (tx, rx) = watch::channel(Arc::new(Catalog::default()));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let mut ready_tx = Some(ready_tx);
    tokio::spawn(async move {
        loop {
            match follow_once(&felix, &tx, &mut ready_tx).await {
                Ok(()) => tracing::warn!("the config watch ended; watching again"),
                Err(err) => tracing::warn!("the config watch failed: {err:#}"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    ready_rx.await.context("load the config cache")?;
    Ok(rx)
}

/// One retained watch, from a fresh snapshot to its end. A watch that ends
/// is replaced by a new retained one, which rebuilds the whole catalog, so
/// nothing that changed in between is missed.
async fn follow_once(
    felix: &Felix,
    tx: &watch::Sender<Arc<Catalog>>,
    ready: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<()> {
    let mut watch = felix
        .client()
        .watch_cache_retained(
            &felix.tenant,
            &felix.namespace,
            CONFIG,
            CacheWatchFilter::Prefix(String::new()),
        )
        .await?;
    let mut snapshot = watch.retained_count().unwrap_or(0);
    let mut catalog = Catalog::default();
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
