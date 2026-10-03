//! One relay tenant: a Felix connection whose token reaches only the
//! tenant's namespace, and the config and jobs followed through it.

use std::sync::Arc;

use anyhow::Result;
use felix_client::TokenProvider;
use tokio::sync::watch;

use crate::catalog::{self, Catalog, Jobs};
use crate::config::Config;
use crate::felix::Felix;
use crate::unix_millis;
use felix_relay_core::Envelope;
use felix_relay_core::jobs::retention_too_short;

pub(crate) struct Tenant {
    pub(crate) name: String,
    pub(crate) felix: Arc<Felix>,
    /// Every source and endpoint, kept current from Felix.
    pub(crate) catalog: watch::Receiver<Arc<Catalog>>,
    /// Replay and redrive jobs.
    pub(crate) jobs: watch::Receiver<Arc<Jobs>>,
}

impl Tenant {
    /// A warning for each source whose stream has already lost records that
    /// the disable and replay windows need: broker retention is too short.
    pub(crate) async fn retention_warnings(&self, config: &Config) -> Vec<String> {
        let needed = config.policy.disable_after + config.replay_window;
        let needed_ms = u64::try_from(needed.as_millis()).unwrap_or(u64::MAX);
        let sources: Vec<String> = self.catalog.borrow().sources.keys().cloned().collect();
        let mut warnings = Vec::new();
        for source in sources {
            let stream = format!("src.{source}");
            let Ok((oldest, tail)) = self.felix.bounds(&stream).await else {
                continue;
            };
            let Ok(records) = self.felix.read(&stream, oldest, tail, 1).await else {
                continue;
            };
            let Some(received_at) = records
                .first()
                .and_then(|(_, payload)| Envelope::decode(payload).ok())
                .map(|e| e.received_at)
            else {
                continue;
            };
            if retention_too_short(oldest, received_at, unix_millis(), needed_ms) {
                warnings.push(format!(
                    "source {source}: the broker has trimmed it to records {} s old, but an \
                     endpoint may stay down {} s and replays reach back {} s; raise \
                     FELIX_DURABLE_RETENTION_SECONDS above {} s",
                    unix_millis().saturating_sub(received_at) / 1000,
                    config.policy.disable_after.as_secs(),
                    config.replay_window.as_secs(),
                    needed.as_secs(),
                ));
            }
        }
        warnings
    }

    /// Connect with `tokens` and load the tenant's config and jobs.
    pub(crate) async fn open(
        config: &Config,
        name: &str,
        tokens: Arc<dyn TokenProvider>,
    ) -> Result<Arc<Self>> {
        let felix = Arc::new(Felix::connect(config, name, tokens).await?);
        let catalog = catalog::follow(Arc::clone(&felix)).await?;
        let jobs = catalog::follow(Arc::clone(&felix)).await?;
        Ok(Arc::new(Self {
            name: name.to_string(),
            felix,
            catalog,
            jobs,
        }))
    }
}
