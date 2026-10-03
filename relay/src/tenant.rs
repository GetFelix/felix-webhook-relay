//! One relay tenant: a Felix connection whose token reaches only the
//! tenant's namespace, and the config and jobs followed through it.

use std::sync::Arc;

use anyhow::Result;
use felix_client::TokenProvider;
use tokio::sync::watch;

use crate::catalog::{self, Catalog, Jobs};
use crate::config::Config;
use crate::felix::Felix;

pub(crate) struct Tenant {
    pub(crate) name: String,
    pub(crate) felix: Arc<Felix>,
    /// Every source and endpoint, kept current from Felix.
    pub(crate) catalog: watch::Receiver<Arc<Catalog>>,
    /// Replay and redrive jobs.
    pub(crate) jobs: watch::Receiver<Arc<Jobs>>,
}

impl Tenant {
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
