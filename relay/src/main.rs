//! `felix-relay`: a webhook relay whose backend is Felix. One binary that runs
//! as intake, delivery, admin, or any mix, picked by `RELAY_ROLES`.

mod admin;
mod catalog;
mod config;
mod deliver;
mod felix;
mod intake;
mod metrics;
mod report;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use tracing_subscriber::EnvFilter;

use crate::catalog::Catalog;
use crate::config::Config;
use crate::felix::Felix;
use crate::metrics::Metrics;

/// What every role shares.
pub(crate) struct App {
    pub(crate) config: Config,
    pub(crate) felix: Arc<Felix>,
    /// Every source and endpoint, kept current from Felix.
    pub(crate) catalog: tokio::sync::watch::Receiver<Arc<Catalog>>,
    pub(crate) metrics: Metrics,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env()?;
    let felix = Arc::new(Felix::connect(&config).await?);
    let catalog = catalog::follow(Arc::clone(&felix)).await?;
    let app = Arc::new(App {
        config,
        felix,
        catalog,
        metrics: Metrics::default(),
    });
    let roles = app.config.roles;

    let mut router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics));
    if roles.intake {
        router = router.merge(intake::routes());
    }
    if roles.admin {
        router = router.merge(admin::routes());
    }
    let router = router.with_state(Arc::clone(&app));

    let listener = tokio::net::TcpListener::bind(app.config.listen)
        .await
        .with_context(|| format!("listen on {}", app.config.listen))?;
    tracing::info!(listen = %listener.local_addr()?, ?roles, "relay ready");

    let server = async {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown())
            .await
            .context("serve HTTP")
    };
    if roles.deliver {
        tokio::select! {
            result = server => result,
            result = deliver::run(Arc::clone(&app)) => result,
        }
    } else {
        server.await
    }
}

async fn metrics(State(app): State<Arc<App>>) -> String {
    app.metrics.render()
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

pub(crate) fn unix_millis() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}
