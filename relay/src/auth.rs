//! Felix tokens, exchanged at the control plane from an IdP ID token and
//! narrowed to one relay tenant's namespace. A token narrowed that way is
//! refused by the broker on any other tenant's streams and caches, so tenant
//! isolation does not rest on the relay's own checks.

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_client::{RefreshingToken, TokenProvider};
use serde_json::{Value, json};

use crate::config::{Config, Roles};

/// What intake needs: append webhooks, check and record idempotency keys,
/// count what it received.
const INTAKE: [&str; 3] = ["stream.publish", "cache.read", "cache.write"];
/// What delivery needs: poll groups (`stream.subscribe` includes
/// `group.consume`), write dead letters and attempts, read config, write
/// health and jobs.
const DELIVER: [&str; 4] = [
    "stream.subscribe",
    "stream.publish",
    "cache.read",
    "cache.write",
];
/// What an admin needs, on top of delivery's: redrive and discard Felix's
/// group dead letters.
pub(crate) const ADMIN: [&str; 5] = [
    "stream.subscribe",
    "stream.publish",
    "cache.read",
    "cache.write",
    "group.manage",
];
/// What creating a source's stream needs, at the control plane.
pub(crate) const MANAGE: [&str; 1] = ["stream.manage"];

/// The control plane said no.
#[derive(Debug)]
pub(crate) struct Refused {
    pub(crate) status: u16,
    pub(crate) message: String,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the control plane refused ({}): {}",
            self.status, self.message
        )
    }
}

impl std::error::Error for Refused {}

/// The actions a process running `roles` asks for on its own behalf.
pub(crate) fn service_actions(roles: Roles) -> Vec<&'static str> {
    let mut actions: Vec<&str> = Vec::new();
    for (wanted, role) in [(roles.intake, &INTAKE[..]), (roles.deliver, &DELIVER[..])] {
        if wanted {
            actions.extend(
                role.iter()
                    .filter(|a| !actions.contains(a))
                    .collect::<Vec<_>>(),
            );
        }
    }
    actions
}

/// Exchange `id_token` for a Felix token limited to `actions` on `tenant`'s
/// namespace. `audience` is `felix-broker` or `felix-controlplane`.
///
/// # Errors
/// [`Refused`] when the control plane refuses the caller, else a transport error.
pub(crate) async fn exchange(
    http: &reqwest::Client,
    config: &Config,
    id_token: &str,
    tenant: &str,
    actions: &[&str],
    audience: &str,
) -> Result<String> {
    let url = format!(
        "{}/v1/tenants/{}/token/exchange",
        config.control_plane, config.felix_tenant
    );
    let response = http
        .post(&url)
        .bearer_auth(id_token)
        .json(&json!({
            "requested": actions,
            "resources": [format!("namespace:{}/{tenant}", config.felix_tenant)],
            "audience": audience,
        }))
        .send()
        .await
        .with_context(|| format!("reach the control plane at {url}"))?;
    let status = response.status();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        let message = body["message"].as_str().unwrap_or("no reason given");
        return Err(Refused {
            status: status.as_u16(),
            message: message.to_string(),
        }
        .into());
    }
    body["felix_token"]
        .as_str()
        .map(str::to_string)
        .context("the exchange answered without a token")
}

/// The relay's own token for one tenant: the IdP token file is read again and
/// exchanged again whenever the Felix token nears its end.
pub(crate) fn service_tokens(config: &Config, tenant: &str) -> Arc<dyn TokenProvider> {
    let config = config.clone();
    let tenant = tenant.to_string();
    let http = reqwest::Client::new();
    Arc::new(RefreshingToken::new(move || {
        let (config, tenant, http) = (config.clone(), tenant.clone(), http.clone());
        async move {
            let file = &config.idp_token_file;
            let id_token = std::fs::read_to_string(file)
                .with_context(|| format!("read {}", file.display()))?;
            let actions = service_actions(config.roles);
            exchange(
                &http,
                &config,
                id_token.trim(),
                &tenant,
                &actions,
                "felix-broker",
            )
            .await
        }
    }))
}

/// A token another party already holds, such as a signed-in admin's, kept
/// fresh by exchanging their ID token again.
pub(crate) fn exchanged_tokens(
    config: &Config,
    tenant: &str,
    id_token: &str,
    initial: String,
) -> Arc<dyn TokenProvider> {
    let config = config.clone();
    let tenant = tenant.to_string();
    let id_token = id_token.to_string();
    let http = reqwest::Client::new();
    Arc::new(RefreshingToken::with_initial(initial, move || {
        let (config, tenant, id_token, http) = (
            config.clone(),
            tenant.clone(),
            id_token.clone(),
            http.clone(),
        );
        async move { exchange(&http, &config, &id_token, &tenant, &ADMIN, "felix-broker").await }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_role_asks_for_only_what_it_needs() {
        let intake = Roles {
            intake: true,
            ..Roles::default()
        };
        assert_eq!(service_actions(intake), INTAKE);
        let both = Roles {
            intake: true,
            deliver: true,
            admin: true,
        };
        let actions = service_actions(both);
        assert_eq!(actions.len(), 4, "{actions:?}");
        assert!(
            !actions.contains(&"group.manage"),
            "admins bring their own token"
        );
        assert!(service_actions(Roles::default()).is_empty());
    }
}
