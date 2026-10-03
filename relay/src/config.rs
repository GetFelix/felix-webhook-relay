//! What the relay reads from its environment.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

/// Which parts of the relay this process runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Roles {
    pub(crate) intake: bool,
    pub(crate) deliver: bool,
    pub(crate) admin: bool,
}

impl Roles {
    fn parse(value: &str) -> Result<Self> {
        let mut roles = Self::default();
        for role in value.split(',').map(str::trim).filter(|r| !r.is_empty()) {
            match role {
                "intake" => roles.intake = true,
                "deliver" => roles.deliver = true,
                "admin" => roles.admin = true,
                other => bail!("unknown role {other:?}; expected intake, deliver or admin"),
            }
        }
        if roles == Self::default() {
            bail!("no roles given");
        }
        Ok(roles)
    }

    pub(crate) fn uses_felix(self) -> bool {
        self.intake || self.deliver
    }
}

/// Relay settings. The defaults match the development stack in `dev/`.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// `RELAY_LISTEN`. Default `127.0.0.1:8090`.
    pub(crate) listen: SocketAddr,
    /// `RELAY_ROLES`: any of `intake`, `deliver`, `admin`, comma-separated.
    /// Default all three.
    pub(crate) roles: Roles,
    /// `RELAY_FELIX_BROKERS`: comma-separated broker addresses. Default `127.0.0.1:5000`.
    pub(crate) brokers: Vec<SocketAddr>,
    /// `RELAY_FELIX_SERVER_NAME`: the name the broker's certificate is checked
    /// against. Default `localhost`, what a development broker's certificate names.
    pub(crate) server_name: String,
    /// `RELAY_FELIX_CA_FILE`: PEM certificates to trust for the broker. Unset
    /// means the platform trust store.
    pub(crate) ca_file: Option<PathBuf>,
    /// `RELAY_FELIX_TOKEN_FILE`: the Felix token the relay connects with.
    pub(crate) token_file: Option<PathBuf>,
    /// `RELAY_FELIX_TENANT`: the Felix tenant the deployment lives in. Default `relay`.
    pub(crate) felix_tenant: String,
    /// `RELAY_TENANT`: the relay tenant, which is a Felix namespace. Default `acme`.
    pub(crate) tenant: String,
    /// `RELAY_SOURCE`: the one source intake accepts. Default `demo`.
    pub(crate) source: String,
    /// `RELAY_EVENT_TYPE_HEADER`: the request header that names the event type.
    pub(crate) event_type_header: Option<String>,
    /// `RELAY_KEEP_HEADERS`: comma-separated request headers to store with the body.
    pub(crate) keep_headers: Vec<String>,
    /// `RELAY_ENDPOINT`: the one endpoint's id, which names its consumer group. Default `demo`.
    pub(crate) endpoint: String,
    /// `RELAY_ENDPOINT_URL`: where the endpoint receives webhooks. Needed by `deliver`.
    pub(crate) endpoint_url: Option<String>,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
    }

    fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let or = |name: &str, default: &str| var(name).unwrap_or_else(|| default.to_string());
        let list = |name: &str| -> Vec<String> {
            var(name)
                .unwrap_or_default()
                .split(',')
                .map(|item| item.trim().to_ascii_lowercase())
                .filter(|item| !item.is_empty())
                .collect()
        };
        let config = Self {
            listen: or("RELAY_LISTEN", "127.0.0.1:8090")
                .parse()
                .context("parse RELAY_LISTEN")?,
            roles: Roles::parse(&or("RELAY_ROLES", "intake,deliver,admin"))
                .context("parse RELAY_ROLES")?,
            brokers: or("RELAY_FELIX_BROKERS", "127.0.0.1:5000")
                .split(',')
                .map(|addr| addr.trim().parse())
                .collect::<Result<_, _>>()
                .context("parse RELAY_FELIX_BROKERS")?,
            server_name: or("RELAY_FELIX_SERVER_NAME", "localhost"),
            ca_file: var("RELAY_FELIX_CA_FILE").map(PathBuf::from),
            token_file: var("RELAY_FELIX_TOKEN_FILE").map(PathBuf::from),
            felix_tenant: or("RELAY_FELIX_TENANT", "relay"),
            tenant: or("RELAY_TENANT", "acme"),
            source: or("RELAY_SOURCE", "demo"),
            event_type_header: var("RELAY_EVENT_TYPE_HEADER").map(|h| h.to_ascii_lowercase()),
            keep_headers: list("RELAY_KEEP_HEADERS"),
            endpoint: or("RELAY_ENDPOINT", "demo"),
            endpoint_url: var("RELAY_ENDPOINT_URL"),
        };
        if config.roles.uses_felix() && config.token_file.is_none() {
            bail!("RELAY_FELIX_TOKEN_FILE is required for the intake and deliver roles");
        }
        if config.roles.deliver && config.endpoint_url.is_none() {
            bail!("RELAY_ENDPOINT_URL is required for the deliver role");
        }
        Ok(config)
    }

    /// The stream that holds a source's webhooks.
    pub(crate) fn source_stream(&self) -> String {
        format!("src.{}", self.source)
    }

    /// The consumer group that tracks an endpoint's position.
    pub(crate) fn endpoint_group(&self) -> String {
        format!("ep.{}", self.endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(vars: &[(&str, &str)]) -> Result<Config> {
        Config::from_lookup(|name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        })
    }

    const FELIX: [(&str, &str); 2] = [
        ("RELAY_FELIX_TOKEN_FILE", "relay.token"),
        ("RELAY_ENDPOINT_URL", "http://127.0.0.1:9000/hook"),
    ];

    #[test]
    fn defaults_run_every_role() {
        let config = config(&FELIX).unwrap();
        assert_eq!(
            config.roles,
            Roles {
                intake: true,
                deliver: true,
                admin: true
            }
        );
        assert_eq!(config.source_stream(), "src.demo");
        assert_eq!(config.endpoint_group(), "ep.demo");
    }

    #[test]
    fn roles_mix() {
        let roles = Roles::parse("deliver, admin").unwrap();
        assert!(!roles.intake && roles.deliver && roles.admin);
    }

    #[test]
    fn unknown_or_missing_roles_are_refused() {
        assert!(Roles::parse("intake,ingest").is_err());
        assert!(Roles::parse(" , ").is_err());
    }

    #[test]
    fn admin_alone_needs_no_felix_or_endpoint() {
        assert!(config(&[("RELAY_ROLES", "admin")]).is_ok());
    }

    #[test]
    fn deliver_needs_an_endpoint_url() {
        let err = config(&[
            ("RELAY_ROLES", "deliver"),
            ("RELAY_FELIX_TOKEN_FILE", "relay.token"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("RELAY_ENDPOINT_URL"));
    }

    #[test]
    fn header_names_are_lowercased() {
        let mut vars = FELIX.to_vec();
        vars.push(("RELAY_KEEP_HEADERS", "User-Agent, X-Request-Id"));
        vars.push(("RELAY_EVENT_TYPE_HEADER", "X-GitHub-Event"));
        let config = config(&vars).unwrap();
        assert_eq!(config.keep_headers, ["user-agent", "x-request-id"]);
        assert_eq!(config.event_type_header.as_deref(), Some("x-github-event"));
    }
}
