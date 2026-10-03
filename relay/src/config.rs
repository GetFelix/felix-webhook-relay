//! What the relay reads from its environment. Sources and endpoints are not
//! here: they live in Felix's `config` cache, written through the admin API.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use felix_relay_core::secret::SecretKey;

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
    /// `RELAY_FELIX_TOKEN_FILE`: the Felix token the relay connects with. Required.
    pub(crate) token_file: PathBuf,
    /// `RELAY_FELIX_TENANT`: the Felix tenant the deployment lives in. Default `relay`.
    pub(crate) felix_tenant: String,
    /// `RELAY_TENANT`: the relay tenant, which is a Felix namespace. Default `acme`.
    pub(crate) tenant: String,
    /// `RELAY_SECRET_KEY`: 32 bytes, base64 or hex, that seal every secret
    /// the relay stores in Felix. Required.
    pub(crate) secret_key: SecretKey,
    /// `RELAY_ENDPOINT`: the endpoint this process delivers to. Default `demo`.
    pub(crate) endpoint: String,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
    }

    fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let or = |name: &str, default: &str| var(name).unwrap_or_else(|| default.to_string());
        let secret_key = var("RELAY_SECRET_KEY").context(
            "RELAY_SECRET_KEY is required: 32 random bytes, base64 or hex, e.g. `openssl rand -base64 32`",
        )?;
        Ok(Self {
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
            token_file: var("RELAY_FELIX_TOKEN_FILE")
                .map(PathBuf::from)
                .context("RELAY_FELIX_TOKEN_FILE is required")?,
            felix_tenant: or("RELAY_FELIX_TENANT", "relay"),
            tenant: or("RELAY_TENANT", "acme"),
            secret_key: SecretKey::parse(&secret_key)?,
            endpoint: or("RELAY_ENDPOINT", "demo"),
        })
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

    const REQUIRED: [(&str, &str); 2] = [
        ("RELAY_FELIX_TOKEN_FILE", "relay.token"),
        (
            "RELAY_SECRET_KEY",
            "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
        ),
    ];

    #[test]
    fn defaults_run_every_role() {
        let config = config(&REQUIRED).unwrap();
        assert_eq!(
            config.roles,
            Roles {
                intake: true,
                deliver: true,
                admin: true
            }
        );
        assert_eq!(config.tenant, "acme");
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
    fn the_secret_key_is_required_and_checked() {
        let err = config(&REQUIRED[..1]).unwrap_err();
        assert!(err.to_string().contains("RELAY_SECRET_KEY"));
        let short = [REQUIRED[0], ("RELAY_SECRET_KEY", "AAEC")];
        assert!(config(&short).is_err());
    }

    #[test]
    fn the_token_file_is_required() {
        let err = config(&REQUIRED[1..]).unwrap_err();
        assert!(err.to_string().contains("RELAY_FELIX_TOKEN_FILE"));
    }
}
