//! What the relay reads from its environment. Sources and endpoints are not
//! here: they live in Felix's `config` cache, written through the admin API.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_relay_core::health::{Policy, parse_duration, parse_durations};
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

#[derive(Debug, Clone)]
pub(crate) struct Oidc {
    pub(crate) issuer: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    /// Where this process reaches the IdP when browsers reach it at the
    /// issuer's address and this process cannot, as with a provider on
    /// another container's loopback.
    pub(crate) internal_url: Option<String>,
}

/// Relay settings. The defaults match the development stack in `dev/`.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// `RELAY_LISTEN`. Default `127.0.0.1:8090`.
    pub(crate) listen: SocketAddr,
    /// `RELAY_ROLES`: any of `intake`, `deliver`, `admin`, comma-separated.
    /// Default all three.
    pub(crate) roles: Roles,
    /// `RELAY_FELIX_BROKERS`: comma-separated broker addresses, `host:port`,
    /// resolved at each connection. Default `127.0.0.1:5000`.
    pub(crate) brokers: Vec<String>,
    /// `RELAY_FELIX_SERVER_NAME`: the name the broker's certificate is checked
    /// against. Default `localhost`, what a development broker's certificate names.
    pub(crate) server_name: String,
    /// `RELAY_FELIX_CA_FILE`: PEM certificates to trust for the broker. Unset
    /// means the platform trust store.
    pub(crate) ca_file: Option<PathBuf>,
    /// `RELAY_IDP_TOKEN_FILE`: an ID token from the deployment's IdP for the
    /// relay's service principal. Read again whenever the relay needs a new
    /// Felix token, so whatever keeps it fresh can just rewrite it. Required.
    pub(crate) idp_token_file: PathBuf,
    /// `RELAY_FELIX_CONTROL_PLANE`: where tokens are exchanged and streams
    /// created. Default `http://127.0.0.1:8443`.
    pub(crate) control_plane: String,
    /// `RELAY_OIDC_ISSUER`, `RELAY_OIDC_CLIENT_ID`, `RELAY_OIDC_CLIENT_SECRET`
    /// and optionally `RELAY_OIDC_INTERNAL_URL`: how admins sign in. Required
    /// for the admin role.
    pub(crate) oidc: Option<Oidc>,
    /// `RELAY_REPLAY_WINDOW`: how far back replays should reach. With
    /// `RELAY_DISABLE_AFTER`, the retention the relay needs from the broker.
    /// Default `7d`.
    pub(crate) replay_window: Duration,
    /// `RELAY_STREAM_REPLICAS`: how many brokers hold each source's stream.
    /// Above 1 its writes also wait for a majority (`Quorum`). Default 1.
    pub(crate) stream_replicas: u32,
    /// `RELAY_PUBLIC_URL`: where browsers reach this process, for the sign-in
    /// redirect. Default `http://<RELAY_LISTEN>`.
    pub(crate) public_url: String,
    /// `RELAY_FELIX_TENANT`: the Felix tenant the deployment lives in. Default `relay`.
    pub(crate) felix_tenant: String,
    /// `RELAY_TENANTS`: the relay tenants this process serves, comma-separated.
    /// Each is a Felix namespace. Default `acme`.
    pub(crate) tenants: Vec<String>,
    /// `RELAY_SECRET_KEY`: 32 bytes, base64 or hex, that seal every secret
    /// the relay stores in Felix. Required.
    pub(crate) secret_key: SecretKey,
    /// `RELAY_WORKER_INDEX` and `RELAY_WORKER_COUNT`: this delivery process
    /// owns the endpoints whose id hashes to its index. Default 0 of 1.
    pub(crate) worker_index: u32,
    pub(crate) worker_count: u32,
    /// `RELAY_ENDPOINT_PREFIXES`: comma-separated; when set, this process
    /// only considers endpoints whose ids start with one of them, so a
    /// deployment can give a group of endpoints its own delivery processes.
    pub(crate) endpoint_prefixes: Vec<String>,
    /// `RELAY_CLAIM_WAIT_MS`: how long a delivery task waits before its first
    /// poll. Must be at least the broker's `FELIX_GROUP_VISIBILITY_TIMEOUT_MS`.
    /// Default 30,000, the broker's default.
    pub(crate) claim_wait: Duration,
    /// `RELAY_REFUSED_RETRIES` (default `5s,30s`), `RELAY_BACKOFF` (default
    /// `5s,15s,1m,2m,5m`) and `RELAY_DISABLE_AFTER` (default `72h`).
    pub(crate) policy: Policy,
    /// Names this process in health entries: `RELAY_WORKER_NAME`, else the
    /// host name and process id.
    pub(crate) reporter: String,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
    }

    fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let or = |name: &str, default: &str| var(name).unwrap_or_else(|| default.to_string());
        let durations = |name: &str, default: Vec<Duration>| match var(name) {
            Some(value) => parse_durations(&value)
                .filter(|list| !list.is_empty())
                .with_context(|| format!("{name} is a comma-separated list like 5s,1m,2h")),
            None => Ok(default),
        };
        let defaults = Policy::default();
        let policy = Policy {
            refused_retries: durations("RELAY_REFUSED_RETRIES", defaults.refused_retries)?,
            backoff: durations("RELAY_BACKOFF", defaults.backoff)?,
            disable_after: match var("RELAY_DISABLE_AFTER") {
                Some(value) => parse_duration(&value).context("RELAY_DISABLE_AFTER is like 72h")?,
                None => defaults.disable_after,
            },
        };
        let worker_count: u32 = or("RELAY_WORKER_COUNT", "1")
            .parse()
            .context("parse RELAY_WORKER_COUNT")?;
        let worker_index: u32 = or("RELAY_WORKER_INDEX", "0")
            .parse()
            .context("parse RELAY_WORKER_INDEX")?;
        if worker_count == 0 || worker_index >= worker_count {
            bail!("RELAY_WORKER_INDEX must be below RELAY_WORKER_COUNT, which must be at least 1");
        }
        let secret_key = var("RELAY_SECRET_KEY").context(
            "RELAY_SECRET_KEY is required: 32 random bytes, base64 or hex, e.g. `openssl rand -base64 32`",
        )?;
        let listen: SocketAddr = or("RELAY_LISTEN", "127.0.0.1:8090")
            .parse()
            .context("parse RELAY_LISTEN")?;
        let roles = Roles::parse(&or("RELAY_ROLES", "intake,deliver,admin"))
            .context("parse RELAY_ROLES")?;
        let oidc = match var("RELAY_OIDC_ISSUER") {
            Some(issuer) => Some(Oidc {
                issuer: issuer.trim_end_matches('/').to_string(),
                client_id: var("RELAY_OIDC_CLIENT_ID")
                    .context("RELAY_OIDC_CLIENT_ID is required")?,
                client_secret: var("RELAY_OIDC_CLIENT_SECRET")
                    .context("RELAY_OIDC_CLIENT_SECRET is required")?,
                internal_url: var("RELAY_OIDC_INTERNAL_URL")
                    .map(|url| url.trim_end_matches('/').to_string()),
            }),
            None if roles.admin => bail!("the admin role needs RELAY_OIDC_ISSUER for sign-in"),
            None => None,
        };
        Ok(Self {
            listen,
            roles,
            brokers: or("RELAY_FELIX_BROKERS", "127.0.0.1:5000")
                .split(',')
                .map(|addr| addr.trim().to_string())
                .filter(|addr| !addr.is_empty())
                .collect(),
            server_name: or("RELAY_FELIX_SERVER_NAME", "localhost"),
            ca_file: var("RELAY_FELIX_CA_FILE").map(PathBuf::from),
            idp_token_file: var("RELAY_IDP_TOKEN_FILE")
                .map(PathBuf::from)
                .context("RELAY_IDP_TOKEN_FILE is required")?,
            control_plane: or("RELAY_FELIX_CONTROL_PLANE", "http://127.0.0.1:8443")
                .trim_end_matches('/')
                .to_string(),
            oidc,
            replay_window: parse_duration(&or("RELAY_REPLAY_WINDOW", "7d"))
                .context("RELAY_REPLAY_WINDOW is like 7d")?,
            stream_replicas: or("RELAY_STREAM_REPLICAS", "1")
                .parse()
                .context("parse RELAY_STREAM_REPLICAS")?,
            public_url: var("RELAY_PUBLIC_URL")
                .unwrap_or_else(|| format!("http://{listen}"))
                .trim_end_matches('/')
                .to_string(),
            felix_tenant: or("RELAY_FELIX_TENANT", "relay"),
            tenants: or("RELAY_TENANTS", "acme")
                .split(',')
                .map(|tenant| tenant.trim().to_string())
                .filter(|tenant| !tenant.is_empty())
                .collect(),
            secret_key: SecretKey::parse(&secret_key)?,
            worker_index,
            worker_count,
            endpoint_prefixes: or("RELAY_ENDPOINT_PREFIXES", "")
                .split(',')
                .map(str::trim)
                .filter(|prefix| !prefix.is_empty())
                .map(str::to_string)
                .collect(),
            claim_wait: Duration::from_millis(
                or("RELAY_CLAIM_WAIT_MS", "30000")
                    .parse()
                    .context("parse RELAY_CLAIM_WAIT_MS")?,
            ),
            policy,
            reporter: var("RELAY_WORKER_NAME").unwrap_or_else(|| {
                let host = var("HOSTNAME").unwrap_or_else(|| "relay".to_string());
                format!("{host}/{} worker {worker_index}", std::process::id())
            }),
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

    const REQUIRED: [(&str, &str); 5] = [
        ("RELAY_IDP_TOKEN_FILE", "relay-idp.token"),
        (
            "RELAY_SECRET_KEY",
            "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
        ),
        ("RELAY_OIDC_ISSUER", "http://127.0.0.1:5556/dex/"),
        ("RELAY_OIDC_CLIENT_ID", "relay-admin"),
        ("RELAY_OIDC_CLIENT_SECRET", "dev-admin-secret"),
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
        assert_eq!(config.tenants, ["acme"]);
    }

    #[test]
    fn the_admin_role_needs_sign_in_settings() {
        let err = config(&REQUIRED[..2]).unwrap_err();
        assert!(err.to_string().contains("RELAY_OIDC_ISSUER"));
        let mut deliver_only = REQUIRED[..2].to_vec();
        deliver_only.push(("RELAY_ROLES", "intake,deliver"));
        assert!(config(&deliver_only).is_ok());
        let parsed = config(&REQUIRED).unwrap();
        assert_eq!(parsed.public_url, "http://127.0.0.1:8090");
        assert_eq!(parsed.oidc.unwrap().issuer, "http://127.0.0.1:5556/dex");
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
    fn delivery_timings_are_read() {
        let mut vars = REQUIRED.to_vec();
        vars.push(("RELAY_BACKOFF", "100ms,1s"));
        vars.push(("RELAY_DISABLE_AFTER", "10m"));
        vars.push(("RELAY_CLAIM_WAIT_MS", "5000"));
        let parsed = config(&vars).unwrap();
        assert_eq!(
            parsed.policy.backoff,
            [Duration::from_millis(100), Duration::from_secs(1)]
        );
        assert_eq!(parsed.policy.disable_after, Duration::from_secs(600));
        assert_eq!(
            parsed.policy.refused_retries,
            Policy::default().refused_retries
        );
        assert_eq!(parsed.claim_wait, Duration::from_secs(5));
        vars.push(("RELAY_REFUSED_RETRIES", "5 s"));
        assert!(config(&vars).is_err());
    }

    #[test]
    fn the_token_file_is_required() {
        let err = config(&REQUIRED[1..]).unwrap_err();
        assert!(err.to_string().contains("RELAY_IDP_TOKEN_FILE"));
    }

    #[test]
    fn worker_index_is_below_the_count() {
        let mut vars = REQUIRED.to_vec();
        vars.push(("RELAY_WORKER_COUNT", "3"));
        vars.push(("RELAY_WORKER_INDEX", "2"));
        vars.push(("RELAY_ENDPOINT_PREFIXES", "team-a-, team-b-"));
        let parsed = config(&vars).unwrap();
        assert_eq!((parsed.worker_index, parsed.worker_count), (2, 3));
        assert_eq!(parsed.endpoint_prefixes, ["team-a-", "team-b-"]);
        let mut over = REQUIRED.to_vec();
        over.extend([("RELAY_WORKER_COUNT", "3"), ("RELAY_WORKER_INDEX", "3")]);
        assert!(config(&over).is_err());
    }
}
