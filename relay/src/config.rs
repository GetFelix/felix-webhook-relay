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

/// Relay settings. The defaults match the development stack in `dev/`.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// `RELAY_LISTEN`. Default `127.0.0.1:8090`. With the admin role it must
    /// be a loopback address unless `RELAY_ADMIN_ALLOW_PUBLIC=true`, because
    /// the admin API has no sign-in yet.
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
        let allow_public = or("RELAY_ADMIN_ALLOW_PUBLIC", "false") == "true";
        if roles.admin && !listen.ip().is_loopback() && !allow_public {
            bail!(
                "the admin API has no sign-in, so with the admin role RELAY_LISTEN must be a \
                 loopback address; run admin in its own process, or set \
                 RELAY_ADMIN_ALLOW_PUBLIC=true if something in front of it authenticates"
            );
        }
        Ok(Self {
            listen,
            roles,
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
    fn the_admin_api_binds_to_loopback_by_default() {
        let default = config(&REQUIRED).unwrap();
        assert!(default.roles.admin && default.listen.ip().is_loopback());

        let mut public = REQUIRED.to_vec();
        public.push(("RELAY_LISTEN", "0.0.0.0:8090"));
        let err = config(&public).unwrap_err();
        assert!(err.to_string().contains("RELAY_ADMIN_ALLOW_PUBLIC"));

        let mut intake_only = public.clone();
        intake_only.push(("RELAY_ROLES", "intake,deliver"));
        assert!(config(&intake_only).is_ok());

        public.push(("RELAY_ADMIN_ALLOW_PUBLIC", "true"));
        assert!(config(&public).is_ok());
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
        assert!(err.to_string().contains("RELAY_FELIX_TOKEN_FILE"));
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
