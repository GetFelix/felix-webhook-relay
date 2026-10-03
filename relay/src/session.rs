//! Admin sign-in: the OIDC authorization code flow, run server-side, with
//! the session in a cookie sealed under `RELAY_SECRET_KEY`, so the relay
//! stores nothing. Each admin request exchanges the admin's ID token for a
//! Felix token narrowed to the tenant they opened; the control plane refuses
//! anyone without the tenant's admin role.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{AppendHeaders, IntoResponse, Redirect, Response};
use axum::routing::get;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_relay_core::secret::{Sealed, random_bytes};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::auth::{self, ADMIN, Refused};
use crate::tenant::Tenant;
use crate::{App, unix_millis};

const SESSION: &str = "relay_session";
const LOGIN: &str = "relay_login";
/// A sign-in that takes longer than this starts over.
const LOGIN_TTL_SECS: u64 = 600;

/// Sign-in state that lives only as long as the process: the IdP's endpoints
/// and the Felix connections opened for signed-in admins.
#[derive(Default)]
pub(crate) struct Sessions {
    endpoints: OnceCell<Endpoints>,
    /// By ID token and tenant, until the ID token expires.
    tenants: Mutex<Opened>,
}

type Opened = HashMap<(String, String), (Arc<Tenant>, u64)>;

/// A signed-in admin.
pub(crate) struct Identity {
    pub(crate) id_token: String,
    pub(crate) name: String,
}

#[derive(Deserialize)]
struct Endpoints {
    authorization_endpoint: String,
    token_endpoint: String,
}

/// What the session cookie holds.
#[derive(Serialize, Deserialize)]
struct Session {
    id_token: String,
    name: String,
    /// Unix seconds.
    exp: u64,
}

/// What the login cookie holds between leaving for the IdP and coming back.
#[derive(Serialize, Deserialize)]
struct Login {
    state: String,
    nonce: String,
    return_to: String,
    /// Unix seconds.
    exp: u64,
}

/// Why an admin request was not let through.
pub(crate) enum Denied {
    /// No session or bearer token: sign in first.
    SignedOut,
    /// The control plane refused this person for this tenant.
    Forbidden(String),
    /// Something went wrong reaching Felix or the IdP.
    Unavailable(anyhow::Error),
}

pub(crate) fn routes() -> axum::Router<Arc<App>> {
    axum::Router::new()
        .route("/auth/login", get(login))
        .route("/auth/callback", get(callback))
        .route("/auth/logout", get(logout))
}

/// The admin's ID token: a bearer token, for scripts, or the session cookie.
pub(crate) fn identity(app: &App, headers: &HeaderMap) -> Option<Identity> {
    if let Some(token) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    {
        let claims = claims(token).ok()?;
        return Some(Identity {
            id_token: token.to_string(),
            name: display_name(&claims),
        });
    }
    let session: Session = open_cookie(app, headers, SESSION)?;
    (session.exp > unix_millis() / 1000).then_some(Identity {
        id_token: session.id_token,
        name: session.name,
    })
}

/// The tenant's Felix connection for this admin, made the first time they
/// open it and kept until their ID token expires.
pub(crate) async fn admin_tenant(
    app: &App,
    headers: &HeaderMap,
    tenant: &str,
) -> Result<(Arc<Tenant>, Identity), Denied> {
    if !app.config.tenants.iter().any(|t| t == tenant) {
        return Err(Denied::Forbidden(format!(
            "{tenant} is not a tenant of this relay"
        )));
    }
    let who = identity(app, headers).ok_or(Denied::SignedOut)?;
    let id_token = who.id_token.clone();
    let key = (id_token.clone(), tenant.to_string());
    let now = unix_millis() / 1000;
    if let Some((open, _)) = app.sessions.tenants.lock().unwrap().get(&key) {
        return Ok((Arc::clone(open), who));
    }
    let http = reqwest::Client::new();
    let token = auth::exchange(
        &http,
        &app.config,
        &id_token,
        tenant,
        &ADMIN,
        "felix-broker",
    )
    .await
    .map_err(|err| match err.downcast::<Refused>() {
        Ok(refused) if refused.status == 401 => Denied::SignedOut,
        Ok(refused) => Denied::Forbidden(refused.to_string()),
        Err(err) => Denied::Unavailable(err),
    })?;
    let exp = claims(&id_token)
        .ok()
        .and_then(|c| c["exp"].as_u64())
        .unwrap_or(now);
    let provider = auth::exchanged_tokens(&app.config, tenant, &id_token, token);
    let opened = Tenant::open(&app.config, tenant, provider)
        .await
        .map_err(Denied::Unavailable)?;
    let mut open = app.sessions.tenants.lock().unwrap();
    open.retain(|_, (_, until)| *until > now);
    open.insert(key, (Arc::clone(&opened), exp));
    Ok((opened, who))
}

async fn endpoints(app: &App) -> Result<&Endpoints> {
    let oidc = app.config.oidc.as_ref().context("no OIDC settings")?;
    app.sessions
        .endpoints
        .get_or_try_init(|| async {
            let url = format!("{}/.well-known/openid-configuration", oidc.issuer);
            reqwest::get(&url)
                .await?
                .error_for_status()?
                .json::<Endpoints>()
                .await
                .with_context(|| format!("read {url}"))
        })
        .await
}

fn redirect_uri(app: &App) -> String {
    format!("{}/auth/callback", app.config.public_url)
}

fn random_token() -> String {
    URL_SAFE_NO_PAD.encode(random_bytes())
}

#[derive(Deserialize)]
struct LoginQuery {
    return_to: Option<String>,
}

async fn login(State(app): State<Arc<App>>, Query(query): Query<LoginQuery>) -> Response {
    let endpoints = match endpoints(&app).await {
        Ok(endpoints) => endpoints,
        Err(err) => return unavailable(&err),
    };
    let oidc = app.config.oidc.as_ref().expect("checked by endpoints");
    // Only paths on this relay, so the sign-in cannot be used to bounce
    // someone to another site.
    let return_to = query
        .return_to
        .filter(|path| path.starts_with('/') && !path.starts_with("//"))
        .unwrap_or_else(|| "/".to_string());
    let login = Login {
        state: random_token(),
        nonce: random_token(),
        return_to,
        exp: unix_millis() / 1000 + LOGIN_TTL_SECS,
    };
    let mut url = reqwest::Url::parse(&endpoints.authorization_endpoint).expect("a URL");
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &oidc.client_id)
        .append_pair("redirect_uri", &redirect_uri(&app))
        .append_pair("scope", "openid email profile")
        .append_pair("state", &login.state)
        .append_pair("nonce", &login.nonce);
    let cookie = seal_cookie(&app, LOGIN, &login, LOGIN_TTL_SECS);
    ([(header::SET_COOKIE, cookie)], Redirect::to(url.as_str())).into_response()
}

#[derive(Deserialize)]
struct Callback {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn callback(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(answer): Query<Callback>,
) -> Response {
    let login: Option<Login> = open_cookie(&app, &headers, LOGIN);
    let result = async {
        if let Some(error) = answer.error {
            bail!("the identity provider said {error}");
        }
        let login = login
            .filter(|l| l.exp > unix_millis() / 1000)
            .context("the sign-in expired; start again")?;
        if answer.state.as_deref() != Some(login.state.as_str()) {
            bail!("the sign-in state does not match; start again");
        }
        let code = answer.code.context("no code")?;
        let id_token = redeem(&app, &code).await?;
        let claims = claims(&id_token)?;
        if claims["nonce"].as_str() != Some(login.nonce.as_str()) {
            bail!("the ID token was not issued for this sign-in");
        }
        let exp = claims["exp"].as_u64().context("an ID token without exp")?;
        let session = Session {
            name: display_name(&claims),
            id_token,
            exp,
        };
        Ok((session, login.return_to))
    }
    .await;
    match result {
        Ok((session, return_to)) => {
            let ttl = session.exp.saturating_sub(unix_millis() / 1000);
            // Appended: an array of headers would keep only the last cookie.
            let cookies = AppendHeaders([
                (
                    header::SET_COOKIE,
                    seal_cookie(&app, SESSION, &session, ttl),
                ),
                (header::SET_COOKIE, clear_cookie(&app, LOGIN)),
            ]);
            (cookies, Redirect::to(&return_to)).into_response()
        }
        Err(err) => (StatusCode::BAD_REQUEST, format!("Sign-in failed: {err:#}")).into_response(),
    }
}

/// Trade the code for an ID token at the IdP's token endpoint. The token
/// comes straight from the IdP over that request, which is what OIDC allows
/// in place of checking its signature here; the control plane checks it on
/// every exchange anyway.
async fn redeem(app: &App, code: &str) -> Result<String> {
    let oidc = app.config.oidc.as_ref().context("no OIDC settings")?;
    let endpoints = endpoints(app).await?;
    let response: Value = reqwest::Client::new()
        .post(&endpoints.token_endpoint)
        .basic_auth(&oidc.client_id, Some(&oidc.client_secret))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &redirect_uri(app)),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    response["id_token"]
        .as_str()
        .map(str::to_string)
        .context("the token endpoint answered without an ID token")
}

async fn logout(State(app): State<Arc<App>>) -> Response {
    let cookie = clear_cookie(&app, SESSION);
    ([(header::SET_COOKIE, cookie)], Redirect::to("/")).into_response()
}

fn unavailable(err: &anyhow::Error) -> Response {
    tracing::error!("sign-in unavailable: {err:#}");
    (StatusCode::SERVICE_UNAVAILABLE, "Sign-in is unavailable.").into_response()
}

/// The claims of a JWT, read without checking its signature.
fn claims(token: &str) -> Result<Value> {
    let payload = token.split('.').nth(1).context("not a JWT")?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .context("not a JWT")?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn display_name(claims: &Value) -> String {
    ["email", "name", "sub"]
        .iter()
        .find_map(|claim| claims[*claim].as_str())
        .unwrap_or("someone")
        .to_string()
}

fn secure(app: &App) -> &'static str {
    if app.config.public_url.starts_with("https://") {
        "; Secure"
    } else {
        ""
    }
}

fn seal_cookie(app: &App, name: &str, value: &impl Serialize, ttl: u64) -> HeaderValue {
    let json = serde_json::to_string(value).expect("a cookie serializes");
    let sealed = app.config.secret_key.seal(name, &json);
    let cookie = format!(
        "{name}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={ttl}{}",
        sealed.as_str(),
        secure(app)
    );
    HeaderValue::from_str(&cookie).expect("base64 is a valid cookie value")
}

fn clear_cookie(app: &App, name: &str) -> HeaderValue {
    let cookie = format!(
        "{name}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0{}",
        secure(app)
    );
    HeaderValue::from_str(&cookie).expect("a valid cookie")
}

fn open_cookie<T: for<'de> Deserialize<'de>>(
    app: &App,
    headers: &HeaderMap,
    name: &str,
) -> Option<T> {
    let value = headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .find_map(|pair| pair.trim().strip_prefix(name)?.strip_prefix('='))?;
    let json = app
        .config
        .secret_key
        .open(name, &Sealed::from(value.to_string()))
        .ok()?;
    serde_json::from_str(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_are_read_from_the_middle_segment() {
        let payload = URL_SAFE_NO_PAD.encode(br#"{"email":"ana@example.com","exp":5}"#);
        let token = format!("eyJhbGciOiJSUzI1NiJ9.{payload}.c2ln");
        let claims = claims(&token).unwrap();
        assert_eq!(claims["exp"], 5);
        assert_eq!(display_name(&claims), "ana@example.com");
        assert!(super::claims("not-a-token").is_err());
    }
}
