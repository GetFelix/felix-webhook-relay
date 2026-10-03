//! The admin page: plain server-rendered HTML over the same data as the JSON
//! API, with a stylesheet and one short script that sends its forms to the
//! API. Nothing to build and nothing to download.

use std::fmt::Write;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use felix_relay_core::Envelope;
use felix_relay_core::catalog::{Endpoint, Mode, Source};
use felix_relay_core::signature::Scheme;
use serde_json::Value;

use super::jobs::dead_letters;
use crate::catalog::STATE;
use crate::config::Config;
use crate::session::{self, Denied, Identity};
use crate::tenant::Tenant;
use crate::{App, unix_millis};

pub(super) fn routes() -> axum::Router<Arc<App>> {
    axum::Router::new()
        .route("/", get(index))
        .route("/admin/{tenant}", get(page))
}

async fn index(State(app): State<Arc<App>>) -> Html<String> {
    let mut list = String::new();
    for tenant in &app.config.tenants {
        let _ = write!(
            list,
            r#"<li><a href="/admin/{0}">{0}</a></li>"#,
            esc(tenant)
        );
    }
    Html(layout(
        "Felix Webhook Relay",
        &format!("<h1>Tenants</h1><ul>{list}</ul>"),
    ))
}

async fn page(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Response {
    match session::admin_tenant(&app, &headers, &tenant).await {
        Ok((open, who)) => match render(&app.config, &open, &who).await {
            Ok(html) => Html(html).into_response(),
            Err(err) => {
                tracing::error!("admin page: {err:#}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Html(layout(
                        "Unavailable",
                        "<p>Felix did not answer. Try again.</p>",
                    )),
                )
                    .into_response()
            }
        },
        Err(Denied::SignedOut) => {
            Redirect::to(&format!("/auth/login?return_to=/admin/{tenant}")).into_response()
        }
        Err(Denied::Forbidden(reason)) => {
            let who = session::identity(&app, &headers).map_or("You".to_string(), |w| w.name);
            let body = format!(
                "<h1>Not allowed</h1><p>{} cannot administer <b>{}</b>. Felix's control plane \
                 refused the token exchange, which is where this is decided:</p><pre>{}</pre>\
                 <p><a href=\"/auth/logout\">Sign out</a></p>",
                esc(&who),
                esc(&tenant),
                esc(&reason)
            );
            (StatusCode::FORBIDDEN, Html(layout("Not allowed", &body))).into_response()
        }
        Err(Denied::Unavailable(err)) => {
            tracing::error!("admin page: {err:#}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Html(layout("Unavailable", "<p>Sign-in is unavailable.</p>")),
            )
                .into_response()
        }
    }
}

async fn render(config: &Config, tenant: &Tenant, who: &Identity) -> anyhow::Result<String> {
    let name = esc(&tenant.name);
    let api = format!("/api/{}", tenant.name);
    let catalog = Arc::clone(&tenant.catalog.borrow());
    let mut sources: Vec<(&String, &Source)> = catalog.sources.iter().collect();
    let mut endpoints: Vec<(&String, &Endpoint)> = catalog.endpoints.iter().collect();
    sources.sort_by_key(|(id, _)| *id);
    endpoints.sort_by_key(|(id, _)| *id);
    let felix = &tenant.felix;

    let mut tails = std::collections::HashMap::new();
    let mut out = format!(
        r#"<header><h1>{name}</h1><span>Signed in as {} · <a href="/auth/logout">sign out</a></span></header>"#,
        esc(&who.name)
    );

    for warning in tenant.retention_warnings(config).await {
        let _ = write!(out, "<p class=bad><b>Retention:</b> {}</p>", esc(&warning));
    }
    out.push_str("<h2>Sources</h2><table><tr><th>Source</th><th>Scheme</th><th>Received</th><th>Tail</th><th>Oldest held</th></tr>");
    for (id, source) in &sources {
        let stream = format!("src.{id}");
        let (oldest, tail) = felix.bounds(&stream).await.unwrap_or((0, 0));
        tails.insert(id.to_string(), tail);
        let oldest_at = match felix.read(&stream, oldest, tail, 1).await {
            Ok(records) => records
                .first()
                .and_then(|(_, payload)| Envelope::decode(payload).ok())
                .map_or("empty".to_string(), |e| time(e.received_at)),
            Err(_) => "unknown".to_string(),
        };
        let scheme = match &source.scheme {
            Scheme::StandardWebhooks => "Standard Webhooks".to_string(),
            Scheme::Github => "GitHub".to_string(),
            Scheme::Stripe => "Stripe".to_string(),
            Scheme::Hmac { header, .. } => format!("HMAC in {header}"),
            Scheme::Token => "URL token (weaker: anyone with the URL can send)".to_string(),
        };
        let received = felix.counter(&format!("received/{id}")).await.unwrap_or(0);
        let _ = write!(
            out,
            "<tr><td><code>{}</code></td><td>{}</td><td>{received}</td><td>{tail}</td><td>offset {oldest}, {}</td></tr>",
            esc(id),
            esc(&scheme),
            esc(&oldest_at)
        );
    }
    out.push_str("</table>");
    let _ = write!(
        out,
        r#"<details><summary>Add or change a source</summary>
<form data-api="{api}/sources/{{id}}" data-method="PUT" data-keep="1">
<label>Id <input name="_id" required pattern="[a-z0-9_-]+"></label>
<label>Scheme <select name="scheme.type"><option value="standard-webhooks">Standard Webhooks</option><option value="github">GitHub</option><option value="stripe">Stripe</option><option value="hmac">HMAC header</option><option value="token">URL token</option></select></label>
<label>HMAC header <input name="scheme.header" placeholder="x-signature"></label>
<label>Secret <input name="secret" type="password" placeholder="made up if empty"></label>
<label>Event id header <input name="event_id.header" placeholder="webhook-id"></label>
<label>Event type header <input name="event_type_header"></label>
<button>Save</button><output></output></form></details>"#
    );

    out.push_str("<h2>Endpoints</h2><table><tr><th>Endpoint</th><th>Delivery</th><th>State</th><th>Lag</th><th>Delivered / failed / dead</th><th></th></tr>");
    for (id, endpoint) in &endpoints {
        let health: Value = felix
            .cache_get(STATE, &format!("health/{id}"))
            .await
            .ok()
            .flatten()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or(Value::Null);
        let tail = tails.get(&endpoint.source).copied().unwrap_or(0);
        let next = health["last_acked"]
            .as_u64()
            .map_or(endpoint.start_offset, |acked| acked + 1);
        let lag = tail.saturating_sub(next);
        let state = match &endpoint.disabled {
            Some(disabled) => format!("<b class=bad>disabled</b>: {}", esc(&disabled.reason)),
            None => {
                let state = health["state"].as_str().unwrap_or("not reported");
                let error = health["last_error"]
                    .as_str()
                    .map(|e| format!("<br><small>{}</small>", esc(e)))
                    .unwrap_or_default();
                format!("<b class={state}>{state}</b>{error}")
            }
        };
        let reporter = health["reporter"].as_str().unwrap_or("no worker yet");
        let counts = [
            felix.counter(&format!("delivered/{id}")).await.unwrap_or(0),
            felix.counter(&format!("failed/{id}")).await.unwrap_or(0),
            felix.counter(&format!("dead/{id}")).await.unwrap_or(0),
        ];
        let mode = match endpoint.mode {
            Mode::Ordered => "ordered".to_string(),
            Mode::Unordered => format!("unordered, {} at once", endpoint.window),
        };
        let filter = if endpoint.event_types.is_empty() {
            String::new()
        } else {
            format!(
                "<br><small>only {}</small>",
                esc(&endpoint.event_types.join(", "))
            )
        };
        let path = format!("{api}/endpoints/{}", esc(id));
        let _ = write!(
            out,
            r#"<tr><td><code>{}</code><br><small>{} from {}</small></td><td>{mode}{filter}</td><td>{state}<br><small>{}</small></td><td>{lag}</td><td>{} / {} / {}</td><td class=actions>
{}{}{}{}
<details><summary>Replay</summary><form data-api="{path}/replays" data-keep="1">
<label>From <input name="since" type="datetime-local" data-type="time" step="1"></label>
<label>Until <input name="until" type="datetime-local" data-type="time" step="1"></label>
<label><input name="pause_live" type="checkbox"> hold live delivery until done</label>
<button>Replay</button><output></output></form></details></td></tr>"#,
            esc(id),
            esc(&endpoint.url),
            esc(&endpoint.source),
            esc(reporter),
            counts[0],
            counts[1],
            counts[2],
            button(&format!("{path}/retry"), "POST", "Retry now", false),
            if endpoint.disabled.is_some() {
                button(&format!("{path}/enable"), "POST", "Enable", false)
            } else {
                String::new()
            },
            button(&format!("{path}/secret"), "POST", "Rotate secret", true),
            button(&path, "DELETE", "Delete", false),
        );
    }
    out.push_str("</table>");
    let mut options = String::new();
    for (id, _) in &sources {
        let _ = write!(options, "<option>{}</option>", esc(id));
    }
    let _ = write!(
        out,
        r#"<details><summary>Add or change an endpoint</summary>
<form data-api="{api}/endpoints/{{id}}" data-method="PUT" data-keep="1">
<label>Id <input name="_id" required pattern="[a-z0-9_-]+"></label>
<label>Source <select name="source">{options}</select></label>
<label>URL <input name="url" type="url" required></label>
<label>Mode <select name="mode"><option>ordered</option><option>unordered</option></select></label>
<label>Window <input name="window" data-type="number" placeholder="16"></label>
<label>Event types <input name="event_types" data-type="list" placeholder="all"></label>
<label><input name="backfill" type="checkbox"> start from the beginning of the log</label>
<button>Save</button><output></output></form></details>"#
    );

    let dead = dead_letters(tenant)
        .await
        .map_err(|_| anyhow::anyhow!("dead letters"))?;
    out.push_str("<h2>Dead letters</h2><table><tr><th>Event</th><th>Endpoint</th><th>Last answer</th><th>When</th><th></th></tr>");
    for letter in dead["relay"].as_array().into_iter().flatten() {
        let offset = letter["offset"].as_u64().unwrap_or(0);
        let actions = match letter["mark"]["status"].as_str() {
            Some(status) => status.to_string(),
            None => format!(
                "{}{}",
                button(
                    &format!("{api}/dead/{offset}/redrive"),
                    "POST",
                    "Redrive",
                    false
                ),
                button(
                    &format!("{api}/dead/{offset}/discard"),
                    "POST",
                    "Discard",
                    false
                )
            ),
        };
        let _ = write!(
            out,
            "<tr><td><code>{}</code><br><small>{} attempts</small></td><td>{}</td><td>{} <small>{}</small></td><td>{}</td><td class=actions>{actions}</td></tr>",
            esc(letter["event_id"].as_str().unwrap_or("")),
            letter["attempts"],
            esc(letter["endpoint"].as_str().unwrap_or("")),
            letter["last_status"],
            esc(letter["last_response"].as_str().unwrap_or("")),
            time(letter["at"].as_u64().unwrap_or(0)),
        );
    }
    out.push_str("</table>");
    let broker = dead["broker"].as_array().cloned().unwrap_or_default();
    if !broker.is_empty() {
        out.push_str("<h3 class=bad>Records Felix gave up on</h3><p>Each was claimed too many times without an acknowledgement, which means workers kept dying on it.</p><table><tr><th>Endpoint</th><th>Offset</th><th></th></tr>");
        for letter in broker {
            let (endpoint, offset) = (
                letter["endpoint"].as_str().unwrap_or(""),
                letter["offset"].as_u64().unwrap_or(0),
            );
            let path = format!("{api}/endpoints/{}/broker-dead/{offset}", esc(endpoint));
            let _ = write!(
                out,
                "<tr><td><code>{}</code></td><td><a href=\"{api}/events/{}/{offset}\">{offset}</a></td><td class=actions>{}{}</td></tr>",
                esc(endpoint),
                esc(letter["source"].as_str().unwrap_or("")),
                button(&format!("{path}/redrive"), "POST", "Redrive", false),
                button(&format!("{path}/discard"), "POST", "Discard", false)
            );
        }
        out.push_str("</table>");
    }
    Ok(layout(
        &format!("{} · Felix Webhook Relay", tenant.name),
        &out,
    ))
}

/// A form that is one button calling the API. `keep` leaves the answer on
/// screen instead of reloading, for answers that hold a new secret.
fn button(api: &str, method: &str, label: &str, keep: bool) -> String {
    let keep = if keep { r#" data-keep="1""# } else { "" };
    format!(
        r#"<form data-api="{api}" data-method="{method}"{keep}><button>{label}</button><output></output></form>"#
    )
}

fn time(millis: u64) -> String {
    let ago = unix_millis().saturating_sub(millis) / 1000;
    match ago {
        0..60 => format!("{ago} s ago"),
        60..3600 => format!("{} min ago", ago / 60),
        3600..86400 => format!("{} h ago", ago / 3600),
        _ => format!("{} days ago", ago / 86400),
    }
}

fn esc(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn layout(title: &str, body: &str) -> String {
    format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{}</title><style>{STYLE}</style></head><body>{body}<script>{SCRIPT}</script></body></html>"#,
        esc(title)
    )
}

const STYLE: &str = "
body { font: 15px/1.45 system-ui, sans-serif; margin: 0 auto; max-width: 72rem; padding: 1rem; color: #1d2329; background: #fbfcfd; }
header { display: flex; justify-content: space-between; align-items: baseline; flex-wrap: wrap; }
table { border-collapse: collapse; width: 100%; margin: .5rem 0 1rem; }
th, td { text-align: left; padding: .4rem .5rem; border-bottom: 1px solid #dde3e8; vertical-align: top; }
th { font-weight: 600; font-size: 13px; color: #56616b; }
code { font: 13px ui-monospace, monospace; }
small { color: #56616b; }
.paused, .bad { color: #b45309; } .disabled { color: #b91c1c; } .active { color: #15803d; }
.actions form { display: inline-block; margin: 0 .25rem .25rem 0; }
form label { display: inline-block; margin: .25rem .75rem .25rem 0; }
details { margin: .25rem 0; } summary { cursor: pointer; color: #0e7490; }
button { font: inherit; padding: .2rem .6rem; border: 1px solid #0e7490; background: #fff; color: #0e7490; border-radius: 4px; cursor: pointer; }
output { display: block; font: 12px ui-monospace, monospace; white-space: pre-wrap; color: #56616b; }
";

/// Sends each form to the API as JSON. A field named `a.b` becomes
/// `{"a": {"b": ...}}`, and `{id}` in the form's address takes the `_id` field.
const SCRIPT: &str = r#"
document.addEventListener('submit', async (event) => {
  const form = event.target;
  if (!form.dataset.api) return;
  event.preventDefault();
  const body = {};
  for (const field of form.elements) {
    if (!field.name || (field.type === 'checkbox' ? !field.checked : field.value === '')) continue;
    let value = field.type === 'checkbox' ? true : field.value;
    if (field.dataset.type === 'number') value = Number(value);
    if (field.dataset.type === 'time') value = new Date(value).getTime();
    if (field.dataset.type === 'list') value = value.split(',').map((s) => s.trim()).filter(Boolean);
    const path = field.name.split('.');
    let target = body;
    while (path.length > 1) { const key = path.shift(); target = target[key] ??= {}; }
    target[path[0]] = value;
  }
  const url = form.dataset.api.replace('{id}', encodeURIComponent(body._id ?? ''));
  delete body._id;
  const response = await fetch(url, { method: form.dataset.method || 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  const answer = await response.text();
  if (response.ok && !form.dataset.keep) location.reload();
  else form.querySelector('output').textContent = `${response.status} ${answer}`;
});
"#;
