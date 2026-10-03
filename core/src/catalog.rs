//! Sources and endpoints as the `config` cache holds them, as JSON under
//! `source/<id>` and `endpoint/<id>`.

use serde::{Deserialize, Serialize};

use crate::secret::Sealed;
use crate::signature::Scheme;

/// How long a rotated-out endpoint secret still signs deliveries, so
/// receivers can roll their own config over without dropping any.
pub const ROTATION_OVERLAP_MS: u64 = 24 * 60 * 60 * 1000;

/// One place webhooks come in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    pub scheme: Scheme,
    /// The verification secret, or the URL token for [`Scheme::Token`].
    pub secret: Sealed,
    /// Where the sender puts its own event id. Without one, the id is
    /// `<source>:<offset>` and intake cannot dedupe retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<EventIdFrom>,
    /// The request header that names the event type, lowercase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type_header: Option<String>,
    /// Request headers stored with the body, lowercase.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keep_headers: Vec<String>,
}

/// Where a source's sender puts its event id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventIdFrom {
    /// A request header, such as `webhook-id` or `x-github-delivery`.
    Header(String),
    /// A dot-separated path into a JSON body, such as `id` or `data.object.id`.
    Json(String),
}

/// One place webhooks go.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// The source whose webhooks it receives.
    pub source: String,
    pub url: String,
    /// The `whsec_` secret deliveries are signed with.
    pub secret: Sealed,
    /// The secret before the last rotation, which also signs until `until`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_secret: Option<PreviousSecret>,
    #[serde(default)]
    pub mode: Mode,
    /// Requests in flight at once for an unordered endpoint.
    #[serde(default = "default_window")]
    pub window: u32,
    /// The event types it receives; empty means all of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_types: Vec<String>,
    /// Records below this offset are acknowledged without being sent. Set to
    /// the source's tail when the endpoint is created, because a new Felix
    /// group starts at the beginning of the log; zero backfills.
    #[serde(default)]
    pub start_offset: u64,
    /// Set by the endpoint's worker when it gives up on the endpoint, and
    /// cleared by an operator. Its records wait in the log meanwhile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled: Option<Disabled>,
}

/// Whether an endpoint gets its webhooks in intake order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// One request at a time, in intake order.
    #[default]
    Ordered,
    /// Up to `window` requests at a time, in no particular order.
    Unordered,
}

pub const DEFAULT_WINDOW: u32 = 16;

fn default_window() -> u32 {
    DEFAULT_WINDOW
}

impl Endpoint {
    /// Whether the record at `offset` with this event type is sent at all.
    pub fn wants(&self, offset: u64, event_type: Option<&str>) -> bool {
        offset >= self.start_offset && self.wants_type(event_type)
    }

    /// Whether its filter lets this event type through.
    pub fn wants_type(&self, event_type: Option<&str>) -> bool {
        self.event_types.is_empty()
            || event_type.is_some_and(|t| self.event_types.iter().any(|want| want == t))
    }

    /// Requests it may have in flight at once.
    pub fn in_flight(&self) -> u32 {
        match self.mode {
            Mode::Ordered => 1,
            Mode::Unordered => self.window.max(1),
        }
    }
}

/// Which of `count` delivery processes owns an endpoint. FNV-1a, so every
/// process agrees without talking to the others.
pub fn owner(endpoint: &str, count: u32) -> u32 {
    let hash = endpoint
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        });
    (hash % u64::from(count.max(1))) as u32
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Disabled {
    pub reason: String,
    /// Unix milliseconds.
    pub at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviousSecret {
    pub secret: Sealed,
    /// Unix milliseconds.
    pub until: u64,
}

/// The `config` cache key of a source.
pub fn source_key(id: &str) -> String {
    format!("source/{id}")
}

/// The `config` cache key of an endpoint.
pub fn endpoint_key(id: &str) -> String {
    format!("endpoint/{id}")
}

/// Whether `id` can name a source or endpoint. Ids end up in stream, group
/// and cache key names, so they are kept to lowercase letters, digits, `-`
/// and `_`.
pub fn valid_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

impl EventIdFrom {
    /// The sender's id for one request, if it gave one.
    pub fn extract<'a>(
        &self,
        header: impl Fn(&str) -> Option<&'a str>,
        body: &[u8],
    ) -> Option<String> {
        let id = match self {
            Self::Header(name) => header(name).map(str::to_string),
            Self::Json(path) => {
                let mut value: serde_json::Value = serde_json::from_slice(body).ok()?;
                for field in path.split('.') {
                    value = value.get_mut(field)?.take();
                }
                match value {
                    serde_json::Value::String(id) => Some(id),
                    serde_json::Value::Number(id) => Some(id.to_string()),
                    _ => None,
                }
            }
        };
        id.filter(|id| !id.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_come_from_a_header() {
        let from = EventIdFrom::Header("x-github-delivery".to_string());
        let header = |name: &str| (name == "x-github-delivery").then_some("72d3162e");
        assert_eq!(from.extract(header, b""), Some("72d3162e".to_string()));
        assert_eq!(from.extract(|_| None, b""), None);
        assert_eq!(from.extract(|_| Some(""), b""), None);
    }

    #[test]
    fn ids_come_from_a_json_path() {
        let body = br#"{"id":"evt_1","data":{"object":{"id":42}},"list":[1]}"#;
        let at = |path: &str| EventIdFrom::Json(path.to_string()).extract(|_| None, body);
        assert_eq!(at("id"), Some("evt_1".to_string()));
        assert_eq!(at("data.object.id"), Some("42".to_string()));
        assert_eq!(at("data.missing"), None);
        assert_eq!(at("list"), None);
        assert_eq!(
            EventIdFrom::Json("id".to_string()).extract(|_| None, b"not json"),
            None
        );
    }

    #[test]
    fn config_reads_from_json() {
        let source: Source = serde_json::from_str(
            r#"{"scheme":{"type":"github"},"secret":"c2VhbGVk","event_id":{"header":"x-github-delivery"}}"#,
        )
        .unwrap();
        assert_eq!(source.scheme, Scheme::Github);
        assert_eq!(
            source.event_id,
            Some(EventIdFrom::Header("x-github-delivery".to_string()))
        );
        assert!(source.keep_headers.is_empty());
    }

    fn endpoint() -> Endpoint {
        serde_json::from_str(r#"{"source":"s","url":"http://e","secret":"x"}"#).unwrap()
    }

    #[test]
    fn endpoints_default_to_ordered_from_the_start() {
        let endpoint = endpoint();
        assert_eq!(endpoint.mode, Mode::Ordered);
        assert_eq!(endpoint.in_flight(), 1);
        assert_eq!(endpoint.window, DEFAULT_WINDOW);
        assert!(endpoint.wants(0, None));
    }

    #[test]
    fn filters_and_start_offsets_pick_records() {
        let mut endpoint = endpoint();
        endpoint.start_offset = 10;
        endpoint.event_types = vec!["invoice.paid".to_string()];
        assert!(endpoint.wants(10, Some("invoice.paid")));
        assert!(!endpoint.wants(9, Some("invoice.paid")));
        assert!(!endpoint.wants(10, Some("invoice.created")));
        assert!(!endpoint.wants(10, None));
        endpoint.mode = Mode::Unordered;
        assert_eq!(endpoint.in_flight(), 16);
    }

    #[test]
    fn owners_split_endpoints_evenly_and_agree() {
        let mut counts = [0; 4];
        for n in 0..1000 {
            counts[owner(&format!("ep-{n}"), 4) as usize] += 1;
        }
        assert!(
            counts.iter().all(|&c| (200..300).contains(&c)),
            "{counts:?}"
        );
        assert_eq!(owner("billing", 3), owner("billing", 3));
        assert_eq!(owner("billing", 1), 0);
        assert_eq!(owner("billing", 0), 0);
    }

    #[test]
    fn ids_are_restricted() {
        assert!(valid_id("github-prod_2"));
        assert!(!valid_id(""));
        assert!(!valid_id("Upper"));
        assert!(!valid_id("a/b"));
        assert!(!valid_id(&"a".repeat(65)));
    }
}
