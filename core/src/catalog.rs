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

    #[test]
    fn ids_are_restricted() {
        assert!(valid_id("github-prod_2"));
        assert!(!valid_id(""));
        assert!(!valid_id("Upper"));
        assert!(!valid_id("a/b"));
        assert!(!valid_id(&"a".repeat(65)));
    }
}
