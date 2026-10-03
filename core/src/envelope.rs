use serde::{Deserialize, Serialize};

/// One accepted webhook as it is stored in a source's stream.
///
/// Encoded as a MessagePack array in field order, which keeps field names out
/// of every record. A field added later goes at the end with
/// `#[serde(default)]`, so records written before it still decode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// The sender's own event id, when the source says where to find it.
    /// `None` means the id is derived from the record's offset, which intake
    /// does not know until the append is acknowledged. See [`Envelope::event_id`].
    pub id: Option<String>,
    /// Unix milliseconds when intake accepted the webhook. Felix does not
    /// hand its own append timestamp to consumers, so the relay carries one.
    pub received_at: u64,
    pub event_type: Option<String>,
    pub content_type: Option<String>,
    /// The sender's headers the source chose to keep, names lowercased, in
    /// the order they arrived.
    pub headers: Vec<(String, String)>,
    /// The exact bytes received.
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

/// A record that is not an envelope.
#[derive(Debug)]
pub struct DecodeError(rmp_serde::decode::Error);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not a relay envelope: {}", self.0)
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl Envelope {
    /// The bytes to append.
    pub fn encode(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).expect("an envelope always encodes")
    }

    /// Read back what [`Envelope::encode`] wrote.
    ///
    /// # Errors
    /// When `bytes` is not an envelope.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        rmp_serde::from_slice(bytes).map_err(DecodeError)
    }

    /// The id every delivery of this event carries: the sender's id when
    /// there is one, else `<source>:<offset>`, which is stable because a
    /// record's offset never changes.
    pub fn event_id(&self, source: &str, offset: u64) -> String {
        self.id
            .clone()
            .unwrap_or_else(|| format!("{source}:{offset}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(body: Vec<u8>) -> Envelope {
        Envelope {
            id: Some("msg_2KWPBgLlAfxdpx2AI54pPJ85f4W".to_string()),
            received_at: 1_790_000_000_000,
            event_type: Some("invoice.paid".to_string()),
            content_type: Some("application/json".to_string()),
            headers: vec![
                ("user-agent".to_string(), "Stripe/1.0".to_string()),
                ("x-request-id".to_string(), "req_8d1c".to_string()),
            ],
            body,
        }
    }

    #[test]
    fn round_trips() {
        let envelope = sample(b"{\"amount\":100}".to_vec());
        assert_eq!(Envelope::decode(&envelope.encode()).unwrap(), envelope);
    }

    #[test]
    fn round_trips_with_only_required_fields() {
        let envelope = Envelope {
            id: None,
            received_at: 1,
            event_type: None,
            content_type: None,
            headers: Vec::new(),
            body: Vec::new(),
        };
        assert_eq!(Envelope::decode(&envelope.encode()).unwrap(), envelope);
    }

    #[test]
    fn body_bytes_survive_unchanged() {
        let body: Vec<u8> = (0..=255).collect();
        let decoded = Envelope::decode(&sample(body.clone()).encode()).unwrap();
        assert_eq!(decoded.body, body);
    }

    #[test]
    fn a_one_kib_body_stays_small() {
        let encoded = sample(vec![b'x'; 1024]).encode();
        assert!(encoded.len() < 1152, "{} bytes", encoded.len());
    }

    #[test]
    fn a_trailing_field_added_later_reads_old_records() {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct WithMore {
            id: Option<String>,
            received_at: u64,
            event_type: Option<String>,
            content_type: Option<String>,
            headers: Vec<(String, String)>,
            #[serde(with = "serde_bytes")]
            body: Vec<u8>,
            #[serde(default)]
            attempt: u32,
        }
        let bytes = sample(b"old".to_vec()).encode();
        let later: WithMore = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(later.body, b"old");
        assert_eq!(later.attempt, 0);
    }

    #[test]
    fn garbage_is_refused() {
        assert!(Envelope::decode(b"not msgpack").is_err());
    }

    #[test]
    fn event_id_prefers_the_senders_id() {
        let mut envelope = sample(Vec::new());
        assert_eq!(
            envelope.event_id("github", 42),
            "msg_2KWPBgLlAfxdpx2AI54pPJ85f4W"
        );
        envelope.id = None;
        assert_eq!(envelope.event_id("github", 42), "github:42");
    }
}
