//! What delivery writes back to Felix: one record per attempt in the
//! `attempts` stream, a copy of each given-up record in the `dead` stream,
//! and each endpoint's health entry in the `state` cache.

use serde::{Deserialize, Serialize};

use crate::Envelope;
use crate::health::State;

/// How much of an endpoint's answer is kept with an attempt or dead letter.
pub const RESPONSE_SNIPPET_BYTES: usize = 512;

/// One request to an endpoint. MessagePack, in field order, like the envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub endpoint: String,
    /// The record's offset in its source's stream.
    pub offset: u64,
    pub event_id: String,
    /// Unix milliseconds when the request was sent.
    pub at: u64,
    pub millis: u64,
    /// `None` when no response came back.
    pub status: Option<u16>,
    /// The transport error, or the start of the response body.
    pub detail: String,
}

/// A record an endpoint refused for good, kept with what it said.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub endpoint: String,
    pub source: String,
    pub offset: u64,
    /// Unix milliseconds when it was given up on.
    pub at: u64,
    pub attempts: u32,
    pub last_status: Option<u16>,
    pub last_response: String,
    /// A copy, so a redrive still works after retention trims the source.
    pub envelope: Envelope,
}

/// `state/health/<endpoint>`, JSON, written by the endpoint's worker every
/// few seconds and whenever its state changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthReport {
    pub state: State,
    /// Unix milliseconds of the first failure in the current run of them.
    pub failing_since: Option<u64>,
    pub last_error: Option<String>,
    pub last_acked: Option<u64>,
    /// Gaps in the offsets the group handed out. A gap is usually a record
    /// the broker settled without delivering, but it can be one retention
    /// trimmed before the endpoint reached it (felix#963).
    pub possibly_trimmed: Vec<(u64, u64)>,
    /// Which process wrote this, so two workers on one endpoint show up.
    pub reporter: String,
    /// Unix milliseconds.
    pub updated_at: u64,
}

macro_rules! msgpack {
    ($($record:ty),*) => {$(
        impl $record {
            pub fn encode(&self) -> Vec<u8> {
                rmp_serde::to_vec(self).expect("a record always encodes")
            }

            /// # Errors
            /// When `bytes` is not this kind of record.
            pub fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
                rmp_serde::from_slice(bytes)
            }
        }
    )*};
}

msgpack!(Attempt, DeadLetter);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letters_round_trip_with_their_envelope() {
        let dead = DeadLetter {
            endpoint: "billing".to_string(),
            source: "stripe".to_string(),
            offset: 41,
            at: 1_790_000_000_000,
            attempts: 3,
            last_status: Some(400),
            last_response: "{\"error\":\"bad amount\"}".to_string(),
            envelope: Envelope {
                id: Some("evt_1".to_string()),
                received_at: 1_789_999_999_000,
                event_type: None,
                content_type: Some("application/json".to_string()),
                headers: Vec::new(),
                body: b"{}".to_vec(),
            },
        };
        assert_eq!(DeadLetter::decode(&dead.encode()).unwrap(), dead);
    }

    #[test]
    fn an_attempt_is_small() {
        let attempt = Attempt {
            endpoint: "billing".to_string(),
            offset: 1_000_000,
            event_id: "evt_1PqR2sT3uV4wX5yZ".to_string(),
            at: 1_790_000_000_000,
            millis: 12,
            status: Some(204),
            detail: String::new(),
        };
        assert!(attempt.encode().len() < 80);
        assert_eq!(Attempt::decode(&attempt.encode()).unwrap(), attempt);
    }
}
