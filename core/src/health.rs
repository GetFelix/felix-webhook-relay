//! What an endpoint's answers mean, and what the delivery task does next.
//!
//! Backoff is per endpoint, not per record: an unwell endpoint pauses as a
//! whole and probes with the record at its head, because an ordered endpoint
//! cannot skip ahead of it.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// One request's result, read for what it says about the endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// `2xx`.
    Delivered,
    /// A `4xx` other than `408`, `425`, `429` and `410`: this record is refused.
    Refused,
    /// `408`, `425`, `429`, `5xx`, a timeout or no connection: the endpoint is unwell.
    Unwell { retry_after: Option<Duration> },
    /// `410 Gone`: the endpoint is retired.
    Gone,
}

impl Outcome {
    /// `status` is `None` when no response came back at all.
    pub fn classify(status: Option<u16>, retry_after: Option<Duration>) -> Self {
        match status {
            Some(200..=299) => Self::Delivered,
            Some(410) => Self::Gone,
            Some(408 | 425 | 429) => Self::Unwell { retry_after },
            Some(400..=499) => Self::Refused,
            // 1xx and 3xx too: redirects are not followed, so they cannot succeed.
            _ => Self::Unwell { retry_after },
        }
    }
}

/// A `Retry-After` in seconds. The HTTP-date form is ignored, which only
/// means the backoff schedule applies instead.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    value.trim().parse().ok().map(Duration::from_secs)
}

/// The timings the state machine follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    /// The waits before each retry of a refused record. With two, a record
    /// is tried three times before it is dead-lettered.
    pub refused_retries: Vec<Duration>,
    /// The waits between probes of a paused endpoint; the last repeats.
    pub backoff: Vec<Duration>,
    /// How long an endpoint may fail without a break before it is disabled.
    pub disable_after: Duration,
}

/// The longest `Retry-After` honoured; a longer one gets the schedule.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);
/// Each backoff wait grows by up to this fraction, so endpoints paused
/// together do not all probe at once.
pub const JITTER: f64 = 0.2;

impl Default for Policy {
    fn default() -> Self {
        let secs = Duration::from_secs;
        Self {
            refused_retries: vec![secs(5), secs(30)],
            backoff: vec![secs(5), secs(15), secs(60), secs(120), secs(300)],
            disable_after: secs(72 * 60 * 60),
        }
    }
}

/// What the delivery task does after an attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Acknowledge the record and move on.
    Ack,
    /// Send the same record again after the wait; the endpoint is fine.
    Retry(Duration),
    /// Record it in the `dead` stream, then acknowledge it and move on.
    DeadLetter,
    /// Poll nothing, and probe with the same record after the wait.
    Pause(Duration),
    /// Stop until an operator enables the endpoint again.
    Disable(String),
}

/// Where an endpoint stands, as its health entry reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Active,
    Paused,
    Disabled,
}

/// One endpoint's delivery state.
#[derive(Debug, Clone)]
pub struct Health {
    pub state: State,
    /// Unix milliseconds of the first failure in the current unbroken run.
    pub failing_since: Option<u64>,
    /// Probes sent since the endpoint paused.
    probes: usize,
    /// Refusals of the record at the head.
    refusals: usize,
}

impl Default for Health {
    fn default() -> Self {
        Self {
            state: State::Active,
            failing_since: None,
            probes: 0,
            refusals: 0,
        }
    }
}

impl Health {
    /// Fold in one attempt at `now` (Unix milliseconds). `jitter` is a
    /// random number in `[0, 1)`.
    pub fn decide(&mut self, outcome: Outcome, now: u64, jitter: f64, policy: &Policy) -> Decision {
        match outcome {
            Outcome::Delivered => {
                *self = Self::default();
                Decision::Ack
            }
            Outcome::Refused => {
                // An answer, even a refusal, means the endpoint is up.
                self.state = State::Active;
                self.failing_since = None;
                self.probes = 0;
                match policy.refused_retries.get(self.refusals) {
                    Some(wait) => {
                        self.refusals += 1;
                        Decision::Retry(*wait)
                    }
                    None => {
                        self.refusals = 0;
                        Decision::DeadLetter
                    }
                }
            }
            Outcome::Gone => {
                self.state = State::Disabled;
                Decision::Disable("the endpoint answered 410 Gone".to_string())
            }
            Outcome::Unwell { retry_after } => {
                let since = *self.failing_since.get_or_insert(now);
                if Duration::from_millis(now.saturating_sub(since)) >= policy.disable_after {
                    self.state = State::Disabled;
                    return Decision::Disable(format!(
                        "failing without a break for {} s",
                        policy.disable_after.as_secs()
                    ));
                }
                self.state = State::Paused;
                let scheduled = policy.backoff[self.probes.min(policy.backoff.len() - 1)];
                self.probes += 1;
                let wait = match retry_after {
                    Some(asked) if asked <= MAX_RETRY_AFTER => asked,
                    _ => scheduled.mul_f64(1.0 + JITTER * jitter.clamp(0.0, 1.0)),
                };
                Decision::Pause(wait)
            }
        }
    }

    /// An operator enabled the endpoint again: start over, as if it had
    /// never failed.
    pub fn enable(&mut self) {
        *self = Self::default();
    }
}

/// Parse `500ms`, `5s`, `2m`, `1h` or `3d`.
pub fn parse_duration(value: &str) -> Option<Duration> {
    let value = value.trim();
    let split = value.find(|c: char| !c.is_ascii_digit())?;
    let (number, unit) = value.split_at(split);
    let number: u64 = number.parse().ok()?;
    let millis = match unit {
        "ms" => 1,
        "s" => 1000,
        "m" => 60 * 1000,
        "h" => 60 * 60 * 1000,
        "d" => 24 * 60 * 60 * 1000,
        _ => return None,
    };
    Some(Duration::from_millis(number.checked_mul(millis)?))
}

/// Parse a comma-separated list of durations.
pub fn parse_durations(value: &str) -> Option<Vec<Duration>> {
    value.split(',').map(parse_duration).collect()
}

#[cfg(test)]
mod tests;
