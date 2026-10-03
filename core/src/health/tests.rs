use std::collections::{BTreeMap, BTreeSet};

use super::*;

const SECOND: u64 = 1000;
const MINUTE: u64 = 60 * SECOND;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn answers_are_classified() {
    let unwell = Outcome::Unwell { retry_after: None };
    for (status, outcome) in [
        (Some(200), Outcome::Delivered),
        (Some(204), Outcome::Delivered),
        (Some(400), Outcome::Refused),
        (Some(404), Outcome::Refused),
        (Some(422), Outcome::Refused),
        (Some(408), unwell),
        (Some(425), unwell),
        (Some(429), unwell),
        (Some(410), Outcome::Gone),
        (Some(500), unwell),
        (Some(503), unwell),
        (Some(301), unwell),
        (None, unwell),
    ] {
        assert_eq!(Outcome::classify(status, None), outcome, "{status:?}");
    }
    assert_eq!(
        Outcome::classify(Some(429), Some(secs(7))),
        Outcome::Unwell {
            retry_after: Some(secs(7))
        }
    );
}

#[test]
fn a_refused_record_is_tried_three_times_then_dead_lettered() {
    let policy = Policy::default();
    let mut health = Health::default();
    let mut decide = |outcome| health.decide(outcome, 0, 0.0, &policy);
    assert_eq!(decide(Outcome::Refused), Decision::Retry(secs(5)));
    assert_eq!(decide(Outcome::Refused), Decision::Retry(secs(30)));
    assert_eq!(decide(Outcome::Refused), Decision::DeadLetter);
    // The next record starts with a full budget.
    assert_eq!(decide(Outcome::Refused), Decision::Retry(secs(5)));
    assert_eq!(decide(Outcome::Delivered), Decision::Ack);
    assert_eq!(decide(Outcome::Refused), Decision::Retry(secs(5)));
}

#[test]
fn an_outage_in_between_does_not_reset_a_records_refusals() {
    let policy = Policy::default();
    let mut health = Health::default();
    let unwell = Outcome::Unwell { retry_after: None };
    assert_eq!(
        health.decide(Outcome::Refused, 0, 0.0, &policy),
        Decision::Retry(secs(5))
    );
    assert_eq!(
        health.decide(unwell, 0, 0.0, &policy),
        Decision::Pause(secs(5))
    );
    assert_eq!(health.state, State::Paused);
    assert_eq!(
        health.decide(Outcome::Refused, 0, 0.0, &policy),
        Decision::Retry(secs(30))
    );
    assert_eq!(health.state, State::Active);
    assert_eq!(
        health.decide(Outcome::Refused, 0, 0.0, &policy),
        Decision::DeadLetter
    );
}

#[test]
fn an_unwell_endpoint_backs_off_on_the_schedule() {
    let policy = Policy::default();
    let mut health = Health::default();
    let unwell = Outcome::Unwell { retry_after: None };
    let waits: Vec<Decision> = (0..7)
        .map(|n| health.decide(unwell, n * MINUTE, 0.0, &policy))
        .collect();
    let expected = [5, 15, 60, 120, 300, 300, 300].map(|s| Decision::Pause(secs(s)));
    assert_eq!(waits, expected);
    assert_eq!(health.failing_since, Some(0));

    assert_eq!(
        health.decide(Outcome::Delivered, 7 * MINUTE, 0.0, &policy),
        Decision::Ack
    );
    assert_eq!(health.state, State::Active);
    assert_eq!(
        health.decide(unwell, 8 * MINUTE, 0.0, &policy),
        Decision::Pause(secs(5)),
        "a success resets the schedule"
    );
}

#[test]
fn jitter_adds_up_to_a_fifth() {
    let policy = Policy::default();
    let unwell = Outcome::Unwell { retry_after: None };
    let Decision::Pause(wait) = Health::default().decide(unwell, 0, 0.999, &policy) else {
        panic!("a pause");
    };
    assert!(
        wait > secs(5) && wait < Duration::from_millis(6000),
        "{wait:?}"
    );
}

#[test]
fn retry_after_is_honoured_up_to_an_hour() {
    let policy = Policy::default();
    let mut health = Health::default();
    let asked = |s| Outcome::Unwell {
        retry_after: Some(secs(s)),
    };
    assert_eq!(
        health.decide(asked(90), 0, 0.5, &policy),
        Decision::Pause(secs(90))
    );
    assert_eq!(
        health.decide(asked(3601), 0, 0.0, &policy),
        Decision::Pause(secs(15))
    );
    assert_eq!(parse_retry_after(" 120 "), Some(secs(120)));
    assert_eq!(parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT"), None);
}

#[test]
fn an_endpoint_failing_for_the_whole_window_is_disabled() {
    let policy = Policy::default();
    let mut health = Health::default();
    let unwell = Outcome::Unwell { retry_after: None };
    let hour = 60 * MINUTE;
    let start = 1_000 * hour;
    let mut now = start;
    while now < start + 72 * hour {
        assert!(matches!(
            health.decide(unwell, now, 0.0, &policy),
            Decision::Pause(_)
        ));
        now += 5 * MINUTE;
    }
    assert!(matches!(
        health.decide(unwell, start + 72 * hour, 0.0, &policy),
        Decision::Disable(_)
    ));
    assert_eq!(health.state, State::Disabled);

    health.enable();
    assert_eq!(health.state, State::Active);
    assert_eq!(
        health.decide(unwell, start + 73 * hour, 0.0, &policy),
        Decision::Pause(secs(5)),
        "enabling starts the window over"
    );
}

#[test]
fn a_success_breaks_the_failing_window() {
    let policy = Policy {
        disable_after: secs(60),
        ..Policy::default()
    };
    let mut health = Health::default();
    let unwell = Outcome::Unwell { retry_after: None };
    health.decide(unwell, 0, 0.0, &policy);
    health.decide(Outcome::Delivered, 59 * SECOND, 0.0, &policy);
    assert!(matches!(
        health.decide(unwell, 61 * SECOND, 0.0, &policy),
        Decision::Pause(_)
    ));
    assert!(matches!(
        health.decide(unwell, 121 * SECOND, 0.0, &policy),
        Decision::Disable(_)
    ));
}

#[test]
fn gone_disables_at_once() {
    let mut health = Health::default();
    assert!(matches!(
        health.decide(Outcome::Gone, 0, 0.0, &Policy::default()),
        Decision::Disable(_)
    ));
    assert_eq!(health.state, State::Disabled);
}

#[test]
fn durations_parse() {
    assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
    assert_eq!(parse_duration("5s"), Some(secs(5)));
    assert_eq!(parse_duration("2m"), Some(secs(120)));
    assert_eq!(parse_duration("72h"), Some(secs(72 * 3600)));
    assert_eq!(parse_duration("3d"), Some(secs(3 * 86400)));
    assert_eq!(parse_duration("5"), None);
    assert_eq!(parse_duration("s"), None);
    assert_eq!(parse_duration("5x"), None);
    assert_eq!(
        parse_durations("5s, 15s,1m"),
        Some(vec![secs(5), secs(15), secs(60)])
    );
    assert_eq!(parse_durations("5s,,1m"), None);
}

/// A consumer group as Felix keeps it, reduced to what ordering depends on:
/// claims lapse after the visibility timeout and become owed, owed records
/// are handed out before new ones, lowest first, and an acknowledgement
/// settles a record whether or not its claim has lapsed.
struct Group {
    len: u64,
    next: u64,
    claimed: BTreeMap<u64, u64>,
    owed: BTreeSet<u64>,
    claims: BTreeMap<u64, u32>,
    visibility: u64,
}

impl Group {
    fn new(len: u64, visibility: u64) -> Self {
        Self {
            len,
            next: 0,
            claimed: BTreeMap::new(),
            owed: BTreeSet::new(),
            claims: BTreeMap::new(),
            visibility,
        }
    }

    fn poll(&mut self, now: u64, max: usize) -> Vec<u64> {
        let lapsed: Vec<u64> = self
            .claimed
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(offset, _)| *offset)
            .collect();
        for offset in lapsed {
            self.claimed.remove(&offset);
            self.owed.insert(offset);
        }
        let mut out: Vec<u64> = self.owed.iter().copied().take(max).collect();
        for offset in &out {
            self.owed.remove(offset);
        }
        while out.len() < max && self.next < self.len {
            out.push(self.next);
            self.next += 1;
        }
        for offset in &out {
            self.claimed.insert(*offset, now + self.visibility);
            *self.claims.entry(*offset).or_default() += 1;
        }
        out
    }

    fn ack(&mut self, offset: u64) {
        self.claimed.remove(&offset);
        self.owed.remove(&offset);
    }

    fn settled(&self) -> bool {
        self.next == self.len && self.claimed.is_empty() && self.owed.is_empty()
    }
}

#[test]
fn an_outage_longer_than_the_claim_timeout_keeps_order_and_claims_once() {
    let policy = Policy::default();
    let mut group = Group::new(40, 30 * SECOND);
    let mut health = Health::default();
    let back_at = 10 * MINUTE;
    let mut now = 0;
    let mut delivered = Vec::new();
    let mut polls = 0;

    while !group.settled() {
        // The polling rule: poll only with nothing held.
        let batch = group.poll(now, 16);
        polls += 1;
        for offset in batch {
            loop {
                now += 50;
                let outcome = if now < back_at {
                    Outcome::Unwell { retry_after: None }
                } else {
                    Outcome::Delivered
                };
                match health.decide(outcome, now, 0.5, &policy) {
                    Decision::Ack => break,
                    Decision::Pause(wait) | Decision::Retry(wait) => {
                        now += u64::try_from(wait.as_millis()).unwrap();
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
            delivered.push(offset);
            group.ack(offset);
        }
    }

    assert_eq!(delivered, (0..40).collect::<Vec<_>>());
    assert!(
        group.claims.values().all(|claims| *claims == 1),
        "no record was handed out twice: {:?}",
        group.claims
    );
    assert_eq!(polls, 3, "one poll before the outage, then the backlog");
}
