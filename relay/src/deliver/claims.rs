//! Keeping the claims on the records a delivery task holds from lapsing.
//!
//! A claim is extended once it has less than half its length left, by one
//! claim length, never more. A worker that dies therefore leaves claims that
//! lapse within one claim length, which is what the next worker's claim wait
//! covers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use felix_client::{BrokerError, ErrorCode};
use felix_wire::GroupRecord;
use futures_util::future::join_all;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::felix::Felix;

struct Claim {
    record: GroupRecord,
    lapses: Instant,
}

type Held = Arc<Mutex<HashMap<u64, Claim>>>;

/// The claims one task holds, and the background task that extends them.
pub(super) struct Claims {
    held: Held,
    length: Duration,
    keeper: JoinHandle<()>,
}

/// Where one endpoint's group lives.
pub(super) struct Group {
    pub(super) felix: Arc<Felix>,
    pub(super) endpoint: String,
    pub(super) stream: String,
    pub(super) group: String,
}

impl Claims {
    /// `length` is how long a claim stands: the broker's visibility timeout.
    pub(super) fn start(group: Group, length: Duration) -> Self {
        let held = Held::default();
        let keeper = tokio::spawn(keep(group, Arc::clone(&held), length));
        Self {
            held,
            length,
            keeper,
        }
    }

    /// Keep `record`'s claim, made no earlier than `claimed`, from lapsing.
    pub(super) fn hold(&self, record: &GroupRecord, claimed: Instant) {
        let mut record = record.clone();
        record.payload = Default::default();
        let claim = Claim {
            record,
            lapses: claimed + self.length,
        };
        self.lock().insert(claim.record.offset, claim);
    }

    pub(super) fn release(&self, offset: u64) {
        self.lock().remove(&offset);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Claim>> {
        self.held.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Drop for Claims {
    fn drop(&mut self) {
        self.keeper.abort();
    }
}

async fn keep(group: Group, held: Held, length: Duration) {
    let mut tick = tokio::time::interval(length / 4);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let now = Instant::now();
        let due: Vec<GroupRecord> = held
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|claim| claim.lapses <= now + length / 2)
            .map(|claim| claim.record.clone())
            .collect();
        let felix = &group.felix;
        let extended = join_all(due.iter().map(|record| {
            felix.group_client(&group.endpoint).group_extend(
                &felix.tenant,
                &felix.namespace,
                &group.stream,
                0,
                &group.group,
                record,
                length,
            )
        }))
        .await;
        let mut held = held.lock().unwrap_or_else(|e| e.into_inner());
        for (record, extended) in due.iter().zip(extended) {
            let Some(claim) = held
                .get_mut(&record.offset)
                .filter(|claim| claim.record.attempts == record.attempts)
            else {
                continue;
            };
            match extended {
                Ok(granted) => claim.lapses = now + granted,
                // Lapsed, or the broker failed over. The record is owed again,
                // and since the task does not poll while holding it, nobody
                // else takes it; the late ack still settles it.
                Err(err) if is_stale(&err) => {
                    tracing::debug!(offset = record.offset, "claim lapsed");
                    held.remove(&record.offset);
                }
                Err(err) => {
                    tracing::warn!(offset = record.offset, "could not extend a claim: {err:#}");
                }
            }
        }
    }
}

pub(super) fn is_stale(err: &anyhow::Error) -> bool {
    err.downcast_ref::<BrokerError>()
        .is_some_and(|broker| broker.code == ErrorCode::StaleClaim)
}
