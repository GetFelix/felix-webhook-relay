//! Replay and redrive jobs. They run beside live delivery and never touch the
//! endpoint's group: a replay reads the source with a plain subscription, and
//! a redrive sends the copy kept with the dead letter.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_client::StartPosition;
use felix_relay_core::Envelope;
use felix_relay_core::health::{Decision, Health, Outcome};
use felix_relay_core::jobs::{
    CHECKPOINT_EVERY, DeadMark, DeadStatus, Job, JobKind, JobStatus, in_window,
};
use felix_relay_core::records::DeadLetter;

use super::send::{Sender, jitter};
use crate::catalog::STATE;
use crate::felix::Felix;
use crate::unix_millis;

/// Finished jobs stay readable this long, then expire from the cache.
const KEEP_FINISHED: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// A replay's records are already in the log, so this long without one
/// means none are left below its end.
const QUIET: Duration = Duration::from_secs(10);

pub(super) async fn run(sender: Sender, id: String, mut job: Job) {
    job.status = JobStatus::Running;
    save(&sender.app.felix, &id, &mut job).await;
    let result = match job.kind.clone() {
        JobKind::Replay { .. } => replay(&sender, &id, &mut job).await,
        JobKind::Redrive { dead_offset } => redrive(&sender, &id, &mut job, dead_offset).await,
    };
    match result {
        Ok(()) => job.status = JobStatus::Done,
        Err(err) => {
            tracing::warn!(job = %id, "job failed: {err:#}");
            job.status = JobStatus::Failed;
            job.error = Some(format!("{err:#}"));
        }
    }
    save(&sender.app.felix, &id, &mut job).await;
}

async fn save(felix: &Felix, id: &str, job: &mut Job) {
    job.updated_at = unix_millis();
    let ttl = (!job.active()).then_some(KEEP_FINISHED);
    let json = serde_json::to_vec(job).expect("a job serializes");
    if let Err(err) = felix
        .cache_put(STATE, &format!("job/{id}"), json, ttl)
        .await
    {
        tracing::warn!(job = %id, "could not save the job: {err:#}");
    }
}

async fn replay(sender: &Sender, id: &str, job: &mut Job) -> Result<()> {
    let JobKind::Replay {
        to,
        next,
        since,
        until,
        ..
    } = job.kind.clone()
    else {
        unreachable!("a replay job");
    };
    if next >= to {
        return Ok(());
    }
    let felix = &sender.app.felix;
    let mut subscription = felix
        .client()
        .subscribe_from(
            &felix.tenant,
            &felix.namespace,
            &format!("src.{}", sender.source),
            Some(StartPosition::Offset(next)),
        )
        .await?;
    let mut unsaved = 0;
    loop {
        let event = match tokio::time::timeout(QUIET, subscription.next_event()).await {
            Ok(event) => event?.context("the subscription ended")?,
            Err(_) => break,
        };
        let offset = event.offset.context("a record without an offset")?;
        if offset >= to {
            break;
        }
        if let Ok(envelope) = Envelope::decode(&event.payload)
            && in_window(envelope.received_at, since, until)
            && sender
                .endpoint()?
                .wants_type(envelope.event_type.as_deref())
        {
            deliver(sender, id, offset, &envelope).await?;
            job.sent += 1;
        }
        if let JobKind::Replay { next, .. } = &mut job.kind {
            *next = offset + 1;
        }
        unsaved += 1;
        if unsaved >= CHECKPOINT_EVERY {
            save(felix, id, job).await;
            unsaved = 0;
        }
        if offset + 1 >= to {
            break;
        }
    }
    if let JobKind::Replay { next, .. } = &mut job.kind {
        *next = to;
    }
    Ok(())
}

async fn redrive(sender: &Sender, id: &str, job: &mut Job, dead_offset: u64) -> Result<()> {
    let felix = &sender.app.felix;
    let dead = read_dead_letter(felix, dead_offset).await?;
    if dead.endpoint != sender.endpoint {
        bail!(
            "dead letter {dead_offset} belongs to endpoint {}",
            dead.endpoint
        );
    }
    deliver(sender, id, dead.offset, &dead.envelope).await?;
    job.sent += 1;
    let mark = DeadMark {
        status: DeadStatus::Redriven,
        at: unix_millis(),
        job: Some(id.to_string()),
    };
    let json = serde_json::to_vec(&mark).expect("a mark serializes");
    felix
        .cache_put(STATE, &format!("dead/{dead_offset}"), json, None)
        .await
}

/// The record at `offset` in the tenant's `dead` stream.
pub(crate) async fn read_dead_letter(felix: &Felix, offset: u64) -> Result<DeadLetter> {
    match felix.read("dead", offset, offset + 1, 1).await?.pop() {
        Some((_, payload)) => Ok(DeadLetter::decode(&payload)?),
        None => bail!("no dead letter at {offset}"),
    }
}

/// Send one record until the endpoint takes it or refuses it for good.
/// The request carries `webhook-replay: <job>`.
async fn deliver(sender: &Sender, job: &str, offset: u64, envelope: &Envelope) -> Result<()> {
    let id = envelope.event_id(&sender.source, offset);
    let policy = &sender.app.config.policy;
    let mut health = Health::default();
    let mut tries = 0;
    loop {
        let endpoint = sender.endpoint()?;
        if let Some(disabled) = &endpoint.disabled {
            bail!("the endpoint is disabled: {}", disabled.reason);
        }
        let answer = sender.send(&endpoint, &id, envelope, Some(job)).await;
        tries += 1;
        sender.log_attempt(offset, &id, &answer);
        let outcome = Outcome::classify(answer.status, answer.retry_after);
        match health.decide(outcome, answer.at, jitter(), policy) {
            Decision::Ack => return Ok(()),
            Decision::Retry(wait) | Decision::Pause(wait) => tokio::time::sleep(wait).await,
            Decision::DeadLetter => {
                sender
                    .dead_letter(offset, envelope.clone(), tries, answer)
                    .await;
                return Ok(());
            }
            // Disabling is for the live task to decide; the job just stops.
            Decision::Disable(reason) => bail!(reason),
        }
    }
}
