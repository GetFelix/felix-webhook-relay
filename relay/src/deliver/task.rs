//! One endpoint's delivery task: poll its group, send what the endpoint
//! wants, signed, and settle each record. A record is settled when the
//! endpoint takes it, when it is dead-lettered after the endpoint refused it
//! for good, or when the endpoint does not want it at all.
//!
//! An ordered endpoint's task never polls while it holds an unsettled record:
//! Felix hands out lapsed claims first, so polling again would let a later
//! record overtake it. The claims of records in hand are extended while they
//! are retried. An unordered endpoint hands a record it will retry back to
//! the group with `group_nack_after`, and keeps polling while it waits.
//!
//! Against a broker without claim control the task only holds records. A
//! claim then lapses during a long retry, and the late acknowledgement
//! settles the record.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::ClusterClient;
use felix_relay_core::Envelope;
use felix_relay_core::catalog::{Disabled, Endpoint, Mode, endpoint_key};
use felix_relay_core::health::{Decision, Health, Outcome, State};
use felix_wire::GroupRecord;
use futures_util::future::join_all;
use tokio::time::Instant;

use super::claims::{self, Claims, Group};
use super::job::save;
use super::send::{Answer, FELIX_RETRY, Sender, jitter};
use crate::catalog::CONFIG;
use crate::report::Reporter;
use crate::tenant::Tenant;
use crate::{App, unix_millis};
use felix_relay_core::jobs::{JobKind, JobStatus};

/// Records an ordered endpoint takes per poll.
const ORDERED_BATCH: u32 = 16;
/// How long the broker may hold a poll open waiting for work.
const POLL_WAIT: Duration = Duration::from_secs(10);

/// A record in hand, not yet settled.
struct Held {
    offset: u64,
    /// The delivery Felix handed out, without its payload.
    claim: GroupRecord,
    id: String,
    envelope: Envelope,
    tries: u32,
    refusals: usize,
}

pub(super) struct Task {
    app: Arc<App>,
    tenant: Arc<Tenant>,
    endpoint: String,
    source: String,
    stream: String,
    group: String,
    mode: Mode,
    window: u32,
    reporter: Reporter,
    sender: Sender,
    health: Health,
    /// Set between deciding to disable the endpoint and seeing that in config.
    disabling: bool,
    last_offset: Option<u64>,
    /// `None` when the broker cannot extend claims or delay nacks.
    claims: Option<Claims>,
    /// Tries and refusals of records handed back to wait out a retry.
    backing_off: HashMap<u64, (u32, usize)>,
}

impl Task {
    pub(super) fn new(
        app: Arc<App>,
        tenant: Arc<Tenant>,
        id: &str,
        endpoint: &Endpoint,
        http: reqwest::Client,
    ) -> Self {
        Self {
            reporter: Reporter::start(Arc::clone(&tenant.felix), id, app.config.reporter.clone()),
            endpoint: id.to_string(),
            source: endpoint.source.clone(),
            stream: format!("src.{}", endpoint.source),
            group: format!("ep.{id}"),
            mode: endpoint.mode,
            window: endpoint.in_flight(),
            sender: Sender {
                app: Arc::clone(&app),
                tenant: Arc::clone(&tenant),
                endpoint: id.to_string(),
                source: endpoint.source.clone(),
                http,
            },
            health: Health::default(),
            disabling: false,
            last_offset: None,
            claims: None,
            backing_off: HashMap::new(),
            tenant,
            app,
        }
    }

    fn felix(&self) -> &ClusterClient {
        self.tenant.felix.group_client(&self.endpoint)
    }

    pub(super) async fn run(mut self) -> Result<()> {
        self.enabled().await?;
        tracing::info!(stream = %self.stream, group = %self.group, "delivering");
        // Claims carry no consumer identity (felix#962), so the claims of a
        // run before this one come back only once they lapse. Polling before
        // then would hand out newer records ahead of them.
        tokio::time::sleep(self.app.config.claim_wait).await;
        let felix = Arc::clone(&self.tenant.felix);
        if self.felix().client().await.supports_group_claim_control() {
            let group = Group {
                felix: Arc::clone(&felix),
                endpoint: self.endpoint.clone(),
                stream: self.stream.clone(),
                group: self.group.clone(),
            };
            self.claims = Some(Claims::start(group, self.app.config.claim_wait));
        }
        let batch = match self.mode {
            Mode::Ordered => ORDERED_BATCH,
            Mode::Unordered => self.window,
        };
        loop {
            self.wait_for_jobs_that_pause_live().await?;
            let claimed = Instant::now();
            let polled = self
                .felix()
                .group_poll_wait(
                    &felix.tenant,
                    &felix.namespace,
                    &self.stream,
                    0,
                    &self.group,
                    batch,
                    POLL_WAIT,
                )
                .await;
            self.app
                .metrics
                .polls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let records = match polled {
                Ok(records) => records,
                Err(err) => {
                    tracing::warn!(group = %self.group, "poll failed: {err:#}");
                    tokio::time::sleep(FELIX_RETRY).await;
                    continue;
                }
            };
            let polled_at = unix_millis();
            let mut wanted = Vec::new();
            let mut skipped = Vec::new();
            for record in records {
                let offset = record.offset;
                match self.take(record, polled_at).await? {
                    Some(held) => {
                        if let Some(claims) = &self.claims {
                            claims.hold(&held.claim, claimed);
                        }
                        wanted.push(held);
                    }
                    None => skipped.push(offset),
                }
            }
            // Records it does not want, or that predate it, settle together:
            // a new endpoint on a long log acknowledges its way past all of it.
            join_all(skipped.into_iter().map(|offset| self.ack(offset))).await;
            match self.mode {
                Mode::Ordered => {
                    for held in wanted {
                        self.settle(vec![held]).await?;
                    }
                }
                Mode::Unordered => self.settle(wanted).await?,
            }
        }
    }

    async fn wait_for_jobs_that_pause_live(&self) -> Result<()> {
        let mut jobs = self.tenant.jobs.clone();
        while jobs
            .borrow_and_update()
            .0
            .values()
            .any(|job| job.endpoint == self.endpoint && job.pauses_live())
        {
            jobs.changed().await.context("the job watch stopped")?;
        }
        Ok(())
    }

    /// A polled record to send, or `None` for one to acknowledge unsent.
    async fn take(&mut self, mut record: GroupRecord, polled_at: u64) -> Result<Option<Held>> {
        let offset = record.offset;
        if let Some(last) = self.last_offset
            && offset > last + 1
        {
            self.reporter.gap(last + 1, offset - 1);
        }
        // A record handed back after a backoff comes in below the last one.
        self.last_offset = self.last_offset.max(Some(offset));
        let backed_off = self.backing_off.remove(&offset);
        let envelope = match Envelope::decode(&record.payload) {
            Ok(envelope) => envelope,
            Err(err) => {
                // Nothing can deliver it, and holding it would stop the endpoint.
                tracing::error!(offset, "skipping: {err}");
                return Ok(None);
            }
        };
        let endpoint = self.enabled().await?;
        if !endpoint.wants(offset, envelope.event_type.as_deref()) {
            return Ok(None);
        }
        if backed_off.is_none() {
            let age = polled_at.saturating_sub(envelope.received_at);
            self.app
                .metrics
                .poll_wakeup
                .record(Duration::from_millis(age));
        }
        let (tries, refusals) = backed_off.unwrap_or_default();
        record.payload = Default::default();
        Ok(Some(Held {
            offset,
            claim: record,
            id: envelope.event_id(&self.source, offset),
            envelope,
            tries,
            refusals,
        }))
    }

    /// Send every held record, at once, until each is settled. An ordered
    /// endpoint calls this with one record at a time.
    async fn settle(&mut self, mut held: Vec<Held>) -> Result<()> {
        while !held.is_empty() {
            let endpoint = self.enabled().await?;
            let answers = join_all(held.iter().map(|record| {
                self.sender
                    .send(&endpoint, &record.id, &record.envelope, None)
            }))
            .await;
            let outcomes: Vec<Outcome> = answers
                .iter()
                .map(|answer| Outcome::classify(answer.status, answer.retry_after))
                .collect();
            let mut refusals: Vec<usize> = held.iter().map(|record| record.refusals).collect();
            let before = self.health.state;
            let decisions = self.health.decide_window(
                &outcomes,
                &mut refusals,
                unix_millis(),
                jitter(),
                &self.app.config.policy,
            );
            self.report(before, &outcomes, &answers);

            let mut wait = Duration::ZERO;
            let mut disable = None;
            let mut kept = Vec::new();
            for (((mut record, answer), decision), refusals) in
                held.into_iter().zip(answers).zip(decisions).zip(refusals)
            {
                record.tries += 1;
                record.refusals = refusals;
                self.sender.log_attempt(record.offset, &record.id, &answer);
                match decision {
                    Decision::Ack => self.ack(record.offset).await,
                    Decision::DeadLetter => {
                        let offset = record.offset;
                        self.sender
                            .dead_letter(offset, record.envelope, record.tries, answer)
                            .await;
                        self.ack(offset).await;
                    }
                    Decision::Retry(after)
                        if self.mode == Mode::Unordered
                            && self.nack_after(&record, after).await =>
                    {
                        self.backing_off
                            .insert(record.offset, (record.tries, record.refusals));
                    }
                    Decision::Retry(after) | Decision::Pause(after) => {
                        wait = wait.max(after);
                        kept.push(record);
                    }
                    Decision::Disable(reason) => {
                        disable = Some(reason);
                        kept.push(record);
                    }
                }
            }
            held = kept;
            match disable {
                Some(reason) => {
                    tracing::warn!(endpoint = %self.endpoint, "disabling: {reason}");
                    self.disable(reason).await;
                }
                None => self.wait_or_retry(wait).await,
            }
        }
        Ok(())
    }

    /// Wait out a backoff, unless an operator asks to retry now.
    async fn wait_or_retry(&self, wait: Duration) {
        let mut jobs = self.tenant.jobs.clone();
        let sleep = tokio::time::sleep(wait);
        tokio::pin!(sleep);
        loop {
            let asked = jobs.borrow_and_update().0.iter().find_map(|(id, job)| {
                (job.endpoint == self.endpoint && job.kind == JobKind::Retry && job.active())
                    .then(|| (id.clone(), job.clone()))
            });
            if let Some((id, mut job)) = asked {
                tracing::info!(endpoint = %self.endpoint, "retrying now, as asked");
                job.status = JobStatus::Done;
                save(&self.tenant.felix, &id, &mut job).await;
                return;
            }
            tokio::select! {
                () = &mut sleep => return,
                changed = jobs.changed() => if changed.is_err() {
                    (&mut sleep).await;
                    return;
                },
            }
        }
    }

    fn report(&self, before: State, outcomes: &[Outcome], answers: &[Answer]) {
        let state = self.health.state;
        let failing_since = self.health.failing_since;
        let failed = outcomes
            .iter()
            .zip(answers)
            .find(|(outcome, _)| **outcome != Outcome::Delivered);
        self.reporter.update(state != before, |report| {
            report.state = state;
            report.failing_since = failing_since;
            if let Some((_, answer)) = failed {
                report.last_error = Some(match answer.status {
                    Some(status) => format!("{status} {}", answer.detail),
                    None => answer.detail.clone(),
                });
            }
        });
    }

    /// The endpoint's config once it exists and is enabled. While it is
    /// disabled the task waits here, holding whatever it had claimed.
    async fn enabled(&mut self) -> Result<Endpoint> {
        let mut catalog = self.tenant.catalog.clone();
        loop {
            let endpoint = catalog
                .borrow_and_update()
                .endpoints
                .get(&self.endpoint)
                .cloned();
            match endpoint {
                Some(endpoint) if endpoint.disabled.is_some() => {
                    self.disabling = false;
                    if self.health.state != State::Disabled {
                        self.health.state = State::Disabled;
                        self.reporter
                            .update(true, |report| report.state = State::Disabled);
                    }
                }
                // Until this task's own write shows up, an enabled entry is stale.
                Some(endpoint) if !self.disabling => {
                    if self.health.state == State::Disabled {
                        tracing::info!(endpoint = %self.endpoint, "enabled again");
                        self.health.enable();
                        self.reporter.update(true, |report| {
                            report.state = State::Active;
                            report.failing_since = None;
                        });
                    }
                    return Ok(endpoint);
                }
                _ => {}
            }
            catalog
                .changed()
                .await
                .context("the config watch stopped")?;
        }
    }

    /// Mark the endpoint disabled in config, where an operator sees it and
    /// can enable it again.
    async fn disable(&mut self, reason: String) {
        let felix = &self.tenant.felix;
        let key = endpoint_key(&self.endpoint);
        loop {
            let written = async {
                let bytes = felix.cache_get(CONFIG, &key).await?.context("no config")?;
                let mut endpoint: Endpoint = serde_json::from_slice(&bytes)?;
                endpoint.disabled = Some(Disabled {
                    reason: reason.clone(),
                    at: unix_millis(),
                });
                felix
                    .cache_put(CONFIG, &key, serde_json::to_vec(&endpoint)?, None)
                    .await
            };
            match written.await {
                Ok(()) => break,
                Err(err) => {
                    tracing::warn!(endpoint = %self.endpoint, "could not disable: {err:#}");
                    tokio::time::sleep(FELIX_RETRY).await;
                }
            }
        }
        self.disabling = true;
    }

    /// Hand `record` back to the group, to come back once `after` has passed,
    /// so the rest of an unordered window keeps moving. False when it is
    /// still in hand and the task has to wait it out.
    async fn nack_after(&self, record: &Held, after: Duration) -> bool {
        let Some(claims) = &self.claims else {
            return false;
        };
        let felix = &self.tenant.felix;
        let nacked = self
            .felix()
            .group_nack_after(
                &felix.tenant,
                &felix.namespace,
                &self.stream,
                0,
                &self.group,
                &record.claim,
                after,
            )
            .await;
        match nacked {
            Ok(()) => {
                claims.release(record.offset);
                true
            }
            Err(err) => {
                // A stale claim is owed already and would come straight back.
                if !claims::is_stale(&err) {
                    tracing::warn!(offset = record.offset, "nack failed: {err:#}");
                }
                false
            }
        }
    }

    async fn ack(&self, offset: u64) {
        if let Some(claims) = &self.claims {
            claims.release(offset);
        }
        let felix = &self.tenant.felix;
        while let Err(err) = self
            .felix()
            .group_ack(
                &felix.tenant,
                &felix.namespace,
                &self.stream,
                0,
                &self.group,
                offset,
            )
            .await
        {
            tracing::warn!(offset, "ack failed: {err:#}");
            tokio::time::sleep(FELIX_RETRY).await;
        }
        self.reporter.update(false, |report| {
            report.last_acked = report.last_acked.max(Some(offset))
        });
    }
}
