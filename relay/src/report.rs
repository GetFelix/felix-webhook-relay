//! An endpoint's health entry, `state/health/<endpoint>`: kept in memory by
//! its delivery task and written to Felix every few seconds, or at once when
//! the endpoint's state changes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use felix_relay_core::health::State;
use felix_relay_core::records::HealthReport;
use tokio::sync::Notify;

use crate::felix::Felix;
use crate::unix_millis;

pub(crate) const STATE: &str = "state";
const EVERY: Duration = Duration::from_secs(3);
/// Only the latest gaps are kept; an operator needs to see that they
/// happen, not every one.
const MAX_GAPS: usize = 20;

#[derive(Clone)]
pub(crate) struct Reporter {
    report: Arc<Mutex<HealthReport>>,
    now: Arc<Notify>,
}

impl Reporter {
    pub(crate) fn start(felix: Arc<Felix>, endpoint: &str, reporter: String) -> Self {
        let report = Arc::new(Mutex::new(HealthReport {
            state: State::Active,
            failing_since: None,
            last_error: None,
            last_acked: None,
            possibly_trimmed: Vec::new(),
            reporter,
            updated_at: unix_millis(),
        }));
        let now = Arc::new(Notify::new());
        let this = Self { report, now };
        let writer = this.clone();
        let key = format!("health/{endpoint}");
        tokio::spawn(async move {
            loop {
                let _ = tokio::time::timeout(EVERY, writer.now.notified()).await;
                let json = {
                    let mut report = writer.report.lock().unwrap();
                    report.updated_at = unix_millis();
                    serde_json::to_vec(&*report).expect("a report serializes")
                };
                if let Err(err) = felix.cache_put(STATE, &key, json, None).await {
                    tracing::warn!("could not write {key}: {err:#}");
                }
            }
        });
        this
    }

    /// Change the report; `urgent` writes it now rather than on the timer.
    pub(crate) fn update(&self, urgent: bool, change: impl FnOnce(&mut HealthReport)) {
        change(&mut self.report.lock().unwrap());
        if urgent {
            self.now.notify_one();
        }
    }

    /// Note a gap between two offsets the group handed out in a row.
    pub(crate) fn gap(&self, from: u64, to: u64) {
        self.update(false, |report| {
            report.possibly_trimmed.push((from, to));
            let excess = report.possibly_trimmed.len().saturating_sub(MAX_GAPS);
            report.possibly_trimmed.drain(..excess);
        });
    }
}
