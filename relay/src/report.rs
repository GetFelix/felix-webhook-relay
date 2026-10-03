//! An endpoint's health entry, `state/health/<endpoint>`: kept in memory by
//! its delivery task and written to Felix every few seconds, or at once when
//! the endpoint's state changes.

use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use felix_relay_core::health::State;
use felix_relay_core::records::HealthReport;
use tokio::sync::Notify;

use crate::catalog::STATE;
use crate::felix::Felix;
use crate::unix_millis;

const EVERY: Duration = Duration::from_secs(3);
/// An unchanged report is still written this often, so its `updated_at`
/// shows the worker is alive.
const HEARTBEAT: Duration = Duration::from_secs(60);
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
        // The writer holds the report weakly, so it stops once the task that
        // owns this reporter is gone.
        let weak: Weak<Mutex<HealthReport>> = Arc::downgrade(&report);
        let wake = Arc::clone(&now);
        let key = format!("health/{endpoint}");
        tokio::spawn(async move {
            let mut written: Option<HealthReport> = None;
            let mut last_write = Instant::now();
            loop {
                let _ = tokio::time::timeout(EVERY, wake.notified()).await;
                let Some(report) = weak.upgrade() else { return };
                // An idle endpoint's report does not change, and a thousand
                // of them rewriting it every few seconds is load for nothing.
                let json = {
                    let mut report = report.lock().unwrap();
                    let mut unchanged = report.clone();
                    unchanged.updated_at = written.as_ref().map_or(0, |w| w.updated_at);
                    if written.as_ref() == Some(&unchanged) && last_write.elapsed() < HEARTBEAT {
                        continue;
                    }
                    report.updated_at = unix_millis();
                    written = Some(report.clone());
                    last_write = Instant::now();
                    serde_json::to_vec(&*report).expect("a report serializes")
                };
                if let Err(err) = felix.cache_put(STATE, &key, json, None).await {
                    tracing::warn!("could not write {key}: {err:#}");
                }
            }
        });
        Self { report, now }
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
