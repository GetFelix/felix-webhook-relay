//! Replay and redrive jobs, and finding an offset by time.
//!
//! Felix's native API has no offset-for-time, and consumers never see the
//! broker's append timestamps. Offsets in a source's stream rise with the
//! envelope's `received_at`, give or take the few milliseconds two intake
//! processes can interleave, so the log is its own index: bisect it.

use serde::{Deserialize, Serialize};

/// How far before the asked-for time a search aims, so records two intake
/// processes interleaved cannot fall off the edge. The replay itself filters
/// by `received_at`, so aiming early costs a few reads, never a wrong record.
pub const SEARCH_MARGIN_MS: u64 = 5_000;
/// A replay writes its position back after this many records, so a worker
/// that dies repeats at most this many.
pub const CHECKPOINT_EVERY: u64 = 100;

/// A bisection over `[oldest, tail)` for the first record received at or
/// after `target`. Ask [`OffsetSearch::next`] which offset to read, tell
/// [`OffsetSearch::observe`] what was there, until `next` is `None`.
#[derive(Debug, Clone)]
pub struct OffsetSearch {
    lo: u64,
    hi: u64,
    target: u64,
    pub probes: u32,
}

impl OffsetSearch {
    pub fn new(oldest: u64, tail: u64, target: u64) -> Self {
        Self {
            lo: oldest,
            hi: tail.max(oldest),
            target,
            probes: 0,
        }
    }

    /// The offset to read next, or `None` once the answer is known.
    pub fn next(&self) -> Option<u64> {
        (self.lo < self.hi).then(|| self.lo + (self.hi - self.lo) / 2)
    }

    /// What reading from `probe` found: the first record at or after it, its
    /// offset and `received_at`, or `None` when there was nothing there. The
    /// broker skips offsets it never delivers, so `found` can be past `probe`.
    pub fn observe(&mut self, probe: u64, found: Option<(u64, u64)>) {
        self.probes += 1;
        match found {
            Some((offset, received_at)) if received_at < self.target => {
                self.lo = offset.max(probe) + 1;
            }
            _ => self.hi = probe,
        }
    }

    /// The first offset at or after the target; the tail if there is none.
    pub fn result(&self) -> u64 {
        self.lo
    }
}

/// `state/job/<id>`, JSON: work the endpoint's worker does beside live delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub endpoint: String,
    pub kind: JobKind,
    pub status: JobStatus,
    /// Requests that got a `2xx`.
    #[serde(default)]
    pub sent: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Unix milliseconds.
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum JobKind {
    /// Send the source's records in `[from, to)` again, keeping those
    /// received in `[since, until)`.
    Replay {
        from: u64,
        to: u64,
        /// Where to carry on: `from`, then each checkpoint.
        next: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        until: Option<u64>,
        /// Hold live delivery until the replay is done, for receivers that
        /// need the replayed range to land first.
        #[serde(default)]
        pause_live: bool,
    },
    /// Send the envelope kept with the dead letter at this offset of `dead`.
    Redrive { dead_offset: u64 },
    /// Cut a paused endpoint's backoff wait short. The endpoint's own task
    /// takes this, at its next wait.
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

impl Job {
    /// Whether a worker still has this to do.
    pub fn active(&self) -> bool {
        matches!(self.status, JobStatus::Pending | JobStatus::Running)
    }

    /// Whether live delivery to its endpoint waits for it.
    pub fn pauses_live(&self) -> bool {
        self.active()
            && matches!(
                self.kind,
                JobKind::Replay {
                    pause_live: true,
                    ..
                }
            )
    }
}

/// Whether a source's retention is shorter than the relay needs: its log
/// has been trimmed (`oldest_offset` above zero) and the oldest record it
/// still holds is younger than `needed`, which is the disable window plus
/// the replay window. An untrimmed log says nothing about retention yet.
pub fn retention_too_short(
    oldest_offset: u64,
    oldest_received_at: u64,
    now: u64,
    needed_ms: u64,
) -> bool {
    oldest_offset > 0 && now.saturating_sub(oldest_received_at) < needed_ms
}

/// Whether a record received at `received_at` is inside a replay's window.
pub fn in_window(received_at: u64, since: Option<u64>, until: Option<u64>) -> bool {
    since.is_none_or(|since| received_at >= since) && until.is_none_or(|until| received_at < until)
}

/// `state/dead/<offset>`, JSON: what became of a dead letter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadMark {
    pub status: DeadStatus,
    /// Unix milliseconds.
    pub at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeadStatus {
    Redriving,
    Redriven,
    Discarded,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run a search over `times`, where the record at index `i` has offset
    /// `i + first` and `received_at` `times[i]`.
    fn search(times: &[u64], first: u64, target: u64) -> (u64, u32) {
        let mut search = OffsetSearch::new(first, first + times.len() as u64, target);
        while let Some(probe) = search.next() {
            let index = (probe - first) as usize;
            let found = times.get(index).map(|at| (probe, *at));
            search.observe(probe, found);
        }
        (search.result(), search.probes)
    }

    #[test]
    fn finds_the_first_record_at_or_after_a_time() {
        let times: Vec<u64> = (0..1000).map(|i| 1_000_000 + i * 10).collect();
        assert_eq!(search(&times, 0, 1_000_000).0, 0);
        assert_eq!(search(&times, 0, 1_000_005).0, 1);
        assert_eq!(search(&times, 0, 1_000_010).0, 1);
        assert_eq!(search(&times, 0, 1_005_000).0, 500);
        assert_eq!(search(&times, 0, 2_000_000).0, 1000, "past the tail");
        assert_eq!(search(&times, 0, 0).0, 0, "before the oldest");
        assert_eq!(search(&times, 250, 1_000_000).0, 250, "a trimmed log");
    }

    #[test]
    fn ten_million_records_take_about_24_reads() {
        let mut search = OffsetSearch::new(0, 10_000_000, 7_654_321);
        while let Some(probe) = search.next() {
            search.observe(probe, Some((probe, probe)));
        }
        assert_eq!(search.result(), 7_654_321);
        assert!(search.probes <= 24, "{}", search.probes);
    }

    #[test]
    fn interleaved_intake_never_loses_an_edge_record() {
        // Two intake processes: their timestamps interleave up to 40 ms out
        // of order, though offsets still rise.
        let times: Vec<u64> = (0..5000u64)
            .map(|i| 1_000_000 + i * 10 + (i * 7919 % 5) * 10)
            .collect();
        for target in (1_000_000..1_050_000).step_by(997) {
            let (start, _) = search(&times, 0, target - SEARCH_MARGIN_MS);
            let first_inside = times.iter().position(|&at| at >= target).unwrap() as u64;
            assert!(start <= first_inside, "{target}: started at {start}");
            assert!(
                times[..start as usize].iter().all(|&at| at < target),
                "{target}: a record in the window was skipped"
            );
        }
    }

    #[test]
    fn offsets_the_broker_skips_do_not_confuse_it() {
        // Offsets 3 to 6 were never delivered; a read from them finds 7.
        let present: Vec<(u64, u64)> = [0, 1, 2, 7, 8, 9]
            .iter()
            .map(|&offset| (offset, 100 + offset))
            .collect();
        for target in [100, 103, 106, 107, 109, 110] {
            let mut search = OffsetSearch::new(0, 10, target);
            while let Some(probe) = search.next() {
                let found = present.iter().copied().find(|(offset, _)| *offset >= probe);
                search.observe(probe, found);
            }
            let expected = present
                .iter()
                .find(|(_, at)| *at >= target)
                .map_or(10, |(offset, _)| *offset);
            let result = search.result();
            assert!(result <= expected, "{target}: {result} > {expected}");
            assert!(
                present.iter().all(|(o, at)| *o >= result || *at < target),
                "{target}: started past a record in the window"
            );
        }
    }

    #[test]
    fn retention_is_short_only_once_the_log_was_trimmed() {
        let hour = 3_600_000;
        assert!(!retention_too_short(0, 0, 10 * hour, 5 * hour), "untrimmed");
        assert!(retention_too_short(500, 9 * hour, 10 * hour, 5 * hour));
        assert!(!retention_too_short(500, 4 * hour, 10 * hour, 5 * hour));
    }

    #[test]
    fn windows_are_half_open() {
        assert!(in_window(10, Some(10), Some(20)));
        assert!(!in_window(20, Some(10), Some(20)));
        assert!(!in_window(9, Some(10), None));
        assert!(in_window(0, None, None));
    }

    #[test]
    fn jobs_read_from_json() {
        let job: Job = serde_json::from_str(
            r#"{"endpoint":"e","kind":{"type":"replay","from":5,"to":9,"next":5,"pause_live":true},
                "status":"pending","created_at":1,"updated_at":1}"#,
        )
        .unwrap();
        assert!(job.active() && job.pauses_live());
        let job: Job = serde_json::from_str(
            r#"{"endpoint":"e","kind":{"type":"redrive","dead_offset":3},"status":"done",
                "created_at":1,"updated_at":2}"#,
        )
        .unwrap();
        assert!(!job.active() && !job.pauses_live());
    }
}
