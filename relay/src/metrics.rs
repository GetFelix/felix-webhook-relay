//! Latency of the relay's three hops, kept apart so a slow delivery can be
//! blamed on the right one. Served by `GET /metrics` in the Prometheus text
//! format.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Bucket bounds in seconds, from half a millisecond to the 15 s request timeout.
const BOUNDS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0,
];

#[derive(Default)]
pub(crate) struct Metrics {
    /// From starting an append to Felix acknowledging it.
    pub(crate) intake_ack: Histogram,
    /// From intake accepting a webhook to a group poll handing it out. For an
    /// endpoint that is keeping up, this is how long the waiting poll took to wake.
    pub(crate) poll_wakeup: Histogram,
    /// One request to the endpoint, from sending to its answer.
    pub(crate) outbound: Histogram,
}

impl Metrics {
    pub(crate) fn render(&self) -> String {
        let mut out = String::new();
        self.intake_ack.render(
            &mut out,
            "relay_intake_ack_seconds",
            "Append to Felix acknowledgement, per accepted webhook.",
        );
        self.poll_wakeup.render(
            &mut out,
            "relay_poll_wakeup_seconds",
            "Intake acceptance to the group poll that handed the record out.",
        );
        self.outbound.render(
            &mut out,
            "relay_outbound_request_seconds",
            "One delivery request to an endpoint, send to response.",
        );
        out
    }
}

#[derive(Default)]
pub(crate) struct Histogram {
    /// Per bucket, not cumulative; the last one is everything above the bounds.
    buckets: [AtomicU64; BOUNDS.len() + 1],
    sum_micros: AtomicU64,
}

impl Histogram {
    pub(crate) fn record(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        let bucket = BOUNDS
            .iter()
            .position(|&bound| seconds <= bound)
            .unwrap_or(BOUNDS.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0;
        for (index, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            let le = BOUNDS
                .get(index)
                .map_or_else(|| "+Inf".to_string(), f64::to_string);
            let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}");
        }
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(out, "{name}_sum {sum}");
        let _ = writeln!(out, "{name}_count {cumulative}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_cumulative() {
        let histogram = Histogram::default();
        histogram.record(Duration::from_micros(300));
        histogram.record(Duration::from_millis(3));
        histogram.record(Duration::from_secs(60));
        let mut out = String::new();
        histogram.render(&mut out, "h", "test");
        assert!(out.contains("h_bucket{le=\"0.0005\"} 1\n"));
        assert!(out.contains("h_bucket{le=\"0.0025\"} 1\n"));
        assert!(out.contains("h_bucket{le=\"0.005\"} 2\n"));
        assert!(out.contains("h_bucket{le=\"15\"} 2\n"));
        assert!(out.contains("h_bucket{le=\"+Inf\"} 3\n"));
        assert!(out.contains("h_count 3\n"));
        assert!(out.contains("h_sum 60.0033\n"));
    }

    #[test]
    fn a_bound_is_inclusive() {
        let histogram = Histogram::default();
        histogram.record(Duration::from_millis(1));
        let mut out = String::new();
        histogram.render(&mut out, "h", "test");
        assert!(out.contains("h_bucket{le=\"0.001\"} 1\n"));
    }

    #[test]
    fn every_hop_is_its_own_series() {
        let out = Metrics::default().render();
        for name in [
            "relay_intake_ack_seconds",
            "relay_poll_wakeup_seconds",
            "relay_outbound_request_seconds",
        ] {
            assert!(out.contains(&format!("# TYPE {name} histogram")));
        }
    }
}
