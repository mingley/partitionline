//! Client counters and latency min / mean / max. Snapshots, not histograms.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Produce-ack or fetch-round latency since connect (nanoseconds).
///
/// `min_nanos` / `max_nanos` are `0` when [`Self::count`] is `0`. This is not
/// a percentile histogram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LatencyStats {
    /// Samples recorded.
    pub count: u64,
    /// Sum of sample durations in nanoseconds.
    pub sum_nanos: u64,
    /// Shortest sample in nanoseconds.
    pub min_nanos: u64,
    /// Longest sample in nanoseconds.
    pub max_nanos: u64,
}

impl LatencyStats {
    /// Mean sample duration in nanoseconds, or `None` when [`Self::count`] is `0`.
    #[must_use]
    pub fn mean_nanos(&self) -> Option<u64> {
        (self.count > 0).then(|| self.sum_nanos / self.count)
    }
}

pub(crate) struct LatencyTracker {
    count: AtomicU64,
    sum_nanos: AtomicU64,
    min_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl LatencyTracker {
    pub(crate) fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
            min_nanos: AtomicU64::new(u64::MAX),
            max_nanos: AtomicU64::new(0),
        }
    }

    pub(crate) fn record(&self, d: Duration) {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        let _ = self.count.fetch_add(1, Ordering::Relaxed);
        let _ = self.sum_nanos.fetch_add(ns, Ordering::Relaxed);
        let mut cur = self.min_nanos.load(Ordering::Relaxed);
        while ns < cur {
            match self.min_nanos.compare_exchange_weak(
                cur,
                ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        cur = self.max_nanos.load(Ordering::Relaxed);
        while ns > cur {
            match self.max_nanos.compare_exchange_weak(
                cur,
                ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    pub(crate) fn snapshot(&self) -> LatencyStats {
        let count = self.count.load(Ordering::Relaxed);
        let min = self.min_nanos.load(Ordering::Relaxed);
        LatencyStats {
            count,
            sum_nanos: self.sum_nanos.load(Ordering::Relaxed),
            min_nanos: if count == 0 { 0 } else { min },
            max_nanos: self.max_nanos.load(Ordering::Relaxed),
        }
    }
}

/// Produce counters since this [`crate::Producer`] connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProducerMetrics {
    /// Records successfully queued (`send` / `send_all` / `try_send`).
    pub records_queued: u64,
    /// Records the broker acknowledged (including `acks=0` local complete).
    pub records_acked: u64,
    /// Records that failed (broker error, timeout, closed).
    pub produce_errors: u64,
    /// Key plus value bytes of queued records.
    pub bytes_queued: u64,
    /// Queue-to-ack latency per acknowledged record (including `acks=0`).
    pub ack_latency: LatencyStats,
}

/// Fetch counters since this [`crate::Consumer`] connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConsumerMetrics {
    /// Successful [`crate::Consumer::fetch`] / [`crate::ConsumerGroup::poll`] calls.
    pub fetch_rounds: u64,
    /// Records returned to the caller.
    pub records_fetched: u64,
    /// Key plus value bytes of returned records.
    pub bytes_fetched: u64,
    /// Failed fetch rounds.
    pub fetch_errors: u64,
    /// End-to-end duration of each successful fetch round.
    pub fetch_latency: LatencyStats,
}

/// Share-group counters since this [`crate::ShareGroup`] joined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShareMetrics {
    /// Successful [`crate::ShareGroup::poll`] calls.
    pub fetch_rounds: u64,
    /// Records returned from ShareFetch.
    pub records_fetched: u64,
    /// Key plus value bytes of returned records.
    pub bytes_fetched: u64,
    /// Failed poll rounds (after retries).
    pub fetch_errors: u64,
    /// Records sent on ShareAcknowledge (`accept` / `release` / `reject`).
    pub records_acknowledged: u64,
    /// End-to-end duration of each successful poll (including leader retries).
    pub fetch_latency: LatencyStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn metrics_default_zero() {
        assert_eq!(ProducerMetrics::default().records_queued, 0);
        assert_eq!(ProducerMetrics::default().ack_latency.count, 0);
        assert_eq!(ConsumerMetrics::default().records_fetched, 0);
        assert_eq!(ConsumerMetrics::default().fetch_latency.count, 0);
        assert_eq!(ShareMetrics::default().records_acknowledged, 0);
        assert_eq!(ShareMetrics::default().bytes_fetched, 0);
        assert_eq!(ShareMetrics::default().fetch_errors, 0);
        assert_eq!(ShareMetrics::default().fetch_latency.count, 0);
        assert_eq!(LatencyStats::default().mean_nanos(), None);
    }

    #[test]
    fn latency_tracker_min_max_mean() {
        let t = LatencyTracker::new();
        assert_eq!(t.snapshot().count, 0);
        t.record(Duration::from_nanos(10));
        t.record(Duration::from_nanos(30));
        t.record(Duration::from_nanos(20));
        let s = t.snapshot();
        assert_eq!(s.count, 3);
        assert_eq!(s.min_nanos, 10);
        assert_eq!(s.max_nanos, 30);
        assert_eq!(s.sum_nanos, 60);
        assert_eq!(s.mean_nanos(), Some(20));
    }
}
