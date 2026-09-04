//! Client counters and latency stats. Snapshots, not HDR histograms.
//!
//! [`Quota`] is Java `org.apache.kafka.common.metrics.Quota`.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Sliding window for [`LatencyStats::p50_nanos`] / [`LatencyStats::p99_nanos`].
const LATENCY_WINDOW: usize = 1024;

/// Produce-ack or fetch-round latency since connect (nanoseconds).
///
/// `min_nanos` / `max_nanos` / `p50_nanos` / `p99_nanos` are `0` when
/// [`Self::count`] is `0`. Percentiles are the last 1024 samples (not a
/// lifetime HDR histogram). Global snapshots are not split by topic;
/// [`ProducerMetrics::topics`] / [`ConsumerMetrics::topics`] /
/// [`ShareMetrics`] field `topics` are. [`AdminMetrics`] has no per-topic rows.
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
    /// Approximate p50 of the last 1024 samples.
    pub p50_nanos: u64,
    /// Approximate p99 of the last 1024 samples.
    pub p99_nanos: u64,
}

impl LatencyStats {
    /// Mean sample duration in nanoseconds, or `None` when [`Self::count`] is `0`.
    #[must_use]
    pub fn mean_nanos(&self) -> Option<u64> {
        (self.count > 0).then(|| self.sum_nanos / self.count)
    }
}

/// Nearest-rank percentile: `ceil(p/100 * n) - 1`.
fn percentile_index(n: usize, p: u32) -> usize {
    if n == 0 {
        return 0;
    }
    let rank = (n * p as usize).div_ceil(100).max(1);
    rank.min(n) - 1
}

fn percentile(sorted: &[u64], p: u32) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted
        .get(percentile_index(sorted.len(), p))
        .copied()
        .unwrap_or(0)
}

const BYTE_SCALE_SUFFIXES: [&str; 9] = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

/// Java `Utils.formatBytes` (English `0.##` scale).
///
/// Negative values print with no unit suffix. Zero is `0.0`.
#[must_use]
pub fn format_bytes(bytes: i64) -> String {
    if bytes < 0 {
        return bytes.to_string();
    }
    if bytes == 0 {
        return "0.0".to_string();
    }
    let n = u64::try_from(bytes).unwrap_or(0);
    let mut ordinal = 0usize;
    let mut scale = 1u64;
    while let Some(next) = scale.checked_mul(1024) {
        if n < next || ordinal + 1 >= BYTE_SCALE_SUFFIXES.len() {
            break;
        }
        scale = next;
        ordinal += 1;
    }
    let Some(suffix) = BYTE_SCALE_SUFFIXES.get(ordinal) else {
        return n.to_string();
    };
    format!("{} {suffix}", format_scaled_two_digit(n, scale))
}

/// DecimalFormat `0.##` with `HALF_EVEN` (Java `Utils.formatBytes`).
fn format_scaled_two_digit(n: u64, scale: u64) -> String {
    let n100 = u128::from(n).saturating_mul(100);
    let scale = u128::from(scale.max(1));
    let q = n100 / scale;
    let r = n100 % scale;
    let twice_r = r.saturating_mul(2);
    let cents = if twice_r > scale {
        q.saturating_add(1)
    } else if twice_r < scale || q % 2 == 0 {
        q
    } else {
        q.saturating_add(1)
    };
    let cents = u64::try_from(cents).unwrap_or(u64::MAX);
    let whole = cents / 100;
    let frac = cents % 100;
    if frac == 0 {
        whole.to_string()
    } else if frac % 10 == 0 {
        format!("{whole}.{}", frac / 10)
    } else {
        format!("{whole}.{frac:02}")
    }
}

/// Java `Double.toString` for [`Quota`] `Display`.
fn write_java_double(f: &mut fmt::Formatter<'_>, v: f64) -> fmt::Result {
    if v.is_nan() {
        return f.write_str("NaN");
    }
    if v == f64::INFINITY {
        return f.write_str("Infinity");
    }
    if v == f64::NEG_INFINITY {
        return f.write_str("-Infinity");
    }
    write!(f, "{v:?}")
}

/// Java `org.apache.kafka.common.metrics.Quota`.
///
/// [`std::fmt::Display`] is Java `Quota.toString` (`upper=1.0` / `lower=1.0`).
/// [`Self::acceptable`] is at or below the bound for an upper bound and at
/// or above for a lower bound.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quota {
    bound: f64,
    upper: bool,
}

impl Quota {
    /// Java `Quota(double, boolean)`.
    #[must_use]
    pub fn new(bound: f64, upper: bool) -> Self {
        Self { bound, upper }
    }

    /// Java `Quota.upperBound`.
    #[must_use]
    pub fn upper_bound(upper_bound: f64) -> Self {
        Self::new(upper_bound, true)
    }

    /// Java `Quota.lowerBound`.
    #[must_use]
    pub fn lower_bound(lower_bound: f64) -> Self {
        Self::new(lower_bound, false)
    }

    /// Java `Quota.isUpperBound`.
    #[must_use]
    pub fn is_upper_bound(&self) -> bool {
        self.upper
    }

    /// Java `Quota.bound`.
    #[must_use]
    pub fn bound(&self) -> f64 {
        self.bound
    }

    /// Java `Quota.acceptable`.
    #[must_use]
    pub fn acceptable(&self, value: f64) -> bool {
        (self.upper && value <= self.bound) || (!self.upper && value >= self.bound)
    }
}

impl fmt::Display for Quota {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.upper { "upper=" } else { "lower=" })?;
        write_java_double(f, self.bound)
    }
}

pub(crate) struct LatencyTracker {
    count: AtomicU64,
    sum_nanos: AtomicU64,
    min_nanos: AtomicU64,
    max_nanos: AtomicU64,
    idx: AtomicU64,
    samples: Box<[AtomicU64]>,
}

impl LatencyTracker {
    pub(crate) fn new() -> Self {
        let samples = (0..LATENCY_WINDOW)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
            min_nanos: AtomicU64::new(u64::MAX),
            max_nanos: AtomicU64::new(0),
            idx: AtomicU64::new(0),
            samples,
        }
    }

    pub(crate) fn record(&self, d: Duration) {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        let _ = self.count.fetch_add(1, Ordering::Relaxed);
        let _ = self.sum_nanos.fetch_add(ns, Ordering::Relaxed);
        let slot =
            usize::try_from(self.idx.fetch_add(1, Ordering::Relaxed) % LATENCY_WINDOW as u64)
                .unwrap_or(0);
        if let Some(slot) = self.samples.get(slot) {
            slot.store(ns, Ordering::Relaxed);
        }
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

    fn window(&self) -> Vec<u64> {
        let count = self.count.load(Ordering::Relaxed);
        let n = usize::try_from(count.min(LATENCY_WINDOW as u64)).unwrap_or(LATENCY_WINDOW);
        if n == 0 {
            return Vec::new();
        }
        let mut v = Vec::with_capacity(n);
        if count <= LATENCY_WINDOW as u64 {
            for sample in self.samples.iter().take(n) {
                v.push(sample.load(Ordering::Relaxed));
            }
        } else {
            let start = usize::try_from(self.idx.load(Ordering::Relaxed) % LATENCY_WINDOW as u64)
                .unwrap_or(0);
            for k in 0..LATENCY_WINDOW {
                let ns = self
                    .samples
                    .get((start + k) % LATENCY_WINDOW)
                    .map(|s| s.load(Ordering::Relaxed))
                    .unwrap_or(0);
                v.push(ns);
            }
        }
        v
    }

    pub(crate) fn snapshot(&self) -> LatencyStats {
        let count = self.count.load(Ordering::Relaxed);
        let min = self.min_nanos.load(Ordering::Relaxed);
        let mut window = self.window();
        window.sort_unstable();
        LatencyStats {
            count,
            sum_nanos: self.sum_nanos.load(Ordering::Relaxed),
            min_nanos: if count == 0 { 0 } else { min },
            max_nanos: self.max_nanos.load(Ordering::Relaxed),
            p50_nanos: percentile(&window, 50),
            p99_nanos: percentile(&window, 99),
        }
    }
}

/// Produce counters since this [`crate::Producer`] connected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProducerMetrics {
    /// Records successfully queued (`send` / `send_all` / `try_send`).
    pub records_queued: u64,
    /// Records the broker acknowledged (including `acks=0` local complete).
    pub records_acked: u64,
    /// Records that failed (broker error, timeout, closed).
    pub produce_errors: u64,
    /// Key plus value bytes of queued records.
    pub bytes_queued: u64,
    /// Key plus value bytes still queued and not yet acked (`buffer.memory` in-flight).
    pub bytes_buffered: u64,
    /// Queue-to-ack latency per acknowledged record (including `acks=0`).
    pub ack_latency: LatencyStats,
    /// Per-topic counters. Topics with no activity are omitted. Sorted by name.
    pub topics: Vec<TopicProduceMetrics>,
}

/// Produce counters for one topic.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicProduceMetrics {
    /// Topic name.
    pub topic: String,
    /// Records queued for this topic.
    pub records_queued: u64,
    /// Records the broker acknowledged for this topic.
    pub records_acked: u64,
    /// Records that failed for this topic.
    pub produce_errors: u64,
    /// Key plus value bytes queued for this topic.
    pub bytes_queued: u64,
    /// Queue-to-ack latency for this topic.
    pub ack_latency: LatencyStats,
}

/// Fetch counters since this [`crate::Consumer`] connected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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
    /// Per-topic counters. Topics with no fetched records are omitted. Sorted by name.
    pub topics: Vec<TopicFetchMetrics>,
}

/// Fetch counters for one topic.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicFetchMetrics {
    /// Topic name.
    pub topic: String,
    /// Records returned for this topic.
    pub records_fetched: u64,
    /// Key plus value bytes returned for this topic.
    pub bytes_fetched: u64,
    /// Duration of each successful fetch/poll round that returned records
    /// for this topic.
    pub fetch_latency: LatencyStats,
}

/// Share-group counters since this [`crate::ShareGroup`] joined.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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
    /// Per-topic counters. Topics with no fetched records are omitted. Sorted by name.
    pub topics: Vec<TopicFetchMetrics>,
}

/// Snapshot of [`crate::Admin`] RPC counters.
///
/// Java `Admin.metrics()` is Kafka's live metric map. This snapshot is the
/// same pattern as [`ProducerMetrics`] / [`ConsumerMetrics`]: counters plus
/// [`LatencyStats`] for every Admin [`crate::net::BrokerConn::roundtrip`].
///
/// `errors` counts I/O, timeout, and protocol failures — not a decoded broker
/// `error_code` on a valid body.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AdminMetrics {
    /// RPCs this client has completed (success or failure).
    pub requests: u64,
    /// Round-trips that returned [`crate::Error`].
    pub errors: u64,
    /// Time from encode to decode on each round-trip.
    pub request_latency: LatencyStats,
    /// Open TCP sockets (bootstrap plus per-node).
    pub connections: u64,
}

/// Live Admin counters. [`crate::Admin::metrics`] snapshots this.
pub(crate) struct AdminTracker {
    requests: AtomicU64,
    errors: AtomicU64,
    latency: LatencyTracker,
}

impl Default for AdminTracker {
    fn default() -> Self {
        Self {
            requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latency: LatencyTracker::new(),
        }
    }
}

impl AdminTracker {
    pub(crate) fn snapshot(&self, connections: u64) -> AdminMetrics {
        AdminMetrics {
            requests: self.requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            request_latency: self.latency.snapshot(),
            connections,
        }
    }

    pub(crate) fn record(&self, elapsed: Duration, ok: bool) {
        let _ = self.requests.fetch_add(1, Ordering::Relaxed);
        if !ok {
            let _ = self.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.latency.record(elapsed);
    }
}

pub(crate) struct ProduceTopicTracker {
    records_queued: AtomicU64,
    records_acked: AtomicU64,
    produce_errors: AtomicU64,
    bytes_queued: AtomicU64,
    ack_latency: LatencyTracker,
}

impl ProduceTopicTracker {
    pub(crate) fn new() -> Self {
        Self {
            records_queued: AtomicU64::new(0),
            records_acked: AtomicU64::new(0),
            produce_errors: AtomicU64::new(0),
            bytes_queued: AtomicU64::new(0),
            ack_latency: LatencyTracker::new(),
        }
    }

    pub(crate) fn note_queued(&self, n: u64, bytes: u64) {
        let _ = self.records_queued.fetch_add(n, Ordering::Relaxed);
        let _ = self.bytes_queued.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn note_acked(&self, n: u64) {
        let _ = self.records_acked.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn note_ack_latency(&self, d: Duration) {
        self.ack_latency.record(d);
    }

    pub(crate) fn note_errors(&self, n: u64) {
        let _ = self.produce_errors.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, topic: String) -> TopicProduceMetrics {
        TopicProduceMetrics {
            topic,
            records_queued: self.records_queued.load(Ordering::Relaxed),
            records_acked: self.records_acked.load(Ordering::Relaxed),
            produce_errors: self.produce_errors.load(Ordering::Relaxed),
            bytes_queued: self.bytes_queued.load(Ordering::Relaxed),
            ack_latency: self.ack_latency.snapshot(),
        }
    }
}

pub(crate) struct FetchTopicTracker {
    records_fetched: AtomicU64,
    bytes_fetched: AtomicU64,
    fetch_latency: LatencyTracker,
}

impl FetchTopicTracker {
    pub(crate) fn new() -> Self {
        Self {
            records_fetched: AtomicU64::new(0),
            bytes_fetched: AtomicU64::new(0),
            fetch_latency: LatencyTracker::new(),
        }
    }

    pub(crate) fn note_fetched(&self, n: u64, bytes: u64, latency: Duration) {
        let _ = self.records_fetched.fetch_add(n, Ordering::Relaxed);
        let _ = self.bytes_fetched.fetch_add(bytes, Ordering::Relaxed);
        self.fetch_latency.record(latency);
    }

    pub(crate) fn snapshot(&self, topic: String) -> TopicFetchMetrics {
        TopicFetchMetrics {
            topic,
            records_fetched: self.records_fetched.load(Ordering::Relaxed),
            bytes_fetched: self.bytes_fetched.load(Ordering::Relaxed),
            fetch_latency: self.fetch_latency.snapshot(),
        }
    }
}

pub(crate) fn snapshot_produce_topics(
    map: &HashMap<Arc<str>, Arc<ProduceTopicTracker>>,
) -> Vec<TopicProduceMetrics> {
    let mut topics: Vec<TopicProduceMetrics> = map
        .iter()
        .map(|(name, t)| t.snapshot(name.to_string()))
        .collect();
    topics.sort_by(|a, b| a.topic.cmp(&b.topic));
    topics
}

pub(crate) fn snapshot_fetch_topics(
    map: &HashMap<String, FetchTopicTracker>,
) -> Vec<TopicFetchMetrics> {
    let mut topics: Vec<TopicFetchMetrics> = map
        .iter()
        .map(|(name, t)| t.snapshot(name.clone()))
        .collect();
    topics.sort_by(|a, b| a.topic.cmp(&b.topic));
    topics
}

pub(crate) fn accumulate_fetch_topics<'a>(
    map: &mut HashMap<String, FetchTopicTracker>,
    recs: impl IntoIterator<Item = (&'a str, u64)>,
    latency: Duration,
) {
    let mut acc: HashMap<&'a str, (u64, u64)> = HashMap::new();
    for (topic, bytes) in recs {
        let e = acc.entry(topic).or_insert((0, 0));
        e.0 = e.0.saturating_add(1);
        e.1 = e.1.saturating_add(bytes);
    }
    for (topic, (n, bytes)) in acc {
        map.entry(topic.to_string())
            .or_insert_with(FetchTopicTracker::new)
            .note_fetched(n, bytes, latency);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn metrics_default_zero() {
        assert_eq!(ProducerMetrics::default().records_queued, 0);
        assert_eq!(ProducerMetrics::default().bytes_buffered, 0);
        assert_eq!(ProducerMetrics::default().ack_latency.count, 0);
        assert_eq!(ProducerMetrics::default().ack_latency.p50_nanos, 0);
        assert_eq!(ProducerMetrics::default().ack_latency.p99_nanos, 0);
        assert_eq!(ConsumerMetrics::default().records_fetched, 0);
        assert_eq!(ConsumerMetrics::default().fetch_latency.count, 0);
        assert_eq!(ShareMetrics::default().records_acknowledged, 0);
        assert_eq!(ShareMetrics::default().bytes_fetched, 0);
        assert_eq!(ShareMetrics::default().fetch_errors, 0);
        assert_eq!(ShareMetrics::default().fetch_latency.count, 0);
        assert!(ProducerMetrics::default().topics.is_empty());
        assert!(ConsumerMetrics::default().topics.is_empty());
        assert!(ShareMetrics::default().topics.is_empty());
        assert_eq!(AdminMetrics::default().requests, 0);
        assert_eq!(AdminMetrics::default().errors, 0);
        assert_eq!(AdminMetrics::default().connections, 0);
        assert_eq!(AdminMetrics::default().request_latency.count, 0);
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
        assert_eq!(s.p50_nanos, 20);
        assert_eq!(s.p99_nanos, 30);
        assert!(s.p50_nanos <= s.p99_nanos);
    }

    #[test]
    fn latency_tracker_p50_p99_of_one_to_hundred() {
        let t = LatencyTracker::new();
        for ns in 1..=100 {
            t.record(Duration::from_nanos(ns));
        }
        let s = t.snapshot();
        assert_eq!(s.p50_nanos, 50);
        assert_eq!(s.p99_nanos, 99);
        assert_eq!(s.min_nanos, 1);
        assert_eq!(s.max_nanos, 100);
    }

    #[test]
    fn percentile_index_ceil_rank() {
        assert_eq!(percentile_index(1, 50), 0);
        assert_eq!(percentile_index(3, 99), 2);
        assert_eq!(percentile_index(100, 50), 49);
        assert_eq!(percentile_index(100, 99), 98);
    }

    #[test]
    fn fetch_topics_sorted_and_grouped() {
        let mut map = HashMap::new();
        accumulate_fetch_topics(
            &mut map,
            [("z", 2u64), ("a", 3), ("z", 1)],
            Duration::from_nanos(10),
        );
        let snap = snapshot_fetch_topics(&map);
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].topic, "a");
        assert_eq!(snap[0].records_fetched, 1);
        assert_eq!(snap[0].bytes_fetched, 3);
        assert_eq!(snap[0].fetch_latency.count, 1);
        assert_eq!(snap[1].topic, "z");
        assert_eq!(snap[1].records_fetched, 2);
        assert_eq!(snap[1].bytes_fetched, 3);
        assert_eq!(snap[1].fetch_latency.count, 1);
    }

    #[test]
    fn format_bytes_matches_java_utils() {
        assert_eq!(format_bytes(-1), "-1");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1 KB");
        assert_eq!(format_bytes(1024 * 1024 - 1), "1024 KB");
        assert_eq!(format_bytes(1024 * 1024), "1 MB");
        assert_eq!(format_bytes(1_153_433), "1.1 MB");
        assert_eq!(format_bytes(10 * 1024 * 1024), "10 MB");
        assert_eq!(format_bytes(0), "0.0");
    }

    #[test]
    fn quota_matches_java_metrics() {
        let upper = Quota::upper_bound(1.0);
        assert!(upper.is_upper_bound());
        assert_eq!(upper.bound(), 1.0);
        assert_eq!(upper.to_string(), "upper=1.0");
        assert!(upper.acceptable(1.0));
        assert!(upper.acceptable(0.5));
        assert!(!upper.acceptable(1.1));
        assert_eq!(Quota::new(1.0, true), upper);

        let lower = Quota::lower_bound(1.0);
        assert!(!lower.is_upper_bound());
        assert_eq!(lower.to_string(), "lower=1.0");
        assert!(lower.acceptable(1.0));
        assert!(lower.acceptable(1.1));
        assert!(!lower.acceptable(0.5));

        assert_eq!(
            Quota::upper_bound(f64::INFINITY).to_string(),
            "upper=Infinity"
        );
        assert_eq!(
            Quota::lower_bound(f64::NEG_INFINITY).to_string(),
            "lower=-Infinity"
        );
        assert_eq!(Quota::upper_bound(f64::NAN).to_string(), "upper=NaN");
        assert!(!Quota::upper_bound(1.0).acceptable(f64::NAN));
    }
}
