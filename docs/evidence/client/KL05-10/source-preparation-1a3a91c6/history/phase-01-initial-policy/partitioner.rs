//! Record partitioning: Kafka murmur2 and a pluggable [`Partitioner`].

use std::fmt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use crate::cluster::Cluster;

/// Maps a produce record to a partition index.
///
/// Called only when [`crate::ProduceRecord::partition`] is `None`. The returned
/// value is clamped into `0..num_partitions` by the producer.
pub trait Partitioner: Send + Sync + 'static {
    /// Choose a partition in `0..num_partitions`.
    ///
    /// `key` is `None` when the record has no key.
    fn partition(&self, topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32;

    /// Optional producer admission/batch policy. Existing partitioners use direct routing.
    ///
    /// State is created per producer, rather than inside the shared partitioner.
    fn unkeyed_batching_policy(&self) -> Option<StickyPartitionerConfig> {
        None
    }
}

/// Bounds and reproducible seed for the opt-in uniform sticky policy.
///
/// Routing accounts admitted conservative record sizes plus one 61-byte batch
/// overhead per cohort. It follows Kafka 4.3's uniform (adaptive=false) sticky
/// state machine, not its exact accumulator/compression byte history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StickyPartitionerConfig {
    /// Maximum cached topic histories (zero disables caching).
    pub max_topics: usize,
    /// Maximum queued cohort ledger rows (zero uses singleton pressure mode).
    pub max_cohorts: usize,
    /// Maximum bytes of topic-name text retained by policy state.
    pub max_topic_bytes: usize,
    /// Reproducible draw seed; None obtains a per-producer seed.
    pub seed: Option<u64>,
}

impl Default for StickyPartitionerConfig {
    fn default() -> Self {
        Self { max_topics: 1024, max_cohorts: 4096, max_topic_bytes: 256 * 1024, seed: None }
    }
}

/// Opt-in Kafka 4.3 uniform sticky state machine with Rust packed-byte accounting.
///
/// Pass this to [`crate::ProducerConfig::partitioner`]. Keyed records (including
/// an empty non-null key) keep murmur2. Explicit partitions bypass selection.
/// Actual producer admission and cohort boundaries provide stickiness; calling
/// `partition` directly uses the legacy default fallback because it has no batch
/// or admission context. Independent producers never share mutable sticky state.
#[derive(Debug, Default)]
pub struct StickyPartitioner {
    config: StickyPartitionerConfig,
    fallback: DefaultPartitioner,
}

impl StickyPartitioner {
    /// Use the default bounded policy with a fresh seed per producer.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Configure hard state caps and, optionally, a reproducible seed.
    ///
    /// At a cap the producer uses uniform singleton cohorts until space becomes
    /// available; it does not introduce a new admission error or unbounded state.
    #[must_use]
    pub fn with_config(config: StickyPartitionerConfig) -> Self {
        Self { config, fallback: DefaultPartitioner::new() }
    }

    /// Use a reproducible per-producer draw seed.
    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self::with_config(StickyPartitionerConfig { seed: Some(seed), ..StickyPartitionerConfig::default() })
    }
}

impl Partitioner for StickyPartitioner {
    fn partition(&self, topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32 {
        self.fallback.partition(topic, key, num_partitions)
    }

    fn unkeyed_batching_policy(&self) -> Option<StickyPartitionerConfig> { Some(self.config) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StickyRoute {
    lifetime: Option<u128>,
    generation: Option<u128>,
    identity: Option<[u8; 16]>,
    rng: Option<u64>,
    pub(crate) partition: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StickyTag {
    /// Zero is a stateless singleton; it must never merge with another zero tag.
    pub(crate) id: u128,
    /// Zero while queued in a live ledger; fixed after its first actual drain.
    pub(crate) members: usize,
}

struct StickyInfo { generation: u128, partition: i32, bytes: usize }
struct StickyTopic { identity: Option<[u8; 16]>, lifetime: u128, info: Option<StickyInfo>, touched: u128 }
struct StickyCohort { topic: Arc<str>, lifetime: u128, partition: i32, worker: u64, bytes: usize, members: usize, sealed: bool }

pub(crate) struct StickyAppend {
    pub(crate) tag: StickyTag,
    old_tail: Option<u128>,
    existing_tail: bool,
    cache: bool,
    reset: bool,
    evict: Option<Arc<str>>,
    identity: Option<[u8; 16]>,
    delta: usize,
    record_bytes: usize,
}

/// All mutable state is owned by one Producer::Shared and protected by its short
/// policy mutex. No payloads or copied partition/metadata arrays are retained.
pub(crate) struct StickyState {
    config: StickyPartitionerConfig,
    batch_bytes: usize,
    batch_records: usize,
    topics: HashMap<Arc<str>, StickyTopic>,
    cohorts: HashMap<u128, StickyCohort>,
    topic_bytes: usize,
    next_id: u128,
    clock: u128,
    rng: u64,
    exhausted: bool,
    pub(crate) pressure_admissions: u64,
}

impl StickyState {
    pub(crate) fn new(config: StickyPartitionerConfig, batch_bytes: usize, batch_records: usize) -> Self {
        let seed = config.seed.unwrap_or_else(|| {
            let mut raw = [0u8; 8];
            if getrandom::fill(&mut raw).is_ok() { return u64::from_le_bytes(raw); }
            // Partition draws are not credentials. Entropy failure must not add
            // a new producer error; a clock/process fallback is sufficient here.
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
            u64::try_from(now.as_nanos()).unwrap_or(u64::MAX) ^ u64::from(std::process::id())
        });
        Self { config, batch_bytes: batch_bytes.max(1), batch_records: batch_records.max(1), topics: HashMap::new(), cohorts: HashMap::new(), topic_bytes: 0, next_id: 1, clock: 0, rng: seed, exhausted: false, pressure_admissions: 0 }
    }

    fn draw(seed: u64) -> (u64, i32) {
        let next = seed.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = next;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^= z >> 31;
        (next, i32::try_from(z & 0x7fff_ffff).unwrap_or(0))
    }

    fn choose(seed: u64, leaders: &[i32]) -> i32 {
        let (_, random) = Self::draw(seed);
        let available = leaders.iter().filter(|&&leader| leader >= 0).count();
        let count = if available == 0 { leaders.len() } else { available };
        if count == 0 { return 0; }
        let index = usize::try_from(random).unwrap_or(0) % count;
        if available == 0 { return i32::try_from(index).unwrap_or(0); }
        leaders.iter().enumerate().filter(|(_, leader)| **leader >= 0).nth(index)
            .and_then(|(p, _)| i32::try_from(p).ok()).unwrap_or(0)
    }

    fn identity(cluster: &Cluster, topic: &str) -> Option<[u8; 16]> { cluster.topic_ids.get(topic).copied().filter(|id| *id != [0; 16]) }
    fn reset(topic: &StickyTopic, identity: Option<[u8; 16]>, partitions: usize) -> bool {
        (topic.identity.is_some() && identity.is_some() && topic.identity != identity)
            || topic.info.as_ref().is_some_and(|info| usize::try_from(info.partition).map_or(true, |p| p >= partitions))
    }

    /// Pure peek: failed routing/admission does not consume a draw, evict a
    /// history, charge bytes, or advance a generation.
    pub(crate) fn route(&self, topic: &str, cluster: &Cluster) -> Option<StickyRoute> {
        let leaders = cluster.leaders.get(topic)?;
        if leaders.is_empty() { return None; }
        let identity = Self::identity(cluster, topic);
        let row = self.topics.get(topic);
        let reset = row.is_some_and(|row| Self::reset(row, identity, leaders.len()));
        let info = row.filter(|_| !reset && !self.exhausted).and_then(|row| row.info.as_ref());
        Some(StickyRoute {
            lifetime: row.map(|row| row.lifetime), generation: info.map(|info| info.generation), identity,
            rng: info.is_none().then_some(self.rng),
            partition: info.map_or_else(|| Self::choose(self.rng, leaders), |info| info.partition),
        })
    }

    pub(crate) fn route_current(&self, topic: &str, route: StickyRoute, cluster: &Cluster) -> bool { self.route(topic, cluster) == Some(route) }

    /// Plan is read-only and is committed only after the actual channel enqueue
    /// succeeds while the same policy lock remains held.
    pub(crate) fn plan(&self, topic: &Arc<str>, partition: i32, worker: u64, record_bytes: usize, cluster: &Cluster) -> StickyAppend {
        let identity = Self::identity(cluster, topic);
        let row = self.topics.get(topic);
        let partitions = cluster.leaders.get(topic.as_ref()).map_or(0, Vec::len);
        let reset = row.is_some_and(|row| Self::reset(row, identity, partitions));
        let mut evict = None;
        let mut cache = row.is_some() && !self.exhausted;
        if row.is_none() && !self.exhausted && self.config.max_topics > 0 && topic.len() <= self.config.max_topic_bytes {
            cache = self.topics.len() < self.config.max_topics && self.topic_bytes.saturating_add(topic.len()) <= self.config.max_topic_bytes;
            if !cache {
                evict = self.topics.iter().filter(|(name, _)| !self.cohorts.values().any(|c| c.topic.as_ref() == name.as_ref()))
                    .filter(|(name, _)| self.topic_bytes.saturating_sub(name.len()).saturating_add(topic.len()) <= self.config.max_topic_bytes)
                    .min_by_key(|(_, row)| row.touched).map(|(name, _)| Arc::clone(name));
                cache = evict.is_some();
            }
        }
        let old_tail = row.filter(|_| !reset).and_then(|row| self.cohorts.iter().find(|(_, c)| c.topic == *topic && c.lifetime == row.lifetime && c.partition == partition && c.worker == worker && !c.sealed).map(|(&id, _)| id));
        let existing_tail = old_tail.and_then(|id| self.cohorts.get(&id)).is_some_and(|c| c.members < self.batch_records && c.bytes.saturating_add(record_bytes) <= self.batch_bytes);
        let tracked = existing_tail || (cache && !self.exhausted && self.cohorts.len() < self.config.max_cohorts);
        StickyAppend { tag: StickyTag { id: if existing_tail { old_tail.unwrap_or(0) } else if tracked { self.next_id } else { 0 }, members: if tracked { 0 } else { 1 } }, old_tail, existing_tail, cache, reset, evict, identity, delta: record_bytes.saturating_add(if existing_tail { 0 } else { 61 }), record_bytes }
    }

    fn allocate_id(&mut self) -> u128 {
        let id = self.next_id;
        if let Some(next) = self.next_id.checked_add(1) { self.next_id = next; } else { self.exhausted = true; }
        id
    }

    fn full(&self, topic: &str, lifetime: u128, partition: i32) -> bool {
        !self.cohorts.values().any(|c| c.topic.as_ref() == topic && c.lifetime == lifetime && c.partition == partition && !c.sealed)
    }

    fn maybe_rotate(&mut self, topic: &str, cluster: &Cluster) {
        if self.exhausted { return; }
        let Some(row) = self.topics.get(topic) else { return; };
        let Some(info) = &row.info else { return; };
        if info.bytes < self.batch_bytes || (info.bytes < self.batch_bytes.saturating_mul(2) && !self.full(topic, row.lifetime, info.partition)) { return; }
        let Some(leaders) = cluster.leaders.get(topic).filter(|v| !v.is_empty()) else { return; };
        let partition = Self::choose(self.rng, leaders);
        self.rng = Self::draw(self.rng).0;
        let generation = self.allocate_id();
        if let Some(row) = self.topics.get_mut(topic) { row.info = Some(StickyInfo { generation, partition, bytes: 0 }); }
    }

    pub(crate) fn commit(&mut self, topic: &Arc<str>, partition: i32, worker: u64, unkeyed: bool, plan: StickyAppend, cluster: &Cluster) {
        self.clock = self.clock.saturating_add(1);
        if let Some(name) = plan.evict { if self.topics.remove(&name).is_some() { self.topic_bytes = self.topic_bytes.saturating_sub(name.len()); } }
        let mut canonical = Arc::clone(topic);
        if plan.cache {
            if !self.topics.contains_key(topic) {
                let lifetime = self.allocate_id();
                self.topic_bytes = self.topic_bytes.saturating_add(topic.len());
                self.topics.insert(Arc::clone(topic), StickyTopic { identity: plan.identity, lifetime, info: None, touched: self.clock });
            }
            if let Some((name, _)) = self.topics.get_key_value(topic) { canonical = Arc::clone(name); }
            if plan.reset {
                let lifetime = self.allocate_id();
                if let Some(row) = self.topics.get_mut(topic) { row.lifetime = lifetime; row.identity = plan.identity; row.info = None; }
            }
            if unkeyed && self.topics.get(topic).is_some_and(|row| row.info.is_none()) {
                let generation = self.allocate_id();
                self.rng = Self::draw(self.rng).0;
                if let Some(row) = self.topics.get_mut(topic) { row.info = Some(StickyInfo { generation, partition, bytes: 0 }); }
            }
            if let Some(row) = self.topics.get_mut(topic) { row.touched = self.clock; if row.identity.is_none() { row.identity = plan.identity; } }
        } else if unkeyed {
            self.rng = Self::draw(self.rng).0;
        }
        if !plan.existing_tail { if let Some(id) = plan.old_tail { if let Some(c) = self.cohorts.get_mut(&id) { c.sealed = true; } } }
        if plan.tag.id != 0 {
            if plan.existing_tail {
                if let Some(c) = self.cohorts.get_mut(&plan.tag.id) { c.bytes = c.bytes.saturating_add(plan.record_bytes); c.members = c.members.saturating_add(1); c.sealed = c.bytes >= self.batch_bytes || c.members >= self.batch_records; }
            } else if let Some(row) = self.topics.get(topic) {
                // The prospective cohort id is reserved under the same mutex;
                // lifetime/generation allocation may have consumed that number
                // in a different namespace, so advance beyond it before reuse.
                while self.next_id <= plan.tag.id && !self.exhausted { let _ = self.allocate_id(); }
                self.cohorts.insert(plan.tag.id, StickyCohort { topic: canonical, lifetime: row.lifetime, partition, worker, bytes: plan.delta, members: 1, sealed: plan.delta >= self.batch_bytes || self.batch_records == 1 });
            }
        } else { self.pressure_admissions = self.pressure_admissions.saturating_add(1); }
        if unkeyed { if let Some(row) = self.topics.get_mut(topic) { if let Some(info) = &mut row.info { info.bytes = info.bytes.saturating_add(plan.delta); } } }
        self.maybe_rotate(topic, cluster);
    }

    pub(crate) fn members(&self, tag: StickyTag) -> usize { if tag.id == 0 { 1 } else { self.cohorts.get(&tag.id).map_or(tag.members.max(1), |c| c.members) } }
    pub(crate) fn seal(&mut self, tag: StickyTag) { if let Some(c) = self.cohorts.get_mut(&tag.id) { c.sealed = true; } }
    pub(crate) fn release(&mut self, tag: StickyTag, count: usize, cluster: &Cluster) {
        let Some(c) = self.cohorts.get_mut(&tag.id) else { return; };
        c.members = c.members.saturating_sub(count);
        if c.members != 0 { return; }
        if let Some(c) = self.cohorts.remove(&tag.id) { self.maybe_rotate(&c.topic, cluster); }
    }
    pub(crate) fn clear(&mut self) { self.topics.clear(); self.cohorts.clear(); self.topic_bytes = 0; }
    pub(crate) fn counts(&self) -> (usize, usize, usize, u64) { (self.topics.len(), self.cohorts.len(), self.topic_bytes, self.pressure_admissions) }
}

/// Java `DefaultPartitioner`: murmur2 when there is a key, round-robin if not.
#[derive(Debug, Default)]
pub struct DefaultPartitioner {
    rr: AtomicI32,
}

impl DefaultPartitioner {
    /// Start round-robin at partition 0.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rr: AtomicI32::new(0),
        }
    }
}

impl Partitioner for DefaultPartitioner {
    fn partition(&self, _topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32 {
        if num_partitions <= 0 {
            return 0;
        }
        match key {
            Some(k) => partition_for_key(k, num_partitions),
            None => to_positive(self.rr.fetch_add(1, Ordering::Relaxed)) % num_partitions,
        }
    }
}

/// [`Arc`] wrapper so [`crate::ProducerConfig`] can stay `Clone` + `Debug`.
#[derive(Clone)]
pub struct PartitionerBox(Arc<dyn Partitioner>);

impl PartitionerBox {
    /// Wrap any [`Partitioner`].
    pub fn new(p: impl Partitioner) -> Self {
        Self(Arc::new(p))
    }

    pub(crate) fn arc(&self) -> Arc<dyn Partitioner> {
        Arc::clone(&self.0)
    }
}

impl Default for PartitionerBox {
    fn default() -> Self {
        Self::new(DefaultPartitioner::new())
    }
}

impl fmt::Debug for PartitionerBox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Partitioner")
    }
}

/// Kafka-compatible Murmur2 (seed 0x9747b28c), matching
/// `org.apache.kafka.common.utils.Utils.murmur2`.
#[must_use]
pub fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;
    const SEED: u32 = 0x9747b28c;
    let len = u32::try_from(data.len()).unwrap_or(u32::MAX);
    let mut h = SEED ^ len;
    let (chunks, rest) = data.split_at(data.len() / 4 * 4);
    for chunk in chunks.chunks_exact(4) {
        let &[a, b, c, d] = chunk else {
            continue;
        };
        let mut k =
            u32::from(a) | (u32::from(b) << 8) | (u32::from(c) << 16) | (u32::from(d) << 24);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    match *rest {
        [a, b, c] => {
            h ^= u32::from(c) << 16;
            h ^= u32::from(b) << 8;
            h ^= u32::from(a);
            h = h.wrapping_mul(M);
        }
        [a, b] => {
            h ^= u32::from(b) << 8;
            h ^= u32::from(a);
            h = h.wrapping_mul(M);
        }
        [a] => {
            h ^= u32::from(a);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    i32::from_ne_bytes(h.to_ne_bytes())
}

/// Java `Utils.toPositive` (`number & 0x7fffffff`).
///
/// Used so a murmur2 hash can be a partition index. This is not
/// [`abs`]: negative inputs keep the low 31 bits rather than the
/// magnitude.
#[must_use]
pub fn to_positive(n: i32) -> i32 {
    n & 0x7fff_ffff
}

/// Java `Utils.abs`. [`i32::MIN`] is `0` (unlike [`i32::abs`]).
#[must_use]
pub fn abs(n: i32) -> i32 {
    n.checked_abs().unwrap_or(0)
}

/// Java `DefaultPartitioner` for a keyed record: `murmur2(key) % num_partitions`.
#[must_use]
pub fn partition_for_key(key: &[u8], num_partitions: i32) -> i32 {
    if num_partitions <= 0 {
        return 0;
    }
    to_positive(murmur2(key)) % num_partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur2_matches_java_utils() {
        // Empty-string vector is the widely copied Java Utils.murmur2 result.
        assert_eq!(murmur2(b""), 275_646_681);
        assert_eq!(murmur2(b"kafka"), -798_503_068);
        assert_eq!(partition_for_key(b"key", 1), 0);
        assert!(partition_for_key(b"key", 16) >= 0);
        assert!(partition_for_key(b"key", 16) < 16);
        assert_eq!(partition_for_key(b"key", 0), 0);
        assert_eq!(to_positive(-1), i32::MAX);
        assert_eq!(to_positive(1), 1);
        assert_eq!(to_positive(i32::MIN), 0);
    }

    #[test]
    fn abs_matches_java_utils() {
        assert_eq!(abs(i32::MIN), 0);
        assert_eq!(abs(-10), 10);
        assert_eq!(abs(10), 10);
        assert_eq!(abs(0), 0);
        assert_eq!(abs(-1), 1);
    }

    #[test]
    fn default_partitioner_keys_match_murmur2() {
        let p = DefaultPartitioner::new();
        assert_eq!(
            p.partition("t", Some(b"key"), 16),
            partition_for_key(b"key", 16)
        );
        let a = p.partition("t", None, 3);
        let b = p.partition("t", None, 3);
        assert_ne!(a, b);
        assert!((0..3).contains(&a));
        assert!((0..3).contains(&b));
    }
}
