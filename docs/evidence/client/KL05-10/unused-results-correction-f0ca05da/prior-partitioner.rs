//! Record partitioning: Kafka murmur2 and a pluggable [`Partitioner`].

use std::collections::HashMap;
use std::fmt;
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
    /// Maximum cached topic histories, capped at 1024 (zero disables caching).
    pub max_topics: usize,
    /// Maximum outstanding cohort ledger rows, capped at 4096 (zero uses singleton pressure mode).
    pub max_cohorts: usize,
    /// Maximum retained topic-name bytes, capped at 256 KiB.
    pub max_topic_bytes: usize,
    /// Reserved admission slots plus accepted records, clamped to 1..=100,000.
    /// The bound includes empty/null records and unlimited payload-byte budgets.
    pub max_pending_records: usize,
    /// Reproducible draw seed; None obtains a per-producer seed.
    pub seed: Option<u64>,
}

impl Default for StickyPartitionerConfig {
    fn default() -> Self {
        Self {
            max_topics: 1024,
            max_cohorts: 4096,
            max_topic_bytes: 256 * 1024,
            max_pending_records: 100_000,
            seed: None,
        }
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure hard state caps and, optionally, a reproducible seed.
    ///
    /// At a topic/cohort/text cap the producer uses uniform singleton cohorts
    /// until space becomes available. Exhausting record slots uses the existing
    /// QueueFull or max_block wait behavior, with no new error type.
    #[must_use]
    pub fn with_config(config: StickyPartitionerConfig) -> Self {
        Self {
            config,
            fallback: DefaultPartitioner::new(),
        }
    }

    /// Use a reproducible per-producer draw seed.
    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self::with_config(StickyPartitionerConfig {
            seed: Some(seed),
            ..StickyPartitionerConfig::default()
        })
    }
}

impl Partitioner for StickyPartitioner {
    fn partition(&self, topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32 {
        self.fallback.partition(topic, key, num_partitions)
    }

    fn unkeyed_batching_policy(&self) -> Option<StickyPartitionerConfig> {
        Some(self.config)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StickyRoute {
    lifetime: Option<u128>,
    generation: Option<u128>,
    identity: Option<[u8; 16]>,
    pub(crate) rng: Option<u64>,
    pub(crate) partition: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StickyTag {
    /// Zero is a stateless singleton; it must never merge with another zero tag.
    pub(crate) id: u128,
    /// Original append order within this bounded cohort, preserved on retries.
    pub(crate) index: usize,
    /// Metadata identity captured only after actual admission; future metadata
    /// resets do not retarget already accepted records to a recreated topic.
    pub(crate) identity: Option<[u8; 16]>,
}

struct StickyInfo {
    generation: u128,
    partition: i32,
    bytes: usize,
}
struct StickyTopic {
    identity: Option<[u8; 16]>,
    lifetime: u128,
    info: Option<StickyInfo>,
    touched: u128,
}
struct StickyCohort {
    topic: Arc<str>,
    lifetime: u128,
    partition: i32,
    worker: Arc<()>,
    bytes: usize,
    members: usize,
    admitted: usize,
    sealed: bool,
    cancelled: bool,
}

pub(crate) struct StickyAppend {
    pub(crate) tag: StickyTag,
    pub(crate) next_rng: Option<u64>,
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
    pressure_admissions: u64,
    admitted_unkeyed_bytes: u64,
    closed: bool,
}

impl StickyState {
    pub(crate) fn new(
        config: StickyPartitionerConfig,
        batch_bytes: usize,
        batch_records: usize,
    ) -> Self {
        let config = StickyPartitionerConfig {
            max_topics: config.max_topics.min(1024),
            max_cohorts: config.max_cohorts.min(4096),
            max_topic_bytes: config.max_topic_bytes.min(256 * 1024),
            max_pending_records: config.max_pending_records.clamp(1, 100_000),
            ..config
        };
        let seed = config.seed.unwrap_or_else(|| {
            let mut raw = [0u8; 8];
            if getrandom::fill(&mut raw).is_ok() {
                return u64::from_le_bytes(raw);
            }
            // Partition draws are not credentials. Entropy failure must not add
            // a new producer error; a clock/process fallback is sufficient here.
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            u64::try_from(now.as_nanos()).unwrap_or(u64::MAX) ^ u64::from(std::process::id())
        });
        Self {
            config,
            batch_bytes: if batch_bytes == 0 {
                usize::MAX
            } else {
                batch_bytes
            },
            batch_records: batch_records.max(1),
            topics: HashMap::new(),
            cohorts: HashMap::new(),
            topic_bytes: 0,
            next_id: 1,
            clock: 0,
            rng: seed,
            exhausted: false,
            pressure_admissions: 0,
            admitted_unkeyed_bytes: 0,
            closed: false,
        }
    }

    fn draw(seed: u64) -> (u64, i32) {
        let next = seed.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = next;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^= z >> 31;
        (next, i32::try_from(z & 0x7fff_ffff).unwrap_or(0))
    }

    /// Uniform over eligible indices: the incomplete modulus bucket is rejected.
    /// Return the state after every attempted draw without mutating the policy.
    fn choose(seed: u64, leaders: &[i32]) -> (i32, u64) {
        let available = leaders.iter().filter(|&&leader| leader >= 0).count();
        let count = if available == 0 {
            leaders.len()
        } else {
            available
        };
        if count == 0 {
            return (0, seed);
        }
        // Kafka partition indices/counts are int32. Ignore an unrepresentable
        // suffix if called with a non-protocol, oversized metadata slice.
        let count = u64::try_from(count.min(2_147_483_647)).unwrap_or(1);
        let domain = 1u64 << 31;
        let accepted_domain = domain - domain % count;
        let mut post = seed;
        let index = loop {
            let (next, random) = Self::draw(post);
            post = next;
            let random = u64::try_from(random).unwrap_or(0);
            if random < accepted_domain {
                break usize::try_from(random % count).unwrap_or(0);
            }
        };
        let partition = if available == 0 {
            i32::try_from(index).unwrap_or(0)
        } else {
            leaders
                .iter()
                .enumerate()
                .filter(|(_, leader)| **leader >= 0)
                .nth(index)
                .and_then(|(p, _)| i32::try_from(p).ok())
                .unwrap_or(0)
        };
        (partition, post)
    }

    fn identity(cluster: &Cluster, topic: &str) -> Option<[u8; 16]> {
        cluster
            .topic_ids
            .get(topic)
            .copied()
            .filter(|id| *id != [0; 16])
    }
    fn reset(topic: &StickyTopic, identity: Option<[u8; 16]>, partitions: usize) -> bool {
        (topic.identity.is_some() && identity.is_some() && topic.identity != identity)
            || topic.info.as_ref().is_some_and(|info| {
                usize::try_from(info.partition).map_or(true, |p| p >= partitions)
            })
    }

    /// Pure peek: failed routing/admission does not consume a draw, evict a
    /// history, charge bytes, or advance a generation.
    pub(crate) fn route(&self, topic: &str, cluster: &Cluster) -> Option<StickyRoute> {
        if self.closed {
            return None;
        }
        let leaders = cluster.leaders.get(topic)?;
        if leaders.is_empty() {
            return None;
        }
        let identity = Self::identity(cluster, topic);
        let row = self.topics.get(topic);
        let reset = row.is_some_and(|row| Self::reset(row, identity, leaders.len()));
        let info = row
            .filter(|_| !reset && !self.exhausted)
            .and_then(|row| row.info.as_ref())
            .filter(|info| {
                self.cohorts.len() < self.config.max_cohorts
                    || row.is_some_and(|row| !self.full(topic, row.lifetime, info.partition))
            });
        let drawn = info.is_none().then(|| Self::choose(self.rng, leaders));
        Some(StickyRoute {
            lifetime: row.map(|row| row.lifetime),
            generation: info.map(|info| info.generation),
            identity,
            rng: drawn.map(|(_, post)| post),
            partition: info.map_or_else(
                || drawn.map_or(0, |(partition, _)| partition),
                |info| info.partition,
            ),
        })
    }

    pub(crate) fn route_current(&self, topic: &str, route: StickyRoute, cluster: &Cluster) -> bool {
        self.route(topic, cluster) == Some(route)
    }

    /// Plan is read-only and is committed only after the actual channel enqueue
    /// succeeds while the same policy lock remains held.
    pub(crate) fn plan(
        &self,
        topic: &Arc<str>,
        partition: i32,
        worker: &Arc<()>,
        record_bytes: usize,
        cluster: &Cluster,
    ) -> StickyAppend {
        let identity = Self::identity(cluster, topic);
        let row = self.topics.get(topic);
        let partitions = cluster.leaders.get(topic.as_ref()).map_or(0, Vec::len);
        let reset = row.is_some_and(|row| Self::reset(row, identity, partitions));
        let mut evict = None;
        let mut cache = row.is_some() && !self.exhausted;
        if row.is_none()
            && !self.exhausted
            && self.config.max_topics > 0
            && topic.len() <= self.config.max_topic_bytes
        {
            cache = self.topics.len() < self.config.max_topics
                && self.topic_bytes.saturating_add(topic.len()) <= self.config.max_topic_bytes;
            if !cache {
                evict = self
                    .topics
                    .iter()
                    .filter(|(name, _)| {
                        !self
                            .cohorts
                            .values()
                            .any(|c| c.topic.as_ref() == name.as_ref())
                    })
                    .filter(|(name, _)| {
                        self.topic_bytes
                            .saturating_sub(name.len())
                            .saturating_add(topic.len())
                            <= self.config.max_topic_bytes
                    })
                    .min_by_key(|(_, row)| row.touched)
                    .map(|(name, _)| Arc::clone(name));
                cache = evict.is_some();
            }
        }
        let old_tail = row.filter(|_| !reset).and_then(|row| {
            self.cohorts
                .iter()
                .find(|(_, c)| {
                    c.topic == *topic
                        && c.lifetime == row.lifetime
                        && c.partition == partition
                        && Arc::ptr_eq(&c.worker, worker)
                        && !c.sealed
                })
                .map(|(&id, _)| id)
        });
        let existing_tail = old_tail
            .and_then(|id| self.cohorts.get(&id))
            .is_some_and(|c| {
                c.admitted < self.batch_records
                    && !self.exhausted
                    && c.bytes.saturating_add(record_bytes) <= self.batch_bytes
            });
        let tracked = existing_tail
            || (cache && !self.exhausted && self.cohorts.len() < self.config.max_cohorts);
        StickyAppend {
            next_rng: None,
            tag: StickyTag {
                id: if existing_tail {
                    old_tail.unwrap_or(0)
                } else if tracked {
                    self.next_id
                } else {
                    0
                },
                index: if existing_tail {
                    old_tail
                        .and_then(|id| self.cohorts.get(&id))
                        .map_or(0, |c| c.admitted)
                } else {
                    0
                },
                identity,
            },
            old_tail,
            existing_tail,
            cache,
            reset,
            evict,
            identity,
            delta: record_bytes.saturating_add(if existing_tail { 0 } else { 61 }),
            record_bytes,
        }
    }

    fn allocate_id(&mut self) -> u128 {
        let id = self.next_id;
        if let Some(next) = self.next_id.checked_add(1) {
            self.next_id = next;
        } else {
            self.exhausted = true;
        }
        id
    }

    fn full(&self, topic: &str, lifetime: u128, partition: i32) -> bool {
        !self.cohorts.values().any(|c| {
            c.topic.as_ref() == topic
                && c.lifetime == lifetime
                && c.partition == partition
                && !c.sealed
        })
    }

    fn maybe_rotate(&mut self, topic: &str, cluster: &Cluster) {
        if self.exhausted || self.closed {
            return;
        }
        let Some(row) = self.topics.get(topic) else {
            return;
        };
        let Some(info) = &row.info else {
            return;
        };
        if info.bytes < self.batch_bytes
            || (info.bytes < self.batch_bytes.saturating_mul(2)
                && !self.full(topic, row.lifetime, info.partition))
        {
            return;
        }
        let Some(leaders) = cluster.leaders.get(topic).filter(|v| !v.is_empty()) else {
            return;
        };
        let (partition, post) = Self::choose(self.rng, leaders);
        self.rng = post;
        let generation = self.allocate_id();
        if let Some(row) = self.topics.get_mut(topic) {
            row.info = Some(StickyInfo {
                generation,
                partition,
                bytes: 0,
            });
        }
    }

    pub(crate) fn commit(
        &mut self,
        topic: &Arc<str>,
        partition: i32,
        worker: &Arc<()>,
        unkeyed: bool,
        plan: StickyAppend,
        cluster: &Cluster,
    ) {
        self.clock = self.clock.saturating_add(1);
        if let Some(name) = plan.evict {
            if self.topics.remove(&name).is_some() {
                self.topic_bytes = self.topic_bytes.saturating_sub(name.len());
            }
        }
        let mut canonical = Arc::clone(topic);
        if plan.cache {
            if !self.topics.contains_key(topic) {
                let lifetime = self.allocate_id();
                self.topic_bytes = self.topic_bytes.saturating_add(topic.len());
                self.topics.insert(
                    Arc::clone(topic),
                    StickyTopic {
                        identity: plan.identity,
                        lifetime,
                        info: None,
                        touched: self.clock,
                    },
                );
            }
            if let Some((name, _)) = self.topics.get_key_value(topic) {
                canonical = Arc::clone(name);
            }
            if plan.reset {
                let lifetime = self.allocate_id();
                if let Some(row) = self.topics.get_mut(topic) {
                    row.lifetime = lifetime;
                    row.identity = plan.identity;
                    row.info = None;
                }
            }
            if unkeyed
                && plan.tag.id != 0
                && self.topics.get(topic).is_some_and(|row| row.info.is_none())
            {
                let generation = self.allocate_id();
                if let Some(post) = plan.next_rng {
                    self.rng = post;
                }
                if let Some(row) = self.topics.get_mut(topic) {
                    row.info = Some(StickyInfo {
                        generation,
                        partition,
                        bytes: 0,
                    });
                }
            }
            if let Some(row) = self.topics.get_mut(topic) {
                row.touched = self.clock;
                if row.identity.is_none() {
                    row.identity = plan.identity;
                }
            }
        }
        if unkeyed && plan.tag.id == 0 {
            if let Some(post) = plan.next_rng {
                self.rng = post;
            }
            if let Some(row) = self.topics.get_mut(topic) {
                row.info = None;
            }
        }
        if !plan.existing_tail {
            if let Some(id) = plan.old_tail {
                if let Some(c) = self.cohorts.get_mut(&id) {
                    c.sealed = true;
                }
            }
        }
        if plan.tag.id != 0 {
            if plan.existing_tail {
                if let Some(c) = self.cohorts.get_mut(&plan.tag.id) {
                    c.bytes = c.bytes.saturating_add(plan.record_bytes);
                    c.members = c.members.saturating_add(1);
                    c.admitted = c.admitted.saturating_add(1);
                    c.sealed = c.bytes >= self.batch_bytes || c.admitted >= self.batch_records;
                }
            } else if let Some(lifetime) = self.topics.get(topic).map(|row| row.lifetime) {
                // The prospective cohort id is reserved under the same mutex;
                // lifetime/generation allocation may have consumed that number
                // in a different namespace, so advance beyond it before reuse.
                while self.next_id <= plan.tag.id && !self.exhausted {
                    let _ = self.allocate_id();
                }
                self.cohorts.insert(
                    plan.tag.id,
                    StickyCohort {
                        topic: canonical,
                        lifetime,
                        partition,
                        worker: Arc::clone(worker),
                        bytes: plan.delta,
                        members: 1,
                        admitted: 1,
                        sealed: plan.delta >= self.batch_bytes || self.batch_records == 1,
                        cancelled: false,
                    },
                );
            }
        } else {
            self.pressure_admissions = self.pressure_admissions.saturating_add(1);
        }
        if unkeyed {
            self.admitted_unkeyed_bytes = self
                .admitted_unkeyed_bytes
                .saturating_add(u64::try_from(plan.delta).unwrap_or(u64::MAX));
            if let Some(row) = self.topics.get_mut(topic) {
                if let Some(info) = &mut row.info {
                    info.bytes = info.bytes.saturating_add(plan.delta);
                }
            }
        }
        self.maybe_rotate(topic, cluster);
    }

    pub(crate) fn members(&self, tag: StickyTag) -> usize {
        if tag.id == 0 {
            1
        } else {
            self.cohorts.get(&tag.id).map_or(1, |c| c.members)
        }
    }
    pub(crate) fn cancelled(&self, tag: StickyTag) -> bool {
        self.cohorts.get(&tag.id).is_some_and(|c| c.cancelled)
    }
    pub(crate) fn cancel(&mut self, tag: StickyTag) {
        if let Some(c) = self.cohorts.get_mut(&tag.id) {
            c.cancelled = true;
            c.sealed = true;
        }
    }
    pub(crate) fn owned_by(&self, tag: StickyTag, worker: &Arc<()>) -> bool {
        tag.id == 0
            || self
                .cohorts
                .get(&tag.id)
                .is_none_or(|c| Arc::ptr_eq(&c.worker, worker))
    }
    pub(crate) fn rebind(&mut self, tag: StickyTag, worker: &Arc<()>) {
        if let Some(c) = self.cohorts.get_mut(&tag.id) {
            c.worker = Arc::clone(worker);
            c.sealed = true;
        }
    }
    pub(crate) fn seal(&mut self, tag: StickyTag) {
        if let Some(c) = self.cohorts.get_mut(&tag.id) {
            c.sealed = true;
        }
    }
    pub(crate) fn release(&mut self, tag: StickyTag, count: usize, cluster: &Cluster) {
        let Some(c) = self.cohorts.get_mut(&tag.id) else {
            return;
        };
        c.members = c.members.saturating_sub(count);
        if c.members != 0 {
            return;
        }
        if let Some(c) = self.cohorts.remove(&tag.id) {
            if self
                .topics
                .get(&c.topic)
                .is_some_and(|t| t.lifetime == c.lifetime)
            {
                self.maybe_rotate(&c.topic, cluster);
            }
        }
    }
    /// Seal on the first actual drain, preserving outstanding membership through retries.
    pub(crate) fn drain(&mut self, tag: StickyTag, cluster: &Cluster) {
        let row = self.cohorts.get_mut(&tag.id).map(|c| {
            c.sealed = true;
            (Arc::clone(&c.topic), c.lifetime)
        });
        if let Some((topic, lifetime)) = row {
            if self
                .topics
                .get(&topic)
                .is_some_and(|t| t.lifetime == lifetime)
            {
                self.maybe_rotate(&topic, cluster);
            }
        }
    }
    pub(crate) fn clear(&mut self) {
        self.closed = true;
        self.topics.clear();
        self.cohorts.clear();
        self.topic_bytes = 0;
    }
    pub(crate) fn counts(&self) -> (usize, usize, usize, u64, u64) {
        (
            self.topics.len(),
            self.cohorts.len(),
            self.topic_bytes,
            self.pressure_admissions,
            self.admitted_unkeyed_bytes,
        )
    }
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

#[cfg(test)]
mod sticky_tests {
    use super::*;

    fn metadata(leaders: Vec<i32>) -> Cluster {
        let mut cluster = Cluster::default();
        let _ = cluster.leaders.insert("t".to_owned(), leaders);
        cluster
    }

    fn append(
        state: &mut StickyState,
        cluster: &Cluster,
        worker: &Arc<()>,
        partition: i32,
        bytes: usize,
        unkeyed: bool,
    ) -> StickyTag {
        let topic: Arc<str> = Arc::from("t");
        let route = unkeyed.then(|| state.route(&topic, cluster)).flatten();
        let mut plan = state.plan(&topic, partition, worker, bytes, cluster);
        plan.next_rng = route.and_then(|route| route.rng);
        let tag = plan.tag;
        state.commit(&topic, partition, worker, unkeyed, plan, cluster);
        tag
    }

    #[test]
    fn prepared_routes_and_plans_are_pure_until_commit() {
        let cluster = metadata(vec![0, -1, 0]);
        let state = StickyState::new(
            StickyPartitionerConfig {
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            },
            300,
            100,
        );
        let route = state.route("t", &cluster).unwrap();
        let worker = Arc::new(());
        let topic: Arc<str> = Arc::from("t");
        let before = state.counts();
        for _ in 0..100 {
            let _ = state.plan(&topic, route.partition, &worker, 100, &cluster);
            assert_eq!(state.route("t", &cluster), Some(route));
            assert_eq!(state.counts(), before);
        }
        assert!(route.partition == 0 || route.partition == 2);
    }

    #[test]
    fn real_tail_fullness_defers_b_then_zero_byte_drain_completes_rotation() {
        let cluster = metadata(vec![0, 0, 0]);
        let mut state = StickyState::new(
            StickyPartitionerConfig {
                seed: Some(1),
                ..StickyPartitionerConfig::default()
            },
            300,
            100,
        );
        let worker = Arc::new(());
        let partition = state.route("t", &cluster).unwrap().partition;
        // Each admission is 139 conservative bytes; its new cohort costs61.
        let first = append(&mut state, &cluster, &worker, partition, 139, true);
        let generation = state.route("t", &cluster).unwrap().generation;
        // Cannot fit in the first200B cohort, so opens a second200B tail.
        let second = append(&mut state, &cluster, &worker, partition, 139, true);
        assert_ne!(first.id, second.id);
        assert_eq!(state.route("t", &cluster).unwrap().generation, generation);
        state.drain(first, &cluster);
        assert_eq!(state.route("t", &cluster).unwrap().generation, generation);
        state.drain(second, &cluster);
        assert_ne!(state.route("t", &cluster).unwrap().generation, generation);
        assert_eq!(state.counts().4, 400);
        // Drain alone retains terminal membership for a retry.
        assert_eq!(state.counts().1, 2);
        state.release(first, 1, &cluster);
        state.release(second, 1, &cluster);
        assert_eq!(state.counts().1, 0);
    }

    #[test]
    fn two_b_forces_rotation_with_an_incomplete_tail_and_can_redraw_same_partition() {
        let cluster = metadata(vec![0]);
        let mut state = StickyState::new(
            StickyPartitionerConfig {
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            },
            300,
            100,
        );
        let worker = Arc::new(());
        let first = append(&mut state, &cluster, &worker, 0, 139, true);
        let generation = state.route("t", &cluster).unwrap().generation;
        let _ = append(&mut state, &cluster, &worker, 0, 139, true);
        let _ = append(&mut state, &cluster, &worker, 0, 139, true);
        let after = state.route("t", &cluster).unwrap();
        assert_eq!(after.partition, 0);
        assert_ne!(after.generation, generation);
        assert_eq!(state.counts().4, 600);
        assert_eq!(state.members(first), 1);
    }

    #[test]
    fn keyed_overhead_is_not_charged_to_the_unkeyed_counter() {
        let cluster = metadata(vec![0]);
        let mut state = StickyState::new(
            StickyPartitionerConfig {
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            },
            1000,
            100,
        );
        let worker = Arc::new(());
        let first = append(&mut state, &cluster, &worker, 0, 100, false);
        assert_eq!(state.counts().4, 0);
        let second = append(&mut state, &cluster, &worker, 0, 100, true);
        assert_eq!(first.id, second.id);
        assert_eq!(second.index, 1);
        assert_eq!(state.counts().4, 100);
    }

    #[test]
    fn uuid_reset_and_removed_partition_preserve_old_delivery_rows_without_stale_rotation() {
        let mut cluster = metadata(vec![0, 0, 0]);
        let _ = cluster.topic_ids.insert("t".to_owned(), [1; 16]);
        let mut state = StickyState::new(
            StickyPartitionerConfig {
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            },
            300,
            100,
        );
        let worker = Arc::new(());
        let partition = state.route("t", &cluster).unwrap().partition;
        let old = append(&mut state, &cluster, &worker, partition, 139, true);
        let before = state.route("t", &cluster).unwrap();
        let _ = cluster.topic_ids.insert("t".to_owned(), [2; 16]);
        let selected = state.route("t", &cluster).unwrap();
        assert_eq!(selected.generation, None);
        let _ = append(&mut state, &cluster, &worker, selected.partition, 139, true);
        let after = state.route("t", &cluster).unwrap();
        assert_ne!(before.lifetime, after.lifetime);
        state.drain(old, &cluster);
        state.release(old, 1, &cluster);
        assert_eq!(state.route("t", &cluster), Some(after));
        let _ = cluster.leaders.insert("t".to_owned(), vec![0]);
        assert_eq!(state.route("t", &cluster).unwrap().partition, 0);
    }

    #[test]
    fn partial_terminal_expiry_shrinks_members_and_sequenced_cancellation_is_sticky() {
        let cluster = metadata(vec![0]);
        let mut state = StickyState::new(StickyPartitionerConfig::default(), 1000, 100);
        let worker = Arc::new(());
        let first = append(&mut state, &cluster, &worker, 0, 100, true);
        let second = append(&mut state, &cluster, &worker, 0, 100, true);
        state.drain(first, &cluster);
        state.release(first, 1, &cluster);
        assert_eq!(state.members(second), 1);
        state.cancel(second);
        assert!(state.cancelled(second));
        let other = Arc::new(());
        state.rebind(second, &other);
        assert!(state.owned_by(second, &other));
        assert!(!state.owned_by(second, &worker));
        assert!(state.cancelled(second));
        state.release(second, 1, &cluster);
        assert_eq!(state.counts().1, 0);
    }

    #[test]
    fn close_discards_bounded_history_and_late_callbacks_cannot_recreate_it() {
        let cluster = metadata(vec![0]);
        let mut state = StickyState::new(StickyPartitionerConfig::default(), 1000, 100);
        let worker = Arc::new(());
        let first = append(&mut state, &cluster, &worker, 0, 100, true);
        state.clear();
        state.drain(first, &cluster);
        state.release(first, 1, &cluster);
        state.rebind(first, &worker);
        assert_eq!(state.route("t", &cluster), None);
        let counts = state.counts();
        assert_eq!((counts.0, counts.1, counts.2), (0, 0, 0));
    }
}

#[cfg(test)]
mod sticky_uniformity_tests {
    use super::*;

    #[test]
    fn actual_seeded_policy_rotations_are_uniform_over_available_leaders() {
        let mut cluster = Cluster::default();
        let _ = cluster
            .leaders
            .insert("t".to_owned(), vec![0, -1, 0, -1, 0, -1]);
        let topic: Arc<str> = Arc::from("t");
        let worker = Arc::new(());
        let mut state = StickyState::new(
            StickyPartitionerConfig {
                seed: Some(79443),
                ..StickyPartitionerConfig::default()
            },
            100,
            100,
        );
        let mut counts = [0usize; 6];
        for _ in 0..60_000 {
            let route = state.route("t", &cluster).unwrap();
            let index = usize::try_from(route.partition).unwrap();
            *counts.get_mut(index).unwrap() += 1;
            let mut plan = state.plan(&topic, route.partition, &worker, 100, &cluster);
            plan.next_rng = route.rng;
            let tag = plan.tag;
            state.commit(&topic, route.partition, &worker, true, plan, &cluster);
            state.release(tag, 1, &cluster);
        }
        for (partition, count) in counts.into_iter().enumerate() {
            if partition % 2 == 0 {
                assert!(
                    (18_000..=22_000).contains(&count),
                    "partition={partition} count={count}"
                );
            } else {
                assert_eq!(count, 0);
            }
        }
        assert_eq!(state.counts().1, 0);
        assert_eq!(state.counts().4, 60_000 * 161);
    }
}

#[cfg(test)]
mod sticky_rejection_tests {
    use super::*;

    #[test]
    fn non_power_of_two_rejection_peeks_do_not_consume_rng_and_commit_all_attempted_draws() {
        // Seeds obtained by inverting the documented SplitMix64 transform; the
        // first masked values are the two excluded values in the 3-way domain.
        for (seed, rejected, partitions, expected_partition, post) in [
            (
                0xf7fd_dcff_99ab_5ded,
                2_147_483_647,
                3,
                1,
                0x346c_d072_9840_5617,
            ),
            (
                0xf199_1e83_504c_5420,
                2_147_483_646,
                3,
                1,
                0x2e08_11f6_4ee1_4c4a,
            ),
            (
                0xf7fd_dcff_99ab_5ded,
                2_147_483_647,
                5,
                3,
                0x346c_d072_9840_5617,
            ),
        ] {
            let mut cluster = Cluster::default();
            let _ = cluster.leaders.insert("t".to_owned(), vec![0; partitions]);
            let topic: Arc<str> = Arc::from("t");
            let worker = Arc::new(());
            let mut state = StickyState::new(
                StickyPartitionerConfig {
                    seed: Some(seed),
                    ..StickyPartitionerConfig::default()
                },
                1000,
                100,
            );
            assert_eq!(StickyState::draw(seed).1, rejected);
            let before = state.counts();
            let route = state.route(&topic, &cluster).unwrap();
            assert_eq!(route.partition, expected_partition);
            assert_eq!(route.rng, Some(post));
            for _ in 0..100 {
                let _ = state.plan(&topic, route.partition, &worker, 100, &cluster);
                assert_eq!(state.route(&topic, &cluster), Some(route));
                assert_eq!(state.rng, seed);
                assert_eq!(state.counts(), before);
            }
            let mut plan = state.plan(&topic, route.partition, &worker, 100, &cluster);
            plan.next_rng = route.rng;
            state.commit(&topic, route.partition, &worker, true, plan, &cluster);
            assert_eq!(state.rng, post);
            assert_eq!(state.route(&topic, &cluster).unwrap().rng, None);
            assert_eq!(state.counts().4, 161);
        }
    }

    #[test]
    fn rotation_and_pressure_consume_the_returned_rejection_state_for_eligible_leaders() {
        let seed = 0xf7fd_dcff_99ab_5ded;
        let post = 0x346c_d072_9840_5617;
        let mut cluster = Cluster::default();
        let _ = cluster
            .leaders
            .insert("t".to_owned(), vec![0, -1, 0, -1, 0]);
        let topic: Arc<str> = Arc::from("t");
        let worker = Arc::new(());
        let mut state = StickyState::new(
            StickyPartitionerConfig {
                seed: Some(seed),
                ..StickyPartitionerConfig::default()
            },
            1000,
            100,
        );
        let route = state.route(&topic, &cluster).unwrap();
        assert_eq!((route.partition, route.rng), (2, Some(post)));
        let mut plan = state.plan(&topic, route.partition, &worker, 100, &cluster);
        plan.next_rng = route.rng;
        let tag = plan.tag;
        state.commit(&topic, route.partition, &worker, true, plan, &cluster);
        state.rng = seed;
        state
            .topics
            .get_mut(&topic)
            .unwrap()
            .info
            .as_mut()
            .unwrap()
            .bytes = 1000;
        state.drain(tag, &cluster);
        assert_eq!(state.rng, post);
        assert_eq!(state.route(&topic, &cluster).unwrap().partition, 2);
        let mut pressure = StickyState::new(
            StickyPartitionerConfig {
                max_topics: 0,
                seed: Some(seed),
                ..StickyPartitionerConfig::default()
            },
            1000,
            100,
        );
        let route = pressure.route(&topic, &cluster).unwrap();
        let mut plan = pressure.plan(&topic, route.partition, &worker, 100, &cluster);
        assert_eq!(plan.tag.id, 0);
        plan.next_rng = route.rng;
        pressure.commit(&topic, route.partition, &worker, true, plan, &cluster);
        assert_eq!(pressure.rng, post);
        assert_eq!(pressure.counts().3, 1);
    }
}

#[cfg(test)]
mod sticky_pressure_rng_tests {
    use super::*;

    #[test]
    fn cached_route_pressure_never_consumes_a_draw_that_was_not_used_for_selection() {
        let mut cluster = Cluster::default();
        let _ = cluster.leaders.insert("t".to_owned(), vec![0, 0, 0]);
        let topic: Arc<str> = Arc::from("t");
        let worker = Arc::new(());
        let mut state = StickyState::new(
            StickyPartitionerConfig {
                max_cohorts: 1,
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            },
            300,
            100,
        );
        let first = state.route(&topic, &cluster).unwrap();
        let mut plan = state.plan(&topic, first.partition, &worker, 100, &cluster);
        plan.next_rng = first.rng;
        state.commit(&topic, first.partition, &worker, true, plan, &cluster);
        let rng = state.rng;
        let cached = state.route(&topic, &cluster).unwrap();
        assert_eq!(cached.rng, None);
        let mut pressure = state.plan(&topic, cached.partition, &worker, 200, &cluster);
        assert_eq!(pressure.tag.id, 0);
        pressure.next_rng = cached.rng;
        state.commit(&topic, cached.partition, &worker, true, pressure, &cluster);
        assert_eq!(state.rng, rng);
        assert_eq!(state.counts().3, 1);
    }
}
