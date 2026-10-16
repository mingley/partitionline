//! Cluster metadata: brokers and per-partition leaders.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::protocol::api::{MetadataResponse, NodeEndpoint};
use crate::protocol::records::RecordBatch;

/// Largest partition index stored in the dense leader cache.
///
/// Metadata can name a partition index of `i32::MAX` in a one-element array.
/// The cache is indexed by that value, so an unbounded index would allocate
/// billions of slots.
const MAX_CACHED_PARTITIONS: i32 = 1_000_000;

/// Host and port that can be dialed.
///
/// An IPv6 host already wrapped in brackets is unwrapped before formatting,
/// so `[::1]` becomes `[::1]:port` rather than `[[::1]]:port`.
pub(crate) fn dial_address(host: &str, port: i32) -> Option<String> {
    if host.chars().any(|c| c.is_control()) {
        return None;
    }
    let host = host.trim();
    let host = match host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        Some(inner) => inner.trim(),
        None => host,
    };
    if host.is_empty()
        || host
            .chars()
            .any(|c| c.is_whitespace() || c == '[' || c == ']')
        || !(1..=i32::from(u16::MAX)).contains(&port)
    {
        None
    } else {
        Some(crate::net::format_address(host, port))
    }
}

/// A broker that can be dialed. Empty hosts, non-positive ids, and ports
/// outside `1..=65535` are not addresses.
fn usable_broker_address(node_id: i32, host: &str, port: i32) -> Option<String> {
    if node_id < 0 {
        None
    } else {
        dial_address(host, port)
    }
}

/// True when `current` is already the dial address [`dial_address`] would build.
fn stored_address_matches(current: &str, host: &str, port: i32) -> bool {
    if host.chars().any(|c| c.is_control()) {
        return false;
    }
    let host = host.trim();
    let host = match host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        Some(inner) => inner.trim(),
        None => host,
    };
    if host.is_empty()
        || host
            .chars()
            .any(|c| c.is_whitespace() || c == '[' || c == ']')
        || !(1..=i32::from(u16::MAX)).contains(&port)
    {
        return false;
    }
    let (stored_host, stored_port) = if host.contains(':') {
        let Some(rest) = current.strip_prefix('[') else {
            return false;
        };
        let Some((stored_host, stored_port)) = rest.rsplit_once("]:") else {
            return false;
        };
        (stored_host, stored_port)
    } else {
        let Some((stored_host, stored_port)) = current.rsplit_once(':') else {
            return false;
        };
        (stored_host, stored_port)
    };
    stored_host == host && stored_port.parse::<i32>().ok() == Some(port)
}

/// Snapshot of brokers and partition leaders from Metadata.
#[derive(Debug, Clone, Default)]
pub(crate) struct Cluster {
    /// `node_id` → `host:port`.
    pub(crate) brokers: HashMap<i32, String>,
    /// Topic → leader `node_id` by partition index.
    ///
    /// The name is one allocation shared with the id, epoch, and freshness maps.
    pub(crate) leaders: HashMap<Arc<str>, Vec<i32>>,
    /// Nonzero Metadata v10+ topic identity, invalidated with leader metadata.
    pub(crate) topic_ids: HashMap<Arc<str>, [u8; 16]>,
    /// Topic → Metadata `leader_epoch` by partition index.
    pub(crate) leader_epochs: HashMap<Arc<str>, Vec<i32>>,
    /// Metadata `controller_id`, or `None` until the first Metadata response.
    pub(crate) controller_id: Option<i32>,
    /// When each topic's leaders were last applied from Metadata.
    topic_fetched_at: HashMap<Arc<str>, Instant>,
    /// Reused per-partition marks for the topic currently being applied.
    decided_scratch: Vec<bool>,
}

impl Cluster {
    /// Merge a Metadata response into this snapshot.
    ///
    /// `version` is the negotiated Metadata api version. Leader epochs are
    /// retained only when [`MetadataResponse::has_reliable_leader_epochs`].
    /// A reliable response does not replace a cached leader with an older
    /// epoch from a current-leader update.
    pub(crate) fn apply(&mut self, md: &MetadataResponse, version: i16) {
        self.controller_id = (md.controller_id >= 0).then_some(md.controller_id);
        for b in &md.brokers {
            if self
                .brokers
                .get(&b.node_id)
                .is_some_and(|current| stored_address_matches(current, &b.host, b.port))
            {
                continue;
            }
            if let Some(address) = usable_broker_address(b.node_id, &b.host, b.port) {
                let _prev = self.brokers.insert(b.node_id, address);
            }
        }
        let retain_epochs = MetadataResponse::has_reliable_leader_epochs(version);
        // A topic named twice in one response is one image. A later copy must
        // not drop partitions the earlier copy already accepted. One topic
        // cannot repeat, so that response skips the set.
        let lone_topic = md.topics.len() <= 1;
        let mut seen_topics: HashSet<&str> = HashSet::new();
        for t in &md.topics {
            let Some(name) = t.name.as_ref() else {
                continue;
            };
            if t.error_code != 0 {
                // A missing topic must not keep a stale leader. Other topic
                // errors are transient, so the last good leaders stay cached.
                // A later copy must not drop a leader this response already
                // accepted.
                if (lone_topic || !seen_topics.contains(name.as_str()))
                    && (t.error_code == crate::error::UNKNOWN_TOPIC_OR_PARTITION
                        || t.error_code == crate::error::UNKNOWN_TOPIC_ID)
                {
                    self.invalidate_topic(name);
                }
                continue;
            }
            // A refresh that names the same leaders, epochs, and topic id
            // only moves the freshness stamp. Building the image again would
            // rewrite every partition slot. A newer CurrentLeader epoch, a
            // recreated topic id, or a second copy of the topic in this
            // response still takes the full merge below.
            let repeated = !lone_topic && seen_topics.contains(name.as_str());
            if !repeated && self.topic_image_unchanged(name, t, version, retain_epochs) {
                if let Some(slot) = self.topic_fetched_at.get_mut(name.as_str()) {
                    *slot = Instant::now();
                } else {
                    let key = self.topic_arc(name);
                    let _prev = self.topic_fetched_at.insert(key, Instant::now());
                }
                if !lone_topic {
                    let _inserted = seen_topics.insert(name.as_str());
                }
                continue;
            }
            // A new topic id is a deleted-and-recreated topic. Its epochs
            // start over, so the previous generation's leader must not stick.
            let cached_topic_id = self
                .topic_ids
                .get(name.as_str())
                .copied()
                .filter(|id| *id != [0; 16]);
            let incoming_topic_id = (version >= 10 && t.topic_id != [0; 16]).then_some(t.topic_id);
            let recreated = match (cached_topic_id, incoming_topic_id) {
                (Some(cached), Some(incoming)) => cached != incoming,
                _ => false,
            };
            if recreated {
                let _removed = self.leaders.remove(name.as_str());
                let _removed = self.leader_epochs.remove(name.as_str());
                let _removed = self.topic_fetched_at.remove(name.as_str());
            }
            if version >= 10 && t.topic_id != [0; 16] {
                if let Some(slot) = self.topic_ids.get_mut(name.as_str()) {
                    *slot = t.topic_id;
                } else {
                    let key = self.topic_arc(name);
                    let _ = self.topic_ids.insert(key, t.topic_id);
                }
            } else if !repeated {
                let _ = self.topic_ids.remove(name.as_str());
            }
            let mut max_idx = -1i32;
            for p in &t.partitions {
                if (0..MAX_CACHED_PARTITIONS).contains(&p.partition_index)
                    && p.partition_index > max_idx
                {
                    max_idx = p.partition_index;
                }
            }
            if max_idx < 0 {
                // A successful image with no usable partition is the topic's
                // current shape. An earlier copy in this same response already
                // accepted partitions, so an empty later copy must not drop them.
                if !repeated {
                    let _removed = self.leaders.remove(name.as_str());
                    let _removed = self.leader_epochs.remove(name.as_str());
                    let _removed = self.topic_fetched_at.remove(name.as_str());
                }
                continue;
            }
            let new_len = match usize::try_from(max_idx.saturating_add(1)) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let mut leaders = if let Some(existing) = self.leaders.get_mut(name.as_str()) {
                std::mem::take(existing)
            } else {
                Vec::new()
            };
            let mut epochs = if let Some(existing) = self.leader_epochs.get_mut(name.as_str()) {
                std::mem::take(existing)
            } else {
                Vec::new()
            };
            let old_leaders_len = leaders.len();
            let old_epochs_len = epochs.len();
            let len = if repeated {
                old_leaders_len.max(new_len)
            } else {
                new_len
            };
            if leaders.len() < len {
                leaders.resize(len, MetadataResponse::NO_LEADER_ID);
            } else if leaders.len() > len {
                leaders.truncate(len);
            }
            if epochs.len() < len {
                epochs.resize(len, RecordBatch::NO_PARTITION_LEADER_EPOCH);
            } else if epochs.len() > len {
                epochs.truncate(len);
            }
            let mut decided = std::mem::take(&mut self.decided_scratch);
            decided.clear();
            decided.resize(len, false);
            if repeated {
                for slot in decided.iter_mut().take(old_leaders_len.min(len)) {
                    *slot = true;
                }
            }
            for p in &t.partitions {
                let Ok(idx) = usize::try_from(p.partition_index) else {
                    continue;
                };
                let already = decided.get(idx).copied().unwrap_or(false);
                let prev_leader = if idx < old_leaders_len {
                    leaders
                        .get(idx)
                        .copied()
                        .unwrap_or(MetadataResponse::NO_LEADER_ID)
                } else {
                    MetadataResponse::NO_LEADER_ID
                };
                let prev_epoch = if idx < old_epochs_len {
                    epochs
                        .get(idx)
                        .copied()
                        .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH)
                } else {
                    RecordBatch::NO_PARTITION_LEADER_EPOCH
                };
                if p.error_code != 0 {
                    // An unknown partition drops its leader, including one
                    // chosen earlier in this response. A transient error keeps
                    // that choice, or the previous cache when this response
                    // has not chosen yet.
                    let unknown = p.error_code == crate::error::UNKNOWN_TOPIC_OR_PARTITION
                        || p.error_code == crate::error::UNKNOWN_TOPIC_ID;
                    if unknown {
                        if let Some(slot) = leaders.get_mut(idx) {
                            *slot = MetadataResponse::NO_LEADER_ID;
                        }
                        if let Some(slot) = epochs.get_mut(idx) {
                            *slot = RecordBatch::NO_PARTITION_LEADER_EPOCH;
                        }
                    } else if !already {
                        if let Some(slot) = leaders.get_mut(idx) {
                            *slot = prev_leader;
                        }
                        if let Some(slot) = epochs.get_mut(idx) {
                            *slot = prev_epoch;
                        }
                    }
                    if let Some(slot) = decided.get_mut(idx) {
                        *slot = true;
                    }
                    continue;
                }
                let in_response = epochs
                    .get(idx)
                    .copied()
                    .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH);
                // A repeated partition in this response is part of the cache
                // being built. An older copy must not replace the newer one.
                let cached_epoch = if already { in_response } else { prev_epoch };
                let keep_cached = retain_epochs
                    && cached_epoch != RecordBatch::NO_PARTITION_LEADER_EPOCH
                    && p.leader_epoch < cached_epoch;
                if keep_cached {
                    if !already {
                        if let Some(slot) = leaders.get_mut(idx) {
                            *slot = prev_leader;
                        }
                        if let Some(slot) = epochs.get_mut(idx) {
                            *slot = cached_epoch;
                        }
                    }
                    if let Some(slot) = decided.get_mut(idx) {
                        *slot = true;
                    }
                    continue;
                }
                if let Some(slot) = leaders.get_mut(idx) {
                    *slot = p.leader_id;
                }
                if let Some(slot) = epochs.get_mut(idx) {
                    *slot = if retain_epochs {
                        p.leader_epoch
                    } else {
                        RecordBatch::NO_PARTITION_LEADER_EPOCH
                    };
                }
                if let Some(slot) = decided.get_mut(idx) {
                    *slot = true;
                }
            }
            for idx in 0..len {
                if decided.get(idx).copied().unwrap_or(false) {
                    continue;
                }
                if let Some(slot) = leaders.get_mut(idx) {
                    *slot = MetadataResponse::NO_LEADER_ID;
                }
                if let Some(slot) = epochs.get_mut(idx) {
                    *slot = RecordBatch::NO_PARTITION_LEADER_EPOCH;
                }
            }
            if let Some(slot) = self.leaders.get_mut(name.as_str()) {
                *slot = leaders;
            } else {
                let key = self.topic_arc(name);
                let _prev = self.leaders.insert(key, leaders);
            }
            if let Some(slot) = self.leader_epochs.get_mut(name.as_str()) {
                *slot = epochs;
            } else {
                let key = self.topic_arc(name);
                let _prev = self.leader_epochs.insert(key, epochs);
            }
            if let Some(slot) = self.topic_fetched_at.get_mut(name.as_str()) {
                *slot = Instant::now();
            } else {
                let key = self.topic_arc(name);
                let _prev = self.topic_fetched_at.insert(key, Instant::now());
            }
            if !lone_topic {
                let _inserted = seen_topics.insert(name.as_str());
            }
            self.decided_scratch = decided;
        }
    }

    /// True when applying `topic` would leave leaders, epochs, and the topic
    /// id as they are.
    ///
    /// The caller still refreshes [`Self::topic_fetched_at`]. Partitions must
    /// be listed in index order with no holes: a gap, a duplicate, or an
    /// error falls through to the full merge. An epoch newer than this image
    /// (from [`Self::apply_current_leader`]) does not match.
    fn topic_image_unchanged(
        &self,
        name: &str,
        topic: &crate::protocol::api::TopicMetadata,
        version: i16,
        retain_epochs: bool,
    ) -> bool {
        let id_stable = if version >= 10 && topic.topic_id != [0; 16] {
            self.topic_ids.get(name).copied() == Some(topic.topic_id)
        } else {
            !self.topic_ids.contains_key(name)
        };
        if !id_stable {
            return false;
        }
        let Some(leaders) = self.leaders.get(name) else {
            return false;
        };
        let Some(epochs) = self.leader_epochs.get(name) else {
            return false;
        };
        if leaders.is_empty()
            || leaders.len() != epochs.len()
            || leaders.len() != topic.partitions.len()
        {
            return false;
        }
        topic.partitions.iter().enumerate().all(|(idx, partition)| {
            let Ok(index) = i32::try_from(idx) else {
                return false;
            };
            if partition.error_code != 0
                || partition.partition_index != index
                || !(0..MAX_CACHED_PARTITIONS).contains(&partition.partition_index)
            {
                return false;
            }
            let expected_epoch = if retain_epochs {
                partition.leader_epoch
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
            leaders.get(idx).copied() == Some(partition.leader_id)
                && epochs.get(idx).copied() == Some(expected_epoch)
        })
    }

    /// The allocation already stored for `name`, or a new one.
    fn topic_arc(&self, name: &str) -> Arc<str> {
        let existing = self
            .leaders
            .get_key_value(name)
            .map(|(key, _)| key)
            .or_else(|| self.topic_ids.get_key_value(name).map(|(key, _)| key))
            .or_else(|| self.leader_epochs.get_key_value(name).map(|(key, _)| key))
            .or_else(|| {
                self.topic_fetched_at
                    .get_key_value(name)
                    .map(|(key, _)| key)
            });
        existing.map(Arc::clone).unwrap_or_else(|| Arc::from(name))
    }

    /// Drop cached leaders for `topic` so the next lookup refetches Metadata.
    pub(crate) fn invalidate_topic(&mut self, topic: &str) {
        let _removed = self.topic_ids.remove(topic);
        let _removed = self.leaders.remove(topic);
        let _removed = self.leader_epochs.remove(topic);
        let _removed = self.topic_fetched_at.remove(topic);
    }

    /// When `topic` leaders were last applied, if Metadata has been seen.
    pub(crate) fn topic_fetched_at(&self, topic: &str) -> Option<Instant> {
        self.topic_fetched_at.get(topic).copied()
    }

    /// True when `topic` has Metadata newer than `max_age`.
    ///
    /// A zero `max_age` is always stale (refresh on every lookup).
    pub(crate) fn topic_fresh(&self, topic: &str, max_age: Duration) -> bool {
        if max_age.is_zero() {
            return false;
        }
        self.topic_fetched_at
            .get(topic)
            .is_some_and(|at| at.elapsed() < max_age)
    }

    /// Drop the cached controller so the next admin RPC refetches Metadata.
    pub(crate) fn invalidate_controller(&mut self) {
        self.controller_id = None;
    }

    /// Last Metadata `controller_id`.
    pub(crate) fn controller(&self) -> Result<i32> {
        let node = self
            .controller_id
            .filter(|&id| id >= 0)
            .ok_or_else(|| Error::protocol("no controller"))?;
        if !self.brokers.contains_key(&node) {
            return Err(Error::protocol(format!("unknown controller {node}")));
        }
        Ok(node)
    }

    /// Last Metadata `leader_epoch` for `topic`/`partition`, or
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub(crate) fn leader_epoch(&self, topic: &str, partition: i32) -> i32 {
        partition_epoch(self.leader_epochs_of(topic), partition)
    }

    /// Store `epoch` when it is unknown or at least the cached leader epoch.
    ///
    /// OffsetForLeaderEpoch can report the epoch of an older offset. That
    /// value must not replace the current leader epoch.
    pub(crate) fn set_leader_epoch(&mut self, topic: &str, partition: i32, epoch: i32) {
        if !(0..MAX_CACHED_PARTITIONS).contains(&partition) {
            return;
        }
        let Ok(idx) = usize::try_from(partition) else {
            return;
        };
        if let Some(v) = self.leader_epochs.get_mut(topic) {
            if v.len() <= idx {
                v.resize(
                    idx.saturating_add(1),
                    RecordBatch::NO_PARTITION_LEADER_EPOCH,
                );
            }
            if let Some(slot) = v.get_mut(idx) {
                if *slot == RecordBatch::NO_PARTITION_LEADER_EPOCH || epoch >= *slot {
                    *slot = epoch;
                }
            }
            return;
        }
        let mut v = vec![RecordBatch::NO_PARTITION_LEADER_EPOCH; idx.saturating_add(1)];
        if let Some(slot) = v.get_mut(idx) {
            *slot = epoch;
        }
        let key = self.topic_arc(topic);
        let _prev = self.leader_epochs.insert(key, v);
    }

    /// Insert Produce v10+ / Fetch v16+ NodeEndpoints into the broker map.
    ///
    /// Call this before [`Self::apply_current_leader`] so an unknown
    /// CurrentLeader id can patch the partition cache (KIP-951).
    pub(crate) fn apply_node_endpoints(&mut self, endpoints: &[NodeEndpoint]) {
        for e in endpoints {
            if self
                .brokers
                .get(&e.node_id)
                .is_some_and(|current| stored_address_matches(current, &e.host, e.port))
            {
                continue;
            }
            if let Some(address) = usable_broker_address(e.node_id, &e.host, e.port) {
                let _prev = self.brokers.insert(e.node_id, address);
            }
        }
    }

    /// Apply Produce v10+ / Fetch v12+ CurrentLeader when `leader_id` is a
    /// known broker.
    ///
    /// Unknown brokers need [`Self::apply_node_endpoints`] first. An epoch
    /// older than the cached leader epoch is ignored. Returns `true` when the
    /// partition leader cache was updated.
    pub(crate) fn apply_current_leader(
        &mut self,
        topic: &str,
        partition: i32,
        leader_id: i32,
        leader_epoch: i32,
    ) -> bool {
        if leader_id < 0 || !self.brokers.contains_key(&leader_id) {
            return false;
        }
        if !(0..MAX_CACHED_PARTITIONS).contains(&partition) {
            return false;
        }
        let known = self.leader_epoch(topic, partition);
        if known != RecordBatch::NO_PARTITION_LEADER_EPOCH && leader_epoch < known {
            return false;
        }
        let Ok(idx) = usize::try_from(partition) else {
            return false;
        };
        if !self.leaders.contains_key(topic) {
            let key = self.topic_arc(topic);
            let _ = self.leaders.insert(key, Vec::new());
        }
        if let Some(leaders) = self.leaders.get_mut(topic) {
            if leaders.len() <= idx {
                leaders.resize(idx.saturating_add(1), MetadataResponse::NO_LEADER_ID);
            }
            if let Some(slot) = leaders.get_mut(idx) {
                *slot = leader_id;
            }
        }
        self.set_leader_epoch(topic, partition, leader_epoch);
        if let Some(fetched_at) = self.topic_fetched_at.get_mut(topic) {
            *fetched_at = Instant::now();
        } else {
            let key = self.topic_arc(topic);
            let _prev = self.topic_fetched_at.insert(key, Instant::now());
        }
        true
    }

    /// Partition count from the last Metadata that listed `topic`.
    pub(crate) fn partition_count(&self, topic: &str) -> Option<i32> {
        self.leaders
            .get(topic)
            .map(|v| i32::try_from(v.len()).unwrap_or(i32::MAX))
    }

    /// Leader node ids for `topic`, or an empty slice when Metadata has not listed it.
    pub(crate) fn leader_ids(&self, topic: &str) -> &[i32] {
        self.leaders.get(topic).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Leader epochs for `topic`, or an empty slice when none are cached.
    pub(crate) fn leader_epochs_of(&self, topic: &str) -> &[i32] {
        self.leader_epochs
            .get(topic)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Leader node id and `host:port` for `topic`/`partition`.
    pub(crate) fn leader_id(&self, topic: &str, partition: i32) -> Result<i32> {
        let parts = self
            .leaders
            .get(topic)
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
        let idx = usize::try_from(partition).map_err(|_| Error::NoLeader {
            topic: topic.to_string(),
            partition,
        })?;
        let node = parts
            .get(idx)
            .copied()
            .filter(|&id| id >= 0)
            .ok_or_else(|| Error::NoLeader {
                topic: topic.to_string(),
                partition,
            })?;
        if self.brokers.contains_key(&node) {
            Ok(node)
        } else {
            Err(Error::NoLeader {
                topic: topic.to_string(),
                partition,
            })
        }
    }

    pub(crate) fn leader(&self, topic: &str, partition: i32) -> Result<(i32, String)> {
        let node = self.leader_id(topic, partition)?;
        let addr = self
            .brokers
            .get(&node)
            .cloned()
            .ok_or_else(|| Error::NoLeader {
                topic: topic.to_string(),
                partition,
            })?;
        Ok((node, addr))
    }
}

/// Leader epoch at `partition`, or the unknown-epoch sentinel.
pub(crate) fn partition_epoch(epochs: &[i32], partition: i32) -> i32 {
    let Ok(idx) = usize::try_from(partition) else {
        return RecordBatch::NO_PARTITION_LEADER_EPOCH;
    };
    epochs
        .get(idx)
        .copied()
        .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH)
}

/// Leader node ids for the topic currently being routed.
///
/// A run of partitions for one topic copies that leader list once. The next
/// topic replaces it. Unknown topics, negative leaders, and leaders with no
/// cached address match [`Cluster::leader_id`].
pub(crate) struct LeaderList {
    topic: String,
    leaders: Vec<i32>,
    present: bool,
}

impl LeaderList {
    pub(crate) fn new() -> Self {
        Self {
            topic: String::new(),
            leaders: Vec::new(),
            present: false,
        }
    }

    pub(crate) fn node(&mut self, cluster: &Cluster, topic: &str, partition: i32) -> Result<i32> {
        if self.topic != topic {
            self.leaders.clear();
            self.present = false;
            if let Some(leaders) = cluster.leaders.get(topic) {
                self.leaders.clone_from(leaders);
                self.present = true;
            }
            self.topic.clear();
            self.topic.push_str(topic);
        }
        if !self.present {
            return Err(Error::UnknownTopic(topic.to_string()));
        }
        let idx = usize::try_from(partition).map_err(|_| Error::NoLeader {
            topic: topic.to_string(),
            partition,
        })?;
        let node = self
            .leaders
            .get(idx)
            .copied()
            .filter(|&id| id >= 0)
            .ok_or_else(|| Error::NoLeader {
                topic: topic.to_string(),
                partition,
            })?;
        if cluster.brokers.contains_key(&node) {
            Ok(node)
        } else {
            Err(Error::NoLeader {
                topic: topic.to_string(),
                partition,
            })
        }
    }
}

/// Leader epochs for the topic currently being read.
///
/// A run of partitions for one topic copies that epoch list once. The next
/// topic replaces it. A missing topic leaves the unknown-epoch sentinel.
pub(crate) struct EpochList {
    topic: String,
    epochs: Vec<i32>,
}

impl EpochList {
    pub(crate) fn new() -> Self {
        Self {
            topic: String::new(),
            epochs: Vec::new(),
        }
    }

    pub(crate) fn epoch(&mut self, cluster: &Cluster, topic: &str, partition: i32) -> i32 {
        if self.topic != topic {
            self.epochs.clear();
            self.epochs
                .extend_from_slice(cluster.leader_epochs_of(topic));
            self.topic.clear();
            self.topic.push_str(topic);
        }
        partition_epoch(&self.epochs, partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error;
    use crate::protocol::api::{Broker, MetadataResponse};

    #[test]
    fn a_cached_topic_is_one_allocation() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let name = "topic-name-longer-than-twenty-three";
        let mut topic_id = [0u8; 16];
        topic_id[0] = 7;
        let topic = TopicMetadata {
            error_code: 0,
            name: Some(name.into()),
            topic_id,
            is_internal: false,
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 3,
                replica_nodes: vec![1],
                isr_nodes: vec![1],
                offline_replicas: Vec::new(),
            }],
            topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        };
        let mut cluster = Cluster::default();
        cluster.apply(
            &MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                topics: vec![topic],
                cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                error_code: 0,
            },
            13,
        );
        let stored = cluster
            .leaders
            .get_key_value(name)
            .map(|(key, _)| Arc::as_ptr(key));
        assert!(stored.is_some());
        assert_eq!(
            cluster
                .topic_ids
                .get_key_value(name)
                .map(|(key, _)| Arc::as_ptr(key)),
            stored
        );
        assert_eq!(
            cluster
                .leader_epochs
                .get_key_value(name)
                .map(|(key, _)| Arc::as_ptr(key)),
            stored
        );
        assert_eq!(
            cluster
                .topic_fetched_at
                .get_key_value(name)
                .map(|(key, _)| Arc::as_ptr(key)),
            stored
        );
        assert!(cluster.apply_current_leader(name, 0, 1, 4));
        assert_eq!(
            cluster
                .leaders
                .get_key_value(name)
                .map(|(key, _)| Arc::as_ptr(key)),
            stored
        );
        assert_eq!(cluster.leader_epoch(name, 0), 4);
    }

    #[test]
    fn a_leader_list_matches_a_direct_lookup() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let topic = |name: &str, leader: i32| TopicMetadata {
            error_code: 0,
            name: Some(name.into()),
            topic_id: [0u8; 16],
            is_internal: false,
            partitions: vec![
                PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: leader,
                    leader_epoch: 1,
                    replica_nodes: vec![leader],
                    isr_nodes: vec![leader],
                    offline_replicas: Vec::new(),
                },
                PartitionMetadata {
                    error_code: 0,
                    partition_index: 1,
                    leader_id: leader,
                    leader_epoch: 1,
                    replica_nodes: vec![leader],
                    isr_nodes: vec![leader],
                    offline_replicas: Vec::new(),
                },
            ],
            topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        };
        let mut cluster = Cluster::default();
        cluster.apply(
            &MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: Some("mock".into()),
                controller_id: 1,
                topics: vec![topic("orders", 1), topic("payments", 1)],
                cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                error_code: 0,
            },
            13,
        );
        let mut leaders = LeaderList::new();
        assert_eq!(leaders.node(&cluster, "orders", 0).unwrap(), 1);
        assert_eq!(
            leaders.node(&cluster, "orders", 1).unwrap(),
            cluster.leader_id("orders", 1).unwrap()
        );
        assert_eq!(leaders.node(&cluster, "payments", 0).unwrap(), 1);
        assert!(leaders.node(&cluster, "missing", 0).is_err());
        assert!(cluster.leader_id("missing", 0).is_err());
    }

    #[test]
    fn an_epoch_list_loads_each_topic_once() {
        let mut cluster = Cluster::default();
        cluster.set_leader_epoch("orders", 0, 4);
        cluster.set_leader_epoch("orders", 1, 5);
        cluster.set_leader_epoch("payments", 0, 9);
        let mut epochs = EpochList::new();
        assert_eq!(epochs.epoch(&cluster, "orders", 0), 4);
        assert_eq!(epochs.epoch(&cluster, "orders", 1), 5);
        assert_eq!(
            epochs.epoch(&cluster, "orders", 3),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(epochs.epoch(&cluster, "payments", 0), 9);
        assert_eq!(
            epochs.epoch(&cluster, "missing", 0),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(epochs.epoch(&cluster, "orders", 1), 5);
    }

    #[test]
    fn apply_stores_controller_id() {
        let mut cluster = Cluster::default();
        assert!(cluster.controller().is_err());
        cluster.apply(
            &MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: Some("mock".into()),
                controller_id: 2,
                topics: Vec::new(),
                cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                error_code: 0,
            },
            13,
        );
        assert_eq!(cluster.controller().unwrap(), 2);
        cluster.invalidate_controller();
        assert!(cluster.controller().is_err());
    }

    #[test]
    fn an_undialable_broker_does_not_replace_a_reachable_address() {
        let metadata = |brokers: Vec<Broker>| MetadataResponse {
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: Vec::new(),
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let broker = |node_id: i32, host: &str, port: i32| Broker {
            node_id,
            host: host.into(),
            port,
            rack: None,
        };
        let mut cluster = Cluster::default();
        cluster.apply(&metadata(vec![broker(1, "127.0.0.1", 9092)]), 13);
        cluster.apply(
            &metadata(vec![
                broker(1, "", 9092),
                broker(1, "   ", 9092),
                broker(1, "127.0.0.1", 0),
                broker(1, "127.0.0.1", 70_000),
                broker(-1, "127.0.0.1", 9092),
            ]),
            13,
        );
        assert_eq!(
            cluster.brokers.get(&1).map(String::as_str),
            Some("127.0.0.1:9092")
        );
        let kept = cluster
            .brokers
            .get(&1)
            .map(|address| address.as_ptr())
            .unwrap_or(std::ptr::null());
        cluster.apply(&metadata(vec![broker(1, "127.0.0.1", 9092)]), 13);
        cluster.apply(&metadata(vec![broker(1, "[127.0.0.1]", 9092)]), 13);
        cluster.apply(&metadata(vec![broker(1, " 127.0.0.1 ", 9092)]), 13);
        assert_eq!(
            cluster.brokers.get(&1).map(|address| address.as_ptr()),
            Some(kept)
        );
        assert!(!cluster.brokers.contains_key(&-1));
        cluster.apply_node_endpoints(&[
            NodeEndpoint::new(-1, "127.0.0.1", 9092, None),
            NodeEndpoint::new(1, "", 9094, None),
            NodeEndpoint::new(1, "127.0.0.1", 70_000, None),
        ]);
        assert_eq!(
            cluster.brokers.get(&1).map(String::as_str),
            Some("127.0.0.1:9092")
        );
        cluster.apply_node_endpoints(&[NodeEndpoint::new(1, " 10.0.0.2 ", 9093, None)]);
        assert_eq!(
            cluster.brokers.get(&1).map(String::as_str),
            Some("10.0.0.2:9093")
        );
        cluster.apply(&metadata(vec![broker(4, "[::1]", 9092)]), 13);
        assert_eq!(
            cluster.brokers.get(&4).map(String::as_str),
            Some("[::1]:9092")
        );
        let ipv6 = cluster
            .brokers
            .get(&4)
            .map(|address| address.as_ptr())
            .unwrap_or(std::ptr::null());
        cluster.apply(&metadata(vec![broker(4, "::1", 9092)]), 13);
        cluster.apply_node_endpoints(&[NodeEndpoint::new(4, "[::1]", 9092, None)]);
        assert_eq!(
            cluster.brokers.get(&4).map(|address| address.as_ptr()),
            Some(ipv6)
        );
        cluster.apply_node_endpoints(&[NodeEndpoint::new(5, "[2001:db8::1]", 9092, None)]);
        assert_eq!(
            cluster.brokers.get(&5).map(String::as_str),
            Some("[2001:db8::1]:9092")
        );
        cluster.apply(&metadata(vec![broker(6, "[]", 9092)]), 13);
        assert!(!cluster.brokers.contains_key(&6));
        assert_eq!(dial_address("[::1]", 9092).as_deref(), Some("[::1]:9092"));
        assert_eq!(
            dial_address("  [::1]  ", 9092).as_deref(),
            Some("[::1]:9092")
        );
        assert_eq!(dial_address("::1", 9092).as_deref(), Some("[::1]:9092"));
        assert_eq!(
            dial_address("coordinator.example", 9092).as_deref(),
            Some("coordinator.example:9092")
        );
        assert!(dial_address("[]", 9092).is_none());
        assert!(dial_address("::1", 0).is_none());
        assert!(dial_address("", 9092).is_none());
        assert!(dial_address("bad host", 9092).is_none());
        assert!(dial_address("broker\n.example", 9092).is_none());
        assert!(dial_address("[::1\n]", 9092).is_none());
        assert!(dial_address("[[::1]]", 9092).is_none());
        assert!(dial_address("[::1", 9092).is_none());
        cluster.apply(&metadata(vec![broker(1, "bad host", 9092)]), 13);
        assert_eq!(
            cluster.brokers.get(&1).map(String::as_str),
            Some("10.0.0.2:9093")
        );
    }

    #[test]
    fn not_controller_is_retriable() {
        assert_eq!(error::NOT_CONTROLLER, 41);
        assert!(Error::broker(error::NOT_CONTROLLER, "CreateTopics").is_retriable());
        assert_eq!(
            error::error_name(error::NOT_CONTROLLER),
            Some("NOT_CONTROLLER")
        );
    }

    #[test]
    fn topic_fresh_respects_max_age() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};
        use std::time::Duration;

        let mut cluster = Cluster::default();
        assert!(!cluster.topic_fresh("t", Duration::from_secs(5)));
        cluster.apply(
            &MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: Some("mock".into()),
                controller_id: 1,
                topics: vec![TopicMetadata {
                    error_code: 0,
                    name: Some("t".into()),
                    topic_id: [0u8; 16],
                    is_internal: false,
                    partitions: vec![PartitionMetadata {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 0,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: Vec::new(),
                    }],
                    topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                }],
                cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                error_code: 0,
            },
            13,
        );
        assert!(cluster.topic_fresh("t", Duration::from_secs(5)));
        assert!(cluster.topic_fetched_at("t").is_some());
        assert!(
            !cluster.topic_fresh("t", Duration::ZERO),
            "zero max.age must refresh every lookup"
        );
        cluster.invalidate_topic("t");
        assert!(!cluster.topic_fresh("t", Duration::from_secs(5)));
    }

    #[test]
    fn an_unchanged_metadata_image_refreshes_without_dropping_a_newer_epoch() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};
        use std::time::Duration;

        let image = |epoch: i32, leader: i32| MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: None,
                },
            ],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [9u8; 16],
                is_internal: false,
                partitions: vec![
                    PartitionMetadata {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: leader,
                        leader_epoch: epoch,
                        replica_nodes: vec![leader],
                        isr_nodes: vec![leader],
                        offline_replicas: Vec::new(),
                    },
                    PartitionMetadata {
                        error_code: 0,
                        partition_index: 1,
                        leader_id: leader,
                        leader_epoch: epoch,
                        replica_nodes: vec![leader],
                        isr_nodes: vec![leader],
                        offline_replicas: Vec::new(),
                    },
                ],
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let mut cluster = Cluster::default();
        let md = image(4, 1);
        cluster.apply(&md, 13);
        let stamped = cluster
            .topic_fetched_at
            .get_mut("t")
            .expect("first apply stamps the topic");
        *stamped = Instant::now() - Duration::from_secs(60);
        assert!(!cluster.topic_fresh("t", Duration::from_secs(30)));
        cluster.apply(&md, 13);
        assert!(cluster.topic_fresh("t", Duration::from_secs(30)));
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader("t", 1).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 4);
        assert_eq!(cluster.leader_epoch("t", 1), 4);
        assert_eq!(cluster.topic_ids.get("t").copied(), Some([9u8; 16]));
        assert!(cluster.apply_current_leader("t", 0, 2, 9));
        cluster.apply(&md, 13);
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 0), 9);
        assert_eq!(cluster.leader("t", 1).unwrap().0, 1);
        assert!(cluster.apply_current_leader("t", 4, 1, 11));
        assert_eq!(cluster.partition_count("t"), Some(5));
        cluster.apply(&md, 13);
        assert_eq!(cluster.partition_count("t"), Some(2));
        assert!(cluster.leader("t", 4).is_err());
        cluster.apply(&image(10, 2), 13);
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 0), 10);
        assert_eq!(cluster.leader_epoch("t", 1), 10);
    }

    #[test]
    fn a_repeated_partition_keeps_the_newer_leader_epoch() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let response = |first_epoch: i32, second_epoch: i32| MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: None,
                },
            ],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: vec![
                    PartitionMetadata {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: first_epoch,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: Vec::new(),
                    },
                    PartitionMetadata {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 2,
                        leader_epoch: second_epoch,
                        replica_nodes: vec![2],
                        isr_nodes: vec![2],
                        offline_replicas: Vec::new(),
                    },
                ],
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let mut older_second = Cluster::default();
        older_second.apply(&response(5, 3), 13);
        assert_eq!(older_second.leader("t", 0).unwrap().0, 1);
        assert_eq!(older_second.leader_epoch("t", 0), 5);
        let mut newer_second = Cluster::default();
        newer_second.apply(&response(3, 5), 13);
        assert_eq!(newer_second.leader("t", 0).unwrap().0, 2);
        assert_eq!(newer_second.leader_epoch("t", 0), 5);
    }

    #[test]
    fn a_later_unknown_topic_copy_does_not_drop_the_accepted_leader() {
        use crate::error;
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let topic = |error_code: i16, leader: i32| TopicMetadata {
            error_code,
            name: Some("t".into()),
            topic_id: [1u8; 16],
            is_internal: false,
            partitions: if error_code == 0 {
                vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: leader,
                    leader_epoch: 4,
                    replica_nodes: vec![leader],
                    isr_nodes: vec![leader],
                    offline_replicas: Vec::new(),
                }]
            } else {
                Vec::new()
            },
            topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        };
        let response = |topics: Vec<TopicMetadata>| MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![Broker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics,
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let mut cluster = Cluster::default();
        cluster.apply(
            &response(vec![
                topic(0, 1),
                topic(error::UNKNOWN_TOPIC_OR_PARTITION, 0),
            ]),
            13,
        );
        assert_eq!(
            cluster.leader_id("t", 0).unwrap(),
            cluster.leader("t", 0).unwrap().0
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 4);
        assert_eq!(cluster.topic_ids.get("t").copied(), Some([1u8; 16]));
        let mut recovered = Cluster::default();
        recovered.apply(
            &response(vec![topic(error::UNKNOWN_TOPIC_ID, 0), topic(0, 1)]),
            13,
        );
        assert_eq!(recovered.leader("t", 0).unwrap().0, 1);
    }

    #[test]
    fn a_later_transient_error_keeps_the_leader_chosen_in_this_response() {
        use crate::error;
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let response = |parts: Vec<PartitionMetadata>| MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: None,
                },
            ],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: parts,
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let partition = |leader: i32, epoch: i32, error_code: i16| PartitionMetadata {
            error_code,
            partition_index: 0,
            leader_id: leader,
            leader_epoch: epoch,
            replica_nodes: vec![leader],
            isr_nodes: vec![leader],
            offline_replicas: Vec::new(),
        };
        let mut cluster = Cluster::default();
        cluster.apply(&response(vec![partition(1, 4, 0)]), 13);
        cluster.apply(
            &response(vec![
                partition(2, 5, 0),
                partition(-1, -1, error::LEADER_NOT_AVAILABLE),
            ]),
            13,
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 0), 5);
        cluster.apply(
            &response(vec![
                partition(2, 6, 0),
                partition(-1, -1, error::UNKNOWN_TOPIC_OR_PARTITION),
            ]),
            13,
        );
        assert!(cluster.leader("t", 0).is_err());
    }

    #[test]
    fn an_empty_partition_list_drops_the_cached_leader() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let mut cluster = Cluster::default();
        let leader = PartitionMetadata {
            error_code: 0,
            partition_index: 0,
            leader_id: 1,
            leader_epoch: 4,
            replica_nodes: vec![1],
            isr_nodes: vec![1],
            offline_replicas: Vec::new(),
        };
        let mut image = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![Broker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [1u8; 16],
                is_internal: false,
                partitions: vec![leader.clone()],
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        cluster.apply(&image, 13);
        assert_eq!(cluster.leader("t", 0).ok().map(|leader| leader.0), Some(1));
        image.topics[0].partitions.clear();
        cluster.apply(&image, 13);
        assert!(cluster.leader("t", 0).is_err());
        assert!(cluster.leaders.get("t").is_none());
        image.topics[0].partitions.push(leader);
        image.topics.push(TopicMetadata {
            error_code: 0,
            name: Some("t".into()),
            topic_id: [1u8; 16],
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        });
        cluster.apply(&image, 13);
        assert_eq!(cluster.leader("t", 0).ok().map(|leader| leader.0), Some(1));
    }

    #[test]
    fn an_absurd_partition_index_does_not_grow_the_leader_cache() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let mut cluster = Cluster::default();
        cluster.apply(
            &MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: Some("mock".into()),
                controller_id: 1,
                topics: vec![TopicMetadata {
                    error_code: 0,
                    name: Some("t".into()),
                    topic_id: [0u8; 16],
                    is_internal: false,
                    partitions: vec![
                        PartitionMetadata {
                            error_code: 0,
                            partition_index: 0,
                            leader_id: 1,
                            leader_epoch: 4,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                            offline_replicas: Vec::new(),
                        },
                        PartitionMetadata {
                            error_code: 0,
                            partition_index: i32::MAX,
                            leader_id: 1,
                            leader_epoch: 9,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                            offline_replicas: Vec::new(),
                        },
                    ],
                    topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                }],
                cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                error_code: 0,
            },
            13,
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 4);
        assert!(cluster.leader("t", i32::MAX).is_err());
        assert_eq!(cluster.leader_epochs.get("t").map(Vec::len), Some(1));
        cluster.set_leader_epoch("t", i32::MAX, 3);
        assert_eq!(cluster.leader_epochs.get("t").map(Vec::len), Some(1));
        assert!(!cluster.apply_current_leader("t", i32::MAX, 1, 5));
        assert_eq!(cluster.leaders.get("t").map(Vec::len), Some(1));
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
    }

    #[test]
    fn apply_current_leader_updates_known_broker() {
        use crate::protocol::api::{NodeEndpoint, PartitionMetadata, TopicMetadata};

        let mut cluster = Cluster::default();
        cluster.apply(
            &MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![
                    Broker {
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: 9092,
                        rack: None,
                    },
                    Broker {
                        node_id: 2,
                        host: "127.0.0.1".into(),
                        port: 9093,
                        rack: None,
                    },
                ],
                cluster_id: Some("mock".into()),
                controller_id: 1,
                topics: vec![TopicMetadata {
                    error_code: 0,
                    name: Some("t".into()),
                    topic_id: [0u8; 16],
                    is_internal: false,
                    partitions: vec![PartitionMetadata {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 0,
                        replica_nodes: vec![1, 2],
                        isr_nodes: vec![1, 2],
                        offline_replicas: Vec::new(),
                    }],
                    topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                }],
                cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                error_code: 0,
            },
            13,
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 0);
        assert!(cluster.apply_current_leader("t", 0, 2, 7));
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 0), 7);
        assert!(
            !cluster.apply_current_leader("t", 0, 99, 8),
            "unknown broker must not patch without NodeEndpoints"
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        cluster.apply_node_endpoints(&[NodeEndpoint {
            node_id: 99,
            host: "127.0.0.1".into(),
            port: 9094,
            rack: None,
        }]);
        assert!(cluster.apply_current_leader("t", 0, 99, 8));
        assert_eq!(
            cluster.leader("t", 0).unwrap(),
            (99, "127.0.0.1:9094".into())
        );
        assert_eq!(cluster.leader_epoch("t", 0), 8);
        assert!(!cluster.apply_current_leader("t", 0, MetadataResponse::NO_LEADER_ID, 8));
        assert_eq!(cluster.leader("t", 0).unwrap().0, 99);
        assert!(
            !cluster.apply_current_leader("t", 0, 1, 3),
            "an older leader epoch must not replace the cached leader"
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 99);
        assert_eq!(cluster.leader_epoch("t", 0), 8);
        assert!(cluster.apply_current_leader("t", 0, 1, 8));
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 8);
    }

    #[test]
    fn metadata_does_not_roll_a_newer_leader_epoch_backward() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let metadata = |leader: i32, epoch: i32| MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: None,
                },
            ],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: leader,
                    leader_epoch: epoch,
                    replica_nodes: vec![1, 2],
                    isr_nodes: vec![1, 2],
                    offline_replicas: Vec::new(),
                }],
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let mut cluster = Cluster::default();
        cluster.apply(&metadata(1, 4), 13);
        assert!(cluster.apply_current_leader("t", 0, 2, 9));
        cluster.apply(&metadata(1, 4), 13);
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 0), 9);
        cluster.apply(&metadata(1, 10), 13);
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 10);
    }

    #[test]
    fn a_later_topic_copy_keeps_partitions_from_the_first_image() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let partition = |index: i32, leader: i32, epoch: i32| PartitionMetadata {
            error_code: 0,
            partition_index: index,
            leader_id: leader,
            leader_epoch: epoch,
            replica_nodes: vec![leader],
            isr_nodes: vec![leader],
            offline_replicas: Vec::new(),
        };
        let topic = |topic_id: [u8; 16], parts: Vec<PartitionMetadata>| TopicMetadata {
            error_code: 0,
            name: Some("t".into()),
            topic_id,
            is_internal: false,
            partitions: parts,
            topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        };
        let response = |topics: Vec<TopicMetadata>| MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: None,
                },
            ],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics,
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let mut cluster = Cluster::default();
        cluster.apply(
            &response(vec![
                topic([1u8; 16], vec![partition(0, 1, 5), partition(1, 1, 5)]),
                topic([1u8; 16], vec![partition(0, 2, 3)]),
            ]),
            13,
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 5);
        assert_eq!(cluster.leader("t", 1).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 1), 5);
        assert_eq!(cluster.topic_ids.get("t").copied(), Some([1u8; 16]));
        cluster.apply(
            &response(vec![
                topic([1u8; 16], vec![partition(0, 1, 3), partition(1, 1, 3)]),
                topic([0u8; 16], vec![partition(0, 2, 6)]),
            ]),
            13,
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 0), 6);
        assert_eq!(cluster.leader("t", 1).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 1), 5);
        assert_eq!(cluster.topic_ids.get("t").copied(), Some([1u8; 16]));
        cluster.apply(
            &response(vec![topic([1u8; 16], vec![partition(0, 2, 7)])]),
            13,
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 0), 7);
        assert!(cluster.leader("t", 1).is_err());
    }

    #[test]
    fn a_recreated_topic_replaces_its_cached_leader() {
        use crate::error;
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let response = |topic_id: [u8; 16], parts: Vec<PartitionMetadata>| MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: None,
                },
            ],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("t".into()),
                topic_id,
                is_internal: false,
                partitions: parts,
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let partition = |index: i32, leader: i32, epoch: i32, error_code: i16| PartitionMetadata {
            error_code,
            partition_index: index,
            leader_id: leader,
            leader_epoch: epoch,
            replica_nodes: vec![1, 2],
            isr_nodes: vec![1, 2],
            offline_replicas: Vec::new(),
        };
        let mut cluster = Cluster::default();
        cluster.apply(
            &response(
                [1u8; 16],
                vec![partition(0, 1, 5, 0), partition(1, 1, 5, 0)],
            ),
            13,
        );
        cluster.apply(&response([1u8; 16], vec![partition(0, 2, 1, 0)]), 13);
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 5);
        assert_eq!(cluster.topic_ids.get("t").copied(), Some([1u8; 16]));
        cluster.apply(&response([2u8; 16], vec![partition(0, 2, 0, 0)]), 13);
        assert_eq!(cluster.leader("t", 0).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 0), 0);
        assert!(cluster.leader("t", 1).is_err());
        assert_eq!(cluster.topic_ids.get("t").copied(), Some([2u8; 16]));
        cluster.apply(
            &response(
                [3u8; 16],
                vec![partition(0, -1, -1, error::LEADER_NOT_AVAILABLE)],
            ),
            13,
        );
        assert!(cluster.leader("t", 0).is_err());
        assert_eq!(cluster.topic_ids.get("t").copied(), Some([3u8; 16]));
    }

    #[test]
    fn an_unknown_topic_drops_its_cached_leader() {
        use crate::error;
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let mut cluster = Cluster::default();
        cluster.apply(
            &MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: Some("mock".into()),
                controller_id: 1,
                topics: vec![TopicMetadata {
                    error_code: 0,
                    name: Some("t".into()),
                    topic_id: [9u8; 16],
                    is_internal: false,
                    partitions: vec![PartitionMetadata {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 4,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: Vec::new(),
                    }],
                    topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                }],
                cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                error_code: 0,
            },
            13,
        );
        let transient = MetadataResponse {
            throttle_time_ms: 0,
            brokers: Vec::new(),
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: error::LEADER_NOT_AVAILABLE,
                name: Some("t".into()),
                topic_id: [9u8; 16],
                is_internal: false,
                partitions: Vec::new(),
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        cluster.apply(&transient, 13);
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 4);
        assert_eq!(cluster.topic_ids.get("t").copied(), Some([9u8; 16]));
        cluster.apply(
            &MetadataResponse {
                topics: vec![TopicMetadata {
                    error_code: error::UNKNOWN_TOPIC_OR_PARTITION,
                    ..transient.topics[0].clone()
                }],
                ..transient
            },
            13,
        );
        assert!(cluster.leader("t", 0).is_err());
        assert_eq!(
            cluster.leader_epoch("t", 0),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(cluster.topic_ids.get("t").is_none());
    }

    #[test]
    fn a_transient_partition_error_keeps_the_cached_leader() {
        use crate::error;
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        fn partition(index: i32, leader: i32, epoch: i32, error_code: i16) -> PartitionMetadata {
            PartitionMetadata {
                error_code,
                partition_index: index,
                leader_id: leader,
                leader_epoch: epoch,
                replica_nodes: vec![1, 2],
                isr_nodes: vec![1, 2],
                offline_replicas: Vec::new(),
            }
        }
        let response = |parts: Vec<PartitionMetadata>| MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: None,
                },
            ],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: parts,
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let mut cluster = Cluster::default();
        cluster.apply(
            &response(vec![partition(0, 1, 4, 0), partition(1, 1, 4, 0)]),
            13,
        );
        cluster.apply(
            &response(vec![
                partition(0, -1, -1, error::LEADER_NOT_AVAILABLE),
                partition(1, 2, 5, 0),
            ]),
            13,
        );
        assert_eq!(cluster.leader("t", 0).unwrap().0, 1);
        assert_eq!(cluster.leader_epoch("t", 0), 4);
        assert_eq!(cluster.leader("t", 1).unwrap().0, 2);
        assert_eq!(cluster.leader_epoch("t", 1), 5);
        cluster.apply(
            &response(vec![
                partition(0, -1, -1, error::UNKNOWN_TOPIC_OR_PARTITION),
                partition(1, 2, 5, 0),
            ]),
            13,
        );
        assert!(cluster.leader("t", 0).is_err());
        assert_eq!(cluster.leader("t", 1).unwrap().0, 2);
    }

    #[test]
    fn apply_drops_unreliable_leader_epochs() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let md = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![Broker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("mock".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [0u8; 16],
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 7,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    offline_replicas: Vec::new(),
                }],
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            }],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let mut cluster = Cluster::default();
        cluster.apply(&md, 8);
        assert_eq!(
            cluster.leader_epoch("t", 0),
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            "Metadata versions before 9 must not retain leader epochs"
        );
        cluster.apply(&md, 9);
        assert_eq!(cluster.leader_epoch("t", 0), 7);
        cluster.apply(&md, 8);
        assert_eq!(
            cluster.leader_epoch("t", 0),
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            "a later Metadata version before 9 must drop previously cached epochs"
        );
    }

    #[test]
    fn apply_fills_sparse_partition_holes_with_sentinels() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let mut cluster = Cluster::default();
        cluster.apply(
            &MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: Some("mock".into()),
                controller_id: 1,
                topics: vec![TopicMetadata {
                    error_code: 0,
                    name: Some("t".into()),
                    topic_id: [0u8; 16],
                    is_internal: false,
                    partitions: vec![PartitionMetadata {
                        error_code: 0,
                        partition_index: 2,
                        leader_id: 1,
                        leader_epoch: 4,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: Vec::new(),
                    }],
                    topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                }],
                cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                error_code: 0,
            },
            13,
        );
        let leaders = cluster.leaders.get("t").expect("topic leaders");
        assert_eq!(
            leaders.first().copied(),
            Some(MetadataResponse::NO_LEADER_ID)
        );
        assert_eq!(
            leaders.get(1).copied(),
            Some(MetadataResponse::NO_LEADER_ID)
        );
        assert_eq!(leaders.get(2).copied(), Some(1));
        assert_eq!(
            cluster.leader_epoch("t", 0),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(
            cluster.leader_epoch("t", 1),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(cluster.leader_epoch("t", 2), 4);
        assert_eq!(
            cluster.leader_epoch("missing", 0),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(cluster.leader("t", 0).is_err());
        assert_eq!(cluster.leader("t", 2).unwrap().0, 1);

        assert!(cluster.apply_current_leader("t", 5, 1, 9));
        let leaders = cluster.leaders.get("t").expect("resized leaders");
        assert_eq!(
            leaders.get(3).copied(),
            Some(MetadataResponse::NO_LEADER_ID)
        );
        assert_eq!(
            leaders.get(4).copied(),
            Some(MetadataResponse::NO_LEADER_ID)
        );
        assert_eq!(leaders.get(5).copied(), Some(1));
        assert_eq!(
            cluster.leader_epoch("t", 3),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(
            cluster.leader_epoch("t", 4),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(cluster.leader_epoch("t", 5), 9);

        cluster.set_leader_epoch("u", 1, 2);
        assert_eq!(
            cluster.leader_epoch("u", 0),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(cluster.leader_epoch("u", 1), 2);
        cluster.set_leader_epoch("u", 1, 1);
        assert_eq!(cluster.leader_epoch("u", 1), 2);
        cluster.set_leader_epoch("u", 1, 4);
        assert_eq!(cluster.leader_epoch("u", 1), 4);
    }
}
