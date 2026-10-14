//! Cluster metadata: brokers and per-partition leaders.

use std::collections::{HashMap, HashSet};
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

/// A broker that can be dialed. Empty hosts, non-positive ids, and ports
/// outside `1..=65535` are not addresses.
fn usable_broker_address(node_id: i32, host: &str, port: i32) -> Option<String> {
    let host = host.trim();
    if node_id < 0 || host.is_empty() || !(1..=i32::from(u16::MAX)).contains(&port) {
        None
    } else {
        Some(crate::net::format_address(host, port))
    }
}

/// Snapshot of brokers and partition leaders from Metadata.
#[derive(Debug, Clone, Default)]
pub(crate) struct Cluster {
    /// `node_id` → `host:port`.
    pub(crate) brokers: HashMap<i32, String>,
    /// Topic → leader `node_id` by partition index.
    pub(crate) leaders: HashMap<String, Vec<i32>>,
    /// Nonzero Metadata v10+ topic identity, invalidated with leader metadata.
    pub(crate) topic_ids: HashMap<String, [u8; 16]>,
    /// Topic → Metadata `leader_epoch` by partition index.
    pub(crate) leader_epochs: HashMap<String, Vec<i32>>,
    /// Metadata `controller_id`, or `None` until the first Metadata response.
    pub(crate) controller_id: Option<i32>,
    /// When each topic's leaders were last applied from Metadata.
    topic_fetched_at: HashMap<String, Instant>,
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
            if let Some(address) = usable_broker_address(b.node_id, &b.host, b.port) {
                let _prev = self.brokers.insert(b.node_id, address);
            }
        }
        let retain_epochs = MetadataResponse::has_reliable_leader_epochs(version);
        // A topic named twice in one response is one image. A later copy must
        // not drop partitions the earlier copy already accepted.
        let mut seen_topics: HashSet<&str> = HashSet::new();
        for t in &md.topics {
            let Some(name) = t.name.as_ref() else {
                continue;
            };
            if t.error_code != 0 {
                // A missing topic must not keep a stale leader. Other topic
                // errors are transient, so the last good leaders stay cached.
                if t.error_code == crate::error::UNKNOWN_TOPIC_OR_PARTITION
                    || t.error_code == crate::error::UNKNOWN_TOPIC_ID
                {
                    self.invalidate_topic(name);
                }
                continue;
            }
            // A new topic id is a deleted-and-recreated topic. Its epochs
            // start over, so the previous generation's leader must not stick.
            let cached_topic_id = self
                .topic_ids
                .get(name)
                .copied()
                .filter(|id| *id != [0; 16]);
            let incoming_topic_id = (version >= 10 && t.topic_id != [0; 16]).then_some(t.topic_id);
            let recreated = match (cached_topic_id, incoming_topic_id) {
                (Some(cached), Some(incoming)) => cached != incoming,
                _ => false,
            };
            let repeated = seen_topics.contains(name.as_str());
            if recreated {
                let _removed = self.leaders.remove(name);
                let _removed = self.leader_epochs.remove(name);
                let _removed = self.topic_fetched_at.remove(name);
            }
            if version >= 10 && t.topic_id != [0; 16] {
                let _ = self.topic_ids.insert(name.clone(), t.topic_id);
            } else if !repeated {
                let _ = self.topic_ids.remove(name);
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
                continue;
            }
            let new_len = match usize::try_from(max_idx.saturating_add(1)) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let previous_leaders = self.leaders.get(name).cloned();
            let previous_epochs = self.leader_epochs.get(name).cloned();
            let previous_len = previous_leaders.as_ref().map(Vec::len).unwrap_or(0);
            let len = if repeated {
                previous_len.max(new_len)
            } else {
                new_len
            };
            let mut leaders = vec![MetadataResponse::NO_LEADER_ID; len];
            let mut epochs = vec![RecordBatch::NO_PARTITION_LEADER_EPOCH; len];
            // A repeated partition is one decision. A later copy must not
            // discard a leader this response already accepted.
            let mut decided = vec![false; len];
            if repeated {
                if let Some(previous) = &previous_leaders {
                    for (idx, id) in previous.iter().enumerate() {
                        if let Some(slot) = leaders.get_mut(idx) {
                            *slot = *id;
                        }
                        if let Some(slot) = decided.get_mut(idx) {
                            *slot = true;
                        }
                    }
                }
                if let Some(previous) = &previous_epochs {
                    for (idx, epoch) in previous.iter().enumerate() {
                        if let Some(slot) = epochs.get_mut(idx) {
                            *slot = *epoch;
                        }
                    }
                }
            }
            for p in &t.partitions {
                let Ok(idx) = usize::try_from(p.partition_index) else {
                    continue;
                };
                let already = decided.get(idx).copied().unwrap_or(false);
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
                            *slot = previous_leaders
                                .as_ref()
                                .and_then(|cached| cached.get(idx).copied())
                                .unwrap_or(MetadataResponse::NO_LEADER_ID);
                        }
                        if let Some(slot) = epochs.get_mut(idx) {
                            *slot = previous_epochs
                                .as_ref()
                                .and_then(|cached| cached.get(idx).copied())
                                .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH);
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
                let prior = previous_epochs
                    .as_ref()
                    .and_then(|cached| cached.get(idx).copied())
                    .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH);
                // A repeated partition in this response is part of the cache
                // being built. An older copy must not replace the newer one.
                let cached_epoch = if already { in_response } else { prior };
                let keep_cached = retain_epochs
                    && cached_epoch != RecordBatch::NO_PARTITION_LEADER_EPOCH
                    && p.leader_epoch < cached_epoch;
                if keep_cached {
                    if !already {
                        if let Some(slot) = leaders.get_mut(idx) {
                            *slot = previous_leaders
                                .as_ref()
                                .and_then(|cached| cached.get(idx).copied())
                                .unwrap_or(MetadataResponse::NO_LEADER_ID);
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
            let _prev = self.leaders.insert(name.clone(), leaders);
            let _prev = self.leader_epochs.insert(name.clone(), epochs);
            let _prev = self.topic_fetched_at.insert(name.clone(), Instant::now());
            let _inserted = seen_topics.insert(name.as_str());
        }
    }

    /// Drop cached leaders for `topic` so the next lookup refetches Metadata.
    pub(crate) fn invalidate_topic(&mut self, topic: &str) {
        let _removed = self.topic_ids.remove(topic);
        let _removed = self.leaders.remove(topic);
        let _removed = self.leader_epochs.remove(topic);
        let _removed = self.topic_fetched_at.remove(topic);
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
        let Ok(idx) = usize::try_from(partition) else {
            return RecordBatch::NO_PARTITION_LEADER_EPOCH;
        };
        self.leader_epochs
            .get(topic)
            .and_then(|v| v.get(idx))
            .copied()
            .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH)
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
        let _prev = self.leader_epochs.insert(topic.to_string(), v);
    }

    /// Insert Produce v10+ / Fetch v16+ NodeEndpoints into the broker map.
    ///
    /// Call this before [`Self::apply_current_leader`] so an unknown
    /// CurrentLeader id can patch the partition cache (KIP-951).
    pub(crate) fn apply_node_endpoints(&mut self, endpoints: &[NodeEndpoint]) {
        for e in endpoints {
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
        {
            let leaders = self.leaders.entry(topic.to_string()).or_default();
            if leaders.len() <= idx {
                leaders.resize(idx.saturating_add(1), MetadataResponse::NO_LEADER_ID);
            }
            if let Some(slot) = leaders.get_mut(idx) {
                *slot = leader_id;
            }
        }
        self.set_leader_epoch(topic, partition, leader_epoch);
        let _prev = self
            .topic_fetched_at
            .insert(topic.to_string(), Instant::now());
        true
    }

    /// Partition count from the last Metadata that listed `topic`.
    pub(crate) fn partition_count(&self, topic: &str) -> Option<i32> {
        self.leaders
            .get(topic)
            .map(|v| i32::try_from(v.len()).unwrap_or(i32::MAX))
    }

    /// Leader node id and `host:port` for `topic`/`partition`.
    pub(crate) fn leader(&self, topic: &str, partition: i32) -> Result<(i32, String)> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error;
    use crate::protocol::api::{Broker, MetadataResponse};

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
        assert!(
            !cluster.topic_fresh("t", Duration::ZERO),
            "zero max.age must refresh every lookup"
        );
        cluster.invalidate_topic("t");
        assert!(!cluster.topic_fresh("t", Duration::from_secs(5)));
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
