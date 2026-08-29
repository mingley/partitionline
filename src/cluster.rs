//! Cluster metadata: brokers and per-partition leaders.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::protocol::api::{MetadataResponse, NodeEndpoint};

/// Snapshot of brokers and partition leaders from Metadata.
#[derive(Debug, Clone, Default)]
pub(crate) struct Cluster {
    /// `node_id` → `host:port`.
    pub(crate) brokers: HashMap<i32, String>,
    /// Topic → leader `node_id` by partition index.
    pub(crate) leaders: HashMap<String, Vec<i32>>,
    /// Topic → Metadata `leader_epoch` by partition index.
    pub(crate) leader_epochs: HashMap<String, Vec<i32>>,
    /// Metadata `controller_id`, or `None` until the first Metadata response.
    pub(crate) controller_id: Option<i32>,
    /// When each topic's leaders were last applied from Metadata.
    topic_fetched_at: HashMap<String, Instant>,
}

impl Cluster {
    /// Merge a Metadata response into this snapshot.
    pub(crate) fn apply(&mut self, md: &MetadataResponse) {
        self.controller_id = (md.controller_id >= 0).then_some(md.controller_id);
        for b in &md.brokers {
            let _prev = self
                .brokers
                .insert(b.node_id, format!("{}:{}", b.host, b.port));
        }
        for t in &md.topics {
            let Some(name) = t.name.as_ref() else {
                continue;
            };
            if t.error_code != 0 {
                continue;
            }
            let mut max_idx = -1i32;
            for p in &t.partitions {
                if p.partition_index > max_idx {
                    max_idx = p.partition_index;
                }
            }
            if max_idx < 0 {
                continue;
            }
            let len = match usize::try_from(max_idx.saturating_add(1)) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let mut leaders = vec![-1; len];
            let mut epochs = vec![-1; len];
            for p in &t.partitions {
                if p.error_code != 0 {
                    continue;
                }
                let Ok(idx) = usize::try_from(p.partition_index) else {
                    continue;
                };
                if let Some(slot) = leaders.get_mut(idx) {
                    *slot = p.leader_id;
                }
                if let Some(slot) = epochs.get_mut(idx) {
                    *slot = p.leader_epoch;
                }
            }
            let _prev = self.leaders.insert(name.clone(), leaders);
            let _prev = self.leader_epochs.insert(name.clone(), epochs);
            let _prev = self.topic_fetched_at.insert(name.clone(), Instant::now());
        }
    }

    /// Drop cached leaders for `topic` so the next lookup refetches Metadata.
    pub(crate) fn invalidate_topic(&mut self, topic: &str) {
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

    /// Last Metadata `leader_epoch` for `topic`/`partition`, or `-1`.
    pub(crate) fn leader_epoch(&self, topic: &str, partition: i32) -> i32 {
        let Ok(idx) = usize::try_from(partition) else {
            return -1;
        };
        self.leader_epochs
            .get(topic)
            .and_then(|v| v.get(idx))
            .copied()
            .unwrap_or(-1)
    }

    pub(crate) fn set_leader_epoch(&mut self, topic: &str, partition: i32, epoch: i32) {
        let Ok(idx) = usize::try_from(partition) else {
            return;
        };
        if let Some(v) = self.leader_epochs.get_mut(topic) {
            if v.len() <= idx {
                v.resize(idx.saturating_add(1), -1);
            }
            if let Some(slot) = v.get_mut(idx) {
                *slot = epoch;
            }
            return;
        }
        let mut v = vec![-1; idx.saturating_add(1)];
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
            if e.node_id < 0 || e.host.is_empty() || e.port <= 0 {
                continue;
            }
            let _prev = self
                .brokers
                .insert(e.node_id, format!("{}:{}", e.host, e.port));
        }
    }

    /// Apply Produce v10+ / Fetch v12+ CurrentLeader when `leader_id` is a
    /// known broker.
    ///
    /// Unknown brokers need [`Self::apply_node_endpoints`] first. Returns
    /// `true` when the partition leader cache was updated.
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
        let Ok(idx) = usize::try_from(partition) else {
            return false;
        };
        {
            let leaders = self.leaders.entry(topic.to_string()).or_default();
            if leaders.len() <= idx {
                leaders.resize(idx.saturating_add(1), -1);
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
        cluster.apply(&MetadataResponse {
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
            error_code: 0,
        });
        assert_eq!(cluster.controller().unwrap(), 2);
        cluster.invalidate_controller();
        assert!(cluster.controller().is_err());
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
        cluster.apply(&MetadataResponse {
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
            }],
            error_code: 0,
        });
        assert!(cluster.topic_fresh("t", Duration::from_secs(5)));
        assert!(
            !cluster.topic_fresh("t", Duration::ZERO),
            "zero max.age must refresh every lookup"
        );
        cluster.invalidate_topic("t");
        assert!(!cluster.topic_fresh("t", Duration::from_secs(5)));
    }

    #[test]
    fn apply_current_leader_updates_known_broker() {
        use crate::protocol::api::{NodeEndpoint, PartitionMetadata, TopicMetadata};

        let mut cluster = Cluster::default();
        cluster.apply(&MetadataResponse {
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
            }],
            error_code: 0,
        });
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
        assert!(!cluster.apply_current_leader("t", 0, -1, 8));
        assert_eq!(cluster.leader("t", 0).unwrap().0, 99);
    }
}
