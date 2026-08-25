//! Cluster metadata: brokers and per-partition leaders.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::protocol::api::MetadataResponse;

/// Snapshot of brokers and partition leaders from Metadata.
#[derive(Debug, Clone, Default)]
pub(crate) struct Cluster {
    /// `node_id` → `host:port`.
    pub(crate) brokers: HashMap<i32, String>,
    /// Topic → leader `node_id` by partition index.
    pub(crate) leaders: HashMap<String, Vec<i32>>,
}

impl Cluster {
    /// Merge a Metadata response into this snapshot.
    pub(crate) fn apply(&mut self, md: &MetadataResponse) {
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
            }
            let _prev = self.leaders.insert(name.clone(), leaders);
        }
    }

    /// Drop cached leaders for `topic` so the next lookup refetches Metadata.
    pub(crate) fn invalidate_topic(&mut self, topic: &str) {
        let _removed = self.leaders.remove(topic);
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
