//! Share groups (KIP-932): queue-style consumption with per-record ack.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::watch;

use crate::consumer::{Consumer, ConsumerConfig};
use crate::error::{self, Error, Result};
use crate::group::{collect_topics, coord_roundtrip, discover_coord};
use crate::net::BrokerConn;
use crate::protocol::api_keys::{SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_HEARTBEAT};
use crate::protocol::group::COORDINATOR_SHARE;
use crate::protocol::share::{
    decode_share_acknowledge_response, decode_share_fetch_response,
    decode_share_group_heartbeat_response, encode_share_acknowledge_request,
    encode_share_acknowledge_topics, encode_share_fetch_request,
    encode_share_group_heartbeat_request, AcknowledgementBatch, ShareAckTopic, ShareFetchPartition,
    ShareFetchTopic, ShareGroupHeartbeatRequest, ShareTopicPartitions, ACK_ACCEPT, ACK_REJECT,
    ACK_RELEASE,
};

pub use crate::protocol::share::{
    ACK_ACCEPT as SHARE_ACK_ACCEPT, ACK_REJECT as SHARE_ACK_REJECT,
    ACK_RELEASE as SHARE_ACK_RELEASE,
};

/// One record from ShareFetch.
#[derive(Debug, Clone)]
pub struct ShareRecord {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// Optional key.
    pub key: Option<Bytes>,
    /// Optional value.
    pub value: Option<Bytes>,
    /// Broker delivery count for this share.
    pub delivery_count: i16,
    /// Partition leader epoch from the record batch, or `None` when `-1`.
    pub leader_epoch: Option<i32>,
}

impl ShareRecord {
    /// Topic and partition of this record.
    #[must_use]
    pub fn topic_partition(&self) -> crate::TopicPartition {
        crate::TopicPartition::new(self.topic.clone(), self.partition)
    }

    /// Serialized key size in bytes, or `-1` if there is no key (Java `serializedKeySize`).
    #[must_use]
    pub fn serialized_key_size(&self) -> i32 {
        self.key
            .as_ref()
            .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
            .unwrap_or(-1)
    }

    /// Serialized value size in bytes, or `-1` if there is no value (Java `serializedValueSize`).
    #[must_use]
    pub fn serialized_value_size(&self) -> i32 {
        self.value
            .as_ref()
            .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
            .unwrap_or(-1)
    }
}

/// Records from one share poll (Java `ConsumerRecords` for KIP-932).
///
/// Indexes and iterates like a slice of [`ShareRecord`].
#[derive(Debug, Clone, Default)]
pub struct ShareRecords {
    records: Vec<ShareRecord>,
}

impl ShareRecords {
    /// Number of records (Java `count`). Same as slice `len` via [`Deref`].
    #[must_use]
    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// Distinct partitions in this batch, in first-seen order.
    #[must_use]
    pub fn partitions(&self) -> Vec<crate::TopicPartition> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for rec in &self.records {
            let tp = rec.topic_partition();
            if seen.insert(tp.clone()) {
                out.push(tp);
            }
        }
        out
    }

    /// Records for this partition.
    pub fn records(
        &self,
        partition: impl Into<crate::TopicPartition>,
    ) -> impl Iterator<Item = &ShareRecord> {
        let tp = partition.into();
        self.records
            .iter()
            .filter(move |r| r.topic == tp.topic && r.partition == tp.partition)
    }
}

impl Deref for ShareRecords {
    type Target = [ShareRecord];

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl AsRef<[ShareRecord]> for ShareRecords {
    fn as_ref(&self) -> &[ShareRecord] {
        &self.records
    }
}

impl From<Vec<ShareRecord>> for ShareRecords {
    fn from(records: Vec<ShareRecord>) -> Self {
        Self { records }
    }
}

impl IntoIterator for ShareRecords {
    type Item = ShareRecord;
    type IntoIter = std::vec::IntoIter<ShareRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<'a> IntoIterator for &'a ShareRecords {
    type Item = &'a ShareRecord;
    type IntoIter = std::slice::Iter<'a, ShareRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

/// KIP-932 share group member (`ShareGroupHeartbeat` / ShareFetch / ShareAcknowledge).
pub struct ShareGroup {
    consumer: Consumer,
    coord: BrokerConn,
    cfg: ConsumerConfig,
    group_id: String,
    member_id: String,
    member_epoch: i32,
    topics: Vec<String>,
    assigned: Vec<(String, i32)>,
    topic_ids: HashMap<String, [u8; 16]>,
    /// Share session epoch per share-partition leader (KIP-932).
    share_epochs: HashMap<i32, i32>,
    hb_err: Arc<AtomicI16>,
    hb_epoch: Arc<AtomicI32>,
    hb_stop: watch::Sender<bool>,
    fetch_rounds: u64,
    records_fetched: u64,
    bytes_fetched: u64,
    fetch_errors: u64,
    records_acknowledged: u64,
}

fn new_member_id() -> Result<String> {
    let mut raw = [0u8; 8];
    getrandom::getrandom(&mut raw).map_err(|_| Error::protocol("share member id rng"))?;
    let mut hex = String::with_capacity(16);
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for b in raw {
        let hi = usize::from(b >> 4);
        let lo = usize::from(b & 0x0f);
        if let (Some(&h), Some(&l)) = (DIGITS.get(hi), DIGITS.get(lo)) {
            hex.push(char::from(h));
            hex.push(char::from(l));
        }
    }
    Ok(format!("s-{hex}"))
}

impl ShareGroup {
    /// Join a share group. One topic.
    pub async fn join(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        Self::join_topics(cfg, group_id, std::iter::once(topic)).await
    }

    /// Join a share group. Several topics.
    pub async fn join_topics(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let group_id = group_id.into();
        let topics = collect_topics(topics)?;
        let consumer = Consumer::new(cfg.clone()).await?;
        let coord = discover_coord(&cfg, &group_id, COORDINATOR_SHARE).await?;
        let member_id = new_member_id()?;
        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_epoch = Arc::new(AtomicI32::new(0));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            consumer,
            coord,
            cfg: cfg.clone(),
            group_id,
            member_id,
            member_epoch: 0,
            topics,
            assigned: Vec::new(),
            topic_ids: HashMap::new(),
            share_epochs: HashMap::new(),
            hb_err,
            hb_epoch,
            hb_stop,
            fetch_rounds: 0,
            records_fetched: 0,
            bytes_fetched: 0,
            fetch_errors: 0,
            records_acknowledged: 0,
        };
        g.heartbeat_join().await?;
        g.spawn_heartbeat(hb_rx);
        Ok(g)
    }

    /// Kafka member id assigned by the coordinator.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Kafka `group.id`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Subscribed topic names, in join order.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Subscribed topic names. Same as [`Self::topics`] (Java `subscription`).
    #[must_use]
    pub fn subscription(&self) -> &[String] {
        self.topics()
    }

    /// Assigned partitions (Java `assignment`).
    #[must_use]
    pub fn assignment(&self) -> Vec<crate::TopicPartition> {
        self.assigned
            .iter()
            .map(|(t, p)| crate::TopicPartition::new(t.clone(), *p))
            .collect()
    }

    /// Same as [`Self::assignment`].
    #[must_use]
    pub fn assigned_partitions(&self) -> Vec<crate::TopicPartition> {
        self.assignment()
    }

    /// Cluster Metadata for every topic (Java `listTopics`).
    pub async fn list_topics(&mut self) -> Result<Vec<crate::PartitionInfo>> {
        self.consumer.list_topics().await
    }

    /// ShareFetch / ShareAcknowledge counters since join.
    #[must_use]
    pub fn metrics(&self) -> crate::ShareMetrics {
        crate::ShareMetrics {
            fetch_rounds: self.fetch_rounds,
            records_fetched: self.records_fetched,
            bytes_fetched: self.bytes_fetched,
            fetch_errors: self.fetch_errors,
            records_acknowledged: self.records_acknowledged,
        }
    }

    /// Interrupt [`Self::poll`]. See [`crate::Consumer::wakeup`].
    pub fn wakeup(&self) {
        self.consumer.wakeup();
    }

    /// Cloneable handle for [`Self::wakeup`] from another task.
    #[must_use]
    pub fn wakeup_handle(&self) -> crate::WakeupHandle {
        self.consumer.wakeup_handle()
    }

    async fn heartbeat_join(&mut self) -> Result<()> {
        let timeout = Duration::from_secs(30);
        self.consumer.refresh_topics(&self.topics).await?;
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: 0,
            subscribed_topic_names: Some(self.topics.clone()),
        };
        let body = self
            .coord
            .roundtrip(
                SHARE_GROUP_HEARTBEAT,
                1,
                |buf| encode_share_group_heartbeat_request(buf, &req),
                timeout,
            )
            .await?;
        let resp = decode_share_group_heartbeat_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ShareGroupHeartbeat"));
        }
        if let Some(id) = resp.member_id {
            if !id.is_empty() {
                self.member_id = id;
            }
        }
        self.member_epoch = resp.member_epoch;
        self.apply_share_assignment(resp.assignment.as_deref());
        if self.assigned.is_empty() {
            if let Some(topic) = self.topics.first() {
                self.assigned.push((topic.clone(), 0));
                let _ = self.topic_ids.entry(topic.clone()).or_insert([0u8; 16]);
            }
        }
        self.hb_epoch.store(self.member_epoch, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn apply_share_assignment(&mut self, assignment: Option<&[ShareTopicPartitions]>) {
        self.assigned.clear();
        self.topic_ids.clear();
        let Some(assigned) = assignment else {
            return;
        };
        let id_to_name = self.consumer.topic_id_names();
        for tp in assigned {
            let name = id_to_name.get(&tp.topic_id).cloned().or_else(|| {
                if self.topics.len() == 1 || tp.topic_id == [0u8; 16] {
                    self.topics.first().cloned()
                } else {
                    None
                }
            });
            let Some(name) = name else {
                continue;
            };
            let _ = self.topic_ids.insert(name.clone(), tp.topic_id);
            for p in &tp.partitions {
                self.assigned.push((name.clone(), *p));
            }
        }
    }

    /// Fetch records from assigned share partitions.
    ///
    /// Returns [`ShareRecords`], which indexes like a slice of [`ShareRecord`].
    pub async fn poll(&mut self) -> Result<ShareRecords> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        let hb = self.hb_err.load(Ordering::SeqCst);
        if hb != 0 {
            return Err(Error::broker(hb, "ShareGroupHeartbeat"));
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.poll_leaders().await {
                Ok(recs) => {
                    self.fetch_rounds = self.fetch_rounds.saturating_add(1);
                    let n = u64::try_from(recs.len()).unwrap_or(u64::MAX);
                    self.records_fetched = self.records_fetched.saturating_add(n);
                    let bytes = recs
                        .iter()
                        .map(share_record_bytes)
                        .fold(0, u64::saturating_add);
                    self.bytes_fetched = self.bytes_fetched.saturating_add(bytes);
                    return Ok(ShareRecords::from(recs));
                }
                Err(e) if share_leader_retriable(&e) => {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    self.refresh_assigned_metadata().await?;
                }
                Err(e) => {
                    self.fetch_errors = self.fetch_errors.saturating_add(1);
                    return Err(e);
                }
            }
        }
    }

    /// Acknowledge records as successfully processed (`ACCEPT`).
    pub async fn accept(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, ACK_ACCEPT).await
    }

    /// Return records to the share (`RELEASE`).
    pub async fn release(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, ACK_RELEASE).await
    }

    /// Reject records (`REJECT`).
    pub async fn reject(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, ACK_REJECT).await
    }

    fn session_epoch(&self, node: i32) -> i32 {
        self.share_epochs.get(&node).copied().unwrap_or(0)
    }

    fn advance_node_epoch(&mut self, node: i32) {
        let next = self.session_epoch(node).saturating_add(1);
        let _ = self.share_epochs.insert(node, next);
    }

    fn reset_node_session(&mut self, node: i32) {
        let _ = self.share_epochs.remove(&node);
        self.consumer.drop_node(node);
    }

    async fn refresh_assigned_metadata(&mut self) -> Result<()> {
        let topics = self.topics.clone();
        for t in &topics {
            self.consumer.invalidate_topic(t);
        }
        self.consumer.refresh_topics(&topics).await
    }

    async fn leaders_of(
        &mut self,
        tps: &[(String, i32)],
    ) -> Result<HashMap<i32, Vec<(String, i32)>>> {
        for (topic, _) in tps {
            self.consumer.ensure_topic_metadata(topic).await?;
        }
        let mut by_leader: HashMap<i32, Vec<(String, i32)>> = HashMap::new();
        for (topic, p) in tps {
            let (node, _) = self.consumer.leader_of(topic, *p)?;
            by_leader.entry(node).or_default().push((topic.clone(), *p));
        }
        Ok(by_leader)
    }

    async fn poll_leaders(&mut self) -> Result<Vec<ShareRecord>> {
        let assigned = self.assigned.clone();
        let by_leader = self.leaders_of(&assigned).await?;
        let timeout = Duration::from_secs(30);
        let max_wait = self.cfg.max_wait_ms;
        let mut out = Vec::new();
        for (node, tps) in by_leader {
            let epoch = self.session_epoch(node);
            let mut by_id: HashMap<[u8; 16], Vec<i32>> = HashMap::new();
            for (topic, p) in &tps {
                let id = self.topic_ids.get(topic).copied().unwrap_or([0u8; 16]);
                by_id.entry(id).or_default().push(*p);
            }
            let topics: Vec<ShareFetchTopic> = by_id
                .into_iter()
                .map(|(topic_id, partitions)| ShareFetchTopic {
                    topic_id,
                    partitions: partitions
                        .into_iter()
                        .map(|p| ShareFetchPartition {
                            partition: p,
                            acknowledgements: Vec::new(),
                        })
                        .collect(),
                })
                .collect();
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_FETCH,
                    1,
                    |buf| {
                        encode_share_fetch_request(
                            buf,
                            &self.group_id,
                            &self.member_id,
                            epoch,
                            max_wait,
                            1,
                            1_048_576,
                            16,
                            &topics,
                        )
                    },
                    timeout,
                )
                .await;
            let mut body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    self.reset_node_session(node);
                    return Err(e);
                }
                Err(e) => return Err(e),
            };
            let fetched = match decode_share_fetch_response(&mut body) {
                Ok(f) => f,
                Err(e) => {
                    if share_session_reset(&e) || share_leader_retriable(&e) {
                        self.reset_node_session(node);
                    }
                    return Err(e);
                }
            };
            for topic in &fetched {
                for part in &topic.partitions {
                    if part.error_code != 0 {
                        let e = Error::broker(part.error_code, "ShareFetch");
                        if share_leader_retriable(&e) || share_session_reset(&e) {
                            self.reset_node_session(node);
                        }
                        return Err(e);
                    }
                }
            }
            self.advance_node_epoch(node);
            for topic in fetched {
                let name = self.name_for_topic_id(topic.topic_id);
                for part in topic.partitions {
                    for batch in part.records {
                        for rec in batch.records {
                            let delivery = part
                                .acquired
                                .iter()
                                .find(|a| {
                                    rec.offset >= a.first_offset && rec.offset <= a.last_offset
                                })
                                .map(|a| a.delivery_count)
                                .unwrap_or(1);
                            out.push(ShareRecord {
                                topic: name.clone(),
                                partition: part.partition,
                                offset: rec.offset,
                                timestamp: rec.timestamp,
                                key: rec.key,
                                value: rec.value,
                                delivery_count: delivery,
                                leader_epoch: (batch.partition_leader_epoch >= 0)
                                    .then_some(batch.partition_leader_epoch),
                            });
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Fetch with a one-shot `fetch.max.wait.ms` (Java `poll(Duration)`).
    ///
    /// [`ConsumerConfig::max_wait_ms`] is restored afterwards.
    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<ShareRecords> {
        let prev = self.cfg.max_wait_ms;
        self.cfg.max_wait_ms = crate::consumer::duration_millis_i32(timeout);
        let out = self.poll().await;
        self.cfg.max_wait_ms = prev;
        out
    }

    /// Leave the share group and drop the subscription (Java `unsubscribe`).
    ///
    /// Heartbeats stop and the assignment is cleared. [`Self::subscribe`] joins
    /// again with a new topic list. [`Self::leave`] after this is a no-op.
    pub async fn unsubscribe(&mut self) -> Result<()> {
        if self.member_id.is_empty() {
            self.topics.clear();
            self.assigned.clear();
            self.topic_ids.clear();
            self.share_epochs.clear();
            return Ok(());
        }
        self.hb_stop.send(true).unwrap_or(());
        self.close_share_session().await?;
        self.leave_coordinator().await?;
        self.assigned.clear();
        self.topics.clear();
        self.topic_ids.clear();
        self.share_epochs.clear();
        self.member_id.clear();
        self.member_epoch = 0;
        self.hb_epoch.store(0, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(())
    }

    /// Replace the subscription and (re)join (Java `subscribe`).
    ///
    /// If this member is already in the group, the coordinator is not left;
    /// a join heartbeat uses the new topic list. After [`Self::unsubscribe`],
    /// this starts a new heartbeat loop.
    pub async fn subscribe(
        &mut self,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<()> {
        let topics = collect_topics(topics)?;
        if topics == self.topics && !self.member_id.is_empty() {
            return Ok(());
        }
        let rejoining = !self.member_id.is_empty();
        if rejoining {
            self.close_share_session().await?;
            self.assigned.clear();
            self.topic_ids.clear();
            self.share_epochs.clear();
        }
        self.topics = topics;
        if rejoining {
            self.heartbeat_join().await?;
            return Ok(());
        }
        self.member_id = new_member_id()?;
        let (hb_stop, hb_rx) = watch::channel(false);
        self.hb_stop = hb_stop;
        self.heartbeat_join().await?;
        self.spawn_heartbeat(hb_rx);
        Ok(())
    }

    fn name_for_topic_id(&self, id: [u8; 16]) -> String {
        self.topic_ids
            .iter()
            .find(|(_, v)| **v == id)
            .map(|(n, _)| n.clone())
            .or_else(|| self.topics.first().cloned())
            .unwrap_or_default()
    }

    async fn acknowledge(&mut self, recs: &[ShareRecord], ack: i8) -> Result<()> {
        if recs.is_empty() {
            return Ok(());
        }
        let partitions = acknowledgement_batches(recs, ack);
        if partitions.is_empty() {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.acknowledge_leaders(&partitions).await {
                Ok(()) => {
                    let n = u64::try_from(recs.len()).unwrap_or(u64::MAX);
                    self.records_acknowledged = self.records_acknowledged.saturating_add(n);
                    return Ok(());
                }
                Err(e) if share_leader_retriable(&e) => {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    self.refresh_assigned_metadata().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn acknowledge_leaders(
        &mut self,
        partitions: &[(String, i32, Vec<AcknowledgementBatch>)],
    ) -> Result<()> {
        let tps: Vec<(String, i32)> = partitions.iter().map(|(t, p, _)| (t.clone(), *p)).collect();
        let by_leader = self.leaders_of(&tps).await?;
        let timeout = Duration::from_secs(30);
        for (node, node_tps) in by_leader {
            let epoch = self.session_epoch(node);
            if epoch <= 0 {
                return Err(Error::protocol(
                    "ShareAcknowledge requires an open share session (poll first)",
                ));
            }
            let mut topics: Vec<ShareAckTopic> = Vec::new();
            for (topic, part, batches) in partitions {
                if !node_tps.iter().any(|(t, p)| t == topic && p == part) {
                    continue;
                }
                let id = self.topic_ids.get(topic).copied().unwrap_or([0u8; 16]);
                match topics.iter_mut().find(|t| t.topic_id == id) {
                    Some(slot) => slot.partitions.push((*part, batches.clone())),
                    None => topics.push(ShareAckTopic {
                        topic_id: id,
                        partitions: vec![(*part, batches.clone())],
                    }),
                }
            }
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_ACKNOWLEDGE,
                    1,
                    |buf| {
                        encode_share_acknowledge_topics(
                            buf,
                            &self.group_id,
                            &self.member_id,
                            epoch,
                            &topics,
                        )
                    },
                    timeout,
                )
                .await;
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    self.reset_node_session(node);
                    return Err(e);
                }
                Err(e) => return Err(e),
            };
            let err = decode_share_acknowledge_response(&mut body.clone())?;
            if err != 0 {
                let e = Error::broker(err, "ShareAcknowledge");
                if share_leader_retriable(&e) || share_session_reset(&e) {
                    self.reset_node_session(node);
                }
                return Err(e);
            }
            self.advance_node_epoch(node);
        }
        Ok(())
    }

    async fn close_share_session(&mut self) -> Result<()> {
        let open: Vec<i32> = self
            .share_epochs
            .iter()
            .filter(|(_, e)| **e > 0)
            .map(|(n, _)| *n)
            .collect();
        if open.is_empty() {
            return Ok(());
        }
        let timeout = Duration::from_secs(30);
        let mut last = Ok(());
        for node in open {
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_ACKNOWLEDGE,
                    1,
                    |buf| {
                        encode_share_acknowledge_request(
                            buf,
                            &self.group_id,
                            &self.member_id,
                            -1,
                            [0u8; 16],
                            &[],
                        )
                    },
                    timeout,
                )
                .await;
            let err = match body {
                Ok(body) => decode_share_acknowledge_response(&mut body.clone())?,
                Err(_) => error::SHARE_SESSION_NOT_FOUND,
            };
            let _ = self.share_epochs.remove(&node);
            if err != 0 && err != error::SHARE_SESSION_NOT_FOUND {
                last = Err(Error::broker(err, "ShareAcknowledge close"));
            }
        }
        last
    }

    /// Leave the share group (member epoch `-1`).
    pub async fn leave(mut self) -> Result<()> {
        if self.member_id.is_empty() {
            self.hb_stop.send(true).unwrap_or(());
            self.consumer.close_interceptors();
            return Ok(());
        }
        self.hb_stop.send(true).unwrap_or(());
        self.close_share_session().await?;
        let out = self.leave_coordinator().await;
        self.consumer.close_interceptors();
        out
    }

    async fn leave_coordinator(&mut self) -> Result<()> {
        let timeout = Duration::from_secs(30);
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: -1,
            subscribed_topic_names: None,
        };
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_SHARE,
            SHARE_GROUP_HEARTBEAT,
            1,
            |buf| encode_share_group_heartbeat_request(buf, &req),
            timeout,
        )
        .await?;
        let resp = decode_share_group_heartbeat_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ShareGroupHeartbeat leave"));
        }
        Ok(())
    }

    /// Leave the share group. Same as [`Self::leave`].
    pub async fn close(self) -> Result<()> {
        self.leave().await
    }

    fn spawn_heartbeat(&self, mut stop: watch::Receiver<bool>) {
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_epoch = self.hb_epoch.clone();
        let cfg = self.cfg.clone();
        drop(tokio::spawn(async move {
            let mut conn: Option<BrokerConn> = None;
            let mut tick = tokio::time::interval(Duration::from_millis(150));
            loop {
                tokio::select! {
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                    _ = tick.tick() => {
                        if conn.is_none() {
                            conn = discover_coord(&cfg, &group_id, COORDINATOR_SHARE).await.ok();
                        }
                        let Some(c) = conn.as_mut() else {
                            continue;
                        };
                        let epoch = hb_epoch.load(Ordering::SeqCst);
                        let req = ShareGroupHeartbeatRequest {
                            group_id: group_id.clone(),
                            member_id: member_id.clone(),
                            member_epoch: epoch,
                            subscribed_topic_names: None,
                        };
                        let res = c
                            .roundtrip(
                                SHARE_GROUP_HEARTBEAT,
                                1,
                                |buf| encode_share_group_heartbeat_request(buf, &req),
                                Duration::from_secs(10),
                            )
                            .await;
                        match res {
                            Ok(body) => {
                                if let Ok(resp) =
                                    decode_share_group_heartbeat_response(&mut body.clone())
                                {
                                    if crate::error::coordinator_retriable(resp.error_code) {
                                        conn = None;
                                    } else {
                                        hb_err.store(resp.error_code, Ordering::SeqCst);
                                        if resp.member_epoch > 0 {
                                            hb_epoch.store(resp.member_epoch, Ordering::SeqCst);
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                conn = None;
                            }
                        }
                    }
                }
            }
        }));
    }
}

fn share_record_bytes(rec: &ShareRecord) -> u64 {
    let k = rec.key.as_ref().map(Bytes::len).unwrap_or(0);
    let v = rec.value.as_ref().map(Bytes::len).unwrap_or(0);
    u64::try_from(k.saturating_add(v)).unwrap_or(u64::MAX)
}

fn share_leader_retriable(e: &Error) -> bool {
    match e {
        Error::NoLeader { .. } => true,
        Error::Broker { code, .. } => matches!(
            *code,
            error::NOT_LEADER_OR_FOLLOWER
                | error::LEADER_NOT_AVAILABLE
                | error::UNKNOWN_TOPIC_OR_PARTITION
        ),
        Error::Io(_) | Error::Timeout => true,
        _ => false,
    }
}

fn share_session_reset(e: &Error) -> bool {
    matches!(
        e,
        Error::Broker {
            code: error::SHARE_SESSION_NOT_FOUND | error::INVALID_SHARE_SESSION_EPOCH,
            ..
        }
    )
}

/// Collapse records into KIP-932 acknowledgement batches.
///
/// Contiguous offsets with the same type become one batch with a single
/// `AcknowledgeType` (applies to the whole range). Gaps start a new batch.
fn acknowledgement_batches(
    recs: &[ShareRecord],
    ack: i8,
) -> Vec<(String, i32, Vec<AcknowledgementBatch>)> {
    let mut by_part: BTreeMap<(String, i32), Vec<i64>> = BTreeMap::new();
    for rec in recs {
        by_part
            .entry((rec.topic.clone(), rec.partition))
            .or_default()
            .push(rec.offset);
    }
    let mut out = Vec::with_capacity(by_part.len());
    for ((topic, partition), mut offs) in by_part {
        offs.sort_unstable();
        offs.dedup();
        let mut batches = Vec::new();
        let mut range: Option<(i64, i64)> = None;
        for off in offs {
            range = match range {
                None => Some((off, off)),
                Some((s, p)) if off == p.saturating_add(1) => Some((s, off)),
                Some((s, p)) => {
                    batches.push(AcknowledgementBatch {
                        first_offset: s,
                        last_offset: p,
                        types: vec![ack],
                    });
                    Some((off, off))
                }
            };
        }
        if let Some((s, p)) = range {
            batches.push(AcknowledgementBatch {
                first_offset: s,
                last_offset: p,
                types: vec![ack],
            });
        }
        if !batches.is_empty() {
            out.push((topic, partition, batches));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(partition: i32, offset: i64) -> ShareRecord {
        ShareRecord {
            topic: "t".into(),
            partition,
            offset,
            timestamp: 0,
            key: None,
            value: None,
            delivery_count: 1,
            leader_epoch: None,
        }
    }

    #[test]
    fn acknowledgement_batches_collapses_contiguous_offsets() {
        let recs = [rec(0, 1), rec(0, 3), rec(0, 2), rec(1, 9)];
        let batches = acknowledgement_batches(&recs, ACK_ACCEPT);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].0, "t");
        assert_eq!(batches[0].1, 0);
        assert_eq!(batches[0].2.len(), 1);
        assert_eq!(batches[0].2[0].first_offset, 1);
        assert_eq!(batches[0].2[0].last_offset, 3);
        assert_eq!(batches[0].2[0].types, vec![ACK_ACCEPT]);
        assert_eq!(batches[1].0, "t");
        assert_eq!(batches[1].1, 1);
        assert_eq!(batches[1].2[0].first_offset, 9);
        assert_eq!(batches[1].2[0].last_offset, 9);
    }

    #[test]
    fn share_leader_retriable_is_not_leader_or_missing() {
        assert!(share_leader_retriable(&Error::broker(
            error::NOT_LEADER_OR_FOLLOWER,
            "x"
        )));
        assert!(share_leader_retriable(&Error::NoLeader {
            topic: "t".into(),
            partition: 0,
        }));
        assert!(!share_leader_retriable(&Error::broker(
            error::INVALID_RECORD_STATE,
            "x"
        )));
        assert!(share_session_reset(&Error::broker(
            error::INVALID_SHARE_SESSION_EPOCH,
            "x"
        )));
    }

    #[test]
    fn acknowledgement_batches_splits_on_gap() {
        let recs = [rec(0, 1), rec(0, 4)];
        let batches = acknowledgement_batches(&recs, ACK_REJECT);
        assert_eq!(batches[0].2.len(), 2);
        assert_eq!(batches[0].2[0].first_offset, 1);
        assert_eq!(batches[0].2[0].last_offset, 1);
        assert_eq!(batches[0].2[1].first_offset, 4);
        assert_eq!(batches[0].2[1].types, vec![ACK_REJECT]);
    }

    #[test]
    fn share_records_partitions_and_filters() {
        let recs = ShareRecords::from(vec![rec(0, 1), rec(1, 2), rec(0, 3)]);
        assert_eq!(recs.count(), 3);
        assert_eq!(recs.len(), 3);
        assert_eq!(
            recs.partitions(),
            vec![
                crate::TopicPartition::new("t", 0),
                crate::TopicPartition::new("t", 1),
            ]
        );
        let p0: Vec<_> = recs
            .records(crate::TopicPartition::new("t", 0))
            .map(|r| r.offset)
            .collect();
        assert_eq!(p0, vec![1, 3]);
        let via_ref: Vec<_> = (&recs).into_iter().map(|r| r.offset).collect();
        assert_eq!(via_ref, vec![1, 2, 3]);
    }
}
