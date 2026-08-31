//! Share groups (KIP-932): queue-style consumption with per-record ack.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::watch;

use crate::consumer::{Consumer, ConsumerConfig};
use crate::error::{self, Error, Result};
use crate::group::{
    collect_topics, coord_roundtrip, discover_coord, filter_matching_topics, TopicMatch,
};
use crate::net::BrokerConn;
use crate::protocol::api_keys::{
    pick_version, SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_HEARTBEAT,
};
use crate::protocol::group::COORDINATOR_SHARE;
use crate::protocol::records::{
    write_java_optional, write_java_optional_bytes, write_java_record_headers, Header,
    TimestampType,
};
use crate::protocol::share::{
    decode_share_acknowledge_response, decode_share_fetch_response,
    decode_share_group_heartbeat_response, encode_share_acknowledge_request,
    encode_share_acknowledge_topics, encode_share_fetch_request,
    encode_share_group_heartbeat_request, AcknowledgementBatch, ShareAckTopic, ShareFetchPartition,
    ShareFetchTopic, ShareGroupHeartbeatRequest, ShareTopicPartitions, ACK_ACCEPT, ACK_REJECT,
    ACK_RELEASE,
};
use crate::Uuid;

pub use crate::protocol::share::{
    ACK_ACCEPT as SHARE_ACK_ACCEPT, ACK_REJECT as SHARE_ACK_REJECT,
    ACK_RELEASE as SHARE_ACK_RELEASE,
};

/// Share-group acknowledgement (Java `AcknowledgeType`, KIP-932).
///
/// [`Display`] is Java `AcknowledgeType.toString` (`accept`). Wire gap `0`
/// is not a Java `AcknowledgeType` ([`crate::protocol::share::ACK_GAP`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum AcknowledgeType {
    /// Java `AcknowledgeType.ACCEPT` (wire [`SHARE_ACK_ACCEPT`]).
    Accept = ACK_ACCEPT,
    /// Java `AcknowledgeType.RELEASE` (wire [`SHARE_ACK_RELEASE`]).
    Release = ACK_RELEASE,
    /// Java `AcknowledgeType.REJECT` (wire [`SHARE_ACK_REJECT`]).
    Reject = ACK_REJECT,
}

impl AcknowledgeType {
    /// Java `AcknowledgeType.id`.
    #[must_use]
    pub const fn id(self) -> i8 {
        self as i8
    }

    /// Java `AcknowledgeType.toString` (`accept`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Release => "release",
            Self::Reject => "reject",
        }
    }

    /// Java `AcknowledgeType.forId`. Unknown ids (including gap `0`) return
    /// `None`.
    #[must_use]
    pub const fn from_id(id: i8) -> Option<Self> {
        match id {
            ACK_ACCEPT => Some(Self::Accept),
            ACK_RELEASE => Some(Self::Release),
            ACK_REJECT => Some(Self::Reject),
            _ => None,
        }
    }
}

impl fmt::Display for AcknowledgeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Java `ShareRequestMetadata` (share session member id and epoch).
///
/// [`Display`] is Java `toString` (`(memberId=..., epoch=INITIAL)`). Member
/// id is Java `Uuid` ([`Uuid`] `Display` is base64url). ShareFetch /
/// ShareAcknowledge encode still take `member_id: &str` and
/// `share_session_epoch: i32`; [`ShareGroup`] uses [`Self::INITIAL_EPOCH`] /
/// [`Self::FINAL_EPOCH`] / [`Self::next_epoch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShareRequestMetadata {
    member_id: Uuid,
    epoch: i32,
}

impl ShareRequestMetadata {
    /// Java `ShareRequestMetadata.INITIAL_EPOCH`.
    pub const INITIAL_EPOCH: i32 = 0;
    /// Java `ShareRequestMetadata.FINAL_EPOCH`.
    pub const FINAL_EPOCH: i32 = -1;

    /// Java `ShareRequestMetadata(Uuid, int)`.
    #[must_use]
    pub const fn new(member_id: Uuid, epoch: i32) -> Self {
        Self { member_id, epoch }
    }

    /// Java `ShareRequestMetadata.initialEpoch`.
    #[must_use]
    pub const fn initial_epoch(member_id: Uuid) -> Self {
        Self::new(member_id, Self::INITIAL_EPOCH)
    }

    /// Java `ShareRequestMetadata.memberId`.
    #[must_use]
    pub const fn member_id(self) -> Uuid {
        self.member_id
    }

    /// Java `ShareRequestMetadata.epoch`.
    #[must_use]
    pub const fn epoch(self) -> i32 {
        self.epoch
    }

    /// Java `ShareRequestMetadata.isNewSession`.
    #[must_use]
    pub const fn is_new_session(self) -> bool {
        self.epoch == Self::INITIAL_EPOCH
    }

    /// Java `ShareRequestMetadata.isFull`.
    #[must_use]
    pub const fn is_full(self) -> bool {
        self.epoch == Self::INITIAL_EPOCH || self.epoch == Self::FINAL_EPOCH
    }

    /// Java `ShareRequestMetadata.isFinalEpoch`.
    #[must_use]
    pub const fn is_final_epoch(self) -> bool {
        self.epoch == Self::FINAL_EPOCH
    }

    /// Java `ShareRequestMetadata.nextEpoch(int)`.
    #[must_use]
    pub const fn next_epoch(prev_epoch: i32) -> i32 {
        if prev_epoch < 0 {
            Self::FINAL_EPOCH
        } else if prev_epoch == i32::MAX {
            1
        } else {
            prev_epoch + 1
        }
    }

    /// Java `ShareRequestMetadata.nextEpoch()` (instance).
    #[must_use]
    pub const fn next_epoch_metadata(self) -> Self {
        Self::new(self.member_id, Self::next_epoch(self.epoch))
    }

    /// Java `ShareRequestMetadata.nextCloseExistingAttemptNew`.
    #[must_use]
    pub const fn next_close_existing_attempt_new(self) -> Self {
        Self::new(self.member_id, Self::INITIAL_EPOCH)
    }

    /// Java `ShareRequestMetadata.finalEpoch`.
    #[must_use]
    pub const fn final_epoch(self) -> Self {
        Self::new(self.member_id, Self::FINAL_EPOCH)
    }
}

impl fmt::Display for ShareRequestMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(memberId={}, ", self.member_id)?;
        if self.epoch == Self::INITIAL_EPOCH {
            f.write_str("epoch=INITIAL)")
        } else if self.epoch == Self::FINAL_EPOCH {
            f.write_str("epoch=FINAL)")
        } else {
            write!(f, "epoch={})", self.epoch)
        }
    }
}

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
    /// Java `ConsumerRecord.timestampType`.
    pub timestamp_type: TimestampType,
    /// Optional key.
    pub key: Option<Bytes>,
    /// Optional value.
    pub value: Option<Bytes>,
    /// Record headers (Java `ConsumerRecord.headers`).
    pub headers: Vec<Header>,
    /// Broker delivery count for this share.
    pub delivery_count: i16,
    /// Partition leader epoch from the record batch, or `None` when `-1`.
    pub leader_epoch: Option<i32>,
}

impl ShareRecord {
    /// Java `ConsumerRecord.NO_TIMESTAMP`.
    pub const NO_TIMESTAMP: i64 = crate::RecordBatch::NO_TIMESTAMP;
    /// Java `ConsumerRecord.NULL_SIZE`.
    pub const NULL_SIZE: i32 = -1;

    /// Topic and partition of this record.
    #[must_use]
    pub fn topic_partition(&self) -> crate::TopicPartition {
        crate::TopicPartition::new(self.topic.clone(), self.partition)
    }

    /// Java `ConsumerRecord.topic`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Java `ConsumerRecord.partition`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Java `ConsumerRecord.offset`.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Java `ConsumerRecord.timestamp`.
    #[must_use]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Java `ConsumerRecord.timestampType`.
    #[must_use]
    pub fn timestamp_type(&self) -> TimestampType {
        self.timestamp_type
    }

    /// Java `ConsumerRecord.key`.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Java `ConsumerRecord.value`.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Java `ConsumerRecord.headers`.
    #[must_use]
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    /// Java `Headers.lastHeader`.
    #[must_use]
    pub fn last_header(&self, key: &str) -> Option<&Header> {
        Header::last_in(&self.headers, key)
    }

    /// Java `Headers.headers(String)`.
    pub fn headers_for_key<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a Header> + 'a {
        Header::for_key(&self.headers, key)
    }

    /// Broker delivery count for this share (KIP-932).
    #[must_use]
    pub fn delivery_count(&self) -> i16 {
        self.delivery_count
    }

    /// Java `ConsumerRecord.leaderEpoch`.
    #[must_use]
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Serialized key size in bytes, or [`Self::NULL_SIZE`] if there is no key (Java `serializedKeySize`).
    #[must_use]
    pub fn serialized_key_size(&self) -> i32 {
        self.key
            .as_ref()
            .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
            .unwrap_or(Self::NULL_SIZE)
    }

    /// Serialized value size in bytes, or [`Self::NULL_SIZE`] if there is no value (Java `serializedValueSize`).
    #[must_use]
    pub fn serialized_value_size(&self) -> i32 {
        self.value
            .as_ref()
            .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
            .unwrap_or(Self::NULL_SIZE)
    }
}

impl fmt::Display for ShareRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConsumerRecord(topic = {}, partition = {}, leaderEpoch = ",
            self.topic, self.partition
        )?;
        write_java_optional(f, self.leader_epoch)?;
        write!(
            f,
            ", offset = {}, {} = {}, deliveryCount = {}, serialized key size = {}, serialized value size = {}, headers = ",
            self.offset,
            self.timestamp_type,
            self.timestamp,
            self.delivery_count,
            self.serialized_key_size(),
            self.serialized_value_size()
        )?;
        write_java_record_headers(f, &self.headers, true)?;
        f.write_str(", key = ")?;
        write_java_optional_bytes(f, self.key.as_deref())?;
        f.write_str(", value = ")?;
        write_java_optional_bytes(f, self.value.as_deref())?;
        f.write_str(")")
    }
}

/// Records from one share poll (Java `ConsumerRecords` for KIP-932).
///
/// Indexes and iterates like a slice of [`ShareRecord`]. [`Self::empty`] /
/// [`Self::is_empty`] / [`Self::partitions`] / [`Self::records`] /
/// [`Self::next_offsets`] match Java `empty` / `isEmpty` / `partitions` /
/// `records(TopicPartition)` / `nextOffsets`.
#[derive(Debug, Clone, Default)]
pub struct ShareRecords {
    records: Vec<ShareRecord>,
}

impl ShareRecords {
    /// Java `ConsumerRecords.empty`.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Java `ConsumerRecords.isEmpty`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

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

    /// Records for this partition (Java `records(TopicPartition)`).
    pub fn records(
        &self,
        partition: impl Into<crate::TopicPartition>,
    ) -> impl Iterator<Item = &ShareRecord> {
        let tp = partition.into();
        self.records
            .iter()
            .filter(move |r| r.topic == tp.topic && r.partition == tp.partition)
    }

    /// Records for this topic name (Java `records(String)`).
    pub fn records_for_topic<'a>(
        &'a self,
        topic: &'a str,
    ) -> impl Iterator<Item = &'a ShareRecord> {
        self.records.iter().filter(move |r| r.topic == topic)
    }

    /// Next offset to consume per partition (Java `nextOffsets`).
    ///
    /// For each partition that has at least one record, this is the last
    /// record's offset plus one, with that record's leader epoch and
    /// [`crate::OffsetAndMetadata::NO_METADATA`]. Partitions appear in
    /// first-seen order.
    #[must_use]
    pub fn next_offsets(&self) -> Vec<(crate::TopicPartition, crate::OffsetAndMetadata)> {
        let mut last = HashMap::new();
        let mut order = Vec::new();
        for rec in &self.records {
            let tp = rec.topic_partition();
            if last.insert(tp.clone(), rec).is_none() {
                order.push(tp);
            }
        }
        order
            .into_iter()
            .filter_map(|tp| {
                last.remove(&tp).map(|rec| {
                    let mut md = crate::OffsetAndMetadata::new(rec.offset.saturating_add(1));
                    if let Some(epoch) = rec.leader_epoch {
                        md = md.with_leader_epoch(epoch);
                    }
                    (tp, md)
                })
            })
            .collect()
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

/// KIP-932 share group member (`ShareGroupHeartbeat` v0–v1 / ShareFetch v0–v1 / ShareAcknowledge v0–v1).
pub struct ShareGroup {
    consumer: Consumer,
    coord: BrokerConn,
    cfg: ConsumerConfig,
    group_id: String,
    member_id: String,
    member_epoch: i32,
    topics: Vec<String>,
    /// Java `subscribe(Pattern)`: re-list cluster topics on poll.
    topic_match: Option<TopicMatch>,
    last_match_refresh: Instant,
    assigned: Vec<(String, i32)>,
    topic_ids: HashMap<String, [u8; 16]>,
    /// Share session epoch per share-partition leader (KIP-932).
    share_epochs: HashMap<i32, i32>,
    /// ShareFetch version negotiated from ApiVersions (`-1` unset). `0` is a
    /// spoken version, so it cannot mean unset.
    share_fetch_version: i16,
    /// ShareAcknowledge version negotiated from ApiVersions (`-1` unset). `0`
    /// is a spoken version, so it cannot mean unset.
    share_acknowledge_version: i16,
    hb_err: Arc<AtomicI16>,
    hb_epoch: Arc<AtomicI32>,
    hb_stop: watch::Sender<bool>,
    fetch_rounds: u64,
    records_fetched: u64,
    bytes_fetched: u64,
    fetch_errors: u64,
    records_acknowledged: u64,
    fetch_latency: crate::metrics::LatencyTracker,
    topic_metrics: HashMap<String, crate::metrics::FetchTopicTracker>,
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

fn spoken_share_acknowledge(version: i16) -> Result<i16> {
    if (0..=1).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support ShareAcknowledge v0-1".into(),
        ))
    }
}

fn spoken_share_fetch(version: i16) -> Result<i16> {
    if (0..=1).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support ShareFetch v0-1".into(),
        ))
    }
}

fn spoken_share_group_heartbeat(version: i16) -> Result<i16> {
    if (0..=1).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support ShareGroupHeartbeat v0-1".into(),
        ))
    }
}

impl ShareGroup {
    /// Join a share group. One topic.
    ///
    /// An empty `group_id` is Java `InvalidGroupIdException`
    /// (`You must provide a valid group.id in the consumer configuration.`).
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
        reject_java_share_group_id(&group_id)?;
        let topics = collect_topics(topics)?;
        Self::join_list(cfg, group_id, topics, None).await
    }

    /// Join a share group with a topic predicate (Java `subscribe(Pattern)`).
    ///
    /// Cluster topics for which `matches` is true become the subscription.
    /// Names starting with `__` are skipped. [`Self::poll`] re-lists Metadata
    /// when [`ConsumerConfig::metadata_max_age`] has elapsed (every poll when
    /// that age is zero).
    pub async fn join_matching(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::join_list(cfg, group_id.into(), Vec::new(), Some(Arc::new(matches))).await
    }

    async fn join_list(
        cfg: ConsumerConfig,
        group_id: String,
        topics: Vec<String>,
        topic_match: Option<TopicMatch>,
    ) -> Result<Self> {
        reject_java_share_group_id(&group_id)?;
        let mut cfg = cfg;
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        let consumer = Consumer::new(cfg.clone()).await?;
        let share_fetch_version = consumer
            .versions()
            .get(&SHARE_FETCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| Error::Unsupported("broker does not support ShareFetch v0-1".into()))?;
        let share_acknowledge_version = consumer
            .versions()
            .get(&SHARE_ACKNOWLEDGE)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support ShareAcknowledge v0-1".into())
            })?;
        let coord = discover_coord(&cfg, &group_id, COORDINATOR_SHARE).await?;
        let member_id = new_member_id()?;
        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_epoch = Arc::new(AtomicI32::new(
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
        ));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            consumer,
            coord,
            cfg: cfg.clone(),
            group_id,
            member_id,
            member_epoch: ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            topics,
            topic_match,
            last_match_refresh: Instant::now(),
            assigned: Vec::new(),
            topic_ids: HashMap::new(),
            share_epochs: HashMap::new(),
            share_fetch_version,
            share_acknowledge_version,
            hb_err,
            hb_epoch,
            hb_stop,
            fetch_rounds: 0,
            records_fetched: 0,
            bytes_fetched: 0,
            fetch_errors: 0,
            records_acknowledged: 0,
            fetch_latency: crate::metrics::LatencyTracker::new(),
            topic_metrics: HashMap::new(),
        };
        if g.topic_match.is_some() {
            g.topics = g.matching_topic_names().await?;
            g.last_match_refresh = Instant::now();
        }
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
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::list_topics_timeout`].
    pub async fn list_topics(&mut self) -> Result<Vec<crate::PartitionInfo>> {
        self.consumer.list_topics().await
    }

    /// [`Self::list_topics`] with a one-shot timeout (Java `listTopics(Duration)`).
    pub async fn list_topics_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<crate::PartitionInfo>> {
        self.consumer.list_topics_timeout(timeout).await
    }

    /// ShareFetch / ShareAcknowledge counters and poll latency since join
    /// (min/mean/max and p50/p99).
    ///
    /// [`ShareMetrics::topics`] is one row per topic that returned at least
    /// one record.
    #[must_use]
    pub fn metrics(&self) -> crate::ShareMetrics {
        crate::ShareMetrics {
            fetch_rounds: self.fetch_rounds,
            records_fetched: self.records_fetched,
            bytes_fetched: self.bytes_fetched,
            fetch_errors: self.fetch_errors,
            records_acknowledged: self.records_acknowledged,
            fetch_latency: self.fetch_latency.snapshot(),
            topics: crate::metrics::snapshot_fetch_topics(&self.topic_metrics),
        }
    }

    /// Java `clientInstanceId` (KIP-714). Delegates to [`crate::Consumer::client_instance_id`].
    ///
    /// Returns [`crate::Uuid`] (Java `Uuid`).
    pub async fn client_instance_id(&mut self) -> Result<crate::Uuid> {
        self.consumer.client_instance_id().await
    }

    /// [`Self::client_instance_id`] with a one-shot timeout (Java
    /// `clientInstanceId(Duration)`).
    pub async fn client_instance_id_timeout(&mut self, timeout: Duration) -> Result<crate::Uuid> {
        self.consumer.client_instance_id_timeout(timeout).await
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
        let timeout = self.cfg.request_timeout;
        self.consumer.refresh_topics(&self.topics).await?;
        let version = spoken_share_group_heartbeat(self.coord.share_group_heartbeat_version)?;
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            subscribed_topic_names: Some(self.topics.clone()),
        };
        let body = self
            .coord
            .roundtrip(
                SHARE_GROUP_HEARTBEAT,
                version,
                |buf| encode_share_group_heartbeat_request(buf, version, &req),
                timeout,
            )
            .await?;
        let resp = decode_share_group_heartbeat_response(&mut body.clone(), version)?;
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
    ///
    /// Not subscribed is Java `IllegalStateException` (`Consumer is not
    /// subscribed to any topics.`).
    pub async fn poll(&mut self) -> Result<ShareRecords> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        if self.topics.is_empty() && self.topic_match.is_none() {
            return Err(reject_java_share_not_subscribed());
        }
        self.maybe_refresh_matching().await?;
        let hb = self.hb_err.load(Ordering::SeqCst);
        if hb != 0 {
            return Err(Error::broker(hb, "ShareGroupHeartbeat"));
        }
        let started = Instant::now();
        let deadline = started + self.cfg.request_timeout;
        let mut attempt = 0u32;
        loop {
            match self.poll_leaders().await {
                Ok(recs) => {
                    let elapsed = started.elapsed();
                    self.fetch_latency.record(elapsed);
                    self.fetch_rounds = self.fetch_rounds.saturating_add(1);
                    let n = u64::try_from(recs.len()).unwrap_or(u64::MAX);
                    self.records_fetched = self.records_fetched.saturating_add(n);
                    let bytes = recs
                        .iter()
                        .map(share_record_bytes)
                        .fold(0, u64::saturating_add);
                    self.bytes_fetched = self.bytes_fetched.saturating_add(bytes);
                    crate::metrics::accumulate_fetch_topics(
                        &mut self.topic_metrics,
                        recs.iter()
                            .map(|r| (r.topic.as_str(), share_record_bytes(r))),
                        elapsed,
                    );
                    return Ok(ShareRecords::from(recs));
                }
                Err(e) if share_leader_retriable(&e) => {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    self.consumer.sleep_retry_backoff(attempt, deadline).await?;
                    attempt = attempt.saturating_add(1);
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
    ///
    /// Java `ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType.ACCEPT)`.
    /// Called before [`Self::poll`] is Java `IllegalStateException`
    /// (`Acknowledge called before poll.`).
    pub async fn accept(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, AcknowledgeType::Accept).await
    }

    /// Return records to the share (`RELEASE`).
    ///
    /// Java `ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType.RELEASE)`.
    /// Called before [`Self::poll`] is the same Java `IllegalStateException`
    /// as [`Self::acknowledge`].
    pub async fn release(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, AcknowledgeType::Release).await
    }

    /// Reject records (`REJECT`, KIP-932).
    ///
    /// Java `ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType.REJECT)`.
    /// Called before [`Self::poll`] is the same Java `IllegalStateException`
    /// as [`Self::acknowledge`].
    pub async fn reject(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, AcknowledgeType::Reject).await
    }

    /// Java `ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType)`.
    ///
    /// Called before [`Self::poll`] is Java `IllegalStateException`
    /// (`Acknowledge called before poll.`).
    pub async fn acknowledge(&mut self, recs: &[ShareRecord], ack: AcknowledgeType) -> Result<()> {
        self.send_acknowledgements(recs, ack.id()).await
    }

    fn session_epoch(&self, node: i32) -> i32 {
        self.share_epochs
            .get(&node)
            .copied()
            .unwrap_or(ShareRequestMetadata::INITIAL_EPOCH)
    }

    fn advance_node_epoch(&mut self, node: i32) {
        let next = ShareRequestMetadata::next_epoch(self.session_epoch(node));
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
        let timeout = self.cfg.request_timeout;
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
                            partition_max_bytes: 1_048_576,
                            acknowledgements: Vec::new(),
                        })
                        .collect(),
                })
                .collect();
            let version = spoken_share_fetch(self.share_fetch_version)?;
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_FETCH,
                    version,
                    |buf| {
                        encode_share_fetch_request(
                            buf,
                            version,
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
            let (fetched, .., error_code) = match decode_share_fetch_response(&mut body, version) {
                Ok(decoded) => decoded,
                Err(e) => {
                    if share_session_reset(&e) || share_leader_retriable(&e) {
                        self.reset_node_session(node);
                    }
                    return Err(e);
                }
            };
            if error_code != 0 {
                let e = Error::broker(error_code, "ShareFetch");
                if share_session_reset(&e) || share_leader_retriable(&e) {
                    self.reset_node_session(node);
                }
                return Err(e);
            }
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
                        let timestamp_type = batch.timestamp_type();
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
                                timestamp_type,
                                key: rec.key,
                                value: rec.value,
                                headers: rec.headers,
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
    /// [`ConsumerConfig::max_wait_ms`] is restored afterwards. Not subscribed
    /// is the same Java `IllegalStateException` as [`Self::poll`].
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
            self.topic_match = None;
            self.topics.clear();
            self.assigned.clear();
            self.topic_ids.clear();
            self.share_epochs.clear();
            return Ok(());
        }
        self.topic_match = None;
        self.hb_stop.send(true).unwrap_or(());
        self.close_share_session().await?;
        self.leave_coordinator().await?;
        self.assigned.clear();
        self.topics.clear();
        self.topic_ids.clear();
        self.share_epochs.clear();
        self.member_id.clear();
        self.member_epoch = ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH;
        self.hb_epoch.store(
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            Ordering::SeqCst,
        );
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
        self.topic_match = None;
        self.apply_topics(topics).await
    }

    /// [`Self::subscribe`] with a topic predicate (Java `subscribe(Pattern)`).
    ///
    /// Names starting with `__` are skipped. [`Self::poll`] re-lists Metadata
    /// when [`ConsumerConfig::metadata_max_age`] has elapsed. [`Self::subscribe`]
    /// with an explicit list drops the predicate.
    pub async fn subscribe_matching(
        &mut self,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Result<()> {
        self.topic_match = Some(Arc::new(matches));
        let topics = self.matching_topic_names().await?;
        self.last_match_refresh = Instant::now();
        self.apply_topics(topics).await
    }

    async fn apply_topics(&mut self, topics: Vec<String>) -> Result<()> {
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

    async fn matching_topic_names(&mut self) -> Result<Vec<String>> {
        let Some(pred) = self.topic_match.clone() else {
            return Ok(self.topics.clone());
        };
        let infos = self.consumer.list_topics().await?;
        Ok(filter_matching_topics(
            infos.iter().map(|i| i.topic.as_str()),
            |n| pred(n),
        ))
    }

    async fn maybe_refresh_matching(&mut self) -> Result<()> {
        if self.topic_match.is_none() {
            return Ok(());
        }
        let age = self.cfg.metadata_max_age;
        if !age.is_zero() && self.last_match_refresh.elapsed() < age {
            return Ok(());
        }
        let topics = self.matching_topic_names().await?;
        self.last_match_refresh = Instant::now();
        self.apply_topics(topics).await
    }

    fn name_for_topic_id(&self, id: [u8; 16]) -> String {
        self.topic_ids
            .iter()
            .find(|(_, v)| **v == id)
            .map(|(n, _)| n.clone())
            .or_else(|| self.topics.first().cloned())
            .unwrap_or_default()
    }

    async fn send_acknowledgements(&mut self, recs: &[ShareRecord], ack: i8) -> Result<()> {
        if recs.is_empty() {
            return Ok(());
        }
        let partitions = acknowledgement_batches(recs, ack);
        if partitions.is_empty() {
            return Ok(());
        }
        let deadline = Instant::now() + self.cfg.request_timeout;
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
        let timeout = self.cfg.request_timeout;
        for (node, node_tps) in by_leader {
            let epoch = self.session_epoch(node);
            if epoch == ShareRequestMetadata::INITIAL_EPOCH {
                return Err(reject_java_acknowledge_before_poll());
            }
            if epoch == ShareRequestMetadata::FINAL_EPOCH {
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
            let version = spoken_share_acknowledge(self.share_acknowledge_version)?;
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_ACKNOWLEDGE,
                    version,
                    |buf| {
                        encode_share_acknowledge_topics(
                            buf,
                            version,
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
            let err = decode_share_acknowledge_response(&mut body.clone(), version)?;
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
            .filter(|(_, e)| {
                **e != ShareRequestMetadata::INITIAL_EPOCH
                    && **e != ShareRequestMetadata::FINAL_EPOCH
            })
            .map(|(n, _)| *n)
            .collect();
        if open.is_empty() {
            return Ok(());
        }
        let timeout = self.cfg.request_timeout;
        let version = spoken_share_acknowledge(self.share_acknowledge_version)?;
        let mut last = Ok(());
        for node in open {
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_ACKNOWLEDGE,
                    version,
                    |buf| {
                        encode_share_acknowledge_request(
                            buf,
                            version,
                            &self.group_id,
                            &self.member_id,
                            ShareRequestMetadata::FINAL_EPOCH,
                            [0u8; 16],
                            &[],
                        )
                    },
                    timeout,
                )
                .await;
            let err = match body {
                Ok(body) => decode_share_acknowledge_response(&mut body.clone(), version)?,
                Err(_) => error::SHARE_SESSION_NOT_FOUND,
            };
            let _ = self.share_epochs.remove(&node);
            if err != 0 && err != error::SHARE_SESSION_NOT_FOUND {
                last = Err(Error::broker(err, "ShareAcknowledge close"));
            }
        }
        last
    }

    /// Leave the share group ([`ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH`]).
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
        let timeout = self.cfg.request_timeout;
        let version = spoken_share_group_heartbeat(self.coord.share_group_heartbeat_version)?;
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH,
            subscribed_topic_names: None,
        };
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_SHARE,
            SHARE_GROUP_HEARTBEAT,
            version,
            |buf| encode_share_group_heartbeat_request(buf, version, &req),
            timeout,
        )
        .await?;
        let resp = decode_share_group_heartbeat_response(&mut body.clone(), version)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ShareGroupHeartbeat leave"));
        }
        Ok(())
    }

    /// Leave the share group. Same as [`Self::leave`].
    pub async fn close(self) -> Result<()> {
        self.leave().await
    }

    /// Leave the share group, waiting up to `timeout` (Java `close(Duration)`).
    ///
    /// [`Self::leave`] / [`Self::close`] wait up to
    /// [`crate::ConsumerConfig::request_timeout`] for the coordinator. A
    /// shorter `timeout` returns [`Error::Timeout`] if leave does not finish
    /// in time.
    pub async fn close_timeout(self, timeout: Duration) -> Result<()> {
        match tokio::time::timeout(timeout, self.leave()).await {
            Ok(out) => out,
            Err(_) => Err(Error::Timeout),
        }
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
                        if conn
                            .as_ref()
                            .is_some_and(|c| c.idle_expired(cfg.connections_max_idle))
                        {
                            conn = None;
                        }
                        if conn.is_none() {
                            conn = discover_coord(&cfg, &group_id, COORDINATOR_SHARE).await.ok();
                        }
                        let Some(c) = conn.as_mut() else {
                            continue;
                        };
                        let epoch = hb_epoch.load(Ordering::SeqCst);
                        let Ok(version) =
                            spoken_share_group_heartbeat(c.share_group_heartbeat_version)
                        else {
                            continue;
                        };
                        let req = ShareGroupHeartbeatRequest {
                            group_id: group_id.clone(),
                            member_id: member_id.clone(),
                            member_epoch: epoch,
                            subscribed_topic_names: None,
                        };
                        let res = c
                            .roundtrip(
                                SHARE_GROUP_HEARTBEAT,
                                version,
                                |buf| encode_share_group_heartbeat_request(buf, version, &req),
                                cfg.request_timeout,
                            )
                            .await;
                        match res {
                            Ok(body) => {
                                if let Ok(resp) = decode_share_group_heartbeat_response(
                                    &mut body.clone(),
                                    version,
                                ) {
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

/// Java `ShareConsumerImpl.maybeThrowInvalidGroupIdException`.
fn reject_java_share_group_id(group_id: &str) -> Result<()> {
    if group_id.is_empty() {
        return Err(Error::protocol(
            "You must provide a valid group.id in the consumer configuration.",
        ));
    }
    Ok(())
}

/// Java `ShareConsumerImpl.poll` when `hasNoSubscriptionOrUserAssignment`.
fn reject_java_share_not_subscribed() -> Error {
    Error::protocol("Consumer is not subscribed to any topics.")
}

/// Java `ShareConsumerImpl.ensureExplicitAcknowledgement` when the mode is
/// `UNKNOWN` (acknowledge before the first poll).
fn reject_java_acknowledge_before_poll() -> Error {
    Error::protocol("Acknowledge called before poll.")
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
            timestamp_type: TimestampType::CreateTime,
            key: None,
            value: None,
            headers: Vec::new(),
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
    fn acknowledge_type_matches_java() {
        assert_eq!(AcknowledgeType::Accept.id(), SHARE_ACK_ACCEPT);
        assert_eq!(AcknowledgeType::Release.id(), SHARE_ACK_RELEASE);
        assert_eq!(AcknowledgeType::Reject.id(), SHARE_ACK_REJECT);
        assert_eq!(AcknowledgeType::Accept.id(), 1);
        assert_eq!(AcknowledgeType::Release.id(), 2);
        assert_eq!(AcknowledgeType::Reject.id(), 3);
        assert_eq!(
            AcknowledgeType::from_id(SHARE_ACK_ACCEPT),
            Some(AcknowledgeType::Accept)
        );
        assert_eq!(
            AcknowledgeType::from_id(SHARE_ACK_RELEASE),
            Some(AcknowledgeType::Release)
        );
        assert_eq!(
            AcknowledgeType::from_id(SHARE_ACK_REJECT),
            Some(AcknowledgeType::Reject)
        );
        assert_eq!(AcknowledgeType::from_id(0), None);
        assert_eq!(AcknowledgeType::from_id(4), None);
        assert_eq!(AcknowledgeType::Accept.to_string(), "accept");
        assert_eq!(AcknowledgeType::Release.to_string(), "release");
        assert_eq!(AcknowledgeType::Reject.to_string(), "reject");
        assert_eq!(AcknowledgeType::Accept.as_str(), "accept");
    }

    #[test]
    fn share_request_metadata_matches_java() {
        let id = Uuid::ONE_UUID;
        assert_eq!(ShareRequestMetadata::INITIAL_EPOCH, 0);
        assert_eq!(ShareRequestMetadata::FINAL_EPOCH, -1);
        let initial = ShareRequestMetadata::initial_epoch(id);
        assert_eq!(initial.member_id(), id);
        assert_eq!(initial.epoch(), ShareRequestMetadata::INITIAL_EPOCH);
        assert!(initial.is_new_session());
        assert!(initial.is_full());
        assert!(!initial.is_final_epoch());
        let fin = initial.final_epoch();
        assert_eq!(fin.member_id(), id);
        assert_eq!(fin.epoch(), ShareRequestMetadata::FINAL_EPOCH);
        assert!(fin.is_final_epoch());
        assert!(fin.is_full());
        assert!(!fin.is_new_session());
        let mid = ShareRequestMetadata::new(id, 3);
        assert!(!mid.is_full());
        assert!(!mid.is_new_session());
        assert!(!mid.is_final_epoch());
        assert_eq!(
            ShareRequestMetadata::next_epoch(-1),
            ShareRequestMetadata::FINAL_EPOCH
        );
        assert_eq!(
            ShareRequestMetadata::next_epoch(-2),
            ShareRequestMetadata::FINAL_EPOCH
        );
        assert_eq!(
            ShareRequestMetadata::next_epoch(ShareRequestMetadata::INITIAL_EPOCH),
            1
        );
        assert_eq!(ShareRequestMetadata::next_epoch(i32::MAX), 1);
        assert_eq!(
            initial.next_epoch_metadata(),
            ShareRequestMetadata::new(id, 1)
        );
        assert_eq!(
            ShareRequestMetadata::new(id, i32::MAX).next_epoch_metadata(),
            ShareRequestMetadata::new(id, 1)
        );
        assert_eq!(
            fin.next_epoch_metadata(),
            ShareRequestMetadata::new(id, ShareRequestMetadata::FINAL_EPOCH)
        );
        assert_eq!(
            mid.next_close_existing_attempt_new(),
            ShareRequestMetadata::initial_epoch(id)
        );
        assert_eq!(
            initial.to_string(),
            format!("(memberId={id}, epoch=INITIAL)")
        );
        assert_eq!(fin.to_string(), format!("(memberId={id}, epoch=FINAL)"));
        assert_eq!(mid.to_string(), format!("(memberId={id}, epoch=3)"));
        assert_eq!(
            ShareRequestMetadata::initial_epoch(Uuid::ZERO_UUID).to_string(),
            format!("(memberId={}, epoch=INITIAL)", Uuid::ZERO_UUID)
        );
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
        let mut last_p0 = rec(0, 3);
        last_p0.leader_epoch = Some(7);
        let recs = ShareRecords::from(vec![rec(0, 1), rec(1, 2), last_p0]);
        assert_eq!(recs.count(), 3);
        assert_eq!(recs.len(), 3);
        assert!(!recs.is_empty());
        assert!(ShareRecords::empty().is_empty());
        assert!(ShareRecords::empty().next_offsets().is_empty());
        assert_eq!(recs.records_for_topic("t").count(), 3);
        assert_eq!(recs.records_for_topic("missing").count(), 0);
        assert_eq!(
            recs.partitions(),
            vec![
                crate::TopicPartition::new("t", 0),
                crate::TopicPartition::new("t", 1),
            ]
        );
        let p0: Vec<_> = recs
            .records(crate::TopicPartition::new("t", 0))
            .map(|r| r.offset())
            .collect();
        assert_eq!(p0, vec![1, 3]);
        assert_eq!(
            recs.next_offsets(),
            vec![
                (
                    crate::TopicPartition::new("t", 0),
                    crate::OffsetAndMetadata::new(4).with_leader_epoch(7)
                ),
                (
                    crate::TopicPartition::new("t", 1),
                    crate::OffsetAndMetadata::new(3),
                ),
            ]
        );
        let via_ref: Vec<_> = (&recs).into_iter().map(|r| r.offset()).collect();
        assert_eq!(via_ref, vec![1, 2, 3]);
        let first = &recs[0];
        assert_eq!(first.topic(), "t");
        assert_eq!(first.partition(), 0);
        assert_eq!(first.offset(), 1);
        assert_eq!(first.timestamp(), 0);
        assert_eq!(first.timestamp_type(), TimestampType::CreateTime);
        assert!(first.key().is_none());
        assert!(first.value().is_none());
        assert!(first.headers().is_empty());
        assert!(first.last_header("k").is_none());
        assert_eq!(first.delivery_count(), 1);
        assert!(first.leader_epoch().is_none());
        assert_eq!(first.serialized_key_size(), ShareRecord::NULL_SIZE);
        assert_eq!(first.serialized_value_size(), ShareRecord::NULL_SIZE);
        assert_eq!(ShareRecord::NO_TIMESTAMP, crate::RecordBatch::NO_TIMESTAMP);
        assert_eq!(ShareRecord::NULL_SIZE, -1);
        assert_eq!(
            first.to_string(),
            "ConsumerRecord(topic = t, partition = 0, leaderEpoch = null, offset = 1, CreateTime = 0, deliveryCount = 1, serialized key size = -1, serialized value size = -1, headers = RecordHeaders(headers = [], isReadOnly = true), key = null, value = null)"
        );
    }
}
