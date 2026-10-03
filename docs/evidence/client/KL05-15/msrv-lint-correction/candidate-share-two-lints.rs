//! Share groups (KIP-932): queue-style consumption with per-record ack.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::sync::{watch, Notify};

use crate::consumer::{Consumer, ConsumerConfig};
use crate::error::{self, Error, Result};
use crate::group::{
    collect_topics, coord_roundtrip, discover_coord, filter_matching_topics, TopicMatch,
};
use crate::net::BrokerConn;
use crate::protocol::api_keys::{
    pick_version, SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_HEARTBEAT,
};
use crate::protocol::group::COORDINATOR_GROUP;
use crate::protocol::records::{
    write_java_optional, write_java_optional_bytes, write_java_record_headers, Header,
    TimestampType,
};
use crate::protocol::share::{
    decode_share_acknowledge_response, decode_share_acknowledge_topics_response_with_lock_timeout,
    decode_share_fetch_response_with_budget, decode_share_group_heartbeat_response,
    encode_share_acknowledge_request, encode_share_acknowledge_topics,
    encode_share_fetch_request_with_options, encode_share_group_heartbeat_request,
    AcknowledgementBatch, ShareAckTopic, ShareFetchPartition, ShareFetchRequestOptions,
    ShareFetchTopic, ShareGroupHeartbeatRequest, ShareTopicPartitions, ACK_ACCEPT, ACK_REJECT,
    ACK_RELEASE, ACK_RENEW,
};
use crate::Uuid;

pub use crate::protocol::share::{
    ACK_ACCEPT as SHARE_ACK_ACCEPT, ACK_REJECT as SHARE_ACK_REJECT,
    ACK_RELEASE as SHARE_ACK_RELEASE, ACK_RENEW as SHARE_ACK_RENEW,
};

/// Share-group acknowledgement (Java `AcknowledgeType`, KIP-932).
///
/// [`std::fmt::Display`] is Java `AcknowledgeType.toString` (`accept`). Wire gap `0`
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
    /// Renew the acquisition lock (share protocol v2, KIP-1222).
    Renew = ACK_RENEW,
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
            Self::Renew => "renew",
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
            ACK_RENEW => Some(Self::Renew),
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
/// [`std::fmt::Display`] is Java `toString` (`(memberId=..., epoch=INITIAL)`). Member
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

const SHARE_FETCH_RUNTIME_MAX_VERSION: i16 = 2;
const SHARE_ACKNOWLEDGE_RUNTIME_MAX_VERSION: i16 = 2;

type AcquisitionKey = (String, i32, i64);

struct Acquisition {
    node: i32,
    delivery_count: i16,
    expires_at: Option<Instant>,
}

struct ShareNodeConnection {
    conn: BrokerConn,
    fetch_version: i16,
    acknowledge_version: i16,
}

/// Kafka v2 share acquisition behavior (KIP-1206).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum ShareAcquireMode {
    /// Permit complete batch boundaries to exceed the requested record limit.
    #[default]
    BatchOptimized = 0,
    /// Acquire at most the requested number of records. Requires ShareFetch v2.
    RecordLimit = 1,
}

/// KIP-932 share group member (`ShareGroupHeartbeat` v0–v1 / ShareFetch v0–v2 / ShareAcknowledge v0–v2).
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
    share_conns: HashMap<i32, ShareNodeConnection>,
    share_addresses: HashMap<i32, String>,
    acquisitions: HashMap<AcquisitionKey, Acquisition>,
    pending_delivery: VecDeque<ShareRecord>,
    acquire_mode: ShareAcquireMode,
    acquisition_lock_timeout_ms: Option<i32>,
    hb_err: Arc<AtomicI16>,
    hb_epoch: Arc<AtomicI32>,
    /// Pending assignment from the background heartbeat task.
    hb_assignment: Arc<Mutex<Option<Vec<ShareTopicPartitions>>>>,
    hb_interval_ms: Arc<AtomicI32>,
    hb_deadline: Arc<Mutex<Option<Instant>>>,
    hb_wake: Arc<Notify>,
    hb_stop: watch::Sender<bool>,
    fetch_rounds: u64,
    records_fetched: u64,
    bytes_fetched: u64,
    fetch_errors: u64,
    records_acknowledged: u64,
    fetch_latency: crate::metrics::LatencyTracker,
    topic_metrics: HashMap<String, crate::metrics::FetchTopicTracker>,
}

fn spoken_share_acknowledge(version: i16) -> Result<i16> {
    if (0..=SHARE_ACKNOWLEDGE_RUNTIME_MAX_VERSION).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support ShareAcknowledge v0-2".into(),
        ))
    }
}

fn spoken_share_fetch(version: i16) -> Result<i16> {
    if (0..=SHARE_FETCH_RUNTIME_MAX_VERSION).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support ShareFetch v0-2".into(),
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
        if cfg.max_poll_records == Some(0) {
            return Err(Error::protocol(
                "max_poll_records must be positive for share groups",
            ));
        }
        let mut cfg = cfg;
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        let consumer = Consumer::new(cfg.clone()).await?;
        // Membership uses the group coordinator (Java ShareConsumer / DescribeShareGroups).
        // CoordinatorType.SHARE is only for share-partition state keys
        // (`groupId:topicId:partition`), not the group id alone (KIP-932).

        let discovery_deadline = Instant::now() + cfg.request_timeout;
        let discovery_backoff = cfg
            .retry_backoff
            .min(cfg.retry_backoff_max)
            .max(Duration::from_millis(1));
        let coord = loop {
            let remaining = discovery_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }
            let mut attempt_cfg = cfg.clone();
            attempt_cfg.request_timeout = remaining;
            attempt_cfg.connect_timeout = cfg.connect_timeout.min(remaining);
            let result = tokio::time::timeout_at(
                discovery_deadline.into(),
                discover_coord(&attempt_cfg, &group_id, COORDINATOR_GROUP),
            )
            .await
            .map_err(|_| Error::Timeout)?;
            match result {
                Ok(coord) => break coord,
                Err(error)
                    if error
                        .broker_code()
                        .is_some_and(error::coordinator_retriable) =>
                {
                    let remaining = discovery_deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(Error::Timeout);
                    }
                    tokio::time::sleep(discovery_backoff.min(remaining)).await;
                }
                Err(error) => return Err(error),
            }
        };

        // Kafka 4.1 ShareGroupHeartbeat requires a client-generated member id
        // for the process lifetime. ShareFetch parses it as a base64url Uuid.
        let member_id = Uuid::random_uuid().to_string();
        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_epoch = Arc::new(AtomicI32::new(
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
        ));
        let hb_assignment = Arc::new(Mutex::new(None));
        let hb_interval_ms = Arc::new(AtomicI32::new(0));
        let hb_deadline = Arc::new(Mutex::new(None));
        let hb_wake = Arc::new(Notify::new());
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
            share_conns: HashMap::new(),
            share_addresses: HashMap::new(),
            acquisitions: HashMap::new(),
            pending_delivery: VecDeque::new(),
            acquire_mode: ShareAcquireMode::BatchOptimized,
            acquisition_lock_timeout_ms: None,
            hb_err,
            hb_epoch,
            hb_assignment,
            hb_interval_ms,
            hb_deadline,
            hb_wake,
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

    /// Set the share acquisition mode. Each partition leader is negotiated
    /// before fetching; RecordLimit fails on a peer older than v2 before its RPC.
    pub fn set_acquire_mode(&mut self, mode: ShareAcquireMode) {
        self.acquire_mode = mode;
    }

    /// Last broker-reported acquisition lock duration (absent for v0 peers).
    #[must_use]
    pub fn acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.acquisition_lock_timeout_ms
    }

    /// Number of locally tracked acquisitions awaiting a terminal acknowledgement.
    #[must_use]
    pub fn acquired_record_count(&self) -> usize {
        self.acquisitions.len()
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

    /// Returns the broker-directed heartbeat interval.
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        let ms = self.hb_interval_ms.load(Ordering::SeqCst);
        if ms > 0 {
            if let Ok(ms_u64) = u64::try_from(ms) {
                return Duration::from_millis(ms_u64);
            }
        }
        self.cfg.heartbeat_interval
    }

    /// The recorded next heartbeat deadline computed from the broker interval,
    /// or `None` if no heartbeat is currently scheduled or the member has left.
    #[must_use]
    pub fn next_heartbeat_deadline(&self) -> Option<Instant> {
        *self.hb_deadline.lock()
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
    /// [`crate::ShareMetrics::topics`] is one row per topic that returned at least
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
        let deadline = Instant::now() + timeout;
        self.consumer.refresh_topics(&self.topics).await?;
        let version = spoken_share_group_heartbeat(self.coord.share_group_heartbeat_version)?;

        let mut first = true;
        loop {
            let req = ShareGroupHeartbeatRequest {
                group_id: self.group_id.clone(),
                member_id: self.member_id.clone(),
                member_epoch: if first {
                    ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
                } else {
                    self.member_epoch
                },
                rack_id: self.cfg.rack.clone(),
                // Re-send subscription until assignment arrives so the
                // coordinator can resolve topic ids (KIP-932).
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
            if resp.heartbeat_interval_ms <= 0 {
                return Err(Error::protocol(format!(
                    "invalid ShareGroupHeartbeat heartbeat_interval_ms: {}",
                    resp.heartbeat_interval_ms
                )));
            }
            self.hb_interval_ms
                .store(resp.heartbeat_interval_ms, Ordering::SeqCst);
            let interval =
                Duration::from_millis(u64::try_from(resp.heartbeat_interval_ms).unwrap_or(0));
            let hb_deadline = Instant::now() + interval;
            *self.hb_deadline.lock() = Some(hb_deadline);
            self.hb_wake.notify_one();
            if let Some(id) = resp.member_id {
                if !id.is_empty() {
                    self.member_id = id;
                }
            }
            if resp.member_epoch > 0 {
                self.member_epoch = resp.member_epoch;
            }
            self.apply_share_assignment(resp.assignment.as_deref());
            self.hb_epoch.store(self.member_epoch, Ordering::SeqCst);
            self.hb_err.store(0, Ordering::SeqCst);
            if !self.assigned.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                // Join succeeded; poll waits for assignment (Java ShareConsumer).
                return Ok(());
            }
            first = false;
            tokio::time::sleep(Duration::from_millis(50)).await;
            if Instant::now() >= deadline {
                return Ok(());
            }
            self.consumer.refresh_topics(&self.topics).await?;
        }
    }

    fn apply_share_assignment(&mut self, assignment: Option<&[ShareTopicPartitions]>) {
        let Some(assigned) = assignment else {
            return;
        };
        // Empty assignment vector means "no partitions right now" — clear.
        self.assigned.clear();
        self.topic_ids.clear();
        if assigned.is_empty() {
            self.acquisitions.clear();
            self.pending_delivery.clear();
            return;
        }
        let id_to_name = self.consumer.topic_id_names();
        let name_to_id = self.consumer.topic_name_ids();
        for tp in assigned {
            if tp.topic_id == [0u8; 16] {
                continue;
            }
            let name = id_to_name.get(&tp.topic_id).cloned().or_else(|| {
                // Single-topic subscribe: map the only name when metadata
                // has not yet indexed this id.
                if self.topics.len() == 1 {
                    self.topics.first().cloned()
                } else {
                    None
                }
            });
            let Some(name) = name else {
                continue;
            };
            let topic_id = name_to_id.get(&name).copied().unwrap_or(tp.topic_id);
            if topic_id == [0u8; 16] {
                continue;
            }
            let _ = self.topic_ids.insert(name.clone(), topic_id);
            for p in &tp.partitions {
                self.assigned.push((name.clone(), *p));
            }
        }
        self.acquisitions.retain(|(topic, part, _), _| {
            self.assigned.iter().any(|(t, p)| t == topic && p == part)
        });
        self.pending_delivery.retain(|r| {
            self.acquisitions
                .contains_key(&(r.topic.clone(), r.partition, r.offset))
        });
    }

    fn apply_pending_assignment(&mut self) {
        let pending = self.hb_assignment.lock().take();
        if let Some(assignment) = pending {
            self.apply_share_assignment(Some(assignment.as_slice()));
        }
    }

    async fn ensure_assignment(&mut self, deadline: Instant) -> Result<()> {
        self.apply_pending_assignment();
        if !self.assigned.is_empty() {
            return Ok(());
        }
        let version = spoken_share_group_heartbeat(self.coord.share_group_heartbeat_version)?;
        let timeout = self.cfg.request_timeout;
        while self.assigned.is_empty() {
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let hb = self.hb_err.load(Ordering::SeqCst);
            if hb != 0 {
                return Err(Error::broker(hb, "ShareGroupHeartbeat"));
            }
            self.apply_pending_assignment();
            if !self.assigned.is_empty() {
                return Ok(());
            }
            self.consumer.refresh_topics(&self.topics).await?;
            let req = ShareGroupHeartbeatRequest {
                group_id: self.group_id.clone(),
                member_id: self.member_id.clone(),
                member_epoch: self.member_epoch,
                rack_id: self.cfg.rack.clone(),
                subscribed_topic_names: Some(self.topics.clone()),
            };
            let body = coord_roundtrip(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
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
            if resp.heartbeat_interval_ms <= 0 {
                return Err(Error::protocol(format!(
                    "invalid ShareGroupHeartbeat heartbeat_interval_ms: {}",
                    resp.heartbeat_interval_ms
                )));
            }
            if let Ok(ms_u64) = u64::try_from(resp.heartbeat_interval_ms) {
                self.hb_interval_ms
                    .store(resp.heartbeat_interval_ms, Ordering::SeqCst);
                let next_deadline = Instant::now() + Duration::from_millis(ms_u64);
                *self.hb_deadline.lock() = Some(next_deadline);
                self.hb_wake.notify_one();
            }
            if let Some(id) = resp.member_id {
                if !id.is_empty() {
                    self.member_id = id;
                }
            }
            if resp.member_epoch > 0 {
                self.member_epoch = resp.member_epoch;
                self.hb_epoch.store(self.member_epoch, Ordering::SeqCst);
            }
            self.apply_share_assignment(resp.assignment.as_deref());
            if !self.assigned.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    /// Fetch records from assigned share partitions.
    ///
    /// Returns [`ShareRecords`], which indexes like a slice of [`ShareRecord`].
    /// `max_poll_records` caps each returned poll. When unset, the legacy
    /// acquisition request cap of 16 records per broker is preserved.
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
        self.prune_expired_acquisitions();
        let latest_epoch = self.hb_epoch.load(Ordering::SeqCst);
        if latest_epoch > 0 {
            self.member_epoch = latest_epoch;
        }
        let hb = self.hb_err.load(Ordering::SeqCst);
        if hb != 0 {
            return Err(Error::broker(hb, "ShareGroupHeartbeat"));
        }
        let started = Instant::now();
        let deadline = started + self.cfg.request_timeout;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.ensure_assignment(deadline),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        let mut attempt = 0u32;
        loop {
            let round = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                self.poll_leaders(),
            )
            .await;
            match round.unwrap_or(Err(Error::Timeout)) {
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
                Err(e) if share_leader_retriable(&e) || share_fetch_session_reset(&e) => {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    self.consumer.sleep_retry_backoff(attempt, deadline).await?;
                    attempt = attempt.saturating_add(1);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    if share_leader_retriable(&e) {
                        self.refresh_assigned_metadata().await?;
                    }
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

    /// Extend the locks of acquired records without accepting/releasing them.
    /// Older peers return Unsupported before an acknowledgement is emitted.
    pub async fn renew(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, AcknowledgeType::Renew).await
    }

    fn decoded_byte_limit(&self) -> usize {
        let hard = crate::protocol::records::DEFAULT_MAX_RECORD_BATCH_DECODE_BYTES;
        if self.cfg.buffer_memory == 0 {
            hard
        } else {
            self.cfg.buffer_memory.min(hard)
        }
    }

    fn prune_expired_acquisitions(&mut self) {
        let poisoned: Vec<_> = self
            .share_conns
            .iter()
            .filter(|(_, peer)| peer.conn.is_closed())
            .map(|(node, _)| *node)
            .collect();
        for node in poisoned {
            self.reset_node_session(node);
        }
        let now = Instant::now();
        self.acquisitions
            .retain(|_, acquisition| acquisition.expires_at.is_none_or(|expiry| expiry > now));
        self.pending_delivery.retain(|r| {
            self.acquisitions
                .get(&(r.topic.clone(), r.partition, r.offset))
                .is_some_and(|a| a.delivery_count == r.delivery_count)
        });
    }

    fn drain_pending_delivery(&mut self) -> Vec<ShareRecord> {
        let count = self
            .cfg
            .max_poll_records
            .unwrap_or(usize::MAX)
            .min(self.pending_delivery.len());
        self.pending_delivery.drain(..count).collect()
    }

    async fn node_share_version(&mut self, node: i32, api: i16) -> Result<i16> {
        if self
            .share_conns
            .get(&node)
            .is_some_and(|peer| peer.conn.is_closed())
        {
            self.reset_node_session(node);
        } else if self
            .share_conns
            .get(&node)
            .is_some_and(|peer| peer.conn.idle_expired(self.cfg.connections_max_idle))
        {
            // A clean idle reconnect preserves broker-side member/session state.
            // Cancelled or failed roundtrips instead invalidate ambiguous locks.
            let _ = self.share_conns.remove(&node);
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = self.share_conns.entry(node) {
            let addr = self
                .assigned
                .iter()
                .find_map(|(topic, part)| {
                    self.consumer
                        .leader_of(topic, *part)
                        .ok()
                        .filter(|(id, _)| *id == node)
                        .map(|(_, addr)| addr)
                })
                .or_else(|| self.share_addresses.get(&node).cloned())
                .ok_or_else(|| Error::protocol(format!("unknown share broker {node}")))?;
            let mut conn = BrokerConn::connect_tls(
                &addr,
                &self.cfg.client_id,
                self.cfg.connect_timeout,
                self.cfg.tls.as_ref(),
            )
            .await?;
            let versions =
                crate::protocol::api::negotiate_api_versions(&mut conn, self.cfg.request_timeout)
                    .await?;
            let fetch_version = versions
                .api_version(SHARE_FETCH)
                .and_then(|v| {
                    pick_version(
                        v.min_version,
                        v.max_version,
                        0,
                        SHARE_FETCH_RUNTIME_MAX_VERSION,
                    )
                })
                .unwrap_or(-1);
            let acknowledge_version = versions
                .api_version(SHARE_ACKNOWLEDGE)
                .and_then(|v| {
                    pick_version(
                        v.min_version,
                        v.max_version,
                        0,
                        SHARE_ACKNOWLEDGE_RUNTIME_MAX_VERSION,
                    )
                })
                .unwrap_or(-1);
            crate::protocol::sasl::apply_api_keys(&mut conn, &versions.api_keys);
            crate::protocol::sasl::authenticate(
                &mut conn,
                self.cfg.sasl_plain.as_ref(),
                self.cfg.sasl_scram.as_ref(),
                self.cfg.sasl_scram_sha512.as_ref(),
                self.cfg.sasl_oauthbearer.as_deref(),
                self.cfg.sasl_oauthbearer_oidc.as_ref(),
                self.cfg.request_timeout,
            )
            .await?;
            let _ = self.share_addresses.insert(node, addr);
            let _ = entry.insert(ShareNodeConnection {
                conn,
                fetch_version,
                acknowledge_version,
            });
        }
        let peer = self
            .share_conns
            .get(&node)
            .ok_or_else(|| Error::protocol("missing share peer"))?;
        if api == SHARE_FETCH {
            // Do not acquire records on a peer whose locks cannot be acknowledged.
            let _ = spoken_share_acknowledge(peer.acknowledge_version)?;
            spoken_share_fetch(peer.fetch_version)
        } else {
            spoken_share_acknowledge(peer.acknowledge_version)
        }
    }

    async fn share_roundtrip(
        &mut self,
        node: i32,
        api: i16,
        version: i16,
        body: &BytesMut,
    ) -> Result<Bytes> {
        let peer = self
            .share_conns
            .get_mut(&node)
            .ok_or_else(|| Error::protocol("missing share connection"))?;
        peer.conn
            .roundtrip(
                api,
                version,
                |buf| {
                    buf.extend_from_slice(body);
                    Ok(())
                },
                self.cfg.request_timeout,
            )
            .await
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
        let _ = self.share_conns.remove(&node);
        self.acquisitions
            .retain(|_, acquisition| acquisition.node != node);
        self.pending_delivery.retain(|r| {
            self.acquisitions
                .contains_key(&(r.topic.clone(), r.partition, r.offset))
        });
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
        if !self.pending_delivery.is_empty() {
            return Ok(self.drain_pending_delivery());
        }
        let assigned = self.assigned.clone();
        let by_leader = self.leaders_of(&assigned).await?;
        let mut nodes: Vec<_> = by_leader.into_iter().collect();
        nodes.sort_by_key(|(node, _)| *node);
        let metadata_bytes = self
            .acquisitions
            .keys()
            .map(|(topic, _, _)| acquisition_storage_bytes(topic))
            .fold(0usize, usize::saturating_add);
        let mut remaining_bytes = self
            .decoded_byte_limit()
            .checked_sub(metadata_bytes)
            .ok_or_else(|| Error::protocol("ShareFetch acquisition metadata budget exceeded"))?;
        for (node, tps) in nodes {
            let version = self.node_share_version(node, SHARE_FETCH).await?;
            if self.acquire_mode == ShareAcquireMode::RecordLimit && version < 2 {
                return Err(Error::Unsupported(format!(
                    "share broker {node} does not support record-limit acquisition"
                )));
            }
            let epoch = self.session_epoch(node);
            let mut by_id: BTreeMap<[u8; 16], Vec<i32>> = BTreeMap::new();
            for (topic, part) in &tps {
                let id = self
                    .topic_ids
                    .get(topic)
                    .copied()
                    .filter(|id| *id != [0; 16])
                    .ok_or_else(|| {
                        Error::protocol(format!("share assignment missing topic id for {topic}"))
                    })?;
                by_id.entry(id).or_default().push(*part);
            }
            let topics: Vec<_> = by_id
                .iter()
                .map(|(topic_id, partitions)| ShareFetchTopic {
                    topic_id: *topic_id,
                    partitions: partitions
                        .iter()
                        .map(|part| ShareFetchPartition {
                            partition: *part,
                            partition_max_bytes: self.cfg.max_partition_fetch_bytes,
                            acknowledgements: Vec::new(),
                        })
                        .collect(),
                })
                .collect();
            let max_records =
                i32::try_from(self.cfg.max_poll_records.unwrap_or(16).max(1)).unwrap_or(i32::MAX);
            let mut request = BytesMut::new();
            encode_share_fetch_request_with_options(
                &mut request,
                version,
                &self.group_id,
                &self.member_id,
                epoch,
                self.cfg.max_wait_ms,
                self.cfg.min_bytes,
                self.cfg.max_bytes,
                max_records,
                &topics,
                &[],
                max_records.min(16),
                ShareFetchRequestOptions {
                    acquire_mode: self.acquire_mode as i8,
                    is_renew_ack: false,
                },
            )?;
            let body = match self
                .share_roundtrip(node, SHARE_FETCH, version, &request)
                .await
            {
                Ok(body) => body,
                Err(error) => {
                    self.reset_node_session(node);
                    return Err(error);
                }
            };
            let mut body = body;
            let ((fetched, _, _, _, lock_timeout, error_code), decoded_bytes) =
                match decode_share_fetch_response_with_budget(&mut body, version, remaining_bytes) {
                    Ok(reply) => reply,
                    Err(error) => {
                        self.reset_node_session(node);
                        return Err(error);
                    }
                };
            if !body.is_empty() {
                self.reset_node_session(node);
                return Err(Error::protocol("trailing ShareFetch response bytes"));
            }
            if error_code != 0 {
                let error = Error::broker(error_code, "ShareFetch");
                if share_fetch_session_reset(&error) || share_leader_retriable(&error) {
                    self.reset_node_session(node);
                } else {
                    self.advance_node_epoch(node);
                }
                return Err(error);
            }
            // A well-formed response consumed this epoch even when a later
            // partition outcome fails. Initial sessions must also be closable.
            self.advance_node_epoch(node);
            if version >= 1 {
                self.acquisition_lock_timeout_ms = Some(lock_timeout);
            }
            let received_at = Instant::now();
            let expiry = if version >= 1 {
                if fetched
                    .iter()
                    .any(|t| t.partitions.iter().any(|p| !p.acquired.is_empty()))
                    && lock_timeout <= 0
                {
                    self.reset_node_session(node);
                    return Err(Error::protocol(
                        "non-positive ShareFetch acquisition lock timeout",
                    ));
                }
                u64::try_from(lock_timeout)
                    .ok()
                    .and_then(|ms| received_at.checked_add(Duration::from_millis(ms)))
            } else {
                None
            };
            let mut incoming = Vec::new();
            let mut incoming_keys = HashSet::new();
            for topic in fetched {
                let requested = by_id.get(&topic.topic_id).ok_or_else(|| {
                    Error::protocol("ShareFetch returned an unrequested topic id")
                })?;
                let name = self.name_for_topic_id(topic.topic_id);
                for part in topic.partitions {
                    if !requested.contains(&part.partition) {
                        return Err(Error::protocol(
                            "ShareFetch returned an unrequested partition",
                        ));
                    }
                    if part.error_code != 0 {
                        if share_ack_ownership_lost(part.error_code) {
                            self.consumer.invalidate_topic(&name);
                        }
                        let error = Error::broker(
                            part.error_code,
                            format!("ShareFetch {name}-{}", part.partition),
                        );
                        if share_session_reset(&error) || share_leader_retriable(&error) {
                            self.reset_node_session(node);
                        }
                        return Err(error);
                    }
                    if part.acknowledge_error_code != 0 {
                        return Err(Error::broker(
                            part.acknowledge_error_code,
                            format!("ShareFetch acknowledgement {name}-{}", part.partition),
                        ));
                    }
                    let mut acquired = part.acquired;
                    acquired.sort_unstable_by_key(|range| range.first_offset);
                    for range in &acquired {
                        if range.first_offset < 0
                            || range.last_offset < range.first_offset
                            || range.delivery_count <= 0
                        {
                            return Err(Error::protocol("invalid ShareFetch acquired range"));
                        }
                    }
                    if acquired.windows(2).any(|pair| {
                        pair.first()
                            .zip(pair.get(1))
                            .is_some_and(|(a, b)| a.last_offset >= b.first_offset)
                    }) {
                        return Err(Error::protocol("overlapping ShareFetch acquisition ranges"));
                    }
                    for batch in part.records {
                        let timestamp_type = batch.timestamp_type();
                        let leader_epoch = (batch.partition_leader_epoch >= 0)
                            .then_some(batch.partition_leader_epoch);
                        for record in batch.records {
                            let range_index =
                                acquired.partition_point(|range| range.last_offset < record.offset);
                            let Some(range) = acquired
                                .get(range_index)
                                .filter(|range| range.first_offset <= record.offset)
                            else {
                                continue;
                            };
                            let delivery_count = range.delivery_count;
                            let key = (name.clone(), part.partition, record.offset);
                            if !incoming_keys.insert(key.clone()) {
                                return Err(Error::protocol("duplicate ShareFetch record offset"));
                            }
                            if let Some(previous) = self.acquisitions.get(&key) {
                                if delivery_count < previous.delivery_count {
                                    return Err(Error::protocol(
                                        "ShareFetch delivery count regressed",
                                    ));
                                }
                                if delivery_count == previous.delivery_count {
                                    continue;
                                }
                            }
                            incoming.push(ShareRecord {
                                topic: name.clone(),
                                partition: part.partition,
                                offset: record.offset,
                                timestamp: record.timestamp,
                                timestamp_type,
                                key: record.key,
                                value: record.value,
                                headers: record.headers,
                                delivery_count,
                                leader_epoch,
                            });
                        }
                    }
                }
            }
            let incoming_bytes = incoming
                .iter()
                .map(share_record_storage_bytes)
                .fold(0usize, usize::saturating_add);
            let new_metadata_bytes = incoming
                .iter()
                .filter(|record| {
                    !self.acquisitions.contains_key(&(
                        record.topic.clone(),
                        record.partition,
                        record.offset,
                    ))
                })
                .map(|record| acquisition_storage_bytes(&record.topic))
                .fold(0usize, usize::saturating_add);
            let retained_bytes = incoming_bytes.saturating_add(new_metadata_bytes);
            remaining_bytes = match remaining_bytes.checked_sub(retained_bytes.max(decoded_bytes)) {
                Some(remaining) => remaining,
                None => {
                    self.reset_node_session(node);
                    return Err(Error::protocol(
                        "ShareFetch retained-record budget exceeded",
                    ));
                }
            };
            for record in incoming {
                let _ = self.acquisitions.insert(
                    (record.topic.clone(), record.partition, record.offset),
                    Acquisition {
                        node,
                        delivery_count: record.delivery_count,
                        expires_at: expiry,
                    },
                );
                self.pending_delivery.push_back(record);
            }
        }
        Ok(self.drain_pending_delivery())
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
            self.share_conns.clear();
            self.acquisitions.clear();
            self.pending_delivery.clear();
            *self.hb_assignment.lock() = None;
            *self.hb_deadline.lock() = None;
            self.hb_interval_ms.store(0, Ordering::SeqCst);
            return Ok(());
        }
        self.topic_match = None;
        self.hb_stop.send(true).unwrap_or(());
        *self.hb_deadline.lock() = None;
        self.hb_interval_ms.store(0, Ordering::SeqCst);
        let close = self.close_share_session().await;
        let leave = self.leave_coordinator().await;
        self.assigned.clear();
        self.topics.clear();
        self.topic_ids.clear();
        self.share_epochs.clear();
        self.member_id.clear();
        *self.hb_assignment.lock() = None;
        self.member_epoch = ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH;
        self.hb_epoch.store(
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            Ordering::SeqCst,
        );
        self.hb_err.store(0, Ordering::SeqCst);
        close.and(leave)
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
        self.member_id = Uuid::random_uuid().to_string();
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
        if self.share_epochs.is_empty() {
            return Err(reject_java_acknowledge_before_poll());
        }
        self.prune_expired_acquisitions();
        for record in recs {
            let acquired =
                self.acquisitions
                    .get(&(record.topic.clone(), record.partition, record.offset));
            if acquired.is_none_or(|a| a.delivery_count != record.delivery_count) {
                return Err(Error::broker(
                    error::INVALID_RECORD_STATE,
                    format!(
                        "stale share acknowledgement {}-{}@{}",
                        record.topic, record.partition, record.offset
                    ),
                ));
            }
        }
        let mut partitions = acknowledgement_batches(recs, ack);
        let deadline = Instant::now() + self.cfg.request_timeout;
        let mut attempt = 0u32;
        loop {
            let outcome = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                self.acknowledge_leaders(&mut partitions, ack),
            )
            .await;
            match outcome.unwrap_or(Err(Error::Timeout)) {
                Ok(()) => return Ok(()),
                Err(error) if share_ack_retriable(&error) && !partitions.is_empty() => {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    self.consumer.sleep_retry_backoff(attempt, deadline).await?;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn acknowledge_leaders(
        &mut self,
        partitions: &mut Vec<(String, i32, Vec<AcknowledgementBatch>)>,
        ack: i8,
    ) -> Result<()> {
        let tps: Vec<_> = partitions
            .iter()
            .map(|(topic, part, _)| (topic.clone(), *part))
            .collect();
        let mut nodes: Vec<_> = self.leaders_of(&tps).await?.into_iter().collect();
        nodes.sort_by_key(|(node, _)| *node);
        let mut first_error = None;
        for (node, node_tps) in nodes {
            let version = self.node_share_version(node, SHARE_ACKNOWLEDGE).await?;
            if ack == ACK_RENEW && version < 2 {
                return Err(Error::Unsupported(format!(
                    "share broker {node} does not support Renew"
                )));
            }
            let epoch = self.session_epoch(node);
            if epoch <= 0 {
                return Err(Error::broker(
                    error::SHARE_SESSION_NOT_FOUND,
                    "ShareAcknowledge requires a poll on the current leader",
                ));
            }
            let mut topics: Vec<ShareAckTopic> = Vec::new();
            for (topic, part, batches) in partitions.iter() {
                if !node_tps.iter().any(|(t, p)| t == topic && p == part) {
                    continue;
                }
                let topic_id = self
                    .topic_ids
                    .get(topic)
                    .copied()
                    .filter(|id| *id != [0; 16])
                    .ok_or_else(|| {
                        Error::protocol(format!("missing share topic id for {topic}"))
                    })?;
                match topics.iter_mut().find(|t| t.topic_id == topic_id) {
                    Some(slot) => slot.partitions.push((*part, batches.clone())),
                    None => topics.push(ShareAckTopic {
                        topic_id,
                        partitions: vec![(*part, batches.clone())],
                    }),
                }
            }
            let mut request = BytesMut::new();
            encode_share_acknowledge_topics(
                &mut request,
                version,
                &self.group_id,
                &self.member_id,
                epoch,
                &topics,
            )?;
            let mut body = match self
                .share_roundtrip(node, SHARE_ACKNOWLEDGE, version, &request)
                .await
            {
                Ok(body) => body,
                Err(error) => {
                    self.reset_node_session(node);
                    return Err(error);
                }
            };
            let (code, responses, _, _, _, lock_timeout) =
                match decode_share_acknowledge_topics_response_with_lock_timeout(&mut body, version)
                {
                    Ok(reply) => reply,
                    Err(error) => {
                        self.reset_node_session(node);
                        return Err(error);
                    }
                };
            if !body.is_empty() {
                self.reset_node_session(node);
                return Err(Error::protocol("trailing ShareAcknowledge response bytes"));
            }
            if code != 0 {
                let error = Error::broker(code, "ShareAcknowledge");
                if share_session_reset(&error) {
                    self.reset_node_session(node);
                } else {
                    self.advance_node_epoch(node);
                }
                return Err(error);
            }
            let expected: HashSet<_> = topics
                .iter()
                .flat_map(|t| t.partitions.iter().map(move |(p, _)| (t.topic_id, *p)))
                .collect();
            let mut seen = HashSet::new();
            for topic in &responses {
                for part in &topic.partitions {
                    if !expected.contains(&(topic.topic_id, part.partition))
                        || !seen.insert((topic.topic_id, part.partition))
                    {
                        return Err(Error::protocol(
                            "unrequested or duplicate ShareAcknowledge partition response",
                        ));
                    }
                }
            }
            if seen != expected {
                return Err(Error::protocol(
                    "ShareAcknowledge omitted a requested partition outcome",
                ));
            }
            self.advance_node_epoch(node);
            if version >= 2 && lock_timeout > 0 {
                self.acquisition_lock_timeout_ms = Some(lock_timeout);
            }
            for topic in responses {
                let name = self.name_for_topic_id(topic.topic_id);
                for part in topic.partitions {
                    if part.error_code == 0 {
                        let count = self
                            .acquisitions
                            .iter()
                            .filter(|((t, p, offset), _)| {
                                t == &name
                                    && *p == part.partition
                                    && partitions.iter().any(|(topic, partition, batches)| {
                                        topic == t
                                            && partition == p
                                            && batches.iter().any(|b| {
                                                *offset >= b.first_offset
                                                    && *offset <= b.last_offset
                                            })
                                    })
                            })
                            .count();
                        if ack == ACK_RENEW {
                            if lock_timeout <= 0 {
                                return Err(Error::protocol(
                                    "non-positive Renew acquisition lock timeout",
                                ));
                            }
                            let expiry = Instant::now().checked_add(Duration::from_millis(
                                u64::try_from(lock_timeout).unwrap_or(0),
                            ));
                            for ((t, p, offset), acquired) in &mut self.acquisitions {
                                if t == &name
                                    && *p == part.partition
                                    && partitions.iter().any(|(topic, partition, batches)| {
                                        topic == t
                                            && partition == p
                                            && batches.iter().any(|b| {
                                                *offset >= b.first_offset
                                                    && *offset <= b.last_offset
                                            })
                                    })
                                {
                                    acquired.expires_at = expiry;
                                }
                            }
                        } else {
                            self.acquisitions.retain(|(t, p, offset), _| {
                                !(t == &name
                                    && *p == part.partition
                                    && partitions.iter().any(|(topic, partition, batches)| {
                                        topic == t
                                            && partition == p
                                            && batches.iter().any(|b| {
                                                *offset >= b.first_offset
                                                    && *offset <= b.last_offset
                                            })
                                    }))
                            });
                            self.records_acknowledged = self
                                .records_acknowledged
                                .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
                        }
                        partitions.retain(|(t, p, _)| t != &name || *p != part.partition);
                    } else {
                        if part.error_code == error::INVALID_RECORD_STATE
                            || share_ack_ownership_lost(part.error_code)
                        {
                            self.acquisitions.retain(|(t, p, offset), _| {
                                !(t == &name
                                    && *p == part.partition
                                    && partitions.iter().any(|(topic, partition, batches)| {
                                        topic == t
                                            && partition == p
                                            && batches.iter().any(|b| {
                                                *offset >= b.first_offset
                                                    && *offset <= b.last_offset
                                            })
                                    }))
                            });
                        }
                        let error = Error::broker(
                            part.error_code,
                            format!("ShareAcknowledge {name}-{}", part.partition),
                        );
                        // A terminal outcome must remain visible even when another
                        // partition asks for a metadata retry.
                        if first_error.as_ref().is_none_or(share_ack_retriable) {
                            first_error = Some(error);
                        }
                        if part.error_code == error::SHARE_SESSION_NOT_FOUND
                            || part.error_code == error::INVALID_SHARE_SESSION_EPOCH
                        {
                            self.reset_node_session(node);
                        }
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn close_share_session(&mut self) -> Result<()> {
        let open: Vec<_> = self
            .share_epochs
            .iter()
            .filter(|(_, epoch)| **epoch > 0)
            .map(|(node, _)| *node)
            .collect();
        let mut last = Ok(());
        for node in open {
            let version = match self.node_share_version(node, SHARE_ACKNOWLEDGE).await {
                Ok(version) => version,
                Err(error) => {
                    last = Err(error);
                    continue;
                }
            };
            let mut request = BytesMut::new();
            let encoded = encode_share_acknowledge_request(
                &mut request,
                version,
                &self.group_id,
                &self.member_id,
                ShareRequestMetadata::FINAL_EPOCH,
                [0; 16],
                &[],
            );
            if let Err(error) = encoded {
                last = Err(error);
                continue;
            }
            let code = match self
                .share_roundtrip(node, SHARE_ACKNOWLEDGE, version, &request)
                .await
            {
                Ok(mut body) => match decode_share_acknowledge_response(&mut body, version) {
                    Ok(code) if body.is_empty() => code,
                    Ok(_) => {
                        last = Err(Error::protocol("trailing ShareAcknowledge close bytes"));
                        continue;
                    }
                    Err(error) => {
                        last = Err(error);
                        continue;
                    }
                },
                Err(_) => error::SHARE_SESSION_NOT_FOUND,
            };
            if code != 0 && code != error::SHARE_SESSION_NOT_FOUND {
                last = Err(Error::broker(code, "ShareAcknowledge close"));
            }
        }
        self.share_epochs.clear();
        self.share_conns.clear();
        self.acquisitions.clear();
        self.pending_delivery.clear();
        last
    }

    /// Leave the share group ([`ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH`]).
    pub async fn leave(mut self) -> Result<()> {
        if self.member_id.is_empty() {
            self.hb_stop.send(true).unwrap_or(());
            *self.hb_deadline.lock() = None;
            self.consumer.close_interceptors();
            return Ok(());
        }
        self.hb_stop.send(true).unwrap_or(());
        *self.hb_deadline.lock() = None;
        let close = self.close_share_session().await;
        let out = self.leave_coordinator().await;
        self.consumer.close_interceptors();
        close.and(out)
    }

    async fn leave_coordinator(&mut self) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        let version = spoken_share_group_heartbeat(self.coord.share_group_heartbeat_version)?;
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH,
            rack_id: self.cfg.rack.clone(),
            subscribed_topic_names: None,
        };
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
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
        let hb_assignment = self.hb_assignment.clone();
        let hb_interval_ms = self.hb_interval_ms.clone();
        let hb_deadline = self.hb_deadline.clone();
        let hb_wake = self.hb_wake.clone();
        let cfg = self.cfg.clone();
        drop(tokio::spawn(async move {
            if *stop.borrow() {
                return;
            }
            let mut conn: Option<BrokerConn> = None;
            let init_ms = hb_interval_ms.load(Ordering::SeqCst);
            let mut current_interval = if let Ok(ms_u64) = u64::try_from(init_ms) {
                if ms_u64 > 0 {
                    Duration::from_millis(ms_u64)
                } else {
                    cfg.heartbeat_interval.max(Duration::from_millis(1))
                }
            } else {
                cfg.heartbeat_interval.max(Duration::from_millis(1))
            };
            // Copy first. parking_lot mutexes are not reentrant, and this task
            // starts only after join has stored a deadline.
            let existing_deadline = *hb_deadline.lock();
            let mut next_hb_deadline = if let Some(d) = existing_deadline {
                d
            } else {
                let d = Instant::now() + current_interval;
                if !update_share_heartbeat_deadline(&hb_deadline, &stop, d) {
                    return;
                }
                d
            };
            loop {
                if *stop.borrow() {
                    break;
                }
                let now = Instant::now();
                if let Some(extern_deadline) = *hb_deadline.lock() {
                    if extern_deadline > now && extern_deadline != next_hb_deadline {
                        next_hb_deadline = extern_deadline;
                        let latest_ms = hb_interval_ms.load(Ordering::SeqCst);
                        if let Ok(ms_u64) = u64::try_from(latest_ms) {
                            if ms_u64 > 0 {
                                current_interval = Duration::from_millis(ms_u64);
                            }
                        }
                    }
                }
                let wake_deadline = next_hb_deadline;
                let sleep_duration = wake_deadline.saturating_duration_since(Instant::now());
                tokio::select! {
                    res = stop.changed() => {
                        if res.is_err() || *stop.borrow() {
                            break;
                        }
                    }
                    _ = hb_wake.notified() => {
                        continue;
                    }
                    _ = tokio::time::sleep(sleep_duration) => {
                        if Instant::now() < next_hb_deadline {
                            continue;
                        }
                        if conn
                            .as_ref()
                            .is_some_and(|c| c.idle_expired(cfg.connections_max_idle))
                        {
                            conn = None;
                        }
                        if conn.is_none() {
                            conn = discover_coord(&cfg, &group_id, COORDINATOR_GROUP).await.ok();
                        }
                        if *stop.borrow() { break; }
                        let Some(c) = conn.as_mut() else {
                            let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                            next_hb_deadline = Instant::now() + retry_delay;
                            if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                            continue;
                        };
                        let epoch = hb_epoch.load(Ordering::SeqCst);
                        let Ok(version) =
                            spoken_share_group_heartbeat(c.share_group_heartbeat_version)
                        else {
                            conn = None;
                            let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                            next_hb_deadline = Instant::now() + retry_delay;
                            if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                            continue;
                        };
                        let req = ShareGroupHeartbeatRequest {
                            group_id: group_id.clone(),
                            member_id: member_id.clone(),
                            member_epoch: epoch,
                            rack_id: cfg.rack.clone(),
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
                        if *stop.borrow() { break; }
                        match res {
                            Ok(body) => {
                                if let Ok(resp) = decode_share_group_heartbeat_response(
                                    &mut body.clone(),
                                    version,
                                ) {
                                    if crate::error::coordinator_retriable(resp.error_code) {
                                        conn = None;
                                        let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                                        next_hb_deadline = Instant::now() + retry_delay;
                                        if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                                    } else {
                                        hb_err.store(resp.error_code, Ordering::SeqCst);
                                        if resp.member_epoch > 0 {
                                            hb_epoch.store(resp.member_epoch, Ordering::SeqCst);
                                        }
                                        if let Some(assignment) = resp.assignment {
                                            *hb_assignment.lock() = Some(assignment);
                                        }
                                        if resp.error_code == 0 {
                                            if resp.heartbeat_interval_ms <= 0 {
                                                hb_err.store(error::INVALID_REQUEST, Ordering::SeqCst);
                                                next_hb_deadline = Instant::now() + current_interval;
                                                if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                                            } else if let Ok(ms_u64) = u64::try_from(resp.heartbeat_interval_ms) {
                                                current_interval = Duration::from_millis(ms_u64);
                                                hb_interval_ms.store(resp.heartbeat_interval_ms, Ordering::SeqCst);
                                                next_hb_deadline = Instant::now() + current_interval;
                                                if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                                            } else {
                                                next_hb_deadline = Instant::now() + current_interval;
                                                if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                                            }
                                        } else {
                                            next_hb_deadline = Instant::now() + current_interval;
                                            if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                                        }
                                    }
                                } else {
                                    conn = None;
                                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                                    next_hb_deadline = Instant::now() + retry_delay;
                                    if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                                }
                            }
                            Err(_) => {
                                conn = None;
                                let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                                next_hb_deadline = Instant::now() + retry_delay;
                                if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
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
    let headers = rec
        .headers
        .iter()
        .map(|h| {
            h.key
                .len()
                .saturating_add(h.value.as_ref().map(Bytes::len).unwrap_or(0))
        })
        .fold(0usize, usize::saturating_add);
    u64::try_from(k.saturating_add(v).saturating_add(headers)).unwrap_or(u64::MAX)
}

// Logical key/value storage plus a conservative hash-entry allowance. Record
// buffers and outstanding acquisition metadata share the configured allowance.
fn acquisition_storage_bytes(topic: &str) -> usize {
    std::mem::size_of::<AcquisitionKey>()
        .saturating_add(std::mem::size_of::<Acquisition>())
        .saturating_add(topic.len())
        .saturating_add(32)
}

fn share_record_storage_bytes(rec: &ShareRecord) -> usize {
    usize::try_from(share_record_bytes(rec))
        .unwrap_or(usize::MAX)
        .saturating_add(std::mem::size_of::<ShareRecord>())
        .saturating_add(rec.topic.len())
        .saturating_add(
            rec.headers
                .len()
                .saturating_mul(std::mem::size_of::<Header>()),
        )
}

fn update_share_heartbeat_deadline(
    shared: &Mutex<Option<Instant>>,
    stop: &watch::Receiver<bool>,
    next: Instant,
) -> bool {
    let mut deadline = shared.lock();
    if *stop.borrow() {
        return false;
    }
    *deadline = Some(next);
    true
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

fn share_ack_ownership_lost(code: i16) -> bool {
    matches!(
        code,
        error::NOT_LEADER_OR_FOLLOWER
            | error::FENCED_LEADER_EPOCH
            | error::UNKNOWN_TOPIC_OR_PARTITION
            | error::UNKNOWN_TOPIC_ID
    )
}

fn share_ack_retriable(error: &Error) -> bool {
    error.broker_code().is_some_and(|code| {
        !share_ack_ownership_lost(code)
            && !share_session_reset(error)
            && (error.is_retriable() || code == error::NETWORK_EXCEPTION)
    })
}

fn share_fetch_session_reset(error: &Error) -> bool {
    share_session_reset(error) || error.broker_code() == Some(error::SHARE_SESSION_LIMIT_REACHED)
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
        assert_eq!(AcknowledgeType::from_id(4), Some(AcknowledgeType::Renew));
        assert_eq!(AcknowledgeType::Renew.id(), 4);
        assert_eq!(AcknowledgeType::Renew.to_string(), "renew");
        assert_eq!(AcknowledgeType::from_id(5), None);
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
