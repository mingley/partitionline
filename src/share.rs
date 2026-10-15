//! Share groups (KIP-932): queue-style consumption with per-record ack.

mod acquired;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicI32, Ordering};
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
    write_java_optional, write_java_optional_bytes, write_java_record_headers, Header, RecordBatch,
    TimestampType,
};
use crate::protocol::share::{
    decode_share_acknowledge_response, decode_share_acknowledge_topics_response_with_lock_timeout,
    decode_share_fetch_response_skipping_corrupt_batches, decode_share_group_heartbeat_response,
    encode_share_acknowledge_request, encode_share_acknowledge_topics,
    encode_share_fetch_request_with_options, encode_share_group_heartbeat_request,
    AcknowledgementBatch, AcquiredRange, ShareAckTopic, ShareFetchPartition, ShareFetchRequest,
    ShareFetchRequestOptions, ShareFetchTopic, ShareForgottenTopic, ShareGroupHeartbeatRequest,
    ShareTopicPartitions, ACK_ACCEPT, ACK_GAP, ACK_REJECT, ACK_RELEASE, ACK_RENEW,
};
use crate::Uuid;

pub use crate::protocol::share::{
    ACK_ACCEPT as SHARE_ACK_ACCEPT, ACK_REJECT as SHARE_ACK_REJECT,
    ACK_RELEASE as SHARE_ACK_RELEASE, ACK_RENEW as SHARE_ACK_RENEW,
};

/// Share-group acknowledgement (Java `AcknowledgeType`, KIP-932).
///
/// [`std::fmt::Display`] is Java `AcknowledgeType.toString` (`accept`). Wire gap `0`
/// is not a Java `AcknowledgeType` ([`crate::protocol::share::ACK_GAP`]);
/// [`ShareGroup::poll`] queues that type for acquired offsets with no record
/// (Java `ShareCompletedFetch`).
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

/// Java `share.acknowledgement.mode` for KIP-932 share groups.
///
/// Java defaults to [`Self::Implicit`]. This client defaults to
/// [`Self::Explicit`] so a missed `accept` cannot silently commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ShareAcknowledgementMode {
    /// Accept the previous poll's records on the next [`ShareGroup::poll`]
    /// or [`ShareGroup::commit`] (Java `implicit`). [`ShareGroup::leave`] /
    /// [`ShareGroup::close`] release that last poll without ACCEPT (Java
    /// `close` "releases any acquired records without acknowledgement").
    /// [`ShareGroup::commit_async`] snapshots the last poll as ACCEPT
    /// locally and sends on the next poll / leave.
    Implicit,
    /// The application must [`ShareGroup::accept`] / [`ShareGroup::release`]
    /// / [`ShareGroup::reject`] every delivered record (Java `explicit`).
    /// Those calls are local until the next [`ShareGroup::poll`],
    /// [`ShareGroup::commit`], [`ShareGroup::leave`], or [`ShareGroup::close`].
    /// A later [`ShareGroup::poll`] that would contact a broker is Java
    /// `IllegalStateException`
    /// (`All records must be acknowledged in explicit acknowledgement mode.`)
    /// until every previously delivered record is acknowledged. Draining
    /// records already buffered from the last ShareFetch is allowed.
    #[default]
    Explicit,
}

impl ShareAcknowledgementMode {
    /// Java config value (`implicit` / `explicit`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Implicit => "implicit",
            Self::Explicit => "explicit",
        }
    }
}

impl fmt::Display for ShareAcknowledgementMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Java `AcknowledgementCommitCallback` exception argument (owned).
///
/// `code` is the Kafka `error_code` when the failure is a broker error.
/// [`Self::message`] is Java `Exception.getMessage`: the broker
/// `AcknowledgeErrorMessage` / partition `ErrorMessage` when
/// ShareFetch/ShareAcknowledge supplies a non-empty one, otherwise
/// the crate fallback or [`Error`] display text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgementCommitError {
    /// Kafka `error_code`, or `None` for timeout / client-side RPC failure.
    pub code: Option<i16>,
    /// Java `Exception.getMessage`.
    pub message: String,
}

impl AcknowledgementCommitError {
    fn from_error(error: &Error) -> Self {
        let message = match error {
            Error::Broker { message, .. } if !message.is_empty() => message.clone(),
            _ => error.to_string(),
        };
        Self {
            code: error.broker_code(),
            message,
        }
    }

    /// Kafka `error_code` when this is a broker failure.
    #[must_use]
    pub fn code(&self) -> Option<i16> {
        self.code
    }

    /// Java `Exception.getMessage`.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for AcknowledgementCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

/// Java `AcknowledgementCommitCallback` for share acknowledgement outcomes.
///
/// `onComplete(Map<TopicIdPartition, Set<OffsetAndMetadata>>, Exception)`.
/// Closures that match [`Self::on_complete`] implement this trait.
/// Each invocation is one topic-partition (Java `COMMIT_ASYNC`
/// `ResultHandler.complete`).
pub trait AcknowledgementCommitCallback: Send {
    /// Java `AcknowledgementCommitCallback.onComplete`.
    ///
    /// `exception` is `None` when that partition's ShareAcknowledge
    /// succeeded.
    fn on_complete(
        &mut self,
        offsets: Vec<(crate::TopicIdPartition, Vec<crate::OffsetAndMetadata>)>,
        exception: Option<AcknowledgementCommitError>,
    );
}

impl<F> AcknowledgementCommitCallback for F
where
    F: FnMut(
            Vec<(crate::TopicIdPartition, Vec<crate::OffsetAndMetadata>)>,
            Option<AcknowledgementCommitError>,
        ) + Send,
{
    fn on_complete(
        &mut self,
        offsets: Vec<(crate::TopicIdPartition, Vec<crate::OffsetAndMetadata>)>,
        exception: Option<AcknowledgementCommitError>,
    ) {
        self(offsets, exception);
    }
}

type AcknowledgementCommitFn = Box<
    dyn FnMut(
            Vec<(crate::TopicIdPartition, Vec<crate::OffsetAndMetadata>)>,
            Option<AcknowledgementCommitError>,
        ) + Send,
>;

/// Java `ShareConsumer.commitSync()` result:
/// `Map<TopicIdPartition, Optional<KafkaException>>`.
///
/// `None` is Java `Optional.empty()` (that partition succeeded).
/// A present [`AcknowledgementCommitError`] is Java `Optional.of`.
pub type AcknowledgementCommitResults =
    Vec<(crate::TopicIdPartition, Option<AcknowledgementCommitError>)>;

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
            if seen.insert((rec.topic.as_str(), rec.partition)) {
                out.push(crate::TopicPartition::new(rec.topic.clone(), rec.partition));
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
        let mut last: HashMap<(&str, i32), &ShareRecord> = HashMap::new();
        let mut order = Vec::new();
        for rec in &self.records {
            let key = (rec.topic.as_str(), rec.partition);
            if last.insert(key, rec).is_none() {
                order.push(key);
            }
        }
        order
            .into_iter()
            .filter_map(|key| {
                last.remove(&key).map(|rec| {
                    let mut md = crate::OffsetAndMetadata::new(rec.offset.saturating_add(1));
                    if let Some(epoch) = rec.leader_epoch {
                        md = md.with_leader_epoch(epoch);
                    }
                    (
                        crate::TopicPartition::new(rec.topic.clone(), rec.partition),
                        md,
                    )
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
    ///
    /// Java `ShareAcquireMode.RECORD_LIMIT`: one partition leader fetches
    /// records per poll. Other leaders still open or adjust the share
    /// session and piggyback acknowledgements, with wait/byte/record
    /// limits set to zero (Java `canSkipIfRequestEmpty`).
    RecordLimit = 1,
}

/// KIP-932 share group member (`ShareGroupHeartbeat` v0–v1 / ShareFetch v0–v2 / ShareAcknowledge v0–v2).
///
/// [`Self::group_metadata`] is Java `ShareConsumer.groupMetadata()`;
/// [`crate::ConsumerGroupMetadata::generation_id`] is the share member epoch.
/// [`Self::commit_sync`] is Java `ShareConsumer.commitSync()`
/// (`Map<TopicIdPartition, Optional<KafkaException>>`).
/// [`Self::commit_async`] is Java `ShareConsumer.commitAsync()`
/// (`acknowledgeBatchIfImplicitAcknowledgement` then a standalone
/// ShareAcknowledge on the next poll / leave). Implicit
/// [`Self::leave`] / [`Self::close`] do not ACCEPT the last poll
/// (Java `close` releases acquired records without acknowledgement).
/// A piggybacked ShareFetch acknowledgement error fires the commit
/// callback and does not fail [`Self::poll`] (Java
/// `ShareConsumeRequestManager`); the callback message is
/// `AcknowledgeErrorMessage` when the broker sends a non-empty one,
/// otherwise `ShareFetch acknowledgement {topic}-{partition}`. A
/// piggybacked `FENCED_LEADER_EPOCH` / `NOT_LEADER_OR_FOLLOWER` /
/// `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` /
/// `INCONSISTENT_TOPIC_ID` still completes without retry, including
/// when `AcknowledgeErrorMessage` is set or empty. A piggybacked
/// `FENCED_LEADER_EPOCH` / `NOT_LEADER_OR_FOLLOWER` /
/// `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` /
/// `INCONSISTENT_TOPIC_ID` on one partition does not fail a sibling
/// partition whose `AcknowledgeErrorCode` is none, including when
/// `AcknowledgeErrorMessage` is set or empty. A top-level
/// ShareFetch error completes in-flight piggybacked acknowledgements
/// with that code (Java `handleResponse` false), preferring a non-empty
/// `ErrorMessage`. Top-level `SHARE_SESSION_LIMIT_REACHED` /
/// `INVALID_SHARE_SESSION_EPOCH` / `SHARE_SESSION_NOT_FOUND` still
/// reset the share session (Java
/// `nextCloseExistingAttemptNew`), including when `ErrorMessage` is
/// empty. A ShareFetch
/// transport / timeout / decode failure completes those acknowledgements
/// the same way (Java `handleShareFetchFailure`). Piggyback is refused
/// on the initial share session and those acknowledgements complete
/// with `NOT_LEADER_OR_FOLLOWER` (Java
/// `testPiggybackAcknowledgementsOnInitialShareSessionError`). After
/// subscribe, leftover acknowledgements whose leader has no share
/// session complete the same way and that leader is not fetched (Java
/// `testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange`).
/// Leftover acknowledgements whose metadata leader has changed complete
/// the same way and are not piggybacked on the new leader (Java
/// `testWhenLeadershipChangeBetweenShareFetchRequests`).
/// A successful ShareFetch that omits a piggybacked partition completes
/// those acknowledgements with `INVALID_RECORD_STATE` (Java
/// `InvalidRecordStateException` / `INVALID_RESPONSE`). An omitted
/// piggyback acknowledgement on one partition does not fail a sibling
/// partition that is present with `AcknowledgeErrorCode` none. A
/// successful ShareAcknowledge that omits a requested partition
/// completes that partition the same way (Java `processingComplete` /
/// `testShareAcknowledgeInvalidResponse`). An omitted ShareAcknowledge
/// partition does not fail a sibling partition that is present with
/// error none. Unrequested ShareAcknowledge partitions, unknown topic
/// ids, and known topic ids that were not in the request are skipped
/// (Java `lookupTopicId` /
/// `handleAcknowledgeErrorCode`).
/// Top-level ShareFetch `UNKNOWN_TOPIC_ID` invalidates topic metadata
/// so the next poll uses the refreshed id, including when
/// `ErrorMessage` is set (Java `handleShareFetchSuccess` /
/// `metadata.requestUpdate` / `testUnknownTopicIdError`). Top-level
/// `INCONSISTENT_TOPIC_ID` still fails that poll and does not request
/// Metadata, including when `ErrorMessage` is set. Incremental ShareFetch sends Java
/// `ShareSessionHandler` forgotten partitions when the assignment
/// shrinks or a topic id is replaced on a node this poll still
/// contacts (remaining assigned partitions or leftover acknowledgements).
/// Abandoned leftover sessions without acks are not fetched (Java
/// `testShareFetchWithSubscriptionChangeMultipleNodesEmptyAcknowledgements`).
/// Standalone ShareAcknowledge
/// fails locally with `NOT_LEADER_OR_FOLLOWER` when Metadata already
/// shows a different leader than the fetch node (Java
/// `isLeaderKnownToHaveChanged` /
/// `testLeadershipChangeAfterFetchBeforeCommitSync`). Acquisition
/// locks are released on the previous broker; those acknowledgements
/// are not sent to the new leader. A ShareAcknowledge partition
/// `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` /
/// `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` with a usable
/// CurrentLeader (`leaderId != -1 && leaderEpoch != -1`) plus
/// NodeEndpoints updates the next ShareFetch target (Java
/// `updateLeaderInfoMap` / KIP-951).
/// [`ShareAcquireMode::RecordLimit`] fetches records from one leader
/// per poll; other leaders send a zero-limit session/ack ShareFetch
/// (Java `canSkipIfRequestEmpty`). An unrequested ShareFetch topic id
/// or partition is skipped (Java `lookupTopicId` /
/// `testShareFetchInvalidResponse`). An unknown ShareFetch topic id
/// on a piggyback response is skipped and does not fail a requested
/// partition whose `AcknowledgeErrorCode` is none. A known cluster
/// topic id that was not in this piggyback ShareFetch is skipped the
/// same way and does not deliver those records. A piggybacked
/// `FENCED_LEADER_EPOCH` / `NOT_LEADER_OR_FOLLOWER` /
/// `UNKNOWN_TOPIC_OR_PARTITION` / `UNKNOWN_TOPIC_ID` /
/// `INCONSISTENT_TOPIC_ID`
/// `AcknowledgeErrorCode` with a usable
/// CurrentLeader does not update the next ShareFetch target (Java
/// `handleShareFetchSuccess` applies CurrentLeader only for fetch
/// `errorCode`). A piggybacked `FENCED_LEADER_EPOCH` /
/// `NOT_LEADER_OR_FOLLOWER` / `UNKNOWN_TOPIC_OR_PARTITION` /
/// `UNKNOWN_TOPIC_ID`
/// `AcknowledgeErrorCode` with a usable CurrentLeader on one
/// partition does not update the next ShareFetch target for a sibling
/// whose `AcknowledgeErrorCode` is none. A partition-level
/// ShareFetch error does not discard sibling partitions in the same
/// response (Java `testMultipleTopicsFetch` / `testMultipleTopicsFetchError` /
/// `ShareFetchCollector.collect`). Partition `NOT_LEADER_OR_FOLLOWER`
/// / `FENCED_LEADER_EPOCH` with a usable CurrentLeader (`leaderId != -1`
/// && `leaderEpoch != -1`) plus NodeEndpoints updates the next ShareFetch
/// target without a Metadata refresh (Java `handleShareFetchSuccess` /
/// KIP-951). `UNKNOWN_LEADER_EPOCH` / `UNKNOWN_SERVER_ERROR` skip without
/// requesting metadata (Java `handleInitializeErrors`), including when
/// a partition `ErrorMessage` is present. A skip-with-metadata code
/// such as `NOT_LEADER_OR_FOLLOWER` / `INCONSISTENT_TOPIC_ID` /
/// `UNKNOWN_TOPIC_OR_PARTITION` / partition `UNKNOWN_TOPIC_ID` /
/// `FENCED_LEADER_EPOCH` without CurrentLeader / `REPLICA_NOT_AVAILABLE`
/// / `OFFSET_NOT_AVAILABLE` / `KAFKA_STORAGE_ERROR` still skips and
/// still requests Metadata when `ErrorMessage` is set (Java
/// `testFetchError` / `handleInitializeErrors`). A throwable
/// partition `ErrorMessage` is the broker exception text (Java
/// `Errors.forCode.exception(errorMessage)`); empty uses
/// `ShareFetch {topic}-{partition}`. A top-level ShareFetch
/// `ErrorMessage` is used the same way; empty uses `ShareFetch`.
/// [`Self::accept`] / [`Self::release`] / [`Self::reject`] are Java
/// `acknowledge` (local until the next poll, commit, or leave).
/// Explicit mode requires those acknowledgements before the next
/// broker poll. [`Self::poll`] returns [`Error::MaxPollInterval`] when
/// [`ConsumerConfig::max_poll_interval`] elapses (Java
/// `max.poll.interval.ms`).
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
    /// Java `ShareSessionHandler.sessionPartitions` per share-partition leader.
    session_partitions: HashMap<i32, HashSet<([u8; 16], i32)>>,
    share_conns: HashMap<i32, ShareNodeConnection>,
    share_addresses: HashMap<i32, String>,
    /// KIP-951 CurrentLeader overrides from ShareFetch (Java
    /// `handleShareFetchSuccess`). Preferred over cached Metadata.
    share_leader_overrides: HashMap<(String, i32), i32>,
    acquisitions: HashMap<AcquisitionKey, Acquisition>,
    /// Java `ShareFetchBuffer`: successful partitions and collectable
    /// partition errors, in ShareFetch response order.
    pending_completed: VecDeque<ShareCompletedFetch>,
    acquire_mode: ShareAcquireMode,
    acknowledgement_mode: ShareAcknowledgementMode,
    last_poll_unacked: Vec<ShareRecord>,
    /// Java `ShareConsumer.acknowledge` queue (Accept/Release/Reject).
    pending_acknowledgements: BTreeMap<(String, i32, i64), (ShareRecord, i8)>,
    /// Java `ShareCompletedFetch` GAP acknowledgements collected during
    /// this ShareFetch. Promoted after piggyback completion so they are
    /// not marked sent on the fetch that discovered them.
    collected_gap_acknowledgements: Vec<ShareRecord>,
    /// Java `ShareCompletedFetch` REJECT acknowledgements for CRC-failed
    /// batches. Promoted after piggyback like GAP acks.
    collected_reject_acknowledgements: Vec<ShareRecord>,
    /// Offsets already counted in [`Self::records_acknowledged`] at queue time.
    counted_acknowledgements: HashSet<(String, i32, i64)>,
    /// Java `ShareAcknowledgeAsyncEvent`: flush on the next poll / leave.
    async_commit: bool,
    ack_commit_callback: Option<AcknowledgementCommitFn>,
    acquisition_lock_timeout_ms: Option<i32>,
    hb_err: Arc<AtomicI16>,
    hb_epoch: Arc<AtomicI32>,
    /// Pending assignment from the background heartbeat task.
    hb_assignment: Arc<Mutex<Option<Vec<ShareTopicPartitions>>>>,
    hb_interval_ms: Arc<AtomicI32>,
    hb_deadline: Arc<Mutex<Option<Instant>>>,
    hb_wake: Arc<Notify>,
    hb_stop: watch::Sender<bool>,
    /// Last successful [`Self::poll`] start. Java `max.poll.interval.ms`.
    last_poll: Arc<Mutex<Option<Instant>>>,
    /// Heartbeat task left after [`ConsumerConfig::max_poll_interval`].
    left_max_poll: Arc<AtomicBool>,
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

struct PreparedShareFetch {
    node: i32,
    version: i16,
    by_id: BTreeMap<[u8; 16], Vec<i32>>,
    ack_tps: Vec<(String, i32)>,
    request: BytesMut,
}

/// One Java `ShareCompletedFetch` (records or a collectable partition error).
enum ShareCompletedFetch {
    Records(VecDeque<ShareRecord>),
    Error(Error),
}

/// Java `ShareConsumeRequestManager.INVALID_RESPONSE`.
const SHARE_ACK_INVALID_RESPONSE: &str =
    "Acknowledgement not successful due to invalid response from broker";

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
        let last_poll = Arc::new(Mutex::new(None));
        let left_max_poll = Arc::new(AtomicBool::new(false));
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
            session_partitions: HashMap::new(),
            share_conns: HashMap::new(),
            share_addresses: HashMap::new(),
            share_leader_overrides: HashMap::new(),
            acquisitions: HashMap::new(),
            pending_completed: VecDeque::new(),
            acquire_mode: ShareAcquireMode::BatchOptimized,
            acknowledgement_mode: cfg.share_acknowledgement_mode,
            last_poll_unacked: Vec::new(),
            pending_acknowledgements: BTreeMap::new(),
            collected_gap_acknowledgements: Vec::new(),
            collected_reject_acknowledgements: Vec::new(),
            counted_acknowledgements: HashSet::new(),
            async_commit: false,
            ack_commit_callback: None,
            acquisition_lock_timeout_ms: None,
            hb_err,
            hb_epoch,
            hb_assignment,
            hb_interval_ms,
            hb_deadline,
            hb_wake,
            hb_stop,
            last_poll,
            left_max_poll,
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
    /// [`ShareAcquireMode::RecordLimit`] fetches records from one leader
    /// per poll (Java `fetchRecordsNodeId`).
    pub fn set_acquire_mode(&mut self, mode: ShareAcquireMode) {
        self.acquire_mode = mode;
    }

    /// Java `share.acknowledgement.mode`.
    pub fn set_acknowledgement_mode(&mut self, mode: ShareAcknowledgementMode) {
        self.acknowledgement_mode = mode;
        if mode == ShareAcknowledgementMode::Explicit {
            self.last_poll_unacked.clear();
        }
    }

    /// Java `share.acknowledgement.mode`.
    #[must_use]
    pub fn acknowledgement_mode(&self) -> ShareAcknowledgementMode {
        self.acknowledgement_mode
    }

    /// Java `ShareConsumer.setAcknowledgementCommitCallback`.
    ///
    /// Invoked after a ShareAcknowledge RPC completes (success or error).
    /// Each call covers one topic-partition (Java
    /// `testAcknowledgementCommitCallbackMultiplePartitionCommitAsync`
    /// / `ResultHandler.complete` for `COMMIT_ASYNC`). Mixed per-partition
    /// outcomes use that partition's exception, not the first error for
    /// every offset. Client-side rejects before a request is sent
    /// (`Acknowledge called before poll.`, empty input) do not invoke
    /// the callback. Pass a new callback to replace; use
    /// [`Self::clear_acknowledgement_commit_callback`] for Java `null`.
    pub fn set_acknowledgement_commit_callback<F>(&mut self, callback: F)
    where
        F: FnMut(
                Vec<(crate::TopicIdPartition, Vec<crate::OffsetAndMetadata>)>,
                Option<AcknowledgementCommitError>,
            ) + Send
            + 'static,
    {
        self.ack_commit_callback = Some(Box::new(callback));
    }

    /// Java `ShareConsumer.setAcknowledgementCommitCallback` for a named
    /// [`AcknowledgementCommitCallback`] implementation.
    pub fn set_acknowledgement_commit_handler(
        &mut self,
        mut callback: impl AcknowledgementCommitCallback + 'static,
    ) {
        self.set_acknowledgement_commit_callback(move |offsets, exception| {
            callback.on_complete(offsets, exception);
        });
    }

    /// Java `ShareConsumer.setAcknowledgementCommitCallback(null)`.
    pub fn clear_acknowledgement_commit_callback(&mut self) {
        self.ack_commit_callback = None;
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

    /// Share member epoch (Java `ConsumerGroupMetadata.generationId` for
    /// share groups). Heartbeat-updated epoch wins when it is positive.
    #[must_use]
    pub fn member_epoch(&self) -> i32 {
        let latest = self.hb_epoch.load(Ordering::SeqCst);
        if latest > 0 {
            latest
        } else {
            self.member_epoch
        }
    }

    /// Same as [`Self::member_epoch`] (Java `generationId`).
    #[must_use]
    pub fn generation_id(&self) -> i32 {
        self.member_epoch()
    }

    /// Snapshot of group id, member id, member epoch, and instance id
    /// (Java `ShareConsumer.groupMetadata()`).
    #[must_use]
    pub fn group_metadata(&self) -> crate::ConsumerGroupMetadata {
        crate::ConsumerGroupMetadata {
            group_id: self.group_id.clone(),
            generation_id: self.member_epoch(),
            member_id: self.member_id.clone(),
            group_instance_id: self.cfg.group_instance_id.clone(),
        }
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

    /// True after the heartbeat task left because
    /// [`ConsumerConfig::max_poll_interval`] elapsed (Java
    /// `max.poll.interval.ms`).
    #[must_use]
    pub fn is_left_max_poll(&self) -> bool {
        self.left_max_poll.load(Ordering::SeqCst)
    }

    fn check_max_poll_interval(&self) -> Result<()> {
        let interval = self.cfg.max_poll_interval;
        let mut last = self.last_poll.lock();
        if !interval.is_zero() {
            if let Some(t) = *last {
                if t.elapsed() > interval {
                    return Err(Error::MaxPollInterval);
                }
            }
        }
        *last = Some(Instant::now());
        Ok(())
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
            self.pending_completed.clear();
            self.pending_acknowledgements.clear();
            self.collected_gap_acknowledgements.clear();
            self.collected_reject_acknowledgements.clear();
            self.counted_acknowledgements.clear();
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
        // Java still acknowledges a previous topic after subscribe
        // (testCloseWithSubscriptionChange). Keep those acquisitions.
        self.acquisitions.retain(|(topic, part, offset), _| {
            self.assigned.iter().any(|(t, p)| t == topic && p == part)
                || self
                    .pending_acknowledgements
                    .contains_key(&(topic.clone(), *part, *offset))
        });
        retain_completed_records(&mut self.pending_completed, |r| {
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
    /// `max_poll_records` caps each returned poll and the ShareFetch
    /// `MaxRecords` and `BatchSize` fields. When unset, both stay at the
    /// legacy 16 records per broker. `max_bytes` and
    /// `max_partition_fetch_bytes` are the request byte limits.
    ///
    /// Not subscribed is Java `IllegalStateException` (`Consumer is not
    /// subscribed to any topics.`). Returns [`Error::MaxPollInterval`] when
    /// [`ConsumerConfig::max_poll_interval`] elapsed since the last poll
    /// (Java `max.poll.interval.ms`; the heartbeat task also leaves). In
    /// [`ShareAcknowledgementMode::Explicit`], a later poll that would
    /// contact a broker is Java `IllegalStateException` (`All records must
    /// be acknowledged in explicit acknowledgement mode.`) until every
    /// previously delivered record is [`Self::accept`] / [`Self::release`]
    /// / [`Self::reject`] (local; Renew does not count). Draining records
    /// already buffered from the last ShareFetch is allowed without that
    /// acknowledgement. A ShareFetch `AcknowledgeErrorCode` completes the
    /// piggybacked acknowledgements (commit callback) and still delivers
    /// this fetch; [`Self::poll`] does not return that error.
    /// After [`Self::subscribe`] replaces the topic list, leftover
    /// acknowledgements for a previous topic still piggyback on that
    /// topic's leader while the new assignment is fetched (Java
    /// `testShareFetchWithSubscriptionChangeMultipleNodes`). With no
    /// leftover acks, only the new leader is fetched (Java
    /// `testShareFetchWithSubscriptionChangeMultipleNodesEmptyAcknowledgements`).
    /// Piggyback is refused on an initial share session and those
    /// acknowledgements complete with `NOT_LEADER_OR_FOLLOWER` (Java
    /// `testPiggybackAcknowledgementsOnInitialShareSessionError`).
    /// Leftover acknowledgements for a previous topic whose leader has
    /// no share session complete the same way and that leader is not
    /// fetched (Java
    /// `testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange`).
    /// Leftover acknowledgements whose metadata leader has changed
    /// complete the same way and are not piggybacked on the new leader
    /// (Java `testWhenLeadershipChangeBetweenShareFetchRequests`).
    /// A partition-level error (Java `ShareFetchCollector.collect` /
    /// `testMultipleTopicsFetch`) leaves successful sibling partitions
    /// deliverable. The error is returned on this poll when it is first
    /// in the completed-fetch buffer, otherwise on a later poll.
    /// `TOPIC_AUTHORIZATION_FAILED` is Java `TopicAuthorizationException`;
    /// [`Error::unauthorized_topics`] names the topic (Java
    /// `testUnauthorizedTopic`). Partition `CORRUPT_MESSAGE` is Java
    /// `KafkaException` and does not deliver the accompanying records
    /// (Java `testCorruptMessageError`). That is distinct from a CRC-failed
    /// batch (`error_code` 0), which REJECT-intersects acquired offsets
    /// and continues later batches. A sole CRC-failed batch throws
    /// once (`Record is corrupt`) and the next poll does not throw again
    /// (Java `testInvalidDefaultRecordBatch` / `testParseInvalidRecordBatch`).
    /// Partition `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` without
    /// a usable CurrentLeader (`leaderId == -1` or `leaderEpoch == -1`)
    /// skips that partition, still delivers siblings, and requests
    /// Metadata (Java
    /// `testWhenShareFetchResponseReturnsALeadershipChangeErrorButNoNewLeaderInformation`,
    /// including `FENCED_LEADER_EPOCH`). A usable CurrentLeader on
    /// `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` redirects the
    /// next ShareFetch without Metadata (Java
    /// `testWhenFetchResponseReturnsWithALeadershipChangeErrorAndNewLeaderInformation`).
    /// `UNKNOWN_LEADER_EPOCH` skips without Metadata; partition
    /// `INCONSISTENT_TOPIC_ID` / `UNKNOWN_TOPIC_OR_PARTITION` /
    /// `UNKNOWN_TOPIC_ID` / `REPLICA_NOT_AVAILABLE` /
    /// `OFFSET_NOT_AVAILABLE` / `KAFKA_STORAGE_ERROR` skip and request
    /// Metadata, including when a partition `ErrorMessage` is set;
    /// siblings still deliver (Java `handleInitializeErrors` /
    /// `ShareFetchCollector.collect`).
    /// Top-level `UNKNOWN_TOPIC_ID` still fails that poll and refreshes
    /// the topic id, including when `ErrorMessage` is set or empty.
    /// Top-level
    /// `INCONSISTENT_TOPIC_ID` still fails that poll without Metadata,
    /// including when `ErrorMessage` is set or empty (Java
    /// `handleShareFetchSuccess` only `requestUpdate`s for
    /// `UNKNOWN_TOPIC_ID`). A sole
    /// partition `NOT_LEADER_OR_FOLLOWER` collects empty (Java
    /// `testFetchError`). A ShareFetch topic id that is
    /// not in this request — including a real cluster topic that is
    /// not subscribed — is skipped and does not fail poll (Java
    /// `lookupTopicId` / `testShareFetchInvalidResponse`), including
    /// on a piggyback ShareFetch.
    pub async fn poll(&mut self) -> Result<ShareRecords> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        if self.left_max_poll.load(Ordering::SeqCst) {
            return Err(Error::MaxPollInterval);
        }
        self.check_max_poll_interval()?;
        if self.topics.is_empty() && self.topic_match.is_none() {
            return Err(reject_java_share_not_subscribed());
        }
        self.ensure_explicit_in_flight_acknowledged()?;
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
        if self.async_commit {
            self.snapshot_implicit_acknowledgements();
            self.async_commit = false;
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                self.flush_pending_acknowledgements(),
            )
            .await
            .map_err(|_| Error::Timeout)??;
        }
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
                    self.complete_poll_piggyback(&recs);
                    self.promote_collected_gap_acknowledgements();
                    self.promote_collected_reject_acknowledgements();
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
    /// Java `ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType.ACCEPT)`:
    /// local until the next [`Self::poll`] (piggybacked on ShareFetch),
    /// [`Self::commit`], [`Self::leave`], or [`Self::close`].
    /// Called before [`Self::poll`] is Java `IllegalStateException`
    /// (`Acknowledge called before poll.`).
    pub async fn accept(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, AcknowledgeType::Accept).await
    }

    /// Return records to the share (`RELEASE`).
    ///
    /// Java `ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType.RELEASE)`:
    /// local until the next [`Self::poll`], [`Self::commit`], [`Self::leave`],
    /// or [`Self::close`]. Called before [`Self::poll`] is the same Java
    /// `IllegalStateException` as [`Self::acknowledge`].
    pub async fn release(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, AcknowledgeType::Release).await
    }

    /// Reject records (`REJECT`, KIP-932).
    ///
    /// Java `ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType.REJECT)`:
    /// local until the next [`Self::poll`], [`Self::commit`], [`Self::leave`],
    /// or [`Self::close`]. Called before [`Self::poll`] is the same Java
    /// `IllegalStateException` as [`Self::acknowledge`].
    pub async fn reject(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, AcknowledgeType::Reject).await
    }

    /// Java `ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType)`.
    ///
    /// Accept / Release / Reject are local until the next [`Self::poll`],
    /// [`Self::commit`], [`Self::leave`], or [`Self::close`]. Renew is sent
    /// immediately so the acquisition lock is extended. Called before
    /// [`Self::poll`] is Java `IllegalStateException`
    /// (`Acknowledge called before poll.`). Called in
    /// [`ShareAcknowledgementMode::Implicit`] is Java `IllegalStateException`
    /// (`Implicit acknowledgement of delivery is being used.`).
    pub async fn acknowledge(&mut self, recs: &[ShareRecord], ack: AcknowledgeType) -> Result<()> {
        if ack == AcknowledgeType::Renew {
            return self.send_acknowledgements(recs, ack.id()).await;
        }
        if self.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            return Err(reject_java_acknowledge_in_implicit_mode());
        }
        self.queue_acknowledgements(recs, ack.id())
    }

    /// Extend the locks of acquired records without accepting/releasing them.
    /// Older peers return Unsupported before an acknowledgement is emitted.
    pub async fn renew(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, AcknowledgeType::Renew).await
    }

    /// Send pending acknowledgements now.
    ///
    /// Unlike [`Self::commit_sync`], the first partition error is [`Err`]
    /// and pending is restored so a later [`Self::commit`] /
    /// [`Self::poll`] can retry, except [`Error::Timeout`] (the broker
    /// may already have applied the acknowledgements). Java
    /// `commitSync()` returns the per-partition map — use
    /// [`Self::commit_sync`].
    ///
    /// Flushes implicit last-poll ACCEPT and queued
    /// [`Self::accept`] / [`Self::release`] / [`Self::reject`] without
    /// waiting for the next [`Self::poll`] or [`Self::leave`]. Leftover
    /// acknowledgements for a previous topic still flush after
    /// [`Self::subscribe`] and no ShareFetch is sent (Java
    /// `testCommitSyncWithSubscriptionChange`). After this flush,
    /// [`Self::leave`] / [`Self::close`] send only a FINAL-epoch session
    /// close — they do not ACCEPT the same records again (Java
    /// `testAcknowledgeOnCloseWithPendingCommitSync`).
    pub async fn commit(&mut self) -> Result<()> {
        self.flush_pending_acknowledgements().await
    }

    /// [`Self::commit`] with a one-shot timeout (Java `commitSync(Duration)`
    /// when the caller only wants success/failure).
    pub async fn commit_timeout(&mut self, timeout: Duration) -> Result<()> {
        let prev = self.cfg.request_timeout;
        self.cfg.request_timeout = timeout;
        let out = self.commit().await;
        self.cfg.request_timeout = prev;
        out
    }

    /// Java `ShareConsumer.commitSync()`:
    /// `Map<TopicIdPartition, Optional<KafkaException>>`.
    ///
    /// `None` is Java `Optional.empty()` (that partition succeeded).
    /// A present [`AcknowledgementCommitError`] is Java `Optional.of`.
    /// The map is empty when there is nothing to flush (no last implicit
    /// poll and no queued `accept` / `release` / `reject`).
    ///
    /// Broker per-partition failures stay in the map ([`Ok`]). Overall
    /// timeout / protocol / closed failures still return [`Err`]. Pending
    /// is completed for partitions that received a response.
    /// A prior [`Self::commit_async`] that has not been polled yet is
    /// flushed here (Java `testPendingCommitAsyncBeforeCommitSync`).
    /// A ShareAcknowledge partition `NOT_LEADER_OR_FOLLOWER` /
    /// `FENCED_LEADER_EPOCH` / `UNKNOWN_TOPIC_OR_PARTITION` /
    /// `UNKNOWN_TOPIC_ID` / `INCONSISTENT_TOPIC_ID` is not retried even
    /// when those codes are otherwise retriable; the map completes (Java
    /// `testFatalErrorsAcknowledgementResponse` plus ownership-lost
    /// topic-id codes), including when `ErrorMessage` is empty or a
    /// non-empty broker `ErrorMessage` is preferred.
    /// Metadata is requested when that error has no usable CurrentLeader.
    /// A successful ShareAcknowledge that omits a requested partition
    /// completes that partition with `INVALID_RECORD_STATE` (Java
    /// `testShareAcknowledgeInvalidResponse`). An omitted ShareAcknowledge
    /// partition does not fail a sibling partition that is present with
    /// error none. Unrequested response partitions, unknown topic ids,
    /// and known topic ids that were not in the request are skipped
    /// (Java `lookupTopicId` /
    /// `handleAcknowledgeErrorCode`). A non-empty partition
    /// `ErrorMessage` is preferred over the crate fallback; an empty
    /// or missing `ErrorMessage` uses `ShareAcknowledge {topic}-{partition}`
    /// (Java `Errors.exception(errorMessage)`). A top-level ShareAcknowledge
    /// `ErrorMessage` is preferred the same way; empty uses
    /// `ShareAcknowledge`. Top-level `SHARE_SESSION_LIMIT_REACHED`
    /// completes without retry and advances the session epoch (Java
    /// `ShareSessionHandler.handleResponse` `nextEpoch`), including
    /// when `ErrorMessage` is empty; it is not a
    /// ShareAcknowledge session reset. Top-level
    /// `SHARE_SESSION_NOT_FOUND` / `INVALID_SHARE_SESSION_EPOCH`
    /// still reset, including when `ErrorMessage` is set or empty.
    pub async fn commit_sync(&mut self) -> Result<AcknowledgementCommitResults> {
        if !self.has_pending_acknowledgements() {
            return Ok(Vec::new());
        }
        let (implicit, explicit) = self.take_pending_acknowledgements();
        let typed = pending_typed(&implicit, &explicit);
        let recs: Vec<ShareRecord> = typed.iter().map(|(rec, _)| rec.clone()).collect();
        match self.acknowledge_typed(&typed).await {
            Ok(outcomes) => Ok(self.acknowledgement_commit_results(&recs, &outcomes, None)),
            Err(error) => {
                self.restore_pending_acknowledgements(implicit, explicit);
                Err(error)
            }
        }
    }

    /// [`Self::commit_sync`] with a one-shot timeout (Java
    /// `commitSync(Duration)`).
    pub async fn commit_sync_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<AcknowledgementCommitResults> {
        let prev = self.cfg.request_timeout;
        self.cfg.request_timeout = timeout;
        let out = self.commit_sync().await;
        self.cfg.request_timeout = prev;
        out
    }

    /// Java `ShareConsumer.commitAsync()` (`ShareAcknowledgeAsyncEvent`).
    ///
    /// Marks implicit last-poll records as ACCEPT immediately (Java
    /// `acknowledgeBatchIfImplicitAcknowledgement`) but does not send
    /// now. The next [`Self::poll`] flushes pending acknowledgements as
    /// a standalone ShareAcknowledge (then fetches). After
    /// [`Self::subscribe`], that flush still acknowledges the previous
    /// topic and the following ShareFetch is only the new assignment
    /// (Java `testCommitAsyncWithSubscriptionChange`).
    /// [`Self::commit_sync`] flushes that snapshot without waiting for
    /// poll (Java `testPendingCommitAsyncBeforeCommitSync`). After a
    /// metadata leader change, that flush fails leftover acknowledgements
    /// whose acquisition node is no longer the Metadata leader
    /// (`NOT_LEADER_OR_FOLLOWER`) and sends only partitions that are
    /// still on that node (Java
    /// `testLeadershipChangeAfterFetchBeforeCommitAsync`). [`Self::leave`],
    /// [`Self::close`], and [`Self::unsubscribe`] also flush those queued
    /// acknowledgements (Java `testAcknowledgeOnCloseWithPendingCommitAsync`).
    /// Implicit last-poll records that were never committed are released
    /// without ACCEPT on leave. Same callback as
    /// [`Self::set_acknowledgement_commit_callback`], invoked once per
    /// topic-partition when the snapshot is flushed (Java
    /// `testAcknowledgementCommitCallbackMultiplePartitionCommitAsync`).
    /// Does not spawn a task.
    pub fn commit_async(&mut self) {
        self.snapshot_implicit_acknowledgements();
        self.async_commit = true;
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
        retain_completed_records(&mut self.pending_completed, |r| {
            self.acquisitions
                .get(&(r.topic.clone(), r.partition, r.offset))
                .is_some_and(|a| a.delivery_count == r.delivery_count)
        });
    }

    /// Java `ShareFetchCollector.collect`: drain records until
    /// `max_poll_records`. A collectable partition error at the front
    /// is returned when no records have been taken yet; otherwise it
    /// stays for the next poll (Java `testMultipleTopicsFetch`).
    fn collect_completed_fetches(&mut self) -> Result<Vec<ShareRecord>> {
        let limit = self.cfg.max_poll_records.unwrap_or(usize::MAX);
        let mut out = Vec::new();
        loop {
            if out.len() >= limit {
                break;
            }
            match self.pending_completed.front() {
                None => break,
                Some(ShareCompletedFetch::Error(_)) if out.is_empty() => {
                    let error = match self.pending_completed.pop_front() {
                        Some(ShareCompletedFetch::Error(error)) => error,
                        _ => break,
                    };
                    return Err(error);
                }
                Some(ShareCompletedFetch::Error(_)) => break,
                Some(ShareCompletedFetch::Records(_)) => {
                    let Some(ShareCompletedFetch::Records(recs)) =
                        self.pending_completed.front_mut()
                    else {
                        break;
                    };
                    let take = limit.saturating_sub(out.len()).min(recs.len());
                    out.extend(recs.drain(..take));
                    if recs.is_empty() {
                        let _ = self.pending_completed.pop_front();
                    }
                }
            }
        }
        Ok(out)
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
        if !self.share_conns.contains_key(&node) {
            let addr = self.share_broker_addr(node).await?;
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
            // share_roundtrip is BrokerConn::roundtrip, so session lifetimes
            // are honored before Fetch/Acknowledge (KL06-12).
            let _ = self.share_addresses.insert(node, addr);
            let _ = self.share_conns.insert(
                node,
                ShareNodeConnection {
                    conn,
                    fetch_version,
                    acknowledge_version,
                },
            );
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

    fn remember_share_session(&mut self, node: i32, by_id: &BTreeMap<[u8; 16], Vec<i32>>) {
        let requested = share_session_requested(by_id);
        if requested.is_empty() {
            let _ = self.session_partitions.remove(&node);
        } else {
            let _ = self.session_partitions.insert(node, requested);
        }
    }

    fn reset_node_session(&mut self, node: i32) {
        let _ = self.share_epochs.remove(&node);
        let _ = self.session_partitions.remove(&node);
        let _ = self.share_conns.remove(&node);
        self.acquisitions
            .retain(|_, acquisition| acquisition.node != node);
        retain_completed_records(&mut self.pending_completed, |r| {
            self.acquisitions
                .contains_key(&(r.topic.clone(), r.partition, r.offset))
        });
        self.consumer.drop_node(node);
    }

    /// Java `handleShareFetchFailure`: complete in-flight piggybacked
    /// acknowledgements, then drop the share session.
    fn fail_share_fetch_request(&mut self, node: i32, ack_tps: &[(String, i32)], error: &Error) {
        self.fail_node_share_fetch_acknowledgements(ack_tps, error);
        self.reset_node_session(node);
    }

    async fn refresh_assigned_metadata(&mut self) -> Result<()> {
        let topics = self.topics.clone();
        for t in &topics {
            self.invalidate_share_topic(t);
        }
        self.consumer.refresh_topics(&topics).await
    }

    fn invalidate_share_topic(&mut self, topic: &str) {
        self.share_leader_overrides
            .retain(|(name, _), _| name != topic);
        self.consumer.invalidate_topic(topic);
    }

    fn remember_share_endpoints(&mut self, endpoints: &[crate::protocol::api::NodeEndpoint]) {
        for endpoint in endpoints {
            if endpoint.node_id < 0 || endpoint.host.is_empty() || endpoint.port <= 0 {
                continue;
            }
            let _ = self.share_addresses.insert(
                endpoint.node_id,
                format!("{}:{}", endpoint.host, endpoint.port),
            );
        }
    }

    fn lookup_share_broker_addr(&self, node: i32) -> Option<String> {
        self.assigned
            .iter()
            .find_map(|(topic, part)| {
                self.consumer
                    .leader_of(topic, *part)
                    .ok()
                    .filter(|(id, _)| *id == node)
                    .map(|(_, addr)| addr)
            })
            .or_else(|| self.share_addresses.get(&node).cloned())
    }

    /// Resolve a share broker address. A KIP-951 CurrentLeader may name a
    /// node we have not fetched from; refresh Metadata without dropping
    /// overrides so `leader_of` can supply the cached cluster address
    /// (Java `updatePartitionLeadership` uses brokers already in the
    /// cluster snapshot).
    async fn share_broker_addr(&mut self, node: i32) -> Result<String> {
        if let Some(addr) = self.lookup_share_broker_addr(node) {
            return Ok(addr);
        }
        let topics = self.topics.clone();
        for topic in &topics {
            self.consumer.invalidate_topic(topic);
        }
        self.consumer.refresh_topics(&topics).await?;
        self.lookup_share_broker_addr(node)
            .ok_or_else(|| Error::protocol(format!("unknown share broker {node}")))
    }

    /// Java `handleShareFetchSuccess`: apply CurrentLeader when the
    /// partition error is `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH`
    /// and `leaderId != -1 && leaderEpoch != -1`.
    fn apply_share_current_leader(
        &mut self,
        topic: &str,
        partition: i32,
        leader_id: i32,
        leader_epoch: i32,
    ) -> bool {
        if leader_id == -1 || leader_epoch == -1 {
            return false;
        }
        let _ = self
            .share_leader_overrides
            .insert((topic.to_string(), partition), leader_id);
        true
    }

    fn share_leader_node(&self, topic: &str, partition: i32) -> Result<i32> {
        if let Some(node) = self
            .share_leader_overrides
            .get(&(topic.to_string(), partition))
        {
            return Ok(*node);
        }
        Ok(self.consumer.leader_of(topic, partition)?.0)
    }

    /// Fetch-node that acquired this topic-partition, if any lock remains.
    fn share_ack_fetch_node(&self, topic: &str, partition: i32) -> Option<i32> {
        self.acquisitions
            .iter()
            .find_map(|((name, part, _), acquired)| {
                (name == topic && *part == partition).then_some(acquired.node)
            })
    }

    /// Java `isLeaderKnownToHaveChanged`: Metadata already names a
    /// different leader than the node that issued the acquisition.
    fn share_ack_leader_changed(&self, topic: &str, partition: i32) -> bool {
        share_ack_fetch_leader_changed(
            self.share_ack_fetch_node(topic, partition),
            self.share_leader_node(topic, partition).ok(),
        )
    }

    /// Drop acquisitions covered by these acknowledgement batches
    /// (Java ownership-lost / omitted / known leader change).
    fn drop_acknowledged_acquisitions(
        &mut self,
        name: &str,
        partition: i32,
        batches: &[AcknowledgementBatch],
    ) {
        let in_batches = |offset: i64| {
            batches
                .iter()
                .any(|batch| offset >= batch.first_offset && offset <= batch.last_offset)
        };
        let matched: Vec<(String, i32, i64)> = self
            .acquisitions
            .keys()
            .filter(|(topic, part, offset)| {
                topic == name && *part == partition && in_batches(*offset)
            })
            .cloned()
            .collect();
        self.uncount_acknowledgements(matched);
        self.acquisitions.retain(|(topic, part, offset), _| {
            !(topic == name && *part == partition && in_batches(*offset))
        });
    }

    async fn leaders_of(
        &mut self,
        tps: &[(String, i32)],
    ) -> Result<HashMap<i32, Vec<(String, i32)>>> {
        for (topic, _) in tps {
            self.consumer.ensure_topic_metadata(topic).await?;
        }
        // Metadata is the source of topic ids (Java top-level
        // UNKNOWN_TOPIC_ID / partition INCONSISTENT_TOPIC_ID refresh).
        // Assignment may still hold the id from join.
        let name_to_id = self.consumer.topic_name_ids();
        for (topic, _) in tps {
            if let Some(id) = name_to_id.get(topic).copied().filter(|id| *id != [0; 16]) {
                let _ = self.topic_ids.insert(topic.clone(), id);
            }
        }
        let mut by_leader: HashMap<i32, Vec<(String, i32)>> = HashMap::new();
        for (topic, p) in tps {
            let node = self.share_leader_node(topic, *p)?;
            by_leader.entry(node).or_default().push((topic.clone(), *p));
        }
        Ok(by_leader)
    }

    /// Java `fetchRecordsNodeId` / `subscriptions.movePartitionToEnd`.
    ///
    /// [`ShareAcquireMode::RecordLimit`] fetches records from the first
    /// assigned partition whose leader is in this poll, then rotates that
    /// partition so the next poll starts on a different leader.
    fn record_limit_fetch_node(&mut self, nodes: &[(i32, Vec<(String, i32)>)]) -> Option<i32> {
        if self.acquire_mode != ShareAcquireMode::RecordLimit {
            return None;
        }
        let present: HashSet<i32> = nodes.iter().map(|(node, _)| *node).collect();
        let assigned = self.assigned.clone();
        for (topic, part) in assigned {
            let Ok(node) = self.share_leader_node(&topic, part) else {
                continue;
            };
            if !present.contains(&node) {
                continue;
            }
            if let Some(idx) = self
                .assigned
                .iter()
                .position(|(name, partition)| *name == topic && *partition == part)
            {
                let tp = self.assigned.remove(idx);
                self.assigned.push(tp);
            }
            return Some(node);
        }
        None
    }

    async fn overlap_share_fetches(
        &mut self,
        prepared: &[PreparedShareFetch],
    ) -> HashMap<i32, Result<Bytes>> {
        let timeout = self.cfg.request_timeout;
        let mut set = tokio::task::JoinSet::new();
        for job in prepared {
            let Some(mut peer) = self.share_conns.remove(&job.node) else {
                continue;
            };
            let node = job.node;
            let version = job.version;
            let request = job.request.clone();
            let _ = set.spawn(async move {
                let result = peer
                    .conn
                    .roundtrip(
                        SHARE_FETCH,
                        version,
                        |buf| {
                            buf.extend_from_slice(&request);
                            Ok(())
                        },
                        timeout,
                    )
                    .await;
                (node, peer, result)
            });
        }
        let mut out = HashMap::new();
        while let Some(joined) = set.join_next().await {
            let Ok((node, peer, result)) = joined else {
                continue;
            };
            let _ = self.share_conns.insert(node, peer);
            let _ = out.insert(node, result);
        }
        out
    }

    async fn poll_leaders(&mut self) -> Result<Vec<ShareRecord>> {
        if !self.pending_completed.is_empty() {
            // Java returns the completed-fetch buffer without a ShareFetch.
            // Queued acknowledgements stay local until the next ShareFetch
            // piggyback, commit, or leave. A leftover partition error is
            // collected here (Java `testMultipleTopicsFetch`).
            return self.collect_completed_fetches();
        }
        let assigned = self.assigned.clone();
        let mut fetch_tps = assigned.clone();
        // Java isLeaderKnownToHaveChanged: leftover acks follow the
        // acquisition node. A metadata leader change fails them with
        // NOT_LEADER_OR_FOLLOWER and they are not piggybacked on the
        // new leader (testWhenLeadershipChangeBetweenShareFetchRequests).
        let mut changed = Vec::new();
        let mut seen_changed = HashSet::new();
        for rec in self.pending_ack_records() {
            let tp = (rec.topic.clone(), rec.partition);
            if seen_changed.insert(tp.clone()) && self.share_ack_leader_changed(&tp.0, tp.1) {
                changed.push(tp);
            }
        }
        if !changed.is_empty() {
            let error = Error::broker(
                error::NOT_LEADER_OR_FOLLOWER,
                "cannot acknowledge on a different share leader",
            );
            self.fail_node_share_fetch_acknowledgements(&changed, &error);
            for (topic, part) in &changed {
                self.acquisitions
                    .retain(|(name, partition, _), _| !(name == topic && *partition == *part));
            }
        }
        for rec in self.pending_ack_records() {
            if !fetch_tps
                .iter()
                .any(|(topic, part)| topic == &rec.topic && *part == rec.partition)
            {
                fetch_tps.push((rec.topic.clone(), rec.partition));
            }
        }
        let by_leader = self.leaders_of(&fetch_tps).await?;
        // Java ShareConsumeRequestManager.sendFetches only builds a
        // ShareFetch for nodes in the current assignment or leftover
        // acknowledgements (handlerMap). Abandoned sessions without
        // acks stay idle until the next fetch to that node or close
        // (testShareFetchWithSubscriptionChangeMultipleNodesEmptyAcknowledgements).
        let assigned_set: HashSet<(String, i32)> = assigned.into_iter().collect();
        let mut pending_acks = self.pending_fetch_acknowledgements();
        let mut nodes: Vec<_> = by_leader.into_iter().collect();
        nodes.sort_by_key(|(node, _)| *node);
        let record_limit_fetch_node = self.record_limit_fetch_node(&nodes);
        let metadata_bytes = self
            .acquisitions
            .keys()
            .map(|(topic, _, _)| acquisition_storage_bytes(topic))
            .fold(0usize, usize::saturating_add);
        let mut remaining_bytes = self
            .decoded_byte_limit()
            .checked_sub(metadata_bytes)
            .ok_or_else(|| Error::protocol("ShareFetch acquisition metadata budget exceeded"))?;
        let mut prepared = Vec::new();
        for (node, mut tps) in nodes {
            let epoch = self.session_epoch(node);
            // Java maybeAddAcknowledgements: piggyback is refused on the
            // initial ShareFetch (isNewSession). Those acks complete with
            // NOT_LEADER_OR_FOLLOWER and are not sent. Leftover-only
            // partitions are not added to that session, so the node is
            // not contacted
            // (testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange).
            let piggyback = epoch > ShareRequestMetadata::INITIAL_EPOCH;
            if !piggyback && !tps.is_empty() {
                let error = Error::broker(
                    error::NOT_LEADER_OR_FOLLOWER,
                    "cannot piggyback acknowledgements on initial ShareFetch",
                );
                self.fail_node_share_fetch_acknowledgements(&tps, &error);
                for (topic, part) in &tps {
                    let _ = pending_acks.remove(&(topic.clone(), *part));
                }
                tps.retain(|(topic, part)| assigned_set.contains(&(topic.clone(), *part)));
            }
            if tps.is_empty() && epoch <= ShareRequestMetadata::INITIAL_EPOCH {
                continue;
            }
            let version = match self.node_share_version(node, SHARE_FETCH).await {
                Ok(version) => version,
                Err(_) if tps.is_empty() => {
                    self.reset_node_session(node);
                    continue;
                }
                Err(error) => return Err(error),
            };
            if self.acquire_mode == ShareAcquireMode::RecordLimit && version < 2 && !tps.is_empty()
            {
                return Err(Error::Unsupported(format!(
                    "share broker {node} does not support record-limit acquisition"
                )));
            }
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
            let requested = share_session_requested(&by_id);
            let forgotten =
                share_session_forgotten(epoch, self.session_partitions.get(&node), &requested);
            if by_id.is_empty() && forgotten.is_empty() {
                continue;
            }
            let ack_only =
                record_limit_fetch_node.is_some() && Some(node) != record_limit_fetch_node;
            let has_acks = piggyback
                && tps.iter().any(|(topic, part)| {
                    pending_acks
                        .get(&(topic.clone(), *part))
                        .is_some_and(|batches| !batches.is_empty())
                });
            // Java newShareFetchBuilder(canSkipIfRequestEmpty): skip an
            // incremental ShareFetch with no added/removed/acks.
            if ack_only
                && epoch > ShareRequestMetadata::INITIAL_EPOCH
                && forgotten.is_empty()
                && !has_acks
            {
                continue;
            }
            let topics: Vec<_> = by_id
                .iter()
                .map(|(topic_id, partitions)| {
                    let name = self.name_for_topic_id(*topic_id);
                    ShareFetchTopic {
                        topic_id: *topic_id,
                        partitions: partitions
                            .iter()
                            .map(|part| ShareFetchPartition {
                                partition: *part,
                                partition_max_bytes: if assigned_set
                                    .contains(&(name.clone(), *part))
                                {
                                    self.cfg.max_partition_fetch_bytes
                                } else {
                                    0
                                },
                                acknowledgements: if piggyback {
                                    pending_acks
                                        .get(&(name.clone(), *part))
                                        .cloned()
                                        .unwrap_or_default()
                                } else {
                                    Vec::new()
                                },
                            })
                            .collect(),
                    }
                })
                .collect();
            let mut ack_tps = Vec::new();
            if piggyback {
                for (topic, part) in &tps {
                    if pending_acks
                        .get(&(topic.clone(), *part))
                        .is_some_and(|batches| !batches.is_empty())
                    {
                        ack_tps.push((topic.clone(), *part));
                    }
                }
            }
            let configured_records =
                i32::try_from(self.cfg.max_poll_records.unwrap_or(16).max(1)).unwrap_or(i32::MAX);
            // Java canSkipIfRequestEmpty: wait/byte/record limits are zero
            // on the non-fetch RecordLimit leader.
            let (max_wait_ms, min_bytes, max_bytes, max_records) = if ack_only {
                (0, 0, 0, 0)
            } else {
                (
                    self.cfg.max_wait_ms,
                    self.cfg.min_bytes,
                    self.cfg.max_bytes,
                    configured_records,
                )
            };
            let mut request = BytesMut::new();
            encode_share_fetch_request_with_options(
                &mut request,
                version,
                &self.group_id,
                &self.member_id,
                epoch,
                max_wait_ms,
                min_bytes,
                max_bytes,
                max_records,
                &topics,
                &forgotten,
                max_records,
                ShareFetchRequestOptions {
                    acquire_mode: self.acquire_mode as i8,
                    is_renew_ack: false,
                },
            )?;
            prepared.push(PreparedShareFetch {
                node,
                version,
                by_id,
                ack_tps,
                request,
            });
        }
        let mut overlapped = if prepared.len() > 1 {
            self.overlap_share_fetches(&prepared).await
        } else {
            HashMap::new()
        };
        let multi = prepared.len() > 1;
        for PreparedShareFetch {
            node,
            version,
            by_id,
            ack_tps,
            request,
        } in prepared
        {
            let body = if multi {
                match overlapped.remove(&node) {
                    Some(Ok(body)) => body,
                    Some(Err(error)) => {
                        self.fail_share_fetch_request(node, &ack_tps, &error);
                        return Err(error);
                    }
                    None => {
                        let error = Error::protocol("missing overlapped ShareFetch response");
                        self.fail_share_fetch_request(node, &ack_tps, &error);
                        return Err(error);
                    }
                }
            } else {
                match self
                    .share_roundtrip(node, SHARE_FETCH, version, &request)
                    .await
                {
                    Ok(body) => body,
                    Err(error) => {
                        self.fail_share_fetch_request(node, &ack_tps, &error);
                        return Err(error);
                    }
                }
            };
            let mut body = body;
            let (
                (fetched, endpoints, _, error_message, lock_timeout, error_code),
                decoded_bytes,
                corrupt_batches,
            ) = match decode_share_fetch_response_skipping_corrupt_batches(
                &mut body,
                version,
                remaining_bytes,
            ) {
                Ok(reply) => reply,
                Err(error) => {
                    self.fail_share_fetch_request(node, &ack_tps, &error);
                    return Err(error);
                }
            };
            self.remember_share_endpoints(&endpoints);
            if !body.is_empty() {
                let error = Error::protocol("trailing ShareFetch response bytes");
                self.fail_share_fetch_request(node, &ack_tps, &error);
                return Err(error);
            }
            if error_code != 0 {
                let error = share_partition_broker_error(
                    error_code,
                    error_message.as_deref(),
                    "ShareFetch",
                );
                // Java ShareSessionHandler.handleResponse false: complete
                // in-flight piggybacked acknowledgements with the top-level
                // code. The next fetch does not resend them.
                let failed: Vec<(String, i32)> = by_id
                    .iter()
                    .flat_map(|(id, parts)| {
                        let name = self.name_for_topic_id(*id);
                        parts.iter().map(move |part| (name.clone(), *part))
                    })
                    .collect();
                self.fail_node_share_fetch_acknowledgements(&failed, &error);
                if share_topic_id_refresh(error_code) {
                    // Java handleShareFetchSuccess: UNKNOWN_TOPIC_ID
                    // metadata.requestUpdate(false).
                    let mut seen = HashSet::new();
                    for (topic, _) in &failed {
                        if seen.insert(topic.clone()) {
                            self.invalidate_share_topic(topic);
                        }
                    }
                }
                if share_fetch_session_reset(&error) || share_leader_retriable(&error) {
                    self.reset_node_session(node);
                } else {
                    self.advance_node_epoch(node);
                    self.remember_share_session(node, &by_id);
                }
                return Err(error);
            }
            // A well-formed response consumed this epoch even when a later
            // partition outcome fails. Initial sessions must also be closable.
            self.advance_node_epoch(node);
            self.remember_share_session(node, &by_id);
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
            let mut seen_ack_tps = HashSet::new();
            let mut completed = Vec::new();
            for topic in fetched {
                // Java lookupTopicId: unknown topic id / partition is
                // skipped (testShareFetchInvalidResponse). poll does not
                // fail. Piggybacked acks for requested TPs stay missing
                // and complete as INVALID_RESPONSE below.
                let Some(requested) = by_id.get(&topic.topic_id) else {
                    continue;
                };
                let name = self.name_for_topic_id(topic.topic_id);
                for part in topic.partitions {
                    if !requested.contains(&part.partition) {
                        continue;
                    }
                    let _ = seen_ack_tps.insert((name.clone(), part.partition));
                    if part.error_code != 0 {
                        let applied_leader = share_fetch_current_leader_usable(part.error_code)
                            && self.apply_share_current_leader(
                                &name,
                                part.partition,
                                part.current_leader_id,
                                part.current_leader_epoch,
                            );
                        // Java handleInitializeErrors: skip-with-metadata
                        // codes request UpdateMetadata. A usable KIP-951
                        // CurrentLeader is applied instead (Java
                        // handleShareFetchSuccess). UNKNOWN_LEADER_EPOCH
                        // / UNKNOWN_SERVER_ERROR skip with no metadata.
                        if share_fetch_partition_metadata_refresh(part.error_code)
                            && !applied_leader
                        {
                            self.invalidate_share_topic(&name);
                        }
                        let error = share_fetch_collect_error(
                            part.error_code,
                            part.error_message.as_deref(),
                            &name,
                            part.partition,
                        );
                        if share_session_reset(&error) {
                            self.reset_node_session(node);
                        }
                        // Java ShareFetchCollector.handleInitializeErrors:
                        // NOT_LEADER / UNKNOWN_TOPIC_ID / … request a
                        // metadata update and do not throw. Sibling
                        // partitions stay collectable.
                        if share_fetch_partition_skipped(part.error_code) {
                            // Java handleInitializeErrors / testFetchError:
                            // skip codes do not throw. Returning a
                            // retriable error here would make poll()
                            // retry in-loop and deliver on this collect.
                            continue;
                        }
                        completed.push(ShareCompletedFetch::Error(error));
                        continue;
                    }
                    if part.acknowledge_error_code != 0 {
                        // Java ShareConsumeRequestManager completes the
                        // piggybacked acknowledgements and still delivers
                        // this ShareFetch. poll() does not throw.
                        // Errors.forCode(ack).exception(acknowledgeErrorMessage)
                        let detail = part
                            .acknowledge_error_message
                            .as_deref()
                            .filter(|msg| !msg.is_empty())
                            .map(str::to_owned)
                            .unwrap_or_else(|| {
                                format!("ShareFetch acknowledgement {name}-{}", part.partition)
                            });
                        let error = Error::broker(part.acknowledge_error_code, detail);
                        self.fail_share_fetch_acknowledgements(&name, part.partition, &error);
                    }
                    // Java ShareCompletedFetch logs overlapping ranges and
                    // keeps the first occurrence (testOverlappingAcquiredRecords).
                    let acquired = acquired::keep_first_acquired(&part.acquired)?;
                    let mut range_lookup = acquired::AcquisitionRanges::new(&acquired);
                    let mut present_offsets = HashSet::new();
                    let mut events: Vec<(i64, ShareFetchPartEvent)> = corrupt_batches
                        .iter()
                        .filter(|batch| {
                            batch.topic_id == topic.topic_id && batch.partition == part.partition
                        })
                        .map(|batch| {
                            (
                                batch.first_offset,
                                ShareFetchPartEvent::Corrupt {
                                    first: batch.first_offset,
                                    last: batch.last_offset,
                                },
                            )
                        })
                        .collect();
                    for batch in part.records {
                        events.push((batch.base_offset, ShareFetchPartEvent::Batch(batch)));
                    }
                    events.sort_by_key(|(offset, _)| *offset);
                    let mut part_records = Vec::new();
                    let flush_part_records =
                        |completed: &mut Vec<ShareCompletedFetch>, recs: &mut Vec<ShareRecord>| {
                            if !recs.is_empty() {
                                completed.push(ShareCompletedFetch::Records(
                                    std::mem::take(recs).into(),
                                ));
                            }
                        };
                    for (_, event) in events {
                        match event {
                            ShareFetchPartEvent::Corrupt { first, last } => {
                                flush_part_records(&mut completed, &mut part_records);
                                for (offset, delivery_count) in
                                    share_fetch_acquired_in_range(&acquired, first, last)?
                                {
                                    let key = (name.clone(), part.partition, offset);
                                    if !incoming_keys.insert(key.clone()) {
                                        continue;
                                    }
                                    let _ = present_offsets.insert(offset);
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
                                    let rec = share_gap_record(
                                        name.clone(),
                                        part.partition,
                                        offset,
                                        delivery_count,
                                    );
                                    incoming.push(rec.clone());
                                    self.collected_reject_acknowledgements.push(rec);
                                }
                                completed.push(ShareCompletedFetch::Error(Error::protocol(
                                    "Record is corrupt",
                                )));
                            }
                            ShareFetchPartEvent::Batch(batch) => {
                                // Java ShareCompletedFetch filters control
                                // records and acknowledges their acquired
                                // offsets as GAP.
                                if batch.is_control_batch() {
                                    continue;
                                }
                                let timestamp_type = batch.timestamp_type();
                                let leader_epoch = (batch.partition_leader_epoch >= 0)
                                    .then_some(batch.partition_leader_epoch);
                                for record in batch.records {
                                    // Java ShareCompletedFetch skips records
                                    // that are present but not acquired
                                    // (testUnaligned, testAcquiredRecords,
                                    // testAcquireOddRecords). Those offsets
                                    // are not ACK_GAP.
                                    let Some(delivery_count) =
                                        range_lookup.delivery_count(record.offset)
                                    else {
                                        continue;
                                    };
                                    let key = (name.clone(), part.partition, record.offset);
                                    if !incoming_keys.insert(key.clone()) {
                                        return Err(Error::protocol(
                                            "duplicate ShareFetch record offset",
                                        ));
                                    }
                                    let _ = present_offsets.insert(record.offset);
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
                                    let rec = ShareRecord {
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
                                    };
                                    incoming.push(rec.clone());
                                    part_records.push(rec);
                                }
                            }
                        }
                    }
                    flush_part_records(&mut completed, &mut part_records);
                    // Java ShareCompletedFetch: acquired offsets with no
                    // record (compaction / control holes) become ACK_GAP.
                    for (offset, delivery_count) in
                        share_fetch_missing_acquired(&acquired, &present_offsets)?
                    {
                        let key = (name.clone(), part.partition, offset);
                        if !incoming_keys.insert(key.clone()) {
                            continue;
                        }
                        if let Some(previous) = self.acquisitions.get(&key) {
                            if delivery_count < previous.delivery_count {
                                return Err(Error::protocol("ShareFetch delivery count regressed"));
                            }
                            if delivery_count == previous.delivery_count {
                                continue;
                            }
                        }
                        let rec =
                            share_gap_record(name.clone(), part.partition, offset, delivery_count);
                        incoming.push(rec.clone());
                        self.collected_gap_acknowledgements.push(rec);
                    }
                }
            }
            // Java handleShareFetchSuccess: piggybacked acknowledgements
            // missing from the response complete with
            // InvalidRecordStateException(INVALID_RESPONSE).
            let missing: Vec<(String, i32)> = ack_tps
                .into_iter()
                .filter(|tp| !seen_ack_tps.contains(tp))
                .collect();
            if !missing.is_empty() {
                let error = Error::broker(error::INVALID_RECORD_STATE, SHARE_ACK_INVALID_RESPONSE);
                self.fail_node_share_fetch_acknowledgements(&missing, &error);
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
            }
            for item in completed {
                self.pending_completed.push_back(item);
            }
        }
        self.collect_completed_fetches()
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
    /// Heartbeats stop and the assignment is cleared. Queued
    /// acknowledgements flush; implicit last-poll records that were
    /// never committed are released without ACCEPT. [`Self::subscribe`]
    /// joins again with a new topic list. [`Self::leave`] after this is
    /// a no-op.
    pub async fn unsubscribe(&mut self) -> Result<()> {
        if self.member_id.is_empty() {
            self.topic_match = None;
            self.topics.clear();
            self.assigned.clear();
            self.topic_ids.clear();
            self.share_epochs.clear();
            self.session_partitions.clear();
            self.share_conns.clear();
            self.share_leader_overrides.clear();
            self.acquisitions.clear();
            self.pending_completed.clear();
            self.last_poll_unacked.clear();
            self.pending_acknowledgements.clear();
            self.collected_gap_acknowledgements.clear();
            self.collected_reject_acknowledgements.clear();
            self.counted_acknowledgements.clear();
            self.async_commit = false;
            *self.hb_assignment.lock() = None;
            *self.hb_deadline.lock() = None;
            self.hb_interval_ms.store(0, Ordering::SeqCst);
            *self.last_poll.lock() = None;
            self.left_max_poll.store(false, Ordering::SeqCst);
            return Ok(());
        }
        self.topic_match = None;
        self.hb_stop.send(true).unwrap_or(());
        *self.hb_deadline.lock() = None;
        self.hb_interval_ms.store(0, Ordering::SeqCst);
        let acks = self.flush_close_acknowledgements().await;
        let close = self.close_share_session().await;
        let leave = if self.left_max_poll.load(Ordering::SeqCst) {
            Ok(())
        } else {
            self.leave_coordinator().await
        };
        self.assigned.clear();
        self.topics.clear();
        self.topic_ids.clear();
        self.share_epochs.clear();
        self.session_partitions.clear();
        self.member_id.clear();
        *self.hb_assignment.lock() = None;
        self.member_epoch = ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH;
        self.hb_epoch.store(
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            Ordering::SeqCst,
        );
        self.hb_err.store(0, Ordering::SeqCst);
        self.last_poll_unacked.clear();
        self.pending_acknowledgements.clear();
        self.collected_gap_acknowledgements.clear();
        self.collected_reject_acknowledgements.clear();
        self.counted_acknowledgements.clear();
        self.async_commit = false;
        *self.last_poll.lock() = None;
        self.left_max_poll.store(false, Ordering::SeqCst);
        acks.and(close).and(leave)
    }

    /// Replace the subscription (Java `subscribe`).
    ///
    /// An already-joined member keeps its share sessions. The next
    /// ShareFetch forgets partitions that left the assignment (Java
    /// `ShareSessionHandler`). Leftover acknowledgements for a previous
    /// topic still flush on the next [`Self::poll`], [`Self::commit`],
    /// or [`Self::leave`] (Java `testShareFetchWithSubscriptionChange` /
    /// `testCommitAsyncWithSubscriptionChange`). When the new topic is
    /// led by a different broker, that poll piggybacks leftover acks on
    /// the previous leader and ShareFetches the new topic on its leader
    /// (Java `testShareFetchWithSubscriptionChangeMultipleNodes`). With
    /// no leftover acks, only the new leader is fetched (Java
    /// `testShareFetchWithSubscriptionChangeMultipleNodesEmptyAcknowledgements`).
    /// Leftover acknowledgements whose leader has no share session
    /// complete with `NOT_LEADER_OR_FOLLOWER` and that leader is not
    /// fetched (Java
    /// `testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange`).
    /// After [`Self::unsubscribe`], this starts a new heartbeat loop.
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
        self.topics = topics;
        if rejoining {
            if self.member_epoch > 0 {
                self.heartbeat_subscription().await?;
            } else {
                self.close_share_session().await?;
                self.assigned.clear();
                self.topic_ids.clear();
                self.share_epochs.clear();
                self.session_partitions.clear();
                self.heartbeat_join().await?;
            }
            return Ok(());
        }
        self.member_id = Uuid::random_uuid().to_string();
        let (hb_stop, hb_rx) = watch::channel(false);
        self.hb_stop = hb_stop;
        self.heartbeat_join().await?;
        self.spawn_heartbeat(hb_rx);
        Ok(())
    }

    /// Heartbeat the new subscription without joining again or closing
    /// share sessions. When the coordinator omits the assignment (or the
    /// assigned topic set still does not match), rebuild from metadata —
    /// a share member fetches every subscribed partition.
    async fn heartbeat_subscription(&mut self) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        let version = spoken_share_group_heartbeat(self.coord.share_group_heartbeat_version)?;
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: self.member_epoch,
            rack_id: self.cfg.rack.clone(),
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
        if resp.heartbeat_interval_ms > 0 {
            self.hb_interval_ms
                .store(resp.heartbeat_interval_ms, Ordering::SeqCst);
            let interval =
                Duration::from_millis(u64::try_from(resp.heartbeat_interval_ms).unwrap_or(0));
            *self.hb_deadline.lock() = Some(Instant::now() + interval);
            self.hb_wake.notify_one();
        }
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
        if !self.assignment_matches_subscription() {
            let synthesized = self.assignment_from_subscription().await?;
            self.apply_share_assignment(Some(synthesized.as_slice()));
        }
        Ok(())
    }

    fn assignment_matches_subscription(&self) -> bool {
        let assigned: HashSet<&str> = self
            .assigned
            .iter()
            .map(|(topic, _)| topic.as_str())
            .collect();
        let wanted: HashSet<&str> = self.topics.iter().map(String::as_str).collect();
        assigned == wanted
    }

    async fn assignment_from_subscription(&mut self) -> Result<Vec<ShareTopicPartitions>> {
        self.consumer.refresh_topics(&self.topics).await?;
        let infos = self.consumer.list_topics().await?;
        let name_to_id = self.consumer.topic_name_ids();
        let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for info in infos {
            if self.topics.iter().any(|topic| topic == &info.topic) {
                by_topic.entry(info.topic).or_default().push(info.partition);
            }
        }
        let mut out = Vec::with_capacity(self.topics.len());
        for name in &self.topics {
            let Some(topic_id) = name_to_id.get(name).copied().filter(|id| *id != [0; 16]) else {
                continue;
            };
            let mut partitions = by_topic.remove(name).unwrap_or_default();
            partitions.sort_unstable();
            out.push(ShareTopicPartitions {
                topic_id,
                partitions,
            });
        }
        Ok(out)
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

    fn topic_id_partition(
        &self,
        topic: impl Into<String>,
        partition: i32,
    ) -> crate::TopicIdPartition {
        let topic = topic.into();
        let topic_id = self
            .topic_ids
            .get(&topic)
            .copied()
            .map(crate::Uuid::from_bytes)
            .unwrap_or(crate::Uuid::ZERO);
        crate::TopicIdPartition::from_topic(topic_id, partition, topic)
    }

    fn acknowledgement_commit_results(
        &self,
        recs: &[ShareRecord],
        outcomes: &BTreeMap<(String, i32), Option<Error>>,
        fallback: Option<&Error>,
    ) -> AcknowledgementCommitResults {
        let partitions: Vec<(String, i32)> = {
            let mut names = BTreeMap::new();
            for rec in recs {
                let _ = names.insert((rec.topic.clone(), rec.partition), ());
            }
            names.into_keys().collect()
        };
        partitions
            .into_iter()
            .map(|(topic, partition)| {
                let exception = match outcomes.get(&(topic.clone(), partition)) {
                    Some(None) => None,
                    Some(Some(error)) => Some(AcknowledgementCommitError::from_error(error)),
                    None => fallback.map(AcknowledgementCommitError::from_error),
                };
                (self.topic_id_partition(topic, partition), exception)
            })
            .collect()
    }

    fn fire_acknowledgement_commit_callback(
        &mut self,
        recs: &[ShareRecord],
        exception: Option<&Error>,
    ) {
        self.fire_acknowledgement_commit_outcomes(recs, &BTreeMap::new(), exception);
    }

    /// Java `ResultHandler.complete` for `COMMIT_ASYNC`: one
    /// `ShareAcknowledgementEvent` per topic-partition. Per-partition
    /// outcomes keep that partition's exception (a sibling success is
    /// not rewritten as the first error).
    fn fire_acknowledgement_commit_outcomes(
        &mut self,
        recs: &[ShareRecord],
        outcomes: &BTreeMap<(String, i32), Option<Error>>,
        fallback: Option<&Error>,
    ) {
        if self.ack_commit_callback.is_none() || recs.is_empty() {
            return;
        }
        let mut grouped: BTreeMap<(String, i32), Vec<i64>> = BTreeMap::new();
        for rec in recs {
            grouped
                .entry((rec.topic.clone(), rec.partition))
                .or_default()
                .push(rec.offset);
        }
        let payloads: Vec<_> = grouped
            .into_iter()
            .map(|((topic, partition), offsets)| {
                let exception = match outcomes.get(&(topic.clone(), partition)) {
                    Some(None) => None,
                    Some(Some(error)) => Some(AcknowledgementCommitError::from_error(error)),
                    None => fallback.map(AcknowledgementCommitError::from_error),
                };
                let offsets = offsets
                    .into_iter()
                    .map(crate::OffsetAndMetadata::new)
                    .collect();
                (
                    vec![(self.topic_id_partition(topic, partition), offsets)],
                    exception,
                )
            })
            .collect();
        if let Some(callback) = self.ack_commit_callback.as_mut() {
            for (offsets, exception) in payloads {
                callback(offsets, exception);
            }
        }
    }

    fn has_pending_acknowledgements(&self) -> bool {
        !self.pending_acknowledgements.is_empty()
            || (self.acknowledgement_mode == ShareAcknowledgementMode::Implicit
                && !self.last_poll_unacked.is_empty())
    }

    fn pending_ack_records(&self) -> impl Iterator<Item = &ShareRecord> {
        let implicit = (self.acknowledgement_mode == ShareAcknowledgementMode::Implicit)
            .then_some(self.last_poll_unacked.iter())
            .into_iter()
            .flatten();
        implicit.chain(self.pending_acknowledgements.values().map(|(rec, _)| rec))
    }

    fn pending_fetch_acknowledgements(&self) -> BTreeMap<(String, i32), Vec<AcknowledgementBatch>> {
        if !self.has_pending_acknowledgements() {
            return BTreeMap::new();
        }
        let mut typed = Vec::new();
        if self.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            for rec in &self.last_poll_unacked {
                typed.push((rec.clone(), ACK_ACCEPT));
            }
        }
        for (rec, ack) in self.pending_acknowledgements.values() {
            typed.push((rec.clone(), *ack));
        }
        acknowledgement_batches_typed(&typed)
            .into_iter()
            .map(|(topic, partition, batches)| ((topic, partition), batches))
            .collect()
    }

    fn complete_poll_piggyback(&mut self, recs: &[ShareRecord]) {
        if self.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            let pending = std::mem::take(&mut self.last_poll_unacked);
            if !pending.is_empty() {
                self.complete_piggyback(&pending);
            }
        }
        if !self.pending_acknowledgements.is_empty() {
            let pending: Vec<ShareRecord> = std::mem::take(&mut self.pending_acknowledgements)
                .into_values()
                .map(|(rec, _)| rec)
                .collect();
            self.complete_piggyback(&pending);
        }
        if self.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            self.last_poll_unacked = recs.to_vec();
        } else {
            for rec in recs {
                if !self.last_poll_unacked.iter().any(|held| {
                    held.topic == rec.topic
                        && held.partition == rec.partition
                        && held.offset == rec.offset
                }) {
                    self.last_poll_unacked.push(rec.clone());
                }
            }
        }
    }

    /// Java `ShareCompletedFetch` GAP acknowledgements: queue after this
    /// poll's piggyback completion so they ride the next fetch / commit.
    fn promote_collected_gap_acknowledgements(&mut self) {
        let gaps = std::mem::take(&mut self.collected_gap_acknowledgements);
        for rec in gaps {
            let key = (rec.topic.clone(), rec.partition, rec.offset);
            let _ = self
                .pending_acknowledgements
                .entry(key)
                .or_insert((rec, ACK_GAP));
        }
    }

    fn promote_collected_reject_acknowledgements(&mut self) {
        let rejected = std::mem::take(&mut self.collected_reject_acknowledgements);
        for rec in rejected {
            let key = (rec.topic.clone(), rec.partition, rec.offset);
            let _ = self
                .pending_acknowledgements
                .entry(key)
                .or_insert((rec, ACK_REJECT));
        }
    }

    fn complete_piggyback(&mut self, recs: &[ShareRecord]) {
        if recs.is_empty() {
            return;
        }
        self.fire_acknowledgement_commit_callback(recs, None);
        self.acquisitions
            .retain(|(topic, partition, offset), acquired| {
                !recs.iter().any(|rec| {
                    rec.topic == *topic
                        && rec.partition == *partition
                        && rec.offset == *offset
                        && rec.delivery_count == acquired.delivery_count
                })
            });
        self.finish_acknowledgement_metrics(
            recs.iter()
                .map(|rec| (rec.topic.clone(), rec.partition, rec.offset)),
        );
    }

    fn take_pending_acknowledgements(&mut self) -> (Vec<ShareRecord>, Vec<(ShareRecord, i8)>) {
        let implicit = if self.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            std::mem::take(&mut self.last_poll_unacked)
        } else {
            Vec::new()
        };
        let explicit = std::mem::take(&mut self.pending_acknowledgements)
            .into_values()
            .collect();
        (implicit, explicit)
    }

    fn restore_pending_acknowledgements(
        &mut self,
        implicit: Vec<ShareRecord>,
        explicit: Vec<(ShareRecord, i8)>,
    ) {
        if self.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            self.last_poll_unacked = implicit
                .into_iter()
                .filter(|rec| self.still_acquired(rec))
                .collect();
        }
        for (rec, ack) in explicit {
            if self.still_acquired(&rec) {
                let _ = self
                    .pending_acknowledgements
                    .insert((rec.topic.clone(), rec.partition, rec.offset), (rec, ack));
            }
        }
    }

    fn still_acquired(&self, rec: &ShareRecord) -> bool {
        self.acquisitions
            .get(&(rec.topic.clone(), rec.partition, rec.offset))
            .is_some_and(|acquired| acquired.delivery_count == rec.delivery_count)
    }

    /// Java `acknowledgeBatchIfImplicitAcknowledgement`: mark the last
    /// implicit poll as ACCEPT without sending.
    fn snapshot_implicit_acknowledgements(&mut self) {
        if self.acknowledgement_mode != ShareAcknowledgementMode::Implicit {
            return;
        }
        let pending = std::mem::take(&mut self.last_poll_unacked);
        for rec in pending {
            let key = (rec.topic.clone(), rec.partition, rec.offset);
            let _ = self
                .pending_acknowledgements
                .entry(key)
                .or_insert((rec, ACK_ACCEPT));
        }
    }

    /// Java `close` / `ShareAcknowledgeOnCloseEvent`: flush queued
    /// acknowledgements (including a prior [`Self::commit_async`]) but
    /// do not implicit-ACCEPT the last poll. Per-partition leader-change
    /// failures complete locally (`NOT_LEADER_OR_FOLLOWER`) and close
    /// still proceeds (Java
    /// `testLeadershipChangeAfterFetchBeforeCloseMove`).
    async fn flush_close_acknowledgements(&mut self) -> Result<()> {
        if self.async_commit {
            self.snapshot_implicit_acknowledgements();
            self.async_commit = false;
        }
        self.last_poll_unacked.clear();
        if !self.has_pending_acknowledgements() {
            return Ok(());
        }
        let (implicit, explicit) = self.take_pending_acknowledgements();
        let typed = pending_typed(&implicit, &explicit);
        match self.acknowledge_typed(&typed).await {
            Ok(_) => Ok(()),
            Err(error) => {
                self.restore_pending_acknowledgements(implicit, explicit);
                Err(error)
            }
        }
    }

    async fn flush_pending_acknowledgements(&mut self) -> Result<()> {
        if !self.has_pending_acknowledgements() {
            return Ok(());
        }
        let (implicit, explicit) = self.take_pending_acknowledgements();
        let typed = pending_typed(&implicit, &explicit);
        match self.send_typed_acknowledgements(&typed).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let keys: Vec<(String, i32, i64)> = typed
                    .iter()
                    .filter(|(rec, _)| {
                        matches!(error, Error::Timeout)
                            || ack_outcome_is_fatal(&error)
                            || !self.still_acquired(rec)
                    })
                    .map(|(rec, _)| (rec.topic.clone(), rec.partition, rec.offset))
                    .collect();
                self.uncount_acknowledgements(keys);
                // A timed-out ShareAcknowledge is ambiguous (the broker may
                // have applied it). Do not retry automatically.
                if !matches!(error, Error::Timeout) {
                    self.restore_pending_acknowledgements(implicit, explicit);
                }
                Err(error)
            }
        }
    }

    fn queue_acknowledgements(&mut self, recs: &[ShareRecord], ack: i8) -> Result<()> {
        self.ensure_acquired(recs)?;
        for rec in recs {
            let key = (rec.topic.clone(), rec.partition, rec.offset);
            let _ = self
                .pending_acknowledgements
                .insert(key.clone(), (rec.clone(), ack));
            if self.counted_acknowledgements.insert(key) {
                self.records_acknowledged = self.records_acknowledged.saturating_add(1);
            }
        }
        self.drop_last_poll_unacked(recs);
        Ok(())
    }

    fn finish_acknowledgement_metrics(
        &mut self,
        keys: impl IntoIterator<Item = (String, i32, i64)>,
    ) {
        let mut added = 0u64;
        for key in keys {
            if !self.counted_acknowledgements.remove(&key) {
                added = added.saturating_add(1);
            }
        }
        self.records_acknowledged = self.records_acknowledged.saturating_add(added);
    }

    fn uncount_acknowledgements(&mut self, keys: impl IntoIterator<Item = (String, i32, i64)>) {
        for key in keys {
            if self.counted_acknowledgements.remove(&key) {
                self.records_acknowledged = self.records_acknowledged.saturating_sub(1);
            }
        }
    }

    fn drop_last_poll_unacked(&mut self, recs: &[ShareRecord]) {
        self.last_poll_unacked.retain(|held| {
            !recs.iter().any(|rec| {
                rec.topic == held.topic
                    && rec.partition == held.partition
                    && rec.offset == held.offset
            })
        });
    }

    /// Java `ShareConsumeRequestManager` piggyback acknowledge error:
    /// fire the commit callback and drop the queued acknowledgements
    /// for this partition. The ShareFetch records are still delivered.
    fn fail_node_share_fetch_acknowledgements(&mut self, tps: &[(String, i32)], error: &Error) {
        for (topic, partition) in tps {
            self.fail_share_fetch_acknowledgements(topic, *partition, error);
        }
    }

    fn fail_share_fetch_acknowledgements(&mut self, topic: &str, partition: i32, error: &Error) {
        let failed: Vec<ShareRecord> = self
            .pending_ack_records()
            .filter(|rec| rec.topic == topic && rec.partition == partition)
            .cloned()
            .collect();
        if failed.is_empty() {
            return;
        }
        self.fire_acknowledgement_commit_callback(&failed, Some(error));
        let keys: Vec<(String, i32, i64)> = failed
            .iter()
            .map(|rec| (rec.topic.clone(), rec.partition, rec.offset))
            .collect();
        self.uncount_acknowledgements(keys);
        self.drop_last_poll_unacked(&failed);
        for rec in &failed {
            let _ = self.pending_acknowledgements.remove(&(
                rec.topic.clone(),
                rec.partition,
                rec.offset,
            ));
        }
    }

    /// Java `ShareConsumerImpl.ensureInFlightAcknowledgedIfExplicitAcknowledgement`.
    ///
    /// Buffered leftover from the last ShareFetch (`pending_completed`)
    /// can still be drained; the check applies when the next poll would
    /// contact a broker.
    fn ensure_explicit_in_flight_acknowledged(&self) -> Result<()> {
        if self.acknowledgement_mode == ShareAcknowledgementMode::Explicit
            && !self.last_poll_unacked.is_empty()
            && self.pending_completed.is_empty()
        {
            return Err(reject_java_explicit_in_flight());
        }
        Ok(())
    }

    fn ensure_acquired(&mut self, recs: &[ShareRecord]) -> Result<()> {
        if self.share_epochs.is_empty() {
            return Err(reject_java_acknowledge_before_poll());
        }
        self.prune_expired_acquisitions();
        for record in recs {
            let key = (record.topic.clone(), record.partition, record.offset);
            if let Some(acquired) = self.acquisitions.get(&key) {
                if acquired.delivery_count != record.delivery_count {
                    return Err(Error::broker(
                        error::INVALID_RECORD_STATE,
                        format!(
                            "stale share acknowledgement {}-{}@{}",
                            record.topic, record.partition, record.offset
                        ),
                    ));
                }
                continue;
            }
            // Already-queued leftover acks survive assignment shrink
            // (Java testCloseWithSubscriptionChange).
            if self
                .pending_acknowledgements
                .get(&key)
                .is_some_and(|(queued, _)| queued.delivery_count == record.delivery_count)
            {
                continue;
            }
            return Err(Error::broker(
                error::INVALID_RECORD_STATE,
                format!(
                    "stale share acknowledgement {}-{}@{}",
                    record.topic, record.partition, record.offset
                ),
            ));
        }
        Ok(())
    }

    async fn send_acknowledgements(&mut self, recs: &[ShareRecord], ack: i8) -> Result<()> {
        let typed: Vec<(ShareRecord, i8)> = recs.iter().cloned().map(|rec| (rec, ack)).collect();
        self.send_typed_acknowledgements(&typed).await
    }

    async fn send_typed_acknowledgements(&mut self, recs: &[(ShareRecord, i8)]) -> Result<()> {
        let outcomes = self.acknowledge_typed(recs).await?;
        if let Some(error) = outcomes.values().find_map(|error| error.as_ref()) {
            return Err(error.clone());
        }
        Ok(())
    }

    /// ShareAcknowledge with per-partition outcomes.
    ///
    /// Broker / leader / unknown-topic failures are [`Ok`] entries with
    /// `Some(error)` (Java `commitSync` map). Timeout, protocol, closed,
    /// and stale-before-send failures stay [`Err`].
    async fn acknowledge_typed(
        &mut self,
        recs: &[(ShareRecord, i8)],
    ) -> Result<BTreeMap<(String, i32), Option<Error>>> {
        if recs.is_empty() {
            return Ok(BTreeMap::new());
        }
        let records: Vec<ShareRecord> = recs.iter().map(|(rec, _)| rec.clone()).collect();
        self.ensure_acquired(&records)?;
        let ack = if recs.iter().all(|(_, ack)| *ack == ACK_RENEW) {
            ACK_RENEW
        } else {
            recs.first().map(|(_, ack)| *ack).unwrap_or(ACK_ACCEPT)
        };
        let mut partitions = acknowledgement_batches_typed(recs);
        let deadline = Instant::now() + self.cfg.request_timeout;
        let mut attempt = 0u32;
        let mut outcomes = BTreeMap::new();
        loop {
            let outcome = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                self.acknowledge_leaders(&mut partitions, ack, &mut outcomes),
            )
            .await;
            match outcome.unwrap_or(Err(Error::Timeout)) {
                Ok(()) => {
                    for rec in &records {
                        let _ = outcomes
                            .entry((rec.topic.clone(), rec.partition))
                            .or_insert(None);
                    }
                    self.fire_acknowledgement_commit_outcomes(&records, &outcomes, None);
                    return Ok(outcomes);
                }
                Err(error) if share_ack_retriable(&error) && !partitions.is_empty() => {
                    if Instant::now() >= deadline {
                        self.fire_acknowledgement_commit_outcomes(
                            &records,
                            &outcomes,
                            Some(&Error::Timeout),
                        );
                        return Err(Error::Timeout);
                    }
                    self.consumer.sleep_retry_backoff(attempt, deadline).await?;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) if ack_outcome_is_fatal(&error) => {
                    self.fire_acknowledgement_commit_outcomes(&records, &outcomes, Some(&error));
                    return Err(error);
                }
                Err(error) => {
                    for rec in &records {
                        let _ = outcomes
                            .entry((rec.topic.clone(), rec.partition))
                            .or_insert_with(|| Some(error.clone()));
                    }
                    self.fire_acknowledgement_commit_outcomes(&records, &outcomes, Some(&error));
                    return Ok(outcomes);
                }
            }
        }
    }

    async fn acknowledge_leaders(
        &mut self,
        partitions: &mut Vec<(String, i32, Vec<AcknowledgementBatch>)>,
        ack: i8,
        outcomes: &mut BTreeMap<(String, i32), Option<Error>>,
    ) -> Result<()> {
        let mut first_error = None;
        // Java isLeaderKnownToHaveChanged: fail acknowledgements when
        // Metadata already shows a different leader. Acquisition locks
        // are released on the previous broker; do not send them to the
        // new leader (testLeadershipChangeAfterFetchBeforeCommitSync).
        let mut kept = Vec::with_capacity(partitions.len());
        for (topic, part, batches) in partitions.drain(..) {
            if self.share_ack_leader_changed(&topic, part) {
                let error = Error::broker(
                    error::NOT_LEADER_OR_FOLLOWER,
                    format!("ShareAcknowledge {topic}-{part}"),
                );
                self.drop_acknowledged_acquisitions(&topic, part, &batches);
                let _ = outcomes.insert((topic, part), Some(error.clone()));
                if first_error.as_ref().is_none_or(share_ack_retriable) {
                    first_error = Some(error);
                }
            } else {
                kept.push((topic, part, batches));
            }
        }
        *partitions = kept;
        if partitions.is_empty() {
            return first_error.map_or(Ok(()), Err);
        }
        let tps: Vec<_> = partitions
            .iter()
            .map(|(topic, part, _)| (topic.clone(), *part))
            .collect();
        let mut nodes: Vec<_> = self.leaders_of(&tps).await?.into_iter().collect();
        nodes.sort_by_key(|(node, _)| *node);
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
            let (code, responses, endpoints, _, error_message, lock_timeout) =
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
            self.remember_share_endpoints(&endpoints);
            if code != 0 {
                let error = share_partition_broker_error(
                    code,
                    error_message.as_deref(),
                    "ShareAcknowledge",
                );
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
            let responded = responses.iter().flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(move |part| (topic.topic_id, part.partition))
            });
            // Java lookupTopicId skips an unknown topic id. Omitted
            // requested partitions complete with INVALID_RESPONSE
            // (processingComplete), not a protocol error.
            let omitted = share_acknowledge_omitted(&expected, responded);
            self.advance_node_epoch(node);
            if version >= 2 && lock_timeout > 0 {
                self.acquisition_lock_timeout_ms = Some(lock_timeout);
            }
            let mut processed = HashSet::new();
            for topic in responses {
                let name = self.name_for_topic_id(topic.topic_id);
                for part in topic.partitions {
                    let key = (topic.topic_id, part.partition);
                    if !expected.contains(&key) || !processed.insert(key) {
                        continue;
                    }
                    if part.error_code == 0 {
                        let _ = outcomes.insert((name.clone(), part.partition), None);
                        let matched: Vec<(String, i32, i64)> = self
                            .acquisitions
                            .keys()
                            .filter(|(t, p, offset)| {
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
                            .cloned()
                            .collect();
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
                            self.finish_acknowledgement_metrics(matched);
                        }
                        partitions.retain(|(t, p, _)| t != &name || *p != part.partition);
                    } else {
                        // Java updateLeaderInfoMap: apply CurrentLeader
                        // for NOT_LEADER / FENCED / UNKNOWN_TOPIC_OR_PARTITION
                        // / UNKNOWN_TOPIC_ID when leaderId and leaderEpoch
                        // are both set.
                        let applied_leader = share_ack_current_leader_usable(part.error_code)
                            && self.apply_share_current_leader(
                                &name,
                                part.partition,
                                part.current_leader_id,
                                part.current_leader_epoch,
                            );
                        if part.error_code == error::INVALID_RECORD_STATE
                            || share_ack_ownership_lost(part.error_code)
                        {
                            if let Some((_, _, batches)) = partitions
                                .iter()
                                .find(|(topic, p, _)| topic == &name && *p == part.partition)
                            {
                                self.drop_acknowledged_acquisitions(&name, part.partition, batches);
                            }
                            // Java testFatalErrorsAcknowledgementResponse:
                            // NOT_LEADER / FENCED / UNKNOWN_TOPIC_OR_PARTITION
                            // complete without retry. Metadata is requested
                            // when there is no usable CurrentLeader (KIP-951
                            // still applies the hint without a refresh).
                            if share_ack_ownership_lost(part.error_code) && !applied_leader {
                                self.invalidate_share_topic(&name);
                            }
                        }
                        let error = share_partition_broker_error(
                            part.error_code,
                            part.error_message.as_deref(),
                            format!("ShareAcknowledge {name}-{}", part.partition),
                        );
                        let _ =
                            outcomes.insert((name.clone(), part.partition), Some(error.clone()));
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
            for (topic_id, partition) in omitted {
                let name = self.name_for_topic_id(topic_id);
                if let Some((_, _, batches)) = partitions
                    .iter()
                    .find(|(topic, part, _)| topic == &name && *part == partition)
                {
                    self.drop_acknowledged_acquisitions(&name, partition, batches);
                }
                let error = Error::broker(error::INVALID_RECORD_STATE, SHARE_ACK_INVALID_RESPONSE);
                let _ = outcomes.insert((name, partition), Some(error.clone()));
                if first_error.as_ref().is_none_or(share_ack_retriable) {
                    first_error = Some(error);
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
        self.session_partitions.clear();
        self.share_conns.clear();
        self.share_leader_overrides.clear();
        self.acquisitions.clear();
        self.pending_completed.clear();
        self.pending_acknowledgements.clear();
        self.collected_gap_acknowledgements.clear();
        self.collected_reject_acknowledgements.clear();
        self.counted_acknowledgements.clear();
        self.async_commit = false;
        last
    }

    /// Leave the share group ([`ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH`]).
    ///
    /// Flushes queued acknowledgements (including a prior
    /// [`Self::commit_async`]; Java
    /// `testAcknowledgeOnCloseWithPendingCommitAsync`). A prior
    /// [`Self::commit`] / [`Self::commit_sync`] has already sent those
    /// acknowledgements, so close is an empty FINAL-epoch ShareAcknowledge
    /// (Java `testAcknowledgeOnCloseWithPendingCommitSync`). Implicit
    /// last-poll records that were never committed are released without
    /// ACCEPT (Java `close`).
    /// Leftover acknowledgements for a previous topic still flush after
    /// [`Self::subscribe`] and no further ShareFetch is sent
    /// (Java `testCloseWithSubscriptionChange`). When the last poll
    /// acquired partitions on more than one leader, close sends one
    /// ShareAcknowledge to each of those nodes (Java
    /// `testShareFetchAndCloseMultipleNodes`). After a metadata leader
    /// change, leftover acknowledgements whose acquisition node is no
    /// longer the Metadata leader complete `NOT_LEADER_OR_FOLLOWER` and
    /// close still sends the still-led partitions (Java
    /// `testLeadershipChangeAfterFetchBeforeCloseMove`). When every
    /// leftover partition has moved, all of those acknowledgements
    /// complete `NOT_LEADER_OR_FOLLOWER` and close still proceeds
    /// (Java `testLeadershipChangeAfterFetchMoveBeforeCloseMove`). The
    /// acknowledgement commit callback is invoked once per partition
    /// with that partition's exception (`None` on the still-led ACCEPT).
    pub async fn leave(mut self) -> Result<()> {
        if self.member_id.is_empty() {
            self.hb_stop.send(true).unwrap_or(());
            *self.hb_deadline.lock() = None;
            self.consumer.close_interceptors();
            return Ok(());
        }
        self.hb_stop.send(true).unwrap_or(());
        *self.hb_deadline.lock() = None;
        let acks = self.flush_close_acknowledgements().await;
        let close = self.close_share_session().await;
        let out = if self.left_max_poll.load(Ordering::SeqCst) {
            Ok(())
        } else {
            self.leave_coordinator().await
        };
        self.consumer.close_interceptors();
        acks.and(close).and(out)
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
        let last_poll = self.last_poll.clone();
        let left_max_poll = self.left_max_poll.clone();
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
                let mut wake_deadline = next_hb_deadline;
                if !cfg.max_poll_interval.is_zero() {
                    if let Some(t) = *last_poll.lock() {
                        let poll_deadline = t + cfg.max_poll_interval;
                        if poll_deadline < wake_deadline {
                            wake_deadline = poll_deadline;
                        }
                    }
                }
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
                        if leave_if_share_max_poll(
                            &cfg,
                            &group_id,
                            &member_id,
                            &last_poll,
                            &left_max_poll,
                        )
                        .await
                        {
                            *hb_deadline.lock() = None;
                            break;
                        }
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

async fn leave_if_share_max_poll(
    cfg: &ConsumerConfig,
    group_id: &str,
    member_id: &str,
    last_poll: &Mutex<Option<Instant>>,
    left: &AtomicBool,
) -> bool {
    if left.load(Ordering::SeqCst) {
        return true;
    }
    if cfg.max_poll_interval.is_zero() {
        return false;
    }
    let expired = last_poll
        .lock()
        .is_some_and(|t| t.elapsed() > cfg.max_poll_interval);
    if !expired {
        return false;
    }
    left.store(true, Ordering::SeqCst);
    if let Ok(mut conn) = discover_coord(cfg, group_id, COORDINATOR_GROUP).await {
        let version = conn.share_group_heartbeat_version;
        if spoken_share_group_heartbeat(version).is_ok() {
            let req = ShareGroupHeartbeatRequest {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
                member_epoch: ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH,
                rack_id: cfg.rack.clone(),
                subscribed_topic_names: None,
            };
            drop(
                conn.roundtrip(
                    SHARE_GROUP_HEARTBEAT,
                    version,
                    |buf| encode_share_group_heartbeat_request(buf, version, &req),
                    cfg.request_timeout,
                )
                .await,
            );
        }
    }
    true
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

/// Java `handleShareFetchSuccess`: only top-level `UNKNOWN_TOPIC_ID`
/// calls `metadata.requestUpdate(false)`. Partition-level
/// `INCONSISTENT_TOPIC_ID` still refreshes via
/// [`share_fetch_partition_metadata_refresh`].
fn share_topic_id_refresh(code: i16) -> bool {
    matches!(code, error::UNKNOWN_TOPIC_ID)
}

fn share_ack_ownership_lost(code: i16) -> bool {
    matches!(
        code,
        error::NOT_LEADER_OR_FOLLOWER
            | error::FENCED_LEADER_EPOCH
            | error::UNKNOWN_TOPIC_OR_PARTITION
            | error::UNKNOWN_TOPIC_ID
            | error::INCONSISTENT_TOPIC_ID
    )
}

/// Java `isLeaderKnownToHaveChanged`: a present Metadata leader that
/// differs from the fetch node. A missing leader is not a known change
/// (Java requests `metadata.requestUpdate` and returns false).
fn share_ack_fetch_leader_changed(fetch_node: Option<i32>, current_leader: Option<i32>) -> bool {
    match (fetch_node, current_leader) {
        (Some(fetch), Some(leader)) => leader != fetch,
        _ => false,
    }
}

/// Java `ShareFetchCollector.handleInitializeErrors` codes that request
/// a metadata update (or are logged) and do not throw from `collect`.
fn share_fetch_partition_skipped(code: i16) -> bool {
    share_fetch_partition_metadata_refresh(code)
        || matches!(
            code,
            error::UNKNOWN_LEADER_EPOCH | error::UNKNOWN_SERVER_ERROR
        )
}

/// Java `handleInitializeErrors` codes that call `metadata.requestUpdate`.
/// `UNKNOWN_LEADER_EPOCH` / `UNKNOWN_SERVER_ERROR` skip with no update.
fn share_fetch_partition_metadata_refresh(code: i16) -> bool {
    matches!(
        code,
        error::NOT_LEADER_OR_FOLLOWER
            | error::REPLICA_NOT_AVAILABLE
            | error::KAFKA_STORAGE_ERROR
            | error::FENCED_LEADER_EPOCH
            | error::OFFSET_NOT_AVAILABLE
            | error::UNKNOWN_TOPIC_OR_PARTITION
            | error::UNKNOWN_TOPIC_ID
            | error::INCONSISTENT_TOPIC_ID
    )
}

/// Java `Errors.forCode(code).exception(errorMessage)`: prefer the
/// broker's partition ErrorMessage when it is non-empty.
fn share_partition_broker_error(
    code: i16,
    broker_message: Option<&str>,
    fallback: impl Into<String>,
) -> Error {
    let detail = broker_message
        .filter(|msg| !msg.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| fallback.into());
    Error::broker(code, detail)
}

/// Java `ShareFetchCollector.collect` / `testUnauthorizedTopic`:
/// `TOPIC_AUTHORIZATION_FAILED` is `TopicAuthorizationException` whose
/// `unauthorizedTopics()` is the partition topic, not ErrorMessage.
fn share_fetch_collect_error(
    code: i16,
    broker_message: Option<&str>,
    topic: &str,
    partition: i32,
) -> Error {
    if code == error::TOPIC_AUTHORIZATION_FAILED {
        return Error::broker(code, topic);
    }
    share_partition_broker_error(
        code,
        broker_message,
        format!("ShareFetch {topic}-{partition}"),
    )
}

/// Java `handleShareFetchSuccess` applies CurrentLeader only for these.
fn share_fetch_current_leader_usable(code: i16) -> bool {
    matches!(
        code,
        error::NOT_LEADER_OR_FOLLOWER | error::FENCED_LEADER_EPOCH
    )
}

/// Java `handlePartitionError` / `updateLeaderInfoMap` applies
/// CurrentLeader for these ShareAcknowledge partition codes.
fn share_ack_current_leader_usable(code: i16) -> bool {
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

/// Timeout / protocol / closed stay [`Err`] on [`ShareGroup::commit_sync`].
/// Broker, missing-leader, and unknown-topic land in the Java result map.
fn ack_outcome_is_fatal(error: &Error) -> bool {
    !matches!(
        error,
        Error::Broker { .. } | Error::NoLeader { .. } | Error::UnknownTopic(_)
    )
}

/// Java `ShareSessionHandler.handleResponse(ShareFetchResponse)`:
/// `SHARE_SESSION_NOT_FOUND` / `INVALID_SHARE_SESSION_EPOCH` /
/// `SHARE_SESSION_LIMIT_REACHED` call `nextCloseExistingAttemptNew`.
/// ShareAcknowledge `handleResponse` omits `SHARE_SESSION_LIMIT_REACHED`
/// (that path uses [`share_session_reset`] only).
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

/// Java `AcknowledgeRequestState.processingComplete`: requested
/// acknowledgements missing from a successful ShareAcknowledge complete
/// with `InvalidRecordStateException(INVALID_RESPONSE)`. Extra response
/// partitions are ignored (`lookupTopicId`).
fn share_acknowledge_omitted(
    expected: &HashSet<([u8; 16], i32)>,
    responded: impl IntoIterator<Item = ([u8; 16], i32)>,
) -> Vec<([u8; 16], i32)> {
    let mut seen = HashSet::new();
    for key in responded {
        if expected.contains(&key) {
            let _ = seen.insert(key);
        }
    }
    let mut omitted: Vec<_> = expected
        .iter()
        .copied()
        .filter(|tp| !seen.contains(tp))
        .collect();
    omitted.sort_unstable();
    omitted
}

fn retain_completed_records(
    pending: &mut VecDeque<ShareCompletedFetch>,
    keep: impl Fn(&ShareRecord) -> bool,
) {
    let mut kept = VecDeque::new();
    for item in std::mem::take(pending) {
        match item {
            ShareCompletedFetch::Records(recs) => {
                let recs: VecDeque<_> = recs.into_iter().filter(&keep).collect();
                if !recs.is_empty() {
                    kept.push_back(ShareCompletedFetch::Records(recs));
                }
            }
            other => kept.push_back(other),
        }
    }
    *pending = kept;
}

/// Topic-id + partition keys currently in a node's ShareFetch body.
fn share_session_requested(by_id: &BTreeMap<[u8; 16], Vec<i32>>) -> HashSet<([u8; 16], i32)> {
    by_id
        .iter()
        .flat_map(|(topic_id, partitions)| {
            partitions.iter().map(|partition| (*topic_id, *partition))
        })
        .collect()
}

/// Java `ShareSessionHandler.newShareFetchBuilder` removed + replaced
/// partitions (`ForgottenTopicsData`).
///
/// Incremental epochs forget session keys that are not in this request,
/// including a topic-id replacement for the same partition. The initial
/// epoch is a new session, so the forget list is empty.
fn share_session_forgotten(
    epoch: i32,
    session: Option<&HashSet<([u8; 16], i32)>>,
    requested: &HashSet<([u8; 16], i32)>,
) -> Vec<ShareForgottenTopic> {
    if epoch <= ShareRequestMetadata::INITIAL_EPOCH {
        return Vec::new();
    }
    let Some(session) = session else {
        return Vec::new();
    };
    let mut removed: Vec<_> = session
        .iter()
        .copied()
        .filter(|key| !requested.contains(key))
        .collect();
    removed.sort_unstable();
    ShareFetchRequest::update_forgotten_data(&[], removed)
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

/// Java `ShareConsumerImpl.ensureExplicitAcknowledgement` when the mode is
/// implicit.
fn reject_java_acknowledge_in_implicit_mode() -> Error {
    Error::protocol("Implicit acknowledgement of delivery is being used.")
}

/// Java `ShareConsumerImpl.ensureInFlightAcknowledgedIfExplicitAcknowledgement`.
fn reject_java_explicit_in_flight() -> Error {
    Error::protocol("All records must be acknowledged in explicit acknowledgement mode.")
}

/// Java `ShareCompletedFetch` synthetic record for an acquired offset
/// that had no deliverable record (gap / control hole).
fn share_gap_record(
    topic: String,
    partition: i32,
    offset: i64,
    delivery_count: i16,
) -> ShareRecord {
    ShareRecord {
        topic,
        partition,
        offset,
        timestamp: ShareRecord::NO_TIMESTAMP,
        timestamp_type: TimestampType::CreateTime,
        key: None,
        value: None,
        headers: Vec::new(),
        delivery_count,
        leader_epoch: None,
    }
}

/// Acquired offsets that were not delivered as records.
///
/// Java `ShareCompletedFetch.fetchRecords` acknowledges those offsets as
/// GAP (`AcknowledgeType` null / wire [`ACK_GAP`]).
fn share_fetch_missing_acquired(
    acquired: &[AcquiredRange],
    present: &HashSet<i64>,
) -> Result<Vec<(i64, i16)>> {
    const MAX_GAPS: usize = 1_048_576;
    let mut missing = Vec::new();
    for range in acquired {
        let width = range
            .last_offset
            .checked_sub(range.first_offset)
            .and_then(|delta| usize::try_from(delta).ok())
            .and_then(|delta| delta.checked_add(1))
            .ok_or_else(|| Error::protocol("invalid ShareFetch acquired range"))?;
        if width > MAX_GAPS || missing.len().saturating_add(width) > MAX_GAPS {
            return Err(Error::protocol("ShareFetch acquired range too large"));
        }
        let mut offset = range.first_offset;
        loop {
            if !present.contains(&offset) {
                missing.push((offset, range.delivery_count));
            }
            if offset == range.last_offset {
                break;
            }
            offset += 1;
        }
    }
    Ok(missing)
}

/// Acquired offsets that fall inside a CRC-failed batch (Java REJECT).
fn share_fetch_acquired_in_range(
    acquired: &[AcquiredRange],
    first: i64,
    last: i64,
) -> Result<Vec<(i64, i16)>> {
    const MAX_REJECTS: usize = 1_048_576;
    if last < first {
        return Err(Error::protocol("invalid ShareFetch corrupt-batch range"));
    }
    let mut rejected = Vec::new();
    for range in acquired {
        let start = range.first_offset.max(first);
        let end = range.last_offset.min(last);
        if end < start {
            continue;
        }
        let width = end
            .checked_sub(start)
            .and_then(|delta| usize::try_from(delta).ok())
            .and_then(|delta| delta.checked_add(1))
            .ok_or_else(|| Error::protocol("invalid ShareFetch acquired range"))?;
        if width > MAX_REJECTS || rejected.len().saturating_add(width) > MAX_REJECTS {
            return Err(Error::protocol("ShareFetch acquired range too large"));
        }
        let mut offset = start;
        loop {
            rejected.push((offset, range.delivery_count));
            if offset == end {
                break;
            }
            offset += 1;
        }
    }
    Ok(rejected)
}

enum ShareFetchPartEvent {
    Corrupt { first: i64, last: i64 },
    Batch(RecordBatch),
}

fn pending_typed(
    implicit: &[ShareRecord],
    explicit: &[(ShareRecord, i8)],
) -> Vec<(ShareRecord, i8)> {
    let mut typed = Vec::with_capacity(implicit.len() + explicit.len());
    for rec in implicit {
        typed.push((rec.clone(), ACK_ACCEPT));
    }
    typed.extend(explicit.iter().cloned());
    typed
}

/// Collapse records into KIP-932 acknowledgement batches.
///
/// Contiguous offsets with the same type become one batch with a single
/// `AcknowledgeType` (applies to the whole range). Gaps or type changes
/// start a new batch.
fn acknowledgement_batches_typed(
    recs: &[(ShareRecord, i8)],
) -> Vec<(String, i32, Vec<AcknowledgementBatch>)> {
    let mut by_part: BTreeMap<(String, i32), BTreeMap<i64, i8>> = BTreeMap::new();
    for (rec, ack) in recs {
        let _ = by_part
            .entry((rec.topic.clone(), rec.partition))
            .or_default()
            .insert(rec.offset, *ack);
    }
    let mut out = Vec::with_capacity(by_part.len());
    for ((topic, partition), offs) in by_part {
        let mut batches = Vec::new();
        let mut range: Option<(i64, i64, i8)> = None;
        for (off, ack) in offs {
            range = match range {
                None => Some((off, off, ack)),
                Some((start, prev, typ)) if off == prev.saturating_add(1) && ack == typ => {
                    Some((start, off, typ))
                }
                Some((start, prev, typ)) => {
                    batches.push(AcknowledgementBatch {
                        first_offset: start,
                        last_offset: prev,
                        types: vec![typ],
                    });
                    Some((off, off, ack))
                }
            };
        }
        if let Some((start, prev, typ)) = range {
            batches.push(AcknowledgementBatch {
                first_offset: start,
                last_offset: prev,
                types: vec![typ],
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
        let typed: Vec<_> = recs.iter().cloned().map(|rec| (rec, ACK_ACCEPT)).collect();
        let batches = acknowledgement_batches_typed(&typed);
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
        assert!(share_fetch_session_reset(&Error::broker(
            error::SHARE_SESSION_LIMIT_REACHED,
            "x"
        )));
        assert!(
            !share_session_reset(&Error::broker(
                error::SHARE_SESSION_LIMIT_REACHED,
                "x"
            )),
            "Java ShareAcknowledge handleResponse only resets on SHARE_SESSION_NOT_FOUND / INVALID_SHARE_SESSION_EPOCH"
        );
        assert!(!share_ack_retriable(&Error::broker(
            error::SHARE_SESSION_LIMIT_REACHED,
            "x"
        )));
    }

    #[test]
    fn share_fetch_partition_skipped_matches_java_collect() {
        assert!(share_fetch_partition_skipped(error::NOT_LEADER_OR_FOLLOWER));
        assert!(share_fetch_partition_skipped(error::UNKNOWN_TOPIC_ID));
        assert!(share_fetch_partition_skipped(error::INCONSISTENT_TOPIC_ID));
        assert!(share_fetch_partition_skipped(error::UNKNOWN_SERVER_ERROR));
        assert!(share_fetch_partition_skipped(error::REPLICA_NOT_AVAILABLE));
        assert!(share_fetch_partition_skipped(error::KAFKA_STORAGE_ERROR));
        assert!(share_fetch_partition_skipped(error::OFFSET_NOT_AVAILABLE));
        assert!(!share_fetch_partition_skipped(
            error::TOPIC_AUTHORIZATION_FAILED
        ));
        assert!(!share_fetch_partition_skipped(error::CORRUPT_MESSAGE));
        assert!(!share_fetch_partition_skipped(error::INVALID_RECORD_STATE));
    }

    #[test]
    fn share_fetch_partition_metadata_refresh_matches_java() {
        assert!(share_fetch_partition_metadata_refresh(
            error::NOT_LEADER_OR_FOLLOWER
        ));
        assert!(share_fetch_partition_metadata_refresh(
            error::FENCED_LEADER_EPOCH
        ));
        assert!(share_fetch_partition_metadata_refresh(
            error::UNKNOWN_TOPIC_ID
        ));
        assert!(share_fetch_partition_metadata_refresh(
            error::INCONSISTENT_TOPIC_ID
        ));
        assert!(share_fetch_partition_metadata_refresh(
            error::REPLICA_NOT_AVAILABLE
        ));
        assert!(share_fetch_partition_metadata_refresh(
            error::KAFKA_STORAGE_ERROR
        ));
        assert!(share_fetch_partition_metadata_refresh(
            error::OFFSET_NOT_AVAILABLE
        ));
        assert!(!share_fetch_partition_metadata_refresh(
            error::UNKNOWN_LEADER_EPOCH
        ));
        assert!(!share_fetch_partition_metadata_refresh(
            error::UNKNOWN_SERVER_ERROR
        ));
        assert!(!share_fetch_partition_metadata_refresh(
            error::TOPIC_AUTHORIZATION_FAILED
        ));
        assert!(share_topic_id_refresh(error::UNKNOWN_TOPIC_ID));
        assert!(
            !share_topic_id_refresh(error::INCONSISTENT_TOPIC_ID),
            "Java handleShareFetchSuccess only requestUpdates for top-level UNKNOWN_TOPIC_ID"
        );
        assert!(share_fetch_current_leader_usable(
            error::NOT_LEADER_OR_FOLLOWER
        ));
        assert!(share_fetch_current_leader_usable(
            error::FENCED_LEADER_EPOCH
        ));
        assert!(!share_fetch_current_leader_usable(
            error::UNKNOWN_LEADER_EPOCH
        ));
        assert!(!share_fetch_current_leader_usable(
            error::REPLICA_NOT_AVAILABLE
        ));
        assert!(share_ack_current_leader_usable(
            error::NOT_LEADER_OR_FOLLOWER
        ));
        assert!(share_ack_current_leader_usable(error::FENCED_LEADER_EPOCH));
        assert!(share_ack_current_leader_usable(
            error::UNKNOWN_TOPIC_OR_PARTITION
        ));
        assert!(share_ack_current_leader_usable(error::UNKNOWN_TOPIC_ID));
        assert!(!share_ack_current_leader_usable(
            error::UNKNOWN_LEADER_EPOCH
        ));
        assert!(!share_ack_current_leader_usable(
            error::UNKNOWN_SERVER_ERROR
        ));
    }

    #[test]
    fn share_partition_broker_error_prefers_nonempty_message() {
        match share_fetch_collect_error(
            error::TOPIC_AUTHORIZATION_FAILED,
            Some("not authorized for t"),
            "orders",
            0,
        ) {
            Error::Broker { code, message } => {
                assert_eq!(code, error::TOPIC_AUTHORIZATION_FAILED);
                assert_eq!(message, "orders");
                assert_eq!(
                    Error::broker(code, message).unauthorized_topics(),
                    ["orders"]
                );
            }
            other => panic!("{other:?}"),
        }
        match share_partition_broker_error(
            error::CORRUPT_MESSAGE,
            Some("not authorized for t"),
            "ShareFetch t-0",
        ) {
            Error::Broker { code, message } => {
                assert_eq!(code, error::CORRUPT_MESSAGE);
                assert_eq!(message, "not authorized for t");
            }
            other => panic!("{other:?}"),
        }
        match share_partition_broker_error(error::CORRUPT_MESSAGE, Some(""), "ShareFetch t-0") {
            Error::Broker { code, message } => {
                assert_eq!(code, error::CORRUPT_MESSAGE);
                assert_eq!(message, "ShareFetch t-0");
            }
            other => panic!("{other:?}"),
        }
        match share_partition_broker_error(error::CORRUPT_MESSAGE, None, "ShareFetch t-0") {
            Error::Broker { code, message } => {
                assert_eq!(code, error::CORRUPT_MESSAGE);
                assert_eq!(message, "ShareFetch t-0");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn share_ack_fetch_leader_changed_matches_java() {
        assert!(share_ack_fetch_leader_changed(Some(1), Some(2)));
        assert!(!share_ack_fetch_leader_changed(Some(1), Some(1)));
        assert!(!share_ack_fetch_leader_changed(None, Some(2)));
        assert!(!share_ack_fetch_leader_changed(Some(1), None));
        assert!(!share_ack_fetch_leader_changed(None, None));
    }

    #[test]
    fn share_acknowledge_omitted_matches_java_invalid_response() {
        let topic = [0x11; 16];
        let extra = [0xCD; 16];
        let other = [0x22; 16];
        let expected = HashSet::from([(topic, 0), (other, 0)]);
        assert_eq!(
            share_acknowledge_omitted(&expected, [(topic, 0)]),
            vec![(other, 0)]
        );
        assert!(share_acknowledge_omitted(&expected, [(topic, 0), (other, 0)]).is_empty());
        assert_eq!(
            share_acknowledge_omitted(&expected, [(topic, 0), (extra, 0)]),
            vec![(other, 0)],
            "Java lookupTopicId skips an unrequested topic id"
        );
        assert!(
            share_acknowledge_omitted(&expected, [(topic, 0), (topic, 0), (other, 0)]).is_empty(),
            "duplicate requested partitions still count as present"
        );
        assert!(matches!(
            Error::broker(error::INVALID_RECORD_STATE, SHARE_ACK_INVALID_RESPONSE),
            Error::Broker {
                code: error::INVALID_RECORD_STATE,
                ref message,
            } if message == SHARE_ACK_INVALID_RESPONSE
        ));
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
    fn share_acknowledgement_mode_matches_java_names() {
        assert_eq!(ShareAcknowledgementMode::Implicit.as_str(), "implicit");
        assert_eq!(ShareAcknowledgementMode::Explicit.as_str(), "explicit");
        assert_eq!(ShareAcknowledgementMode::Implicit.to_string(), "implicit");
        assert_eq!(
            ShareAcknowledgementMode::default(),
            ShareAcknowledgementMode::Explicit
        );
    }

    #[test]
    fn acknowledgement_commit_error_carries_broker_code() {
        let error = AcknowledgementCommitError::from_error(&Error::broker(
            error::INVALID_RECORD_STATE,
            "x",
        ));
        assert_eq!(error.code(), Some(error::INVALID_RECORD_STATE));
        assert_eq!(error.message(), "x");
        assert_eq!(error.to_string(), error.message());
        assert!(AcknowledgementCommitError::from_error(&Error::Timeout)
            .code()
            .is_none());
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
    fn share_session_forgotten_matches_java_partition_removal() {
        let foo = [1u8; 16];
        let bar = [2u8; 16];
        let mut session = HashSet::new();
        let _ = session.insert((foo, 0));
        let _ = session.insert((foo, 1));
        let _ = session.insert((bar, 0));

        assert!(share_session_forgotten(
            ShareRequestMetadata::INITIAL_EPOCH,
            Some(&session),
            &HashSet::new(),
        )
        .is_empty());

        let mut keep_foo1 = HashSet::new();
        let _ = keep_foo1.insert((foo, 1));
        assert_eq!(
            share_session_forgotten(1, Some(&session), &keep_foo1),
            vec![
                ShareForgottenTopic {
                    topic_id: foo,
                    partitions: vec![0],
                },
                ShareForgottenTopic {
                    topic_id: bar,
                    partitions: vec![0],
                },
            ]
        );

        let mut replaced = HashSet::new();
        let _ = replaced.insert(([3u8; 16], 0));
        let _ = replaced.insert((foo, 1));
        let _ = replaced.insert((bar, 0));
        assert_eq!(
            share_session_forgotten(1, Some(&session), &replaced),
            vec![ShareForgottenTopic {
                topic_id: foo,
                partitions: vec![0],
            }]
        );

        assert!(share_session_forgotten(1, Some(&session), &session).is_empty());
        assert!(share_session_forgotten(1, None, &keep_foo1).is_empty());
        assert_eq!(
            share_session_forgotten(1, Some(&session), &HashSet::new()),
            vec![
                ShareForgottenTopic {
                    topic_id: foo,
                    partitions: vec![0, 1],
                },
                ShareForgottenTopic {
                    topic_id: bar,
                    partitions: vec![0],
                },
            ]
        );
    }

    #[test]
    fn share_session_forgotten_encodes_on_share_fetch() {
        let foo = [1u8; 16];
        let mut session = HashSet::new();
        let _ = session.insert((foo, 0));
        let _ = session.insert((foo, 1));
        let mut requested = HashSet::new();
        let _ = requested.insert((foo, 1));
        let forgotten = share_session_forgotten(1, Some(&session), &requested);
        let topics = [ShareFetchTopic {
            topic_id: foo,
            partitions: vec![ShareFetchPartition {
                partition: 1,
                partition_max_bytes: 1024,
                acknowledgements: vec![],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_share_fetch_request_with_options(
            &mut buf,
            0,
            "sg",
            "m1",
            1,
            10,
            1,
            1024,
            16,
            &topics,
            &forgotten,
            16,
            ShareFetchRequestOptions::default(),
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (_, _, epoch, _, got_topics, got_forgotten, ..) =
            crate::protocol::share::decode_share_fetch_request(&mut cur, 0).unwrap();
        assert!(cur.is_empty());
        assert_eq!(epoch, 1);
        assert_eq!(got_topics.len(), 1);
        assert_eq!(got_topics[0].partitions[0].partition, 1);
        assert_eq!(got_forgotten, forgotten);
    }

    #[test]
    fn acknowledgement_batches_typed_splits_on_type_change() {
        let recs = [
            (rec(0, 1), ACK_ACCEPT),
            (rec(0, 2), ACK_RELEASE),
            (rec(0, 3), ACK_RELEASE),
        ];
        let batches = acknowledgement_batches_typed(&recs);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].2.len(), 2);
        assert_eq!(batches[0].2[0].first_offset, 1);
        assert_eq!(batches[0].2[0].last_offset, 1);
        assert_eq!(batches[0].2[0].types, vec![ACK_ACCEPT]);
        assert_eq!(batches[0].2[1].first_offset, 2);
        assert_eq!(batches[0].2[1].last_offset, 3);
        assert_eq!(batches[0].2[1].types, vec![ACK_RELEASE]);
    }

    #[test]
    fn acknowledgement_batches_splits_on_gap() {
        let recs = [rec(0, 1), rec(0, 4)];
        let typed: Vec<_> = recs.iter().cloned().map(|rec| (rec, ACK_REJECT)).collect();
        let batches = acknowledgement_batches_typed(&typed);
        assert_eq!(batches[0].2.len(), 2);
        assert_eq!(batches[0].2[0].first_offset, 1);
        assert_eq!(batches[0].2[0].last_offset, 1);
        assert_eq!(batches[0].2[1].first_offset, 4);
        assert_eq!(batches[0].2[1].types, vec![ACK_REJECT]);
    }

    #[test]
    fn acknowledgement_batches_collapses_contiguous_gaps() {
        let recs = [rec(0, 10), rec(0, 11), rec(0, 12)];
        let typed: Vec<_> = recs.iter().cloned().map(|rec| (rec, ACK_GAP)).collect();
        let batches = acknowledgement_batches_typed(&typed);
        assert_eq!(batches[0].2.len(), 1);
        assert_eq!(batches[0].2[0].first_offset, 10);
        assert_eq!(batches[0].2[0].last_offset, 12);
        assert_eq!(batches[0].2[0].types, vec![ACK_GAP]);
    }

    #[test]
    fn share_fetch_missing_acquired_skips_present_offsets() {
        let acquired = [
            AcquiredRange {
                first_offset: 0,
                last_offset: 4,
                delivery_count: 1,
            },
            AcquiredRange {
                first_offset: 10,
                last_offset: 14,
                delivery_count: 2,
            },
        ];
        let present = HashSet::from([0, 1, 2, 3, 4]);
        let missing = share_fetch_missing_acquired(&acquired, &present).unwrap();
        assert_eq!(missing, vec![(10, 2), (11, 2), (12, 2), (13, 2), (14, 2)]);
    }

    #[test]
    fn share_fetch_acquired_in_range_intersects_first_batch() {
        let acquired = [AcquiredRange {
            first_offset: 0,
            last_offset: 29,
            delivery_count: 1,
        }];
        let rejected = share_fetch_acquired_in_range(&acquired, 0, 9).unwrap();
        assert_eq!(
            rejected,
            (0..10).map(|offset| (offset, 1)).collect::<Vec<_>>()
        );
        let unaligned = share_fetch_acquired_in_range(&acquired, 10, 19).unwrap();
        assert_eq!(
            unaligned,
            (10..20).map(|offset| (offset, 1)).collect::<Vec<_>>()
        );
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
