//! Share groups (KIP-932): queue-style consumption with per-record ack.

mod acquired;

use std::collections::{BTreeMap, HashMap, HashSet};
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
    collect_topics, coord_roundtrip, discover_coord_within, filter_matching_topics, TopicMatch,
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
    encode_one_share_acknowledge, encode_share_acknowledge_request,
    encode_share_acknowledge_topics, encode_share_fetch_request_with_options,
    encode_share_group_heartbeat_request, AcknowledgementBatch, ShareAckTopic, ShareFetchPartition,
    ShareFetchRequestOptions, ShareFetchTopic, ShareGroupHeartbeatRequest, ShareTopicPartitions,
    ACK_ACCEPT, ACK_REJECT, ACK_RELEASE, ACK_RENEW,
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
    /// Topic name. Short names are stored inline, the same as [`crate::TopicName`].
    pub topic: crate::consumer::TopicName,
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
        crate::consumer::partitions_from(&self.records, |rec| &rec.topic, |rec| rec.partition)
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
    /// first-seen order. A record at [`i64::MAX`] has no next offset, so that
    /// partition is omitted.
    #[must_use]
    pub fn next_offsets(&self) -> Vec<(crate::TopicPartition, crate::OffsetAndMetadata)> {
        crate::consumer::next_offsets_from(
            &self.records,
            |rec| &rec.topic,
            |rec| rec.partition,
            |rec| rec.offset,
            |rec| rec.leader_epoch,
        )
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

type AcquisitionMap = HashMap<crate::consumer::TopicName, Vec<AcquisitionRun>>;

#[derive(Clone, Copy)]
struct Acquisition {
    node: i32,
    delivery_count: i16,
    expires_at: Option<Instant>,
}

struct AcquisitionRun {
    partition: i32,
    first: i64,
    last: i64,
    acquisition: Acquisition,
}

fn same_acquisition(left: &Acquisition, right: &Acquisition) -> bool {
    left.node == right.node
        && left.delivery_count == right.delivery_count
        && left.expires_at == right.expires_at
}

fn remember_acquisition(
    map: &mut AcquisitionMap,
    topic: &crate::consumer::TopicName,
    partition: i32,
    offset: i64,
    acquisition: Acquisition,
) {
    if map.get(topic.as_str()).is_none() {
        let _ = map.insert(topic.clone(), Vec::new());
    }
    let Some(runs) = map.get_mut(topic.as_str()) else {
        return;
    };
    if let Some(run) = runs.iter_mut().find(|run| {
        run.partition == partition
            && run.last < i64::MAX
            && run.last.saturating_add(1) == offset
            && same_acquisition(&run.acquisition, &acquisition)
    }) {
        run.last = offset;
        return;
    }
    if let Some(run) = runs.iter_mut().find(|run| {
        run.partition == partition
            && run.first > i64::MIN
            && run.first.saturating_sub(1) == offset
            && same_acquisition(&run.acquisition, &acquisition)
    }) {
        run.first = offset;
        return;
    }
    if runs.iter().any(|run| {
        run.partition == partition
            && run.first <= offset
            && offset <= run.last
            && same_acquisition(&run.acquisition, &acquisition)
    }) {
        return;
    }
    if let Some(position) = runs
        .iter()
        .position(|run| run.partition == partition && run.first <= offset && offset <= run.last)
    {
        replace_acquired_offset(runs, position, offset, acquisition);
        return;
    }
    runs.push(AcquisitionRun {
        partition,
        first: offset,
        last: offset,
        acquisition,
    });
}

fn replace_acquired_offset(
    runs: &mut Vec<AcquisitionRun>,
    position: usize,
    offset: i64,
    acquisition: Acquisition,
) {
    let Some(run) = runs.get(position) else {
        return;
    };
    let partition = run.partition;
    let first = run.first;
    let last = run.last;
    let old = run.acquisition;
    let _ = runs.swap_remove(position);
    if first < offset {
        runs.push(AcquisitionRun {
            partition,
            first,
            last: offset.saturating_sub(1),
            acquisition: old,
        });
    }
    runs.push(AcquisitionRun {
        partition,
        first: offset,
        last: offset,
        acquisition,
    });
    if offset < last {
        runs.push(AcquisitionRun {
            partition,
            first: offset.saturating_add(1),
            last,
            acquisition: old,
        });
    }
}

fn acquisition_run(
    runs: &[AcquisitionRun],
    partition: i32,
    offset: i64,
) -> Option<&AcquisitionRun> {
    runs.iter()
        .find(|run| run.partition == partition && run.first <= offset && offset <= run.last)
}

fn acquisition_at<'a>(
    map: &'a AcquisitionMap,
    topic: &crate::consumer::TopicName,
    partition: i32,
    offset: i64,
) -> Option<&'a Acquisition> {
    acquisition_run(map.get(topic)?, partition, offset).map(|run| &run.acquisition)
}

fn acquisition_node(map: &AcquisitionMap, topic: &str, partition: i32, offset: i64) -> Option<i32> {
    acquisition_run(map.get(topic)?, partition, offset).map(|run| run.acquisition.node)
}

fn acquisition_node_for_range(
    map: &AcquisitionMap,
    topic: &str,
    partition: i32,
    first: i64,
    last: i64,
) -> Option<i32> {
    if last < first {
        return None;
    }
    let runs = map.get(topic)?;
    let mut cursor = first;
    let mut node = None;
    while cursor <= last {
        let covering = runs
            .iter()
            .find(|run| run.partition == partition && run.first <= cursor && cursor <= run.last)?;
        if node.is_some_and(|current| current != covering.acquisition.node) {
            return None;
        }
        node = Some(covering.acquisition.node);
        if covering.last >= last {
            return node;
        }
        if covering.last == i64::MAX {
            return node;
        }
        cursor = covering.last.saturating_add(1);
    }
    node
}

fn span_len(first: i64, last: i64) -> usize {
    if last < first {
        return 0;
    }
    match last
        .checked_sub(first)
        .and_then(|delta| delta.checked_add(1))
    {
        Some(len) => usize::try_from(len).unwrap_or(usize::MAX),
        None => usize::MAX,
    }
}

fn covered_len(run: &AcquisitionRun, sent: &[AcknowledgementBatch]) -> usize {
    sent.iter()
        .map(|batch| {
            let start = batch.first_offset.max(run.first);
            let end = batch.last_offset.min(run.last);
            if start > end {
                0
            } else {
                span_len(start, end)
            }
        })
        .fold(0usize, usize::saturating_add)
}

fn expand_partial_runs(
    runs: &mut Vec<AcquisitionRun>,
    partition: i32,
    sent: &[AcknowledgementBatch],
) {
    let partial = runs.iter().any(|run| {
        if run.partition != partition {
            return false;
        }
        let covered = covered_len(run, sent);
        let whole = span_len(run.first, run.last);
        covered > 0 && covered < whole
    });
    if !partial {
        return;
    }
    let mut next = Vec::new();
    for run in runs.drain(..) {
        let whole = span_len(run.first, run.last);
        let covered = covered_len(&run, sent);
        if run.partition != partition || covered == 0 || covered >= whole {
            next.push(run);
            continue;
        }
        let mut offset = run.first;
        loop {
            next.push(AcquisitionRun {
                partition: run.partition,
                first: offset,
                last: offset,
                acquisition: run.acquisition,
            });
            if offset == run.last || offset == i64::MAX {
                break;
            }
            offset = offset.saturating_add(1);
        }
    }
    *runs = next;
}

fn drop_covered_acquisitions(
    runs: &mut Vec<AcquisitionRun>,
    partition: i32,
    sent: &[AcknowledgementBatch],
) {
    expand_partial_runs(runs, partition, sent);
    runs.retain(|run| {
        run.partition != partition || covered_len(run, sent) < span_len(run.first, run.last)
    });
}

fn renew_covered_acquisitions(
    runs: &mut Vec<AcquisitionRun>,
    partition: i32,
    sent: &[AcknowledgementBatch],
    expiry: Option<Instant>,
) {
    expand_partial_runs(runs, partition, sent);
    for run in runs.iter_mut() {
        if run.partition == partition && covered_len(run, sent) >= span_len(run.first, run.last) {
            run.acquisition.expires_at = expiry;
        }
    }
}

fn count_covered_acquisitions(
    runs: &[AcquisitionRun],
    partition: i32,
    sent: &[AcknowledgementBatch],
) -> usize {
    runs.iter()
        .filter(|run| run.partition == partition)
        .map(|run| covered_len(run, sent))
        .fold(0usize, usize::saturating_add)
}

fn take_delivery(pending: &mut Vec<ShareRecord>, count: usize) -> Vec<ShareRecord> {
    let count = count.min(pending.len());
    if count == pending.len() {
        return std::mem::take(pending);
    }
    let rest = pending.split_off(count);
    std::mem::replace(pending, rest)
}

fn take_record_buf(spare: &mut Vec<ShareRecord>) -> Vec<ShareRecord> {
    let mut incoming = std::mem::take(spare);
    incoming.clear();
    incoming
}

fn park_record_buf(spare: &mut Vec<ShareRecord>, incoming: Vec<ShareRecord>) {
    if incoming.capacity() > spare.capacity() {
        *spare = incoming;
    }
}

fn note_share_offset(
    cursors: &mut Vec<(i32, i64)>,
    spilled: &mut Vec<(i32, HashSet<i64>)>,
    partition: i32,
    offset: i64,
) -> bool {
    if let Some(set) = spilled
        .iter_mut()
        .find(|(part, _)| *part == partition)
        .map(|(_, set)| set)
    {
        return set.insert(offset);
    }
    if let Some((_, last)) = cursors.iter_mut().find(|(part, _)| *part == partition) {
        if offset == *last {
            return false;
        }
        if offset < *last {
            let mut set = HashSet::new();
            let _ = set.insert(*last);
            let fresh = set.insert(offset);
            spilled.push((partition, set));
            *last = offset;
            return fresh;
        }
        *last = offset;
        return true;
    }
    cursors.push((partition, offset));
    true
}

fn acquisition_count(map: &AcquisitionMap) -> usize {
    map.values()
        .map(|runs| {
            runs.iter()
                .map(|run| span_len(run.first, run.last))
                .fold(0usize, usize::saturating_add)
        })
        .sum()
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

/// Leader node to the topics and partitions it serves.
///
/// A topic name is the assignment's allocation. Later polls refill the
/// partition list and leave that name in place.
type ShareLeaderPlan = HashMap<i32, HashMap<Arc<str>, Vec<i32>>>;

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
    /// Assigned partitions. Every partition of a topic shares that topic's name.
    assigned: Vec<(Arc<str>, i32)>,
    /// Topic whose leader list is loaded. Cleared at the start of each poll so
    /// a metadata refresh is visible. The name is the assignment's allocation.
    leader_topic: Option<Arc<str>>,
    /// Leader ids for `leader_topic`. Kept so the next poll does not allocate
    /// the list again.
    leader_ids: Vec<i32>,
    /// Topic names checked for metadata freshness. Cleared in place so a
    /// steady subscription does not allocate the name again.
    metadata_names: Vec<String>,
    /// Leader to topic to partitions. Partition lists are cleared and refilled
    /// each poll; topic names stay when the assignment is unchanged.
    leader_plan: ShareLeaderPlan,
    /// Leader node ids for the current plan. Reused so a poll does not allocate
    /// the id list again.
    leader_nodes: Vec<i32>,
    /// ShareFetch topic lists returned from a previous poll.
    spare_fetch_topics: Vec<Vec<ShareFetchTopic>>,
    /// ShareFetch partition indexes returned from a previous poll.
    spare_fetch_ids: Vec<BTreeMap<[u8; 16], Vec<i32>>>,
    /// Offsets for a one-partition acknowledgement. Reused across calls.
    ack_offsets: Vec<i64>,
    /// Short topic name shared by successive acknowledgements of that name.
    ack_topic: Option<Arc<str>>,
    /// Acknowledge-type lists from the previous call. A one-type range refills one.
    spare_ack_types: Vec<Vec<i8>>,
    /// Partition and offset pairs for a one-topic acknowledgement. Reused
    /// across calls.
    ack_pairs: Vec<(i32, i64)>,
    /// Last ShareFetch offset seen for each partition in the current response.
    /// In-order records do not need a per-offset set.
    fetch_cursors: Vec<(i32, i64)>,
    /// Record list from the previous ShareFetch. Cleared and refilled so a
    /// steady poll does not allocate the list again.
    spare_incoming: Vec<ShareRecord>,
    /// Prepared ShareFetch jobs from the previous poll. The jobs are drained;
    /// the list itself is refilled.
    spare_prepared: Vec<PreparedShareFetch>,
    /// Finished ShareFetch replies from the previous poll. Drained after they
    /// are applied, then refilled.
    spare_ready: Vec<ReadyShareFetch>,
    /// Subscribed topic id by name. A long name is the allocation fetched records share.
    topic_ids: HashMap<Arc<str>, [u8; 16]>,
    /// Share session epoch per share-partition leader (KIP-932).
    share_epochs: HashMap<i32, i32>,
    share_conns: HashMap<i32, ShareNodeConnection>,
    share_addresses: HashMap<i32, String>,
    /// CurrentLeader values from the latest share response. A metadata refresh
    /// drops cached leaders, and these patches are put back afterward.
    leader_patches: Vec<ShareLeaderPatch>,
    acquisitions: AcquisitionMap,
    pending_delivery: Vec<ShareRecord>,
    acquire_mode: ShareAcquireMode,
    acquisition_lock_timeout_ms: Option<i32>,
    hb_err: Arc<AtomicI16>,
    hb_epoch: Arc<AtomicI32>,
    /// Member id shared with the heartbeat task so a broker-rotated id is used
    /// on the next heartbeat, fetch, and acknowledge.
    hb_member_id: Arc<Mutex<String>>,
    /// Topics sent when the background heartbeat rejoins at epoch 0.
    hb_topics: Arc<Mutex<Vec<String>>>,
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
    /// Group id shared with multi-leader fetch tasks. Rebuilt only when the
    /// text changes.
    fetch_group_id: Arc<str>,
    /// Member id shared with multi-leader fetch tasks. Rebuilt only when the
    /// text changes.
    fetch_member_id: Arc<str>,
    /// ShareFetch requests sent and not yet applied. A multi-leader poll
    /// returns after the collection window once any peer has responded.
    inflight_share_fetches: Option<tokio::task::JoinSet<ShareFetchTask>>,
    inflight_share_nodes: HashMap<i32, InflightShareMeta>,
}

struct InflightShareMeta {
    version: i16,
    by_id: BTreeMap<[u8; 16], Vec<i32>>,
}

struct ReadyShareFetch {
    node: i32,
    version: i16,
    by_id: BTreeMap<[u8; 16], Vec<i32>>,
    body: Result<Bytes>,
}

/// One finished or in-flight ShareFetch. The topic list rides with the task so
/// a completed fetch can return it to the spare pool.
type ShareFetchTask = (
    i32,
    ShareNodeConnection,
    Result<Bytes>,
    Vec<ShareFetchTopic>,
);

/// Aborts parked share fetches if this round is cancelled before they are stored.
struct InflightShareFetches {
    set: Option<tokio::task::JoinSet<ShareFetchTask>>,
    pending: HashMap<i32, InflightShareMeta>,
}

impl Drop for InflightShareFetches {
    fn drop(&mut self) {
        if let Some(mut set) = self.set.take() {
            set.abort_all();
        }
    }
}

impl InflightShareFetches {
    fn park(mut self, group: &mut ShareGroup) {
        let set = self.set.take().unwrap_or_default();
        let pending = std::mem::take(&mut self.pending);
        if set.is_empty() {
            group.inflight_share_fetches = None;
            group.inflight_share_nodes.clear();
        } else {
            group.inflight_share_fetches = Some(set);
            group.inflight_share_nodes = pending;
        }
    }
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
    epoch: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    acquire_mode: i8,
    topics: Vec<ShareFetchTopic>,
}

/// Return the cached text, replacing it only when `current` differs.
fn shared_text(cached: &mut Arc<str>, current: &str) -> Arc<str> {
    if cached.as_ref() != current {
        *cached = Arc::from(current);
    }
    Arc::clone(cached)
}

fn encode_prepared_share_fetch(
    buf: &mut BytesMut,
    job: &PreparedShareFetch,
    group_id: &str,
    member_id: &str,
) -> Result<()> {
    encode_share_fetch_request_with_options(
        buf,
        job.version,
        group_id,
        member_id,
        job.epoch,
        job.max_wait_ms,
        job.min_bytes,
        job.max_bytes,
        job.max_records,
        &job.topics,
        &[],
        job.max_records,
        ShareFetchRequestOptions {
            acquire_mode: job.acquire_mode,
            is_renew_ack: false,
        },
    )
}

struct ShareAssignmentUpdate {
    assigned: Vec<(Arc<str>, i32)>,
    topic_ids: HashMap<Arc<str>, [u8; 16]>,
}

/// Resolve a share-group assignment.
///
/// `None` means the topic ids could not be named, so the caller keeps its
/// current partitions. An empty input is an explicit revoke. Negative
/// partition indexes are omitted, and a repeated partition is stored once.
fn next_share_assignment(
    incoming: &[ShareTopicPartitions],
    id_to_name: &HashMap<[u8; 16], String>,
    name_to_id: &HashMap<String, [u8; 16]>,
    subscribed: &[String],
) -> Option<ShareAssignmentUpdate> {
    if incoming.is_empty() {
        return Some(ShareAssignmentUpdate {
            assigned: Vec::new(),
            topic_ids: HashMap::new(),
        });
    }
    let mut assigned = Vec::new();
    let mut topic_ids = HashMap::new();
    let mut seen: HashMap<&str, HashSet<i32>> = HashMap::new();
    let mut resolved_topic = false;
    for tp in incoming {
        if tp.topic_id == [0u8; 16] {
            continue;
        }
        let name = id_to_name
            .get(&tp.topic_id)
            .map(String::as_str)
            .or_else(|| {
                // Single-topic subscribe: map the only name when metadata
                // has not yet indexed this id.
                if subscribed.len() == 1 {
                    subscribed.first().map(String::as_str)
                } else {
                    None
                }
            });
        let Some(name) = name else {
            continue;
        };
        let topic_id = name_to_id.get(name).copied().unwrap_or(tp.topic_id);
        if topic_id == [0u8; 16] {
            continue;
        }
        resolved_topic = true;
        if let Some(stored) = topic_ids.get_mut(name) {
            *stored = topic_id;
        } else {
            let _ = topic_ids.insert(Arc::from(name), topic_id);
        }
        let Some(shared) = topic_ids
            .get_key_value(name)
            .map(|(key, _)| Arc::clone(key))
        else {
            continue;
        };
        if !seen.contains_key(name) {
            let _ = seen.insert(name, HashSet::new());
        }
        let Some(parts) = seen.get_mut(name) else {
            continue;
        };
        for partition in &tp.partitions {
            if *partition < 0 || !parts.insert(*partition) {
                continue;
            }
            assigned.push((Arc::clone(&shared), *partition));
        }
    }
    if !resolved_topic {
        return None;
    }
    Some(ShareAssignmentUpdate {
        assigned,
        topic_ids,
    })
}

fn fill_unique_names<'a>(names: &mut Vec<String>, topics: impl Iterator<Item = &'a str>) {
    let mut used = 0usize;
    for topic in topics {
        let seen = names
            .get(..used)
            .is_some_and(|prefix| prefix.iter().any(|name| name == topic));
        if seen {
            continue;
        }
        if let Some(slot) = names.get_mut(used) {
            if slot != topic {
                slot.clear();
                slot.push_str(topic);
            }
        } else {
            names.push(topic.to_string());
        }
        used = used.saturating_add(1);
    }
    names.truncate(used);
}

fn clear_share_leader_plan(plan: &mut ShareLeaderPlan) {
    for topics in plan.values_mut() {
        for parts in topics.values_mut() {
            parts.clear();
        }
    }
}

/// Record `topic` as the one whose leader list is loaded.
///
/// Returns whether the caller must load that list. The same name keeps the
/// allocation already stored, including when `topic` is a different copy.
fn remember_share_leader_topic(cached: &mut Option<Arc<str>>, topic: &Arc<str>) -> bool {
    if cached
        .as_ref()
        .is_some_and(|stored| stored.as_ref() == topic.as_ref())
    {
        return false;
    }
    *cached = Some(Arc::clone(topic));
    true
}

fn remember_share_leader(plan: &mut ShareLeaderPlan, node: i32, topic: &Arc<str>, partition: i32) {
    let topics = plan.entry(node).or_default();
    if let Some(parts) = topics.get_mut(topic.as_ref()) {
        parts.push(partition);
        return;
    }
    let _ = topics.insert(Arc::clone(topic), vec![partition]);
}

fn prune_share_leader_plan(plan: &mut ShareLeaderPlan) {
    plan.retain(|_, topics| {
        topics.retain(|_, parts| !parts.is_empty());
        !topics.is_empty()
    });
}

fn refill_share_partitions(dest: &mut Vec<ShareFetchPartition>, parts: &[i32], max_bytes: i32) {
    let mut used = 0usize;
    for &partition in parts {
        if let Some(slot) = dest.get_mut(used) {
            slot.partition = partition;
            slot.partition_max_bytes = max_bytes;
            slot.acknowledgements.clear();
        } else {
            dest.push(ShareFetchPartition {
                partition,
                partition_max_bytes: max_bytes,
                acknowledgements: Vec::new(),
            });
        }
        used = used.saturating_add(1);
    }
    dest.truncate(used);
}

fn refill_share_fetch(
    by_id: &mut BTreeMap<[u8; 16], Vec<i32>>,
    topics: &mut Vec<ShareFetchTopic>,
    groups: &HashMap<Arc<str>, Vec<i32>>,
    topic_ids: &HashMap<Arc<str>, [u8; 16]>,
    max_partition_bytes: i32,
) -> Result<()> {
    for parts in by_id.values_mut() {
        parts.clear();
    }
    for (topic, parts) in groups {
        let id = topic_ids
            .get(topic.as_ref())
            .copied()
            .filter(|id| *id != [0; 16])
            .ok_or_else(|| {
                Error::protocol(format!("share assignment missing topic id for {topic}"))
            })?;
        if let Some(slot) = by_id.get_mut(&id) {
            slot.extend(parts.iter().copied());
        } else {
            let mut owned = Vec::new();
            owned.extend_from_slice(parts);
            let _ = by_id.insert(id, owned);
        }
    }
    by_id.retain(|_, parts| !parts.is_empty());
    let mut used = 0usize;
    for (topic_id, partitions) in by_id.iter() {
        if let Some(topic) = topics.get_mut(used) {
            topic.topic_id = *topic_id;
            refill_share_partitions(&mut topic.partitions, partitions, max_partition_bytes);
        } else {
            topics.push(ShareFetchTopic {
                topic_id: *topic_id,
                partitions: partitions
                    .iter()
                    .copied()
                    .map(|partition| ShareFetchPartition {
                        partition,
                        partition_max_bytes: max_partition_bytes,
                        acknowledgements: Vec::new(),
                    })
                    .collect(),
            });
        }
        used = used.saturating_add(1);
    }
    topics.truncate(used);
    Ok(())
}

fn park_capped<T>(pool: &mut Vec<T>, item: T) {
    if pool.len() < 8 {
        pool.push(item);
    }
}

fn share_partition_leader(topic: &str, partition: i32, leaders: &[i32]) -> Result<i32> {
    let idx = usize::try_from(partition).map_err(|_| Error::NoLeader {
        topic: topic.to_string(),
        partition,
    })?;
    leaders
        .get(idx)
        .copied()
        .filter(|&id| id >= 0)
        .ok_or_else(|| Error::NoLeader {
            topic: topic.to_string(),
            partition,
        })
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
        let discovery_deadline = Instant::now() + cfg.request_timeout;
        let consumer = Consumer::connect_within(cfg.clone(), discovery_deadline).await?;
        // Membership uses the group coordinator (Java ShareConsumer / DescribeShareGroups).
        // CoordinatorType.SHARE is only for share-partition state keys
        // (`groupId:topicId:partition`), not the group id alone (KIP-932).
        let discovery_backoff = cfg
            .retry_backoff
            .min(cfg.retry_backoff_max)
            .max(Duration::from_millis(1));
        let coord = loop {
            if Instant::now() >= discovery_deadline {
                return Err(Error::Timeout);
            }
            let result =
                discover_coord_within(&cfg, &group_id, COORDINATOR_GROUP, discovery_deadline).await;
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
        let fetch_group_id = Arc::<str>::from(group_id.as_str());
        let fetch_member_id = Arc::<str>::from(member_id.as_str());
        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_epoch = Arc::new(AtomicI32::new(
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
        ));
        let hb_member_id = Arc::new(Mutex::new(member_id.clone()));
        let hb_topics = Arc::new(Mutex::new(topics.clone()));
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
            leader_topic: None,
            leader_ids: Vec::new(),
            metadata_names: Vec::new(),
            leader_plan: HashMap::new(),
            leader_nodes: Vec::new(),
            spare_fetch_topics: Vec::new(),
            spare_fetch_ids: Vec::new(),
            ack_offsets: Vec::new(),
            ack_topic: None,
            spare_ack_types: Vec::new(),
            ack_pairs: Vec::new(),
            fetch_cursors: Vec::new(),
            spare_incoming: Vec::new(),
            spare_prepared: Vec::new(),
            spare_ready: Vec::new(),
            topic_ids: HashMap::new(),
            share_epochs: HashMap::new(),
            share_conns: HashMap::new(),
            share_addresses: HashMap::new(),
            leader_patches: Vec::new(),
            acquisitions: HashMap::new(),
            pending_delivery: Vec::new(),
            acquire_mode: ShareAcquireMode::BatchOptimized,
            acquisition_lock_timeout_ms: None,
            hb_err,
            hb_epoch,
            hb_member_id,
            hb_topics,
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
            fetch_group_id,
            fetch_member_id,
            inflight_share_fetches: None,
            inflight_share_nodes: HashMap::new(),
        };
        if g.topic_match.is_some() {
            g.topics = g.matching_topic_names_within(discovery_deadline).await?;
            g.last_match_refresh = Instant::now();
            g.publish_hb_topics();
        }

        g.heartbeat_join_within(discovery_deadline).await?;

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
        acquisition_count(&self.acquisitions)
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
            .map(|(t, p)| crate::TopicPartition::new(t.as_ref(), *p))
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

    fn note_share_member_id(&mut self, id: &str) {
        publish_member_id(&mut self.member_id, &mut self.hb_member_id.lock(), id);
    }

    fn adopt_share_member_id(&mut self) {
        let id = self.hb_member_id.lock();
        if !id.is_empty() {
            crate::group::copy_member_id(&mut self.member_id, &id);
        }
    }

    fn publish_hb_topics(&self) {
        *self.hb_topics.lock() = self.topics.clone();
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
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        self.heartbeat_join_within(deadline).await
    }

    async fn heartbeat_join_within(&mut self, deadline: Instant) -> Result<()> {
        self.refresh_share_topics_within(deadline).await?;
        let version = spoken_share_group_heartbeat(self.coord.share_group_heartbeat_version)?;

        let mut first = true;
        let mut req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            rack_id: self.cfg.rack.clone(),
            // Re-send subscription until assignment arrives so the
            // coordinator can resolve topic ids (KIP-932).
            subscribed_topic_names: Some(self.topics.clone()),
        };
        loop {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return if first { Err(Error::Timeout) } else { Ok(()) };
            }
            crate::group::copy_member_id(&mut req.member_id, &self.member_id);
            req.member_epoch = if first {
                ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
            } else {
                self.member_epoch
            };
            reuse_share_subscription(&mut req, &self.topics);
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
                self.note_share_member_id(&id);
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
            // Join succeeded; poll waits for assignment (Java ShareConsumer).
            let nap =
                Duration::from_millis(50).min(deadline.saturating_duration_since(Instant::now()));
            if nap.is_zero() {
                return Ok(());
            }
            first = false;
            tokio::time::sleep(nap).await;
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                return Ok(());
            }
            self.refresh_share_topics_within(deadline).await?;
        }
    }

    async fn refresh_share_topics_within(&mut self, deadline: Instant) -> Result<()> {
        if self.topics.is_empty() {
            return Ok(());
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        self.consumer
            .refresh_metadata_timeout(Some(&self.topics), timeout)
            .await
    }

    fn apply_share_assignment(&mut self, assignment: Option<&[ShareTopicPartitions]>) {
        let Some(assigned) = assignment else {
            return;
        };
        let id_to_name = self.consumer.topic_id_names();
        let name_to_id = self.consumer.topic_name_ids();
        let Some(update) = next_share_assignment(assigned, id_to_name, name_to_id, &self.topics)
        else {
            return;
        };
        self.assigned = update.assigned;
        self.topic_ids = update.topic_ids;
        if self.assigned.is_empty() {
            self.acquisitions.clear();
            self.pending_delivery.clear();
            return;
        }
        let still_assigned: HashSet<(&str, i32)> = self
            .assigned
            .iter()
            .map(|(topic, partition)| (topic.as_ref(), *partition))
            .collect();
        self.acquisitions.retain(|topic, parts| {
            parts.retain(|run| still_assigned.contains(&(topic.as_str(), run.partition)));
            !parts.is_empty()
        });
        self.pending_delivery.retain(|r| {
            acquisition_at(&self.acquisitions, &r.topic, r.partition, r.offset).is_some()
        });
    }

    /// Replace a stored topic id when metadata has a new one.
    ///
    /// Records acquired under the previous id belong to the deleted topic.
    /// A zero metadata id does not clear a stored id.
    fn refresh_share_topic_ids(&mut self) {
        if share_topic_ids_current(
            &self.topic_ids,
            &self.assigned,
            self.consumer.topic_name_ids(),
        ) {
            return;
        }
        let metadata = self.consumer.topic_name_ids();
        let mut names: Vec<Arc<str>> = self.topic_ids.keys().cloned().collect();
        let known: HashSet<&str> = names.iter().map(|name| name.as_ref()).collect();
        let mut added = HashSet::new();
        let mut extra = Vec::new();
        for (topic, _) in &self.assigned {
            if !known.contains(topic.as_ref()) && added.insert(topic.as_ref()) {
                extra.push(Arc::clone(topic));
            }
        }
        names.extend(extra);
        for topic in names {
            let stored = self.topic_ids.get(topic.as_ref()).copied();
            let next = metadata.get(topic.as_ref()).copied();
            let Some(id) = refreshed_share_topic_id(stored, next) else {
                continue;
            };
            if stored.is_some_and(|stored| share_topic_recreated(stored, next)) {
                let _ = self.acquisitions.remove(topic.as_ref());
                self.pending_delivery
                    .retain(|record| record.topic.as_str() != topic.as_ref());
            }
            let _previous = self.topic_ids.insert(topic, id);
        }
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
        let mut req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: self.member_epoch,
            rack_id: self.cfg.rack.clone(),
            subscribed_topic_names: Some(self.topics.clone()),
        };
        while self.assigned.is_empty() {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
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
            self.refresh_share_topics_within(deadline).await?;
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            crate::group::copy_member_id(&mut req.member_id, &self.member_id);
            req.member_epoch = self.member_epoch;
            reuse_share_subscription(&mut req, &self.topics);
            let (version, body) = coord_roundtrip(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
                SHARE_GROUP_HEARTBEAT,
                |version, buf| encode_share_group_heartbeat_request(buf, version, &req),
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
                self.note_share_member_id(&id);
            }
            if resp.member_epoch > 0 {
                self.member_epoch = resp.member_epoch;
                self.hb_epoch.store(self.member_epoch, Ordering::SeqCst);
            }
            self.apply_share_assignment(resp.assignment.as_deref());
            if !self.assigned.is_empty() {
                return Ok(());
            }
            let nap =
                Duration::from_millis(50).min(deadline.saturating_duration_since(Instant::now()));
            if nap.is_zero() {
                return Err(Error::Timeout);
            }
            tokio::time::sleep(nap).await;
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
    /// subscribed to any topics.`).
    pub async fn poll(&mut self) -> Result<ShareRecords> {
        self.poll_with_wait(self.cfg.max_wait_ms).await
    }

    async fn poll_with_wait(&mut self, max_wait_ms: i32) -> Result<ShareRecords> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        self.adopt_share_member_id();
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
        if share_membership_lost(hb) {
            self.member_epoch = ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH;
            self.hb_epoch.store(
                ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
                Ordering::SeqCst,
            );
            self.heartbeat_join().await?;
        } else if hb != 0 {
            return Err(Error::broker(hb, "ShareGroupHeartbeat"));
        }
        let started = Instant::now();
        let deadline = started
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        self.ensure_assignment(deadline).await?;
        let mut attempt = 0u32;
        loop {
            let recs = match self.poll_leaders(deadline, max_wait_ms).await {
                Ok(recs) => recs,
                // Records already acquired stay delivered when a later leader
                // runs out of time.
                Err(Error::Timeout) if !self.pending_delivery.is_empty() => {
                    self.drain_pending_delivery()
                }
                Err(e) if share_leader_retriable(&e) || share_fetch_session_reset(&e) => {
                    if Instant::now() >= deadline {
                        if self.pending_delivery.is_empty() {
                            return Err(Error::Timeout);
                        }
                        self.drain_pending_delivery()
                    } else {
                        self.consumer.sleep_retry_backoff(attempt, deadline).await?;
                        attempt = attempt.saturating_add(1);
                        if Instant::now() >= deadline {
                            if self.pending_delivery.is_empty() {
                                return Err(Error::Timeout);
                            }
                            self.drain_pending_delivery()
                        } else {
                            if share_leader_retriable(&e) {
                                let patches = std::mem::take(&mut self.leader_patches);
                                let refreshed = self
                                    .refresh_assigned_metadata(
                                        deadline.saturating_duration_since(Instant::now()),
                                    )
                                    .await;
                                reapply_share_leader_patches(&mut self.consumer, &patches);
                                if let Err(error) = refreshed {
                                    if patches.is_empty() {
                                        return Err(error);
                                    }
                                }
                            } else {
                                self.leader_patches.clear();
                            }
                            continue;
                        }
                    }
                }
                Err(e) => {
                    self.fetch_errors = self.fetch_errors.saturating_add(1);
                    return Err(e);
                }
            };
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
        self.acquisitions.retain(|_, parts| {
            parts.retain(|run| run.acquisition.expires_at.is_none_or(|expiry| expiry > now));
            !parts.is_empty()
        });
        self.pending_delivery.retain(|r| {
            acquisition_at(&self.acquisitions, &r.topic, r.partition, r.offset)
                .is_some_and(|a| a.delivery_count == r.delivery_count)
        });
    }

    fn drain_pending_delivery(&mut self) -> Vec<ShareRecord> {
        let count = self
            .cfg
            .max_poll_records
            .unwrap_or(usize::MAX)
            .min(self.pending_delivery.len());
        take_delivery(&mut self.pending_delivery, count)
    }

    /// Open missing share-leader sockets together. Failures stay for the
    /// per-node lookup, which reports them on the same deadline.
    async fn warm_share_leaders(&mut self, nodes: &[i32], deadline: Instant) {
        let mut dials = Vec::new();
        for &node in nodes {
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
                let _ = self.share_conns.remove(&node);
            }
            if self.share_conns.contains_key(&node) {
                continue;
            }
            let Some(addr) = self
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
            else {
                continue;
            };
            dials.push((node, addr));
        }
        if dials.is_empty() || Instant::now() >= deadline {
            return;
        }
        let client_id = self.cfg.client_id.clone();
        let connect_timeout = self.cfg.connect_timeout;
        let tls = self.cfg.tls.clone();
        let sasl_plain = self.cfg.sasl_plain.clone();
        let sasl_scram = self.cfg.sasl_scram.clone();
        let sasl_scram_sha512 = self.cfg.sasl_scram_sha512.clone();
        let sasl_oauthbearer = self.cfg.sasl_oauthbearer.clone();
        let sasl_oauthbearer_oidc = self.cfg.sasl_oauthbearer_oidc.clone();
        let mut waiting = dials.into_iter();
        type ShareDial = (
            i32,
            String,
            Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)>,
        );
        let mut inflight = tokio::task::JoinSet::<ShareDial>::new();
        let spawn = |inflight: &mut tokio::task::JoinSet<ShareDial>, node: i32, addr: String| {
            let client_id = client_id.clone();
            let tls = tls.clone();
            let sasl_plain = sasl_plain.clone();
            let sasl_scram = sasl_scram.clone();
            let sasl_scram_sha512 = sasl_scram_sha512.clone();
            let sasl_oauthbearer = sasl_oauthbearer.clone();
            let sasl_oauthbearer_oidc = sasl_oauthbearer_oidc.clone();
            let addr_for_task = addr.clone();
            let _ = inflight.spawn(async move {
                let result = Consumer::open_fetch_conn(
                    &addr_for_task,
                    &client_id,
                    connect_timeout,
                    tls.as_ref(),
                    sasl_plain.as_ref(),
                    sasl_scram.as_ref(),
                    sasl_scram_sha512.as_ref(),
                    sasl_oauthbearer.as_deref(),
                    sasl_oauthbearer_oidc.as_ref(),
                    deadline,
                )
                .await;
                (node, addr, result)
            });
        };
        while inflight.len() < 8 {
            let Some((node, addr)) = waiting.next() else {
                break;
            };
            spawn(&mut inflight, node, addr);
        }
        while !inflight.is_empty() {
            if Instant::now() >= deadline {
                inflight.abort_all();
                break;
            }
            let rest = deadline.saturating_duration_since(Instant::now());
            let joined = tokio::select! {
                joined = inflight.join_next() => joined,
                _ = tokio::time::sleep(rest) => {
                    inflight.abort_all();
                    break;
                }
            };
            let Some(Ok((node, addr, result))) = joined else {
                break;
            };
            if let Ok((conn, versions)) = result {
                let fetch_version = versions
                    .api_version(SHARE_FETCH)
                    .and_then(|version| {
                        pick_version(
                            version.min_version,
                            version.max_version,
                            0,
                            SHARE_FETCH_RUNTIME_MAX_VERSION,
                        )
                    })
                    .unwrap_or(-1);
                let acknowledge_version = versions
                    .api_version(SHARE_ACKNOWLEDGE)
                    .and_then(|version| {
                        pick_version(
                            version.min_version,
                            version.max_version,
                            0,
                            SHARE_ACKNOWLEDGE_RUNTIME_MAX_VERSION,
                        )
                    })
                    .unwrap_or(-1);
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
            while inflight.len() < 8 {
                let Some((node, addr)) = waiting.next() else {
                    break;
                };
                spawn(&mut inflight, node, addr);
            }
        }
    }

    async fn node_share_version_until(
        &mut self,
        node: i32,
        api: i16,
        deadline: Instant,
    ) -> Result<i16> {
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
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                return Err(Error::Timeout);
            }
            let mut conn = BrokerConn::connect_tls(
                &addr,
                &self.cfg.client_id,
                self.cfg.connect_timeout.min(rest),
                self.cfg.tls.as_ref(),
            )
            .await?;
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                return Err(Error::Timeout);
            }
            let versions = crate::protocol::api::negotiate_api_versions(&mut conn, rest).await?;
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
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                return Err(Error::Timeout);
            }
            crate::protocol::sasl::authenticate(
                &mut conn,
                self.cfg.sasl_plain.as_ref(),
                self.cfg.sasl_scram.as_ref(),
                self.cfg.sasl_scram_sha512.as_ref(),
                self.cfg.sasl_oauthbearer.as_deref(),
                self.cfg.sasl_oauthbearer_oidc.as_ref(),
                rest,
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
        self.acquisitions.retain(|_, parts| {
            parts.retain(|run| run.acquisition.node != node);
            !parts.is_empty()
        });
        self.pending_delivery.retain(|r| {
            acquisition_at(&self.acquisitions, &r.topic, r.partition, r.offset).is_some()
        });
        self.consumer.drop_node(node);
    }

    async fn refresh_assigned_metadata(&mut self, timeout: Duration) -> Result<()> {
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        for t in &self.topics {
            self.consumer.invalidate_topic(t);
        }
        self.consumer
            .refresh_metadata_timeout(Some(&self.topics), timeout)
            .await
    }

    fn take_share_ids(&mut self) -> BTreeMap<[u8; 16], Vec<i32>> {
        self.spare_fetch_ids.pop().unwrap_or_default()
    }

    fn take_share_topics(&mut self) -> Vec<ShareFetchTopic> {
        self.spare_fetch_topics.pop().unwrap_or_default()
    }

    fn park_share_ids(&mut self, ids: BTreeMap<[u8; 16], Vec<i32>>) {
        park_capped(&mut self.spare_fetch_ids, ids);
    }

    fn park_share_topics(&mut self, topics: Vec<ShareFetchTopic>) {
        park_capped(&mut self.spare_fetch_topics, topics);
    }

    fn park_prepared(&mut self, prepared: &mut Vec<PreparedShareFetch>) {
        for job in prepared.drain(..) {
            self.park_share_ids(job.by_id);
            self.park_share_topics(job.topics);
        }
    }

    fn take_prepared(&mut self) -> Vec<PreparedShareFetch> {
        std::mem::take(&mut self.spare_prepared)
    }

    fn keep_prepared(&mut self, mut prepared: Vec<PreparedShareFetch>) {
        self.park_prepared(&mut prepared);
        if prepared.capacity() > self.spare_prepared.capacity() {
            self.spare_prepared = prepared;
        }
    }

    fn take_share_ready(&mut self) -> Vec<ReadyShareFetch> {
        std::mem::take(&mut self.spare_ready)
    }

    fn keep_share_ready(&mut self, mut ready: Vec<ReadyShareFetch>) {
        for reply in ready.drain(..) {
            self.park_share_ids(reply.by_id);
        }
        if ready.capacity() > self.spare_ready.capacity() {
            self.spare_ready = ready;
        }
    }

    async fn leaders_of(&mut self, deadline: Instant) -> Result<()> {
        // One stored name per topic. A fresh topic skips the metadata RPC
        // without copying that name again.
        fill_unique_names(
            &mut self.metadata_names,
            self.assigned.iter().map(|(topic, _)| topic.as_ref()),
        );
        let count = self.metadata_names.len();
        for index in 0..count {
            let fresh = self
                .metadata_names
                .get(index)
                .is_some_and(|topic| self.consumer.topic_metadata_fresh(topic));
            if fresh {
                continue;
            }
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                return Err(Error::Timeout);
            }
            let topic = self.metadata_names.get(index).cloned().unwrap_or_default();
            self.consumer
                .ensure_topic_metadata_timeout(&topic, rest)
                .await?;
        }
        clear_share_leader_plan(&mut self.leader_plan);
        self.leader_topic = None;
        for (topic, partition) in &self.assigned {
            if remember_share_leader_topic(&mut self.leader_topic, topic) {
                let ids = self.consumer.leader_ids_of(topic)?;
                self.leader_ids.clear();
                self.leader_ids.extend_from_slice(ids);
            }
            let node = share_partition_leader(topic, *partition, &self.leader_ids)?;
            if !self.consumer.broker_known(node) {
                return Err(Error::NoLeader {
                    topic: topic.as_ref().to_owned(),
                    partition: *partition,
                });
            }
            remember_share_leader(&mut self.leader_plan, node, topic, *partition);
        }
        prune_share_leader_plan(&mut self.leader_plan);
        Ok(())
    }

    fn abort_inflight_share_fetches(&mut self) {
        if let Some(mut set) = self.inflight_share_fetches.take() {
            set.abort_all();
        }
        self.inflight_share_nodes.clear();
    }

    /// Send prepared fetches and collect the replies that finish in time.
    ///
    /// After the first reply, slower leaders stay parked for a later poll.
    /// A poll with nothing ready returns a timeout.
    async fn collect_share_fetches(
        &mut self,
        prepared: &mut Vec<PreparedShareFetch>,
        ready: &mut Vec<ReadyShareFetch>,
        deadline: Instant,
    ) -> Result<()> {
        let mut inflight = InflightShareFetches {
            set: Some(self.inflight_share_fetches.take().unwrap_or_default()),
            pending: std::mem::take(&mut self.inflight_share_nodes),
        };
        let timeout = deadline.saturating_duration_since(Instant::now());
        let group_id = shared_text(&mut self.fetch_group_id, self.group_id.as_str());
        let member_id = shared_text(&mut self.fetch_member_id, self.member_id.as_str());
        if timeout.is_zero() {
            self.park_prepared(prepared);
        } else if let Some(set) = inflight.set.as_mut() {
            for mut job in prepared.drain(..) {
                if inflight.pending.contains_key(&job.node) {
                    self.park_share_ids(job.by_id);
                    self.park_share_topics(job.topics);
                    continue;
                }
                let Some(mut peer) = self.share_conns.remove(&job.node) else {
                    self.park_share_ids(job.by_id);
                    self.park_share_topics(job.topics);
                    continue;
                };
                let node = job.node;
                let version = job.version;
                let group_id = Arc::clone(&group_id);
                let member_id = Arc::clone(&member_id);
                let by_id = std::mem::take(&mut job.by_id);
                let _ = inflight.pending.insert(
                    node,
                    InflightShareMeta {
                        version: job.version,
                        by_id,
                    },
                );
                let _ = set.spawn(async move {
                    let result = peer
                        .conn
                        .roundtrip(
                            SHARE_FETCH,
                            version,
                            |buf| encode_prepared_share_fetch(buf, &job, &group_id, &member_id),
                            timeout,
                        )
                        .await;
                    (node, peer, result, job.topics)
                });
            }
        } else {
            self.park_prepared(prepared);
            return Err(Error::protocol("inflight share fetch set missing"));
        }
        let mut first_reply: Option<Instant> = None;
        while inflight.set.as_ref().is_some_and(|set| !set.is_empty()) {
            let wait = if let Some(started) = first_reply {
                crate::consumer::multi_leader_grace(
                    started.elapsed(),
                    deadline.saturating_duration_since(Instant::now()),
                )
            } else {
                deadline.saturating_duration_since(Instant::now())
            };
            if wait.is_zero() {
                break;
            }
            let Some(set) = inflight.set.as_mut() else {
                break;
            };
            let joined = tokio::select! {
                biased;
                joined = set.join_next() => joined,
                () = tokio::time::sleep(wait) => break,
            };
            let Some(joined) = joined else {
                break;
            };
            let (node, peer, result, topics) = match joined {
                Ok(joined) => joined,
                Err(error) => {
                    inflight.park(self);
                    return Err(Error::protocol(format!("share fetch task: {error}")));
                }
            };
            self.park_share_topics(topics);
            let meta = inflight.pending.remove(&node);
            let _ = self.share_conns.insert(node, peer);
            if first_reply.is_none() {
                first_reply = Some(Instant::now());
            }
            if let Some(meta) = meta {
                ready.push(ReadyShareFetch {
                    node,
                    version: meta.version,
                    by_id: meta.by_id,
                    body: result,
                });
            }
        }
        if let Some(set) = inflight.set.as_mut() {
            while let Some(joined) = set.try_join_next() {
                let (node, peer, result, topics) = match joined {
                    Ok(joined) => joined,
                    Err(error) => {
                        inflight.park(self);
                        return Err(Error::protocol(format!("share fetch task: {error}")));
                    }
                };
                self.park_share_topics(topics);
                let meta = inflight.pending.remove(&node);
                let _ = self.share_conns.insert(node, peer);
                if let Some(meta) = meta {
                    ready.push(ReadyShareFetch {
                        node,
                        version: meta.version,
                        by_id: meta.by_id,
                        body: result,
                    });
                }
            }
        }
        inflight.park(self);
        if ready.is_empty() && Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        ready.sort_by_key(|reply| reply.node);
        Ok(())
    }

    async fn poll_leaders(
        &mut self,
        deadline: Instant,
        max_wait_ms: i32,
    ) -> Result<Vec<ShareRecord>> {
        self.refresh_share_topic_ids();
        if !self.pending_delivery.is_empty() {
            return Ok(self.drain_pending_delivery());
        }
        self.leaders_of(deadline).await?;
        self.leader_nodes.clear();
        self.leader_nodes.extend(self.leader_plan.keys().copied());
        self.leader_nodes.sort_unstable();
        let metadata_bytes = self
            .acquisitions
            .iter()
            .fold(0usize, |sum, (topic, parts)| {
                sum.saturating_add(
                    acquisition_storage_bytes(topic.as_str()).saturating_mul(parts.len()),
                )
            });
        let mut remaining_bytes = self
            .decoded_byte_limit()
            .checked_sub(metadata_bytes)
            .ok_or_else(|| Error::protocol("ShareFetch acquisition metadata budget exceeded"))?;
        if self.leader_nodes.len() > 1 {
            let nodes = std::mem::take(&mut self.leader_nodes);
            self.warm_share_leaders(&nodes, deadline).await;
            self.leader_nodes = nodes;
        }
        let node_count = self.leader_nodes.len();
        let mut prepared = self.take_prepared();
        for index in 0..node_count {
            let Some(node) = self.leader_nodes.get(index).copied() else {
                continue;
            };
            if self.inflight_share_nodes.contains_key(&node) {
                continue;
            }
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                break;
            }
            let version = match self
                .node_share_version_until(node, SHARE_FETCH, deadline)
                .await
            {
                Ok(version) => version,
                Err(Error::Timeout) => break,
                Err(error) => {
                    self.keep_prepared(prepared);
                    return Err(error);
                }
            };
            if self.acquire_mode == ShareAcquireMode::RecordLimit && version < 2 {
                self.keep_prepared(prepared);
                return Err(Error::Unsupported(format!(
                    "share broker {node} does not support record-limit acquisition"
                )));
            }
            let epoch = self.session_epoch(node);
            let max_partition_bytes = self.cfg.max_partition_fetch_bytes;
            let mut by_id = self.take_share_ids();
            let mut topics = self.take_share_topics();
            let Some(tps) = self.leader_plan.get(&node) else {
                self.park_share_ids(by_id);
                self.park_share_topics(topics);
                continue;
            };
            if let Err(error) = refill_share_fetch(
                &mut by_id,
                &mut topics,
                tps,
                &self.topic_ids,
                max_partition_bytes,
            ) {
                self.park_share_ids(by_id);
                self.park_share_topics(topics);
                self.keep_prepared(prepared);
                return Err(error);
            }
            let max_records =
                i32::try_from(self.cfg.max_poll_records.unwrap_or(16).max(1)).unwrap_or(i32::MAX);
            prepared.push(PreparedShareFetch {
                node,
                version,
                by_id,
                epoch,
                max_wait_ms,
                min_bytes: self.cfg.min_bytes,
                max_bytes: self.cfg.max_bytes,
                max_records,
                acquire_mode: self.acquire_mode as i8,
                topics,
            });
        }
        let parked = self
            .inflight_share_fetches
            .as_ref()
            .is_some_and(|set| !set.is_empty());
        let mut ready = self.take_share_ready();
        let fetched = if prepared.len() > 1 || parked {
            let fetched = self
                .collect_share_fetches(&mut prepared, &mut ready, deadline)
                .await;
            self.keep_prepared(prepared);
            fetched
        } else if let Some(job) = prepared.pop() {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                self.park_share_ids(job.by_id);
                self.park_share_topics(job.topics);
                self.keep_prepared(prepared);
                self.keep_share_ready(ready);
                return Err(Error::Timeout);
            }
            let node = job.node;
            let Some(mut peer) = self.share_conns.remove(&node) else {
                self.park_share_ids(job.by_id);
                self.park_share_topics(job.topics);
                self.keep_prepared(prepared);
                self.keep_share_ready(ready);
                return Err(Error::protocol("missing share connection"));
            };
            let body = peer
                .conn
                .roundtrip(
                    SHARE_FETCH,
                    job.version,
                    |buf| encode_prepared_share_fetch(buf, &job, &self.group_id, &self.member_id),
                    timeout,
                )
                .await;
            let _ = self.share_conns.insert(node, peer);
            self.park_share_topics(job.topics);
            ready.push(ReadyShareFetch {
                node,
                version: job.version,
                by_id: job.by_id,
                body,
            });
            self.keep_prepared(prepared);
            Ok(())
        } else if Instant::now() >= deadline {
            self.keep_prepared(prepared);
            self.keep_share_ready(ready);
            return Err(Error::Timeout);
        } else {
            self.keep_prepared(prepared);
            Ok(())
        };
        if let Err(error) = fetched {
            self.keep_share_ready(ready);
            return Err(error);
        }
        for ReadyShareFetch {
            node,
            version,
            by_id,
            body,
        } in ready.drain(..)
        {
            let mut body = match body {
                Ok(body) => body,
                Err(error) => {
                    self.reset_node_session(node);
                    self.park_share_ids(by_id);
                    if matches!(error, Error::Timeout) && !self.pending_delivery.is_empty() {
                        return Ok(self.drain_pending_delivery());
                    }
                    return Err(error);
                }
            };
            let ((fetched, endpoints, _, _, lock_timeout, error_code), decoded_bytes) =
                match decode_share_fetch_response_with_budget(&mut body, version, remaining_bytes) {
                    Ok(reply) => reply,
                    Err(error) => {
                        self.reset_node_session(node);
                        return Err(error);
                    }
                };
            self.consumer.apply_node_endpoints(&endpoints);
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
            let mut incoming = take_record_buf(&mut self.spare_incoming);
            self.fetch_cursors.clear();
            let mut spilled_offsets: Vec<(i32, HashSet<i64>)> = Vec::new();
            for topic in fetched {
                let requested = by_id.get(&topic.topic_id).ok_or_else(|| {
                    Error::protocol("ShareFetch returned an unrequested topic id")
                })?;
                let topic_name = self.fetched_topic_name(topic.topic_id)?;
                let name = topic_name.as_str();
                for part in topic.partitions {
                    if !requested.contains(&part.partition) {
                        return Err(Error::protocol(
                            "ShareFetch returned an unrequested partition",
                        ));
                    }
                    if part.error_code != 0 {
                        let leader_moved = share_leader_moved(part.error_code);
                        let patched = leader_moved
                            && !name.is_empty()
                            && self.consumer.apply_share_leader(
                                name,
                                part.partition,
                                part.current_leader_id,
                                part.current_leader_epoch,
                            );
                        if let Some(patch) = share_leader_patch(
                            patched,
                            name,
                            part.partition,
                            part.error_code,
                            part.current_leader_id,
                            part.current_leader_epoch,
                        ) {
                            remember_share_leader_patch(&mut self.leader_patches, patch);
                        } else if share_ack_ownership_lost(part.error_code) {
                            self.consumer.invalidate_topic(name);
                            self.leader_patches.retain(|patch| {
                                patch.topic != name || patch.partition != part.partition
                            });
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
                    let mut range_lookup = acquired::AcquisitionRanges::new(&acquired);
                    for batch in part.records {
                        let timestamp_type = batch.timestamp_type();
                        let log_append_timestamp = (timestamp_type == TimestampType::LogAppendTime)
                            .then_some(batch.max_timestamp());
                        let leader_epoch = (batch.partition_leader_epoch >= 0)
                            .then_some(batch.partition_leader_epoch);
                        for record in batch.records {
                            let Some(delivery_count) = range_lookup.delivery_count(record.offset)
                            else {
                                continue;
                            };
                            if !note_share_offset(
                                &mut self.fetch_cursors,
                                &mut spilled_offsets,
                                part.partition,
                                record.offset,
                            ) {
                                return Err(Error::protocol("duplicate ShareFetch record offset"));
                            }
                            if let Some(previous) = acquisition_at(
                                &self.acquisitions,
                                &topic_name,
                                part.partition,
                                record.offset,
                            ) {
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
                                topic: topic_name.clone(),
                                partition: part.partition,
                                offset: record.offset,
                                timestamp: log_append_timestamp.unwrap_or(record.timestamp),
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
                    acquisition_at(
                        &self.acquisitions,
                        &record.topic,
                        record.partition,
                        record.offset,
                    )
                    .is_none()
                })
                .map(|record| acquisition_storage_bytes(&record.topic))
                .fold(0usize, usize::saturating_add);
            let retained_bytes = incoming_bytes.saturating_add(new_metadata_bytes);
            remaining_bytes = match remaining_bytes.checked_sub(retained_bytes.max(decoded_bytes)) {
                Some(remaining) => remaining,
                None => {
                    self.reset_node_session(node);
                    park_record_buf(&mut self.spare_incoming, incoming);
                    return Err(Error::protocol(
                        "ShareFetch retained-record budget exceeded",
                    ));
                }
            };
            for record in incoming.drain(..) {
                remember_acquisition(
                    &mut self.acquisitions,
                    &record.topic,
                    record.partition,
                    record.offset,
                    Acquisition {
                        node,
                        delivery_count: record.delivery_count,
                        expires_at: expiry,
                    },
                );
                self.pending_delivery.push(record);
            }
            park_record_buf(&mut self.spare_incoming, incoming);
            self.park_share_ids(by_id);
        }
        self.keep_share_ready(ready);
        Ok(self.drain_pending_delivery())
    }

    /// Fetch with a one-shot `fetch.max.wait.ms` (Java `poll(Duration)`).
    ///
    /// A cancelled one-shot future leaves [`ConsumerConfig::max_wait_ms`]
    /// intact. Not subscribed is the same Java `IllegalStateException` as
    /// [`Self::poll`].
    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<ShareRecords> {
        self.poll_with_wait(crate::consumer::duration_millis_i32(timeout))
            .await
    }

    /// Leave the share group and drop the subscription (Java `unsubscribe`).
    ///
    /// Heartbeats stop and the assignment is cleared. [`Self::subscribe`] joins
    /// again with a new topic list. [`Self::leave`] after this is a no-op.
    pub async fn unsubscribe(&mut self) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        if self.member_id.is_empty() {
            self.topic_match = None;
            self.topics.clear();
            self.publish_hb_topics();
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
        let deadline = Instant::now() + self.cfg.request_timeout;
        let close = self.close_share_session(deadline).await;
        let leave = self.leave_coordinator(deadline).await;
        self.assigned.clear();
        self.topics.clear();
        self.publish_hb_topics();
        self.topic_ids.clear();
        self.share_epochs.clear();
        self.member_id.clear();
        self.hb_member_id.lock().clear();
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
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        self.topic_match = Some(Arc::new(matches));
        let topics = self.matching_topic_names_within(deadline).await?;
        self.last_match_refresh = Instant::now();
        self.apply_topics_within(topics, deadline).await
    }

    async fn apply_topics(&mut self, topics: Vec<String>) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        self.apply_topics_within(topics, deadline).await
    }

    async fn apply_topics_within(&mut self, topics: Vec<String>, deadline: Instant) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        if topics == self.topics && !self.member_id.is_empty() {
            return Ok(());
        }
        let rejoining = !self.member_id.is_empty();
        if rejoining {
            self.close_share_session(deadline).await?;
            self.assigned.clear();
            self.topic_ids.clear();
            self.share_epochs.clear();
        }
        self.topics = topics;
        self.publish_hb_topics();
        if rejoining {
            self.heartbeat_join_within(deadline).await?;
            return Ok(());
        }
        self.member_id = Uuid::random_uuid().to_string();
        *self.hb_member_id.lock() = self.member_id.clone();
        let (hb_stop, hb_rx) = watch::channel(false);
        self.hb_stop = hb_stop;
        self.heartbeat_join_within(deadline).await?;
        self.spawn_heartbeat(hb_rx);
        Ok(())
    }

    async fn matching_topic_names_within(&mut self, deadline: Instant) -> Result<Vec<String>> {
        let Some(pred) = self.topic_match.clone() else {
            return Ok(self.topics.clone());
        };
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let infos = self.consumer.list_topics_timeout(timeout).await?;
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
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        let topics = self.matching_topic_names_within(deadline).await?;
        self.last_match_refresh = Instant::now();
        self.apply_topics_within(topics, deadline).await
    }

    fn fetched_topic_name(&self, id: [u8; 16]) -> Result<crate::consumer::TopicName> {
        topic_name_for_fetch(&self.topic_ids, id)
    }

    fn reclaim_ack_types(&mut self, topics: Vec<crate::protocol::share::ShareAckTopic>) {
        for topic in topics {
            for (_, batches) in topic.partitions {
                for batch in batches {
                    if batch.types.capacity() > 0 {
                        self.spare_ack_types.push(batch.types);
                    }
                }
            }
        }
    }

    fn name_for_topic_id(&self, id: [u8; 16]) -> Result<String> {
        self.topic_name_ref(id).map(str::to_owned)
    }

    fn topic_name_ref(&self, id: [u8; 16]) -> Result<&str> {
        share_topic_by_id(&self.topic_ids, id).map(|name| name.as_ref())
    }

    async fn send_acknowledgements(&mut self, recs: &[ShareRecord], ack: i8) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        self.adopt_share_member_id();
        if recs.is_empty() {
            return Ok(());
        }
        if self.share_epochs.is_empty() {
            return Err(reject_java_acknowledge_before_poll());
        }
        self.refresh_share_topic_ids();
        self.prune_expired_acquisitions();
        for record in recs {
            let acquired = acquisition_at(
                &self.acquisitions,
                &record.topic,
                record.partition,
                record.offset,
            );
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
        let mut partitions = acknowledgement_batches_with(
            recs,
            ack,
            &mut self.ack_offsets,
            &mut self.ack_pairs,
            &mut self.ack_topic,
            &mut self.spare_ack_types,
        );
        let deadline = Instant::now() + self.cfg.request_timeout;
        let mut attempt = 0u32;
        loop {
            match self
                .acknowledge_leaders(&mut partitions, ack, deadline)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) if share_ack_retriable(&error) && !partitions.is_empty() => {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    self.consumer.sleep_retry_backoff(attempt, deadline).await?;
                    attempt = attempt.saturating_add(1);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn acknowledge_single_leader(
        &mut self,
        node: i32,
        partitions: &mut Vec<AckPartition>,
        ack: i8,
        deadline: Instant,
    ) -> Result<()> {
        if deadline.saturating_duration_since(Instant::now()).is_zero() {
            return Err(Error::Timeout);
        }
        let Some((topic_name, partition, _)) = partitions.first() else {
            return Ok(());
        };
        let topic_name = Arc::clone(topic_name);
        let partition = *partition;
        let version = self
            .node_share_version_until(node, SHARE_ACKNOWLEDGE, deadline)
            .await?;
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
        let topic_id = self
            .topic_ids
            .get(topic_name.as_ref())
            .copied()
            .filter(|id| *id != [0; 16])
            .ok_or_else(|| Error::protocol(format!("missing share topic id for {topic_name}")))?;
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let Some(mut peer) = self.share_conns.remove(&node) else {
            self.reset_node_session(node);
            return Err(Error::protocol("missing share connection"));
        };
        let mut encode_failed = false;
        let sent = peer
            .conn
            .roundtrip(
                SHARE_ACKNOWLEDGE,
                version,
                |buf| {
                    let batches = partitions
                        .first()
                        .map(|part| part.2.as_slice())
                        .unwrap_or(&[]);
                    encode_one_share_acknowledge(
                        buf,
                        version,
                        &self.group_id,
                        &self.member_id,
                        epoch,
                        &topic_id,
                        partition,
                        batches,
                    )
                    .inspect_err(|_| encode_failed = true)
                },
                timeout,
            )
            .await;
        let _ = self.share_conns.insert(node, peer);
        if encode_failed {
            return match sent {
                Err(error) => Err(error),
                Ok(_) => Err(Error::protocol("share acknowledge encode")),
            };
        }
        let mut body = match sent {
            Ok(body) => body,
            Err(error) => {
                self.reset_node_session(node);
                return Err(error);
            }
        };
        let (code, responses, endpoints, _, _, lock_timeout) =
            match decode_share_acknowledge_topics_response_with_lock_timeout(&mut body, version) {
                Ok(reply) => reply,
                Err(error) => {
                    self.reset_node_session(node);
                    return Err(error);
                }
            };
        self.consumer.apply_node_endpoints(&endpoints);
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
        let response = responses.first();
        let part = response.and_then(|topic| topic.partitions.first());
        if response.is_none_or(|topic| topic.topic_id != topic_id)
            || part.is_none_or(|part| part.partition != partition)
            || responses.len() != 1
            || response.is_some_and(|topic| topic.partitions.len() != 1)
        {
            return Err(Error::protocol(
                "unrequested or duplicate ShareAcknowledge partition response",
            ));
        }
        let Some(part) = part else {
            return Err(Error::protocol(
                "ShareAcknowledge omitted a requested partition outcome",
            ));
        };
        self.advance_node_epoch(node);
        if version >= 2 && lock_timeout > 0 {
            self.acquisition_lock_timeout_ms = Some(lock_timeout);
        }
        let name = self.name_for_topic_id(topic_id)?;
        if part.error_code != 0 && !name.is_empty() && share_leader_moved(part.error_code) {
            let patched = self.consumer.apply_share_leader(
                &name,
                part.partition,
                part.current_leader_id,
                part.current_leader_epoch,
            );
            if let Some(patch) = share_leader_patch(
                patched,
                &name,
                part.partition,
                part.error_code,
                part.current_leader_id,
                part.current_leader_epoch,
            ) {
                remember_share_leader_patch(&mut self.leader_patches, patch);
            }
        }
        let sent_batches = partitions
            .first()
            .map(|item| item.2.as_slice())
            .unwrap_or(&[]);
        if part.error_code == 0 {
            let count = self
                .acquisitions
                .get(name.as_str())
                .map(|parts| count_covered_acquisitions(parts, part.partition, sent_batches))
                .unwrap_or(0);
            if ack == ACK_RENEW {
                if lock_timeout <= 0 {
                    return Err(Error::protocol(
                        "non-positive Renew acquisition lock timeout",
                    ));
                }
                let expiry = Instant::now().checked_add(Duration::from_millis(
                    u64::try_from(lock_timeout).unwrap_or(0),
                ));
                if let Some(parts) = self.acquisitions.get_mut(name.as_str()) {
                    renew_covered_acquisitions(parts, part.partition, sent_batches, expiry);
                }
            } else {
                let empty = if let Some(parts) = self.acquisitions.get_mut(name.as_str()) {
                    drop_covered_acquisitions(parts, part.partition, sent_batches);
                    parts.is_empty()
                } else {
                    false
                };
                if empty {
                    let _ = self.acquisitions.remove(name.as_str());
                }
                self.records_acknowledged = self
                    .records_acknowledged
                    .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
            }
            let batches = partitions
                .first_mut()
                .map(|item| std::mem::take(&mut item.2))
                .unwrap_or_default();
            for batch in batches {
                if batch.types.capacity() > 0 {
                    self.spare_ack_types.push(batch.types);
                }
            }
            partitions.clear();
            Ok(())
        } else {
            if part.error_code == error::INVALID_RECORD_STATE
                || share_ack_ownership_lost(part.error_code)
            {
                let empty = if let Some(parts) = self.acquisitions.get_mut(name.as_str()) {
                    drop_covered_acquisitions(parts, part.partition, sent_batches);
                    parts.is_empty()
                } else {
                    false
                };
                if empty {
                    let _ = self.acquisitions.remove(name.as_str());
                }
            }
            let error = Error::broker(
                part.error_code,
                format!("ShareAcknowledge {name}-{}", part.partition),
            );
            if part.error_code == error::SHARE_SESSION_NOT_FOUND
                || part.error_code == error::INVALID_SHARE_SESSION_EPOCH
            {
                self.reset_node_session(node);
            }
            Err(error)
        }
    }

    async fn acknowledge_leaders(
        &mut self,
        partitions: &mut Vec<AckPartition>,
        ack: i8,
        deadline: Instant,
    ) -> Result<()> {
        if let Some(node) = single_leader_node(partitions, |topic, partition, first, last| {
            acquisition_node_for_range(&self.acquisitions, topic, partition, first, last)
        }) {
            return self
                .acknowledge_single_leader(node, partitions, ack, deadline)
                .await;
        }
        let nodes = {
            let grouped = group_acks_by_acquisition_node(
                |topic, partition, offset| {
                    acquisition_node(&self.acquisitions, topic, partition, offset)
                },
                |topic, partition, first, last| {
                    acquisition_node_for_range(&self.acquisitions, topic, partition, first, last)
                },
                partitions,
            )?;
            // A split across brokers replaces the retry list with the batches
            // that are sent. One broker already matches that list, so those
            // batches move into the request without a second copy.
            let one_leader = grouped.len() == 1;
            let nodes: Vec<_> = grouped.into_iter().collect();
            if !one_leader {
                *partitions = nodes
                    .iter()
                    .flat_map(|(_, parts)| parts.iter().cloned())
                    .collect();
            }
            nodes
        };
        let mut first_error = None;
        for (node, node_tps) in nodes {
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                // Brokers already acknowledged stay acknowledged. The ones not
                // sent remain in `partitions` for the caller.
                return Err(Error::Timeout);
            }
            let version = self
                .node_share_version_until(node, SHARE_ACKNOWLEDGE, deadline)
                .await?;
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
            for (topic, part, batches) in node_tps {
                let topic_id = self
                    .topic_ids
                    .get(topic.as_ref())
                    .copied()
                    .filter(|id| *id != [0; 16])
                    .ok_or_else(|| {
                        Error::protocol(format!("missing share topic id for {topic}"))
                    })?;
                match topics.iter_mut().find(|t| t.topic_id == topic_id) {
                    Some(slot) => slot.partitions.push((part, batches)),
                    None => topics.push(ShareAckTopic {
                        topic_id,
                        partitions: vec![(part, batches)],
                    }),
                }
            }
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let Some(mut peer) = self.share_conns.remove(&node) else {
                self.reset_node_session(node);
                return Err(Error::protocol("missing share connection"));
            };
            let mut encode_failed = false;
            let sent = peer
                .conn
                .roundtrip(
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
                        .inspect_err(|_| encode_failed = true)
                    },
                    timeout,
                )
                .await;
            let _ = self.share_conns.insert(node, peer);
            if encode_failed {
                return match sent {
                    Err(error) => Err(error),
                    Ok(_) => Err(Error::protocol("share acknowledge encode")),
                };
            }
            let mut body = match sent {
                Ok(body) => body,
                Err(error) => {
                    self.reset_node_session(node);
                    return Err(error);
                }
            };
            let (code, responses, endpoints, _, _, lock_timeout) =
                match decode_share_acknowledge_topics_response_with_lock_timeout(&mut body, version)
                {
                    Ok(reply) => reply,
                    Err(error) => {
                        self.reset_node_session(node);
                        return Err(error);
                    }
                };
            self.consumer.apply_node_endpoints(&endpoints);
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
                let name = self.name_for_topic_id(topic.topic_id)?;
                for part in topic.partitions {
                    if part.error_code != 0
                        && !name.is_empty()
                        && share_leader_moved(part.error_code)
                    {
                        let patched = self.consumer.apply_share_leader(
                            &name,
                            part.partition,
                            part.current_leader_id,
                            part.current_leader_epoch,
                        );
                        if let Some(patch) = share_leader_patch(
                            patched,
                            &name,
                            part.partition,
                            part.error_code,
                            part.current_leader_id,
                            part.current_leader_epoch,
                        ) {
                            remember_share_leader_patch(&mut self.leader_patches, patch);
                        }
                    }
                    let sent = sent_ack_batches_by_id(&topics, topic.topic_id, part.partition);
                    if part.error_code == 0 {
                        let count = self
                            .acquisitions
                            .get(name.as_str())
                            .map(|parts| count_covered_acquisitions(parts, part.partition, sent))
                            .unwrap_or(0);
                        if ack == ACK_RENEW {
                            if lock_timeout <= 0 {
                                return Err(Error::protocol(
                                    "non-positive Renew acquisition lock timeout",
                                ));
                            }
                            let expiry = Instant::now().checked_add(Duration::from_millis(
                                u64::try_from(lock_timeout).unwrap_or(0),
                            ));
                            if let Some(parts) = self.acquisitions.get_mut(name.as_str()) {
                                renew_covered_acquisitions(parts, part.partition, sent, expiry);
                            }
                        } else {
                            let empty =
                                if let Some(parts) = self.acquisitions.get_mut(name.as_str()) {
                                    drop_covered_acquisitions(parts, part.partition, sent);
                                    parts.is_empty()
                                } else {
                                    false
                                };
                            if empty {
                                let _ = self.acquisitions.remove(name.as_str());
                            }
                            self.records_acknowledged = self
                                .records_acknowledged
                                .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
                        }
                        drop_sent_ack_batches(partitions, &name, part.partition, sent);
                    } else {
                        if part.error_code == error::INVALID_RECORD_STATE
                            || share_ack_ownership_lost(part.error_code)
                        {
                            let empty =
                                if let Some(parts) = self.acquisitions.get_mut(name.as_str()) {
                                    drop_covered_acquisitions(parts, part.partition, sent);
                                    parts.is_empty()
                                } else {
                                    false
                                };
                            if empty {
                                let _ = self.acquisitions.remove(name.as_str());
                            }
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
            self.reclaim_ack_types(topics);
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn close_share_session(&mut self, deadline: Instant) -> Result<()> {
        self.abort_inflight_share_fetches();
        let open: Vec<_> = self
            .share_epochs
            .iter()
            .filter(|(_, epoch)| **epoch > 0)
            .map(|(node, _)| *node)
            .collect();
        let mut last = Ok(());
        for node in open {
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                last = Err(Error::Timeout);
                break;
            }
            let version = match self
                .node_share_version_until(node, SHARE_ACKNOWLEDGE, deadline)
                .await
            {
                Ok(version) => version,
                Err(error) => {
                    last = Err(error);
                    continue;
                }
            };
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                last = Err(Error::Timeout);
                break;
            }
            let Some(mut peer) = self.share_conns.remove(&node) else {
                last = Err(Error::protocol("missing share connection"));
                continue;
            };
            let mut encode_failed = false;
            let sent = peer
                .conn
                .roundtrip(
                    SHARE_ACKNOWLEDGE,
                    version,
                    |buf| {
                        encode_share_acknowledge_request(
                            buf,
                            version,
                            &self.group_id,
                            &self.member_id,
                            ShareRequestMetadata::FINAL_EPOCH,
                            [0; 16],
                            &[],
                        )
                        .inspect_err(|_| encode_failed = true)
                    },
                    timeout,
                )
                .await;
            let _ = self.share_conns.insert(node, peer);
            if encode_failed {
                last = match sent {
                    Err(error) => Err(error),
                    Ok(_) => Err(Error::protocol("share acknowledge encode")),
                };
                continue;
            }
            let code = match sent {
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
    pub async fn leave(self) -> Result<()> {
        let deadline = Instant::now() + self.cfg.request_timeout;
        self.leave_within(deadline).await
    }

    async fn leave_within(mut self, deadline: Instant) -> Result<()> {
        if self.member_id.is_empty() {
            self.hb_stop.send(true).unwrap_or(());
            *self.hb_deadline.lock() = None;
            self.consumer.close_interceptors();
            return Ok(());
        }
        self.hb_stop.send(true).unwrap_or(());
        *self.hb_deadline.lock() = None;
        let close = self.close_share_session(deadline).await;
        let out = self.leave_coordinator(deadline).await;
        self.consumer.close_interceptors();
        close.and(out)
    }

    async fn leave_coordinator(&mut self, deadline: Instant) -> Result<()> {
        self.adopt_share_member_id();
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH,
            rack_id: self.cfg.rack.clone(),
            subscribed_topic_names: None,
        };
        let (version, body) = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            SHARE_GROUP_HEARTBEAT,
            |version, buf| encode_share_group_heartbeat_request(buf, version, &req),
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
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::protocol("close deadline overflow"))?;
        self.leave_within(deadline).await
    }

    fn spawn_heartbeat(&self, mut stop: watch::Receiver<bool>) {
        let group_id = self.group_id.clone();
        let hb_member_id = self.hb_member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_epoch = self.hb_epoch.clone();
        let hb_topics = self.hb_topics.clone();
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
            let mut member_id = hb_member_id.lock().clone();
            let mut heartbeat = ShareGroupHeartbeatRequest {
                group_id: group_id.clone(),
                member_id: member_id.clone(),
                member_epoch: 0,
                rack_id: cfg.rack.clone(),
                subscribed_topic_names: None,
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
                        let deadline = Instant::now() + cfg.request_timeout;
                        if conn.is_none() {
                            conn = crate::group::discover_coord_within(
                                &cfg,
                                &group_id,
                                COORDINATOR_GROUP,
                                deadline,
                            )
                            .await
                            .ok();
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
                            hb_err.store(error::UNSUPPORTED_VERSION, Ordering::SeqCst);
                            let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                            next_hb_deadline = Instant::now() + retry_delay;
                            if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                            continue;
                        };
                        let timeout = deadline.saturating_duration_since(Instant::now());
                        if timeout.is_zero() {
                            let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                            next_hb_deadline = Instant::now() + retry_delay;
                            if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) { break; }
                            continue;
                        }
                        crate::group::copy_member_id(&mut member_id, &hb_member_id.lock());
                        crate::group::copy_member_id(&mut heartbeat.member_id, &member_id);
                        heartbeat.member_epoch = epoch;
                        if epoch == ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH {
                            reuse_share_subscription(&mut heartbeat, &hb_topics.lock());
                        } else {
                            heartbeat.subscribed_topic_names = None;
                        }
                        let res = c
                            .roundtrip(
                                SHARE_GROUP_HEARTBEAT,
                                version,
                                |buf| {
                                    encode_share_group_heartbeat_request(buf, version, &heartbeat)
                                },
                                timeout,
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
                                        if let Some(id) = resp.member_id.as_deref() {
                                            if !id.is_empty() && id != member_id {
                                                publish_member_id(
                                                    &mut member_id,
                                                    &mut hb_member_id.lock(),
                                                    id,
                                                );
                                                crate::group::copy_member_id(
                                                    &mut heartbeat.member_id,
                                                    &member_id,
                                                );
                                            }
                                        }
                                        hb_epoch.store(
                                            share_heartbeat_epoch(
                                                resp.error_code,
                                                resp.member_epoch,
                                                hb_epoch.load(Ordering::SeqCst),
                                            ),
                                            Ordering::SeqCst,
                                        );
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

fn reuse_share_subscription(req: &mut ShareGroupHeartbeatRequest, topics: &[String]) {
    let same = req
        .subscribed_topic_names
        .as_ref()
        .is_some_and(|names| names.as_slice() == topics);
    if !same {
        req.subscribed_topic_names = Some(topics.to_vec());
    }
}

/// Copy `id` into the local member and the heartbeat task's copy.
///
/// An empty id is ignored. An unchanged id keeps both buffers.
fn publish_member_id(local: &mut String, shared: &mut String, id: &str) {
    if id.is_empty() {
        return;
    }
    crate::group::copy_member_id(local, id);
    crate::group::copy_member_id(shared, local);
}

#[cfg(test)]
fn share_heartbeat_subscription(epoch: i32, topics: &[String]) -> Option<Vec<String>> {
    if epoch == ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH {
        Some(topics.to_vec())
    } else {
        None
    }
}

fn share_membership_lost(code: i16) -> bool {
    matches!(code, error::FENCED_MEMBER_EPOCH | error::UNKNOWN_MEMBER_ID)
}

fn share_heartbeat_epoch(code: i16, response_epoch: i32, current: i32) -> i32 {
    if share_membership_lost(code) {
        ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
    } else if code == error::STALE_MEMBER_EPOCH {
        // The coordinator's epoch, including 0. A negative response epoch
        // cannot be applied, so the next heartbeat rejoins.
        if response_epoch >= 0 {
            response_epoch
        } else {
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        }
    } else if response_epoch > 0 {
        response_epoch
    } else {
        current
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
    std::mem::size_of::<crate::consumer::TopicName>()
        .saturating_add(std::mem::size_of::<i32>())
        .saturating_add(std::mem::size_of::<i64>())
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

/// One KIP-951 CurrentLeader learned from a share fetch or acknowledge.
struct ShareLeaderPatch {
    topic: String,
    partition: i32,
    leader_id: i32,
    leader_epoch: i32,
}

/// Broker errors that can name the partition's new leader.
fn share_leader_moved(code: i16) -> bool {
    share_ack_ownership_lost(code) || code == error::LEADER_NOT_AVAILABLE
}

fn share_leader_patch(
    applied: bool,
    topic: &str,
    partition: i32,
    error_code: i16,
    leader_id: i32,
    leader_epoch: i32,
) -> Option<ShareLeaderPatch> {
    if applied && share_leader_moved(error_code) {
        Some(ShareLeaderPatch {
            topic: topic.to_owned(),
            partition,
            leader_id,
            leader_epoch,
        })
    } else {
        None
    }
}

fn remember_share_leader_patch(patches: &mut Vec<ShareLeaderPatch>, patch: ShareLeaderPatch) {
    if let Some(existing) = patches
        .iter_mut()
        .find(|saved| saved.topic == patch.topic && saved.partition == patch.partition)
    {
        if patch.leader_epoch >= existing.leader_epoch {
            *existing = patch;
        }
    } else {
        patches.push(patch);
    }
}

fn reapply_share_leader_patches(consumer: &mut Consumer, patches: &[ShareLeaderPatch]) {
    for patch in patches {
        let _applied = consumer.apply_share_leader(
            &patch.topic,
            patch.partition,
            patch.leader_id,
            patch.leader_epoch,
        );
    }
}

#[cfg(test)]
fn reapply_share_leader_patches_on(
    cluster: &mut crate::cluster::Cluster,
    patches: &[ShareLeaderPatch],
) {
    for patch in patches {
        let _applied = cluster.apply_current_leader(
            &patch.topic,
            patch.partition,
            patch.leader_id,
            patch.leader_epoch,
        );
    }
}

#[cfg(test)]
fn share_topic_name<'a>(
    names: impl IntoIterator<Item = (&'a str, &'a [u8; 16])>,
    id: [u8; 16],
) -> Result<&'a str> {
    share_named(names, id, |name| name.is_empty())
}

/// The subscribed name for one topic id.
///
/// An id that matches nothing is not guessed from the first subscription.
/// An id that matches two names is ambiguous and must not label records.
fn share_topic_by_id(topic_ids: &HashMap<Arc<str>, [u8; 16]>, id: [u8; 16]) -> Result<&Arc<str>> {
    share_named(topic_ids.iter(), id, |name| name.is_empty())
}

/// Topic stored for a fetched share record.
///
/// A short name is copied inline. A long name clones the assignment's allocation.
fn topic_name_for_fetch(
    topic_ids: &HashMap<Arc<str>, [u8; 16]>,
    id: [u8; 16],
) -> Result<crate::consumer::TopicName> {
    Ok(crate::consumer::TopicName::shared(share_topic_by_id(
        topic_ids, id,
    )?))
}

fn share_named<'a, N>(
    names: impl IntoIterator<Item = (N, &'a [u8; 16])>,
    id: [u8; 16],
    empty: impl Fn(&N) -> bool,
) -> Result<N> {
    let mut found = None;
    for (name, stored) in names {
        if *stored != id {
            continue;
        }
        if found.is_some() {
            return Err(Error::protocol("share topic id is assigned to two names"));
        }
        found = Some(name);
    }
    match found {
        Some(name) if !empty(&name) => Ok(name),
        _ => Err(Error::protocol("share topic id has no subscribed name")),
    }
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

/// True when a refresh would leave every stored id unchanged.
///
/// A steady poll then skips cloning the topic-name list.
fn share_topic_ids_current<T: AsRef<str>>(
    stored_map: &HashMap<Arc<str>, [u8; 16]>,
    assigned: &[(T, i32)],
    metadata: &HashMap<String, [u8; 16]>,
) -> bool {
    for (name, stored) in stored_map {
        let next = metadata.get(name.as_ref()).copied();
        if let Some(id) = refreshed_share_topic_id(Some(*stored), next) {
            if id != *stored {
                return false;
            }
        }
    }
    for (topic, _) in assigned {
        let topic = topic.as_ref();
        if stored_map.contains_key(topic) {
            continue;
        }
        if refreshed_share_topic_id(None, metadata.get(topic).copied()).is_some() {
            return false;
        }
    }
    true
}

/// Metadata id when it is present, otherwise the id stored at assignment.
fn refreshed_share_topic_id(
    stored: Option<[u8; 16]>,
    metadata: Option<[u8; 16]>,
) -> Option<[u8; 16]> {
    match metadata.filter(|id| *id != [0; 16]) {
        Some(id) => Some(id),
        None => stored.filter(|id| *id != [0; 16]),
    }
}

/// A new non-zero metadata id replaces the id captured with the assignment.
fn share_topic_recreated(stored: [u8; 16], metadata: Option<[u8; 16]>) -> bool {
    metadata.is_some_and(|id| id != [0; 16] && id != stored)
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

/// One topic name is shared by every partition acknowledged for that topic.
type AckPartition = (Arc<str>, i32, Vec<AcknowledgementBatch>);

fn sent_ack_batches_by_id(
    topics: &[ShareAckTopic],
    topic_id: [u8; 16],
    partition: i32,
) -> &[AcknowledgementBatch] {
    topics
        .iter()
        .find(|topic| topic.topic_id == topic_id)
        .and_then(|topic| topic.partitions.iter().find(|(part, _)| *part == partition))
        .map(|(_, batches)| batches.as_slice())
        .unwrap_or(&[])
}

fn drop_sent_ack_batches(
    partitions: &mut Vec<AckPartition>,
    topic: &str,
    partition: i32,
    sent: &[AcknowledgementBatch],
) {
    let mut kept = Vec::new();
    for (name, part, batches) in partitions.drain(..) {
        if name.as_ref() != topic || part != partition {
            kept.push((name, part, batches));
            continue;
        }
        let remaining = batches
            .into_iter()
            .filter(|batch| !sent.contains(batch))
            .collect::<Vec<_>>();
        if !remaining.is_empty() {
            kept.push((name, part, remaining));
        }
    }
    *partitions = kept;
}

/// Route each acknowledgement to the broker that acquired those offsets.
///
/// A later metadata leader is the target of the next fetch. Locks already
/// taken stay on the session that delivered them, including when one
/// partition's offsets came from two brokers.
type AckRoute = BTreeMap<i32, Vec<AckPartition>>;

fn single_leader_node(
    partitions: &[AckPartition],
    range_node: impl Fn(&str, i32, i64, i64) -> Option<i32>,
) -> Option<i32> {
    if partitions.len() != 1 {
        return None;
    }
    let (topic, partition, batches) = partitions.first()?;
    if batches.len() != 1 {
        return None;
    }
    let batch = batches.first()?;
    if batch.types.len() != 1 {
        return None;
    }
    range_node(topic, *partition, batch.first_offset, batch.last_offset)
}

fn group_acks_by_acquisition_node(
    node_of: impl Fn(&str, i32, i64) -> Option<i32>,
    range_node: impl Fn(&str, i32, i64, i64) -> Option<i32>,
    partitions: &[AckPartition],
) -> Result<AckRoute> {
    let mut by_node: AckRoute = BTreeMap::new();
    let mut spare_types = Vec::new();
    for (topic, partition, batches) in partitions {
        let mut node_batches: BTreeMap<i32, Vec<AcknowledgementBatch>> = BTreeMap::new();
        let mut run_types = Vec::new();
        for batch in batches {
            let span = batch
                .last_offset
                .checked_sub(batch.first_offset)
                .and_then(|delta| delta.checked_add(1))
                .ok_or_else(|| Error::protocol("share acknowledgement range is too large"))?;
            if !(1..=1_000_000).contains(&span) {
                return Err(Error::protocol("share acknowledgement range is too large"));
            }
            let span = usize::try_from(span)
                .map_err(|_| Error::protocol("share acknowledgement range is too large"))?;
            let per_offset = if batch.types.len() == 1 {
                None
            } else if batch.types.len() == span {
                Some(batch.types.as_slice())
            } else {
                return Err(Error::protocol(
                    "share acknowledgement type count does not match the range",
                ));
            };
            // One type covers the whole range. Splits still walk each offset
            // for its broker, without storing that type once per offset.
            let single_type = if per_offset.is_none() {
                Some(batch.types.first().copied().ok_or_else(|| {
                    Error::protocol("share acknowledgement type count does not match the range")
                })?)
            } else {
                None
            };
            if let Some(kind) = single_type {
                if let Some(node) =
                    range_node(topic, *partition, batch.first_offset, batch.last_offset)
                {
                    node_batches
                        .entry(node)
                        .or_default()
                        .push(AcknowledgementBatch {
                            first_offset: batch.first_offset,
                            last_offset: batch.last_offset,
                            types: reused_one_ack_type(&mut spare_types, kind),
                        });
                    continue;
                }
            }
            let mut offset = batch.first_offset;
            let mut index = 0usize;
            let mut run_node = None;
            let mut run_first = offset;
            run_types.clear();
            while index < span {
                let node = node_of(topic, *partition, offset).ok_or_else(|| {
                    Error::broker(
                        error::INVALID_RECORD_STATE,
                        format!("stale share acknowledgement {topic}-{partition}@{offset}"),
                    )
                })?;
                let ack_type = match per_offset {
                    Some(types) => *types.get(index).ok_or_else(|| {
                        Error::protocol("share acknowledgement type count does not match the range")
                    })?,
                    None => single_type.ok_or_else(|| {
                        Error::protocol("share acknowledgement type count does not match the range")
                    })?,
                };
                if run_node.is_some_and(|current| current != node) {
                    if let Some(current) = run_node {
                        node_batches.entry(current).or_default().push(ack_run(
                            run_first,
                            offset.saturating_sub(1),
                            single_type,
                            &run_types,
                            &mut spare_types,
                        ));
                    }
                    run_first = offset;
                    run_types.clear();
                }
                run_node = Some(node);
                if single_type.is_none() {
                    run_types.push(ack_type);
                }
                if offset == i64::MAX {
                    break;
                }
                offset = offset.saturating_add(1);
                index += 1;
            }
            if let Some(current) = run_node {
                node_batches.entry(current).or_default().push(ack_run(
                    run_first,
                    batch.last_offset,
                    single_type,
                    &run_types,
                    &mut spare_types,
                ));
            }
        }
        for (node, batches) in node_batches {
            by_node
                .entry(node)
                .or_default()
                .push((Arc::clone(topic), *partition, batches));
        }
    }
    Ok(by_node)
}

fn ack_run(
    first: i64,
    last: i64,
    single_type: Option<i8>,
    types: &[i8],
    spare: &mut Vec<Vec<i8>>,
) -> AcknowledgementBatch {
    if let Some(kind) = single_type {
        return AcknowledgementBatch {
            first_offset: first,
            last_offset: last,
            types: reused_one_ack_type(spare, kind),
        };
    }
    finish_ack_run(first, last, types, spare)
}

fn finish_ack_run(
    first: i64,
    last: i64,
    types: &[i8],
    spare: &mut Vec<Vec<i8>>,
) -> AcknowledgementBatch {
    let collapsed = types
        .first()
        .is_some_and(|kind| types.iter().all(|item| item == kind));
    AcknowledgementBatch {
        first_offset: first,
        last_offset: last,
        types: if let Some(kind) = types.first().copied().filter(|_| collapsed) {
            reused_one_ack_type(spare, kind)
        } else {
            reused_ack_types(spare, types)
        },
    }
}

/// Collapse records into KIP-932 acknowledgement batches.
///
/// Contiguous offsets with the same type become one batch with a single
/// `AcknowledgeType` (applies to the whole range). Gaps start a new batch.
/// Partitions of one topic share that topic's name.
#[cfg(test)]
fn acknowledgement_batches(recs: &[ShareRecord], ack: i8) -> Vec<AckPartition> {
    let mut offsets = Vec::new();
    let mut pairs = Vec::new();
    let mut topic = None;
    let mut types = Vec::new();
    acknowledgement_batches_with(recs, ack, &mut offsets, &mut pairs, &mut topic, &mut types)
}

fn reused_one_ack_type(spare: &mut Vec<Vec<i8>>, kind: i8) -> Vec<i8> {
    let mut types = spare.pop().unwrap_or_default();
    types.clear();
    types.push(kind);
    types
}

fn reused_ack_types(spare: &mut Vec<Vec<i8>>, values: &[i8]) -> Vec<i8> {
    let mut types = spare.pop().unwrap_or_default();
    types.clear();
    types.extend_from_slice(values);
    types
}

fn share_topic_arc(topic: &crate::consumer::TopicName, cached: &mut Option<Arc<str>>) -> Arc<str> {
    if let crate::consumer::TopicName::Heap(shared) = topic {
        return Arc::clone(shared);
    }
    if let Some(name) = cached.as_ref() {
        if name.as_ref() == topic.as_str() {
            return Arc::clone(name);
        }
    }
    let shared = Arc::<str>::from(topic.as_str());
    *cached = Some(Arc::clone(&shared));
    shared
}

fn acknowledgement_batches_with(
    recs: &[ShareRecord],
    ack: i8,
    offsets: &mut Vec<i64>,
    pairs: &mut Vec<(i32, i64)>,
    topic_name: &mut Option<Arc<str>>,
    ack_types: &mut Vec<Vec<i8>>,
) -> Vec<AckPartition> {
    if let Some(first) = recs.first() {
        if recs
            .iter()
            .skip(1)
            .all(|rec| rec.topic == first.topic && rec.partition == first.partition)
        {
            offsets.clear();
            offsets.extend(recs.iter().map(|rec| rec.offset));
            let batches = collapse_ack_offsets(offsets, ack, ack_types);
            if batches.is_empty() {
                return Vec::new();
            }
            return vec![(
                share_topic_arc(&first.topic, topic_name),
                first.partition,
                batches,
            )];
        }
        if recs.iter().skip(1).all(|rec| rec.topic == first.topic) {
            pairs.clear();
            pairs.extend(recs.iter().map(|rec| (rec.partition, rec.offset)));
            pairs.sort_unstable();
            let name = share_topic_arc(&first.topic, topic_name);
            let mut out = Vec::new();
            let mut start = 0usize;
            while start < pairs.len() {
                let Some(partition) = pairs.get(start).map(|(partition, _)| *partition) else {
                    break;
                };
                let mut end = start;
                while pairs.get(end).is_some_and(|(next, _)| *next == partition) {
                    end = end.saturating_add(1);
                }
                offsets.clear();
                if let Some(group) = pairs.get(start..end) {
                    offsets.extend(group.iter().map(|(_, offset)| *offset));
                }
                let batches = collapse_ack_offsets(offsets, ack, ack_types);
                if !batches.is_empty() {
                    out.push((Arc::clone(&name), partition, batches));
                }
                start = end;
            }
            return out;
        }
    }
    let mut by_part: BTreeMap<crate::consumer::TopicName, BTreeMap<i32, Vec<i64>>> =
        BTreeMap::new();
    for rec in recs {
        if let Some(parts) = by_part.get_mut(&rec.topic) {
            parts.entry(rec.partition).or_default().push(rec.offset);
        } else {
            let mut parts = BTreeMap::new();
            let _ = parts.insert(rec.partition, vec![rec.offset]);
            let _ = by_part.insert(rec.topic.clone(), parts);
        }
    }
    let mut out = Vec::new();
    for (topic, parts) in by_part {
        let name = share_topic_arc(&topic, topic_name);
        for (partition, mut offs) in parts {
            let batches = collapse_ack_offsets(&mut offs, ack, ack_types);
            if !batches.is_empty() {
                out.push((Arc::clone(&name), partition, batches));
            }
        }
    }
    out
}

fn collapse_ack_offsets(
    offsets: &mut Vec<i64>,
    ack: i8,
    ack_types: &mut Vec<Vec<i8>>,
) -> Vec<AcknowledgementBatch> {
    offsets.sort_unstable();
    offsets.dedup();
    let mut batches = Vec::new();
    let mut range: Option<(i64, i64)> = None;
    for off in offsets.iter().copied() {
        range = match range {
            None => Some((off, off)),
            Some((start, end)) if off == end.saturating_add(1) => Some((start, off)),
            Some((start, end)) => {
                batches.push(AcknowledgementBatch {
                    first_offset: start,
                    last_offset: end,
                    types: reused_one_ack_type(ack_types, ack),
                });
                Some((off, off))
            }
        };
    }
    if let Some((start, end)) = range {
        batches.push(AcknowledgementBatch {
            first_offset: start,
            last_offset: end,
            types: reused_one_ack_type(ack_types, ack),
        });
    }
    batches
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn an_unchanged_share_identity_keeps_its_allocation() {
        let mut cached = Arc::<str>::from("member");
        let first = shared_text(&mut cached, "member");
        let second = shared_text(&mut cached, "member");
        assert!(Arc::ptr_eq(&first, &second));
        let rotated = shared_text(&mut cached, "rotated");
        assert!(!Arc::ptr_eq(&first, &rotated));
        assert_eq!(rotated.as_ref(), "rotated");
        assert!(Arc::ptr_eq(&rotated, &cached));
    }

    #[test]
    fn an_unchanged_share_subscription_keeps_its_buffer() {
        let topics = vec!["orders".to_string()];
        let mut req = crate::protocol::share::ShareGroupHeartbeatRequest {
            group_id: "group".to_string(),
            member_id: "member".to_string(),
            member_epoch: 1,
            rack_id: None,
            subscribed_topic_names: Some(topics.clone()),
        };
        super::reuse_share_subscription(&mut req, &topics);
        let ptr = req.subscribed_topic_names.as_ref().map(Vec::as_ptr);
        super::reuse_share_subscription(&mut req, &topics);
        assert_eq!(req.subscribed_topic_names.as_ref().map(Vec::as_ptr), ptr);
        let changed = ["payments".to_string()];
        super::reuse_share_subscription(&mut req, &changed);
        assert_eq!(
            req.subscribed_topic_names.as_deref(),
            Some(changed.as_slice())
        );
    }

    #[test]
    fn an_unchanged_share_member_id_keeps_both_buffers() {
        let mut local = String::from("member-1");
        let mut shared = String::from("member-1");
        let local_ptr = local.as_ptr();
        let shared_ptr = shared.as_ptr();
        super::publish_member_id(&mut local, &mut shared, "member-1");
        assert_eq!(local.as_ptr(), local_ptr);
        assert_eq!(shared.as_ptr(), shared_ptr);
        super::publish_member_id(&mut local, &mut shared, "");
        assert_eq!(local, "member-1");
        assert_eq!(shared, "member-1");
        super::publish_member_id(&mut local, &mut shared, "member-2");
        assert_eq!(local, "member-2");
        assert_eq!(shared, "member-2");
    }

    #[test]
    fn a_repeated_share_topic_keeps_the_loaded_name() {
        let orders: Arc<str> = Arc::from("topic-name-longer-than-twenty-three");
        let copy: Arc<str> = Arc::from("topic-name-longer-than-twenty-three");
        let mut cached = None;
        assert!(super::remember_share_leader_topic(&mut cached, &orders));
        assert_eq!(cached.as_ref().map(Arc::as_ptr), Some(Arc::as_ptr(&orders)));
        assert!(!super::remember_share_leader_topic(&mut cached, &orders));
        assert!(!super::remember_share_leader_topic(&mut cached, &copy));
        assert_eq!(cached.as_ref().map(Arc::as_ptr), Some(Arc::as_ptr(&orders)));
        let payments: Arc<str> = Arc::from("payments");
        assert!(super::remember_share_leader_topic(&mut cached, &payments));
        assert_eq!(
            cached.as_ref().map(Arc::as_ptr),
            Some(Arc::as_ptr(&payments))
        );
    }

    #[test]
    fn a_second_share_plan_reuses_topic_names() {
        let assigned = [("orders".to_string(), 0), ("orders".to_string(), 1)];
        let mut names = Vec::new();
        let topics = assigned.iter().map(|(topic, _)| topic.as_str());
        super::fill_unique_names(&mut names, topics);
        let ptr = names.first().map(|name| name.as_ptr());
        let topics = assigned.iter().map(|(topic, _)| topic.as_str());
        super::fill_unique_names(&mut names, topics);
        assert_eq!(names.as_slice(), ["orders".to_string()].as_slice());
        assert_eq!(names.first().map(|name| name.as_ptr()), ptr);

        let orders: Arc<str> = Arc::from("orders");
        let again: Arc<str> = Arc::from("orders");
        let mut plan = HashMap::new();
        super::clear_share_leader_plan(&mut plan);
        super::remember_share_leader(&mut plan, 2, &orders, 0);
        super::remember_share_leader(&mut plan, 2, &again, 1);
        super::prune_share_leader_plan(&mut plan);
        let key_ptr = plan
            .get(&2)
            .and_then(|topics| topics.get_key_value("orders"))
            .map(|(key, _)| Arc::as_ptr(key));
        assert_eq!(key_ptr, Some(Arc::as_ptr(&orders)));
        let parts_ptr = plan
            .get(&2)
            .and_then(|topics| topics.get("orders"))
            .map(Vec::as_ptr);
        super::clear_share_leader_plan(&mut plan);
        super::remember_share_leader(&mut plan, 2, &again, 0);
        super::remember_share_leader(&mut plan, 2, &orders, 1);
        super::prune_share_leader_plan(&mut plan);
        assert_eq!(
            plan.get(&2)
                .and_then(|topics| topics.get("orders"))
                .map(Vec::as_slice),
            Some([0, 1].as_slice())
        );
        assert_eq!(
            plan.get(&2)
                .and_then(|topics| topics.get_key_value("orders"))
                .map(|(key, _)| Arc::as_ptr(key)),
            key_ptr
        );
        assert_eq!(
            plan.get(&2)
                .and_then(|topics| topics.get("orders"))
                .map(Vec::as_ptr),
            parts_ptr
        );
        super::clear_share_leader_plan(&mut plan);
        super::remember_share_leader(&mut plan, 2, &again, 0);
        super::prune_share_leader_plan(&mut plan);
        assert_eq!(
            plan.get(&2)
                .and_then(|topics| topics.get("orders"))
                .map(Vec::as_slice),
            Some([0].as_slice())
        );
        assert_eq!(
            plan.get(&2)
                .and_then(|topics| topics.get_key_value("orders"))
                .map(|(key, _)| Arc::as_ptr(key)),
            key_ptr
        );
    }

    #[test]
    fn a_second_share_fetch_reuses_partition_lists() {
        let mut by_id = BTreeMap::new();
        let mut topics = Vec::new();
        let mut groups = HashMap::new();
        let _ = groups.insert(Arc::<str>::from("orders"), vec![0, 1]);
        let mut topic_ids = HashMap::new();
        let _ = topic_ids.insert(Arc::<str>::from("orders"), [9u8; 16]);
        super::refill_share_fetch(&mut by_id, &mut topics, &groups, &topic_ids, 1024)
            .unwrap_or_else(|error| panic!("{error}"));
        let parts_ptr = by_id.get(&[9u8; 16]).map(Vec::as_ptr);
        let topic_parts_ptr = topics.first().map(|topic| topic.partitions.as_ptr());
        super::refill_share_fetch(&mut by_id, &mut topics, &groups, &topic_ids, 1024)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            by_id.get(&[9u8; 16]).map(Vec::as_slice),
            Some([0, 1].as_slice())
        );
        assert_eq!(by_id.get(&[9u8; 16]).map(Vec::as_ptr), parts_ptr);
        assert_eq!(
            topics.first().map(|topic| topic.partitions.as_ptr()),
            topic_parts_ptr
        );
        assert_eq!(topics.first().map(|topic| topic.topic_id), Some([9u8; 16]));
        let mut fewer = HashMap::new();
        let _ = fewer.insert(Arc::<str>::from("orders"), vec![1]);
        super::refill_share_fetch(&mut by_id, &mut topics, &fewer, &topic_ids, 2048)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            topics
                .first()
                .and_then(|topic| topic.partitions.first())
                .map(|part| (part.partition, part.partition_max_bytes)),
            Some((1, 2048))
        );
        assert_eq!(by_id.get(&[9u8; 16]).map(Vec::as_ptr), parts_ptr);
        assert_eq!(
            topics.first().map(|topic| topic.partitions.as_ptr()),
            topic_parts_ptr
        );
    }

    #[test]
    fn a_share_topic_keeps_one_acquisition_entry() {
        let mut map = AcquisitionMap::new();
        let topic = crate::consumer::TopicName::from("orders");
        let again = crate::consumer::TopicName::from("orders");
        let stored = Acquisition {
            node: 2,
            delivery_count: 1,
            expires_at: None,
        };
        remember_acquisition(&mut map, &topic, 0, 1, stored);
        remember_acquisition(
            &mut map,
            &again,
            0,
            2,
            Acquisition {
                node: 2,
                delivery_count: 1,
                expires_at: None,
            },
        );
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("orders").map(Vec::len), Some(1));
        assert_eq!(acquisition_count(&map), 2);
        assert_eq!(acquisition_node_for_range(&map, "orders", 0, 1, 2), Some(2));
        assert_eq!(acquisition_node_for_range(&map, "orders", 0, 1, 3), None);
        assert_eq!(
            acquisition_at(&map, &topic, 0, 1).map(|found| found.node),
            Some(2)
        );
        assert_eq!(
            acquisition_at(&map, &again, 0, 2).map(|found| found.delivery_count),
            Some(1)
        );
        let partitions = vec![(
            Arc::<str>::from("orders"),
            0,
            vec![AcknowledgementBatch {
                first_offset: 1,
                last_offset: 2,
                types: vec![ACK_ACCEPT],
            }],
        )];
        let kept = partitions
            .first()
            .and_then(|part| part.2.first())
            .map(|batch| batch.types.as_ptr())
            .expect("ack type");
        let direct = single_leader_node(&partitions, |topic, partition, first, last| {
            acquisition_node_for_range(&map, topic, partition, first, last)
        });
        assert_eq!(direct, Some(2));
        assert_eq!(
            partitions
                .first()
                .and_then(|part| part.2.first())
                .map(|batch| batch.types.as_ptr()),
            Some(kept)
        );
        assert!(single_leader_node(&partitions, |_, _, _, _| None).is_none());
    }

    #[test]
    fn share_delivery_returns_its_buffer_when_the_poll_takes_every_record() {
        let mut pending = vec![rec(0, 1), rec(0, 2), rec(0, 3)];
        let all = super::take_delivery(&mut pending, 3);
        assert_eq!(all.len(), 3);
        assert!(pending.is_empty());
        assert_eq!(all.first().map(|record| record.offset), Some(1));
        assert_eq!(all.get(2).map(|record| record.offset), Some(3));
        let mut pending = vec![rec(0, 1), rec(0, 2), rec(0, 3)];
        let head = super::take_delivery(&mut pending, 1);
        assert_eq!(head.len(), 1);
        assert_eq!(head.first().map(|record| record.offset), Some(1));
        assert_eq!(pending.len(), 2);
        assert_eq!(pending.first().map(|record| record.offset), Some(2));
        assert_eq!(pending.get(1).map(|record| record.offset), Some(3));
        assert!(super::take_delivery(&mut pending, 0).is_empty());
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn share_fetch_record_buffer_keeps_capacity() {
        let mut spare = Vec::new();
        let mut incoming = super::take_record_buf(&mut spare);
        incoming.push(rec(0, 1));
        incoming.push(rec(0, 2));
        let capacity = incoming.capacity();
        incoming.clear();
        super::park_record_buf(&mut spare, incoming);
        let again = super::take_record_buf(&mut spare);
        assert!(again.capacity() >= capacity);
        assert!(again.is_empty());
    }

    #[test]
    fn in_order_share_offsets_do_not_spill() {
        let mut cursors = Vec::new();
        let mut spilled = Vec::new();
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 0, 1));
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 0, 2));
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 1, 2));
        assert!(spilled.is_empty());
        assert!(!super::note_share_offset(&mut cursors, &mut spilled, 0, 2));
        let mut cursors = vec![(0, 5)];
        let mut spilled = Vec::new();
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 0, 3));
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 0, 4));
        assert!(!super::note_share_offset(&mut cursors, &mut spilled, 0, 5));
    }

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
    fn an_unresolved_share_assignment_keeps_the_previous_one() {
        let orders = [1u8; 16];
        let unknown = [9u8; 16];
        let mut id_to_name = HashMap::new();
        let _ = id_to_name.insert(orders, "orders".to_owned());
        let mut name_to_id = HashMap::new();
        let _ = name_to_id.insert("orders".to_owned(), orders);
        let subscribed = vec!["orders".to_owned(), "payments".to_owned()];
        assert!(next_share_assignment(
            &[ShareTopicPartitions {
                topic_id: unknown,
                partitions: vec![0, 1],
            }],
            &id_to_name,
            &name_to_id,
            &subscribed,
        )
        .is_none());
        assert!(next_share_assignment(
            &[ShareTopicPartitions {
                topic_id: [0u8; 16],
                partitions: vec![0],
            }],
            &id_to_name,
            &name_to_id,
            &subscribed,
        )
        .is_none());
        let resolved = next_share_assignment(
            &[
                ShareTopicPartitions {
                    topic_id: orders,
                    partitions: vec![1, -1, 1, 0],
                },
                ShareTopicPartitions {
                    topic_id: orders,
                    partitions: vec![0, 3],
                },
                ShareTopicPartitions {
                    topic_id: unknown,
                    partitions: vec![3],
                },
            ],
            &id_to_name,
            &name_to_id,
            &subscribed,
        )
        .expect("known topic replaces the assignment");
        let shared = resolved
            .topic_ids
            .get_key_value("orders")
            .map(|(name, _)| Arc::as_ptr(name));
        assert_eq!(
            resolved
                .assigned
                .iter()
                .map(|(topic, partition)| (topic.as_ref(), *partition))
                .collect::<Vec<_>>(),
            vec![("orders", 1), ("orders", 0), ("orders", 3)]
        );
        assert!(resolved
            .assigned
            .iter()
            .all(|(topic, _)| Some(Arc::as_ptr(topic)) == shared));
        let only = vec!["orders".to_owned()];
        let mapped = next_share_assignment(
            &[ShareTopicPartitions {
                topic_id: unknown,
                partitions: vec![2],
            }],
            &HashMap::new(),
            &HashMap::new(),
            &only,
        )
        .expect("one subscription names the only topic");
        assert_eq!(
            mapped
                .assigned
                .iter()
                .map(|(topic, partition)| (topic.as_ref(), *partition))
                .collect::<Vec<_>>(),
            vec![("orders", 2)]
        );
        assert_eq!(
            mapped
                .topic_ids
                .get_key_value("orders")
                .map(|(name, _)| Arc::as_ptr(name)),
            mapped.assigned.first().map(|(topic, _)| Arc::as_ptr(topic))
        );
        assert_eq!(mapped.topic_ids.get("orders").copied(), Some(unknown));
        let cleared = next_share_assignment(&[], &id_to_name, &name_to_id, &subscribed)
            .expect("empty assignment revokes");
        assert!(cleared.assigned.is_empty());
    }

    #[test]
    fn a_recreated_share_topic_replaces_its_stored_id() {
        assert_eq!(
            refreshed_share_topic_id(Some([1u8; 16]), Some([2u8; 16])),
            Some([2u8; 16])
        );
        assert!(share_topic_recreated([1u8; 16], Some([2u8; 16])));
        assert_eq!(
            refreshed_share_topic_id(Some([1u8; 16]), Some([0u8; 16])),
            Some([1u8; 16])
        );
        assert!(!share_topic_recreated([1u8; 16], Some([0u8; 16])));
        assert!(!share_topic_recreated([1u8; 16], Some([1u8; 16])));
        assert_eq!(
            refreshed_share_topic_id(None, Some([4u8; 16])),
            Some([4u8; 16])
        );
        let mut stored = HashMap::new();
        let _ = stored.insert(Arc::<str>::from("orders"), [1u8; 16]);
        let mut metadata = HashMap::new();
        let _ = metadata.insert("orders".to_owned(), [1u8; 16]);
        let assigned = [("orders".to_owned(), 0)];
        assert!(share_topic_ids_current(&stored, &assigned, &metadata));
        let _ = metadata.insert("orders".to_owned(), [2u8; 16]);
        assert!(!share_topic_ids_current(&stored, &assigned, &metadata));
        metadata.clear();
        assert!(share_topic_ids_current(&stored, &assigned, &metadata));
        let assigned = [("payments".to_owned(), 1)];
        assert!(share_topic_ids_current(&stored, &assigned, &metadata));
        let _ = metadata.insert("payments".to_owned(), [4u8; 16]);
        assert!(!share_topic_ids_current(&stored, &assigned, &metadata));
    }

    #[test]
    fn fenced_share_member_restarts_at_the_join_epoch() {
        assert!(share_membership_lost(error::FENCED_MEMBER_EPOCH));
        assert!(share_membership_lost(error::UNKNOWN_MEMBER_ID));
        assert!(!share_membership_lost(0));
        assert!(!share_membership_lost(error::FENCED_INSTANCE_ID));
        assert_eq!(
            share_heartbeat_epoch(error::FENCED_MEMBER_EPOCH, 9, 9),
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        );
        assert_eq!(
            share_heartbeat_epoch(error::UNKNOWN_MEMBER_ID, -1, 4),
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        );
        assert_eq!(share_heartbeat_epoch(0, 6, 4), 6);
        assert_eq!(share_heartbeat_epoch(0, 0, 4), 4);
        assert_eq!(share_heartbeat_epoch(error::STALE_MEMBER_EPOCH, 2, 5), 2);
        assert_eq!(share_heartbeat_epoch(error::STALE_MEMBER_EPOCH, 0, 5), 0);
        assert_eq!(
            share_heartbeat_epoch(error::STALE_MEMBER_EPOCH, -1, 5),
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        );
        assert_eq!(
            share_heartbeat_subscription(
                ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
                &["t".into()]
            ),
            Some(vec!["t".into()])
        );
        assert_eq!(share_heartbeat_subscription(4, &["t".into()]), None);
    }

    #[test]
    fn acknowledgement_batches_collapses_contiguous_offsets() {
        let recs = [rec(0, 1), rec(0, 3), rec(0, 2), rec(1, 9)];
        let batches = acknowledgement_batches(&recs, ACK_ACCEPT);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].0.as_ref(), "t");
        assert!(Arc::ptr_eq(&batches[0].0, &batches[1].0));
        assert_eq!(batches[0].1, 0);
        assert_eq!(batches[0].2.len(), 1);
        assert_eq!(batches[0].2[0].first_offset, 1);
        assert_eq!(batches[0].2[0].last_offset, 3);
        assert_eq!(batches[0].2[0].types, vec![ACK_ACCEPT]);
        assert_eq!(batches[1].0.as_ref(), "t");
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
    fn one_partition_ack_offsets_keep_their_buffer() {
        let recs = [rec(0, 3), rec(0, 1), rec(0, 2), rec(0, 2)];
        let mut offsets = Vec::new();
        let mut pairs = Vec::new();
        let mut topic = None;
        let mut ack_types = Vec::new();
        let first = super::acknowledgement_batches_with(
            &recs,
            ACK_ACCEPT,
            &mut offsets,
            &mut pairs,
            &mut topic,
            &mut ack_types,
        );
        let ptr = offsets.as_ptr();
        let kept = first
            .first()
            .map(|part| Arc::clone(&part.0))
            .expect("partition");
        let kept_types = first
            .first()
            .and_then(|part| part.2.first())
            .map(|batch| batch.types.as_ptr())
            .expect("ack type");
        for (_, _, batches) in first {
            for batch in batches {
                ack_types.push(batch.types);
            }
        }
        let again = super::acknowledgement_batches_with(
            &recs,
            ACK_ACCEPT,
            &mut offsets,
            &mut pairs,
            &mut topic,
            &mut ack_types,
        );
        assert_eq!(offsets.as_ptr(), ptr);
        let again_name = again
            .first()
            .map(|part| Arc::clone(&part.0))
            .expect("partition");
        assert!(Arc::ptr_eq(&kept, &again_name));
        assert_eq!(
            again
                .first()
                .and_then(|part| part.2.first())
                .map(|batch| batch.types.as_ptr()),
            Some(kept_types)
        );
        assert_eq!(again.len(), 1);
        assert_eq!(
            again
                .first()
                .and_then(|part| part.2.first())
                .map(|batch| { (batch.first_offset, batch.last_offset) }),
            Some((1, 3))
        );
    }

    #[test]
    fn one_topic_ack_pairs_keep_their_buffer() {
        let recs = [rec(1, 4), rec(0, 2), rec(0, 1), rec(1, 5)];
        let mut offsets = Vec::new();
        let mut pairs = Vec::new();
        let mut topic = None;
        let mut ack_types = Vec::new();
        let first = super::acknowledgement_batches_with(
            &recs,
            ACK_ACCEPT,
            &mut offsets,
            &mut pairs,
            &mut topic,
            &mut ack_types,
        );
        let ptr = pairs.as_ptr();
        let again = super::acknowledgement_batches_with(
            &recs,
            ACK_ACCEPT,
            &mut offsets,
            &mut pairs,
            &mut topic,
            &mut ack_types,
        );
        assert_eq!(pairs.as_ptr(), ptr);
        assert_eq!(first.len(), 2);
        assert_eq!(
            first
                .first()
                .map(|part| (part.1, part.2.first().map(|batch| batch.first_offset))),
            Some((0, Some(1)))
        );
        assert_eq!(
            first
                .get(1)
                .map(|part| (part.1, part.2.first().map(|batch| batch.last_offset))),
            Some((1, Some(5)))
        );
        assert_eq!(first, again);
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
        let at_end = ShareRecords::from(vec![rec(0, i64::MAX), rec(1, 4)]);
        assert_eq!(
            at_end.next_offsets(),
            vec![(
                crate::TopicPartition::new("t", 1),
                crate::OffsetAndMetadata::new(5)
            )]
        );
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

    #[test]
    fn a_share_topic_id_is_not_guessed_from_the_first_subscription() {
        let orders = [1u8; 16];
        let trades = [2u8; 16];
        let names = [("orders", &orders), ("trades", &trades)];
        let named = share_topic_name(names, orders);
        assert!(named.is_ok());
        if let Ok(name) = named {
            assert_eq!(name, "orders");
        }
        assert!(share_topic_name(names, [9u8; 16]).is_err());
        assert!(share_topic_name([("orders", &orders), ("other", &orders)], orders).is_err());
        assert!(share_topic_name([("", &orders)], orders).is_err());
    }

    #[test]
    fn a_fetched_share_topic_keeps_the_stored_name() {
        let long: Arc<str> = Arc::from("topic-name-longer-than-twenty-three");
        let stored = Arc::as_ptr(&long);
        let id = [7u8; 16];
        let mut topic_ids = HashMap::new();
        let _ = topic_ids.insert(Arc::clone(&long), id);
        let topic = super::topic_name_for_fetch(&topic_ids, id).expect("long name");
        let crate::consumer::TopicName::Heap(shared) = &topic else {
            panic!("long topic was copied");
        };
        assert_eq!(Arc::as_ptr(shared), stored);
        let again = super::topic_name_for_fetch(&topic_ids, id).expect("long name");
        let crate::consumer::TopicName::Heap(second) = &again else {
            panic!("long topic was copied");
        };
        assert_eq!(Arc::as_ptr(second), stored);
        let short_id = [8u8; 16];
        let _ = topic_ids.insert(Arc::<str>::from("orders"), short_id);
        let inline = super::topic_name_for_fetch(&topic_ids, short_id).expect("short name");
        assert!(matches!(inline, crate::consumer::TopicName::Inline(_, _)));
        assert!(super::topic_name_for_fetch(&topic_ids, [9u8; 16]).is_err());
    }

    #[test]
    fn acknowledgements_go_to_the_broker_that_acquired_them() {
        let partitions = vec![(
            Arc::<str>::from("orders"),
            0,
            vec![AcknowledgementBatch {
                first_offset: 1,
                last_offset: 3,
                types: vec![ACK_ACCEPT],
            }],
        )];
        let grouped = group_acks_by_acquisition_node(
            |_topic, _partition, offset| match offset {
                1 | 2 => Some(4),
                3 => Some(9),
                _ => None,
            },
            |_, _, _, _| None,
            &partitions,
        );
        assert!(grouped.is_ok());
        if let Ok(grouped) = grouped {
            assert_eq!(grouped.len(), 2);
            let first = grouped.get(&4).and_then(|parts| parts.first());
            let second = grouped.get(&9).and_then(|parts| parts.first());
            assert_eq!(
                first
                    .and_then(|part| part.2.first())
                    .map(|batch| batch.first_offset),
                Some(1)
            );
            assert_eq!(
                first
                    .and_then(|part| part.2.first())
                    .map(|batch| batch.last_offset),
                Some(2)
            );
            assert_eq!(
                second
                    .and_then(|part| part.2.first())
                    .map(|batch| batch.first_offset),
                Some(3)
            );
            assert_eq!(
                second
                    .and_then(|part| part.2.first())
                    .map(|batch| batch.last_offset),
                Some(3)
            );
            assert_eq!(
                first
                    .and_then(|part| part.2.first())
                    .map(|batch| batch.types.as_slice()),
                Some([ACK_ACCEPT].as_slice())
            );
            assert_eq!(
                second
                    .and_then(|part| part.2.first())
                    .map(|batch| batch.types.as_slice()),
                Some([ACK_ACCEPT].as_slice())
            );
        }
        let missing =
            group_acks_by_acquisition_node(|_, _, _| None, |_, _, _, _| None, &partitions);
        assert!(missing.is_err());
        let same = vec![(
            Arc::<str>::from("orders"),
            0,
            vec![AcknowledgementBatch {
                first_offset: 1,
                last_offset: 4,
                types: vec![ACK_ACCEPT],
            }],
        )];
        let same_node = group_acks_by_acquisition_node(|_, _, _| Some(4), |_, _, _, _| None, &same);
        assert!(same_node.is_ok());
        if let Ok(same_node) = same_node {
            assert_eq!(same_node.get(&4).map(Vec::as_slice), Some(same.as_slice()));
        }
        let mixed = vec![(
            Arc::<str>::from("orders"),
            0,
            vec![AcknowledgementBatch {
                first_offset: 1,
                last_offset: 2,
                types: vec![ACK_ACCEPT, ACK_REJECT],
            }],
        )];
        let mixed = group_acks_by_acquisition_node(
            |_topic, _partition, offset| Some(if offset == 1 { 4 } else { 9 }),
            |_, _, _, _| None,
            &mixed,
        );
        assert!(mixed.is_ok());
        if let Ok(mixed) = mixed {
            assert_eq!(
                mixed
                    .get(&4)
                    .and_then(|parts| parts.first())
                    .and_then(|part| part.2.first())
                    .map(|batch| batch.types.as_slice()),
                Some([ACK_ACCEPT].as_slice())
            );
            assert_eq!(
                mixed
                    .get(&9)
                    .and_then(|parts| parts.first())
                    .and_then(|part| part.2.first())
                    .map(|batch| batch.types.as_slice()),
                Some([ACK_REJECT].as_slice())
            );
        }
        let mut pending = vec![
            (
                Arc::<str>::from("orders"),
                0,
                vec![AcknowledgementBatch {
                    first_offset: 1,
                    last_offset: 2,
                    types: vec![ACK_ACCEPT],
                }],
            ),
            (
                Arc::<str>::from("orders"),
                0,
                vec![AcknowledgementBatch {
                    first_offset: 3,
                    last_offset: 3,
                    types: vec![ACK_ACCEPT],
                }],
            ),
        ];
        drop_sent_ack_batches(
            &mut pending,
            "orders",
            0,
            &[AcknowledgementBatch {
                first_offset: 1,
                last_offset: 2,
                types: vec![ACK_ACCEPT],
            }],
        );
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending
                .first()
                .and_then(|part| part.2.first())
                .map(|batch| batch.first_offset),
            Some(3)
        );
    }

    #[test]
    fn share_current_leader_survives_a_metadata_refresh() {
        use crate::protocol::api::NodeEndpoint;
        let mut cluster = crate::cluster::Cluster::default();
        cluster.apply_node_endpoints(&[NodeEndpoint::new(1, "10.0.0.1", 9092, None)]);
        assert!(cluster.apply_current_leader("orders", 0, 1, 2));
        cluster.apply_node_endpoints(&[
            NodeEndpoint::new(2, "[2001:db8::9]", 9092, None),
            NodeEndpoint::new(3, "[[::1]]", 9092, None),
        ]);
        let moved = cluster.apply_current_leader("orders", 0, 2, 5);
        let patch = share_leader_patch(moved, "orders", 0, error::NOT_LEADER_OR_FOLLOWER, 2, 5);
        assert!(patch.is_some());
        assert!(share_leader_patch(true, "orders", 0, error::INVALID_RECORD_STATE, 2, 5).is_none());
        assert!(!cluster.apply_current_leader("orders", 0, 3, 6));
        let mut patches = Vec::new();
        if let Some(patch) = patch {
            remember_share_leader_patch(&mut patches, patch);
        }
        if let Some(older) =
            share_leader_patch(true, "orders", 0, error::NOT_LEADER_OR_FOLLOWER, 2, 4)
        {
            remember_share_leader_patch(&mut patches, older);
        }
        assert_eq!(patches.first().map(|patch| patch.leader_epoch), Some(5));
        if let Some(newer) =
            share_leader_patch(true, "orders", 0, error::LEADER_NOT_AVAILABLE, 2, 8)
        {
            remember_share_leader_patch(&mut patches, newer);
        }
        assert_eq!(patches.len(), 1);
        assert_eq!(patches.first().map(|patch| patch.leader_epoch), Some(8));
        cluster.invalidate_topic("orders");
        assert!(cluster.leader("orders", 0).is_err());
        reapply_share_leader_patches_on(&mut cluster, &patches);
        let restored = cluster.leader("orders", 0);
        assert!(restored.is_ok());
        if let Ok((node, address)) = restored {
            assert_eq!(node, 2);
            assert_eq!(address, "[2001:db8::9]:9092");
        }
    }
}
