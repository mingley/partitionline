//! Share groups (KIP-932): queue-style consumption with per-record ack.

mod acquired;

use std::collections::{BTreeMap, HashMap, HashSet};
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
    collect_topics, coord_roundtrip_with_bootstrap, filter_matching_topics, TopicMatch,
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
    decode_share_fetch_response_reusing_cooperative, decode_share_group_heartbeat_response,
    encode_one_share_acknowledge_cooperative, encode_share_acknowledge_request,
    encode_share_acknowledge_topics_cooperative, encode_share_fetch_request_with_options,
    encode_share_group_heartbeat_request, AcknowledgementBatch, ShareAckTopic, ShareFetchPartition,
    ShareFetchRequestOptions, ShareFetchTopic, ShareFetchedTopic, ShareGroupHeartbeatRequest,
    ShareTopicPartitions, ACK_ACCEPT, ACK_REJECT, ACK_RELEASE, ACK_RENEW,
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

#[derive(Default)]
struct PartitionIndex {
    /// Insertion order. Overlapping ranges stay in this order.
    order: Vec<usize>,
    /// `first` offset to run index while ranges on this partition do not overlap.
    by_first: BTreeMap<i64, usize>,
    overlapped: bool,
}

struct TopicRuns {
    runs: Vec<AcquisitionRun>,
    by_partition: HashMap<i32, PartitionIndex>,
}

impl TopicRuns {
    fn new() -> Self {
        Self {
            runs: Vec::new(),
            by_partition: HashMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.runs.len()
    }

    fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    fn reindex(&mut self) {
        let snapshot: Vec<(i32, i64, i64)> = self
            .runs
            .iter()
            .map(|run| (run.partition, run.first, run.last))
            .collect();
        self.by_partition.clear();
        for (index, &(partition, first, _)) in snapshot.iter().enumerate() {
            let part = self.by_partition.entry(partition).or_default();
            part.order.push(index);
            if part.overlapped {
                continue;
            }
            let pred = part
                .by_first
                .range(..=first)
                .next_back()
                .map(|(_, &pred)| pred);
            let overlaps = part.by_first.contains_key(&first)
                || pred.is_some_and(|pred| {
                    snapshot
                        .get(pred)
                        .is_some_and(|&(_, _, prev_last)| prev_last >= first)
                });
            if overlaps {
                part.overlapped = true;
                part.by_first.clear();
                continue;
            }
            let _ = part.by_first.insert(first, index);
        }
    }

    fn retain_runs(&mut self, keep: impl FnMut(&AcquisitionRun) -> bool) {
        self.runs.retain(keep);
        self.reindex();
    }
}

type AcquisitionMap = HashMap<crate::consumer::TopicName, TopicRuns>;

#[derive(Clone, Copy)]
struct Acquisition {
    node: i32,
    delivery_count: i16,
    expires_at: Option<Instant>,
}

#[derive(Clone, Copy)]
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
        let _ = map.insert(topic.clone(), TopicRuns::new());
    }
    let Some(topic_runs) = map.get_mut(topic.as_str()) else {
        return;
    };
    if topic_runs
        .by_partition
        .get(&partition)
        .is_some_and(|part| part.overlapped)
    {
        remember_overlapped(topic_runs, partition, offset, acquisition);
    } else {
        remember_indexed(topic_runs, partition, offset, acquisition);
    }
}

fn remember_overlapped(
    topic_runs: &mut TopicRuns,
    partition: i32,
    offset: i64,
    acquisition: Acquisition,
) {
    let slots = topic_runs
        .by_partition
        .get(&partition)
        .map(|part| part.order.len())
        .unwrap_or(0);
    for slot in 0..slots {
        let Some(index) = topic_runs
            .by_partition
            .get(&partition)
            .and_then(|part| part.order.get(slot))
            .copied()
        else {
            break;
        };
        let Some(run) = topic_runs.runs.get_mut(index) else {
            continue;
        };
        if run.partition == partition
            && run.last < i64::MAX
            && run.last.saturating_add(1) == offset
            && same_acquisition(&run.acquisition, &acquisition)
        {
            run.last = offset;
            return;
        }
    }
    for slot in 0..slots {
        let Some(index) = topic_runs
            .by_partition
            .get(&partition)
            .and_then(|part| part.order.get(slot))
            .copied()
        else {
            break;
        };
        let Some(run) = topic_runs.runs.get_mut(index) else {
            continue;
        };
        if run.partition == partition
            && run.first > i64::MIN
            && run.first.saturating_sub(1) == offset
            && same_acquisition(&run.acquisition, &acquisition)
        {
            run.first = offset;
            return;
        }
    }
    for slot in 0..slots {
        let Some(index) = topic_runs
            .by_partition
            .get(&partition)
            .and_then(|part| part.order.get(slot))
            .copied()
        else {
            break;
        };
        let Some(run) = topic_runs.runs.get(index) else {
            continue;
        };
        if run.partition == partition
            && run.first <= offset
            && offset <= run.last
            && same_acquisition(&run.acquisition, &acquisition)
        {
            return;
        }
    }
    let mut cover = None;
    for slot in 0..slots {
        let Some(index) = topic_runs
            .by_partition
            .get(&partition)
            .and_then(|part| part.order.get(slot))
            .copied()
        else {
            break;
        };
        let Some(run) = topic_runs.runs.get(index) else {
            continue;
        };
        if run.partition == partition && run.first <= offset && offset <= run.last {
            cover = Some(index);
            break;
        }
    }
    if let Some(position) = cover {
        replace_acquired_offset(topic_runs, position, offset, acquisition);
        return;
    }
    let index = topic_runs.runs.len();
    topic_runs.runs.push(AcquisitionRun {
        partition,
        first: offset,
        last: offset,
        acquisition,
    });
    topic_runs
        .by_partition
        .entry(partition)
        .or_default()
        .order
        .push(index);
}

fn remember_indexed(
    topic_runs: &mut TopicRuns,
    partition: i32,
    offset: i64,
    acquisition: Acquisition,
) {
    let extend = topic_runs.by_partition.get(&partition).and_then(|part| {
        part.by_first
            .range(..offset)
            .next_back()
            .map(|(&first, &index)| (first, index))
    });
    if let Some((first, index)) = extend {
        let can_extend = topic_runs.runs.get(index).is_some_and(|run| {
            run.partition == partition
                && run.last < i64::MAX
                && run.last.saturating_add(1) == offset
                && same_acquisition(&run.acquisition, &acquisition)
        });
        if can_extend {
            if let Some(run) = topic_runs.runs.get_mut(index) {
                run.last = offset;
            }
            let succ = topic_runs.by_partition.get(&partition).and_then(|part| {
                part.by_first
                    .range((first.saturating_add(1))..)
                    .next()
                    .map(|(&start, _)| start)
            });
            if succ.is_some_and(|start| start <= offset) {
                if let Some(part) = topic_runs.by_partition.get_mut(&partition) {
                    part.overlapped = true;
                    part.by_first.clear();
                }
            }
            return;
        }
    }
    let prepend = if offset < i64::MAX {
        topic_runs
            .by_partition
            .get(&partition)
            .and_then(|part| part.by_first.get(&offset.saturating_add(1)).copied())
    } else {
        None
    };
    if let Some(index) = prepend {
        let can_prepend = topic_runs.runs.get(index).is_some_and(|run| {
            run.partition == partition
                && run.first > i64::MIN
                && run.first.saturating_sub(1) == offset
                && same_acquisition(&run.acquisition, &acquisition)
        });
        if can_prepend {
            let pred_overlaps = topic_runs
                .by_partition
                .get(&partition)
                .and_then(|part| {
                    part.by_first
                        .range(..offset)
                        .next_back()
                        .map(|(_, &pred)| pred)
                })
                .is_some_and(|pred| {
                    topic_runs
                        .runs
                        .get(pred)
                        .is_some_and(|run| run.last >= offset)
                });
            let old_first = topic_runs.runs.get(index).map(|run| run.first);
            if let Some(run) = topic_runs.runs.get_mut(index) {
                run.first = offset;
            }
            if let Some(part) = topic_runs.by_partition.get_mut(&partition) {
                if let Some(old_first) = old_first {
                    let _ = part.by_first.remove(&old_first);
                }
                let _ = part.by_first.insert(offset, index);
                if pred_overlaps {
                    part.overlapped = true;
                    part.by_first.clear();
                }
            }
            return;
        }
    }
    let cover = topic_runs.by_partition.get(&partition).and_then(|part| {
        part.by_first
            .range(..=offset)
            .next_back()
            .map(|(_, &index)| index)
    });
    if let Some(index) = cover {
        let contained = topic_runs.runs.get(index).is_some_and(|run| {
            run.partition == partition && run.first <= offset && offset <= run.last
        });
        if contained {
            let same = topic_runs
                .runs
                .get(index)
                .is_some_and(|run| same_acquisition(&run.acquisition, &acquisition));
            if !same {
                replace_acquired_offset(topic_runs, index, offset, acquisition);
            }
            return;
        }
    }
    let index = topic_runs.runs.len();
    topic_runs.runs.push(AcquisitionRun {
        partition,
        first: offset,
        last: offset,
        acquisition,
    });
    let part = topic_runs.by_partition.entry(partition).or_default();
    part.order.push(index);
    if part.by_first.insert(offset, index).is_some() {
        part.overlapped = true;
        part.by_first.clear();
    }
}

fn replace_acquired_offset(
    topic_runs: &mut TopicRuns,
    position: usize,
    offset: i64,
    acquisition: Acquisition,
) {
    let Some(run) = topic_runs.runs.get(position) else {
        return;
    };
    let partition = run.partition;
    let first = run.first;
    let last = run.last;
    let old = run.acquisition;
    let _removed = topic_runs.runs.swap_remove(position);
    if first < offset {
        topic_runs.runs.push(AcquisitionRun {
            partition,
            first,
            last: offset.saturating_sub(1),
            acquisition: old,
        });
    }
    topic_runs.runs.push(AcquisitionRun {
        partition,
        first: offset,
        last: offset,
        acquisition,
    });
    if offset < last {
        topic_runs.runs.push(AcquisitionRun {
            partition,
            first: offset.saturating_add(1),
            last,
            acquisition: old,
        });
    }
    topic_runs.reindex();
}

fn acquisition_run(topic_runs: &TopicRuns, partition: i32, offset: i64) -> Option<&AcquisitionRun> {
    let part = topic_runs.by_partition.get(&partition)?;
    if part.overlapped {
        for &index in &part.order {
            let run = topic_runs.runs.get(index)?;
            if run.partition == partition && run.first <= offset && offset <= run.last {
                return Some(run);
            }
        }
        return None;
    }
    let (&_first, &index) = part.by_first.range(..=offset).next_back()?;
    let run = topic_runs.runs.get(index)?;
    (run.partition == partition && run.first <= offset && offset <= run.last).then_some(run)
}

fn acquisition_at<'a>(
    map: &'a AcquisitionMap,
    topic: &crate::consumer::TopicName,
    partition: i32,
    offset: i64,
) -> Option<&'a Acquisition> {
    acquisition_run(map.get(topic)?, partition, offset).map(|run| &run.acquisition)
}

/// Reject an acknowledgement whose delivery count is no longer the stored one.
///
/// A long batch pauses between slices so the check can still reach a deadline.
async fn reject_stale_share_acks(map: &AcquisitionMap, recs: &[ShareRecord]) -> Result<()> {
    let mut walked = 0usize;
    for record in recs {
        let acquired = acquisition_at(map, &record.topic, record.partition, record.offset);
        if acquired.is_none_or(|a| a.delivery_count != record.delivery_count) {
            return Err(Error::broker(
                error::INVALID_RECORD_STATE,
                format!(
                    "stale share acknowledgement {}-{}@{}",
                    record.topic, record.partition, record.offset
                ),
            ));
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    Ok(())
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
    let topic_runs = map.get(topic)?;
    if topic_runs
        .by_partition
        .get(&partition)
        .is_some_and(|part| part.overlapped)
    {
        return acquisition_node_along_runs(topic_runs, partition, first, last);
    }
    let mut scan_from = topic_runs.by_partition.get(&partition).and_then(|part| {
        part.by_first
            .range(..=first)
            .next_back()
            .map(|(&start, _)| start)
    })?;
    let mut cursor = first;
    let mut node = None;
    loop {
        let chunk = covering_run_indexes(topic_runs, partition, scan_from, last, 8 * 1024);
        if chunk.is_empty() {
            break;
        }
        for (start, index) in chunk {
            let run = topic_runs.runs.get(index)?;
            scan_from = if start == i64::MAX {
                start
            } else {
                start.saturating_add(1)
            };
            if run.last < cursor {
                continue;
            }
            if run.first > cursor || cursor > run.last {
                return None;
            }
            if node.is_some_and(|current| current != run.acquisition.node) {
                return None;
            }
            node = Some(run.acquisition.node);
            if run.last >= last || run.last == i64::MAX {
                return node;
            }
            let next = run.last.saturating_add(1);
            if next <= run.last {
                return node;
            }
            cursor = next;
        }
        if scan_from > last || scan_from == i64::MAX {
            break;
        }
    }
    None
}

fn covering_run_indexes(
    topic_runs: &TopicRuns,
    partition: i32,
    scan_from: i64,
    last: i64,
    limit: usize,
) -> Vec<(i64, usize)> {
    topic_runs
        .by_partition
        .get(&partition)
        .map(|part| {
            part.by_first
                .range(scan_from..=last)
                .take(limit)
                .map(|(&start, &index)| (start, index))
                .collect()
        })
        .unwrap_or_default()
}

fn acquisition_node_along_runs(
    runs: &TopicRuns,
    partition: i32,
    first: i64,
    last: i64,
) -> Option<i32> {
    let mut cursor = first;
    let mut node = None;
    while cursor <= last {
        let covering = acquisition_run(runs, partition, cursor)?;
        if node.is_some_and(|current| current != covering.acquisition.node) {
            return None;
        }
        node = Some(covering.acquisition.node);
        if covering.last >= last || covering.last == i64::MAX {
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

/// Sum of each acknowledgement's overlap with each range.
///
/// Overlapping batches stack. The coordinate sweep answers every range together
/// so one partition's runs do not each rescan the whole batch list.
fn covered_lengths(ranges: &[(i64, i64)], sent: &[AcknowledgementBatch]) -> Vec<usize> {
    if ranges.is_empty() {
        return Vec::new();
    }
    if sent.is_empty() {
        return vec![0; ranges.len()];
    }
    let mut coords = Vec::with_capacity(ranges.len().saturating_add(sent.len()).saturating_mul(2));
    for &(first, last) in ranges {
        if last < first {
            continue;
        }
        coords.push(i128::from(first));
        coords.push(i128::from(last) + 1);
    }
    for batch in sent {
        if batch.last_offset < batch.first_offset {
            continue;
        }
        coords.push(i128::from(batch.first_offset));
        coords.push(i128::from(batch.last_offset) + 1);
    }
    coords.sort_unstable();
    coords.dedup();
    let mut diff = vec![0_i128; coords.len()];
    for batch in sent {
        if batch.last_offset < batch.first_offset {
            continue;
        }
        let start = i128::from(batch.first_offset);
        let end = i128::from(batch.last_offset) + 1;
        let left = coords.partition_point(|&point| point < start);
        let right = coords.partition_point(|&point| point < end);
        if let Some(slot) = diff.get_mut(left) {
            *slot = slot.saturating_add(1);
        }
        if let Some(slot) = diff.get_mut(right) {
            *slot = slot.saturating_sub(1);
        }
    }
    let mut coverage = 0_i128;
    let mut prefix = vec![0_i128; coords.len()];
    for index in 0..coords.len().saturating_sub(1) {
        coverage = coverage.saturating_add(diff.get(index).copied().unwrap_or(0));
        let left = coords.get(index).copied().unwrap_or(0);
        let right = coords.get(index.saturating_add(1)).copied().unwrap_or(left);
        let gap = coverage.saturating_mul(right.saturating_sub(left));
        let previous = prefix.get(index).copied().unwrap_or(0);
        if let Some(slot) = prefix.get_mut(index.saturating_add(1)) {
            *slot = previous.saturating_add(gap);
        }
    }
    ranges
        .iter()
        .map(|&(first, last)| {
            if last < first {
                return 0usize;
            }
            let start = i128::from(first);
            let end = i128::from(last) + 1;
            let left = coords.partition_point(|&point| point < start);
            let right = coords.partition_point(|&point| point < end);
            let total = prefix
                .get(right)
                .copied()
                .unwrap_or(0)
                .saturating_sub(prefix.get(left).copied().unwrap_or(0));
            usize::try_from(total).unwrap_or(usize::MAX)
        })
        .collect()
}

fn merged_sent_cuts(sent: &[AcknowledgementBatch]) -> Vec<(i64, i64)> {
    let mut cuts = Vec::new();
    for batch in sent {
        if batch.last_offset >= batch.first_offset {
            cuts.push((batch.first_offset, batch.last_offset));
        }
    }
    cuts.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for (start, end) in cuts {
        if let Some((_, prev_end)) = merged.last_mut() {
            if start <= prev_end.saturating_add(1) {
                if end > *prev_end {
                    *prev_end = end;
                }
                continue;
            }
        }
        merged.push((start, end));
    }
    merged
}

/// Covered and uncovered pieces of one run, in offset order.
///
/// `cuts` is the merged acknowledgement coverage for the whole call, so each
/// run does not sort the batch list again.
fn partial_ack_spans(first: i64, last: i64, cuts: &[(i64, i64)]) -> Vec<(i64, i64, bool)> {
    let mut spans = Vec::new();
    let mut cursor = first;
    let start_at = cuts.partition_point(|&(_, end)| end < first);
    let overlapping = cuts.get(start_at..).unwrap_or(&[]);
    for &(start, end) in overlapping {
        if start > last {
            break;
        }
        let start = start.max(first);
        let end = end.min(last);
        if start > end {
            continue;
        }
        if cursor < start {
            spans.push((cursor, start.saturating_sub(1), false));
        }
        spans.push((start, end, true));
        if end == i64::MAX {
            return spans;
        }
        let next = end.saturating_add(1);
        if next <= end {
            return spans;
        }
        cursor = next;
    }
    if cursor <= last {
        spans.push((cursor, last, false));
    }
    spans
}

fn replace_acquisition_runs(runs: &mut Vec<AcquisitionRun>, kept: Vec<AcquisitionRun>) {
    if kept.len() <= runs.capacity() {
        runs.clear();
        runs.extend(kept);
    } else {
        *runs = kept;
    }
}

fn drop_covered_runs(
    runs: &mut Vec<AcquisitionRun>,
    partition: i32,
    sent: &[AcknowledgementBatch],
) {
    let ranges: Vec<(i64, i64)> = runs.iter().map(|run| (run.first, run.last)).collect();
    let covered_by_run = covered_lengths(&ranges, sent);
    let cuts = merged_sent_cuts(sent);
    let mut kept = Vec::new();
    for (run, covered) in runs.iter().copied().zip(covered_by_run) {
        if run.partition != partition {
            kept.push(run);
            continue;
        }
        let whole = span_len(run.first, run.last);
        if covered == 0 {
            kept.push(run);
        } else if covered < whole {
            for (first, last, is_covered) in partial_ack_spans(run.first, run.last, &cuts) {
                if !is_covered {
                    kept.push(AcquisitionRun {
                        partition,
                        first,
                        last,
                        acquisition: run.acquisition,
                    });
                }
            }
        }
    }
    replace_acquisition_runs(runs, kept);
}

fn drop_covered_acquisitions(
    topic_runs: &mut TopicRuns,
    partition: i32,
    sent: &[AcknowledgementBatch],
) {
    drop_covered_runs(&mut topic_runs.runs, partition, sent);
    topic_runs.reindex();
}

fn renew_covered_runs(
    runs: &mut Vec<AcquisitionRun>,
    partition: i32,
    sent: &[AcknowledgementBatch],
    expiry: Option<Instant>,
) {
    let ranges: Vec<(i64, i64)> = runs.iter().map(|run| (run.first, run.last)).collect();
    let covered_by_run = covered_lengths(&ranges, sent);
    let cuts = merged_sent_cuts(sent);
    let mut kept = Vec::new();
    for (run, covered) in runs.iter().copied().zip(covered_by_run) {
        if run.partition != partition {
            kept.push(run);
            continue;
        }
        let whole = span_len(run.first, run.last);
        if covered == 0 {
            kept.push(run);
        } else if covered >= whole {
            let mut renewed = run;
            renewed.acquisition.expires_at = expiry;
            kept.push(renewed);
        } else {
            for (first, last, is_covered) in partial_ack_spans(run.first, run.last, &cuts) {
                let mut piece = run;
                piece.first = first;
                piece.last = last;
                if is_covered {
                    piece.acquisition.expires_at = expiry;
                }
                kept.push(piece);
            }
        }
    }
    replace_acquisition_runs(runs, kept);
}

fn renew_covered_acquisitions(
    topic_runs: &mut TopicRuns,
    partition: i32,
    sent: &[AcknowledgementBatch],
    expiry: Option<Instant>,
) {
    renew_covered_runs(&mut topic_runs.runs, partition, sent, expiry);
    topic_runs.reindex();
}

fn count_covered_acquisitions(
    topic_runs: &TopicRuns,
    partition: i32,
    sent: &[AcknowledgementBatch],
) -> usize {
    let Some(indexes) = topic_runs.by_partition.get(&partition) else {
        return 0;
    };
    let ranges: Vec<(i64, i64)> = indexes
        .order
        .iter()
        .filter_map(|&index| topic_runs.runs.get(index).map(|run| (run.first, run.last)))
        .collect();
    covered_lengths(&ranges, sent)
        .into_iter()
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

fn park_fetched_topics(spare: &mut Vec<ShareFetchedTopic>, topics: Vec<ShareFetchedTopic>) {
    if topics.capacity() > spare.capacity() {
        *spare = topics;
    }
}

fn reserve_share_delivery(pending: &mut Vec<ShareRecord>, additional: usize) {
    let needed = pending.len().saturating_add(additional);
    if pending.capacity() < needed {
        pending.reserve(needed - pending.capacity());
    }
}

fn note_share_offset(
    cursors: &mut HashMap<i32, i64>,
    spilled: &mut HashMap<i32, HashSet<i64>>,
    partition: i32,
    offset: i64,
) -> bool {
    if let Some(set) = spilled.get_mut(&partition) {
        return set.insert(offset);
    }
    if let Some(last) = cursors.get_mut(&partition) {
        if offset == *last {
            return false;
        }
        if offset < *last {
            let mut set = HashSet::new();
            let _kept = set.insert(*last);
            let fresh = set.insert(offset);
            let _previous = spilled.insert(partition, set);
            *last = offset;
            return fresh;
        }
        *last = offset;
        return true;
    }
    let _previous = cursors.insert(partition, offset);
    true
}

#[cfg(test)]
async fn note_share_offsets(
    cursors: &mut HashMap<i32, i64>,
    spilled: &mut HashMap<i32, HashSet<i64>>,
    notes: &[(i32, i64)],
) -> bool {
    let mut walked = 0usize;
    for &(partition, offset) in notes {
        if !note_share_offset(cursors, spilled, partition, offset) {
            return false;
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    true
}

#[cfg(test)]
async fn remember_share_offsets(
    map: &mut AcquisitionMap,
    topic: &crate::consumer::TopicName,
    offsets: &[(i32, i64)],
    acquisition: Acquisition,
) {
    let mut walked = 0usize;
    for &(partition, offset) in offsets {
        remember_acquisition(map, topic, partition, offset, acquisition);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
}

fn acquisition_count(map: &AcquisitionMap) -> usize {
    map.values()
        .map(|runs| {
            runs.runs
                .iter()
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

async fn retire_share_socket(mut peer: ShareNodeConnection) {
    peer.conn.close();
    peer.conn.release_if_closed().await;
}

/// Drop a share leader after its final acknowledgement.
///
/// The task that sent the acknowledgement owns the socket. Dropping that
/// task aborts the idle watch, so the broker stays in its read until the
/// watch is scheduled.
async fn finish_share_peer(peer: ShareNodeConnection) {
    retire_share_socket(peer).await;
}

/// Drop every share leader socket.
///
/// Clearing the map drops each connection. The idle watch is aborted with
/// that drop, so the broker stays in its read until the watch is scheduled.
async fn discard_share_sockets(conns: &mut HashMap<i32, ShareNodeConnection>) {
    let nodes: Vec<i32> = conns.keys().copied().collect();
    for node in nodes {
        if let Some(peer) = conns.remove(&node) {
            retire_share_socket(peer).await;
        }
    }
}

/// Read share leader sockets until one closes. Closed sockets are dropped.
///
/// The sockets stay in `conns`. Cancelling this wait, which is how a retry
/// backoff ends, must not drop a leader that is still up. A queued response
/// does not count as a close.
async fn drop_closed_share_conns(conns: &mut HashMap<i32, ShareNodeConnection>) {
    if conns.is_empty() {
        std::future::pending::<()>().await;
        return;
    }
    let nodes: Vec<i32> = conns.keys().copied().collect();
    loop {
        for &node in &nodes {
            let Some(peer) = conns.get_mut(&node) else {
                continue;
            };
            let closed = tokio::select! {
                biased;
                _ = peer.conn.peer_left() => true,
                _ = tokio::time::sleep(Duration::from_millis(50)) => false,
            };
            if closed {
                let _ = conns.remove(&node);
                return;
            }
        }
    }
}

struct ShareDialConfig {
    client_id: String,
    connect_timeout: Duration,
    tls: Option<crate::net::TlsConfig>,
    sasl_plain: Option<(String, String)>,
    sasl_scram: Option<(String, String)>,
    sasl_scram_sha512: Option<(String, String)>,
    sasl_oauthbearer: Option<String>,
    sasl_oauthbearer_oidc: Option<crate::OidcConfig>,
    bootstrap_addr: String,
    known_keys: Vec<crate::protocol::api::ApiVersion>,
    /// Versions already learned for each share leader.
    node_keys: HashMap<i32, Vec<crate::protocol::api::ApiVersion>>,
    /// Versions learned for a broker address, including sibling bootstraps.
    addr_keys: HashMap<String, Vec<crate::protocol::api::ApiVersion>>,
    coord_addr: String,
    coord_keys: Vec<crate::protocol::api::ApiVersion>,
}

type ShareDialAttempt = (
    i32,
    String,
    Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)>,
);

enum ShareDialEvent {
    Joined(Box<Option<std::result::Result<ShareDialAttempt, tokio::task::JoinError>>>),
    Wake,
    Timeout,
    WatchClosed,
}

fn push_opened_share_dial(
    opened: &mut Vec<OpenedShareDial>,
    node: i32,
    addr: String,
    conn: BrokerConn,
    versions: crate::protocol::api::ApiVersionsResponse,
) {
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
    opened.push(OpenedShareDial {
        node,
        addr,
        conn,
        fetch_version,
        acknowledge_version,
        keys: versions.api_keys,
    });
}

struct OpenedShareDial {
    node: i32,
    addr: String,
    conn: BrokerConn,
    fetch_version: i16,
    acknowledge_version: i16,
    keys: Vec<crate::protocol::api::ApiVersion>,
}

struct PreparedShareAck {
    node: i32,
    version: i16,
    topics: Vec<ShareAckTopic>,
    sent: Option<Result<Bytes>>,
}

struct ShareAckFlight {
    index: usize,
    peer: ShareNodeConnection,
    sent: Result<Bytes>,
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
    /// Stale topic names from the last metadata refresh. The strings stay
    /// so the next poll does not allocate them again.
    stale_metadata_names: Vec<String>,
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
    /// Acknowledgement partitions from the previous call. Cleared and refilled.
    ack_partitions: Vec<AckPartition>,
    /// Acknowledgement-batch lists from the previous call.
    spare_ack_batches: Vec<Vec<AcknowledgementBatch>>,
    /// Partition and offset pairs for a one-topic acknowledgement. Reused
    /// across calls.
    ack_pairs: Vec<(i32, i64)>,
    /// Last ShareFetch offset seen for each partition in the current response.
    /// In-order records do not need a per-offset set.
    fetch_cursors: HashMap<i32, i64>,
    /// Record list from the previous ShareFetch. Cleared and refilled so a
    /// steady poll does not allocate the list again.
    spare_incoming: Vec<ShareRecord>,
    /// Prepared ShareFetch jobs from the previous poll. The jobs are drained;
    /// the list itself is refilled.
    spare_prepared: Vec<PreparedShareFetch>,
    /// Finished ShareFetch replies from the previous poll. Drained after they
    /// are applied, then refilled.
    spare_ready: Vec<ReadyShareFetch>,
    /// ShareFetch response shells from the previous poll. Topic, partition,
    /// record-batch, and acquired-range buffers are refilled in place.
    spare_fetched: Vec<ShareFetchedTopic>,
    /// Subscribed topic id by name. A long name is the allocation fetched records share.
    topic_ids: HashMap<Arc<str>, [u8; 16]>,
    /// Share session epoch per share-partition leader (KIP-932).
    share_epochs: HashMap<i32, i32>,
    share_conns: HashMap<i32, ShareNodeConnection>,
    share_addresses: HashMap<i32, String>,
    /// ApiVersions learned on each share leader. An idle replacement reuses them.
    share_api_keys: HashMap<i32, Vec<crate::protocol::api::ApiVersion>>,
    /// Coordinators this member already contacted.
    prior_coords: HashMap<String, crate::group::KnownCoord>,
    /// Address and ApiVersions of the group coordinator socket.
    coord_addr: String,
    coord_keys: Vec<crate::protocol::api::ApiVersion>,
    /// CurrentLeader values from the latest share response. A metadata refresh
    /// drops cached leaders, and these patches are put back afterward.
    leader_patches: Vec<ShareLeaderPatch>,
    /// Slot of each saved leader patch. A later response for the same
    /// partition replaces that slot instead of scanning the list.
    leader_patch_slots: HashMap<String, HashMap<i32, usize>>,
    /// Share leaders whose socket was dropped after a retriable fetch error.
    ///
    /// Dialed again while metadata reloads. Broker addresses in
    /// `share_addresses` survive topic invalidation.
    share_redial_nodes: Vec<i32>,
    share_redial_seen: HashSet<i32>,
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
    /// Background heartbeat task. [`Self::leave`] waits for it to stop.
    hb_task: Option<Arc<tokio::task::JoinHandle<()>>>,
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
    /// Abort handles for [`Self::inflight_share_fetches`].
    inflight_share_tasks: Vec<tokio::task::AbortHandle>,
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

struct DecodedShareFetch<'a> {
    node: i32,
    version: i16,
    by_id: &'a BTreeMap<[u8; 16], Vec<i32>>,
    fetched: &'a mut Vec<ShareFetchedTopic>,
    lock_timeout: i32,
    error_code: i16,
    decoded_bytes: usize,
    remaining: &'a mut usize,
}

/// One finished or in-flight ShareFetch. The topic list rides with the task so
/// a completed fetch can return it to the spare pool.
type ShareFetchTask = (
    i32,
    ShareNodeConnection,
    Result<Bytes>,
    Vec<ShareFetchTopic>,
);

/// Cancel every task in `set` and poll until each one leaves the runtime.
///
/// Abort only schedules cancellation. On a current-thread runtime a fetch
/// blocked in a broker read stays alive until this caller yields. The wait
/// stops at `deadline`, then a few extra turns let a task already at an
/// await notice the abort.
async fn finish_aborted_join_set<T: 'static>(mut set: tokio::task::JoinSet<T>, deadline: Instant) {
    set.abort_all();
    let mut extra = 0u32;
    while !set.is_empty() {
        let open = Instant::now() < deadline;
        if !open {
            if extra >= 8 {
                break;
            }
            extra = extra.saturating_add(1);
        }
        tokio::select! {
            biased;
            joined = set.join_next() => {
                if joined.is_none() {
                    break;
                }
            }
            () = tokio::task::yield_now() => {}
        }
    }
    // The finished dial drops its socket while this call is still in the
    // same poll. Yield so the broker observes that close before the caller
    // continues.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

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
        group
            .inflight_share_tasks
            .retain(|handle| !handle.is_finished());
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

/// A metadata name that can be stored on a share assignment.
///
/// An [`Arc`] is cloned. A [`String`] is copied once, for tests that still
/// pass owned text.
trait ShareCachedName {
    fn share_cached_name(&self) -> Arc<str>;
}

impl ShareCachedName for Arc<str> {
    fn share_cached_name(&self) -> Arc<str> {
        Arc::clone(self)
    }
}

impl ShareCachedName for String {
    fn share_cached_name(&self) -> Arc<str> {
        Arc::from(self.as_str())
    }
}

enum ResolvedShareName<'a, I> {
    Cached(&'a I),
    Only(&'a str),
}

impl<I> Copy for ResolvedShareName<'_, I> {}

impl<I> Clone for ResolvedShareName<'_, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, I: AsRef<str>> ResolvedShareName<'a, I> {
    fn as_str(self) -> &'a str {
        match self {
            Self::Cached(name) => name.as_ref(),
            Self::Only(name) => name,
        }
    }

    fn cached_arc(self) -> Arc<str>
    where
        I: ShareCachedName,
    {
        match self {
            Self::Cached(name) => name.share_cached_name(),
            Self::Only(name) => Arc::from(name),
        }
    }
}

/// Resolve a share-group assignment.
///
/// `None` means the topic ids could not be named, so the caller keeps its
/// current partitions. An empty input is an explicit revoke. Negative
/// partition indexes are omitted, and a repeated partition is stored once.
fn next_share_assignment<I, N>(
    incoming: &[ShareTopicPartitions],
    id_to_name: &HashMap<[u8; 16], I>,
    name_to_id: &HashMap<N, [u8; 16]>,
    subscribed: &[String],
) -> Option<ShareAssignmentUpdate>
where
    I: AsRef<str> + ShareCachedName,
    N: std::borrow::Borrow<str> + std::hash::Hash + Eq,
{
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
        let resolved = if let Some(name) = id_to_name.get(&tp.topic_id) {
            Some(ResolvedShareName::Cached(name))
        } else if subscribed.len() == 1 {
            // Single-topic subscribe: map the only name when metadata
            // has not yet indexed this id.
            subscribed
                .first()
                .map(|name| ResolvedShareName::Only(name.as_ref()))
        } else {
            None
        };
        let Some(resolved) = resolved else {
            continue;
        };
        let name = resolved.as_str();
        let topic_id = name_to_id.get(name).copied().unwrap_or(tp.topic_id);
        if topic_id == [0u8; 16] {
            continue;
        }
        resolved_topic = true;
        if let Some(stored) = topic_ids.get_mut(name) {
            *stored = topic_id;
        } else {
            let _ = topic_ids.insert(resolved.cached_arc(), topic_id);
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

fn refill_stale_names(dest: &mut Vec<String>, names: &[String], fresh: impl Fn(&str) -> bool) {
    let mut used = 0usize;
    for topic in names {
        if fresh(topic) {
            continue;
        }
        if let Some(slot) = dest.get_mut(used) {
            if slot != topic {
                slot.clear();
                slot.push_str(topic);
            }
        } else {
            dest.push(topic.clone());
        }
        used = used.saturating_add(1);
    }
    dest.truncate(used);
}

#[cfg(test)]
fn fill_unique_names<'a>(names: &mut Vec<String>, topics: impl Iterator<Item = &'a str>) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut used = 0usize;
    for topic in topics {
        if seen.contains(topic) {
            continue;
        }
        let _ = seen.insert(topic.to_string());
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

async fn share_nodes_for(
    assigned: &[(Arc<str>, i32)],
    leaders: &HashMap<Arc<str>, Vec<i32>>,
) -> Vec<i32> {
    let mut nodes = Vec::new();
    let mut seen = HashSet::new();
    let mut walked = 0usize;
    for (topic, partition) in assigned {
        let node = leaders
            .get(topic.as_ref())
            .and_then(|ids| {
                usize::try_from(*partition)
                    .ok()
                    .and_then(|index| ids.get(index))
            })
            .copied()
            .filter(|node| *node >= 0);
        if let Some(node) = node {
            Consumer::remember_node(&mut nodes, &mut seen, node);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    nodes
}

async fn fill_unique_topic_names(names: &mut Vec<String>, assigned: &[(Arc<str>, i32)]) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut used = 0usize;
    let mut walked = 0usize;
    for (topic, _) in assigned {
        let topic = topic.as_ref();
        if !seen.contains(topic) {
            let _ = seen.insert(topic.to_string());
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
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    names.truncate(used);
}

async fn build_share_leader_plan(
    assigned: &[(Arc<str>, i32)],
    cluster: &crate::cluster::Cluster,
    plan: &mut ShareLeaderPlan,
    leader_topic: &mut Option<Arc<str>>,
    leader_ids: &mut Vec<i32>,
) -> Result<()> {
    clear_share_leader_plan(plan);
    *leader_topic = None;
    let mut walked = 0usize;
    for (topic, partition) in assigned {
        if remember_share_leader_topic(leader_topic, topic) {
            let ids = cluster
                .leaders
                .get(topic.as_ref())
                .map(Vec::as_slice)
                .ok_or_else(|| Error::UnknownTopic(topic.as_ref().to_string()))?;
            leader_ids.clear();
            leader_ids.extend_from_slice(ids);
        }
        let node = share_partition_leader(topic, *partition, leader_ids)?;
        if !cluster.brokers.contains_key(&node) {
            return Err(Error::NoLeader {
                topic: topic.as_ref().to_owned(),
                partition: *partition,
            });
        }
        remember_share_leader(plan, node, topic, *partition);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    prune_share_leader_plan(plan);
    Ok(())
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

fn reserve_share_len<T>(items: &mut Vec<T>, count: usize) {
    if items.capacity() < count {
        items.reserve(count - items.capacity());
    }
}

fn refill_share_partitions(dest: &mut Vec<ShareFetchPartition>, parts: &[i32], max_bytes: i32) {
    reserve_share_len(dest, parts.len());
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
    reserve_share_len(topics, by_id.len());
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

impl Drop for ShareGroup {
    fn drop(&mut self) {
        // Leave waits for this task. Dropping the group without leave only
        // drops the handle, which would detach a heartbeat stuck in a broker
        // read. Abort it so the next poll of the runtime stops that call.
        if let Some(task) = self.hb_task.take() {
            task.abort();
        }
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
        let discovery_deadline = Instant::now() + cfg.request_timeout;
        let (consumer, first_coordinator) = crate::group::connect_member_and_coordinator(
            &cfg,
            &group_id,
            &topics,
            topic_match.is_some(),
            discovery_deadline,
        )
        .await?;
        // Membership uses the group coordinator (Java ShareConsumer / DescribeShareGroups).
        // CoordinatorType.SHARE is only for share-partition state keys
        // (`groupId:topicId:partition`), not the group id alone (KIP-932).
        let discovery_backoff = cfg
            .retry_backoff
            .min(cfg.retry_backoff_max)
            .max(Duration::from_millis(1));
        let (coord, coord_keys) = match first_coordinator {
            Ok(coord) => coord,
            Err(error)
                if error
                    .broker_code()
                    .is_some_and(error::coordinator_retriable) =>
            {
                loop {
                    if Instant::now() >= discovery_deadline {
                        return Err(Error::Timeout);
                    }
                    let result = crate::group::discover_coord_with_versions(
                        &cfg,
                        &group_id,
                        COORDINATOR_GROUP,
                        discovery_deadline,
                    )
                    .await
                    .map(|(conn, versions)| (conn, versions.api_keys));
                    match result {
                        Ok(coord) => break coord,
                        Err(error)
                            if error
                                .broker_code()
                                .is_some_and(error::coordinator_retriable) =>
                        {
                            let remaining =
                                discovery_deadline.saturating_duration_since(Instant::now());
                            if remaining.is_zero() {
                                return Err(Error::Timeout);
                            }
                            tokio::time::sleep(discovery_backoff.min(remaining)).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            Err(error) => return Err(error),
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
        let coord_addr = coord.addr().to_string();
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
            stale_metadata_names: Vec::new(),
            leader_plan: HashMap::new(),
            leader_nodes: Vec::new(),
            spare_fetch_topics: Vec::new(),
            spare_fetch_ids: Vec::new(),
            ack_offsets: Vec::new(),
            ack_topic: None,
            spare_ack_types: Vec::new(),
            ack_partitions: Vec::new(),
            spare_ack_batches: Vec::new(),
            ack_pairs: Vec::new(),
            fetch_cursors: HashMap::new(),
            spare_incoming: Vec::new(),
            spare_prepared: Vec::new(),
            spare_ready: Vec::new(),
            spare_fetched: Vec::new(),
            topic_ids: HashMap::new(),
            share_epochs: HashMap::new(),
            share_conns: HashMap::new(),
            share_addresses: HashMap::new(),
            share_api_keys: HashMap::new(),
            leader_patches: Vec::new(),
            leader_patch_slots: HashMap::new(),
            share_redial_nodes: Vec::new(),
            share_redial_seen: HashSet::new(),
            prior_coords: HashMap::new(),
            coord_addr,
            coord_keys,
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
            hb_task: None,
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
            inflight_share_tasks: Vec::new(),
        };
        if g.topic_match.is_some() {
            g.topics = g.matching_topic_names_within(discovery_deadline).await?;
            g.last_match_refresh = Instant::now();
            g.publish_hb_topics();
        }

        g.heartbeat_join_within(discovery_deadline).await?;

        g.hb_task = Some(g.spawn_heartbeat(hb_rx));
        Ok(g)
    }

    /// Test hook: the background heartbeat task, if one has been started.
    #[doc(hidden)]
    pub fn test_heartbeat_task(&self) -> Option<Arc<tokio::task::JoinHandle<()>>> {
        self.hb_task.as_ref().map(Arc::clone)
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
        let _supported = spoken_share_group_heartbeat(self.coord.share_group_heartbeat_version)?;

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
            // The heartbeat carries topic names. A stale image loads on the
            // member socket while the coordinator answers.
            let stale = self
                .topics
                .iter()
                .any(|topic| !self.consumer.topic_metadata_fresh(topic));
            let (version, body, metadata_refresh) = if stale {
                let bootstrap_keys = self.consumer.copy_api_keys();
                let address_keys = self.consumer.copy_learned_broker_keys();
                let coord = &mut self.coord;
                let consumer = &mut self.consumer;
                let topics = &self.topics;
                let cfg = &self.cfg;
                let group_id = self.group_id.as_str();
                let prior = &mut self.prior_coords;
                let refresh = Self::refresh_topic_names_within(consumer, topics, deadline);
                let rpc = coord_roundtrip_with_bootstrap(
                    coord,
                    cfg,
                    group_id,
                    COORDINATOR_GROUP,
                    SHARE_GROUP_HEARTBEAT,
                    |version, buf| encode_share_group_heartbeat_request(buf, version, &req),
                    &bootstrap_keys,
                    &address_keys,
                    prior,
                    timeout,
                );
                let (refresh, rpc) = tokio::join!(refresh, rpc);
                let (version, body) = rpc?;
                (version, body, Some(refresh))
            } else {
                let bootstrap_keys = self.consumer.copy_api_keys();
                let address_keys = self.consumer.copy_learned_broker_keys();
                let (version, body) = coord_roundtrip_with_bootstrap(
                    &mut self.coord,
                    &self.cfg,
                    &self.group_id,
                    COORDINATOR_GROUP,
                    SHARE_GROUP_HEARTBEAT,
                    |version, buf| encode_share_group_heartbeat_request(buf, version, &req),
                    &bootstrap_keys,
                    &address_keys,
                    &mut self.prior_coords,
                    timeout,
                )
                .await?;
                (version, body, None)
            };

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
            if let Some(Err(error)) = metadata_refresh {
                self.hb_epoch.store(self.member_epoch, Ordering::SeqCst);
                self.hb_err.store(0, Ordering::SeqCst);
                return Err(error);
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
        }
    }

    /// Metadata for `topics` when any name is stale.
    ///
    /// An empty list and a fully fresh list skip the RPC. A zero remaining
    /// budget is [`Error::Timeout`] before the request is written.
    async fn refresh_topic_names_within(
        consumer: &mut Consumer,
        topics: &[String],
        deadline: Instant,
    ) -> Result<()> {
        if topics.is_empty()
            || topics
                .iter()
                .all(|topic| consumer.topic_metadata_fresh(topic))
        {
            return Ok(());
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        consumer
            .refresh_metadata_timeout(Some(topics), timeout)
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
            parts.retain_runs(|run| still_assigned.contains(&(topic.as_str(), run.partition)));
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
        let mut recreated = Vec::new();
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
                recreated.push(Arc::clone(&topic));
            }
            let _previous = self.topic_ids.insert(topic, id);
        }
        drop_share_records_for_topics(&mut self.pending_delivery, &recreated);
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
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            crate::group::copy_member_id(&mut req.member_id, &self.member_id);
            req.member_epoch = self.member_epoch;
            reuse_share_subscription(&mut req, &self.topics);
            // The heartbeat carries topic names. A stale image loads on the
            // member socket while the coordinator answers.
            let stale = self
                .topics
                .iter()
                .any(|topic| !self.consumer.topic_metadata_fresh(topic));
            let (version, body, metadata_refresh) = if stale {
                let bootstrap_keys = self.consumer.copy_api_keys();
                let address_keys = self.consumer.copy_learned_broker_keys();
                let coord = &mut self.coord;
                let consumer = &mut self.consumer;
                let topics = &self.topics;
                let cfg = &self.cfg;
                let group_id = self.group_id.as_str();
                let prior = &mut self.prior_coords;
                let refresh = Self::refresh_topic_names_within(consumer, topics, deadline);
                let rpc = coord_roundtrip_with_bootstrap(
                    coord,
                    cfg,
                    group_id,
                    COORDINATOR_GROUP,
                    SHARE_GROUP_HEARTBEAT,
                    |version, buf| encode_share_group_heartbeat_request(buf, version, &req),
                    &bootstrap_keys,
                    &address_keys,
                    prior,
                    timeout,
                );
                let (refresh, rpc) = tokio::join!(refresh, rpc);
                let (version, body) = rpc?;
                (version, body, Some(refresh))
            } else {
                let bootstrap_keys = self.consumer.copy_api_keys();
                let address_keys = self.consumer.copy_learned_broker_keys();
                let (version, body) = coord_roundtrip_with_bootstrap(
                    &mut self.coord,
                    &self.cfg,
                    &self.group_id,
                    COORDINATOR_GROUP,
                    SHARE_GROUP_HEARTBEAT,
                    |version, buf| encode_share_group_heartbeat_request(buf, version, &req),
                    &bootstrap_keys,
                    &address_keys,
                    &mut self.prior_coords,
                    timeout,
                )
                .await?;
                (version, body, None)
            };
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
            if let Some(Err(error)) = metadata_refresh {
                return Err(error);
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
        self.prune_expired_acquisitions().await;
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
                Err(Error::Wakeup) => return Err(Error::Wakeup),
                Err(e) if share_leader_retriable(&e) || share_fetch_session_reset(&e) => {
                    if Instant::now() >= deadline {
                        if self.pending_delivery.is_empty() {
                            return Err(Error::Timeout);
                        }
                        self.drain_pending_delivery()
                    } else {
                        self.sleep_share_retry(attempt, deadline).await?;
                        attempt = attempt.saturating_add(1);
                        if Instant::now() >= deadline {
                            if self.pending_delivery.is_empty() {
                                return Err(Error::Timeout);
                            }
                            self.drain_pending_delivery()
                        } else {
                            if share_leader_retriable(&e) {
                                let patches = std::mem::take(&mut self.leader_patches);
                                self.leader_patch_slots.clear();
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
                                self.leader_patch_slots.clear();
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

    async fn prune_expired_acquisitions(&mut self) {
        let poisoned: Vec<_> = self
            .share_conns
            .iter()
            .filter(|(_, peer)| peer.conn.is_closed())
            .map(|(node, _)| *node)
            .collect();
        for node in poisoned {
            self.reset_node_session(node).await;
        }
        let now = Instant::now();
        self.acquisitions.retain(|_, parts| {
            parts.retain_runs(|run| run.acquisition.expires_at.is_none_or(|expiry| expiry > now));
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
    async fn cached_share_nodes(&self) -> Vec<i32> {
        share_nodes_for(&self.assigned, &self.consumer.cluster().leaders).await
    }

    async fn share_dial_targets(&mut self, nodes: &[i32]) -> Vec<(i32, String)> {
        let leaders = share_node_addresses(nodes, &self.assigned, |topic, part| {
            self.consumer.leader_of(topic, part).ok()
        });
        let mut dials = Vec::new();
        for &node in nodes {
            if self
                .share_conns
                .get(&node)
                .is_some_and(|peer| peer.conn.is_closed())
            {
                self.reset_node_session(node).await;
            } else if self
                .share_conns
                .get(&node)
                .is_some_and(|peer| peer.conn.idle_expired(self.cfg.connections_max_idle))
            {
                if let Some(peer) = self.share_conns.remove(&node) {
                    retire_share_socket(peer).await;
                }
            }
            if self.share_conns.contains_key(&node) {
                continue;
            }
            let Some(addr) = leaders
                .get(&node)
                .cloned()
                .or_else(|| self.share_addresses.get(&node).cloned())
            else {
                continue;
            };
            dials.push((node, addr));
        }
        dials
    }

    fn share_dial_config(&self) -> ShareDialConfig {
        ShareDialConfig {
            client_id: self.cfg.client_id.clone(),
            connect_timeout: self.cfg.connect_timeout,
            tls: self.cfg.tls.clone(),
            sasl_plain: self.cfg.sasl_plain.clone(),
            sasl_scram: self.cfg.sasl_scram.clone(),
            sasl_scram_sha512: self.cfg.sasl_scram_sha512.clone(),
            sasl_oauthbearer: self.cfg.sasl_oauthbearer.clone(),
            sasl_oauthbearer_oidc: self.cfg.sasl_oauthbearer_oidc.clone(),
            bootstrap_addr: self.consumer.bootstrap_addr().to_string(),
            known_keys: self.consumer.copy_api_keys(),
            node_keys: self.share_api_keys.clone(),
            addr_keys: self.consumer.copy_learned_broker_keys(),
            coord_addr: self.coord_addr.clone(),
            coord_keys: self.coord_keys.clone(),
        }
    }

    async fn open_share_dials(
        cfg: ShareDialConfig,
        dials: Vec<(i32, String)>,
        deadline: Instant,
        wakeup: Arc<AtomicBool>,
        mut wakeup_rx: watch::Receiver<bool>,
    ) -> Vec<OpenedShareDial> {
        if dials.is_empty() || Instant::now() >= deadline {
            return Vec::new();
        }
        let mut waiting = dials.into_iter();
        let mut inflight = tokio::task::JoinSet::<ShareDialAttempt>::new();
        let spawn =
            |inflight: &mut tokio::task::JoinSet<ShareDialAttempt>, node: i32, addr: String| {
                let cfg = ShareDialConfig {
                    client_id: cfg.client_id.clone(),
                    connect_timeout: cfg.connect_timeout,
                    tls: cfg.tls.clone(),
                    sasl_plain: cfg.sasl_plain.clone(),
                    sasl_scram: cfg.sasl_scram.clone(),
                    sasl_scram_sha512: cfg.sasl_scram_sha512.clone(),
                    sasl_oauthbearer: cfg.sasl_oauthbearer.clone(),
                    sasl_oauthbearer_oidc: cfg.sasl_oauthbearer_oidc.clone(),
                    bootstrap_addr: cfg.bootstrap_addr.clone(),
                    known_keys: cfg.known_keys.clone(),
                    node_keys: cfg.node_keys.clone(),
                    addr_keys: cfg.addr_keys.clone(),
                    coord_addr: cfg.coord_addr.clone(),
                    coord_keys: cfg.coord_keys.clone(),
                };
                let addr_for_task = addr.clone();
                let reused = if addr_for_task == cfg.bootstrap_addr && !cfg.known_keys.is_empty() {
                    Some(cfg.known_keys.clone())
                } else if addr_for_task == cfg.coord_addr && !cfg.coord_keys.is_empty() {
                    Some(cfg.coord_keys.clone())
                } else if let Some(keys) = cfg.addr_keys.get(&addr_for_task).cloned() {
                    Some(keys)
                } else {
                    cfg.node_keys.get(&node).cloned()
                };
                let _ = inflight.spawn(async move {
                    let result = if let Some(keys) = reused {
                        Consumer::open_fetch_conn_known(
                            &addr_for_task,
                            &cfg.client_id,
                            cfg.connect_timeout,
                            cfg.tls.as_ref(),
                            cfg.sasl_plain.as_ref(),
                            cfg.sasl_scram.as_ref(),
                            cfg.sasl_scram_sha512.as_ref(),
                            cfg.sasl_oauthbearer.as_deref(),
                            cfg.sasl_oauthbearer_oidc.as_ref(),
                            &keys,
                            deadline,
                        )
                        .await
                    } else {
                        Consumer::open_fetch_conn(
                            &addr_for_task,
                            &cfg.client_id,
                            cfg.connect_timeout,
                            cfg.tls.as_ref(),
                            cfg.sasl_plain.as_ref(),
                            cfg.sasl_scram.as_ref(),
                            cfg.sasl_scram_sha512.as_ref(),
                            cfg.sasl_oauthbearer.as_deref(),
                            cfg.sasl_oauthbearer_oidc.as_ref(),
                            deadline,
                        )
                        .await
                    };
                    (node, addr, result)
                });
            };
        while inflight.len() < 8 {
            let Some((node, addr)) = waiting.next() else {
                break;
            };
            spawn(&mut inflight, node, addr);
        }
        let mut opened = Vec::new();
        let mut watch_live = true;
        while !inflight.is_empty() {
            if wakeup.load(Ordering::SeqCst) || Instant::now() >= deadline {
                Self::keep_completed_share_dials(&mut inflight, &mut opened);
                finish_aborted_join_set(std::mem::take(&mut inflight), Instant::now()).await;
                break;
            }
            let rest = deadline.saturating_duration_since(Instant::now());
            let event = tokio::select! {
                biased;
                joined = inflight.join_next() => ShareDialEvent::Joined(Box::new(joined)),
                result = wakeup_rx.wait_for(|on| *on), if watch_live => {
                    match result {
                        Ok(flag) => {
                            drop(flag);
                            ShareDialEvent::Wake
                        }
                        Err(_) => ShareDialEvent::WatchClosed,
                    }
                }
                () = tokio::time::sleep(rest) => ShareDialEvent::Timeout,
            };
            let joined = match event {
                ShareDialEvent::Joined(joined) => *joined,
                ShareDialEvent::WatchClosed => {
                    watch_live = false;
                    continue;
                }
                ShareDialEvent::Wake | ShareDialEvent::Timeout => {
                    Self::keep_completed_share_dials(&mut inflight, &mut opened);
                    finish_aborted_join_set(std::mem::take(&mut inflight), Instant::now()).await;
                    break;
                }
            };
            let Some(Ok((node, addr, result))) = joined else {
                break;
            };
            if let Ok((conn, versions)) = result {
                push_opened_share_dial(&mut opened, node, addr, conn, versions);
            }
            while inflight.len() < 8 {
                let Some((node, addr)) = waiting.next() else {
                    break;
                };
                spawn(&mut inflight, node, addr);
            }
        }
        opened
    }

    /// Keep share-leader sockets that already finished before the rest of the wave is aborted.
    fn keep_completed_share_dials(
        inflight: &mut tokio::task::JoinSet<ShareDialAttempt>,
        opened: &mut Vec<OpenedShareDial>,
    ) {
        while let Some(joined) = inflight.try_join_next() {
            let Ok((node, addr, Ok((conn, versions)))) = joined else {
                continue;
            };
            push_opened_share_dial(opened, node, addr, conn, versions);
        }
    }

    fn store_opened_share_dials(&mut self, opened: Vec<OpenedShareDial>) {
        for dial in opened {
            let _ = self.share_addresses.insert(dial.node, dial.addr);
            let _stored = self.share_api_keys.insert(dial.node, dial.keys);
            let _ = self.share_conns.insert(
                dial.node,
                ShareNodeConnection {
                    conn: dial.conn,
                    fetch_version: dial.fetch_version,
                    acknowledge_version: dial.acknowledge_version,
                },
            );
        }
    }

    async fn warm_share_leaders(&mut self, nodes: &[i32], deadline: Instant) {
        let dials = self.share_dial_targets(nodes).await;
        let wakeup = self.consumer.wakeup_flag();
        let wakeup_rx = self.consumer.subscribe_wakeup();
        let opened =
            Self::open_share_dials(self.share_dial_config(), dials, deadline, wakeup, wakeup_rx)
                .await;
        self.store_opened_share_dials(opened);
    }

    async fn node_share_version_until(
        &mut self,
        node: i32,
        api: i16,
        deadline: Instant,
        leaders: Option<&HashMap<i32, String>>,
    ) -> Result<i16> {
        // A failed handshake drops its socket in this call. Yield so the
        // broker observes that close before the error returns.
        let resolved = async {
            if self
                .share_conns
                .get(&node)
                .is_some_and(|peer| peer.conn.is_closed())
            {
                self.reset_node_session(node).await;
            } else if self
                .share_conns
                .get(&node)
                .is_some_and(|peer| peer.conn.idle_expired(self.cfg.connections_max_idle))
            {
                // A clean idle reconnect preserves broker-side member/session state.
                // Cancelled or failed roundtrips instead invalidate ambiguous locks.
                if let Some(peer) = self.share_conns.remove(&node) {
                    retire_share_socket(peer).await;
                }
            }
            if let std::collections::hash_map::Entry::Vacant(entry) = self.share_conns.entry(node) {
                let scanned = if leaders.is_none() {
                    self.assigned.iter().find_map(|(topic, part)| {
                        self.consumer
                            .leader_of(topic, *part)
                            .ok()
                            .filter(|(id, _)| *id == node)
                            .map(|(_, addr)| addr)
                    })
                } else {
                    None
                };
                let addr = leaders
                    .and_then(|leaders| leaders.get(&node).cloned())
                    .or(scanned)
                    .or_else(|| self.share_addresses.get(&node).cloned())
                    .ok_or_else(|| Error::protocol(format!("unknown share broker {node}")))?;
                let rest = deadline.saturating_duration_since(Instant::now());
                if rest.is_zero() {
                    return Err(Error::Timeout);
                }
                let reuse = addr == self.consumer.bootstrap_addr() && self.consumer.has_api_keys();
                let remembered = if reuse {
                    None
                } else if addr == self.coord_addr && !self.coord_keys.is_empty() {
                    Some(self.coord_keys.clone())
                } else if let Some(keys) = self.consumer.learned_keys_for(&addr) {
                    Some(keys)
                } else {
                    self.share_api_keys.get(&node).cloned()
                };
                let (mut conn, versions, already_authenticated) = if reuse {
                    let opened = self.consumer.open_leader_socket(&addr, deadline).await?;
                    (opened.0, opened.1, true)
                } else if let Some(keys) = remembered {
                    let opened = Consumer::open_fetch_conn_known(
                        &addr,
                        &self.cfg.client_id,
                        self.cfg.connect_timeout,
                        self.cfg.tls.as_ref(),
                        self.cfg.sasl_plain.as_ref(),
                        self.cfg.sasl_scram.as_ref(),
                        self.cfg.sasl_scram_sha512.as_ref(),
                        self.cfg.sasl_oauthbearer.as_deref(),
                        self.cfg.sasl_oauthbearer_oidc.as_ref(),
                        &keys,
                        deadline,
                    )
                    .await?;
                    (opened.0, opened.1, true)
                } else {
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
                    let versions =
                        crate::protocol::api::negotiate_api_versions(&mut conn, rest).await?;
                    (conn, versions, false)
                };
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
                if !already_authenticated {
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
                }
                let _stored = self.share_api_keys.insert(node, versions.api_keys);
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
        .await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        resolved
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

    /// A share group that already holds `leader`, for tests of socket closes.
    #[cfg(test)]
    fn test_holding(consumer: Consumer, coord: BrokerConn, leader: BrokerConn) -> Self {
        let (hb_stop, _) = watch::channel(false);
        let mut share_conns = HashMap::new();
        let _stored = share_conns.insert(
            1,
            ShareNodeConnection {
                conn: leader,
                fetch_version: 0,
                acknowledge_version: 0,
            },
        );
        Self {
            consumer,
            coord,
            cfg: ConsumerConfig::default(),
            group_id: "g".to_string(),
            member_id: "m".to_string(),
            member_epoch: 0,
            topics: Vec::new(),
            topic_match: None,
            last_match_refresh: Instant::now(),
            assigned: Vec::new(),
            leader_topic: None,
            leader_ids: Vec::new(),
            metadata_names: Vec::new(),
            stale_metadata_names: Vec::new(),
            leader_plan: HashMap::new(),
            leader_nodes: Vec::new(),
            spare_fetch_topics: Vec::new(),
            spare_fetch_ids: Vec::new(),
            ack_offsets: Vec::new(),
            ack_topic: None,
            spare_ack_types: Vec::new(),
            ack_partitions: Vec::new(),
            spare_ack_batches: Vec::new(),
            ack_pairs: Vec::new(),
            fetch_cursors: HashMap::new(),
            spare_incoming: Vec::new(),
            spare_prepared: Vec::new(),
            spare_ready: Vec::new(),
            spare_fetched: Vec::new(),
            topic_ids: HashMap::new(),
            share_epochs: HashMap::new(),
            share_conns,
            share_addresses: HashMap::new(),
            share_api_keys: HashMap::new(),
            leader_patches: Vec::new(),
            leader_patch_slots: HashMap::new(),
            share_redial_nodes: Vec::new(),
            share_redial_seen: HashSet::new(),
            prior_coords: HashMap::new(),
            coord_addr: String::new(),
            coord_keys: Vec::new(),
            acquisitions: HashMap::new(),
            pending_delivery: Vec::new(),
            acquire_mode: ShareAcquireMode::BatchOptimized,
            acquisition_lock_timeout_ms: None,
            hb_err: Arc::new(AtomicI16::new(0)),
            hb_epoch: Arc::new(AtomicI32::new(0)),
            hb_member_id: Arc::new(Mutex::new(String::new())),
            hb_topics: Arc::new(Mutex::new(Vec::new())),
            hb_assignment: Arc::new(Mutex::new(None)),
            hb_interval_ms: Arc::new(AtomicI32::new(0)),
            hb_deadline: Arc::new(Mutex::new(None)),
            hb_wake: Arc::new(Notify::new()),
            hb_stop,
            hb_task: None,
            fetch_rounds: 0,
            records_fetched: 0,
            bytes_fetched: 0,
            fetch_errors: 0,
            records_acknowledged: 0,
            fetch_latency: crate::metrics::LatencyTracker::new(),
            topic_metrics: HashMap::new(),
            fetch_group_id: Arc::<str>::from("g"),
            fetch_member_id: Arc::<str>::from("m"),
            inflight_share_fetches: None,
            inflight_share_nodes: HashMap::new(),
            inflight_share_tasks: Vec::new(),
        }
    }

    async fn reset_node_session(&mut self, node: i32) {
        let _ = self.share_epochs.remove(&node);
        if let Some(peer) = self.share_conns.remove(&node) {
            retire_share_socket(peer).await;
        }
        self.acquisitions.retain(|_, parts| {
            parts.retain_runs(|run| run.acquisition.node != node);
            !parts.is_empty()
        });
        self.pending_delivery.retain(|r| {
            acquisition_at(&self.acquisitions, &r.topic, r.partition, r.offset).is_some()
        });
        self.consumer.drop_node(node).await;
    }

    fn note_share_redial(&mut self, node: i32) {
        if node < 0 || !self.share_redial_seen.insert(node) {
            return;
        }
        self.share_redial_nodes.push(node);
    }

    async fn refresh_assigned_metadata(&mut self, timeout: Duration) -> Result<()> {
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let deadline = Instant::now().checked_add(timeout).ok_or(Error::Timeout)?;
        let mut nodes = self.cached_share_nodes().await;
        let mut seen: HashSet<i32> = nodes.iter().copied().collect();
        for node in self.share_redial_nodes.drain(..) {
            Consumer::remember_node(&mut nodes, &mut seen, node);
        }
        self.share_redial_seen.clear();
        // Capture addresses before leaders are dropped. A leader that moves is
        // opened on the next fetch after metadata and CurrentLeader patches.
        let targets = self.share_dial_targets(&nodes).await;
        for t in &self.topics {
            self.consumer.invalidate_topic(t);
        }
        if targets.is_empty() {
            return self
                .consumer
                .refresh_metadata_timeout(Some(&self.topics), timeout)
                .await;
        }
        let wakeup = self.consumer.wakeup_flag();
        let wakeup_rx = self.consumer.subscribe_wakeup();
        let opened = Self::open_share_dials(
            self.share_dial_config(),
            targets,
            deadline,
            wakeup,
            wakeup_rx,
        );
        let refreshed = self
            .consumer
            .refresh_metadata_timeout(Some(&self.topics), timeout);
        let (opened, refreshed) = tokio::join!(opened, refreshed);
        self.store_opened_share_dials(opened);
        refreshed
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
        fill_unique_topic_names(&mut self.metadata_names, &self.assigned).await;
        let mut stale = std::mem::take(&mut self.stale_metadata_names);
        refill_stale_names(&mut stale, &self.metadata_names, |topic| {
            self.consumer.topic_metadata_fresh(topic)
        });
        if !stale.is_empty() {
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                self.stale_metadata_names = stale;
                return Err(Error::Timeout);
            }
            // Cached leaders can be dialed while metadata reloads. A leader
            // that moves is opened afterward by the normal warm path.
            let cached = self.cached_share_nodes().await;
            let targets = self.share_dial_targets(&cached).await;
            if targets.is_empty() {
                let refreshed = self
                    .consumer
                    .refresh_metadata_timeout(Some(&stale), rest)
                    .await;
                self.stale_metadata_names = stale;
                refreshed?;
            } else {
                let wakeup = self.consumer.wakeup_flag();
                let wakeup_rx = self.consumer.subscribe_wakeup();
                let opened = Self::open_share_dials(
                    self.share_dial_config(),
                    targets,
                    deadline,
                    wakeup,
                    wakeup_rx,
                );
                let refreshed = self.consumer.refresh_metadata_timeout(Some(&stale), rest);
                let (opened, refreshed) = tokio::join!(opened, refreshed);
                self.store_opened_share_dials(opened);
                self.stale_metadata_names = stale;
                refreshed?;
            }
        } else {
            self.stale_metadata_names = stale;
        }
        build_share_leader_plan(
            &self.assigned,
            self.consumer.cluster(),
            &mut self.leader_plan,
            &mut self.leader_topic,
            &mut self.leader_ids,
        )
        .await
    }

    /// Test hook: share fetches whose replies have not been applied yet.
    #[doc(hidden)]
    #[must_use]
    pub fn test_inflight_share_fetches(&self) -> Vec<tokio::task::AbortHandle> {
        self.inflight_share_tasks.clone()
    }

    /// Stop share reads cancelled by wakeup.
    ///
    /// Abort only schedules cancellation. On a current-thread runtime a fetch
    /// blocked in a broker read stays alive until this caller yields.
    async fn stop_share_fetch_flight(&mut self, mut inflight: InflightShareFetches) {
        let nodes: Vec<i32> = inflight.pending.keys().copied().collect();
        let Some(set) = inflight.set.take() else {
            self.inflight_share_nodes.clear();
            return;
        };
        self.inflight_share_nodes.clear();
        for node in nodes {
            // The fetch task owns the socket. Aborting it drops that socket,
            // so the session cannot be acknowledged on it.
            self.reset_node_session(node).await;
        }
        finish_aborted_join_set(set, Instant::now()).await;
    }

    async fn finish_inflight_share_fetches(&mut self, deadline: Instant) {
        let Some(set) = self.inflight_share_fetches.take() else {
            self.inflight_share_nodes.clear();
            return;
        };
        self.inflight_share_nodes.clear();
        finish_aborted_join_set(set, deadline).await;
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
                let handle = set.spawn(async move {
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
                self.inflight_share_tasks.push(handle);
            }
        } else {
            self.park_prepared(prepared);
            return Err(Error::protocol("inflight share fetch set missing"));
        }
        let mut first_reply: Option<Instant> = None;
        let wakeup = self.consumer.wakeup_flag();
        let mut wakeup_rx = self.consumer.subscribe_wakeup();
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
            if wakeup.load(Ordering::SeqCst) {
                self.stop_share_fetch_flight(inflight).await;
                return Err(Error::Wakeup);
            }
            let Some(set) = inflight.set.as_mut() else {
                break;
            };
            let woken = tokio::select! {
                biased;
                joined = set.join_next() => Some(joined),
                result = wakeup_rx.wait_for(|on| *on) => {
                    drop(result);
                    None
                }
                () = tokio::time::sleep(wait) => Some(None),
            };
            let Some(joined) = woken else {
                self.stop_share_fetch_flight(inflight).await;
                return Err(Error::Wakeup);
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
        let leaders = share_node_addresses(&self.leader_nodes, &self.assigned, |topic, part| {
            self.consumer.leader_of(topic, part).ok()
        });
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
                .node_share_version_until(node, SHARE_FETCH, deadline, Some(&leaders))
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
                    if share_leader_retriable(&error) {
                        self.note_share_redial(node);
                    }
                    self.reset_node_session(node).await;
                    self.park_share_ids(by_id);
                    if matches!(error, Error::Timeout) && !self.pending_delivery.is_empty() {
                        return Ok(self.drain_pending_delivery());
                    }
                    return Err(error);
                }
            };
            let scratch = std::mem::take(&mut self.spare_fetched);
            let ((mut fetched, endpoints, _, _, lock_timeout, error_code), decoded_bytes, retained) =
                match decode_share_fetch_response_reusing_cooperative(
                    &mut body,
                    version,
                    remaining_bytes,
                    scratch,
                )
                .await
                {
                    Ok(reply) => reply,
                    Err(error) => {
                        self.reset_node_session(node).await;
                        return Err(error);
                    }
                };
            self.consumer.apply_node_endpoints(&endpoints);
            if !body.is_empty() {
                park_fetched_topics(&mut self.spare_fetched, fetched);
                park_fetched_topics(&mut self.spare_fetched, retained);
                self.reset_node_session(node).await;
                return Err(Error::protocol("trailing ShareFetch response bytes"));
            }
            let applied = self
                .apply_decoded_share_fetch(DecodedShareFetch {
                    node,
                    version,
                    by_id: &by_id,
                    fetched: &mut fetched,
                    lock_timeout,
                    error_code,
                    decoded_bytes,
                    remaining: &mut remaining_bytes,
                })
                .await;
            park_fetched_topics(&mut self.spare_fetched, fetched);
            park_fetched_topics(&mut self.spare_fetched, retained);
            applied?;
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
        let deadline = Instant::now() + self.cfg.request_timeout;
        self.hb_stop.send(true).unwrap_or(());
        let heartbeat = self.hb_task.take();
        if self.member_id.is_empty() {
            self.topic_match = None;
            self.topics.clear();
            self.publish_hb_topics();
            self.assigned.clear();
            self.topic_ids.clear();
            self.share_epochs.clear();
            discard_share_sockets(&mut self.share_conns).await;
            self.acquisitions.clear();
            self.pending_delivery.clear();
            *self.hb_assignment.lock() = None;
            *self.hb_deadline.lock() = None;
            self.hb_interval_ms.store(0, Ordering::SeqCst);
            if let Some(heartbeat) = heartbeat {
                finish_share_heartbeat(heartbeat, deadline).await;
            }
            return Ok(());
        }
        self.topic_match = None;
        *self.hb_deadline.lock() = None;
        self.hb_interval_ms.store(0, Ordering::SeqCst);
        self.finish_inflight_share_fetches(deadline).await;
        let (set, prepare_error) = self.spawn_final_share_acks(deadline).await;
        let close = Self::finish_final_share_acks(set, prepare_error);
        let leave = self.leave_coordinator(deadline);
        let (close, leave) = tokio::join!(close, leave);
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
        if let Some(heartbeat) = heartbeat {
            finish_share_heartbeat(heartbeat, deadline).await;
        }
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
        if rejoining && self.share_close_sockets_are_open() {
            // Final acknowledgements use the leader sockets. Metadata for the
            // new names uses the member socket, so both fit in one budget.
            self.finish_inflight_share_fetches(deadline).await;
            let (set, prepare_error) = self.spawn_final_share_acks(deadline).await;
            let close = Self::finish_final_share_acks(set, prepare_error);
            let refresh = Self::refresh_topic_names_within(&mut self.consumer, &topics, deadline);
            let (close, refresh) = tokio::join!(close, refresh);
            close?;
            self.assigned.clear();
            self.topic_ids.clear();
            self.topics = topics;
            self.publish_hb_topics();
            refresh?;
            self.heartbeat_join_within(deadline).await?;
            return Ok(());
        }
        if rejoining {
            // Idle leader sockets are dialed again before the final
            // acknowledgement. That handshake, a stale metadata load, and an
            // idle coordinator reconnect use different sockets.
            let open: Vec<i32> = self
                .share_epochs
                .iter()
                .filter(|(_, epoch)| **epoch > 0)
                .map(|(node, _)| *node)
                .collect();
            let metadata_stale = topics
                .iter()
                .any(|topic| !self.consumer.topic_metadata_fresh(topic));
            let coord_idle = self.coord.idle_expired(self.cfg.connections_max_idle);
            let targets = if (metadata_stale || coord_idle) && !open.is_empty() {
                self.share_dial_targets(&open).await
            } else {
                Vec::new()
            };
            if !targets.is_empty() {
                let known = coord_idle.then(|| crate::group::KnownCoord::capture(&self.coord));
                let cfg = self.cfg.clone();
                let wakeup = self.consumer.wakeup_flag();
                let wakeup_rx = self.consumer.subscribe_wakeup();
                let opened = Self::open_share_dials(
                    self.share_dial_config(),
                    targets,
                    deadline,
                    wakeup,
                    wakeup_rx,
                );
                let refreshed =
                    Self::refresh_topic_names_within(&mut self.consumer, &topics, deadline);
                let coord_opened = async move {
                    match known {
                        Some(known) => crate::group::connect_known_coord(&cfg, &known).await.ok(),
                        None => None,
                    }
                };
                let (opened, refreshed, coord_opened) =
                    tokio::join!(opened, refreshed, coord_opened);
                self.store_opened_share_dials(opened);
                if let Some(conn) = coord_opened {
                    self.coord = conn;
                }
                self.close_share_session(deadline).await?;
                self.assigned.clear();
                self.topic_ids.clear();
                self.share_epochs.clear();
                self.topics = topics;
                self.publish_hb_topics();
                refreshed?;
                self.heartbeat_join_within(deadline).await?;
                return Ok(());
            }
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
        self.hb_task = Some(self.spawn_heartbeat(hb_rx));
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

    async fn apply_decoded_share_fetch(&mut self, fetch: DecodedShareFetch<'_>) -> Result<()> {
        let DecodedShareFetch {
            node,
            version,
            by_id,
            fetched,
            lock_timeout,
            error_code,
            decoded_bytes,
            remaining,
        } = fetch;
        if error_code != 0 {
            let error = Error::broker(error_code, "ShareFetch");
            if share_fetch_session_reset(&error) || share_leader_retriable(&error) {
                if share_leader_retriable(&error) {
                    self.note_share_redial(node);
                }
                self.reset_node_session(node).await;
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
                self.reset_node_session(node).await;
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
        let mut spilled_offsets: HashMap<i32, HashSet<i64>> = HashMap::new();
        let mut noted = 0usize;
        for topic in fetched.iter_mut() {
            let requested = by_id
                .get(&topic.topic_id)
                .ok_or_else(|| Error::protocol("ShareFetch returned an unrequested topic id"))?;
            let allowed = share_partition_set(requested).await;
            let topic_name = self.fetched_topic_name(topic.topic_id)?;
            let name = topic_name.as_str();
            let mut walked = 0usize;
            for part in &mut topic.partitions {
                if !allowed.contains(&part.partition) {
                    return Err(Error::protocol(
                        "ShareFetch returned an unrequested partition",
                    ));
                }
                walked = walked.saturating_add(1);
                if walked == 8 * 1024 {
                    walked = 0;
                    tokio::task::yield_now().await;
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
                        remember_share_leader_patch(
                            &mut self.leader_patches,
                            &mut self.leader_patch_slots,
                            patch,
                        );
                    } else if share_ack_ownership_lost(part.error_code) {
                        self.consumer.invalidate_topic(name);
                        self.leader_patches.retain(|patch| {
                            patch.topic != name || patch.partition != part.partition
                        });
                        self.leader_patch_slots = index_share_leader_patches(&self.leader_patches);
                    }
                    let error = Error::broker(
                        part.error_code,
                        format!("ShareFetch {name}-{}", part.partition),
                    );
                    if share_session_reset(&error) || share_leader_retriable(&error) {
                        if share_leader_retriable(&error) {
                            self.note_share_redial(node);
                        }
                        self.reset_node_session(node).await;
                    }
                    return Err(error);
                }
                if part.acknowledge_error_code != 0 {
                    return Err(Error::broker(
                        part.acknowledge_error_code,
                        format!("ShareFetch acknowledgement {name}-{}", part.partition),
                    ));
                }
                let partition_index = part.partition;
                let records = &mut part.records;
                let acquired = &mut part.acquired;
                acquired.sort_unstable_by_key(|range| range.first_offset);
                for range in acquired.iter() {
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
                let declared = records
                    .iter()
                    .fold(0usize, |sum, batch| sum.saturating_add(batch.records.len()));
                reserve_share_delivery(&mut incoming, declared);
                {
                    let mut range_lookup = acquired::AcquisitionRanges::new(acquired);
                    for batch in records.iter_mut() {
                        let timestamp_type = batch.timestamp_type();
                        let log_append_timestamp = (timestamp_type == TimestampType::LogAppendTime)
                            .then_some(batch.max_timestamp());
                        let leader_epoch = (batch.partition_leader_epoch >= 0)
                            .then_some(batch.partition_leader_epoch);
                        for record in batch.records.drain(..) {
                            let Some(delivery_count) = range_lookup.delivery_count(record.offset)
                            else {
                                continue;
                            };
                            if !note_share_offset(
                                &mut self.fetch_cursors,
                                &mut spilled_offsets,
                                partition_index,
                                record.offset,
                            ) {
                                return Err(Error::protocol("duplicate ShareFetch record offset"));
                            }
                            noted = noted.saturating_add(1);
                            if noted == 8 * 1024 {
                                noted = 0;
                                tokio::task::yield_now().await;
                            }
                            if let Some(previous) = acquisition_at(
                                &self.acquisitions,
                                &topic_name,
                                partition_index,
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
                                partition: partition_index,
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
                acquired.clear();
            }
        }
        let retained_bytes = share_delivery_accounted_bytes(&incoming, &self.acquisitions).await;
        *remaining = match remaining.checked_sub(retained_bytes.max(decoded_bytes)) {
            Some(left) => left,
            None => {
                self.reset_node_session(node).await;
                park_record_buf(&mut self.spare_incoming, incoming);
                return Err(Error::protocol(
                    "ShareFetch retained-record budget exceeded",
                ));
            }
        };
        reserve_share_delivery(&mut self.pending_delivery, incoming.len());
        let mut remembered = 0usize;
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
            remembered = remembered.saturating_add(1);
            if remembered == 8 * 1024 {
                remembered = 0;
                tokio::task::yield_now().await;
            }
        }
        park_record_buf(&mut self.spare_incoming, incoming);
        Ok(())
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

    fn name_for_topic_id(&self, id: [u8; 16]) -> Result<Arc<str>> {
        share_topic_by_id(&self.topic_ids, id).map(Arc::clone)
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
        self.prune_expired_acquisitions().await;
        reject_stale_share_acks(&self.acquisitions, recs).await?;
        let mut partitions = std::mem::take(&mut self.ack_partitions);
        acknowledgement_batches_with(
            recs,
            ack,
            &mut self.ack_offsets,
            &mut self.ack_pairs,
            &mut self.ack_topic,
            &mut self.spare_ack_types,
            &mut partitions,
            &mut self.spare_ack_batches,
        )
        .await;
        let deadline = Instant::now() + self.cfg.request_timeout;
        let mut attempt = 0u32;
        let result = loop {
            match self
                .acknowledge_leaders(&mut partitions, ack, deadline)
                .await
            {
                Ok(()) => break Ok(()),
                Err(error) if share_ack_retriable(&error) && !partitions.is_empty() => {
                    if Instant::now() >= deadline {
                        break Err(Error::Timeout);
                    }
                    if let Err(error) = self.sleep_share_retry(attempt, deadline).await {
                        break Err(error);
                    }
                    attempt = attempt.saturating_add(1);
                    if Instant::now() >= deadline {
                        break Err(Error::Timeout);
                    }
                }
                Err(error) => break Err(error),
            }
        };
        self.park_ack_partitions(partitions);
        result
    }

    fn park_ack_partitions(&mut self, mut partitions: Vec<AckPartition>) {
        partitions.clear();
        if partitions.capacity() >= self.ack_partitions.capacity() {
            self.ack_partitions = partitions;
        }
    }

    fn park_ack_batches(&mut self, mut batches: Vec<AcknowledgementBatch>) {
        batches.clear();
        if batches.capacity() > 0 {
            self.spare_ack_batches.push(batches);
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
            .node_share_version_until(node, SHARE_ACKNOWLEDGE, deadline, None)
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
        let batches = partitions
            .first()
            .map(|part| part.2.as_slice())
            .unwrap_or(&[]);
        let mut encoded = BytesMut::new();
        encode_one_share_acknowledge_cooperative(
            &mut encoded,
            version,
            &self.group_id,
            &self.member_id,
            epoch,
            &topic_id,
            partition,
            batches,
        )
        .await?;
        let payload = encoded.freeze();
        let Some(mut peer) = self.share_conns.remove(&node) else {
            self.reset_node_session(node).await;
            return Err(Error::protocol("missing share connection"));
        };
        let sent = peer
            .conn
            .roundtrip(
                SHARE_ACKNOWLEDGE,
                version,
                |buf| {
                    buf.extend_from_slice(&payload);
                    Ok(())
                },
                timeout,
            )
            .await;
        let _ = self.share_conns.insert(node, peer);
        let mut body = match sent {
            Ok(body) => body,
            Err(error) => {
                self.reset_node_session(node).await;
                return Err(error);
            }
        };
        let (code, responses, endpoints, _, _, lock_timeout) =
            match decode_share_acknowledge_topics_response_with_lock_timeout(&mut body, version) {
                Ok(reply) => reply,
                Err(error) => {
                    self.reset_node_session(node).await;
                    return Err(error);
                }
            };
        self.consumer.apply_node_endpoints(&endpoints);
        if !body.is_empty() {
            self.reset_node_session(node).await;
            return Err(Error::protocol("trailing ShareAcknowledge response bytes"));
        }
        if code != 0 {
            let error = Error::broker(code, "ShareAcknowledge");
            if share_session_reset(&error) {
                self.reset_node_session(node).await;
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
                remember_share_leader_patch(
                    &mut self.leader_patches,
                    &mut self.leader_patch_slots,
                    patch,
                );
            }
        }
        let sent_batches = partitions
            .first()
            .map(|item| item.2.as_slice())
            .unwrap_or(&[]);
        if part.error_code == 0 {
            let count = self
                .acquisitions
                .get(name.as_ref())
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
                if let Some(parts) = self.acquisitions.get_mut(name.as_ref()) {
                    renew_covered_acquisitions(parts, part.partition, sent_batches, expiry);
                }
            } else {
                let empty = if let Some(parts) = self.acquisitions.get_mut(name.as_ref()) {
                    drop_covered_acquisitions(parts, part.partition, sent_batches);
                    parts.is_empty()
                } else {
                    false
                };
                if empty {
                    let _ = self.acquisitions.remove(name.as_ref());
                }
                self.records_acknowledged = self
                    .records_acknowledged
                    .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
            }
            let mut batches = partitions
                .first_mut()
                .map(|item| std::mem::take(&mut item.2))
                .unwrap_or_default();
            for batch in batches.drain(..) {
                if batch.types.capacity() > 0 {
                    self.spare_ack_types.push(batch.types);
                }
            }
            self.park_ack_batches(batches);
            partitions.clear();
            Ok(())
        } else {
            if part.error_code == error::INVALID_RECORD_STATE
                || share_ack_ownership_lost(part.error_code)
            {
                let empty = if let Some(parts) = self.acquisitions.get_mut(name.as_ref()) {
                    drop_covered_acquisitions(parts, part.partition, sent_batches);
                    parts.is_empty()
                } else {
                    false
                };
                if empty {
                    let _ = self.acquisitions.remove(name.as_ref());
                }
            }
            let error = Error::broker(
                part.error_code,
                format!("ShareAcknowledge {name}-{}", part.partition),
            );
            if part.error_code == error::SHARE_SESSION_NOT_FOUND
                || part.error_code == error::INVALID_SHARE_SESSION_EPOCH
            {
                self.reset_node_session(node).await;
            }
            Err(error)
        }
    }

    /// Wait out a retry, and drop a leader socket that closes during it.
    async fn sleep_share_retry(&mut self, attempt: u32, deadline: Instant) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        let delay = crate::config::retry_backoff_delay(
            self.cfg.retry_backoff,
            self.cfg.retry_backoff_max,
            attempt,
        );
        if delay.is_zero() || Instant::now() >= deadline {
            return Ok(());
        }
        let rest = delay.min(deadline.saturating_duration_since(Instant::now()));
        let sleep = tokio::time::sleep(rest);
        tokio::pin!(sleep);
        let mut wakeup = self.consumer.subscribe_wakeup();
        loop {
            tokio::select! {
                biased;
                result = wakeup.wait_for(|on| *on) => {
                    drop(result);
                    return Err(Error::Wakeup);
                }
                () = &mut sleep => return Ok(()),
                () = self.drop_closed_share_sockets() => {}
            }
        }
    }

    /// Read share leader sockets until one closes. Closed sockets are dropped.
    async fn drop_closed_share_sockets(&mut self) {
        drop_closed_share_conns(&mut self.share_conns).await;
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
            let grouped = group_acks_from_acquisitions(&self.acquisitions, partitions).await?;
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
        if nodes.len() > 1 {
            return self
                .acknowledge_leaders_together(nodes, partitions, ack, deadline)
                .await;
        }
        let mut first_error = None;
        for (node, node_tps) in nodes {
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                // Brokers already acknowledged stay acknowledged. The ones not
                // sent remain in `partitions` for the caller.
                return Err(Error::Timeout);
            }
            let version = self
                .node_share_version_until(node, SHARE_ACKNOWLEDGE, deadline, None)
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
            let topics = self.share_ack_topics(node_tps).await?;
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let Some(mut peer) = self.share_conns.remove(&node) else {
                self.reset_node_session(node).await;
                return Err(Error::protocol("missing share connection"));
            };
            let mut encoded = BytesMut::new();
            if let Err(error) = encode_share_acknowledge_topics_cooperative(
                &mut encoded,
                version,
                &self.group_id,
                &self.member_id,
                epoch,
                &topics,
            )
            .await
            {
                let _ = self.share_conns.insert(node, peer);
                return Err(error);
            }
            let payload = encoded.freeze();
            let sent = peer
                .conn
                .roundtrip(
                    SHARE_ACKNOWLEDGE,
                    version,
                    |buf| {
                        buf.extend_from_slice(&payload);
                        Ok(())
                    },
                    timeout,
                )
                .await;
            let _ = self.share_conns.insert(node, peer);
            self.apply_share_ack_reply(
                node,
                version,
                ack,
                topics,
                partitions,
                &mut first_error,
                sent,
                false,
            )
            .await?;
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn share_ack_topics(&self, node_tps: Vec<AckPartition>) -> Result<Vec<ShareAckTopic>> {
        let mut resolved = Vec::with_capacity(node_tps.len());
        let mut walked = 0usize;
        for (topic, part, batches) in node_tps {
            let topic_id = self
                .topic_ids
                .get(topic.as_ref())
                .copied()
                .filter(|id| *id != [0; 16])
                .ok_or_else(|| Error::protocol(format!("missing share topic id for {topic}")))?;
            resolved.push((topic_id, part, batches));
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
        Ok(group_share_ack_topics(resolved).await)
    }

    /// Acknowledge every leader within one remaining budget.
    ///
    /// The requests leave together. Replies are applied in leader order, and
    /// a failure on one leader still records an acknowledgement another
    /// leader already accepted.
    async fn acknowledge_leaders_together(
        &mut self,
        nodes: Vec<(i32, Vec<AckPartition>)>,
        partitions: &mut Vec<AckPartition>,
        ack: i8,
        deadline: Instant,
    ) -> Result<()> {
        let ids: Vec<i32> = nodes.iter().map(|(node, _)| *node).collect();
        let leaders = share_node_addresses(&ids, &self.assigned, |topic, part| {
            self.consumer.leader_of(topic, part).ok()
        });
        self.warm_share_leaders(&ids, deadline).await;
        let mut flights = tokio::task::JoinSet::new();
        let mut ready = Vec::new();
        let mut rpc_timeout = None;
        for (node, node_tps) in nodes {
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                return self
                    .finish_share_ack_flights(
                        &mut flights,
                        &mut ready,
                        partitions,
                        ack,
                        Some(Error::Timeout),
                    )
                    .await;
            }
            let version = match self
                .node_share_version_until(node, SHARE_ACKNOWLEDGE, deadline, Some(&leaders))
                .await
            {
                Ok(version) => version,
                Err(error) => {
                    return self
                        .finish_share_ack_flights(
                            &mut flights,
                            &mut ready,
                            partitions,
                            ack,
                            Some(error),
                        )
                        .await;
                }
            };
            if ack == ACK_RENEW && version < 2 {
                return self
                    .finish_share_ack_flights(
                        &mut flights,
                        &mut ready,
                        partitions,
                        ack,
                        Some(Error::Unsupported(format!(
                            "share broker {node} does not support Renew"
                        ))),
                    )
                    .await;
            }
            let epoch = self.session_epoch(node);
            if epoch <= 0 {
                return self
                    .finish_share_ack_flights(
                        &mut flights,
                        &mut ready,
                        partitions,
                        ack,
                        Some(Error::broker(
                            error::SHARE_SESSION_NOT_FOUND,
                            "ShareAcknowledge requires a poll on the current leader",
                        )),
                    )
                    .await;
            }
            let topics = match self.share_ack_topics(node_tps).await {
                Ok(topics) => topics,
                Err(error) => {
                    return self
                        .finish_share_ack_flights(
                            &mut flights,
                            &mut ready,
                            partitions,
                            ack,
                            Some(error),
                        )
                        .await;
                }
            };
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return self
                    .finish_share_ack_flights(
                        &mut flights,
                        &mut ready,
                        partitions,
                        ack,
                        Some(Error::Timeout),
                    )
                    .await;
            }
            let Some(peer) = self.share_conns.remove(&node) else {
                self.reset_node_session(node).await;
                return self
                    .finish_share_ack_flights(
                        &mut flights,
                        &mut ready,
                        partitions,
                        ack,
                        Some(Error::protocol("missing share connection")),
                    )
                    .await;
            };
            let mut body = BytesMut::new();
            if let Err(error) = encode_share_acknowledge_topics_cooperative(
                &mut body,
                version,
                &self.group_id,
                &self.member_id,
                epoch,
                &topics,
            )
            .await
            {
                let _ = self.share_conns.insert(node, peer);
                return self
                    .finish_share_ack_flights(
                        &mut flights,
                        &mut ready,
                        partitions,
                        ack,
                        Some(error),
                    )
                    .await;
            }
            let payload = body.freeze();
            let timeout = *rpc_timeout.get_or_insert(timeout);
            let index = ready.len();
            ready.push(PreparedShareAck {
                node,
                version,
                topics,
                sent: None,
            });
            let _ = flights.spawn(async move {
                let mut peer = peer;
                let sent = peer
                    .conn
                    .roundtrip(
                        SHARE_ACKNOWLEDGE,
                        version,
                        |buf| {
                            buf.extend_from_slice(&payload);
                            Ok(())
                        },
                        timeout,
                    )
                    .await;
                ShareAckFlight { index, peer, sent }
            });
        }
        self.finish_share_ack_flights(&mut flights, &mut ready, partitions, ack, None)
            .await
    }

    async fn finish_share_ack_flights(
        &mut self,
        flights: &mut tokio::task::JoinSet<ShareAckFlight>,
        ready: &mut Vec<PreparedShareAck>,
        partitions: &mut Vec<AckPartition>,
        ack: i8,
        prep_error: Option<Error>,
    ) -> Result<()> {
        let mut join_error = None;
        while let Some(joined) = flights.join_next().await {
            match joined {
                Ok(flight) => {
                    let node = ready.get(flight.index).map(|slot| slot.node);
                    if let Some(node) = node {
                        let _ = self.share_conns.insert(node, flight.peer);
                    }
                    if let Some(slot) = ready.get_mut(flight.index) {
                        slot.sent = Some(flight.sent);
                    }
                }
                Err(_) => {
                    join_error = Some(Error::protocol("share acknowledge task"));
                }
            }
        }
        let mut first_error = None;
        let mut hard = None;
        for slot in ready.drain(..) {
            let Some(sent) = slot.sent else {
                continue;
            };
            if let Err(error) = self
                .apply_share_ack_reply(
                    slot.node,
                    slot.version,
                    ack,
                    slot.topics,
                    partitions,
                    &mut first_error,
                    sent,
                    false,
                )
                .await
            {
                if hard.is_none() {
                    hard = Some(error);
                }
            }
        }
        if let Some(error) = hard {
            return Err(error);
        }
        if let Some(error) = prep_error {
            return Err(error);
        }
        if let Some(error) = join_error {
            return Err(error);
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Apply one ShareAcknowledge reply.
    ///
    /// Transport and top-level failures return immediately. Partition errors
    /// stay in `first_error` so a later partition can still record its outcome.
    #[expect(
        clippy::too_many_arguments,
        reason = "ShareAcknowledge apply keeps the leader, versions, batches, and reply together"
    )]
    async fn apply_share_ack_reply(
        &mut self,
        node: i32,
        version: i16,
        ack: i8,
        topics: Vec<ShareAckTopic>,
        partitions: &mut Vec<AckPartition>,
        first_error: &mut Option<Error>,
        sent: Result<Bytes>,
        encode_failed: bool,
    ) -> Result<()> {
        if encode_failed {
            return match sent {
                Err(error) => Err(error),
                Ok(_) => Err(Error::protocol("share acknowledge encode")),
            };
        }
        let mut body = match sent {
            Ok(body) => body,
            Err(error) => {
                self.reset_node_session(node).await;
                return Err(error);
            }
        };
        let (code, responses, endpoints, _, _, lock_timeout) =
            match decode_share_acknowledge_topics_response_with_lock_timeout(&mut body, version) {
                Ok(reply) => reply,
                Err(error) => {
                    self.reset_node_session(node).await;
                    return Err(error);
                }
            };
        self.consumer.apply_node_endpoints(&endpoints);
        if !body.is_empty() {
            self.reset_node_session(node).await;
            return Err(Error::protocol("trailing ShareAcknowledge response bytes"));
        }
        if code != 0 {
            let error = Error::broker(code, "ShareAcknowledge");
            if share_session_reset(&error) {
                self.reset_node_session(node).await;
            } else {
                self.advance_node_epoch(node);
            }
            return Err(error);
        }
        let expected = share_ack_expected(&topics).await;
        let mut seen = HashSet::new();
        let mut walked = 0usize;
        for topic in &responses {
            for part in &topic.partitions {
                if !expected.contains(&(topic.topic_id, part.partition))
                    || !seen.insert((topic.topic_id, part.partition))
                {
                    return Err(Error::protocol(
                        "unrequested or duplicate ShareAcknowledge partition response",
                    ));
                }
                walked = walked.saturating_add(1);
                if walked == 8 * 1024 {
                    walked = 0;
                    tokio::task::yield_now().await;
                }
            }
        }
        // Every accepted response partition was inserted only when it was
        // requested, so the sets match exactly when their lengths match.
        if seen.len() != expected.len() {
            return Err(Error::protocol(
                "ShareAcknowledge omitted a requested partition outcome",
            ));
        }
        let sent_batches = index_sent_ack_batches(&topics).await;
        let slots = index_ack_partition_slots(partitions).await;
        let mut cleared = false;
        self.advance_node_epoch(node);
        if version >= 2 && lock_timeout > 0 {
            self.acquisition_lock_timeout_ms = Some(lock_timeout);
        }
        let mut applied = 0usize;
        for topic in responses {
            let name = match self.name_for_topic_id(topic.topic_id) {
                Ok(name) => name,
                Err(error) => {
                    drop_empty_ack_partitions(partitions, cleared).await;
                    return Err(error);
                }
            };
            for part in topic.partitions {
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
                        remember_share_leader_patch(
                            &mut self.leader_patches,
                            &mut self.leader_patch_slots,
                            patch,
                        );
                    }
                }
                let sent = sent_batches
                    .get(&(topic.topic_id, part.partition))
                    .copied()
                    .unwrap_or(&[]);
                if part.error_code == 0 {
                    let count = self
                        .acquisitions
                        .get(name.as_ref())
                        .map(|parts| count_covered_acquisitions(parts, part.partition, sent))
                        .unwrap_or(0);
                    if ack == ACK_RENEW {
                        if lock_timeout <= 0 {
                            drop_empty_ack_partitions(partitions, cleared).await;
                            return Err(Error::protocol(
                                "non-positive Renew acquisition lock timeout",
                            ));
                        }
                        let expiry = Instant::now().checked_add(Duration::from_millis(
                            u64::try_from(lock_timeout).unwrap_or(0),
                        ));
                        if let Some(parts) = self.acquisitions.get_mut(name.as_ref()) {
                            renew_covered_acquisitions(parts, part.partition, sent, expiry);
                        }
                    } else {
                        let empty = if let Some(parts) = self.acquisitions.get_mut(name.as_ref()) {
                            drop_covered_acquisitions(parts, part.partition, sent);
                            parts.is_empty()
                        } else {
                            false
                        };
                        if empty {
                            let _ = self.acquisitions.remove(name.as_ref());
                        }
                        self.records_acknowledged = self
                            .records_acknowledged
                            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
                    }
                    if clear_indexed_ack_batches(
                        partitions,
                        &slots,
                        name.as_ref(),
                        part.partition,
                        sent,
                    )
                    .await
                    {
                        cleared = true;
                    }
                } else {
                    if part.error_code == error::INVALID_RECORD_STATE
                        || share_ack_ownership_lost(part.error_code)
                    {
                        let empty = if let Some(parts) = self.acquisitions.get_mut(name.as_ref()) {
                            drop_covered_acquisitions(parts, part.partition, sent);
                            parts.is_empty()
                        } else {
                            false
                        };
                        if empty {
                            let _ = self.acquisitions.remove(name.as_ref());
                        }
                    }
                    let error = Error::broker(
                        part.error_code,
                        format!("ShareAcknowledge {name}-{}", part.partition),
                    );
                    // A terminal outcome must remain visible even when another
                    // partition asks for a metadata retry.
                    if first_error.as_ref().is_none_or(share_ack_retriable) {
                        *first_error = Some(error);
                    }
                    if part.error_code == error::SHARE_SESSION_NOT_FOUND
                        || part.error_code == error::INVALID_SHARE_SESSION_EPOCH
                    {
                        self.reset_node_session(node).await;
                    }
                }
                applied = applied.saturating_add(1);
                if applied == 8 * 1024 {
                    applied = 0;
                    tokio::task::yield_now().await;
                }
            }
        }
        drop(sent_batches);
        drop_empty_ack_partitions(partitions, cleared).await;
        self.reclaim_ack_types(topics);
        Ok(())
    }

    /// Map a ShareAcknowledge close roundtrip to the error the caller keeps.
    ///
    /// A transport failure, including a timeout, is share-session-not-found:
    /// the broker already dropped the session, so close does not fail the leave.
    fn share_close_error(sent: Result<Bytes>, version: i16) -> Option<Error> {
        let code = match sent {
            Ok(mut body) => match decode_share_acknowledge_response(&mut body, version) {
                Ok(code) if body.is_empty() => code,
                Ok(_) => {
                    return Some(Error::protocol("trailing ShareAcknowledge close bytes"));
                }
                Err(error) => return Some(error),
            },
            Err(_) => error::SHARE_SESSION_NOT_FOUND,
        };
        if code != 0 && code != error::SHARE_SESSION_NOT_FOUND {
            Some(Error::broker(code, "ShareAcknowledge close"))
        } else {
            None
        }
    }

    /// True when every open share session still has a live leader socket.
    ///
    /// A closed or idle socket has to be dialed again. That dial needs the
    /// share connection map, so it stays on the sequential close path.
    fn share_close_sockets_are_open(&self) -> bool {
        let idle = self.cfg.connections_max_idle;
        self.share_epochs.iter().all(|(node, epoch)| {
            *epoch <= 0
                || self
                    .share_conns
                    .get(node)
                    .is_some_and(|peer| !peer.conn.is_closed() && !peer.conn.idle_expired(idle))
        })
    }

    /// Final share acknowledgements for the open sessions.
    ///
    /// The sockets are taken first so the coordinator leave can run while
    /// those acknowledgements are in flight.
    async fn spawn_final_share_acks(
        &mut self,
        deadline: Instant,
    ) -> (tokio::task::JoinSet<(Result<Bytes>, i16)>, Option<Error>) {
        let open: Vec<i32> = self
            .share_epochs
            .iter()
            .filter(|(_, epoch)| **epoch > 0)
            .map(|(node, _)| *node)
            .collect();
        self.share_epochs.clear();
        let mut conns = std::mem::take(&mut self.share_conns);
        self.acquisitions.clear();
        self.pending_delivery.clear();
        let mut set = tokio::task::JoinSet::new();
        let mut prepare_error = None;
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            discard_share_sockets(&mut conns).await;
            return (set, Some(Error::Timeout));
        }
        for node in open {
            let Some(peer) = conns.remove(&node) else {
                prepare_error = Some(Error::protocol("missing share connection"));
                continue;
            };
            let version = match spoken_share_acknowledge(peer.acknowledge_version) {
                Ok(version) => version,
                Err(error) => {
                    prepare_error = Some(error);
                    finish_share_peer(peer).await;
                    continue;
                }
            };
            let mut body = BytesMut::new();
            if let Err(error) = encode_share_acknowledge_request(
                &mut body,
                version,
                &self.group_id,
                &self.member_id,
                ShareRequestMetadata::FINAL_EPOCH,
                [0; 16],
                &[],
            ) {
                prepare_error = Some(error);
                finish_share_peer(peer).await;
                continue;
            }
            let payload = body.freeze();
            let _ = set.spawn(async move {
                let mut peer = peer;
                let sent = peer
                    .conn
                    .roundtrip(
                        SHARE_ACKNOWLEDGE,
                        version,
                        |buf| {
                            buf.extend_from_slice(&payload);
                            Ok(())
                        },
                        timeout,
                    )
                    .await;
                finish_share_peer(peer).await;
                (sent, version)
            });
        }
        discard_share_sockets(&mut conns).await;
        (set, prepare_error)
    }

    async fn finish_final_share_acks(
        mut set: tokio::task::JoinSet<(Result<Bytes>, i16)>,
        prepare_error: Option<Error>,
    ) -> Result<()> {
        let mut last = match prepare_error {
            Some(error) => Err(error),
            None => Ok(()),
        };
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((sent, version)) => {
                    if let Some(error) = Self::share_close_error(sent, version) {
                        last = Err(error);
                    }
                }
                Err(_) => last = Err(Error::protocol("share acknowledge close task")),
            }
        }
        last
    }

    async fn close_share_session(&mut self, deadline: Instant) -> Result<()> {
        self.finish_inflight_share_fetches(deadline).await;
        let open: Vec<_> = self
            .share_epochs
            .iter()
            .filter(|(_, epoch)| **epoch > 0)
            .map(|(node, _)| *node)
            .collect();
        let last = if open.len() > 1 {
            self.close_share_sessions_together(&open, deadline).await
        } else {
            self.close_share_sessions_in_order(&open, deadline).await
        };
        self.share_epochs.clear();
        discard_share_sockets(&mut self.share_conns).await;
        self.acquisitions.clear();
        self.pending_delivery.clear();
        last
    }

    /// Close one open share session at a time.
    ///
    /// A single leader stays on this path so a transport failure is still
    /// reported as share-session-not-found and the socket is put back before
    /// the caller drops every share connection.
    async fn close_share_sessions_in_order(
        &mut self,
        open: &[i32],
        deadline: Instant,
    ) -> Result<()> {
        let leaders = share_node_addresses(open, &self.assigned, |topic, part| {
            self.consumer.leader_of(topic, part).ok()
        });
        let mut last = Ok(());
        for node in open {
            let node = *node;
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                last = Err(Error::Timeout);
                break;
            }
            let version = match self
                .node_share_version_until(node, SHARE_ACKNOWLEDGE, deadline, Some(&leaders))
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
            if let Some(error) = Self::share_close_error(sent, version) {
                last = Err(error);
            }
        }
        last
    }

    /// Close every open share session within one remaining budget.
    ///
    /// Version lookup stays in order. The final-epoch acknowledgements then
    /// leave together, so one slow leader cannot hold the other sessions
    /// open until this deadline expires.
    async fn close_share_sessions_together(
        &mut self,
        open: &[i32],
        deadline: Instant,
    ) -> Result<()> {
        let leaders = share_node_addresses(open, &self.assigned, |topic, part| {
            self.consumer.leader_of(topic, part).ok()
        });
        let mut decided = vec![None; open.len()];
        let mut set = tokio::task::JoinSet::new();
        let mut rpc_timeout = None;
        let mut timed_out = false;
        for (index, node) in open.iter().copied().enumerate() {
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                timed_out = true;
                break;
            }
            let version = match self
                .node_share_version_until(node, SHARE_ACKNOWLEDGE, deadline, Some(&leaders))
                .await
            {
                Ok(version) => version,
                Err(error) => {
                    if let Some(slot) = decided.get_mut(index) {
                        *slot = Some(error);
                    }
                    continue;
                }
            };
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                timed_out = true;
                break;
            }
            let Some(peer) = self.share_conns.remove(&node) else {
                if let Some(slot) = decided.get_mut(index) {
                    *slot = Some(Error::protocol("missing share connection"));
                }
                continue;
            };
            let mut body = BytesMut::new();
            if let Err(error) = encode_share_acknowledge_request(
                &mut body,
                version,
                &self.group_id,
                &self.member_id,
                ShareRequestMetadata::FINAL_EPOCH,
                [0; 16],
                &[],
            ) {
                if let Some(slot) = decided.get_mut(index) {
                    *slot = Some(error);
                }
                finish_share_peer(peer).await;
                continue;
            }
            let payload = body.freeze();
            let timeout = *rpc_timeout.get_or_insert(timeout);
            let _ = set.spawn(async move {
                let mut peer = peer;
                let sent = peer
                    .conn
                    .roundtrip(
                        SHARE_ACKNOWLEDGE,
                        version,
                        |buf| {
                            buf.extend_from_slice(&payload);
                            Ok(())
                        },
                        timeout,
                    )
                    .await;
                finish_share_peer(peer).await;
                (index, sent, version)
            });
        }
        let mut join_error = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((index, sent, version)) => {
                    if let Some(error) = Self::share_close_error(sent, version) {
                        if let Some(slot) = decided.get_mut(index) {
                            *slot = Some(error);
                        }
                    }
                }
                Err(_) => {
                    join_error = Some(Error::protocol("share acknowledge close task"));
                }
            }
        }
        if timed_out {
            return Err(Error::Timeout);
        }
        let mut last = Ok(());
        for slot in &mut decided {
            if let Some(error) = slot.take() {
                last = Err(error);
            }
        }
        if let Some(error) = join_error {
            last = Err(error);
        }
        last
    }

    /// Leave the share group ([`ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH`]).
    pub async fn leave(self) -> Result<()> {
        let deadline = Instant::now() + self.cfg.request_timeout;
        self.leave_within(deadline).await
    }

    async fn leave_within(mut self, deadline: Instant) -> Result<()> {
        self.hb_stop.send(true).unwrap_or(());
        let heartbeat = self.hb_task.take();
        if self.member_id.is_empty() {
            *self.hb_deadline.lock() = None;
            self.consumer.close_interceptors();
            if let Some(heartbeat) = heartbeat {
                finish_share_heartbeat(heartbeat, deadline).await;
            }
            // The member socket drops with this group. Yield so the broker
            // observes that close before leave returns.
            drop(self);
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            return Ok(());
        }
        *self.hb_deadline.lock() = None;
        self.finish_inflight_share_fetches(deadline).await;
        let (set, prepare_error) = self.spawn_final_share_acks(deadline).await;
        let close = Self::finish_final_share_acks(set, prepare_error);
        let out = self.leave_coordinator(deadline);
        let (close, out) = tokio::join!(close, out);
        self.consumer.close_interceptors();
        if let Some(heartbeat) = heartbeat {
            finish_share_heartbeat(heartbeat, deadline).await;
        }
        let result = close.and(out);
        // Member and coordinator sockets drop with this group. Yield so the
        // broker observes that close before leave returns.
        drop(self);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        result
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
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        let (version, body) = coord_roundtrip_with_bootstrap(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            SHARE_GROUP_HEARTBEAT,
            |version, buf| encode_share_group_heartbeat_request(buf, version, &req),
            &bootstrap_keys,
            &address_keys,
            &mut self.prior_coords,
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

    fn spawn_heartbeat(&self, mut stop: watch::Receiver<bool>) -> Arc<tokio::task::JoinHandle<()>> {
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
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        Arc::new(tokio::spawn(async move {
            if *stop.borrow() {
                return;
            }
            let mut conn: Option<BrokerConn> = None;
            let mut remembered: Option<crate::group::KnownCoord> = None;
            let mut prior_coords = std::collections::HashMap::new();
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
                enum Wake {
                    Stop,
                    Again,
                    Beat,
                }
                let wake = tokio::select! {
                    res = stop.changed() => {
                        if res.is_err() || *stop.borrow() {
                            Wake::Stop
                        } else {
                            Wake::Again
                        }
                    }
                    _ = hb_wake.notified() => Wake::Again,
                    _ = tokio::time::sleep(sleep_duration) => Wake::Beat,
                    _ = crate::group::coordinator_lost(&mut conn) => {
                        retire_share_conn(&mut conn).await;
                        next_hb_deadline = Instant::now();
                        Wake::Beat
                    }
                };
                match wake {
                    Wake::Stop => break,
                    Wake::Again => continue,
                    Wake::Beat => {}
                }
                if Instant::now() < next_hb_deadline {
                    continue;
                }
                let deadline = Instant::now() + cfg.request_timeout;
                if conn.is_none() {
                    if let Some(known) = remembered.take() {
                        conn = Box::pin(crate::group::rediscover_known_coord(
                            &cfg,
                            &group_id,
                            crate::protocol::group::COORDINATOR_GROUP,
                            &known,
                            &mut prior_coords,
                        ))
                        .await
                        .ok();
                    }
                }
                crate::group::reopen_idle_coordinator(
                    &mut conn,
                    &cfg,
                    &group_id,
                    deadline,
                    &bootstrap_keys,
                    &address_keys,
                    &mut prior_coords,
                )
                .await;
                if *stop.borrow() {
                    break;
                }
                let Some(c) = conn.as_mut() else {
                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                    next_hb_deadline = Instant::now() + retry_delay;
                    if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) {
                        break;
                    }
                    continue;
                };
                let epoch = hb_epoch.load(Ordering::SeqCst);
                let Ok(version) = spoken_share_group_heartbeat(c.share_group_heartbeat_version)
                else {
                    retire_share_conn(&mut conn).await;
                    hb_err.store(error::UNSUPPORTED_VERSION, Ordering::SeqCst);
                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                    next_hb_deadline = Instant::now() + retry_delay;
                    if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) {
                        break;
                    }
                    continue;
                };
                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                    next_hb_deadline = Instant::now() + retry_delay;
                    if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) {
                        break;
                    }
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
                        |buf| encode_share_group_heartbeat_request(buf, version, &heartbeat),
                        timeout,
                    )
                    .await;
                if *stop.borrow() {
                    break;
                }
                match res {
                    Ok(body) => {
                        if let Ok(resp) =
                            decode_share_group_heartbeat_response(&mut body.clone(), version)
                        {
                            if crate::error::coordinator_retriable(resp.error_code) {
                                if let Some(current) = conn.as_ref() {
                                    remembered = Some(
                                        crate::group::KnownCoord::capture(current)
                                            .with_bootstrap_keys(&bootstrap_keys)
                                            .with_address_keys(&address_keys),
                                    );
                                }
                                retire_share_conn(&mut conn).await;
                                let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                                next_hb_deadline = Instant::now() + retry_delay;
                                if !update_share_heartbeat_deadline(
                                    &hb_deadline,
                                    &stop,
                                    next_hb_deadline,
                                ) {
                                    break;
                                }
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
                                        if !update_share_heartbeat_deadline(
                                            &hb_deadline,
                                            &stop,
                                            next_hb_deadline,
                                        ) {
                                            break;
                                        }
                                    } else if let Ok(ms_u64) =
                                        u64::try_from(resp.heartbeat_interval_ms)
                                    {
                                        current_interval = Duration::from_millis(ms_u64);
                                        hb_interval_ms
                                            .store(resp.heartbeat_interval_ms, Ordering::SeqCst);
                                        next_hb_deadline = Instant::now() + current_interval;
                                        if !update_share_heartbeat_deadline(
                                            &hb_deadline,
                                            &stop,
                                            next_hb_deadline,
                                        ) {
                                            break;
                                        }
                                    } else {
                                        next_hb_deadline = Instant::now() + current_interval;
                                        if !update_share_heartbeat_deadline(
                                            &hb_deadline,
                                            &stop,
                                            next_hb_deadline,
                                        ) {
                                            break;
                                        }
                                    }
                                } else {
                                    next_hb_deadline = Instant::now() + current_interval;
                                    if !update_share_heartbeat_deadline(
                                        &hb_deadline,
                                        &stop,
                                        next_hb_deadline,
                                    ) {
                                        break;
                                    }
                                }
                            }
                        } else {
                            if let Some(current) = conn.as_ref() {
                                remembered = Some(
                                    crate::group::KnownCoord::capture(current)
                                        .with_bootstrap_keys(&bootstrap_keys)
                                        .with_address_keys(&address_keys),
                                );
                            }
                            retire_share_conn(&mut conn).await;
                            let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                            next_hb_deadline = Instant::now() + retry_delay;
                            if !update_share_heartbeat_deadline(
                                &hb_deadline,
                                &stop,
                                next_hb_deadline,
                            ) {
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        if let Some(current) = conn.as_ref() {
                            remembered = Some(
                                crate::group::KnownCoord::capture(current)
                                    .with_bootstrap_keys(&bootstrap_keys)
                                    .with_address_keys(&address_keys),
                            );
                        }
                        retire_share_conn(&mut conn).await;
                        let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                        next_hb_deadline = Instant::now() + retry_delay;
                        if !update_share_heartbeat_deadline(&hb_deadline, &stop, next_hb_deadline) {
                            break;
                        }
                    }
                }
            }
        }))
    }
}

async fn retire_share_conn(conn: &mut Option<BrokerConn>) {
    if let Some(mut current) = conn.take() {
        current.close();
        current.release_if_closed().await;
    }
}

/// Stop a share heartbeat task that has been signalled, then abort it if it is still running.
async fn finish_share_heartbeat(task: Arc<tokio::task::JoinHandle<()>>, deadline: Instant) {
    while !task.is_finished() && Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    if !task.is_finished() {
        task.abort();
    }
    let mut turns = 0u32;
    while !task.is_finished() && turns < 8 {
        turns = turns.saturating_add(1);
        tokio::task::yield_now().await;
    }
    // The abort drops the coordinator when the task is polled. The idle watch
    // then has to run before the broker sees that close.
    for _ in 0..8 {
        tokio::task::yield_now().await;
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

fn share_node_addresses(
    nodes: &[i32],
    assigned: &[(Arc<str>, i32)],
    mut leader_of: impl FnMut(&str, i32) -> Option<(i32, String)>,
) -> HashMap<i32, String> {
    let wanted: HashSet<i32> = nodes.iter().copied().collect();
    let mut addresses = HashMap::new();
    for (topic, part) in assigned {
        let Some((node, addr)) = leader_of(topic.as_ref(), *part) else {
            continue;
        };
        if !wanted.contains(&node) || addresses.contains_key(&node) {
            continue;
        }
        let _ = addresses.insert(node, addr);
    }
    addresses
}

fn drop_share_records_for_topics(records: &mut Vec<ShareRecord>, topics: &[Arc<str>]) {
    if records.is_empty() || topics.is_empty() {
        return;
    }
    let dropped: HashSet<&str> = topics.iter().map(|topic| topic.as_ref()).collect();
    records.retain(|record| !dropped.contains(record.topic.as_str()));
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

/// Record bytes plus new acquisition metadata, pausing between slices.
async fn share_delivery_accounted_bytes(
    incoming: &[ShareRecord],
    acquisitions: &AcquisitionMap,
) -> usize {
    let mut incoming_bytes = 0usize;
    let mut new_metadata_bytes = 0usize;
    let mut walked = 0usize;
    for record in incoming {
        incoming_bytes = incoming_bytes.saturating_add(share_record_storage_bytes(record));
        if acquisition_at(acquisitions, &record.topic, record.partition, record.offset).is_none() {
            new_metadata_bytes =
                new_metadata_bytes.saturating_add(acquisition_storage_bytes(&record.topic));
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    incoming_bytes.saturating_add(new_metadata_bytes)
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

async fn group_share_ack_topics(
    resolved: Vec<([u8; 16], i32, Vec<AcknowledgementBatch>)>,
) -> Vec<ShareAckTopic> {
    let mut topics: Vec<ShareAckTopic> = Vec::new();
    let mut index: HashMap<[u8; 16], usize> = HashMap::new();
    let mut walked = 0usize;
    for (topic_id, part, batches) in resolved {
        if let Some(&slot) = index.get(&topic_id) {
            if let Some(topic) = topics.get_mut(slot) {
                topic.partitions.push((part, batches));
            }
        } else {
            let slot = topics.len();
            let _previous = index.insert(topic_id, slot);
            topics.push(ShareAckTopic {
                topic_id,
                partitions: vec![(part, batches)],
            });
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    topics
}

async fn share_ack_expected(topics: &[ShareAckTopic]) -> HashSet<([u8; 16], i32)> {
    let mut expected =
        HashSet::with_capacity(topics.iter().map(|topic| topic.partitions.len()).sum());
    let mut walked = 0usize;
    for topic in topics {
        for (partition, _) in &topic.partitions {
            let _ = expected.insert((topic.topic_id, *partition));
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    expected
}

async fn share_partition_set(requested: &[i32]) -> HashSet<i32> {
    let mut allowed = HashSet::with_capacity(requested.len());
    let mut walked = 0usize;
    for partition in requested {
        let _ = allowed.insert(*partition);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    allowed
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

fn remember_share_leader_patch(
    patches: &mut Vec<ShareLeaderPatch>,
    slots: &mut HashMap<String, HashMap<i32, usize>>,
    patch: ShareLeaderPatch,
) {
    if let Some(parts) = slots.get_mut(patch.topic.as_str()) {
        if let Some(&slot) = parts.get(&patch.partition) {
            if let Some(existing) = patches.get_mut(slot) {
                if patch.leader_epoch >= existing.leader_epoch {
                    *existing = patch;
                }
            }
            return;
        }
        let slot = patches.len();
        let _stored = parts.insert(patch.partition, slot);
        patches.push(patch);
        return;
    }
    let mut parts = HashMap::new();
    let slot = patches.len();
    let _stored = parts.insert(patch.partition, slot);
    let _stored_topic = slots.insert(patch.topic.clone(), parts);
    patches.push(patch);
}

fn index_share_leader_patches(
    patches: &[ShareLeaderPatch],
) -> HashMap<String, HashMap<i32, usize>> {
    let mut slots: HashMap<String, HashMap<i32, usize>> = HashMap::new();
    for (slot, patch) in patches.iter().enumerate() {
        if let Some(parts) = slots.get_mut(patch.topic.as_str()) {
            let _stored = parts.insert(patch.partition, slot);
            continue;
        }
        let mut parts = HashMap::new();
        let _stored = parts.insert(patch.partition, slot);
        let _stored_topic = slots.insert(patch.topic.clone(), parts);
    }
    slots
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
fn share_topic_ids_current<T, M>(
    stored_map: &HashMap<Arc<str>, [u8; 16]>,
    assigned: &[(T, i32)],
    metadata: &HashMap<M, [u8; 16]>,
) -> bool
where
    T: AsRef<str>,
    M: std::borrow::Borrow<str> + std::hash::Hash + Eq,
{
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

#[cfg(test)]
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

#[cfg(test)]
fn drop_sent_ack_batches(
    partitions: &mut Vec<AckPartition>,
    topic: &str,
    partition: i32,
    sent: &[AcknowledgementBatch],
) {
    partitions.retain_mut(|(name, part, batches)| {
        if name.as_ref() != topic || *part != partition {
            return true;
        }
        batches.retain(|batch| !sent.contains(batch));
        !batches.is_empty()
    });
}

/// First batch list for each `(topic id, partition)` in a ShareAcknowledge request.
///
/// A repeated partition keeps the earlier list. Inserting every partition pauses
/// so the caller can still reach a deadline.
async fn index_sent_ack_batches(
    topics: &[ShareAckTopic],
) -> HashMap<([u8; 16], i32), &[AcknowledgementBatch]> {
    let mut index = HashMap::with_capacity(topics.iter().map(|topic| topic.partitions.len()).sum());
    let mut walked = 0usize;
    for topic in topics {
        for (partition, batches) in &topic.partitions {
            let _ = index
                .entry((topic.topic_id, *partition))
                .or_insert(batches.as_slice());
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    index
}

/// Positions of every pending acknowledgement for one topic and partition.
///
/// The same partition can appear more than once when its offsets came from
/// different ranges. Recording those positions pauses between slices.
async fn index_ack_partition_slots(
    partitions: &[AckPartition],
) -> HashMap<Arc<str>, HashMap<i32, Vec<usize>>> {
    let mut slots: HashMap<Arc<str>, HashMap<i32, Vec<usize>>> = HashMap::new();
    let mut walked = 0usize;
    for (index, (name, partition, _)) in partitions.iter().enumerate() {
        if let Some(parts) = slots.get_mut(name.as_ref()) {
            parts.entry(*partition).or_default().push(index);
        } else {
            let mut parts = HashMap::new();
            let _previous = parts.insert(*partition, vec![index]);
            let _previous = slots.insert(Arc::clone(name), parts);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    slots
}

#[derive(Clone, Copy)]
struct AckBatchKey<'a> {
    first: i64,
    last: i64,
    types: &'a [i8],
}

impl PartialEq for AckBatchKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.first == other.first && self.last == other.last && self.types == other.types
    }
}

impl Eq for AckBatchKey<'_> {}

impl std::hash::Hash for AckBatchKey<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.first.hash(state);
        self.last.hash(state);
        self.types.hash(state);
    }
}

fn ack_batch_key(batch: &AcknowledgementBatch) -> AckBatchKey<'_> {
    AckBatchKey {
        first: batch.first_offset,
        last: batch.last_offset,
        types: batch.types.as_slice(),
    }
}

/// Remove batches that were acknowledged, leaving emptied rows for one later compact.
async fn clear_indexed_ack_batches(
    partitions: &mut [AckPartition],
    slots: &HashMap<Arc<str>, HashMap<i32, Vec<usize>>>,
    topic: &str,
    partition: i32,
    sent: &[AcknowledgementBatch],
) -> bool {
    let Some(indexes) = slots.get(topic).and_then(|parts| parts.get(&partition)) else {
        return false;
    };
    let mut sent_set = HashSet::with_capacity(sent.len());
    let mut walked = 0usize;
    for batch in sent {
        let _ = sent_set.insert(ack_batch_key(batch));
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    let mut cleared = false;
    for index in indexes {
        let Some((_, _, batches)) = partitions.get_mut(*index) else {
            continue;
        };
        let before = batches.len();
        let mut write = 0usize;
        let mut seen = 0usize;
        for read in 0..batches.len() {
            let keep = batches
                .get(read)
                .is_some_and(|batch| !sent_set.contains(&ack_batch_key(batch)));
            if keep {
                if write != read {
                    batches.swap(write, read);
                }
                write = write.saturating_add(1);
            }
            seen = seen.saturating_add(1);
            if seen == 8 * 1024 {
                seen = 0;
                tokio::task::yield_now().await;
            }
        }
        batches.truncate(write);
        if before > 0 && batches.is_empty() {
            cleared = true;
        }
    }
    cleared
}

async fn drop_empty_ack_partitions(partitions: &mut Vec<AckPartition>, cleared: bool) {
    if cleared {
        compact_cleared_ack_partitions(partitions).await;
    }
}

async fn compact_cleared_ack_partitions(partitions: &mut Vec<AckPartition>) {
    let mut kept = Vec::with_capacity(partitions.len());
    let mut walked = 0usize;
    for part in partitions.drain(..) {
        if !part.2.is_empty() {
            kept.push(part);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
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

fn stale_share_ack(topic: &str, partition: i32, offset: i64) -> Error {
    Error::broker(
        error::INVALID_RECORD_STATE,
        format!("stale share acknowledgement {topic}-{partition}@{offset}"),
    )
}

/// Group acknowledgement batches by the broker that acquired each offset.
///
/// Runs are visited in start order. A broker change starts a new batch, and a
/// gap is still rejected. The scan pauses between slices.
async fn group_acks_from_acquisitions(
    map: &AcquisitionMap,
    partitions: &[AckPartition],
) -> Result<AckRoute> {
    let mut by_node: AckRoute = BTreeMap::new();
    let mut spare_types = Vec::new();
    for (topic, partition, batches) in partitions {
        let overlapped = map
            .get(topic.as_ref())
            .and_then(|runs| runs.by_partition.get(partition))
            .is_some_and(|part| part.overlapped);
        if overlapped {
            let one = [(Arc::clone(topic), *partition, batches.clone())];
            let grouped = group_acks_by_acquisition_node(
                |name, part, offset| acquisition_node(map, name, part, offset),
                |name, part, first, last| acquisition_node_for_range(map, name, part, first, last),
                &one,
            )?;
            for (node, parts) in grouped {
                by_node.entry(node).or_default().extend(parts);
            }
            continue;
        }
        let mut node_batches: BTreeMap<i32, Vec<AcknowledgementBatch>> = BTreeMap::new();
        for batch in batches {
            append_ack_batch_from_runs(
                map,
                topic,
                *partition,
                batch,
                &mut node_batches,
                &mut spare_types,
            )
            .await?;
        }
        for (node, grouped) in node_batches {
            by_node
                .entry(node)
                .or_default()
                .push((Arc::clone(topic), *partition, grouped));
        }
    }
    Ok(by_node)
}

async fn append_ack_batch_from_runs(
    map: &AcquisitionMap,
    topic: &Arc<str>,
    partition: i32,
    batch: &AcknowledgementBatch,
    node_batches: &mut BTreeMap<i32, Vec<AcknowledgementBatch>>,
    spare_types: &mut Vec<Vec<i8>>,
) -> Result<()> {
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
    if batch.types.len() != 1 && batch.types.len() != span {
        return Err(Error::protocol(
            "share acknowledgement type count does not match the range",
        ));
    }
    let single_type = (batch.types.len() == 1)
        .then(|| batch.types.first().copied())
        .flatten();
    let topic_runs = map
        .get(topic.as_ref())
        .ok_or_else(|| stale_share_ack(topic, partition, batch.first_offset))?;
    let mut scan_from = topic_runs
        .by_partition
        .get(&partition)
        .and_then(|part| {
            part.by_first
                .range(..=batch.first_offset)
                .next_back()
                .map(|(&start, _)| start)
        })
        .ok_or_else(|| stale_share_ack(topic, partition, batch.first_offset))?;
    let mut cursor = batch.first_offset;
    let mut open: Option<(i32, i64)> = None;
    let mut walked = 0usize;
    loop {
        let chunk = covering_run_indexes(
            topic_runs,
            partition,
            scan_from,
            batch.last_offset,
            8 * 1024,
        );
        if chunk.is_empty() {
            break;
        }
        for (start, index) in chunk {
            let (run_first, run_last, node_id) = {
                let run = topic_runs
                    .runs
                    .get(index)
                    .ok_or_else(|| stale_share_ack(topic, partition, cursor))?;
                (run.first, run.last, run.acquisition.node)
            };
            scan_from = if start == i64::MAX {
                start
            } else {
                start.saturating_add(1)
            };
            if run_last < cursor {
                continue;
            }
            if run_first > cursor || cursor > run_last {
                return Err(stale_share_ack(topic, partition, cursor));
            }
            let end = run_last.min(batch.last_offset);
            if open.is_some_and(|(node, _)| node != node_id) {
                if let Some((node, first)) = open.take() {
                    push_ack_run(
                        node_batches,
                        spare_types,
                        batch,
                        node,
                        first,
                        cursor.saturating_sub(1),
                        single_type,
                    )?;
                }
            }
            if open.is_none() {
                open = Some((node_id, cursor));
            }
            if end >= batch.last_offset {
                if let Some((node, first)) = open.take() {
                    push_ack_run(
                        node_batches,
                        spare_types,
                        batch,
                        node,
                        first,
                        batch.last_offset,
                        single_type,
                    )?;
                }
                return Ok(());
            }
            let next = end.saturating_add(1);
            if next <= end {
                return Err(stale_share_ack(topic, partition, cursor));
            }
            cursor = next;
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
        if scan_from > batch.last_offset || scan_from == i64::MAX {
            break;
        }
        tokio::task::yield_now().await;
    }
    Err(stale_share_ack(topic, partition, cursor))
}

fn push_ack_run(
    node_batches: &mut BTreeMap<i32, Vec<AcknowledgementBatch>>,
    spare_types: &mut Vec<Vec<i8>>,
    batch: &AcknowledgementBatch,
    node: i32,
    first: i64,
    last: i64,
    single_type: Option<i8>,
) -> Result<()> {
    let types = if let Some(kind) = single_type {
        reused_one_ack_type(spare_types, kind)
    } else {
        let origin = batch.first_offset;
        let start = usize::try_from(first.saturating_sub(origin)).map_err(|_| {
            Error::protocol("share acknowledgement type count does not match the range")
        })?;
        let end = usize::try_from(last.saturating_sub(origin))
            .map_err(|_| {
                Error::protocol("share acknowledgement type count does not match the range")
            })?
            .saturating_add(1);
        let slice = batch.types.get(start..end).ok_or_else(|| {
            Error::protocol("share acknowledgement type count does not match the range")
        })?;
        finish_ack_run(first, last, slice, spare_types).types
    };
    node_batches
        .entry(node)
        .or_default()
        .push(AcknowledgementBatch {
            first_offset: first,
            last_offset: last,
            types,
        });
    Ok(())
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
    let mut partitions = Vec::new();
    let mut batches = Vec::new();
    block_on_ack(acknowledgement_batches_with(
        recs,
        ack,
        &mut offsets,
        &mut pairs,
        &mut topic,
        &mut types,
        &mut partitions,
        &mut batches,
    ));
    partitions
}

#[cfg(test)]
fn block_on_ack<T>(future: impl std::future::Future<Output = T>) -> T {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| panic!("{error}"));
    runtime.block_on(future)
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

#[expect(
    clippy::too_many_arguments,
    reason = "acknowledgement grouping refills the caller's offset, pair, type, partition, and batch lists"
)]
async fn acknowledgement_batches_with(
    recs: &[ShareRecord],
    ack: i8,
    offsets: &mut Vec<i64>,
    pairs: &mut Vec<(i32, i64)>,
    topic_name: &mut Option<Arc<str>>,
    ack_types: &mut Vec<Vec<i8>>,
    out: &mut Vec<AckPartition>,
    spare_batches: &mut Vec<Vec<AcknowledgementBatch>>,
) {
    out.clear();
    let Some(first) = recs.first() else {
        return;
    };
    if ack_records_share_partition(recs).await {
        offsets.clear();
        copy_ack_offsets(offsets, recs.iter().map(|rec| rec.offset)).await;
        let batches = collapse_ack_offsets(offsets, ack, ack_types, spare_batches).await;
        if batches.is_empty() {
            park_unused_ack_batches(spare_batches, batches);
            return;
        }
        out.push((
            share_topic_arc(&first.topic, topic_name),
            first.partition,
            batches,
        ));
        return;
    }
    if ack_records_share_topic(recs).await {
        pairs.clear();
        copy_ack_offsets(pairs, recs.iter().map(|rec| (rec.partition, rec.offset))).await;
        pairs.sort_unstable();
        let name = share_topic_arc(&first.topic, topic_name);
        let mut start = 0usize;
        let mut walked = 0usize;
        while start < pairs.len() {
            let Some(partition) = pairs.get(start).map(|(partition, _)| *partition) else {
                break;
            };
            let mut end = start;
            while pairs.get(end).is_some_and(|(next, _)| *next == partition) {
                end = end.saturating_add(1);
                walked = walked.saturating_add(1);
                if walked == 8 * 1024 {
                    walked = 0;
                    tokio::task::yield_now().await;
                }
            }
            offsets.clear();
            if let Some(group) = pairs.get(start..end) {
                offsets.extend(group.iter().map(|(_, offset)| *offset));
            }
            let batches = collapse_ack_offsets(offsets, ack, ack_types, spare_batches).await;
            if batches.is_empty() {
                park_unused_ack_batches(spare_batches, batches);
            } else {
                out.push((Arc::clone(&name), partition, batches));
            }
            start = end;
        }
        return;
    }
    let mut by_part: BTreeMap<crate::consumer::TopicName, BTreeMap<i32, Vec<i64>>> =
        BTreeMap::new();
    let mut walked = 0usize;
    for rec in recs {
        if let Some(parts) = by_part.get_mut(&rec.topic) {
            parts.entry(rec.partition).or_default().push(rec.offset);
        } else {
            let mut parts = BTreeMap::new();
            let _ = parts.insert(rec.partition, vec![rec.offset]);
            let _ = by_part.insert(rec.topic.clone(), parts);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    for (topic, parts) in by_part {
        let name = share_topic_arc(&topic, topic_name);
        for (partition, mut offs) in parts {
            let batches = collapse_ack_offsets(&mut offs, ack, ack_types, spare_batches).await;
            if batches.is_empty() {
                park_unused_ack_batches(spare_batches, batches);
            } else {
                out.push((Arc::clone(&name), partition, batches));
            }
        }
    }
}

/// True when every record names the first record's topic and partition.
async fn ack_records_share_partition(recs: &[ShareRecord]) -> bool {
    let Some(first) = recs.first() else {
        return true;
    };
    let mut walked = 0usize;
    for rec in recs.iter().skip(1) {
        if rec.topic != first.topic || rec.partition != first.partition {
            return false;
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    true
}

/// True when every record names the first record's topic.
async fn ack_records_share_topic(recs: &[ShareRecord]) -> bool {
    let Some(first) = recs.first() else {
        return true;
    };
    let mut walked = 0usize;
    for rec in recs.iter().skip(1) {
        if rec.topic != first.topic {
            return false;
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    true
}

/// Copy one acknowledgement field per record, pausing between slices.
async fn copy_ack_offsets<T>(out: &mut Vec<T>, values: impl Iterator<Item = T>) {
    let mut walked = 0usize;
    for value in values {
        out.push(value);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    if walked != 0 {
        tokio::task::yield_now().await;
    }
}

fn park_unused_ack_batches(
    spare: &mut Vec<Vec<AcknowledgementBatch>>,
    mut batches: Vec<AcknowledgementBatch>,
) {
    batches.clear();
    if batches.capacity() > 0 {
        spare.push(batches);
    }
}

fn take_ack_batches(spare: &mut Vec<Vec<AcknowledgementBatch>>) -> Vec<AcknowledgementBatch> {
    let mut batches = spare.pop().unwrap_or_default();
    batches.clear();
    batches
}

async fn collapse_ack_offsets(
    offsets: &mut Vec<i64>,
    ack: i8,
    ack_types: &mut Vec<Vec<i8>>,
    spare_batches: &mut Vec<Vec<AcknowledgementBatch>>,
) -> Vec<AcknowledgementBatch> {
    offsets.sort_unstable();
    offsets.dedup();
    let mut batches = take_ack_batches(spare_batches);
    let mut range: Option<(i64, i64)> = None;
    let mut walked = 0usize;
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
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn checking_many_share_acks_does_not_hold_the_caller() {
        // 2_000_000 records: checking stored acquisitions held the caller 694ms.
        let topic = crate::consumer::TopicName::from("orders");
        let mut runs = TopicRuns::new();
        runs.runs.push(AcquisitionRun {
            partition: 0,
            first: 0,
            last: 1_999_999,
            acquisition: Acquisition {
                node: 1,
                delivery_count: 1,
                expires_at: None,
            },
        });
        runs.reindex();
        let mut map = AcquisitionMap::new();
        let _ = map.insert(topic.clone(), runs);
        let mut recs = Vec::with_capacity(2_000_000);
        for offset in 0..2_000_000 {
            recs.push(ShareRecord {
                topic: topic.clone(),
                partition: 0,
                offset,
                timestamp: 1,
                timestamp_type: TimestampType::CreateTime,
                key: None,
                value: None,
                headers: Vec::new(),
                delivery_count: 1,
                leader_epoch: None,
            });
        }
        let started = std::time::Instant::now();
        tokio::select! {
            result = reject_stale_share_acks(&map, &recs) => {
                assert!(result.is_ok());
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "checking share acknowledgements held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn accounting_many_share_records_does_not_hold_the_caller() {
        // 2_000_000 records: accounting a share delivery held the caller 165ms.
        let topic = crate::consumer::TopicName::from("orders");
        let few = [
            share_rec(&topic, 0),
            share_rec(&topic, 1),
            share_rec(&topic, 2),
        ];
        let mut stored = AcquisitionMap::new();
        remember_acquisition(
            &mut stored,
            &topic,
            0,
            1,
            Acquisition {
                node: 4,
                delivery_count: 1,
                expires_at: None,
            },
        );
        let expected_records = few
            .iter()
            .map(share_record_storage_bytes)
            .fold(0usize, usize::saturating_add);
        let expected_metadata = few
            .iter()
            .filter(|record| {
                acquisition_at(&stored, &record.topic, record.partition, record.offset).is_none()
            })
            .map(|record| acquisition_storage_bytes(record.topic()))
            .fold(0usize, usize::saturating_add);
        assert_eq!(
            share_delivery_accounted_bytes(&few, &stored).await,
            expected_records.saturating_add(expected_metadata)
        );
        let mut incoming = Vec::with_capacity(2_000_000);
        for offset in 0..2_000_000 {
            incoming.push(share_rec(&topic, offset));
        }
        let map = AcquisitionMap::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = share_delivery_accounted_bytes(&incoming, &map) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "accounting share delivery held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collapsing_many_ack_offsets_does_not_hold_the_caller() {
        // 2_000_000 gapped offsets: collapsing acknowledgements held the caller 284ms.
        let mut offsets: Vec<i64> = (0..2_000_000).map(|offset| offset * 2).collect();
        let mut types = Vec::new();
        let mut spare = Vec::new();
        let started = std::time::Instant::now();
        tokio::select! {
            batches = collapse_ack_offsets(&mut offsets, ACK_ACCEPT, &mut types, &mut spare) => {
                assert_eq!(batches.len(), 2_000_000);
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "collapsing acknowledgements held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn grouping_many_share_acks_does_not_hold_the_caller() {
        // 2_000_000 records: grouping one partition held the caller 183ms.
        let topic = crate::consumer::TopicName::from("orders");
        let mut recs = Vec::with_capacity(2_000_000);
        for offset in 0..2_000_000 {
            recs.push(ShareRecord {
                topic: topic.clone(),
                partition: 0,
                offset,
                timestamp: 1,
                timestamp_type: TimestampType::CreateTime,
                key: None,
                value: None,
                headers: Vec::new(),
                delivery_count: 1,
                leader_epoch: None,
            });
        }
        let started = std::time::Instant::now();
        tokio::select! {
            batches = async {
                let mut offsets = Vec::new();
                let mut pairs = Vec::new();
                let mut topic_name = None;
                let mut ack_types = Vec::new();
                let mut out = Vec::new();
                let mut spare = Vec::new();
                acknowledgement_batches_with(
                    &recs,
                    ACK_ACCEPT,
                    &mut offsets,
                    &mut pairs,
                    &mut topic_name,
                    &mut ack_types,
                    &mut out,
                    &mut spare,
                )
                .await;
                out.into_iter().map(|(_, _, batches)| batches.len()).sum::<usize>()
            } => {
                assert_eq!(batches, 1);
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "grouping share acknowledgements held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remembering_gapped_acquisitions_does_not_hold_the_caller() {
        // 20_000 gapped offsets on one partition held the caller 116s.
        let topic = crate::consumer::TopicName::from("orders");
        let stored = Acquisition {
            node: 1,
            delivery_count: 1,
            expires_at: None,
        };
        let mut map = AcquisitionMap::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = async {
                for offset in 0..2_000 {
                    remember_acquisition(&mut map, &topic, 0, offset * 2, stored);
                }
            } => {
                assert_eq!(map.get("orders").map(TopicRuns::len), Some(2_000));
                assert_eq!(
                    acquisition_at(&map, &topic, 0, 0).map(|found| found.delivery_count),
                    Some(1)
                );
                assert!(acquisition_at(&map, &topic, 0, 1).is_none());
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "remembering gapped acquisitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remembering_many_acquisitions_does_not_hold_the_caller() {
        // 20_000 partitions: scanning earlier runs held the caller 2.5s.
        // Recording 1_000_000 partitions without a pause held the caller 1.3s.
        let topic = crate::consumer::TopicName::from("orders");
        let offsets: Vec<(i32, i64)> = (0..1_000_000).map(|partition| (partition, 0)).collect();
        let stored = Acquisition {
            node: 1,
            delivery_count: 1,
            expires_at: None,
        };
        let mut map = AcquisitionMap::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = remember_share_offsets(&mut map, &topic, &offsets, stored) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "remembering share acquisitions held the caller for {elapsed:?}"
        );
    }

    #[test]
    fn an_overlapping_extend_keeps_the_earlier_acquisition() {
        let topic = crate::consumer::TopicName::from("orders");
        let mut map = AcquisitionMap::new();
        let earlier = Acquisition {
            node: 1,
            delivery_count: 2,
            expires_at: None,
        };
        let later = Acquisition {
            node: 1,
            delivery_count: 1,
            expires_at: None,
        };
        remember_acquisition(&mut map, &topic, 0, 1, earlier);
        remember_acquisition(&mut map, &topic, 0, 0, later);
        remember_acquisition(&mut map, &topic, 0, 1, later);
        assert_eq!(
            acquisition_at(&map, &topic, 0, 1).map(|found| found.delivery_count),
            Some(2)
        );
    }

    #[test]
    fn remembered_acquisitions_merge_and_split() {
        let topic = crate::consumer::TopicName::from("orders");
        let mut map = AcquisitionMap::new();
        let same = Acquisition {
            node: 4,
            delivery_count: 1,
            expires_at: None,
        };
        remember_acquisition(&mut map, &topic, 0, 1, same);
        remember_acquisition(&mut map, &topic, 0, 3, same);
        remember_acquisition(&mut map, &topic, 0, 2, same);
        let runs = map.get("orders").expect("runs");
        assert_eq!(
            runs.runs
                .iter()
                .map(|run| (run.first, run.last))
                .collect::<Vec<_>>(),
            vec![(1, 2), (3, 3)]
        );
        let later = Acquisition {
            node: 4,
            delivery_count: 2,
            expires_at: None,
        };
        remember_acquisition(&mut map, &topic, 1, 8, later);
        remember_acquisition(&mut map, &topic, 1, 8, later);
        assert_eq!(
            acquisition_at(&map, &topic, 1, 8).map(|found| found.delivery_count),
            Some(2)
        );
        remember_acquisition(&mut map, &topic, 1, 8, same);
        assert_eq!(
            acquisition_at(&map, &topic, 1, 8).map(|found| found.delivery_count),
            Some(1)
        );
        assert_eq!(
            acquisition_at(&map, &topic, 0, 2).map(|found| found.node),
            Some(4)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn noting_many_share_offsets_does_not_hold_the_caller() {
        // 20_000 partitions: scanning the cursor list held the caller 469ms.
        // Noting 1_000_000 partitions without a pause held the caller 722ms.
        let notes: Vec<(i32, i64)> = (0..1_000_000).map(|partition| (partition, 1)).collect();
        let mut cursors = HashMap::new();
        let mut spilled = HashMap::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = note_share_offsets(&mut cursors, &mut spilled, &notes) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "noting share offsets held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn grouping_many_share_ack_topics_does_not_hold_the_caller() {
        // 20_000 distinct topic ids: scanning earlier topics held the caller 698ms.
        // Indexing 1_000_000 topic ids without a pause held the caller 1.1s.
        let resolved: Vec<([u8; 16], i32, Vec<AcknowledgementBatch>)> = (0..1_000_000u32)
            .map(|index| {
                let mut topic_id = [0; 16];
                topic_id[..4].copy_from_slice(&index.to_le_bytes());
                (topic_id, 0, Vec::new())
            })
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = group_share_ack_topics(resolved) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "grouping share acknowledgement topics held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn grouped_share_ack_topics_keep_the_first_id() {
        let resolved = vec![
            ([2; 16], 0, Vec::new()),
            ([1; 16], 4, Vec::new()),
            ([2; 16], 1, Vec::new()),
        ];
        let topics = group_share_ack_topics(resolved).await;
        assert_eq!(topics.len(), 2);
        assert_eq!(topics.first().map(|topic| topic.topic_id), Some([2; 16]));
        assert_eq!(
            topics.first().map(|topic| topic
                .partitions
                .iter()
                .map(|(part, _)| *part)
                .collect::<Vec<_>>()),
            Some(vec![0, 1])
        );
        assert_eq!(topics.get(1).map(|topic| topic.topic_id), Some([1; 16]));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_sent_ack_batches_does_not_hold_the_caller() {
        // 20_000 reverse lookups walked the request for 434ms.
        // Indexing 1_000_000 partitions without a pause held the caller 500ms.
        let topics = vec![ShareAckTopic {
            topic_id: [7; 16],
            partitions: (0..1_000_000).map(|index| (index, Vec::new())).collect(),
        }];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = index_sent_ack_batches(&topics) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "indexing acknowledged batches held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexed_sent_ack_batches_keep_the_first_list() {
        let first = AcknowledgementBatch {
            first_offset: 4,
            last_offset: 4,
            types: vec![ACK_ACCEPT],
        };
        let later = AcknowledgementBatch {
            first_offset: 9,
            last_offset: 9,
            types: vec![ACK_REJECT],
        };
        let topics = vec![ShareAckTopic {
            topic_id: [1; 16],
            partitions: vec![(0, vec![first.clone()]), (0, vec![later]), (1, Vec::new())],
        }];
        let indexed = index_sent_ack_batches(&topics).await;
        assert_eq!(indexed.len(), 2);
        assert_eq!(
            indexed.get(&([1; 16], 0)).copied(),
            Some(sent_ack_batches_by_id(&topics, [1; 16], 0))
        );
        assert_eq!(
            indexed
                .get(&([1; 16], 0))
                .and_then(|batches| batches.first())
                .map(|batch| batch.first_offset),
            Some(4)
        );
        assert!(indexed
            .get(&([1; 16], 1))
            .is_some_and(|batches| batches.is_empty()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_ack_partition_slots_does_not_hold_the_caller() {
        // 20_000 one-at-a-time drops walked the pending list for 4.3s.
        // Indexing 1_000_000 pending rows without a pause held the caller 941ms.
        let pending: Vec<AckPartition> = (0..1_000_000)
            .map(|index| (Arc::<str>::from("orders"), index, Vec::new()))
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = index_ack_partition_slots(&pending) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "indexing pending acknowledgements held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexed_ack_drops_match_one_at_a_time() {
        let batch = |offset| AcknowledgementBatch {
            first_offset: offset,
            last_offset: offset,
            types: vec![ACK_ACCEPT],
        };
        let pending = vec![
            (Arc::<str>::from("orders"), 0, vec![batch(1)]),
            (Arc::<str>::from("orders"), 0, vec![batch(3)]),
            (Arc::<str>::from("orders"), 1, vec![batch(5), batch(6)]),
            (Arc::<str>::from("payments"), 1, vec![batch(7)]),
        ];
        let mut one = pending.clone();
        drop_sent_ack_batches(&mut one, "orders", 0, &[batch(1)]);
        drop_sent_ack_batches(&mut one, "orders", 1, &[batch(5)]);
        let mut indexed = pending;
        let slots = index_ack_partition_slots(&indexed).await;
        let mut cleared =
            clear_indexed_ack_batches(&mut indexed, &slots, "orders", 0, &[batch(1)]).await;
        cleared |= clear_indexed_ack_batches(&mut indexed, &slots, "orders", 1, &[batch(5)]).await;
        drop_empty_ack_partitions(&mut indexed, cleared).await;
        assert_eq!(indexed, one);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_many_share_acks_does_not_hold_the_caller() {
        // 1_000_000 partitions: building the expected set held the caller 430ms.
        let topics = vec![ShareAckTopic {
            topic_id: [7; 16],
            partitions: (0..1_000_000).map(|index| (index, Vec::new())).collect(),
        }];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = share_ack_expected(&topics) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "indexing share acknowledgements held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexed_share_acks_keep_the_first_partition() {
        let topics = vec![ShareAckTopic {
            topic_id: [1; 16],
            partitions: vec![(0, Vec::new()), (0, Vec::new()), (1, Vec::new())],
        }];
        let expected = share_ack_expected(&topics).await;
        assert_eq!(expected.len(), 2);
        assert!(expected.contains(&([1; 16], 0)) && expected.contains(&([1; 16], 1)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_many_share_partitions_does_not_hold_the_caller() {
        // 20_000 partitions: scanning the request list held the caller 714ms.
        // Indexing 1_000_000 partitions still has to pause.
        let requested: Vec<i32> = (0..1_000_000).collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = share_partition_set(&requested) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "indexing share partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexed_share_partitions_match_the_request() {
        let requested = vec![2, 0, 2, 1];
        let allowed = share_partition_set(&requested).await;
        assert_eq!(allowed.len(), 3);
        assert!(allowed.contains(&0) && allowed.contains(&1) && allowed.contains(&2));
    }

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
    fn filling_many_unique_names_does_not_hold_the_caller() {
        // 20_000 names: scanning earlier names held the caller 1.18s.
        let topics: Vec<String> = (0..20_000).map(|index| format!("t{index}")).collect();
        let mut names = Vec::new();
        let started = std::time::Instant::now();
        super::fill_unique_names(&mut names, topics.iter().map(String::as_str));
        let elapsed = started.elapsed();
        assert_eq!(names.len(), 20_000);
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "filling unique names held the caller for {elapsed:?}"
        );
        let ptr = names.first().map(|name| name.as_ptr());
        super::fill_unique_names(&mut names, topics.iter().map(String::as_str));
        assert_eq!(names.first().map(|name| name.as_ptr()), ptr);
        assert_eq!(names.get(1).map(String::as_str), Some("t1"));
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
        let mut wider = HashMap::new();
        let _ = wider.insert(Arc::<str>::from("orders"), vec![0, 1, 2, 3]);
        super::refill_share_fetch(&mut by_id, &mut topics, &wider, &topic_ids, 1024)
            .unwrap_or_else(|error| panic!("{error}"));
        let part_capacity = topics
            .first()
            .map(|topic| topic.partitions.capacity())
            .unwrap_or(0);
        assert!(part_capacity >= 4);
        assert_eq!(topics.first().map(|topic| topic.partitions.len()), Some(4));
        super::refill_share_fetch(&mut by_id, &mut topics, &wider, &topic_ids, 1024)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(topics
            .first()
            .is_some_and(|topic| topic.partitions.capacity() == part_capacity));

        let mut many = HashMap::new();
        let _ = many.insert(Arc::<str>::from("orders"), vec![0]);
        let _ = many.insert(Arc::<str>::from("payments"), vec![1]);
        let _ = many.insert(Arc::<str>::from("trades"), vec![2]);
        let mut ids = topic_ids;
        let _ = ids.insert(Arc::<str>::from("payments"), [8u8; 16]);
        let _ = ids.insert(Arc::<str>::from("trades"), [7u8; 16]);
        let mut fresh_ids = BTreeMap::new();
        let mut fresh_topics = Vec::new();
        super::refill_share_fetch(&mut fresh_ids, &mut fresh_topics, &many, &ids, 64)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(fresh_topics.len(), 3);
        let topic_capacity = fresh_topics.capacity();
        assert!(topic_capacity >= 3);
        super::refill_share_fetch(&mut fresh_ids, &mut fresh_topics, &many, &ids, 64)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(fresh_topics.capacity(), topic_capacity);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn grouping_many_ack_nodes_does_not_hold_the_caller() {
        // 200_000 alternating brokers: grouping acknowledgements held the caller 207ms.
        let topic = crate::consumer::TopicName::from("orders");
        let mut map = AcquisitionMap::new();
        for offset in 0..200_000 {
            remember_acquisition(
                &mut map,
                &topic,
                0,
                offset,
                Acquisition {
                    node: if offset % 2 == 0 { 4 } else { 9 },
                    delivery_count: if offset % 2 == 0 { 1 } else { 2 },
                    expires_at: None,
                },
            );
        }
        let partitions = vec![(
            Arc::<str>::from("orders"),
            0,
            vec![AcknowledgementBatch {
                first_offset: 0,
                last_offset: 199_999,
                types: vec![ACK_ACCEPT],
            }],
        )];
        let missing = group_acks_from_acquisitions(
            &map,
            &[(
                Arc::<str>::from("orders"),
                0,
                vec![AcknowledgementBatch {
                    first_offset: 0,
                    last_offset: 200_000,
                    types: vec![ACK_ACCEPT],
                }],
            )],
        )
        .await;
        assert!(missing.is_err());
        let started = std::time::Instant::now();
        tokio::select! {
            grouped = group_acks_from_acquisitions(&map, &partitions) => {
                let grouped = grouped.expect("grouped");
                assert_eq!(grouped.get(&4).and_then(|parts| parts.first()).map(|part| part.2.len()), Some(100_000));
                assert_eq!(grouped.get(&9).and_then(|parts| parts.first()).map(|part| part.2.len()), Some(100_000));
                assert_eq!(
                    grouped.get(&4).and_then(|parts| parts.first()).and_then(|part| part.2.first()).map(|batch| batch.first_offset),
                    Some(0)
                );
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "grouping acknowledgements held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ranging_many_acquisitions_does_not_hold_the_caller() {
        // 200_000 single-offset runs: finding one broker held the caller 114ms.
        let topic = crate::consumer::TopicName::from("orders");
        let mut map = AcquisitionMap::new();
        for offset in 0..200_000 {
            remember_acquisition(
                &mut map,
                &topic,
                0,
                offset,
                Acquisition {
                    node: 4,
                    delivery_count: if offset % 2 == 0 { 1 } else { 2 },
                    expires_at: None,
                },
            );
        }
        remember_acquisition(
            &mut map,
            &topic,
            1,
            0,
            Acquisition {
                node: 4,
                delivery_count: 1,
                expires_at: None,
            },
        );
        remember_acquisition(
            &mut map,
            &topic,
            1,
            1,
            Acquisition {
                node: 9,
                delivery_count: 2,
                expires_at: None,
            },
        );
        let started = std::time::Instant::now();
        tokio::select! {
            node = async { acquisition_node_for_range(&map, "orders", 0, 0, 199_999) } => {
                assert_eq!(node, Some(4));
                assert_eq!(acquisition_node_for_range(&map, "orders", 1, 0, 1), None);
                assert_eq!(acquisition_node_for_range(&map, "orders", 0, 0, 200_000), None);
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "ranging acquisitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn splitting_many_partial_acks_does_not_hold_the_caller() {
        // 6_000 partial ranges: rescanning every batch held the caller 297ms.
        let acquisition = Acquisition {
            node: 1,
            delivery_count: 1,
            expires_at: None,
        };
        let mut runs: Vec<_> = (0..6_000)
            .map(|offset| AcquisitionRun {
                partition: 0,
                first: offset * 3,
                last: offset * 3 + 1,
                acquisition,
            })
            .collect();
        let sent: Vec<_> = (0..6_000)
            .map(|offset| AcknowledgementBatch {
                first_offset: offset * 3,
                last_offset: offset * 3,
                types: vec![ACK_ACCEPT],
            })
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            () = async { drop_covered_runs(&mut runs, 0, &sent); } => {
                assert_eq!(runs.len(), 6_000);
                assert_eq!(runs.first().map(|run| (run.first, run.last)), Some((1, 1)));
                assert_eq!(
                    runs.last().map(|run| (run.first, run.last)),
                    Some((17_998, 17_998))
                );
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "splitting partial acknowledgements held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn covering_many_acquisitions_does_not_hold_the_caller() {
        // 8_000 runs against 8_000 batches held the caller 600ms.
        let acquisition = Acquisition {
            node: 1,
            delivery_count: 1,
            expires_at: None,
        };
        let mut runs: Vec<_> = (0..8_000)
            .map(|offset| AcquisitionRun {
                partition: 0,
                first: offset * 2,
                last: offset * 2,
                acquisition,
            })
            .collect();
        let sent: Vec<_> = (0..8_000)
            .map(|offset| AcknowledgementBatch {
                first_offset: offset * 2,
                last_offset: offset * 2,
                types: vec![ACK_ACCEPT],
            })
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            () = async { drop_covered_runs(&mut runs, 0, &sent); } => {
                assert!(runs.is_empty());
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "covering acquisitions held the caller for {elapsed:?}"
        );
    }

    #[test]
    fn stacked_ack_batches_cover_a_run() {
        let mut runs = vec![AcquisitionRun {
            partition: 0,
            first: 0,
            last: 3,
            acquisition: Acquisition {
                node: 1,
                delivery_count: 1,
                expires_at: None,
            },
        }];
        let batch = AcknowledgementBatch {
            first_offset: 0,
            last_offset: 1,
            types: vec![ACK_ACCEPT],
        };
        drop_covered_runs(&mut runs, 0, &[batch.clone(), batch]);
        assert!(runs.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn splitting_a_large_acquisition_does_not_hold_the_caller() {
        // One offset inside 2_000_000: splitting the run held the caller 166ms.
        let acquisition = Acquisition {
            node: 1,
            delivery_count: 1,
            expires_at: None,
        };
        let mut runs = vec![AcquisitionRun {
            partition: 0,
            first: 0,
            last: 1_999_999,
            acquisition,
        }];
        let started = std::time::Instant::now();
        tokio::select! {
            () = async {
                drop_covered_runs(
                    &mut runs,
                    0,
                    &[AcknowledgementBatch {
                        first_offset: 10,
                        last_offset: 10,
                        types: vec![ACK_ACCEPT],
                    }],
                );
            } => {
                assert_eq!(runs.len(), 2);
                assert_eq!(runs.first().map(|run| (run.first, run.last)), Some((0, 9)));
                assert_eq!(
                    runs.get(1).map(|run| (run.first, run.last)),
                    Some((11, 1_999_999))
                );
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "splitting an acquisition held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clearing_many_ack_batches_does_not_hold_the_caller() {
        // 12_000 batches: matching acknowledged batches held the caller 231ms.
        let batch = |offset: i64| AcknowledgementBatch {
            first_offset: offset,
            last_offset: offset,
            types: vec![ACK_ACCEPT],
        };
        let batches: Vec<_> = (0..12_000).map(batch).collect();
        let mut partitions = vec![(Arc::<str>::from("orders"), 0, batches.clone())];
        let slots = index_ack_partition_slots(&partitions).await;
        let started = std::time::Instant::now();
        tokio::select! {
            cleared = clear_indexed_ack_batches(&mut partitions, &slots, "orders", 0, &batches) => {
                assert!(cleared);
                assert!(partitions.first().is_some_and(|part| part.2.is_empty()));
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "clearing acknowledgement batches held the caller for {elapsed:?}"
        );
    }

    #[test]
    fn a_partial_ack_keeps_the_acquisition_buffer() {
        let acquisition = Acquisition {
            node: 1,
            delivery_count: 1,
            expires_at: None,
        };
        let mut runs = Vec::with_capacity(8);
        runs.push(AcquisitionRun {
            partition: 0,
            first: 1,
            last: 4,
            acquisition,
        });
        let stored = runs.as_ptr();
        drop_covered_runs(
            &mut runs,
            0,
            &[AcknowledgementBatch {
                first_offset: 2,
                last_offset: 3,
                types: vec![ACK_ACCEPT],
            }],
        );
        assert_eq!(runs.as_ptr(), stored);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs.first().map(|run| (run.first, run.last)), Some((1, 1)));
        assert_eq!(runs.get(1).map(|run| (run.first, run.last)), Some((4, 4)));
        let expiry = Some(std::time::Instant::now());
        let renewed = expiry.map(|at| at + std::time::Duration::from_secs(5));
        let mut live = vec![AcquisitionRun {
            partition: 0,
            first: 1,
            last: 4,
            acquisition: Acquisition {
                expires_at: expiry,
                ..acquisition
            },
        }];
        renew_covered_runs(
            &mut live,
            0,
            &[AcknowledgementBatch {
                first_offset: 2,
                last_offset: 3,
                types: vec![ACK_ACCEPT],
            }],
            renewed,
        );
        assert_eq!(
            live.iter()
                .map(|run| (run.first, run.last, run.acquisition.expires_at))
                .collect::<Vec<_>>(),
            vec![(1, 1, expiry), (2, 3, renewed), (4, 4, expiry)]
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
        assert_eq!(map.get("orders").map(|runs| runs.len()), Some(1));
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
    fn share_delivery_reserves_the_returned_buffer_once() {
        let mut pending = Vec::new();
        super::reserve_share_delivery(&mut pending, 8);
        assert!(pending.capacity() >= 8);
        let kept = pending.capacity();
        let taken = std::mem::take(&mut pending);
        assert!(taken.capacity() >= kept);
        assert_eq!(pending.capacity(), 0);
        super::reserve_share_delivery(&mut pending, 8);
        assert!(pending.capacity() >= 8);
        let again = pending.capacity();
        pending.push(rec(0, 1));
        assert_eq!(pending.capacity(), again);
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
        let mut cursors = HashMap::new();
        let mut spilled = HashMap::new();
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 0, 1));
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 0, 2));
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 1, 2));
        assert!(spilled.is_empty());
        assert!(!super::note_share_offset(&mut cursors, &mut spilled, 0, 2));
        let mut cursors = HashMap::from([(0, 5)]);
        let mut spilled = HashMap::new();
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 0, 3));
        assert!(super::note_share_offset(&mut cursors, &mut spilled, 0, 4));
        assert!(!super::note_share_offset(&mut cursors, &mut spilled, 0, 5));
    }

    fn share_rec(topic: &crate::consumer::TopicName, offset: i64) -> ShareRecord {
        ShareRecord {
            topic: topic.clone(),
            partition: 0,
            offset,
            timestamp: 1,
            timestamp_type: TimestampType::CreateTime,
            key: None,
            value: None,
            headers: Vec::new(),
            delivery_count: 1,
            leader_epoch: None,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reading_many_share_nodes_does_not_hold_the_caller() {
        // 2_000_000 partitions: reading each leader held the caller 612ms.
        let mut leaders = HashMap::new();
        let _ = leaders.insert(
            Arc::<str>::from("orders"),
            (0..2_000_000).map(|index| index % 8).collect(),
        );
        let topic: Arc<str> = Arc::from("orders");
        let assigned: Vec<(Arc<str>, i32)> = (0..2_000_000)
            .map(|partition| (Arc::clone(&topic), partition))
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = share_nodes_for(&assigned, &leaders) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "reading share nodes held the caller for {elapsed:?}"
        );
        let small = [
            (Arc::clone(&topic), 3),
            (Arc::clone(&topic), 3),
            (Arc::clone(&topic), 1),
        ];
        let nodes = share_nodes_for(&small, &leaders).await;
        assert_eq!(nodes, vec![3, 1]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn planning_many_share_leaders_does_not_hold_the_caller() {
        // 2_000_000 partitions: grouping each leader held the caller 918ms.
        let mut cluster = crate::cluster::Cluster::default();
        for node in 0..8 {
            let _ = cluster
                .brokers
                .insert(node, format!("127.0.0.1:{}", 9092 + node));
        }
        let _ = cluster.leaders.insert(
            Arc::<str>::from("orders"),
            (0..2_000_000).map(|index| index % 8).collect(),
        );
        let topic: Arc<str> = Arc::from("orders");
        let assigned: Vec<(Arc<str>, i32)> = (0..2_000_000)
            .map(|partition| (Arc::clone(&topic), partition))
            .collect();
        let mut plan = ShareLeaderPlan::new();
        let mut leader_topic = None;
        let mut leader_ids = Vec::new();
        let started = std::time::Instant::now();
        tokio::select! {
            result = build_share_leader_plan(
                &assigned,
                &cluster,
                &mut plan,
                &mut leader_topic,
                &mut leader_ids,
            ) => {
                result.unwrap_or_else(|error| panic!("{error}"));
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "planning share leaders held the caller for {elapsed:?}"
        );
        let missing = [(Arc::<str>::from("none"), 0)];
        let error = build_share_leader_plan(
            &missing,
            &cluster,
            &mut plan,
            &mut leader_topic,
            &mut leader_ids,
        )
        .await
        .expect_err("missing topic");
        assert!(error.to_string().contains("none"), "{error}");
        let known = [(Arc::clone(&topic), 0), (Arc::clone(&topic), 1)];
        build_share_leader_plan(
            &known,
            &cluster,
            &mut plan,
            &mut leader_topic,
            &mut leader_ids,
        )
        .await
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            plan.get(&0).and_then(|topics| topics.get(topic.as_ref())),
            Some(&vec![0])
        );
        assert_eq!(
            plan.get(&1).and_then(|topics| topics.get(topic.as_ref())),
            Some(&vec![1])
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collecting_many_share_topic_names_does_not_hold_the_caller() {
        // 2_000_000 partitions: collecting each topic name held the caller 319ms.
        let topic: Arc<str> = Arc::from("orders");
        let assigned: Vec<(Arc<str>, i32)> = (0..2_000_000)
            .map(|partition| (Arc::clone(&topic), partition))
            .collect();
        let mut names = vec!["orders".to_string()];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = fill_unique_topic_names(&mut names, &assigned) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "collecting share topic names held the caller for {elapsed:?}"
        );
        names.clear();
        names.push("orders".to_string());
        let kept = names.first().map(|name| name.as_ptr());
        fill_unique_topic_names(&mut names, &assigned[..2]).await;
        assert_eq!(names, vec!["orders".to_string()]);
        assert_eq!(names.first().map(|name| name.as_ptr()), kept);
    }

    #[test]
    fn finding_many_share_leader_addresses_does_not_hold_the_caller() {
        // 512 nodes and 8,192 partitions: scanning the assignment for each node
        // held the caller 195ms.
        let assigned: Vec<(Arc<str>, i32)> = (0..8_192)
            .map(|partition| (Arc::from("orders"), partition))
            .collect();
        let nodes: Vec<i32> = (0..512).collect();
        let started = std::time::Instant::now();
        let found = share_node_addresses(&nodes, &assigned, |_topic, partition| {
            Some((511, format!("10.0.0.1:{}", 9000 + partition)))
        });
        let elapsed = started.elapsed();
        assert_eq!(found.get(&511).map(String::as_str), Some("10.0.0.1:9000"));
        assert!(found.get(&0).is_none());
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "finding share leader addresses held the caller for {elapsed:?}"
        );
    }

    #[test]
    fn dropping_many_recreated_share_records_does_not_hold_the_caller() {
        // 8_192 topics: scanning the buffer for each topic held the caller 1.20s.
        let topics: Vec<Arc<str>> = (0..8_192)
            .map(|index| Arc::from(format!("t{index}")))
            .collect();
        let mut records: Vec<ShareRecord> = topics
            .iter()
            .map(|topic| ShareRecord {
                topic: crate::consumer::TopicName::from(topic.as_ref()),
                partition: 0,
                offset: 1,
                timestamp: 0,
                timestamp_type: TimestampType::CreateTime,
                key: None,
                value: None,
                headers: Vec::new(),
                delivery_count: 1,
                leader_epoch: None,
            })
            .collect();
        records.push(ShareRecord {
            topic: crate::consumer::TopicName::from("kept"),
            partition: 0,
            offset: 2,
            timestamp: 0,
            timestamp_type: TimestampType::CreateTime,
            key: None,
            value: None,
            headers: Vec::new(),
            delivery_count: 1,
            leader_epoch: None,
        });
        let started = std::time::Instant::now();
        drop_share_records_for_topics(&mut records, &topics);
        let elapsed = started.elapsed();
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().map(|record| record.offset), Some(2));
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "dropping recreated share records held the caller for {elapsed:?}"
        );
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
    fn a_share_ack_name_keeps_the_assignment_topic() {
        let topic: Arc<str> = Arc::from("topic-name-longer-than-twenty-three");
        let stored = Arc::as_ptr(&topic);
        let id = [4u8; 16];
        let mut ids = HashMap::new();
        let _ = ids.insert(Arc::clone(&topic), id);
        let name = share_topic_by_id(&ids, id)
            .map(Arc::clone)
            .expect("known id");
        assert!(std::ptr::addr_eq(Arc::as_ptr(&name), stored));
    }

    #[test]
    fn a_share_assignment_keeps_the_cached_topic_name() {
        let orders: Arc<str> = Arc::from("topic-name-longer-than-twenty-three");
        let stored = Arc::as_ptr(&orders);
        let id = [1u8; 16];
        let mut id_to_name = HashMap::new();
        let _ = id_to_name.insert(id, Arc::clone(&orders));
        let name_to_id = HashMap::<Arc<str>, [u8; 16]>::new();
        let update = next_share_assignment(
            &[
                ShareTopicPartitions {
                    topic_id: id,
                    partitions: vec![0, 1],
                },
                ShareTopicPartitions {
                    topic_id: id,
                    partitions: vec![2],
                },
            ],
            &id_to_name,
            &name_to_id,
            &[],
        )
        .expect("cached name");
        assert_eq!(
            update
                .topic_ids
                .get_key_value(orders.as_ref())
                .map(|(name, _)| Arc::as_ptr(name)),
            Some(stored)
        );
        assert!(update
            .assigned
            .iter()
            .all(|(topic, _)| std::ptr::addr_eq(Arc::as_ptr(topic), stored)));
        assert_eq!(update.assigned.len(), 3);
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
            &HashMap::<[u8; 16], String>::new(),
            &HashMap::<String, [u8; 16]>::new(),
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
        let mut spare_batches = Vec::new();
        let mut first = Vec::new();
        block_on_ack(super::acknowledgement_batches_with(
            &recs,
            ACK_ACCEPT,
            &mut offsets,
            &mut pairs,
            &mut topic,
            &mut ack_types,
            &mut first,
            &mut spare_batches,
        ));
        let ptr = offsets.as_ptr();
        let list = first.as_ptr();
        let kept = first
            .first()
            .map(|part| Arc::clone(&part.0))
            .expect("partition");
        let kept_types = first
            .first()
            .and_then(|part| part.2.first())
            .map(|batch| batch.types.as_ptr())
            .expect("ack type");
        let mut kept_batches = first
            .first_mut()
            .map(|part| std::mem::take(&mut part.2))
            .expect("batches");
        let batch_ptr = kept_batches.as_ptr();
        for batch in kept_batches.drain(..) {
            ack_types.push(batch.types);
        }
        spare_batches.push(kept_batches);
        first.clear();
        block_on_ack(super::acknowledgement_batches_with(
            &recs,
            ACK_ACCEPT,
            &mut offsets,
            &mut pairs,
            &mut topic,
            &mut ack_types,
            &mut first,
            &mut spare_batches,
        ));
        assert_eq!(first.as_ptr(), list);
        assert_eq!(first.first().map(|part| part.2.as_ptr()), Some(batch_ptr));
        let again = &first;
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
        let mut spare_batches = Vec::new();
        let mut first = Vec::new();
        block_on_ack(super::acknowledgement_batches_with(
            &recs,
            ACK_ACCEPT,
            &mut offsets,
            &mut pairs,
            &mut topic,
            &mut ack_types,
            &mut first,
            &mut spare_batches,
        ));
        let ptr = pairs.as_ptr();
        let mut again = Vec::new();
        block_on_ack(super::acknowledgement_batches_with(
            &recs,
            ACK_ACCEPT,
            &mut offsets,
            &mut pairs,
            &mut topic,
            &mut ack_types,
            &mut again,
            &mut spare_batches,
        ));
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
        let partition_buf = pending.as_ptr();
        let kept_batches = pending.get(1).map(|part| part.2.as_ptr());
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
        assert_eq!(pending.as_ptr(), partition_buf);
        assert_eq!(pending.first().map(|part| part.2.as_ptr()), kept_batches);
    }

    #[test]
    fn remembering_many_share_leaders_does_not_hold_the_caller() {
        // 20_000 partitions: scanning the saved patches for each one held the caller 1.33s.
        let mut patches = Vec::new();
        let mut slots = HashMap::new();
        let started = std::time::Instant::now();
        for partition in 0..20_000 {
            remember_share_leader_patch(
                &mut patches,
                &mut slots,
                ShareLeaderPatch {
                    topic: "orders".to_owned(),
                    partition,
                    leader_id: 1,
                    leader_epoch: 4,
                },
            );
        }
        let elapsed = started.elapsed();
        assert_eq!(patches.len(), 20_000);
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "remembering share leaders held the caller for {elapsed:?}"
        );
        remember_share_leader_patch(
            &mut patches,
            &mut slots,
            ShareLeaderPatch {
                topic: "orders".to_owned(),
                partition: 0,
                leader_id: 2,
                leader_epoch: 9,
            },
        );
        assert_eq!(patches.len(), 20_000);
        assert_eq!(patches.first().map(|patch| patch.leader_epoch), Some(9));
        remember_share_leader_patch(
            &mut patches,
            &mut slots,
            ShareLeaderPatch {
                topic: "orders".to_owned(),
                partition: 0,
                leader_id: 3,
                leader_epoch: 3,
            },
        );
        assert_eq!(patches.first().map(|patch| patch.leader_epoch), Some(9));
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
        let mut slots = HashMap::new();
        if let Some(patch) = patch {
            remember_share_leader_patch(&mut patches, &mut slots, patch);
        }
        if let Some(older) =
            share_leader_patch(true, "orders", 0, error::NOT_LEADER_OR_FOLLOWER, 2, 4)
        {
            remember_share_leader_patch(&mut patches, &mut slots, older);
        }
        assert_eq!(patches.first().map(|patch| patch.leader_epoch), Some(5));
        if let Some(newer) =
            share_leader_patch(true, "orders", 0, error::LEADER_NOT_AVAILABLE, 2, 8)
        {
            remember_share_leader_patch(&mut patches, &mut slots, newer);
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

    #[tokio::test(flavor = "current_thread")]
    async fn stopping_a_share_heartbeat_lets_the_broker_leave() {
        use crate::net::BrokerConn;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{Duration, Instant};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "share-heartbeat", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let task = Arc::new(tokio::spawn(async move {
            let _hold = conn;
            std::future::pending::<()>().await;
        }));
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        finish_share_heartbeat(task, Instant::now()).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "stopping a share heartbeat left the broker blocked in a read"
        );
        peer.abort();
    }
}

#[cfg(test)]
mod dial_tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use super::*;
    use crate::protocol::api::ApiVersion;
    use crate::protocol::api_keys::FETCH;

    #[tokio::test(flavor = "current_thread")]
    async fn retiring_a_share_coordinator_lets_the_broker_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "share-coord-retire", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut held = Some(conn);
        retire_share_conn(&mut held).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "retiring a share coordinator left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn leaving_a_share_group_lets_the_leader_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "share-leave", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut conns = HashMap::new();
        let _stored = conns.insert(
            1,
            ShareNodeConnection {
                conn,
                fetch_version: 0,
                acknowledge_version: 0,
            },
        );
        discard_share_sockets(&mut conns).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "leaving a share group left the leader blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finishing_a_share_acknowledgement_lets_the_broker_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "share-ack-done", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        finish_share_peer(ShareNodeConnection {
            conn,
            fetch_version: 0,
            acknowledge_version: 0,
        })
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "finishing a share acknowledgement left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resetting_a_share_session_lets_the_broker_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let leader_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_addr = leader_listener.local_addr().unwrap().to_string();
        let other = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other_addr = other.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let leader_peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = leader_listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let other_peer = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = other.accept().await else {
                    return;
                };
                let _task = tokio::spawn(async move {
                    let mut buf = [0u8; 8];
                    loop {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => continue,
                        }
                    }
                });
            }
        });
        let leader = BrokerConn::connect(&leader_addr, "share-leader", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let consumer_conn =
            BrokerConn::connect(&other_addr, "share-consumer", Duration::from_secs(2))
                .await
                .unwrap();
        let coord = BrokerConn::connect(&other_addr, "share-coord", Duration::from_secs(2))
            .await
            .unwrap();
        let mut group = ShareGroup::test_holding(
            Consumer::test_holding(ConsumerConfig::default(), consumer_conn),
            coord,
            leader,
        );
        group.reset_node_session(1).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "resetting a share session left the broker blocked in a read"
        );
        drop(group);
        leader_peer.abort();
        other_peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ending_a_share_retry_watch_keeps_the_leader_socket() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.write_all(&[0x01]).await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let conn = BrokerConn::connect(&addr.to_string(), "share-keep", Duration::from_secs(2))
            .await
            .unwrap();
        let queued = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if conn.has_queued_response() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(queued.is_ok(), "the queued byte never became visible");
        let mut conns = HashMap::new();
        let _ = conns.insert(
            1,
            ShareNodeConnection {
                conn,
                fetch_version: 0,
                acknowledge_version: 0,
            },
        );
        {
            let mut waiting = std::pin::pin!(drop_closed_share_conns(&mut conns));
            tokio::select! {
                biased;
                _ = &mut waiting => panic!("the share retry watch finished before the backoff ended"),
                _ = tokio::time::sleep(Duration::from_millis(30)) => {}
            }
        }
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Err(_)),
            "ending the share retry watch shut a live leader socket: {saw:?}"
        );
        assert!(
            conns.contains_key(&1),
            "ending the share retry watch dropped the leader connection"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_byte_does_not_hide_a_later_share_close() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.write_all(&[0x01]).await.unwrap();
            gate_rx.await.unwrap();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let conn = BrokerConn::connect(&addr.to_string(), "share-byte", Duration::from_secs(2))
            .await
            .unwrap();
        let queued = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if conn.has_queued_response() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(queued.is_ok(), "the queued byte never became visible");
        let mut conns = HashMap::new();
        let _ = conns.insert(
            1,
            ShareNodeConnection {
                conn,
                fetch_version: 0,
                acknowledge_version: 0,
            },
        );
        {
            let mut waiting = std::pin::pin!(drop_closed_share_conns(&mut conns));
            tokio::select! {
                biased;
                _ = &mut waiting => panic!("a queued byte finished the share retry watch"),
                _ = tokio::time::sleep(Duration::from_millis(30)) => {}
            }
            gate_tx.send(()).unwrap();
            let noticed = tokio::time::timeout(Duration::from_millis(300), waiting).await;
            assert!(
                noticed.is_ok(),
                "a close after a queued byte was not noticed during the share retry"
            );
        }
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after it closed a share socket that already had a queued byte: {saw:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wakeup_keeps_a_share_dial_that_already_finished() {
        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap().to_string();
        let addr_b = listener_b.local_addr().unwrap().to_string();
        let wakeup = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&wakeup);
        let hold = tokio::spawn(async move {
            let accepted = tokio::join!(listener_a.accept(), listener_b.accept());
            flag.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
            drop(accepted);
        });
        let mut node_keys = HashMap::new();
        let learned = vec![ApiVersion {
            api_key: FETCH,
            min_version: 0,
            max_version: 4,
        }];
        let _ = node_keys.insert(1, learned.clone());
        let _ = node_keys.insert(2, learned);
        let opened = ShareGroup::open_share_dials(
            ShareDialConfig {
                client_id: "share-dial".to_string(),
                connect_timeout: Duration::from_secs(2),
                tls: None,
                sasl_plain: None,
                sasl_scram: None,
                sasl_scram_sha512: None,
                sasl_oauthbearer: None,
                sasl_oauthbearer_oidc: None,
                bootstrap_addr: String::new(),
                known_keys: Vec::new(),
                node_keys,
                addr_keys: HashMap::new(),
                coord_addr: String::new(),
                coord_keys: Vec::new(),
            },
            vec![(1, addr_a), (2, addr_b)],
            Instant::now() + Duration::from_secs(2),
            wakeup,
            tokio::sync::watch::channel(false).1,
        )
        .await;
        hold.abort();
        let mut nodes: Vec<i32> = opened.iter().map(|dial| dial.node).collect();
        nodes.sort_unstable();
        assert_eq!(nodes, vec![1, 2]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wakeup_stops_a_blocked_share_dial() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let accepted_flag = Arc::clone(&accepted);
        let dropped_flag = Arc::clone(&dropped);
        let peer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            accepted_flag.store(true, Ordering::SeqCst);
            let mut buf = [0u8; 1];
            let _read = sock.read(&mut buf).await;
            dropped_flag.store(true, Ordering::SeqCst);
        });
        let wakeup = Arc::new(AtomicBool::new(false));
        let (wake_tx, wake_rx) = tokio::sync::watch::channel(false);
        let dial = ShareGroup::open_share_dials(
            ShareDialConfig {
                client_id: "share-wake".to_string(),
                connect_timeout: Duration::from_secs(30),
                tls: None,
                sasl_plain: None,
                sasl_scram: None,
                sasl_scram_sha512: None,
                sasl_oauthbearer: None,
                sasl_oauthbearer_oidc: None,
                bootstrap_addr: String::new(),
                known_keys: Vec::new(),
                node_keys: HashMap::new(),
                addr_keys: HashMap::new(),
                coord_addr: String::new(),
                coord_keys: Vec::new(),
            },
            vec![(2, addr)],
            Instant::now() + Duration::from_secs(2),
            Arc::clone(&wakeup),
            wake_rx,
        );
        tokio::pin!(dial);
        while !accepted.load(Ordering::SeqCst) {
            tokio::select! {
                biased;
                _ = &mut dial => panic!("dial returned before the broker accepted"),
                () = tokio::task::yield_now() => {}
            }
        }
        wakeup.store(true, Ordering::SeqCst);
        wake_tx.send(true).unwrap_or(());
        let started = Instant::now();
        let opened = (&mut dial).await;
        assert!(
            opened.is_empty(),
            "a blocked handshake is not a finished dial"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "wakeup waited for the blocked handshake: {:?}",
            started.elapsed()
        );
        assert!(
            dropped.load(Ordering::SeqCst),
            "wakeup left the share dial blocked in a broker read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finishing_aborted_share_fetches_stops_the_task() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&accepted);
        let hold = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            flag.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
            drop(sock);
        });
        let mut set = tokio::task::JoinSet::new();
        let handle = set.spawn(async move {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut buf = [0u8; 1];
            let _n = sock.read(&mut buf).await;
        });
        while !accepted.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        assert!(
            !handle.is_finished(),
            "the fetch should still be waiting on the broker"
        );
        let started = Instant::now();
        finish_aborted_join_set(set, Instant::now()).await;
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "finish waited for the broker read: {:?}",
            started.elapsed()
        );
        assert!(
            handle.is_finished(),
            "finish returned while the share fetch was still alive"
        );
        hold.abort();
    }
}
