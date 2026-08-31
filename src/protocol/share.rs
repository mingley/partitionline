//! Share groups (KIP-932): ShareGroupHeartbeat (76), ShareFetch (78),
//! ShareAcknowledge (79). ShareGroupHeartbeat is flexible from v0
//! (Kafka 4.0 early access v0; Kafka 4.1 stable v1). This crate
//! speaks 0–1. Same fields. v2+ is not spoken. ShareFetch is flexible
//! from v0. Kafka 4.0 `validVersions` is `"0"`; Kafka 4.1 `"1"`
//! (v0 removed). This crate speaks 0–1. v0 and v1 fields differ
//! (v0 PartitionMaxBytes; v1 MaxRecords / BatchSize /
//! AcquisitionLockTimeoutMs). v2+ is not spoken. ShareAcknowledge is
//! flexible from v0. Kafka 4.0 `validVersions` is `"0"`; Kafka 4.1 `"1"`
//! (v0 removed). This crate speaks 0–1. Same fields. v2+ is not spoken.

use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::api::NodeEndpoint;
use super::buf;
use super::records::{self, RecordBatch};
use crate::error::{Error, Result};

/// Gap in an acknowledgement batch.
pub const ACK_GAP: i8 = 0;
/// Accept an acquired record.
pub const ACK_ACCEPT: i8 = 1;
/// Release an acquired record back to available.
pub const ACK_RELEASE: i8 = 2;
/// Reject an acquired record.
pub const ACK_REJECT: i8 = 3;

/// Topic UUID plus partition indexes in a share-group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareTopicPartitions {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// Assigned partition indexes.
    pub partitions: Vec<i32>,
}

/// ShareGroupHeartbeat request (join, heartbeat, or leave).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatRequest {
    /// Group id.
    pub group_id: String,
    /// Member id (`""` on join).
    pub member_id: String,
    /// Member epoch ([`Self::JOIN_GROUP_MEMBER_EPOCH`] join,
    /// [`Self::LEAVE_GROUP_MEMBER_EPOCH`] leave, otherwise heartbeat).
    pub member_epoch: i32,
    /// Subscribed topic names (`None` means unchanged).
    pub subscribed_topic_names: Option<Vec<String>>,
}

impl ShareGroupHeartbeatRequest {
    /// Java `ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
    ///
    /// Kafka 4.0 ShareGroupHeartbeat has no static-member leave epoch.
    pub const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;
    /// Java `ShareGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH`.
    pub const JOIN_GROUP_MEMBER_EPOCH: i32 = 0;
}

/// ShareGroupHeartbeat response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatResponse {
    /// ShareGroupHeartbeat `ThrottleTimeMs` (JSON `0+`). JSON default is `0`.
    pub throttle_time_ms: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message.
    pub error_message: Option<String>,
    /// Assigned member id.
    pub member_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Next heartbeat interval.
    pub heartbeat_interval_ms: i32,
    /// New assignment, or `None` when unchanged.
    pub assignment: Option<Vec<ShareTopicPartitions>>,
}

/// One contiguous offset range in ShareAcknowledge / ShareFetch acks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgementBatch {
    /// First offset in the range.
    pub first_offset: i64,
    /// Last offset in the range (inclusive).
    pub last_offset: i64,
    /// Per-offset ack type ([`ACK_ACCEPT`], [`ACK_RELEASE`], [`ACK_REJECT`], [`ACK_GAP`]).
    pub types: Vec<i8>,
}

/// One partition in a ShareFetch request.
///
/// [`Self::partition_max_bytes`] is Java `PartitionMaxBytes` (v0). v1
/// omits the field (decode fills `0`). Encode writes this value on v0,
/// not the request-level MaxBytes. [`ShareFetchRequest::for_consumer`]
/// fills it from `fetchSize` (or `0` for ack-only partitions when the
/// share session is closing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchPartition {
    /// Partition index.
    pub partition: i32,
    /// Java `ShareFetchRequestData.FetchPartition.partitionMaxBytes` (v0).
    /// v1 omits the field; decode fills `0`.
    pub partition_max_bytes: i32,
    /// Acknowledgements piggybacked on this fetch.
    pub acknowledgements: Vec<AcknowledgementBatch>,
}

/// One topic in a ShareFetch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchTopic {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// Partitions to fetch.
    pub partitions: Vec<ShareFetchPartition>,
}

/// One forgotten topic in a ShareFetch request (session increment).
///
/// Java `ShareFetchRequestData.ForgottenTopic`.
/// [`encode_share_fetch_request_with_forgotten`] writes this list;
/// [`encode_share_fetch_request`] still writes empty. This type is also
/// the in-memory list [`ShareFetchRequest::forgotten_topics`] reads and
/// [`ShareFetchRequest::update_forgotten_data`] builds from a forget
/// list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareForgottenTopic {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// Partitions to forget.
    pub partitions: Vec<i32>,
}

/// Java `ShareFetchRequest` helpers.
pub struct ShareFetchRequest;

impl ShareFetchRequest {
    /// Java `ShareFetchRequest.forgottenTopics`.
    ///
    /// Looks up each topic id in `topic_names` (`None` when missing; Java
    /// still inserts that `TopicIdPartition`). Duplicate partitions are
    /// kept (`ArrayList`). [`encode_share_fetch_request_with_forgotten`]
    /// writes the list; [`encode_share_fetch_request`] still writes empty.
    /// Unlike [`ShareFetchResponse::response_data`], a missing name
    /// is not skipped.
    #[must_use]
    pub fn forgotten_topics(
        forgotten: &[ShareForgottenTopic],
        topic_names: &HashMap<[u8; 16], String>,
    ) -> Vec<([u8; 16], Option<String>, i32)> {
        let mut to_forget = Vec::new();
        for topic in forgotten {
            let name = topic_names.get(&topic.topic_id).cloned();
            for partition in &topic.partitions {
                to_forget.push((topic.topic_id, name.clone(), *partition));
            }
        }
        to_forget
    }

    /// Java `ShareFetchRequest.Builder.updateForgottenData`.
    ///
    /// Groups the forget list by topic id (Java `HashMap`; first-seen id
    /// order — `HashMap.forEach` order is unspecified). Partitions for
    /// the same id append (`ArrayList`, duplicates kept). The grouped
    /// entries are **appended** to `forgotten` (a second call with the
    /// same id is another list entry, not a merge).
    /// [`encode_share_fetch_request_with_forgotten`] writes the list;
    /// [`encode_share_fetch_request`] still writes empty. Distinct from
    /// [`Self::forgotten_topics`], which flattens an already-built list
    /// and looks up names, and from
    /// [`crate::protocol::fetch::FetchRequest::forgotten_from_removed`],
    /// which groups by topic **name**.
    #[must_use]
    pub fn update_forgotten_data<I>(
        forgotten: &[ShareForgottenTopic],
        forget: I,
    ) -> Vec<ShareForgottenTopic>
    where
        I: IntoIterator<Item = ([u8; 16], i32)>,
    {
        let mut out = forgotten.to_vec();
        let mut order: Vec<[u8; 16]> = Vec::new();
        let mut by_id: HashMap<[u8; 16], Vec<i32>> = HashMap::new();
        for (topic_id, partition) in forget {
            by_id
                .entry(topic_id)
                .or_insert_with(|| {
                    order.push(topic_id);
                    Vec::new()
                })
                .push(partition);
        }
        for topic_id in order {
            if let Some(partitions) = by_id.remove(&topic_id) {
                out.push(ShareForgottenTopic {
                    topic_id,
                    partitions,
                });
            }
        }
        out
    }

    /// Java `ShareFetchRequest.shareFetchData`.
    ///
    /// Looks up each topic id in `topic_names` (`None` when missing; Java
    /// still inserts that `TopicIdPartition`). Values are
    /// `PartitionMaxBytes` (Java `SharePartitionData.maxBytes`). A later
    /// partition overwrites the same triple (Java `LinkedHashMap.put`).
    /// Unlike [`ShareFetchResponse::response_data`], a missing name is
    /// not skipped. Distinct from [`Self::forgotten_topics`], which
    /// keeps duplicate partitions (`ArrayList`).
    #[must_use]
    pub fn share_fetch_data(
        topics: &[ShareFetchTopic],
        topic_names: &HashMap<[u8; 16], String>,
    ) -> HashMap<([u8; 16], Option<String>, i32), i32> {
        let mut share_fetch_data = HashMap::new();
        for topic in topics {
            let name = topic_names.get(&topic.topic_id).cloned();
            for partition in &topic.partitions {
                let _prev = share_fetch_data.insert(
                    (topic.topic_id, name.clone(), partition.partition),
                    partition.partition_max_bytes,
                );
            }
        }
        share_fetch_data
    }

    /// Java `ShareFetchRequest.Builder.forConsumer` Topics.
    ///
    /// Groups send partitions and piggybacked acknowledgements by topic
    /// id (Java `HashMap`; first-seen id order — `HashMap.forEach`
    /// order is unspecified). A later partition for the same id appends
    /// in first-seen partition order. Duplicate `(id, partition)` on
    /// send **replaces** the partition body (Java `HashMap.put`, last
    /// wins). An acknowledgement for an existing partition replaces the
    /// batches (`setAcknowledgementBatches`) and keeps that partition's
    /// `PartitionMaxBytes`. An acknowledgement-only partition uses
    /// `fetch_size`, or `0` when `is_closing_share_session` (Java
    /// `ShareRequestMetadata.isFinalEpoch`). Closing skips the send
    /// list. Empty is empty Topics. Topic name is not used (Java
    /// `TopicIdPartition.topicId` / `partition` only). GroupId,
    /// MemberId, ShareSessionEpoch, MaxWaitMs / MinBytes / MaxBytes,
    /// MaxRecords / BatchSize, and ForgottenTopicsData stay with the
    /// encode caller.
    /// [`Self::update_forgotten_data`] is the forget-list half.
    /// Encode writes each partition's `partition_max_bytes` on v0.
    /// Distinct from [`ShareAcknowledgeRequest::for_consumer`], which
    /// has no send list or `PartitionMaxBytes`, and from
    /// [`ShareFetchResponse::to_message`], which groups response bodies.
    #[must_use]
    pub fn for_consumer<S, A>(
        is_closing_share_session: bool,
        fetch_size: i32,
        send: S,
        acknowledgements: A,
    ) -> Vec<ShareFetchTopic>
    where
        S: IntoIterator<Item = ([u8; 16], i32)>,
        A: IntoIterator<Item = ([u8; 16], i32, Vec<AcknowledgementBatch>)>,
    {
        let ack_only_partition_max_bytes = if is_closing_share_session {
            0
        } else {
            fetch_size
        };
        let mut topic_order = Vec::new();
        let mut by_id = HashMap::new();
        if is_closing_share_session {
            drop(send);
        } else {
            for (topic_id, partition) in send {
                let (part_order, part_map) = by_id.entry(topic_id).or_insert_with(|| {
                    topic_order.push(topic_id);
                    (Vec::new(), HashMap::new())
                });
                if part_map
                    .insert(
                        partition,
                        ShareFetchPartition {
                            partition,
                            partition_max_bytes: fetch_size,
                            acknowledgements: Vec::new(),
                        },
                    )
                    .is_none()
                {
                    part_order.push(partition);
                }
            }
        }
        for (topic_id, partition, batches) in acknowledgements {
            let (part_order, part_map) = by_id.entry(topic_id).or_insert_with(|| {
                topic_order.push(topic_id);
                (Vec::new(), HashMap::new())
            });
            if let Some(existing) = part_map.get_mut(&partition) {
                existing.acknowledgements = batches;
            } else {
                part_order.push(partition);
                let _prev = part_map.insert(
                    partition,
                    ShareFetchPartition {
                        partition,
                        partition_max_bytes: ack_only_partition_max_bytes,
                        acknowledgements: batches,
                    },
                );
            }
        }
        let mut topics = Vec::with_capacity(topic_order.len());
        for topic_id in topic_order {
            let Some((part_order, mut part_map)) = by_id.remove(&topic_id) else {
                continue;
            };
            let mut partitions = Vec::with_capacity(part_order.len());
            for partition in part_order {
                if let Some(part) = part_map.remove(&partition) {
                    partitions.push(part);
                }
            }
            topics.push(ShareFetchTopic {
                topic_id,
                partitions,
            });
        }
        topics
    }
}

/// Acquired offset range in a ShareFetch response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredRange {
    /// First acquired offset.
    pub first_offset: i64,
    /// Last acquired offset (inclusive).
    pub last_offset: i64,
    /// Delivery count for this range.
    pub delivery_count: i16,
}

/// One partition in a ShareFetch response.
///
/// [`Self::partition_response`] is Java `ShareFetchResponse.partitionResponse`
/// (`PartitionIndex` and `ErrorCode`). Records and acquired ranges stay
/// empty. Official Java leaves ErrorMessage, AcknowledgeErrorCode,
/// AcknowledgeErrorMessage, CurrentLeader, and Records at JSON defaults
/// (null / 0 / 0/0 / null). Crate encode writes ErrorMessage from the
/// partition fields (JSON default null), AcknowledgeErrorCode from the
/// partition fields (JSON default 0), AcknowledgeErrorMessage from the
/// partition fields (JSON default null), CurrentLeader from the partition
/// fields (JSON default 0/0), empty Records, empty AcquiredRecords.
/// NodeEndpoints stay empty on [`encode_share_fetch_response`];
/// [`encode_share_fetch_response_with_endpoints`] writes a non-empty list.
/// v1 AcquisitionLockTimeoutMs is JSON `1+`
/// ([`encode_share_fetch_response_with_acquisition_lock_timeout`];
/// [`encode_share_fetch_response`] still writes 15000). Top-level
/// ErrorCode is JSON `0+`
/// ([`encode_share_fetch_response_with_error_code`];
/// [`encode_share_fetch_response`] still writes `0`). ThrottleTimeMs is
/// JSON `0+` ([`encode_share_fetch_response_with_throttle`];
/// [`encode_share_fetch_response`] still writes `0`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchedPartition {
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// ShareFetch partition `ErrorMessage` (JSON `0+` nullable compact
    /// STRING). JSON default is null. This is not the top-level
    /// `ErrorMessage`.
    pub error_message: Option<String>,
    /// ShareFetch `AcknowledgeErrorCode` (JSON `0+`). JSON default is `0`.
    /// This is not fetch `ErrorCode`. JSON lists `INVALID_RECORD_STATE` as
    /// acknowledge-only.
    pub acknowledge_error_code: i16,
    /// ShareFetch `AcknowledgeErrorMessage` (JSON `0+` nullable compact
    /// STRING). JSON default is null. This is not fetch `ErrorMessage`.
    pub acknowledge_error_message: Option<String>,
    /// ShareFetch CurrentLeader `LeaderId` (JSON `0+` untagged nested
    /// `LeaderIdAndEpoch`). JSON default is `0` (`-1` means unknown).
    pub current_leader_id: i32,
    /// ShareFetch CurrentLeader `LeaderEpoch` (JSON `0+` untagged nested
    /// `LeaderIdAndEpoch`). JSON default is `0`.
    pub current_leader_epoch: i32,
    /// Record batches.
    pub records: Vec<RecordBatch>,
    /// Offsets acquired by this member.
    pub acquired: Vec<AcquiredRange>,
}

impl ShareFetchedPartition {
    /// Java `ShareFetchResponse.partitionResponse(int, Errors)`.
    ///
    /// Sets `PartitionIndex` and `ErrorCode`. Records and acquired ranges
    /// stay empty. Official Java leaves ErrorMessage, AcknowledgeErrorCode,
    /// AcknowledgeErrorMessage, CurrentLeader, and Records at JSON defaults
    /// (null / 0 / 0/0 / null). Crate encode writes ErrorMessage from the
    /// partition fields (JSON default null), AcknowledgeErrorCode from the
    /// partition fields (JSON default 0), AcknowledgeErrorMessage from the
    /// partition fields (JSON default null), CurrentLeader from the
    /// partition fields (JSON default 0/0), empty Records, empty
    /// AcquiredRecords. NodeEndpoints stay empty on
    /// [`encode_share_fetch_response`];
    /// [`encode_share_fetch_response_with_endpoints`] writes a non-empty
    /// list. v1 AcquisitionLockTimeoutMs is JSON `1+`
    /// ([`encode_share_fetch_response_with_acquisition_lock_timeout`];
    /// [`encode_share_fetch_response`] still writes 15000). Top-level
    /// ErrorCode is JSON `0+`
    /// ([`encode_share_fetch_response_with_error_code`];
    /// [`encode_share_fetch_response`] still writes `0`). ThrottleTimeMs
    /// is JSON `0+` ([`encode_share_fetch_response_with_throttle`];
    /// [`encode_share_fetch_response`] still writes `0`).
    #[must_use]
    pub fn partition_response(partition: i32, error_code: i16) -> Self {
        Self {
            partition,
            error_code,
            error_message: None,
            acknowledge_error_code: 0,
            acknowledge_error_message: None,
            current_leader_id: 0,
            current_leader_epoch: 0,
            records: Vec::new(),
            acquired: Vec::new(),
        }
    }

    /// Java `ShareFetchResponse.recordsSize`.
    ///
    /// `0` when records are empty (Java `null` or `MemoryRecords.EMPTY`).
    /// Otherwise the encoded size of the records blob.
    pub fn records_size(&self) -> Result<i32> {
        if self.records.is_empty() {
            return Ok(0);
        }
        let mut recs = BytesMut::new();
        for batch in &self.records {
            records::encode_record_batch(&mut recs, batch)?;
        }
        buf::i32_from_usize(recs.len())
    }
}

/// One topic in a ShareFetch response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchedTopic {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// Partition bodies.
    pub partitions: Vec<ShareFetchedPartition>,
}

/// Java `ShareFetchResponse` helpers.
pub struct ShareFetchResponse;

impl ShareFetchResponse {
    /// Java `ShareFetchResponse.errorCounts`.
    ///
    /// Counts the top-level `errorCode` (including `NONE`) plus each
    /// partition-level code (including `NONE`). Decode returns the
    /// top-level code and does not fail on a non-zero value. Convenience
    /// encode still writes `0`.
    #[must_use]
    pub fn error_counts(error_code: i16, topics: &[ShareFetchedTopic]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        let count = counts.entry(error_code).or_insert(0);
        *count += 1;
        for topic in topics {
            for partition in &topic.partitions {
                let count = counts.entry(partition.error_code).or_insert(0);
                *count += 1;
            }
        }
        counts
    }

    /// Java `ShareFetchResponse.responseData`.
    ///
    /// Looks up each topic id in `topic_names` and skips a topic whose
    /// id is missing (Java `name != null`). Keys are
    /// `(topic_id, name, partition)` (Java `TopicIdPartition`). A later
    /// partition overwrites the same triple (Java `LinkedHashMap.put`).
    #[must_use]
    pub fn response_data(
        topics: &[ShareFetchedTopic],
        topic_names: &HashMap<[u8; 16], String>,
    ) -> HashMap<([u8; 16], String, i32), ShareFetchedPartition> {
        let mut response_data = HashMap::new();
        for topic in topics {
            let Some(name) = topic_names.get(&topic.topic_id) else {
                continue;
            };
            for partition in &topic.partitions {
                let _prev = response_data.insert(
                    (topic.topic_id, name.clone(), partition.partition),
                    partition.clone(),
                );
            }
        }
        response_data
    }

    /// Java `ShareFetchResponse.toMessage` Responses grouping.
    ///
    /// `entries` are `(topic_id, partition)` plus a body. Java
    /// `setPartitionIndex` copies the key partition onto each body.
    /// Topics are grouped by id in first-seen order (Java
    /// `LinkedHashMap`). A later entry for an already-seen topic is
    /// appended, including when another topic sits in between. Throttle,
    /// top-level error, and NodeEndpoints stay with crate encode (`0` /
    /// empty).
    #[must_use]
    pub fn to_message(
        entries: &[([u8; 16], i32, ShareFetchedPartition)],
    ) -> Vec<ShareFetchedTopic> {
        let mut topics: Vec<ShareFetchedTopic> = Vec::new();
        for (topic_id, partition, body) in entries {
            let mut body = body.clone();
            body.partition = *partition;
            if let Some(topic) = topics.iter_mut().find(|topic| topic.topic_id == *topic_id) {
                topic.partitions.push(body);
            } else {
                topics.push(ShareFetchedTopic {
                    topic_id: *topic_id,
                    partitions: vec![body],
                });
            }
        }
        topics
    }
}

/// One partition in a ShareAcknowledge response.
///
/// [`Self::partition_response`] is Java `ShareAcknowledgeResponse.partitionResponse`
/// (`PartitionIndex` and `ErrorCode`). Official Java leaves ErrorMessage
/// and CurrentLeader at JSON defaults (null / 0/0). Crate encode writes
/// ErrorMessage from the partition fields (JSON default null),
/// CurrentLeader from the partition fields (JSON default 0/0).
/// NodeEndpoints stay empty on
/// [`encode_share_acknowledge_topics_response`];
/// [`encode_share_acknowledge_topics_response_with_endpoints`] writes a
/// non-empty list.
/// Top-level ErrorCode stays 0 (crate encode of this factory). ThrottleTimeMs
/// is JSON `0+` ([`encode_share_acknowledge_topics_response_with_throttle`];
/// [`encode_share_acknowledge_topics_response`] still writes `0`). Official
/// Java `ShareAcknowledgeRequest.getErrorResponse` writes ThrottleTimeMs
/// plus the top-level ErrorCode (empty Responses); crate
/// [`encode_share_acknowledge_response`] still writes ThrottleTimeMs `0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeResponsePartition {
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// ShareAcknowledge partition `ErrorMessage` (JSON `0+` nullable compact
    /// STRING). JSON default is null. This is not the top-level
    /// `ErrorMessage`.
    pub error_message: Option<String>,
    /// ShareAcknowledge CurrentLeader `LeaderId` (JSON `0+` untagged nested
    /// `LeaderIdAndEpoch`). JSON default is `0` (`-1` means unknown).
    pub current_leader_id: i32,
    /// ShareAcknowledge CurrentLeader `LeaderEpoch` (JSON `0+` untagged nested
    /// `LeaderIdAndEpoch`). JSON default is `0`.
    pub current_leader_epoch: i32,
}

impl ShareAcknowledgeResponsePartition {
    /// Java `ShareAcknowledgeResponse.partitionResponse(int, Errors)`.
    ///
    /// Sets `PartitionIndex` and `ErrorCode`. Official Java leaves
    /// ErrorMessage and CurrentLeader at JSON defaults (null / 0/0).
    /// Crate encode writes ErrorMessage from the partition fields (JSON
    /// default null), CurrentLeader from the partition fields (JSON
    /// default 0/0). NodeEndpoints stay empty on
    /// [`encode_share_acknowledge_topics_response`];
    /// [`encode_share_acknowledge_topics_response_with_endpoints`] writes
    /// a non-empty list. Top-level ErrorCode stays 0 (crate encode
    /// of this factory). ThrottleTimeMs is JSON `0+`
    /// ([`encode_share_acknowledge_topics_response_with_throttle`];
    /// [`encode_share_acknowledge_topics_response`] still writes `0`).
    #[must_use]
    pub fn partition_response(partition: i32, error_code: i16) -> Self {
        Self {
            partition,
            error_code,
            error_message: None,
            current_leader_id: 0,
            current_leader_epoch: 0,
        }
    }
}

/// One topic in a ShareAcknowledge response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeResponseTopic {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// Partition bodies.
    pub partitions: Vec<ShareAcknowledgeResponsePartition>,
}

/// Java `ShareAcknowledgeResponse` helpers.
pub struct ShareAcknowledgeResponse;

impl ShareAcknowledgeResponse {
    /// Java `ShareAcknowledgeResponse.errorCounts`.
    ///
    /// Counts the top-level `errorCode` (including `NONE`) plus each
    /// partition-level code (including `NONE`).
    #[must_use]
    pub fn error_counts(
        error_code: i16,
        topics: &[ShareAcknowledgeResponseTopic],
    ) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        let count = counts.entry(error_code).or_insert(0);
        *count += 1;
        for topic in topics {
            for partition in &topic.partitions {
                let count = counts.entry(partition.error_code).or_insert(0);
                *count += 1;
            }
        }
        counts
    }

    /// Java `ShareAcknowledgeResponse.toMessage` Responses grouping.
    ///
    /// `entries` are `(topic_id, partition)` plus a body. Java
    /// `setPartitionIndex` copies the key partition onto each body.
    /// Topics are grouped by id in first-seen order (Java
    /// `LinkedHashMap`). A later entry for an already-seen topic is
    /// appended, including when another topic sits in between. Throttle
    /// and top-level error stay with crate encode (`0`). NodeEndpoints
    /// stay empty on [`encode_share_acknowledge_topics_response`];
    /// [`encode_share_acknowledge_topics_response_with_endpoints`] writes
    /// a non-empty list.
    #[must_use]
    pub fn to_message(
        entries: &[([u8; 16], i32, ShareAcknowledgeResponsePartition)],
    ) -> Vec<ShareAcknowledgeResponseTopic> {
        let mut topics: Vec<ShareAcknowledgeResponseTopic> = Vec::new();
        for (topic_id, partition, body) in entries {
            let mut body = body.clone();
            body.partition = *partition;
            if let Some(topic) = topics.iter_mut().find(|topic| topic.topic_id == *topic_id) {
                topic.partitions.push(body);
            } else {
                topics.push(ShareAcknowledgeResponseTopic {
                    topic_id: *topic_id,
                    partitions: vec![body],
                });
            }
        }
        topics
    }
}

/// `true` when ShareGroupHeartbeat `version` is flexible.
///
/// v0 and v1 are both flexible (`flexibleVersions: "0+"`). Kafka 4.0
/// `validVersions` is `"0"` (`latestVersionUnstable`). Kafka 4.1
/// `validVersions` is `"1"` (v0 removed). This crate speaks 0–1.
/// Same fields. v2+ is not spoken.
fn share_group_heartbeat_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(true),
        other => Err(Error::protocol(format!(
            "ShareGroupHeartbeat version {other} is not implemented"
        ))),
    }
}

/// Encode a flexible ShareGroupHeartbeat request (v0–v1). Same fields.
pub fn encode_share_group_heartbeat_request(
    buf: &mut BytesMut,
    version: i16,
    req: &ShareGroupHeartbeatRequest,
) -> crate::error::Result<()> {
    let flexible = share_group_heartbeat_flexible(version)?;
    buf::put_string(buf, flexible, Some(&req.group_id))?;
    buf::put_string(buf, flexible, Some(&req.member_id))?;
    buf.put_i32(req.member_epoch);
    buf::put_string(buf, flexible, None)?;
    match &req.subscribed_topic_names {
        None => buf::put_array_len(buf, flexible, None)?,
        Some(names) => {
            buf::put_array_len(buf, flexible, Some(names.len()))?;
            for n in names {
                buf::put_string(buf, flexible, Some(n))?;
            }
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a flexible ShareGroupHeartbeat request (v0–v1).
pub fn decode_share_group_heartbeat_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ShareGroupHeartbeatRequest> {
    let flexible = share_group_heartbeat_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let member_epoch = buf::get_i32(buf)?;
    let _rack = buf::get_string(buf, flexible)?;
    let subscribed_topic_names = {
        let n = buf::get_array_len(buf, flexible)?;
        match n {
            None => None,
            Some(n) => {
                let mut names = Vec::with_capacity(n);
                for _ in 0..n {
                    names.push(buf::get_string(buf, flexible)?.unwrap_or_default());
                }
                Some(names)
            }
        }
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ShareGroupHeartbeatRequest {
        group_id,
        member_id,
        member_epoch,
        subscribed_topic_names,
    })
}

/// Encode a flexible ShareGroupHeartbeat response (v0–v1). Same fields.
///
/// ThrottleTimeMs is JSON `0+` (from [`ShareGroupHeartbeatResponse::throttle_time_ms`];
/// JSON default `0`).
pub fn encode_share_group_heartbeat_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ShareGroupHeartbeatResponse,
) -> crate::error::Result<()> {
    let flexible = share_group_heartbeat_flexible(version)?;
    buf.put_i32(resp.throttle_time_ms);
    buf.put_i16(resp.error_code);
    buf::put_string(buf, flexible, resp.error_message.as_deref())?;
    buf::put_string(buf, flexible, resp.member_id.as_deref())?;
    buf.put_i32(resp.member_epoch);
    buf.put_i32(resp.heartbeat_interval_ms);
    match &resp.assignment {
        None => buf::put_unsigned_varint(buf, 0),
        Some(parts) => {
            buf::put_unsigned_varint(buf, 1);
            buf::put_array_len(buf, flexible, Some(parts.len()))?;
            for t in parts {
                buf.extend_from_slice(&t.topic_id);
                buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
                for p in &t.partitions {
                    buf.put_i32(*p);
                }
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a flexible ShareGroupHeartbeat response (v0–v1).
///
/// ThrottleTimeMs is JSON `0+` (always on the wire).
pub fn decode_share_group_heartbeat_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ShareGroupHeartbeatResponse> {
    let flexible = share_group_heartbeat_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_string(buf, flexible)?;
    let member_id = buf::get_string(buf, flexible)?;
    let member_epoch = buf::get_i32(buf)?;
    let heartbeat_interval_ms = buf::get_i32(buf)?;
    let present = buf::get_unsigned_varint(buf)?;
    let assignment = if present == 0 {
        None
    } else {
        let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut parts = Vec::with_capacity(n);
        for _ in 0..n {
            let topic_id = buf::get_uuid(buf)?;
            let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                partitions.push(buf::get_i32(buf)?);
            }
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            parts.push(ShareTopicPartitions {
                topic_id,
                partitions,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        Some(parts)
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ShareGroupHeartbeatResponse {
        throttle_time_ms,
        error_code,
        error_message,
        member_id,
        member_epoch,
        heartbeat_interval_ms,
        assignment,
    })
}

fn share_fetch_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(true),
        other => Err(Error::protocol(format!(
            "ShareFetch version {other} is not implemented"
        ))),
    }
}

fn encode_ack_batches(
    buf: &mut BytesMut,
    batches: &[AcknowledgementBatch],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(batches.len()))?;
    for b in batches {
        buf.put_i64(b.first_offset);
        buf.put_i64(b.last_offset);
        buf::put_array_len(buf, true, Some(b.types.len()))?;
        for t in &b.types {
            buf.put_i8(*t);
        }
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

fn decode_ack_batches<B: Buf>(buf: &mut B) -> Result<Vec<AcknowledgementBatch>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let first_offset = buf::get_i64(buf)?;
        let last_offset = buf::get_i64(buf)?;
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut types = Vec::with_capacity(tn);
        for _ in 0..tn {
            types.push(buf::get_i8(buf)?);
        }
        buf::skip_tagged_fields(buf)?;
        out.push(AcknowledgementBatch {
            first_offset,
            last_offset,
            types,
        });
    }
    Ok(out)
}

#[expect(
    clippy::too_many_arguments,
    reason = "ShareFetch v0–v1 body fields are a single wire encode"
)]
/// Encode a ShareFetch request (`version` 0–1).
///
/// Kafka 4.0 JSON (`apiKey: 78`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, `latestVersionUnstable: true`) and Kafka
/// 4.1 JSON (`validVersions: "1"` — v0 removed). This crate speaks 0–1.
/// v1 adds MaxRecords / BatchSize after MaxBytes and omits
/// PartitionMaxBytes (v0 only). [`encode_share_fetch_request`] still
/// writes BatchSize as MaxRecords ([`encode_share_fetch_request_with_batch_size`]
/// writes a distinct value). v0 PartitionMaxBytes is each partition's
/// [`ShareFetchPartition::partition_max_bytes`] (Java
/// `Builder.forConsumer` `fetchSize`, not request-level MaxBytes).
/// ForgottenTopicsData stays empty ([`encode_share_fetch_request_with_forgotten`]
/// writes a non-empty list). MaxWaitMs is JSON `0+` (decode returns it).
/// MinBytes is JSON `0+` (decode returns it; encode already takes `min_bytes`).
/// MaxBytes is JSON `0+` (decode returns it; encode already takes `max_bytes`;
/// JSON default `0x7fffffff`).
/// v2+ is not spoken.
pub fn encode_share_fetch_request(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    topics: &[ShareFetchTopic],
) -> crate::error::Result<()> {
    encode_share_fetch_request_with_forgotten(
        buf,
        version,
        group_id,
        member_id,
        share_session_epoch,
        max_wait_ms,
        min_bytes,
        max_bytes,
        max_records,
        topics,
        &[],
    )
}

/// Encode ShareFetch plus ForgottenTopicsData.
///
/// ForgottenTopicsData is JSON `0+` (on the wire for every spoken
/// version). [`encode_share_fetch_request`] still writes empty.
/// Duplicate partition indexes are kept. v1 BatchSize stays equal to
/// MaxRecords on this helper
/// ([`encode_share_fetch_request_with_batch_size`] writes a distinct
/// value).
#[expect(
    clippy::too_many_arguments,
    reason = "ShareFetch ForgottenTopicsData is encoded with the rest of the v0–v1 body"
)]
pub fn encode_share_fetch_request_with_forgotten(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    topics: &[ShareFetchTopic],
    forgotten: &[ShareForgottenTopic],
) -> crate::error::Result<()> {
    encode_share_fetch_request_fields(
        buf,
        version,
        group_id,
        member_id,
        share_session_epoch,
        max_wait_ms,
        min_bytes,
        max_bytes,
        max_records,
        topics,
        forgotten,
        max_records,
    )
}

/// Encode ShareFetch v0–v1 with BatchSize.
///
/// BatchSize is JSON `1+` (INT32 after MaxRecords). Kafka 4.1.0 JSON has
/// no default; generated Java int32 default is `0`. Official Java
/// `ShareFetchRequest.Builder.forConsumer` takes `maxRecords` and
/// `batchSize` as separate arguments. [`encode_share_fetch_request`]
/// still writes BatchSize as MaxRecords. v0 omits the field even when
/// the body is non-zero; decode fills `0`. ForgottenTopicsData stays
/// empty on this helper. This is not MaxRecords, not MaxBytes, not v0
/// PartitionMaxBytes, and not response AcquisitionLockTimeoutMs.
#[expect(
    clippy::too_many_arguments,
    reason = "ShareFetch BatchSize is encoded with the rest of the v0–v1 body"
)]
pub fn encode_share_fetch_request_with_batch_size(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    topics: &[ShareFetchTopic],
    batch_size: i32,
) -> crate::error::Result<()> {
    encode_share_fetch_request_fields(
        buf,
        version,
        group_id,
        member_id,
        share_session_epoch,
        max_wait_ms,
        min_bytes,
        max_bytes,
        max_records,
        topics,
        &[],
        batch_size,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "ShareFetch request body fields are a single wire encode"
)]
fn encode_share_fetch_request_fields(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    topics: &[ShareFetchTopic],
    forgotten: &[ShareForgottenTopic],
    batch_size: i32,
) -> crate::error::Result<()> {
    let flexible = share_fetch_flexible(version)?;
    buf::put_string(buf, flexible, Some(group_id))?;
    buf::put_string(buf, flexible, Some(member_id))?;
    buf.put_i32(share_session_epoch);
    buf.put_i32(max_wait_ms);
    buf.put_i32(min_bytes);
    buf.put_i32(max_bytes);
    if version >= 1 {
        buf.put_i32(max_records);
        buf.put_i32(batch_size);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            if version == 0 {
                buf.put_i32(p.partition_max_bytes);
            }
            encode_ack_batches(buf, &p.acknowledgements)?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf::put_array_len(buf, flexible, Some(forgotten.len()))?;
    for t in forgotten {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(*p);
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ShareFetch request (`version` 0–1):
/// `(group_id, member_id, epoch, max_records, topics, forgotten,
/// batch_size, max_wait_ms, min_bytes, max_bytes)`.
///
/// `max_records` is the v1 MaxRecords field; v0 omits it and decode
/// fills `0`. `batch_size` is the v1 BatchSize field; v0 omits it and
/// decode fills `0`. `partition_max_bytes` is the v0 PartitionMaxBytes field;
/// v1 omits it and decode fills `0`. ForgottenTopicsData is JSON `0+`
/// (on the wire for every spoken version). MaxWaitMs is JSON `0+` (INT32
/// after ShareSessionEpoch; official Java `ShareFetchRequest.maxWait`).
/// MinBytes is JSON `0+` (INT32 after MaxWaitMs; official Java
/// `ShareFetchRequest.minBytes`). MaxBytes is JSON `0+` (INT32 after
/// MinBytes; JSON default `0x7fffffff`; official Java
/// `ShareFetchRequest.maxBytes`).
#[expect(
    clippy::type_complexity,
    reason = "ShareFetch request decode returns group, member, epoch, max records, topics, forgotten, batch size, max wait, min bytes, and max bytes together"
)]
pub fn decode_share_fetch_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    String,
    String,
    i32,
    i32,
    Vec<ShareFetchTopic>,
    Vec<ShareForgottenTopic>,
    i32,
    i32,
    i32,
    i32,
)> {
    let flexible = share_fetch_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let epoch = buf::get_i32(buf)?;
    let max_wait_ms = buf::get_i32(buf)?;
    let min_bytes = buf::get_i32(buf)?;
    let max_bytes = buf::get_i32(buf)?;
    let (max_records, batch_size) = if version >= 1 {
        (buf::get_i32(buf)?, buf::get_i32(buf)?)
    } else {
        (0, 0)
    };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let partition_max_bytes = if version == 0 { buf::get_i32(buf)? } else { 0 };
            let acknowledgements = decode_ack_batches(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(ShareFetchPartition {
                partition,
                partition_max_bytes,
                acknowledgements,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(ShareFetchTopic {
            topic_id,
            partitions,
        });
    }
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut forgotten_out = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        forgotten_out.push(ShareForgottenTopic {
            topic_id,
            partitions,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((
        group_id,
        member_id,
        epoch,
        max_records,
        topics,
        forgotten_out,
        batch_size,
        max_wait_ms,
        min_bytes,
        max_bytes,
    ))
}

fn encode_leader(buf: &mut BytesMut, leader_id: i32, leader_epoch: i32) {
    buf.put_i32(leader_id);
    buf.put_i32(leader_epoch);
    buf::put_empty_tagged_fields(buf);
}

fn decode_leader<B: Buf>(buf: &mut B) -> Result<(i32, i32)> {
    let id = buf::get_i32(buf)?;
    let epoch = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((id, epoch))
}

/// Encode a successful ShareFetch response (`version` 0–1).
///
/// v1 adds AcquisitionLockTimeoutMs after ErrorMessage. v0 omits it.
/// Top-level ErrorCode is 0 on this helper
/// ([`encode_share_fetch_response_with_error_code`] writes a non-zero
/// code).
/// [`ShareFetchedPartition::partition_response`] is Java
/// `ShareFetchResponse.partitionResponse` (PartitionIndex and ErrorCode;
/// crate encode writes CurrentLeader from the partition fields, JSON
/// default 0/0, partition ErrorMessage from the partition fields, JSON
/// default null, and v1 AcquisitionLockTimeoutMs 15000). NodeEndpoints stay
/// empty ([`encode_share_fetch_response_with_endpoints`] writes a non-empty
/// list). NodeEndpoints is JSON `0+` (untagged compact array, not Fetch
/// v16 tagged field 0). CurrentLeader is JSON `0+` (untagged nested
/// `LeaderIdAndEpoch`, not Fetch v12+ tagged field 1). Partition
/// ErrorMessage is JSON `0+` (nullable compact STRING, not the top-level
/// ErrorMessage). AcknowledgeErrorCode is JSON `0+` (not fetch
/// `ErrorCode`). AcknowledgeErrorMessage is JSON `0+` (nullable compact
/// STRING, not fetch `ErrorMessage`). ThrottleTimeMs is JSON `0+`
/// ([`encode_share_fetch_response_with_throttle`]; this helper still
/// writes `0`). Top-level ErrorMessage is JSON `0+` (nullable compact
/// STRING; [`encode_share_fetch_response_with_error_message`]; this helper
/// still writes null). AcquisitionLockTimeoutMs is JSON `1+`
/// ([`encode_share_fetch_response_with_acquisition_lock_timeout`]; this
/// helper still writes 15000 on v1). Top-level ErrorCode is JSON `0+`
/// ([`encode_share_fetch_response_with_error_code`]; this helper still
/// writes `0`).
pub fn encode_share_fetch_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ShareFetchedTopic],
) -> crate::error::Result<()> {
    encode_share_fetch_response_with_endpoints(buf, version, topics, &[])
}

/// Encode ShareFetch plus NodeEndpoints.
///
/// NodeEndpoints is JSON `0+` (on the wire for every spoken version).
/// Inner layout matches Produce / Fetch (`NodeId` INT32 + `Host` compact
/// STRING + `Port` INT32 + `Rack` compact nullable STRING + nested tagged
/// fields). [`encode_share_fetch_response`] still writes empty. This is
/// not Fetch v16 tagged field 0. CurrentLeader is JSON `0+` (untagged
/// nested `LeaderIdAndEpoch` from each partition, not Fetch v12+ tagged
/// field 1). Partition ErrorMessage is JSON `0+` (nullable compact STRING
/// from each partition, not the top-level ErrorMessage).
/// AcknowledgeErrorCode is JSON `0+` (from each partition, not fetch
/// `ErrorCode`). AcknowledgeErrorMessage is JSON `0+` (nullable compact
/// STRING from each partition, not fetch `ErrorMessage`). ThrottleTimeMs
/// is JSON `0+` ([`encode_share_fetch_response_with_throttle`]; this
/// helper still writes `0`). Top-level ErrorMessage is JSON `0+`
/// ([`encode_share_fetch_response_with_error_message`]; this helper still
/// writes null).
pub fn encode_share_fetch_response_with_endpoints(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ShareFetchedTopic],
    endpoints: &[NodeEndpoint],
) -> crate::error::Result<()> {
    encode_share_fetch_response_full(buf, version, topics, endpoints, 0, None, 15_000, 0)
}

/// Encode ShareFetch v0–v1 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+`: written on every spoken version.
/// [`encode_share_fetch_response`] still writes `0`. NodeEndpoints stay
/// empty on this helper ([`encode_share_fetch_response_with_endpoints`]
/// writes a non-empty list and still writes ThrottleTimeMs `0`). This is
/// not the top-level ErrorMessage and not v1 AcquisitionLockTimeoutMs.
pub fn encode_share_fetch_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ShareFetchedTopic],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    encode_share_fetch_response_full(buf, version, topics, &[], throttle_time_ms, None, 15_000, 0)
}

/// Encode ShareFetch v0–v1 with top-level ErrorMessage.
///
/// ErrorMessage is JSON `0+` (nullable compact STRING on every spoken
/// version). JSON default is null. [`encode_share_fetch_response`] still
/// writes null. ThrottleTimeMs stays `0` and NodeEndpoints stay empty.
/// This is not partition ErrorMessage, not AcknowledgeErrorMessage, and
/// not ShareAcknowledge top-level ErrorMessage. This helper still writes
/// ErrorCode `0`.
pub fn encode_share_fetch_response_with_error_message(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ShareFetchedTopic],
    error_message: Option<&str>,
) -> crate::error::Result<()> {
    encode_share_fetch_response_full(buf, version, topics, &[], 0, error_message, 15_000, 0)
}

/// Encode ShareFetch v0–v1 with AcquisitionLockTimeoutMs.
///
/// AcquisitionLockTimeoutMs is JSON `1+` (INT32 after ErrorMessage).
/// Kafka 4.1.0 JSON has no default; generated Java int32 default is `0`.
/// Official Java `ShareFetchResponse.of` / `toMessage` / `sizeOf` take
/// `acquisitionLockTimeout` as an argument (not a named constant).
/// [`encode_share_fetch_response`] still writes 15000 on v1. v0 omits
/// the field even when the body is non-zero; decode fills `0`.
/// ThrottleTimeMs stays `0`, ErrorMessage stays null, and NodeEndpoints
/// stay empty. Error-path encode still writes `0` (Java `of` last
/// argument). This is not ThrottleTimeMs and not ShareAcknowledge.
pub fn encode_share_fetch_response_with_acquisition_lock_timeout(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ShareFetchedTopic],
    acquisition_lock_timeout_ms: i32,
) -> crate::error::Result<()> {
    encode_share_fetch_response_full(
        buf,
        version,
        topics,
        &[],
        0,
        None,
        acquisition_lock_timeout_ms,
        0,
    )
}

/// Encode ShareFetch v0–v1 with top-level ErrorCode.
///
/// ErrorCode is JSON `0+` (INT16 after ThrottleTimeMs). JSON default is
/// `0`. [`encode_share_fetch_response`] still writes `0`. Decode returns
/// it and does not fail on a non-zero code. Official Java
/// `ShareFetchResponse.of` / `toMessage` / `error` /
/// `ShareFetchRequest.getErrorResponse` set it. ThrottleTimeMs stays `0`,
/// ErrorMessage stays null, NodeEndpoints stay empty, and v1
/// AcquisitionLockTimeoutMs stays 15000 on this helper.
/// [`encode_share_fetch_error`] still writes empty Responses and v1
/// AcquisitionLockTimeoutMs `0`. This is not partition `ErrorCode` and
/// not ShareAcknowledge ErrorCode.
pub fn encode_share_fetch_response_with_error_code(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ShareFetchedTopic],
    error_code: i16,
) -> crate::error::Result<()> {
    encode_share_fetch_response_full(buf, version, topics, &[], 0, None, 15_000, error_code)
}

#[expect(
    clippy::too_many_arguments,
    reason = "ShareFetch response body fields are a single wire encode"
)]
fn encode_share_fetch_response_full(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ShareFetchedTopic],
    endpoints: &[NodeEndpoint],
    throttle_time_ms: i32,
    error_message: Option<&str>,
    acquisition_lock_timeout_ms: i32,
    error_code: i16,
) -> crate::error::Result<()> {
    let flexible = share_fetch_flexible(version)?;
    buf.put_i32(throttle_time_ms);
    buf.put_i16(error_code);
    buf::put_string(buf, flexible, error_message)?;
    if version >= 1 {
        buf.put_i32(acquisition_lock_timeout_ms);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf::put_string(buf, flexible, p.error_message.as_deref())?;
            buf.put_i16(p.acknowledge_error_code);
            buf::put_string(buf, flexible, p.acknowledge_error_message.as_deref())?;
            encode_leader(buf, p.current_leader_id, p.current_leader_epoch);
            let mut recs = BytesMut::new();
            for batch in &p.records {
                records::encode_record_batch(&mut recs, batch)?;
            }
            buf::put_bytes(buf, flexible, Some(&recs))?;
            buf::put_array_len(buf, flexible, Some(p.acquired.len()))?;
            for a in &p.acquired {
                buf.put_i64(a.first_offset);
                buf.put_i64(a.last_offset);
                buf.put_i16(a.delivery_count);
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    super::api::put_compact_node_endpoints(buf, endpoints)?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ShareFetch response (`version` 0–1):
/// `(topics, node_endpoints, throttle_time_ms, error_message,
/// acquisition_lock_timeout_ms, error_code)`.
///
/// NodeEndpoints is JSON `0+` (untagged compact array). ThrottleTimeMs is
/// JSON `0+` (always on the wire). Top-level ErrorMessage is JSON `0+`
/// (nullable compact STRING). AcquisitionLockTimeoutMs is JSON `1+`
/// (INT32 after ErrorMessage). v0 omits it; decode fills `0`. ErrorCode
/// is JSON `0+` (INT16 after ThrottleTimeMs). Decode does not fail on a
/// non-zero top-level ErrorCode; callers decide.
#[expect(
    clippy::type_complexity,
    reason = "ShareFetch response decode returns topics, node endpoints, throttle, ErrorMessage, AcquisitionLockTimeoutMs, and ErrorCode together"
)]
pub fn decode_share_fetch_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    Vec<ShareFetchedTopic>,
    Vec<NodeEndpoint>,
    i32,
    Option<String>,
    i32,
    i16,
)> {
    let flexible = share_fetch_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_string(buf, flexible)?;
    let acquisition_lock_timeout_ms = if version >= 1 { buf::get_i32(buf)? } else { 0 };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let error_message = buf::get_string(buf, flexible)?;
            let acknowledge_error_code = buf::get_i16(buf)?;
            let acknowledge_error_message = buf::get_string(buf, flexible)?;
            let (current_leader_id, current_leader_epoch) = decode_leader(buf)?;
            let rec_bytes = if flexible {
                buf::take_compact_bytes(buf)?.unwrap_or_else(Bytes::new)
            } else {
                buf::take_classic_bytes(buf)?.unwrap_or_else(Bytes::new)
            };
            let records = if rec_bytes.is_empty() {
                Vec::new()
            } else {
                let mut rec_buf = rec_bytes;
                records::decode_record_batches(&mut rec_buf)?
            };
            let an = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut acquired = Vec::with_capacity(an);
            for _ in 0..an {
                let first_offset = buf::get_i64(buf)?;
                let last_offset = buf::get_i64(buf)?;
                let delivery_count = buf::get_i16(buf)?;
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                acquired.push(AcquiredRange {
                    first_offset,
                    last_offset,
                    delivery_count,
                });
            }
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(ShareFetchedPartition {
                partition,
                error_code,
                error_message,
                acknowledge_error_code,
                acknowledge_error_message,
                current_leader_id,
                current_leader_epoch,
                records,
                acquired,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(ShareFetchedTopic {
            topic_id,
            partitions,
        });
    }
    let endpoints = super::api::get_compact_node_endpoints(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((
        topics,
        endpoints,
        throttle_time_ms,
        error_message,
        acquisition_lock_timeout_ms,
        error_code,
    ))
}

fn share_acknowledge_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(true),
        other => Err(Error::protocol(format!(
            "ShareAcknowledge version {other} is not implemented"
        ))),
    }
}

/// Encode ShareAcknowledge for one topic (`version` 0–1).
pub fn encode_share_acknowledge_request(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    topic_id: [u8; 16],
    partitions: &[(i32, Vec<AcknowledgementBatch>)],
) -> crate::error::Result<()> {
    if partitions.is_empty() {
        encode_share_acknowledge_topics(buf, version, group_id, member_id, share_session_epoch, &[])
    } else {
        encode_share_acknowledge_topics(
            buf,
            version,
            group_id,
            member_id,
            share_session_epoch,
            &[ShareAckTopic {
                topic_id,
                partitions: partitions.to_vec(),
            }],
        )
    }
}

/// One topic in a multi-topic ShareAcknowledge request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAckTopic {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// `(partition, acknowledgement batches)`.
    pub partitions: Vec<(i32, Vec<AcknowledgementBatch>)>,
}

/// Java `ShareAcknowledgeRequest` helpers.
pub struct ShareAcknowledgeRequest;

impl ShareAcknowledgeRequest {
    /// Java `ShareAcknowledgeRequest.Builder.forConsumer` Topics.
    ///
    /// Groups `(topic id, partition, batches)` by topic id (Java
    /// `HashMap`; first-seen id order — `HashMap.forEach` order is
    /// unspecified). A later partition for the same id appends in
    /// first-seen partition order. Duplicate `(id, partition)`
    /// **replaces** the batches (Java `setAcknowledgementBatches`, last
    /// wins). Empty is empty. Topic name is not used (Java
    /// `TopicIdPartition.topicId` / `partition` only). GroupId,
    /// MemberId, and ShareSessionEpoch stay with the encode caller.
    /// Encode still writes the caller's Topics as-is. Distinct from
    /// [`ShareAcknowledgeResponse::to_message`], which groups response
    /// bodies and overwrites the partition index from the key.
    #[must_use]
    pub fn for_consumer<I>(acknowledgements: I) -> Vec<ShareAckTopic>
    where
        I: IntoIterator<Item = ([u8; 16], i32, Vec<AcknowledgementBatch>)>,
    {
        let mut topic_order = Vec::new();
        let mut by_id = HashMap::new();
        for (topic_id, partition, batches) in acknowledgements {
            let (part_order, part_map) = by_id.entry(topic_id).or_insert_with(|| {
                topic_order.push(topic_id);
                (Vec::new(), HashMap::new())
            });
            if part_map.insert(partition, batches).is_none() {
                part_order.push(partition);
            }
        }
        let mut topics = Vec::with_capacity(topic_order.len());
        for topic_id in topic_order {
            let Some((part_order, mut part_map)) = by_id.remove(&topic_id) else {
                continue;
            };
            let mut partitions = Vec::with_capacity(part_order.len());
            for partition in part_order {
                if let Some(batches) = part_map.remove(&partition) {
                    partitions.push((partition, batches));
                }
            }
            topics.push(ShareAckTopic {
                topic_id,
                partitions,
            });
        }
        topics
    }
}

/// ShareAcknowledge with several topics in one request (`version` 0–1).
///
/// Kafka 4.0 JSON (`apiKey: 79`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, `latestVersionUnstable: true`) and Kafka
/// 4.1 JSON (`validVersions: "1"` — v0 removed). Request and response
/// fields are identical. This crate speaks 0–1. v2+ is not spoken.
pub fn encode_share_acknowledge_topics(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    topics: &[ShareAckTopic],
) -> crate::error::Result<()> {
    let flexible = share_acknowledge_flexible(version)?;
    buf::put_string(buf, flexible, Some(group_id))?;
    buf::put_string(buf, flexible, Some(member_id))?;
    buf.put_i32(share_session_epoch);
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for (partition, batches) in &t.partitions {
            buf.put_i32(*partition);
            encode_ack_batches(buf, batches)?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Top-level ShareFetch error (session not found / bad epoch).
///
/// ThrottleTimeMs is the JSON default (`0`). Official Java
/// `ShareFetchRequest.getErrorResponse` sets `throttleTimeMs` from the
/// argument via `ShareFetchResponse.of`. AcquisitionLockTimeoutMs stays
/// `0` on this path (Java `of` last argument), not the success-path
/// 15000.
pub fn encode_share_fetch_error(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
) -> crate::error::Result<()> {
    encode_share_fetch_error_with_throttle(buf, version, error_code, 0)
}

/// Encode a top-level ShareFetch error with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+` (first field). Official Java
/// `ShareFetchRequest.getErrorResponse` / `ShareFetchResponse.of` sets it.
/// Convenience encode still writes `0`. Empty Responses and NodeEndpoints.
/// ErrorMessage stays null. v1 AcquisitionLockTimeoutMs stays `0` (Java
/// `of` last argument), not 15000. Decode returns the top-level ErrorCode
/// and does not fail on a non-zero code. This is not
/// [`encode_share_fetch_response_with_throttle`] and not
/// [`encode_share_fetch_response_with_error_code`].
pub fn encode_share_fetch_error_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = share_fetch_flexible(version)?;
    buf.put_i32(throttle_time_ms);
    buf.put_i16(error_code);
    buf::put_string(buf, flexible, None)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, flexible, Some(0))?;
    buf::put_array_len(buf, flexible, Some(0))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

#[expect(
    clippy::type_complexity,
    reason = "ack request is group, member, epoch, and topic-partition batches"
)]
/// Decode a ShareAcknowledge request (`version` 0–1).
///
/// Returns `(group_id, member_id, epoch, topic-partition batches)`.
pub fn decode_share_acknowledge_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    String,
    String,
    i32,
    Vec<([u8; 16], i32, Vec<AcknowledgementBatch>)>,
)> {
    let flexible = share_acknowledge_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let epoch = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::new();
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let batches = decode_ack_batches(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            topics.push((topic_id, partition, batches));
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((group_id, member_id, epoch, topics))
}

/// Encode a ShareAcknowledge response (`version` 0–1): throttle `0` plus
/// top-level error code and empty Responses.
///
/// Official Java `ShareAcknowledgeRequest.getErrorResponse` writes
/// ThrottleTimeMs plus the top-level ErrorCode (empty Responses). This
/// helper still writes ThrottleTimeMs `0`. For partition bodies, use
/// [`encode_share_acknowledge_topics_response`] with
/// [`ShareAcknowledgeResponsePartition::partition_response`].
pub fn encode_share_acknowledge_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
) -> crate::error::Result<()> {
    encode_share_acknowledge_topics_response(buf, version, error_code, &[])
}

/// Encode a ShareAcknowledge response (`version` 0–1) with topic/partition
/// bodies.
///
/// ThrottleTimeMs is JSON `0+` ([`encode_share_acknowledge_topics_response_with_throttle`];
/// this helper still writes `0`). Top-level ErrorMessage is JSON `0+`
/// ([`encode_share_acknowledge_topics_response_with_error_message`]; this
/// helper still writes null).
/// Each partition is PartitionIndex and ErrorCode; ErrorMessage is taken
/// from the partition fields (JSON default null) and CurrentLeader is
/// taken from the partition fields (JSON default id 0 epoch 0).
/// NodeEndpoints stay empty
/// ([`encode_share_acknowledge_topics_response_with_endpoints`]
/// writes a non-empty list). NodeEndpoints is JSON `0+` (untagged compact
/// array, not Fetch v16 tagged field 0). CurrentLeader is JSON `0+`
/// (untagged nested `LeaderIdAndEpoch`, not Fetch v12+ tagged field 1).
/// Partition ErrorMessage is JSON `0+` (nullable compact STRING, not the
/// top-level ErrorMessage).
/// [`ShareAcknowledgeResponsePartition::partition_response`] is
/// Java `ShareAcknowledgeResponse.partitionResponse`.
pub fn encode_share_acknowledge_topics_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    topics: &[ShareAcknowledgeResponseTopic],
) -> crate::error::Result<()> {
    encode_share_acknowledge_topics_response_with_endpoints(buf, version, error_code, topics, &[])
}

/// Encode ShareAcknowledge plus NodeEndpoints.
///
/// NodeEndpoints is JSON `0+` (on the wire for every spoken version).
/// Inner layout matches Produce / Fetch / ShareFetch (`NodeId` INT32 +
/// `Host` compact STRING + `Port` INT32 + `Rack` compact nullable STRING +
/// nested tagged fields). [`encode_share_acknowledge_topics_response`]
/// still writes empty. v0 and v1 bodies match. This is not Fetch v16
/// tagged field 0. CurrentLeader is JSON `0+` (untagged nested
/// `LeaderIdAndEpoch` from each partition, not Fetch v12+ tagged field 1).
/// Partition ErrorMessage is JSON `0+` (nullable compact STRING from each
/// partition, not the top-level ErrorMessage). ThrottleTimeMs is JSON `0+`
/// ([`encode_share_acknowledge_topics_response_with_throttle`]; this
/// helper still writes `0`). Top-level ErrorMessage is JSON `0+`
/// ([`encode_share_acknowledge_topics_response_with_error_message`]; this
/// helper still writes null).
pub fn encode_share_acknowledge_topics_response_with_endpoints(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    topics: &[ShareAcknowledgeResponseTopic],
    endpoints: &[NodeEndpoint],
) -> crate::error::Result<()> {
    encode_share_acknowledge_topics_response_full(
        buf, version, error_code, topics, endpoints, 0, None,
    )
}

/// Encode ShareAcknowledge v0–v1 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+`: written on every spoken version.
/// [`encode_share_acknowledge_topics_response`] still writes `0`.
/// NodeEndpoints stay empty on this helper
/// ([`encode_share_acknowledge_topics_response_with_endpoints`] writes a
/// non-empty list and still writes ThrottleTimeMs `0`). v0 and v1 bodies
/// match. This is not the top-level ErrorMessage and not ShareFetch
/// ThrottleTimeMs (ShareFetch v1 still adds AcquisitionLockTimeoutMs).
pub fn encode_share_acknowledge_topics_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    topics: &[ShareAcknowledgeResponseTopic],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    encode_share_acknowledge_topics_response_full(
        buf,
        version,
        error_code,
        topics,
        &[],
        throttle_time_ms,
        None,
    )
}

/// Encode ShareAcknowledge v0–v1 with top-level ErrorMessage.
///
/// ErrorMessage is JSON `0+` (nullable compact STRING on every spoken
/// version). JSON default is null. [`encode_share_acknowledge_topics_response`]
/// still writes null. ThrottleTimeMs stays `0` and NodeEndpoints stay
/// empty. v0 and v1 bodies match. This is not partition ErrorMessage and
/// not ShareFetch top-level ErrorMessage (ShareFetch v1 still adds
/// AcquisitionLockTimeoutMs). Official Java `of` / `toMessage` /
/// `getErrorResponse` leave it at JSON null.
pub fn encode_share_acknowledge_topics_response_with_error_message(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    topics: &[ShareAcknowledgeResponseTopic],
    error_message: Option<&str>,
) -> crate::error::Result<()> {
    encode_share_acknowledge_topics_response_full(
        buf,
        version,
        error_code,
        topics,
        &[],
        0,
        error_message,
    )
}

fn encode_share_acknowledge_topics_response_full(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    topics: &[ShareAcknowledgeResponseTopic],
    endpoints: &[NodeEndpoint],
    throttle_time_ms: i32,
    error_message: Option<&str>,
) -> crate::error::Result<()> {
    let flexible = share_acknowledge_flexible(version)?;
    buf.put_i32(throttle_time_ms);
    buf.put_i16(error_code);
    buf::put_string(buf, flexible, error_message)?;
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf::put_string(buf, flexible, p.error_message.as_deref())?;
            encode_leader(buf, p.current_leader_id, p.current_leader_epoch);
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    super::api::put_compact_node_endpoints(buf, endpoints)?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ShareAcknowledge response (`version` 0–1): error code.
///
/// Does not fail on a non-zero top-level ErrorCode. Topic/partition
/// bodies are decoded and discarded. For those, use
/// [`decode_share_acknowledge_topics_response`].
pub fn decode_share_acknowledge_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let (error_code, ..) = decode_share_acknowledge_topics_response(buf, version)?;
    Ok(error_code)
}

/// Decode a ShareAcknowledge response (`version` 0–1):
/// `(error_code, topics, node_endpoints, throttle_time_ms, error_message)`.
///
/// Does not fail on a non-zero top-level or partition ErrorCode; callers
/// decide. ThrottleTimeMs is JSON `0+` (always on the wire). Top-level
/// ErrorMessage is JSON `0+` (nullable compact STRING). Partition
/// ErrorMessage is JSON `0+` (nullable compact STRING). CurrentLeader is
/// JSON `0+` (untagged nested `LeaderIdAndEpoch`). NodeEndpoints is JSON
/// `0+` (untagged compact array).
#[expect(
    clippy::type_complexity,
    reason = "ShareAcknowledge response decode returns error, topics, node endpoints, throttle, and top-level ErrorMessage together"
)]
pub fn decode_share_acknowledge_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    i16,
    Vec<ShareAcknowledgeResponseTopic>,
    Vec<NodeEndpoint>,
    i32,
    Option<String>,
)> {
    let flexible = share_acknowledge_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_string(buf, flexible)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let part_error = buf::get_i16(buf)?;
            let error_message = buf::get_string(buf, flexible)?;
            let (current_leader_id, current_leader_epoch) = decode_leader(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(ShareAcknowledgeResponsePartition {
                partition,
                error_code: part_error,
                error_message,
                current_leader_id,
                current_leader_epoch,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(ShareAcknowledgeResponseTopic {
            topic_id,
            partitions,
        });
    }
    let endpoints = super::api::get_compact_node_endpoints(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((
        error_code,
        topics,
        endpoints,
        throttle_time_ms,
        error_message,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::records::Record;
    use bytes::{Buf, Bytes};
    use std::collections::HashMap;

    #[test]
    fn share_group_heartbeat_join_leave_roundtrip() {
        let req = ShareGroupHeartbeatRequest {
            group_id: "sg".into(),
            member_id: "m1".into(),
            member_epoch: ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            subscribed_topic_names: Some(vec!["t".into()]),
        };
        let mut buf = BytesMut::new();
        encode_share_group_heartbeat_request(&mut buf, 1, &req).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_share_group_heartbeat_request(&mut cur, 1).unwrap();
        assert!(!cur.has_remaining(), "v1 request leftover-empty");
        assert_eq!(
            decoded.member_epoch,
            ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        );
        assert_eq!(decoded.member_id, "m1");
        assert_eq!(decoded.subscribed_topic_names, Some(vec!["t".into()]));

        let resp = ShareGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: Some(vec![ShareTopicPartitions {
                topic_id: [1u8; 16],
                partitions: vec![0],
            }]),
        };
        buf.clear();
        encode_share_group_heartbeat_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_share_group_heartbeat_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(!cur.has_remaining(), "v1 response leftover-empty");

        let leave = ShareGroupHeartbeatRequest {
            group_id: "sg".into(),
            member_id: "m1".into(),
            member_epoch: ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH,
            subscribed_topic_names: None,
        };
        buf.clear();
        encode_share_group_heartbeat_request(&mut buf, 1, &leave).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_share_group_heartbeat_request(&mut cur, 1)
                .unwrap()
                .member_epoch,
            ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH
        );
        assert!(!cur.has_remaining(), "v1 leave leftover-empty");
    }

    #[test]
    fn share_group_heartbeat_v0_matches_v1_and_does_not_speak_v2() {
        // Official Kafka 4.0 JSON: validVersions "0", flexibleVersions "0+",
        // latestVersionUnstable. Official Kafka 4.1 JSON: validVersions "1"
        // (v0 removed). Same request/response fields. This crate speaks 0–1.
        let req = ShareGroupHeartbeatRequest {
            group_id: "sg".into(),
            member_id: "m1".into(),
            member_epoch: ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            subscribed_topic_names: Some(vec!["t".into()]),
        };
        let mut v0 = BytesMut::new();
        encode_share_group_heartbeat_request(&mut v0, 0, &req).unwrap();
        let mut v1 = BytesMut::new();
        encode_share_group_heartbeat_request(&mut v1, 1, &req).unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 request bodies match");
        let mut cur = v0.as_ref();
        assert_eq!(
            decode_share_group_heartbeat_request(&mut cur, 0).unwrap(),
            req
        );
        assert!(!cur.has_remaining(), "v0 request leftover-empty");
        let err = encode_share_group_heartbeat_request(&mut BytesMut::new(), 2, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 is not spoken, got {err}"
        );
        let mut empty: &[u8] = &[];
        let err = decode_share_group_heartbeat_request(&mut empty, 2).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 decode is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);
        assert_eq!(ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH, -1);
        assert_eq!(ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH, 0);

        let resp = ShareGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: None,
        };
        v0.clear();
        encode_share_group_heartbeat_response(&mut v0, 0, &resp).unwrap();
        v1.clear();
        encode_share_group_heartbeat_response(&mut v1, 1, &resp).unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 response bodies match");
        let mut cur = v0.as_ref();
        assert_eq!(
            decode_share_group_heartbeat_response(&mut cur, 0).unwrap(),
            resp
        );
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
        v0.clear();
        let err = encode_share_group_heartbeat_response(&mut v0, 2, &resp).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 response is not spoken, got {err}"
        );
    }

    #[test]
    fn share_group_heartbeat_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareGroupHeartbeatResponse.json ThrottleTimeMs
        // is versions 0+ (INT32 on every spoken version). Official Java
        // ShareGroupHeartbeatRequest.getErrorResponse /
        // ShareGroupHeartbeatResponse.throttleTimeMs set / read it.
        // Encode writes ShareGroupHeartbeatResponse.throttle_time_ms
        // (JSON default 0). v0 and v1 bodies match. This is not
        // ShareFetch / ShareAcknowledge ThrottleTimeMs.
        let zero = ShareGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: None,
        };
        let with = ShareGroupHeartbeatResponse {
            throttle_time_ms: 3_600_000,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: None,
        };
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_group_heartbeat_response(&mut buf, version, &with).unwrap();
            let mut cur = buf.as_ref();
            let got = decode_share_group_heartbeat_response(&mut cur, version).unwrap();
            assert_eq!(got, with);
            assert_eq!(got.throttle_time_ms, 3_600_000);
            assert!(
                cur.is_empty(),
                "ShareGroupHeartbeat v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with_buf = BytesMut::new();
        encode_share_group_heartbeat_response(&mut with_buf, 0, &with).unwrap();
        let mut zero_buf = BytesMut::new();
        encode_share_group_heartbeat_response(&mut zero_buf, 0, &zero).unwrap();
        assert_ne!(
            &with_buf[..],
            &zero_buf[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );

        let mut v1_with = BytesMut::new();
        encode_share_group_heartbeat_response(&mut v1_with, 1, &with).unwrap();
        assert_eq!(
            &with_buf[..],
            &v1_with[..],
            "v0 and v1 both write ThrottleTimeMs (JSON 0+); ShareGroupHeartbeat has no AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_fetch_and_acknowledge_roundtrip() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"s")),
            headers: vec![],
        };
        let topics = vec![ShareFetchedTopic {
            topic_id: [0u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 0,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: vec![RecordBatch::from_records(vec![rec])],
                acquired: vec![AcquiredRange {
                    first_offset: 0,
                    last_offset: 0,
                    delivery_count: 1,
                }],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_share_fetch_response(&mut buf, 1, &topics).unwrap();
        let (decoded, ..) = decode_share_fetch_response(&mut &buf[..], 1).unwrap();
        assert_eq!(decoded[0].partitions[0].acquired[0].first_offset, 0);
        assert_eq!(
            decoded[0].partitions[0].records[0].records[0]
                .value
                .as_deref(),
            Some(&b"s"[..])
        );

        let req_topics = vec![ShareFetchTopic {
            topic_id: [0u8; 16],
            partitions: vec![ShareFetchPartition {
                partition: 0,
                partition_max_bytes: 1024,
                acknowledgements: vec![],
            }],
        }];
        buf.clear();
        encode_share_fetch_request(&mut buf, 1, "sg", "m1", 0, 10, 1, 1024, 16, &req_topics)
            .unwrap();
        let mut cur = &buf[..];
        let (gid, mid, epoch, max_records, got, ..) =
            decode_share_fetch_request(&mut cur, 1).unwrap();
        assert_eq!(
            (gid.as_str(), mid.as_str(), epoch, max_records),
            ("sg", "m1", 0, 16)
        );
        assert_eq!(got[0].partitions[0].partition, 0);
        assert_eq!(
            got[0].partitions[0].partition_max_bytes, 0,
            "v1 omits PartitionMaxBytes; decode fills 0"
        );
        assert!(!cur.has_remaining(), "v1 request leftover-empty");

        buf.clear();
        encode_share_acknowledge_request(
            &mut buf,
            1,
            "sg",
            "m1",
            1,
            [0u8; 16],
            &[(
                0,
                vec![AcknowledgementBatch {
                    first_offset: 0,
                    last_offset: 2,
                    types: vec![ACK_ACCEPT],
                }],
            )],
        )
        .unwrap();
        let (gid, mid, _e, acks) = decode_share_acknowledge_request(&mut &buf[..], 1).unwrap();
        assert_eq!(gid, "sg");
        assert_eq!(mid, "m1");
        assert_eq!(acks[0].2[0].types, vec![ACK_ACCEPT]);
        assert_eq!(acks[0].2[0].last_offset, 2);
        buf.clear();
        encode_share_acknowledge_response(&mut buf, 1, 0).unwrap();
        assert_eq!(
            decode_share_acknowledge_response(&mut &buf[..], 1).unwrap(),
            0
        );
    }

    #[test]
    fn share_acknowledge_encodes_several_partitions() {
        let mut buf = BytesMut::new();
        encode_share_acknowledge_request(
            &mut buf,
            1,
            "sg",
            "m1",
            2,
            [7u8; 16],
            &[
                (
                    0,
                    vec![AcknowledgementBatch {
                        first_offset: 1,
                        last_offset: 3,
                        types: vec![ACK_ACCEPT],
                    }],
                ),
                (
                    1,
                    vec![AcknowledgementBatch {
                        first_offset: 8,
                        last_offset: 8,
                        types: vec![ACK_REJECT],
                    }],
                ),
            ],
        )
        .unwrap();
        let (_gid, _mid, epoch, acks) = decode_share_acknowledge_request(&mut &buf[..], 1).unwrap();
        assert_eq!(epoch, 2);
        assert_eq!(acks.len(), 2);
        assert_eq!(acks[0].1, 0);
        assert_eq!(acks[1].1, 1);
        assert_eq!(acks[1].2[0].types, vec![ACK_REJECT]);
    }

    #[test]
    fn share_acknowledge_close_session_has_no_topics() {
        let mut buf = BytesMut::new();
        encode_share_acknowledge_request(&mut buf, 1, "sg", "m1", -1, [0u8; 16], &[]).unwrap();
        let (_gid, _mid, epoch, acks) = decode_share_acknowledge_request(&mut &buf[..], 1).unwrap();
        assert_eq!(epoch, -1);
        assert!(acks.is_empty());
    }

    #[test]
    fn share_acknowledge_v0_matches_v1_and_does_not_speak_v2() {
        // Official Kafka 4.0 JSON: validVersions "0", flexibleVersions "0+",
        // latestVersionUnstable. Official Kafka 4.1 JSON: validVersions "1"
        // (v0 removed). Same request/response fields. This crate speaks 0–1.
        let partitions = [(
            0,
            vec![AcknowledgementBatch {
                first_offset: 0,
                last_offset: 1,
                types: vec![ACK_ACCEPT],
            }],
        )];
        let mut v0 = BytesMut::new();
        encode_share_acknowledge_request(&mut v0, 0, "sg", "m1", 1, [0u8; 16], &partitions)
            .unwrap();
        let mut v1 = BytesMut::new();
        encode_share_acknowledge_request(&mut v1, 1, "sg", "m1", 1, [0u8; 16], &partitions)
            .unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 request bodies match");
        let mut cur = v0.as_ref();
        let (gid, mid, epoch, acks) = decode_share_acknowledge_request(&mut cur, 0).unwrap();
        assert_eq!((gid.as_str(), mid.as_str(), epoch), ("sg", "m1", 1));
        assert_eq!(acks[0].2[0].types, vec![ACK_ACCEPT]);
        assert!(!cur.has_remaining(), "v0 request leftover-empty");
        let err = encode_share_acknowledge_request(
            &mut BytesMut::new(),
            2,
            "sg",
            "m1",
            1,
            [0u8; 16],
            &partitions,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 is not spoken, got {err}"
        );
        let mut empty: &[u8] = &[];
        let err = decode_share_acknowledge_request(&mut empty, 2).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 decode is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);

        v0.clear();
        encode_share_acknowledge_response(&mut v0, 0, 0).unwrap();
        v1.clear();
        encode_share_acknowledge_response(&mut v1, 1, 0).unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 response bodies match");
        let mut cur = v0.as_ref();
        assert_eq!(decode_share_acknowledge_response(&mut cur, 0).unwrap(), 0);
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
        v0.clear();
        let err = encode_share_acknowledge_response(&mut v0, 2, 0).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 response is not spoken, got {err}"
        );
    }

    #[test]
    fn share_fetch_error_roundtrip() {
        let mut buf = BytesMut::new();
        encode_share_fetch_error(&mut buf, 1, crate::error::INVALID_SHARE_SESSION_EPOCH).unwrap();
        let mut cur = &buf[..];
        let _th = crate::protocol::buf::get_i32(&mut cur).unwrap();
        let err = crate::protocol::buf::get_i16(&mut cur).unwrap();
        assert_eq!(err, crate::error::INVALID_SHARE_SESSION_EPOCH);
    }

    #[test]
    fn share_fetch_error_throttle_time_ms_matches_java() {
        // Kafka 4.1 ShareFetchRequest.getErrorResponse calls
        // ShareFetchResponse.of(error, throttleTimeMs, empty map, empty
        // endpoints, 0). ThrottleTimeMs is JSON 0+ INT32 first field.
        // encode_share_fetch_error still writes 0. v1
        // AcquisitionLockTimeoutMs stays 0 on this path (Java of last
        // argument), not the success-path 15000. Decode returns the
        // top-level ErrorCode and does not fail on a non-zero code.
        // Empty-Responses v0 != v1 (AcquisitionLockTimeoutMs). Top-level
        // ErrorCode is at bytes 4–5. This crate speaks 0–1. This is not
        // ShareFetch success-path / ShareAcknowledge / Fetch ThrottleTimeMs.
        let err = crate::error::INVALID_SHARE_SESSION_EPOCH;
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_error_with_throttle(&mut buf, version, err, 3_600_000).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(crate::protocol::buf::get_i32(&mut cur).unwrap(), 3_600_000);
            assert_eq!(crate::protocol::buf::get_i16(&mut cur).unwrap(), err);
            assert_eq!(
                crate::protocol::buf::get_string(&mut cur, true).unwrap(),
                None
            );
            if version >= 1 {
                assert_eq!(
                    crate::protocol::buf::get_i32(&mut cur).unwrap(),
                    0,
                    "ShareFetch error v1 AcquisitionLockTimeoutMs stays 0"
                );
            }
            assert_eq!(
                crate::protocol::buf::get_array_len(&mut cur, true)
                    .unwrap()
                    .unwrap_or(0),
                0
            );
            assert_eq!(
                crate::protocol::buf::get_array_len(&mut cur, true)
                    .unwrap()
                    .unwrap_or(0),
                0
            );
            crate::protocol::buf::skip_tagged_fields(&mut cur).unwrap();
            assert!(
                cur.is_empty(),
                "ShareFetch error v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_error_with_throttle(&mut with, 0, err, 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_share_fetch_error_with_throttle(&mut zero, 0, err, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 error ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_share_fetch_error(&mut conv, 0, err).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_share_fetch_error still writes ThrottleTimeMs 0"
        );
        assert_eq!(
            &with[..4],
            &3_600_000i32.to_be_bytes(),
            "ShareFetch error ThrottleTimeMs is the first field"
        );
        assert_eq!(
            &with[4..6],
            &err.to_be_bytes(),
            "ShareFetch error ErrorCode is at bytes 4–5"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_error_with_throttle(&mut v1_with, 1, err, 3_600_000).unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v1 error adds AcquisitionLockTimeoutMs 0 after ErrorMessage"
        );
    }

    #[test]
    fn share_fetch_v0_omits_v1_fields_and_does_not_speak_v2() {
        // Official Kafka 4.0 JSON: validVersions "0", PartitionMaxBytes on
        // each partition, no MaxRecords / BatchSize / AcquisitionLockTimeoutMs.
        // Official Kafka 4.1 JSON: validVersions "1" (v0 removed); MaxRecords
        // and BatchSize after MaxBytes; no PartitionMaxBytes;
        // AcquisitionLockTimeoutMs after ErrorMessage. This crate speaks 0–1.
        let req_topics = vec![ShareFetchTopic {
            topic_id: [0u8; 16],
            partitions: vec![ShareFetchPartition {
                partition: 0,
                partition_max_bytes: 1024,
                acknowledgements: vec![],
            }],
        }];
        let mut v0 = BytesMut::new();
        encode_share_fetch_request(&mut v0, 0, "sg", "m1", 0, 10, 1, 1024, 16, &req_topics)
            .unwrap();
        let mut v1 = BytesMut::new();
        encode_share_fetch_request(&mut v1, 1, "sg", "m1", 0, 10, 1, 1024, 16, &req_topics)
            .unwrap();
        assert_ne!(
            v0.as_ref(),
            v1.as_ref(),
            "v0 PartitionMaxBytes and v1 MaxRecords/BatchSize differ on the wire"
        );
        let mut cur = v0.as_ref();
        let (gid, mid, epoch, max_records, got, ..) =
            decode_share_fetch_request(&mut cur, 0).unwrap();
        assert_eq!(
            (gid.as_str(), mid.as_str(), epoch, max_records),
            ("sg", "m1", 0, 0),
            "v0 omits MaxRecords; decode fills 0"
        );
        assert_eq!(got[0].partitions[0].partition, 0);
        assert_eq!(
            got[0].partitions[0].partition_max_bytes, 1024,
            "v0 stores PartitionMaxBytes"
        );
        assert!(!cur.has_remaining(), "v0 request leftover-empty");
        let err = encode_share_fetch_request(
            &mut BytesMut::new(),
            2,
            "sg",
            "m1",
            0,
            10,
            1,
            1024,
            16,
            &req_topics,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 is not spoken, got {err}"
        );
        let mut empty: &[u8] = &[];
        let err = decode_share_fetch_request(&mut empty, 2).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 decode is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);

        let resp = vec![ShareFetchedTopic {
            topic_id: [0u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 0,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        v0.clear();
        encode_share_fetch_response(&mut v0, 0, &resp).unwrap();
        v1.clear();
        encode_share_fetch_response(&mut v1, 1, &resp).unwrap();
        assert_ne!(
            v0.as_ref(),
            v1.as_ref(),
            "v1 AcquisitionLockTimeoutMs is absent on v0"
        );
        let mut cur = v0.as_ref();
        let (decoded, endpoints, ..) = decode_share_fetch_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, resp);
        assert!(endpoints.is_empty());
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
        v0.clear();
        let err = encode_share_fetch_response(&mut v0, 2, &resp).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 response is not spoken, got {err}"
        );
    }

    #[test]
    fn share_fetch_response_node_endpoints_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchResponse.json NodeEndpoints is
        // versions 0+ (untagged compact array on every spoken version).
        // Inner layout matches Produce / Fetch NodeEndpoint. This is not
        // Fetch v16 tagged field 0. encode_share_fetch_response still
        // writes empty.
        let topics = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let endpoints = [crate::protocol::api::NodeEndpoint {
            node_id: 3,
            host: "h".into(),
            port: 1,
            rack: Some("r".into()),
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response_with_endpoints(&mut buf, version, &topics, &endpoints)
                .unwrap();
            let mut cur = buf.as_ref();
            let (got, eps, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, topics);
            assert_eq!(eps, endpoints);
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} NodeEndpoints leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_response_with_endpoints(&mut with, 0, &topics, &endpoints).unwrap();
        let mut empty = BytesMut::new();
        encode_share_fetch_response_with_endpoints(&mut empty, 0, &topics, &[]).unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareFetch NodeEndpoints is not always empty"
        );
        let mut conv = BytesMut::new();
        encode_share_fetch_response(&mut conv, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "encode_share_fetch_response still writes empty NodeEndpoints"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_response_with_endpoints(&mut v1_with, 1, &topics, &endpoints).unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 NodeEndpoints share compact layout; v1 still adds AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_fetch_response_current_leader_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchResponse.json partition CurrentLeader is
        // versions 0+ (untagged nested LeaderIdAndEpoch on every spoken
        // version). LeaderId INT32 then LeaderEpoch INT32 then nested tagged
        // fields. Official Java ShareFetchResponse.partitionResponse leaves
        // CurrentLeader at JSON 0/0. Apache ShareFetchResponse.java has no
        // currentLeader helper. This is not Fetch v12+ tagged field 1. This
        // does not start ShareAcknowledge CurrentLeader.
        let defaults = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let with_leader = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 2,
                current_leader_epoch: 7,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &with_leader).unwrap();
            let mut cur = buf.as_ref();
            let (got, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, with_leader);
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} CurrentLeader leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_response(&mut with, 0, &with_leader).unwrap();
        let mut empty = BytesMut::new();
        encode_share_fetch_response(&mut empty, 0, &defaults).unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareFetch CurrentLeader is not always 0/0"
        );

        let pr = ShareFetchedPartition::partition_response(0, 6);
        assert_eq!(pr.current_leader_id, 0);
        assert_eq!(pr.current_leader_epoch, 0);
        let mut conv = BytesMut::new();
        encode_share_fetch_response(
            &mut conv,
            0,
            &[ShareFetchedTopic {
                topic_id: [7u8; 16],
                partitions: vec![pr],
            }],
        )
        .unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "partition_response still writes CurrentLeader 0/0"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_response(&mut v1_with, 1, &with_leader).unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 CurrentLeader share layout; v1 still adds AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_fetch_response_error_message_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchResponse.json partition ErrorMessage
        // is versions 0+ (nullable compact STRING on every spoken version).
        // Official Java ShareFetchResponse.partitionResponse leaves it at
        // JSON null. Apache ShareFetchResponse.java has no errorMessage
        // helper. This is not the top-level ErrorMessage. This does not
        // start AcknowledgeErrorCode / AcknowledgeErrorMessage.
        let defaults = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let with_msg = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: Some("e".into()),
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let with_empty = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: Some(String::new()),
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &with_msg).unwrap();
            let mut cur = buf.as_ref();
            let (got, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, with_msg);
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} ErrorMessage leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_response(&mut with, 0, &with_msg).unwrap();
        let mut empty = BytesMut::new();
        encode_share_fetch_response(&mut empty, 0, &defaults).unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareFetch ErrorMessage is not always null"
        );

        let mut empty_present = BytesMut::new();
        encode_share_fetch_response(&mut empty_present, 0, &with_empty).unwrap();
        assert_ne!(
            &empty_present[..],
            &empty[..],
            "empty-but-present ErrorMessage is not JSON null"
        );
        let mut cur = empty_present.as_ref();
        let (got, ..) = decode_share_fetch_response(&mut cur, 0).unwrap();
        assert_eq!(got, with_empty);
        assert!(cur.is_empty(), "ShareFetch v0 ErrorMessage leftover-empty");

        let pr = ShareFetchedPartition::partition_response(0, 6);
        assert!(pr.error_message.is_none());
        let mut conv = BytesMut::new();
        encode_share_fetch_response(
            &mut conv,
            0,
            &[ShareFetchedTopic {
                topic_id: [7u8; 16],
                partitions: vec![pr],
            }],
        )
        .unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "partition_response still writes ErrorMessage null"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_response(&mut v1_with, 1, &with_msg).unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 ErrorMessage share compact layout; v1 still adds AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_fetch_response_acknowledge_error_code_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchResponse.json AcknowledgeErrorCode
        // is versions 0+ (INT16 on every spoken version). Official Java
        // ShareFetchResponse.partitionResponse leaves it at JSON 0.
        // Apache ShareFetchResponse.java has no acknowledgeErrorCode
        // helper. errorCounts uses ErrorCode, not AcknowledgeErrorCode.
        // JSON lists INVALID_RECORD_STATE as acknowledge-only. This is
        // not fetch ErrorCode. This does not start
        // AcknowledgeErrorMessage.
        let defaults = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let with_ack = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: crate::error::INVALID_RECORD_STATE,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &with_ack).unwrap();
            let mut cur = buf.as_ref();
            let (got, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, with_ack);
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} AcknowledgeErrorCode leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_response(&mut with, 0, &with_ack).unwrap();
        let mut empty = BytesMut::new();
        encode_share_fetch_response(&mut empty, 0, &defaults).unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareFetch AcknowledgeErrorCode is not always 0"
        );

        let pr = ShareFetchedPartition::partition_response(0, 6);
        assert_eq!(pr.acknowledge_error_code, 0);
        let mut conv = BytesMut::new();
        encode_share_fetch_response(
            &mut conv,
            0,
            &[ShareFetchedTopic {
                topic_id: [7u8; 16],
                partitions: vec![pr],
            }],
        )
        .unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "partition_response still writes AcknowledgeErrorCode 0"
        );

        assert_eq!(
            ShareFetchResponse::error_counts(0, &with_ack),
            HashMap::from([(0, 1), (6, 1)]),
            "errorCounts uses ErrorCode, not AcknowledgeErrorCode"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_response(&mut v1_with, 1, &with_ack).unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 AcknowledgeErrorCode share layout; v1 still adds AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_fetch_response_acknowledge_error_message_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchResponse.json AcknowledgeErrorMessage
        // is versions 0+ (nullable compact STRING on every spoken version).
        // Official Java ShareFetchResponse.partitionResponse leaves it at
        // JSON null. Apache ShareFetchResponse.java has no
        // acknowledgeErrorMessage helper. This is not fetch ErrorMessage
        // and not the top-level ErrorMessage. This does not start
        // ShareAcknowledge ErrorMessage.
        let defaults = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let with_msg = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: Some("e".into()),
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let with_empty = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: Some(String::new()),
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let with_fetch_msg = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: Some("e".into()),
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &with_msg).unwrap();
            let mut cur = buf.as_ref();
            let (got, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, with_msg);
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} AcknowledgeErrorMessage leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_response(&mut with, 0, &with_msg).unwrap();
        let mut empty = BytesMut::new();
        encode_share_fetch_response(&mut empty, 0, &defaults).unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareFetch AcknowledgeErrorMessage is not always null"
        );

        let mut fetch_msg = BytesMut::new();
        encode_share_fetch_response(&mut fetch_msg, 0, &with_fetch_msg).unwrap();
        assert_ne!(
            &with[..],
            &fetch_msg[..],
            "AcknowledgeErrorMessage is not fetch ErrorMessage"
        );

        let mut empty_present = BytesMut::new();
        encode_share_fetch_response(&mut empty_present, 0, &with_empty).unwrap();
        assert_ne!(
            &empty_present[..],
            &empty[..],
            "empty-but-present AcknowledgeErrorMessage is not JSON null"
        );
        let mut cur = empty_present.as_ref();
        let (got, ..) = decode_share_fetch_response(&mut cur, 0).unwrap();
        assert_eq!(got, with_empty);
        assert!(
            cur.is_empty(),
            "ShareFetch v0 AcknowledgeErrorMessage leftover-empty"
        );

        let pr = ShareFetchedPartition::partition_response(0, 6);
        assert!(pr.acknowledge_error_message.is_none());
        let mut conv = BytesMut::new();
        encode_share_fetch_response(
            &mut conv,
            0,
            &[ShareFetchedTopic {
                topic_id: [7u8; 16],
                partitions: vec![pr],
            }],
        )
        .unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "partition_response still writes AcknowledgeErrorMessage null"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_response(&mut v1_with, 1, &with_msg).unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 AcknowledgeErrorMessage share compact layout; v1 still adds AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_fetch_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchResponse.json ThrottleTimeMs is
        // versions 0+ (INT32 on every spoken version). Official Java
        // ShareFetchResponse.of / toMessage / getErrorResponse set it;
        // ShareFetchResponse.throttleTimeMs reads it. encode_share_fetch_response
        // still writes the JSON default 0. This is not v1
        // AcquisitionLockTimeoutMs, not the top-level ErrorMessage, and
        // not ShareAcknowledge ThrottleTimeMs.
        let topics = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response_with_throttle(&mut buf, version, &topics, 3_600_000)
                .unwrap();
            let mut cur = buf.as_ref();
            let (got, endpoints, throttle, ..) =
                decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, topics);
            assert!(endpoints.is_empty());
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_response_with_throttle(&mut with, 0, &topics, 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_share_fetch_response_with_throttle(&mut zero, 0, &topics, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_share_fetch_response(&mut conv, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_share_fetch_response still writes ThrottleTimeMs 0"
        );
        let mut endpoints_zero = BytesMut::new();
        encode_share_fetch_response_with_endpoints(&mut endpoints_zero, 0, &topics, &[]).unwrap();
        assert_eq!(
            &endpoints_zero[..],
            &zero[..],
            "encode_share_fetch_response_with_endpoints still writes ThrottleTimeMs 0"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_response_with_throttle(&mut v1_with, 1, &topics, 3_600_000).unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 both write ThrottleTimeMs (JSON 0+); v1 still adds AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_fetch_response_top_level_error_message_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchResponse.json top-level ErrorMessage
        // is versions 0+ (nullable compact STRING on every spoken version).
        // Official Java ShareFetchResponse.of / toMessage /
        // ShareFetchRequest.getErrorResponse leave it at JSON null.
        // Apache ShareFetchResponse.java has no errorMessage helper.
        // encode_share_fetch_response still writes null. This is not
        // partition ErrorMessage, not AcknowledgeErrorMessage, and not
        // ShareAcknowledge top-level ErrorMessage. Convenience encode of
        // this helper still writes ErrorCode 0.
        let topics = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        let with_part = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: Some("e".into()),
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response_with_error_message(&mut buf, version, &topics, Some("e"))
                .unwrap();
            let mut cur = buf.as_ref();
            let (got, endpoints, throttle, msg, ..) =
                decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, topics);
            assert!(endpoints.is_empty());
            assert_eq!(throttle, 0);
            assert_eq!(msg.as_deref(), Some("e"));
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} top-level ErrorMessage leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_response_with_error_message(&mut with, 0, &topics, Some("e")).unwrap();
        let mut empty = BytesMut::new();
        encode_share_fetch_response_with_error_message(&mut empty, 0, &topics, None).unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareFetch top-level ErrorMessage is not always null"
        );
        let mut conv = BytesMut::new();
        encode_share_fetch_response(&mut conv, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "encode_share_fetch_response still writes top-level ErrorMessage null"
        );

        let mut part_msg = BytesMut::new();
        encode_share_fetch_response(&mut part_msg, 0, &with_part).unwrap();
        assert_ne!(
            &with[..],
            &part_msg[..],
            "top-level ErrorMessage is not partition ErrorMessage"
        );

        let mut empty_present = BytesMut::new();
        encode_share_fetch_response_with_error_message(&mut empty_present, 0, &topics, Some(""))
            .unwrap();
        assert_ne!(
            &empty_present[..],
            &empty[..],
            "empty-but-present top-level ErrorMessage is not JSON null"
        );
        let mut cur = empty_present.as_ref();
        let (got, .., msg, _, _) = decode_share_fetch_response(&mut cur, 0).unwrap();
        assert_eq!(got, topics);
        assert_eq!(msg.as_deref(), Some(""));
        assert!(
            cur.is_empty(),
            "ShareFetch v0 top-level ErrorMessage leftover-empty"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_response_with_error_message(&mut v1_with, 1, &topics, Some("e"))
            .unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 top-level ErrorMessage share compact layout; v1 still adds AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_fetch_response_acquisition_lock_timeout_ms_matches_java() {
        // Kafka 4.1 ShareFetchResponse.json AcquisitionLockTimeoutMs is
        // versions 1+ (INT32 after ErrorMessage). Kafka 4.1.0 JSON has no
        // default; generated Java int32 default is 0. Official Java
        // ShareFetchResponse.of / toMessage / sizeOf take
        // acquisitionLockTimeout as an argument (not a named constant).
        // encode_share_fetch_response still writes 15000 on v1. v0 omits
        // even when the body is non-zero and decode fills 0. Error-path
        // encode still writes 0 (Java of last argument). Decode returns
        // the top-level ErrorCode and does not fail on a non-zero code.
        // This crate speaks 0–1.
        // This is not ThrottleTimeMs / top-level ErrorMessage /
        // ShareAcknowledge.
        let topics = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response_with_acquisition_lock_timeout(
                &mut buf, version, &topics, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (got, endpoints, throttle, msg, lock, ..) =
                decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, topics);
            assert!(endpoints.is_empty());
            assert_eq!(throttle, 0);
            assert_eq!(msg, None);
            if version >= 1 {
                assert_eq!(lock, 3_600_000);
            } else {
                assert_eq!(
                    lock, 0,
                    "ShareFetch v{version} omits AcquisitionLockTimeoutMs even when the body has a non-zero value"
                );
            }
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} AcquisitionLockTimeoutMs leftover-empty"
            );
        }

        let mut v0_with = BytesMut::new();
        encode_share_fetch_response_with_acquisition_lock_timeout(
            &mut v0_with,
            0,
            &topics,
            3_600_000,
        )
        .unwrap();
        let mut v0_zero = BytesMut::new();
        encode_share_fetch_response_with_acquisition_lock_timeout(&mut v0_zero, 0, &topics, 0)
            .unwrap();
        assert_eq!(
            &v0_with[..],
            &v0_zero[..],
            "v0 omits AcquisitionLockTimeoutMs even when the body has a non-zero value"
        );
        let mut conv_v0 = BytesMut::new();
        encode_share_fetch_response(&mut conv_v0, 0, &topics).unwrap();
        assert_eq!(
            &conv_v0[..],
            &v0_zero[..],
            "encode_share_fetch_response v0 has no AcquisitionLockTimeoutMs"
        );

        let mut with = BytesMut::new();
        encode_share_fetch_response_with_acquisition_lock_timeout(&mut with, 1, &topics, 3_600_000)
            .unwrap();
        let mut zero = BytesMut::new();
        encode_share_fetch_response_with_acquisition_lock_timeout(&mut zero, 1, &topics, 0)
            .unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v1 AcquisitionLockTimeoutMs is not always the generated Java default 0"
        );
        let mut conv = BytesMut::new();
        encode_share_fetch_response(&mut conv, 1, &topics).unwrap();
        let mut fifteen = BytesMut::new();
        encode_share_fetch_response_with_acquisition_lock_timeout(&mut fifteen, 1, &topics, 15_000)
            .unwrap();
        assert_eq!(
            &conv[..],
            &fifteen[..],
            "encode_share_fetch_response still writes AcquisitionLockTimeoutMs 15000"
        );
        assert_ne!(
            &conv[..],
            &with[..],
            "encode_share_fetch_response still writes 15000, not the helper value"
        );
        let mut endpoints_fifteen = BytesMut::new();
        encode_share_fetch_response_with_endpoints(&mut endpoints_fifteen, 1, &topics, &[])
            .unwrap();
        assert_eq!(
            &endpoints_fifteen[..],
            &fifteen[..],
            "encode_share_fetch_response_with_endpoints still writes AcquisitionLockTimeoutMs 15000"
        );
        let mut throttle_zero = BytesMut::new();
        encode_share_fetch_response_with_throttle(&mut throttle_zero, 1, &topics, 0).unwrap();
        assert_eq!(
            &throttle_zero[..],
            &fifteen[..],
            "encode_share_fetch_response_with_throttle still writes AcquisitionLockTimeoutMs 15000"
        );

        let mut throttle_same = BytesMut::new();
        encode_share_fetch_response_with_throttle(&mut throttle_same, 1, &topics, 3_600_000)
            .unwrap();
        assert_ne!(
            &with[..],
            &throttle_same[..],
            "AcquisitionLockTimeoutMs is not ThrottleTimeMs"
        );
        assert_eq!(
            &with[..4],
            &0i32.to_be_bytes(),
            "ShareFetch success-path ThrottleTimeMs stays 0 on this helper"
        );
        assert_eq!(
            &with[4..6],
            &0i16.to_be_bytes(),
            "ShareFetch success-path ErrorCode stays 0"
        );
        assert_eq!(
            with[6], 0,
            "ShareFetch success-path ErrorMessage stays null (compact)"
        );
        assert_eq!(
            &with[7..11],
            &3_600_000i32.to_be_bytes(),
            "ShareFetch v1 AcquisitionLockTimeoutMs is after ErrorMessage"
        );
        assert_ne!(
            &v0_with[..],
            &with[..],
            "v1 adds AcquisitionLockTimeoutMs after ErrorMessage"
        );
    }

    #[test]
    fn share_fetch_response_error_code_matches_java() {
        // Kafka 4.1 ShareFetchResponse.json ErrorCode is versions 0+
        // (INT16 after ThrottleTimeMs). JSON default is 0. Official Java
        // ShareFetchResponse.of / toMessage / error /
        // ShareFetchRequest.getErrorResponse set it.
        // encode_share_fetch_response still writes 0. Decode returns it
        // and does not fail on a non-zero code. v1 AcquisitionLockTimeoutMs
        // stays 15000 on this helper; error-path encode still writes 0
        // (Java of last argument) and empty Responses. This crate speaks
        // 0–1. This is not partition ErrorCode / ThrottleTimeMs /
        // ShareAcknowledge ErrorCode.
        let err = crate::error::INVALID_SHARE_SESSION_EPOCH;
        let topics = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchedPartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response_with_error_code(&mut buf, version, &topics, err).unwrap();
            let mut cur = buf.as_ref();
            let (got, endpoints, throttle, msg, lock, error_code) =
                decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(got, topics);
            assert!(endpoints.is_empty());
            assert_eq!(throttle, 0);
            assert_eq!(msg, None);
            if version >= 1 {
                assert_eq!(lock, 15_000);
            } else {
                assert_eq!(lock, 0);
            }
            assert_eq!(error_code, err);
            assert_ne!(
                error_code, 6,
                "top-level ErrorCode is not partition ErrorCode"
            );
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} ErrorCode leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_response_with_error_code(&mut with, 0, &topics, err).unwrap();
        let mut zero = BytesMut::new();
        encode_share_fetch_response_with_error_code(&mut zero, 0, &topics, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ErrorCode is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_share_fetch_response(&mut conv, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_share_fetch_response still writes ErrorCode 0"
        );
        let mut lock_zero = BytesMut::new();
        encode_share_fetch_response_with_acquisition_lock_timeout(&mut lock_zero, 0, &topics, 0)
            .unwrap();
        assert_eq!(
            &lock_zero[..],
            &zero[..],
            "encode_share_fetch_response_with_acquisition_lock_timeout still writes ErrorCode 0"
        );
        assert_eq!(
            &with[..4],
            &0i32.to_be_bytes(),
            "ShareFetch success-path ThrottleTimeMs stays 0 on this helper"
        );
        assert_eq!(
            &with[4..6],
            &err.to_be_bytes(),
            "ShareFetch ErrorCode is at bytes 4–5"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_response_with_error_code(&mut v1_with, 1, &topics, err).unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 both write ErrorCode (JSON 0+); v1 still adds AcquisitionLockTimeoutMs"
        );

        let mut err_path = BytesMut::new();
        encode_share_fetch_error(&mut err_path, 1, err).unwrap();
        assert_ne!(
            &v1_with[..],
            &err_path[..],
            "success-path ErrorCode helper is not encode_share_fetch_error (lock 15000 vs 0, topics vs empty)"
        );
        let mut err_cur = err_path.as_ref();
        let (got, endpoints, throttle, msg, lock, error_code) =
            decode_share_fetch_response(&mut err_cur, 1).unwrap();
        assert!(got.is_empty());
        assert!(endpoints.is_empty());
        assert_eq!(throttle, 0);
        assert_eq!(msg, None);
        assert_eq!(
            lock, 0,
            "ShareFetch error v1 AcquisitionLockTimeoutMs stays 0"
        );
        assert_eq!(error_code, err);
        assert!(
            err_cur.is_empty(),
            "ShareFetch error v1 ErrorCode leftover-empty"
        );
    }

    #[test]
    fn share_fetch_partition_response_leftover_empty() {
        let err = ShareFetchedPartition::partition_response(3, crate::error::UNKNOWN_TOPIC_ID);
        assert_eq!(
            err,
            ShareFetchedPartition {
                partition: 3,
                error_code: crate::error::UNKNOWN_TOPIC_ID,
                error_message: None,
                acknowledge_error_code: 0,
                acknowledge_error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
                records: Vec::new(),
                acquired: Vec::new(),
            }
        );
        let topics = vec![ShareFetchedTopic {
            topic_id: [7u8; 16],
            partitions: vec![
                ShareFetchedPartition::partition_response(0, crate::error::UNKNOWN_TOPIC_ID),
                ShareFetchedPartition::partition_response(3, crate::error::UNKNOWN_TOPIC_ID),
            ],
        }];
        for version in [0i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &topics).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(decoded, topics);
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} partitionResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        let empty: Vec<ShareFetchedTopic> = Vec::new();
        for version in [0i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &empty).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(decoded, empty);
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} empty partitionResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn share_fetch_response_records_size_matches_java() {
        // Java ShareFetchResponse.recordsSize: 0 when records are null or
        // MemoryRecords.EMPTY. Otherwise sizeInBytes of the records blob.
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"s")),
            headers: vec![],
        };
        let mut part = ShareFetchedPartition::partition_response(0, 0);
        assert_eq!(part.records_size().unwrap(), 0);
        part.records = vec![RecordBatch::from_records(vec![rec])];
        let size = part.records_size().unwrap();
        assert!(size > 0, "non-empty recordsSize must be the blob length");
        let mut recs = BytesMut::new();
        let batch = part.records.first().expect("one batch");
        records::encode_record_batch(&mut recs, batch).unwrap();
        assert_eq!(
            size,
            buf::i32_from_usize(recs.len()).unwrap(),
            "recordsSize must match encoded records blob"
        );
        let topics = vec![ShareFetchedTopic {
            topic_id: [0u8; 16],
            partitions: vec![part.clone()],
        }];
        for version in [0i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &topics).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} recordsSize leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            let got = decoded
                .first()
                .and_then(|t| t.partitions.first())
                .expect("one partition");
            assert_eq!(
                got.records_size().unwrap(),
                size,
                "v{version} decoded recordsSize must match"
            );
        }
    }

    #[test]
    fn share_acknowledge_partition_response_leftover_empty() {
        let err = ShareAcknowledgeResponsePartition::partition_response(
            3,
            crate::error::UNKNOWN_TOPIC_ID,
        );
        assert_eq!(
            err,
            ShareAcknowledgeResponsePartition {
                partition: 3,
                error_code: crate::error::UNKNOWN_TOPIC_ID,
                error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
            }
        );
        let topics = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![
                ShareAcknowledgeResponsePartition::partition_response(
                    0,
                    crate::error::UNKNOWN_TOPIC_ID,
                ),
                ShareAcknowledgeResponsePartition::partition_response(
                    3,
                    crate::error::UNKNOWN_TOPIC_ID,
                ),
            ],
        }];
        for version in [0i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_acknowledge_topics_response(&mut buf, version, 0, &topics).unwrap();
            let mut cur = buf.as_ref();
            let (top, decoded, ..) =
                decode_share_acknowledge_topics_response(&mut cur, version).unwrap();
            assert_eq!(top, 0);
            assert_eq!(decoded, topics);
            assert!(
                !cur.has_remaining(),
                "ShareAcknowledge v{version} partitionResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            assert_eq!(
                decode_share_acknowledge_response(&mut buf.as_ref(), version).unwrap(),
                0
            );
        }
        let empty: Vec<ShareAcknowledgeResponseTopic> = Vec::new();
        for version in [0i16, 1] {
            let mut via_topics = BytesMut::new();
            encode_share_acknowledge_topics_response(&mut via_topics, version, 0, &empty).unwrap();
            let mut via_empty = BytesMut::new();
            encode_share_acknowledge_response(&mut via_empty, version, 0).unwrap();
            assert_eq!(
                via_topics.as_ref(),
                via_empty.as_ref(),
                "empty Responses matches getErrorResponse encode"
            );
            let mut cur = via_topics.as_ref();
            let (top, decoded, ..) =
                decode_share_acknowledge_topics_response(&mut cur, version).unwrap();
            assert_eq!(top, 0);
            assert_eq!(decoded, empty);
            assert!(
                !cur.has_remaining(),
                "ShareAcknowledge v{version} empty partitionResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn share_acknowledge_response_node_endpoints_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareAcknowledgeResponse.json NodeEndpoints
        // is versions 0+ (untagged compact array on every spoken version).
        // v0 and v1 bodies match. Inner layout matches Produce / Fetch /
        // ShareFetch NodeEndpoint. This is not Fetch v16 tagged field 0.
        // encode_share_acknowledge_topics_response still writes empty.
        let topics = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
            }],
        }];
        let endpoints = [crate::protocol::api::NodeEndpoint {
            node_id: 3,
            host: "h".into(),
            port: 1,
            rack: Some("r".into()),
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_acknowledge_topics_response_with_endpoints(
                &mut buf, version, 0, &topics, &endpoints,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (top, got, eps, ..) =
                decode_share_acknowledge_topics_response(&mut cur, version).unwrap();
            assert_eq!(top, 0);
            assert_eq!(got, topics);
            assert_eq!(eps, endpoints);
            assert!(
                cur.is_empty(),
                "ShareAcknowledge v{version} NodeEndpoints leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_acknowledge_topics_response_with_endpoints(
            &mut with, 0, 0, &topics, &endpoints,
        )
        .unwrap();
        let mut empty = BytesMut::new();
        encode_share_acknowledge_topics_response_with_endpoints(&mut empty, 0, 0, &topics, &[])
            .unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareAcknowledge NodeEndpoints is not always empty"
        );
        let mut conv = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut conv, 0, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "encode_share_acknowledge_topics_response still writes empty NodeEndpoints"
        );

        let mut v1_with = BytesMut::new();
        encode_share_acknowledge_topics_response_with_endpoints(
            &mut v1_with,
            1,
            0,
            &topics,
            &endpoints,
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "v0 and v1 ShareAcknowledge NodeEndpoints layout match; do not confuse with ShareFetch AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_acknowledge_response_current_leader_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareAcknowledgeResponse.json partition
        // CurrentLeader is versions 0+ (untagged nested LeaderIdAndEpoch
        // on every spoken version). LeaderId INT32 then LeaderEpoch INT32
        // then nested tagged fields. Official Java
        // ShareAcknowledgeResponse.partitionResponse leaves CurrentLeader
        // at JSON 0/0. Apache ShareAcknowledgeResponse.java has no
        // currentLeader helper. v0 and v1 bodies match. This is not Fetch
        // v12+ tagged field 1. This is not ShareFetch CurrentLeader
        // (ShareFetch v1 still adds AcquisitionLockTimeoutMs).
        let defaults = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
            }],
        }];
        let with_leader = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                current_leader_id: 2,
                current_leader_epoch: 7,
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_acknowledge_topics_response(&mut buf, version, 0, &with_leader).unwrap();
            let mut cur = buf.as_ref();
            let (top, got, ..) =
                decode_share_acknowledge_topics_response(&mut cur, version).unwrap();
            assert_eq!(top, 0);
            assert_eq!(got, with_leader);
            assert!(
                cur.is_empty(),
                "ShareAcknowledge v{version} CurrentLeader leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut with, 0, 0, &with_leader).unwrap();
        let mut empty = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut empty, 0, 0, &defaults).unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareAcknowledge CurrentLeader is not always 0/0"
        );

        let pr = ShareAcknowledgeResponsePartition::partition_response(0, 6);
        assert_eq!(pr.current_leader_id, 0);
        assert_eq!(pr.current_leader_epoch, 0);
        let mut conv = BytesMut::new();
        encode_share_acknowledge_topics_response(
            &mut conv,
            0,
            0,
            &[ShareAcknowledgeResponseTopic {
                topic_id: [7u8; 16],
                partitions: vec![pr],
            }],
        )
        .unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "partition_response still writes CurrentLeader 0/0"
        );

        let mut v1_with = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut v1_with, 1, 0, &with_leader).unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "v0 and v1 CurrentLeader layout match; ShareAcknowledge has no AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_acknowledge_response_error_message_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareAcknowledgeResponse.json partition
        // ErrorMessage is versions 0+ (nullable compact STRING on every
        // spoken version). Official Java
        // ShareAcknowledgeResponse.partitionResponse leaves it at JSON
        // null. Apache ShareAcknowledgeResponse.java has no errorMessage
        // helper. v0 and v1 bodies match. This is not the top-level
        // ErrorMessage. This is not ShareFetch ErrorMessage (ShareFetch v1
        // still adds AcquisitionLockTimeoutMs).
        let defaults = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
            }],
        }];
        let with_msg = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: Some("e".into()),
                current_leader_id: 0,
                current_leader_epoch: 0,
            }],
        }];
        let with_empty = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: Some(String::new()),
                current_leader_id: 0,
                current_leader_epoch: 0,
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_acknowledge_topics_response(&mut buf, version, 0, &with_msg).unwrap();
            let mut cur = buf.as_ref();
            let (top, got, ..) =
                decode_share_acknowledge_topics_response(&mut cur, version).unwrap();
            assert_eq!(top, 0);
            assert_eq!(got, with_msg);
            assert!(
                cur.is_empty(),
                "ShareAcknowledge v{version} ErrorMessage leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut with, 0, 0, &with_msg).unwrap();
        let mut empty = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut empty, 0, 0, &defaults).unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareAcknowledge ErrorMessage is not always null"
        );

        let mut empty_present = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut empty_present, 0, 0, &with_empty).unwrap();
        assert_ne!(
            &empty_present[..],
            &empty[..],
            "empty-but-present ErrorMessage is not JSON null"
        );
        let mut cur = empty_present.as_ref();
        let (top, got, ..) = decode_share_acknowledge_topics_response(&mut cur, 0).unwrap();
        assert_eq!(top, 0);
        assert_eq!(got, with_empty);
        assert!(
            cur.is_empty(),
            "ShareAcknowledge v0 ErrorMessage leftover-empty"
        );

        let pr = ShareAcknowledgeResponsePartition::partition_response(0, 6);
        assert!(pr.error_message.is_none());
        let mut conv = BytesMut::new();
        encode_share_acknowledge_topics_response(
            &mut conv,
            0,
            0,
            &[ShareAcknowledgeResponseTopic {
                topic_id: [7u8; 16],
                partitions: vec![pr],
            }],
        )
        .unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "partition_response still writes ErrorMessage null"
        );

        let mut v1_with = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut v1_with, 1, 0, &with_msg).unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "v0 and v1 ErrorMessage layout match; ShareAcknowledge has no AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_acknowledge_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareAcknowledgeResponse.json ThrottleTimeMs is
        // versions 0+ (INT32 on every spoken version; ignorable). Official
        // Java ShareAcknowledgeRequest.getErrorResponse /
        // ShareAcknowledgeResponse.of / toMessage set it;
        // ShareAcknowledgeResponse.throttleTimeMs reads it.
        // encode_share_acknowledge_topics_response still writes the JSON
        // default 0. v0 and v1 bodies match. This is not the top-level
        // ErrorMessage and not ShareFetch ThrottleTimeMs (ShareFetch v1
        // still adds AcquisitionLockTimeoutMs).
        let topics = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_acknowledge_topics_response_with_throttle(
                &mut buf, version, 0, &topics, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (top, got, endpoints, throttle, ..) =
                decode_share_acknowledge_topics_response(&mut cur, version).unwrap();
            assert_eq!(top, 0);
            assert_eq!(got, topics);
            assert!(endpoints.is_empty());
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "ShareAcknowledge v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_acknowledge_topics_response_with_throttle(&mut with, 0, 0, &topics, 3_600_000)
            .unwrap();
        let mut zero = BytesMut::new();
        encode_share_acknowledge_topics_response_with_throttle(&mut zero, 0, 0, &topics, 0)
            .unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut conv, 0, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_share_acknowledge_topics_response still writes ThrottleTimeMs 0"
        );
        let mut endpoints_zero = BytesMut::new();
        encode_share_acknowledge_topics_response_with_endpoints(
            &mut endpoints_zero,
            0,
            0,
            &topics,
            &[],
        )
        .unwrap();
        assert_eq!(
            &endpoints_zero[..],
            &zero[..],
            "encode_share_acknowledge_topics_response_with_endpoints still writes ThrottleTimeMs 0"
        );
        let mut error_enc = BytesMut::new();
        encode_share_acknowledge_response(&mut error_enc, 0, 0).unwrap();
        let mut empty_topics = BytesMut::new();
        encode_share_acknowledge_topics_response_with_throttle(&mut empty_topics, 0, 0, &[], 0)
            .unwrap();
        assert_eq!(
            &error_enc[..],
            &empty_topics[..],
            "encode_share_acknowledge_response still writes ThrottleTimeMs 0"
        );

        let mut v1_with = BytesMut::new();
        encode_share_acknowledge_topics_response_with_throttle(
            &mut v1_with,
            1,
            0,
            &topics,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "v0 and v1 both write ThrottleTimeMs (JSON 0+); ShareAcknowledge has no AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_acknowledge_response_top_level_error_message_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareAcknowledgeResponse.json top-level
        // ErrorMessage is versions 0+ (nullable compact STRING on every
        // spoken version). Official Java ShareAcknowledgeResponse.of /
        // toMessage / ShareAcknowledgeRequest.getErrorResponse leave it at
        // JSON null. Apache ShareAcknowledgeResponse.java has no
        // errorMessage helper. encode_share_acknowledge_topics_response
        // still writes null. v0 and v1 bodies match. This is not partition
        // ErrorMessage and not ShareFetch top-level ErrorMessage
        // (ShareFetch v1 still adds AcquisitionLockTimeoutMs).
        let topics = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: None,
                current_leader_id: 0,
                current_leader_epoch: 0,
            }],
        }];
        let with_part = vec![ShareAcknowledgeResponseTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareAcknowledgeResponsePartition {
                partition: 0,
                error_code: 6,
                error_message: Some("e".into()),
                current_leader_id: 0,
                current_leader_epoch: 0,
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_acknowledge_topics_response_with_error_message(
                &mut buf,
                version,
                0,
                &topics,
                Some("e"),
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (top, got, endpoints, throttle, msg) =
                decode_share_acknowledge_topics_response(&mut cur, version).unwrap();
            assert_eq!(top, 0);
            assert_eq!(got, topics);
            assert!(endpoints.is_empty());
            assert_eq!(throttle, 0);
            assert_eq!(msg.as_deref(), Some("e"));
            assert!(
                cur.is_empty(),
                "ShareAcknowledge v{version} top-level ErrorMessage leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_acknowledge_topics_response_with_error_message(
            &mut with,
            0,
            0,
            &topics,
            Some("e"),
        )
        .unwrap();
        let mut empty = BytesMut::new();
        encode_share_acknowledge_topics_response_with_error_message(
            &mut empty, 0, 0, &topics, None,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareAcknowledge top-level ErrorMessage is not always null"
        );
        let mut conv = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut conv, 0, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "encode_share_acknowledge_topics_response still writes top-level ErrorMessage null"
        );

        let mut part_msg = BytesMut::new();
        encode_share_acknowledge_topics_response(&mut part_msg, 0, 0, &with_part).unwrap();
        assert_ne!(
            &with[..],
            &part_msg[..],
            "top-level ErrorMessage is not partition ErrorMessage"
        );

        let mut empty_present = BytesMut::new();
        encode_share_acknowledge_topics_response_with_error_message(
            &mut empty_present,
            0,
            0,
            &topics,
            Some(""),
        )
        .unwrap();
        assert_ne!(
            &empty_present[..],
            &empty[..],
            "empty-but-present top-level ErrorMessage is not JSON null"
        );
        let mut cur = empty_present.as_ref();
        let (top, got, .., msg) = decode_share_acknowledge_topics_response(&mut cur, 0).unwrap();
        assert_eq!(top, 0);
        assert_eq!(got, topics);
        assert_eq!(msg.as_deref(), Some(""));
        assert!(
            cur.is_empty(),
            "ShareAcknowledge v0 top-level ErrorMessage leftover-empty"
        );

        let mut v1_with = BytesMut::new();
        encode_share_acknowledge_topics_response_with_error_message(
            &mut v1_with,
            1,
            0,
            &topics,
            Some("e"),
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "v0 and v1 top-level ErrorMessage layout match; ShareAcknowledge has no AcquisitionLockTimeoutMs"
        );
    }

    #[test]
    fn share_acknowledge_response_error_counts_matches_java() {
        assert_eq!(
            ShareAcknowledgeResponse::error_counts(0, &[]),
            HashMap::from([(0, 1)])
        );
        let topics = vec![
            ShareAcknowledgeResponseTopic {
                topic_id: [1u8; 16],
                partitions: vec![
                    ShareAcknowledgeResponsePartition::partition_response(0, 0),
                    ShareAcknowledgeResponsePartition::partition_response(
                        1,
                        crate::error::UNKNOWN_TOPIC_ID,
                    ),
                ],
            },
            ShareAcknowledgeResponseTopic {
                topic_id: [2u8; 16],
                partitions: vec![ShareAcknowledgeResponsePartition::partition_response(0, 0)],
            },
        ];
        assert_eq!(
            ShareAcknowledgeResponse::error_counts(0, &topics),
            HashMap::from([(0, 3), (crate::error::UNKNOWN_TOPIC_ID, 1)])
        );
        let top =
            ShareAcknowledgeResponse::error_counts(crate::error::GROUP_AUTHORIZATION_FAILED, &[]);
        assert_eq!(
            top,
            HashMap::from([(crate::error::GROUP_AUTHORIZATION_FAILED, 1)])
        );
        let same = ShareAcknowledgeResponse::error_counts(
            crate::error::UNKNOWN_TOPIC_ID,
            &[ShareAcknowledgeResponseTopic {
                topic_id: [3u8; 16],
                partitions: vec![ShareAcknowledgeResponsePartition::partition_response(
                    0,
                    crate::error::UNKNOWN_TOPIC_ID,
                )],
            }],
        );
        assert_eq!(same, HashMap::from([(crate::error::UNKNOWN_TOPIC_ID, 2)]));
    }

    #[test]
    fn share_acknowledge_response_to_message_matches_java() {
        // Java ShareAcknowledgeResponse.toMessage: LinkedHashMap by
        // topicId, first-seen order, setPartitionIndex from the key.
        // Non-adjacent same topicId still merges.
        assert!(ShareAcknowledgeResponse::to_message(&[]).is_empty());
        let a = [1u8; 16];
        let b = [2u8; 16];
        let body0 = ShareAcknowledgeResponsePartition::partition_response(99, 0);
        let body1 = ShareAcknowledgeResponsePartition::partition_response(
            1,
            crate::error::UNKNOWN_TOPIC_ID,
        );
        let body2 = ShareAcknowledgeResponsePartition::partition_response(2, 0);
        let grouped = ShareAcknowledgeResponse::to_message(&[
            (a, 0, body0.clone()),
            (b, 1, body1),
            (a, 3, body2),
        ]);
        assert_eq!(
            grouped,
            vec![
                ShareAcknowledgeResponseTopic {
                    topic_id: a,
                    partitions: vec![
                        ShareAcknowledgeResponsePartition {
                            partition: 0,
                            error_code: 0,
                            error_message: None,
                            current_leader_id: 0,
                            current_leader_epoch: 0,
                        },
                        ShareAcknowledgeResponsePartition {
                            partition: 3,
                            error_code: 0,
                            error_message: None,
                            current_leader_id: 0,
                            current_leader_epoch: 0,
                        },
                    ],
                },
                ShareAcknowledgeResponseTopic {
                    topic_id: b,
                    partitions: vec![ShareAcknowledgeResponsePartition {
                        partition: 1,
                        error_code: crate::error::UNKNOWN_TOPIC_ID,
                        error_message: None,
                        current_leader_id: 0,
                        current_leader_epoch: 0,
                    }],
                },
            ]
        );
        assert_eq!(
            grouped
                .first()
                .and_then(|topic| topic.partitions.first())
                .map(|part| part.partition),
            Some(0),
            "setPartitionIndex copies the key partition onto the body"
        );
        assert_eq!(body0.partition, 99);
        for version in [0i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_acknowledge_topics_response(&mut buf, version, 0, &grouped).unwrap();
            let mut cur = buf.as_ref();
            let (top, decoded, ..) =
                decode_share_acknowledge_topics_response(&mut cur, version).unwrap();
            assert_eq!(top, 0);
            assert_eq!(decoded, grouped);
            assert!(
                !cur.has_remaining(),
                "ShareAcknowledge v{version} toMessage leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn share_acknowledge_request_for_consumer_matches_java() {
        // Java ShareAcknowledgeRequest.Builder.forConsumer: HashMap by
        // topicId, inner HashMap by partitionIndex.
        // setAcknowledgementBatches replaces (last wins). Empty map is
        // empty Topics. Intervening ids still merge. Topic name is not
        // used.
        assert!(ShareAcknowledgeRequest::for_consumer(std::iter::empty::<(
            [u8; 16],
            i32,
            Vec<AcknowledgementBatch>
        )>())
        .is_empty());
        let a = [1u8; 16];
        let b = [2u8; 16];
        let b0 = AcknowledgementBatch {
            first_offset: 0,
            last_offset: 1,
            types: vec![ACK_ACCEPT],
        };
        let b1 = AcknowledgementBatch {
            first_offset: 2,
            last_offset: 3,
            types: vec![ACK_RELEASE],
        };
        let b2 = AcknowledgementBatch {
            first_offset: 4,
            last_offset: 5,
            types: vec![ACK_REJECT],
        };
        let grouped = ShareAcknowledgeRequest::for_consumer([
            (a, 0, vec![b0.clone()]),
            (b, 1, vec![b1.clone()]),
            (a, 2, vec![b2.clone()]),
        ]);
        assert_eq!(
            grouped,
            vec![
                ShareAckTopic {
                    topic_id: a,
                    partitions: vec![(0, vec![b0.clone()]), (2, vec![b2.clone()])],
                },
                ShareAckTopic {
                    topic_id: b,
                    partitions: vec![(1, vec![b1.clone()])],
                },
            ]
        );
        let last_wins = ShareAcknowledgeRequest::for_consumer([
            (a, 0, vec![b0.clone()]),
            (a, 0, vec![b2.clone()]),
        ]);
        assert_eq!(
            last_wins,
            vec![ShareAckTopic {
                topic_id: a,
                partitions: vec![(0, vec![b2.clone()])],
            }]
        );
        let order = ShareAcknowledgeRequest::for_consumer([
            (a, 0, vec![b0]),
            (a, 1, vec![b1.clone()]),
            (a, 0, vec![b2.clone()]),
        ]);
        assert_eq!(
            order,
            vec![ShareAckTopic {
                topic_id: a,
                partitions: vec![(0, vec![b2]), (1, vec![b1])],
            }]
        );
        leftover_share_ack_for_consumer(0, &grouped);
        leftover_share_ack_for_consumer(0, &[]);
        leftover_share_ack_for_consumer(1, &grouped);
        leftover_share_ack_for_consumer(1, &[]);
    }

    fn leftover_share_ack_for_consumer(version: i16, topics: &[ShareAckTopic]) {
        let mut buf = BytesMut::new();
        encode_share_acknowledge_topics(&mut buf, version, "g", "m", 1, topics).unwrap();
        let mut cur = buf.as_ref();
        let (gid, mid, epoch, flat) = decode_share_acknowledge_request(&mut cur, version).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(mid, "m");
        assert_eq!(epoch, 1);
        assert_eq!(
            ShareAcknowledgeRequest::for_consumer(flat).as_slice(),
            topics
        );
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            !cur.has_remaining(),
            "ShareAcknowledge v{version} Builder.forConsumer {empty}leftover-empty; leftover {} bytes",
            cur.remaining()
        );
    }

    #[test]
    fn share_fetch_response_error_counts_matches_java() {
        assert_eq!(
            ShareFetchResponse::error_counts(0, &[]),
            HashMap::from([(0, 1)])
        );
        let topics = vec![ShareFetchedTopic {
            topic_id: [1u8; 16],
            partitions: vec![
                ShareFetchedPartition::partition_response(0, 0),
                ShareFetchedPartition::partition_response(1, crate::error::UNKNOWN_TOPIC_ID),
            ],
        }];
        assert_eq!(
            ShareFetchResponse::error_counts(0, &topics),
            HashMap::from([(0, 2), (crate::error::UNKNOWN_TOPIC_ID, 1)])
        );
        assert_eq!(
            ShareFetchResponse::error_counts(crate::error::GROUP_AUTHORIZATION_FAILED, &[]),
            HashMap::from([(crate::error::GROUP_AUTHORIZATION_FAILED, 1)])
        );
        let same = ShareFetchResponse::error_counts(
            crate::error::UNKNOWN_TOPIC_ID,
            &[ShareFetchedTopic {
                topic_id: [2u8; 16],
                partitions: vec![ShareFetchedPartition::partition_response(
                    0,
                    crate::error::UNKNOWN_TOPIC_ID,
                )],
            }],
        );
        assert_eq!(same, HashMap::from([(crate::error::UNKNOWN_TOPIC_ID, 2)]));
    }

    #[test]
    fn share_fetch_response_response_data_matches_java() {
        // Java ShareFetchResponse.responseData: look up topicId in
        // topicNames and skip a missing name (name != null). Keys are
        // TopicIdPartition (topic_id, name, partition). LinkedHashMap.put
        // overwrites the same triple.
        let topic_id = [1u8; 16];
        let unknown_id = [2u8; 16];
        let p0 = ShareFetchedPartition::partition_response(0, 0);
        let p1 = ShareFetchedPartition::partition_response(1, crate::error::UNKNOWN_TOPIC_ID);
        let overwrite =
            ShareFetchedPartition::partition_response(0, crate::error::NOT_LEADER_OR_FOLLOWER);
        let topics = vec![
            ShareFetchedTopic {
                topic_id,
                partitions: vec![p0.clone(), p1.clone()],
            },
            ShareFetchedTopic {
                topic_id,
                partitions: vec![overwrite.clone()],
            },
            ShareFetchedTopic {
                topic_id: unknown_id,
                partitions: vec![ShareFetchedPartition::partition_response(0, 0)],
            },
        ];
        assert!(ShareFetchResponse::response_data(&[], &HashMap::new()).is_empty());
        assert!(ShareFetchResponse::response_data(&topics, &HashMap::new()).is_empty());
        let names = HashMap::from([(topic_id, "t".into())]);
        assert_eq!(
            ShareFetchResponse::response_data(&topics, &names),
            HashMap::from([
                ((topic_id, "t".into(), 0), overwrite.clone()),
                ((topic_id, "t".into(), 1), p1.clone()),
            ])
        );
        for version in [0i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &topics).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(decoded, topics);
            assert_eq!(
                ShareFetchResponse::response_data(&decoded, &names),
                ShareFetchResponse::response_data(&topics, &names)
            );
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} responseData leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn share_fetch_request_forgotten_topics_matches_java() {
        // Java ShareFetchRequest.forgottenTopics: look up topicId in
        // topicNames and still insert when the name is null. ArrayList
        // keeps duplicate partitions. responseData skips a missing name.
        let topic_id = [1u8; 16];
        let forgotten = vec![
            ShareForgottenTopic {
                topic_id,
                partitions: vec![0, 1],
            },
            ShareForgottenTopic {
                topic_id,
                partitions: vec![0],
            },
        ];
        let empty_names = HashMap::new();
        assert!(ShareFetchRequest::forgotten_topics(&[], &empty_names).is_empty());
        let unresolved = ShareFetchRequest::forgotten_topics(&forgotten, &empty_names);
        assert_eq!(
            unresolved,
            vec![
                (topic_id, None, 0),
                (topic_id, None, 1),
                (topic_id, None, 0),
            ],
            "missing name is still inserted"
        );
        let names = HashMap::from([(topic_id, "t".into())]);
        assert_eq!(
            ShareFetchRequest::forgotten_topics(&forgotten, &names),
            vec![
                (topic_id, Some("t".into()), 0),
                (topic_id, Some("t".into()), 1),
                (topic_id, Some("t".into()), 0),
            ]
        );

        let fetch = vec![ShareFetchTopic {
            topic_id,
            partitions: vec![ShareFetchPartition {
                partition: 0,
                partition_max_bytes: 1024,
                acknowledgements: vec![],
            }],
        }];
        for version in [0_i16, 1] {
            let got = ShareFetchRequest::forgotten_topics(&forgotten, &names);
            assert_eq!(got.len(), 3);
            let mut buf = BytesMut::new();
            encode_share_fetch_request(&mut buf, version, "sg", "m1", 0, 10, 1, 1024, 16, &fetch)
                .unwrap();
            let mut cur = buf.as_ref();
            let (_gid, _mid, _epoch, _max, _decoded, ..) =
                decode_share_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} forgottenTopics leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [0_i16, 1] {
            let got = ShareFetchRequest::forgotten_topics(&[], &empty_names);
            assert!(got.is_empty());
            let mut buf = BytesMut::new();
            encode_share_fetch_request(&mut buf, version, "sg", "m1", 0, 10, 1, 1024, 16, &fetch)
                .unwrap();
            let mut cur = buf.as_ref();
            let (_gid, _mid, _epoch, _max, _decoded, ..) =
                decode_share_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} forgottenTopics empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn share_fetch_request_forgotten_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchRequest.json ForgottenTopicsData is
        // versions 0+ (always on the wire for spoken v0–v1). TopicId UUID
        // then compact []int32 Partitions plus nested tagged fields.
        // encode_share_fetch_request still writes empty.
        let topics: Vec<ShareFetchTopic> = Vec::new();
        let forgotten = [ShareForgottenTopic {
            topic_id: [7u8; 16],
            partitions: vec![1, 1],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_request_with_forgotten(
                &mut buf, version, "sg", "m1", 0, 10, 1, 1024, 16, &topics, &forgotten,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (.., got, _, _, _, _) = decode_share_fetch_request(&mut cur, version).unwrap();
            assert_eq!(got.as_slice(), forgotten.as_slice());
            assert!(
                cur.is_empty(),
                "ShareFetch v{version} ForgottenTopicsData leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_request_with_forgotten(
            &mut with, 0, "sg", "m1", 0, 10, 1, 1024, 16, &topics, &forgotten,
        )
        .unwrap();
        let mut empty = BytesMut::new();
        encode_share_fetch_request_with_forgotten(
            &mut empty,
            0,
            "sg",
            "m1",
            0,
            10,
            1,
            1024,
            16,
            &topics,
            &[],
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &empty[..],
            "ShareFetch ForgottenTopicsData is not always empty"
        );
        let mut conv = BytesMut::new();
        encode_share_fetch_request(&mut conv, 0, "sg", "m1", 0, 10, 1, 1024, 16, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &empty[..],
            "encode_share_fetch_request still writes empty ForgottenTopicsData"
        );

        let mut v1_with = BytesMut::new();
        encode_share_fetch_request_with_forgotten(
            &mut v1_with,
            1,
            "sg",
            "m1",
            0,
            10,
            1,
            1024,
            16,
            &topics,
            &forgotten,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 ForgottenTopicsData share TopicId layout; v1 still adds MaxRecords / BatchSize"
        );
    }

    #[test]
    fn share_fetch_request_batch_size_matches_java() {
        // Kafka 4.1 ShareFetchRequest.json BatchSize is versions 1+
        // (INT32 after MaxRecords). Kafka 4.1.0 JSON has no default;
        // generated Java int32 default is 0. Official Java
        // ShareFetchRequest.Builder.forConsumer takes maxRecords and
        // batchSize as separate arguments. encode_share_fetch_request
        // still writes BatchSize as MaxRecords. v0 omits even when the
        // body is non-zero and decode fills 0. This crate speaks 0–1.
        // This is not MaxRecords / MaxBytes / PartitionMaxBytes /
        // AcquisitionLockTimeoutMs.
        let topics = vec![ShareFetchTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchPartition {
                partition: 0,
                partition_max_bytes: 0,
                acknowledgements: vec![],
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_request_with_batch_size(
                &mut buf, version, "sg", "m1", 0, 10, 1, 1024, 16, &topics, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (gid, mid, epoch, max_records, got, forgotten, batch_size, ..) =
                decode_share_fetch_request(&mut cur, version).unwrap();
            assert_eq!(gid.as_str(), "sg");
            assert_eq!(mid.as_str(), "m1");
            assert_eq!(epoch, 0);
            assert!(forgotten.is_empty());
            assert_eq!(got, topics);
            if version >= 1 {
                assert_eq!(max_records, 16);
                assert_eq!(batch_size, 3_600_000);
            } else {
                assert_eq!(max_records, 0, "v0 omits MaxRecords");
                assert_eq!(
                    batch_size, 0,
                    "ShareFetch v{version} omits BatchSize even when the body has a non-zero value"
                );
            }
            assert!(
                cur.is_empty(),
                "ShareFetch request v{version} BatchSize leftover-empty"
            );
        }

        let mut v0_with = BytesMut::new();
        encode_share_fetch_request_with_batch_size(
            &mut v0_with,
            0,
            "sg",
            "m1",
            0,
            10,
            1,
            1024,
            16,
            &topics,
            3_600_000,
        )
        .unwrap();
        let mut v0_zero = BytesMut::new();
        encode_share_fetch_request_with_batch_size(
            &mut v0_zero,
            0,
            "sg",
            "m1",
            0,
            10,
            1,
            1024,
            16,
            &topics,
            0,
        )
        .unwrap();
        assert_eq!(
            &v0_with[..],
            &v0_zero[..],
            "v0 omits BatchSize even when the body has a non-zero value"
        );
        let mut conv_v0 = BytesMut::new();
        encode_share_fetch_request(&mut conv_v0, 0, "sg", "m1", 0, 10, 1, 1024, 16, &topics)
            .unwrap();
        assert_eq!(
            &conv_v0[..],
            &v0_zero[..],
            "encode_share_fetch_request v0 has no BatchSize"
        );

        let mut with = BytesMut::new();
        encode_share_fetch_request_with_batch_size(
            &mut with, 1, "sg", "m1", 0, 10, 1, 1024, 16, &topics, 3_600_000,
        )
        .unwrap();
        let mut as_max = BytesMut::new();
        encode_share_fetch_request_with_batch_size(
            &mut as_max,
            1,
            "sg",
            "m1",
            0,
            10,
            1,
            1024,
            16,
            &topics,
            16,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &as_max[..],
            "v1 BatchSize is not always copied from MaxRecords"
        );
        let mut conv = BytesMut::new();
        encode_share_fetch_request(&mut conv, 1, "sg", "m1", 0, 10, 1, 1024, 16, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &as_max[..],
            "encode_share_fetch_request still writes BatchSize as MaxRecords"
        );
        let mut forgotten_copy = BytesMut::new();
        encode_share_fetch_request_with_forgotten(
            &mut forgotten_copy,
            1,
            "sg",
            "m1",
            0,
            10,
            1,
            1024,
            16,
            &topics,
            &[],
        )
        .unwrap();
        assert_eq!(
            &forgotten_copy[..],
            &as_max[..],
            "encode_share_fetch_request_with_forgotten still writes BatchSize as MaxRecords"
        );
        assert_ne!(
            &v0_with[..],
            &with[..],
            "v1 adds MaxRecords and BatchSize after MaxBytes"
        );
    }

    #[test]
    fn share_fetch_request_max_wait_ms_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchRequest.json MaxWaitMs is versions
        // 0+ (INT32 after ShareSessionEpoch). Official Java
        // ShareFetchRequest.maxWait reads it. Encode already takes
        // max_wait_ms; decode previously discarded it. This crate speaks
        // 0–1. This is not MinBytes / MaxBytes / BatchSize / Fetch
        // MaxWaitMs.
        let topics = vec![ShareFetchTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchPartition {
                partition: 0,
                partition_max_bytes: 0,
                acknowledgements: vec![],
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_request(
                &mut buf, version, "sg", "m1", 0, 3_600_000, 1, 1024, 16, &topics,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (gid, mid, epoch, max_records, got, forgotten, batch_size, max_wait, ..) =
                decode_share_fetch_request(&mut cur, version).unwrap();
            assert_eq!(gid.as_str(), "sg");
            assert_eq!(mid.as_str(), "m1");
            assert_eq!(epoch, 0);
            assert!(forgotten.is_empty());
            assert_eq!(got, topics);
            assert_eq!(max_wait, 3_600_000);
            if version >= 1 {
                assert_eq!(max_records, 16);
                assert_eq!(batch_size, 16);
            } else {
                assert_eq!(max_records, 0);
                assert_eq!(batch_size, 0);
            }
            assert!(
                cur.is_empty(),
                "ShareFetch request v{version} MaxWaitMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_request(&mut with, 0, "sg", "m1", 0, 3_600_000, 1, 1024, 16, &topics)
            .unwrap();
        let mut ten = BytesMut::new();
        encode_share_fetch_request(&mut ten, 0, "sg", "m1", 0, 10, 1, 1024, 16, &topics).unwrap();
        assert_ne!(&with[..], &ten[..], "v0 MaxWaitMs is not always 10");
        let mut cur = ten.as_ref();
        let (.., max_wait, _, _) = decode_share_fetch_request(&mut cur, 0).unwrap();
        assert_eq!(max_wait, 10);

        let mut v1_with = BytesMut::new();
        encode_share_fetch_request(
            &mut v1_with,
            1,
            "sg",
            "m1",
            0,
            3_600_000,
            1,
            1024,
            16,
            &topics,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 both write MaxWaitMs (JSON 0+); v1 still adds MaxRecords / BatchSize"
        );
    }

    #[test]
    fn share_fetch_request_min_bytes_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchRequest.json MinBytes is versions
        // 0+ (INT32 after MaxWaitMs). Official Java
        // ShareFetchRequest.minBytes reads it. Encode already takes
        // min_bytes; decode previously discarded it. This crate speaks
        // 0–1. This is not MaxBytes / MaxWaitMs / BatchSize / Fetch
        // MinBytes.
        let topics = vec![ShareFetchTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchPartition {
                partition: 0,
                partition_max_bytes: 0,
                acknowledgements: vec![],
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_request(
                &mut buf, version, "sg", "m1", 0, 10, 3_600_000, 1024, 16, &topics,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (gid, mid, epoch, max_records, got, forgotten, batch_size, max_wait, min_bytes, ..) =
                decode_share_fetch_request(&mut cur, version).unwrap();
            assert_eq!(gid.as_str(), "sg");
            assert_eq!(mid.as_str(), "m1");
            assert_eq!(epoch, 0);
            assert!(forgotten.is_empty());
            assert_eq!(got, topics);
            assert_eq!(max_wait, 10);
            assert_eq!(min_bytes, 3_600_000);
            if version >= 1 {
                assert_eq!(max_records, 16);
                assert_eq!(batch_size, 16);
            } else {
                assert_eq!(max_records, 0);
                assert_eq!(batch_size, 0);
            }
            assert!(
                cur.is_empty(),
                "ShareFetch request v{version} MinBytes leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_request(
            &mut with, 0, "sg", "m1", 0, 10, 3_600_000, 1024, 16, &topics,
        )
        .unwrap();
        let mut one = BytesMut::new();
        encode_share_fetch_request(&mut one, 0, "sg", "m1", 0, 10, 1, 1024, 16, &topics).unwrap();
        assert_ne!(&with[..], &one[..], "v0 MinBytes is not always 1");
        let mut cur = one.as_ref();
        let (.., min_bytes, _) = decode_share_fetch_request(&mut cur, 0).unwrap();
        assert_eq!(min_bytes, 1);

        let mut v1_with = BytesMut::new();
        encode_share_fetch_request(
            &mut v1_with,
            1,
            "sg",
            "m1",
            0,
            10,
            3_600_000,
            1024,
            16,
            &topics,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 both write MinBytes (JSON 0+); v1 still adds MaxRecords / BatchSize"
        );
    }

    #[test]
    fn share_fetch_request_max_bytes_matches_java() {
        // Kafka 4.0.0 / 4.1 ShareFetchRequest.json MaxBytes is versions
        // 0+ (INT32 after MinBytes; JSON default 0x7fffffff). Official
        // Java ShareFetchRequest.maxBytes reads it. Encode already takes
        // max_bytes; decode previously discarded it. This crate speaks
        // 0–1. This is not MinBytes / MaxWaitMs / BatchSize /
        // PartitionMaxBytes / Fetch MaxBytes.
        let topics = vec![ShareFetchTopic {
            topic_id: [7u8; 16],
            partitions: vec![ShareFetchPartition {
                partition: 0,
                partition_max_bytes: 0,
                acknowledgements: vec![],
            }],
        }];
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_request(
                &mut buf, version, "sg", "m1", 0, 10, 1, 3_600_000, 16, &topics,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (
                gid,
                mid,
                epoch,
                max_records,
                got,
                forgotten,
                batch_size,
                max_wait,
                min_bytes,
                max_bytes,
            ) = decode_share_fetch_request(&mut cur, version).unwrap();
            assert_eq!(gid.as_str(), "sg");
            assert_eq!(mid.as_str(), "m1");
            assert_eq!(epoch, 0);
            assert!(forgotten.is_empty());
            assert_eq!(got, topics);
            assert_eq!(max_wait, 10);
            assert_eq!(min_bytes, 1);
            assert_eq!(max_bytes, 3_600_000);
            if version >= 1 {
                assert_eq!(max_records, 16);
                assert_eq!(batch_size, 16);
            } else {
                assert_eq!(max_records, 0);
                assert_eq!(batch_size, 0);
            }
            assert!(
                cur.is_empty(),
                "ShareFetch request v{version} MaxBytes leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_share_fetch_request(&mut with, 0, "sg", "m1", 0, 10, 1, 3_600_000, 16, &topics)
            .unwrap();
        let mut kilo = BytesMut::new();
        encode_share_fetch_request(&mut kilo, 0, "sg", "m1", 0, 10, 1, 1024, 16, &topics).unwrap();
        assert_ne!(&with[..], &kilo[..], "v0 MaxBytes is not always 1024");
        let mut cur = kilo.as_ref();
        let (.., max_bytes) = decode_share_fetch_request(&mut cur, 0).unwrap();
        assert_eq!(max_bytes, 1024);
        let mut json_default = BytesMut::new();
        encode_share_fetch_request(
            &mut json_default,
            0,
            "sg",
            "m1",
            0,
            10,
            1,
            i32::MAX,
            16,
            &topics,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &json_default[..],
            "v0 MaxBytes is not always JSON default 0x7fffffff"
        );
        let mut cur = json_default.as_ref();
        let (.., max_bytes) = decode_share_fetch_request(&mut cur, 0).unwrap();
        assert_eq!(max_bytes, i32::MAX);

        let mut v1_with = BytesMut::new();
        encode_share_fetch_request(
            &mut v1_with,
            1,
            "sg",
            "m1",
            0,
            10,
            1,
            3_600_000,
            16,
            &topics,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &v1_with[..],
            "v0 and v1 both write MaxBytes (JSON 0+); v1 still adds MaxRecords / BatchSize"
        );
    }

    #[test]
    fn share_fetch_request_update_forgotten_data_matches_java() {
        // Java ShareFetchRequest.Builder.updateForgottenData: HashMap by
        // topic id; partitions append (duplicates kept). Grouped entries
        // are appended to ForgottenTopicsData (same id is a second list
        // entry, not a merge). encode_share_fetch_request still writes empty.
        let none: Vec<([u8; 16], i32)> = Vec::new();
        assert!(ShareFetchRequest::update_forgotten_data(&[], none.clone()).is_empty());

        let id_a = [1u8; 16];
        let id_b = [2u8; 16];
        let forget = [(id_a, 0i32), (id_b, 1), (id_a, 2), (id_a, 0)];
        let grouped = ShareFetchRequest::update_forgotten_data(&[], forget);
        assert_eq!(
            grouped,
            vec![
                ShareForgottenTopic {
                    topic_id: id_a,
                    partitions: vec![0, 2, 0],
                },
                ShareForgottenTopic {
                    topic_id: id_b,
                    partitions: vec![1],
                },
            ]
        );

        let existing = [ShareForgottenTopic {
            topic_id: id_a,
            partitions: vec![9],
        }];
        let appended = ShareFetchRequest::update_forgotten_data(&existing, forget);
        assert_eq!(
            appended,
            vec![
                ShareForgottenTopic {
                    topic_id: id_a,
                    partitions: vec![9],
                },
                ShareForgottenTopic {
                    topic_id: id_a,
                    partitions: vec![0, 2, 0],
                },
                ShareForgottenTopic {
                    topic_id: id_b,
                    partitions: vec![1],
                },
            ],
            "a second call with the same id appends, it does not merge"
        );

        let fetch = vec![ShareFetchTopic {
            topic_id: id_a,
            partitions: vec![ShareFetchPartition {
                partition: 0,
                partition_max_bytes: 1024,
                acknowledgements: vec![],
            }],
        }];
        for version in [0_i16, 1] {
            let got = ShareFetchRequest::update_forgotten_data(&[], forget);
            assert_eq!(got.len(), 2);
            let mut buf = BytesMut::new();
            encode_share_fetch_request(&mut buf, version, "sg", "m1", 0, 10, 1, 1024, 16, &fetch)
                .unwrap();
            let mut cur = buf.as_ref();
            let (_gid, _mid, _epoch, _max, _decoded, ..) =
                decode_share_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} updateForgottenData leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [0_i16, 1] {
            let got = ShareFetchRequest::update_forgotten_data(&[], none.clone());
            assert!(got.is_empty());
            let mut buf = BytesMut::new();
            encode_share_fetch_request(&mut buf, version, "sg", "m1", 0, 10, 1, 1024, 16, &fetch)
                .unwrap();
            let mut cur = buf.as_ref();
            let (_gid, _mid, _epoch, _max, _decoded, ..) =
                decode_share_fetch_request(&mut cur, version).unwrap();
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} updateForgottenData empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn share_fetch_request_for_consumer_matches_java() {
        // Java ShareFetchRequest.Builder.forConsumer: HashMap by topicId,
        // inner HashMap by partitionIndex. Send last-wins the partition
        // body. Acks replace batches on an existing partition and keep
        // PartitionMaxBytes. Closing skips send; ack-only max bytes is 0.
        // Empty is empty Topics. Intervening ids still merge. Topic name
        // is not used.
        assert!(ShareFetchRequest::for_consumer(
            false,
            1024,
            std::iter::empty::<([u8; 16], i32)>(),
            std::iter::empty::<([u8; 16], i32, Vec<AcknowledgementBatch>)>(),
        )
        .is_empty());
        let a = [1u8; 16];
        let b = [2u8; 16];
        let b0 = AcknowledgementBatch {
            first_offset: 0,
            last_offset: 1,
            types: vec![ACK_ACCEPT],
        };
        let b1 = AcknowledgementBatch {
            first_offset: 2,
            last_offset: 3,
            types: vec![ACK_RELEASE],
        };
        let grouped = ShareFetchRequest::for_consumer(
            false,
            1024,
            [(a, 0), (b, 1), (a, 2)],
            std::iter::empty::<([u8; 16], i32, Vec<AcknowledgementBatch>)>(),
        );
        assert_eq!(
            grouped,
            vec![
                ShareFetchTopic {
                    topic_id: a,
                    partitions: vec![
                        ShareFetchPartition {
                            partition: 0,
                            partition_max_bytes: 1024,
                            acknowledgements: vec![],
                        },
                        ShareFetchPartition {
                            partition: 2,
                            partition_max_bytes: 1024,
                            acknowledgements: vec![],
                        },
                    ],
                },
                ShareFetchTopic {
                    topic_id: b,
                    partitions: vec![ShareFetchPartition {
                        partition: 1,
                        partition_max_bytes: 1024,
                        acknowledgements: vec![],
                    }],
                },
            ]
        );
        let last_wins = ShareFetchRequest::for_consumer(
            false,
            2048,
            [(a, 0), (a, 0)],
            std::iter::empty::<([u8; 16], i32, Vec<AcknowledgementBatch>)>(),
        );
        assert_eq!(
            last_wins,
            vec![ShareFetchTopic {
                topic_id: a,
                partitions: vec![ShareFetchPartition {
                    partition: 0,
                    partition_max_bytes: 2048,
                    acknowledgements: vec![],
                }],
            }]
        );
        let with_acks = ShareFetchRequest::for_consumer(
            false,
            1024,
            [(a, 0)],
            [(a, 0, vec![b0.clone()]), (a, 1, vec![b1.clone()])],
        );
        assert_eq!(
            with_acks,
            vec![ShareFetchTopic {
                topic_id: a,
                partitions: vec![
                    ShareFetchPartition {
                        partition: 0,
                        partition_max_bytes: 1024,
                        acknowledgements: vec![b0.clone()],
                    },
                    ShareFetchPartition {
                        partition: 1,
                        partition_max_bytes: 1024,
                        acknowledgements: vec![b1.clone()],
                    },
                ],
            }]
        );
        let ack_last_wins = ShareFetchRequest::for_consumer(
            false,
            1024,
            [(a, 0)],
            [(a, 0, vec![b0.clone()]), (a, 0, vec![b1.clone()])],
        );
        assert_eq!(
            ack_last_wins
                .first()
                .and_then(|topic| topic.partitions.first())
                .map(|part| part.acknowledgements.as_slice()),
            Some(std::slice::from_ref(&b1))
        );
        let closing = ShareFetchRequest::for_consumer(
            true,
            1024,
            [(a, 0), (b, 1)],
            [(a, 0, vec![b0.clone()])],
        );
        assert_eq!(
            closing,
            vec![ShareFetchTopic {
                topic_id: a,
                partitions: vec![ShareFetchPartition {
                    partition: 0,
                    partition_max_bytes: 0,
                    acknowledgements: vec![b0],
                }],
            }],
            "closing skips send; ack-only PartitionMaxBytes is 0"
        );
        leftover_share_fetch_for_consumer(0, &grouped);
        leftover_share_fetch_for_consumer(0, &[]);
        leftover_share_fetch_for_consumer(1, &grouped);
        leftover_share_fetch_for_consumer(1, &[]);
    }

    fn leftover_share_fetch_for_consumer(version: i16, topics: &[ShareFetchTopic]) {
        let mut buf = BytesMut::new();
        encode_share_fetch_request(&mut buf, version, "g", "m", 1, 10, 1, 1024, 16, topics)
            .unwrap();
        let mut cur = buf.as_ref();
        let (gid, mid, epoch, _max, decoded, ..) =
            decode_share_fetch_request(&mut cur, version).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(mid, "m");
        assert_eq!(epoch, 1);
        if version == 0 {
            assert_eq!(decoded.as_slice(), topics);
        } else {
            let zeroed: Vec<ShareFetchTopic> = topics
                .iter()
                .map(|topic| ShareFetchTopic {
                    topic_id: topic.topic_id,
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|part| ShareFetchPartition {
                            partition: part.partition,
                            partition_max_bytes: 0,
                            acknowledgements: part.acknowledgements.clone(),
                        })
                        .collect(),
                })
                .collect();
            assert_eq!(decoded, zeroed);
        }
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            !cur.has_remaining(),
            "ShareFetch v{version} Builder.forConsumer {empty}leftover-empty; leftover {} bytes",
            cur.remaining()
        );
    }

    #[test]
    fn share_fetch_request_share_fetch_data_matches_java() {
        // Java ShareFetchRequest.shareFetchData: LinkedHashMap by
        // TopicIdPartition. Missing name is still inserted (null). Last
        // partition overwrites. Values are PartitionMaxBytes.
        assert!(ShareFetchRequest::share_fetch_data(&[], &HashMap::new()).is_empty());
        let a = [1u8; 16];
        let b = [2u8; 16];
        let topics = vec![
            ShareFetchTopic {
                topic_id: a,
                partitions: vec![
                    ShareFetchPartition {
                        partition: 0,
                        partition_max_bytes: 1024,
                        acknowledgements: vec![],
                    },
                    ShareFetchPartition {
                        partition: 0,
                        partition_max_bytes: 2048,
                        acknowledgements: vec![],
                    },
                    ShareFetchPartition {
                        partition: 1,
                        partition_max_bytes: 512,
                        acknowledgements: vec![],
                    },
                ],
            },
            ShareFetchTopic {
                topic_id: b,
                partitions: vec![ShareFetchPartition {
                    partition: 2,
                    partition_max_bytes: 256,
                    acknowledgements: vec![],
                }],
            },
        ];
        let empty_names = HashMap::new();
        let unresolved = ShareFetchRequest::share_fetch_data(&topics, &empty_names);
        assert_eq!(
            unresolved,
            HashMap::from([
                ((a, None, 0), 2048),
                ((a, None, 1), 512),
                ((b, None, 2), 256),
            ]),
            "missing name is still inserted; later partition overwrites"
        );
        let names = HashMap::from([(a, "t".into()), (b, "u".into())]);
        assert_eq!(
            ShareFetchRequest::share_fetch_data(&topics, &names),
            HashMap::from([
                ((a, Some("t".into()), 0), 2048),
                ((a, Some("t".into()), 1), 512),
                ((b, Some("u".into()), 2), 256),
            ])
        );
        leftover_share_fetch_share_fetch_data(0, &topics);
        leftover_share_fetch_share_fetch_data(0, &[]);
        leftover_share_fetch_share_fetch_data(1, &topics);
        leftover_share_fetch_share_fetch_data(1, &[]);
    }

    fn leftover_share_fetch_share_fetch_data(version: i16, topics: &[ShareFetchTopic]) {
        let mut buf = BytesMut::new();
        encode_share_fetch_request(&mut buf, version, "g", "m", 1, 10, 1, 1024, 16, topics)
            .unwrap();
        let mut cur = buf.as_ref();
        let (_gid, _mid, _epoch, _max, decoded, ..) =
            decode_share_fetch_request(&mut cur, version).unwrap();
        let names = HashMap::new();
        let _got = ShareFetchRequest::share_fetch_data(&decoded, &names);
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            !cur.has_remaining(),
            "ShareFetch v{version} shareFetchData {empty}leftover-empty; leftover {} bytes",
            cur.remaining()
        );
    }

    #[test]
    fn share_fetch_response_to_message_matches_java() {
        // Java ShareFetchResponse.toMessage: LinkedHashMap by topicId,
        // first-seen order, setPartitionIndex from the key. Non-adjacent
        // same topicId still merges.
        assert!(ShareFetchResponse::to_message(&[]).is_empty());
        let a = [1u8; 16];
        let b = [2u8; 16];
        let body0 = ShareFetchedPartition::partition_response(99, 0);
        let body1 = ShareFetchedPartition::partition_response(1, crate::error::UNKNOWN_TOPIC_ID);
        let body2 = ShareFetchedPartition::partition_response(2, 0);
        let grouped = ShareFetchResponse::to_message(&[
            (a, 0, body0.clone()),
            (b, 1, body1.clone()),
            (a, 3, body2.clone()),
        ]);
        assert_eq!(
            grouped,
            vec![
                ShareFetchedTopic {
                    topic_id: a,
                    partitions: vec![
                        ShareFetchedPartition {
                            partition: 0,
                            error_code: 0,
                            error_message: None,
                            acknowledge_error_code: 0,
                            acknowledge_error_message: None,
                            current_leader_id: 0,
                            current_leader_epoch: 0,
                            records: Vec::new(),
                            acquired: Vec::new(),
                        },
                        ShareFetchedPartition {
                            partition: 3,
                            error_code: 0,
                            error_message: None,
                            acknowledge_error_code: 0,
                            acknowledge_error_message: None,
                            current_leader_id: 0,
                            current_leader_epoch: 0,
                            records: Vec::new(),
                            acquired: Vec::new(),
                        },
                    ],
                },
                ShareFetchedTopic {
                    topic_id: b,
                    partitions: vec![ShareFetchedPartition {
                        partition: 1,
                        error_code: crate::error::UNKNOWN_TOPIC_ID,
                        error_message: None,
                        acknowledge_error_code: 0,
                        acknowledge_error_message: None,
                        current_leader_id: 0,
                        current_leader_epoch: 0,
                        records: Vec::new(),
                        acquired: Vec::new(),
                    }],
                },
            ]
        );
        assert_eq!(
            grouped
                .first()
                .and_then(|topic| topic.partitions.first())
                .map(|part| part.partition),
            Some(0),
            "setPartitionIndex copies the key partition onto the body"
        );
        assert_eq!(body0.partition, 99);
        for version in [0i16, 1] {
            let mut buf = BytesMut::new();
            encode_share_fetch_response(&mut buf, version, &grouped).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_share_fetch_response(&mut cur, version).unwrap();
            assert_eq!(decoded, grouped);
            assert!(
                !cur.has_remaining(),
                "ShareFetch v{version} toMessage leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }
}
