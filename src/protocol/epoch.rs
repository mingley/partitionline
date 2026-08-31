//! OffsetForLeaderEpoch (api key 23). Classic v0–v3; flexible v4.

use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use super::records::RecordBatch;
use crate::error::{Error, Result};

/// Java `OffsetsForLeaderEpochRequest.CONSUMER_REPLICA_ID`. ReplicaId is
/// request-level (v3+).
pub const CONSUMER_REPLICA_ID: i32 = -1;
/// Java `OffsetsForLeaderEpochRequest.DEBUGGING_REPLICA_ID`.
pub const DEBUGGING_REPLICA_ID: i32 = -2;

/// Java `OffsetsForLeaderEpochRequest.supportsTopicPermission`.
///
/// OffsetForLeaderEpoch v3+ needs topic Describe instead of Cluster
/// permission. Java `Builder.forConsumer` therefore negotiates from v3.
#[must_use]
pub const fn supports_topic_permission(latest_usable_version: i16) -> bool {
    latest_usable_version >= 3
}

/// Check that OffsetForLeaderEpoch `version` is spoken (0–4).
///
/// v0–v1 have no ReplicaId and no CurrentLeaderEpoch. v1 response adds
/// LeaderEpoch. v2 adds CurrentLeaderEpoch and response ThrottleTimeMs.
/// v3 adds ReplicaId ([`CONSUMER_REPLICA_ID`] for a consumer). v4 is flexible (compact
/// strings/arrays plus tagged fields; request header 2, response header
/// 1). Kafka 4.0 `validVersions` is `2-4` (v0–v1 removed). This crate
/// speaks 0–4. v5+ is not spoken.
fn offset_for_leader_epoch_spoken(version: i16) -> Result<i16> {
    match version {
        0..=4 => Ok(version),
        other => Err(Error::protocol(format!(
            "OffsetForLeaderEpoch version {other} is not implemented"
        ))),
    }
}

fn offset_for_leader_epoch_flexible(version: i16) -> Result<bool> {
    Ok(offset_for_leader_epoch_spoken(version)? >= 4)
}

/// One partition in an OffsetForLeaderEpoch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderPartition {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch (v2+). Decode below v2 fills
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub current_leader_epoch: i32,
    /// Epoch to look up an end offset for.
    pub leader_epoch: i32,
}

impl OffsetForLeaderPartition {
    /// Partition `partition` at `current_leader_epoch` / `leader_epoch`.
    #[must_use]
    pub fn new(partition: i32, current_leader_epoch: i32, leader_epoch: i32) -> Self {
        Self {
            partition,
            current_leader_epoch,
            leader_epoch,
        }
    }
}

/// One topic in an OffsetForLeaderEpoch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions in this topic.
    pub partitions: Vec<OffsetForLeaderPartition>,
}

impl OffsetForLeaderTopic {
    /// Topic `topic` with these partition queries.
    #[must_use]
    pub fn new(topic: impl Into<String>, partitions: Vec<OffsetForLeaderPartition>) -> Self {
        Self {
            topic: topic.into(),
            partitions,
        }
    }

    /// Java `OffsetsForLeaderEpochRequest.getErrorResponse` one topic.
    ///
    /// Each partition is [`EpochEndOffset::error`]. Throttle on the response
    /// is the JSON default (`0`; Java does not set `ThrottleTimeMs`).
    #[must_use]
    pub fn error_result(&self, error_code: i16) -> OffsetForLeaderTopicResult {
        OffsetForLeaderTopicResult::new(
            self.topic.as_str(),
            self.partitions
                .iter()
                .map(|p| EpochEndOffset::error(error_code, p.partition))
                .collect(),
        )
    }
}

/// One partition in an OffsetForLeaderEpoch response (`EpochEndOffset`).
///
/// [`Self::UNDEFINED_EPOCH`] / [`Self::UNDEFINED_EPOCH_OFFSET`] are Java
/// `OffsetsForLeaderEpochResponse` sentinels
/// (`RecordBatch.NO_PARTITION_LEADER_EPOCH`).
/// [`Self::error`] is Java `OffsetsForLeaderEpochRequest.getErrorResponse`
/// partition body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochEndOffset {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Partition index.
    pub partition: i32,
    /// Leader epoch (v1+), or [`Self::UNDEFINED_EPOCH`].
    pub leader_epoch: i32,
    /// End offset of the epoch, or [`Self::UNDEFINED_EPOCH_OFFSET`].
    pub end_offset: i64,
}

impl EpochEndOffset {
    /// Java `OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH`.
    pub const UNDEFINED_EPOCH: i32 = RecordBatch::NO_PARTITION_LEADER_EPOCH;
    /// Java `OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET`.
    pub const UNDEFINED_EPOCH_OFFSET: i64 = RecordBatch::NO_PARTITION_LEADER_EPOCH as i64;

    /// Partition `partition` with this epoch end.
    #[must_use]
    pub fn new(error_code: i16, partition: i32, leader_epoch: i32, end_offset: i64) -> Self {
        Self {
            error_code,
            partition,
            leader_epoch,
            end_offset,
        }
    }

    /// Java `OffsetsForLeaderEpochRequest.getErrorResponse` partition body.
    ///
    /// Fills [`Self::UNDEFINED_EPOCH`] / [`Self::UNDEFINED_EPOCH_OFFSET`].
    #[must_use]
    pub fn error(error_code: i16, partition: i32) -> Self {
        Self::new(
            error_code,
            partition,
            Self::UNDEFINED_EPOCH,
            Self::UNDEFINED_EPOCH_OFFSET,
        )
    }
}

/// One topic in an OffsetForLeaderEpoch response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderTopicResult {
    /// Topic name.
    pub topic: String,
    /// Partition results in request order.
    pub partitions: Vec<EpochEndOffset>,
}

impl OffsetForLeaderTopicResult {
    /// Topic `topic` with these partition results.
    #[must_use]
    pub fn new(topic: impl Into<String>, partitions: Vec<EpochEndOffset>) -> Self {
        Self {
            topic: topic.into(),
            partitions,
        }
    }
}

/// Java `OffsetsForLeaderEpochResponse` helpers.
pub struct OffsetsForLeaderEpochResponse;

impl OffsetsForLeaderEpochResponse {
    /// Java `OffsetsForLeaderEpochResponse.errorCounts`.
    ///
    /// Counts partition-level error codes (including `NONE`).
    #[must_use]
    pub fn error_counts(topics: &[OffsetForLeaderTopicResult]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        for topic in topics {
            for partition in &topic.partitions {
                let count = counts.entry(partition.error_code).or_insert(0);
                *count += 1;
            }
        }
        counts
    }
}

/// Encode a single-topic, single-partition OffsetForLeaderEpoch request.
///
/// [`CONSUMER_REPLICA_ID`] is written on v3+. `current_leader_epoch`
/// is written on v2+.
pub fn encode_offset_for_leader_epoch_request(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> crate::error::Result<()> {
    encode_offset_for_leader_epoch_topics_request(
        buf,
        version,
        &[OffsetForLeaderTopic::new(
            topic,
            vec![OffsetForLeaderPartition::new(
                partition,
                current_leader_epoch,
                leader_epoch,
            )],
        )],
    )
}

/// Encode OffsetForLeaderEpoch with one or more topics (v0–v3 classic, v4
/// flexible). [`CONSUMER_REPLICA_ID`] is written on v3+.
pub fn encode_offset_for_leader_epoch_topics_request(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetForLeaderTopic],
) -> crate::error::Result<()> {
    encode_offset_for_leader_epoch_topics_request_with_replica_id(
        buf,
        version,
        topics,
        CONSUMER_REPLICA_ID,
    )
}

/// Encode OffsetForLeaderEpoch with ReplicaId.
///
/// ReplicaId is JSON `3+` (INT32 first field; default `-2`). Official Java
/// `OffsetsForLeaderEpochRequest.replicaId()` /
/// `OffsetForLeaderEpochRequestData.replicaId`. Below v3 the field is
/// omitted even when `replica_id` is not [`DEBUGGING_REPLICA_ID`]. Decode
/// fills [`DEBUGGING_REPLICA_ID`]. [`encode_offset_for_leader_epoch_topics_request`]
/// still writes [`CONSUMER_REPLICA_ID`] on v3+. This is not Fetch ReplicaId
/// / ListOffsets ReplicaId.
pub fn encode_offset_for_leader_epoch_topics_request_with_replica_id(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetForLeaderTopic],
    replica_id: i32,
) -> crate::error::Result<()> {
    let flexible = offset_for_leader_epoch_flexible(version)?;
    if version >= 3 {
        buf.put_i32(replica_id);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            if version >= 2 {
                buf.put_i32(p.current_leader_epoch);
            }
            buf.put_i32(p.leader_epoch);
            if flexible {
                buf::put_empty_tagged_fields(buf); // partition
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf); // topic
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf); // top-level
    }
    Ok(())
}

/// Decode a single-topic, single-partition OffsetForLeaderEpoch request.
///
/// Returns `(topic, partition, current_leader_epoch, leader_epoch)`.
/// `current_leader_epoch` is [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]
/// below v2. Empty Topics/Partitions is a
/// protocol error.
pub fn decode_offset_for_leader_epoch_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, i32, i32, i32)> {
    let (topics, ..) = decode_offset_for_leader_epoch_topics_request(buf, version)?;
    let t = topics
        .first()
        .ok_or_else(|| Error::protocol("OffsetForLeaderEpoch Topics is empty".to_string()))?;
    let p = t
        .partitions
        .first()
        .ok_or_else(|| Error::protocol("OffsetForLeaderEpoch Partitions is empty".to_string()))?;
    Ok((
        t.topic.clone(),
        p.partition,
        p.current_leader_epoch,
        p.leader_epoch,
    ))
}

/// Decode OffsetForLeaderEpoch Topics of N (v0–v4).
///
/// Returns `(topics, replica_id)`. ReplicaId is JSON `3+` (INT32 first
/// field; official Java `OffsetsForLeaderEpochRequest.replicaId()`). Below
/// v3 decode fills [`DEBUGGING_REPLICA_ID`] (JSON default `-2`).
pub fn decode_offset_for_leader_epoch_topics_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<OffsetForLeaderTopic>, i32)> {
    let flexible = offset_for_leader_epoch_flexible(version)?;
    let replica_id = if version >= 3 {
        buf::get_i32(buf)?
    } else {
        DEBUGGING_REPLICA_ID
    };
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let current_leader_epoch = if version >= 2 {
                buf::get_i32(buf)?
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
            let leader_epoch = buf::get_i32(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?; // partition
            }
            partitions.push(OffsetForLeaderPartition::new(
                partition,
                current_leader_epoch,
                leader_epoch,
            ));
        }
        if flexible {
            buf::skip_tagged_fields(buf)?; // topic
        }
        topics.push(OffsetForLeaderTopic::new(topic, partitions));
    }
    if flexible {
        buf::skip_tagged_fields(buf)?; // top-level
    }
    Ok((topics, replica_id))
}

/// Encode a single-topic, single-partition OffsetForLeaderEpoch response.
///
/// Throttle is `0` on v2+. `leader_epoch` is written on v1+.
pub fn encode_offset_for_leader_epoch_response(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    error_code: i16,
    leader_epoch: i32,
    end_offset: i64,
) -> crate::error::Result<()> {
    encode_offset_for_leader_epoch_topics_response(
        buf,
        version,
        &[OffsetForLeaderTopicResult::new(
            topic,
            vec![EpochEndOffset::new(
                error_code,
                partition,
                leader_epoch,
                end_offset,
            )],
        )],
    )
}

/// Encode OffsetForLeaderEpoch with one or more topic results.
///
/// ThrottleTimeMs is the JSON default (`0`) on v2+ (JSON `2+`).
/// `leader_epoch` is written on v1+.
pub fn encode_offset_for_leader_epoch_topics_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetForLeaderTopicResult],
) -> crate::error::Result<()> {
    encode_offset_for_leader_epoch_topics_response_with_throttle(buf, version, topics, 0)
}

/// Encode OffsetForLeaderEpoch v0–v4 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `2+`: written on v2–v4. Below v2 it is omitted
/// even when the body has a non-zero value; decode fills `0`. v0–v3 are
/// classic. v4 is flexible. v1 adds LeaderEpoch on partitions. v3 ReplicaId
/// is on the request. Kafka 4.0 `validVersions` is `2-4` (v0–v1 removed).
/// This crate speaks 0–4. v5+ is not spoken. Official Java
/// `getErrorResponse` does not set `throttleTimeMs` (JSON default `0`).
/// There is no top-level ErrorCode.
pub fn encode_offset_for_leader_epoch_topics_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetForLeaderTopicResult],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = offset_for_leader_epoch_flexible(version)?;
    if version >= 2 {
        buf.put_i32(throttle_time_ms);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i16(p.error_code);
            buf.put_i32(p.partition);
            if version >= 1 {
                buf.put_i32(p.leader_epoch);
            }
            buf.put_i64(p.end_offset);
            if flexible {
                buf::put_empty_tagged_fields(buf); // partition
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf); // topic
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf); // top-level
    }
    Ok(())
}

/// Decode a single-topic, single-partition OffsetForLeaderEpoch response.
///
/// Returns `(error_code, leader_epoch, end_offset)`. `leader_epoch` is
/// [`EpochEndOffset::UNDEFINED_EPOCH`] below v1. Empty Topics/Partitions is a
/// protocol error.
pub fn decode_offset_for_leader_epoch_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i32, i64)> {
    let (topics, ..) = decode_offset_for_leader_epoch_topics_response(buf, version)?;
    let t = topics
        .first()
        .ok_or_else(|| Error::protocol("OffsetForLeaderEpoch Topics is empty"))?;
    let p = t
        .partitions
        .first()
        .ok_or_else(|| Error::protocol("OffsetForLeaderEpoch Partitions is empty"))?;
    Ok((p.error_code, p.leader_epoch, p.end_offset))
}

/// Decode OffsetForLeaderEpoch Topics of N (v0–v4).
///
/// Returns `(topics, throttle_time_ms)`. Below v2 ThrottleTimeMs is
/// omitted; decode fills `0`. There is no top-level ErrorCode.
pub fn decode_offset_for_leader_epoch_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<OffsetForLeaderTopicResult>, i32)> {
    let flexible = offset_for_leader_epoch_flexible(version)?;
    let throttle_time_ms = if version >= 2 { buf::get_i32(buf)? } else { 0 };
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let error_code = buf::get_i16(buf)?;
            let partition = buf::get_i32(buf)?;
            let leader_epoch = if version >= 1 {
                buf::get_i32(buf)?
            } else {
                EpochEndOffset::UNDEFINED_EPOCH
            };
            let end_offset = buf::get_i64(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?; // partition
            }
            partitions.push(EpochEndOffset::new(
                error_code,
                partition,
                leader_epoch,
                end_offset,
            ));
        }
        if flexible {
            buf::skip_tagged_fields(buf)?; // topic
        }
        topics.push(OffsetForLeaderTopicResult::new(topic, partitions));
    }
    if flexible {
        buf::skip_tagged_fields(buf)?; // top-level
    }
    Ok((topics, throttle_time_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use std::collections::HashMap;

    #[test]
    fn epoch_end_offset_undefined_sentinels_match_java() {
        assert_eq!(
            EpochEndOffset::UNDEFINED_EPOCH,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(
            EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
            i64::from(RecordBatch::NO_PARTITION_LEADER_EPOCH)
        );
        assert_eq!(EpochEndOffset::UNDEFINED_EPOCH, -1);
        assert_eq!(EpochEndOffset::UNDEFINED_EPOCH_OFFSET, -1);
        let err = EpochEndOffset::error(crate::error::UNKNOWN_TOPIC_OR_PARTITION, 3);
        assert_eq!(err.error_code, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(err.partition, 3);
        assert_eq!(err.leader_epoch, EpochEndOffset::UNDEFINED_EPOCH);
        assert_eq!(err.end_offset, EpochEndOffset::UNDEFINED_EPOCH_OFFSET);
        let topic = OffsetForLeaderTopic::new(
            "t",
            vec![
                OffsetForLeaderPartition::new(0, 1, 2),
                OffsetForLeaderPartition::new(3, 4, 5),
            ],
        );
        let result = topic.error_result(crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            result,
            OffsetForLeaderTopicResult::new(
                "t",
                vec![
                    EpochEndOffset::error(crate::error::UNKNOWN_TOPIC_OR_PARTITION, 0),
                    EpochEndOffset::error(crate::error::UNKNOWN_TOPIC_OR_PARTITION, 3),
                ]
            )
        );
        let mut buf = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response(&mut buf, 4, std::slice::from_ref(&result))
            .unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_for_leader_epoch_topics_response(&mut cur, 4).unwrap();
        assert_eq!(decoded, vec![result]);
        assert!(
            cur.is_empty(),
            "error-response leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offsets_for_leader_epoch_response_error_counts_matches_java() {
        assert!(OffsetsForLeaderEpochResponse::error_counts(&[]).is_empty());
        let counts = OffsetsForLeaderEpochResponse::error_counts(&[
            OffsetForLeaderTopicResult::new(
                "t",
                vec![
                    EpochEndOffset::error(0, 0),
                    EpochEndOffset::error(crate::error::NOT_LEADER_OR_FOLLOWER, 1),
                ],
            ),
            OffsetForLeaderTopicResult::new(
                "u",
                vec![EpochEndOffset::error(
                    crate::error::NOT_LEADER_OR_FOLLOWER,
                    0,
                )],
            ),
        ]);
        assert_eq!(
            counts,
            HashMap::from([(0, 1), (crate::error::NOT_LEADER_OR_FOLLOWER, 2)])
        );
        assert_eq!(
            OffsetsForLeaderEpochResponse::error_counts(&[OffsetForLeaderTopicResult::new(
                "ok",
                vec![EpochEndOffset::error(0, 0)]
            )]),
            HashMap::from([(0, 1)])
        );
    }

    #[test]
    fn offset_for_leader_epoch_replica_id_sentinels_match_java() {
        assert_eq!(CONSUMER_REPLICA_ID, -1);
        assert_eq!(DEBUGGING_REPLICA_ID, -2);
        assert!(!supports_topic_permission(2));
        assert!(supports_topic_permission(3));
        assert!(supports_topic_permission(4));
    }

    #[test]
    fn offset_for_leader_epoch_request_replica_id_matches_java() {
        // Kafka 4.0 OffsetForLeaderEpochRequest.json ReplicaId is versions
        // 3+ (INT32 first field; default -2; ignorable). Official Java
        // OffsetsForLeaderEpochRequest.replicaId() /
        // OffsetForLeaderEpochRequestData.replicaId read it. Encode
        // previously always wrote CONSUMER_REPLICA_ID on v3+; decode
        // discarded it. Below v3 encode omits even when non-default;
        // decode fills DEBUGGING_REPLICA_ID. This crate speaks 0–4. This
        // is not Fetch ReplicaId / ListOffsets ReplicaId.
        let topics = [OffsetForLeaderTopic::new(
            "t",
            vec![OffsetForLeaderPartition::new(0, 3, 3)],
        )];
        for version in [0_i16, 2, 3, 4] {
            let mut buf = BytesMut::new();
            encode_offset_for_leader_epoch_topics_request_with_replica_id(
                &mut buf, version, &topics, 7,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (.., replica_id) =
                decode_offset_for_leader_epoch_topics_request(&mut cur, version).unwrap();
            if version >= 3 {
                assert_eq!(replica_id, 7);
            } else {
                assert_eq!(replica_id, DEBUGGING_REPLICA_ID);
            }
            assert!(
                cur.is_empty(),
                "OffsetForLeaderEpoch request v{version} ReplicaId leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_offset_for_leader_epoch_topics_request_with_replica_id(&mut with, 3, &topics, 7)
            .unwrap();
        let mut consumer = BytesMut::new();
        encode_offset_for_leader_epoch_topics_request(&mut consumer, 3, &topics).unwrap();
        assert_ne!(
            &with[..],
            &consumer[..],
            "v3 ReplicaId is not always CONSUMER_REPLICA_ID"
        );
        let (.., replica_id) =
            decode_offset_for_leader_epoch_topics_request(&mut consumer.as_ref(), 3).unwrap();
        assert_eq!(replica_id, CONSUMER_REPLICA_ID);

        let mut v2_with = BytesMut::new();
        encode_offset_for_leader_epoch_topics_request_with_replica_id(&mut v2_with, 2, &topics, 7)
            .unwrap();
        let mut v2_consumer = BytesMut::new();
        encode_offset_for_leader_epoch_topics_request(&mut v2_consumer, 2, &topics).unwrap();
        assert_eq!(
            &v2_with[..],
            &v2_consumer[..],
            "v2 omits ReplicaId even when the body is non-default"
        );
    }

    #[test]
    fn offset_for_leader_epoch_v2_roundtrip() {
        let mut req = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut req, 2, "t", 0, 3, 3).unwrap();
        let (topic, part, current, epoch) =
            decode_offset_for_leader_epoch_request(&mut &req[..], 2).unwrap();
        assert_eq!(topic, "t");
        assert_eq!(part, 0);
        assert_eq!(current, 3);
        assert_eq!(epoch, 3);

        let mut resp = BytesMut::new();
        encode_offset_for_leader_epoch_response(&mut resp, 2, "t", 0, 0, 4, 12).unwrap();
        let (err, got_epoch, end) =
            decode_offset_for_leader_epoch_response(&mut &resp[..], 2).unwrap();
        assert_eq!(err, 0);
        assert_eq!(got_epoch, 4);
        assert_eq!(end, 12);
    }

    #[test]
    fn offset_for_leader_epoch_v0_has_no_replica_or_current_epoch() {
        let mut req = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut req, 0, "t", 1, 9, 2).unwrap();
        let (topic, part, current, epoch) =
            decode_offset_for_leader_epoch_request(&mut &req[..], 0).unwrap();
        assert_eq!(
            (topic.as_str(), part, current, epoch),
            ("t", 1, RecordBatch::NO_PARTITION_LEADER_EPOCH, 2)
        );
    }

    #[test]
    fn offset_for_leader_epoch_v4_compact_layout_matches_independent_encode() {
        // ReplicaId -1, 1 topic "t", 1 partition 0, current 3, epoch 3,
        // empty tagged fields on the partition, topic, and top-level.
        const REQ_V4: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
        ];
        const REQ_V3: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03,
        ];
        const REQ_V2: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03,
        ];
        const RESP_V4: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00,
            0x00,
        ];
        let mut buf = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut buf, 4, "t", 0, 3, 3).unwrap();
        assert_eq!(&buf[..], REQ_V4);
        buf.clear();
        encode_offset_for_leader_epoch_request(&mut buf, 3, "t", 0, 3, 3).unwrap();
        assert_eq!(&buf[..], REQ_V3);
        buf.clear();
        encode_offset_for_leader_epoch_request(&mut buf, 2, "t", 0, 3, 3).unwrap();
        assert_eq!(&buf[..], REQ_V2);
        assert_ne!(&buf[..], REQ_V3, "v2 must not send ReplicaId (v3+)");
        buf.clear();
        encode_offset_for_leader_epoch_response(&mut buf, 4, "t", 0, 0, 4, 12).unwrap();
        assert_eq!(&buf[..], RESP_V4);
        let mut v3 = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut v3, 3, "t", 0, 3, 3).unwrap();
        assert_eq!(&v3[..], REQ_V3, "v3 ReplicaId then classic arrays");
        assert!(
            encode_offset_for_leader_epoch_request(&mut BytesMut::new(), 5, "t", 0, 3, 3).is_err(),
            "OffsetForLeaderEpoch v5+ is not spoken"
        );
    }

    #[test]
    fn offset_for_leader_epoch_v4_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut buf, 4, "t", 0, 3, 3).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_offset_for_leader_epoch_request(&mut cur, 4).unwrap(),
            ("t".into(), 0, 3, 3)
        );
        assert!(
            !cur.has_remaining(),
            "OffsetForLeaderEpoch v4 request must be leftover-empty"
        );

        buf.clear();
        encode_offset_for_leader_epoch_response(&mut buf, 4, "t", 0, 0, 4, 12).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_offset_for_leader_epoch_response(&mut cur, 4).unwrap(),
            (0, 4, 12)
        );
        assert!(
            !cur.has_remaining(),
            "OffsetForLeaderEpoch v4 response must be leftover-empty"
        );
    }

    #[test]
    fn offset_for_leader_epoch_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 OffsetForLeaderEpochResponse.json ThrottleTimeMs is
        // versions 2+ (INT32 on spoken v2–v4; first field; ignorable).
        // Official Java OffsetsForLeaderEpochRequest.getErrorResponse does
        // not set throttleTimeMs (JSON default 0).
        // encode_offset_for_leader_epoch_topics_response still writes 0
        // on v2+. Below v2 encode omits ThrottleTimeMs even when the body
        // is non-zero and decode fills 0. Empty-Topics v0 == v1 (classic;
        // LeaderEpoch is on partitions); v2 == v3 (ReplicaId is on the
        // request); v4 is compact. There is no top-level ErrorCode. This
        // crate speaks 0–4. This is not OffsetDelete / JoinGroup /
        // Fetch ThrottleTimeMs.
        let topics: Vec<OffsetForLeaderTopicResult> = vec![];
        for version in [0, 1, 2, 3, 4] {
            let mut buf = BytesMut::new();
            encode_offset_for_leader_epoch_topics_response_with_throttle(
                &mut buf, version, &topics, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_offset_for_leader_epoch_topics_response(&mut cur, version).unwrap();
            assert_eq!(decoded, topics);
            if version >= 2 {
                assert_eq!(throttle, 3_600_000);
            } else {
                assert_eq!(
                    throttle, 0,
                    "OffsetForLeaderEpoch v{version} omits ThrottleTimeMs even when the body has a non-zero value"
                );
            }
            assert!(
                cur.is_empty(),
                "OffsetForLeaderEpoch v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut v0_with = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response_with_throttle(
            &mut v0_with,
            0,
            &topics,
            3_600_000,
        )
        .unwrap();
        let mut v0_zero = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response_with_throttle(&mut v0_zero, 0, &topics, 0)
            .unwrap();
        assert_eq!(
            &v0_with[..],
            &v0_zero[..],
            "v0 omits ThrottleTimeMs even when the body has a non-zero value"
        );
        let mut v1_with = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response_with_throttle(
            &mut v1_with,
            1,
            &topics,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &v0_with[..],
            &v1_with[..],
            "empty-Topics ThrottleTimeMs bodies: v0 == v1"
        );

        let mut with = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response_with_throttle(
            &mut with, 2, &topics, 3_600_000,
        )
        .unwrap();
        let mut zero = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response_with_throttle(&mut zero, 2, &topics, 0)
            .unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v2 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response(&mut conv, 2, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_offset_for_leader_epoch_topics_response still writes ThrottleTimeMs 0"
        );
        assert_ne!(
            &v1_with[..],
            &with[..],
            "v2 adds ThrottleTimeMs before Topics"
        );

        let mut v3_with = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response_with_throttle(
            &mut v3_with,
            3,
            &topics,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v3_with[..],
            "empty-Topics ThrottleTimeMs bodies: v2 == v3"
        );
        let mut v4_with = BytesMut::new();
        encode_offset_for_leader_epoch_topics_response_with_throttle(
            &mut v4_with,
            4,
            &topics,
            3_600_000,
        )
        .unwrap();
        assert_ne!(&v3_with[..], &v4_with[..], "v4 adds compact tagged fields");
    }

    #[test]
    fn offset_for_leader_epoch_v1_response_has_epoch_not_throttle() {
        // Official: v1 added LeaderEpoch on the partition; v2 added
        // ThrottleTimeMs. v1 body is topics then {error, partition,
        // leader_epoch, end_offset} with no leading throttle.
        let mut buf = BytesMut::new();
        encode_offset_for_leader_epoch_response(&mut buf, 1, "t", 0, 0, 4, 12).unwrap();
        let mut v2 = BytesMut::new();
        encode_offset_for_leader_epoch_response(&mut v2, 2, "t", 0, 0, 4, 12).unwrap();
        assert!(buf.len() < v2.len(), "v1 response must omit ThrottleTimeMs");
        let (err, epoch, end) = decode_offset_for_leader_epoch_response(&mut &buf[..], 1).unwrap();
        assert_eq!((err, epoch, end), (0, 4, 12));
    }

    #[test]
    fn offset_for_leader_epoch_topics_of_n_v4_compact() {
        // ReplicaId -1, Topics of 1 "t", Partitions of 2 (p0/p1 current 3
        // epoch 3), empty tagged fields on each partition, the topic, and
        // the top-level.
        const REQ_V4: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x02, 0x02, 0x74, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
        ];
        // Topics of 2 "a"/"b", each one partition 0.
        const REQ_V4_TWO_TOPICS: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x03, 0x02, 0x61, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x02, 0x62, 0x02, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
        ];
        const RESP_V4: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x0d, 0x00, 0x00, 0x00,
        ];
        let topics = [OffsetForLeaderTopic::new(
            "t",
            vec![
                OffsetForLeaderPartition::new(0, 3, 3),
                OffsetForLeaderPartition::new(1, 3, 3),
            ],
        )];
        let mut buf = BytesMut::new();
        encode_offset_for_leader_epoch_topics_request(&mut buf, 4, &topics).unwrap();
        assert_eq!(&buf[..], REQ_V4);
        let mut cur = &buf[..];
        assert_eq!(
            decode_offset_for_leader_epoch_topics_request(&mut cur, 4)
                .unwrap()
                .0,
            topics
        );
        assert!(
            !cur.has_remaining(),
            "OffsetForLeaderEpoch v4 Topics of 1 / Partitions of 2 must be leftover-empty"
        );

        let two = [
            OffsetForLeaderTopic::new("a", vec![OffsetForLeaderPartition::new(0, 3, 3)]),
            OffsetForLeaderTopic::new("b", vec![OffsetForLeaderPartition::new(0, 3, 3)]),
        ];
        buf.clear();
        encode_offset_for_leader_epoch_topics_request(&mut buf, 4, &two).unwrap();
        assert_eq!(&buf[..], REQ_V4_TWO_TOPICS);
        let mut cur = &buf[..];
        assert_eq!(
            decode_offset_for_leader_epoch_topics_request(&mut cur, 4)
                .unwrap()
                .0,
            two
        );
        assert!(!cur.has_remaining(), "Topics of 2 must be leftover-empty");

        let resp = [OffsetForLeaderTopicResult::new(
            "t",
            vec![
                EpochEndOffset::new(0, 0, 4, 12),
                EpochEndOffset::new(0, 1, 4, 13),
            ],
        )];
        buf.clear();
        encode_offset_for_leader_epoch_topics_response(&mut buf, 4, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V4);
        let mut cur = &buf[..];
        assert_eq!(
            decode_offset_for_leader_epoch_topics_response(&mut cur, 4)
                .unwrap()
                .0,
            resp
        );
        assert!(
            !cur.has_remaining(),
            "OffsetForLeaderEpoch v4 response Partitions of 2 must be leftover-empty"
        );

        buf.clear();
        encode_offset_for_leader_epoch_topics_request(
            &mut buf,
            4,
            &[OffsetForLeaderTopic::new(
                "t",
                vec![OffsetForLeaderPartition::new(0, 3, 3)],
            )],
        )
        .unwrap();
        let mut one = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut one, 4, "t", 0, 3, 3).unwrap();
        assert_eq!(
            &buf[..],
            &one[..],
            "Topics of 1 must match encode_offset_for_leader_epoch_request"
        );
    }
}
