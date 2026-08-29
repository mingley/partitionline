//! Share groups (KIP-932): ShareGroupHeartbeat (76), ShareFetch (78),
//! ShareAcknowledge (79). ShareGroupHeartbeat is flexible from v0
//! (Kafka 4.0 early access v0; Kafka 4.1 stable v1). This crate
//! speaks 0–1. Same fields. v2+ is not spoken. ShareFetch is flexible
//! from v0. Kafka 4.0 `validVersions` is `"0"`; Kafka 4.1 `"1"`
//! (v0 removed). This crate speaks 0–1. v0 and v1 fields differ
//! (v0 PartitionMaxBytes; v1 MaxRecords / BatchSize /
//! AcquisitionLockTimeoutMs). v2+ is not spoken.

use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    /// Member epoch (`0` join, `-1` leave, otherwise heartbeat).
    pub member_epoch: i32,
    /// Subscribed topic names (`None` means unchanged).
    pub subscribed_topic_names: Option<Vec<String>>,
}

/// ShareGroupHeartbeat response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatResponse {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchPartition {
    /// Partition index.
    pub partition: i32,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchedPartition {
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Record batches.
    pub records: Vec<RecordBatch>,
    /// Offsets acquired by this member.
    pub acquired: Vec<AcquiredRange>,
}

/// One topic in a ShareFetch response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchedTopic {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// Partition bodies.
    pub partitions: Vec<ShareFetchedPartition>,
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
pub fn encode_share_group_heartbeat_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ShareGroupHeartbeatResponse,
) -> crate::error::Result<()> {
    let flexible = share_group_heartbeat_flexible(version)?;
    buf.put_i32(0);
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
pub fn decode_share_group_heartbeat_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ShareGroupHeartbeatResponse> {
    let flexible = share_group_heartbeat_flexible(version)?;
    let _th = buf::get_i32(buf)?;
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
/// PartitionMaxBytes (v0 only). v2+ is not spoken.
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
    let flexible = share_fetch_flexible(version)?;
    buf::put_string(buf, flexible, Some(group_id))?;
    buf::put_string(buf, flexible, Some(member_id))?;
    buf.put_i32(share_session_epoch);
    buf.put_i32(max_wait_ms);
    buf.put_i32(min_bytes);
    buf.put_i32(max_bytes);
    if version >= 1 {
        buf.put_i32(max_records);
        buf.put_i32(max_records);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            if version == 0 {
                buf.put_i32(max_bytes);
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
    buf::put_array_len(buf, flexible, Some(0))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ShareFetch request (`version` 0–1).
///
/// Returns `(group_id, member_id, epoch, max_records, topics)`.
/// `max_records` is the v1 MaxRecords field; v0 omits it and decode
/// fills `0`.
pub fn decode_share_fetch_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, i32, i32, Vec<ShareFetchTopic>)> {
    let flexible = share_fetch_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let epoch = buf::get_i32(buf)?;
    let _max_wait = buf::get_i32(buf)?;
    let _min_bytes = buf::get_i32(buf)?;
    let _max_bytes = buf::get_i32(buf)?;
    let max_records = if version >= 1 {
        let max_records = buf::get_i32(buf)?;
        let _batch = buf::get_i32(buf)?;
        max_records
    } else {
        0
    };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            if version == 0 {
                let _partition_max_bytes = buf::get_i32(buf)?;
            }
            let acknowledgements = decode_ack_batches(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(ShareFetchPartition {
                partition,
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
    let forgotten = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    for _ in 0..forgotten {
        let _id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        for _ in 0..pn {
            let _p = buf::get_i32(buf)?;
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((group_id, member_id, epoch, max_records, topics))
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
pub fn encode_share_fetch_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ShareFetchedTopic],
) -> crate::error::Result<()> {
    let flexible = share_fetch_flexible(version)?;
    buf.put_i32(0);
    buf.put_i16(0);
    buf::put_string(buf, flexible, None)?;
    if version >= 1 {
        buf.put_i32(15_000);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf::put_string(buf, flexible, None)?;
            buf.put_i16(0);
            buf::put_string(buf, flexible, None)?;
            encode_leader(buf, 1, 0);
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
    buf::put_array_len(buf, flexible, Some(0))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ShareFetch response (`version` 0–1) into topic/partition bodies.
pub fn decode_share_fetch_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<ShareFetchedTopic>> {
    let flexible = share_fetch_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let err = buf::get_i16(buf)?;
    let _msg = buf::get_string(buf, flexible)?;
    if err != 0 {
        return Err(crate::error::Error::broker(err, "ShareFetch"));
    }
    if version >= 1 {
        let _lock = buf::get_i32(buf)?;
    }
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let _em = buf::get_string(buf, flexible)?;
            let _ack_err = buf::get_i16(buf)?;
            let _ack_msg = buf::get_string(buf, flexible)?;
            let _leader = decode_leader(buf)?;
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
    let nodes = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    for _ in 0..nodes {
        let _id = buf::get_i32(buf)?;
        let _h = buf::get_string(buf, flexible)?;
        let _p = buf::get_i32(buf)?;
        let _r = buf::get_string(buf, flexible)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(topics)
}

/// Encode ShareAcknowledge for one topic.
pub fn encode_share_acknowledge_request(
    buf: &mut BytesMut,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    topic_id: [u8; 16],
    partitions: &[(i32, Vec<AcknowledgementBatch>)],
) -> crate::error::Result<()> {
    if partitions.is_empty() {
        encode_share_acknowledge_topics(buf, group_id, member_id, share_session_epoch, &[])
    } else {
        encode_share_acknowledge_topics(
            buf,
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
#[derive(Debug, Clone)]
pub struct ShareAckTopic {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// `(partition, acknowledgement batches)`.
    pub partitions: Vec<(i32, Vec<AcknowledgementBatch>)>,
}

/// ShareAcknowledge with several topics in one request.
pub fn encode_share_acknowledge_topics(
    buf: &mut BytesMut,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    topics: &[ShareAckTopic],
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(group_id))?;
    buf::put_compact_string(buf, Some(member_id))?;
    buf.put_i32(share_session_epoch);
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for (partition, batches) in &t.partitions {
            buf.put_i32(*partition);
            encode_ack_batches(buf, batches)?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Top-level ShareFetch error (session not found / bad epoch).
pub fn encode_share_fetch_error(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
) -> crate::error::Result<()> {
    let flexible = share_fetch_flexible(version)?;
    buf.put_i32(0);
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
/// Decode ShareAcknowledge: `(group_id, member_id, epoch, topic-partition batches)`.
pub fn decode_share_acknowledge_request<B: Buf>(
    buf: &mut B,
) -> Result<(
    String,
    String,
    i32,
    Vec<([u8; 16], i32, Vec<AcknowledgementBatch>)>,
)> {
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let epoch = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::new();
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let batches = decode_ack_batches(buf)?;
            buf::skip_tagged_fields(buf)?;
            topics.push((topic_id, partition, batches));
        }
        buf::skip_tagged_fields(buf)?;
    }
    buf::skip_tagged_fields(buf)?;
    Ok((group_id, member_id, epoch, topics))
}

/// Encode ShareAcknowledge: throttle `0` plus error code.
pub fn encode_share_acknowledge_response(
    buf: &mut BytesMut,
    error_code: i16,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(error_code);
    buf::put_compact_string(buf, None)?;
    buf::put_array_len(buf, true, Some(0))?;
    buf::put_array_len(buf, true, Some(0))?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode ShareAcknowledge: error code.
pub fn decode_share_acknowledge_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let _msg = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    for _ in 0..n {
        let _id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        for _ in 0..pn {
            let _p = buf::get_i32(buf)?;
            let _e = buf::get_i16(buf)?;
            let _m = buf::get_compact_string(buf)?;
            let _l = decode_leader(buf)?;
            buf::skip_tagged_fields(buf)?;
        }
        buf::skip_tagged_fields(buf)?;
    }
    let nodes = buf::get_array_len(buf, true)?.unwrap_or(0);
    for _ in 0..nodes {
        let _id = buf::get_i32(buf)?;
        let _h = buf::get_compact_string(buf)?;
        let _p = buf::get_i32(buf)?;
        let _r = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
    }
    buf::skip_tagged_fields(buf)?;
    Ok(error_code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::records::Record;
    use bytes::Bytes;

    #[test]
    fn share_group_heartbeat_join_leave_roundtrip() {
        let req = ShareGroupHeartbeatRequest {
            group_id: "sg".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
        };
        let mut buf = BytesMut::new();
        encode_share_group_heartbeat_request(&mut buf, 1, &req).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_share_group_heartbeat_request(&mut cur, 1).unwrap();
        assert!(!cur.has_remaining(), "v1 request leftover-empty");
        assert_eq!(decoded.member_epoch, 0);
        assert_eq!(decoded.member_id, "m1");
        assert_eq!(decoded.subscribed_topic_names, Some(vec!["t".into()]));

        let resp = ShareGroupHeartbeatResponse {
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
            member_epoch: -1,
            subscribed_topic_names: None,
        };
        buf.clear();
        encode_share_group_heartbeat_request(&mut buf, 1, &leave).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_share_group_heartbeat_request(&mut cur, 1)
                .unwrap()
                .member_epoch,
            -1
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
            member_epoch: 0,
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

        let resp = ShareGroupHeartbeatResponse {
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
        let decoded = decode_share_fetch_response(&mut &buf[..], 1).unwrap();
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
                acknowledgements: vec![],
            }],
        }];
        buf.clear();
        encode_share_fetch_request(&mut buf, 1, "sg", "m1", 0, 10, 1, 1024, 16, &req_topics)
            .unwrap();
        let mut cur = &buf[..];
        let (gid, mid, epoch, max_records, got) = decode_share_fetch_request(&mut cur, 1).unwrap();
        assert_eq!(
            (gid.as_str(), mid.as_str(), epoch, max_records),
            ("sg", "m1", 0, 16)
        );
        assert_eq!(got[0].partitions[0].partition, 0);
        assert!(!cur.has_remaining(), "v1 request leftover-empty");

        buf.clear();
        encode_share_acknowledge_request(
            &mut buf,
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
        let (gid, mid, _e, acks) = decode_share_acknowledge_request(&mut &buf[..]).unwrap();
        assert_eq!(gid, "sg");
        assert_eq!(mid, "m1");
        assert_eq!(acks[0].2[0].types, vec![ACK_ACCEPT]);
        assert_eq!(acks[0].2[0].last_offset, 2);
        buf.clear();
        encode_share_acknowledge_response(&mut buf, 0).unwrap();
        assert_eq!(decode_share_acknowledge_response(&mut &buf[..]).unwrap(), 0);
    }

    #[test]
    fn share_acknowledge_encodes_several_partitions() {
        let mut buf = BytesMut::new();
        encode_share_acknowledge_request(
            &mut buf,
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
        let (_gid, _mid, epoch, acks) = decode_share_acknowledge_request(&mut &buf[..]).unwrap();
        assert_eq!(epoch, 2);
        assert_eq!(acks.len(), 2);
        assert_eq!(acks[0].1, 0);
        assert_eq!(acks[1].1, 1);
        assert_eq!(acks[1].2[0].types, vec![ACK_REJECT]);
    }

    #[test]
    fn share_acknowledge_close_session_has_no_topics() {
        let mut buf = BytesMut::new();
        encode_share_acknowledge_request(&mut buf, "sg", "m1", -1, [0u8; 16], &[]).unwrap();
        let (_gid, _mid, epoch, acks) = decode_share_acknowledge_request(&mut &buf[..]).unwrap();
        assert_eq!(epoch, -1);
        assert!(acks.is_empty());
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
        let (gid, mid, epoch, max_records, got) = decode_share_fetch_request(&mut cur, 0).unwrap();
        assert_eq!(
            (gid.as_str(), mid.as_str(), epoch, max_records),
            ("sg", "m1", 0, 0),
            "v0 omits MaxRecords; decode fills 0"
        );
        assert_eq!(got[0].partitions[0].partition, 0);
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
        assert_eq!(decode_share_fetch_response(&mut cur, 0).unwrap(), resp);
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
        v0.clear();
        let err = encode_share_fetch_response(&mut v0, 2, &resp).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 response is not spoken, got {err}"
        );
    }
}
