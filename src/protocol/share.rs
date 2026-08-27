//! Share groups (KIP-932): ShareGroupHeartbeat (76), ShareFetch (78),
//! ShareAcknowledge (79). Flexible v1 (Kafka 4.1 stable).

#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use super::records::{self, RecordBatch};
use crate::error::Result;

/// Gap in an acknowledgement batch.
pub const ACK_GAP: i8 = 0;
/// Accept an acquired record.
pub const ACK_ACCEPT: i8 = 1;
/// Release an acquired record back to available.
pub const ACK_RELEASE: i8 = 2;
/// Reject an acquired record.
pub const ACK_REJECT: i8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareTopicPartitions {
    pub topic_id: [u8; 16],
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatRequest {
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub subscribed_topic_names: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub member_id: Option<String>,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub assignment: Option<Vec<ShareTopicPartitions>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgementBatch {
    pub first_offset: i64,
    pub last_offset: i64,
    pub types: Vec<i8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchPartition {
    pub partition: i32,
    pub acknowledgements: Vec<AcknowledgementBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ShareFetchPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredRange {
    pub first_offset: i64,
    pub last_offset: i64,
    pub delivery_count: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchedPartition {
    pub partition: i32,
    pub error_code: i16,
    pub records: Vec<RecordBatch>,
    pub acquired: Vec<AcquiredRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchedTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ShareFetchedPartition>,
}

pub fn encode_share_group_heartbeat_request(
    buf: &mut BytesMut,
    req: &ShareGroupHeartbeatRequest,
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(&req.group_id))?;
    buf::put_compact_string(buf, Some(&req.member_id))?;
    buf.put_i32(req.member_epoch);
    buf::put_compact_string(buf, None)?;
    match &req.subscribed_topic_names {
        None => buf::put_array_len(buf, true, None)?,
        Some(names) => {
            buf::put_array_len(buf, true, Some(names.len()))?;
            for n in names {
                buf::put_compact_string(buf, Some(n))?;
            }
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_share_group_heartbeat_request<B: Buf>(
    buf: &mut B,
) -> Result<ShareGroupHeartbeatRequest> {
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_epoch = buf::get_i32(buf)?;
    let _rack = buf::get_compact_string(buf)?;
    let subscribed_topic_names = {
        let n = buf::get_array_len(buf, true)?;
        match n {
            None => None,
            Some(n) => {
                let mut names = Vec::with_capacity(n);
                for _ in 0..n {
                    names.push(buf::get_compact_string(buf)?.unwrap_or_default());
                }
                Some(names)
            }
        }
    };
    buf::skip_tagged_fields(buf)?;
    Ok(ShareGroupHeartbeatRequest {
        group_id,
        member_id,
        member_epoch,
        subscribed_topic_names,
    })
}

pub fn encode_share_group_heartbeat_response(
    buf: &mut BytesMut,
    resp: &ShareGroupHeartbeatResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_compact_string(buf, resp.member_id.as_deref())?;
    buf.put_i32(resp.member_epoch);
    buf.put_i32(resp.heartbeat_interval_ms);
    match &resp.assignment {
        None => buf::put_unsigned_varint(buf, 0),
        Some(parts) => {
            buf::put_unsigned_varint(buf, 1);
            buf::put_array_len(buf, true, Some(parts.len()))?;
            for t in parts {
                buf.extend_from_slice(&t.topic_id);
                buf::put_array_len(buf, true, Some(t.partitions.len()))?;
                for p in &t.partitions {
                    buf.put_i32(*p);
                }
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_share_group_heartbeat_response<B: Buf>(
    buf: &mut B,
) -> Result<ShareGroupHeartbeatResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let member_id = buf::get_compact_string(buf)?;
    let member_epoch = buf::get_i32(buf)?;
    let heartbeat_interval_ms = buf::get_i32(buf)?;
    let present = buf::get_unsigned_varint(buf)?;
    let assignment = if present == 0 {
        None
    } else {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut parts = Vec::with_capacity(n);
        for _ in 0..n {
            let topic_id = buf::get_uuid(buf)?;
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                partitions.push(buf::get_i32(buf)?);
            }
            buf::skip_tagged_fields(buf)?;
            parts.push(ShareTopicPartitions {
                topic_id,
                partitions,
            });
        }
        buf::skip_tagged_fields(buf)?;
        Some(parts)
    };
    buf::skip_tagged_fields(buf)?;
    Ok(ShareGroupHeartbeatResponse {
        error_code,
        error_message,
        member_id,
        member_epoch,
        heartbeat_interval_ms,
        assignment,
    })
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
    reason = "ShareFetch v1 body fields are a single wire encode"
)]
pub fn encode_share_fetch_request(
    buf: &mut BytesMut,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    topics: &[ShareFetchTopic],
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(group_id))?;
    buf::put_compact_string(buf, Some(member_id))?;
    buf.put_i32(share_session_epoch);
    buf.put_i32(max_wait_ms);
    buf.put_i32(min_bytes);
    buf.put_i32(max_bytes);
    buf.put_i32(max_records);
    buf.put_i32(max_records);
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            encode_ack_batches(buf, &p.acknowledgements)?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_array_len(buf, true, Some(0))?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_share_fetch_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, String, i32, i32, Vec<ShareFetchTopic>)> {
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let epoch = buf::get_i32(buf)?;
    let _max_wait = buf::get_i32(buf)?;
    let _min_bytes = buf::get_i32(buf)?;
    let _max_bytes = buf::get_i32(buf)?;
    let max_records = buf::get_i32(buf)?;
    let _batch = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let acknowledgements = decode_ack_batches(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(ShareFetchPartition {
                partition,
                acknowledgements,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(ShareFetchTopic {
            topic_id,
            partitions,
        });
    }
    let forgotten = buf::get_array_len(buf, true)?.unwrap_or(0);
    for _ in 0..forgotten {
        let _id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        for _ in 0..pn {
            let _p = buf::get_i32(buf)?;
        }
        buf::skip_tagged_fields(buf)?;
    }
    buf::skip_tagged_fields(buf)?;
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

pub fn encode_share_fetch_response(
    buf: &mut BytesMut,
    topics: &[ShareFetchedTopic],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(0);
    buf::put_compact_string(buf, None)?;
    buf.put_i32(15_000);
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf::put_compact_string(buf, None)?;
            buf.put_i16(0);
            buf::put_compact_string(buf, None)?;
            encode_leader(buf, 1, 0);
            let mut recs = BytesMut::new();
            for batch in &p.records {
                records::encode_record_batch(&mut recs, batch)?;
            }
            buf::put_compact_bytes(buf, Some(&recs))?;
            buf::put_array_len(buf, true, Some(p.acquired.len()))?;
            for a in &p.acquired {
                buf.put_i64(a.first_offset);
                buf.put_i64(a.last_offset);
                buf.put_i16(a.delivery_count);
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_array_len(buf, true, Some(0))?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

pub fn decode_share_fetch_response<B: Buf>(buf: &mut B) -> Result<Vec<ShareFetchedTopic>> {
    let _th = buf::get_i32(buf)?;
    let err = buf::get_i16(buf)?;
    let _msg = buf::get_compact_string(buf)?;
    if err != 0 {
        return Err(crate::error::Error::broker(err, "ShareFetch"));
    }
    let _lock = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let _em = buf::get_compact_string(buf)?;
            let _ack_err = buf::get_i16(buf)?;
            let _ack_msg = buf::get_compact_string(buf)?;
            let _leader = decode_leader(buf)?;
            let rec_bytes = buf::take_compact_bytes(buf)?.unwrap_or_else(Bytes::new);
            let records = if rec_bytes.is_empty() {
                Vec::new()
            } else {
                let mut rec_buf = rec_bytes;
                records::decode_record_batches(&mut rec_buf)?
            };
            let an = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut acquired = Vec::with_capacity(an);
            for _ in 0..an {
                let first_offset = buf::get_i64(buf)?;
                let last_offset = buf::get_i64(buf)?;
                let delivery_count = buf::get_i16(buf)?;
                buf::skip_tagged_fields(buf)?;
                acquired.push(AcquiredRange {
                    first_offset,
                    last_offset,
                    delivery_count,
                });
            }
            buf::skip_tagged_fields(buf)?;
            partitions.push(ShareFetchedPartition {
                partition,
                error_code,
                records,
                acquired,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(ShareFetchedTopic {
            topic_id,
            partitions,
        });
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
    Ok(topics)
}

pub fn encode_share_acknowledge_request(
    buf: &mut BytesMut,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
    topic_id: [u8; 16],
    partitions: &[(i32, Vec<AcknowledgementBatch>)],
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(group_id))?;
    buf::put_compact_string(buf, Some(member_id))?;
    buf.put_i32(share_session_epoch);
    if partitions.is_empty() {
        buf::put_array_len(buf, true, Some(0))?;
    } else {
        buf::put_array_len(buf, true, Some(1))?;
        buf.extend_from_slice(&topic_id);
        buf::put_array_len(buf, true, Some(partitions.len()))?;
        for (partition, batches) in partitions {
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
pub fn encode_share_fetch_error(buf: &mut BytesMut, error_code: i16) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(error_code);
    buf::put_compact_string(buf, None)?;
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(0))?;
    buf::put_array_len(buf, true, Some(0))?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

#[expect(
    clippy::type_complexity,
    reason = "ack request is group, member, epoch, and topic-partition batches"
)]
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
        encode_share_group_heartbeat_request(&mut buf, &req).unwrap();
        let decoded = decode_share_group_heartbeat_request(&mut &buf[..]).unwrap();
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
        encode_share_group_heartbeat_response(&mut buf, &resp).unwrap();
        assert_eq!(
            decode_share_group_heartbeat_response(&mut &buf[..]).unwrap(),
            resp
        );

        let leave = ShareGroupHeartbeatRequest {
            group_id: "sg".into(),
            member_id: "m1".into(),
            member_epoch: -1,
            subscribed_topic_names: None,
        };
        buf.clear();
        encode_share_group_heartbeat_request(&mut buf, &leave).unwrap();
        assert_eq!(
            decode_share_group_heartbeat_request(&mut &buf[..])
                .unwrap()
                .member_epoch,
            -1
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
        encode_share_fetch_response(&mut buf, &topics).unwrap();
        let decoded = decode_share_fetch_response(&mut &buf[..]).unwrap();
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
        encode_share_fetch_request(&mut buf, "sg", "m1", 0, 10, 1, 1024, 16, &req_topics).unwrap();
        let (gid, mid, epoch, max_records, got) =
            decode_share_fetch_request(&mut &buf[..]).unwrap();
        assert_eq!(
            (gid.as_str(), mid.as_str(), epoch, max_records),
            ("sg", "m1", 0, 16)
        );
        assert_eq!(got[0].partitions[0].partition, 0);

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
        encode_share_fetch_error(&mut buf, crate::error::INVALID_SHARE_SESSION_EPOCH).unwrap();
        let mut cur = &buf[..];
        let _th = crate::protocol::buf::get_i32(&mut cur).unwrap();
        let err = crate::protocol::buf::get_i16(&mut cur).unwrap();
        assert_eq!(err, crate::error::INVALID_SHARE_SESSION_EPOCH);
    }
}
