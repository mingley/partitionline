#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

pub const COORDINATOR_GROUP: i8 = 0;
/// FindCoordinator `key_type` for a transactional.id (KIP-98).
pub const COORDINATOR_TRANSACTION: i8 = 1;
pub const COORDINATOR_SHARE: i8 = 2;

pub fn encode_find_coordinator_request(buf: &mut BytesMut, key: &str) -> crate::error::Result<()> {
    encode_find_coordinator_request_typed(buf, key, COORDINATOR_GROUP)
}

pub fn encode_find_coordinator_request_typed(
    buf: &mut BytesMut,
    key: &str,
    key_type: i8,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(key))?;
    buf.put_i8(key_type);
    Ok(())
}

pub fn decode_find_coordinator_request<B: Buf>(buf: &mut B) -> Result<(String, i8)> {
    let key = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let key_type = buf::get_i8(buf)?;
    Ok((key, key_type))
}

pub fn encode_find_coordinator_response(
    buf: &mut BytesMut,
    node_id: i32,
    host: &str,
    port: i32,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(0);
    buf::put_classic_nullable_string(buf, None)?;
    buf.put_i32(node_id);
    buf::put_classic_nullable_string(buf, Some(host))?;
    buf.put_i32(port);
    Ok(())
}

pub fn decode_find_coordinator_response<B: Buf>(buf: &mut B) -> Result<(i16, i32, String, i32)> {
    let _throttle = buf::get_i32(buf)?;
    let error = buf::get_i16(buf)?;
    let _msg = buf::get_classic_nullable_string(buf)?;
    let node_id = buf::get_i32(buf)?;
    let host = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let port = buf::get_i32(buf)?;
    Ok((error, node_id, host, port))
}

pub fn encode_join_group_request(
    buf: &mut BytesMut,
    group_id: &str,
    session_timeout_ms: i32,
    member_id: &str,
    protocol_type: &str,
    protocol_name: &str,
    metadata: &[u8],
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf.put_i32(session_timeout_ms);
    buf.put_i32(session_timeout_ms); // rebalance timeout
    buf::put_classic_nullable_string(buf, Some(member_id))?;
    buf::put_classic_nullable_string(buf, None)?; // group_instance_id
    buf::put_classic_nullable_string(buf, Some(protocol_type))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(protocol_name))?;
    buf::put_classic_bytes(buf, Some(metadata))?;
    Ok(())
}

pub fn decode_join_group_request<B: Buf>(buf: &mut B) -> Result<(String, String, Vec<u8>)> {
    let group_id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _session = buf::get_i32(buf)?;
    let _rebalance = buf::get_i32(buf)?;
    let member_id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _instance = buf::get_classic_nullable_string(buf)?;
    let _ptype = buf::get_classic_nullable_string(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut metadata = Vec::new();
    for _ in 0..n {
        let _name = buf::get_classic_nullable_string(buf)?;
        metadata = buf::get_classic_bytes(buf)?.unwrap_or_default();
    }
    Ok((group_id, member_id, metadata))
}

#[derive(Debug, Clone)]
pub struct JoinMember {
    pub member_id: String,
    pub metadata: Vec<u8>,
}

pub fn encode_join_group_response(
    buf: &mut BytesMut,
    error_code: i16,
    generation_id: i32,
    protocol_name: &str,
    leader: &str,
    member_id: &str,
    members: &[JoinMember],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(error_code);
    buf.put_i32(generation_id);
    buf::put_classic_nullable_string(buf, Some(protocol_name))?;
    buf::put_classic_nullable_string(buf, Some(leader))?;
    buf::put_classic_nullable_string(buf, Some(member_id))?;
    buf::put_array_len(buf, false, Some(members.len()))?;
    for m in members {
        buf::put_classic_nullable_string(buf, Some(&m.member_id))?;
        buf::put_classic_nullable_string(buf, None)?;
        buf::put_classic_bytes(buf, Some(&m.metadata))?;
    }
    Ok(())
}

pub fn decode_join_group_response<B: Buf>(
    buf: &mut B,
) -> Result<(i16, i32, String, String, String, Vec<JoinMember>)> {
    let _throttle = buf::get_i32(buf)?;
    let error = buf::get_i16(buf)?;
    let generation = buf::get_i32(buf)?;
    let protocol = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let leader = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let member_id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut members = Vec::with_capacity(n);
    for _ in 0..n {
        let mid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let _inst = buf::get_classic_nullable_string(buf)?;
        let metadata = buf::get_classic_bytes(buf)?.unwrap_or_default();
        members.push(JoinMember {
            member_id: mid,
            metadata,
        });
    }
    Ok((error, generation, protocol, leader, member_id, members))
}

pub fn encode_sync_group_request(
    buf: &mut BytesMut,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    assignments: &[(String, Vec<u8>)],
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf.put_i32(generation_id);
    buf::put_classic_nullable_string(buf, Some(member_id))?;
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_array_len(buf, false, Some(assignments.len()))?;
    for (id, bytes) in assignments {
        buf::put_classic_nullable_string(buf, Some(id))?;
        buf::put_classic_bytes(buf, Some(bytes))?;
    }
    Ok(())
}

#[expect(
    clippy::type_complexity,
    reason = "SyncGroup assignment list is (member_id, bytes) pairs"
)]
pub fn decode_sync_group_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, String, Vec<(String, Vec<u8>)>)> {
    let group_id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _gen = buf::get_i32(buf)?;
    let member_id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _inst = buf::get_classic_nullable_string(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut assignments = Vec::with_capacity(n);
    for _ in 0..n {
        let id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let bytes = buf::get_classic_bytes(buf)?.unwrap_or_default();
        assignments.push((id, bytes));
    }
    Ok((group_id, member_id, assignments))
}

pub fn encode_sync_group_response(
    buf: &mut BytesMut,
    error_code: i16,
    assignment: &[u8],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(error_code);
    buf::put_classic_bytes(buf, Some(assignment))?;
    Ok(())
}

pub fn decode_sync_group_response<B: Buf>(buf: &mut B) -> Result<(i16, Vec<u8>)> {
    let _throttle = buf::get_i32(buf)?;
    let error = buf::get_i16(buf)?;
    let assignment = buf::get_classic_bytes(buf)?.unwrap_or_default();
    Ok((error, assignment))
}

pub fn encode_heartbeat_request(
    buf: &mut BytesMut,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf.put_i32(generation_id);
    buf::put_classic_nullable_string(buf, Some(member_id))?;
    buf::put_classic_nullable_string(buf, None)?;
    Ok(())
}

pub fn decode_heartbeat_request<B: Buf>(buf: &mut B) -> Result<(String, i32, String)> {
    let g = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let gen = buf::get_i32(buf)?;
    let m = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    Ok((g, gen, m))
}

pub fn encode_heartbeat_response(buf: &mut BytesMut, error_code: i16) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(error_code);
    Ok(())
}

pub fn decode_heartbeat_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _throttle = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

pub fn encode_leave_group_request(
    buf: &mut BytesMut,
    group_id: &str,
    member_id: &str,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf::put_classic_nullable_string(buf, Some(member_id))?;
    Ok(())
}

pub fn decode_leave_group_request<B: Buf>(buf: &mut B) -> Result<(String, String)> {
    let g = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let m = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    Ok((g, m))
}

pub fn encode_leave_group_response(
    buf: &mut BytesMut,
    error_code: i16,
) -> crate::error::Result<()> {
    buf.put_i16(error_code);
    Ok(())
}

pub fn decode_leave_group_response<B: Buf>(buf: &mut B) -> Result<i16> {
    buf::get_i16(buf)
}

/// One partition in OffsetCommit v7 / OffsetFetch v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetPartition {
    pub partition: i32,
    pub offset: i64,
}

/// Topic + partitions for OffsetCommit v7.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetTopic {
    pub topic: String,
    pub partitions: Vec<OffsetPartition>,
}

/// Topic + partition indexes for OffsetFetch v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// One partition in an OffsetFetch v5 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedOffset {
    pub partition: i32,
    pub offset: i64,
    pub error_code: i16,
}

/// Topic + committed offsets from OffsetFetch v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedOffsetTopic {
    pub topic: String,
    pub partitions: Vec<FetchedOffset>,
}

pub fn encode_offset_commit_request(
    buf: &mut BytesMut,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topics: &[OffsetTopic],
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf.put_i32(generation_id);
    buf::put_classic_nullable_string(buf, Some(member_id))?;
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic))?;
        buf::put_array_len(buf, false, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i64(p.offset);
            buf.put_i32(-1); // committed_leader_epoch
            buf::put_classic_nullable_string(buf, None)?;
        }
    }
    Ok(())
}

pub fn decode_offset_commit_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, String, Vec<OffsetTopic>)> {
    let group = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _gen = buf::get_i32(buf)?;
    let member = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _inst = buf::get_classic_nullable_string(buf)?;
    let tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let offset = buf::get_i64(buf)?;
            let _epoch = buf::get_i32(buf)?;
            let _meta = buf::get_classic_nullable_string(buf)?;
            partitions.push(OffsetPartition { partition, offset });
        }
        topics.push(OffsetTopic { topic, partitions });
    }
    Ok((group, member, topics))
}

pub fn encode_offset_commit_response(
    buf: &mut BytesMut,
    topics: &[OffsetTopic],
    error: i16,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic))?;
        buf::put_array_len(buf, false, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(error);
        }
    }
    Ok(())
}

pub fn decode_offset_commit_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _throttle = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut first_err = 0i16;
    for _ in 0..n {
        let _topic = buf::get_classic_nullable_string(buf)?;
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        for _ in 0..pn {
            let _p = buf::get_i32(buf)?;
            let err = buf::get_i16(buf)?;
            if first_err == 0 && err != 0 {
                first_err = err;
            }
        }
    }
    Ok(first_err)
}

pub fn encode_offset_fetch_request(
    buf: &mut BytesMut,
    group_id: &str,
    topics: &[OffsetFetchTopic],
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic))?;
        buf::put_array_len(buf, false, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(*p);
        }
    }
    Ok(())
}

pub fn decode_offset_fetch_request<B: Buf>(buf: &mut B) -> Result<(String, Vec<OffsetFetchTopic>)> {
    let group = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        topics.push(OffsetFetchTopic { topic, partitions });
    }
    Ok((group, topics))
}

pub fn encode_offset_fetch_response(
    buf: &mut BytesMut,
    topics: &[FetchedOffsetTopic],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic))?;
        buf::put_array_len(buf, false, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i64(p.offset);
            buf.put_i32(-1);
            buf::put_classic_nullable_string(buf, None)?;
            buf.put_i16(p.error_code);
        }
    }
    buf.put_i16(0); // top-level error
    Ok(())
}

pub fn decode_offset_fetch_response<B: Buf>(buf: &mut B) -> Result<Vec<FetchedOffsetTopic>> {
    let _throttle = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let offset = buf::get_i64(buf)?;
            let _epoch = buf::get_i32(buf)?;
            let _meta = buf::get_classic_nullable_string(buf)?;
            let error_code = buf::get_i16(buf)?;
            partitions.push(FetchedOffset {
                partition,
                offset,
                error_code,
            });
        }
        topics.push(FetchedOffsetTopic { topic, partitions });
    }
    let top = buf::get_i16(buf)?;
    if top != 0 {
        return Err(crate::error::Error::broker(top, "OffsetFetch"));
    }
    Ok(topics)
}

/// ConsumerProtocol subscription v0.
pub fn encode_subscription(topics: &[String]) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_i16(0);
    buf::put_array_len(&mut buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(&mut buf, Some(t))?;
    }
    buf.put_i32(-1);
    Ok(buf.to_vec())
}

pub fn decode_subscription(mut bytes: &[u8]) -> Result<Vec<String>> {
    let _ver = buf::get_i16(&mut bytes)?;
    let n = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        topics.push(buf::get_classic_nullable_string(&mut bytes)?.unwrap_or_default());
    }
    Ok(topics)
}

pub fn encode_assignment(topic: &str, partitions: &[i32]) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_i16(0);
    buf::put_array_len(&mut buf, false, Some(1))?;
    buf::put_classic_nullable_string(&mut buf, Some(topic))?;
    buf::put_array_len(&mut buf, false, Some(partitions.len()))?;
    for p in partitions {
        buf.put_i32(*p);
    }
    buf.put_i32(-1);
    Ok(buf.to_vec())
}

pub fn decode_assignment(mut bytes: &[u8]) -> Result<Vec<(String, Vec<i32>)>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let _ver = buf::get_i16(&mut bytes)?;
    let n = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_classic_nullable_string(&mut bytes)?.unwrap_or_default();
        let pn = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
        let mut parts = Vec::with_capacity(pn);
        for _ in 0..pn {
            parts.push(buf::get_i32(&mut bytes)?);
        }
        out.push((topic, parts));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_coordinator_v2_sends_key_type_and_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_find_coordinator_request_typed(&mut buf, "tx-1", COORDINATOR_TRANSACTION).unwrap();
        let mut cur = &buf[..];
        let (key, key_type) = decode_find_coordinator_request(&mut cur).unwrap();
        assert_eq!((key.as_str(), key_type), ("tx-1", COORDINATOR_TRANSACTION));
        assert!(
            cur.is_empty(),
            "v2 decoder must consume key_type; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_find_coordinator_request_typed(&mut buf, "g", COORDINATOR_GROUP).unwrap();
        let mut cur = &buf[..];
        let (key, key_type) = decode_find_coordinator_request(&mut cur).unwrap();
        assert_eq!((key.as_str(), key_type), ("g", COORDINATOR_GROUP));
        assert!(
            cur.is_empty(),
            "group key_type leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn subscription_assignment_roundtrip() {
        let sub = encode_subscription(&["t".into()]).unwrap();
        assert_eq!(decode_subscription(&sub).unwrap(), vec!["t".to_string()]);
        let asg = encode_assignment("t", &[0, 1]).unwrap();
        let decoded = decode_assignment(&asg).unwrap();
        assert_eq!(decoded[0].0, "t");
        assert_eq!(decoded[0].1, vec![0, 1]);
    }

    #[test]
    fn offset_commit_v7_batches_partitions_and_consumes_epoch_metadata() {
        let topics = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![
                OffsetPartition {
                    partition: 0,
                    offset: 3,
                },
                OffsetPartition {
                    partition: 2,
                    offset: 9,
                },
            ],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_request(&mut buf, "g", 7, "m1", &topics).unwrap();
        let mut cur = &buf[..];
        let (gid, mid, got) = decode_offset_commit_request(&mut cur).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got, topics);
        assert!(
            cur.is_empty(),
            "v7 decoder must consume leader epoch and metadata; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_offset_commit_response(&mut buf, &topics, 0).unwrap();
        assert_eq!(decode_offset_commit_response(&mut &buf[..]).unwrap(), 0);
    }

    #[test]
    fn offset_commit_response_returns_first_partition_error() {
        let topics = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![
                OffsetPartition {
                    partition: 0,
                    offset: 1,
                },
                OffsetPartition {
                    partition: 1,
                    offset: 2,
                },
            ],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_response(&mut buf, &topics, 16).unwrap();
        assert_eq!(decode_offset_commit_response(&mut &buf[..]).unwrap(), 16);
    }

    #[test]
    fn offset_fetch_v5_batches_partitions_and_consumes_tail() {
        let req = vec![OffsetFetchTopic {
            topic: "t".into(),
            partitions: vec![0, 1, 2],
        }];
        let mut buf = BytesMut::new();
        encode_offset_fetch_request(&mut buf, "g", &req).unwrap();
        let mut cur = &buf[..];
        let (gid, got) = decode_offset_fetch_request(&mut cur).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(got, req);
        assert!(cur.is_empty());

        let resp = vec![FetchedOffsetTopic {
            topic: "t".into(),
            partitions: vec![
                FetchedOffset {
                    partition: 0,
                    offset: 4,
                    error_code: 0,
                },
                FetchedOffset {
                    partition: 1,
                    offset: -1,
                    error_code: 0,
                },
            ],
        }];
        buf.clear();
        encode_offset_fetch_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_offset_fetch_response(&mut cur).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            cur.is_empty(),
            "v5 decoder must consume epoch, metadata, partition error, and top-level error"
        );
    }
}
