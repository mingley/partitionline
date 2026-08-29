//! Consumer group codecs: FindCoordinator, Join/Sync/Heartbeat/Leave,
//! OffsetCommit/OffsetFetch, OffsetDelete, and ConsumerProtocol assignment.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// FindCoordinator `key_type` for a consumer group.
pub const COORDINATOR_GROUP: i8 = 0;
/// FindCoordinator `key_type` for a transactional.id (KIP-98).
pub const COORDINATOR_TRANSACTION: i8 = 1;
/// FindCoordinator `key_type` for a share group (KIP-932).
pub const COORDINATOR_SHARE: i8 = 2;

/// Encode FindCoordinator for a consumer group id.
pub fn encode_find_coordinator_request(buf: &mut BytesMut, key: &str) -> crate::error::Result<()> {
    encode_find_coordinator_request_typed(buf, key, COORDINATOR_GROUP)
}

/// Encode FindCoordinator with an explicit `key_type`.
pub fn encode_find_coordinator_request_typed(
    buf: &mut BytesMut,
    key: &str,
    key_type: i8,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(key))?;
    buf.put_i8(key_type);
    Ok(())
}

/// Decode FindCoordinator: `(key, key_type)`.
pub fn decode_find_coordinator_request<B: Buf>(buf: &mut B) -> Result<(String, i8)> {
    let key = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let key_type = buf::get_i8(buf)?;
    Ok((key, key_type))
}

/// Encode FindCoordinator: node, host, port (error `0`).
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

/// Decode FindCoordinator: `(error_code, node_id, host, port)`.
pub fn decode_find_coordinator_response<B: Buf>(buf: &mut B) -> Result<(i16, i32, String, i32)> {
    let _throttle = buf::get_i32(buf)?;
    let error = buf::get_i16(buf)?;
    let _msg = buf::get_classic_nullable_string(buf)?;
    let node_id = buf::get_i32(buf)?;
    let host = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let port = buf::get_i32(buf)?;
    Ok((error, node_id, host, port))
}

/// JoinGroup request (classic v5–7 shape this crate speaks).
#[derive(Debug, Clone, Copy)]
pub struct JoinGroupRequest<'a> {
    /// Group id.
    pub group_id: &'a str,
    /// Session timeout.
    pub session_timeout_ms: i32,
    /// Member id (`""` on first join).
    pub member_id: &'a str,
    /// Kafka `group.instance.id`.
    pub group_instance_id: Option<&'a str>,
    /// Protocol type (`"consumer"`).
    pub protocol_type: &'a str,
    /// Protocol name (`"range"`, `"sticky"`, `"cooperative-sticky"`).
    pub protocol_name: &'a str,
    /// Subscription metadata bytes.
    pub metadata: &'a [u8],
}

/// Encode JoinGroup.
pub fn encode_join_group_request(
    buf: &mut BytesMut,
    req: &JoinGroupRequest<'_>,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(req.group_id))?;
    buf.put_i32(req.session_timeout_ms);
    buf.put_i32(req.session_timeout_ms); // rebalance timeout
    buf::put_classic_nullable_string(buf, Some(req.member_id))?;
    buf::put_classic_nullable_string(buf, req.group_instance_id)?;
    buf::put_classic_nullable_string(buf, Some(req.protocol_type))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(req.protocol_name))?;
    buf::put_classic_bytes(buf, Some(req.metadata))?;
    Ok(())
}

/// Decode JoinGroup: `(group_id, member_id, instance_id, metadata)`.
pub fn decode_join_group_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, String, Option<String>, Vec<u8>)> {
    let group_id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _session = buf::get_i32(buf)?;
    let _rebalance = buf::get_i32(buf)?;
    let member_id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let instance = buf::get_classic_nullable_string(buf)?;
    let _ptype = buf::get_classic_nullable_string(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut metadata = Vec::new();
    for _ in 0..n {
        let _name = buf::get_classic_nullable_string(buf)?;
        metadata = buf::get_classic_bytes(buf)?.unwrap_or_default();
    }
    Ok((group_id, member_id, instance, metadata))
}

/// One member in a JoinGroup response (leader sees all).
#[derive(Debug, Clone)]
pub struct JoinMember {
    /// Member id.
    pub member_id: String,
    /// Subscription metadata bytes.
    pub metadata: Vec<u8>,
}

/// Encode JoinGroup: generation, protocol, leader, members.
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

/// Decode JoinGroup: `(error, generation, protocol, leader, member_id, members)`.
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

/// Encode SyncGroup with member assignments (`member_id` → assignment bytes).
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
/// Decode SyncGroup: `(group_id, member_id, assignments)`.
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

/// Encode SyncGroup: error plus this member's assignment bytes.
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

/// Decode SyncGroup: `(error_code, assignment)`.
pub fn decode_sync_group_response<B: Buf>(buf: &mut B) -> Result<(i16, Vec<u8>)> {
    let _throttle = buf::get_i32(buf)?;
    let error = buf::get_i16(buf)?;
    let assignment = buf::get_classic_bytes(buf)?.unwrap_or_default();
    Ok((error, assignment))
}

/// Encode Heartbeat.
pub fn encode_heartbeat_request(
    buf: &mut BytesMut,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf.put_i32(generation_id);
    buf::put_classic_nullable_string(buf, Some(member_id))?;
    buf::put_classic_nullable_string(buf, group_instance_id)?;
    Ok(())
}

/// Decode Heartbeat: `(group_id, generation_id, member_id)`.
pub fn decode_heartbeat_request<B: Buf>(buf: &mut B) -> Result<(String, i32, String)> {
    let g = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let gen = buf::get_i32(buf)?;
    let m = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    Ok((g, gen, m))
}

/// Encode Heartbeat: throttle `0` plus error code.
pub fn encode_heartbeat_response(buf: &mut BytesMut, error_code: i16) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(error_code);
    Ok(())
}

/// Decode Heartbeat: error code.
pub fn decode_heartbeat_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _throttle = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

/// Encode LeaveGroup v0 (group id + one member id).
pub fn encode_leave_group_request(
    buf: &mut BytesMut,
    group_id: &str,
    member_id: &str,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf::put_classic_nullable_string(buf, Some(member_id))?;
    Ok(())
}

/// One member in LeaveGroup v3+ (KIP-345). `reason` is v5+ (KIP-800).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupMember {
    /// Kafka member id, or empty when removing by [`Self::group_instance_id`].
    pub member_id: String,
    /// Kafka `group.instance.id`, when present.
    pub group_instance_id: Option<String>,
    /// Why the member left (LeaveGroup v5+). `None` is a null reason.
    pub reason: Option<String>,
}

/// Per-member LeaveGroup v3+ result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupMemberResult {
    /// Kafka member id.
    pub member_id: String,
    /// Kafka `group.instance.id`, when present.
    pub group_instance_id: Option<String>,
    /// Per-member error code (`0` is success).
    pub error_code: i16,
}

/// `true` when LeaveGroup `version` is flexible (v4+).
///
/// v0–v3 are classic. v4 is compact arrays/strings plus tagged fields
/// (Apache JSON `flexibleVersions: "4+"`). v5 adds per-member `Reason`
/// (KIP-800).
fn leave_group_flexible(version: i16) -> Result<bool> {
    match version {
        0..=3 => Ok(false),
        4 | 5 => Ok(true),
        other => Err(Error::protocol(format!(
            "LeaveGroup version {other} is not implemented"
        ))),
    }
}

/// Encode LeaveGroup v3 (classic), v4 (flexible), or v5 (Reason).
pub fn encode_leave_group_request_members(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    members: &[LeaveGroupMember],
) -> crate::error::Result<()> {
    if !(3..=5).contains(&version) {
        return Err(Error::protocol("LeaveGroup members require version 3–5"));
    }
    let flexible = leave_group_flexible(version)?;
    buf::put_string(buf, flexible, Some(group_id))?;
    buf::put_array_len(buf, flexible, Some(members.len()))?;
    for m in members {
        buf::put_string(buf, flexible, Some(&m.member_id))?;
        buf::put_string(buf, flexible, m.group_instance_id.as_deref())?;
        if version >= 5 {
            buf::put_string(buf, flexible, m.reason.as_deref())?;
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

/// Decode LeaveGroup: `(group_id, member_id)` (v0 body).
pub fn decode_leave_group_request<B: Buf>(buf: &mut B) -> Result<(String, String)> {
    let (g, members) = decode_leave_group_request_version(buf, 0)?;
    Ok((
        g,
        members
            .into_iter()
            .next()
            .map(|m| m.member_id)
            .unwrap_or_default(),
    ))
}

/// Decode LeaveGroup v0–v5: `(group_id, members)`.
pub fn decode_leave_group_request_version<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, Vec<LeaveGroupMember>)> {
    let flexible = leave_group_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    if version >= 3 {
        let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut members = Vec::with_capacity(n);
        for _ in 0..n {
            let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
            let group_instance_id = buf::get_string(buf, flexible)?;
            let reason = if version >= 5 {
                buf::get_string(buf, flexible)?
            } else {
                None
            };
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            members.push(LeaveGroupMember {
                member_id,
                group_instance_id,
                reason,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        return Ok((group_id, members));
    }
    let member_id = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let group_instance_id = if version >= 2 {
        buf::get_classic_nullable_string(buf)?
    } else {
        None
    };
    Ok((
        group_id,
        vec![LeaveGroupMember {
            member_id,
            group_instance_id,
            reason: None,
        }],
    ))
}

/// Encode LeaveGroup v0: error code only.
pub fn encode_leave_group_response(
    buf: &mut BytesMut,
    error_code: i16,
) -> crate::error::Result<()> {
    encode_leave_group_response_version(buf, 0, error_code, &[])
}

/// Encode LeaveGroup v0–v5. Throttle is `0` on v1+. Members are v3+.
/// v4+ is flexible. v5 response body matches v4.
pub fn encode_leave_group_response_version(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    members: &[LeaveGroupMemberResult],
) -> crate::error::Result<()> {
    let flexible = leave_group_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf.put_i16(error_code);
    if version >= 3 {
        buf::put_array_len(buf, flexible, Some(members.len()))?;
        for m in members {
            buf::put_string(buf, flexible, Some(&m.member_id))?;
            buf::put_string(buf, flexible, m.group_instance_id.as_deref())?;
            buf.put_i16(m.error_code);
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

/// Decode LeaveGroup v0: error code.
pub fn decode_leave_group_response<B: Buf>(buf: &mut B) -> Result<i16> {
    Ok(decode_leave_group_response_version(buf, 0)?.0)
}

/// Decode LeaveGroup v0–v5: `(error_code, members)`. Members are empty below v3.
pub fn decode_leave_group_response_version<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, Vec<LeaveGroupMemberResult>)> {
    let flexible = leave_group_flexible(version)?;
    if version >= 1 {
        let _throttle = buf::get_i32(buf)?;
    }
    let error_code = buf::get_i16(buf)?;
    let mut members = Vec::new();
    if version >= 3 {
        let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        for _ in 0..n {
            let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
            let group_instance_id = buf::get_string(buf, flexible)?;
            let err = buf::get_i16(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            members.push(LeaveGroupMemberResult {
                member_id,
                group_instance_id,
                error_code: err,
            });
        }
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((error_code, members))
}

/// One partition in OffsetCommit v7 / OffsetFetch v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetPartition {
    /// Partition index.
    pub partition: i32,
    /// Committed offset.
    pub offset: i64,
    /// Leader epoch, or `-1`.
    pub leader_epoch: i32,
    /// Commit metadata string.
    pub metadata: String,
}

impl OffsetPartition {
    /// Offset and partition with unknown epoch and empty metadata.
    #[must_use]
    pub fn new(partition: i32, offset: i64) -> Self {
        Self {
            partition,
            offset,
            leader_epoch: -1,
            metadata: String::new(),
        }
    }
}

/// Topic + partitions for OffsetCommit v7.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions to commit.
    pub partitions: Vec<OffsetPartition>,
}

/// Topic + partition indexes for OffsetFetch v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to fetch.
    pub partitions: Vec<i32>,
}

/// One partition in an OffsetFetch v5 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedOffset {
    /// Partition index.
    pub partition: i32,
    /// Committed offset, or `-1` when none.
    pub offset: i64,
    /// Leader epoch, or `-1`.
    pub leader_epoch: i32,
    /// Commit metadata string.
    pub metadata: String,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl FetchedOffset {
    /// Offset with unknown epoch and empty metadata.
    #[must_use]
    pub fn new(partition: i32, offset: i64, error_code: i16) -> Self {
        Self {
            partition,
            offset,
            leader_epoch: -1,
            metadata: String::new(),
            error_code,
        }
    }
}

/// Topic + committed offsets from OffsetFetch v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedOffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions in this topic.
    pub partitions: Vec<FetchedOffset>,
}

/// Encode OffsetCommit v7 (leader epoch + metadata).
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
            buf.put_i32(p.leader_epoch);
            let meta = if p.metadata.is_empty() {
                None
            } else {
                Some(p.metadata.as_str())
            };
            buf::put_classic_nullable_string(buf, meta)?;
        }
    }
    Ok(())
}

/// Decode OffsetCommit: `(group_id, generation_id, member_id, topics)`.
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
            let leader_epoch = buf::get_i32(buf)?;
            let metadata = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
            partitions.push(OffsetPartition {
                partition,
                offset,
                leader_epoch,
                metadata,
            });
        }
        topics.push(OffsetTopic { topic, partitions });
    }
    Ok((group, member, topics))
}

/// Encode OffsetCommit: one error code applied to every partition.
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

/// Decode OffsetCommit: first non-zero partition error, or `0`.
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

/// Encode OffsetFetch v5.
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

/// Decode OffsetFetch: `(group_id, topics)`.
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

/// Encode OffsetFetch v5.
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
            buf.put_i32(p.leader_epoch);
            let meta = if p.metadata.is_empty() {
                None
            } else {
                Some(p.metadata.as_str())
            };
            buf::put_classic_nullable_string(buf, meta)?;
            buf.put_i16(p.error_code);
        }
    }
    buf.put_i16(0); // top-level error
    Ok(())
}

/// Decode OffsetFetch. Top-level error is [`crate::error::Error::Broker`].
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
            let leader_epoch = buf::get_i32(buf)?;
            let metadata = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
            let error_code = buf::get_i16(buf)?;
            partitions.push(FetchedOffset {
                partition,
                offset,
                leader_epoch,
                metadata,
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

/// Topic + partitions for OffsetDelete (api 47) v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to delete.
    pub partitions: Vec<i32>,
}

/// One partition result from OffsetDelete (api 47) v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteResult {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

/// OffsetDelete v0 (classic; error_code is *before* throttle).
pub fn encode_offset_delete_request(
    buf: &mut BytesMut,
    group_id: &str,
    topics: &[OffsetDeleteTopic],
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

/// Decode OffsetDelete: `(group_id, topics)`.
pub fn decode_offset_delete_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, Vec<OffsetDeleteTopic>)> {
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
        topics.push(OffsetDeleteTopic { topic, partitions });
    }
    Ok((group, topics))
}

/// Encode OffsetDelete (error code before throttle).
pub fn encode_offset_delete_response(
    buf: &mut BytesMut,
    error_code: i16,
    results: &[OffsetDeleteResult],
) -> crate::error::Result<()> {
    buf.put_i16(error_code);
    buf.put_i32(0);
    let mut by_topic: std::collections::HashMap<String, Vec<(i32, i16)>> =
        std::collections::HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for r in results {
        match by_topic.entry(r.topic.clone()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                order.push(r.topic.clone());
                let _ = slot.insert(vec![(r.partition, r.error_code)]);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                slot.get_mut().push((r.partition, r.error_code));
            }
        }
    }
    buf::put_array_len(buf, false, Some(order.len()))?;
    for name in &order {
        let parts = by_topic.get(name).map(Vec::as_slice).unwrap_or(&[]);
        buf::put_classic_nullable_string(buf, Some(name))?;
        buf::put_array_len(buf, false, Some(parts.len()))?;
        for (partition, err) in parts {
            buf.put_i32(*partition);
            buf.put_i16(*err);
        }
    }
    Ok(())
}

/// Decode OffsetDelete: `(error_code, results)`.
pub fn decode_offset_delete_response<B: Buf>(
    buf: &mut B,
) -> Result<(i16, Vec<OffsetDeleteResult>)> {
    let error_code = buf::get_i16(buf)?;
    let _throttle = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::new();
    for _ in 0..n {
        let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let part_err = buf::get_i16(buf)?;
            out.push(OffsetDeleteResult {
                topic: topic.clone(),
                partition,
                error_code: part_err,
            });
        }
    }
    Ok((error_code, out))
}

/// ConsumerProtocol subscription v0 (topics only).
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

/// ConsumerProtocol subscription v1: topics plus currently owned partitions
/// (KIP-429 cooperative / sticky).
pub fn encode_subscription_owned(topics: &[String], owned: &[(String, i32)]) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    buf::put_array_len(&mut buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(&mut buf, Some(t))?;
    }
    buf::put_classic_bytes(&mut buf, None)?;
    let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
    for (topic, part) in owned {
        match by_topic.iter_mut().find(|(t, _)| t == topic) {
            Some((_, ps)) => ps.push(*part),
            None => by_topic.push((topic.clone(), vec![*part])),
        }
    }
    buf::put_array_len(&mut buf, false, Some(by_topic.len()))?;
    for (topic, parts) in &by_topic {
        buf::put_classic_nullable_string(&mut buf, Some(topic))?;
        buf::put_array_len(&mut buf, false, Some(parts.len()))?;
        for p in parts {
            buf.put_i32(*p);
        }
    }
    Ok(buf.to_vec())
}

/// Decode ConsumerProtocol subscription topics (v0 or v1, owned partitions ignored).
pub fn decode_subscription(bytes: &[u8]) -> Result<Vec<String>> {
    Ok(decode_subscription_owned(bytes)?.0)
}

/// Topics plus owned `(topic, partition)` pairs from ConsumerProtocol subscription metadata.
///
/// v0 metadata yields an empty owned list.
#[expect(
    clippy::type_complexity,
    reason = "subscription is topics plus owned topic-partitions"
)]
pub fn decode_subscription_owned(mut bytes: &[u8]) -> Result<(Vec<String>, Vec<(String, i32)>)> {
    if bytes.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let ver = buf::get_i16(&mut bytes)?;
    let n = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        topics.push(buf::get_classic_nullable_string(&mut bytes)?.unwrap_or_default());
    }
    let mut owned = Vec::new();
    if bytes.is_empty() {
        return Ok((topics, owned));
    }
    let _user = buf::get_classic_bytes(&mut bytes)?;
    if ver >= 1 && !bytes.is_empty() {
        let tn = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
        for _ in 0..tn {
            let topic = buf::get_classic_nullable_string(&mut bytes)?.unwrap_or_default();
            let pn = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
            for _ in 0..pn {
                owned.push((topic.clone(), buf::get_i32(&mut bytes)?));
            }
        }
    }
    Ok((topics, owned))
}

/// ConsumerProtocol assignment v0 for one topic.
pub fn encode_assignment(topic: &str, partitions: &[i32]) -> Result<Vec<u8>> {
    encode_owned_assignment(&[(topic.to_string(), partitions.to_vec())])
}

/// ConsumerProtocol assignment v0 for one member, several topics.
pub fn encode_owned_assignment(topics: &[(String, Vec<i32>)]) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_i16(0);
    buf::put_array_len(&mut buf, false, Some(topics.len()))?;
    for (topic, partitions) in topics {
        buf::put_classic_nullable_string(&mut buf, Some(topic))?;
        buf::put_array_len(&mut buf, false, Some(partitions.len()))?;
        for p in partitions {
            buf.put_i32(*p);
        }
    }
    buf.put_i32(-1);
    Ok(buf.to_vec())
}

/// Group `(topic, partition)` pairs by topic, preserving first-seen order.
pub fn encode_tp_assignment(parts: &[(String, i32)]) -> Result<Vec<u8>> {
    let mut topics: Vec<(String, Vec<i32>)> = Vec::new();
    for (topic, part) in parts {
        match topics.iter_mut().find(|(t, _)| t == topic) {
            Some((_, ps)) => ps.push(*part),
            None => topics.push((topic.clone(), vec![*part])),
        }
    }
    encode_owned_assignment(&topics)
}

/// Decode ConsumerProtocol assignment: `(topic, partitions)` per topic.
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
        let owned = encode_subscription_owned(
            &["t".into(), "u".into()],
            &[("t".into(), 0), ("u".into(), 1), ("t".into(), 2)],
        )
        .unwrap();
        let (topics, tps) = decode_subscription_owned(&owned).unwrap();
        assert_eq!(topics, vec!["t".to_string(), "u".to_string()]);
        assert_eq!(tps, vec![("t".into(), 0), ("t".into(), 2), ("u".into(), 1)]);
        assert_eq!(decode_subscription(&owned).unwrap(), topics);
        let asg = encode_assignment("t", &[0, 1]).unwrap();
        let decoded = decode_assignment(&asg).unwrap();
        assert_eq!(decoded[0].0, "t");
        assert_eq!(decoded[0].1, vec![0, 1]);
        let multi =
            encode_tp_assignment(&[("a".into(), 0), ("b".into(), 1), ("a".into(), 2)]).unwrap();
        let decoded = decode_assignment(&multi).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], ("a".into(), vec![0, 2]));
        assert_eq!(decoded[1], ("b".into(), vec![1]));
    }

    #[test]
    fn join_group_v5_sends_instance_id() {
        let mut buf = BytesMut::new();
        encode_join_group_request(
            &mut buf,
            &JoinGroupRequest {
                group_id: "g",
                session_timeout_ms: 10_000,
                member_id: "m1",
                group_instance_id: Some("worker-1"),
                protocol_type: "consumer",
                protocol_name: "range",
                metadata: &[1, 2, 3],
            },
        )
        .unwrap();
        let (gid, member, instance, meta) = decode_join_group_request(&mut &buf[..]).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(member, "m1");
        assert_eq!(instance.as_deref(), Some("worker-1"));
        assert_eq!(meta, vec![1, 2, 3]);
    }

    #[test]
    fn offset_commit_v7_batches_partitions_and_consumes_epoch_metadata() {
        let topics = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![
                OffsetPartition {
                    partition: 0,
                    offset: 3,
                    leader_epoch: 4,
                    metadata: "ckpt".into(),
                },
                OffsetPartition::new(2, 9),
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
            partitions: vec![OffsetPartition::new(0, 1), OffsetPartition::new(1, 2)],
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
                    leader_epoch: 2,
                    metadata: "m".into(),
                    error_code: 0,
                },
                FetchedOffset::new(1, -1, 0),
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

    #[test]
    fn offset_delete_v0_roundtrip_is_leftover_empty() {
        let topics = vec![OffsetDeleteTopic {
            topic: "t".into(),
            partitions: vec![0, 1],
        }];
        let mut buf = BytesMut::new();
        encode_offset_delete_request(&mut buf, "g", &topics).unwrap();
        let mut cur = &buf[..];
        let (gid, got) = decode_offset_delete_request(&mut cur).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(got, topics);
        assert!(
            cur.is_empty(),
            "OffsetDelete v0 request must be leftover-empty"
        );

        let results = vec![
            OffsetDeleteResult {
                topic: "t".into(),
                partition: 0,
                error_code: 0,
            },
            OffsetDeleteResult {
                topic: "t".into(),
                partition: 1,
                error_code: 0,
            },
        ];
        buf.clear();
        encode_offset_delete_response(&mut buf, 0, &results).unwrap();
        let mut cur = &buf[..];
        let (err, decoded) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(err, 0);
        assert_eq!(decoded, results);
        assert!(
            cur.is_empty(),
            "OffsetDelete v0 response must be leftover-empty"
        );
    }

    #[test]
    fn offset_delete_not_coordinator_is_not_at_byte_four() {
        let mut buf = BytesMut::new();
        encode_offset_delete_response(&mut buf, crate::error::NOT_COORDINATOR, &[]).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "error is at bytes 0-1; throttle occupies bytes 2-5"
        );
        let mut cur = &buf[..];
        let (err, results) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(err, crate::error::NOT_COORDINATOR);
        assert!(results.is_empty());
        assert!(
            !cur.has_remaining(),
            "OffsetDelete v0 NOT_COORDINATOR must be leftover-empty"
        );
    }

    #[test]
    fn leave_group_v3_roundtrip_is_leftover_empty() {
        let members = vec![LeaveGroupMember {
            member_id: String::new(),
            group_instance_id: Some("worker-1".into()),
            reason: None,
        }];
        let mut buf = BytesMut::new();
        encode_leave_group_request_members(&mut buf, 3, "g-rm", &members).unwrap();
        let mut cur = &buf[..];
        let (gid, got) = decode_leave_group_request_version(&mut cur, 3).unwrap();
        assert_eq!(gid, "g-rm");
        assert_eq!(got, members);
        assert!(
            cur.is_empty(),
            "LeaveGroup v3 request leftover {} bytes",
            cur.len()
        );

        let results = vec![LeaveGroupMemberResult {
            member_id: String::new(),
            group_instance_id: Some("worker-1".into()),
            error_code: 0,
        }];
        buf.clear();
        encode_leave_group_response_version(&mut buf, 3, 0, &results).unwrap();
        let mut cur = &buf[..];
        let (err, decoded) = decode_leave_group_response_version(&mut cur, 3).unwrap();
        assert_eq!(err, 0);
        assert_eq!(decoded, results);
        assert!(
            cur.is_empty(),
            "LeaveGroup v3 response leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn leave_group_v4_roundtrip_is_leftover_empty() {
        let members = vec![LeaveGroupMember {
            member_id: String::new(),
            group_instance_id: Some("worker-1".into()),
            reason: Some("ignored-on-v4".into()),
        }];
        let mut buf = BytesMut::new();
        encode_leave_group_request_members(&mut buf, 4, "g-rm", &members).unwrap();
        let mut cur = &buf[..];
        let (gid, got) = decode_leave_group_request_version(&mut cur, 4).unwrap();
        assert_eq!(gid, "g-rm");
        assert_eq!(got[0].member_id, "");
        assert_eq!(got[0].group_instance_id.as_deref(), Some("worker-1"));
        assert_eq!(got[0].reason, None, "v4 must not write Reason");
        assert!(
            cur.is_empty(),
            "LeaveGroup v4 request leftover {} bytes",
            cur.len()
        );

        let results = vec![LeaveGroupMemberResult {
            member_id: String::new(),
            group_instance_id: Some("worker-1".into()),
            error_code: 0,
        }];
        buf.clear();
        encode_leave_group_response_version(&mut buf, 4, 0, &results).unwrap();
        let mut cur = &buf[..];
        let (err, decoded) = decode_leave_group_response_version(&mut cur, 4).unwrap();
        assert_eq!(err, 0);
        assert_eq!(decoded, results);
        assert!(
            cur.is_empty(),
            "LeaveGroup v4 response leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn leave_group_v5_reason_roundtrip_is_leftover_empty() {
        let members = vec![LeaveGroupMember {
            member_id: String::new(),
            group_instance_id: Some("worker-1".into()),
            reason: Some("member was removed by an admin".into()),
        }];
        let mut buf = BytesMut::new();
        encode_leave_group_request_members(&mut buf, 5, "g-rm", &members).unwrap();
        let mut cur = &buf[..];
        let (gid, got) = decode_leave_group_request_version(&mut cur, 5).unwrap();
        assert_eq!(gid, "g-rm");
        assert_eq!(got, members);
        assert!(
            cur.is_empty(),
            "LeaveGroup v5 request leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_leave_group_response_version(&mut buf, 5, 0, &[]).unwrap();
        let mut cur = &buf[..];
        let (err, decoded) = decode_leave_group_response_version(&mut cur, 5).unwrap();
        assert_eq!(err, 0);
        assert!(decoded.is_empty());
        assert!(
            cur.is_empty(),
            "LeaveGroup v5 response leftover {} bytes",
            cur.len()
        );
        buf.clear();
        assert!(
            encode_leave_group_request_members(&mut buf, 6, "g-rm", &members).is_err(),
            "LeaveGroup v6 is not spoken"
        );
    }

    #[test]
    fn leave_group_v5_abort_matches_compact_layout() {
        // Compact GroupId "g-rm", one MemberIdentity { empty MemberId,
        // GroupInstanceId "worker-1", Reason "member was removed by an admin",
        // tagged }, tagged.
        const REQ: &[u8] = &[
            0x05, 0x67, 0x2d, 0x72, 0x6d, 0x02, 0x01, 0x09, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72,
            0x2d, 0x31, 0x1f, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x20, 0x77, 0x61, 0x73, 0x20,
            0x72, 0x65, 0x6d, 0x6f, 0x76, 0x65, 0x64, 0x20, 0x62, 0x79, 0x20, 0x61, 0x6e, 0x20,
            0x61, 0x64, 0x6d, 0x69, 0x6e, 0x00, 0x00,
        ];
        let members = vec![LeaveGroupMember {
            member_id: String::new(),
            group_instance_id: Some("worker-1".into()),
            reason: Some("member was removed by an admin".into()),
        }];
        let mut buf = BytesMut::new();
        encode_leave_group_request_members(&mut buf, 5, "g-rm", &members).unwrap();
        assert_eq!(&buf[..], REQ);
    }
}
