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

/// `true` when FindCoordinator `version` is flexible (v3+).
///
/// v1–v2 are classic (Key + KeyType). v3 is compact strings plus tagged
/// fields (Apache JSON `flexibleVersions: "3+"`). v4–v6 replace Key with
/// CoordinatorKeys and the top-level coordinator fields with Coordinators
/// (KIP-699). v5 is TRANSACTION_ABORTABLE (KIP-890). v6 is share groups
/// (KIP-932). Kafka 4.0 `validVersions` is `0-6`. v0 (no KeyType) and
/// v7+ are not spoken.
fn find_coordinator_flexible(version: i16) -> Result<bool> {
    match version {
        1..=2 => Ok(false),
        3..=6 => Ok(true),
        other => Err(Error::protocol(format!(
            "FindCoordinator version {other} is not implemented"
        ))),
    }
}

fn find_coordinator_batched(version: i16) -> bool {
    version >= 4
}

/// Encode FindCoordinator for a consumer group id.
pub fn encode_find_coordinator_request(
    buf: &mut BytesMut,
    version: i16,
    key: &str,
) -> crate::error::Result<()> {
    encode_find_coordinator_request_typed(buf, version, key, COORDINATOR_GROUP)
}

/// Encode FindCoordinator with an explicit `key_type`.
pub fn encode_find_coordinator_request_typed(
    buf: &mut BytesMut,
    version: i16,
    key: &str,
    key_type: i8,
) -> crate::error::Result<()> {
    let flexible = find_coordinator_flexible(version)?;
    if find_coordinator_batched(version) {
        buf.put_i8(key_type);
        buf::put_array_len(buf, true, Some(1))?;
        buf::put_compact_string(buf, Some(key))?;
        buf::put_empty_tagged_fields(buf);
        return Ok(());
    }
    buf::put_string(buf, flexible, Some(key))?;
    buf.put_i8(key_type);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode FindCoordinator: `(key, key_type)`.
pub fn decode_find_coordinator_request<B: Buf>(buf: &mut B, version: i16) -> Result<(String, i8)> {
    let flexible = find_coordinator_flexible(version)?;
    if find_coordinator_batched(version) {
        let key_type = buf::get_i8(buf)?;
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut key = String::new();
        let mut first = true;
        for _ in 0..n {
            let next = buf::get_compact_string(buf)?.unwrap_or_default();
            if first {
                key = next;
                first = false;
            }
        }
        buf::skip_tagged_fields(buf)?;
        return Ok((key, key_type));
    }
    let key = buf::get_string(buf, flexible)?.unwrap_or_default();
    let key_type = buf::get_i8(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((key, key_type))
}

/// Encode FindCoordinator: node, host, port (error `0`).
///
/// `key` is the coordinator key. v1–v3 omit it (top-level NodeId / Host /
/// Port). v4+ writes it as `Coordinators[].Key` (KIP-699).
pub fn encode_find_coordinator_response(
    buf: &mut BytesMut,
    version: i16,
    node_id: i32,
    host: &str,
    port: i32,
    key: &str,
) -> crate::error::Result<()> {
    let flexible = find_coordinator_flexible(version)?;
    buf.put_i32(0);
    if find_coordinator_batched(version) {
        buf::put_array_len(buf, true, Some(1))?;
        buf::put_compact_string(buf, Some(key))?;
        buf.put_i32(node_id);
        buf::put_compact_string(buf, Some(host))?;
        buf.put_i32(port);
        buf.put_i16(0);
        buf::put_compact_string(buf, None)?;
        buf::put_empty_tagged_fields(buf);
        buf::put_empty_tagged_fields(buf);
        return Ok(());
    }
    buf.put_i16(0);
    buf::put_string(buf, flexible, None)?;
    buf.put_i32(node_id);
    buf::put_string(buf, flexible, Some(host))?;
    buf.put_i32(port);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode FindCoordinator: `(error_code, node_id, host, port)`.
pub fn decode_find_coordinator_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i32, String, i32)> {
    let flexible = find_coordinator_flexible(version)?;
    let _throttle = buf::get_i32(buf)?;
    if find_coordinator_batched(version) {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut first = None;
        for _ in 0..n {
            let _key = buf::get_compact_string(buf)?;
            let node_id = buf::get_i32(buf)?;
            let host = buf::get_compact_string(buf)?.unwrap_or_default();
            let port = buf::get_i32(buf)?;
            let error = buf::get_i16(buf)?;
            let _msg = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            if first.is_none() {
                first = Some((error, node_id, host, port));
            }
        }
        buf::skip_tagged_fields(buf)?;
        return first.ok_or_else(|| Error::protocol("missing FindCoordinator Coordinators"));
    }
    let error = buf::get_i16(buf)?;
    let _msg = buf::get_string(buf, flexible)?;
    let node_id = buf::get_i32(buf)?;
    let host = buf::get_string(buf, flexible)?.unwrap_or_default();
    let port = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((error, node_id, host, port))
}

/// `true` when JoinGroup `version` is flexible (v6+).
///
/// v5 is classic (GroupInstanceId). v6 is compact strings/bytes/arrays
/// plus tagged fields (Apache JSON `flexibleVersions: "6+"`). v7 is the
/// same request as v6; the response adds ProtocolType (KIP-559) and
/// nullable ProtocolName. v8 adds Reason (KIP-800). v9 adds
/// SkipAssignment on the response. Kafka 4.0 `validVersions` is `2-9`.
/// This crate speaks 5–9. v2–v4 (no instance id) and v10+ are not spoken.
fn join_group_flexible(version: i16) -> Result<bool> {
    match version {
        5 => Ok(false),
        6..=9 => Ok(true),
        other => Err(Error::protocol(format!(
            "JoinGroup version {other} is not implemented"
        ))),
    }
}

/// JoinGroup request (classic v5 or flexible v6–v9).
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
    /// Why the member (re-)joins (v8+, KIP-800). `None` is a null reason.
    pub reason: Option<&'a str>,
}

/// Encode JoinGroup v5 (classic) or v6–v9 (flexible).
pub fn encode_join_group_request(
    buf: &mut BytesMut,
    version: i16,
    req: &JoinGroupRequest<'_>,
) -> crate::error::Result<()> {
    let flexible = join_group_flexible(version)?;
    buf::put_string(buf, flexible, Some(req.group_id))?;
    buf.put_i32(req.session_timeout_ms);
    buf.put_i32(req.session_timeout_ms); // rebalance timeout
    buf::put_string(buf, flexible, Some(req.member_id))?;
    buf::put_string(buf, flexible, req.group_instance_id)?;
    buf::put_string(buf, flexible, Some(req.protocol_type))?;
    buf::put_array_len(buf, flexible, Some(1))?;
    buf::put_string(buf, flexible, Some(req.protocol_name))?;
    buf::put_bytes(buf, flexible, Some(req.metadata))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    if version >= 8 {
        buf::put_string(buf, true, req.reason)?;
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode JoinGroup: `(group_id, member_id, instance_id, metadata)`.
pub fn decode_join_group_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, Option<String>, Vec<u8>)> {
    let flexible = join_group_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _session = buf::get_i32(buf)?;
    let _rebalance = buf::get_i32(buf)?;
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let instance = buf::get_string(buf, flexible)?;
    let _ptype = buf::get_string(buf, flexible)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut metadata = Vec::new();
    for _ in 0..n {
        let _name = buf::get_string(buf, flexible)?;
        metadata = buf::get_bytes(buf, flexible)?.unwrap_or_default();
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    if version >= 8 {
        let _reason = buf::get_string(buf, true)?;
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
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
#[expect(
    clippy::too_many_arguments,
    reason = "JoinGroup response fields match the Apache JSON layout"
)]
pub fn encode_join_group_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    generation_id: i32,
    protocol_name: &str,
    leader: &str,
    member_id: &str,
    members: &[JoinMember],
) -> crate::error::Result<()> {
    let flexible = join_group_flexible(version)?;
    buf.put_i32(0);
    buf.put_i16(error_code);
    buf.put_i32(generation_id);
    if version >= 7 {
        buf::put_string(buf, true, None)?;
    }
    buf::put_string(buf, flexible, Some(protocol_name))?;
    buf::put_string(buf, flexible, Some(leader))?;
    if version >= 9 {
        buf.put_u8(0);
    }
    buf::put_string(buf, flexible, Some(member_id))?;
    buf::put_array_len(buf, flexible, Some(members.len()))?;
    for m in members {
        buf::put_string(buf, flexible, Some(&m.member_id))?;
        buf::put_string(buf, flexible, None)?;
        buf::put_bytes(buf, flexible, Some(&m.metadata))?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode JoinGroup: `(error, generation, protocol, leader, member_id, skip_assignment, members)`.
#[expect(
    clippy::type_complexity,
    reason = "JoinGroup response is error, generation, protocol, leader, member, skip, members"
)]
pub fn decode_join_group_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i32, String, String, String, bool, Vec<JoinMember>)> {
    let flexible = join_group_flexible(version)?;
    let _throttle = buf::get_i32(buf)?;
    let error = buf::get_i16(buf)?;
    let generation = buf::get_i32(buf)?;
    if version >= 7 {
        let _ptype = buf::get_string(buf, true)?;
    }
    let protocol = buf::get_string(buf, flexible)?.unwrap_or_default();
    let leader = buf::get_string(buf, flexible)?.unwrap_or_default();
    let skip_assignment = if version >= 9 {
        buf::get_bool(buf)?
    } else {
        false
    };
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut members = Vec::with_capacity(n);
    for _ in 0..n {
        let mid = buf::get_string(buf, flexible)?.unwrap_or_default();
        let _inst = buf::get_string(buf, flexible)?;
        let metadata = buf::get_bytes(buf, flexible)?.unwrap_or_default();
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        members.push(JoinMember {
            member_id: mid,
            metadata,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((
        error,
        generation,
        protocol,
        leader,
        member_id,
        skip_assignment,
        members,
    ))
}

/// `true` when SyncGroup `version` is flexible (v4+).
///
/// v0–v3 are classic. v4 is compact strings/bytes/arrays plus tagged
/// fields (Apache JSON `flexibleVersions: "4+"`). v5 adds ProtocolType /
/// ProtocolName (KIP-559). Kafka 4.0 `validVersions` is `0-5`. Official
/// JSON: v1 and v2 match v0. v1+ ThrottleTimeMs. v3 GroupInstanceId.
/// This crate speaks 0–5. v6+ is not spoken.
fn sync_group_flexible(version: i16) -> Result<bool> {
    match version {
        0..=3 => Ok(false),
        4..=5 => Ok(true),
        other => Err(Error::protocol(format!(
            "SyncGroup version {other} is not implemented"
        ))),
    }
}

/// SyncGroup request (classic v0–v3 or flexible v4–v5).
#[derive(Debug, Clone, Copy)]
pub struct SyncGroupRequest<'a> {
    /// Group id.
    pub group_id: &'a str,
    /// Generation id from JoinGroup.
    pub generation_id: i32,
    /// Member id assigned by the coordinator.
    pub member_id: &'a str,
    /// Kafka `group.instance.id`.
    pub group_instance_id: Option<&'a str>,
    /// Protocol type (`"consumer"`). Written on v5+ (KIP-559).
    pub protocol_type: &'a str,
    /// Protocol name (`"range"`, `"sticky"`, `"cooperative-sticky"`).
    /// Written on v5+ (KIP-559).
    pub protocol_name: &'a str,
    /// Member assignments (`member_id` → assignment bytes). Empty for
    /// followers.
    pub assignments: &'a [(String, Vec<u8>)],
}

/// Encode SyncGroup v0–v5.
///
/// Kafka 4.0 JSON: `validVersions: "0-5"`, `flexibleVersions: "4+"`.
/// v0–v2 are GroupId + GenerationId + MemberId + Assignments (v1 and v2
/// match v0). v3 GroupInstanceId. v4 flexible. v5 ProtocolType /
/// ProtocolName (KIP-559). This crate speaks 0–5. v6+ is not spoken.
pub fn encode_sync_group_request(
    buf: &mut BytesMut,
    version: i16,
    req: &SyncGroupRequest<'_>,
) -> crate::error::Result<()> {
    let flexible = sync_group_flexible(version)?;
    buf::put_string(buf, flexible, Some(req.group_id))?;
    buf.put_i32(req.generation_id);
    buf::put_string(buf, flexible, Some(req.member_id))?;
    if version >= 3 {
        buf::put_string(buf, flexible, req.group_instance_id)?;
    }
    if version >= 5 {
        buf::put_string(buf, true, Some(req.protocol_type))?;
        buf::put_string(buf, true, Some(req.protocol_name))?;
    }
    buf::put_array_len(buf, flexible, Some(req.assignments.len()))?;
    for (id, bytes) in req.assignments {
        buf::put_string(buf, flexible, Some(id))?;
        buf::put_bytes(buf, flexible, Some(bytes))?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
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
    version: i16,
) -> Result<(String, String, Vec<(String, Vec<u8>)>)> {
    let flexible = sync_group_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _gen = buf::get_i32(buf)?;
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    if version >= 3 {
        let _inst = buf::get_string(buf, flexible)?;
    }
    if version >= 5 {
        let _ptype = buf::get_string(buf, true)?;
        let _pname = buf::get_string(buf, true)?;
    }
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut assignments = Vec::with_capacity(n);
    for _ in 0..n {
        let id = buf::get_string(buf, flexible)?.unwrap_or_default();
        let bytes = buf::get_bytes(buf, flexible)?.unwrap_or_default();
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        assignments.push((id, bytes));
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((group_id, member_id, assignments))
}

/// Encode SyncGroup v0–v5. Throttle is `0` on v1+. ProtocolType /
/// ProtocolName are v5+ (null here).
pub fn encode_sync_group_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    assignment: &[u8],
) -> crate::error::Result<()> {
    let flexible = sync_group_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf.put_i16(error_code);
    if version >= 5 {
        buf::put_string(buf, true, None)?;
        buf::put_string(buf, true, None)?;
    }
    buf::put_bytes(buf, flexible, Some(assignment))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode SyncGroup: `(error_code, assignment)`. Throttle is v1+.
pub fn decode_sync_group_response<B: Buf>(buf: &mut B, version: i16) -> Result<(i16, Vec<u8>)> {
    let flexible = sync_group_flexible(version)?;
    if version >= 1 {
        let _throttle = buf::get_i32(buf)?;
    }
    let error = buf::get_i16(buf)?;
    if version >= 5 {
        let _ptype = buf::get_string(buf, true)?;
        let _pname = buf::get_string(buf, true)?;
    }
    let assignment = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((error, assignment))
}

/// `true` when Heartbeat `version` is flexible (v4+).
///
/// v0–v3 are classic. v4 is compact strings plus tagged fields (Apache
/// JSON `flexibleVersions: "4+"`). Kafka 4.0 `validVersions` is `0-4`.
/// Official JSON: v1 and v2 match v0. v1+ ThrottleTimeMs. v3
/// GroupInstanceId. This crate speaks 0–4. v5+ is not spoken.
fn heartbeat_flexible(version: i16) -> Result<bool> {
    match version {
        0..=3 => Ok(false),
        4 => Ok(true),
        other => Err(Error::protocol(format!(
            "Heartbeat version {other} is not implemented"
        ))),
    }
}

/// Encode Heartbeat v0–v4.
///
/// Kafka 4.0 JSON: `validVersions: "0-4"`, `flexibleVersions: "4+"`.
/// v0–v2 are GroupId + GenerationId + MemberId (v1 and v2 match v0).
/// v3 GroupInstanceId. v4 flexible. This crate speaks 0–4. v5+ is not
/// spoken.
pub fn encode_heartbeat_request(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> crate::error::Result<()> {
    let flexible = heartbeat_flexible(version)?;
    buf::put_string(buf, flexible, Some(group_id))?;
    buf.put_i32(generation_id);
    buf::put_string(buf, flexible, Some(member_id))?;
    if version >= 3 {
        buf::put_string(buf, flexible, group_instance_id)?;
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode Heartbeat: `(group_id, generation_id, member_id)`.
pub fn decode_heartbeat_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, i32, String)> {
    let flexible = heartbeat_flexible(version)?;
    let g = buf::get_string(buf, flexible)?.unwrap_or_default();
    let gen = buf::get_i32(buf)?;
    let m = buf::get_string(buf, flexible)?.unwrap_or_default();
    if version >= 3 {
        let _inst = buf::get_string(buf, flexible)?;
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((g, gen, m))
}

/// Encode Heartbeat v0–v4. Throttle is `0` on v1+.
pub fn encode_heartbeat_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
) -> crate::error::Result<()> {
    let flexible = heartbeat_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf.put_i16(error_code);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode Heartbeat: error code. Throttle is v1+.
pub fn decode_heartbeat_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let flexible = heartbeat_flexible(version)?;
    if version >= 1 {
        let _throttle = buf::get_i32(buf)?;
    }
    let err = buf::get_i16(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(err)
}

/// Encode LeaveGroup v0 (group id + one member id).
pub fn encode_leave_group_request(
    buf: &mut BytesMut,
    group_id: &str,
    member_id: &str,
) -> crate::error::Result<()> {
    encode_leave_group_request_members(
        buf,
        0,
        group_id,
        &[LeaveGroupMember {
            member_id: member_id.to_string(),
            group_instance_id: None,
            reason: None,
        }],
    )
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

/// Encode LeaveGroup v0–v5.
///
/// Kafka 4.0 JSON: `validVersions: "0-5"`, `flexibleVersions: "4+"`.
/// v0–v2 are GroupId + MemberId (v1 and v2 match v0). v3 Members +
/// GroupInstanceId. v4 flexible. v5 Reason (KIP-800). This crate
/// speaks 0–5. v6+ is not spoken.
pub fn encode_leave_group_request_members(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    members: &[LeaveGroupMember],
) -> crate::error::Result<()> {
    let flexible = leave_group_flexible(version)?;
    buf::put_string(buf, flexible, Some(group_id))?;
    if version >= 3 {
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
        return Ok(());
    }
    let member_id = members.first().map(|m| m.member_id.as_str()).unwrap_or("");
    buf::put_string(buf, flexible, Some(member_id))?;
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
    // Official JSON: "Version 1 and 2 are the same as version 0."
    // MemberId is v0–v2. GroupInstanceId lives on Members (v3+).
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    Ok((
        group_id,
        vec![LeaveGroupMember {
            member_id,
            group_instance_id: None,
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

/// One partition in OffsetCommit v7–v9 / OffsetFetch v5.
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

/// Topic + partitions for OffsetCommit v7–v9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions to commit.
    pub partitions: Vec<OffsetPartition>,
}

/// Topic + partition indexes for OffsetFetch v5–v9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to fetch.
    pub partitions: Vec<i32>,
}

/// One partition in an OffsetFetch v5–v9 response.
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

/// Topic + committed offsets from OffsetFetch v5–v9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedOffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions in this topic.
    pub partitions: Vec<FetchedOffset>,
}

/// `true` when OffsetCommit `version` is flexible (v8+).
///
/// v7 is classic (GroupId / MemberId / GroupInstanceId, leader epoch,
/// metadata). v8–v9 are compact strings/arrays plus tagged fields on
/// partitions / topics / top-level (Apache JSON `flexibleVersions: "8+"`).
/// v9 is KIP-848 error codes (`GROUP_ID_NOT_FOUND`, `STALE_MEMBER_EPOCH`)
/// with the same layout as v8. Kafka 4.0 `validVersions` is `2-9`. This
/// crate speaks 7–9. v2–v6 (retention time / no instance id) and v10+
/// are not spoken.
fn offset_commit_flexible(version: i16) -> Result<bool> {
    match version {
        7 => Ok(false),
        8..=9 => Ok(true),
        other => Err(Error::protocol(format!(
            "OffsetCommit version {other} is not implemented"
        ))),
    }
}

/// Encode OffsetCommit v7 (classic) or v8–v9 (flexible).
pub fn encode_offset_commit_request(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
    topics: &[OffsetTopic],
) -> crate::error::Result<()> {
    let flexible = offset_commit_flexible(version)?;
    buf::put_string(buf, flexible, Some(group_id))?;
    buf.put_i32(generation_id);
    buf::put_string(buf, flexible, Some(member_id))?;
    buf::put_string(buf, flexible, group_instance_id)?;
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i64(p.offset);
            buf.put_i32(p.leader_epoch);
            let meta = if p.metadata.is_empty() {
                None
            } else {
                Some(p.metadata.as_str())
            };
            buf::put_string(buf, flexible, meta)?;
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

/// Decode OffsetCommit: `(group_id, member_id, topics)`.
pub fn decode_offset_commit_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, Vec<OffsetTopic>)> {
    let flexible = offset_commit_flexible(version)?;
    let group = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _gen = buf::get_i32(buf)?;
    let member = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _inst = buf::get_string(buf, flexible)?;
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let offset = buf::get_i64(buf)?;
            let leader_epoch = buf::get_i32(buf)?;
            let metadata = buf::get_string(buf, flexible)?.unwrap_or_default();
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(OffsetPartition {
                partition,
                offset,
                leader_epoch,
                metadata,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(OffsetTopic { topic, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((group, member, topics))
}

/// Encode OffsetCommit: one error code applied to every partition.
pub fn encode_offset_commit_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetTopic],
    error: i16,
) -> crate::error::Result<()> {
    let flexible = offset_commit_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(error);
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

/// Decode OffsetCommit: first non-zero partition error, or `0`.
pub fn decode_offset_commit_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let flexible = offset_commit_flexible(version)?;
    let _throttle = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut first_err = 0i16;
    for _ in 0..n {
        let _topic = buf::get_string(buf, flexible)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        for _ in 0..pn {
            let _p = buf::get_i32(buf)?;
            let err = buf::get_i16(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            if first_err == 0 && err != 0 {
                first_err = err;
            }
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(first_err)
}

/// `true` when OffsetFetch `version` is flexible (v6+).
///
/// v5 is classic (GroupId + Topics, committed leader epoch). v6 is compact
/// strings/arrays plus tagged fields (Apache JSON `flexibleVersions: "6+"`).
/// v7 adds RequireStable. v8 replaces GroupId / Topics with Groups (KIP-709).
/// v9 adds MemberId / MemberEpoch on each group (KIP-848). Kafka 4.0
/// `validVersions` is `1-9`. This crate speaks 5–9. v1–v4 (no leader epoch)
/// and v10+ (topic IDs) are not spoken.
fn offset_fetch_flexible(version: i16) -> Result<bool> {
    match version {
        5 => Ok(false),
        6..=9 => Ok(true),
        other => Err(Error::protocol(format!(
            "OffsetFetch version {other} is not implemented"
        ))),
    }
}

fn encode_offset_fetch_topics(
    buf: &mut BytesMut,
    flexible: bool,
    topics: &[OffsetFetchTopic],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(*p);
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    Ok(())
}

fn decode_offset_fetch_topics<B: Buf>(
    buf: &mut B,
    flexible: bool,
) -> Result<Vec<OffsetFetchTopic>> {
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(OffsetFetchTopic { topic, partitions });
    }
    Ok(topics)
}

fn encode_fetched_offset_topics(
    buf: &mut BytesMut,
    flexible: bool,
    topics: &[FetchedOffsetTopic],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i64(p.offset);
            buf.put_i32(p.leader_epoch);
            let meta = if p.metadata.is_empty() {
                None
            } else {
                Some(p.metadata.as_str())
            };
            buf::put_string(buf, flexible, meta)?;
            buf.put_i16(p.error_code);
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    Ok(())
}

fn decode_fetched_offset_topics<B: Buf>(
    buf: &mut B,
    flexible: bool,
) -> Result<Vec<FetchedOffsetTopic>> {
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let offset = buf::get_i64(buf)?;
            let leader_epoch = buf::get_i32(buf)?;
            let metadata = buf::get_string(buf, flexible)?.unwrap_or_default();
            let error_code = buf::get_i16(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(FetchedOffset {
                partition,
                offset,
                leader_epoch,
                metadata,
                error_code,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(FetchedOffsetTopic { topic, partitions });
    }
    Ok(topics)
}

/// Encode OffsetFetch v5 (classic) or v6–v9 (flexible).
pub fn encode_offset_fetch_request(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: i32,
    require_stable: bool,
    topics: &[OffsetFetchTopic],
) -> crate::error::Result<()> {
    let flexible = offset_fetch_flexible(version)?;
    if version <= 7 {
        buf::put_string(buf, flexible, Some(group_id))?;
        encode_offset_fetch_topics(buf, flexible, topics)?;
    } else {
        buf::put_array_len(buf, true, Some(1))?;
        buf::put_compact_string(buf, Some(group_id))?;
        if version >= 9 {
            buf::put_compact_string(buf, member_id)?;
            buf.put_i32(member_epoch);
        }
        encode_offset_fetch_topics(buf, true, topics)?;
        buf::put_empty_tagged_fields(buf);
    }
    if version >= 7 {
        buf.put_u8(u8::from(require_stable));
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode OffsetFetch: `(group_id, topics, require_stable)`.
pub fn decode_offset_fetch_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, Vec<OffsetFetchTopic>, bool)> {
    let flexible = offset_fetch_flexible(version)?;
    let (group, topics) = if version <= 7 {
        let group = buf::get_string(buf, flexible)?.unwrap_or_default();
        let topics = decode_offset_fetch_topics(buf, flexible)?;
        (group, topics)
    } else {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut group = String::new();
        let mut topics = Vec::new();
        let mut first = true;
        for _ in 0..n {
            let next = buf::get_compact_string(buf)?.unwrap_or_default();
            if version >= 9 {
                let _member = buf::get_compact_string(buf)?;
                let _epoch = buf::get_i32(buf)?;
            }
            let next_topics = decode_offset_fetch_topics(buf, true)?;
            buf::skip_tagged_fields(buf)?;
            if first {
                group = next;
                topics = next_topics;
                first = false;
            }
        }
        (group, topics)
    };
    let require_stable = if version >= 7 {
        buf::get_bool(buf)?
    } else {
        false
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((group, topics, require_stable))
}

/// Encode OffsetFetch: throttle, topics or Groups, then error / tagged fields.
pub fn encode_offset_fetch_response(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    topics: &[FetchedOffsetTopic],
    error: i16,
) -> crate::error::Result<()> {
    let flexible = offset_fetch_flexible(version)?;
    buf.put_i32(0);
    if version <= 7 {
        encode_fetched_offset_topics(buf, flexible, topics)?;
        buf.put_i16(error);
    } else {
        buf::put_array_len(buf, true, Some(1))?;
        buf::put_compact_string(buf, Some(group_id))?;
        encode_fetched_offset_topics(buf, true, topics)?;
        buf.put_i16(error);
        buf::put_empty_tagged_fields(buf);
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode OffsetFetch. Top-level / group error is [`crate::error::Error::Broker`].
pub fn decode_offset_fetch_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<FetchedOffsetTopic>> {
    let flexible = offset_fetch_flexible(version)?;
    let _throttle = buf::get_i32(buf)?;
    let (topics, top) = if version <= 7 {
        let topics = decode_fetched_offset_topics(buf, flexible)?;
        let top = buf::get_i16(buf)?;
        (topics, top)
    } else {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::new();
        let mut top = 0i16;
        let mut first = true;
        for _ in 0..n {
            let _gid = buf::get_compact_string(buf)?;
            let next = decode_fetched_offset_topics(buf, true)?;
            let err = buf::get_i16(buf)?;
            buf::skip_tagged_fields(buf)?;
            if first {
                topics = next;
                top = err;
                first = false;
            }
        }
        (topics, top)
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
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
        encode_find_coordinator_request_typed(&mut buf, 2, "tx-1", COORDINATOR_TRANSACTION)
            .unwrap();
        let mut cur = &buf[..];
        let (key, key_type) = decode_find_coordinator_request(&mut cur, 2).unwrap();
        assert_eq!((key.as_str(), key_type), ("tx-1", COORDINATOR_TRANSACTION));
        assert!(
            cur.is_empty(),
            "v2 decoder must consume key_type; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_find_coordinator_request_typed(&mut buf, 2, "g", COORDINATOR_GROUP).unwrap();
        let mut cur = &buf[..];
        let (key, key_type) = decode_find_coordinator_request(&mut cur, 2).unwrap();
        assert_eq!((key.as_str(), key_type), ("g", COORDINATOR_GROUP));
        assert!(
            cur.is_empty(),
            "group key_type leftover {} bytes",
            cur.len()
        );
        buf.clear();
        assert!(
            encode_find_coordinator_request_typed(&mut buf, 7, "g", COORDINATOR_GROUP).is_err(),
            "FindCoordinator v7+ is not spoken"
        );
        buf.clear();
        assert!(
            encode_find_coordinator_request_typed(&mut buf, 0, "g", COORDINATOR_GROUP).is_err(),
            "FindCoordinator v0 (no KeyType) is not spoken"
        );
    }

    #[test]
    fn find_coordinator_v3_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_find_coordinator_request_typed(&mut req, 3, "tx-1", COORDINATOR_TRANSACTION)
            .unwrap();
        let mut cur = &req[..];
        let (key, key_type) = decode_find_coordinator_request(&mut cur, 3).unwrap();
        assert_eq!((key.as_str(), key_type), ("tx-1", COORDINATOR_TRANSACTION));
        assert!(
            cur.is_empty(),
            "FindCoordinator v3 request must consume compact tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_find_coordinator_response(&mut resp, 3, 1, "h", 9092, "tx-1").unwrap();
        let mut cur = &resp[..];
        let (err, node, host, port) = decode_find_coordinator_response(&mut cur, 3).unwrap();
        assert_eq!(err, 0);
        assert_eq!(node, 1);
        assert_eq!(host, "h");
        assert_eq!(port, 9092);
        assert!(
            cur.is_empty(),
            "FindCoordinator v3 response must consume compact tagged fields"
        );
    }

    #[test]
    fn find_coordinator_v3_request_matches_compact_layout() {
        // Compact "g" (n+1 = 2), key_type 0, tagged.
        const REQ: &[u8] = &[0x02, 0x67, 0x00, 0x00];
        let mut buf = BytesMut::new();
        encode_find_coordinator_request_typed(&mut buf, 3, "g", COORDINATOR_GROUP).unwrap();
        assert_eq!(&buf[..], REQ);
    }

    #[test]
    fn find_coordinator_v3_response_matches_compact_layout() {
        // Throttle 0, error 0, null ErrorMessage, node 1, compact "h", port 9092, tagged.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x68, 0x00,
            0x00, 0x23, 0x84, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_find_coordinator_response(&mut buf, 3, 1, "h", 9092, "g").unwrap();
        assert_eq!(&buf[..], RESP);
    }

    #[test]
    fn find_coordinator_v4_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_find_coordinator_request_typed(&mut req, 4, "g", COORDINATOR_GROUP).unwrap();
        let mut cur = &req[..];
        let (key, key_type) = decode_find_coordinator_request(&mut cur, 4).unwrap();
        assert_eq!((key.as_str(), key_type), ("g", COORDINATOR_GROUP));
        assert!(
            cur.is_empty(),
            "FindCoordinator v4 request must consume CoordinatorKeys tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_find_coordinator_response(&mut resp, 4, 1, "h", 9092, "g").unwrap();
        let mut cur = &resp[..];
        let (err, node, host, port) = decode_find_coordinator_response(&mut cur, 4).unwrap();
        assert_eq!(err, 0);
        assert_eq!(node, 1);
        assert_eq!(host, "h");
        assert_eq!(port, 9092);
        assert!(
            cur.is_empty(),
            "FindCoordinator v4 response must consume Coordinators tagged fields"
        );
    }

    #[test]
    fn find_coordinator_v4_request_matches_compact_layout() {
        // KeyType 0, compact CoordinatorKeys of 1 ("g"), tagged.
        const REQ: &[u8] = &[0x00, 0x02, 0x02, 0x67, 0x00];
        let mut buf = BytesMut::new();
        encode_find_coordinator_request_typed(&mut buf, 4, "g", COORDINATOR_GROUP).unwrap();
        assert_eq!(&buf[..], REQ);
        buf.clear();
        encode_find_coordinator_request_typed(&mut buf, 6, "g", COORDINATOR_GROUP).unwrap();
        assert_eq!(&buf[..], REQ, "v6 request layout matches v4");
    }

    #[test]
    fn find_coordinator_v4_response_matches_compact_layout() {
        // Throttle 0, compact Coordinators of 1: key "g", node 1, host "h",
        // port 9092, error 0, null ErrorMessage, nested tags, top tags.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x67, 0x00, 0x00, 0x00, 0x01, 0x02, 0x68, 0x00,
            0x00, 0x23, 0x84, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_find_coordinator_response(&mut buf, 4, 1, "h", 9092, "g").unwrap();
        assert_eq!(&buf[..], RESP);
        buf.clear();
        encode_find_coordinator_response(&mut buf, 6, 1, "h", 9092, "g").unwrap();
        assert_eq!(&buf[..], RESP, "v6 response layout matches v4");
    }

    #[test]
    fn find_coordinator_v4_vs_v3_request_layout_differs() {
        let mut v3 = BytesMut::new();
        encode_find_coordinator_request_typed(&mut v3, 3, "g", COORDINATOR_GROUP).unwrap();
        let mut v4 = BytesMut::new();
        encode_find_coordinator_request_typed(&mut v4, 4, "g", COORDINATOR_GROUP).unwrap();
        assert_ne!(
            &v3[..],
            &v4[..],
            "v4 CoordinatorKeys must not match v3 Key + KeyType"
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
            5,
            &JoinGroupRequest {
                group_id: "g",
                session_timeout_ms: 10_000,
                member_id: "m1",
                group_instance_id: Some("worker-1"),
                protocol_type: "consumer",
                protocol_name: "range",
                metadata: &[1, 2, 3],
                reason: None,
            },
        )
        .unwrap();
        let mut cur = &buf[..];
        let (gid, member, instance, meta) = decode_join_group_request(&mut cur, 5).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(member, "m1");
        assert_eq!(instance.as_deref(), Some("worker-1"));
        assert_eq!(meta, vec![1, 2, 3]);
        assert!(cur.is_empty(), "v5 decoder leftover {} bytes", cur.len());
    }

    fn join_req(metadata: &[u8]) -> JoinGroupRequest<'_> {
        JoinGroupRequest {
            group_id: "g",
            session_timeout_ms: 10_000,
            member_id: "m1",
            group_instance_id: None,
            protocol_type: "consumer",
            protocol_name: "range",
            metadata,
            reason: None,
        }
    }

    #[test]
    fn join_group_v6_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_join_group_request(&mut req, 6, &join_req(&[1, 2, 3])).unwrap();
        let mut cur = &req[..];
        let (gid, member, instance, meta) = decode_join_group_request(&mut cur, 6).unwrap();
        assert_eq!((gid.as_str(), member.as_str()), ("g", "m1"));
        assert_eq!(instance, None);
        assert_eq!(meta, vec![1, 2, 3]);
        assert!(
            cur.is_empty(),
            "v6 decoder must consume compact fields and tagged fields; leftover {} bytes",
            cur.len()
        );

        let members = [JoinMember {
            member_id: "m1".into(),
            metadata: vec![1, 2, 3],
        }];
        let mut resp = BytesMut::new();
        encode_join_group_response(&mut resp, 6, 0, 7, "range", "l", "m1", &members).unwrap();
        let mut cur = &resp[..];
        let (err, gen, proto, leader, mid, skip, got) =
            decode_join_group_response(&mut cur, 6).unwrap();
        assert_eq!(
            (
                err,
                gen,
                proto.as_str(),
                leader.as_str(),
                mid.as_str(),
                skip
            ),
            (0, 7, "range", "l", "m1", false)
        );
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].metadata, vec![1, 2, 3]);
        assert!(cur.is_empty(), "v6 response leftover {} bytes", cur.len());
    }

    #[test]
    fn join_group_v8_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        let mut body = join_req(&[1, 2, 3]);
        body.reason = Some("rejoin");
        encode_join_group_request(&mut req, 8, &body).unwrap();
        let mut cur = &req[..];
        let (gid, member, _, meta) = decode_join_group_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), member.as_str()), ("g", "m1"));
        assert_eq!(meta, vec![1, 2, 3]);
        assert!(
            cur.is_empty(),
            "v8 decoder must consume Reason; leftover {} bytes",
            cur.len()
        );

        let mut resp = BytesMut::new();
        encode_join_group_response(&mut resp, 8, 0, 7, "range", "l", "m1", &[]).unwrap();
        let mut cur = &resp[..];
        let (err, _, _, _, _, skip, members) = decode_join_group_response(&mut cur, 8).unwrap();
        assert_eq!((err, skip, members.len()), (0, false, 0));
        assert!(cur.is_empty(), "v8 response leftover {} bytes", cur.len());
    }

    #[test]
    fn join_group_v9_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_join_group_request(&mut req, 9, &join_req(&[1, 2, 3])).unwrap();
        let mut v8 = BytesMut::new();
        encode_join_group_request(&mut v8, 8, &join_req(&[1, 2, 3])).unwrap();
        assert_eq!(&req[..], &v8[..], "JoinGroup v9 request matches v8");
        let mut cur = &req[..];
        let _ = decode_join_group_request(&mut cur, 9).unwrap();
        assert!(cur.is_empty(), "v9 request leftover {} bytes", cur.len());

        let mut resp = BytesMut::new();
        encode_join_group_response(&mut resp, 9, 0, 7, "range", "l", "m1", &[]).unwrap();
        let mut cur = &resp[..];
        let (err, _, _, _, _, skip, _) = decode_join_group_response(&mut cur, 9).unwrap();
        assert_eq!((err, skip), (0, false));
        assert!(
            cur.is_empty(),
            "v9 response must consume SkipAssignment; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn join_group_v6_request_matches_compact_layout() {
        // Compact "g", session/rebalance 10000, compact "m1", null instance,
        // compact "consumer", one protocol "range" metadata [1,2,3], tagged.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x00, 0x00, 0x27, 0x10, 0x00, 0x00, 0x27, 0x10, 0x03, 0x6d, 0x31, 0x00,
            0x09, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x02, 0x06, 0x72, 0x61, 0x6e,
            0x67, 0x65, 0x04, 0x01, 0x02, 0x03, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_join_group_request(&mut buf, 6, &join_req(&[1, 2, 3])).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v5 = BytesMut::new();
        encode_join_group_request(&mut v5, 5, &join_req(&[1, 2, 3])).unwrap();
        assert_ne!(&buf[..], &v5[..], "JoinGroup v6 must not be classic v5");
        assert!(
            encode_join_group_request(&mut BytesMut::new(), 4, &join_req(&[1, 2, 3])).is_err(),
            "JoinGroup v4 is not spoken"
        );
        assert!(
            encode_join_group_request(&mut BytesMut::new(), 10, &join_req(&[1, 2, 3])).is_err(),
            "JoinGroup v10+ is not spoken"
        );
    }

    #[test]
    fn join_group_v8_request_matches_compact_layout() {
        // v6 body plus null Reason before top-level tagged fields.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x00, 0x00, 0x27, 0x10, 0x00, 0x00, 0x27, 0x10, 0x03, 0x6d, 0x31, 0x00,
            0x09, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x02, 0x06, 0x72, 0x61, 0x6e,
            0x67, 0x65, 0x04, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_join_group_request(&mut buf, 8, &join_req(&[1, 2, 3])).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v6 = BytesMut::new();
        encode_join_group_request(&mut v6, 6, &join_req(&[1, 2, 3])).unwrap();
        assert_ne!(&buf[..], &v6[..], "JoinGroup v8 must include Reason");
    }

    #[test]
    fn join_group_v6_response_matches_compact_layout() {
        // Throttle 0, error 0, generation 7, compact "range", compact "l",
        // compact "m1", empty members, tagged.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x06, 0x72, 0x61, 0x6e,
            0x67, 0x65, 0x02, 0x6c, 0x03, 0x6d, 0x31, 0x01, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_join_group_response(&mut buf, 6, 0, 7, "range", "l", "m1", &[]).unwrap();
        assert_eq!(&buf[..], RESP);
    }

    #[test]
    fn join_group_v7_response_matches_compact_layout() {
        // v6 plus null ProtocolType before ProtocolName.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x06, 0x72, 0x61,
            0x6e, 0x67, 0x65, 0x02, 0x6c, 0x03, 0x6d, 0x31, 0x01, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_join_group_response(&mut buf, 7, 0, 7, "range", "l", "m1", &[]).unwrap();
        assert_eq!(&buf[..], RESP);
        let mut v6 = BytesMut::new();
        encode_join_group_response(&mut v6, 6, 0, 7, "range", "l", "m1", &[]).unwrap();
        assert_ne!(
            &buf[..],
            &v6[..],
            "JoinGroup v7 response must include ProtocolType"
        );
    }

    #[test]
    fn join_group_v9_response_matches_compact_layout() {
        // v7 plus SkipAssignment 0 after Leader.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x06, 0x72, 0x61,
            0x6e, 0x67, 0x65, 0x02, 0x6c, 0x00, 0x03, 0x6d, 0x31, 0x01, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_join_group_response(&mut buf, 9, 0, 7, "range", "l", "m1", &[]).unwrap();
        assert_eq!(&buf[..], RESP);
        let mut v7 = BytesMut::new();
        encode_join_group_response(&mut v7, 7, 0, 7, "range", "l", "m1", &[]).unwrap();
        assert_ne!(
            &buf[..],
            &v7[..],
            "JoinGroup v9 response must include SkipAssignment"
        );
    }

    fn offset_commit_topics() -> Vec<OffsetTopic> {
        vec![OffsetTopic {
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
        }]
    }

    #[test]
    fn offset_commit_v7_batches_partitions_and_consumes_epoch_metadata() {
        let topics = offset_commit_topics();
        let mut buf = BytesMut::new();
        encode_offset_commit_request(&mut buf, 7, "g", 7, "m1", None, &topics).unwrap();
        let mut cur = &buf[..];
        let (gid, mid, got) = decode_offset_commit_request(&mut cur, 7).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got, topics);
        assert!(
            cur.is_empty(),
            "v7 decoder must consume leader epoch and metadata; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_offset_commit_response(&mut buf, 7, &topics, 0).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_offset_commit_response(&mut cur, 7).unwrap(), 0);
        assert!(cur.is_empty(), "v7 response leftover {} bytes", cur.len());
    }

    #[test]
    fn offset_commit_v8_roundtrip_is_leftover_empty() {
        let topics = offset_commit_topics();
        let mut req = BytesMut::new();
        encode_offset_commit_request(&mut req, 8, "g", 7, "m1", Some("i"), &topics).unwrap();
        let mut cur = &req[..];
        let (gid, mid, got) = decode_offset_commit_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got, topics);
        assert!(
            cur.is_empty(),
            "v8 decoder must consume compact strings and tagged fields; leftover {} bytes",
            cur.len()
        );

        let mut resp = BytesMut::new();
        encode_offset_commit_response(&mut resp, 8, &topics, 0).unwrap();
        let mut cur = &resp[..];
        assert_eq!(decode_offset_commit_response(&mut cur, 8).unwrap(), 0);
        assert!(
            cur.is_empty(),
            "v8 response must consume tagged fields; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_commit_v9_matches_v8_layout() {
        let topics = offset_commit_topics();
        let mut v8 = BytesMut::new();
        encode_offset_commit_request(&mut v8, 8, "g", 7, "m1", None, &topics).unwrap();
        let mut v9 = BytesMut::new();
        encode_offset_commit_request(&mut v9, 9, "g", 7, "m1", None, &topics).unwrap();
        assert_eq!(&v8[..], &v9[..], "OffsetCommit v9 request matches v8");

        v8.clear();
        encode_offset_commit_response(&mut v8, 8, &topics, 0).unwrap();
        v9.clear();
        encode_offset_commit_response(&mut v9, 9, &topics, 0).unwrap();
        assert_eq!(&v8[..], &v9[..], "OffsetCommit v9 response matches v8");
    }

    #[test]
    fn offset_commit_v8_request_matches_compact_layout() {
        // Compact "g", generation 7, compact "m1", null instance, one topic
        // "t" partition 0 offset 3 epoch 4, null metadata, tagged.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x00, 0x00, 0x00, 0x07, 0x03, 0x6d, 0x31, 0x00, 0x02, 0x02, 0x74, 0x02,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
            0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
        ];
        let topics = [OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition {
                partition: 0,
                offset: 3,
                leader_epoch: 4,
                metadata: String::new(),
            }],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_request(&mut buf, 8, "g", 7, "m1", None, &topics).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v7 = BytesMut::new();
        encode_offset_commit_request(&mut v7, 7, "g", 7, "m1", None, &topics).unwrap();
        assert_ne!(&buf[..], &v7[..], "OffsetCommit v8 must not be classic v7");
        assert!(
            encode_offset_commit_request(&mut BytesMut::new(), 6, "g", 7, "m1", None, &topics)
                .is_err(),
            "OffsetCommit v6 is not spoken"
        );
        assert!(
            encode_offset_commit_request(&mut BytesMut::new(), 10, "g", 7, "m1", None, &topics)
                .is_err(),
            "OffsetCommit v10+ is not spoken"
        );
    }

    #[test]
    fn offset_commit_v8_response_matches_compact_layout() {
        // Throttle 0, one topic "t" partition 0 error 0, tagged.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00,
        ];
        let topics = [OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition::new(0, 3)],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_response(&mut buf, 8, &topics, 0).unwrap();
        assert_eq!(&buf[..], RESP);
    }

    #[test]
    fn offset_commit_response_returns_first_partition_error() {
        let topics = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition::new(0, 1), OffsetPartition::new(1, 2)],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_response(&mut buf, 7, &topics, 16).unwrap();
        assert_eq!(decode_offset_commit_response(&mut &buf[..], 7).unwrap(), 16);
        buf.clear();
        encode_offset_commit_response(&mut buf, 8, &topics, 16).unwrap();
        assert_eq!(decode_offset_commit_response(&mut &buf[..], 8).unwrap(), 16);
    }

    #[test]
    fn offset_fetch_v5_batches_partitions_and_consumes_tail() {
        let req = vec![OffsetFetchTopic {
            topic: "t".into(),
            partitions: vec![0, 1, 2],
        }];
        let mut buf = BytesMut::new();
        encode_offset_fetch_request(&mut buf, 5, "g", None, -1, false, &req).unwrap();
        let mut cur = &buf[..];
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 5).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(got, req);
        assert!(!stable);
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
        encode_offset_fetch_response(&mut buf, 5, "g", &resp, 0).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_offset_fetch_response(&mut cur, 5).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            cur.is_empty(),
            "v5 decoder must consume epoch, metadata, partition error, and top-level error"
        );
    }

    fn offset_fetch_one_topic() -> Vec<OffsetFetchTopic> {
        vec![OffsetFetchTopic {
            topic: "t".into(),
            partitions: vec![0],
        }]
    }

    #[test]
    fn offset_fetch_v6_roundtrip_is_leftover_empty() {
        let req = offset_fetch_one_topic();
        let mut buf = BytesMut::new();
        encode_offset_fetch_request(&mut buf, 6, "g", None, -1, false, &req).unwrap();
        let mut cur = &buf[..];
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 6).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, req);
        assert!(
            cur.is_empty(),
            "v6 request must consume tagged fields; leftover {} bytes",
            cur.len()
        );

        let resp = vec![FetchedOffsetTopic {
            topic: "t".into(),
            partitions: vec![FetchedOffset::new(0, 4, 0)],
        }];
        buf.clear();
        encode_offset_fetch_response(&mut buf, 6, "g", &resp, 0).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_offset_fetch_response(&mut cur, 6).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            cur.is_empty(),
            "v6 response must consume tagged fields; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_v6_request_matches_compact_layout() {
        // Compact "g", one topic "t" partition 0, tagged.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let req = offset_fetch_one_topic();
        let mut buf = BytesMut::new();
        encode_offset_fetch_request(&mut buf, 6, "g", None, -1, false, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v5 = BytesMut::new();
        encode_offset_fetch_request(&mut v5, 5, "g", None, -1, false, &req).unwrap();
        assert_ne!(&buf[..], &v5[..], "OffsetFetch v6 must not be classic v5");
        assert!(
            encode_offset_fetch_request(&mut BytesMut::new(), 4, "g", None, -1, false, &req)
                .is_err(),
            "OffsetFetch v4 is not spoken"
        );
        assert!(
            encode_offset_fetch_request(&mut BytesMut::new(), 10, "g", None, -1, false, &req)
                .is_err(),
            "OffsetFetch v10+ is not spoken"
        );
    }

    #[test]
    fn offset_fetch_v7_sends_require_stable() {
        let req = offset_fetch_one_topic();
        let mut off = BytesMut::new();
        encode_offset_fetch_request(&mut off, 7, "g", None, -1, false, &req).unwrap();
        let mut on = BytesMut::new();
        encode_offset_fetch_request(&mut on, 7, "g", None, -1, true, &req).unwrap();
        assert_ne!(&off[..], &on[..], "RequireStable must change the v7 body");
        let mut cur = &on[..];
        let (_gid, _got, stable) = decode_offset_fetch_request(&mut cur, 7).unwrap();
        assert!(stable);
        assert!(cur.is_empty());
        // Compact "g", one topic "t" p0, RequireStable 1, tagged.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        ];
        assert_eq!(&on[..], REQ);
    }

    #[test]
    fn offset_fetch_v8_groups_roundtrip_is_leftover_empty() {
        let req = offset_fetch_one_topic();
        let mut buf = BytesMut::new();
        encode_offset_fetch_request(&mut buf, 8, "g", None, -1, false, &req).unwrap();
        let mut cur = &buf[..];
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, req);
        assert!(
            cur.is_empty(),
            "v8 request must consume Groups tagged fields; leftover {} bytes",
            cur.len()
        );
        // Compact Groups of 1 ("g"), one topic "t" p0, group tags, RequireStable 0, top tags.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x67, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        assert_eq!(&buf[..], REQ);

        let resp = vec![FetchedOffsetTopic {
            topic: "t".into(),
            partitions: vec![FetchedOffset::new(0, 4, 0)],
        }];
        buf.clear();
        encode_offset_fetch_response(&mut buf, 8, "g", &resp, 0).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_offset_fetch_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            cur.is_empty(),
            "v8 response must consume Groups tagged fields; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_v9_sends_member_id_and_epoch() {
        let req = offset_fetch_one_topic();
        let mut v8 = BytesMut::new();
        encode_offset_fetch_request(&mut v8, 8, "g", Some("m1"), 3, false, &req).unwrap();
        let mut v9 = BytesMut::new();
        encode_offset_fetch_request(&mut v9, 9, "g", Some("m1"), 3, false, &req).unwrap();
        assert_ne!(&v8[..], &v9[..], "v9 must write MemberId / MemberEpoch");
        let mut cur = &v9[..];
        let (gid, got, _stable) = decode_offset_fetch_request(&mut cur, 9).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(got, req);
        assert!(cur.is_empty());
        // Compact Groups of 1: "g", compact "m1", epoch 3, one topic "t" p0,
        // group tags, RequireStable 0, top tags.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x67, 0x03, 0x6d, 0x31, 0x00, 0x00, 0x00, 0x03, 0x02, 0x02, 0x74, 0x02,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(&v9[..], REQ);

        let resp = vec![FetchedOffsetTopic {
            topic: "t".into(),
            partitions: vec![FetchedOffset::new(0, 4, 0)],
        }];
        let mut r8 = BytesMut::new();
        encode_offset_fetch_response(&mut r8, 8, "g", &resp, 0).unwrap();
        let mut r9 = BytesMut::new();
        encode_offset_fetch_response(&mut r9, 9, "g", &resp, 0).unwrap();
        assert_eq!(&r8[..], &r9[..], "OffsetFetch v9 response matches v8");
    }

    #[test]
    fn heartbeat_v0_v2_match_and_v1_adds_throttle() {
        // Official JSON: "Version 1 and version 2 are the same as version 0."
        // Request: STRING "g", INT32 7, STRING "m1". Instance id is v3+.
        const REQ: &[u8] = &[
            0x00, 0x01, 0x67, 0x00, 0x00, 0x00, 0x07, 0x00, 0x02, 0x6d, 0x31,
        ];
        let mut v0 = BytesMut::new();
        encode_heartbeat_request(&mut v0, 0, "g", 7, "m1", Some("ignored-on-v0")).unwrap();
        let mut v1 = BytesMut::new();
        encode_heartbeat_request(&mut v1, 1, "g", 7, "m1", Some("ignored-on-v1")).unwrap();
        let mut v2 = BytesMut::new();
        encode_heartbeat_request(&mut v2, 2, "g", 7, "m1", Some("ignored-on-v2")).unwrap();
        assert_eq!(&v0[..], REQ);
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 request bodies match");
        assert_eq!(v1.as_ref(), v2.as_ref(), "v1 and v2 request bodies match");
        let mut cur = v0.as_ref();
        let (gid, gen, mid) = decode_heartbeat_request(&mut cur, 0).unwrap();
        assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
        assert!(cur.is_empty(), "v0 request leftover-empty");
        let mut cur = v2.as_ref();
        let (_gid, _gen, mid) = decode_heartbeat_request(&mut cur, 2).unwrap();
        assert_eq!(mid, "m1");
        assert!(cur.is_empty(), "v2 request leftover-empty");

        v0.clear();
        encode_heartbeat_response(&mut v0, 0, 0).unwrap();
        v1.clear();
        encode_heartbeat_response(&mut v1, 1, 0).unwrap();
        v2.clear();
        encode_heartbeat_response(&mut v2, 2, 0).unwrap();
        // v0: error 0. v1+: throttle 0 then error 0.
        const RESP_V0: &[u8] = &[0x00, 0x00];
        const RESP_V1: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(&v0[..], RESP_V0);
        assert_eq!(&v1[..], RESP_V1);
        assert_ne!(v0.as_ref(), v1.as_ref(), "v1 response adds ThrottleTimeMs");
        assert_eq!(v1.as_ref(), v2.as_ref(), "v1 and v2 response bodies match");
        let mut cur = v0.as_ref();
        assert_eq!(decode_heartbeat_response(&mut cur, 0).unwrap(), 0);
        assert!(cur.is_empty(), "v0 response leftover-empty");
        let mut cur = v1.as_ref();
        assert_eq!(decode_heartbeat_response(&mut cur, 1).unwrap(), 0);
        assert!(cur.is_empty(), "v1 response leftover-empty");

        v0.clear();
        let err = encode_heartbeat_request(&mut v0, 5, "g", 7, "m1", None).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v5 is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 4), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 4, 0, 4), Some(4));
        assert_eq!(crate::protocol::api_keys::pick_version(3, 4, 0, 4), Some(4));
        assert_eq!(crate::protocol::api_keys::pick_version(5, 5, 0, 4), None);
    }

    #[test]
    fn heartbeat_v3_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_heartbeat_request(&mut buf, 3, "g", 7, "m1", Some("i")).unwrap();
        let mut cur = &buf[..];
        let (gid, gen, mid) = decode_heartbeat_request(&mut cur, 3).unwrap();
        assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
        assert!(
            cur.is_empty(),
            "v3 decoder must consume instance id; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_heartbeat_response(&mut buf, 3, 0).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_heartbeat_response(&mut cur, 3).unwrap(), 0);
        assert!(cur.is_empty(), "v3 response leftover {} bytes", cur.len());
    }

    #[test]
    fn heartbeat_v4_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_heartbeat_request(&mut req, 4, "g", 7, "m1", Some("i")).unwrap();
        let mut cur = &req[..];
        let (gid, gen, mid) = decode_heartbeat_request(&mut cur, 4).unwrap();
        assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
        assert!(
            cur.is_empty(),
            "v4 decoder must consume compact strings and tagged fields; leftover {} bytes",
            cur.len()
        );

        let mut resp = BytesMut::new();
        encode_heartbeat_response(&mut resp, 4, 0).unwrap();
        let mut cur = &resp[..];
        assert_eq!(decode_heartbeat_response(&mut cur, 4).unwrap(), 0);
        assert!(
            cur.is_empty(),
            "v4 response must consume tagged fields; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn heartbeat_v4_request_matches_compact_layout() {
        // Compact "g", generation 7, compact "m1", null instance, tagged.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x00, 0x00, 0x00, 0x07, 0x03, 0x6d, 0x31, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_heartbeat_request(&mut buf, 4, "g", 7, "m1", None).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v3 = BytesMut::new();
        encode_heartbeat_request(&mut v3, 3, "g", 7, "m1", None).unwrap();
        assert_ne!(&buf[..], &v3[..], "Heartbeat v4 must not be classic v3");
        assert!(
            encode_heartbeat_request(&mut BytesMut::new(), 5, "g", 7, "m1", None).is_err(),
            "Heartbeat v5+ is not spoken"
        );
    }

    #[test]
    fn heartbeat_v4_response_matches_compact_layout() {
        // Throttle 0, error 0, tagged.
        const RESP: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut buf = BytesMut::new();
        encode_heartbeat_response(&mut buf, 4, 0).unwrap();
        assert_eq!(&buf[..], RESP);
        let mut v3 = BytesMut::new();
        encode_heartbeat_response(&mut v3, 3, 0).unwrap();
        assert_ne!(
            &buf[..],
            &v3[..],
            "Heartbeat v4 response must include tagged fields"
        );
    }

    fn sync_req(assignments: &[(String, Vec<u8>)]) -> SyncGroupRequest<'_> {
        SyncGroupRequest {
            group_id: "g",
            generation_id: 7,
            member_id: "m1",
            group_instance_id: None,
            protocol_type: "consumer",
            protocol_name: "range",
            assignments,
        }
    }

    #[test]
    fn sync_group_v0_v2_match_and_v1_adds_throttle() {
        // Official JSON: "Versions 1 and 2 are the same as version 0."
        // Request: STRING "g", INT32 7, STRING "m1", empty assignments.
        // Instance id is v3+. ProtocolType / ProtocolName are v5+.
        const REQ: &[u8] = &[
            0x00, 0x01, 0x67, 0x00, 0x00, 0x00, 0x07, 0x00, 0x02, 0x6d, 0x31, 0x00, 0x00, 0x00,
            0x00,
        ];
        let empty: [(String, Vec<u8>); 0] = [];
        let req = SyncGroupRequest {
            group_id: "g",
            generation_id: 7,
            member_id: "m1",
            group_instance_id: Some("ignored-on-v0"),
            protocol_type: "consumer",
            protocol_name: "range",
            assignments: &empty,
        };
        let mut v0 = BytesMut::new();
        encode_sync_group_request(&mut v0, 0, &req).unwrap();
        let mut v1 = BytesMut::new();
        encode_sync_group_request(&mut v1, 1, &req).unwrap();
        let mut v2 = BytesMut::new();
        encode_sync_group_request(&mut v2, 2, &req).unwrap();
        assert_eq!(&v0[..], REQ);
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 request bodies match");
        assert_eq!(v1.as_ref(), v2.as_ref(), "v1 and v2 request bodies match");
        let mut cur = v0.as_ref();
        let (gid, mid, got) = decode_sync_group_request(&mut cur, 0).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert!(got.is_empty());
        assert!(cur.is_empty(), "v0 request leftover-empty");
        let mut cur = v2.as_ref();
        let (_gid, mid, _got) = decode_sync_group_request(&mut cur, 2).unwrap();
        assert_eq!(mid, "m1");
        assert!(cur.is_empty(), "v2 request leftover-empty");

        v0.clear();
        encode_sync_group_response(&mut v0, 0, 0, &[]).unwrap();
        v1.clear();
        encode_sync_group_response(&mut v1, 1, 0, &[]).unwrap();
        v2.clear();
        encode_sync_group_response(&mut v2, 2, 0, &[]).unwrap();
        // v0: error 0, empty BYTES. v1+: throttle 0 then error 0 then BYTES.
        const RESP_V0: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const RESP_V1: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(&v0[..], RESP_V0);
        assert_eq!(&v1[..], RESP_V1);
        assert_ne!(v0.as_ref(), v1.as_ref(), "v1 response adds ThrottleTimeMs");
        assert_eq!(v1.as_ref(), v2.as_ref(), "v1 and v2 response bodies match");
        let mut cur = v0.as_ref();
        let (err, asg) = decode_sync_group_response(&mut cur, 0).unwrap();
        assert_eq!((err, asg.as_slice()), (0, &[][..]));
        assert!(cur.is_empty(), "v0 response leftover-empty");
        let mut cur = v1.as_ref();
        let (err, asg) = decode_sync_group_response(&mut cur, 1).unwrap();
        assert_eq!((err, asg.as_slice()), (0, &[][..]));
        assert!(cur.is_empty(), "v1 response leftover-empty");

        v0.clear();
        let err = encode_sync_group_request(&mut v0, 6, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v6 is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 5), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 5, 0, 5), Some(5));
        assert_eq!(crate::protocol::api_keys::pick_version(3, 5, 0, 5), Some(5));
        assert_eq!(crate::protocol::api_keys::pick_version(6, 6, 0, 5), None);
    }

    #[test]
    fn sync_group_v3_roundtrip_is_leftover_empty() {
        let assignments = vec![("m1".into(), vec![1, 2, 3])];
        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 3, &sync_req(&assignments)).unwrap();
        let mut cur = &buf[..];
        let (gid, mid, got) = decode_sync_group_request(&mut cur, 3).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got, assignments);
        assert!(
            cur.is_empty(),
            "v3 decoder must consume instance id and assignments; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_sync_group_response(&mut buf, 3, 0, &[1, 2, 3]).unwrap();
        let mut cur = &buf[..];
        let (err, asg) = decode_sync_group_response(&mut cur, 3).unwrap();
        assert_eq!((err, asg.as_slice()), (0, &[1, 2, 3][..]));
        assert!(cur.is_empty(), "v3 response leftover {} bytes", cur.len());
    }

    #[test]
    fn sync_group_v4_roundtrip_is_leftover_empty() {
        let assignments = vec![("m1".into(), vec![1, 2, 3])];
        let mut req = BytesMut::new();
        encode_sync_group_request(&mut req, 4, &sync_req(&assignments)).unwrap();
        let mut cur = &req[..];
        let (gid, mid, got) = decode_sync_group_request(&mut cur, 4).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got, assignments);
        assert!(
            cur.is_empty(),
            "v4 decoder must consume compact fields and tagged fields; leftover {} bytes",
            cur.len()
        );

        let mut resp = BytesMut::new();
        encode_sync_group_response(&mut resp, 4, 0, &[1, 2, 3]).unwrap();
        let mut cur = &resp[..];
        let (err, asg) = decode_sync_group_response(&mut cur, 4).unwrap();
        assert_eq!((err, asg.as_slice()), (0, &[1, 2, 3][..]));
        assert!(
            cur.is_empty(),
            "v4 response must consume tagged fields; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn sync_group_v5_roundtrip_is_leftover_empty() {
        let assignments = vec![("m1".into(), vec![1, 2, 3])];
        let mut req = BytesMut::new();
        encode_sync_group_request(&mut req, 5, &sync_req(&assignments)).unwrap();
        let mut cur = &req[..];
        let (gid, mid, got) = decode_sync_group_request(&mut cur, 5).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got, assignments);
        assert!(
            cur.is_empty(),
            "v5 decoder must consume ProtocolType / ProtocolName; leftover {} bytes",
            cur.len()
        );

        let mut resp = BytesMut::new();
        encode_sync_group_response(&mut resp, 5, 0, &[1, 2, 3]).unwrap();
        let mut cur = &resp[..];
        let (err, asg) = decode_sync_group_response(&mut cur, 5).unwrap();
        assert_eq!((err, asg.as_slice()), (0, &[1, 2, 3][..]));
        assert!(
            cur.is_empty(),
            "v5 response must consume ProtocolType / ProtocolName; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn sync_group_v4_request_matches_compact_layout() {
        // Compact "g", generation 7, compact "m1", null instance, empty
        // assignments, tagged. v4 has no ProtocolType / ProtocolName.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x00, 0x00, 0x00, 0x07, 0x03, 0x6d, 0x31, 0x00, 0x01, 0x00,
        ];
        let empty: [(String, Vec<u8>); 0] = [];
        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 4, &sync_req(&empty)).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v3 = BytesMut::new();
        encode_sync_group_request(&mut v3, 3, &sync_req(&empty)).unwrap();
        assert_ne!(&buf[..], &v3[..], "SyncGroup v4 must not be classic v3");
        assert!(
            encode_sync_group_request(&mut BytesMut::new(), 6, &sync_req(&empty)).is_err(),
            "SyncGroup v6+ is not spoken"
        );
    }

    #[test]
    fn sync_group_v5_request_matches_compact_layout() {
        // v4 body plus compact "consumer" / "range" after instance.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x00, 0x00, 0x00, 0x07, 0x03, 0x6d, 0x31, 0x00, 0x09, 0x63, 0x6f, 0x6e,
            0x73, 0x75, 0x6d, 0x65, 0x72, 0x06, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x01, 0x00,
        ];
        let empty: [(String, Vec<u8>); 0] = [];
        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 5, &sync_req(&empty)).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v4 = BytesMut::new();
        encode_sync_group_request(&mut v4, 4, &sync_req(&empty)).unwrap();
        assert_ne!(
            &buf[..],
            &v4[..],
            "SyncGroup v5 must include ProtocolType / ProtocolName"
        );
    }

    #[test]
    fn sync_group_v4_response_matches_compact_layout() {
        // Throttle 0, error 0, compact empty assignment, tagged.
        const RESP: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        let mut buf = BytesMut::new();
        encode_sync_group_response(&mut buf, 4, 0, &[]).unwrap();
        assert_eq!(&buf[..], RESP);
        let mut v3 = BytesMut::new();
        encode_sync_group_response(&mut v3, 3, 0, &[]).unwrap();
        assert_ne!(
            &buf[..],
            &v3[..],
            "SyncGroup v4 response must use compact bytes"
        );
    }

    #[test]
    fn sync_group_v5_response_matches_compact_layout() {
        // Throttle 0, error 0, null ProtocolType / ProtocolName, compact
        // empty assignment, tagged.
        const RESP: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        let mut buf = BytesMut::new();
        encode_sync_group_response(&mut buf, 5, 0, &[]).unwrap();
        assert_eq!(&buf[..], RESP);
        let mut v4 = BytesMut::new();
        encode_sync_group_response(&mut v4, 4, 0, &[]).unwrap();
        assert_ne!(
            &buf[..],
            &v4[..],
            "SyncGroup v5 response must include ProtocolType / ProtocolName"
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
    fn leave_group_v0_v1_v2_match_and_do_not_speak_v6() {
        // Official Kafka 4.0 JSON: validVersions 0-5, flexibleVersions 4+.
        // "Version 1 and 2 are the same as version 0." MemberId is v0–v2.
        // This crate speaks 0–5. v6+ is not spoken.
        let members = [LeaveGroupMember {
            member_id: "m1".into(),
            group_instance_id: Some("ignored-on-v0".into()),
            reason: Some("ignored-on-v0".into()),
        }];
        let mut v0 = BytesMut::new();
        encode_leave_group_request_members(&mut v0, 0, "g", &members).unwrap();
        let mut v1 = BytesMut::new();
        encode_leave_group_request_members(&mut v1, 1, "g", &members).unwrap();
        let mut v2 = BytesMut::new();
        encode_leave_group_request_members(&mut v2, 2, "g", &members).unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 request bodies match");
        assert_eq!(v1.as_ref(), v2.as_ref(), "v1 and v2 request bodies match");
        let mut cur = v0.as_ref();
        let (gid, got) = decode_leave_group_request_version(&mut cur, 0).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(got[0].member_id, "m1");
        assert_eq!(got[0].group_instance_id, None);
        assert!(!cur.has_remaining(), "v0 request leftover-empty");
        let mut cur = v2.as_ref();
        let (_gid, got) = decode_leave_group_request_version(&mut cur, 2).unwrap();
        assert_eq!(got[0].member_id, "m1");
        assert_eq!(got[0].group_instance_id, None);
        assert!(!cur.has_remaining(), "v2 request leftover-empty");

        v0.clear();
        encode_leave_group_response_version(&mut v0, 0, 0, &[]).unwrap();
        v1.clear();
        encode_leave_group_response_version(&mut v1, 1, 0, &[]).unwrap();
        v2.clear();
        encode_leave_group_response_version(&mut v2, 2, 0, &[]).unwrap();
        assert_ne!(v0.as_ref(), v1.as_ref(), "v1 response adds ThrottleTimeMs");
        assert_eq!(v1.as_ref(), v2.as_ref(), "v1 and v2 response bodies match");
        let mut cur = v0.as_ref();
        assert_eq!(
            decode_leave_group_response_version(&mut cur, 0).unwrap().0,
            0
        );
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
        let mut cur = v1.as_ref();
        assert_eq!(
            decode_leave_group_response_version(&mut cur, 1).unwrap().0,
            0
        );
        assert!(!cur.has_remaining(), "v1 response leftover-empty");

        v0.clear();
        let err = encode_leave_group_request_members(&mut v0, 6, "g", &members).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v6 is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 5), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 5, 0, 5), Some(5));
        assert_eq!(crate::protocol::api_keys::pick_version(3, 5, 0, 5), Some(5));
        assert_eq!(crate::protocol::api_keys::pick_version(6, 6, 0, 5), None);
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
