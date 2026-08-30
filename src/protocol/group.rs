//! Consumer group codecs: FindCoordinator, Join/Sync/Heartbeat/Leave,
//! OffsetCommit/OffsetFetch, OffsetDelete, and ConsumerProtocol assignment.

use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use super::records::RecordBatch;
use crate::error::{error_name, Error, Result};

/// FindCoordinator `key_type` for a consumer group.
pub const COORDINATOR_GROUP: i8 = 0;
/// FindCoordinator `key_type` for a transactional.id (KIP-98).
pub const COORDINATOR_TRANSACTION: i8 = 1;
/// FindCoordinator `key_type` for a share group (KIP-932).
pub const COORDINATOR_SHARE: i8 = 2;
/// Java `FindCoordinatorRequest.MIN_BATCHED_VERSION` (KIP-699 CoordinatorKeys).
pub const MIN_BATCHED_VERSION: i16 = 4;

/// Java `FindCoordinatorRequest.CoordinatorType`.
///
/// [`Display`] is Java `CoordinatorType.toString` (`GROUP`). [`Self::from_id`]
/// is Java `CoordinatorType.forId` (unknown is `None`; Java throws).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordinatorType {
    /// Java `GROUP` (`key_type` 0).
    Group,
    /// Java `TRANSACTION` (`key_type` 1).
    Transaction,
    /// Java `SHARE` (`key_type` 2).
    Share,
}

impl CoordinatorType {
    /// Java `CoordinatorType.id`.
    #[must_use]
    pub const fn id(self) -> i8 {
        match self {
            Self::Group => COORDINATOR_GROUP,
            Self::Transaction => COORDINATOR_TRANSACTION,
            Self::Share => COORDINATOR_SHARE,
        }
    }

    /// Java `CoordinatorType.forId`. Unknown values return `None`.
    #[must_use]
    pub const fn from_id(id: i8) -> Option<Self> {
        match id {
            COORDINATOR_GROUP => Some(Self::Group),
            COORDINATOR_TRANSACTION => Some(Self::Transaction),
            COORDINATOR_SHARE => Some(Self::Share),
            _ => None,
        }
    }

    /// Java `CoordinatorType.toString` (`GROUP` / `TRANSACTION` / `SHARE`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Group => "GROUP",
            Self::Transaction => "TRANSACTION",
            Self::Share => "SHARE",
        }
    }
}

impl fmt::Display for CoordinatorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Java `ConsumerProtocol` (classic JoinGroup / SyncGroup protocol type).
pub struct ConsumerProtocol;

impl ConsumerProtocol {
    /// Java `ConsumerProtocol.PROTOCOL_TYPE`.
    pub const PROTOCOL_TYPE: &'static str = "consumer";
    /// Java `ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION` /
    /// `ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION`.
    pub const LOWEST_SUPPORTED_VERSION: i16 = 0;
    /// Java `ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION` /
    /// `ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION` (`0-3`).
    pub const HIGHEST_SUPPORTED_VERSION: i16 = 3;

    /// Java `ConsumerProtocol.deserializeVersion`.
    pub fn deserialize_version(bytes: &[u8]) -> Result<i16> {
        let mut bytes = bytes;
        buf::get_i16(&mut bytes)
    }

    /// Java `ConsumerProtocol.serializeSubscription` at
    /// [`Self::HIGHEST_SUPPORTED_VERSION`].
    pub fn serialize_subscription(subscription: &ConsumerProtocolSubscription) -> Result<Vec<u8>> {
        Self::serialize_subscription_version(subscription, Self::HIGHEST_SUPPORTED_VERSION)
    }

    /// Java `ConsumerProtocol.serializeSubscription` at `version`.
    ///
    /// Versions above [`Self::HIGHEST_SUPPORTED_VERSION`] encode as that
    /// cap (Java `checkSubscriptionVersion`). Topics are sorted. Owned
    /// partitions are sorted by topic then partition.
    pub fn serialize_subscription_version(
        subscription: &ConsumerProtocolSubscription,
        version: i16,
    ) -> Result<Vec<u8>> {
        let version = check_consumer_protocol_version(version)?;
        let mut topics = subscription.topics.clone();
        topics.sort();
        let mut buf = BytesMut::new();
        buf.put_i16(version);
        buf::put_array_len(&mut buf, false, Some(topics.len()))?;
        for t in &topics {
            buf::put_classic_nullable_string(&mut buf, Some(t))?;
        }
        buf::put_classic_bytes(&mut buf, None)?;
        if version >= 1 {
            let by_topic = group_sorted_owned_partitions(&subscription.owned_partitions);
            buf::put_array_len(&mut buf, false, Some(by_topic.len()))?;
            for (topic, parts) in &by_topic {
                buf::put_classic_nullable_string(&mut buf, Some(topic))?;
                buf::put_array_len(&mut buf, false, Some(parts.len()))?;
                for p in parts {
                    buf.put_i32(*p);
                }
            }
        }
        if version >= 2 {
            buf.put_i32(subscription.generation_id);
        }
        if version >= 3 {
            let rack = subscription.rack_id.as_deref().filter(|s| !s.is_empty());
            buf::put_classic_nullable_string(&mut buf, rack)?;
        }
        Ok(buf.to_vec())
    }

    /// Java `ConsumerProtocol.deserializeSubscription`.
    ///
    /// Empty bytes are an empty subscription. Versions above
    /// [`Self::HIGHEST_SUPPORTED_VERSION`] parse with that schema. v2+
    /// omitted `GenerationId` is [`ConsumerProtocolSubscription::DEFAULT_GENERATION`].
    /// Null or empty `RackId` is `None`.
    pub fn deserialize_subscription(bytes: &[u8]) -> Result<ConsumerProtocolSubscription> {
        if bytes.is_empty() {
            return Ok(ConsumerProtocolSubscription::default());
        }
        let mut bytes = bytes;
        let raw = buf::get_i16(&mut bytes)?;
        let ver = check_consumer_protocol_version(raw)?;
        let n = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(n);
        for _ in 0..n {
            topics.push(buf::get_classic_nullable_string(&mut bytes)?.unwrap_or_default());
        }
        let mut owned = Vec::new();
        let mut generation_id = ConsumerProtocolSubscription::DEFAULT_GENERATION;
        let mut rack_id = None;
        if bytes.is_empty() {
            return Ok(ConsumerProtocolSubscription {
                topics,
                owned_partitions: owned,
                generation_id,
                rack_id,
            });
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
        if ver >= 2 && !bytes.is_empty() {
            generation_id = buf::get_i32(&mut bytes)?;
        }
        if ver >= 3 && !bytes.is_empty() {
            let rack = buf::get_classic_nullable_string(&mut bytes)?;
            rack_id = rack.filter(|s| !s.is_empty());
        }
        Ok(ConsumerProtocolSubscription {
            topics,
            owned_partitions: owned,
            generation_id,
            rack_id,
        })
    }
}

/// Java `ConsumerPartitionAssignor.Subscription` /
/// `ConsumerProtocolSubscription` (classic JoinGroup member metadata).
///
/// [`ConsumerProtocol::serialize_subscription`] is Java
/// `ConsumerProtocol.serializeSubscription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerProtocolSubscription {
    /// Subscribed topic names (Java `Subscription.topics`).
    pub topics: Vec<String>,
    /// Currently owned topic-partitions (Java `Subscription.ownedPartitions`;
    /// v1+).
    pub owned_partitions: Vec<(String, i32)>,
    /// Member generation (Java `Subscription.generationId`; v2+).
    /// [`Self::DEFAULT_GENERATION`] when unknown.
    pub generation_id: i32,
    /// `client.rack` (Java `Subscription.rackId`; v3+).
    pub rack_id: Option<String>,
}

impl ConsumerProtocolSubscription {
    /// Java `AbstractStickyAssignor.DEFAULT_GENERATION` (`Subscription`
    /// `generationId` when unknown).
    pub const DEFAULT_GENERATION: i32 = -1;

    /// Java `Subscription(List)` (no owned partitions, default generation,
    /// no rack).
    #[must_use]
    pub fn new(topics: Vec<String>) -> Self {
        Self {
            topics,
            owned_partitions: Vec::new(),
            generation_id: Self::DEFAULT_GENERATION,
            rack_id: None,
        }
    }
}

impl Default for ConsumerProtocolSubscription {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// Cap `version` to [`ConsumerProtocol::HIGHEST_SUPPORTED_VERSION`].
///
/// Java `ConsumerProtocol.checkSubscriptionVersion` /
/// `checkAssignmentVersion`: below lowest is an error; above highest
/// uses the highest schema.
fn check_consumer_protocol_version(version: i16) -> Result<i16> {
    if version < ConsumerProtocol::LOWEST_SUPPORTED_VERSION {
        return Err(Error::protocol(format!(
            "Unsupported consumer protocol version: {version}"
        )));
    }
    if version > ConsumerProtocol::HIGHEST_SUPPORTED_VERSION {
        Ok(ConsumerProtocol::HIGHEST_SUPPORTED_VERSION)
    } else {
        Ok(version)
    }
}

/// Group owned partitions by topic after sorting by topic then partition
/// (Java `ConsumerProtocol.serializeSubscription`).
fn group_sorted_owned_partitions(owned: &[(String, i32)]) -> Vec<(String, Vec<i32>)> {
    let mut sorted = owned.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
    for (topic, part) in sorted {
        match by_topic.last_mut() {
            Some((t, ps)) if *t == topic => ps.push(part),
            _ => by_topic.push((topic, vec![part])),
        }
    }
    by_topic
}

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
    version >= MIN_BATCHED_VERSION
}

/// One coordinator in a FindCoordinator response (v1–v6).
///
/// v1–v3 have a single top-level coordinator (`key` is empty). v4+ is
/// Coordinators[] (KIP-699); `key` is `Coordinators[].Key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorResult {
    /// Coordinator key (group id, transactional.id, …). Empty on v1–v3.
    pub key: String,
    /// Broker node id (`-1` when ErrorCode is non-zero).
    pub node_id: i32,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Nullable error message (v4+ Coordinators[].ErrorMessage; v1–v3
    /// top-level ErrorMessage).
    pub error_message: Option<String>,
}

/// Encode FindCoordinator for a consumer group id.
pub fn encode_find_coordinator_request(
    buf: &mut BytesMut,
    version: i16,
    key: &str,
) -> crate::error::Result<()> {
    encode_find_coordinator_request_typed(buf, version, key, CoordinatorType::Group.id())
}

/// Encode FindCoordinator with an explicit `key_type` (one key).
pub fn encode_find_coordinator_request_typed(
    buf: &mut BytesMut,
    version: i16,
    key: &str,
    key_type: i8,
) -> crate::error::Result<()> {
    encode_find_coordinator_request_keys(buf, version, &[key], key_type)
}

/// Encode FindCoordinator v4+ CoordinatorKeys of N (KIP-699).
///
/// v1–v3 support one key only (`does not support CoordinatorKeys` when
/// `keys.len() != 1`).
pub fn encode_find_coordinator_request_keys(
    buf: &mut BytesMut,
    version: i16,
    keys: &[&str],
    key_type: i8,
) -> crate::error::Result<()> {
    let flexible = find_coordinator_flexible(version)?;
    if find_coordinator_batched(version) {
        buf.put_i8(key_type);
        buf::put_array_len(buf, true, Some(keys.len()))?;
        for key in keys {
            buf::put_compact_string(buf, Some(key))?;
        }
        buf::put_empty_tagged_fields(buf);
        return Ok(());
    }
    if keys.len() != 1 {
        return Err(Error::protocol(format!(
            "FindCoordinator version {version} does not support CoordinatorKeys"
        )));
    }
    let key = keys.first().copied().unwrap_or("");
    buf::put_string(buf, flexible, Some(key))?;
    buf.put_i8(key_type);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode FindCoordinator: `(key, key_type)` (first CoordinatorKey).
pub fn decode_find_coordinator_request<B: Buf>(buf: &mut B, version: i16) -> Result<(String, i8)> {
    let (keys, key_type) = decode_find_coordinator_request_keys(buf, version)?;
    let key = keys.into_iter().next().unwrap_or_default();
    Ok((key, key_type))
}

/// Decode FindCoordinator: every CoordinatorKey plus KeyType.
///
/// v1–v3 return a vec of 1 (the Key field). v4+ is CoordinatorKeys.
pub fn decode_find_coordinator_request_keys<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<String>, i8)> {
    let flexible = find_coordinator_flexible(version)?;
    if find_coordinator_batched(version) {
        let key_type = buf::get_i8(buf)?;
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut keys = Vec::with_capacity(n);
        for _ in 0..n {
            keys.push(buf::get_compact_string(buf)?.unwrap_or_default());
        }
        buf::skip_tagged_fields(buf)?;
        return Ok((keys, key_type));
    }
    let key = buf::get_string(buf, flexible)?.unwrap_or_default();
    let key_type = buf::get_i8(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((vec![key], key_type))
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
    encode_find_coordinator_response_coordinators(
        buf,
        version,
        &[CoordinatorResult {
            key: key.to_string(),
            node_id,
            host: host.to_string(),
            port,
            error_code: 0,
            error_message: None,
        }],
    )
}

/// Encode FindCoordinator v4+ Coordinators of N (KIP-699).
///
/// v1–v3 support one coordinator only (`does not support Coordinators`
/// when `coordinators.len() != 1`).
pub fn encode_find_coordinator_response_coordinators(
    buf: &mut BytesMut,
    version: i16,
    coordinators: &[CoordinatorResult],
) -> crate::error::Result<()> {
    let flexible = find_coordinator_flexible(version)?;
    buf.put_i32(0);
    if find_coordinator_batched(version) {
        buf::put_array_len(buf, true, Some(coordinators.len()))?;
        for c in coordinators {
            buf::put_compact_string(buf, Some(&c.key))?;
            buf.put_i32(c.node_id);
            buf::put_compact_string(buf, Some(&c.host))?;
            buf.put_i32(c.port);
            buf.put_i16(c.error_code);
            buf::put_compact_string(buf, c.error_message.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
        return Ok(());
    }
    if coordinators.len() != 1 {
        return Err(Error::protocol(format!(
            "FindCoordinator version {version} does not support Coordinators"
        )));
    }
    let c = coordinators
        .first()
        .ok_or_else(|| Error::protocol("missing FindCoordinator Coordinators"))?;
    buf.put_i16(c.error_code);
    buf::put_string(buf, flexible, c.error_message.as_deref())?;
    buf.put_i32(c.node_id);
    buf::put_string(buf, flexible, Some(&c.host))?;
    buf.put_i32(c.port);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode FindCoordinator: `(error_code, node_id, host, port)` (first
/// Coordinators entry).
pub fn decode_find_coordinator_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i32, String, i32)> {
    let coords = decode_find_coordinator_response_coordinators(buf, version)?;
    let c = coords
        .into_iter()
        .next()
        .ok_or_else(|| Error::protocol("missing FindCoordinator Coordinators"))?;
    Ok((c.error_code, c.node_id, c.host, c.port))
}

/// Decode FindCoordinator v0–v6: every Coordinators entry.
///
/// v1–v3 return a vec of 1 (`key` empty). v4+ is Coordinators[].
pub fn decode_find_coordinator_response_coordinators<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<CoordinatorResult>> {
    let flexible = find_coordinator_flexible(version)?;
    let _throttle = buf::get_i32(buf)?;
    if find_coordinator_batched(version) {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let key = buf::get_compact_string(buf)?.unwrap_or_default();
            let node_id = buf::get_i32(buf)?;
            let host = buf::get_compact_string(buf)?.unwrap_or_default();
            let port = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let error_message = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            out.push(CoordinatorResult {
                key,
                node_id,
                host,
                port,
                error_code,
                error_message,
            });
        }
        buf::skip_tagged_fields(buf)?;
        return Ok(out);
    }
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_string(buf, flexible)?;
    let node_id = buf::get_i32(buf)?;
    let host = buf::get_string(buf, flexible)?.unwrap_or_default();
    let port = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(vec![CoordinatorResult {
        key: String::new(),
        node_id,
        host,
        port,
        error_code,
        error_message,
    }])
}

/// `true` when JoinGroup `version` is flexible (v6+).
///
/// v2–v5 are classic. v6 is compact strings/bytes/arrays plus tagged
/// fields (Apache JSON `flexibleVersions: "6+"`). Official JSON: v2 and
/// v3 match v1 (RebalanceTimeoutMs). v4 second join with assigned id.
/// v5 GroupInstanceId. v7 response ProtocolType / nullable ProtocolName
/// (KIP-559). v8 Reason (KIP-800). v9 SkipAssignment. Kafka 4.0
/// `validVersions` is `2-9` (v0–v1 removed). This crate speaks 2–9.
/// v0–v1 and v10+ are not spoken.
fn join_group_flexible(version: i16) -> Result<bool> {
    match version {
        2..=5 => Ok(false),
        6..=9 => Ok(true),
        other => Err(Error::protocol(format!(
            "JoinGroup version {other} is not implemented"
        ))),
    }
}

/// JoinGroup request (classic v2–v5 or flexible v6–v9).
#[derive(Debug, Clone, Copy)]
pub struct JoinGroupRequest<'a> {
    /// Group id.
    pub group_id: &'a str,
    /// Session timeout.
    pub session_timeout_ms: i32,
    /// Member id ([`Self::UNKNOWN_MEMBER_ID`] on first join).
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

impl JoinGroupRequest<'_> {
    /// Java `JoinGroupRequest.UNKNOWN_MEMBER_ID`.
    pub const UNKNOWN_MEMBER_ID: &'static str = "";
    /// Java `JoinGroupRequest.UNKNOWN_GENERATION_ID`.
    pub const UNKNOWN_GENERATION_ID: i32 = -1;
    /// Java `JoinGroupRequest.UNKNOWN_PROTOCOL_NAME`.
    pub const UNKNOWN_PROTOCOL_NAME: &'static str = "";

    /// Java `JoinGroupRequest.maybeTruncateReason` (KIP-800; 255 chars).
    #[must_use]
    pub fn maybe_truncate_reason(reason: &str) -> String {
        const MAX: usize = 255;
        reason.chars().take(MAX).collect()
    }

    /// Java `JoinGroupRequest.requiresKnownMemberId(short)` (KIP-394; v4+).
    #[must_use]
    pub const fn requires_known_member_id(api_version: i16) -> bool {
        api_version >= 4
    }

    /// Java `JoinGroupRequest.requiresKnownMemberId(JoinGroupRequestData, short)`.
    ///
    /// Dynamic members on JoinGroup v4+ with [`Self::UNKNOWN_MEMBER_ID`] must
    /// rejoin after `MEMBER_ID_REQUIRED`. Static members (`group.instance.id`)
    /// and JoinGroup v2–v3 join in one RPC.
    #[must_use]
    pub fn requires_known_member_id_for(
        member_id: &str,
        group_instance_id: Option<&str>,
        api_version: i16,
    ) -> bool {
        group_instance_id.is_none()
            && member_id == Self::UNKNOWN_MEMBER_ID
            && Self::requires_known_member_id(api_version)
    }

    /// Java `JoinGroupRequest.supportsSkippingAssignment`.
    #[must_use]
    pub const fn supports_skipping_assignment(api_version: i16) -> bool {
        api_version >= 9
    }
}

/// Java `JoinGroupResponse` helpers.
pub struct JoinGroupResponse;

impl JoinGroupResponse {
    /// Java `JoinGroupResponse.isLeader` (`memberId.equals(leader)`).
    #[must_use]
    pub fn is_leader(member_id: &str, leader: &str) -> bool {
        member_id == leader
    }
}

/// One JoinGroup Protocols entry (Java `partition.assignment.strategy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinGroupProtocol<'a> {
    /// Protocol name (`"range"`, `"sticky"`, `"cooperative-sticky"`).
    pub name: &'a str,
    /// Subscription metadata bytes for this assignor.
    pub metadata: &'a [u8],
}

impl<'a> JoinGroupProtocol<'a> {
    /// Protocol `name` with `metadata`.
    #[must_use]
    pub fn new(name: &'a str, metadata: &'a [u8]) -> Self {
        Self { name, metadata }
    }
}

/// Owned JoinGroup Protocols entry (decode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupProtocolOwned {
    /// Protocol name.
    pub name: String,
    /// Subscription metadata bytes for this assignor.
    pub metadata: Vec<u8>,
}

/// JoinGroup request with Protocols of N.
#[derive(Debug, Clone, Copy)]
pub struct JoinGroupProtocolsRequest<'a> {
    /// Group id.
    pub group_id: &'a str,
    /// Session timeout.
    pub session_timeout_ms: i32,
    /// Member id ([`JoinGroupRequest::UNKNOWN_MEMBER_ID`] on first join).
    pub member_id: &'a str,
    /// Kafka `group.instance.id`.
    pub group_instance_id: Option<&'a str>,
    /// Protocol type (`"consumer"`).
    pub protocol_type: &'a str,
    /// Assignors in preference order (Java `partition.assignment.strategy`).
    pub protocols: &'a [JoinGroupProtocol<'a>],
    /// Why the member (re-)joins (v8+, KIP-800). `None` is a null reason.
    pub reason: Option<&'a str>,
}

/// Encode JoinGroup v2–v9.
///
/// Kafka 4.0 JSON: `validVersions: "2-9"`, `flexibleVersions: "6+"`.
/// v2–v4 are GroupId, SessionTimeoutMs, RebalanceTimeoutMs, MemberId,
/// ProtocolType, and Protocols (v2 and v3 match). v5 GroupInstanceId.
/// v6 flexible. v8 Reason. This crate speaks 2–9. v0–v1 and v10+ are
/// not spoken. Protocols of 1.
pub fn encode_join_group_request(
    buf: &mut BytesMut,
    version: i16,
    req: &JoinGroupRequest<'_>,
) -> crate::error::Result<()> {
    encode_join_group_protocols_request(
        buf,
        version,
        &JoinGroupProtocolsRequest {
            group_id: req.group_id,
            session_timeout_ms: req.session_timeout_ms,
            member_id: req.member_id,
            group_instance_id: req.group_instance_id,
            protocol_type: req.protocol_type,
            protocols: &[JoinGroupProtocol::new(req.protocol_name, req.metadata)],
            reason: req.reason,
        },
    )
}

/// Encode JoinGroup with Protocols of N (v2–v9).
pub fn encode_join_group_protocols_request(
    buf: &mut BytesMut,
    version: i16,
    req: &JoinGroupProtocolsRequest<'_>,
) -> crate::error::Result<()> {
    let flexible = join_group_flexible(version)?;
    buf::put_string(buf, flexible, Some(req.group_id))?;
    buf.put_i32(req.session_timeout_ms);
    buf.put_i32(req.session_timeout_ms); // rebalance timeout
    buf::put_string(buf, flexible, Some(req.member_id))?;
    if version >= 5 {
        buf::put_string(buf, flexible, req.group_instance_id)?;
    }
    buf::put_string(buf, flexible, Some(req.protocol_type))?;
    buf::put_array_len(buf, flexible, Some(req.protocols.len()))?;
    for p in req.protocols {
        buf::put_string(buf, flexible, Some(p.name))?;
        buf::put_bytes(buf, flexible, Some(p.metadata))?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if version >= 8 {
        buf::put_string(buf, true, req.reason)?;
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode JoinGroup: `(group_id, member_id, instance_id, metadata, reason)`.
///
/// `reason` is `None` below v8 (KIP-800). `metadata` is the first Protocols
/// entry; use [`decode_join_group_request_protocols`] for Protocols of N.
#[expect(
    clippy::type_complexity,
    reason = "decoded JoinGroup is group, member, instance, metadata, reason"
)]
pub fn decode_join_group_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, Option<String>, Vec<u8>, Option<String>)> {
    let (group_id, member_id, instance, protocols, reason) =
        decode_join_group_request_protocols(buf, version)?;
    let metadata = protocols
        .first()
        .map(|p| p.metadata.clone())
        .unwrap_or_default();
    Ok((group_id, member_id, instance, metadata, reason))
}

/// Decode JoinGroup Protocols of N (v2–v9).
#[expect(
    clippy::type_complexity,
    reason = "decoded JoinGroup is group, member, instance, protocols, reason"
)]
pub fn decode_join_group_request_protocols<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    String,
    String,
    Option<String>,
    Vec<JoinGroupProtocolOwned>,
    Option<String>,
)> {
    let flexible = join_group_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _session = buf::get_i32(buf)?;
    let _rebalance = buf::get_i32(buf)?;
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let instance = if version >= 5 {
        buf::get_string(buf, flexible)?
    } else {
        None
    };
    let _ptype = buf::get_string(buf, flexible)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut protocols = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let metadata = buf::get_bytes(buf, flexible)?.unwrap_or_default();
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        protocols.push(JoinGroupProtocolOwned { name, metadata });
    }
    let reason = if version >= 8 {
        buf::get_string(buf, true)?
    } else {
        None
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((group_id, member_id, instance, protocols, reason))
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
///
/// Java `JoinGroupResponse` writes empty [`JoinGroupRequest::UNKNOWN_PROTOCOL_NAME`]
/// as a null ProtocolName on v7+ (nullable) and as an empty string below v7.
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
    let protocol_name = if version >= 7 && protocol_name.is_empty() {
        None
    } else {
        Some(protocol_name)
    };
    buf::put_string(buf, flexible, protocol_name)?;
    buf::put_string(buf, flexible, Some(leader))?;
    if JoinGroupRequest::supports_skipping_assignment(version) {
        buf.put_u8(0);
    }
    buf::put_string(buf, flexible, Some(member_id))?;
    buf::put_array_len(buf, flexible, Some(members.len()))?;
    for m in members {
        buf::put_string(buf, flexible, Some(&m.member_id))?;
        if version >= 5 {
            buf::put_string(buf, flexible, None)?;
        }
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
    let skip_assignment = if JoinGroupRequest::supports_skipping_assignment(version) {
        buf::get_bool(buf)?
    } else {
        false
    };
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut members = Vec::with_capacity(n);
    for _ in 0..n {
        let mid = buf::get_string(buf, flexible)?.unwrap_or_default();
        if version >= 5 {
            let _inst = buf::get_string(buf, flexible)?;
        }
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
///
/// Java `LeaveGroupRequest.Builder` rejects an empty member list
/// and rejects more than one member below v3. Instance id and reason
/// on v0–v2 are omitted (not an error).
pub fn encode_leave_group_request_members(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    members: &[LeaveGroupMember],
) -> crate::error::Result<()> {
    let flexible = leave_group_flexible(version)?;
    if members.is_empty() {
        return Err(Error::protocol("leaving members should not be empty"));
    }
    if version < 3 && members.len() != 1 {
        return Err(Error::Unsupported(format!(
            "Version {version} leave group request only supports single member instance than {} members",
            members.len()
        )));
    }
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

/// One partition in OffsetCommit v2–v9 / OffsetFetch v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetPartition {
    /// Partition index.
    pub partition: i32,
    /// Committed offset.
    pub offset: i64,
    /// Leader epoch, or [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub leader_epoch: i32,
    /// Commit metadata string ([`FetchedOffset::NO_METADATA`] when empty).
    pub metadata: String,
}

impl OffsetPartition {
    /// Offset and partition with unknown epoch and
    /// [`FetchedOffset::NO_METADATA`].
    #[must_use]
    pub fn new(partition: i32, offset: i64) -> Self {
        Self {
            partition,
            offset,
            leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            metadata: FetchedOffset::NO_METADATA.into(),
        }
    }
}

/// Topic + partitions for OffsetCommit v2–v9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions to commit.
    pub partitions: Vec<OffsetPartition>,
}

/// Topic + partition indexes for OffsetFetch v1–v9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to fetch.
    pub partitions: Vec<i32>,
}

/// One partition in an OffsetFetch v1–v9 response.
///
/// Java `OffsetFetchResponse.PartitionData` plus the partition index.
/// [`Self::INVALID_OFFSET`] / [`Self::NO_METADATA`] / [`Self::has_error`]
/// are Java `OffsetFetchResponse.INVALID_OFFSET` / `NO_METADATA` /
/// `PartitionData.hasError`. [`Display`] is Java `PartitionData.toString`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedOffset {
    /// Partition index.
    pub partition: i32,
    /// Committed offset, or [`Self::INVALID_OFFSET`] when none.
    pub offset: i64,
    /// Leader epoch, or [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub leader_epoch: i32,
    /// Commit metadata string ([`Self::NO_METADATA`] when empty).
    pub metadata: String,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl FetchedOffset {
    /// Java `OffsetFetchResponse.INVALID_OFFSET`.
    pub const INVALID_OFFSET: i64 = -1;
    /// Java `OffsetFetchResponse.NO_METADATA`.
    pub const NO_METADATA: &'static str = "";

    /// Offset with unknown epoch and [`Self::NO_METADATA`].
    #[must_use]
    pub fn new(partition: i32, offset: i64, error_code: i16) -> Self {
        Self {
            partition,
            offset,
            leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            metadata: Self::NO_METADATA.into(),
            error_code,
        }
    }

    /// Java `OffsetFetchResponse.PartitionData.hasError`.
    #[must_use]
    pub fn has_error(&self) -> bool {
        self.error_code != 0
    }
}

impl fmt::Display for FetchedOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PartitionData(offset=")?;
        write!(f, "{}", self.offset)?;
        f.write_str(", leaderEpoch=")?;
        write!(f, "{}", self.leader_epoch)?;
        f.write_str(", metadata=")?;
        f.write_str(&self.metadata)?;
        f.write_str(", error='")?;
        f.write_str(error_name(self.error_code).unwrap_or("UNKNOWN_SERVER_ERROR"))?;
        f.write_str("')")
    }
}

/// Topic + committed offsets from OffsetFetch v1–v9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedOffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions in this topic.
    pub partitions: Vec<FetchedOffset>,
}

/// One group in OffsetFetch v8+ (KIP-709).
///
/// v1–v7 encode a single group as GroupId + Topics. v9 MemberId /
/// MemberEpoch are null / `-1` for classic admin fetches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchGroup {
    /// Consumer group id.
    pub group_id: String,
    /// Member id (v9). `None` is null.
    pub member_id: Option<String>,
    /// Member epoch (v9). `-1` when unknown.
    pub member_epoch: i32,
    /// Topic-partitions to fetch. `None` is null Topics (all committed
    /// partitions; v2+).
    pub topics: Option<Vec<OffsetFetchTopic>>,
}

impl OffsetFetchGroup {
    /// Group id and optional Topics. MemberId is null; MemberEpoch is `-1`.
    #[must_use]
    pub fn new(group_id: impl Into<String>, topics: Option<Vec<OffsetFetchTopic>>) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: None,
            member_epoch: -1,
            topics,
        }
    }
}

/// One group's OffsetFetch v8+ result (KIP-709).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchGroupResult {
    /// Consumer group id. Empty on v1–v7 (those versions have no Groups).
    pub group_id: String,
    /// Committed offsets for this group.
    pub topics: Vec<FetchedOffsetTopic>,
    /// Group-level error code (`0` is success).
    pub error_code: i16,
}

/// `true` when OffsetCommit `version` is flexible (v8+).
///
/// v2–v7 are classic. v8–v9 are compact strings/arrays plus tagged fields
/// on partitions / topics / top-level (Apache JSON `flexibleVersions:
/// "8+"`). Official JSON: v3 and v4 match v2 (RetentionTimeMs). v5
/// drops retention. v6 CommittedLeaderEpoch. v7 GroupInstanceId. v9 is
/// KIP-848 error codes with the same layout as v8. Kafka 4.0
/// `validVersions` is `2-9` (v0–v1 removed). This crate speaks 2–9.
/// v0–v1 and v10+ are not spoken.
fn offset_commit_flexible(version: i16) -> Result<bool> {
    match version {
        2..=7 => Ok(false),
        8..=9 => Ok(true),
        other => Err(Error::protocol(format!(
            "OffsetCommit version {other} is not implemented"
        ))),
    }
}

/// Java `OffsetCommitRequest.DEFAULT_GENERATION_ID`.
pub const DEFAULT_GENERATION_ID: i32 = -1;
/// Java `OffsetCommitRequest.DEFAULT_MEMBER_ID`.
pub const DEFAULT_MEMBER_ID: &str = "";
/// Java `OffsetCommitRequest.DEFAULT_RETENTION_TIME` (v2–v4 RetentionTimeMs).
pub const DEFAULT_RETENTION_TIME: i64 = -1;

/// Encode OffsetCommit v2–v9.
///
/// Kafka 4.0 JSON: `validVersions: "2-9"`, `flexibleVersions: "8+"`.
/// v2–v4 send [`DEFAULT_RETENTION_TIME`] after MemberId. v5 omits retention.
/// v6 CommittedLeaderEpoch. v7 GroupInstanceId. v8 flexible. v9 matches
/// v8. This crate speaks 2–9. v0–v1 and v10+ are not spoken.
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
    if version >= 7 {
        buf::put_string(buf, flexible, group_instance_id)?;
    }
    if (2..=4).contains(&version) {
        buf.put_i64(DEFAULT_RETENTION_TIME);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i64(p.offset);
            if version >= 6 {
                buf.put_i32(p.leader_epoch);
            }
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
///
/// Decode below v6 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] for
/// omitted `CommittedLeaderEpoch`.
pub fn decode_offset_commit_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, Vec<OffsetTopic>)> {
    let flexible = offset_commit_flexible(version)?;
    let group = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _gen = buf::get_i32(buf)?;
    let member = buf::get_string(buf, flexible)?.unwrap_or_default();
    if version >= 7 {
        let _inst = buf::get_string(buf, flexible)?;
    }
    if (2..=4).contains(&version) {
        let _retention = buf::get_i64(buf)?;
    }
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let offset = buf::get_i64(buf)?;
            let leader_epoch = if version >= 6 {
                buf::get_i32(buf)?
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
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

/// Encode OffsetCommit v2–v9. Throttle is `0` on v3+.
pub fn encode_offset_commit_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetTopic],
    error: i16,
) -> crate::error::Result<()> {
    let flexible = offset_commit_flexible(version)?;
    if version >= 3 {
        buf.put_i32(0);
    }
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
/// Throttle is v3+.
pub fn decode_offset_commit_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let flexible = offset_commit_flexible(version)?;
    if version >= 3 {
        let _throttle = buf::get_i32(buf)?;
    }
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
/// v1–v5 are classic. v6–v9 are compact strings/arrays plus tagged
/// fields (Apache JSON `flexibleVersions: "6+"`). Official JSON: v3,
/// v4, and v5 match v2 on the request (GroupId, Topics). v2 nullable
/// Topics and top-level ErrorCode. v3 ThrottleTimeMs. v5
/// CommittedLeaderEpoch. v7 RequireStable. v8 Groups (KIP-709). v9
/// MemberId / MemberEpoch (KIP-848). Kafka 4.0 `validVersions` is
/// `1-9` (v0 removed). This crate speaks 1–9. v0 and v10+ (topic IDs)
/// are not spoken.
fn offset_fetch_flexible(version: i16) -> Result<bool> {
    match version {
        1..=5 => Ok(false),
        6..=9 => Ok(true),
        other => Err(Error::protocol(format!(
            "OffsetFetch version {other} is not implemented"
        ))),
    }
}

fn encode_offset_fetch_topics(
    buf: &mut BytesMut,
    flexible: bool,
    topics: Option<&[OffsetFetchTopic]>,
) -> crate::error::Result<()> {
    match topics {
        None => buf::put_array_len(buf, flexible, None),
        Some(topics) => {
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
    }
}

fn decode_offset_fetch_topics<B: Buf>(
    buf: &mut B,
    flexible: bool,
) -> Result<Option<Vec<OffsetFetchTopic>>> {
    let Some(n) = buf::get_array_len(buf, flexible)? else {
        return Ok(None);
    };
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
    Ok(Some(topics))
}

fn encode_fetched_offset_topics(
    buf: &mut BytesMut,
    version: i16,
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
            if version >= 5 {
                buf.put_i32(p.leader_epoch);
            }
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
    version: i16,
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
            let leader_epoch = if version >= 5 {
                buf::get_i32(buf)?
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
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

/// Encode OffsetFetch v1–v9.
///
/// Kafka 4.0 JSON: `validVersions: "1-9"`, `flexibleVersions: "6+"`.
/// v1–v5 request is GroupId, Topics (v2–v5 match when Topics is
/// non-null). v2–v7 Topics is nullable (`None` = all committed
/// partitions). v7 RequireStable. v8 Groups (nullable Topics per
/// group; one or more). v9 MemberId / MemberEpoch.
/// This crate speaks 1–9. v0 and v10+ are not spoken. Null Topics
/// is v2+; v1 returns a protocol error. For several groups on v8+,
/// use [`encode_offset_fetch_groups_request`].
pub fn encode_offset_fetch_request(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: i32,
    require_stable: bool,
    topics: Option<&[OffsetFetchTopic]>,
) -> crate::error::Result<()> {
    let flexible = offset_fetch_flexible(version)?;
    if version < 2 && topics.is_none() {
        return Err(Error::protocol(format!(
            "OffsetFetch version {version} does not support null Topics"
        )));
    }
    if version <= 7 {
        buf::put_string(buf, flexible, Some(group_id))?;
        encode_offset_fetch_topics(buf, flexible, topics)?;
        if version >= 7 {
            buf.put_u8(u8::from(require_stable));
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
        return Ok(());
    }
    encode_offset_fetch_group_item(
        buf,
        version,
        true,
        group_id,
        member_id,
        member_epoch,
        topics,
    )?;
    buf.put_u8(u8::from(require_stable));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn encode_offset_fetch_group_item(
    buf: &mut BytesMut,
    version: i16,
    write_array_len: bool,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: i32,
    topics: Option<&[OffsetFetchTopic]>,
) -> crate::error::Result<()> {
    if write_array_len {
        buf::put_array_len(buf, true, Some(1))?;
    }
    buf::put_compact_string(buf, Some(group_id))?;
    if version >= 9 {
        buf::put_compact_string(buf, member_id)?;
        buf.put_i32(member_epoch);
    }
    encode_offset_fetch_topics(buf, true, topics)?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Encode OffsetFetch v8–v9 with a Groups array of N (KIP-709).
///
/// v1–v7 return a protocol error (`does not support Groups`). Null Topics
/// per group is allowed. Empty `groups` writes Groups length 0.
pub fn encode_offset_fetch_groups_request(
    buf: &mut BytesMut,
    version: i16,
    groups: &[OffsetFetchGroup],
    require_stable: bool,
) -> crate::error::Result<()> {
    let _flexible = offset_fetch_flexible(version)?;
    if version < 8 {
        return Err(Error::protocol(format!(
            "OffsetFetch version {version} does not support Groups"
        )));
    }
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        encode_offset_fetch_group_item(
            buf,
            version,
            false,
            &g.group_id,
            g.member_id.as_deref(),
            g.member_epoch,
            g.topics.as_deref(),
        )?;
    }
    buf.put_u8(u8::from(require_stable));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode OffsetFetch: `(group_id, topics, require_stable)`.
///
/// `topics` is `None` when the request used a null Topics array (v2+;
/// all committed partitions). v8+ with several groups returns the first
/// group. For every group, use [`decode_offset_fetch_groups_request`].
pub fn decode_offset_fetch_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, Option<Vec<OffsetFetchTopic>>, bool)> {
    let (groups, require_stable) = decode_offset_fetch_groups_request(buf, version)?;
    let first = groups
        .into_iter()
        .next()
        .unwrap_or_else(|| OffsetFetchGroup {
            group_id: String::new(),
            member_id: None,
            member_epoch: -1,
            topics: None,
        });
    Ok((first.group_id, first.topics, require_stable))
}

/// Decode OffsetFetch v1–v9: every group plus RequireStable.
///
/// v1–v7 yield one group (GroupId + Topics). v8+ yields the Groups array.
pub fn decode_offset_fetch_groups_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<OffsetFetchGroup>, bool)> {
    let flexible = offset_fetch_flexible(version)?;
    let groups = if version <= 7 {
        let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
        let topics = decode_offset_fetch_topics(buf, flexible)?;
        vec![OffsetFetchGroup {
            group_id,
            member_id: None,
            member_epoch: -1,
            topics,
        }]
    } else {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut groups = Vec::with_capacity(n);
        for _ in 0..n {
            let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
            let (member_id, member_epoch) = if version >= 9 {
                (buf::get_compact_string(buf)?, buf::get_i32(buf)?)
            } else {
                (None, -1)
            };
            let topics = decode_offset_fetch_topics(buf, true)?;
            buf::skip_tagged_fields(buf)?;
            groups.push(OffsetFetchGroup {
                group_id,
                member_id,
                member_epoch,
                topics,
            });
        }
        groups
    };
    let require_stable = if version >= 7 {
        buf::get_bool(buf)?
    } else {
        false
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((groups, require_stable))
}

/// Encode OffsetFetch v1–v9. Throttle is `0` on v3+. Top-level error is v2–v7.
pub fn encode_offset_fetch_response(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    topics: &[FetchedOffsetTopic],
    error: i16,
) -> crate::error::Result<()> {
    encode_offset_fetch_groups_response(
        buf,
        version,
        &[OffsetFetchGroupResult {
            group_id: group_id.to_string(),
            topics: topics.to_vec(),
            error_code: error,
        }],
    )
}

/// Encode OffsetFetch v1–v9 for every group (KIP-709 on v8+).
///
/// v1–v7 write the first group's Topics and ErrorCode. Empty `groups`
/// writes an empty Topics array (v1–v7) or Groups length 0 (v8+).
pub fn encode_offset_fetch_groups_response(
    buf: &mut BytesMut,
    version: i16,
    groups: &[OffsetFetchGroupResult],
) -> crate::error::Result<()> {
    let flexible = offset_fetch_flexible(version)?;
    if version >= 3 {
        buf.put_i32(0);
    }
    if version <= 7 {
        let first = groups.first();
        let topics = first.map(|g| g.topics.as_slice()).unwrap_or(&[]);
        let error = first.map(|g| g.error_code).unwrap_or(0);
        encode_fetched_offset_topics(buf, version, flexible, topics)?;
        if version >= 2 {
            buf.put_i16(error);
        }
    } else {
        buf::put_array_len(buf, true, Some(groups.len()))?;
        for g in groups {
            buf::put_compact_string(buf, Some(&g.group_id))?;
            encode_fetched_offset_topics(buf, version, true, &g.topics)?;
            buf.put_i16(g.error_code);
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode OffsetFetch. Top-level / group error is [`crate::error::Error::Broker`].
/// Throttle is v3+. Top-level error is v2–v7. Leader epoch is v5+.
/// v8+ with several groups returns the first group. For every group,
/// use [`decode_offset_fetch_groups_response`].
pub fn decode_offset_fetch_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<FetchedOffsetTopic>> {
    let groups = decode_offset_fetch_groups_response(buf, version)?;
    let first = groups.into_iter().next().unwrap_or(OffsetFetchGroupResult {
        group_id: String::new(),
        topics: Vec::new(),
        error_code: 0,
    });
    if first.error_code != 0 {
        return Err(crate::error::Error::broker(first.error_code, "OffsetFetch"));
    }
    Ok(first.topics)
}

/// Decode OffsetFetch v1–v9: every group's Topics and ErrorCode.
///
/// Does not fail on a non-zero group ErrorCode; callers decide. Throttle
/// is v3+. v1–v7 yield one group with an empty `group_id`.
pub fn decode_offset_fetch_groups_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<OffsetFetchGroupResult>> {
    let flexible = offset_fetch_flexible(version)?;
    if version >= 3 {
        let _throttle = buf::get_i32(buf)?;
    }
    let groups = if version <= 7 {
        let topics = decode_fetched_offset_topics(buf, version, flexible)?;
        let error_code = if version >= 2 { buf::get_i16(buf)? } else { 0 };
        vec![OffsetFetchGroupResult {
            group_id: String::new(),
            topics,
            error_code,
        }]
    } else {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut groups = Vec::with_capacity(n);
        for _ in 0..n {
            let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
            let topics = decode_fetched_offset_topics(buf, version, true)?;
            let error_code = buf::get_i16(buf)?;
            buf::skip_tagged_fields(buf)?;
            groups.push(OffsetFetchGroupResult {
                group_id,
                topics,
                error_code,
            });
        }
        groups
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(groups)
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

/// ConsumerProtocol subscription at [`ConsumerProtocol::HIGHEST_SUPPORTED_VERSION`]
/// (Java `serializeSubscription`; topics only, default generation, no rack).
pub fn encode_subscription(topics: &[String]) -> Result<Vec<u8>> {
    ConsumerProtocol::serialize_subscription(&ConsumerProtocolSubscription::new(topics.to_vec()))
}

/// ConsumerProtocol subscription with owned partitions (KIP-429).
///
/// Encodes at [`ConsumerProtocol::HIGHEST_SUPPORTED_VERSION`] with
/// [`ConsumerProtocolSubscription::DEFAULT_GENERATION`] and no rack.
pub fn encode_subscription_owned(topics: &[String], owned: &[(String, i32)]) -> Result<Vec<u8>> {
    ConsumerProtocol::serialize_subscription(&ConsumerProtocolSubscription {
        topics: topics.to_vec(),
        owned_partitions: owned.to_vec(),
        generation_id: ConsumerProtocolSubscription::DEFAULT_GENERATION,
        rack_id: None,
    })
}

/// Decode ConsumerProtocol subscription topics (v0–v3, owned partitions ignored).
pub fn decode_subscription(bytes: &[u8]) -> Result<Vec<String>> {
    Ok(decode_subscription_owned(bytes)?.0)
}

/// Topics plus owned `(topic, partition)` pairs from ConsumerProtocol subscription metadata.
///
/// v0 metadata yields an empty owned list. Generation and rack are
/// available on [`ConsumerProtocol::deserialize_subscription`].
#[expect(
    clippy::type_complexity,
    reason = "subscription is topics plus owned topic-partitions"
)]
pub fn decode_subscription_owned(bytes: &[u8]) -> Result<(Vec<String>, Vec<(String, i32)>)> {
    let sub = ConsumerProtocol::deserialize_subscription(bytes)?;
    Ok((sub.topics, sub.owned_partitions))
}

/// ConsumerProtocol assignment at [`ConsumerProtocol::HIGHEST_SUPPORTED_VERSION`]
/// (Java `serializeAssignment`; same fields as v0).
pub fn encode_assignment(topic: &str, partitions: &[i32]) -> Result<Vec<u8>> {
    encode_owned_assignment(&[(topic.to_string(), partitions.to_vec())])
}

/// ConsumerProtocol assignment for one member, several topics.
pub fn encode_owned_assignment(topics: &[(String, Vec<i32>)]) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_i16(ConsumerProtocol::HIGHEST_SUPPORTED_VERSION);
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
    fn coordinator_type_matches_java() {
        assert_eq!(CoordinatorType::Group.id(), COORDINATOR_GROUP);
        assert_eq!(CoordinatorType::Transaction.id(), COORDINATOR_TRANSACTION);
        assert_eq!(CoordinatorType::Share.id(), COORDINATOR_SHARE);
        assert_eq!(
            CoordinatorType::from_id(COORDINATOR_GROUP),
            Some(CoordinatorType::Group)
        );
        assert_eq!(
            CoordinatorType::from_id(COORDINATOR_TRANSACTION),
            Some(CoordinatorType::Transaction)
        );
        assert_eq!(
            CoordinatorType::from_id(COORDINATOR_SHARE),
            Some(CoordinatorType::Share)
        );
        assert_eq!(CoordinatorType::from_id(3), None);
        assert_eq!(CoordinatorType::Group.to_string(), "GROUP");
        assert_eq!(CoordinatorType::Transaction.to_string(), "TRANSACTION");
        assert_eq!(CoordinatorType::Share.to_string(), "SHARE");
        assert_eq!(MIN_BATCHED_VERSION, 4);
        assert!(find_coordinator_batched(MIN_BATCHED_VERSION));
        assert!(!find_coordinator_batched(MIN_BATCHED_VERSION - 1));
    }

    #[test]
    fn consumer_protocol_type_matches_java() {
        assert_eq!(ConsumerProtocol::PROTOCOL_TYPE, "consumer");
        assert_eq!(ConsumerProtocol::LOWEST_SUPPORTED_VERSION, 0);
        assert_eq!(ConsumerProtocol::HIGHEST_SUPPORTED_VERSION, 3);
        assert_eq!(ConsumerProtocolSubscription::DEFAULT_GENERATION, -1);
        assert_eq!(
            ConsumerProtocolSubscription::DEFAULT_GENERATION,
            JoinGroupRequest::UNKNOWN_GENERATION_ID
        );
    }

    #[test]
    fn offset_commit_defaults_match_java() {
        assert_eq!(DEFAULT_GENERATION_ID, -1);
        assert_eq!(DEFAULT_MEMBER_ID, "");
        assert_eq!(DEFAULT_RETENTION_TIME, -1);
    }

    #[test]
    fn join_group_request_matches_java() {
        assert_eq!(JoinGroupRequest::UNKNOWN_MEMBER_ID, "");
        assert_eq!(JoinGroupRequest::UNKNOWN_GENERATION_ID, -1);
        assert_eq!(JoinGroupRequest::UNKNOWN_PROTOCOL_NAME, "");
        assert!(!JoinGroupRequest::requires_known_member_id(3));
        assert!(JoinGroupRequest::requires_known_member_id(4));
        assert!(!JoinGroupRequest::requires_known_member_id_for(
            JoinGroupRequest::UNKNOWN_MEMBER_ID,
            None,
            3
        ));
        assert!(JoinGroupRequest::requires_known_member_id_for(
            JoinGroupRequest::UNKNOWN_MEMBER_ID,
            None,
            4
        ));
        assert!(
            !JoinGroupRequest::requires_known_member_id_for(
                JoinGroupRequest::UNKNOWN_MEMBER_ID,
                Some("worker-1"),
                4
            ),
            "static members skip MEMBER_ID_REQUIRED"
        );
        assert!(!JoinGroupRequest::requires_known_member_id_for(
            "m-1", None, 9
        ));
        assert!(!JoinGroupRequest::supports_skipping_assignment(8));
        assert!(JoinGroupRequest::supports_skipping_assignment(9));
        assert!(JoinGroupResponse::is_leader("m-1", "m-1"));
        assert!(!JoinGroupResponse::is_leader("m-1", "m-2"));
        let keep = "a".repeat(255);
        assert_eq!(JoinGroupRequest::maybe_truncate_reason(&keep), keep);
        let long = "a".repeat(256);
        assert_eq!(JoinGroupRequest::maybe_truncate_reason(&long).len(), 255);
    }

    #[test]
    fn fetched_offset_partition_data_matches_java() {
        assert_eq!(FetchedOffset::INVALID_OFFSET, -1);
        assert_eq!(FetchedOffset::NO_METADATA, "");
        assert_eq!(
            FetchedOffset::INVALID_OFFSET,
            crate::OffsetAndMetadata::INVALID_OFFSET
        );
        assert_eq!(
            FetchedOffset::NO_METADATA,
            crate::OffsetAndMetadata::NO_METADATA
        );
        let none = FetchedOffset::new(0, FetchedOffset::INVALID_OFFSET, 0);
        assert!(!none.has_error());
        assert_eq!(none.leader_epoch, RecordBatch::NO_PARTITION_LEADER_EPOCH);
        assert_eq!(
            none.to_string(),
            "PartitionData(offset=-1, leaderEpoch=-1, metadata=, error='NONE')"
        );
        let unknown = FetchedOffset::new(
            1,
            FetchedOffset::INVALID_OFFSET,
            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
        );
        assert!(unknown.has_error());
        assert_eq!(
            unknown.to_string(),
            "PartitionData(offset=-1, leaderEpoch=-1, metadata=, error='UNKNOWN_TOPIC_OR_PARTITION')"
        );
        let ok = FetchedOffset {
            partition: 0,
            offset: 5,
            leader_epoch: 2,
            metadata: "m".into(),
            error_code: 0,
        };
        assert_eq!(
            ok.to_string(),
            "PartitionData(offset=5, leaderEpoch=2, metadata=m, error='NONE')"
        );
    }

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
    fn find_coordinator_v4_coordinator_keys_array_of_two() {
        let mut buf = BytesMut::new();
        encode_find_coordinator_request_keys(&mut buf, 4, &["g", "h"], COORDINATOR_GROUP).unwrap();
        let mut cur = &buf[..];
        let (keys, key_type) = decode_find_coordinator_request_keys(&mut cur, 4).unwrap();
        assert_eq!(key_type, COORDINATOR_GROUP);
        assert_eq!(keys, vec!["g".to_string(), "h".to_string()]);
        assert!(
            cur.is_empty(),
            "FindCoordinator v4 CoordinatorKeys-of-2 leftover-empty"
        );
        // KeyType 0, compact CoordinatorKeys of 2 ("g", "h"), tagged.
        const REQ: &[u8] = &[0x00, 0x03, 0x02, 0x67, 0x02, 0x68, 0x00];
        assert_eq!(&buf[..], REQ);

        let mut via_keys = BytesMut::new();
        encode_find_coordinator_request_keys(&mut via_keys, 4, &["g"], COORDINATOR_GROUP).unwrap();
        let mut via_one = BytesMut::new();
        encode_find_coordinator_request_typed(&mut via_one, 4, "g", COORDINATOR_GROUP).unwrap();
        assert_eq!(
            via_keys.as_ref(),
            via_one.as_ref(),
            "CoordinatorKeys of 1 must match encode_find_coordinator_request_typed"
        );
        assert!(
            encode_find_coordinator_request_keys(
                &mut BytesMut::new(),
                3,
                &["g", "h"],
                COORDINATOR_GROUP
            )
            .unwrap_err()
            .to_string()
            .contains("does not support CoordinatorKeys"),
            "FindCoordinator v3 does not support CoordinatorKeys"
        );

        let coords = vec![
            CoordinatorResult {
                key: "g".into(),
                node_id: 1,
                host: "h".into(),
                port: 9092,
                error_code: 0,
                error_message: None,
            },
            CoordinatorResult {
                key: "h".into(),
                node_id: 1,
                host: "h".into(),
                port: 9092,
                error_code: 0,
                error_message: None,
            },
        ];
        buf.clear();
        encode_find_coordinator_response_coordinators(&mut buf, 4, &coords).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_find_coordinator_response_coordinators(&mut cur, 4).unwrap();
        assert_eq!(decoded, coords);
        assert!(
            cur.is_empty(),
            "FindCoordinator v4 Coordinators-of-2 leftover-empty"
        );
        let mut via_one = BytesMut::new();
        encode_find_coordinator_response(&mut via_one, 4, 1, "h", 9092, "g").unwrap();
        let one = coords.first().cloned().map(|c| vec![c]).unwrap_or_default();
        let mut via_coords = BytesMut::new();
        encode_find_coordinator_response_coordinators(&mut via_coords, 4, &one).unwrap();
        assert_eq!(
            via_one.as_ref(),
            via_coords.as_ref(),
            "Coordinators of 1 must match encode_find_coordinator_response"
        );
    }

    #[test]
    fn subscription_assignment_roundtrip() {
        let sub = encode_subscription(&["t".into()]).unwrap();
        assert_eq!(
            ConsumerProtocol::deserialize_version(&sub).unwrap(),
            ConsumerProtocol::HIGHEST_SUPPORTED_VERSION
        );
        assert_eq!(decode_subscription(&sub).unwrap(), vec!["t".to_string()]);
        let decoded = ConsumerProtocol::deserialize_subscription(&sub).unwrap();
        assert_eq!(decoded.topics, vec!["t".to_string()]);
        assert!(decoded.owned_partitions.is_empty());
        assert_eq!(
            decoded.generation_id,
            ConsumerProtocolSubscription::DEFAULT_GENERATION
        );
        assert_eq!(decoded.rack_id, None);
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
        assert_eq!(
            ConsumerProtocol::deserialize_version(&asg).unwrap(),
            ConsumerProtocol::HIGHEST_SUPPORTED_VERSION
        );
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
    fn consumer_protocol_subscription_v3_matches_java_serialize() {
        let sub = ConsumerProtocolSubscription {
            topics: vec!["u".into(), "t".into()],
            owned_partitions: vec![("u".into(), 1), ("t".into(), 2), ("t".into(), 0)],
            generation_id: 7,
            rack_id: Some("az1".into()),
        };
        let bytes = ConsumerProtocol::serialize_subscription(&sub).unwrap();
        assert_eq!(ConsumerProtocol::deserialize_version(&bytes).unwrap(), 3);
        let got = ConsumerProtocol::deserialize_subscription(&bytes).unwrap();
        assert_eq!(got.topics, vec!["t".to_string(), "u".to_string()]);
        assert_eq!(
            got.owned_partitions,
            vec![("t".into(), 0), ("t".into(), 2), ("u".into(), 1)]
        );
        assert_eq!(got.generation_id, 7);
        assert_eq!(got.rack_id.as_deref(), Some("az1"));

        let v1 = ConsumerProtocol::serialize_subscription_version(&sub, 1).unwrap();
        assert_eq!(ConsumerProtocol::deserialize_version(&v1).unwrap(), 1);
        let v1_got = ConsumerProtocol::deserialize_subscription(&v1).unwrap();
        assert_eq!(
            v1_got.generation_id,
            ConsumerProtocolSubscription::DEFAULT_GENERATION
        );
        assert_eq!(v1_got.rack_id, None);
        assert_eq!(
            v1_got.owned_partitions,
            vec![("t".into(), 0), ("t".into(), 2), ("u".into(), 1)]
        );

        let v0 = ConsumerProtocol::serialize_subscription_version(
            &ConsumerProtocolSubscription::new(vec!["t".into()]),
            0,
        )
        .unwrap();
        assert_eq!(ConsumerProtocol::deserialize_version(&v0).unwrap(), 0);
        let v0_got = ConsumerProtocol::deserialize_subscription(&v0).unwrap();
        assert!(v0_got.owned_partitions.is_empty());
        assert_eq!(
            v0_got.generation_id,
            ConsumerProtocolSubscription::DEFAULT_GENERATION
        );

        let empty_rack = ConsumerProtocolSubscription {
            topics: vec!["t".into()],
            owned_partitions: Vec::new(),
            generation_id: ConsumerProtocolSubscription::DEFAULT_GENERATION,
            rack_id: Some(String::new()),
        };
        let empty_bytes = ConsumerProtocol::serialize_subscription(&empty_rack).unwrap();
        let empty_got = ConsumerProtocol::deserialize_subscription(&empty_bytes).unwrap();
        assert_eq!(empty_got.rack_id, None);

        let capped = ConsumerProtocol::serialize_subscription_version(&sub, 9).unwrap();
        assert_eq!(
            ConsumerProtocol::deserialize_version(&capped).unwrap(),
            ConsumerProtocol::HIGHEST_SUPPORTED_VERSION
        );
        assert!(ConsumerProtocol::serialize_subscription_version(&sub, -1).is_err());
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
        let (gid, member, instance, meta, reason) = decode_join_group_request(&mut cur, 5).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(member, "m1");
        assert_eq!(instance.as_deref(), Some("worker-1"));
        assert_eq!(meta, vec![1, 2, 3]);
        assert_eq!(reason, None);
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
    fn join_group_v2_v4_match_and_omit_instance() {
        // Official JSON: "Version 2 and 3 are the same as version 1."
        // Kafka 4.0 removed v0–v1. Request: "g", session/rebalance 10000,
        // "m1", "consumer", one protocol "range" metadata [1,2,3].
        // Instance id is v5+.
        const REQ: &[u8] = &[
            0x00, 0x01, 0x67, 0x00, 0x00, 0x27, 0x10, 0x00, 0x00, 0x27, 0x10, 0x00, 0x02, 0x6d,
            0x31, 0x00, 0x08, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02,
            0x03,
        ];
        let req = JoinGroupRequest {
            group_id: "g",
            session_timeout_ms: 10_000,
            member_id: "m1",
            group_instance_id: Some("ignored-on-v2"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &[1, 2, 3],
            reason: None,
        };
        let mut v2 = BytesMut::new();
        encode_join_group_request(&mut v2, 2, &req).unwrap();
        let mut v3 = BytesMut::new();
        encode_join_group_request(&mut v3, 3, &req).unwrap();
        let mut v4 = BytesMut::new();
        encode_join_group_request(&mut v4, 4, &req).unwrap();
        assert_eq!(&v2[..], REQ);
        assert_eq!(v2.as_ref(), v3.as_ref(), "v2 and v3 request bodies match");
        assert_eq!(v3.as_ref(), v4.as_ref(), "v3 and v4 request bodies match");
        let mut cur = v2.as_ref();
        let (gid, member, instance, meta, reason) = decode_join_group_request(&mut cur, 2).unwrap();
        assert_eq!((gid.as_str(), member.as_str()), ("g", "m1"));
        assert_eq!(instance, None);
        assert_eq!(meta, vec![1, 2, 3]);
        assert_eq!(reason, None);
        assert!(cur.is_empty(), "v2 request leftover-empty");
        let mut cur = v4.as_ref();
        let (_gid, _member, instance, _meta, _reason) =
            decode_join_group_request(&mut cur, 4).unwrap();
        assert_eq!(instance, None);
        assert!(cur.is_empty(), "v4 request leftover-empty");

        let mut v5 = BytesMut::new();
        encode_join_group_request(&mut v5, 5, &req).unwrap();
        assert_ne!(v2.as_ref(), v5.as_ref(), "v5 request adds GroupInstanceId");

        let members = [JoinMember {
            member_id: "m1".into(),
            metadata: vec![1, 2, 3],
        }];
        v2.clear();
        encode_join_group_response(&mut v2, 2, 0, 7, "range", "l", "m1", &members).unwrap();
        v3.clear();
        encode_join_group_response(&mut v3, 3, 0, 7, "range", "l", "m1", &members).unwrap();
        v4.clear();
        encode_join_group_response(&mut v4, 4, 0, 7, "range", "l", "m1", &members).unwrap();
        v5.clear();
        encode_join_group_response(&mut v5, 5, 0, 7, "range", "l", "m1", &members).unwrap();
        assert_eq!(v2.as_ref(), v3.as_ref(), "v2 and v3 response bodies match");
        assert_eq!(v3.as_ref(), v4.as_ref(), "v3 and v4 response bodies match");
        assert_ne!(
            v2.as_ref(),
            v5.as_ref(),
            "v5 response members add GroupInstanceId"
        );
        let mut cur = v2.as_ref();
        let (err, gen, proto, leader, mid, skip, got) =
            decode_join_group_response(&mut cur, 2).unwrap();
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
        assert_eq!(got[0].member_id, "m1");
        assert_eq!(got[0].metadata, vec![1, 2, 3]);
        assert!(cur.is_empty(), "v2 response leftover-empty");

        v2.clear();
        let err = encode_join_group_request(&mut v2, 0, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        v2.clear();
        let err = encode_join_group_request(&mut v2, 1, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v1 is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 2, 9), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 9, 2, 9), Some(9));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 2, 9), None);
        assert_eq!(crate::protocol::api_keys::pick_version(10, 10, 2, 9), None);
    }

    #[test]
    fn join_group_v6_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_join_group_request(&mut req, 6, &join_req(&[1, 2, 3])).unwrap();
        let mut cur = &req[..];
        let (gid, member, instance, meta, reason) = decode_join_group_request(&mut cur, 6).unwrap();
        assert_eq!((gid.as_str(), member.as_str()), ("g", "m1"));
        assert_eq!(instance, None);
        assert_eq!(meta, vec![1, 2, 3]);
        assert_eq!(reason, None);
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
        let (gid, member, _, meta, reason) = decode_join_group_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), member.as_str()), ("g", "m1"));
        assert_eq!(meta, vec![1, 2, 3]);
        assert_eq!(reason.as_deref(), Some("rejoin"));
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
            encode_join_group_request(&mut BytesMut::new(), 1, &join_req(&[1, 2, 3])).is_err(),
            "JoinGroup v0–v1 are not spoken"
        );
        assert!(
            encode_join_group_request(&mut BytesMut::new(), 10, &join_req(&[1, 2, 3])).is_err(),
            "JoinGroup v10+ is not spoken"
        );
    }

    #[test]
    fn join_group_protocols_of_n_v6_compact() {
        // Same header as v6 one-protocol, then Protocols of 2: "range" and
        // "sticky", each metadata [1,2,3], empty tags on each protocol and
        // the top-level.
        const REQ: &[u8] = &[
            0x02, 0x67, 0x00, 0x00, 0x27, 0x10, 0x00, 0x00, 0x27, 0x10, 0x03, 0x6d, 0x31, 0x00,
            0x09, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x03, 0x06, 0x72, 0x61, 0x6e,
            0x67, 0x65, 0x04, 0x01, 0x02, 0x03, 0x00, 0x07, 0x73, 0x74, 0x69, 0x63, 0x6b, 0x79,
            0x04, 0x01, 0x02, 0x03, 0x00, 0x00,
        ];
        let meta = [1u8, 2, 3];
        let protocols = [
            JoinGroupProtocol::new("range", &meta),
            JoinGroupProtocol::new("sticky", &meta),
        ];
        let req = JoinGroupProtocolsRequest {
            group_id: "g",
            session_timeout_ms: 10_000,
            member_id: "m1",
            group_instance_id: None,
            protocol_type: "consumer",
            protocols: &protocols,
            reason: None,
        };
        let mut buf = BytesMut::new();
        encode_join_group_protocols_request(&mut buf, 6, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let (gid, member, instance, got, reason) =
            decode_join_group_request_protocols(&mut cur, 6).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(member, "m1");
        assert_eq!(instance, None);
        assert_eq!(reason, None);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].name, "range");
        assert_eq!(got[0].metadata, meta);
        assert_eq!(got[1].name, "sticky");
        assert_eq!(got[1].metadata, meta);
        assert!(
            cur.is_empty(),
            "JoinGroup v6 Protocols of 2 must be leftover-empty"
        );
        buf.clear();
        encode_join_group_protocols_request(
            &mut buf,
            6,
            &JoinGroupProtocolsRequest {
                group_id: "g",
                session_timeout_ms: 10_000,
                member_id: "m1",
                group_instance_id: None,
                protocol_type: "consumer",
                protocols: &[JoinGroupProtocol::new("range", &meta)],
                reason: None,
            },
        )
        .unwrap();
        let mut one = BytesMut::new();
        encode_join_group_request(&mut one, 6, &join_req(&meta)).unwrap();
        assert_eq!(
            &buf[..],
            &one[..],
            "Protocols of 1 must match encode_join_group_request"
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

    #[test]
    fn join_group_v7_empty_protocol_name_is_null() {
        // Java JoinGroupResponse: v7+ empty ProtocolName is null (compact 0x00);
        // v6 empty is compact empty string (0x01). Leader / member stay "".
        const V7: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x01, 0x01,
            0x01, 0x00,
        ];
        const V6: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0xff, 0xff, 0xff, 0xff, 0x01, 0x01, 0x01, 0x01,
            0x00,
        ];
        let mut v7 = BytesMut::new();
        encode_join_group_response(
            &mut v7,
            7,
            16,
            JoinGroupRequest::UNKNOWN_GENERATION_ID,
            JoinGroupRequest::UNKNOWN_PROTOCOL_NAME,
            JoinGroupRequest::UNKNOWN_MEMBER_ID,
            JoinGroupRequest::UNKNOWN_MEMBER_ID,
            &[],
        )
        .unwrap();
        assert_eq!(&v7[..], V7);
        let mut v6 = BytesMut::new();
        encode_join_group_response(
            &mut v6,
            6,
            16,
            JoinGroupRequest::UNKNOWN_GENERATION_ID,
            JoinGroupRequest::UNKNOWN_PROTOCOL_NAME,
            JoinGroupRequest::UNKNOWN_MEMBER_ID,
            JoinGroupRequest::UNKNOWN_MEMBER_ID,
            &[],
        )
        .unwrap();
        assert_eq!(&v6[..], V6);
        let mut cur = &v7[..];
        let (err, gen, protocol, leader, member, skip, members) =
            decode_join_group_response(&mut cur, 7).unwrap();
        assert_eq!(err, 16);
        assert_eq!(gen, JoinGroupRequest::UNKNOWN_GENERATION_ID);
        assert_eq!(protocol, JoinGroupRequest::UNKNOWN_PROTOCOL_NAME);
        assert_eq!(leader, JoinGroupRequest::UNKNOWN_MEMBER_ID);
        assert_eq!(member, JoinGroupRequest::UNKNOWN_MEMBER_ID);
        assert!(!skip);
        assert!(members.is_empty());
        assert!(
            cur.is_empty(),
            "v7 null ProtocolName leftover {} bytes",
            cur.len()
        );
        let mut cur = &v6[..];
        let (err, _, protocol, _, _, _, _) = decode_join_group_response(&mut cur, 6).unwrap();
        assert_eq!(err, 16);
        assert_eq!(protocol, JoinGroupRequest::UNKNOWN_PROTOCOL_NAME);
        assert!(
            cur.is_empty(),
            "v6 empty ProtocolName leftover {} bytes",
            cur.len()
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
    fn offset_commit_v2_v4_match_and_v5_drops_retention() {
        // Official JSON: "Version 3 and 4 are the same as version 2."
        // Request: "g", gen 7, "m1", RetentionTimeMs -1, topic "t" p0
        // offset 3, null metadata. Leader epoch is v6+. Instance is v7+.
        const REQ: &[u8] = &[
            0x00, 0x01, 0x67, 0x00, 0x00, 0x00, 0x07, 0x00, 0x02, 0x6d, 0x31, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
            0xff, 0xff,
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
        let mut v2 = BytesMut::new();
        encode_offset_commit_request(&mut v2, 2, "g", 7, "m1", Some("ignored"), &topics).unwrap();
        let mut v3 = BytesMut::new();
        encode_offset_commit_request(&mut v3, 3, "g", 7, "m1", Some("ignored"), &topics).unwrap();
        let mut v4 = BytesMut::new();
        encode_offset_commit_request(&mut v4, 4, "g", 7, "m1", Some("ignored"), &topics).unwrap();
        assert_eq!(&v2[..], REQ);
        assert_eq!(v2.as_ref(), v3.as_ref(), "v2 and v3 request bodies match");
        assert_eq!(v3.as_ref(), v4.as_ref(), "v3 and v4 request bodies match");
        let mut cur = v2.as_ref();
        let (gid, mid, got) = decode_offset_commit_request(&mut cur, 2).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got[0].partitions[0].offset, 3);
        assert_eq!(
            got[0].partitions[0].leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(cur.is_empty(), "v2 request leftover-empty");

        let mut v5 = BytesMut::new();
        encode_offset_commit_request(&mut v5, 5, "g", 7, "m1", Some("ignored"), &topics).unwrap();
        assert_ne!(v4.as_ref(), v5.as_ref(), "v5 drops RetentionTimeMs");
        let mut cur = v5.as_ref();
        let (_gid, _mid, got) = decode_offset_commit_request(&mut cur, 5).unwrap();
        assert_eq!(
            got[0].partitions[0].leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(cur.is_empty(), "v5 request leftover-empty");

        let mut v6 = BytesMut::new();
        encode_offset_commit_request(&mut v6, 6, "g", 7, "m1", Some("ignored"), &topics).unwrap();
        assert_ne!(v5.as_ref(), v6.as_ref(), "v6 adds CommittedLeaderEpoch");
        let mut cur = v6.as_ref();
        let (_gid, _mid, got) = decode_offset_commit_request(&mut cur, 6).unwrap();
        assert_eq!(got[0].partitions[0].leader_epoch, 4);
        assert!(cur.is_empty(), "v6 request leftover-empty");

        v2.clear();
        encode_offset_commit_response(&mut v2, 2, &topics, 0).unwrap();
        v3.clear();
        encode_offset_commit_response(&mut v3, 3, &topics, 0).unwrap();
        assert_ne!(v2.as_ref(), v3.as_ref(), "v3 response adds ThrottleTimeMs");
        let mut cur = v2.as_ref();
        assert_eq!(decode_offset_commit_response(&mut cur, 2).unwrap(), 0);
        assert!(cur.is_empty(), "v2 response leftover-empty");
        let mut cur = v3.as_ref();
        assert_eq!(decode_offset_commit_response(&mut cur, 3).unwrap(), 0);
        assert!(cur.is_empty(), "v3 response leftover-empty");

        v2.clear();
        let err =
            encode_offset_commit_request(&mut v2, 0, "g", 7, "m1", None, &topics).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 2, 9), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 9, 2, 9), Some(9));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 2, 9), None);
        assert_eq!(crate::protocol::api_keys::pick_version(10, 10, 2, 9), None);
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
            encode_offset_commit_request(&mut BytesMut::new(), 1, "g", 7, "m1", None, &topics)
                .is_err(),
            "OffsetCommit v0–v1 are not spoken"
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
        encode_offset_fetch_request(&mut buf, 5, "g", None, -1, false, Some(&req)).unwrap();
        let mut cur = &buf[..];
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 5).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(got, Some(req));
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
    fn offset_fetch_v1_v5_match_and_v5_adds_epoch() {
        // Official JSON: "Version 3, 4, and 5 are the same as version 2."
        // Request: STRING "g", one topic "t" partition 0. Topics is non-null
        // so v1 matches v2–v5.
        const REQ: &[u8] = &[
            0x00, 0x01, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00,
        ];
        let req = offset_fetch_one_topic();
        let mut v1 = BytesMut::new();
        encode_offset_fetch_request(&mut v1, 1, "g", None, -1, false, Some(&req)).unwrap();
        let mut v2 = BytesMut::new();
        encode_offset_fetch_request(&mut v2, 2, "g", None, -1, false, Some(&req)).unwrap();
        let mut v3 = BytesMut::new();
        encode_offset_fetch_request(&mut v3, 3, "g", None, -1, false, Some(&req)).unwrap();
        let mut v4 = BytesMut::new();
        encode_offset_fetch_request(&mut v4, 4, "g", None, -1, false, Some(&req)).unwrap();
        let mut v5 = BytesMut::new();
        encode_offset_fetch_request(&mut v5, 5, "g", None, -1, false, Some(&req)).unwrap();
        assert_eq!(&v1[..], REQ);
        assert_eq!(v1.as_ref(), v2.as_ref(), "v1 and v2 request bodies match");
        assert_eq!(v2.as_ref(), v3.as_ref(), "v2 and v3 request bodies match");
        assert_eq!(v3.as_ref(), v4.as_ref(), "v3 and v4 request bodies match");
        assert_eq!(v4.as_ref(), v5.as_ref(), "v4 and v5 request bodies match");
        let mut cur = v1.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 1).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got.as_deref(), Some(req.as_slice()));
        assert!(cur.is_empty(), "v1 request leftover-empty");

        let resp = vec![FetchedOffsetTopic {
            topic: "t".into(),
            partitions: vec![FetchedOffset {
                partition: 0,
                offset: 4,
                leader_epoch: 2,
                metadata: String::new(),
                error_code: 0,
            }],
        }];
        v1.clear();
        encode_offset_fetch_response(&mut v1, 1, "g", &resp, 0).unwrap();
        v2.clear();
        encode_offset_fetch_response(&mut v2, 2, "g", &resp, 0).unwrap();
        assert_ne!(
            v1.as_ref(),
            v2.as_ref(),
            "v2 response adds top-level ErrorCode"
        );
        let mut cur = v1.as_ref();
        let decoded = decode_offset_fetch_response(&mut cur, 1).unwrap();
        assert_eq!(decoded[0].partitions[0].leader_epoch, -1);
        assert!(cur.is_empty(), "v1 response leftover-empty");
        let mut cur = v2.as_ref();
        let decoded = decode_offset_fetch_response(&mut cur, 2).unwrap();
        assert_eq!(decoded[0].partitions[0].leader_epoch, -1);
        assert!(cur.is_empty(), "v2 response leftover-empty");

        v3.clear();
        encode_offset_fetch_response(&mut v3, 3, "g", &resp, 0).unwrap();
        assert_ne!(v2.as_ref(), v3.as_ref(), "v3 response adds ThrottleTimeMs");
        v4.clear();
        encode_offset_fetch_response(&mut v4, 4, "g", &resp, 0).unwrap();
        assert_eq!(v3.as_ref(), v4.as_ref(), "v3 and v4 response bodies match");
        let mut cur = v4.as_ref();
        let decoded = decode_offset_fetch_response(&mut cur, 4).unwrap();
        assert_eq!(decoded[0].partitions[0].leader_epoch, -1);
        assert!(cur.is_empty(), "v4 response leftover-empty");

        v5.clear();
        encode_offset_fetch_response(&mut v5, 5, "g", &resp, 0).unwrap();
        assert_ne!(
            v4.as_ref(),
            v5.as_ref(),
            "v5 response adds CommittedLeaderEpoch"
        );
        let mut cur = v5.as_ref();
        let decoded = decode_offset_fetch_response(&mut cur, 5).unwrap();
        assert_eq!(decoded[0].partitions[0].leader_epoch, 2);
        assert!(cur.is_empty(), "v5 response leftover-empty");

        v1.clear();
        let err =
            encode_offset_fetch_request(&mut v1, 0, "g", None, -1, false, Some(&req)).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 9), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 9, 1, 9), Some(9));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 1, 9), None);
        assert_eq!(crate::protocol::api_keys::pick_version(10, 10, 1, 9), None);
    }

    #[test]
    fn offset_fetch_v6_roundtrip_is_leftover_empty() {
        let req = offset_fetch_one_topic();
        let mut buf = BytesMut::new();
        encode_offset_fetch_request(&mut buf, 6, "g", None, -1, false, Some(&req)).unwrap();
        let mut cur = &buf[..];
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 6).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, Some(req));
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
        encode_offset_fetch_request(&mut buf, 6, "g", None, -1, false, Some(&req)).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v5 = BytesMut::new();
        encode_offset_fetch_request(&mut v5, 5, "g", None, -1, false, Some(&req)).unwrap();
        assert_ne!(&buf[..], &v5[..], "OffsetFetch v6 must not be classic v5");
        assert!(
            encode_offset_fetch_request(&mut BytesMut::new(), 0, "g", None, -1, false, Some(&req))
                .is_err(),
            "OffsetFetch v0 is not spoken"
        );
        assert!(
            encode_offset_fetch_request(&mut BytesMut::new(), 10, "g", None, -1, false, Some(&req))
                .is_err(),
            "OffsetFetch v10+ is not spoken"
        );
    }

    #[test]
    fn offset_fetch_v7_sends_require_stable() {
        let req = offset_fetch_one_topic();
        let mut off = BytesMut::new();
        encode_offset_fetch_request(&mut off, 7, "g", None, -1, false, Some(&req)).unwrap();
        let mut on = BytesMut::new();
        encode_offset_fetch_request(&mut on, 7, "g", None, -1, true, Some(&req)).unwrap();
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
        encode_offset_fetch_request(&mut buf, 8, "g", None, -1, false, Some(&req)).unwrap();
        let mut cur = &buf[..];
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, Some(req));
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
        encode_offset_fetch_request(&mut v8, 8, "g", Some("m1"), 3, false, Some(&req)).unwrap();
        let mut v9 = BytesMut::new();
        encode_offset_fetch_request(&mut v9, 9, "g", Some("m1"), 3, false, Some(&req)).unwrap();
        assert_ne!(&v8[..], &v9[..], "v9 must write MemberId / MemberEpoch");
        let mut cur = &v9[..];
        let (gid, got, _stable) = decode_offset_fetch_request(&mut cur, 9).unwrap();
        assert_eq!(gid, "g");
        assert_eq!(got, Some(req));
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
    fn offset_fetch_null_topics_is_all_partitions() {
        let mut v1 = BytesMut::new();
        let err = encode_offset_fetch_request(&mut v1, 1, "g", None, -1, false, None).unwrap_err();
        assert!(
            err.to_string().contains("null Topics"),
            "v1 Topics is not nullable, got {err}"
        );

        let mut v2 = BytesMut::new();
        encode_offset_fetch_request(&mut v2, 2, "g", None, -1, false, None).unwrap();
        let mut cur = v2.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 2).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, None);
        assert!(cur.is_empty(), "v2 null Topics leftover-empty");
        const V2: &[u8] = &[0x00, 0x01, 0x67, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(&v2[..], V2);

        let mut v8 = BytesMut::new();
        encode_offset_fetch_request(&mut v8, 8, "g", None, -1, false, None).unwrap();
        let mut cur = v8.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, None);
        assert!(cur.is_empty(), "v8 null Topics leftover-empty");
        const V8: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(&v8[..], V8);

        let mut v9 = BytesMut::new();
        encode_offset_fetch_request(&mut v9, 9, "g", None, -1, false, None).unwrap();
        let mut cur = v9.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 9).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, None);
        assert!(cur.is_empty(), "v9 null Topics leftover-empty");
    }

    #[test]
    fn offset_fetch_v8_groups_array_of_two() {
        let req = offset_fetch_one_topic();
        let groups = vec![
            OffsetFetchGroup::new("a", Some(req.clone())),
            OffsetFetchGroup::new("b", Some(req)),
        ];
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_request(&mut buf, 8, &groups, false).unwrap();
        let mut cur = buf.as_ref();
        let (got, stable) = decode_offset_fetch_groups_request(&mut cur, 8).unwrap();
        assert!(!stable);
        assert_eq!(got, groups);
        assert!(
            cur.is_empty(),
            "v8 Groups-of-2 request leftover-empty; leftover {} bytes",
            cur.len()
        );
        // Compact Groups of 2 ("a", "b"), each one topic "t" p0, RequireStable 0.
        const REQ: &[u8] = &[
            0x03, 0x02, 0x61, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
            0x62, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(&buf[..], REQ);

        let one = vec![OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()))];
        let mut single = BytesMut::new();
        encode_offset_fetch_groups_request(&mut single, 8, &one, false).unwrap();
        let mut via_one = BytesMut::new();
        encode_offset_fetch_request(
            &mut via_one,
            8,
            "g",
            None,
            -1,
            false,
            Some(&offset_fetch_one_topic()),
        )
        .unwrap();
        assert_eq!(
            single.as_ref(),
            via_one.as_ref(),
            "Groups of 1 must match encode_offset_fetch_request"
        );

        let resp = vec![
            OffsetFetchGroupResult {
                group_id: "a".into(),
                topics: vec![FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![FetchedOffset::new(0, 1, 0)],
                }],
                error_code: 0,
            },
            OffsetFetchGroupResult {
                group_id: "b".into(),
                topics: vec![FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![FetchedOffset::new(0, 2, 0)],
                }],
                error_code: 0,
            },
        ];
        buf.clear();
        encode_offset_fetch_groups_response(&mut buf, 8, &resp).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_offset_fetch_groups_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            cur.is_empty(),
            "v8 Groups-of-2 response leftover-empty; leftover {} bytes",
            cur.len()
        );

        let err = encode_offset_fetch_groups_request(&mut BytesMut::new(), 7, &groups, false)
            .unwrap_err();
        assert!(err.to_string().contains("does not support Groups"));
    }

    #[test]
    fn offset_fetch_v8_groups_null_topics_array_of_two() {
        let groups = vec![
            OffsetFetchGroup::new("a", None),
            OffsetFetchGroup::new("b", None),
        ];
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_request(&mut buf, 8, &groups, true).unwrap();
        let mut cur = buf.as_ref();
        let (got, stable) = decode_offset_fetch_groups_request(&mut cur, 8).unwrap();
        assert!(stable);
        assert_eq!(got, groups);
        assert!(cur.is_empty(), "v8 null-Topics Groups leftover-empty");
        const REQ: &[u8] = &[
            0x03, 0x02, 0x61, 0x00, 0x00, 0x02, 0x62, 0x00, 0x00, 0x01, 0x00,
        ];
        assert_eq!(&buf[..], REQ);
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

    #[test]
    fn leave_group_builder_matches_java() {
        let empty =
            encode_leave_group_request_members(&mut BytesMut::new(), 5, "g", &[]).unwrap_err();
        assert!(
            matches!(empty, Error::Protocol(_)),
            "empty members is Java IllegalArgumentException, got {empty}"
        );
        assert!(
            empty
                .to_string()
                .contains("leaving members should not be empty"),
            "got {empty}"
        );
        let two = [
            LeaveGroupMember {
                member_id: "m1".into(),
                group_instance_id: None,
                reason: None,
            },
            LeaveGroupMember {
                member_id: "m2".into(),
                group_instance_id: None,
                reason: None,
            },
        ];
        let batched =
            encode_leave_group_request_members(&mut BytesMut::new(), 0, "g", &two).unwrap_err();
        assert!(
            matches!(batched, Error::Unsupported(_)),
            "v0 with two members is Java UnsupportedVersionException, got {batched}"
        );
        assert!(
            batched.to_string().contains("only supports single member"),
            "got {batched}"
        );
        encode_leave_group_request_members(&mut BytesMut::new(), 3, "g", &two).unwrap();
    }
}
