//! Consumer group codecs: FindCoordinator, Join/Sync/Heartbeat/Leave,
//! OffsetCommit/OffsetFetch, OffsetDelete, and ConsumerProtocol assignment.

use std::collections::{HashMap, HashSet};
use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use super::records::RecordBatch;
use crate::error::{for_code, Error, Result};

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

/// Java `FindCoordinatorResponse` helpers.
pub struct FindCoordinatorResponse;

impl FindCoordinatorResponse {
    /// Java `FindCoordinatorResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 2
    }

    /// Java `FindCoordinatorResponse.errorCounts`.
    ///
    /// When `coordinators` is non-empty, counts each coordinator
    /// `errorCode` (including `NONE`) and does not add a separate
    /// top-level code. That matches v4+ `Coordinators[]` and this
    /// crate's v0–v3 one-entry vec (top-level fields folded into that
    /// entry). When empty, Java falls back to the top-level
    /// `errorCode` (v4+ JSON default is `NONE`).
    #[must_use]
    pub fn error_counts(coordinators: &[CoordinatorResult]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        if coordinators.is_empty() {
            let count = counts.entry(0).or_insert(0);
            *count += 1;
            return counts;
        }
        for coordinator in coordinators {
            let count = counts.entry(coordinator.error_code).or_insert(0);
            *count += 1;
        }
        counts
    }

    /// Java `FindCoordinatorResponse.prepareErrorResponse`.
    ///
    /// One [`CoordinatorResult`] per key: copies `Key`, sets ErrorCode,
    /// and fills `Node.noNode` (`id` `-1`, empty host, port `-1`).
    /// `ErrorMessage` is the JSON default (null); official Java also
    /// sets the English `Errors.message` string. Throttle is the JSON
    /// default (`0`).
    #[must_use]
    pub fn prepare_error_response<I>(keys: I, error_code: i16) -> Vec<CoordinatorResult>
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        keys.into_iter()
            .map(|key| CoordinatorResult::error_for_key(error_code, key))
            .collect()
    }

    /// Java `FindCoordinatorRequest.getErrorResponse`.
    ///
    /// Below [`MIN_BATCHED_VERSION`] this is `prepareOldResponse` (one
    /// top-level coordinator; request keys are not copied). v4+ is
    /// [`Self::prepare_error_response`].
    #[must_use]
    pub fn error_results<I>(version: i16, keys: I, error_code: i16) -> Vec<CoordinatorResult>
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        if version < MIN_BATCHED_VERSION {
            drop(keys);
            vec![CoordinatorResult::error(error_code)]
        } else {
            Self::prepare_error_response(keys, error_code)
        }
    }

    /// Java `FindCoordinatorResponse.coordinatorByKey`.
    ///
    /// v4+: the first coordinator whose `Key` equals `key`, or `None`
    /// when none match. An empty `Coordinators[]` follows Java's v0–v3
    /// path: a synthesized coordinator from JSON defaults (`errorCode`
    /// `NONE`, `nodeId` / `port` `0`, empty host, null `errorMessage`)
    /// with `Key` set to `key`. v0–v3 crate decode folds the top-level
    /// coordinator into a one-entry vec with empty `Key`; this helper
    /// copies that entry and sets `Key` to `key` (Java stuffs the lookup
    /// key because the wire has none).
    #[must_use]
    pub fn coordinator_by_key(
        version: i16,
        coordinators: &[CoordinatorResult],
        key: &str,
    ) -> Option<CoordinatorResult> {
        if version < MIN_BATCHED_VERSION {
            return Some(match coordinators.first() {
                Some(first) => CoordinatorResult {
                    key: key.into(),
                    node_id: first.node_id,
                    host: first.host.clone(),
                    port: first.port,
                    error_code: first.error_code,
                    error_message: first.error_message.clone(),
                },
                None => find_coordinator_top_level_for_key(key),
            });
        }
        if let Some(found) = coordinators.iter().find(|c| c.key == key) {
            return Some(found.clone());
        }
        if coordinators.is_empty() {
            return Some(find_coordinator_top_level_for_key(key));
        }
        None
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

    /// Java `ConsumerProtocol.serializeAssignment` at
    /// [`Self::HIGHEST_SUPPORTED_VERSION`].
    pub fn serialize_assignment(assignment: &ConsumerProtocolAssignment) -> Result<Vec<u8>> {
        Self::serialize_assignment_version(assignment, Self::HIGHEST_SUPPORTED_VERSION)
    }

    /// Java `ConsumerProtocol.serializeAssignment` at `version`.
    ///
    /// Versions above [`Self::HIGHEST_SUPPORTED_VERSION`] encode as that
    /// cap (Java `checkAssignmentVersion`). Assigned partitions are grouped
    /// by first-seen topic and are **not** sorted (unlike
    /// [`Self::serialize_subscription`]). User data is null.
    pub fn serialize_assignment_version(
        assignment: &ConsumerProtocolAssignment,
        version: i16,
    ) -> Result<Vec<u8>> {
        let version = check_assignment_version(version)?;
        let by_topic = group_assigned_partitions(&assignment.partitions);
        let mut buf = BytesMut::new();
        buf.put_i16(version);
        buf::put_array_len(&mut buf, false, Some(by_topic.len()))?;
        for (topic, parts) in &by_topic {
            buf::put_classic_nullable_string(&mut buf, Some(topic))?;
            buf::put_array_len(&mut buf, false, Some(parts.len()))?;
            for p in parts {
                buf.put_i32(*p);
            }
        }
        buf::put_classic_bytes(&mut buf, None)?;
        Ok(buf.to_vec())
    }

    /// Java `ConsumerProtocol.deserializeAssignment`.
    ///
    /// Empty bytes are an empty assignment. Versions above
    /// [`Self::HIGHEST_SUPPORTED_VERSION`] parse with that schema. User
    /// data is discarded (this crate does not expose it).
    pub fn deserialize_assignment(bytes: &[u8]) -> Result<ConsumerProtocolAssignment> {
        if bytes.is_empty() {
            return Ok(ConsumerProtocolAssignment::default());
        }
        let mut bytes = bytes;
        let raw = buf::get_i16(&mut bytes)?;
        let _ver = check_assignment_version(raw)?;
        let n = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
        let mut partitions = Vec::new();
        for _ in 0..n {
            let topic = buf::get_classic_nullable_string(&mut bytes)?.unwrap_or_default();
            let pn = buf::get_array_len(&mut bytes, false)?.unwrap_or(0);
            for _ in 0..pn {
                partitions.push((topic.clone(), buf::get_i32(&mut bytes)?));
            }
        }
        if !bytes.is_empty() {
            let _user = buf::get_classic_bytes(&mut bytes)?;
        }
        Ok(ConsumerProtocolAssignment { partitions })
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
/// `ConsumerProtocol.serializeSubscription`. [`Display`] is Java
/// `Subscription.toString` (`groupInstanceId` is always `null`; this type
/// does not store it. User data is omitted when null).
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

    /// Java `Subscription.topics`.
    #[must_use]
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Java `Subscription.ownedPartitions`.
    #[must_use]
    pub fn owned_partitions(&self) -> &[(String, i32)] {
        &self.owned_partitions
    }

    /// Java `Subscription.generationId` (`None` when the stored value is
    /// negative).
    #[must_use]
    pub fn generation_id(&self) -> Option<i32> {
        (self.generation_id >= 0).then_some(self.generation_id)
    }

    /// Java `Subscription.rackId`.
    #[must_use]
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }
}

impl Default for ConsumerProtocolSubscription {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl fmt::Display for ConsumerProtocolSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Subscription(topics=[")?;
        for (i, topic) in self.topics.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(topic)?;
        }
        f.write_str("], ownedPartitions=[")?;
        for (i, (topic, partition)) in self.owned_partitions.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{topic}-{partition}")?;
        }
        f.write_str("], groupInstanceId=null, generationId=")?;
        write!(f, "{}", self.generation_id)?;
        f.write_str(", rackId=")?;
        match self.rack_id.as_deref() {
            Some(rack) => f.write_str(rack)?,
            None => f.write_str("null")?,
        }
        f.write_str(")")
    }
}

/// Java `ConsumerPartitionAssignor.Assignment` /
/// `ConsumerProtocolAssignment` (classic SyncGroup member assignment).
///
/// [`ConsumerProtocol::serialize_assignment`] is Java
/// `ConsumerProtocol.serializeAssignment`. [`Display`] is Java
/// `Assignment.toString` (comma-space `topic-partition` list; user data
/// is omitted when null).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerProtocolAssignment {
    /// Assigned topic-partitions (Java `Assignment.partitions`).
    pub partitions: Vec<(String, i32)>,
}

impl ConsumerProtocolAssignment {
    /// Java `Assignment(List)` (null user data).
    #[must_use]
    pub fn new(partitions: Vec<(String, i32)>) -> Self {
        Self { partitions }
    }

    /// Java `Assignment.partitions`.
    #[must_use]
    pub fn partitions(&self) -> &[(String, i32)] {
        &self.partitions
    }
}

impl Default for ConsumerProtocolAssignment {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl fmt::Display for ConsumerProtocolAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Assignment(partitions=[")?;
        for (i, (topic, partition)) in self.partitions.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{topic}-{partition}")?;
        }
        f.write_str("])")
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

/// Java `ConsumerProtocol.checkAssignmentVersion`.
fn check_assignment_version(version: i16) -> Result<i16> {
    if version < ConsumerProtocol::LOWEST_SUPPORTED_VERSION {
        return Err(Error::protocol(format!(
            "Unsupported assignment version: {version}"
        )));
    }
    if version > ConsumerProtocol::HIGHEST_SUPPORTED_VERSION {
        Ok(ConsumerProtocol::HIGHEST_SUPPORTED_VERSION)
    } else {
        Ok(version)
    }
}

/// Group assigned partitions by first-seen topic (Java
/// `ConsumerProtocol.serializeAssignment`; not sorted).
fn group_assigned_partitions(parts: &[(String, i32)]) -> Vec<(String, Vec<i32>)> {
    let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
    for (topic, part) in parts {
        match by_topic.iter_mut().find(|(t, _)| t == topic) {
            Some((_, ps)) => ps.push(*part),
            None => by_topic.push((topic.clone(), vec![*part])),
        }
    }
    by_topic
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

/// Java `FindCoordinatorResponse.coordinatorByKey` when `Coordinators[]`
/// is empty: top-level fields at JSON defaults, lookup `Key` stuffed in.
fn find_coordinator_top_level_for_key(key: impl Into<String>) -> CoordinatorResult {
    CoordinatorResult {
        key: key.into(),
        node_id: 0,
        host: String::new(),
        port: 0,
        error_code: 0,
        error_message: None,
    }
}

/// One coordinator in a FindCoordinator response (v1–v6).
///
/// v1–v3 have a single top-level coordinator (`key` is empty). v4+ is
/// Coordinators[] (KIP-699); `key` is `Coordinators[].Key`.
///
/// [`Self::error`] is Java `FindCoordinatorResponse.prepareOldResponse`
/// with `Node.noNode`. [`Self::error_for_key`] is Java
/// `prepareCoordinatorResponse` with `Node.noNode`.
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

impl CoordinatorResult {
    /// Java `FindCoordinatorResponse.prepareOldResponse` (`error`,
    /// `Node.noNode()`).
    ///
    /// Sets ErrorCode, empty Key (v1–v3 / JSON default), NodeId `-1`,
    /// empty Host, and Port `-1`. `ErrorMessage` is the JSON default
    /// (null); official Java also sets the English `Errors.message`
    /// string. Throttle is the JSON default (`0`).
    #[must_use]
    pub fn error(error_code: i16) -> Self {
        Self::error_for_key(error_code, "")
    }

    /// Java `FindCoordinatorResponse.prepareCoordinatorResponse`
    /// (`error`, `key`, `Node.noNode()`).
    ///
    /// Copies `Key` and sets ErrorCode / `Node.noNode` (`id` `-1`, empty
    /// host, port `-1`). `ErrorMessage` is the JSON default (null);
    /// official Java also sets the English `Errors.message` string.
    #[must_use]
    pub fn error_for_key(error_code: i16, key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            node_id: -1,
            host: String::new(),
            port: -1,
            error_code,
            error_message: None,
        }
    }
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
/// v1–v3 support one key only. More than one key below
/// [`MIN_BATCHED_VERSION`] is Java `NoBatchedFindCoordinatorsException`.
/// Empty `keys` below v4 is a protocol error (`does not support
/// CoordinatorKeys`; use [`encode_find_coordinator_request_typed`]).
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
    if keys.len() > 1 {
        return Err(Error::Unsupported(format!(
            "Cannot create a v{version} FindCoordinator request because we require features supported only in {MIN_BATCHED_VERSION} or later."
        )));
    }
    let Some(key) = keys.first().copied() else {
        return Err(Error::protocol(format!(
            "FindCoordinator version {version} does not support CoordinatorKeys"
        )));
    };
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
/// when `coordinators.len() != 1`). ThrottleTimeMs is the JSON default
/// (`0`) on every spoken version (JSON `1+`).
pub fn encode_find_coordinator_response_coordinators(
    buf: &mut BytesMut,
    version: i16,
    coordinators: &[CoordinatorResult],
) -> crate::error::Result<()> {
    encode_find_coordinator_response_coordinators_with_throttle(buf, version, coordinators, 0)
}

/// Encode FindCoordinator v1–v6 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `1+`: written on every spoken version (this
/// crate does not speak v0). v1–v2 are classic. v3 is flexible. v4–v6
/// are Coordinators (KIP-699; v5 TRANSACTION_ABORTABLE; v6 share groups).
/// Kafka 4.0 `validVersions` is `0-6`. This crate speaks 1–6. v0 and
/// v7+ are not spoken. Official Java `getErrorResponse` sets
/// `throttleTimeMs` from the argument on v2+; v1 leaves the JSON
/// default `0`. Top-level ErrorCode is at bytes 4–5 on v1–v3; v4+ has
/// no top-level ErrorCode.
pub fn encode_find_coordinator_response_coordinators_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    coordinators: &[CoordinatorResult],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = find_coordinator_flexible(version)?;
    buf.put_i32(throttle_time_ms);
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
    let (coords, ..) = decode_find_coordinator_response_coordinators(buf, version)?;
    let c = coords
        .into_iter()
        .next()
        .ok_or_else(|| Error::protocol("missing FindCoordinator Coordinators"))?;
    Ok((c.error_code, c.node_id, c.host, c.port))
}

/// Decode FindCoordinator v0–v6: every Coordinators entry.
///
/// Returns `(coordinators, throttle_time_ms)`. ThrottleTimeMs is JSON
/// `1+` (always on the wire for spoken versions). v1–v3 return a vec
/// of 1 (`key` empty). v4+ is Coordinators[]. Top-level ErrorCode is at
/// bytes 4–5 on v1–v3; v4+ has no top-level ErrorCode.
pub fn decode_find_coordinator_response_coordinators<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<CoordinatorResult>, i32)> {
    let flexible = find_coordinator_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
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
        return Ok((out, throttle_time_ms));
    }
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_string(buf, flexible)?;
    let node_id = buf::get_i32(buf)?;
    let host = buf::get_string(buf, flexible)?.unwrap_or_default();
    let port = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((
        vec![CoordinatorResult {
            key: String::new(),
            node_id,
            host,
            port,
            error_code,
            error_message,
        }],
        throttle_time_ms,
    ))
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
    /// Rebalance timeout (JSON `1+`).
    ///
    /// Spoken v2–v9 always write the field. Official Java
    /// `JoinGroupRequestData.rebalanceTimeoutMs`. Classic Java
    /// `ClassicKafkaConsumer` sends `max.poll.interval.ms`.
    pub rebalance_timeout_ms: i32,
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

    /// Java `JoinGroupRequest.joinReason`.
    ///
    /// Null or empty JoinGroup Reason is `"not provided"`. Encode still writes
    /// the stored field; this is the logged / reported reason (KIP-800).
    #[must_use]
    pub fn join_reason(reason: Option<&str>) -> &str {
        match reason {
            Some(r) if !r.is_empty() => r,
            _ => "not provided",
        }
    }

    /// Java `JoinGroupRequest.validateGroupInstanceId`.
    ///
    /// Java `Topic.validate` with prefix `"Group instance id"` (empty, `.`,
    /// `..`, UTF-16 length above 249, or characters other than ASCII
    /// alphanumerics / `.` / `_` / `-`). Encode does not call this; Java
    /// Builder does not either. See [`Topic::validate`] for the topic-name
    /// prefix.
    pub fn validate_group_instance_id(id: &str) -> Result<()> {
        match detect_invalid_topic_name(id) {
            Some(reason) => Err(Error::protocol(format!(
                "Group instance id is invalid: {reason}"
            ))),
            None => Ok(()),
        }
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

    /// Java `JoinGroupRequest.getErrorResponse`.
    ///
    /// Generation is [`Self::UNKNOWN_GENERATION_ID`]. Protocol name is
    /// [`Self::UNKNOWN_PROTOCOL_NAME`] (encode writes null on v7+). Leader
    /// and member id are [`Self::UNKNOWN_MEMBER_ID`]. Members is empty.
    /// ProtocolType stays the JSON default (`null`) on v7+. Throttle is
    /// the JSON default (`0`); official Java `getErrorResponse` sets
    /// `throttleTimeMs` from the argument. Crate convenience encode
    /// still writes `0`.
    pub fn error_response(
        buf: &mut BytesMut,
        version: i16,
        error_code: i16,
    ) -> crate::error::Result<()> {
        encode_join_group_response(
            buf,
            version,
            error_code,
            Self::UNKNOWN_GENERATION_ID,
            Self::UNKNOWN_PROTOCOL_NAME,
            Self::UNKNOWN_MEMBER_ID,
            Self::UNKNOWN_MEMBER_ID,
            &[],
        )
    }

    /// Java `JoinGroupRequest.Builder.build`.
    ///
    /// A present `group.instance.id` below v5 is
    /// `UnsupportedVersionException` (Java `!= null`, so empty is still
    /// present). Encode still omits the field on those versions; this is
    /// the Builder check. This crate speaks JoinGroup v2–v9.
    pub fn build(version: i16, group_instance_id: Option<&str>) -> Result<()> {
        if group_instance_id.is_some() && version < 5 {
            return Err(Error::Unsupported(format!(
                "The broker join group protocol version {version} does not support usage of config group.instance.id."
            )));
        }
        Ok(())
    }
}

/// Java `org.apache.kafka.common.internals.Topic`.
pub struct Topic;

impl Topic {
    /// Java `Topic.MAX_NAME_LENGTH` (UTF-16 units).
    pub const MAX_NAME_LENGTH: usize = 249;

    /// Java `Topic.GROUP_METADATA_TOPIC_NAME`.
    pub const GROUP_METADATA_TOPIC_NAME: &str = "__consumer_offsets";
    /// Java `Topic.TRANSACTION_STATE_TOPIC_NAME`.
    pub const TRANSACTION_STATE_TOPIC_NAME: &str = "__transaction_state";
    /// Java `Topic.SHARE_GROUP_STATE_TOPIC_NAME`.
    pub const SHARE_GROUP_STATE_TOPIC_NAME: &str = "__share_group_state";
    /// Java `Topic.CLUSTER_METADATA_TOPIC_NAME`.
    ///
    /// Not in Java `INTERNAL_TOPICS` ([`Self::is_internal`] is false).
    pub const CLUSTER_METADATA_TOPIC_NAME: &str = "__cluster_metadata";
    /// Java `Topic.LEGAL_CHARS` (documentation regex; validate uses the charset).
    pub const LEGAL_CHARS: &str = "[a-zA-Z0-9._-]";

    /// Java `Topic.validate` (`Topic name is invalid: ...`).
    ///
    /// Empty, `.`, `..`, UTF-16 length above [`Self::MAX_NAME_LENGTH`], or
    /// characters other than ASCII alphanumerics / `.` / `_` / `-`.
    pub fn validate(name: &str) -> Result<()> {
        match detect_invalid_topic_name(name) {
            Some(reason) => Err(Error::protocol(format!("Topic name is invalid: {reason}"))),
            None => Ok(()),
        }
    }

    /// Java `Topic.isValid`.
    #[must_use]
    pub fn is_valid(name: &str) -> bool {
        detect_invalid_topic_name(name).is_none()
    }

    /// Java `Topic.isInternal`.
    ///
    /// Only [`Self::GROUP_METADATA_TOPIC_NAME`],
    /// [`Self::TRANSACTION_STATE_TOPIC_NAME`], and
    /// [`Self::SHARE_GROUP_STATE_TOPIC_NAME`]. [`Self::CLUSTER_METADATA_TOPIC_NAME`]
    /// is not internal. Subscription matching still skips every `__` prefix
    /// (not this set).
    #[must_use]
    pub fn is_internal(topic: &str) -> bool {
        matches!(
            topic,
            Self::GROUP_METADATA_TOPIC_NAME
                | Self::TRANSACTION_STATE_TOPIC_NAME
                | Self::SHARE_GROUP_STATE_TOPIC_NAME
        )
    }

    /// Java `Topic.hasCollisionChars` (`.` or `_`).
    #[must_use]
    pub fn has_collision_chars(topic: &str) -> bool {
        topic.contains('_') || topic.contains('.')
    }

    /// Java `Topic.unifyCollisionChars` (`.` → `_`).
    #[must_use]
    pub fn unify_collision_chars(topic: &str) -> String {
        topic.replace('.', "_")
    }

    /// Java `Topic.hasCollision`.
    #[must_use]
    pub fn has_collision(topic_a: &str, topic_b: &str) -> bool {
        Self::unify_collision_chars(topic_a) == Self::unify_collision_chars(topic_b)
    }
}

/// Java `Topic.detectInvalidTopic` (UTF-16 length, same charset).
fn detect_invalid_topic_name(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("the empty string is not allowed".into());
    }
    if name == "." {
        return Some("'.' is not allowed".into());
    }
    if name == ".." {
        return Some("'..' is not allowed".into());
    }
    if name.encode_utf16().count() > Topic::MAX_NAME_LENGTH {
        return Some(format!(
            "the length of '{name}' is longer than the max allowed length {}",
            Topic::MAX_NAME_LENGTH
        ));
    }
    if !topic_name_contains_valid_pattern(name) {
        return Some(format!(
            "'{name}' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
        ));
    }
    None
}

/// Java `Topic.containsValidPattern`.
fn topic_name_contains_valid_pattern(name: &str) -> bool {
    name.chars()
        .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-'))
}

/// Java `JoinGroupResponse` helpers.
pub struct JoinGroupResponse;

impl JoinGroupResponse {
    /// Java `JoinGroupResponse.isLeader` (`memberId.equals(leader)`).
    #[must_use]
    pub fn is_leader(member_id: &str, leader: &str) -> bool {
        member_id == leader
    }

    /// Java `JoinGroupResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 3
    }

    /// Java `JoinGroupResponse(JoinGroupResponseData, short)` ProtocolName rewrite.
    ///
    /// Below v7 a null name becomes empty (those versions have no nullable
    /// ProtocolName). v7+ an empty name becomes null. Other values stay
    /// as given. Encode of [`JoinGroupRequest::UNKNOWN_PROTOCOL_NAME`]
    /// already writes null on v7+.
    #[must_use]
    pub fn protocol_name(version: i16, protocol_name: Option<&str>) -> Option<&str> {
        if version < 7 && protocol_name.is_none() {
            Some("")
        } else if version >= 7 && protocol_name == Some("") {
            None
        } else {
            protocol_name
        }
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
    /// Rebalance timeout (JSON `1+`). Spoken v2–v9 always write the field.
    pub rebalance_timeout_ms: i32,
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
            rebalance_timeout_ms: req.rebalance_timeout_ms,
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
    buf.put_i32(req.rebalance_timeout_ms);
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
    let (group_id, member_id, instance, protocols, reason, ..) =
        decode_join_group_request_protocols(buf, version)?;
    let metadata = protocols
        .first()
        .map(|p| p.metadata.clone())
        .unwrap_or_default();
    Ok((group_id, member_id, instance, metadata, reason))
}

/// Decode JoinGroup Protocols of N (v2–v9).
///
/// Returns `(group_id, member_id, instance_id, protocols, reason,
/// session_timeout_ms, rebalance_timeout_ms, protocol_type)`. SessionTimeoutMs
/// is JSON `0+` (INT32 after GroupId; official Java
/// `JoinGroupRequestData.sessionTimeoutMs`). RebalanceTimeoutMs is JSON
/// `1+` (INT32 after SessionTimeoutMs; spoken v2–v9 always on the wire;
/// official Java `JoinGroupRequestData.rebalanceTimeoutMs`). ProtocolType
/// is JSON `0+` (STRING after GroupInstanceId on v5+ / after MemberId
/// below v5; official Java `JoinGroupRequestData.protocolType`).
#[expect(
    clippy::type_complexity,
    reason = "decoded JoinGroup is group, member, instance, protocols, reason, session timeout, rebalance timeout, and protocol type together"
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
    i32,
    i32,
    String,
)> {
    let flexible = join_group_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let session_timeout_ms = buf::get_i32(buf)?;
    let rebalance_timeout_ms = buf::get_i32(buf)?;
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let instance = if version >= 5 {
        buf::get_string(buf, flexible)?
    } else {
        None
    };
    let protocol_type = buf::get_string(buf, flexible)?.unwrap_or_default();
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
    Ok((
        group_id,
        member_id,
        instance,
        protocols,
        reason,
        session_timeout_ms,
        rebalance_timeout_ms,
        protocol_type,
    ))
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
/// ProtocolType is the JSON default (`null`) on v7+.
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
    encode_join_group_response_with_protocol_type(
        buf,
        version,
        error_code,
        generation_id,
        protocol_name,
        leader,
        member_id,
        members,
        None,
    )
}

/// Encode JoinGroup v2–v9 with ProtocolType.
///
/// Below v7 ProtocolType is omitted even when the body has a value.
/// Decode fills `None`. Empty string is still present (Java `!= null`).
/// [`encode_join_group_response`] still writes null. ProtocolName empty
/// still becomes null on v7+. ThrottleTimeMs is the JSON default (`0`)
/// (JSON `2+`).
#[expect(
    clippy::too_many_arguments,
    reason = "JoinGroup response fields match the Apache JSON layout plus ProtocolType"
)]
pub fn encode_join_group_response_with_protocol_type(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    generation_id: i32,
    protocol_name: &str,
    leader: &str,
    member_id: &str,
    members: &[JoinMember],
    protocol_type: Option<&str>,
) -> crate::error::Result<()> {
    encode_join_group_response_fields(
        buf,
        version,
        error_code,
        generation_id,
        protocol_name,
        leader,
        member_id,
        members,
        protocol_type,
        0,
    )
}

/// Encode JoinGroup v2–v9 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `2+`: written on every spoken version (this
/// crate does not speak v0–v1). v2–v5 are classic. v6–v9 are flexible.
/// v5 GroupInstanceId is on members. v7 ProtocolType / nullable
/// ProtocolName. v8 is the same layout as v7 (Reason is on the request).
/// v9 SkipAssignment. Kafka 4.0 `validVersions` is `2-9` (v0–v1
/// removed). This crate speaks 2–9. v0–v1 and v10+ are not spoken.
/// Official Java `getErrorResponse` sets `throttleTimeMs` from the
/// argument. ProtocolType stays the JSON default (`null`). Top-level
/// ErrorCode is at bytes 4–5.
#[expect(
    clippy::too_many_arguments,
    reason = "JoinGroup response fields match the Apache JSON layout plus ThrottleTimeMs"
)]
pub fn encode_join_group_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    generation_id: i32,
    protocol_name: &str,
    leader: &str,
    member_id: &str,
    members: &[JoinMember],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    encode_join_group_response_fields(
        buf,
        version,
        error_code,
        generation_id,
        protocol_name,
        leader,
        member_id,
        members,
        None,
        throttle_time_ms,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "JoinGroup response fields match the Apache JSON layout plus ProtocolType and ThrottleTimeMs"
)]
fn encode_join_group_response_fields(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    generation_id: i32,
    protocol_name: &str,
    leader: &str,
    member_id: &str,
    members: &[JoinMember],
    protocol_type: Option<&str>,
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = join_group_flexible(version)?;
    buf.put_i32(throttle_time_ms);
    buf.put_i16(error_code);
    buf.put_i32(generation_id);
    if version >= 7 {
        buf::put_string(buf, true, protocol_type)?;
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

/// Decode JoinGroup: `(error, generation, protocol, leader, member_id, skip_assignment, members, protocol_type, throttle_time_ms)`.
///
/// Below v7 ProtocolType is omitted; decode fills `None`. ThrottleTimeMs
/// is JSON `2+` (always on the wire for spoken versions). Top-level
/// ErrorCode is at bytes 4–5.
#[expect(
    clippy::type_complexity,
    reason = "JoinGroup response is error, generation, protocol, leader, member, skip, members, protocol type, throttle"
)]
pub fn decode_join_group_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    i16,
    i32,
    String,
    String,
    String,
    bool,
    Vec<JoinMember>,
    Option<String>,
    i32,
)> {
    let flexible = join_group_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let error = buf::get_i16(buf)?;
    let generation = buf::get_i32(buf)?;
    let protocol_type = if version >= 7 {
        buf::get_string(buf, true)?
    } else {
        None
    };
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
        protocol_type,
        throttle_time_ms,
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

impl SyncGroupRequest<'_> {
    /// Java `SyncGroupRequest.areMandatoryProtocolTypeAndNamePresent`.
    ///
    /// ProtocolType and ProtocolName are mandatory since version 5. Below v5
    /// this is always `true`. On v5+, both must be present (`Some`). Empty
    /// string is present: Java checks `!= null`, not empty. This crate's
    /// request type stores `&str` (never JSON-null); pass `None` here to
    /// model a null STRING. Encode still writes the stored fields.
    #[must_use]
    pub const fn are_mandatory_protocol_type_and_name_present(
        version: i16,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
    ) -> bool {
        if version >= 5 {
            protocol_type.is_some() && protocol_name.is_some()
        } else {
            true
        }
    }

    /// Java `SyncGroupRequest.getErrorResponse`.
    ///
    /// Assignment is empty. ProtocolType / ProtocolName stay the JSON default
    /// (null) on v5+. ThrottleTimeMs is written on v1+ from `throttle_time_ms`.
    /// Below v1 the field is omitted even when that value is non-zero. Decode
    /// fills `0`.
    pub fn error_response(
        buf: &mut BytesMut,
        version: i16,
        error_code: i16,
        throttle_time_ms: i32,
    ) -> crate::error::Result<()> {
        encode_sync_group_response_with_throttle(buf, version, error_code, &[], throttle_time_ms)
    }

    /// Java `SyncGroupRequest.groupAssignments`.
    ///
    /// Member id to assignment bytes. A later assignment for the same
    /// member overwrites (Java `HashMap.put`).
    #[must_use]
    pub fn group_assignments(assignments: &[(String, Vec<u8>)]) -> HashMap<String, Vec<u8>> {
        let mut map = HashMap::with_capacity(assignments.len());
        for (member_id, assignment) in assignments {
            let _prev = map.insert(member_id.clone(), assignment.clone());
        }
        map
    }

    /// Java `SyncGroupRequest.Builder.build`.
    ///
    /// A present `group.instance.id` below v3 is
    /// `UnsupportedVersionException` (Java `!= null`, so empty is still
    /// present). Encode still omits the field on those versions; this is
    /// the Builder check.
    pub fn build(version: i16, group_instance_id: Option<&str>) -> Result<()> {
        if group_instance_id.is_some() && version < 3 {
            return Err(Error::Unsupported(format!(
                "The broker sync group protocol version {version} does not support usage of config group.instance.id."
            )));
        }
        Ok(())
    }
}

/// Java `SyncGroupResponse` helpers.
pub struct SyncGroupResponse;

impl SyncGroupResponse {
    /// Java `SyncGroupResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 2
    }
}

/// Encode SyncGroup v0–v5.
///
/// Kafka 4.0 JSON: `validVersions: "0-5"`, `flexibleVersions: "4+"`.
/// v0–v2 are GroupId + GenerationId + MemberId + Assignments (v1 and v2
/// match v0). v3 GroupInstanceId. Below v3 GroupInstanceId is omitted
/// even when the body has an instance id; decode fills `None`. v4
/// flexible. v5 ProtocolType / ProtocolName (KIP-559). Below v5 those
/// fields are omitted even when the body has values; decode fills `None`.
/// GenerationId is JSON `0+` (INT32 after GroupId; decode returns it
/// last). This crate speaks 0–5. v6+ is not spoken.
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

/// Decode SyncGroup: `(group_id, member_id, assignments, group_instance_id,
/// protocol_type, protocol_name, generation_id)`.
///
/// GenerationId is JSON `0+` (always on the wire; last). Below v3
/// GroupInstanceId is omitted; decode fills `None`. Below v5 ProtocolType
/// / ProtocolName are omitted; decode fills `None`.
#[expect(
    clippy::type_complexity,
    reason = "SyncGroup decode is group, member, assignments, instance, protocol type, protocol name, generation"
)]
pub fn decode_sync_group_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    String,
    String,
    Vec<(String, Vec<u8>)>,
    Option<String>,
    Option<String>,
    Option<String>,
    i32,
)> {
    let flexible = sync_group_flexible(version)?;
    let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let generation_id = buf::get_i32(buf)?;
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let inst = if version >= 3 {
        buf::get_string(buf, flexible)?
    } else {
        None
    };
    let (ptype, pname) = if version >= 5 {
        (buf::get_string(buf, true)?, buf::get_string(buf, true)?)
    } else {
        (None, None)
    };
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
    Ok((
        group_id,
        member_id,
        assignments,
        inst,
        ptype,
        pname,
        generation_id,
    ))
}

/// Encode SyncGroup v0–v5. Throttle is `0` on v1+. ProtocolType /
/// ProtocolName are the JSON default (`null`) on v5+.
pub fn encode_sync_group_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    assignment: &[u8],
) -> crate::error::Result<()> {
    encode_sync_group_response_with_throttle(buf, version, error_code, assignment, 0)
}

/// Encode SyncGroup v0–v5 with ThrottleTimeMs.
///
/// Below v1 ThrottleTimeMs is omitted even when the body has a non-zero
/// value. Decode fills `0`. ProtocolType / ProtocolName stay the JSON
/// default (`null`) on v5+. v4+ is flexible.
pub fn encode_sync_group_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    assignment: &[u8],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    write_sync_group_response(
        buf,
        version,
        error_code,
        assignment,
        None,
        None,
        throttle_time_ms,
    )
}

/// Encode SyncGroup v0–v5 with ProtocolType / ProtocolName.
///
/// Below v5 those fields are omitted even when the body has values.
/// Decode fills `None`. Throttle is `0` on v1+. v5 is flexible.
pub fn encode_sync_group_response_with_protocol(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    assignment: &[u8],
    protocol_type: Option<&str>,
    protocol_name: Option<&str>,
) -> crate::error::Result<()> {
    write_sync_group_response(
        buf,
        version,
        error_code,
        assignment,
        protocol_type,
        protocol_name,
        0,
    )
}

fn write_sync_group_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    assignment: &[u8],
    protocol_type: Option<&str>,
    protocol_name: Option<&str>,
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = sync_group_flexible(version)?;
    if version >= 1 {
        buf.put_i32(throttle_time_ms);
    }
    buf.put_i16(error_code);
    if version >= 5 {
        buf::put_string(buf, true, protocol_type)?;
        buf::put_string(buf, true, protocol_name)?;
    }
    buf::put_bytes(buf, flexible, Some(assignment))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode SyncGroup: `(error_code, assignment, protocol_type, protocol_name, throttle_time_ms)`.
///
/// Below v1 ThrottleTimeMs is omitted; decode fills `0`. Below v5
/// ProtocolType / ProtocolName are omitted; decode fills `None`.
#[expect(
    clippy::type_complexity,
    reason = "SyncGroup decode returns error, assignment, protocol type, protocol name, and throttle together"
)]
pub fn decode_sync_group_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, Vec<u8>, Option<String>, Option<String>, i32)> {
    let flexible = sync_group_flexible(version)?;
    let throttle_time_ms = if version >= 1 { buf::get_i32(buf)? } else { 0 };
    let error = buf::get_i16(buf)?;
    let (ptype, pname) = if version >= 5 {
        (buf::get_string(buf, true)?, buf::get_string(buf, true)?)
    } else {
        (None, None)
    };
    let assignment = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((error, assignment, ptype, pname, throttle_time_ms))
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

/// Java `HeartbeatResponse` helpers.
pub struct HeartbeatResponse;

impl HeartbeatResponse {
    /// Java `HeartbeatResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 2
    }
}

/// Java `HeartbeatRequest` helpers.
pub struct HeartbeatRequest;

impl HeartbeatRequest {
    /// Java `HeartbeatRequest.Builder.build`.
    ///
    /// A present `group.instance.id` below v3 is
    /// `UnsupportedVersionException` (Java `!= null`, so empty is still
    /// present). Encode still omits the field on those versions; this is
    /// the Builder check.
    pub fn build(version: i16, group_instance_id: Option<&str>) -> Result<()> {
        if group_instance_id.is_some() && version < 3 {
            return Err(Error::Unsupported(format!(
                "The broker heartbeat protocol version {version} does not support usage of config group.instance.id."
            )));
        }
        Ok(())
    }

    /// Java `HeartbeatRequest.getErrorResponse`.
    ///
    /// ThrottleTimeMs is written on v1+ from `throttle_time_ms`. Below v1
    /// the field is omitted even when that value is non-zero. Decode fills
    /// `0`.
    pub fn error_response(
        buf: &mut BytesMut,
        version: i16,
        error_code: i16,
        throttle_time_ms: i32,
    ) -> crate::error::Result<()> {
        encode_heartbeat_response_with_throttle(buf, version, error_code, throttle_time_ms)
    }
}

/// Encode Heartbeat v0–v4.
///
/// Kafka 4.0 JSON: `validVersions: "0-4"`, `flexibleVersions: "4+"`.
/// v0–v2 are GroupId + GenerationId + MemberId (v1 and v2 match v0).
/// v3 GroupInstanceId. v4 flexible. This crate speaks 0–4. v5+ is not
/// spoken.
/// Below v3 GroupInstanceId is omitted even when the body has an instance
/// id. Decode fills `None`.
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

/// Decode Heartbeat: `(group_id, generation_id, member_id, group_instance_id)`.
///
/// Below v3 GroupInstanceId is omitted; decode fills `None`.
pub fn decode_heartbeat_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, i32, String, Option<String>)> {
    let flexible = heartbeat_flexible(version)?;
    let g = buf::get_string(buf, flexible)?.unwrap_or_default();
    let gen = buf::get_i32(buf)?;
    let m = buf::get_string(buf, flexible)?.unwrap_or_default();
    let inst = if version >= 3 {
        buf::get_string(buf, flexible)?
    } else {
        None
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((g, gen, m, inst))
}

/// Encode Heartbeat v0–v4. Throttle is `0` on v1+.
pub fn encode_heartbeat_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
) -> crate::error::Result<()> {
    encode_heartbeat_response_with_throttle(buf, version, error_code, 0)
}

/// Encode Heartbeat v0–v4 with ThrottleTimeMs.
///
/// Below v1 ThrottleTimeMs is omitted even when the body has a non-zero
/// value. Decode fills `0`. v4 is flexible.
pub fn encode_heartbeat_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = heartbeat_flexible(version)?;
    if version >= 1 {
        buf.put_i32(throttle_time_ms);
    }
    buf.put_i16(error_code);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode Heartbeat: `(error_code, throttle_time_ms)`.
///
/// Below v1 ThrottleTimeMs is omitted; decode fills `0`.
pub fn decode_heartbeat_response<B: Buf>(buf: &mut B, version: i16) -> Result<(i16, i32)> {
    let flexible = heartbeat_flexible(version)?;
    let throttle_time_ms = if version >= 1 { buf::get_i32(buf)? } else { 0 };
    let err = buf::get_i16(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((err, throttle_time_ms))
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
    encode_leave_group_response_with_throttle(buf, version, error_code, members, 0)
}

/// Encode LeaveGroup v0–v5 with ThrottleTimeMs.
///
/// Below v1 ThrottleTimeMs is omitted even when the body has a non-zero
/// value. Decode fills `0`. Members are v3+. v4+ is flexible.
pub fn encode_leave_group_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    members: &[LeaveGroupMemberResult],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = leave_group_flexible(version)?;
    if version >= 1 {
        buf.put_i32(throttle_time_ms);
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

/// Decode LeaveGroup v0–v5: `(error_code, members, throttle_time_ms)`.
///
/// Members are empty below v3. Below v1 ThrottleTimeMs is omitted; decode
/// fills `0`.
pub fn decode_leave_group_response_version<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, Vec<LeaveGroupMemberResult>, i32)> {
    let flexible = leave_group_flexible(version)?;
    let throttle_time_ms = if version >= 1 { buf::get_i32(buf)? } else { 0 };
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
    Ok((error_code, members, throttle_time_ms))
}

/// Java `LeaveGroupRequest` helpers.
pub struct LeaveGroupRequest;

impl LeaveGroupRequest {
    /// Java `LeaveGroupRequest.getErrorResponse`.
    ///
    /// Members stay empty (request members are not copied). ThrottleTimeMs
    /// is written on v1+ from `throttle_time_ms`. Below v1 the field is
    /// omitted even when that value is non-zero. Decode fills `0`.
    pub fn error_response(
        buf: &mut BytesMut,
        version: i16,
        error_code: i16,
        throttle_time_ms: i32,
    ) -> crate::error::Result<()> {
        encode_leave_group_response_with_throttle(buf, version, error_code, &[], throttle_time_ms)
    }

    /// Java `LeaveGroupRequest.members`.
    ///
    /// v0–v2 is a singleton of the first member's `member_id` (Java
    /// `data.memberId()`); instance id and reason stay unset. Empty
    /// members is a singleton with empty `member_id` (JSON default).
    /// v3+ is the request Members list.
    #[must_use]
    pub fn members(version: i16, members: &[LeaveGroupMember]) -> Vec<LeaveGroupMember> {
        if version <= 2 {
            let member_id = members
                .first()
                .map(|m| m.member_id.clone())
                .unwrap_or_default();
            vec![LeaveGroupMember {
                member_id,
                group_instance_id: None,
                reason: None,
            }]
        } else {
            members.to_vec()
        }
    }
}

/// Java `LeaveGroupResponse` helpers.
pub struct LeaveGroupResponse;

impl LeaveGroupResponse {
    /// Java `LeaveGroupResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 2
    }

    /// Java `LeaveGroupResponse.errorCounts`.
    ///
    /// Counts the top-level `errorCode` (including `NONE`) plus each
    /// member-level code (including `NONE`). Members are empty below v3,
    /// so that case is the top-level code alone.
    #[must_use]
    pub fn error_counts(error_code: i16, members: &[LeaveGroupMemberResult]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        let count = counts.entry(error_code).or_insert(0);
        *count += 1;
        for member in members {
            let count = counts.entry(member.error_code).or_insert(0);
            *count += 1;
        }
        counts
    }

    /// Java `LeaveGroupResponse.error`.
    ///
    /// Top-level `errorCode` when it is not `NONE`; otherwise the first
    /// member-level non-`NONE` code; otherwise `NONE`.
    #[must_use]
    pub fn error(error_code: i16, members: &[LeaveGroupMemberResult]) -> i16 {
        if error_code != 0 {
            return error_code;
        }
        for member in members {
            if member.error_code != 0 {
                return member.error_code;
            }
        }
        0
    }

    /// Java `LeaveGroupResponse(LeaveGroupResponseData, short)`.
    ///
    /// v3+ keeps `error_code` and `members`. Below v3, a non-`NONE`
    /// top-level code drops members and keeps that code. `NONE` at the
    /// top requires exactly one member (`UnsupportedVersionException`
    /// otherwise) and copies that member's `errorCode` onto the
    /// top-level; members are dropped. Throttle is the JSON default
    /// (`0`) on v1+ (Java `new LeaveGroupResponseData()` does not copy
    /// throttle).
    pub fn for_version(
        version: i16,
        error_code: i16,
        members: &[LeaveGroupMemberResult],
    ) -> Result<(i16, Vec<LeaveGroupMemberResult>)> {
        if version >= 3 {
            return Ok((error_code, members.to_vec()));
        }
        if error_code != 0 {
            return Ok((error_code, Vec::new()));
        }
        match members {
            [member] => Ok((member.error_code, Vec::new())),
            _ => Err(Error::Unsupported(format!(
                "LeaveGroup response version {version} can only contain one member, got {} members.",
                members.len()
            ))),
        }
    }

    /// Java `LeaveGroupResponse(List, Errors, int, short)`.
    ///
    /// v3+ keeps `error_code` and `members`. v0–v2 fold [`Self::error`]
    /// into the top-level code and drop members (zero or many members
    /// are allowed; unlike [`Self::for_version`] this does not require
    /// exactly one). Throttle is the JSON default (`0`) on v1+ (omitted
    /// on v0; not part of this helper).
    #[must_use]
    pub fn from_members(
        version: i16,
        error_code: i16,
        members: &[LeaveGroupMemberResult],
    ) -> (i16, Vec<LeaveGroupMemberResult>) {
        if version <= 2 {
            (Self::error(error_code, members), Vec::new())
        } else {
            (error_code, members.to_vec())
        }
    }
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
///
/// [`Self::error_result`] is Java `OffsetCommitRequest.getErrorResponse` one
/// topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions to commit.
    pub partitions: Vec<OffsetPartition>,
}

impl OffsetTopic {
    /// Java `OffsetCommitRequest.getErrorResponse` one topic.
    ///
    /// Copies `Name` and each `PartitionIndex`. Nested body is
    /// PartitionIndex + ErrorCode (no English message). Committed
    /// offset / metadata / leader epoch stay on the request. Throttle
    /// on the response is the JSON default (`0`).
    #[must_use]
    pub fn error_result(&self, error_code: i16) -> OffsetCommitResponseTopic {
        OffsetCommitResponseTopic {
            topic: self.topic.clone(),
            partitions: self
                .partitions
                .iter()
                .map(|p| OffsetCommitResponsePartition::error(p.partition, error_code))
                .collect(),
        }
    }

    /// Java `OffsetCommitRequest.getErrorResponse` Topics.
    ///
    /// Maps each request topic through [`Self::error_result`].
    #[must_use]
    pub fn error_results(topics: &[Self], error_code: i16) -> Vec<OffsetCommitResponseTopic> {
        topics
            .iter()
            .map(|topic| topic.error_result(error_code))
            .collect()
    }
}

/// One partition in an OffsetCommit v2–v9 response.
///
/// [`Self::error`] is Java `OffsetCommitRequest.getErrorResponse` partition
/// body (PartitionIndex + ErrorCode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitResponsePartition {
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl OffsetCommitResponsePartition {
    /// Java `OffsetCommitRequest.getErrorResponse` partition body.
    ///
    /// Sets `PartitionIndex` and `ErrorCode`. The nested body has no
    /// error message field.
    #[must_use]
    pub fn error(partition: i32, error_code: i16) -> Self {
        Self {
            partition,
            error_code,
        }
    }

    /// Java `OffsetCommitResponsePartition.partitionIndex`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Per-partition error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }
}

/// One topic in an OffsetCommit v2–v9 response.
///
/// [`OffsetTopic::error_result`] is Java
/// `OffsetCommitRequest.getErrorResponse` one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitResponseTopic {
    /// Topic name (Java `Name`).
    pub topic: String,
    /// Per-partition results.
    pub partitions: Vec<OffsetCommitResponsePartition>,
}

impl OffsetCommitResponseTopic {
    /// Java `OffsetCommitResponseTopic.name`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Java `OffsetCommitResponseTopic.partitions`.
    #[must_use]
    pub fn partitions(&self) -> &[OffsetCommitResponsePartition] {
        &self.partitions
    }
}

/// Topic + partition indexes for OffsetFetch v1–v9.
///
/// [`Self::error_result`] is Java `OffsetFetchRequest.getErrorResponse` one
/// topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to fetch.
    pub partitions: Vec<i32>,
}

impl OffsetFetchTopic {
    /// Java `OffsetFetchRequest.getErrorResponse` one topic.
    ///
    /// OffsetFetch v1 fills each partition via [`FetchedOffset::error`]. v2
    /// and later omit partitions (top-level / group ErrorCode). Throttle on
    /// the response is the JSON default (`0`).
    #[must_use]
    pub fn error_result(&self, version: i16, error_code: i16) -> FetchedOffsetTopic {
        FetchedOffsetTopic {
            topic: self.topic.clone(),
            partitions: if version < 2 {
                self.partitions
                    .iter()
                    .map(|&p| FetchedOffset::error(p, error_code))
                    .collect()
            } else {
                Vec::new()
            },
        }
    }
}

/// One partition in an OffsetFetch v1–v9 response.
///
/// Java `OffsetFetchResponse.PartitionData` plus the partition index.
/// [`Self::INVALID_OFFSET`] / [`Self::NO_METADATA`] / [`Self::has_error`]
/// / [`Self::unknown_partition`] / [`Self::unauthorized_partition`] /
/// [`Self::error`] are Java
/// `OffsetFetchResponse.INVALID_OFFSET` / `NO_METADATA` /
/// `PartitionData.hasError` / `UNKNOWN_PARTITION` / `UNAUTHORIZED_PARTITION`
/// / `OffsetFetchRequest.getErrorResponse` partition body.
/// [`Display`] is Java `PartitionData.toString`.
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

    /// Java `OffsetFetchRequest.getErrorResponse` partition body.
    ///
    /// Fills [`Self::INVALID_OFFSET`], empty leader epoch, and
    /// [`Self::NO_METADATA`]. OffsetFetch v1 writes this on every request
    /// partition; v2 and later omit partitions.
    #[must_use]
    pub fn error(partition: i32, error_code: i16) -> Self {
        Self::new(partition, Self::INVALID_OFFSET, error_code)
    }

    /// Java `OffsetFetchResponse.UNKNOWN_PARTITION`.
    ///
    /// [`Self::INVALID_OFFSET`], empty leader epoch, [`Self::NO_METADATA`],
    /// and `UNKNOWN_TOPIC_OR_PARTITION`. Java `PartitionData` has no
    /// partition index; this crate carries it on [`Self`].
    #[must_use]
    pub fn unknown_partition(partition: i32) -> Self {
        Self::new(
            partition,
            Self::INVALID_OFFSET,
            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
        )
    }

    /// Java `OffsetFetchResponse.UNAUTHORIZED_PARTITION`.
    ///
    /// [`Self::INVALID_OFFSET`], empty leader epoch, [`Self::NO_METADATA`],
    /// and `TOPIC_AUTHORIZATION_FAILED`. Java `PartitionData` has no
    /// partition index; this crate carries it on [`Self`].
    #[must_use]
    pub fn unauthorized_partition(partition: i32) -> Self {
        Self::new(
            partition,
            Self::INVALID_OFFSET,
            crate::error::TOPIC_AUTHORIZATION_FAILED,
        )
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
        f.write_str(for_code(self.error_code))?;
        f.write_str("')")
    }
}

/// Java `OffsetFetchResponse` helpers.
pub struct OffsetFetchResponse;

impl OffsetFetchResponse {
    /// Java `OffsetFetchResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 4
    }

    /// Java `OffsetFetchResponse.errorCounts`.
    ///
    /// v8+ counts each group-level `errorCode` (including `NONE`) plus
    /// each partition-level code (including `NONE`). v2–v7 count the
    /// top-level `errorCode` stored on the single
    /// [`OffsetFetchGroupResult`] plus partitions. v1 has no top-level
    /// field; Java uses the first non-partition error from the
    /// partitions (or `NONE`) plus each partition code. Partition errors
    /// are `UNKNOWN_TOPIC_OR_PARTITION` and `TOPIC_AUTHORIZATION_FAILED`.
    #[must_use]
    pub fn error_counts(version: i16, groups: &[OffsetFetchGroupResult]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        if version >= 8 {
            for group in groups {
                let count = counts.entry(group.error_code).or_insert(0);
                *count += 1;
                offset_fetch_add_partition_error_counts(&mut counts, &group.topics);
            }
            return counts;
        }
        let top = offset_fetch_v0_to_v7_error(version, groups);
        let count = counts.entry(top).or_insert(0);
        *count += 1;
        offset_fetch_add_partition_error_counts(
            &mut counts,
            groups.first().map(|g| g.topics.as_slice()).unwrap_or(&[]),
        );
        counts
    }

    /// Java `OffsetFetchResponse.groupHasError`.
    ///
    /// v8+: `true` when the named group's `errorCode` is not `NONE`. A
    /// missing group is `false` (Java `groupLevelErrors.get` is null and
    /// the wrapper `error` field is null). v1–v7 ignore `group_id` and
    /// return whether the top-level code is not `NONE`. v1 has no
    /// top-level field; Java uses the first non-partition error from the
    /// partitions (or `NONE`).
    #[must_use]
    pub fn group_has_error(
        version: i16,
        groups: &[OffsetFetchGroupResult],
        group_id: &str,
    ) -> bool {
        Self::group_level_error(version, groups, group_id).is_some_and(|code| code != 0)
    }

    /// Java `OffsetFetchResponse.groupLevelError`.
    ///
    /// v8+: the named group's `errorCode`, or `None` when the group is
    /// missing (Java `null`). Duplicate group ids keep the last match
    /// (Java `HashMap.put`). v1–v7 ignore `group_id` and return `Some` of
    /// the top-level code (including `NONE`). v1 synthesizes that code as
    /// in [`Self::error_counts`]. Distinct from [`Self::error`], which is
    /// always `None` on v8+.
    #[must_use]
    pub fn group_level_error(
        version: i16,
        groups: &[OffsetFetchGroupResult],
        group_id: &str,
    ) -> Option<i16> {
        if version >= 8 {
            groups
                .iter()
                .rfind(|group| group.group_id == group_id)
                .map(|group| group.error_code)
        } else {
            Some(offset_fetch_v0_to_v7_error(version, groups))
        }
    }

    /// Java `OffsetFetchResponse.error`.
    ///
    /// v8+ is `None` even when groups have errors (Java the wrapper
    /// `error` field is null; `groupLevelErrors` is the map). Distinct
    /// from [`Self::group_level_error`], which looks up a named group on
    /// v8+. v2–v7 are `Some` of the top-level `errorCode` (including
    /// `NONE`). v1 synthesizes the first non-partition error as in
    /// [`Self::error_counts`]. Java `hasError()` is not mapped: it NPEs
    /// on v8+ (`error != Errors.NONE` when `error` is null).
    #[must_use]
    pub fn error(version: i16, groups: &[OffsetFetchGroupResult]) -> Option<i16> {
        if version >= 8 {
            None
        } else {
            Some(offset_fetch_v0_to_v7_error(version, groups))
        }
    }

    /// Java `OffsetFetchResponse.partitionDataMap`.
    ///
    /// v1–v7 ignore `group_id` and flatten the first group's topics
    /// (Java `data.topics()`). v8+ with no groups is an empty map (Java
    /// `groupLevelErrors` is empty, so the v0–v7 path runs on empty
    /// Topics). v8+ with groups uses the first matching `group_id`
    /// (Java `stream().filter().collect().get(0)`). A missing group is
    /// [`Error::protocol`] (Java `IndexOutOfBoundsException`). A later
    /// partition overwrites the same pair (Java `HashMap.put`). Values
    /// are [`FetchedOffset`] (Java `PartitionData` plus the partition
    /// index).
    pub fn partition_data_map(
        version: i16,
        groups: &[OffsetFetchGroupResult],
        group_id: &str,
    ) -> Result<HashMap<(String, i32), FetchedOffset>> {
        if version >= 8 {
            if groups.is_empty() {
                return Ok(HashMap::new());
            }
            let Some(group) = groups.iter().find(|group| group.group_id == group_id) else {
                return Err(Error::protocol(format!(
                    "no group named {group_id} in OffsetFetchResponse"
                )));
            };
            return Ok(offset_fetch_partition_data_map(&group.topics));
        }
        Ok(offset_fetch_partition_data_map(
            groups.first().map(|g| g.topics.as_slice()).unwrap_or(&[]),
        ))
    }

    /// Java `OffsetFetchResponse(Errors, Map)` / `(int, Errors, Map)` Topics.
    ///
    /// Groups `(topic, PartitionData)` by name. A later entry for the
    /// same topic appends (Java `HashMap.getOrDefault` then
    /// `partitions().add`). Topic order is first-seen (Java
    /// `HashMap.values` order is unspecified). The Java map key is
    /// `TopicPartition`; grouping uses only the name. Duplicate
    /// partitions for the same pair are kept (`ArrayList`). Inverse of
    /// [`Self::partition_data_map`] flatten (v1–v7 `data.topics()`).
    /// Throttle and the top-level error are not part of this helper
    /// (crate encode writes throttle `0` on v3+; v2 has no throttle
    /// field).
    #[must_use]
    pub fn from_partition_data<'a, I>(response_data: I) -> Vec<FetchedOffsetTopic>
    where
        I: IntoIterator<Item = (&'a str, FetchedOffset)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<FetchedOffset>> = HashMap::new();
        for (topic, partition) in response_data {
            by_topic
                .entry(topic.to_string())
                .or_insert_with(|| {
                    order.push(topic.to_string());
                    Vec::new()
                })
                .push(partition);
        }
        order
            .into_iter()
            .filter_map(|topic| {
                by_topic
                    .remove(&topic)
                    .map(|partitions| FetchedOffsetTopic { topic, partitions })
            })
            .collect()
    }

    /// Java `OffsetFetchResponse(int, Map, Map)` Groups (v8+).
    ///
    /// Each outer entry is one group. Inner `(topic, PartitionData)`
    /// grouping is [`Self::from_partition_data`]. A group in `errors`
    /// but not in `response_data` is omitted (Java iterates
    /// `responseData.entrySet`). A group in `response_data` missing from
    /// `errors` is [`Error::protocol`] (Java `NullPointerException` on
    /// `errors.get`). Group order is iterator order (Java outer
    /// `HashMap.entrySet` order is unspecified). Throttle is not part of
    /// this helper (crate encode writes the JSON default `0`).
    pub fn from_groups_partition_data<'a, I, J>(
        errors: &HashMap<String, i16>,
        response_data: I,
    ) -> Result<Vec<OffsetFetchGroupResult>>
    where
        I: IntoIterator<Item = (&'a str, J)>,
        J: IntoIterator<Item = (&'a str, FetchedOffset)>,
    {
        let mut groups = Vec::new();
        for (group_id, partitions) in response_data {
            let Some(&error_code) = errors.get(group_id) else {
                return Err(Error::protocol(format!(
                    "no group named {group_id} in OffsetFetchResponse errors"
                )));
            };
            groups.push(OffsetFetchGroupResult {
                group_id: group_id.to_string(),
                topics: Self::from_partition_data(partitions),
                error_code,
            });
        }
        Ok(groups)
    }

    /// Java `OffsetFetchResponse` constructor from a group list and version.
    ///
    /// v8+ returns `groups` as-is. Below v8 Java requires exactly one
    /// group (`UnsupportedVersionException` otherwise) and copies that
    /// group's Topics. On versions before 2, a non-NONE group error
    /// replaces every partition body with [`FetchedOffset::INVALID_OFFSET`]
    /// / [`FetchedOffset::NO_METADATA`] /
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] and that group error
    /// (those versions have no top-level error field).
    pub fn from_groups(
        version: i16,
        groups: &[OffsetFetchGroupResult],
    ) -> Result<Vec<OffsetFetchGroupResult>> {
        if version >= 8 {
            return Ok(groups.to_vec());
        }
        let [group] = groups else {
            return Err(Error::Unsupported(format!(
                "Version {version} of OffsetFetchResponse only supports one group."
            )));
        };
        if version < 2 && group.error_code != 0 {
            let topics = group
                .topics
                .iter()
                .map(|topic| FetchedOffsetTopic {
                    topic: topic.topic.clone(),
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            FetchedOffset::error(partition.partition, group.error_code)
                        })
                        .collect(),
                })
                .collect();
            return Ok(vec![OffsetFetchGroupResult {
                group_id: group.group_id.clone(),
                topics,
                error_code: group.error_code,
            }]);
        }
        Ok(vec![group.clone()])
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
/// [`Self::is_all_partitions`] is Java `OffsetFetchRequest.isAllPartitions`
/// (`topics == null`, not empty). [`OffsetFetchRequest::is_all_partitions_for_group`]
/// is Java `isAllPartitionsForGroup` (first matching GroupId).
/// [`Self::error_result`] is Java `OffsetFetchRequest.getErrorResponse` one
/// group on v8+ (empty Topics; request partitions are not copied).
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

    /// Java `OffsetFetchRequest.isAllPartitions` (`data.topics() == null`)
    /// and the per-group check inside `isAllPartitionsForGroup`.
    ///
    /// `None` Topics is every committed partition (v2+). `Some` empty is
    /// not all partitions (empty classic INT32 `0` / compact `0x01`, not
    /// null). Named lookup with a missing group is
    /// [`OffsetFetchRequest::is_all_partitions_for_group`].
    #[must_use]
    pub fn is_all_partitions(&self) -> bool {
        self.topics.is_none()
    }

    /// Java `OffsetFetchRequest.getErrorResponse` one group on v8+.
    ///
    /// Copies `GroupId` and sets ErrorCode. Topics stay empty (Java empty
    /// `HashMap`; request partitions are not copied). Throttle on the
    /// response is the JSON default (`0`). OffsetFetch v1 fills partitions
    /// via [`OffsetFetchTopic::error_result`]; v2–v7 omit partitions at
    /// the top-level Topics field.
    #[must_use]
    pub fn error_result(&self, error_code: i16) -> OffsetFetchGroupResult {
        OffsetFetchGroupResult::error(self.group_id.as_str(), error_code)
    }

    /// Java `OffsetFetchRequest.getErrorResponse` Groups on v8+.
    ///
    /// Maps each request group through [`Self::error_result`].
    #[must_use]
    pub fn error_results(groups: &[Self], error_code: i16) -> Vec<OffsetFetchGroupResult> {
        groups
            .iter()
            .map(|group| group.error_result(error_code))
            .collect()
    }
}

/// Java `OffsetFetchRequest` helpers.
pub struct OffsetFetchRequest;

impl OffsetFetchRequest {
    /// Java `OffsetFetchRequest.groupIdsToPartitions`.
    ///
    /// Each group id maps to that group's `(topic, partition)` list.
    /// `None` Topics is `None` (every committed partition). `Some` empty
    /// is an empty list, not all partitions. A later group overwrites an
    /// earlier one for the same id (Java `HashMap.put`).
    #[must_use]
    pub fn group_ids_to_partitions(
        groups: &[OffsetFetchGroup],
    ) -> HashMap<String, Option<Vec<(String, i32)>>> {
        let mut group_ids_to_partitions = HashMap::new();
        for group in groups {
            let tp_list = group.topics.as_deref().map(Self::topic_partitions);
            let _prev = group_ids_to_partitions.insert(group.group_id.clone(), tp_list);
        }
        group_ids_to_partitions
    }

    /// Java `OffsetFetchRequest.groupIdsToTopics`.
    ///
    /// Each group id maps to that group's Topics list as-is. `None`
    /// Topics is `None` (every committed partition). `Some` empty is an
    /// empty list, not all partitions. A later group overwrites an
    /// earlier one for the same id (Java `HashMap.put`). Distinct from
    /// [`Self::group_ids_to_partitions`], which flattens to
    /// `(topic, partition)` pairs.
    #[must_use]
    pub fn group_ids_to_topics(
        groups: &[OffsetFetchGroup],
    ) -> HashMap<String, Option<Vec<OffsetFetchTopic>>> {
        let mut group_ids_to_topics = HashMap::new();
        for group in groups {
            let _prev = group_ids_to_topics.insert(group.group_id.clone(), group.topics.clone());
        }
        group_ids_to_topics
    }

    /// Java `OffsetFetchRequest.groupIds`.
    ///
    /// Group ids in request order. Duplicate ids are kept (Java `stream`
    /// into a `List`). Distinct from [`Self::group_ids_to_topics`], which
    /// overwrites the same id (`HashMap.put`).
    #[must_use]
    pub fn group_ids(groups: &[OffsetFetchGroup]) -> Vec<String> {
        groups.iter().map(|group| group.group_id.clone()).collect()
    }

    /// Java `OffsetFetchRequest.groups`.
    ///
    /// v8+ is `groups` as-is (including empty; member id / epoch kept).
    /// Below v8 is always a singleton from the first group's GroupId and
    /// Topics (member id null / epoch `-1`). Empty input below v8 is a
    /// singleton with empty GroupId and null Topics (JSON defaults). Extra
    /// groups below v8 are dropped (Java reads old GroupId / Topics, not
    /// Groups). Distinct from [`Self::group_ids`], which keeps every id.
    #[must_use]
    pub fn groups(version: i16, groups: &[OffsetFetchGroup]) -> Vec<OffsetFetchGroup> {
        if version >= 8 {
            groups.to_vec()
        } else {
            let (group_id, topics) = match groups.first() {
                Some(group) => (group.group_id.clone(), group.topics.clone()),
                None => (String::new(), None),
            };
            vec![OffsetFetchGroup::new(group_id, topics)]
        }
    }

    /// Java `OffsetFetchRequest.isAllPartitionsForGroup`.
    ///
    /// First matching `GroupId` (Java `stream.filter` then `List.get(0)`).
    /// A missing group is [`Error::protocol`] (Java
    /// `IndexOutOfBoundsException`). Duplicate ids keep the first.
    /// `None` Topics is every committed partition. `Some` empty is not.
    /// Looks at `groups` as-is (the v8+ Groups field) and does not apply
    /// [`Self::groups`]. Distinct from [`OffsetFetchGroup::is_all_partitions`],
    /// which is the per-group `topics == null` check with no lookup.
    pub fn is_all_partitions_for_group(
        groups: &[OffsetFetchGroup],
        group_id: &str,
    ) -> Result<bool> {
        groups
            .iter()
            .find(|group| group.group_id == group_id)
            .map(OffsetFetchGroup::is_all_partitions)
            .ok_or_else(|| {
                Error::protocol(format!(
                    "no group named {group_id} in OffsetFetchRequest groups"
                ))
            })
    }

    /// Java `OffsetFetchRequest.partitions`.
    ///
    /// `None` Topics is `None` (every committed partition). Otherwise each
    /// `(topic, partition)` in request order. Duplicate pairs are kept
    /// (Java `ArrayList`).
    #[must_use]
    pub fn partitions(topics: Option<&[OffsetFetchTopic]>) -> Option<Vec<(String, i32)>> {
        topics.map(Self::topic_partitions)
    }

    /// Java `OffsetFetchRequest.Builder` Topics from a partition list.
    ///
    /// `None` is null Topics (every committed partition; Java
    /// `ALL_TOPIC_PARTITIONS`). `Some` groups `(topic, partition)` by
    /// name. A later entry for the same topic appends (Java
    /// `HashMap.getOrDefault` then `partitionIndexes().add`). Topic
    /// order is first-seen (Java `HashMap.values` order is unspecified).
    /// Duplicate partitions for the same pair are kept (`ArrayList`).
    /// `Some` empty is empty Topics, not all partitions.
    #[must_use]
    pub fn from_partitions<'a, I>(partitions: Option<I>) -> Option<Vec<OffsetFetchTopic>>
    where
        I: IntoIterator<Item = (&'a str, i32)>,
    {
        partitions.map(|ps| {
            let mut order: Vec<String> = Vec::new();
            let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
            for (topic, partition) in ps {
                by_topic
                    .entry(topic.to_string())
                    .or_insert_with(|| {
                        order.push(topic.to_string());
                        Vec::new()
                    })
                    .push(partition);
            }
            order
                .into_iter()
                .filter_map(|topic| {
                    by_topic
                        .remove(&topic)
                        .map(|partitions| OffsetFetchTopic { topic, partitions })
                })
                .collect()
        })
    }

    /// Java `OffsetFetchRequest.Builder.build` RequireStable check.
    ///
    /// `requireStable` below v7 with `throwOnFetchStableOffsetsUnsupported`
    /// is `UnsupportedVersionException`. Otherwise Java falls back to
    /// false (`log.trace`). Encode still omits the field below v7; this
    /// is the Builder check. v7+ returns `require_stable` as-is. Batched
    /// groups below v8 and null Topics below v2 stay on encode.
    pub fn build(
        version: i16,
        require_stable: bool,
        throw_on_fetch_stable_offsets_unsupported: bool,
    ) -> Result<bool> {
        if require_stable && version < 7 {
            if throw_on_fetch_stable_offsets_unsupported {
                return Err(Error::Unsupported(format!(
                    "Broker unexpectedly doesn't support requireStable flag on version {version}"
                )));
            }
            return Ok(false);
        }
        Ok(require_stable)
    }

    /// Java `OffsetFetchRequest.getErrorResponse`.
    ///
    /// v1 fills each request partition via [`FetchedOffset::error`]
    /// (`INVALID_OFFSET` / `NO_METADATA`); duplicate `(topic, partition)`
    /// keys are unique (Java `HashMap.put`). Null Topics is
    /// [`Error::protocol`] (Java `NullPointerException`). v2–v7 omit
    /// partitions (top-level ErrorCode). Below v8 is the
    /// [`Self::groups`] singleton (extra groups dropped). v8+ is one empty
    /// Topics group per unique GroupId; duplicate ids keep the first
    /// (Java `HashMap.put` values are the same error). Distinct from
    /// [`OffsetFetchGroup::error_results`], which keeps duplicate ids, and
    /// from [`OffsetFetchTopic::error_result`], which keeps duplicate
    /// partitions.
    pub fn error_response(
        version: i16,
        groups: &[OffsetFetchGroup],
        error_code: i16,
    ) -> Result<Vec<OffsetFetchGroupResult>> {
        if version >= 8 {
            let mut order = Vec::new();
            let mut seen = HashSet::new();
            for group in groups {
                if seen.insert(group.group_id.clone()) {
                    order.push(group.group_id.clone());
                }
            }
            return Ok(order
                .into_iter()
                .map(|group_id| OffsetFetchGroupResult::error(group_id, error_code))
                .collect());
        }
        let rewritten = Self::groups(version, groups);
        let Some(group) = rewritten.first() else {
            return Ok(Vec::new());
        };
        if version < 2 {
            let Some(topics) = group.topics.as_deref() else {
                return Err(Error::protocol(format!(
                    "null Topics for OffsetFetch v{version} getErrorResponse"
                )));
            };
            let mut order = Vec::new();
            let mut by_topic: HashMap<String, Vec<FetchedOffset>> = HashMap::new();
            let mut seen = HashSet::new();
            for topic in topics {
                for &partition in &topic.partitions {
                    if !seen.insert((topic.topic.clone(), partition)) {
                        continue;
                    }
                    by_topic
                        .entry(topic.topic.clone())
                        .or_insert_with(|| {
                            order.push(topic.topic.clone());
                            Vec::new()
                        })
                        .push(FetchedOffset::error(partition, error_code));
                }
            }
            let topics = order
                .into_iter()
                .filter_map(|topic| {
                    by_topic
                        .remove(&topic)
                        .map(|partitions| FetchedOffsetTopic { topic, partitions })
                })
                .collect();
            return Ok(vec![OffsetFetchGroupResult {
                group_id: group.group_id.clone(),
                topics,
                error_code,
            }]);
        }
        Ok(vec![OffsetFetchGroupResult::error(
            group.group_id.clone(),
            error_code,
        )])
    }

    fn topic_partitions(topics: &[OffsetFetchTopic]) -> Vec<(String, i32)> {
        let mut partitions = Vec::new();
        for topic in topics {
            for &partition in &topic.partitions {
                partitions.push((topic.topic.clone(), partition));
            }
        }
        partitions
    }
}

/// One group's OffsetFetch v8+ result (KIP-709).
///
/// [`Self::error`] is Java `OffsetFetchRequest.getErrorResponse` one group
/// on v8+ (empty Topics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchGroupResult {
    /// Consumer group id. Empty on v1–v7 (those versions have no Groups).
    pub group_id: String,
    /// Committed offsets for this group.
    pub topics: Vec<FetchedOffsetTopic>,
    /// Group-level error code (`0` is success).
    pub error_code: i16,
}

impl OffsetFetchGroupResult {
    /// Java `OffsetFetchRequest.getErrorResponse` one group on v8+.
    ///
    /// Copies `GroupId` and sets ErrorCode. Topics stay empty (Java empty
    /// `HashMap`). Throttle on the response is the JSON default (`0`).
    #[must_use]
    pub fn error(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            group_id: group_id.into(),
            topics: Vec::new(),
            error_code,
        }
    }
}

fn offset_fetch_v0_to_v7_error(version: i16, groups: &[OffsetFetchGroupResult]) -> i16 {
    if version >= 2 {
        groups.first().map(|g| g.error_code).unwrap_or(0)
    } else {
        offset_fetch_v1_top_level_error(groups.first().map(|g| g.topics.as_slice()).unwrap_or(&[]))
    }
}

fn offset_fetch_add_partition_error_counts(
    counts: &mut HashMap<i16, i32>,
    topics: &[FetchedOffsetTopic],
) {
    for topic in topics {
        for partition in &topic.partitions {
            let count = counts.entry(partition.error_code).or_insert(0);
            *count += 1;
        }
    }
}

fn offset_fetch_v1_top_level_error(topics: &[FetchedOffsetTopic]) -> i16 {
    for topic in topics {
        for partition in &topic.partitions {
            if partition.error_code != 0
                && partition.error_code != crate::error::UNKNOWN_TOPIC_OR_PARTITION
                && partition.error_code != crate::error::TOPIC_AUTHORIZATION_FAILED
            {
                return partition.error_code;
            }
        }
    }
    0
}

fn offset_fetch_partition_data_map(
    topics: &[FetchedOffsetTopic],
) -> HashMap<(String, i32), FetchedOffset> {
    let mut response_data = HashMap::new();
    for topic in topics {
        for partition in &topic.partitions {
            let _prev = response_data.insert(
                (topic.topic.clone(), partition.partition),
                partition.clone(),
            );
        }
    }
    response_data
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

/// Java `OffsetCommitRequest` helpers.
pub struct OffsetCommitRequest;

impl OffsetCommitRequest {
    /// Java `OffsetCommitRequest.offsets`.
    ///
    /// Each `(topic, partition)` maps to that committed offset. A later
    /// partition overwrites an earlier one for the same pair (Java
    /// `HashMap.put`).
    #[must_use]
    pub fn offsets(topics: &[OffsetTopic]) -> HashMap<(String, i32), i64> {
        let mut offsets = HashMap::new();
        for topic in topics {
            for partition in &topic.partitions {
                let _prev =
                    offsets.insert((topic.topic.clone(), partition.partition), partition.offset);
            }
        }
        offsets
    }

    /// Java `OffsetCommitRequest.Builder.build`.
    ///
    /// A present `group.instance.id` below v7 is
    /// `UnsupportedVersionException` (Java `!= null`, so empty is still
    /// present). Encode still omits the field on those versions; this is
    /// the Builder check.
    pub fn build(version: i16, group_instance_id: Option<&str>) -> Result<()> {
        if group_instance_id.is_some() && version < 7 {
            return Err(Error::Unsupported(format!(
                "The broker offset commit protocol version {version} does not support usage of config group.instance.id."
            )));
        }
        Ok(())
    }

    /// Java `OffsetCommitRequest.getErrorResponse`.
    ///
    /// Topics copy names and partition indexes with `error_code` (Java
    /// `getErrorResponse(data, error)`). ThrottleTimeMs is written on v3+
    /// from `throttle_time_ms`. Below v3 the field is omitted even when
    /// that value is non-zero. Decode fills `0`.
    pub fn error_response(
        buf: &mut BytesMut,
        version: i16,
        topics: &[OffsetTopic],
        error_code: i16,
        throttle_time_ms: i32,
    ) -> crate::error::Result<()> {
        encode_offset_commit_topics_response_with_throttle(
            buf,
            version,
            &OffsetTopic::error_results(topics, error_code),
            throttle_time_ms,
        )
    }
}

/// Java `OffsetCommitResponse` helpers.
pub struct OffsetCommitResponse;

impl OffsetCommitResponse {
    /// Java `OffsetCommitResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 4
    }

    /// Java `OffsetCommitResponse.errorCounts`.
    ///
    /// Counts partition-level error codes (including `NONE`).
    #[must_use]
    pub fn error_counts(topics: &[OffsetCommitResponseTopic]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        for topic in topics {
            for partition in &topic.partitions {
                let count = counts.entry(partition.error_code).or_insert(0);
                *count += 1;
            }
        }
        counts
    }

    /// Java `OffsetCommitResponse(int, Map)`.
    ///
    /// Groups `(topic, partition, error)` by name. A later entry for the
    /// same topic appends (Java `HashMap.getOrDefault` then
    /// `partitions().add`). Topic order is first-seen (Java
    /// `HashMap.values` order is unspecified). The Java map key is
    /// `TopicPartition`; grouping uses only the name. Duplicate
    /// partitions for the same pair are kept (`ArrayList`). Throttle is
    /// not part of this helper (crate encode writes the JSON default
    /// `0` on v3+; v2 has no throttle field).
    #[must_use]
    pub fn from_errors<'a, I>(response_data: I) -> Vec<OffsetCommitResponseTopic>
    where
        I: IntoIterator<Item = (&'a str, i32, i16)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<OffsetCommitResponsePartition>> = HashMap::new();
        for (topic, partition, error_code) in response_data {
            by_topic
                .entry(topic.to_string())
                .or_insert_with(|| {
                    order.push(topic.to_string());
                    Vec::new()
                })
                .push(OffsetCommitResponsePartition {
                    partition,
                    error_code,
                });
        }
        order
            .into_iter()
            .filter_map(|topic| {
                by_topic
                    .remove(&topic)
                    .map(|partitions| OffsetCommitResponseTopic { topic, partitions })
            })
            .collect()
    }

    /// Java `OffsetCommitResponse.Builder.merge`.
    ///
    /// If `current` has no topics, the result is `new_topics`. Otherwise
    /// new topics are appended and partitions of an existing topic are
    /// appended to that topic. Java does not check for overlapping
    /// partitions. Topic order is first-seen.
    #[must_use]
    pub fn merge(
        current: &[OffsetCommitResponseTopic],
        new_topics: &[OffsetCommitResponseTopic],
    ) -> Vec<OffsetCommitResponseTopic> {
        if current.is_empty() {
            return new_topics.to_vec();
        }
        let mut out = current.to_vec();
        for new_topic in new_topics {
            if let Some(i) = out.iter().position(|t| t.topic == new_topic.topic) {
                if let Some(existing) = out.get_mut(i) {
                    existing
                        .partitions
                        .extend(new_topic.partitions.iter().cloned());
                }
            } else {
                out.push(new_topic.clone());
            }
        }
        out
    }
}

/// Encode OffsetCommit v2–v9.
///
/// Kafka 4.0 JSON: `validVersions: "2-9"`, `flexibleVersions: "8+"`.
/// v2–v4 send `retention_time_ms` after MemberId. v5 omits retention even
/// when the body has a non-default value; decode fills
/// [`DEFAULT_RETENTION_TIME`]. v6 CommittedLeaderEpoch. v7
/// GroupInstanceId. Below v7 GroupInstanceId is omitted even when the
/// body has an instance id; decode fills `None`. v8 flexible. v9 matches
/// v8. This crate speaks 2–9. v0–v1 and v10+ are not spoken.
#[expect(
    clippy::too_many_arguments,
    reason = "OffsetCommit request body needs version, group, generation, member, instance, retention, and topics together"
)]
pub fn encode_offset_commit_request(
    buf: &mut BytesMut,
    version: i16,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
    retention_time_ms: i64,
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
        buf.put_i64(retention_time_ms);
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

/// Decode OffsetCommit: `(group_id, member_id, topics, retention_time_ms,
/// group_instance_id)`.
///
/// Decode below v6 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] for
/// omitted `CommittedLeaderEpoch`. RetentionTimeMs is omitted outside
/// v2–v4; decode fills [`DEFAULT_RETENTION_TIME`]. Below v7
/// GroupInstanceId is omitted; decode fills `None`.
#[expect(
    clippy::type_complexity,
    reason = "OffsetCommit decode returns group, member, topics, retention, and instance together"
)]
pub fn decode_offset_commit_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, Vec<OffsetTopic>, i64, Option<String>)> {
    let flexible = offset_commit_flexible(version)?;
    let group = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _gen = buf::get_i32(buf)?;
    let member = buf::get_string(buf, flexible)?.unwrap_or_default();
    let inst = if version >= 7 {
        buf::get_string(buf, flexible)?
    } else {
        None
    };
    let retention_time_ms = if (2..=4).contains(&version) {
        buf::get_i64(buf)?
    } else {
        DEFAULT_RETENTION_TIME
    };
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
    Ok((group, member, topics, retention_time_ms, inst))
}

/// Encode OffsetCommit v2–v9. Throttle is `0` on v3+.
///
/// Applies `error` on every request partition via
/// [`OffsetTopic::error_results`].
pub fn encode_offset_commit_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetTopic],
    error: i16,
) -> crate::error::Result<()> {
    encode_offset_commit_topics_response(buf, version, &OffsetTopic::error_results(topics, error))
}

/// Encode OffsetCommit v2–v9 from response Topics.
///
/// Throttle is the JSON default (`0`) on v3+. Nested body is
/// PartitionIndex + ErrorCode.
pub fn encode_offset_commit_topics_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetCommitResponseTopic],
) -> crate::error::Result<()> {
    encode_offset_commit_topics_response_with_throttle(buf, version, topics, 0)
}

/// Encode OffsetCommit v2–v9 with ThrottleTimeMs.
///
/// Below v3 ThrottleTimeMs is omitted even when the body has a non-zero
/// value. Decode fills `0`. Nested body is PartitionIndex + ErrorCode.
/// v8+ is flexible.
pub fn encode_offset_commit_topics_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    topics: &[OffsetCommitResponseTopic],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = offset_commit_flexible(version)?;
    if version >= 3 {
        buf.put_i32(throttle_time_ms);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
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
    let (topics, ..) = decode_offset_commit_topics_response(buf, version)?;
    let mut first_err = 0i16;
    for t in &topics {
        for p in &t.partitions {
            if first_err == 0 && p.error_code != 0 {
                first_err = p.error_code;
            }
        }
    }
    Ok(first_err)
}

/// Decode OffsetCommit: `(topics, throttle_time_ms)`.
///
/// Below v3 ThrottleTimeMs is omitted; decode fills `0`.
pub fn decode_offset_commit_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<OffsetCommitResponseTopic>, i32)> {
    let flexible = offset_commit_flexible(version)?;
    let throttle_time_ms = if version >= 3 { buf::get_i32(buf)? } else { 0 };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(OffsetCommitResponsePartition {
                partition,
                error_code,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(OffsetCommitResponseTopic { topic, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((topics, throttle_time_ms))
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
/// is v2+; v1 is Java `UnsupportedVersionException`. For several groups
/// on v8+, use [`encode_offset_fetch_groups_request`].
///
/// Java `OffsetFetchRequest.Builder.build` rejects null Topics below v2.
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
        return Err(Error::Unsupported(format!(
            "The broker only supports OffsetFetchRequest v{version}, but we need v2 or newer to request all topic partitions."
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
/// v1–v7 return an error. More than one group below v8 is Java
/// `NoBatchedOffsetFetchRequestException`. A single group below v8
/// is a protocol error (`does not support Groups`; use
/// [`encode_offset_fetch_request`]). Null Topics per group is allowed.
/// Empty `groups` writes Groups length 0.
pub fn encode_offset_fetch_groups_request(
    buf: &mut BytesMut,
    version: i16,
    groups: &[OffsetFetchGroup],
    require_stable: bool,
) -> crate::error::Result<()> {
    let _flexible = offset_fetch_flexible(version)?;
    if version < 8 {
        if groups.len() > 1 {
            return Err(Error::Unsupported(format!(
                "Broker does not support batching groups for fetch offset request on version {version}"
            )));
        }
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
/// v1–v7 write the first group's Topics and ErrorCode. More than one
/// group below v8 is Java `UnsupportedVersionException`. Empty `groups`
/// writes an empty Topics array (v1–v7) or Groups length 0 (v8+).
/// Throttle is `0` on v3+.
pub fn encode_offset_fetch_groups_response(
    buf: &mut BytesMut,
    version: i16,
    groups: &[OffsetFetchGroupResult],
) -> crate::error::Result<()> {
    encode_offset_fetch_groups_response_with_throttle(buf, version, groups, 0)
}

/// Encode OffsetFetch v1–v9 with ThrottleTimeMs.
///
/// Below v3 ThrottleTimeMs is omitted even when the body has a non-zero
/// value. Decode fills `0`. v1–v7 write the first group's Topics and
/// ErrorCode. More than one group below v8 is Java
/// `UnsupportedVersionException`. v6+ is flexible.
pub fn encode_offset_fetch_groups_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    groups: &[OffsetFetchGroupResult],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = offset_fetch_flexible(version)?;
    if version < 8 && groups.len() > 1 {
        return Err(Error::Unsupported(format!(
            "Version {version} of OffsetFetchResponse only supports one group."
        )));
    }
    if version >= 3 {
        buf.put_i32(throttle_time_ms);
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
    let (groups, ..) = decode_offset_fetch_groups_response(buf, version)?;
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

/// Decode OffsetFetch v1–v9: `(groups, throttle_time_ms)`.
///
/// Does not fail on a non-zero group ErrorCode; callers decide. Below v3
/// ThrottleTimeMs is omitted; decode fills `0`. v1–v7 yield one group
/// with an empty `group_id`.
pub fn decode_offset_fetch_groups_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<OffsetFetchGroupResult>, i32)> {
    let flexible = offset_fetch_flexible(version)?;
    let throttle_time_ms = if version >= 3 { buf::get_i32(buf)? } else { 0 };
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
    Ok((groups, throttle_time_ms))
}

/// Topic + partitions for OffsetDelete (api 47) v0.
///
/// [`Self::error_result`] is Java `OffsetDeleteResponse.Builder.addPartitions`
/// one topic. Official Java `OffsetDeleteRequest.getErrorResponse` writes
/// only the top-level ErrorCode (empty Topics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to delete.
    pub partitions: Vec<i32>,
}

impl OffsetDeleteTopic {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(topic: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            topic: topic.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Partition indexes to delete.
    #[must_use]
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    /// Java `OffsetDeleteResponse.Builder.addPartitions` one topic.
    ///
    /// Each partition is [`OffsetDeleteResult::new`]. Throttle on the
    /// response is the JSON default (`0`). Official Java
    /// `OffsetDeleteRequest.getErrorResponse` does not fill Topics.
    #[must_use]
    pub fn error_result(&self, error_code: i16) -> Vec<OffsetDeleteResult> {
        self.partitions
            .iter()
            .map(|&partition| OffsetDeleteResult::new(self.topic.clone(), partition, error_code))
            .collect()
    }
}

/// One partition result from OffsetDelete (api 47) v0.
///
/// [`Self::new`] is Java `OffsetDeleteResponse.Builder.addPartition`
/// partition body (`PartitionIndex` / `ErrorCode`) plus the topic name
/// (this crate stores a flat list).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteResult {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl OffsetDeleteResult {
    /// Java `OffsetDeleteResponse.Builder.addPartition` partition body.
    #[must_use]
    pub fn new(topic: impl Into<String>, partition: i32, error_code: i16) -> Self {
        Self {
            topic: topic.into(),
            partition,
            error_code,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Partition index.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }
}

/// Java `OffsetDeleteRequest` helpers.
pub struct OffsetDeleteRequest;

impl OffsetDeleteRequest {
    /// Java `OffsetDeleteRequest.getErrorResponse`.
    ///
    /// Writes only the top-level ErrorCode. Topics stay empty (request
    /// partitions are not copied). Throttle is the JSON default (`0`);
    /// official Java `getErrorResponse` sets `throttleTimeMs` from the
    /// argument. ErrorCode is encoded before throttle. Crate convenience
    /// encode still writes `0`.
    pub fn error_response(buf: &mut BytesMut, error_code: i16) -> crate::error::Result<()> {
        encode_offset_delete_response(buf, error_code, &[])
    }
}

/// Java `OffsetDeleteResponse` helpers.
///
/// [`Self::merge`] is Java `OffsetDeleteResponse.Builder.merge`.
pub struct OffsetDeleteResponse;

fn offset_delete_group_results(results: &[OffsetDeleteResult]) -> Vec<(String, Vec<(i32, i16)>)> {
    let mut groups: Vec<(String, Vec<(i32, i16)>)> = Vec::new();
    for r in results {
        if let Some(i) = groups.iter().position(|(topic, _)| topic == &r.topic) {
            if let Some((_, parts)) = groups.get_mut(i) {
                parts.push((r.partition, r.error_code));
            }
        } else {
            groups.push((r.topic.clone(), vec![(r.partition, r.error_code)]));
        }
    }
    groups
}

fn offset_delete_flatten_results(
    groups: Vec<(String, Vec<(i32, i16)>)>,
) -> Vec<OffsetDeleteResult> {
    let mut out = Vec::new();
    for (topic, parts) in groups {
        for (partition, error_code) in parts {
            out.push(OffsetDeleteResult::new(
                topic.clone(),
                partition,
                error_code,
            ));
        }
    }
    out
}

impl OffsetDeleteResponse {
    /// Java `OffsetDeleteResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 0
    }

    /// Java `OffsetDeleteResponse.errorCounts`.
    ///
    /// Counts the top-level `errorCode` (including `NONE`) plus each
    /// partition-level code (including `NONE`).
    #[must_use]
    pub fn error_counts(error_code: i16, results: &[OffsetDeleteResult]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        let count = counts.entry(error_code).or_insert(0);
        *count += 1;
        for result in results {
            let count = counts.entry(result.error_code).or_insert(0);
            *count += 1;
        }
        counts
    }

    /// Java `OffsetDeleteResponse.Builder.merge`.
    ///
    /// If `new_error` is not `NONE`, the result is `new_results` (current
    /// Topics are discarded). If `current` has no topics, the result is
    /// `new_results`. Otherwise new topics are appended and partitions of
    /// an existing topic are appended to that topic. Java does not check
    /// for overlapping partitions. Topic order is first-seen.
    #[must_use]
    pub fn merge(
        current_error: i16,
        current: &[OffsetDeleteResult],
        new_error: i16,
        new_results: &[OffsetDeleteResult],
    ) -> (i16, Vec<OffsetDeleteResult>) {
        if new_error != crate::error::NONE || current.is_empty() {
            return (new_error, new_results.to_vec());
        }
        let mut groups = offset_delete_group_results(current);
        for (name, parts) in offset_delete_group_results(new_results) {
            if let Some(i) = groups.iter().position(|(topic, _)| topic == &name) {
                if let Some((_, existing)) = groups.get_mut(i) {
                    existing.extend(parts);
                }
            } else {
                groups.push((name, parts));
            }
        }
        (current_error, offset_delete_flatten_results(groups))
    }
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
///
/// ThrottleTimeMs is the JSON default (`0`) on the spoken version
/// (JSON `0+`, second field). ErrorCode is first.
pub fn encode_offset_delete_response(
    buf: &mut BytesMut,
    error_code: i16,
    results: &[OffsetDeleteResult],
) -> crate::error::Result<()> {
    encode_offset_delete_response_with_throttle(buf, error_code, results, 0)
}

/// Encode OffsetDelete v0 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+` (INT32, ignorable) but **after** ErrorCode
/// (not first). Classic only (`flexibleVersions: "none"`). Kafka 4.0
/// `validVersions` is `"0"`. This crate speaks 0. v1+ is not spoken.
/// Official Java `getErrorResponse` sets `throttleTimeMs` from the
/// argument. Empty-Topics only one version. Top-level ErrorCode is at
/// bytes 0–1 (throttle occupies bytes 2–5).
pub fn encode_offset_delete_response_with_throttle(
    buf: &mut BytesMut,
    error_code: i16,
    results: &[OffsetDeleteResult],
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    buf.put_i16(error_code);
    buf.put_i32(throttle_time_ms);
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

/// Decode OffsetDelete: `(error_code, results, throttle_time_ms)`.
///
/// ThrottleTimeMs is JSON `0+` (always on the wire) after ErrorCode.
/// Top-level ErrorCode is at bytes 0–1.
pub fn decode_offset_delete_response<B: Buf>(
    buf: &mut B,
) -> Result<(i16, Vec<OffsetDeleteResult>, i32)> {
    let error_code = buf::get_i16(buf)?;
    let throttle_time_ms = buf::get_i32(buf)?;
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
    Ok((error_code, out, throttle_time_ms))
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
    let mut partitions = Vec::new();
    for (topic, parts) in topics {
        for partition in parts {
            partitions.push((topic.clone(), *partition));
        }
    }
    ConsumerProtocol::serialize_assignment(&ConsumerProtocolAssignment::new(partitions))
}

/// Group `(topic, partition)` pairs by topic, preserving first-seen order.
pub fn encode_tp_assignment(parts: &[(String, i32)]) -> Result<Vec<u8>> {
    ConsumerProtocol::serialize_assignment(&ConsumerProtocolAssignment::new(parts.to_vec()))
}

/// Decode ConsumerProtocol assignment: `(topic, partitions)` per topic.
pub fn decode_assignment(bytes: &[u8]) -> Result<Vec<(String, Vec<i32>)>> {
    Ok(group_assigned_partitions(
        ConsumerProtocol::deserialize_assignment(bytes)?.partitions(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        assert!(!FindCoordinatorResponse::should_client_throttle(1));
        assert!(FindCoordinatorResponse::should_client_throttle(2));
    }

    #[test]
    fn find_coordinator_response_error_counts_matches_java() {
        assert_eq!(
            FindCoordinatorResponse::error_counts(&[]),
            HashMap::from([(0, 1)])
        );
        assert_eq!(
            FindCoordinatorResponse::error_counts(&[CoordinatorResult::error(0)]),
            HashMap::from([(0, 1)])
        );
        let batched = FindCoordinatorResponse::error_counts(&[
            CoordinatorResult::error_for_key(0, "g1"),
            CoordinatorResult::error_for_key(crate::error::NOT_COORDINATOR, "g2"),
            CoordinatorResult::error_for_key(0, "g3"),
        ]);
        assert_eq!(
            batched,
            HashMap::from([(0, 2), (crate::error::NOT_COORDINATOR, 1)])
        );
        let same = FindCoordinatorResponse::error_counts(&[
            CoordinatorResult::error_for_key(crate::error::NOT_COORDINATOR, "a"),
            CoordinatorResult::error_for_key(crate::error::NOT_COORDINATOR, "b"),
        ]);
        assert_eq!(same, HashMap::from([(crate::error::NOT_COORDINATOR, 2)]));
        assert_eq!(
            FindCoordinatorResponse::error_counts(&[CoordinatorResult::error(
                crate::error::NOT_COORDINATOR
            )]),
            HashMap::from([(crate::error::NOT_COORDINATOR, 1)])
        );
    }

    #[test]
    fn find_coordinator_response_coordinator_by_key_matches_java() {
        let v3 = CoordinatorResult {
            key: String::new(),
            node_id: 1,
            host: "localhost".into(),
            port: 9092,
            error_code: 0,
            error_message: None,
        };
        let stuffed =
            FindCoordinatorResponse::coordinator_by_key(3, std::slice::from_ref(&v3), "g").unwrap();
        assert_eq!(stuffed.key, "g");
        assert_eq!(stuffed.node_id, 1);
        assert_eq!(stuffed.host, "localhost");
        assert_eq!(stuffed.port, 9092);
        assert_eq!(stuffed.error_code, 0);
        assert_eq!(
            FindCoordinatorResponse::coordinator_by_key(3, &[], "g").unwrap(),
            CoordinatorResult {
                key: "g".into(),
                node_id: 0,
                host: String::new(),
                port: 0,
                error_code: 0,
                error_message: None,
            }
        );

        let batched = [
            CoordinatorResult::error_for_key(0, "g1"),
            CoordinatorResult {
                key: "g2".into(),
                node_id: 2,
                host: "broker".into(),
                port: 9093,
                error_code: crate::error::NOT_COORDINATOR,
                error_message: Some("no".into()),
            },
        ];
        assert_eq!(
            FindCoordinatorResponse::coordinator_by_key(4, &batched, "g2").unwrap(),
            batched[1]
        );
        assert!(FindCoordinatorResponse::coordinator_by_key(4, &batched, "g3").is_none());
        let empty_v4 = FindCoordinatorResponse::coordinator_by_key(4, &[], "g").unwrap();
        assert_eq!(empty_v4.key, "g");
        assert_eq!(empty_v4.node_id, 0);
        assert_eq!(empty_v4.port, 0);
        assert_eq!(empty_v4.error_code, 0);
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
        assert!(!OffsetCommitResponse::should_client_throttle(3));
        assert!(OffsetCommitResponse::should_client_throttle(4));
        assert!(!SyncGroupResponse::should_client_throttle(1));
        assert!(SyncGroupResponse::should_client_throttle(2));
        assert!(!HeartbeatResponse::should_client_throttle(1));
        assert!(HeartbeatResponse::should_client_throttle(2));
    }

    #[test]
    fn sync_group_are_mandatory_protocol_type_and_name_present_matches_java() {
        assert!(SyncGroupRequest::are_mandatory_protocol_type_and_name_present(4, None, None));
        assert!(
            SyncGroupRequest::are_mandatory_protocol_type_and_name_present(4, None, Some("range"))
        );
        assert!(
            SyncGroupRequest::are_mandatory_protocol_type_and_name_present(
                5,
                Some("consumer"),
                Some("range")
            )
        );
        assert!(
            SyncGroupRequest::are_mandatory_protocol_type_and_name_present(5, Some(""), Some(""))
        );
        assert!(
            !SyncGroupRequest::are_mandatory_protocol_type_and_name_present(5, None, Some("range"))
        );
        assert!(
            !SyncGroupRequest::are_mandatory_protocol_type_and_name_present(
                5,
                Some("consumer"),
                None
            )
        );
        assert!(!SyncGroupRequest::are_mandatory_protocol_type_and_name_present(5, None, None));
    }

    #[test]
    fn sync_group_request_group_assignments_matches_java() {
        // Java SyncGroupRequest.groupAssignments: HashMap.put of
        // memberId → assignment bytes. A later member overwrites.
        assert!(SyncGroupRequest::group_assignments(&[]).is_empty());
        let one = vec![("m1".into(), vec![1, 2, 3])];
        assert_eq!(
            SyncGroupRequest::group_assignments(&one),
            HashMap::from([("m1".into(), vec![1, 2, 3])])
        );
        let overwrite = vec![
            ("m1".into(), vec![1]),
            ("m2".into(), vec![9]),
            ("m1".into(), vec![7, 8]),
        ];
        assert_eq!(
            SyncGroupRequest::group_assignments(&overwrite),
            HashMap::from([("m1".into(), vec![7, 8]), ("m2".into(), vec![9])])
        );
        let two = vec![("a".into(), vec![1]), ("b".into(), vec![2, 3])];
        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 0, &sync_req(&two)).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, _mid, decoded, ..) = decode_sync_group_request(&mut cur, 0).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(
            SyncGroupRequest::group_assignments(&decoded),
            HashMap::from([("a".into(), vec![1]), ("b".into(), vec![2, 3])])
        );
        assert!(
            cur.is_empty(),
            "SyncGroup v0 groupAssignments leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_sync_group_request(&mut buf, 5, &sync_req(&two)).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, _mid, decoded, ..) = decode_sync_group_request(&mut cur, 5).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(
            SyncGroupRequest::group_assignments(&decoded),
            SyncGroupRequest::group_assignments(&two)
        );
        assert!(
            cur.is_empty(),
            "SyncGroup v5 groupAssignments leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn sync_group_request_build_matches_java() {
        SyncGroupRequest::build(2, None).unwrap();
        SyncGroupRequest::build(3, None).unwrap();
        SyncGroupRequest::build(3, Some("i")).unwrap();
        SyncGroupRequest::build(3, Some("")).unwrap();
        SyncGroupRequest::build(5, Some("i")).unwrap();
        let v2 = SyncGroupRequest::build(2, Some("i")).unwrap_err();
        assert!(
            matches!(v2, Error::Unsupported(_)),
            "v2 with group.instance.id is Java UnsupportedVersionException, got {v2}"
        );
        assert!(
            v2.to_string()
                .contains("does not support usage of config group.instance.id"),
            "got {v2}"
        );
        let empty = SyncGroupRequest::build(0, Some("")).unwrap_err();
        assert!(
            matches!(empty, Error::Unsupported(_)),
            "empty group.instance.id is still present (Java != null), got {empty}"
        );
        let empty_assign: [(String, Vec<u8>); 0] = [];
        let ignored = SyncGroupRequest {
            group_id: "g",
            generation_id: 7,
            member_id: "m1",
            group_instance_id: Some("ignored-on-v0"),
            protocol_type: "consumer",
            protocol_name: "range",
            assignments: &empty_assign,
        };
        encode_sync_group_request(&mut BytesMut::new(), 0, &ignored).unwrap();
        assert!(
            SyncGroupRequest::build(0, Some("ignored-on-v0")).is_err(),
            "encode omits group.instance.id below v3; Builder.build rejects it"
        );

        let two = vec![("a".into(), vec![1]), ("b".into(), vec![2, 3])];
        for version in [3_i16, 5] {
            SyncGroupRequest::build(version, Some("i")).unwrap();
            let req = SyncGroupRequest {
                group_id: "g",
                generation_id: 7,
                member_id: "m1",
                group_instance_id: Some("i"),
                protocol_type: "consumer",
                protocol_name: "range",
                assignments: &two,
            };
            let mut buf = BytesMut::new();
            encode_sync_group_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let decoded = decode_sync_group_request(&mut cur, version).unwrap().2;
            assert_eq!(decoded, two);
            assert!(
                cur.is_empty(),
                "SyncGroup v{version} Builder.build leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        for version in [0_i16, 2, 3, 5] {
            SyncGroupRequest::build(version, None).unwrap();
            let mut buf = BytesMut::new();
            encode_sync_group_request(&mut buf, version, &sync_req(&two)).unwrap();
            let mut cur = buf.as_ref();
            let decoded = decode_sync_group_request(&mut cur, version).unwrap().2;
            assert_eq!(decoded, two);
            assert!(
                cur.is_empty(),
                "SyncGroup v{version} Builder.build empty leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
    }

    #[test]
    fn offset_commit_response_error_counts_matches_java() {
        assert!(OffsetCommitResponse::error_counts(&[]).is_empty());
        let counts = OffsetCommitResponse::error_counts(&[
            OffsetCommitResponseTopic {
                topic: "ok".into(),
                partitions: vec![
                    OffsetCommitResponsePartition::error(0, 0),
                    OffsetCommitResponsePartition::error(1, crate::error::NOT_LEADER_OR_FOLLOWER),
                ],
            },
            OffsetCommitResponseTopic {
                topic: "missing".into(),
                partitions: vec![OffsetCommitResponsePartition::error(
                    0,
                    crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                )],
            },
            OffsetCommitResponseTopic {
                topic: "ok2".into(),
                partitions: vec![OffsetCommitResponsePartition::error(0, 0)],
            },
        ]);
        assert_eq!(
            counts,
            HashMap::from([
                (0, 2),
                (crate::error::NOT_LEADER_OR_FOLLOWER, 1),
                (crate::error::UNKNOWN_TOPIC_OR_PARTITION, 1),
            ])
        );
    }

    #[test]
    fn offset_commit_response_from_errors_matches_java() {
        // Java OffsetCommitResponse(int, Map): HashMap.getOrDefault by
        // topic name, then partitions().add. Empty map is empty. A later
        // entry for the same name appends even when another topic sits
        // between. Duplicate partitions for the same pair are kept
        // (ArrayList).
        assert!(
            OffsetCommitResponse::from_errors(std::iter::empty::<(&str, i32, i16)>()).is_empty()
        );
        let grouped = OffsetCommitResponse::from_errors([
            ("a", 0, crate::error::UNKNOWN_TOPIC_OR_PARTITION),
            ("b", 0, crate::error::NOT_LEADER_OR_FOLLOWER),
            ("a", 1, 0i16),
        ]);
        assert_eq!(
            grouped,
            vec![
                OffsetCommitResponseTopic {
                    topic: "a".into(),
                    partitions: vec![
                        OffsetCommitResponsePartition::error(
                            0,
                            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                        ),
                        OffsetCommitResponsePartition::error(1, 0),
                    ],
                },
                OffsetCommitResponseTopic {
                    topic: "b".into(),
                    partitions: vec![OffsetCommitResponsePartition::error(
                        0,
                        crate::error::NOT_LEADER_OR_FOLLOWER,
                    )],
                },
            ]
        );
        let dup = OffsetCommitResponse::from_errors([
            ("t", 0, 0i16),
            ("t", 0, crate::error::NOT_LEADER_OR_FOLLOWER),
        ]);
        assert_eq!(
            dup,
            vec![OffsetCommitResponseTopic {
                topic: "t".into(),
                partitions: vec![
                    OffsetCommitResponsePartition::error(0, 0),
                    OffsetCommitResponsePartition::error(0, crate::error::NOT_LEADER_OR_FOLLOWER),
                ],
            }]
        );
        let mut buf = BytesMut::new();
        encode_offset_commit_topics_response(&mut buf, 2, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_commit_topics_response(&mut cur, 2).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "OffsetCommit v2 from_errors leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_commit_topics_response(&mut buf, 8, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_commit_topics_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "OffsetCommit v8 from_errors leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_commit_response_merge_matches_java() {
        // Java OffsetCommitResponse.Builder.merge: replace when current
        // Topics are empty. Otherwise append topics / partitions (no
        // overlap check). There is no top-level ErrorCode replacement.
        let t1 = OffsetCommitResponseTopic {
            topic: "t1".into(),
            partitions: vec![OffsetCommitResponsePartition::error(0, 0)],
        };
        let t1_extra = OffsetCommitResponseTopic {
            topic: "t1".into(),
            partitions: vec![OffsetCommitResponsePartition::error(
                1,
                crate::error::NOT_LEADER_OR_FOLLOWER,
            )],
        };
        let t2 = OffsetCommitResponseTopic {
            topic: "t2".into(),
            partitions: vec![OffsetCommitResponsePartition::error(
                0,
                crate::error::UNKNOWN_TOPIC_OR_PARTITION,
            )],
        };
        let current = vec![t1.clone()];
        let merged_same = OffsetCommitResponse::merge(&current, std::slice::from_ref(&t1_extra));
        assert_eq!(
            merged_same,
            vec![OffsetCommitResponseTopic {
                topic: "t1".into(),
                partitions: vec![
                    OffsetCommitResponsePartition::error(0, 0),
                    OffsetCommitResponsePartition::error(1, crate::error::NOT_LEADER_OR_FOLLOWER),
                ],
            }]
        );
        for version in [2_i16, 3, 8] {
            let mut got = BytesMut::new();
            encode_offset_commit_topics_response(&mut got, version, &merged_same).unwrap();
            let mut cur = &got[..];
            let (decoded, ..) = decode_offset_commit_topics_response(&mut cur, version).unwrap();
            assert_eq!(decoded, merged_same, "v{version} same-topic merge decode");
            assert!(
                cur.is_empty(),
                "OffsetCommit v{version} merge same-topic leftover-empty; leftover {} bytes",
                cur.len()
            );
        }

        let merged_new = OffsetCommitResponse::merge(&current, std::slice::from_ref(&t2));
        assert_eq!(merged_new, vec![t1.clone(), t2.clone()]);
        let mut got = BytesMut::new();
        encode_offset_commit_topics_response(&mut got, 8, &merged_new).unwrap();
        let mut cur = &got[..];
        let (decoded, ..) = decode_offset_commit_topics_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, merged_new);
        assert!(
            cur.is_empty(),
            "OffsetCommit v8 merge new-topic leftover-empty; leftover {} bytes",
            cur.len()
        );

        let from_empty = OffsetCommitResponse::merge(&[], &current);
        assert_eq!(from_empty, current, "empty current Topics takes new Topics");
        got.clear();
        encode_offset_commit_topics_response(&mut got, 2, &from_empty).unwrap();
        let mut cur = &got[..];
        let (decoded, ..) = decode_offset_commit_topics_response(&mut cur, 2).unwrap();
        assert_eq!(decoded, current);
        assert!(
            cur.is_empty(),
            "OffsetCommit v2 merge empty-current leftover-empty; leftover {} bytes",
            cur.len()
        );

        let empty_both = OffsetCommitResponse::merge(&[], &[]);
        assert!(empty_both.is_empty());
        got.clear();
        encode_offset_commit_topics_response(&mut got, 3, &empty_both).unwrap();
        let mut cur = &got[..];
        let (decoded, ..) = decode_offset_commit_topics_response(&mut cur, 3).unwrap();
        assert!(decoded.is_empty());
        assert!(
            cur.is_empty(),
            "OffsetCommit v3 merge empty leftover-empty; leftover {} bytes",
            cur.len()
        );

        let grouped = OffsetCommitResponse::merge(&current, &[t2, t1_extra]);
        assert_eq!(
            grouped,
            vec![
                OffsetCommitResponseTopic {
                    topic: "t1".into(),
                    partitions: vec![
                        OffsetCommitResponsePartition::error(0, 0),
                        OffsetCommitResponsePartition::error(
                            1,
                            crate::error::NOT_LEADER_OR_FOLLOWER
                        ),
                    ],
                },
                OffsetCommitResponseTopic {
                    topic: "t2".into(),
                    partitions: vec![OffsetCommitResponsePartition::error(
                        0,
                        crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                    )],
                },
            ]
        );
        got.clear();
        encode_offset_commit_topics_response(&mut got, 8, &grouped).unwrap();
        let mut cur = &got[..];
        let (decoded, ..) = decode_offset_commit_topics_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "OffsetCommit v8 merge grouped leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_delete_response_error_counts_matches_java() {
        assert_eq!(
            OffsetDeleteResponse::error_counts(0, &[]),
            HashMap::from([(0, 1)])
        );
        assert!(OffsetDeleteResponse::should_client_throttle(0));
        let counts = OffsetDeleteResponse::error_counts(
            0,
            &[
                OffsetDeleteResult::new("ok", 0, 0),
                OffsetDeleteResult::new("ok", 1, crate::error::GROUP_SUBSCRIBED_TO_TOPIC),
                OffsetDeleteResult::new("missing", 0, crate::error::UNKNOWN_TOPIC_OR_PARTITION),
                OffsetDeleteResult::new("ok2", 0, 0),
            ],
        );
        assert_eq!(
            counts,
            HashMap::from([
                (0, 3),
                (crate::error::GROUP_SUBSCRIBED_TO_TOPIC, 1),
                (crate::error::UNKNOWN_TOPIC_OR_PARTITION, 1),
            ])
        );
        let top = OffsetDeleteResponse::error_counts(crate::error::NOT_COORDINATOR, &[]);
        assert_eq!(top, HashMap::from([(crate::error::NOT_COORDINATOR, 1)]));
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
        assert!(!JoinGroupResponse::should_client_throttle(2));
        assert!(JoinGroupResponse::should_client_throttle(3));
        let keep = "a".repeat(255);
        assert_eq!(JoinGroupRequest::maybe_truncate_reason(&keep), keep);
        let long = "a".repeat(256);
        assert_eq!(JoinGroupRequest::maybe_truncate_reason(&long).len(), 255);
        assert_eq!(JoinGroupRequest::join_reason(None), "not provided");
        assert_eq!(JoinGroupRequest::join_reason(Some("")), "not provided");
        assert_eq!(
            JoinGroupRequest::join_reason(Some("rebalance enforced by user")),
            "rebalance enforced by user"
        );
        assert_eq!(
            JoinGroupRequest::join_reason(Some(" ")),
            " ",
            "Java isEmpty, not isBlank"
        );
        assert!(JoinGroupRequest::validate_group_instance_id("worker-1").is_ok());
        assert!(JoinGroupRequest::validate_group_instance_id(&"a".repeat(249)).is_ok());
        let empty = JoinGroupRequest::validate_group_instance_id("").unwrap_err();
        assert!(empty
            .to_string()
            .contains("Group instance id is invalid: the empty string is not allowed"));
        let dot = JoinGroupRequest::validate_group_instance_id(".").unwrap_err();
        assert!(dot
            .to_string()
            .contains("Group instance id is invalid: '.' is not allowed"));
        let dots = JoinGroupRequest::validate_group_instance_id("..").unwrap_err();
        assert!(dots
            .to_string()
            .contains("Group instance id is invalid: '..' is not allowed"));
        let long_id = "a".repeat(250);
        let long = JoinGroupRequest::validate_group_instance_id(&long_id).unwrap_err();
        assert!(long.to_string().contains(&format!(
            "Group instance id is invalid: the length of '{long_id}' is longer than the max allowed length 249"
        )));
        let bad = JoinGroupRequest::validate_group_instance_id("bad id").unwrap_err();
        assert!(bad.to_string().contains(
            "Group instance id is invalid: 'bad id' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
        ));
        Topic::validate("orders").unwrap();
        Topic::validate(&"a".repeat(249)).unwrap();
        let topic_empty = Topic::validate("").unwrap_err().to_string();
        assert!(
            topic_empty.contains("Topic name is invalid: the empty string is not allowed"),
            "{topic_empty}"
        );
        let topic_dot = Topic::validate(".").unwrap_err().to_string();
        assert!(
            topic_dot.contains("Topic name is invalid: '.' is not allowed"),
            "{topic_dot}"
        );
        let topic_dots = Topic::validate("..").unwrap_err().to_string();
        assert!(
            topic_dots.contains("Topic name is invalid: '..' is not allowed"),
            "{topic_dots}"
        );
        let long_topic = "a".repeat(250);
        let topic_long = Topic::validate(&long_topic).unwrap_err().to_string();
        assert!(
            topic_long.contains(&format!(
                "Topic name is invalid: the length of '{long_topic}' is longer than the max allowed length 249"
            )),
            "{topic_long}"
        );
        let topic_bad = Topic::validate("bad id").unwrap_err().to_string();
        assert!(
            topic_bad.contains(
                "Topic name is invalid: 'bad id' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
            ),
            "{topic_bad}"
        );
        assert_eq!(Topic::MAX_NAME_LENGTH, 249);
        assert_eq!(Topic::GROUP_METADATA_TOPIC_NAME, "__consumer_offsets");
        assert_eq!(Topic::TRANSACTION_STATE_TOPIC_NAME, "__transaction_state");
        assert_eq!(Topic::SHARE_GROUP_STATE_TOPIC_NAME, "__share_group_state");
        assert_eq!(Topic::CLUSTER_METADATA_TOPIC_NAME, "__cluster_metadata");
        assert_eq!(Topic::LEGAL_CHARS, "[a-zA-Z0-9._-]");
        assert!(Topic::is_valid("orders"));
        assert!(Topic::is_valid(&"a".repeat(249)));
        assert!(!Topic::is_valid(""));
        assert!(!Topic::is_valid("."));
        assert!(!Topic::is_valid(".."));
        assert!(!Topic::is_valid("bad id"));
        assert!(Topic::is_internal(Topic::GROUP_METADATA_TOPIC_NAME));
        assert!(Topic::is_internal(Topic::TRANSACTION_STATE_TOPIC_NAME));
        assert!(Topic::is_internal(Topic::SHARE_GROUP_STATE_TOPIC_NAME));
        assert!(!Topic::is_internal(Topic::CLUSTER_METADATA_TOPIC_NAME));
        assert!(!Topic::is_internal("orders"));
        assert!(Topic::has_collision_chars("foo.bar"));
        assert!(Topic::has_collision_chars("foo_bar"));
        assert!(!Topic::has_collision_chars("foobar"));
        assert_eq!(Topic::unify_collision_chars("foo.bar"), "foo_bar");
        assert!(Topic::has_collision("foo.bar", "foo_bar"));
        assert!(!Topic::has_collision("foo", "bar"));
    }

    #[test]
    fn fetched_offset_partition_data_matches_java() {
        assert_eq!(FetchedOffset::INVALID_OFFSET, -1);
        assert_eq!(FetchedOffset::NO_METADATA, "");
        assert!(!OffsetFetchResponse::should_client_throttle(3));
        assert!(OffsetFetchResponse::should_client_throttle(4));
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
        let unknown = FetchedOffset::unknown_partition(1);
        assert_eq!(
            unknown,
            FetchedOffset::new(
                1,
                FetchedOffset::INVALID_OFFSET,
                crate::error::UNKNOWN_TOPIC_OR_PARTITION,
            )
        );
        assert!(unknown.has_error());
        assert_eq!(
            unknown.to_string(),
            "PartitionData(offset=-1, leaderEpoch=-1, metadata=, error='UNKNOWN_TOPIC_OR_PARTITION')"
        );
        let unauthorized = FetchedOffset::unauthorized_partition(2);
        assert_eq!(
            unauthorized,
            FetchedOffset::new(
                2,
                FetchedOffset::INVALID_OFFSET,
                crate::error::TOPIC_AUTHORIZATION_FAILED,
            )
        );
        assert!(unauthorized.has_error());
        assert_eq!(
            unauthorized.to_string(),
            "PartitionData(offset=-1, leaderEpoch=-1, metadata=, error='TOPIC_AUTHORIZATION_FAILED')"
        );
        assert_eq!(
            FetchedOffset::unknown_partition(1),
            FetchedOffset::error(1, crate::error::UNKNOWN_TOPIC_OR_PARTITION)
        );
        let topic = OffsetFetchTopic {
            topic: "t".into(),
            partitions: vec![0, 3],
        };
        let v1 = topic.error_result(1, crate::error::NOT_COORDINATOR);
        assert_eq!(
            v1,
            FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![
                    FetchedOffset::error(0, crate::error::NOT_COORDINATOR),
                    FetchedOffset::error(3, crate::error::NOT_COORDINATOR),
                ],
            }
        );
        let v2 = topic.error_result(2, crate::error::NOT_COORDINATOR);
        assert_eq!(
            v2,
            FetchedOffsetTopic {
                topic: "t".into(),
                partitions: Vec::new(),
            }
        );
        let mut buf = BytesMut::new();
        encode_offset_fetch_response(
            &mut buf,
            1,
            "g",
            std::slice::from_ref(&v1),
            crate::error::NOT_COORDINATOR,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_offset_fetch_response(&mut cur, 1).unwrap();
        assert_eq!(decoded, vec![v1]);
        assert!(
            cur.is_empty(),
            "OffsetFetch getErrorResponse v1 leftover-empty; leftover {} bytes",
            cur.len()
        );
        let mut buf = BytesMut::new();
        encode_offset_fetch_response(
            &mut buf,
            2,
            "g",
            std::slice::from_ref(&v2),
            crate::error::NOT_COORDINATOR,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 2).unwrap();
        assert_eq!(
            decoded,
            vec![OffsetFetchGroupResult {
                group_id: String::new(),
                topics: vec![v2],
                error_code: crate::error::NOT_COORDINATOR,
            }]
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch getErrorResponse v2 leftover-empty; leftover {} bytes",
            cur.len()
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
    fn offset_fetch_response_error_counts_matches_java() {
        assert!(OffsetFetchResponse::error_counts(8, &[]).is_empty());
        assert_eq!(
            OffsetFetchResponse::error_counts(7, &[]),
            HashMap::from([(0, 1)])
        );
        assert_eq!(
            OffsetFetchResponse::error_counts(1, &[]),
            HashMap::from([(0, 1)])
        );
        let v8 = OffsetFetchResponse::error_counts(
            8,
            &[
                OffsetFetchGroupResult {
                    group_id: "ok".into(),
                    topics: vec![FetchedOffsetTopic {
                        topic: "t".into(),
                        partitions: vec![
                            FetchedOffset::error(0, 0),
                            FetchedOffset::error(1, crate::error::UNKNOWN_TOPIC_OR_PARTITION),
                        ],
                    }],
                    error_code: 0,
                },
                OffsetFetchGroupResult::error("missing", crate::error::NOT_COORDINATOR),
            ],
        );
        assert_eq!(
            v8,
            HashMap::from([
                (0, 2),
                (crate::error::UNKNOWN_TOPIC_OR_PARTITION, 1),
                (crate::error::NOT_COORDINATOR, 1),
            ])
        );
        let v7 = OffsetFetchResponse::error_counts(
            7,
            &[OffsetFetchGroupResult {
                group_id: String::new(),
                topics: vec![FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![
                        FetchedOffset::error(0, 0),
                        FetchedOffset::error(1, crate::error::NOT_LEADER_OR_FOLLOWER),
                    ],
                }],
                error_code: 0,
            }],
        );
        assert_eq!(
            v7,
            HashMap::from([(0, 2), (crate::error::NOT_LEADER_OR_FOLLOWER, 1)])
        );
        let v1_partition = OffsetFetchResponse::error_counts(
            1,
            &[OffsetFetchGroupResult {
                group_id: String::new(),
                topics: vec![FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![FetchedOffset::unknown_partition(0)],
                }],
                error_code: 0,
            }],
        );
        assert_eq!(
            v1_partition,
            HashMap::from([(0, 1), (crate::error::UNKNOWN_TOPIC_OR_PARTITION, 1)])
        );
        let v1_coord = OffsetFetchResponse::error_counts(
            1,
            &[OffsetFetchGroupResult {
                group_id: String::new(),
                topics: vec![FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![FetchedOffset::error(0, crate::error::NOT_COORDINATOR)],
                }],
                error_code: 0,
            }],
        );
        assert_eq!(
            v1_coord,
            HashMap::from([(crate::error::NOT_COORDINATOR, 2)])
        );
    }

    #[test]
    fn offset_fetch_response_group_has_error_matches_java() {
        assert!(!OffsetFetchResponse::group_has_error(8, &[], "g"));
        assert_eq!(OffsetFetchResponse::group_level_error(8, &[], "g"), None);
        assert!(!OffsetFetchResponse::group_has_error(7, &[], "g"));
        assert_eq!(OffsetFetchResponse::group_level_error(7, &[], "g"), Some(0));
        assert!(!OffsetFetchResponse::group_has_error(1, &[], "g"));
        assert_eq!(OffsetFetchResponse::group_level_error(1, &[], "g"), Some(0));

        let groups = [
            OffsetFetchGroupResult {
                group_id: "ok".into(),
                topics: vec![FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![FetchedOffset::error(0, 0)],
                }],
                error_code: 0,
            },
            OffsetFetchGroupResult::error("missing", crate::error::NOT_COORDINATOR),
            OffsetFetchGroupResult::error("loading", crate::error::COORDINATOR_LOAD_IN_PROGRESS),
        ];
        assert!(!OffsetFetchResponse::group_has_error(8, &groups, "ok"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(8, &groups, "ok"),
            Some(0)
        );
        assert!(OffsetFetchResponse::group_has_error(8, &groups, "missing"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(8, &groups, "missing"),
            Some(crate::error::NOT_COORDINATOR)
        );
        assert!(OffsetFetchResponse::group_has_error(8, &groups, "loading"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(8, &groups, "loading"),
            Some(crate::error::COORDINATOR_LOAD_IN_PROGRESS)
        );
        assert!(!OffsetFetchResponse::group_has_error(8, &groups, "other"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(8, &groups, "other"),
            None
        );

        let v7 = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::error(0, 0)],
            }],
            error_code: crate::error::NOT_COORDINATOR,
        }];
        assert!(OffsetFetchResponse::group_has_error(7, &v7, "ignored"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(7, &v7, "ignored"),
            Some(crate::error::NOT_COORDINATOR)
        );
        let v7_none = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: Vec::new(),
            error_code: 0,
        }];
        assert!(!OffsetFetchResponse::group_has_error(7, &v7_none, "g"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(7, &v7_none, "g"),
            Some(0)
        );

        let v1_partition = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::unknown_partition(0)],
            }],
            error_code: 0,
        }];
        assert!(!OffsetFetchResponse::group_has_error(1, &v1_partition, "g"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(1, &v1_partition, "g"),
            Some(0)
        );
        let v1_coord = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::error(0, crate::error::NOT_COORDINATOR)],
            }],
            error_code: 0,
        }];
        assert!(OffsetFetchResponse::group_has_error(1, &v1_coord, "g"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(1, &v1_coord, "g"),
            Some(crate::error::NOT_COORDINATOR)
        );

        let dup = [
            OffsetFetchGroupResult::error("g", 0),
            OffsetFetchGroupResult::error("g", crate::error::NOT_COORDINATOR),
        ];
        assert!(OffsetFetchResponse::group_has_error(8, &dup, "g"));
        assert_eq!(
            OffsetFetchResponse::group_level_error(8, &dup, "g"),
            Some(crate::error::NOT_COORDINATOR)
        );
    }

    #[test]
    fn offset_fetch_response_error_matches_java() {
        // Java OffsetFetchResponse.error: v8+ the wrapper error field is
        // null even when groups have errors. Distinct from groupLevelError,
        // which looks up a named group. v2–v7 are the top-level errorCode.
        // v1 synthesizes the first non-partition error. Java hasError() is
        // not mapped (NPE on v8+).
        assert_eq!(OffsetFetchResponse::error(8, &[]), None);
        assert_eq!(OffsetFetchResponse::error(7, &[]), Some(0));
        assert_eq!(OffsetFetchResponse::error(1, &[]), Some(0));

        let groups = [
            OffsetFetchGroupResult {
                group_id: "ok".into(),
                topics: vec![FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![FetchedOffset::error(0, 0)],
                }],
                error_code: 0,
            },
            OffsetFetchGroupResult::error("missing", crate::error::NOT_COORDINATOR),
        ];
        assert_eq!(OffsetFetchResponse::error(8, &groups), None);
        assert_eq!(
            OffsetFetchResponse::group_level_error(8, &groups, "missing"),
            Some(crate::error::NOT_COORDINATOR),
            "groupLevelError still sees the group on v8+"
        );

        let v7 = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::error(0, 0)],
            }],
            error_code: crate::error::NOT_COORDINATOR,
        }];
        assert_eq!(
            OffsetFetchResponse::error(7, &v7),
            Some(crate::error::NOT_COORDINATOR)
        );
        assert_eq!(OffsetFetchResponse::error(8, &v7), None);

        let v1_partition = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::unknown_partition(0)],
            }],
            error_code: 0,
        }];
        assert_eq!(OffsetFetchResponse::error(1, &v1_partition), Some(0));

        let v1_coord = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::error(0, crate::error::NOT_COORDINATOR)],
            }],
            error_code: 0,
        }];
        assert_eq!(
            OffsetFetchResponse::error(1, &v1_coord),
            Some(crate::error::NOT_COORDINATOR)
        );

        for (version, groups, what) in [
            (
                8_i16,
                groups.as_slice(),
                "OffsetFetch v8 Response.error leftover-empty",
            ),
            (
                7,
                v7.as_slice(),
                "OffsetFetch v7 Response.error leftover-empty",
            ),
            (
                1,
                v1_coord.as_slice(),
                "OffsetFetch v1 Response.error leftover-empty",
            ),
        ] {
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_response(&mut buf, version, groups).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            assert_eq!(
                OffsetFetchResponse::error(version, &decoded),
                OffsetFetchResponse::error(version, groups)
            );
            assert!(
                !cur.has_remaining(),
                "{what}; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [1_i16, 7, 8] {
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_response(&mut buf, version, &[]).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            assert_eq!(
                OffsetFetchResponse::error(version, &decoded),
                OffsetFetchResponse::error(version, &[])
            );
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} Response.error empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn offset_fetch_response_partition_data_map_matches_java() {
        // Java OffsetFetchResponse.partitionDataMap: v1–v7 ignore
        // group_id and flatten data.topics(). v8+ with no groups is
        // empty (groupLevelErrors empty → v0–v7 path). v8+ with groups
        // uses the first matching group_id (stream filter get(0)).
        // Missing group is IndexOutOfBoundsException. HashMap.put
        // overwrites the same pair.
        assert!(OffsetFetchResponse::partition_data_map(8, &[], "g")
            .unwrap()
            .is_empty());
        assert!(OffsetFetchResponse::partition_data_map(7, &[], "g")
            .unwrap()
            .is_empty());
        let first = FetchedOffset {
            partition: 0,
            offset: 5,
            leader_epoch: 2,
            metadata: "m".into(),
            error_code: 0,
        };
        let second = FetchedOffset::error(1, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        let overwrite = FetchedOffset {
            partition: 0,
            offset: 9,
            leader_epoch: 3,
            metadata: "n".into(),
            error_code: crate::error::NOT_LEADER_OR_FOLLOWER,
        };
        let v7 = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![
                FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![first.clone(), second.clone()],
                },
                FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![overwrite.clone()],
                },
            ],
            error_code: 0,
        }];
        assert_eq!(
            OffsetFetchResponse::partition_data_map(7, &v7, "ignored").unwrap(),
            HashMap::from([
                (("t".into(), 0), overwrite.clone()),
                (("t".into(), 1), second.clone()),
            ])
        );
        let v8 = [
            OffsetFetchGroupResult {
                group_id: "g".into(),
                topics: vec![FetchedOffsetTopic {
                    topic: "a".into(),
                    partitions: vec![first.clone()],
                }],
                error_code: 0,
            },
            OffsetFetchGroupResult {
                group_id: "g".into(),
                topics: vec![FetchedOffsetTopic {
                    topic: "b".into(),
                    partitions: vec![second.clone()],
                }],
                error_code: crate::error::NOT_COORDINATOR,
            },
            OffsetFetchGroupResult {
                group_id: "other".into(),
                topics: vec![FetchedOffsetTopic {
                    topic: "c".into(),
                    partitions: vec![overwrite.clone()],
                }],
                error_code: 0,
            },
        ];
        assert_eq!(
            OffsetFetchResponse::partition_data_map(8, &v8, "g").unwrap(),
            HashMap::from([(("a".into(), 0), first.clone())])
        );
        assert_eq!(
            OffsetFetchResponse::group_level_error(8, &v8, "g"),
            Some(crate::error::NOT_COORDINATOR)
        );
        assert_eq!(
            OffsetFetchResponse::partition_data_map(8, &v8, "other").unwrap(),
            HashMap::from([(("c".into(), 0), overwrite.clone())])
        );
        let missing = OffsetFetchResponse::partition_data_map(8, &v8, "missing").unwrap_err();
        assert!(
            matches!(missing, Error::Protocol(_)),
            "v8+ missing group is Java IndexOutOfBoundsException"
        );
        let v2 = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::new(0, 5, 0)],
            }],
            error_code: 0,
        }];
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_response(&mut buf, 2, &v2).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 2).unwrap();
        assert_eq!(decoded, v2);
        assert_eq!(
            OffsetFetchResponse::partition_data_map(2, &decoded, "ignored").unwrap(),
            OffsetFetchResponse::partition_data_map(2, &v2, "ignored").unwrap()
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v2 partitionDataMap leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_groups_response(&mut buf, 8, &v8).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, v8);
        assert_eq!(
            OffsetFetchResponse::partition_data_map(8, &decoded, "g").unwrap(),
            OffsetFetchResponse::partition_data_map(8, &v8, "g").unwrap()
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v8 partitionDataMap leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_response_from_partition_data_matches_java() {
        // Java OffsetFetchResponse(Errors, Map) / (int, Errors, Map):
        // HashMap.getOrDefault by topic name, then partitions().add.
        // Empty map is empty Topics. A later entry for the same name
        // appends even when another topic sits between. Duplicate
        // partitions for the same pair are kept (ArrayList).
        assert!(OffsetFetchResponse::from_partition_data(
            std::iter::empty::<(&str, FetchedOffset)>()
        )
        .is_empty());
        let a0 = FetchedOffset {
            partition: 0,
            offset: 5,
            leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            metadata: "m".into(),
            error_code: 0,
        };
        let b2 = FetchedOffset::new(2, 7, 0);
        let a1 = FetchedOffset::error(1, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        let grouped = OffsetFetchResponse::from_partition_data([
            ("a", a0.clone()),
            ("b", b2.clone()),
            ("a", a1.clone()),
        ]);
        assert_eq!(
            grouped,
            vec![
                FetchedOffsetTopic {
                    topic: "a".into(),
                    partitions: vec![a0, a1],
                },
                FetchedOffsetTopic {
                    topic: "b".into(),
                    partitions: vec![b2],
                },
            ]
        );
        let dup = OffsetFetchResponse::from_partition_data([
            ("t", FetchedOffset::new(0, 1, 0)),
            ("t", FetchedOffset::new(0, 2, 0)),
        ]);
        assert_eq!(
            dup,
            vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::new(0, 1, 0), FetchedOffset::new(0, 2, 0)],
            }]
        );
        let groups = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: grouped.clone(),
            error_code: 0,
        }];
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_response(&mut buf, 2, &groups).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 2).unwrap();
        assert_eq!(decoded, groups);
        assert_eq!(
            decoded.first().map(|g| g.topics.as_slice()),
            Some(grouped.as_slice())
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v2 from_partition_data leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_groups_response(&mut buf, 6, &groups).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 6).unwrap();
        assert_eq!(decoded, groups);
        assert!(
            cur.is_empty(),
            "OffsetFetch v6 from_partition_data leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_response_from_groups_partition_data_matches_java() {
        // Java OffsetFetchResponse(int, Map, Map) v8+: iterate
        // responseData, group inner partitions like (Errors, Map),
        // errors.get(groupId).code(). A group only in errors is omitted.
        // A group in responseData missing from errors is NPE.
        let errors = HashMap::from([
            ("g".into(), 0i16),
            ("h".into(), crate::error::NOT_COORDINATOR),
            ("unused".into(), crate::error::GROUP_AUTHORIZATION_FAILED),
        ]);
        assert!(OffsetFetchResponse::from_groups_partition_data(
            &errors,
            std::iter::empty::<(&str, Vec<(&str, FetchedOffset)>)>(),
        )
        .unwrap()
        .is_empty());
        let missing = OffsetFetchResponse::from_groups_partition_data(
            &errors,
            [("missing", Vec::<(&str, FetchedOffset)>::new())],
        )
        .unwrap_err();
        assert!(
            matches!(missing, Error::Protocol(_)),
            "missing errors.get is Java NullPointerException, got {missing}"
        );
        let a0 = FetchedOffset {
            partition: 0,
            offset: 5,
            leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            metadata: "m".into(),
            error_code: 0,
        };
        let b2 = FetchedOffset::new(2, 7, 0);
        let a1 = FetchedOffset::error(1, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        let grouped = OffsetFetchResponse::from_groups_partition_data(
            &errors,
            [
                (
                    "g",
                    vec![("a", a0.clone()), ("b", b2.clone()), ("a", a1.clone())],
                ),
                ("h", Vec::new()),
            ],
        )
        .unwrap();
        assert_eq!(
            grouped,
            vec![
                OffsetFetchGroupResult {
                    group_id: "g".into(),
                    topics: OffsetFetchResponse::from_partition_data([
                        ("a", a0),
                        ("b", b2),
                        ("a", a1),
                    ]),
                    error_code: 0,
                },
                OffsetFetchGroupResult {
                    group_id: "h".into(),
                    topics: Vec::new(),
                    error_code: crate::error::NOT_COORDINATOR,
                },
            ]
        );
        assert_eq!(grouped.len(), 2, "unused errors entry is omitted");
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_response(&mut buf, 8, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "OffsetFetch v8 from_groups_partition_data leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_groups_response(&mut buf, 9, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 9).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "OffsetFetch v9 from_groups_partition_data leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_response_from_groups_matches_java() {
        // Java OffsetFetchResponse(List, short): v8+ keeps groups. Below
        // v8 requires exactly one group (UnsupportedVersionException).
        // v1 with a non-NONE group error rewrites every partition body
        // (no top-level error field).
        let empty = OffsetFetchResponse::from_groups(7, &[]).unwrap_err();
        assert!(
            matches!(empty, Error::Unsupported(_)),
            "empty groups below v8 is Java UnsupportedVersionException, got {empty}"
        );
        assert!(
            empty.to_string().contains("only supports one group"),
            "got {empty}"
        );
        let two = [
            OffsetFetchGroupResult {
                group_id: "g".into(),
                topics: Vec::new(),
                error_code: 0,
            },
            OffsetFetchGroupResult {
                group_id: "h".into(),
                topics: Vec::new(),
                error_code: 0,
            },
        ];
        let two_err = OffsetFetchResponse::from_groups(7, &two).unwrap_err();
        assert!(
            matches!(two_err, Error::Unsupported(_)),
            "two groups below v8 is Java UnsupportedVersionException, got {two_err}"
        );
        assert_eq!(OffsetFetchResponse::from_groups(8, &two).unwrap(), two);
        let partition = FetchedOffset {
            partition: 0,
            offset: 5,
            leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            metadata: "m".into(),
            error_code: 0,
        };
        let v1_err = [OffsetFetchGroupResult {
            group_id: String::new(),
            topics: vec![FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![partition.clone()],
            }],
            error_code: crate::error::NOT_COORDINATOR,
        }];
        let rewritten = OffsetFetchResponse::from_groups(1, &v1_err).unwrap();
        let rewritten_part = rewritten
            .first()
            .and_then(|g| g.topics.first())
            .and_then(|t| t.partitions.first())
            .expect("rewritten partition");
        assert_eq!(
            rewritten_part,
            &FetchedOffset::error(0, crate::error::NOT_COORDINATOR)
        );
        assert_eq!(
            rewritten.first().map(|g| g.error_code),
            Some(crate::error::NOT_COORDINATOR)
        );
        let v2_err = OffsetFetchResponse::from_groups(2, &v1_err).unwrap();
        assert_eq!(
            v2_err, v1_err,
            "v2+ copies partitions when the group has an error"
        );
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_response(&mut buf, 1, &rewritten).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 1).unwrap();
        assert_eq!(
            decoded.first().map(|g| g.topics.as_slice()),
            rewritten.first().map(|g| g.topics.as_slice())
        );
        assert_eq!(
            decoded.first().map(|g| g.error_code),
            Some(0),
            "v1 wire has no top-level error"
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v1 from_groups leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_groups_response(&mut buf, 2, &v2_err).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 2).unwrap();
        assert_eq!(decoded, v2_err);
        assert!(
            cur.is_empty(),
            "OffsetFetch v2 from_groups leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_groups_response(&mut buf, 8, &two).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, OffsetFetchResponse::from_groups(8, &two).unwrap());
        assert!(
            cur.is_empty(),
            "OffsetFetch v8 from_groups leftover-empty; leftover {} bytes",
            cur.len()
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

        let err = FindCoordinatorResponse::error_results(
            2,
            ["g"],
            crate::error::COORDINATOR_NOT_AVAILABLE,
        );
        assert_eq!(err.len(), 1);
        let first = err.first().expect("old response");
        assert_eq!(first.key, "");
        assert_eq!(first.node_id, -1);
        assert_eq!(first.host, "");
        assert_eq!(first.port, -1);
        assert_eq!(first.error_code, crate::error::COORDINATOR_NOT_AVAILABLE);
        assert!(first.error_message.is_none());
        assert_eq!(
            err,
            vec![CoordinatorResult::error(
                crate::error::COORDINATOR_NOT_AVAILABLE
            )]
        );
        buf.clear();
        encode_find_coordinator_response_coordinators(&mut buf, 2, &err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_find_coordinator_response_coordinators(&mut cur, 2)
                .unwrap()
                .0,
            err
        );
        assert!(
            cur.is_empty(),
            "FindCoordinator v2 getErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_find_coordinator_response_coordinators(&mut buf, 1, &err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_find_coordinator_response_coordinators(&mut cur, 1)
                .unwrap()
                .0,
            err
        );
        assert!(
            cur.is_empty(),
            "FindCoordinator v1 getErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn find_coordinator_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 FindCoordinatorResponse.json ThrottleTimeMs is
        // versions 1+ (INT32 on spoken v1–v6; first field; ignorable).
        // Official Java FindCoordinatorRequest.getErrorResponse sets
        // throttleTimeMs from the argument on v2+; v1 leaves the JSON
        // default 0. encode_find_coordinator_response_coordinators still
        // writes 0. KIP-219 only changes shouldClientThrottle (v2+).
        // Empty-error v1 == v2 (classic); v3 is compact; empty-Coordinators
        // v4 == v5 == v6 (KIP-699; TRANSACTION_ABORTABLE / share groups
        // same layout). Top-level ErrorCode is at bytes 4–5 on v1–v3;
        // v4+ has no top-level ErrorCode. This crate speaks 1–6. This is
        // not JoinGroup / OffsetDelete ThrottleTimeMs.
        let one = vec![CoordinatorResult::error(0)];
        for version in [1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_find_coordinator_response_coordinators_with_throttle(
                &mut buf, version, &one, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_find_coordinator_response_coordinators(&mut cur, version).unwrap();
            assert_eq!(decoded, one);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "FindCoordinator v{version} ThrottleTimeMs leftover-empty"
            );
        }
        let empty: Vec<CoordinatorResult> = vec![];
        for version in [4, 5, 6] {
            let mut buf = BytesMut::new();
            encode_find_coordinator_response_coordinators_with_throttle(
                &mut buf, version, &empty, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_find_coordinator_response_coordinators(&mut cur, version).unwrap();
            assert_eq!(decoded, empty);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "FindCoordinator v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_find_coordinator_response_coordinators_with_throttle(&mut with, 1, &one, 3_600_000)
            .unwrap();
        let mut zero = BytesMut::new();
        encode_find_coordinator_response_coordinators_with_throttle(&mut zero, 1, &one, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v1 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_find_coordinator_response_coordinators(&mut conv, 1, &one).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_find_coordinator_response_coordinators still writes ThrottleTimeMs 0"
        );
        assert_eq!(
            &with[4..6],
            &[0, 0],
            "v1 top-level ErrorCode is at bytes 4-5"
        );

        let mut v2_with = BytesMut::new();
        encode_find_coordinator_response_coordinators_with_throttle(
            &mut v2_with,
            2,
            &one,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v2_with[..],
            "empty-error ThrottleTimeMs bodies: v1 == v2"
        );
        let mut v3_with = BytesMut::new();
        encode_find_coordinator_response_coordinators_with_throttle(
            &mut v3_with,
            3,
            &one,
            3_600_000,
        )
        .unwrap();
        assert_ne!(&v2_with[..], &v3_with[..], "v3 adds compact tagged fields");
        let mut v4_with = BytesMut::new();
        encode_find_coordinator_response_coordinators_with_throttle(
            &mut v4_with,
            4,
            &empty,
            3_600_000,
        )
        .unwrap();
        assert_ne!(
            &v3_with[..],
            &v4_with[..],
            "v4 Coordinators must not match v3 top-level fields"
        );
        assert_ne!(&v4_with[4..6], &[0, 0], "v4+ has no top-level ErrorCode");
        let mut v5_with = BytesMut::new();
        encode_find_coordinator_response_coordinators_with_throttle(
            &mut v5_with,
            5,
            &empty,
            3_600_000,
        )
        .unwrap();
        let mut v6_with = BytesMut::new();
        encode_find_coordinator_response_coordinators_with_throttle(
            &mut v6_with,
            6,
            &empty,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &v4_with[..],
            &v5_with[..],
            "empty-Coordinators ThrottleTimeMs bodies: v4 == v5"
        );
        assert_eq!(
            &v5_with[..],
            &v6_with[..],
            "empty-Coordinators ThrottleTimeMs bodies: v5 == v6"
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

        let err = FindCoordinatorResponse::error_results(
            3,
            ["tx-1", "tx-2"],
            crate::error::COORDINATOR_NOT_AVAILABLE,
        );
        assert_eq!(
            err,
            vec![CoordinatorResult::error(
                crate::error::COORDINATOR_NOT_AVAILABLE
            )],
            "v3 getErrorResponse is prepareOldResponse; keys are not copied"
        );
        let mut buf = BytesMut::new();
        encode_find_coordinator_response_coordinators(&mut buf, 3, &err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_find_coordinator_response_coordinators(&mut cur, 3)
                .unwrap()
                .0,
            err
        );
        assert!(
            cur.is_empty(),
            "FindCoordinator v3 getErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
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

        let err = FindCoordinatorResponse::error_results(
            4,
            ["g"],
            crate::error::COORDINATOR_NOT_AVAILABLE,
        );
        assert_eq!(
            err,
            vec![CoordinatorResult::error_for_key(
                crate::error::COORDINATOR_NOT_AVAILABLE,
                "g"
            )]
        );
        let mut buf = BytesMut::new();
        encode_find_coordinator_response_coordinators(&mut buf, 4, &err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_find_coordinator_response_coordinators(&mut cur, 4)
                .unwrap()
                .0,
            err
        );
        assert!(
            cur.is_empty(),
            "FindCoordinator v4 getErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
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
        let batched = encode_find_coordinator_request_keys(
            &mut BytesMut::new(),
            3,
            &["g", "h"],
            COORDINATOR_GROUP,
        )
        .unwrap_err();
        assert!(
            matches!(batched, Error::Unsupported(_)),
            "two keys on v3 is Java NoBatchedFindCoordinatorsException, got {batched}"
        );
        assert!(
            batched.to_string().contains("only in 4 or later"),
            "got {batched}"
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
        let (decoded, ..) = decode_find_coordinator_response_coordinators(&mut cur, 4).unwrap();
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

        let two_err = FindCoordinatorResponse::prepare_error_response(
            ["g", "h"],
            crate::error::COORDINATOR_NOT_AVAILABLE,
        );
        assert_eq!(two_err.len(), 2);
        assert_eq!(two_err.first().expect("g key").key, "g");
        assert_eq!(two_err.get(1).expect("h key").key, "h");
        assert_eq!(two_err.first().expect("g node").node_id, -1);
        assert_eq!(two_err.first().expect("g host").host, "");
        assert_eq!(two_err.first().expect("g port").port, -1);
        assert!(two_err.first().expect("g msg").error_message.is_none());
        assert_eq!(
            two_err,
            vec![
                CoordinatorResult::error_for_key(crate::error::COORDINATOR_NOT_AVAILABLE, "g"),
                CoordinatorResult::error_for_key(crate::error::COORDINATOR_NOT_AVAILABLE, "h"),
            ]
        );
        buf.clear();
        encode_find_coordinator_response_coordinators(&mut buf, 4, &two_err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_find_coordinator_response_coordinators(&mut cur, 4)
                .unwrap()
                .0,
            two_err
        );
        assert!(
            cur.is_empty(),
            "FindCoordinator v4 prepareErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_find_coordinator_response_coordinators(&mut buf, 6, &two_err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_find_coordinator_response_coordinators(&mut cur, 6)
                .unwrap()
                .0,
            two_err
        );
        assert!(
            cur.is_empty(),
            "FindCoordinator v6 prepareErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
        );

        let batched =
            FindCoordinatorResponse::error_results(4, ["g", "h"], crate::error::NOT_COORDINATOR);
        assert_eq!(
            batched,
            FindCoordinatorResponse::prepare_error_response(
                ["g", "h"],
                crate::error::NOT_COORDINATOR
            )
        );
        let empty = FindCoordinatorResponse::prepare_error_response(
            Vec::<String>::new(),
            crate::error::COORDINATOR_NOT_AVAILABLE,
        );
        assert!(empty.is_empty());
        buf.clear();
        encode_find_coordinator_response_coordinators(&mut buf, 4, &empty).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_find_coordinator_response_coordinators(&mut cur, 4)
                .unwrap()
                .0,
            empty
        );
        assert!(
            cur.is_empty(),
            "FindCoordinator v4 empty prepareErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn find_coordinator_builder_matches_java() {
        let batched = encode_find_coordinator_request_keys(
            &mut BytesMut::new(),
            1,
            &["g", "h"],
            COORDINATOR_GROUP,
        )
        .unwrap_err();
        assert!(
            matches!(batched, Error::Unsupported(_)),
            "two keys on v1 is Java NoBatchedFindCoordinatorsException, got {batched}"
        );
        assert!(
            batched
                .to_string()
                .contains("Cannot create a v1 FindCoordinator request"),
            "got {batched}"
        );
        let empty =
            encode_find_coordinator_request_keys(&mut BytesMut::new(), 3, &[], COORDINATOR_GROUP)
                .unwrap_err();
        assert!(
            empty
                .to_string()
                .contains("does not support CoordinatorKeys"),
            "empty keys below v4 stays crate API, got {empty}"
        );
        encode_find_coordinator_request_keys(&mut BytesMut::new(), 4, &[], COORDINATOR_GROUP)
            .unwrap();
        let v0 = encode_find_coordinator_request_keys(
            &mut BytesMut::new(),
            0,
            &["g", "h"],
            COORDINATOR_GROUP,
        )
        .unwrap_err();
        assert!(
            v0.to_string().contains("not implemented"),
            "v0 stays unspoken first, got {v0}"
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
        assert_eq!(
            sub.to_string(),
            "Subscription(topics=[u, t], ownedPartitions=[u-1, t-2, t-0], groupInstanceId=null, generationId=7, rackId=az1)",
            "Java Subscription.toString"
        );
        assert_eq!(
            ConsumerProtocolSubscription::new(Vec::new()).to_string(),
            "Subscription(topics=[], ownedPartitions=[], groupInstanceId=null, generationId=-1, rackId=null)"
        );
        assert_eq!(sub.topics(), &["u".to_string(), "t".to_string()]);
        assert_eq!(
            sub.owned_partitions(),
            &[("u".into(), 1), ("t".into(), 2), ("t".into(), 0)]
        );
        assert_eq!(sub.generation_id(), Some(7));
        assert_eq!(sub.rack_id(), Some("az1"));
        assert_eq!(
            ConsumerProtocolSubscription::new(Vec::new()).generation_id(),
            None
        );
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
    fn consumer_protocol_assignment_matches_java_serialize() {
        let asg = ConsumerProtocolAssignment::new(vec![
            ("u".into(), 1),
            ("t".into(), 2),
            ("t".into(), 0),
        ]);
        assert_eq!(
            asg.to_string(),
            "Assignment(partitions=[u-1, t-2, t-0])",
            "Java Assignment.toString"
        );
        assert_eq!(
            ConsumerProtocolAssignment::new(Vec::new()).to_string(),
            "Assignment(partitions=[])"
        );
        assert_eq!(
            asg.partitions(),
            &[("u".into(), 1), ("t".into(), 2), ("t".into(), 0)]
        );

        let bytes = ConsumerProtocol::serialize_assignment(&asg).unwrap();
        assert_eq!(
            ConsumerProtocol::deserialize_version(&bytes).unwrap(),
            ConsumerProtocol::HIGHEST_SUPPORTED_VERSION
        );
        let got = ConsumerProtocol::deserialize_assignment(&bytes).unwrap();
        assert_eq!(
            got.partitions,
            vec![("u".into(), 1), ("t".into(), 2), ("t".into(), 0)],
            "serializeAssignment does not sort partitions"
        );
        assert_eq!(
            decode_assignment(&bytes).unwrap(),
            vec![("u".into(), vec![1]), ("t".into(), vec![2, 0])],
            "first-seen topic order"
        );
        assert_eq!(
            encode_tp_assignment(&[("u".into(), 1), ("t".into(), 2), ("t".into(), 0)]).unwrap(),
            bytes
        );

        let v0 = ConsumerProtocol::serialize_assignment_version(&asg, 0).unwrap();
        assert_eq!(ConsumerProtocol::deserialize_version(&v0).unwrap(), 0);
        assert_eq!(
            ConsumerProtocol::deserialize_assignment(&v0)
                .unwrap()
                .partitions,
            asg.partitions
        );

        let capped = ConsumerProtocol::serialize_assignment_version(&asg, 9).unwrap();
        assert_eq!(
            ConsumerProtocol::deserialize_version(&capped).unwrap(),
            ConsumerProtocol::HIGHEST_SUPPORTED_VERSION
        );
        let err = ConsumerProtocol::serialize_assignment_version(&asg, -1).unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported assignment version: -1"),
            "Java checkAssignmentVersion SchemaException, got {err}"
        );
        assert!(ConsumerProtocol::deserialize_assignment(&[])
            .unwrap()
            .partitions
            .is_empty());
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
                rebalance_timeout_ms: 10_000,
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
            rebalance_timeout_ms: 10_000,
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
            rebalance_timeout_ms: 10_000,
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
        let (err, gen, proto, leader, mid, skip, got, ..) =
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
        let (err, gen, proto, leader, mid, skip, got, ..) =
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
        let (err, _, _, _, _, skip, members, ..) = decode_join_group_response(&mut cur, 8).unwrap();
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
        let (err, _, _, _, _, skip, ..) = decode_join_group_response(&mut cur, 9).unwrap();
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
    fn join_group_request_build_matches_java() {
        JoinGroupRequest::build(4, None).unwrap();
        JoinGroupRequest::build(5, None).unwrap();
        JoinGroupRequest::build(5, Some("i")).unwrap();
        JoinGroupRequest::build(5, Some("")).unwrap();
        JoinGroupRequest::build(9, Some("i")).unwrap();
        let v4 = JoinGroupRequest::build(4, Some("i")).unwrap_err();
        assert!(
            matches!(v4, Error::Unsupported(_)),
            "v4 with group.instance.id is Java UnsupportedVersionException, got {v4}"
        );
        assert!(
            v4.to_string()
                .contains("does not support usage of config group.instance.id"),
            "got {v4}"
        );
        let empty = JoinGroupRequest::build(2, Some("")).unwrap_err();
        assert!(
            matches!(empty, Error::Unsupported(_)),
            "empty group.instance.id is still present (Java != null), got {empty}"
        );
        let ignored = JoinGroupRequest {
            group_id: "g",
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            member_id: "m1",
            group_instance_id: Some("ignored-on-v2"),
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &[1, 2, 3],
            reason: None,
        };
        encode_join_group_request(&mut BytesMut::new(), 2, &ignored).unwrap();
        assert!(
            JoinGroupRequest::build(2, Some("ignored-on-v2")).is_err(),
            "encode omits group.instance.id below v5; Builder.build rejects it"
        );

        for version in [5_i16, 6] {
            JoinGroupRequest::build(version, Some("i")).unwrap();
            let req = JoinGroupRequest {
                group_id: "g",
                session_timeout_ms: 10_000,
                rebalance_timeout_ms: 10_000,
                member_id: "m1",
                group_instance_id: Some("i"),
                protocol_type: "consumer",
                protocol_name: "range",
                metadata: &[1, 2, 3],
                reason: None,
            };
            let mut buf = BytesMut::new();
            encode_join_group_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let (gid, member, instance, meta, reason) =
                decode_join_group_request(&mut cur, version).unwrap();
            assert_eq!((gid.as_str(), member.as_str()), ("g", "m1"));
            assert_eq!(instance.as_deref(), Some("i"));
            assert_eq!(meta, vec![1, 2, 3]);
            assert_eq!(reason, None);
            assert!(
                !cur.has_remaining(),
                "JoinGroup v{version} Builder.build leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [2_i16, 4, 5, 9] {
            JoinGroupRequest::build(version, None).unwrap();
            let mut buf = BytesMut::new();
            encode_join_group_request(&mut buf, version, &join_req(&[1, 2, 3])).unwrap();
            let mut cur = buf.as_ref();
            let (gid, member, instance, meta, reason) =
                decode_join_group_request(&mut cur, version).unwrap();
            assert_eq!((gid.as_str(), member.as_str()), ("g", "m1"));
            assert_eq!(instance, None);
            assert_eq!(meta, vec![1, 2, 3]);
            assert_eq!(reason, None);
            assert!(
                !cur.has_remaining(),
                "JoinGroup v{version} Builder.build empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
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
            rebalance_timeout_ms: 10_000,
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
        let (gid, member, instance, got, reason, ..) =
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
                rebalance_timeout_ms: 10_000,
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
    fn join_group_request_session_timeout_ms_matches_java() {
        // Kafka 4.0 JoinGroupRequest.json SessionTimeoutMs is versions 0+
        // (INT32 after GroupId). Official Java JoinGroupRequestData.sessionTimeoutMs
        // reads it. Encode already takes session_timeout_ms; decode previously
        // discarded it. This crate speaks 2–9. This is not RebalanceTimeoutMs
        // / ProtocolType.
        let meta = [1u8, 2, 3];
        let req = JoinGroupRequest {
            group_id: "g",
            session_timeout_ms: 3_600_000,
            rebalance_timeout_ms: 3_600_000,
            member_id: "m1",
            group_instance_id: None,
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &meta,
            reason: None,
        };
        for version in [2_i16, 5, 6, 8, 9] {
            let mut buf = BytesMut::new();
            encode_join_group_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let (gid, member, instance, got, reason, session_timeout_ms, ..) =
                decode_join_group_request_protocols(&mut cur, version).unwrap();
            assert_eq!(gid, "g");
            assert_eq!(member, "m1");
            assert_eq!(got.len(), 1);
            assert_eq!(reason, None);
            assert_eq!(session_timeout_ms, 3_600_000);
            if version >= 5 {
                assert_eq!(instance, None);
            }
            assert!(
                cur.is_empty(),
                "JoinGroup request v{version} SessionTimeoutMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_join_group_request(&mut with, 2, &req).unwrap();
        let mut ten = BytesMut::new();
        encode_join_group_request(&mut ten, 2, &join_req(&meta)).unwrap();
        assert_ne!(
            &with[..],
            &ten[..],
            "v2 SessionTimeoutMs is not always 10000"
        );
        let mut cur = ten.as_ref();
        let (.., session_timeout_ms, _, _) =
            decode_join_group_request_protocols(&mut cur, 2).unwrap();
        assert_eq!(session_timeout_ms, 10_000);
    }

    #[test]
    fn join_group_request_rebalance_timeout_ms_matches_java() {
        // Kafka 4.0 JoinGroupRequest.json RebalanceTimeoutMs is versions 1+
        // (INT32 after SessionTimeoutMs; default -1; ignorable). Spoken v2–v9
        // always write the field. Official Java JoinGroupRequestData.rebalanceTimeoutMs
        // reads it. Encode previously copied session_timeout_ms; decode discarded
        // it. Classic Java ClassicKafkaConsumer sends max.poll.interval.ms.
        // This crate speaks 2–9. This is not SessionTimeoutMs / ProtocolType.
        let meta = [1u8, 2, 3];
        let req = JoinGroupRequest {
            group_id: "g",
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 300_000,
            member_id: "m1",
            group_instance_id: None,
            protocol_type: "consumer",
            protocol_name: "range",
            metadata: &meta,
            reason: None,
        };
        for version in [2_i16, 5, 6, 8, 9] {
            let mut buf = BytesMut::new();
            encode_join_group_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let (gid, member, instance, got, reason, session_timeout_ms, rebalance_timeout_ms, ..) =
                decode_join_group_request_protocols(&mut cur, version).unwrap();
            assert_eq!(gid, "g");
            assert_eq!(member, "m1");
            assert_eq!(got.len(), 1);
            assert_eq!(reason, None);
            assert_eq!(session_timeout_ms, 10_000);
            assert_eq!(rebalance_timeout_ms, 300_000);
            if version >= 5 {
                assert_eq!(instance, None);
            }
            assert!(
                cur.is_empty(),
                "JoinGroup request v{version} RebalanceTimeoutMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_join_group_request(&mut with, 2, &req).unwrap();
        let mut copied = BytesMut::new();
        encode_join_group_request(&mut copied, 2, &join_req(&meta)).unwrap();
        assert_ne!(
            &with[..],
            &copied[..],
            "v2 RebalanceTimeoutMs is not always SessionTimeoutMs"
        );
        let mut cur = copied.as_ref();
        let (.., rebalance_timeout_ms, _) =
            decode_join_group_request_protocols(&mut cur, 2).unwrap();
        assert_eq!(rebalance_timeout_ms, 10_000);
    }

    #[test]
    fn join_group_request_protocol_type_matches_java() {
        // Kafka 4.0 JoinGroupRequest.json ProtocolType is versions 0+
        // (STRING after GroupInstanceId on v5+ / after MemberId below v5).
        // Official Java JoinGroupRequestData.protocolType reads it. Encode
        // already takes protocol_type; decode previously discarded it. This
        // crate speaks 2–9. This is not response ProtocolType / ProtocolName
        // / SessionTimeoutMs / RebalanceTimeoutMs.
        let meta = [1u8, 2, 3];
        let req = JoinGroupRequest {
            group_id: "g",
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            member_id: "m1",
            group_instance_id: None,
            protocol_type: "connect",
            protocol_name: "range",
            metadata: &meta,
            reason: None,
        };
        for version in [2_i16, 5, 6, 8, 9] {
            let mut buf = BytesMut::new();
            encode_join_group_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let (gid, member, instance, got, reason, session, rebalance, protocol_type) =
                decode_join_group_request_protocols(&mut cur, version).unwrap();
            assert_eq!(gid, "g");
            assert_eq!(member, "m1");
            assert_eq!(got.len(), 1);
            assert_eq!(reason, None);
            assert_eq!(session, 10_000);
            assert_eq!(rebalance, 10_000);
            assert_eq!(protocol_type, "connect");
            if version >= 5 {
                assert_eq!(instance, None);
            }
            assert!(
                cur.is_empty(),
                "JoinGroup request v{version} ProtocolType leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_join_group_request(&mut with, 2, &req).unwrap();
        let mut consumer = BytesMut::new();
        encode_join_group_request(&mut consumer, 2, &join_req(&meta)).unwrap();
        assert_ne!(
            &with[..],
            &consumer[..],
            "v2 ProtocolType is not always consumer"
        );
        let mut cur = consumer.as_ref();
        let (.., protocol_type) = decode_join_group_request_protocols(&mut cur, 2).unwrap();
        assert_eq!(protocol_type, "consumer");
    }

    #[test]
    fn join_group_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 JoinGroupResponse.json ThrottleTimeMs is versions
        // 2+ (INT32 on spoken v2–v9; first field; ignorable). Official
        // Java JoinGroupRequest.getErrorResponse /
        // JoinGroupResponse.throttleTimeMs set / read it.
        // encode_join_group_response still writes the JSON default 0.
        // KIP-219 only changes shouldClientThrottle (v3+). Empty-Members
        // v2 == v3 == v4 == v5 (classic; GroupInstanceId is on members);
        // v6 is compact; v7 == v8 (ProtocolType / nullable ProtocolName;
        // Reason is on the request); v9 adds SkipAssignment. Top-level
        // ErrorCode is at bytes 4–5. This crate speaks 2–9. This is not
        // FindCoordinator / OffsetDelete ThrottleTimeMs.
        let members: &[JoinMember] = &[];
        for version in [2, 3, 4, 5, 6, 7, 8, 9] {
            let mut buf = BytesMut::new();
            encode_join_group_response_with_throttle(
                &mut buf, version, 0, 0, "", "", "", members, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (err, gen, protocol, leader, member, skip, got, ptype, throttle) =
                decode_join_group_response(&mut cur, version).unwrap();
            assert_eq!(err, 0);
            assert_eq!(gen, 0);
            assert_eq!(protocol, "");
            assert_eq!(leader, "");
            assert_eq!(member, "");
            assert!(!skip);
            assert!(got.is_empty());
            assert_eq!(ptype, None);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "JoinGroup v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_join_group_response_with_throttle(
            &mut with, 2, 0, 0, "", "", "", members, 3_600_000,
        )
        .unwrap();
        let mut zero = BytesMut::new();
        encode_join_group_response_with_throttle(&mut zero, 2, 0, 0, "", "", "", members, 0)
            .unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v2 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_join_group_response(&mut conv, 2, 0, 0, "", "", "", members).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_join_group_response still writes ThrottleTimeMs 0"
        );
        assert_eq!(
            &with[4..6],
            &[0, 0],
            "v2 top-level ErrorCode is at bytes 4-5"
        );

        let mut v3_with = BytesMut::new();
        encode_join_group_response_with_throttle(
            &mut v3_with,
            3,
            0,
            0,
            "",
            "",
            "",
            members,
            3_600_000,
        )
        .unwrap();
        let mut v4_with = BytesMut::new();
        encode_join_group_response_with_throttle(
            &mut v4_with,
            4,
            0,
            0,
            "",
            "",
            "",
            members,
            3_600_000,
        )
        .unwrap();
        let mut v5_with = BytesMut::new();
        encode_join_group_response_with_throttle(
            &mut v5_with,
            5,
            0,
            0,
            "",
            "",
            "",
            members,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v3_with[..],
            "empty-Members ThrottleTimeMs bodies: v2 == v3"
        );
        assert_eq!(
            &v3_with[..],
            &v4_with[..],
            "empty-Members ThrottleTimeMs bodies: v3 == v4"
        );
        assert_eq!(
            &v4_with[..],
            &v5_with[..],
            "empty-Members ThrottleTimeMs bodies: v4 == v5"
        );
        let mut v6_with = BytesMut::new();
        encode_join_group_response_with_throttle(
            &mut v6_with,
            6,
            0,
            0,
            "",
            "",
            "",
            members,
            3_600_000,
        )
        .unwrap();
        assert_ne!(&v5_with[..], &v6_with[..], "v6 adds compact tagged fields");
        let mut v7_with = BytesMut::new();
        encode_join_group_response_with_throttle(
            &mut v7_with,
            7,
            0,
            0,
            "",
            "",
            "",
            members,
            3_600_000,
        )
        .unwrap();
        assert_ne!(
            &v6_with[..],
            &v7_with[..],
            "v7 adds ProtocolType after GenerationId"
        );
        let mut v8_with = BytesMut::new();
        encode_join_group_response_with_throttle(
            &mut v8_with,
            8,
            0,
            0,
            "",
            "",
            "",
            members,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &v7_with[..],
            &v8_with[..],
            "empty-Members ThrottleTimeMs bodies: v7 == v8"
        );
        let mut v9_with = BytesMut::new();
        encode_join_group_response_with_throttle(
            &mut v9_with,
            9,
            0,
            0,
            "",
            "",
            "",
            members,
            3_600_000,
        )
        .unwrap();
        assert_ne!(
            &v8_with[..],
            &v9_with[..],
            "v9 adds SkipAssignment after Leader"
        );
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
        let (err, gen, protocol, leader, member, skip, members, ..) =
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
        let (err, _, protocol, ..) = decode_join_group_response(&mut cur, 6).unwrap();
        assert_eq!(err, 16);
        assert_eq!(protocol, JoinGroupRequest::UNKNOWN_PROTOCOL_NAME);
        assert!(
            cur.is_empty(),
            "v6 empty ProtocolName leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn join_group_response_protocol_name_matches_java() {
        assert_eq!(JoinGroupResponse::protocol_name(6, None), Some(""));
        assert_eq!(JoinGroupResponse::protocol_name(6, Some("")), Some(""));
        assert_eq!(
            JoinGroupResponse::protocol_name(6, Some("range")),
            Some("range")
        );
        assert_eq!(JoinGroupResponse::protocol_name(7, Some("")), None);
        assert_eq!(JoinGroupResponse::protocol_name(7, None), None);
        assert_eq!(
            JoinGroupResponse::protocol_name(7, Some("range")),
            Some("range")
        );
        assert_eq!(
            JoinGroupResponse::protocol_name(2, None),
            Some(JoinGroupRequest::UNKNOWN_PROTOCOL_NAME)
        );

        for version in [2_i16, 6, 7, 9] {
            let rewritten = JoinGroupResponse::protocol_name(version, Some(""));
            let mut buf = BytesMut::new();
            encode_join_group_response(
                &mut buf,
                version,
                16,
                JoinGroupRequest::UNKNOWN_GENERATION_ID,
                rewritten.unwrap_or(""),
                JoinGroupRequest::UNKNOWN_MEMBER_ID,
                JoinGroupRequest::UNKNOWN_MEMBER_ID,
                &[],
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (err, _, protocol, ..) = decode_join_group_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert_eq!(protocol, JoinGroupRequest::UNKNOWN_PROTOCOL_NAME);
            assert!(
                cur.is_empty(),
                "JoinGroup v{version} protocolName leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        for version in [2_i16, 6, 7, 9] {
            let rewritten = JoinGroupResponse::protocol_name(version, None);
            let mut buf = BytesMut::new();
            encode_join_group_response(
                &mut buf,
                version,
                16,
                JoinGroupRequest::UNKNOWN_GENERATION_ID,
                rewritten.unwrap_or(""),
                JoinGroupRequest::UNKNOWN_MEMBER_ID,
                JoinGroupRequest::UNKNOWN_MEMBER_ID,
                &[],
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (err, _, protocol, ..) = decode_join_group_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert_eq!(protocol, JoinGroupRequest::UNKNOWN_PROTOCOL_NAME);
            assert!(
                cur.is_empty(),
                "JoinGroup v{version} protocolName empty leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
    }

    #[test]
    fn join_group_error_response_matches_java() {
        // Java JoinGroupRequest.getErrorResponse: UNKNOWN generation /
        // protocol / member sentinels, empty members, throttle JSON default 0.
        // ProtocolName is null on v7+ and empty string below. SkipAssignment
        // on v9+ stays the JSON default (false).
        const V7: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x01, 0x01,
            0x01, 0x00,
        ];
        for version in [2_i16, 6, 7, 9] {
            let mut expected = BytesMut::new();
            encode_join_group_response(
                &mut expected,
                version,
                16,
                JoinGroupRequest::UNKNOWN_GENERATION_ID,
                JoinGroupRequest::UNKNOWN_PROTOCOL_NAME,
                JoinGroupRequest::UNKNOWN_MEMBER_ID,
                JoinGroupRequest::UNKNOWN_MEMBER_ID,
                &[],
            )
            .unwrap();
            let mut got = BytesMut::new();
            JoinGroupRequest::error_response(&mut got, version, 16).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "JoinGroup v{version} getErrorResponse must match sentinel encode"
            );
            if version == 7 {
                assert_eq!(&got[..], V7);
            }
            let mut cur = &got[..];
            let (err, gen, protocol, leader, member, skip, members, ..) =
                decode_join_group_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert_eq!(gen, JoinGroupRequest::UNKNOWN_GENERATION_ID);
            assert_eq!(protocol, JoinGroupRequest::UNKNOWN_PROTOCOL_NAME);
            assert_eq!(leader, JoinGroupRequest::UNKNOWN_MEMBER_ID);
            assert_eq!(member, JoinGroupRequest::UNKNOWN_MEMBER_ID);
            assert!(!skip, "v{version} SkipAssignment JSON default is false");
            assert!(members.is_empty());
            assert!(
                cur.is_empty(),
                "JoinGroup v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        let mut v6 = BytesMut::new();
        JoinGroupRequest::error_response(&mut v6, 6, 16).unwrap();
        let mut v7 = BytesMut::new();
        JoinGroupRequest::error_response(&mut v7, 7, 16).unwrap();
        assert_ne!(
            &v6[..],
            &v7[..],
            "v7+ getErrorResponse ProtocolName is null, not empty compact string"
        );
        let mut v9 = BytesMut::new();
        JoinGroupRequest::error_response(&mut v9, 9, 16).unwrap();
        assert_ne!(
            &v7[..],
            &v9[..],
            "v9 getErrorResponse must include SkipAssignment"
        );
    }

    #[test]
    fn join_group_response_protocol_type_matches_java() {
        for version in [7_i16, 8, 9] {
            let mut buf = BytesMut::new();
            encode_join_group_response_with_protocol_type(
                &mut buf,
                version,
                0,
                7,
                "range",
                "l",
                "m1",
                &[],
                Some("consumer"),
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (err, gen, proto, leader, mid, skip, members, ptype, ..) =
                decode_join_group_response(&mut cur, version).unwrap();
            assert_eq!(err, 0);
            assert_eq!(gen, 7);
            assert_eq!(proto, "range");
            assert_eq!(leader, "l");
            assert_eq!(mid, "m1");
            assert!(!skip);
            assert!(members.is_empty());
            assert_eq!(ptype.as_deref(), Some("consumer"));
            assert!(
                cur.is_empty(),
                "JoinGroup v{version} response ProtocolType leftover-empty"
            );
        }

        for version in [2_i16, 6] {
            let mut buf = BytesMut::new();
            encode_join_group_response_with_protocol_type(
                &mut buf,
                version,
                0,
                7,
                "range",
                "l",
                "m1",
                &[],
                Some("consumer"),
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (.., ptype, _) = decode_join_group_response(&mut cur, version).unwrap();
            assert!(
                cur.is_empty(),
                "JoinGroup v{version} response ProtocolType leftover-empty"
            );
            assert_eq!(
                ptype, None,
                "JoinGroup v{version} omits response ProtocolType even when the body has a value"
            );
        }

        let mut with = BytesMut::new();
        encode_join_group_response_with_protocol_type(
            &mut with,
            7,
            0,
            7,
            "range",
            "l",
            "m1",
            &[],
            Some("consumer"),
        )
        .unwrap();
        let mut none = BytesMut::new();
        encode_join_group_response_with_protocol_type(
            &mut none,
            7,
            0,
            7,
            "range",
            "l",
            "m1",
            &[],
            None,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &none[..],
            "v7 response ProtocolType is not always the JSON default null"
        );
        let mut conv = BytesMut::new();
        encode_join_group_response(&mut conv, 7, 0, 7, "range", "l", "m1", &[]).unwrap();
        assert_eq!(
            &conv[..],
            &none[..],
            "encode_join_group_response still writes null ProtocolType"
        );
        let mut v6_with = BytesMut::new();
        encode_join_group_response_with_protocol_type(
            &mut v6_with,
            6,
            0,
            7,
            "range",
            "l",
            "m1",
            &[],
            Some("consumer"),
        )
        .unwrap();
        let mut v6_none = BytesMut::new();
        encode_join_group_response_with_protocol_type(
            &mut v6_none,
            6,
            0,
            7,
            "range",
            "l",
            "m1",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(
            &v6_with[..],
            &v6_none[..],
            "v6 encode omits response ProtocolType even when the body has a value"
        );
        assert_ne!(
            &v6_with[..],
            &with[..],
            "v7 adds response ProtocolType after GenerationId"
        );

        let mut empty_buf = BytesMut::new();
        encode_join_group_response_with_protocol_type(
            &mut empty_buf,
            7,
            0,
            7,
            "range",
            "l",
            "m1",
            &[],
            Some(""),
        )
        .unwrap();
        let mut cur = empty_buf.as_ref();
        let (.., ptype, _) = decode_join_group_response(&mut cur, 7).unwrap();
        assert_eq!(ptype.as_deref(), Some(""));
        assert!(
            cur.is_empty(),
            "JoinGroup v7 empty response ProtocolType leftover-empty"
        );
        assert_ne!(
            &empty_buf[..],
            &none[..],
            "empty response ProtocolType is still present (Java != null)"
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
        encode_offset_commit_request(
            &mut v2,
            2,
            "g",
            7,
            "m1",
            Some("ignored"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut v3 = BytesMut::new();
        encode_offset_commit_request(
            &mut v3,
            3,
            "g",
            7,
            "m1",
            Some("ignored"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut v4 = BytesMut::new();
        encode_offset_commit_request(
            &mut v4,
            4,
            "g",
            7,
            "m1",
            Some("ignored"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_eq!(&v2[..], REQ);
        assert_eq!(v2.as_ref(), v3.as_ref(), "v2 and v3 request bodies match");
        assert_eq!(v3.as_ref(), v4.as_ref(), "v3 and v4 request bodies match");
        let mut cur = v2.as_ref();
        let (gid, mid, got, retention, ..) = decode_offset_commit_request(&mut cur, 2).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(retention, DEFAULT_RETENTION_TIME);
        assert_eq!(got[0].partitions[0].offset, 3);
        assert_eq!(
            got[0].partitions[0].leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(cur.is_empty(), "v2 request leftover-empty");

        let mut v5 = BytesMut::new();
        encode_offset_commit_request(
            &mut v5,
            5,
            "g",
            7,
            "m1",
            Some("ignored"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_ne!(v4.as_ref(), v5.as_ref(), "v5 drops RetentionTimeMs");
        let mut cur = v5.as_ref();
        let (_gid, _mid, got, retention, ..) = decode_offset_commit_request(&mut cur, 5).unwrap();
        assert_eq!(retention, DEFAULT_RETENTION_TIME);
        assert_eq!(
            got[0].partitions[0].leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert!(cur.is_empty(), "v5 request leftover-empty");

        let mut v6 = BytesMut::new();
        encode_offset_commit_request(
            &mut v6,
            6,
            "g",
            7,
            "m1",
            Some("ignored"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_ne!(v5.as_ref(), v6.as_ref(), "v6 adds CommittedLeaderEpoch");
        let mut cur = v6.as_ref();
        let (_gid, _mid, got, retention, ..) = decode_offset_commit_request(&mut cur, 6).unwrap();
        assert_eq!(retention, DEFAULT_RETENTION_TIME);
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
        let err = encode_offset_commit_request(
            &mut v2,
            0,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap_err();
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
    fn offset_commit_retention_time_matches_java() {
        let topics = [OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition {
                partition: 0,
                offset: 3,
                leader_epoch: 4,
                metadata: String::new(),
            }],
        }];
        for version in [2_i16, 3, 4] {
            let mut buf = BytesMut::new();
            encode_offset_commit_request(&mut buf, version, "g", 7, "m1", None, 3_600_000, &topics)
                .unwrap();
            let mut cur = buf.as_ref();
            let (_, _, got, retention, ..) =
                decode_offset_commit_request(&mut cur, version).unwrap();
            assert_eq!(got[0].partitions[0].offset, 3);
            assert_eq!(retention, 3_600_000);
            assert!(
                cur.is_empty(),
                "OffsetCommit v{version} RetentionTimeMs leftover-empty"
            );
        }

        let mut buf = BytesMut::new();
        encode_offset_commit_request(&mut buf, 5, "g", 7, "m1", None, 3_600_000, &topics).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, retention, ..) = decode_offset_commit_request(&mut cur, 5).unwrap();
        assert!(
            cur.is_empty(),
            "OffsetCommit v5 RetentionTimeMs leftover-empty"
        );
        assert_eq!(
            retention, DEFAULT_RETENTION_TIME,
            "OffsetCommit v5 omits RetentionTimeMs even when the body has a non-default value"
        );

        let mut with = BytesMut::new();
        encode_offset_commit_request(&mut with, 2, "g", 7, "m1", None, 3_600_000, &topics).unwrap();
        let mut default = BytesMut::new();
        encode_offset_commit_request(
            &mut default,
            2,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &default[..],
            "v2 RetentionTimeMs is not always the JSON default -1"
        );
        let mut v5_nonzero = BytesMut::new();
        encode_offset_commit_request(&mut v5_nonzero, 5, "g", 7, "m1", None, 3_600_000, &topics)
            .unwrap();
        let mut v5_default = BytesMut::new();
        encode_offset_commit_request(
            &mut v5_default,
            5,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_eq!(
            &v5_nonzero[..],
            &v5_default[..],
            "v5 encode omits RetentionTimeMs even when the body has a non-default value"
        );

        for version in [8_i16, 9] {
            let mut buf = BytesMut::new();
            encode_offset_commit_request(&mut buf, version, "g", 7, "m1", None, 3_600_000, &topics)
                .unwrap();
            let mut cur = buf.as_ref();
            let (_, _, _, retention, ..) = decode_offset_commit_request(&mut cur, version).unwrap();
            assert!(
                cur.is_empty(),
                "OffsetCommit v{version} RetentionTimeMs leftover-empty"
            );
            assert_eq!(
                retention, DEFAULT_RETENTION_TIME,
                "OffsetCommit v{version} omits RetentionTimeMs even when the body has a non-default value"
            );
        }
    }

    #[test]
    fn offset_commit_group_instance_id_matches_java() {
        let topics = [OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition {
                partition: 0,
                offset: 3,
                leader_epoch: 4,
                metadata: String::new(),
            }],
        }];
        for version in [7_i16, 8, 9] {
            let mut buf = BytesMut::new();
            encode_offset_commit_request(
                &mut buf,
                version,
                "g",
                7,
                "m1",
                Some("i"),
                DEFAULT_RETENTION_TIME,
                &topics,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (gid, mid, got, retention, inst) =
                decode_offset_commit_request(&mut cur, version).unwrap();
            assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
            assert_eq!(got[0].partitions[0].offset, 3);
            assert_eq!(retention, DEFAULT_RETENTION_TIME);
            assert_eq!(inst.as_deref(), Some("i"));
            assert!(
                cur.is_empty(),
                "OffsetCommit v{version} GroupInstanceId leftover-empty"
            );
        }

        let mut buf = BytesMut::new();
        encode_offset_commit_request(
            &mut buf,
            2,
            "g",
            7,
            "m1",
            Some("i"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, _, inst) = decode_offset_commit_request(&mut cur, 2).unwrap();
        assert!(
            cur.is_empty(),
            "OffsetCommit v2 GroupInstanceId leftover-empty"
        );
        assert_eq!(
            inst, None,
            "OffsetCommit v2 omits GroupInstanceId even when the body has an instance id"
        );

        let mut buf = BytesMut::new();
        encode_offset_commit_request(
            &mut buf,
            6,
            "g",
            7,
            "m1",
            Some("i"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, _, inst) = decode_offset_commit_request(&mut cur, 6).unwrap();
        assert!(
            cur.is_empty(),
            "OffsetCommit v6 GroupInstanceId leftover-empty"
        );
        assert_eq!(
            inst, None,
            "OffsetCommit v6 omits GroupInstanceId even when the body has an instance id"
        );

        let mut with = BytesMut::new();
        encode_offset_commit_request(
            &mut with,
            7,
            "g",
            7,
            "m1",
            Some("i"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut none = BytesMut::new();
        encode_offset_commit_request(
            &mut none,
            7,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &none[..],
            "v7 GroupInstanceId is not always the JSON default null"
        );
        let mut v6_with = BytesMut::new();
        encode_offset_commit_request(
            &mut v6_with,
            6,
            "g",
            7,
            "m1",
            Some("i"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut v6_none = BytesMut::new();
        encode_offset_commit_request(
            &mut v6_none,
            6,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_eq!(
            &v6_with[..],
            &v6_none[..],
            "v6 encode omits GroupInstanceId even when the body has an instance id"
        );
        assert_ne!(
            &v6_with[..],
            &with[..],
            "v7 adds GroupInstanceId after MemberId"
        );

        let mut empty = BytesMut::new();
        encode_offset_commit_request(
            &mut empty,
            7,
            "g",
            7,
            "m1",
            Some(""),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut cur = empty.as_ref();
        let (_, _, _, _, inst) = decode_offset_commit_request(&mut cur, 7).unwrap();
        assert_eq!(inst.as_deref(), Some(""));
        assert!(
            cur.is_empty(),
            "OffsetCommit v7 empty GroupInstanceId leftover-empty"
        );
        assert_ne!(
            &empty[..],
            &none[..],
            "empty GroupInstanceId is still present (Java != null)"
        );
    }

    #[test]
    fn offset_commit_v7_batches_partitions_and_consumes_epoch_metadata() {
        let topics = offset_commit_topics();
        let mut buf = BytesMut::new();
        encode_offset_commit_request(
            &mut buf,
            7,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut cur = &buf[..];
        let (gid, mid, got, retention, ..) = decode_offset_commit_request(&mut cur, 7).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got, topics);
        assert_eq!(retention, DEFAULT_RETENTION_TIME);
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
        encode_offset_commit_request(
            &mut req,
            8,
            "g",
            7,
            "m1",
            Some("i"),
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut cur = &req[..];
        let (gid, mid, got, retention, ..) = decode_offset_commit_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert_eq!(got, topics);
        assert_eq!(retention, DEFAULT_RETENTION_TIME);
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
        encode_offset_commit_request(
            &mut v8,
            8,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        let mut v9 = BytesMut::new();
        encode_offset_commit_request(
            &mut v9,
            9,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
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
        encode_offset_commit_request(
            &mut buf,
            8,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v7 = BytesMut::new();
        encode_offset_commit_request(
            &mut v7,
            7,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &topics,
        )
        .unwrap();
        assert_ne!(&buf[..], &v7[..], "OffsetCommit v8 must not be classic v7");
        assert!(
            encode_offset_commit_request(
                &mut BytesMut::new(),
                1,
                "g",
                7,
                "m1",
                None,
                DEFAULT_RETENTION_TIME,
                &topics
            )
            .is_err(),
            "OffsetCommit v0–v1 are not spoken"
        );
        assert!(
            encode_offset_commit_request(
                &mut BytesMut::new(),
                10,
                "g",
                7,
                "m1",
                None,
                DEFAULT_RETENTION_TIME,
                &topics
            )
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
    fn offset_commit_get_error_response_copies_names_and_partitions() {
        let topics = [
            OffsetTopic {
                topic: "orders".into(),
                partitions: vec![OffsetPartition::new(0, 10), OffsetPartition::new(1, 20)],
            },
            OffsetTopic {
                topic: "payments".into(),
                partitions: vec![OffsetPartition::new(2, 30)],
            },
        ];
        let err = OffsetTopic::error_results(&topics, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert_eq!(err.len(), 2);
        let orders = err.first().expect("orders");
        assert_eq!(orders.topic(), "orders");
        assert_eq!(
            orders.partitions(),
            [
                OffsetCommitResponsePartition::error(0, crate::error::CLUSTER_AUTHORIZATION_FAILED),
                OffsetCommitResponsePartition::error(1, crate::error::CLUSTER_AUTHORIZATION_FAILED),
            ]
        );
        let payments = err.get(1).expect("payments");
        assert_eq!(payments.topic(), "payments");
        assert_eq!(
            payments.partitions(),
            [OffsetCommitResponsePartition::error(
                2,
                crate::error::CLUSTER_AUTHORIZATION_FAILED
            )]
        );
        assert_eq!(
            err,
            vec![
                topics[0].error_result(crate::error::CLUSTER_AUTHORIZATION_FAILED),
                topics[1].error_result(crate::error::CLUSTER_AUTHORIZATION_FAILED),
            ]
        );
        for version in [2i16, 3, 7, 8, 9] {
            let mut buf = BytesMut::new();
            encode_offset_commit_topics_response(&mut buf, version, &err).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(
                decode_offset_commit_topics_response(&mut cur, version)
                    .unwrap()
                    .0,
                err
            );
            assert!(
                !cur.has_remaining(),
                "OffsetCommit v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            assert_eq!(
                decode_offset_commit_response(&mut buf.as_ref(), version).unwrap(),
                crate::error::CLUSTER_AUTHORIZATION_FAILED
            );
        }
        let empty = OffsetTopic::error_results(&[], crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert!(empty.is_empty());
        for version in [2i16, 3, 7, 8, 9] {
            let mut buf = BytesMut::new();
            encode_offset_commit_topics_response(&mut buf, version, &empty).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(
                decode_offset_commit_topics_response(&mut cur, version)
                    .unwrap()
                    .0,
                empty
            );
            assert!(
                !cur.has_remaining(),
                "OffsetCommit v{version} empty getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            assert_eq!(
                decode_offset_commit_response(&mut buf.as_ref(), version).unwrap(),
                0
            );
        }
    }

    #[test]
    fn offset_commit_throttle_time_ms_matches_java() {
        let topics = [OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition::new(0, 1)],
        }];
        let err = OffsetTopic::error_results(&topics, 16);
        for version in [3_i16, 4, 7, 8, 9] {
            let mut buf = BytesMut::new();
            OffsetCommitRequest::error_response(&mut buf, version, &topics, 16, 3_600_000).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_offset_commit_topics_response(&mut cur, version).unwrap();
            assert_eq!(decoded, err);
            assert_eq!(throttle, 3_600_000);
            assert_eq!(
                decode_offset_commit_response(&mut buf.as_ref(), version).unwrap(),
                16
            );
            assert!(
                cur.is_empty(),
                "OffsetCommit v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut buf = BytesMut::new();
        OffsetCommitRequest::error_response(&mut buf, 2, &topics, 16, 3_600_000).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, throttle) = decode_offset_commit_topics_response(&mut cur, 2).unwrap();
        assert_eq!(decoded, err);
        assert!(
            cur.is_empty(),
            "OffsetCommit v2 ThrottleTimeMs leftover-empty"
        );
        assert_eq!(
            throttle, 0,
            "OffsetCommit v2 omits ThrottleTimeMs even when the body has a non-zero value"
        );

        let mut with = BytesMut::new();
        encode_offset_commit_topics_response_with_throttle(&mut with, 3, &err, 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_offset_commit_topics_response_with_throttle(&mut zero, 3, &err, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v3 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_offset_commit_topics_response(&mut conv, 3, &err).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_offset_commit_topics_response still writes ThrottleTimeMs 0"
        );
        let mut v2_with = BytesMut::new();
        encode_offset_commit_topics_response_with_throttle(&mut v2_with, 2, &err, 3_600_000)
            .unwrap();
        let mut v2_zero = BytesMut::new();
        encode_offset_commit_topics_response_with_throttle(&mut v2_zero, 2, &err, 0).unwrap();
        assert_eq!(
            &v2_with[..],
            &v2_zero[..],
            "v2 encode omits ThrottleTimeMs even when the body has a non-zero value"
        );
        assert_ne!(
            &v2_with[..],
            &with[..],
            "v3 adds ThrottleTimeMs before Topics"
        );

        for version in [2_i16, 3, 8, 9] {
            let mut expected = BytesMut::new();
            encode_offset_commit_topics_response_with_throttle(
                &mut expected,
                version,
                &err,
                3_600_000,
            )
            .unwrap();
            let mut got = BytesMut::new();
            OffsetCommitRequest::error_response(&mut got, version, &topics, 16, 3_600_000).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "OffsetCommit v{version} getErrorResponse must match with_throttle encode"
            );
            let mut cur = got.as_ref();
            let (decoded, throttle) =
                decode_offset_commit_topics_response(&mut cur, version).unwrap();
            assert_eq!(decoded, err);
            if version >= 3 {
                assert_eq!(throttle, 3_600_000);
            } else {
                assert_eq!(throttle, 0);
            }
            assert!(
                cur.is_empty(),
                "OffsetCommit v{version} getErrorResponse leftover-empty"
            );
        }
    }

    #[test]
    fn offset_commit_offsets_matches_java() {
        // Java OffsetCommitRequest.offsets: each (topic, partition) maps
        // to that committed offset. Later partitions overwrite the same
        // pair (HashMap.put).
        assert!(OffsetCommitRequest::offsets(&[]).is_empty());
        let one = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition::new(0, 10), OffsetPartition::new(3, 20)],
        }];
        assert_eq!(
            OffsetCommitRequest::offsets(&one),
            HashMap::from([(("t".into(), 0), 10), (("t".into(), 3), 20)])
        );
        let two = vec![
            OffsetTopic {
                topic: "a".into(),
                partitions: vec![OffsetPartition::new(0, 1)],
            },
            OffsetTopic {
                topic: "a".into(),
                partitions: vec![OffsetPartition::new(0, 2)],
            },
            OffsetTopic {
                topic: "b".into(),
                partitions: vec![OffsetPartition::new(1, 3)],
            },
        ];
        assert_eq!(
            OffsetCommitRequest::offsets(&two),
            HashMap::from([(("a".into(), 0), 2), (("b".into(), 1), 3)])
        );
        let mut buf = BytesMut::new();
        encode_offset_commit_request(
            &mut buf,
            2,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &two,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_offset_commit_request(&mut cur, 2).unwrap().2;
        assert_eq!(decoded, two);
        assert_eq!(
            OffsetCommitRequest::offsets(&decoded),
            OffsetCommitRequest::offsets(&two)
        );
        assert!(
            !cur.has_remaining(),
            "OffsetCommit v2 offsets leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        buf.clear();
        encode_offset_commit_request(
            &mut buf,
            8,
            "g",
            7,
            "m1",
            None,
            DEFAULT_RETENTION_TIME,
            &two,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_offset_commit_request(&mut cur, 8).unwrap().2;
        assert_eq!(decoded, two);
        assert_eq!(
            OffsetCommitRequest::offsets(&decoded),
            OffsetCommitRequest::offsets(&two)
        );
        assert!(
            !cur.has_remaining(),
            "OffsetCommit v8 offsets leftover-empty; leftover {} bytes",
            cur.remaining()
        );
    }

    #[test]
    fn offset_commit_request_build_matches_java() {
        OffsetCommitRequest::build(6, None).unwrap();
        OffsetCommitRequest::build(7, None).unwrap();
        OffsetCommitRequest::build(7, Some("i")).unwrap();
        OffsetCommitRequest::build(7, Some("")).unwrap();
        OffsetCommitRequest::build(8, Some("i")).unwrap();
        let v6 = OffsetCommitRequest::build(6, Some("i")).unwrap_err();
        assert!(
            matches!(v6, Error::Unsupported(_)),
            "v6 with group.instance.id is Java UnsupportedVersionException, got {v6}"
        );
        assert!(
            v6.to_string()
                .contains("does not support usage of config group.instance.id"),
            "got {v6}"
        );
        let empty = OffsetCommitRequest::build(2, Some("")).unwrap_err();
        assert!(
            matches!(empty, Error::Unsupported(_)),
            "empty group.instance.id is still present (Java != null), got {empty}"
        );
        encode_offset_commit_request(
            &mut BytesMut::new(),
            2,
            "g",
            7,
            "m1",
            Some("ignored"),
            DEFAULT_RETENTION_TIME,
            &offset_commit_topics(),
        )
        .unwrap();
        assert!(
            OffsetCommitRequest::build(2, Some("ignored")).is_err(),
            "encode omits group.instance.id below v7; Builder.build rejects it"
        );

        let topics = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition::new(0, 3), OffsetPartition::new(2, 9)],
        }];
        for (version, instance) in [(7_i16, Some("i")), (8, Some("i"))] {
            OffsetCommitRequest::build(version, instance).unwrap();
            let mut buf = BytesMut::new();
            encode_offset_commit_request(
                &mut buf,
                version,
                "g",
                7,
                "m1",
                instance,
                DEFAULT_RETENTION_TIME,
                &topics,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let decoded = decode_offset_commit_request(&mut cur, version).unwrap().2;
            assert_eq!(decoded, topics);
            assert!(
                !cur.has_remaining(),
                "OffsetCommit v{version} Builder.build leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [2_i16, 6, 7, 8] {
            OffsetCommitRequest::build(version, None).unwrap();
            let mut buf = BytesMut::new();
            encode_offset_commit_request(
                &mut buf,
                version,
                "g",
                7,
                "m1",
                None,
                DEFAULT_RETENTION_TIME,
                &topics,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let decoded = decode_offset_commit_request(&mut cur, version).unwrap().2;
            assert_eq!(decoded, topics);
            assert!(
                !cur.has_remaining(),
                "OffsetCommit v{version} Builder.build empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
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
        let all = OffsetFetchGroup::new("g", None);
        let empty = OffsetFetchGroup::new("g", Some(Vec::new()));
        let one = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
        assert!(all.is_all_partitions());
        assert!(!empty.is_all_partitions());
        assert!(!one.is_all_partitions());

        let mut v1 = BytesMut::new();
        let err = encode_offset_fetch_request(&mut v1, 1, "g", None, -1, false, None).unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "null Topics on v1 is Java UnsupportedVersionException, got {err}"
        );
        assert!(
            err.to_string().contains("v2 or newer"),
            "v1 Topics is not nullable, got {err}"
        );

        let mut v1_empty = BytesMut::new();
        encode_offset_fetch_request(&mut v1_empty, 1, "g", None, -1, false, Some(&[])).unwrap();
        let mut cur = v1_empty.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 1).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, Some(Vec::new()));
        assert!(cur.is_empty(), "v1 empty Topics leftover-empty");

        let mut v2 = BytesMut::new();
        encode_offset_fetch_request(&mut v2, 2, "g", None, -1, false, None).unwrap();
        let mut cur = v2.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 2).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, None);
        assert!(cur.is_empty(), "v2 null Topics leftover-empty");
        const V2: &[u8] = &[0x00, 0x01, 0x67, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(&v2[..], V2);

        let mut v2_empty = BytesMut::new();
        encode_offset_fetch_request(&mut v2_empty, 2, "g", None, -1, false, Some(&[])).unwrap();
        let mut cur = v2_empty.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 2).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, Some(Vec::new()));
        assert!(cur.is_empty(), "v2 empty Topics leftover-empty");
        assert_ne!(
            v2.as_ref(),
            v2_empty.as_ref(),
            "v2 null Topics (INT32 -1) differs from empty (INT32 0)"
        );

        let mut v8 = BytesMut::new();
        encode_offset_fetch_request(&mut v8, 8, "g", None, -1, false, None).unwrap();
        let mut cur = v8.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, None);
        assert!(cur.is_empty(), "v8 null Topics leftover-empty");
        const V8: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(&v8[..], V8);

        let mut v8_empty = BytesMut::new();
        encode_offset_fetch_request(&mut v8_empty, 8, "g", None, -1, false, Some(&[])).unwrap();
        let mut cur = v8_empty.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 8).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, Some(Vec::new()));
        assert!(cur.is_empty(), "v8 empty Topics leftover-empty");
        assert_ne!(
            v8.as_ref(),
            v8_empty.as_ref(),
            "v8 null Topics (compact 0x00) differs from empty (compact 0x01)"
        );

        let mut v9 = BytesMut::new();
        encode_offset_fetch_request(&mut v9, 9, "g", None, -1, false, None).unwrap();
        let mut cur = v9.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 9).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, None);
        assert!(cur.is_empty(), "v9 null Topics leftover-empty");

        let mut v9_empty = BytesMut::new();
        encode_offset_fetch_request(&mut v9_empty, 9, "g", None, -1, false, Some(&[])).unwrap();
        let mut cur = v9_empty.as_ref();
        let (gid, got, stable) = decode_offset_fetch_request(&mut cur, 9).unwrap();
        assert_eq!((gid.as_str(), stable), ("g", false));
        assert_eq!(got, Some(Vec::new()));
        assert!(cur.is_empty(), "v9 empty Topics leftover-empty");
        assert_ne!(
            v9.as_ref(),
            v9_empty.as_ref(),
            "v9 null Topics differs from empty"
        );
    }

    #[test]
    fn offset_fetch_group_ids_to_partitions_matches_java() {
        // Java OffsetFetchRequest.groupIdsToPartitions: each group id maps
        // to that group's (topic, partition) list. Null Topics is null.
        // Empty Topics is empty, not all partitions. Later groups
        // overwrite the same id (HashMap.put).
        assert!(OffsetFetchRequest::group_ids_to_partitions(&[]).is_empty());
        let all = OffsetFetchGroup::new("all", None);
        let empty = OffsetFetchGroup::new("empty", Some(Vec::new()));
        let one = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
        assert_eq!(
            OffsetFetchRequest::group_ids_to_partitions(&[all, empty, one]),
            HashMap::from([
                ("all".into(), None),
                ("empty".into(), Some(Vec::new())),
                ("g".into(), Some(vec![("t".into(), 0)])),
            ])
        );
        let first = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
        let second = OffsetFetchGroup::new(
            "g",
            Some(vec![OffsetFetchTopic {
                topic: "b".into(),
                partitions: vec![1, 2],
            }]),
        );
        let other = OffsetFetchGroup::new(
            "h",
            Some(vec![OffsetFetchTopic {
                topic: "c".into(),
                partitions: vec![3],
            }]),
        );
        let two = vec![first, second, other];
        assert_eq!(
            OffsetFetchRequest::group_ids_to_partitions(&two),
            HashMap::from([
                ("g".into(), Some(vec![("b".into(), 1), ("b".into(), 2)])),
                ("h".into(), Some(vec![("c".into(), 3)])),
            ])
        );
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_request(&mut buf, 8, &two, false).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, 8).unwrap();
        assert!(!stable);
        assert_eq!(decoded, two);
        assert_eq!(
            OffsetFetchRequest::group_ids_to_partitions(&decoded),
            OffsetFetchRequest::group_ids_to_partitions(&two)
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v8 groupIdsToPartitions leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_groups_request(&mut buf, 9, &two, false).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, 9).unwrap();
        assert!(!stable);
        assert_eq!(decoded, two);
        assert_eq!(
            OffsetFetchRequest::group_ids_to_partitions(&decoded),
            OffsetFetchRequest::group_ids_to_partitions(&two)
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v9 groupIdsToPartitions leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_group_ids_to_topics_matches_java() {
        // Java OffsetFetchRequest.groupIdsToTopics: each group id maps
        // to that group's Topics list as-is. Null Topics is null. Empty
        // Topics is empty, not all partitions. Later groups overwrite
        // the same id (HashMap.put). Distinct from groupIdsToPartitions,
        // which flattens.
        assert!(OffsetFetchRequest::group_ids_to_topics(&[]).is_empty());
        let all = OffsetFetchGroup::new("all", None);
        let empty = OffsetFetchGroup::new("empty", Some(Vec::new()));
        let one = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
        assert_eq!(
            OffsetFetchRequest::group_ids_to_topics(&[all, empty, one]),
            HashMap::from([
                ("all".into(), None),
                ("empty".into(), Some(Vec::new())),
                ("g".into(), Some(offset_fetch_one_topic())),
            ])
        );
        let first = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
        let second = OffsetFetchGroup::new(
            "g",
            Some(vec![OffsetFetchTopic {
                topic: "b".into(),
                partitions: vec![1, 2],
            }]),
        );
        let other = OffsetFetchGroup::new(
            "h",
            Some(vec![OffsetFetchTopic {
                topic: "c".into(),
                partitions: vec![3],
            }]),
        );
        let two = vec![first, second, other];
        assert_eq!(
            OffsetFetchRequest::group_ids_to_topics(&two),
            HashMap::from([
                (
                    "g".into(),
                    Some(vec![OffsetFetchTopic {
                        topic: "b".into(),
                        partitions: vec![1, 2],
                    }])
                ),
                (
                    "h".into(),
                    Some(vec![OffsetFetchTopic {
                        topic: "c".into(),
                        partitions: vec![3],
                    }])
                ),
            ])
        );
        assert_eq!(
            OffsetFetchRequest::group_ids_to_partitions(&two)
                .get("g")
                .cloned()
                .flatten(),
            Some(vec![("b".into(), 1), ("b".into(), 2)])
        );
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_request(&mut buf, 8, &two, false).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, 8).unwrap();
        assert!(!stable);
        assert_eq!(decoded, two);
        assert_eq!(
            OffsetFetchRequest::group_ids_to_topics(&decoded),
            OffsetFetchRequest::group_ids_to_topics(&two)
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v8 groupIdsToTopics leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_groups_request(&mut buf, 9, &two, false).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, 9).unwrap();
        assert!(!stable);
        assert_eq!(decoded, two);
        assert_eq!(
            OffsetFetchRequest::group_ids_to_topics(&decoded),
            OffsetFetchRequest::group_ids_to_topics(&two)
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v9 groupIdsToTopics leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_group_ids_matches_java() {
        // Java OffsetFetchRequest.groupIds: stream GroupId into a List.
        // Duplicate ids are kept. Distinct from groupIdsToTopics, which
        // HashMap.put overwrites the same id.
        assert!(OffsetFetchRequest::group_ids(&[]).is_empty());
        let first = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
        let second = OffsetFetchGroup::new(
            "g",
            Some(vec![OffsetFetchTopic {
                topic: "b".into(),
                partitions: vec![1],
            }]),
        );
        let other = OffsetFetchGroup::new("h", None);
        let groups = vec![first, second, other];
        assert_eq!(
            OffsetFetchRequest::group_ids(&groups),
            ["g".to_string(), "g".to_string(), "h".to_string()]
        );
        assert_eq!(OffsetFetchRequest::group_ids_to_topics(&groups).len(), 2);
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_request(&mut buf, 8, &groups, false).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, 8).unwrap();
        assert!(!stable);
        assert_eq!(decoded, groups);
        assert_eq!(
            OffsetFetchRequest::group_ids(&decoded),
            OffsetFetchRequest::group_ids(&groups)
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v8 groupIds leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_groups_request(&mut buf, 9, &groups, false).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, 9).unwrap();
        assert!(!stable);
        assert_eq!(decoded, groups);
        assert_eq!(
            OffsetFetchRequest::group_ids(&decoded),
            OffsetFetchRequest::group_ids(&groups)
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v9 groupIds leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_partitions_matches_java() {
        // Java OffsetFetchRequest.partitions: null Topics is null. Otherwise
        // each (topic, partition) in request order. Duplicate pairs are
        // kept (ArrayList).
        assert_eq!(OffsetFetchRequest::partitions(None), None);
        assert_eq!(OffsetFetchRequest::partitions(Some(&[])), Some(Vec::new()));
        let one = offset_fetch_one_topic();
        assert_eq!(
            OffsetFetchRequest::partitions(Some(&one)),
            Some(vec![("t".into(), 0)])
        );
        let two = [
            OffsetFetchTopic {
                topic: "a".into(),
                partitions: vec![0, 1],
            },
            OffsetFetchTopic {
                topic: "b".into(),
                partitions: vec![2],
            },
        ];
        assert_eq!(
            OffsetFetchRequest::partitions(Some(&two)),
            Some(vec![("a".into(), 0), ("a".into(), 1), ("b".into(), 2),])
        );
        let mut buf = BytesMut::new();
        encode_offset_fetch_request(&mut buf, 2, "g", None, -1, false, Some(&two)).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, decoded, stable) = decode_offset_fetch_request(&mut cur, 2).unwrap();
        assert!(!stable);
        assert_eq!(decoded.as_deref(), Some(two.as_slice()));
        assert_eq!(
            OffsetFetchRequest::partitions(decoded.as_deref()),
            OffsetFetchRequest::partitions(Some(&two))
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v2 partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_request(&mut buf, 7, "g", None, -1, false, Some(&two)).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, decoded, stable) = decode_offset_fetch_request(&mut cur, 7).unwrap();
        assert!(!stable);
        assert_eq!(decoded.as_deref(), Some(two.as_slice()));
        assert_eq!(
            OffsetFetchRequest::partitions(decoded.as_deref()),
            OffsetFetchRequest::partitions(Some(&two))
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v7 partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_request(&mut buf, 2, "g", None, -1, false, None).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, decoded, _stable) = decode_offset_fetch_request(&mut cur, 2).unwrap();
        assert_eq!(OffsetFetchRequest::partitions(decoded.as_deref()), None);
        assert!(
            cur.is_empty(),
            "OffsetFetch v2 null partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_from_partitions_matches_java() {
        // Java OffsetFetchRequest.Builder Topics from a partition list:
        // null is ALL_TOPIC_PARTITIONS. HashMap.getOrDefault by topic
        // name, then partitionIndexes().add. Empty list is empty Topics,
        // not all partitions. A later entry for the same name appends
        // even when another topic sits between. Duplicate partitions
        // for the same pair are kept (ArrayList).
        assert_eq!(
            OffsetFetchRequest::from_partitions(None::<std::iter::Empty<(&str, i32)>>),
            None
        );
        assert_eq!(
            OffsetFetchRequest::from_partitions(Some(std::iter::empty::<(&str, i32)>())),
            Some(Vec::new())
        );
        let grouped = OffsetFetchRequest::from_partitions(Some([("a", 0), ("b", 2), ("a", 1)]));
        assert_eq!(
            grouped,
            Some(vec![
                OffsetFetchTopic {
                    topic: "a".into(),
                    partitions: vec![0, 1],
                },
                OffsetFetchTopic {
                    topic: "b".into(),
                    partitions: vec![2],
                },
            ])
        );
        let dup = OffsetFetchRequest::from_partitions(Some([("t", 0), ("t", 0)]));
        assert_eq!(
            dup,
            Some(vec![OffsetFetchTopic {
                topic: "t".into(),
                partitions: vec![0, 0],
            }])
        );
        let grouped = grouped.expect("grouped topics");
        let mut buf = BytesMut::new();
        encode_offset_fetch_request(&mut buf, 2, "g", None, -1, false, Some(&grouped)).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, decoded, stable) = decode_offset_fetch_request(&mut cur, 2).unwrap();
        assert!(!stable);
        assert_eq!(decoded.as_deref(), Some(grouped.as_slice()));
        assert!(
            cur.is_empty(),
            "OffsetFetch v2 from_partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_request(&mut buf, 7, "g", None, -1, false, Some(&grouped)).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, decoded, stable) = decode_offset_fetch_request(&mut cur, 7).unwrap();
        assert!(!stable);
        assert_eq!(decoded.as_deref(), Some(grouped.as_slice()));
        assert!(
            cur.is_empty(),
            "OffsetFetch v7 from_partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_offset_fetch_request(&mut buf, 2, "g", None, -1, false, None).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, decoded, _stable) = decode_offset_fetch_request(&mut cur, 2).unwrap();
        assert_eq!(decoded, None);
        assert_eq!(
            OffsetFetchRequest::from_partitions(None::<std::iter::Empty<(&str, i32)>>),
            None
        );
        assert!(
            cur.is_empty(),
            "OffsetFetch v2 null from_partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn offset_fetch_request_build_matches_java() {
        assert!(!OffsetFetchRequest::build(6, false, true).unwrap());
        assert!(!OffsetFetchRequest::build(6, true, false).unwrap());
        assert!(OffsetFetchRequest::build(7, true, true).unwrap());
        assert!(OffsetFetchRequest::build(7, true, false).unwrap());
        assert!(!OffsetFetchRequest::build(7, false, true).unwrap());
        let v6 = OffsetFetchRequest::build(6, true, true).unwrap_err();
        assert!(
            matches!(v6, Error::Unsupported(_)),
            "v6 requireStable with throwOnFetchStableOffsetsUnsupported is Java UnsupportedVersionException, got {v6}"
        );
        assert!(
            v6.to_string()
                .contains("doesn't support requireStable flag on version 6"),
            "got {v6}"
        );
        let req = offset_fetch_one_topic();
        encode_offset_fetch_request(&mut BytesMut::new(), 6, "g", None, -1, true, Some(&req))
            .unwrap();
        assert!(
            OffsetFetchRequest::build(6, true, true).is_err(),
            "encode omits RequireStable below v7; Builder.build rejects it when throwOnFetchStableOffsetsUnsupported"
        );

        for version in [7_i16, 8] {
            assert!(OffsetFetchRequest::build(version, true, false).unwrap());
            let mut buf = BytesMut::new();
            encode_offset_fetch_request(&mut buf, version, "g", None, -1, true, Some(&req))
                .unwrap();
            let mut cur = buf.as_ref();
            let (_gid, decoded, stable) = decode_offset_fetch_request(&mut cur, version).unwrap();
            assert!(stable);
            assert_eq!(decoded.as_deref(), Some(req.as_slice()));
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} Builder.build leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [1_i16, 6, 7, 9] {
            assert!(!OffsetFetchRequest::build(version, false, false).unwrap());
            let mut buf = BytesMut::new();
            encode_offset_fetch_request(&mut buf, version, "g", None, -1, false, Some(&req))
                .unwrap();
            let mut cur = buf.as_ref();
            let (_gid, decoded, stable) = decode_offset_fetch_request(&mut cur, version).unwrap();
            assert!(!stable);
            assert_eq!(decoded.as_deref(), Some(req.as_slice()));
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} Builder.build empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn offset_fetch_request_groups_matches_java() {
        // Java OffsetFetchRequest.groups: v8+ is data.groups() as-is.
        // Below v8 is always a singleton from old GroupId / Topics (member
        // id null / epoch -1). Empty Groups below v8 is still that
        // singleton. Extra groups below v8 are dropped.
        assert!(OffsetFetchRequest::groups(8, &[]).is_empty());
        let empty_v7 = OffsetFetchRequest::groups(7, &[]);
        assert_eq!(empty_v7, vec![OffsetFetchGroup::new("", None)]);
        assert_eq!(OffsetFetchRequest::group_ids(&[]).len(), 0);

        let first = OffsetFetchGroup {
            group_id: "g".into(),
            member_id: Some("m1".into()),
            member_epoch: 3,
            topics: Some(offset_fetch_one_topic()),
        };
        let second = OffsetFetchGroup::new("h", None);
        let two = vec![first.clone(), second.clone()];
        let v8 = OffsetFetchRequest::groups(8, &two);
        assert_eq!(v8, two);
        assert_eq!(v8.first().and_then(|g| g.member_id.as_deref()), Some("m1"));
        let v7 = OffsetFetchRequest::groups(7, &two);
        assert_eq!(v7.len(), 1);
        let v7_one = v7.first().expect("v7 singleton");
        assert_eq!(v7_one.group_id, "g");
        assert_eq!(v7_one.member_id, None);
        assert_eq!(v7_one.member_epoch, -1);
        assert_eq!(v7_one.topics, Some(offset_fetch_one_topic()));
        assert_eq!(OffsetFetchRequest::group_ids(&two).len(), 2);

        let req = offset_fetch_one_topic();
        for version in [7_i16, 8] {
            let grouped = OffsetFetchRequest::groups(
                version,
                std::slice::from_ref(&OffsetFetchGroup::new("g", Some(req.clone()))),
            );
            assert_eq!(grouped.len(), 1);
            let mut buf = BytesMut::new();
            encode_offset_fetch_request(&mut buf, version, "g", None, -1, false, Some(&req))
                .unwrap();
            let mut cur = buf.as_ref();
            let (_gid, decoded, stable) = decode_offset_fetch_request(&mut cur, version).unwrap();
            assert!(!stable);
            assert_eq!(decoded.as_deref(), Some(req.as_slice()));
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} groups leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [8_i16, 9] {
            let grouped = OffsetFetchRequest::groups(version, &[]);
            assert!(grouped.is_empty());
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_request(&mut buf, version, &[], false).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, version).unwrap();
            assert!(!stable);
            assert!(decoded.is_empty());
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} groups empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [2_i16, 7] {
            let grouped = OffsetFetchRequest::groups(version, &[]);
            assert_eq!(grouped, vec![OffsetFetchGroup::new("", None)]);
            let mut buf = BytesMut::new();
            encode_offset_fetch_request(&mut buf, version, "", None, -1, false, None).unwrap();
            let mut cur = buf.as_ref();
            let (gid, decoded, stable) = decode_offset_fetch_request(&mut cur, version).unwrap();
            assert!(!stable);
            assert_eq!(gid, "");
            assert_eq!(decoded, None);
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} groups singleton leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn offset_fetch_request_is_all_partitions_for_group_matches_java() {
        // Java OffsetFetchRequest.isAllPartitionsForGroup: first matching
        // GroupId (stream.filter then List.get(0)). Missing group is
        // IndexOutOfBoundsException. Duplicate ids keep the first.
        // None Topics is every committed partition.
        let missing = OffsetFetchRequest::is_all_partitions_for_group(&[], "g").unwrap_err();
        assert!(
            missing.to_string().contains("no group named g"),
            "empty Groups is Java IndexOutOfBoundsException, got {missing}"
        );
        let named = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
        let all = OffsetFetchGroup::new("g", None);
        let other = OffsetFetchGroup::new("h", None);
        assert!(!OffsetFetchRequest::is_all_partitions_for_group(
            std::slice::from_ref(&named),
            "g"
        )
        .unwrap());
        assert!(
            OffsetFetchRequest::is_all_partitions_for_group(std::slice::from_ref(&all), "g")
                .unwrap()
        );
        assert!(OffsetFetchRequest::is_all_partitions_for_group(
            &[named.clone(), other.clone()],
            "h"
        )
        .unwrap());
        assert!(
            !OffsetFetchRequest::is_all_partitions_for_group(&[named.clone(), all.clone()], "g")
                .unwrap(),
            "duplicate GroupId keeps the first (Some Topics, not later None)"
        );
        let empty_topics = OffsetFetchGroup::new("g", Some(Vec::new()));
        assert!(
            !OffsetFetchRequest::is_all_partitions_for_group(
                std::slice::from_ref(&empty_topics),
                "g"
            )
            .unwrap(),
            "Some empty is not all partitions"
        );
        let unknown =
            OffsetFetchRequest::is_all_partitions_for_group(std::slice::from_ref(&named), "nope")
                .unwrap_err();
        assert!(
            unknown.to_string().contains("no group named nope"),
            "got {unknown}"
        );

        for version in [8_i16, 9] {
            let groups = [named.clone(), other.clone()];
            assert!(!OffsetFetchRequest::is_all_partitions_for_group(&groups, "g").unwrap());
            assert!(OffsetFetchRequest::is_all_partitions_for_group(&groups, "h").unwrap());
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_request(&mut buf, version, &groups, false).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, version).unwrap();
            assert!(!stable);
            assert_eq!(
                OffsetFetchRequest::is_all_partitions_for_group(&decoded, "g").unwrap(),
                OffsetFetchRequest::is_all_partitions_for_group(&groups, "g").unwrap()
            );
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} isAllPartitionsForGroup leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [8_i16, 9] {
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_request(&mut buf, version, &[], false).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, stable) = decode_offset_fetch_groups_request(&mut cur, version).unwrap();
            assert!(!stable);
            assert!(decoded.is_empty());
            let empty_err =
                OffsetFetchRequest::is_all_partitions_for_group(&decoded, "g").unwrap_err();
            assert!(
                empty_err.to_string().contains("no group named g"),
                "got {empty_err}"
            );
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} isAllPartitionsForGroup empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn offset_fetch_request_error_response_matches_java() {
        // Java OffsetFetchRequest.getErrorResponse: v1 fills unique
        // partitions from data.topics() (null is NPE). v2–v7 omit
        // partitions. Below v8 is the groups() singleton. v8+ unique
        // GroupId (HashMap.put); error_results keeps duplicates.
        let named = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
        let all = OffsetFetchGroup::new("g", None);
        let extra = OffsetFetchGroup::new("h", None);
        let null_v1 =
            OffsetFetchRequest::error_response(1, std::slice::from_ref(&all), 16).unwrap_err();
        assert!(
            null_v1.to_string().contains("null Topics"),
            "v1 null Topics is Java NullPointerException, got {null_v1}"
        );

        let v1 = OffsetFetchRequest::error_response(1, std::slice::from_ref(&named), 16).unwrap();
        assert_eq!(
            v1,
            vec![OffsetFetchGroupResult {
                group_id: "g".into(),
                topics: vec![FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![FetchedOffset::error(0, 16)],
                }],
                error_code: 16,
            }]
        );
        let dup_part = OffsetFetchTopic {
            topic: "t".into(),
            partitions: vec![0, 0, 1],
        };
        let v1_dup = OffsetFetchRequest::error_response(
            1,
            std::slice::from_ref(&OffsetFetchGroup::new("g", Some(vec![dup_part.clone()]))),
            16,
        )
        .unwrap();
        assert_eq!(
            v1_dup.first().map(|g| g.topics.as_slice()),
            Some(
                [FetchedOffsetTopic {
                    topic: "t".into(),
                    partitions: vec![FetchedOffset::error(0, 16), FetchedOffset::error(1, 16)],
                }]
                .as_slice()
            ),
            "v1 duplicate (topic, partition) is unique"
        );
        assert_eq!(
            OffsetFetchTopic {
                topic: "t".into(),
                partitions: vec![0, 0, 1],
            }
            .error_result(1, 16)
            .partitions
            .len(),
            3,
            "error_result keeps duplicate partitions"
        );
        let v1_drop =
            OffsetFetchRequest::error_response(1, &[named.clone(), extra.clone()], 16).unwrap();
        assert_eq!(v1_drop.len(), 1, "below v8 extra groups are dropped");

        let v2 = OffsetFetchRequest::error_response(2, std::slice::from_ref(&named), 16).unwrap();
        assert_eq!(v2, vec![OffsetFetchGroupResult::error("g", 16)]);
        assert!(v2.first().is_some_and(|g| g.topics.is_empty()));
        let v2_all = OffsetFetchRequest::error_response(2, std::slice::from_ref(&all), 16).unwrap();
        assert_eq!(v2_all, vec![OffsetFetchGroupResult::error("g", 16)]);

        let v8 =
            OffsetFetchRequest::error_response(8, &[named.clone(), extra.clone()], 16).unwrap();
        assert_eq!(
            v8,
            vec![
                OffsetFetchGroupResult::error("g", 16),
                OffsetFetchGroupResult::error("h", 16),
            ]
        );
        let dup_id =
            OffsetFetchRequest::error_response(8, &[named.clone(), all.clone()], 16).unwrap();
        assert_eq!(
            dup_id,
            vec![OffsetFetchGroupResult::error("g", 16)],
            "v8+ duplicate GroupId is unique"
        );
        assert_eq!(
            OffsetFetchGroup::error_results(&[named.clone(), all.clone()], 16).len(),
            2,
            "error_results keeps duplicate ids"
        );
        assert!(OffsetFetchRequest::error_response(8, &[], 16)
            .unwrap()
            .is_empty());

        for version in [1_i16, 2, 7] {
            let got = OffsetFetchRequest::error_response(version, std::slice::from_ref(&named), 16)
                .unwrap();
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_response(&mut buf, version, &got).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            if version < 2 {
                assert_eq!(
                    decoded.first().map(|g| g.topics.as_slice()),
                    got.first().map(|g| g.topics.as_slice())
                );
                assert_eq!(
                    decoded.first().map(|g| g.error_code),
                    Some(0),
                    "v1 has no top-level ErrorCode on the wire"
                );
            } else {
                assert_eq!(
                    decoded.first().map(|g| g.error_code),
                    got.first().map(|g| g.error_code)
                );
                assert!(
                    decoded.first().is_some_and(|g| g.topics.is_empty()),
                    "v2–v7 omit partitions"
                );
                assert_eq!(
                    decoded.first().map(|g| g.group_id.as_str()),
                    Some(""),
                    "v1–v7 have no Groups on the wire"
                );
            }
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} Request.getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [8_i16, 9] {
            let got =
                OffsetFetchRequest::error_response(version, &[named.clone(), extra.clone()], 16)
                    .unwrap();
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_response(&mut buf, version, &got).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            assert_eq!(decoded, got);
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} Request.getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [8_i16, 9] {
            let got = OffsetFetchRequest::error_response(version, &[], 16).unwrap();
            assert!(got.is_empty());
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_response(&mut buf, version, &got).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            assert!(decoded.is_empty());
            assert!(
                !cur.has_remaining(),
                "OffsetFetch v{version} Request.getErrorResponse empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn offset_fetch_group_error_result_leftover_empty() {
        let topic = OffsetFetchTopic {
            topic: "t".into(),
            partitions: vec![0, 3],
        };
        let groups = [
            OffsetFetchGroup::new("a", Some(vec![topic.clone()])),
            OffsetFetchGroup::new("b", Some(vec![topic])),
        ];
        let err = OffsetFetchGroup::error_results(&groups, crate::error::NOT_COORDINATOR);
        assert_eq!(
            err,
            vec![
                OffsetFetchGroupResult::error("a", crate::error::NOT_COORDINATOR),
                OffsetFetchGroupResult::error("b", crate::error::NOT_COORDINATOR),
            ]
        );
        assert!(
            err.iter().all(|g| g.topics.is_empty()),
            "v8+ getErrorResponse does not copy request partitions"
        );
        assert_eq!(
            groups
                .first()
                .map(|g| g.error_result(crate::error::NOT_COORDINATOR)),
            err.first().cloned()
        );

        for version in [8, 9] {
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_response(&mut buf, version, &err).unwrap();
            let mut cur = buf.as_ref();
            let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            assert_eq!(decoded, err);
            assert!(
                cur.is_empty(),
                "OffsetFetch getErrorResponse v{version} leftover-empty; leftover {} bytes",
                cur.len()
            );
        }

        let mut v8 = BytesMut::new();
        encode_offset_fetch_groups_response(&mut v8, 8, &err).unwrap();
        let mut v9 = BytesMut::new();
        encode_offset_fetch_groups_response(&mut v9, 9, &err).unwrap();
        assert_eq!(&v8[..], &v9[..], "OffsetFetch v9 response matches v8");
        // Throttle 0, compact Groups of 2 ("a", "b"), empty Topics, NOT_COORDINATOR.
        const TWO: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x03, 0x02, 0x61, 0x01, 0x00, 0x10, 0x00, 0x02, 0x62, 0x01,
            0x00, 0x10, 0x00, 0x00,
        ];
        assert_eq!(&v8[..], TWO);

        let empty = OffsetFetchGroup::error_results(&[], crate::error::NOT_COORDINATOR);
        assert!(empty.is_empty());
        let mut buf = BytesMut::new();
        encode_offset_fetch_groups_response(&mut buf, 8, &empty).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 8).unwrap();
        assert!(decoded.is_empty());
        assert!(
            cur.is_empty(),
            "OffsetFetch getErrorResponse n=0 leftover-empty; leftover {} bytes",
            cur.len()
        );
        const ZERO: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        assert_eq!(&buf[..], ZERO);
    }

    #[test]
    fn offset_fetch_throttle_time_ms_matches_java() {
        let groups = [OffsetFetchGroupResult::error("g", 16)];
        for version in [3_i16, 4, 6, 7, 8, 9] {
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_response_with_throttle(
                &mut buf, version, &groups, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            if version >= 8 {
                assert_eq!(decoded, groups);
            } else {
                assert_eq!(
                    decoded,
                    vec![OffsetFetchGroupResult {
                        group_id: String::new(),
                        topics: Vec::new(),
                        error_code: 16,
                    }]
                );
            }
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "OffsetFetch v{version} ThrottleTimeMs leftover-empty"
            );
        }

        for version in [1_i16, 2] {
            let mut buf = BytesMut::new();
            encode_offset_fetch_groups_response_with_throttle(
                &mut buf, version, &groups, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            assert!(
                cur.is_empty(),
                "OffsetFetch v{version} ThrottleTimeMs leftover-empty"
            );
            assert_eq!(
                throttle, 0,
                "OffsetFetch v{version} omits ThrottleTimeMs even when the body has a non-zero value"
            );
            if version == 1 {
                assert_eq!(decoded.first().map(|g| g.error_code), Some(0));
            } else {
                assert_eq!(decoded.first().map(|g| g.error_code), Some(16));
            }
        }

        let mut with = BytesMut::new();
        encode_offset_fetch_groups_response_with_throttle(&mut with, 3, &groups, 3_600_000)
            .unwrap();
        let mut zero = BytesMut::new();
        encode_offset_fetch_groups_response_with_throttle(&mut zero, 3, &groups, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v3 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_offset_fetch_groups_response(&mut conv, 3, &groups).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_offset_fetch_groups_response still writes ThrottleTimeMs 0"
        );
        let mut v2_with = BytesMut::new();
        encode_offset_fetch_groups_response_with_throttle(&mut v2_with, 2, &groups, 3_600_000)
            .unwrap();
        let mut v2_zero = BytesMut::new();
        encode_offset_fetch_groups_response_with_throttle(&mut v2_zero, 2, &groups, 0).unwrap();
        assert_eq!(
            &v2_with[..],
            &v2_zero[..],
            "v2 encode omits ThrottleTimeMs even when the body has a non-zero value"
        );
        assert_ne!(
            &v2_with[..],
            &with[..],
            "v3 adds ThrottleTimeMs before Topics"
        );

        for version in [2_i16, 3, 8] {
            let named = OffsetFetchGroup::new("g", Some(offset_fetch_one_topic()));
            let err = OffsetFetchRequest::error_response(version, std::slice::from_ref(&named), 16)
                .unwrap();
            let mut expected = BytesMut::new();
            encode_offset_fetch_groups_response_with_throttle(
                &mut expected,
                version,
                &err,
                3_600_000,
            )
            .unwrap();
            let mut got = BytesMut::new();
            encode_offset_fetch_groups_response_with_throttle(
                &mut got, version, &groups, 3_600_000,
            )
            .unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "OffsetFetch v{version} getErrorResponse must match with_throttle encode"
            );
            let mut cur = got.as_ref();
            let (_, throttle) = decode_offset_fetch_groups_response(&mut cur, version).unwrap();
            if version >= 3 {
                assert_eq!(throttle, 3_600_000);
            } else {
                assert_eq!(throttle, 0);
            }
            assert!(
                cur.is_empty(),
                "OffsetFetch v{version} getErrorResponse leftover-empty"
            );
        }
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
        let (decoded, ..) = decode_offset_fetch_groups_response(&mut cur, 8).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            cur.is_empty(),
            "v8 Groups-of-2 response leftover-empty; leftover {} bytes",
            cur.len()
        );

        let err = encode_offset_fetch_groups_request(&mut BytesMut::new(), 7, &groups, false)
            .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "two groups on v7 is Java NoBatchedOffsetFetchRequestException, got {err}"
        );
        assert!(err.to_string().contains("batching groups"), "got {err}");

        let err = encode_offset_fetch_groups_response(&mut BytesMut::new(), 7, &resp).unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "two groups on v7 response is Java UnsupportedVersionException, got {err}"
        );
        assert!(
            err.to_string().contains("only supports one group"),
            "got {err}"
        );
    }

    #[test]
    fn offset_fetch_builder_matches_java() {
        let err = encode_offset_fetch_request(&mut BytesMut::new(), 1, "g", None, -1, false, None)
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)), "got {err}");
        let one = [OffsetFetchGroup::new("g", None)];
        let err =
            encode_offset_fetch_groups_request(&mut BytesMut::new(), 7, &one, false).unwrap_err();
        assert!(
            err.to_string().contains("does not support Groups"),
            "single group below v8 stays on encode_offset_fetch_request, got {err}"
        );
        encode_offset_fetch_groups_request(&mut BytesMut::new(), 8, &one, false).unwrap();
        encode_offset_fetch_groups_response(&mut BytesMut::new(), 7, &[]).unwrap();
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
        let (gid, gen, mid, ..) = decode_heartbeat_request(&mut cur, 0).unwrap();
        assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
        assert!(cur.is_empty(), "v0 request leftover-empty");
        let mut cur = v2.as_ref();
        let (_gid, _gen, mid, ..) = decode_heartbeat_request(&mut cur, 2).unwrap();
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
        assert_eq!(decode_heartbeat_response(&mut cur, 0).unwrap().0, 0);
        assert!(cur.is_empty(), "v0 response leftover-empty");
        let mut cur = v1.as_ref();
        assert_eq!(decode_heartbeat_response(&mut cur, 1).unwrap().0, 0);
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
        let (gid, gen, mid, ..) = decode_heartbeat_request(&mut cur, 3).unwrap();
        assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
        assert!(
            cur.is_empty(),
            "v3 decoder must consume instance id; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_heartbeat_response(&mut buf, 3, 0).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_heartbeat_response(&mut cur, 3).unwrap().0, 0);
        assert!(cur.is_empty(), "v3 response leftover {} bytes", cur.len());
    }

    #[test]
    fn heartbeat_v4_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_heartbeat_request(&mut req, 4, "g", 7, "m1", Some("i")).unwrap();
        let mut cur = &req[..];
        let (gid, gen, mid, ..) = decode_heartbeat_request(&mut cur, 4).unwrap();
        assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
        assert!(
            cur.is_empty(),
            "v4 decoder must consume compact strings and tagged fields; leftover {} bytes",
            cur.len()
        );

        let mut resp = BytesMut::new();
        encode_heartbeat_response(&mut resp, 4, 0).unwrap();
        let mut cur = &resp[..];
        assert_eq!(decode_heartbeat_response(&mut cur, 4).unwrap().0, 0);
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

    #[test]
    fn heartbeat_request_build_matches_java() {
        HeartbeatRequest::build(2, None).unwrap();
        HeartbeatRequest::build(3, None).unwrap();
        HeartbeatRequest::build(3, Some("i")).unwrap();
        HeartbeatRequest::build(3, Some("")).unwrap();
        HeartbeatRequest::build(4, Some("i")).unwrap();
        let v2 = HeartbeatRequest::build(2, Some("i")).unwrap_err();
        assert!(
            matches!(v2, Error::Unsupported(_)),
            "v2 with group.instance.id is Java UnsupportedVersionException, got {v2}"
        );
        assert!(
            v2.to_string()
                .contains("does not support usage of config group.instance.id"),
            "got {v2}"
        );
        let empty = HeartbeatRequest::build(0, Some("")).unwrap_err();
        assert!(
            matches!(empty, Error::Unsupported(_)),
            "empty group.instance.id is still present (Java != null), got {empty}"
        );
        encode_heartbeat_request(&mut BytesMut::new(), 0, "g", 7, "m1", Some("ignored-on-v0"))
            .unwrap();
        assert!(
            HeartbeatRequest::build(0, Some("ignored-on-v0")).is_err(),
            "encode omits group.instance.id below v3; Builder.build rejects it"
        );

        for version in [3_i16, 4] {
            HeartbeatRequest::build(version, Some("i")).unwrap();
            let mut buf = BytesMut::new();
            encode_heartbeat_request(&mut buf, version, "g", 7, "m1", Some("i")).unwrap();
            let mut cur = buf.as_ref();
            let (gid, gen, mid, ..) = decode_heartbeat_request(&mut cur, version).unwrap();
            assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
            assert!(
                !cur.has_remaining(),
                "Heartbeat v{version} Builder.build leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        for version in [0_i16, 2, 3, 4] {
            HeartbeatRequest::build(version, None).unwrap();
            let mut buf = BytesMut::new();
            encode_heartbeat_request(&mut buf, version, "g", 7, "m1", None).unwrap();
            let mut cur = buf.as_ref();
            let (gid, gen, mid, ..) = decode_heartbeat_request(&mut cur, version).unwrap();
            assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
            assert!(
                !cur.has_remaining(),
                "Heartbeat v{version} Builder.build empty leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
    }

    #[test]
    fn heartbeat_group_instance_id_matches_java() {
        for version in [3_i16, 4] {
            let mut buf = BytesMut::new();
            encode_heartbeat_request(&mut buf, version, "g", 7, "m1", Some("i")).unwrap();
            let mut cur = buf.as_ref();
            let (gid, gen, mid, inst) = decode_heartbeat_request(&mut cur, version).unwrap();
            assert_eq!((gid.as_str(), gen, mid.as_str()), ("g", 7, "m1"));
            assert_eq!(inst.as_deref(), Some("i"));
            assert!(
                cur.is_empty(),
                "Heartbeat v{version} GroupInstanceId leftover-empty"
            );
        }

        let mut buf = BytesMut::new();
        encode_heartbeat_request(&mut buf, 0, "g", 7, "m1", Some("i")).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, inst) = decode_heartbeat_request(&mut cur, 0).unwrap();
        assert!(
            cur.is_empty(),
            "Heartbeat v0 GroupInstanceId leftover-empty"
        );
        assert_eq!(
            inst, None,
            "Heartbeat v0 omits GroupInstanceId even when the body has an instance id"
        );

        let mut buf = BytesMut::new();
        encode_heartbeat_request(&mut buf, 2, "g", 7, "m1", Some("i")).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, inst) = decode_heartbeat_request(&mut cur, 2).unwrap();
        assert!(
            cur.is_empty(),
            "Heartbeat v2 GroupInstanceId leftover-empty"
        );
        assert_eq!(
            inst, None,
            "Heartbeat v2 omits GroupInstanceId even when the body has an instance id"
        );

        let mut with = BytesMut::new();
        encode_heartbeat_request(&mut with, 3, "g", 7, "m1", Some("i")).unwrap();
        let mut none = BytesMut::new();
        encode_heartbeat_request(&mut none, 3, "g", 7, "m1", None).unwrap();
        assert_ne!(
            &with[..],
            &none[..],
            "v3 GroupInstanceId is not always the JSON default null"
        );
        let mut v0_with = BytesMut::new();
        encode_heartbeat_request(&mut v0_with, 0, "g", 7, "m1", Some("i")).unwrap();
        let mut v0_none = BytesMut::new();
        encode_heartbeat_request(&mut v0_none, 0, "g", 7, "m1", None).unwrap();
        assert_eq!(
            &v0_with[..],
            &v0_none[..],
            "v0 encode omits GroupInstanceId even when the body has an instance id"
        );
        let mut v2_with = BytesMut::new();
        encode_heartbeat_request(&mut v2_with, 2, "g", 7, "m1", Some("i")).unwrap();
        assert_eq!(
            &v0_with[..],
            &v2_with[..],
            "v0–v2 Heartbeat requests omit GroupInstanceId"
        );
        assert_ne!(
            &v2_with[..],
            &with[..],
            "v3 adds GroupInstanceId after MemberId"
        );

        let mut empty = BytesMut::new();
        encode_heartbeat_request(&mut empty, 3, "g", 7, "m1", Some("")).unwrap();
        let mut cur = empty.as_ref();
        let (_, _, _, inst) = decode_heartbeat_request(&mut cur, 3).unwrap();
        assert_eq!(inst.as_deref(), Some(""));
        assert!(
            cur.is_empty(),
            "Heartbeat v3 empty GroupInstanceId leftover-empty"
        );
        assert_ne!(
            &empty[..],
            &none[..],
            "empty GroupInstanceId is still present (Java != null)"
        );
    }

    #[test]
    fn heartbeat_throttle_time_ms_matches_java() {
        for version in [1_i16, 2, 3, 4] {
            let mut buf = BytesMut::new();
            encode_heartbeat_response_with_throttle(&mut buf, version, 16, 3_600_000).unwrap();
            let mut cur = buf.as_ref();
            let (err, throttle) = decode_heartbeat_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "Heartbeat v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut buf = BytesMut::new();
        encode_heartbeat_response_with_throttle(&mut buf, 0, 16, 3_600_000).unwrap();
        let mut cur = buf.as_ref();
        let (err, throttle) = decode_heartbeat_response(&mut cur, 0).unwrap();
        assert_eq!(err, 16);
        assert!(cur.is_empty(), "Heartbeat v0 ThrottleTimeMs leftover-empty");
        assert_eq!(
            throttle, 0,
            "Heartbeat v0 omits ThrottleTimeMs even when the body has a non-zero value"
        );

        let mut with = BytesMut::new();
        encode_heartbeat_response_with_throttle(&mut with, 1, 16, 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_heartbeat_response_with_throttle(&mut zero, 1, 16, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v1 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_heartbeat_response(&mut conv, 1, 16).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_heartbeat_response still writes ThrottleTimeMs 0"
        );
        let mut v0_with = BytesMut::new();
        encode_heartbeat_response_with_throttle(&mut v0_with, 0, 16, 3_600_000).unwrap();
        let mut v0_zero = BytesMut::new();
        encode_heartbeat_response_with_throttle(&mut v0_zero, 0, 16, 0).unwrap();
        assert_eq!(
            &v0_with[..],
            &v0_zero[..],
            "v0 encode omits ThrottleTimeMs even when the body has a non-zero value"
        );
        assert_ne!(
            &v0_with[..],
            &with[..],
            "v1 adds ThrottleTimeMs before ErrorCode"
        );

        for version in [0_i16, 1, 4] {
            let mut expected = BytesMut::new();
            encode_heartbeat_response_with_throttle(&mut expected, version, 16, 3_600_000).unwrap();
            let mut got = BytesMut::new();
            HeartbeatRequest::error_response(&mut got, version, 16, 3_600_000).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "Heartbeat v{version} getErrorResponse must match with_throttle encode"
            );
            let mut cur = got.as_ref();
            let (err, throttle) = decode_heartbeat_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            if version >= 1 {
                assert_eq!(throttle, 3_600_000);
            } else {
                assert_eq!(throttle, 0);
            }
            assert!(
                cur.is_empty(),
                "Heartbeat v{version} getErrorResponse leftover-empty"
            );
        }
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
        let (gid, mid, got, ..) = decode_sync_group_request(&mut cur, 0).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert!(got.is_empty());
        assert!(cur.is_empty(), "v0 request leftover-empty");
        let mut cur = v2.as_ref();
        let (_gid, mid, _got, ..) = decode_sync_group_request(&mut cur, 2).unwrap();
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
        let (err, asg, ..) = decode_sync_group_response(&mut cur, 0).unwrap();
        assert_eq!((err, asg.as_slice()), (0, &[][..]));
        assert!(cur.is_empty(), "v0 response leftover-empty");
        let mut cur = v1.as_ref();
        let (err, asg, ..) = decode_sync_group_response(&mut cur, 1).unwrap();
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
        let (gid, mid, got, ..) = decode_sync_group_request(&mut cur, 3).unwrap();
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
        let (err, asg, ..) = decode_sync_group_response(&mut cur, 3).unwrap();
        assert_eq!((err, asg.as_slice()), (0, &[1, 2, 3][..]));
        assert!(cur.is_empty(), "v3 response leftover {} bytes", cur.len());
    }

    #[test]
    fn sync_group_v4_roundtrip_is_leftover_empty() {
        let assignments = vec![("m1".into(), vec![1, 2, 3])];
        let mut req = BytesMut::new();
        encode_sync_group_request(&mut req, 4, &sync_req(&assignments)).unwrap();
        let mut cur = &req[..];
        let (gid, mid, got, ..) = decode_sync_group_request(&mut cur, 4).unwrap();
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
        let (err, asg, ..) = decode_sync_group_response(&mut cur, 4).unwrap();
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
        let (gid, mid, got, ..) = decode_sync_group_request(&mut cur, 5).unwrap();
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
        let (err, asg, ..) = decode_sync_group_response(&mut cur, 5).unwrap();
        assert_eq!((err, asg.as_slice()), (0, &[1, 2, 3][..]));
        assert!(
            cur.is_empty(),
            "v5 response must consume ProtocolType / ProtocolName; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn sync_group_group_instance_id_matches_java() {
        let empty: [(String, Vec<u8>); 0] = [];
        for version in [3_i16, 4, 5] {
            let mut req = sync_req(&empty);
            req.group_instance_id = Some("i");
            let mut buf = BytesMut::new();
            encode_sync_group_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let (gid, mid, got, inst, ..) = decode_sync_group_request(&mut cur, version).unwrap();
            assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
            assert!(got.is_empty());
            assert_eq!(inst.as_deref(), Some("i"));
            assert!(
                cur.is_empty(),
                "SyncGroup v{version} GroupInstanceId leftover-empty"
            );
        }

        let mut req = sync_req(&empty);
        req.group_instance_id = Some("i");
        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 0, &req).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, inst, ..) = decode_sync_group_request(&mut cur, 0).unwrap();
        assert!(
            cur.is_empty(),
            "SyncGroup v0 GroupInstanceId leftover-empty"
        );
        assert_eq!(
            inst, None,
            "SyncGroup v0 omits GroupInstanceId even when the body has an instance id"
        );

        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 2, &req).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, inst, ..) = decode_sync_group_request(&mut cur, 2).unwrap();
        assert!(
            cur.is_empty(),
            "SyncGroup v2 GroupInstanceId leftover-empty"
        );
        assert_eq!(
            inst, None,
            "SyncGroup v2 omits GroupInstanceId even when the body has an instance id"
        );

        let mut with = BytesMut::new();
        encode_sync_group_request(&mut with, 3, &req).unwrap();
        let none_req = sync_req(&empty);
        let mut none = BytesMut::new();
        encode_sync_group_request(&mut none, 3, &none_req).unwrap();
        assert_ne!(
            &with[..],
            &none[..],
            "v3 GroupInstanceId is not always the JSON default null"
        );
        let mut v0_with = BytesMut::new();
        encode_sync_group_request(&mut v0_with, 0, &req).unwrap();
        let mut v0_none = BytesMut::new();
        encode_sync_group_request(&mut v0_none, 0, &none_req).unwrap();
        assert_eq!(
            &v0_with[..],
            &v0_none[..],
            "v0 encode omits GroupInstanceId even when the body has an instance id"
        );
        let mut v2_with = BytesMut::new();
        encode_sync_group_request(&mut v2_with, 2, &req).unwrap();
        assert_eq!(
            &v0_with[..],
            &v2_with[..],
            "v0–v2 SyncGroup requests omit GroupInstanceId"
        );
        assert_ne!(
            &v2_with[..],
            &with[..],
            "v3 adds GroupInstanceId after MemberId"
        );

        let mut empty_id = sync_req(&empty);
        empty_id.group_instance_id = Some("");
        let mut empty_buf = BytesMut::new();
        encode_sync_group_request(&mut empty_buf, 3, &empty_id).unwrap();
        let mut cur = empty_buf.as_ref();
        let (_, _, _, inst, ..) = decode_sync_group_request(&mut cur, 3).unwrap();
        assert_eq!(inst.as_deref(), Some(""));
        assert!(
            cur.is_empty(),
            "SyncGroup v3 empty GroupInstanceId leftover-empty"
        );
        assert_ne!(
            &empty_buf[..],
            &none[..],
            "empty GroupInstanceId is still present (Java != null)"
        );
    }

    #[test]
    fn sync_group_generation_id_matches_java() {
        // Kafka 4.0.0 SyncGroupRequest.json GenerationId is versions 0+
        // (INT32 after GroupId / before MemberId on spoken v0–v5). Official
        // Java SyncGroupRequestData.generationId. Encode already writes
        // generation_id. Decode previously discarded it. This crate
        // speaks 0–5. This is not OffsetCommit GenerationId / Heartbeat
        // GenerationId / JoinGroup response GenerationId / GroupInstanceId.
        let empty: [(String, Vec<u8>); 0] = [];
        for version in [0_i16, 1, 2, 3, 4, 5] {
            let req = sync_req(&empty);
            let mut buf = BytesMut::new();
            encode_sync_group_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let (gid, mid, got, .., gen) = decode_sync_group_request(&mut cur, version).unwrap();
            assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
            assert!(got.is_empty());
            assert_eq!(gen, 7);
            assert!(
                cur.is_empty(),
                "SyncGroup v{version} GenerationId leftover-empty"
            );
        }

        let mut with = sync_req(&empty);
        with.generation_id = 7;
        let mut other = sync_req(&empty);
        other.generation_id = 1;
        let mut seven = BytesMut::new();
        encode_sync_group_request(&mut seven, 0, &with).unwrap();
        let mut one = BytesMut::new();
        encode_sync_group_request(&mut one, 0, &other).unwrap();
        assert_ne!(
            &seven[..],
            &one[..],
            "v0 GenerationId is not always the same INT32"
        );
        let mut cur = seven.as_ref();
        let (.., gen) = decode_sync_group_request(&mut cur, 0).unwrap();
        assert_eq!(gen, 7);
        assert!(cur.is_empty(), "SyncGroup v0 GenerationId leftover-empty");
        let mut cur = one.as_ref();
        let (.., gen) = decode_sync_group_request(&mut cur, 0).unwrap();
        assert_eq!(gen, 1);
        assert_eq!(
            seven.get(3..7),
            Some([0, 0, 0, 7].as_slice()),
            "v0 classic GenerationId follows GroupId STRING g"
        );

        let mut v1 = BytesMut::new();
        encode_sync_group_request(&mut v1, 1, &with).unwrap();
        assert_eq!(
            &seven[..],
            &v1[..],
            "empty-Assignments GenerationId bodies: v0 == v1"
        );
        let mut v2 = BytesMut::new();
        encode_sync_group_request(&mut v2, 2, &with).unwrap();
        assert_eq!(
            &v1[..],
            &v2[..],
            "empty-Assignments GenerationId bodies: v1 == v2"
        );
        let mut v3 = BytesMut::new();
        encode_sync_group_request(&mut v3, 3, &with).unwrap();
        assert_ne!(&v2[..], &v3[..], "v3 adds GroupInstanceId after MemberId");
        let mut v4 = BytesMut::new();
        encode_sync_group_request(&mut v4, 4, &with).unwrap();
        assert_ne!(&v3[..], &v4[..], "v4 adds compact tagged fields");
        let mut v5 = BytesMut::new();
        encode_sync_group_request(&mut v5, 5, &with).unwrap();
        assert_ne!(
            &v4[..],
            &v5[..],
            "v5 adds ProtocolType / ProtocolName after GroupInstanceId"
        );
    }

    #[test]
    fn sync_group_protocol_type_name_matches_java() {
        let empty: [(String, Vec<u8>); 0] = [];
        let req = sync_req(&empty);
        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 5, &req).unwrap();
        let mut cur = buf.as_ref();
        let (gid, mid, got, _, ptype, pname, ..) = decode_sync_group_request(&mut cur, 5).unwrap();
        assert_eq!((gid.as_str(), mid.as_str()), ("g", "m1"));
        assert!(got.is_empty());
        assert_eq!(ptype.as_deref(), Some("consumer"));
        assert_eq!(pname.as_deref(), Some("range"));
        assert!(cur.is_empty(), "SyncGroup v5 ProtocolType leftover-empty");

        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 4, &req).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, _, ptype, pname, ..) = decode_sync_group_request(&mut cur, 4).unwrap();
        assert!(cur.is_empty(), "SyncGroup v4 ProtocolType leftover-empty");
        assert_eq!(
            (ptype, pname),
            (None, None),
            "SyncGroup v4 omits ProtocolType / ProtocolName even when the body has values"
        );

        let mut buf = BytesMut::new();
        encode_sync_group_request(&mut buf, 0, &req).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, _, ptype, pname, ..) = decode_sync_group_request(&mut cur, 0).unwrap();
        assert!(cur.is_empty(), "SyncGroup v0 ProtocolType leftover-empty");
        assert_eq!(
            (ptype, pname),
            (None, None),
            "SyncGroup v0 omits ProtocolType / ProtocolName even when the body has values"
        );

        let mut other = sync_req(&empty);
        other.protocol_type = "sticky";
        other.protocol_name = "cooperative-sticky";
        let mut v5_consumer = BytesMut::new();
        encode_sync_group_request(&mut v5_consumer, 5, &req).unwrap();
        let mut v5_sticky = BytesMut::new();
        encode_sync_group_request(&mut v5_sticky, 5, &other).unwrap();
        assert_ne!(
            &v5_consumer[..],
            &v5_sticky[..],
            "v5 ProtocolType / ProtocolName are not always the JSON default null"
        );
        let mut v4_consumer = BytesMut::new();
        encode_sync_group_request(&mut v4_consumer, 4, &req).unwrap();
        let mut v4_sticky = BytesMut::new();
        encode_sync_group_request(&mut v4_sticky, 4, &other).unwrap();
        assert_eq!(
            &v4_consumer[..],
            &v4_sticky[..],
            "v4 encode omits ProtocolType / ProtocolName even when the body has values"
        );
        assert_ne!(
            &v4_consumer[..],
            &v5_consumer[..],
            "v5 adds ProtocolType / ProtocolName after GroupInstanceId"
        );

        let mut empty_proto = sync_req(&empty);
        empty_proto.protocol_type = "";
        empty_proto.protocol_name = "";
        let mut empty_buf = BytesMut::new();
        encode_sync_group_request(&mut empty_buf, 5, &empty_proto).unwrap();
        let mut cur = empty_buf.as_ref();
        let (_, _, _, _, ptype, pname, ..) = decode_sync_group_request(&mut cur, 5).unwrap();
        assert_eq!(ptype.as_deref(), Some(""));
        assert_eq!(pname.as_deref(), Some(""));
        assert!(
            cur.is_empty(),
            "SyncGroup v5 empty ProtocolType leftover-empty"
        );
        assert_ne!(
            &empty_buf[..],
            &v5_consumer[..],
            "empty ProtocolType / ProtocolName are still present (Java != null)"
        );
    }

    #[test]
    fn sync_group_response_protocol_type_name_matches_java() {
        let assignment = b"asg";
        let mut buf = BytesMut::new();
        encode_sync_group_response_with_protocol(
            &mut buf,
            5,
            0,
            assignment,
            Some("consumer"),
            Some("range"),
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (err, asg, ptype, pname, ..) = decode_sync_group_response(&mut cur, 5).unwrap();
        assert_eq!(err, 0);
        assert_eq!(asg, assignment);
        assert_eq!(ptype.as_deref(), Some("consumer"));
        assert_eq!(pname.as_deref(), Some("range"));
        assert!(
            cur.is_empty(),
            "SyncGroup v5 response ProtocolType leftover-empty"
        );

        let mut buf = BytesMut::new();
        encode_sync_group_response_with_protocol(
            &mut buf,
            4,
            0,
            assignment,
            Some("consumer"),
            Some("range"),
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (_, asg, ptype, pname, ..) = decode_sync_group_response(&mut cur, 4).unwrap();
        assert_eq!(asg, assignment);
        assert!(
            cur.is_empty(),
            "SyncGroup v4 response ProtocolType leftover-empty"
        );
        assert_eq!(
            (ptype, pname),
            (None, None),
            "SyncGroup v4 omits response ProtocolType / ProtocolName even when the body has values"
        );

        let mut buf = BytesMut::new();
        encode_sync_group_response_with_protocol(
            &mut buf,
            0,
            0,
            assignment,
            Some("consumer"),
            Some("range"),
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (_, _, ptype, pname, ..) = decode_sync_group_response(&mut cur, 0).unwrap();
        assert!(
            cur.is_empty(),
            "SyncGroup v0 response ProtocolType leftover-empty"
        );
        assert_eq!(
            (ptype, pname),
            (None, None),
            "SyncGroup v0 omits response ProtocolType / ProtocolName even when the body has values"
        );

        let mut with = BytesMut::new();
        encode_sync_group_response_with_protocol(
            &mut with,
            5,
            0,
            assignment,
            Some("consumer"),
            Some("range"),
        )
        .unwrap();
        let mut none = BytesMut::new();
        encode_sync_group_response_with_protocol(&mut none, 5, 0, assignment, None, None).unwrap();
        assert_ne!(
            &with[..],
            &none[..],
            "v5 response ProtocolType / ProtocolName are not always the JSON default null"
        );
        let mut conv = BytesMut::new();
        encode_sync_group_response(&mut conv, 5, 0, assignment).unwrap();
        assert_eq!(
            &conv[..],
            &none[..],
            "encode_sync_group_response still writes null ProtocolType / ProtocolName"
        );
        let mut v4_with = BytesMut::new();
        encode_sync_group_response_with_protocol(
            &mut v4_with,
            4,
            0,
            assignment,
            Some("consumer"),
            Some("range"),
        )
        .unwrap();
        let mut v4_none = BytesMut::new();
        encode_sync_group_response_with_protocol(&mut v4_none, 4, 0, assignment, None, None)
            .unwrap();
        assert_eq!(
            &v4_with[..],
            &v4_none[..],
            "v4 encode omits response ProtocolType / ProtocolName even when the body has values"
        );
        assert_ne!(
            &v4_with[..],
            &with[..],
            "v5 adds response ProtocolType / ProtocolName after ErrorCode"
        );

        let mut empty_buf = BytesMut::new();
        encode_sync_group_response_with_protocol(
            &mut empty_buf,
            5,
            0,
            assignment,
            Some(""),
            Some(""),
        )
        .unwrap();
        let mut cur = empty_buf.as_ref();
        let (_, _, ptype, pname, ..) = decode_sync_group_response(&mut cur, 5).unwrap();
        assert_eq!(ptype.as_deref(), Some(""));
        assert_eq!(pname.as_deref(), Some(""));
        assert!(
            cur.is_empty(),
            "SyncGroup v5 empty response ProtocolType leftover-empty"
        );
        assert_ne!(
            &empty_buf[..],
            &none[..],
            "empty response ProtocolType / ProtocolName are still present (Java != null)"
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
    fn sync_group_throttle_time_ms_matches_java() {
        for version in [1_i16, 2, 3, 4, 5] {
            let mut buf = BytesMut::new();
            encode_sync_group_response_with_throttle(&mut buf, version, 16, &[], 3_600_000)
                .unwrap();
            let mut cur = buf.as_ref();
            let (err, asg, ptype, pname, throttle) =
                decode_sync_group_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert!(asg.is_empty());
            assert_eq!((ptype, pname), (None, None));
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "SyncGroup v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut buf = BytesMut::new();
        encode_sync_group_response_with_throttle(&mut buf, 0, 16, &[], 3_600_000).unwrap();
        let mut cur = buf.as_ref();
        let (err, _, _, _, throttle) = decode_sync_group_response(&mut cur, 0).unwrap();
        assert_eq!(err, 16);
        assert!(cur.is_empty(), "SyncGroup v0 ThrottleTimeMs leftover-empty");
        assert_eq!(
            throttle, 0,
            "SyncGroup v0 omits ThrottleTimeMs even when the body has a non-zero value"
        );

        let mut with = BytesMut::new();
        encode_sync_group_response_with_throttle(&mut with, 1, 16, &[], 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_sync_group_response_with_throttle(&mut zero, 1, 16, &[], 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v1 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_sync_group_response(&mut conv, 1, 16, &[]).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_sync_group_response still writes ThrottleTimeMs 0"
        );
        let mut v0_with = BytesMut::new();
        encode_sync_group_response_with_throttle(&mut v0_with, 0, 16, &[], 3_600_000).unwrap();
        let mut v0_zero = BytesMut::new();
        encode_sync_group_response_with_throttle(&mut v0_zero, 0, 16, &[], 0).unwrap();
        assert_eq!(
            &v0_with[..],
            &v0_zero[..],
            "v0 encode omits ThrottleTimeMs even when the body has a non-zero value"
        );
        assert_ne!(
            &v0_with[..],
            &with[..],
            "v1 adds ThrottleTimeMs before ErrorCode"
        );

        for version in [0_i16, 1, 4, 5] {
            let mut expected = BytesMut::new();
            encode_sync_group_response_with_throttle(&mut expected, version, 16, &[], 3_600_000)
                .unwrap();
            let mut got = BytesMut::new();
            SyncGroupRequest::error_response(&mut got, version, 16, 3_600_000).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "SyncGroup v{version} getErrorResponse must match with_throttle encode"
            );
            let mut cur = got.as_ref();
            let (err, asg, _, _, throttle) = decode_sync_group_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert!(asg.is_empty(), "v{version} assignment must be empty");
            if version >= 1 {
                assert_eq!(throttle, 3_600_000);
            } else {
                assert_eq!(throttle, 0);
            }
            assert!(
                cur.is_empty(),
                "SyncGroup v{version} getErrorResponse leftover-empty"
            );
        }
    }

    #[test]
    fn sync_group_error_response_matches_java() {
        // Java SyncGroupRequest.getErrorResponse: empty assignment, throttle
        // from the argument (0 here is the JSON default) on v1+. ProtocolType /
        // ProtocolName stay JSON default (null) on v5+.
        const V5: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x01, 0x00];
        for version in [0_i16, 1, 4, 5] {
            let mut expected = BytesMut::new();
            encode_sync_group_response(&mut expected, version, 16, &[]).unwrap();
            let mut got = BytesMut::new();
            SyncGroupRequest::error_response(&mut got, version, 16, 0).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "SyncGroup v{version} getErrorResponse must match empty-assignment encode"
            );
            if version == 5 {
                assert_eq!(&got[..], V5);
            }
            let mut cur = &got[..];
            let (err, assignment, ..) = decode_sync_group_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert!(assignment.is_empty(), "v{version} assignment must be empty");
            assert!(
                cur.is_empty(),
                "SyncGroup v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        let mut v4 = BytesMut::new();
        SyncGroupRequest::error_response(&mut v4, 4, 16, 0).unwrap();
        let mut v5 = BytesMut::new();
        SyncGroupRequest::error_response(&mut v5, 5, 16, 0).unwrap();
        assert_ne!(
            &v4[..],
            &v5[..],
            "v5 getErrorResponse ProtocolType / ProtocolName are null"
        );
        let mut v0 = BytesMut::new();
        SyncGroupRequest::error_response(&mut v0, 0, 16, 0).unwrap();
        let mut v1 = BytesMut::new();
        SyncGroupRequest::error_response(&mut v1, 1, 16, 0).unwrap();
        assert_ne!(&v0[..], &v1[..], "v1+ getErrorResponse includes throttle");
    }

    #[test]
    fn offset_delete_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 OffsetDeleteResponse.json ThrottleTimeMs is versions
        // 0+ (INT32 on spoken v0; second field; ignorable). ErrorCode is
        // first (bytes 0–1); throttle occupies bytes 2–5. Official Java
        // OffsetDeleteRequest.getErrorResponse /
        // OffsetDeleteResponse.throttleTimeMs set / read it.
        // encode_offset_delete_response still writes the JSON default 0.
        // shouldClientThrottle is already v0+. Empty-Topics only one
        // version. This crate speaks 0. This is not JoinGroup /
        // FindCoordinator ThrottleTimeMs.
        let results: Vec<OffsetDeleteResult> = vec![];
        let mut buf = BytesMut::new();
        encode_offset_delete_response_with_throttle(&mut buf, 0, &results, 3_600_000).unwrap();
        let mut cur = buf.as_ref();
        let (err, decoded, throttle) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(err, 0);
        assert_eq!(decoded, results);
        assert_eq!(throttle, 3_600_000);
        assert!(
            cur.is_empty(),
            "OffsetDelete v0 ThrottleTimeMs leftover-empty"
        );

        let mut with = BytesMut::new();
        encode_offset_delete_response_with_throttle(&mut with, 0, &results, 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_offset_delete_response_with_throttle(&mut zero, 0, &results, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_offset_delete_response(&mut conv, 0, &results).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_offset_delete_response still writes ThrottleTimeMs 0"
        );
        assert_eq!(
            &with[0..2],
            &[0, 0],
            "v0 top-level ErrorCode is at bytes 0-1"
        );
        assert_eq!(
            &with[2..6],
            &3_600_000_i32.to_be_bytes(),
            "v0 ThrottleTimeMs occupies bytes 2-5"
        );
    }

    #[test]
    fn offset_delete_v0_roundtrip_is_leftover_empty() {
        let topics = vec![OffsetDeleteTopic::new("t", vec![0, 1])];
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
            OffsetDeleteResult::new("t", 0, 0),
            OffsetDeleteResult::new("t", 1, 0),
        ];
        buf.clear();
        encode_offset_delete_response(&mut buf, 0, &results).unwrap();
        let mut cur = &buf[..];
        let (err, decoded, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(err, 0);
        assert_eq!(decoded, results);
        assert!(
            cur.is_empty(),
            "OffsetDelete v0 response must be leftover-empty"
        );

        let topic = OffsetDeleteTopic::new("t", vec![0, 3]);
        assert_eq!(topic.topic(), "t");
        assert_eq!(topic.partitions(), &[0, 3]);
        let part_err = topic.error_result(crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            part_err,
            vec![
                OffsetDeleteResult::new("t", 0, crate::error::UNKNOWN_TOPIC_OR_PARTITION),
                OffsetDeleteResult::new("t", 3, crate::error::UNKNOWN_TOPIC_OR_PARTITION),
            ]
        );
        let first = part_err.first().expect("error partition");
        assert_eq!(first.topic(), "t");
        assert_eq!(first.partition(), 0);
        assert_eq!(first.error_code(), crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        buf.clear();
        encode_offset_delete_response(&mut buf, 0, &part_err).unwrap();
        let mut cur = buf.as_ref();
        let (top, decoded, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(top, 0);
        assert_eq!(decoded, part_err);
        assert!(
            !cur.has_remaining(),
            "OffsetDelete addPartitions leftover-empty; leftover {} bytes",
            cur.remaining()
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
        let (err, results, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(err, crate::error::NOT_COORDINATOR);
        assert!(results.is_empty());
        assert!(
            !cur.has_remaining(),
            "OffsetDelete v0 NOT_COORDINATOR must be leftover-empty"
        );
    }

    #[test]
    fn offset_delete_error_response_matches_java() {
        // Java OffsetDeleteRequest.getErrorResponse: top-level ErrorCode
        // only. Topics stay empty (Builder.addPartitions is a different
        // path). Throttle JSON default 0. ErrorCode is before throttle.
        let mut expected = BytesMut::new();
        encode_offset_delete_response(&mut expected, 16, &[]).unwrap();
        let mut got = BytesMut::new();
        OffsetDeleteRequest::error_response(&mut got, 16).unwrap();
        assert_eq!(
            &got[..],
            &expected[..],
            "OffsetDelete getErrorResponse must match empty-Topics encode"
        );
        let mut cur = &got[..];
        let (err, results, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(err, 16);
        assert!(results.is_empty(), "getErrorResponse Topics must be empty");
        assert!(
            cur.is_empty(),
            "OffsetDelete getErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
        );
        let copied = OffsetDeleteTopic::new("t", vec![0, 1]).error_result(16);
        let mut with_topics = BytesMut::new();
        encode_offset_delete_response(&mut with_topics, 16, &copied).unwrap();
        assert_ne!(
            &got[..],
            &with_topics[..],
            "getErrorResponse must not copy request partitions"
        );
    }

    #[test]
    fn offset_delete_response_merge_matches_java() {
        // Java OffsetDeleteResponse.Builder.merge: replace when the new
        // top-level ErrorCode is not NONE, or when current Topics are
        // empty. Otherwise append topics / partitions (no overlap check).
        let t1_p0 = OffsetDeleteResult::new("t1", 0, 0);
        let t1_p1 = OffsetDeleteResult::new("t1", 1, crate::error::GROUP_SUBSCRIBED_TO_TOPIC);
        let t2_p0 = OffsetDeleteResult::new("t2", 0, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        let current = vec![t1_p0.clone()];
        let new_same_topic = vec![t1_p1.clone()];
        let (err, merged) = OffsetDeleteResponse::merge(0, &current, 0, &new_same_topic);
        assert_eq!(err, 0, "merge keeps current ErrorCode when new is NONE");
        assert_eq!(merged, vec![t1_p0.clone(), t1_p1.clone()]);
        let mut got = BytesMut::new();
        encode_offset_delete_response(&mut got, err, &merged).unwrap();
        let mut cur = &got[..];
        let (decoded_err, decoded, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(decoded_err, 0);
        assert_eq!(decoded, merged);
        assert!(
            cur.is_empty(),
            "OffsetDelete merge same-topic leftover-empty; leftover {} bytes",
            cur.len()
        );

        let (err, merged) =
            OffsetDeleteResponse::merge(0, &current, 0, std::slice::from_ref(&t2_p0));
        assert_eq!(err, 0);
        assert_eq!(merged, vec![t1_p0.clone(), t2_p0.clone()]);
        got.clear();
        encode_offset_delete_response(&mut got, err, &merged).unwrap();
        let mut cur = &got[..];
        let (decoded_err, decoded, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(decoded_err, 0);
        assert_eq!(decoded, merged);
        assert!(
            cur.is_empty(),
            "OffsetDelete merge new-topic leftover-empty; leftover {} bytes",
            cur.len()
        );

        let (err, replaced) =
            OffsetDeleteResponse::merge(0, &current, crate::error::NOT_COORDINATOR, &[]);
        assert_eq!(err, crate::error::NOT_COORDINATOR);
        assert!(
            replaced.is_empty(),
            "non-NONE new ErrorCode replaces current Topics"
        );
        got.clear();
        encode_offset_delete_response(&mut got, err, &replaced).unwrap();
        let mut expected = BytesMut::new();
        OffsetDeleteRequest::error_response(&mut expected, crate::error::NOT_COORDINATOR).unwrap();
        assert_eq!(
            &got[..],
            &expected[..],
            "non-NONE merge must match empty-Topics getErrorResponse"
        );
        let mut cur = &got[..];
        let (decoded_err, decoded, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(decoded_err, crate::error::NOT_COORDINATOR);
        assert!(decoded.is_empty());
        assert!(
            cur.is_empty(),
            "OffsetDelete merge replace leftover-empty; leftover {} bytes",
            cur.len()
        );

        let (err, from_empty) =
            OffsetDeleteResponse::merge(crate::error::NOT_COORDINATOR, &[], 0, &current);
        assert_eq!(err, 0, "empty current Topics takes new ErrorCode");
        assert_eq!(from_empty, current);
        got.clear();
        encode_offset_delete_response(&mut got, err, &from_empty).unwrap();
        let mut cur = &got[..];
        let (decoded_err, decoded, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(decoded_err, 0);
        assert_eq!(decoded, current);
        assert!(
            cur.is_empty(),
            "OffsetDelete merge empty-current leftover-empty; leftover {} bytes",
            cur.len()
        );

        let (err, empty_both) = OffsetDeleteResponse::merge(0, &[], 0, &[]);
        assert_eq!(err, 0);
        assert!(empty_both.is_empty());
        let interleaved_new = vec![t2_p0.clone(), t1_p1.clone()];
        let (err, grouped) = OffsetDeleteResponse::merge(0, &current, 0, &interleaved_new);
        assert_eq!(err, 0);
        assert_eq!(
            grouped,
            vec![t1_p0, t1_p1, t2_p0],
            "new partitions of an existing topic append onto that topic"
        );
        got.clear();
        encode_offset_delete_response(&mut got, err, &grouped).unwrap();
        let mut cur = &got[..];
        let (decoded_err, decoded, ..) = decode_offset_delete_response(&mut cur).unwrap();
        assert_eq!(decoded_err, 0);
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "OffsetDelete merge grouped leftover-empty; leftover {} bytes",
            cur.len()
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
        let (err, decoded, ..) = decode_leave_group_response_version(&mut cur, 3).unwrap();
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
        let (err, decoded, ..) = decode_leave_group_response_version(&mut cur, 4).unwrap();
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
        let (err, decoded, ..) = decode_leave_group_response_version(&mut cur, 5).unwrap();
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
    fn leave_group_request_members_matches_java() {
        // Java LeaveGroupRequest.members: v0–v2 is a singleton of
        // data.memberId(); instance id and reason stay unset. v3+ is
        // data.members().
        let empty = LeaveGroupRequest::members(0, &[]);
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].member_id, "");
        assert!(empty[0].group_instance_id.is_none());
        assert!(empty[0].reason.is_none());
        let one = [LeaveGroupMember {
            member_id: "m1".into(),
            group_instance_id: Some("worker-1".into()),
            reason: Some("admin".into()),
        }];
        let v2 = LeaveGroupRequest::members(2, &one);
        assert_eq!(
            v2,
            vec![LeaveGroupMember {
                member_id: "m1".into(),
                group_instance_id: None,
                reason: None,
            }]
        );
        let v5 = LeaveGroupRequest::members(5, &one);
        assert_eq!(v5, one);
        let two = vec![
            LeaveGroupMember {
                member_id: "a".into(),
                group_instance_id: Some("i1".into()),
                reason: None,
            },
            LeaveGroupMember {
                member_id: "b".into(),
                group_instance_id: None,
                reason: Some("gone".into()),
            },
        ];
        assert_eq!(LeaveGroupRequest::members(3, &two), two);
        let mut buf = BytesMut::new();
        encode_leave_group_request_members(&mut buf, 0, "g", &one).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, decoded) = decode_leave_group_request_version(&mut cur, 0).unwrap();
        assert_eq!(LeaveGroupRequest::members(0, &decoded), v2);
        assert!(
            cur.is_empty(),
            "LeaveGroup v0 members leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_leave_group_request_members(&mut buf, 5, "g", &two).unwrap();
        let mut cur = buf.as_ref();
        let (_gid, decoded) = decode_leave_group_request_version(&mut cur, 5).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(LeaveGroupRequest::members(5, &decoded), two);
        assert!(
            cur.is_empty(),
            "LeaveGroup v5 members leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn leave_group_throttle_time_ms_matches_java() {
        for version in [1_i16, 2, 3, 4, 5] {
            let mut buf = BytesMut::new();
            encode_leave_group_response_with_throttle(&mut buf, version, 16, &[], 3_600_000)
                .unwrap();
            let mut cur = buf.as_ref();
            let (err, members, throttle) =
                decode_leave_group_response_version(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert!(members.is_empty());
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "LeaveGroup v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut buf = BytesMut::new();
        encode_leave_group_response_with_throttle(&mut buf, 0, 16, &[], 3_600_000).unwrap();
        let mut cur = buf.as_ref();
        let (err, _, throttle) = decode_leave_group_response_version(&mut cur, 0).unwrap();
        assert_eq!(err, 16);
        assert!(
            cur.is_empty(),
            "LeaveGroup v0 ThrottleTimeMs leftover-empty"
        );
        assert_eq!(
            throttle, 0,
            "LeaveGroup v0 omits ThrottleTimeMs even when the body has a non-zero value"
        );

        let mut with = BytesMut::new();
        encode_leave_group_response_with_throttle(&mut with, 1, 16, &[], 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_leave_group_response_with_throttle(&mut zero, 1, 16, &[], 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v1 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_leave_group_response_version(&mut conv, 1, 16, &[]).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_leave_group_response_version still writes ThrottleTimeMs 0"
        );
        let mut v0_with = BytesMut::new();
        encode_leave_group_response_with_throttle(&mut v0_with, 0, 16, &[], 3_600_000).unwrap();
        let mut v0_zero = BytesMut::new();
        encode_leave_group_response_with_throttle(&mut v0_zero, 0, 16, &[], 0).unwrap();
        assert_eq!(
            &v0_with[..],
            &v0_zero[..],
            "v0 encode omits ThrottleTimeMs even when the body has a non-zero value"
        );
        assert_ne!(
            &v0_with[..],
            &with[..],
            "v1 adds ThrottleTimeMs before ErrorCode"
        );

        for version in [0_i16, 1, 4, 5] {
            let mut expected = BytesMut::new();
            encode_leave_group_response_with_throttle(&mut expected, version, 16, &[], 3_600_000)
                .unwrap();
            let mut got = BytesMut::new();
            LeaveGroupRequest::error_response(&mut got, version, 16, 3_600_000).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "LeaveGroup v{version} getErrorResponse must match with_throttle encode"
            );
            let mut cur = got.as_ref();
            let (err, members, throttle) =
                decode_leave_group_response_version(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert!(members.is_empty(), "v{version} Members must be empty");
            if version >= 1 {
                assert_eq!(throttle, 3_600_000);
            } else {
                assert_eq!(throttle, 0);
            }
            assert!(
                cur.is_empty(),
                "LeaveGroup v{version} getErrorResponse leftover-empty"
            );
        }
    }

    #[test]
    fn leave_group_error_response_matches_java() {
        // Java LeaveGroupRequest.getErrorResponse: top-level error only.
        // Members stay empty (request members are not copied). Throttle is
        // from the argument (0 here is the JSON default) on v1+.
        for version in [0_i16, 1, 3, 5] {
            let mut expected = BytesMut::new();
            encode_leave_group_response_version(&mut expected, version, 16, &[]).unwrap();
            let mut got = BytesMut::new();
            LeaveGroupRequest::error_response(&mut got, version, 16, 0).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "LeaveGroup v{version} getErrorResponse must match empty-Members encode"
            );
            let mut cur = &got[..];
            let (err, members, ..) =
                decode_leave_group_response_version(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert!(members.is_empty(), "v{version} Members must be empty");
            assert!(
                cur.is_empty(),
                "LeaveGroup v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        let mut v0 = BytesMut::new();
        LeaveGroupRequest::error_response(&mut v0, 0, 16, 0).unwrap();
        let mut v1 = BytesMut::new();
        LeaveGroupRequest::error_response(&mut v1, 1, 16, 0).unwrap();
        assert_ne!(&v0[..], &v1[..], "v1+ getErrorResponse includes throttle");
        let copied = [LeaveGroupMemberResult {
            member_id: "m1".into(),
            group_instance_id: None,
            error_code: 16,
        }];
        let mut with_member = BytesMut::new();
        encode_leave_group_response_version(&mut with_member, 3, 16, &copied).unwrap();
        let mut empty = BytesMut::new();
        LeaveGroupRequest::error_response(&mut empty, 3, 16, 0).unwrap();
        assert_ne!(
            &empty[..],
            &with_member[..],
            "getErrorResponse must not copy request members"
        );
    }

    #[test]
    fn leave_group_response_error_counts_matches_java() {
        assert_eq!(
            LeaveGroupResponse::error_counts(0, &[]),
            HashMap::from([(0, 1)])
        );
        let counts = LeaveGroupResponse::error_counts(
            0,
            &[
                LeaveGroupMemberResult {
                    member_id: "ok".into(),
                    group_instance_id: None,
                    error_code: 0,
                },
                LeaveGroupMemberResult {
                    member_id: "unknown".into(),
                    group_instance_id: None,
                    error_code: crate::error::UNKNOWN_MEMBER_ID,
                },
                LeaveGroupMemberResult {
                    member_id: "ok2".into(),
                    group_instance_id: Some("i1".into()),
                    error_code: 0,
                },
            ],
        );
        assert_eq!(
            counts,
            HashMap::from([(0, 3), (crate::error::UNKNOWN_MEMBER_ID, 1)])
        );
        let top = LeaveGroupResponse::error_counts(crate::error::NOT_COORDINATOR, &[]);
        assert_eq!(top, HashMap::from([(crate::error::NOT_COORDINATOR, 1)]));
        let same = LeaveGroupResponse::error_counts(
            crate::error::UNKNOWN_MEMBER_ID,
            &[LeaveGroupMemberResult {
                member_id: "m".into(),
                group_instance_id: None,
                error_code: crate::error::UNKNOWN_MEMBER_ID,
            }],
        );
        assert_eq!(same, HashMap::from([(crate::error::UNKNOWN_MEMBER_ID, 2)]));
    }

    #[test]
    fn leave_group_response_error_matches_java() {
        assert_eq!(LeaveGroupResponse::error(0, &[]), 0);
        assert_eq!(
            LeaveGroupResponse::error(crate::error::NOT_COORDINATOR, &[]),
            crate::error::NOT_COORDINATOR
        );
        let members = [
            LeaveGroupMemberResult {
                member_id: "ok".into(),
                group_instance_id: None,
                error_code: 0,
            },
            LeaveGroupMemberResult {
                member_id: "unknown".into(),
                group_instance_id: None,
                error_code: crate::error::UNKNOWN_MEMBER_ID,
            },
        ];
        assert_eq!(
            LeaveGroupResponse::error(0, &members),
            crate::error::UNKNOWN_MEMBER_ID
        );
        assert_eq!(
            LeaveGroupResponse::error(crate::error::NOT_COORDINATOR, &members),
            crate::error::NOT_COORDINATOR
        );
    }

    #[test]
    fn leave_group_response_for_version_matches_java() {
        let members = [
            LeaveGroupMemberResult {
                member_id: "ok".into(),
                group_instance_id: None,
                error_code: 0,
            },
            LeaveGroupMemberResult {
                member_id: "unknown".into(),
                group_instance_id: Some("i1".into()),
                error_code: crate::error::UNKNOWN_MEMBER_ID,
            },
        ];
        assert_eq!(
            LeaveGroupResponse::for_version(3, crate::error::NOT_COORDINATOR, &members).unwrap(),
            (crate::error::NOT_COORDINATOR, members.to_vec())
        );
        assert_eq!(
            LeaveGroupResponse::for_version(5, 0, &members).unwrap(),
            (0, members.to_vec())
        );
        assert_eq!(
            LeaveGroupResponse::for_version(2, crate::error::NOT_COORDINATOR, &members).unwrap(),
            (crate::error::NOT_COORDINATOR, Vec::new())
        );
        let one = LeaveGroupMemberResult {
            member_id: "unknown".into(),
            group_instance_id: Some("i1".into()),
            error_code: crate::error::UNKNOWN_MEMBER_ID,
        };
        assert_eq!(
            LeaveGroupResponse::for_version(0, 0, std::slice::from_ref(&one)).unwrap(),
            (crate::error::UNKNOWN_MEMBER_ID, Vec::new())
        );
        assert_eq!(
            LeaveGroupResponse::for_version(3, 0, std::slice::from_ref(&one)).unwrap(),
            (0, vec![one.clone()])
        );
        let empty = LeaveGroupResponse::for_version(1, 0, &[]).unwrap_err();
        assert!(
            matches!(empty, Error::Unsupported(_)),
            "v1 NONE with no members is Java UnsupportedVersionException, got {empty}"
        );
        assert!(
            empty
                .to_string()
                .contains("can only contain one member, got 0 members"),
            "got {empty}"
        );
        let two = LeaveGroupResponse::for_version(2, 0, &members).unwrap_err();
        assert!(
            matches!(two, Error::Unsupported(_)),
            "v2 NONE with two members is Java UnsupportedVersionException, got {two}"
        );
        assert!(
            two.to_string()
                .contains("can only contain one member, got 2 members"),
            "got {two}"
        );
        let (error_code, rewritten) = LeaveGroupResponse::for_version(3, 0, &[]).unwrap();
        assert_eq!(error_code, 0);
        assert!(rewritten.is_empty(), "v3 NONE with no members is identity");

        for version in [0_i16, 1, 3, 5] {
            let (error_code, rewritten) =
                LeaveGroupResponse::for_version(version, crate::error::NOT_COORDINATOR, &members)
                    .unwrap();
            let mut buf = BytesMut::new();
            encode_leave_group_response_version(&mut buf, version, error_code, &rewritten).unwrap();
            let mut cur = buf.as_ref();
            let (decoded_err, decoded_members, ..) =
                decode_leave_group_response_version(&mut cur, version).unwrap();
            assert_eq!(decoded_err, crate::error::NOT_COORDINATOR);
            if version >= 3 {
                assert_eq!(decoded_members, members);
            } else {
                assert!(
                    decoded_members.is_empty(),
                    "v{version} members must be dropped"
                );
            }
            assert!(
                cur.is_empty(),
                "LeaveGroup v{version} (data, version) leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        for version in [0_i16, 1, 3, 5] {
            let (error_code, rewritten) =
                LeaveGroupResponse::for_version(version, crate::error::NOT_COORDINATOR, &[])
                    .unwrap();
            assert_eq!(error_code, crate::error::NOT_COORDINATOR);
            assert!(rewritten.is_empty());
            let mut buf = BytesMut::new();
            encode_leave_group_response_version(&mut buf, version, error_code, &rewritten).unwrap();
            let mut cur = buf.as_ref();
            let (decoded_err, decoded_members, ..) =
                decode_leave_group_response_version(&mut cur, version).unwrap();
            assert_eq!(decoded_err, crate::error::NOT_COORDINATOR);
            assert!(decoded_members.is_empty());
            assert!(
                cur.is_empty(),
                "LeaveGroup v{version} (data, version) empty leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
    }

    #[test]
    fn leave_group_response_from_members_matches_java() {
        let members = [
            LeaveGroupMemberResult {
                member_id: "ok".into(),
                group_instance_id: None,
                error_code: 0,
            },
            LeaveGroupMemberResult {
                member_id: "unknown".into(),
                group_instance_id: Some("i1".into()),
                error_code: crate::error::UNKNOWN_MEMBER_ID,
            },
        ];
        assert_eq!(
            LeaveGroupResponse::from_members(3, crate::error::NOT_COORDINATOR, &members),
            (crate::error::NOT_COORDINATOR, members.to_vec())
        );
        assert_eq!(
            LeaveGroupResponse::from_members(5, 0, &members),
            (0, members.to_vec())
        );
        assert_eq!(
            LeaveGroupResponse::from_members(2, 0, &members),
            (crate::error::UNKNOWN_MEMBER_ID, Vec::new())
        );
        assert_eq!(
            LeaveGroupResponse::from_members(0, crate::error::NOT_COORDINATOR, &members),
            (crate::error::NOT_COORDINATOR, Vec::new())
        );
        assert_eq!(LeaveGroupResponse::from_members(1, 0, &[]), (0, Vec::new()));
        assert!(
            LeaveGroupResponse::for_version(1, 0, &[]).is_err(),
            "List constructor allows empty members on v<=2; data constructor does not"
        );
        assert!(
            LeaveGroupResponse::for_version(2, 0, &members).is_err(),
            "List constructor allows many members on v<=2; data constructor does not"
        );
        let one = LeaveGroupMemberResult {
            member_id: "unknown".into(),
            group_instance_id: Some("i1".into()),
            error_code: crate::error::UNKNOWN_MEMBER_ID,
        };
        assert_eq!(
            LeaveGroupResponse::from_members(0, 0, std::slice::from_ref(&one)),
            (crate::error::UNKNOWN_MEMBER_ID, Vec::new())
        );
        assert_eq!(
            LeaveGroupResponse::from_members(3, 0, std::slice::from_ref(&one)),
            (0, vec![one.clone()])
        );
        assert_eq!(LeaveGroupResponse::from_members(3, 0, &[]), (0, Vec::new()));

        for version in [0_i16, 1, 3, 5] {
            let (error_code, rewritten) = LeaveGroupResponse::from_members(version, 0, &members);
            let mut buf = BytesMut::new();
            encode_leave_group_response_version(&mut buf, version, error_code, &rewritten).unwrap();
            let mut cur = buf.as_ref();
            let (decoded_err, decoded_members, ..) =
                decode_leave_group_response_version(&mut cur, version).unwrap();
            if version >= 3 {
                assert_eq!(decoded_err, 0);
                assert_eq!(decoded_members, members);
            } else {
                assert_eq!(decoded_err, crate::error::UNKNOWN_MEMBER_ID);
                assert!(
                    decoded_members.is_empty(),
                    "v{version} members must be dropped"
                );
            }
            assert!(
                cur.is_empty(),
                "LeaveGroup v{version} from_members leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        for version in [0_i16, 1, 3, 5] {
            let (error_code, rewritten) = LeaveGroupResponse::from_members(version, 0, &[]);
            assert_eq!(error_code, 0);
            assert!(rewritten.is_empty());
            let mut buf = BytesMut::new();
            encode_leave_group_response_version(&mut buf, version, error_code, &rewritten).unwrap();
            let mut cur = buf.as_ref();
            let (decoded_err, decoded_members, ..) =
                decode_leave_group_response_version(&mut cur, version).unwrap();
            assert_eq!(decoded_err, 0);
            assert!(decoded_members.is_empty());
            assert!(
                cur.is_empty(),
                "LeaveGroup v{version} from_members empty leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
    }

    #[test]
    fn leave_group_builder_matches_java() {
        assert!(!LeaveGroupResponse::should_client_throttle(1));
        assert!(LeaveGroupResponse::should_client_throttle(2));
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
