//! DescribeQuorum (KIP-595 / KIP-836 / KIP-853, api key 55). Flexible v0–v2.

use std::collections::HashMap;
use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Java `Topic.CLUSTER_METADATA_TOPIC_NAME`.
pub const CLUSTER_METADATA_TOPIC_NAME: &str = "__cluster_metadata";
/// Java `Topic.CLUSTER_METADATA_TOPIC_PARTITION` partition index.
pub const CLUSTER_METADATA_PARTITION_INDEX: i32 = 0;
/// JSON default for ReplicaState `LastFetchTimestamp` / `LastCaughtUpTimestamp`.
pub const UNKNOWN_REPLICA_TIMESTAMP: i64 = -1;

fn describe_quorum_spoken(version: i16) -> Result<()> {
    if !(0..=2).contains(&version) {
        return Err(Error::Unsupported(format!(
            "DescribeQuorum version {version} is not implemented"
        )));
    }
    Ok(())
}

/// One partition in DescribeQuorum request `Topics`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumRequestPartition {
    /// Partition index.
    pub partition_index: i32,
}

impl DescribeQuorumRequestPartition {
    /// Partition `partition_index`.
    #[must_use]
    pub const fn new(partition_index: i32) -> Self {
        Self { partition_index }
    }

    /// Partition index.
    #[must_use]
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }
}

/// One topic in DescribeQuorum `Topics`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumTopic {
    /// Topic name.
    pub topic_name: String,
    /// Partition indexes to describe.
    pub partitions: Vec<DescribeQuorumRequestPartition>,
}

impl DescribeQuorumTopic {
    /// Topic `topic_name` plus partition indexes.
    #[must_use]
    pub fn new(
        topic_name: impl Into<String>,
        partitions: Vec<DescribeQuorumRequestPartition>,
    ) -> Self {
        Self {
            topic_name: topic_name.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Partition indexes.
    #[must_use]
    pub fn partitions(&self) -> &[DescribeQuorumRequestPartition] {
        &self.partitions
    }
}

/// DescribeQuorum request. Unchanged across v0–v2.
///
/// [`Self::singleton_request`] is Java
/// `DescribeQuorumRequest.singletonRequest` for
/// `__cluster_metadata`-0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumRequest {
    /// Topics to describe.
    pub topics: Vec<DescribeQuorumTopic>,
}

impl DescribeQuorumRequest {
    /// Topics `topics`.
    #[must_use]
    pub fn new(topics: Vec<DescribeQuorumTopic>) -> Self {
        Self { topics }
    }

    /// Java `DescribeQuorumRequest.singletonRequest` for
    /// [`CLUSTER_METADATA_TOPIC_NAME`] partition
    /// [`CLUSTER_METADATA_PARTITION_INDEX`].
    #[must_use]
    pub fn singleton_request() -> Self {
        Self {
            topics: vec![DescribeQuorumTopic::new(
                CLUSTER_METADATA_TOPIC_NAME,
                vec![DescribeQuorumRequestPartition::new(
                    CLUSTER_METADATA_PARTITION_INDEX,
                )],
            )],
        }
    }

    /// Topics to describe.
    #[must_use]
    pub fn topics(&self) -> &[DescribeQuorumTopic] {
        &self.topics
    }

    /// Java `DescribeQuorumRequest.getErrorResponse` /
    /// `getTopLevelErrorResponse`.
    ///
    /// Top-level [`DescribeQuorumResponse::error_code`] only; Topics and
    /// Nodes stay empty. `ErrorMessage` stays the JSON default (null);
    /// official Java also sets `Errors.message`. The Java
    /// `throttleTimeMs` argument is unused (no ThrottleTimeMs in the
    /// schema).
    #[must_use]
    pub fn error_response(&self, error_code: i16) -> DescribeQuorumResponse {
        let _ = self;
        DescribeQuorumResponse {
            error_code,
            error_message: None,
            topics: Vec::new(),
            nodes: Vec::new(),
        }
    }

    /// Java `DescribeQuorumRequest.getPartitionLevelErrorResponse`.
    ///
    /// Copies each request topic-partition and sets per-partition
    /// `ErrorCode`. `ErrorMessage` stays the JSON default (null);
    /// official Java also sets `Errors.message`. Leader / watermark /
    /// replica lists stay JSON defaults.
    #[must_use]
    pub fn partition_level_error_response(&self, error_code: i16) -> DescribeQuorumResponse {
        DescribeQuorumResponse {
            error_code: 0,
            error_message: None,
            topics: self
                .topics
                .iter()
                .map(|t| DescribeQuorumResponseTopic {
                    topic_name: t.topic_name.clone(),
                    partitions: t
                        .partitions
                        .iter()
                        .map(|p| DescribeQuorumResponsePartition {
                            partition_index: p.partition_index,
                            error_code,
                            error_message: None,
                            leader_id: 0,
                            leader_epoch: 0,
                            high_watermark: 0,
                            current_voters: Vec::new(),
                            observers: Vec::new(),
                        })
                        .collect(),
                })
                .collect(),
            nodes: Vec::new(),
        }
    }
}

/// ReplicaState common struct (voters and observers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaState {
    /// Replica broker id.
    pub replica_id: i32,
    /// Replica directory UUID. Zeros below v2 (Java `Uuid.ZERO_UUID`).
    pub replica_directory_id: [u8; 16],
    /// Last known log end offset, or `-1` if unknown.
    pub log_end_offset: i64,
    /// Last fetch timestamp. JSON default `-1` (omitted below v1).
    pub last_fetch_timestamp: i64,
    /// Last caught-up timestamp. JSON default `-1` (omitted below v1).
    pub last_caught_up_timestamp: i64,
}

impl ReplicaState {
    /// Replica `replica_id` with `log_end_offset`. Directory id is
    /// zeros; timestamps are [`UNKNOWN_REPLICA_TIMESTAMP`].
    #[must_use]
    pub const fn new(replica_id: i32, log_end_offset: i64) -> Self {
        Self {
            replica_id,
            replica_directory_id: [0; 16],
            log_end_offset,
            last_fetch_timestamp: UNKNOWN_REPLICA_TIMESTAMP,
            last_caught_up_timestamp: UNKNOWN_REPLICA_TIMESTAMP,
        }
    }

    /// Replica broker id.
    #[must_use]
    pub const fn replica_id(&self) -> i32 {
        self.replica_id
    }

    /// Replica directory UUID (zeros below v2).
    #[must_use]
    pub const fn replica_directory_id(&self) -> [u8; 16] {
        self.replica_directory_id
    }

    /// Last known log end offset, or `-1` if unknown.
    #[must_use]
    pub const fn log_end_offset(&self) -> i64 {
        self.log_end_offset
    }

    /// Last fetch timestamp (`-1` if unknown / below v1).
    #[must_use]
    pub const fn last_fetch_timestamp(&self) -> i64 {
        self.last_fetch_timestamp
    }

    /// Last caught-up timestamp (`-1` if unknown / below v1).
    #[must_use]
    pub const fn last_caught_up_timestamp(&self) -> i64 {
        self.last_caught_up_timestamp
    }
}

/// One partition in DescribeQuorum response `Topics`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Partition error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present. `None` below v2.
    pub error_message: Option<String>,
    /// Current leader broker id, or `-1` if unknown.
    pub leader_id: i32,
    /// Latest known leader epoch.
    pub leader_epoch: i32,
    /// High watermark.
    pub high_watermark: i64,
    /// Current voters.
    pub current_voters: Vec<ReplicaState>,
    /// Observers.
    pub observers: Vec<ReplicaState>,
}

impl DescribeQuorumResponsePartition {
    /// Partition `partition_index` with `error_code`. Other fields are
    /// JSON defaults.
    #[must_use]
    pub const fn new(partition_index: i32, error_code: i16) -> Self {
        Self {
            partition_index,
            error_code,
            error_message: None,
            leader_id: 0,
            leader_epoch: 0,
            high_watermark: 0,
            current_voters: Vec::new(),
            observers: Vec::new(),
        }
    }

    /// Partition index.
    #[must_use]
    pub const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Partition error code (`0` is success).
    #[must_use]
    pub const fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Current leader broker id.
    #[must_use]
    pub const fn leader_id(&self) -> i32 {
        self.leader_id
    }

    /// Latest known leader epoch.
    #[must_use]
    pub const fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// High watermark.
    #[must_use]
    pub const fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Current voters.
    #[must_use]
    pub fn current_voters(&self) -> &[ReplicaState] {
        &self.current_voters
    }

    /// Observers.
    #[must_use]
    pub fn observers(&self) -> &[ReplicaState] {
        &self.observers
    }
}

/// One topic in DescribeQuorum response `Topics`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumResponseTopic {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition quorum state.
    pub partitions: Vec<DescribeQuorumResponsePartition>,
}

impl DescribeQuorumResponseTopic {
    /// Topic `topic_name` plus partition results.
    #[must_use]
    pub fn new(
        topic_name: impl Into<String>,
        partitions: Vec<DescribeQuorumResponsePartition>,
    ) -> Self {
        Self {
            topic_name: topic_name.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Per-partition results.
    #[must_use]
    pub fn partitions(&self) -> &[DescribeQuorumResponsePartition] {
        &self.partitions
    }
}

/// One listener on a v2 quorum node (KIP-853).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumListener {
    /// Endpoint name (for example `CONTROLLER`).
    pub name: String,
    /// Hostname.
    pub host: String,
    /// Port (UINT16 on the wire).
    pub port: u16,
}

impl DescribeQuorumListener {
    /// Listener `name` at `host`:`port`.
    #[must_use]
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            host: host.into(),
            port,
        }
    }

    /// Endpoint name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Hostname.
    #[must_use]
    pub fn host(&self) -> &str {
        self.host.as_str()
    }

    /// Port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }
}

/// One node in DescribeQuorum v2 `Nodes` (KIP-853).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumNode {
    /// Node id.
    pub node_id: i32,
    /// Controller listeners.
    pub listeners: Vec<DescribeQuorumListener>,
}

impl DescribeQuorumNode {
    /// Node `node_id` plus listeners.
    #[must_use]
    pub fn new(node_id: i32, listeners: Vec<DescribeQuorumListener>) -> Self {
        Self { node_id, listeners }
    }

    /// Node id.
    #[must_use]
    pub const fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Controller listeners.
    #[must_use]
    pub fn listeners(&self) -> &[DescribeQuorumListener] {
        &self.listeners
    }
}

/// DescribeQuorum response.
///
/// [`Self::error_counts`] is Java `DescribeQuorumResponse.errorCounts`.
/// [`Self::should_client_throttle`] is Java
/// `DescribeQuorumResponse.shouldClientThrottle` (not overridden; Java
/// `AbstractResponse` default is `false`). There is no ThrottleTimeMs
/// in the schema (`DEFAULT_THROTTLE_TIME`; `maybeSetThrottleTimeMs` is
/// a no-op).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumResponse {
    /// Top-level error code.
    pub error_code: i16,
    /// Top-level error message (v2+). `None` below v2.
    pub error_message: Option<String>,
    /// Per-topic quorum state.
    pub topics: Vec<DescribeQuorumResponseTopic>,
    /// Quorum nodes (v2+ / KIP-853). Empty below v2.
    pub nodes: Vec<DescribeQuorumNode>,
}

impl DescribeQuorumResponse {
    /// Construct [`Self`]. Nodes empty; `ErrorMessage` JSON-null.
    #[must_use]
    pub fn new(error_code: i16, topics: Vec<DescribeQuorumResponseTopic>) -> Self {
        Self {
            error_code,
            error_message: None,
            topics,
            nodes: Vec::new(),
        }
    }

    /// Top-level error code (`0` is success).
    #[must_use]
    pub const fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Top-level error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Per-topic results.
    #[must_use]
    pub fn topics(&self) -> &[DescribeQuorumResponseTopic] {
        &self.topics
    }

    /// Quorum nodes (empty below v2).
    #[must_use]
    pub fn nodes(&self) -> &[DescribeQuorumNode] {
        &self.nodes
    }

    /// Java `DescribeQuorumResponse.throttleTimeMs`
    /// (`AbstractResponse.DEFAULT_THROTTLE_TIME`). Schema has no
    /// ThrottleTimeMs field.
    #[must_use]
    pub const fn throttle_time_ms() -> i32 {
        0
    }

    /// Java `DescribeQuorumResponse.shouldClientThrottle`.
    ///
    /// Not overridden; Java `AbstractResponse` default is `false`.
    #[must_use]
    pub const fn should_client_throttle(_version: i16) -> bool {
        false
    }

    /// Java `DescribeQuorumResponse.errorCounts`.
    ///
    /// Top-level `errorCode` (including `NONE`) plus each partition
    /// `errorCode`.
    #[must_use]
    pub fn error_counts(&self) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        let _ = counts.insert(self.error_code, 1);
        for topic in &self.topics {
            for partition in &topic.partitions {
                *counts.entry(partition.error_code).or_insert(0) += 1;
            }
        }
        counts
    }
}

impl fmt::Display for DescribeQuorumRequestPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PartitionData(partitionIndex={})", self.partition_index)
    }
}

/// Encode DescribeQuorum v0–v2 request (compact strings/arrays plus
/// tagged fields; request unchanged across versions).
pub fn encode_describe_quorum_request(
    buf: &mut BytesMut,
    version: i16,
    req: &DescribeQuorumRequest,
) -> Result<()> {
    describe_quorum_spoken(version)?;
    buf::put_array_len(buf, true, Some(req.topics.len()))?;
    for topic in &req.topics {
        buf::put_compact_string(buf, Some(topic.topic_name.as_str()))?;
        buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
        for partition in &topic.partitions {
            buf.put_i32(partition.partition_index);
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode DescribeQuorum v0–v2 request.
pub fn decode_describe_quorum_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribeQuorumRequest> {
    describe_quorum_spoken(version)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(DescribeQuorumRequestPartition { partition_index });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(DescribeQuorumTopic {
            topic_name,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeQuorumRequest { topics })
}

fn encode_replica_state(buf: &mut BytesMut, version: i16, replica: &ReplicaState) {
    buf.put_i32(replica.replica_id);
    if version >= 2 {
        buf.extend_from_slice(&replica.replica_directory_id);
    }
    buf.put_i64(replica.log_end_offset);
    if version >= 1 {
        buf.put_i64(replica.last_fetch_timestamp);
        buf.put_i64(replica.last_caught_up_timestamp);
    }
    buf::put_empty_tagged_fields(buf);
}

fn decode_replica_state<B: Buf>(buf: &mut B, version: i16) -> Result<ReplicaState> {
    let replica_id = buf::get_i32(buf)?;
    let replica_directory_id = if version >= 2 {
        buf::get_uuid(buf)?
    } else {
        [0; 16]
    };
    let log_end_offset = buf::get_i64(buf)?;
    let (last_fetch_timestamp, last_caught_up_timestamp) = if version >= 1 {
        (buf::get_i64(buf)?, buf::get_i64(buf)?)
    } else {
        (UNKNOWN_REPLICA_TIMESTAMP, UNKNOWN_REPLICA_TIMESTAMP)
    };
    buf::skip_tagged_fields(buf)?;
    Ok(ReplicaState {
        replica_id,
        replica_directory_id,
        log_end_offset,
        last_fetch_timestamp,
        last_caught_up_timestamp,
    })
}

fn encode_replica_states(
    buf: &mut BytesMut,
    version: i16,
    replicas: &[ReplicaState],
) -> Result<()> {
    buf::put_array_len(buf, true, Some(replicas.len()))?;
    for replica in replicas {
        encode_replica_state(buf, version, replica);
    }
    Ok(())
}

fn decode_replica_states<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<ReplicaState>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut replicas = Vec::with_capacity(n);
    for _ in 0..n {
        replicas.push(decode_replica_state(buf, version)?);
    }
    Ok(replicas)
}

fn encode_response_partition(
    buf: &mut BytesMut,
    version: i16,
    partition: &DescribeQuorumResponsePartition,
) -> Result<()> {
    buf.put_i32(partition.partition_index);
    buf.put_i16(partition.error_code);
    if version >= 2 {
        buf::put_compact_string(buf, partition.error_message.as_deref())?;
    }
    buf.put_i32(partition.leader_id);
    buf.put_i32(partition.leader_epoch);
    buf.put_i64(partition.high_watermark);
    encode_replica_states(buf, version, &partition.current_voters)?;
    encode_replica_states(buf, version, &partition.observers)?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn decode_response_partition<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribeQuorumResponsePartition> {
    let partition_index = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = if version >= 2 {
        buf::get_compact_string(buf)?
    } else {
        None
    };
    let leader_id = buf::get_i32(buf)?;
    let leader_epoch = buf::get_i32(buf)?;
    let high_watermark = buf::get_i64(buf)?;
    let current_voters = decode_replica_states(buf, version)?;
    let observers = decode_replica_states(buf, version)?;
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeQuorumResponsePartition {
        partition_index,
        error_code,
        error_message,
        leader_id,
        leader_epoch,
        high_watermark,
        current_voters,
        observers,
    })
}

fn encode_nodes(buf: &mut BytesMut, nodes: &[DescribeQuorumNode]) -> Result<()> {
    buf::put_array_len(buf, true, Some(nodes.len()))?;
    for node in nodes {
        buf.put_i32(node.node_id);
        buf::put_array_len(buf, true, Some(node.listeners.len()))?;
        for listener in &node.listeners {
            buf::put_compact_string(buf, Some(listener.name.as_str()))?;
            buf::put_compact_string(buf, Some(listener.host.as_str()))?;
            buf.put_u16(listener.port);
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

fn decode_nodes<B: Buf>(buf: &mut B) -> Result<Vec<DescribeQuorumNode>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut nodes = Vec::with_capacity(n);
    for _ in 0..n {
        let node_id = buf::get_i32(buf)?;
        let ln = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut listeners = Vec::with_capacity(ln);
        for _ in 0..ln {
            let name = buf::get_compact_string(buf)?.unwrap_or_default();
            let host = buf::get_compact_string(buf)?.unwrap_or_default();
            let port = buf::get_u16(buf)?;
            buf::skip_tagged_fields(buf)?;
            listeners.push(DescribeQuorumListener { name, host, port });
        }
        buf::skip_tagged_fields(buf)?;
        nodes.push(DescribeQuorumNode { node_id, listeners });
    }
    Ok(nodes)
}

/// Encode DescribeQuorum v0–v2 response.
///
/// v1 LastFetchTimestamp / LastCaughtUpTimestamp (KIP-836). v2
/// ErrorMessage, ReplicaDirectoryId, Nodes (KIP-853).
pub fn encode_describe_quorum_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &DescribeQuorumResponse,
) -> Result<()> {
    describe_quorum_spoken(version)?;
    buf.put_i16(resp.error_code);
    if version >= 2 {
        buf::put_compact_string(buf, resp.error_message.as_deref())?;
    }
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for topic in &resp.topics {
        buf::put_compact_string(buf, Some(topic.topic_name.as_str()))?;
        buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
        for partition in &topic.partitions {
            encode_response_partition(buf, version, partition)?;
        }
        buf::put_empty_tagged_fields(buf);
    }
    if version >= 2 {
        encode_nodes(buf, &resp.nodes)?;
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode DescribeQuorum v0–v2 response.
///
/// Below v1, replica timestamps fill [`UNKNOWN_REPLICA_TIMESTAMP`].
/// Below v2, `ErrorMessage` is `None`, replica directory ids are zeros,
/// and `Nodes` is empty.
pub fn decode_describe_quorum_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribeQuorumResponse> {
    describe_quorum_spoken(version)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = if version >= 2 {
        buf::get_compact_string(buf)?
    } else {
        None
    };
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(decode_response_partition(buf, version)?);
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(DescribeQuorumResponseTopic {
            topic_name,
            partitions,
        });
    }
    let nodes = if version >= 2 {
        decode_nodes(buf)?
    } else {
        Vec::new()
    };
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeQuorumResponse {
        error_code,
        error_message,
        topics,
        nodes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_req() -> DescribeQuorumRequest {
        DescribeQuorumRequest::singleton_request()
    }

    fn sample_replica(id: i32, leo: i64) -> ReplicaState {
        let mut replica = ReplicaState::new(id, leo);
        replica.last_fetch_timestamp = 1_700_000_000_000;
        replica.last_caught_up_timestamp = 1_700_000_000_100;
        replica.replica_directory_id = [
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            u8::try_from(id).unwrap_or(0),
        ];
        replica
    }

    fn sample_resp() -> DescribeQuorumResponse {
        DescribeQuorumResponse {
            error_code: 0,
            error_message: None,
            topics: vec![DescribeQuorumResponseTopic::new(
                CLUSTER_METADATA_TOPIC_NAME,
                vec![DescribeQuorumResponsePartition {
                    partition_index: 0,
                    error_code: 0,
                    error_message: None,
                    leader_id: 1,
                    leader_epoch: 7,
                    high_watermark: 42,
                    current_voters: vec![sample_replica(1, 42), sample_replica(2, 40)],
                    observers: vec![sample_replica(3, 10)],
                }],
            )],
            nodes: vec![DescribeQuorumNode::new(
                1,
                vec![DescribeQuorumListener::new("CONTROLLER", "127.0.0.1", 9093)],
            )],
        }
    }

    fn roundtrip_req(version: i16, req: &DescribeQuorumRequest) -> DescribeQuorumRequest {
        let mut buf = BytesMut::new();
        encode_describe_quorum_request(&mut buf, version, req).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_describe_quorum_request(&mut cur, version).unwrap();
        assert!(!cur.has_remaining(), "v{version} request leftover-empty");
        decoded
    }

    fn roundtrip_resp(version: i16, resp: &DescribeQuorumResponse) -> DescribeQuorumResponse {
        let mut buf = BytesMut::new();
        encode_describe_quorum_response(&mut buf, version, resp).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_describe_quorum_response(&mut cur, version).unwrap();
        assert!(!cur.has_remaining(), "v{version} response leftover-empty");
        decoded
    }

    #[test]
    fn describe_quorum_singleton_request_is_cluster_metadata_zero() {
        let req = sample_req();
        assert_eq!(req.topics().len(), 1);
        assert_eq!(req.topics()[0].topic_name(), CLUSTER_METADATA_TOPIC_NAME);
        assert_eq!(req.topics()[0].partitions()[0].partition_index(), 0);
        assert_eq!(
            req.topics()[0].partitions()[0].to_string(),
            "PartitionData(partitionIndex=0)"
        );
    }

    #[test]
    fn describe_quorum_request_bytes_match_across_spoken_versions() {
        let req = sample_req();
        let mut v0 = BytesMut::new();
        let mut v1 = BytesMut::new();
        let mut v2 = BytesMut::new();
        encode_describe_quorum_request(&mut v0, 0, &req).unwrap();
        encode_describe_quorum_request(&mut v1, 1, &req).unwrap();
        encode_describe_quorum_request(&mut v2, 2, &req).unwrap();
        assert_eq!(&v0[..], &v1[..]);
        assert_eq!(&v1[..], &v2[..]);
        assert_eq!(roundtrip_req(0, &req), req);
        assert_eq!(roundtrip_req(2, &req), req);
    }

    #[test]
    fn describe_quorum_v0_roundtrip_fills_timestamp_and_directory_defaults() {
        let resp = sample_resp();
        let got = roundtrip_resp(0, &resp);
        assert_eq!(got.error_code, 0);
        assert!(got.error_message.is_none());
        assert!(got.nodes.is_empty(), "v0 omits Nodes");
        let part = &got.topics[0].partitions[0];
        assert_eq!(part.leader_id, 1);
        assert_eq!(part.leader_epoch, 7);
        assert_eq!(part.high_watermark, 42);
        assert_eq!(part.current_voters.len(), 2);
        assert_eq!(part.current_voters[0].replica_id, 1);
        assert_eq!(part.current_voters[0].log_end_offset, 42);
        assert_eq!(
            part.current_voters[0].last_fetch_timestamp,
            UNKNOWN_REPLICA_TIMESTAMP
        );
        assert_eq!(
            part.current_voters[0].last_caught_up_timestamp,
            UNKNOWN_REPLICA_TIMESTAMP
        );
        assert_eq!(part.current_voters[0].replica_directory_id, [0; 16]);
        assert_eq!(part.observers[0].replica_id, 3);
        assert!(part.error_message.is_none());
    }

    #[test]
    fn describe_quorum_v1_roundtrip_keeps_timestamps_omits_kip853() {
        let resp = sample_resp();
        let got = roundtrip_resp(1, &resp);
        assert!(got.nodes.is_empty(), "v1 omits Nodes");
        let voter = &got.topics[0].partitions[0].current_voters[0];
        assert_eq!(voter.last_fetch_timestamp, 1_700_000_000_000);
        assert_eq!(voter.last_caught_up_timestamp, 1_700_000_000_100);
        assert_eq!(voter.replica_directory_id, [0; 16]);
        assert!(got.error_message.is_none());
    }

    #[test]
    fn describe_quorum_v2_roundtrip_keeps_nodes_directory_and_error_message() {
        let mut resp = sample_resp();
        resp.error_message = Some("ok".into());
        resp.topics[0].partitions[0].error_message = None;
        let got = roundtrip_resp(2, &resp);
        assert_eq!(got, resp);
        assert_eq!(got.nodes[0].node_id(), 1);
        assert_eq!(got.nodes[0].listeners()[0].name(), "CONTROLLER");
        assert_eq!(got.nodes[0].listeners()[0].host(), "127.0.0.1");
        assert_eq!(got.nodes[0].listeners()[0].port(), 9093);
        assert_eq!(
            got.topics[0].partitions[0].current_voters[0].replica_directory_id[15],
            1
        );
        assert_eq!(got.error_message.as_deref(), Some("ok"));
    }

    #[test]
    fn describe_quorum_error_response_is_top_level_only() {
        let req = sample_req();
        let resp = req.error_response(41);
        assert_eq!(resp.error_code, 41);
        assert!(resp.topics.is_empty());
        assert!(resp.nodes.is_empty());
        assert!(resp.error_message.is_none());
        let counts = resp.error_counts();
        assert_eq!(counts.get(&41), Some(&1));
        assert!(!DescribeQuorumResponse::should_client_throttle(0));
        assert!(!DescribeQuorumResponse::should_client_throttle(2));
        assert_eq!(DescribeQuorumResponse::throttle_time_ms(), 0);
    }

    #[test]
    fn describe_quorum_partition_level_error_response_copies_indexes() {
        let req = sample_req();
        let resp = req.partition_level_error_response(3);
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.topics[0].topic_name, CLUSTER_METADATA_TOPIC_NAME);
        assert_eq!(resp.topics[0].partitions[0].partition_index, 0);
        assert_eq!(resp.topics[0].partitions[0].error_code, 3);
        assert!(resp.topics[0].partitions[0].current_voters.is_empty());
        let counts = resp.error_counts();
        assert_eq!(counts.get(&0), Some(&1));
        assert_eq!(counts.get(&3), Some(&1));
    }

    #[test]
    fn describe_quorum_error_counts_includes_none() {
        let resp = sample_resp();
        let counts = resp.error_counts();
        assert_eq!(counts.get(&0), Some(&2));
    }

    #[test]
    fn describe_quorum_version_3_is_not_spoken() {
        let err =
            encode_describe_quorum_request(&mut BytesMut::new(), 3, &sample_req()).unwrap_err();
        assert!(
            err.to_string()
                .contains("DescribeQuorum version 3 is not implemented"),
            "{err}"
        );
        let err = decode_describe_quorum_response(&mut &[][..], 3).unwrap_err();
        assert!(
            err.to_string()
                .contains("DescribeQuorum version 3 is not implemented"),
            "{err}"
        );
    }

    #[test]
    fn describe_quorum_replica_state_getters_match_fields() {
        let replica = sample_replica(2, 9);
        assert_eq!(replica.replica_id(), 2);
        assert_eq!(replica.log_end_offset(), 9);
        assert_eq!(replica.last_fetch_timestamp(), 1_700_000_000_000);
        assert_eq!(replica.replica_directory_id()[15], 2);
    }
}
