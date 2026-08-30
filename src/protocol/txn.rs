//! AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, WriteTxnMarkers, and
//! TxnOffsetCommit (api keys 24–28).

use std::collections::HashMap;
use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use super::group::JoinGroupRequest;
use super::records::RecordBatch;
use crate::error::{Error, Result};

/// AddPartitionsToTxn (24).
pub const ADD_PARTITIONS_TO_TXN: i16 = 24;
/// AddOffsetsToTxn (25).
pub const ADD_OFFSETS_TO_TXN: i16 = 25;
/// EndTxn (26).
pub const END_TXN: i16 = 26;

/// Java `EndTxnRequest` version helpers (KIP-890 transaction V2).
pub struct EndTxnRequest;

impl EndTxnRequest {
    /// Java `EndTxnRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`.
    pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 4;
}

/// Java `EndTxnResponse` helpers.
pub struct EndTxnResponse;

impl EndTxnResponse {
    /// Java `EndTxnResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 1
    }
}

/// Java `TransactionResult` (EndTxn committed flag / WriteTxnMarkers
/// `transactionResult`).
///
/// [`Display`] is Java `TransactionResult.toString` (`ABORT` / `COMMIT`).
/// [`Self::id`] is the public Java `id` field. [`Self::from_id`] is Java
/// `TransactionResult.forId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionResult {
    /// Java `ABORT` (`id` false).
    Abort,
    /// Java `COMMIT` (`id` true).
    Commit,
}

impl TransactionResult {
    /// Java `TransactionResult.id`.
    #[must_use]
    pub const fn id(self) -> bool {
        matches!(self, Self::Commit)
    }

    /// Java `TransactionResult.forId`.
    #[must_use]
    pub const fn from_id(id: bool) -> Self {
        if id {
            Self::Commit
        } else {
            Self::Abort
        }
    }

    /// Java `TransactionResult.toString` (`ABORT` / `COMMIT`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Abort => "ABORT",
            Self::Commit => "COMMIT",
        }
    }
}

impl fmt::Display for TransactionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// WriteTxnMarkers (27).
pub const WRITE_TXN_MARKERS: i16 = 27;
/// TxnOffsetCommit (28).
pub const TXN_OFFSET_COMMIT: i16 = 28;

/// One topic in AddPartitionsToTxn v0–v3.
///
/// [`Self::error_result`] is Java `AddPartitionsToTxnRequest.getErrorResponse`
/// / `errorResponseForTopics` one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnPartitionsTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to add to the transaction.
    pub partitions: Vec<i32>,
}

impl TxnPartitionsTopic {
    /// Java `AddPartitionsToTxnRequest.getErrorResponse` /
    /// `errorResponseForTopics` one topic.
    ///
    /// Copies `Name` and each `PartitionIndex`. Nested body is
    /// PartitionIndex and PartitionErrorCode (no English message). This
    /// crate speaks v0–v3 (`ResultsByTopicV3AndBelow`); v4+ top-level
    /// ErrorCode is not spoken. Throttle on the response is the JSON
    /// default (`0`).
    #[must_use]
    pub fn error_result(&self, error_code: i16) -> AddPartitionsToTxnTopicResult {
        AddPartitionsToTxnTopicResult {
            topic: self.topic.clone(),
            partitions: self
                .partitions
                .iter()
                .copied()
                .map(|p| AddPartitionsToTxnPartitionResult::error(p, error_code))
                .collect(),
        }
    }

    /// Java `AddPartitionsToTxnRequest.getErrorResponse` /
    /// `errorResponseForTopics` Topics.
    ///
    /// Maps each request topic through [`Self::error_result`].
    #[must_use]
    pub fn error_results(topics: &[Self], error_code: i16) -> Vec<AddPartitionsToTxnTopicResult> {
        topics
            .iter()
            .map(|topic| topic.error_result(error_code))
            .collect()
    }
}

/// One partition in an AddPartitionsToTxn v0–v3 response.
///
/// [`Self::error`] is Java `AddPartitionsToTxnRequest.getErrorResponse`
/// / `errorResponseForTopics` partition body (PartitionIndex +
/// PartitionErrorCode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnPartitionResult {
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success). Java `PartitionErrorCode`.
    pub error_code: i16,
}

impl AddPartitionsToTxnPartitionResult {
    /// Java `AddPartitionsToTxnRequest.getErrorResponse` /
    /// `errorResponseForTopics` partition body.
    ///
    /// Sets `PartitionIndex` and `PartitionErrorCode`. The nested body
    /// has no error message field.
    #[must_use]
    pub fn error(partition: i32, error_code: i16) -> Self {
        Self {
            partition,
            error_code,
        }
    }

    /// Java `AddPartitionsToTxnPartitionResult.partitionIndex`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Java `AddPartitionsToTxnPartitionResult.partitionErrorCode`.
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }
}

/// One topic in an AddPartitionsToTxn v0–v3 response.
///
/// [`TxnPartitionsTopic::error_result`] is Java
/// `AddPartitionsToTxnRequest.getErrorResponse` /
/// `errorResponseForTopics` one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnTopicResult {
    /// Topic name (Java `Name`).
    pub topic: String,
    /// Per-partition results.
    pub partitions: Vec<AddPartitionsToTxnPartitionResult>,
}

impl AddPartitionsToTxnTopicResult {
    /// Java `AddPartitionsToTxnTopicResult.name`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Java `AddPartitionsToTxnTopicResult.resultsByPartition`.
    #[must_use]
    pub fn partitions(&self) -> &[AddPartitionsToTxnPartitionResult] {
        &self.partitions
    }
}

/// `true` when AddPartitionsToTxn `version` is flexible (v3).
///
/// v0–v2 are classic. v3 is compact strings/arrays plus tagged fields
/// (Apache JSON `flexibleVersions: "3+"`). v4+ (batched transactions,
/// broker-only layout) is not spoken.
fn add_partitions_to_txn_flexible(version: i16) -> Result<bool> {
    match version {
        0..=2 => Ok(false),
        3 => Ok(true),
        other => Err(Error::protocol(format!(
            "AddPartitionsToTxn version {other} is not implemented"
        ))),
    }
}

/// Java `AddPartitionsToTxnResponse` helpers.
pub struct AddPartitionsToTxnResponse;

impl AddPartitionsToTxnResponse {
    /// Java `AddPartitionsToTxnResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 1
    }

    /// Java `AddPartitionsToTxnResponse.errorCounts` for v0–v3
    /// (`resultsByTopicV3AndBelow`).
    ///
    /// Counts partition-level error codes (including `NONE`). v4+ also
    /// counts the top-level `errorCode`; this crate does not speak v4+.
    #[must_use]
    pub fn error_counts(topics: &[AddPartitionsToTxnTopicResult]) -> HashMap<i16, i32> {
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

/// Encode AddPartitionsToTxn v0–v2 (classic) or v3 (flexible).
pub fn encode_add_partitions_to_txn_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    topics: &[TxnPartitionsTopic],
) -> crate::error::Result<()> {
    let flexible = add_partitions_to_txn_flexible(version)?;
    buf::put_string(buf, flexible, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
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
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode AddPartitionsToTxn: `(transactional_id, producer_id, producer_epoch, topics)`.
pub fn decode_add_partitions_to_txn_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, i64, i16, Vec<TxnPartitionsTopic>)> {
    let flexible = add_partitions_to_txn_flexible(version)?;
    let tid = buf::get_string(buf, flexible)?.unwrap_or_default();
    let pid = buf::get_i64(buf)?;
    let epoch = buf::get_i16(buf)?;
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(TxnPartitionsTopic { topic, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((tid, pid, epoch, topics))
}

/// Encode AddPartitionsToTxn: one error code applied to every partition.
///
/// Applies `error` on every request partition via
/// [`TxnPartitionsTopic::error_results`]. Throttle is the JSON default
/// (`0`).
pub fn encode_add_partitions_to_txn_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[TxnPartitionsTopic],
    error: i16,
) -> Result<()> {
    encode_add_partitions_to_txn_topics_response(
        buf,
        version,
        &TxnPartitionsTopic::error_results(topics, error),
    )
}

/// Encode AddPartitionsToTxn v0–3 from response Topics.
///
/// Throttle is the JSON default (`0`) on every spoken version. Nested
/// body is PartitionIndex and PartitionErrorCode (`ResultsByTopicV3AndBelow`).
pub fn encode_add_partitions_to_txn_topics_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[AddPartitionsToTxnTopicResult],
) -> Result<()> {
    let flexible = add_partitions_to_txn_flexible(version)?;
    buf.put_i32(0);
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

/// Decode AddPartitionsToTxn: first non-zero partition error, or `0`.
pub fn decode_add_partitions_to_txn_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let topics = decode_add_partitions_to_txn_topics_response(buf, version)?;
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

/// Decode AddPartitionsToTxn: every v0–3 topic result.
pub fn decode_add_partitions_to_txn_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<AddPartitionsToTxnTopicResult>> {
    let flexible = add_partitions_to_txn_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(AddPartitionsToTxnPartitionResult {
                partition,
                error_code,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(AddPartitionsToTxnTopicResult { topic, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(topics)
}

/// `true` when AddOffsetsToTxn `version` is flexible (v3+).
///
/// v0–v2 are classic. v3–v4 are compact strings plus tagged fields
/// (Apache JSON `flexibleVersions: "3+"`). v4 is TRANSACTION_ABORTABLE
/// (KIP-890; same layout as v3). v5+ is not spoken.
fn add_offsets_to_txn_flexible(version: i16) -> Result<bool> {
    match version {
        0..=2 => Ok(false),
        3..=4 => Ok(true),
        other => Err(Error::protocol(format!(
            "AddOffsetsToTxn version {other} is not implemented"
        ))),
    }
}

/// Java `AddOffsetsToTxnResponse` helpers.
pub struct AddOffsetsToTxnResponse;

impl AddOffsetsToTxnResponse {
    /// Java `AddOffsetsToTxnResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 1
    }
}

/// Encode AddOffsetsToTxn v0–v2 (classic) or v3–v4 (flexible).
pub fn encode_add_offsets_to_txn_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
) -> crate::error::Result<()> {
    let flexible = add_offsets_to_txn_flexible(version)?;
    buf::put_string(buf, flexible, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf::put_string(buf, flexible, Some(group_id))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode AddOffsetsToTxn: `(transactional_id, group_id)`.
pub fn decode_add_offsets_to_txn_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String)> {
    let flexible = add_offsets_to_txn_flexible(version)?;
    let tid = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _pid = buf::get_i64(buf)?;
    let _epoch = buf::get_i16(buf)?;
    let gid = buf::get_string(buf, flexible)?.unwrap_or_default();
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((tid, gid))
}

/// Encode AddOffsetsToTxn: throttle `0` plus error code.
pub fn encode_add_offsets_to_txn_response(
    buf: &mut BytesMut,
    version: i16,
    error: i16,
) -> Result<()> {
    let flexible = add_offsets_to_txn_flexible(version)?;
    buf.put_i32(0);
    buf.put_i16(error);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode AddOffsetsToTxn: error code.
pub fn decode_add_offsets_to_txn_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let flexible = add_offsets_to_txn_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let err = buf::get_i16(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(err)
}

/// `true` when EndTxn `version` is flexible (v3+).
///
/// v0–v2 are classic. v3–v5 are compact strings plus tagged fields
/// (Apache JSON `flexibleVersions: "3+"`). v4 is TRANSACTION_ABORTABLE
/// (KIP-890; same request layout as v3). v5 adds ProducerId /
/// ProducerEpoch on the response (KIP-890 Part 2 epoch bump). v6+ is
/// not spoken.
fn end_txn_flexible(version: i16) -> Result<bool> {
    match version {
        0..=2 => Ok(false),
        3..=5 => Ok(true),
        other => Err(Error::protocol(format!(
            "EndTxn version {other} is not implemented"
        ))),
    }
}

/// Encode EndTxn v0–v2 (classic) or v3–v5 (flexible).
pub fn encode_end_txn_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> crate::error::Result<()> {
    let flexible = end_txn_flexible(version)?;
    buf::put_string(buf, flexible, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf.put_u8(u8::from(committed));
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode EndTxn: `(transactional_id, producer_id, producer_epoch, committed)`.
pub fn decode_end_txn_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, i64, i16, bool)> {
    let flexible = end_txn_flexible(version)?;
    let tid = buf::get_string(buf, flexible)?.unwrap_or_default();
    let pid = buf::get_i64(buf)?;
    let epoch = buf::get_i16(buf)?;
    let committed = buf::get_bool(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((tid, pid, epoch, committed))
}

/// Encode EndTxn: throttle `0`, error code, and v5+ producer id / epoch.
///
/// Java `EndTxnResponseData` defaults ProducerId / ProducerEpoch to
/// [`RecordBatch::NO_PRODUCER_ID`] / [`RecordBatch::NO_PRODUCER_EPOCH`].
pub fn encode_end_txn_response(
    buf: &mut BytesMut,
    version: i16,
    error: i16,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<()> {
    let flexible = end_txn_flexible(version)?;
    buf.put_i32(0);
    buf.put_i16(error);
    if version > EndTxnRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 {
        buf.put_i64(producer_id);
        buf.put_i16(producer_epoch);
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode EndTxn: `(error, producer_id, producer_epoch)`.
///
/// Below v5, producer id and epoch are [`RecordBatch::NO_PRODUCER_ID`] /
/// [`RecordBatch::NO_PRODUCER_EPOCH`] (JSON default `-1`).
pub fn decode_end_txn_response<B: Buf>(buf: &mut B, version: i16) -> Result<(i16, i64, i16)> {
    let flexible = end_txn_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let err = buf::get_i16(buf)?;
    let (producer_id, producer_epoch) =
        if version > EndTxnRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 {
            (buf::get_i64(buf)?, buf::get_i16(buf)?)
        } else {
            (RecordBatch::NO_PRODUCER_ID, RecordBatch::NO_PRODUCER_EPOCH)
        };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((err, producer_id, producer_epoch))
}

/// Java `TxnOffsetCommitRequest` version helpers (KIP-890 transaction V2).
pub struct TxnOffsetCommitRequest;

impl TxnOffsetCommitRequest {
    /// Java `TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`.
    pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 4;
}

/// Java `TxnOffsetCommitResponse` helpers.
pub struct TxnOffsetCommitResponse;

impl TxnOffsetCommitResponse {
    /// Java `TxnOffsetCommitResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 1
    }

    /// Java `TxnOffsetCommitResponse.errorCounts`.
    ///
    /// Counts partition-level error codes (including `NONE`).
    #[must_use]
    pub fn error_counts(topics: &[TxnOffsetCommitResponseTopic]) -> HashMap<i16, i32> {
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

/// One partition in TxnOffsetCommit v0–5.
///
/// [`Display`] is Java `TxnOffsetCommitRequest.CommittedOffset.toString`.
/// [`Self::leader_epoch`] is Java `CommittedOffset.leaderEpoch` (`None`
/// when [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetPartition {
    /// Partition index.
    pub partition: i32,
    /// Committed offset.
    pub offset: i64,
    /// Leader epoch (v2+), or [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub leader_epoch: i32,
    /// Commit metadata string.
    pub metadata: String,
}

impl TxnOffsetPartition {
    /// Partition `partition` at `offset` with `leader_epoch` and `metadata`.
    #[must_use]
    pub fn new(
        partition: i32,
        offset: i64,
        leader_epoch: i32,
        metadata: impl Into<String>,
    ) -> Self {
        Self {
            partition,
            offset,
            leader_epoch,
            metadata: metadata.into(),
        }
    }

    /// Partition index.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Java `CommittedOffset.offset`.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Java `CommittedOffset.leaderEpoch` (`None` when
    /// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`]; Java
    /// `RequestUtils.getLeaderEpoch`).
    #[must_use]
    pub fn leader_epoch(&self) -> Option<i32> {
        (self.leader_epoch != RecordBatch::NO_PARTITION_LEADER_EPOCH).then_some(self.leader_epoch)
    }

    /// Java `CommittedOffset.metadata`.
    #[must_use]
    pub fn metadata(&self) -> &str {
        self.metadata.as_str()
    }
}

impl fmt::Display for TxnOffsetPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommittedOffset(offset={}, leaderEpoch=", self.offset)?;
        write_java_optional(f, self.leader_epoch())?;
        write!(f, ", metadata='{}')", self.metadata)
    }
}

/// Java `Optional.toString` (`Optional[n]` / `Optional.empty`).
fn write_java_optional(f: &mut fmt::Formatter<'_>, v: Option<i32>) -> fmt::Result {
    match v {
        Some(n) => write!(f, "Optional[{n}]"),
        None => f.write_str("Optional.empty"),
    }
}

/// Topic + partitions for TxnOffsetCommit v0–5.
///
/// [`Self::error_result`] is Java `TxnOffsetCommitRequest.getErrorResponse`
/// / `getErrorResponseTopics` one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions in this topic.
    pub partitions: Vec<TxnOffsetPartition>,
}

impl TxnOffsetTopic {
    /// Java `TxnOffsetCommitRequest.getErrorResponse` /
    /// `getErrorResponseTopics` one topic.
    ///
    /// Copies `Name` and each `PartitionIndex`. Nested body is
    /// PartitionIndex + ErrorCode (no English message). Committed
    /// offset / metadata / leader epoch stay on the request. Throttle
    /// on the response is the JSON default (`0`).
    #[must_use]
    pub fn error_result(&self, error_code: i16) -> TxnOffsetCommitResponseTopic {
        TxnOffsetCommitResponseTopic {
            topic: self.topic.clone(),
            partitions: self
                .partitions
                .iter()
                .map(|p| TxnOffsetCommitResponsePartition::error(p.partition, error_code))
                .collect(),
        }
    }

    /// Java `TxnOffsetCommitRequest.getErrorResponse` /
    /// `getErrorResponseTopics` Topics.
    ///
    /// Maps each request topic through [`Self::error_result`].
    #[must_use]
    pub fn error_results(topics: &[Self], error_code: i16) -> Vec<TxnOffsetCommitResponseTopic> {
        topics
            .iter()
            .map(|topic| topic.error_result(error_code))
            .collect()
    }
}

/// One partition in a TxnOffsetCommit v0–5 response.
///
/// [`Self::error`] is Java `TxnOffsetCommitRequest.getErrorResponse`
/// / `getErrorResponseTopics` partition body (PartitionIndex + ErrorCode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitResponsePartition {
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl TxnOffsetCommitResponsePartition {
    /// Java `TxnOffsetCommitRequest.getErrorResponse` /
    /// `getErrorResponseTopics` partition body.
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

    /// Java `TxnOffsetCommitResponsePartition.partitionIndex`.
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

/// One topic in a TxnOffsetCommit v0–5 response.
///
/// [`TxnOffsetTopic::error_result`] is Java
/// `TxnOffsetCommitRequest.getErrorResponse` / `getErrorResponseTopics`
/// one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitResponseTopic {
    /// Topic name (Java `Name`).
    pub topic: String,
    /// Per-partition results.
    pub partitions: Vec<TxnOffsetCommitResponsePartition>,
}

impl TxnOffsetCommitResponseTopic {
    /// Java `TxnOffsetCommitResponseTopic.name`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Java `TxnOffsetCommitResponseTopic.partitions`.
    #[must_use]
    pub fn partitions(&self) -> &[TxnOffsetCommitResponsePartition] {
        &self.partitions
    }
}

/// Group member identity for TxnOffsetCommit v3+ (`generation.id`,
/// `member.id`, `group.instance.id`).
///
/// [`Self::unknown`] is Java `TxnOffsetCommitRequest.Builder` without
/// group metadata ([`JoinGroupRequest::UNKNOWN_GENERATION_ID`] /
/// [`JoinGroupRequest::UNKNOWN_MEMBER_ID`] / null instance). v0–v2
/// reject [`Self::group_metadata_set`] (Java `groupMetadataSet`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitMember {
    /// Classic generation, or [`JoinGroupRequest::UNKNOWN_GENERATION_ID`].
    pub generation_id: i32,
    /// Coordinator-assigned member id, or [`JoinGroupRequest::UNKNOWN_MEMBER_ID`].
    pub member_id: String,
    /// Kafka `group.instance.id`, if static membership is set.
    pub group_instance_id: Option<String>,
}

impl TxnOffsetCommitMember {
    /// Java `TxnOffsetCommitRequest.Builder` without group metadata.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            generation_id: JoinGroupRequest::UNKNOWN_GENERATION_ID,
            member_id: JoinGroupRequest::UNKNOWN_MEMBER_ID.into(),
            group_instance_id: None,
        }
    }

    /// Java `TxnOffsetCommitRequest.Builder.groupMetadataSet`.
    ///
    /// `Some` instance id (including empty) is present (`!= null`).
    #[must_use]
    pub fn group_metadata_set(&self) -> bool {
        self.member_id != JoinGroupRequest::UNKNOWN_MEMBER_ID
            || self.generation_id != JoinGroupRequest::UNKNOWN_GENERATION_ID
            || self.group_instance_id.is_some()
    }
}

/// `true` when TxnOffsetCommit `version` is flexible (v3+).
///
/// v0–v2 are classic (v2 adds committed leader epoch). v3–v5 are compact
/// strings/arrays plus tagged fields, and add GenerationId / MemberId /
/// GroupInstanceId (Apache JSON `flexibleVersions: "3+"`). v4 is
/// TRANSACTION_ABORTABLE (KIP-890; same layout as v3). v5 is the same
/// layout (KIP-890 Part 2 transaction V2: TxnOffsetCommit also performs
/// AddOffsetsToTxn). v6+ is not spoken.
fn txn_offset_commit_flexible(version: i16) -> Result<bool> {
    match version {
        0..=2 => Ok(false),
        3..=5 => Ok(true),
        other => Err(Error::protocol(format!(
            "TxnOffsetCommit version {other} is not implemented"
        ))),
    }
}

/// Encode TxnOffsetCommit v0–v2 (classic) or v3–v5 (flexible).
#[expect(
    clippy::too_many_arguments,
    reason = "TxnOffsetCommit request body needs version, ids, member identity, and topics together"
)]
pub fn encode_txn_offset_commit_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: &str,
    group_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    member: &TxnOffsetCommitMember,
    topics: &[TxnOffsetTopic],
) -> crate::error::Result<()> {
    let flexible = txn_offset_commit_flexible(version)?;
    if version < 3 && member.group_metadata_set() {
        return Err(Error::Unsupported(format!(
            "Broker doesn't support group metadata commit API on version {version}, minimum supported request version is 3 which requires brokers to be on version 2.5 or above."
        )));
    }
    buf::put_string(buf, flexible, Some(transactional_id))?;
    buf::put_string(buf, flexible, Some(group_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    if version >= 3 {
        buf.put_i32(member.generation_id);
        buf::put_string(buf, flexible, Some(&member.member_id))?;
        buf::put_string(buf, flexible, member.group_instance_id.as_deref())?;
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i64(p.offset);
            if version >= 2 {
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

/// Decode TxnOffsetCommit: `(transactional_id, group_id, member, topics)`.
///
/// Decode below v2 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] for
/// omitted `CommittedLeaderEpoch`.
pub fn decode_txn_offset_commit_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, TxnOffsetCommitMember, Vec<TxnOffsetTopic>)> {
    let flexible = txn_offset_commit_flexible(version)?;
    let tid = buf::get_string(buf, flexible)?.unwrap_or_default();
    let gid = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _pid = buf::get_i64(buf)?;
    let _epoch = buf::get_i16(buf)?;
    let member = if version >= 3 {
        let generation_id = buf::get_i32(buf)?;
        let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
        let group_instance_id = buf::get_string(buf, flexible)?;
        TxnOffsetCommitMember {
            generation_id,
            member_id,
            group_instance_id,
        }
    } else {
        TxnOffsetCommitMember::unknown()
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
            let leader_epoch = if version >= 2 {
                buf::get_i32(buf)?
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
            let metadata = buf::get_string(buf, flexible)?.unwrap_or_default();
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(TxnOffsetPartition {
                partition,
                offset,
                leader_epoch,
                metadata,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(TxnOffsetTopic { topic, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((tid, gid, member, topics))
}

/// Encode TxnOffsetCommit: one error code applied to every partition.
///
/// Applies `error` on every request partition via
/// [`TxnOffsetTopic::error_results`]. Throttle is the JSON default (`0`).
pub fn encode_txn_offset_commit_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[TxnOffsetTopic],
    error: i16,
) -> Result<()> {
    encode_txn_offset_commit_topics_response(
        buf,
        version,
        &TxnOffsetTopic::error_results(topics, error),
    )
}

/// Encode TxnOffsetCommit v0–5 from response Topics.
///
/// Throttle is the JSON default (`0`) on every spoken version. Nested
/// body is PartitionIndex + ErrorCode.
pub fn encode_txn_offset_commit_topics_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[TxnOffsetCommitResponseTopic],
) -> Result<()> {
    let flexible = txn_offset_commit_flexible(version)?;
    buf.put_i32(0);
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

/// Decode TxnOffsetCommit: first non-zero partition error, or `0`.
pub fn decode_txn_offset_commit_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let topics = decode_txn_offset_commit_topics_response(buf, version)?;
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

/// Decode TxnOffsetCommit: every response topic.
pub fn decode_txn_offset_commit_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
    let flexible = txn_offset_commit_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(TxnOffsetCommitResponsePartition {
                partition,
                error_code,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(TxnOffsetCommitResponseTopic { topic, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(topics)
}

/// One topic in a WriteTxnMarkers marker (api 27 v0–1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableTxnMarkerTopic {
    /// Topic name.
    pub name: String,
    /// Partition indexes to write the marker on.
    pub partitions: Vec<i32>,
}

/// One transaction marker in WriteTxnMarkers v0–1.
///
/// v0 is classic. v1 is flexible (Kafka 4.0 baseline). v2
/// `TransactionVersion` (KIP-1228) is not spoken.
///
/// [`Display`] is Java `WriteTxnMarkersRequest.TxnMarkerEntry.toString`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableTxnMarker {
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// `true` is [`TransactionResult::Commit`], `false` is
    /// [`TransactionResult::Abort`] (Java `TransactionResult.id`).
    pub transaction_result: bool,
    /// Topics and partitions that receive the marker.
    pub topics: Vec<WritableTxnMarkerTopic>,
    /// Transaction coordinator epoch.
    pub coordinator_epoch: i32,
}

impl WritableTxnMarker {
    /// Per-partition result with the same layout and `error_code`.
    #[must_use]
    pub fn result(&self, error_code: i16) -> WritableTxnMarkerResult {
        WritableTxnMarkerResult {
            producer_id: self.producer_id,
            topics: self
                .topics
                .iter()
                .map(|t| WritableTxnMarkerTopicResult {
                    name: t.name.clone(),
                    partitions: t
                        .partitions
                        .iter()
                        .map(|&partition_index| WritableTxnMarkerPartitionResult {
                            partition_index,
                            error_code,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

impl fmt::Display for WritableTxnMarker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TxnMarkerEntry{producerId=")?;
        write!(f, "{}", self.producer_id)?;
        f.write_str(", producerEpoch=")?;
        write!(f, "{}", self.producer_epoch)?;
        f.write_str(", coordinatorEpoch=")?;
        write!(f, "{}", self.coordinator_epoch)?;
        f.write_str(", result=")?;
        f.write_str(TransactionResult::from_id(self.transaction_result).as_str())?;
        f.write_str(", partitions=[")?;
        let mut first = true;
        for topic in &self.topics {
            for partition in &topic.partitions {
                if !first {
                    f.write_str(", ")?;
                }
                first = false;
                write!(f, "{}-{}", topic.name, partition)?;
            }
        }
        f.write_str("]}")
    }
}

/// One partition in a WriteTxnMarkers response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableTxnMarkerPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

/// One topic in a WriteTxnMarkers response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableTxnMarkerTopicResult {
    /// Topic name.
    pub name: String,
    /// Per-partition results.
    pub partitions: Vec<WritableTxnMarkerPartitionResult>,
}

/// One marker in a WriteTxnMarkers response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableTxnMarkerResult {
    /// Producer id from the request.
    pub producer_id: i64,
    /// Per-topic results.
    pub topics: Vec<WritableTxnMarkerTopicResult>,
}

/// `true` when WriteTxnMarkers `version` is flexible (v1).
///
/// v0 is classic. v1 is compact arrays/strings plus tagged fields
/// (Apache JSON `flexibleVersions: "1+"`). v2 adds `TransactionVersion`
/// (KIP-1228) and is not implemented.
fn write_txn_markers_flexible(version: i16) -> Result<bool> {
    match version {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(Error::protocol(format!(
            "WriteTxnMarkers version {other} is not implemented"
        ))),
    }
}

/// WriteTxnMarkers v0 (classic) or v1 (flexible). v2 is not implemented.
pub fn encode_write_txn_markers_request(
    buf: &mut BytesMut,
    version: i16,
    markers: &[WritableTxnMarker],
) -> Result<()> {
    let flexible = write_txn_markers_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(markers.len()))?;
    for m in markers {
        buf.put_i64(m.producer_id);
        buf.put_i16(m.producer_epoch);
        buf.put_u8(u8::from(m.transaction_result));
        buf::put_array_len(buf, flexible, Some(m.topics.len()))?;
        for t in &m.topics {
            buf::put_string(buf, flexible, Some(&t.name))?;
            buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
            for p in &t.partitions {
                buf.put_i32(*p);
            }
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        buf.put_i32(m.coordinator_epoch);
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode WriteTxnMarkers v0 (classic) or v1 (flexible).
pub fn decode_write_txn_markers_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<WritableTxnMarker>> {
    let flexible = write_txn_markers_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut markers = Vec::with_capacity(n);
    for _ in 0..n {
        let producer_id = buf::get_i64(buf)?;
        let producer_epoch = buf::get_i16(buf)?;
        let transaction_result = buf::get_bool(buf)?;
        let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let name = buf::get_string(buf, flexible)?.unwrap_or_default();
            let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                partitions.push(buf::get_i32(buf)?);
            }
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            topics.push(WritableTxnMarkerTopic { name, partitions });
        }
        let coordinator_epoch = buf::get_i32(buf)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        markers.push(WritableTxnMarker {
            producer_id,
            producer_epoch,
            transaction_result,
            topics,
            coordinator_epoch,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(markers)
}

/// Encode WriteTxnMarkers v0 (classic) or v1 (flexible).
pub fn encode_write_txn_markers_response(
    buf: &mut BytesMut,
    version: i16,
    markers: &[WritableTxnMarkerResult],
) -> Result<()> {
    let flexible = write_txn_markers_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(markers.len()))?;
    for m in markers {
        buf.put_i64(m.producer_id);
        buf::put_array_len(buf, flexible, Some(m.topics.len()))?;
        for t in &m.topics {
            buf::put_string(buf, flexible, Some(&t.name))?;
            buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
            for p in &t.partitions {
                buf.put_i32(p.partition_index);
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
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode WriteTxnMarkers v0 (classic) or v1 (flexible).
pub fn decode_write_txn_markers_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<WritableTxnMarkerResult>> {
    let flexible = write_txn_markers_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut markers = Vec::with_capacity(n);
    for _ in 0..n {
        let producer_id = buf::get_i64(buf)?;
        let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let name = buf::get_string(buf, flexible)?.unwrap_or_default();
            let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                let error_code = buf::get_i16(buf)?;
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                partitions.push(WritableTxnMarkerPartitionResult {
                    partition_index,
                    error_code,
                });
            }
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            topics.push(WritableTxnMarkerTopicResult { name, partitions });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        markers.push(WritableTxnMarkerResult {
            producer_id,
            topics,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(markers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn transaction_v2_version_caps_match_java() {
        assert_eq!(EndTxnRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2, 4);
        assert_eq!(
            TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2,
            4
        );
        assert!(!AddPartitionsToTxnResponse::should_client_throttle(0));
        assert!(AddPartitionsToTxnResponse::should_client_throttle(1));
        assert!(!AddOffsetsToTxnResponse::should_client_throttle(0));
        assert!(AddOffsetsToTxnResponse::should_client_throttle(1));
        assert!(!EndTxnResponse::should_client_throttle(0));
        assert!(EndTxnResponse::should_client_throttle(1));
        assert!(!TxnOffsetCommitResponse::should_client_throttle(0));
        assert!(TxnOffsetCommitResponse::should_client_throttle(1));
    }

    #[test]
    fn txn_offset_commit_response_error_counts_matches_java() {
        assert!(TxnOffsetCommitResponse::error_counts(&[]).is_empty());
        let counts = TxnOffsetCommitResponse::error_counts(&[
            TxnOffsetCommitResponseTopic {
                topic: "ok".into(),
                partitions: vec![
                    TxnOffsetCommitResponsePartition::error(0, 0),
                    TxnOffsetCommitResponsePartition::error(
                        1,
                        crate::error::NOT_LEADER_OR_FOLLOWER,
                    ),
                ],
            },
            TxnOffsetCommitResponseTopic {
                topic: "missing".into(),
                partitions: vec![TxnOffsetCommitResponsePartition::error(
                    0,
                    crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                )],
            },
            TxnOffsetCommitResponseTopic {
                topic: "ok2".into(),
                partitions: vec![TxnOffsetCommitResponsePartition::error(0, 0)],
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
    fn add_partitions_to_txn_response_error_counts_matches_java() {
        assert!(AddPartitionsToTxnResponse::error_counts(&[]).is_empty());
        let counts = AddPartitionsToTxnResponse::error_counts(&[
            AddPartitionsToTxnTopicResult {
                topic: "ok".into(),
                partitions: vec![
                    AddPartitionsToTxnPartitionResult::error(0, 0),
                    AddPartitionsToTxnPartitionResult::error(
                        1,
                        crate::error::NOT_LEADER_OR_FOLLOWER,
                    ),
                ],
            },
            AddPartitionsToTxnTopicResult {
                topic: "missing".into(),
                partitions: vec![AddPartitionsToTxnPartitionResult::error(
                    0,
                    crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                )],
            },
            AddPartitionsToTxnTopicResult {
                topic: "ok2".into(),
                partitions: vec![AddPartitionsToTxnPartitionResult::error(0, 0)],
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
    fn transaction_result_matches_java() {
        assert!(!TransactionResult::Abort.id());
        assert!(TransactionResult::Commit.id());
        assert_eq!(TransactionResult::from_id(false), TransactionResult::Abort);
        assert_eq!(TransactionResult::from_id(true), TransactionResult::Commit);
        assert_eq!(TransactionResult::Abort.to_string(), "ABORT");
        assert_eq!(TransactionResult::Commit.to_string(), "COMMIT");
        let mut buf = BytesMut::new();
        encode_end_txn_request(&mut buf, 0, "tx", 9, 1, TransactionResult::Commit.id()).unwrap();
        let mut cur = buf.as_ref();
        let (_, _, _, committed) = decode_end_txn_request(&mut cur, 0).unwrap();
        assert!(committed);
        assert_eq!(
            TransactionResult::from_id(committed),
            TransactionResult::Commit
        );
        let marker = WritableTxnMarker {
            producer_id: 1000,
            producer_epoch: 0,
            transaction_result: TransactionResult::Abort.id(),
            topics: vec![WritableTxnMarkerTopic {
                name: "t".into(),
                partitions: vec![0, 1],
            }],
            coordinator_epoch: 1,
        };
        assert_eq!(
            marker.to_string(),
            "TxnMarkerEntry{producerId=1000, producerEpoch=0, coordinatorEpoch=1, result=ABORT, partitions=[t-0, t-1]}"
        );
        let commit = WritableTxnMarker {
            producer_id: 1,
            producer_epoch: 2,
            transaction_result: TransactionResult::Commit.id(),
            topics: vec![],
            coordinator_epoch: 3,
        };
        assert_eq!(
            commit.to_string(),
            "TxnMarkerEntry{producerId=1, producerEpoch=2, coordinatorEpoch=3, result=COMMIT, partitions=[]}"
        );
    }

    #[test]
    fn end_txn_roundtrip() {
        let mut buf = BytesMut::new();
        encode_end_txn_request(&mut buf, 0, "tx", 9, 1, true).unwrap();
        let mut cur = &buf[..];
        let (tid, pid, epoch, committed) = decode_end_txn_request(&mut cur, 0).unwrap();
        assert_eq!((tid.as_str(), pid, epoch, committed), ("tx", 9, 1, true));
        assert!(cur.is_empty());
        let mut resp = BytesMut::new();
        encode_end_txn_response(
            &mut resp,
            0,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        let mut cur = &resp[..];
        assert_eq!(
            decode_end_txn_response(&mut cur, 0).unwrap(),
            (
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH
            )
        );
        assert!(cur.is_empty());
    }

    #[test]
    fn end_txn_v3_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_end_txn_request(&mut req, 3, "tx", 9, 1, true).unwrap();
        let mut cur = &req[..];
        let (tid, pid, epoch, committed) = decode_end_txn_request(&mut cur, 3).unwrap();
        assert_eq!((tid.as_str(), pid, epoch, committed), ("tx", 9, 1, true));
        assert!(
            cur.is_empty(),
            "EndTxn v3 request must consume compact tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_end_txn_response(
            &mut resp,
            3,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        let mut cur = &resp[..];
        assert_eq!(
            decode_end_txn_response(&mut cur, 3).unwrap(),
            (
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH
            )
        );
        assert!(
            cur.is_empty(),
            "EndTxn v3 response must consume compact tagged fields"
        );

        req.clear();
        encode_end_txn_request(&mut req, 4, "tx", 9, 1, false).unwrap();
        let mut cur = &req[..];
        let (tid, pid, epoch, committed) = decode_end_txn_request(&mut cur, 4).unwrap();
        assert_eq!((tid.as_str(), pid, epoch, committed), ("tx", 9, 1, false));
        assert!(cur.is_empty(), "EndTxn v4 shares the v3 layout");

        req.clear();
        encode_end_txn_request(&mut req, 5, "tx", 9, 1, true).unwrap();
        let mut cur = &req[..];
        let (tid, pid, epoch, committed) = decode_end_txn_request(&mut cur, 5).unwrap();
        assert_eq!((tid.as_str(), pid, epoch, committed), ("tx", 9, 1, true));
        assert!(cur.is_empty(), "EndTxn v5 request shares the v3 layout");

        let mut resp = BytesMut::new();
        encode_end_txn_response(&mut resp, 5, 0, 9, 2).unwrap();
        let mut cur = &resp[..];
        assert_eq!(decode_end_txn_response(&mut cur, 5).unwrap(), (0, 9, 2));
        assert!(
            cur.is_empty(),
            "EndTxn v5 response must consume ProducerId, ProducerEpoch, and tagged fields"
        );
        req.clear();
        assert!(
            encode_end_txn_request(&mut req, 6, "tx", 9, 1, true).is_err(),
            "EndTxn v6+ is not spoken"
        );
    }

    #[test]
    fn end_txn_v3_request_matches_compact_layout() {
        // Compact "tx", pid 9, epoch 1, committed true, tagged.
        const REQ: &[u8] = &[
            0x03, 0x74, 0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x01, 0x01,
            0x00,
        ];
        let mut buf = BytesMut::new();
        encode_end_txn_request(&mut buf, 3, "tx", 9, 1, true).unwrap();
        assert_eq!(&buf[..], REQ);
    }

    #[test]
    fn end_txn_v3_response_matches_compact_layout() {
        // Throttle 0, error 0, tagged.
        const RESP: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut buf = BytesMut::new();
        encode_end_txn_response(
            &mut buf,
            3,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        assert_eq!(&buf[..], RESP);
    }

    #[test]
    fn end_txn_v5_response_matches_compact_layout() {
        // Throttle 0, error 0, pid 9, epoch 2, tagged.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
            0x00, 0x02, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_end_txn_response(&mut buf, 5, 0, 9, 2).unwrap();
        assert_eq!(&buf[..], RESP);
    }

    #[test]
    fn txn_offset_partition_matches_java_committed_offset() {
        let p = TxnOffsetPartition::new(0, 7, 9, "");
        assert_eq!(p.partition(), 0);
        assert_eq!(p.offset(), 7);
        assert_eq!(p.leader_epoch(), Some(9));
        assert_eq!(p.metadata(), "");
        assert_eq!(
            p.to_string(),
            "CommittedOffset(offset=7, leaderEpoch=Optional[9], metadata='')"
        );
        let empty = TxnOffsetPartition::new(1, 3, RecordBatch::NO_PARTITION_LEADER_EPOCH, "m");
        assert_eq!(empty.leader_epoch(), None);
        assert_eq!(
            empty.to_string(),
            "CommittedOffset(offset=3, leaderEpoch=Optional.empty, metadata='m')"
        );
    }

    #[test]
    fn txn_offset_commit_member_matches_java() {
        let unknown = TxnOffsetCommitMember::unknown();
        assert_eq!(
            unknown.generation_id,
            JoinGroupRequest::UNKNOWN_GENERATION_ID
        );
        assert_eq!(unknown.member_id, JoinGroupRequest::UNKNOWN_MEMBER_ID);
        assert!(unknown.group_instance_id.is_none());
        assert!(!unknown.group_metadata_set());
        assert!(TxnOffsetCommitMember {
            generation_id: 1,
            member_id: JoinGroupRequest::UNKNOWN_MEMBER_ID.into(),
            group_instance_id: None,
        }
        .group_metadata_set());
        assert!(TxnOffsetCommitMember {
            generation_id: JoinGroupRequest::UNKNOWN_GENERATION_ID,
            member_id: "m".into(),
            group_instance_id: None,
        }
        .group_metadata_set());
        assert!(TxnOffsetCommitMember {
            generation_id: JoinGroupRequest::UNKNOWN_GENERATION_ID,
            member_id: JoinGroupRequest::UNKNOWN_MEMBER_ID.into(),
            group_instance_id: Some("worker-1".into()),
        }
        .group_metadata_set());
        assert!(
            TxnOffsetCommitMember {
                generation_id: JoinGroupRequest::UNKNOWN_GENERATION_ID,
                member_id: JoinGroupRequest::UNKNOWN_MEMBER_ID.into(),
                group_instance_id: Some(String::new()),
            }
            .group_metadata_set(),
            "Java groupInstanceId != null is true for empty Optional.of(\"\")"
        );

        let topics = [TxnOffsetTopic {
            topic: "t".into(),
            partitions: vec![TxnOffsetPartition {
                partition: 0,
                offset: 7,
                leader_epoch: 9,
                metadata: String::new(),
            }],
        }];
        let member = TxnOffsetCommitMember {
            generation_id: 7,
            member_id: "m".into(),
            group_instance_id: None,
        };
        let err = encode_txn_offset_commit_request(
            &mut BytesMut::new(),
            2,
            "tx",
            "g",
            9,
            1,
            &member,
            &topics,
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "v2 with group metadata is Java UnsupportedVersionException, got {err}"
        );
        encode_txn_offset_commit_request(
            &mut BytesMut::new(),
            2,
            "tx",
            "g",
            9,
            1,
            &TxnOffsetCommitMember::unknown(),
            &topics,
        )
        .unwrap();
    }

    #[test]
    fn txn_offset_commit_v0_has_no_leader_epoch() {
        let topics = vec![TxnOffsetTopic {
            topic: "t".into(),
            partitions: vec![TxnOffsetPartition {
                partition: 0,
                offset: 7,
                leader_epoch: 9,
                metadata: String::new(),
            }],
        }];
        let mut buf = BytesMut::new();
        encode_txn_offset_commit_request(
            &mut buf,
            0,
            "tx",
            "g",
            9,
            1,
            &TxnOffsetCommitMember::unknown(),
            &topics,
        )
        .unwrap();
        let mut cur = &buf[..];
        let (tid, gid, member, got) = decode_txn_offset_commit_request(&mut cur, 0).unwrap();
        assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
        assert_eq!(member, TxnOffsetCommitMember::unknown());
        let part = got
            .first()
            .and_then(|t| t.partitions.first())
            .expect("one partition");
        assert_eq!(part.partition, 0);
        assert_eq!(part.offset, 7);
        assert_eq!(
            part.leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            "v0 must not write committed_leader_epoch"
        );
        assert!(
            cur.is_empty(),
            "v0 decoder must consume metadata; leftover {} bytes means an extra i32",
            cur.len()
        );
    }

    #[test]
    fn txn_offset_commit_v2_batches_and_sends_leader_epoch() {
        let topics = vec![TxnOffsetTopic {
            topic: "t".into(),
            partitions: vec![
                TxnOffsetPartition {
                    partition: 0,
                    offset: 3,
                    leader_epoch: 4,
                    metadata: "eos".into(),
                },
                TxnOffsetPartition {
                    partition: 2,
                    offset: 9,
                    leader_epoch: 4,
                    metadata: String::new(),
                },
            ],
        }];
        let mut buf = BytesMut::new();
        encode_txn_offset_commit_request(
            &mut buf,
            2,
            "tx",
            "g",
            9,
            1,
            &TxnOffsetCommitMember::unknown(),
            &topics,
        )
        .unwrap();
        let mut cur = &buf[..];
        let (tid, gid, member, got) = decode_txn_offset_commit_request(&mut cur, 2).unwrap();
        assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
        assert_eq!(member, TxnOffsetCommitMember::unknown());
        assert_eq!(got, topics);
        assert!(
            cur.is_empty(),
            "v2 decoder must consume leader epoch and metadata; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_txn_offset_commit_response(&mut buf, 2, &topics, 0).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_txn_offset_commit_response(&mut cur, 2).unwrap(), 0);
        assert!(cur.is_empty());
    }

    #[test]
    fn txn_offset_commit_v3_roundtrip_is_leftover_empty() {
        let member = TxnOffsetCommitMember {
            generation_id: 7,
            member_id: "m".into(),
            group_instance_id: Some("i".into()),
        };
        let topics = vec![TxnOffsetTopic {
            topic: "t".into(),
            partitions: vec![TxnOffsetPartition {
                partition: 0,
                offset: 7,
                leader_epoch: 9,
                metadata: String::new(),
            }],
        }];
        let mut req = BytesMut::new();
        encode_txn_offset_commit_request(&mut req, 3, "tx", "g", 9, 1, &member, &topics).unwrap();
        let mut cur = &req[..];
        let (tid, gid, got_member, got) = decode_txn_offset_commit_request(&mut cur, 3).unwrap();
        assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
        assert_eq!(got_member, member);
        assert_eq!(got, topics);
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v3 request must consume compact tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_txn_offset_commit_response(&mut resp, 3, &topics, 0).unwrap();
        let mut cur = &resp[..];
        assert_eq!(decode_txn_offset_commit_response(&mut cur, 3).unwrap(), 0);
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v3 response must consume compact tagged fields"
        );

        req.clear();
        encode_txn_offset_commit_request(&mut req, 4, "tx", "g", 9, 1, &member, &topics).unwrap();
        let mut cur = &req[..];
        let (_tid, _gid, got_member, got) = decode_txn_offset_commit_request(&mut cur, 4).unwrap();
        assert_eq!(got_member, member);
        assert_eq!(got, topics);
        assert!(cur.is_empty(), "TxnOffsetCommit v4 shares the v3 layout");

        req.clear();
        encode_txn_offset_commit_request(&mut req, 5, "tx", "g", 9, 1, &member, &topics).unwrap();
        let mut cur = &req[..];
        let (_tid, _gid, got_member, got) = decode_txn_offset_commit_request(&mut cur, 5).unwrap();
        assert_eq!(got_member, member);
        assert_eq!(got, topics);
        assert!(cur.is_empty(), "TxnOffsetCommit v5 shares the v3 layout");
        req.clear();
        assert!(
            encode_txn_offset_commit_request(&mut req, 6, "tx", "g", 9, 1, &member, &topics)
                .is_err(),
            "TxnOffsetCommit v6+ is not spoken"
        );
    }

    #[test]
    fn txn_offset_commit_v3_request_matches_compact_layout() {
        // Compact "tx"/"g", pid 9, epoch 1, generation -1, empty member,
        // null instance, one topic "t" partition 0 offset 7 epoch 9,
        // null metadata, tagged.
        const REQ: &[u8] = &[
            0x03, 0x74, 0x78, 0x02, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00,
            0x01, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x09, 0x00,
            0x00, 0x00, 0x00,
        ];
        let topics = [TxnOffsetTopic {
            topic: "t".into(),
            partitions: vec![TxnOffsetPartition {
                partition: 0,
                offset: 7,
                leader_epoch: 9,
                metadata: String::new(),
            }],
        }];
        let mut buf = BytesMut::new();
        encode_txn_offset_commit_request(
            &mut buf,
            3,
            "tx",
            "g",
            9,
            1,
            &TxnOffsetCommitMember::unknown(),
            &topics,
        )
        .unwrap();
        assert_eq!(&buf[..], REQ);
    }

    #[test]
    fn txn_offset_commit_v3_response_matches_compact_layout() {
        // Throttle 0, one topic "t" partition 0 error 0, tagged.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00,
        ];
        let topics = [TxnOffsetTopic {
            topic: "t".into(),
            partitions: vec![TxnOffsetPartition {
                partition: 0,
                offset: 7,
                leader_epoch: 9,
                metadata: String::new(),
            }],
        }];
        let mut buf = BytesMut::new();
        encode_txn_offset_commit_response(&mut buf, 3, &topics, 0).unwrap();
        assert_eq!(&buf[..], RESP);
    }

    #[test]
    fn txn_offset_commit_get_error_response_copies_names_and_partitions() {
        let topics = [
            TxnOffsetTopic {
                topic: "orders".into(),
                partitions: vec![
                    TxnOffsetPartition::new(0, 10, 1, "m0"),
                    TxnOffsetPartition::new(1, 20, 2, "m1"),
                ],
            },
            TxnOffsetTopic {
                topic: "payments".into(),
                partitions: vec![TxnOffsetPartition::new(
                    2,
                    30,
                    RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    "",
                )],
            },
        ];
        let err =
            TxnOffsetTopic::error_results(&topics, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert_eq!(err.len(), 2);
        let orders = err.first().expect("orders");
        assert_eq!(orders.topic(), "orders");
        assert_eq!(
            orders.partitions(),
            [
                TxnOffsetCommitResponsePartition::error(
                    0,
                    crate::error::CLUSTER_AUTHORIZATION_FAILED
                ),
                TxnOffsetCommitResponsePartition::error(
                    1,
                    crate::error::CLUSTER_AUTHORIZATION_FAILED
                ),
            ]
        );
        let payments = err.get(1).expect("payments");
        assert_eq!(payments.topic(), "payments");
        assert_eq!(
            payments.partitions(),
            [TxnOffsetCommitResponsePartition::error(
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
        for version in [0i16, 1, 2, 3, 5] {
            let mut buf = BytesMut::new();
            encode_txn_offset_commit_topics_response(&mut buf, version, &err).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(
                decode_txn_offset_commit_topics_response(&mut cur, version).unwrap(),
                err
            );
            assert!(
                !cur.has_remaining(),
                "TxnOffsetCommit v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            assert_eq!(
                decode_txn_offset_commit_response(&mut buf.as_ref(), version).unwrap(),
                crate::error::CLUSTER_AUTHORIZATION_FAILED
            );
        }
        let empty = TxnOffsetTopic::error_results(&[], crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert!(empty.is_empty());
        for version in [0i16, 1, 2, 3, 5] {
            let mut buf = BytesMut::new();
            encode_txn_offset_commit_topics_response(&mut buf, version, &empty).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(
                decode_txn_offset_commit_topics_response(&mut cur, version).unwrap(),
                empty
            );
            assert!(
                !cur.has_remaining(),
                "TxnOffsetCommit v{version} empty getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            assert_eq!(
                decode_txn_offset_commit_response(&mut buf.as_ref(), version).unwrap(),
                0
            );
        }
    }

    #[test]
    fn add_partitions_to_txn_batches_partitions() {
        let topics = vec![TxnPartitionsTopic {
            topic: "t".into(),
            partitions: vec![0, 1, 2],
        }];
        let mut buf = BytesMut::new();
        encode_add_partitions_to_txn_request(&mut buf, 1, "tx", 9, 1, &topics).unwrap();
        let mut cur = &buf[..];
        let (tid, pid, epoch, got) = decode_add_partitions_to_txn_request(&mut cur, 1).unwrap();
        assert_eq!((tid.as_str(), pid, epoch), ("tx", 9, 1));
        assert_eq!(got, topics);
        assert!(cur.is_empty());

        buf.clear();
        encode_add_partitions_to_txn_response(&mut buf, 1, &topics, 0).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_add_partitions_to_txn_response(&mut cur, 1).unwrap(),
            0
        );
        assert!(cur.is_empty());
    }

    #[test]
    fn add_partitions_to_txn_v3_roundtrip_is_leftover_empty() {
        let topics = vec![TxnPartitionsTopic {
            topic: "t".into(),
            partitions: vec![0, 1],
        }];
        let mut req = BytesMut::new();
        encode_add_partitions_to_txn_request(&mut req, 3, "tx", 9, 1, &topics).unwrap();
        let mut cur = &req[..];
        let (tid, pid, epoch, got) = decode_add_partitions_to_txn_request(&mut cur, 3).unwrap();
        assert_eq!((tid.as_str(), pid, epoch), ("tx", 9, 1));
        assert_eq!(got, topics);
        assert!(
            cur.is_empty(),
            "AddPartitionsToTxn v3 request must consume compact tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_add_partitions_to_txn_response(&mut resp, 3, &topics, 0).unwrap();
        let mut cur = &resp[..];
        assert_eq!(
            decode_add_partitions_to_txn_response(&mut cur, 3).unwrap(),
            0
        );
        assert!(
            cur.is_empty(),
            "AddPartitionsToTxn v3 response must consume compact tagged fields"
        );
        req.clear();
        assert!(
            encode_add_partitions_to_txn_request(&mut req, 4, "tx", 9, 1, &topics).is_err(),
            "AddPartitionsToTxn v4+ (batched transactions) is not spoken"
        );
    }

    #[test]
    fn add_partitions_to_txn_v3_request_matches_compact_layout() {
        // Compact "tx", pid 9, epoch 1, one topic "t" partition 0, tagged.
        const REQ: &[u8] = &[
            0x03, 0x74, 0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x01, 0x02,
            0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let topics = [TxnPartitionsTopic {
            topic: "t".into(),
            partitions: vec![0],
        }];
        let mut buf = BytesMut::new();
        encode_add_partitions_to_txn_request(&mut buf, 3, "tx", 9, 1, &topics).unwrap();
        assert_eq!(&buf[..], REQ);
    }

    #[test]
    fn add_partitions_to_txn_v3_response_matches_compact_layout() {
        // Throttle 0, one topic "t" partition 0 error 0, tagged.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00,
        ];
        let topics = [TxnPartitionsTopic {
            topic: "t".into(),
            partitions: vec![0],
        }];
        let mut buf = BytesMut::new();
        encode_add_partitions_to_txn_response(&mut buf, 3, &topics, 0).unwrap();
        assert_eq!(&buf[..], RESP);
    }

    #[test]
    fn add_partitions_to_txn_get_error_response_copies_names_and_partitions() {
        let topics = [
            TxnPartitionsTopic {
                topic: "orders".into(),
                partitions: vec![0, 1],
            },
            TxnPartitionsTopic {
                topic: "payments".into(),
                partitions: vec![2],
            },
        ];
        let err =
            TxnPartitionsTopic::error_results(&topics, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert_eq!(err.len(), 2);
        let orders = err.first().expect("orders");
        assert_eq!(orders.topic(), "orders");
        assert_eq!(
            orders.partitions(),
            [
                AddPartitionsToTxnPartitionResult::error(
                    0,
                    crate::error::CLUSTER_AUTHORIZATION_FAILED
                ),
                AddPartitionsToTxnPartitionResult::error(
                    1,
                    crate::error::CLUSTER_AUTHORIZATION_FAILED
                ),
            ]
        );
        let payments = err.get(1).expect("payments");
        assert_eq!(payments.topic(), "payments");
        assert_eq!(
            payments.partitions(),
            [AddPartitionsToTxnPartitionResult::error(
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
        for version in [0i16, 1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_add_partitions_to_txn_topics_response(&mut buf, version, &err).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(
                decode_add_partitions_to_txn_topics_response(&mut cur, version).unwrap(),
                err
            );
            assert!(
                !cur.has_remaining(),
                "AddPartitionsToTxn v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            assert_eq!(
                decode_add_partitions_to_txn_response(&mut buf.as_ref(), version).unwrap(),
                crate::error::CLUSTER_AUTHORIZATION_FAILED
            );
        }
        let empty =
            TxnPartitionsTopic::error_results(&[], crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert!(empty.is_empty());
        for version in [0i16, 1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_add_partitions_to_txn_topics_response(&mut buf, version, &empty).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(
                decode_add_partitions_to_txn_topics_response(&mut cur, version).unwrap(),
                empty
            );
            assert!(
                !cur.has_remaining(),
                "AddPartitionsToTxn v{version} empty getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
            assert_eq!(
                decode_add_partitions_to_txn_response(&mut buf.as_ref(), version).unwrap(),
                0
            );
        }
    }

    #[test]
    fn add_offsets_to_txn_v3_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut req, 3, "tx", 9, 1, "g").unwrap();
        let mut cur = &req[..];
        let (tid, gid) = decode_add_offsets_to_txn_request(&mut cur, 3).unwrap();
        assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
        assert!(
            cur.is_empty(),
            "AddOffsetsToTxn v3 request must consume compact tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_add_offsets_to_txn_response(&mut resp, 3, 0).unwrap();
        let mut cur = &resp[..];
        assert_eq!(decode_add_offsets_to_txn_response(&mut cur, 3).unwrap(), 0);
        assert!(
            cur.is_empty(),
            "AddOffsetsToTxn v3 response must consume compact tagged fields"
        );

        req.clear();
        encode_add_offsets_to_txn_request(&mut req, 4, "tx", 9, 1, "g").unwrap();
        let mut cur = &req[..];
        let (tid, gid) = decode_add_offsets_to_txn_request(&mut cur, 4).unwrap();
        assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
        assert!(cur.is_empty(), "AddOffsetsToTxn v4 shares the v3 layout");
        req.clear();
        assert!(
            encode_add_offsets_to_txn_request(&mut req, 5, "tx", 9, 1, "g").is_err(),
            "AddOffsetsToTxn v5+ is not spoken"
        );
    }

    #[test]
    fn add_offsets_to_txn_v3_request_matches_compact_layout() {
        // Compact "tx", pid 9, epoch 1, compact "g", tagged.
        const REQ: &[u8] = &[
            0x03, 0x74, 0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x01, 0x02,
            0x67, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut buf, 3, "tx", 9, 1, "g").unwrap();
        assert_eq!(&buf[..], REQ);
    }

    #[test]
    fn add_offsets_to_txn_v3_response_matches_compact_layout() {
        // Throttle 0, error 0, tagged.
        const RESP: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut buf = BytesMut::new();
        encode_add_offsets_to_txn_response(&mut buf, 3, 0).unwrap();
        assert_eq!(&buf[..], RESP);
    }

    #[test]
    fn write_txn_markers_v0_roundtrip_is_leftover_empty() {
        let markers = vec![WritableTxnMarker {
            producer_id: 1000,
            producer_epoch: 0,
            transaction_result: TransactionResult::Abort.id(),
            topics: vec![WritableTxnMarkerTopic {
                name: "t".into(),
                partitions: vec![0],
            }],
            coordinator_epoch: 1,
        }];
        let mut buf = BytesMut::new();
        encode_write_txn_markers_request(&mut buf, 0, &markers).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_write_txn_markers_request(&mut cur, 0).unwrap(),
            markers
        );
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v0 request must be leftover-empty"
        );

        let resp = vec![markers[0].result(0)];
        buf.clear();
        encode_write_txn_markers_response(&mut buf, 0, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_write_txn_markers_response(&mut cur, 0).unwrap(),
            resp
        );
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v0 response must be leftover-empty"
        );
    }

    #[test]
    fn write_txn_markers_v1_roundtrip_is_leftover_empty() {
        let markers = vec![WritableTxnMarker {
            producer_id: 1000,
            producer_epoch: 0,
            transaction_result: TransactionResult::Abort.id(),
            topics: vec![WritableTxnMarkerTopic {
                name: "t".into(),
                partitions: vec![0],
            }],
            coordinator_epoch: 1,
        }];
        let mut buf = BytesMut::new();
        encode_write_txn_markers_request(&mut buf, 1, &markers).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_write_txn_markers_request(&mut cur, 1).unwrap(),
            markers
        );
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v1 request must consume compact tagged fields"
        );

        let resp = vec![markers[0].result(0)];
        buf.clear();
        encode_write_txn_markers_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_write_txn_markers_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v1 response must consume compact tagged fields"
        );
    }

    #[test]
    fn write_txn_markers_v0_abort_matches_classic_layout() {
        // Independent encode: markers INT32, {ProducerId INT64,
        // ProducerEpoch INT16, TransactionResult BOOLEAN, topics
        // {Name STRING, PartitionIndexes INT32 array}, CoordinatorEpoch
        // INT32}. Response has no throttle; first partition ErrorCode
        // for topic "t" / partition 0 is at bytes 27–28.
        const REQ: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        ];
        const RESP_6: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x06,
        ];
        let markers = vec![WritableTxnMarker {
            producer_id: 1000,
            producer_epoch: 0,
            transaction_result: TransactionResult::Abort.id(),
            topics: vec![WritableTxnMarkerTopic {
                name: "t".into(),
                partitions: vec![0],
            }],
            coordinator_epoch: 1,
        }];
        let mut buf = BytesMut::new();
        encode_write_txn_markers_request(&mut buf, 0, &markers).unwrap();
        assert_eq!(&buf[..], REQ);
        buf.clear();
        encode_write_txn_markers_response(
            &mut buf,
            0,
            &[markers[0].result(crate::error::NOT_LEADER_OR_FOLLOWER)],
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_6);
        assert_eq!(
            &RESP_6[27..29],
            &crate::error::NOT_LEADER_OR_FOLLOWER.to_be_bytes()
        );
    }

    #[test]
    fn write_txn_markers_v1_abort_matches_compact_layout() {
        // Compact: Markers uvarint n+1, {ProducerId, ProducerEpoch,
        // TransactionResult, Topics compact {Name COMPACT_STRING,
        // PartitionIndexes compact INT32 array, tagged}, CoordinatorEpoch,
        // tagged}, tagged. Response: same plus per-partition ErrorCode
        // and tagged fields on partition / topic / marker / top-level.
        const REQ: &[u8] = &[
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, 0x00, 0x00, 0x00, 0x02, 0x02,
            0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        ];
        const RESP_6: &[u8] = &[
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, 0x02, 0x02, 0x74, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00,
        ];
        let markers = vec![WritableTxnMarker {
            producer_id: 1000,
            producer_epoch: 0,
            transaction_result: TransactionResult::Abort.id(),
            topics: vec![WritableTxnMarkerTopic {
                name: "t".into(),
                partitions: vec![0],
            }],
            coordinator_epoch: 1,
        }];
        let mut buf = BytesMut::new();
        encode_write_txn_markers_request(&mut buf, 1, &markers).unwrap();
        assert_eq!(&buf[..], REQ);
        buf.clear();
        encode_write_txn_markers_response(
            &mut buf,
            1,
            &[markers[0].result(crate::error::NOT_LEADER_OR_FOLLOWER)],
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_6);
        assert_eq!(
            &RESP_6[17..19],
            &crate::error::NOT_LEADER_OR_FOLLOWER.to_be_bytes()
        );
        buf.clear();
        assert!(
            encode_write_txn_markers_request(&mut buf, 2, &markers).is_err(),
            "WriteTxnMarkers v2 TransactionVersion is not spoken"
        );
    }
}
