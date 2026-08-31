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

    /// Java `EndTxnRequest.getErrorResponse`.
    ///
    /// Producer id / epoch stay the JSON defaults
    /// ([`RecordBatch::NO_PRODUCER_ID`] / [`RecordBatch::NO_PRODUCER_EPOCH`])
    /// on v5+. ThrottleTimeMs is JSON `0+`; convenience encode still
    /// writes `0`. Official Java `getErrorResponse` sets
    /// `throttleTimeMs` from the argument.
    pub fn error_response(buf: &mut BytesMut, version: i16, error_code: i16) -> Result<()> {
        encode_end_txn_response(
            buf,
            version,
            error_code,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
    }
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

/// Java `AddPartitionsToTxnRequest` helpers.
pub struct AddPartitionsToTxnRequest;

impl AddPartitionsToTxnRequest {
    /// Java `AddPartitionsToTxnRequest.getPartitions`.
    ///
    /// Each `(topic, partition)` in request order. Duplicate pairs are
    /// kept (Java `ArrayList`).
    #[must_use]
    pub fn partitions(topics: &[TxnPartitionsTopic]) -> Vec<(String, i32)> {
        let mut partitions = Vec::new();
        for topic in topics {
            for &partition in &topic.partitions {
                partitions.push((topic.topic.clone(), partition));
            }
        }
        partitions
    }

    /// Java `AddPartitionsToTxnRequest.buildTxnTopicCollection`.
    ///
    /// Groups `(topic, partition)` by name. A later entry for the same
    /// topic appends (Java `HashMap.compute` then `List.add`). Topic
    /// order is first-seen (Java `HashMap.entrySet` order is
    /// unspecified). Duplicate partitions for the same pair are kept
    /// (`ArrayList` of `Partitions`).
    #[must_use]
    pub fn from_partitions<'a, I>(partitions: I) -> Vec<TxnPartitionsTopic>
    where
        I: IntoIterator<Item = (&'a str, i32)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partition) in partitions {
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
                    .map(|partitions| TxnPartitionsTopic { topic, partitions })
            })
            .collect()
    }
}

/// Java `AddPartitionsToTxnResponse` helpers.
pub struct AddPartitionsToTxnResponse;

impl AddPartitionsToTxnResponse {
    /// Java `AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID`.
    ///
    /// Key for [`Self::errors`] when the response is v0–v3
    /// (`resultsByTopicV3AndBelow`).
    pub const V3_AND_BELOW_TXN_ID: &'static str = "";

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

    /// Java `AddPartitionsToTxnResponse.errorsForTransaction`.
    ///
    /// Each `(topic, partition)` maps to that partition's error code
    /// (including `NONE`). A later partition overwrites an earlier one
    /// for the same pair (Java `HashMap.put`).
    #[must_use]
    pub fn errors_for_transaction(
        topics: &[AddPartitionsToTxnTopicResult],
    ) -> HashMap<(String, i32), i16> {
        let mut topic_results = HashMap::new();
        for topic in topics {
            for partition in &topic.partitions {
                let _prev = topic_results.insert(
                    (topic.topic.clone(), partition.partition),
                    partition.error_code,
                );
            }
        }
        topic_results
    }

    /// Java `AddPartitionsToTxnResponse.errors` for v0–v3
    /// (`resultsByTopicV3AndBelow`).
    ///
    /// Empty Topics is an empty map. Otherwise the only key is
    /// [`Self::V3_AND_BELOW_TXN_ID`]. v4+ `resultsByTransaction` is not
    /// spoken.
    #[must_use]
    pub fn errors(
        topics: &[AddPartitionsToTxnTopicResult],
    ) -> HashMap<String, HashMap<(String, i32), i16>> {
        let mut errors_map = HashMap::new();
        if !topics.is_empty() {
            let _prev = errors_map.insert(
                Self::V3_AND_BELOW_TXN_ID.to_string(),
                Self::errors_for_transaction(topics),
            );
        }
        errors_map
    }

    /// Java `AddPartitionsToTxnResponse.topicCollectionForErrors` /
    /// `resultForTransaction` topic results.
    ///
    /// Groups `(topic, partition, error)` by name. A later entry for the
    /// same topic appends (Java `HashMap.getOrDefault` then collection
    /// `add`). A later partition with the same index is ignored (Java
    /// `AddPartitionsToTxnPartitionResultCollection` mapKey
    /// `PartitionIndex`; `ImplicitLinkedHashCollection.add` keeps the
    /// first). Topic order is first-seen (Java `HashMap.entrySet` order
    /// is unspecified). This crate does not speak v4+
    /// `resultsByTransaction`, so the transactional id wrapper is not
    /// part of this helper.
    #[must_use]
    pub fn from_errors<'a, I>(errors: I) -> Vec<AddPartitionsToTxnTopicResult>
    where
        I: IntoIterator<Item = (&'a str, i32, i16)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<AddPartitionsToTxnPartitionResult>> = HashMap::new();
        for (topic, partition, error_code) in errors {
            let partitions = by_topic.entry(topic.to_string()).or_insert_with(|| {
                order.push(topic.to_string());
                Vec::new()
            });
            if partitions.iter().any(|p| p.partition == partition) {
                continue;
            }
            partitions.push(AddPartitionsToTxnPartitionResult {
                partition,
                error_code,
            });
        }
        order
            .into_iter()
            .filter_map(|topic| {
                by_topic
                    .remove(&topic)
                    .map(|partitions| AddPartitionsToTxnTopicResult { topic, partitions })
            })
            .collect()
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
/// ThrottleTimeMs is the JSON default (`0`) on every spoken version
/// (JSON `0+`). Nested body is PartitionIndex and PartitionErrorCode
/// (`ResultsByTopicV3AndBelow`).
pub fn encode_add_partitions_to_txn_topics_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[AddPartitionsToTxnTopicResult],
) -> Result<()> {
    encode_add_partitions_to_txn_topics_response_with_throttle(buf, version, topics, 0)
}

/// Encode AddPartitionsToTxn v0–v3 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+`: written on every spoken version.
/// v0–v2 are classic. v3 is flexible. Kafka 4.0 `validVersions` is
/// `0-5`. This crate speaks 0–3. v4+ (batched transactions) is not
/// spoken. There is no top-level ErrorCode on spoken versions.
pub fn encode_add_partitions_to_txn_topics_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    topics: &[AddPartitionsToTxnTopicResult],
    throttle_time_ms: i32,
) -> Result<()> {
    let flexible = add_partitions_to_txn_flexible(version)?;
    buf.put_i32(throttle_time_ms);
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
    let (topics, ..) = decode_add_partitions_to_txn_topics_response(buf, version)?;
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
///
/// Returns `(topics, throttle_time_ms)`. ThrottleTimeMs is JSON `0+`
/// (always on the wire). There is no top-level ErrorCode on spoken
/// versions.
pub fn decode_add_partitions_to_txn_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<AddPartitionsToTxnTopicResult>, i32)> {
    let flexible = add_partitions_to_txn_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
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
    Ok((topics, throttle_time_ms))
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

/// Decode AddOffsetsToTxn: `(transactional_id, group_id, producer_id, producer_epoch)`.
pub fn decode_add_offsets_to_txn_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, i64, i16)> {
    let flexible = add_offsets_to_txn_flexible(version)?;
    let tid = buf::get_string(buf, flexible)?.unwrap_or_default();
    let producer_id = buf::get_i64(buf)?;
    let producer_epoch = buf::get_i16(buf)?;
    let gid = buf::get_string(buf, flexible)?.unwrap_or_default();
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((tid, gid, producer_id, producer_epoch))
}

/// Encode AddOffsetsToTxn: throttle `0` plus error code.
///
/// ThrottleTimeMs is the JSON default (`0`) on every spoken version
/// (JSON `0+`).
pub fn encode_add_offsets_to_txn_response(
    buf: &mut BytesMut,
    version: i16,
    error: i16,
) -> Result<()> {
    encode_add_offsets_to_txn_response_with_throttle(buf, version, error, 0)
}

/// Encode AddOffsetsToTxn v0–v4 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+`: written on every spoken version.
/// v0–v2 are classic. v3–v4 are flexible. v4 is the same layout
/// (KIP-890 TRANSACTION_ABORTABLE). Kafka 4.0 `validVersions` is `0-4`.
/// This crate speaks 0–4. v5+ is not spoken. Top-level ErrorCode is at
/// bytes 4–5.
pub fn encode_add_offsets_to_txn_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error: i16,
    throttle_time_ms: i32,
) -> Result<()> {
    let flexible = add_offsets_to_txn_flexible(version)?;
    buf.put_i32(throttle_time_ms);
    buf.put_i16(error);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode AddOffsetsToTxn: `(error_code, throttle_time_ms)`.
///
/// ThrottleTimeMs is JSON `0+` (always on the wire). Top-level ErrorCode
/// is at bytes 4–5.
pub fn decode_add_offsets_to_txn_response<B: Buf>(buf: &mut B, version: i16) -> Result<(i16, i32)> {
    let flexible = add_offsets_to_txn_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let err = buf::get_i16(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((err, throttle_time_ms))
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
/// ThrottleTimeMs is the JSON default (`0`) on every spoken version
/// (JSON `0+`). Java `EndTxnResponseData` defaults ProducerId /
/// ProducerEpoch to [`RecordBatch::NO_PRODUCER_ID`] /
/// [`RecordBatch::NO_PRODUCER_EPOCH`].
pub fn encode_end_txn_response(
    buf: &mut BytesMut,
    version: i16,
    error: i16,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<()> {
    encode_end_txn_response_with_throttle(buf, version, error, producer_id, producer_epoch, 0)
}

/// Encode EndTxn v0–v5 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+`: written on every spoken version.
/// v0–v2 are classic. v3–v5 are flexible. v4 is the same layout
/// (KIP-890 TRANSACTION_ABORTABLE). v5 adds ProducerId / ProducerEpoch
/// (KIP-890 Part 2). Kafka 4.0 `validVersions` is `0-5`. This crate
/// speaks 0–5. v6+ is not spoken. Top-level ErrorCode is at bytes 4–5.
pub fn encode_end_txn_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error: i16,
    producer_id: i64,
    producer_epoch: i16,
    throttle_time_ms: i32,
) -> Result<()> {
    let flexible = end_txn_flexible(version)?;
    buf.put_i32(throttle_time_ms);
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

/// Decode EndTxn: `(error, producer_id, producer_epoch, throttle_time_ms)`.
///
/// Below v5, producer id and epoch are [`RecordBatch::NO_PRODUCER_ID`] /
/// [`RecordBatch::NO_PRODUCER_EPOCH`] (JSON default `-1`). ThrottleTimeMs
/// is JSON `0+` (always on the wire). Top-level ErrorCode is at bytes 4–5.
pub fn decode_end_txn_response<B: Buf>(buf: &mut B, version: i16) -> Result<(i16, i64, i16, i32)> {
    let flexible = end_txn_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
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
    Ok((err, producer_id, producer_epoch, throttle_time_ms))
}

/// Java `TxnOffsetCommitRequest` version helpers (KIP-890 transaction V2).
pub struct TxnOffsetCommitRequest;

impl TxnOffsetCommitRequest {
    /// Java `TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`.
    pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 4;

    /// Java `TxnOffsetCommitRequest.offsets`.
    ///
    /// Each `(topic, partition)` maps to that [`TxnOffsetPartition`]
    /// (Java `CommittedOffset`). A later partition overwrites an earlier
    /// one for the same pair (Java `HashMap.put`).
    #[must_use]
    pub fn offsets(topics: &[TxnOffsetTopic]) -> HashMap<(String, i32), TxnOffsetPartition> {
        let mut offset_map = HashMap::new();
        for topic in topics {
            for partition in &topic.partitions {
                let _prev = offset_map.insert(
                    (topic.topic.clone(), partition.partition),
                    partition.clone(),
                );
            }
        }
        offset_map
    }

    /// Java `TxnOffsetCommitRequest.getTopics`.
    ///
    /// Groups `(topic, CommittedOffset body)` by name. A later entry
    /// for the same topic appends (Java `HashMap.getOrDefault` then
    /// `partitions.add`). Topic order is first-seen (Java
    /// `HashMap.entrySet` order is unspecified). The Java map key is
    /// `TopicPartition`; grouping uses only the name. The partition
    /// index on the body is kept as-is. Duplicate partitions for the
    /// same pair are kept (`ArrayList`).
    #[must_use]
    pub fn from_offsets<'a, I>(pending_txn_offset_commits: I) -> Vec<TxnOffsetTopic>
    where
        I: IntoIterator<Item = (&'a str, TxnOffsetPartition)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<TxnOffsetPartition>> = HashMap::new();
        for (topic, partition) in pending_txn_offset_commits {
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
                    .map(|partitions| TxnOffsetTopic { topic, partitions })
            })
            .collect()
    }
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

    /// Java `TxnOffsetCommitResponse.errors`.
    ///
    /// Each `(topic, partition)` maps to that partition's error code
    /// (including `NONE`). A later partition overwrites an earlier one
    /// for the same pair (Java `HashMap.put`).
    #[must_use]
    pub fn errors(topics: &[TxnOffsetCommitResponseTopic]) -> HashMap<(String, i32), i16> {
        let mut error_map = HashMap::new();
        for topic in topics {
            for partition in &topic.partitions {
                let _prev = error_map.insert(
                    (topic.topic.clone(), partition.partition),
                    partition.error_code,
                );
            }
        }
        error_map
    }

    /// Java `TxnOffsetCommitResponse(int, Map)`.
    ///
    /// Groups `(topic, partition, error)` by name. A later entry for the
    /// same topic appends (Java `HashMap.getOrDefault` then
    /// `partitions().add`). Topic order is first-seen (Java
    /// `HashMap.values` order is unspecified). The Java map key is
    /// `TopicPartition`; grouping uses only the name. Duplicate
    /// partitions for the same pair are kept (`ArrayList`). Throttle is
    /// not part of this helper (crate encode writes the JSON default
    /// `0`).
    #[must_use]
    pub fn from_errors<'a, I>(response_data: I) -> Vec<TxnOffsetCommitResponseTopic>
    where
        I: IntoIterator<Item = (&'a str, i32, i16)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<TxnOffsetCommitResponsePartition>> = HashMap::new();
        for (topic, partition, error_code) in response_data {
            by_topic
                .entry(topic.to_string())
                .or_insert_with(|| {
                    order.push(topic.to_string());
                    Vec::new()
                })
                .push(TxnOffsetCommitResponsePartition {
                    partition,
                    error_code,
                });
        }
        order
            .into_iter()
            .filter_map(|topic| {
                by_topic
                    .remove(&topic)
                    .map(|partitions| TxnOffsetCommitResponseTopic { topic, partitions })
            })
            .collect()
    }

    /// Java `TxnOffsetCommitResponse.Builder.merge`.
    ///
    /// If `current` has no topics, the result is `new_topics`. Otherwise
    /// new topics are appended and partitions of an existing topic are
    /// appended to that topic. Java does not check for overlapping
    /// partitions. Topic order is first-seen.
    #[must_use]
    pub fn merge(
        current: &[TxnOffsetCommitResponseTopic],
        new_topics: &[TxnOffsetCommitResponseTopic],
    ) -> Vec<TxnOffsetCommitResponseTopic> {
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

/// Decode TxnOffsetCommit: `(transactional_id, group_id, member, topics, producer_id)`.
///
/// Decode below v2 fills [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] for
/// omitted `CommittedLeaderEpoch`.
pub fn decode_txn_offset_commit_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(
    String,
    String,
    TxnOffsetCommitMember,
    Vec<TxnOffsetTopic>,
    i64,
)> {
    let flexible = txn_offset_commit_flexible(version)?;
    let tid = buf::get_string(buf, flexible)?.unwrap_or_default();
    let gid = buf::get_string(buf, flexible)?.unwrap_or_default();
    let producer_id = buf::get_i64(buf)?;
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
    Ok((tid, gid, member, topics, producer_id))
}

/// Encode TxnOffsetCommit: one error code applied to every partition.
///
/// Applies `error` on every request partition via
/// [`TxnOffsetTopic::error_results`]. ThrottleTimeMs is the JSON
/// default (`0`) on every spoken version (JSON `0+`).
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
/// ThrottleTimeMs is the JSON default (`0`) on every spoken version
/// (JSON `0+`). Nested body is PartitionIndex + ErrorCode.
pub fn encode_txn_offset_commit_topics_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[TxnOffsetCommitResponseTopic],
) -> Result<()> {
    encode_txn_offset_commit_topics_response_with_throttle(buf, version, topics, 0)
}

/// Encode TxnOffsetCommit v0–v5 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+`: written on every spoken version.
/// v0–v2 are classic. v3–v5 are flexible. v4 is the same layout
/// (KIP-890 TRANSACTION_ABORTABLE). v5 is the same layout (KIP-890
/// Part 2). Kafka 4.0 `validVersions` is `0-5`. This crate speaks 0–5.
/// v6+ is not spoken. There is no top-level ErrorCode.
pub fn encode_txn_offset_commit_topics_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    topics: &[TxnOffsetCommitResponseTopic],
    throttle_time_ms: i32,
) -> Result<()> {
    let flexible = txn_offset_commit_flexible(version)?;
    buf.put_i32(throttle_time_ms);
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
    let (topics, ..) = decode_txn_offset_commit_topics_response(buf, version)?;
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
///
/// Returns `(topics, throttle_time_ms)`. ThrottleTimeMs is JSON `0+`
/// (always on the wire). There is no top-level ErrorCode.
pub fn decode_txn_offset_commit_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<TxnOffsetCommitResponseTopic>, i32)> {
    let flexible = txn_offset_commit_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
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
    Ok((topics, throttle_time_ms))
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
    /// Java `WriteTxnMarkersRequest.TxnMarkerEntry.partitions`.
    ///
    /// Each `(topic, partition)` in request order. Duplicate pairs are
    /// kept (Java `ArrayList`).
    #[must_use]
    pub fn partitions(&self) -> Vec<(String, i32)> {
        let mut partitions = Vec::new();
        for topic in &self.topics {
            for &partition in &topic.partitions {
                partitions.push((topic.name.clone(), partition));
            }
        }
        partitions
    }

    /// Java `WriteTxnMarkersRequest.Builder(List)` one marker.
    ///
    /// Groups `(topic, partition)` by name. A later entry for the same
    /// topic appends (Java `HashMap.getOrDefault` then
    /// `partitionIndexes().add`). Topic order is first-seen (Java
    /// `HashMap.values` order is unspecified). Duplicate partitions for
    /// the same pair are kept (`ArrayList` of `PartitionIndexes`).
    #[must_use]
    pub fn from_partitions<'a, I>(
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i32,
        transaction_result: bool,
        partitions: I,
    ) -> Self
    where
        I: IntoIterator<Item = (&'a str, i32)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partition) in partitions {
            by_topic
                .entry(topic.to_string())
                .or_insert_with(|| {
                    order.push(topic.to_string());
                    Vec::new()
                })
                .push(partition);
        }
        Self {
            producer_id,
            producer_epoch,
            transaction_result,
            topics: order
                .into_iter()
                .filter_map(|name| {
                    by_topic
                        .remove(&name)
                        .map(|partitions| WritableTxnMarkerTopic { name, partitions })
                })
                .collect(),
            coordinator_epoch,
        }
    }

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

/// Java `WriteTxnMarkersRequest` helpers.
pub struct WriteTxnMarkersRequest;

impl WriteTxnMarkersRequest {
    /// Java `WriteTxnMarkersRequest.getErrorResponse`.
    ///
    /// One error on every request partition. Inner `HashMap.put` keeps
    /// the last `(topic, partition)` per marker (duplicate pairs are not
    /// kept). A later marker overwrites an earlier one for the same
    /// producer id (Java outer `HashMap.put`). Producer order is
    /// first-seen (Java outer `HashMap.entrySet` order is unspecified).
    /// Topic grouping is [`WriteTxnMarkersResponse::from_errors`]
    /// (append; first-seen topic order). Empty topics are dropped.
    /// Distinct from [`WritableTxnMarker::result`], which copies the
    /// request layout (`ArrayList` duplicates and empty topics). The
    /// Java `throttleTimeMs` argument is unused (no throttle field).
    #[must_use]
    pub fn error_response(
        markers: &[WritableTxnMarker],
        error_code: i16,
    ) -> Vec<WritableTxnMarkerResult> {
        let mut producer_order: Vec<i64> = Vec::new();
        let mut by_producer: HashMap<i64, Vec<(String, i32)>> = HashMap::new();
        for marker in markers {
            let mut partition_order: Vec<(String, i32)> = Vec::new();
            let mut errors_per_partition: HashMap<(String, i32), i16> = HashMap::new();
            for topic in &marker.topics {
                for &partition in &topic.partitions {
                    let key = (topic.name.clone(), partition);
                    if errors_per_partition
                        .insert(key.clone(), error_code)
                        .is_none()
                    {
                        partition_order.push(key);
                    }
                }
            }
            if by_producer
                .insert(marker.producer_id, partition_order)
                .is_none()
            {
                producer_order.push(marker.producer_id);
            }
        }
        let owned = producer_order
            .into_iter()
            .filter_map(|producer_id| {
                by_producer.remove(&producer_id).map(|partitions| {
                    (
                        producer_id,
                        partitions
                            .into_iter()
                            .map(|(topic, partition)| (topic, partition, error_code))
                            .collect::<Vec<_>>(),
                    )
                })
            })
            .collect::<Vec<_>>();
        WriteTxnMarkersResponse::from_errors(owned.iter().map(|(producer_id, partitions)| {
            (
                *producer_id,
                partitions
                    .iter()
                    .map(|(topic, partition, error)| (topic.as_str(), *partition, *error)),
            )
        }))
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

/// Java `WriteTxnMarkersResponse` helpers.
pub struct WriteTxnMarkersResponse;

impl WriteTxnMarkersResponse {
    /// Java `WriteTxnMarkersResponse.errorCounts`.
    ///
    /// Counts partition-level error codes (including `NONE`).
    #[must_use]
    pub fn error_counts(markers: &[WritableTxnMarkerResult]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        for marker in markers {
            for topic in &marker.topics {
                for partition in &topic.partitions {
                    let count = counts.entry(partition.error_code).or_insert(0);
                    *count += 1;
                }
            }
        }
        counts
    }

    /// Java `WriteTxnMarkersResponse.errorsByProducerId`.
    ///
    /// Each producer id maps to `(topic, partition)` error codes. A later
    /// marker overwrites an earlier one for the same producer id (Java
    /// `HashMap.put`). A later partition overwrites an earlier one for
    /// the same pair.
    #[must_use]
    pub fn errors_by_producer_id(
        markers: &[WritableTxnMarkerResult],
    ) -> HashMap<i64, HashMap<(String, i32), i16>> {
        let mut errors = HashMap::new();
        for marker in markers {
            let mut topic_partition_errors = HashMap::new();
            for topic in &marker.topics {
                for partition in &topic.partitions {
                    let _prev = topic_partition_errors.insert(
                        (topic.name.clone(), partition.partition_index),
                        partition.error_code,
                    );
                }
            }
            let _prev = errors.insert(marker.producer_id, topic_partition_errors);
        }
        errors
    }

    /// Java `WriteTxnMarkersResponse(Map)`.
    ///
    /// Groups each producer's `(topic, partition, error)` triples by
    /// topic name. A later entry for the same topic appends (Java
    /// `HashMap.getOrDefault` then `partitions().add`). Topic order
    /// within a producer is first-seen (Java `HashMap.values` order is
    /// unspecified). Producer order is iterator order (Java outer
    /// `HashMap.entrySet` order is unspecified). Java outer map keys
    /// are unique; this helper emits one marker per outer entry.
    /// Duplicate partitions for the same pair are kept (`ArrayList`).
    #[must_use]
    pub fn from_errors<'a, I, J>(errors: I) -> Vec<WritableTxnMarkerResult>
    where
        I: IntoIterator<Item = (i64, J)>,
        J: IntoIterator<Item = (&'a str, i32, i16)>,
    {
        let mut markers = Vec::new();
        for (producer_id, partitions) in errors {
            let mut topic_order: Vec<String> = Vec::new();
            let mut by_topic: HashMap<String, Vec<WritableTxnMarkerPartitionResult>> =
                HashMap::new();
            for (topic, partition_index, error_code) in partitions {
                by_topic
                    .entry(topic.to_string())
                    .or_insert_with(|| {
                        topic_order.push(topic.to_string());
                        Vec::new()
                    })
                    .push(WritableTxnMarkerPartitionResult {
                        partition_index,
                        error_code,
                    });
            }
            markers.push(WritableTxnMarkerResult {
                producer_id,
                topics: topic_order
                    .into_iter()
                    .filter_map(|name| {
                        by_topic
                            .remove(&name)
                            .map(|partitions| WritableTxnMarkerTopicResult { name, partitions })
                    })
                    .collect(),
            });
        }
        markers
    }
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
    fn txn_offset_commit_response_errors_matches_java() {
        // Java TxnOffsetCommitResponse.errors: each (topic, partition)
        // maps to that partition's error code. A later partition
        // overwrites the same pair (HashMap.put).
        assert!(TxnOffsetCommitResponse::errors(&[]).is_empty());
        let one = vec![TxnOffsetCommitResponseTopic {
            topic: "t".into(),
            partitions: vec![
                TxnOffsetCommitResponsePartition::error(0, 0),
                TxnOffsetCommitResponsePartition::error(1, crate::error::NOT_LEADER_OR_FOLLOWER),
            ],
        }];
        assert_eq!(
            TxnOffsetCommitResponse::errors(&one),
            HashMap::from([
                (("t".into(), 0), 0),
                (("t".into(), 1), crate::error::NOT_LEADER_OR_FOLLOWER),
            ])
        );
        let two = vec![
            TxnOffsetCommitResponseTopic {
                topic: "a".into(),
                partitions: vec![TxnOffsetCommitResponsePartition::error(
                    0,
                    crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                )],
            },
            TxnOffsetCommitResponseTopic {
                topic: "a".into(),
                partitions: vec![TxnOffsetCommitResponsePartition::error(
                    0,
                    crate::error::NOT_LEADER_OR_FOLLOWER,
                )],
            },
            TxnOffsetCommitResponseTopic {
                topic: "b".into(),
                partitions: vec![TxnOffsetCommitResponsePartition::error(3, 0)],
            },
        ];
        assert_eq!(
            TxnOffsetCommitResponse::errors(&two),
            HashMap::from([
                (("a".into(), 0), crate::error::NOT_LEADER_OR_FOLLOWER),
                (("b".into(), 3), 0),
            ])
        );
        let mut buf = BytesMut::new();
        encode_txn_offset_commit_topics_response(&mut buf, 0, &two).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_txn_offset_commit_topics_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(
            TxnOffsetCommitResponse::errors(&decoded),
            TxnOffsetCommitResponse::errors(&two)
        );
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v0 errors leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_txn_offset_commit_topics_response(&mut buf, 3, &two).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_txn_offset_commit_topics_response(&mut cur, 3).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(
            TxnOffsetCommitResponse::errors(&decoded),
            TxnOffsetCommitResponse::errors(&two)
        );
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v3 errors leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn txn_offset_commit_response_from_errors_matches_java() {
        // Java TxnOffsetCommitResponse(int, Map): HashMap.getOrDefault by
        // topic name, then partitions().add. Empty map is empty. A later
        // entry for the same name appends even when another topic sits
        // between. Duplicate partitions for the same pair are kept
        // (ArrayList).
        assert!(
            TxnOffsetCommitResponse::from_errors(std::iter::empty::<(&str, i32, i16)>()).is_empty()
        );
        let grouped = TxnOffsetCommitResponse::from_errors([
            ("a", 0, crate::error::UNKNOWN_TOPIC_OR_PARTITION),
            ("b", 0, crate::error::NOT_LEADER_OR_FOLLOWER),
            ("a", 1, 0i16),
        ]);
        assert_eq!(
            grouped,
            vec![
                TxnOffsetCommitResponseTopic {
                    topic: "a".into(),
                    partitions: vec![
                        TxnOffsetCommitResponsePartition::error(
                            0,
                            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                        ),
                        TxnOffsetCommitResponsePartition::error(1, 0),
                    ],
                },
                TxnOffsetCommitResponseTopic {
                    topic: "b".into(),
                    partitions: vec![TxnOffsetCommitResponsePartition::error(
                        0,
                        crate::error::NOT_LEADER_OR_FOLLOWER,
                    )],
                },
            ]
        );
        let dup = TxnOffsetCommitResponse::from_errors([
            ("t", 0, 0i16),
            ("t", 0, crate::error::NOT_LEADER_OR_FOLLOWER),
        ]);
        assert_eq!(
            dup,
            vec![TxnOffsetCommitResponseTopic {
                topic: "t".into(),
                partitions: vec![
                    TxnOffsetCommitResponsePartition::error(0, 0),
                    TxnOffsetCommitResponsePartition::error(
                        0,
                        crate::error::NOT_LEADER_OR_FOLLOWER,
                    ),
                ],
            }]
        );
        let mut buf = BytesMut::new();
        encode_txn_offset_commit_topics_response(&mut buf, 0, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_txn_offset_commit_topics_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v0 from_errors leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_txn_offset_commit_topics_response(&mut buf, 3, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_txn_offset_commit_topics_response(&mut cur, 3).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v3 from_errors leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn txn_offset_commit_response_merge_matches_java() {
        // Java TxnOffsetCommitResponse.Builder.merge: replace when current
        // Topics are empty. Otherwise append topics / partitions (no
        // overlap check). There is no top-level ErrorCode replacement.
        let t1 = TxnOffsetCommitResponseTopic {
            topic: "t1".into(),
            partitions: vec![TxnOffsetCommitResponsePartition::error(0, 0)],
        };
        let t1_extra = TxnOffsetCommitResponseTopic {
            topic: "t1".into(),
            partitions: vec![TxnOffsetCommitResponsePartition::error(
                1,
                crate::error::NOT_LEADER_OR_FOLLOWER,
            )],
        };
        let t2 = TxnOffsetCommitResponseTopic {
            topic: "t2".into(),
            partitions: vec![TxnOffsetCommitResponsePartition::error(
                0,
                crate::error::UNKNOWN_TOPIC_OR_PARTITION,
            )],
        };
        let current = vec![t1.clone()];
        let merged_same = TxnOffsetCommitResponse::merge(&current, std::slice::from_ref(&t1_extra));
        assert_eq!(
            merged_same,
            vec![TxnOffsetCommitResponseTopic {
                topic: "t1".into(),
                partitions: vec![
                    TxnOffsetCommitResponsePartition::error(0, 0),
                    TxnOffsetCommitResponsePartition::error(
                        1,
                        crate::error::NOT_LEADER_OR_FOLLOWER
                    ),
                ],
            }]
        );
        for version in [0_i16, 1, 3] {
            let mut got = BytesMut::new();
            encode_txn_offset_commit_topics_response(&mut got, version, &merged_same).unwrap();
            let mut cur = &got[..];
            let (decoded, ..) =
                decode_txn_offset_commit_topics_response(&mut cur, version).unwrap();
            assert_eq!(decoded, merged_same, "v{version} same-topic merge decode");
            assert!(
                cur.is_empty(),
                "TxnOffsetCommit v{version} merge same-topic leftover-empty; leftover {} bytes",
                cur.len()
            );
        }

        let merged_new = TxnOffsetCommitResponse::merge(&current, std::slice::from_ref(&t2));
        assert_eq!(merged_new, vec![t1.clone(), t2.clone()]);
        let mut got = BytesMut::new();
        encode_txn_offset_commit_topics_response(&mut got, 3, &merged_new).unwrap();
        let mut cur = &got[..];
        let (decoded, ..) = decode_txn_offset_commit_topics_response(&mut cur, 3).unwrap();
        assert_eq!(decoded, merged_new);
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v3 merge new-topic leftover-empty; leftover {} bytes",
            cur.len()
        );

        let from_empty = TxnOffsetCommitResponse::merge(&[], &current);
        assert_eq!(from_empty, current, "empty current Topics takes new Topics");
        got.clear();
        encode_txn_offset_commit_topics_response(&mut got, 0, &from_empty).unwrap();
        let mut cur = &got[..];
        let (decoded, ..) = decode_txn_offset_commit_topics_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, current);
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v0 merge empty-current leftover-empty; leftover {} bytes",
            cur.len()
        );

        let empty_both = TxnOffsetCommitResponse::merge(&[], &[]);
        assert!(empty_both.is_empty());
        got.clear();
        encode_txn_offset_commit_topics_response(&mut got, 1, &empty_both).unwrap();
        let mut cur = &got[..];
        let (decoded, ..) = decode_txn_offset_commit_topics_response(&mut cur, 1).unwrap();
        assert!(decoded.is_empty());
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v1 merge empty leftover-empty; leftover {} bytes",
            cur.len()
        );

        let grouped = TxnOffsetCommitResponse::merge(&current, &[t2, t1_extra]);
        assert_eq!(
            grouped,
            vec![
                TxnOffsetCommitResponseTopic {
                    topic: "t1".into(),
                    partitions: vec![
                        TxnOffsetCommitResponsePartition::error(0, 0),
                        TxnOffsetCommitResponsePartition::error(
                            1,
                            crate::error::NOT_LEADER_OR_FOLLOWER
                        ),
                    ],
                },
                TxnOffsetCommitResponseTopic {
                    topic: "t2".into(),
                    partitions: vec![TxnOffsetCommitResponsePartition::error(
                        0,
                        crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                    )],
                },
            ]
        );
        got.clear();
        encode_txn_offset_commit_topics_response(&mut got, 3, &grouped).unwrap();
        let mut cur = &got[..];
        let (decoded, ..) = decode_txn_offset_commit_topics_response(&mut cur, 3).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v3 merge grouped leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn write_txn_markers_response_error_counts_matches_java() {
        assert!(WriteTxnMarkersResponse::error_counts(&[]).is_empty());
        let counts = WriteTxnMarkersResponse::error_counts(&[
            WritableTxnMarkerResult {
                producer_id: 1,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "ok".into(),
                    partitions: vec![
                        WritableTxnMarkerPartitionResult {
                            partition_index: 0,
                            error_code: 0,
                        },
                        WritableTxnMarkerPartitionResult {
                            partition_index: 1,
                            error_code: crate::error::NOT_LEADER_OR_FOLLOWER,
                        },
                    ],
                }],
            },
            WritableTxnMarkerResult {
                producer_id: 2,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "missing".into(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code: crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                    }],
                }],
            },
            WritableTxnMarkerResult {
                producer_id: 3,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "ok2".into(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code: 0,
                    }],
                }],
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
    fn write_txn_markers_errors_by_producer_id_matches_java() {
        // Java WriteTxnMarkersResponse.errorsByProducerId: each producer
        // id maps to (topic, partition) error codes. Later markers
        // overwrite the same producer id (HashMap.put). Later partitions
        // overwrite the same pair.
        assert!(WriteTxnMarkersResponse::errors_by_producer_id(&[]).is_empty());
        let one = vec![WritableTxnMarkerResult {
            producer_id: 1000,
            topics: vec![WritableTxnMarkerTopicResult {
                name: "t".into(),
                partitions: vec![
                    WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code: 0,
                    },
                    WritableTxnMarkerPartitionResult {
                        partition_index: 1,
                        error_code: crate::error::NOT_LEADER_OR_FOLLOWER,
                    },
                ],
            }],
        }];
        assert_eq!(
            WriteTxnMarkersResponse::errors_by_producer_id(&one),
            HashMap::from([(
                1000,
                HashMap::from([
                    (("t".into(), 0), 0),
                    (("t".into(), 1), crate::error::NOT_LEADER_OR_FOLLOWER),
                ])
            )])
        );
        let two = vec![
            WritableTxnMarkerResult {
                producer_id: 1000,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "a".into(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code: crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                    }],
                }],
            },
            WritableTxnMarkerResult {
                producer_id: 1000,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "b".into(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code: crate::error::NOT_LEADER_OR_FOLLOWER,
                    }],
                }],
            },
            WritableTxnMarkerResult {
                producer_id: 2000,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "c".into(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 3,
                        error_code: 0,
                    }],
                }],
            },
        ];
        assert_eq!(
            WriteTxnMarkersResponse::errors_by_producer_id(&two),
            HashMap::from([
                (
                    1000,
                    HashMap::from([(("b".into(), 0), crate::error::NOT_LEADER_OR_FOLLOWER)])
                ),
                (2000, HashMap::from([(("c".into(), 3), 0)])),
            ])
        );
        let mut buf = BytesMut::new();
        encode_write_txn_markers_response(&mut buf, 0, &two).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(
            WriteTxnMarkersResponse::errors_by_producer_id(&decoded),
            WriteTxnMarkersResponse::errors_by_producer_id(&two)
        );
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v0 errorsByProducerId leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_write_txn_markers_response(&mut buf, 1, &two).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_response(&mut cur, 1).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(
            WriteTxnMarkersResponse::errors_by_producer_id(&decoded),
            WriteTxnMarkersResponse::errors_by_producer_id(&two)
        );
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v1 errorsByProducerId leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn write_txn_markers_from_errors_matches_java() {
        // Java WriteTxnMarkersResponse(Map): HashMap.getOrDefault by
        // topic name, then partitions().add. Empty map is empty. A later
        // entry for the same name appends even when another topic sits
        // between. One marker per outer entry. Duplicate partitions
        // for the same pair are kept (ArrayList).
        assert!(WriteTxnMarkersResponse::from_errors(std::iter::empty::<(
            i64,
            std::iter::Empty<(&str, i32, i16)>
        )>())
        .is_empty());
        let grouped = WriteTxnMarkersResponse::from_errors([
            (
                1000i64,
                vec![
                    ("a", 0, crate::error::UNKNOWN_TOPIC_OR_PARTITION),
                    ("b", 0, crate::error::NOT_LEADER_OR_FOLLOWER),
                    ("a", 1, 0i16),
                ],
            ),
            (2000, vec![("c", 3, 0)]),
        ]);
        assert_eq!(
            grouped,
            vec![
                WritableTxnMarkerResult {
                    producer_id: 1000,
                    topics: vec![
                        WritableTxnMarkerTopicResult {
                            name: "a".into(),
                            partitions: vec![
                                WritableTxnMarkerPartitionResult {
                                    partition_index: 0,
                                    error_code: crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                                },
                                WritableTxnMarkerPartitionResult {
                                    partition_index: 1,
                                    error_code: 0,
                                },
                            ],
                        },
                        WritableTxnMarkerTopicResult {
                            name: "b".into(),
                            partitions: vec![WritableTxnMarkerPartitionResult {
                                partition_index: 0,
                                error_code: crate::error::NOT_LEADER_OR_FOLLOWER,
                            }],
                        },
                    ],
                },
                WritableTxnMarkerResult {
                    producer_id: 2000,
                    topics: vec![WritableTxnMarkerTopicResult {
                        name: "c".into(),
                        partitions: vec![WritableTxnMarkerPartitionResult {
                            partition_index: 3,
                            error_code: 0,
                        }],
                    }],
                },
            ]
        );
        let dup = WriteTxnMarkersResponse::from_errors([(
            1000i64,
            [
                ("t", 0, 0i16),
                ("t", 0, crate::error::NOT_LEADER_OR_FOLLOWER),
            ],
        )]);
        assert_eq!(
            dup,
            vec![WritableTxnMarkerResult {
                producer_id: 1000,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "t".into(),
                    partitions: vec![
                        WritableTxnMarkerPartitionResult {
                            partition_index: 0,
                            error_code: 0,
                        },
                        WritableTxnMarkerPartitionResult {
                            partition_index: 0,
                            error_code: crate::error::NOT_LEADER_OR_FOLLOWER,
                        },
                    ],
                }],
            }]
        );
        let mut buf = BytesMut::new();
        encode_write_txn_markers_response(&mut buf, 0, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v0 from_errors leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_write_txn_markers_response(&mut buf, 1, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_response(&mut cur, 1).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v1 from_errors leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn writable_txn_marker_partitions_matches_java() {
        // Java WriteTxnMarkersRequest.TxnMarkerEntry.partitions /
        // WriteTxnMarkersRequest.markers inner flatten: each
        // (topic, partition) in request order. Duplicates are kept
        // (ArrayList).
        let empty = WritableTxnMarker {
            producer_id: 1,
            producer_epoch: 0,
            transaction_result: TransactionResult::Abort.id(),
            topics: vec![],
            coordinator_epoch: 0,
        };
        assert!(empty.partitions().is_empty());
        let marker = WritableTxnMarker {
            producer_id: 1000,
            producer_epoch: 1,
            transaction_result: TransactionResult::Commit.id(),
            topics: vec![
                WritableTxnMarkerTopic {
                    name: "a".into(),
                    partitions: vec![0, 1],
                },
                WritableTxnMarkerTopic {
                    name: "b".into(),
                    partitions: vec![2],
                },
                WritableTxnMarkerTopic {
                    name: "a".into(),
                    partitions: vec![0],
                },
            ],
            coordinator_epoch: 3,
        };
        assert_eq!(
            marker.partitions(),
            vec![
                ("a".into(), 0),
                ("a".into(), 1),
                ("b".into(), 2),
                ("a".into(), 0),
            ]
        );
        let markers = std::slice::from_ref(&marker);
        let mut buf = BytesMut::new();
        encode_write_txn_markers_request(&mut buf, 0, markers).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_request(&mut cur, 0).unwrap();
        assert_eq!(decoded, vec![marker.clone()]);
        assert_eq!(decoded[0].partitions(), marker.partitions());
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v0 partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_write_txn_markers_request(&mut buf, 1, markers).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_request(&mut cur, 1).unwrap();
        assert_eq!(decoded, vec![marker.clone()]);
        assert_eq!(decoded[0].partitions(), marker.partitions());
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v1 partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn writable_txn_marker_from_partitions_matches_java() {
        // Java WriteTxnMarkersRequest.Builder(List) one marker:
        // HashMap.getOrDefault by topic name, then
        // partitionIndexes().add. Empty list is empty Topics. A later
        // entry for the same name appends even when another topic sits
        // between. Duplicate partitions for the same pair are kept
        // (ArrayList of PartitionIndexes).
        let empty = WritableTxnMarker::from_partitions(
            1,
            0,
            0,
            TransactionResult::Abort.id(),
            std::iter::empty::<(&str, i32)>(),
        );
        assert!(empty.topics.is_empty());
        assert_eq!(empty.producer_id, 1);
        assert_eq!(empty.producer_epoch, 0);
        assert_eq!(empty.coordinator_epoch, 0);
        assert!(!empty.transaction_result);
        let grouped = WritableTxnMarker::from_partitions(
            1000,
            1,
            3,
            TransactionResult::Commit.id(),
            [("a", 0), ("b", 2), ("a", 1)],
        );
        assert_eq!(
            grouped,
            WritableTxnMarker {
                producer_id: 1000,
                producer_epoch: 1,
                transaction_result: TransactionResult::Commit.id(),
                topics: vec![
                    WritableTxnMarkerTopic {
                        name: "a".into(),
                        partitions: vec![0, 1],
                    },
                    WritableTxnMarkerTopic {
                        name: "b".into(),
                        partitions: vec![2],
                    },
                ],
                coordinator_epoch: 3,
            }
        );
        let dup = WritableTxnMarker::from_partitions(
            1000,
            0,
            0,
            TransactionResult::Abort.id(),
            [("t", 0), ("t", 0)],
        );
        assert_eq!(
            dup.topics,
            vec![WritableTxnMarkerTopic {
                name: "t".into(),
                partitions: vec![0, 0],
            }]
        );
        let markers = std::slice::from_ref(&grouped);
        let mut buf = BytesMut::new();
        encode_write_txn_markers_request(&mut buf, 0, markers).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_request(&mut cur, 0).unwrap();
        assert_eq!(decoded, vec![grouped.clone()]);
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v0 from_partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_write_txn_markers_request(&mut buf, 1, markers).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_request(&mut cur, 1).unwrap();
        assert_eq!(decoded, vec![grouped.clone()]);
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v1 from_partitions leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn write_txn_markers_get_error_response_matches_java() {
        // Java WriteTxnMarkersRequest.getErrorResponse: inner HashMap.put
        // keeps the last (topic, partition) per marker. A later marker
        // overwrites an earlier one for the same producer id. Empty
        // topics are dropped. Duplicate pairs are not kept. Topic
        // grouping is WriteTxnMarkersResponse(Map). Distinct from
        // WritableTxnMarker.result, which copies the request layout.
        assert!(WriteTxnMarkersRequest::error_response(
            &[],
            crate::error::CLUSTER_AUTHORIZATION_FAILED
        )
        .is_empty());
        let grouped = WritableTxnMarker {
            producer_id: 1000,
            producer_epoch: 1,
            transaction_result: TransactionResult::Commit.id(),
            topics: vec![
                WritableTxnMarkerTopic {
                    name: "a".into(),
                    partitions: vec![0],
                },
                WritableTxnMarkerTopic {
                    name: "b".into(),
                    partitions: vec![1],
                },
                WritableTxnMarkerTopic {
                    name: "a".into(),
                    partitions: vec![2],
                },
            ],
            coordinator_epoch: 3,
        };
        let err = WriteTxnMarkersRequest::error_response(
            std::slice::from_ref(&grouped),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
        );
        assert_eq!(
            err,
            vec![WritableTxnMarkerResult {
                producer_id: 1000,
                topics: vec![
                    WritableTxnMarkerTopicResult {
                        name: "a".into(),
                        partitions: vec![
                            WritableTxnMarkerPartitionResult {
                                partition_index: 0,
                                error_code: crate::error::CLUSTER_AUTHORIZATION_FAILED,
                            },
                            WritableTxnMarkerPartitionResult {
                                partition_index: 2,
                                error_code: crate::error::CLUSTER_AUTHORIZATION_FAILED,
                            },
                        ],
                    },
                    WritableTxnMarkerTopicResult {
                        name: "b".into(),
                        partitions: vec![WritableTxnMarkerPartitionResult {
                            partition_index: 1,
                            error_code: crate::error::CLUSTER_AUTHORIZATION_FAILED,
                        }],
                    },
                ],
            }]
        );
        let result_layout = grouped.result(crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert_eq!(result_layout.topics.len(), 3);
        assert_eq!(err.first().map(|m| m.topics.len()), Some(2));
        let dup = WritableTxnMarker {
            producer_id: 1000,
            producer_epoch: 0,
            transaction_result: TransactionResult::Abort.id(),
            topics: vec![WritableTxnMarkerTopic {
                name: "t".into(),
                partitions: vec![0, 0],
            }],
            coordinator_epoch: 0,
        };
        let dup_err = WriteTxnMarkersRequest::error_response(
            std::slice::from_ref(&dup),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
        );
        assert_eq!(
            dup_err.first().and_then(|m| m.topics.first()),
            Some(&WritableTxnMarkerTopicResult {
                name: "t".into(),
                partitions: vec![WritableTxnMarkerPartitionResult {
                    partition_index: 0,
                    error_code: crate::error::CLUSTER_AUTHORIZATION_FAILED,
                }],
            })
        );
        assert_eq!(
            dup.result(crate::error::CLUSTER_AUTHORIZATION_FAILED)
                .topics
                .first()
                .map(|t| t.partitions.len()),
            Some(2)
        );
        let empty_topic = WritableTxnMarker {
            producer_id: 7,
            producer_epoch: 0,
            transaction_result: false,
            topics: vec![
                WritableTxnMarkerTopic {
                    name: "empty".into(),
                    partitions: Vec::new(),
                },
                WritableTxnMarkerTopic {
                    name: "kept".into(),
                    partitions: vec![4],
                },
            ],
            coordinator_epoch: 0,
        };
        let dropped = WriteTxnMarkersRequest::error_response(
            std::slice::from_ref(&empty_topic),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
        );
        assert_eq!(
            dropped
                .first()
                .map(|m| m.topics.iter().map(|t| t.name.as_str()).collect::<Vec<_>>()),
            Some(vec!["kept"])
        );
        let empty_marker = WritableTxnMarker {
            producer_id: 8,
            producer_epoch: 0,
            transaction_result: false,
            topics: Vec::new(),
            coordinator_epoch: 0,
        };
        let kept_empty = WriteTxnMarkersRequest::error_response(
            std::slice::from_ref(&empty_marker),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
        );
        assert_eq!(
            kept_empty,
            vec![WritableTxnMarkerResult {
                producer_id: 8,
                topics: Vec::new(),
            }]
        );
        let overwrite = WriteTxnMarkersRequest::error_response(
            &[
                WritableTxnMarker {
                    producer_id: 1,
                    producer_epoch: 0,
                    transaction_result: false,
                    topics: vec![WritableTxnMarkerTopic {
                        name: "first".into(),
                        partitions: vec![0],
                    }],
                    coordinator_epoch: 0,
                },
                WritableTxnMarker {
                    producer_id: 2,
                    producer_epoch: 0,
                    transaction_result: false,
                    topics: vec![WritableTxnMarkerTopic {
                        name: "other".into(),
                        partitions: vec![1],
                    }],
                    coordinator_epoch: 0,
                },
                WritableTxnMarker {
                    producer_id: 1,
                    producer_epoch: 0,
                    transaction_result: false,
                    topics: vec![WritableTxnMarkerTopic {
                        name: "last".into(),
                        partitions: vec![9],
                    }],
                    coordinator_epoch: 0,
                },
            ],
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
        );
        assert_eq!(
            overwrite
                .iter()
                .map(|m| (
                    m.producer_id,
                    m.topics.iter().map(|t| t.name.as_str()).collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>(),
            vec![(1, vec!["last"]), (2, vec!["other"])]
        );
        let mut buf = BytesMut::new();
        encode_write_txn_markers_request(&mut buf, 0, std::slice::from_ref(&grouped)).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_request(&mut cur, 0).unwrap();
        assert_eq!(
            WriteTxnMarkersRequest::error_response(
                &decoded,
                crate::error::CLUSTER_AUTHORIZATION_FAILED
            ),
            err
        );
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v0 getErrorResponse request leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_write_txn_markers_response(&mut buf, 0, &err).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, err);
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v0 getErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_write_txn_markers_response(&mut buf, 1, &err).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_write_txn_markers_response(&mut cur, 1).unwrap();
        assert_eq!(decoded, err);
        assert!(
            cur.is_empty(),
            "WriteTxnMarkers v1 getErrorResponse leftover-empty; leftover {} bytes",
            cur.len()
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
    fn add_partitions_to_txn_response_errors_matches_java() {
        // Java AddPartitionsToTxnResponse.errors for v0–v3: empty
        // resultsByTopicV3AndBelow is an empty map. Otherwise the only
        // key is V3_AND_BELOW_TXN_ID (""). errorsForTransaction flattens
        // (topic, partition) codes; a later partition overwrites
        // (HashMap.put). v4+ resultsByTransaction is not spoken.
        assert_eq!(AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID, "");
        assert!(AddPartitionsToTxnResponse::errors(&[]).is_empty());
        assert!(AddPartitionsToTxnResponse::errors_for_transaction(&[]).is_empty());
        let one = vec![AddPartitionsToTxnTopicResult {
            topic: "t".into(),
            partitions: vec![
                AddPartitionsToTxnPartitionResult::error(0, 0),
                AddPartitionsToTxnPartitionResult::error(1, crate::error::NOT_LEADER_OR_FOLLOWER),
            ],
        }];
        let one_inner = HashMap::from([
            (("t".into(), 0), 0),
            (("t".into(), 1), crate::error::NOT_LEADER_OR_FOLLOWER),
        ]);
        assert_eq!(
            AddPartitionsToTxnResponse::errors_for_transaction(&one),
            one_inner
        );
        assert_eq!(
            AddPartitionsToTxnResponse::errors(&one),
            HashMap::from([(
                AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID.to_string(),
                one_inner
            )])
        );
        let two = vec![
            AddPartitionsToTxnTopicResult {
                topic: "a".into(),
                partitions: vec![AddPartitionsToTxnPartitionResult::error(
                    0,
                    crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                )],
            },
            AddPartitionsToTxnTopicResult {
                topic: "a".into(),
                partitions: vec![AddPartitionsToTxnPartitionResult::error(
                    0,
                    crate::error::NOT_LEADER_OR_FOLLOWER,
                )],
            },
            AddPartitionsToTxnTopicResult {
                topic: "b".into(),
                partitions: vec![AddPartitionsToTxnPartitionResult::error(3, 0)],
            },
        ];
        let two_inner = HashMap::from([
            (("a".into(), 0), crate::error::NOT_LEADER_OR_FOLLOWER),
            (("b".into(), 3), 0),
        ]);
        assert_eq!(
            AddPartitionsToTxnResponse::errors_for_transaction(&two),
            two_inner
        );
        assert_eq!(
            AddPartitionsToTxnResponse::errors(&two),
            HashMap::from([(
                AddPartitionsToTxnResponse::V3_AND_BELOW_TXN_ID.to_string(),
                two_inner.clone()
            )])
        );
        let mut buf = BytesMut::new();
        encode_add_partitions_to_txn_topics_response(&mut buf, 0, &two).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_add_partitions_to_txn_topics_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(
            AddPartitionsToTxnResponse::errors(&decoded),
            AddPartitionsToTxnResponse::errors(&two)
        );
        assert!(
            cur.is_empty(),
            "AddPartitionsToTxn v0 errors leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_add_partitions_to_txn_topics_response(&mut buf, 3, &two).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_add_partitions_to_txn_topics_response(&mut cur, 3).unwrap();
        assert_eq!(decoded, two);
        assert_eq!(
            AddPartitionsToTxnResponse::errors(&decoded),
            AddPartitionsToTxnResponse::errors(&two)
        );
        assert!(
            cur.is_empty(),
            "AddPartitionsToTxn v3 errors leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn add_partitions_to_txn_response_from_errors_matches_java() {
        // Java AddPartitionsToTxnResponse.topicCollectionForErrors:
        // HashMap.getOrDefault by topic name, then collection add.
        // Empty map is empty. A later entry for the same name appends
        // even when another topic sits between. A later partition with
        // the same index is ignored (mapKey PartitionIndex;
        // ImplicitLinkedHashCollection.add keeps the first).
        assert!(
            AddPartitionsToTxnResponse::from_errors(std::iter::empty::<(&str, i32, i16)>())
                .is_empty()
        );
        let grouped = AddPartitionsToTxnResponse::from_errors([
            ("a", 0, crate::error::UNKNOWN_TOPIC_OR_PARTITION),
            ("b", 0, crate::error::NOT_LEADER_OR_FOLLOWER),
            ("a", 1, 0i16),
        ]);
        assert_eq!(
            grouped,
            vec![
                AddPartitionsToTxnTopicResult {
                    topic: "a".into(),
                    partitions: vec![
                        AddPartitionsToTxnPartitionResult::error(
                            0,
                            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                        ),
                        AddPartitionsToTxnPartitionResult::error(1, 0),
                    ],
                },
                AddPartitionsToTxnTopicResult {
                    topic: "b".into(),
                    partitions: vec![AddPartitionsToTxnPartitionResult::error(
                        0,
                        crate::error::NOT_LEADER_OR_FOLLOWER,
                    )],
                },
            ]
        );
        let dup = AddPartitionsToTxnResponse::from_errors([
            ("t", 0, 0i16),
            ("t", 0, crate::error::NOT_LEADER_OR_FOLLOWER),
        ]);
        assert_eq!(
            dup,
            vec![AddPartitionsToTxnTopicResult {
                topic: "t".into(),
                partitions: vec![AddPartitionsToTxnPartitionResult::error(0, 0)],
            }]
        );
        let mut buf = BytesMut::new();
        encode_add_partitions_to_txn_topics_response(&mut buf, 0, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_add_partitions_to_txn_topics_response(&mut cur, 0).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "AddPartitionsToTxn v0 from_errors leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_add_partitions_to_txn_topics_response(&mut buf, 3, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let (decoded, ..) = decode_add_partitions_to_txn_topics_response(&mut cur, 3).unwrap();
        assert_eq!(decoded, grouped);
        assert!(
            cur.is_empty(),
            "AddPartitionsToTxn v3 from_errors leftover-empty; leftover {} bytes",
            cur.len()
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
    fn end_txn_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 EndTxnResponse.json ThrottleTimeMs is versions 0+
        // (INT32 on spoken v0–v5; first field). Official Java
        // EndTxnRequest.getErrorResponse / EndTxnResponse.throttleTimeMs
        // set / read it. encode_end_txn_response still writes the JSON
        // default 0. KIP-219 only changes shouldClientThrottle (v1+).
        // Empty-error v0 == v1 == v2 (classic); v3 == v4 (flexible;
        // TRANSACTION_ABORTABLE same layout); v5 adds ProducerId /
        // ProducerEpoch. Top-level ErrorCode is at bytes 4–5. This crate
        // speaks 0–5. This is not AddOffsetsToTxn ThrottleTimeMs.
        for version in [0, 1, 2, 3, 4, 5] {
            let mut buf = BytesMut::new();
            encode_end_txn_response_with_throttle(
                &mut buf,
                version,
                0,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, pid, epoch, throttle) =
                decode_end_txn_response(&mut cur, version).unwrap();
            assert_eq!(decoded, 0);
            assert_eq!(pid, RecordBatch::NO_PRODUCER_ID);
            assert_eq!(epoch, RecordBatch::NO_PRODUCER_EPOCH);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "EndTxn v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_end_txn_response_with_throttle(
            &mut with,
            0,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            3_600_000,
        )
        .unwrap();
        let mut zero = BytesMut::new();
        encode_end_txn_response_with_throttle(
            &mut zero,
            0,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            0,
        )
        .unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_end_txn_response(
            &mut conv,
            0,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_end_txn_response still writes ThrottleTimeMs 0"
        );

        let mut v1_with = BytesMut::new();
        encode_end_txn_response_with_throttle(
            &mut v1_with,
            1,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            3_600_000,
        )
        .unwrap();
        let mut v2_with = BytesMut::new();
        encode_end_txn_response_with_throttle(
            &mut v2_with,
            2,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "empty-error ThrottleTimeMs bodies: v0 == v1"
        );
        assert_eq!(
            &v1_with[..],
            &v2_with[..],
            "empty-error ThrottleTimeMs bodies: v1 == v2"
        );
        let mut v3_with = BytesMut::new();
        encode_end_txn_response_with_throttle(
            &mut v3_with,
            3,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            3_600_000,
        )
        .unwrap();
        assert_ne!(&v2_with[..], &v3_with[..], "v3 adds compact tagged fields");
        let mut v4_with = BytesMut::new();
        encode_end_txn_response_with_throttle(
            &mut v4_with,
            4,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &v3_with[..],
            &v4_with[..],
            "empty-error ThrottleTimeMs bodies: v3 == v4"
        );
        let mut v5_with = BytesMut::new();
        encode_end_txn_response_with_throttle(
            &mut v5_with,
            5,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            3_600_000,
        )
        .unwrap();
        assert_ne!(
            &v4_with[..],
            &v5_with[..],
            "v5 adds ProducerId / ProducerEpoch"
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
                RecordBatch::NO_PRODUCER_EPOCH,
                0
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
                RecordBatch::NO_PRODUCER_EPOCH,
                0
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
        assert_eq!(decode_end_txn_response(&mut cur, 5).unwrap(), (0, 9, 2, 0));
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
    fn end_txn_error_response_matches_java() {
        // Java EndTxnRequest.getErrorResponse sets throttleTimeMs from
        // the argument. Crate convenience encode still writes 0.
        // ProducerId / ProducerEpoch stay JSON defaults (-1) on v5+.
        for version in [0_i16, 3, 5] {
            let mut expected = BytesMut::new();
            encode_end_txn_response(
                &mut expected,
                version,
                16,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
            )
            .unwrap();
            let mut got = BytesMut::new();
            EndTxnRequest::error_response(&mut got, version, 16).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "EndTxn v{version} getErrorResponse must match sentinel encode"
            );
            let mut cur = &got[..];
            let (err, pid, epoch, ..) = decode_end_txn_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert_eq!(pid, RecordBatch::NO_PRODUCER_ID);
            assert_eq!(epoch, RecordBatch::NO_PRODUCER_EPOCH);
            assert!(
                cur.is_empty(),
                "EndTxn v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        let mut v3 = BytesMut::new();
        EndTxnRequest::error_response(&mut v3, 3, 16).unwrap();
        let mut v5 = BytesMut::new();
        EndTxnRequest::error_response(&mut v5, 5, 16).unwrap();
        assert_ne!(
            &v3[..],
            &v5[..],
            "v5 getErrorResponse includes ProducerId / ProducerEpoch JSON defaults"
        );
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
        let (tid, gid, member, got, ..) = decode_txn_offset_commit_request(&mut cur, 0).unwrap();
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
        let (tid, gid, member, got, ..) = decode_txn_offset_commit_request(&mut cur, 2).unwrap();
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
    fn txn_offset_commit_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 TxnOffsetCommitResponse.json ThrottleTimeMs is
        // versions 0+ (INT32 on spoken v0–v5; first field). Official
        // Java TxnOffsetCommitRequest.getErrorResponse /
        // TxnOffsetCommitResponse.throttleTimeMs set / read it.
        // encode_txn_offset_commit_topics_response still writes the
        // JSON default 0. KIP-219 only changes shouldClientThrottle
        // (v1+). Empty-Topics v0 == v1 == v2 (classic); v3 == v4 == v5
        // (flexible; TRANSACTION_ABORTABLE / KIP-890 Part 2 same
        // layout). There is no top-level ErrorCode. This crate speaks
        // 0–5. This is not EndTxn ThrottleTimeMs.
        let topics: Vec<TxnOffsetCommitResponseTopic> = vec![];
        for version in [0, 1, 2, 3, 4, 5] {
            let mut buf = BytesMut::new();
            encode_txn_offset_commit_topics_response_with_throttle(
                &mut buf, version, &topics, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_txn_offset_commit_topics_response(&mut cur, version).unwrap();
            assert_eq!(decoded, topics);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "TxnOffsetCommit v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_txn_offset_commit_topics_response_with_throttle(&mut with, 0, &topics, 3_600_000)
            .unwrap();
        let mut zero = BytesMut::new();
        encode_txn_offset_commit_topics_response_with_throttle(&mut zero, 0, &topics, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_txn_offset_commit_topics_response(&mut conv, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_txn_offset_commit_topics_response still writes ThrottleTimeMs 0"
        );

        let mut v1_with = BytesMut::new();
        encode_txn_offset_commit_topics_response_with_throttle(&mut v1_with, 1, &topics, 3_600_000)
            .unwrap();
        let mut v2_with = BytesMut::new();
        encode_txn_offset_commit_topics_response_with_throttle(&mut v2_with, 2, &topics, 3_600_000)
            .unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "empty-Topics ThrottleTimeMs bodies: v0 == v1"
        );
        assert_eq!(
            &v1_with[..],
            &v2_with[..],
            "empty-Topics ThrottleTimeMs bodies: v1 == v2"
        );
        let mut v3_with = BytesMut::new();
        encode_txn_offset_commit_topics_response_with_throttle(&mut v3_with, 3, &topics, 3_600_000)
            .unwrap();
        assert_ne!(&v2_with[..], &v3_with[..], "v3 adds compact tagged fields");
        let mut v4_with = BytesMut::new();
        encode_txn_offset_commit_topics_response_with_throttle(&mut v4_with, 4, &topics, 3_600_000)
            .unwrap();
        let mut v5_with = BytesMut::new();
        encode_txn_offset_commit_topics_response_with_throttle(&mut v5_with, 5, &topics, 3_600_000)
            .unwrap();
        assert_eq!(
            &v3_with[..],
            &v4_with[..],
            "empty-Topics ThrottleTimeMs bodies: v3 == v4"
        );
        assert_eq!(
            &v4_with[..],
            &v5_with[..],
            "empty-Topics ThrottleTimeMs bodies: v4 == v5"
        );
    }

    #[test]
    fn txn_offset_commit_request_producer_id_matches_java() {
        // Kafka 4.0.0 TxnOffsetCommitRequest.json ProducerId is versions
        // 0+ (INT64 after GroupId / before ProducerEpoch). Official Java
        // TxnOffsetCommitRequestData.producerId. Encode already writes
        // producer_id. Decode previously discarded it. Kafka 4.0
        // validVersions is 0-5. This crate speaks 0–5. This is not
        // ProducerEpoch / AddOffsetsToTxn ProducerId / EndTxn response
        // ProducerId / InitProducerId / AddPartitionsToTxn ProducerId /
        // WriteTxnMarkers ProducerId.
        let member = TxnOffsetCommitMember::unknown();
        let topics: [TxnOffsetTopic; 0] = [];
        for version in [0_i16, 1, 2, 3, 4, 5] {
            let mut buf = BytesMut::new();
            encode_txn_offset_commit_request(&mut buf, version, "tx", "g", 9, 1, &member, &topics)
                .unwrap();
            let mut cur = buf.as_ref();
            let (tid, gid, got_member, got, producer_id) =
                decode_txn_offset_commit_request(&mut cur, version).unwrap();
            assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
            assert_eq!(got_member, member);
            assert!(got.is_empty());
            assert_eq!(producer_id, 9);
            assert!(
                cur.is_empty(),
                "TxnOffsetCommit v{version} ProducerId leftover-empty"
            );
        }

        let mut nine = BytesMut::new();
        encode_txn_offset_commit_request(&mut nine, 0, "tx", "g", 9, 1, &member, &topics).unwrap();
        let mut ten = BytesMut::new();
        encode_txn_offset_commit_request(&mut ten, 0, "tx", "g", 10, 1, &member, &topics).unwrap();
        assert_ne!(
            &nine[..],
            &ten[..],
            "v0 ProducerId is not always the same INT64"
        );
        let mut cur = nine.as_ref();
        let (.., producer_id) = decode_txn_offset_commit_request(&mut cur, 0).unwrap();
        assert_eq!(producer_id, 9);
        assert!(
            cur.is_empty(),
            "TxnOffsetCommit v0 ProducerId leftover-empty"
        );
        let mut cur = ten.as_ref();
        let (.., producer_id) = decode_txn_offset_commit_request(&mut cur, 0).unwrap();
        assert_eq!(producer_id, 10);
        assert_eq!(
            nine.get(7..15),
            Some([0, 0, 0, 0, 0, 0, 0, 9].as_slice()),
            "v0 classic ProducerId follows TransactionalId STRING tx and GroupId STRING g"
        );

        let mut v1 = BytesMut::new();
        encode_txn_offset_commit_request(&mut v1, 1, "tx", "g", 9, 1, &member, &topics).unwrap();
        assert_eq!(
            &nine[..],
            &v1[..],
            "empty-Topics ProducerId bodies: v0 == v1"
        );
        let mut v2 = BytesMut::new();
        encode_txn_offset_commit_request(&mut v2, 2, "tx", "g", 9, 1, &member, &topics).unwrap();
        assert_eq!(&v1[..], &v2[..], "empty-Topics ProducerId bodies: v1 == v2");
        let mut v3 = BytesMut::new();
        encode_txn_offset_commit_request(&mut v3, 3, "tx", "g", 9, 1, &member, &topics).unwrap();
        assert_ne!(
            &v2[..],
            &v3[..],
            "v3 adds GenerationId / MemberId / GroupInstanceId and compact tagged fields"
        );
        let mut v4 = BytesMut::new();
        encode_txn_offset_commit_request(&mut v4, 4, "tx", "g", 9, 1, &member, &topics).unwrap();
        assert_eq!(&v3[..], &v4[..], "empty-Topics ProducerId bodies: v3 == v4");
        let mut v5 = BytesMut::new();
        encode_txn_offset_commit_request(&mut v5, 5, "tx", "g", 9, 1, &member, &topics).unwrap();
        assert_eq!(&v4[..], &v5[..], "empty-Topics ProducerId bodies: v4 == v5");
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
        let (tid, gid, got_member, got, ..) =
            decode_txn_offset_commit_request(&mut cur, 3).unwrap();
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
        let (_tid, _gid, got_member, got, ..) =
            decode_txn_offset_commit_request(&mut cur, 4).unwrap();
        assert_eq!(got_member, member);
        assert_eq!(got, topics);
        assert!(cur.is_empty(), "TxnOffsetCommit v4 shares the v3 layout");

        req.clear();
        encode_txn_offset_commit_request(&mut req, 5, "tx", "g", 9, 1, &member, &topics).unwrap();
        let mut cur = &req[..];
        let (_tid, _gid, got_member, got, ..) =
            decode_txn_offset_commit_request(&mut cur, 5).unwrap();
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
                decode_txn_offset_commit_topics_response(&mut cur, version)
                    .unwrap()
                    .0,
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
                decode_txn_offset_commit_topics_response(&mut cur, version)
                    .unwrap()
                    .0,
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
    fn txn_offset_commit_offsets_matches_java() {
        // Java TxnOffsetCommitRequest.offsets: each (topic, partition) maps
        // to that CommittedOffset. Later partitions overwrite the same pair
        // (HashMap.put).
        assert!(TxnOffsetCommitRequest::offsets(&[]).is_empty());
        let p0 = TxnOffsetPartition::new(0, 10, 1, "m0");
        let p3 = TxnOffsetPartition::new(3, 20, 2, "m3");
        let one = vec![TxnOffsetTopic {
            topic: "t".into(),
            partitions: vec![p0.clone(), p3.clone()],
        }];
        assert_eq!(
            TxnOffsetCommitRequest::offsets(&one),
            HashMap::from([(("t".into(), 0), p0), (("t".into(), 3), p3)])
        );
        let first = TxnOffsetPartition::new(0, 1, RecordBatch::NO_PARTITION_LEADER_EPOCH, "");
        let second = TxnOffsetPartition::new(0, 2, 4, "eos");
        let other = TxnOffsetPartition::new(1, 3, 5, "b");
        let two = vec![
            TxnOffsetTopic {
                topic: "a".into(),
                partitions: vec![first],
            },
            TxnOffsetTopic {
                topic: "a".into(),
                partitions: vec![second.clone()],
            },
            TxnOffsetTopic {
                topic: "b".into(),
                partitions: vec![other.clone()],
            },
        ];
        assert_eq!(
            TxnOffsetCommitRequest::offsets(&two),
            HashMap::from([(("a".into(), 0), second), (("b".into(), 1), other),])
        );
        let mut buf = BytesMut::new();
        encode_txn_offset_commit_request(
            &mut buf,
            2,
            "tx",
            "g",
            9,
            1,
            &TxnOffsetCommitMember::unknown(),
            &two,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_txn_offset_commit_request(&mut cur, 2).unwrap().3;
        assert_eq!(decoded, two);
        assert_eq!(
            TxnOffsetCommitRequest::offsets(&decoded),
            TxnOffsetCommitRequest::offsets(&two)
        );
        assert!(
            !cur.has_remaining(),
            "TxnOffsetCommit v2 offsets leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        buf.clear();
        encode_txn_offset_commit_request(
            &mut buf,
            3,
            "tx",
            "g",
            9,
            1,
            &TxnOffsetCommitMember::unknown(),
            &two,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_txn_offset_commit_request(&mut cur, 3).unwrap().3;
        assert_eq!(decoded, two);
        assert_eq!(
            TxnOffsetCommitRequest::offsets(&decoded),
            TxnOffsetCommitRequest::offsets(&two)
        );
        assert!(
            !cur.has_remaining(),
            "TxnOffsetCommit v3 offsets leftover-empty; leftover {} bytes",
            cur.remaining()
        );
    }

    #[test]
    fn txn_offset_commit_from_offsets_matches_java() {
        // Java TxnOffsetCommitRequest.getTopics: HashMap.getOrDefault by
        // topic name, then partitions.add. Empty map is empty. A later
        // entry for the same name appends even when another topic sits
        // between. Duplicate partitions for the same pair are kept
        // (ArrayList).
        assert!(TxnOffsetCommitRequest::from_offsets(
            std::iter::empty::<(&str, TxnOffsetPartition)>()
        )
        .is_empty());
        let a0 = TxnOffsetPartition::new(0, 10, 1, "m0");
        let a1 = TxnOffsetPartition::new(1, 11, 2, "m1");
        let b0 = TxnOffsetPartition::new(0, 20, RecordBatch::NO_PARTITION_LEADER_EPOCH, "");
        let grouped = TxnOffsetCommitRequest::from_offsets([
            ("a", a0.clone()),
            ("b", b0.clone()),
            ("a", a1.clone()),
        ]);
        assert_eq!(
            grouped,
            vec![
                TxnOffsetTopic {
                    topic: "a".into(),
                    partitions: vec![a0, a1],
                },
                TxnOffsetTopic {
                    topic: "b".into(),
                    partitions: vec![b0],
                },
            ]
        );
        let first = TxnOffsetPartition::new(0, 1, RecordBatch::NO_PARTITION_LEADER_EPOCH, "");
        let second = TxnOffsetPartition::new(0, 2, 4, "eos");
        let dup =
            TxnOffsetCommitRequest::from_offsets([("t", first.clone()), ("t", second.clone())]);
        assert_eq!(
            dup,
            vec![TxnOffsetTopic {
                topic: "t".into(),
                partitions: vec![first, second],
            }]
        );
        let mut buf = BytesMut::new();
        encode_txn_offset_commit_request(
            &mut buf,
            2,
            "tx",
            "g",
            9,
            1,
            &TxnOffsetCommitMember::unknown(),
            &grouped,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_txn_offset_commit_request(&mut cur, 2).unwrap().3;
        assert_eq!(decoded, grouped);
        assert!(
            !cur.has_remaining(),
            "TxnOffsetCommit v2 from_offsets leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        buf.clear();
        encode_txn_offset_commit_request(
            &mut buf,
            3,
            "tx",
            "g",
            9,
            1,
            &TxnOffsetCommitMember::unknown(),
            &grouped,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_txn_offset_commit_request(&mut cur, 3).unwrap().3;
        assert_eq!(decoded, grouped);
        assert!(
            !cur.has_remaining(),
            "TxnOffsetCommit v3 from_offsets leftover-empty; leftover {} bytes",
            cur.remaining()
        );
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
    fn add_partitions_to_txn_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 AddPartitionsToTxnResponse.json ThrottleTimeMs is
        // versions 0+ (INT32 on spoken v0–v3; first field). Official
        // Java AddPartitionsToTxnRequest.getErrorResponse /
        // AddPartitionsToTxnResponse.throttleTimeMs set / read it.
        // encode_add_partitions_to_txn_topics_response still writes the
        // JSON default 0. KIP-219 only changes shouldClientThrottle
        // (v1+). Empty-ResultsByTopicV3AndBelow v0 == v1 == v2
        // (classic); v3 is flexible. There is no top-level ErrorCode on
        // spoken versions. This crate speaks 0–3. This is not
        // DeleteGroups ThrottleTimeMs.
        let topics: Vec<AddPartitionsToTxnTopicResult> = vec![];
        for version in [0, 1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_add_partitions_to_txn_topics_response_with_throttle(
                &mut buf, version, &topics, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_add_partitions_to_txn_topics_response(&mut cur, version).unwrap();
            assert_eq!(decoded, topics);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "AddPartitionsToTxn v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_add_partitions_to_txn_topics_response_with_throttle(
            &mut with, 0, &topics, 3_600_000,
        )
        .unwrap();
        let mut zero = BytesMut::new();
        encode_add_partitions_to_txn_topics_response_with_throttle(&mut zero, 0, &topics, 0)
            .unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_add_partitions_to_txn_topics_response(&mut conv, 0, &topics).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_add_partitions_to_txn_topics_response still writes ThrottleTimeMs 0"
        );

        let mut v1_with = BytesMut::new();
        encode_add_partitions_to_txn_topics_response_with_throttle(
            &mut v1_with,
            1,
            &topics,
            3_600_000,
        )
        .unwrap();
        let mut v2_with = BytesMut::new();
        encode_add_partitions_to_txn_topics_response_with_throttle(
            &mut v2_with,
            2,
            &topics,
            3_600_000,
        )
        .unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "empty-ResultsByTopicV3AndBelow ThrottleTimeMs bodies: v0 == v1"
        );
        assert_eq!(
            &v1_with[..],
            &v2_with[..],
            "empty-ResultsByTopicV3AndBelow ThrottleTimeMs bodies: v1 == v2"
        );
        let mut v3_with = BytesMut::new();
        encode_add_partitions_to_txn_topics_response_with_throttle(
            &mut v3_with,
            3,
            &topics,
            3_600_000,
        )
        .unwrap();
        assert_ne!(
            &v2_with[..],
            &v3_with[..],
            "v3 adds compact arrays/strings plus tagged fields"
        );
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
                decode_add_partitions_to_txn_topics_response(&mut cur, version)
                    .unwrap()
                    .0,
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
                decode_add_partitions_to_txn_topics_response(&mut cur, version)
                    .unwrap()
                    .0,
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
    fn add_partitions_to_txn_partitions_matches_java() {
        // Java AddPartitionsToTxnRequest.getPartitions: each (topic,
        // partition) in request order. Duplicate pairs are kept
        // (ArrayList).
        assert!(AddPartitionsToTxnRequest::partitions(&[]).is_empty());
        let one = vec![TxnPartitionsTopic {
            topic: "t".into(),
            partitions: vec![0, 3],
        }];
        assert_eq!(
            AddPartitionsToTxnRequest::partitions(&one),
            vec![("t".into(), 0), ("t".into(), 3)]
        );
        let two = vec![
            TxnPartitionsTopic {
                topic: "a".into(),
                partitions: vec![0, 0],
            },
            TxnPartitionsTopic {
                topic: "b".into(),
                partitions: vec![1],
            },
        ];
        assert_eq!(
            AddPartitionsToTxnRequest::partitions(&two),
            vec![("a".into(), 0), ("a".into(), 0), ("b".into(), 1),]
        );
        let mut buf = BytesMut::new();
        encode_add_partitions_to_txn_request(&mut buf, 0, "tx", 9, 1, &two).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_add_partitions_to_txn_request(&mut cur, 0).unwrap().3;
        assert_eq!(decoded, two);
        assert_eq!(
            AddPartitionsToTxnRequest::partitions(&decoded),
            AddPartitionsToTxnRequest::partitions(&two)
        );
        assert!(
            !cur.has_remaining(),
            "AddPartitionsToTxn v0 getPartitions leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        buf.clear();
        encode_add_partitions_to_txn_request(&mut buf, 3, "tx", 9, 1, &two).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_add_partitions_to_txn_request(&mut cur, 3).unwrap().3;
        assert_eq!(decoded, two);
        assert_eq!(
            AddPartitionsToTxnRequest::partitions(&decoded),
            AddPartitionsToTxnRequest::partitions(&two)
        );
        assert!(
            !cur.has_remaining(),
            "AddPartitionsToTxn v3 getPartitions leftover-empty; leftover {} bytes",
            cur.remaining()
        );
    }

    #[test]
    fn add_partitions_to_txn_from_partitions_matches_java() {
        // Java AddPartitionsToTxnRequest.buildTxnTopicCollection:
        // HashMap.compute by topic name, then List.add. Empty list is
        // empty Topics. A later entry for the same name appends even
        // when another topic sits between. Duplicate partitions for
        // the same pair are kept (ArrayList of Partitions).
        assert!(
            AddPartitionsToTxnRequest::from_partitions(std::iter::empty::<(&str, i32)>())
                .is_empty()
        );
        let grouped = AddPartitionsToTxnRequest::from_partitions([("a", 0), ("b", 1), ("a", 2)]);
        assert_eq!(
            grouped,
            vec![
                TxnPartitionsTopic {
                    topic: "a".into(),
                    partitions: vec![0, 2],
                },
                TxnPartitionsTopic {
                    topic: "b".into(),
                    partitions: vec![1],
                },
            ]
        );
        let dup = AddPartitionsToTxnRequest::from_partitions([("t", 0), ("t", 0)]);
        assert_eq!(
            dup,
            vec![TxnPartitionsTopic {
                topic: "t".into(),
                partitions: vec![0, 0],
            }]
        );
        let mut buf = BytesMut::new();
        encode_add_partitions_to_txn_request(&mut buf, 0, "tx", 9, 1, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_add_partitions_to_txn_request(&mut cur, 0).unwrap().3;
        assert_eq!(decoded, grouped);
        assert!(
            !cur.has_remaining(),
            "AddPartitionsToTxn v0 from_partitions leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        buf.clear();
        encode_add_partitions_to_txn_request(&mut buf, 3, "tx", 9, 1, &grouped).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_add_partitions_to_txn_request(&mut cur, 3).unwrap().3;
        assert_eq!(decoded, grouped);
        assert!(
            !cur.has_remaining(),
            "AddPartitionsToTxn v3 from_partitions leftover-empty; leftover {} bytes",
            cur.remaining()
        );
    }

    #[test]
    fn add_offsets_to_txn_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 AddOffsetsToTxnResponse.json ThrottleTimeMs is
        // versions 0+ (INT32 on spoken v0–v4; first field). Official
        // Java AddOffsetsToTxnRequest.getErrorResponse /
        // AddOffsetsToTxnResponse.throttleTimeMs set / read it.
        // encode_add_offsets_to_txn_response still writes the JSON
        // default 0. KIP-219 only changes shouldClientThrottle (v1+).
        // Empty-error v0 == v1 == v2 (classic); v3 == v4 (flexible;
        // TRANSACTION_ABORTABLE same layout). Top-level ErrorCode is at
        // bytes 4–5. This crate speaks 0–4. This is not
        // AddPartitionsToTxn ThrottleTimeMs.
        for version in [0, 1, 2, 3, 4] {
            let mut buf = BytesMut::new();
            encode_add_offsets_to_txn_response_with_throttle(&mut buf, version, 0, 3_600_000)
                .unwrap();
            let mut cur = buf.as_ref();
            let (decoded, throttle) =
                decode_add_offsets_to_txn_response(&mut cur, version).unwrap();
            assert_eq!(decoded, 0);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "AddOffsetsToTxn v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_add_offsets_to_txn_response_with_throttle(&mut with, 0, 0, 3_600_000).unwrap();
        let mut zero = BytesMut::new();
        encode_add_offsets_to_txn_response_with_throttle(&mut zero, 0, 0, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );
        let mut conv = BytesMut::new();
        encode_add_offsets_to_txn_response(&mut conv, 0, 0).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_add_offsets_to_txn_response still writes ThrottleTimeMs 0"
        );

        let mut v1_with = BytesMut::new();
        encode_add_offsets_to_txn_response_with_throttle(&mut v1_with, 1, 0, 3_600_000).unwrap();
        let mut v2_with = BytesMut::new();
        encode_add_offsets_to_txn_response_with_throttle(&mut v2_with, 2, 0, 3_600_000).unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "empty-error ThrottleTimeMs bodies: v0 == v1"
        );
        assert_eq!(
            &v1_with[..],
            &v2_with[..],
            "empty-error ThrottleTimeMs bodies: v1 == v2"
        );
        let mut v3_with = BytesMut::new();
        encode_add_offsets_to_txn_response_with_throttle(&mut v3_with, 3, 0, 3_600_000).unwrap();
        assert_ne!(&v2_with[..], &v3_with[..], "v3 adds compact tagged fields");
        let mut v4_with = BytesMut::new();
        encode_add_offsets_to_txn_response_with_throttle(&mut v4_with, 4, 0, 3_600_000).unwrap();
        assert_eq!(
            &v3_with[..],
            &v4_with[..],
            "empty-error ThrottleTimeMs bodies: v3 == v4"
        );
    }

    #[test]
    fn add_offsets_to_txn_request_producer_id_matches_java() {
        // Kafka 4.0.0 AddOffsetsToTxnRequest.json ProducerId is versions
        // 0+ (INT64 after TransactionalId / before ProducerEpoch). Official
        // Java AddOffsetsToTxnRequestData.producerId. Encode already writes
        // producer_id. Decode previously discarded it. Kafka 4.0
        // validVersions is 0-4. This crate speaks 0–4. This is not
        // ProducerEpoch / EndTxn response ProducerId / InitProducerId /
        // TxnOffsetCommit ProducerId / AddPartitionsToTxn ProducerId /
        // WriteTxnMarkers ProducerId.
        for version in [0_i16, 1, 2, 3, 4] {
            let mut buf = BytesMut::new();
            encode_add_offsets_to_txn_request(&mut buf, version, "tx", 9, 1, "g").unwrap();
            let mut cur = buf.as_ref();
            let (tid, gid, producer_id, ..) =
                decode_add_offsets_to_txn_request(&mut cur, version).unwrap();
            assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
            assert_eq!(producer_id, 9);
            assert!(
                cur.is_empty(),
                "AddOffsetsToTxn v{version} ProducerId leftover-empty"
            );
        }

        let mut nine = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut nine, 0, "tx", 9, 1, "g").unwrap();
        let mut ten = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut ten, 0, "tx", 10, 1, "g").unwrap();
        assert_ne!(
            &nine[..],
            &ten[..],
            "v0 ProducerId is not always the same INT64"
        );
        let mut cur = nine.as_ref();
        let (_tid, _gid, producer_id, ..) = decode_add_offsets_to_txn_request(&mut cur, 0).unwrap();
        assert_eq!(producer_id, 9);
        assert!(
            cur.is_empty(),
            "AddOffsetsToTxn v0 ProducerId leftover-empty"
        );
        let mut cur = ten.as_ref();
        let (_tid, _gid, producer_id, ..) = decode_add_offsets_to_txn_request(&mut cur, 0).unwrap();
        assert_eq!(producer_id, 10);
        assert_eq!(
            nine.get(4..12),
            Some([0, 0, 0, 0, 0, 0, 0, 9].as_slice()),
            "v0 classic ProducerId follows TransactionalId STRING tx"
        );

        let mut v1 = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut v1, 1, "tx", 9, 1, "g").unwrap();
        assert_eq!(&nine[..], &v1[..], "ProducerId bodies: v0 == v1");
        let mut v2 = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut v2, 2, "tx", 9, 1, "g").unwrap();
        assert_eq!(&v1[..], &v2[..], "ProducerId bodies: v1 == v2");
        let mut v3 = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut v3, 3, "tx", 9, 1, "g").unwrap();
        assert_ne!(
            &v2[..],
            &v3[..],
            "v3 adds compact strings and tagged fields"
        );
        let mut v4 = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut v4, 4, "tx", 9, 1, "g").unwrap();
        assert_eq!(&v3[..], &v4[..], "ProducerId bodies: v3 == v4");
    }

    #[test]
    fn add_offsets_to_txn_request_producer_epoch_matches_java() {
        // Kafka 4.0.0 AddOffsetsToTxnRequest.json ProducerEpoch is versions
        // 0+ (INT16 after ProducerId / before GroupId). Official Java
        // AddOffsetsToTxnRequestData.producerEpoch. Encode already writes
        // producer_epoch. Decode previously discarded it. Kafka 4.0
        // validVersions is 0-4. This crate speaks 0–4. This is not
        // ProducerId / EndTxn response ProducerEpoch / InitProducerId /
        // TxnOffsetCommit ProducerEpoch / AddPartitionsToTxn ProducerEpoch /
        // WriteTxnMarkers ProducerEpoch.
        for version in [0_i16, 1, 2, 3, 4] {
            let mut buf = BytesMut::new();
            encode_add_offsets_to_txn_request(&mut buf, version, "tx", 9, 1, "g").unwrap();
            let mut cur = buf.as_ref();
            let (tid, gid, producer_id, producer_epoch) =
                decode_add_offsets_to_txn_request(&mut cur, version).unwrap();
            assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
            assert_eq!(producer_id, 9);
            assert_eq!(producer_epoch, 1);
            assert!(
                cur.is_empty(),
                "AddOffsetsToTxn v{version} ProducerEpoch leftover-empty"
            );
        }

        let mut one = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut one, 0, "tx", 9, 1, "g").unwrap();
        let mut two = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut two, 0, "tx", 9, 2, "g").unwrap();
        assert_ne!(
            &one[..],
            &two[..],
            "v0 ProducerEpoch is not always the same INT16"
        );
        let mut cur = one.as_ref();
        let (.., producer_epoch) = decode_add_offsets_to_txn_request(&mut cur, 0).unwrap();
        assert_eq!(producer_epoch, 1);
        assert!(
            cur.is_empty(),
            "AddOffsetsToTxn v0 ProducerEpoch leftover-empty"
        );
        let mut cur = two.as_ref();
        let (.., producer_epoch) = decode_add_offsets_to_txn_request(&mut cur, 0).unwrap();
        assert_eq!(producer_epoch, 2);
        assert_eq!(
            one.get(12..14),
            Some([0, 1].as_slice()),
            "v0 classic ProducerEpoch follows TransactionalId STRING tx and ProducerId"
        );

        let mut v1 = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut v1, 1, "tx", 9, 1, "g").unwrap();
        assert_eq!(&one[..], &v1[..], "ProducerEpoch bodies: v0 == v1");
        let mut v2 = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut v2, 2, "tx", 9, 1, "g").unwrap();
        assert_eq!(&v1[..], &v2[..], "ProducerEpoch bodies: v1 == v2");
        let mut v3 = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut v3, 3, "tx", 9, 1, "g").unwrap();
        assert_ne!(
            &v2[..],
            &v3[..],
            "v3 adds compact strings and tagged fields"
        );
        let mut v4 = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut v4, 4, "tx", 9, 1, "g").unwrap();
        assert_eq!(&v3[..], &v4[..], "ProducerEpoch bodies: v3 == v4");
    }

    #[test]
    fn add_offsets_to_txn_v3_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_add_offsets_to_txn_request(&mut req, 3, "tx", 9, 1, "g").unwrap();
        let mut cur = &req[..];
        let (tid, gid, ..) = decode_add_offsets_to_txn_request(&mut cur, 3).unwrap();
        assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
        assert!(
            cur.is_empty(),
            "AddOffsetsToTxn v3 request must consume compact tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_add_offsets_to_txn_response(&mut resp, 3, 0).unwrap();
        let mut cur = &resp[..];
        assert_eq!(
            decode_add_offsets_to_txn_response(&mut cur, 3).unwrap().0,
            0
        );
        assert!(
            cur.is_empty(),
            "AddOffsetsToTxn v3 response must consume compact tagged fields"
        );

        req.clear();
        encode_add_offsets_to_txn_request(&mut req, 4, "tx", 9, 1, "g").unwrap();
        let mut cur = &req[..];
        let (tid, gid, ..) = decode_add_offsets_to_txn_request(&mut cur, 4).unwrap();
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
