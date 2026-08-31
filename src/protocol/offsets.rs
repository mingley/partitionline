//! ListOffsets (api key 2). v1–v5 classic; v6–v10 flexible.

use std::collections::{HashMap, HashSet};
use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use super::records::RecordBatch;
use crate::error::{Error, Result};

/// Log start (earliest).
pub const EARLIEST_TIMESTAMP: i64 = -2;
/// High watermark (latest).
pub const LATEST_TIMESTAMP: i64 = -1;
/// Offset of the record with the largest timestamp (KIP-734). ListOffsets v7+.
pub const MAX_TIMESTAMP: i64 = -3;
/// Earliest offset still in local storage (KIP-405). ListOffsets v8+.
pub const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;
/// Last offset in tiered/remote storage (KIP-1005). ListOffsets v9+.
pub const LATEST_TIERED_TIMESTAMP: i64 = -5;
/// Java `ListOffsetsRequest.CONSUMER_REPLICA_ID`. ReplicaId is request-level.
pub const CONSUMER_REPLICA_ID: i32 = -1;
/// Java `ListOffsetsRequest.DEBUGGING_REPLICA_ID`.
pub const DEBUGGING_REPLICA_ID: i32 = -2;

/// Java `OffsetSpec` for [`crate::Admin::list_offsets`].
///
/// Converts to the ListOffsets Timestamp INT64:
/// [`EARLIEST_TIMESTAMP`], [`LATEST_TIMESTAMP`], [`MAX_TIMESTAMP`],
/// [`EARLIEST_LOCAL_TIMESTAMP`], [`LATEST_TIERED_TIMESTAMP`], or a
/// millisecond Unix timestamp from [`Self::for_timestamp`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OffsetSpec {
    timestamp: i64,
}

impl OffsetSpec {
    /// Java `OffsetSpec.earliest()` (`-2`).
    #[must_use]
    pub const fn earliest() -> Self {
        Self {
            timestamp: EARLIEST_TIMESTAMP,
        }
    }

    /// Java `OffsetSpec.latest()` (`-1`).
    #[must_use]
    pub const fn latest() -> Self {
        Self {
            timestamp: LATEST_TIMESTAMP,
        }
    }

    /// Java `OffsetSpec.maxTimestamp()` (`-3`, ListOffsets v7+).
    #[must_use]
    pub const fn max_timestamp() -> Self {
        Self {
            timestamp: MAX_TIMESTAMP,
        }
    }

    /// Java `OffsetSpec.earliestLocal()` (`-4`, ListOffsets v8+).
    #[must_use]
    pub const fn earliest_local() -> Self {
        Self {
            timestamp: EARLIEST_LOCAL_TIMESTAMP,
        }
    }

    /// Java `OffsetSpec.latestTiered()` (`-5`, ListOffsets v9+).
    #[must_use]
    pub const fn latest_tiered() -> Self {
        Self {
            timestamp: LATEST_TIERED_TIMESTAMP,
        }
    }

    /// Java `OffsetSpec.forTimestamp(long)`.
    #[must_use]
    pub const fn for_timestamp(timestamp: i64) -> Self {
        Self { timestamp }
    }

    /// ListOffsets Timestamp INT64.
    #[must_use]
    pub const fn timestamp(self) -> i64 {
        self.timestamp
    }
}

impl From<OffsetSpec> for i64 {
    fn from(spec: OffsetSpec) -> Self {
        spec.timestamp
    }
}

/// One partition in a ListOffsets response.
///
/// Getters and [`Display`] match Java `ListOffsetsResult.ListOffsetsResultInfo`.
/// [`Self::leader_epoch`] is `None` when the wire value is
/// [`Self::UNKNOWN_EPOCH`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListOffsetsPartition {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Matched timestamp, or `-1` when unknown.
    pub timestamp: i64,
    /// Log offset, or `-1` when unknown.
    pub offset: i64,
    /// Leader epoch (v4+). [`Self::UNKNOWN_EPOCH`] when unknown or the
    /// request version is below 4.
    pub leader_epoch: i32,
}

impl ListOffsetsPartition {
    /// Java `ListOffsetsResponse.UNKNOWN_OFFSET`.
    pub const UNKNOWN_OFFSET: i64 = -1;
    /// Java `ListOffsetsResponse.UNKNOWN_TIMESTAMP`.
    pub const UNKNOWN_TIMESTAMP: i64 = -1;
    /// Java `ListOffsetsResponse.UNKNOWN_EPOCH`.
    pub const UNKNOWN_EPOCH: i32 = RecordBatch::NO_PARTITION_LEADER_EPOCH;

    /// Successful partition body.
    #[must_use]
    pub fn ok(timestamp: i64, offset: i64, leader_epoch: i32) -> Self {
        Self {
            error_code: 0,
            timestamp,
            offset,
            leader_epoch,
        }
    }

    /// Java `ListOffsetsResult.ListOffsetsResultInfo.offset`.
    #[must_use]
    pub fn offset(self) -> i64 {
        self.offset
    }

    /// Java `ListOffsetsResult.ListOffsetsResultInfo.timestamp`.
    #[must_use]
    pub fn timestamp(self) -> i64 {
        self.timestamp
    }

    /// Java `ListOffsetsResult.ListOffsetsResultInfo.leaderEpoch`.
    ///
    /// `None` when the wire value is [`Self::UNKNOWN_EPOCH`].
    #[must_use]
    pub fn leader_epoch(self) -> Option<i32> {
        (self.leader_epoch != Self::UNKNOWN_EPOCH).then_some(self.leader_epoch)
    }
}

impl fmt::Display for ListOffsetsPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ListOffsetsResultInfo(offset={}, timestamp={}, leaderEpoch=",
            self.offset, self.timestamp
        )?;
        write_java_optional(f, self.leader_epoch())?;
        f.write_str(")")
    }
}

/// Java `Optional.toString` (`Optional[n]` / `Optional.empty`).
fn write_java_optional(f: &mut fmt::Formatter<'_>, v: Option<i32>) -> fmt::Result {
    match v {
        Some(n) => write!(f, "Optional[{n}]"),
        None => f.write_str("Optional.empty"),
    }
}

/// One partition in a ListOffsets request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsPartitionRequest {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch (v4+), or [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub current_leader_epoch: i32,
    /// Timestamp to search (`-2` earliest, `-1` latest, `-3` max
    /// timestamp, `-4` earliest local, `-5` latest tiered, or milliseconds).
    pub timestamp: i64,
}

impl ListOffsetsPartitionRequest {
    /// Partition `partition` at `timestamp` with leader epoch.
    #[must_use]
    pub fn new(partition: i32, current_leader_epoch: i32, timestamp: i64) -> Self {
        Self {
            partition,
            current_leader_epoch,
            timestamp,
        }
    }
}

/// One topic in a ListOffsets request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopicRequest {
    /// Topic name.
    pub name: String,
    /// Partitions in this topic (duplicates keep separate timestamps).
    pub partitions: Vec<ListOffsetsPartitionRequest>,
}

impl ListOffsetsTopicRequest {
    /// Topic `name` with these partition queries.
    #[must_use]
    pub fn new(name: impl Into<String>, partitions: Vec<ListOffsetsPartitionRequest>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }

    /// Java `ListOffsetsRequest.getErrorResponse` one topic.
    ///
    /// Each partition is [`ListOffsetsResponsePartition::error`]. Throttle on
    /// the response is the JSON default (`0`).
    #[must_use]
    pub fn error_result(&self, error_code: i16) -> ListOffsetsTopicResponse {
        ListOffsetsTopicResponse::new(
            self.name.as_str(),
            self.partitions
                .iter()
                .map(|p| ListOffsetsResponsePartition::error(p.partition, error_code))
                .collect(),
        )
    }
}

/// One partition in a ListOffsets response, including index.
///
/// [`Self::error`] is Java `ListOffsetsRequest.getErrorResponse` partition
/// body (`UNKNOWN_OFFSET` / `UNKNOWN_TIMESTAMP` / `UNKNOWN_EPOCH`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Matched timestamp, or `-1` when unknown.
    pub timestamp: i64,
    /// Log offset, or `-1` when unknown.
    pub offset: i64,
    /// Leader epoch (v4+). [`ListOffsetsPartition::UNKNOWN_EPOCH`] when
    /// unknown or the request version is below 4.
    pub leader_epoch: i32,
}

impl ListOffsetsResponsePartition {
    /// Partition `partition_index` with this result body.
    #[must_use]
    pub fn new(partition_index: i32, result: ListOffsetsPartition) -> Self {
        Self {
            partition_index,
            error_code: result.error_code,
            timestamp: result.timestamp,
            offset: result.offset,
            leader_epoch: result.leader_epoch,
        }
    }

    /// Java `ListOffsetsRequest.getErrorResponse` partition body.
    ///
    /// Fills [`ListOffsetsPartition::UNKNOWN_TIMESTAMP`] /
    /// [`ListOffsetsPartition::UNKNOWN_OFFSET`] /
    /// [`ListOffsetsPartition::UNKNOWN_EPOCH`] (JSON default for omitted
    /// `LeaderEpoch`).
    #[must_use]
    pub fn error(partition_index: i32, error_code: i16) -> Self {
        Self::new(
            partition_index,
            ListOffsetsPartition {
                error_code,
                timestamp: ListOffsetsPartition::UNKNOWN_TIMESTAMP,
                offset: ListOffsetsPartition::UNKNOWN_OFFSET,
                leader_epoch: ListOffsetsPartition::UNKNOWN_EPOCH,
            },
        )
    }
}

/// One topic in a ListOffsets response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopicResponse {
    /// Topic name.
    pub name: String,
    /// Partition results in request order.
    pub partitions: Vec<ListOffsetsResponsePartition>,
}

impl ListOffsetsTopicResponse {
    /// Topic `name` with these partition results.
    #[must_use]
    pub fn new(name: impl Into<String>, partitions: Vec<ListOffsetsResponsePartition>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }
}

/// Java `ListOffsetsRequest` helpers.
pub struct ListOffsetsRequest;

impl ListOffsetsRequest {
    /// Java `ListOffsetsRequest.duplicatePartitions`.
    ///
    /// `(topic, partition)` pairs that appear more than once. The first
    /// occurrence is not a duplicate (Java `Set.add`).
    #[must_use]
    pub fn duplicate_partitions(topics: &[ListOffsetsTopicRequest]) -> HashSet<(String, i32)> {
        let mut seen = HashSet::new();
        let mut duplicates = HashSet::new();
        for topic in topics {
            for partition in &topic.partitions {
                let tp = (topic.name.clone(), partition.partition);
                if !seen.insert(tp.clone()) {
                    let _inserted = duplicates.insert(tp);
                }
            }
        }
        duplicates
    }

    /// Java `ListOffsetsRequest.toListOffsetsTopics`.
    ///
    /// Groups `(topic, partition body)` by name. A later entry for the
    /// same topic appends (Java `HashMap.computeIfAbsent` then
    /// `partitions().add`). Topic order is first-seen (Java
    /// `HashMap.values` order is unspecified). The Java map key is
    /// `TopicPartition`; grouping uses only the name. The partition index
    /// on the body is kept as-is.
    #[must_use]
    pub fn to_list_offsets_topics<'a, I>(timestamps_to_search: I) -> Vec<ListOffsetsTopicRequest>
    where
        I: IntoIterator<Item = (&'a str, ListOffsetsPartitionRequest)>,
    {
        let mut order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<ListOffsetsPartitionRequest>> = HashMap::new();
        for (topic, partition) in timestamps_to_search {
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
            .filter_map(|name| {
                by_topic
                    .remove(&name)
                    .map(|partitions| ListOffsetsTopicRequest { name, partitions })
            })
            .collect()
    }

    /// Java `ListOffsetsRequest.Builder.forConsumer`.
    ///
    /// Oldest ListOffsets version a consumer builder will negotiate.
    /// Else-if first match: tiered storage (v9) wins over earliest-local
    /// (v8) over max-timestamp (v7) over `READ_COMMITTED` (v2) over
    /// timestamp (v1). All false is `0` (Java still returns `0` even
    /// though Kafka 4.0 `validVersions` is `1-10`; this crate speaks
    /// 1–10). Isolation is a `bool` (`true` is `READ_COMMITTED`) so
    /// this module does not import [`crate::IsolationLevel`]. ReplicaId
    /// is always [`CONSUMER_REPLICA_ID`]. The two-argument Java
    /// `forConsumer` is this call with the last three flags `false`.
    #[must_use]
    pub const fn for_consumer(
        require_timestamp: bool,
        read_committed: bool,
        require_max_timestamp: bool,
        require_earliest_local_timestamp: bool,
        require_tiered_storage_timestamp: bool,
    ) -> i16 {
        if require_tiered_storage_timestamp {
            9
        } else if require_earliest_local_timestamp {
            8
        } else if require_max_timestamp {
            7
        } else if read_committed {
            2
        } else if require_timestamp {
            1
        } else {
            0
        }
    }
}

/// Java `ListOffsetsResponse` helpers.
pub struct ListOffsetsResponse;

impl ListOffsetsResponse {
    /// Java `ListOffsetsResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 3
    }

    /// Java `ListOffsetsResponse.errorCounts`.
    ///
    /// Counts partition-level error codes (including `NONE`).
    #[must_use]
    pub fn error_counts(topics: &[ListOffsetsTopicResponse]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        for topic in topics {
            for partition in &topic.partitions {
                let count = counts.entry(partition.error_code).or_insert(0);
                *count += 1;
            }
        }
        counts
    }

    /// Java `ListOffsetsResponse.singletonListOffsetsTopicResponse`.
    ///
    /// Java takes `TopicPartition` plus `Errors`; this type stores the topic
    /// name and partition index as fields, so callers pass them.
    #[must_use]
    pub fn singleton_list_offsets_topic_response(
        topic: impl Into<String>,
        partition: i32,
        error_code: i16,
        timestamp: i64,
        offset: i64,
        epoch: i32,
    ) -> ListOffsetsTopicResponse {
        ListOffsetsTopicResponse::new(
            topic,
            vec![ListOffsetsResponsePartition {
                partition_index: partition,
                error_code,
                timestamp,
                offset,
                leader_epoch: epoch,
            }],
        )
    }
}

/// ListOffsets v1–v5 (classic) or v6–v10 (flexible). Isolation is v2+.
/// `current_leader_epoch` is v4+. v10 `TimeoutMs` (KIP-1075) follows Topics.
#[expect(
    clippy::too_many_arguments,
    reason = "ListOffsets body is isolation, topic, partition, epoch, timestamp, and v10 TimeoutMs"
)]
pub fn encode_list_offsets_request(
    buf: &mut BytesMut,
    version: i16,
    isolation_level: i8,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    timestamp: i64,
    timeout_ms: i32,
) -> crate::error::Result<()> {
    encode_list_offsets_topics_request(
        buf,
        version,
        isolation_level,
        &[ListOffsetsTopicRequest::new(
            topic,
            vec![ListOffsetsPartitionRequest::new(
                partition,
                current_leader_epoch,
                timestamp,
            )],
        )],
        timeout_ms,
    )
}

/// `true` when ListOffsets `version` is flexible (v6+).
///
/// v0–v5 are classic. v6–v10 are compact arrays/strings plus tagged
/// fields (Apache JSON `flexibleVersions: "6+"`). v7 is MAX_TIMESTAMP
/// (KIP-734). v8 is EARLIEST_LOCAL (KIP-405). v9 is LATEST_TIERED
/// (KIP-1005). v10 adds TimeoutMs after Topics (KIP-1075). Kafka 4.0
/// `validVersions` is `1-10`. This crate speaks 1–10. v11+ is not spoken.
fn list_offsets_flexible(version: i16) -> Result<bool> {
    match version {
        0..=5 => Ok(false),
        6..=10 => Ok(true),
        other => Err(Error::protocol(format!(
            "ListOffsets version {other} is not implemented"
        ))),
    }
}

/// Encode ListOffsets with one or more topics (v1–v5 classic, v6–v10 flexible).
/// `timeout_ms` is written at v10+ (KIP-1075); ignored below.
pub fn encode_list_offsets_topics_request(
    buf: &mut BytesMut,
    version: i16,
    isolation_level: i8,
    topics: &[ListOffsetsTopicRequest],
    timeout_ms: i32,
) -> crate::error::Result<()> {
    let flexible = list_offsets_flexible(version)?;
    buf.put_i32(CONSUMER_REPLICA_ID);
    if version >= 2 {
        buf.put_i8(isolation_level);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.name))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            if version >= 4 {
                buf.put_i32(p.current_leader_epoch);
            }
            buf.put_i64(p.timestamp);
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if version >= 10 {
        buf.put_i32(timeout_ms);
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a single-topic, single-partition ListOffsets request.
///
/// Returns `(isolation_level, topic, partition, current_leader_epoch, timestamp)`.
/// Isolation is `0` below v2. `current_leader_epoch` is
/// [`RecordBatch::NO_PARTITION_LEADER_EPOCH`] below v4.
/// Extra topics or partitions in the body are consumed and ignored.
pub fn decode_list_offsets_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i8, String, i32, i32, i64)> {
    let (isolation, topics, _timeout_ms) = decode_list_offsets_topics_request(buf, version)?;
    let t = topics
        .first()
        .ok_or_else(|| Error::protocol("empty ListOffsets topics"))?;
    let p = t
        .partitions
        .first()
        .ok_or_else(|| Error::protocol("empty ListOffsets partitions"))?;
    Ok((
        isolation,
        t.name.clone(),
        p.partition,
        p.current_leader_epoch,
        p.timestamp,
    ))
}

/// Decode ListOffsets topics (v1–v5 classic, v6–v10 flexible).
///
/// Returns `(isolation_level, topics, timeout_ms)`. Isolation is `0`
/// below v2. `timeout_ms` is `Some` at v10+ (KIP-1075) and `None` below.
pub fn decode_list_offsets_topics_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i8, Vec<ListOffsetsTopicRequest>, Option<i32>)> {
    let flexible = list_offsets_flexible(version)?;
    let _replica = buf::get_i32(buf)?;
    let isolation = if version >= 2 { buf::get_i8(buf)? } else { 0 };
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let current_leader_epoch = if version >= 4 {
                buf::get_i32(buf)?
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            };
            let timestamp = buf::get_i64(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(ListOffsetsPartitionRequest {
                partition,
                current_leader_epoch,
                timestamp,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(ListOffsetsTopicRequest { name, partitions });
    }
    let timeout_ms = if version >= 10 {
        Some(buf::get_i32(buf)?)
    } else {
        None
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((isolation, topics, timeout_ms))
}

/// Encode a single-topic, single-partition ListOffsets response.
pub fn encode_list_offsets_response(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    result: ListOffsetsPartition,
) -> crate::error::Result<()> {
    encode_list_offsets_topics_response(
        buf,
        version,
        &[ListOffsetsResponse::singleton_list_offsets_topic_response(
            topic,
            partition,
            result.error_code,
            result.timestamp,
            result.offset,
            result.leader_epoch,
        )],
    )
}

/// Encode ListOffsets with one or more topics (v1–v5 classic, v6–v10 flexible).
pub fn encode_list_offsets_topics_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[ListOffsetsTopicResponse],
) -> crate::error::Result<()> {
    let flexible = list_offsets_flexible(version)?;
    if version >= 2 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.name))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i16(p.error_code);
            buf.put_i64(p.timestamp);
            buf.put_i64(p.offset);
            if version >= 4 {
                buf.put_i32(p.leader_epoch);
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

/// Decode a single-topic, single-partition ListOffsets response.
///
/// Broker `error_code != 0` is [`Error::Broker`]. Below v4 the leader
/// epoch field is [`ListOffsetsPartition::UNKNOWN_EPOCH`]
/// ([`ListOffsetsPartition::leader_epoch`] is then `None`).
pub fn decode_list_offsets_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ListOffsetsPartition> {
    let topics = decode_list_offsets_topics_response(buf, version)?;
    let t = topics
        .first()
        .ok_or_else(|| Error::protocol("empty ListOffsets response topics"))?;
    let p = t
        .partitions
        .first()
        .ok_or_else(|| Error::protocol("empty ListOffsets response partitions"))?;
    if p.error_code != 0 {
        return Err(Error::broker(p.error_code, "ListOffsets"));
    }
    Ok(ListOffsetsPartition {
        error_code: p.error_code,
        timestamp: p.timestamp,
        offset: p.offset,
        leader_epoch: p.leader_epoch,
    })
}

/// Decode ListOffsets topics (v1–v5 classic, v6–v10 flexible). Partition errors stay on the row.
pub fn decode_list_offsets_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<ListOffsetsTopicResponse>> {
    let flexible = list_offsets_flexible(version)?;
    if version >= 2 {
        let _throttle = buf::get_i32(buf)?;
    }
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let timestamp = buf::get_i64(buf)?;
            let offset = buf::get_i64(buf)?;
            let leader_epoch = if version >= 4 {
                buf::get_i32(buf)?
            } else {
                ListOffsetsPartition::UNKNOWN_EPOCH
            };
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(ListOffsetsResponsePartition {
                partition_index,
                error_code,
                timestamp,
                offset,
                leader_epoch,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(ListOffsetsTopicResponse { name, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(topics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn offset_spec_matches_list_offsets_timestamp_constants() {
        assert_eq!(i64::from(OffsetSpec::earliest()), EARLIEST_TIMESTAMP);
        assert_eq!(i64::from(OffsetSpec::latest()), LATEST_TIMESTAMP);
        assert_eq!(i64::from(OffsetSpec::max_timestamp()), MAX_TIMESTAMP);
        assert_eq!(
            i64::from(OffsetSpec::earliest_local()),
            EARLIEST_LOCAL_TIMESTAMP
        );
        assert_eq!(
            i64::from(OffsetSpec::latest_tiered()),
            LATEST_TIERED_TIMESTAMP
        );
        assert_eq!(
            OffsetSpec::for_timestamp(1_700_000_000_000).timestamp(),
            1_700_000_000_000
        );
    }

    #[test]
    fn list_offsets_replica_id_sentinels_match_java() {
        assert_eq!(CONSUMER_REPLICA_ID, -1);
        assert_eq!(DEBUGGING_REPLICA_ID, -2);
        assert!(!ListOffsetsResponse::should_client_throttle(2));
        assert!(ListOffsetsResponse::should_client_throttle(3));
        let singleton = ListOffsetsResponse::singleton_list_offsets_topic_response(
            "t",
            3,
            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
            ListOffsetsPartition::UNKNOWN_TIMESTAMP,
            ListOffsetsPartition::UNKNOWN_OFFSET,
            ListOffsetsPartition::UNKNOWN_EPOCH,
        );
        assert_eq!(singleton.name, "t");
        let part = singleton.partitions.first().expect("one partition");
        assert_eq!(part.partition_index, 3);
        assert_eq!(part.error_code, crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(part.timestamp, ListOffsetsPartition::UNKNOWN_TIMESTAMP);
        assert_eq!(part.offset, ListOffsetsPartition::UNKNOWN_OFFSET);
        assert_eq!(part.leader_epoch, ListOffsetsPartition::UNKNOWN_EPOCH);
        assert_eq!(
            part,
            &ListOffsetsResponsePartition::error(3, crate::error::UNKNOWN_TOPIC_OR_PARTITION)
        );
        let topic = ListOffsetsTopicRequest::new(
            "t",
            vec![
                ListOffsetsPartitionRequest::new(0, 1, -1),
                ListOffsetsPartitionRequest::new(3, 4, -2),
            ],
        );
        let result = topic.error_result(crate::error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            result,
            ListOffsetsTopicResponse::new(
                "t",
                vec![
                    ListOffsetsResponsePartition::error(
                        0,
                        crate::error::UNKNOWN_TOPIC_OR_PARTITION
                    ),
                    ListOffsetsResponsePartition::error(
                        3,
                        crate::error::UNKNOWN_TOPIC_OR_PARTITION
                    ),
                ]
            )
        );
        let mut buf = BytesMut::new();
        encode_list_offsets_topics_response(&mut buf, 6, std::slice::from_ref(&result)).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_list_offsets_topics_response(&mut cur, 6).unwrap();
        assert_eq!(decoded, vec![result]);
        assert!(
            cur.is_empty(),
            "error-response leftover-empty; leftover {} bytes",
            cur.len()
        );
        let ok = ListOffsetsResponse::singleton_list_offsets_topic_response(
            "events",
            1,
            0,
            1_700_000_000_000,
            44,
            7,
        );
        assert_eq!(
            ok,
            ListOffsetsTopicResponse::new(
                "events",
                vec![ListOffsetsResponsePartition::new(
                    1,
                    ListOffsetsPartition::ok(1_700_000_000_000, 44, 7)
                )]
            )
        );
    }

    #[test]
    fn list_offsets_response_error_counts_matches_java() {
        assert!(ListOffsetsResponse::error_counts(&[]).is_empty());
        let counts = ListOffsetsResponse::error_counts(&[
            ListOffsetsTopicResponse::new(
                "ok",
                vec![
                    ListOffsetsResponsePartition::error(0, 0),
                    ListOffsetsResponsePartition::error(1, crate::error::NOT_LEADER_OR_FOLLOWER),
                ],
            ),
            ListOffsetsTopicResponse::new(
                "missing",
                vec![ListOffsetsResponsePartition::error(
                    0,
                    crate::error::UNKNOWN_TOPIC_OR_PARTITION,
                )],
            ),
            ListOffsetsTopicResponse::new("ok2", vec![ListOffsetsResponsePartition::error(0, 0)]),
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
    fn list_offsets_partition_matches_java_list_offsets_result_info() {
        let with_epoch = ListOffsetsPartition::ok(1_700_000_000_000, 44, 3);
        assert_eq!(with_epoch.offset(), 44);
        assert_eq!(with_epoch.timestamp(), 1_700_000_000_000);
        assert_eq!(with_epoch.leader_epoch(), Some(3));
        assert_eq!(
            with_epoch.to_string(),
            "ListOffsetsResultInfo(offset=44, timestamp=1700000000000, leaderEpoch=Optional[3])"
        );

        let epoch_zero = ListOffsetsPartition::ok(1, 2, 0);
        assert_eq!(epoch_zero.leader_epoch(), Some(0));
        assert_eq!(
            epoch_zero.to_string(),
            "ListOffsetsResultInfo(offset=2, timestamp=1, leaderEpoch=Optional[0])"
        );

        let unknown = ListOffsetsPartition::ok(
            ListOffsetsPartition::UNKNOWN_TIMESTAMP,
            ListOffsetsPartition::UNKNOWN_OFFSET,
            ListOffsetsPartition::UNKNOWN_EPOCH,
        );
        assert_eq!(ListOffsetsPartition::UNKNOWN_EPOCH, -1);
        assert_eq!(
            ListOffsetsPartition::UNKNOWN_EPOCH,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(unknown.offset(), ListOffsetsPartition::UNKNOWN_OFFSET);
        assert_eq!(unknown.timestamp(), ListOffsetsPartition::UNKNOWN_TIMESTAMP);
        assert_eq!(unknown.leader_epoch(), None);
        assert_eq!(
            unknown.to_string(),
            "ListOffsetsResultInfo(offset=-1, timestamp=-1, leaderEpoch=Optional.empty)"
        );
    }

    #[test]
    fn list_offsets_v2_roundtrip() {
        let mut req = BytesMut::new();
        encode_list_offsets_request(&mut req, 2, 1, "t", 3, 9, EARLIEST_TIMESTAMP, 0).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 2).unwrap();
        assert_eq!(
            (iso, topic.as_str(), part, epoch, ts),
            (1, "t", 3, RecordBatch::NO_PARTITION_LEADER_EPOCH, -2)
        );
        assert!(
            cur.is_empty(),
            "v2 request has no current_leader_epoch; leftover {} bytes",
            cur.len()
        );
        let mut resp = BytesMut::new();
        encode_list_offsets_response(&mut resp, 2, "t", 3, ListOffsetsPartition::ok(-1, 7, 4))
            .unwrap();
        let mut cur = &resp[..];
        let got = decode_list_offsets_response(&mut cur, 2).unwrap();
        assert_eq!(got, ListOffsetsPartition::ok(-1, 7, -1));
        assert!(cur.is_empty(), "v2 response leftover {} bytes", cur.len());
    }

    #[test]
    fn list_offsets_v4_sends_current_leader_epoch_and_consumes_response_epoch() {
        let mut req = BytesMut::new();
        encode_list_offsets_request(&mut req, 4, 1, "t", 0, 7, LATEST_TIMESTAMP, 0).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 4).unwrap();
        assert_eq!((iso, topic.as_str(), part, epoch, ts), (1, "t", 0, 7, -1));
        assert!(
            cur.is_empty(),
            "v4 request must place current_leader_epoch before timestamp; leftover {} bytes",
            cur.len()
        );

        let mut resp = BytesMut::new();
        encode_list_offsets_response(&mut resp, 4, "t", 0, ListOffsetsPartition::ok(-1, 12, 3))
            .unwrap();
        let mut cur = &resp[..];
        let got = decode_list_offsets_response(&mut cur, 4).unwrap();
        assert_eq!(got, ListOffsetsPartition::ok(-1, 12, 3));
        assert!(
            cur.is_empty(),
            "v4 decoder must consume leader_epoch after offset; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn list_offsets_v5_matches_v4_layout() {
        let mut req = BytesMut::new();
        encode_list_offsets_request(&mut req, 5, 0, "orders", 2, 3, 1_700_000_000_000, 0).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 5).unwrap();
        assert_eq!(
            (iso, topic.as_str(), part, epoch, ts),
            (0, "orders", 2, 3, 1_700_000_000_000)
        );
        assert!(cur.is_empty());

        let mut resp = BytesMut::new();
        encode_list_offsets_response(
            &mut resp,
            5,
            "orders",
            2,
            ListOffsetsPartition::ok(1_700_000_000_000, 44, 3),
        )
        .unwrap();
        let mut cur = &resp[..];
        let got = decode_list_offsets_response(&mut cur, 5).unwrap();
        assert_eq!(got, ListOffsetsPartition::ok(1_700_000_000_000, 44, 3));
        assert!(cur.is_empty());
    }

    #[test]
    fn list_offsets_v4_two_partitions_roundtrip_is_leftover_empty() {
        let req_topics = [ListOffsetsTopicRequest::new(
            "t",
            vec![
                ListOffsetsPartitionRequest::new(0, 1, EARLIEST_TIMESTAMP),
                ListOffsetsPartitionRequest::new(1, 1, LATEST_TIMESTAMP),
            ],
        )];
        let mut req = BytesMut::new();
        encode_list_offsets_topics_request(&mut req, 4, 0, &req_topics, 0).unwrap();
        let mut cur = &req[..];
        let (iso, got, timeout) = decode_list_offsets_topics_request(&mut cur, 4).unwrap();
        assert_eq!(iso, 0);
        assert_eq!(got, req_topics);
        assert_eq!(timeout, None);
        assert!(
            cur.is_empty(),
            "v4 multi request leftover {} bytes",
            cur.len()
        );

        let resp_topics = [ListOffsetsTopicResponse::new(
            "t",
            vec![
                ListOffsetsResponsePartition::new(0, ListOffsetsPartition::ok(-2, 0, 1)),
                ListOffsetsResponsePartition::new(1, ListOffsetsPartition::ok(-1, 4, 1)),
            ],
        )];
        let mut resp = BytesMut::new();
        encode_list_offsets_topics_response(&mut resp, 4, &resp_topics).unwrap();
        let mut cur = &resp[..];
        let got = decode_list_offsets_topics_response(&mut cur, 4).unwrap();
        assert_eq!(got, resp_topics);
        assert!(
            cur.is_empty(),
            "v4 multi response leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn list_offsets_v6_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_list_offsets_request(&mut req, 6, 1, "t", 0, 7, LATEST_TIMESTAMP, 0).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 6).unwrap();
        assert_eq!((iso, topic.as_str(), part, epoch, ts), (1, "t", 0, 7, -1));
        assert!(
            cur.is_empty(),
            "ListOffsets v6 request must consume compact tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_list_offsets_response(&mut resp, 6, "t", 0, ListOffsetsPartition::ok(-1, 12, 3))
            .unwrap();
        let mut cur = &resp[..];
        let got = decode_list_offsets_response(&mut cur, 6).unwrap();
        assert_eq!(got, ListOffsetsPartition::ok(-1, 12, 3));
        assert!(
            cur.is_empty(),
            "ListOffsets v6 response must consume compact tagged fields"
        );
        req.clear();
        encode_list_offsets_request(&mut req, 9, 1, "t", 0, 7, MAX_TIMESTAMP, 0).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 9).unwrap();
        assert_eq!(
            (iso, topic.as_str(), part, epoch, ts),
            (1, "t", 0, 7, MAX_TIMESTAMP)
        );
        assert!(cur.is_empty(), "ListOffsets v9 shares the v6 layout");
        req.clear();
        encode_list_offsets_request(&mut req, 10, 0, "t", 0, 0, LATEST_TIMESTAMP, 1500).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 10).unwrap();
        assert_eq!((iso, topic.as_str(), part, epoch, ts), (0, "t", 0, 0, -1));
        assert!(
            cur.is_empty(),
            "ListOffsets v10 request must consume TimeoutMs before tagged fields"
        );
        let mut cur = &req[..];
        let (_, _, timeout) = decode_list_offsets_topics_request(&mut cur, 10).unwrap();
        assert_eq!(timeout, Some(1500));
        req.clear();
        assert!(
            encode_list_offsets_request(&mut req, 11, 0, "t", 0, 0, LATEST_TIMESTAMP, 0).is_err(),
            "ListOffsets v11+ is not spoken"
        );
    }

    #[test]
    fn list_offsets_v6_latest_matches_compact_layout() {
        // ReplicaId INT32 -1, IsolationLevel 0, compact Topics {Name
        // "t", compact Partitions {0, epoch 0, timestamp -1, tagged},
        // tagged}, tagged.
        const REQ: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_list_offsets_request(&mut buf, 6, 0, "t", 0, 0, LATEST_TIMESTAMP, 0).unwrap();
        assert_eq!(&buf[..], REQ);
        buf.clear();
        encode_list_offsets_request(&mut buf, 9, 0, "t", 0, 0, LATEST_TIMESTAMP, 0).unwrap();
        assert_eq!(&buf[..], REQ, "ListOffsets v9 request shares the v6 layout");
        buf.clear();
        encode_list_offsets_request(&mut buf, 10, 0, "t", 0, 0, LATEST_TIMESTAMP, 1500).unwrap();
        // v9 compact plus TimeoutMs 1500 (INT32) before top-level tagged fields.
        const REQ_V10: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
            0x00, 0x05, 0xdc, 0x00,
        ];
        assert_eq!(&buf[..], REQ_V10);
        let mut cur = &buf[..];
        let _ = decode_list_offsets_request(&mut cur, 10).unwrap();
        assert!(
            cur.is_empty(),
            "ListOffsets v10 compact must be leftover-empty"
        );
        buf.clear();
        encode_list_offsets_response(&mut buf, 10, "t", 0, ListOffsetsPartition::ok(-1, 12, 3))
            .unwrap();
        let mut cur = &buf[..];
        let got = decode_list_offsets_response(&mut cur, 10).unwrap();
        assert_eq!(got, ListOffsetsPartition::ok(-1, 12, 3));
        assert!(
            cur.is_empty(),
            "ListOffsets v10 response shares the v6 layout"
        );
    }

    #[test]
    fn list_offsets_duplicate_partitions_matches_java() {
        // Java ListOffsetsRequest.duplicatePartitions: (topic, partition)
        // pairs that appear more than once. The first occurrence is not a
        // duplicate (Set.add).
        assert!(ListOffsetsRequest::duplicate_partitions(&[]).is_empty());
        let unique = [ListOffsetsTopicRequest::new(
            "t",
            vec![
                ListOffsetsPartitionRequest::new(0, RecordBatch::NO_PARTITION_LEADER_EPOCH, -1),
                ListOffsetsPartitionRequest::new(3, RecordBatch::NO_PARTITION_LEADER_EPOCH, -2),
            ],
        )];
        assert!(ListOffsetsRequest::duplicate_partitions(&unique).is_empty());
        let two = [
            ListOffsetsTopicRequest::new(
                "a",
                vec![ListOffsetsPartitionRequest::new(
                    0,
                    RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    -1,
                )],
            ),
            ListOffsetsTopicRequest::new(
                "a",
                vec![
                    ListOffsetsPartitionRequest::new(0, RecordBatch::NO_PARTITION_LEADER_EPOCH, -2),
                    ListOffsetsPartitionRequest::new(1, RecordBatch::NO_PARTITION_LEADER_EPOCH, -1),
                ],
            ),
        ];
        assert_eq!(
            ListOffsetsRequest::duplicate_partitions(&two),
            HashSet::from([("a".into(), 0)])
        );
        let mut buf = BytesMut::new();
        encode_list_offsets_topics_request(&mut buf, 1, 0, &two, 0).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_list_offsets_topics_request(&mut cur, 1).unwrap().1;
        assert_eq!(decoded, two);
        assert_eq!(
            ListOffsetsRequest::duplicate_partitions(&decoded),
            ListOffsetsRequest::duplicate_partitions(&two)
        );
        assert!(
            cur.is_empty(),
            "ListOffsets v1 duplicatePartitions leftover-empty; leftover {} bytes",
            cur.len()
        );
        buf.clear();
        encode_list_offsets_topics_request(&mut buf, 6, 0, &two, 0).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_list_offsets_topics_request(&mut cur, 6).unwrap().1;
        assert_eq!(decoded, two);
        assert_eq!(
            ListOffsetsRequest::duplicate_partitions(&decoded),
            ListOffsetsRequest::duplicate_partitions(&two)
        );
        assert!(
            cur.is_empty(),
            "ListOffsets v6 duplicatePartitions leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn list_offsets_to_list_offsets_topics_matches_java() {
        // Java ListOffsetsRequest.toListOffsetsTopics: HashMap.computeIfAbsent
        // by topic name, then partitions().add. Empty map is empty. A later
        // entry for the same name appends even when another topic sits
        // between (unlike Fetch toMessage consecutive matchingTopic).
        assert!(
            ListOffsetsRequest::to_list_offsets_topics(std::iter::empty::<(
                &str,
                ListOffsetsPartitionRequest
            )>())
            .is_empty()
        );
        let epoch = RecordBatch::NO_PARTITION_LEADER_EPOCH;
        let a0 = ListOffsetsPartitionRequest::new(0, epoch, -1);
        let a1 = ListOffsetsPartitionRequest::new(1, epoch, -2);
        let b0 = ListOffsetsPartitionRequest::new(0, epoch, -1);
        let grouped = ListOffsetsRequest::to_list_offsets_topics([
            ("a", a0.clone()),
            ("b", b0.clone()),
            ("a", a1.clone()),
        ]);
        assert_eq!(
            grouped,
            vec![
                ListOffsetsTopicRequest::new("a", vec![a0, a1]),
                ListOffsetsTopicRequest::new("b", vec![b0]),
            ]
        );
        let mut buf = BytesMut::new();
        encode_list_offsets_topics_request(&mut buf, 1, 0, &grouped, 0).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_list_offsets_topics_request(&mut cur, 1).unwrap().1;
        assert_eq!(decoded, grouped);
        assert_eq!(
            ListOffsetsRequest::to_list_offsets_topics([
                ("a", ListOffsetsPartitionRequest::new(0, epoch, -1)),
                ("b", ListOffsetsPartitionRequest::new(0, epoch, -1)),
                ("a", ListOffsetsPartitionRequest::new(1, epoch, -2)),
            ]),
            decoded
        );
        assert!(
            cur.is_empty(),
            "ListOffsets v1 toListOffsetsTopics leftover-empty; leftover {} bytes",
            cur.len()
        );
        let epoch4 = 4;
        let with_epoch = ListOffsetsRequest::to_list_offsets_topics([
            ("t", ListOffsetsPartitionRequest::new(3, epoch4, -3)),
            ("t", ListOffsetsPartitionRequest::new(5, epoch4, -1)),
        ]);
        buf.clear();
        encode_list_offsets_topics_request(&mut buf, 6, 0, &with_epoch, 0).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_list_offsets_topics_request(&mut cur, 6).unwrap().1;
        assert_eq!(decoded, with_epoch);
        let first = decoded.first().expect("one topic");
        let part = first.partitions.first().expect("one partition");
        assert_eq!(part.current_leader_epoch, epoch4);
        assert!(
            cur.is_empty(),
            "ListOffsets v6 toListOffsetsTopics leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn list_offsets_request_for_consumer_matches_java() {
        // Java ListOffsetsRequest.Builder.forConsumer: else-if first
        // match among flags. All false is 0 even though Kafka 4.0
        // validVersions is 1-10. Isolation is independent of min
        // version (READ_COMMITTED still writes isolation 1 when a
        // higher flag wins). ReplicaId is CONSUMER_REPLICA_ID.
        assert_eq!(
            ListOffsetsRequest::for_consumer(false, false, false, false, false),
            0
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(true, false, false, false, false),
            1
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(true, true, false, false, false),
            2,
            "READ_COMMITTED wins over timestamp"
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(false, true, false, false, false),
            2
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(true, true, true, false, false),
            7,
            "max-timestamp wins over READ_COMMITTED"
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(false, false, true, false, false),
            7
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(true, true, true, true, false),
            8,
            "earliest-local wins over max-timestamp"
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(false, false, false, true, false),
            8
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(true, true, true, true, true),
            9,
            "tiered wins over earliest-local"
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(false, false, false, false, true),
            9
        );
        assert_eq!(
            ListOffsetsRequest::for_consumer(false, false, false, true, true),
            9
        );

        let epoch = RecordBatch::NO_PARTITION_LEADER_EPOCH;
        let topics = [ListOffsetsTopicRequest::new(
            "t",
            vec![ListOffsetsPartitionRequest::new(
                0,
                epoch,
                EARLIEST_TIMESTAMP,
            )],
        )];

        // min 0 is not spoken; leftover-empty at v1. Isolation is
        // omitted below v2 (decode fills 0).
        leftover_for_consumer(1, 0, &topics);
        leftover_for_consumer(1, 0, &[]);
        leftover_for_consumer(2, 1, &topics);
        leftover_for_consumer(2, 1, &[]);
        leftover_for_consumer(7, 0, &topics);
        leftover_for_consumer(7, 0, &[]);
        leftover_for_consumer(8, 0, &topics);
        leftover_for_consumer(8, 0, &[]);
        leftover_for_consumer(9, 1, &topics);
        leftover_for_consumer(9, 1, &[]);
    }

    fn leftover_for_consumer(version: i16, isolation: i8, topics: &[ListOffsetsTopicRequest]) {
        let mut buf = BytesMut::new();
        encode_list_offsets_topics_request(&mut buf, version, isolation, topics, 0).unwrap();
        let mut cur = buf.as_ref();
        let (decoded_isolation, decoded, timeout) =
            decode_list_offsets_topics_request(&mut cur, version).unwrap();
        if version >= 2 {
            assert_eq!(decoded_isolation, isolation);
        } else {
            assert_eq!(decoded_isolation, 0);
        }
        assert_eq!(decoded.as_slice(), topics);
        assert!(timeout.is_none(), "forConsumer does not set TimeoutMs");
        let empty = if topics.is_empty() { "empty " } else { "" };
        assert!(
            cur.is_empty(),
            "ListOffsets v{version} Builder.forConsumer {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }
}
