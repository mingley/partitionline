//! ListOffsets (api key 2). v1–v5 classic; v6 flexible.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Log start (earliest).
pub const EARLIEST_TIMESTAMP: i64 = -2;
/// High watermark (latest).
pub const LATEST_TIMESTAMP: i64 = -1;

/// One partition in a ListOffsets response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListOffsetsPartition {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Matched timestamp, or `-1` when unknown.
    pub timestamp: i64,
    /// Log offset, or `-1` when unknown.
    pub offset: i64,
    /// Leader epoch (v4+). `-1` when unknown or the request version is below 4.
    pub leader_epoch: i32,
}

impl ListOffsetsPartition {
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
}

/// One partition in a ListOffsets request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsPartitionRequest {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch (v4+), or `-1`.
    pub current_leader_epoch: i32,
    /// Timestamp to search (`-2` earliest, `-1` latest, or milliseconds).
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
}

/// One partition in a ListOffsets response, including index.
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
    /// Leader epoch (v4+). `-1` when unknown or the request version is below 4.
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

/// ListOffsets v1–v5 (classic) or v6 (flexible). Isolation is v2+.
/// `current_leader_epoch` is v4+. v7+ (max timestamp, KIP-734) is not spoken.
pub fn encode_list_offsets_request(
    buf: &mut BytesMut,
    version: i16,
    isolation_level: i8,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    timestamp: i64,
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
    )
}

/// `true` when ListOffsets `version` is flexible (v6).
///
/// v0–v5 are classic. v6 is compact arrays/strings plus tagged fields
/// (Apache JSON `flexibleVersions: "6+"`). Kafka 4.0 removed v0.
/// v7+ timestamp sentinels and v10 `TimeoutMs` are not spoken.
fn list_offsets_flexible(version: i16) -> Result<bool> {
    match version {
        0..=5 => Ok(false),
        6 => Ok(true),
        other => Err(Error::protocol(format!(
            "ListOffsets version {other} is not implemented"
        ))),
    }
}

/// Encode ListOffsets with one or more topics (v1–v5 classic, v6 flexible).
pub fn encode_list_offsets_topics_request(
    buf: &mut BytesMut,
    version: i16,
    isolation_level: i8,
    topics: &[ListOffsetsTopicRequest],
) -> crate::error::Result<()> {
    let flexible = list_offsets_flexible(version)?;
    buf.put_i32(-1); // replica_id
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
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a single-topic, single-partition ListOffsets request.
///
/// Returns `(isolation_level, topic, partition, current_leader_epoch, timestamp)`.
/// Isolation is `0` below v2. `current_leader_epoch` is `-1` below v4.
/// Extra topics or partitions in the body are consumed and ignored.
pub fn decode_list_offsets_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i8, String, i32, i32, i64)> {
    let (isolation, topics) = decode_list_offsets_topics_request(buf, version)?;
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

/// Decode ListOffsets topics (v1–v5 classic, v6 flexible).
pub fn decode_list_offsets_topics_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i8, Vec<ListOffsetsTopicRequest>)> {
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
            let current_leader_epoch = if version >= 4 { buf::get_i32(buf)? } else { -1 };
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
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((isolation, topics))
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
        &[ListOffsetsTopicResponse::new(
            topic,
            vec![ListOffsetsResponsePartition::new(partition, result)],
        )],
    )
}

/// Encode ListOffsets with one or more topics (v1–v5 classic, v6 flexible).
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
/// Broker `error_code != 0` is [`Error::Broker`]. [`ListOffsetsPartition::leader_epoch`]
/// is `-1` below v4.
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

/// Decode ListOffsets topics (v1–v5 classic, v6 flexible). Partition errors stay on the row.
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
            let leader_epoch = if version >= 4 { buf::get_i32(buf)? } else { -1 };
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

    #[test]
    fn list_offsets_v2_roundtrip() {
        let mut req = BytesMut::new();
        encode_list_offsets_request(&mut req, 2, 1, "t", 3, 9, EARLIEST_TIMESTAMP).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 2).unwrap();
        assert_eq!((iso, topic.as_str(), part, epoch, ts), (1, "t", 3, -1, -2));
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
        encode_list_offsets_request(&mut req, 4, 1, "t", 0, 7, LATEST_TIMESTAMP).unwrap();
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
        encode_list_offsets_request(&mut req, 5, 0, "orders", 2, 3, 1_700_000_000_000).unwrap();
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
        encode_list_offsets_topics_request(&mut req, 4, 0, &req_topics).unwrap();
        let mut cur = &req[..];
        let (iso, got) = decode_list_offsets_topics_request(&mut cur, 4).unwrap();
        assert_eq!(iso, 0);
        assert_eq!(got, req_topics);
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
        encode_list_offsets_request(&mut req, 6, 1, "t", 0, 7, LATEST_TIMESTAMP).unwrap();
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
        assert!(
            encode_list_offsets_request(&mut req, 7, 0, "t", 0, 0, LATEST_TIMESTAMP).is_err(),
            "ListOffsets v7+ is not spoken"
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
        encode_list_offsets_request(&mut buf, 6, 0, "t", 0, 0, LATEST_TIMESTAMP).unwrap();
        assert_eq!(&buf[..], REQ);
    }
}
