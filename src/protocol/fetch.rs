//! Fetch (api key 1). v4–v11 classic; v12–v17 flexible.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use super::records::{self, RecordBatch};
use crate::error::{Error, Result};

/// One partition in a Fetch request.
#[derive(Debug, Clone)]
pub struct FetchPartition {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch from Metadata, or `-1`.
    pub current_leader_epoch: i32,
    /// Next offset to fetch.
    pub fetch_offset: i64,
    /// Epoch of the last fetched record (v12+), or `-1`.
    pub last_fetched_epoch: i32,
    /// Max bytes for this partition.
    pub partition_max_bytes: i32,
}

/// One topic in a Fetch request.
#[derive(Debug, Clone)]
pub struct FetchTopic {
    /// Topic name (v4–v12). Empty at v13+ (topic id on the wire).
    pub topic: String,
    /// Topic id (v13+). Zeros when the request uses a name.
    pub topic_id: [u8; 16],
    /// Partitions to fetch.
    pub partitions: Vec<FetchPartition>,
}

/// One partition in a Fetch response.
#[derive(Debug, Clone)]
pub struct FetchedPartition {
    /// Partition index.
    pub partition: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// High watermark.
    pub high_watermark: i64,
    /// Last stable offset (transactions).
    pub last_stable_offset: i64,
    /// Log start offset.
    pub log_start_offset: i64,
    /// `(producer_id, first_offset)` for aborted transactions (Fetch isolation=1).
    pub aborted_transactions: Vec<(i64, i64)>,
    /// Broker id to fetch from next, or `-1`.
    pub preferred_read_replica: i32,
    /// Fetch v12+ CurrentLeader `LeaderId` (tagged field 1), or `-1`.
    pub current_leader_id: i32,
    /// Fetch v12+ CurrentLeader `LeaderEpoch` (tagged field 1), or `-1`.
    pub current_leader_epoch: i32,
    /// Fetch v12+ DivergingEpoch `Epoch` (tagged field 0), or `-1`.
    pub diverging_epoch: i32,
    /// Fetch v12+ DivergingEpoch `EndOffset` (tagged field 0), or `-1`.
    pub diverging_end_offset: i64,
    /// Record batches for this partition.
    pub records: Vec<RecordBatch>,
}

/// One topic in a Fetch response.
#[derive(Debug, Clone)]
pub struct FetchedTopic {
    /// Topic name (v4–v12). Empty at v13+ (topic id on the wire).
    pub topic: String,
    /// Topic id (v13+). Zeros when the response uses a name.
    pub topic_id: [u8; 16],
    /// Partition bodies.
    pub partitions: Vec<FetchedPartition>,
}

/// Fetch v4–v11 (classic) or v12–v17 (flexible). LastFetchedEpoch is v12+.
#[expect(
    clippy::too_many_arguments,
    reason = "Fetch request body needs version, wait/min/max bytes, isolation, topics, and rack together"
)]
pub fn encode_fetch_request(
    buf: &mut BytesMut,
    version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
) -> crate::error::Result<()> {
    let flexible = fetch_flexible(version)?;
    // ReplicaId is untagged only through v14. v15+ uses ReplicaState tagged
    // field 1 (KIP-903). Consumers omit it (ReplicaId / ReplicaEpoch default
    // -1 / -1).
    if version <= 14 {
        buf.put_i32(-1); // replica_id
    }
    buf.put_i32(max_wait_ms);
    buf.put_i32(min_bytes);
    buf.put_i32(max_bytes);
    buf.put_i8(isolation_level);
    buf.put_i32(0); // session_id
    buf.put_i32(-1); // session_epoch
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        put_fetch_topic_identity(buf, version, flexible, &t.topic, &t.topic_id)?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i32(p.current_leader_epoch);
            buf.put_i64(p.fetch_offset);
            if version >= 12 {
                buf.put_i32(p.last_fetched_epoch);
            }
            buf.put_i64(-1); // log_start_offset
            buf.put_i32(p.partition_max_bytes);
            if flexible {
                // v17+ ReplicaDirectoryId is partition tagged field 0.
                // Consumers omit it (empty tagged fields).
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf::put_array_len(buf, flexible, Some(0))?; // forgotten
                                                 // Fetch v11 RackId is STRING, not nullable (Apache JSON / kafka-protocol
                                                 // 0.18.0). Kafka 3.9.1 rejects a null rackId. v12 is compact STRING.
    buf::put_string(buf, flexible, Some(rack_id.unwrap_or("")))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// `true` when Fetch `version` is flexible (v12+).
///
/// v4–v11 are classic. v12–v15 are compact arrays/strings/bytes plus tagged
/// fields (Apache JSON `flexibleVersions: "12+"`). Kafka 4.0 removed
/// v0–v3. v13 replaces topic names with topic ids (KIP-516). v14 is the
/// same layout as v13 (`OffsetMovedToTieredStorageException`). v15 drops
/// untagged ReplicaId and adds ReplicaState tagged field 1 (KIP-903;
/// consumers omit it). v16 is the same request as v15 (KIP-951). v17 is
/// the same consumer request as v16 (ReplicaDirectoryId tagged field 0 is
/// follower-only and omitted). This crate speaks 4–17. Partition
/// CurrentLeader tagged field 1 and DivergingEpoch tagged field 0 are
/// decoded (v12+); top-level NodeEndpoints tagged field 0 is not applied.
/// v18+ (KIP-1166 HighWatermark) is not spoken.
fn fetch_flexible(version: i16) -> Result<bool> {
    match version {
        4..=11 => Ok(false),
        12..=17 => Ok(true),
        other => Err(Error::protocol(format!(
            "Fetch version {other} is not implemented"
        ))),
    }
}

fn put_fetch_topic_identity(
    buf: &mut BytesMut,
    version: i16,
    flexible: bool,
    name: &str,
    topic_id: &[u8; 16],
) -> Result<()> {
    if version >= 13 {
        buf.extend_from_slice(topic_id);
        Ok(())
    } else {
        buf::put_string(buf, flexible, Some(name))
    }
}

fn get_fetch_topic_identity<B: Buf>(
    buf: &mut B,
    version: i16,
    flexible: bool,
) -> Result<(String, [u8; 16])> {
    if version >= 13 {
        Ok((String::new(), buf::get_uuid(buf)?))
    } else {
        Ok((
            buf::get_string(buf, flexible)?.unwrap_or_default(),
            [0u8; 16],
        ))
    }
}

/// EpochEndOffset inside Fetch partition tagged field 0 (13 bytes when
/// present: INT32 + INT64 + empty nested tagged fields).
fn encode_diverging_epoch(epoch: i32, end_offset: i64) -> Bytes {
    let mut inner = BytesMut::new();
    inner.put_i32(epoch);
    inner.put_i64(end_offset);
    buf::put_empty_tagged_fields(&mut inner);
    inner.freeze()
}

fn decode_diverging_epoch(value: &Bytes) -> Result<(i32, i64)> {
    let mut cur = value.as_ref();
    let epoch = buf::get_i32(&mut cur)?;
    let end_offset = buf::get_i64(&mut cur)?;
    buf::skip_tagged_fields(&mut cur)?;
    if !cur.is_empty() {
        return Err(Error::protocol("DivergingEpoch leftover bytes"));
    }
    Ok((epoch, end_offset))
}

/// LeaderIdAndEpoch inside Fetch partition tagged field 1 (9 bytes when
/// present: INT32 + INT32 + empty nested tagged fields).
fn encode_current_leader(leader_id: i32, leader_epoch: i32) -> Bytes {
    let mut inner = BytesMut::new();
    inner.put_i32(leader_id);
    inner.put_i32(leader_epoch);
    buf::put_empty_tagged_fields(&mut inner);
    inner.freeze()
}

fn decode_current_leader(value: &Bytes) -> Result<(i32, i32)> {
    let mut cur = value.as_ref();
    let leader_id = buf::get_i32(&mut cur)?;
    let leader_epoch = buf::get_i32(&mut cur)?;
    buf::skip_tagged_fields(&mut cur)?;
    if !cur.is_empty() {
        return Err(Error::protocol("CurrentLeader leftover bytes"));
    }
    Ok((leader_id, leader_epoch))
}

fn encode_fetch_partition_tags(
    buf: &mut BytesMut,
    diverging_epoch: i32,
    diverging_end_offset: i64,
    current_leader_id: i32,
    current_leader_epoch: i32,
) -> Result<()> {
    let mut fields: Vec<(u32, Bytes)> = Vec::new();
    if diverging_epoch >= 0 {
        fields.push((
            0,
            encode_diverging_epoch(diverging_epoch, diverging_end_offset),
        ));
    }
    if current_leader_id >= 0 {
        fields.push((
            1,
            encode_current_leader(current_leader_id, current_leader_epoch),
        ));
    }
    if fields.is_empty() {
        buf::put_empty_tagged_fields(buf);
        Ok(())
    } else {
        buf::put_tagged_fields(buf, &fields)
    }
}

fn decode_fetch_partition_tags<B: Buf>(buf: &mut B) -> Result<(i32, i64, i32, i32)> {
    let tags = buf::get_tagged_fields(buf)?;
    let mut diverging_epoch = -1;
    let mut diverging_end_offset = -1;
    let mut current_leader_id = -1;
    let mut current_leader_epoch = -1;
    for (tag, value) in tags {
        match tag {
            0 => (diverging_epoch, diverging_end_offset) = decode_diverging_epoch(&value)?,
            1 => (current_leader_id, current_leader_epoch) = decode_current_leader(&value)?,
            _ => {}
        }
    }
    Ok((
        diverging_epoch,
        diverging_end_offset,
        current_leader_id,
        current_leader_epoch,
    ))
}

/// Decode Fetch: `(isolation_level, max_bytes, topics, rack_id)`.
///
/// `last_fetched_epoch` is `-1` below v12.
pub fn decode_fetch_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i8, i32, Vec<FetchTopic>, String)> {
    let flexible = fetch_flexible(version)?;
    if version <= 14 {
        let _replica = buf::get_i32(buf)?;
    }
    let _max_wait = buf::get_i32(buf)?;
    let _min_bytes = buf::get_i32(buf)?;
    let max_bytes = buf::get_i32(buf)?;
    let isolation = buf::get_i8(buf)?;
    let _session_id = buf::get_i32(buf)?;
    let _session_epoch = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let (topic, topic_id) = get_fetch_topic_identity(buf, version, flexible)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let current_leader_epoch = buf::get_i32(buf)?;
            let fetch_offset = buf::get_i64(buf)?;
            let last_fetched_epoch = if version >= 12 {
                buf::get_i32(buf)?
            } else {
                -1
            };
            let _log_start = buf::get_i64(buf)?;
            let partition_max_bytes = buf::get_i32(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(FetchPartition {
                partition,
                current_leader_epoch,
                fetch_offset,
                last_fetched_epoch,
                partition_max_bytes,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(FetchTopic {
            topic,
            topic_id,
            partitions,
        });
    }
    let forgotten = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    for _ in 0..forgotten {
        if version >= 13 {
            let _id = buf::get_uuid(buf)?;
        } else {
            let _t = buf::get_string(buf, flexible)?;
        }
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        for _ in 0..pn {
            let _p = buf::get_i32(buf)?;
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    let rack = buf::get_string(buf, flexible)?.unwrap_or_default();
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((isolation, max_bytes, topics, rack))
}

/// Encode a Fetch v4–v11 (classic) or v12–v17 (flexible) response.
pub fn encode_fetch_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[FetchedTopic],
) -> Result<()> {
    let flexible = fetch_flexible(version)?;
    buf.put_i32(0); // throttle
    buf.put_i16(0); // top-level error
    buf.put_i32(0); // session_id
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        put_fetch_topic_identity(buf, version, flexible, &t.topic, &t.topic_id)?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf.put_i64(p.high_watermark);
            buf.put_i64(p.last_stable_offset);
            buf.put_i64(p.log_start_offset);
            buf::put_array_len(buf, flexible, Some(p.aborted_transactions.len()))?;
            for (pid, first) in &p.aborted_transactions {
                buf.put_i64(*pid);
                buf.put_i64(*first);
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
            buf.put_i32(p.preferred_read_replica);
            let mut recs = BytesMut::new();
            for batch in &p.records {
                records::encode_record_batch(&mut recs, batch)?;
            }
            if recs.is_empty() {
                buf::put_bytes(buf, flexible, None)?;
            } else {
                buf::put_bytes(buf, flexible, Some(&recs))?;
            }
            if flexible {
                encode_fetch_partition_tags(
                    buf,
                    p.diverging_epoch,
                    p.diverging_end_offset,
                    p.current_leader_id,
                    p.current_leader_epoch,
                )?;
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

/// Decode a Fetch v4–v11 (classic) or v12–v17 (flexible) response.
pub fn decode_fetch_response<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<FetchedTopic>> {
    let flexible = fetch_flexible(version)?;
    let _throttle = buf::get_i32(buf)?;
    let _error = buf::get_i16(buf)?;
    let _session = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let (topic, topic_id) = get_fetch_topic_identity(buf, version, flexible)?;
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let high_watermark = buf::get_i64(buf)?;
            let last_stable_offset = buf::get_i64(buf)?;
            let log_start_offset = buf::get_i64(buf)?;
            let aborted_len = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut aborted_transactions = Vec::with_capacity(aborted_len);
            for _ in 0..aborted_len {
                let pid = buf::get_i64(buf)?;
                let first = buf::get_i64(buf)?;
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                aborted_transactions.push((pid, first));
            }
            let preferred_read_replica = buf::get_i32(buf)?;
            let rec_bytes = if flexible {
                buf::take_compact_bytes(buf)?.unwrap_or_else(Bytes::new)
            } else {
                buf::take_classic_bytes(buf)?.unwrap_or_else(Bytes::new)
            };
            let records = if rec_bytes.is_empty() {
                Vec::new()
            } else {
                let mut rec_buf = rec_bytes;
                records::decode_record_batches(&mut rec_buf)?
            };
            let (diverging_epoch, diverging_end_offset, current_leader_id, current_leader_epoch) =
                if flexible {
                    decode_fetch_partition_tags(buf)?
                } else {
                    (-1, -1, -1, -1)
                };
            partitions.push(FetchedPartition {
                partition,
                error_code,
                high_watermark,
                last_stable_offset,
                log_start_offset,
                aborted_transactions,
                preferred_read_replica,
                current_leader_id,
                current_leader_epoch,
                diverging_epoch,
                diverging_end_offset,
                records,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(FetchedTopic {
            topic,
            topic_id,
            partitions,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(topics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::records::Record;
    use bytes::{BufMut, Bytes};

    #[test]
    fn fetch_request_sends_current_leader_epoch() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 7,
                fetch_offset: 3,
                last_fetched_epoch: -1,
                partition_max_bytes: 1024,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 11, 10, 1, 1024, 0, &topics, None).unwrap();
        let mut cur = &buf[..];
        let (iso, max_bytes, decoded, rack) = decode_fetch_request(&mut cur, 11).unwrap();
        assert_eq!(iso, 0);
        assert_eq!(max_bytes, 1024);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert_eq!(decoded[0].partitions[0].partition_max_bytes, 1024);
        assert!(rack.is_empty());
        assert!(
            cur.is_empty(),
            "Fetch v11 request leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn fetch_request_rack_id_is_empty_string_not_null() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: -1,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                partition_max_bytes: 1024,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 11, 10, 1, 1024, 0, &topics, None).unwrap();
        let tail = buf.get(buf.len().saturating_sub(2)..).unwrap();
        assert_eq!(
            tail,
            [0, 0],
            "v11 RackId must be empty STRING, not null i16=-1"
        );
    }

    #[test]
    fn fetch_request_sends_rack_id() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: -1,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                partition_max_bytes: 1024,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 11, 10, 1, 1024, 0, &topics, Some("az1")).unwrap();
        let (_iso, _max_bytes, _decoded, rack) = decode_fetch_request(&mut &buf[..], 11).unwrap();
        assert_eq!(rack, "az1");
    }

    #[test]
    fn fetch_v11_roundtrip() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &topics).unwrap();
        let decoded = decode_fetch_response(&mut &buf[..], 11).unwrap();
        assert_eq!(decoded[0].topic, "t");
        assert_eq!(
            decoded[0].partitions[0].records[0].records[0]
                .value
                .as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(decoded[0].partitions[0].log_start_offset, 0);
        assert!(decoded[0].partitions[0].aborted_transactions.is_empty());
    }

    #[test]
    fn fetch_response_preserves_aborted_transactions() {
        let rec = Record {
            offset: 1,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"aborted")),
            headers: vec![],
        };
        let mut batch = RecordBatch::from_records(vec![rec]);
        batch.producer_id = 1000;
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 2,
                last_stable_offset: 2,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![batch],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &topics).unwrap();
        let decoded = decode_fetch_response(&mut &buf[..], 11).unwrap();
        assert_eq!(
            decoded[0].partitions[0].aborted_transactions,
            vec![(1000, 1)]
        );
        assert_eq!(decoded[0].partitions[0].records[0].producer_id, 1000);
    }

    #[test]
    fn decode_fetch_response_keeps_log_start_on_offset_out_of_range() {
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: crate::error::OFFSET_OUT_OF_RANGE,
                high_watermark: 20,
                last_stable_offset: 20,
                log_start_offset: 10,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &topics).unwrap();
        let decoded = decode_fetch_response(&mut &buf[..], 11).unwrap();
        assert_eq!(
            decoded[0].partitions[0].error_code,
            crate::error::OFFSET_OUT_OF_RANGE
        );
        assert_eq!(decoded[0].partitions[0].log_start_offset, 10);
        assert!(decoded[0].partitions[0].records.is_empty());
    }

    #[test]
    fn decode_fetch_response_uses_record_batch_decoder_on_partition_bytes() {
        let rec = |v: &'static [u8]| Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(v)),
            headers: vec![],
        };
        let mut recs = BytesMut::new();
        records::encode_record_batch(&mut recs, &RecordBatch::from_records(vec![rec(b"one")]))
            .unwrap();
        records::encode_record_batch(&mut recs, &RecordBatch::from_records(vec![rec(b"two")]))
            .unwrap();
        recs.extend_from_slice(&[0u8; 8]);
        let mut body = BytesMut::new();
        body.put_i32(0);
        body.put_i16(0);
        body.put_i32(0);
        crate::protocol::buf::put_array_len(&mut body, false, Some(1)).unwrap();
        crate::protocol::buf::put_classic_nullable_string(&mut body, Some("t")).unwrap();
        crate::protocol::buf::put_array_len(&mut body, false, Some(1)).unwrap();
        body.put_i32(0);
        body.put_i16(0);
        body.put_i64(2);
        body.put_i64(2);
        body.put_i64(0);
        body.put_i32(-1);
        body.put_i32(-1);
        crate::protocol::buf::put_classic_bytes(&mut body, Some(&recs)).unwrap();
        let decoded = decode_fetch_response(&mut &body[..], 11).unwrap();
        assert_eq!(decoded[0].partitions[0].records.len(), 2);
        assert_eq!(
            decoded[0].partitions[0].records[0].records[0]
                .value
                .as_deref(),
            Some(&b"one"[..])
        );
        assert_eq!(
            decoded[0].partitions[0].records[1].records[0]
                .value
                .as_deref(),
            Some(&b"two"[..])
        );
    }

    #[test]
    fn decode_fetch_response_from_bytes_shares_record_value() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"view-me")),
            headers: vec![],
        };
        let mut batch = RecordBatch::from_records(vec![rec]);
        batch.base_offset = 20;
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 21,
                last_stable_offset: 21,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![batch],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 11, &topics).unwrap();
        let frozen = buf.freeze();
        let decoded = decode_fetch_response(&mut frozen.clone(), 11).unwrap();
        let got = &decoded[0].partitions[0].records[0].records[0];
        assert_eq!(got.offset, 20);
        assert_eq!(got.value.as_deref(), Some(&b"view-me"[..]));
        let start = frozen.as_ptr();
        let end = start.wrapping_add(frozen.len());
        let value = got.value.as_ref().unwrap();
        assert!(
            value.as_ptr() >= start && value.as_ptr() < end,
            "fetch record value must be a view into the response frame"
        );
    }

    #[test]
    fn fetch_v12_roundtrip_is_leftover_empty() {
        let req_topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 7,
                fetch_offset: 3,
                last_fetched_epoch: 4,
                partition_max_bytes: 1024,
            }],
        }];
        let mut req = BytesMut::new();
        encode_fetch_request(&mut req, 12, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut cur = &req[..];
        let (iso, max_bytes, decoded, rack) = decode_fetch_request(&mut cur, 12).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert_eq!(decoded[0].partitions[0].last_fetched_epoch, 4);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v12 request must consume compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp = BytesMut::new();
        encode_fetch_response(&mut resp, 12, &topics).unwrap();
        let mut cur = &resp[..];
        let got = decode_fetch_response(&mut cur, 12).unwrap();
        assert_eq!(got[0].topic, "t");
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(got[0].partitions[0].aborted_transactions, vec![(1000, 1)]);
        assert!(
            cur.is_empty(),
            "Fetch v12 response must consume compact tagged fields"
        );
        req.clear();
        assert!(
            encode_fetch_request(&mut req, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }

    #[test]
    fn fetch_v12_request_matches_compact_layout() {
        // ReplicaId -1, MaxWait 10, MinBytes 1, MaxBytes 1024, isolation 0,
        // session 0 / -1, compact Topics {Name "t", compact Partitions
        // {0, epoch 0, offset 0, lastFetched -1, logStart -1, maxBytes
        // 1024, tagged}, tagged}, empty forgotten, empty RackId, tagged.
        const REQ: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x02, 0x02, 0x74,
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00,
        ];
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: [0; 16],
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 0,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                partition_max_bytes: 1024,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 12, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(&buf[..], REQ);
    }

    const SAMPLE_TOPIC_ID: [u8; 16] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];

    fn sample_v13_topic() -> FetchTopic {
        FetchTopic {
            topic: "t".into(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 7,
                fetch_offset: 3,
                last_fetched_epoch: -1,
                partition_max_bytes: 1024,
            }],
        }
    }

    #[test]
    fn fetch_v14_roundtrip_is_leftover_empty() {
        let req_topics = vec![sample_v13_topic()];
        let mut req = BytesMut::new();
        encode_fetch_request(&mut req, 14, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut cur = &req[..];
        let (iso, max_bytes, decoded, rack) = decode_fetch_request(&mut cur, 14).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert!(decoded[0].topic.is_empty());
        assert_eq!(decoded[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert_eq!(decoded[0].partitions[0].last_fetched_epoch, -1);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v14 request must consume TopicId plus compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp = BytesMut::new();
        encode_fetch_response(&mut resp, 14, &topics).unwrap();
        let mut cur = &resp[..];
        let got = decode_fetch_response(&mut cur, 14).unwrap();
        assert!(got[0].topic.is_empty());
        assert_eq!(got[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(got[0].partitions[0].aborted_transactions, vec![(1000, 1)]);
        assert!(
            cur.is_empty(),
            "Fetch v14 response must consume TopicId plus compact tagged fields"
        );
        let mut v13 = BytesMut::new();
        encode_fetch_request(&mut v13, 13, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        assert_eq!(
            &v13[..],
            &req[..],
            "Fetch v13 and v14 request layout must match"
        );
        req.clear();
        assert!(
            encode_fetch_request(&mut req, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }

    #[test]
    fn fetch_v14_request_matches_topic_id_layout() {
        // Same as v12 compact layout except Topics uses TopicId UUID
        // instead of compact Name "t" (0x02 0x74).
        const REQ: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x02, 0x01, 0x02,
            0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00,
        ];
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 0,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                partition_max_bytes: 1024,
            }],
        }];
        let mut v12 = BytesMut::new();
        encode_fetch_request(&mut v12, 12, 10, 1, 1024, 0, &topics, None).unwrap();
        let mut v14 = BytesMut::new();
        encode_fetch_request(&mut v14, 14, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_ne!(
            &v14[..],
            &v12[..],
            "Fetch v14 TopicId bytes must not equal v12 compact name"
        );
        assert_eq!(&v14[..], REQ);
    }

    #[test]
    fn fetch_v14_forgotten_topic_id_is_leftover_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1);
        buf.put_i32(10);
        buf.put_i32(1);
        buf.put_i32(1024);
        buf.put_i8(0);
        buf.put_i32(0);
        buf.put_i32(-1);
        crate::protocol::buf::put_array_len(&mut buf, true, Some(0)).unwrap();
        crate::protocol::buf::put_array_len(&mut buf, true, Some(1)).unwrap();
        buf.extend_from_slice(&SAMPLE_TOPIC_ID);
        crate::protocol::buf::put_array_len(&mut buf, true, Some(1)).unwrap();
        buf.put_i32(0);
        crate::protocol::buf::put_empty_tagged_fields(&mut buf);
        crate::protocol::buf::put_string(&mut buf, true, Some("")).unwrap();
        crate::protocol::buf::put_empty_tagged_fields(&mut buf);
        let mut cur = &buf[..];
        let (iso, max_bytes, topics, rack) = decode_fetch_request(&mut cur, 14).unwrap();
        assert_eq!(iso, 0);
        assert_eq!(max_bytes, 1024);
        assert!(topics.is_empty());
        assert!(rack.is_empty());
        assert!(
            cur.is_empty(),
            "Fetch v14 forgotten TopicId must be consumed"
        );
    }

    #[test]
    fn fetch_v15_roundtrip_is_leftover_empty() {
        let req_topics = vec![sample_v13_topic()];
        let mut req = BytesMut::new();
        encode_fetch_request(&mut req, 15, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut cur = &req[..];
        let (iso, max_bytes, decoded, rack) = decode_fetch_request(&mut cur, 15).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert!(decoded[0].topic.is_empty());
        assert_eq!(decoded[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert_eq!(decoded[0].partitions[0].last_fetched_epoch, -1);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v15 request must omit untagged ReplicaId and consume compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp = BytesMut::new();
        encode_fetch_response(&mut resp, 15, &topics).unwrap();
        let mut cur = &resp[..];
        let got = decode_fetch_response(&mut cur, 15).unwrap();
        assert!(got[0].topic.is_empty());
        assert_eq!(got[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(got[0].partitions[0].aborted_transactions, vec![(1000, 1)]);
        assert!(
            cur.is_empty(),
            "Fetch v15 response must match the v14 compact layout"
        );
        let mut v14_resp = BytesMut::new();
        encode_fetch_response(&mut v14_resp, 14, &topics).unwrap();
        assert_eq!(
            &v14_resp[..],
            &resp[..],
            "Fetch v14 and v15 response layout must match"
        );
        req.clear();
        assert!(
            encode_fetch_request(&mut req, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }

    #[test]
    fn fetch_v15_request_omits_untagged_replica_id() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 0,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                partition_max_bytes: 1024,
            }],
        }];
        let mut v14 = BytesMut::new();
        encode_fetch_request(&mut v14, 14, 10, 1, 1024, 0, &topics, None).unwrap();
        let mut v15 = BytesMut::new();
        encode_fetch_request(&mut v15, 15, 10, 1, 1024, 0, &topics, None).unwrap();
        assert_eq!(
            v14.get(..4),
            Some([0xff, 0xff, 0xff, 0xff].as_slice()),
            "Fetch v14 starts with untagged ReplicaId -1"
        );
        assert_eq!(
            v15.as_ref(),
            v14.get(4..).unwrap(),
            "Fetch v15 request is v14 without untagged ReplicaId"
        );
    }

    #[test]
    fn fetch_v16_roundtrip_is_leftover_empty() {
        let req_topics = vec![sample_v13_topic()];
        let mut v15 = BytesMut::new();
        encode_fetch_request(&mut v15, 15, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut v16 = BytesMut::new();
        encode_fetch_request(&mut v16, 16, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        assert_eq!(
            &v15[..],
            &v16[..],
            "Fetch v16 request layout must match v15"
        );
        let mut cur = &v16[..];
        let (iso, max_bytes, decoded, rack) = decode_fetch_request(&mut cur, 16).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert_eq!(decoded[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v16 request must consume compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp15 = BytesMut::new();
        encode_fetch_response(&mut resp15, 15, &topics).unwrap();
        let mut resp16 = BytesMut::new();
        encode_fetch_response(&mut resp16, 16, &topics).unwrap();
        assert_eq!(
            &resp15[..],
            &resp16[..],
            "Fetch v16 empty CurrentLeader / NodeEndpoints must match v15"
        );
        let mut cur = &resp16[..];
        let got = decode_fetch_response(&mut cur, 16).unwrap();
        assert_eq!(got[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(got[0].partitions[0].current_leader_id, -1);
        assert_eq!(got[0].partitions[0].current_leader_epoch, -1);
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert!(
            cur.is_empty(),
            "Fetch v16 response must consume compact tagged fields"
        );
        v16.clear();
        assert!(
            encode_fetch_request(&mut v16, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }

    #[test]
    fn fetch_v16_current_leader_tagged_is_leftover_empty() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let mut with_leader = FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 6,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: 2,
                current_leader_epoch: 7,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        };
        let topics = vec![with_leader.clone()];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 16, &topics).unwrap();
        let mut cur = &buf[..];
        let got = decode_fetch_response(&mut cur, 16).unwrap();
        assert_eq!(got[0].partitions[0].current_leader_id, 2);
        assert_eq!(got[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(got[0].partitions[0].diverging_epoch, -1);
        assert_eq!(got[0].partitions[0].diverging_end_offset, -1);
        assert!(
            cur.is_empty(),
            "Fetch CurrentLeader tagged field 1 must consume nested tagged fields"
        );
        let mut v15 = BytesMut::new();
        encode_fetch_response(&mut v15, 15, &topics).unwrap();
        assert_eq!(
            &v15[..],
            &buf[..],
            "Fetch v12+ CurrentLeader layout is unchanged at v16"
        );
        with_leader.partitions[0].current_leader_id = -1;
        with_leader.partitions[0].current_leader_epoch = -1;
        let mut omitted = BytesMut::new();
        encode_fetch_response(&mut omitted, 16, &[with_leader]).unwrap();
        assert_ne!(
            &buf[..],
            &omitted[..],
            "CurrentLeader tagged field 1 must not equal empty tags"
        );
    }

    #[test]
    fn fetch_v16_diverging_epoch_tagged_is_leftover_empty() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let mut with_div = FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: 3,
                diverging_end_offset: 12,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        };
        let topics = vec![with_div.clone()];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, 16, &topics).unwrap();
        let mut cur = &buf[..];
        let got = decode_fetch_response(&mut cur, 16).unwrap();
        assert_eq!(got[0].partitions[0].diverging_epoch, 3);
        assert_eq!(got[0].partitions[0].diverging_end_offset, 12);
        assert_eq!(got[0].partitions[0].current_leader_id, -1);
        assert!(
            cur.is_empty(),
            "Fetch DivergingEpoch tagged field 0 must consume nested tagged fields"
        );
        let mut v12 = BytesMut::new();
        encode_fetch_response(&mut v12, 12, &topics).unwrap();
        assert_eq!(
            &v12[..],
            &buf[..],
            "Fetch v12+ DivergingEpoch layout is unchanged at v16"
        );
        with_div.partitions[0].diverging_epoch = -1;
        with_div.partitions[0].diverging_end_offset = -1;
        let mut omitted = BytesMut::new();
        encode_fetch_response(&mut omitted, 16, &[with_div]).unwrap();
        assert_ne!(
            &buf[..],
            &omitted[..],
            "DivergingEpoch tagged field 0 must not equal empty tags"
        );
    }

    #[test]
    fn fetch_v17_roundtrip_matches_v16() {
        let req_topics = vec![sample_v13_topic()];
        let mut v16 = BytesMut::new();
        encode_fetch_request(&mut v16, 16, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        let mut v17 = BytesMut::new();
        encode_fetch_request(&mut v17, 17, 10, 1, 1024, 1, &req_topics, Some("az1")).unwrap();
        assert_eq!(
            &v16[..],
            &v17[..],
            "Fetch v17 consumer request must omit ReplicaDirectoryId and match v16"
        );
        let mut cur = &v17[..];
        let (iso, max_bytes, decoded, rack) = decode_fetch_request(&mut cur, 17).unwrap();
        assert_eq!(iso, 1);
        assert_eq!(max_bytes, 1024);
        assert_eq!(decoded[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(rack, "az1");
        assert!(
            cur.is_empty(),
            "Fetch v17 request must consume compact tagged fields"
        );

        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"f")),
            headers: vec![],
        };
        let topics = vec![FetchedTopic {
            topic: String::new(),
            topic_id: SAMPLE_TOPIC_ID,
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                current_leader_id: -1,
                current_leader_epoch: -1,
                diverging_epoch: -1,
                diverging_end_offset: -1,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut resp16 = BytesMut::new();
        encode_fetch_response(&mut resp16, 16, &topics).unwrap();
        let mut resp17 = BytesMut::new();
        encode_fetch_response(&mut resp17, 17, &topics).unwrap();
        assert_eq!(
            &resp16[..],
            &resp17[..],
            "Fetch v17 response layout must match v16"
        );
        let mut cur = &resp17[..];
        let got = decode_fetch_response(&mut cur, 17).unwrap();
        assert_eq!(got[0].topic_id, SAMPLE_TOPIC_ID);
        assert_eq!(
            got[0].partitions[0].records[0].records[0].value.as_deref(),
            Some(&b"f"[..])
        );
        assert!(
            cur.is_empty(),
            "Fetch v17 response must consume compact tagged fields"
        );
        v17.clear();
        assert!(
            encode_fetch_request(&mut v17, 18, 10, 1, 1024, 0, &req_topics, None).is_err(),
            "Fetch v18+ (HighWatermark) is not spoken"
        );
    }
}
