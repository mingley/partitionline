//! AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, WriteTxnMarkers, and
//! TxnOffsetCommit (api keys 24–28).

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// AddPartitionsToTxn (24).
pub const ADD_PARTITIONS_TO_TXN: i16 = 24;
/// AddOffsetsToTxn (25).
pub const ADD_OFFSETS_TO_TXN: i16 = 25;
/// EndTxn (26).
pub const END_TXN: i16 = 26;
/// WriteTxnMarkers (27).
pub const WRITE_TXN_MARKERS: i16 = 27;
/// TxnOffsetCommit (28).
pub const TXN_OFFSET_COMMIT: i16 = 28;

/// One topic in AddPartitionsToTxn v0–v3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnPartitionsTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to add to the transaction.
    pub partitions: Vec<i32>,
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
pub fn encode_add_partitions_to_txn_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[TxnPartitionsTopic],
    error: i16,
) -> Result<()> {
    let flexible = add_partitions_to_txn_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(*p);
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

/// Decode AddPartitionsToTxn: first non-zero partition error, or `0`.
pub fn decode_add_partitions_to_txn_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let flexible = add_partitions_to_txn_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut first_err = 0i16;
    for _ in 0..tn {
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

/// Encode EndTxn (`committed` is commit vs abort).
pub fn encode_end_txn_request(
    buf: &mut BytesMut,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf.put_u8(u8::from(committed));
    Ok(())
}

/// Decode EndTxn: `(transactional_id, producer_id, producer_epoch, committed)`.
pub fn decode_end_txn_request<B: Buf>(buf: &mut B) -> Result<(String, i64, i16, bool)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let pid = buf::get_i64(buf)?;
    let epoch = buf::get_i16(buf)?;
    let committed = buf.get_u8() != 0;
    Ok((tid, pid, epoch, committed))
}

/// Encode EndTxn: throttle `0` plus error code.
pub fn encode_end_txn_response(buf: &mut BytesMut, error: i16) -> Result<()> {
    buf.put_i32(0);
    buf.put_i16(error);
    Ok(())
}

/// Decode EndTxn: error code.
pub fn decode_end_txn_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

/// One partition in TxnOffsetCommit v0–2 (classic).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetPartition {
    /// Partition index.
    pub partition: i32,
    /// Committed offset.
    pub offset: i64,
    /// Leader epoch (v2+), or `-1`.
    pub leader_epoch: i32,
    /// Commit metadata string.
    pub metadata: String,
}

/// Topic + partitions for TxnOffsetCommit v0–2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions in this topic.
    pub partitions: Vec<TxnOffsetPartition>,
}

/// TxnOffsetCommit v0–2 (classic). `committed_leader_epoch` is v2+.
pub fn encode_txn_offset_commit_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: &str,
    group_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    topics: &[TxnOffsetTopic],
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(transactional_id))?;
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic))?;
        buf::put_array_len(buf, false, Some(t.partitions.len()))?;
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
            buf::put_classic_nullable_string(buf, meta)?;
        }
    }
    Ok(())
}

/// Decode TxnOffsetCommit: `(transactional_id, group_id, topics)`.
pub fn decode_txn_offset_commit_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, String, Vec<TxnOffsetTopic>)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let gid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _pid = buf::get_i64(buf)?;
    let _epoch = buf::get_i16(buf)?;
    let tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let offset = buf::get_i64(buf)?;
            let leader_epoch = if version >= 2 { buf::get_i32(buf)? } else { -1 };
            let metadata = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
            partitions.push(TxnOffsetPartition {
                partition,
                offset,
                leader_epoch,
                metadata,
            });
        }
        topics.push(TxnOffsetTopic { topic, partitions });
    }
    Ok((tid, gid, topics))
}

/// Encode TxnOffsetCommit: one error code applied to every partition.
pub fn encode_txn_offset_commit_response(
    buf: &mut BytesMut,
    topics: &[TxnOffsetTopic],
    error: i16,
) -> Result<()> {
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

/// Decode TxnOffsetCommit: first non-zero partition error, or `0`.
pub fn decode_txn_offset_commit_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    let tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut first_err = 0i16;
    for _ in 0..tn {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableTxnMarker {
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// `true` is COMMIT, `false` is ABORT.
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

    #[test]
    fn end_txn_roundtrip() {
        let mut buf = BytesMut::new();
        encode_end_txn_request(&mut buf, "tx", 9, 1, true).unwrap();
        let (tid, pid, epoch, committed) = decode_end_txn_request(&mut &buf[..]).unwrap();
        assert_eq!((tid.as_str(), pid, epoch, committed), ("tx", 9, 1, true));
        let mut resp = BytesMut::new();
        encode_end_txn_response(&mut resp, 0).unwrap();
        assert_eq!(decode_end_txn_response(&mut &resp[..]).unwrap(), 0);
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
        encode_txn_offset_commit_request(&mut buf, 0, "tx", "g", 9, 1, &topics).unwrap();
        let mut cur = &buf[..];
        let (tid, gid, got) = decode_txn_offset_commit_request(&mut cur, 0).unwrap();
        assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
        let part = got
            .first()
            .and_then(|t| t.partitions.first())
            .expect("one partition");
        assert_eq!(part.partition, 0);
        assert_eq!(part.offset, 7);
        assert_eq!(
            part.leader_epoch, -1,
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
        encode_txn_offset_commit_request(&mut buf, 2, "tx", "g", 9, 1, &topics).unwrap();
        let mut cur = &buf[..];
        let (tid, gid, got) = decode_txn_offset_commit_request(&mut cur, 2).unwrap();
        assert_eq!((tid.as_str(), gid.as_str()), ("tx", "g"));
        assert_eq!(got, topics);
        assert!(
            cur.is_empty(),
            "v2 decoder must consume leader epoch and metadata; leftover {} bytes",
            cur.len()
        );

        buf.clear();
        encode_txn_offset_commit_response(&mut buf, &topics, 0).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_txn_offset_commit_response(&mut cur).unwrap(), 0);
        assert!(cur.is_empty());
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
            transaction_result: false,
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
            transaction_result: false,
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
            transaction_result: false,
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
            transaction_result: false,
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
