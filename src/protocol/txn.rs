#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

pub const ADD_PARTITIONS_TO_TXN: i16 = 24;
pub const ADD_OFFSETS_TO_TXN: i16 = 25;
pub const END_TXN: i16 = 26;
pub const TXN_OFFSET_COMMIT: i16 = 28;

/// One topic in AddPartitionsToTxn v0–1 (classic).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnPartitionsTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

pub fn encode_add_partitions_to_txn_request(
    buf: &mut BytesMut,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    topics: &[TxnPartitionsTopic],
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
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

pub fn decode_add_partitions_to_txn_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, i64, i16, Vec<TxnPartitionsTopic>)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let pid = buf::get_i64(buf)?;
    let epoch = buf::get_i16(buf)?;
    let tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        topics.push(TxnPartitionsTopic { topic, partitions });
    }
    Ok((tid, pid, epoch, topics))
}

pub fn encode_add_partitions_to_txn_response(
    buf: &mut BytesMut,
    topics: &[TxnPartitionsTopic],
    error: i16,
) -> Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic))?;
        buf::put_array_len(buf, false, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(*p);
            buf.put_i16(error);
        }
    }
    Ok(())
}

pub fn decode_add_partitions_to_txn_response<B: Buf>(buf: &mut B) -> Result<i16> {
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

pub fn decode_end_txn_request<B: Buf>(buf: &mut B) -> Result<(String, i64, i16, bool)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let pid = buf::get_i64(buf)?;
    let epoch = buf::get_i16(buf)?;
    let committed = buf.get_u8() != 0;
    Ok((tid, pid, epoch, committed))
}

pub fn encode_end_txn_response(buf: &mut BytesMut, error: i16) -> Result<()> {
    buf.put_i32(0);
    buf.put_i16(error);
    Ok(())
}

pub fn decode_end_txn_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

pub fn encode_add_offsets_to_txn_request(
    buf: &mut BytesMut,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    Ok(())
}

pub fn decode_add_offsets_to_txn_request<B: Buf>(buf: &mut B) -> Result<(String, String)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _pid = buf::get_i64(buf)?;
    let _epoch = buf::get_i16(buf)?;
    let gid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    Ok((tid, gid))
}

pub fn encode_add_offsets_to_txn_response(buf: &mut BytesMut, error: i16) -> Result<()> {
    buf.put_i32(0);
    buf.put_i16(error);
    Ok(())
}

pub fn decode_add_offsets_to_txn_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

/// One partition in TxnOffsetCommit v0–2 (classic).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetPartition {
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: String,
}

/// Topic + partitions for TxnOffsetCommit v0–2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetTopic {
    pub topic: String,
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
        encode_add_partitions_to_txn_request(&mut buf, "tx", 9, 1, &topics).unwrap();
        let mut cur = &buf[..];
        let (tid, pid, epoch, got) = decode_add_partitions_to_txn_request(&mut cur).unwrap();
        assert_eq!((tid.as_str(), pid, epoch), ("tx", 9, 1));
        assert_eq!(got, topics);
        assert!(cur.is_empty());

        buf.clear();
        encode_add_partitions_to_txn_response(&mut buf, &topics, 0).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_add_partitions_to_txn_response(&mut cur).unwrap(), 0);
        assert!(cur.is_empty());
    }
}
