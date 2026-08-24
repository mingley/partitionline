use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use super::records::{self, RecordBatch};
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct FetchPartition {
    pub partition: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug, Clone)]
pub struct FetchTopic {
    pub topic: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug, Clone)]
pub struct FetchedPartition {
    pub partition: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub records: Vec<RecordBatch>,
}

#[derive(Debug, Clone)]
pub struct FetchedTopic {
    pub topic: String,
    pub partitions: Vec<FetchedPartition>,
}

/// Fetch v11 (classic; last version before flexible v12).
pub fn encode_fetch_request(
    buf: &mut BytesMut,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    topics: &[FetchTopic],
) {
    buf.put_i32(-1); // replica_id
    buf.put_i32(max_wait_ms);
    buf.put_i32(min_bytes);
    buf.put_i32(max_bytes);
    buf.put_i8(0); // isolation_level READ_UNCOMMITTED
    buf.put_i32(0); // session_id
    buf.put_i32(-1); // session_epoch
    buf::put_array_len(buf, false, Some(topics.len()));
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic));
        buf::put_array_len(buf, false, Some(t.partitions.len()));
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i32(-1); // current_leader_epoch
            buf.put_i64(p.fetch_offset);
            buf.put_i64(-1); // log_start_offset
            buf.put_i32(p.partition_max_bytes);
        }
    }
    buf::put_array_len(buf, false, Some(0)); // forgotten
    buf::put_classic_nullable_string(buf, Some("")); // rack_id
}

pub fn decode_fetch_request<B: Buf>(buf: &mut B) -> Result<Vec<FetchTopic>> {
    let _replica = buf::get_i32(buf)?;
    let _max_wait = buf::get_i32(buf)?;
    let _min_bytes = buf::get_i32(buf)?;
    let _max_bytes = buf::get_i32(buf)?;
    let _isolation = buf::get_i8(buf)?;
    let _session_id = buf::get_i32(buf)?;
    let _session_epoch = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let _epoch = buf::get_i32(buf)?;
            let fetch_offset = buf::get_i64(buf)?;
            let _log_start = buf::get_i64(buf)?;
            let partition_max_bytes = buf::get_i32(buf)?;
            partitions.push(FetchPartition {
                partition,
                fetch_offset,
                partition_max_bytes,
            });
        }
        topics.push(FetchTopic { topic, partitions });
    }
    Ok(topics)
}

pub fn encode_fetch_response(buf: &mut BytesMut, topics: &[FetchedTopic]) -> Result<()> {
    buf.put_i32(0); // throttle
    buf.put_i16(0); // top-level error
    buf.put_i32(0); // session_id
    buf::put_array_len(buf, false, Some(topics.len()));
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic));
        buf::put_array_len(buf, false, Some(t.partitions.len()));
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf.put_i64(p.high_watermark);
            buf.put_i64(p.high_watermark); // last_stable_offset
            buf.put_i64(p.log_start_offset);
            buf.put_i32(-1); // aborted_transactions null
            buf.put_i32(-1); // preferred_read_replica
            let mut recs = BytesMut::new();
            for batch in &p.records {
                records::encode_record_batch(&mut recs, batch)?;
            }
            if recs.is_empty() {
                buf::put_classic_bytes(buf, None);
            } else {
                buf::put_classic_bytes(buf, Some(&recs));
            }
        }
    }
    Ok(())
}

pub fn decode_fetch_response<B: Buf>(buf: &mut B) -> Result<Vec<FetchedTopic>> {
    let _throttle = buf::get_i32(buf)?;
    let _error = buf::get_i16(buf)?;
    let _session = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let high_watermark = buf::get_i64(buf)?;
            let _lso = buf::get_i64(buf)?;
            let log_start_offset = buf::get_i64(buf)?;
            let aborted_len = buf::get_i32(buf)?;
            if aborted_len > 0 {
                for _ in 0..aborted_len {
                    let _ = buf::get_i64(buf)?;
                    let _ = buf::get_i64(buf)?;
                }
            }
            let _pref = buf::get_i32(buf)?;
            let rec_bytes = buf::get_classic_bytes(buf)?.unwrap_or_default();
            let mut rec_buf = &rec_bytes[..];
            let records = if rec_buf.is_empty() {
                Vec::new()
            } else {
                records::decode_record_batches(&mut rec_buf)?
            };
            partitions.push(FetchedPartition {
                partition,
                error_code,
                high_watermark,
                log_start_offset,
                records,
            });
        }
        topics.push(FetchedTopic { topic, partitions });
    }
    Ok(topics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::records::Record;
    use bytes::Bytes;

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
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                log_start_offset: 0,
                records: vec![RecordBatch::from_records(vec![rec])],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, &topics).unwrap();
        let decoded = decode_fetch_response(&mut &buf[..]).unwrap();
        assert_eq!(decoded[0].topic, "t");
        assert_eq!(
            decoded[0].partitions[0].records[0].records[0]
                .value
                .as_deref(),
            Some(&b"f"[..])
        );
        assert_eq!(decoded[0].partitions[0].log_start_offset, 0);
    }

    #[test]
    fn decode_fetch_response_keeps_log_start_on_offset_out_of_range() {
        let topics = vec![FetchedTopic {
            topic: "t".into(),
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: crate::error::OFFSET_OUT_OF_RANGE,
                high_watermark: 20,
                log_start_offset: 10,
                records: vec![],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, &topics).unwrap();
        let decoded = decode_fetch_response(&mut &buf[..]).unwrap();
        assert_eq!(
            decoded[0].partitions[0].error_code,
            crate::error::OFFSET_OUT_OF_RANGE
        );
        assert_eq!(decoded[0].partitions[0].log_start_offset, 10);
        assert!(decoded[0].partitions[0].records.is_empty());
    }
}
