#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use super::records::{self, RecordBatch};
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct FetchPartition {
    pub partition: i32,
    pub current_leader_epoch: i32,
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
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    /// `(producer_id, first_offset)` for aborted transactions (Fetch isolation=1).
    pub aborted_transactions: Vec<(i64, i64)>,
    /// Broker id to fetch from next, or `-1`.
    pub preferred_read_replica: i32,
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
    isolation_level: i8,
    topics: &[FetchTopic],
    rack_id: Option<&str>,
) -> crate::error::Result<()> {
    buf.put_i32(-1); // replica_id
    buf.put_i32(max_wait_ms);
    buf.put_i32(min_bytes);
    buf.put_i32(max_bytes);
    buf.put_i8(isolation_level);
    buf.put_i32(0); // session_id
    buf.put_i32(-1); // session_epoch
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic))?;
        buf::put_array_len(buf, false, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i32(p.current_leader_epoch);
            buf.put_i64(p.fetch_offset);
            buf.put_i64(-1); // log_start_offset
            buf.put_i32(p.partition_max_bytes);
        }
    }
    buf::put_array_len(buf, false, Some(0))?; // forgotten
    buf::put_classic_nullable_string(buf, rack_id.filter(|s| !s.is_empty()))?;
    Ok(())
}

pub fn decode_fetch_request<B: Buf>(buf: &mut B) -> Result<(i8, Vec<FetchTopic>, String)> {
    let _replica = buf::get_i32(buf)?;
    let _max_wait = buf::get_i32(buf)?;
    let _min_bytes = buf::get_i32(buf)?;
    let _max_bytes = buf::get_i32(buf)?;
    let isolation = buf::get_i8(buf)?;
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
            let current_leader_epoch = buf::get_i32(buf)?;
            let fetch_offset = buf::get_i64(buf)?;
            let _log_start = buf::get_i64(buf)?;
            let partition_max_bytes = buf::get_i32(buf)?;
            partitions.push(FetchPartition {
                partition,
                current_leader_epoch,
                fetch_offset,
                partition_max_bytes,
            });
        }
        topics.push(FetchTopic { topic, partitions });
    }
    let forgotten = buf::get_array_len(buf, false)?.unwrap_or(0);
    for _ in 0..forgotten {
        let _t = buf::get_classic_nullable_string(buf)?;
        let pn = buf::get_array_len(buf, false)?.unwrap_or(0);
        for _ in 0..pn {
            let _p = buf::get_i32(buf)?;
        }
    }
    let rack = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    Ok((isolation, topics, rack))
}

pub fn encode_fetch_response(buf: &mut BytesMut, topics: &[FetchedTopic]) -> Result<()> {
    buf.put_i32(0); // throttle
    buf.put_i16(0); // top-level error
    buf.put_i32(0); // session_id
    buf::put_array_len(buf, false, Some(topics.len()))?;
    for t in topics {
        buf::put_classic_nullable_string(buf, Some(&t.topic))?;
        buf::put_array_len(buf, false, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i16(p.error_code);
            buf.put_i64(p.high_watermark);
            buf.put_i64(p.last_stable_offset);
            buf.put_i64(p.log_start_offset);
            buf::put_array_len(buf, false, Some(p.aborted_transactions.len()))?;
            for (pid, first) in &p.aborted_transactions {
                buf.put_i64(*pid);
                buf.put_i64(*first);
            }
            buf.put_i32(p.preferred_read_replica);
            let mut recs = BytesMut::new();
            for batch in &p.records {
                records::encode_record_batch(&mut recs, batch)?;
            }
            if recs.is_empty() {
                buf::put_classic_bytes(buf, None)?;
            } else {
                buf::put_classic_bytes(buf, Some(&recs))?;
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
            let last_stable_offset = buf::get_i64(buf)?;
            let log_start_offset = buf::get_i64(buf)?;
            let aborted_len = buf::get_i32(buf)?;
            let mut aborted_transactions = Vec::new();
            if aborted_len > 0 {
                for _ in 0..aborted_len {
                    let pid = buf::get_i64(buf)?;
                    let first = buf::get_i64(buf)?;
                    aborted_transactions.push((pid, first));
                }
            }
            let preferred_read_replica = buf::get_i32(buf)?;
            let rec_bytes = buf::take_classic_bytes(buf)?.unwrap_or_else(Bytes::new);
            let records = if rec_bytes.is_empty() {
                Vec::new()
            } else {
                let mut rec_buf = rec_bytes;
                records::decode_record_batches(&mut rec_buf)?
            };
            partitions.push(FetchedPartition {
                partition,
                error_code,
                high_watermark,
                last_stable_offset,
                log_start_offset,
                aborted_transactions,
                preferred_read_replica,
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
    fn fetch_request_sends_current_leader_epoch() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 7,
                fetch_offset: 3,
                partition_max_bytes: 1024,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 10, 1, 1024, 0, &topics, None).unwrap();
        let (iso, decoded, rack) = decode_fetch_request(&mut &buf[..]).unwrap();
        assert_eq!(iso, 0);
        assert_eq!(decoded[0].partitions[0].current_leader_epoch, 7);
        assert_eq!(decoded[0].partitions[0].fetch_offset, 3);
        assert!(rack.is_empty());
    }

    #[test]
    fn fetch_request_sends_rack_id() {
        let topics = vec![FetchTopic {
            topic: "t".into(),
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: -1,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_request(&mut buf, 10, 1, 1024, 0, &topics, Some("az1")).unwrap();
        let (_iso, _decoded, rack) = decode_fetch_request(&mut &buf[..]).unwrap();
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
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
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
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 2,
                last_stable_offset: 2,
                log_start_offset: 0,
                aborted_transactions: vec![(1000, 1)],
                preferred_read_replica: -1,
                records: vec![batch],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, &topics).unwrap();
        let decoded = decode_fetch_response(&mut &buf[..]).unwrap();
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
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: crate::error::OFFSET_OUT_OF_RANGE,
                high_watermark: 20,
                last_stable_offset: 20,
                log_start_offset: 10,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
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
        let decoded = decode_fetch_response(&mut &body[..]).unwrap();
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
            partitions: vec![FetchedPartition {
                partition: 0,
                error_code: 0,
                high_watermark: 21,
                last_stable_offset: 21,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: -1,
                records: vec![batch],
            }],
        }];
        let mut buf = BytesMut::new();
        encode_fetch_response(&mut buf, &topics).unwrap();
        let frozen = buf.freeze();
        let decoded = decode_fetch_response(&mut frozen.clone()).unwrap();
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
}
