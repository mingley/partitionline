use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use crate::error::{Error, Result};

pub const MAGIC_V2: i8 = 2;
const BATCH_OVERHEAD: usize = 61;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub attributes: i16,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl RecordBatch {
    pub fn from_records(records: Vec<Record>) -> Self {
        let base_timestamp = records.first().map(|r| r.timestamp).unwrap_or(0);
        let max_timestamp = records
            .iter()
            .map(|r| r.timestamp)
            .max()
            .unwrap_or(base_timestamp);
        Self {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: 0,
            base_timestamp,
            max_timestamp,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        }
    }
}

pub fn encode_record_batch(buf: &mut BytesMut, batch: &RecordBatch) {
    let batch_start = buf.len();
    buf.put_i64(batch.base_offset);
    let batch_len_pos = buf.len();
    buf.put_i32(0);
    buf.put_i32(batch.partition_leader_epoch);
    buf.put_i8(MAGIC_V2);
    let crc_pos = buf.len();
    buf.put_u32(0);
    let crc_start = buf.len();
    buf.put_i16(batch.attributes);
    let last_delta = if batch.records.is_empty() {
        0
    } else {
        batch.records.len() as i32 - 1
    };
    buf.put_i32(last_delta);
    buf.put_i64(batch.base_timestamp);
    buf.put_i64(batch.max_timestamp);
    buf.put_i64(batch.producer_id);
    buf.put_i16(batch.producer_epoch);
    buf.put_i32(batch.base_sequence);
    buf.put_i32(batch.records.len() as i32);
    for (i, rec) in batch.records.iter().enumerate() {
        encode_record(buf, rec, i as i32, rec.timestamp - batch.base_timestamp);
    }
    let end = buf.len();
    let batch_len = (end - batch_len_pos - 4) as i32;
    buf[batch_len_pos..batch_len_pos + 4].copy_from_slice(&batch_len.to_be_bytes());
    let crc = crc32c::crc32c(&buf[crc_start..end]);
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
    debug_assert_eq!(end - batch_start, 12 + batch_len as usize);
    let _ = BATCH_OVERHEAD;
}

fn encode_record(buf: &mut BytesMut, rec: &Record, offset_delta: i32, timestamp_delta: i64) {
    let mut inner = BytesMut::new();
    inner.put_i8(0);
    buf::put_varlong(&mut inner, timestamp_delta);
    buf::put_varint(&mut inner, offset_delta);
    match &rec.key {
        None => buf::put_varint(&mut inner, -1),
        Some(k) => {
            buf::put_varint(&mut inner, k.len() as i32);
            inner.extend_from_slice(k);
        }
    }
    match &rec.value {
        None => buf::put_varint(&mut inner, -1),
        Some(v) => {
            buf::put_varint(&mut inner, v.len() as i32);
            inner.extend_from_slice(v);
        }
    }
    buf::put_varint(&mut inner, rec.headers.len() as i32);
    for h in &rec.headers {
        buf::put_varint(&mut inner, h.key.len() as i32);
        inner.extend_from_slice(h.key.as_bytes());
        match &h.value {
            None => buf::put_varint(&mut inner, -1),
            Some(v) => {
                buf::put_varint(&mut inner, v.len() as i32);
                inner.extend_from_slice(v);
            }
        }
    }
    buf::put_varint(buf, inner.len() as i32);
    buf.extend_from_slice(&inner);
}

pub fn decode_record_batch<B: Buf>(buf: &mut B) -> Result<RecordBatch> {
    let base_offset = buf::get_i64(buf)?;
    let batch_len = buf::get_i32(buf)?;
    if batch_len < 49 {
        return Err(Error::protocol(format!(
            "record batch too small: {batch_len}"
        )));
    }
    buf::need(buf, batch_len as usize)?;
    let mut body = buf.copy_to_bytes(batch_len as usize);
    let partition_leader_epoch = buf::get_i32(&mut body)?;
    let magic = buf::get_i8(&mut body)?;
    if magic != MAGIC_V2 {
        return Err(Error::protocol(format!("unsupported record magic {magic}")));
    }
    let crc = buf::get_u32(&mut body)?;
    let crc_start = body.clone();
    let computed = crc32c::crc32c(&crc_start);
    if computed != crc {
        return Err(Error::protocol(format!(
            "record batch crc mismatch: wire={crc:#010x} computed={computed:#010x}"
        )));
    }
    let attributes = buf::get_i16(&mut body)?;
    if attributes & 0x07 != 0 {
        return Err(Error::protocol(
            "compressed record batches are not implemented",
        ));
    }
    let _last_delta = buf::get_i32(&mut body)?;
    let base_timestamp = buf::get_i64(&mut body)?;
    let max_timestamp = buf::get_i64(&mut body)?;
    let producer_id = buf::get_i64(&mut body)?;
    let producer_epoch = buf::get_i16(&mut body)?;
    let base_sequence = buf::get_i32(&mut body)?;
    let count = buf::get_i32(&mut body)?;
    if count < 0 {
        return Err(Error::protocol("negative record count"));
    }
    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        records.push(decode_record(&mut body, base_timestamp)?);
    }
    Ok(RecordBatch {
        base_offset,
        partition_leader_epoch,
        attributes,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    })
}

fn decode_record<B: Buf>(buf: &mut B, base_timestamp: i64) -> Result<Record> {
    let len = buf::get_varint(buf)?;
    if len < 0 {
        return Err(Error::protocol("negative record length"));
    }
    buf::need(buf, len as usize)?;
    let mut inner = buf.copy_to_bytes(len as usize);
    let _attributes = buf::get_i8(&mut inner)?;
    let timestamp_delta = buf::get_varlong(&mut inner)?;
    let _offset_delta = buf::get_varint(&mut inner)?;
    let key = read_bytes_varint(&mut inner)?;
    let value = read_bytes_varint(&mut inner)?;
    let header_count = buf::get_varint(&mut inner)?;
    if header_count < 0 {
        return Err(Error::protocol("negative header count"));
    }
    let mut headers = Vec::with_capacity(header_count as usize);
    for _ in 0..header_count {
        let key_len = buf::get_varint(&mut inner)?;
        if key_len < 0 {
            return Err(Error::protocol("null header key"));
        }
        buf::need(&inner, key_len as usize)?;
        let mut key_buf = vec![0u8; key_len as usize];
        inner.copy_to_slice(&mut key_buf);
        let key = String::from_utf8(key_buf).map_err(|e| Error::protocol(e.to_string()))?;
        let value = read_bytes_varint(&mut inner)?;
        headers.push(Header { key, value });
    }
    Ok(Record {
        timestamp: base_timestamp + timestamp_delta,
        key,
        value,
        headers,
    })
}

fn read_bytes_varint<B: Buf>(buf: &mut B) -> Result<Option<Bytes>> {
    let len = buf::get_varint(buf)?;
    if len < 0 {
        return Ok(None);
    }
    buf::need(buf, len as usize)?;
    Ok(Some(buf.copy_to_bytes(len as usize)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_batch_roundtrip() {
        let rec = Record {
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"hello")),
            headers: vec![Header {
                key: "h".into(),
                value: Some(Bytes::from_static(b"v")),
            }],
        };
        let batch = RecordBatch::from_records(vec![rec]);
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch);
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert_eq!(decoded, batch);
    }

    #[test]
    fn null_key_is_not_empty_key() {
        let null_key = Record {
            timestamp: 0,
            key: None,
            value: Some(Bytes::from_static(b"v")),
            headers: vec![],
        };
        let empty_key = Record {
            timestamp: 0,
            key: Some(Bytes::new()),
            value: Some(Bytes::from_static(b"v")),
            headers: vec![],
        };
        let mut a = BytesMut::new();
        let mut b = BytesMut::new();
        encode_record_batch(&mut a, &RecordBatch::from_records(vec![null_key.clone()]));
        encode_record_batch(&mut b, &RecordBatch::from_records(vec![empty_key.clone()]));
        assert_ne!(&a[..], &b[..]);
        assert_eq!(
            decode_record_batch(&mut &a[..]).unwrap().records[0].key,
            None
        );
        assert_eq!(
            decode_record_batch(&mut &b[..]).unwrap().records[0].key,
            Some(Bytes::new())
        );
    }
}
