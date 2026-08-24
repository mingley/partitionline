use std::io::{Read, Write};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use crate::error::{Error, Result};

pub const MAGIC_V2: i8 = 2;
const BATCH_OVERHEAD: usize = 61;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i16)]
pub enum Compression {
    #[default]
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
}

impl Compression {
    pub fn from_attributes(attr: i16) -> Result<Self> {
        match attr & 0x07 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            n => Err(Error::protocol(format!("unsupported compression {n}"))),
        }
    }

    pub fn from_name(name: &str) -> Result<Self> {
        match name {
            "none" | "" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            other => Err(Error::protocol(format!("unknown compression {other}"))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: i64,
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
    pub fn from_records(mut records: Vec<Record>) -> Self {
        for (i, rec) in records.iter_mut().enumerate() {
            rec.offset = i as i64;
        }
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

    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.attributes = (self.attributes & !0x07) | (compression as i16);
        self
    }
}

fn gzip_compress(src: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(src)
        .map_err(|e| Error::protocol(e.to_string()))?;
    encoder.finish().map_err(|e| Error::protocol(e.to_string()))
}

fn gzip_decompress(src: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = flate2::read::GzDecoder::new(src);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::protocol(e.to_string()))?;
    Ok(out)
}

/// xerial snappy-java header used by the Java Kafka client.
/// 8 magic + 4 version + 4 compatible, then chunks of [be32 clen][snappy].
const SNAPPY_JAVA_MAGIC: &[u8] = &[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

fn snappy_compress(src: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = snap::raw::Encoder::new();
    let compressed = encoder
        .compress_vec(src)
        .map_err(|e| Error::protocol(e.to_string()))?;
    let mut out = Vec::with_capacity(16 + 4 + compressed.len());
    out.extend_from_slice(SNAPPY_JAVA_MAGIC);
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn snappy_decompress(src: &[u8]) -> Result<Vec<u8>> {
    if src.len() > 20 && src.starts_with(SNAPPY_JAVA_MAGIC) {
        let mut cur = &src[16..];
        let mut out = Vec::new();
        let mut decoder = snap::raw::Decoder::new();
        while cur.len() >= 4 {
            let clen = u32::from_be_bytes(cur[..4].try_into().unwrap()) as usize;
            cur = &cur[4..];
            if clen > cur.len() {
                return Err(Error::protocol("snappy-java chunk overruns buffer"));
            }
            let chunk = decoder
                .decompress_vec(&cur[..clen])
                .map_err(|e| Error::protocol(e.to_string()))?;
            out.extend_from_slice(&chunk);
            cur = &cur[clen..];
        }
        Ok(out)
    } else {
        snap::raw::Decoder::new()
            .decompress_vec(src)
            .map_err(|e| Error::protocol(e.to_string()))
    }
}

/// Kafka RecordBatch (magic ≥ 1) uses LZ4 **frame** with independent blocks.
/// Magic 0 used a broken header checksum; we only emit/accept proper HC.
fn lz4_compress(src: &[u8]) -> Result<Vec<u8>> {
    use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
    let info = FrameInfo::new()
        .block_mode(BlockMode::Independent)
        .block_size(BlockSize::Max64KB)
        .block_checksums(false)
        .content_checksum(false)
        .content_size(Some(src.len() as u64));
    let mut encoder = FrameEncoder::with_frame_info(info, Vec::new());
    encoder
        .write_all(src)
        .map_err(|e| Error::protocol(e.to_string()))?;
    encoder.finish().map_err(|e| Error::protocol(e.to_string()))
}

fn lz4_decompress(src: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = lz4_flex::frame::FrameDecoder::new(src);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::protocol(e.to_string()))?;
    Ok(out)
}

#[derive(Clone, Copy)]
pub struct EncodeRecord<'a> {
    pub timestamp: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [Header],
}

impl<'a> EncodeRecord<'a> {
    pub fn from_record(rec: &'a Record) -> Self {
        Self {
            timestamp: rec.timestamp,
            key: rec.key.as_deref(),
            value: rec.value.as_deref(),
            headers: &rec.headers,
        }
    }
}

pub struct BatchHeader {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub attributes: i16,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub count: i32,
}

impl Default for BatchHeader {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            count: 0,
        }
    }
}

pub fn encode_record_batch(buf: &mut BytesMut, batch: &RecordBatch) -> Result<()> {
    write_record_batch(
        buf,
        &BatchHeader {
            base_offset: batch.base_offset,
            partition_leader_epoch: batch.partition_leader_epoch,
            attributes: batch.attributes,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            count: batch.records.len() as i32,
        },
        batch.records.iter().map(EncodeRecord::from_record),
    )
}

pub fn write_record_batch<'a, I>(buf: &mut BytesMut, header: &BatchHeader, records: I) -> Result<()>
where
    I: Iterator<Item = EncodeRecord<'a>>,
{
    let compression = Compression::from_attributes(header.attributes)?;
    let batch_start = buf.len();
    buf.put_i64(header.base_offset);
    let batch_len_pos = buf.len();
    buf.put_i32(0);
    buf.put_i32(header.partition_leader_epoch);
    buf.put_i8(MAGIC_V2);
    let crc_pos = buf.len();
    buf.put_u32(0);
    let crc_start = buf.len();
    buf.put_i16(header.attributes);
    let last_delta = if header.count <= 0 {
        0
    } else {
        header.count - 1
    };
    buf.put_i32(last_delta);
    buf.put_i64(header.base_timestamp);
    buf.put_i64(header.max_timestamp);
    buf.put_i64(header.producer_id);
    buf.put_i16(header.producer_epoch);
    buf.put_i32(header.base_sequence);
    buf.put_i32(header.count);
    match compression {
        Compression::None => {
            for (i, rec) in records.enumerate() {
                encode_record(buf, &rec, i as i32, rec.timestamp - header.base_timestamp);
            }
        }
        Compression::Gzip | Compression::Snappy | Compression::Lz4 => {
            let mut section = BytesMut::new();
            for (i, rec) in records.enumerate() {
                encode_record(
                    &mut section,
                    &rec,
                    i as i32,
                    rec.timestamp - header.base_timestamp,
                );
            }
            let packed = match compression {
                Compression::Gzip => gzip_compress(&section)?,
                Compression::Snappy => snappy_compress(&section)?,
                Compression::Lz4 => lz4_compress(&section)?,
                Compression::None => unreachable!(),
            };
            buf.extend_from_slice(&packed);
        }
    }
    let end = buf.len();
    let batch_len = (end - batch_len_pos - 4) as i32;
    buf[batch_len_pos..batch_len_pos + 4].copy_from_slice(&batch_len.to_be_bytes());
    let crc = crc32c::crc32c(&buf[crc_start..end]);
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
    debug_assert_eq!(end - batch_start, 12 + batch_len as usize);
    let _ = BATCH_OVERHEAD;
    Ok(())
}

fn nullable_bytes_len(bytes: Option<&[u8]>) -> usize {
    match bytes {
        None => buf::varint_size(-1),
        Some(b) => buf::varint_size(b.len() as i32) + b.len(),
    }
}

fn encode_record(
    buf: &mut BytesMut,
    rec: &EncodeRecord<'_>,
    offset_delta: i32,
    timestamp_delta: i64,
) {
    let mut inner = 1
        + buf::varlong_size(timestamp_delta)
        + buf::varint_size(offset_delta)
        + nullable_bytes_len(rec.key)
        + nullable_bytes_len(rec.value)
        + buf::varint_size(rec.headers.len() as i32);
    for h in rec.headers {
        inner += buf::varint_size(h.key.len() as i32) + h.key.len();
        inner += nullable_bytes_len(h.value.as_deref());
    }
    buf::put_varint(buf, inner as i32);
    buf.put_i8(0);
    buf::put_varlong(buf, timestamp_delta);
    buf::put_varint(buf, offset_delta);
    match rec.key {
        None => buf::put_varint(buf, -1),
        Some(k) => {
            buf::put_varint(buf, k.len() as i32);
            buf.extend_from_slice(k);
        }
    }
    match rec.value {
        None => buf::put_varint(buf, -1),
        Some(v) => {
            buf::put_varint(buf, v.len() as i32);
            buf.extend_from_slice(v);
        }
    }
    buf::put_varint(buf, rec.headers.len() as i32);
    for h in rec.headers {
        buf::put_varint(buf, h.key.len() as i32);
        buf.extend_from_slice(h.key.as_bytes());
        match &h.value {
            None => buf::put_varint(buf, -1),
            Some(v) => {
                buf::put_varint(buf, v.len() as i32);
                buf.extend_from_slice(v);
            }
        }
    }
}

pub fn decode_record_batches<B: Buf>(buf: &mut B) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    while buf.has_remaining() {
        out.push(decode_record_batch(buf)?);
    }
    Ok(out)
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
    let compression = Compression::from_attributes(attributes)?;
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
    let records_bytes = body;
    let decompressed;
    let mut records_cur: &[u8] = match compression {
        Compression::None => &records_bytes,
        Compression::Gzip => {
            decompressed = gzip_decompress(&records_bytes)?;
            &decompressed
        }
        Compression::Snappy => {
            decompressed = snappy_decompress(&records_bytes)?;
            &decompressed
        }
        Compression::Lz4 => {
            decompressed = lz4_decompress(&records_bytes)?;
            &decompressed
        }
    };
    let mut records = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut rec = decode_record(&mut records_cur, base_timestamp)?;
        rec.offset = base_offset + i as i64;
        records.push(rec);
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
        offset: 0,
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
            offset: 0,
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
        encode_record_batch(&mut buf, &batch).unwrap();
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert_eq!(decoded, batch);
    }

    #[test]
    fn null_key_is_not_empty_key() {
        let null_key = Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from_static(b"v")),
            headers: vec![],
        };
        let empty_key = Record {
            offset: 0,
            timestamp: 0,
            key: Some(Bytes::new()),
            value: Some(Bytes::from_static(b"v")),
            headers: vec![],
        };
        let mut a = BytesMut::new();
        let mut b = BytesMut::new();
        encode_record_batch(&mut a, &RecordBatch::from_records(vec![null_key.clone()])).unwrap();
        encode_record_batch(&mut b, &RecordBatch::from_records(vec![empty_key.clone()])).unwrap();
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

    #[test]
    fn gzip_record_batch_roundtrip() {
        let rec = Record {
            offset: 0,
            timestamp: 9,
            key: None,
            value: Some(Bytes::from_static(b"gzip-payload")),
            headers: vec![],
        };
        let batch = RecordBatch::from_records(vec![rec]).with_compression(Compression::Gzip);
        assert_eq!(batch.attributes & 0x07, Compression::Gzip as i16);
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch).unwrap();
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert_eq!(
            decoded.records[0].value.as_deref(),
            Some(&b"gzip-payload"[..])
        );
        assert_eq!(decoded.attributes & 0x07, Compression::Gzip as i16);
    }

    #[test]
    fn snappy_record_batch_roundtrip() {
        let rec = Record {
            offset: 0,
            timestamp: 11,
            key: None,
            value: Some(Bytes::from_static(b"snappy-payload")),
            headers: vec![],
        };
        let batch = RecordBatch::from_records(vec![rec]).with_compression(Compression::Snappy);
        assert_eq!(batch.attributes & 0x07, Compression::Snappy as i16);
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch).unwrap();
        assert!(
            buf.windows(SNAPPY_JAVA_MAGIC.len())
                .any(|w| w == SNAPPY_JAVA_MAGIC),
            "produce path must emit snappy-java framing"
        );
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert_eq!(
            decoded.records[0].value.as_deref(),
            Some(&b"snappy-payload"[..])
        );
        assert_eq!(decoded.attributes & 0x07, Compression::Snappy as i16);
    }

    #[test]
    fn snappy_decompress_raw_block_from_librdkafka() {
        let payload = b"raw-snappy-from-c-client";
        let raw = snap::raw::Encoder::new().compress_vec(payload).unwrap();
        assert_eq!(snappy_decompress(&raw).unwrap(), payload);
        let framed = snappy_compress(payload).unwrap();
        assert!(framed.starts_with(SNAPPY_JAVA_MAGIC));
        assert_eq!(snappy_decompress(&framed).unwrap(), payload);
    }

    #[test]
    fn lz4_record_batch_roundtrip() {
        let rec = Record {
            offset: 0,
            timestamp: 13,
            key: None,
            value: Some(Bytes::from_static(b"lz4-payload")),
            headers: vec![],
        };
        let batch = RecordBatch::from_records(vec![rec]).with_compression(Compression::Lz4);
        assert_eq!(batch.attributes & 0x07, Compression::Lz4 as i16);
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch).unwrap();
        assert!(
            buf.windows(4).any(|w| w == [0x04, 0x22, 0x4d, 0x18]),
            "produce path must emit LZ4 frame magic"
        );
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert_eq!(
            decoded.records[0].value.as_deref(),
            Some(&b"lz4-payload"[..])
        );
        assert_eq!(decoded.attributes & 0x07, Compression::Lz4 as i16);
    }

    #[test]
    fn lz4_frame_roundtrip_shipped_codec() {
        let payload = vec![b'x'; 4096];
        let framed = lz4_compress(&payload).unwrap();
        assert_eq!(&framed[..4], &[0x04, 0x22, 0x4d, 0x18]);
        assert_eq!(lz4_decompress(&framed).unwrap(), payload);
    }
}
