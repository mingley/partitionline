//! Magic-v2 RecordBatch codec (gzip, snappy, lz4).

use std::io::{Read, Write};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Record batch magic for Kafka 0.11+ (v2).
pub const MAGIC_V2: i8 = 2;
const _BATCH_OVERHEAD: usize = 61;

/// Kafka record-batch compression codec.
///
/// zstd is not implemented (the usual ecosystem codec is C).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i16)]
pub enum Compression {
    /// Uncompressed.
    #[default]
    None = 0,
    /// gzip (`flate2` Rust backend).
    Gzip = 1,
    /// Snappy.
    Snappy = 2,
    /// LZ4 frame.
    Lz4 = 3,
}

impl Compression {
    /// Codec from the low 3 bits of batch attributes.
    pub fn from_attributes(attr: i16) -> Result<Self> {
        match attr & 0x07 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            n => Err(Error::protocol(format!("unsupported compression {n}"))),
        }
    }

    /// `none` / `gzip` / `snappy` / `lz4`.
    pub fn from_name(name: &str) -> Result<Self> {
        match name {
            "none" | "" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            other => Err(Error::protocol(format!("unknown compression {other}"))),
        }
    }

    /// Config name for this codec.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
        }
    }
}

/// One Kafka record header (`RecordHeader`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Header key.
    pub key: String,
    /// Header value, or `None` when the wire value is null.
    pub value: Option<Bytes>,
}

impl Header {
    /// Header with a non-null value.
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }

    /// Header with a null value (Java `RecordHeader(key, null)`).
    #[must_use]
    pub fn null(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }

    /// Java `RecordHeader.key`.
    #[must_use]
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Java `RecordHeader.value` (`None` is Java `null`).
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

/// One record inside a magic-v2 batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Log offset after decode; relative to the batch base when building with
    /// [`RecordBatch::from_records`].
    pub offset: i64,
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// Optional key.
    pub key: Option<Bytes>,
    /// Optional value.
    pub value: Option<Bytes>,
    /// Record headers.
    pub headers: Vec<Header>,
}

/// RecordBatch attributes: transactional.
pub const ATTR_TRANSACTIONAL: i16 = 0x10;
/// RecordBatch attributes: control batch (commit/abort marker).
pub const ATTR_CONTROL: i16 = 0x20;

/// Magic-v2 record batch (Kafka 0.11+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    /// First offset in this batch.
    pub base_offset: i64,
    /// Partition leader epoch, or `-1` when unknown.
    pub partition_leader_epoch: i32,
    /// Attribute bits: compression in the low 3, plus
    /// [`ATTR_TRANSACTIONAL`] / [`ATTR_CONTROL`].
    pub attributes: i16,
    /// Timestamp of the first record (milliseconds since the Unix epoch).
    pub base_timestamp: i64,
    /// Max timestamp among records in this batch.
    pub max_timestamp: i64,
    /// Idempotent / transactional producer id, or `-1`.
    pub producer_id: i64,
    /// Producer epoch, or `-1`.
    pub producer_epoch: i16,
    /// First sequence number in this batch, or `-1`.
    pub base_sequence: i32,
    /// Records in this batch.
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// Build a batch from records. Offsets become `0..n`; timestamps set
    /// `base_timestamp` / `max_timestamp`. Producer id / epoch / sequence
    /// stay `-1`.
    pub fn from_records(mut records: Vec<Record>) -> Self {
        for (i, rec) in records.iter_mut().enumerate() {
            rec.offset = i64::try_from(i).unwrap_or(i64::MAX);
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

    /// Set the compression bits in [`Self::attributes`].
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
    let _n = decoder
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
    out.extend_from_slice(&buf::u32_from_usize(compressed.len())?.to_be_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn snappy_decompress(src: &[u8]) -> Result<Vec<u8>> {
    if src.len() > 20 && src.starts_with(SNAPPY_JAVA_MAGIC) {
        let mut cur = src.get(16..).unwrap_or(&[]);
        let mut out = Vec::new();
        let mut decoder = snap::raw::Decoder::new();
        while cur.len() >= 4 {
            let prefix = cur
                .get(..4)
                .ok_or_else(|| Error::protocol("snappy-java short chunk"))?;
            let clen = buf::usize_from_u32(u32::from_be_bytes(
                prefix
                    .try_into()
                    .map_err(|_| Error::protocol("snappy-java short chunk"))?,
            ))?;
            cur = cur.get(4..).unwrap_or(&[]);
            if clen > cur.len() {
                return Err(Error::protocol("snappy-java chunk overruns buffer"));
            }
            let block = cur
                .get(..clen)
                .ok_or_else(|| Error::protocol("snappy-java chunk overruns buffer"))?;
            let chunk = decoder
                .decompress_vec(block)
                .map_err(|e| Error::protocol(e.to_string()))?;
            out.extend_from_slice(&chunk);
            cur = cur.get(clen..).unwrap_or(&[]);
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
    let _n = decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::protocol(e.to_string()))?;
    Ok(out)
}

/// One record as borrowed slices for the produce encode path.
#[derive(Clone, Copy)]
pub struct EncodeRecord<'a> {
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// Optional key.
    pub key: Option<&'a [u8]>,
    /// Optional value.
    pub value: Option<&'a [u8]>,
    /// Record headers.
    pub headers: &'a [Header],
}

impl<'a> EncodeRecord<'a> {
    /// Borrow a [`Record`] for encoding.
    pub fn from_record(rec: &'a Record) -> Self {
        Self {
            timestamp: rec.timestamp,
            key: rec.key.as_deref(),
            value: rec.value.as_deref(),
            headers: &rec.headers,
        }
    }
}

/// Magic-v2 batch header fields used by [`write_record_batch`].
pub struct BatchHeader {
    /// First offset in this batch.
    pub base_offset: i64,
    /// Partition leader epoch, or `-1` when unknown.
    pub partition_leader_epoch: i32,
    /// Attribute bits: compression in the low 3, plus
    /// [`ATTR_TRANSACTIONAL`] / [`ATTR_CONTROL`].
    pub attributes: i16,
    /// Timestamp of the first record (milliseconds since the Unix epoch).
    pub base_timestamp: i64,
    /// Max timestamp among records in this batch.
    pub max_timestamp: i64,
    /// Idempotent / transactional producer id, or `-1`.
    pub producer_id: i64,
    /// Producer epoch, or `-1`.
    pub producer_epoch: i16,
    /// First sequence number in this batch, or `-1`.
    pub base_sequence: i32,
    /// Number of records that will be written.
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

/// Encode a [`RecordBatch`] (CRC32-C, optional compression).
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
            count: buf::i32_from_usize(batch.records.len())?,
        },
        batch.records.iter().map(EncodeRecord::from_record),
    )
}

/// Encode a magic-v2 batch from a header plus borrowed records (produce hot path).
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
                encode_record(
                    buf,
                    &rec,
                    buf::i32_from_usize(i)?,
                    rec.timestamp - header.base_timestamp,
                )?;
            }
        }
        Compression::Gzip | Compression::Snappy | Compression::Lz4 => {
            let mut section = BytesMut::new();
            for (i, rec) in records.enumerate() {
                encode_record(
                    &mut section,
                    &rec,
                    buf::i32_from_usize(i)?,
                    rec.timestamp - header.base_timestamp,
                )?;
            }
            let packed = match compression {
                Compression::Gzip => gzip_compress(&section)?,
                Compression::Snappy => snappy_compress(&section)?,
                Compression::Lz4 => lz4_compress(&section)?,
                Compression::None => {
                    return Err(Error::protocol("internal: none after compressed branch"));
                }
            };
            buf.extend_from_slice(&packed);
        }
    }
    let end = buf.len();
    let batch_len = buf::i32_from_usize(end.saturating_sub(batch_len_pos + 4))?;
    buf::patch_i32(buf, batch_len_pos, batch_len)?;
    let crc_bytes = buf
        .get(crc_start..end)
        .ok_or_else(|| Error::protocol("short crc range"))?;
    let crc = crc32c::crc32c(crc_bytes);
    buf::patch_i32(buf, crc_pos, i32::from_be_bytes(crc.to_be_bytes()))?;
    debug_assert_eq!(
        end.saturating_sub(batch_start),
        12 + buf::usize_from_i32(batch_len).unwrap_or(0)
    );
    Ok(())
}

fn nullable_bytes_len(bytes: Option<&[u8]>) -> usize {
    match bytes {
        None => buf::varint_size(-1),
        Some(b) => buf::varint_size(i32::try_from(b.len()).unwrap_or(i32::MAX)) + b.len(),
    }
}

fn encode_record(
    buf: &mut BytesMut,
    rec: &EncodeRecord<'_>,
    offset_delta: i32,
    timestamp_delta: i64,
) -> crate::error::Result<()> {
    let mut inner = 1
        + buf::varlong_size(timestamp_delta)
        + buf::varint_size(offset_delta)
        + nullable_bytes_len(rec.key)
        + nullable_bytes_len(rec.value)
        + buf::varint_size(buf::i32_from_usize(rec.headers.len())?);
    for h in rec.headers {
        inner += buf::varint_size(buf::i32_from_usize(h.key.len())?) + h.key.len();
        inner += nullable_bytes_len(h.value.as_deref());
    }
    buf::put_varint(buf, buf::i32_from_usize(inner)?);
    buf.put_i8(0);
    buf::put_varlong(buf, timestamp_delta);
    buf::put_varint(buf, offset_delta);
    match rec.key {
        None => buf::put_varint(buf, -1),
        Some(k) => {
            buf::put_varint(buf, buf::i32_from_usize(k.len())?);
            buf.extend_from_slice(k);
        }
    }
    match rec.value {
        None => buf::put_varint(buf, -1),
        Some(v) => {
            buf::put_varint(buf, buf::i32_from_usize(v.len())?);
            buf.extend_from_slice(v);
        }
    }
    buf::put_varint(buf, buf::i32_from_usize(rec.headers.len())?);
    for h in rec.headers {
        buf::put_varint(buf, buf::i32_from_usize(h.key.len())?);
        buf.extend_from_slice(h.key.as_bytes());
        match &h.value {
            None => buf::put_varint(buf, -1),
            Some(v) => {
                buf::put_varint(buf, buf::i32_from_usize(v.len())?);
                buf.extend_from_slice(v);
            }
        }
    }
    Ok(())
}

/// Decode zero or more consecutive magic-v2 batches until the buffer is too short.
pub fn decode_record_batches<B: Buf>(buf: &mut B) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    while buf.remaining() >= 12 {
        let chunk = buf.chunk();
        if chunk.len() < 12 {
            let mut rest = buf.copy_to_bytes(buf.remaining());
            out.append(&mut decode_record_batches(&mut rest)?);
            break;
        }
        let len_bytes = chunk
            .get(8..12)
            .ok_or_else(|| Error::protocol("short batch length"))?;
        let batch_len = i32::from_be_bytes(
            len_bytes
                .try_into()
                .map_err(|_| Error::protocol("short batch length"))?,
        );
        if batch_len < 0 {
            return Err(Error::protocol("negative record batch length"));
        }
        let need = 12usize.saturating_add(buf::usize_from_i32(batch_len)?);
        if buf.remaining() < need {
            break;
        }
        out.push(decode_record_batch(buf)?);
    }
    Ok(out)
}

/// Decode one magic-v2 batch (CRC32-C checked).
pub fn decode_record_batch<B: Buf>(buf: &mut B) -> Result<RecordBatch> {
    let base_offset = buf::get_i64(buf)?;
    let batch_len = buf::get_i32(buf)?;
    if batch_len < 49 {
        return Err(Error::protocol(format!(
            "record batch too small: {batch_len}"
        )));
    }
    let batch_len_usize = buf::usize_from_i32(batch_len)?;
    buf::need(buf, batch_len_usize)?;
    let mut body = buf.copy_to_bytes(batch_len_usize);
    let partition_leader_epoch = buf::get_i32(&mut body)?;
    let magic = buf::get_i8(&mut body)?;
    if magic != MAGIC_V2 {
        return Err(Error::protocol(format!("unsupported record magic {magic}")));
    }
    let crc = buf::get_u32(&mut body)?;
    let computed = crc32c::crc32c(&body);
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
    let mut records_cur = match compression {
        Compression::None => body,
        Compression::Gzip => Bytes::from(gzip_decompress(&body)?),
        Compression::Snappy => Bytes::from(snappy_decompress(&body)?),
        Compression::Lz4 => Bytes::from(lz4_decompress(&body)?),
    };
    let mut records = Vec::with_capacity(buf::usize_from_i32(count.max(0))?);
    for _ in 0..count {
        records.push(decode_record(
            &mut records_cur,
            base_offset,
            base_timestamp,
        )?);
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

fn decode_record<B: Buf>(buf: &mut B, base_offset: i64, base_timestamp: i64) -> Result<Record> {
    let len = buf::get_varint(buf)?;
    if len < 0 {
        return Err(Error::protocol("negative record length"));
    }
    let len_usize = buf::usize_from_i32(len)?;
    buf::need(buf, len_usize)?;
    let mut inner = buf.copy_to_bytes(len_usize);
    let _attributes = buf::get_i8(&mut inner)?;
    let timestamp_delta = buf::get_varlong(&mut inner)?;
    let offset_delta = buf::get_varint(&mut inner)?;
    let key = read_bytes_varint(&mut inner)?;
    let value = read_bytes_varint(&mut inner)?;
    let header_count = buf::get_varint(&mut inner)?;
    if header_count < 0 {
        return Err(Error::protocol("negative header count"));
    }
    let mut headers = Vec::with_capacity(buf::usize_from_i32(header_count)?);
    for _ in 0..header_count {
        let key_len = buf::get_varint(&mut inner)?;
        if key_len < 0 {
            return Err(Error::protocol("null header key"));
        }
        let key_len_usize = buf::usize_from_i32(key_len)?;
        buf::need(&inner, key_len_usize)?;
        let mut key_buf = vec![0u8; key_len_usize];
        inner.copy_to_slice(&mut key_buf);
        let key = String::from_utf8(key_buf).map_err(|e| Error::protocol(e.to_string()))?;
        let value = read_bytes_varint(&mut inner)?;
        headers.push(Header { key, value });
    }
    Ok(Record {
        offset: base_offset + i64::from(offset_delta),
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
    let len_usize = buf::usize_from_i32(len)?;
    buf::need(buf, len_usize)?;
    Ok(Some(buf.copy_to_bytes(len_usize)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_getters_match_java() {
        let h = Header::new("k", Bytes::from_static(b"v"));
        assert_eq!(h.key(), "k");
        assert_eq!(h.value(), Some(&b"v"[..]));
        let n = Header::null("n");
        assert_eq!(n.key(), "n");
        assert!(n.value().is_none());
    }

    #[test]
    fn idempotent_batch_preserves_pid_and_seq() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"idem")),
            headers: vec![],
        };
        let mut batch = RecordBatch::from_records(vec![rec]);
        batch.producer_id = 99;
        batch.producer_epoch = 2;
        batch.base_sequence = 17;
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch).unwrap();
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert_eq!(decoded.producer_id, 99);
        assert_eq!(decoded.producer_epoch, 2);
        assert_eq!(decoded.base_sequence, 17);
        assert_eq!(decoded.records[0].value.as_deref(), Some(&b"idem"[..]));
    }

    #[test]
    fn record_batch_roundtrip() {
        let rec = Record {
            offset: 0,
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"hello")),
            headers: vec![
                Header::new("h", Bytes::from_static(b"v")),
                Header::null("empty"),
            ],
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

    fn one_batch(value: &'static [u8]) -> BytesMut {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(value)),
            headers: vec![],
        };
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &RecordBatch::from_records(vec![rec])).unwrap();
        buf
    }

    #[test]
    fn decode_record_batches_stops_at_truncated_tail() {
        let mut raw = one_batch(b"one");
        raw.extend_from_slice(&one_batch(b"two"));
        raw.extend_from_slice(&[0u8; 8]);
        let batches = decode_record_batches(&mut &raw[..]).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].records[0].value.as_deref(), Some(&b"one"[..]));
        assert_eq!(batches[1].records[0].value.as_deref(), Some(&b"two"[..]));
    }

    fn ptr_in_bytes(ptr: *const u8, buf: &Bytes) -> bool {
        let start = buf.as_ptr();
        let end = start.wrapping_add(buf.len());
        ptr >= start && ptr < end
    }

    #[test]
    fn decode_record_batch_from_bytes_shares_value() {
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"zero-copy-value")),
            headers: vec![],
        };
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &RecordBatch::from_records(vec![rec])).unwrap();
        let frozen = buf.freeze();
        let decoded = decode_record_batch(&mut frozen.clone()).unwrap();
        let key = decoded.records[0].key.as_ref().unwrap();
        let value = decoded.records[0].value.as_ref().unwrap();
        assert!(
            ptr_in_bytes(key.as_ptr(), &frozen),
            "key must be a view into the fetch frame"
        );
        assert!(
            ptr_in_bytes(value.as_ptr(), &frozen),
            "value must be a view into the fetch frame"
        );
    }

    #[test]
    fn decode_record_uses_wire_offset_delta() {
        let mut inner = BytesMut::new();
        inner.put_i8(0);
        buf::put_varlong(&mut inner, 3);
        buf::put_varint(&mut inner, 7);
        buf::put_varint(&mut inner, -1);
        buf::put_varint(&mut inner, 1);
        inner.extend_from_slice(b"x");
        buf::put_varint(&mut inner, 0);
        let mut rec = BytesMut::new();
        buf::put_varint(&mut rec, buf::i32_from_usize(inner.len()).unwrap());
        rec.extend_from_slice(&inner);
        let decoded = decode_record(&mut &rec[..], 100, 1_000).unwrap();
        assert_eq!(decoded.offset, 107);
        assert_eq!(decoded.timestamp, 1_003);
        assert_eq!(decoded.value.as_deref(), Some(&b"x"[..]));
    }

    #[test]
    fn decode_record_batch_honors_base_offset_plus_delta() {
        let rec = Record {
            offset: 0,
            timestamp: 5,
            key: None,
            value: Some(Bytes::from_static(b"off")),
            headers: vec![],
        };
        let mut batch = RecordBatch::from_records(vec![rec.clone(), rec]);
        batch.base_offset = 50;
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch).unwrap();
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert_eq!(decoded.records[0].offset, 50);
        assert_eq!(decoded.records[1].offset, 51);
    }
}
