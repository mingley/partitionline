//! Magic-v2 RecordBatch codec (gzip, snappy, lz4).

use std::fmt;
use std::io::{Read, Write};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Record batch magic for Kafka 0.11+ (v2).
pub const MAGIC_V2: i8 = 2;

/// Kafka record-batch compression codec.
///
/// zstd is not implemented (the usual ecosystem codec is C).
///
/// [`Display`] is Java `CompressionType.toString` (`gzip`).
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

    /// Java `CompressionType.id`.
    #[must_use]
    pub fn id(self) -> i8 {
        match self {
            Self::None => 0,
            Self::Gzip => 1,
            Self::Snappy => 2,
            Self::Lz4 => 3,
        }
    }

    /// Java `CompressionType.forId`. Unknown ids (including zstd `4`) return
    /// `None`.
    #[must_use]
    pub fn from_id(id: i32) -> Option<Self> {
        match id {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            _ => None,
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

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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

    /// Last header whose key is `key` (Java `Headers.lastHeader`).
    #[must_use]
    pub fn last_in<'a>(headers: &'a [Self], key: &str) -> Option<&'a Self> {
        headers.iter().rev().find(|h| h.key() == key)
    }

    /// Headers whose key is `key`, in insertion order (Java `Headers.headers(String)`).
    pub fn for_key<'a>(headers: &'a [Self], key: &'a str) -> impl Iterator<Item = &'a Self> + 'a {
        headers.iter().filter(move |h| h.key() == key)
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordHeader(key = {}, value = ", self.key())?;
        write_java_byte_array(f, self.value())?;
        f.write_str(")")
    }
}

fn write_java_byte_array(f: &mut fmt::Formatter<'_>, value: Option<&[u8]>) -> fmt::Result {
    let Some(bytes) = value else {
        return f.write_str("null");
    };
    f.write_str("[")?;
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{}", i8::from_ne_bytes([*b]))?;
    }
    f.write_str("]")
}

pub(crate) fn write_java_optional_bytes(
    f: &mut fmt::Formatter<'_>,
    bytes: Option<&[u8]>,
) -> fmt::Result {
    match bytes {
        None => f.write_str("null"),
        Some(b) => match std::str::from_utf8(b) {
            Ok(s) => f.write_str(s),
            Err(_) => write_java_byte_array(f, Some(b)),
        },
    }
}

pub(crate) fn write_java_record_headers(
    f: &mut fmt::Formatter<'_>,
    headers: &[Header],
    read_only: bool,
) -> fmt::Result {
    f.write_str("RecordHeaders(headers = [")?;
    for (i, h) in headers.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{h}")?;
    }
    write!(f, "], isReadOnly = {read_only})")
}

pub(crate) fn write_java_optional<T: fmt::Display>(
    f: &mut fmt::Formatter<'_>,
    v: Option<T>,
) -> fmt::Result {
    match v {
        Some(n) => write!(f, "{n}"),
        None => f.write_str("null"),
    }
}

/// One record inside a magic-v2 batch.
///
/// [`Display`] is Java `DefaultRecord.toString` (`key=N bytes`; null is
/// `0 bytes`, not `keySize` `-1`).
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

impl Record {
    /// Java `Record.EMPTY_HEADERS`.
    pub const EMPTY_HEADERS: &'static [Header] = &[];
    /// Java `DefaultRecord.MAX_RECORD_OVERHEAD` (max bytes excluding key, value, and headers).
    pub const MAX_RECORD_OVERHEAD: i32 = 21;

    /// Java `Record.offset`.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Java `Record.timestamp`.
    #[must_use]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Java `Record.key` (`None` is Java `null`).
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Java `Record.hasKey`.
    #[must_use]
    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    /// Java `Record.value` (`None` is Java `null`).
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Java `Record.hasValue`.
    #[must_use]
    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    /// Java `Record.keySize` (`-1` when there is no key).
    #[must_use]
    pub fn key_size(&self) -> i32 {
        self.key
            .as_ref()
            .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
            .unwrap_or(-1)
    }

    /// Java `Record.valueSize` (`-1` when the value is null).
    #[must_use]
    pub fn value_size(&self) -> i32 {
        self.value
            .as_ref()
            .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
            .unwrap_or(-1)
    }

    /// Java `Record.headers`.
    #[must_use]
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    /// Java `Headers.lastHeader`.
    #[must_use]
    pub fn last_header(&self, key: &str) -> Option<&Header> {
        Header::last_in(&self.headers, key)
    }

    /// Java `Headers.headers(String)`.
    pub fn headers_for_key<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a Header> + 'a {
        Header::for_key(&self.headers, key)
    }
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DefaultRecord(offset=")?;
        write!(f, "{}", self.offset())?;
        f.write_str(", timestamp=")?;
        write!(f, "{}", self.timestamp())?;
        f.write_str(", key=")?;
        write!(
            f,
            "{} bytes, value=",
            self.key.as_ref().map_or(0, Bytes::len)
        )?;
        write!(f, "{} bytes)", self.value.as_ref().map_or(0, Bytes::len))
    }
}

/// RecordBatch attributes: transactional.
pub const ATTR_TRANSACTIONAL: i16 = 0x10;
/// RecordBatch attributes: control batch (commit/abort marker).
pub const ATTR_CONTROL: i16 = 0x20;
/// RecordBatch attributes: delete horizon. When set,
/// [`RecordBatch::delete_horizon_ms`] is [`RecordBatch::base_timestamp`].
pub const ATTR_DELETE_HORIZON: i16 = 0x40;
/// RecordBatch attributes bit 3: [`TimestampType::LogAppendTime`] when set,
/// [`TimestampType::CreateTime`] when clear (Java `TIMESTAMP_TYPE_MASK`).
pub const ATTR_TIMESTAMP_TYPE: i16 = 0x08;

/// Kafka record timestamp type (Java `TimestampType`).
///
/// Magic-v2 batches encode [`Self::CreateTime`] vs [`Self::LogAppendTime`] in
/// [`ATTR_TIMESTAMP_TYPE`]. [`Self::NoTimestampType`] is the legacy magic-v0
/// value; this crate does not produce it on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i8)]
pub enum TimestampType {
    /// Java `NO_TIMESTAMP_TYPE` (records without timestamps).
    NoTimestampType = -1,
    /// Java `CREATE_TIME` (producer timestamp).
    #[default]
    CreateTime = 0,
    /// Java `LOG_APPEND_TIME` (broker append time).
    LogAppendTime = 1,
}

impl TimestampType {
    /// Java `TimestampType.id`.
    #[must_use]
    pub fn id(self) -> i32 {
        i32::from(self as i8)
    }

    /// Java `TimestampType.name`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoTimestampType => "NoTimestampType",
            Self::CreateTime => "CreateTime",
            Self::LogAppendTime => "LogAppendTime",
        }
    }

    /// Parse Java `TimestampType.id`. Unknown values return `None`.
    #[must_use]
    pub fn from_id(id: i32) -> Option<Self> {
        match id {
            -1 => Some(Self::NoTimestampType),
            0 => Some(Self::CreateTime),
            1 => Some(Self::LogAppendTime),
            _ => None,
        }
    }

    /// Java `TimestampType.forName`.
    pub fn from_name(name: &str) -> Result<Self> {
        match name {
            "NoTimestampType" => Ok(Self::NoTimestampType),
            "CreateTime" => Ok(Self::CreateTime),
            "LogAppendTime" => Ok(Self::LogAppendTime),
            other => Err(Error::protocol(format!("unknown timestamp type {other}"))),
        }
    }

    /// Magic-v2 batch attributes: bit 3 set is [`Self::LogAppendTime`].
    #[must_use]
    pub fn from_attributes(attr: i16) -> Self {
        if attr & ATTR_TIMESTAMP_TYPE == 0 {
            Self::CreateTime
        } else {
            Self::LogAppendTime
        }
    }
}

impl std::fmt::Display for TimestampType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Magic-v2 record batch (Kafka 0.11+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    /// First offset in this batch.
    pub base_offset: i64,
    /// Partition leader epoch, or `-1` when unknown.
    pub partition_leader_epoch: i32,
    /// Attribute bits: compression in the low 3, plus
    /// [`ATTR_TIMESTAMP_TYPE`] / [`ATTR_TRANSACTIONAL`] / [`ATTR_CONTROL`] /
    /// [`ATTR_DELETE_HORIZON`].
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
    /// Java `RecordBatch.NO_TIMESTAMP`.
    pub const NO_TIMESTAMP: i64 = -1;
    /// Java `RecordBatch.NO_PRODUCER_ID`.
    pub const NO_PRODUCER_ID: i64 = -1;
    /// Java `RecordBatch.NO_PRODUCER_EPOCH`.
    pub const NO_PRODUCER_EPOCH: i16 = -1;
    /// Java `RecordBatch.NO_SEQUENCE`.
    pub const NO_SEQUENCE: i32 = -1;
    /// Java `RecordBatch.NO_PARTITION_LEADER_EPOCH`.
    pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;
    /// Java `RecordBatch.MAGIC_VALUE_V2`. This crate speaks magic-v2 only.
    pub const MAGIC_VALUE_V2: i8 = MAGIC_V2;
    /// Java `RecordBatch.CURRENT_MAGIC_VALUE` ([`Self::MAGIC_VALUE_V2`]).
    pub const CURRENT_MAGIC_VALUE: i8 = Self::MAGIC_VALUE_V2;
    /// Java `DefaultRecordBatch.RECORD_BATCH_OVERHEAD` (bytes before the records).
    pub const RECORD_BATCH_OVERHEAD: i32 = 61;

    /// Build a batch from records. Offsets become `0..n`; timestamps set
    /// `base_timestamp` / `max_timestamp`. Producer id / epoch / sequence
    /// stay [`Self::NO_PRODUCER_ID`].
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
            partition_leader_epoch: Self::NO_PARTITION_LEADER_EPOCH,
            attributes: 0,
            base_timestamp,
            max_timestamp,
            producer_id: Self::NO_PRODUCER_ID,
            producer_epoch: Self::NO_PRODUCER_EPOCH,
            base_sequence: Self::NO_SEQUENCE,
            records,
        }
    }

    /// Set the compression bits in [`Self::attributes`].
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.attributes = (self.attributes & !0x07) | (compression as i16);
        self
    }

    /// Java `DefaultRecordBatch.timestampType` from [`Self::attributes`].
    #[must_use]
    pub fn timestamp_type(&self) -> TimestampType {
        TimestampType::from_attributes(self.attributes)
    }

    /// Set [`ATTR_TIMESTAMP_TYPE`] for [`TimestampType::LogAppendTime`].
    ///
    /// [`TimestampType::NoTimestampType`] is encoded as
    /// [`TimestampType::CreateTime`] (magic-v2 has no "no timestamp" bit).
    #[must_use]
    pub fn with_timestamp_type(mut self, timestamp_type: TimestampType) -> Self {
        match timestamp_type {
            TimestampType::LogAppendTime => self.attributes |= ATTR_TIMESTAMP_TYPE,
            TimestampType::CreateTime | TimestampType::NoTimestampType => {
                self.attributes &= !ATTR_TIMESTAMP_TYPE;
            }
        }
        self
    }

    /// Java `DefaultRecordBatch.isTransactional`.
    #[must_use]
    pub fn is_transactional(&self) -> bool {
        self.attributes & ATTR_TRANSACTIONAL != 0
    }

    /// Java `DefaultRecordBatch.isControlBatch`.
    #[must_use]
    pub fn is_control_batch(&self) -> bool {
        self.attributes & ATTR_CONTROL != 0
    }

    /// Set [`ATTR_TRANSACTIONAL`].
    #[must_use]
    pub fn with_transactional(mut self, transactional: bool) -> Self {
        if transactional {
            self.attributes |= ATTR_TRANSACTIONAL;
        } else {
            self.attributes &= !ATTR_TRANSACTIONAL;
        }
        self
    }

    /// Set [`ATTR_CONTROL`].
    #[must_use]
    pub fn with_control_batch(mut self, control: bool) -> Self {
        if control {
            self.attributes |= ATTR_CONTROL;
        } else {
            self.attributes &= !ATTR_CONTROL;
        }
        self
    }

    /// Java `DefaultRecordBatch.magic` (this crate speaks magic-v2 only).
    #[must_use]
    pub fn magic(&self) -> i8 {
        Self::CURRENT_MAGIC_VALUE
    }

    /// Java `DefaultRecordBatch.baseOffset`.
    #[must_use]
    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    /// Java `DefaultRecordBatch.lastOffset` (`baseOffset + count - 1`).
    #[must_use]
    pub fn last_offset(&self) -> i64 {
        self.base_offset.saturating_add(self.record_count_i64() - 1)
    }

    /// Java `DefaultRecordBatch.nextOffset` (`lastOffset + 1`).
    #[must_use]
    pub fn next_offset(&self) -> i64 {
        self.base_offset.saturating_add(self.record_count_i64())
    }

    /// Java `DefaultRecordBatch.count`.
    #[must_use]
    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// Records in this batch.
    #[must_use]
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Java `DefaultRecordBatch.partitionLeaderEpoch` (`-1` when unknown).
    #[must_use]
    pub fn partition_leader_epoch(&self) -> i32 {
        self.partition_leader_epoch
    }

    /// Java `DefaultRecordBatch.baseTimestamp`.
    #[must_use]
    pub fn base_timestamp(&self) -> i64 {
        self.base_timestamp
    }

    /// Java `DefaultRecordBatch.maxTimestamp`.
    #[must_use]
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// Java `DefaultRecordBatch.producerId` (`-1` when none).
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Java `DefaultRecordBatch.hasProducerId` (`producerId` is not
    /// [`Self::NO_PRODUCER_ID`]).
    #[must_use]
    pub fn has_producer_id(&self) -> bool {
        self.producer_id >= 0
    }

    /// Java `DefaultRecordBatch.producerEpoch` (`-1` when none).
    #[must_use]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Java `DefaultRecordBatch.baseSequence` (`-1` when none).
    #[must_use]
    pub fn base_sequence(&self) -> i32 {
        self.base_sequence
    }

    /// Java `DefaultRecordBatch.lastSequence`.
    ///
    /// [`Self::NO_SEQUENCE`] when [`Self::base_sequence`] is unset. Otherwise
    /// [`Self::increment_sequence`] of the base and last-offset delta
    /// (`count - 1`, or `0` when empty — same as the encode path).
    #[must_use]
    pub fn last_sequence(&self) -> i32 {
        if self.base_sequence == Self::NO_SEQUENCE {
            return Self::NO_SEQUENCE;
        }
        Self::increment_sequence(self.base_sequence, self.last_offset_delta())
    }

    /// Java `DefaultRecordBatch.incrementSequence` (wraps past [`i32::MAX`]
    /// to `0`).
    #[must_use]
    pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
        if sequence > i32::MAX.wrapping_sub(increment) {
            increment
                .wrapping_sub(i32::MAX.wrapping_sub(sequence))
                .wrapping_sub(1)
        } else {
            sequence.wrapping_add(increment)
        }
    }

    /// Java `DefaultRecordBatch.decrementSequence` (wraps below `0` to
    /// [`i32::MAX`]).
    #[must_use]
    pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
        if sequence < decrement {
            i32::MAX
                .wrapping_sub(decrement.wrapping_sub(sequence))
                .wrapping_add(1)
        } else {
            sequence.wrapping_sub(decrement)
        }
    }

    /// Java `DefaultRecordBatch.compressionType`.
    pub fn compression_type(&self) -> Result<Compression> {
        Compression::from_attributes(self.attributes)
    }

    /// Java `RecordBatch.isCompressed` (attributes codec bits are not
    /// [`Compression::None`]).
    #[must_use]
    pub fn is_compressed(&self) -> bool {
        self.attributes & 0x07 != 0
    }

    /// Set [`ATTR_DELETE_HORIZON`]. When set, [`Self::delete_horizon_ms`] is
    /// [`Self::base_timestamp`].
    #[must_use]
    pub fn with_delete_horizon(mut self, delete_horizon: bool) -> Self {
        if delete_horizon {
            self.attributes |= ATTR_DELETE_HORIZON;
        } else {
            self.attributes &= !ATTR_DELETE_HORIZON;
        }
        self
    }

    /// Java `RecordBatch.deleteHorizonMs` (`baseTimestamp` when
    /// [`ATTR_DELETE_HORIZON`] is set).
    #[must_use]
    pub fn delete_horizon_ms(&self) -> Option<i64> {
        if self.attributes & ATTR_DELETE_HORIZON != 0 {
            Some(self.base_timestamp)
        } else {
            None
        }
    }

    /// Java `RecordBatch.offsetOfMaxTimestamp` (earliest offset among records
    /// with [`Self::max_timestamp`]; `None` when none match).
    #[must_use]
    pub fn offset_of_max_timestamp(&self) -> Option<i64> {
        let max = self.max_timestamp;
        self.records
            .iter()
            .find(|r| r.timestamp == max)
            .map(|r| r.offset)
    }

    fn last_offset_delta(&self) -> i32 {
        let count = i32::try_from(self.records.len()).unwrap_or(i32::MAX);
        if count <= 0 {
            0
        } else {
            count - 1
        }
    }

    fn record_count_i64(&self) -> i64 {
        i64::try_from(self.records.len()).unwrap_or(i64::MAX)
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
    /// [`ATTR_TIMESTAMP_TYPE`] / [`ATTR_TRANSACTIONAL`] / [`ATTR_CONTROL`] /
    /// [`ATTR_DELETE_HORIZON`].
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
        assert_eq!(h.to_string(), "RecordHeader(key = k, value = [118])");
        assert_eq!(n.to_string(), "RecordHeader(key = n, value = null)");
        let signed = Header::new("s", Bytes::from_static(&[0xff]));
        assert_eq!(signed.to_string(), "RecordHeader(key = s, value = [-1])");
        let many = [
            Header::new("k", Bytes::from_static(b"1")),
            Header::new("k", Bytes::from_static(b"2")),
            Header::null("empty"),
        ];
        assert_eq!(
            Header::last_in(&many, "k").and_then(Header::value),
            Some(&b"2"[..])
        );
        assert!(Header::last_in(&many, "missing").is_none());
        let keyed: Vec<_> = Header::for_key(&many, "k").map(Header::key).collect();
        assert_eq!(keyed, vec!["k", "k"]);
    }

    #[test]
    fn record_and_batch_getters_match_java() {
        let rec = Record {
            offset: 7,
            timestamp: 9,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"val")),
            headers: vec![Header::new("h", Bytes::from_static(b"v"))],
        };
        assert_eq!(rec.offset(), 7);
        assert_eq!(rec.timestamp(), 9);
        assert_eq!(rec.key(), Some(&b"k"[..]));
        assert!(rec.has_key());
        assert_eq!(rec.key_size(), 1);
        assert_eq!(rec.value(), Some(&b"val"[..]));
        assert!(rec.has_value());
        assert_eq!(rec.value_size(), 3);
        assert_eq!(rec.headers().len(), 1);
        assert_eq!(rec.last_header("h").map(Header::key), Some("h"));
        assert_eq!(rec.headers_for_key("h").count(), 1);
        assert_eq!(
            rec.to_string(),
            "DefaultRecord(offset=7, timestamp=9, key=1 bytes, value=3 bytes)"
        );
        let empty = Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: None,
            headers: vec![],
        };
        assert!(!empty.has_key());
        assert!(!empty.has_value());
        assert_eq!(empty.key_size(), -1);
        assert_eq!(empty.value_size(), -1);
        assert_eq!(empty.headers(), Record::EMPTY_HEADERS);
        assert_eq!(Record::MAX_RECORD_OVERHEAD, 21);
        assert_eq!(
            empty.to_string(),
            "DefaultRecord(offset=0, timestamp=0, key=0 bytes, value=0 bytes)"
        );
        let batch = RecordBatch::from_records(vec![rec])
            .with_transactional(true)
            .with_control_batch(true);
        assert!(batch.is_transactional());
        assert!(batch.is_control_batch());
        assert_eq!(batch.magic(), MAGIC_V2);
        assert_eq!(batch.magic(), RecordBatch::MAGIC_VALUE_V2);
        assert_eq!(batch.magic(), RecordBatch::CURRENT_MAGIC_VALUE);
        assert_eq!(RecordBatch::MAGIC_VALUE_V2, 2);
        assert_eq!(RecordBatch::CURRENT_MAGIC_VALUE, 2);
        assert_eq!(RecordBatch::RECORD_BATCH_OVERHEAD, 61);
        assert_eq!(batch.base_offset(), 0);
        assert_eq!(batch.last_offset(), 0);
        assert_eq!(batch.next_offset(), 1);
        assert_eq!(batch.last_sequence(), RecordBatch::NO_SEQUENCE);
        assert!(!batch.is_compressed());
        assert_eq!(batch.count(), 1);
        assert_eq!(batch.records().len(), 1);
        assert_eq!(
            batch.partition_leader_epoch(),
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(batch.base_timestamp(), 9);
        assert_eq!(batch.max_timestamp(), 9);
        assert_eq!(batch.offset_of_max_timestamp(), Some(0));
        assert!(batch.delete_horizon_ms().is_none());
        let horizon = batch.clone().with_delete_horizon(true);
        assert_eq!(horizon.delete_horizon_ms(), Some(9));
        assert!(horizon
            .with_delete_horizon(false)
            .delete_horizon_ms()
            .is_none());
        assert_eq!(batch.producer_id(), RecordBatch::NO_PRODUCER_ID);
        assert!(!batch.has_producer_id());
        assert_eq!(batch.producer_epoch(), RecordBatch::NO_PRODUCER_EPOCH);
        assert_eq!(batch.base_sequence(), RecordBatch::NO_SEQUENCE);
        assert_eq!(batch.compression_type().unwrap(), Compression::None);
        let empty_batch = RecordBatch::from_records(vec![]);
        assert_eq!(empty_batch.count(), 0);
        assert_eq!(empty_batch.last_offset(), -1);
        assert_eq!(empty_batch.next_offset(), 0);
        assert_eq!(empty_batch.last_sequence(), RecordBatch::NO_SEQUENCE);
        assert!(empty_batch.offset_of_max_timestamp().is_none());
        let mut early = empty.clone();
        early.timestamp = 5;
        let mut late = empty.clone();
        late.timestamp = 9;
        let mixed = RecordBatch::from_records(vec![early, late]);
        assert_eq!(mixed.max_timestamp(), 9);
        assert_eq!(mixed.offset_of_max_timestamp(), Some(1));
        let mut first = empty.clone();
        first.timestamp = 9;
        let mut second = empty.clone();
        second.timestamp = 9;
        let tie = RecordBatch::from_records(vec![first, second]);
        assert_eq!(tie.offset_of_max_timestamp(), Some(0));
        assert_eq!(RecordBatch::NO_TIMESTAMP, -1);
        assert_eq!(RecordBatch::NO_PRODUCER_ID, -1);
        assert_eq!(RecordBatch::NO_PRODUCER_EPOCH, -1);
        assert_eq!(RecordBatch::NO_SEQUENCE, -1);
        assert_eq!(RecordBatch::NO_PARTITION_LEADER_EPOCH, -1);
        let mut with_pid = RecordBatch::from_records(vec![empty.clone()]);
        with_pid.producer_id = 5;
        with_pid.producer_epoch = 1;
        with_pid.base_sequence = 0;
        with_pid.partition_leader_epoch = 3;
        assert!(with_pid.has_producer_id());
        assert_eq!(with_pid.producer_id(), 5);
        assert_eq!(with_pid.producer_epoch(), 1);
        assert_eq!(with_pid.base_sequence(), 0);
        assert_eq!(with_pid.last_sequence(), 0);
        assert_eq!(with_pid.partition_leader_epoch(), 3);
        let mut three =
            RecordBatch::from_records(vec![empty.clone(), empty.clone(), empty.clone()]);
        three.base_sequence = 10;
        assert_eq!(three.last_sequence(), 12);
        let mut wrap = RecordBatch::from_records(vec![empty.clone(), empty]);
        wrap.base_sequence = i32::MAX;
        assert_eq!(wrap.last_sequence(), 0);
        assert_eq!(RecordBatch::increment_sequence(i32::MAX, 1), 0);
        assert_eq!(RecordBatch::decrement_sequence(0, 1), i32::MAX);
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch).unwrap();
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert!(decoded.is_transactional());
        assert!(decoded.is_control_batch());
        assert_eq!(decoded.records[0].offset(), 0);
        assert_eq!(decoded.records[0].value(), Some(&b"val"[..]));
        let cleared = decoded.with_transactional(false).with_control_batch(false);
        assert!(!cleared.is_transactional());
        assert!(!cleared.is_control_batch());
    }

    #[test]
    fn timestamp_type_matches_java() {
        assert_eq!(TimestampType::NoTimestampType.id(), -1);
        assert_eq!(TimestampType::CreateTime.id(), 0);
        assert_eq!(TimestampType::LogAppendTime.id(), 1);
        assert_eq!(TimestampType::CreateTime.as_str(), "CreateTime");
        assert_eq!(TimestampType::LogAppendTime.as_str(), "LogAppendTime");
        assert_eq!(TimestampType::NoTimestampType.as_str(), "NoTimestampType");
        assert_eq!(TimestampType::CreateTime.to_string(), "CreateTime");
        assert_eq!(Compression::None.to_string(), "none");
        assert_eq!(Compression::Gzip.to_string(), "gzip");
        assert_eq!(Compression::Snappy.to_string(), "snappy");
        assert_eq!(Compression::Lz4.to_string(), "lz4");
        assert_eq!(Compression::None.id(), 0);
        assert_eq!(Compression::Gzip.id(), 1);
        assert_eq!(Compression::Snappy.id(), 2);
        assert_eq!(Compression::Lz4.id(), 3);
        assert_eq!(Compression::from_id(0), Some(Compression::None));
        assert_eq!(Compression::from_id(1), Some(Compression::Gzip));
        assert_eq!(Compression::from_id(2), Some(Compression::Snappy));
        assert_eq!(Compression::from_id(3), Some(Compression::Lz4));
        assert!(Compression::from_id(4).is_none());
        assert!(Compression::from_id(-1).is_none());
        assert_eq!(TimestampType::from_id(0), Some(TimestampType::CreateTime));
        assert_eq!(
            TimestampType::from_id(1),
            Some(TimestampType::LogAppendTime)
        );
        assert_eq!(
            TimestampType::from_id(-1),
            Some(TimestampType::NoTimestampType)
        );
        assert!(TimestampType::from_id(2).is_none());
        assert_eq!(
            TimestampType::from_name("LogAppendTime").unwrap(),
            TimestampType::LogAppendTime
        );
        let unknown = TimestampType::from_name("bogus").unwrap_err();
        assert!(
            unknown.to_string().contains("unknown timestamp type"),
            "{unknown}"
        );
        assert_eq!(TimestampType::from_attributes(0), TimestampType::CreateTime);
        assert_eq!(
            TimestampType::from_attributes(ATTR_TIMESTAMP_TYPE),
            TimestampType::LogAppendTime
        );
        assert_eq!(
            TimestampType::from_attributes(ATTR_TIMESTAMP_TYPE | Compression::Gzip as i16),
            TimestampType::LogAppendTime
        );
        let rec = Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"ts")),
            headers: vec![],
        };
        let create = RecordBatch::from_records(vec![rec.clone()]);
        assert_eq!(create.timestamp_type(), TimestampType::CreateTime);
        let append = create
            .clone()
            .with_timestamp_type(TimestampType::LogAppendTime);
        assert_eq!(append.timestamp_type(), TimestampType::LogAppendTime);
        assert_eq!(append.attributes & ATTR_TIMESTAMP_TYPE, ATTR_TIMESTAMP_TYPE);
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &append).unwrap();
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert_eq!(decoded.timestamp_type(), TimestampType::LogAppendTime);
        let cleared = append.with_timestamp_type(TimestampType::NoTimestampType);
        assert_eq!(cleared.timestamp_type(), TimestampType::CreateTime);
        assert_eq!(TimestampType::LogAppendTime.to_string(), "LogAppendTime");
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
            null_key.to_string(),
            "DefaultRecord(offset=0, timestamp=0, key=0 bytes, value=1 bytes)"
        );
        assert_eq!(
            empty_key.to_string(),
            "DefaultRecord(offset=0, timestamp=0, key=0 bytes, value=1 bytes)"
        );
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
        assert!(batch.is_compressed());
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
