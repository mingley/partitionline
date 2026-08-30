//! Magic-v2 RecordBatch codec (gzip, snappy, lz4).

use std::fmt;
use std::io::{Read, Write};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Record batch magic for Kafka 0.11+ (v2).
pub const MAGIC_V2: i8 = 2;

/// Java `Records` log-entry layout (offset + size prefix before the batch body).
pub struct Records;

impl Records {
    /// Java `Records.OFFSET_OFFSET`.
    pub const OFFSET_OFFSET: i32 = 0;
    /// Java `Records.OFFSET_LENGTH`.
    pub const OFFSET_LENGTH: i32 = 8;
    /// Java `Records.SIZE_OFFSET`.
    pub const SIZE_OFFSET: i32 = Self::OFFSET_OFFSET + Self::OFFSET_LENGTH;
    /// Java `Records.SIZE_LENGTH`.
    pub const SIZE_LENGTH: i32 = 4;
    /// Java `Records.LOG_OVERHEAD`.
    pub const LOG_OVERHEAD: i32 = Self::SIZE_OFFSET + Self::SIZE_LENGTH;
    /// Java `Records.MAGIC_OFFSET`.
    pub const MAGIC_OFFSET: i32 = Self::LOG_OVERHEAD + 4;
    /// Java `Records.MAGIC_LENGTH`.
    pub const MAGIC_LENGTH: i32 = 1;
    /// Java `Records.HEADER_SIZE_UP_TO_MAGIC`.
    pub const HEADER_SIZE_UP_TO_MAGIC: i32 = Self::MAGIC_OFFSET + Self::MAGIC_LENGTH;

    /// Java `AbstractRecords.estimateSizeInBytesUpperBound` (magic-v2).
    ///
    /// Compression is ignored: v2 uses
    /// [`RecordBatch::estimate_batch_size_upper_bound`].
    pub fn estimate_size_in_bytes_upper_bound(
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header],
    ) -> Result<i32> {
        RecordBatch::estimate_batch_size_upper_bound(key, value, headers)
    }

    /// Java `AbstractRecords.estimateSizeInBytes` (magic-v2, offset deltas
    /// `0..n`).
    pub fn estimate_size_in_bytes(compression: Compression, records: &[Record]) -> Result<i32> {
        let size = RecordBatch::size_in_bytes_of(records)?;
        Ok(estimate_compressed_size_in_bytes(size, compression))
    }

    /// Java `AbstractRecords.estimateSizeInBytes` with a base offset (magic-v2).
    pub fn estimate_size_in_bytes_from(
        base_offset: i64,
        compression: Compression,
        records: &[Record],
    ) -> Result<i32> {
        let size = RecordBatch::size_in_bytes_from(base_offset, records)?;
        Ok(estimate_compressed_size_in_bytes(size, compression))
    }

    /// Java `AbstractRecords.recordBatchHeaderSizeInBytes` (magic-v2).
    #[must_use]
    pub const fn record_batch_header_size_in_bytes() -> i32 {
        RecordBatch::RECORD_BATCH_OVERHEAD
    }

    /// Java `AbstractRecords.hasMatchingMagic`. Empty is true (vacuous).
    ///
    /// This crate's [`RecordBatch::magic`] is always
    /// [`RecordBatch::CURRENT_MAGIC_VALUE`] (`2`).
    #[must_use]
    pub fn has_matching_magic(batches: &[RecordBatch], magic: i8) -> bool {
        batches.iter().all(|batch| batch.magic() == magic)
    }

    /// Java `AbstractRecords.firstBatch`. Empty is `None`.
    #[must_use]
    pub fn first_batch(batches: &[RecordBatch]) -> Option<&RecordBatch> {
        batches.first()
    }

    /// Java `AbstractRecords.lastBatch`. Empty is `None` (Java `Optional.empty`).
    #[must_use]
    pub fn last_batch(batches: &[RecordBatch]) -> Option<&RecordBatch> {
        batches.last()
    }

    /// Java `MemoryRecords.firstBatchSize`.
    ///
    /// Fewer than [`Self::HEADER_SIZE_UP_TO_MAGIC`] bytes is `None` (the size
    /// field is not validated). Size below Java `LegacyRecord.RECORD_OVERHEAD_V0`
    /// (14) or magic outside 0 through [`RecordBatch::CURRENT_MAGIC_VALUE`] is
    /// [`Error::protocol`] (`CorruptRecordException`). The returned size includes
    /// [`Self::LOG_OVERHEAD`].
    pub fn first_batch_size(buffer: &[u8]) -> Result<Option<i32>> {
        let header_up_to_magic = buf::usize_from_i32(Self::HEADER_SIZE_UP_TO_MAGIC)?;
        if buffer.len() < header_up_to_magic {
            return Ok(None);
        }
        next_batch_size(buffer, i32::MAX)
    }

    /// Java `MemoryRecords.validBytes`.
    ///
    /// Sum of complete batch sizes (including [`Self::LOG_OVERHEAD`]). A
    /// truncated trailing batch is ignored. Header corruption uses the same
    /// [`Error::protocol`] messages as [`Self::first_batch_size`].
    pub fn valid_bytes(buffer: &[u8]) -> Result<i32> {
        let mut offset = 0;
        let mut bytes = 0i32;
        while let Some(remaining) = buffer.get(offset..) {
            let Some(batch_size) = next_batch_size(remaining, i32::MAX)? else {
                break;
            };
            let need = buf::usize_from_i32(batch_size)?;
            if remaining.len() < need {
                break;
            }
            bytes = bytes.wrapping_add(batch_size);
            offset = offset.saturating_add(need);
        }
        Ok(bytes)
    }
}

/// Java `ByteBufferLogInputStream.nextBatchSize` (`maxMessageSize` is the
/// constructor argument; `MemoryRecords` uses `Integer.MAX_VALUE`).
fn next_batch_size(buffer: &[u8], max_message_size: i32) -> Result<Option<i32>> {
    let log_overhead = buf::usize_from_i32(Records::LOG_OVERHEAD)?;
    if buffer.len() < log_overhead {
        return Ok(None);
    }
    let size_off = buf::usize_from_i32(Records::SIZE_OFFSET)?;
    let record_size = buf::read_int_be(buffer, size_off)?;
    // Java `LegacyRecord.RECORD_OVERHEAD_V0`.
    const RECORD_OVERHEAD_V0: i32 = 14;
    if record_size < RECORD_OVERHEAD_V0 {
        return Err(Error::protocol(format!(
            "Record size {record_size} is less than the minimum record overhead ({RECORD_OVERHEAD_V0})"
        )));
    }
    if record_size > max_message_size {
        return Err(Error::protocol(format!(
            "Record size {record_size} exceeds the largest allowable message size ({max_message_size})."
        )));
    }
    let header_up_to_magic = buf::usize_from_i32(Records::HEADER_SIZE_UP_TO_MAGIC)?;
    if buffer.len() < header_up_to_magic {
        return Ok(None);
    }
    let magic_off = buf::usize_from_i32(Records::MAGIC_OFFSET)?;
    let magic_byte = buffer
        .get(magic_off)
        .copied()
        .ok_or_else(|| Error::protocol("short magic byte"))?;
    let magic = i8::from_ne_bytes([magic_byte]);
    if !(0..=RecordBatch::CURRENT_MAGIC_VALUE).contains(&magic) {
        return Err(Error::protocol(format!(
            "Invalid magic found in record: {magic}"
        )));
    }
    Ok(Some(record_size.wrapping_add(Records::LOG_OVERHEAD)))
}

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

    /// Java `CompressionType.forName` (`none` / `gzip` / `snappy` / `lz4`).
    /// Empty is [`Self::None`]. zstd is not spoken.
    pub fn from_name(name: &str) -> Result<Self> {
        match name {
            "none" | "" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            other => Err(Error::protocol(format!(
                "Unknown compression name: {other}"
            ))),
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

    /// Java `CompressionType.GZIP.MIN_LEVEL` (`Deflater.BEST_SPEED`).
    pub const GZIP_MIN_LEVEL: i32 = 1;
    /// Java `CompressionType.GZIP.MAX_LEVEL` (`Deflater.BEST_COMPRESSION`).
    pub const GZIP_MAX_LEVEL: i32 = 9;
    /// Java `CompressionType.GZIP.DEFAULT_LEVEL` (`Deflater.DEFAULT_COMPRESSION`).
    pub const GZIP_DEFAULT_LEVEL: i32 = -1;
    /// Java `CompressionType.LZ4` min (`LZ4Constants`).
    pub const LZ4_MIN_LEVEL: i32 = 1;
    /// Java `CompressionType.LZ4` max (`LZ4Constants`).
    pub const LZ4_MAX_LEVEL: i32 = 17;
    /// Java `CompressionType.LZ4` default (`LZ4Constants`).
    pub const LZ4_DEFAULT_LEVEL: i32 = 9;

    fn levels_unsupported(self) -> Error {
        Error::Unsupported(format!(
            "Compression levels are not defined for this compression type: {}",
            self.as_str()
        ))
    }

    /// Java `CompressionType.defaultLevel`.
    ///
    /// [`Self::None`] / [`Self::Snappy`] are [`Error::Unsupported`]. zstd is
    /// not spoken.
    pub fn default_level(self) -> Result<i32> {
        match self {
            Self::Gzip => Ok(Self::GZIP_DEFAULT_LEVEL),
            Self::Lz4 => Ok(Self::LZ4_DEFAULT_LEVEL),
            other => Err(other.levels_unsupported()),
        }
    }

    /// Java `CompressionType.minLevel`.
    ///
    /// [`Self::None`] / [`Self::Snappy`] are [`Error::Unsupported`].
    pub fn min_level(self) -> Result<i32> {
        match self {
            Self::Gzip => Ok(Self::GZIP_MIN_LEVEL),
            Self::Lz4 => Ok(Self::LZ4_MIN_LEVEL),
            other => Err(other.levels_unsupported()),
        }
    }

    /// Java `CompressionType.maxLevel`.
    ///
    /// [`Self::None`] / [`Self::Snappy`] are [`Error::Unsupported`].
    pub fn max_level(self) -> Result<i32> {
        match self {
            Self::Gzip => Ok(Self::GZIP_MAX_LEVEL),
            Self::Lz4 => Ok(Self::LZ4_MAX_LEVEL),
            other => Err(other.levels_unsupported()),
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

fn be_i16_at(buf: &[u8], offset: usize) -> Result<i16> {
    let b0 = buf
        .get(offset)
        .copied()
        .ok_or_else(|| Error::protocol("short control record"))?;
    let b1 = buf
        .get(offset.saturating_add(1))
        .copied()
        .ok_or_else(|| Error::protocol("short control record"))?;
    Ok(i16::from_be_bytes([b0, b1]))
}

fn be_i32_at(buf: &[u8], offset: usize) -> Result<i32> {
    let b0 = buf
        .get(offset)
        .copied()
        .ok_or_else(|| Error::protocol("short control record"))?;
    let b1 = buf
        .get(offset.saturating_add(1))
        .copied()
        .ok_or_else(|| Error::protocol("short control record"))?;
    let b2 = buf
        .get(offset.saturating_add(2))
        .copied()
        .ok_or_else(|| Error::protocol("short control record"))?;
    let b3 = buf
        .get(offset.saturating_add(3))
        .copied()
        .ok_or_else(|| Error::protocol("short control record"))?;
    Ok(i32::from_be_bytes([b0, b1, b2, b3]))
}

/// Java `ControlRecordType` (control-record key `type`).
///
/// [`Display`] is Java `ControlRecordType.toString` (`ABORT` / `COMMIT` /
/// `LEADER_CHANGE` / `SNAPSHOT_HEADER` / `SNAPSHOT_FOOTER` / `KRAFT_VERSION` /
/// `KRAFT_VOTERS` / `UNKNOWN`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlRecordType {
    /// Java `ABORT` (`type` 0).
    Abort,
    /// Java `COMMIT` (`type` 1).
    Commit,
    /// Java `LEADER_CHANGE` (`type` 2).
    LeaderChange,
    /// Java `SNAPSHOT_HEADER` (`type` 3).
    SnapshotHeader,
    /// Java `SNAPSHOT_FOOTER` (`type` 4).
    SnapshotFooter,
    /// Java `KRAFT_VERSION` (`type` 5).
    KraftVersion,
    /// Java `KRAFT_VOTERS` (`type` 6).
    KraftVoters,
    /// Java `UNKNOWN` (`type` -1).
    Unknown,
}

impl ControlRecordType {
    /// Java `ControlRecordType.CURRENT_CONTROL_RECORD_KEY_VERSION` (package-private).
    const CURRENT_CONTROL_RECORD_KEY_VERSION: i16 = 0;
    /// Java `ControlRecordType.CURRENT_CONTROL_RECORD_KEY_SIZE` (package-private).
    const CURRENT_CONTROL_RECORD_KEY_SIZE: usize = 4;

    /// Java `ControlRecordType.type`.
    #[must_use]
    pub const fn type_id(self) -> i16 {
        match self {
            Self::Abort => 0,
            Self::Commit => 1,
            Self::LeaderChange => 2,
            Self::SnapshotHeader => 3,
            Self::SnapshotFooter => 4,
            Self::KraftVersion => 5,
            Self::KraftVoters => 6,
            Self::Unknown => -1,
        }
    }

    /// Java `ControlRecordType.fromTypeId`. Unknown ids are [`Self::Unknown`].
    #[must_use]
    pub const fn from_type_id(type_id: i16) -> Self {
        match type_id {
            0 => Self::Abort,
            1 => Self::Commit,
            2 => Self::LeaderChange,
            3 => Self::SnapshotHeader,
            4 => Self::SnapshotFooter,
            5 => Self::KraftVersion,
            6 => Self::KraftVoters,
            _ => Self::Unknown,
        }
    }

    /// Java `ControlRecordType.toString`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Abort => "ABORT",
            Self::Commit => "COMMIT",
            Self::LeaderChange => "LEADER_CHANGE",
            Self::SnapshotHeader => "SNAPSHOT_HEADER",
            Self::SnapshotFooter => "SNAPSHOT_FOOTER",
            Self::KraftVersion => "KRAFT_VERSION",
            Self::KraftVoters => "KRAFT_VOTERS",
            Self::Unknown => "UNKNOWN",
        }
    }

    /// Java `ControlRecordType.parseTypeId`.
    pub fn parse_type_id(key: &[u8]) -> Result<i16> {
        if key.len() < Self::CURRENT_CONTROL_RECORD_KEY_SIZE {
            return Err(Error::protocol(format!(
                "Invalid value size found for end control record key. Must have at least {} bytes, but found only {}",
                Self::CURRENT_CONTROL_RECORD_KEY_SIZE,
                key.len()
            )));
        }
        let version = be_i16_at(key, 0)?;
        if version < 0 {
            return Err(Error::protocol(format!(
                "Invalid version found for control record: {version}. May indicate data corruption"
            )));
        }
        be_i16_at(key, 2)
    }

    /// Java `ControlRecordType.parse`.
    pub fn parse(key: &[u8]) -> Result<Self> {
        Ok(Self::from_type_id(Self::parse_type_id(key)?))
    }

    /// Java `ControlRecordType.recordKey` (version 0 key bytes). [`Self::Unknown`]
    /// cannot be serialized.
    pub fn record_key(self) -> Result<[u8; 4]> {
        if matches!(self, Self::Unknown) {
            return Err(Error::protocol(
                "Cannot serialize UNKNOWN control record type",
            ));
        }
        let tid = self.type_id().to_be_bytes();
        let hi = tid.first().copied().unwrap_or(0);
        let lo = tid.get(1).copied().unwrap_or(0);
        let ver = Self::CURRENT_CONTROL_RECORD_KEY_VERSION.to_be_bytes();
        let vh = ver.first().copied().unwrap_or(0);
        let vl = ver.get(1).copied().unwrap_or(0);
        Ok([vh, vl, hi, lo])
    }
}

impl fmt::Display for ControlRecordType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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

    /// Java `Record.hasMagic`. Magic-v2 is `true` when `magic` is 2 or greater.
    #[must_use]
    pub fn has_magic(&self, magic: i8) -> bool {
        magic >= MAGIC_V2
    }

    /// Java `DefaultRecord.isCompressed` (always `false`; compression is on the batch).
    #[must_use]
    pub fn is_compressed(&self) -> bool {
        false
    }

    /// Java `DefaultRecord.hasTimestampType` (always `false`; timestamp type is on the batch).
    #[must_use]
    pub fn has_timestamp_type(&self, _timestamp_type: TimestampType) -> bool {
        false
    }

    /// Java `DefaultRecord.sizeOfBodyInBytes` (no length-prefix varint).
    ///
    /// `offset_delta` / `timestamp_delta` are the magic-v2 relative fields
    /// (`offset - baseOffset`, `timestamp - baseTimestamp`).
    pub fn size_of_body_in_bytes(&self, offset_delta: i32, timestamp_delta: i64) -> Result<i32> {
        record_body_size(
            &EncodeRecord::from_record(self),
            offset_delta,
            timestamp_delta,
        )
    }

    /// Java `DefaultRecord.sizeInBytes(int, long, ByteBuffer, ByteBuffer, Header[])`.
    ///
    /// Body size plus the zigzag varint that prefixes the body on the wire.
    pub fn size_in_bytes(&self, offset_delta: i32, timestamp_delta: i64) -> Result<i32> {
        let body = self.size_of_body_in_bytes(offset_delta, timestamp_delta)?;
        body.checked_add(buf::size_of_varint(body))
            .ok_or_else(|| Error::protocol("length exceeds i32"))
    }

    /// Java `DefaultRecord.recordSizeUpperBound` (`MAX_RECORD_OVERHEAD` plus
    /// key, value, and headers; not the on-wire size).
    pub fn record_size_upper_bound(&self) -> Result<i32> {
        record_size_upper_bound(self.key(), self.value(), self.headers())
    }

    /// Control record for [`EndTransactionMarker`] (Java
    /// `MemoryRecordsBuilder.appendEndTxnMarker`).
    pub fn from_end_transaction_marker(
        timestamp: i64,
        marker: &EndTransactionMarker,
    ) -> Result<Self> {
        let key = marker.control_type().record_key()?;
        Ok(Self {
            offset: 0,
            timestamp,
            key: Some(Bytes::copy_from_slice(&key)),
            value: Some(Bytes::copy_from_slice(&marker.serialize_value())),
            headers: Vec::new(),
        })
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

/// Java `EndTransactionMarker` (COMMIT/ABORT control record value).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndTransactionMarker {
    control_type: ControlRecordType,
    coordinator_epoch: i32,
}

impl EndTransactionMarker {
    /// Java `EndTransactionMarker.CURRENT_END_TXN_MARKER_VERSION` (package-private).
    const CURRENT_END_TXN_MARKER_VERSION: i16 = 0;
    /// Java `EndTransactionMarker.CURRENT_END_TXN_MARKER_VALUE_SIZE` (package-private).
    const CURRENT_END_TXN_MARKER_VALUE_SIZE: usize = 6;

    /// Java `EndTransactionMarker(ControlRecordType, int)`. Only
    /// [`ControlRecordType::Commit`] and [`ControlRecordType::Abort`] are valid.
    pub fn new(control_type: ControlRecordType, coordinator_epoch: i32) -> Result<Self> {
        Self::ensure_transaction_marker_control_type(control_type)?;
        Ok(Self {
            control_type,
            coordinator_epoch,
        })
    }

    /// Java `EndTransactionMarker.coordinatorEpoch`.
    #[must_use]
    pub fn coordinator_epoch(self) -> i32 {
        self.coordinator_epoch
    }

    /// Java `EndTransactionMarker.controlType`.
    #[must_use]
    pub fn control_type(self) -> ControlRecordType {
        self.control_type
    }

    /// Java `EndTransactionMarker.serializeValue`.
    #[must_use]
    pub fn serialize_value(self) -> [u8; 6] {
        let epoch = self.coordinator_epoch.to_be_bytes();
        let ver = Self::CURRENT_END_TXN_MARKER_VERSION.to_be_bytes();
        [
            ver.first().copied().unwrap_or(0),
            ver.get(1).copied().unwrap_or(0),
            epoch.first().copied().unwrap_or(0),
            epoch.get(1).copied().unwrap_or(0),
            epoch.get(2).copied().unwrap_or(0),
            epoch.get(3).copied().unwrap_or(0),
        ]
    }

    /// Java `EndTransactionMarker.deserialize`.
    pub fn deserialize(record: &Record) -> Result<Self> {
        let key = record
            .key
            .as_deref()
            .ok_or_else(|| Error::protocol("end transaction marker key is null"))?;
        let value = record
            .value
            .as_deref()
            .ok_or_else(|| Error::protocol("end transaction marker value is null"))?;
        let control_type = ControlRecordType::parse(key)?;
        Self::deserialize_value(control_type, value)
    }

    fn ensure_transaction_marker_control_type(control_type: ControlRecordType) -> Result<()> {
        if matches!(
            control_type,
            ControlRecordType::Commit | ControlRecordType::Abort
        ) {
            Ok(())
        } else {
            Err(Error::protocol(format!(
                "Invalid control record type for end transaction marker{control_type}"
            )))
        }
    }

    fn deserialize_value(control_type: ControlRecordType, value: &[u8]) -> Result<Self> {
        Self::ensure_transaction_marker_control_type(control_type)?;
        if value.len() < Self::CURRENT_END_TXN_MARKER_VALUE_SIZE {
            return Err(Error::protocol(format!(
                "Invalid value size found for end transaction marker. Must have at least {} bytes, but found only {}",
                Self::CURRENT_END_TXN_MARKER_VALUE_SIZE,
                value.len()
            )));
        }
        let version = be_i16_at(value, 0)?;
        if version < 0 {
            return Err(Error::protocol(format!(
                "Invalid version found for end transaction marker: {version}. May indicate data corruption"
            )));
        }
        let coordinator_epoch = be_i32_at(value, 2)?;
        Ok(Self {
            control_type,
            coordinator_epoch,
        })
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
            other => Err(Error::protocol(format!("Invalid timestamp type {other}"))),
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
    /// Partition leader epoch, or [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub partition_leader_epoch: i32,
    /// Attribute bits: compression in the low 3, plus
    /// [`ATTR_TIMESTAMP_TYPE`] / [`ATTR_TRANSACTIONAL`] / [`ATTR_CONTROL`] /
    /// [`ATTR_DELETE_HORIZON`].
    pub attributes: i16,
    /// Timestamp of the first record (milliseconds since the Unix epoch).
    pub base_timestamp: i64,
    /// Max timestamp among records in this batch.
    pub max_timestamp: i64,
    /// Idempotent / transactional producer id, or [`RecordBatch::NO_PRODUCER_ID`].
    pub producer_id: i64,
    /// Producer epoch, or [`RecordBatch::NO_PRODUCER_EPOCH`].
    pub producer_epoch: i16,
    /// First sequence number in this batch, or [`RecordBatch::NO_SEQUENCE`].
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
    /// Java `RecordBatch.MAGIC_VALUE_V0`. This crate does not encode magic-v0.
    pub const MAGIC_VALUE_V0: i8 = 0;
    /// Java `RecordBatch.MAGIC_VALUE_V1`. This crate does not encode magic-v1.
    pub const MAGIC_VALUE_V1: i8 = 1;
    /// Java `RecordBatch.MAGIC_VALUE_V2`. This crate speaks magic-v2 only.
    pub const MAGIC_VALUE_V2: i8 = MAGIC_V2;
    /// Java `RecordBatch.CURRENT_MAGIC_VALUE` ([`Self::MAGIC_VALUE_V2`]).
    pub const CURRENT_MAGIC_VALUE: i8 = Self::MAGIC_VALUE_V2;
    /// Java `DefaultRecordBatch.RECORD_BATCH_OVERHEAD` (bytes before the records).
    pub const RECORD_BATCH_OVERHEAD: i32 = 61;
    /// Java `DefaultRecordBatch.CRC_OFFSET`.
    pub const CRC_OFFSET: i32 = 17;
    /// Java `DefaultRecordBatch.ATTRIBUTES_OFFSET`.
    pub const ATTRIBUTES_OFFSET: i32 = Self::CRC_OFFSET + 4;
    /// Java `DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET`.
    pub const LAST_OFFSET_DELTA_OFFSET: i32 = 23;
    /// Java `DefaultRecordBatch.BASE_TIMESTAMP_OFFSET`.
    pub const BASE_TIMESTAMP_OFFSET: i32 = 27;
    /// Java `DefaultRecordBatch.PRODUCER_ID_OFFSET`.
    pub const PRODUCER_ID_OFFSET: i32 = 43;
    /// Java `DefaultRecordBatch.BASE_SEQUENCE_OFFSET`.
    pub const BASE_SEQUENCE_OFFSET: i32 = 53;
    /// Java `DefaultRecordBatch.RECORDS_COUNT_OFFSET`.
    pub const RECORDS_COUNT_OFFSET: i32 = 57;

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

    /// Java `MemoryRecords.withEndTransactionMarker`.
    ///
    /// Builds a transactional control batch with one COMMIT/ABORT record.
    /// [`Self::base_sequence`] is [`Self::NO_SEQUENCE`].
    pub fn with_end_transaction_marker(
        initial_offset: i64,
        timestamp: i64,
        partition_leader_epoch: i32,
        producer_id: i64,
        producer_epoch: i16,
        marker: &EndTransactionMarker,
    ) -> Result<Self> {
        let rec = Record::from_end_transaction_marker(timestamp, marker)?;
        let mut batch = Self::from_records(vec![rec])
            .with_transactional(true)
            .with_control_batch(true);
        batch.base_offset = initial_offset;
        batch.partition_leader_epoch = partition_leader_epoch;
        batch.producer_id = producer_id;
        batch.producer_epoch = producer_epoch;
        batch.base_sequence = Self::NO_SEQUENCE;
        Ok(batch)
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

    /// Java `RecordBatch.countOrNull`. Magic-v2 always has a count.
    #[must_use]
    pub fn count_or_null(&self) -> Option<i32> {
        Some(i32::try_from(self.records.len()).unwrap_or(i32::MAX))
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

    /// Java `AbstractRecordBatch.hasProducerId` (`NO_PRODUCER_ID < producerId`).
    #[must_use]
    pub fn has_producer_id(&self) -> bool {
        Self::NO_PRODUCER_ID < self.producer_id
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

    /// Encoded size of this batch, including compression.
    ///
    /// Distinct from [`Self::encoded_size_in_bytes`], which reads the length
    /// field from a buffer (Java `DefaultRecordBatch.sizeInBytes()`).
    pub fn size_in_bytes(&self) -> Result<i32> {
        buf::i32_from_usize(self.encoded()?.len())
    }

    /// Java `DefaultRecordBatch.sizeInBytes()` on a buffer
    /// (`Records.LOG_OVERHEAD` plus the length field at
    /// [`Records::SIZE_OFFSET`]).
    ///
    /// Uses wrapping add (Java `int` overflow). Short size field is
    /// [`Error::protocol`] `need 4 bytes`. Distinct from [`Self::size_in_bytes`],
    /// which encodes this batch.
    pub fn encoded_size_in_bytes(buffer: &[u8]) -> Result<i32> {
        let size_off = buf::usize_from_i32(Records::SIZE_OFFSET)?;
        let record_size = buf::read_int_be(buffer, size_off)?;
        Ok(Records::LOG_OVERHEAD.wrapping_add(record_size))
    }

    fn read_i64_be(buffer: &[u8], offset: usize) -> Result<i64> {
        let have = buffer.len().saturating_sub(offset);
        let end = offset
            .checked_add(8)
            .ok_or_else(|| Error::protocol(format!("need 8 bytes, have {have}")))?;
        let slice = buffer
            .get(offset..end)
            .ok_or_else(|| Error::protocol(format!("need 8 bytes, have {have}")))?;
        let arr = <[u8; 8]>::try_from(slice)
            .map_err(|_| Error::protocol(format!("need 8 bytes, have {have}")))?;
        Ok(i64::from_be_bytes(arr))
    }

    fn read_i16_be(buffer: &[u8], offset: usize) -> Result<i16> {
        let have = buffer.len().saturating_sub(offset);
        let end = offset
            .checked_add(2)
            .ok_or_else(|| Error::protocol(format!("need 2 bytes, have {have}")))?;
        let slice = buffer
            .get(offset..end)
            .ok_or_else(|| Error::protocol(format!("need 2 bytes, have {have}")))?;
        let arr = <[u8; 2]>::try_from(slice)
            .map_err(|_| Error::protocol(format!("need 2 bytes, have {have}")))?;
        Ok(i16::from_be_bytes(arr))
    }

    /// Java `DefaultRecordBatch.lastOffset` on a buffer (`baseOffset` plus
    /// `lastOffsetDelta` at [`Self::LAST_OFFSET_DELTA_OFFSET`]).
    ///
    /// Uses wrapping add (Java `long` overflow). Distinct from
    /// [`Self::last_offset`], which uses `count - 1`. Short base-offset or
    /// last-offset-delta fields are [`Error::protocol`] `need N bytes`.
    pub fn encoded_last_offset(buffer: &[u8]) -> Result<i64> {
        let base_off = buf::usize_from_i32(Records::OFFSET_OFFSET)?;
        let base = Self::read_i64_be(buffer, base_off)?;
        let delta_off = buf::usize_from_i32(Self::LAST_OFFSET_DELTA_OFFSET)?;
        let delta = buf::read_int_be(buffer, delta_off)?;
        Ok(base.wrapping_add(i64::from(delta)))
    }

    /// Java `DefaultRecordBatch.nextOffset` on a buffer
    /// ([`Self::encoded_last_offset`] plus one).
    pub fn encoded_next_offset(buffer: &[u8]) -> Result<i64> {
        Ok(Self::encoded_last_offset(buffer)?.wrapping_add(1))
    }

    /// Java `DefaultRecordBatch.lastSequence` on a buffer.
    ///
    /// [`Self::NO_SEQUENCE`] when the stored base sequence is unset (delta is
    /// not read). Otherwise [`Self::increment_sequence`] of the base sequence
    /// and header `lastOffsetDelta`. Distinct from [`Self::last_sequence`],
    /// which uses `count - 1`. Short sequence or delta fields are
    /// [`Error::protocol`] `need 4 bytes`.
    pub fn encoded_last_sequence(buffer: &[u8]) -> Result<i32> {
        let seq_off = buf::usize_from_i32(Self::BASE_SEQUENCE_OFFSET)?;
        let base_sequence = buf::read_int_be(buffer, seq_off)?;
        if base_sequence == Self::NO_SEQUENCE {
            return Ok(Self::NO_SEQUENCE);
        }
        let delta_off = buf::usize_from_i32(Self::LAST_OFFSET_DELTA_OFFSET)?;
        let delta = buf::read_int_be(buffer, delta_off)?;
        Ok(Self::increment_sequence(base_sequence, delta))
    }

    fn encoded_attributes(buffer: &[u8]) -> Result<i16> {
        let attr_off = buf::usize_from_i32(Self::ATTRIBUTES_OFFSET)?;
        Self::read_i16_be(buffer, attr_off)
    }

    /// Java `DefaultRecordBatch.deleteHorizonMs` on a buffer.
    ///
    /// Unset [`ATTR_DELETE_HORIZON`] is `None` without reading
    /// [`Self::BASE_TIMESTAMP_OFFSET`]. Otherwise the stored base timestamp.
    /// Distinct from [`Self::delete_horizon_ms`], which uses this batch's
    /// fields. Short attributes or timestamp fields are [`Error::protocol`]
    /// `need N bytes`.
    pub fn encoded_delete_horizon_ms(buffer: &[u8]) -> Result<Option<i64>> {
        let attributes = Self::encoded_attributes(buffer)?;
        if attributes & ATTR_DELETE_HORIZON == 0 {
            return Ok(None);
        }
        let ts_off = buf::usize_from_i32(Self::BASE_TIMESTAMP_OFFSET)?;
        Ok(Some(Self::read_i64_be(buffer, ts_off)?))
    }

    /// Java `DefaultRecordBatch.isTransactional` on a buffer.
    ///
    /// Distinct from [`Self::is_transactional`], which uses this batch's
    /// attributes. Short attributes field is [`Error::protocol`] `need 2 bytes`.
    pub fn encoded_is_transactional(buffer: &[u8]) -> Result<bool> {
        Ok(Self::encoded_attributes(buffer)? & ATTR_TRANSACTIONAL != 0)
    }

    /// Java `DefaultRecordBatch.isControlBatch` on a buffer.
    ///
    /// Distinct from [`Self::is_control_batch`], which uses this batch's
    /// attributes. Short attributes field is [`Error::protocol`] `need 2 bytes`.
    pub fn encoded_is_control_batch(buffer: &[u8]) -> Result<bool> {
        Ok(Self::encoded_attributes(buffer)? & ATTR_CONTROL != 0)
    }

    /// Java `DefaultRecordBatch.timestampType` on a buffer.
    ///
    /// Distinct from [`Self::timestamp_type`], which uses this batch's
    /// attributes. Short attributes field is [`Error::protocol`] `need 2 bytes`.
    pub fn encoded_timestamp_type(buffer: &[u8]) -> Result<TimestampType> {
        Ok(TimestampType::from_attributes(Self::encoded_attributes(
            buffer,
        )?))
    }

    /// Java `AbstractRecordBatch.hasProducerId` on a buffer (producer id is
    /// greater than [`Self::NO_PRODUCER_ID`]).
    ///
    /// Distinct from [`Self::has_producer_id`], which uses this batch's
    /// producer id. Short producer-id field is [`Error::protocol`]
    /// `need 8 bytes`.
    pub fn encoded_has_producer_id(buffer: &[u8]) -> Result<bool> {
        let off = buf::usize_from_i32(Self::PRODUCER_ID_OFFSET)?;
        Ok(Self::NO_PRODUCER_ID < Self::read_i64_be(buffer, off)?)
    }

    /// Java `DefaultRecordBatch.checksum` (unsigned CRC32-C as `long`).
    pub fn checksum(&self) -> Result<u32> {
        let buf = self.encoded()?;
        let off = buf::usize_from_i32(Self::CRC_OFFSET)?;
        let end = off.saturating_add(4);
        let bytes = buf
            .get(off..end)
            .ok_or_else(|| Error::protocol("short crc field"))?;
        let arr = <[u8; 4]>::try_from(bytes).map_err(|_| Error::protocol("short crc field"))?;
        Ok(u32::from_be_bytes(arr))
    }

    /// Java `DefaultRecordBatch.isValid`.
    ///
    /// Declared size ([`Self::encoded_size_in_bytes`]) below
    /// [`Self::RECORD_BATCH_OVERHEAD`] is `false`. Otherwise the stored CRC must
    /// match CRC32-C of the bytes from [`Self::ATTRIBUTES_OFFSET`] to the end of
    /// `buffer`. Short size or CRC fields are [`Error::protocol`] `need 4 bytes`.
    pub fn is_valid(buffer: &[u8]) -> Result<bool> {
        if Self::encoded_size_in_bytes(buffer)? < Self::RECORD_BATCH_OVERHEAD {
            return Ok(false);
        }
        let crc_off = buf::usize_from_i32(Self::CRC_OFFSET)?;
        let stored = buf::read_unsigned_int_at(buffer, crc_off)?;
        let attr_off = buf::usize_from_i32(Self::ATTRIBUTES_OFFSET)?;
        let rest = buffer
            .get(attr_off..)
            .ok_or_else(|| Error::protocol("short attributes field"))?;
        Ok(stored == i64::from(crc32c::crc32c(rest)))
    }

    /// Java `DefaultRecordBatch.ensureValid`.
    ///
    /// Declared size ([`Self::encoded_size_in_bytes`]) below
    /// [`Self::RECORD_BATCH_OVERHEAD`] is [`Error::protocol`] matching Java
    /// `CorruptRecordException` (`Record batch is corrupt`). Otherwise a CRC
    /// mismatch is [`Error::protocol`] `Record is corrupt` (stored vs computed
    /// over bytes from [`Self::ATTRIBUTES_OFFSET`] to the end of `buffer`).
    /// Short size or CRC fields are [`Error::protocol`] `need 4 bytes`.
    /// Distinct from [`decode_record_batch`], which CRC-checks the declared
    /// body only.
    pub fn ensure_valid(buffer: &[u8]) -> Result<()> {
        let size_in_bytes = Self::encoded_size_in_bytes(buffer)?;
        if size_in_bytes < Self::RECORD_BATCH_OVERHEAD {
            return Err(Error::protocol(format!(
                "Record batch is corrupt (the size {size_in_bytes} is smaller than the minimum allowed overhead {})",
                Self::RECORD_BATCH_OVERHEAD
            )));
        }
        if Self::is_valid(buffer)? {
            return Ok(());
        }
        let crc_off = buf::usize_from_i32(Self::CRC_OFFSET)?;
        let stored = buf::read_unsigned_int_at(buffer, crc_off)?;
        let attr_off = buf::usize_from_i32(Self::ATTRIBUTES_OFFSET)?;
        let rest = buffer
            .get(attr_off..)
            .ok_or_else(|| Error::protocol("short attributes field"))?;
        let computed = i64::from(crc32c::crc32c(rest));
        Err(Error::protocol(format!(
            "Record is corrupt (stored crc = {stored}, computed crc = {computed})"
        )))
    }

    /// Java `DefaultRecordBatch.sizeInBytes(Iterable)` (uncompressed).
    ///
    /// Empty is `0` (not [`Self::RECORD_BATCH_OVERHEAD`]). Offset deltas are
    /// `0..n`; timestamps are relative to the first record.
    pub fn size_in_bytes_of(records: &[Record]) -> Result<i32> {
        record_batch_size_in_bytes(records, None)
    }

    /// Java `DefaultRecordBatch.sizeInBytes(long, Iterable)` (uncompressed).
    ///
    /// Empty is `0`. Offset deltas are `record.offset - base_offset`;
    /// timestamps are relative to the first record.
    pub fn size_in_bytes_from(base_offset: i64, records: &[Record]) -> Result<i32> {
        record_batch_size_in_bytes(records, Some(base_offset))
    }

    /// Java `DefaultRecordBatch.estimateBatchSizeUpperBound` (one-record batch
    /// overhead; compression is not included).
    pub fn estimate_batch_size_upper_bound(
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[Header],
    ) -> Result<i32> {
        buf::i32_from_usize(
            buf::usize_from_i32(Self::RECORD_BATCH_OVERHEAD)?
                + buf::usize_from_i32(record_size_upper_bound(key, value, headers)?)?,
        )
    }

    fn encoded(&self) -> Result<BytesMut> {
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, self)?;
        Ok(buf)
    }
}

impl fmt::Display for RecordBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let crc = self.checksum().unwrap_or(0);
        let compression = self.compression_type().unwrap_or(Compression::None);
        f.write_str("RecordBatch(magic=")?;
        write!(f, "{}", self.magic())?;
        f.write_str(", offsets=[")?;
        write!(f, "{}, {}", self.base_offset(), self.last_offset())?;
        f.write_str("], sequence=[")?;
        write!(f, "{}, {}", self.base_sequence(), self.last_sequence())?;
        f.write_str("], isTransactional=")?;
        write!(f, "{}", self.is_transactional())?;
        f.write_str(", isControlBatch=")?;
        write!(f, "{}", self.is_control_batch())?;
        f.write_str(", compression=")?;
        write!(f, "{compression}")?;
        f.write_str(", timestampType=")?;
        write!(f, "{}", self.timestamp_type())?;
        f.write_str(", crc=")?;
        write!(f, "{crc}")?;
        f.write_str(")")
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
    /// Partition leader epoch, or [`RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub partition_leader_epoch: i32,
    /// Attribute bits: compression in the low 3, plus
    /// [`ATTR_TIMESTAMP_TYPE`] / [`ATTR_TRANSACTIONAL`] / [`ATTR_CONTROL`] /
    /// [`ATTR_DELETE_HORIZON`].
    pub attributes: i16,
    /// Timestamp of the first record (milliseconds since the Unix epoch).
    pub base_timestamp: i64,
    /// Max timestamp among records in this batch.
    pub max_timestamp: i64,
    /// Idempotent / transactional producer id, or [`RecordBatch::NO_PRODUCER_ID`].
    pub producer_id: i64,
    /// Producer epoch, or [`RecordBatch::NO_PRODUCER_EPOCH`].
    pub producer_epoch: i16,
    /// First sequence number in this batch, or [`RecordBatch::NO_SEQUENCE`].
    pub base_sequence: i32,
    /// Number of records that will be written.
    pub count: i32,
}

impl Default for BatchHeader {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: RecordBatch::NO_PARTITION_LEADER_EPOCH,
            attributes: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: RecordBatch::NO_PRODUCER_ID,
            producer_epoch: RecordBatch::NO_PRODUCER_EPOCH,
            base_sequence: RecordBatch::NO_SEQUENCE,
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
        Records::LOG_OVERHEAD as usize + buf::usize_from_i32(batch_len).unwrap_or(0)
    );
    Ok(())
}

fn nullable_bytes_len(bytes: Option<&[u8]>) -> usize {
    match bytes {
        None => buf::varint_size(-1),
        Some(b) => buf::varint_size(i32::try_from(b.len()).unwrap_or(i32::MAX)) + b.len(),
    }
}

/// Java `AbstractRecords.estimateCompressedSizeInBytes`.
fn estimate_compressed_size_in_bytes(size: i32, compression: Compression) -> i32 {
    if compression == Compression::None {
        size
    } else {
        (size / 2).clamp(1024, 65_536)
    }
}
fn size_of_key_value_headers(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[Header],
) -> Result<usize> {
    let mut size = nullable_bytes_len(key)
        + nullable_bytes_len(value)
        + buf::varint_size(buf::i32_from_usize(headers.len())?);
    for h in headers {
        let header_key_size = buf::utf8_length(&h.key);
        size += buf::varint_size(header_key_size) + h.key.len();
        size += nullable_bytes_len(h.value.as_deref());
    }
    Ok(size)
}

/// Java `DefaultRecord.recordSizeUpperBound`.
fn record_size_upper_bound(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[Header],
) -> Result<i32> {
    buf::i32_from_usize(
        buf::usize_from_i32(Record::MAX_RECORD_OVERHEAD)?
            + size_of_key_value_headers(key, value, headers)?,
    )
}

/// Java `DefaultRecord.sizeOfBodyInBytes` (attributes + deltas + key/value/headers).
fn record_body_size(
    rec: &EncodeRecord<'_>,
    offset_delta: i32,
    timestamp_delta: i64,
) -> Result<i32> {
    buf::i32_from_usize(
        1 + buf::varlong_size(timestamp_delta)
            + buf::varint_size(offset_delta)
            + size_of_key_value_headers(rec.key, rec.value, rec.headers)?,
    )
}

/// Java `DefaultRecordBatch.sizeInBytes` static helpers.
///
/// `None` base offset is `0..n` deltas (SimpleRecord). `Some` uses
/// `record.offset - base`. Empty is `0`.
fn record_batch_size_in_bytes(records: &[Record], base_offset: Option<i64>) -> Result<i32> {
    let Some((first, rest)) = records.split_first() else {
        return Ok(0);
    };
    let mut size = buf::usize_from_i32(RecordBatch::RECORD_BATCH_OVERHEAD)?;
    let base_timestamp = first.timestamp;
    let first_delta = match base_offset {
        Some(base) => i32::try_from(first.offset.wrapping_sub(base))
            .map_err(|_| Error::protocol("offset delta exceeds i32"))?,
        None => 0,
    };
    size += buf::usize_from_i32(first.size_in_bytes(first_delta, 0)?)?;
    for (i, rec) in rest.iter().enumerate() {
        let offset_delta = match base_offset {
            Some(base) => i32::try_from(rec.offset.wrapping_sub(base))
                .map_err(|_| Error::protocol("offset delta exceeds i32"))?,
            None => buf::i32_from_usize(i.saturating_add(1))?,
        };
        let timestamp_delta = rec.timestamp.wrapping_sub(base_timestamp);
        size += buf::usize_from_i32(rec.size_in_bytes(offset_delta, timestamp_delta)?)?;
    }
    buf::i32_from_usize(size)
}

fn encode_record(
    buf: &mut BytesMut,
    rec: &EncodeRecord<'_>,
    offset_delta: i32,
    timestamp_delta: i64,
) -> crate::error::Result<()> {
    let inner = record_body_size(rec, offset_delta, timestamp_delta)?;
    buf::put_varint(buf, inner);
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
        buf::put_varint(buf, buf::utf8_length(&h.key));
        buf.extend_from_slice(h.key.as_bytes());
        match h.value.as_deref() {
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
///
/// Nested records match Java `DefaultRecord.readFrom`: a negative header
/// count, a header count larger than remaining bytes, a negative header
/// key size, a declared body larger than remaining, and leftover payload
/// bytes after headers use the Java `InvalidRecordException` messages.
/// Batch record-count checks match Java `DefaultRecordBatch.RecordIterator`
/// (`Found invalid record count`, leftover records after the declared
/// count, premature EOF when the count is larger than the payload).
/// Size and CRC checks match Java `DefaultRecordBatch.ensureValid`.
pub fn decode_record_batch<B: Buf>(buf: &mut B) -> Result<RecordBatch> {
    let base_offset = buf::get_i64(buf)?;
    let batch_len = buf::get_i32(buf)?;
    let size_in_bytes = Records::LOG_OVERHEAD.wrapping_add(batch_len);
    if size_in_bytes < RecordBatch::RECORD_BATCH_OVERHEAD {
        return Err(Error::protocol(format!(
            "Record batch is corrupt (the size {size_in_bytes} is smaller than the minimum allowed overhead {})",
            RecordBatch::RECORD_BATCH_OVERHEAD
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
            "Record is corrupt (stored crc = {crc}, computed crc = {computed})"
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
        return Err(Error::protocol(format!(
            "Found invalid record count {count} in magic v{magic} batch"
        )));
    }
    let mut records_cur = match compression {
        Compression::None => body,
        Compression::Gzip => Bytes::from(gzip_decompress(&body)?),
        Compression::Snappy => Bytes::from(snappy_decompress(&body)?),
        Compression::Lz4 => Bytes::from(lz4_decompress(&body)?),
    };
    let count_usize = buf::usize_from_i32(count)?;
    let mut records = Vec::with_capacity(count_usize);
    for _ in 0..count_usize {
        if !records_cur.has_remaining() {
            return Err(Error::protocol(
                "Incorrect declared batch size, premature EOF reached",
            ));
        }
        records.push(decode_record(
            &mut records_cur,
            base_offset,
            base_timestamp,
        )?);
    }
    if count > 0 && records_cur.has_remaining() {
        return Err(Error::protocol(
            "Incorrect declared batch size, records still remaining in file",
        ));
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

/// Java `DefaultRecord.readFrom` (magic-v2 inner record).
fn decode_record<B: Buf>(buf: &mut B, base_offset: i64, base_timestamp: i64) -> Result<Record> {
    let size_of_body_in_bytes = buf::get_varint(buf)?;
    if size_of_body_in_bytes < 0 {
        return Err(Error::protocol("negative record length"));
    }
    let remaining = buf.remaining();
    let len_usize = buf::usize_from_i32(size_of_body_in_bytes)?;
    if remaining < len_usize {
        return Err(Error::protocol(format!(
            "Invalid record size: expected {size_of_body_in_bytes} bytes in record payload, but instead the buffer has only {remaining} remaining bytes."
        )));
    }
    let mut inner = buf.copy_to_bytes(len_usize);
    let _attributes = buf::get_i8(&mut inner)?;
    let timestamp_delta = buf::get_varlong(&mut inner)?;
    let offset_delta = buf::get_varint(&mut inner)?;
    let key = read_bytes_varint(&mut inner)?;
    let value = read_bytes_varint(&mut inner)?;
    let num_headers = buf::get_varint(&mut inner)?;
    if num_headers < 0 {
        return Err(Error::protocol(format!(
            "Found invalid number of record headers {num_headers}"
        )));
    }
    let num_headers_usize = buf::usize_from_i32(num_headers)?;
    if num_headers_usize > inner.remaining() {
        return Err(Error::protocol(format!(
            "Found invalid number of record headers. {num_headers} is larger than the remaining size of the buffer"
        )));
    }
    let mut headers = Vec::with_capacity(num_headers_usize);
    for _ in 0..num_headers_usize {
        let header_key_size = buf::get_varint(&mut inner)?;
        if header_key_size < 0 {
            return Err(Error::protocol(format!(
                "Invalid negative header key size {header_key_size}"
            )));
        }
        let key_len_usize = buf::usize_from_i32(header_key_size)?;
        buf::need(&inner, key_len_usize)?;
        let mut key_buf = vec![0u8; key_len_usize];
        inner.copy_to_slice(&mut key_buf);
        let key = String::from_utf8(key_buf).map_err(|e| Error::protocol(e.to_string()))?;
        let value = read_bytes_varint(&mut inner)?;
        headers.push(Header { key, value });
    }
    if inner.remaining() != 0 {
        let consumed = len_usize - inner.remaining();
        return Err(Error::protocol(format!(
            "Invalid record size: expected to read {size_of_body_in_bytes} bytes in record payload, but instead read {consumed}"
        )));
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
    buf::read_bytes(buf, len)
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
        assert!(rec.has_magic(MAGIC_V2));
        assert!(rec.has_magic(3));
        assert!(!rec.has_magic(1));
        assert!(!rec.is_compressed());
        assert!(!rec.has_timestamp_type(TimestampType::CreateTime));
        assert!(!rec.has_timestamp_type(TimestampType::LogAppendTime));
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
        assert_eq!(RecordBatch::MAGIC_VALUE_V0, 0);
        assert_eq!(RecordBatch::MAGIC_VALUE_V1, 1);
        assert_eq!(RecordBatch::MAGIC_VALUE_V2, 2);
        assert_eq!(RecordBatch::CURRENT_MAGIC_VALUE, 2);
        assert_eq!(RecordBatch::RECORD_BATCH_OVERHEAD, 61);
        assert_eq!(RecordBatch::CRC_OFFSET, 17);
        assert_eq!(RecordBatch::ATTRIBUTES_OFFSET, 21);
        assert_eq!(RecordBatch::LAST_OFFSET_DELTA_OFFSET, 23);
        assert_eq!(RecordBatch::BASE_TIMESTAMP_OFFSET, 27);
        assert_eq!(RecordBatch::PRODUCER_ID_OFFSET, 43);
        assert_eq!(RecordBatch::BASE_SEQUENCE_OFFSET, 53);
        assert_eq!(RecordBatch::RECORDS_COUNT_OFFSET, 57);
        assert_eq!(batch.base_offset(), 0);
        assert_eq!(batch.last_offset(), 0);
        assert_eq!(batch.next_offset(), 1);
        assert_eq!(batch.last_sequence(), RecordBatch::NO_SEQUENCE);
        assert!(!batch.is_compressed());
        assert_eq!(batch.count(), 1);
        assert_eq!(batch.count_or_null(), Some(1));
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
        assert_eq!(empty_batch.count_or_null(), Some(0));
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
        let header = BatchHeader::default();
        assert_eq!(
            header.partition_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(header.producer_id, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(header.producer_epoch, RecordBatch::NO_PRODUCER_EPOCH);
        assert_eq!(header.base_sequence, RecordBatch::NO_SEQUENCE);
        let mut zero_pid = RecordBatch::from_records(vec![empty.clone()]);
        zero_pid.producer_id = 0;
        assert!(
            RecordBatch::NO_PRODUCER_ID < zero_pid.producer_id,
            "Java AbstractRecordBatch.hasProducerId is NO_PRODUCER_ID < producerId"
        );
        assert!(zero_pid.has_producer_id());
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
    fn control_record_type_and_end_txn_marker_match_java() {
        assert_eq!(ControlRecordType::Abort.type_id(), 0);
        assert_eq!(ControlRecordType::Commit.type_id(), 1);
        assert_eq!(ControlRecordType::LeaderChange.type_id(), 2);
        assert_eq!(ControlRecordType::SnapshotHeader.type_id(), 3);
        assert_eq!(ControlRecordType::SnapshotFooter.type_id(), 4);
        assert_eq!(ControlRecordType::KraftVersion.type_id(), 5);
        assert_eq!(ControlRecordType::KraftVoters.type_id(), 6);
        assert_eq!(ControlRecordType::Unknown.type_id(), -1);
        assert_eq!(ControlRecordType::from_type_id(0), ControlRecordType::Abort);
        assert_eq!(
            ControlRecordType::from_type_id(1),
            ControlRecordType::Commit
        );
        assert_eq!(
            ControlRecordType::from_type_id(99),
            ControlRecordType::Unknown
        );
        assert_eq!(ControlRecordType::Abort.to_string(), "ABORT");
        assert_eq!(ControlRecordType::Commit.to_string(), "COMMIT");
        assert_eq!(ControlRecordType::LeaderChange.to_string(), "LEADER_CHANGE");
        assert_eq!(ControlRecordType::Unknown.to_string(), "UNKNOWN");
        let commit_key = ControlRecordType::Commit.record_key().unwrap();
        assert_eq!(commit_key, [0, 0, 0, 1]);
        assert_eq!(
            ControlRecordType::parse(&commit_key).unwrap(),
            ControlRecordType::Commit
        );
        assert_eq!(
            ControlRecordType::parse_type_id(&[0, 1, 0, 0]).unwrap(),
            0,
            "newer key version still reads type at offset 2"
        );
        assert!(ControlRecordType::parse_type_id(&[0, 0, 0]).is_err());
        assert!(ControlRecordType::parse_type_id(&[0xff, 0xff, 0, 1]).is_err());
        assert!(ControlRecordType::Unknown.record_key().is_err());
        assert!(EndTransactionMarker::new(ControlRecordType::LeaderChange, 1).is_err());
        let marker = EndTransactionMarker::new(ControlRecordType::Abort, 7).unwrap();
        assert_eq!(marker.control_type(), ControlRecordType::Abort);
        assert_eq!(marker.coordinator_epoch(), 7);
        assert_eq!(marker.serialize_value(), [0, 0, 0, 0, 0, 7]);
        let batch = RecordBatch::with_end_transaction_marker(10, 99, 3, 42, 1, &marker).unwrap();
        assert!(batch.is_transactional());
        assert!(batch.is_control_batch());
        assert_eq!(batch.base_offset(), 10);
        assert_eq!(batch.partition_leader_epoch(), 3);
        assert_eq!(batch.producer_id(), 42);
        assert_eq!(batch.producer_epoch(), 1);
        assert_eq!(batch.base_sequence(), RecordBatch::NO_SEQUENCE);
        assert_eq!(batch.count(), 1);
        let decoded_marker =
            EndTransactionMarker::deserialize(batch.records().first().expect("marker record"))
                .unwrap();
        assert_eq!(decoded_marker, marker);
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch).unwrap();
        let decoded = decode_record_batch(&mut &buf[..]).unwrap();
        assert!(decoded.is_control_batch());
        assert!(decoded.is_transactional());
        assert_eq!(decoded.base_offset(), 10);
        assert_eq!(
            EndTransactionMarker::deserialize(decoded.records().first().expect("marker record"))
                .unwrap(),
            marker
        );
        let commit = EndTransactionMarker::new(ControlRecordType::Commit, 0).unwrap();
        assert_eq!(
            EndTransactionMarker::deserialize(
                &Record::from_end_transaction_marker(0, &commit).unwrap()
            )
            .unwrap(),
            commit
        );
        assert!(EndTransactionMarker::deserialize(&Record {
            offset: 0,
            timestamp: 0,
            key: Some(Bytes::copy_from_slice(
                &ControlRecordType::LeaderChange.record_key().unwrap()
            )),
            value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
            headers: vec![],
        })
        .is_err());
        assert!(EndTransactionMarker::deserialize(&Record {
            offset: 0,
            timestamp: 0,
            key: Some(Bytes::copy_from_slice(
                &ControlRecordType::Abort.record_key().unwrap()
            )),
            value: Some(Bytes::from_static(&[0, 0, 0])),
            headers: vec![],
        })
        .is_err());
        assert_eq!(Records::OFFSET_OFFSET, 0);
        assert_eq!(Records::OFFSET_LENGTH, 8);
        assert_eq!(Records::SIZE_OFFSET, 8);
        assert_eq!(Records::SIZE_LENGTH, 4);
        assert_eq!(Records::LOG_OVERHEAD, 12);
        assert_eq!(Records::MAGIC_OFFSET, 16);
        assert_eq!(Records::MAGIC_LENGTH, 1);
        assert_eq!(Records::HEADER_SIZE_UP_TO_MAGIC, 17);
        assert_eq!(RecordBatch::CRC_OFFSET, Records::HEADER_SIZE_UP_TO_MAGIC);
        assert_eq!(
            RecordBatch::RECORD_BATCH_OVERHEAD - Records::LOG_OVERHEAD,
            49
        );
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
            unknown.to_string().contains("Invalid timestamp type bogus"),
            "{unknown}"
        );
        let unknown_codec = Compression::from_name("bogus").unwrap_err();
        assert!(
            unknown_codec
                .to_string()
                .contains("Unknown compression name: bogus"),
            "{unknown_codec}"
        );
        assert_eq!(Compression::from_name("gzip").unwrap(), Compression::Gzip);
        assert_eq!(Compression::from_name("").unwrap(), Compression::None);
        assert_eq!(
            Compression::Gzip.default_level().unwrap(),
            Compression::GZIP_DEFAULT_LEVEL
        );
        assert_eq!(
            Compression::Gzip.min_level().unwrap(),
            Compression::GZIP_MIN_LEVEL
        );
        assert_eq!(
            Compression::Gzip.max_level().unwrap(),
            Compression::GZIP_MAX_LEVEL
        );
        assert_eq!(Compression::GZIP_DEFAULT_LEVEL, -1);
        assert_eq!(Compression::GZIP_MIN_LEVEL, 1);
        assert_eq!(Compression::GZIP_MAX_LEVEL, 9);
        assert_eq!(
            Compression::Lz4.default_level().unwrap(),
            Compression::LZ4_DEFAULT_LEVEL
        );
        assert_eq!(
            Compression::Lz4.min_level().unwrap(),
            Compression::LZ4_MIN_LEVEL
        );
        assert_eq!(
            Compression::Lz4.max_level().unwrap(),
            Compression::LZ4_MAX_LEVEL
        );
        assert_eq!(Compression::LZ4_DEFAULT_LEVEL, 9);
        assert_eq!(Compression::LZ4_MIN_LEVEL, 1);
        assert_eq!(Compression::LZ4_MAX_LEVEL, 17);
        let none_lvl = Compression::None.default_level().unwrap_err();
        assert!(
            matches!(none_lvl, crate::error::Error::Unsupported(_)),
            "{none_lvl}"
        );
        assert!(
            none_lvl
                .to_string()
                .contains("Compression levels are not defined for this compression type: none"),
            "{none_lvl}"
        );
        let snappy_lvl = Compression::Snappy.min_level().unwrap_err();
        assert!(
            snappy_lvl
                .to_string()
                .contains("Compression levels are not defined for this compression type: snappy"),
            "{snappy_lvl}"
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

    fn wrap_record_body(inner: &[u8]) -> BytesMut {
        let mut rec = BytesMut::new();
        buf::put_varint(&mut rec, buf::i32_from_usize(inner.len()).unwrap());
        rec.extend_from_slice(inner);
        rec
    }

    fn decode_record_err(inner: &[u8]) -> String {
        let rec = wrap_record_body(inner);
        decode_record(&mut &rec[..], 0, 0).unwrap_err().to_string()
    }

    fn record_body_before_headers() -> BytesMut {
        let mut inner = BytesMut::new();
        inner.put_i8(0);
        buf::put_varlong(&mut inner, 0);
        buf::put_varint(&mut inner, 0);
        buf::put_varint(&mut inner, -1);
        buf::put_varint(&mut inner, -1);
        inner
    }

    #[test]
    fn decode_record_invalid_headers_match_java() {
        let mut negative_count = record_body_before_headers();
        buf::put_varint(&mut negative_count, -1);
        let negative_count_err = decode_record_err(&negative_count);
        assert!(
            negative_count_err.contains("Found invalid number of record headers -1"),
            "{negative_count_err}"
        );

        let mut too_many = record_body_before_headers();
        buf::put_varint(&mut too_many, 10);
        let too_many_err = decode_record_err(&too_many);
        assert!(
            too_many_err.contains(
                "Found invalid number of record headers. 10 is larger than the remaining size of the buffer"
            ),
            "{too_many_err}"
        );

        let mut negative_key = record_body_before_headers();
        buf::put_varint(&mut negative_key, 1);
        buf::put_varint(&mut negative_key, -1);
        let negative_key_err = decode_record_err(&negative_key);
        assert!(
            negative_key_err.contains("Invalid negative header key size -1"),
            "{negative_key_err}"
        );

        let mut leftover = record_body_before_headers();
        buf::put_varint(&mut leftover, 0);
        let consumed = leftover.len();
        leftover.put_u8(0);
        let size = leftover.len();
        let leftover_err = decode_record_err(&leftover);
        assert!(
            leftover_err.contains(&format!(
                "Invalid record size: expected to read {size} bytes in record payload, but instead read {consumed}"
            )),
            "{leftover_err}"
        );

        let mut short = BytesMut::new();
        buf::put_varint(&mut short, 10);
        short.extend_from_slice(&[0, 1, 2]);
        let short_err = decode_record(&mut &short[..], 0, 0)
            .unwrap_err()
            .to_string();
        assert!(
            short_err.contains(
                "Invalid record size: expected 10 bytes in record payload, but instead the buffer has only 3 remaining bytes."
            ),
            "{short_err}"
        );
    }

    fn sample_record() -> Record {
        Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"x")),
            headers: vec![],
        }
    }

    fn patch_batch_count(buf: &mut [u8], count: i32) {
        let count_off = RecordBatch::RECORDS_COUNT_OFFSET as usize;
        let crc_off = RecordBatch::CRC_OFFSET as usize;
        buf[count_off..count_off + 4].copy_from_slice(&count.to_be_bytes());
        let crc_start = crc_off + 4;
        let crc = crc32c::crc32c(&buf[crc_start..]);
        buf[crc_off..crc_off + 4].copy_from_slice(&crc.to_be_bytes());
    }

    #[test]
    fn decode_record_batch_count_checks_match_java() {
        let rec = sample_record();
        let mut two = BytesMut::new();
        encode_record_batch(
            &mut two,
            &RecordBatch::from_records(vec![rec.clone(), rec.clone()]),
        )
        .unwrap();

        let mut leftover = two.clone();
        patch_batch_count(&mut leftover, 1);
        let leftover_err = decode_record_batch(&mut leftover.as_ref())
            .unwrap_err()
            .to_string();
        assert!(
            leftover_err.contains("Incorrect declared batch size, records still remaining in file"),
            "{leftover_err}"
        );

        let mut one = BytesMut::new();
        encode_record_batch(&mut one, &RecordBatch::from_records(vec![rec.clone()])).unwrap();
        let mut eof = one.clone();
        patch_batch_count(&mut eof, 2);
        let eof_err = decode_record_batch(&mut eof.as_ref())
            .unwrap_err()
            .to_string();
        assert!(
            eof_err.contains("Incorrect declared batch size, premature EOF reached"),
            "{eof_err}"
        );

        let mut negative = one.clone();
        patch_batch_count(&mut negative, -1);
        let negative_err = decode_record_batch(&mut negative.as_ref())
            .unwrap_err()
            .to_string();
        assert!(
            negative_err.contains("Found invalid record count -1 in magic v2 batch"),
            "{negative_err}"
        );

        let mut zero = two.clone();
        patch_batch_count(&mut zero, 0);
        let skipped = decode_record_batch(&mut zero.as_ref()).unwrap();
        assert!(skipped.records.is_empty());
    }

    #[test]
    fn decode_record_batch_ensure_valid_match_java() {
        let mut too_small = BytesMut::new();
        too_small.put_i64(0);
        too_small.put_i32(0);
        let small_err = decode_record_batch(&mut too_small.as_ref())
            .unwrap_err()
            .to_string();
        assert!(
            small_err.contains(
                "Record batch is corrupt (the size 12 is smaller than the minimum allowed overhead 61)"
            ),
            "{small_err}"
        );

        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &RecordBatch::from_records(vec![sample_record()])).unwrap();
        let crc_off = RecordBatch::CRC_OFFSET as usize;
        let attr = crc_off + 4;
        buf[attr] ^= 0xff;
        let stored = u32::from_be_bytes(buf[crc_off..crc_off + 4].try_into().unwrap());
        let computed = crc32c::crc32c(&buf[attr..]);
        let crc_err = decode_record_batch(&mut buf.as_ref())
            .unwrap_err()
            .to_string();
        assert!(
            crc_err.contains(&format!(
                "Record is corrupt (stored crc = {stored}, computed crc = {computed})"
            )),
            "{crc_err}"
        );
    }

    #[test]
    fn is_valid_matches_java_default_record_batch() {
        let mut encoded = BytesMut::new();
        encode_record_batch(
            &mut encoded,
            &RecordBatch::from_records(vec![sample_record()]),
        )
        .unwrap();
        assert!(RecordBatch::is_valid(&encoded).unwrap());

        let mut flipped = encoded.clone();
        let attr = RecordBatch::ATTRIBUTES_OFFSET as usize;
        flipped[attr] ^= 0xff;
        assert!(!RecordBatch::is_valid(&flipped).unwrap());

        assert!(!RecordBatch::is_valid(&[0; 12]).unwrap());
        let err = RecordBatch::is_valid(&[0; 11]).unwrap_err().to_string();
        assert!(err.contains("need 4 bytes"), "{err}");

        let mut undersized = encoded.clone();
        undersized[8..12].copy_from_slice(&0i32.to_be_bytes());
        assert!(!RecordBatch::is_valid(&undersized).unwrap());

        let mut short_crc = [0u8; 12];
        short_crc[8..12].copy_from_slice(&49i32.to_be_bytes());
        let err = RecordBatch::is_valid(&short_crc).unwrap_err().to_string();
        assert!(err.contains("need 4 bytes"), "{err}");

        let mut header_only = [0u8; 21];
        header_only[8..12].copy_from_slice(&49i32.to_be_bytes());
        assert!(RecordBatch::is_valid(&header_only).unwrap());
    }

    #[test]
    fn encoded_size_in_bytes_matches_java_default_record_batch() {
        let mut encoded = BytesMut::new();
        encode_record_batch(
            &mut encoded,
            &RecordBatch::from_records(vec![sample_record()]),
        )
        .unwrap();
        let encoded_len = i32::try_from(encoded.len()).unwrap();
        assert_eq!(
            RecordBatch::encoded_size_in_bytes(&encoded).unwrap(),
            encoded_len
        );
        assert_eq!(
            RecordBatch::from_records(vec![sample_record()])
                .size_in_bytes()
                .unwrap(),
            encoded_len
        );

        assert_eq!(RecordBatch::encoded_size_in_bytes(&[0; 12]).unwrap(), 12);

        let err = RecordBatch::encoded_size_in_bytes(&[0; 11])
            .unwrap_err()
            .to_string();
        assert!(err.contains("need 4 bytes"), "{err}");

        let mut wrap = [0u8; 12];
        wrap[8..12].copy_from_slice(&i32::MAX.to_be_bytes());
        assert_eq!(
            RecordBatch::encoded_size_in_bytes(&wrap).unwrap(),
            Records::LOG_OVERHEAD.wrapping_add(i32::MAX)
        );

        let mut declared = encoded.clone();
        declared[8..12].copy_from_slice(&100i32.to_be_bytes());
        assert_eq!(RecordBatch::encoded_size_in_bytes(&declared).unwrap(), 112);
        assert_ne!(
            RecordBatch::encoded_size_in_bytes(&declared).unwrap(),
            encoded_len
        );
    }

    #[test]
    fn ensure_valid_matches_java_default_record_batch() {
        let mut encoded = BytesMut::new();
        encode_record_batch(
            &mut encoded,
            &RecordBatch::from_records(vec![sample_record()]),
        )
        .unwrap();
        RecordBatch::ensure_valid(&encoded).unwrap();

        let size_err = RecordBatch::ensure_valid(&[0; 12]).unwrap_err().to_string();
        assert!(
            size_err.contains(
                "Record batch is corrupt (the size 12 is smaller than the minimum allowed overhead 61)"
            ),
            "{size_err}"
        );

        let short = RecordBatch::ensure_valid(&[0; 11]).unwrap_err().to_string();
        assert!(short.contains("need 4 bytes"), "{short}");

        let mut undersized = encoded.clone();
        undersized[8..12].copy_from_slice(&0i32.to_be_bytes());
        let under = RecordBatch::ensure_valid(&undersized)
            .unwrap_err()
            .to_string();
        assert!(
            under.contains(
                "Record batch is corrupt (the size 12 is smaller than the minimum allowed overhead 61)"
            ),
            "{under}"
        );

        let mut flipped = encoded.clone();
        let attr = RecordBatch::ATTRIBUTES_OFFSET as usize;
        flipped[attr] ^= 0xff;
        let crc_off = RecordBatch::CRC_OFFSET as usize;
        let stored = u32::from_be_bytes(flipped[crc_off..crc_off + 4].try_into().unwrap());
        let computed = crc32c::crc32c(&flipped[attr..]);
        let crc_err = RecordBatch::ensure_valid(&flipped).unwrap_err().to_string();
        assert!(
            crc_err.contains(&format!(
                "Record is corrupt (stored crc = {stored}, computed crc = {computed})"
            )),
            "{crc_err}"
        );

        let mut short_crc = [0u8; 12];
        short_crc[8..12].copy_from_slice(&49i32.to_be_bytes());
        let err = RecordBatch::ensure_valid(&short_crc)
            .unwrap_err()
            .to_string();
        assert!(err.contains("need 4 bytes"), "{err}");

        let mut header_only = [0u8; 21];
        header_only[8..12].copy_from_slice(&49i32.to_be_bytes());
        RecordBatch::ensure_valid(&header_only).unwrap();

        let mut trailing = encoded.clone();
        trailing.put_u8(0xff);
        assert!(!RecordBatch::is_valid(&trailing).unwrap());
        let trail_err = RecordBatch::ensure_valid(&trailing)
            .unwrap_err()
            .to_string();
        assert!(trail_err.contains("Record is corrupt"), "{trail_err}");
        let mut trail_slice = trailing.as_ref();
        drop(decode_record_batch(&mut trail_slice).unwrap());
        assert_eq!(trail_slice, &[0xff]);
    }

    #[test]
    fn encoded_last_offset_matches_java_default_record_batch() {
        let empty = RecordBatch::from_records(vec![]);
        let mut empty_buf = BytesMut::new();
        encode_record_batch(&mut empty_buf, &empty).unwrap();
        assert_eq!(RecordBatch::encoded_last_offset(&empty_buf).unwrap(), 0);
        assert_eq!(empty.last_offset(), -1);
        assert_eq!(RecordBatch::encoded_next_offset(&empty_buf).unwrap(), 1);
        assert_eq!(empty.next_offset(), 0);

        let mut two = BytesMut::new();
        encode_record_batch(
            &mut two,
            &RecordBatch::from_records(vec![sample_record(), sample_record()]),
        )
        .unwrap();
        assert_eq!(RecordBatch::encoded_last_offset(&two).unwrap(), 1);
        assert_eq!(RecordBatch::encoded_next_offset(&two).unwrap(), 2);

        let mut mutated = two.clone();
        mutated[23..27].copy_from_slice(&5i32.to_be_bytes());
        assert_eq!(RecordBatch::encoded_last_offset(&mutated).unwrap(), 5);
        assert_eq!(RecordBatch::encoded_next_offset(&mutated).unwrap(), 6);

        let mut wrap = [0u8; 27];
        wrap[0..8].copy_from_slice(&i64::MAX.to_be_bytes());
        wrap[23..27].copy_from_slice(&1i32.to_be_bytes());
        assert_eq!(RecordBatch::encoded_last_offset(&wrap).unwrap(), i64::MIN);
        assert_eq!(
            RecordBatch::encoded_next_offset(&wrap).unwrap(),
            i64::MIN.wrapping_add(1)
        );

        let short_base = RecordBatch::encoded_last_offset(&[0; 7])
            .unwrap_err()
            .to_string();
        assert!(short_base.contains("need 8 bytes"), "{short_base}");

        let short_delta = RecordBatch::encoded_last_offset(&[0; 26])
            .unwrap_err()
            .to_string();
        assert!(short_delta.contains("need 4 bytes"), "{short_delta}");
    }

    #[test]
    fn encoded_last_sequence_matches_java_default_record_batch() {
        let empty = RecordBatch::from_records(vec![]);
        let mut empty_buf = BytesMut::new();
        encode_record_batch(&mut empty_buf, &empty).unwrap();
        assert_eq!(
            RecordBatch::encoded_last_sequence(&empty_buf).unwrap(),
            RecordBatch::NO_SEQUENCE
        );
        assert_eq!(empty.last_sequence(), RecordBatch::NO_SEQUENCE);

        let mut batch = RecordBatch::from_records(vec![sample_record(), sample_record()]);
        batch.base_sequence = 5;
        let mut encoded = BytesMut::new();
        encode_record_batch(&mut encoded, &batch).unwrap();
        assert_eq!(RecordBatch::encoded_last_sequence(&encoded).unwrap(), 6);
        assert_eq!(batch.last_sequence(), 6);

        let mut mutated = encoded.clone();
        mutated[23..27].copy_from_slice(&5i32.to_be_bytes());
        assert_eq!(RecordBatch::encoded_last_sequence(&mutated).unwrap(), 10);
        assert_eq!(batch.last_sequence(), 6);

        let mut no_seq = [0u8; 57];
        no_seq[23..27].copy_from_slice(&5i32.to_be_bytes());
        no_seq[53..57].copy_from_slice(&RecordBatch::NO_SEQUENCE.to_be_bytes());
        assert_eq!(
            RecordBatch::encoded_last_sequence(&no_seq).unwrap(),
            RecordBatch::NO_SEQUENCE
        );

        let mut wrap = [0u8; 57];
        wrap[23..27].copy_from_slice(&1i32.to_be_bytes());
        wrap[53..57].copy_from_slice(&i32::MAX.to_be_bytes());
        assert_eq!(
            RecordBatch::encoded_last_sequence(&wrap).unwrap(),
            RecordBatch::increment_sequence(i32::MAX, 1)
        );

        let short = RecordBatch::encoded_last_sequence(&[0; 52])
            .unwrap_err()
            .to_string();
        assert!(short.contains("need 4 bytes"), "{short}");
    }

    #[test]
    fn encoded_delete_horizon_ms_matches_java_default_record_batch() {
        let plain = RecordBatch::from_records(vec![sample_record()]);
        let mut plain_buf = BytesMut::new();
        encode_record_batch(&mut plain_buf, &plain).unwrap();
        assert_eq!(
            RecordBatch::encoded_delete_horizon_ms(&plain_buf).unwrap(),
            None
        );
        assert!(plain.delete_horizon_ms().is_none());

        let horizon = plain.clone().with_delete_horizon(true);
        let mut horizon_buf = BytesMut::new();
        encode_record_batch(&mut horizon_buf, &horizon).unwrap();
        assert_eq!(
            RecordBatch::encoded_delete_horizon_ms(&horizon_buf).unwrap(),
            Some(1)
        );
        assert_eq!(horizon.delete_horizon_ms(), Some(1));

        let mut mutated = horizon_buf.clone();
        mutated[27..35].copy_from_slice(&99i64.to_be_bytes());
        assert_eq!(
            RecordBatch::encoded_delete_horizon_ms(&mutated).unwrap(),
            Some(99)
        );
        assert_eq!(horizon.delete_horizon_ms(), Some(1));

        assert_eq!(
            RecordBatch::encoded_delete_horizon_ms(&[0; 23]).unwrap(),
            None
        );
        let short_attr = RecordBatch::encoded_delete_horizon_ms(&[0; 22])
            .unwrap_err()
            .to_string();
        assert!(short_attr.contains("need 2 bytes"), "{short_attr}");

        let mut flag_only = [0u8; 23];
        flag_only[21..23].copy_from_slice(&ATTR_DELETE_HORIZON.to_be_bytes());
        let short_ts = RecordBatch::encoded_delete_horizon_ms(&flag_only)
            .unwrap_err()
            .to_string();
        assert!(short_ts.contains("need 8 bytes"), "{short_ts}");
    }

    #[test]
    fn encoded_is_transactional_matches_java_default_record_batch() {
        let plain = RecordBatch::from_records(vec![sample_record()]);
        let mut plain_buf = BytesMut::new();
        encode_record_batch(&mut plain_buf, &plain).unwrap();
        assert!(!RecordBatch::encoded_is_transactional(&plain_buf).unwrap());
        assert!(!RecordBatch::encoded_is_control_batch(&plain_buf).unwrap());
        assert_eq!(
            RecordBatch::encoded_timestamp_type(&plain_buf).unwrap(),
            TimestampType::CreateTime
        );

        let flagged = plain
            .clone()
            .with_transactional(true)
            .with_control_batch(true)
            .with_timestamp_type(TimestampType::LogAppendTime);
        let mut flagged_buf = BytesMut::new();
        encode_record_batch(&mut flagged_buf, &flagged).unwrap();
        assert!(RecordBatch::encoded_is_transactional(&flagged_buf).unwrap());
        assert!(RecordBatch::encoded_is_control_batch(&flagged_buf).unwrap());
        assert_eq!(
            RecordBatch::encoded_timestamp_type(&flagged_buf).unwrap(),
            TimestampType::LogAppendTime
        );
        assert!(flagged.is_transactional());
        assert!(flagged.is_control_batch());
        assert_eq!(flagged.timestamp_type(), TimestampType::LogAppendTime);

        let mut mutated = flagged_buf.clone();
        mutated[21..23].copy_from_slice(&0i16.to_be_bytes());
        assert!(!RecordBatch::encoded_is_transactional(&mutated).unwrap());
        assert!(!RecordBatch::encoded_is_control_batch(&mutated).unwrap());
        assert_eq!(
            RecordBatch::encoded_timestamp_type(&mutated).unwrap(),
            TimestampType::CreateTime
        );
        assert!(flagged.is_transactional());
        assert!(flagged.is_control_batch());
        assert_eq!(flagged.timestamp_type(), TimestampType::LogAppendTime);

        assert!(!RecordBatch::encoded_is_transactional(&[0; 23]).unwrap());
        assert!(!RecordBatch::encoded_is_control_batch(&[0; 23]).unwrap());
        assert_eq!(
            RecordBatch::encoded_timestamp_type(&[0; 23]).unwrap(),
            TimestampType::CreateTime
        );
        let short = RecordBatch::encoded_is_transactional(&[0; 22])
            .unwrap_err()
            .to_string();
        assert!(short.contains("need 2 bytes"), "{short}");
        let short_ctrl = RecordBatch::encoded_is_control_batch(&[0; 22])
            .unwrap_err()
            .to_string();
        assert!(short_ctrl.contains("need 2 bytes"), "{short_ctrl}");
        let short_ts = RecordBatch::encoded_timestamp_type(&[0; 22])
            .unwrap_err()
            .to_string();
        assert!(short_ts.contains("need 2 bytes"), "{short_ts}");
    }

    #[test]
    fn encoded_has_producer_id_matches_java_abstract_record_batch() {
        let plain = RecordBatch::from_records(vec![sample_record()]);
        let mut plain_buf = BytesMut::new();
        encode_record_batch(&mut plain_buf, &plain).unwrap();
        assert!(!RecordBatch::encoded_has_producer_id(&plain_buf).unwrap());
        assert!(!plain.has_producer_id());

        let mut with_id = plain.clone();
        with_id.producer_id = 7;
        let mut with_id_buf = BytesMut::new();
        encode_record_batch(&mut with_id_buf, &with_id).unwrap();
        assert!(RecordBatch::encoded_has_producer_id(&with_id_buf).unwrap());
        assert!(with_id.has_producer_id());

        let mut mutated = with_id_buf.clone();
        mutated[43..51].copy_from_slice(&RecordBatch::NO_PRODUCER_ID.to_be_bytes());
        assert!(!RecordBatch::encoded_has_producer_id(&mutated).unwrap());
        assert!(with_id.has_producer_id());

        // Java `NO_PRODUCER_ID < 0` is true: a zero producer id counts as set.
        assert!(RecordBatch::encoded_has_producer_id(&[0; 51]).unwrap());

        let short = RecordBatch::encoded_has_producer_id(&[0; 42])
            .unwrap_err()
            .to_string();
        assert!(short.contains("need 8 bytes"), "{short}");
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

    #[test]
    fn record_and_batch_size_in_bytes_match_java() {
        let rec = Record {
            offset: 0,
            timestamp: 10,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"val")),
            headers: vec![Header::new("h", Bytes::from_static(b"v"))],
        };
        let offset_delta = 3;
        let timestamp_delta = 5;
        let body = rec
            .size_of_body_in_bytes(offset_delta, timestamp_delta)
            .unwrap();
        let size = rec.size_in_bytes(offset_delta, timestamp_delta).unwrap();
        assert_eq!(size, body + buf::size_of_varint(body));
        let mut encoded = BytesMut::new();
        encode_record(
            &mut encoded,
            &EncodeRecord::from_record(&rec),
            offset_delta,
            timestamp_delta,
        )
        .unwrap();
        assert_eq!(size, buf::i32_from_usize(encoded.len()).unwrap());

        let rec_a = Record {
            offset: 0,
            timestamp: 100,
            key: None,
            value: Some(Bytes::from_static(b"a")),
            headers: vec![],
        };
        let rec_b = Record {
            offset: 1,
            timestamp: 110,
            key: None,
            value: Some(Bytes::from_static(b"bb")),
            headers: vec![],
        };
        let batch = RecordBatch::from_records(vec![rec_a.clone(), rec_b.clone()]);
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &batch).unwrap();
        let encoded_len = buf::i32_from_usize(buf.len()).unwrap();
        assert_eq!(batch.size_in_bytes().unwrap(), encoded_len);
        assert_eq!(
            RecordBatch::size_in_bytes_of(&batch.records).unwrap(),
            encoded_len
        );

        assert_eq!(RecordBatch::size_in_bytes_of(&[]).unwrap(), 0);
        let empty_batch = RecordBatch::from_records(vec![]);
        assert_eq!(
            empty_batch.size_in_bytes().unwrap(),
            RecordBatch::RECORD_BATCH_OVERHEAD
        );

        let aligned_a = Record {
            offset: 10,
            timestamp: 5,
            key: None,
            value: Some(Bytes::from_static(b"a")),
            headers: vec![],
        };
        let aligned_b = Record {
            offset: 11,
            timestamp: 7,
            key: None,
            value: Some(Bytes::from_static(b"a")),
            headers: vec![],
        };
        assert_eq!(
            RecordBatch::size_in_bytes_from(10, &[aligned_a.clone(), aligned_b.clone()]).unwrap(),
            RecordBatch::size_in_bytes_of(&[aligned_a.clone(), aligned_b]).unwrap()
        );
        let far_b = Record {
            offset: 74,
            timestamp: 5,
            key: None,
            value: Some(Bytes::from_static(b"a")),
            headers: vec![],
        };
        assert_ne!(
            RecordBatch::size_in_bytes_from(10, &[aligned_a.clone(), far_b.clone()]).unwrap(),
            RecordBatch::size_in_bytes_of(&[aligned_a, far_b]).unwrap()
        );

        let crc = batch.checksum().unwrap();
        let off = buf::usize_from_i32(RecordBatch::CRC_OFFSET).unwrap();
        let crc_bytes = buf.get(off..off + 4).unwrap();
        assert_eq!(crc, u32::from_be_bytes(crc_bytes.try_into().unwrap()));
        assert_eq!(
            batch.to_string(),
            format!(
                "RecordBatch(magic=2, offsets=[0, 1], sequence=[-1, -1], isTransactional=false, isControlBatch=false, compression=none, timestampType=CreateTime, crc={crc})"
            )
        );

        let gz = batch.clone().with_compression(Compression::Gzip);
        let mut gz_buf = BytesMut::new();
        encode_record_batch(&mut gz_buf, &gz).unwrap();
        assert_eq!(
            gz.size_in_bytes().unwrap(),
            buf::i32_from_usize(gz_buf.len()).unwrap()
        );
        let gz_crc = gz.checksum().unwrap();
        assert_eq!(
            gz.to_string(),
            format!(
                "RecordBatch(magic=2, offsets=[0, 1], sequence=[-1, -1], isTransactional=false, isControlBatch=false, compression=gzip, timestampType=CreateTime, crc={gz_crc})"
            )
        );
        let lat = batch.with_timestamp_type(TimestampType::LogAppendTime);
        assert!(lat.to_string().contains("timestampType=LogAppendTime"));
    }

    #[test]
    fn record_size_upper_bound_matches_java() {
        let rec = Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from_static(b"abcd")),
            headers: vec![],
        };
        let size_of = buf::i32_from_usize(
            size_of_key_value_headers(rec.key(), rec.value(), rec.headers()).unwrap(),
        )
        .unwrap();
        let upper = rec.record_size_upper_bound().unwrap();
        assert_eq!(upper, Record::MAX_RECORD_OVERHEAD + size_of);
        assert!(upper >= rec.size_in_bytes(0, 0).unwrap());
        let batch_upper =
            RecordBatch::estimate_batch_size_upper_bound(rec.key(), rec.value(), rec.headers())
                .unwrap();
        assert_eq!(batch_upper, RecordBatch::RECORD_BATCH_OVERHEAD + upper);
        assert_eq!(
            Records::estimate_size_in_bytes_upper_bound(rec.key(), rec.value(), rec.headers())
                .unwrap(),
            batch_upper
        );
        assert_eq!(batch_upper, 89);
    }

    #[test]
    fn estimate_size_in_bytes_matches_java() {
        assert_eq!(
            Records::record_batch_header_size_in_bytes(),
            RecordBatch::RECORD_BATCH_OVERHEAD
        );
        assert_eq!(
            Records::estimate_size_in_bytes(Compression::None, &[]).unwrap(),
            0
        );
        assert_eq!(
            Records::estimate_size_in_bytes(Compression::Gzip, &[]).unwrap(),
            1024
        );
        let rec = Record {
            offset: 10,
            timestamp: 5,
            key: None,
            value: Some(Bytes::from_static(b"a")),
            headers: vec![],
        };
        let uncompressed = RecordBatch::size_in_bytes_of(std::slice::from_ref(&rec)).unwrap();
        assert_eq!(
            Records::estimate_size_in_bytes(Compression::None, std::slice::from_ref(&rec)).unwrap(),
            uncompressed
        );
        assert_eq!(
            Records::estimate_size_in_bytes_from(10, Compression::None, std::slice::from_ref(&rec))
                .unwrap(),
            uncompressed
        );
        let gzip =
            Records::estimate_size_in_bytes(Compression::Gzip, std::slice::from_ref(&rec)).unwrap();
        assert_eq!(gzip, (uncompressed / 2).clamp(1024, 65_536));
        let huge = vec![Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from(vec![0u8; 200_000])),
            headers: vec![],
        }];
        assert_eq!(
            Records::estimate_size_in_bytes(Compression::Snappy, &huge).unwrap(),
            65_536
        );
    }

    #[test]
    fn has_matching_magic_matches_java_abstract_records() {
        assert!(Records::has_matching_magic(
            &[],
            RecordBatch::CURRENT_MAGIC_VALUE
        ));
        assert!(Records::has_matching_magic(&[], 0));
        assert!(Records::first_batch(&[]).is_none());
        assert!(Records::last_batch(&[]).is_none());

        let first = RecordBatch::from_records(vec![Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"a")),
            headers: vec![],
        }]);
        let mut second = RecordBatch::from_records(vec![Record {
            offset: 0,
            timestamp: 2,
            key: None,
            value: Some(Bytes::from_static(b"b")),
            headers: vec![],
        }]);
        second.base_offset = 10;
        let batches = [first, second];
        assert!(Records::has_matching_magic(
            &batches,
            RecordBatch::CURRENT_MAGIC_VALUE
        ));
        assert!(!Records::has_matching_magic(&batches, 0));
        assert_eq!(
            Records::first_batch(&batches).map(RecordBatch::base_offset),
            Some(0)
        );
        assert_eq!(
            Records::last_batch(&batches).map(RecordBatch::base_offset),
            Some(10)
        );
        assert_eq!(
            Records::first_batch(std::slice::from_ref(&batches[0])).map(RecordBatch::base_offset),
            Some(0)
        );
        assert_eq!(
            Records::last_batch(std::slice::from_ref(&batches[0])).map(RecordBatch::base_offset),
            Some(0)
        );
    }

    #[test]
    fn first_batch_size_matches_java_memory_records() {
        assert_eq!(Records::first_batch_size(&[]).unwrap(), None);
        assert_eq!(Records::first_batch_size(&[0; 16]).unwrap(), None);
        let mut almost = [0u8; 16];
        almost[8..12].copy_from_slice(&1i32.to_be_bytes());
        assert_eq!(Records::first_batch_size(&almost).unwrap(), None);

        let mut buf = [0u8; 17];
        buf[8..12].copy_from_slice(&14i32.to_be_bytes());
        buf[16] = 2;
        assert_eq!(Records::first_batch_size(&buf).unwrap(), Some(26));
        buf[16] = 0;
        assert_eq!(Records::first_batch_size(&buf).unwrap(), Some(26));

        let mut small = [0u8; 17];
        small[8..12].copy_from_slice(&13i32.to_be_bytes());
        small[16] = 2;
        let err = Records::first_batch_size(&small).unwrap_err().to_string();
        assert!(
            err.contains("Record size 13 is less than the minimum record overhead (14)"),
            "{err}"
        );

        let mut neg = [0u8; 17];
        neg[8..12].copy_from_slice(&(-1i32).to_be_bytes());
        let err = Records::first_batch_size(&neg).unwrap_err().to_string();
        assert!(
            err.contains("Record size -1 is less than the minimum record overhead (14)"),
            "{err}"
        );

        let mut min = [0u8; 17];
        min[8..12].copy_from_slice(&i32::MIN.to_be_bytes());
        let err = Records::first_batch_size(&min).unwrap_err().to_string();
        assert!(
            err.contains("Record size -2147483648 is less than the minimum record overhead (14)"),
            "{err}"
        );

        let mut bad_magic = [0u8; 17];
        bad_magic[8..12].copy_from_slice(&14i32.to_be_bytes());
        bad_magic[16] = 3;
        let err = Records::first_batch_size(&bad_magic)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Invalid magic found in record: 3"), "{err}");

        let mut signed_magic = [0u8; 17];
        signed_magic[8..12].copy_from_slice(&14i32.to_be_bytes());
        signed_magic[16] = 0xff;
        let err = Records::first_batch_size(&signed_magic)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Invalid magic found in record: -1"), "{err}");

        let batch = RecordBatch::from_records(vec![Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"a")),
            headers: vec![],
        }]);
        let mut encoded = BytesMut::new();
        encode_record_batch(&mut encoded, &batch).unwrap();
        let encoded_len = i32::try_from(encoded.len()).unwrap();
        assert_eq!(
            Records::first_batch_size(&encoded).unwrap(),
            Some(encoded_len)
        );
        assert_eq!(encoded_len, batch.size_in_bytes().unwrap());
        assert_eq!(Records::valid_bytes(&encoded).unwrap(), encoded_len);
    }

    #[test]
    fn valid_bytes_matches_java_memory_records() {
        assert_eq!(Records::valid_bytes(&[]).unwrap(), 0);
        assert_eq!(Records::valid_bytes(&[0; 11]).unwrap(), 0);

        let err = Records::valid_bytes(&[0; 16]).unwrap_err().to_string();
        assert!(
            err.contains("Record size 0 is less than the minimum record overhead (14)"),
            "{err}"
        );

        let mut almost = [0u8; 16];
        almost[8..12].copy_from_slice(&1i32.to_be_bytes());
        let err = Records::valid_bytes(&almost).unwrap_err().to_string();
        assert!(
            err.contains("Record size 1 is less than the minimum record overhead (14)"),
            "{err}"
        );
        assert!(Records::first_batch_size(&almost).unwrap().is_none());

        let batch = RecordBatch::from_records(vec![Record {
            offset: 0,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"a")),
            headers: vec![],
        }]);
        let mut encoded = BytesMut::new();
        encode_record_batch(&mut encoded, &batch).unwrap();
        let encoded_len = i32::try_from(encoded.len()).unwrap();

        let mut truncated = encoded.to_vec();
        truncated.extend_from_slice(&[0u8; 11]);
        assert_eq!(Records::valid_bytes(&truncated).unwrap(), encoded_len);

        let mut two = encoded.clone();
        two.extend_from_slice(&encoded);
        assert_eq!(
            Records::valid_bytes(&two).unwrap(),
            encoded_len.wrapping_mul(2)
        );

        let mut tail_corrupt = encoded.to_vec();
        let mut almost = [0u8; 16];
        almost[8..12].copy_from_slice(&1i32.to_be_bytes());
        tail_corrupt.extend_from_slice(&almost);
        let err = Records::valid_bytes(&tail_corrupt).unwrap_err().to_string();
        assert!(
            err.contains("Record size 1 is less than the minimum record overhead (14)"),
            "{err}"
        );

        let err = next_batch_size(&[0u8; 17], 10).unwrap_err().to_string();
        assert!(
            err.contains("Record size 0 is less than the minimum record overhead (14)"),
            "{err}"
        );
        let mut over = [0u8; 17];
        over[8..12].copy_from_slice(&20i32.to_be_bytes());
        over[16] = 2;
        let err = next_batch_size(&over, 19).unwrap_err().to_string();
        assert!(
            err.contains("Record size 20 exceeds the largest allowable message size (19)."),
            "{err}"
        );
        assert_eq!(next_batch_size(&over, 20).unwrap(), Some(32));
        let mut header_only = [0u8; 12];
        header_only[8..12].copy_from_slice(&14i32.to_be_bytes());
        assert_eq!(next_batch_size(&header_only, 20).unwrap(), None);
        let err = next_batch_size(&[0; 12], 20).unwrap_err().to_string();
        assert!(
            err.contains("Record size 0 is less than the minimum record overhead (14)"),
            "{err}"
        );
    }
}
