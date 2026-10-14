//! Allocation-free admission validation for ordinary, uncompressed magic-2 batches.
//!
//! Produce admission requires positive counts and contiguous offset deltas. The
//! separate [`validate_read`] path accepts ordinary sparse/empty compacted batches
//! and delete horizons while checking their full logical extent and actual count.
//! Both paths accept
//! nullable keys/values and repeated headers, and validates the entire input before
//! returning a borrowed token. Legacy magic, codecs, producer
//! sequencing, transactions/control records and broker-selected
//! log-append timestamps require separate implementations. Nothing is advertised.
//!
//! CRC32C protects bytes starting at batch attributes; the base offset, length,
//! leader epoch and magic are outside that checksum and are checked separately.
//! Caller-owned input remains borrowed. Validation allocates no buffers or indexes;
//! byte and count limits bound checksum/parsing work. Retained inputs and concurrent
//! calls must be bounded by the caller. Checksums do not authenticate hostile input.

use std::fmt;

const HEADER: usize = 61;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_ITEMS: usize = 1_000_000;

/// Positive byte/work budgets for one validation call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    input_bytes: usize,
    batch_bytes: usize,
    batches: usize,
    records: usize,
    record_bytes: usize,
    field_bytes: usize,
    headers_per_record: usize,
    headers: usize,
}
impl Limits {
    pub(crate) fn remaining_records(mut self, remaining: usize) -> Self {
        self.records = self.records.min(remaining);
        self
    }
    /// Validate byte limits (at most 64 MiB) and count limits (at most one million).
    ///
    /// `record_bytes` includes the encoded length prefix. `field_bytes` bounds
    /// each key, value, header key and header value individually. Header counts
    /// have both a per-record ceiling and an aggregate ceiling for the input.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent explicit admission budgets"
    )]
    pub fn new(
        input_bytes: usize,
        batch_bytes: usize,
        batches: usize,
        records: usize,
        record_bytes: usize,
        field_bytes: usize,
        headers_per_record: usize,
        headers: usize,
    ) -> Result<Self, Error> {
        if [input_bytes, batch_bytes, record_bytes, field_bytes]
            .iter()
            .any(|&n| !(1..=MAX_BYTES).contains(&n))
            || [batches, records, headers_per_record, headers]
                .iter()
                .any(|&n| !(1..=MAX_ITEMS).contains(&n))
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            input_bytes,
            batch_bytes,
            batches,
            records,
            record_bytes,
            field_bytes,
            headers_per_record,
            headers,
        })
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            input_bytes: 16 * 1024 * 1024,
            batch_bytes: 1024 * 1024,
            batches: 1024,
            records: 65536,
            record_bytes: 1024 * 1024,
            field_bytes: 1024 * 1024,
            headers_per_record: 1024,
            headers: 65536,
        }
    }
}

/// A configured work or byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Budget {
    /// Caller-owned bytes submitted to this call.
    InputBytes,
    /// Complete encoded batch bytes.
    BatchBytes,
    /// Number of batches in the input.
    Batches,
    /// Aggregate record count.
    Records,
    /// Complete encoded record bytes including length prefix.
    RecordBytes,
    /// Individual key/value/header key/header value bytes.
    FieldBytes,
    /// Number of headers in an individual record.
    HeadersPerRecord,
    /// Aggregate header count.
    Headers,
}
/// A feature requiring a separate implementation before admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsupported {
    /// Legacy magic 0 or 1.
    LegacyMagic(i8),
    /// Known Kafka codec ID: gzip=1, Snappy=2, LZ4=3, Zstandard=4.
    Compression(u8),
    /// Transactional batch semantics.
    Transactional,
    /// Control-record batch semantics.
    Control,
    /// Producer ID/epoch/sequence deduplication and fencing.
    Idempotent,
    /// Compaction delete-horizon semantics.
    DeleteHorizon,
    /// Broker-selected log-append timestamps.
    LogAppendTime,
}
/// A structural or arithmetic defect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalid {
    /// Empty input or a batch without records.
    Empty,
    /// Declared data extends beyond its enclosing input.
    Truncated,
    /// Negative or impossibly small length/count.
    Length,
    /// Unknown magic value.
    Magic,
    /// Protected bytes differ from the recorded CRC32C.
    Checksum,
    /// Unknown codec or reserved batch attribute bits.
    Attributes,
    /// Leader epoch or producer sentinel tuple is invalid.
    Metadata,
    /// Offset delta is negative, gapped, inconsistent or overflows signed offsets.
    Offset,
    /// Timestamp addition overflows or the maximum timestamp disagrees.
    Timestamp,
    /// Varint exceeds its signed 32/64-bit encoded width.
    VarintOverflow,
    /// Overlong varint with a redundant terminating zero group.
    NonCanonicalVarint,
    /// A record or the declared record sequence has unconsumed bytes.
    TrailingBytes,
    /// Record attribute bits are reserved.
    RecordAttributes,
    /// Header key is not valid UTF-8.
    HeaderKey,
}
/// Typed errors never retain input payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Limits are zero or exceed hard byte/count maxima.
    InvalidLimits,
    /// A configured budget would be exceeded, at the input byte offset.
    BudgetExceeded {
        /// Byte offset of the rejected structure.
        at: usize,
        /// Exceeded budget.
        budget: Budget,
    },
    /// Malformed input, at the input byte offset.
    Invalid {
        /// Byte offset of the rejected field/structure.
        at: usize,
        /// Structural defect.
        kind: Invalid,
    },
    /// Structurally recognized feature unsupported by this foundation.
    Unsupported {
        /// Byte offset of the feature.
        at: usize,
        /// Feature requiring separate handling.
        feature: Unsupported,
    },
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => f.write_str("invalid record validation limits"),
            Self::BudgetExceeded { at, budget } => {
                write!(f, "record budget {budget:?} exceeded at {at}")
            }
            Self::Invalid { at, kind } => write!(f, "invalid record structure {kind:?} at {at}"),
            Self::Unsupported { at, feature } => {
                write!(f, "unsupported record feature {feature:?} at {at}")
            }
        }
    }
}
impl std::error::Error for Error {}
fn invalid(at: usize, kind: Invalid) -> Error {
    Error::Invalid { at, kind }
}
fn budget(at: usize, what: Budget) -> Error {
    Error::BudgetExceeded { at, budget: what }
}
fn unsupported(at: usize, feature: Unsupported) -> Error {
    Error::Unsupported { at, feature }
}

/// Immutable borrowed proof that the entire submitted input passed validation.
#[derive(Debug, Clone, Copy)]
pub struct Validated<'a> {
    bytes: &'a [u8],
    batches: usize,
    records: usize,
    headers: usize,
}
impl<'a> Validated<'a> {
    /// Exact, completely consumed validated input; no rewriting has occurred.
    #[must_use]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
    /// Number of validated batches.
    #[must_use]
    pub fn batch_count(&self) -> usize {
        self.batches
    }
    /// Aggregate validated record count, independent of payload byte length.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records
    }
    /// Aggregate validated header count.
    #[must_use]
    pub fn header_count(&self) -> usize {
        self.headers
    }
    /// Borrowed batch projections without retaining an index.
    ///
    /// Iteration reads only fixed headers. Items are fallible to preserve checked
    /// slicing/arithmetic; immutable successfully validated input yields no errors.
    pub fn batches(&self) -> Batches<'a> {
        Batches {
            cursor: Cursor::new(self.bytes, 0),
            failed: false,
        }
    }
}
/// Borrowed metadata and exact bytes for one validated batch.
#[derive(Debug, Clone, Copy)]
pub struct Batch<'a> {
    /// Complete original batch bytes including its offset and length prefix.
    pub bytes: &'a [u8],
    /// Nonnegative signed Kafka base offset.
    pub base_offset: i64,
    /// Checked signed offset immediately following the last record.
    pub next_offset: i64,
    /// Actual record count; compacted batches can be empty.
    pub record_count: u32,
    /// CreateTime base or, when present, the delete horizon.
    pub base_timestamp: i64,
    /// Maximum CreateTime timestamp.
    pub max_timestamp: i64,
    /// Authenticated compaction horizon in the protected batch attributes/header.
    pub delete_horizon_ms: Option<i64>,
}
impl<'a> Batch<'a> {
    /// Constant-space fallible projections of the encoded records.
    ///
    /// This also checks slicing/arithmetic for externally constructed `Batch`
    /// values; it does not substitute for full CRC/admission validation.
    pub fn records(&self) -> Records<'a> {
        Records::new(self.bytes)
    }
}

/// Borrowed proof for complete ordinary stored bytes, including sparse batches.
#[derive(Debug, Clone, Copy)]
pub struct ReadSummary<'a> {
    validated: Validated<'a>,
    first: Option<i64>,
    next: Option<i64>,
}
impl<'a> ReadSummary<'a> {
    /// Exact fully consumed input; empty input describes an empty physical entry.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.validated.as_bytes()
    }
    /// Actual batch count, independent of the enclosing persisted logical span.
    pub fn batch_count(&self) -> usize {
        self.validated.batch_count()
    }
    /// Actual retained record count.
    pub fn record_count(&self) -> usize {
        self.validated.record_count()
    }
    /// Actual retained header count.
    pub fn header_count(&self) -> usize {
        self.validated.header_count()
    }
    /// Borrowed batch projections.
    pub fn batches(&self) -> Batches<'a> {
        self.validated.batches()
    }
    /// First physical batch base; absent for zero-payload entries.
    pub fn first_offset(&self) -> Option<i64> {
        self.first
    }
    /// Exclusive original extent of the final physical batch, not actual count.
    pub fn next_offset(&self) -> Option<i64> {
        self.next
    }
}

/// Borrowed ordinary record, retaining its original logical offset and fields.
#[derive(Debug, Clone, Copy)]
pub struct Record<'a> {
    /// Checked absolute logical offset.
    pub offset: i64,
    /// Checked absolute CreateTime timestamp.
    pub timestamp: i64,
    /// Nullable key; an empty non-null key remains distinct from null.
    pub key: Option<&'a [u8]>,
    /// Nullable value; null is a tombstone only with a non-null key.
    pub value: Option<&'a [u8]>,
    /// Complete original record including its encoded length prefix.
    pub encoded: &'a [u8],
    pub(crate) suffix: &'a [u8],
}
/// Constant-space fallible record iterator.
#[derive(Debug)]
pub struct Records<'a> {
    cursor: Cursor<'a>,
    base: i64,
    base_time: i64,
    last: i32,
    left: usize,
    previous: Option<i32>,
    error: Option<Error>,
    failed: bool,
}
impl<'a> Records<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        let mut input = Cursor::new(bytes, 0);
        let parsed = projection(&mut input).and_then(|b| {
            if !input.remaining().is_empty() {
                return Err(invalid(input.at(), Invalid::TrailingBytes));
            }
            let last = i32::try_from(b.next_offset - b.base_offset - 1)
                .map_err(|_| invalid(23, Invalid::Offset))?;
            Ok((
                b.base_offset,
                b.base_timestamp,
                last,
                b.record_count as usize,
            ))
        });
        let (base, base_time, last, left, error) = match parsed {
            Ok((a, b, c, d)) => (a, b, c, d, None),
            Err(e) => (0, 0, 0, 0, Some(e)),
        };
        Self {
            cursor: Cursor::new(bytes.get(HEADER..).unwrap_or_default(), HEADER),
            base,
            base_time,
            last,
            left,
            previous: None,
            error,
            failed: false,
        }
    }
}
impl<'a> Iterator for Records<'a> {
    type Item = Result<Record<'a>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Some(error) = self.error.take() {
            self.failed = true;
            return Some(Err(error));
        }
        if self.left == 0 {
            if self.cursor.remaining().is_empty() {
                return None;
            }
            self.failed = true;
            return Some(Err(invalid(self.cursor.at(), Invalid::TrailingBytes)));
        }
        let result = (|| {
            #[cfg(test)]
            RECORD_VISITS.with(|n| n.set(n.get() + 1));
            let start = self.cursor.position;
            let at = self.cursor.at();
            let length = self.cursor.varint()?;
            if length < 6 {
                return Err(invalid(at, Invalid::Length));
            }
            let body = self.cursor.take(length as usize)?;
            let mut r = Cursor::new(body, self.cursor.at() - body.len());
            if r.byte()? != 0 {
                return Err(invalid(at, Invalid::RecordAttributes));
            }
            let timestamp = self
                .base_time
                .checked_add(r.varlong()?)
                .ok_or_else(|| invalid(r.at(), Invalid::Timestamp))?;
            let suffix = r.remaining();
            let delta = r.varint()?;
            if delta < 0 || delta > self.last || self.previous.is_some_and(|n| delta <= n) {
                return Err(invalid(r.at(), Invalid::Offset));
            }
            let offset = self
                .base
                .checked_add(i64::from(delta))
                .ok_or_else(|| invalid(r.at(), Invalid::Offset))?;
            let key = nullable(&mut r, MAX_BYTES)?;
            let value = nullable(&mut r, MAX_BYTES)?;
            let headers = r.varint()?;
            let headers = usize::try_from(headers).map_err(|_| invalid(r.at(), Invalid::Length))?;
            if headers > MAX_ITEMS || headers > r.remaining().len() / 2 {
                return Err(invalid(r.at(), Invalid::Length));
            }
            for _ in 0..headers {
                let n = r.varint()?;
                let n = usize::try_from(n).map_err(|_| invalid(r.at(), Invalid::Length))?;
                if std::str::from_utf8(r.take(n)?).is_err() {
                    return Err(invalid(r.at(), Invalid::HeaderKey));
                }
                nullable(&mut r, MAX_BYTES)?;
            }
            if !r.remaining().is_empty() {
                return Err(invalid(r.at(), Invalid::TrailingBytes));
            }
            self.previous = Some(delta);
            self.left -= 1;
            Ok(Record {
                offset,
                timestamp,
                key,
                value,
                suffix,
                encoded: &self.cursor.bytes[start..self.cursor.position],
            })
        })();
        if result.is_err() {
            self.failed = true;
        }
        Some(result)
    }
}
/// Constant-space iterator over previously validated batches.
#[derive(Debug)]
pub struct Batches<'a> {
    cursor: Cursor<'a>,
    failed: bool,
}
impl<'a> Iterator for Batches<'a> {
    type Item = Result<Batch<'a>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.cursor.remaining().is_empty() {
            return None;
        }
        let result = projection(&mut self.cursor);
        if result.is_err() {
            self.failed = true;
        }
        Some(result)
    }
}

/// Validate all ordinary uncompressed magic-2 record batches without allocation.
///
/// No append, filesystem operation or broker API advertisement occurs. This
/// admission subset deliberately rejects compacted/gapped/empty batches and
/// noncanonical varints even if an upstream low-level parser accepts them.
/// Batch base offsets are checked independently: producer batches commonly each
/// start at zero, so cross-batch continuity is not imposed. A future handler must
/// assign offsets and enforce topic, authorization, timestamp and Produce policy.
pub fn validate(bytes: &[u8], limits: Limits) -> Result<Validated<'_>, Error> {
    if bytes.len() > limits.input_bytes {
        return Err(budget(0, Budget::InputBytes));
    }
    if bytes.is_empty() {
        return Err(invalid(0, Invalid::Empty));
    }
    let mut input = Cursor::new(bytes, 0);
    let mut batches = 0usize;
    let mut records = 0usize;
    let mut headers = 0usize;
    while !input.remaining().is_empty() {
        let at = input.at();
        if batches == limits.batches {
            return Err(budget(at, Budget::Batches));
        }
        let batch = take_batch(&mut input, limits.batch_bytes)?;
        validate_batch(batch, at, limits, &mut records, &mut headers, false, true)?;
        batches += 1;
    }
    Ok(Validated {
        bytes,
        batches,
        records,
        headers,
    })
}

/// Like [`validate`], for a single batch whose CRC32-C the caller already matched.
///
/// The structural checks are unchanged. The checksum is not computed again.
/// Codec builds validate inside `codecs::normalize` instead, so this entry
/// point exists only for the no-codec produce path.
#[cfg(not(feature = "codecs"))]
pub(crate) fn validate_known_crc(bytes: &[u8], limits: Limits) -> Result<Validated<'_>, Error> {
    if bytes.len() > limits.input_bytes {
        return Err(budget(0, Budget::InputBytes));
    }
    if bytes.is_empty() {
        return Err(invalid(0, Invalid::Empty));
    }
    let mut input = Cursor::new(bytes, 0);
    let mut batches = 0usize;
    let mut records = 0usize;
    let mut headers = 0usize;
    while !input.remaining().is_empty() {
        let at = input.at();
        if batches == limits.batches {
            return Err(budget(at, Budget::Batches));
        }
        let batch = take_batch(&mut input, limits.batch_bytes)?;
        validate_batch(batch, at, limits, &mut records, &mut headers, false, false)?;
        batches += 1;
    }
    Ok(Validated {
        bytes,
        batches,
        records,
        headers,
    })
}
/// Validate complete ordinary stored bytes without allocating an index.
///
/// Sparse deltas are strictly increasing and within each original batch extent.
/// Empty 61-byte batches retain that extent, and empty whole payloads have zero
/// physical batches. Signed offset overflow, extra bytes and protected features
/// fail closed even where an Apache component parser is more permissive.
pub fn validate_read(bytes: &[u8], limits: Limits) -> Result<ReadSummary<'_>, Error> {
    if bytes.len() > limits.input_bytes {
        return Err(budget(0, Budget::InputBytes));
    }
    let mut input = Cursor::new(bytes, 0);
    let (mut batches, mut records, mut headers) = (0usize, 0usize, 0usize);
    let (mut first, mut next) = (None, None);
    while !input.remaining().is_empty() {
        let at = input.at();
        if batches == limits.batches {
            return Err(budget(at, Budget::Batches));
        }
        let bytes = take_batch(&mut input, limits.batch_bytes)?;
        validate_batch(bytes, at, limits, &mut records, &mut headers, true, true)?;
        let mut one = Cursor::new(bytes, at);
        let b = projection(&mut one)?;
        if next.is_some_and(|end| b.base_offset < end) {
            return Err(invalid(at, Invalid::Offset));
        }
        if first.is_none() {
            first = Some(b.base_offset);
        }
        next = Some(b.next_offset);
        batches += 1;
    }
    Ok(ReadSummary {
        validated: Validated {
            bytes,
            batches,
            records,
            headers,
        },
        first,
        next,
    })
}
fn take_batch<'a>(input: &mut Cursor<'a>, limit: usize) -> Result<&'a [u8], Error> {
    let at = input.at();
    let mut peek = *input;
    peek.take(8)?;
    let length = peek.i32()?;
    if length < 5 {
        return Err(invalid(at + 8, Invalid::Length));
    }
    let size = usize::try_from(length)
        .map_err(|_| invalid(at + 8, Invalid::Length))?
        .checked_add(12)
        .ok_or_else(|| invalid(at + 8, Invalid::Length))?;
    if size > limit {
        return Err(budget(at + 8, Budget::BatchBytes));
    }
    input.take(size)
}
fn validate_batch(
    bytes: &[u8],
    at: usize,
    limits: Limits,
    total_records: &mut usize,
    total_headers: &mut usize,
    read: bool,
    check_crc: bool,
) -> Result<(), Error> {
    let mut c = Cursor::new(bytes, at);
    let base = c.i64()?;
    c.i32()?;
    let epoch = c.i32()?;
    let magic = c.byte()? as i8;
    if magic == 0 || magic == 1 {
        return Err(unsupported(at + 16, Unsupported::LegacyMagic(magic)));
    }
    if magic != 2 {
        return Err(invalid(at + 16, Invalid::Magic));
    }
    if bytes.len() < HEADER {
        return Err(invalid(at, Invalid::Truncated));
    }
    let crc = c.u32()?;
    if check_crc && crc32c::crc32c(c.remaining()) != crc {
        return Err(invalid(at + 17, Invalid::Checksum));
    }
    let attrs = c.u16()?;
    if attrs & !0x7f != 0 || attrs & 7 > 4 {
        return Err(invalid(at + 21, Invalid::Attributes));
    }
    if attrs & 7 != 0 {
        return Err(unsupported(
            at + 21,
            Unsupported::Compression((attrs & 7) as u8),
        ));
    }
    if attrs & 0x20 != 0 {
        return Err(unsupported(at + 21, Unsupported::Control));
    }
    if attrs & 0x10 != 0 {
        return Err(unsupported(at + 21, Unsupported::Transactional));
    }
    if attrs & 0x40 != 0 && !read {
        return Err(unsupported(at + 21, Unsupported::DeleteHorizon));
    }
    if attrs & 8 != 0 {
        return Err(unsupported(at + 21, Unsupported::LogAppendTime));
    }
    let last = c.i32()?;
    let base_time = c.i64()?;
    let max_time = c.i64()?;
    let producer = c.i64()?;
    let producer_epoch = c.i16()?;
    let sequence = c.i32()?;
    let count = c.i32()?;
    if epoch < -1
        || producer < -1
        || producer_epoch < -1
        || sequence < -1
        || (producer == -1 && (producer_epoch != -1 || sequence != -1))
        || (producer >= 0 && (producer_epoch < 0 || sequence < 0))
    {
        return Err(invalid(at + 12, Invalid::Metadata));
    }
    if producer >= 0 {
        return Err(unsupported(at + 43, Unsupported::Idempotent));
    }
    if count < 0 || (count == 0 && !read) {
        return Err(invalid(at + 57, Invalid::Empty));
    }
    let count = usize::try_from(count).map_err(|_| invalid(at + 57, Invalid::Length))?;
    if count > limits.records - *total_records {
        return Err(budget(at + 57, Budget::Records));
    }
    if base < 0
        || last < 0
        || (!read && usize::try_from(last).ok() != count.checked_sub(1))
        || base
            .checked_add(i64::from(last))
            .and_then(|n| n.checked_add(1))
            .is_none()
    {
        return Err(invalid(at, Invalid::Offset));
    }
    if read && attrs & 0x40 != 0 && base_time < 0 {
        return Err(invalid(at + 27, Invalid::Timestamp));
    }
    if read && count == 0 {
        if attrs & 0x40 != 0 {
            return Err(invalid(at + 21, Invalid::Attributes));
        }
        if base_time != -1 {
            return Err(invalid(at + 27, Invalid::Timestamp));
        }
    }
    if count > c.remaining().len() / 7 {
        return Err(invalid(at + 57, Invalid::Length));
    }
    if plain_enabled()
        && try_accept_plain(
            c.remaining(),
            count,
            last,
            base_time,
            max_time,
            read,
            limits,
        )
    {
        #[cfg(test)]
        RECORD_VISITS.with(|n| n.set(n.get() + count));
        *total_records += count;
        return Ok(());
    }
    let mut observed_max = i64::MIN;
    let mut previous = None;
    for delta in 0..count {
        #[cfg(test)]
        RECORD_VISITS.with(|n| n.set(n.get() + 1));
        let record_at = c.at();
        let prefix_start = c.position;
        let length = c.varint()?;
        if length < 6 {
            return Err(invalid(record_at, Invalid::Length));
        }
        let length = usize::try_from(length).map_err(|_| invalid(record_at, Invalid::Length))?;
        let encoded = length
            .checked_add(c.position - prefix_start)
            .ok_or_else(|| invalid(record_at, Invalid::Length))?;
        if encoded > limits.record_bytes {
            return Err(budget(record_at, Budget::RecordBytes));
        }
        let mut r = Cursor::new(c.take(length)?, c.origin + c.position - length);
        if r.byte()? != 0 {
            return Err(invalid(record_at, Invalid::RecordAttributes));
        }
        let timestamp = base_time
            .checked_add(r.varlong()?)
            .ok_or_else(|| invalid(r.at(), Invalid::Timestamp))?;
        observed_max = observed_max.max(timestamp);
        let offset = r.varint()?;
        if (!read && usize::try_from(offset).ok() != Some(delta))
            || (read && (offset < 0 || offset > last || previous.is_some_and(|n| offset <= n)))
        {
            return Err(invalid(r.at(), Invalid::Offset));
        }
        previous = Some(offset);
        nullable(&mut r, limits.field_bytes)?;
        nullable(&mut r, limits.field_bytes)?;
        let header_at = r.at();
        let headers = r.varint()?;
        let headers = usize::try_from(headers).map_err(|_| invalid(header_at, Invalid::Length))?;
        if headers > limits.headers_per_record {
            return Err(budget(header_at, Budget::HeadersPerRecord));
        }
        if headers > limits.headers - *total_headers {
            return Err(budget(header_at, Budget::Headers));
        }
        if headers > r.remaining().len() / 2 {
            return Err(invalid(header_at, Invalid::Length));
        }
        *total_headers += headers;
        for _ in 0..headers {
            let key_at = r.at();
            let size = r.varint()?;
            let size = usize::try_from(size).map_err(|_| invalid(key_at, Invalid::Length))?;
            if size > limits.field_bytes {
                return Err(budget(key_at, Budget::FieldBytes));
            }
            if std::str::from_utf8(r.take(size)?).is_err() {
                return Err(invalid(key_at, Invalid::HeaderKey));
            }
            nullable(&mut r, limits.field_bytes)?;
        }
        if !r.remaining().is_empty() {
            return Err(invalid(r.at(), Invalid::TrailingBytes));
        }
    }
    if !c.remaining().is_empty() {
        return Err(invalid(c.at(), Invalid::TrailingBytes));
    }
    if count > 0 && observed_max != max_time {
        return Err(invalid(at + 35, Invalid::Timestamp));
    }
    *total_records += count;
    Ok(())
}
#[cfg(test)]
thread_local! {
    static RECORD_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SKIP_PLAIN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
fn plain_enabled() -> bool {
    #[cfg(test)]
    {
        !SKIP_PLAIN.with(std::cell::Cell::get)
    }
    #[cfg(not(test))]
    {
        true
    }
}
#[cfg(test)]
pub(crate) fn reset_record_visits() {
    RECORD_VISITS.with(|n| n.set(0));
}
#[cfg(test)]
pub(crate) fn record_visits() -> usize {
    RECORD_VISITS.with(std::cell::Cell::get)
}
/// Accept a no-header batch encoded with canonical varints.
///
/// Returns false without deciding an error so the generic walk keeps the
/// existing rejection positions.
fn try_accept_plain(
    bytes: &[u8],
    count: usize,
    last: i32,
    base_time: i64,
    max_time: i64,
    read: bool,
    limits: Limits,
) -> bool {
    let mut index = 0usize;
    let mut observed_max = i64::MIN;
    let mut previous: Option<i32> = None;
    for delta in 0..count {
        let Some((body_len, next)) = read_canonical_i32(bytes, index) else {
            return false;
        };
        if body_len < 6 {
            return false;
        }
        let Ok(body_len) = usize::try_from(body_len) else {
            return false;
        };
        let encoded = match next.checked_add(body_len) {
            Some(end) => end.saturating_sub(index),
            None => return false,
        };
        if encoded > limits.record_bytes {
            return false;
        }
        let Some(body_end) = next.checked_add(body_len) else {
            return false;
        };
        if bytes.len() < body_end || bytes.get(next) != Some(&0) {
            return false;
        }
        let mut cursor = next.saturating_add(1);
        let Some((timestamp_delta, next)) = read_canonical_i64(bytes, cursor) else {
            return false;
        };
        cursor = next;
        let Some(timestamp) = base_time.checked_add(timestamp_delta) else {
            return false;
        };
        observed_max = observed_max.max(timestamp);
        let Some((offset, next)) = read_canonical_i32(bytes, cursor) else {
            return false;
        };
        cursor = next;
        let offset_ok = if read {
            offset >= 0 && offset <= last && previous.is_none_or(|n| offset > n)
        } else {
            i32::try_from(delta).ok() == Some(offset)
        };
        if !offset_ok {
            return false;
        }
        previous = Some(offset);
        let Some((key_len, next)) = read_canonical_i32(bytes, cursor) else {
            return false;
        };
        cursor = next;
        if !skip_plain_field(bytes, &mut cursor, body_end, key_len, limits.field_bytes) {
            return false;
        }
        let Some((value_len, next)) = read_canonical_i32(bytes, cursor) else {
            return false;
        };
        cursor = next;
        if !skip_plain_field(bytes, &mut cursor, body_end, value_len, limits.field_bytes) {
            return false;
        }
        let Some((headers, next)) = read_canonical_i32(bytes, cursor) else {
            return false;
        };
        if headers != 0 || next != body_end {
            return false;
        }
        index = body_end;
    }
    index == bytes.len() && (count == 0 || observed_max == max_time)
}

fn skip_plain_field(
    bytes: &[u8],
    cursor: &mut usize,
    body_end: usize,
    len: i32,
    field_bytes: usize,
) -> bool {
    if len == -1 {
        return true;
    }
    if len < 0 {
        return false;
    }
    let Ok(len) = usize::try_from(len) else {
        return false;
    };
    if len > field_bytes {
        return false;
    }
    let Some(end) = cursor.checked_add(len) else {
        return false;
    };
    if end > body_end || bytes.len() < end {
        return false;
    }
    *cursor = end;
    true
}

fn read_canonical_i32(bytes: &[u8], index: usize) -> Option<(i32, usize)> {
    let (raw, next) = read_canonical_u64(bytes, index, 5, 15)?;
    let raw = u32::try_from(raw).ok()?;
    Some((((raw >> 1) as i32) ^ -((raw & 1) as i32), next))
}

fn read_canonical_i64(bytes: &[u8], index: usize) -> Option<(i64, usize)> {
    let (raw, next) = read_canonical_u64(bytes, index, 10, 1)?;
    Some((((raw >> 1) as i64) ^ -((raw & 1) as i64), next))
}

fn read_canonical_u64(bytes: &[u8], index: usize, width: usize, last: u8) -> Option<(u64, usize)> {
    let first = *bytes.get(index)?;
    if first & 0x80 == 0 {
        return Some((u64::from(first), index.saturating_add(1)));
    }
    let second = *bytes.get(index.saturating_add(1))?;
    if second & 0x80 == 0 {
        if second == 0 {
            return None;
        }
        return Some((
            u64::from(first & 0x7f) | (u64::from(second) << 7),
            index.saturating_add(2),
        ));
    }
    if width < 3 {
        return None;
    }
    let mut value = u64::from(first & 0x7f) | (u64::from(second & 0x7f) << 7);
    let mut pos = index.saturating_add(2);
    for group in 2..width {
        let byte = *bytes.get(pos)?;
        pos = pos.checked_add(1)?;
        let shift = group.saturating_mul(7);
        if shift >= 64 {
            return None;
        }
        let shift = u32::try_from(shift).ok()?;
        if group + 1 == width && byte > last {
            return None;
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            if byte == 0 {
                return None;
            }
            return Some((value, pos));
        }
    }
    None
}

fn nullable<'a>(c: &mut Cursor<'a>, limit: usize) -> Result<Option<&'a [u8]>, Error> {
    let at = c.at();
    let size = c.varint()?;
    if size == -1 {
        return Ok(None);
    }
    let size = usize::try_from(size).map_err(|_| invalid(at, Invalid::Length))?;
    if size > limit {
        return Err(budget(at, Budget::FieldBytes));
    }
    Ok(Some(c.take(size)?))
}
fn projection<'a>(input: &mut Cursor<'a>) -> Result<Batch<'a>, Error> {
    let at = input.at();
    let bytes = take_batch(input, MAX_BYTES)?;
    let mut c = Cursor::new(bytes, at);
    let base_offset = c.i64()?;
    c.take(13)?;
    let attrs = c.u16()?;
    let last = c.i32()?;
    let base_timestamp = c.i64()?;
    let max_timestamp = c.i64()?;
    c.take(14)?;
    let record_count = c.u32()?;
    let next_offset = base_offset
        .checked_add(i64::from(last))
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| invalid(at, Invalid::Offset))?;
    if base_offset < 0 || last < 0 || record_count > i32::MAX as u32 {
        return Err(invalid(at, Invalid::Offset));
    }
    Ok(Batch {
        bytes,
        base_offset,
        next_offset,
        record_count,
        base_timestamp,
        max_timestamp,
        delete_horizon_ms: (attrs & 0x40 != 0).then_some(base_timestamp),
    })
}
#[derive(Debug, Clone, Copy)]
struct Cursor<'a> {
    bytes: &'a [u8],
    origin: usize,
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8], origin: usize) -> Self {
        Self {
            bytes,
            origin,
            position: 0,
        }
    }
    fn at(&self) -> usize {
        self.origin + self.position
    }
    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.position..]
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self
            .position
            .checked_add(n)
            .ok_or_else(|| invalid(self.at(), Invalid::Length))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid(self.at(), Invalid::Truncated))?;
        self.position = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| invalid(self.at(), Invalid::Truncated))?,
        ))
    }
    fn i16(&mut self) -> Result<i16, Error> {
        Ok(self.u16()? as i16)
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| invalid(self.at(), Invalid::Truncated))?,
        ))
    }
    fn i32(&mut self) -> Result<i32, Error> {
        Ok(self.u32()? as i32)
    }
    fn i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| invalid(self.at(), Invalid::Truncated))?,
        ))
    }
    fn varint(&mut self) -> Result<i32, Error> {
        let u = self.var(5, 15)? as u32;
        Ok(((u >> 1) as i32) ^ -((u & 1) as i32))
    }
    fn varlong(&mut self) -> Result<i64, Error> {
        let u = self.var(10, 1)?;
        Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
    }
    fn var(&mut self, width: usize, last: u8) -> Result<u64, Error> {
        let at = self.at();
        let mut value = 0u64;
        for group in 0..width {
            let pos = self.position;
            let Some(byte) = self.bytes.get(pos).copied() else {
                return Err(invalid(self.at(), Invalid::Truncated));
            };
            self.position = pos
                .checked_add(1)
                .ok_or_else(|| invalid(self.at(), Invalid::Length))?;
            if group + 1 == width && byte > last {
                return Err(invalid(at, Invalid::VarintOverflow));
            }
            let shift = group.saturating_mul(7);
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                if group > 0 && byte == 0 {
                    return Err(invalid(at, Invalid::NonCanonicalVarint));
                }
                return Ok(value);
            }
        }
        Err(invalid(at, Invalid::VarintOverflow))
    }
}

#[cfg(test)]
mod tests {
    use super::{validate, validate_batch, validate_read, Error, Invalid, Limits, SKIP_PLAIN};

    fn put_i32(out: &mut Vec<u8>, n: i32) {
        let mut u = ((n as u32) << 1) ^ ((n >> 31) as u32);
        loop {
            let mut byte = (u & 0x7f) as u8;
            u >>= 7;
            if u == 0 {
                out.push(byte);
                return;
            }
            byte |= 0x80;
            out.push(byte);
        }
    }

    fn put_i64(out: &mut Vec<u8>, n: i64) {
        let mut u = ((n as u64) << 1) ^ ((n >> 63) as u64);
        loop {
            let mut byte = (u & 0x7f) as u8;
            u >>= 7;
            if u == 0 {
                out.push(byte);
                return;
            }
            byte |= 0x80;
            out.push(byte);
        }
    }

    fn put_field(out: &mut Vec<u8>, field: Option<&[u8]>) {
        match field {
            None => put_i32(out, -1),
            Some(bytes) => {
                put_i32(out, i32::try_from(bytes.len()).unwrap());
                out.extend_from_slice(bytes);
            }
        }
    }

    struct Sample {
        timestamp_delta: i64,
        offset: i32,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        attributes: u8,
    }

    fn sample(offset: i32, value: &[u8]) -> Sample {
        Sample {
            timestamp_delta: i64::from(offset),
            offset,
            key: None,
            value: Some(value.to_vec()),
            headers: Vec::new(),
            attributes: 0,
        }
    }

    fn encode(records: &[Sample], base_time: i64, max_time: i64, last: i32) -> Vec<u8> {
        let mut payload = Vec::new();
        for record in records {
            let start = payload.len();
            put_i32(&mut payload, 0);
            let body = payload.len();
            payload.push(record.attributes);
            put_i64(&mut payload, record.timestamp_delta);
            put_i32(&mut payload, record.offset);
            put_field(&mut payload, record.key.as_deref());
            put_field(&mut payload, record.value.as_deref());
            put_i32(&mut payload, i32::try_from(record.headers.len()).unwrap());
            for (key, value) in &record.headers {
                put_i32(&mut payload, i32::try_from(key.len()).unwrap());
                payload.extend_from_slice(key);
                put_field(&mut payload, value.as_deref());
            }
            let body_len = i32::try_from(payload.len() - body).unwrap();
            let mut prefix = Vec::new();
            put_i32(&mut prefix, body_len);
            for removed in payload.splice(start..start + 1, prefix) {
                let _ = removed;
            }
        }
        let mut bytes = vec![0_u8; 61];
        let length = i32::try_from(bytes.len() + payload.len() - 12).unwrap();
        bytes[8..12].copy_from_slice(&length.to_be_bytes());
        bytes[16] = 2;
        bytes[23..27].copy_from_slice(&last.to_be_bytes());
        bytes[27..35].copy_from_slice(&base_time.to_be_bytes());
        bytes[35..43].copy_from_slice(&max_time.to_be_bytes());
        bytes[43..51].copy_from_slice(&(-1_i64).to_be_bytes());
        bytes[51..53].copy_from_slice(&(-1_i16).to_be_bytes());
        bytes[53..57].copy_from_slice(&(-1_i32).to_be_bytes());
        let count = i32::try_from(records.len()).unwrap();
        bytes[57..61].copy_from_slice(&count.to_be_bytes());
        bytes.extend_from_slice(&payload);
        let crc = crc32c::crc32c(&bytes[21..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        bytes
    }

    fn both(bytes: &[u8], read: bool) -> (Result<(), Error>, Result<(), Error>) {
        let limits = Limits::default();
        let mut records = 0_usize;
        let mut headers = 0_usize;
        let fast = validate_batch(bytes, 0, limits, &mut records, &mut headers, read, false);
        SKIP_PLAIN.with(|flag| flag.set(true));
        let mut slow_records = 0_usize;
        let mut slow_headers = 0_usize;
        let slow = validate_batch(
            bytes,
            0,
            limits,
            &mut slow_records,
            &mut slow_headers,
            read,
            false,
        );
        SKIP_PLAIN.with(|flag| flag.set(false));
        assert_eq!(fast.is_ok(), slow.is_ok());
        if fast.is_ok() {
            assert_eq!(records, slow_records);
            assert_eq!(headers, slow_headers);
        }
        assert_eq!(fast, slow);
        (fast, slow)
    }

    #[test]
    fn plain_batches_match_the_generic_walk() {
        let records: Vec<_> = (0..4).map(|n| sample(n, b"value-100")).collect();
        let bytes = encode(&records, 1_000, 1_003, 3);
        assert!(validate(&bytes, Limits::default()).is_ok());
        assert!(both(&bytes, false).0.is_ok());
        assert!(both(&bytes, true).0.is_ok());

        let wide: Vec<_> = (0..9_000).map(|n| sample(n, b"x")).collect();
        let wide_bytes = encode(&wide, 1_000, 1_000 + 8_999, 8_999);
        assert_eq!(
            validate(&wide_bytes, Limits::default())
                .unwrap()
                .record_count(),
            9_000
        );
        assert!(both(&wide_bytes, false).0.is_ok());

        let mut with_header = sample(0, b"v");
        with_header
            .headers
            .push((b"k".to_vec(), Some(b"v".to_vec())));
        let header_bytes = encode(&[with_header], 1_000, 1_000, 0);
        assert_eq!(
            validate(&header_bytes, Limits::default())
                .unwrap()
                .header_count(),
            1
        );
        assert!(both(&header_bytes, false).0.is_ok());

        let sparse = [sample(0, b"a"), sample(2, b"b"), sample(5, b"c")];
        let sparse_bytes = encode(&sparse, 50, 55, 5);
        assert!(both(&sparse_bytes, true).0.is_ok());
        assert!(matches!(
            both(&sparse_bytes, false).0,
            Err(Error::Invalid {
                kind: Invalid::Offset,
                ..
            })
        ));
        assert_eq!(
            validate_read(&sparse_bytes, Limits::default())
                .unwrap()
                .record_count(),
            3
        );

        let mut bad_time = encode(&records, 1_000, 1_004, 3);
        let crc = crc32c::crc32c(&bad_time[21..]);
        bad_time[17..21].copy_from_slice(&crc.to_be_bytes());
        assert!(matches!(
            both(&bad_time, false).0,
            Err(Error::Invalid {
                kind: Invalid::Timestamp,
                ..
            })
        ));

        for index in 0..bytes.len() {
            let mut flipped = bytes.clone();
            flipped[index] ^= 0x5a;
            let _ = both(&flipped, false);
            let _ = both(&flipped, true);
        }

        let mut state = 0x1234_5678_u64;
        for _ in 0..48 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let count = 1 + (state % 5) as usize;
            let mut garbage = vec![0_u8; 61 + (state % 180) as usize];
            for (pos, byte) in garbage.iter_mut().enumerate().skip(61) {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                *byte = (state >> 33) as u8;
                let _ = pos;
            }
            let length = i32::try_from(garbage.len() - 12).unwrap();
            garbage[8..12].copy_from_slice(&length.to_be_bytes());
            garbage[16] = 2;
            let last = i32::try_from(count - 1).unwrap();
            garbage[23..27].copy_from_slice(&last.to_be_bytes());
            garbage[27..35].copy_from_slice(&1_000_i64.to_be_bytes());
            garbage[35..43].copy_from_slice(&1_000_i64.to_be_bytes());
            garbage[43..51].copy_from_slice(&(-1_i64).to_be_bytes());
            garbage[51..53].copy_from_slice(&(-1_i16).to_be_bytes());
            garbage[53..57].copy_from_slice(&(-1_i32).to_be_bytes());
            garbage[57..61].copy_from_slice(&(count as i32).to_be_bytes());
            let _ = both(&garbage, false);
            let _ = both(&garbage, true);
        }
    }
}
