//! Bounded ordinary persisted Fetch4–6 and ListOffsets1–3.
//!
//! [`crate::metadata::Router::open_with_read_store`] explicitly selects the
//! read/write profile. The Produce-only and metadata-only profiles stay separate.
//! Snapshot reads, catalog identity resolution and record work run on the same
//! blocking actor as append/delete. Long polling waits outside that actor while
//! retaining bounded data admission. No incremental sessions, replica reads,
//! transactions or replication are implemented. The explicit retention profile
//! shares durable logical starts with these reads. Ordinary-only writes
//! make last-stable-offset equal high-watermark; read-committed has no aborts.
//!
//! Whole batches are returned, including a containing batch for an interior
//! offset. The first nonempty batch may exceed request/partition byte limits,
//! but never the configured response ceiling. Scan exhaustion fails explicitly;
//! it cannot become a false timestamp miss or silently skip retained records.

use crate::{catalog::Catalog, journal, metadata, partition, produce, protocol, records};
use std::time::Duration;

/// Explicit composed ordinary read/write advertisement; not the Produce-only one.
pub static DATA_API_VERSIONS: [protocol::ApiVersion; 7] = [
    protocol::ApiVersion {
        api_key: 0,
        min_version: 3,
        max_version: 13,
    },
    protocol::ApiVersion {
        api_key: 1,
        min_version: 4,
        max_version: 6,
    },
    protocol::ApiVersion {
        api_key: 2,
        min_version: 1,
        max_version: 3,
    },
    protocol::IMPLEMENTED_API_VERSIONS[0],
    protocol::IMPLEMENTED_API_VERSIONS[1],
    protocol::IMPLEMENTED_API_VERSIONS[2],
    protocol::IMPLEMENTED_API_VERSIONS[3],
];

/// Positive scan-work, long-poll and retained read input/output bounds.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    scan_bytes: usize,
    scan_entries: usize,
    wait_ms: u32,
    retained_bytes: usize,
}
impl Limits {
    /// Validate bytes1..=64MiB, entries1..=65536 and wait1..=600000ms.
    ///
    /// Bytes count fetched journal payloads, entries count complete journal
    /// inputs. A read that cannot fit fails before its payload is allocated.
    pub fn new(scan_bytes: usize, scan_entries: usize, wait_ms: u32) -> Result<Self, Error> {
        if !(1..=64 * 1024 * 1024).contains(&scan_bytes)
            || !(1..=65_536).contains(&scan_entries)
            || !(1..=600_000).contains(&wait_ms)
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            scan_bytes,
            scan_entries,
            wait_ms,
            retained_bytes: 1024 * 1024 * 1024,
        })
    }
    /// Maximum journal payload bytes visited per snapshot, across all partitions.
    pub fn max_scan_bytes(self) -> usize {
        self.scan_bytes
    }
    /// Maximum journal entries visited per snapshot, across all partitions.
    pub fn max_scan_entries(self) -> usize {
        self.scan_entries
    }
    /// Maximum admitted long-poll time, measured from initial admission.
    pub fn max_wait(self) -> Duration {
        Duration::from_millis(u64::from(self.wait_ms))
    }
    /// Set the combined retained input/output ceiling, 1byte..=1GiB.
    ///
    /// Router startup requires admission count times (maximum request plus
    /// maximum response bytes) to fit this ceiling. Waiting inputs and completed
    /// unconsumed snapshots share those permits. Actor scan buffers, parser
    /// metadata, allocator overhead, transport/caller ownership and RSS are
    /// separate from this byte envelope.
    pub fn with_retained_bytes(mut self, bytes: usize) -> Result<Self, Error> {
        if !(1..=1024 * 1024 * 1024).contains(&bytes) {
            return Err(Error::InvalidLimits);
        }
        self.retained_bytes = bytes;
        Ok(self)
    }
    /// Combined admitted read input/output byte ceiling, default1GiB.
    pub fn max_retained_bytes(self) -> usize {
        self.retained_bytes
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            scan_bytes: 16 * 1024 * 1024,
            scan_entries: 65_536,
            wait_ms: 60_000,
            retained_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// Structural/resource failures close the affected connection without partial bytes.
#[derive(Debug)]
pub enum Error {
    /// Invalid positive local scan/wait bounds.
    InvalidLimits,
    /// Header/body failed complete bounded schema parsing.
    Protocol(protocol::Error),
    /// Aggregate topic/partition count exceeded its configured/byte bound.
    RequestCount,
    /// Scan work cannot complete inside configured byte/entry limits.
    ScanLimit,
    /// Complete response cannot fit the hard configured output ceiling.
    ResponseLimit,
    /// A bounded allocation failed.
    Allocation,
    /// A previously validated stored batch has an inconsistent projection.
    StoredRecords,
    /// Request was canceled or the actor is stopping.
    Canceled,
}
impl From<protocol::Error> for Error {
    fn from(value: protocol::Error) -> Self {
        Self::Protocol(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fetch: {self:?}")
    }
}
impl std::error::Error for Error {}

pub(crate) struct Snapshot {
    pub(crate) response: Vec<u8>,
    pub(crate) record_bytes: usize,
    pub(crate) minimum_bytes: usize,
    pub(crate) wait: Duration,
    pub(crate) immediate: bool,
}
struct Part {
    index: i32,
    offset: i64,
    maximum: i32,
}
struct Topic<'a> {
    name: &'a str,
    parts: Vec<Part>,
}
struct Request<'a> {
    replica: i32,
    wait: i32,
    minimum: i32,
    maximum: i32,
    isolation: u8,
    topics: Vec<Topic<'a>>,
}
fn reserved<T>(count: usize) -> Result<Vec<T>, Error> {
    let mut value = Vec::new();
    value
        .try_reserve_exact(count)
        .map_err(|_| Error::Allocation)?;
    Ok(value)
}
fn parse<'a>(
    key: i16,
    version: i16,
    body: &'a [u8],
    common: &metadata::Config,
    maximum_parts: usize,
) -> Result<Request<'a>, Error> {
    let mut reader = Reader { bytes: body };
    let replica = reader.i32()?;
    let (wait, minimum, maximum) = if key == 1 {
        (reader.i32()?, reader.i32()?, reader.i32()?)
    } else {
        (0, 0, 0)
    };
    let isolation = if key == 1 || version >= 2 {
        reader.byte()?
    } else {
        0
    };
    let count = reader.count(common.max_topics, 6)?;
    let mut topics = reserved(count)?;
    let mut total = 0usize;
    for _ in 0..count {
        let name = reader.string()?;
        let count = reader.count(
            maximum_parts,
            if key == 2 {
                12
            } else if version >= 5 {
                24
            } else {
                16
            },
        )?;
        total = total.checked_add(count).ok_or(Error::RequestCount)?;
        if total > maximum_parts {
            return Err(Error::RequestCount);
        }
        let mut parts = reserved(count)?;
        for _ in 0..count {
            let index = reader.i32()?;
            let offset = reader.i64()?;
            if key == 1 && version >= 5 {
                let _ = reader.i64()?;
            }
            let maximum = if key == 1 { reader.i32()? } else { 0 };
            parts.push(Part {
                index,
                offset,
                maximum,
            });
        }
        topics.push(Topic { name, parts });
    }
    if !reader.bytes.is_empty() {
        return Err(protocol::Error::TrailingBytes.into());
    }
    Ok(Request {
        replica,
        wait,
        minimum,
        maximum,
        isolation,
        topics,
    })
}

struct Reader<'a> {
    bytes: &'a [u8],
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let value = self.bytes.get(..n).ok_or(protocol::Error::Truncated)?;
        self.bytes = &self.bytes[n..];
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16, Error> {
        Ok(i16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn string(&mut self) -> Result<&'a str, Error> {
        let count = usize::try_from(self.i16()?).map_err(|_| protocol::Error::InvalidLength)?;
        Ok(std::str::from_utf8(self.take(count)?).map_err(|_| protocol::Error::InvalidUtf8)?)
    }
    fn count(&mut self, maximum: usize, minimum: usize) -> Result<usize, Error> {
        let count = usize::try_from(self.i32()?).map_err(|_| protocol::Error::InvalidLength)?;
        if count > maximum || count > self.bytes.len() / minimum {
            return Err(Error::RequestCount);
        }
        Ok(count)
    }
    fn var(&mut self, width: u32) -> Result<i64, Error> {
        let mut encoded = 0u64;
        for shift in (0..width).step_by(7) {
            let byte = self.byte()?;
            if (width == 64 && shift == 63 && byte > 1) || (width == 32 && shift == 28 && byte > 15)
            {
                return Err(Error::StoredRecords);
            }
            encoded |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok((encoded >> 1) as i64 ^ -((encoded & 1) as i64));
            }
        }
        Err(Error::StoredRecords)
    }
}

struct Writer {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Writer {
    fn new(fixed: usize, maximum: usize) -> Result<Self, Error> {
        if fixed > maximum {
            return Err(Error::ResponseLimit);
        }
        Ok(Self {
            bytes: reserved(fixed)?,
            maximum,
        })
    }
    fn bytes(&mut self, value: &[u8]) -> Result<(), Error> {
        if self
            .bytes
            .len()
            .checked_add(value.len())
            .is_none_or(|n| n > self.maximum)
        {
            return Err(Error::ResponseLimit);
        }
        self.bytes
            .try_reserve_exact(value.len())
            .map_err(|_| Error::Allocation)?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }
    fn i16(&mut self, value: i16) -> Result<(), Error> {
        self.bytes(&value.to_be_bytes())
    }
    fn i32(&mut self, value: i32) -> Result<(), Error> {
        self.bytes(&value.to_be_bytes())
    }
    fn i64(&mut self, value: i64) -> Result<(), Error> {
        self.bytes(&value.to_be_bytes())
    }
    fn count(&mut self, value: usize) -> Result<(), Error> {
        self.i32(i32::try_from(value).map_err(|_| Error::ResponseLimit)?)
    }
    fn string(&mut self, value: &str) -> Result<(), Error> {
        self.i16(i16::try_from(value.len()).map_err(|_| Error::ResponseLimit)?)?;
        self.bytes(value.as_bytes())
    }
    fn patch_length(&mut self, position: usize, length: usize) -> Result<(), Error> {
        self.bytes
            .get_mut(position..position + 4)
            .ok_or(Error::StoredRecords)?
            .copy_from_slice(
                &i32::try_from(length)
                    .map_err(|_| Error::ResponseLimit)?
                    .to_be_bytes(),
            );
        Ok(())
    }
}
struct Budget {
    bytes: usize,
    entries: usize,
}
impl Budget {
    fn read(
        &mut self,
        store: &mut produce::Store,
        id: crate::catalog::TopicId,
        index: i32,
        offset: i64,
    ) -> Result<Result<Option<journal::Entry>, i16>, Error> {
        if self.entries == 0 || self.bytes == 0 {
            return Err(Error::ScanLimit);
        }
        let (entry, work) = match store.read_entry(id, index, offset, self.bytes, self.entries) {
            Ok(value) => value,
            Err(partition::Error::Storage(journal::Error::FetchBudgetExceeded)) => {
                return Err(Error::ScanLimit)
            }
            Err(partition::Error::Segments(crate::segments::Error::ScanBudget)) => {
                return Err(Error::ScanLimit)
            }
            Err(_) => return Ok(Err(56)),
        };
        self.bytes = self.bytes.checked_sub(work.bytes).ok_or(Error::ScanLimit)?;
        self.entries = self
            .entries
            .checked_sub(work.entries)
            .ok_or(Error::ScanLimit)?;
        Ok(Ok(entry))
    }
}
fn fixed_size(request: &Request<'_>, key: i16, version: i16) -> Result<usize, Error> {
    request.topics.iter().try_fold(
        if key == 1 || version >= 2 { 12usize } else { 8 },
        |sum, topic| {
            let part = if key == 2 {
                22usize
            } else if version >= 5 {
                38
            } else {
                30
            };
            sum.checked_add(6)
                .and_then(|n| n.checked_add(topic.name.len()))
                .and_then(|n| {
                    topic
                        .parts
                        .len()
                        .checked_mul(part)
                        .and_then(|p| n.checked_add(p))
                })
                .ok_or(Error::ResponseLimit)
        },
    )
}
fn selected<'a>(
    catalog: &'a Catalog,
    topic: &Topic<'_>,
    position: usize,
    part: &Part,
    request: &Request<'_>,
    invalid: bool,
) -> Result<&'a crate::catalog::Topic, i16> {
    if invalid
        || topic.parts[..position]
            .iter()
            .any(|p| p.index == part.index)
        || topic.parts[position + 1..]
            .iter()
            .any(|p| p.index == part.index)
        || request
            .topics
            .iter()
            .filter(|t| t.name == topic.name)
            .count()
            != 1
    {
        return Err(42);
    }
    let target = catalog.by_name(topic.name).ok_or(3i16)?;
    if part.index < 0 || part.index as u32 >= target.partition_count() {
        return Err(3);
    }
    Ok(target)
}
fn storage_code(error: i16, key: i16, version: i16) -> i16 {
    if key == 1 && version <= 5 && error == 56 {
        6
    } else {
        error
    }
}
fn first_timestamp(
    batch: records::Batch<'_>,
    wanted: i64,
    log_start: i64,
    active: &impl Fn() -> Result<(), Error>,
) -> Result<Option<(i64, i64)>, Error> {
    if batch.max_timestamp < wanted {
        return Ok(None);
    }
    let base_time = i64::from_be_bytes(
        batch
            .bytes
            .get(27..35)
            .ok_or(Error::StoredRecords)?
            .try_into()
            .map_err(|_| Error::StoredRecords)?,
    );
    let mut reader = Reader {
        bytes: batch.bytes.get(61..).ok_or(Error::StoredRecords)?,
    };
    for _ in 0..batch.record_count {
        active()?;
        let length = usize::try_from(reader.var(32)?).map_err(|_| Error::StoredRecords)?;
        let mut record = Reader {
            bytes: reader.take(length)?,
        };
        if record.byte()? != 0 {
            return Err(Error::StoredRecords);
        }
        let timestamp = base_time
            .checked_add(record.var(64)?)
            .ok_or(Error::StoredRecords)?;
        let offset = batch
            .base_offset
            .checked_add(record.var(32)?)
            .ok_or(Error::StoredRecords)?;
        if offset >= log_start && timestamp >= wanted {
            return Ok(Some((timestamp, offset)));
        }
    }
    Ok(None)
}

pub(crate) fn process(
    catalog: &Catalog,
    store: &mut produce::Store,
    input: &[u8],
    common: &metadata::Config,
    limits: Limits,
    active: impl Fn() -> Result<(), Error>,
) -> Result<Snapshot, Error> {
    active()?;
    let (header, body) = protocol::RequestHeader::parse(input, 1, common.protocol_limits)?;
    let key = header.api_key;
    let version = header.api_version;
    let request = parse(key, version, body, common, store.max_partitions())?;
    let fixed = fixed_size(&request, key, version)?;
    let mut out = Writer::new(fixed, common.max_response_bytes)?;
    let invalid = request.replica != -1
        || request.isolation > 1
        || request.wait < 0
        || request.minimum < 0
        || request.maximum < 0;
    let maximum = usize::try_from(request.maximum)
        .unwrap_or(0)
        .min(common.max_response_bytes - fixed);
    let minimum = usize::try_from(request.minimum).unwrap_or(0).min(maximum);
    let mut record_bytes = 0usize;
    let mut immediate = request.topics.iter().all(|t| t.parts.is_empty()) || invalid;
    let mut budget = Budget {
        bytes: limits.scan_bytes,
        entries: limits.scan_entries,
    };
    out.i32(header.correlation_id)?;
    if key == 1 || version >= 2 {
        out.i32(0)?;
    }
    out.count(request.topics.len())?;
    for topic in &request.topics {
        out.string(topic.name)?;
        out.count(topic.parts.len())?;
        for (position, part) in topic.parts.iter().enumerate() {
            active()?;
            let target = selected(catalog, topic, position, part, &request, invalid);
            let (id, log_start, watermark, mut error) = match target {
                Err(error) => (None, -1, -1, error),
                Ok(target) => match (
                    store.log_start(target.id(), part.index),
                    store.watermark(target.id(), part.index),
                ) {
                    (Ok(log_start), Ok(watermark)) => (Some(target.id()), log_start, watermark, 0),
                    (Err(error), _) | (_, Err(error)) => {
                        (None, -1, -1, storage_code(error, key, version))
                    }
                },
            };
            if key == 2 {
                let mut timestamp = -1;
                let mut offset = -1;
                if error == 0 {
                    if part.offset == -2 {
                        offset = log_start;
                    } else if part.offset == -1 {
                        offset = watermark;
                    } else if part.offset < 0 {
                        error = 35;
                    } else if let Some(id) = id {
                        let mut cursor = match store.timestamp_start(id, part.index, part.offset) {
                            Ok(offset) => offset.max(log_start),
                            Err(_) => {
                                error = 56;
                                watermark
                            }
                        };
                        'scan: while cursor < watermark {
                            active()?;
                            let entry = match budget.read(store, id, part.index, cursor)? {
                                Err(code) => {
                                    error = code;
                                    break;
                                }
                                Ok(Some(entry)) => entry,
                                Ok(None) => {
                                    error = 56;
                                    break;
                                }
                            };
                            let checked = records::validate(&entry.payload, store.record_limits())
                                .map_err(|_| Error::StoredRecords)?;
                            for batch in checked.batches() {
                                active()?;
                                let batch = batch.map_err(|_| Error::StoredRecords)?;
                                if let Some(found) =
                                    first_timestamp(batch, part.offset, log_start, &active)?
                                {
                                    (timestamp, offset) = found;
                                    break 'scan;
                                }
                            }
                            cursor = i64::try_from(entry.first_offset)
                                .ok()
                                .and_then(|first| first.checked_add(i64::from(entry.record_count)))
                                .ok_or(Error::StoredRecords)?;
                        }
                    }
                }
                out.i32(part.index)?;
                out.i16(error)?;
                out.i64(timestamp)?;
                out.i64(offset)?;
                continue;
            }
            if error == 0 && (part.offset < log_start || part.offset > watermark) {
                error = 1;
            }
            if error == 0 && part.maximum < 0 {
                error = 42;
            }
            let part_start = out.bytes.len();
            out.i32(part.index)?;
            out.i16(error)?;
            out.i64(if error == 0 { watermark } else { -1 })?;
            out.i64(if error == 0 { watermark } else { -1 })?;
            if version >= 5 {
                out.i64(if error == 0 { log_start } else { -1 })?;
            }
            out.i32(if error == 0 && request.isolation == 1 {
                0
            } else {
                -1
            })?;
            let length_at = out.bytes.len();
            out.i32(0)?;
            let data_at = out.bytes.len();
            if error == 0 {
                if let Some(id) = id {
                    let mut cursor = part.offset;
                    let part_maximum =
                        usize::try_from(part.maximum).map_err(|_| Error::StoredRecords)?;
                    let mut part_bytes = 0usize;
                    'read: while cursor < watermark {
                        active()?;
                        if record_bytes != 0
                            && (record_bytes >= maximum || part_bytes >= part_maximum)
                        {
                            break;
                        }
                        let start = cursor;
                        let entry = match budget.read(store, id, part.index, cursor)? {
                            Err(code) => {
                                error = storage_code(code, key, version);
                                break;
                            }
                            Ok(Some(entry)) => entry,
                            Ok(None) => {
                                error = storage_code(56, key, version);
                                break;
                            }
                        };
                        let checked = records::validate(&entry.payload, store.record_limits())
                            .map_err(|_| Error::StoredRecords)?;
                        for batch in checked.batches() {
                            active()?;
                            let batch = batch.map_err(|_| Error::StoredRecords)?;
                            if batch.next_offset <= cursor {
                                continue;
                            }
                            let next_part = part_bytes
                                .checked_add(batch.bytes.len())
                                .ok_or(Error::ResponseLimit)?;
                            let next_total = record_bytes
                                .checked_add(batch.bytes.len())
                                .ok_or(Error::ResponseLimit)?;
                            if record_bytes != 0
                                && (next_part > part_maximum || next_total > maximum)
                            {
                                break 'read;
                            }
                            if fixed
                                .checked_add(next_total)
                                .is_none_or(|n| n > common.max_response_bytes)
                            {
                                return Err(Error::ResponseLimit);
                            }
                            out.bytes(batch.bytes)?;
                            part_bytes = next_part;
                            record_bytes = next_total;
                            cursor = batch.next_offset;
                        }
                        if cursor < watermark {
                            let next = i64::try_from(entry.first_offset)
                                .ok()
                                .and_then(|first| first.checked_add(i64::from(entry.record_count)))
                                .ok_or(Error::StoredRecords)?;
                            if next <= start {
                                return Err(Error::StoredRecords);
                            }
                            cursor = next;
                        }
                    }
                }
            }
            if error != 0 {
                immediate = true;
                let removed = out.bytes.len() - data_at;
                record_bytes -= removed;
                out.bytes.truncate(part_start);
                out.i32(part.index)?;
                out.i16(error)?;
                out.i64(-1)?;
                out.i64(-1)?;
                if version >= 5 {
                    out.i64(-1)?;
                }
                out.i32(0)?;
                out.i32(0)?;
            } else {
                out.patch_length(length_at, out.bytes.len() - data_at)?;
            }
        }
    }
    active()?;
    Ok(Snapshot {
        response: out.bytes,
        record_bytes,
        minimum_bytes: minimum,
        wait: Duration::from_millis(u64::from(
            u32::try_from(request.wait).unwrap_or(0).min(limits.wait_ms),
        )),
        immediate: immediate || key == 2,
    })
}
