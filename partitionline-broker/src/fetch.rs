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

    fn read_into(
        &mut self,
        store: &mut produce::Store,
        id: crate::catalog::TopicId,
        index: i32,
        offset: i64,
        dest: &mut Vec<u8>,
    ) -> Result<Result<Option<journal::PayloadView>, i16>, Error> {
        if self.entries == 0 || self.bytes == 0 {
            return Err(Error::ScanLimit);
        }
        let (view, work) =
            match store.read_entry_into(id, index, offset, self.bytes, self.entries, dest) {
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
        Ok(Ok(view))
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
/// A log start past the high watermark is storage corruption, not a reset hint.
fn stored_log_error(error: i16, log_start: i64, watermark: i64) -> i16 {
    if error == 0 && log_start > watermark {
        56
    } else {
        error
    }
}

/// High watermark, last stable offset, and log start written on a Fetch
/// partition. Offset-out-of-range still carries the boundaries so the
/// consumer can reset without another lookup. Other errors clear them.
fn fetch_boundary(error: i16, watermark: i64, log_start: i64) -> (i64, i64, i64) {
    if error == 0 || error == 1 {
        (watermark, watermark, log_start)
    } else {
        (-1, -1, -1)
    }
}

fn storage_code(error: i16, key: i16, version: i16) -> i16 {
    if key == 1 && version <= 5 && error == 56 {
        6
    } else {
        error
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListQuery {
    Latest,
    Earliest,
    MaxTimestamp,
    TieredEnd,
    Timestamp(i64),
}

fn list_query(timestamp: i64) -> ListQuery {
    match timestamp {
        -1 => ListQuery::Latest,
        -2 | -4 => ListQuery::Earliest,
        -3 => ListQuery::MaxTimestamp,
        -5 => ListQuery::TieredEnd,
        other => ListQuery::Timestamp(other),
    }
}

fn offset_in_log(offset: i64, log_start: i64, watermark: i64) -> bool {
    offset >= log_start && offset < watermark
}

fn observe_max(
    batch: records::Batch<'_>,
    log_start: i64,
    watermark: i64,
    active: &impl Fn() -> Result<(), Error>,
) -> Result<Option<(i64, i64)>, Error> {
    let mut best = None;
    for record in batch.records() {
        active()?;
        let record = record.map_err(|_| Error::StoredRecords)?;
        if !offset_in_log(record.offset, log_start, watermark) {
            continue;
        }
        let candidate = (record.timestamp, record.offset);
        best = Some(match best {
            Some(current) if candidate <= current => current,
            _ => candidate,
        });
    }
    Ok(best)
}
fn first_timestamp(
    batch: records::Batch<'_>,
    wanted: i64,
    log_start: i64,
    watermark: i64,
    active: &impl Fn() -> Result<(), Error>,
) -> Result<Option<(i64, i64)>, Error> {
    if batch.max_timestamp < wanted {
        return Ok(None);
    }
    for record in batch.records() {
        active()?;
        let record = record.map_err(|_| Error::StoredRecords)?;
        if offset_in_log(record.offset, log_start, watermark) && record.timestamp >= wanted {
            return Ok(Some((record.timestamp, record.offset)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::list_query;
    use super::ListQuery;

    #[test]
    fn a_listed_offset_must_fall_inside_the_log() {
        assert!(super::offset_in_log(1, 1, 4));
        assert!(super::offset_in_log(3, 1, 4));
        assert!(!super::offset_in_log(0, 1, 4));
        assert!(!super::offset_in_log(4, 1, 4));
        assert!(!super::offset_in_log(1, 2, 2));
    }

    #[test]
    fn a_log_start_past_the_high_watermark_is_storage_corruption() {
        assert_eq!(super::stored_log_error(0, 5, 2), 56);
        assert_eq!(super::stored_log_error(0, 2, 2), 0);
        assert_eq!(super::stored_log_error(0, 1, 4), 0);
        assert_eq!(super::stored_log_error(3, 5, 2), 3);
    }

    #[test]
    fn out_of_range_keeps_the_log_boundaries() {
        assert_eq!(super::fetch_boundary(0, 4, 1), (4, 4, 1));
        assert_eq!(super::fetch_boundary(1, 4, 1), (4, 4, 1));
        assert_eq!(super::fetch_boundary(3, 4, 1), (-1, -1, -1));
    }

    #[test]
    fn a_pre_epoch_timestamp_is_searched() {
        assert_eq!(list_query(-1), ListQuery::Latest);
        assert_eq!(list_query(-2), ListQuery::Earliest);
        assert_eq!(list_query(-4), ListQuery::Earliest);
        assert_eq!(list_query(-3), ListQuery::MaxTimestamp);
        assert_eq!(list_query(-5), ListQuery::TieredEnd);
        assert_eq!(list_query(-100), ListQuery::Timestamp(-100));
        assert_eq!(list_query(0), ListQuery::Timestamp(0));
    }
}

fn stored_records(
    payload: &[u8],
    first_offset: u64,
    record_count: u32,
    limits: records::Limits,
) -> Result<(records::ReadSummary<'_>, i64), Error> {
    let first = i64::try_from(first_offset).map_err(|_| Error::StoredRecords)?;
    let next = first
        .checked_add(i64::from(record_count))
        .filter(|next| *next > first)
        .ok_or(Error::StoredRecords)?;
    let checked = records::frame_stored(payload, limits)
        .or_else(|_| records::validate_read(payload, limits))
        .map_err(|_| Error::StoredRecords)?;
    if checked.first_offset().is_some_and(|offset| offset < first)
        || checked.next_offset().is_some_and(|offset| offset > next)
    {
        return Err(Error::StoredRecords);
    }
    Ok((checked, next))
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
    store.invalidate_length_observations();
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
            let (id, log_start, watermark, error) = match target {
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
            let mut error = stored_log_error(error, log_start, watermark);
            if key == 2 {
                let mut timestamp = -1;
                let mut offset = -1;
                if error == 0 {
                    // -1 latest, -2 earliest, -4 earliest local. -5 is the
                    // tiered end, which this log does not have. -3 is the
                    // maximum timestamp. Every other value, including a time
                    // before the epoch, is a timestamp search.
                    let query = list_query(part.offset);
                    if query == ListQuery::Earliest {
                        offset = log_start;
                    } else if query == ListQuery::Latest {
                        offset = watermark;
                    } else if query != ListQuery::TieredEnd {
                        if let Some(id) = id {
                            let want_max = query == ListQuery::MaxTimestamp;
                            let mut best = None;
                            let mut cursor = if want_max {
                                log_start
                            } else {
                                match store.timestamp_start(id, part.index, part.offset) {
                                    Ok(offset) => offset.max(log_start),
                                    Err(_) => {
                                        error = 56;
                                        watermark
                                    }
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
                                let (checked, next) = stored_records(
                                    &entry.payload,
                                    entry.first_offset,
                                    entry.record_count,
                                    store.record_limits(),
                                )?;
                                for batch in checked.batches() {
                                    active()?;
                                    let batch = batch.map_err(|_| Error::StoredRecords)?;
                                    if want_max {
                                        if let Some(found) =
                                            observe_max(batch, log_start, watermark, &active)?
                                        {
                                            best = Some(match best {
                                                Some(current) if found <= current => current,
                                                _ => found,
                                            });
                                        }
                                    } else if let Some(found) = first_timestamp(
                                        batch,
                                        part.offset,
                                        log_start,
                                        watermark,
                                        &active,
                                    )? {
                                        (timestamp, offset) = found;
                                        break 'scan;
                                    }
                                }
                                if next <= cursor {
                                    return Err(Error::StoredRecords);
                                }
                                cursor = next;
                            }
                            if want_max && error == 0 {
                                if let Some((found_timestamp, found_offset)) = best {
                                    timestamp = found_timestamp;
                                    offset = found_offset;
                                }
                            }
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
            let (high_watermark, stable, start) = fetch_boundary(error, watermark, log_start);
            out.i32(part.index)?;
            out.i16(error)?;
            out.i64(high_watermark)?;
            out.i64(stable)?;
            if version >= 5 {
                out.i64(start)?;
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
                        let payload_at = out.bytes.len();
                        let loaded = match budget.read_into(
                            store,
                            id,
                            part.index,
                            cursor,
                            &mut out.bytes,
                        )? {
                            Err(code) => {
                                error = storage_code(code, key, version);
                                break;
                            }
                            Ok(Some(view)) => view,
                            Ok(None) => {
                                error = storage_code(56, key, version);
                                break;
                            }
                        };
                        let payload_end = out.bytes.len();
                        let mut keep_from = None;
                        let mut keep_to = payload_at;
                        let mut stop = false;
                        let entry_next;
                        let mut failed = None;
                        {
                            let payload = out
                                .bytes
                                .get(payload_at..payload_end)
                                .ok_or(Error::StoredRecords)?;
                            let (checked, next) = stored_records(
                                payload,
                                loaded.first_offset,
                                loaded.record_count,
                                store.record_limits(),
                            )?;
                            entry_next = next;
                            let mut at = 0usize;
                            for batch in checked.batches() {
                                active()?;
                                let batch = match batch {
                                    Ok(batch) => batch,
                                    Err(_) => {
                                        failed = Some(Error::StoredRecords);
                                        break;
                                    }
                                };
                                let end = match at.checked_add(batch.bytes.len()) {
                                    Some(end) => end,
                                    None => {
                                        failed = Some(Error::StoredRecords);
                                        break;
                                    }
                                };
                                if batch.next_offset <= cursor {
                                    at = end;
                                    continue;
                                }
                                let next_part = match part_bytes.checked_add(batch.bytes.len()) {
                                    Some(next_part) => next_part,
                                    None => {
                                        failed = Some(Error::ResponseLimit);
                                        break;
                                    }
                                };
                                let next_total = match record_bytes.checked_add(batch.bytes.len()) {
                                    Some(next_total) => next_total,
                                    None => {
                                        failed = Some(Error::ResponseLimit);
                                        break;
                                    }
                                };
                                if record_bytes != 0
                                    && (next_part > part_maximum || next_total > maximum)
                                {
                                    stop = true;
                                    break;
                                }
                                if fixed
                                    .checked_add(next_total)
                                    .is_none_or(|n| n > common.max_response_bytes)
                                {
                                    failed = Some(Error::ResponseLimit);
                                    break;
                                }
                                if keep_from.is_none() {
                                    keep_from = Some(payload_at.saturating_add(at));
                                }
                                keep_to = payload_at.saturating_add(end);
                                part_bytes = next_part;
                                record_bytes = next_total;
                                cursor = batch.next_offset;
                                at = end;
                            }
                        }
                        match keep_from {
                            Some(from) => {
                                if from != payload_at {
                                    out.bytes.copy_within(from..keep_to, payload_at);
                                }
                                out.bytes.truncate(
                                    payload_at.saturating_add(keep_to.saturating_sub(from)),
                                );
                            }
                            None => out.bytes.truncate(payload_at),
                        }
                        if let Some(error) = failed {
                            return Err(error);
                        }
                        if stop {
                            break 'read;
                        }
                        if cursor < watermark {
                            if entry_next <= start {
                                return Err(Error::StoredRecords);
                            }
                            cursor = entry_next;
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
