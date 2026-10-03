//! Bounded ordinary Kafka Produce and exclusively owned durable partition stores.
//!
//! The data router accepts Produce3..13, one ordinary magic2 batch per partition,
//! with CreateTime, no producer sequencing and no transaction identifier. Local
//! fsync precedes acks1/-1 success; -1 covers only the configured single-node ISR.
//! Acks0 success emits no bytes and keeps the channel open; errors close it.
//! This is not replicated durability, idempotence or production qualification.
//!
//! All filesystem/codec work runs on the existing catalog blocking actor. Store
//! filenames contain only a validated UUID and partition index. Deleted identities'
//! logs remain bounded history; recreating a name never exposes old data. Configured
//! paths and cross-process exclusivity remain trusted operator responsibilities.

use crate::{
    catalog::{Catalog, TopicId},
    compaction, journal, metadata, partition, protocol, records, retention, segments,
};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Advertisement selected only by an explicitly configured ordinary data router.
pub static DATA_API_VERSIONS: [protocol::ApiVersion; 5] = [
    protocol::ApiVersion {
        api_key: 0,
        min_version: 3,
        max_version: 13,
    },
    protocol::IMPLEMENTED_API_VERSIONS[0],
    protocol::IMPLEMENTED_API_VERSIONS[1],
    protocol::IMPLEMENTED_API_VERSIONS[2],
    protocol::IMPLEMENTED_API_VERSIONS[3],
];

/// Positive storage, normalized-work and historical-resource envelopes.
#[derive(Debug, Clone)]
pub struct Config {
    /// Exclusively owned directory; its parent must exist. Names never enter it.
    pub directory: PathBuf,
    /// Active and historical partition store handles, 1..=4096; no eviction.
    /// Rolling limits separately bound the data/sidecar files in each store.
    pub max_stores: usize,
    /// Worst-case sum of configured backend disk budgets, at most one TiB.
    pub max_disk_bytes: u64,
    /// Aggregate configured index envelopes; monolithic slots charge64bytes
    /// each, while rolling limits include retained and transient index buffers.
    /// Allocator bookkeeping is separate; the total is at most512MiB.
    pub max_index_bytes: usize,
    /// Aggregate partitions in a request, 1..=4096.
    pub max_partitions: usize,
    /// Aggregate owned normalized batches retained before append, at most64MiB.
    pub max_normalized_bytes: usize,
    /// Per-file/index/entry/read budgets for the selected storage backend.
    pub journal_limits: journal::Limits,
    /// Explicit rolling backend. None preserves the flat monolithic layout.
    /// Rolling startup rejects legacy/mixed layouts rather than hiding records.
    /// File/index budgets include sealed generations, staging and replacement.
    /// At most `max_stores` active files plus four transient actor files are
    /// open; no sealed-reader cache or unbounded index mapping is retained.
    pub segment_limits: Option<segments::Limits>,
    /// Independent ordinary-record byte/count/work limits.
    pub record_limits: records::Limits,
    /// Optional pure Rust codec workspace, frame/window and work budgets.
    #[cfg(feature = "codecs")]
    pub codec_limits: crate::codecs::Limits,
}
impl Config {
    /// Default64 stores/FDs,64GiB file envelope,256MiB conservative index envelope.
    /// One actor also retains at most16MiB normalized work, one append copy and
    /// one maximum-entry validation/scan payload during rolling/recovery/copy.
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            max_stores: 64,
            max_disk_bytes: 64 << 30,
            max_index_bytes: 256 * 1024 * 1024,
            max_partitions: 1024,
            max_normalized_bytes: 16 * 1024 * 1024,
            journal_limits: journal::Limits::default(),
            segment_limits: None,
            record_limits: records::Limits::default(),
            #[cfg(feature = "codecs")]
            codec_limits: crate::codecs::Limits::default(),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let per_index = self
            .segment_limits
            .map_or_else(
                || self.journal_limits.max_index_entries().checked_mul(64),
                |limits| Some(limits.max_index_bytes()),
            )
            .ok_or(Error::InvalidConfig)?;
        let index = self
            .max_stores
            .checked_mul(per_index)
            .ok_or(Error::InvalidConfig)?;
        let disk = (self.max_stores as u64)
            .checked_mul(self.segment_limits.map_or(
                self.journal_limits.max_file_bytes(),
                segments::Limits::max_disk_bytes,
            ))
            .ok_or(Error::InvalidConfig)?;
        if let Some(limits) = self.segment_limits {
            limits
                .journal_limits(self.journal_limits)
                .map_err(|_| Error::InvalidConfig)?;
            if limits.index_envelope().map_err(|_| Error::InvalidConfig)? > limits.max_index_bytes()
            {
                return Err(Error::InvalidConfig);
            }
        }
        if !(1..=4096).contains(&self.max_stores)
            || !(1..=4096).contains(&self.max_partitions)
            || !(1..=64 * 1024 * 1024).contains(&self.max_normalized_bytes)
            || self.directory.as_os_str().is_empty()
            || self.directory.as_os_str().len() > 4096
            || index > self.max_index_bytes
            || self.max_index_bytes > 512 * 1024 * 1024
            || disk > self.max_disk_bytes
            || self.max_disk_bytes > 1 << 40
            || self
                .journal_limits
                .max_entry_bytes()
                .saturating_add(std::mem::size_of::<journal::Entry>())
                > self.journal_limits.max_fetch_bytes()
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
    pub(crate) fn validate_compaction(&self, policy: compaction::Policy) -> Result<(), Error> {
        self.validate()?;
        let limits = self.segment_limits.ok_or(Error::InvalidConfig)?;
        if limits
            .compaction_index_envelope()
            .map_err(|_| Error::InvalidConfig)?
            > limits.max_index_bytes()
        {
            return Err(Error::InvalidConfig);
        }
        // The cleaner, source payload and publication buffers share one total
        // envelope; no normalized Produce request is retained during this job.
        limits
            .compaction_scratch_bytes(policy.limits(), self.journal_limits)
            .map_err(|_| Error::InvalidConfig)?;
        Ok(())
    }
}

/// Structural/lifecycle failures; no payload or configured path is retained.
#[derive(Debug)]
pub enum Error {
    /// Invalid resource envelope or operator path configuration.
    InvalidConfig,
    /// Malformed bounded Kafka header/body.
    Protocol(protocol::Error),
    /// Request count exceeds the configured or remaining-byte bound.
    RequestCount,
    /// Request has no response because acks0 was successful.
    NoResponse,
    /// Acks0 contained a partition error; transport must close the connection.
    AcksZeroError,
    /// Cancellation/shutdown before an operation was admitted to storage.
    Canceled,
    /// Response/normalized-work allocation or configured budget failed preflight.
    ResourceLimit,
    /// Directory layout or unknown persisted identity is invalid.
    InvalidStore,
    /// Startup filesystem failure; no data router was returned.
    Io(std::io::Error),
    /// Startup partition recovery failed; no data router was returned.
    Partition(partition::Error),
}
impl From<protocol::Error> for Error {
    fn from(value: protocol::Error) -> Self {
        Self::Protocol(value)
    }
}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<partition::Error> for Error {
    fn from(value: partition::Error) -> Self {
        Self::Partition(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "produce storage I/O: {:?}", error.kind()),
            Self::Partition(error) => write!(f, "produce recovery: {error}"),
            _ => write!(f, "produce error: {self:?}"),
        }
    }
}
impl std::error::Error for Error {}

struct Slot {
    id: TopicId,
    index: i32,
    partition: partition::Partition,
}
fn open_partition(path: &Path, config: &Config) -> Result<partition::Partition, Error> {
    match config.segment_limits {
        Some(limits) => partition::Partition::open_segmented(
            path,
            0,
            config.journal_limits,
            config.record_limits,
            limits,
        )
        .map(|(partition, _)| partition)
        .map_err(Into::into),
        None => partition::Partition::open(path, 0, config.journal_limits, config.record_limits)
            .map(|(partition, _)| partition)
            .map_err(Into::into),
    }
}
pub(crate) struct Store {
    config: Config,
    slots: Vec<Slot>,
    changed: bool,
    retention: Option<retention::Config>,
    compaction: Option<compaction::Policy>,
    sweep_cursor: usize,
}
impl Store {
    pub(crate) fn open(config: Config, catalog: &Catalog) -> Result<Self, Error> {
        config.validate()?;
        match fs::create_dir(&config.directory) {
            Ok(()) => {
                let parent = config
                    .directory
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                File::open(parent)?.sync_all()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let info = fs::symlink_metadata(&config.directory)?;
        if !info.file_type().is_dir() {
            return Err(Error::InvalidStore);
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(config.max_stores)
            .map_err(|_| Error::ResourceLimit)?;
        for entry in fs::read_dir(&config.directory)? {
            if slots.len() >= config.max_stores {
                return Err(Error::ResourceLimit);
            }
            let entry = entry?;
            let kind = entry.file_type()?;
            if if config.segment_limits.is_some() {
                !kind.is_dir()
            } else {
                !kind.is_file()
            } {
                return Err(Error::InvalidStore);
            }
            let name = entry.file_name();
            let (id, index) = parse_storage_filename(
                name.to_str().ok_or(Error::InvalidStore)?,
                config.segment_limits.is_some(),
            )?;
            if slots
                .iter()
                .any(|slot: &Slot| slot.id == id && slot.index == index)
                || (!catalog.is_tombstoned(id) && catalog.by_id(id).is_none())
                || catalog
                    .by_id(id)
                    .is_some_and(|topic| index as u32 >= topic.partition_count())
            {
                return Err(Error::InvalidStore);
            }
            let partition = open_partition(&entry.path(), &config)?;
            slots.push(Slot {
                id,
                index,
                partition,
            });
        }
        Ok(Self {
            config,
            slots,
            changed: false,
            retention: None,
            compaction: None,
            sweep_cursor: 0,
        })
    }
    pub(crate) fn enable_retention(&mut self, config: retention::Config) -> Result<(), Error> {
        config.validate().map_err(|_| Error::InvalidConfig)?;
        if self.config.segment_limits.is_none() || self.retention.is_some() {
            return Err(Error::InvalidConfig);
        }
        self.retention = Some(config);
        Ok(())
    }
    pub(crate) fn enable_compaction(&mut self, policy: compaction::Policy) -> Result<(), Error> {
        self.config.validate_compaction(policy)?;
        if self.compaction.is_some() {
            return Err(Error::InvalidConfig);
        }
        self.compaction = Some(policy);
        Ok(())
    }
    pub(crate) fn compact_partition(
        &mut self,
        catalog: &Catalog,
        id: TopicId,
        index: i32,
        now_ms: i64,
    ) -> Result<compaction::Outcome, Error> {
        let policy = self.compaction.ok_or(Error::InvalidConfig)?;
        let topic = catalog.by_id(id).ok_or(Error::InvalidStore)?;
        if index < 0
            || index as u32 >= topic.partition_count()
            || matches!(
                topic.name(),
                "__consumer_offsets" | "__transaction_state" | "__share_group_state"
            )
        {
            return Err(Error::InvalidStore);
        }
        if now_ms < 0 {
            return Err(Error::InvalidConfig);
        }
        let Some(slot) = self
            .slots
            .iter_mut()
            .find(|slot| slot.id == id && slot.index == index)
        else {
            // Known empty partitions need no file or zero-length cleaner swap.
            return Ok(compaction::Outcome::default());
        };
        let watermark = slot.partition.next_offset();
        let guard =
            segments::DeletionGuard::new(watermark, watermark).map_err(|_| Error::InvalidConfig)?;
        let poisoned = slot.partition.is_poisoned();
        let result = slot.partition.compact(now_ms, policy, guard);
        self.changed |= result
            .as_ref()
            .is_ok_and(|outcome| outcome.rewritten_segments != 0)
            || poisoned != slot.partition.is_poisoned();
        result.map_err(Into::into)
    }
    fn append(&mut self, id: TopicId, index: i32, bytes: &[u8]) -> Result<partition::Append, i16> {
        let position = match self
            .slots
            .iter()
            .position(|slot| slot.id == id && slot.index == index)
        {
            Some(position) => position,
            None => {
                if self.slots.len() >= self.config.max_stores {
                    return Err(56);
                }
                let path = self.config.directory.join(storage_filename(
                    id,
                    index,
                    self.config.segment_limits.is_some(),
                ));
                let partition = open_partition(&path, &self.config).map_err(|_| 56i16)?;
                self.slots.push(Slot {
                    id,
                    index,
                    partition,
                });
                self.slots.len() - 1
            }
        };
        let partition = &mut self.slots[position].partition;
        let poisoned = partition.is_poisoned();
        let result = partition.append(bytes);
        self.changed |= result.is_ok() || poisoned != partition.is_poisoned();
        result.map_err(storage_error)
    }
    pub(crate) fn take_changed(&mut self) -> bool {
        std::mem::take(&mut self.changed)
    }
    pub(crate) fn max_partitions(&self) -> usize {
        self.config.max_partitions
    }
    pub(crate) fn record_limits(&self) -> records::Limits {
        self.config.record_limits
    }
    pub(crate) fn watermark(&self, id: TopicId, index: i32) -> Result<i64, i16> {
        match self
            .slots
            .iter()
            .find(|slot| slot.id == id && slot.index == index)
        {
            None => Ok(0),
            Some(slot) if slot.partition.is_poisoned() => Err(56),
            Some(slot) => Ok(slot.partition.next_offset()),
        }
    }
    pub(crate) fn log_start(&self, id: TopicId, index: i32) -> Result<i64, i16> {
        match self
            .slots
            .iter()
            .find(|slot| slot.id == id && slot.index == index)
        {
            None => Ok(0),
            Some(slot) if slot.partition.is_poisoned() => Err(56),
            Some(slot) => Ok(slot.partition.log_start_offset()),
        }
    }
    pub(crate) fn delete_records(
        &mut self,
        id: TopicId,
        index: i32,
        offset: i64,
    ) -> Result<i64, i16> {
        if self.retention.is_none() {
            return Err(43);
        }
        let watermark = self.watermark(id, index)?;
        let offset = if offset == -1 { watermark } else { offset };
        if offset < 0 || offset > watermark {
            return Err(1);
        }
        let Some(slot) = self
            .slots
            .iter_mut()
            .find(|slot| slot.id == id && slot.index == index)
        else {
            // A known empty partition needs no filesystem handle or manifest.
            return Ok(0);
        };
        let guard = segments::DeletionGuard::new(watermark, watermark).map_err(|_| 56i16)?;
        let before = slot.partition.log_start_offset();
        let poisoned = slot.partition.is_poisoned();
        let result = slot.partition.delete_records(offset, guard);
        self.changed |=
            slot.partition.log_start_offset() != before || slot.partition.is_poisoned() != poisoned;
        result
            .map(|outcome| outcome.log_start_offset)
            .map_err(|_| 56)
    }
    pub(crate) fn sweep_retention(
        &mut self,
        catalog: &Catalog,
        now_ms: i64,
        active: impl Fn() -> Result<(), retention::Error>,
    ) -> Result<retention::SweepReport, retention::Error> {
        let config = self.retention.ok_or(retention::Error::InvalidConfig)?;
        let policy = segments::RetentionPolicy::new(
            config.retention_ms,
            config.retention_bytes,
            config.max_segments_per_store,
        )
        .map_err(|_| retention::Error::InvalidConfig)?;
        let mut report = retention::SweepReport::default();
        let count = self.slots.len().min(config.max_sweep_stores);
        for _ in 0..count {
            active()?;
            let position = self.sweep_cursor % self.slots.len();
            self.sweep_cursor = (position + 1) % self.slots.len();
            let slot = &mut self.slots[position];
            report.stores_visited += 1;
            // Deleted identities and internal coordination logs keep their history.
            let Some(topic) = catalog.by_id(slot.id) else {
                continue;
            };
            if matches!(
                topic.name(),
                "__consumer_offsets" | "__transaction_state" | "__share_group_state"
            ) {
                continue;
            }
            let before = slot.partition.log_start_offset();
            let poisoned = slot.partition.is_poisoned();
            let watermark = slot.partition.next_offset();
            let guard = segments::DeletionGuard::new(watermark, watermark)
                .map_err(|_| retention::Error::InvalidConfig)?;
            let result = slot.partition.apply_retention(now_ms, policy, guard);
            self.changed |= slot.partition.log_start_offset() != before
                || slot.partition.is_poisoned() != poisoned;
            let outcome = result.map_err(retention::Error::Storage)?;
            report.stores_changed +=
                usize::from(outcome.log_start_offset != before || outcome.reclaimed_files != 0);
            report.reclaimed_files = report
                .reclaimed_files
                .checked_add(outcome.reclaimed_files)
                .ok_or(retention::Error::ResourceLimit)?;
            report.reclaimed_bytes = report
                .reclaimed_bytes
                .checked_add(outcome.reclaimed_bytes)
                .ok_or(retention::Error::ResourceLimit)?;
        }
        active()?;
        Ok(report)
    }
    pub(crate) fn read_entry(
        &mut self,
        id: TopicId,
        index: i32,
        offset: i64,
        remaining_bytes: usize,
        remaining_entries: usize,
    ) -> Result<(Option<journal::Entry>, segments::Work), partition::Error> {
        let Some(slot) = self
            .slots
            .iter_mut()
            .find(|slot| slot.id == id && slot.index == index)
        else {
            return Ok((None, segments::Work::default()));
        };
        let maximum = self
            .config
            .journal_limits
            .max_entry_bytes()
            .min(remaining_bytes)
            .checked_add(std::mem::size_of::<journal::Entry>())
            .ok_or(partition::Error::InvalidLimits)?;
        let poisoned = slot.partition.is_poisoned();
        let result =
            slot.partition
                .read_with_work(offset, maximum, remaining_bytes, remaining_entries);
        self.changed |= poisoned != slot.partition.is_poisoned();
        result
    }
    pub(crate) fn timestamp_start(
        &mut self,
        id: TopicId,
        index: i32,
        wanted: i64,
    ) -> Result<i64, partition::Error> {
        let Some(slot) = self
            .slots
            .iter_mut()
            .find(|slot| slot.id == id && slot.index == index)
        else {
            return Ok(0);
        };
        let poisoned = slot.partition.is_poisoned();
        let result = slot.partition.timestamp_start(wanted);
        self.changed |= poisoned != slot.partition.is_poisoned();
        result
    }
}
fn storage_filename(id: TopicId, index: i32, rolling: bool) -> String {
    let mut name = filename(id, index);
    if rolling {
        name.truncate(name.len() - ".journal".len());
        name.push_str(".segments");
    }
    name
}
fn parse_storage_filename(name: &str, rolling: bool) -> Result<(TopicId, i32), Error> {
    if !rolling {
        return parse_filename(name);
    }
    let base = name.strip_suffix(".segments").ok_or(Error::InvalidStore)?;
    let parsed = parse_filename(&format!("{base}.journal"))?;
    if storage_filename(parsed.0, parsed.1, true) != name {
        return Err(Error::InvalidStore);
    }
    Ok(parsed)
}
fn filename(id: TopicId, index: i32) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(51);
    for byte in id.bytes() {
        text.push(char::from(HEX[(byte >> 4) as usize]));
        text.push(char::from(HEX[(byte & 15) as usize]));
    }
    text.push('-');
    text.push_str(&index.to_string());
    text.push_str(".journal");
    text
}
fn parse_filename(name: &str) -> Result<(TopicId, i32), Error> {
    let base = name.strip_suffix(".journal").ok_or(Error::InvalidStore)?;
    if base.len() < 34 || base.as_bytes()[32] != b'-' {
        return Err(Error::InvalidStore);
    }
    let hex = base.as_bytes().get(..32).ok_or(Error::InvalidStore)?;
    let mut bytes = [0; 16];
    for (n, pair) in hex.chunks_exact(2).enumerate() {
        let nibble = |b| match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            _ => Err(Error::InvalidStore),
        };
        bytes[n] = nibble(pair[0])? * 16 + nibble(pair[1])?;
    }
    let id = TopicId::new(bytes).map_err(|_| Error::InvalidStore)?;
    let index = base
        .get(33..)
        .ok_or(Error::InvalidStore)?
        .parse::<i32>()
        .map_err(|_| Error::InvalidStore)?;
    if index < 0 || filename(id, index) != name {
        return Err(Error::InvalidStore);
    }
    Ok((id, index))
}

struct Reader<'a> {
    bytes: &'a [u8],
    count: usize,
    tags: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let result = self.bytes.get(..n).ok_or(protocol::Error::Truncated)?;
        self.bytes = &self.bytes[n..];
        Ok(result)
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
    fn varint(&mut self) -> Result<u32, Error> {
        let mut value = 0;
        for shift in [0, 7, 14, 21, 28] {
            let byte = self.take(1)?[0];
            if shift == 28 && byte > 15 {
                return Err(protocol::Error::InvalidVarint.into());
            }
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(protocol::Error::InvalidVarint.into())
    }
    fn length(&mut self, flex: bool, nullable: bool, string: bool) -> Result<Option<usize>, Error> {
        let length = if flex {
            i64::from(self.varint()?) - 1
        } else if string {
            i64::from(self.i16()?)
        } else {
            i64::from(self.i32()?)
        };
        if length == -1 && nullable {
            return Ok(None);
        }
        if length < 0 || (string && length > 32767) {
            return Err(protocol::Error::InvalidLength.into());
        }
        usize::try_from(length)
            .map(Some)
            .map_err(|_| protocol::Error::InvalidLength.into())
    }
    fn string(&mut self, flex: bool, nullable: bool) -> Result<Option<&'a str>, Error> {
        self.length(flex, nullable, true)?
            .map(|n| {
                std::str::from_utf8(self.take(n)?).map_err(|_| protocol::Error::InvalidUtf8.into())
            })
            .transpose()
    }
    fn array(&mut self, flex: bool, minimum: usize) -> Result<usize, Error> {
        let n = self
            .length(flex, false, false)?
            .ok_or(Error::RequestCount)?;
        if n > self.count || n > self.bytes.len() / minimum.max(1) {
            return Err(Error::RequestCount);
        }
        Ok(n)
    }
    fn tags(&mut self) -> Result<(), Error> {
        let count = self.varint()? as usize;
        if count > self.tags {
            return Err(protocol::Error::TooManyTags.into());
        }
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varint()?;
            if previous.is_some_and(|n| tag <= n) {
                return Err(protocol::Error::InvalidTagOrder.into());
            }
            previous = Some(tag);
            let length = self.varint()? as usize;
            self.take(length)?;
        }
        Ok(())
    }
}
fn reserved<T>(count: usize) -> Result<Vec<T>, Error> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(count)
        .map_err(|_| Error::ResourceLimit)?;
    Ok(result)
}
struct Topic<'a> {
    name: Option<&'a str>,
    id: [u8; 16],
    partitions: Vec<Part<'a>>,
}
struct Part<'a> {
    index: i32,
    bytes: Option<&'a [u8]>,
}
struct Request<'a> {
    tx: Option<&'a str>,
    acks: i16,
    timeout: i32,
    topics: Vec<Topic<'a>>,
}
fn parse<'a>(
    body: &'a [u8],
    flex: bool,
    version: i16,
    common: &metadata::Config,
    limits: &Config,
) -> Result<Request<'a>, Error> {
    let mut reader = Reader {
        bytes: body,
        count: common.max_topics.max(limits.max_partitions),
        tags: common.protocol_limits.max_tagged_fields(),
    };
    let tx = reader.string(flex, true)?;
    let acks = reader.i16()?;
    let timeout = reader.i32()?;
    let count = reader.array(flex, if version >= 13 { 17 } else { 3 })?;
    if count > common.max_topics {
        return Err(Error::RequestCount);
    }
    let mut topics = reserved(count)?;
    let mut total = 0usize;
    for _ in 0..count {
        let (name, id) = if version >= 13 {
            (
                None,
                reader
                    .take(16)?
                    .try_into()
                    .map_err(|_| protocol::Error::Truncated)?,
            )
        } else {
            (reader.string(flex, false)?, [0; 16])
        };
        let count = reader.array(flex, if flex { 6 } else { 8 })?;
        total = total.checked_add(count).ok_or(Error::RequestCount)?;
        if total > limits.max_partitions {
            return Err(Error::RequestCount);
        }
        let mut partitions = reserved(count)?;
        for _ in 0..count {
            let index = reader.i32()?;
            let bytes = reader
                .length(flex, true, false)?
                .map(|n| reader.take(n))
                .transpose()?;
            if flex {
                reader.tags()?;
            }
            partitions.push(Part { index, bytes });
        }
        if flex {
            reader.tags()?;
        }
        topics.push(Topic {
            name,
            id,
            partitions,
        });
    }
    if flex {
        reader.tags()?;
    }
    if !reader.bytes.is_empty() {
        return Err(protocol::Error::TrailingBytes.into());
    }
    Ok(Request {
        tx,
        acks,
        timeout,
        topics,
    })
}
struct ResultPart {
    index: i32,
    error: i16,
    base: i64,
    log_start: i64,
    bytes: Vec<u8>,
    id: Option<TopicId>,
}
struct ResultTopic<'a> {
    name: Option<&'a str>,
    id: [u8; 16],
    parts: Vec<ResultPart>,
}
fn record_error(error: records::Error) -> i16 {
    match error {
        records::Error::BudgetExceeded { .. } => 10,
        records::Error::Unsupported {
            feature: records::Unsupported::LegacyMagic(_),
            ..
        } => 87,
        records::Error::Unsupported {
            feature: records::Unsupported::Compression(_),
            ..
        } => 76,
        records::Error::Unsupported { .. } => 43,
        records::Error::Invalid {
            kind: records::Invalid::Checksum,
            ..
        } => 2,
        _ => 87,
    }
}
fn normalize(bytes: Option<&[u8]>, version: i16, config: &Config) -> Result<Vec<u8>, i16> {
    let input = bytes.filter(|b| !b.is_empty()).ok_or(87i16)?;
    if input.len() < 17 {
        return Err(87);
    }
    if input[16] != 2 {
        return Err(87);
    }
    let length = i32::from_be_bytes(input[8..12].try_into().map_err(|_| 87i16)?);
    if length < 49 {
        return Err(87);
    }
    let end = usize::try_from(length)
        .map_err(|_| 87i16)?
        .checked_add(12)
        .ok_or(87i16)?;
    if end != input.len() {
        return Err(87);
    }
    if input.len() > config.journal_limits.max_entry_bytes() {
        return Err(10);
    }
    let crc = u32::from_be_bytes(input[17..21].try_into().map_err(|_| 87i16)?);
    if crc32c::crc32c(&input[21..]) != crc {
        return Err(2);
    }
    let codec = input[22] & 7;
    if codec == 4 && version < 7 {
        return Err(76);
    }
    #[cfg(feature = "codecs")]
    let normalized = crate::codecs::normalize(input, config.codec_limits, config.record_limits)
        .map_err(|error| match error {
            crate::codecs::Error::Records(error) => record_error(error),
            crate::codecs::Error::BudgetExceeded { .. } => 10,
            _ => 2,
        })?;
    #[cfg(feature = "codecs")]
    let input = normalized.as_bytes();
    records::validate(input, config.record_limits).map_err(record_error)?;
    if input.len() > config.journal_limits.max_entry_bytes() {
        return Err(10);
    }
    let mut output = Vec::new();
    output.try_reserve_exact(input.len()).map_err(|_| 10i16)?;
    output.extend_from_slice(input);
    output[12..16].copy_from_slice(&0i32.to_be_bytes());
    Ok(output)
}
fn storage_error(error: partition::Error) -> i16 {
    match error {
        partition::Error::InputTooLarge => 10,
        partition::Error::Records(error) => record_error(error),
        _ => 56,
    }
}
fn timed_out(admitted: Instant, timeout: i32) -> bool {
    timeout <= 0 || admitted.elapsed() >= Duration::from_millis(timeout as u64)
}

pub(crate) fn process(
    catalog: &Catalog,
    store: &mut Store,
    input: &[u8],
    common: &metadata::Config,
    admitted: Instant,
    active: impl Fn() -> Result<(), Error>,
) -> Result<Option<Vec<u8>>, Error> {
    let version = i16::from_be_bytes(
        input
            .get(2..4)
            .ok_or(protocol::Error::Truncated)?
            .try_into()
            .map_err(|_| protocol::Error::Truncated)?,
    );
    if !(3..=13).contains(&version) {
        return Err(protocol::Error::UnsupportedHeaderVersion.into());
    }
    let flex = version >= 9;
    let (header, body) =
        protocol::RequestHeader::parse(input, if flex { 2 } else { 1 }, common.protocol_limits)?;
    let parsed = parse(body, flex, version, common, &store.config)?;
    let count = parsed
        .topics
        .iter()
        .map(|t| t.partitions.len())
        .sum::<usize>();
    let response_bound = parsed
        .topics
        .iter()
        .try_fold(16usize, |n, t| {
            n.checked_add(t.name.map_or(16, str::len) + 16)
                .ok_or(Error::ResourceLimit)
        })?
        .checked_add(count.checked_mul(40).ok_or(Error::ResourceLimit)?)
        .ok_or(Error::ResourceLimit)?;
    if response_bound > common.max_response_bytes {
        return Err(Error::ResourceLimit);
    }
    let mut writer = Writer::new(response_bound)?;
    let mut results = reserved(parsed.topics.len())?;
    let mut normalized = 0usize;
    for (topic_position, topic) in parsed.topics.iter().enumerate() {
        active()?;
        let target = if version >= 13 {
            TopicId::new(topic.id).ok().and_then(|id| catalog.by_id(id))
        } else {
            topic.name.and_then(|name| catalog.by_name(name))
        };
        let same_topic = |other: &Topic<'_>| {
            if version >= 13 {
                topic.id == other.id
            } else {
                topic.name == other.name
            }
        };
        if parsed.topics[..topic_position].iter().any(same_topic) {
            continue;
        }
        let duplicate_topic = parsed.topics[topic_position + 1..].iter().any(same_topic);
        let mut parts = reserved(topic.partitions.len())?;
        for (position, part) in topic.partitions.iter().enumerate() {
            if topic.partitions[..position]
                .iter()
                .any(|p| p.index == part.index)
            {
                continue;
            }
            let mut error = if !matches!(parsed.acks, -1..=1) {
                21
            } else if parsed.timeout < 0 {
                42
            } else if parsed.tx.is_some() {
                43
            } else if duplicate_topic
                || topic.partitions[position + 1..]
                    .iter()
                    .any(|p| p.index == part.index)
            {
                42
            } else if target.is_none() {
                if version >= 13 {
                    100
                } else {
                    3
                }
            } else if part.index < 0
                || target.is_some_and(|t| part.index as u32 >= t.partition_count())
            {
                3
            } else {
                0
            };
            let mut bytes = Vec::new();
            if error == 0 {
                match normalize(part.bytes, version, &store.config) {
                    Ok(value) => {
                        normalized = normalized
                            .checked_add(value.len())
                            .ok_or(Error::ResourceLimit)?;
                        if normalized > store.config.max_normalized_bytes {
                            return Err(Error::ResourceLimit);
                        }
                        bytes = value;
                    }
                    Err(code) => error = code,
                }
            }
            parts.push(ResultPart {
                index: part.index,
                error,
                base: -1,
                log_start: -1,
                bytes,
                id: target.map(|t| t.id()),
            });
        }
        results.push(ResultTopic {
            name: topic.name,
            id: topic.id,
            parts,
        });
    }
    // Complete structural/record/response preflight precedes every storage mutation.
    for topic in &mut results {
        for part in &mut topic.parts {
            active()?;
            if part.error == 0 {
                if timed_out(admitted, parsed.timeout) {
                    part.error = 7;
                    continue;
                }
                if let Some(id) = part.id {
                    match store.append(id, part.index, &part.bytes) {
                        Ok(result) => {
                            part.base = result.base_offset;
                            match store.log_start(id, part.index) {
                                Ok(log_start) => part.log_start = log_start,
                                Err(error) => {
                                    part.error = error;
                                    part.base = -1;
                                }
                            }
                            if timed_out(admitted, parsed.timeout) {
                                part.error = 7;
                                part.base = -1;
                            }
                        }
                        Err(error) => part.error = error,
                    }
                } else {
                    part.error = 3;
                }
            }
        }
    }
    if parsed.acks == 0 {
        if results.iter().any(|t| t.parts.iter().any(|p| p.error != 0)) {
            return Err(Error::AcksZeroError);
        }
        return Ok(None);
    }
    writer.i32(header.correlation_id)?;
    if flex {
        writer.byte(0)?;
    }
    writer.count(results.len(), flex)?;
    for topic in results {
        if version >= 13 {
            writer.put(&topic.id)?;
        } else {
            writer.string(topic.name, flex)?;
        }
        writer.count(topic.parts.len(), flex)?;
        for part in topic.parts {
            writer.i32(part.index)?;
            writer.i16(part.error)?;
            writer.i64(part.base)?;
            writer.i64(-1)?;
            if version >= 5 {
                writer.i64(if part.error == 0 { part.log_start } else { -1 })?;
            }
            if version >= 8 {
                writer.count(0, flex)?;
                writer.string(None, flex)?;
            }
            if flex {
                writer.byte(0)?;
            }
        }
        if flex {
            writer.byte(0)?;
        }
    }
    writer.i32(0)?;
    if flex {
        writer.byte(0)?;
    }
    Ok(Some(writer.bytes))
}
struct Writer {
    bytes: Vec<u8>,
    cap: usize,
}
impl Writer {
    fn new(cap: usize) -> Result<Self, Error> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(cap)
            .map_err(|_| Error::ResourceLimit)?;
        Ok(Self { bytes, cap })
    }
    fn put(&mut self, v: &[u8]) -> Result<(), Error> {
        if v.len() > self.cap.saturating_sub(self.bytes.len()) {
            return Err(Error::ResourceLimit);
        }
        self.bytes.extend_from_slice(v);
        Ok(())
    }
    fn byte(&mut self, v: u8) -> Result<(), Error> {
        self.put(&[v])
    }
    fn i16(&mut self, v: i16) -> Result<(), Error> {
        self.put(&v.to_be_bytes())
    }
    fn i32(&mut self, v: i32) -> Result<(), Error> {
        self.put(&v.to_be_bytes())
    }
    fn i64(&mut self, v: i64) -> Result<(), Error> {
        self.put(&v.to_be_bytes())
    }
    fn varint(&mut self, mut v: u32) -> Result<(), Error> {
        while v >= 128 {
            self.byte(v as u8 | 128)?;
            v >>= 7;
        }
        self.byte(v as u8)
    }
    fn count(&mut self, n: usize, flex: bool) -> Result<(), Error> {
        if flex {
            self.varint(
                u32::try_from(n)
                    .map_err(|_| Error::ResourceLimit)?
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit)?,
            )
        } else {
            self.i32(i32::try_from(n).map_err(|_| Error::ResourceLimit)?)
        }
    }
    fn string(&mut self, value: Option<&str>, flex: bool) -> Result<(), Error> {
        if let Some(v) = value {
            if flex {
                self.varint((v.len() + 1) as u32)?;
            } else {
                self.i16(v.len() as i16)?;
            }
            self.put(v.as_bytes())
        } else if flex {
            self.byte(0)
        } else {
            self.i16(-1)
        }
    }
}
