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
    borrow::Cow,
    collections::HashMap,
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
    /// [`Store::read_epoch`] last used to drop this log's cached file length.
    seen_epoch: u64,
}

/// An open partition log, resolved once and reused for each record read.
#[derive(Clone, Copy)]
pub(crate) struct OpenLog {
    position: usize,
    id: TopicId,
    index: i32,
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
    /// Open log position in `slots`, keyed by topic identity and partition.
    located: HashMap<(TopicId, i32), usize>,
    changed: bool,
    retention: Option<retention::Config>,
    compaction: Option<compaction::Policy>,
    sweep_cursor: usize,
    /// Bumped once per fetch request. A log rechecks its file length the first
    /// time it is read in that request, then reuses the check for later entries.
    read_epoch: u64,
    /// Response bytes for the job currently on this actor. The connection
    /// refills the buffer after the reply is written.
    response_buf: Vec<u8>,
    /// Payload bytes for a ListOffsets timestamp scan. Fetch reads into the
    /// response instead. The buffer stays for the next scan.
    scan_buf: Vec<u8>,
    /// Staged grouped produces. Cleared between batches; the slots stay.
    queued_captured: Vec<Option<CapturedProduce>>,
    /// Replies for the grouped batch currently being finished.
    queued_replies: Vec<Result<Option<Vec<u8>>, Error>>,
    /// One sync representative per partition in the grouped batch.
    sync_reps: Vec<(usize, usize)>,
    /// Partition identity to the representative slot in `sync_reps`.
    sync_slots: HashMap<(TopicId, i32), usize>,
    /// Join handles for the grouped partition sync. Cleared between batches.
    sync_joins: Vec<Option<Option<std::thread::JoinHandle<std::io::Result<()>>>>>,
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
        let mut located = HashMap::new();
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
            if located.contains_key(&(id, index))
                || (!catalog.is_tombstoned(id) && catalog.by_id(id).is_none())
                || catalog
                    .by_id(id)
                    .is_some_and(|topic| index as u32 >= topic.partition_count())
            {
                return Err(Error::InvalidStore);
            }
            let partition = open_partition(&entry.path(), &config)?;
            let _ = located.insert((id, index), slots.len());
            slots.push(Slot {
                id,
                index,
                partition,
                seen_epoch: 0,
            });
        }
        Ok(Self {
            config,
            slots,
            located,
            changed: false,
            retention: None,
            compaction: None,
            sweep_cursor: 0,
            read_epoch: 0,
            response_buf: Vec::new(),
            scan_buf: Vec::new(),
            queued_captured: Vec::new(),
            queued_replies: Vec::new(),
            sync_reps: Vec::new(),
            sync_slots: HashMap::new(),
            sync_joins: Vec::new(),
        })
    }
    pub(crate) fn prepare_response_buf(&mut self, buf: Vec<u8>) {
        if buf.capacity() >= self.response_buf.capacity() {
            self.response_buf = buf;
        }
    }
    pub(crate) fn take_response_buf(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.response_buf)
    }
    pub(crate) fn take_scan_buf(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.scan_buf)
    }
    pub(crate) fn keep_scan_buf(&mut self, buf: Vec<u8>) {
        if buf.capacity() > self.scan_buf.capacity() {
            self.scan_buf = buf;
        }
    }
    fn keep_response_buf(&mut self, buf: Vec<u8>) {
        if buf.capacity() > self.response_buf.capacity() {
            self.response_buf = buf;
        }
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
        let Some(slot) = self.located_slot_mut(id, index) else {
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
    fn stage_in_place(
        &mut self,
        id: TopicId,
        index: i32,
        bytes: &mut [u8],
    ) -> Result<(partition::Stage, Option<i64>, usize), i16> {
        self.stage_in_place_inner(id, index, bytes, false)
    }
    fn stage_in_place_grouped(
        &mut self,
        id: TopicId,
        index: i32,
        bytes: &mut [u8],
    ) -> Result<(partition::Stage, Option<i64>, usize), i16> {
        self.stage_in_place_inner(id, index, bytes, true)
    }
    fn stage_in_place_inner(
        &mut self,
        id: TopicId,
        index: i32,
        bytes: &mut [u8],
        grouped: bool,
    ) -> Result<(partition::Stage, Option<i64>, usize), i16> {
        let position = self.slot(id, index)?;
        let partition = &mut self.slots[position].partition;
        let poisoned = partition.is_poisoned();
        let result = if grouped {
            partition.stage_in_place_grouped(bytes)
        } else {
            partition.stage_in_place(bytes)
        };
        self.changed |= result.is_ok() || poisoned != partition.is_poisoned();
        let staged = result.map_err(storage_error)?;
        let log_start = (!partition.is_poisoned()).then(|| partition.log_start_offset());
        Ok((staged, log_start, position))
    }
    fn staged_position(&mut self, position: usize, id: TopicId, index: i32) -> Result<usize, i16> {
        if self
            .slots
            .get(position)
            .is_some_and(|slot| slot.id == id && slot.index == index)
        {
            Ok(position)
        } else {
            self.slot(id, index)
        }
    }
    fn commit_staged(
        &mut self,
        position: usize,
        id: TopicId,
        index: i32,
        batch_count: usize,
    ) -> Result<partition::Append, i16> {
        let position = self.staged_position(position, id, index)?;
        let Some(slot) = self.slots.get_mut(position) else {
            return Err(56);
        };
        let result = slot.partition.commit_staged(batch_count);
        self.changed = true;
        result.map_err(storage_error)
    }
    fn unstage_last(&mut self, position: usize, id: TopicId, index: i32) -> Result<(), i16> {
        let position = self.staged_position(position, id, index)?;
        let Some(slot) = self.slots.get_mut(position) else {
            return Err(56);
        };
        let poisoned = slot.partition.is_poisoned();
        let result = slot.partition.unstage_last();
        self.changed |= result.is_err() || poisoned != slot.partition.is_poisoned();
        result.map_err(storage_error)
    }
    fn abort_staged(&mut self, position: usize, id: TopicId, index: i32) {
        let position = if self
            .slots
            .get(position)
            .is_some_and(|slot| slot.id == id && slot.index == index)
        {
            Some(position)
        } else {
            self.located.get(&(id, index)).copied()
        };
        let Some(position) = position else {
            return;
        };
        if let Some(slot) = self.slots.get_mut(position) {
            slot.partition.abort_staged();
            self.changed = true;
        }
    }
    fn located_slot_mut(&mut self, id: TopicId, index: i32) -> Option<&mut Slot> {
        let position = self.located.get(&(id, index)).copied()?;
        let slot = self.slots.get_mut(position)?;
        let matches = slot.id == id && slot.index == index;
        matches.then_some(slot)
    }
    fn slot(&mut self, id: TopicId, index: i32) -> Result<usize, i16> {
        if let Some(position) = self.located.get(&(id, index)).copied() {
            return Ok(position);
        }
        if self.slots.len() >= self.config.max_stores {
            return Err(56);
        }
        let path = self.config.directory.join(storage_filename(
            id,
            index,
            self.config.segment_limits.is_some(),
        ));
        let partition = open_partition(&path, &self.config).map_err(|_| 56i16)?;
        let position = self.slots.len();
        self.slots.push(Slot {
            id,
            index,
            partition,
            seen_epoch: 0,
        });
        let _ = self.located.insert((id, index), position);
        Ok(position)
    }
    pub(crate) fn take_changed(&mut self) -> bool {
        std::mem::take(&mut self.changed)
    }
    pub(crate) fn max_partitions(&self) -> usize {
        self.config.max_partitions
    }
    /// Log start, high watermark, and the open handle from one lookup.
    ///
    /// A missing partition is an empty log. The handle is present only when
    /// that slot still names this partition. A poisoned partition is error 56.
    pub(crate) fn open_bounds(
        &self,
        id: TopicId,
        index: i32,
    ) -> Result<(Option<OpenLog>, i64, i64), i16> {
        let Some(position) = self.located.get(&(id, index)).copied() else {
            return Ok((None, 0, 0));
        };
        let Some(slot) = self.slots.get(position) else {
            return Ok((None, 0, 0));
        };
        if slot.partition.is_poisoned() {
            return Err(56);
        }
        let log = (slot.id == id && slot.index == index).then_some(OpenLog {
            position,
            id,
            index,
        });
        Ok((
            log,
            slot.partition.log_start_offset(),
            slot.partition.next_offset(),
        ))
    }
    /// Log start and high watermark from one open-partition lookup.
    pub(crate) fn log_bounds(&self, id: TopicId, index: i32) -> Result<(i64, i64), i16> {
        self.open_bounds(id, index)
            .map(|(_, log_start, watermark)| (log_start, watermark))
    }
    pub(crate) fn watermark(&self, id: TopicId, index: i32) -> Result<i64, i16> {
        self.log_bounds(id, index).map(|(_, watermark)| watermark)
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
        let Some(slot) = self.located_slot_mut(id, index) else {
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
    /// Start a fetch request. Only logs read after this drop their cached
    /// file length, so an unrelated partition is not touched.
    pub(crate) fn begin_read(&mut self) {
        self.read_epoch = self.read_epoch.wrapping_add(1);
    }
    pub(crate) fn read_entry_at(
        &mut self,
        log: Option<OpenLog>,
        offset: i64,
        remaining_bytes: usize,
        remaining_entries: usize,
        dest: &mut Vec<u8>,
    ) -> Result<(Option<journal::PayloadView>, segments::Work), partition::Error> {
        let Some(log) = log else {
            return Ok((None, segments::Work::default()));
        };
        let epoch = self.read_epoch;
        let max_entry = self.config.journal_limits.max_entry_bytes();
        let direct = if let Some(slot) = self.slots.get_mut(log.position) {
            if slot.id == log.id && slot.index == log.index {
                Some(observe_cached_read(
                    slot,
                    epoch,
                    max_entry,
                    offset,
                    remaining_bytes,
                    remaining_entries,
                    dest,
                )?)
            } else {
                None
            }
        } else {
            None
        };
        if let Some((result, changed)) = direct {
            self.changed |= changed;
            return Ok(result);
        }
        let Some(slot) = self.located_slot_mut(log.id, log.index) else {
            return Ok((None, segments::Work::default()));
        };
        let (result, changed) = observe_cached_read(
            slot,
            epoch,
            max_entry,
            offset,
            remaining_bytes,
            remaining_entries,
            dest,
        )?;
        self.changed |= changed;
        Ok(result)
    }
    pub(crate) fn timestamp_start(
        &mut self,
        log: Option<OpenLog>,
        wanted: i64,
    ) -> Result<i64, partition::Error> {
        let Some(log) = log else {
            return Ok(0);
        };
        let Some(slot) = self.slots.get_mut(log.position) else {
            return Ok(0);
        };
        if slot.id != log.id || slot.index != log.index {
            return Ok(0);
        }
        let poisoned = slot.partition.is_poisoned();
        let result = slot.partition.timestamp_start(wanted);
        self.changed |= poisoned != slot.partition.is_poisoned();
        result
    }
}
fn observe_cached_read(
    slot: &mut Slot,
    epoch: u64,
    max_entry: usize,
    offset: i64,
    remaining_bytes: usize,
    remaining_entries: usize,
    dest: &mut Vec<u8>,
) -> Result<((Option<journal::PayloadView>, segments::Work), bool), partition::Error> {
    if slot.seen_epoch != epoch {
        slot.partition.invalidate_length_observation();
        slot.seen_epoch = epoch;
    }
    let maximum = max_entry
        .min(remaining_bytes)
        .checked_add(std::mem::size_of::<journal::Entry>())
        .ok_or(partition::Error::InvalidLimits)?;
    let poisoned = slot.partition.is_poisoned();
    let result =
        slot.partition
            .read_into(offset, maximum, remaining_bytes, remaining_entries, dest)?;
    let changed = poisoned != slot.partition.is_poisoned();
    Ok((result, changed))
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
    for (n, pair) in hex.as_chunks::<2>().0.iter().enumerate() {
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
    pos: usize,
    count: usize,
    tags: usize,
}
impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.pos.checked_add(n).ok_or(protocol::Error::Truncated)?;
        let result = self
            .bytes
            .get(self.pos..end)
            .ok_or(protocol::Error::Truncated)?;
        self.pos = end;
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
    fn string_range(
        &mut self,
        flex: bool,
        nullable: bool,
    ) -> Result<Option<(usize, usize)>, Error> {
        self.length(flex, nullable, true)?
            .map(|n| {
                let start = self.pos;
                let bytes = self.take(n)?;
                if std::str::from_utf8(bytes).is_err() {
                    return Err(protocol::Error::InvalidUtf8.into());
                }
                Ok((start, self.pos))
            })
            .transpose()
    }
    fn array(&mut self, flex: bool, minimum: usize) -> Result<usize, Error> {
        let n = self
            .length(flex, false, false)?
            .ok_or(Error::RequestCount)?;
        if n > self.count || n > self.remaining() / minimum.max(1) {
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
struct Topic {
    name: Option<(usize, usize)>,
    id: [u8; 16],
    partitions: List<Part>,
}

/// One produce slot stays inline. Several slots own a vector.
enum List<T> {
    Empty,
    One(T),
    Many(Vec<T>),
}

impl<T> List<T> {
    fn as_slice(&self) -> &[T] {
        match self {
            Self::Empty => &[],
            Self::One(item) => std::slice::from_ref(item),
            Self::Many(items) => items,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        match self {
            Self::Empty => &mut [],
            Self::One(item) => std::slice::from_mut(item),
            Self::Many(items) => items,
        }
    }

    fn push(&mut self, item: T) {
        match self {
            Self::Many(items) => items.push(item),
            Self::Empty => *self = Self::One(item),
            Self::One(_) => {
                let Self::One(first) = std::mem::replace(self, Self::Empty) else {
                    return;
                };
                *self = Self::Many(vec![first, item]);
            }
        }
    }

    fn pop(&mut self) -> Option<T> {
        match self {
            Self::Empty => None,
            Self::Many(items) => items.pop(),
            Self::One(_) => match std::mem::replace(self, Self::Empty) {
                Self::One(item) => Some(item),
                _ => None,
            },
        }
    }

    #[cfg(test)]
    fn from_vec(items: Vec<T>) -> Self {
        match items.len() {
            0 => Self::Empty,
            1 => match items.into_iter().next() {
                Some(item) => Self::One(item),
                None => Self::Empty,
            },
            _ => Self::Many(items),
        }
    }
}

impl<T> std::ops::Deref for List<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> std::ops::DerefMut for List<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<'a, T> IntoIterator for &'a List<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl<'a, T> IntoIterator for &'a mut List<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_mut_slice().iter_mut()
    }
}

impl<T> IntoIterator for List<T> {
    type Item = T;
    type IntoIter = ListIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Empty => ListIntoIter {
                one: None,
                many: Vec::new().into_iter(),
            },
            Self::One(item) => ListIntoIter {
                one: Some(item),
                many: Vec::new().into_iter(),
            },
            Self::Many(items) => ListIntoIter {
                one: None,
                many: items.into_iter(),
            },
        }
    }
}

struct ListIntoIter<T> {
    one: Option<T>,
    many: std::vec::IntoIter<T>,
}

impl<T> Iterator for ListIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.one.take() {
            return Some(item);
        }
        self.many.next()
    }
}

fn exact_list<T>(count: usize) -> Result<List<T>, Error> {
    if count <= 1 {
        Ok(List::Empty)
    } else {
        Ok(List::Many(reserved(count)?))
    }
}
struct Part {
    index: i32,
    bytes: Option<(usize, usize)>,
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum ProduceTopicKey<'a> {
    Id([u8; 16]),
    Name(Option<&'a [u8]>),
}

fn short_partition_copies(parts: &[Part]) -> ([bool; 16], [bool; 16]) {
    let mut first = [false; 16];
    let mut duplicated = [false; 16];
    let mut indexes = [0i32; 16];
    let mut filled = [false; 16];
    for (index, part) in parts.iter().enumerate().take(16) {
        let mut matched = None;
        for prior in 0..index {
            let same = filled.get(prior).copied().unwrap_or(false)
                && indexes.get(prior).copied() == Some(part.index);
            if same {
                matched = Some(prior);
                break;
            }
        }
        if let Some(earlier) = matched {
            if let Some(flag) = duplicated.get_mut(earlier) {
                *flag = true;
            }
            if let Some(flag) = duplicated.get_mut(index) {
                *flag = true;
            }
        } else if let Some(flag) = first.get_mut(index) {
            *flag = true;
        }
        if let Some(slot) = indexes.get_mut(index) {
            *slot = part.index;
        }
        if let Some(flag) = filled.get_mut(index) {
            *flag = true;
        }
    }
    (first, duplicated)
}

fn strictly_increasing_indexes(parts: &[Part]) -> bool {
    let mut previous = None;
    for part in parts {
        if previous.is_some_and(|earlier| part.index <= earlier) {
            return false;
        }
        previous = Some(part.index);
    }
    true
}

fn short_topic_copies<'a>(
    topics: &'a [Topic],
    version: i16,
    body: &'a [u8],
) -> ([bool; 16], [bool; 16]) {
    let mut first = [false; 16];
    let mut duplicated = [false; 16];
    let mut keys: [Option<ProduceTopicKey<'a>>; 16] = std::array::from_fn(|_| None);
    for (index, topic) in topics.iter().enumerate().take(16) {
        let key = produce_topic_key(version, topic, body);
        let mut matched = None;
        for prior in 0..index {
            let same = keys
                .get(prior)
                .and_then(Option::as_ref)
                .is_some_and(|prior_key| prior_key == &key);
            if same {
                matched = Some(prior);
                break;
            }
        }
        if let Some(earlier) = matched {
            if let Some(flag) = duplicated.get_mut(earlier) {
                *flag = true;
            }
            if let Some(flag) = duplicated.get_mut(index) {
                *flag = true;
            }
        } else if let Some(flag) = first.get_mut(index) {
            *flag = true;
        }
        if let Some(slot) = keys.get_mut(index) {
            *slot = Some(key);
        }
    }
    (first, duplicated)
}

fn produce_topic_key<'a>(version: i16, topic: &Topic, body: &'a [u8]) -> ProduceTopicKey<'a> {
    if version >= 13 {
        ProduceTopicKey::Id(topic.id)
    } else {
        // A truncated name is the same absent key as a missing one. The bytes
        // stay in the request so equal names compare without a copy.
        ProduceTopicKey::Name(ranged(body, topic.name).ok().flatten())
    }
}
struct Request {
    tx: Option<(usize, usize)>,
    acks: i16,
    timeout: i32,
    topics: List<Topic>,
}
fn parse(
    body: &[u8],
    flex: bool,
    version: i16,
    common: &metadata::Config,
    limits: &Config,
) -> Result<Request, Error> {
    let mut reader = Reader {
        bytes: body,
        pos: 0,
        count: common.max_topics.max(limits.max_partitions),
        tags: common.protocol_limits.max_tagged_fields(),
    };
    let tx = reader.string_range(flex, true)?;
    let acks = reader.i16()?;
    let timeout = reader.i32()?;
    let count = reader.array(flex, if version >= 13 { 17 } else { 3 })?;
    if count > common.max_topics {
        return Err(Error::RequestCount);
    }
    let mut topics = exact_list(count)?;
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
            (reader.string_range(flex, false)?, [0; 16])
        };
        let count = reader.array(flex, if flex { 6 } else { 8 })?;
        total = total.checked_add(count).ok_or(Error::RequestCount)?;
        if total > limits.max_partitions {
            return Err(Error::RequestCount);
        }
        let mut partitions = exact_list(count)?;
        for _ in 0..count {
            let index = reader.i32()?;
            let bytes = match reader.length(flex, true, false)? {
                None => None,
                Some(n) => {
                    let start = reader.pos;
                    reader.take(n)?;
                    Some((start, reader.pos))
                }
            };
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
    if reader.remaining() != 0 {
        return Err(protocol::Error::TrailingBytes.into());
    }
    Ok(Request {
        tx,
        acks,
        timeout,
        topics,
    })
}
enum Held {
    /// Body-relative span of the request buffer.
    Shared((usize, usize)),
    Owned(Vec<u8>),
}
struct ResultPart {
    index: i32,
    error: i16,
    base: i64,
    log_start: i64,
    bytes: Held,
    id: Option<TopicId>,
}
struct ResultTopic {
    name: Option<(usize, usize)>,
    id: [u8; 16],
    parts: List<ResultPart>,
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
fn normalize<'a>(
    bytes: Option<&'a [u8]>,
    version: i16,
    config: &Config,
) -> Result<Cow<'a, [u8]>, i16> {
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
    let codec = input[22] & 7;
    if codec == 4 && version < 7 {
        return Err(76);
    }
    // Codec normalization matches this single batch's CRC32-C before it
    // rewrites a compressed payload. The no-codec path matches it here.
    #[cfg(feature = "codecs")]
    {
        let normalized = crate::codecs::normalize(input, config.codec_limits, config.record_limits)
            .map_err(|error| match error {
                crate::codecs::Error::Records(error) => record_error(error),
                crate::codecs::Error::BudgetExceeded { .. } => 10,
                _ => 2,
            })?;
        // Uncompressed batches are already in the request. `process` stamped
        // leader epoch 0 there, so a borrowed admission does not need a copy.
        match normalized.into_bytes() {
            Cow::Borrowed(_) => {
                if input.len() > config.journal_limits.max_entry_bytes() {
                    return Err(10);
                }
                if input.get(12..16) == Some(&[0, 0, 0, 0]) {
                    return Ok(Cow::Borrowed(input));
                }
                owned_epoch(input, config)
            }
            Cow::Owned(mut buf) => {
                if buf.len() > config.journal_limits.max_entry_bytes() {
                    return Err(10);
                }
                if buf.len() >= 16 {
                    buf[12..16].copy_from_slice(&0i32.to_be_bytes());
                }
                Ok(Cow::Owned(buf))
            }
        }
    }
    #[cfg(not(feature = "codecs"))]
    {
        let crc = u32::from_be_bytes(input[17..21].try_into().map_err(|_| 87i16)?);
        if crc32c::crc32c(&input[21..]) != crc {
            return Err(2);
        }
        records::validate_known_crc(input, config.record_limits).map_err(record_error)?;
        if input.len() > config.journal_limits.max_entry_bytes() {
            return Err(10);
        }
        // `process` stamps leader epoch 0 before this check. Other callers
        // still borrow when the bytes are already zero.
        if input.get(12..16) == Some(&[0, 0, 0, 0]) {
            return Ok(Cow::Borrowed(input));
        }
        owned_epoch(input, config)
    }
}

fn owned_epoch(input: &[u8], config: &Config) -> Result<Cow<'static, [u8]>, i16> {
    if input.len() > config.journal_limits.max_entry_bytes() {
        return Err(10);
    }
    let mut output = Vec::new();
    output.try_reserve_exact(input.len()).map_err(|_| 10i16)?;
    output.extend_from_slice(input);
    output[12..16].copy_from_slice(&0i32.to_be_bytes());
    Ok(Cow::Owned(output))
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

fn ranged(buf: &[u8], range: Option<(usize, usize)>) -> Result<Option<&[u8]>, Error> {
    let Some((start, end)) = range else {
        return Ok(None);
    };
    buf.get(start..end)
        .map(Some)
        .ok_or_else(|| protocol::Error::Truncated.into())
}
fn ranged_str(buf: &[u8], range: Option<(usize, usize)>) -> Result<Option<&str>, Error> {
    ranged(buf, range)?
        .map(|bytes| std::str::from_utf8(bytes).map_err(|_| protocol::Error::InvalidUtf8.into()))
        .transpose()
}
/// Leader epoch is outside the batch CRC. The single-node log stores epoch 0,
/// so write that into the request buffer instead of copying the payload.
struct PendingSync {
    topic_at: usize,
    part_at: usize,
    id: TopicId,
    index: i32,
    file: std::fs::File,
    batch_count: usize,
    failed: bool,
    /// Log start read while the partition was staged. `None` means the log
    /// was poisoned and the append must not be acknowledged.
    log_start: Option<i64>,
    /// Open-log slot captured with the staged append.
    position: usize,
}
fn note_append(part: &mut ResultPart, result: partition::Append, log_start: Option<i64>) {
    // The request timer is checked before mutation. A finished single-node
    // append already met acks, so hiding its offset would make the producer
    // retry a durable batch.
    match log_start {
        Some(log_start) => {
            part.base = result.base_offset;
            part.log_start = log_start;
        }
        None => {
            part.error = 56;
            part.base = -1;
        }
    }
}
fn reserve_sync_joins<T>(joins: &mut Vec<T>, count: usize) {
    if joins.capacity() < count {
        joins.reserve(count - joins.capacity());
    }
}

fn sync_staged(items: &mut [PendingSync]) {
    if items.len() <= 1 {
        if let Some(item) = items.first_mut() {
            item.failed = item.file.sync_data().is_err();
        }
        return;
    }
    let mut joins = Vec::new();
    reserve_sync_joins(&mut joins, items.len());
    for item in items.iter_mut() {
        match item.file.try_clone() {
            Ok(file) => joins.push(Some(std::thread::spawn(move || file.sync_data()))),
            Err(_) => {
                item.failed = true;
                joins.push(None);
            }
        }
    }
    for (item, join) in items.iter_mut().zip(joins) {
        if item.failed {
            continue;
        }
        item.failed = join
            .and_then(|handle| handle.join().ok())
            .is_none_or(|result| result.is_err());
    }
}
fn stamp_leader_epoch(batch: &mut [u8]) {
    if batch.len() >= 17 && batch[16] == 2 {
        batch[12..16].copy_from_slice(&0i32.to_be_bytes());
    }
}
struct CapturedProduce {
    version: i16,
    correlation_id: i32,
    body_off: usize,
    acks: i16,
    response_bound: usize,
    results: List<ResultTopic>,
    pending_sync: List<PendingSync>,
}
enum ProcessOut {
    Reply(Option<Vec<u8>>),
    Captured(CapturedProduce),
}

pub(crate) fn process(
    catalog: &Catalog,
    store: &mut Store,
    input: &mut [u8],
    common: &metadata::Config,
    admitted: Instant,
    active: impl Fn() -> Result<(), Error>,
) -> Result<Option<Vec<u8>>, Error> {
    match process_inner(catalog, store, input, common, admitted, active, false)? {
        ProcessOut::Reply(body) => Ok(body),
        ProcessOut::Captured(_) => Err(Error::Canceled),
    }
}

/// Stage every already-queued produce, synchronize each partition once, then
/// reply. A single job uses [`process`]. The first job is not delayed when
/// nothing else is queued; callers decide that.
pub(crate) fn process_queued(
    catalog: &Catalog,
    store: &mut Store,
    inputs: &mut [Vec<u8>],
    admitted: &[Instant],
    common: &metadata::Config,
    spares: &mut [Vec<u8>],
    mut active: impl FnMut(usize) -> Result<(), Error>,
) -> Vec<Result<Option<Vec<u8>>, Error>> {
    let mut captured = std::mem::take(&mut store.queued_captured);
    captured.clear();
    captured.resize_with(inputs.len(), || None);
    let mut replies = std::mem::take(&mut store.queued_replies);
    replies.clear();
    for (index, input) in inputs.iter_mut().enumerate() {
        let admitted_at = admitted.get(index).copied().unwrap_or_else(Instant::now);
        match process_inner(
            catalog,
            store,
            input,
            common,
            admitted_at,
            || active(index),
            true,
        ) {
            Ok(ProcessOut::Captured(item)) => {
                captured[index] = Some(item);
                replies.push(Ok(None));
            }
            Ok(ProcessOut::Reply(body)) => replies.push(Ok(body)),
            Err(error) => replies.push(Err(error)),
        }
    }
    sync_partition_once(store, &mut captured);
    for (index, slot) in captured.iter_mut().enumerate() {
        let Some(item) = slot.take() else {
            continue;
        };
        let input = inputs.get(index).map(Vec::as_slice).unwrap_or(&[]);
        if let Some(spare) = spares.get_mut(index) {
            replies[index] = finish_captured(store, item, input, spare);
        } else {
            let mut spare = Vec::new();
            replies[index] = finish_captured(store, item, input, &mut spare);
        }
    }
    store.queued_captured = captured;
    replies
}

pub(crate) fn recycle_queued_replies(
    store: &mut Store,
    mut replies: Vec<Result<Option<Vec<u8>>, Error>>,
) {
    replies.clear();
    if replies.capacity() >= store.queued_replies.capacity() {
        store.queued_replies = replies;
    }
}

/// Last staged sync for each topic partition. Earlier copies of that
/// partition wait on this fsync instead of scanning every queued batch.
fn latest_sync_slots(
    captured: &[Option<CapturedProduce>],
    reps: &mut Vec<(usize, usize)>,
    slots: &mut HashMap<(TopicId, i32), usize>,
) {
    reps.clear();
    slots.clear();
    for (state_index, item) in captured.iter().enumerate() {
        let Some(item) = item else {
            continue;
        };
        for (local_index, sync) in item.pending_sync.iter().enumerate() {
            let key = (sync.id, sync.index);
            if let Some(&slot) = slots.get(&key) {
                if let Some(entry) = reps.get_mut(slot) {
                    *entry = (state_index, local_index);
                    continue;
                }
            }
            let slot = reps.len();
            reps.push((state_index, local_index));
            let _ = slots.insert(key, slot);
        }
    }
}

fn sync_partition_once(store: &mut Store, captured: &mut [Option<CapturedProduce>]) {
    let mut reps = std::mem::take(&mut store.sync_reps);
    let mut slots = std::mem::take(&mut store.sync_slots);
    let mut joins = std::mem::take(&mut store.sync_joins);
    joins.clear();
    latest_sync_slots(captured, &mut reps, &mut slots);
    reserve_sync_joins(&mut joins, reps.len());
    let parallel = reps.len() > 1;
    for &(state_index, local_index) in &reps {
        if !parallel {
            joins.push(None);
            continue;
        }
        let cloned = captured
            .get(state_index)
            .and_then(|item| item.as_ref())
            .and_then(|item| item.pending_sync.get(local_index))
            .and_then(|sync| sync.file.try_clone().ok());
        joins.push(Some(
            cloned.map(|file| std::thread::spawn(move || file.sync_data())),
        ));
    }
    for (index, join) in joins.drain(..).enumerate() {
        let Some(&(state_index, local_index)) = reps.get(index) else {
            continue;
        };
        let failed = if let Some(join) = join {
            match join {
                Some(handle) => handle.join().ok().is_none_or(|result| result.is_err()),
                None => true,
            }
        } else {
            captured
                .get(state_index)
                .and_then(|item| item.as_ref())
                .and_then(|item| item.pending_sync.get(local_index))
                .is_none_or(|sync| sync.file.sync_data().is_err())
        };
        let Some(sync) = captured
            .get_mut(state_index)
            .and_then(|item| item.as_mut())
            .and_then(|item| item.pending_sync.get_mut(local_index))
        else {
            continue;
        };
        sync.failed = failed;
        if !failed {
            continue;
        }
        let id = sync.id;
        let index = sync.index;
        for other in captured.iter_mut().flatten() {
            for pending in &mut other.pending_sync {
                if pending.id == id && pending.index == index {
                    pending.failed = true;
                }
            }
        }
    }
    store.sync_reps = reps;
    store.sync_slots = slots;
    store.sync_joins = joins;
}

fn finish_captured(
    store: &mut Store,
    mut item: CapturedProduce,
    input: &[u8],
    spare: &mut Vec<u8>,
) -> Result<Option<Vec<u8>>, Error> {
    for sync in item.pending_sync {
        let Some(part) = item
            .results
            .get_mut(sync.topic_at)
            .and_then(|topic| topic.parts.get_mut(sync.part_at))
        else {
            store.abort_staged(sync.position, sync.id, sync.index);
            continue;
        };
        if sync.failed {
            store.abort_staged(sync.position, sync.id, sync.index);
            part.error = 56;
            part.base = -1;
            continue;
        }
        match store.commit_staged(sync.position, sync.id, sync.index, sync.batch_count) {
            Ok(result) => note_append(part, result, sync.log_start),
            Err(error) => {
                part.error = error;
                part.base = -1;
            }
        }
    }
    let mut writer = Writer::new(item.response_bound, std::mem::take(spare))?;
    let written = write_produce_response(
        &mut writer,
        item.version,
        item.correlation_id,
        item.body_off,
        item.acks,
        &item.results,
        input,
    );
    if written.as_ref().ok().is_none_or(|body| body.is_none()) {
        *spare = std::mem::take(&mut writer.bytes);
    }
    written
}

fn process_inner(
    catalog: &Catalog,
    store: &mut Store,
    input: &mut [u8],
    common: &metadata::Config,
    admitted: Instant,
    mut active: impl FnMut() -> Result<(), Error>,
    grouped: bool,
) -> Result<ProcessOut, Error> {
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
    let (correlation_id, body_off, parsed) = {
        let (header, body) = protocol::RequestHeader::parse(
            input,
            if flex { 2 } else { 1 },
            common.protocol_limits,
        )?;
        let correlation_id = header.correlation_id;
        let body_off = input.len().saturating_sub(body.len());
        let parsed = parse(body, flex, version, common, &store.config)?;
        (correlation_id, body_off, parsed)
    };
    for topic in &parsed.topics {
        for part in &topic.partitions {
            let Some((start, end)) = part.bytes else {
                continue;
            };
            let abs_start = body_off.saturating_add(start);
            let abs_end = body_off.saturating_add(end);
            if let Some(batch) = input.get_mut(abs_start..abs_end) {
                stamp_leader_epoch(batch);
            }
        }
    }
    let body = input.get(body_off..).ok_or(protocol::Error::Truncated)?;
    let count = parsed
        .topics
        .iter()
        .map(|t| t.partitions.len())
        .sum::<usize>();
    let response_bound = parsed
        .topics
        .iter()
        .try_fold(16usize, |n, t| {
            n.checked_add(t.name.map_or(16, |(start, end)| end.saturating_sub(start)) + 16)
                .ok_or(Error::ResourceLimit)
        })?
        .checked_add(count.checked_mul(40).ok_or(Error::ResourceLimit)?)
        .ok_or(Error::ResourceLimit)?;
    if response_bound > common.max_response_bytes {
        return Err(Error::ResourceLimit);
    }
    let mut results = exact_list(parsed.topics.len())?;
    let mut normalized = 0usize;
    let lone_topic = parsed.topics.len() <= 1;
    // A short produce is scanned per topic. A longer one records the first copy.
    let scan_topics = !lone_topic && parsed.topics.len() <= 16;
    let (short_first, short_duplicated) = if scan_topics {
        short_topic_copies(&parsed.topics, version, body)
    } else {
        ([false; 16], [false; 16])
    };
    let mut topic_first = Vec::new();
    let mut topic_duplicated = Vec::new();
    if !lone_topic && !scan_topics {
        topic_first = vec![false; parsed.topics.len()];
        topic_duplicated = vec![false; parsed.topics.len()];
        let mut topic_owner: HashMap<ProduceTopicKey, usize> = HashMap::new();
        for (index, topic) in parsed.topics.iter().enumerate() {
            let key = produce_topic_key(version, topic, body);
            if let Some(earlier) = topic_owner.get(&key).copied() {
                if let Some(flag) = topic_duplicated.get_mut(earlier) {
                    *flag = true;
                }
            } else {
                if let Some(flag) = topic_first.get_mut(index) {
                    *flag = true;
                }
                let _ = topic_owner.insert(key, index);
            }
        }
    }
    for (topic_position, topic) in parsed.topics.iter().enumerate() {
        active()?;
        let topic_name = ranged_str(body, topic.name)?;
        let target = if version >= 13 {
            TopicId::new(topic.id).ok().and_then(|id| catalog.by_id(id))
        } else {
            topic_name.and_then(|name| catalog.by_name(name))
        };
        let (is_first, duplicate_topic) = if lone_topic {
            (true, false)
        } else if scan_topics {
            (
                short_first.get(topic_position).copied().unwrap_or(false),
                short_duplicated
                    .get(topic_position)
                    .copied()
                    .unwrap_or(false),
            )
        } else {
            (
                topic_first.get(topic_position).copied().unwrap_or(false),
                topic_duplicated
                    .get(topic_position)
                    .copied()
                    .unwrap_or(false),
            )
        };
        if !is_first {
            continue;
        }
        let mut parts = exact_list(topic.partitions.len())?;
        // Strictly increasing indexes cannot repeat, including a single partition.
        let unique_parts = strictly_increasing_indexes(&topic.partitions);
        let scan_parts = !unique_parts && topic.partitions.len() <= 16;
        let (short_part_first, short_part_duplicated) = if scan_parts {
            short_partition_copies(&topic.partitions)
        } else {
            ([false; 16], [false; 16])
        };
        let mut part_first = Vec::new();
        let mut part_duplicated = Vec::new();
        if !unique_parts && !scan_parts {
            part_first = vec![false; topic.partitions.len()];
            part_duplicated = vec![false; topic.partitions.len()];
            let mut part_owner: HashMap<i32, usize> = HashMap::new();
            for (position, part) in topic.partitions.iter().enumerate() {
                if let Some(earlier) = part_owner.get(&part.index).copied() {
                    if let Some(flag) = part_duplicated.get_mut(earlier) {
                        *flag = true;
                    }
                } else {
                    if let Some(flag) = part_first.get_mut(position) {
                        *flag = true;
                    }
                    let _ = part_owner.insert(part.index, position);
                }
            }
        }
        for (position, part) in topic.partitions.iter().enumerate() {
            let (is_part_first, duplicate_part) = if unique_parts {
                (true, false)
            } else if scan_parts {
                (
                    short_part_first.get(position).copied().unwrap_or(false),
                    short_part_duplicated
                        .get(position)
                        .copied()
                        .unwrap_or(false),
                )
            } else {
                (
                    part_first.get(position).copied().unwrap_or(false),
                    part_duplicated.get(position).copied().unwrap_or(false),
                )
            };
            if !is_part_first {
                continue;
            }
            let mut error = if !matches!(parsed.acks, -1..=1) {
                21
            } else if parsed.timeout < 0 {
                42
            } else if parsed.tx.is_some() {
                43
            } else if duplicate_topic || duplicate_part {
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
            let mut held = Held::Owned(Vec::new());
            if error == 0 {
                match normalize(ranged(body, part.bytes)?, version, &store.config) {
                    Ok(value) => {
                        normalized = normalized
                            .checked_add(value.len())
                            .ok_or(Error::ResourceLimit)?;
                        if normalized > store.config.max_normalized_bytes {
                            return Err(Error::ResourceLimit);
                        }
                        held = match value {
                            Cow::Borrowed(_) => match part.bytes {
                                Some(range) => Held::Shared(range),
                                None => {
                                    error = 87;
                                    Held::Owned(Vec::new())
                                }
                            },
                            Cow::Owned(buf) => Held::Owned(buf),
                        };
                    }
                    Err(code) => error = code,
                }
            }
            parts.push(ResultPart {
                index: part.index,
                error,
                base: -1,
                log_start: -1,
                bytes: held,
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
    // Monolithic journals write first and sync together; rolling storage syncs
    // inside its own append.
    let mut pending_sync = List::Empty;
    for (topic_at, topic) in results.iter_mut().enumerate() {
        for (part_at, part) in topic.parts.iter_mut().enumerate() {
            if let Err(error) = active() {
                if grouped {
                    rollback_pending(store, &mut pending_sync);
                }
                return Err(error);
            }
            if part.error == 0 {
                if timed_out(admitted, parsed.timeout) {
                    part.error = 7;
                    continue;
                }
                if let Some(id) = part.id {
                    let shared = if let Held::Shared((start, end)) = &part.bytes {
                        Some((*start, *end))
                    } else {
                        None
                    };
                    let partition_index = part.index;
                    let stage = |store: &mut Store, buf: &mut [u8]| {
                        if grouped {
                            store.stage_in_place_grouped(id, partition_index, buf)
                        } else {
                            store.stage_in_place(id, partition_index, buf)
                        }
                    };
                    let staged = if let Some((start, end)) = shared {
                        let abs_start = body_off.saturating_add(start);
                        let abs_end = body_off.saturating_add(end);
                        let Some(buf) = input.get_mut(abs_start..abs_end) else {
                            part.error = 87;
                            continue;
                        };
                        stage(store, buf)
                    } else if let Held::Owned(buf) = &mut part.bytes {
                        stage(store, buf)
                    } else {
                        Err(87)
                    };
                    match staged {
                        Ok((partition::Stage::Done(result), log_start, _)) => {
                            note_append(part, result, log_start);
                        }
                        Ok((partition::Stage::Sync(staged), log_start, position)) => pending_sync
                            .push(PendingSync {
                                topic_at,
                                part_at,
                                id,
                                index: part.index,
                                file: staged.file,
                                batch_count: staged.batch_count,
                                failed: false,
                                log_start,
                                position,
                            }),
                        Err(error) => part.error = error,
                    }
                } else {
                    part.error = 3;
                }
            }
        }
    }
    if grouped {
        return Ok(ProcessOut::Captured(CapturedProduce {
            version,
            correlation_id,
            body_off,
            acks: parsed.acks,
            response_bound,
            results,
            pending_sync,
        }));
    }
    sync_staged(&mut pending_sync);
    for item in pending_sync {
        let Some(part) = results
            .get_mut(item.topic_at)
            .and_then(|topic| topic.parts.get_mut(item.part_at))
        else {
            store.abort_staged(item.position, item.id, item.index);
            continue;
        };
        if item.failed {
            store.abort_staged(item.position, item.id, item.index);
            part.error = 56;
            part.base = -1;
            continue;
        }
        match store.commit_staged(item.position, item.id, item.index, item.batch_count) {
            Ok(result) => note_append(part, result, item.log_start),
            Err(error) => {
                part.error = error;
                part.base = -1;
            }
        }
    }
    let mut writer = Writer::new(response_bound, store.take_response_buf())?;
    let written = write_produce_response(
        &mut writer,
        version,
        correlation_id,
        body_off,
        parsed.acks,
        &results,
        input,
    );
    if written.as_ref().ok().is_none_or(|body| body.is_none()) {
        store.keep_response_buf(std::mem::take(&mut writer.bytes));
    }
    Ok(ProcessOut::Reply(written?))
}

fn write_produce_response(
    writer: &mut Writer,
    version: i16,
    correlation_id: i32,
    body_off: usize,
    acks: i16,
    results: &[ResultTopic],
    input: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    let flex = version >= 9;
    if acks == 0 {
        if results.iter().any(|t| t.parts.iter().any(|p| p.error != 0)) {
            return Err(Error::AcksZeroError);
        }
        return Ok(None);
    }
    writer.i32(correlation_id)?;
    if flex {
        writer.byte(0)?;
    }
    writer.count(results.len(), flex)?;
    for topic in results {
        if version >= 13 {
            writer.put(&topic.id)?;
        } else {
            writer.string(
                ranged_str(
                    input.get(body_off..).ok_or(protocol::Error::Truncated)?,
                    topic.name,
                )?,
                flex,
            )?;
        }
        writer.count(topic.parts.len(), flex)?;
        for part in &topic.parts {
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
    Ok(Some(std::mem::take(&mut writer.bytes)))
}

fn rollback_pending(store: &mut Store, pending: &mut List<PendingSync>) {
    while let Some(sync) = pending.pop() {
        if store
            .unstage_last(sync.position, sync.id, sync.index)
            .is_err()
        {
            store.abort_staged(sync.position, sync.id, sync.index);
        }
    }
}
struct Writer {
    bytes: Vec<u8>,
    cap: usize,
}
impl Writer {
    fn new(cap: usize, mut bytes: Vec<u8>) -> Result<Self, Error> {
        bytes.clear();
        let extra = cap.saturating_sub(bytes.capacity());
        if extra > 0 {
            bytes
                .try_reserve_exact(extra)
                .map_err(|_| Error::ResourceLimit)?;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn part(index: i32) -> Part {
        Part { index, bytes: None }
    }

    #[test]
    fn a_multi_partition_sync_sizes_its_join_list_once() {
        let mut joins = Vec::<u8>::new();
        super::reserve_sync_joins(&mut joins, 8);
        assert!(joins.capacity() >= 8);
        let kept = joins.capacity();
        super::reserve_sync_joins(&mut joins, 8);
        assert_eq!(joins.capacity(), kept);
        super::reserve_sync_joins(&mut joins, 3);
        assert_eq!(joins.capacity(), kept);
    }

    #[test]
    fn a_timestamp_scan_reuses_its_payload_buffer() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-scan-{nanos}"));
        let _ = std::fs::create_dir(&path);
        let common = metadata::Config::new(0, "127.0.0.1".into(), 9092, "scan".into());
        let (catalog, _) = Catalog::open(path.join("catalog"), common.catalog_limits)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut store = Store::open(Config::new(path.join("data")), &catalog)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut buf = store.take_scan_buf();
        buf.try_reserve_exact(4096)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let capacity = buf.capacity();
        store.keep_scan_buf(buf);
        let again = store.take_scan_buf();
        assert!(again.capacity() >= capacity);
        store.keep_scan_buf(again);
    }

    #[test]
    fn a_produce_response_reuses_a_parked_buffer() {
        let mut parked = Vec::new();
        parked
            .try_reserve_exact(128)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let capacity = parked.capacity();
        assert!(capacity >= 128);
        let mut writer = Writer::new(32, parked).unwrap_or_else(|error| panic!("{error:?}"));
        writer.i32(7).unwrap_or_else(|error| panic!("{error:?}"));
        let body = std::mem::take(&mut writer.bytes);
        assert!(body.capacity() >= capacity);
        let reused = Writer::new(32, body).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(reused.bytes.capacity() >= capacity);
    }

    #[test]
    fn a_second_grouped_produce_reuses_the_capture_list() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-queued-{nanos}"));
        let _ = std::fs::create_dir(&path);
        let common = metadata::Config::new(0, "127.0.0.1".into(), 9092, "queued".into());
        let (catalog, _) = Catalog::open(path.join("catalog"), common.catalog_limits)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut store = Store::open(Config::new(path.join("data")), &catalog)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut inputs = vec![vec![0, 0, 0, 1]];
        let admitted = [std::time::Instant::now()];
        let mut spare = vec![Vec::new()];
        let first = process_queued(
            &catalog,
            &mut store,
            &mut inputs,
            &admitted,
            &common,
            &mut spare,
            |_| Ok(()),
        );
        assert!(first.first().is_some_and(|result| result.is_err()));
        let captured = store.queued_captured.capacity();
        let replies = first.capacity();
        assert!(captured >= 1);
        assert!(replies >= 1);
        recycle_queued_replies(&mut store, first);
        let second = process_queued(
            &catalog,
            &mut store,
            &mut inputs,
            &admitted,
            &common,
            &mut spare,
            |_| Ok(()),
        );
        assert!(store.queued_captured.capacity() >= captured);
        assert!(second.capacity() >= replies);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn a_single_produce_slot_stays_inline() {
        let mut parts = exact_list(1).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(parts, List::Empty));
        parts.push(part(0));
        assert!(matches!(parts, List::One(_)));
        assert_eq!(parts.len(), 1);
        assert_eq!(parts.first().map(|part| part.index), Some(0));
        let mut many = exact_list(2).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(many, List::Many(_)));
        many.push(part(0));
        many.push(part(1));
        assert_eq!(many.len(), 2);
        assert!(matches!(many, List::Many(_)));
        let inline = List::from_vec(vec![part(3)]);
        assert!(matches!(inline, List::One(_)));
        assert_eq!(inline.first().map(|part| part.index), Some(3));
    }

    #[test]
    fn a_produce_topic_key_borrows_the_request_name() {
        let body = b"orderspayments";
        let orders = Topic {
            name: Some((0, 6)),
            id: [1; 16],
            partitions: List::Empty,
        };
        let payments = Topic {
            name: Some((6, 14)),
            id: [2; 16],
            partitions: List::Empty,
        };
        let again = Topic {
            name: Some((0, 6)),
            id: [9; 16],
            partitions: List::Empty,
        };
        let orders_key = produce_topic_key(9, &orders, body);
        assert!(matches!(
            orders_key,
            ProduceTopicKey::Name(Some(bytes))
                if bytes.as_ptr() == body.as_ptr() && bytes.len() == 6
        ));
        assert_eq!(orders_key, produce_topic_key(9, &again, body));
        assert_ne!(orders_key, produce_topic_key(9, &payments, body));
        let topics = [
            Topic {
                name: Some((0, 6)),
                id: [1; 16],
                partitions: List::Empty,
            },
            Topic {
                name: Some((6, 14)),
                id: [2; 16],
                partitions: List::Empty,
            },
            Topic {
                name: Some((0, 6)),
                id: [9; 16],
                partitions: List::Empty,
            },
        ];
        let (first, duplicated) = short_topic_copies(&topics, 9, body);
        assert_eq!(
            (
                first.first().copied(),
                duplicated.first().copied(),
                first.get(1).copied(),
                duplicated.get(1).copied(),
                first.get(2).copied(),
                duplicated.get(2).copied()
            ),
            (
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(true)
            )
        );
        let payments_key = produce_topic_key(9, &payments, body);
        assert!(matches!(
            payments_key,
            ProduceTopicKey::Name(Some(bytes)) if bytes.as_ptr() == body[6..].as_ptr()
        ));
        let missing = Topic {
            name: None,
            id: [0; 16],
            partitions: List::Empty,
        };
        assert_eq!(
            produce_topic_key(9, &missing, body),
            ProduceTopicKey::Name(None)
        );
        assert_eq!(
            produce_topic_key(13, &orders, body),
            ProduceTopicKey::Id([1; 16])
        );
    }

    #[test]
    fn strictly_increasing_partition_indexes_need_no_duplicate_map() {
        assert!(strictly_increasing_indexes(&[]));
        assert!(strictly_increasing_indexes(&[part(0)]));
        assert!(strictly_increasing_indexes(&[part(0), part(1), part(4)]));
        assert!(!strictly_increasing_indexes(&[part(1), part(1)]));
        assert!(!strictly_increasing_indexes(&[part(1), part(0)]));
        assert!(!strictly_increasing_indexes(&[part(0), part(2), part(2)]));
        let mixed = [part(1), part(0), part(1)];
        let (first, duplicated) = short_partition_copies(&mixed);
        assert_eq!(
            (
                first.first().copied(),
                duplicated.first().copied(),
                first.get(1).copied(),
                duplicated.get(1).copied(),
                first.get(2).copied(),
                duplicated.get(2).copied()
            ),
            (
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(true)
            )
        );
    }

    #[test]
    fn finished_append_keeps_its_offset_after_the_request_timer() {
        let mut part = ResultPart {
            index: 0,
            error: 0,
            base: -1,
            log_start: -1,
            bytes: Held::Owned(Vec::new()),
            id: None,
        };
        note_append(
            &mut part,
            partition::Append {
                base_offset: 7,
                next_offset: 8,
                record_count: 1,
                batch_count: 1,
            },
            Some(0),
        );
        // The timer elapsed before this observation. The offset is already assigned.
        let expired = Instant::now()
            .checked_sub(Duration::from_secs(5))
            .expect("past");
        assert!(timed_out(expired, 1));
        assert_eq!(part.error, 0);
        assert_eq!(part.base, 7);
        assert_eq!(part.log_start, 0);
    }

    fn null_sync(id: TopicId, index: i32) -> PendingSync {
        PendingSync {
            topic_at: 0,
            part_at: 0,
            id,
            index,
            file: File::open("/dev/null").expect("null device"),
            batch_count: 1,
            failed: false,
            log_start: Some(0),
            position: 0,
        }
    }

    fn captured(syncs: Vec<PendingSync>) -> CapturedProduce {
        CapturedProduce {
            version: 9,
            correlation_id: 1,
            body_off: 0,
            acks: 1,
            response_bound: 16,
            results: List::Empty,
            pending_sync: List::from_vec(syncs),
        }
    }

    #[test]
    fn queued_produces_keep_the_last_sync_for_each_partition() {
        let orders = TopicId::new([9; 16]).expect("orders");
        let payments = TopicId::new([8; 16]).expect("payments");
        let captured = vec![
            Some(captured(vec![null_sync(orders, 0), null_sync(orders, 1)])),
            None,
            Some(captured(vec![null_sync(orders, 0), null_sync(payments, 0)])),
        ];
        let mut reps = Vec::new();
        let mut slots = HashMap::new();
        latest_sync_slots(&captured, &mut reps, &mut slots);
        assert_eq!(reps, vec![(2, 0), (0, 1), (2, 1)]);
        latest_sync_slots(&captured, &mut reps, &mut slots);
        assert_eq!(reps, vec![(2, 0), (0, 1), (2, 1)]);
        assert!(slots.capacity() > 0);
    }
}
