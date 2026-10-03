//! Bounded ordinary Kafka batch storage with synchronized assigned offsets.
//!
//! Each admitted input is one atomic journal entry, including inputs with several
//! batches. This stores only the uncompressed, nontransactional subset admitted
//! by `records`; it does not advertise a Kafka handler or replication guarantee.
//! Run this synchronous API on an exclusively owned storage thread. Paths and
//! cross-process ownership are caller configuration, as for `Journal`.

use crate::{journal, records, segments};
use std::path::Path;

/// Storage errors retain neither input records nor configured paths.
#[derive(Debug)]
pub enum Error {
    /// Negative base or recovery output budget smaller than a possible entry.
    InvalidLimits,
    /// Input exceeds the configured journal entry bound.
    InputTooLarge,
    /// Assigned offsets exceed the nonnegative signed Kafka offset domain.
    OffsetOverflow,
    /// Complete input failed ordinary batch admission.
    Records(records::Error),
    /// Journal initialization, recovery, append, or fetch failed.
    Storage(journal::Error),
    /// Rolling publication, seek work, layout or resource policy failed.
    Segments(segments::Error),
    /// A checksummed payload disagrees with its stored logical record range.
    CorruptPayload,
    /// A bounded payload allocation failed.
    AllocationFailed,
    /// A prior payload or storage failure requires reopening.
    Poisoned,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Records(error) => write!(f, "partition records: {error}"),
            Self::Storage(error) => write!(f, "partition storage: {error}"),
            Self::Segments(error) => write!(f, "partition segments: {error}"),
            _ => write!(f, "partition error: {self:?}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}
impl From<records::Error> for Error {
    fn from(error: records::Error) -> Self {
        Self::Records(error)
    }
}
impl From<segments::Error> for Error {
    fn from(error: segments::Error) -> Self {
        match error {
            segments::Error::Storage(error) => Self::Storage(error),
            other => Self::Segments(other),
        }
    }
}

#[allow(
    clippy::large_enum_variant,
    reason = "One bounded inline backend per configured store; avoids an infallible heap allocation when selecting rolling storage."
)]
enum Backend {
    Journal(journal::Journal),
    Segments(segments::Log),
}
impl Backend {
    fn append(&mut self, count: u32, payload: &[u8]) -> Result<journal::Append, Error> {
        match self {
            Self::Journal(log) => Ok(log.append(count, payload)?),
            Self::Segments(log) => Ok(log.append(count, payload)?),
        }
    }
    fn fetch(
        &mut self,
        offset: u64,
        entries: usize,
        bytes: usize,
    ) -> Result<Vec<journal::Entry>, Error> {
        match self {
            Self::Journal(log) => Ok(log.fetch(offset, entries, bytes)?),
            Self::Segments(log) => Ok(log.fetch(offset, entries, bytes)?),
        }
    }
    fn next_offset(&self) -> u64 {
        match self {
            Self::Journal(log) => log.next_offset(),
            Self::Segments(log) => log.next_offset(),
        }
    }
    fn entry_count(&self) -> usize {
        match self {
            Self::Journal(log) => log.entry_count(),
            Self::Segments(log) => log.entry_count(),
        }
    }
    fn is_poisoned(&self) -> bool {
        match self {
            Self::Journal(log) => log.is_poisoned(),
            Self::Segments(log) => log.is_poisoned(),
        }
    }
}

/// Successfully synchronized, contiguous record range assigned to one input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Append {
    /// First assigned record offset.
    pub base_offset: i64,
    /// Exclusive end of the assigned record range.
    pub next_offset: i64,
    /// Number of ordinary records, independent of input byte count.
    pub record_count: u32,
    /// Number of batches in the input.
    pub batch_count: usize,
}

/// An exclusively owned journal whose complete payloads are validated batches.
pub struct Partition {
    journal: Backend,
    record_limits: records::Limits,
    journal_limits: journal::Limits,
    poisoned: bool,
    next_offset: i64,
}
impl Partition {
    /// Recover and validate every complete entry, including its assigned range.
    ///
    /// The output budget must fit one maximum-size journal entry including its
    /// owned `Entry` charge, permitting bounded one-entry recovery. Journal first
    /// repairs only a demonstrably incomplete final entry and synchronizes its
    /// recovered bytes; structurally corrupt complete Kafka payloads then fail
    /// closed. Index, total file, entry, validation and output limits all apply.
    pub fn open(
        path: impl AsRef<Path>,
        base_offset: i64,
        journal_limits: journal::Limits,
        record_limits: records::Limits,
    ) -> Result<(Self, journal::Recovery), Error> {
        let base_offset = u64::try_from(base_offset).map_err(|_| Error::InvalidLimits)?;
        let recovery_bytes = journal_limits
            .max_entry_bytes()
            .checked_add(std::mem::size_of::<journal::Entry>())
            .ok_or(Error::InvalidLimits)?;
        if recovery_bytes > journal_limits.max_fetch_bytes() {
            return Err(Error::InvalidLimits);
        }
        let (journal, recovery) = journal::Journal::open(path, base_offset, journal_limits)?;
        let next_offset =
            i64::try_from(journal.next_offset()).map_err(|_| Error::OffsetOverflow)?;
        let mut result = Self {
            journal: Backend::Journal(journal),
            record_limits,
            journal_limits,
            poisoned: false,
            next_offset,
        };
        let mut cursor = base_offset;
        while cursor < result.journal.next_offset() {
            let entries = result.journal.fetch(cursor, 1, recovery_bytes)?;
            let entry = entries.first().ok_or(Error::CorruptPayload)?;
            if entry.first_offset != cursor {
                return Err(Error::CorruptPayload);
            }
            cursor = result.check_entry(entry)?;
        }
        Ok((result, recovery))
    }

    /// Select genuine rolling storage with bounded verified seek checkpoints.
    ///
    /// The directory must use the rolling layout; existing monolithic files are
    /// never silently migrated or hidden. Recovery validates all ordinary data,
    /// rejects/rebuilds stale indexes and reports active-tail/staging outcomes.
    /// The caller still owns exclusive synchronous access to this partition.
    pub fn open_segmented(
        path: impl AsRef<Path>,
        base_offset: i64,
        journal_limits: journal::Limits,
        record_limits: records::Limits,
        segment_limits: segments::Limits,
    ) -> Result<(Self, segments::Recovery), Error> {
        let base = u64::try_from(base_offset).map_err(|_| Error::InvalidLimits)?;
        let (log, recovery) =
            segments::Log::open(path, base, journal_limits, record_limits, segment_limits)?;
        let next_offset = i64::try_from(log.next_offset()).map_err(|_| Error::OffsetOverflow)?;
        Ok((
            Self {
                journal: Backend::Segments(log),
                record_limits,
                journal_limits,
                poisoned: false,
                next_offset,
            },
            recovery,
        ))
    }

    /// Validate the entire input, assign offsets, then append and synchronize once.
    ///
    /// Only the eight-byte base offset of each batch is replaced. Record data,
    /// timestamps, leader epoch and protected CRC bytes stay identical. Selected
    /// leader epochs/timestamp policy belong to the future Produce handler.
    /// Validation/allocation/budget failures cause no append. A journal write or
    /// synchronization failure is ambiguous and disables further use until reopen.
    pub fn append(&mut self, bytes: &[u8]) -> Result<Append, Error> {
        self.check_alive()?;
        if bytes.len() > self.journal_limits.max_entry_bytes() {
            return Err(Error::InputTooLarge);
        }
        let checked = records::validate(bytes, self.record_limits)?;
        let record_count =
            u32::try_from(checked.record_count()).map_err(|_| Error::OffsetOverflow)?;
        let base_offset = self.next_offset();
        let next_offset = base_offset
            .checked_add(i64::from(record_count))
            .ok_or(Error::OffsetOverflow)?;
        let mut assigned = Vec::new();
        assigned
            .try_reserve_exact(bytes.len())
            .map_err(|_| Error::AllocationFailed)?;
        let mut offset = base_offset;
        for batch in checked.batches() {
            let batch = batch?;
            assigned.extend_from_slice(&offset.to_be_bytes());
            assigned.extend_from_slice(batch.bytes.get(8..).ok_or(Error::CorruptPayload)?);
            offset = offset
                .checked_add(i64::from(batch.record_count))
                .ok_or(Error::OffsetOverflow)?;
        }
        if offset != next_offset || assigned.len() != bytes.len() {
            return Err(Error::CorruptPayload);
        }
        self.journal.append(record_count, &assigned)?;
        self.next_offset = next_offset;
        Ok(Append {
            base_offset,
            next_offset,
            record_count,
            batch_count: checked.batch_count(),
        })
    }

    /// Fetch bounded whole atomic inputs containing/following the supplied offset.
    ///
    /// An offset inside an entry returns the entire entry, possibly several Kafka
    /// batches before that offset. The wire Fetch owner must select batch boundaries
    /// and implement its oversized-first-batch policy. Charge and stop semantics
    /// are exactly `Journal::fetch`; no retained secondary index or extra copy.
    pub fn fetch(
        &mut self,
        first_offset: i64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<journal::Entry>, Error> {
        self.check_alive()?;
        let offset = u64::try_from(first_offset).map_err(|_| Error::OffsetOverflow)?;
        let entries = self.journal.fetch(offset, max_entries, max_bytes)?;
        for entry in &entries {
            if let Err(error) = self.check_entry(entry) {
                self.poisoned = true;
                return Err(error);
            }
        }
        Ok(entries)
    }

    /// Exclusive durable end offset; no failed append advances this value.
    pub fn next_offset(&self) -> i64 {
        self.next_offset
    }
    /// Retained atomic journal entries, bounded by the configured index budget.
    pub fn entry_count(&self) -> usize {
        self.journal.entry_count()
    }
    /// Data segment count; the inherited monolithic backend always has one.
    pub fn segment_count(&self) -> usize {
        match &self.journal {
            Backend::Journal(_) => 1,
            Backend::Segments(log) => log.segment_count(),
        }
    }
    /// Byte-preserving atomic rewrite of a sealed rolling generation.
    ///
    /// The storage owner must serialize this with all append/read operations.
    /// Monolithic storage has no sealed generation and rejects the operation.
    /// This implements no retention, compaction or Kafka administration API.
    pub fn replace_sealed(&mut self, base_offset: i64) -> Result<(), Error> {
        self.check_alive()?;
        let base = u64::try_from(base_offset).map_err(|_| Error::OffsetOverflow)?;
        match &mut self.journal {
            Backend::Segments(log) => Ok(log.replace_sealed(base)?),
            Backend::Journal(_) => Err(Error::InvalidLimits),
        }
    }
    pub(crate) fn timestamp_start(&self, wanted: i64) -> Result<i64, Error> {
        self.check_alive()?;
        let offset = match &self.journal {
            Backend::Journal(log) => log.base_offset(),
            Backend::Segments(log) => log.timestamp_start(wanted)?,
        };
        i64::try_from(offset).map_err(|_| Error::OffsetOverflow)
    }
    pub(crate) fn read_with_work(
        &mut self,
        offset: i64,
        maximum: usize,
        scan_bytes: usize,
        scan_entries: usize,
    ) -> Result<(Option<journal::Entry>, segments::Work), Error> {
        self.check_alive()?;
        let offset = u64::try_from(offset).map_err(|_| Error::OffsetOverflow)?;
        let (entry, work) = match &mut self.journal {
            Backend::Journal(log) => {
                let entry = log.fetch(offset, 1, maximum)?.pop();
                let work = entry
                    .as_ref()
                    .map_or(segments::Work::default(), |e| segments::Work {
                        bytes: e.payload.len(),
                        entries: 1,
                    });
                (entry, work)
            }
            Backend::Segments(log) => log.read_one(offset, maximum, scan_bytes, scan_entries)?,
        };
        if let Some(entry) = &entry {
            if let Err(error) = self.check_entry(entry) {
                self.poisoned = true;
                return Err(error);
            }
        }
        Ok((entry, work))
    }
    /// Whether reopening is required after a payload or storage failure.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned || self.journal.is_poisoned()
    }
    fn check_alive(&self) -> Result<(), Error> {
        if self.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    fn check_entry(&self, entry: &journal::Entry) -> Result<u64, Error> {
        let checked = records::validate(&entry.payload, self.record_limits)
            .map_err(|_| Error::CorruptPayload)?;
        if checked.record_count() != entry.record_count as usize {
            return Err(Error::CorruptPayload);
        }
        let mut offset = i64::try_from(entry.first_offset).map_err(|_| Error::CorruptPayload)?;
        for batch in checked.batches() {
            let batch = batch.map_err(|_| Error::CorruptPayload)?;
            if batch.base_offset != offset {
                return Err(Error::CorruptPayload);
            }
            offset = batch.next_offset;
        }
        let next = entry
            .first_offset
            .checked_add(u64::from(entry.record_count))
            .ok_or(Error::CorruptPayload)?;
        if u64::try_from(offset).ok() != Some(next) {
            return Err(Error::CorruptPayload);
        }
        Ok(next)
    }
}
