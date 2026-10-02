//! Bounded ordinary Kafka batch storage with synchronized assigned offsets.
//!
//! Each admitted input is one atomic journal entry, including inputs with several
//! batches. This stores only the uncompressed, nontransactional subset admitted
//! by `records`; it does not advertise a Kafka handler or replication guarantee.
//! Run this synchronous API on an exclusively owned storage thread. Paths and
//! cross-process ownership are caller configuration, as for `Journal`.

use crate::{journal, records};
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
    journal: journal::Journal,
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
            journal,
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
