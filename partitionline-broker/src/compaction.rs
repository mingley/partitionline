//! Explicit bounded cleaning of ordinary keyed records.
//!
//! The storage owner supplies confirmed/protected offset bounds and serializes
//! cleaning with append/read/retention. The batch planner still rejects
//! transactional, control, and idempotent batches. [`Cleaner`] compacts records
//! the caller has already classified and publishes that result only after a
//! durable swap commit. There is no background cleaner or Kafka wire API.

use crate::journal::Journal;
use crate::{journal, records};
use std::fmt;

/// Positive total-work, key-map and operation-scratch ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub(crate) scan_bytes: u64,
    pub(crate) scan_entries: usize,
    pub(crate) records: usize,
    pub(crate) keys: usize,
    pub(crate) key_bytes: usize,
    pub(crate) probes: usize,
    pub(crate) max_segments: usize,
    pub(crate) scratch_bytes: usize,
}
impl Limits {
    /// Bound cumulative payload scan bytes, entry/record visits, distinct keys,
    /// copied key bytes, hash-table probes, selected segments and scratch bytes.
    /// Counts are at most one million, segments1024, scan512MiB, key64MiB and
    /// scratch256MiB. Every pass consumes work; exhaustion precedes publication.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent explicit cleaning budgets"
    )]
    pub fn new(
        scan_bytes: u64,
        scan_entries: usize,
        records: usize,
        keys: usize,
        key_bytes: usize,
        probes: usize,
        max_segments: usize,
        scratch_bytes: usize,
    ) -> Result<Self, Error> {
        if !(1..=512 * 1024 * 1024).contains(&scan_bytes)
            || [scan_entries, records, keys]
                .iter()
                .any(|n| !(1..=1_000_000).contains(n))
            || !(1..=64 * 1024 * 1024).contains(&key_bytes)
            || !(1..=16_000_000).contains(&probes)
            || !(1..=1024).contains(&max_segments)
            || !(1..=256 * 1024 * 1024).contains(&scratch_bytes)
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            scan_bytes,
            scan_entries,
            records,
            keys,
            key_bytes,
            probes,
            max_segments,
            scratch_bytes,
        })
    }
    /// Configured operation scratch ceiling; retained Store resources are separate.
    pub fn scratch_bytes(self) -> usize {
        self.scratch_bytes
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            scan_bytes: 128 * 1024 * 1024,
            scan_entries: 65536,
            records: 262144,
            keys: 65536,
            key_bytes: 4 * 1024 * 1024,
            probes: 1_000_000,
            max_segments: 64,
            scratch_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Ordinary tombstone horizon and bounded explicit cleaning policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    delete_retention_ms: u64,
    limits: Limits,
}
impl Policy {
    /// Zero retention is valid; creating a horizon still checks signed overflow.
    pub fn new(delete_retention_ms: u64, limits: Limits) -> Result<Self, Error> {
        if delete_retention_ms > i64::MAX as u64 {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            delete_retention_ms,
            limits,
        })
    }
    /// Window applied when a retained tombstone first receives a delete horizon.
    pub fn delete_retention_ms(self) -> u64 {
        self.delete_retention_ms
    }
    /// Total-work and operation-resource limits.
    pub fn limits(self) -> Limits {
        self.limits
    }
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            delete_retention_ms: 24 * 60 * 60 * 1000,
            limits: Limits::default(),
        }
    }
}

/// Completed durable prefix rewrite and obsolete-generation cleanup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Outcome {
    /// First logical offset covered by the cleaning round.
    pub start_offset: i64,
    /// Exclusive end; active/protected suffix and partition end remain unchanged.
    pub end_offset: i64,
    /// Ordinary records inspected in the source round (not repeated work visits).
    pub scanned_records: usize,
    /// Actual retained records; logical extents count independently.
    pub retained_records: usize,
    /// Sealed generations atomically replaced.
    pub rewritten_segments: usize,
    /// Physical data-file lengths before cleaning, excluding seek/manifest files.
    pub bytes_before: u64,
    /// Physical selected data-file lengths after cleaning.
    pub bytes_after: u64,
}
/// Exhausted configured resource; no record contents are retained in errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Budget {
    /// Cumulative loaded/certified payload bytes over all passes.
    ScanBytes,
    /// Cumulative source/output entry visits over all passes.
    ScanEntries,
    /// Parsing and projection visits, including repeated encoder passes.
    Records,
    /// Distinct non-null keys in the mapped round.
    Keys,
    /// Copied distinct key bytes; empty keys still occupy a map slot.
    KeyBytes,
    /// Total bounded open-address table probes.
    Probes,
    /// Eligible sealed prefix exceeds the configured round cap.
    Segments,
    /// Map, prepared output, metadata and publication scratch capacities.
    Scratch,
    /// Encoded entry or segment exceeds its hard output ceiling.
    OutputBytes,
}
/// Structural, policy, arithmetic or bounded-operation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Zero/outside-hard-ceiling configuration.
    InvalidLimits,
    /// Negative clock or overflowing new horizon/timestamp delta.
    InvalidClock,
    /// Exhausted work/allocation envelope before publication.
    BudgetExceeded(Budget),
    /// A fallible bounded allocation failed.
    AllocationFailed,
    /// Ordinary stored-record validation failed or recognized protected features.
    Records(records::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "compaction error: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<records::Error> for Error {
    fn from(error: records::Error) -> Self {
        Self::Records(error)
    }
}

#[derive(Clone, Copy, Default)]
struct Slot {
    hash: u64,
    start: usize,
    len: usize,
    offset: i64,
    occupied: bool,
}
/// Internal bounded map and preflight encoder. Its arena, output and every
/// entry/record visit are charged before any storage publication can start.
pub(crate) struct Planner {
    policy: Policy,
    now: i64,
    horizon: i64,
    slots: Vec<Slot>,
    keys: Vec<u8>,
    distinct: usize,
    probes: usize,
    scan_bytes: u64,
    scan_entries: usize,
    visits: usize,
    scratch: usize,
    pub(crate) scanned: usize,
    pub(crate) retained: usize,
    last_batch: Option<(i64, i64)>,
}
impl Planner {
    pub(crate) fn new(policy: Policy, now: i64, fixed: usize) -> Result<Self, Error> {
        let horizon = now
            .checked_add(policy.delete_retention_ms as i64)
            .ok_or(Error::InvalidClock)?;
        if now < 0 {
            return Err(Error::InvalidClock);
        }
        let count = policy
            .limits
            .keys
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(Error::InvalidLimits)?;
        let scratch = fixed
            .checked_add(
                count
                    .checked_mul(std::mem::size_of::<Slot>())
                    .ok_or(Error::InvalidLimits)?,
            )
            .and_then(|n| n.checked_add(policy.limits.key_bytes))
            .ok_or(Error::InvalidLimits)?;
        if scratch > policy.limits.scratch_bytes {
            return Err(Error::BudgetExceeded(Budget::Scratch));
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(count)
            .map_err(|_| Error::AllocationFailed)?;
        slots.resize(count, Slot::default());
        let mut keys = Vec::new();
        keys.try_reserve_exact(policy.limits.key_bytes)
            .map_err(|_| Error::AllocationFailed)?;
        let scratch = fixed
            .checked_add(slots.capacity() * std::mem::size_of::<Slot>())
            .and_then(|n| n.checked_add(keys.capacity()))
            .ok_or(Error::InvalidLimits)?;
        if scratch > policy.limits.scratch_bytes {
            return Err(Error::BudgetExceeded(Budget::Scratch));
        }
        Ok(Self {
            policy,
            now,
            horizon,
            slots,
            keys,
            distinct: 0,
            probes: 0,
            scan_bytes: 0,
            scan_entries: 0,
            visits: 0,
            scratch,
            scanned: 0,
            retained: 0,
            last_batch: None,
        })
    }
    pub(crate) fn charge_scratch(&mut self, bytes: usize) -> Result<(), Error> {
        let total = self
            .scratch
            .checked_add(bytes)
            .ok_or(Error::BudgetExceeded(Budget::Scratch))?;
        if total > self.policy.limits.scratch_bytes {
            return Err(Error::BudgetExceeded(Budget::Scratch));
        }
        self.scratch = total;
        Ok(())
    }
    pub(crate) fn charge_entry(&mut self, bytes: usize) -> Result<(), Error> {
        let total = self
            .scan_bytes
            .checked_add(bytes as u64)
            .ok_or(Error::BudgetExceeded(Budget::ScanBytes))?;
        if total > self.policy.limits.scan_bytes {
            return Err(Error::BudgetExceeded(Budget::ScanBytes));
        }
        if self.scan_entries == self.policy.limits.scan_entries {
            return Err(Error::BudgetExceeded(Budget::ScanEntries));
        }
        self.scan_bytes = total;
        self.scan_entries += 1;
        Ok(())
    }
    pub(crate) fn source_cap(&self, maximum: usize) -> Result<usize, Error> {
        if self.scan_entries == self.policy.limits.scan_entries {
            return Err(Error::BudgetExceeded(Budget::ScanEntries));
        }
        let remaining = self.policy.limits.scan_bytes - self.scan_bytes;
        usize::try_from(remaining.min(maximum as u64))
            .map_err(|_| Error::BudgetExceeded(Budget::ScanBytes))?
            .checked_add(std::mem::size_of::<journal::Entry>())
            .ok_or(Error::BudgetExceeded(Budget::ScanBytes))
    }
    fn validation_limits(&self, limits: records::Limits) -> records::Limits {
        limits.remaining_records(self.policy.limits.records - self.visits)
    }
    fn record_error(error: records::Error) -> Error {
        if matches!(
            error,
            records::Error::BudgetExceeded {
                budget: records::Budget::Records,
                ..
            }
        ) {
            Error::BudgetExceeded(Budget::Records)
        } else {
            Error::Records(error)
        }
    }
    pub(crate) fn reserve_certification(
        &mut self,
        bytes: usize,
        records: usize,
    ) -> Result<(), Error> {
        self.charge_entry(bytes)?;
        self.parsed_records(records)
    }
    fn parsed_records(&mut self, n: usize) -> Result<(), Error> {
        if n > self.policy.limits.records - self.visits {
            return Err(Error::BudgetExceeded(Budget::Records));
        }
        self.visits += n;
        Ok(())
    }
    fn locate(&mut self, key: &[u8], insert: Option<i64>) -> Result<Option<i64>, Error> {
        let hash = key.iter().fold(0xcbf29ce484222325u64, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
        });
        let mask = self.slots.len() - 1;
        let mut at = hash as usize & mask;
        loop {
            if self.probes == self.policy.limits.probes {
                return Err(Error::BudgetExceeded(Budget::Probes));
            }
            self.probes += 1;
            let slot = self.slots[at];
            if !slot.occupied {
                let Some(offset) = insert else {
                    return Ok(None);
                };
                if self.distinct == self.policy.limits.keys {
                    return Err(Error::BudgetExceeded(Budget::Keys));
                }
                if key.len() > self.policy.limits.key_bytes - self.keys.len() {
                    return Err(Error::BudgetExceeded(Budget::KeyBytes));
                }
                let start = self.keys.len();
                self.keys.extend_from_slice(key);
                self.slots[at] = Slot {
                    hash,
                    start,
                    len: key.len(),
                    offset,
                    occupied: true,
                };
                self.distinct += 1;
                return Ok(Some(offset));
            }
            if slot.hash == hash && self.keys[slot.start..slot.start + slot.len] == *key {
                if let Some(offset) = insert {
                    self.slots[at].offset = offset;
                    return Ok(Some(offset));
                }
                return Ok(Some(slot.offset));
            }
            at = (at + 1) & mask;
        }
    }
    pub(crate) fn map(
        &mut self,
        entry: &journal::Entry,
        kind: u32,
        limits: records::Limits,
    ) -> Result<i64, Error> {
        self.charge_entry(entry.payload.len())?;
        // Reserve enough record work for validation AND map projections before
        // the validator can enter its first record body. Empty entries need no
        // record work but still consume bounded entry/header work.
        let limits = limits.remaining_records((self.policy.limits.records - self.visits) / 2);
        let (batches, count) = if kind == 0 {
            let checked = records::validate(&entry.payload, limits).map_err(Self::record_error)?;
            if checked.record_count() != entry.record_count as usize {
                return Err(Error::Records(records::Error::Invalid {
                    at: 0,
                    kind: records::Invalid::Offset,
                }));
            }
            (checked.batches(), checked.record_count())
        } else {
            let checked =
                records::validate_read(&entry.payload, limits).map_err(Self::record_error)?;
            (checked.batches(), checked.record_count())
        };
        self.parsed_records(count)?;
        self.parsed_records(count)?;
        let first = i64::try_from(entry.first_offset).map_err(|_| Error::InvalidLimits)?;
        let end = entry
            .first_offset
            .checked_add(u64::from(entry.record_count))
            .and_then(|n| i64::try_from(n).ok())
            .ok_or(Error::InvalidLimits)?;
        let (mut next, mut maximum) = (first, i64::MIN);
        if entry.record_count == 0 {
            return Err(Error::InvalidLimits);
        }
        for batch in batches {
            let batch = batch?;
            if batch.base_offset < first
                || batch.next_offset > end
                || (kind == 0 && batch.base_offset != next)
            {
                return Err(Error::Records(records::Error::Invalid {
                    at: 0,
                    kind: records::Invalid::Offset,
                }));
            }
            next = batch.next_offset;
            maximum = maximum.max(batch.max_timestamp);
            self.last_batch = Some((batch.base_offset, batch.next_offset));
            for record in batch.records() {
                let record = record?;
                self.scanned += 1;
                if let Some(key) = record.key {
                    self.locate(key, Some(record.offset))?;
                }
            }
        }
        if kind == 0 && next != end {
            return Err(Error::Records(records::Error::Invalid {
                at: 0,
                kind: records::Invalid::Offset,
            }));
        }
        Ok(maximum)
    }
    fn keep(&mut self, record: records::Record<'_>, horizon: Option<i64>) -> Result<bool, Error> {
        let Some(key) = record.key else {
            return Ok(false);
        };
        Ok(self.locate(key, None)? == Some(record.offset)
            && (record.value.is_some() || horizon.is_none_or(|n| self.now < n)))
    }
    pub(crate) fn encode(
        &mut self,
        bytes: &[u8],
        limits: records::Limits,
        maximum: usize,
    ) -> Result<(Vec<u8>, usize), Error> {
        self.charge_entry(bytes.len())?;
        let checked = records::validate_read(bytes, self.validation_limits(limits))
            .map_err(Self::record_error)?;
        self.parsed_records(checked.record_count())?;
        let size = self.measure(checked, maximum)?;
        self.charge_scratch(size)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(size)
            .map_err(|_| Error::AllocationFailed)?;
        if output.capacity() > size {
            self.charge_scratch(output.capacity() - size)?;
        }
        // One exact reservation per output avoids repeated quadratic copies.
        // Capacities remain charged across all prepared entries in the round.
        for batch in checked.batches() {
            let batch = batch?;
            let (mut kept, mut tombstone, mut first, mut maximum_time) =
                (0usize, false, None, i64::MIN);
            self.parsed_records(batch.record_count as usize)?;
            for record in batch.records() {
                let record = record?;
                if self.keep(record, batch.delete_horizon_ms)? {
                    kept += 1;
                    tombstone |= record.value.is_none();
                    if first.is_none() {
                        first = Some(record.timestamp);
                    }
                    maximum_time = maximum_time.max(record.timestamp);
                }
            }
            let horizon = batch
                .delete_horizon_ms
                .or_else(|| tombstone.then_some(self.horizon));
            let unchanged = kept > 0
                && kept == batch.record_count as usize
                && (!tombstone || batch.delete_horizon_ms.is_some());
            if unchanged {
                self.extend(&mut output, batch.bytes, maximum)?;
                self.retained += kept;
                continue;
            }
            if kept == 0 {
                if self.last_batch != Some((batch.base_offset, batch.next_offset)) {
                    continue;
                }
                let mut header = [0u8; 61];
                header.copy_from_slice(&batch.bytes[..61]);
                header[8..12].copy_from_slice(&49i32.to_be_bytes());
                header[21..23].copy_from_slice(&0u16.to_be_bytes());
                header[27..35].copy_from_slice(&(-1i64).to_be_bytes());
                header[57..61].copy_from_slice(&0i32.to_be_bytes());
                let crc = crc32c::crc32c(&header[21..]);
                header[17..21].copy_from_slice(&crc.to_be_bytes());
                self.extend(&mut output, &header, maximum)?;
                continue;
            }
            let base_time = horizon.or(first).ok_or(Error::InvalidClock)?;
            let start = output.len();
            self.extend(&mut output, &batch.bytes[..61], maximum)?;
            self.parsed_records(batch.record_count as usize)?;
            for record in batch.records() {
                let record = record?;
                if !self.keep(record, batch.delete_horizon_ms)? {
                    continue;
                }
                let delta = record
                    .timestamp
                    .checked_sub(base_time)
                    .ok_or(Error::InvalidClock)?;
                let mut timestamp = [0u8; 10];
                let n = encode_varlong(delta, &mut timestamp);
                let length = 1usize
                    .checked_add(n)
                    .and_then(|n| n.checked_add(record.suffix.len()))
                    .and_then(|n| i32::try_from(n).ok())
                    .ok_or(Error::BudgetExceeded(Budget::OutputBytes))?;
                let mut prefix = [0u8; 10];
                let p = encode_varlong(i64::from(length), &mut prefix);
                self.extend(&mut output, &prefix[..p], maximum)?;
                self.extend(&mut output, &[0], maximum)?;
                self.extend(&mut output, &timestamp[..n], maximum)?;
                self.extend(&mut output, record.suffix, maximum)?;
            }
            let batch_bytes = output.len() - start;
            let length = i32::try_from(batch_bytes - 12)
                .map_err(|_| Error::BudgetExceeded(Budget::OutputBytes))?;
            output[start + 8..start + 12].copy_from_slice(&length.to_be_bytes());
            output[start + 21..start + 23]
                .copy_from_slice(&(if horizon.is_some() { 0x40u16 } else { 0 }).to_be_bytes());
            output[start + 27..start + 35].copy_from_slice(&base_time.to_be_bytes());
            output[start + 35..start + 43].copy_from_slice(&maximum_time.to_be_bytes());
            output[start + 57..start + 61].copy_from_slice(&(kept as i32).to_be_bytes());
            let crc = crc32c::crc32c(&output[start + 21..]);
            output[start + 17..start + 21].copy_from_slice(&crc.to_be_bytes());
            self.retained += kept;
        }
        // Full sparse validation, including changed varint lengths/CRC/extent,
        // precedes file creation. This additional parser work is charged.
        self.charge_entry(output.len())?;
        let validated = records::validate_read(&output, self.validation_limits(limits))
            .map_err(Self::record_error)?;
        self.parsed_records(validated.record_count())?;
        let count = validated.record_count();
        Ok((output, count))
    }
    fn measure(
        &mut self,
        checked: records::ReadSummary<'_>,
        maximum: usize,
    ) -> Result<usize, Error> {
        let mut total = 0usize;
        for batch in checked.batches() {
            let batch = batch?;
            let (mut kept, mut tombstone, mut first) = (0usize, false, None);
            self.parsed_records(batch.record_count as usize)?;
            for record in batch.records() {
                let record = record?;
                if self.keep(record, batch.delete_horizon_ms)? {
                    kept += 1;
                    tombstone |= record.value.is_none();
                    if first.is_none() {
                        first = Some(record.timestamp);
                    }
                }
            }
            let unchanged = kept > 0
                && kept == batch.record_count as usize
                && (!tombstone || batch.delete_horizon_ms.is_some());
            let mut size = if unchanged {
                batch.bytes.len()
            } else if kept == 0 {
                if self.last_batch == Some((batch.base_offset, batch.next_offset)) {
                    61
                } else {
                    0
                }
            } else {
                61
            };
            if kept > 0 && !unchanged {
                let base_time = batch
                    .delete_horizon_ms
                    .or_else(|| tombstone.then_some(self.horizon))
                    .or(first)
                    .ok_or(Error::InvalidClock)?;
                self.parsed_records(batch.record_count as usize)?;
                for record in batch.records() {
                    let record = record?;
                    if !self.keep(record, batch.delete_horizon_ms)? {
                        continue;
                    }
                    let delta = record
                        .timestamp
                        .checked_sub(base_time)
                        .ok_or(Error::InvalidClock)?;
                    let mut bytes = [0; 10];
                    let n = encode_varlong(delta, &mut bytes);
                    let body = 1usize
                        .checked_add(n)
                        .and_then(|n| n.checked_add(record.suffix.len()))
                        .and_then(|n| i32::try_from(n).ok())
                        .ok_or(Error::BudgetExceeded(Budget::OutputBytes))?;
                    let prefix = encode_varlong(i64::from(body), &mut bytes);
                    size = size
                        .checked_add(prefix + body as usize)
                        .ok_or(Error::BudgetExceeded(Budget::OutputBytes))?;
                }
            }
            total = total
                .checked_add(size)
                .ok_or(Error::BudgetExceeded(Budget::OutputBytes))?;
            if total > maximum {
                return Err(Error::BudgetExceeded(Budget::OutputBytes));
            }
        }
        Ok(total)
    }
    fn extend(&self, out: &mut Vec<u8>, bytes: &[u8], maximum: usize) -> Result<(), Error> {
        let end = out
            .len()
            .checked_add(bytes.len())
            .ok_or(Error::BudgetExceeded(Budget::OutputBytes))?;
        if end > maximum || end > out.capacity() {
            return Err(Error::BudgetExceeded(Budget::OutputBytes));
        }
        out.extend_from_slice(bytes);
        Ok(())
    }
}
const CLEAN_MAGIC: &[u8; 8] = b"PLCMP01\0";
const CLEAN_APPEND: u8 = 1;
const CLEAN_DECIDE: u8 = 2;
const CLEAN_PLAN: u8 = 3;
const CLEAN_COMMIT: u8 = 4;
const NULL_FIELD: u16 = u16::MAX;

/// Why a classified-record cleaning swap was refused.
#[derive(Debug)]
pub enum CleanError {
    /// The policy or journal cannot store a cleaning round.
    InvalidLimits,
    /// The clock or a tombstone horizon is not a valid timestamp.
    InvalidClock,
    /// The record, key, or decision is not usable.
    InvalidRecord,
    /// This producer epoch already has the opposite outcome.
    ConflictingDecision,
    /// `publish` named a generation that is not the staged plan.
    UnknownGeneration,
    /// The round exceeded a configured budget before publication.
    BudgetExceeded(Budget),
    /// A failed append left this cleaner unable to confirm further writes.
    Poisoned,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl From<journal::Error> for CleanError {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// How a classified record participates in cleaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Ordinary or idempotent data. A producer id on this role is preserved.
    Ordinary,
    /// Application data that belongs to a producer epoch.
    TransactionData,
    /// Commit marker for a producer epoch.
    ControlCommit,
    /// Abort marker for a producer epoch.
    ControlAbort,
}

/// One record supplied to [`Cleaner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Input<'a> {
    /// Log offset. Offsets must be strictly increasing.
    pub offset: i64,
    /// Record timestamp, in milliseconds.
    pub timestamp: i64,
    /// Producer id. Zero with epoch `-1` means no producer identity.
    pub producer_id: i64,
    /// Producer epoch. `-1` only with a zero producer id.
    pub epoch: i16,
    /// Cleaning role.
    pub role: Role,
    /// Record key. `None` is a null key.
    pub key: Option<&'a [u8]>,
    /// Record value. `None` is a tombstone.
    pub value: Option<&'a [u8]>,
}

/// One record in the published cleaned view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retained {
    /// Log offset.
    pub offset: i64,
    /// Record timestamp, in milliseconds.
    pub timestamp: i64,
    /// Producer id preserved from the input.
    pub producer_id: i64,
    /// Producer epoch preserved from the input.
    pub epoch: i16,
    /// Cleaning role.
    pub role: Role,
    /// Record key.
    pub key: Option<Vec<u8>>,
    /// Record value. `None` is a tombstone.
    pub value: Option<Vec<u8>>,
}

/// One marker for the transactional records already appended in an epoch.
struct EpochDecision {
    producer_id: i64,
    epoch: i16,
    commit: bool,
    /// Source length when the marker was recorded. It covers earlier indexes.
    through: usize,
}

/// Durable plan-then-commit cleaner for classified records.
///
/// Records before the first undecided transaction are eligible. A decision
/// covers only the transactional records already appended for that producer
/// epoch; a later transaction in the same epoch stays undecided. Aborted data
/// and resolved control markers in the stable prefix are removed. The last
/// remaining value of each key in that prefix is kept, and a last tombstone
/// stays until its delete horizon. The undecided suffix is copied unchanged.
/// A plan is not the published view until `publish` records the same generation.
pub struct Cleaner {
    policy: Policy,
    source: Vec<Retained>,
    decisions: Vec<EpochDecision>,
    pending: Option<(u64, Vec<Retained>)>,
    published: Option<Vec<Retained>>,
    published_generation: Option<u64>,
    next_generation: u64,
    journal: Journal,
    journal_limits: journal::Limits,
    poisoned: bool,
}

impl Cleaner {
    /// Open or recover a cleaner. An uncommitted plan is discarded.
    pub fn open(
        path: impl AsRef<std::path::Path>,
        policy: Policy,
        journal_limits: journal::Limits,
    ) -> Result<Self, CleanError> {
        if journal_limits.max_entry_bytes() < 64
            || journal_limits.max_index_entries() < policy.limits.records
        {
            return Err(CleanError::InvalidLimits);
        }
        let (journal, _) = Journal::open(path, 0, journal_limits)?;
        let mut cleaner = Self {
            policy,
            source: Vec::new(),
            decisions: Vec::new(),
            pending: None,
            published: None,
            published_generation: None,
            next_generation: 1,
            journal,
            journal_limits,
            poisoned: false,
        };
        let mut cursor = cleaner.journal.base_offset();
        loop {
            let batch =
                cleaner
                    .journal
                    .fetch(cursor, 32, cleaner.journal_limits.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(CleanError::InvalidRecord);
                }
                cleaner.replay(&entry.payload)?;
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(CleanError::InvalidRecord)?;
            }
        }
        cleaner.pending = None;
        Ok(cleaner)
    }

    /// Append one classified record.
    pub fn append(&mut self, input: Input<'_>) -> Result<(), CleanError> {
        self.ready()?;
        if self.source.len() >= self.policy.limits.records {
            return Err(CleanError::BudgetExceeded(Budget::Records));
        }
        let record = validate_input(&self.source, input)?;
        self.persist(&encode_append(&record)?)?;
        self.source.push(record);
        Ok(())
    }

    /// Record that the current transaction in a producer epoch committed or aborted.
    ///
    /// The marker applies to transactional records already appended for that
    /// epoch. The same marker with no newer records is a no-op. The opposite
    /// marker stores nothing. Records appended afterward stay undecided.
    pub fn decide(&mut self, producer_id: i64, epoch: i16, commit: bool) -> Result<(), CleanError> {
        self.ready()?;
        if producer_id <= 0 || epoch < 0 {
            return Err(CleanError::InvalidRecord);
        }
        if let Some(stored) = self.trailing_commit(producer_id, epoch) {
            if stored != commit {
                return Err(CleanError::ConflictingDecision);
            }
            return Ok(());
        }
        self.persist(&encode_decide(producer_id, epoch, commit)?)?;
        self.decisions.push(EpochDecision {
            producer_id,
            epoch,
            commit,
            through: self.source.len(),
        });
        Ok(())
    }

    /// Write the next cleaned generation without publishing it.
    pub fn stage(&mut self, now_ms: i64) -> Result<u64, CleanError> {
        self.ready()?;
        if now_ms < 0 {
            return Err(CleanError::InvalidClock);
        }
        let generation = self.next_generation;
        let retained = plan(&self.source, &self.decisions, self.policy, now_ms)?;
        let payload = encode_plan(generation, &retained)?;
        if payload.len() > self.policy.limits.scratch_bytes {
            return Err(CleanError::BudgetExceeded(Budget::Scratch));
        }
        if payload.len() > self.journal_limits.max_entry_bytes() {
            return Err(CleanError::BudgetExceeded(Budget::OutputBytes));
        }
        self.persist(&payload)?;
        self.pending = Some((generation, retained));
        self.next_generation = generation.saturating_add(1);
        Ok(generation)
    }

    /// Publish a staged generation. Repeating the published generation is a no-op.
    pub fn publish(&mut self, generation: u64) -> Result<(), CleanError> {
        self.ready()?;
        if self.published_generation == Some(generation)
            && self
                .pending
                .as_ref()
                .is_none_or(|(pending, _)| *pending != generation)
        {
            return Ok(());
        }
        let Some((pending, retained)) = self.pending.take() else {
            return Err(CleanError::UnknownGeneration);
        };
        if pending != generation {
            self.pending = Some((pending, retained));
            return Err(CleanError::UnknownGeneration);
        }
        if let Err(error) = self.persist(&encode_commit(generation)?) {
            self.pending = Some((pending, retained));
            return Err(error);
        }
        self.published = Some(retained);
        self.published_generation = Some(generation);
        Ok(())
    }

    /// Stage and publish one generation.
    pub fn compact(&mut self, now_ms: i64) -> Result<u64, CleanError> {
        let generation = self.stage(now_ms)?;
        self.publish(generation)?;
        Ok(generation)
    }

    /// Published records, or the original source when nothing has been published.
    pub fn retained(&self) -> &[Retained] {
        self.published.as_deref().unwrap_or(&self.source)
    }

    fn ready(&self) -> Result<(), CleanError> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(CleanError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), CleanError> {
        match self.journal.append(1, payload) {
            Ok(_) => Ok(()),
            Err(error) => {
                self.poisoned = true;
                Err(error.into())
            }
        }
    }

    fn replay(&mut self, payload: &[u8]) -> Result<(), CleanError> {
        match decode_kind(payload)? {
            CleanRecord::Append(record) => {
                accepted_record(self.source.last(), &record)?;
                self.source.push(record);
            }
            CleanRecord::Decide {
                producer_id,
                epoch,
                commit,
            } => {
                if producer_id <= 0 || epoch < 0 {
                    return Err(CleanError::InvalidRecord);
                }
                if let Some(stored) = self.trailing_commit(producer_id, epoch) {
                    if stored != commit {
                        return Err(CleanError::InvalidRecord);
                    }
                } else {
                    self.decisions.push(EpochDecision {
                        producer_id,
                        epoch,
                        commit,
                        through: self.source.len(),
                    });
                }
            }
            CleanRecord::Plan {
                generation,
                records,
            } => {
                if generation != self.next_generation {
                    return Err(CleanError::InvalidRecord);
                }
                plan_matches_source(&self.source, &self.decisions, self.policy, &records)?;
                let next = generation.checked_add(1).ok_or(CleanError::InvalidRecord)?;
                self.pending = Some((generation, records));
                self.next_generation = next;
            }
            CleanRecord::Commit { generation } => {
                let Some((pending, retained)) = self.pending.take() else {
                    return Err(CleanError::InvalidRecord);
                };
                if pending != generation {
                    self.pending = Some((pending, retained));
                    return Err(CleanError::InvalidRecord);
                }
                self.published = Some(retained);
                self.published_generation = Some(generation);
            }
        }
        Ok(())
    }

    /// Commit flag of the marker that already closed this epoch, when no later
    /// transactional record has been appended for it.
    fn trailing_commit(&self, producer_id: i64, epoch: i16) -> Option<bool> {
        let span = self
            .decisions
            .iter()
            .rev()
            .find(|decision| decision.producer_id == producer_id && decision.epoch == epoch)?;
        let open = self.source.iter().enumerate().any(|(index, record)| {
            index >= span.through
                && record.producer_id == producer_id
                && record.epoch == epoch
                && record.role != Role::Ordinary
        });
        if open {
            None
        } else {
            Some(span.commit)
        }
    }
}

enum CleanRecord {
    Append(Retained),
    Decide {
        producer_id: i64,
        epoch: i16,
        commit: bool,
    },
    Plan {
        generation: u64,
        records: Vec<Retained>,
    },
    Commit {
        generation: u64,
    },
}

/// A stored plan is the subsequence [`plan`] emits: every undecided suffix
/// record, every latest non-tombstone key, and only those latest tombstones
/// whose horizon fits in `i64`. Tombstones may be absent because the staging
/// clock is not stored.
fn plan_matches_source(
    source: &[Retained],
    decisions: &[EpochDecision],
    policy: Policy,
    records: &[Retained],
) -> Result<(), CleanError> {
    let lso = source
        .iter()
        .enumerate()
        .position(|(index, record)| {
            transactional(record) && decision(decisions, record, index).is_none()
        })
        .unwrap_or(source.len());
    let mut last = std::collections::BTreeMap::<&[u8], i64>::new();
    for (index, record) in source.iter().enumerate().take(lso) {
        if record.role != Role::Ordinary && record.role != Role::TransactionData {
            continue;
        }
        if decision(decisions, record, index) == Some(false) {
            continue;
        }
        if let Some(key) = record.key.as_deref() {
            last.insert(key, record.offset);
        }
    }
    let mut planned = records.iter();
    let mut next = planned.next();
    for (index, record) in source.iter().enumerate() {
        let latest = record
            .key
            .as_deref()
            .is_some_and(|key| last.get(key) == Some(&record.offset))
            && (record.role == Role::Ordinary || record.role == Role::TransactionData)
            && decision(decisions, record, index) != Some(false);
        if index >= lso || (latest && record.value.is_some()) {
            if next != Some(record) {
                return Err(CleanError::InvalidRecord);
            }
            next = planned.next();
        } else if latest {
            let retention =
                i64::try_from(policy.delete_retention_ms).map_err(|_| CleanError::InvalidClock)?;
            record
                .timestamp
                .checked_add(retention)
                .ok_or(CleanError::InvalidClock)?;
            if next == Some(record) {
                next = planned.next();
            }
        } else if next == Some(record) {
            return Err(CleanError::InvalidRecord);
        }
    }
    if next.is_some() {
        return Err(CleanError::InvalidRecord);
    }
    Ok(())
}

fn plan(
    source: &[Retained],
    decisions: &[EpochDecision],
    policy: Policy,
    now_ms: i64,
) -> Result<Vec<Retained>, CleanError> {
    let limits = policy.limits;
    if source.len() > limits.scan_entries || source.len() > limits.records {
        return Err(CleanError::BudgetExceeded(Budget::ScanEntries));
    }
    let mut scan_bytes = 0u64;
    for record in source {
        let key = record.key.as_ref().map_or(0, Vec::len);
        let value = record.value.as_ref().map_or(0, Vec::len);
        scan_bytes = scan_bytes
            .saturating_add(u64::try_from(key).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(value).unwrap_or(u64::MAX));
        if scan_bytes > limits.scan_bytes {
            return Err(CleanError::BudgetExceeded(Budget::ScanBytes));
        }
    }
    let lso = source
        .iter()
        .enumerate()
        .position(|(index, record)| {
            transactional(record) && decision(decisions, record, index).is_none()
        })
        .unwrap_or(source.len());
    let mut last = std::collections::BTreeMap::<Vec<u8>, i64>::new();
    let mut key_bytes = 0usize;
    for (index, record) in source.iter().enumerate() {
        if index >= lso {
            break;
        }
        if record.role != Role::Ordinary && record.role != Role::TransactionData {
            continue;
        }
        if decision(decisions, record, index) == Some(false) {
            continue;
        }
        let Some(key) = record.key.clone() else {
            continue;
        };
        if let Some(offset) = last.get_mut(key.as_slice()) {
            *offset = record.offset;
            continue;
        }
        if last.len() >= limits.keys {
            return Err(CleanError::BudgetExceeded(Budget::Keys));
        }
        key_bytes = key_bytes
            .checked_add(key.len())
            .ok_or(CleanError::BudgetExceeded(Budget::KeyBytes))?;
        if key_bytes > limits.key_bytes {
            return Err(CleanError::BudgetExceeded(Budget::KeyBytes));
        }
        last.insert(key, record.offset);
    }
    let mut retained = Vec::new();
    for (index, record) in source.iter().enumerate() {
        if index >= lso {
            retained.push(record.clone());
            continue;
        }
        if record.role == Role::ControlCommit || record.role == Role::ControlAbort {
            continue;
        }
        if decision(decisions, record, index) == Some(false) {
            continue;
        }
        let Some(key) = &record.key else {
            continue;
        };
        if last.get(key) != Some(&record.offset) {
            continue;
        }
        if record.value.is_none() && !tombstone_visible(record.timestamp, policy, now_ms)? {
            continue;
        }
        retained.push(record.clone());
    }
    Ok(retained)
}

fn transactional(record: &Retained) -> bool {
    record.role != Role::Ordinary
}

fn decision(decisions: &[EpochDecision], record: &Retained, index: usize) -> Option<bool> {
    if record.role == Role::Ordinary {
        Some(true)
    } else {
        decisions
            .iter()
            .find(|decision| {
                decision.producer_id == record.producer_id
                    && decision.epoch == record.epoch
                    && index < decision.through
            })
            .map(|decision| decision.commit)
    }
}

fn tombstone_visible(timestamp: i64, policy: Policy, now_ms: i64) -> Result<bool, CleanError> {
    let retention =
        i64::try_from(policy.delete_retention_ms).map_err(|_| CleanError::InvalidClock)?;
    let horizon = timestamp
        .checked_add(retention)
        .ok_or(CleanError::InvalidClock)?;
    Ok(now_ms < horizon)
}

fn validate_input(source: &[Retained], input: Input<'_>) -> Result<Retained, CleanError> {
    if input.offset < 0
        || source
            .last()
            .is_some_and(|last| input.offset <= last.offset)
    {
        return Err(CleanError::InvalidRecord);
    }
    if !producer_role_ok(input.role, input.producer_id, input.epoch) {
        return Err(CleanError::InvalidRecord);
    }
    Ok(Retained {
        offset: input.offset,
        timestamp: input.timestamp,
        producer_id: input.producer_id,
        epoch: input.epoch,
        role: input.role,
        key: field_owned(input.key)?,
        value: field_owned(input.value)?,
    })
}

fn accepted_record(previous: Option<&Retained>, record: &Retained) -> Result<(), CleanError> {
    if record.offset < 0
        || previous.is_some_and(|last| record.offset <= last.offset)
        || !producer_role_ok(record.role, record.producer_id, record.epoch)
    {
        return Err(CleanError::InvalidRecord);
    }
    Ok(())
}

fn producer_role_ok(role: Role, producer_id: i64, epoch: i16) -> bool {
    match role {
        Role::Ordinary => (producer_id == 0 && epoch == -1) || (producer_id > 0 && epoch >= 0),
        Role::TransactionData | Role::ControlCommit | Role::ControlAbort => {
            producer_id > 0 && epoch >= 0
        }
    }
}

fn field_owned(field: Option<&[u8]>) -> Result<Option<Vec<u8>>, CleanError> {
    let Some(bytes) = field else {
        return Ok(None);
    };
    if bytes.len() >= usize::from(NULL_FIELD) {
        return Err(CleanError::InvalidRecord);
    }
    Ok(Some(bytes.to_vec()))
}

fn encode_append(record: &Retained) -> Result<Vec<u8>, CleanError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CLEAN_MAGIC);
    bytes.push(CLEAN_APPEND);
    put_record(&mut bytes, record)?;
    Ok(bytes)
}

fn encode_decide(producer_id: i64, epoch: i16, commit: bool) -> Result<Vec<u8>, CleanError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CLEAN_MAGIC);
    bytes.push(CLEAN_DECIDE);
    bytes.extend_from_slice(&producer_id.to_be_bytes());
    bytes.extend_from_slice(&epoch.to_be_bytes());
    bytes.push(u8::from(commit));
    Ok(bytes)
}

fn encode_plan(generation: u64, records: &[Retained]) -> Result<Vec<u8>, CleanError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CLEAN_MAGIC);
    bytes.push(CLEAN_PLAN);
    bytes.extend_from_slice(&generation.to_be_bytes());
    let count = u32::try_from(records.len()).map_err(|_| CleanError::InvalidRecord)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for record in records {
        put_record(&mut bytes, record)?;
    }
    Ok(bytes)
}

fn encode_commit(generation: u64) -> Result<Vec<u8>, CleanError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CLEAN_MAGIC);
    bytes.push(CLEAN_COMMIT);
    bytes.extend_from_slice(&generation.to_be_bytes());
    Ok(bytes)
}

fn put_record(bytes: &mut Vec<u8>, record: &Retained) -> Result<(), CleanError> {
    bytes.extend_from_slice(&record.offset.to_be_bytes());
    bytes.extend_from_slice(&record.timestamp.to_be_bytes());
    bytes.extend_from_slice(&record.producer_id.to_be_bytes());
    bytes.extend_from_slice(&record.epoch.to_be_bytes());
    bytes.push(role_byte(record.role));
    put_field(bytes, record.key.as_deref())?;
    put_field(bytes, record.value.as_deref())?;
    Ok(())
}

fn role_byte(role: Role) -> u8 {
    match role {
        Role::Ordinary => 0,
        Role::TransactionData => 1,
        Role::ControlCommit => 2,
        Role::ControlAbort => 3,
    }
}

fn role_from(byte: u8) -> Result<Role, CleanError> {
    match byte {
        0 => Ok(Role::Ordinary),
        1 => Ok(Role::TransactionData),
        2 => Ok(Role::ControlCommit),
        3 => Ok(Role::ControlAbort),
        _ => Err(CleanError::InvalidRecord),
    }
}

fn put_field(bytes: &mut Vec<u8>, field: Option<&[u8]>) -> Result<(), CleanError> {
    let Some(field) = field else {
        bytes.extend_from_slice(&NULL_FIELD.to_be_bytes());
        return Ok(());
    };
    let len = u16::try_from(field.len()).map_err(|_| CleanError::InvalidRecord)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(field);
    Ok(())
}

fn decode_kind(mut bytes: &[u8]) -> Result<CleanRecord, CleanError> {
    if bytes.len() < CLEAN_MAGIC.len() + 1 || &bytes[..CLEAN_MAGIC.len()] != CLEAN_MAGIC {
        return Err(CleanError::InvalidRecord);
    }
    let kind = bytes[CLEAN_MAGIC.len()];
    bytes = &bytes[CLEAN_MAGIC.len() + 1..];
    let record = match kind {
        CLEAN_APPEND => CleanRecord::Append(take_record(&mut bytes)?),
        CLEAN_DECIDE => CleanRecord::Decide {
            producer_id: take_i64(&mut bytes)?,
            epoch: take_i16(&mut bytes)?,
            commit: match take_u8(&mut bytes)? {
                0 => false,
                1 => true,
                _ => return Err(CleanError::InvalidRecord),
            },
        },
        CLEAN_PLAN => {
            let generation = take_u64(&mut bytes)?;
            let count = take_u32(&mut bytes)?;
            let mut records = Vec::new();
            for _ in 0..count {
                records.push(take_record(&mut bytes)?);
            }
            CleanRecord::Plan {
                generation,
                records,
            }
        }
        CLEAN_COMMIT => CleanRecord::Commit {
            generation: take_u64(&mut bytes)?,
        },
        _ => return Err(CleanError::InvalidRecord),
    };
    if !bytes.is_empty() {
        return Err(CleanError::InvalidRecord);
    }
    Ok(record)
}

fn take_record(bytes: &mut &[u8]) -> Result<Retained, CleanError> {
    Ok(Retained {
        offset: take_i64(bytes)?,
        timestamp: take_i64(bytes)?,
        producer_id: take_i64(bytes)?,
        epoch: take_i16(bytes)?,
        role: role_from(take_u8(bytes)?)?,
        key: take_field(bytes)?,
        value: take_field(bytes)?,
    })
}

fn take_field(bytes: &mut &[u8]) -> Result<Option<Vec<u8>>, CleanError> {
    let len = u16::from_be_bytes(
        take(bytes, 2)?
            .try_into()
            .map_err(|_| CleanError::InvalidRecord)?,
    );
    if len == NULL_FIELD {
        return Ok(None);
    }
    Ok(Some(take(bytes, usize::from(len))?.to_vec()))
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], CleanError> {
    if input.len() < len {
        return Err(CleanError::InvalidRecord);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_i64(input: &mut &[u8]) -> Result<i64, CleanError> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(i64::from_be_bytes(value))
}

fn take_u64(input: &mut &[u8]) -> Result<u64, CleanError> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(u64::from_be_bytes(value))
}

fn take_u32(input: &mut &[u8]) -> Result<u32, CleanError> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(u32::from_be_bytes(value))
}

fn take_i16(input: &mut &[u8]) -> Result<i16, CleanError> {
    let mut value = [0u8; 2];
    value.copy_from_slice(take(input, 2)?);
    Ok(i16::from_be_bytes(value))
}

fn take_u8(input: &mut &[u8]) -> Result<u8, CleanError> {
    Ok(take(input, 1)?[0])
}

fn encode_varlong(value: i64, out: &mut [u8; 10]) -> usize {
    let mut value = ((value as u64) << 1) ^ ((value >> 63) as u64);
    let mut at = 0;
    while value >= 128 {
        out[at] = (value as u8) | 128;
        value >>= 7;
        at += 1;
    }
    out[at] = value as u8;
    at + 1
}

#[cfg(test)]
mod cleaner_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn policy(retention: u64, scratch: usize) -> Policy {
        let limits = Limits::new(64 * 1024, 100, 100, 16, 4096, 1000, 4, scratch)
            .unwrap_or_else(|_| panic!("limits"));
        Policy::new(retention, limits).unwrap_or_else(|_| panic!("policy"))
    }

    fn open_cleaner(name: &str, retention: u64, scratch: usize) -> (Cleaner, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-cmp-{name}-{nanos}"));
        let journal = journal::Limits::new(4096, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        let cleaner = Cleaner::open(&path, policy(retention, scratch), journal)
            .unwrap_or_else(|error| panic!("{error:?}"));
        (cleaner, path)
    }

    fn record<'a>(
        offset: i64,
        timestamp: i64,
        producer_id: i64,
        epoch: i16,
        role: Role,
        key: Option<&'a [u8]>,
        value: Option<&'a [u8]>,
    ) -> Input<'a> {
        Input {
            offset,
            timestamp,
            producer_id,
            epoch,
            role,
            key,
            value,
        }
    }

    #[test]
    fn a_plan_that_is_not_the_cleaned_source_is_not_recovered() {
        let (mut cleaner, _) = open_cleaner("bad-plan", 5_000, 4096);
        cleaner
            .append(record(0, 0, 0, -1, Role::Ordinary, Some(b"a"), Some(b"v")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let foreign = Retained {
            offset: 9,
            timestamp: 0,
            producer_id: 0,
            epoch: -1,
            role: Role::Ordinary,
            key: Some(b"z".to_vec()),
            value: Some(b"nope".to_vec()),
        };
        let encoded = encode_plan(1, &[foreign]).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            cleaner.replay(&encoded),
            Err(CleanError::InvalidRecord)
        ));
        let dropped = encode_plan(1, &[]).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            cleaner.replay(&dropped),
            Err(CleanError::InvalidRecord)
        ));
        let commit = encode_commit(1).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            cleaner.replay(&commit),
            Err(CleanError::InvalidRecord)
        ));
        assert_eq!(cleaner.retained().len(), 1);
        assert_eq!(cleaner.retained()[0].offset, 0);
    }

    #[test]
    fn a_transactional_record_without_a_producer_is_not_replayed() {
        let (mut cleaner, _) = open_cleaner("bad-producer", 5_000, 4096);
        let bad = Retained {
            offset: 0,
            timestamp: 0,
            producer_id: 0,
            epoch: -1,
            role: Role::TransactionData,
            key: None,
            value: Some(b"x".to_vec()),
        };
        let encoded = encode_append(&bad).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            cleaner.replay(&encoded),
            Err(CleanError::InvalidRecord)
        ));
        assert!(cleaner.retained().is_empty());
        let decided = encode_decide(0, 0, true).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            cleaner.replay(&decided),
            Err(CleanError::InvalidRecord)
        ));
    }

    #[test]
    fn transactional_prefix_tombstones_and_interrupted_swap() {
        let (mut cleaner, path) = open_cleaner("swap", 5_000, 4096);
        cleaner
            .append(record(0, 0, 0, -1, Role::Ordinary, None, Some(b"null")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .append(record(
                1,
                0,
                0,
                -1,
                Role::Ordinary,
                Some(b"a"),
                Some(b"old"),
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .append(record(
                2,
                1_000,
                9,
                1,
                Role::Ordinary,
                Some(b"a"),
                Some(b"new"),
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .append(record(3, 1_000, 0, -1, Role::Ordinary, Some(b"c"), None))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .append(record(
                4,
                1_000,
                4,
                0,
                Role::TransactionData,
                Some(b"b"),
                Some(b"gone"),
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .append(record(5, 1_000, 4, 0, Role::ControlAbort, None, None))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .decide(4, 0, false)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            cleaner.decide(4, 0, true),
            Err(CleanError::ConflictingDecision)
        ));
        cleaner
            .append(record(
                6,
                2_000,
                4,
                1,
                Role::TransactionData,
                Some(b"d"),
                Some(b"open"),
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .compact(2_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let kept: Vec<_> = cleaner
            .retained()
            .iter()
            .map(|record| record.offset)
            .collect();
        assert_eq!(kept, vec![2, 3, 6]);
        assert_eq!(cleaner.retained()[0].producer_id, 9);
        assert_eq!(cleaner.retained()[0].value.as_deref(), Some(&b"new"[..]));
        assert!(cleaner
            .retained()
            .iter()
            .all(|record| record.value.as_deref() != Some(&b"gone"[..])));
        let staged = cleaner
            .stage(7_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        drop(cleaner);
        let restored = Cleaner::open(&path, policy(5_000, 4096), journal_limits())
            .unwrap_or_else(|error| panic!("{error:?}"));
        let restored_offsets: Vec<_> = restored
            .retained()
            .iter()
            .map(|record| record.offset)
            .collect();
        assert_eq!(restored_offsets, vec![2, 3, 6]);
        drop(restored);
        let (mut cleaner, _) = open_at(&path);
        assert!(matches!(
            cleaner.publish(staged),
            Err(CleanError::UnknownGeneration)
        ));
        cleaner
            .compact(7_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let generation = cleaner
            .compact(7_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .publish(generation)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let final_offsets: Vec<_> = cleaner
            .retained()
            .iter()
            .map(|record| record.offset)
            .collect();
        assert_eq!(final_offsets, vec![2, 6]);
        assert!(cleaner.retained().iter().all(|record| record.offset != 3));

        let (mut bounded, _) = open_cleaner("scratch", 5_000, 1);
        bounded
            .append(record(0, 0, 0, -1, Role::Ordinary, Some(b"a"), Some(b"v")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            bounded.stage(1),
            Err(CleanError::BudgetExceeded(Budget::Scratch))
        ));
        assert_eq!(bounded.retained().len(), 1);
        assert_eq!(bounded.retained()[0].value.as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn a_later_transaction_in_the_same_epoch_stays_undecided() {
        let (mut cleaner, path) = open_cleaner("same-epoch", 5_000, 4096);
        cleaner
            .append(record(
                0,
                1_000,
                4,
                0,
                Role::TransactionData,
                Some(b"a"),
                Some(b"committed"),
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .decide(4, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .append(record(
                1,
                1_000,
                4,
                0,
                Role::TransactionData,
                Some(b"a"),
                Some(b"open"),
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .compact(2_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let kept: Vec<_> = cleaner
            .retained()
            .iter()
            .map(|record| record.offset)
            .collect();
        assert_eq!(kept, vec![0, 1]);
        cleaner
            .decide(4, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        cleaner
            .decide(4, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            cleaner.decide(4, 0, false),
            Err(CleanError::ConflictingDecision)
        ));
        cleaner
            .compact(2_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            cleaner
                .retained()
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(cleaner.retained()[0].value.as_deref(), Some(&b"open"[..]));
        drop(cleaner);
        let restored = Cleaner::open(&path, policy(5_000, 4096), journal_limits())
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            restored
                .retained()
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    fn journal_limits() -> journal::Limits {
        journal::Limits::new(4096, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"))
    }

    fn open_at(path: &std::path::Path) -> (Cleaner, ()) {
        let cleaner = Cleaner::open(path, policy(5_000, 4096), journal_limits())
            .unwrap_or_else(|error| panic!("{error:?}"));
        (cleaner, ())
    }
}
