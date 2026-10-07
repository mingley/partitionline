//! Explicit bounded cleaning of ordinary keyed records.
//!
//! The storage owner supplies confirmed/protected offset bounds and serializes
//! cleaning with append/read/retention. Transactions, control batches and producer
//! state remain unsupported. There is no background cleaner or Kafka wire API.

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
