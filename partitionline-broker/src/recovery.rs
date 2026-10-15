//! Leader-epoch recovery and bounded replica truncation.
//!
//! `OffsetForLeaderEpoch` returns the exclusive end offset of a stored epoch:
//! the first offset written by a later epoch, or the log end when that epoch
//! is still current. A failover may truncate only the divergent suffix at or
//! above the high watermark. Committed offsets stay, and the high watermark
//! never advances over a record that has been truncated.
//!
//! Confirmed appends, truncations, and watermark advances are journaled.
//! This is not the OffsetForLeaderEpoch RPC and it does not replicate bytes.

use crate::journal::{self, Journal};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLRCVR01";
const KIND_RECORD: u8 = 1;
const KIND_TRUNCATE: u8 = 2;
const KIND_WATERMARK: u8 = 3;

/// Kafka `OFFSET_OUT_OF_RANGE`.
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
/// Kafka `FENCED_LEADER_EPOCH`.
pub const FENCED_LEADER_EPOCH: i16 = 74;
/// Kafka `UNKNOWN_LEADER_EPOCH`.
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;

/// One recovered log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Log offset.
    pub offset: i64,
    /// Epoch that wrote the record.
    pub leader_epoch: i32,
    /// Producer id. Committed ids are those with `offset < high_watermark`.
    pub id: u64,
}

/// Why recovery was refused.
#[derive(Debug)]
pub enum Error {
    /// The requested epoch is not in this log.
    UnknownEpoch,
    /// The offset is outside this log.
    OffsetOutOfRange,
    /// Truncation would remove a committed offset.
    BelowHighWatermark,
    /// The new epoch is older than the current epoch.
    FencedEpoch,
    /// A failed append left this log unable to confirm further recovery.
    Poisoned,
    /// The limits cannot store the log.
    InvalidLimits,
    /// A recovered record is not usable.
    InvalidRecord,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a replica-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::UnknownEpoch => UNKNOWN_LEADER_EPOCH,
            Self::OffsetOutOfRange => OFFSET_OUT_OF_RANGE,
            Self::FencedEpoch => FENCED_LEADER_EPOCH,
            Self::BelowHighWatermark
            | Self::Poisoned
            | Self::InvalidLimits
            | Self::InvalidRecord
            | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Log and journal bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a positive record cap. The journal must index it.
    pub fn new(max_records: usize, journal: journal::Limits) -> Result<Self, Error> {
        if !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < 32
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_records,
            journal,
        })
    }
}

/// Durable epoch log for one replica.
pub struct EpochLog {
    limits: Limits,
    records: Vec<Record>,
    high_watermark: i64,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl EpochLog {
    /// Open or recover an epoch log. The high watermark starts at 0.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut log = Self {
            limits,
            records: Vec::new(),
            high_watermark: 0,
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut offset = log.journal.base_offset();
        loop {
            let batch = log
                .journal
                .fetch(offset, 64, log.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRecord);
                }
                log.apply(&entry.payload)?;
                log.stored = log.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRecord)?;
            }
        }
        Ok(log)
    }

    /// Exclusive log end.
    pub fn log_end_offset(&self) -> i64 {
        self.records
            .last()
            .map(|record| record.offset.saturating_add(1))
            .unwrap_or(0)
    }

    /// Offsets strictly below this value are committed and visible.
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Confirmed records, including those not yet committed.
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Producer ids that are still below the high watermark.
    pub fn committed_ids(&self) -> Vec<u64> {
        self.records
            .iter()
            .filter(|record| record.offset < self.high_watermark)
            .map(|record| record.id)
            .collect()
    }

    /// Append one record at the log end.
    pub fn append(&mut self, leader_epoch: i32, id: u64) -> Result<i64, Error> {
        self.ready()?;
        if leader_epoch < 0 {
            return Err(Error::InvalidRecord);
        }
        if let Some(last) = self.records.last() {
            if leader_epoch < last.leader_epoch {
                return Err(Error::FencedEpoch);
            }
        }
        if self.records.len() >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        // The exclusive end must fit in `i64`. An offset of `i64::MAX` would
        // make that end wrap and hide the record from the high watermark.
        let offset = match self.records.last() {
            Some(last) => last
                .offset
                .checked_add(1)
                .filter(|next| next.checked_add(1).is_some())
                .ok_or(Error::OffsetOutOfRange)?,
            None => 0,
        };
        let record = Record {
            offset,
            leader_epoch,
            id,
        };
        self.persist(&encode_record(&record)?)?;
        self.records.push(record);
        Ok(offset)
    }

    /// Advance the high watermark. It cannot pass the log end or move backward.
    pub fn advance_high_watermark(&mut self, offset: i64) -> Result<i64, Error> {
        self.ready()?;
        if offset < self.high_watermark || offset > self.log_end_offset() {
            return Err(Error::OffsetOutOfRange);
        }
        if offset == self.high_watermark {
            return Ok(offset);
        }
        self.persist(&encode_watermark(offset)?)?;
        self.high_watermark = offset;
        Ok(offset)
    }

    /// End offset of `leader_epoch`, or the log end when that epoch is current.
    ///
    /// The returned offset is never inside a truncated suffix: truncation
    /// removes those records before this lookup.
    pub fn offset_for_leader_epoch(&self, leader_epoch: i32) -> Result<i64, Error> {
        if leader_epoch < 0 {
            return Err(Error::UnknownEpoch);
        }
        let Some(latest) = self.records.last().map(|record| record.leader_epoch) else {
            return Ok(0);
        };
        if leader_epoch > latest {
            return Err(Error::UnknownEpoch);
        }
        for record in &self.records {
            if record.leader_epoch > leader_epoch {
                return Ok(record.offset);
            }
        }
        Ok(self.log_end_offset())
    }

    /// Truncate the divergent suffix to `offset`.
    ///
    /// Offsets below the high watermark cannot be removed. Records at or above
    /// `offset` disappear from both the log and `OffsetForLeaderEpoch`.
    pub fn truncate_divergent(&mut self, offset: i64) -> Result<usize, Error> {
        self.ready()?;
        if offset < self.high_watermark {
            return Err(Error::BelowHighWatermark);
        }
        if offset > self.log_end_offset() {
            return Err(Error::OffsetOutOfRange);
        }
        let removed = self
            .records
            .iter()
            .filter(|record| record.offset >= offset)
            .count();
        if removed == 0 {
            return Ok(0);
        }
        self.persist(&encode_truncate(offset)?)?;
        self.records.retain(|record| record.offset < offset);
        Ok(removed)
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn apply(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let (kind, rest) = split_kind(bytes)?;
        match kind {
            KIND_RECORD => {
                let record = decode_record(rest)?;
                let expected = match self.records.last() {
                    Some(last) => {
                        if record.leader_epoch < last.leader_epoch {
                            return Err(Error::InvalidRecord);
                        }
                        last.offset.checked_add(1).ok_or(Error::InvalidRecord)?
                    }
                    None => 0,
                };
                if record.offset != expected {
                    return Err(Error::InvalidRecord);
                }
                self.records.push(record);
            }
            KIND_TRUNCATE => {
                let offset = decode_i64(rest)?;
                if offset < 0 || offset < self.high_watermark || offset > self.log_end_offset() {
                    return Err(Error::InvalidRecord);
                }
                self.records.retain(|record| record.offset < offset);
            }
            KIND_WATERMARK => {
                let offset = decode_i64(rest)?;
                if offset < self.high_watermark || offset > self.log_end_offset() {
                    return Err(Error::InvalidRecord);
                }
                self.high_watermark = offset;
            }
            _ => return Err(Error::InvalidRecord),
        }
        Ok(())
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        if self.stored >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        match self.journal.append(1, payload) {
            Ok(_) => {
                self.stored = self.stored.saturating_add(1);
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error.into())
            }
        }
    }
}

fn split_kind(bytes: &[u8]) -> Result<(u8, &[u8]), Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRecord);
    }
    Ok((bytes[RECORD_MAGIC.len()], &bytes[RECORD_MAGIC.len() + 1..]))
}

fn encode_record(record: &Record) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(21);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_RECORD);
    bytes.extend_from_slice(&record.offset.to_be_bytes());
    bytes.extend_from_slice(&record.leader_epoch.to_be_bytes());
    bytes.extend_from_slice(&record.id.to_be_bytes());
    Ok(bytes)
}

fn encode_truncate(offset: i64) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_TRUNCATE);
    bytes.extend_from_slice(&offset.to_be_bytes());
    Ok(bytes)
}

fn encode_watermark(offset: i64) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_WATERMARK);
    bytes.extend_from_slice(&offset.to_be_bytes());
    Ok(bytes)
}

fn decode_record(mut bytes: &[u8]) -> Result<Record, Error> {
    let offset = decode_i64_slice(take(&mut bytes, 8)?)?;
    let leader_epoch = i32::from_be_bytes(
        take(&mut bytes, 4)?
            .try_into()
            .map_err(|_| Error::InvalidRecord)?,
    );
    let id = u64::from_be_bytes(
        take(&mut bytes, 8)?
            .try_into()
            .map_err(|_| Error::InvalidRecord)?,
    );
    // An offset of `i64::MAX` has no exclusive end. The high watermark would
    // stop on that record and `committed_ids` would omit it.
    if !bytes.is_empty() || offset < 0 || offset.checked_add(1).is_none() || leader_epoch < 0 {
        return Err(Error::InvalidRecord);
    }
    Ok(Record {
        offset,
        leader_epoch,
        id,
    })
}

fn decode_i64(bytes: &[u8]) -> Result<i64, Error> {
    if bytes.len() != 8 {
        return Err(Error::InvalidRecord);
    }
    decode_i64_slice(bytes)
}

fn decode_i64_slice(bytes: &[u8]) -> Result<i64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(bytes);
    Ok(i64::from_be_bytes(value))
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidRecord);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(64, 1 << 20, 200, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(64, journal).unwrap_or_else(|_| panic!("recovery limits"))
    }

    fn log(name: &str) -> (EpochLog, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-recovery-{name}-{nanos}"));
        let log = EpochLog::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (log, path)
    }

    #[test]
    fn a_gapped_recovered_record_is_refused() {
        let (mut log, _) = log("gap");
        let record = Record {
            offset: 3,
            leader_epoch: 1,
            id: 9,
        };
        let encoded = encode_record(&record).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(log.apply(&encoded), Err(Error::InvalidRecord)));
        assert_eq!(log.log_end_offset(), 0);
    }

    #[test]
    fn offset_for_leader_epoch_hides_a_truncated_suffix() {
        let (mut log, path) = log("epoch");
        for id in 0..4 {
            log.append(1, id)
                .unwrap_or_else(|error| panic!("{error:?}"));
        }
        log.append(2, 9).unwrap_or_else(|error| panic!("{error:?}"));
        log.advance_high_watermark(4)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.offset_for_leader_epoch(1).unwrap_or(99), 4);
        assert_eq!(log.offset_for_leader_epoch(2).unwrap_or(99), 5);
        assert!(matches!(
            log.truncate_divergent(3),
            Err(Error::BelowHighWatermark)
        ));
        assert_eq!(log.truncate_divergent(4).unwrap_or(99), 1);
        assert!(matches!(
            log.offset_for_leader_epoch(2),
            Err(Error::UnknownEpoch)
        ));
        assert_eq!(log.committed_ids(), vec![0, 1, 2, 3]);
        drop(log);
        let restored = EpochLog::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.high_watermark(), 4);
        assert_eq!(restored.log_end_offset(), 4);
        assert!(matches!(
            restored.offset_for_leader_epoch(2),
            Err(Error::UnknownEpoch)
        ));
    }

    #[test]
    fn an_offset_at_the_end_of_i64_is_not_appended() {
        let (mut log, _) = log("end");
        log.append(1, 1).unwrap_or_else(|error| panic!("{error:?}"));
        log.records[0].offset = i64::MAX - 1;
        assert_eq!(log.log_end_offset(), i64::MAX);
        assert!(matches!(log.append(1, 2), Err(Error::OffsetOutOfRange)));
        assert_eq!(log.records().len(), 1);
        assert_eq!(log.log_end_offset(), i64::MAX);
        log.advance_high_watermark(i64::MAX)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.committed_ids(), vec![1]);
    }

    #[test]
    fn a_record_at_i64_max_is_not_recovered() {
        let encoded = encode_record(&Record {
            offset: i64::MAX,
            leader_epoch: 1,
            id: 7,
        })
        .unwrap_or_else(|error| panic!("{error:?}"));
        let (mut recovered, _) = log("max-offset");
        assert!(matches!(
            recovered.apply(&encoded),
            Err(Error::InvalidRecord)
        ));
        assert!(recovered.records().is_empty());
        assert_eq!(recovered.log_end_offset(), 0);
        assert!(matches!(
            recovered.advance_high_watermark(i64::MAX),
            Err(Error::OffsetOutOfRange)
        ));
    }

    #[test]
    fn seeded_failover_keeps_committed_ids_once() {
        let (mut leader, _) = log("seed-leader");
        let (mut follower, _) = log("seed-follower");
        let mut seed = 0x51_u64;
        for id in 0..8 {
            leader
                .append(1, id)
                .unwrap_or_else(|error| panic!("{error:?}"));
            follower
                .append(1, id)
                .unwrap_or_else(|error| panic!("{error:?}"));
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        }
        leader
            .advance_high_watermark(4)
            .unwrap_or_else(|error| panic!("{error:?}"));
        follower
            .advance_high_watermark(4)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let diverge_at = 4 + i64::from((seed % 3) as u8);
        follower
            .truncate_divergent(diverge_at)
            .unwrap_or_else(|error| panic!("{error:?}"));
        follower
            .append(1, 100 + seed)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let leader_end = leader.offset_for_leader_epoch(1).unwrap_or(99);
        let removed = follower
            .truncate_divergent(diverge_at)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(removed >= 1);
        assert_eq!(
            follower.offset_for_leader_epoch(1).unwrap_or(99),
            diverge_at
        );
        assert!(follower.offset_for_leader_epoch(1).unwrap_or(99) <= leader_end);
        let committed = follower.committed_ids();
        assert_eq!(committed, vec![0, 1, 2, 3]);
        let offsets: BTreeSet<i64> = follower
            .records()
            .iter()
            .map(|record| record.offset)
            .collect();
        assert_eq!(offsets.len(), follower.records().len());
        assert!(follower
            .records()
            .iter()
            .all(|record| record.offset >= follower.high_watermark()
                || committed.contains(&record.id)));
    }
}
