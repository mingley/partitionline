//! Follower replication of a leader partition log.
//!
//! Each record keeps the leader epoch and the exact bytes stored at its
//! offset. A follower copies a bounded prefix of the leader. Where the bytes
//! or epoch diverge, the follower truncates to that offset before copying.
//! A leader epoch older than one the follower has already accepted is fenced
//! and copies nothing.
//!
//! Confirmed records and truncations are journaled. Reopen replays the logical
//! log, so an interrupted copy resumes at the last confirmed offset. This is
//! not a Kafka Fetch response and it does not replicate through the network.

use crate::journal::{self, Journal};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLREPL01";
const KIND_RECORD: u8 = 1;
const KIND_TRUNCATE: u8 = 2;

/// Kafka `FENCED_LEADER_EPOCH`.
pub const FENCED_LEADER_EPOCH: i16 = 74;
/// Kafka `OFFSET_OUT_OF_RANGE`.
pub const OFFSET_OUT_OF_RANGE: i16 = 1;

/// One replicated record. The bytes are the leader's bytes at `offset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Log offset assigned by the leader.
    pub offset: i64,
    /// Leader epoch that wrote this record.
    pub leader_epoch: i32,
    /// Exact payload bytes.
    pub bytes: Vec<u8>,
}

/// How many leader bytes one replication call may copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchBudget {
    /// Maximum payload bytes to copy. Zero is rejected.
    pub max_bytes: usize,
    /// Maximum records to copy. Zero is rejected.
    pub max_records: usize,
}

/// What one replication call changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Records removed because they diverged from the leader.
    pub truncated: usize,
    /// Records copied from the leader.
    pub copied: usize,
    /// Follower end offset after the call.
    pub next_offset: i64,
}

/// Why replication or append failed.
#[derive(Debug)]
pub enum Error {
    /// The offered leader epoch is older than one this follower already stored.
    FencedLeader,
    /// The follower has no record at the requested offset.
    OffsetOutOfRange,
    /// The payload or budget is empty or too large.
    Budget,
    /// A failed append left this log unable to confirm further copies.
    Poisoned,
    /// The limits cannot store one maximum record.
    InvalidLimits,
    /// The leader epoch or payload is not usable.
    InvalidRecord,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a follower-visible refusal. Storage and budget failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::FencedLeader => FENCED_LEADER_EPOCH,
            Self::OffsetOutOfRange => OFFSET_OUT_OF_RANGE,
            Self::Budget
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

/// Record and journal bounds for one replica log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_records: usize,
    max_payload_bytes: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a record cap up to one million and a positive payload cap.
    ///
    /// The journal must accept one maximum record and index every confirmed
    /// append and truncation.
    pub fn new(
        max_records: usize,
        max_payload_bytes: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=1_000_000).contains(&max_records)
            || max_payload_bytes == 0
            || max_payload_bytes > 1024 * 1024
            || journal.max_entry_bytes() < max_payload_bytes.saturating_add(21)
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_records,
            max_payload_bytes,
            journal,
        })
    }
}

/// Durable leader or follower log.
pub struct Log {
    limits: Limits,
    records: Vec<Record>,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Log {
    /// Open or recover a replica log.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut log = Self {
            limits,
            records: Vec::new(),
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
        if log.records.len() > log.limits.max_records
            || log.stored > log.limits.journal.max_index_entries()
        {
            return Err(Error::Budget);
        }
        Ok(log)
    }

    /// Confirmed records in offset order.
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Next offset a new leader append would receive.
    pub fn next_offset(&self) -> i64 {
        self.records
            .last()
            .map(|record| record.offset.saturating_add(1))
            .unwrap_or(0)
    }

    /// Highest leader epoch stored, or -1 when the log is empty.
    pub fn leader_epoch(&self) -> i32 {
        self.records
            .iter()
            .map(|record| record.leader_epoch)
            .max()
            .unwrap_or(-1)
    }

    /// Append one payload at the next offset under `leader_epoch`.
    ///
    /// The epoch must be nonnegative and at least the epoch of the previous
    /// record. This is the leader write path.
    pub fn append(&mut self, leader_epoch: i32, bytes: &[u8]) -> Result<i64, Error> {
        self.ready()?;
        if leader_epoch < 0 || bytes.is_empty() || bytes.len() > self.limits.max_payload_bytes {
            return Err(Error::InvalidRecord);
        }
        if let Some(last) = self.records.last() {
            if leader_epoch < last.leader_epoch {
                return Err(Error::FencedLeader);
            }
        }
        if self.records.len() >= self.limits.max_records {
            return Err(Error::Budget);
        }
        let offset = self.next_offset();
        let record = Record {
            offset,
            leader_epoch,
            bytes: bytes.to_vec(),
        };
        self.persist(&encode_record(&record)?)?;
        self.records.push(record);
        Ok(offset)
    }

    /// Copy a bounded leader prefix, truncating any divergent follower suffix.
    ///
    /// `leader_epoch` is the epoch the caller claims for this leader. If it is
    /// older than an epoch already stored here, the call is fenced and the
    /// follower log is unchanged.
    pub fn replicate_from(
        &mut self,
        leader: &Log,
        leader_epoch: i32,
        budget: FetchBudget,
    ) -> Result<Progress, Error> {
        self.ready()?;
        if budget.max_bytes == 0 || budget.max_records == 0 {
            return Err(Error::Budget);
        }
        if leader_epoch < self.leader_epoch() {
            return Err(Error::FencedLeader);
        }
        let keep = self
            .records
            .iter()
            .zip(leader.records.iter())
            .take_while(|(follower, source)| {
                follower.offset == source.offset
                    && follower.leader_epoch == source.leader_epoch
                    && follower.bytes == source.bytes
            })
            .count();
        let truncated = self.records.len() - keep;
        if truncated > 0 {
            let offset = self
                .records
                .get(keep)
                .map(|record| record.offset)
                .unwrap_or(0);
            self.persist(&encode_truncate(offset)?)?;
            self.records.truncate(keep);
        }
        let mut copied = 0usize;
        let mut used = 0usize;
        while copied < budget.max_records {
            let Some(source) = leader.records.get(self.records.len()) else {
                break;
            };
            if source.bytes.len() > budget.max_bytes.saturating_sub(used) {
                if copied == 0 && truncated == 0 {
                    return Err(Error::Budget);
                }
                break;
            }
            if self.records.len() >= self.limits.max_records {
                break;
            }
            self.persist(&encode_record(source)?)?;
            used = used.saturating_add(source.bytes.len());
            self.records.push(source.clone());
            copied = copied.saturating_add(1);
        }
        // A full follower that copied or truncated has a durable result. An
        // unchanged follower that still has leader records it cannot store
        // reports the limit.
        if copied == 0 && truncated == 0 && leader.records.len() > self.records.len() {
            return Err(Error::Budget);
        }
        Ok(Progress {
            truncated,
            copied,
            next_offset: self.next_offset(),
        })
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
            KIND_RECORD => self.records.push(decode_record(rest)?),
            KIND_TRUNCATE => {
                let offset = decode_truncate(rest)?;
                self.records.retain(|record| record.offset < offset);
            }
            _ => return Err(Error::InvalidRecord),
        }
        Ok(())
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        // Truncation is a journal record of its own. The logical cap limits
        // live records; the index cap limits the append and truncate history.
        if self.stored >= self.limits.journal.max_index_entries() {
            return Err(Error::Budget);
        }
        match self.journal.append(1, payload) {
            Ok(_) => {
                self.stored = self.stored.saturating_add(1);
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(Error::Storage(error))
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
    let mut bytes = Vec::with_capacity(21 + record.bytes.len());
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_RECORD);
    bytes.extend_from_slice(&record.offset.to_be_bytes());
    bytes.extend_from_slice(&record.leader_epoch.to_be_bytes());
    let len = u32::try_from(record.bytes.len()).map_err(|_| Error::InvalidRecord)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(&record.bytes);
    Ok(bytes)
}

fn encode_truncate(offset: i64) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_TRUNCATE);
    bytes.extend_from_slice(&offset.to_be_bytes());
    Ok(bytes)
}

fn decode_record(mut bytes: &[u8]) -> Result<Record, Error> {
    let offset = i64::from_be_bytes(
        take(&mut bytes, 8)?
            .try_into()
            .map_err(|_| Error::InvalidRecord)?,
    );
    let leader_epoch = i32::from_be_bytes(
        take(&mut bytes, 4)?
            .try_into()
            .map_err(|_| Error::InvalidRecord)?,
    );
    let len = usize::try_from(u32::from_be_bytes(
        take(&mut bytes, 4)?
            .try_into()
            .map_err(|_| Error::InvalidRecord)?,
    ))
    .map_err(|_| Error::InvalidRecord)?;
    let payload = take(&mut bytes, len)?.to_vec();
    if !bytes.is_empty() || offset < 0 || leader_epoch < 0 || payload.is_empty() {
        return Err(Error::InvalidRecord);
    }
    Ok(Record {
        offset,
        leader_epoch,
        bytes: payload,
    })
}

fn decode_truncate(bytes: &[u8]) -> Result<i64, Error> {
    if bytes.len() != 8 {
        return Err(Error::InvalidRecord);
    }
    let mut offset = [0u8; 8];
    offset.copy_from_slice(bytes);
    let offset = i64::from_be_bytes(offset);
    if offset < 0 {
        return Err(Error::InvalidRecord);
    }
    Ok(offset)
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(64, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(32, 32, journal).unwrap_or_else(|_| panic!("replica limits"))
    }

    fn log(name: &str) -> (Log, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-{name}-{nanos}"));
        let log = Log::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (log, path)
    }

    fn budget(max_records: usize) -> FetchBudget {
        FetchBudget {
            max_bytes: 1024,
            max_records,
        }
    }

    #[test]
    fn follower_matches_leader_bytes_and_resumes_after_restart() {
        let (mut leader, _) = log("leader-match");
        leader
            .append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        leader
            .append(1, b"bbb")
            .unwrap_or_else(|error| panic!("{error:?}"));
        leader
            .append(2, b"ccc")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let (mut follower, path) = log("follower-match");
        let first = follower
            .replicate_from(&leader, 2, budget(1))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(first.copied, 1);
        assert_eq!(follower.next_offset(), 1);
        drop(follower);
        let mut follower = Log::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let rest = follower
            .replicate_from(&leader, 2, budget(8))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(rest.copied, 2);
        assert_eq!(follower.records(), leader.records());
    }

    #[test]
    fn divergent_suffix_is_truncated_and_a_stale_leader_is_fenced() {
        let (mut leader, _) = log("leader-epoch");
        leader
            .append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        leader
            .append(2, b"new")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let (mut follower, _) = log("follower-epoch");
        follower
            .append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        follower
            .append(1, b"old")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let progress = follower
            .replicate_from(&leader, 2, budget(8))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(progress.truncated, 1);
        assert_eq!(follower.records(), leader.records());
        let (mut stale, _) = log("stale-leader");
        stale
            .append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let before = follower.records().to_vec();
        assert!(matches!(
            follower.replicate_from(&stale, 1, budget(8)),
            Err(Error::FencedLeader)
        ));
        assert_eq!(follower.records(), before.as_slice());
    }

    #[test]
    fn fetch_budget_stops_before_a_later_record_and_an_empty_budget_is_refused() {
        let (mut leader, _) = log("leader-budget");
        leader
            .append(1, b"aaaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        leader
            .append(1, b"bbbb")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let (mut follower, _) = log("follower-budget");
        let progress = follower
            .replicate_from(
                &leader,
                1,
                FetchBudget {
                    max_bytes: 6,
                    max_records: 8,
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(progress.copied, 1);
        assert_eq!(follower.records().len(), 1);
        assert!(matches!(
            follower.replicate_from(
                &leader,
                1,
                FetchBudget {
                    max_bytes: 0,
                    max_records: 1,
                },
            ),
            Err(Error::Budget)
        ));
    }

    #[test]
    fn a_full_follower_keeps_records_copied_before_the_cap() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let journal = journal::Limits::new(64, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        let tight = Limits::new(2, 32, journal).unwrap_or_else(|_| panic!("replica limits"));
        let leader_path = std::env::temp_dir().join(format!("pl-repl-cap-leader-{nanos}"));
        let follower_path = std::env::temp_dir().join(format!("pl-repl-cap-follower-{nanos}"));
        let mut leader =
            Log::open(&leader_path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        leader
            .append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        leader
            .append(1, b"bbb")
            .unwrap_or_else(|error| panic!("{error:?}"));
        leader
            .append(1, b"ccc")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut follower =
            Log::open(&follower_path, tight).unwrap_or_else(|error| panic!("{error:?}"));
        let progress = follower
            .replicate_from(&leader, 1, budget(8))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(progress.copied, 2);
        assert_eq!(follower.records().len(), 2);
        assert_eq!(follower.records()[1].bytes, b"bbb");
        assert!(matches!(
            follower.replicate_from(&leader, 1, budget(8)),
            Err(Error::Budget)
        ));
        assert_eq!(follower.records().len(), 2);
        let diverge_path = std::env::temp_dir().join(format!("pl-repl-cap-diverge-{nanos}"));
        let mut diverge =
            Log::open(&diverge_path, tight).unwrap_or_else(|error| panic!("{error:?}"));
        diverge
            .append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        diverge
            .append(1, b"old")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let replaced = diverge
            .replicate_from(&leader, 1, budget(8))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(replaced.truncated, 1);
        assert_eq!(replaced.copied, 1);
        assert_eq!(diverge.records()[1].bytes, b"bbb");
        assert_eq!(diverge.records().len(), 2);
        drop(diverge);
        let diverge = Log::open(&diverge_path, tight).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(diverge.records().len(), 2);
        assert_eq!(diverge.records()[1].bytes, b"bbb");
    }
}
