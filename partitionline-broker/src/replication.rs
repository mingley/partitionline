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
//! Accepted leader epochs are persisted in a protected journal header, even
//! when the leader has no new records. Raising the epoch atomically replaces
//! the journal and needs temporary space for both files. Older readers reject
//! that header. This local model still has no committed-watermark guard or
//! authenticated leadership; do not use it to authorize a network leader.

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
    /// A persistence or integrity failure requires reopening this log.
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
    /// The journal entry limit must accept a maximum record plus its 25-byte
    /// header, and its index must hold every confirmed append and truncation.
    /// Earlier versions accepted a 21-byte allowance; raise an affected
    /// journal's entry limit by up to four bytes before opening it.
    pub fn new(
        max_records: usize,
        max_payload_bytes: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=1_000_000).contains(&max_records)
            || max_payload_bytes == 0
            || max_payload_bytes > 1024 * 1024
            || journal.max_entry_bytes() < max_payload_bytes.saturating_add(25)
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
    accepted_epoch: i32,
}

impl Log {
    /// Open or recover a replica log.
    ///
    /// `<path>.epoch.tmp` is reserved for atomic epoch-fence publication.
    /// Once a fence is stored, older binaries reject the journal header.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open_replica(path, 0, limits.journal)?;
        let accepted_epoch = journal.replica_epoch().unwrap_or(-1);
        let mut log = Self {
            limits,
            records: Vec::new(),
            journal,
            stored: 0,
            poisoned: false,
            accepted_epoch,
        };
        let mut offset = log.journal.base_offset();
        loop {
            let batch = log.journal.fetch(
                offset,
                64.min(log.limits.journal.max_index_entries()),
                log.limits.journal.max_fetch_bytes(),
            )?;
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
    pub fn records(&self) -> Result<&[Record], Error> {
        self.ready()?;
        Ok(&self.records)
    }

    /// Next offset a new leader append would receive.
    pub fn next_offset(&self) -> Result<i64, Error> {
        self.ready()?;
        Ok(self
            .records
            .last()
            .map(|record| record.offset.saturating_add(1))
            .unwrap_or(0))
    }

    /// Highest accepted leader epoch, or -1 before any epoch is accepted.
    ///
    /// Unlike a maximum over the retained records, this fence survives empty
    /// leader responses, truncation and restart.
    pub fn leader_epoch(&self) -> Result<i32, Error> {
        self.ready()?;
        Ok(self.accepted_epoch)
    }

    /// Append one payload at the next offset under `leader_epoch`.
    ///
    /// The epoch must be nonnegative and at least the highest accepted epoch,
    /// including epochs accepted without a new record. This is the leader write path.
    pub fn append(&mut self, leader_epoch: i32, bytes: &[u8]) -> Result<i64, Error> {
        self.ready()?;
        if leader_epoch < 0 || bytes.is_empty() || bytes.len() > self.limits.max_payload_bytes {
            return Err(Error::InvalidRecord);
        }
        if leader_epoch < self.accepted_epoch {
            return Err(Error::FencedLeader);
        }
        if self.records.len() >= self.limits.max_records {
            return Err(Error::Budget);
        }
        // The next offset must itself have an exclusive end that fits in `i64`.
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
            bytes: bytes.to_vec(),
        };
        self.persist(&encode_record(&record)?)?;
        self.records.push(record);
        self.accepted_epoch = self.accepted_epoch.max(leader_epoch);
        Ok(offset)
    }

    /// Copy a bounded leader prefix, truncating any divergent follower suffix.
    ///
    /// `leader_epoch` is the epoch the caller claims for this leader. If it is
    /// older than an epoch already stored here, the call is fenced and the
    /// follower log is unchanged. A divergent suffix is not truncated unless
    /// a replacement fits this call and the journal has capacity for both the
    /// truncate and first replacement. If later records exhaust the budget,
    /// the returned progress covers the records already durably copied. A
    /// process exit or storage failure after a successful truncation can leave
    /// that prefix without the replacement; reopen before retrying. This local
    /// model has no committed-watermark guard.
    pub fn replicate_from(
        &mut self,
        leader: &Log,
        leader_epoch: i32,
        budget: FetchBudget,
    ) -> Result<Progress, Error> {
        self.ready()?;
        leader.ready()?;
        if budget.max_bytes == 0 || budget.max_records == 0 {
            return Err(Error::Budget);
        }
        if leader_epoch < 0 {
            return Err(Error::InvalidRecord);
        }
        if leader_epoch < self.accepted_epoch || leader_epoch < leader.accepted_epoch {
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
        // Inspect only the prefix this request and this journal can admit.
        // Validate it before a divergent suffix is durably removed.
        let truncated = self.records.len() - keep;
        let mut preflight_bytes = 0usize;
        let available = self.limits.max_records.saturating_sub(keep);
        let mut first_encoded_len = None;
        let mut planned = 0usize;
        let truncate_entries = usize::from(truncated > 0);
        let mut available_entries = self
            .limits
            .journal
            .max_index_entries()
            .saturating_sub(self.stored)
            .saturating_sub(truncate_entries);
        let truncation_cost = if truncated > 0 {
            journal::ENTRY_HEADER as u64 + 17
        } else {
            0
        };
        let mut available_file = self
            .limits
            .journal
            .max_file_bytes()
            .saturating_sub(self.journal.file_bytes())
            .saturating_sub(truncation_cost);
        for (index, source) in leader
            .records
            .iter()
            .skip(keep)
            .take(budget.max_records.min(available))
            .enumerate()
        {
            if source.bytes.len() > budget.max_bytes.saturating_sub(preflight_bytes)
                || available_entries == 0
            {
                break;
            }
            let encoded_len = source.bytes.len().checked_add(25).ok_or(Error::Budget)?;
            let file_cost = (journal::ENTRY_HEADER as u64)
                .checked_add(u64::try_from(encoded_len).map_err(|_| Error::Budget)?)
                .ok_or(Error::Budget)?;
            if file_cost > available_file {
                break;
            }
            if source.bytes.is_empty() || source.bytes.len() > self.limits.max_payload_bytes {
                return Err(Error::Budget);
            }
            if encoded_len
                .checked_add(std::mem::size_of::<journal::Entry>())
                .is_none_or(|bytes| bytes > self.limits.journal.max_fetch_bytes())
            {
                return Err(Error::Budget);
            }
            let expected = keep
                .checked_add(index)
                .and_then(|value| i64::try_from(value).ok())
                .ok_or(Error::InvalidRecord)?;
            if source.offset != expected || source.offset.checked_add(1).is_none() {
                return Err(Error::InvalidRecord);
            }
            if first_encoded_len.is_none() {
                first_encoded_len = Some(encoded_len);
            }
            preflight_bytes = preflight_bytes.saturating_add(source.bytes.len());
            available_entries -= 1;
            available_file -= file_cost;
            planned += 1;
        }
        if truncated > 0 && leader.records.get(keep).is_some() {
            // Ensure both writes fit and pre-reserve their index entries. The
            // truncate and replacement are still separate durable operations.
            let first = first_encoded_len.ok_or(Error::Budget)?;
            if 17usize
                .checked_add(std::mem::size_of::<journal::Entry>())
                .is_none_or(|bytes| bytes > self.limits.journal.max_fetch_bytes())
                || !self.journal.can_append_pair(17, first)
            {
                return Err(Error::Budget);
            }
            self.journal.reserve_append_pair()?;
        }
        if truncated == 0 && planned == 0 && leader.records.len() > keep {
            return Err(Error::Budget);
        }
        if leader_epoch > self.journal.replica_epoch().unwrap_or(-1) {
            if let Err(error) = self.journal.persist_replica_epoch(leader_epoch) {
                self.poisoned = self.journal.is_poisoned();
                return Err(Error::Storage(error));
            }
            self.accepted_epoch = leader_epoch;
        }
        if truncated > 0 {
            let offset = self
                .records
                .get(keep)
                .map(|record| record.offset)
                .unwrap_or(0);
            self.persist(&encode_truncate(offset)?)?;
            self.records.truncate(keep);
            #[cfg(test)]
            replication_test_crash("truncated");
        }
        let mut copied = 0usize;
        let mut used = 0usize;
        while copied < planned {
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
            if source.offset.checked_add(1).is_none() {
                return Err(Error::InvalidRecord);
            }
            match self.persist(&encode_record(source)?) {
                Ok(()) => {}
                // Already durable work is progress. The caller can resume and
                // observe the remaining budget refusal on its next attempt.
                Err(Error::Budget) if copied > 0 || truncated > 0 => break,
                Err(error) => return Err(error),
            }
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
            next_offset: self.next_offset()?,
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
            KIND_RECORD => {
                let record = decode_record(rest)?;
                if record.bytes.len() > self.limits.max_payload_bytes {
                    return Err(Error::InvalidRecord);
                }
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
                self.accepted_epoch = self.accepted_epoch.max(record.leader_epoch);
                self.records.push(record);
            }
            KIND_TRUNCATE => {
                let offset = decode_truncate(rest)?;
                if offset < 0 || !self.records.iter().any(|record| record.offset >= offset) {
                    return Err(Error::InvalidRecord);
                }
                self.records.retain(|record| record.offset < offset);
            }
            _ => return Err(Error::InvalidRecord),
        }
        Ok(())
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        // Truncation is a journal record of its own. The logical cap limits
        // live records; the index cap limits the append and truncate history.
        if self.stored >= self.limits.journal.max_index_entries()
            || !self.journal.can_append_snapshot(payload.len())
            || payload
                .len()
                .checked_add(std::mem::size_of::<journal::Entry>())
                .is_none_or(|bytes| bytes > self.limits.journal.max_fetch_bytes())
        {
            return Err(Error::Budget);
        }
        match self.journal.append(1, payload) {
            Ok(_) => {
                self.stored = self.stored.saturating_add(1);
                Ok(())
            }
            Err(error) => {
                // Journal admission errors happen before writing; only an
                // ambiguous write or synchronization failure poisons the log.
                self.poisoned = self.journal.is_poisoned();
                Err(Error::Storage(error))
            }
        }
    }
}

#[cfg(test)]
fn replication_test_crash(phase: &str) {
    if std::env::var("PLB_REPLICATION_CRASH").ok().as_deref() == Some(phase) {
        std::process::exit(71);
    }
}

fn split_kind(bytes: &[u8]) -> Result<(u8, &[u8]), Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRecord);
    }
    Ok((bytes[RECORD_MAGIC.len()], &bytes[RECORD_MAGIC.len() + 1..]))
}

fn encode_record(record: &Record) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(25 + record.bytes.len());
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
    // An offset of `i64::MAX` has no exclusive end. `next_offset` would stay
    // on the record and a follower would treat it as already replicated.
    if !bytes.is_empty()
        || offset < 0
        || offset.checked_add(1).is_none()
        || leader_epoch < 0
        || payload.is_empty()
    {
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
    fn a_gapped_replica_record_is_not_recovered() {
        let (mut log, _) = log("gap");
        let record = Record {
            offset: 4,
            leader_epoch: 1,
            bytes: b"gap".to_vec(),
        };
        let encoded = encode_record(&record).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(log.apply(&encoded), Err(Error::InvalidRecord)));
        assert_eq!(log.next_offset().unwrap(), 0);
        log.append(1, b"kept")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let truncate = encode_truncate(1).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(log.apply(&truncate), Err(Error::InvalidRecord)));
        assert_eq!(log.next_offset().unwrap(), 1);
        assert_eq!(log.records().unwrap()[0].bytes, b"kept");
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
        assert_eq!(follower.next_offset().unwrap(), 1);
        drop(follower);
        let mut follower = Log::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let rest = follower
            .replicate_from(&leader, 2, budget(8))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(rest.copied, 2);
        assert_eq!(follower.records().unwrap(), leader.records().unwrap());
    }

    #[test]
    fn an_offset_at_the_end_of_i64_is_not_appended() {
        let (mut log, _) = log("end");
        log.append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.records[0].offset = i64::MAX - 1;
        assert_eq!(log.next_offset().unwrap(), i64::MAX);
        assert!(matches!(
            log.append(1, b"bbb"),
            Err(Error::OffsetOutOfRange)
        ));
        assert_eq!(log.records().unwrap().len(), 1);
        assert_eq!(log.records().unwrap()[0].bytes, b"aaa");
    }

    #[test]
    fn a_record_at_i64_max_is_not_recovered_or_copied() {
        let encoded = encode_record(&Record {
            offset: i64::MAX,
            leader_epoch: 1,
            bytes: b"aaa".to_vec(),
        })
        .unwrap_or_else(|error| panic!("{error:?}"));
        let (mut follower, _) = log("max-offset");
        assert!(matches!(
            follower.apply(&encoded),
            Err(Error::InvalidRecord)
        ));
        assert!(follower.records().unwrap().is_empty());

        let (mut leader, _) = log("max-leader");
        leader
            .append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        leader.records[0].offset = i64::MAX;
        assert!(matches!(
            follower.replicate_from(&leader, 1, budget(8)),
            Err(Error::InvalidRecord)
        ));
        assert!(follower.records().unwrap().is_empty());
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
        assert_eq!(follower.records().unwrap(), leader.records().unwrap());
        let (mut stale, _) = log("stale-leader");
        stale
            .append(1, b"aaa")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let before = follower.records().unwrap().to_vec();
        assert!(matches!(
            follower.replicate_from(&stale, 1, budget(8)),
            Err(Error::FencedLeader)
        ));
        assert_eq!(follower.records().unwrap(), before.as_slice());
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
        assert_eq!(follower.records().unwrap().len(), 1);
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
        assert_eq!(follower.records().unwrap().len(), 2);
        assert_eq!(follower.records().unwrap()[1].bytes, b"bbb");
        assert!(matches!(
            follower.replicate_from(&leader, 1, budget(8)),
            Err(Error::Budget)
        ));
        assert_eq!(follower.records().unwrap().len(), 2);
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
        assert_eq!(diverge.records().unwrap()[1].bytes, b"bbb");
        assert_eq!(diverge.records().unwrap().len(), 2);
        drop(diverge);
        let diverge = Log::open(&diverge_path, tight).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(diverge.records().unwrap().len(), 2);
        assert_eq!(diverge.records().unwrap()[1].bytes, b"bbb");
    }
    #[test]
    fn maximum_record_header_uses_twenty_five_bytes() {
        let too_small = journal::Limits::new(56, 1024, 2, 1024).unwrap();
        assert!(matches!(
            Limits::new(1, 32, too_small),
            Err(Error::InvalidLimits)
        ));
        let exact = journal::Limits::new(57, 1024, 2, 1024).unwrap();
        assert!(Limits::new(1, 32, exact).is_ok());
    }

    #[test]
    fn smaller_follower_payload_limit_rejects_before_divergent_truncation() {
        let (mut leader, _) = log("larger-leader");
        leader.append(2, b"large-record").unwrap();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-small-payload-{nanos}"));
        let j = journal::Limits::new(64, 1 << 20, 100, 1 << 20).unwrap();
        let limits = Limits::new(4, 4, j).unwrap();
        let mut follower = Log::open(&path, limits).unwrap();
        follower.append(1, b"old").unwrap();
        assert!(matches!(
            follower.replicate_from(&leader, 2, budget(4)),
            Err(Error::Budget)
        ));
        assert_eq!(follower.records().unwrap()[0].bytes, b"old");
        drop(follower);
        let mut follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.records().unwrap()[0].bytes, b"old");
        follower.append(1, b"next").unwrap();
        drop(follower);
        let follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.records().unwrap().len(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_fetch_admission_refuses_unreadable_record_without_poison() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-fetch-limit-{nanos}"));
        let fetch = std::mem::size_of::<journal::Entry>() + 29;
        let j = journal::Limits::new(64, 1 << 20, 3, fetch).unwrap();
        let limits = Limits::new(2, 32, j).unwrap();
        let mut log = Log::open(&path, limits).unwrap();
        assert!(matches!(log.append(1, b"12345"), Err(Error::Budget)));
        log.append(1, b"1234").unwrap();
        drop(log);
        let log = Log::open(&path, limits).unwrap();
        assert_eq!(log.records().unwrap()[0].bytes, b"1234");
        drop(log);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn full_file_rejection_keeps_confirmed_data_readable() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-file-limit-{nanos}"));
        let j = journal::Limits::new(64, 24 + 32 + 28, 3, 1024).unwrap();
        let limits = Limits::new(2, 32, j).unwrap();
        let mut log = Log::open(&path, limits).unwrap();
        log.append(1, b"old").unwrap();
        assert!(matches!(log.append(1, b"new"), Err(Error::Budget)));
        assert!(matches!(log.append(1, b"x"), Err(Error::Budget)));
        drop(log);
        let log = Log::open(&path, limits).unwrap();
        assert_eq!(log.records().unwrap()[0].bytes, b"old");
        drop(log);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn one_index_entry_can_replicate_and_recover_one_record() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-one-index-{nanos}"));
        let j = journal::Limits::new(64, 1024, 1, 1024).unwrap();
        let limits = Limits::new(1, 32, j).unwrap();
        let (mut leader, _) = log("single-index-leader");
        leader.append(1, b"one").unwrap();
        let mut follower = Log::open(&path, limits).unwrap();
        assert_eq!(
            follower
                .replicate_from(&leader, 1, budget(1))
                .unwrap()
                .copied,
            1
        );
        drop(follower);
        let follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.records().unwrap(), leader.records().unwrap());
        drop(follower);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replication_refuses_poisoned_source_before_changing_follower() {
        let (mut leader, _) = log("poisoned-source");
        let (mut follower, _) = log("poisoned-source-follower");
        leader.append(1, b"first").unwrap();
        journal::fail_append_once_after_sync();
        assert!(leader.append(1, b"unknown").is_err());
        assert!(matches!(
            follower.replicate_from(&leader, 1, budget(8)),
            Err(Error::Poisoned)
        ));
        assert!(follower.records().unwrap().is_empty());
    }
    #[test]
    fn divergent_replacement_needs_two_journal_slots_and_file_space_before_truncation() {
        let (mut leader, _) = log("admission-leader");
        leader.append(2, b"new").unwrap();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        for (name, index, file_bytes) in [("index", 2, 1024), ("file", 3, 192)] {
            let path = std::env::temp_dir().join(format!("pl-repl-admission-{name}-{nanos}"));
            let journal = journal::Limits::new(64, file_bytes, index, 1024).unwrap();
            let limits = Limits::new(2, 32, journal).unwrap();
            let mut follower = Log::open(&path, limits).unwrap();
            follower.append(1, b"old").unwrap();
            assert!(matches!(
                follower.replicate_from(&leader, 2, budget(2)),
                Err(Error::Budget)
            ));
            assert_eq!(follower.records().unwrap()[0].bytes, b"old");
            follower.append(1, b"x").unwrap();
            drop(follower);
            let follower = Log::open(&path, limits).unwrap();
            assert_eq!(follower.records().unwrap().len(), 2);
            assert_eq!(follower.records().unwrap()[0].bytes, b"old");
            drop(follower);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn divergence_with_no_copyable_first_record_is_unchanged() {
        let (mut leader, _) = log("no-first-leader");
        leader.append(2, b"long").unwrap();
        let (mut follower, path) = log("no-first-follower");
        follower.append(1, b"old").unwrap();
        assert!(matches!(
            follower.replicate_from(
                &leader,
                2,
                FetchBudget {
                    max_bytes: 3,
                    max_records: 1
                }
            ),
            Err(Error::Budget)
        ));
        assert_eq!(follower.records().unwrap()[0].bytes, b"old");
        drop(follower);
        let follower = Log::open(&path, limits()).unwrap();
        assert_eq!(follower.records().unwrap()[0].bytes, b"old");
    }

    #[test]
    fn later_unselected_record_does_not_reject_valid_bounded_prefix() {
        let (mut leader, _) = log("partial-limit-leader");
        leader.append(1, b"ok").unwrap();
        leader.append(1, b"too-large").unwrap();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-partial-limit-{nanos}"));
        let journal = journal::Limits::new(64, 1024, 4, 1024).unwrap();
        let limits = Limits::new(2, 4, journal).unwrap();
        let mut follower = Log::open(&path, limits).unwrap();
        let progress = follower
            .replicate_from(
                &leader,
                1,
                FetchBudget {
                    max_bytes: 2,
                    max_records: 2,
                },
            )
            .unwrap();
        assert_eq!(progress.copied, 1);
        assert_eq!(follower.records().unwrap()[0].bytes, b"ok");
        assert!(matches!(
            follower.replicate_from(&leader, 1, budget(2)),
            Err(Error::Budget)
        ));
        drop(follower);
        let follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.records().unwrap().len(), 1);
        assert_eq!(follower.records().unwrap()[0].bytes, b"ok");
        drop(follower);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn later_journal_capacity_exhaustion_reports_durable_partial_progress() {
        let (mut leader, _) = log("partial-journal-leader");
        leader.append(1, b"one").unwrap();
        leader.append(1, b"two").unwrap();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-partial-journal-{nanos}"));
        // One previously confirmed truncate consumes a slot with no live data.
        let journal = journal::Limits::new(64, 1024, 2, 1024).unwrap();
        let limits = Limits::new(2, 32, journal).unwrap();
        let mut follower = Log::open(&path, limits).unwrap();
        follower.append(1, b"old").unwrap();
        follower.persist(&encode_truncate(0).unwrap()).unwrap();
        follower.records.clear();
        drop(follower);
        let mut follower = Log::open(&path, limits).unwrap();
        // Both history entries are occupied, so zero-progress refusal remains
        // an error and leaves the handle usable and recoverable.
        assert!(matches!(
            follower.replicate_from(&leader, 1, budget(2)),
            Err(Error::Budget)
        ));
        assert!(follower.records().unwrap().is_empty());
        drop(follower);
        let follower = Log::open(&path, limits).unwrap();
        assert!(follower.records().unwrap().is_empty());
        drop(follower);
        let _ = std::fs::remove_file(path);

        let path = std::env::temp_dir().join(format!("pl-repl-partial-copy-{nanos}"));
        let journal = journal::Limits::new(64, 24 + 32 + 28, 2, 1024).unwrap();
        let limits = Limits::new(2, 32, journal).unwrap();
        let mut follower = Log::open(&path, limits).unwrap();
        let progress = follower.replicate_from(&leader, 1, budget(2)).unwrap();
        assert_eq!(progress.copied, 1);
        assert!(matches!(
            follower.replicate_from(&leader, 1, budget(2)),
            Err(Error::Budget)
        ));
        drop(follower);
        let follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.records().unwrap().len(), 1);
        assert_eq!(follower.records().unwrap()[0].bytes, b"one");
        drop(follower);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn old_small_entry_configuration_recovers_with_migrated_limit() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-migrated-{nanos}"));
        let old = journal::Limits::new(56, 1024, 1, 1024).unwrap();
        let (mut journal, _) = Journal::open(&path, 0, old).unwrap();
        let bytes = encode_record(&Record {
            offset: 0,
            leader_epoch: 1,
            bytes: b"old".to_vec(),
        })
        .unwrap();
        journal.append(1, &bytes).unwrap();
        drop(journal);
        assert!(matches!(Limits::new(1, 32, old), Err(Error::InvalidLimits)));
        let migrated = journal::Limits::new(57, 1024, 1, 1024).unwrap();
        let mut log = Log::open(&path, Limits::new(1, 32, migrated).unwrap()).unwrap();
        assert_eq!(log.records().unwrap()[0].bytes, b"old");
        assert!(matches!(log.append(1, b"new"), Err(Error::Budget)));
        drop(log);
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn index_reservation_failure_precedes_durable_truncation() {
        let (mut leader, _) = log("reserve-fail-leader");
        leader.append(2, b"new").unwrap();
        let (mut follower, path) = log("reserve-fail-follower");
        follower.append(1, b"old").unwrap();
        journal::fail_pair_reserve_once();
        assert!(matches!(
            follower.replicate_from(&leader, 2, budget(1)),
            Err(Error::Storage(journal::Error::AllocationFailed))
        ));
        assert_eq!(follower.records().unwrap()[0].bytes, b"old");
        drop(follower);
        let mut follower = Log::open(&path, limits()).unwrap();
        assert_eq!(follower.records().unwrap()[0].bytes, b"old");
        assert_eq!(
            follower
                .replicate_from(&leader, 2, budget(1))
                .unwrap()
                .copied,
            1
        );
        assert_eq!(follower.records().unwrap(), leader.records().unwrap());
    }

    #[test]
    fn record_beyond_journal_capacity_does_not_block_valid_prefix() {
        let (mut leader, _) = log("file-prefix-leader");
        leader.append(1, b"ok").unwrap();
        leader.append(1, b"too-large").unwrap();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-repl-file-prefix-{nanos}"));
        let journal = journal::Limits::new(64, 24 + 32 + 27, 2, 1024).unwrap();
        let limits = Limits::new(2, 4, journal).unwrap();
        let mut follower = Log::open(&path, limits).unwrap();
        let progress = follower.replicate_from(&leader, 1, budget(2)).unwrap();
        assert_eq!(progress.copied, 1);
        assert!(matches!(
            follower.replicate_from(&leader, 1, budget(2)),
            Err(Error::Budget)
        ));
        drop(follower);
        let follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.records().unwrap()[0].bytes, b"ok");
        drop(follower);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    #[cfg(unix)]
    fn replication_resumes_after_process_exit_following_truncation() {
        const TEST: &str =
            "replication::tests::replication_resumes_after_process_exit_following_truncation";
        if let Some(dir) = std::env::var_os("PLB_REPLICATION_CRASH_DIR") {
            let dir = std::path::PathBuf::from(dir);
            let leader = Log::open(dir.join("leader"), limits()).unwrap();
            let mut follower = Log::open(dir.join("follower"), limits()).unwrap();
            follower.replicate_from(&leader, 2, budget(1)).unwrap();
            panic!("replication failpoint did not exit");
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "pl-repl-trunc-crash-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut leader = Log::open(dir.join("leader"), limits()).unwrap();
        leader.append(2, b"new").unwrap();
        let mut follower = Log::open(dir.join("follower"), limits()).unwrap();
        follower.append(1, b"old").unwrap();
        drop(leader);
        drop(follower);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env("PLB_REPLICATION_CRASH", "truncated")
            .env("PLB_REPLICATION_CRASH_DIR", &dir)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(71));
        let leader = Log::open(dir.join("leader"), limits()).unwrap();
        let mut follower = Log::open(dir.join("follower"), limits()).unwrap();
        assert!(follower.records().unwrap().is_empty());
        assert_eq!(
            follower
                .replicate_from(&leader, 2, budget(1))
                .unwrap()
                .copied,
            1
        );
        drop(follower);
        let follower = Log::open(dir.join("follower"), limits()).unwrap();
        assert_eq!(follower.records().unwrap(), leader.records().unwrap());
        drop(follower);
        drop(leader);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn empty_epoch_fence_preserves_one_entry_capacity_and_blocks_old_readers() {
        let (leader, leader_path) = log("empty-fence-source");
        let (_, path) = log("empty-fence-target-name");
        // The helper's empty handle was dropped at the statement boundary.
        let journal_limits = journal::Limits::new(57, 24 + 32 + 57, 1, 1024).unwrap();
        let limits = Limits::new(1, 32, journal_limits).unwrap();
        let mut follower = Log::open(&path, limits).unwrap();
        for epoch in [0, 4, i32::MAX] {
            let progress = follower.replicate_from(&leader, epoch, budget(1)).unwrap();
            assert_eq!(
                (progress.copied, progress.truncated, progress.next_offset),
                (0, 0, 0)
            );
            assert_eq!(follower.journal.file_bytes(), 24);
            assert_eq!(follower.stored, 0);
        }
        drop(follower);
        let original = tokio::fs::read(&path).await.unwrap();
        assert!(matches!(
            Journal::open(&path, 0, journal_limits),
            Err(journal::Error::Corrupt {
                kind: journal::Corruption::FileHeader,
                ..
            })
        ));
        assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
        let mut follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.leader_epoch().unwrap(), i32::MAX);
        assert!(matches!(
            follower.append(i32::MAX - 1, b"stale"),
            Err(Error::FencedLeader)
        ));
        assert_eq!(follower.append(i32::MAX, &[1; 32]).unwrap(), 0);
        drop(follower);
        let follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.records().unwrap()[0].bytes, [1; 32]);
        drop(follower);
        drop(leader);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(leader_path).unwrap();
    }

    #[test]
    fn unchanged_full_one_entry_log_accepts_later_epochs_without_index_growth() {
        let (mut leader, _) = log("full-fence-source");
        leader.append(1, b"same").unwrap();
        let (_, path) = log("full-fence-target-name");
        let j = journal::Limits::new(57, 24 + 32 + 57, 1, 1024).unwrap();
        let limits = Limits::new(1, 32, j).unwrap();
        let mut follower = Log::open(&path, limits).unwrap();
        follower.append(1, b"same").unwrap();
        let bytes = follower.journal.file_bytes();
        for epoch in [2, 8, 9] {
            assert_eq!(
                follower
                    .replicate_from(&leader, epoch, budget(1))
                    .unwrap()
                    .copied,
                0
            );
            assert_eq!(follower.journal.file_bytes(), bytes);
            assert_eq!(follower.stored, 1);
        }
        drop(follower);
        let mut follower = Log::open(&path, limits).unwrap();
        assert_eq!(follower.leader_epoch().unwrap(), 9);
        assert!(matches!(
            follower.replicate_from(&leader, 8, budget(1)),
            Err(Error::FencedLeader)
        ));
        assert_eq!(follower.records().unwrap(), leader.records().unwrap());
    }

    #[test]
    fn accepted_epoch_survives_removing_its_last_record() {
        let (mut follower, path) = log("truncate-fence-target");
        follower.append(1, b"shared").unwrap();
        follower.append(7, b"divergent").unwrap();
        let (mut leader, _) = log("truncate-fence-source");
        leader.append(1, b"shared").unwrap();
        let progress = follower.replicate_from(&leader, 7, budget(1)).unwrap();
        assert_eq!((progress.truncated, progress.copied), (1, 0));
        assert_eq!(follower.leader_epoch().unwrap(), 7);
        drop(follower);
        let mut follower = Log::open(&path, limits()).unwrap();
        assert_eq!(follower.leader_epoch().unwrap(), 7);
        assert!(matches!(
            follower.append(6, b"stale"),
            Err(Error::FencedLeader)
        ));
        assert!(matches!(
            follower.replicate_from(&leader, 6, budget(1)),
            Err(Error::FencedLeader)
        ));
        let (empty, _) = log("truncate-fence-empty");
        assert_eq!(
            follower
                .replicate_from(&empty, 8, budget(1))
                .unwrap()
                .truncated,
            1
        );
        drop(follower);
        let mut follower = Log::open(&path, limits()).unwrap();
        assert!(follower.records().unwrap().is_empty());
        assert_eq!(follower.leader_epoch().unwrap(), 8);
        assert!(matches!(
            follower.append(7, b"stale"),
            Err(Error::FencedLeader)
        ));
        assert_eq!(follower.append(8, b"new").unwrap(), 0);
    }

    #[test]
    fn legacy_event_replay_remembers_epochs_from_truncated_records() {
        let (_, path) = log("legacy-epoch-history");
        let (mut journal, _) = Journal::open(&path, 0, limits().journal).unwrap();
        for payload in [
            encode_record(&Record {
                offset: 0,
                leader_epoch: 5,
                bytes: b"old".to_vec(),
            })
            .unwrap(),
            encode_truncate(0).unwrap(),
            encode_record(&Record {
                offset: 0,
                leader_epoch: 1,
                bytes: b"legacy".to_vec(),
            })
            .unwrap(),
        ] {
            journal.append(1, &payload).unwrap();
        }
        drop(journal);
        let mut log = Log::open(&path, limits()).unwrap();
        assert_eq!(log.records().unwrap()[0].leader_epoch, 1);
        assert_eq!(log.leader_epoch().unwrap(), 5);
        assert!(matches!(log.append(4, b"stale"), Err(Error::FencedLeader)));
        assert_eq!(log.append(5, b"current").unwrap(), 1);
    }

    #[test]
    fn epoch_publication_errors_distinguish_pre_rename_from_ambiguous_outcomes() {
        for phase in [
            "epoch-permissions-sync",
            "epoch-sync",
            "epoch-prepared",
            "epoch-rename",
            "epoch-renamed",
        ] {
            let (mut follower, path) = log(phase);
            follower.append(1, b"same").unwrap();
            let (mut leader, _) = log("epoch-error-source");
            leader.append(1, b"same").unwrap();
            journal::fail_checkpoint_once_at(phase);
            assert!(matches!(
                follower.replicate_from(&leader, 7, budget(1)),
                Err(Error::Storage(journal::Error::Io(_)))
            ));
            let published = matches!(phase, "epoch-rename" | "epoch-renamed");
            if phase == "epoch-permissions-sync" {
                let mut temporary = path.clone().into_os_string();
                temporary.push(".epoch.tmp");
                assert_eq!(
                    std::fs::metadata(std::path::PathBuf::from(temporary))
                        .unwrap()
                        .len(),
                    0
                );
            }
            if !published {
                assert_eq!(follower.leader_epoch().unwrap(), 1);
                assert_eq!(follower.records().unwrap()[0].bytes, b"same");
                assert_eq!(follower.append(1, b"next").unwrap(), 1);
            } else {
                assert!(matches!(follower.records(), Err(Error::Poisoned)));
                assert!(matches!(follower.next_offset(), Err(Error::Poisoned)));
                assert!(matches!(follower.leader_epoch(), Err(Error::Poisoned)));
                assert!(matches!(follower.append(7, b"next"), Err(Error::Poisoned)));
            }
            drop(follower);
            let mut follower = Log::open(&path, limits()).unwrap();
            assert_eq!(
                follower.leader_epoch().unwrap(),
                if published { 7 } else { 1 }
            );
            assert!(!follower.records().unwrap().is_empty());
            follower.replicate_from(&leader, 7, budget(1)).unwrap();
            assert_eq!(follower.leader_epoch().unwrap(), 7);
        }
    }

    #[test]
    #[cfg(unix)]
    fn epoch_header_recovery_at_each_atomic_publication_stage() {
        const TEST: &str =
            "replication::tests::epoch_header_recovery_at_each_atomic_publication_stage";
        if let Some(dir) = std::env::var_os("PLB_EPOCH_HEADER_CRASH_DIR") {
            let dir = std::path::PathBuf::from(dir);
            let leader = Log::open(dir.join("leader"), limits()).unwrap();
            let mut follower = Log::open(dir.join("follower"), limits()).unwrap();
            follower.replicate_from(&leader, 7, budget(1)).unwrap();
            panic!("epoch publication failpoint did not exit");
        }
        for phase in ["epoch-prepared", "epoch-renamed", "epoch-synced"] {
            let (mut leader, leader_path) = log("epoch-crash-parent-source");
            leader.append(1, b"same").unwrap();
            let parent = leader_path.with_extension(phase);
            std::fs::create_dir_all(&parent).unwrap();
            let mut follower = Log::open(parent.join("follower"), limits()).unwrap();
            follower.append(1, b"same").unwrap();
            drop(follower);
            drop(leader);
            std::fs::rename(leader_path, parent.join("leader")).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture"])
                .env("PLB_JOURNAL_CHECKPOINT_CRASH", phase)
                .env("PLB_EPOCH_HEADER_CRASH_DIR", &parent)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(71));
            let leader = Log::open(parent.join("leader"), limits()).unwrap();
            let mut follower = Log::open(parent.join("follower"), limits()).unwrap();
            let expected = if phase == "epoch-prepared" { 1 } else { 7 };
            assert_eq!(follower.leader_epoch().unwrap(), expected);
            assert_eq!(follower.records().unwrap(), leader.records().unwrap());
            if expected == 7 {
                assert!(matches!(
                    follower.replicate_from(&leader, 6, budget(1)),
                    Err(Error::FencedLeader)
                ));
            }
            follower.replicate_from(&leader, 7, budget(1)).unwrap();
            drop(follower);
            let follower = Log::open(parent.join("follower"), limits()).unwrap();
            assert_eq!(follower.leader_epoch().unwrap(), 7);
            drop(follower);
            drop(leader);
            std::fs::remove_dir_all(parent).unwrap();
        }
    }

    #[tokio::test]
    async fn corrupted_or_unsupported_epoch_headers_are_not_repaired() {
        let (mut log, path) = log("invalid-epoch-header");
        log.append(1, b"old").unwrap();
        drop(log);
        let original = tokio::fs::read(&path).await.unwrap();
        for encoded in [u32::MAX, (i32::MAX as u32) + 2] {
            let mut damaged = original.clone();
            damaged[16..20].copy_from_slice(&encoded.to_be_bytes());
            let checksum = crc32c::crc32c(&damaged[..20]);
            damaged[20..24].copy_from_slice(&checksum.to_be_bytes());
            tokio::fs::write(&path, &damaged).await.unwrap();
            assert!(matches!(
                Log::open(&path, limits()),
                Err(Error::Storage(journal::Error::Corrupt {
                    kind: journal::Corruption::FileHeader,
                    ..
                }))
            ));
            assert_eq!(tokio::fs::read(&path).await.unwrap(), damaged);
        }
        let mut damaged = original;
        damaged[16] ^= 1;
        tokio::fs::write(&path, &damaged).await.unwrap();
        assert!(matches!(
            Log::open(&path, limits()),
            Err(Error::Storage(journal::Error::Corrupt {
                kind: journal::Corruption::FileHeader,
                ..
            }))
        ));
        assert_eq!(tokio::fs::read(&path).await.unwrap(), damaged);
    }

    #[test]
    #[cfg(unix)]
    fn accepted_epoch_replacement_preserves_private_file_permissions() {
        use std::os::unix::fs::PermissionsExt;
        for mode in [0o600, 0o700, 0o4700, 0o2700] {
            let (mut follower, path) = log("private-epoch-header");
            follower.append(1, b"private").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
                mode
            );
            let (mut leader, _) = log("private-epoch-source");
            leader.append(1, b"private").unwrap();
            journal::fail_checkpoint_once_at("epoch-prepared");
            assert!(matches!(
                follower.replicate_from(&leader, 2, budget(1)),
                Err(Error::Storage(journal::Error::Io(_)))
            ));
            assert_eq!(follower.leader_epoch().unwrap(), 1);
            assert_eq!(follower.records().unwrap()[0].bytes, b"private");
            let mut temporary = path.clone().into_os_string();
            temporary.push(".epoch.tmp");
            assert_eq!(
                std::fs::metadata(std::path::PathBuf::from(temporary))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o7777,
                mode
            );
            follower.replicate_from(&leader, 2, budget(1)).unwrap();
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
                mode
            );
            drop(follower);
            let follower = Log::open(&path, limits()).unwrap();
            assert_eq!(follower.leader_epoch().unwrap(), 2);
            assert_eq!(follower.records().unwrap()[0].bytes, b"private");
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
                mode
            );
        }
    }

    #[tokio::test]
    async fn detected_source_changes_poison_reads_and_writes_without_repair() {
        for kind in [0, 1, 2] {
            let (mut follower, path) = log("active-source-corruption");
            follower.append(1, b"old").unwrap();
            let (mut leader, _) = log("active-source-corruption-leader");
            leader.append(1, b"old").unwrap();
            let mut damaged = tokio::fs::read(&path).await.unwrap();
            // Deliberately violate exclusive ownership to verify fail-closed
            // source checks, including a changed record with valid CRCs.
            damaged[if kind == 0 { 16 } else { 24 + 32 + 25 }] ^= 1;
            if kind == 2 {
                let payload_crc = crc32c::crc32c(&damaged[24 + 32..]);
                damaged[24 + 24..24 + 28].copy_from_slice(&payload_crc.to_be_bytes());
                let header_crc = crc32c::crc32c(&damaged[24..24 + 28]);
                damaged[24 + 28..24 + 32].copy_from_slice(&header_crc.to_be_bytes());
            }
            tokio::fs::write(&path, &damaged).await.unwrap();
            let result = follower.replicate_from(&leader, 2, budget(1));
            if kind == 2 {
                assert!(matches!(
                    result,
                    Err(Error::Storage(journal::Error::ChangedFile))
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(Error::Storage(journal::Error::Corrupt { .. }))
                ));
            }
            assert!(matches!(follower.records(), Err(Error::Poisoned)));
            assert!(matches!(follower.next_offset(), Err(Error::Poisoned)));
            assert!(matches!(follower.leader_epoch(), Err(Error::Poisoned)));
            assert!(matches!(follower.append(2, b"later"), Err(Error::Poisoned)));
            assert_eq!(tokio::fs::read(&path).await.unwrap(), damaged);
            drop(follower);
            if kind == 2 {
                let reopened = Log::open(&path, limits()).unwrap();
                assert_ne!(reopened.records().unwrap()[0].bytes, b"old");
            } else {
                assert!(matches!(
                    Log::open(&path, limits()),
                    Err(Error::Storage(journal::Error::Corrupt { .. }))
                ));
            }
        }
    }
}
