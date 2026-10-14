//! High watermark, minimum ISR, and replicated produce acknowledgments.
//!
//! The high watermark is the minimum log end offset of the current in-sync
//! replicas, and it never moves backward. Offsets below it are committed.
//! `acks=1` may succeed when only the leader has the records. `acks=-1`
//! succeeds only after the high watermark covers them and the ISR is at least
//! `min.insync.replicas`. A later leader failure does not drop those committed
//! ids.
//!
//! Confirmed watermark advances are journaled. This is not a Produce RPC and
//! it does not move replica bytes.

use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLHWMK01";
const KIND_COMMIT: u8 = 1;

/// Kafka `NOT_ENOUGH_REPLICAS`.
pub const NOT_ENOUGH_REPLICAS: i16 = 19;
/// Kafka `NOT_ENOUGH_REPLICAS_AFTER_APPEND`.
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
/// Kafka `INVALID_REQUIRED_ACKS`.
pub const INVALID_REQUIRED_ACKS: i16 = 21;

/// Whether an acknowledgment satisfied its durability guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    /// `acks=0` or `acks=1` completed. The records are not necessarily committed.
    Local,
    /// `acks=-1` completed and the offsets are at or below the high watermark.
    Committed,
}

/// Why an acknowledgment or membership change was refused.
#[derive(Debug)]
pub enum Error {
    /// `acks=all` cannot be satisfied because the ISR is already too small.
    NotEnoughReplicas,
    /// The leader stored the records, then the ISR fell below the minimum.
    NotEnoughReplicasAfterAppend,
    /// The acknowledgment mode is not 0, 1, or -1.
    InvalidAcks,
    /// The requested range is not on the leader.
    NotWritten,
    /// The replacement leader does not contain the committed prefix.
    IneligibleLeader,
    /// A replica reported an offset behind its previous end or past the leader.
    OffsetMoved,
    /// A failed append left this handle unable to confirm further commits.
    Poisoned,
    /// The limits cannot store the committed prefix.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a producer-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::NotEnoughReplicas => NOT_ENOUGH_REPLICAS,
            Self::NotEnoughReplicasAfterAppend => NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            Self::InvalidAcks => INVALID_REQUIRED_ACKS,
            Self::NotWritten
            | Self::IneligibleLeader
            | Self::OffsetMoved
            | Self::Poisoned
            | Self::InvalidLimits
            | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// How many committed ids and journal records to retain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a positive committed-record cap. The journal must index it.
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

/// Leader durability state for one partition.
pub struct Durability {
    limits: Limits,
    leader: i32,
    min_isr: usize,
    isr: BTreeSet<i32>,
    end_offset: BTreeMap<i32, i64>,
    high_watermark: i64,
    ids: BTreeMap<i64, u64>,
    committed: BTreeSet<u64>,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Durability {
    /// Open a partition whose initial ISR is the leader alone and whose high
    /// watermark is 0. `min_isr` must be at least 1.
    pub fn open(
        path: impl AsRef<Path>,
        leader: i32,
        min_isr: usize,
        limits: Limits,
    ) -> Result<Self, Error> {
        if leader < 0 || min_isr == 0 {
            return Err(Error::InvalidLimits);
        }
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut state = Self {
            limits,
            leader,
            min_isr,
            isr: BTreeSet::from([leader]),
            end_offset: BTreeMap::from([(leader, 0)]),
            high_watermark: 0,
            ids: BTreeMap::new(),
            committed: BTreeSet::new(),
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut offset = state.journal.base_offset();
        loop {
            let batch = state
                .journal
                .fetch(offset, 64, state.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidLimits);
                }
                state.apply(&entry.payload)?;
                state.stored = state.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidLimits)?;
            }
        }
        Ok(state)
    }

    /// Offsets strictly below this value are committed.
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Whether this producer id was acknowledged under `acks=-1`.
    pub fn is_committed(&self, id: u64) -> bool {
        self.committed.contains(&id)
    }

    /// Current ISR, including the leader.
    pub fn isr(&self) -> impl Iterator<Item = i32> + '_ {
        self.isr.iter().copied()
    }

    /// Store `ids` on the leader and return their base offset.
    ///
    /// This does not acknowledge them. `acks=all` still waits for the high
    /// watermark.
    pub fn append(&mut self, ids: &[u64]) -> Result<i64, Error> {
        self.ready()?;
        if ids.is_empty() {
            return Err(Error::NotWritten);
        }
        let base = self.end_offset.get(&self.leader).copied().unwrap_or(0);
        let count = i64::try_from(ids.len()).map_err(|_| Error::InvalidLimits)?;
        let end = base.checked_add(count).ok_or(Error::InvalidLimits)?;
        let mut next = base;
        for id in ids {
            self.ids.insert(next, *id);
            next = next.saturating_add(1);
        }
        self.end_offset.insert(self.leader, end);
        self.advance_watermark();
        Ok(base)
    }

    /// Record a follower's log end offset. The offset must not move backward.
    ///
    /// A replica outside the ISR may catch up. Its offset does not move the
    /// high watermark until [`Self::add_isr`] accepts it.
    pub fn replicate_to(&mut self, replica: i32, end_offset: i64) -> Result<i64, Error> {
        self.ready()?;
        if replica < 0 || end_offset < 0 {
            return Err(Error::IneligibleLeader);
        }
        let previous = self.end_offset.get(&replica).copied().unwrap_or(0);
        let leader_end = self.end_offset.get(&self.leader).copied().unwrap_or(0);
        if end_offset < previous || end_offset > leader_end {
            return Err(Error::OffsetMoved);
        }
        self.end_offset.insert(replica, end_offset);
        if self.isr.contains(&replica) {
            self.advance_watermark();
        }
        Ok(self.high_watermark)
    }

    /// Add a follower that has already reached the high watermark.
    pub fn add_isr(&mut self, replica: i32) -> Result<(), Error> {
        self.ready()?;
        let caught_up = self.end_offset.get(&replica).copied().unwrap_or(0);
        if replica < 0 || caught_up < self.high_watermark {
            return Err(Error::IneligibleLeader);
        }
        self.isr.insert(replica);
        self.advance_watermark();
        Ok(())
    }

    /// Remove a follower. The leader cannot be removed except by [`Self::fail_over`].
    ///
    /// The high watermark is the minimum end offset of the replicas that remain,
    /// so dropping a lagging follower lets it advance.
    pub fn remove_isr(&mut self, replica: i32) -> Result<(), Error> {
        self.ready()?;
        if replica == self.leader || !self.isr.remove(&replica) {
            return Err(Error::IneligibleLeader);
        }
        self.advance_watermark();
        Ok(())
    }

    /// Replace the leader with an ISR member that already holds the committed prefix.
    pub fn fail_over(&mut self, new_leader: i32) -> Result<(), Error> {
        self.ready()?;
        let caught_up = self.end_offset.get(&new_leader).copied().unwrap_or(0);
        if new_leader == self.leader
            || !self.isr.contains(&new_leader)
            || caught_up < self.high_watermark
        {
            return Err(Error::IneligibleLeader);
        }
        self.isr.remove(&self.leader);
        self.leader = new_leader;
        self.advance_watermark();
        Ok(())
    }

    /// Decide whether `acks` may succeed for `[base, base+count)`.
    ///
    /// `None` means the guarantee does not hold yet. Callers must not treat
    /// that as an acknowledgment. `acks=-1` records the ids only when the high
    /// watermark already covers them and the ISR is large enough.
    pub fn acknowledge(
        &mut self,
        acks: i16,
        base: i64,
        count: usize,
    ) -> Result<Option<Ack>, Error> {
        if count == 0 || base < 0 {
            return Err(Error::NotWritten);
        }
        let end = base
            .checked_add(i64::try_from(count).map_err(|_| Error::NotWritten)?)
            .ok_or(Error::NotWritten)?;
        let leader_end = self.end_offset.get(&self.leader).copied().unwrap_or(0);
        match acks {
            0 => Ok(Some(Ack::Local)),
            1 => {
                if leader_end >= end {
                    Ok(Some(Ack::Local))
                } else {
                    Ok(None)
                }
            }
            -1 => {
                if self.isr.len() < self.min_isr {
                    return Err(if leader_end >= end {
                        Error::NotEnoughReplicasAfterAppend
                    } else {
                        Error::NotEnoughReplicas
                    });
                }
                if self.high_watermark >= end {
                    self.record_commit(base, end)?;
                    Ok(Some(Ack::Committed))
                } else {
                    Ok(None)
                }
            }
            _ => Err(Error::InvalidAcks),
        }
    }

    fn advance_watermark(&mut self) {
        let Some(candidate) = self
            .isr
            .iter()
            .map(|replica| self.end_offset.get(replica).copied().unwrap_or(0))
            .min()
        else {
            return;
        };
        if candidate > self.high_watermark {
            self.high_watermark = candidate;
        }
    }

    fn record_commit(&mut self, base: i64, end: i64) -> Result<(), Error> {
        let mut newly = Vec::new();
        for offset in base..end {
            if let Some(id) = self.ids.get(&offset).copied() {
                if self.committed.insert(id) {
                    newly.push((offset, id));
                }
            }
        }
        if newly.is_empty() {
            return Ok(());
        }
        let payload = match encode_commit(&newly) {
            Ok(payload) => payload,
            Err(error) => {
                for (_, id) in &newly {
                    self.committed.remove(id);
                }
                return Err(error);
            }
        };
        if let Err(error) = self.persist(&payload) {
            for (_, id) in &newly {
                self.committed.remove(id);
            }
            return Err(error);
        }
        Ok(())
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn apply(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let (kind, mut rest) = split_kind(bytes)?;
        if kind != KIND_COMMIT {
            return Err(Error::InvalidLimits);
        }
        let count = usize::try_from(u32::from_be_bytes(
            take(&mut rest, 4)?
                .try_into()
                .map_err(|_| Error::InvalidLimits)?,
        ))
        .map_err(|_| Error::InvalidLimits)?;
        for _ in 0..count {
            let offset = i64::from_be_bytes(
                take(&mut rest, 8)?
                    .try_into()
                    .map_err(|_| Error::InvalidLimits)?,
            );
            let id = u64::from_be_bytes(
                take(&mut rest, 8)?
                    .try_into()
                    .map_err(|_| Error::InvalidLimits)?,
            );
            self.ids.insert(offset, id);
            self.committed.insert(id);
            self.high_watermark = self.high_watermark.max(offset.saturating_add(1));
        }
        if !rest.is_empty() {
            return Err(Error::InvalidLimits);
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
        return Err(Error::InvalidLimits);
    }
    Ok((bytes[RECORD_MAGIC.len()], &bytes[RECORD_MAGIC.len() + 1..]))
}

fn encode_commit(ids: &[(i64, u64)]) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(13 + ids.len() * 16);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_COMMIT);
    let count = u32::try_from(ids.len()).map_err(|_| Error::InvalidLimits)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (offset, id) in ids {
        bytes.extend_from_slice(&offset.to_be_bytes());
        bytes.extend_from_slice(&id.to_be_bytes());
    }
    Ok(bytes)
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidLimits);
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
        let journal = journal::Limits::new(256, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(32, journal).unwrap_or_else(|_| panic!("watermark limits"))
    }

    fn open(name: &str, min_isr: usize) -> (Durability, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-hw-{name}-{nanos}"));
        let state = Durability::open(&path, 1, min_isr, limits())
            .unwrap_or_else(|error| panic!("{error:?}"));
        (state, path)
    }

    #[test]
    fn acks_all_waits_for_the_isr_and_committed_ids_survive_failover() {
        let (mut state, path) = open("commit", 2);
        state.add_isr(2).unwrap_or_else(|error| panic!("{error:?}"));
        let base = state
            .append(&[10, 11])
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            state
                .acknowledge(-1, base, 2)
                .unwrap_or_else(|error| panic!("{error:?}")),
            None
        );
        assert!(!state.is_committed(10));
        state
            .replicate_to(2, 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(state.high_watermark(), 2);
        assert_eq!(
            state
                .acknowledge(-1, base, 2)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Some(Ack::Committed)
        );
        state
            .fail_over(2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(state.is_committed(10) && state.is_committed(11));
        drop(state);
        let restored =
            Durability::open(&path, 1, 2, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(restored.is_committed(10) && restored.is_committed(11));
        assert_eq!(restored.high_watermark(), 2);
    }

    #[test]
    fn below_min_isr_rejects_after_the_leader_append() {
        let (mut state, _) = open("short", 2);
        let base = state
            .append(&[7])
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            state.acknowledge(-1, base, 1),
            Err(Error::NotEnoughReplicasAfterAppend)
        ));
        assert!(!state.is_committed(7));
        assert_eq!(
            state
                .acknowledge(1, base, 1)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Some(Ack::Local)
        );
        assert!(matches!(
            state.acknowledge(2, base, 1),
            Err(Error::InvalidAcks)
        ));
    }

    #[test]
    fn removing_a_lagging_replica_advances_the_high_watermark() {
        let (mut state, _) = open("shrink", 1);
        state.add_isr(2).unwrap_or_else(|error| panic!("{error:?}"));
        let base = state
            .append(&[1, 2, 3])
            .unwrap_or_else(|error| panic!("{error:?}"));
        state
            .replicate_to(2, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(state.high_watermark(), 1);
        assert_eq!(
            state
                .acknowledge(-1, base, 3)
                .unwrap_or_else(|error| panic!("{error:?}")),
            None
        );
        state
            .remove_isr(2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(state.high_watermark(), 3);
        assert_eq!(
            state
                .acknowledge(-1, base, 3)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Some(Ack::Committed)
        );
        assert!(state.is_committed(1) && state.is_committed(2) && state.is_committed(3));
    }

    #[test]
    fn a_replica_outside_the_isr_can_catch_up_and_rejoin() {
        let (mut state, _) = open("rejoin", 1);
        state.add_isr(2).unwrap_or_else(|error| panic!("{error:?}"));
        state
            .append(&[1, 2, 3])
            .unwrap_or_else(|error| panic!("{error:?}"));
        state
            .replicate_to(2, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        state
            .remove_isr(2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(state.high_watermark(), 3);
        state
            .replicate_to(2, 3)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(state.high_watermark(), 3);
        state.add_isr(2).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(state.add_isr(4), Err(Error::IneligibleLeader)));
        state
            .replicate_to(4, 3)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(state.high_watermark(), 3);
        state.add_isr(4).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            state.replicate_to(-1, 0),
            Err(Error::IneligibleLeader)
        ));
        assert!(state.isr().any(|replica| replica == 4));
    }

    #[test]
    fn an_append_past_the_end_of_i64_does_not_store_a_partial_batch() {
        let (mut state, _) = open("end", 1);
        state
            .append(&[1])
            .unwrap_or_else(|error| panic!("{error:?}"));
        let _ = state.end_offset.insert(state.leader, i64::MAX);
        assert!(matches!(state.append(&[2, 3]), Err(Error::InvalidLimits)));
        assert!(!state.ids.contains_key(&i64::MAX));
        assert_eq!(state.end_offset.get(&state.leader).copied(), Some(i64::MAX));
        assert_eq!(state.high_watermark(), 1);
    }
}
