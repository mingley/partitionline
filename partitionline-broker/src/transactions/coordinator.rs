//! Durable transaction coordinator state and epoch fencing.
//!
//! `init` allocates or bumps a producer epoch for a transactional id and fences
//! the previous epoch. A transaction that is preparing commit or abort stays in
//! that state. `begin` and `add_partitions` require the current producer
//! epoch and an ongoing transaction. `end` only prepares commit or abort.
//! `finish` records completion. A retry of the same decision is idempotent; the
//! opposite decision is `INVALID_TXN_STATE`. An older producer epoch and a stale
//! coordinator epoch cannot commit.
//!
//! Every transition is journaled, so coordinator failover reopens the same
//! state. This is not the InitProducerId, AddPartitionsToTxn, or EndTxn RPC.

use crate::catalog;
use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLTXNC01";
const CHECKPOINT_MAGIC: &[u8; 8] = b"PLTXCP01";
const KIND_COORDINATOR: u8 = 1;
const KIND_INIT: u8 = 2;
const KIND_BEGIN: u8 = 3;
const KIND_ADD: u8 = 4;
const KIND_PREPARE: u8 = 5;
const KIND_FINISH: u8 = 6;

/// Kafka `COORDINATOR_NOT_AVAILABLE`.
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// Kafka `INVALID_TXN_STATE`.
pub const INVALID_TXN_STATE: i16 = 48;
/// Kafka `INVALID_PRODUCER_ID_MAPPING`.
pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
/// Kafka `CONCURRENT_TRANSACTIONS`.
pub const CONCURRENT_TRANSACTIONS: i16 = 51;
/// Kafka `PRODUCER_FENCED`.
pub const PRODUCER_FENCED: i16 = 90;

/// Where a transactional id sits in the coordinator state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Initialized and not inside a transaction.
    Empty,
    /// `begin` succeeded and partitions may be added.
    Ongoing,
    /// Commit was chosen and `finish` has not been recorded.
    PrepareCommit,
    /// Abort was chosen and `finish` has not been recorded.
    PrepareAbort,
    /// Commit completed.
    CompleteCommit,
    /// Abort completed.
    CompleteAbort,
}

/// Result of `end` or `finish`. Prepared is not a committed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The decision is durable and still incomplete.
    Prepared {
        /// True when the decision was commit.
        commit: bool,
    },
    /// Completion is durable.
    Completed {
        /// True when the completed decision was commit.
        commit: bool,
    },
}

/// Why a coordinator transition was refused.
#[derive(Debug)]
pub enum Error {
    /// The coordinator epoch is not current.
    StaleCoordinator,
    /// The producer epoch is older than the current one.
    ProducerFenced,
    /// The producer id does not match the transactional id.
    InvalidMapping,
    /// The state does not allow this transition.
    InvalidState,
    /// A transaction is already open.
    Concurrent,
    /// The transactional id, partition, or epoch is not usable.
    InvalidRequest,
    /// A persistence failure requires reopening before reads or writes.
    Poisoned,
    /// The limits cannot store the transactions.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a caller-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::StaleCoordinator => COORDINATOR_NOT_AVAILABLE,
            Self::ProducerFenced => PRODUCER_FENCED,
            Self::InvalidMapping => INVALID_PRODUCER_ID_MAPPING,
            Self::InvalidState | Self::InvalidRequest | Self::InvalidLimits => INVALID_TXN_STATE,
            Self::Concurrent => CONCURRENT_TRANSACTIONS,
            Self::Poisoned | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Transaction and journal bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_transactions: usize,
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive caps. A checkpoint must fit a single journal entry.
    pub fn new(
        max_transactions: usize,
        max_records: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_transactions)
            || !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < 512
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_transactions,
            max_records,
            journal,
        })
    }
}

type TxnPartitions = BTreeMap<String, BTreeSet<i32>>;

fn txn_partition_count(parts: &TxnPartitions) -> usize {
    parts.values().map(BTreeSet::len).sum()
}

fn txn_has_partition(parts: &TxnPartitions, topic: &str, partition: i32) -> bool {
    parts.get(topic).is_some_and(|set| set.contains(&partition))
}

fn txn_add_partition(parts: &mut TxnPartitions, topic: &str, partition: i32) {
    if let Some(set) = parts.get_mut(topic) {
        let _ = set.insert(partition);
        return;
    }
    let mut set = BTreeSet::new();
    let _ = set.insert(partition);
    let _ = parts.insert(topic.to_owned(), set);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Transaction {
    producer_id: i64,
    producer_epoch: i16,
    state: State,
    partitions: TxnPartitions,
}

/// Durable coordinator for transactional ids.
pub struct Coordinator {
    limits: Limits,
    epoch: i64,
    next_producer: i64,
    transactions: BTreeMapTxn,
    journal: Journal,
    stored: usize,
    poisoned: bool,
    /// Reused journal record. Cleared and rewritten on each append.
    encoded: Vec<u8>,
    /// Reused complete-state image, built only on rollover.
    checkpoint: Vec<u8>,
    /// Exact byte length of a snapshot of the current logical state.
    snapshot_len: usize,
    /// An older event-only history may exceed the single-entry snapshot limit.
    legacy_uncheckpointable: bool,
}

type BTreeMapTxn = std::collections::BTreeMap<String, Transaction>;

impl Coordinator {
    /// Open or recover coordinator state. The epoch starts at 0.
    ///
    /// The adjacent `<path>.checkpoint.tmp` name is reserved for atomic
    /// checkpoint publication and must not be used by other applications.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut coordinator = Self {
            limits,
            epoch: 0,
            next_producer: 1,
            transactions: BTreeMapTxn::new(),
            journal,
            stored: 0,
            poisoned: false,
            encoded: Vec::new(),
            checkpoint: Vec::new(),
            snapshot_len: CHECKPOINT_MAGIC.len() + 8 + 8 + 4,
            legacy_uncheckpointable: false,
        };
        let mut offset = coordinator.journal.base_offset();
        loop {
            let batch = coordinator.journal.fetch(
                offset,
                32.min(coordinator.limits.journal.max_index_entries()),
                coordinator.limits.journal.max_fetch_bytes(),
            )?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                if entry.payload.get(..8) == Some(CHECKPOINT_MAGIC.as_slice()) {
                    if entry.first_offset != coordinator.journal.base_offset()
                        || coordinator.stored != 0
                    {
                        return Err(Error::InvalidRequest);
                    }
                    coordinator.apply_checkpoint(&entry.payload)?;
                } else {
                    coordinator.apply(&entry.payload)?;
                }
                coordinator.stored = coordinator.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        coordinator.snapshot_len = checkpoint_length(&coordinator.transactions)?;
        if !coordinator.checkpointable(coordinator.snapshot_len) {
            coordinator.legacy_uncheckpointable = true;
        }
        Ok(coordinator)
    }

    /// Current coordinator epoch.
    pub fn epoch(&self) -> Result<i64, Error> {
        self.ready()?;
        Ok(self.epoch)
    }

    /// State of a transactional id, if it has been initialized.
    pub fn state(&self, transactional_id: &str) -> Result<Option<State>, Error> {
        self.ready()?;
        Ok(self.transactions.get(transactional_id).map(|txn| txn.state))
    }

    /// Move the coordinator lineage forward. A smaller or equal epoch is stale.
    pub fn advance(&mut self, epoch: i64) -> Result<i64, Error> {
        self.ready()?;
        if epoch <= self.epoch {
            return Err(Error::StaleCoordinator);
        }
        self.append_encoded(|bytes| write_epoch(bytes, epoch))?;
        self.epoch = epoch;
        Ok(epoch)
    }

    /// Allocate or bump the producer epoch for `transactional_id`.
    ///
    /// A transaction that is preparing commit or abort is left unchanged. That
    /// decision has to be finished before the epoch can move.
    pub fn init(
        &mut self,
        transactional_id: &str,
        coordinator_epoch: i64,
    ) -> Result<(i64, i16), Error> {
        self.ready()?;
        self.require_coordinator(coordinator_epoch)?;
        validate_id(transactional_id)?;
        if let Some(existing) = self.transactions.get(transactional_id) {
            if matches!(existing.state, State::PrepareCommit | State::PrepareAbort) {
                return Err(Error::Concurrent);
            }
            if existing.producer_epoch == i16::MAX {
                return Err(Error::InvalidRequest);
            }
            let epoch = existing.producer_epoch + 1;
            let producer_id = existing.producer_id;
            self.append_encoded(|bytes| write_init(bytes, transactional_id, producer_id, epoch))?;
            let txn = self
                .transactions
                .get_mut(transactional_id)
                .ok_or(Error::InvalidState)?;
            txn.producer_epoch = epoch;
            txn.state = State::Empty;
            txn.partitions.clear();
            return Ok((producer_id, epoch));
        }
        if self.transactions.len() >= self.limits.max_transactions {
            return Err(Error::InvalidLimits);
        }
        let producer_id = self.next_producer;
        let next_producer = self
            .next_producer
            .checked_add(1)
            .ok_or(Error::InvalidRequest)?;
        self.append_encoded(|bytes| write_init(bytes, transactional_id, producer_id, 0))?;
        self.transactions.insert(
            transactional_id.to_owned(),
            Transaction {
                producer_id,
                producer_epoch: 0,
                state: State::Empty,
                partitions: BTreeMap::new(),
            },
        );
        self.next_producer = next_producer;
        Ok((producer_id, 0))
    }

    /// Start a transaction. An ongoing transaction is left unchanged.
    pub fn begin(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i64,
    ) -> Result<(), Error> {
        self.ready()?;
        self.require_coordinator(coordinator_epoch)?;
        let txn = self.current(transactional_id, producer_id, producer_epoch)?;
        match txn.state {
            State::Empty | State::CompleteCommit | State::CompleteAbort => {}
            State::Ongoing | State::PrepareCommit | State::PrepareAbort => {
                return Err(Error::Concurrent)
            }
        }
        self.append_encoded(|bytes| write_begin(bytes, transactional_id))?;
        let txn = self
            .transactions
            .get_mut(transactional_id)
            .ok_or(Error::InvalidState)?;
        txn.state = State::Ongoing;
        txn.partitions.clear();
        Ok(())
    }

    /// Add one partition to the ongoing transaction.
    ///
    /// A partition that is already in the transaction does not consume another
    /// journal record.
    pub fn add_partition(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i64,
        topic: &str,
        partition: i32,
    ) -> Result<(), Error> {
        self.ready()?;
        self.require_coordinator(coordinator_epoch)?;
        catalog::validate_topic_name(topic).map_err(|_| Error::InvalidRequest)?;
        if partition < 0 {
            return Err(Error::InvalidRequest);
        }
        let txn = self.current(transactional_id, producer_id, producer_epoch)?;
        if txn.state != State::Ongoing {
            return Err(Error::InvalidState);
        }
        if txn_has_partition(&txn.partitions, topic, partition) {
            return Ok(());
        }
        if txn_partition_count(&txn.partitions) >= 64 {
            return Err(Error::InvalidRequest);
        }
        self.append_encoded(|bytes| write_add(bytes, transactional_id, topic, partition))?;
        let txn = self
            .transactions
            .get_mut(transactional_id)
            .ok_or(Error::InvalidState)?;
        txn_add_partition(&mut txn.partitions, topic, partition);
        Ok(())
    }

    /// Prepare commit or abort. This does not complete the transaction.
    pub fn end(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i64,
        commit: bool,
    ) -> Result<Outcome, Error> {
        self.ready()?;
        self.require_coordinator(coordinator_epoch)?;
        let txn = self.current(transactional_id, producer_id, producer_epoch)?;
        let prepared = if commit {
            State::PrepareCommit
        } else {
            State::PrepareAbort
        };
        let completed = if commit {
            State::CompleteCommit
        } else {
            State::CompleteAbort
        };
        match txn.state {
            State::Ongoing => {
                self.append_encoded(|bytes| write_prepare(bytes, transactional_id, commit))?;
                let txn = self
                    .transactions
                    .get_mut(transactional_id)
                    .ok_or(Error::InvalidState)?;
                txn.state = prepared;
                Ok(Outcome::Prepared { commit })
            }
            state if state == prepared => Ok(Outcome::Prepared { commit }),
            state if state == completed => Ok(Outcome::Completed { commit }),
            _ => Err(Error::InvalidState),
        }
    }

    /// Record completion of a prepared transaction. A repeat is the same outcome.
    pub fn finish(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i64,
    ) -> Result<Outcome, Error> {
        self.ready()?;
        self.require_coordinator(coordinator_epoch)?;
        let txn = self.current(transactional_id, producer_id, producer_epoch)?;
        let commit = match txn.state {
            State::PrepareCommit => true,
            State::PrepareAbort => false,
            State::CompleteCommit => return Ok(Outcome::Completed { commit: true }),
            State::CompleteAbort => return Ok(Outcome::Completed { commit: false }),
            _ => return Err(Error::InvalidState),
        };
        self.append_encoded(|bytes| write_finish(bytes, transactional_id, commit))?;
        let txn = self
            .transactions
            .get_mut(transactional_id)
            .ok_or(Error::InvalidState)?;
        txn.state = if commit {
            State::CompleteCommit
        } else {
            State::CompleteAbort
        };
        Ok(Outcome::Completed { commit })
    }

    fn current(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<&Transaction, Error> {
        let Some(txn) = self.transactions.get(transactional_id) else {
            return Err(Error::InvalidMapping);
        };
        if txn.producer_id != producer_id {
            return Err(Error::InvalidMapping);
        }
        if txn.producer_epoch != producer_epoch {
            return Err(Error::ProducerFenced);
        }
        Ok(txn)
    }

    fn require_coordinator(&self, epoch: i64) -> Result<(), Error> {
        if epoch == self.epoch {
            Ok(())
        } else {
            Err(Error::StaleCoordinator)
        }
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn apply_checkpoint(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let (epoch, next_producer, transactions) = decode_checkpoint(bytes, self.limits)?;
        self.epoch = epoch;
        self.next_producer = next_producer;
        self.transactions = transactions;
        Ok(())
    }

    fn apply(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let mut state = StateRef {
            epoch: &mut self.epoch,
            next_producer: &mut self.next_producer,
            transactions: &mut self.transactions,
        };
        apply_event(&mut state, self.limits, bytes)
    }

    fn append_encoded(
        &mut self,
        write: impl FnOnce(&mut Vec<u8>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.encoded.clear();
        write(&mut self.encoded)?;
        let payload = std::mem::take(&mut self.encoded);
        let result = self.persist(&payload);
        self.encoded = payload;
        result
    }

    fn checkpointable(&self, len: usize) -> bool {
        self.journal.snapshot_fits(len)
            && len
                .checked_add(std::mem::size_of::<journal::Entry>())
                .is_some_and(|size| size <= self.limits.journal.max_fetch_bytes())
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        let next_len = prospective_checkpoint_len(self, payload)?;
        let next_fits = self.checkpointable(next_len);
        if (!next_fits && !self.legacy_uncheckpointable)
            || payload
                .len()
                .checked_add(std::mem::size_of::<journal::Entry>())
                .is_none_or(|size| size > self.limits.journal.max_fetch_bytes())
        {
            return Err(Error::InvalidLimits);
        }
        if self.stored < self.limits.max_records && self.journal.can_append_snapshot(payload.len())
        {
            return match self.journal.append(1, payload) {
                Ok(_) => {
                    self.stored = self.stored.saturating_add(1);
                    self.snapshot_len = next_len;
                    self.legacy_uncheckpointable = !next_fits;
                    Ok(())
                }
                Err(error) => {
                    self.poisoned = true;
                    Err(error.into())
                }
            };
        }
        if !next_fits {
            // Existing legacy state remains readable and appendable while its
            // original journal budgets permit, even if one checkpoint cannot
            // represent it. Do not claim rollover for this compatibility path.
            return Err(Error::InvalidLimits);
        }
        let (kind, mut rest) = split_kind(payload)?;
        let id = if kind == KIND_COORDINATOR {
            None
        } else {
            Some(take_str_ref(&mut rest)?)
        };
        let mut epoch = self.epoch;
        let mut next_producer = self.next_producer;
        let mut modified = BTreeMapTxn::new();
        if let Some(id) = id {
            if let Some(txn) = self.transactions.get(id) {
                modified.insert(id.to_owned(), txn.clone());
            }
        }
        apply_event(
            &mut StateRef {
                epoch: &mut epoch,
                next_producer: &mut next_producer,
                transactions: &mut modified,
            },
            self.limits,
            payload,
        )?;
        let overlay = match id {
            Some(id) => Some((id, modified.get(id).ok_or(Error::InvalidState)?)),
            None => None,
        };
        let mut checkpoint = std::mem::take(&mut self.checkpoint);
        let prepared = encode_checkpoint(
            epoch,
            next_producer,
            &self.transactions,
            overlay,
            next_len,
            &mut checkpoint,
        );
        if let Err(error) = prepared {
            self.checkpoint = checkpoint;
            return Err(error);
        }
        let result = self.journal.replace_with_snapshot(&checkpoint);
        self.checkpoint = checkpoint;
        match result {
            Ok(()) => {
                self.stored = 1;
                self.snapshot_len = next_len;
                self.legacy_uncheckpointable = false;
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error.into())
            }
        }
    }
}

struct StateRef<'a> {
    epoch: &'a mut i64,
    next_producer: &'a mut i64,
    transactions: &'a mut BTreeMapTxn,
}

fn apply_event(state: &mut StateRef<'_>, limits: Limits, bytes: &[u8]) -> Result<(), Error> {
    let (kind, mut rest) = split_kind(bytes)?;
    match kind {
        KIND_COORDINATOR => {
            let epoch = take_i64(&mut rest)?;
            if !rest.is_empty() || epoch <= *state.epoch {
                return Err(Error::InvalidRequest);
            }
            *state.epoch = epoch;
        }
        KIND_INIT => {
            let id = take_str(&mut rest)?;
            let producer_id = take_i64(&mut rest)?;
            let epoch = take_i16(&mut rest)?;
            if !rest.is_empty() {
                return Err(Error::InvalidRequest);
            }
            validate_id(&id)?;
            if let Some(existing) = state.transactions.get(&id) {
                if matches!(existing.state, State::PrepareCommit | State::PrepareAbort) {
                    return Err(Error::InvalidState);
                }
                if producer_id != existing.producer_id
                    || existing.producer_epoch.checked_add(1) != Some(epoch)
                {
                    return Err(Error::InvalidRequest);
                }
                let txn = state.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
                txn.producer_epoch = epoch;
                txn.state = State::Empty;
                txn.partitions.clear();
            } else {
                if state.transactions.len() >= limits.max_transactions {
                    return Err(Error::InvalidLimits);
                }
                if producer_id != *state.next_producer || epoch != 0 {
                    return Err(Error::InvalidRequest);
                }
                let next = state
                    .next_producer
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
                state.transactions.insert(
                    id,
                    Transaction {
                        producer_id,
                        producer_epoch: 0,
                        state: State::Empty,
                        partitions: BTreeMap::new(),
                    },
                );
                *state.next_producer = next;
            }
        }
        KIND_BEGIN => {
            let id = take_str(&mut rest)?;
            if !rest.is_empty() {
                return Err(Error::InvalidRequest);
            }
            let txn = state.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
            match txn.state {
                State::Empty | State::CompleteCommit | State::CompleteAbort => {
                    txn.state = State::Ongoing;
                    txn.partitions.clear();
                }
                State::Ongoing | State::PrepareCommit | State::PrepareAbort => {
                    return Err(Error::InvalidState);
                }
            }
        }
        KIND_ADD => {
            let id = take_str(&mut rest)?;
            let topic = take_str(&mut rest)?;
            let partition = take_i32(&mut rest)?;
            if !rest.is_empty() || catalog::validate_topic_name(&topic).is_err() || partition < 0 {
                return Err(Error::InvalidRequest);
            }
            let txn = state.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
            if txn.state != State::Ongoing {
                return Err(Error::InvalidState);
            }
            if txn_partition_count(&txn.partitions) >= 64
                && !txn_has_partition(&txn.partitions, &topic, partition)
            {
                return Err(Error::InvalidRequest);
            }
            txn_add_partition(&mut txn.partitions, &topic, partition);
        }
        KIND_PREPARE => {
            let id = take_str(&mut rest)?;
            let commit = take_bool(&mut rest)?;
            if !rest.is_empty() {
                return Err(Error::InvalidRequest);
            }
            let txn = state.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
            let prepared = if commit {
                State::PrepareCommit
            } else {
                State::PrepareAbort
            };
            match txn.state {
                State::Ongoing => txn.state = prepared,
                _ => return Err(Error::InvalidState),
            }
        }
        KIND_FINISH => {
            let id = take_str(&mut rest)?;
            let commit = take_bool(&mut rest)?;
            if !rest.is_empty() {
                return Err(Error::InvalidRequest);
            }
            let txn = state.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
            let completed = if commit {
                State::CompleteCommit
            } else {
                State::CompleteAbort
            };
            match txn.state {
                State::PrepareCommit if commit => txn.state = completed,
                State::PrepareAbort if !commit => txn.state = completed,
                _ => return Err(Error::InvalidState),
            }
        }
        _ => return Err(Error::InvalidRequest),
    }
    Ok(())
}

fn state_byte(state: State) -> u8 {
    match state {
        State::Empty => 0,
        State::Ongoing => 1,
        State::PrepareCommit => 2,
        State::PrepareAbort => 3,
        State::CompleteCommit => 4,
        State::CompleteAbort => 5,
    }
}

fn partition_bytes(partitions: &TxnPartitions) -> Result<usize, Error> {
    let mut len = 0usize;
    for (topic, values) in partitions {
        let _ = u16::try_from(topic.len()).map_err(|_| Error::InvalidLimits)?;
        len = len
            .checked_add(
                (2 + topic.len() + 4)
                    .checked_mul(values.len())
                    .ok_or(Error::InvalidLimits)?,
            )
            .ok_or(Error::InvalidLimits)?;
    }
    Ok(len)
}

fn checkpoint_length(transactions: &BTreeMapTxn) -> Result<usize, Error> {
    let _ = u32::try_from(transactions.len()).map_err(|_| Error::InvalidLimits)?;
    let mut len = CHECKPOINT_MAGIC.len() + 8 + 8 + 4;
    for (id, txn) in transactions {
        let _ = u16::try_from(id.len()).map_err(|_| Error::InvalidLimits)?;
        let _ = u16::try_from(txn_partition_count(&txn.partitions))
            .map_err(|_| Error::InvalidLimits)?;
        len = len
            .checked_add(2 + id.len() + 8 + 2 + 1 + 2)
            .and_then(|len| len.checked_add(partition_bytes(&txn.partitions).ok()?))
            .ok_or(Error::InvalidLimits)?;
    }
    Ok(len)
}

fn take_str_ref<'a>(input: &mut &'a [u8]) -> Result<&'a str, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ));
    std::str::from_utf8(take(input, len)?).map_err(|_| Error::InvalidRequest)
}

fn prospective_checkpoint_len(coordinator: &Coordinator, payload: &[u8]) -> Result<usize, Error> {
    let (kind, mut rest) = split_kind(payload)?;
    let len = coordinator.snapshot_len;
    match kind {
        KIND_COORDINATOR | KIND_PREPARE | KIND_FINISH => Ok(len),
        KIND_INIT | KIND_BEGIN => {
            let id = take_str_ref(&mut rest)?;
            if let Some(txn) = coordinator.transactions.get(id) {
                len.checked_sub(partition_bytes(&txn.partitions)?)
                    .ok_or(Error::InvalidLimits)
            } else if kind == KIND_INIT {
                len.checked_add(2 + id.len() + 8 + 2 + 1 + 2)
                    .ok_or(Error::InvalidLimits)
            } else {
                Err(Error::InvalidState)
            }
        }
        KIND_ADD => {
            let id = take_str_ref(&mut rest)?;
            let topic = take_str_ref(&mut rest)?;
            let partition = take_i32(&mut rest)?;
            let txn = coordinator
                .transactions
                .get(id)
                .ok_or(Error::InvalidState)?;
            if txn_has_partition(&txn.partitions, topic, partition) {
                Ok(len)
            } else {
                len.checked_add(2 + topic.len() + 4)
                    .ok_or(Error::InvalidLimits)
            }
        }
        _ => Err(Error::InvalidRequest),
    }
}

fn encode_checkpoint_transaction(
    out: &mut Vec<u8>,
    id: &str,
    txn: &Transaction,
) -> Result<(), Error> {
    put_str(out, id)?;
    out.extend_from_slice(&txn.producer_id.to_be_bytes());
    out.extend_from_slice(&txn.producer_epoch.to_be_bytes());
    out.push(state_byte(txn.state));
    out.extend_from_slice(
        &u16::try_from(txn_partition_count(&txn.partitions))
            .map_err(|_| Error::InvalidLimits)?
            .to_be_bytes(),
    );
    for (topic, partitions) in &txn.partitions {
        for partition in partitions {
            put_str(out, topic)?;
            out.extend_from_slice(&partition.to_be_bytes());
        }
    }
    Ok(())
}

fn encode_checkpoint(
    epoch: i64,
    next_producer: i64,
    transactions: &BTreeMapTxn,
    overlay: Option<(&str, &Transaction)>,
    len: usize,
    out: &mut Vec<u8>,
) -> Result<(), Error> {
    let added = overlay.is_some_and(|(id, _)| !transactions.contains_key(id));
    let count = transactions
        .len()
        .checked_add(usize::from(added))
        .ok_or(Error::InvalidLimits)?;
    let mut calculated = CHECKPOINT_MAGIC.len() + 8 + 8 + 4;
    for (id, txn) in transactions {
        let txn = overlay
            .filter(|(key, _)| *key == id.as_str())
            .map_or(txn, |(_, txn)| txn);
        calculated = calculated
            .checked_add(2 + id.len() + 8 + 2 + 1 + 2)
            .and_then(|size| size.checked_add(partition_bytes(&txn.partitions).ok()?))
            .ok_or(Error::InvalidLimits)?;
    }
    if let Some((id, txn)) = overlay.filter(|_| added) {
        calculated = calculated
            .checked_add(2 + id.len() + 8 + 2 + 1 + 2)
            .and_then(|size| size.checked_add(partition_bytes(&txn.partitions).ok()?))
            .ok_or(Error::InvalidLimits)?;
    }
    if calculated != len {
        return Err(Error::InvalidLimits);
    }
    out.clear();
    out.try_reserve_exact(len)
        .map_err(|_| Error::InvalidLimits)?;
    out.extend_from_slice(CHECKPOINT_MAGIC);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.extend_from_slice(&next_producer.to_be_bytes());
    out.extend_from_slice(
        &u32::try_from(count)
            .map_err(|_| Error::InvalidLimits)?
            .to_be_bytes(),
    );
    let mut inserted = false;
    for (id, txn) in transactions {
        if let Some((key, value)) = overlay {
            if added && !inserted && key < id.as_str() {
                encode_checkpoint_transaction(out, key, value)?;
                inserted = true;
            }
            if key == id.as_str() {
                encode_checkpoint_transaction(out, id, value)?;
                continue;
            }
        }
        encode_checkpoint_transaction(out, id, txn)?;
    }
    if let Some((key, value)) = overlay {
        if added && !inserted {
            encode_checkpoint_transaction(out, key, value)?;
        }
    }
    debug_assert_eq!(out.len(), len);
    Ok(())
}

fn decode_checkpoint(bytes: &[u8], limits: Limits) -> Result<(i64, i64, BTreeMapTxn), Error> {
    if bytes.get(..8) != Some(CHECKPOINT_MAGIC.as_slice()) {
        return Err(Error::InvalidRequest);
    }
    let mut bytes = &bytes[8..];
    let epoch = take_i64(&mut bytes)?;
    let next_producer = take_i64(&mut bytes)?;
    let count = usize::try_from(u32::from_be_bytes(
        take(&mut bytes, 4)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ))
    .map_err(|_| Error::InvalidLimits)?;
    if epoch < 0
        || count > limits.max_transactions
        || count > bytes.len() / 16
        || i64::try_from(count)
            .ok()
            .and_then(|count| count.checked_add(1))
            != Some(next_producer)
    {
        return Err(Error::InvalidRequest);
    }
    let mut transactions = BTreeMapTxn::new();
    let mut producer_ids = BTreeSet::new();
    for _ in 0..count {
        let id = take_str(&mut bytes)?;
        validate_id(&id)?;
        let producer_id = take_i64(&mut bytes)?;
        let producer_epoch = take_i16(&mut bytes)?;
        let state = match take(&mut bytes, 1)?[0] {
            0 => State::Empty,
            1 => State::Ongoing,
            2 => State::PrepareCommit,
            3 => State::PrepareAbort,
            4 => State::CompleteCommit,
            5 => State::CompleteAbort,
            _ => return Err(Error::InvalidRequest),
        };
        let partition_count = usize::from(u16::from_be_bytes(
            take(&mut bytes, 2)?
                .try_into()
                .map_err(|_| Error::InvalidRequest)?,
        ));
        if producer_id <= 0
            || producer_id >= next_producer
            || producer_epoch < 0
            || !producer_ids.insert(producer_id)
            || partition_count > 64
            || (state == State::Empty && partition_count != 0)
        {
            return Err(Error::InvalidRequest);
        }
        let mut partitions = TxnPartitions::new();
        for _ in 0..partition_count {
            let topic = take_str(&mut bytes)?;
            let partition = take_i32(&mut bytes)?;
            if catalog::validate_topic_name(&topic).is_err()
                || partition < 0
                || txn_has_partition(&partitions, &topic, partition)
            {
                return Err(Error::InvalidRequest);
            }
            txn_add_partition(&mut partitions, &topic, partition);
        }
        if transactions
            .insert(
                id,
                Transaction {
                    producer_id,
                    producer_epoch,
                    state,
                    partitions,
                },
            )
            .is_some()
        {
            return Err(Error::InvalidRequest);
        }
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok((epoch, next_producer, transactions))
}

fn validate_id(id: &str) -> Result<(), Error> {
    if id.is_empty() || id.len() > 249 || id.chars().any(|c| c.is_control()) {
        Err(Error::InvalidRequest)
    } else {
        Ok(())
    }
}

fn split_kind(bytes: &[u8]) -> Result<(u8, &[u8]), Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    Ok((bytes[RECORD_MAGIC.len()], &bytes[RECORD_MAGIC.len() + 1..]))
}

fn write_epoch(bytes: &mut Vec<u8>, epoch: i64) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_COORDINATOR);
    bytes.extend_from_slice(&epoch.to_be_bytes());
    Ok(())
}

#[cfg(test)]
fn encode_init(id: &str, producer_id: i64, epoch: i16) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    write_init(&mut bytes, id, producer_id, epoch)?;
    Ok(bytes)
}

fn write_init(bytes: &mut Vec<u8>, id: &str, producer_id: i64, epoch: i16) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_INIT);
    put_str(bytes, id)?;
    bytes.extend_from_slice(&producer_id.to_be_bytes());
    bytes.extend_from_slice(&epoch.to_be_bytes());
    Ok(())
}

fn write_begin(bytes: &mut Vec<u8>, id: &str) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_BEGIN);
    put_str(bytes, id)?;
    Ok(())
}

#[cfg(test)]
fn encode_add(id: &str, topic: &str, partition: i32) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    write_add(&mut bytes, id, topic, partition)?;
    Ok(bytes)
}

fn write_add(bytes: &mut Vec<u8>, id: &str, topic: &str, partition: i32) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_ADD);
    put_str(bytes, id)?;
    put_str(bytes, topic)?;
    bytes.extend_from_slice(&partition.to_be_bytes());
    Ok(())
}

fn write_prepare(bytes: &mut Vec<u8>, id: &str, commit: bool) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_PREPARE);
    put_str(bytes, id)?;
    bytes.push(u8::from(commit));
    Ok(())
}

#[cfg(test)]
fn encode_finish(id: &str, commit: bool) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    write_finish(&mut bytes, id, commit)?;
    Ok(bytes)
}

fn write_finish(bytes: &mut Vec<u8>, id: &str, commit: bool) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_FINISH);
    put_str(bytes, id)?;
    bytes.push(u8::from(commit));
    Ok(())
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidRequest);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ));
    let bytes = take(input, len)?;
    std::str::from_utf8(bytes)
        .map(|text| text.to_owned())
        .map_err(|_| Error::InvalidRequest)
}

fn take_i64(input: &mut &[u8]) -> Result<i64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(i64::from_be_bytes(value))
}

fn take_i32(input: &mut &[u8]) -> Result<i32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(i32::from_be_bytes(value))
}

fn take_i16(input: &mut &[u8]) -> Result<i16, Error> {
    let mut value = [0u8; 2];
    value.copy_from_slice(take(input, 2)?);
    Ok(i16::from_be_bytes(value))
}

fn take_bool(input: &mut &[u8]) -> Result<bool, Error> {
    match take(input, 1)?[0] {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::InvalidRequest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(512, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, 64, journal).unwrap_or_else(|_| panic!("coordinator limits"))
    }

    fn open(name: &str) -> (Coordinator, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-txn-{name}-{nanos}"));
        let coordinator =
            Coordinator::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (coordinator, path)
    }

    #[test]
    fn prepared_commit_survives_failover_and_an_old_epoch_cannot_finish() {
        let (mut coordinator, path) = open("commit");
        let (producer, epoch) = coordinator
            .init("orders-txn", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .begin("orders-txn", producer, epoch, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            coordinator
                .end("orders-txn", producer, epoch, 0, true)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Outcome::Prepared { commit: true }
        );
        assert!(matches!(
            coordinator.end("orders-txn", producer, epoch, 0, false),
            Err(Error::InvalidState)
        ));
        assert_eq!(
            coordinator.state("orders-txn").unwrap(),
            Some(State::PrepareCommit)
        );
        coordinator
            .advance(1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.finish("orders-txn", producer, epoch, 0),
            Err(Error::StaleCoordinator)
        ));
        drop(coordinator);
        let mut restored =
            Coordinator::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.epoch().unwrap(), 1);
        assert_eq!(
            restored.state("orders-txn").unwrap(),
            Some(State::PrepareCommit)
        );
        assert_eq!(
            restored
                .finish("orders-txn", producer, epoch, 1)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Outcome::Completed { commit: true }
        );
        let bumped = restored
            .init("orders-txn", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(bumped.0, producer);
        assert_eq!(bumped.1, epoch + 1);
        assert!(matches!(
            restored.finish("orders-txn", producer, epoch, 1),
            Err(Error::ProducerFenced)
        ));
    }

    #[test]
    fn a_second_begin_does_not_reset_an_ongoing_transaction() {
        let (mut coordinator, _) = open("concurrent");
        let (producer, epoch) = coordinator
            .init("orders-txn", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .begin("orders-txn", producer, epoch, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.begin("orders-txn", producer, epoch, 0),
            Err(Error::Concurrent)
        ));
        assert_eq!(
            coordinator.state("orders-txn").unwrap(),
            Some(State::Ongoing)
        );
    }

    #[test]
    fn init_leaves_a_prepared_transaction_in_place() {
        let (mut coordinator, path) = open("prepare-init");
        let (producer, epoch) = coordinator
            .init("orders-txn", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .begin("orders-txn", producer, epoch, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .end("orders-txn", producer, epoch, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.init("orders-txn", 0),
            Err(Error::Concurrent)
        ));
        assert_eq!(
            coordinator.state("orders-txn").unwrap(),
            Some(State::PrepareCommit)
        );
        drop(coordinator);
        let mut restored =
            Coordinator::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            restored.state("orders-txn").unwrap(),
            Some(State::PrepareCommit)
        );
        assert_eq!(
            restored
                .finish("orders-txn", producer, epoch, 0)
                .unwrap_or_else(|error| panic!("{error:?}")),
            Outcome::Completed { commit: true }
        );
    }

    #[test]
    fn a_replayed_init_cannot_skip_the_producer_sequence() {
        let (mut coordinator, _) = open("replay-init");
        let skipped = encode_init("orders-txn", 9, 0).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.apply(&skipped),
            Err(Error::InvalidRequest)
        ));
        let (producer, epoch) = coordinator
            .init("orders-txn", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .begin("orders-txn", producer, epoch, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .end("orders-txn", producer, epoch, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let during = encode_init("orders-txn", producer, epoch.saturating_add(1))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.apply(&during),
            Err(Error::InvalidState)
        ));
        coordinator
            .finish("orders-txn", producer, epoch, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let jumped = encode_init("orders-txn", producer, epoch.saturating_add(2))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.apply(&jumped),
            Err(Error::InvalidRequest)
        ));
        assert_eq!(
            coordinator.state("orders-txn").unwrap(),
            Some(State::CompleteCommit)
        );
        assert_eq!(coordinator.next_producer, producer.saturating_add(1));
    }

    #[test]
    fn a_replayed_add_cannot_change_a_prepared_transaction() {
        let (mut coordinator, _) = open("replay-add");
        let (producer, epoch) = coordinator
            .init("orders-txn", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .begin("orders-txn", producer, epoch, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let again =
            encode_add("orders-txn", "orders", 0).unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .apply(&again)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let negative =
            encode_add("orders-txn", "orders", -1).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.apply(&negative),
            Err(Error::InvalidRequest)
        ));
        let illegal =
            encode_add("orders-txn", "bad name", 1).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.apply(&illegal),
            Err(Error::InvalidRequest)
        ));
        coordinator
            .end("orders-txn", producer, epoch, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let late =
            encode_add("orders-txn", "orders", 1).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(coordinator.apply(&late), Err(Error::InvalidState)));
        let flip = encode_finish("orders-txn", false).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(coordinator.apply(&flip), Err(Error::InvalidState)));
        assert_eq!(
            coordinator.state("orders-txn").unwrap(),
            Some(State::PrepareCommit)
        );
        let stored = coordinator
            .transactions
            .get("orders-txn")
            .map(|txn| txn.partitions.clone())
            .unwrap_or_default();
        let mut expected = BTreeMap::new();
        let mut parts = BTreeSet::new();
        let _ = parts.insert(0);
        let _ = expected.insert("orders".to_owned(), parts);
        assert_eq!(stored, expected);
    }

    #[test]
    fn a_repeated_partition_does_not_consume_a_journal_record() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-txn-repeat-{nanos}"));
        let journal = journal::Limits::new(512, 1 << 20, 32, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        let tight = Limits::new(8, 4, journal).unwrap_or_else(|_| panic!("coordinator limits"));
        let mut coordinator =
            Coordinator::open(&path, tight).unwrap_or_else(|error| panic!("{error:?}"));
        let (producer, epoch) = coordinator
            .init("orders-txn", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .begin("orders-txn", producer, epoch, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let encoded_capacity = coordinator.encoded.capacity();
        let before_duplicate = coordinator.journal.entry_count();
        assert!(encoded_capacity > 0);
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(coordinator.journal.entry_count(), before_duplicate);
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(coordinator.encoded.capacity() >= encoded_capacity);
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(coordinator.journal.entry_count(), 1);
        assert_eq!(
            coordinator.state("orders-txn").unwrap(),
            Some(State::Ongoing)
        );
    }
    #[test]
    fn checkpoints_preserve_fencing_and_all_transaction_states() {
        for (file_bytes, index_entries, max_records) in
            [(1 << 20, 1, 1), (1 << 20, 100, 2), (1_024, 100, 100)]
        {
            let journal = journal::Limits::new(4096, file_bytes, index_entries, 1 << 20).unwrap();
            let limits = Limits::new(16, max_records, journal).unwrap();
            let (_, path) = open("txn-checkpoint");
            let mut c = Coordinator::open(&path, limits).unwrap();
            c.advance(1).unwrap();
            let (empty, e0) = c.init("empty", 1).unwrap();
            let (ongoing, e1) = c.init("ongoing", 1).unwrap();
            c.begin("ongoing", ongoing, e1, 1).unwrap();
            c.add_partition("ongoing", ongoing, e1, 1, "orders", 0)
                .unwrap();
            let (prepared, e2) = c.init("prepared", 1).unwrap();
            c.begin("prepared", prepared, e2, 1).unwrap();
            c.add_partition("prepared", prepared, e2, 1, "orders", 1)
                .unwrap();
            assert_eq!(
                c.end("prepared", prepared, e2, 1, true).unwrap(),
                Outcome::Prepared { commit: true }
            );
            let (aborting, e3) = c.init("aborting", 1).unwrap();
            c.begin("aborting", aborting, e3, 1).unwrap();
            c.end("aborting", aborting, e3, 1, false).unwrap();
            let (complete, e4) = c.init("complete", 1).unwrap();
            c.begin("complete", complete, e4, 1).unwrap();
            c.end("complete", complete, e4, 1, true).unwrap();
            c.finish("complete", complete, e4, 1).unwrap();
            let (aborted, e5) = c.init("aborted", 1).unwrap();
            c.begin("aborted", aborted, e5, 1).unwrap();
            c.end("aborted", aborted, e5, 1, false).unwrap();
            c.finish("aborted", aborted, e5, 1).unwrap();
            for _ in 0..10 {
                drop(c);
                c = Coordinator::open(&path, limits).unwrap();
                assert_eq!(c.epoch().unwrap(), 1);
                for (id, state) in [
                    ("empty", State::Empty),
                    ("ongoing", State::Ongoing),
                    ("prepared", State::PrepareCommit),
                    ("aborting", State::PrepareAbort),
                    ("complete", State::CompleteCommit),
                    ("aborted", State::CompleteAbort),
                ] {
                    assert_eq!(c.state(id).unwrap(), Some(state));
                }
                assert_eq!(
                    c.end("prepared", prepared, e2, 1, true).unwrap(),
                    Outcome::Prepared { commit: true }
                );
                assert!(matches!(
                    c.end("prepared", prepared, e2, 1, false),
                    Err(Error::InvalidState)
                ));
                assert!(matches!(
                    c.begin("empty", empty, e0 - 1, 1),
                    Err(Error::ProducerFenced)
                ));
                assert!(matches!(
                    c.begin("empty", empty, e0, 0),
                    Err(Error::StaleCoordinator)
                ));
                assert_eq!(
                    txn_partition_count(&c.transactions["ongoing"].partitions),
                    1
                );
                let _ = c.init("empty", 1).unwrap();
            }
            drop(c);
            let c = Coordinator::open(&path, limits).unwrap();
            assert_eq!(c.transactions["empty"].producer_epoch, 10);
            assert_eq!(
                c.transactions["prepared"].partitions["orders"],
                BTreeSet::from([1])
            );
            drop(c);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn failed_checkpoint_does_not_acknowledge_or_expose_a_decision() {
        for phase in ["prepared", "renamed"] {
            let journal = journal::Limits::new(4096, 1 << 20, 1, 1 << 20).unwrap();
            let limits = Limits::new(8, 1, journal).unwrap();
            let (_, path) = open("txn-checkpoint-failure");
            let mut c = Coordinator::open(&path, limits).unwrap();
            let (producer, epoch) = c.init("id", 0).unwrap();
            c.begin("id", producer, epoch, 0).unwrap();
            journal::fail_checkpoint_once_at(phase);
            assert!(matches!(
                c.end("id", producer, epoch, 0, true),
                Err(Error::Storage(_))
            ));
            assert!(matches!(c.state("id"), Err(Error::Poisoned)));
            assert!(matches!(c.epoch(), Err(Error::Poisoned)));
            assert!(matches!(
                c.finish("id", producer, epoch, 0),
                Err(Error::Poisoned)
            ));
            drop(c);
            let mut c = Coordinator::open(&path, limits).unwrap();
            let expected = if phase == "prepared" {
                State::Ongoing
            } else {
                State::PrepareCommit
            };
            assert_eq!(c.state("id").unwrap(), Some(expected));
            assert_eq!(
                c.end("id", producer, epoch, 0, true).unwrap(),
                Outcome::Prepared { commit: true }
            );
            assert_eq!(
                c.finish("id", producer, epoch, 0).unwrap(),
                Outcome::Completed { commit: true }
            );
            drop(c);
            let c = Coordinator::open(&path, limits).unwrap();
            assert_eq!(c.state("id").unwrap(), Some(State::CompleteCommit));
            drop(c);
            let _ = std::fs::remove_file(format!("{}.checkpoint.tmp", path.display()));
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn checkpoint_limits_fail_before_changing_acknowledged_state() {
        let journal = journal::Limits::new(512, 1 << 20, 1, 1 << 20).unwrap();
        let limits = Limits::new(8, 1, journal).unwrap();
        let (_, path) = open("txn-checkpoint-size");
        let mut c = Coordinator::open(&path, limits).unwrap();
        let mut accepted = Vec::new();
        for n in 0..8 {
            let id = format!("{n}{}", "x".repeat(150));
            match c.init(&id, 0) {
                Ok(identity) => accepted.push((id, identity)),
                Err(Error::InvalidLimits) => {
                    assert!(c.state(&id).unwrap().is_none());
                    break;
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        assert!(!accepted.is_empty());
        assert!(accepted.len() < 8);
        drop(c);
        let c = Coordinator::open(&path, limits).unwrap();
        for (id, (producer, epoch)) in accepted {
            assert_eq!(c.state(&id).unwrap(), Some(State::Empty));
            assert_eq!(c.transactions[&id].producer_id, producer);
            assert_eq!(c.transactions[&id].producer_epoch, epoch);
        }
        drop(c);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn empty_journal_with_tiny_legacy_limits_remains_readable() {
        let max_fetch = std::mem::size_of::<journal::Entry>() + 17;
        let journal = journal::Limits::new(512, 80, 32, max_fetch).unwrap();
        let limits = Limits::new(8, 32, journal).unwrap();
        let (_, path) = open("txn-tiny-legacy");
        let mut c = Coordinator::open(&path, limits).unwrap();
        assert!(c.legacy_uncheckpointable);
        assert_eq!(c.advance(1).unwrap(), 1);
        assert!(matches!(c.advance(2), Err(Error::InvalidLimits)));
        assert_eq!(c.epoch().unwrap(), 1);
        drop(c);
        let c = Coordinator::open(&path, limits).unwrap();
        assert!(c.legacy_uncheckpointable);
        assert_eq!(c.epoch().unwrap(), 1);
        drop(c);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn checkpoint_rejects_invalid_state_and_non_initial_position() {
        let (mut c, path) = open("txn-checkpoint-invalid");
        c.init("id", 0).unwrap();
        let mut image = Vec::new();
        encode_checkpoint(
            c.epoch,
            c.next_producer,
            &c.transactions,
            None,
            c.snapshot_len,
            &mut image,
        )
        .unwrap();
        let mut invalid = image.clone();
        invalid[8..16].copy_from_slice(&(-1i64).to_be_bytes());
        assert!(matches!(
            decode_checkpoint(&invalid, c.limits),
            Err(Error::InvalidRequest)
        ));
        invalid = image.clone();
        invalid[16..24].copy_from_slice(&1i64.to_be_bytes());
        assert!(matches!(
            decode_checkpoint(&invalid, c.limits),
            Err(Error::InvalidRequest)
        ));
        c.journal.append(1, &image).unwrap();
        drop(c);
        assert!(matches!(
            Coordinator::open(&path, limits()),
            Err(Error::InvalidRequest)
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn producer_id_exhaustion_does_not_append() {
        let (mut c, path) = open("txn-id-exhausted");
        c.next_producer = i64::MAX;
        assert!(matches!(c.init("id", 0), Err(Error::InvalidRequest)));
        assert!(c.transactions.is_empty());
        assert_eq!(c.journal.entry_count(), 0);
        drop(c);
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn compacted_and_uncompacted_histories_remain_equivalent() {
        let compact_journal = journal::Limits::new(4096, 1 << 20, 1, 1 << 20).unwrap();
        let baseline_journal = journal::Limits::new(4096, 1 << 20, 1_000, 1 << 20).unwrap();
        let compact_limits = Limits::new(8, 1, compact_journal).unwrap();
        let baseline_limits = Limits::new(8, 1_000, baseline_journal).unwrap();
        let (_, compact_path) = open("txn-diff-compact");
        let (_, baseline_path) = open("txn-diff-baseline");
        let mut compact = Coordinator::open(&compact_path, compact_limits).unwrap();
        let mut baseline = Coordinator::open(&baseline_path, baseline_limits).unwrap();
        compact.advance(1).unwrap();
        baseline.advance(1).unwrap();
        let mut identities = Vec::new();
        for id in ["alpha", "bravo", "charlie", "delta"] {
            let identity = compact.init(id, 1).unwrap();
            assert_eq!(identity, baseline.init(id, 1).unwrap());
            identities.push((id, identity.0, identity.1));
        }
        for cycle in 0..40 {
            let slot = cycle % identities.len();
            let (id, producer, epoch) = identities[slot];
            for c in [&mut compact, &mut baseline] {
                c.begin(id, producer, epoch, 1).unwrap();
                c.add_partition(
                    id,
                    producer,
                    epoch,
                    1,
                    "orders",
                    i32::try_from(cycle).unwrap(),
                )
                .unwrap();
                c.end(id, producer, epoch, 1, cycle % 2 == 0).unwrap();
                c.finish(id, producer, epoch, 1).unwrap();
            }
            if cycle % 5 == 0 {
                let bumped = compact.init(id, 1).unwrap();
                assert_eq!(bumped, baseline.init(id, 1).unwrap());
                identities[slot].2 = bumped.1;
                assert!(matches!(
                    compact.begin(id, producer, epoch, 1),
                    Err(Error::ProducerFenced)
                ));
                assert!(matches!(
                    baseline.begin(id, producer, epoch, 1),
                    Err(Error::ProducerFenced)
                ));
            }
            assert_eq!(compact.epoch, baseline.epoch);
            assert_eq!(compact.next_producer, baseline.next_producer);
            assert_eq!(compact.transactions, baseline.transactions);
            drop(compact);
            drop(baseline);
            compact = Coordinator::open(&compact_path, compact_limits).unwrap();
            baseline = Coordinator::open(&baseline_path, baseline_limits).unwrap();
            assert_eq!(compact.transactions, baseline.transactions);
        }
        drop(compact);
        drop(baseline);
        let _ = std::fs::remove_file(compact_path);
        let _ = std::fs::remove_file(baseline_path);
    }
    #[test]
    fn admitted_transactions_keep_closure_and_fencing_headroom_at_512_bytes() {
        let journal = journal::Limits::new(512, 1 << 20, 1, 1 << 20).unwrap();
        let limits = Limits::new(8, 1, journal).unwrap();
        let (_, path) = open("txn-close-headroom");
        let mut c = Coordinator::open(&path, limits).unwrap();
        let a = format!("a{}", "x".repeat(150));
        let b = format!("b{}", "x".repeat(150));
        let (producer_a, epoch_a) = c.init(&a, 0).unwrap();
        c.begin(&a, producer_a, epoch_a, 0).unwrap();
        let (producer_b, epoch_b) = c.init(&b, 0).unwrap();
        assert_eq!(
            c.end(&a, producer_a, epoch_a, 0, true).unwrap(),
            Outcome::Prepared { commit: true }
        );
        assert_eq!(
            c.finish(&a, producer_a, epoch_a, 0).unwrap(),
            Outcome::Completed { commit: true }
        );
        c.advance(1).unwrap();
        let bumped = c.init(&b, 1).unwrap();
        assert_eq!(bumped, (producer_b, epoch_b + 1));
        drop(c);
        let mut c = Coordinator::open(&path, limits).unwrap();
        assert_eq!(c.epoch().unwrap(), 1);
        assert_eq!(c.state(&a).unwrap(), Some(State::CompleteCommit));
        assert!(matches!(
            c.begin(&b, producer_b, epoch_b, 1),
            Err(Error::ProducerFenced)
        ));
        c.begin(&b, producer_b, bumped.1, 1).unwrap();
        c.end(&b, producer_b, bumped.1, 1, false).unwrap();
        c.finish(&b, producer_b, bumped.1, 1).unwrap();
        drop(c);
        let c = Coordinator::open(&path, limits).unwrap();
        assert_eq!(c.state(&b).unwrap(), Some(State::CompleteAbort));
        drop(c);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn entry_file_and_fetch_boundaries_preserve_admitted_state() {
        let id = "a".repeat(151);
        let checkpoint_len = CHECKPOINT_MAGIC.len() + 8 + 8 + 4 + 2 + id.len() + 8 + 2 + 1 + 2;
        let fetch_charge = checkpoint_len + std::mem::size_of::<journal::Entry>();
        for (file_bytes, max_fetch, succeeds) in [
            ((checkpoint_len + 56) as u64, fetch_charge, true),
            ((checkpoint_len + 55) as u64, fetch_charge, false),
            ((checkpoint_len + 56) as u64, fetch_charge - 1, false),
        ] {
            let journal = journal::Limits::new(512, file_bytes, 1, max_fetch).unwrap();
            let limits = Limits::new(8, 1, journal).unwrap();
            let (_, path) = open("txn-cap-boundary");
            let mut c = Coordinator::open(&path, limits).unwrap();
            if succeeds {
                let (producer, epoch) = c.init(&id, 0).unwrap();
                c.begin(&id, producer, epoch, 0).unwrap();
                c.end(&id, producer, epoch, 0, true).unwrap();
                c.finish(&id, producer, epoch, 0).unwrap();
                drop(c);
                let c = Coordinator::open(&path, limits).unwrap();
                assert_eq!(c.state(&id).unwrap(), Some(State::CompleteCommit));
                drop(c);
            } else {
                assert!(matches!(c.init(&id, 0), Err(Error::InvalidLimits)));
                assert_eq!(c.state(&id).unwrap(), None);
                c.advance(1).unwrap();
                drop(c);
                let c = Coordinator::open(&path, limits).unwrap();
                assert_eq!(c.epoch().unwrap(), 1);
                drop(c);
            }
            let _ = std::fs::remove_file(path);
        }
        let journal = journal::Limits::new(512, 1 << 20, 1, 1 << 20).unwrap();
        let limits = Limits::new(8, 1, journal).unwrap();
        let (_, path) = open("txn-entry-boundary");
        let mut c = Coordinator::open(&path, limits).unwrap();
        let a = "a".repeat(249);
        let b = "b".repeat(205);
        let (producer_a, epoch_a) = c.init(&a, 0).unwrap();
        let (producer_b, epoch_b) = c.init(&b, 0).unwrap();
        assert_eq!(c.snapshot_len, 512);
        assert!(matches!(c.init("c", 0), Err(Error::InvalidLimits)));
        for (id, producer, epoch) in [(&a, producer_a, epoch_a), (&b, producer_b, epoch_b)] {
            c.begin(id, producer, epoch, 0).unwrap();
            c.end(id, producer, epoch, 0, true).unwrap();
            c.finish(id, producer, epoch, 0).unwrap();
        }
        drop(c);
        let c = Coordinator::open(&path, limits).unwrap();
        assert_eq!(c.state(&a).unwrap(), Some(State::CompleteCommit));
        assert_eq!(c.state(&b).unwrap(), Some(State::CompleteCommit));
        drop(c);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn event_only_history_replays_and_rolls_to_checkpoint() {
        let journal_limits = journal::Limits::new(4096, 1 << 20, 3, 1 << 20).unwrap();
        let limits = Limits::new(8, 3, journal_limits).unwrap();
        let (_, path) = open("txn-legacy");
        let (mut journal, _) = Journal::open(&path, 0, journal_limits).unwrap();
        let mut event = Vec::new();
        write_init(&mut event, "id", 1, 0).unwrap();
        journal.append(1, &event).unwrap();
        write_begin(&mut event, "id").unwrap();
        journal.append(1, &event).unwrap();
        write_add(&mut event, "id", "orders", 1).unwrap();
        journal.append(1, &event).unwrap();
        drop(journal);
        let mut c = Coordinator::open(&path, limits).unwrap();
        assert_eq!(c.state("id").unwrap(), Some(State::Ongoing));
        c.end("id", 1, 0, 0, true).unwrap();
        assert_eq!(c.journal.entry_count(), 1);
        let entry = c
            .journal
            .fetch(0, 1, journal_limits.max_fetch_bytes())
            .unwrap();
        assert_eq!(entry[0].payload.get(..8), Some(CHECKPOINT_MAGIC.as_slice()));
        assert!(matches!(
            split_kind(&entry[0].payload),
            Err(Error::InvalidRequest)
        ));
        drop(c);
        let mut c = Coordinator::open(&path, limits).unwrap();
        assert_eq!(c.state("id").unwrap(), Some(State::PrepareCommit));
        c.finish("id", 1, 0, 0).unwrap();
        drop(c);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn oversized_legacy_history_remains_accessible_until_it_can_roll_over() {
        for history_full in [false, true] {
            let old_journal = journal::Limits::new(512, 1 << 20, 32, 1 << 20).unwrap();
            let old_limits =
                Limits::new(8, if history_full { 5 } else { 32 }, old_journal).unwrap();
            let (_, path) = open("txn-oversized-legacy");
            let (mut journal, _) = Journal::open(&path, 0, old_journal).unwrap();
            let ids: Vec<String> = (0..4).map(|n| format!("{n}{}", "x".repeat(150))).collect();
            let mut event = Vec::new();
            for (i, id) in ids.iter().enumerate() {
                write_init(&mut event, id, i64::try_from(i + 1).unwrap(), 0).unwrap();
                journal.append(1, &event).unwrap();
            }
            write_begin(&mut event, &ids[0]).unwrap();
            journal.append(1, &event).unwrap();
            drop(journal);
            let mut c = Coordinator::open(&path, old_limits).unwrap();
            assert!(c.legacy_uncheckpointable);
            assert_eq!(c.state(&ids[0]).unwrap(), Some(State::Ongoing));
            if history_full {
                assert!(matches!(
                    c.end(&ids[0], 1, 0, 0, true),
                    Err(Error::InvalidLimits)
                ));
                assert_eq!(c.state(&ids[0]).unwrap(), Some(State::Ongoing));
            } else {
                c.end(&ids[0], 1, 0, 0, true).unwrap();
                c.finish(&ids[0], 1, 0, 0).unwrap();
                let before = c.snapshot_len;
                let extra = format!("z{}", "x".repeat(150));
                c.init(&extra, 0).unwrap();
                assert!(c.snapshot_len > before);
                assert!(c.legacy_uncheckpointable);
            }
            drop(c);
            let larger = journal::Limits::new(4096, 1 << 20, 32, 1 << 20).unwrap();
            let limits = Limits::new(8, 1, larger).unwrap();
            let mut c = Coordinator::open(&path, limits).unwrap();
            if history_full {
                assert_eq!(c.state(&ids[0]).unwrap(), Some(State::Ongoing));
                c.end(&ids[0], 1, 0, 0, true).unwrap();
                c.finish(&ids[0], 1, 0, 0).unwrap();
            } else {
                assert_eq!(c.state(&ids[0]).unwrap(), Some(State::CompleteCommit));
                c.init(&ids[0], 0).unwrap();
                c.begin(&ids[0], 1, 1, 0).unwrap();
                c.end(&ids[0], 1, 1, 0, true).unwrap();
                c.finish(&ids[0], 1, 1, 0).unwrap();
            }
            assert!(!c.legacy_uncheckpointable);
            drop(c);
            let c = Coordinator::open(&path, limits).unwrap();
            assert_eq!(c.state(&ids[0]).unwrap(), Some(State::CompleteCommit));
            drop(c);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn completed_legacy_transaction_can_shrink_and_roll_over_at_capacity() {
        let journal_limits = journal::Limits::new(512, 1 << 20, 7, 1 << 20).unwrap();
        let limits = Limits::new(8, 7, journal_limits).unwrap();
        let (_, path) = open("txn-legacy-shrink");
        let (mut journal, _) = Journal::open(&path, 0, journal_limits).unwrap();
        let mut event = Vec::new();
        write_init(&mut event, "id", 1, 0).unwrap();
        journal.append(1, &event).unwrap();
        write_begin(&mut event, "id").unwrap();
        journal.append(1, &event).unwrap();
        for prefix in ['a', 'b', 'c'] {
            let topic = format!("{prefix}{}", "x".repeat(149));
            write_add(&mut event, "id", &topic, 0).unwrap();
            journal.append(1, &event).unwrap();
        }
        write_prepare(&mut event, "id", true).unwrap();
        journal.append(1, &event).unwrap();
        write_finish(&mut event, "id", true).unwrap();
        journal.append(1, &event).unwrap();
        drop(journal);
        let mut c = Coordinator::open(&path, limits).unwrap();
        assert!(c.legacy_uncheckpointable);
        assert_eq!(c.state("id").unwrap(), Some(State::CompleteCommit));
        assert_eq!(c.init("id", 0).unwrap(), (1, 1));
        assert!(!c.legacy_uncheckpointable);
        assert_eq!(c.journal.entry_count(), 1);
        assert!(matches!(c.begin("id", 1, 0, 0), Err(Error::ProducerFenced)));
        drop(c);
        let mut c = Coordinator::open(&path, limits).unwrap();
        assert_eq!(c.state("id").unwrap(), Some(State::Empty));
        assert!(c.transactions["id"].partitions.is_empty());
        c.begin("id", 1, 1, 0).unwrap();
        drop(c);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn valid_legacy_growth_can_exceed_the_maximum_single_entry() {
        // Each distinct transactional id owns 64 partitions with a 249-byte topic.
        // All events and counts fit legacy limits, but adding one transaction can
        // move a checkpoint across the journal's hard 64 MiB entry cap.
        let below = 4_106usize;
        let per_txn = 2 + 8 + 8 + 2 + 1 + 2 + 64 * (2 + 249 + 4);
        let header = CHECKPOINT_MAGIC.len() + 8 + 8 + 4;
        let before = header + below * per_txn;
        let after = before + per_txn;
        assert!(before <= 64 * 1024 * 1024);
        assert!(after > 64 * 1024 * 1024);
        assert!((below + 1) * (1 + 1 + 64) < 1_000_000);
    }

    #[test]
    #[cfg(unix)]
    fn checkpoint_process_exit_recovers_complete_transaction_state() {
        const TEST: &str = "transactions::coordinator::tests::checkpoint_process_exit_recovers_complete_transaction_state";
        let journal = journal::Limits::new(4096, 1 << 20, 1, 1 << 20).unwrap();
        let limits = Limits::new(8, 1, journal).unwrap();
        if let Some(path) = std::env::var_os("PLB_TXN_CHECKPOINT_PATH") {
            let mut c = Coordinator::open(path, limits).unwrap();
            c.end("id", 1, 0, 1, true).unwrap();
            panic!("checkpoint failpoint did not exit");
        }
        for phase in ["prepared", "renamed", "synced"] {
            let (_, path) = open("txn-checkpoint-crash");
            let mut c = Coordinator::open(&path, limits).unwrap();
            c.advance(1).unwrap();
            let (producer, epoch) = c.init("id", 1).unwrap();
            c.begin("id", producer, epoch, 1).unwrap();
            c.add_partition("id", producer, epoch, 1, "orders", 0)
                .unwrap();
            let (abort_producer, old_epoch) = c.init("abort", 1).unwrap();
            let (_, abort_epoch) = c.init("abort", 1).unwrap();
            c.begin("abort", abort_producer, abort_epoch, 1).unwrap();
            c.add_partition("abort", abort_producer, abort_epoch, 1, "payments", 2)
                .unwrap();
            c.end("abort", abort_producer, abort_epoch, 1, false)
                .unwrap();
            drop(c);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture"])
                .env("PLB_JOURNAL_CHECKPOINT_CRASH", phase)
                .env("PLB_TXN_CHECKPOINT_PATH", &path)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(71), "{phase}");
            let mut c = Coordinator::open(&path, limits).unwrap();
            let expected = if phase == "prepared" {
                State::Ongoing
            } else {
                State::PrepareCommit
            };
            assert_eq!(c.epoch().unwrap(), 1);
            assert_eq!(c.state("id").unwrap(), Some(expected));
            assert_eq!(c.state("abort").unwrap(), Some(State::PrepareAbort));
            assert_eq!(
                c.transactions["id"].partitions["orders"],
                BTreeSet::from([0])
            );
            assert_eq!(
                c.transactions["abort"].partitions["payments"],
                BTreeSet::from([2])
            );
            assert!(matches!(
                c.end("id", producer, epoch, 0, true),
                Err(Error::StaleCoordinator)
            ));
            assert!(matches!(
                c.finish("abort", abort_producer, old_epoch, 1),
                Err(Error::ProducerFenced)
            ));
            c.end("id", producer, epoch, 1, true).unwrap();
            assert_eq!(
                c.finish("id", producer, epoch, 1).unwrap(),
                Outcome::Completed { commit: true }
            );
            assert_eq!(
                c.finish("abort", abort_producer, abort_epoch, 1).unwrap(),
                Outcome::Completed { commit: false }
            );
            drop(c);
            let c = Coordinator::open(&path, limits).unwrap();
            assert_eq!(c.state("id").unwrap(), Some(State::CompleteCommit));
            assert_eq!(c.state("abort").unwrap(), Some(State::CompleteAbort));
            assert_eq!(
                c.transactions["id"].partitions["orders"],
                BTreeSet::from([0])
            );
            assert_eq!(
                c.transactions["abort"].partitions["payments"],
                BTreeSet::from([2])
            );
            drop(c);
            let _ = std::fs::remove_file(format!("{}.checkpoint.tmp", path.display()));
            let _ = std::fs::remove_file(path);
        }
    }
}
