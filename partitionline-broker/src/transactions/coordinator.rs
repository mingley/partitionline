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
    /// A failed append left this coordinator unable to confirm further changes.
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
    /// Configure positive caps. The journal must index every transition.
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
}

type BTreeMapTxn = std::collections::BTreeMap<String, Transaction>;

impl Coordinator {
    /// Open or recover coordinator state. The epoch starts at 0.
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
        };
        let mut offset = coordinator.journal.base_offset();
        loop {
            let batch = coordinator.journal.fetch(
                offset,
                32,
                coordinator.limits.journal.max_fetch_bytes(),
            )?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                coordinator.apply(&entry.payload)?;
                coordinator.stored = coordinator.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(coordinator)
    }

    /// Current coordinator epoch.
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// State of a transactional id, if it has been initialized.
    pub fn state(&self, transactional_id: &str) -> Option<State> {
        self.transactions.get(transactional_id).map(|txn| txn.state)
    }

    /// Move the coordinator lineage forward. A smaller or equal epoch is stale.
    pub fn advance(&mut self, epoch: i64) -> Result<i64, Error> {
        self.ready()?;
        if epoch <= self.epoch {
            return Err(Error::StaleCoordinator);
        }
        self.persist(&encode_epoch(epoch)?)?;
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
            self.persist(&encode_init(transactional_id, producer_id, epoch)?)?;
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
        self.persist(&encode_init(transactional_id, producer_id, 0)?)?;
        self.transactions.insert(
            transactional_id.to_owned(),
            Transaction {
                producer_id,
                producer_epoch: 0,
                state: State::Empty,
                partitions: BTreeMap::new(),
            },
        );
        self.next_producer = self.next_producer.saturating_add(1);
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
        self.persist(&encode_begin(transactional_id)?)?;
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
        self.persist(&encode_add(transactional_id, topic, partition)?)?;
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
                self.persist(&encode_prepare(transactional_id, commit)?)?;
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
        self.persist(&encode_finish(transactional_id, commit)?)?;
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

    fn apply(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let (kind, mut rest) = split_kind(bytes)?;
        match kind {
            KIND_COORDINATOR => {
                let epoch = take_i64(&mut rest)?;
                if !rest.is_empty() || epoch <= self.epoch {
                    return Err(Error::InvalidRequest);
                }
                self.epoch = epoch;
            }
            KIND_INIT => {
                let id = take_str(&mut rest)?;
                let producer_id = take_i64(&mut rest)?;
                let epoch = take_i16(&mut rest)?;
                if !rest.is_empty() {
                    return Err(Error::InvalidRequest);
                }
                validate_id(&id)?;
                if let Some(existing) = self.transactions.get(&id) {
                    if matches!(existing.state, State::PrepareCommit | State::PrepareAbort) {
                        return Err(Error::InvalidState);
                    }
                    if producer_id != existing.producer_id
                        || existing.producer_epoch.checked_add(1) != Some(epoch)
                    {
                        return Err(Error::InvalidRequest);
                    }
                    let txn = self.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
                    txn.producer_epoch = epoch;
                    txn.state = State::Empty;
                    txn.partitions.clear();
                } else {
                    if self.transactions.len() >= self.limits.max_transactions {
                        return Err(Error::InvalidLimits);
                    }
                    if producer_id != self.next_producer || epoch != 0 {
                        return Err(Error::InvalidRequest);
                    }
                    let next = self
                        .next_producer
                        .checked_add(1)
                        .ok_or(Error::InvalidRequest)?;
                    self.transactions.insert(
                        id,
                        Transaction {
                            producer_id,
                            producer_epoch: 0,
                            state: State::Empty,
                            partitions: BTreeMap::new(),
                        },
                    );
                    self.next_producer = next;
                }
            }
            KIND_BEGIN => {
                let id = take_str(&mut rest)?;
                if !rest.is_empty() {
                    return Err(Error::InvalidRequest);
                }
                let txn = self.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
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
                if !rest.is_empty()
                    || catalog::validate_topic_name(&topic).is_err()
                    || partition < 0
                {
                    return Err(Error::InvalidRequest);
                }
                let txn = self.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
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
                let txn = self.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
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
                let txn = self.transactions.get_mut(&id).ok_or(Error::InvalidState)?;
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

fn encode_epoch(epoch: i64) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_COORDINATOR);
    bytes.extend_from_slice(&epoch.to_be_bytes());
    Ok(bytes)
}

fn encode_init(id: &str, producer_id: i64, epoch: i16) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_INIT);
    put_str(&mut bytes, id)?;
    bytes.extend_from_slice(&producer_id.to_be_bytes());
    bytes.extend_from_slice(&epoch.to_be_bytes());
    Ok(bytes)
}

fn encode_begin(id: &str) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_BEGIN);
    put_str(&mut bytes, id)?;
    Ok(bytes)
}

fn encode_add(id: &str, topic: &str, partition: i32) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_ADD);
    put_str(&mut bytes, id)?;
    put_str(&mut bytes, topic)?;
    bytes.extend_from_slice(&partition.to_be_bytes());
    Ok(bytes)
}

fn encode_prepare(id: &str, commit: bool) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_PREPARE);
    put_str(&mut bytes, id)?;
    bytes.push(u8::from(commit));
    Ok(bytes)
}

fn encode_finish(id: &str, commit: bool) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_FINISH);
    put_str(&mut bytes, id)?;
    bytes.push(u8::from(commit));
    Ok(bytes)
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
    Ok(take(input, 1)?[0] == 1)
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
        assert_eq!(coordinator.state("orders-txn"), Some(State::PrepareCommit));
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
        assert_eq!(restored.epoch(), 1);
        assert_eq!(restored.state("orders-txn"), Some(State::PrepareCommit));
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
        assert_eq!(coordinator.state("orders-txn"), Some(State::Ongoing));
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
        assert_eq!(coordinator.state("orders-txn"), Some(State::PrepareCommit));
        drop(coordinator);
        let mut restored =
            Coordinator::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.state("orders-txn"), Some(State::PrepareCommit));
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
        assert_eq!(coordinator.state("orders-txn"), Some(State::CompleteCommit));
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
        assert_eq!(coordinator.state("orders-txn"), Some(State::PrepareCommit));
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
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        coordinator
            .add_partition("orders-txn", producer, epoch, 0, "orders", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            coordinator.add_partition("orders-txn", producer, epoch, 0, "orders", 2),
            Err(Error::InvalidLimits)
        ));
        assert_eq!(coordinator.state("orders-txn"), Some(State::Ongoing));
    }
}
