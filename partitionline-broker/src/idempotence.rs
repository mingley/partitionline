//! Durable producer ids, epochs, and per-partition idempotent sequences.
//!
//! The first accepted sequence on a producer epoch and partition must be 0.
//! The next batch must start at the next sequence. A retry of a sequence still
//! in the duplicate window returns the original base offset and does not append
//! another record. An older epoch is fenced. Epoch `i16::MAX` cannot be bumped.
//!
//! Allocations, bumps, and accepted batches are journaled, so a restart still
//! answers a lost acknowledgement with the original offset. This is not the
//! InitProducerId or Produce RPC.

use crate::journal::{self, Journal};
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLIDEM01";
const KIND_ALLOC: u8 = 1;
const KIND_BUMP: u8 = 2;
const KIND_APPEND: u8 = 3;
const DUPLICATE_WINDOW: usize = 5;

/// Kafka `OUT_OF_ORDER_SEQUENCE_NUMBER`.
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
/// Kafka `DUPLICATE_SEQUENCE_NUMBER`.
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
/// Kafka `INVALID_PRODUCER_EPOCH`.
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
/// Kafka `UNKNOWN_PRODUCER_ID`.
pub const UNKNOWN_PRODUCER_ID: i16 = 59;

/// An accepted or replayed batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Batch {
    /// First log offset assigned to this sequence.
    pub offset: i64,
    /// Records in the batch.
    pub count: i32,
}

/// Why a producer write was refused.
#[derive(Debug)]
pub enum Error {
    /// The producer id was never allocated.
    UnknownProducer,
    /// The epoch is not the current one.
    InvalidEpoch,
    /// The sequence is ahead of or behind the expected sequence.
    OutOfOrder,
    /// The sequence was accepted and has left the duplicate window.
    Duplicate,
    /// The producer epoch cannot increase past `i16::MAX`.
    EpochExhausted,
    /// The sequence or count is not usable.
    InvalidRequest,
    /// A failed append left this store unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the producers.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a producer-visible refusal. Exhaustion and storage have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::UnknownProducer => UNKNOWN_PRODUCER_ID,
            Self::InvalidEpoch => INVALID_PRODUCER_EPOCH,
            Self::OutOfOrder => OUT_OF_ORDER_SEQUENCE_NUMBER,
            Self::Duplicate => DUPLICATE_SEQUENCE_NUMBER,
            Self::EpochExhausted
            | Self::InvalidRequest
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

/// Producer and journal bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_producers: usize,
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive producer and record caps.
    pub fn new(
        max_producers: usize,
        max_records: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=1_000_000).contains(&max_producers)
            || !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < 40
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_producers,
            max_records,
            journal,
        })
    }
}

#[derive(Debug, Clone)]
struct PartitionState {
    next: i32,
    recent: VecDeque<BatchRecord>,
}

#[derive(Debug, Clone, Copy)]
struct BatchRecord {
    sequence: i32,
    offset: i64,
    count: i32,
}

#[derive(Debug, Clone)]
struct Producer {
    epoch: i16,
    partitions: BTreeMap<i32, PartitionState>,
}

/// Durable idempotent producer state.
pub struct Producers {
    limits: Limits,
    producers: BTreeMap<i64, Producer>,
    next_id: i64,
    log_end: i64,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Producers {
    /// Open or recover producer state. The next producer id starts at 1.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut state = Self {
            limits,
            producers: BTreeMap::new(),
            next_id: 1,
            log_end: 0,
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
                    return Err(Error::InvalidRequest);
                }
                state.apply(&entry.payload)?;
                state.stored = state.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(state)
    }

    /// Next offset that a new batch would receive.
    pub fn log_end(&self) -> i64 {
        self.log_end
    }

    /// Current epoch for a producer, if it was allocated.
    pub fn epoch(&self, producer_id: i64) -> Option<i16> {
        self.producers
            .get(&producer_id)
            .map(|producer| producer.epoch)
    }

    /// Allocate a new producer id at epoch 0.
    pub fn allocate(&mut self) -> Result<(i64, i16), Error> {
        self.ready()?;
        if self.producers.len() >= self.limits.max_producers || self.next_id == i64::MAX {
            return Err(Error::InvalidLimits);
        }
        let id = self.next_id;
        self.persist(&encode_alloc(id)?)?;
        self.producers.insert(
            id,
            Producer {
                epoch: 0,
                partitions: BTreeMap::new(),
            },
        );
        self.next_id = self.next_id.saturating_add(1);
        Ok((id, 0))
    }

    /// Fence the current epoch and start the next one.
    ///
    /// Per-partition sequences reset. The log end does not move, so no new
    /// record is invented. Epoch `i16::MAX` cannot be bumped.
    pub fn bump_epoch(&mut self, producer_id: i64) -> Result<i16, Error> {
        self.ready()?;
        let Some(producer) = self.producers.get(&producer_id) else {
            return Err(Error::UnknownProducer);
        };
        if producer.epoch == i16::MAX {
            return Err(Error::EpochExhausted);
        }
        let epoch = producer.epoch + 1;
        self.persist(&encode_bump(producer_id, epoch)?)?;
        let producer = self
            .producers
            .get_mut(&producer_id)
            .ok_or(Error::UnknownProducer)?;
        producer.epoch = epoch;
        producer.partitions.clear();
        Ok(epoch)
    }

    /// Accept a batch or return the original offset of a duplicate retry.
    pub fn append(
        &mut self,
        producer_id: i64,
        epoch: i16,
        partition: i32,
        sequence: i32,
        count: i32,
    ) -> Result<Batch, Error> {
        self.ready()?;
        if partition < 0 || sequence < 0 || count <= 0 {
            return Err(Error::InvalidRequest);
        }
        let Some(producer) = self.producers.get(&producer_id) else {
            return Err(Error::UnknownProducer);
        };
        if epoch != producer.epoch {
            return Err(Error::InvalidEpoch);
        }
        if let Some(state) = producer.partitions.get(&partition) {
            if let Some(existing) = state.recent.iter().find(|batch| batch.sequence == sequence) {
                if existing.count != count {
                    return Err(Error::InvalidRequest);
                }
                return Ok(Batch {
                    offset: existing.offset,
                    count,
                });
            }
            if sequence < state.next {
                return Err(Error::Duplicate);
            }
            if sequence != state.next {
                return Err(Error::OutOfOrder);
            }
        } else if sequence != 0 {
            return Err(Error::OutOfOrder);
        }
        let offset = self.log_end;
        let next_end = offset
            .checked_add(i64::from(count))
            .ok_or(Error::InvalidRequest)?;
        // Reject before the journal write. `note` cannot represent a sequence
        // that overflows `i32`, and a persisted record it cannot apply would
        // make the next reopen fail.
        let _ = sequence
            .checked_add(count)
            .ok_or(Error::InvalidRequest)?;
        self.persist(&encode_append(
            producer_id,
            epoch,
            partition,
            sequence,
            count,
            offset,
        )?)?;
        self.note(producer_id, partition, sequence, count, offset)?;
        self.log_end = next_end;
        Ok(Batch { offset, count })
    }

    fn note(
        &mut self,
        producer_id: i64,
        partition: i32,
        sequence: i32,
        count: i32,
        offset: i64,
    ) -> Result<(), Error> {
        let producer = self
            .producers
            .get_mut(&producer_id)
            .ok_or(Error::UnknownProducer)?;
        let state = producer
            .partitions
            .entry(partition)
            .or_insert(PartitionState {
                next: 0,
                recent: VecDeque::new(),
            });
        let next = sequence.checked_add(count).ok_or(Error::InvalidRequest)?;
        state.next = next;
        state.recent.push_back(BatchRecord {
            sequence,
            offset,
            count,
        });
        while state.recent.len() > DUPLICATE_WINDOW {
            state.recent.pop_front();
        }
        Ok(())
    }

    #[cfg(test)]
    fn exhaust_epoch_for_test(&mut self, producer_id: i64) {
        if let Some(producer) = self.producers.get_mut(&producer_id) {
            producer.epoch = i16::MAX;
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
            KIND_ALLOC => {
                let id = take_i64(&mut rest)?;
                if !rest.is_empty() {
                    return Err(Error::InvalidRequest);
                }
                self.producers.insert(
                    id,
                    Producer {
                        epoch: 0,
                        partitions: BTreeMap::new(),
                    },
                );
                self.next_id = self.next_id.max(id.saturating_add(1));
            }
            KIND_BUMP => {
                let id = take_i64(&mut rest)?;
                let epoch = take_i16(&mut rest)?;
                if !rest.is_empty() {
                    return Err(Error::InvalidRequest);
                }
                let producer = self.producers.get_mut(&id).ok_or(Error::UnknownProducer)?;
                producer.epoch = epoch;
                producer.partitions.clear();
            }
            KIND_APPEND => {
                let id = take_i64(&mut rest)?;
                let _epoch = take_i16(&mut rest)?;
                let partition = take_i32(&mut rest)?;
                let sequence = take_i32(&mut rest)?;
                let count = take_i32(&mut rest)?;
                let offset = take_i64(&mut rest)?;
                if !rest.is_empty() {
                    return Err(Error::InvalidRequest);
                }
                self.note(id, partition, sequence, count, offset)?;
                self.log_end = self.log_end.max(offset.saturating_add(i64::from(count)));
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

fn split_kind(bytes: &[u8]) -> Result<(u8, &[u8]), Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    Ok((bytes[RECORD_MAGIC.len()], &bytes[RECORD_MAGIC.len() + 1..]))
}

fn encode_alloc(id: i64) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_ALLOC);
    bytes.extend_from_slice(&id.to_be_bytes());
    Ok(bytes)
}

fn encode_bump(id: i64, epoch: i16) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(19);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_BUMP);
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend_from_slice(&epoch.to_be_bytes());
    Ok(bytes)
}

fn encode_append(
    id: i64,
    epoch: i16,
    partition: i32,
    sequence: i32,
    count: i32,
    offset: i64,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(35);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_APPEND);
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend_from_slice(&epoch.to_be_bytes());
    bytes.extend_from_slice(&partition.to_be_bytes());
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(&offset.to_be_bytes());
    Ok(bytes)
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidRequest);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(64, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, 64, journal).unwrap_or_else(|_| panic!("producer limits"))
    }

    fn open(name: &str) -> (Producers, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-idem-{name}-{nanos}"));
        let producers =
            Producers::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (producers, path)
    }

    #[test]
    fn duplicate_retry_returns_the_original_offset_after_restart() {
        let (mut producers, path) = open("retry");
        let (id, epoch) = producers
            .allocate()
            .unwrap_or_else(|error| panic!("{error:?}"));
        let first = producers
            .append(id, epoch, 0, 0, 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(first.offset, 0);
        let retry = producers
            .append(id, epoch, 0, 0, 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(retry, first);
        assert_eq!(producers.log_end(), 2);
        assert!(matches!(
            producers.append(id, epoch, 0, 5, 1),
            Err(Error::OutOfOrder)
        ));
        assert_eq!(producers.log_end(), 2);
        drop(producers);
        let mut restored =
            Producers::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let replayed = restored
            .append(id, epoch, 0, 0, 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(replayed, first);
        assert_eq!(restored.log_end(), 2);
    }

    #[test]
    fn old_epoch_and_exhausted_epoch_do_not_append() {
        let (mut producers, _) = open("epoch");
        let (id, epoch) = producers
            .allocate()
            .unwrap_or_else(|error| panic!("{error:?}"));
        producers
            .append(id, epoch, 1, 0, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let bumped = producers
            .bump_epoch(id)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(bumped, 1);
        assert!(matches!(
            producers.append(id, epoch, 1, 0, 1),
            Err(Error::InvalidEpoch)
        ));
        assert_eq!(producers.log_end(), 1);
        producers.exhaust_epoch_for_test(id);
        assert!(matches!(
            producers.bump_epoch(id),
            Err(Error::EpochExhausted)
        ));
        assert_eq!(producers.epoch(id), Some(i16::MAX));
        assert_eq!(producers.log_end(), 1);
    }

    #[test]
    fn a_sequence_that_overflows_is_not_persisted() {
        let (mut producers, path) = open("overflow");
        let (id, epoch) = producers
            .allocate()
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            producers.append(id, epoch, 0, i32::MAX, 1),
            Err(Error::InvalidRequest)
        ));
        assert_eq!(producers.log_end(), 0);
        let stored = producers
            .append(id, epoch, 0, 0, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(stored.offset, 0);
        drop(producers);
        let restored =
            Producers::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.log_end(), 1);
        assert_eq!(restored.epoch(id), Some(epoch));
    }
}
