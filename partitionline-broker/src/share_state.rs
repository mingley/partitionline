//! Durable share-group partition state and offset administration.
//!
//! `init` creates epoch 0. `write` replaces the batch list only when the
//! caller presents that epoch, then advances it by one. A stale epoch changes
//! nothing. `timeout` turns acquired batches back to available and keeps their
//! delivery counts and every acknowledged batch. `delete` removes the
//! partition. Reopen shows the same epoch, counts, and acknowledgements.
//!
//! Delivery states use the persister values available 0, acquired 1,
//! acknowledged 2, and archived 4. This is not the ReadShareGroupState,
//! WriteShareGroupState, InitializeShareGroupState, or DeleteShareGroupState RPC.

use crate::journal::{self, Journal};
use std::collections::BTreeMap;
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLSHS001";

/// Kafka `FENCED_LEADER_EPOCH`, used when the share state epoch is stale.
pub const FENCED_STATE_EPOCH: i16 = 74;

/// Persisted delivery state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryState {
    /// Available for delivery.
    Available = 0,
    /// Locked by a share member.
    Acquired = 1,
    /// Acknowledged by a share member.
    Acknowledged = 2,
    /// No longer delivered.
    Archived = 4,
}

/// One contiguous delivery-state span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// First offset in the span.
    pub first_offset: i64,
    /// Last offset in the span.
    pub last_offset: i64,
    /// Times this span has been delivered.
    pub delivery_count: u16,
    /// Delivery state.
    pub state: DeliveryState,
}

/// One share partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    /// State generation. Writes must present this value.
    pub state_epoch: i32,
    /// Leader epoch stored with the snapshot.
    pub leader_epoch: i32,
    /// Start offset.
    pub start_offset: i64,
    /// Delivery spans.
    pub batches: Vec<Batch>,
}

/// Read summary without copying every batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    /// State generation.
    pub state_epoch: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Start offset.
    pub start_offset: i64,
    /// Number of spans.
    pub batch_count: usize,
    /// One past the highest acknowledged offset, when any span is acknowledged.
    pub acknowledged_offset: Option<i64>,
}

/// A replacement snapshot for `write`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    /// Epoch returned by the last read or init.
    pub state_epoch: i32,
    /// Leader epoch. Must be at least the stored leader epoch.
    pub leader_epoch: i32,
    /// New start offset.
    pub start_offset: i64,
    /// Replacement spans.
    pub batches: Vec<Batch>,
}

/// Why a share-state operation was refused.
#[derive(Debug)]
pub enum Error {
    /// The state or leader epoch does not match the stored generation.
    Fenced,
    /// The partition has no state.
    NotFound,
    /// The partition was already initialized.
    AlreadyInitialized,
    /// The group, offsets, or batches are not usable.
    InvalidRequest,
    /// A failed append left this store unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the partitions.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a generation fence. Other failures have none.
    pub fn code(&self) -> Option<i16> {
        match self {
            Self::Fenced => Some(FENCED_STATE_EPOCH),
            Self::NotFound
            | Self::AlreadyInitialized
            | Self::InvalidRequest
            | Self::Poisoned
            | Self::InvalidLimits
            | Self::Storage(_) => None,
        }
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Share-state bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_partitions: usize,
    max_batches: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive partition and batch caps.
    pub fn new(
        max_partitions: usize,
        max_batches: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_partitions)
            || !(1..=100_000).contains(&max_batches)
            || journal.max_entry_bytes() < 1024
            || journal.max_index_entries() < max_partitions
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_partitions,
            max_batches,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    group: String,
    topic: String,
    partition: i32,
}

/// Durable share-group state.
pub struct Store {
    limits: Limits,
    partitions: BTreeMap<Key, Partition>,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Store {
    /// Open or recover share state.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut store = Self {
            limits,
            partitions: BTreeMap::new(),
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut cursor = store.journal.base_offset();
        loop {
            let batch = store
                .journal
                .fetch(cursor, 32, store.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                store.partitions = decode(&entry.payload)?;
                store.stored = store.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(store)
    }

    /// Create epoch 0. A second init does not reset the partition.
    pub fn init(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> Result<Partition, Error> {
        self.ready()?;
        let key = key(group, topic, partition)?;
        if start_offset < 0 {
            return Err(Error::InvalidRequest);
        }
        if self.partitions.contains_key(&key) {
            return Err(Error::AlreadyInitialized);
        }
        if self.partitions.len() >= self.limits.max_partitions {
            return Err(Error::InvalidLimits);
        }
        let created = Partition {
            state_epoch: 0,
            leader_epoch: 0,
            start_offset,
            batches: Vec::new(),
        };
        let mut next = self.partitions.clone();
        next.insert(key, created.clone());
        self.commit(next)?;
        Ok(created)
    }

    /// Read the current partition snapshot.
    pub fn read(&self, group: &str, topic: &str, partition: i32) -> Result<Partition, Error> {
        let key = key(group, topic, partition)?;
        self.partitions.get(&key).cloned().ok_or(Error::NotFound)
    }

    /// Read the epoch, start offset, and acknowledged offset.
    pub fn summary(&self, group: &str, topic: &str, partition: i32) -> Result<Summary, Error> {
        let partition = self.read(group, topic, partition)?;
        Ok(Summary {
            state_epoch: partition.state_epoch,
            leader_epoch: partition.leader_epoch,
            start_offset: partition.start_offset,
            batch_count: partition.batches.len(),
            acknowledged_offset: acknowledged_offset(&partition.batches),
        })
    }

    /// Replace the snapshot when `write.state_epoch` is current.
    ///
    /// The start offset cannot move backward, and every batch must begin at
    /// or after that offset. A refused write leaves the stored snapshot unchanged.
    pub fn write(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        write: Write,
    ) -> Result<Partition, Error> {
        self.ready()?;
        let key = key(group, topic, partition)?;
        let Some(current) = self.partitions.get(&key) else {
            return Err(Error::NotFound);
        };
        if write.state_epoch != current.state_epoch || write.leader_epoch < current.leader_epoch {
            return Err(Error::Fenced);
        }
        // The start offset only moves forward. A batch that begins before it
        // would put offsets back into the delivery window.
        if write.start_offset < current.start_offset
            || write.start_offset < 0
            || write.state_epoch == i32::MAX
        {
            return Err(Error::InvalidRequest);
        }
        validate_batches(&write.batches, self.limits.max_batches, write.start_offset)?;
        let updated = Partition {
            state_epoch: write.state_epoch.saturating_add(1),
            leader_epoch: write.leader_epoch,
            start_offset: write.start_offset,
            batches: write.batches,
        };
        let mut next = self.partitions.clone();
        next.insert(key, updated.clone());
        self.commit(next)?;
        Ok(updated)
    }

    /// Return acquired spans to available without clearing counts or acknowledgements.
    pub fn timeout(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        state_epoch: i32,
    ) -> Result<Partition, Error> {
        self.ready()?;
        let key = key(group, topic, partition)?;
        let Some(current) = self.partitions.get(&key).cloned() else {
            return Err(Error::NotFound);
        };
        if state_epoch != current.state_epoch || current.state_epoch == i32::MAX {
            return Err(Error::Fenced);
        }
        let mut updated = current;
        for batch in &mut updated.batches {
            if batch.state == DeliveryState::Acquired {
                batch.state = DeliveryState::Available;
            }
        }
        updated.state_epoch = updated.state_epoch.saturating_add(1);
        let mut next = self.partitions.clone();
        next.insert(key, updated.clone());
        self.commit(next)?;
        Ok(updated)
    }

    /// Remove one partition. A missing partition is an error and changes nothing else.
    pub fn delete(&mut self, group: &str, topic: &str, partition: i32) -> Result<(), Error> {
        self.ready()?;
        let key = key(group, topic, partition)?;
        if !self.partitions.contains_key(&key) {
            return Err(Error::NotFound);
        }
        let mut next = self.partitions.clone();
        next.remove(&key);
        self.commit(next)
    }

    fn commit(&mut self, partitions: BTreeMap<Key, Partition>) -> Result<(), Error> {
        let payload = encode(&partitions)?;
        if payload.len() > self.limits.journal.max_entry_bytes() {
            return Err(Error::InvalidLimits);
        }
        self.persist(&payload)?;
        self.partitions = partitions;
        Ok(())
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        if self.stored >= self.limits.max_partitions.saturating_mul(8) {
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

fn key(group: &str, topic: &str, partition: i32) -> Result<Key, Error> {
    if !valid_name(group) || !valid_name(topic) || partition < 0 {
        return Err(Error::InvalidRequest);
    }
    Ok(Key {
        group: group.to_owned(),
        topic: topic.to_owned(),
        partition,
    })
}

fn valid_name(text: &str) -> bool {
    !text.is_empty() && text.len() <= 249 && !text.chars().any(char::is_control)
}

fn validate_batches(batches: &[Batch], max_batches: usize, start_offset: i64) -> Result<(), Error> {
    if batches.len() > max_batches {
        return Err(Error::InvalidLimits);
    }
    let mut previous_last = -1i64;
    for batch in batches {
        if batch.first_offset < start_offset
            || batch.last_offset < batch.first_offset
            || batch.first_offset <= previous_last
        {
            return Err(Error::InvalidRequest);
        }
        if batch.state != DeliveryState::Available && batch.delivery_count == 0 {
            return Err(Error::InvalidRequest);
        }
        previous_last = batch.last_offset;
    }
    Ok(())
}

fn acknowledged_offset(batches: &[Batch]) -> Option<i64> {
    batches
        .iter()
        .filter(|batch| batch.state == DeliveryState::Acknowledged)
        .map(|batch| batch.last_offset.saturating_add(1))
        .max()
}

fn encode(partitions: &BTreeMap<Key, Partition>) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    put_u32(
        &mut bytes,
        u32::try_from(partitions.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for (key, partition) in partitions {
        put_str(&mut bytes, &key.group)?;
        put_str(&mut bytes, &key.topic)?;
        bytes.extend_from_slice(&key.partition.to_be_bytes());
        bytes.extend_from_slice(&partition.state_epoch.to_be_bytes());
        bytes.extend_from_slice(&partition.leader_epoch.to_be_bytes());
        bytes.extend_from_slice(&partition.start_offset.to_be_bytes());
        put_u32(
            &mut bytes,
            u32::try_from(partition.batches.len()).map_err(|_| Error::InvalidRequest)?,
        )?;
        for batch in &partition.batches {
            bytes.extend_from_slice(&batch.first_offset.to_be_bytes());
            bytes.extend_from_slice(&batch.last_offset.to_be_bytes());
            bytes.extend_from_slice(&batch.delivery_count.to_be_bytes());
            bytes.push(match batch.state {
                DeliveryState::Available => 0,
                DeliveryState::Acquired => 1,
                DeliveryState::Acknowledged => 2,
                DeliveryState::Archived => 4,
            });
        }
    }
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<BTreeMap<Key, Partition>, Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let count = take_u32(&mut bytes)?;
    let mut partitions = BTreeMap::new();
    for _ in 0..count {
        let key = Key {
            group: take_str(&mut bytes)?,
            topic: take_str(&mut bytes)?,
            partition: take_i32(&mut bytes)?,
        };
        let state_epoch = take_i32(&mut bytes)?;
        let leader_epoch = take_i32(&mut bytes)?;
        let start_offset = take_i64(&mut bytes)?;
        let batch_count = take_u32(&mut bytes)?;
        let mut batches = Vec::new();
        for _ in 0..batch_count {
            batches.push(Batch {
                first_offset: take_i64(&mut bytes)?,
                last_offset: take_i64(&mut bytes)?,
                delivery_count: take_u16(&mut bytes)?,
                state: match take_u8(&mut bytes)? {
                    0 => DeliveryState::Available,
                    1 => DeliveryState::Acquired,
                    2 => DeliveryState::Acknowledged,
                    4 => DeliveryState::Archived,
                    _ => return Err(Error::InvalidRequest),
                },
            });
        }
        partitions.insert(
            key,
            Partition {
                state_epoch,
                leader_epoch,
                start_offset,
                batches,
            },
        );
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(partitions)
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) -> Result<(), Error> {
    bytes.extend_from_slice(&value.to_be_bytes());
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
    String::from_utf8(take(input, len)?.to_vec()).map_err(|_| Error::InvalidRequest)
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

fn take_u32(input: &mut &[u8]) -> Result<u32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(u32::from_be_bytes(value))
}

fn take_u16(input: &mut &[u8]) -> Result<u16, Error> {
    let mut value = [0u8; 2];
    value.copy_from_slice(take(input, 2)?);
    Ok(u16::from_be_bytes(value))
}

fn take_u8(input: &mut &[u8]) -> Result<u8, Error> {
    Ok(take(input, 1)?[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(8192, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, 16, journal).unwrap_or_else(|_| panic!("share state limits"))
    }

    fn open(name: &str) -> (Store, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-shs-{name}-{nanos}"));
        let store = Store::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (store, path)
    }

    #[test]
    fn write_is_atomic_and_timeout_keeps_delivery_counts() {
        let (mut store, path) = open("state");
        let created = store
            .init("share", "orders", 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(created.state_epoch, 0);
        assert!(matches!(
            store.init("share", "orders", 0, 10),
            Err(Error::AlreadyInitialized)
        ));
        assert!(matches!(
            store.write(
                "share",
                "orders",
                0,
                Write {
                    state_epoch: 0,
                    leader_epoch: 0,
                    start_offset: 10,
                    batches: vec![Batch {
                        first_offset: 12,
                        last_offset: 11,
                        delivery_count: 1,
                        state: DeliveryState::Acquired,
                    },],
                },
            ),
            Err(Error::InvalidRequest)
        ));
        assert_eq!(
            store
                .read("share", "orders", 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .batches
                .len(),
            0
        );
        let written = store
            .write(
                "share",
                "orders",
                0,
                Write {
                    state_epoch: 0,
                    leader_epoch: 1,
                    start_offset: 10,
                    batches: vec![
                        Batch {
                            first_offset: 10,
                            last_offset: 12,
                            delivery_count: 2,
                            state: DeliveryState::Acquired,
                        },
                        Batch {
                            first_offset: 13,
                            last_offset: 13,
                            delivery_count: 1,
                            state: DeliveryState::Acknowledged,
                        },
                    ],
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(written.state_epoch, 1);
        assert!(matches!(
            store.write(
                "share",
                "orders",
                0,
                Write {
                    state_epoch: 0,
                    leader_epoch: 1,
                    start_offset: 99,
                    batches: Vec::new(),
                },
            ),
            Err(Error::Fenced)
        ));
        assert_eq!(
            store
                .read("share", "orders", 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .start_offset,
            10
        );
        let timed = store
            .timeout("share", "orders", 0, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(timed.state_epoch, 2);
        assert_eq!(timed.batches[0].state, DeliveryState::Available);
        assert_eq!(timed.batches[0].delivery_count, 2);
        assert_eq!(timed.batches[1].state, DeliveryState::Acknowledged);
        let summary = store
            .summary("share", "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(summary.acknowledged_offset, Some(14));
        assert_eq!(summary.state_epoch, 2);
        drop(store);
        let mut store = Store::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let restored = store
            .read("share", "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.batches[0].delivery_count, 2);
        assert_eq!(restored.batches[1].state, DeliveryState::Acknowledged);
        store
            .delete("share", "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            store.read("share", "orders", 0),
            Err(Error::NotFound)
        ));
        drop(store);
        let store = Store::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            store.read("share", "orders", 0),
            Err(Error::NotFound)
        ));
    }

    #[test]
    fn a_start_offset_does_not_move_backward_or_cover_an_earlier_batch() {
        let (mut store, path) = open("start");
        store
            .init("share", "orders", 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        store
            .write(
                "share",
                "orders",
                0,
                Write {
                    state_epoch: 0,
                    leader_epoch: 1,
                    start_offset: 10,
                    batches: vec![Batch {
                        first_offset: 10,
                        last_offset: 11,
                        delivery_count: 1,
                        state: DeliveryState::Acknowledged,
                    }],
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            store.write(
                "share",
                "orders",
                0,
                Write {
                    state_epoch: 1,
                    leader_epoch: 1,
                    start_offset: 9,
                    batches: vec![Batch {
                        first_offset: 9,
                        last_offset: 11,
                        delivery_count: 1,
                        state: DeliveryState::Available,
                    }],
                },
            ),
            Err(Error::InvalidRequest)
        ));
        assert!(matches!(
            store.write(
                "share",
                "orders",
                0,
                Write {
                    state_epoch: 1,
                    leader_epoch: 1,
                    start_offset: 12,
                    batches: vec![Batch {
                        first_offset: 11,
                        last_offset: 12,
                        delivery_count: 1,
                        state: DeliveryState::Acquired,
                    }],
                },
            ),
            Err(Error::InvalidRequest)
        ));
        let kept = store
            .read("share", "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(kept.start_offset, 10);
        assert_eq!(kept.state_epoch, 1);
        assert_eq!(kept.batches[0].state, DeliveryState::Acknowledged);
        drop(store);
        let store = Store::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let restored = store
            .read("share", "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.start_offset, 10);
        assert_eq!(restored.batches[0].last_offset, 11);
    }
}
