//! Transactional consumer-offset commits.
//!
//! Data records and group offsets staged by one producer epoch stay hidden
//! until that epoch's marker commits them. A marker decides only the records
//! written since the previous marker, so the same epoch can start another
//! transaction. An abort hides its own records. A later committed epoch of the
//! same producer can publish new offsets. A generation older than one still
//! open or already committed is refused. An aborted generation does not raise
//! that floor. An older producer epoch stores nothing. Repeating a commit after a lost
//! acknowledgement returns success and does not append another marker.
//!
//! The log is journaled, so reopen shows the same committed offsets and data.
//! This is not the transactional OffsetCommit RPC.

use crate::catalog;
use crate::journal::{self, Journal};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLTXNO01";
const KIND_DATA: u8 = 1;
const KIND_OFFSET: u8 = 2;
const KIND_MARKER: u8 = 3;

/// Kafka `ILLEGAL_GENERATION`.
pub const ILLEGAL_GENERATION: i16 = 22;
/// Kafka `UNKNOWN_MEMBER_ID`.
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Kafka `PRODUCER_FENCED`.
pub const PRODUCER_FENCED: i16 = 90;

/// One offset staged inside a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedOffset<'a> {
    /// Consumer group.
    pub group: &'a str,
    /// Member id that staged the offset.
    pub member: &'a str,
    /// Group generation.
    pub generation: i32,
    /// Topic.
    pub topic: &'a str,
    /// Partition.
    pub partition: i32,
    /// Consumer offset.
    pub offset: i64,
}

/// A group offset that became visible with its transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleOffset {
    /// Consumer group.
    pub group: String,
    /// Member id that staged the offset.
    pub member: String,
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Committed offset.
    pub offset: i64,
    /// Group generation that staged it.
    pub generation: i32,
}

/// Why a transactional offset write was refused.
#[derive(Debug)]
pub enum Error {
    /// The group generation is older than one already staged for this group.
    IllegalGeneration,
    /// The member id is empty.
    UnknownMember,
    /// The producer epoch is older than one already used for this id.
    ProducerFenced,
    /// The producer, group, or offset is not usable.
    InvalidRequest,
    /// The epoch already has the opposite marker.
    ConflictingMarker,
    /// A failed append left this log unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the log.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a caller-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::IllegalGeneration => ILLEGAL_GENERATION,
            Self::UnknownMember => UNKNOWN_MEMBER_ID,
            Self::ProducerFenced => PRODUCER_FENCED,
            Self::InvalidRequest
            | Self::ConflictingMarker
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

/// Log bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a positive record cap.
    pub fn new(max_records: usize, journal: journal::Limits) -> Result<Self, Error> {
        if !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < 1024
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum Row {
    Data {
        producer_id: i64,
        epoch: i16,
        id: u64,
    },
    Offset {
        producer_id: i64,
        epoch: i16,
        group: String,
        member: String,
        generation: i32,
        topic: String,
        partition: i32,
        offset: i64,
    },
    Marker {
        producer_id: i64,
        epoch: i16,
        commit: bool,
    },
}

/// Durable consume-transform-produce offset log.
pub struct TxnOffsets {
    limits: Limits,
    rows: Vec<Row>,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl TxnOffsets {
    /// Open or recover the transactional offset log.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut log = Self {
            limits,
            rows: Vec::new(),
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut cursor = log.journal.base_offset();
        loop {
            let batch = log
                .journal
                .fetch(cursor, 32, log.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                log.rows.push(decode(&entry.payload)?);
                log.stored = log.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(log)
    }

    /// Stage one input record. It stays hidden until this epoch commits.
    pub fn append_data(&mut self, producer_id: i64, epoch: i16, id: u64) -> Result<(), Error> {
        self.ready()?;
        self.check_epoch(producer_id, epoch)?;
        self.push(Row::Data {
            producer_id,
            epoch,
            id,
        })
    }

    /// Stage a group offset in the same transaction as the consumed data.
    pub fn stage_offset(
        &mut self,
        producer_id: i64,
        epoch: i16,
        staged: StagedOffset<'_>,
    ) -> Result<(), Error> {
        self.ready()?;
        self.check_epoch(producer_id, epoch)?;
        if staged.group.is_empty()
            || staged.member.is_empty()
            || staged.member.len() > 249
            || catalog::validate_topic_name(staged.topic).is_err()
            || staged.partition < 0
            || staged.offset < 0
            || staged.generation < 0
        {
            return Err(if staged.member.is_empty() {
                Error::UnknownMember
            } else {
                Error::InvalidRequest
            });
        }
        if let Some(existing) = self.generation_for(staged.group) {
            if staged.generation < existing {
                return Err(Error::IllegalGeneration);
            }
        }
        self.push(Row::Offset {
            producer_id,
            epoch,
            group: staged.group.to_owned(),
            member: staged.member.to_owned(),
            generation: staged.generation,
            topic: staged.topic.to_owned(),
            partition: staged.partition,
            offset: staged.offset,
        })
    }

    /// Commit or abort the open transaction for this producer epoch.
    ///
    /// A repeat of that decision, with no data or offset staged after it, is a
    /// no-op. Records staged after the marker belong to the next transaction.
    pub fn marker(&mut self, producer_id: i64, epoch: i16, commit: bool) -> Result<(), Error> {
        self.ready()?;
        self.check_epoch(producer_id, epoch)?;
        if let Some(stored) = self.trailing_marker(producer_id, epoch) {
            if stored != commit {
                return Err(Error::ConflictingMarker);
            }
            return Ok(());
        }
        self.push(Row::Marker {
            producer_id,
            epoch,
            commit,
        })
    }

    /// Application ids whose transaction has committed and is stable.
    pub fn visible_data(&self) -> Vec<u64> {
        let stable = self.last_stable();
        self.rows
            .iter()
            .enumerate()
            .take(stable)
            .filter_map(|(index, row)| match row {
                Row::Data {
                    producer_id,
                    epoch,
                    id,
                } if self.closing_marker(*producer_id, *epoch, index) == Some(true) => Some(*id),
                _ => None,
            })
            .collect()
    }

    /// Group offsets whose transaction has committed and is stable.
    pub fn visible_offsets(&self) -> Vec<VisibleOffset> {
        let stable = self.last_stable();
        self.rows
            .iter()
            .enumerate()
            .take(stable)
            .filter_map(|(index, row)| match row {
                Row::Offset {
                    producer_id,
                    epoch,
                    group,
                    member,
                    generation,
                    topic,
                    partition,
                    offset,
                } if self.closing_marker(*producer_id, *epoch, index) == Some(true) => {
                    Some(VisibleOffset {
                        group: group.clone(),
                        member: member.clone(),
                        topic: topic.clone(),
                        partition: *partition,
                        offset: *offset,
                        generation: *generation,
                    })
                }
                _ => None,
            })
            .collect()
    }

    fn last_stable(&self) -> usize {
        for (index, row) in self.rows.iter().enumerate() {
            let key = match row {
                Row::Data {
                    producer_id, epoch, ..
                }
                | Row::Offset {
                    producer_id, epoch, ..
                } => Some((*producer_id, *epoch)),
                Row::Marker { .. } => None,
            };
            if let Some((producer_id, epoch)) = key {
                if self.closing_marker(producer_id, epoch, index).is_none() {
                    return index;
                }
            }
        }
        self.rows.len()
    }

    fn closing_marker(&self, producer_id: i64, epoch: i16, index: usize) -> Option<bool> {
        for row in self.rows.iter().skip(index.saturating_add(1)) {
            if let Row::Marker {
                producer_id: id,
                epoch: stored,
                commit,
            } = row
            {
                if *id == producer_id && *stored == epoch {
                    return Some(*commit);
                }
            }
        }
        None
    }

    fn trailing_marker(&self, producer_id: i64, epoch: i16) -> Option<bool> {
        let mut marker = None;
        for row in &self.rows {
            match row {
                Row::Data {
                    producer_id: id,
                    epoch: stored,
                    ..
                }
                | Row::Offset {
                    producer_id: id,
                    epoch: stored,
                    ..
                } if *id == producer_id && *stored == epoch => marker = None,
                Row::Marker {
                    producer_id: id,
                    epoch: stored,
                    commit,
                } if *id == producer_id && *stored == epoch => marker = Some(*commit),
                _ => {}
            }
        }
        marker
    }

    fn generation_for(&self, group: &str) -> Option<i32> {
        self.rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| match row {
                Row::Offset {
                    producer_id,
                    epoch,
                    group: stored,
                    generation,
                    ..
                } if stored == group
                    && self.closing_marker(*producer_id, *epoch, index) != Some(false) =>
                {
                    Some(*generation)
                }
                _ => None,
            })
            .max()
    }

    fn check_epoch(&self, producer_id: i64, epoch: i16) -> Result<(), Error> {
        if producer_id <= 0 || epoch < 0 {
            return Err(Error::InvalidRequest);
        }
        let newer = self.rows.iter().any(|row| match row {
            Row::Data {
                producer_id: id,
                epoch: stored,
                ..
            }
            | Row::Offset {
                producer_id: id,
                epoch: stored,
                ..
            }
            | Row::Marker {
                producer_id: id,
                epoch: stored,
                ..
            } => *id == producer_id && *stored > epoch,
        });
        if newer {
            Err(Error::ProducerFenced)
        } else {
            Ok(())
        }
    }

    fn push(&mut self, row: Row) -> Result<(), Error> {
        if self.rows.len() >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        self.persist(&encode(&row)?)?;
        self.rows.push(row);
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

fn encode(row: &Row) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    match row {
        Row::Data {
            producer_id,
            epoch,
            id,
        } => {
            bytes.push(KIND_DATA);
            bytes.extend_from_slice(&producer_id.to_be_bytes());
            bytes.extend_from_slice(&epoch.to_be_bytes());
            bytes.extend_from_slice(&id.to_be_bytes());
        }
        Row::Offset {
            producer_id,
            epoch,
            group,
            member,
            generation,
            topic,
            partition,
            offset,
        } => {
            bytes.push(KIND_OFFSET);
            bytes.extend_from_slice(&producer_id.to_be_bytes());
            bytes.extend_from_slice(&epoch.to_be_bytes());
            put_str(&mut bytes, group)?;
            put_str(&mut bytes, member)?;
            bytes.extend_from_slice(&generation.to_be_bytes());
            put_str(&mut bytes, topic)?;
            bytes.extend_from_slice(&partition.to_be_bytes());
            bytes.extend_from_slice(&offset.to_be_bytes());
        }
        Row::Marker {
            producer_id,
            epoch,
            commit,
        } => {
            bytes.push(KIND_MARKER);
            bytes.extend_from_slice(&producer_id.to_be_bytes());
            bytes.extend_from_slice(&epoch.to_be_bytes());
            bytes.push(u8::from(*commit));
        }
    }
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<Row, Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    let kind = bytes[RECORD_MAGIC.len()];
    bytes = &bytes[RECORD_MAGIC.len() + 1..];
    let producer_id = take_i64(&mut bytes)?;
    let epoch = take_i16(&mut bytes)?;
    let row = match kind {
        KIND_DATA => Row::Data {
            producer_id,
            epoch,
            id: take_u64(&mut bytes)?,
        },
        KIND_OFFSET => {
            let group = take_str(&mut bytes)?;
            let member = take_str(&mut bytes)?;
            let generation = take_i32(&mut bytes)?;
            let topic = take_str(&mut bytes)?;
            let partition = take_i32(&mut bytes)?;
            let offset = take_i64(&mut bytes)?;
            Row::Offset {
                producer_id,
                epoch,
                group,
                member,
                generation,
                topic,
                partition,
                offset,
            }
        }
        KIND_MARKER => Row::Marker {
            producer_id,
            epoch,
            commit: take_u8(&mut bytes)? == 1,
        },
        _ => return Err(Error::InvalidRequest),
    };
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(row)
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

fn take_u64(input: &mut &[u8]) -> Result<u64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(u64::from_be_bytes(value))
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

fn take_u8(input: &mut &[u8]) -> Result<u8, Error> {
    Ok(take(input, 1)?[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(1024, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(32, journal).unwrap_or_else(|_| panic!("txn offset limits"))
    }

    fn staged<'a>(
        group: &'a str,
        member: &'a str,
        generation: i32,
        topic: &'a str,
        partition: i32,
        offset: i64,
    ) -> StagedOffset<'a> {
        StagedOffset {
            group,
            member,
            generation,
            topic,
            partition,
            offset,
        }
    }

    fn open(name: &str) -> (TxnOffsets, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-txno-{name}-{nanos}"));
        let log = TxnOffsets::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (log, path)
    }

    #[test]
    fn offsets_and_data_stay_hidden_until_commit_and_a_lost_ack_does_not_duplicate() {
        let (mut log, path) = open("atomic");
        log.append_data(4, 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.stage_offset(4, 0, staged("orders", "m-1", 2, "orders", 0, 10))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(log.visible_data().is_empty());
        assert!(log.visible_offsets().is_empty());
        log.marker(4, 0, false)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(log.visible_data().is_empty());
        assert!(log.visible_offsets().is_empty());
        log.append_data(4, 1, 11)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            log.stage_offset(4, 1, staged("orders", "", 2, "orders", 0, 11)),
            Err(Error::UnknownMember)
        ));
        log.stage_offset(4, 1, staged("orders", "m-1", 2, "orders", 0, 11))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            log.stage_offset(4, 1, staged("orders", "m-1", 1, "orders", 1, 1)),
            Err(Error::IllegalGeneration)
        ));
        assert!(matches!(
            log.stage_offset(4, 0, staged("orders", "m-1", 2, "orders", 1, 1)),
            Err(Error::ProducerFenced)
        ));
        log.marker(4, 1, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let before = log.rows.len();
        log.marker(4, 1, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.rows.len(), before);
        assert_eq!(log.visible_data(), vec![11]);
        let visible = log.visible_offsets();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].offset, 11);
        assert_eq!(visible[0].member, "m-1");
        drop(log);
        let restored =
            TxnOffsets::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.visible_data(), vec![11]);
        let restored_offsets = restored.visible_offsets();
        assert_eq!(restored_offsets[0].offset, 11);
        assert_eq!(restored_offsets[0].member, "m-1");
        assert!(restored.visible_data().iter().all(|id| *id != 10));
    }

    #[test]
    fn a_later_transaction_in_the_same_epoch_does_not_publish_early() {
        let (mut log, path) = open("same-epoch");
        log.append_data(4, 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.stage_offset(4, 0, staged("orders", "m-1", 2, "orders", 0, 10))
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.marker(4, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.append_data(4, 0, 11)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.stage_offset(4, 0, staged("orders", "m-1", 3, "orders", 1, 12))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.visible_data(), vec![10]);
        assert_eq!(log.visible_offsets().len(), 1);
        assert_eq!(log.visible_offsets()[0].offset, 10);
        let before = log.rows.len();
        log.marker(4, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.marker(4, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.rows.len(), before.saturating_add(1));
        assert_eq!(log.visible_data(), vec![10, 11]);
        let visible = log.visible_offsets();
        assert_eq!(visible.len(), 2);
        assert_eq!(visible[1].offset, 12);
        assert!(matches!(
            log.marker(4, 0, false),
            Err(Error::ConflictingMarker)
        ));
        log.append_data(4, 0, 13)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.stage_offset(4, 0, staged("orders", "m-1", 4, "orders", 2, 14))
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.marker(4, 0, false)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.visible_data(), vec![10, 11]);
        assert!(log
            .visible_offsets()
            .iter()
            .all(|offset| offset.offset != 14));
        drop(log);
        let restored =
            TxnOffsets::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.visible_data(), vec![10, 11]);
        assert_eq!(restored.visible_offsets().len(), 2);
        assert!(restored
            .visible_offsets()
            .iter()
            .all(|offset| offset.offset != 14));
    }

    #[test]
    fn an_aborted_generation_does_not_fence_the_next_transaction() {
        let (mut log, path) = open("generation");
        log.stage_offset(4, 0, staged("orders", "m-1", 5, "orders", 0, 9))
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.marker(4, 0, false)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.stage_offset(4, 0, staged("orders", "m-1", 2, "orders", 0, 10))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            log.stage_offset(4, 0, staged("orders", "m-1", 1, "orders", 1, 1)),
            Err(Error::IllegalGeneration)
        ));
        log.marker(4, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            log.stage_offset(4, 1, staged("orders", "m-1", 1, "orders", 1, 1)),
            Err(Error::IllegalGeneration)
        ));
        assert!(log
            .visible_offsets()
            .iter()
            .all(|offset| offset.generation != 5));
        drop(log);
        let log = TxnOffsets::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.visible_offsets()[0].generation, 2);
        assert!(log
            .visible_offsets()
            .iter()
            .all(|offset| offset.offset != 9));
    }
}
