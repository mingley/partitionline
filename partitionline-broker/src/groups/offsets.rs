//! Durable consumer-group offsets and group administration.
//!
//! A commit must name a member and the group's current generation. A stale
//! generation commits nothing. Inside one request, an invalid partition is
//! reported on its own and the valid partitions are stored together. Deleting
//! one group does not remove another group's offsets.
//!
//! The offset map is journaled. This is not the OffsetCommit or OffsetFetch RPC.

use crate::catalog;
use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLGOFF01";

/// Kafka `ILLEGAL_GENERATION`.
pub const ILLEGAL_GENERATION: i16 = 22;
/// Kafka `UNKNOWN_MEMBER_ID`.
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Kafka `OFFSET_METADATA_TOO_LARGE`.
pub const OFFSET_METADATA_TOO_LARGE: i16 = 12;

/// One committed offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// Consumer offset.
    pub offset: i64,
    /// Optional metadata, at most 4096 bytes.
    pub metadata: String,
    /// Group generation that committed it.
    pub generation: i32,
}

/// Per-partition result of one commit request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionResult {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Zero when this partition was stored.
    pub error_code: i16,
}

/// Why a group administration call was refused.
#[derive(Debug)]
pub enum Error {
    /// The generation does not match the group.
    IllegalGeneration,
    /// The member id is empty or unknown to this commit.
    UnknownMember,
    /// The group id, topic, or offset is not usable.
    InvalidRequest,
    /// The group has no stored offsets.
    UnknownGroup,
    /// A failed append left this store unable to confirm further commits.
    Poisoned,
    /// The limits cannot store the groups.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a group-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::IllegalGeneration => ILLEGAL_GENERATION,
            Self::UnknownMember => UNKNOWN_MEMBER_ID,
            Self::InvalidRequest
            | Self::UnknownGroup
            | Self::InvalidLimits
            | Self::Poisoned
            | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Group offset bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_groups: usize,
    max_offsets: usize,
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive group and offset caps.
    pub fn new(
        max_groups: usize,
        max_offsets: usize,
        max_records: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_groups)
            || !(1..=1_000_000).contains(&max_offsets)
            || !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < 8192
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_groups,
            max_offsets,
            max_records,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Group {
    generation: i32,
    offsets: BTreeMap<(String, i32), Committed>,
}

/// Durable offset store.
pub struct Offsets {
    limits: Limits,
    groups: BTreeMap<String, Group>,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Offsets {
    /// Open or recover committed offsets.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut offsets = Self {
            limits,
            groups: BTreeMap::new(),
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut cursor = offsets.journal.base_offset();
        loop {
            let batch =
                offsets
                    .journal
                    .fetch(cursor, 16, offsets.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                offsets.groups = decode(&entry.payload)?;
                offsets.stored = offsets.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(offsets)
    }

    /// Group ids that currently have state.
    pub fn list(&self) -> Vec<String> {
        self.groups.keys().cloned().collect()
    }

    /// Generation and offset count for one group.
    pub fn describe(&self, group: &str) -> Option<(i32, usize)> {
        self.groups
            .get(group)
            .map(|group| (group.generation, group.offsets.len()))
    }

    /// Fetch one committed offset.
    pub fn fetch(&self, group: &str, topic: &str, partition: i32) -> Option<&Committed> {
        self.groups
            .get(group)
            .and_then(|group| group.offsets.get(&(topic.to_owned(), partition)))
    }

    /// Commit a batch. A generation mismatch stores nothing.
    ///
    /// Valid partitions in the batch are written together. Invalid partitions
    /// are returned with `OFFSET_METADATA_TOO_LARGE` or a local non-zero code
    /// and do not roll back the valid ones. A later copy of the same partition
    /// is rejected and does not replace the first copy. Other groups are unchanged.
    pub fn commit(
        &mut self,
        group_id: &str,
        member_id: &str,
        generation: i32,
        batch: &[(&str, i32, i64, &str)],
    ) -> Result<Vec<PartitionResult>, Error> {
        self.ready()?;
        validate_group(group_id)?;
        if member_id.is_empty() {
            return Err(Error::UnknownMember);
        }
        if batch.is_empty() || batch.len() > 1024 {
            return Err(Error::InvalidRequest);
        }
        if let Some(group) = self.groups.get(group_id) {
            if generation != group.generation {
                return Err(Error::IllegalGeneration);
            }
        } else if self.groups.len() >= self.limits.max_groups {
            return Err(Error::InvalidLimits);
        }
        let mut results = Vec::with_capacity(batch.len());
        let mut accepted = Vec::new();
        let mut seen = BTreeSet::new();
        for (topic, partition, offset, metadata) in batch {
            // The first copy of a partition is the one that can be stored.
            // A later copy must not replace it.
            let duplicate = !seen.insert((*topic, *partition));
            let error_code = if duplicate
                || catalog::validate_topic_name(topic).is_err()
                || *partition < 0
                || *offset < 0
            {
                42
            } else if metadata.len() > 4096 {
                OFFSET_METADATA_TOO_LARGE
            } else {
                0
            };
            results.push(PartitionResult {
                topic: (*topic).to_owned(),
                partition: *partition,
                error_code,
            });
            if error_code == 0 {
                accepted.push((*topic, *partition, *offset, *metadata));
            }
        }
        if !accepted.is_empty() {
            let mut fresh = BTreeSet::new();
            let existing = self.groups.get(group_id);
            for (topic, partition, _, _) in &accepted {
                let key = ((*topic).to_owned(), *partition);
                if existing.is_none_or(|group| !group.offsets.contains_key(&key)) {
                    fresh.insert(key);
                }
            }
            let stored = existing.map(|group| group.offsets.len()).unwrap_or(0);
            if stored.saturating_add(fresh.len()) > self.limits.max_offsets {
                return Err(Error::InvalidLimits);
            }
            let group = self.groups.entry(group_id.to_owned()).or_insert(Group {
                generation,
                offsets: BTreeMap::new(),
            });
            for (topic, partition, offset, metadata) in accepted {
                group.offsets.insert(
                    (topic.to_owned(), partition),
                    Committed {
                        offset,
                        metadata: metadata.to_owned(),
                        generation,
                    },
                );
            }
            self.persist()?;
        }
        Ok(results)
    }

    /// Delete specific partitions. Unknown partitions are ignored.
    pub fn delete_partitions(
        &mut self,
        group_id: &str,
        partitions: &[(&str, i32)],
    ) -> Result<usize, Error> {
        self.ready()?;
        let Some(group) = self.groups.get_mut(group_id) else {
            return Err(Error::UnknownGroup);
        };
        let mut removed = 0usize;
        for (topic, partition) in partitions {
            if group
                .offsets
                .remove(&(topic.to_string(), *partition))
                .is_some()
            {
                removed = removed.saturating_add(1);
            }
        }
        if removed > 0 {
            self.persist()?;
        }
        Ok(removed)
    }

    /// Delete one group. Other groups stay.
    pub fn delete_group(&mut self, group_id: &str) -> Result<(), Error> {
        self.ready()?;
        if self.groups.remove(group_id).is_none() {
            return Err(Error::UnknownGroup);
        }
        self.persist()?;
        Ok(())
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&mut self) -> Result<(), Error> {
        if self.stored >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        let payload = encode(&self.groups)?;
        match self.journal.append(1, &payload) {
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

fn validate_group(group: &str) -> Result<(), Error> {
    if group.is_empty() || group.len() > 249 || group.chars().any(|c| c.is_control()) {
        Err(Error::InvalidRequest)
    } else {
        Ok(())
    }
}

fn encode(groups: &BTreeMap<String, Group>) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    put_u16(&mut bytes, groups.len())?;
    for (id, group) in groups {
        put_str(&mut bytes, id)?;
        bytes.extend_from_slice(&group.generation.to_be_bytes());
        put_u16(&mut bytes, group.offsets.len())?;
        for ((topic, partition), committed) in &group.offsets {
            put_str(&mut bytes, topic)?;
            bytes.extend_from_slice(&partition.to_be_bytes());
            bytes.extend_from_slice(&committed.offset.to_be_bytes());
            bytes.extend_from_slice(&committed.generation.to_be_bytes());
            put_str(&mut bytes, &committed.metadata)?;
        }
    }
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<BTreeMap<String, Group>, Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let count = take_u16(&mut bytes)?;
    let mut groups = BTreeMap::new();
    for _ in 0..count {
        let id = take_str(&mut bytes)?;
        let generation = take_i32(&mut bytes)?;
        let offsets_len = take_u16(&mut bytes)?;
        let mut offsets = BTreeMap::new();
        for _ in 0..offsets_len {
            let topic = take_str(&mut bytes)?;
            let partition = take_i32(&mut bytes)?;
            let offset = take_i64(&mut bytes)?;
            let committed_generation = take_i32(&mut bytes)?;
            let metadata = take_str(&mut bytes)?;
            offsets.insert(
                (topic, partition),
                Committed {
                    offset,
                    metadata,
                    generation: committed_generation,
                },
            );
        }
        groups.insert(
            id,
            Group {
                generation,
                offsets,
            },
        );
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(groups)
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    put_u16(bytes, value.len())?;
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u16(bytes: &mut Vec<u8>, value: usize) -> Result<(), Error> {
    let value = u16::try_from(value).map_err(|_| Error::InvalidRequest)?;
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

fn take_u16(input: &mut &[u8]) -> Result<usize, Error> {
    Ok(usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    )))
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = take_u16(input)?;
    let bytes = take(input, len)?;
    std::str::from_utf8(bytes)
        .map(|text| text.to_owned())
        .map_err(|_| Error::InvalidRequest)
}

fn take_i32(input: &mut &[u8]) -> Result<i32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(i32::from_be_bytes(value))
}

fn take_i64(input: &mut &[u8]) -> Result<i64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(i64::from_be_bytes(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(8192, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, 32, 32, journal).unwrap_or_else(|_| panic!("offset limits"))
    }

    fn open(name: &str) -> (Offsets, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-goff-{name}-{nanos}"));
        let offsets = Offsets::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (offsets, path)
    }

    #[test]
    fn a_duplicate_commit_keeps_the_first_offset() {
        let (mut offsets, path) = open("duplicate");
        let results = offsets
            .commit(
                "orders",
                "m-1",
                1,
                &[("orders", 0, 4, "first"), ("orders", 0, 9, "second")],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].error_code, 0);
        assert_eq!(results[1].error_code, 42);
        assert_eq!(
            offsets
                .fetch("orders", "orders", 0)
                .map(|committed| committed.offset),
            Some(4)
        );
        drop(offsets);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn partial_commit_keeps_the_valid_partition_and_the_other_group() {
        let (mut offsets, path) = open("partial");
        offsets
            .commit("payments", "m-1", 1, &[("paid", 0, 5, "ok")])
            .unwrap_or_else(|error| panic!("{error:?}"));
        let results = offsets
            .commit(
                "orders",
                "m-1",
                3,
                &[
                    ("orders", 0, 9, "kept"),
                    ("orders", -1, 4, "bad"),
                    ("orders", 1, -5, "neg"),
                ],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(results[0].error_code, 0);
        assert_ne!(results[1].error_code, 0);
        assert_ne!(results[2].error_code, 0);
        assert!(matches!(
            offsets.commit("orders", "m-1", 2, &[("orders", 0, 1, "")]),
            Err(Error::IllegalGeneration)
        ));
        assert_eq!(
            offsets
                .fetch("orders", "orders", 0)
                .map(|committed| committed.offset),
            Some(9)
        );
        assert!(offsets.fetch("orders", "orders", 1).is_none());
        assert_eq!(
            offsets
                .fetch("payments", "paid", 0)
                .map(|committed| committed.offset),
            Some(5)
        );
        offsets
            .delete_group("orders")
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(offsets.fetch("orders", "orders", 0).is_none());
        assert_eq!(offsets.list(), vec!["payments".to_owned()]);
        drop(offsets);
        let restored = Offsets::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            restored
                .fetch("payments", "paid", 0)
                .map(|committed| committed.offset),
            Some(5)
        );
        assert_eq!(restored.describe("payments"), Some((1, 1)));
        assert!(restored.describe("orders").is_none());
    }

    #[test]
    fn replacing_an_offset_does_not_consume_another_slot() {
        let journal = journal::Limits::new(8192, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        let limits = Limits::new(8, 1, 32, journal).unwrap_or_else(|_| panic!("offset limits"));
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-goff-replace-{nanos}"));
        let mut offsets = Offsets::open(&path, limits).unwrap_or_else(|error| panic!("{error:?}"));
        offsets
            .commit("orders", "m-1", 1, &[("orders", 0, 4, "")])
            .unwrap_or_else(|error| panic!("{error:?}"));
        offsets
            .commit(
                "orders",
                "m-1",
                1,
                &[("orders", 0, 9, "next"), ("orders", 0, 11, "later")],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            offsets
                .fetch("orders", "orders", 0)
                .map(|committed| committed.offset),
            Some(11)
        );
        assert!(matches!(
            offsets.commit("orders", "m-1", 1, &[("orders", 1, 1, "")]),
            Err(Error::InvalidLimits)
        ));
        assert!(offsets.fetch("orders", "orders", 1).is_none());
        assert_eq!(offsets.describe("orders"), Some((1, 1)));
        assert!(matches!(
            offsets.commit(
                "payments",
                "m-1",
                1,
                &[("paid", 0, 1, ""), ("paid", 1, 2, "")]
            ),
            Err(Error::InvalidLimits)
        ));
        assert!(offsets.describe("payments").is_none());
    }
}
