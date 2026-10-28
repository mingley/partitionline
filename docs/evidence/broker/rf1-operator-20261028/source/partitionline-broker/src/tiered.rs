//! Remote segment offload with checksums and a bounded disk budget.
//!
//! `stage` writes a partial object and does not make the local copy releasable.
//! `finish` publishes it only after that partial record is durable. Restore
//! checks the checksum before returning bytes. An abort, a crash before
//! `finish`, or a retention expiry never marks the local copy releasable.
//!
//! This is not a Kafka tiered-storage RPC and it does not delete local logs.

use crate::journal::{self, Journal};
use std::collections::BTreeMap;
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLTIER01";
const KIND_PARTIAL: u8 = 1;
const KIND_COMMIT: u8 = 2;
const KIND_ABORT: u8 = 3;
const KIND_EXPIRE: u8 = 4;

/// A local segment offered for offload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment<'a> {
    /// Topic name.
    pub topic: &'a str,
    /// Partition.
    pub partition: i32,
    /// First offset in the segment.
    pub base_offset: i64,
    /// Record key that must round-trip.
    pub key: &'a [u8],
    /// Segment bytes.
    pub body: &'a [u8],
    /// Remote retention deadline, in milliseconds.
    pub retain_until_ms: i64,
}

/// Restored remote object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// First offset.
    pub base_offset: i64,
    /// Record key.
    pub key: Vec<u8>,
    /// Segment bytes.
    pub body: Vec<u8>,
    /// CRC32C of the canonical payload.
    pub checksum: u32,
}

/// Why an offload was refused.
#[derive(Debug)]
pub enum Error {
    /// The topic, offset, or retention deadline is not usable.
    InvalidRequest,
    /// The payload exceeds the transfer scratch cap.
    ScratchExceeded,
    /// Published and partial objects would exceed the disk cap.
    DiskExceeded,
    /// The object is not published, so local data must be kept.
    Incomplete,
    /// The stored checksum does not match the payload.
    Corrupt,
    /// A failed append left this store unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the store.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Offload bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_objects: usize,
    max_scratch_bytes: usize,
    max_disk_bytes: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive object, scratch, and disk caps.
    pub fn new(
        max_objects: usize,
        max_scratch_bytes: usize,
        max_disk_bytes: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_objects)
            || !(1..=16 * 1024 * 1024).contains(&max_scratch_bytes)
            || max_disk_bytes < max_scratch_bytes
            || journal.max_entry_bytes() < max_scratch_bytes.saturating_add(256)
            || journal.max_index_entries() < max_objects
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_objects,
            max_scratch_bytes,
            max_disk_bytes,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    topic: String,
    partition: i32,
    base_offset: i64,
}

struct Stored {
    object: Object,
    retain_until_ms: i64,
    committed: bool,
}

/// Durable remote segment store.
pub struct Store {
    limits: Limits,
    objects: BTreeMap<Key, Stored>,
    disk_bytes: usize,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Store {
    /// Open or recover the remote store. Partials stay unpublished.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut store = Self {
            limits,
            objects: BTreeMap::new(),
            disk_bytes: 0,
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
                store.apply(&entry.payload)?;
                store.stored = store.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(store)
    }

    /// Write a partial object. The local segment is not releasable yet.
    pub fn stage(&mut self, segment: Segment<'_>) -> Result<(), Error> {
        self.ready()?;
        let (key, object, retain_until_ms) = validate(segment)?;
        if object.body.len().saturating_add(object.key.len()) > self.limits.max_scratch_bytes {
            return Err(Error::ScratchExceeded);
        }
        if self.objects.contains_key(&key) {
            return Err(Error::InvalidRequest);
        }
        if self.objects.len() >= self.limits.max_objects {
            return Err(Error::DiskExceeded);
        }
        let next_disk = self
            .disk_bytes
            .saturating_add(object.body.len())
            .saturating_add(object.key.len());
        if next_disk > self.limits.max_disk_bytes {
            return Err(Error::DiskExceeded);
        }
        self.persist(&encode_partial(&object, retain_until_ms)?)?;
        self.disk_bytes = next_disk;
        self.objects.insert(
            key,
            Stored {
                object,
                retain_until_ms,
                committed: false,
            },
        );
        Ok(())
    }

    /// Publish a staged object. Repeating a publish is a no-op.
    pub fn finish(&mut self, segment: Segment<'_>) -> Result<(), Error> {
        self.ready()?;
        let (key, object, _) = validate(segment)?;
        let Some(stored) = self.objects.get(&key) else {
            return Err(Error::Incomplete);
        };
        if stored.committed {
            return Ok(());
        }
        if stored.object.checksum != object.checksum || stored.object.body != object.body {
            return Err(Error::Corrupt);
        }
        self.persist(&encode_key(KIND_COMMIT, &key)?)?;
        let stored = self.objects.get_mut(&key).ok_or(Error::Incomplete)?;
        stored.committed = true;
        Ok(())
    }

    /// Drop a partial object. A published object stays until retention.
    pub fn abort(&mut self, topic: &str, partition: i32, base_offset: i64) -> Result<(), Error> {
        self.ready()?;
        let key = Key {
            topic: topic.to_owned(),
            partition,
            base_offset,
        };
        let Some(stored) = self.objects.get(&key) else {
            return Err(Error::Incomplete);
        };
        if stored.committed {
            return Err(Error::InvalidRequest);
        }
        let bytes = stored
            .object
            .body
            .len()
            .saturating_add(stored.object.key.len());
        self.persist(&encode_key(KIND_ABORT, &key)?)?;
        self.objects.remove(&key);
        self.disk_bytes = self.disk_bytes.saturating_sub(bytes);
        Ok(())
    }

    /// True only after a durable publish. Callers must keep local bytes until then.
    pub fn releasable(&self, topic: &str, partition: i32, base_offset: i64) -> bool {
        self.objects
            .get(&Key {
                topic: topic.to_owned(),
                partition,
                base_offset,
            })
            .is_some_and(|stored| stored.committed)
    }

    /// Return a published object whose checksum matches its payload.
    pub fn restore(&self, topic: &str, partition: i32, base_offset: i64) -> Result<Object, Error> {
        let stored = self
            .objects
            .get(&Key {
                topic: topic.to_owned(),
                partition,
                base_offset,
            })
            .ok_or(Error::Incomplete)?;
        if !stored.committed {
            return Err(Error::Incomplete);
        }
        let checksum = checksum(&stored.object);
        if checksum != stored.object.checksum {
            return Err(Error::Corrupt);
        }
        Ok(stored.object.clone())
    }

    /// Remove objects whose retention deadline has passed.
    ///
    /// Published objects and abandoned partial uploads are both removed, so a
    /// stage that never finishes does not hold disk after its deadline. The
    /// local copy stays unreleasable.
    pub fn expire(&mut self, now_ms: i64) -> Result<usize, Error> {
        self.ready()?;
        let due: Vec<Key> = self
            .objects
            .iter()
            .filter(|(_, stored)| now_ms >= stored.retain_until_ms)
            .map(|(key, _)| key.clone())
            .collect();
        let count = due.len();
        for key in due {
            let bytes = self
                .objects
                .get(&key)
                .map(|stored| {
                    stored
                        .object
                        .body
                        .len()
                        .saturating_add(stored.object.key.len())
                })
                .unwrap_or(0);
            self.persist(&encode_key(KIND_EXPIRE, &key)?)?;
            self.objects.remove(&key);
            self.disk_bytes = self.disk_bytes.saturating_sub(bytes);
        }
        Ok(count)
    }

    /// Bytes held by partial and published payloads.
    pub fn disk_bytes(&self) -> usize {
        self.disk_bytes
    }

    /// Flip a published checksum in memory so restore fails closed.
    #[cfg(test)]
    pub fn tamper_checksum(&mut self, topic: &str, partition: i32, base_offset: i64) {
        if let Some(stored) = self.objects.get_mut(&Key {
            topic: topic.to_owned(),
            partition,
            base_offset,
        }) {
            stored.object.checksum ^= 1;
        }
    }

    fn apply(&mut self, payload: &[u8]) -> Result<(), Error> {
        match decode(payload)? {
            Record::Partial {
                object,
                retain_until_ms,
            } => {
                if !usable_key(&object.topic, object.partition, object.base_offset)
                    || retain_until_ms < 0
                    || object.key.len() > 1024
                    || object.body.is_empty()
                {
                    return Err(Error::InvalidRequest);
                }
                if checksum(&object) != object.checksum {
                    return Err(Error::Corrupt);
                }
                let key = key_of(&object);
                if self.objects.contains_key(&key) {
                    return Err(Error::InvalidRequest);
                }
                let added = object.body.len().saturating_add(object.key.len());
                self.disk_bytes = self.disk_bytes.saturating_add(added);
                self.objects.insert(
                    key,
                    Stored {
                        object,
                        retain_until_ms,
                        committed: false,
                    },
                );
            }
            Record::Commit(key) => {
                if !usable_key(&key.topic, key.partition, key.base_offset) {
                    return Err(Error::InvalidRequest);
                }
                let stored = self.objects.get_mut(&key).ok_or(Error::Incomplete)?;
                if stored.committed {
                    return Err(Error::InvalidRequest);
                }
                stored.committed = true;
            }
            Record::Abort(key) => {
                if !usable_key(&key.topic, key.partition, key.base_offset) {
                    return Err(Error::InvalidRequest);
                }
                let stored = self.objects.get(&key).ok_or(Error::Incomplete)?;
                if stored.committed {
                    return Err(Error::InvalidRequest);
                }
                let bytes = stored
                    .object
                    .body
                    .len()
                    .saturating_add(stored.object.key.len());
                self.objects.remove(&key);
                self.disk_bytes = self.disk_bytes.saturating_sub(bytes);
            }
            Record::Expire(key) => {
                if !usable_key(&key.topic, key.partition, key.base_offset) {
                    return Err(Error::InvalidRequest);
                }
                let stored = self.objects.remove(&key).ok_or(Error::Incomplete)?;
                self.disk_bytes = self.disk_bytes.saturating_sub(
                    stored
                        .object
                        .body
                        .len()
                        .saturating_add(stored.object.key.len()),
                );
            }
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

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        if self.stored >= self.limits.max_objects.saturating_mul(8) {
            return Err(Error::DiskExceeded);
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

enum Record {
    Partial {
        object: Object,
        retain_until_ms: i64,
    },
    Commit(Key),
    Abort(Key),
    Expire(Key),
}

fn usable_key(topic: &str, partition: i32, base_offset: i64) -> bool {
    !topic.is_empty()
        && topic.len() <= 249
        && !topic.chars().any(char::is_control)
        && partition >= 0
        && base_offset >= 0
}

fn validate(segment: Segment<'_>) -> Result<(Key, Object, i64), Error> {
    if segment.topic.is_empty()
        || segment.topic.len() > 249
        || segment.topic.chars().any(char::is_control)
        || segment.partition < 0
        || segment.base_offset < 0
        || segment.retain_until_ms < 0
        || segment.key.len() > 1024
        || segment.body.is_empty()
    {
        return Err(Error::InvalidRequest);
    }
    let object = Object {
        topic: segment.topic.to_owned(),
        partition: segment.partition,
        base_offset: segment.base_offset,
        key: segment.key.to_vec(),
        body: segment.body.to_vec(),
        checksum: 0,
    };
    let checksum = checksum(&object);
    let object = Object { checksum, ..object };
    Ok((key_of(&object), object, segment.retain_until_ms))
}

fn key_of(object: &Object) -> Key {
    Key {
        topic: object.topic.clone(),
        partition: object.partition,
        base_offset: object.base_offset,
    }
}

fn checksum(object: &Object) -> u32 {
    let mut bytes = Vec::new();
    let _ = put_str(&mut bytes, &object.topic);
    bytes.extend_from_slice(&object.partition.to_be_bytes());
    bytes.extend_from_slice(&object.base_offset.to_be_bytes());
    let _ = put_bytes(&mut bytes, &object.key);
    let _ = put_bytes(&mut bytes, &object.body);
    crc32c::crc32c(&bytes)
}

fn encode_partial(object: &Object, retain_until_ms: i64) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_PARTIAL);
    put_str(&mut bytes, &object.topic)?;
    bytes.extend_from_slice(&object.partition.to_be_bytes());
    bytes.extend_from_slice(&object.base_offset.to_be_bytes());
    bytes.extend_from_slice(&retain_until_ms.to_be_bytes());
    bytes.extend_from_slice(&object.checksum.to_be_bytes());
    put_bytes(&mut bytes, &object.key)?;
    put_bytes(&mut bytes, &object.body)?;
    Ok(bytes)
}

fn encode_key(kind: u8, key: &Key) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(kind);
    put_str(&mut bytes, &key.topic)?;
    bytes.extend_from_slice(&key.partition.to_be_bytes());
    bytes.extend_from_slice(&key.base_offset.to_be_bytes());
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<Record, Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    let kind = bytes[RECORD_MAGIC.len()];
    bytes = &bytes[RECORD_MAGIC.len() + 1..];
    let record = match kind {
        KIND_PARTIAL => {
            let topic = take_str(&mut bytes)?;
            let partition = take_i32(&mut bytes)?;
            let base_offset = take_i64(&mut bytes)?;
            let retain_until_ms = take_i64(&mut bytes)?;
            let checksum = take_u32(&mut bytes)?;
            let key = take_bytes(&mut bytes)?;
            let body = take_bytes(&mut bytes)?;
            Record::Partial {
                object: Object {
                    topic,
                    partition,
                    base_offset,
                    key,
                    body,
                    checksum,
                },
                retain_until_ms,
            }
        }
        KIND_COMMIT => Record::Commit(take_key(&mut bytes)?),
        KIND_ABORT => Record::Abort(take_key(&mut bytes)?),
        KIND_EXPIRE => Record::Expire(take_key(&mut bytes)?),
        _ => return Err(Error::InvalidRequest),
    };
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(record)
}

fn take_key(bytes: &mut &[u8]) -> Result<Key, Error> {
    Ok(Key {
        topic: take_str(bytes)?,
        partition: take_i32(bytes)?,
        base_offset: take_i64(bytes)?,
    })
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    put_bytes(bytes, value.as_bytes())
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), Error> {
    let len = u32::try_from(value.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value);
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

fn take_bytes(input: &mut &[u8]) -> Result<Vec<u8>, Error> {
    let len = usize::try_from(u32::from_be_bytes(
        take(input, 4)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ))
    .map_err(|_| Error::InvalidRequest)?;
    Ok(take(input, len)?.to_vec())
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let bytes = take_bytes(input)?;
    String::from_utf8(bytes).map_err(|_| Error::InvalidRequest)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits(scratch: usize, disk: usize) -> Limits {
        let journal = journal::Limits::new(scratch.saturating_add(256), 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, scratch, disk, journal).unwrap_or_else(|_| panic!("tier limits"))
    }

    fn path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("pl-tier-{name}-{nanos}"))
    }

    fn segment<'a>(offset: i64, key: &'a [u8], body: &'a [u8], retain: i64) -> Segment<'a> {
        Segment {
            topic: "orders",
            partition: 0,
            base_offset: offset,
            key,
            body,
            retain_until_ms: retain,
        }
    }

    #[test]
    fn an_abort_of_a_published_object_is_not_recovered() {
        let dir = path("abort-published");
        let mut store =
            Store::open(&dir, limits(1024, 4096)).unwrap_or_else(|error| panic!("{error:?}"));
        store
            .stage(segment(4, b"k", b"local", 5_000))
            .unwrap_or_else(|error| panic!("{error:?}"));
        store
            .finish(segment(4, b"k", b"local", 5_000))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let encoded = encode_key(
            KIND_ABORT,
            &Key {
                topic: "orders".to_owned(),
                partition: 0,
                base_offset: 4,
            },
        )
        .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(store.apply(&encoded), Err(Error::InvalidRequest)));
        assert!(store.releasable("orders", 0, 4));
    }

    #[test]
    fn a_negative_partition_is_not_recovered() {
        let dir = path("bad-partition");
        let mut store =
            Store::open(&dir, limits(1024, 4096)).unwrap_or_else(|error| panic!("{error:?}"));
        let mut object = Object {
            topic: "orders".to_owned(),
            partition: -1,
            base_offset: 0,
            key: b"k".to_vec(),
            body: b"v".to_vec(),
            checksum: 0,
        };
        object.checksum = checksum(&object);
        let encoded = encode_partial(&object, 1).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(store.apply(&encoded), Err(Error::InvalidRequest)));
        assert_eq!(store.disk_bytes(), 0);
        let missing = encode_key(
            KIND_COMMIT,
            &Key {
                topic: "orders".to_owned(),
                partition: 0,
                base_offset: 0,
            },
        )
        .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(store.apply(&missing), Err(Error::Incomplete)));
    }

    #[test]
    fn partial_offload_keeps_local_data_and_restore_checks_the_checksum() {
        let dir = path("tier");
        let mut store =
            Store::open(&dir, limits(1024, 4096)).unwrap_or_else(|error| panic!("{error:?}"));
        let offered = segment(10, b"k", b"value-10", 5_000);
        store
            .stage(offered)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(!store.releasable("orders", 0, 10));
        assert!(matches!(
            store.restore("orders", 0, 10),
            Err(Error::Incomplete)
        ));
        drop(store);
        let restored =
            Store::open(&dir, limits(1024, 4096)).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(!restored.releasable("orders", 0, 10));
        assert!(matches!(
            restored.restore("orders", 0, 10),
            Err(Error::Incomplete)
        ));
        drop(restored);
        let mut store =
            Store::open(&dir, limits(1024, 4096)).unwrap_or_else(|error| panic!("{error:?}"));
        store
            .finish(offered)
            .unwrap_or_else(|error| panic!("{error:?}"));
        store
            .finish(offered)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(store.releasable("orders", 0, 10));
        let object = store
            .restore("orders", 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(object.key, b"k");
        assert_eq!(object.body, b"value-10");
        assert_eq!(object.base_offset, 10);
        store.tamper_checksum("orders", 0, 10);
        assert!(matches!(
            store.restore("orders", 0, 10),
            Err(Error::Corrupt)
        ));
        drop(store);
        let mut store =
            Store::open(&dir, limits(1024, 4096)).unwrap_or_else(|error| panic!("{error:?}"));
        let object = store
            .restore("orders", 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(object.key, b"k");
        assert_eq!(object.body, b"value-10");
        assert!(matches!(
            store.stage(segment(11, b"k", &vec![1u8; 2000], 5_000)),
            Err(Error::ScratchExceeded)
        ));
        assert!(!store.releasable("orders", 0, 11));
        store
            .stage(segment(11, b"m", b"other", 1_000))
            .unwrap_or_else(|error| panic!("{error:?}"));
        store
            .abort("orders", 0, 11)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(!store.releasable("orders", 0, 11));
        store
            .stage(segment(12, b"n", b"kept", 1_000))
            .unwrap_or_else(|error| panic!("{error:?}"));
        store
            .finish(segment(12, b"n", b"kept", 1_000))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            store
                .expire(1_000)
                .unwrap_or_else(|error| panic!("{error:?}")),
            1
        );
        assert!(store.releasable("orders", 0, 10));
        assert!(!store.releasable("orders", 0, 12));
    }

    #[test]
    fn an_abandoned_partial_is_dropped_when_its_deadline_passes() {
        let dir = path("partial-expire");
        let mut store =
            Store::open(&dir, limits(1024, 4096)).unwrap_or_else(|error| panic!("{error:?}"));
        store
            .stage(segment(4, b"k", b"local", 1_000))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let held = store.disk_bytes();
        assert!(held > 0);
        assert_eq!(store.expire(999).unwrap_or(99), 0);
        assert_eq!(store.disk_bytes(), held);
        assert!(!store.releasable("orders", 0, 4));
        assert_eq!(store.expire(1_000).unwrap_or(99), 1);
        assert_eq!(store.disk_bytes(), 0);
        assert!(matches!(
            store.finish(segment(4, b"k", b"local", 1_000)),
            Err(Error::Incomplete)
        ));
        store
            .stage(segment(4, b"k", b"local", 5_000))
            .unwrap_or_else(|error| panic!("{error:?}"));
        store
            .finish(segment(4, b"k", b"local", 5_000))
            .unwrap_or_else(|error| panic!("{error:?}"));
        drop(store);
        let store =
            Store::open(&dir, limits(1024, 4096)).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(store.releasable("orders", 0, 4));
        assert_eq!(store.disk_bytes(), held);
    }
}
