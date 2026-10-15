//! Transaction markers and read-committed visibility.
//!
//! Data records belong to a producer epoch. A commit or abort marker decides
//! the records written since the previous marker for that epoch. The same
//! epoch may start another transaction after the marker. The last stable
//! offset is the first offset of the oldest undecided transaction, or the log
//! end when every transaction has a marker. `read_committed` returns only data
//! below that offset whose own marker committed. Aborted and incomplete
//! records stay hidden. A later committed epoch of the same producer id is
//! visible on its own.
//!
//! A repeated marker returns the original offset and does not append another
//! control record. Reopen restores the same committed history. This is not a
//! Fetch response and it does not write Kafka control batches.

use crate::journal::{self, Journal};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLTXNM01";
const KIND_DATA: u8 = 1;
const KIND_MARKER: u8 = 2;

/// One log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    /// An application record inside a transaction.
    Data {
        /// Log offset.
        offset: i64,
        /// Producer id.
        producer_id: i64,
        /// Producer epoch.
        epoch: i16,
        /// Application id.
        id: u64,
    },
    /// A commit or abort control record.
    Marker {
        /// Log offset.
        offset: i64,
        /// Producer id.
        producer_id: i64,
        /// Producer epoch.
        epoch: i16,
        /// True when the transaction committed.
        commit: bool,
    },
}

/// Why a marker write was refused.
#[derive(Debug)]
pub enum Error {
    /// The producer epoch already has the opposite marker.
    ConflictingMarker,
    /// The producer, epoch, or id is not usable.
    InvalidRequest,
    /// A failed append left this log unable to confirm further markers.
    Poisoned,
    /// The limits cannot store the log.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
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
            || journal.max_entry_bytes() < 40
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

/// Durable transaction marker log.
pub struct MarkerLog {
    limits: Limits,
    entries: Vec<Entry>,
    journal: Journal,
    /// Reused journal record. Cleared and rewritten on each append.
    encoded: Vec<u8>,
    stored: usize,
    poisoned: bool,
}

impl MarkerLog {
    /// Open or recover a marker log.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut log = Self {
            limits,
            entries: Vec::new(),
            journal,
            encoded: Vec::new(),
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
                    return Err(Error::InvalidRequest);
                }
                replay_payload(&mut log, &entry.payload)?;
                log.stored = log.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(log)
    }

    /// Exclusive log end.
    pub fn log_end_offset(&self) -> i64 {
        self.entries.len() as i64
    }

    /// First offset that is not yet stable.
    pub fn last_stable_offset(&self) -> i64 {
        for (index, entry) in self.entries.iter().enumerate() {
            if let Entry::Data {
                producer_id, epoch, ..
            } = entry
            {
                if self.closing_marker(*producer_id, *epoch, index).is_none() {
                    return index as i64;
                }
            }
        }
        self.log_end_offset()
    }

    /// Application ids visible to a `read_committed` consumer from `from`.
    ///
    /// Offsets at or after the last stable offset are omitted. Aborted
    /// transactions are omitted. Control markers are not application data.
    pub fn read_committed(&self, from: i64) -> Vec<u64> {
        let stable = self.last_stable_offset();
        let mut ids = Vec::new();
        for (index, entry) in self.entries.iter().enumerate() {
            let offset = index as i64;
            if offset < from || offset >= stable {
                continue;
            }
            if let Entry::Data {
                producer_id,
                epoch,
                id,
                ..
            } = entry
            {
                if self.closing_marker(*producer_id, *epoch, index) == Some(true) {
                    ids.push(*id);
                }
            }
        }
        ids
    }

    /// Append one transactional data record.
    pub fn append_data(&mut self, producer_id: i64, epoch: i16, id: u64) -> Result<i64, Error> {
        self.ready()?;
        if producer_id <= 0 || epoch < 0 {
            return Err(Error::InvalidRequest);
        }
        if self.entries.len() >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        let offset = self.log_end_offset();
        let entry = Entry::Data {
            offset,
            producer_id,
            epoch,
            id,
        };
        self.append_encoded(|bytes| write(bytes, &entry))?;
        self.entries.push(entry);
        Ok(offset)
    }

    /// Append a commit or abort marker, or return the original marker offset.
    ///
    /// The marker decides the data written since the previous marker for this
    /// producer epoch. A retry that adds no new data returns that marker. The
    /// opposite decision is refused. Data appended after the marker belongs to
    /// the next transaction and stays hidden until its own marker.
    pub fn append_marker(
        &mut self,
        producer_id: i64,
        epoch: i16,
        commit: bool,
    ) -> Result<i64, Error> {
        self.ready()?;
        if producer_id <= 0 || epoch < 0 {
            return Err(Error::InvalidRequest);
        }
        if let Some((offset, stored)) = self.trailing_marker(producer_id, epoch) {
            if stored != commit {
                return Err(Error::ConflictingMarker);
            }
            return Ok(offset);
        }
        if self.entries.len() >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        let offset = self.log_end_offset();
        let entry = Entry::Marker {
            offset,
            producer_id,
            epoch,
            commit,
        };
        self.append_encoded(|bytes| write(bytes, &entry))?;
        self.entries.push(entry);
        Ok(offset)
    }

    /// Commit flag of the marker that closes the data record at `data_index`.
    ///
    /// `None` means that record is still in an open transaction.
    fn closing_marker(&self, producer_id: i64, epoch: i16, data_index: usize) -> Option<bool> {
        for entry in self.entries.iter().skip(data_index.saturating_add(1)) {
            if let Entry::Marker {
                producer_id: id,
                epoch: stored,
                commit,
                ..
            } = entry
            {
                if *id == producer_id && *stored == epoch {
                    return Some(*commit);
                }
            }
        }
        None
    }

    /// Marker that already decided this epoch and has no data after it.
    fn trailing_marker(&self, producer_id: i64, epoch: i16) -> Option<(i64, bool)> {
        let mut marker = None;
        for entry in &self.entries {
            match entry {
                Entry::Data {
                    producer_id: id,
                    epoch: stored,
                    ..
                } if *id == producer_id && *stored == epoch => marker = None,
                Entry::Marker {
                    offset,
                    producer_id: id,
                    epoch: stored,
                    commit,
                } if *id == producer_id && *stored == epoch => marker = Some((*offset, *commit)),
                _ => {}
            }
        }
        marker
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
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

#[cfg(test)]
fn encode(entry: &Entry) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(32);
    write(&mut bytes, entry)?;
    Ok(bytes)
}

fn write(bytes: &mut Vec<u8>, entry: &Entry) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    match entry {
        Entry::Data {
            offset,
            producer_id,
            epoch,
            id,
        } => {
            bytes.push(KIND_DATA);
            bytes.extend_from_slice(&offset.to_be_bytes());
            bytes.extend_from_slice(&producer_id.to_be_bytes());
            bytes.extend_from_slice(&epoch.to_be_bytes());
            bytes.extend_from_slice(&id.to_be_bytes());
        }
        Entry::Marker {
            offset,
            producer_id,
            epoch,
            commit,
        } => {
            bytes.push(KIND_MARKER);
            bytes.extend_from_slice(&offset.to_be_bytes());
            bytes.extend_from_slice(&producer_id.to_be_bytes());
            bytes.extend_from_slice(&epoch.to_be_bytes());
            bytes.push(u8::from(*commit));
        }
    }
    Ok(())
}

fn decode(mut bytes: &[u8]) -> Result<Entry, Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    let kind = bytes[RECORD_MAGIC.len()];
    bytes = &bytes[RECORD_MAGIC.len() + 1..];
    let offset = take_i64(&mut bytes)?;
    let producer_id = take_i64(&mut bytes)?;
    let epoch = take_i16(&mut bytes)?;
    let entry = match kind {
        KIND_DATA => Entry::Data {
            offset,
            producer_id,
            epoch,
            id: take_u64(&mut bytes)?,
        },
        KIND_MARKER => {
            let flag = take_u8(&mut bytes)?;
            if flag > 1 {
                return Err(Error::InvalidRequest);
            }
            Entry::Marker {
                offset,
                producer_id,
                epoch,
                commit: flag == 1,
            }
        }
        _ => return Err(Error::InvalidRequest),
    };
    if !bytes.is_empty() || offset < 0 || producer_id <= 0 || epoch < 0 {
        return Err(Error::InvalidRequest);
    }
    Ok(entry)
}

fn replay_payload(log: &mut MarkerLog, payload: &[u8]) -> Result<(), Error> {
    let decoded = decode(payload)?;
    let expected = i64::try_from(log.entries.len()).map_err(|_| Error::InvalidRequest)?;
    if entry_offset(&decoded) != expected {
        return Err(Error::InvalidRequest);
    }
    if let Entry::Marker {
        producer_id,
        epoch,
        commit,
        ..
    } = &decoded
    {
        if let Some((_, stored)) = log.trailing_marker(*producer_id, *epoch) {
            return Err(if stored == *commit {
                Error::InvalidRequest
            } else {
                Error::ConflictingMarker
            });
        }
    }
    log.entries.push(decoded);
    Ok(())
}

fn entry_offset(entry: &Entry) -> i64 {
    match entry {
        Entry::Data { offset, .. } | Entry::Marker { offset, .. } => *offset,
    }
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

fn take_u64(input: &mut &[u8]) -> Result<u64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(u64::from_be_bytes(value))
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
        let journal = journal::Limits::new(64, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(32, journal).unwrap_or_else(|_| panic!("marker limits"))
    }

    fn open(name: &str) -> (MarkerLog, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-marker-{name}-{nanos}"));
        let log = MarkerLog::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (log, path)
    }

    #[test]
    fn a_gapped_marker_offset_is_not_recovered() {
        let (mut log, _) = open("gap");
        let entry = Entry::Data {
            offset: 4,
            producer_id: 7,
            epoch: 0,
            id: 1,
        };
        let encoded = encode(&entry).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            replay_payload(&mut log, &encoded),
            Err(Error::InvalidRequest)
        ));
        assert_eq!(log.log_end_offset(), 0);
        log.append_data(7, 0, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.append_marker(7, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let repeated = Entry::Marker {
            offset: log.log_end_offset(),
            producer_id: 7,
            epoch: 0,
            commit: false,
        };
        let encoded = encode(&repeated).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            replay_payload(&mut log, &encoded),
            Err(Error::ConflictingMarker)
        ));
        assert_eq!(log.read_committed(0), vec![1]);
    }

    #[test]
    fn aborted_and_incomplete_records_stay_hidden_and_a_later_commit_is_visible() {
        let (mut log, path) = open("visible");
        log.append_data(7, 0, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let encoded_capacity = log.encoded.capacity();
        assert!(encoded_capacity > 0);
        log.append_data(7, 0, 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(log.encoded.capacity() >= encoded_capacity);
        let abort = log
            .append_marker(7, 0, false)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            log.append_marker(7, 0, false)
                .unwrap_or_else(|error| panic!("{error:?}")),
            abort
        );
        assert!(matches!(
            log.append_marker(7, 0, true),
            Err(Error::ConflictingMarker)
        ));
        log.append_data(7, 1, 3)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.append_marker(7, 1, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.append_data(8, 0, 4)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.last_stable_offset(), log.log_end_offset() - 1);
        assert_eq!(log.read_committed(0), vec![3]);
        drop(log);
        let restored = MarkerLog::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let first = restored.read_committed(0);
        let second = restored.read_committed(0);
        assert_eq!(first, second);
        assert_eq!(first, vec![3]);
        assert!(!first.contains(&1) && !first.contains(&2) && !first.contains(&4));
    }

    #[test]
    fn a_later_transaction_in_the_same_epoch_waits_for_its_own_marker() {
        let (mut log, path) = open("same-epoch");
        log.append_data(7, 0, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let first = log
            .append_marker(7, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            log.append_marker(7, 0, true)
                .unwrap_or_else(|error| panic!("{error:?}")),
            first
        );
        let second_data = log
            .append_data(7, 0, 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.last_stable_offset(), second_data);
        assert_eq!(log.read_committed(0), vec![1]);
        let second = log
            .append_marker(7, 0, true)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_ne!(second, first);
        assert_eq!(log.read_committed(0), vec![1, 2]);
        assert_eq!(
            log.append_marker(7, 0, true)
                .unwrap_or_else(|error| panic!("{error:?}")),
            second
        );
        assert!(matches!(
            log.append_marker(7, 0, false),
            Err(Error::ConflictingMarker)
        ));
        log.append_data(7, 0, 3)
            .unwrap_or_else(|error| panic!("{error:?}"));
        log.append_marker(7, 0, false)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(log.read_committed(0), vec![1, 2]);
        assert_eq!(log.last_stable_offset(), log.log_end_offset());
        drop(log);
        let restored = MarkerLog::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.read_committed(0), vec![1, 2]);
        assert!(!restored.read_committed(0).contains(&3));
    }
}
