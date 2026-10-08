//! Bounded durable partition journal, independent of Kafka batch semantics.
//!
//! This synchronous API performs blocking file I/O and synchronization. Run it
//! on a dedicated storage thread or a bounded blocking executor, never directly
//! on an asynchronous executor. The caller must hold exclusive ownership of the
//! configured file; a process-local guard rejects duplicate live handles, but
//! this foundation does not provide cross-process file locking.
//! Paths are trusted configuration and are never derived from topic names.
//!
//! Each nonempty opaque payload carries an explicit positive record count and
//! first logical offset. Headers protect lengths/offsets with CRC32C; another
//! CRC32C protects payload bytes. A successful append has called `sync_data`.
//! Failed write/sync operations poison the handle without advancing its committed
//! offset. Their outcome is ambiguous: a complete entry may appear on recovery.
//! Only an incomplete final entry is repaired, with a reported, synchronized
//! truncation; complete checksum/offset damage fails closed. CRC is corruption
//! detection, not authentication. No Kafka batch validation, replication,
//! transactional visibility, segment retention or production qualification is
//! implied. File-system/hardware synchronization guarantees still apply.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;

const FILE_MAGIC: &[u8; 8] = b"PLJRNL01";
const ENTRY_MAGIC: &[u8; 8] = b"PLENTRY1";
const FILE_HEADER: usize = 24;
const ENTRY_HEADER: usize = 32;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_FETCH_BYTES: usize = 128 * 1024 * 1024;

/// Validated entry, total-file, retained-index and owned-fetch limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_entry_bytes: usize,
    max_file_bytes: u64,
    max_index_entries: usize,
    max_fetch_bytes: usize,
}

impl Limits {
    /// Configure positive limits: entries up to 64 MiB, files up to 1 TiB,
    /// indexes up to one million entries, and fetch output up to 128 MiB.
    ///
    /// File bytes include all headers; the minimum fits one one-byte entry.
    /// Fetch bytes charge each returned [`Entry`] plus its payload length. Index
    /// and output bounds exclude allocator overhead and caller-retained results.
    pub fn new(
        max_entry_bytes: usize,
        max_file_bytes: u64,
        max_index_entries: usize,
        max_fetch_bytes: usize,
    ) -> Result<Self, Error> {
        if !(1..=MAX_BYTES).contains(&max_entry_bytes)
            || !((FILE_HEADER + ENTRY_HEADER + 1) as u64..=1 << 40).contains(&max_file_bytes)
            || !(1..=1_000_000).contains(&max_index_entries)
            || !(size_of::<Entry>() + 1..=MAX_FETCH_BYTES).contains(&max_fetch_bytes)
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_entry_bytes,
            max_file_bytes,
            max_index_entries,
            max_fetch_bytes,
        })
    }

    /// Maximum opaque payload bytes per entry.
    pub fn max_entry_bytes(self) -> usize {
        self.max_entry_bytes
    }
    /// Maximum complete file bytes, including headers.
    pub fn max_file_bytes(self) -> u64 {
        self.max_file_bytes
    }
    /// Maximum number of retained offset-index entries.
    pub fn max_index_entries(self) -> usize {
        self.max_index_entries
    }
    /// Maximum charged owned output bytes per fetch call.
    pub fn max_fetch_bytes(self) -> usize {
        self.max_fetch_bytes
    }
}

impl Default for Limits {
    /// One MiB entries, one GiB file, 65,536 index entries and 16 MiB fetch output.
    fn default() -> Self {
        Self {
            max_entry_bytes: 1024 * 1024,
            max_file_bytes: 1 << 30,
            max_index_entries: 65_536,
            max_fetch_bytes: 16 * 1024 * 1024,
        }
    }
}

/// The damaged part of a checksummed journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Corruption {
    /// Missing, damaged or unsupported file header.
    FileHeader,
    /// Bad entry magic or header checksum.
    EntryHeader,
    /// Zero or otherwise invalid payload length.
    Length,
    /// Zero logical record count.
    RecordCount,
    /// Nonmonotonic, mismatched or overflowing logical offsets.
    Offset,
    /// A complete payload's checksum differs from the protected header.
    PayloadChecksum,
    /// A valid later entry header occurs inside a supposedly incomplete tail.
    InteriorTail,
}

/// Structured journal failure; no payload or configured path is included.
#[derive(Debug)]
pub enum Error {
    /// Invalid/nonpositive limit configuration.
    InvalidLimits,
    /// Empty payload or zero record count supplied for append.
    InvalidEntry,
    /// An entry exceeds the configured payload bound.
    EntryTooLarge,
    /// An existing or prospective file exceeds its configured byte bound.
    FileBudgetExceeded,
    /// Retained entries would exceed the configured offset-index bound.
    IndexBudgetExceeded,
    /// The first selected entry cannot fit the fetch's charged output budget.
    FetchBudgetExceeded,
    /// A fetch limit is zero or exceeds the journal's configured maximum.
    InvalidFetchLimits,
    /// Advancing a logical offset would overflow `u64`.
    OffsetOverflow,
    /// The persisted base offset differs from the caller's selected partition.
    BaseOffsetMismatch,
    /// Fetch starts before this journal's base offset.
    OffsetBeforeBase,
    /// An I/O or integrity failure requires reopening before further operations.
    Poisoned,
    /// File length changed outside the exclusive journal handle.
    ChangedFile,
    /// Bounded index, tail-inspection or output reservation failed.
    AllocationFailed,
    /// This process already owns a live journal for the same path/file identity.
    AlreadyOpen,
    /// Process-local ownership state became unavailable after a panic.
    OwnershipUnavailable,
    /// Corruption is never converted into a successful recovery.
    Corrupt {
        /// File byte offset of the damaged header/entry.
        byte_offset: u64,
        /// Kind of damage.
        kind: Corruption,
    },
    /// File I/O or synchronization failed.
    Io(io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "journal I/O: {error}"),
            other => write!(f, "journal: {other:?}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Successful recovery, including any durable repair of an incomplete tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovery {
    /// Length observed before recovery or initialization.
    pub original_file_bytes: u64,
    /// Length after initialization/recovery.
    pub recovered_file_bytes: u64,
    /// Number of valid, indexed entries.
    pub recovered_entries: usize,
    /// Bytes removed from a demonstrably incomplete final entry.
    pub truncated_bytes: u64,
    /// Next logical offset after the recovered entries.
    pub next_offset: u64,
    /// Whether an empty file was initialized with a synchronized file header.
    pub initialized: bool,
}

/// Result of an append whose bytes have been synchronized successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Append {
    /// First logical offset assigned to this payload.
    pub first_offset: u64,
    /// Explicit record count persisted with the payload.
    pub record_count: u32,
    /// Next logical offset, using checked addition of the record count.
    pub next_offset: u64,
    /// Complete synchronized file length after this append.
    pub file_bytes: u64,
}

/// An entire opaque stored payload and its logical record range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// First logical record offset in the payload.
    pub first_offset: u64,
    /// Number of logical records declared by the caller.
    pub record_count: u32,
    /// Owned bytes; this foundation does not parse individual records.
    pub payload: Vec<u8>,
}

/// Exclusive, synchronous file-backed partition journal.
pub struct Journal {
    inner: Engine<File>,
    _ownership: ownership::Guard,
}

// The rolled backend shares this parser rather than retaining a dense Journal
// index for every sealed file. These interfaces are intentionally crate-private;
// Journal::open and its recovery/append contract are unchanged.
pub(crate) struct DirectoryOwnership(ownership::Guard);
impl DirectoryOwnership {
    pub(crate) fn acquire(path: &Path) -> Result<Self, Error> {
        Ok(Self(ownership::Guard::acquire(path)?))
    }
    pub(crate) fn identify(&mut self, file: &File) -> Result<(), Error> {
        self.0.identify(file)
    }
}

pub(crate) struct CheckedCursor {
    file: File,
    _ownership: ownership::Guard,
    limits: Limits,
    bytes: u64,
    position: u64,
    next_offset: u64,
    fingerprint: u32,
}
impl CheckedCursor {
    pub(crate) fn open(path: &Path, base: u64, limits: Limits) -> Result<Self, Error> {
        if !std::fs::symlink_metadata(path)?.file_type().is_file() {
            return Err(Error::ChangedFile);
        }
        let mut ownership = ownership::Guard::acquire(path)?;
        let mut file = OpenOptions::new().read(true).open(path)?;
        ownership.identify(&file)?;
        let bytes = file.metadata()?.len();
        if bytes > limits.max_file_bytes {
            return Err(Error::FileBudgetExceeded);
        }
        if bytes < FILE_HEADER as u64 {
            return Err(corrupt(0, Corruption::FileHeader));
        }
        let mut header = [0; FILE_HEADER];
        require_read(&mut file, &mut header)?;
        if &header[..8] != FILE_MAGIC
            || header[16..20] != [0; 4]
            || crc32c::crc32c(&header[..20]) != u32_at(&header, 20)
        {
            return Err(corrupt(0, Corruption::FileHeader));
        }
        if u64_at(&header, 8) != base {
            return Err(Error::BaseOffsetMismatch);
        }
        Ok(Self {
            file,
            _ownership: ownership,
            limits,
            bytes,
            position: FILE_HEADER as u64,
            next_offset: base,
            fingerprint: crc32c::crc32c(&header),
        })
    }
    pub(crate) fn seek(&mut self, position: u64, offset: u64) -> Result<(), Error> {
        if position < FILE_HEADER as u64 || position > self.bytes {
            return Err(corrupt(position, Corruption::Offset));
        }
        self.file.seek(SeekFrom::Start(position))?;
        self.position = position;
        self.next_offset = offset;
        Ok(())
    }
    pub(crate) fn next(&mut self, max_bytes: usize) -> Result<Option<(u64, Entry)>, Error> {
        if self.file.metadata()?.len() != self.bytes {
            return Err(Error::ChangedFile);
        }
        if self.position == self.bytes {
            return Ok(None);
        }
        let position = self.position;
        if self.bytes - position < ENTRY_HEADER as u64 {
            return Err(corrupt(position, Corruption::EntryHeader));
        }
        let mut header = [0; ENTRY_HEADER];
        require_read(&mut self.file, &mut header)?;
        let index = parse_header(&header, self.next_offset, position, self.limits)?;
        let end = position
            .checked_add(ENTRY_HEADER as u64 + u64::from(index.payload_len))
            .ok_or(Error::FileBudgetExceeded)?;
        if end > self.bytes {
            return Err(corrupt(position, Corruption::Length));
        }
        let charged = size_of::<Entry>()
            .checked_add(index.payload_len as usize)
            .ok_or(Error::FetchBudgetExceeded)?;
        if charged > max_bytes || charged > self.limits.max_fetch_bytes {
            return Err(Error::FetchBudgetExceeded);
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(index.payload_len as usize)
            .map_err(|_| Error::AllocationFailed)?;
        payload.resize(index.payload_len as usize, 0);
        require_read(&mut self.file, &mut payload)?;
        if crc32c::crc32c(&payload) != index.checksum {
            return Err(corrupt(position, Corruption::PayloadChecksum));
        }
        if self.file.metadata()?.len() != self.bytes {
            return Err(Error::ChangedFile);
        }
        self.fingerprint = crc32c::crc32c_append(self.fingerprint, &header);
        self.fingerprint = crc32c::crc32c_append(self.fingerprint, &payload);
        self.position = end;
        self.next_offset = index.next_offset;
        Ok(Some((
            position,
            Entry {
                first_offset: index.first_offset,
                record_count: index.record_count,
                payload,
            },
        )))
    }
    pub(crate) fn file_bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn next_offset(&self) -> u64 {
        self.next_offset
    }
    pub(crate) fn fingerprint(&self) -> u32 {
        self.fingerprint
    }
    pub(crate) fn synchronize(&self) -> Result<(), Error> {
        self.file.sync_data()?;
        Ok(())
    }
}

impl Journal {
    /// Open/create a caller-selected path and recover its committed entry index.
    ///
    /// `base_offset` must match the protected file header on reopening. Empty
    /// files get a synchronized header; a damaged/partial file header fails
    /// closed. The parent directory is synchronized before success, so the
    /// configured filesystem must support opening/synchronizing directories.
    /// Only an incomplete final entry can be truncated. Every recovered file is
    /// synchronized before success, including complete entries left by a failed
    /// or unconfirmed prior append; directory synchronization follows that.
    pub fn open(
        path: impl AsRef<Path>,
        base_offset: u64,
        limits: Limits,
    ) -> Result<(Self, Recovery), Error> {
        let path = path.as_ref();
        let mut ownership = ownership::Guard::acquire(path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        ownership.identify(&file)?;
        let (inner, recovery) = Engine::recover(file, base_offset, limits)?;
        let resolved = std::fs::canonicalize(path)?;
        let parent = resolved
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok((
            Self {
                inner,
                _ownership: ownership,
            },
            recovery,
        ))
    }

    /// Append a nonempty opaque payload, synchronize data, then advance offsets.
    ///
    /// A failed write/synchronization poisons this handle. Reopen to establish
    /// whether the failed operation left a complete entry or an incomplete tail.
    pub fn append(&mut self, record_count: u32, payload: &[u8]) -> Result<Append, Error> {
        self.inner.append(record_count, payload)
    }

    /// Fetch bounded whole entries containing/following `first_offset`.
    ///
    /// An offset within an entry returns that whole entry. Offsets at/beyond the
    /// next offset return an empty vector; offsets before the base fail. Output
    /// charges `size_of::<Entry>() + payload.len()` per entry. If the first entry
    /// cannot fit, return an error; if a later entry cannot fit, stop successfully.
    /// I/O/integrity failures poison the handle and return no partial vector.
    pub fn fetch(
        &mut self,
        first_offset: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<Entry>, Error> {
        self.inner.fetch(first_offset, max_entries, max_bytes)
    }

    /// Configured first logical offset.
    pub fn base_offset(&self) -> u64 {
        self.inner.base_offset
    }
    /// Next committed logical offset; failed appends never advance this value.
    pub fn next_offset(&self) -> u64 {
        self.inner.next_offset
    }
    /// Complete synchronized file length retained by this handle.
    pub fn file_bytes(&self) -> u64 {
        self.inner.file_bytes
    }
    /// Number of retained committed offset-index entries.
    pub fn entry_count(&self) -> usize {
        self.inner.index.len()
    }
    pub(crate) fn index_capacity_bytes(&self) -> usize {
        self.inner.index.capacity() * size_of::<Index>()
    }
    /// Whether a failure requires reopening before further append/fetch calls.
    pub fn is_poisoned(&self) -> bool {
        self.inner.poisoned
    }
}

// Private I/O core supports deterministic partial-read/write/sync failures in
// tests. Production durability is always File::sync_data, not an injected API.
trait Storage: Read + Write + Seek {
    fn byte_len(&mut self) -> io::Result<u64>;
    fn truncate_to(&mut self, length: u64) -> io::Result<()>;
    fn synchronize(&mut self) -> io::Result<()>;
}
impl Storage for File {
    fn byte_len(&mut self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
    fn truncate_to(&mut self, length: u64) -> io::Result<()> {
        self.set_len(length)
    }
    fn synchronize(&mut self) -> io::Result<()> {
        self.sync_data()
    }
}

#[derive(Clone, Copy)]
struct Index {
    first_offset: u64,
    next_offset: u64,
    position: u64,
    payload_len: u32,
    record_count: u32,
    checksum: u32,
}
struct Engine<S> {
    storage: S,
    limits: Limits,
    base_offset: u64,
    next_offset: u64,
    file_bytes: u64,
    index: Vec<Index>,
    poisoned: bool,
}

fn corrupt(position: u64, kind: Corruption) -> Error {
    Error::Corrupt {
        byte_offset: position,
        kind,
    }
}
fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}
fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}
fn read_complete(reader: &mut impl Read, output: &mut [u8]) -> io::Result<usize> {
    let mut done = 0;
    while done < output.len() {
        match reader.read(&mut output[done..]) {
            Ok(0) => break,
            Ok(count) => done += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(done)
}
fn require_read(reader: &mut impl Read, output: &mut [u8]) -> io::Result<()> {
    if read_complete(reader, output)? != output.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "journal changed during read",
        ));
    }
    Ok(())
}
fn file_header(base_offset: u64) -> [u8; FILE_HEADER] {
    let mut header = [0; FILE_HEADER];
    header[..8].copy_from_slice(FILE_MAGIC);
    header[8..16].copy_from_slice(&base_offset.to_be_bytes());
    let checksum = crc32c::crc32c(&header[..20]);
    header[20..24].copy_from_slice(&checksum.to_be_bytes());
    header
}
fn entry_header(index: Index) -> [u8; ENTRY_HEADER] {
    let mut header = [0; ENTRY_HEADER];
    header[..8].copy_from_slice(ENTRY_MAGIC);
    header[8..12].copy_from_slice(&index.payload_len.to_be_bytes());
    header[12..20].copy_from_slice(&index.first_offset.to_be_bytes());
    header[20..24].copy_from_slice(&index.record_count.to_be_bytes());
    header[24..28].copy_from_slice(&index.checksum.to_be_bytes());
    let checksum = crc32c::crc32c(&header[..28]);
    header[28..32].copy_from_slice(&checksum.to_be_bytes());
    header
}
fn validate_prefix(
    header: &[u8],
    expected: u64,
    position: u64,
    limits: Limits,
) -> Result<(), Error> {
    let magic_bytes = header.len().min(8);
    if header[..magic_bytes] != ENTRY_MAGIC[..magic_bytes] {
        return Err(corrupt(position, Corruption::EntryHeader));
    }
    if header.len() >= 12 {
        let length = u32_at(header, 8) as usize;
        if length == 0 {
            return Err(corrupt(position, Corruption::Length));
        }
        if length > limits.max_entry_bytes {
            return Err(Error::EntryTooLarge);
        }
    }
    if header.len() >= 20 && u64_at(header, 12) != expected {
        return Err(corrupt(position, Corruption::Offset));
    }
    if header.len() >= 24 {
        let count = u32_at(header, 20);
        if count == 0 {
            return Err(corrupt(position, Corruption::RecordCount));
        }
        if expected.checked_add(u64::from(count)).is_none() {
            return Err(corrupt(position, Corruption::Offset));
        }
    }
    Ok(())
}
fn parse_header(
    header: &[u8; ENTRY_HEADER],
    expected: u64,
    position: u64,
    limits: Limits,
) -> Result<Index, Error> {
    if crc32c::crc32c(&header[..28]) != u32_at(header, 28) {
        return Err(corrupt(position, Corruption::EntryHeader));
    }
    validate_prefix(header, expected, position, limits)?;
    let count = u32_at(header, 20);
    let next_offset = expected
        .checked_add(u64::from(count))
        .ok_or_else(|| corrupt(position, Corruption::Offset))?;
    Ok(Index {
        first_offset: expected,
        next_offset,
        position,
        payload_len: u32_at(header, 8),
        record_count: count,
        checksum: u32_at(header, 24),
    })
}
fn has_later_header(bytes: &[u8], expected_next: u64, limits: Limits) -> bool {
    // Refuse repair conservatively if a later valid header appears within a
    // short tail. Opaque payloads could contain such a header; refusal is safer
    // than deleting a complete later entry after interior byte loss.
    for offset in 0..bytes.len().saturating_sub(ENTRY_HEADER - 1) {
        let candidate = &bytes[offset..offset + ENTRY_HEADER];
        if &candidate[..8] != ENTRY_MAGIC {
            continue;
        }
        let first = u64_at(candidate, 12);
        if first < expected_next {
            continue;
        }
        let mut header = [0; ENTRY_HEADER];
        header.copy_from_slice(candidate);
        if parse_header(&header, first, 0, limits).is_ok() {
            return true;
        }
    }
    false
}

fn reserve_index(index: &mut Vec<Index>, limit: usize) -> Result<(), Error> {
    if index.len() == index.capacity() {
        let capacity = index.capacity().saturating_mul(2).max(1).min(limit);
        index
            .try_reserve_exact(capacity - index.len())
            .map_err(|_| Error::AllocationFailed)?;
    }
    Ok(())
}

mod ownership {
    #![allow(
        clippy::disallowed_types,
        reason = "Short process-local ownership bookkeeping in a synchronous storage API; no lock spans I/O or an await."
    )]
    use super::Error;
    use std::collections::HashSet;
    use std::fs::{self, File};
    use std::io;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    #[derive(Clone, PartialEq, Eq, Hash)]
    enum Key {
        Path(PathBuf),
        #[cfg(unix)]
        Inode(u64, u64),
    }
    static OPEN: OnceLock<Mutex<HashSet<Key>>> = OnceLock::new();
    fn registry() -> &'static Mutex<HashSet<Key>> {
        OPEN.get_or_init(|| Mutex::new(HashSet::new()))
    }

    pub(super) struct Guard {
        keys: Vec<Key>,
    }
    impl Guard {
        pub(super) fn acquire(path: &Path) -> Result<Self, Error> {
            let key = match fs::canonicalize(path) {
                Ok(path) => path,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let parent = path
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or_else(|| Path::new("."));
                    let name = path.file_name().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "journal path has no file name")
                    })?;
                    fs::canonicalize(parent)?.join(name)
                }
                Err(error) => return Err(Error::Io(error)),
            };
            let mut guard = Self { keys: Vec::new() };
            guard
                .keys
                .try_reserve_exact(2)
                .map_err(|_| Error::AllocationFailed)?;
            guard.add(Key::Path(key))?;
            Ok(guard)
        }
        fn add(&mut self, key: Key) -> Result<(), Error> {
            let mut open = registry().lock().map_err(|_| Error::OwnershipUnavailable)?;
            if open.contains(&key) {
                return Err(Error::AlreadyOpen);
            }
            open.try_reserve(1).map_err(|_| Error::AllocationFailed)?;
            open.insert(key.clone());
            self.keys.push(key);
            Ok(())
        }
        pub(super) fn identify(&mut self, file: &File) -> Result<(), Error> {
            #[cfg(unix)]
            {
                let metadata = file.metadata()?;
                self.add(Key::Inode(metadata.dev(), metadata.ino()))?;
            }
            #[cfg(not(unix))]
            {
                let _ = file;
            }
            Ok(())
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Ok(mut open) = registry().lock() {
                for key in &self.keys {
                    open.remove(key);
                }
                if open.is_empty() {
                    open.shrink_to_fit();
                }
            }
        }
    }
}

impl<S: Storage> Engine<S> {
    fn recover(
        mut storage: S,
        base_offset: u64,
        limits: Limits,
    ) -> Result<(Self, Recovery), Error> {
        let original = storage.byte_len()?;
        if original > limits.max_file_bytes {
            return Err(Error::FileBudgetExceeded);
        }
        storage.seek(SeekFrom::Start(0))?;
        let initialized = original == 0;
        if initialized {
            storage.write_all(&file_header(base_offset))?;
        } else {
            if original < FILE_HEADER as u64 {
                return Err(corrupt(0, Corruption::FileHeader));
            }
            let mut header = [0; FILE_HEADER];
            require_read(&mut storage, &mut header)?;
            if &header[..8] != FILE_MAGIC
                || header[16..20] != [0; 4]
                || crc32c::crc32c(&header[..20]) != u32_at(&header, 20)
            {
                return Err(corrupt(0, Corruption::FileHeader));
            }
            if u64_at(&header, 8) != base_offset {
                return Err(Error::BaseOffsetMismatch);
            }
        }
        let observed = if initialized {
            FILE_HEADER as u64
        } else {
            original
        };
        let mut engine = Self {
            storage,
            limits,
            base_offset,
            next_offset: base_offset,
            file_bytes: FILE_HEADER as u64,
            index: Vec::new(),
            poisoned: false,
        };
        let mut truncated = 0;
        while engine.file_bytes < observed {
            let position = engine.file_bytes;
            let available = observed - position;
            let mut header = [0; ENTRY_HEADER];
            let header_bytes = available.min(ENTRY_HEADER as u64) as usize;
            require_read(&mut engine.storage, &mut header[..header_bytes])?;
            if header_bytes < ENTRY_HEADER {
                validate_prefix(
                    &header[..header_bytes],
                    engine.next_offset,
                    position,
                    limits,
                )?;
                truncated = available;
                break;
            }
            let index = parse_header(&header, engine.next_offset, position, limits)?;
            let total = ENTRY_HEADER as u64 + u64::from(index.payload_len);
            if available < total {
                let tail_len = (available - ENTRY_HEADER as u64) as usize;
                let mut tail = Vec::new();
                tail.try_reserve_exact(tail_len)
                    .map_err(|_| Error::AllocationFailed)?;
                tail.resize(tail_len, 0);
                require_read(&mut engine.storage, &mut tail)?;
                if has_later_header(&tail, index.next_offset, limits) {
                    return Err(corrupt(position, Corruption::InteriorTail));
                }
                truncated = available;
                break;
            }
            if engine.index.len() >= limits.max_index_entries {
                return Err(Error::IndexBudgetExceeded);
            }
            let mut scratch = [0; 4096];
            let mut remaining = index.payload_len as usize;
            let mut checksum = 0;
            while remaining > 0 {
                let length = remaining.min(scratch.len());
                require_read(&mut engine.storage, &mut scratch[..length])?;
                checksum = crc32c::crc32c_append(checksum, &scratch[..length]);
                remaining -= length;
            }
            if checksum != index.checksum {
                return Err(corrupt(position, Corruption::PayloadChecksum));
            }
            reserve_index(&mut engine.index, limits.max_index_entries)?;
            engine.index.push(index);
            engine.next_offset = index.next_offset;
            engine.file_bytes = position
                .checked_add(total)
                .ok_or(Error::FileBudgetExceeded)?;
        }
        if engine.storage.byte_len()? != observed {
            return Err(Error::ChangedFile);
        }
        if truncated > 0 {
            engine.storage.truncate_to(engine.file_bytes)?;
        }
        // Complete entries after a failed/uncertain append may still be only in
        // the page cache. Certify all accepted bytes before returning recovery.
        engine.storage.synchronize()?;
        let recovery = Recovery {
            original_file_bytes: original,
            recovered_file_bytes: engine.file_bytes,
            recovered_entries: engine.index.len(),
            truncated_bytes: truncated,
            next_offset: engine.next_offset,
            initialized,
        };
        Ok((engine, recovery))
    }

    fn check_alive(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    fn append(&mut self, record_count: u32, payload: &[u8]) -> Result<Append, Error> {
        self.check_alive()?;
        if record_count == 0 || payload.is_empty() {
            return Err(Error::InvalidEntry);
        }
        if payload.len() > self.limits.max_entry_bytes {
            return Err(Error::EntryTooLarge);
        }
        if self.index.len() >= self.limits.max_index_entries {
            return Err(Error::IndexBudgetExceeded);
        }
        let next_offset = self
            .next_offset
            .checked_add(u64::from(record_count))
            .ok_or(Error::OffsetOverflow)?;
        let total = ENTRY_HEADER as u64 + payload.len() as u64;
        let file_bytes = self
            .file_bytes
            .checked_add(total)
            .ok_or(Error::FileBudgetExceeded)?;
        if file_bytes > self.limits.max_file_bytes {
            return Err(Error::FileBudgetExceeded);
        }
        reserve_index(&mut self.index, self.limits.max_index_entries)?;
        let index = Index {
            first_offset: self.next_offset,
            next_offset,
            position: self.file_bytes,
            payload_len: payload.len() as u32,
            record_count,
            checksum: crc32c::crc32c(payload),
        };
        let result = (|| {
            if self.storage.byte_len()? != self.file_bytes {
                return Err(Error::ChangedFile);
            }
            self.storage.seek(SeekFrom::Start(self.file_bytes))?;
            self.storage.write_all(&entry_header(index))?;
            self.storage.write_all(payload)?;
            self.storage.synchronize()?;
            Ok(())
        })();
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        self.index.push(index);
        self.next_offset = next_offset;
        self.file_bytes = file_bytes;
        Ok(Append {
            first_offset: index.first_offset,
            record_count,
            next_offset,
            file_bytes,
        })
    }

    fn fetch(
        &mut self,
        first_offset: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<Entry>, Error> {
        self.check_alive()?;
        if max_entries == 0
            || max_entries > self.limits.max_index_entries
            || max_bytes == 0
            || max_bytes > self.limits.max_fetch_bytes
        {
            return Err(Error::InvalidFetchLimits);
        }
        if first_offset < self.base_offset {
            return Err(Error::OffsetBeforeBase);
        }
        let start = self
            .index
            .partition_point(|entry| entry.next_offset <= first_offset);
        let mut count = 0usize;
        let mut bytes = 0usize;
        for index in self.index.iter().skip(start).take(max_entries) {
            let charge = size_of::<Entry>()
                .checked_add(index.payload_len as usize)
                .ok_or(Error::FetchBudgetExceeded)?;
            let next = bytes
                .checked_add(charge)
                .ok_or(Error::FetchBudgetExceeded)?;
            if next > max_bytes {
                if count == 0 {
                    return Err(Error::FetchBudgetExceeded);
                }
                break;
            }
            bytes = next;
            count += 1;
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| Error::AllocationFailed)?;
        let result = (|| {
            if self.storage.byte_len()? != self.file_bytes {
                return Err(Error::ChangedFile);
            }
            for position in start..start + count {
                let index = self.index[position];
                self.storage.seek(SeekFrom::Start(index.position))?;
                let mut header = [0; ENTRY_HEADER];
                require_read(&mut self.storage, &mut header)?;
                let parsed =
                    parse_header(&header, index.first_offset, index.position, self.limits)?;
                if parsed.payload_len != index.payload_len
                    || parsed.record_count != index.record_count
                    || parsed.checksum != index.checksum
                {
                    return Err(corrupt(index.position, Corruption::EntryHeader));
                }
                let mut payload = Vec::new();
                payload
                    .try_reserve_exact(index.payload_len as usize)
                    .map_err(|_| Error::AllocationFailed)?;
                payload.resize(index.payload_len as usize, 0);
                require_read(&mut self.storage, &mut payload)?;
                if crc32c::crc32c(&payload) != index.checksum {
                    return Err(corrupt(index.position, Corruption::PayloadChecksum));
                }
                entries.push(Entry {
                    first_offset: index.first_offset,
                    record_count: index.record_count,
                    payload,
                });
            }
            if self.storage.byte_len()? != self.file_bytes {
                return Err(Error::ChangedFile);
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::io::Cursor;
    use std::rc::Rc;

    struct Faults {
        cursor: Cursor<Vec<u8>>,
        read_chunk: usize,
        interruptions: usize,
        read_budget: Option<usize>,
        write_budget: Option<usize>,
        zero_write: bool,
        fail_sync: bool,
        fail_truncate: bool,
        sync_attempts: Rc<Cell<usize>>,
        truncations: Rc<Cell<usize>>,
        synchronized: Vec<Vec<u8>>,
    }
    impl Faults {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                cursor: Cursor::new(bytes),
                read_chunk: usize::MAX,
                interruptions: 0,
                read_budget: None,
                write_budget: None,
                zero_write: false,
                fail_sync: false,
                fail_truncate: false,
                sync_attempts: Rc::new(Cell::new(0)),
                truncations: Rc::new(Cell::new(0)),
                synchronized: Vec::new(),
            }
        }
    }
    impl Read for Faults {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            if self.interruptions > 0 {
                self.interruptions -= 1;
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            let limit = output
                .len()
                .min(self.read_chunk)
                .min(self.read_budget.unwrap_or(usize::MAX));
            let read = self.cursor.read(&mut output[..limit])?;
            if let Some(budget) = &mut self.read_budget {
                *budget -= read;
            }
            Ok(read)
        }
    }
    impl Write for Faults {
        fn write(&mut self, input: &[u8]) -> io::Result<usize> {
            if self.zero_write {
                return Ok(0);
            }
            if self.write_budget == Some(0) {
                return Err(io::Error::other("injected write failure"));
            }
            let length = input.len().min(self.write_budget.unwrap_or(usize::MAX));
            let written = self.cursor.write(&input[..length])?;
            if let Some(budget) = &mut self.write_budget {
                *budget -= written;
            }
            Ok(written)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl Seek for Faults {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            self.cursor.seek(position)
        }
    }
    impl Storage for Faults {
        fn byte_len(&mut self) -> io::Result<u64> {
            Ok(self.cursor.get_ref().len() as u64)
        }
        fn truncate_to(&mut self, length: u64) -> io::Result<()> {
            self.truncations.set(self.truncations.get() + 1);
            if self.fail_truncate {
                return Err(io::Error::other("injected truncate failure"));
            }
            self.cursor.get_mut().truncate(length as usize);
            Ok(())
        }
        fn synchronize(&mut self) -> io::Result<()> {
            self.sync_attempts.set(self.sync_attempts.get() + 1);
            if self.fail_sync {
                return Err(io::Error::other("injected sync failure"));
            }
            self.synchronized.push(self.cursor.get_ref().clone());
            Ok(())
        }
    }
    fn engine(base: u64) -> Result<Engine<Faults>, Error> {
        Ok(Engine::recover(Faults::new(Vec::new()), base, Limits::default())?.0)
    }
    fn persisted() -> Result<Vec<u8>, Error> {
        let mut journal = engine(7)?;
        journal.append(3, &vec![b'a'; 8201])?;
        journal.append(2, b"next")?;
        Ok(journal.storage.cursor.into_inner())
    }

    #[test]
    fn success_synchronizes_complete_bytes_before_return() {
        let mut journal = engine(7).unwrap();
        assert_eq!(journal.storage.synchronized.len(), 1);
        let result = journal.append(3, b"durable").unwrap();
        assert_eq!(result.first_offset, 7);
        assert_eq!(result.next_offset, 10);
        assert_eq!(journal.storage.synchronized.len(), 2);
        assert_eq!(
            journal.storage.synchronized[1],
            *journal.storage.cursor.get_ref()
        );
        assert_eq!(journal.index.len(), 1);
    }

    #[test]
    fn partial_header_and_payload_writes_poison_without_advancing() {
        for budget in [1, 7, 20, 31, ENTRY_HEADER, ENTRY_HEADER + 2] {
            let mut journal = engine(7).unwrap();
            journal.storage.write_budget = Some(budget);
            assert!(matches!(journal.append(3, b"partial"), Err(Error::Io(_))));
            assert!(journal.poisoned);
            assert_eq!(journal.next_offset, 7);
            assert_eq!(journal.file_bytes, FILE_HEADER as u64);
            assert!(journal.index.is_empty());
            assert_eq!(journal.storage.sync_attempts.get(), 1);
            assert!(matches!(
                journal.append(1, b"blocked"),
                Err(Error::Poisoned)
            ));
            assert!(matches!(journal.fetch(7, 1, 1024), Err(Error::Poisoned)));
            journal.storage.write_budget = None;
            let (mut recovered, outcome) =
                Engine::recover(journal.storage, 7, Limits::default()).unwrap();
            assert_eq!(outcome.truncated_bytes, budget as u64);
            assert_eq!(outcome.next_offset, 7);
            assert_eq!(recovered.append(1, b"retry").unwrap().first_offset, 7);
        }
    }

    #[test]
    fn zero_write_and_sync_failure_poison_with_ambiguous_recovery() {
        let mut journal = engine(0).unwrap();
        journal.storage.zero_write = true;
        assert!(
            matches!(journal.append(1, b"x"), Err(Error::Io(ref e)) if e.kind() == io::ErrorKind::WriteZero)
        );
        assert!(journal.poisoned);
        assert_eq!(journal.next_offset, 0);
        let mut journal = engine(0).unwrap();
        journal.storage.fail_sync = true;
        assert!(matches!(
            journal.append(2, b"written but unconfirmed"),
            Err(Error::Io(_))
        ));
        assert_eq!(journal.next_offset, 0);
        assert_eq!(journal.storage.synchronized.len(), 1);
        assert!(journal.poisoned);
        journal.storage.fail_sync = false;
        let (recovered, outcome) = Engine::recover(journal.storage, 0, Limits::default()).unwrap();
        assert_eq!(outcome.truncated_bytes, 0);
        assert_eq!(recovered.next_offset, 2);
        assert_eq!(recovered.index.len(), 1);
    }

    #[test]
    fn recovery_loops_through_partial_reads_and_interrupted_calls() {
        let mut storage = Faults::new(persisted().unwrap());
        storage.read_chunk = 3;
        storage.interruptions = 4;
        let (mut journal, outcome) = Engine::recover(storage, 7, Limits::default()).unwrap();
        assert_eq!(outcome.recovered_entries, 2);
        assert_eq!(outcome.next_offset, 12);
        assert_eq!(outcome.truncated_bytes, 0);
        let entries = journal.fetch(8, 2, 16 * 1024).unwrap();
        assert_eq!(entries[0].payload, vec![b'a'; 8201]);
        assert_eq!(entries[1].payload, b"next");
    }

    #[test]
    fn early_zero_read_does_not_become_a_successful_tail_repair() {
        let mut storage = Faults::new(persisted().unwrap());
        storage.read_budget = Some(FILE_HEADER + ENTRY_HEADER + 10);
        let truncations = storage.truncations.clone();
        assert!(
            matches!(Engine::recover(storage, 7, Limits::default()), Err(Error::Io(ref error)) if error.kind() == io::ErrorKind::UnexpectedEof)
        );
        assert_eq!(truncations.get(), 0);
    }

    #[test]
    fn tail_repair_truncate_and_sync_errors_never_report_success() {
        for fail_sync in [false, true] {
            let mut bytes = file_header(0).to_vec();
            bytes.extend_from_slice(&ENTRY_MAGIC[..3]);
            let mut storage = Faults::new(bytes);
            storage.fail_truncate = !fail_sync;
            storage.fail_sync = fail_sync;
            let truncations = storage.truncations.clone();
            let syncs = storage.sync_attempts.clone();
            assert!(matches!(
                Engine::recover(storage, 0, Limits::default()),
                Err(Error::Io(_))
            ));
            assert_eq!(truncations.get(), 1);
            assert_eq!(syncs.get(), usize::from(fail_sync));
        }
    }

    #[test]
    fn complete_recovery_sync_failure_never_reports_durable_state() {
        let mut storage = Faults::new(persisted().unwrap());
        storage.fail_sync = true;
        let syncs = storage.sync_attempts.clone();
        let truncations = storage.truncations.clone();
        assert!(matches!(
            Engine::recover(storage, 7, Limits::default()),
            Err(Error::Io(_))
        ));
        assert_eq!(syncs.get(), 1);
        assert_eq!(truncations.get(), 0);
    }

    #[test]
    fn valid_later_header_prevents_repair_after_interior_byte_loss() {
        let mut journal = engine(0).unwrap();
        journal.append(3, &[b'a'; 100]).unwrap();
        journal.append(2, b"later").unwrap();
        let bytes = journal.storage.cursor.into_inner();
        let mut damaged = bytes[..FILE_HEADER + ENTRY_HEADER + 2].to_vec();
        damaged.extend_from_slice(&bytes[FILE_HEADER + ENTRY_HEADER + 100..]);
        let storage = Faults::new(damaged);
        let truncations = storage.truncations.clone();
        assert!(matches!(
            Engine::recover(storage, 0, Limits::default()),
            Err(Error::Corrupt {
                kind: Corruption::InteriorTail,
                ..
            })
        ));
        assert_eq!(truncations.get(), 0);
    }

    #[test]
    fn header_length_bomb_fails_before_tail_allocation_or_repair() {
        let mut bytes = file_header(0).to_vec();
        bytes.extend_from_slice(&entry_header(Index {
            first_offset: 0,
            next_offset: 1,
            position: FILE_HEADER as u64,
            payload_len: u32::MAX,
            record_count: 1,
            checksum: 0,
        }));
        let storage = Faults::new(bytes);
        let truncations = storage.truncations.clone();
        assert!(matches!(
            Engine::recover(storage, 0, Limits::default()),
            Err(Error::EntryTooLarge)
        ));
        assert_eq!(truncations.get(), 0);
    }

    #[test]
    fn bounded_index_capacity_and_rejections_never_write() {
        let limits = Limits::new(8, 4096, 7, 1024).unwrap();
        let (mut journal, _) = Engine::recover(Faults::new(Vec::new()), 0, limits).unwrap();
        for _ in 0..7 {
            journal.append(1, b"x").unwrap();
            assert!(journal.index.capacity() <= 7);
        }
        let bytes = journal.storage.cursor.get_ref().clone();
        assert!(matches!(
            journal.append(1, b"x"),
            Err(Error::IndexBudgetExceeded)
        ));
        assert_eq!(*journal.storage.cursor.get_ref(), bytes);
        assert_eq!(journal.storage.sync_attempts.get(), 8);
        assert!(!journal.poisoned);
    }

    #[test]
    fn corrupted_file_header_and_recovery_offset_overflow_fail_closed() {
        for length in 1..FILE_HEADER {
            assert!(matches!(
                Engine::recover(
                    Faults::new(file_header(0)[..length].to_vec()),
                    0,
                    Limits::default()
                ),
                Err(Error::Corrupt {
                    kind: Corruption::FileHeader,
                    ..
                })
            ));
        }
        let mut bytes = file_header(u64::MAX).to_vec();
        bytes.extend_from_slice(&entry_header(Index {
            first_offset: u64::MAX,
            next_offset: 0,
            position: FILE_HEADER as u64,
            payload_len: 1,
            record_count: 1,
            checksum: crc32c::crc32c(b"x"),
        }));
        bytes.push(b'x');
        assert!(matches!(
            Engine::recover(Faults::new(bytes), u64::MAX, Limits::default()),
            Err(Error::Corrupt {
                kind: Corruption::Offset,
                ..
            })
        ));
    }
}
