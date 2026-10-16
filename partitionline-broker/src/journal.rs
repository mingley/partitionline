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
use std::io::{self, IoSlice, Read, Seek, SeekFrom, Write};
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

/// One fetched payload already appended to the caller's buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PayloadView {
    /// First logical record offset in the payload.
    pub first_offset: u64,
    /// Number of logical records declared by the caller.
    pub record_count: u32,
    /// Payload bytes appended to the destination.
    pub len: usize,
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
    /// The file length matched `bytes` for this cursor.
    length_ok: bool,
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
            length_ok: false,
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
    pub(crate) fn next_into(
        &mut self,
        max_bytes: usize,
        dest: &mut Vec<u8>,
    ) -> Result<Option<(u64, PayloadView)>, Error> {
        if !self.length_ok {
            if self.file.metadata()?.len() != self.bytes {
                return Err(Error::ChangedFile);
            }
            self.length_ok = true;
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
        let mark = dest.len();
        if let Err(error) = read_exact_into(&mut self.file, dest, index.payload_len as usize) {
            dest.truncate(mark);
            return Err(error);
        }
        let Some(payload_crc) = dest.get(mark..).map(crc32c::crc32c) else {
            dest.truncate(mark);
            return Err(corrupt(position, Corruption::PayloadChecksum));
        };
        if payload_crc != index.checksum {
            dest.truncate(mark);
            return Err(corrupt(position, Corruption::PayloadChecksum));
        }
        let payload_len = dest.len().saturating_sub(mark);
        self.fingerprint = extend_fingerprint(self.fingerprint, &header, payload_crc, payload_len);
        self.position = end;
        self.next_offset = index.next_offset;
        Ok(Some((
            position,
            PayloadView {
                first_offset: index.first_offset,
                record_count: index.record_count,
                len: index.payload_len as usize,
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

    /// Append using a caller-supplied payload checksum.
    ///
    /// [`checksum_reusing_record_crc`] builds that value from a validated
    /// single-batch record CRC plus the 21-byte prefix, skipping a second
    /// scan of the records.
    pub(crate) fn append_with_checksum(
        &mut self,
        record_count: u32,
        payload: &[u8],
        checksum: u32,
    ) -> Result<Append, Error> {
        self.inner
            .append_checksummed(record_count, payload, checksum)
    }

    /// Write one entry and return a cloned descriptor for a later `sync_data`.
    ///
    /// The offset index advances only in [`Self::commit_staged`], after that
    /// sync succeeds. [`Self::abort_staged`] poisons the handle instead.
    /// A crash after the write and before commit recovers the entry on reopen,
    /// the same window as a crash during [`Self::append`]'s sync.
    /// A second call while an entry is already staged poisons the handle.
    /// [`Self::append_staged_grouped`] is the only way to queue another entry
    /// behind the first and cover both with one sync.
    pub(crate) fn append_staged(
        &mut self,
        record_count: u32,
        payload: &[u8],
        checksum: u32,
    ) -> Result<File, Error> {
        self.append_staged_inner(record_count, payload, checksum, false)
    }

    /// Queue another entry behind one already staged on this handle.
    ///
    /// Writes stay in order. One `sync_data` on the last cloned descriptor
    /// covers every earlier write. [`Self::commit_staged`] must then run once
    /// per entry, oldest first. A crash before that sync recovers whatever
    /// complete entries reached disk, the same window as one staged append.
    pub(crate) fn append_staged_grouped(
        &mut self,
        record_count: u32,
        payload: &[u8],
        checksum: u32,
    ) -> Result<File, Error> {
        self.append_staged_inner(record_count, payload, checksum, true)
    }

    fn append_staged_inner(
        &mut self,
        record_count: u32,
        payload: &[u8],
        checksum: u32,
        extend: bool,
    ) -> Result<File, Error> {
        self.inner.stage(record_count, payload, checksum, extend)?;
        match self.inner.storage.try_clone() {
            Ok(file) => Ok(file),
            Err(error) => {
                self.inner.abort_staged();
                Err(error.into())
            }
        }
    }

    /// Publish the staged entry after its cloned descriptor has been synchronized.
    pub(crate) fn commit_staged(&mut self) -> Result<Append, Error> {
        self.inner.commit_staged()
    }

    /// Drop the newest staged entry and truncate the file back to its start.
    ///
    /// Returns that entry's first offset. Used when a later grouped produce
    /// is rejected before sync. Earlier staged entries stay queued. A
    /// truncate failure poisons the handle.
    pub(crate) fn unstage_last(&mut self) -> Result<u64, Error> {
        self.inner.unstage_last()
    }

    /// Drop a staged entry from memory and require reopen.
    pub(crate) fn abort_staged(&mut self) {
        self.inner.abort_staged();
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

    /// Append the one entry containing `first_offset` onto `dest`.
    ///
    /// The charge and checksum rules match [`Self::fetch`] of a single entry.
    /// On failure `dest` is restored to its incoming length. An offset at or
    /// beyond the next offset returns [`None`] and leaves `dest` unchanged.
    /// The next read stats the file again.
    ///
    /// Fetch checks the length once per request, then reuses that result for
    /// every later entry until an append or a failed read.
    pub(crate) fn invalidate_length_observation(&mut self) {
        self.inner.length_ok = false;
    }

    pub(crate) fn fetch_one_into(
        &mut self,
        first_offset: u64,
        max_bytes: usize,
        dest: &mut Vec<u8>,
    ) -> Result<Option<PayloadView>, Error> {
        self.inner.fetch_one_into(first_offset, max_bytes, dest)
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
struct Staged {
    index: Index,
    file_bytes: u64,
}
struct Engine<S> {
    storage: S,
    limits: Limits,
    base_offset: u64,
    next_offset: u64,
    file_bytes: u64,
    index: Vec<Index>,
    poisoned: bool,
    staged: Vec<Staged>,
    /// File position after the last successful read or append. The next
    /// contiguous read skips `seek`.
    read_pos: Option<u64>,
    /// `byte_len` matched `file_bytes` since the last mutation or explicit
    /// invalidation. Later reads in that interval skip the stat.
    length_ok: bool,
}

/// Running file fingerprint after `header` and a payload whose CRC is known.
///
/// `payload_crc` is `crc32c` of that payload and `payload_len` is its length.
/// The result matches hashing the payload a second time.
pub(crate) fn extend_fingerprint(
    fingerprint: u32,
    header: &[u8],
    payload_crc: u32,
    payload_len: usize,
) -> u32 {
    let with_header = crc32c::crc32c_append(fingerprint, header);
    crc32c::crc32c_combine(with_header, payload_crc, payload_len)
}

/// Journal checksum of `payload`.
///
/// A single Kafka record batch stores its CRC32-C at bytes 17..21 over the
/// suffix starting at byte 21. That suffix is outside the base-offset and
/// leader-epoch fields, so a validated batch can reuse it. Several batches
/// reuse each stored CRC when their lengths tile the payload.
pub(crate) fn checksum_reusing_record_crc(payload: &[u8], batches: usize) -> u32 {
    if batches == 1 && payload.len() >= 21 {
        let suffix = u32::from_be_bytes([payload[17], payload[18], payload[19], payload[20]]);
        return crc32c::crc32c_combine(crc32c::crc32c(&payload[..21]), suffix, payload.len() - 21);
    }
    if batches > 1 {
        if let Some(sum) = checksum_from_batch_crcs(payload, batches) {
            return sum;
        }
    }
    crc32c::crc32c(payload)
}

/// Journal checksum from each batch CRC when `payload` is exactly `batches` batches.
///
/// Each batch length must cover that batch, and the stored CRC must sit at
/// bytes 17..21. A shape that does not match returns `None` so the caller
/// hashes the bytes directly.
fn checksum_from_batch_crcs(payload: &[u8], batches: usize) -> Option<u32> {
    let mut fingerprint = 0u32;
    let mut offset = 0usize;
    for _ in 0..batches {
        let header = payload.get(offset..offset.checked_add(21)?)?;
        let length_bytes: [u8; 4] = header.get(8..12)?.try_into().ok()?;
        let batch_len = usize::try_from(i32::from_be_bytes(length_bytes))
            .ok()?
            .checked_add(12)?;
        if batch_len < 21 {
            return None;
        }
        let end = offset.checked_add(batch_len)?;
        if end > payload.len() {
            return None;
        }
        let crc_bytes: [u8; 4] = header.get(17..21)?.try_into().ok()?;
        let suffix_len = batch_len.checked_sub(21)?;
        fingerprint = extend_fingerprint(
            fingerprint,
            header,
            u32::from_be_bytes(crc_bytes),
            suffix_len,
        );
        offset = end;
    }
    if offset == payload.len() {
        Some(fingerprint)
    } else {
        None
    }
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

/// Read exactly `len` bytes without zero-filling them first.
///
/// `Read::read_to_end` fills reserved capacity directly. A short read is the
/// same interrupted-tail failure as [`require_read`].
fn read_exact_into(reader: &mut impl Read, dest: &mut Vec<u8>, len: usize) -> Result<(), Error> {
    let start = dest.len();
    // Geometric growth: a fetch appends many entries into one buffer.
    dest.try_reserve(len).map_err(|_| Error::AllocationFailed)?;
    let limit = u64::try_from(len).map_err(|_| Error::AllocationFailed)?;
    let result = reader.take(limit).read_to_end(dest);
    let complete = matches!(result, Ok(n) if n == len) && dest.len() == start.saturating_add(len);
    if complete {
        return Ok(());
    }
    dest.truncate(start);
    match result {
        Ok(_) => {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "journal changed during read").into())
        }
        Err(error) => Err(error.into()),
    }
}

fn file_header(base_offset: u64) -> [u8; FILE_HEADER] {
    let mut header = [0; FILE_HEADER];
    header[..8].copy_from_slice(FILE_MAGIC);
    header[8..16].copy_from_slice(&base_offset.to_be_bytes());
    let checksum = crc32c::crc32c(&header[..20]);
    header[20..24].copy_from_slice(&checksum.to_be_bytes());
    header
}
/// Write `first` then `second` with one vectored transfer.
pub(crate) fn write_two<W: Write>(w: &mut W, first: &[u8], second: &[u8]) -> io::Result<()> {
    let mut slices = [IoSlice::new(first), IoSlice::new(second)];
    let mut bufs: &mut [IoSlice<'_>] = &mut slices;
    while !bufs.is_empty() {
        match w.write_vectored(bufs) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "journal write returned 0",
                ));
            }
            Ok(n) => IoSlice::advance_slices(&mut bufs, n),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
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
            staged: Vec::new(),
            read_pos: None,
            length_ok: false,
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
                read_exact_into(&mut engine.storage, &mut tail, tail_len)?;
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
    fn plan(
        &mut self,
        record_count: u32,
        payload: &[u8],
        checksum: u32,
        extend: bool,
    ) -> Result<(Index, u64), Error> {
        self.check_alive()?;
        if !self.staged.is_empty() && !extend {
            self.poisoned = true;
            return Err(Error::Poisoned);
        }
        if record_count == 0 || payload.is_empty() {
            return Err(Error::InvalidEntry);
        }
        if payload.len() > self.limits.max_entry_bytes {
            return Err(Error::EntryTooLarge);
        }
        if self.index.len().saturating_add(self.staged.len()) >= self.limits.max_index_entries {
            return Err(Error::IndexBudgetExceeded);
        }
        let (origin_offset, origin_bytes) = match self.staged.last() {
            Some(staged) => (staged.index.next_offset, staged.file_bytes),
            None => (self.next_offset, self.file_bytes),
        };
        let next_offset = origin_offset
            .checked_add(u64::from(record_count))
            .ok_or(Error::OffsetOverflow)?;
        let total = ENTRY_HEADER as u64 + payload.len() as u64;
        let file_bytes = origin_bytes
            .checked_add(total)
            .ok_or(Error::FileBudgetExceeded)?;
        if file_bytes > self.limits.max_file_bytes {
            return Err(Error::FileBudgetExceeded);
        }
        reserve_index(&mut self.index, self.limits.max_index_entries)?;
        let index = Index {
            first_offset: origin_offset,
            next_offset,
            position: origin_bytes,
            payload_len: payload.len() as u32,
            record_count,
            checksum,
        };
        Ok((index, file_bytes))
    }
    fn ensure_length(&mut self) -> Result<(), Error> {
        if self.length_ok {
            return Ok(());
        }
        if self.storage.byte_len()? != self.file_bytes {
            return Err(Error::ChangedFile);
        }
        self.length_ok = true;
        Ok(())
    }
    fn write_planned(&mut self, index: Index, payload: &[u8]) -> Result<(), Error> {
        // `index.position` is the committed end, or the end of an earlier
        // grouped write that is not published yet. Either way it is the
        // length this handle last left on disk.
        if self.storage.byte_len()? != index.position {
            return Err(Error::ChangedFile);
        }
        self.read_pos = None;
        self.length_ok = false;
        self.storage.seek(SeekFrom::Start(index.position))?;
        let header = entry_header(index);
        write_two(&mut self.storage, &header, payload)?;
        // `file_bytes` stays at the last commit while grouped writes extend
        // the staged tail. The cursor has to follow this write, or the next
        // read of a later entry skips the seek and reads past EOF.
        self.read_pos = index
            .position
            .checked_add(ENTRY_HEADER as u64)
            .and_then(|n| n.checked_add(payload.len() as u64));
        Ok(())
    }
    fn publish(&mut self, index: Index, file_bytes: u64) -> Append {
        let appended = Append {
            first_offset: index.first_offset,
            record_count: index.record_count,
            next_offset: index.next_offset,
            file_bytes,
        };
        self.index.push(index);
        self.next_offset = index.next_offset;
        self.file_bytes = file_bytes;
        appended
    }
    fn append_checksummed(
        &mut self,
        record_count: u32,
        payload: &[u8],
        checksum: u32,
    ) -> Result<Append, Error> {
        let (index, file_bytes) = self.plan(record_count, payload, checksum, false)?;
        let result = (|| {
            self.write_planned(index, payload)?;
            self.storage.synchronize()?;
            Ok(())
        })();
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        Ok(self.publish(index, file_bytes))
    }
    fn append(&mut self, record_count: u32, payload: &[u8]) -> Result<Append, Error> {
        self.append_checksummed(record_count, payload, crc32c::crc32c(payload))
    }
    fn stage(
        &mut self,
        record_count: u32,
        payload: &[u8],
        checksum: u32,
        extend: bool,
    ) -> Result<(), Error> {
        let (index, file_bytes) = self.plan(record_count, payload, checksum, extend)?;
        if let Err(error) = self.write_planned(index, payload) {
            self.poisoned = true;
            return Err(error);
        }
        self.staged.push(Staged { index, file_bytes });
        Ok(())
    }
    fn commit_staged(&mut self) -> Result<Append, Error> {
        if self.staged.is_empty() {
            self.poisoned = true;
            return Err(Error::Poisoned);
        }
        let staged = self.staged.remove(0);
        Ok(self.publish(staged.index, staged.file_bytes))
    }
    fn abort_staged(&mut self) {
        if !self.staged.is_empty() {
            self.staged.clear();
            self.poisoned = true;
        }
    }
    fn unstage_last(&mut self) -> Result<u64, Error> {
        let Some(staged) = self.staged.pop() else {
            self.poisoned = true;
            return Err(Error::Poisoned);
        };
        if let Err(error) = self.storage.truncate_to(staged.index.position) {
            self.poisoned = true;
            return Err(error.into());
        }
        self.read_pos = None;
        self.length_ok = false;
        Ok(staged.index.first_offset)
    }

    fn fetch(
        &mut self,
        first_offset: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<Entry>, Error> {
        if !self.staged.is_empty() {
            self.abort_staged();
            return Err(Error::Poisoned);
        }
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
                let mut payload = Vec::new();
                self.read_indexed(&index, &mut payload)?;
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

    fn fetch_one_into(
        &mut self,
        first_offset: u64,
        max_bytes: usize,
        dest: &mut Vec<u8>,
    ) -> Result<Option<PayloadView>, Error> {
        if !self.staged.is_empty() {
            self.abort_staged();
            return Err(Error::Poisoned);
        }
        self.check_alive()?;
        if max_bytes == 0 || max_bytes > self.limits.max_fetch_bytes {
            return Err(Error::InvalidFetchLimits);
        }
        if first_offset < self.base_offset {
            return Err(Error::OffsetBeforeBase);
        }
        let start = self
            .index
            .partition_point(|entry| entry.next_offset <= first_offset);
        let Some(index) = self.index.get(start).copied() else {
            return Ok(None);
        };
        let charge = size_of::<Entry>()
            .checked_add(index.payload_len as usize)
            .ok_or(Error::FetchBudgetExceeded)?;
        if charge > max_bytes {
            return Err(Error::FetchBudgetExceeded);
        }
        let mark = dest.len();
        let result = (|| {
            self.ensure_length()?;
            self.read_indexed(&index, dest)?;
            Ok(())
        })();
        if let Err(error) = result {
            dest.truncate(mark);
            self.length_ok = false;
            self.poisoned = true;
            return Err(error);
        }
        Ok(Some(PayloadView {
            first_offset: index.first_offset,
            record_count: index.record_count,
            len: index.payload_len as usize,
        }))
    }

    fn read_indexed(&mut self, index: &Index, dest: &mut Vec<u8>) -> Result<(), Error> {
        let mark = dest.len();
        let result = (|| {
            if self.read_pos != Some(index.position) {
                self.storage.seek(SeekFrom::Start(index.position))?;
                self.read_pos = Some(index.position);
            }
            let mut header = [0; ENTRY_HEADER];
            require_read(&mut self.storage, &mut header)?;
            let parsed = parse_header(&header, index.first_offset, index.position, self.limits)?;
            if parsed.payload_len != index.payload_len
                || parsed.record_count != index.record_count
                || parsed.checksum != index.checksum
            {
                return Err(corrupt(index.position, Corruption::EntryHeader));
            }
            read_exact_into(&mut self.storage, dest, index.payload_len as usize)?;
            let payload = dest
                .get(mark..)
                .ok_or_else(|| corrupt(index.position, Corruption::PayloadChecksum))?;
            if crc32c::crc32c(payload) != index.checksum {
                return Err(corrupt(index.position, Corruption::PayloadChecksum));
            }
            self.read_pos = index
                .position
                .checked_add(ENTRY_HEADER as u64)
                .and_then(|n| n.checked_add(u64::from(index.payload_len)));
            Ok(())
        })();
        if result.is_err() {
            dest.truncate(mark);
            self.read_pos = None;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::io::Cursor;
    use std::rc::Rc;

    #[test]
    fn a_known_payload_crc_extends_the_fingerprint() {
        let header = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let payload = b"records-in-the-log-entry";
        let start = 0xabcd_u32;
        let direct = crc32c::crc32c_append(crc32c::crc32c_append(start, &header), payload);
        let combined = extend_fingerprint(start, &header, crc32c::crc32c(payload), payload.len());
        assert_eq!(combined, direct);
        let empty = extend_fingerprint(start, &header, crc32c::crc32c(&[]), 0);
        assert_eq!(empty, crc32c::crc32c_append(start, &header));
    }

    #[test]
    fn repeated_reads_append_without_dropping_earlier_bytes() {
        let mut src = Cursor::new(vec![7u8; 24]);
        let mut dest = Vec::new();
        read_exact_into(&mut src, &mut dest, 8).unwrap();
        read_exact_into(&mut src, &mut dest, 8).unwrap();
        read_exact_into(&mut src, &mut dest, 8).unwrap();
        assert_eq!(dest, vec![7u8; 24]);
    }

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

    #[test]
    fn record_crc_plus_prefix_matches_the_payload_checksum() {
        let mut payload = vec![0u8; 21 + 4_096];
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte = (i as u8).wrapping_mul(17).wrapping_add(3);
        }
        let suffix = crc32c::crc32c(&payload[21..]);
        payload[17..21].copy_from_slice(&suffix.to_be_bytes());
        assert_eq!(
            checksum_reusing_record_crc(&payload, 1),
            crc32c::crc32c(&payload)
        );
        assert_eq!(
            checksum_reusing_record_crc(&payload, 2),
            crc32c::crc32c(&payload)
        );
        assert_eq!(
            checksum_reusing_record_crc(b"short", 1),
            crc32c::crc32c(b"short")
        );
        let mut first = batch_with_suffix(b"alpha-batch");
        let second = batch_with_suffix(b"beta-batch-bytes");
        first.extend_from_slice(&second);
        assert_eq!(
            checksum_reusing_record_crc(&first, 2),
            crc32c::crc32c(&first)
        );
    }

    fn batch_with_suffix(suffix: &[u8]) -> Vec<u8> {
        let mut payload = vec![0u8; 21 + suffix.len()];
        payload[21..].copy_from_slice(suffix);
        let batch_len = i32::try_from(payload.len() - 12).expect("batch length");
        payload[8..12].copy_from_slice(&batch_len.to_be_bytes());
        let crc = crc32c::crc32c(&payload[21..]);
        payload[17..21].copy_from_slice(&crc.to_be_bytes());
        payload
    }

    #[test]
    fn staged_sync_commits_and_abort_poisons() -> Result<(), Error> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "partitionline-stage-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir)?;
        let limits = Limits::default();
        let (mut first, _) = Journal::open(dir.join("a"), 0, limits)?;
        let (mut second, _) = Journal::open(dir.join("b"), 0, limits)?;
        let left = first.append_staged(1, b"left", crc32c::crc32c(b"left"))?;
        let right = second.append_staged(1, b"right", crc32c::crc32c(b"right"))?;
        let joined = std::thread::spawn(move || left.sync_data());
        right.sync_data()?;
        joined
            .join()
            .map_err(|_| std::io::Error::other("sync thread"))??;
        let committed = first.commit_staged()?;
        assert_eq!(committed.first_offset, 0);
        second.commit_staged()?;
        let fetched = first.fetch(0, 1, 1024)?;
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].payload, b"left");
        drop(first);
        drop(second);
        let (mut reopened, _) = Journal::open(dir.join("a"), 0, limits)?;
        let fetched = reopened.fetch(0, 1, 1024)?;
        assert_eq!(fetched[0].payload, b"left");
        let (mut aborted, _) = Journal::open(dir.join("c"), 0, limits)?;
        let _file = aborted.append_staged(1, b"gone", crc32c::crc32c(b"gone"))?;
        aborted.abort_staged();
        assert!(aborted.is_poisoned());
        assert!(aborted.append(1, b"next").is_err());
        drop(aborted);
        let (mut recovered, recovery) = Journal::open(dir.join("c"), 0, limits)?;
        assert_eq!(recovery.recovered_entries, 1);
        assert_eq!(recovered.fetch(0, 1, 1024)?[0].payload, b"gone");
        drop(reopened);
        drop(recovered);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn grouped_stages_share_one_sync_and_commit_in_order() -> Result<(), Error> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "partitionline-group-stage-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir)?;
        let limits = Limits::default();
        let (mut log, _) = Journal::open(dir.join("log"), 0, limits)?;
        let first = log.append_staged(1, b"one", crc32c::crc32c(b"one"))?;
        let second = log.append_staged_grouped(2, b"two", crc32c::crc32c(b"two"))?;
        drop(first);
        second.sync_data()?;
        let committed = log.commit_staged()?;
        assert_eq!(committed.first_offset, 0);
        assert_eq!(committed.record_count, 1);
        let committed = log.commit_staged()?;
        assert_eq!(committed.first_offset, 1);
        assert_eq!(committed.record_count, 2);
        assert_eq!(log.next_offset(), 3);
        let mut dest = Vec::new();
        let view = log
            .fetch_one_into(1, 4096, &mut dest)?
            .expect("second grouped entry");
        assert_eq!(view.first_offset, 1);
        assert_eq!(view.record_count, 2);
        assert_eq!(&dest[..view.len], b"two");
        let only_second = log.fetch(1, 1, 4096)?;
        assert_eq!(only_second.len(), 1);
        assert_eq!(only_second[0].payload, b"two");
        let fetched = log.fetch(0, 2, 4096)?;
        assert_eq!(fetched.len(), 2);
        assert_eq!(fetched[0].payload, b"one");
        assert_eq!(fetched[1].payload, b"two");
        drop(log);
        let (mut reopened, recovery) = Journal::open(dir.join("log"), 0, limits)?;
        assert_eq!(recovery.recovered_entries, 2);
        assert_eq!(reopened.next_offset(), 3);
        let fetched = reopened.fetch(0, 2, 4096)?;
        assert_eq!(fetched[0].payload, b"one");
        assert_eq!(fetched[1].payload, b"two");
        drop(reopened);

        let (mut undone, _) = Journal::open(dir.join("undo"), 0, limits)?;
        let kept = undone.append_staged(1, b"keep", crc32c::crc32c(b"keep"))?;
        let _dropped = undone.append_staged_grouped(1, b"drop", crc32c::crc32c(b"drop"))?;
        assert_eq!(undone.unstage_last()?, 1);
        kept.sync_data()?;
        let committed = undone.commit_staged()?;
        assert_eq!(committed.first_offset, 0);
        assert_eq!(committed.next_offset, 1);
        let fetched = undone.fetch(0, 2, 4096)?;
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].payload, b"keep");
        drop(undone);

        let (mut guarded, _) = Journal::open(dir.join("guard"), 0, limits)?;
        let _held = guarded.append_staged(1, b"a", crc32c::crc32c(b"a"))?;
        assert!(guarded
            .append_staged(1, b"b", crc32c::crc32c(b"b"))
            .is_err());
        assert!(guarded.is_poisoned());
        drop(guarded);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn length_is_rechecked_after_invalidation() {
        let dir = std::env::temp_dir().join(format!(
            "pl-journal-length-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let (mut journal, _) = Journal::open(&path, 0, Limits::default()).unwrap();
        journal.append(1, b"one").unwrap();
        journal.append(1, b"two").unwrap();
        let mut dest = Vec::new();
        let first = journal.fetch_one_into(0, 4096, &mut dest).unwrap().unwrap();
        assert_eq!(&dest[..first.len], b"one");
        let second = journal.fetch_one_into(1, 4096, &mut dest).unwrap().unwrap();
        assert_eq!(&dest[first.len..first.len + second.len], b"two");
        let mut external = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut external, b"xxx").unwrap();
        external.sync_all().unwrap();
        let cached = journal.fetch_one_into(0, 4096, &mut Vec::new()).unwrap();
        assert_eq!(cached.unwrap().len, 3);
        journal.invalidate_length_observation();
        assert!(matches!(
            journal.fetch_one_into(0, 4096, &mut Vec::new()),
            Err(Error::ChangedFile)
        ));
        assert!(journal.is_poisoned());
        drop(journal);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
