//! Bounded rolling ordinary-record journals and verified offset/time checkpoints.
//!
//! Data uses `PLJRNL01`; this is not Kafka's disk format. One manifest selects
//! contiguous sealed generations and one active journal. Derived seek bundles
//! are rejected and rebuilt from verified records. Publication synchronizes
//! files, atomically renames the manifest, then synchronizes its directory;
//! ambiguous failures poison the handle. Run exclusively on a blocking storage
//! owner. Explicit retention publishes a version-two manifest containing a
//! monotonic logical floor and cleanup victims before unlinking files. Opening
//! or appending a version-one log never migrates it. Process-local ownership is
//! not cross-process locking; no replication or physical power-loss claim.

use crate::{journal, records};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};

const MANIFEST: &str = "manifest";
const MANIFEST_TEMP: &str = "manifest.tmp";
const MANIFEST_MAGIC: &[u8; 8] = b"PLSEGM01";
const RETENTION_MAGIC: &[u8; 8] = b"PLSEGM02";
const INDEX_MAGIC: &[u8; 8] = b"PLSEEK01";
const MANIFEST_HEADER: usize = 44;
const RETENTION_HEADER: usize = 64;
const DESCRIPTOR_BYTES: usize = 56;
const CHECKPOINT_BYTES: usize = 24;
const EMPTY_TIME: i64 = i64::MIN;

/// Positive rolling, persistent-resource and bounded scan limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    roll_bytes: u64,
    max_segments: usize,
    max_entries: usize,
    interval: usize,
    disk_bytes: u64,
    index_bytes: usize,
    scan_bytes: u64,
}
impl Limits {
    /// Configure a soft file target, total segment count, entries per segment,
    /// checkpoint interval in entries, physical file bytes, retained index bytes
    /// and complete-file bytes per recovery segment/payload bytes per seek.
    /// Seek entry counts separately bound 32-byte entry and 24-byte file headers.
    /// Hard ceilings are 1 GiB
    /// targets, 1024 segments, 65,536 entries, 1 TiB disk, 64 MiB indexes and
    /// 128 MiB per scan. Allocator/OS/caller output and directory blocks are
    /// separate; disk charges all file lengths including replacement/staging.
    pub fn new(
        roll_bytes: u64,
        max_segments: usize,
        max_entries: usize,
        interval: usize,
        disk_bytes: u64,
        index_bytes: usize,
        scan_bytes: u64,
    ) -> Result<Self, Error> {
        if !(57..=1 << 30).contains(&roll_bytes)
            || !(1..=1024).contains(&max_segments)
            || !(1..=65_536).contains(&max_entries)
            || !(1..=max_entries).contains(&interval)
            || !(4096..=1 << 40).contains(&disk_bytes)
            || !(1..=64 * 1024 * 1024).contains(&index_bytes)
            || !(57..=128 * 1024 * 1024).contains(&scan_bytes)
        {
            return Err(Error::InvalidLimits);
        }
        let value = Self {
            roll_bytes,
            max_segments,
            max_entries,
            interval,
            disk_bytes,
            index_bytes,
            scan_bytes,
        };
        if value.index_envelope()? > index_bytes || roll_bytes > scan_bytes {
            return Err(Error::InvalidLimits);
        }
        Ok(value)
    }
    /// Worst-case physical file bytes, including old/new replacement generations.
    pub fn max_disk_bytes(self) -> u64 {
        self.disk_bytes
    }
    /// Conservative retained index/capacity/replacement-buffer envelope.
    pub fn max_index_bytes(self) -> usize {
        self.index_bytes
    }
    /// Maximum selected data segments, including the active one. Replacement
    /// or active retirement may briefly retain one additional charged old file.
    pub fn max_segments(self) -> usize {
        self.max_segments
    }
    /// Conservative checked index requirement, including growth/rebuild buffers.
    pub fn index_envelope(self) -> Result<usize, Error> {
        let checkpoints = self.max_entries.div_ceil(self.interval);
        self.max_segments
            .checked_mul(checkpoints)
            .and_then(|n| n.checked_mul(size_of::<Checkpoint>()))
            .and_then(|n| {
                n.checked_add(self.max_segments.checked_mul(
                    size_of::<Segment>() + size_of::<Descriptor>() * 2 + DESCRIPTOR_BYTES * 2,
                )?)
            })
            .and_then(|n| n.checked_add(self.max_entries.checked_mul(128)?))
            .and_then(|n| n.checked_add(checkpoints.checked_mul(CHECKPOINT_BYTES * 4)?))
            .and_then(|n| {
                n.checked_add((self.max_segments * 4 + 8).checked_mul(size_of::<String>() + 42)?)
            })
            .and_then(|n| n.checked_add(4096))
            .ok_or(Error::InvalidLimits)
    }
    pub(crate) fn journal_limits(self, source: journal::Limits) -> Result<journal::Limits, Error> {
        let hard = self.roll_bytes.max(source.max_entry_bytes() as u64 + 56);
        if hard > source.max_file_bytes() || hard > self.scan_bytes {
            return Err(Error::InvalidLimits);
        }
        journal::Limits::new(
            source.max_entry_bytes(),
            hard,
            self.max_entries.min(source.max_index_entries()),
            source.max_fetch_bytes(),
        )
        .map_err(Error::Storage)
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            roll_bytes: 16 * 1024 * 1024,
            max_segments: 64,
            max_entries: 4096,
            interval: 16,
            disk_bytes: 1 << 30,
            index_bytes: 2 * 1024 * 1024,
            scan_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Rolled-storage failure without retained paths or record contents.
#[derive(Debug)]
pub enum Error {
    /// Invalid or inconsistent positive/aggregate bounds.
    InvalidLimits,
    /// A guard is negative or outside the current logical/durable range.
    InvalidRetentionBounds,
    /// The requested logical floor is negative or above confirmed high watermark.
    OffsetOutOfRange,
    /// The requested floor would remove caller-protected records.
    ProtectedRecords,
    /// A referenced manifest/file or unexpected directory entry is invalid.
    InvalidLayout,
    /// Derived bundle is not a verified index for its selected segment.
    InvalidIndex,
    /// Referenced data does not match its protected manifest descriptor.
    CorruptData,
    /// Physical file lengths including staging would exceed the disk ceiling.
    DiskBudget,
    /// Another segment cannot be added under the configured count ceiling.
    SegmentBudget,
    /// Bounded index/output reservation failed.
    AllocationFailed,
    /// A bounded seek/recovery scan cannot complete within its work budget.
    ScanBudget,
    /// Generation arithmetic would wrap.
    GenerationOverflow,
    /// Ambiguous publication or I/O failure requires reopening.
    Poisoned,
    /// Checked journal operation failed.
    Storage(journal::Error),
    /// Filesystem operation failed.
    Io(std::io::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "segment I/O: {:?}", e.kind()),
            Self::Storage(e) => write!(f, "segment journal: {e}"),
            other => write!(f, "segments: {other:?}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<journal::Error> for Error {
    fn from(e: journal::Error) -> Self {
        Self::Storage(e)
    }
}

/// Caller-confirmed deletion ceilings, not a fabricated replication guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionGuard {
    confirmed_high_watermark: i64,
    retain_from: i64,
}
impl DeletionGuard {
    /// Both nonnegative bounds are checked against the actual log on each call.
    pub fn new(confirmed_high_watermark: i64, retain_from: i64) -> Result<Self, Error> {
        if confirmed_high_watermark < 0 || retain_from < 0 {
            return Err(Error::InvalidRetentionBounds);
        }
        Ok(Self {
            confirmed_high_watermark,
            retain_from,
        })
    }
    /// Highest caller-confirmed durable record boundary eligible for deletion.
    pub fn confirmed_high_watermark(self) -> i64 {
        self.confirmed_high_watermark
    }
    /// Earliest boundary required by the caller's retained-record constraint.
    pub fn retain_from(self) -> i64 {
        self.retain_from
    }
}

/// Completed durable floor publication and cleanup result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionOutcome {
    /// Monotonic logical start, independent of the containing segment's base.
    pub log_start_offset: i64,
    /// Actual data and seek files removed by this operation.
    pub reclaimed_files: usize,
    /// Lengths of those removed files; manifest size changes are excluded.
    pub reclaimed_bytes: u64,
}

/// Explicit caller-driven age/record-payload size policy; no background timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    retention_ms: Option<u64>,
    retention_bytes: Option<u64>,
    max_segments: usize,
}
impl RetentionPolicy {
    /// Zero thresholds are valid; absent thresholds disable that criterion.
    /// At most 1..=1024 whole sealed segments are selected per invocation.
    pub fn new(
        retention_ms: Option<u64>,
        retention_bytes: Option<u64>,
        max_segments: usize,
    ) -> Result<Self, Error> {
        if !(1..=1024).contains(&max_segments) {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            retention_ms,
            retention_bytes,
            max_segments,
        })
    }
    /// Optional strict age threshold in milliseconds.
    pub fn retention_ms(self) -> Option<u64> {
        self.retention_ms
    }
    /// Optional total retained record-payload byte target, excluding file headers.
    pub fn retention_bytes(self) -> Option<u64> {
        self.retention_bytes
    }
    /// Maximum prefix segments selected by one bounded sweep.
    pub fn max_segments(self) -> usize {
        self.max_segments
    }
}

/// Explicit recovery outcomes; rejected caches never supply seek positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Recovery {
    /// Referenced sealed and active segments.
    pub segments: usize,
    /// Rebuilt missing, stale, malformed or corrupt derived bundles.
    pub rebuilt_indexes: usize,
    /// Known unreferenced staging/retired files removed after manifest selection.
    pub removed_staging_files: usize,
    /// Active final incomplete-entry bytes repaired by Journal.
    pub truncated_bytes: u64,
    /// Recovered exclusive durable end offset.
    pub next_offset: u64,
    /// Recovered monotonic logical start (may be inside the first physical batch).
    pub log_start_offset: u64,
    /// Recorded retention victims completed during recovery.
    pub removed_retention_files: usize,
}
/// Bounded payload work examined during a checked physical seek.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Work {
    /// Payload bytes checked, including entries before the requested offset.
    pub bytes: usize,
    /// Entries checked, including entries before the requested offset.
    pub entries: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Checkpoint {
    offset: u64,
    position: u64,
    prefix_max: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Descriptor {
    base: u64,
    end: u64,
    generation: u64,
    bytes: u64,
    entries: u64,
    max_time: i64,
    fingerprint: u32,
}
struct Segment {
    descriptor: Descriptor,
    checkpoints: Vec<Checkpoint>,
}
struct Manifest {
    revision: u64,
    base: u64,
    physical_base: u64,
    floor: u64,
    retention: bool,
    active_base: u64,
    active_generation: u64,
    sealed: Vec<Descriptor>,
    victims: Vec<Descriptor>,
}

/// Exclusive synchronous rolling log for validated assigned ordinary batches.
///
/// One active Journal retains a bounded dense index. Sealed indexes are sparse;
/// reads open at most one transient checked reader. Publication/replacement adds
/// at most three further files. Store must aggregate these bounds across logs.
pub struct Log {
    directory: PathBuf,
    _ownership: journal::DirectoryOwnership,
    limits: Limits,
    journal_limits: journal::Limits,
    fetch_entries: usize,
    records: records::Limits,
    manifest: Manifest,
    sealed: Vec<Segment>,
    active: Option<journal::Journal>,
    active_checkpoints: Vec<Checkpoint>,
    active_max: i64,
    committed_next: u64,
    poisoned: bool,
    #[cfg(test)]
    fault: Option<(Phase, bool)>,
}
fn data_name(base: u64, generation: u64) -> String {
    format!("{base:016x}-{generation:016x}.journal")
}
fn index_name(base: u64, generation: u64) -> String {
    format!("{base:016x}-{generation:016x}.seek")
}
fn number(bytes: &[u8], at: usize) -> Result<u64, Error> {
    Ok(u64::from_be_bytes(
        bytes
            .get(at..at + 8)
            .ok_or(Error::InvalidLayout)?
            .try_into()
            .map_err(|_| Error::InvalidLayout)?,
    ))
}
fn count(bytes: &[u8], at: usize) -> Result<u32, Error> {
    Ok(u32::from_be_bytes(
        bytes
            .get(at..at + 4)
            .ok_or(Error::InvalidLayout)?
            .try_into()
            .map_err(|_| Error::InvalidLayout)?,
    ))
}
fn put_descriptor(out: &mut Vec<u8>, d: Descriptor) {
    for n in [
        d.base,
        d.end,
        d.generation,
        d.bytes,
        d.entries,
        d.max_time as u64,
    ] {
        out.extend_from_slice(&n.to_be_bytes());
    }
    out.extend_from_slice(&d.fingerprint.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
}
fn parse_descriptor(bytes: &[u8], at: usize) -> Result<Descriptor, Error> {
    if count(bytes, at + 52)? != 0 {
        return Err(Error::InvalidLayout);
    }
    Ok(Descriptor {
        base: number(bytes, at)?,
        end: number(bytes, at + 8)?,
        generation: number(bytes, at + 16)?,
        bytes: number(bytes, at + 24)?,
        entries: number(bytes, at + 32)?,
        max_time: number(bytes, at + 40)? as i64,
        fingerprint: count(bytes, at + 48)?,
    })
}
fn buffer(size: usize) -> Result<Vec<u8>, Error> {
    let mut v = Vec::new();
    v.try_reserve_exact(size)
        .map_err(|_| Error::AllocationFailed)?;
    Ok(v)
}
fn manifest_bytes(m: &Manifest) -> Result<Vec<u8>, Error> {
    let mut out = buffer(manifest_size(m))?;
    if m.retention {
        out.extend_from_slice(RETENTION_MAGIC);
        for n in [
            m.revision,
            m.base,
            m.physical_base,
            m.floor,
            m.active_base,
            m.active_generation,
        ] {
            out.extend_from_slice(&n.to_be_bytes());
        }
        out.extend_from_slice(&(m.sealed.len() as u32).to_be_bytes());
        out.extend_from_slice(&(m.victims.len() as u32).to_be_bytes());
    } else {
        out.extend_from_slice(MANIFEST_MAGIC);
        for n in [m.revision, m.base, m.active_base, m.active_generation] {
            out.extend_from_slice(&n.to_be_bytes());
        }
        out.extend_from_slice(&(m.sealed.len() as u32).to_be_bytes());
    }
    for d in m.sealed.iter().chain(&m.victims) {
        put_descriptor(&mut out, *d);
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}
fn manifest_size(m: &Manifest) -> usize {
    (if m.retention {
        RETENTION_HEADER
    } else {
        MANIFEST_HEADER
    }) + (m.sealed.len() + m.victims.len()) * DESCRIPTOR_BYTES
        + 4
}
fn bundle_bytes(segment: &Segment) -> Result<Vec<u8>, Error> {
    let mut out = buffer(72 + segment.checkpoints.len() * CHECKPOINT_BYTES)?;
    out.extend_from_slice(INDEX_MAGIC);
    put_descriptor(&mut out, segment.descriptor);
    out.extend_from_slice(&(segment.checkpoints.len() as u32).to_be_bytes());
    for p in &segment.checkpoints {
        for n in [p.offset, p.position, p.prefix_max as u64] {
            out.extend_from_slice(&n.to_be_bytes());
        }
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}
fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, Error> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(Error::InvalidLayout);
    }
    let mut file = File::open(path)?;
    let len = usize::try_from(file.metadata()?.len()).map_err(|_| Error::InvalidLayout)?;
    if len > maximum {
        return Err(Error::InvalidIndex);
    }
    let mut out = buffer(len)?;
    out.resize(len, 0);
    file.read_exact(&mut out)?;
    if file.metadata()?.len() != len as u64 {
        return Err(Error::InvalidLayout);
    }
    Ok(out)
}
fn parse_manifest(bytes: &[u8], limits: Limits, base: u64) -> Result<Manifest, Error> {
    let retention = bytes.get(..8) == Some(RETENTION_MAGIC);
    let header = if retention {
        RETENTION_HEADER
    } else {
        MANIFEST_HEADER
    };
    if bytes.len() < header + 4 || (!retention && bytes.get(..8) != Some(MANIFEST_MAGIC)) {
        return Err(Error::InvalidLayout);
    }
    let n = count(bytes, if retention { 56 } else { 40 })? as usize;
    let victims = if retention {
        count(bytes, 60)? as usize
    } else {
        0
    };
    if n >= limits.max_segments
        || n.checked_add(victims)
            .is_none_or(|n| n > limits.max_segments)
        || bytes.len() != header + 4 + (n + victims) * DESCRIPTOR_BYTES
        || crc32c::crc32c(&bytes[..bytes.len() - 4]) != count(bytes, bytes.len() - 4)?
    {
        return Err(Error::InvalidLayout);
    }
    let mut m = Manifest {
        revision: number(bytes, 8)?,
        base: number(bytes, 16)?,
        physical_base: if retention { number(bytes, 24)? } else { base },
        floor: if retention { number(bytes, 32)? } else { base },
        retention,
        active_base: number(bytes, if retention { 40 } else { 24 })?,
        active_generation: number(bytes, if retention { 48 } else { 32 })?,
        sealed: Vec::new(),
        victims: Vec::new(),
    };
    if m.base != base
        || m.active_generation > m.revision
        || m.physical_base < base
        || m.floor < m.physical_base
        || i64::try_from(m.floor).is_err()
    {
        return Err(Error::InvalidLayout);
    }
    m.sealed
        .try_reserve_exact(limits.max_segments)
        .map_err(|_| Error::AllocationFailed)?;
    m.victims
        .try_reserve_exact(victims)
        .map_err(|_| Error::AllocationFailed)?;
    let mut next = m.physical_base;
    for at in (header..header + n * DESCRIPTOR_BYTES).step_by(DESCRIPTOR_BYTES) {
        let d = parse_descriptor(bytes, at)?;
        if d.base != next || !valid_descriptor(d, limits, m.revision) {
            return Err(Error::InvalidLayout);
        }
        next = d.end;
        m.sealed.push(d);
    }
    if next != m.active_base {
        return Err(Error::InvalidLayout);
    }
    let mut end = None;
    for at in (header + n * DESCRIPTOR_BYTES..header + (n + victims) * DESCRIPTOR_BYTES)
        .step_by(DESCRIPTOR_BYTES)
    {
        let d = parse_descriptor(bytes, at)?;
        if !valid_descriptor(d, limits, m.revision)
            || d.base < base
            || d.end > m.physical_base
            || end.is_some_and(|end| end != d.base)
        {
            return Err(Error::InvalidLayout);
        }
        end = Some(d.end);
        m.victims.push(d);
    }
    if end.is_some_and(|end| end != m.physical_base) {
        return Err(Error::InvalidLayout);
    }
    Ok(m)
}
fn valid_descriptor(d: Descriptor, limits: Limits, revision: u64) -> bool {
    d.end > d.base
        && i64::try_from(d.end).is_ok()
        && d.generation <= revision
        && d.entries != 0
        && d.entries <= limits.max_entries as u64
        && d.base
            .checked_add(d.entries)
            .is_some_and(|minimum| d.end >= minimum)
        && d.bytes >= 24 + d.entries * 33
}
fn summarize(entry: &journal::Entry, limits: records::Limits) -> Result<i64, Error> {
    let checked = records::validate(&entry.payload, limits).map_err(|_| Error::CorruptData)?;
    if checked.record_count() != entry.record_count as usize {
        return Err(Error::CorruptData);
    }
    let mut offset = i64::try_from(entry.first_offset).map_err(|_| Error::CorruptData)?;
    let mut maximum = EMPTY_TIME;
    for b in checked.batches() {
        let b = b.map_err(|_| Error::CorruptData)?;
        if b.base_offset != offset {
            return Err(Error::CorruptData);
        }
        offset = b.next_offset;
        maximum = maximum.max(b.max_timestamp);
    }
    if u64::try_from(offset).ok()
        != entry
            .first_offset
            .checked_add(u64::from(entry.record_count))
    {
        return Err(Error::CorruptData);
    }
    Ok(maximum)
}
fn summarize_payload(
    offset: u64,
    count: u32,
    payload: &[u8],
    limits: records::Limits,
) -> Result<i64, Error> {
    let checked = records::validate(payload, limits).map_err(|_| Error::CorruptData)?;
    if checked.record_count() != count as usize {
        return Err(Error::CorruptData);
    }
    let mut next = i64::try_from(offset).map_err(|_| Error::CorruptData)?;
    let mut maximum = EMPTY_TIME;
    for b in checked.batches() {
        let b = b.map_err(|_| Error::CorruptData)?;
        if b.base_offset != next {
            return Err(Error::CorruptData);
        }
        next = b.next_offset;
        maximum = maximum.max(b.max_timestamp);
    }
    if u64::try_from(next).ok() != offset.checked_add(u64::from(count)) {
        return Err(Error::CorruptData);
    }
    Ok(maximum)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    CopyEntrySynced,
    IndexWritten,
    IndexSynced,
    IndexRenamed,
    NewActiveSynced,
    ManifestWritten,
    ManifestSynced,
    ManifestRenamed,
    DirectorySynced,
    OldDataRemoved,
    RetentionDataRemoved,
    RetentionIndexRemoved,
    RetentionCleanupSynced,
    RetentionClearWritten,
    RetentionClearSynced,
    RetentionClearRenamed,
    RetentionClearDirectorySynced,
}
impl Log {
    /// Recover selected generations and certify every payload within bounded work.
    /// Only active incomplete tails may be repaired; stale derived bundles are
    /// reported/rebuilt, never trusted. Wrong layouts/missing data fail closed.
    pub fn open(
        directory: impl AsRef<Path>,
        base: u64,
        source: journal::Limits,
        records: records::Limits,
        limits: Limits,
    ) -> Result<(Self, Recovery), Error> {
        let directory = directory.as_ref();
        if directory.as_os_str().is_empty() || directory.as_os_str().len() > 4096 {
            return Err(Error::InvalidLimits);
        }
        let journal_limits = limits.journal_limits(source)?;
        if limits.index_envelope()? > limits.index_bytes
            || journal_limits.max_entry_bytes() + size_of::<journal::Entry>()
                > journal_limits.max_fetch_bytes()
            || i64::try_from(base).is_err()
        {
            return Err(Error::InvalidLimits);
        }
        let mut ownership = journal::DirectoryOwnership::acquire(directory)?;
        match fs::create_dir(directory) {
            Ok(()) => {
                File::open(directory.parent().unwrap_or(Path::new(".")))?.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        if !fs::symlink_metadata(directory)?.file_type().is_dir() {
            return Err(Error::InvalidLayout);
        }
        ownership.identify(&File::open(directory)?)?;
        let manifest_path = directory.join(MANIFEST);
        let mut initial = false;
        let manifest = if manifest_path.try_exists()? {
            parse_manifest(
                &read_bounded(
                    &manifest_path,
                    RETENTION_HEADER + 4 + limits.max_segments * DESCRIPTOR_BYTES,
                )?,
                limits,
                base,
            )?
        } else {
            if fs::read_dir(directory)?.next().transpose()?.is_some() {
                return Err(Error::InvalidLayout);
            }
            initial = true;
            let mut sealed = Vec::new();
            sealed
                .try_reserve_exact(limits.max_segments)
                .map_err(|_| Error::AllocationFailed)?;
            Manifest {
                revision: 0,
                base,
                physical_base: base,
                floor: base,
                retention: false,
                active_base: base,
                active_generation: 0,
                sealed,
                victims: Vec::new(),
            }
        };
        let mut sealed = Vec::new();
        sealed
            .try_reserve_exact(limits.max_segments)
            .map_err(|_| Error::AllocationFailed)?;
        let mut result = Self {
            directory: directory.to_path_buf(),
            _ownership: ownership,
            limits,
            journal_limits,
            fetch_entries: source.max_index_entries(),
            records,
            manifest,
            sealed,
            active: None,
            active_checkpoints: Vec::new(),
            active_max: EMPTY_TIME,
            committed_next: base,
            poisoned: false,
            #[cfg(test)]
            fault: None,
        };
        result
            .active_checkpoints
            .try_reserve_exact(limits.max_entries.div_ceil(limits.interval))
            .map_err(|_| Error::AllocationFailed)?;
        result.disk_bytes()?;
        let active_path = result.data_path(
            result.manifest.active_base,
            result.manifest.active_generation,
        );
        if !initial && !active_path.try_exists()? {
            return Err(Error::InvalidLayout);
        }
        if !initial && !fs::symlink_metadata(&active_path)?.file_type().is_file() {
            return Err(Error::InvalidLayout);
        }
        let (active, recovered) =
            journal::Journal::open(&active_path, result.manifest.active_base, journal_limits)?;
        result.committed_next = active.next_offset();
        if result.manifest.floor > result.committed_next {
            return Err(Error::InvalidLayout);
        }
        result.active = Some(active);
        let mut recovery = Recovery {
            truncated_bytes: recovered.truncated_bytes,
            ..Recovery::default()
        };
        result.rebuild_active()?;
        recovery.removed_staging_files = result.cleanup()?;
        for position in 0..result.manifest.sealed.len() {
            let d = result.manifest.sealed[position];
            let segment = result.scan(d.base, d.generation)?;
            if segment.descriptor != d {
                return Err(Error::CorruptData);
            }
            let expected = bundle_bytes(&segment)?;
            let path = result.directory.join(index_name(d.base, d.generation));
            // An unchecked cache length cannot consume the whole configured
            // envelope on top of retained tables and the expected bundle.
            let valid = match read_bounded(&path, expected.len()) {
                Ok(bytes) => bytes == expected,
                Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(Error::InvalidIndex) => false,
                Err(e) => return Err(e),
            };
            if !valid {
                result.write_index(&segment)?;
                recovery.rebuilt_indexes += 1;
            }
            result.sealed.push(segment);
        }
        if !result.manifest.victims.is_empty() {
            let (removed, _) = result.finish_retention_cleanup()?;
            recovery.removed_retention_files = removed;
        }
        if initial {
            result.publish_manifest()?;
        }
        result.disk_bytes()?;
        File::open(&result.directory)?.sync_all()?;
        recovery.segments = result.sealed.len() + 1;
        recovery.next_offset = result.next_offset();
        recovery.log_start_offset = result.base_offset();
        Ok((result, recovery))
    }
    fn active(&self) -> Result<&journal::Journal, Error> {
        self.active.as_ref().ok_or(Error::Poisoned)
    }
    fn active_mut(&mut self) -> Result<&mut journal::Journal, Error> {
        self.active.as_mut().ok_or(Error::Poisoned)
    }
    fn data_path(&self, base: u64, generation: u64) -> PathBuf {
        self.directory.join(data_name(base, generation))
    }
    fn alive(&self) -> Result<(), Error> {
        if self.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    /// Whether reopening is required after an ambiguous storage failure.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
            || self
                .active
                .as_ref()
                .is_none_or(journal::Journal::is_poisoned)
    }
    /// Monotonic logical floor, possibly inside the first physical atomic input.
    pub fn base_offset(&self) -> u64 {
        self.manifest.floor
    }
    /// Kafka signed-domain logical start; opening validated its range.
    pub fn log_start_offset(&self) -> i64 {
        self.manifest.floor as i64
    }
    /// Exclusive committed end; failures do not advance this value in memory.
    pub fn next_offset(&self) -> u64 {
        self.committed_next
    }
    /// Total retained atomic inputs across all selected segments.
    pub fn entry_count(&self) -> usize {
        self.sealed
            .iter()
            .map(|s| s.descriptor.entries as usize)
            .sum::<usize>()
            + self
                .active
                .as_ref()
                .map_or(0, journal::Journal::entry_count)
    }
    /// Selected segment count, including the possibly empty active journal.
    pub fn segment_count(&self) -> usize {
        self.sealed.len() + 1
    }
    /// Actual retained array capacity charged by the conservative index envelope.
    /// Temporary bundle/replacement/growth buffers have additional reservations;
    /// allocator/ownership/path/OS metadata and caller payload output are separate.
    pub fn retained_index_bytes(&self) -> usize {
        self.manifest.sealed.capacity() * size_of::<Descriptor>()
            + self.manifest.victims.capacity() * size_of::<Descriptor>()
            + self.sealed.capacity() * size_of::<Segment>()
            + self.active_checkpoints.capacity() * size_of::<Checkpoint>()
            + self
                .sealed
                .iter()
                .map(|s| s.checkpoints.capacity() * size_of::<Checkpoint>())
                .sum::<usize>()
            + self
                .active
                .as_ref()
                .map_or(0, journal::Journal::index_capacity_bytes)
    }
    /// Current charged physical file lengths, with bounded directory enumeration.
    pub fn disk_bytes(&self) -> Result<u64, Error> {
        let mut n = 0usize;
        let mut total = 0u64;
        for e in fs::read_dir(&self.directory)? {
            let e = e?;
            n += 1;
            if n > self.limits.max_segments * 4 + 8 || !e.file_type()?.is_file() {
                return Err(Error::InvalidLayout);
            }
            total = total
                .checked_add(e.metadata()?.len())
                .ok_or(Error::DiskBudget)?;
            if total > self.limits.disk_bytes {
                return Err(Error::DiskBudget);
            }
        }
        Ok(total)
    }
    fn reserve_disk(&self, extra: u64) -> Result<(), Error> {
        if self
            .disk_bytes()?
            .checked_add(extra)
            .is_none_or(|n| n > self.limits.disk_bytes)
        {
            Err(Error::DiskBudget)
        } else {
            Ok(())
        }
    }
    fn hit(&mut self, phase: Phase) -> Result<(), Error> {
        #[cfg(test)]
        if self.fault.is_some_and(|(p, _)| p == phase) {
            let exit = self.fault.take().is_some_and(|(_, exit)| exit);
            if exit {
                std::process::exit(0);
            }
            return Err(std::io::Error::other("injected publication failure").into());
        }
        let _ = phase;
        Ok(())
    }
    fn write_index(&mut self, segment: &Segment) -> Result<(), Error> {
        let bytes = bundle_bytes(segment)?;
        self.reserve_disk(bytes.len() as u64)?;
        let name = index_name(segment.descriptor.base, segment.descriptor.generation);
        let tmp = self.directory.join(format!("{name}.tmp"));
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        file.write_all(&bytes)?;
        self.hit(Phase::IndexWritten)?;
        file.sync_all()?;
        self.hit(Phase::IndexSynced)?;
        drop(file);
        fs::rename(tmp, self.directory.join(name))?;
        self.hit(Phase::IndexRenamed)?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
    fn publish_manifest(&mut self) -> Result<(), Error> {
        self.publish_manifest_kind(false)
    }
    fn publish_manifest_kind(&mut self, cleanup: bool) -> Result<(), Error> {
        let bytes = manifest_bytes(&self.manifest)?;
        self.reserve_disk(bytes.len() as u64)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.directory.join(MANIFEST_TEMP))?;
        file.write_all(&bytes)?;
        self.hit(if cleanup {
            Phase::RetentionClearWritten
        } else {
            Phase::ManifestWritten
        })?;
        file.sync_all()?;
        self.hit(if cleanup {
            Phase::RetentionClearSynced
        } else {
            Phase::ManifestSynced
        })?;
        drop(file);
        fs::rename(
            self.directory.join(MANIFEST_TEMP),
            self.directory.join(MANIFEST),
        )?;
        self.hit(if cleanup {
            Phase::RetentionClearRenamed
        } else {
            Phase::ManifestRenamed
        })?;
        File::open(&self.directory)?.sync_all()?;
        self.hit(if cleanup {
            Phase::RetentionClearDirectorySynced
        } else {
            Phase::DirectorySynced
        })?;
        Ok(())
    }
    fn scan(&self, base: u64, generation: u64) -> Result<Segment, Error> {
        let mut cursor = journal::CheckedCursor::open(
            &self.data_path(base, generation),
            base,
            self.journal_limits,
        )?;
        if cursor.file_bytes() > self.limits.scan_bytes {
            return Err(Error::ScanBudget);
        }
        let mut checkpoints = Vec::new();
        checkpoints
            .try_reserve_exact(self.limits.max_entries.div_ceil(self.limits.interval))
            .map_err(|_| Error::AllocationFailed)?;
        let mut entries = 0usize;
        let mut maximum = EMPTY_TIME;
        while let Some((position, entry)) = cursor.next(self.journal_limits.max_fetch_bytes())? {
            if entries >= self.limits.max_entries {
                return Err(Error::ScanBudget);
            }
            if entries % self.limits.interval == 0 {
                checkpoints.push(Checkpoint {
                    offset: entry.first_offset,
                    position,
                    prefix_max: maximum,
                });
            }
            maximum = maximum.max(summarize(&entry, self.records)?);
            entries += 1;
        }
        cursor.synchronize()?;
        Ok(Segment {
            descriptor: Descriptor {
                base,
                end: cursor.next_offset(),
                generation,
                bytes: cursor.file_bytes(),
                entries: entries as u64,
                max_time: maximum,
                fingerprint: cursor.fingerprint(),
            },
            checkpoints,
        })
    }
    fn rebuild_active(&mut self) -> Result<(), Error> {
        let mut offset = self.manifest.active_base;
        let mut position = 24u64;
        let mut ordinal = 0usize;
        if self.active()?.file_bytes() > self.limits.scan_bytes {
            return Err(Error::ScanBudget);
        }
        while offset < self.active()?.next_offset() {
            let cap = self.journal_limits.max_fetch_bytes();
            let entry = self
                .active_mut()?
                .fetch(offset, 1, cap)?
                .pop()
                .ok_or(Error::CorruptData)?;
            if entry.first_offset != offset {
                return Err(Error::CorruptData);
            }
            if ordinal % self.limits.interval == 0 {
                self.active_checkpoints.push(Checkpoint {
                    offset,
                    position,
                    prefix_max: self.active_max,
                });
            }
            self.active_max = self.active_max.max(summarize(&entry, self.records)?);
            position += 32 + entry.payload.len() as u64;
            offset = offset
                .checked_add(u64::from(entry.record_count))
                .ok_or(Error::CorruptData)?;
            ordinal += 1;
        }
        if position != self.active()?.file_bytes() {
            return Err(Error::CorruptData);
        }
        Ok(())
    }
    fn cleanup(&mut self) -> Result<usize, Error> {
        let mut removed = 0usize;
        let mut names = Vec::new();
        names
            .try_reserve_exact(self.limits.max_segments * 4 + 8)
            .map_err(|_| Error::AllocationFailed)?;
        for e in fs::read_dir(&self.directory)? {
            let e = e?;
            if names.len() >= self.limits.max_segments * 4 + 8 {
                return Err(Error::InvalidLayout);
            }
            let name = e
                .file_name()
                .into_string()
                .map_err(|_| Error::InvalidLayout)?;
            if name.len() > 42 {
                return Err(Error::InvalidLayout);
            }
            names.push(name);
        }
        for name in names {
            if name == MANIFEST {
                continue;
            }
            let mut selected =
                name == data_name(self.manifest.active_base, self.manifest.active_generation);
            for d in &self.manifest.sealed {
                selected |= name == data_name(d.base, d.generation)
                    || name == index_name(d.base, d.generation);
            }
            // Only an authoritative version-two manifest may authorize these
            // names for deletion; recovery validates survivors first.
            for d in &self.manifest.victims {
                selected |= name == data_name(d.base, d.generation)
                    || name == index_name(d.base, d.generation);
            }
            if selected {
                continue;
            }
            let known = if name == MANIFEST_TEMP {
                true
            } else {
                let stem = name
                    .strip_suffix(".seek.tmp")
                    .or_else(|| name.strip_suffix(".seek"))
                    .or_else(|| name.strip_suffix(".journal"));
                if let Some(stem) = stem {
                    let parsed = stem.split_once('-').and_then(|(b, g)| {
                        Some((
                            u64::from_str_radix(b, 16).ok()?,
                            u64::from_str_radix(g, 16).ok()?,
                        ))
                    });
                    parsed.is_some_and(|(base, generation)| {
                        let canonical = format!("{base:016x}-{generation:016x}") == stem;
                        let retired = self
                            .manifest
                            .sealed
                            .iter()
                            .any(|d| d.base == base && generation < d.generation);
                        let pending = self.manifest.revision.checked_add(1) == Some(generation)
                            && (base == self.next_offset()
                                || base == self.manifest.active_base
                                || self.manifest.sealed.iter().any(|d| d.base == base));
                        let derived = self
                            .manifest
                            .sealed
                            .iter()
                            .any(|d| d.base == base && generation == d.generation)
                            && name.ends_with(".tmp");
                        let old_active_index = base == self.manifest.active_base
                            && generation == self.manifest.active_generation
                            && (name.ends_with(".seek") || name.ends_with(".seek.tmp"));
                        canonical && (retired || pending || derived || old_active_index)
                    })
                } else {
                    false
                }
            };
            if !known {
                return Err(Error::InvalidLayout);
            }
            fs::remove_file(self.directory.join(name))?;
            removed += 1;
        }
        if removed != 0 {
            File::open(&self.directory)?.sync_all()?;
        }
        Ok(removed)
    }
    fn roll(&mut self, append_charge: u64) -> Result<(), Error> {
        if self.segment_count() >= self.limits.max_segments {
            return Err(Error::SegmentBudget);
        }
        let revision = self
            .manifest
            .revision
            .checked_add(1)
            .ok_or(Error::GenerationOverflow)?;
        let extra = (72
            + self.active_checkpoints.len() * 24
            + 24
            + manifest_size(&self.manifest)
            + DESCRIPTOR_BYTES) as u64;
        self.reserve_disk(extra.checked_add(append_charge).ok_or(Error::DiskBudget)?)?;
        drop(self.active.take());
        let segment = self.scan(self.manifest.active_base, self.manifest.active_generation)?;
        if segment.descriptor.entries == 0 {
            return Err(Error::CorruptData);
        }
        self.write_index(&segment)?;
        let base = segment.descriptor.end;
        let (active, _) =
            journal::Journal::open(self.data_path(base, revision), base, self.journal_limits)?;
        self.hit(Phase::NewActiveSynced)?;
        self.manifest.sealed.push(segment.descriptor);
        self.manifest.revision = revision;
        self.manifest.active_base = base;
        self.manifest.active_generation = revision;
        self.publish_manifest()?;
        self.sealed.push(segment);
        self.active = Some(active);
        self.active_checkpoints.clear();
        self.active_max = EMPTY_TIME;
        Ok(())
    }
    /// Append one validated assigned ordinary input and synchronize before success.
    /// Rolling occurs before admitting it; an oversized soft-target input remains
    /// atomic within the separate hard entry/file/scan/disk ceilings.
    pub fn append(&mut self, record_count: u32, payload: &[u8]) -> Result<journal::Append, Error> {
        self.alive()?;
        if payload.is_empty() || record_count == 0 {
            return Err(journal::Error::InvalidEntry.into());
        }
        if payload.len() > self.journal_limits.max_entry_bytes() {
            return Err(journal::Error::EntryTooLarge.into());
        }
        let maximum = summarize_payload(self.next_offset(), record_count, payload, self.records)?;
        let charge = 32u64 + payload.len() as u64;
        self.reserve_disk(charge)?;
        let roll = self.active()?.entry_count() != 0
            && (self
                .active()?
                .file_bytes()
                .checked_add(charge)
                .is_none_or(|n| n > self.limits.roll_bytes)
                || self.active()?.entry_count() >= self.journal_limits.max_index_entries());
        if roll {
            if let Err(e) = self.roll(charge) {
                if !matches!(
                    e,
                    Error::SegmentBudget | Error::DiskBudget | Error::GenerationOverflow
                ) {
                    self.poisoned = true;
                }
                return Err(e);
            }
        }
        let position = self.active()?.file_bytes();
        let offset = self.next_offset();
        let ordinal = self.active()?.entry_count();
        let result = self.active_mut()?.append(record_count, payload);
        match result {
            Ok(result) => {
                if ordinal % self.limits.interval == 0 {
                    self.active_checkpoints.push(Checkpoint {
                        offset,
                        position,
                        prefix_max: self.active_max,
                    });
                }
                self.active_max = self.active_max.max(maximum);
                self.committed_next = result.next_offset;
                Ok(result)
            }
            Err(e) => {
                self.poisoned |= self.active()?.is_poisoned();
                Err(e.into())
            }
        }
    }
    /// Conservative first-entry candidate for an offset-ordered timestamp search.
    /// Only prefixes with verified maximum strictly BELOW the query are skipped;
    /// equal/repeated/regressing timestamps never skip an earlier matching record.
    pub fn timestamp_start(&self, wanted: i64) -> Result<u64, Error> {
        self.alive()?;
        for segment in &self.sealed {
            if segment.descriptor.max_time >= wanted {
                return Ok(
                    time_checkpoint(&segment.checkpoints, wanted, segment.descriptor.base)
                        .max(self.base_offset()),
                );
            }
        }
        if self.active_max >= wanted {
            Ok(
                time_checkpoint(&self.active_checkpoints, wanted, self.manifest.active_base)
                    .max(self.base_offset()),
            )
        } else {
            Ok(self.next_offset())
        }
    }
    pub(crate) fn read_one(
        &mut self,
        offset: u64,
        maximum: usize,
        scan_bytes: usize,
        scan_entries: usize,
    ) -> Result<(Option<journal::Entry>, Work), Error> {
        self.alive()?;
        if offset < self.base_offset() {
            return Err(journal::Error::OffsetBeforeBase.into());
        }
        if offset >= self.next_offset() {
            return Ok((None, Work::default()));
        }
        let result = (|| {
            if offset >= self.manifest.active_base {
                let entry = self.active_mut()?.fetch(offset, 1, maximum)?.pop();
                let work = entry.as_ref().map_or(Work::default(), |e| Work {
                    bytes: e.payload.len(),
                    entries: 1,
                });
                if work.bytes > scan_bytes || work.entries > scan_entries {
                    return Err(Error::ScanBudget);
                }
                return Ok((entry, work));
            }
            let segment = self
                .sealed
                .iter()
                .find(|s| s.descriptor.end > offset)
                .ok_or(Error::CorruptData)?;
            let cp = segment
                .checkpoints
                .iter()
                .rfind(|p| p.offset <= offset)
                .ok_or(Error::InvalidIndex)?;
            let mut cursor = journal::CheckedCursor::open(
                &self.data_path(segment.descriptor.base, segment.descriptor.generation),
                segment.descriptor.base,
                self.journal_limits,
            )?;
            if cursor.file_bytes() != segment.descriptor.bytes {
                return Err(Error::CorruptData);
            }
            cursor.seek(cp.position, cp.offset)?;
            let mut work = Work::default();
            loop {
                if work.entries >= scan_entries {
                    return Err(Error::ScanBudget);
                }
                let remaining = scan_bytes
                    .checked_sub(work.bytes)
                    .ok_or(Error::ScanBudget)?;
                let cap = remaining
                    .min(self.journal_limits.max_entry_bytes())
                    .checked_add(size_of::<journal::Entry>())
                    .ok_or(Error::ScanBudget)?;
                // Cursor allocation is bounded by the remaining scan work, not
                // the caller's output capacity. Exhaustion must not become a
                // successful short fetch after earlier entries were returned.
                let (_, entry) = cursor
                    .next(cap)
                    .map_err(|e| match e {
                        journal::Error::FetchBudgetExceeded => Error::ScanBudget,
                        other => Error::Storage(other),
                    })?
                    .ok_or(Error::CorruptData)?;
                work.bytes = work
                    .bytes
                    .checked_add(entry.payload.len())
                    .ok_or(Error::ScanBudget)?;
                work.entries += 1;
                let end = entry
                    .first_offset
                    .checked_add(u64::from(entry.record_count))
                    .ok_or(Error::CorruptData)?;
                if end > offset {
                    if entry.payload.len() + size_of::<journal::Entry>() > maximum {
                        return Err(journal::Error::FetchBudgetExceeded.into());
                    }
                    return Ok((Some(entry), work));
                }
            }
        })();
        if let Err(e) = &result {
            if !matches!(
                e,
                Error::ScanBudget
                    | Error::Storage(
                        journal::Error::FetchBudgetExceeded
                            | journal::Error::InvalidFetchLimits
                            | journal::Error::OffsetBeforeBase
                    )
            ) {
                self.poisoned = true;
            }
        }
        result
    }
    /// Fetch bounded containing/following whole atomic inputs across generations.
    /// Output charges Entry plus payload, as Journal does. Hidden checkpoint
    /// scans are also bounded; exhaustion is an error, never a successful miss.
    pub fn fetch(
        &mut self,
        offset: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<journal::Entry>, Error> {
        self.alive()?;
        if max_entries == 0
            || max_entries > self.fetch_entries
            || max_bytes == 0
            || max_bytes > self.journal_limits.max_fetch_bytes()
        {
            return Err(journal::Error::InvalidFetchLimits.into());
        }
        let mut out = Vec::new();
        out.try_reserve_exact(max_entries.min(max_bytes / (size_of::<journal::Entry>() + 1)))
            .map_err(|_| Error::AllocationFailed)?;
        let mut cursor = offset;
        let mut bytes = 0usize;
        let mut work = Work::default();
        while out.len() < max_entries {
            let remaining = max_bytes - bytes;
            let scan_bytes = (self.limits.scan_bytes as usize)
                .checked_sub(work.bytes)
                .ok_or(Error::ScanBudget)?;
            let scan_entries = self
                .fetch_entries
                .checked_sub(work.entries)
                .ok_or(Error::ScanBudget)?;
            let (entry, used) = match self.read_one(cursor, remaining, scan_bytes, scan_entries) {
                Err(Error::Storage(journal::Error::FetchBudgetExceeded)) if !out.is_empty() => {
                    break
                }
                other => other?,
            };
            work.bytes += used.bytes;
            work.entries += used.entries;
            let Some(entry) = entry else {
                break;
            };
            cursor = entry
                .first_offset
                .checked_add(u64::from(entry.record_count))
                .ok_or(Error::CorruptData)?;
            bytes += size_of::<journal::Entry>() + entry.payload.len();
            out.push(entry);
        }
        Ok(out)
    }
    fn check_guard(&self, guard: DeletionGuard) -> Result<u64, Error> {
        let floor = self.log_start_offset();
        let end = self.next_offset() as i64;
        if !(floor..=end).contains(&guard.confirmed_high_watermark)
            || !(floor..=end).contains(&guard.retain_from)
        {
            return Err(Error::InvalidRetentionBounds);
        }
        Ok(guard.confirmed_high_watermark.min(guard.retain_from) as u64)
    }
    /// Publish a monotonic logical start before unlinking whole sealed files.
    ///
    /// A containing batch/segment and the active journal remain physical. Reads
    /// below the new floor fail. Guards describe caller-confirmed safety only;
    /// ordinary RF1 owners may use their synchronized end for both bounds.
    /// A successful operation includes complete, directory-synchronized cleanup.
    /// Any ambiguous publication/deletion failure poisons this handle; reopening
    /// replays the bounded protected victim list. This explicitly migrates V1
    /// to V2; older V1 readers then reject the manifest rather than reset its floor.
    pub fn delete_records(
        &mut self,
        offset: i64,
        guard: DeletionGuard,
    ) -> Result<DeletionOutcome, Error> {
        self.alive()?;
        self.check_guard(guard)?;
        if offset < 0 || offset > guard.confirmed_high_watermark {
            return Err(Error::OffsetOutOfRange);
        }
        if offset > guard.retain_from {
            return Err(Error::ProtectedRecords);
        }
        let requested = (offset as u64).max(self.base_offset());
        let retire_active = requested == self.next_offset() && self.active()?.entry_count() != 0;
        self.publish_floor(requested, None, retire_active)
    }
    fn publish_floor(
        &mut self,
        requested: u64,
        victim_limit: Option<usize>,
        retire_active: bool,
    ) -> Result<DeletionOutcome, Error> {
        let count = self
            .sealed
            .iter()
            .take_while(|s| s.descriptor.end <= requested)
            .take(victim_limit.unwrap_or(usize::MAX))
            .count();
        if requested == self.base_offset()
            && !retire_active
            && (victim_limit.is_none() || count == 0)
        {
            return Ok(DeletionOutcome {
                log_start_offset: self.log_start_offset(),
                reclaimed_files: 0,
                reclaimed_bytes: 0,
            });
        }
        let revision = self
            .manifest
            .revision
            .checked_add(1)
            .ok_or(Error::GenerationOverflow)?;
        let mut victims = Vec::new();
        victims
            .try_reserve_exact(count + usize::from(retire_active))
            .map_err(|_| Error::AllocationFailed)?;
        victims.extend(self.manifest.sealed.iter().take(count).copied());
        // Reserve the initial temp manifest and the worst later clear-temp peak
        // before changing state. Victim files stay in disk_bytes until unlink.
        let new_size = RETENTION_HEADER
            + 4
            + (self.manifest.sealed.len() + usize::from(retire_active)) * DESCRIPTOR_BYTES;
        let extra = new_size
            .checked_add(new_size.saturating_sub(manifest_size(&self.manifest)))
            .and_then(|n| n.checked_add(if retire_active { 24 } else { 0 }))
            .ok_or(Error::DiskBudget)?;
        self.reserve_disk(extra as u64)?;
        let result = (|| {
            if retire_active {
                // Certify before authorizing deletion. Drop the dense old index
                // before opening its synchronized empty successor.
                drop(self.active.take());
                let old = self.scan(self.manifest.active_base, self.manifest.active_generation)?;
                if old.descriptor.end != requested || count != self.sealed.len() {
                    return Err(Error::CorruptData);
                }
                victims.push(old.descriptor);
                let (active, _) = journal::Journal::open(
                    self.data_path(requested, revision),
                    requested,
                    self.journal_limits,
                )?;
                self.hit(Phase::NewActiveSynced)?;
                self.active = Some(active);
                self.manifest.active_base = requested;
                self.manifest.active_generation = revision;
            }
            self.manifest.revision = revision;
            self.manifest.retention = true;
            self.manifest.floor = requested;
            self.manifest.physical_base = victims
                .last()
                .map_or(self.manifest.physical_base, |d| d.end);
            self.manifest.victims = victims;
            self.manifest.sealed.drain(..count);
            self.publish_manifest()?;
            self.sealed.drain(..count);
            if retire_active {
                self.active_checkpoints.clear();
                self.active_max = EMPTY_TIME;
            }
            let (reclaimed_files, reclaimed_bytes) = if self.manifest.victims.is_empty() {
                (0, 0)
            } else {
                self.finish_retention_cleanup()?
            };
            Ok(DeletionOutcome {
                log_start_offset: self.log_start_offset(),
                reclaimed_files,
                reclaimed_bytes,
            })
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    /// Select only an age/size-eligible safe sealed prefix, capped per invocation.
    ///
    /// Age is strict `now - segment_max_timestamp > retention_ms`, and requires
    /// a nonnegative verified maximum. Entirely negative/unknown-time segments
    /// are retained by age; unlike Apache, mutable file modification times do
    /// not supply a retention clock. Explicit deletion/size still apply. Size counts
    /// payload bytes, excluding journal and entry headers. Size selects a segment
    /// only when its removal leaves at least the configured target. Age OR size
    /// makes a prefix segment eligible; selection stops at a protected boundary
    /// or a first ineligible segment. A fully eligible active segment is retired
    /// to a synchronized empty successor, which is always retained. A protected
    /// boundary or sweep cap can prevent attaining the target.
    pub fn apply_retention(
        &mut self,
        now_ms: i64,
        policy: RetentionPolicy,
        guard: DeletionGuard,
    ) -> Result<DeletionOutcome, Error> {
        self.alive()?;
        let ceiling = self.check_guard(guard)?;
        if now_ms < 0 {
            return Err(Error::InvalidRetentionBounds);
        }
        let mut payload_bytes = self
            .active()?
            .file_bytes()
            .checked_sub(24 + self.active()?.entry_count() as u64 * 32)
            .ok_or(Error::CorruptData)?;
        for s in &self.sealed {
            payload_bytes = payload_bytes
                .checked_add(descriptor_payload(s.descriptor)?)
                .ok_or(Error::DiskBudget)?;
        }
        let mut target = self.base_offset();
        let mut selected = 0usize;
        for s in self.sealed.iter().take(policy.max_segments) {
            let d = s.descriptor;
            if d.end > ceiling {
                break;
            }
            let aged = age_eligible(now_ms, policy.retention_ms, d.max_time);
            let segment_bytes = descriptor_payload(d)?;
            let sized = policy
                .retention_bytes
                .is_some_and(|limit| payload_bytes.saturating_sub(segment_bytes) >= limit);
            if !aged && !sized {
                break;
            }
            payload_bytes -= segment_bytes;
            target = target.max(d.end);
            selected += 1;
        }
        let active_bytes =
            self.active()?.file_bytes() - 24 - self.active()?.entry_count() as u64 * 32;
        let retire_active = selected == self.sealed.len()
            && selected < policy.max_segments
            && self.active()?.entry_count() != 0
            && self.next_offset() <= ceiling
            && (age_eligible(now_ms, policy.retention_ms, self.active_max)
                || policy
                    .retention_bytes
                    .is_some_and(|limit| payload_bytes.saturating_sub(active_bytes) >= limit));
        if retire_active {
            target = self.next_offset();
        }
        // A later roll may seal an active file entirely below an already
        // acknowledged floor. A sweep can clean that prefix without moving it.
        self.publish_floor(target, Some(selected), retire_active)
    }
    fn finish_retention_cleanup(&mut self) -> Result<(usize, u64), Error> {
        let mut removed = 0usize;
        let mut bytes = 0u64;
        for n in 0..self.manifest.victims.len() {
            let d = self.manifest.victims[n];
            for (name, phase) in [
                (data_name(d.base, d.generation), Phase::RetentionDataRemoved),
                (
                    index_name(d.base, d.generation),
                    Phase::RetentionIndexRemoved,
                ),
            ] {
                let path = self.directory.join(name);
                match fs::symlink_metadata(&path) {
                    Ok(meta) => {
                        if !meta.file_type().is_file() {
                            return Err(Error::InvalidLayout);
                        }
                        bytes = bytes.checked_add(meta.len()).ok_or(Error::DiskBudget)?;
                        fs::remove_file(path)?;
                        removed += 1;
                        self.hit(phase)?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        File::open(&self.directory)?.sync_all()?;
        self.hit(Phase::RetentionCleanupSynced)?;
        self.manifest.victims.clear();
        self.publish_manifest_kind(true)?;
        Ok((removed, bytes))
    }
    /// Atomically replace one sealed generation with an identical checked copy.
    /// Both generations remain charged until durable manifest publication and
    /// cleanup. This removes no records and implements no compaction policy.
    pub fn replace_sealed(&mut self, base: u64) -> Result<(), Error> {
        self.alive()?;
        let position = self
            .sealed
            .iter()
            .position(|s| s.descriptor.base == base)
            .ok_or(Error::InvalidLayout)?;
        let old = self.sealed[position].descriptor;
        let revision = self
            .manifest
            .revision
            .checked_add(1)
            .ok_or(Error::GenerationOverflow)?;
        let extra = old
            .bytes
            .checked_add(
                (72 + self.sealed[position].checkpoints.len() * 24 + manifest_size(&self.manifest))
                    as u64,
            )
            .ok_or(Error::DiskBudget)?;
        self.reserve_disk(extra)?;
        let result = (|| {
            let mut source = journal::CheckedCursor::open(
                &self.data_path(base, old.generation),
                base,
                self.journal_limits,
            )?;
            let (mut destination, _) =
                journal::Journal::open(self.data_path(base, revision), base, self.journal_limits)?;
            while let Some((_, entry)) = source.next(self.journal_limits.max_fetch_bytes())? {
                destination.append(entry.record_count, &entry.payload)?;
                self.hit(Phase::CopyEntrySynced)?;
            }
            if destination.next_offset() != old.end
                || destination.file_bytes() != old.bytes
                || source.fingerprint() != old.fingerprint
            {
                return Err(Error::CorruptData);
            }
            drop(source);
            drop(destination);
            let replacement = self.scan(base, revision)?;
            let mut same = replacement.descriptor;
            same.generation = old.generation;
            if same != old {
                return Err(Error::CorruptData);
            }
            self.write_index(&replacement)?;
            self.manifest.revision = revision;
            self.manifest.sealed[position] = replacement.descriptor;
            self.publish_manifest()?;
            self.sealed[position] = replacement;
            fs::remove_file(self.data_path(base, old.generation))?;
            self.hit(Phase::OldDataRemoved)?;
            fs::remove_file(self.directory.join(index_name(base, old.generation)))?;
            File::open(&self.directory)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
}
fn age_eligible(now_ms: i64, retention_ms: Option<u64>, maximum: i64) -> bool {
    maximum >= 0
        && retention_ms.is_some_and(|ms| i128::from(now_ms) - i128::from(maximum) > i128::from(ms))
}
fn descriptor_payload(d: Descriptor) -> Result<u64, Error> {
    d.bytes
        .checked_sub(24 + d.entries * 32)
        .ok_or(Error::CorruptData)
}
fn time_checkpoint(checkpoints: &[Checkpoint], wanted: i64, base: u64) -> u64 {
    checkpoints
        .iter()
        .rfind(|p| p.prefix_max < wanted)
        .map_or(base, |p| p.offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    const BASIC: &[u8] = include_bytes!("../tests/fixtures/records/valid-basic.bin");
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> std::io::Result<Self> {
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "partitionline-segment-fault-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&p)?;
            Ok(Self(p))
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            drop(fs::remove_dir_all(&self.0));
        }
    }
    fn payload(n: u64) -> Vec<u8> {
        let mut b = BASIC.to_vec();
        b[..8].copy_from_slice(&n.to_be_bytes());
        b
    }
    fn open(path: &Path) -> Result<(Log, Recovery), Error> {
        Log::open(
            path,
            0,
            journal::Limits::new(1024, 8192, 16, 4096)?,
            records::Limits::default(),
            Limits::new(150, 16, 4, 2, 1024 * 1024, 65536, 4096)?,
        )
    }
    const PHASES: [Phase; 8] = [
        Phase::IndexWritten,
        Phase::IndexSynced,
        Phase::IndexRenamed,
        Phase::NewActiveSynced,
        Phase::ManifestWritten,
        Phase::ManifestSynced,
        Phase::ManifestRenamed,
        Phase::DirectorySynced,
    ];
    fn retain_fault_files(
        path: &Path,
        kind: &str,
        phase: Phase,
        stage: &str,
        acknowledged_end: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(root) = std::env::var_os("PARTITIONLINE_SEGMENTS_FAULT_DIR") else {
            return Ok(());
        };
        let out = PathBuf::from(root)
            .join(kind)
            .join(format!("{phase:?}"))
            .join(stage);
        fs::create_dir_all(&out)?;
        let mut count = 0;
        for item in fs::read_dir(path)? {
            let item = item?;
            count += 1;
            if count > 72 || !item.file_type()?.is_file() || item.metadata()?.len() > 32768 {
                return Err("bounded fault artifact".into());
            }
            fs::copy(item.path(), out.join(item.file_name()))?;
        }
        let receipt = format!(
            "{{\"schema\":1,\"kind\":\"{kind}\",\"phase\":\"{phase:?}\",\"stage\":\"{stage}\",\"acknowledged_end\":{acknowledged_end},\"physical_power_loss_claim\":false}}\n"
        );
        File::create(out.join("case.json"))?.write_all(receipt.as_bytes())?;
        Ok(())
    }
    #[test]
    fn every_roll_publication_failure_preserves_prior_acknowledgments(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for phase in PHASES {
            let temp = Temp::new()?;
            let path = temp.0.join("log");
            let (mut log, _) = open(&path)?;
            log.append(1, &payload(0))?;
            log.fault = Some((phase, false));
            assert!(log.append(1, &payload(1)).is_err(), "{phase:?}");
            assert!(log.is_poisoned());
            assert_eq!(log.next_offset(), 1);
            assert!(matches!(log.append(1, &payload(1)), Err(Error::Poisoned)));
            retain_fault_files(&path, "roll-error", phase, "interrupted", 1)?;
            drop(log);
            let (mut recovered, _) = open(&path)?;
            assert_eq!(recovered.next_offset(), 1);
            assert_eq!(recovered.fetch(0, 1, 4096)?[0].payload, payload(0));
            retain_fault_files(&path, "roll-error", phase, "recovered", 1)?;
            assert_eq!(recovered.append(1, &payload(1))?.first_offset, 1);
            println!("roll failure {phase:?}: acknowledged payload preserved");
        }
        Ok(())
    }
    #[test]
    fn replacement_publication_and_cleanup_failures_select_complete_generations(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for phase in [
            Phase::IndexWritten,
            Phase::IndexSynced,
            Phase::IndexRenamed,
            Phase::ManifestWritten,
            Phase::ManifestSynced,
            Phase::ManifestRenamed,
            Phase::DirectorySynced,
            Phase::OldDataRemoved,
        ] {
            let temp = Temp::new()?;
            let path = temp.0.join("log");
            let (mut log, _) = open(&path)?;
            log.append(1, &payload(0))?;
            log.append(1, &payload(1))?;
            log.fault = Some((phase, false));
            assert!(log.replace_sealed(0).is_err(), "{phase:?}");
            assert!(log.is_poisoned());
            assert_eq!(log.next_offset(), 2);
            retain_fault_files(&path, "replace-error", phase, "interrupted", 2)?;
            drop(log);
            let (mut recovered, _) = open(&path)?;
            let entries = recovered.fetch(0, 2, 4096)?;
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].payload, payload(0));
            assert_eq!(entries[1].payload, payload(1));
            retain_fault_files(&path, "replace-error", phase, "recovered", 2)?;
            println!("replacement failure {phase:?}: both acknowledged payloads preserved");
        }
        Ok(())
    }
    #[test]
    fn process_exit_at_each_roll_publication_stage_recovers_committed_payloads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(path) = std::env::var_os("PL_SEGMENT_CRASH_CHILD_PATH") {
            let phase = std::env::var("PL_SEGMENT_CRASH_CHILD_PHASE")?.parse::<usize>()?;
            let (mut log, _) = open(Path::new(&path))?;
            log.append(1, &payload(0))?;
            log.fault = Some((PHASES[phase], true));
            log.append(1, &payload(1))?;
            return Err("fault was not reached".into());
        }
        for (n, phase) in PHASES.iter().enumerate() {
            let temp = Temp::new()?;
            let path = temp.0.join("log");
            let child=std::process::Command::new(std::env::current_exe()?).args(["--exact","segments::tests::process_exit_at_each_roll_publication_stage_recovers_committed_payloads","--nocapture"]).env("PL_SEGMENT_CRASH_CHILD_PATH",&path).env("PL_SEGMENT_CRASH_CHILD_PHASE",n.to_string()).output()?;
            assert!(
                child.status.success(),
                "{phase:?}: {}",
                String::from_utf8_lossy(&child.stderr)
            );
            retain_fault_files(&path, "roll-exit", *phase, "interrupted", 1)?;
            let (mut log, _) = open(&path)?;
            assert_eq!(log.next_offset(), 1);
            assert_eq!(log.fetch(0, 1, 4096)?[0].payload, payload(0));
            retain_fault_files(&path, "roll-exit", *phase, "recovered", 1)?;
            println!("process exit {phase:?}: prior durable append recovered");
        }
        Ok(())
    }
    #[test]
    fn process_exit_during_partial_copy_and_each_replacement_publication_recovers_all_records(
    ) -> Result<(), Box<dyn std::error::Error>> {
        const PHASES: [Phase; 9] = [
            Phase::CopyEntrySynced,
            Phase::IndexWritten,
            Phase::IndexSynced,
            Phase::IndexRenamed,
            Phase::ManifestWritten,
            Phase::ManifestSynced,
            Phase::ManifestRenamed,
            Phase::DirectorySynced,
            Phase::OldDataRemoved,
        ];
        fn open_replacement(path: &Path) -> Result<(Log, Recovery), Error> {
            Log::open(
                path,
                0,
                journal::Limits::new(1024, 32768, 16, 4096)?,
                records::Limits::default(),
                Limits::new(10000, 16, 3, 2, 1024 * 1024, 65536, 32768)?,
            )
        }
        if let Some(path) = std::env::var_os("PL_SEGMENT_REPLACE_CHILD_PATH") {
            let phase = std::env::var("PL_SEGMENT_REPLACE_CHILD_PHASE")?.parse::<usize>()?;
            let (mut log, _) = open_replacement(Path::new(&path))?;
            for n in 0..4 {
                log.append(1, &payload(n))?;
            }
            log.fault = Some((PHASES[phase], true));
            log.replace_sealed(0)?;
            return Err("replacement fault was not reached".into());
        }
        for (n, phase) in PHASES.iter().enumerate() {
            let temp = Temp::new()?;
            let path = temp.0.join("log");
            let child=std::process::Command::new(std::env::current_exe()?).args(["--exact","segments::tests::process_exit_during_partial_copy_and_each_replacement_publication_recovers_all_records","--nocapture"]).env("PL_SEGMENT_REPLACE_CHILD_PATH",&path).env("PL_SEGMENT_REPLACE_CHILD_PHASE",n.to_string()).output()?;
            assert!(
                child.status.success(),
                "{phase:?}: {}",
                String::from_utf8_lossy(&child.stderr)
            );
            retain_fault_files(&path, "replace-exit", *phase, "interrupted", 4)?;
            let (mut log, _) = open_replacement(&path)?;
            assert_eq!(log.next_offset(), 4);
            let entries = log.fetch(0, 4, 4096)?;
            assert_eq!(entries.len(), 4);
            for (n, e) in entries.iter().enumerate() {
                assert_eq!(e.payload, payload(n as u64));
            }
            retain_fault_files(&path, "replace-exit", *phase, "recovered", 4)?;
            println!("replacement process exit {phase:?}: all4 durable payloads recovered");
        }
        Ok(())
    }

    const RETENTION_PHASES: [Phase; 12] = [
        Phase::NewActiveSynced,
        Phase::ManifestWritten,
        Phase::ManifestSynced,
        Phase::ManifestRenamed,
        Phase::DirectorySynced,
        Phase::RetentionDataRemoved,
        Phase::RetentionIndexRemoved,
        Phase::RetentionCleanupSynced,
        Phase::RetentionClearWritten,
        Phase::RetentionClearSynced,
        Phase::RetentionClearRenamed,
        Phase::RetentionClearDirectorySynced,
    ];
    fn retention_seed(path: &Path) -> Result<Log, Error> {
        let (mut log, _) = open(path)?;
        for n in 0..5 {
            log.append(1, &payload(n))?;
        }
        log.delete_records(1, DeletionGuard::new(5, 5)?)?;
        Ok(log)
    }
    fn expected_retention_floor(phase: Phase, target: i64) -> i64 {
        if matches!(
            phase,
            Phase::NewActiveSynced | Phase::ManifestWritten | Phase::ManifestSynced
        ) {
            1
        } else {
            target
        }
    }
    fn retain_retention_files(
        path: &Path,
        kind: &str,
        phase: Phase,
        stage: &str,
        target: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(root) = std::env::var_os("PARTITIONLINE_RETENTION_FAULT_DIR") else {
            return Ok(());
        };
        let out = PathBuf::from(root)
            .join(kind)
            .join(target.to_string())
            .join(format!("{phase:?}"))
            .join(stage);
        fs::create_dir_all(&out)?;
        let mut count = 0usize;
        for item in fs::read_dir(path)? {
            let item = item?;
            count += 1;
            if count > 72 || !item.file_type()?.is_file() || item.metadata()?.len() > 32768 {
                return Err("bounded retention capture".into());
            }
            fs::copy(item.path(), out.join(item.file_name()))?;
        }
        let expected = expected_retention_floor(phase, target);
        let receipt = format!("{{\"schema\":1,\"kind\":\"{kind}\",\"phase\":\"{phase:?}\",\"stage\":\"{stage}\",\"previous_acknowledged_floor\":1,\"requested_floor\":{target},\"expected_recovered_floor\":{expected},\"durable_end\":5,\"confirmed_high_watermark\":5,\"retain_from\":5,\"seed\":\"valid-basic.bin assigned to offsets 0..4, prior acknowledged deletion through 1\",\"physical_power_loss_claim\":false}}\n");
        File::create(out.join("case.json"))?.write_all(receipt.as_bytes())?;
        Ok(())
    }
    fn verify_retention_recovery(path: &Path, phase: Phase, target: i64) -> Result<Log, Error> {
        let (mut log, recovery) = open(path)?;
        let expected = expected_retention_floor(phase, target);
        assert_eq!(log.log_start_offset(), expected, "{phase:?}/target{target}");
        assert_eq!(recovery.log_start_offset, expected as u64);
        assert_eq!(log.next_offset(), 5);
        for n in expected..5 {
            assert_eq!(log.fetch(n as u64, 1, 4096)?[0].payload, payload(n as u64));
        }
        assert!(matches!(
            log.fetch(expected as u64 - 1, 1, 4096),
            Err(Error::Storage(journal::Error::OffsetBeforeBase))
        ));
        assert!(log.manifest.victims.is_empty());
        Ok(log)
    }
    #[test]
    fn retention_publication_partial_unlink_and_active_retirement_failures_preserve_acknowledged_floor(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for target in [3, 5] {
            for phase in RETENTION_PHASES {
                if target == 3 && phase == Phase::NewActiveSynced {
                    continue;
                }
                let temp = Temp::new()?;
                let path = temp.0.join("log");
                let mut log = retention_seed(&path)?;
                let before_disk = log.disk_bytes()?;
                log.fault = Some((phase, false));
                assert!(
                    log.delete_records(target, DeletionGuard::new(5, 5)?)
                        .is_err(),
                    "{phase:?}/{target}"
                );
                assert!(log.is_poisoned());
                assert_eq!(log.next_offset(), 5);
                if matches!(phase, Phase::ManifestWritten | Phase::ManifestSynced) {
                    assert!(log.disk_bytes()? >= before_disk);
                }
                assert!(matches!(
                    log.delete_records(target, DeletionGuard::new(5, 5)?),
                    Err(Error::Poisoned)
                ));
                retain_retention_files(&path, "retention-error", phase, "interrupted", target)?;
                drop(log);
                let mut log = verify_retention_recovery(&path, phase, target)?;
                retain_retention_files(&path, "retention-error", phase, "recovered", target)?;
                log.delete_records(target, DeletionGuard::new(5, 5)?)?;
                assert_eq!(log.log_start_offset(), target);
                assert_eq!(log.append(1, &payload(5))?.first_offset, 5);
                println!(
                    "retention error {phase:?}/target{target}: floor and surviving bytes recovered"
                );
            }
        }
        Ok(())
    }
    #[test]
    fn process_exit_at_retention_publication_partial_unlink_and_active_retirement_recovers_monotonic_floor(
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(path) = std::env::var_os("PL_RETENTION_CRASH_PATH") {
            let phase = std::env::var("PL_RETENTION_CRASH_PHASE")?.parse::<usize>()?;
            let target = std::env::var("PL_RETENTION_CRASH_TARGET")?.parse::<i64>()?;
            let mut log = retention_seed(Path::new(&path))?;
            log.fault = Some((RETENTION_PHASES[phase], true));
            log.delete_records(target, DeletionGuard::new(5, 5)?)?;
            return Err("retention fault was not reached".into());
        }
        for target in [3, 5] {
            for (n, phase) in RETENTION_PHASES.iter().enumerate() {
                if target == 3 && *phase == Phase::NewActiveSynced {
                    continue;
                }
                let temp = Temp::new()?;
                let path = temp.0.join("log");
                let child = std::process::Command::new(std::env::current_exe()?)
                    .args(["--exact", "segments::tests::process_exit_at_retention_publication_partial_unlink_and_active_retirement_recovers_monotonic_floor", "--nocapture"])
                    .env("PL_RETENTION_CRASH_PATH", &path).env("PL_RETENTION_CRASH_PHASE", n.to_string()).env("PL_RETENTION_CRASH_TARGET", target.to_string()).output()?;
                assert!(
                    child.status.success(),
                    "{phase:?}/{target}: {}",
                    String::from_utf8_lossy(&child.stderr)
                );
                retain_retention_files(&path, "retention-exit", *phase, "interrupted", target)?;
                let mut log = verify_retention_recovery(&path, *phase, target)?;
                retain_retention_files(&path, "retention-exit", *phase, "recovered", target)?;
                log.delete_records(target, DeletionGuard::new(5, 5)?)?;
                assert_eq!(log.log_start_offset(), target);
                assert_eq!(log.append(1, &payload(5))?.first_offset, 5);
                println!(
                    "retention process exit {phase:?}/target{target}: acknowledged floor preserved"
                );
            }
        }
        Ok(())
    }
    #[test]
    fn checksum_valid_impossible_victim_descriptor_is_rejected_before_unlink(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp = Temp::new()?;
        let path = temp.0.join("log");
        let mut log = retention_seed(&path)?;
        log.fault = Some((Phase::ManifestRenamed, false));
        assert!(log.delete_records(3, DeletionGuard::new(5, 5)?).is_err());
        drop(log);
        let manifest = path.join(MANIFEST);
        let mut bytes = read_bounded(&manifest, 4096)?;
        let selected = count(&bytes, 56)? as usize;
        let victim = RETENTION_HEADER + selected * DESCRIPTOR_BYTES;
        // A nonempty journal cannot have a header-only length. Recompute the
        // checksum to exercise structural victim validation rather than CRC.
        bytes[victim + 24..victim + 32].copy_from_slice(&24u64.to_be_bytes());
        let end = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[..end]);
        bytes[end..].copy_from_slice(&crc.to_be_bytes());
        File::create(&manifest)?.write_all(&bytes)?;
        let mut before = Vec::new();
        for item in fs::read_dir(&path)? {
            before.push(item?.file_name());
        }
        before.sort();
        assert!(matches!(open(&path), Err(Error::InvalidLayout)));
        let mut after = Vec::new();
        for item in fs::read_dir(&path)? {
            after.push(item?.file_name());
        }
        after.sort();
        assert_eq!(before, after);
        assert_eq!(read_bounded(&manifest, 4096)?, bytes);
        Ok(())
    }
    #[test]
    fn failed_floor_getter_reports_confirmed_state_and_reopen_never_decreases(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for target in [3, 5] {
            for phase in RETENTION_PHASES {
                if target == 3 && phase == Phase::NewActiveSynced {
                    continue;
                }
                let temp = Temp::new()?;
                let path = temp.0.join("log");
                let mut log = retention_seed(&path)?;
                log.fault = Some((phase, false));
                assert!(log
                    .delete_records(target, DeletionGuard::new(5, 5)?)
                    .is_err());
                assert!(log.is_poisoned());
                let reported = log.log_start_offset();
                let expected = if matches!(
                    phase,
                    Phase::NewActiveSynced
                        | Phase::ManifestWritten
                        | Phase::ManifestSynced
                        | Phase::ManifestRenamed
                        | Phase::DirectorySynced
                ) {
                    1
                } else {
                    target
                };
                assert_eq!(
                    reported, expected,
                    "prospective floor leaked at {phase:?}/target{target}"
                );
                assert_eq!(log.base_offset(), expected as u64);
                drop(log);
                let recovered = verify_retention_recovery(&path, phase, target)?;
                assert!(
                    recovered.log_start_offset() >= reported,
                    "floor decreased at {phase:?}/target{target}"
                );
            }
        }
        Ok(())
    }
}
