//! Bounded rolling ordinary-record journals and verified offset/time checkpoints.
//!
//! Dense data uses `PLJRNL01`; explicit ordinary cleaning selects immutable
//! `PLSPRS01` generations with positive logical entry spans and possibly zero
//! payloads. These are local formats, not Kafka's disk format. One manifest selects
//! contiguous sealed generations and one active journal. Derived seek bundles
//! are rejected and rebuilt from verified records. Publication synchronizes
//! files, atomically renames the manifest, then synchronizes its directory;
//! ambiguous failures poison the handle. Run exclusively on a blocking storage
//! owner. Explicit retention publishes a version-two manifest containing a
//! monotonic logical floor and cleanup victims before unlinking files. Opening
//! or appending a version-one log never migrates it. Cleaning explicitly selects
//! V3, whose obsolete changed generations are separate from retention victims;
//! older readers fail closed. No background cleaner, transaction/control or
//! producer-state cleaning is qualified. Process-local ownership is
//! not cross-process locking; no replication or physical power-loss claim.

use crate::{compaction, journal, records};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};

const MANIFEST: &str = "manifest";
const MANIFEST_TEMP: &str = "manifest.tmp";
const MANIFEST_MAGIC: &[u8; 8] = b"PLSEGM01";
const RETENTION_MAGIC: &[u8; 8] = b"PLSEGM02";
const COMPACTION_MAGIC: &[u8; 8] = b"PLSEGM03";
const SPARSE_MAGIC: &[u8; 8] = b"PLSPRS01";
const INDEX_MAGIC: &[u8; 8] = b"PLSEEK01";
const SPARSE_INDEX_MAGIC: &[u8; 8] = b"PLSEEK02";
const MANIFEST_HEADER: usize = 44;
const RETENTION_HEADER: usize = 64;
const COMPACTION_HEADER: usize = 72;
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
    /// Additional V3 obsolete-descriptor, simultaneous replacement-checkpoint
    /// and bounded directory/manifest capacities. Legacy constructors stay dense.
    pub fn compaction_index_envelope(self) -> Result<usize, Error> {
        self.index_envelope()?
            .checked_add(
                self.max_segments
                    .checked_mul(
                        size_of::<Descriptor>() * 3
                            + DESCRIPTOR_BYTES * 4
                            + self.max_entries.div_ceil(self.interval) * size_of::<Checkpoint>()
                            + (size_of::<String>() + 42) * 4,
                    )
                    .ok_or(Error::InvalidLimits)?,
            )
            .ok_or(Error::InvalidLimits)
    }
    /// Conservative maximum operation scratch, including the configured output
    /// arena, map/keys, one source payload and replacement certification buffers.
    /// The compactor deducts all of these from this same configured ceiling.
    pub fn compaction_scratch_bytes(
        self,
        policy: compaction::Limits,
        source: journal::Limits,
    ) -> Result<usize, Error> {
        let fixed = self.compaction_fixed_scratch(source)?;
        if fixed >= policy.scratch_bytes() {
            return Err(Error::InvalidLimits);
        }
        Ok(policy.scratch_bytes())
    }
    fn compaction_fixed_scratch(self, source: journal::Limits) -> Result<usize, Error> {
        self.max_segments
            .checked_mul(
                size_of::<Segment>()
                    + size_of::<Descriptor>() * 2
                    + self.max_entries.div_ceil(self.interval) * size_of::<Checkpoint>()
                    + DESCRIPTOR_BYTES * 4,
            )
            .and_then(|n| n.checked_add(source.max_entry_bytes().checked_mul(2)?))
            .and_then(|n| n.checked_add(8192))
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
    /// Ordinary cleaning policy, map, encoded output or work failed preflight.
    Compaction(compaction::Error),
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
    kind: u32,
}
struct Segment {
    descriptor: Descriptor,
    checkpoints: Vec<Checkpoint>,
}
struct PreparedSegment {
    old: Descriptor,
    entries: Vec<journal::Entry>,
    bytes: u64,
}
struct Manifest {
    revision: u64,
    base: u64,
    physical_base: u64,
    floor: u64,
    retention: bool,
    compacted: bool,
    active_base: u64,
    active_generation: u64,
    sealed: Vec<Descriptor>,
    victims: Vec<Descriptor>,
    obsolete: Vec<Descriptor>,
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
    committed_floor: u64,
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
    out.extend_from_slice(&d.kind.to_be_bytes());
}
fn parse_descriptor(bytes: &[u8], at: usize) -> Result<Descriptor, Error> {
    let kind = count(bytes, at + 52)?;
    if kind > 1 {
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
        kind,
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
    if m.retention || m.compacted {
        out.extend_from_slice(if m.compacted {
            COMPACTION_MAGIC
        } else {
            RETENTION_MAGIC
        });
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
        if m.compacted {
            out.extend_from_slice(&(m.obsolete.len() as u32).to_be_bytes());
            out.extend_from_slice(&0u32.to_be_bytes());
        }
    } else {
        out.extend_from_slice(MANIFEST_MAGIC);
        for n in [m.revision, m.base, m.active_base, m.active_generation] {
            out.extend_from_slice(&n.to_be_bytes());
        }
        out.extend_from_slice(&(m.sealed.len() as u32).to_be_bytes());
    }
    for d in m.sealed.iter().chain(&m.victims).chain(&m.obsolete) {
        put_descriptor(&mut out, *d);
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}
fn manifest_size(m: &Manifest) -> usize {
    (if m.compacted {
        COMPACTION_HEADER
    } else if m.retention {
        RETENTION_HEADER
    } else {
        MANIFEST_HEADER
    }) + (m.sealed.len() + m.victims.len() + m.obsolete.len()) * DESCRIPTOR_BYTES
        + 4
}
fn bundle_bytes(segment: &Segment) -> Result<Vec<u8>, Error> {
    let mut out = buffer(72 + segment.checkpoints.len() * CHECKPOINT_BYTES)?;
    out.extend_from_slice(if segment.descriptor.kind == 0 {
        INDEX_MAGIC
    } else {
        SPARSE_INDEX_MAGIC
    });
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
    let compacted = bytes.get(..8) == Some(COMPACTION_MAGIC);
    let retention = bytes.get(..8) == Some(RETENTION_MAGIC) || compacted;
    let header = if compacted {
        COMPACTION_HEADER
    } else if retention {
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
    let obsolete = if compacted {
        if count(bytes, 68)? != 0 || limits.compaction_index_envelope()? > limits.index_bytes {
            return Err(Error::InvalidLayout);
        }
        count(bytes, 64)? as usize
    } else {
        0
    };
    if n >= limits.max_segments
        || n.checked_add(victims)
            .is_none_or(|n| n > limits.max_segments)
        || obsolete > n
        || bytes.len() != header + 4 + (n + victims + obsolete) * DESCRIPTOR_BYTES
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
        compacted,
        active_base: number(bytes, if retention { 40 } else { 24 })?,
        active_generation: number(bytes, if retention { 48 } else { 32 })?,
        sealed: Vec::new(),
        victims: Vec::new(),
        obsolete: Vec::new(),
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
    m.obsolete
        .try_reserve_exact(obsolete)
        .map_err(|_| Error::AllocationFailed)?;
    let mut next = m.physical_base;
    for at in (header..header + n * DESCRIPTOR_BYTES).step_by(DESCRIPTOR_BYTES) {
        let d = parse_descriptor(bytes, at)?;
        if d.base != next || (!compacted && d.kind != 0) || !valid_descriptor(d, limits, m.revision)
        {
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
            || (!compacted && d.kind != 0)
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
    let mut previous = None;
    for at in (header + (n + victims) * DESCRIPTOR_BYTES
        ..header + (n + victims + obsolete) * DESCRIPTOR_BYTES)
        .step_by(DESCRIPTOR_BYTES)
    {
        let d = parse_descriptor(bytes, at)?;
        let selected = m
            .sealed
            .iter()
            .find(|s| s.base == d.base)
            .ok_or(Error::InvalidLayout)?;
        if !valid_descriptor(d, limits, m.revision)
            || d.end != selected.end
            || d.entries != selected.entries
            || d.generation >= selected.generation
            || selected.generation != m.revision
            || previous.is_some_and(|end| d.base < end)
        {
            return Err(Error::InvalidLayout);
        }
        previous = Some(d.end);
        m.obsolete.push(d);
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
        && d.kind <= 1
        && d.bytes >= 24 + d.entries * if d.kind == 0 { 33 } else { 32 }
}
// Immutable sparse generations have a distinct magic. The enclosing entry's
// positive count is its original logical span, independently of actual records.
// Unlike Journal recovery, no incomplete sparse tail is repaired.
struct SparseCursor {
    file: File,
    _ownership: journal::DirectoryOwnership,
    limits: journal::Limits,
    bytes: u64,
    position: u64,
    next: u64,
    fingerprint: u32,
}
impl SparseCursor {
    fn open(path: &Path, base: u64, limits: journal::Limits) -> Result<Self, Error> {
        if !fs::symlink_metadata(path)?.file_type().is_file() {
            return Err(Error::InvalidLayout);
        }
        let mut ownership = journal::DirectoryOwnership::acquire(path)?;
        let mut file = File::open(path)?;
        ownership.identify(&file)?;
        let bytes = file.metadata()?.len();
        if bytes < 24 || bytes > limits.max_file_bytes() {
            return Err(Error::CorruptData);
        }
        let mut header = [0; 24];
        file.read_exact(&mut header)?;
        if &header[..8] != SPARSE_MAGIC
            || number(&header, 8)? != base
            || count(&header, 16)? != 0
            || count(&header, 20)? != crc32c::crc32c(&header[..20])
        {
            return Err(Error::CorruptData);
        }
        Ok(Self {
            file,
            _ownership: ownership,
            limits,
            bytes,
            position: 24,
            next: base,
            fingerprint: crc32c::crc32c(&header),
        })
    }
    fn seek(&mut self, position: u64, offset: u64) -> Result<(), Error> {
        if !(24..=self.bytes).contains(&position) {
            return Err(Error::InvalidIndex);
        }
        self.file.seek(SeekFrom::Start(position))?;
        self.position = position;
        self.next = offset;
        Ok(())
    }
    fn next(&mut self, maximum: usize) -> Result<Option<(u64, journal::Entry)>, Error> {
        if self.file.metadata()?.len() != self.bytes {
            return Err(Error::CorruptData);
        }
        if self.position == self.bytes {
            return Ok(None);
        }
        if self.bytes - self.position < 32 {
            return Err(Error::CorruptData);
        }
        let position = self.position;
        let mut header = [0; 32];
        self.file.read_exact(&mut header)?;
        let length = count(&header, 8)? as usize;
        let first = number(&header, 12)?;
        let span = count(&header, 20)?;
        let end = first
            .checked_add(u64::from(span))
            .ok_or(Error::CorruptData)?;
        if &header[..8] != b"PLENTRY1"
            || count(&header, 28)? != crc32c::crc32c(&header[..28])
            || first != self.next
            || span == 0
            || i64::try_from(end).is_err()
            || length > self.limits.max_entry_bytes()
            || position
                .checked_add(32 + length as u64)
                .is_none_or(|n| n > self.bytes)
        {
            return Err(Error::CorruptData);
        }
        let charged = length
            .checked_add(size_of::<journal::Entry>())
            .ok_or(Error::ScanBudget)?;
        if charged > maximum || charged > self.limits.max_fetch_bytes() {
            return Err(journal::Error::FetchBudgetExceeded.into());
        }
        let mut payload = buffer(length)?;
        payload.resize(length, 0);
        self.file.read_exact(&mut payload)?;
        if crc32c::crc32c(&payload) != count(&header, 24)?
            || self.file.metadata()?.len() != self.bytes
        {
            return Err(Error::CorruptData);
        }
        self.fingerprint = crc32c::crc32c_append(self.fingerprint, &header);
        self.fingerprint = crc32c::crc32c_append(self.fingerprint, &payload);
        self.position = position + 32 + length as u64;
        self.next = end;
        Ok(Some((
            position,
            journal::Entry {
                first_offset: first,
                record_count: span,
                payload,
            },
        )))
    }
}
enum StoredCursor {
    Dense(journal::CheckedCursor),
    Sparse(SparseCursor),
}
impl StoredCursor {
    fn open(path: &Path, base: u64, limits: journal::Limits, kind: u32) -> Result<Self, Error> {
        match kind {
            0 => Ok(Self::Dense(journal::CheckedCursor::open(
                path, base, limits,
            )?)),
            1 => Ok(Self::Sparse(SparseCursor::open(path, base, limits)?)),
            _ => Err(Error::InvalidLayout),
        }
    }
    fn seek(&mut self, p: u64, o: u64) -> Result<(), Error> {
        match self {
            Self::Dense(c) => Ok(c.seek(p, o)?),
            Self::Sparse(c) => c.seek(p, o),
        }
    }
    fn next(&mut self, n: usize) -> Result<Option<(u64, journal::Entry)>, Error> {
        let result = match self {
            Self::Dense(c) => Ok(c.next(n)?),
            Self::Sparse(c) => c.next(n),
        };
        #[cfg(test)]
        if let Ok(Some((_, entry))) = &result {
            COMPACTION_LOADED.with(|n| n.set(n.get() + entry.payload.len()));
        }
        result
    }
    fn file_bytes(&self) -> u64 {
        match self {
            Self::Dense(c) => c.file_bytes(),
            Self::Sparse(c) => c.bytes,
        }
    }
    fn next_offset(&self) -> u64 {
        match self {
            Self::Dense(c) => c.next_offset(),
            Self::Sparse(c) => c.next,
        }
    }
    fn fingerprint(&self) -> u32 {
        match self {
            Self::Dense(c) => c.fingerprint(),
            Self::Sparse(c) => c.fingerprint,
        }
    }
    fn synchronize(&self) -> Result<(), Error> {
        match self {
            Self::Dense(c) => Ok(c.synchronize()?),
            Self::Sparse(c) => Ok(c.file.sync_data()?),
        }
    }
}
#[cfg(test)]
thread_local! { static COMPACTION_LOADED:std::cell::Cell<usize>=const { std::cell::Cell::new(0) }; }
struct SparseWriter {
    file: File,
    next: u64,
    bytes: u64,
    entries: usize,
    limits: journal::Limits,
}
impl SparseWriter {
    fn new(path: &Path, base: u64, limits: journal::Limits) -> Result<Self, Error> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut header = [0; 24];
        header[..8].copy_from_slice(SPARSE_MAGIC);
        header[8..16].copy_from_slice(&base.to_be_bytes());
        let crc = crc32c::crc32c(&header[..20]);
        header[20..24].copy_from_slice(&crc.to_be_bytes());
        file.write_all(&header)?;
        file.sync_all()?;
        Ok(Self {
            file,
            next: base,
            bytes: 24,
            entries: 0,
            limits,
        })
    }
    fn append(&mut self, entry: &journal::Entry) -> Result<(), Error> {
        let end = entry
            .first_offset
            .checked_add(u64::from(entry.record_count))
            .ok_or(Error::CorruptData)?;
        let bytes = self
            .bytes
            .checked_add(32 + entry.payload.len() as u64)
            .ok_or(Error::DiskBudget)?;
        if entry.first_offset != self.next
            || entry.record_count == 0
            || i64::try_from(end).is_err()
            || entry.payload.len() > self.limits.max_entry_bytes()
            || bytes > self.limits.max_file_bytes()
            || self.entries >= self.limits.max_index_entries()
        {
            return Err(Error::CorruptData);
        }
        let mut header = [0; 32];
        header[..8].copy_from_slice(b"PLENTRY1");
        header[8..12].copy_from_slice(&(entry.payload.len() as u32).to_be_bytes());
        header[12..20].copy_from_slice(&entry.first_offset.to_be_bytes());
        header[20..24].copy_from_slice(&entry.record_count.to_be_bytes());
        header[24..28].copy_from_slice(&crc32c::crc32c(&entry.payload).to_be_bytes());
        let crc = crc32c::crc32c(&header[..28]);
        header[28..32].copy_from_slice(&crc.to_be_bytes());
        self.file.write_all(&header)?;
        self.file.write_all(&entry.payload)?;
        self.file.sync_data()?;
        self.next = end;
        self.bytes = bytes;
        self.entries += 1;
        Ok(())
    }
}
enum StoredWriter {
    Dense(journal::Journal),
    Sparse(SparseWriter),
}
impl StoredWriter {
    fn new(path: &Path, base: u64, limits: journal::Limits, kind: u32) -> Result<Self, Error> {
        match kind {
            0 => Ok(Self::Dense(journal::Journal::open(path, base, limits)?.0)),
            1 => Ok(Self::Sparse(SparseWriter::new(path, base, limits)?)),
            _ => Err(Error::InvalidLayout),
        }
    }
    fn append(&mut self, entry: &journal::Entry) -> Result<(), Error> {
        match self {
            Self::Dense(w) => {
                w.append(entry.record_count, &entry.payload)?;
                Ok(())
            }
            Self::Sparse(w) => w.append(entry),
        }
    }
    fn next_offset(&self) -> u64 {
        match self {
            Self::Dense(w) => w.next_offset(),
            Self::Sparse(w) => w.next,
        }
    }
    fn file_bytes(&self) -> u64 {
        match self {
            Self::Dense(w) => w.file_bytes(),
            Self::Sparse(w) => w.bytes,
        }
    }
}
fn summarize_kind(
    entry: &journal::Entry,
    limits: records::Limits,
    kind: u32,
) -> Result<i64, Error> {
    if kind == 0 {
        return summarize(entry, limits);
    }
    let checked = records::validate_read(&entry.payload, limits).map_err(|_| Error::CorruptData)?;
    let first = i64::try_from(entry.first_offset).map_err(|_| Error::CorruptData)?;
    let end = entry
        .first_offset
        .checked_add(u64::from(entry.record_count))
        .and_then(|n| i64::try_from(n).ok())
        .ok_or(Error::CorruptData)?;
    let mut maximum = EMPTY_TIME;
    if entry.record_count == 0 {
        return Err(Error::CorruptData);
    }
    for batch in checked.batches() {
        let batch = batch.map_err(|_| Error::CorruptData)?;
        if batch.base_offset < first || batch.next_offset > end {
            return Err(Error::CorruptData);
        }
        // Authentic terminal empty headers carry their original maximum. It is
        // a conservative seek/age hint, never a retained timestamp match; the
        // record iterator remains empty. Zero-payload entries have no such hint.
        maximum = maximum.max(batch.max_timestamp);
    }
    Ok(maximum)
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
    CompactionDataRemoved,
    CompactionIndexRemoved,
    CompactionCleanupSynced,
    CompactionHeaderSynced,
    CompactionEntrySynced,
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
                    COMPACTION_HEADER + 4 + limits.max_segments * DESCRIPTOR_BYTES * 2,
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
                compacted: false,
                active_base: base,
                active_generation: 0,
                sealed,
                victims: Vec::new(),
                obsolete: Vec::new(),
            }
        };
        let mut sealed = Vec::new();
        sealed
            .try_reserve_exact(limits.max_segments)
            .map_err(|_| Error::AllocationFailed)?;
        let committed_floor = manifest.floor;
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
            committed_floor,
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
            let segment = result.scan_kind(d.base, d.generation, d.kind)?;
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
        if !result.manifest.obsolete.is_empty() {
            let (removed, _) = result.finish_obsolete_cleanup()?;
            recovery.removed_staging_files += removed;
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
    /// Confirmed monotonic floor, possibly inside the first physical input.
    /// Failed publication does not expose its prospective boundary.
    pub fn base_offset(&self) -> u64 {
        self.committed_floor
    }
    /// Kafka signed-domain confirmed start; opening validated its range.
    pub fn log_start_offset(&self) -> i64 {
        self.committed_floor as i64
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
            + self.manifest.obsolete.capacity() * size_of::<Descriptor>()
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
        self.scan_kind(base, generation, 0)
    }
    fn scan_kind(&self, base: u64, generation: u64, kind: u32) -> Result<Segment, Error> {
        let mut cursor = StoredCursor::open(
            &self.data_path(base, generation),
            base,
            self.journal_limits,
            kind,
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
            maximum = maximum.max(summarize_kind(&entry, self.records, kind)?);
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
                kind,
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
            for d in self.manifest.victims.iter().chain(&self.manifest.obsolete) {
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
                        let retired = !self.manifest.compacted
                            && self
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
            let mut cursor = StoredCursor::open(
                &self.data_path(segment.descriptor.base, segment.descriptor.generation),
                segment.descriptor.base,
                self.journal_limits,
                segment.descriptor.kind,
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
                        Error::Storage(journal::Error::FetchBudgetExceeded) => Error::ScanBudget,
                        other => other,
                    })?
                    .ok_or(Error::CorruptData)?;
                summarize_kind(&entry, self.records, segment.descriptor.kind)?;
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
        let new_size = (if self.manifest.compacted {
            COMPACTION_HEADER
        } else {
            RETENTION_HEADER
        }) + 4
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
            self.committed_floor = requested;
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
    // The V3 list authorizes changed generations only; V1/V2 prefix victims are
    // deliberately separate. Called only after all selected survivors validate.
    fn finish_obsolete_cleanup(&mut self) -> Result<(usize, u64), Error> {
        let (mut removed, mut bytes) = (0usize, 0u64);
        for n in 0..self.manifest.obsolete.len() {
            let d = self.manifest.obsolete[n];
            for (name, phase) in [
                (
                    data_name(d.base, d.generation),
                    Phase::CompactionDataRemoved,
                ),
                (
                    index_name(d.base, d.generation),
                    Phase::CompactionIndexRemoved,
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
        self.hit(Phase::CompactionCleanupSynced)?;
        self.manifest.obsolete.clear();
        self.publish_manifest_kind(true)?;
        Ok((removed, bytes))
    }
    pub(crate) fn sparse_at(&self, offset: u64) -> bool {
        self.manifest.compacted
            && self.sealed.iter().any(|s| {
                s.descriptor.base <= offset && offset < s.descriptor.end && s.descriptor.kind == 1
            })
    }
    /// Clean the complete eligible sealed prefix atomically. A bounded key map
    /// and all encoded replacements are prepared and validated before creating
    /// files. V3 separates changed generations from V2 retention-prefix victims.
    /// Null keys are discarded, last keyed values retained, and tombstones use
    /// authentic ordinary delete horizons; active/protected suffixes are untouched.
    /// The original positive atomic extents survive even with zero payload bytes.
    pub fn compact(
        &mut self,
        now_ms: i64,
        policy: compaction::Policy,
        guard: DeletionGuard,
    ) -> Result<compaction::Outcome, Error> {
        self.alive()?;
        let ceiling = self.check_guard(guard)?.min(self.manifest.active_base);
        let count = self
            .sealed
            .iter()
            .take_while(|s| s.descriptor.end <= ceiling)
            .count();
        let start = self
            .sealed
            .first()
            .map_or(self.manifest.active_base, |s| s.descriptor.base);
        let end = count
            .checked_sub(1)
            .map_or(start, |n| self.sealed[n].descriptor.end);
        let mut outcome = compaction::Outcome {
            start_offset: start as i64,
            end_offset: end as i64,
            ..compaction::Outcome::default()
        };
        if now_ms < 0
            || now_ms
                .checked_add(policy.delete_retention_ms() as i64)
                .is_none()
        {
            return Err(Error::Compaction(compaction::Error::InvalidClock));
        }
        if count == 0 {
            return Ok(outcome);
        }
        if count > policy.limits().max_segments {
            return Err(Error::Compaction(compaction::Error::BudgetExceeded(
                compaction::Budget::Segments,
            )));
        }
        if self.limits.compaction_index_envelope()? > self.limits.index_bytes {
            return Err(Error::InvalidLimits);
        }
        self.limits
            .compaction_scratch_bytes(policy.limits(), self.journal_limits)?;
        let revision = self
            .manifest
            .revision
            .checked_add(1)
            .ok_or(Error::GenerationOverflow)?;
        let fixed = self.limits.compaction_fixed_scratch(self.journal_limits)?;
        let mut planner =
            compaction::Planner::new(policy, now_ms, fixed).map_err(Error::Compaction)?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(count)
            .map_err(|_| Error::AllocationFailed)?;
        let mut obsolete = Vec::new();
        obsolete
            .try_reserve_exact(count)
            .map_err(|_| Error::AllocationFailed)?;
        let preparation = (|| {
            // Source certification and map construction never mutate selected files.
            for n in 0..count {
                let d = self.sealed[n].descriptor;
                let mut cursor = StoredCursor::open(
                    &self.data_path(d.base, d.generation),
                    d.base,
                    self.journal_limits,
                    d.kind,
                )?;
                let (mut loaded, mut maximum) = (24u64, EMPTY_TIME);
                for _ in 0..d.entries {
                    let cap = planner
                        .source_cap(self.journal_limits.max_entry_bytes())
                        .map_err(Error::Compaction)?;
                    let (_, entry) = cursor
                        .next(cap)
                        .map_err(|e| match e {
                            Error::Storage(journal::Error::FetchBudgetExceeded) => {
                                Error::Compaction(compaction::Error::BudgetExceeded(
                                    compaction::Budget::ScanBytes,
                                ))
                            }
                            other => other,
                        })?
                        .ok_or(Error::CorruptData)?;
                    loaded = loaded
                        .checked_add(32 + entry.payload.len() as u64)
                        .ok_or(Error::CorruptData)?;
                    maximum = maximum.max(
                        planner
                            .map(&entry, d.kind, self.records)
                            .map_err(Error::Compaction)?,
                    );
                }
                if cursor.next_offset() != d.end
                    || cursor.file_bytes() != d.bytes
                    || cursor.fingerprint() != d.fingerprint
                    || loaded != d.bytes
                    || maximum != d.max_time
                {
                    return Err(Error::CorruptData);
                }
            }
            for n in 0..count {
                let old = self.sealed[n].descriptor;
                let mut entries = Vec::new();
                let capacity = usize::try_from(old.entries).map_err(|_| Error::CorruptData)?;
                planner
                    .charge_scratch(
                        capacity
                            .checked_mul(size_of::<journal::Entry>())
                            .ok_or(Error::InvalidLimits)?,
                    )
                    .map_err(Error::Compaction)?;
                entries
                    .try_reserve_exact(capacity)
                    .map_err(|_| Error::AllocationFailed)?;
                if entries.capacity() > capacity {
                    planner
                        .charge_scratch(
                            (entries.capacity() - capacity) * size_of::<journal::Entry>(),
                        )
                        .map_err(Error::Compaction)?;
                }
                let mut cursor = StoredCursor::open(
                    &self.data_path(old.base, old.generation),
                    old.base,
                    self.journal_limits,
                    old.kind,
                )?;
                let mut bytes = 24u64;
                let mut loaded = 24u64;
                for _ in 0..old.entries {
                    let cap = planner
                        .source_cap(self.journal_limits.max_entry_bytes())
                        .map_err(Error::Compaction)?;
                    let (_, entry) = cursor
                        .next(cap)
                        .map_err(|e| match e {
                            Error::Storage(journal::Error::FetchBudgetExceeded) => {
                                Error::Compaction(compaction::Error::BudgetExceeded(
                                    compaction::Budget::ScanBytes,
                                ))
                            }
                            other => other,
                        })?
                        .ok_or(Error::CorruptData)?;
                    loaded = loaded
                        .checked_add(32 + entry.payload.len() as u64)
                        .ok_or(Error::CorruptData)?;
                    let (payload, actual_records) = planner
                        .encode(
                            &entry.payload,
                            self.records,
                            self.journal_limits.max_entry_bytes(),
                        )
                        .map_err(Error::Compaction)?;
                    planner
                        .reserve_certification(payload.len(), actual_records)
                        .map_err(Error::Compaction)?;
                    bytes = bytes
                        .checked_add(32 + payload.len() as u64)
                        .ok_or(Error::DiskBudget)?;
                    if bytes > self.journal_limits.max_file_bytes()
                        || bytes > self.limits.scan_bytes
                    {
                        return Err(Error::Compaction(compaction::Error::BudgetExceeded(
                            compaction::Budget::OutputBytes,
                        )));
                    }
                    let retained = journal::Entry { payload, ..entry };
                    entries.push(retained);
                }
                if entries.len() != capacity
                    || cursor.next_offset() != old.end
                    || cursor.file_bytes() != old.bytes
                    || cursor.fingerprint() != old.fingerprint
                    || loaded != old.bytes
                {
                    return Err(Error::CorruptData);
                }
                outcome.bytes_before = outcome
                    .bytes_before
                    .checked_add(old.bytes)
                    .ok_or(Error::DiskBudget)?;
                outcome.bytes_after = outcome
                    .bytes_after
                    .checked_add(bytes)
                    .ok_or(Error::DiskBudget)?;
                obsolete.push(old);
                prepared.push(PreparedSegment {
                    old,
                    entries,
                    bytes,
                });
            }
            let mut replacements = Vec::new();
            replacements
                .try_reserve_exact(count)
                .map_err(|_| Error::AllocationFailed)?;
            let new_manifest =
                COMPACTION_HEADER + 4 + (self.manifest.sealed.len() + count) * DESCRIPTOR_BYTES;
            let index_bytes = prepared.iter().try_fold(0u64, |total, p| {
                total
                    .checked_add(72 + p.old.entries.div_ceil(self.limits.interval as u64) * 24)
                    .ok_or(Error::DiskBudget)
            })?;
            let extra = outcome
                .bytes_after
                .checked_add(index_bytes)
                .and_then(|n| n.checked_add((new_manifest as u64) * 2))
                .ok_or(Error::DiskBudget)?;
            self.reserve_disk(extra)?;
            Ok(replacements)
        })();
        let mut replacements = match preparation {
            Ok(replacements) => replacements,
            Err(error) => {
                self.poisoned |= matches!(
                    &error,
                    Error::Io(_)
                        | Error::CorruptData
                        | Error::InvalidLayout
                        | Error::Storage(
                            journal::Error::Corrupt { .. }
                                | journal::Error::Io(_)
                                | journal::Error::ChangedFile
                                | journal::Error::Poisoned
                        )
                        | Error::Compaction(compaction::Error::Records(
                            records::Error::Invalid { .. } | records::Error::Unsupported { .. }
                        ))
                );
                return Err(error);
            }
        };
        outcome.scanned_records = planner.scanned;
        outcome.retained_records = planner.retained;
        outcome.rewritten_segments = count;
        let result = (|| {
            for p in prepared {
                let mut writer = SparseWriter::new(
                    &self.data_path(p.old.base, revision),
                    p.old.base,
                    self.journal_limits,
                )?;
                self.hit(Phase::CompactionHeaderSynced)?;
                for entry in &p.entries {
                    writer.append(entry)?;
                    self.hit(Phase::CompactionEntrySynced)?;
                }
                if writer.next != p.old.end || writer.bytes != p.bytes {
                    return Err(Error::CorruptData);
                }
                drop(writer);
                drop(p.entries);
                let replacement = self.scan_kind(p.old.base, revision, 1)?;
                if replacement.descriptor.end != p.old.end
                    || replacement.descriptor.entries != p.old.entries
                    || replacement.descriptor.bytes != p.bytes
                {
                    return Err(Error::CorruptData);
                }
                self.write_index(&replacement)?;
                replacements.push(replacement);
            }
            self.manifest.revision = revision;
            self.manifest.compacted = true;
            self.manifest.retention = true;
            self.manifest.obsolete = obsolete;
            for (n, s) in replacements.iter().enumerate() {
                self.manifest.sealed[n] = s.descriptor;
            }
            self.publish_manifest()?;
            for (n, s) in replacements.into_iter().enumerate() {
                self.sealed[n] = s;
            }
            self.finish_obsolete_cleanup()?;
            Ok(outcome)
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
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
                (72 + self.sealed[position].checkpoints.len() * 24
                    + manifest_size(&self.manifest)
                    + if self.manifest.compacted {
                        manifest_size(&self.manifest) + DESCRIPTOR_BYTES * 2
                    } else {
                        0
                    }) as u64,
            )
            .ok_or(Error::DiskBudget)?;
        self.reserve_disk(extra)?;
        let mut obsolete = Vec::new();
        if self.manifest.compacted {
            obsolete
                .try_reserve_exact(1)
                .map_err(|_| Error::AllocationFailed)?;
            obsolete.push(old);
        }
        let result = (|| {
            let mut source = StoredCursor::open(
                &self.data_path(base, old.generation),
                base,
                self.journal_limits,
                old.kind,
            )?;
            let mut destination = StoredWriter::new(
                &self.data_path(base, revision),
                base,
                self.journal_limits,
                old.kind,
            )?;
            while let Some((_, entry)) = source.next(self.journal_limits.max_fetch_bytes())? {
                destination.append(&entry)?;
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
            let replacement = self.scan_kind(base, revision, old.kind)?;
            let mut same = replacement.descriptor;
            same.generation = old.generation;
            if same != old {
                return Err(Error::CorruptData);
            }
            self.write_index(&replacement)?;
            self.manifest.revision = revision;
            self.manifest.sealed[position] = replacement.descriptor;
            if self.manifest.compacted {
                self.manifest.obsolete = obsolete;
            }
            self.publish_manifest()?;
            self.sealed[position] = replacement;
            if self.manifest.compacted {
                self.finish_obsolete_cleanup()?;
                return Ok(());
            }
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
    const COMPACTION_PHASES: [Phase; 16] = [
        Phase::CompactionHeaderSynced,
        Phase::CompactionEntrySynced,
        Phase::IndexWritten,
        Phase::IndexSynced,
        Phase::IndexRenamed,
        Phase::ManifestWritten,
        Phase::ManifestSynced,
        Phase::ManifestRenamed,
        Phase::DirectorySynced,
        Phase::CompactionDataRemoved,
        Phase::CompactionIndexRemoved,
        Phase::CompactionCleanupSynced,
        Phase::RetentionClearWritten,
        Phase::RetentionClearSynced,
        Phase::RetentionClearRenamed,
        Phase::RetentionClearDirectorySynced,
    ];
    fn compaction_selected(phase: Phase) -> bool {
        matches!(
            phase,
            Phase::ManifestRenamed
                | Phase::DirectorySynced
                | Phase::CompactionDataRemoved
                | Phase::CompactionIndexRemoved
                | Phase::CompactionCleanupSynced
                | Phase::RetentionClearWritten
                | Phase::RetentionClearSynced
                | Phase::RetentionClearRenamed
                | Phase::RetentionClearDirectorySynced
        )
    }
    fn compaction_seed(path: &Path) -> Result<Log, Error> {
        let (mut log, _) = open(path)?;
        for n in 0..4 {
            log.append(1, &payload(n))?;
        }
        Ok(log)
    }
    fn check_tiny_compaction_cap(
        bytes: u64,
        records_cap: usize,
        expected: compaction::Budget,
        expect_no_load: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp = Temp::new()?;
        let path = temp.0.join("large");
        let (mut log, _) = Log::open(
            &path,
            0,
            journal::Limits::new(65536, 131072, 4, 131072)?,
            records::Limits::default(),
            Limits::new(150, 4, 4, 2, 1024 * 1024, 65536, 131072)?,
        )?;
        let mut large = Vec::new();
        for n in 0..512 {
            large.extend_from_slice(&payload(n));
        }
        log.append(512, &large)?;
        log.append(1, &payload(512))?;
        let before = fs::read_dir(&path)?
            .map(|e| {
                e.and_then(|e| {
                    let b = read_bounded(&e.path(), 131072)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    Ok((e.file_name(), b))
                })
            })
            .collect::<std::io::Result<Vec<_>>>()?;
        {
            COMPACTION_LOADED.with(|n| n.set(0));
            records::reset_record_visits();
            let limits = compaction::Limits::new(
                bytes,
                65536,
                records_cap,
                65536,
                4 * 1024 * 1024,
                1000000,
                64,
                64 * 1024 * 1024,
            )?;
            assert!(
                matches!(log.compact(2000,compaction::Policy::new(1000,limits)?,DeletionGuard::new(513,513)?),
                Err(Error::Compaction(compaction::Error::BudgetExceeded(b))) if b==expected)
            );
            let loaded = COMPACTION_LOADED.with(std::cell::Cell::get);
            let visits = records::record_visits();
            eprintln!("tiny cleaner cap {expected:?}: loaded_payload_bytes={loaded}, parsed_or_projected_record_bodies={visits}");
            assert_eq!(loaded, if expect_no_load { 0 } else { large.len() });
            assert_eq!(
                records::record_visits(),
                0,
                "no record body parsed/projected past policy cap"
            );
            assert!(!log.is_poisoned());
            for (name, bytes) in &before {
                assert_eq!(read_bounded(&path.join(name), 131072)?, *bytes);
            }
            assert_eq!(fs::read_dir(&path)?.count(), before.len());
        }
        Ok(())
    }
    #[test]
    fn compaction_one_byte_cap_rejects_before_loading_a_large_payload(
    ) -> Result<(), Box<dyn std::error::Error>> {
        check_tiny_compaction_cap(1, 262144, compaction::Budget::ScanBytes, true)
    }
    #[test]
    fn compaction_one_record_cap_rejects_before_parsing_any_large_entry_body(
    ) -> Result<(), Box<dyn std::error::Error>> {
        check_tiny_compaction_cap(128 * 1024 * 1024, 1, compaction::Budget::Records, false)
    }
    fn capture_compaction(
        path: &Path,
        kind: &str,
        phase: Phase,
        stage: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(root) = std::env::var_os("PL_COMPACTION_FAULT_DIR") else {
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
                return Err("bounded cleaner capture".into());
            }
            fs::copy(item.path(), out.join(item.file_name()))?;
        }
        let mut receipt = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(out.join("case.json"))?;
        std::io::Write::write_all(&mut receipt,format!("{{\"schema\":1,\"kind\":\"{kind}\",\"phase\":\"{phase:?}\",\"stage\":\"{stage}\",\"source_end\":4,\"confirmed_floor\":0,\"cleaned_end\":3,\"expected_retained_offsets\":{},\"active_offset\":3,\"physical_power_loss_claim\":false}}\n",
            if compaction_selected(phase) { "[2,3]" } else { "[0,1,2,3]" }).as_bytes())?;
        receipt.sync_all()?;
        Ok(())
    }
    fn verify_compaction(path: &Path, phase: Phase) -> Result<Log, Error> {
        let (mut log, _) = open(path)?;
        if log.next_offset() != 4 || log.log_start_offset() != 0 {
            return Err(Error::CorruptData);
        }
        let entries = log.fetch(0, 4, 4096)?;
        if entries.len() != 4 {
            return Err(Error::CorruptData);
        }
        for (n, e) in entries.iter().enumerate() {
            if e.first_offset != n as u64 || e.record_count != 1 {
                return Err(Error::CorruptData);
            }
            let empty = compaction_selected(phase) && n < 2;
            if (empty && !e.payload.is_empty()) || (!empty && e.payload != payload(n as u64)) {
                return Err(Error::CorruptData);
            }
        }
        Ok(log)
    }
    #[test]
    fn compaction_publication_partial_cleanup_io_errors_recover_one_complete_selected_round(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for phase in COMPACTION_PHASES.iter().copied() {
            let temp = Temp::new()?;
            let path = temp.0.join("log");
            let mut log = compaction_seed(&path)?;
            log.fault = Some((phase, false));
            assert!(
                log.compact(
                    2000,
                    compaction::Policy::default(),
                    DeletionGuard::new(4, 4)?
                )
                .is_err(),
                "{phase:?}"
            );
            assert!(log.is_poisoned());
            assert_eq!((log.next_offset(), log.log_start_offset()), (4, 0));
            assert!(matches!(log.fetch(0, 4, 4096), Err(Error::Poisoned)));
            capture_compaction(&path, "io-error", phase, "interrupted")?;
            drop(log);
            let mut log = verify_compaction(&path, phase)?;
            capture_compaction(&path, "io-error", phase, "recovered")?;
            assert_eq!(log.append(1, &payload(4))?.next_offset, 5);
        }
        Ok(())
    }
    #[test]
    fn compaction_process_exit_at_publication_and_cleanup_recovers_atomic_round(
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(path) = std::env::var_os("PL_COMPACTION_CHILD") {
            let name = std::env::var("PL_COMPACTION_PHASE")?;
            let phase = COMPACTION_PHASES
                .iter()
                .copied()
                .find(|p| format!("{p:?}") == name)
                .ok_or("phase")?;
            let mut log = compaction_seed(Path::new(&path))?;
            log.fault = Some((phase, true));
            log.compact(
                2000,
                compaction::Policy::default(),
                DeletionGuard::new(4, 4)?,
            )?;
            return Err("process cut not reached".into());
        }
        for phase in COMPACTION_PHASES.iter().copied() {
            let temp = Temp::new()?;
            let path = temp.0.join("log");
            let status=std::process::Command::new(std::env::current_exe()?)
                .args(["--exact","segments::tests::compaction_process_exit_at_publication_and_cleanup_recovers_atomic_round","--nocapture"])
                .env("PL_COMPACTION_CHILD",&path).env("PL_COMPACTION_PHASE",format!("{phase:?}")).status()?;
            assert_eq!(status.code(), Some(0), "{phase:?}");
            capture_compaction(&path, "process-exit", phase, "interrupted")?;
            let mut log = verify_compaction(&path, phase)?;
            capture_compaction(&path, "process-exit", phase, "recovered")?;
            assert_eq!(log.append(1, &payload(4))?.next_offset, 5);
        }
        Ok(())
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
