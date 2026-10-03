//! Bounded, checksummed images of the canonical committed opaque metadata prefix.
//!
//! Publication creates an inactive image. Replication must synchronize its own
//! authoritative Install WAL receipt before exposing that image or its boundary.
//! Images never carry a replica-local current election term or vote. This custom
//! format is not an Apache Kafka snapshot serialization or a compaction claim.
//! One trusted owner exclusively owns the configured directory and its files.

use super::{
    election::LogPosition,
    membership::{Voters, MAX_CONFIGURATION_BYTES},
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const MAGIC: &[u8; 8] = b"PLSNAP01";
const SEAL: &[u8; 8] = b"PLSNEND1";
const HEADER_MAX: usize = 838;
const DYNAMIC_MAGIC: &[u8; 8] = b"PLSNAP02";
const DYNAMIC_HEADER_MAX: usize = HEADER_MAX + 4 + MAX_CONFIGURATION_BYTES;
const ENTRY_HEADER: usize = 32;
const FOOTER: usize = 24;
const MAX_TERM: u64 = i32::MAX as u64 + 1;

/// Transferable fixed-group identity. Local replica ownership is deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    cluster: String,
    topic: String,
    partition: u32,
    voters: Vec<u32>,
    genesis: Option<Voters>,
}
impl Identity {
    /// Validate bounded UTF-8 cluster/topic identity and sorted distinct voters.
    pub fn new(
        cluster: String,
        topic: String,
        partition: u32,
        voters: Vec<u32>,
    ) -> Result<Self, Error> {
        if cluster.is_empty()
            || cluster.len() > 249
            || topic.is_empty()
            || topic.len() > 249
            || partition > i32::MAX as u32
            || !(1..=64).contains(&voters.len())
            || voters.iter().any(|id| *id > i32::MAX as u32)
            || voters.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            cluster,
            topic,
            partition,
            voters,
            genesis: None,
        })
    }
    /// Explicit directory-aware group identity for typed voter records.
    /// The immutable genesis is transferable and excludes replica-local identity.
    pub fn dynamic(
        cluster: String,
        topic: String,
        partition: u32,
        genesis: Voters,
    ) -> Result<Self, Error> {
        if genesis.epoch() != 0 || genesis.position() != LogPosition::default() {
            return Err(Error::InvalidConfig);
        }
        let mut identity = Self::new(
            cluster,
            topic,
            partition,
            genesis.voters().iter().map(|v| v.key().id).collect(),
        )?;
        identity.genesis = Some(genesis);
        Ok(identity)
    }
    /// Immutable initial directory-aware configuration, absent for fixed images.
    pub fn genesis(&self) -> Option<&Voters> {
        self.genesis.as_ref()
    }
    fn header_max(&self) -> usize {
        if self.genesis.is_some() {
            DYNAMIC_HEADER_MAX
        } else {
            HEADER_MAX
        }
    }
    /// Cluster bytes form data, never a filesystem path.
    pub fn cluster(&self) -> &str {
        &self.cluster
    }
    /// Fixed metadata topic identity.
    pub fn topic(&self) -> &str {
        &self.topic
    }
    /// Fixed nonnegative partition.
    pub fn partition(&self) -> u32 {
        self.partition
    }
    /// Sorted distinct fixed voters.
    pub fn voters(&self) -> &[u32] {
        &self.voters
    }
}

/// Positive image, transfer, disk-generation and recovery budgets.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    image_bytes: u64,
    records: usize,
    payload_bytes: u64,
    record_bytes: usize,
    chunk_bytes: usize,
    generations: usize,
    directory_entries: usize,
}
impl Limits {
    /// Check ceilings before any peer-sized allocation or filesystem mutation.
    pub fn new(
        image_bytes: u64,
        records: usize,
        payload_bytes: u64,
        record_bytes: usize,
        chunk_bytes: usize,
        generations: usize,
        directory_entries: usize,
    ) -> Result<Self, Error> {
        let minimum = payload_bytes
            .checked_add((records as u64).saturating_mul(ENTRY_HEADER as u64))
            .and_then(|n| n.checked_add((HEADER_MAX + FOOTER) as u64));
        if !(1024..=128 * 1024 * 1024).contains(&image_bytes)
            || !(1..=4096).contains(&records)
            || !(1..=64 * 1024 * 1024).contains(&payload_bytes)
            || !(1..=1024 * 1024).contains(&record_bytes)
            || record_bytes as u64 > payload_bytes
            || !(1..=4 * 1024 * 1024).contains(&chunk_bytes)
            || !(2..=32).contains(&generations)
            || !(generations * 2..=1024).contains(&directory_entries)
            || minimum.is_none_or(|n| n > image_bytes)
        {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            image_bytes,
            records,
            payload_bytes,
            record_bytes,
            chunk_bytes,
            generations,
            directory_entries,
        })
    }
    /// Maximum encoded bytes per image, including its explicit completion seal.
    pub fn image_bytes(&self) -> u64 {
        self.image_bytes
    }
    /// Maximum canonical records per image.
    pub fn records(&self) -> usize {
        self.records
    }
    /// Maximum sum of opaque payload bytes.
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
    /// Maximum one-record payload.
    pub fn record_bytes(&self) -> usize {
        self.record_bytes
    }
    /// Maximum retained transfer chunk.
    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }
    /// Maximum retained complete generations, including inactive images.
    pub fn generations(&self) -> usize {
        self.generations
    }
    /// Maximum decode allocation, including header and vector element overhead.
    /// The fixed 8192-byte checksum scratch is on the storage thread's stack.
    pub fn decoded_bytes(&self) -> u64 {
        self.payload_bytes
            + (self.records * std::mem::size_of::<Entry>() + DYNAMIC_HEADER_MAX) as u64
    }
    /// Maximum image disk reservation plus one staged replacement.
    pub fn disk_bytes(&self) -> u64 {
        self.image_bytes * (self.generations as u64 + 1)
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            image_bytes: 65 * 1024 * 1024,
            records: 4096,
            payload_bytes: 64 * 1024 * 1024,
            record_bytes: 1024 * 1024,
            chunk_bytes: 4 * 1024 * 1024,
            generations: 8,
            directory_entries: 32,
        }
    }
}

/// Canonical committed metadata entry; empty payload is reserved for a barrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Positive normalized core term, not a wire epoch.
    pub term: u64,
    /// Inclusive one-based index, including barriers.
    pub index: u64,
    /// Internal current-term barrier rather than opaque application metadata.
    pub barrier: bool,
    /// Canonical custom Voters record, valid only in the explicit dynamic format.
    pub voters: bool,
    /// Exact caller-owned opaque bytes.
    pub payload: Vec<u8>,
}

/// Complete transfer description. Names derive only from the fixed generation bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Descriptor {
    /// Nonzero generation supplied by the owner or validated peer envelope.
    pub generation: [u8; 16],
    /// Last included inclusive index and normalized term; empty is `(0,0)`.
    pub base: LogPosition,
    /// Canonical prefix record count, equal to `base.index`.
    pub records: usize,
    /// Sum of exact opaque payload lengths.
    pub payload_bytes: u64,
    /// Exact full encoded length including the completion seal.
    pub bytes: u64,
    /// CRC32C of all encoded bytes, including the completion seal.
    pub checksum: u32,
}
impl Descriptor {
    fn validate(self, limits: Limits) -> Result<(), Error> {
        let minimum = (self.records as u64)
            .checked_mul(ENTRY_HEADER as u64)
            .and_then(|value| value.checked_add(self.payload_bytes))
            .and_then(|value| value.checked_add(90 + FOOTER as u64));
        if self.generation == [0; 16]
            || self.records > limits.records
            || self.base.index != self.records as u64
            || (self.base.index == 0) != (self.base.term == 0)
            || self.base.term > MAX_TERM
            || self.payload_bytes > limits.payload_bytes
            || self.bytes > limits.image_bytes
            || minimum.is_none_or(|value| value > self.bytes)
        {
            return Err(Error::InvalidDescriptor);
        }
        Ok(())
    }
}

/// Fully decoded bounded image; no replica-local current term or vote is imported.
#[derive(Debug)]
pub struct Image {
    /// Complete verified generation, boundary and checksums.
    pub descriptor: Descriptor,
    /// Canonical committed opaque prefix in index order.
    pub entries: Vec<Entry>,
}
/// One bounded sequential transfer result.
#[derive(Debug)]
pub struct Chunk {
    /// Generation pinned by the reader.
    pub generation: [u8; 16],
    /// Exact starting byte position.
    pub offset: u64,
    /// At most the configured chunk budget.
    pub bytes: Vec<u8>,
    /// This chunk completes the exact declared image length.
    pub done: bool,
}
/// Inactive published image. Replication visibility additionally requires its WAL receipt.
#[derive(Debug, Clone, Copy)]
pub struct Published {
    descriptor: Descriptor,
}
impl Published {
    /// Complete verified description suitable for a checked Install WAL receipt.
    pub fn descriptor(&self) -> Descriptor {
        self.descriptor
    }
}

/// Explicit bounded admission, malformed image or storage failure.
#[derive(Debug)]
pub enum Error {
    /// Operator bounds/group identity are invalid.
    InvalidConfig,
    /// Transfer description is structurally invalid or exceeds its declared profile.
    InvalidDescriptor,
    /// A stale, reordered, empty, overlapping or oversized transfer chunk was rejected.
    InvalidChunk,
    /// One staged incoming image or outgoing reader is already open.
    Busy,
    /// A configured disk, directory, record, payload or allocation envelope is exhausted.
    Bounds,
    /// Complete bytes violate the canonical image contract.
    Corrupt,
    /// A header, body, completion seal or transfer checksum differs.
    Checksum,
    /// The transferable fixed-group identity differs.
    ForeignIdentity,
    /// This generation already exists or is being used.
    DuplicateGeneration,
    /// A requested complete generation is absent.
    MissingImage,
    /// Exact image length/completion seal has not arrived.
    Incomplete,
    /// An ambiguous storage mutation requires verified reopening.
    Poisoned,
    /// Bounded allocation failed.
    Allocation,
    /// Local filesystem operation failed.
    Storage(io::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "snapshot: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Self::Incomplete
        } else {
            Self::Storage(error)
        }
    }
}

fn allocated(size: usize) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::Allocation)?;
    Ok(bytes)
}
fn name(generation: [u8; 16], partial: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(49);
    result.push_str("snapshot-");
    for byte in generation {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 15)]));
    }
    result.push_str(if partial { ".partial" } else { ".image" });
    result
}
fn parse_name(filename: &str) -> Result<([u8; 16], bool), Error> {
    let (body, partial) = filename
        .strip_prefix("snapshot-")
        .and_then(|text| {
            text.strip_suffix(".image")
                .map(|value| (value, false))
                .or_else(|| text.strip_suffix(".partial").map(|value| (value, true)))
        })
        .ok_or(Error::ForeignIdentity)?;
    if body.len() != 32 {
        return Err(Error::ForeignIdentity);
    }
    let mut generation = [0; 16];
    for (slot, pair) in generation.iter_mut().zip(body.as_bytes().chunks_exact(2)) {
        let digit = |value: u8| match value {
            b'0'..=b'9' => Ok(value - b'0'),
            b'a'..=b'f' => Ok(value - b'a' + 10),
            _ => Err(Error::ForeignIdentity),
        };
        *slot = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    if generation == [0; 16] {
        return Err(Error::ForeignIdentity);
    }
    Ok((generation, partial))
}

fn image_header(identity: &Identity, descriptor: Descriptor) -> Result<Vec<u8>, Error> {
    let mut out = allocated(identity.header_max())?;
    out.extend_from_slice(if identity.genesis.is_some() {
        DYNAMIC_MAGIC
    } else {
        MAGIC
    });
    out.extend_from_slice(
        &if identity.genesis.is_some() {
            2u16
        } else {
            1u16
        }
        .to_be_bytes(),
    );
    out.extend_from_slice(&[0; 6]);
    out.extend_from_slice(&descriptor.generation);
    for value in [
        descriptor.base.index,
        descriptor.base.term,
        descriptor.records as u64,
        descriptor.payload_bytes,
    ] {
        out.extend_from_slice(&value.to_be_bytes());
    }
    out.extend_from_slice(&(identity.cluster.len() as u32).to_be_bytes());
    out.extend_from_slice(identity.cluster.as_bytes());
    out.extend_from_slice(&(identity.topic.len() as u32).to_be_bytes());
    out.extend_from_slice(identity.topic.as_bytes());
    out.extend_from_slice(&identity.partition.to_be_bytes());
    out.extend_from_slice(&(identity.voters.len() as u32).to_be_bytes());
    for voter in &identity.voters {
        out.extend_from_slice(&voter.to_be_bytes());
    }
    if let Some(genesis) = &identity.genesis {
        let bytes = genesis.encode().map_err(|_| Error::Corrupt)?;
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(&bytes);
    }
    let length = (out.len() + 4) as u32;
    out[12..16].copy_from_slice(&length.to_be_bytes());
    let checksum = crc32c::crc32c(&out);
    out.extend_from_slice(&checksum.to_be_bytes());
    Ok(out)
}
fn entry_header(entry: &Entry) -> [u8; ENTRY_HEADER] {
    let mut bytes = [0; ENTRY_HEADER];
    bytes[..8].copy_from_slice(&entry.term.to_be_bytes());
    bytes[8..16].copy_from_slice(&entry.index.to_be_bytes());
    bytes[16] = if entry.voters {
        2
    } else {
        u8::from(entry.barrier)
    };
    bytes[24..28].copy_from_slice(&(entry.payload.len() as u32).to_be_bytes());
    bytes
}
fn seal(bytes: u64, checksum: u32) -> [u8; FOOTER] {
    let mut out = [0; FOOTER];
    out[..8].copy_from_slice(SEAL);
    out[8..16].copy_from_slice(&bytes.to_be_bytes());
    out[16..20].copy_from_slice(&checksum.to_be_bytes());
    let footer_crc = crc32c::crc32c(&out[..20]);
    out[20..].copy_from_slice(&footer_crc.to_be_bytes());
    out
}
struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}
impl Decoder<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let end = self.cursor.checked_add(N).ok_or(Error::Corrupt)?;
        let bytes = self.bytes.get(self.cursor..end).ok_or(Error::Corrupt)?;
        self.cursor = end;
        bytes.try_into().map_err(|_| Error::Corrupt)
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(self.take()?))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(self.take()?))
    }
    fn identity_text(&mut self, expected: &str) -> Result<(), Error> {
        let length = self.u32()? as usize;
        let end = self.cursor.checked_add(length).ok_or(Error::Bounds)?;
        let actual = self.bytes.get(self.cursor..end).ok_or(Error::Corrupt)?;
        self.cursor = end;
        if actual == expected.as_bytes() {
            Ok(())
        } else {
            Err(Error::ForeignIdentity)
        }
    }
}

fn decode(
    file: &mut File,
    identity: &Identity,
    limits: Limits,
    collect: bool,
) -> Result<Image, Error> {
    let bytes = file.metadata()?.len();
    if bytes > limits.image_bytes {
        return Err(Error::Bounds);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut prefix = [0; 16];
    file.read_exact(&mut prefix)?;
    let (magic, version) = if identity.genesis.is_some() {
        (DYNAMIC_MAGIC, [0, 2, 0, 0])
    } else {
        (MAGIC, [0, 1, 0, 0])
    };
    if &prefix[..8] != magic || prefix[8..12] != version {
        return Err(Error::Corrupt);
    }
    let length =
        u32::from_be_bytes(prefix[12..16].try_into().map_err(|_| Error::Corrupt)?) as usize;
    if !(90..=identity.header_max()).contains(&length) || length as u64 + FOOTER as u64 > bytes {
        return Err(Error::Incomplete);
    }
    let mut header = allocated(length)?;
    header.extend_from_slice(&prefix);
    header.resize(length, 0);
    file.read_exact(&mut header[16..])?;
    let (data, checksum) = header.split_at(length - 4);
    if crc32c::crc32c(data) != u32::from_be_bytes(checksum.try_into().map_err(|_| Error::Corrupt)?)
    {
        return Err(Error::Checksum);
    }
    let mut parser = Decoder {
        bytes: data,
        cursor: 16,
    };
    let generation = parser.take()?;
    let base = LogPosition {
        index: parser.u64()?,
        term: parser.u64()?,
    };
    let count = usize::try_from(parser.u64()?).map_err(|_| Error::Bounds)?;
    let payload_bytes = parser.u64()?;
    parser.identity_text(&identity.cluster)?;
    parser.identity_text(&identity.topic)?;
    if parser.u32()? != identity.partition || parser.u32()? as usize != identity.voters.len() {
        return Err(Error::ForeignIdentity);
    }
    for voter in &identity.voters {
        if parser.u32()? != *voter {
            return Err(Error::ForeignIdentity);
        }
    }
    if let Some(genesis) = &identity.genesis {
        let size = parser.u32()? as usize;
        if size > MAX_CONFIGURATION_BYTES {
            return Err(Error::Bounds);
        }
        let end = parser.cursor.checked_add(size).ok_or(Error::Bounds)?;
        let raw = parser.bytes.get(parser.cursor..end).ok_or(Error::Corrupt)?;
        if Voters::decode(raw).map_err(|_| Error::Corrupt)? != *genesis {
            return Err(Error::ForeignIdentity);
        }
        parser.cursor = end;
    }
    if parser.cursor != parser.bytes.len() {
        return Err(Error::Corrupt);
    }
    let mut descriptor = Descriptor {
        generation,
        base,
        records: count,
        payload_bytes,
        bytes,
        checksum: 0,
    };
    descriptor.validate(limits)?;
    let expected = (length as u64)
        .checked_add(
            (count as u64)
                .checked_mul(ENTRY_HEADER as u64)
                .ok_or(Error::Bounds)?,
        )
        .and_then(|n| n.checked_add(payload_bytes))
        .and_then(|n| n.checked_add(FOOTER as u64))
        .ok_or(Error::Bounds)?;
    if bytes != expected {
        return Err(Error::Incomplete);
    }
    let mut entries = Vec::new();
    if collect {
        entries
            .try_reserve_exact(count)
            .map_err(|_| Error::Allocation)?;
    }
    let mut body_crc = crc32c::crc32c(&header);
    let mut observed_payload = 0u64;
    let mut last_term = 0;
    let mut scratch = [0; 8192];
    let mut configuration = identity.genesis.clone();
    for ordinal in 0..count {
        let mut raw = [0; ENTRY_HEADER];
        file.read_exact(&mut raw)?;
        body_crc = crc32c::crc32c_append(body_crc, &raw);
        let mut parser = Decoder {
            bytes: &raw,
            cursor: 0,
        };
        let term = parser.u64()?;
        let index = parser.u64()?;
        let flag = parser.take::<1>()?[0];
        if parser.take::<7>()? != [0; 7] {
            return Err(Error::Corrupt);
        }
        let payload_length = parser.u32()? as usize;
        if parser.take::<4>()? != [0; 4]
            || flag > if identity.genesis.is_some() { 2 } else { 1 }
            || index != ordinal as u64 + 1
            || !(1..=MAX_TERM).contains(&term)
            || term < last_term
            || payload_length > limits.record_bytes
            || (flag == 1) != (payload_length == 0)
            || (flag == 2 && payload_length > MAX_CONFIGURATION_BYTES)
        {
            return Err(Error::Corrupt);
        }
        last_term = term;
        observed_payload = observed_payload
            .checked_add(payload_length as u64)
            .ok_or(Error::Bounds)?;
        if observed_payload > payload_bytes {
            return Err(Error::Bounds);
        }
        let mut payload = if collect || flag == 2 {
            allocated(payload_length)?
        } else {
            Vec::new()
        };
        let mut left = payload_length;
        while left > 0 {
            let part = left.min(scratch.len());
            file.read_exact(&mut scratch[..part])?;
            body_crc = crc32c::crc32c_append(body_crc, &scratch[..part]);
            if collect || flag == 2 {
                payload.extend_from_slice(&scratch[..part]);
            }
            left -= part;
        }
        if flag == 2 {
            let next = Voters::decode(&payload).map_err(|_| Error::Corrupt)?;
            if next.position() != (LogPosition { term, index }) {
                return Err(Error::Corrupt);
            }
            configuration
                .as_ref()
                .ok_or(Error::Corrupt)?
                .validate_successor(&next)
                .map_err(|_| Error::Corrupt)?;
            configuration = Some(next);
        }
        if collect {
            entries.push(Entry {
                term,
                index,
                barrier: flag == 1,
                voters: flag == 2,
                payload,
            });
        }
    }
    if observed_payload != payload_bytes || last_term != base.term {
        return Err(Error::Corrupt);
    }
    let mut footer = [0; FOOTER];
    file.read_exact(&mut footer)?;
    if footer != seal(bytes, body_crc) {
        return Err(Error::Checksum);
    }
    descriptor.checksum = crc32c::crc32c_append(body_crc, &footer);
    if file.stream_position()? != bytes {
        return Err(Error::Corrupt);
    }
    Ok(Image {
        descriptor,
        entries,
    })
}

struct Incoming {
    descriptor: Descriptor,
    file: File,
    received: u64,
    checksum: u32,
}
struct Outgoing {
    descriptor: Descriptor,
    file: File,
    position: u64,
}
/// Exclusive synchronous image owner, intended for the replication storage thread.
///
/// At most four descriptors are open: directory, incoming stage, outgoing reader
/// and one synchronous decode. One image decode allocates at most
/// `Limits::decoded_bytes`; callers must
/// additionally charge old/replacement state and queued/unconsumed chunk replies.
/// Complete inactive generations consume the configured disk budget; this owner
/// never deletes an accepted image or invents an authoritative install pointer.
pub struct Store {
    directory: PathBuf,
    directory_file: File,
    identity: Identity,
    limits: Limits,
    generations: Vec<[u8; 16]>,
    incoming: Option<Incoming>,
    outgoing: Option<Outgoing>,
    poisoned: bool,
    #[cfg(test)]
    publication_fault: Option<PublicationFault>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublicationPhase {
    CandidateSynced,
    ImageRenamed,
    DirectorySynced,
}
#[cfg(test)]
struct PublicationFault {
    generation: [u8; 16],
    phase: PublicationPhase,
    exit_process: bool,
}
impl Store {
    /// Open an exclusively owned directory after bounded namespace/size inspection.
    /// The trusted configured parent must exist. A newly created directory is
    /// synchronized in that parent before any image can receive an Install receipt.
    /// Known `.partial` files are unreceipted candidates and are removed on recovery.
    pub fn open(path: impl AsRef<Path>, identity: Identity, limits: Limits) -> Result<Self, Error> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() || path.as_os_str().len() > 4096 {
            return Err(Error::InvalidConfig);
        }
        match fs::create_dir(path) {
            Ok(()) => File::open(
                path.parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )?
            .sync_all()?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if !fs::symlink_metadata(path)?.file_type().is_dir() {
            return Err(Error::ForeignIdentity);
        }
        let directory_file = File::open(path)?;
        let mut generations = Vec::new();
        generations
            .try_reserve_exact(limits.generations)
            .map_err(|_| Error::Allocation)?;
        let mut partial = None;
        let mut count = 0;
        for item in fs::read_dir(path)? {
            count += 1;
            if count > limits.directory_entries {
                return Err(Error::Bounds);
            }
            let item = item?;
            let metadata = fs::symlink_metadata(item.path())?;
            if !metadata.is_file() || metadata.len() > limits.image_bytes {
                return Err(Error::Bounds);
            }
            let filename = item.file_name();
            let filename = filename.to_str().ok_or(Error::ForeignIdentity)?;
            let (generation, is_partial) = parse_name(filename)?;
            if is_partial {
                if partial.replace(item.path()).is_some() {
                    return Err(Error::Bounds);
                }
            } else {
                if generations.len() >= limits.generations {
                    return Err(Error::Bounds);
                }
                generations.push(generation);
            }
        }
        if let Some(path) = partial {
            fs::remove_file(path)?;
            directory_file.sync_all()?;
        }
        Ok(Self {
            directory: path.to_path_buf(),
            directory_file,
            identity,
            limits,
            generations,
            incoming: None,
            outgoing: None,
            poisoned: false,
            #[cfg(test)]
            publication_fault: None,
        })
    }
    fn ready(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    fn mutation<T>(&mut self, result: io::Result<T>) -> Result<T, Error> {
        result.map_err(|error| {
            self.poisoned = true;
            Error::Storage(error)
        })
    }
    /// Diagnose ambiguous local mutation; no successful operation proceeds while poisoned.
    pub fn poisoned(&self) -> bool {
        self.poisoned
    }
    /// Whether there is no staged incoming image or outgoing reader to inherit.
    pub fn idle(&self) -> bool {
        self.incoming.is_none() && self.outgoing.is_none()
    }
    #[cfg(test)]
    pub(crate) fn fault_publication(
        &mut self,
        generation: [u8; 16],
        phase: PublicationPhase,
        exit_process: bool,
    ) {
        self.publication_fault = Some(PublicationFault {
            generation,
            phase,
            exit_process,
        });
    }
    #[cfg(test)]
    fn publication_stage(
        &mut self,
        generation: [u8; 16],
        phase: PublicationPhase,
    ) -> Result<(), Error> {
        if self
            .publication_fault
            .as_ref()
            .is_some_and(|fault| fault.generation == generation && fault.phase == phase)
        {
            let fault = self.publication_fault.take().ok_or(Error::Corrupt)?;
            self.poisoned = true;
            if fault.exit_process {
                std::process::exit(44);
            }
            return Err(Error::Storage(io::Error::other(
                "injected snapshot publication failure",
            )));
        }
        Ok(())
    }
    /// Immutable fixed identity for coordinated replication install validation.
    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    /// Exact configured allocation/transfer/disk envelopes.
    pub fn limits(&self) -> Limits {
        self.limits
    }
    /// Count complete generations, including published images without an Install receipt.
    pub fn generation_count(&self) -> usize {
        self.generations.len()
    }
    /// Begin one bounded incoming image. Every path derives from the fixed generation bytes.
    pub fn begin_receive(&mut self, descriptor: Descriptor) -> Result<(), Error> {
        self.ready()?;
        descriptor.validate(self.limits)?;
        let extra = self
            .identity
            .genesis
            .as_ref()
            .map(|v| v.encode().map(|b| b.len() + 4))
            .transpose()
            .map_err(|_| Error::Corrupt)?
            .unwrap_or(0);
        let expected = (84
            + extra
            + self.identity.cluster.len()
            + self.identity.topic.len()
            + self.identity.voters.len() * 4) as u64
            + descriptor.records as u64 * ENTRY_HEADER as u64
            + descriptor.payload_bytes
            + FOOTER as u64;
        if descriptor.bytes != expected {
            return Err(Error::InvalidDescriptor);
        }
        if self.incoming.is_some() {
            return Err(Error::Busy);
        }
        if self.generations.contains(&descriptor.generation) {
            return Err(Error::DuplicateGeneration);
        }
        if self.generations.len() >= self.limits.generations {
            return Err(Error::Bounds);
        }
        let path = self.directory.join(name(descriptor.generation, true));
        let opened = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path);
        let file = self.mutation(opened)?;
        self.incoming = Some(Incoming {
            descriptor,
            file,
            received: 0,
            checksum: 0,
        });
        Ok(())
    }
    /// Append exactly the next declared bytes, rejecting reorder/overflow before writing.
    pub fn receive_chunk(
        &mut self,
        generation: [u8; 16],
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), Error> {
        self.ready()?;
        let incoming = self.incoming.as_mut().ok_or(Error::InvalidChunk)?;
        if incoming.descriptor.generation != generation
            || incoming.received != offset
            || bytes.is_empty()
            || bytes.len() > self.limits.chunk_bytes
            || offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|n| n > incoming.descriptor.bytes)
        {
            return Err(Error::InvalidChunk);
        }
        if let Err(error) = incoming.file.write_all(bytes) {
            self.poisoned = true;
            return Err(error.into());
        }
        incoming.received += bytes.len() as u64;
        incoming.checksum = crc32c::crc32c_append(incoming.checksum, bytes);
        Ok(())
    }
    /// Validate the full canonical image/seal, sync, rename and sync its directory.
    /// The returned image is inactive until replication synchronizes its Install receipt.
    pub fn finish_receive(&mut self, generation: [u8; 16]) -> Result<Published, Error> {
        self.ready()?;
        let incoming = self.incoming.as_mut().ok_or(Error::InvalidChunk)?;
        if incoming.descriptor.generation != generation {
            return Err(Error::InvalidChunk);
        }
        if incoming.received != incoming.descriptor.bytes {
            return Err(Error::Incomplete);
        }
        if incoming.checksum != incoming.descriptor.checksum {
            return Err(Error::Checksum);
        }
        let image = decode(&mut incoming.file, &self.identity, self.limits, false)?;
        if image.descriptor != incoming.descriptor {
            return Err(Error::InvalidDescriptor);
        }
        let result = incoming.file.sync_all();
        self.mutation(result)?;
        #[cfg(test)]
        self.publication_stage(generation, PublicationPhase::CandidateSynced)?;
        let incoming = self.incoming.take().ok_or(Error::InvalidChunk)?;
        drop(incoming.file);
        let result = fs::rename(
            self.directory.join(name(generation, true)),
            self.directory.join(name(generation, false)),
        );
        self.mutation(result)?;
        #[cfg(test)]
        self.publication_stage(generation, PublicationPhase::ImageRenamed)?;
        let result = self.directory_file.sync_all();
        self.mutation(result)?;
        #[cfg(test)]
        self.publication_stage(generation, PublicationPhase::DirectorySynced)?;
        self.generations.push(generation);
        Ok(Published {
            descriptor: image.descriptor,
        })
    }
    /// Discard an unreceipted partial candidate, preserving every complete generation.
    pub fn abort_receive(&mut self) -> Result<(), Error> {
        let Some(incoming) = self.incoming.take() else {
            return Ok(());
        };
        drop(incoming.file);
        let result = fs::remove_file(
            self.directory
                .join(name(incoming.descriptor.generation, true)),
        );
        self.mutation(result)?;
        let result = self.directory_file.sync_all();
        self.mutation(result)
    }
    /// Create an inactive image after preflighting every canonical prefix record.
    pub fn create(
        &mut self,
        generation: [u8; 16],
        base: LogPosition,
        entries: &[Entry],
    ) -> Result<Published, Error> {
        self.ready()?;
        if entries.len() > self.limits.records {
            return Err(Error::Bounds);
        }
        let mut payload_bytes = 0u64;
        let mut last_term = 0;
        let mut configuration = self.identity.genesis.clone();
        for (ordinal, entry) in entries.iter().enumerate() {
            if entry.index != ordinal as u64 + 1
                || !(1..=MAX_TERM).contains(&entry.term)
                || entry.term < last_term
                || entry.payload.len() > self.limits.record_bytes
                || entry.barrier != entry.payload.is_empty()
                || (entry.voters && (entry.barrier || self.identity.genesis.is_none()))
            {
                return Err(Error::Corrupt);
            }
            if entry.voters {
                let next = Voters::decode(&entry.payload).map_err(|_| Error::Corrupt)?;
                if next.position()
                    != (LogPosition {
                        term: entry.term,
                        index: entry.index,
                    })
                {
                    return Err(Error::Corrupt);
                }
                configuration
                    .as_ref()
                    .ok_or(Error::Corrupt)?
                    .validate_successor(&next)
                    .map_err(|_| Error::Corrupt)?;
                configuration = Some(next);
            }
            last_term = entry.term;
            payload_bytes = payload_bytes
                .checked_add(entry.payload.len() as u64)
                .ok_or(Error::Bounds)?;
        }
        if payload_bytes > self.limits.payload_bytes || base.term != last_term {
            return Err(Error::Bounds);
        }
        let mut descriptor = Descriptor {
            generation,
            base,
            records: entries.len(),
            payload_bytes,
            bytes: FOOTER as u64,
            checksum: 0,
        };
        let header = image_header(&self.identity, descriptor)?;
        descriptor.bytes =
            (header.len() + entries.len() * ENTRY_HEADER + FOOTER) as u64 + payload_bytes;
        descriptor.validate(self.limits)?;
        let mut body_crc = crc32c::crc32c(&header);
        for entry in entries {
            body_crc = crc32c::crc32c_append(body_crc, &entry_header(entry));
            body_crc = crc32c::crc32c_append(body_crc, &entry.payload);
        }
        let footer = seal(descriptor.bytes, body_crc);
        descriptor.checksum = crc32c::crc32c_append(body_crc, &footer);
        self.begin_receive(descriptor)?;
        let mut position = 0;
        let mut write = |this: &mut Self, bytes: &[u8]| -> Result<(), Error> {
            for chunk in bytes.chunks(this.limits.chunk_bytes) {
                this.receive_chunk(generation, position, chunk)?;
                position += chunk.len() as u64;
            }
            Ok(())
        };
        let result = (|| {
            write(self, &header)?;
            for entry in entries {
                write(self, &entry_header(entry))?;
                write(self, &entry.payload)?;
            }
            write(self, &footer)?;
            self.finish_receive(generation)
        })();
        if result.is_err() {
            let _ = self.abort_receive();
        }
        result
    }
    fn open_image(&self, generation: [u8; 16]) -> Result<File, Error> {
        self.ready()?;
        if !self.generations.contains(&generation) {
            return Err(Error::MissingImage);
        }
        let path = self.directory.join(name(generation, false));
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.len() > self.limits.image_bytes {
            return Err(Error::Bounds);
        }
        Ok(File::open(path)?)
    }
    /// Decode one complete image within the explicit record/payload allocation budget.
    pub fn load(&mut self, generation: [u8; 16]) -> Result<Image, Error> {
        let mut file = self.open_image(generation)?;
        let image = decode(&mut file, &self.identity, self.limits, true)?;
        if image.descriptor.generation != generation {
            return Err(Error::InvalidDescriptor);
        }
        Ok(image)
    }
    /// Start one bounded sequential reader after full structural/checksum verification.
    pub fn start_read(&mut self, generation: [u8; 16]) -> Result<Descriptor, Error> {
        self.ready()?;
        if self.outgoing.is_some() {
            return Err(Error::Busy);
        }
        let mut file = self.open_image(generation)?;
        let image = decode(&mut file, &self.identity, self.limits, false)?;
        if image.descriptor.generation != generation {
            return Err(Error::InvalidDescriptor);
        }
        file.seek(SeekFrom::Start(0))?;
        self.outgoing = Some(Outgoing {
            descriptor: image.descriptor,
            file,
            position: 0,
        });
        Ok(image.descriptor)
    }
    /// Read at most one configured chunk; the reader closes after the final chunk.
    pub fn next_chunk(&mut self) -> Result<Chunk, Error> {
        self.ready()?;
        let outgoing = self.outgoing.as_mut().ok_or(Error::InvalidChunk)?;
        let length = (outgoing.descriptor.bytes - outgoing.position)
            .min(self.limits.chunk_bytes as u64) as usize;
        let mut bytes = allocated(length)?;
        bytes.resize(length, 0);
        if let Err(error) = outgoing.file.read_exact(&mut bytes) {
            self.poisoned = true;
            return Err(error.into());
        }
        let offset = outgoing.position;
        outgoing.position += length as u64;
        let done = outgoing.position == outgoing.descriptor.bytes;
        let result = Chunk {
            generation: outgoing.descriptor.generation,
            offset,
            bytes,
            done,
        };
        if done {
            self.outgoing = None;
        }
        Ok(result)
    }
    /// Cancel one outgoing reader without altering any durable image.
    pub fn cancel_read(&mut self) {
        self.outgoing = None;
    }
}
impl Drop for Store {
    fn drop(&mut self) {
        if let Some(incoming) = self.incoming.take() {
            drop(incoming.file);
            let _ = fs::remove_file(
                self.directory
                    .join(name(incoming.descriptor.generation, true)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> io::Result<Self> {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "partitionline-snapshot-inner-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn open(path: &Path) -> Result<Store> {
        Ok(Store::open(
            path,
            Identity::new(
                "snapshot-inner".into(),
                "__cluster_metadata".into(),
                0,
                vec![0],
            )?,
            Limits::new(8192, 8, 4096, 2048, 17, 4, 16)?,
        )?)
    }
    fn prefix() -> Vec<Entry> {
        vec![
            Entry {
                term: 1,
                index: 1,
                barrier: false,
                voters: false,
                payload: b"alpha=a".to_vec(),
            },
            Entry {
                term: 1,
                index: 2,
                barrier: false,
                voters: false,
                payload: b"beta=b".to_vec(),
            },
        ]
    }
    fn phase(index: usize) -> Result<PublicationPhase> {
        match index {
            0 => Ok(PublicationPhase::CandidateSynced),
            1 => Ok(PublicationPhase::ImageRenamed),
            2 => Ok(PublicationPhase::DirectorySynced),
            _ => Err("unreviewed snapshot cut phase".into()),
        }
    }

    #[test]
    fn inner_publication_io_failures_preserve_previous_complete_image() -> Result {
        for index in 0..3 {
            let temp = Temp::new()?;
            let mut store = open(&temp.0)?;
            let entries = prefix();
            store.fault_publication([2; 16], phase(index)?, false);
            store.create([1; 16], LogPosition { term: 1, index: 1 }, &entries[..1])?;
            assert!(matches!(
                store.create([2; 16], LogPosition { term: 1, index: 2 }, &entries),
                Err(Error::Storage(_))
            ));
            assert!(store.poisoned());
            assert!(matches!(store.load([1; 16]), Err(Error::Poisoned)));
            drop(store);
            let mut recovered = open(&temp.0)?;
            assert_eq!(recovered.load([1; 16])?.entries, entries[..1]);
            assert_eq!(recovered.generation_count(), if index == 0 { 1 } else { 2 });
        }
        Ok(())
    }

    #[test]
    fn inner_publication_process_helper() -> Result {
        let Some(directory) = std::env::var_os("PL_SNAPSHOT_INNER_CUT_DIR") else {
            return Ok(());
        };
        let index = std::env::var("PL_SNAPSHOT_INNER_CUT_PHASE")?.parse()?;
        let mut store = open(Path::new(&directory))?;
        store.fault_publication([2; 16], phase(index)?, true);
        store.create([2; 16], LogPosition { term: 1, index: 2 }, &prefix())?;
        Err("configured publication process cut did not execute".into())
    }

    #[test]
    fn inner_publication_process_exits_preserve_previous_complete_image() -> Result {
        for index in 0..3 {
            let temp = Temp::new()?;
            let entries = prefix();
            let mut store = open(&temp.0)?;
            store.create([1; 16], LogPosition { term: 1, index: 1 }, &entries[..1])?;
            drop(store);
            let status = Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "raft::snapshot::tests::inner_publication_process_helper",
                    "--nocapture",
                ])
                .env("PL_SNAPSHOT_INNER_CUT_DIR", &temp.0)
                .env("PL_SNAPSHOT_INNER_CUT_PHASE", index.to_string())
                .status()?;
            assert_eq!(status.code(), Some(44));
            let mut recovered = open(&temp.0)?;
            assert_eq!(recovered.load([1; 16])?.entries, entries[..1]);
            assert_eq!(recovered.generation_count(), if index == 0 { 1 } else { 2 });
        }
        Ok(())
    }

    fn node_config() -> Result<crate::raft::replication::Config> {
        let mut controller =
            crate::raft::protocol::Config::new(0, vec![0], "snapshot-inner".into())?;
        controller.election_timeouts = crate::raft::election::Timeouts::new(5, 5)?;
        let mut config = crate::raft::replication::Config::new(controller);
        config.max_queued_requests = 2;
        Ok(config)
    }

    fn node_open(
        root: &Path,
        fault: Option<(usize, bool)>,
    ) -> Result<crate::raft::replication::Node> {
        let mut store = open(&root.join("images"))?;
        if let Some((index, exit)) = fault {
            store.fault_publication([2; 16], phase(index)?, exit);
        }
        Ok(crate::raft::replication::Node::open_with_snapshots(
            root.join("node.wal"),
            root.join("node.election"),
            node_config()?,
            store,
            0,
        )?)
    }

    fn prepare_node(root: &Path, fault: (usize, bool)) -> Result<crate::raft::replication::Node> {
        let mut node = node_open(root, Some(fault))?;
        let _ = node.campaign(5, 7)?;
        assert_eq!(
            node.state().election.role,
            crate::raft::election::Role::Leader
        );
        assert_eq!(node.activate_leader(5)?, 1);
        assert_eq!(node.propose(b"alpha=a", 5)?, 2);
        let image = node.checkpoint([1; 16], 6)?;
        assert_eq!(image.base, LogPosition { term: 2, index: 2 });
        assert_eq!(node.selected_snapshot()?, Some(image));
        assert_eq!(node.propose(b"beta=b", 7)?, 3);
        assert_eq!(node.state().committed_end, 3);
        assert_eq!(node.state().election.persistent.term, 2);
        assert_eq!(node.state().election.persistent.voted_for, Some(0));
        Ok(node)
    }

    fn copy_raw(source: &Path, destination: &Path, copied: &mut usize) -> Result {
        fs::create_dir(destination)?;
        for item in fs::read_dir(source)? {
            let item = item?;
            *copied += 1;
            if *copied > 64 {
                return Err("snapshot cut artifact file bound exceeded".into());
            }
            let target = destination.join(item.file_name());
            let kind = item.file_type()?;
            if kind.is_dir() {
                copy_raw(&item.path(), &target, copied)?;
            } else if kind.is_file() {
                if item.metadata()?.len() > 1024 * 1024 {
                    return Err("snapshot cut artifact byte bound exceeded".into());
                }
                fs::copy(item.path(), target)?;
            } else {
                return Err("unexpected snapshot cut artifact type".into());
            }
        }
        Ok(())
    }

    fn retain_node_cut(root: &Path, mode: &str, index: usize, stage: &str) -> Result {
        if let Some(destination) = std::env::var_os("PL_SNAPSHOT_NODE_INNER_PROOF_DIR") {
            let parent = PathBuf::from(destination).join(format!("{mode}-{index}"));
            fs::create_dir_all(&parent)?;
            copy_raw(root, &parent.join(stage), &mut 0)?;
            let mut file = File::create(parent.join("history.json"))?;
            writeln!(file, "{{\"schema_version\":1,\"mode\":\"{mode}\",\"publication_phase\":{index},\"local_voter\":0,\"term\":2,\"vote\":0,\"selected_generation\":\"01010101010101010101010101010101\",\"selected_base_index\":2,\"committed_end\":3,\"durable_tail\":3,\"unreceipted_candidate_generation\":\"02020202020202020202020202020202\",\"unreceipted_candidate_base_index\":3,\"committed_payloads\":[\"\",\"alpha=a\",\"beta=b\"],\"scope\":\"actual one-voter Node checkpoint publication cut; local durable replay only, no multi-node catchup claim\"}}")?;
            file.sync_all()?;
        }
        Ok(())
    }

    fn verify_node_replay(root: &Path, index: usize) -> Result {
        let node = node_open(root, None)?;
        let state = node.state();
        assert!(state.ready);
        assert!(!state.poisoned);
        assert_eq!(state.base_position, LogPosition { term: 2, index: 2 });
        assert_eq!(state.last_position, LogPosition { term: 2, index: 3 });
        assert_eq!(state.committed_end, 3);
        assert_eq!(state.election.persistent.term, 2);
        assert_eq!(state.election.persistent.voted_for, Some(0));
        let image = node
            .selected_snapshot()?
            .ok_or("previous checkpoint was not selected")?;
        assert_eq!(image.generation, [1; 16]);
        let records = node.fetch_committed(1, 8, 4096)?;
        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0].kind,
            crate::raft::replication::RecordKind::Barrier
        );
        assert!(records[0].payload.is_empty());
        assert_eq!(records[1].payload, b"alpha=a");
        assert_eq!(records[2].payload, b"beta=b");
        assert_eq!(node.term_at(2)?, Some(2));
        assert_eq!(node.term_at(3)?, Some(2));
        drop(node);
        let mut store = open(&root.join("images"))?;
        assert_eq!(store.generation_count(), if index == 0 { 1 } else { 2 });
        assert_eq!(store.load([1; 16])?.descriptor, image);
        if index != 0 {
            assert_eq!(store.load([2; 16])?.descriptor.base.index, 3);
        }
        Ok(())
    }

    #[test]
    fn node_inner_publication_io_failures_replay_previous_receipt_and_committed_suffix() -> Result {
        for index in 0..3 {
            let temp = Temp::new()?;
            let mut node = prepare_node(&temp.0, (index, false))?;
            assert!(matches!(
                node.checkpoint([2; 16], 8),
                Err(crate::raft::replication::Error::Snapshot(Error::Storage(_)))
            ));
            assert!(node.state().poisoned);
            assert!(node.selected_snapshot().is_err());
            drop(node);
            retain_node_cut(&temp.0, "io", index, "interrupted")?;
            verify_node_replay(&temp.0, index)?;
            retain_node_cut(&temp.0, "io", index, "recovered")?;
        }
        Ok(())
    }

    #[test]
    fn node_inner_publication_process_helper() -> Result {
        let Some(directory) = std::env::var_os("PL_SNAPSHOT_NODE_INNER_CUT_DIR") else {
            return Ok(());
        };
        let index = std::env::var("PL_SNAPSHOT_NODE_INNER_CUT_PHASE")?.parse()?;
        let mut node = prepare_node(Path::new(&directory), (index, true))?;
        node.checkpoint([2; 16], 8)?;
        Err("configured Node publication process cut did not execute".into())
    }

    #[test]
    fn node_inner_publication_process_exits_replay_previous_receipt_and_committed_suffix() -> Result
    {
        for index in 0..3 {
            let temp = Temp::new()?;
            let status = Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "raft::snapshot::tests::node_inner_publication_process_helper",
                    "--nocapture",
                ])
                .env("PL_SNAPSHOT_NODE_INNER_CUT_DIR", &temp.0)
                .env("PL_SNAPSHOT_NODE_INNER_CUT_PHASE", index.to_string())
                .status()?;
            assert_eq!(status.code(), Some(44));
            retain_node_cut(&temp.0, "exit", index, "interrupted")?;
            verify_node_replay(&temp.0, index)?;
            retain_node_cut(&temp.0, "exit", index, "recovered")?;
        }
        Ok(())
    }
}
