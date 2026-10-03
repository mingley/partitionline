//! Durable fixed-member replication primitives with caller-driven typed exchange.
//!
//! This is not a Kafka Fetch codec or an autonomous controller network loop.
//! Metadata indices are inclusive and one-based; zero denotes an empty prefix.
//! Terms use the controller's checked wire epoch plus one representation. Storage
//! paths and peer identities are trusted configuration, not authentication.
//! The append-only operation WAL synchronizes suffix changes and commit markers;
//! readers never observe entries above its durable committed end. Snapshot and
//! dynamic voter transitions belong to separate contracts.
//! Defaults bound records to1MiB, chunks/fetches to2MiB (4MiB configurable
//! ceiling), live content to4096 entries/64MiB and the WAL to65536 operations/
//! 256MiB. Startup uses the Journal's bounded index plus one bounded chunk and
//! at most1MiB of operation-tail positions, released before readiness. Actor
//! queue/canceled-input/unconsumed-receipt/one-in-flight envelopes are charged
//! conservatively against512MiB; returned consumed records are caller-owned.

use super::{
    election::{self, LogPosition, Role, Tally},
    protocol::{self, Controller},
};
use crate::journal::{self, Journal};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{
    sync::{mpsc, oneshot, Mutex, Semaphore, SemaphorePermit},
    task::JoinHandle,
};

const MAGIC: &[u8; 8] = b"PLREPL01";
const MAX_TERM: u64 = i32::MAX as u64 + 1;
const RECORD_HEADER: usize = 24;

/// Positive bounded storage, message and recovery budgets.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    record_bytes: usize,
    chunk_bytes: usize,
    live_entries: usize,
    live_bytes: usize,
    operations: usize,
    wal_bytes: u64,
    fetch_bytes: usize,
}
impl Limits {
    /// Validate conservative ceilings without allocating from any peer field.
    pub fn new(
        record_bytes: usize,
        chunk_bytes: usize,
        live_entries: usize,
        live_bytes: usize,
        operations: usize,
        wal_bytes: u64,
        fetch_bytes: usize,
    ) -> Result<Self, Error> {
        if !(1..=1024 * 1024).contains(&record_bytes)
            || !(1024..=4 * 1024 * 1024).contains(&chunk_bytes)
            || record_bytes
                .checked_add(RECORD_HEADER + 32)
                .is_none_or(|n| n > chunk_bytes)
            || !(1..=4096).contains(&live_entries)
            || !(1..=64 * 1024 * 1024).contains(&live_bytes)
            || live_bytes < record_bytes
            || !(2..=65_536).contains(&operations)
            || !(1024..=256 * 1024 * 1024).contains(&wal_bytes)
            || !(1024..=4 * 1024 * 1024).contains(&fetch_bytes)
            || record_bytes
                .checked_add(std::mem::size_of::<Record>())
                .is_none_or(|n| n > fetch_bytes)
        {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            record_bytes,
            chunk_bytes,
            live_entries,
            live_bytes,
            operations,
            wal_bytes,
            fetch_bytes,
        })
    }
    fn journal(self) -> Result<journal::Limits, Error> {
        Ok(journal::Limits::new(
            self.chunk_bytes,
            self.wal_bytes,
            self.operations,
            self.chunk_bytes + std::mem::size_of::<journal::Entry>(),
        )?)
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            record_bytes: 1024 * 1024,
            chunk_bytes: 2 * 1024 * 1024,
            live_entries: 4096,
            live_bytes: 64 * 1024 * 1024,
            operations: 65_536,
            wal_bytes: 256 * 1024 * 1024,
            fetch_bytes: 2 * 1024 * 1024,
        }
    }
}
/// Fixed controller identity and replication admission/time bounds.
#[derive(Debug, Clone)]
pub struct Config {
    /// Existing fixed trusted-controller configuration; advertisement is unchanged.
    pub controller: protocol::Config,
    /// Durable operation/content/recovery budgets.
    pub limits: Limits,
    /// Pending/completed-unconsumed replication actor slots,1..=1024.
    /// Separate from the standalone controller actor's setting; defaults to16.
    pub max_queued_requests: usize,
    /// Positive interval after which absent current-term quorum contact fences a leader.
    pub quorum_timeout_ms: u64,
}
impl Config {
    /// Defaults retain the controller's explicit membership and queue budgets.
    pub fn new(controller: protocol::Config) -> Self {
        Self {
            controller,
            limits: Limits::default(),
            max_queued_requests: 16,
            quorum_timeout_ms: 1000,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        self.controller.validate()?;
        if !(1..=600_000).contains(&self.quorum_timeout_ms)
            || !(1..=1024).contains(&self.max_queued_requests)
        {
            return Err(Error::InvalidConfig);
        }
        let envelope = self
            .limits
            .chunk_bytes
            .checked_add(self.limits.live_entries * std::mem::size_of::<Record>())
            .ok_or(Error::Bounds)?;
        let resident_slots = self
            .max_queued_requests
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .ok_or(Error::Bounds)?;
        let queue_bytes = envelope
            .max(self.limits.fetch_bytes)
            .max(self.controller.protocol_limits.max_request_bytes())
            .checked_add(std::mem::size_of::<Command>() + std::mem::size_of::<State>())
            .and_then(|n| n.checked_mul(resident_slots))
            .ok_or(Error::Bounds)?;
        if queue_bytes > 512 * 1024 * 1024 {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
}
/// Distinguish opaque metadata from an internal current-term commit barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    /// Caller-owned opaque metadata bytes.
    Data,
    /// Internal empty barrier; not a Kafka control-record serialization.
    Barrier,
}
/// One durable metadata position. Barriers count toward indices but carry no data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Positive normalized core term of the entry.
    pub term: u64,
    /// Inclusive one-based metadata index.
    pub index: u64,
    /// Opaque data or internal barrier.
    pub kind: RecordKind,
    /// Bounded bytes, empty only for a barrier.
    pub payload: Vec<u8>,
}
/// Explicit admission, causal fencing, recovery or durable storage failure.
#[derive(Debug)]
pub enum Error {
    /// Operator bounds/identity are invalid.
    InvalidConfig,
    /// A peer/configured budget would be exceeded before mutation.
    Bounds,
    /// WAL identity does not match this configured fixed group/replica.
    ForeignGroup,
    /// Complete checksummed bytes violate the operation/state contract.
    Corrupt,
    /// A failed/ambiguous persistence attempt requires verified reopening.
    Poisoned,
    /// Local election is not a current replication leader.
    NotLeader,
    /// Typed request/response is stale, forged, conflicting or miscorrelated.
    InvalidPeer,
    /// Fixed membership cannot be changed by this profile.
    MembershipChangeUnsupported,
    /// One outstanding message or the actor queue is already full.
    Busy,
    /// Admission stopped or actor exited.
    Stopped,
    /// Bounded allocation failed.
    Allocation,
    /// Election/controller persistence or parsing failed.
    Controller(protocol::Error),
    /// Existing append-only Journal failure.
    Storage(journal::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "replication: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Self::Controller(e)
    }
}
impl From<journal::Error> for Error {
    fn from(e: journal::Error) -> Self {
        Self::Storage(e)
    }
}

fn reserve(size: usize) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    out.try_reserve_exact(size).map_err(|_| Error::Allocation)?;
    Ok(out)
}
fn header(op: u8, size: usize) -> Result<Vec<u8>, Error> {
    let mut out = reserve(size)?;
    out.extend_from_slice(MAGIC);
    out.push(op);
    out.extend_from_slice(&[0; 7]);
    Ok(out)
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.position.checked_add(n).ok_or(Error::Corrupt)?;
        let result = self.bytes.get(self.position..end).ok_or(Error::Corrupt)?;
        self.position = end;
        Ok(result)
    }
    fn zero(&mut self, n: usize) -> Result<(), Error> {
        if self.take(n)?.iter().any(|x| *x != 0) {
            Err(Error::Corrupt)
        } else {
            Ok(())
        }
    }
    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Error::Corrupt)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Error::Corrupt)?,
        ))
    }
    fn finished(&self) -> Result<(), Error> {
        if self.position != self.bytes.len() {
            Err(Error::Corrupt)
        } else {
            Ok(())
        }
    }
}

struct Log {
    journal: Journal,
    records: Vec<Record>,
    live_bytes: usize,
    committed: u64,
    max_term: u64,
    recovery: journal::Recovery,
    limits: Limits,
    local: u32,
    voters: Vec<u32>,
}
impl Log {
    fn open(path: &Path, config: &Config) -> Result<(Self, Vec<LogPosition>), Error> {
        let (journal, recovery) = Journal::open(path, 0, config.limits.journal()?)?;
        let mut log = Self {
            journal,
            records: Vec::new(),
            live_bytes: 0,
            committed: 0,
            max_term: 0,
            recovery,
            limits: config.limits,
            local: config.controller.local_id,
            voters: config.controller.voters.clone(),
        };
        log.voters.sort_unstable();
        let identity = Self::identity(config)?;
        if log.journal.entry_count() == 0 {
            log.journal.append(1, &identity)?;
        }
        let mut confirmed_tails = Vec::new();
        confirmed_tails
            .try_reserve_exact(log.journal.entry_count())
            .map_err(|_| Error::Allocation)?;
        for offset in 0..log.journal.next_offset() {
            let entries = log.journal.fetch(
                offset,
                1,
                log.limits.chunk_bytes + std::mem::size_of::<journal::Entry>(),
            )?;
            let entry = entries.first().ok_or(Error::Corrupt)?;
            if entry.record_count != 1 || entry.first_offset != offset {
                return Err(Error::Corrupt);
            }
            if offset == 0 {
                if entry.payload != identity {
                    return Err(Error::ForeignGroup);
                }
            } else {
                log.replay(&entry.payload)?;
            }
            confirmed_tails.push(log.last());
        }
        Ok((log, confirmed_tails))
    }
    fn identity(config: &Config) -> Result<Vec<u8>, Error> {
        let c = &config.controller;
        let mut voters = c.voters.clone();
        voters.sort_unstable();
        let mut out = header(
            1,
            32 + voters.len() * 4 + c.cluster_id.len() + c.topic.len(),
        )?;
        out.extend_from_slice(&c.local_id.to_be_bytes());
        out.extend_from_slice(&c.partition.to_be_bytes());
        out.extend_from_slice(&(voters.len() as u16).to_be_bytes());
        out.extend_from_slice(&(c.cluster_id.len() as u16).to_be_bytes());
        out.extend_from_slice(&(c.topic.len() as u16).to_be_bytes());
        out.extend_from_slice(&[0; 2]);
        for voter in voters {
            out.extend_from_slice(&voter.to_be_bytes());
        }
        out.extend_from_slice(c.cluster_id.as_bytes());
        out.extend_from_slice(c.topic.as_bytes());
        Ok(out)
    }
    fn last(&self) -> LogPosition {
        self.records
            .last()
            .map_or(LogPosition::default(), |r| LogPosition {
                term: r.term,
                index: r.index,
            })
    }
    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            Some(0)
        } else {
            usize::try_from(index - 1)
                .ok()
                .and_then(|i| self.records.get(i))
                .map(|r| r.term)
        }
    }
    fn validate_records(&self, records: &[Record]) -> Result<usize, Error> {
        if records.is_empty()
            || records.len() > self.limits.live_entries
            || self
                .records
                .len()
                .checked_add(records.len())
                .is_none_or(|n| n > self.limits.live_entries)
        {
            return Err(Error::Bounds);
        }
        let mut bytes = 32usize;
        let mut live = self.live_bytes;
        let mut index = self.last().index;
        let mut term = self.last().term;
        for r in records {
            index = index.checked_add(1).ok_or(Error::Bounds)?;
            if r.index != index
                || !(1..=MAX_TERM).contains(&r.term)
                || r.term < term
                || r.payload.len() > self.limits.record_bytes
                || (r.kind == RecordKind::Data) == r.payload.is_empty()
            {
                return Err(Error::Corrupt);
            }
            term = r.term;
            bytes = bytes
                .checked_add(RECORD_HEADER)
                .and_then(|n| n.checked_add(r.payload.len()))
                .ok_or(Error::Bounds)?;
            live = live.checked_add(r.payload.len()).ok_or(Error::Bounds)?;
        }
        if bytes > self.limits.chunk_bytes || live > self.limits.live_bytes {
            return Err(Error::Bounds);
        }
        Ok(bytes)
    }
    fn append(&mut self, records: &[Record]) -> Result<(), Error> {
        let bytes = self.validate_records(records)?;
        let mut copied = Vec::new();
        copied
            .try_reserve_exact(records.len())
            .map_err(|_| Error::Allocation)?;
        let mut out = header(2, bytes)?;
        out.extend_from_slice(&records[0].index.to_be_bytes());
        out.extend_from_slice(&(records.len() as u32).to_be_bytes());
        out.extend_from_slice(&[0; 4]);
        for r in records {
            out.extend_from_slice(&r.term.to_be_bytes());
            out.push(u8::from(r.kind == RecordKind::Barrier));
            out.extend_from_slice(&[0; 7]);
            out.extend_from_slice(&(r.payload.len() as u32).to_be_bytes());
            out.extend_from_slice(&[0; 4]);
            out.extend_from_slice(&r.payload);
            let mut payload = reserve(r.payload.len())?;
            payload.extend_from_slice(&r.payload);
            copied.push(Record {
                payload,
                ..r.clone_header()
            });
        }
        self.records
            .try_reserve_exact(records.len())
            .map_err(|_| Error::Allocation)?;
        self.journal.append(1, &out)?;
        for r in copied {
            self.live_bytes += r.payload.len();
            self.max_term = self.max_term.max(r.term);
            self.records.push(r);
        }
        Ok(())
    }
    fn truncate(&mut self, index: u64, term: u64) -> Result<(), Error> {
        let old = self.last();
        if index < self.committed
            || index >= old.index
            || term <= old.term
            || term < self.max_term
            || term > MAX_TERM
        {
            return Err(Error::InvalidPeer);
        }
        let mut out = header(3, 56)?;
        for n in [index, old.term, old.index, term, self.committed] {
            out.extend_from_slice(&n.to_be_bytes());
        }
        self.journal.append(1, &out)?;
        self.truncate_memory(index)?;
        self.max_term = self.max_term.max(term);
        Ok(())
    }
    fn truncate_memory(&mut self, index: u64) -> Result<(), Error> {
        let index = usize::try_from(index).map_err(|_| Error::Corrupt)?;
        for r in self.records.get(index..).ok_or(Error::Corrupt)? {
            self.live_bytes -= r.payload.len();
        }
        self.records.truncate(index);
        Ok(())
    }
    fn commit(
        &mut self,
        index: u64,
        term: u64,
        leader: u32,
        authority: u8,
        sequence: u64,
    ) -> Result<(), Error> {
        if index <= self.committed {
            return Ok(());
        }
        if index > self.last().index
            || !(1..=MAX_TERM).contains(&term)
            || term < self.max_term
            || self
                .term_at(index)
                .is_none_or(|entry_term| entry_term > term)
            || !self.voters.contains(&leader)
            || authority > 1
            || (authority == 0
                && (leader != self.local || sequence != 0 || self.term_at(index) != Some(term)))
            || (authority == 1 && (leader == self.local || sequence == 0))
        {
            return Err(Error::InvalidPeer);
        }
        let mut out = header(4, 48)?;
        out.extend_from_slice(&index.to_be_bytes());
        out.extend_from_slice(&term.to_be_bytes());
        out.extend_from_slice(&leader.to_be_bytes());
        out.push(authority);
        out.extend_from_slice(&[0; 3]);
        out.extend_from_slice(&sequence.to_be_bytes());
        self.journal.append(1, &out)?;
        self.committed = index;
        self.max_term = self.max_term.max(term);
        Ok(())
    }
    fn replay(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let mut r = Reader::new(bytes);
        if r.take(8)? != MAGIC {
            return Err(Error::Corrupt);
        }
        let op = r.u8()?;
        r.zero(7)?;
        match op {
            2 => {
                let start = r.u64()?;
                let count = usize::try_from(r.u32()?).map_err(|_| Error::Corrupt)?;
                r.zero(4)?;
                if count == 0 || count > self.limits.live_entries {
                    return Err(Error::Corrupt);
                }
                let mut records = Vec::new();
                records
                    .try_reserve_exact(count)
                    .map_err(|_| Error::Allocation)?;
                for i in 0..count {
                    let term = r.u64()?;
                    let kind = match r.u8()? {
                        0 => RecordKind::Data,
                        1 => RecordKind::Barrier,
                        _ => return Err(Error::Corrupt),
                    };
                    r.zero(7)?;
                    let length = usize::try_from(r.u32()?).map_err(|_| Error::Corrupt)?;
                    r.zero(4)?;
                    if length > self.limits.record_bytes {
                        return Err(Error::Corrupt);
                    }
                    let mut payload = reserve(length)?;
                    payload.extend_from_slice(r.take(length)?);
                    records.push(Record {
                        term,
                        index: start.checked_add(i as u64).ok_or(Error::Corrupt)?,
                        kind,
                        payload,
                    });
                }
                r.finished()?;
                self.validate_records(&records)?;
                self.records
                    .try_reserve_exact(count)
                    .map_err(|_| Error::Allocation)?;
                for record in records {
                    self.live_bytes += record.payload.len();
                    self.max_term = self.max_term.max(record.term);
                    self.records.push(record);
                }
            }
            3 => {
                let index = r.u64()?;
                let old = LogPosition {
                    term: r.u64()?,
                    index: r.u64()?,
                };
                let term = r.u64()?;
                let floor = r.u64()?;
                r.finished()?;
                if old != self.last()
                    || floor != self.committed
                    || index < floor
                    || index >= old.index
                    || term <= old.term
                    || term < self.max_term
                    || term > MAX_TERM
                {
                    return Err(Error::Corrupt);
                }
                self.truncate_memory(index)?;
                self.max_term = self.max_term.max(term);
            }
            4 => {
                let index = r.u64()?;
                let term = r.u64()?;
                let leader = r.u32()?;
                let authority = r.u8()?;
                r.zero(3)?;
                let sequence = r.u64()?;
                r.finished()?;
                if index <= self.committed
                    || index > self.last().index
                    || !(1..=MAX_TERM).contains(&term)
                    || term < self.max_term
                    || self
                        .term_at(index)
                        .is_none_or(|entry_term| entry_term > term)
                    || !self.voters.contains(&leader)
                    || authority > 1
                    || (authority == 0
                        && (leader != self.local
                            || sequence != 0
                            || self.term_at(index) != Some(term)))
                    || (authority == 1 && (leader == self.local || sequence == 0))
                {
                    return Err(Error::Corrupt);
                }
                self.committed = index;
                self.max_term = self.max_term.max(term);
            }
            _ => return Err(Error::Corrupt),
        }
        Ok(())
    }
    fn fetch(
        &self,
        from: u64,
        end: u64,
        entries: usize,
        bytes: usize,
    ) -> Result<Vec<Record>, Error> {
        if entries == 0
            || entries > self.limits.live_entries
            || bytes == 0
            || bytes > self.limits.fetch_bytes
            || from == 0
            || end > self.last().index
        {
            return Err(Error::Bounds);
        }
        let start = usize::try_from(from - 1).map_err(|_| Error::Bounds)?;
        let mut out = Vec::new();
        let mut used = 0usize;
        for record in self.records.get(start..).unwrap_or(&[]) {
            if record.index > end || out.len() == entries {
                break;
            }
            let cost = std::mem::size_of::<Record>()
                .checked_add(record.payload.len())
                .ok_or(Error::Bounds)?;
            if used.checked_add(cost).is_none_or(|n| n > bytes) {
                break;
            }
            out.try_reserve_exact(1).map_err(|_| Error::Allocation)?;
            let mut payload = reserve(record.payload.len())?;
            payload.extend_from_slice(&record.payload);
            out.push(Record {
                payload,
                ..record.clone_header()
            });
            used += cost;
        }
        Ok(out)
    }
}
impl Record {
    fn clone_header(&self) -> Self {
        Self {
            term: self.term,
            index: self.index,
            kind: self.kind,
            payload: Vec::new(),
        }
    }
}

impl Record {
    /// Construct a checked internal barrier, explicitly visible in durable traces.
    pub fn barrier(term: u64, index: u64) -> Result<Self, Error> {
        if !(1..=MAX_TERM).contains(&term) || index == 0 {
            return Err(Error::Bounds);
        }
        Ok(Self {
            term,
            index,
            kind: RecordKind::Barrier,
            payload: Vec::new(),
        })
    }
}
/// Bounded typed request; transport must authenticate the configured leader separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Configured sending leader.
    pub leader: u32,
    /// Configured receiving follower.
    pub peer: u32,
    /// Positive owner-assigned correlation, one outstanding request per peer.
    pub sequence: u64,
    /// Current normalized leader term.
    pub term: u64,
    /// Prefix position immediately before the entries.
    pub previous: LogPosition,
    /// Leader's synchronized committed end.
    pub leader_commit: u64,
    /// Contiguous bounded records; empty means a prefix-verifying heartbeat.
    pub entries: Vec<Record>,
}
/// Durable typed reply; a successful match never exceeds the correlated request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Response {
    /// Configured replying follower.
    pub peer: u32,
    /// Original leader.
    pub leader: u32,
    /// Original request correlation.
    pub sequence: u64,
    /// Current normalized follower term.
    pub term: u64,
    /// Matching durable prefix was confirmed before this reply.
    pub success: bool,
    /// Exact request-matched prefix on success; empty otherwise.
    pub matched: LogPosition,
    /// Positive retry index on rejection; zero on success.
    pub conflict_index: u64,
}
/// Confirmed storage and election diagnostics; no volatile role is a read lease.
#[derive(Debug, Clone, Copy)]
pub struct State {
    /// Replica identity.
    pub local_id: u32,
    /// Initial profile has no snapshot base; `(0,0)` denotes empty.
    pub base_position: LogPosition,
    /// Durable local tail, including uncommitted records.
    pub last_position: LogPosition,
    /// Inclusive committed metadata end, synchronized before visibility.
    pub committed_end: u64,
    /// Same election core used by the v0 controller RPCs.
    pub election: election::State,
    /// An ambiguous persistence failure prevents admission until reopening.
    pub poisoned: bool,
    /// WAL and election summaries agree and storage is usable.
    pub ready: bool,
    /// Term whose explicit local barrier has activated replication leadership.
    pub active_term: Option<u64>,
    /// Confirmed outer Journal operation count.
    pub wal_durable_ops: usize,
    /// Confirmed election snapshot count.
    pub election_durable_states: usize,
    /// Existing outer-Journal recovery receipt.
    pub recovery: journal::Recovery,
}
#[derive(Debug, Clone, Copy)]
struct Outstanding {
    sequence: u64,
    term: u64,
    previous: LogPosition,
    target: LogPosition,
}
struct Peer {
    id: u32,
    next: u64,
    matched: u64,
    contact: Option<u64>,
    outstanding: Option<Outstanding>,
}
/// Exclusive synchronous durable owner. Run it on a dedicated storage thread.
///
/// Election RPCs and typed replication share one core. Calls must use a monotonic
/// caller clock. Successful follower replies require both content and election
/// summaries synchronized. A local proposal is an uncommitted position, never
/// an acknowledgment of application visibility. Membership is immutable here.
pub struct Node {
    config: Config,
    controller: Controller,
    log: Log,
    peers: Vec<Peer>,
    sequence: u64,
    active_term: Option<u64>,
    grace_deadline: u64,
    now_ms: u64,
    poisoned: bool,
}
impl Node {
    /// Recover both journals, validate identity/content, and reconcile before admission.
    ///
    /// Election term/vote are replica-local. Recovery never invents a majority or
    /// leadership. A complete ambiguous append may be recovered; a torn tail is
    /// handled by the existing Journal policy and complete corrupt bytes fail.
    pub fn open(
        wal_path: impl AsRef<Path>,
        election_path: impl AsRef<Path>,
        config: Config,
        now_ms: u64,
    ) -> Result<Self, Error> {
        config.validate()?;
        let (log, confirmed_tails) = Log::open(wal_path.as_ref(), &config)?;
        let mut controller = Controller::open(election_path, config.controller.clone(), now_ms)?;
        let old = controller.state().persistent;
        if log.max_term > old.term
            || log.committed > log.last().index
            || !confirmed_tails.contains(&old.log)
        {
            return Err(Error::Corrupt);
        }
        controller.reconcile_durable_log(old.log, log.last(), log.committed)?;
        drop(confirmed_tails);
        let mut peers = Vec::new();
        peers
            .try_reserve_exact(config.controller.voters.len() - 1)
            .map_err(|_| Error::Allocation)?;
        for id in &config.controller.voters {
            if *id != config.controller.local_id {
                peers.push(Peer {
                    id: *id,
                    next: log.last().index + 1,
                    matched: 0,
                    contact: None,
                    outstanding: None,
                });
            }
        }
        Ok(Self {
            config,
            controller,
            log,
            peers,
            sequence: 0,
            active_term: None,
            grace_deadline: 0,
            now_ms,
            poisoned: false,
        })
    }
    /// Borrow the immutable fixed group configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }
    /// Confirmed state; poisoned content cannot be fetched or mutated.
    pub fn state(&self) -> State {
        let election = self.controller.state();
        let poisoned = self.poisoned || election.poisoned;
        State {
            local_id: self.config.controller.local_id,
            base_position: LogPosition::default(),
            last_position: self.log.last(),
            committed_end: self.log.committed,
            election,
            poisoned,
            ready: !poisoned && election.persistent.log == self.log.last(),
            active_term: self.active_term,
            wal_durable_ops: self.log.journal.entry_count(),
            election_durable_states: self.controller.durable_states(),
            recovery: self.log.recovery,
        }
    }
    fn ready(&self) -> Result<(), Error> {
        if self.state().ready {
            Ok(())
        } else {
            Err(Error::Poisoned)
        }
    }
    fn clock(&mut self, now: u64) -> Result<(), Error> {
        self.ready()?;
        if now < self.now_ms {
            return Err(Error::InvalidPeer);
        }
        self.now_ms = now;
        Ok(())
    }
    fn reset_authority(&mut self) {
        self.active_term = None;
        for peer in &mut self.peers {
            peer.outstanding = None;
            peer.contact = None;
            peer.matched = 0;
        }
    }
    fn update_authority(&mut self) {
        let state = self.controller.state();
        if state.role != Role::Leader
            || self
                .active_term
                .is_some_and(|term| term != state.persistent.term)
        {
            self.reset_authority();
        }
    }
    fn controller_result<T>(&mut self, result: Result<T, protocol::Error>) -> Result<T, Error> {
        if self.controller.state().poisoned {
            self.poisoned = true;
        }
        self.update_authority();
        result.map_err(Error::Controller)
    }
    fn sync_summary(&mut self) -> Result<(), Error> {
        let old = self.controller.state().persistent.log;
        if let Err(error) =
            self.controller
                .reconcile_durable_log(old, self.log.last(), self.log.committed)
        {
            self.poisoned = true;
            self.reset_authority();
            return Err(error.into());
        }
        Ok(())
    }
    fn storage_result<T>(&mut self, result: Result<T, Error>) -> Result<T, Error> {
        if matches!(
            &result,
            Err(Error::Storage(
                journal::Error::Io(_)
                    | journal::Error::Poisoned
                    | journal::Error::ChangedFile
                    | journal::Error::Corrupt { .. }
            ))
        ) {
            self.poisoned = true;
            self.reset_authority();
        }
        result
    }
    /// Existing bounded v0 controller request path, sharing this recovered owner.
    pub fn respond_controller(&mut self, input: &[u8], now_ms: u64) -> Result<Vec<u8>, Error> {
        self.clock(now_ms)?;
        let result = self.controller.respond(input, now_ms);
        self.controller_result(result)
    }
    /// Trigger a due actual election and return its existing Vote0 request payload.
    pub fn campaign(&mut self, now_ms: u64, correlation: i32) -> Result<Option<Vec<u8>>, Error> {
        self.clock(now_ms)?;
        let result = self.controller.tick(now_ms, correlation);
        self.controller_result(result)
    }
    /// Count an independently correlated existing Vote0 reply in the election core.
    pub fn receive_vote(
        &mut self,
        peer: u32,
        correlation: i32,
        input: &[u8],
        now_ms: u64,
    ) -> Result<Tally, Error> {
        self.clock(now_ms)?;
        let result = self
            .controller
            .receive_vote(peer, correlation, input, now_ms);
        self.controller_result(result)
    }
    /// Persist a visible current-term barrier after an actual majority election.
    ///
    /// This explicit step starts a bounded initial contact interval; it does not
    /// commit the barrier until a durable strict majority matches it. Repeated
    /// activation in one term returns the existing barrier without extending time.
    pub fn activate_leader(&mut self, now_ms: u64) -> Result<u64, Error> {
        self.clock(now_ms)?;
        let state = self.controller.state();
        if state.role != Role::Leader {
            return Err(Error::NotLeader);
        }
        if self.active_term == Some(state.persistent.term) {
            return self
                .log
                .records
                .iter()
                .find(|r| r.term == state.persistent.term && r.kind == RecordKind::Barrier)
                .map(|r| r.index)
                .ok_or(Error::Corrupt);
        }
        let deadline = now_ms
            .checked_add(self.config.quorum_timeout_ms)
            .ok_or(Error::Bounds)?;
        let index = self.log.last().index.checked_add(1).ok_or(Error::Bounds)?;
        let record = Record::barrier(state.persistent.term, index)?;
        let result = self.log.append(&[record]);
        self.storage_result(result)?;
        self.sync_summary()?;
        self.active_term = Some(state.persistent.term);
        self.grace_deadline = deadline;
        for peer in &mut self.peers {
            peer.next = index + 1;
            peer.matched = 0;
            peer.contact = None;
            peer.outstanding = None;
        }
        self.advance_commit()?;
        Ok(index)
    }
    fn quorum_recent(&self, now: u64) -> bool {
        1 + self
            .peers
            .iter()
            .filter(|p| {
                p.contact
                    .is_some_and(|seen| now.saturating_sub(seen) <= self.config.quorum_timeout_ms)
            })
            .count()
            > self.config.controller.voters.len() / 2
    }
    /// Fence a leader after its bounded initial interval or last quorum contact expires.
    pub fn poll(&mut self, now_ms: u64) -> Result<bool, Error> {
        self.clock(now_ms)?;
        self.update_authority();
        if self.active_term.is_some()
            && now_ms >= self.grace_deadline
            && !self.quorum_recent(now_ms)
        {
            let result = self.controller.lose_quorum(now_ms);
            self.controller_result(result)?;
            self.reset_authority();
            return Ok(true);
        }
        Ok(false)
    }
    fn leader(&mut self, now: u64) -> Result<u64, Error> {
        self.poll(now)?;
        let state = self.controller.state();
        if state.role != Role::Leader || self.active_term != Some(state.persistent.term) {
            return Err(Error::NotLeader);
        }
        Ok(state.persistent.term)
    }
    /// Synchronize opaque metadata locally; returned index is not yet committed.
    pub fn propose(&mut self, payload: &[u8], now_ms: u64) -> Result<u64, Error> {
        let term = self.leader(now_ms)?;
        if payload.is_empty() || payload.len() > self.config.limits.record_bytes {
            return Err(Error::Bounds);
        }
        let index = self.log.last().index.checked_add(1).ok_or(Error::Bounds)?;
        let mut bytes = reserve(payload.len())?;
        bytes.extend_from_slice(payload);
        let result = self.log.append(&[Record {
            term,
            index,
            kind: RecordKind::Data,
            payload: bytes,
        }]);
        self.storage_result(result)?;
        self.sync_summary()?;
        self.advance_commit()?;
        Ok(index)
    }
    fn peer_position(&self, peer: u32) -> Result<usize, Error> {
        self.peers
            .iter()
            .position(|p| p.id == peer)
            .ok_or(Error::InvalidPeer)
    }
    /// Prepare at most one bounded outstanding exchange for a configured follower.
    pub fn prepare(&mut self, peer: u32, now_ms: u64) -> Result<Request, Error> {
        let term = self.leader(now_ms)?;
        let position = self.peer_position(peer)?;
        if self.peers[position].outstanding.is_some() {
            return Err(Error::Busy);
        }
        let next = self.peers[position].next;
        let previous = LogPosition {
            index: next - 1,
            term: self.log.term_at(next - 1).ok_or(Error::Corrupt)?,
        };
        let mut entries = self.log.fetch(
            next,
            self.log.last().index,
            self.config.limits.live_entries,
            self.config.limits.fetch_bytes,
        )?;
        let mut wire_bytes = 32usize;
        let mut count = 0;
        for record in &entries {
            wire_bytes = wire_bytes
                .checked_add(RECORD_HEADER + record.payload.len())
                .ok_or(Error::Bounds)?;
            if wire_bytes > self.config.limits.chunk_bytes {
                break;
            }
            count += 1;
        }
        entries.truncate(count);
        if entries.is_empty() && next <= self.log.last().index {
            return Err(Error::Bounds);
        }
        let target = entries.last().map_or(previous, |r| LogPosition {
            term: r.term,
            index: r.index,
        });
        let sequence = self.sequence.checked_add(1).ok_or(Error::Bounds)?;
        self.peers[position].outstanding = Some(Outstanding {
            sequence,
            term,
            previous,
            target,
        });
        self.sequence = sequence;
        Ok(Request {
            leader: self.config.controller.local_id,
            peer,
            sequence,
            term,
            previous,
            leader_commit: self.log.committed,
            entries,
        })
    }
    /// Release a timed-out correlation for retry; any late reply becomes invalid.
    pub fn timeout_peer(&mut self, peer: u32, sequence: u64) -> Result<(), Error> {
        self.ready()?;
        let position = self.peer_position(peer)?;
        if self.peers[position]
            .outstanding
            .is_none_or(|o| o.sequence != sequence)
        {
            return Err(Error::InvalidPeer);
        }
        self.peers[position].outstanding = None;
        Ok(())
    }
    fn validate_request(&self, request: &Request) -> Result<LogPosition, Error> {
        if request.peer != self.config.controller.local_id
            || request.leader == request.peer
            || !self.config.controller.voters.contains(&request.leader)
            || request.sequence == 0
            || !(1..=MAX_TERM).contains(&request.term)
            || (request.previous.term == 0) != (request.previous.index == 0)
            || request.previous.term > request.term
            || request.entries.len() > self.config.limits.live_entries
        {
            return Err(Error::InvalidPeer);
        }
        let mut target = request.previous;
        let mut bytes = 32usize;
        for r in &request.entries {
            let index = target.index.checked_add(1).ok_or(Error::Bounds)?;
            if r.index != index
                || !(1..=request.term).contains(&r.term)
                || r.term < target.term
                || r.payload.len() > self.config.limits.record_bytes
                || (r.kind == RecordKind::Data) == r.payload.is_empty()
            {
                return Err(Error::InvalidPeer);
            }
            bytes = bytes
                .checked_add(RECORD_HEADER + r.payload.len())
                .ok_or(Error::Bounds)?;
            target = LogPosition {
                term: r.term,
                index: r.index,
            };
        }
        if bytes > self.config.limits.chunk_bytes {
            return Err(Error::Bounds);
        }
        Ok(target)
    }
    fn response(
        &self,
        request: &Request,
        success: bool,
        matched: LogPosition,
        conflict_index: u64,
    ) -> Response {
        Response {
            peer: self.config.controller.local_id,
            leader: request.leader,
            sequence: request.sequence,
            term: self.controller.state().persistent.term,
            success,
            matched,
            conflict_index,
        }
    }
    /// Validate a bounded typed request and synchronize both journals before success.
    ///
    /// Follower commit is capped by this request's matched end, so a partial
    /// catch-up cannot expose an old unverified local suffix. Committed records
    /// are never truncated. Same-term conflicting bytes fail rather than repair.
    pub fn receive(&mut self, request: &Request, now_ms: u64) -> Result<Response, Error> {
        self.clock(now_ms)?;
        let target = self.validate_request(request)?;
        let current = self.controller.state().persistent.term;
        if request.term < current {
            return Ok(self.response(
                request,
                false,
                LogPosition::default(),
                self.log.last().index + 1,
            ));
        }
        // Preflight complete repair and resulting live bounds before any epoch/WAL mutation.
        let mut append_from = request.entries.len();
        let mut truncate_to = None;
        if self.log.term_at(request.previous.index) == Some(request.previous.term) {
            for (i, r) in request.entries.iter().enumerate() {
                let existing = usize::try_from(r.index - 1)
                    .ok()
                    .and_then(|index| self.log.records.get(index));
                match existing {
                    Some(old) if old == r => {}
                    Some(old) if old.term == r.term => return Err(Error::InvalidPeer),
                    Some(_) => {
                        if r.index <= self.log.committed || request.term <= self.log.last().term {
                            return Err(Error::InvalidPeer);
                        }
                        append_from = i;
                        truncate_to = Some(r.index - 1);
                        break;
                    }
                    None => {
                        append_from = i;
                        break;
                    }
                }
            }
            let prefix = truncate_to.unwrap_or(self.log.last().index);
            let added = &request.entries[append_from..];
            if prefix
                .checked_add(added.len() as u64)
                .is_none_or(|n| n > self.config.limits.live_entries as u64)
            {
                return Err(Error::Bounds);
            }
            let retained_bytes: usize = self
                .log
                .records
                .iter()
                .take(prefix as usize)
                .map(|r| r.payload.len())
                .sum();
            let added_bytes = added.iter().try_fold(0usize, |n, r| {
                n.checked_add(r.payload.len()).ok_or(Error::Bounds)
            })?;
            if retained_bytes
                .checked_add(added_bytes)
                .is_none_or(|n| n > self.config.limits.live_bytes)
            {
                return Err(Error::Bounds);
            }
        }
        let observed =
            self.controller
                .observe_replication_leader(request.leader, request.term, now_ms);
        let observed = self.controller_result(observed)?;
        if !observed {
            return Ok(self.response(
                request,
                false,
                LogPosition::default(),
                self.log.last().index + 1,
            ));
        }
        if self.log.term_at(request.previous.index) != Some(request.previous.term) {
            let hint = request.previous.index.min(self.log.last().index + 1).max(1);
            return Ok(self.response(request, false, LogPosition::default(), hint));
        }
        if let Some(index) = truncate_to {
            let result = self.log.truncate(index, request.term);
            self.storage_result(result)?;
            self.sync_summary()?;
        }
        if append_from < request.entries.len() {
            let result = self.log.append(&request.entries[append_from..]);
            self.storage_result(result)?;
            self.sync_summary()?;
        }
        let commit = request
            .leader_commit
            .min(self.log.last().index)
            .min(target.index);
        let result = self
            .log
            .commit(commit, request.term, request.leader, 1, request.sequence);
        self.storage_result(result)?;
        Ok(self.response(request, true, target, 0))
    }
    /// Validate a single correlated durable reply before updating distinct majority progress.
    pub fn acknowledge(
        &mut self,
        peer: u32,
        response: Response,
        now_ms: u64,
    ) -> Result<u64, Error> {
        let term = self.leader(now_ms)?;
        let position = self.peer_position(peer)?;
        let outstanding = self.peers[position].outstanding.ok_or(Error::InvalidPeer)?;
        if response.peer != peer
            || response.leader != self.config.controller.local_id
            || response.sequence != outstanding.sequence
            || outstanding.term != term
            || response.term < term
            || response.term > MAX_TERM
        {
            return Err(Error::InvalidPeer);
        }
        if response.term > term {
            if response.success
                || response.matched != LogPosition::default()
                || response.conflict_index == 0
            {
                return Err(Error::InvalidPeer);
            }
            let result = self
                .controller
                .adopt_epoch((response.term - 1) as i32, now_ms);
            self.controller_result(result)?;
            return Ok(self.log.committed);
        }
        if response.success {
            if response.matched != outstanding.target
                || response.conflict_index != 0
                || response.matched.index > self.log.last().index
                || self.log.term_at(response.matched.index) != Some(response.matched.term)
            {
                return Err(Error::InvalidPeer);
            }
            self.peers[position].matched = self.peers[position].matched.max(response.matched.index);
            self.peers[position].next =
                response.matched.index.checked_add(1).ok_or(Error::Bounds)?;
        } else {
            if response.matched != LogPosition::default()
                || response.conflict_index == 0
                || response.conflict_index > outstanding.previous.index.max(1)
            {
                return Err(Error::InvalidPeer);
            }
            self.peers[position].next = response.conflict_index;
        }
        self.peers[position].outstanding = None;
        self.peers[position].contact = Some(now_ms);
        self.advance_commit()?;
        Ok(self.log.committed)
    }
    fn advance_commit(&mut self) -> Result<(), Error> {
        let state = self.controller.state();
        if state.role != Role::Leader || self.active_term != Some(state.persistent.term) {
            return Err(Error::NotLeader);
        }
        let mut matched = [0u64; 64];
        matched[0] = self.log.last().index;
        for (i, peer) in self.peers.iter().enumerate() {
            matched[i + 1] = peer.matched;
        }
        let count = self.peers.len() + 1;
        matched[..count].sort_unstable();
        let index = matched[count - (count / 2 + 1)];
        if index > self.log.committed && self.log.term_at(index) == Some(state.persistent.term) {
            let result = self.log.commit(
                index,
                state.persistent.term,
                self.config.controller.local_id,
                0,
                0,
            );
            self.storage_result(result)?;
        }
        Ok(())
    }
    /// Bounded reads expose only synchronized committed records; no linearizable lease is asserted.
    pub fn fetch_committed(
        &self,
        from: u64,
        entries: usize,
        bytes: usize,
    ) -> Result<Vec<Record>, Error> {
        self.ready()?;
        self.log.fetch(from, self.log.committed, entries, bytes)
    }
    /// Look up a confirmed term for future snapshot/replay boundary reconciliation.
    pub fn term_at(&self, index: u64) -> Result<Option<u64>, Error> {
        self.ready()?;
        Ok(self.log.term_at(index))
    }
    /// Explicitly reject membership changes; the dynamic-quorum task remains open.
    pub fn reconfigure(&mut self, _voters: &[u32]) -> Result<(), Error> {
        Err(Error::MembershipChangeUnsupported)
    }
}

enum Command {
    Controller(Vec<u8>, oneshot::Sender<Result<Vec<u8>, Error>>),
    Campaign(i32, oneshot::Sender<Result<Option<Vec<u8>>, Error>>),
    Vote(u32, i32, Vec<u8>, oneshot::Sender<Result<Tally, Error>>),
    Activate(oneshot::Sender<Result<u64, Error>>),
    Propose(Vec<u8>, oneshot::Sender<Result<u64, Error>>),
    Prepare(u32, oneshot::Sender<Result<Request, Error>>),
    Receive(Request, oneshot::Sender<Result<Response, Error>>),
    Acknowledge(u32, Response, oneshot::Sender<Result<u64, Error>>),
    Timeout(u32, u64, oneshot::Sender<Result<(), Error>>),
    Poll(oneshot::Sender<Result<bool, Error>>),
    State(oneshot::Sender<Result<State, Error>>),
    Fetch(
        u64,
        usize,
        usize,
        oneshot::Sender<Result<Vec<Record>, Error>>,
    ),
    Stop,
}
fn deliver<T>(reply: oneshot::Sender<Result<T, Error>>, work: impl FnOnce() -> Result<T, Error>) {
    if !reply.is_closed() {
        drop(reply.send(work()));
    }
}
fn run_actor(
    mut node: Node,
    mut receiver: mpsc::Receiver<Command>,
    stopping: Arc<AtomicBool>,
    start: Instant,
) -> State {
    while let Some(command) = receiver.blocking_recv() {
        if stopping.load(Ordering::Acquire) {
            break;
        }
        let now = u64::try_from(start.elapsed().as_millis()).map_err(|_| Error::Bounds);
        match command {
            Command::Stop => break,
            Command::Controller(bytes, reply) => {
                deliver(reply, || node.respond_controller(&bytes, now?))
            }
            Command::Campaign(correlation, reply) => {
                deliver(reply, || node.campaign(now?, correlation))
            }
            Command::Vote(peer, correlation, bytes, reply) => {
                deliver(reply, || node.receive_vote(peer, correlation, &bytes, now?))
            }
            Command::Activate(reply) => deliver(reply, || node.activate_leader(now?)),
            Command::Propose(bytes, reply) => deliver(reply, || node.propose(&bytes, now?)),
            Command::Prepare(peer, reply) => deliver(reply, || node.prepare(peer, now?)),
            Command::Receive(request, reply) => deliver(reply, || node.receive(&request, now?)),
            Command::Acknowledge(peer, response, reply) => {
                deliver(reply, || node.acknowledge(peer, response, now?))
            }
            Command::Timeout(peer, sequence, reply) => {
                deliver(reply, || node.timeout_peer(peer, sequence))
            }
            Command::Poll(reply) => deliver(reply, || node.poll(now?)),
            Command::State(reply) => deliver(reply, || {
                node.ready()?;
                Ok(node.state())
            }),
            Command::Fetch(from, entries, bytes, reply) => {
                deliver(reply, || node.fetch_committed(from, entries, bytes))
            }
        }
    }

    node.state()
}
/// One bounded blocking storage actor for controller RPCs and typed replication.
///
/// This owns no outbound network, Kafka replication codec or autonomous timer.
/// Callers drive campaign, quorum polls and typed exchange. The same owner handles
/// the unchanged controller-v0 listener profile. Queued cancellation skips work;
/// cancellation while an fsync is executing has an ambiguous client outcome.
/// Admission slots remain held until completed replies are consumed; returned
/// records become caller-owned after consumption. Canceled queued work is also
/// bounded by the channel, with at most one additional in-flight operation.
/// Admission is stopped before joined shutdown; a canceled join can be retried.
pub struct ReplicationHandler {
    sender: mpsc::Sender<Command>,
    stopping: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
    config: Config,
    admission: Semaphore,
}
impl ReplicationHandler {
    /// Open and reconcile exclusively owned storage before accepting asynchronous work.
    pub async fn open(
        wal_path: impl AsRef<Path>,
        election_path: impl AsRef<Path>,
        config: Config,
    ) -> Result<Self, Error> {
        config.validate()?;
        let wal = wal_path.as_ref().to_owned();
        let election = election_path.as_ref().to_owned();
        let local = config.clone();
        let node = tokio::task::spawn_blocking(move || Node::open(wal, election, local, 0))
            .await
            .map_err(|_| Error::Stopped)??;
        let (sender, receiver) = mpsc::channel(config.max_queued_requests);
        let stopping = Arc::new(AtomicBool::new(false));
        let local_stop = stopping.clone();
        let start = Instant::now();
        let join = tokio::task::spawn_blocking(move || {
            let _confirmed = run_actor(node, receiver, local_stop, start);
        });
        Ok(Self {
            sender,
            stopping,
            join: Mutex::new(Some(join)),
            admission: Semaphore::new(config.max_queued_requests),
            config,
        })
    }
    fn admit(&self) -> Result<SemaphorePermit<'_>, Error> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        self.admission.try_acquire().map_err(|_| Error::Busy)
    }
    fn enqueue(&self, command: Command) -> Result<(), Error> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        self.sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => Error::Busy,
            mpsc::error::TrySendError::Closed(_) => Error::Stopped,
        })
    }
    async fn receive_result<T>(receiver: oneshot::Receiver<Result<T, Error>>) -> Result<T, Error> {
        receiver.await.map_err(|_| Error::Stopped)?
    }
    /// Drive the existing controller candidacy timer and encoded Vote0 request.
    pub async fn campaign(&self, correlation: i32) -> Result<Option<Vec<u8>>, Error> {
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Campaign(correlation, sender))?;
        Self::receive_result(receiver).await
    }
    /// Count a bounded, correlated existing Vote0 response in the owned election.
    pub async fn receive_vote(
        &self,
        peer: u32,
        correlation: i32,
        input: Vec<u8>,
    ) -> Result<Tally, Error> {
        if input.capacity() > self.config.controller.protocol_limits.max_request_bytes() {
            return Err(Error::Bounds);
        }
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Vote(peer, correlation, input, sender))?;
        Self::receive_result(receiver).await
    }
    /// Explicitly persist the elected leader's internal current-term barrier.
    pub async fn activate_leader(&self) -> Result<u64, Error> {
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Activate(sender))?;
        Self::receive_result(receiver).await
    }
    /// Synchronize an opaque local proposal; completion does not mean committed.
    pub async fn propose(&self, bytes: Vec<u8>) -> Result<u64, Error> {
        if bytes.is_empty() || bytes.capacity() > self.config.limits.record_bytes {
            return Err(Error::Bounds);
        }
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Propose(bytes, sender))?;
        Self::receive_result(receiver).await
    }
    /// Prepare one bounded correlation for a configured typed peer.
    pub async fn prepare(&self, peer: u32) -> Result<Request, Error> {
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Prepare(peer, sender))?;
        Self::receive_result(receiver).await
    }
    /// Queue only a count/byte-bounded typed request; semantic validation remains owner-local.
    pub async fn receive(&self, request: Request) -> Result<Response, Error> {
        if request.entries.capacity() > self.config.limits.live_entries {
            return Err(Error::Bounds);
        }
        let mut bytes = 32usize;
        for record in &request.entries {
            if record.payload.capacity() > self.config.limits.record_bytes {
                return Err(Error::Bounds);
            }
            bytes = bytes
                .checked_add(RECORD_HEADER + record.payload.capacity())
                .ok_or(Error::Bounds)?;
        }
        if bytes > self.config.limits.chunk_bytes {
            return Err(Error::Bounds);
        }
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Receive(request, sender))?;
        Self::receive_result(receiver).await
    }
    /// Count an exact outstanding durable reply after the actor's current-term fencing.
    pub async fn acknowledge(&self, peer: u32, response: Response) -> Result<u64, Error> {
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Acknowledge(peer, response, sender))?;
        Self::receive_result(receiver).await
    }
    /// Release a timed-out correlation without accepting any late reply.
    pub async fn timeout_peer(&self, peer: u32, sequence: u64) -> Result<(), Error> {
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Timeout(peer, sequence, sender))?;
        Self::receive_result(receiver).await
    }
    /// Drive bounded current-term quorum expiration; no background timer is implied.
    pub async fn poll(&self) -> Result<bool, Error> {
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Poll(sender))?;
        Self::receive_result(receiver).await
    }
    /// Read confirmed owner state after recovery; volatile leadership is not a lease.
    pub async fn state(&self) -> Result<State, Error> {
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::State(sender))?;
        Self::receive_result(receiver).await
    }
    /// Fetch bounded synchronized committed content only.
    pub async fn fetch_committed(
        &self,
        from: u64,
        entries: usize,
        bytes: usize,
    ) -> Result<Vec<Record>, Error> {
        if entries == 0
            || entries > self.config.limits.live_entries
            || bytes == 0
            || bytes > self.config.limits.fetch_bytes
        {
            return Err(Error::Bounds);
        }
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Fetch(from, entries, bytes, sender))?;
        Self::receive_result(receiver).await
    }
    /// Stop admission, discard pending work, and join the sole durable owner.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.stopping.store(true, Ordering::Release);
        drop(self.sender.try_send(Command::Stop));
        let mut join = self.join.lock().await;
        if let Some(task) = join.as_mut() {
            task.await.map_err(|_| Error::Stopped)?;
        }
        *join = None;
        Ok(())
    }
}
impl crate::transport::Handler for ReplicationHandler {
    type Error = Error;
    async fn handle(&self, request: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        if request.capacity() > self.config.controller.protocol_limits.max_request_bytes() {
            return Err(Error::Bounds);
        }
        let _admission = self.admit()?;
        let (sender, receiver) = oneshot::channel();
        self.enqueue(Command::Controller(request, sender))?;
        Ok(Some(Self::receive_result(receiver).await?))
    }
}
impl Drop for ReplicationHandler {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        drop(self.sender.try_send(Command::Stop));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_queue_canceled_work_and_owner_completion_have_distinct_outcomes(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "partitionline-replication-queue-{}",
            std::process::id()
        ));
        std::fs::create_dir(&root)?;
        let mut controller = protocol::Config::new(1, vec![1], "queue-test".into())?;
        controller.election_timeouts = election::Timeouts::new(5, 5)?;
        controller.max_queued_requests = 2;
        let mut config = Config::new(controller);
        config.max_queued_requests = 2;
        let mut node = Node::open(
            root.join("metadata"),
            root.join("election"),
            config.clone(),
            0,
        )?;
        node.campaign(5, 77)?;
        node.activate_leader(5)?;
        let (sender, receiver) = mpsc::channel(2);
        let stopping = Arc::new(AtomicBool::new(false));
        let handler = ReplicationHandler {
            sender,
            stopping: stopping.clone(),
            join: Mutex::new(None),
            admission: Semaphore::new(config.max_queued_requests),
            config,
        };
        let (canceled, canceled_receiver) = oneshot::channel();
        handler.enqueue(Command::Propose(b"canceled".to_vec(), canceled))?;
        let (valid, valid_receiver) = oneshot::channel();
        handler.enqueue(Command::Propose(b"accepted".to_vec(), valid))?;
        let (extra, _extra_receiver) = oneshot::channel();
        assert!(matches!(
            handler.enqueue(Command::State(extra)),
            Err(Error::Busy)
        ));
        drop(canceled_receiver);
        let start = Instant::now()
            .checked_sub(std::time::Duration::from_millis(10))
            .ok_or("clock underflow")?;
        let worker = std::thread::spawn(move || run_actor(node, receiver, stopping, start));
        assert_eq!(valid_receiver.blocking_recv()??, 2);
        handler.enqueue(Command::Stop)?;
        let final_state = worker.join().map_err(|_| "owner panicked")?;
        assert_eq!(final_state.committed_end, 2);
        assert_eq!(final_state.wal_durable_ops, 5);
        drop(handler);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
    #[tokio::test]
    async fn unconsumed_fetch_receipt_keeps_admission_bounded(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "partitionline-replication-receipt-{}",
            std::process::id()
        ));
        std::fs::create_dir(&root)?;
        let mut controller = protocol::Config::new(1, vec![1], "receipt-test".into())?;
        controller.election_timeouts = election::Timeouts::new(5, 5)?;
        controller.max_queued_requests = 1;
        let mut config = Config::new(controller);
        config.max_queued_requests = 1;
        let local_config = config.clone();
        let storage = root.clone();
        let node = tokio::task::spawn_blocking(move || -> Result<Node, Error> {
            let mut node = Node::open(
                storage.join("metadata"),
                storage.join("election"),
                local_config,
                0,
            )?;
            node.campaign(5, 77)?;
            node.activate_leader(5)?;
            node.propose(b"initial", 5)?;
            Ok(node)
        })
        .await??;
        let (sender, receiver) = mpsc::channel(1);
        let (release, parked) = oneshot::channel();
        let stopping = Arc::new(AtomicBool::new(false));
        let owner_stop = stopping.clone();
        let start = Instant::now()
            .checked_sub(std::time::Duration::from_millis(10))
            .ok_or("clock underflow")?;
        let task = tokio::task::spawn_blocking(move || {
            if parked.blocking_recv().is_ok() {
                let _confirmed = run_actor(node, receiver, owner_stop, start);
            }
        });
        let handler = ReplicationHandler {
            sender,
            stopping,
            join: Mutex::new(Some(task)),
            config,
            admission: Semaphore::new(1),
        };
        let mut pending = Box::pin(handler.fetch_committed(1, 2, 1024));
        std::future::poll_fn(|cx| match std::future::Future::poll(pending.as_mut(), cx) {
            std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(_) => {
                std::task::Poll::Ready(Err("parked owner replied unexpectedly"))
            }
        })
        .await?;
        release.send(()).map_err(|_| "owner stopped")?;
        // Internal FIFO state query proves the receipt exists without polling its receiver.
        let (barrier, barrier_receiver) = oneshot::channel();
        handler
            .sender
            .reserve()
            .await?
            .send(Command::State(barrier));
        assert_eq!(barrier_receiver.await??.committed_end, 2);
        assert!(matches!(
            handler.propose(b"too-early".to_vec()).await,
            Err(Error::Busy)
        ));
        assert_eq!(pending.await?.len(), 2);
        assert_eq!(handler.propose(b"after-consumption".to_vec()).await?, 3);
        handler.shutdown().await?;
        drop(handler);
        tokio::task::spawn_blocking(move || std::fs::remove_dir_all(root)).await??;
        Ok(())
    }
    #[test]
    fn follower_commit_runtime_checks_entry_term_before_persistence(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "partitionline-replication-term-{}",
            std::process::id()
        ));
        std::fs::create_dir(&root)?;
        let config = Config::new(protocol::Config::new(1, vec![1, 2, 3], "term-test".into())?);
        let mut node = Node::open(root.join("metadata"), root.join("election"), config, 0)?;
        let request = Request {
            leader: 2,
            peer: 1,
            sequence: 1,
            term: 3,
            previous: LogPosition::default(),
            leader_commit: 0,
            entries: vec![Record::barrier(3, 1)?],
        };
        node.receive(&request, 0)?;
        let before = node.state().wal_durable_ops;
        assert!(matches!(
            node.log.commit(1, 2, 2, 1, 2),
            Err(Error::InvalidPeer)
        ));
        assert_eq!(node.state().wal_durable_ops, before);
        assert_eq!(node.state().committed_end, 0);
        drop(node);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
