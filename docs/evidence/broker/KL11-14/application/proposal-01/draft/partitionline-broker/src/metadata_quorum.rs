//! Explicit committed metadata application over the trusted private Raft runtime.
//!
//! One blocking owner holds this application journal and projection. A proposal
//! is not an application receipt: only records returned by `fetch_committed` may
//! advance the durable cursor. Every position, including Barrier/Voters records,
//! gets an exact term/index/kind/payload receipt before visible publication.
//! Recovery revalidates every old receipt against the source committed prefix.
//! This is a custom local application format, not Kafka metadata record encoding.
//!
//! Request IDs fence retries. Repeating the same ID and bytes yields its original
//! outcome; different bytes are rejected deterministically. A valid command that
//! loses a catalog race becomes a durable no-op outcome on every replica. Unknown
//! Data formats fail closed. All group, budget and catalog limits are pinned in
//! initialization, so a changed configuration cannot reinterpret old outcomes.
//!
//! Blocking methods belong on the router's existing exclusive storage actor or
//! another joined blocking owner, never an async executor. The adapter creates
//! no task, queue, socket or DNS job. Its deadline bounds async source admission
//! and waiting, not an uninterruptible filesystem sync. Cancellation/timeout may
//! leave a committed command: refresh/reopen and retry the SAME request ID.
//! Reads expose the applied committed projection, not a linearizable read lease.
//! Partition placement/files, Kafka wire handlers, native KRaft records and
//! autonomous application refresh are separate integration work.

use crate::{
    catalog, journal,
    raft::{
        election::LogPosition,
        replication::{Record, RecordKind},
        runtime,
    },
};
use std::{mem::size_of, path::Path, time::Duration};
use tokio::time::Instant;

const COMMAND_MAGIC: &[u8; 8] = b"PLMETCMD";
const INIT_MAGIC: &[u8; 8] = b"PLMETINI";
const APPLY_MAGIC: &[u8; 8] = b"PLMETAPP";
const COMMAND_HEADER: usize = 50;
const COMMAND_MAX: usize = COMMAND_HEADER + 249;
const APPLY_HEADER: usize = 32;
const INIT_FIXED: usize = 104;
const MAX_INIT: usize = INIT_FIXED + 498 + 64 * 1024;

/// Stable caller-generated retry identity. Zero is reserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestId([u8; 16]);
impl RequestId {
    /// Reject the reserved zero token; callers retain the token across ambiguity.
    pub fn new(bytes: [u8; 16]) -> Result<Self, Error> {
        if bytes == [0; 16] {
            return Err(Error::InvalidCommand);
        }
        Ok(Self(bytes))
    }
    /// Original token bytes.
    pub fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Canonical bounded command. It is safe to retain this object across retries.
#[derive(Debug)]
pub struct Command {
    bytes: Vec<u8>,
}
impl Command {
    /// Encode a create; catalog semantics are decided at committed log order.
    pub fn create(
        request: RequestId,
        id: catalog::TopicId,
        name: &str,
        count: u32,
    ) -> Result<Self, Error> {
        if name.len() > 249 {
            return Err(Error::InvalidCommand);
        }
        Self::encode(request, id, name, count, 1)
    }
    /// Encode a delete by immutable identity.
    pub fn delete(request: RequestId, id: catalog::TopicId) -> Result<Self, Error> {
        Self::encode(request, id, "", 0, 2)
    }
    fn encode(
        request: RequestId,
        id: catalog::TopicId,
        name: &str,
        count: u32,
        opcode: u8,
    ) -> Result<Self, Error> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(COMMAND_HEADER + name.len())
            .map_err(|_| Error::Allocation)?;
        bytes.resize(COMMAND_HEADER, 0);
        bytes[..8].copy_from_slice(COMMAND_MAGIC);
        bytes[8..24].copy_from_slice(&request.bytes());
        bytes[24] = opcode;
        bytes[28..44].copy_from_slice(&id.bytes());
        bytes[44..48].copy_from_slice(&count.to_be_bytes());
        bytes[48..50].copy_from_slice(&(name.len() as u16).to_be_bytes());
        bytes.extend_from_slice(name.as_bytes());
        Ok(Self { bytes })
    }
    /// Borrow exact bytes used for the Raft Data proposal.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Request identity embedded in canonical bytes.
    pub fn request_id(&self) -> RequestId {
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&self.bytes[8..24]);
        RequestId(bytes)
    }
}
struct Parsed<'a> {
    request: RequestId,
    id: catalog::TopicId,
    name: &'a str,
    count: u32,
    create: bool,
}
fn parse(bytes: &[u8]) -> Result<Parsed<'_>, Error> {
    if !(COMMAND_HEADER..=COMMAND_MAX).contains(&bytes.len())
        || bytes.get(..8) != Some(&COMMAND_MAGIC[..])
        || bytes[25..28] != [0; 3]
    {
        return Err(Error::InvalidCommand);
    }
    let mut token = [0; 16];
    token.copy_from_slice(&bytes[8..24]);
    let request = RequestId::new(token)?;
    let mut raw_id = [0; 16];
    raw_id.copy_from_slice(&bytes[28..44]);
    let id = catalog::TopicId::new(raw_id).map_err(|_| Error::InvalidCommand)?;
    let count = u32::from_be_bytes(
        bytes[44..48]
            .try_into()
            .map_err(|_| Error::InvalidCommand)?,
    );
    let len = usize::from(u16::from_be_bytes(
        bytes[48..50]
            .try_into()
            .map_err(|_| Error::InvalidCommand)?,
    ));
    if COMMAND_HEADER.checked_add(len) != Some(bytes.len()) {
        return Err(Error::InvalidCommand);
    }
    let name = std::str::from_utf8(&bytes[COMMAND_HEADER..]).map_err(|_| Error::InvalidCommand)?;
    let create = match bytes[24] {
        1 => true,
        2 if name.is_empty() && count == 0 => false,
        _ => return Err(Error::InvalidCommand),
    };
    Ok(Parsed {
        request,
        id,
        name,
        count,
        create,
    })
}

/// Deterministic catalog or idempotency outcome, persisted by source receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Catalog mutation published after both source commit and application sync.
    Applied,
    /// Exact name/identity/collision and bounds match the legacy catalog rules.
    InvalidName,
    /// Reserved metadata topic name.
    ReservedName,
    /// Invalid positive signed32-bit partition count.
    InvalidPartitions,
    /// A live topic has that exact name.
    DuplicateName,
    /// Identity is live or tombstoned.
    DuplicateIdentity,
    /// Dot/underscore-equivalent live name.
    NameCollision,
    /// Delete references an absent/tombstoned identity.
    UnknownIdentity,
    /// Configured live-topic bound.
    TopicBudget,
    /// Configured historical-identity bound.
    IdentityBudget,
    /// Per-topic/total partition bound.
    PartitionBudget,
    /// Successful catalog operation bound.
    OperationBudget,
    /// Successful catalog payload replay bound.
    ReplayBudget,
    /// Same request identity with different command bytes: committed no-op.
    RequestConflict,
}
fn classify(error: catalog::Error) -> Result<Outcome, Error> {
    use catalog::Error as C;
    Ok(match error {
        C::InvalidName => Outcome::InvalidName,
        C::ReservedName => Outcome::ReservedName,
        C::InvalidPartitionCount => Outcome::InvalidPartitions,
        C::DuplicateName => Outcome::DuplicateName,
        C::DuplicateIdentity => Outcome::DuplicateIdentity,
        C::NameCollision => Outcome::NameCollision,
        C::UnknownIdentity => Outcome::UnknownIdentity,
        C::TopicBudgetExceeded => Outcome::TopicBudget,
        C::IdentityBudgetExceeded => Outcome::IdentityBudget,
        C::PartitionBudgetExceeded => Outcome::PartitionBudget,
        C::OperationBudgetExceeded => Outcome::OperationBudget,
        C::ReplayBudgetExceeded => Outcome::ReplayBudget,
        C::AllocationFailed => return Err(Error::Allocation),
        _ => return Err(Error::Catalog(error)),
    })
}

/// A completed application decision tied to its original source position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    /// First committed appearance of this exact request/command.
    pub position: LogPosition,
    /// Deterministic catalog result at that position.
    pub outcome: Outcome,
}
/// Application errors do not fabricate a successful source commit.
#[derive(Debug)]
pub enum Error {
    /// Invalid, overflowing or incompatible finite limits/group binding.
    InvalidConfig,
    /// Command bytes/token fail canonical decoding.
    InvalidCommand,
    /// Malformed/noncontiguous source or mismatched persisted source receipt.
    SourceMismatch,
    /// Application cursor is ahead of the actual committed source prefix.
    CursorAhead,
    /// An ambiguous append requires reopening; no later state is published.
    Poisoned,
    /// Application recovery has not verified all old receipts yet.
    Recovering,
    /// Caller reused a token for different bytes without proposing them.
    RequestConflict,
    /// Finite source/application budget exhausted.
    Budget,
    /// Fallible allocation failed before durable application mutation.
    Allocation,
    /// Absolute caller deadline expired; source outcome may be ambiguous.
    Deadline,
    /// Catalog invariant/unsupported internal error.
    Catalog(catalog::Error),
    /// Application journal integrity/I/O failure.
    Journal(journal::Error),
    /// Durable runtime failure; proposal/commit outcomes may be ambiguous.
    Runtime(runtime::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metadata application: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<journal::Error> for Error {
    fn from(v: journal::Error) -> Self {
        Self::Journal(v)
    }
}
impl From<runtime::Error> for Error {
    fn from(v: runtime::Error) -> Self {
        Self::Runtime(v)
    }
}

/// Immutable group identity and canonical genesis; excludes receiving local ID.
#[derive(Debug, PartialEq, Eq)]
pub struct Identity {
    cluster: String,
    topic: String,
    partition: u32,
    genesis: Vec<u8>,
}
impl Identity {
    /// Pin a trusted configured group. Genesis is the canonical voter encoding.
    pub fn new(
        cluster: String,
        topic: String,
        partition: u32,
        genesis: Vec<u8>,
    ) -> Result<Self, Error> {
        if cluster.is_empty()
            || cluster.len() > 249
            || topic.is_empty()
            || topic.len() > 249
            || partition > i32::MAX as u32
            || genesis.len() > 64 * 1024
        {
            return Err(Error::InvalidConfig);
        }
        let decoded =
            crate::raft::membership::Voters::decode(&genesis).map_err(|_| Error::InvalidConfig)?;
        if decoded.position() != (LogPosition { term: 0, index: 0 }) {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            cluster,
            topic,
            partition,
            genesis,
        })
    }
}

/// Finite application storage and output limits pinned in the Init record.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Catalog limits; every replica must use identical values.
    pub catalog: catalog::Limits,
    /// Maximum source positions, including barriers and voter configurations.
    pub positions: usize,
    /// Maximum single source payload accepted by this projection.
    pub record_bytes: usize,
    /// Cumulative source payload budget; rejected/duplicate commands count too.
    pub source_bytes: usize,
    /// Complete custom application journal byte budget.
    pub journal_bytes: u64,
}
impl Limits {
    fn validated(self) -> Result<journal::Limits, Error> {
        if !(1..=4096).contains(&self.positions)
            || !(COMMAND_MAX..=1024 * 1024).contains(&self.record_bytes)
            || self.source_bytes == 0
            || self.source_bytes > 64 * 1024 * 1024
            || self.record_bytes > self.source_bytes
        {
            return Err(Error::InvalidConfig);
        }
        let entry_bytes = self
            .record_bytes
            .checked_add(APPLY_HEADER)
            .ok_or(Error::InvalidConfig)?
            .max(MAX_INIT);
        let fetch = entry_bytes
            .checked_add(size_of::<journal::Entry>())
            .ok_or(Error::InvalidConfig)?;
        let count = self.positions.checked_add(1).ok_or(Error::InvalidConfig)?;
        // Actual aggregate source bytes, not positions*maximum record bytes.
        let minimum = self
            .source_bytes
            .checked_add(
                self.positions
                    .checked_mul(32 + APPLY_HEADER)
                    .ok_or(Error::InvalidConfig)?,
            )
            .and_then(|v| v.checked_add(24 + 32 + MAX_INIT))
            .ok_or(Error::InvalidConfig)?;
        if self.journal_bytes < u64::try_from(minimum).map_err(|_| Error::InvalidConfig)? {
            return Err(Error::InvalidConfig);
        }
        journal::Limits::new(entry_bytes, self.journal_bytes, count, fetch).map_err(Error::Journal)
    }
    /// Conservative additional Rust-managed state/fetch bound, excluding the
    /// existing runtime, allocator overhead, caller-returned data, OS buffers/RSS.
    pub fn managed_bytes(self) -> Result<usize, Error> {
        self.validated()?;
        self.positions
            .checked_mul(size_of::<Decision>() + COMMAND_MAX + 24)
            .and_then(|v| {
                self.catalog
                    .max_identities()
                    .checked_mul(size_of::<catalog::Topic>() + 249 + 32)
                    .and_then(|n| v.checked_add(n))
            })
            .and_then(|v| {
                self.record_bytes
                    .max(MAX_INIT)
                    .checked_mul(3)
                    .and_then(|n| v.checked_add(n))
            })
            .and_then(|v| {
                self.positions
                    .checked_mul(64)
                    .and_then(|n| n.checked_add(MAX_INIT * 2))
                    .and_then(|n| v.checked_add(n))
            })
            .ok_or(Error::InvalidConfig)
    }
}
fn init(identity: &Identity, limits: Limits) -> Result<Vec<u8>, Error> {
    let size = INIT_FIXED
        .checked_add(identity.cluster.len())
        .and_then(|n| n.checked_add(identity.topic.len()))
        .and_then(|n| n.checked_add(identity.genesis.len()))
        .ok_or(Error::InvalidConfig)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::Allocation)?;
    bytes.resize(INIT_FIXED, 0);
    bytes[..8].copy_from_slice(INIT_MAGIC);
    bytes[8..12].copy_from_slice(&identity.partition.to_be_bytes());
    bytes[12..14].copy_from_slice(&(identity.cluster.len() as u16).to_be_bytes());
    bytes[14..16].copy_from_slice(&(identity.topic.len() as u16).to_be_bytes());
    bytes[16..20].copy_from_slice(&(identity.genesis.len() as u32).to_be_bytes());
    let values = [
        limits.positions as u64,
        limits.record_bytes as u64,
        limits.source_bytes as u64,
        limits.journal_bytes,
        limits.catalog.max_live_topics() as u64,
        limits.catalog.max_identities() as u64,
        u64::from(limits.catalog.max_partitions_per_topic()),
        limits.catalog.max_total_partitions(),
    ];
    for (n, value) in values.iter().enumerate() {
        let from = 24 + n * 8;
        bytes[from..from + 8].copy_from_slice(&value.to_be_bytes());
    }
    // Pin all semantic catalog limits; journal limits do not affect decisions.
    bytes[88..96].copy_from_slice(&(limits.catalog.max_operations() as u64).to_be_bytes());
    bytes[96..104].copy_from_slice(&limits.catalog.max_replay_bytes().to_be_bytes());
    bytes.extend_from_slice(identity.cluster.as_bytes());
    bytes.extend_from_slice(identity.topic.as_bytes());
    bytes.extend_from_slice(&identity.genesis);
    Ok(bytes)
}
fn encode_receipt(record: &Record) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(APPLY_HEADER + record.payload.len())
        .map_err(|_| Error::Allocation)?;
    bytes.resize(APPLY_HEADER, 0);
    bytes[..8].copy_from_slice(APPLY_MAGIC);
    bytes[8..16].copy_from_slice(&record.term.to_be_bytes());
    bytes[16..24].copy_from_slice(&record.index.to_be_bytes());
    bytes[24] = match record.kind {
        RecordKind::Data => 1,
        RecordKind::Barrier => 2,
        RecordKind::Voters => 3,
    };
    bytes[28..32].copy_from_slice(&(record.payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&record.payload);
    Ok(bytes)
}
struct Decision {
    request: RequestId,
    bytes: Vec<u8>,
    receipt: Receipt,
}
struct PreparedDecision {
    change: Option<catalog::Prepared>,
    decision: Option<Decision>,
    receipt: Option<Receipt>,
}

// Private: the only public production writer is the runtime-backed adapter.
struct Application {
    journal: journal::Journal,
    projection: catalog::Projection,
    limits: Limits,
    cursor: LogPosition,
    persisted: u64,
    source_bytes: usize,
    ready: bool,
    poisoned: bool,
    decisions: Vec<Decision>,
    recovery: journal::Recovery,
}
impl Application {
    fn open(path: impl AsRef<Path>, identity: &Identity, limits: Limits) -> Result<Self, Error> {
        let storage = limits.validated()?;
        let expected = init(identity, limits)?;
        let (mut journal, recovery) = journal::Journal::open(path, 0, storage)?;
        if journal.next_offset() == 0 {
            journal.append(1, &expected)?;
        }
        let first = journal.fetch(0, 1, storage.max_fetch_bytes())?;
        if first.len() != 1
            || first[0].first_offset != 0
            || first[0].record_count != 1
            || first[0].payload != expected
        {
            return Err(Error::SourceMismatch);
        }
        let persisted = journal
            .next_offset()
            .checked_sub(1)
            .ok_or(Error::SourceMismatch)?;
        if persisted > limits.positions as u64 {
            return Err(Error::Budget);
        }
        let mut decisions = Vec::new();
        decisions
            .try_reserve_exact(limits.positions)
            .map_err(|_| Error::Allocation)?;
        Ok(Self {
            journal,
            projection: catalog::Projection::new(limits.catalog),
            limits,
            cursor: LogPosition { term: 0, index: 0 },
            persisted,
            source_bytes: 0,
            ready: persisted == 0,
            poisoned: false,
            decisions,
            recovery,
        })
    }
    fn alive(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    fn lookup(&self, bytes: &[u8]) -> Result<Option<Receipt>, Error> {
        let parsed = parse(bytes)?;
        match self.decisions.iter().find(|d| d.request == parsed.request) {
            Some(d) if d.bytes == bytes => Ok(Some(d.receipt)),
            Some(_) => Err(Error::RequestConflict),
            None => Ok(None),
        }
    }
    fn prepare(&mut self, record: &Record) -> Result<PreparedDecision, Error> {
        if record.kind != RecordKind::Data {
            return Ok(PreparedDecision {
                change: None,
                decision: None,
                receipt: None,
            });
        }
        let parsed = parse(&record.payload)?;
        if let Some(old) = self.decisions.iter().find(|d| d.request == parsed.request) {
            let receipt = if old.bytes == record.payload {
                old.receipt
            } else {
                Receipt {
                    position: LogPosition {
                        term: record.term,
                        index: record.index,
                    },
                    outcome: Outcome::RequestConflict,
                }
            };
            return Ok(PreparedDecision {
                change: None,
                decision: None,
                receipt: Some(receipt),
            });
        }
        let result = if parsed.create {
            self.projection
                .prepare_create(parsed.name, parsed.id, parsed.count)
        } else {
            self.projection.prepare_delete(parsed.id)
        };
        let (change, outcome) = match result {
            Ok(c) => (Some(c), Outcome::Applied),
            Err(error) => (None, classify(error)?),
        };
        let receipt = Receipt {
            position: LogPosition {
                term: record.term,
                index: record.index,
            },
            outcome,
        };
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(record.payload.len())
            .map_err(|_| Error::Allocation)?;
        bytes.extend_from_slice(&record.payload);
        let decision = Decision {
            request: parsed.request,
            bytes,
            receipt,
        };
        Ok(PreparedDecision {
            change,
            decision: Some(decision),
            receipt: Some(receipt),
        })
    }
    fn apply(&mut self, record: &Record) -> Result<Option<Receipt>, Error> {
        self.alive()?;
        if record.index != self.cursor.index.checked_add(1).ok_or(Error::Budget)?
            || record.term == 0
            || record.term < self.cursor.term
            || record.index > self.limits.positions as u64
            || record.payload.len() > self.limits.record_bytes
            || (record.kind == RecordKind::Barrier && !record.payload.is_empty())
        {
            return Err(Error::SourceMismatch);
        }
        let total = self
            .source_bytes
            .checked_add(record.payload.len())
            .filter(|n| *n <= self.limits.source_bytes)
            .ok_or(Error::Budget)?;
        let bytes = encode_receipt(record)?;
        if record.index <= self.persisted {
            let entries =
                self.journal
                    .fetch(record.index, 1, self.limits.validated()?.max_fetch_bytes())?;
            if entries.len() != 1
                || entries[0].first_offset != record.index
                || entries[0].record_count != 1
                || entries[0].payload != bytes
            {
                self.poisoned = true;
                self.ready = false;
                return Err(Error::SourceMismatch);
            }
        }
        // All command validation and fallible state allocation precede append.
        let prepared = self.prepare(record)?;
        if record.index > self.persisted {
            if let Err(error) = self.journal.append(1, &bytes) {
                self.poisoned = true;
                return Err(Error::Journal(error));
            }
            self.persisted = record.index;
        }
        if let Some(change) = prepared.change {
            self.projection.publish(change);
        }
        if let Some(decision) = prepared.decision {
            self.decisions.push(decision);
        }
        self.cursor = LogPosition {
            term: record.term,
            index: record.index,
        };
        self.source_bytes = total;
        self.ready = self.cursor.index >= self.persisted;
        Ok(prepared.receipt)
    }
    fn confirmed_view(&self) -> Result<&catalog::Projection, Error> {
        self.alive()?;
        if !self.ready {
            return Err(Error::Recovering);
        }
        Ok(&self.projection)
    }
}

/// Exclusive blocking application owner borrowing an existing runtime authority.
///
/// `open` refreshes every old application receipt before returning a readable
/// view. The caller must serialize ownership on its storage actor. This object
/// does not own/shut down the Raft runtime; shutdown ordering is application owner
/// first, then runtime. Source getters below are a PROPOSED narrow runtime hook.
pub struct QuorumCatalog {
    source: runtime::Handle,
    executor: tokio::runtime::Handle,
    application: Application,
    deadline: Duration,
    controller_id: i32,
}
impl QuorumCatalog {
    /// Open an explicit quorum-only application path. Do not reuse a local
    /// PLTCAT01 journal. Identity is derived from the configured runtime itself.
    pub fn open(
        path: impl AsRef<Path>,
        source: runtime::Handle,
        executor: tokio::runtime::Handle,
        limits: Limits,
        deadline: Duration,
    ) -> Result<Self, Error> {
        if deadline.is_zero() || deadline > Duration::from_secs(60) {
            return Err(Error::InvalidConfig);
        }
        let (cluster, topic, partition, genesis) = source.metadata_binding()?;
        let identity = Identity::new(cluster, topic, partition, genesis)?;
        let storage = source.storage_limits();
        if limits.record_bytes < storage.record_bytes
            || limits.positions < storage.live_entries
            || limits.source_bytes < storage.live_bytes
        {
            return Err(Error::InvalidConfig);
        }
        let application = Application::open(path, &identity, limits)?;
        let mut owner = Self {
            source,
            executor,
            application,
            deadline,
            controller_id: -1,
        };
        owner.refresh()?;
        Ok(owner)
    }
    fn source_until<T>(
        &self,
        deadline: Instant,
        future: impl std::future::Future<Output = Result<T, runtime::Error>>,
    ) -> Result<T, Error> {
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        self.executor
            .block_on(async { tokio::time::timeout_at(deadline, future).await })
            .map_err(|_| Error::Deadline)?
            .map_err(Error::Runtime)
    }
    fn refresh_until(&mut self, deadline: Instant) -> Result<LogPosition, Error> {
        self.application.alive()?;
        let state = self.source_until(deadline, self.source.state())?;
        if state.poisoned || !state.ready {
            return Err(Error::Poisoned);
        }
        self.controller_id = state
            .election
            .leader
            .and_then(|id| i32::try_from(id).ok())
            .unwrap_or(-1);
        if self.application.persisted > state.committed_end
            || self.application.cursor.index > state.committed_end
        {
            self.application.poisoned = true;
            self.application.ready = false;
            return Err(Error::CursorAhead);
        }
        let bound = self.source.storage_limits();
        // Freeze one committed end per refresh; subsequent commits wait for the
        // next call, so an active writer cannot make this operation endless.
        while self.application.cursor.index < state.committed_end {
            let from = self
                .application
                .cursor
                .index
                .checked_add(1)
                .ok_or(Error::Budget)?;
            let records = self.source_until(
                deadline,
                self.source.fetch_committed(from, 1, bound.fetch_bytes),
            )?;
            if records.len() != 1 || records[0].index != from {
                return Err(Error::SourceMismatch);
            }
            self.application.apply(&records[0])?;
        }
        self.application.confirmed_view()?;
        Ok(self.application.cursor)
    }
    /// Apply the observed committed prefix; follower reads may remain stale.
    pub fn refresh(&mut self) -> Result<LogPosition, Error> {
        self.refresh_until(Instant::now() + self.deadline)
    }
    pub(crate) fn projection(&self) -> &catalog::Projection {
        &self.application.projection
    }
    pub(crate) fn journal_bytes(&self) -> u64 {
        self.application.journal.file_bytes()
    }
    pub(crate) fn is_poisoned(&self) -> bool {
        self.application.poisoned
            || self.application.journal.is_poisoned()
            || !self.application.ready
    }
    pub(crate) fn controller_id(&self) -> i32 {
        self.controller_id
    }
    /// Actual application journal recovery, not a fabricated quorum receipt.
    pub fn recovery(&self) -> journal::Recovery {
        self.application.recovery
    }
    /// Last locally synced, source-verified application position.
    pub fn cursor(&self) -> LogPosition {
        self.application.cursor
    }
    /// Borrow the confirmed applied projection; this is not a quorum read lease.
    pub fn view(&self) -> Result<&catalog::Projection, Error> {
        self.application.confirmed_view()
    }
    /// Propose, wait for exact durable quorum commit, then sync application.
    ///
    /// Repeating a confirmed token returns its original decision without another
    /// proposal. A source timeout or application sync failure is ambiguous; retain
    /// the token and refresh/reopen. The total async budget is never renewed.
    pub fn execute(&mut self, command: &Command) -> Result<Receipt, Error> {
        self.execute_until(command, Instant::now() + self.deadline)
    }
    /// Execute within the earlier caller/application deadline; it is never renewed.
    pub fn execute_until(&mut self, command: &Command, outer: Instant) -> Result<Receipt, Error> {
        let deadline = outer.min(Instant::now() + self.deadline);
        self.refresh_until(deadline)?;
        if let Some(receipt) = self.application.lookup(command.bytes())? {
            return Ok(receipt);
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(command.bytes.len())
            .map_err(|_| Error::Allocation)?;
        payload.extend_from_slice(&command.bytes);
        let position = self.source_until(deadline, self.source.propose(payload))?;
        self.source_until(deadline, self.source.wait_committed(position))?;
        self.refresh_until(deadline)?;
        let receipt = self
            .application
            .lookup(command.bytes())?
            .ok_or(Error::SourceMismatch)?;
        // Concurrent retry can have committed an earlier appearance. Either
        // receipt still binds this exact token/bytes; do not claim the later copy
        // made a second catalog mutation.
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{
        election::{Role, Timeouts},
        membership::{Endpoint, Key, Voter, Voters},
        protocol,
        replication::{Config as NodeConfig, Node},
    };
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> TestResult<Self> {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "partitionline-metadata-apply-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&p)?;
            Ok(Self(p))
        }
        fn app(&self, id: u32) -> PathBuf {
            self.0.join(format!("{id}.application"))
        }
        fn wal(&self, id: u32) -> PathBuf {
            self.0.join(format!("{id}.content"))
        }
        fn election(&self, id: u32) -> PathBuf {
            self.0.join(format!("{id}.election"))
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            drop(fs::remove_dir_all(&self.0));
        }
    }
    fn limits() -> Limits {
        Limits {
            catalog: catalog::Limits::default(),
            positions: 64,
            record_bytes: 64 * 1024,
            source_bytes: 1024 * 1024,
            journal_bytes: 2 * 1024 * 1024,
        }
    }
    fn identity(ids: &[u32]) -> TestResult<Identity> {
        let voters = ids
            .iter()
            .map(|id| {
                Voter::new(
                    Key::new(*id, [(*id + 1) as u8; 16])?,
                    vec![Endpoint::new(
                        "CONTROLLER".into(),
                        "127.0.0.1".into(),
                        9000 + *id as u16,
                    )?],
                    0,
                    1,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Identity::new(
            "metadata-core-test".into(),
            "__cluster_metadata".into(),
            0,
            Voters::new(0, LogPosition::default(), 1, voters)?.encode()?,
        )?)
    }
    fn command(token: u8, topic: u8, name: &str) -> TestResult<Command> {
        Ok(Command::create(
            RequestId::new([token; 16])?,
            catalog::TopicId::new([topic; 16])?,
            name,
            1,
        )?)
    }
    fn data(index: u64, command: &Command) -> Record {
        Record {
            term: 2,
            index,
            kind: RecordKind::Data,
            payload: command.bytes().to_vec(),
        }
    }
    fn barrier(index: u64) -> TestResult<Record> {
        Ok(Record::barrier(2, index)?)
    }
    fn open_node(temp: &Temp, id: u32, ids: &[u32], now: u64) -> TestResult<Node> {
        let mut c = protocol::Config::new(id, ids.to_vec(), "metadata-core-test".into())?;
        c.election_timeouts = Timeouts::new(5, 5)?;
        let mut config = NodeConfig::new(c);
        config.quorum_timeout_ms = 100;
        Ok(Node::open(temp.wal(id), temp.election(id), config, now)?)
    }
    fn elect(nodes: &mut [Node], leader: usize, voters: &[usize], now: u64) -> TestResult {
        let req = nodes[leader].campaign(now, 77)?.ok_or("campaign not due")?;
        for peer in voters {
            let reply = nodes[*peer].respond_controller(&req, now)?;
            let id = nodes[*peer].state().local_id;
            nodes[leader].receive_vote(id, 77, &reply, now)?;
        }
        assert_eq!(nodes[leader].state().election.role, Role::Leader);
        nodes[leader].activate_leader(now)?;
        Ok(())
    }
    fn exchange(nodes: &mut [Node], leader: usize, peer: usize, now: u64) -> TestResult {
        let id = nodes[peer].state().local_id;
        for _ in 0..8 {
            let req = nodes[leader].prepare(id, now)?;
            let reply = nodes[peer].receive(&req, now)?;
            let ok = reply.success;
            nodes[leader].acknowledge(id, reply, now)?;
            if ok {
                return Ok(());
            }
        }
        Err("bounded catch-up".into())
    }
    fn apply_all(app: &mut Application, node: &Node) -> TestResult {
        let records = node.fetch_committed(1, 64, 256 * 1024)?;
        for r in &records {
            if r.index > app.cursor.index {
                app.apply(r)?;
            }
        }
        if app.persisted > app.cursor.index {
            return Err("application cursor ahead of source".into());
        }
        app.confirmed_view()?;
        Ok(())
    }
    #[test]
    fn duplicate_request_replay_returns_original_receipt_without_second_mutation() -> TestResult {
        let temp = Temp::new()?;
        let group = identity(&[1, 2, 3])?;
        let c = command(1, 3, "durable")?;
        let mut app = Application::open(temp.app(1), &group, limits())?;
        app.apply(&barrier(1)?)?;
        let first = app.apply(&data(2, &c))?.ok_or("missing data receipt")?;
        assert_eq!(first.outcome, Outcome::Applied);
        assert_eq!(app.apply(&data(3, &c))?, Some(first));
        assert_eq!(app.projection.operation_count(), 1);
        drop(app); // reply loss after receipt sync
        let mut app = Application::open(temp.app(1), &group, limits())?;
        assert!(matches!(app.confirmed_view(), Err(Error::Recovering)));
        app.apply(&barrier(1)?)?;
        app.apply(&data(2, &c))?;
        app.apply(&data(3, &c))?;
        assert_eq!(app.lookup(c.bytes())?, Some(first));
        assert_eq!(app.confirmed_view()?.operation_count(), 1);
        assert_eq!(app.cursor.index, 3);
        Ok(())
    }
    #[test]
    fn racing_commands_and_reused_tokens_are_committed_deterministic_noops() -> TestResult {
        let temp = Temp::new()?;
        let group = identity(&[1])?;
        let mut app = Application::open(temp.app(1), &group, limits())?;
        let first = command(1, 3, "same")?;
        let race = command(2, 4, "same")?;
        let forge = command(1, 5, "other")?;
        assert_eq!(
            app.apply(&data(1, &first))?.ok_or("receipt")?.outcome,
            Outcome::Applied
        );
        assert_eq!(
            app.apply(&data(2, &race))?.ok_or("receipt")?.outcome,
            Outcome::DuplicateName
        );
        assert_eq!(
            app.apply(&data(3, &forge))?.ok_or("receipt")?.outcome,
            Outcome::RequestConflict
        );
        assert!(matches!(
            app.lookup(forge.bytes()),
            Err(Error::RequestConflict)
        ));
        assert_eq!(app.projection.operation_count(), 1);
        assert!(app.projection.by_name("other").is_none());
        drop(app);
        let mut app = Application::open(temp.app(1), &group, limits())?;
        for r in [data(1, &first), data(2, &race), data(3, &forge)] {
            app.apply(&r)?;
        }
        assert_eq!(app.confirmed_view()?.operation_count(), 1);
        assert_eq!(app.cursor.index, 3);
        Ok(())
    }
    #[test]
    fn exact_source_payload_term_and_gap_forgery_fail_before_journal_mutation() -> TestResult {
        let temp = Temp::new()?;
        let group = identity(&[1])?;
        let c = command(1, 3, "bound")?;
        let mut app = Application::open(temp.app(1), &group, limits())?;
        app.apply(&data(1, &c))?;
        drop(app);
        let original = fs::metadata(temp.app(1))?.len();
        for altered in [
            Record {
                term: 3,
                ..data(1, &c)
            },
            data(1, &command(1, 4, "bound")?),
            data(2, &c),
        ] {
            let mut app = Application::open(temp.app(1), &group, limits())?;
            assert!(matches!(app.apply(&altered), Err(Error::SourceMismatch)));
            assert_eq!(fs::metadata(temp.app(1))?.len(), original);
            assert!(app.confirmed_view().is_err());
        }
        Ok(())
    }
    #[test]
    fn group_and_semantic_limit_changes_cannot_reinterpret_durable_outcomes() -> TestResult {
        let temp = Temp::new()?;
        let group = identity(&[1])?;
        drop(Application::open(temp.app(1), &group, limits())?);
        assert!(matches!(
            Application::open(temp.app(1), &identity(&[1, 2, 3])?, limits()),
            Err(Error::SourceMismatch)
        ));
        let mut changed = limits();
        changed.positions = 63;
        assert!(matches!(
            Application::open(temp.app(1), &group, changed),
            Err(Error::SourceMismatch)
        ));
        // Copying/replaying a legacy local catalog is never treated as a quorum receipt.
        let (mut legacy, _) = catalog::Catalog::open(temp.app(2), catalog::Limits::default())?;
        legacy.create("local", catalog::TopicId::new([4; 16])?, 1)?;
        drop(legacy);
        assert!(matches!(
            Application::open(temp.app(2), &group, limits()),
            Err(Error::SourceMismatch)
        ));
        Ok(())
    }
    #[test]
    fn malformed_data_and_application_budget_failure_leave_cursor_unchanged() -> TestResult {
        let temp = Temp::new()?;
        let group = identity(&[1])?;
        let mut app = Application::open(temp.app(1), &group, limits())?;
        let original = app.journal.file_bytes();
        let mut bad = data(1, &command(1, 3, "test")?);
        bad.payload[25] = 1;
        assert!(matches!(app.apply(&bad), Err(Error::InvalidCommand)));
        assert_eq!(app.cursor, LogPosition::default());
        assert_eq!(app.journal.file_bytes(), original);
        let mut bad = data(1, &command(1, 3, "test")?);
        bad.payload.resize(64 * 1024 + 1, 0);
        assert!(matches!(app.apply(&bad), Err(Error::SourceMismatch)));
        assert_eq!(app.journal.file_bytes(), original);
        Ok(())
    }
    #[test]
    fn complete_synced_receipt_before_publication_replays_once_and_partial_tail_repairs(
    ) -> TestResult {
        use std::io::Write;
        let temp = Temp::new()?;
        let group = identity(&[1])?;
        let c = command(1, 3, "after-sync")?;
        let app = Application::open(temp.app(1), &group, limits())?;
        drop(app);
        // Actual Journal append/sync models the durable cut before projection
        // publication. This is a local file cut, not a process/power-loss claim.
        let (mut journal, _) = journal::Journal::open(temp.app(1), 0, limits().validated()?)?;
        journal.append(1, &encode_receipt(&barrier(1)?)?)?;
        journal.append(1, &encode_receipt(&data(2, &c))?)?;
        drop(journal);
        let mut file = fs::OpenOptions::new().append(true).open(temp.app(1))?;
        file.write_all(b"PLENTRY1")?;
        file.sync_all()?;
        drop(file);
        let mut app = Application::open(temp.app(1), &group, limits())?;
        assert_eq!(app.recovery.truncated_bytes, 8);
        assert!(matches!(app.confirmed_view(), Err(Error::Recovering)));
        app.apply(&barrier(1)?)?;
        app.apply(&data(2, &c))?;
        assert_eq!(app.confirmed_view()?.operation_count(), 1);
        assert_eq!(
            app.lookup(c.bytes())?.ok_or("receipt")?.outcome,
            Outcome::Applied
        );
        Ok(())
    }
    fn quorum_survival(count: usize) -> TestResult {
        let temp = Temp::new()?;
        let ids = (1..=count as u32).collect::<Vec<_>>();
        let group = identity(&ids)?;
        let mut nodes = ids
            .iter()
            .map(|id| open_node(&temp, *id, &ids, 0))
            .collect::<TestResult<Vec<_>>>()?;
        let majority = count / 2 + 1;
        elect(&mut nodes, 0, &(1..majority).collect::<Vec<_>>(), 5)?;
        let create = command(1, 3, "majority-only")?;
        let index = nodes[0].propose(create.bytes(), 5)?;
        let mut app = Application::open(temp.app(1), &group, limits())?;
        apply_all(&mut app, &nodes[0])?;
        assert!(app.confirmed_view()?.by_name("majority-only").is_none());
        for peer in 1..majority {
            exchange(&mut nodes, 0, peer, 5)?;
            if peer + 1 < majority {
                assert!(nodes[0].state().committed_end < index);
            }
        }
        assert_eq!(nodes[0].state().committed_end, index);
        apply_all(&mut app, &nodes[0])?;
        assert!(app.confirmed_view()?.by_name("majority-only").is_some());
        nodes.truncate(majority); // real closed minority file owners; no process/network claim
        let delete = Command::delete(RequestId::new([2; 16])?, catalog::TopicId::new([3; 16])?)?;
        let end = nodes[0].propose(delete.bytes(), 6)?;
        for peer in 1..majority {
            exchange(&mut nodes, 0, peer, 6)?;
        }
        assert_eq!(nodes[0].state().committed_end, end);
        apply_all(&mut app, &nodes[0])?;
        let source = nodes[0].fetch_committed(1, 64, 256 * 1024)?;
        assert!(app.confirmed_view()?.by_name("majority-only").is_none());
        assert!(app
            .confirmed_view()?
            .is_tombstoned(catalog::TopicId::new([3; 16])?));
        drop(app);
        drop(nodes);
        let node = open_node(&temp, 1, &ids, 0)?;
        assert_eq!(node.fetch_committed(1, 64, 256 * 1024)?, source);
        let mut app = Application::open(temp.app(1), &group, limits())?;
        apply_all(&mut app, &node)?;
        assert_eq!(app.confirmed_view()?.operation_count(), 2);
        assert_eq!(app.cursor.index, end);
        Ok(())
    }
    #[test]
    fn three_node_metadata_is_invisible_before_majority_and_survives_minority_reopen() -> TestResult
    {
        quorum_survival(3)
    }
    #[test]
    fn five_node_old_minority_cannot_publish_metadata_and_majority_reopens_exact_prefix(
    ) -> TestResult {
        quorum_survival(5)
    }
    #[test]
    fn new_leader_repairs_uncommitted_metadata_without_creating_a_ghost_topic() -> TestResult {
        let temp = Temp::new()?;
        let ids = [1, 2, 3];
        let group = identity(&ids)?;
        let mut nodes = ids
            .iter()
            .map(|id| open_node(&temp, *id, &ids, 0))
            .collect::<TestResult<Vec<_>>>()?;
        elect(&mut nodes, 0, &[1], 5)?;
        let ghost = command(1, 3, "ghost")?;
        nodes[0].propose(ghost.bytes(), 5)?;
        let mut app = Application::open(temp.app(1), &group, limits())?;
        apply_all(&mut app, &nodes[0])?;
        assert_eq!(app.cursor.index, 0);
        elect(&mut nodes, 1, &[2], 11)?;
        let live = command(2, 4, "surviving")?;
        let end = nodes[1].propose(live.bytes(), 11)?;
        exchange(&mut nodes, 1, 2, 11)?;
        exchange(&mut nodes, 1, 0, 11)?;
        exchange(&mut nodes, 1, 0, 11)?;
        assert_eq!(nodes[0].state().committed_end, end);
        apply_all(&mut app, &nodes[0])?;
        assert!(app.confirmed_view()?.by_name("ghost").is_none());
        assert!(app.confirmed_view()?.by_name("surviving").is_some());
        assert!(app.lookup(ghost.bytes())?.is_none());
        Ok(())
    }
}
