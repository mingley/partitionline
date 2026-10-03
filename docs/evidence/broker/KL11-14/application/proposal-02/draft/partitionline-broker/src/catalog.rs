//! Bounded, synchronous topic identities persisted in the partition journal.
//!
//! Each create or delete is one atomic, checksummed custom journal operation.
//! All validation and allocation precede append; visible state changes only
//! after the journal synchronizes successfully. An ambiguous journal failure
//! poisons mutations. Reads then expose only previously confirmed state; reopen
//! to discover whether an unconfirmed complete operation survived.
//!
//! Names follow Apache Kafka's ASCII/249-character validation and its
//! dot/underscore collision rule. This catalog additionally reserves
//! `__cluster_metadata`. IDs are caller-supplied 128-bit values in big-endian
//! order; Kafka's zero and one UUIDs are reserved. No random allocation is
//! performed. Deleted IDs remain tombstoned and cannot be reused; a deleted
//! name can be recreated with a fresh ID. Live topics, retained identities,
//! partitions, operations, replay bytes, and journal bytes are bounded.
//!
//! The caller selects a trusted journal path, never a path derived from a topic.
//! Blocking I/O belongs on a storage thread. The journal's process-local
//! ownership and filesystem synchronization limitations apply; there is no
//! cross-process locking, Kafka metadata-log format, quorum, replica placement,
//! partition-log creation/deletion, wire handler, retention, or qualification.

use crate::journal;
use std::mem::size_of;
use std::path::Path;

const MAGIC: &[u8; 8] = b"PLTCAT01";
const HEADER: usize = 34;
const MAX_NAME: usize = 249;
const MAX_OPERATION: usize = HEADER + MAX_NAME;
const FETCH_BYTES: usize = size_of::<journal::Entry>() + MAX_OPERATION;

/// Immutable caller-supplied Kafka-style 128-bit topic identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicId([u8; 16]);

impl TopicId {
    /// Validate raw big-endian UUID bytes, rejecting reserved zero and one.
    ///
    /// No UUID version/variant or random-allocation claim is made. Callers must
    /// supply fresh identities; the catalog also rejects previously deleted IDs.
    pub fn new(bytes: [u8; 16]) -> Result<Self, Error> {
        if bytes[..15] == [0; 15] && bytes[15] <= 1 {
            return Err(Error::ReservedIdentity);
        }
        Ok(Self(bytes))
    }

    /// Return the original big-endian identity bytes.
    pub fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Confirmed topic identity and a contiguous `0..partition_count` range.
#[derive(Debug, PartialEq, Eq)]
pub struct Topic {
    id: TopicId,
    name: String,
    partition_count: u32,
}

impl Topic {
    /// Stable topic identity.
    pub fn id(&self) -> TopicId {
        self.id
    }
    /// Original validated name; no normalization changes the stored name.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Number of logical partitions; no replica or partition journal is created.
    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }
}

/// Positive catalog bounds and the enclosing journal's byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_live_topics: usize,
    max_identities: usize,
    max_partitions_per_topic: u32,
    max_total_partitions: u64,
    max_operations: usize,
    max_replay_bytes: u64,
    journal: journal::Limits,
}

impl Limits {
    /// Configure live topics (up to 100,000), historical identities and
    /// operations (up to one million), positive partition and replay budgets.
    ///
    /// Partition counts must fit Kafka's positive `int32` domain. Replay bytes
    /// charge all committed payloads, including tombstones, and also bound new
    /// appends so accepted state remains replayable. Journal entry/index/fetch
    /// limits are tightened to 283 bytes, `max_operations`, and one owned entry.
    /// Supplied journal limits must accommodate the largest valid operation.
    /// Retained state is at most `max_identities` records and 249 name bytes per
    /// record, excluding allocator overhead and caller-owned input/results.
    pub fn new(
        max_live_topics: usize,
        max_identities: usize,
        max_partitions_per_topic: u32,
        max_total_partitions: u64,
        max_operations: usize,
        max_replay_bytes: u64,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_live_topics)
            || !(max_live_topics..=1_000_000).contains(&max_identities)
            || !(1..=i32::MAX as u32).contains(&max_partitions_per_topic)
            || max_total_partitions == 0
            || !(1..=1_000_000).contains(&max_operations)
            || max_replay_bytes < HEADER as u64 + 1
            || journal.max_entry_bytes() < MAX_OPERATION
            || journal.max_fetch_bytes() < FETCH_BYTES
            || journal.max_file_bytes() < (24 + 32 + HEADER + 1) as u64
        {
            return Err(Error::InvalidLimits);
        }
        let journal = journal::Limits::new(
            MAX_OPERATION,
            journal.max_file_bytes(),
            max_operations.min(journal.max_index_entries()),
            FETCH_BYTES,
        )?;
        Ok(Self {
            max_live_topics,
            max_identities,
            max_partitions_per_topic,
            max_total_partitions,
            max_operations: max_operations.min(journal.max_index_entries()),
            max_replay_bytes,
            journal,
        })
    }

    /// Maximum simultaneously live topics.
    pub fn max_live_topics(self) -> usize {
        self.max_live_topics
    }
    /// Maximum identities, counting live topics and permanent tombstones.
    pub fn max_identities(self) -> usize {
        self.max_identities
    }
    /// Maximum logical partitions in one topic.
    pub fn max_partitions_per_topic(self) -> u32 {
        self.max_partitions_per_topic
    }
    /// Maximum simultaneously live logical partitions.
    pub fn max_total_partitions(self) -> u64 {
        self.max_total_partitions
    }
    /// Effective maximum retained operations, also bounded by the journal index.
    pub fn max_operations(self) -> usize {
        self.max_operations
    }
    /// Maximum sum of all retained operation payload bytes.
    pub fn max_replay_bytes(self) -> u64 {
        self.max_replay_bytes
    }
    /// Effective tightened journal bounds.
    pub fn journal_limits(self) -> journal::Limits {
        self.journal
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_live_topics: 1_024,
            max_identities: 4_096,
            max_partitions_per_topic: 10_000,
            max_total_partitions: 100_000,
            max_operations: 8_192,
            max_replay_bytes: 4 * 1024 * 1024,
            journal: journal::Limits::new(MAX_OPERATION, 16 * 1024 * 1024, 8_192, FETCH_BYTES)
                .unwrap_or_default(),
        }
    }
}

/// Invalid complete persisted operation or contradictory history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Corruption {
    /// Bad magic, opcode, flags, length, UTF-8, or delete-only fields.
    Format,
    /// A catalog operation does not occupy exactly one logical journal record.
    RecordCount,
    /// Illegal or locally reserved topic name.
    Name,
    /// Reserved topic ID.
    Identity,
    /// Zero or non-`int32` partition count.
    Partitions,
    /// A live name is created twice without a tombstone.
    DuplicateName,
    /// An identity is reused, including after a tombstone.
    DuplicateIdentity,
    /// Names collide under Apache's dot/underscore normalization.
    NameCollision,
    /// A deletion references an absent or already deleted identity.
    UnknownIdentity,
}

/// Validation, resource, persisted-history, or ambiguous storage failure.
#[derive(Debug)]
pub enum Error {
    /// Invalid/nonpositive bounds or journal bounds too small for this format.
    InvalidLimits,
    /// Name violates Apache's ASCII, length, empty, dot, or double-dot rule.
    InvalidName,
    /// The local metadata topic name is reserved for a future control plane.
    ReservedName,
    /// Topic identity zero or one is reserved by Apache.
    ReservedIdentity,
    /// Partition count is zero or outside the positive `int32` domain.
    InvalidPartitionCount,
    /// A live topic already has this exact name.
    DuplicateName,
    /// This identity is already live or permanently tombstoned.
    DuplicateIdentity,
    /// A live name differs only by Apache's dot/underscore collision mapping.
    NameCollision,
    /// The requested identity is absent or already deleted.
    UnknownIdentity,
    /// Creating a topic exceeds the live-topic bound.
    TopicBudgetExceeded,
    /// Creating a topic exceeds the retained-identity bound.
    IdentityBudgetExceeded,
    /// Per-topic or total live partitions exceed their configured bound.
    PartitionBudgetExceeded,
    /// An append/replay exceeds the retained-operation bound.
    OperationBudgetExceeded,
    /// An append/replay exceeds the cumulative payload byte budget.
    ReplayBudgetExceeded,
    /// A bounded state/name reservation failed before append.
    AllocationFailed,
    /// An ambiguous storage failure requires reopening before mutation.
    Poisoned,
    /// Complete persisted metadata fails closed without becoming visible.
    Corrupt {
        /// Journal operation offset containing invalid metadata.
        first_offset: u64,
        /// Type of persisted damage or contradictory history.
        kind: Corruption,
    },
    /// A legacy direct-create/delete call cannot bypass explicit quorum admission.
    QuorumMode,
    /// Local node has no current replication leadership.
    NotController,
    /// Source or application operation is unavailable/ambiguous.
    QuorumUnavailable,
    /// Absolute request/source deadline expired; the result can be ambiguous.
    QuorumDeadline,
    /// Random request-token allocation or exact token fencing failed.
    RequestIdentity,
    /// Enclosing journal integrity, resource, ownership, or I/O failure.
    Journal(journal::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Journal(error) => write!(f, "catalog storage: {error}"),
            other => write!(f, "catalog: {other:?}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            _ => None,
        }
    }
}
impl From<journal::Error> for Error {
    fn from(value: journal::Error) -> Self {
        Self::Journal(value)
    }
}

/// Validate Apache's ASCII/249-character topic naming rules.
///
/// The catalog's additional metadata-topic reservation and live-name collision
/// checks occur during creation. Names are never treated as file paths.
pub fn validate_topic_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > MAX_NAME
        || name == "."
        || name == ".."
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Err(Error::InvalidName)
    } else {
        Ok(())
    }
}

/// Exclusive journal-backed catalog; reads borrow confirmed state without copies.
pub struct Catalog {
    inner: Backend,
}
enum Backend {
    Local(Core<journal::Journal>),
    Quorum(crate::metadata_quorum::QuorumCatalog),
}

impl Catalog {
    /// Open/create the trusted path, synchronize journal recovery, then replay.
    ///
    /// Replay accepts only this custom catalog format and one record per entry.
    /// Complete malformed/contradictory operations fail closed. Incomplete final
    /// journal entries alone may be durably repaired, as reported by `Recovery`.
    pub fn open(
        path: impl AsRef<Path>,
        limits: Limits,
    ) -> Result<(Self, journal::Recovery), Error> {
        let (mut journal, recovery) = journal::Journal::open(path, 0, limits.journal)?;
        let mut state = State::default();
        let mut offset = 0;
        while offset < journal.next_offset() {
            let entries = journal.fetch(offset, 1, FETCH_BYTES)?;
            let entry = entries.first().ok_or(Error::Corrupt {
                first_offset: offset,
                kind: Corruption::RecordCount,
            })?;
            if entry.record_count != 1 || entry.first_offset != offset {
                return Err(Error::Corrupt {
                    first_offset: offset,
                    kind: Corruption::RecordCount,
                });
            }
            replay(&mut state, &entry.payload, offset, limits)?;
            offset = offset
                .checked_add(1)
                .ok_or(Error::OperationBudgetExceeded)?;
        }
        Ok((
            Self {
                inner: Backend::Local(Core {
                    journal,
                    state,
                    limits,
                }),
            },
            recovery,
        ))
    }

    /// Open explicit committed metadata application on the router storage actor.
    ///
    /// Legacy local journal formats fail closed. No data partition Store is
    /// attached by this constructor. The caller joins the application owner
    /// before shutting down the separately owned private Raft runtime.
    pub fn open_quorum(
        path: impl AsRef<Path>,
        source: crate::raft::runtime::Handle,
        executor: tokio::runtime::Handle,
        limits: crate::metadata_quorum::Limits,
        deadline: std::time::Duration,
    ) -> Result<(Self, journal::Recovery), Error> {
        let inner =
            crate::metadata_quorum::QuorumCatalog::open(path, source, executor, limits, deadline)
                .map_err(quorum_error)?;
        let recovery = inner.recovery();
        Ok((
            Self {
                inner: Backend::Quorum(inner),
            },
            recovery,
        ))
    }
    fn state(&self) -> &State {
        match &self.inner {
            Backend::Local(inner) => &inner.state,
            Backend::Quorum(inner) => &inner.projection().state,
        }
    }
    /// Whether mutations must use the explicit committed-application path.
    pub fn is_quorum(&self) -> bool {
        matches!(&self.inner, Backend::Quorum(_))
    }
    /// Last applied source position, absent for the legacy local catalog.
    pub fn applied_position(&self) -> Option<crate::raft::election::LogPosition> {
        match &self.inner {
            Backend::Local(_) => None,
            Backend::Quorum(inner) => Some(inner.cursor()),
        }
    }
    pub(crate) fn controller_id(&self) -> Option<i32> {
        match &self.inner {
            Backend::Local(_) => None,
            Backend::Quorum(inner) => Some(inner.controller_id()),
        }
    }
    pub(crate) fn refresh(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            Backend::Local(_) => Ok(()),
            Backend::Quorum(inner) => inner.refresh().map(|_| ()).map_err(quorum_error),
        }
    }
    pub(crate) fn refresh_until(&mut self, until: std::time::Instant) -> Result<(), Error> {
        match &mut self.inner {
            Backend::Local(_) => Ok(()),
            Backend::Quorum(inner) => inner
                .refresh_until(tokio::time::Instant::from_std(until))
                .map(|_| ())
                .map_err(quorum_error),
        }
    }
    pub(crate) fn apply_create_until(
        &mut self,
        name: &str,
        id: TopicId,
        count: u32,
        until: std::time::Instant,
    ) -> Result<(), Error> {
        match &mut self.inner {
            Backend::Local(inner) => inner.create(name, id, count).map(|_| ()),
            Backend::Quorum(inner) => {
                let token = fresh_request_id()?;
                let command = crate::metadata_quorum::Command::create(token, id, name, count)
                    .map_err(quorum_error)?;
                let receipt = inner
                    .execute_until(&command, tokio::time::Instant::from_std(until))
                    .map_err(quorum_error)?;
                outcome(receipt.outcome)
            }
        }
    }
    pub(crate) fn apply_delete_until(
        &mut self,
        id: TopicId,
        until: std::time::Instant,
    ) -> Result<(), Error> {
        match &mut self.inner {
            Backend::Local(inner) => inner.delete(id).map(|_| ()),
            Backend::Quorum(inner) => {
                let token = fresh_request_id()?;
                let command =
                    crate::metadata_quorum::Command::delete(token, id).map_err(quorum_error)?;
                let receipt = inner
                    .execute_until(&command, tokio::time::Instant::from_std(until))
                    .map_err(quorum_error)?;
                outcome(receipt.outcome)
            }
        }
    }

    /// Durably create one validated topic before publishing its identity.
    ///
    /// A returned journal append is the confirmed operation receipt. All input,
    /// budget and reservation failures precede journal mutation. Failed writes
    /// or syncs are ambiguous and require recovery, even if the entry survived.
    pub fn create(
        &mut self,
        name: &str,
        id: TopicId,
        partition_count: u32,
    ) -> Result<journal::Append, Error> {
        match &mut self.inner {
            Backend::Local(inner) => inner.create(name, id, partition_count),
            Backend::Quorum(_) => Err(Error::QuorumMode),
        }
    }

    /// Durably tombstone a live identity and all of its logical partitions.
    ///
    /// The name becomes reusable, but the old identity remains reserved forever
    /// within the configured history budget. Partition files are not removed.
    pub fn delete(&mut self, id: TopicId) -> Result<journal::Append, Error> {
        match &mut self.inner {
            Backend::Local(inner) => inner.delete(id),
            Backend::Quorum(_) => Err(Error::QuorumMode),
        }
    }

    /// Look up a confirmed live topic by its exact, case-sensitive name.
    pub fn by_name(&self, name: &str) -> Option<&Topic> {
        self.state()
            .records
            .iter()
            .find_map(|record| (record.live && record.topic.name == name).then_some(&record.topic))
    }
    /// Look up a confirmed live topic by its stable identity.
    pub fn by_id(&self, id: TopicId) -> Option<&Topic> {
        self.state()
            .records
            .iter()
            .find_map(|record| (record.live && record.topic.id == id).then_some(&record.topic))
    }
    /// Whether this identity is permanently tombstoned in confirmed state.
    pub fn is_tombstoned(&self, id: TopicId) -> bool {
        self.state()
            .records
            .iter()
            .any(|record| !record.live && record.topic.id == id)
    }
    /// Iterate live topics in creation order without allocating a snapshot.
    pub fn topics(&self) -> impl Iterator<Item = &Topic> {
        self.state()
            .records
            .iter()
            .filter_map(|record| record.live.then_some(&record.topic))
    }
    /// Number of confirmed live topics.
    pub fn topic_count(&self) -> usize {
        self.state().live_topics
    }
    /// Number of retained live and tombstoned identities.
    pub fn identity_count(&self) -> usize {
        self.state().records.len()
    }
    /// Number of confirmed logical partitions in live topics.
    pub fn total_partitions(&self) -> u64 {
        self.state().partitions
    }
    /// Number of confirmed durable create/delete operations.
    pub fn operation_count(&self) -> usize {
        self.state().operations
    }
    /// Sum of confirmed retained operation payload bytes.
    pub fn replay_bytes(&self) -> u64 {
        self.state().payload_bytes
    }
    /// Last confirmed synchronized journal file length, including headers.
    pub fn journal_bytes(&self) -> u64 {
        match &self.inner {
            Backend::Local(inner) => inner.journal.file_bytes(),
            Backend::Quorum(inner) => inner.journal_bytes(),
        }
    }
    /// Whether mutations require reopening after an ambiguous storage failure.
    ///
    /// Lookups still expose the previous confirmed state, which may differ from
    /// recovery's eventual state. Do not treat it as the current durable outcome.
    pub fn is_poisoned(&self) -> bool {
        match &self.inner {
            Backend::Local(inner) => inner.journal.is_poisoned(),
            Backend::Quorum(inner) => inner.is_poisoned(),
        }
    }
}

/// In-memory catalog state prepared by the committed metadata application owner.
///
/// This type performs no I/O or quorum admission. Its mutation methods remain
/// crate-private: the metadata application journal synchronizes its source
/// receipt before publishing a prepared change. Legacy [`Catalog`] is unchanged.
pub struct Projection {
    state: State,
    limits: Limits,
}

impl Projection {
    pub(crate) fn new(limits: Limits) -> Self {
        Self {
            state: State::default(),
            limits,
        }
    }
    /// Borrow a confirmed topic by its exact name.
    pub fn by_name(&self, name: &str) -> Option<&Topic> {
        self.state
            .records
            .iter()
            .find_map(|r| (r.live && r.topic.name == name).then_some(&r.topic))
    }
    /// Borrow a confirmed topic by identity.
    pub fn by_id(&self, id: TopicId) -> Option<&Topic> {
        self.state
            .records
            .iter()
            .find_map(|r| (r.live && r.topic.id == id).then_some(&r.topic))
    }
    /// A deleted identity stays reserved within the bounded history.
    pub fn is_tombstoned(&self, id: TopicId) -> bool {
        self.state
            .records
            .iter()
            .any(|r| !r.live && r.topic.id == id)
    }
    /// Borrow live topics in deterministic creation order.
    pub fn topics(&self) -> impl Iterator<Item = &Topic> {
        self.state
            .records
            .iter()
            .filter_map(|r| r.live.then_some(&r.topic))
    }
    /// Number of confirmed live topics.
    pub fn topic_count(&self) -> usize {
        self.state.live_topics
    }
    /// Number of retained identities including tombstones.
    pub fn identity_count(&self) -> usize {
        self.state.records.len()
    }
    /// Confirmed logical partition count; no partition file is created.
    pub fn total_partitions(&self) -> u64 {
        self.state.partitions
    }
    /// Number of successful catalog mutations; rejected commands do not count.
    pub fn operation_count(&self) -> usize {
        self.state.operations
    }
    pub(crate) fn prepare_create(
        &mut self,
        name: &str,
        id: TopicId,
        count: u32,
    ) -> Result<Prepared, Error> {
        self.state
            .prepare_create(name, id, count, self.limits)
            .map(Prepared::Create)
    }
    pub(crate) fn prepare_delete(&self, id: TopicId) -> Result<Prepared, Error> {
        self.state
            .prepare_delete(id, self.limits)
            .map(Prepared::Delete)
    }
    pub(crate) fn publish(&mut self, prepared: Prepared) {
        match prepared {
            Prepared::Create(change) => self.state.publish_create(change),
            Prepared::Delete(change) => self.state.publish_delete(change),
        }
    }
}

pub(crate) enum Prepared {
    Create(PreparedCreate),
    Delete(PreparedDelete),
}

fn fresh_request_id() -> Result<crate::metadata_quorum::RequestId, Error> {
    for _ in 0..32 {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|_| Error::RequestIdentity)?;
        if let Ok(id) = crate::metadata_quorum::RequestId::new(bytes) {
            return Ok(id);
        }
    }
    Err(Error::RequestIdentity)
}
fn quorum_error(error: crate::metadata_quorum::Error) -> Error {
    use crate::metadata_quorum::Error as Q;
    match error {
        Q::Deadline | Q::Runtime(crate::raft::runtime::Error::Deadline) => Error::QuorumDeadline,
        Q::Runtime(crate::raft::runtime::Error::Node(
            crate::raft::replication::Error::NotLeader,
        )) => Error::NotController,
        Q::Poisoned => Error::Poisoned,
        Q::Allocation => Error::AllocationFailed,
        Q::Budget => Error::OperationBudgetExceeded,
        Q::RequestConflict | Q::InvalidCommand => Error::RequestIdentity,
        Q::Catalog(error) => error,
        _ => Error::QuorumUnavailable,
    }
}
fn outcome(outcome: crate::metadata_quorum::Outcome) -> Result<(), Error> {
    use crate::metadata_quorum::Outcome as O;
    Err(match outcome {
        O::Applied => return Ok(()),
        O::InvalidName => Error::InvalidName,
        O::ReservedName => Error::ReservedName,
        O::InvalidPartitions => Error::InvalidPartitionCount,
        O::DuplicateName => Error::DuplicateName,
        O::DuplicateIdentity => Error::DuplicateIdentity,
        O::NameCollision => Error::NameCollision,
        O::UnknownIdentity => Error::UnknownIdentity,
        O::TopicBudget => Error::TopicBudgetExceeded,
        O::IdentityBudget => Error::IdentityBudgetExceeded,
        O::PartitionBudget => Error::PartitionBudgetExceeded,
        O::OperationBudget => Error::OperationBudgetExceeded,
        O::ReplayBudget => Error::ReplayBudgetExceeded,
        O::RequestConflict => Error::RequestIdentity,
    })
}

struct Record {
    topic: Topic,
    live: bool,
}
#[derive(Default)]
struct State {
    records: Vec<Record>,
    live_topics: usize,
    partitions: u64,
    operations: usize,
    payload_bytes: u64,
}
struct Totals {
    live_topics: usize,
    partitions: u64,
    operations: usize,
    payload_bytes: u64,
}
pub(crate) struct PreparedCreate {
    record: Record,
    totals: Totals,
}
pub(crate) struct PreparedDelete {
    index: usize,
    totals: Totals,
}

fn collision(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && left
            .bytes()
            .zip(right.bytes())
            .all(|(a, b)| (if a == b'.' { b'_' } else { a }) == (if b == b'.' { b'_' } else { b }))
}

impl State {
    fn totals(&self, length: usize, limits: Limits) -> Result<Totals, Error> {
        let operations = self
            .operations
            .checked_add(1)
            .ok_or(Error::OperationBudgetExceeded)?;
        if operations > limits.max_operations {
            return Err(Error::OperationBudgetExceeded);
        }
        let payload_bytes = self
            .payload_bytes
            .checked_add(length as u64)
            .ok_or(Error::ReplayBudgetExceeded)?;
        if payload_bytes > limits.max_replay_bytes {
            return Err(Error::ReplayBudgetExceeded);
        }
        Ok(Totals {
            live_topics: self.live_topics,
            partitions: self.partitions,
            operations,
            payload_bytes,
        })
    }

    fn prepare_create(
        &mut self,
        name: &str,
        id: TopicId,
        count: u32,
        limits: Limits,
    ) -> Result<PreparedCreate, Error> {
        validate_topic_name(name)?;
        if name == "__cluster_metadata" {
            return Err(Error::ReservedName);
        }
        if count == 0 || count > i32::MAX as u32 {
            return Err(Error::InvalidPartitionCount);
        }
        for record in &self.records {
            if record.topic.id == id {
                return Err(Error::DuplicateIdentity);
            }
            if record.live && record.topic.name == name {
                return Err(Error::DuplicateName);
            }
            if record.live && collision(&record.topic.name, name) {
                return Err(Error::NameCollision);
            }
        }
        if self.live_topics >= limits.max_live_topics {
            return Err(Error::TopicBudgetExceeded);
        }
        if self.records.len() >= limits.max_identities {
            return Err(Error::IdentityBudgetExceeded);
        }
        let mut totals = self.totals(HEADER + name.len(), limits)?;
        totals.live_topics = self
            .live_topics
            .checked_add(1)
            .ok_or(Error::TopicBudgetExceeded)?;
        totals.partitions = self
            .partitions
            .checked_add(u64::from(count))
            .ok_or(Error::PartitionBudgetExceeded)?;
        if count > limits.max_partitions_per_topic
            || totals.partitions > limits.max_total_partitions
        {
            return Err(Error::PartitionBudgetExceeded);
        }
        if self.records.len() == self.records.capacity() {
            let capacity = self
                .records
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(limits.max_identities);
            self.records
                .try_reserve_exact(capacity - self.records.len())
                .map_err(|_| Error::AllocationFailed)?;
        }
        let mut owned = String::new();
        owned
            .try_reserve_exact(name.len())
            .map_err(|_| Error::AllocationFailed)?;
        owned.push_str(name);
        Ok(PreparedCreate {
            record: Record {
                topic: Topic {
                    id,
                    name: owned,
                    partition_count: count,
                },
                live: true,
            },
            totals,
        })
    }

    fn prepare_delete(&self, id: TopicId, limits: Limits) -> Result<PreparedDelete, Error> {
        let index = self
            .records
            .iter()
            .position(|record| record.live && record.topic.id == id)
            .ok_or(Error::UnknownIdentity)?;
        let mut totals = self.totals(HEADER, limits)?;
        totals.live_topics = self
            .live_topics
            .checked_sub(1)
            .ok_or(Error::UnknownIdentity)?;
        totals.partitions = self
            .partitions
            .checked_sub(u64::from(self.records[index].topic.partition_count))
            .ok_or(Error::UnknownIdentity)?;
        Ok(PreparedDelete { index, totals })
    }

    fn publish_totals(&mut self, totals: Totals) {
        self.live_topics = totals.live_topics;
        self.partitions = totals.partitions;
        self.operations = totals.operations;
        self.payload_bytes = totals.payload_bytes;
    }
    fn publish_create(&mut self, prepared: PreparedCreate) {
        self.records.push(prepared.record);
        self.publish_totals(prepared.totals);
    }
    fn publish_delete(&mut self, prepared: PreparedDelete) {
        self.records[prepared.index].live = false;
        self.publish_totals(prepared.totals);
    }
}

fn encode(id: TopicId, name: &str, partitions: u32, create: bool) -> ([u8; MAX_OPERATION], usize) {
    let mut bytes = [0; MAX_OPERATION];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8] = if create { 1 } else { 2 };
    bytes[12..28].copy_from_slice(&id.bytes());
    bytes[28..32].copy_from_slice(&partitions.to_be_bytes());
    bytes[32..34].copy_from_slice(&(name.len() as u16).to_be_bytes());
    bytes[HEADER..HEADER + name.len()].copy_from_slice(name.as_bytes());
    (bytes, HEADER + name.len())
}

trait Appender {
    fn append(&mut self, payload: &[u8]) -> Result<journal::Append, journal::Error>;
    fn poisoned(&self) -> bool;
}
impl Appender for journal::Journal {
    fn append(&mut self, payload: &[u8]) -> Result<journal::Append, journal::Error> {
        journal::Journal::append(self, 1, payload)
    }
    fn poisoned(&self) -> bool {
        self.is_poisoned()
    }
}
struct Core<J> {
    journal: J,
    state: State,
    limits: Limits,
}
impl<J: Appender> Core<J> {
    fn alive(&self) -> Result<(), Error> {
        if self.journal.poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    fn create(&mut self, name: &str, id: TopicId, count: u32) -> Result<journal::Append, Error> {
        self.alive()?;
        let prepared = self.state.prepare_create(name, id, count, self.limits)?;
        let (payload, length) = encode(id, name, count, true);
        let appended = self.journal.append(&payload[..length])?;
        self.state.publish_create(prepared);
        Ok(appended)
    }
    fn delete(&mut self, id: TopicId) -> Result<journal::Append, Error> {
        self.alive()?;
        let prepared = self.state.prepare_delete(id, self.limits)?;
        let (payload, length) = encode(id, "", 0, false);
        let appended = self.journal.append(&payload[..length])?;
        self.state.publish_delete(prepared);
        Ok(appended)
    }
}

fn replay_error(error: Error, offset: u64) -> Error {
    let kind = match error {
        Error::InvalidName | Error::ReservedName => Corruption::Name,
        Error::ReservedIdentity => Corruption::Identity,
        Error::InvalidPartitionCount => Corruption::Partitions,
        Error::DuplicateName => Corruption::DuplicateName,
        Error::DuplicateIdentity => Corruption::DuplicateIdentity,
        Error::NameCollision => Corruption::NameCollision,
        Error::UnknownIdentity => Corruption::UnknownIdentity,
        other => return other,
    };
    Error::Corrupt {
        first_offset: offset,
        kind,
    }
}
fn replay(state: &mut State, payload: &[u8], offset: u64, limits: Limits) -> Result<(), Error> {
    let bad = |kind| Error::Corrupt {
        first_offset: offset,
        kind,
    };
    if payload.len() < HEADER || &payload[..8] != MAGIC || payload[9..12] != [0; 3] {
        return Err(bad(Corruption::Format));
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&payload[12..28]);
    let id = TopicId::new(identity).map_err(|error| replay_error(error, offset))?;
    let count = u32::from_be_bytes([payload[28], payload[29], payload[30], payload[31]]);
    let length = usize::from(u16::from_be_bytes([payload[32], payload[33]]));
    if length > MAX_NAME || payload.len() != HEADER + length {
        return Err(bad(Corruption::Format));
    }
    match payload[8] {
        1 => {
            let name =
                std::str::from_utf8(&payload[HEADER..]).map_err(|_| bad(Corruption::Format))?;
            let prepared = state
                .prepare_create(name, id, count, limits)
                .map_err(|error| replay_error(error, offset))?;
            state.publish_create(prepared);
        }
        2 if length == 0 && count == 0 => {
            let prepared = state
                .prepare_delete(id, limits)
                .map_err(|error| replay_error(error, offset))?;
            state.publish_delete(prepared);
        }
        _ => return Err(bad(Corruption::Format)),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[derive(Default)]
    struct SyncFault {
        entries: Vec<Vec<u8>>,
        fail: bool,
        poisoned: bool,
    }
    impl Appender for SyncFault {
        fn append(&mut self, payload: &[u8]) -> Result<journal::Append, journal::Error> {
            self.entries.push(payload.to_vec());
            if self.fail {
                self.poisoned = true;
                return Err(journal::Error::Io(io::Error::other(
                    "injected sync failure after complete write",
                )));
            }
            let next = self.entries.len() as u64;
            Ok(journal::Append {
                first_offset: next - 1,
                next_offset: next,
                record_count: 1,
                file_bytes: next,
            })
        }
        fn poisoned(&self) -> bool {
            self.poisoned
        }
    }

    #[test]
    fn complete_unconfirmed_create_is_invisible_until_synced_replay() {
        let id = TopicId::new(2u128.to_be_bytes()).unwrap();
        let mut core = Core {
            journal: SyncFault {
                fail: true,
                ..SyncFault::default()
            },
            state: State::default(),
            limits: Limits::default(),
        };
        assert!(matches!(
            core.create("alpha", id, 3),
            Err(Error::Journal(journal::Error::Io(_)))
        ));
        assert!(core.state.records.is_empty());
        assert_eq!(
            (
                core.state.partitions,
                core.state.operations,
                core.state.payload_bytes
            ),
            (0, 0, 0)
        );
        assert!(matches!(core.create("beta", id, 2), Err(Error::Poisoned)));
        assert!(matches!(core.delete(id), Err(Error::Poisoned)));
        assert_eq!(core.journal.entries.len(), 1);
        let mut recovered = State::default();
        replay(&mut recovered, &core.journal.entries[0], 0, core.limits).unwrap();
        assert_eq!(
            (
                recovered.live_topics,
                recovered.partitions,
                recovered.operations
            ),
            (1, 3, 1)
        );
    }

    #[test]
    fn complete_unconfirmed_tombstone_keeps_previous_state_until_recovery() {
        let id = TopicId::new(2u128.to_be_bytes()).unwrap();
        let mut core = Core {
            journal: SyncFault::default(),
            state: State::default(),
            limits: Limits::default(),
        };
        core.create("alpha", id, 3).unwrap();
        core.journal.fail = true;
        assert!(matches!(
            core.delete(id),
            Err(Error::Journal(journal::Error::Io(_)))
        ));
        assert!(core.state.records[0].live);
        assert_eq!(
            (
                core.state.live_topics,
                core.state.partitions,
                core.state.operations
            ),
            (1, 3, 1)
        );
        assert!(matches!(core.delete(id), Err(Error::Poisoned)));
        let mut recovered = State::default();
        for (offset, entry) in core.journal.entries.iter().enumerate() {
            replay(&mut recovered, entry, offset as u64, core.limits).unwrap();
        }
        assert!(!recovered.records[0].live);
        assert_eq!(
            (
                recovered.live_topics,
                recovered.partitions,
                recovered.operations
            ),
            (0, 0, 2)
        );
    }
}
