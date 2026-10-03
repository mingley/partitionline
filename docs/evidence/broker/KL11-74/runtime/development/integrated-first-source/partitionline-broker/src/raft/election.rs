//! Durable fixed-membership election state and deterministic bounded timers.
//!
//! The synchronous journal owns the configured path exclusively within this
//! process and synchronizes every term/vote/log-summary transition before an
//! affirmative vote or outbound candidacy is returned. Run this API on a
//! dedicated storage thread. A persistence failure poisons the election until
//! reopening; an ambiguous complete journal append may be accepted on recovery.
//!
//! Log positions summarize caller-confirmed durable metadata; this module does
//! not replicate or validate that content. Leader state is an election result,
//! not a read/write lease. The replication owner must call [`Election::lose_quorum`]
//! when its quorum is lost. Membership changes, pre-votes, directory identities,
//! Kafka wire epochs and full KRaft compatibility belong to later contracts.

use super::membership::{Key, Voters, MAX_CONFIGURATION_BYTES};
use crate::journal::{self, Journal};
use std::path::Path;

const MAGIC: &[u8; 8] = b"PLELECT1";
const RECONCILE_MAGIC: &[u8; 8] = b"PLRECON1";
const RECONCILE_BYTES: usize = 24;
const HEADER: usize = 48;
const MAX_MEMBERS: usize = 64;
const MAX_STATES: usize = 65_536;

/// Nonnegative voter identity representable by a Kafka node ID.
pub type NodeId = u32;

/// A bounded, immutable voter set containing the local node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    local: NodeId,
    voters: Vec<NodeId>,
    can_vote: bool,
}

impl Membership {
    /// Validate one to 64 distinct nonnegative signed-32-bit IDs, including local.
    pub fn new(local: NodeId, mut voters: Vec<NodeId>) -> Result<Self, Error> {
        if voters.is_empty() || voters.len() > MAX_MEMBERS {
            return Err(Error::InvalidMembership);
        }
        voters.sort_unstable();
        if voters.windows(2).any(|pair| pair[0] == pair[1])
            || voters.iter().any(|id| *id > i32::MAX as u32)
            || voters.binary_search(&local).is_err()
        {
            return Err(Error::InvalidMembership);
        }
        Ok(Self {
            local,
            voters,
            can_vote: true,
        })
    }
    /// Local voter ID.
    pub fn local(&self) -> NodeId {
        self.local
    }
    /// Sorted immutable voters.
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }
    /// Strict majority required to elect a leader.
    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }
    fn dynamic(local: Key, view: &Voters) -> Result<Self, Error> {
        let mut voters = Vec::new();
        voters
            .try_reserve_exact(view.voters().len())
            .map_err(|_| Error::AllocationFailed)?;
        voters.extend(view.voters().iter().map(|v| v.key().id));
        Ok(Self {
            local: local.id,
            voters,
            can_vote: view.contains(local),
        })
    }
    fn position(&self, node: NodeId) -> Result<usize, Error> {
        self.voters
            .binary_search(&node)
            .map_err(|_| Error::UnknownVoter)
    }
}

/// Positive inclusive randomized election timeout interval, in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    minimum_ms: u64,
    maximum_ms: u64,
}

impl Timeouts {
    /// Validate `1 <= minimum <= maximum <= 600000` milliseconds.
    pub fn new(minimum_ms: u64, maximum_ms: u64) -> Result<Self, Error> {
        if minimum_ms == 0 || minimum_ms > maximum_ms || maximum_ms > 600_000 {
            return Err(Error::InvalidTimeouts);
        }
        Ok(Self {
            minimum_ms,
            maximum_ms,
        })
    }
}

/// Last durable log entry's term and one-based index; `(0,0)` is the empty log.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogPosition {
    /// Term of the last durable entry, independent of the election term.
    pub term: u64,
    /// One-based last durable entry index, zero only for an empty log.
    pub index: u64,
}

impl LogPosition {
    /// Reject mixed empty/nonempty coordinates.
    pub fn new(term: u64, index: u64) -> Result<Self, Error> {
        let result = Self { term, index };
        result.validate()?;
        Ok(result)
    }
    fn validate(self) -> Result<(), Error> {
        if (self.term == 0) != (self.index == 0) {
            return Err(Error::InvalidLogPosition);
        }
        Ok(())
    }
}

/// Volatile election role; a leader still requires replication/quorum fencing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Waiting for a leader or election deadline.
    Follower,
    /// Durable self-vote written; collecting distinct peer votes.
    Candidate,
    /// A strict majority voted for this node in its current term.
    Leader,
}

/// Last successfully synchronized persistent election state.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PersistentState {
    /// Current term; checked `u64` domain, not a wire epoch representation.
    pub term: u64,
    /// At most one voter choice for this term.
    pub voted_for: Option<NodeId>,
    /// Summary of caller-confirmed durable log content.
    pub log: LogPosition,
}

/// Diagnostic state, including whether an ambiguous failure disabled mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct State {
    /// Last confirmed state; poisoned storage may contain a later complete state.
    pub persistent: PersistentState,
    /// Volatile role, reset to follower on recovery or storage failure.
    pub role: Role,
    /// Known leader for the local term, reset on candidacy/recovery/quorum loss.
    pub leader: Option<NodeId>,
    /// Absolute caller-clock deadline, never derived from processing completion.
    pub deadline_ms: u64,
    /// Number of distinct grants for this candidate's current term.
    pub granted_votes: usize,
    /// No protocol mutation or successful vote may be emitted until reopening.
    pub poisoned: bool,
}

/// Vote request supplied by a trusted fixed-member peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteRequest {
    /// Requested term, positive and at least the candidate's last log term.
    pub term: u64,
    /// Candidate voter ID.
    pub candidate: NodeId,
    /// Candidate's caller-confirmed durable log summary.
    pub log: LogPosition,
}

/// Vote response; positive responses follow synchronized persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteResponse {
    /// Responder's current term.
    pub term: u64,
    /// Responding fixed-member voter.
    pub voter: NodeId,
    /// Original candidate, used to reject misrouted responses.
    pub candidate: NodeId,
    /// Whether this voter granted the candidate its current-term vote.
    pub granted: bool,
}

/// Result of a due timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tick {
    /// No follower/candidate deadline expired; leaders await quorum fencing.
    Idle,
    /// A synchronized self-vote permits sending this request to peers.
    Campaign(VoteRequest),
}

/// Result of handling a response for a current candidacy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tally {
    /// Stale, duplicate or no longer candidating response.
    Ignored,
    /// A distinct current-term positive vote was counted.
    Counted,
    /// A current-term negative vote; the candidate waits/retries on timeout.
    Rejected,
    /// A strict majority elected this node.
    Elected,
    /// A higher valid member term was persisted and leadership was relinquished.
    SteppedDown,
}

/// Bounded election or durable journal failure; paths/payloads are not exposed.
#[derive(Debug)]
pub enum Error {
    /// Empty, excessive, duplicate, out-of-range or locally absent membership.
    InvalidMembership,
    /// Nonpositive/reversed/excessive timeout interval.
    InvalidTimeouts,
    /// Durable state budget outside one to 65,536 snapshots.
    InvalidStateBudget,
    /// A peer identity is outside the fixed membership.
    UnknownVoter,
    /// Invalid term, candidate log, or misrouted response identity.
    InvalidRequest,
    /// Empty and nonempty log coordinates were mixed.
    InvalidLogPosition,
    /// Log summary regresses, rewrites an index, or exceeds the election term.
    LogRegression,
    /// Increasing the current term would overflow.
    TermOverflow,
    /// Caller clock moved backwards.
    ClockRegression,
    /// Absolute deadline would overflow.
    DeadlineOverflow,
    /// An already known same-term leader conflicts with a new assertion.
    ConflictingLeader,
    /// A node cannot adopt a self-leader assertion without winning an election.
    NotElected,
    /// Reopened journal identities differ from the selected fixed membership.
    MembershipChanged,
    /// Checksummed bytes contain an invalid election transition or encoding.
    CorruptState,
    /// A failed persistence attempt disables mutation until durable recovery.
    Poisoned,
    /// Bounded allocation failed.
    AllocationFailed,
    /// Journal initialization, recovery, append or synchronization failed.
    Storage(journal::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "election storage: {error}"),
            other => write!(formatter, "election: {other:?}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}
impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// File-backed election; ownership, synchronization and corruption policy reuse Journal.
pub struct Election {
    machine: Machine<DurableStore>,
    recovery: journal::Recovery,
}

impl Election {
    /// Recover a bounded election journal, validating every complete state.
    ///
    /// Membership/local identity are persisted and immutable. The snapshot budget
    /// includes initialization; exhausting it fails closed without unbounded file
    /// growth. Compaction belongs to metadata snapshots, not this election API.
    /// Seeded timeouts are deterministic, node-separated and not cryptographic.
    /// The caller supplies monotonic milliseconds and exclusively owns the file
    /// across processes; Journal currently enforces process-local ownership only.
    pub fn open(
        path: impl AsRef<Path>,
        membership: Membership,
        timeouts: Timeouts,
        max_states: usize,
        seed: u64,
        now_ms: u64,
    ) -> Result<Self, Error> {
        if !(1..=MAX_STATES).contains(&max_states) {
            return Err(Error::InvalidStateBudget);
        }
        let payload_bytes = HEADER + membership.voters.len() * 4 + RECONCILE_BYTES;
        let file_bytes = 24 + (32 + payload_bytes) as u64 * max_states as u64;
        let fetch_bytes = std::mem::size_of::<journal::Entry>() + payload_bytes;
        let limits = journal::Limits::new(payload_bytes, file_bytes, max_states, fetch_bytes)?;
        let (mut journal, recovery) = Journal::open(path, 0, limits)?;
        let mut persistent = PersistentState::default();
        for offset in 0..journal.next_offset() {
            let entries = journal.fetch(offset, 1, fetch_bytes)?;
            let entry = entries.first().ok_or(Error::CorruptState)?;
            if entry.first_offset != offset || entry.record_count != 1 {
                return Err(Error::CorruptState);
            }
            let (next, reconciliation) = decode_record(&entry.payload, &membership)?;
            if offset == 0 {
                if next != PersistentState::default() || reconciliation.is_some() {
                    return Err(Error::CorruptState);
                }
            } else if let Some((expected, floor)) = reconciliation {
                validate_reconciliation(persistent, next, expected, floor, &membership)?;
            } else {
                validate_transition(persistent, next, &membership)?;
            }
            persistent = next;
        }
        let mut store = DurableStore {
            journal,
            membership: membership.clone(),
            dynamic: None,
        };
        if store.journal.entry_count() == 0 {
            store.persist(persistent)?;
        }
        let machine = Machine::new(store, membership, persistent, timeouts, seed, now_ms)?;
        Ok(Self { machine, recovery })
    }
    /// Last confirmed durable state and volatile diagnostics.
    pub fn state(&self) -> State {
        self.machine.state()
    }
    /// Advance to a trusted higher term without voting or asserting a leader.
    ///
    /// The term is synchronized before return; an equal/lower term is a no-op.
    /// Wire adapters can initialize their nonzero internal epoch representation
    /// through this transition. It does not prove a peer election or quorum.
    pub fn adopt_term(&mut self, term: u64, now_ms: u64) -> Result<bool, Error> {
        self.machine.adopt_term(term, now_ms)
    }
    /// Override a follower's election deadline after a validated leader resignation.
    ///
    /// The caller must already have fenced the leader/term through
    /// [`Self::observe_leader`]. A leader cannot use this to retain authority.
    /// The current durable term/vote and known leader remain unchanged; this
    /// permits a new election when the absolute deadline expires, not a new
    /// same-term vote. Backoff is bounded to ten minutes and may be zero.
    pub fn end_epoch_backoff(&mut self, backoff_ms: u64, now_ms: u64) -> Result<(), Error> {
        self.machine.end_epoch_backoff(backoff_ms, now_ms)
    }
    /// Initialization or reported incomplete-tail repair performed while opening.
    pub fn recovery(&self) -> journal::Recovery {
        self.recovery
    }
    /// Number of complete durable state snapshots, including initialization.
    pub fn durable_states(&self) -> usize {
        self.machine.store.journal.entry_count()
    }
    /// Trigger one due follower/candidate election, synchronizing its self-vote.
    pub fn tick(&mut self, now_ms: u64) -> Result<Tick, Error> {
        self.machine.tick(now_ms)
    }
    /// Grant at most one fresh candidate per term, after synchronization.
    pub fn request_vote(
        &mut self,
        request: VoteRequest,
        now_ms: u64,
    ) -> Result<VoteResponse, Error> {
        self.machine.request_vote(request, now_ms)
    }
    /// Count distinct fixed-member grants only for this current candidacy.
    pub fn receive_vote(&mut self, response: VoteResponse, now_ms: u64) -> Result<Tally, Error> {
        self.machine.receive_vote(response, now_ms)
    }
    /// Observe a trusted member leader; reject lower terms and same-term conflicts.
    ///
    /// This is a local term/identity fence, not proof of election or a lease.
    /// A wire/replication owner must validate authenticated peer messages and
    /// election/quorum semantics before supplying this assertion.
    pub fn observe_leader(
        &mut self,
        leader: NodeId,
        term: u64,
        now_ms: u64,
    ) -> Result<bool, Error> {
        self.machine.observe_leader(leader, term, now_ms)
    }
    /// Persist a monotonically extended caller-confirmed durable log summary.
    ///
    /// This does not write log content. Truncation and content reconciliation
    /// require the later replication contract and are deliberately unsupported.
    pub fn advance_log(&mut self, log: LogPosition) -> Result<(), Error> {
        self.machine.advance_log(log)
    }
    /// Reconcile a caller-verified durable content suffix before exposing votes.
    ///
    /// Only the crate's replication/recovery owner may supply this transition.
    /// Its operation WAL is authoritative for the committed floor and bytes;
    /// this journal separately retains term/vote and the exact prior summary.
    pub(crate) fn reconcile_durable_log(
        &mut self,
        expected: LogPosition,
        replacement: LogPosition,
        committed_end: u64,
    ) -> Result<(), Error> {
        self.machine
            .reconcile_durable_log(expected, replacement, committed_end)
    }

    /// Relinquish volatile authority on replication-owner quorum loss.
    ///
    /// The durable term/vote remain intact; this cannot permit a second vote.
    pub fn lose_quorum(&mut self, now_ms: u64) -> Result<(), Error> {
        self.machine.lose_quorum(now_ms)
    }
}

trait Store {
    fn validate_transition(
        &self,
        old: PersistentState,
        next: PersistentState,
        membership: &Membership,
    ) -> Result<(), Error> {
        validate_transition(old, next, membership)
    }
    fn validate_reconciliation(
        &self,
        old: PersistentState,
        next: PersistentState,
        expected: LogPosition,
        floor: u64,
        membership: &Membership,
    ) -> Result<(), Error> {
        validate_reconciliation(old, next, expected, floor, membership)
    }
    fn persist(&mut self, state: PersistentState) -> Result<(), Error>;
    fn persist_reconciled(
        &mut self,
        state: PersistentState,
        expected: LogPosition,
        floor: u64,
    ) -> Result<(), Error> {
        let _ = (expected, floor);
        self.persist(state)
    }
}

struct DurableStore {
    journal: Journal,
    membership: Membership,
    dynamic: Option<DynamicStore>,
}
impl Store for DurableStore {
    fn validate_transition(
        &self,
        old: PersistentState,
        next: PersistentState,
        membership: &Membership,
    ) -> Result<(), Error> {
        if self.dynamic.is_some() {
            validate_dynamic_transition(old, next, membership)
        } else {
            validate_transition(old, next, membership)
        }
    }
    fn validate_reconciliation(
        &self,
        old: PersistentState,
        next: PersistentState,
        expected: LogPosition,
        floor: u64,
        membership: &Membership,
    ) -> Result<(), Error> {
        if self.dynamic.is_some() {
            let mut checked = membership.clone();
            if let Some(id) = next.voted_for {
                if checked.position(id).is_err() {
                    checked.voters.push(id);
                    checked.voters.sort_unstable();
                }
            }
            validate_reconciliation(old, next, expected, floor, &checked)
        } else {
            validate_reconciliation(old, next, expected, floor, membership)
        }
    }
    fn persist(&mut self, state: PersistentState) -> Result<(), Error> {
        if let Some(dynamic) = &mut self.dynamic {
            let (payload, vote) = dynamic.encode_state(state, None)?;
            self.journal.append(1, &payload)?;
            dynamic.state = state;
            dynamic.vote = vote;
            return Ok(());
        }
        let payload = encode(state, &self.membership)?;
        self.journal.append(1, &payload)?;
        Ok(())
    }
    fn persist_reconciled(
        &mut self,
        state: PersistentState,
        expected: LogPosition,
        floor: u64,
    ) -> Result<(), Error> {
        if let Some(dynamic) = &mut self.dynamic {
            let (payload, vote) = dynamic.encode_state(state, Some((expected, floor)))?;
            self.journal.append(1, &payload)?;
            dynamic.state = state;
            dynamic.vote = vote;
            return Ok(());
        }
        let mut payload = encode(state, &self.membership)?;
        payload
            .try_reserve_exact(RECONCILE_BYTES)
            .map_err(|_| Error::AllocationFailed)?;
        payload[..8].copy_from_slice(RECONCILE_MAGIC);
        payload.extend_from_slice(&expected.term.to_be_bytes());
        payload.extend_from_slice(&expected.index.to_be_bytes());
        payload.extend_from_slice(&floor.to_be_bytes());
        self.journal.append(1, &payload)?;
        Ok(())
    }
}

struct Machine<S> {
    store: S,
    membership: Membership,
    persistent: PersistentState,
    role: Role,
    leader: Option<NodeId>,
    granted: Vec<bool>,
    timeouts: Timeouts,
    random: u64,
    now_ms: u64,
    deadline_ms: u64,
    poisoned: bool,
    retained_leader: Option<NodeId>,
}

impl<S: Store> Machine<S> {
    fn reconcile_durable_log(
        &mut self,
        expected: LogPosition,
        replacement: LogPosition,
        floor: u64,
    ) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if expected != self.persistent.log || replacement.index < floor || floor > expected.index {
            return Err(Error::LogRegression);
        }
        if expected == replacement {
            return Ok(());
        }
        if replacement.index > expected.index && replacement.term >= expected.term {
            return self.advance_log(replacement);
        }
        if self.role != Role::Follower {
            return Err(Error::NotElected);
        }
        let next = PersistentState {
            log: replacement,
            ..self.persistent
        };
        self.store.validate_reconciliation(
            self.persistent,
            next,
            expected,
            floor,
            &self.membership,
        )?;
        if let Err(error) = self.store.persist_reconciled(next, expected, floor) {
            self.poisoned = true;
            self.role = Role::Follower;
            self.leader = None;
            self.granted.fill(false);
            return Err(error);
        }
        self.persistent = next;
        Ok(())
    }

    fn adopt_term(&mut self, term: u64, now_ms: u64) -> Result<bool, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if term == 0 {
            return Err(Error::InvalidRequest);
        }
        self.clock(now_ms)?;
        if term <= self.persistent.term {
            return Ok(false);
        }
        self.follower(self.deadline_ms);
        self.retained_leader = None;
        let deadline = self.draw_deadline(now_ms)?;
        self.persist(PersistentState {
            term,
            voted_for: None,
            log: self.persistent.log,
        })?;
        self.follower(deadline);
        Ok(true)
    }
    fn end_epoch_backoff(&mut self, backoff_ms: u64, now_ms: u64) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.role != Role::Follower || self.leader.is_none() {
            return Err(Error::InvalidRequest);
        }
        if backoff_ms > 600_000 {
            return Err(Error::InvalidTimeouts);
        }
        self.clock(now_ms)?;
        self.deadline_ms = now_ms
            .checked_add(backoff_ms)
            .ok_or(Error::DeadlineOverflow)?;
        Ok(())
    }
    fn new(
        store: S,
        membership: Membership,
        persistent: PersistentState,
        timeouts: Timeouts,
        seed: u64,
        now_ms: u64,
    ) -> Result<Self, Error> {
        let mut granted = Vec::new();
        granted
            .try_reserve_exact(membership.voters.len())
            .map_err(|_| Error::AllocationFailed)?;
        granted.resize(membership.voters.len(), false);
        let random = seed ^ u64::from(membership.local).wrapping_mul(0xd134_2543_de82_ef95);
        let mut result = Self {
            store,
            membership,
            persistent,
            role: Role::Follower,
            leader: None,
            granted,
            timeouts,
            random,
            now_ms,
            deadline_ms: 0,
            poisoned: false,
            retained_leader: None,
        };
        result.deadline_ms = result.draw_deadline(now_ms)?;
        Ok(result)
    }
    fn state(&self) -> State {
        State {
            persistent: self.persistent,
            role: self.role,
            leader: self.leader,
            deadline_ms: self.deadline_ms,
            granted_votes: self.granted.iter().filter(|value| **value).count(),
            poisoned: self.poisoned,
        }
    }
    fn clock(&mut self, now_ms: u64) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if now_ms < self.now_ms {
            return Err(Error::ClockRegression);
        }
        self.now_ms = now_ms;
        Ok(())
    }
    fn draw_deadline(&mut self, now_ms: u64) -> Result<u64, Error> {
        // SplitMix64 is a deterministic bounded-cost scheduling PRNG, not security.
        self.random = self.random.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.random;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        let interval = self.timeouts.maximum_ms - self.timeouts.minimum_ms + 1;
        let delay = self.timeouts.minimum_ms + value % interval;
        now_ms.checked_add(delay).ok_or(Error::DeadlineOverflow)
    }
    fn persist(&mut self, next: PersistentState) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if next == self.persistent {
            return Ok(());
        }
        self.store
            .validate_transition(self.persistent, next, &self.membership)?;
        if let Err(error) = self.store.persist(next) {
            self.poisoned = true;
            self.role = Role::Follower;
            self.leader = None;
            self.granted.fill(false);
            return Err(error);
        }
        self.persistent = next;
        Ok(())
    }
    fn follower(&mut self, deadline_ms: u64) {
        self.role = Role::Follower;
        self.leader = None;
        self.granted.fill(false);
        self.deadline_ms = deadline_ms;
    }
    fn tick(&mut self, now_ms: u64) -> Result<Tick, Error> {
        self.clock(now_ms)?;
        if !self.membership.can_vote || self.role == Role::Leader || now_ms < self.deadline_ms {
            return Ok(Tick::Idle);
        }
        let term = self
            .persistent
            .term
            .checked_add(1)
            .ok_or(Error::TermOverflow)?;
        let deadline = self.draw_deadline(now_ms)?;
        self.persist(PersistentState {
            term,
            voted_for: Some(self.membership.local),
            log: self.persistent.log,
        })?;
        self.follower(deadline);
        self.role = Role::Candidate;
        self.granted[self.membership.position(self.membership.local)?] = true;
        if self.membership.majority() == 1 {
            self.role = Role::Leader;
            self.leader = Some(self.membership.local);
        }
        Ok(Tick::Campaign(VoteRequest {
            term,
            candidate: self.membership.local,
            log: self.persistent.log,
        }))
    }
    fn request_vote(&mut self, request: VoteRequest, now_ms: u64) -> Result<VoteResponse, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.membership.position(request.candidate)?;
        request.log.validate()?;
        if request.term == 0 || request.log.term > request.term {
            return Err(Error::InvalidRequest);
        }
        self.clock(now_ms)?;
        let higher = request.term > self.persistent.term;
        if higher {
            // A valid newer term revokes volatile leadership even when arming
            // its timer fails. No protocol reply is emitted on that error.
            self.follower(self.deadline_ms);
            self.retained_leader = None;
        }
        let term = self.persistent.term.max(request.term);
        let vote = if higher {
            None
        } else {
            self.persistent.voted_for
        };
        let leader_ok = higher || self.leader.is_none() || self.leader == Some(request.candidate);
        let granted = self.membership.can_vote
            && request.term >= self.persistent.term
            && request.log >= self.persistent.log
            && (vote.is_none() || vote == Some(request.candidate))
            && leader_ok;
        let deadline = if higher || granted {
            Some(self.draw_deadline(now_ms)?)
        } else {
            None
        };
        self.persist(PersistentState {
            term,
            voted_for: if granted {
                Some(request.candidate)
            } else {
                vote
            },
            log: self.persistent.log,
        })?;
        if higher {
            self.follower(deadline.ok_or(Error::DeadlineOverflow)?);
        }
        if let Some(deadline) = deadline {
            self.deadline_ms = deadline;
        }
        Ok(VoteResponse {
            term: self.persistent.term,
            voter: self.membership.local,
            candidate: request.candidate,
            granted,
        })
    }
    fn receive_vote(&mut self, response: VoteResponse, now_ms: u64) -> Result<Tally, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let position = self.membership.position(response.voter)?;
        if response.candidate != self.membership.local || response.term == 0 {
            return Err(Error::InvalidRequest);
        }
        self.clock(now_ms)?;
        if response.term > self.persistent.term {
            self.follower(self.deadline_ms);
            self.retained_leader = None;
            let deadline = self.draw_deadline(now_ms)?;
            self.persist(PersistentState {
                term: response.term,
                voted_for: None,
                log: self.persistent.log,
            })?;
            self.follower(deadline);
            return Ok(Tally::SteppedDown);
        }
        if response.term != self.persistent.term
            || self.role != Role::Candidate
            || self.granted[position]
        {
            return Ok(Tally::Ignored);
        }
        if !response.granted {
            return Ok(Tally::Rejected);
        }
        self.granted[position] = true;
        if self.granted.iter().filter(|value| **value).count() >= self.membership.majority() {
            self.role = Role::Leader;
            self.leader = Some(self.membership.local);
            Ok(Tally::Elected)
        } else {
            Ok(Tally::Counted)
        }
    }
    fn observe_leader(&mut self, leader: NodeId, term: u64, now_ms: u64) -> Result<bool, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if !(self.retained_leader == Some(leader) && term == self.persistent.term) {
            self.membership.position(leader)?;
        }
        if term == 0 {
            return Err(Error::InvalidRequest);
        }
        self.clock(now_ms)?;
        if term < self.persistent.term {
            return Ok(false);
        }
        if leader == self.membership.local
            && (term != self.persistent.term || self.role != Role::Leader)
        {
            return Err(Error::NotElected);
        }
        if term == self.persistent.term && self.leader.is_some() && self.leader != Some(leader) {
            return Err(Error::ConflictingLeader);
        }
        if term > self.persistent.term {
            self.follower(self.deadline_ms);
            self.retained_leader = None;
        }
        let deadline = self.draw_deadline(now_ms)?;
        if term > self.persistent.term {
            self.persist(PersistentState {
                term,
                voted_for: None,
                log: self.persistent.log,
            })?;
        }
        if leader != self.membership.local {
            self.follower(deadline);
        }
        self.leader = Some(leader);
        self.deadline_ms = deadline;
        Ok(true)
    }
    fn advance_log(&mut self, log: LogPosition) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        log.validate()?;
        self.persist(PersistentState {
            log,
            ..self.persistent
        })
    }
    fn lose_quorum(&mut self, now_ms: u64) -> Result<(), Error> {
        self.clock(now_ms)?;
        let deadline = self.draw_deadline(now_ms)?;
        self.follower(deadline);
        Ok(())
    }
}

fn validate_transition(
    old: PersistentState,
    next: PersistentState,
    membership: &Membership,
) -> Result<(), Error> {
    next.log.validate()?;
    if next.term < old.term
        || next.log.term > next.term
        || (next.term == 0 && next.voted_for.is_some())
    {
        return Err(Error::CorruptState);
    }
    if let Some(node) = next.voted_for {
        membership.position(node)?;
    }
    if next.term == old.term && old.voted_for.is_some() && next.voted_for != old.voted_for {
        return Err(Error::CorruptState);
    }
    if next.log.index < old.log.index
        || next.log.term < old.log.term
        || (next.log.index == old.log.index && next.log != old.log)
    {
        return Err(Error::LogRegression);
    }
    Ok(())
}

fn validate_reconciliation(
    old: PersistentState,
    next: PersistentState,
    expected: LogPosition,
    floor: u64,
    membership: &Membership,
) -> Result<(), Error> {
    next.log.validate()?;
    if old.log != expected
        || next.term != old.term
        || next.voted_for != old.voted_for
        || next.log == old.log
        || old.term <= old.log.term
        || next.log.term > next.term
        || floor > old.log.index
        || next.log.index < floor
    {
        return Err(Error::LogRegression);
    }
    if let Some(voter) = next.voted_for {
        membership.position(voter)?;
    }
    Ok(())
}

fn decode_record(
    bytes: &[u8],
    membership: &Membership,
) -> Result<(PersistentState, Option<(LogPosition, u64)>), Error> {
    if !bytes.starts_with(RECONCILE_MAGIC) {
        return Ok((decode(bytes, membership)?, None));
    }
    let size = HEADER + membership.voters.len() * 4;
    if bytes.len() != size + RECONCILE_BYTES {
        return Err(Error::CorruptState);
    }
    let mut legacy = Vec::new();
    legacy
        .try_reserve_exact(size)
        .map_err(|_| Error::AllocationFailed)?;
    legacy.extend_from_slice(&bytes[..size]);
    legacy[..8].copy_from_slice(MAGIC);
    let number = |offset: usize| -> Result<u64, Error> {
        Ok(u64::from_be_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .map_err(|_| Error::CorruptState)?,
        ))
    };
    let expected = LogPosition::new(number(size)?, number(size + 8)?)?;
    Ok((
        decode(&legacy, membership)?,
        Some((expected, number(size + 16)?)),
    ))
}

fn encode(state: PersistentState, membership: &Membership) -> Result<Vec<u8>, Error> {
    let length = HEADER + membership.voters.len() * 4;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| Error::AllocationFailed)?;
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&membership.local.to_be_bytes());
    bytes.extend_from_slice(&(membership.voters.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&[0; 2]);
    bytes.extend_from_slice(&state.term.to_be_bytes());
    bytes.extend_from_slice(&state.voted_for.unwrap_or(0).to_be_bytes());
    bytes.push(u8::from(state.voted_for.is_some()));
    bytes.extend_from_slice(&[0; 3]);
    bytes.extend_from_slice(&state.log.term.to_be_bytes());
    bytes.extend_from_slice(&state.log.index.to_be_bytes());
    for voter in &membership.voters {
        bytes.extend_from_slice(&voter.to_be_bytes());
    }
    Ok(bytes)
}

fn decode(bytes: &[u8], membership: &Membership) -> Result<PersistentState, Error> {
    if bytes.len() != HEADER + membership.voters.len() * 4
        || &bytes[..8] != MAGIC
        || bytes[14..16] != [0; 2]
        || bytes[29..32] != [0; 3]
        || bytes[28] > 1
    {
        return Err(Error::CorruptState);
    }
    fn number<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Error> {
        bytes.try_into().map_err(|_| Error::CorruptState)
    }
    let local = u32::from_be_bytes(number(&bytes[8..12])?);
    let count = usize::from(u16::from_be_bytes(number(&bytes[12..14])?));
    if local != membership.local || count != membership.voters.len() {
        return Err(Error::MembershipChanged);
    }
    for (index, voter) in membership.voters.iter().enumerate() {
        let offset = HEADER + index * 4;
        if u32::from_be_bytes(number(&bytes[offset..offset + 4])?) != *voter {
            return Err(Error::MembershipChanged);
        }
    }
    let node = u32::from_be_bytes(number(&bytes[24..28])?);
    if bytes[28] == 0 && node != 0 {
        return Err(Error::CorruptState);
    }
    let state = PersistentState {
        term: u64::from_be_bytes(number(&bytes[16..24])?),
        voted_for: if bytes[28] == 1 { Some(node) } else { None },
        log: LogPosition::new(
            u64::from_be_bytes(number(&bytes[32..40])?),
            u64::from_be_bytes(number(&bytes[40..48])?),
        )?,
    };
    validate_transition(PersistentState::default(), state, membership)?;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum Failure {
        BeforeWrite,
        CompleteUnconfirmed,
    }
    #[derive(Default)]
    struct MemoryStore {
        confirmed: PersistentState,
        complete: Option<PersistentState>,
        failure: Option<Failure>,
        synchronizations: usize,
    }
    impl Store for MemoryStore {
        fn persist(&mut self, state: PersistentState) -> Result<(), Error> {
            if let Some(failure) = self.failure.take() {
                if matches!(failure, Failure::CompleteUnconfirmed) {
                    self.complete = Some(state);
                }
                return Err(Error::Storage(journal::Error::Io(std::io::Error::other(
                    "injected election write/sync failure",
                ))));
            }
            self.synchronizations += 1;
            self.confirmed = state;
            Ok(())
        }
    }
    fn node(store: MemoryStore, state: PersistentState) -> Result<Machine<MemoryStore>, Error> {
        Machine::new(
            store,
            Membership::new(1, vec![1, 2, 3])?,
            state,
            Timeouts::new(5, 5)?,
            7,
            0,
        )
    }
    #[test]
    fn owner_reconciliation_preserves_term_vote_and_checks_floor_and_role() {
        let old = PersistentState {
            term: 3,
            voted_for: Some(2),
            log: LogPosition { term: 2, index: 4 },
        };
        let mut machine = node(MemoryStore::default(), old).unwrap();
        let replacement = LogPosition { term: 3, index: 4 };
        machine
            .reconcile_durable_log(old.log, replacement, 2)
            .unwrap();
        assert_eq!(machine.state().persistent.term, old.term);
        assert_eq!(machine.state().persistent.voted_for, old.voted_for);
        assert_eq!(machine.store.confirmed.log, replacement);
        assert_eq!(machine.store.synchronizations, 1);
        let mut machine = node(MemoryStore::default(), old).unwrap();
        assert!(machine
            .reconcile_durable_log(old.log, LogPosition { term: 2, index: 1 }, 2)
            .is_err());
        assert!(machine
            .reconcile_durable_log(LogPosition { term: 2, index: 5 }, replacement, 2)
            .is_err());
        machine.role = Role::Leader;
        assert!(machine
            .reconcile_durable_log(old.log, replacement, 2)
            .is_err());
        machine.role = Role::Follower;
        machine.store.failure = Some(Failure::CompleteUnconfirmed);
        assert!(machine
            .reconcile_durable_log(old.log, replacement, 2)
            .is_err());
        assert!(machine.state().poisoned);
        assert_eq!(machine.state().persistent, old);
        assert_eq!(machine.store.complete.unwrap().voted_for, old.voted_for);
    }
    #[test]
    fn synchronization_precedes_grant_and_ambiguous_failure_requires_recovery() {
        for failure in [Failure::BeforeWrite, Failure::CompleteUnconfirmed] {
            let mut machine = node(MemoryStore::default(), PersistentState::default()).unwrap();
            machine.store.failure = Some(failure);
            let request = VoteRequest {
                term: 1,
                candidate: 2,
                log: LogPosition::default(),
            };
            assert!(machine.request_vote(request, 0).is_err());
            assert!(machine.state().poisoned);
            assert_eq!(machine.state().role, Role::Follower);
            assert_eq!(machine.store.synchronizations, 0);
            assert_eq!(machine.state().persistent.term, 0);
            assert!(matches!(
                machine.request_vote(request, 0),
                Err(Error::Poisoned)
            ));
            assert!(matches!(machine.tick(5), Err(Error::Poisoned)));
            let recovered = machine.store.complete.unwrap_or(machine.store.confirmed);
            // Journal recovery synchronizes any complete unconfirmed bytes before
            // exposing them. This private store isolates the protocol consequence.
            let store = MemoryStore {
                confirmed: recovered,
                synchronizations: 1,
                ..MemoryStore::default()
            };
            let mut reopened = node(store, recovered).unwrap();
            let different = reopened
                .request_vote(
                    VoteRequest {
                        candidate: 3,
                        ..request
                    },
                    0,
                )
                .unwrap();
            assert_eq!(different.granted, matches!(failure, Failure::BeforeWrite));
            let same = reopened
                .request_vote(VoteRequest { term: 2, ..request }, 1)
                .unwrap();
            assert!(same.granted);
            assert_eq!(reopened.store.confirmed.term, same.term);
            assert_eq!(reopened.store.confirmed.voted_for, Some(2));
            assert!(reopened.store.synchronizations >= 2);
        }
    }
    #[test]
    fn higher_term_revokes_old_leadership_even_if_timer_cannot_be_armed() {
        for action in 0..3 {
            let mut machine = node(MemoryStore::default(), PersistentState::default()).unwrap();
            machine.tick(5).unwrap();
            assert_eq!(
                machine
                    .receive_vote(
                        VoteResponse {
                            term: 1,
                            voter: 2,
                            candidate: 1,
                            granted: true,
                        },
                        5,
                    )
                    .unwrap(),
                Tally::Elected
            );
            let error = match action {
                0 => machine
                    .request_vote(
                        VoteRequest {
                            term: 2,
                            candidate: 3,
                            log: LogPosition::default(),
                        },
                        u64::MAX,
                    )
                    .unwrap_err(),
                1 => machine
                    .receive_vote(
                        VoteResponse {
                            term: 2,
                            voter: 3,
                            candidate: 1,
                            granted: false,
                        },
                        u64::MAX,
                    )
                    .unwrap_err(),
                _ => machine.observe_leader(3, 2, u64::MAX).unwrap_err(),
            };
            assert!(matches!(error, Error::DeadlineOverflow));
            assert_eq!(machine.state().role, Role::Follower);
            assert_eq!(machine.state().leader, None);
            assert_eq!(machine.state().granted_votes, 0);
            assert_eq!(machine.state().persistent.term, 1);
            assert_eq!(machine.state().persistent.voted_for, Some(1));
            assert!(!machine.state().poisoned);
            assert!(matches!(
                machine.tick(u64::MAX),
                Err(Error::DeadlineOverflow)
            ));
        }
    }
    #[test]
    fn failed_higher_term_persistence_disables_old_leader_before_any_reply() {
        let mut machine = node(MemoryStore::default(), PersistentState::default()).unwrap();
        machine.tick(5).unwrap();
        assert_eq!(
            machine
                .receive_vote(
                    VoteResponse {
                        term: 1,
                        candidate: 1,
                        voter: 2,
                        granted: true
                    },
                    5
                )
                .unwrap(),
            Tally::Elected
        );
        machine.store.failure = Some(Failure::CompleteUnconfirmed);
        assert!(machine
            .receive_vote(
                VoteResponse {
                    term: 7,
                    candidate: 1,
                    voter: 3,
                    granted: false
                },
                6
            )
            .is_err());
        assert!(machine.state().poisoned);
        assert_eq!(machine.state().role, Role::Follower);
        assert_eq!(machine.state().leader, None);
        assert!(matches!(
            machine.observe_leader(1, 1, 6),
            Err(Error::Poisoned)
        ));
        assert!(matches!(
            machine.receive_vote(
                VoteResponse {
                    term: 1,
                    candidate: 1,
                    voter: 3,
                    granted: true
                },
                6
            ),
            Err(Error::Poisoned)
        ));
        let recovered = machine.store.complete.unwrap();
        let reopened = node(MemoryStore::default(), recovered).unwrap();
        assert_eq!(reopened.state().persistent.term, 7);
        assert_eq!(reopened.state().persistent.voted_for, None);
        assert_eq!(reopened.state().role, Role::Follower);
    }
    #[test]
    fn strict_state_encoding_rejects_reserved_bits_unknown_votes_and_empty_mismatch() {
        let members = Membership::new(1, vec![1, 2, 3]).unwrap();
        let state = PersistentState {
            term: 4,
            voted_for: Some(2),
            log: LogPosition { term: 3, index: 10 },
        };
        let bytes = encode(state, &members).unwrap();
        assert_eq!(decode(&bytes, &members).unwrap(), state);
        for index in [14, 29, 30, 31] {
            let mut changed = bytes.clone();
            changed[index] = 1;
            assert!(decode(&changed, &members).is_err());
        }
        let mut changed = bytes.clone();
        changed[24..28].copy_from_slice(&99u32.to_be_bytes());
        assert!(decode(&changed, &members).is_err());
        let mut changed = bytes.clone();
        changed[32..40].fill(0);
        assert!(decode(&changed, &members).is_err());
        assert!(decode(&bytes[..bytes.len() - 1], &members).is_err());
    }
}

const DYNAMIC_INIT: &[u8; 8] = b"PLDINIT2";
const DYNAMIC_STATE: &[u8; 8] = b"PLELECT2";
const DYNAMIC_RECONCILE: &[u8; 8] = b"PLRECON2";
const DYNAMIC_CONFIG: &[u8; 8] = b"PLELCFG2";
const DYNAMIC_HEADER: usize = 96;

struct DynamicStore {
    local: Key,
    view: Voters,
    source_operation: u64,
    state: PersistentState,
    vote: Option<Key>,
}
impl DynamicStore {
    fn encode_state(
        &self,
        state: PersistentState,
        reconciliation: Option<(LogPosition, u64)>,
    ) -> Result<(Vec<u8>, Option<Key>), Error> {
        let vote = match state.voted_for {
            None => None,
            Some(id)
                if state.term == self.state.term && state.voted_for == self.state.voted_for =>
            {
                Some(
                    self.vote
                        .filter(|key| key.id == id)
                        .ok_or(Error::CorruptState)?,
                )
            }
            Some(id) => Some(self.view.by_id(id).ok_or(Error::UnknownVoter)?.key()),
        };
        let magic = if reconciliation.is_some() {
            DYNAMIC_RECONCILE
        } else {
            DYNAMIC_STATE
        };
        let mut bytes = encode_dynamic(
            magic,
            self.local,
            state,
            vote,
            &self.view,
            self.source_operation,
        )?;
        if let Some((expected, floor)) = reconciliation {
            bytes
                .try_reserve_exact(24)
                .map_err(|_| Error::AllocationFailed)?;
            bytes.extend_from_slice(&expected.term.to_be_bytes());
            bytes.extend_from_slice(&expected.index.to_be_bytes());
            bytes.extend_from_slice(&floor.to_be_bytes());
        }
        Ok((bytes, vote))
    }
}
fn encode_dynamic(
    magic: &[u8; 8],
    local: Key,
    state: PersistentState,
    vote: Option<Key>,
    view: &Voters,
    source_operation: u64,
) -> Result<Vec<u8>, Error> {
    let config = view.encode().map_err(|_| Error::CorruptState)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(DYNAMIC_HEADER + config.len())
        .map_err(|_| Error::AllocationFailed)?;
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&local.id.to_be_bytes());
    bytes.extend_from_slice(&local.directory);
    bytes.extend_from_slice(&state.term.to_be_bytes());
    bytes.extend_from_slice(&state.voted_for.unwrap_or(0).to_be_bytes());
    bytes.push(u8::from(vote.is_some()));
    bytes.extend_from_slice(&[0; 3]);
    bytes.extend_from_slice(&vote.map_or([0; 16], |key| key.directory));
    bytes.extend_from_slice(&state.log.term.to_be_bytes());
    bytes.extend_from_slice(&state.log.index.to_be_bytes());
    bytes.extend_from_slice(&source_operation.to_be_bytes());
    bytes.extend_from_slice(&(config.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&[0; 8]);
    bytes.extend_from_slice(&config);
    Ok(bytes)
}
fn dynamic_initial(local: Key, genesis: &Voters) -> Result<Vec<u8>, Error> {
    if genesis.epoch() != 0 || genesis.position() != LogPosition::default() {
        return Err(Error::InvalidMembership);
    }
    Key::new(local.id, local.directory).map_err(|_| Error::InvalidMembership)?;
    let config = genesis.encode().map_err(|_| Error::InvalidMembership)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(40 + config.len())
        .map_err(|_| Error::AllocationFailed)?;
    bytes.extend_from_slice(DYNAMIC_INIT);
    bytes.extend_from_slice(&local.id.to_be_bytes());
    bytes.extend_from_slice(&local.directory);
    bytes.extend_from_slice(&(config.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&[0; 8]);
    bytes.extend_from_slice(&config);
    Ok(bytes)
}
struct DynamicDecoded {
    state: PersistentState,
    vote: Option<Key>,
    view: Voters,
    source_operation: u64,
    reconciliation: Option<(LogPosition, u64)>,
    configuration_floor: Option<u64>,
}
fn decode_dynamic(bytes: &[u8], local: Key) -> Result<DynamicDecoded, Error> {
    if bytes.len() < DYNAMIC_HEADER
        || bytes.len() > DYNAMIC_HEADER + MAX_CONFIGURATION_BYTES + 24
        || bytes[40] > 1
        || bytes[41..44] != [0; 3]
        || bytes[88..96] != [0; 8]
    {
        return Err(Error::CorruptState);
    }
    let u32_at = |offset| -> Result<u32, Error> {
        Ok(u32::from_be_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| Error::CorruptState)?,
        ))
    };
    let u64_at = |offset| -> Result<u64, Error> {
        Ok(u64::from_be_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .map_err(|_| Error::CorruptState)?,
        ))
    };
    if u32_at(8)? != local.id || bytes[12..28] != local.directory {
        return Err(Error::MembershipChanged);
    }
    let count = u32_at(84)? as usize;
    let end = DYNAMIC_HEADER
        .checked_add(count)
        .ok_or(Error::CorruptState)?;
    if count > MAX_CONFIGURATION_BYTES || end > bytes.len() {
        return Err(Error::CorruptState);
    }
    let view = Voters::decode(&bytes[DYNAMIC_HEADER..end]).map_err(|_| Error::CorruptState)?;
    let vote_id = u32_at(36)?;
    let vote = if bytes[40] == 1 {
        Some(
            Key::new(
                vote_id,
                bytes[44..60].try_into().map_err(|_| Error::CorruptState)?,
            )
            .map_err(|_| Error::CorruptState)?,
        )
    } else {
        if vote_id != 0 || bytes[44..60] != [0; 16] {
            return Err(Error::CorruptState);
        }
        None
    };
    let state = PersistentState {
        term: u64_at(28)?,
        voted_for: vote.map(|key| key.id),
        log: LogPosition::new(u64_at(60)?, u64_at(68)?)?,
    };
    let source_operation = u64_at(76)?;
    let mut decoded = DynamicDecoded {
        state,
        vote,
        view,
        source_operation,
        reconciliation: None,
        configuration_floor: None,
    };
    match &bytes[..8] {
        magic if magic == DYNAMIC_STATE && bytes.len() == end => {}
        magic if magic == DYNAMIC_RECONCILE && bytes.len() == end + 24 => {
            decoded.reconciliation = Some((
                LogPosition::new(u64_at(end)?, u64_at(end + 8)?)?,
                u64_at(end + 16)?,
            ));
        }
        magic if magic == DYNAMIC_CONFIG && bytes.len() == end + 8 => {
            decoded.configuration_floor = Some(u64_at(end)?);
        }
        _ => return Err(Error::CorruptState),
    }
    Ok(decoded)
}
fn validate_dynamic_transition(
    old: PersistentState,
    next: PersistentState,
    membership: &Membership,
) -> Result<(), Error> {
    let mut checked = membership.clone();
    if next.term == old.term && next.voted_for == old.voted_for {
        if let Some(id) = next.voted_for {
            if checked.position(id).is_err() {
                checked.voters.push(id);
                checked.voters.sort_unstable();
            }
        }
    }
    validate_transition(old, next, &checked)
}
fn validate_dynamic_vote(
    old: PersistentState,
    next: PersistentState,
    old_vote: Option<Key>,
    next_vote: Option<Key>,
    view: &Voters,
) -> Result<(), Error> {
    if next.voted_for != next_vote.map(|key| key.id)
        || (next.term == 0 && next_vote.is_some())
        || (next.term == old.term && old_vote.is_some() && next_vote != old_vote)
    {
        return Err(Error::CorruptState);
    }
    if let Some(key) = next_vote {
        if !(next.term == old.term && old_vote == Some(key)) && !view.contains(key) {
            return Err(Error::CorruptState);
        }
    }
    Ok(())
}

pub(crate) struct DynamicConfig {
    pub local: Key,
    pub genesis: Voters,
    pub timeouts: Timeouts,
    pub max_states: usize,
    pub seed: u64,
}
pub(crate) struct MembershipUpdate<'a> {
    pub expected: &'a Voters,
    pub replacement: Voters,
    pub source_operation: u64,
    pub committed_configuration_index: u64,
    pub keep_removed_leader: bool,
}
impl Election {
    /// Replication-owner-only dynamic open with actual content-WAL validation.
    pub(crate) fn open_dynamic(
        path: impl AsRef<Path>,
        options: DynamicConfig,
        now_ms: u64,
        mut validate_source: impl FnMut(u64, &Voters) -> Result<(), Error>,
    ) -> Result<Self, Error> {
        let DynamicConfig {
            local,
            genesis,
            timeouts,
            max_states,
            seed,
        } = options;
        if !(1..=MAX_STATES).contains(&max_states) {
            return Err(Error::InvalidStateBudget);
        }
        let initial = dynamic_initial(local, &genesis)?;
        let payload = DYNAMIC_HEADER + MAX_CONFIGURATION_BYTES + 24;
        let bytes = (24 + (payload + 32) as u64 * max_states as u64).min(256 * 1024 * 1024);
        let limits = journal::Limits::new(
            payload,
            bytes,
            max_states,
            payload + std::mem::size_of::<journal::Entry>(),
        )?;
        let (mut journal, recovery) = Journal::open(path, 0, limits)?;
        if journal.entry_count() == 0 {
            journal.append(1, &initial)?;
        }
        let mut dynamic = DynamicStore {
            local,
            view: genesis,
            source_operation: 0,
            state: PersistentState::default(),
            vote: None,
        };
        validate_source(0, &dynamic.view)?;
        for offset in 0..journal.next_offset() {
            let entries =
                journal.fetch(offset, 1, payload + std::mem::size_of::<journal::Entry>())?;
            let entry = entries.first().ok_or(Error::CorruptState)?;
            if entry.first_offset != offset || entry.record_count != 1 {
                return Err(Error::CorruptState);
            }
            if offset == 0 {
                if entry.payload != initial {
                    return Err(Error::MembershipChanged);
                }
                continue;
            }
            let next = decode_dynamic(&entry.payload, local)?;
            validate_source(next.source_operation, &next.view)?;
            validate_dynamic_vote(
                dynamic.state,
                next.state,
                dynamic.vote,
                next.vote,
                &next.view,
            )?;
            let membership = Membership::dynamic(local, &next.view)?;
            if let Some(floor) = next.configuration_floor {
                if next.state != dynamic.state
                    || next.vote != dynamic.vote
                    || next.source_operation < dynamic.source_operation
                    || floor > next.state.log.index
                    || next.view.position().index > next.state.log.index
                    || next.view.position().index < floor
                {
                    return Err(Error::CorruptState);
                }
                if next.view.position().index < dynamic.view.position().index {
                    if dynamic.view.position().index <= floor
                        || next.view.epoch() >= dynamic.view.epoch()
                    {
                        return Err(Error::CorruptState);
                    }
                } else if next.view.position().index > dynamic.view.position().index {
                    if next.view.epoch() <= dynamic.view.epoch()
                        || next.view.position().term < dynamic.view.position().term
                    {
                        return Err(Error::CorruptState);
                    }
                } else if next.view != dynamic.view {
                    return Err(Error::CorruptState);
                }
            } else {
                if next.view != dynamic.view || next.source_operation != dynamic.source_operation {
                    return Err(Error::CorruptState);
                }
                if let Some((expected, floor)) = next.reconciliation {
                    let mut checked = membership;
                    if let Some(id) = next.state.voted_for {
                        if checked.position(id).is_err() {
                            checked.voters.push(id);
                            checked.voters.sort_unstable();
                        }
                    }
                    validate_reconciliation(dynamic.state, next.state, expected, floor, &checked)?;
                } else {
                    validate_dynamic_transition(dynamic.state, next.state, &membership)?;
                    if next.view.position().index > next.state.log.index {
                        return Err(Error::CorruptState);
                    }
                }
            }
            dynamic.view = next.view;
            dynamic.source_operation = next.source_operation;
            dynamic.state = next.state;
            dynamic.vote = next.vote;
        }
        let persistent = dynamic.state;
        let membership = Membership::dynamic(local, &dynamic.view)?;
        let store = DurableStore {
            journal,
            membership: membership.clone(),
            dynamic: Some(dynamic),
        };
        let machine = Machine::new(store, membership, persistent, timeouts, seed, now_ms)?;
        Ok(Self { machine, recovery })
    }
    /// Immutable confirmed configuration/source operation and directory-qualified vote.
    pub(crate) fn membership_context(&self) -> Option<(&Voters, u64, Option<Key>)> {
        self.machine
            .store
            .dynamic
            .as_ref()
            .map(|d| (&d.view, d.source_operation, d.vote))
    }
    /// Supply the owner-verified active configuration after a synced content change.
    /// Local current term and the complete already-cast vote identity remain intact.
    pub(crate) fn reconcile_membership(
        &mut self,
        update: MembershipUpdate<'_>,
        now_ms: u64,
    ) -> Result<(), Error> {
        let MembershipUpdate {
            expected,
            replacement,
            source_operation,
            committed_configuration_index,
            keep_removed_leader,
        } = update;
        self.machine.clock(now_ms)?;
        let dynamic = self
            .machine
            .store
            .dynamic
            .as_ref()
            .ok_or(Error::MembershipChanged)?;
        if &dynamic.view != expected
            || source_operation < dynamic.source_operation
            || replacement.position().index > self.machine.persistent.log.index
            || committed_configuration_index > self.machine.persistent.log.index
            || replacement.position().index < committed_configuration_index
        {
            return Err(Error::CorruptState);
        }
        if replacement.position().index < expected.position().index {
            if expected.position().index <= committed_configuration_index
                || replacement.epoch() >= expected.epoch()
            {
                return Err(Error::CorruptState);
            }
        } else if replacement.position().index > expected.position().index {
            if replacement.epoch() <= expected.epoch()
                || replacement.position().term < expected.position().term
            {
                return Err(Error::CorruptState);
            }
        } else if &replacement != expected {
            return Err(Error::CorruptState);
        }
        let local = dynamic.local;
        let vote = dynamic.vote;
        let membership = Membership::dynamic(local, &replacement)?;
        let mut granted = Vec::new();
        granted
            .try_reserve_exact(membership.voters.len())
            .map_err(|_| Error::AllocationFailed)?;
        granted.resize(membership.voters.len(), false);
        let old_leader = self.machine.leader;
        let removed_leader = old_leader.filter(|id| membership.position(*id).is_err());
        let deadline = self.machine.draw_deadline(now_ms)?;
        if &replacement != expected {
            let mut bytes = encode_dynamic(
                DYNAMIC_CONFIG,
                local,
                self.machine.persistent,
                vote,
                &replacement,
                source_operation,
            )?;
            bytes
                .try_reserve_exact(8)
                .map_err(|_| Error::AllocationFailed)?;
            bytes.extend_from_slice(&committed_configuration_index.to_be_bytes());
            if let Err(error) = self.machine.store.journal.append(1, &bytes) {
                self.machine.poisoned = true;
                self.machine.follower(self.machine.deadline_ms);
                self.machine.retained_leader = None;
                return Err(error.into());
            }
            let dynamic = self
                .machine
                .store
                .dynamic
                .as_mut()
                .ok_or(Error::MembershipChanged)?;
            dynamic.view = replacement;
            dynamic.source_operation = source_operation;
        }
        if membership.can_vote
            && self.machine.role == Role::Candidate
            && self.machine.persistent.voted_for == Some(membership.local)
        {
            granted[membership.position(membership.local)?] = true;
        }
        self.machine.membership = membership;
        self.machine.granted = granted;
        self.machine.retained_leader = if keep_removed_leader {
            removed_leader
        } else {
            None
        };
        if (self.machine.role == Role::Candidate && !self.machine.membership.can_vote)
            || (removed_leader.is_some() && !keep_removed_leader)
        {
            self.machine.follower(deadline);
        }
        Ok(())
    }
    /// Admit a directory-fenced candidate without a second same-term vote after re-add.
    pub(crate) fn request_vote_key(
        &mut self,
        candidate: Key,
        request: VoteRequest,
        now_ms: u64,
    ) -> Result<VoteResponse, Error> {
        let dynamic = self
            .machine
            .store
            .dynamic
            .as_ref()
            .ok_or(Error::MembershipChanged)?;
        if candidate.id != request.candidate || !dynamic.view.contains(candidate) {
            return Err(Error::UnknownVoter);
        }
        if request.term == self.machine.persistent.term
            && self.machine.persistent.voted_for == Some(candidate.id)
            && dynamic.vote != Some(candidate)
        {
            self.machine.clock(now_ms)?;
            return Ok(VoteResponse {
                term: self.machine.persistent.term,
                voter: self.machine.membership.local,
                candidate: candidate.id,
                granted: false,
            });
        }
        self.machine.request_vote(request, now_ms)
    }
    /// Count only the current exact directory's correlated response.
    pub(crate) fn receive_vote_key(
        &mut self,
        voter: Key,
        response: VoteResponse,
        now_ms: u64,
    ) -> Result<Tally, Error> {
        let dynamic = self
            .machine
            .store
            .dynamic
            .as_ref()
            .ok_or(Error::MembershipChanged)?;
        if response.voter != voter.id
            || !dynamic.view.contains(voter)
            || !dynamic.view.contains(dynamic.local)
        {
            return Err(Error::UnknownVoter);
        }
        self.machine.receive_vote(response, now_ms)
    }
}
