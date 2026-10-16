//! Fixed trusted-member controller RPCs: Vote52/0, Begin53/0 and End54/0.
//!
//! This is one configured metadata partition, not replication or full KRaft.
//! Directory-aware pre-vote is evaluated locally and is not advertised.
//! Dynamic membership stays unadvertised.
//! A controller listener must be restricted to trusted peers: node IDs and
//! client IDs are not authentication. Its operator-owned election journal path
//! must remain bound to the same cluster/topic and this epoch representation.
//!
//! Kafka epochs map to internal terms through checked `epoch + 1`; empty logs
//! map to `(0,0)`, nonempty logs also shift their epoch. Core term1 (wire epoch0)
//! is synchronized before exposing a controller. Affirmative votes follow fsync.
//! End's preferred successor ranks match Apache for at most31 distinct fixed
//! members; larger lists fail before mutation, excluding Java shift wrapping.
//!
//! The synchronous [`Controller`] belongs on a storage thread. [`ControllerHandler`]
//! supplies a bounded blocking actor for asynchronous transport. Cancellation
//! skips queued work; cancellation during synchronization is ambiguous. Shutdown
//! stops admission and joins the actor; a canceled shutdown can be retried.
//! Work/queues/bytes are bounded, without allocations from peer array counts.

use super::election::{
    self, Election, LogPosition, Membership, Role, State, Tally, Tick, Timeouts,
};
use crate::{
    protocol::{self, ApiVersion, ApiVersionsHandler, RequestHeader},
    transport::{ExchangeOutcome, Handler, Peer},
};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
};

/// Actual controller-listener dispatch; other listener profiles remain separate.
pub const CONTROLLER_API_VERSIONS: [ApiVersion; 4] = [
    ApiVersion {
        api_key: 18,
        min_version: 0,
        max_version: 4,
    },
    ApiVersion {
        api_key: 52,
        min_version: 0,
        max_version: 0,
    },
    ApiVersion {
        api_key: 53,
        min_version: 0,
        max_version: 0,
    },
    ApiVersion {
        api_key: 54,
        min_version: 0,
        max_version: 0,
    },
];
const MAX_TERM: u64 = i32::MAX as u64 + 1;
const INVALID_REQUEST: i16 = 42;
const FENCED_EPOCH: i16 = 74;
const CLUSTER_MISMATCH: i16 = 104;
const UNSUPPORTED_VERSION: i16 = 35;
const MAX_SUCCESSORS: usize = 31;
const MAX_ARRAY: usize = 64;

/// Select a tested controller version from a bounded, consistent peer inventory.
///
/// Unknown keys do not confer support; duplicate/reversed/negative ranges fail
/// closed. At most128 peer ranges are inspected. No higher directory/pre-vote
/// version can be selected even if a current Apache peer advertises it.
pub fn negotiate(peer: &[ApiVersion], api_key: i16) -> Option<i16> {
    if peer.len() > 128
        || peer.iter().enumerate().any(|(i, a)| {
            a.api_key < 0
                || a.min_version < 0
                || a.max_version < a.min_version
                || peer[..i].iter().any(|b| a.api_key == b.api_key)
        })
    {
        return None;
    }
    let local = CONTROLLER_API_VERSIONS
        .iter()
        .find(|a| a.api_key == api_key)?;
    let remote = peer.iter().find(|a| a.api_key == api_key)?;
    let maximum = local.max_version.min(remote.max_version);
    (maximum >= local.min_version.max(remote.min_version)).then_some(maximum)
}

/// Explicit fixed-quorum, parsing, queue and timer budgets.
#[derive(Debug, Clone)]
pub struct Config {
    /// Local nonnegative signed32-bit voter identity.
    pub local_id: u32,
    /// One to64 distinct fixed voters including local.
    pub voters: Vec<u32>,
    /// Stable configured cluster ID; null request IDs follow Apache's acceptance.
    pub cluster_id: String,
    /// Single configured metadata topic, strict UTF8 and at most249 bytes.
    pub topic: String,
    /// Single nonnegative metadata partition.
    pub partition: i32,
    /// Transport-aligned request-byte and per-block tag bounds.
    pub protocol_limits: protocol::Limits,
    /// Pending actor requests,1..=1024; queue bytes must not exceed512MiB.
    pub max_queued_requests: usize,
    /// Maximum ranked End election backoff,1..=600000 milliseconds.
    pub election_backoff_max_ms: u32,
    /// Normal bounded randomized election timeout interval.
    pub election_timeouts: Timeouts,
    /// Maximum durable election snapshots, including initialization.
    pub max_states: usize,
    /// Deterministic election timer seed, not a security primitive.
    pub timer_seed: u64,
}
impl Config {
    /// Defaults for one trusted metadata quorum; all values revalidate on open.
    pub fn new(local_id: u32, voters: Vec<u32>, cluster_id: String) -> Result<Self, Error> {
        let config = Self {
            local_id,
            voters,
            cluster_id,
            topic: "__cluster_metadata".into(),
            partition: 0,
            protocol_limits: protocol::Limits::default(),
            max_queued_requests: 64,
            election_backoff_max_ms: 1000,
            election_timeouts: Timeouts::new(1000, 2000)?,
            max_states: 65_536,
            timer_seed: 7,
        };
        config.validate()?;
        Ok(config)
    }
    pub(crate) fn validate(&self) -> Result<Membership, Error> {
        if self.cluster_id.is_empty()
            || self.cluster_id.len() > 249
            || !(1..=64).contains(&self.voters.len())
            || self.topic.is_empty()
            || self.topic.len() > 249
            || self.partition < 0
            || !(1..=1024).contains(&self.max_queued_requests)
            || self
                .max_queued_requests
                .checked_mul(self.protocol_limits.max_request_bytes())
                .is_none_or(|n| n > 512 * 1024 * 1024)
            || !(1..=600_000).contains(&self.election_backoff_max_ms)
            || !(2..=65_536).contains(&self.max_states)
        {
            return Err(Error::InvalidConfig);
        }
        Membership::new(self.local_id, self.voters.clone()).map_err(Error::Election)
    }
}

/// Explicit parsing, admission, persistence and peer-response failure.
#[derive(Debug)]
pub enum Error {
    /// Local bounds or configured fixed identities are invalid.
    InvalidConfig,
    /// Header/body is malformed or violates its positive bounded schema.
    Protocol(protocol::Error),
    /// Only version0 of the three controller RPCs is implemented; close the
    /// connection rather than inventing a generic response schema.
    UnsupportedVersion,
    /// Durable election failed; no successful response is returned.
    Election(election::Error),
    /// A core epoch/log coordinate cannot be represented on Kafka's wire.
    EpochOverflow,
    /// A peer response does not identify the expected correlation/partition.
    UnexpectedResponse,
    /// A peer returned a semantic error that cannot count as a vote.
    PeerError(i16),
    /// Fixed small request/response allocation failed.
    Allocation,
    /// Bounded actor admission was full.
    Busy,
    /// Actor admission is stopped.
    Stopped,
    /// Actor or its response channel closed unexpectedly.
    Actor,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "controller: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Self::Protocol(e)
    }
}
impl From<election::Error> for Error {
    fn from(e: election::Error) -> Self {
        Self::Election(e)
    }
}

/// Synchronous durable controller; callers supply monotonic elapsed milliseconds.
pub struct Controller {
    config: Config,
    election: Election,
}
impl Controller {
    /// Open a dedicated fixed-profile journal and synchronize initial wire epoch0.
    ///
    /// A newly created journal needs two state slots. Recovered state must fit
    /// the wire domain; caller-confirmed log content/replication remains external.
    pub fn open(path: impl AsRef<Path>, config: Config, now_ms: u64) -> Result<Self, Error> {
        let members = config.validate()?;
        let mut election = Election::open(
            path,
            members,
            config.election_timeouts,
            config.max_states,
            config.timer_seed,
            now_ms,
        )?;
        check_state(election.state())?;
        election.adopt_term(1, now_ms)?;
        Ok(Self { config, election })
    }
    /// Explicit private typed-membership profile. The legacy wire parser cannot
    /// represent directory identities and is unavailable on this owner.
    pub(crate) fn open_dynamic(
        path: impl AsRef<Path>,
        config: Config,
        local: super::membership::Key,
        genesis: super::membership::Voters,
        now_ms: u64,
        validate_source: impl FnMut(u64, &super::membership::Voters) -> Result<(), election::Error>,
    ) -> Result<Self, Error> {
        config.validate()?;
        let mut group = Vec::new();
        group
            .try_reserve_exact(8 + config.cluster_id.len() + config.topic.len())
            .map_err(|_| Error::Allocation)?;
        group.extend_from_slice(&(config.cluster_id.len() as u16).to_be_bytes());
        group.extend_from_slice(config.cluster_id.as_bytes());
        group.extend_from_slice(&(config.topic.len() as u16).to_be_bytes());
        group.extend_from_slice(config.topic.as_bytes());
        group.extend_from_slice(&(config.partition as u32).to_be_bytes());
        let mut election = Election::open_dynamic(
            path,
            election::DynamicConfig {
                local,
                genesis,
                group,
                timeouts: config.election_timeouts,
                max_states: config.max_states,
                seed: config.timer_seed,
            },
            now_ms,
            validate_source,
        )?;
        check_state(election.state())?;
        election.adopt_term(1, now_ms)?;
        Ok(Self { config, election })
    }
    pub(crate) fn membership_context(
        &self,
    ) -> Option<(
        &super::membership::Voters,
        u64,
        Option<super::membership::Key>,
    )> {
        self.election.membership_context()
    }
    pub(crate) fn reconcile_membership(
        &mut self,
        update: election::MembershipUpdate<'_>,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.election.reconcile_membership(update, now_ms)?;
        Ok(())
    }
    pub(crate) fn tick_dynamic(&mut self, now_ms: u64) -> Result<election::Tick, Error> {
        if self.state().persistent.term >= MAX_TERM
            && self.state().role != Role::Leader
            && now_ms >= self.state().deadline_ms
        {
            return Err(Error::EpochOverflow);
        }
        Ok(self.election.tick(now_ms)?)
    }
    pub(crate) fn request_vote_key(
        &mut self,
        key: super::membership::Key,
        request: election::VoteRequest,
        now_ms: u64,
    ) -> Result<election::VoteResponse, Error> {
        Ok(self.election.request_vote_key(key, request, now_ms)?)
    }
    pub(crate) fn receive_vote_key(
        &mut self,
        key: super::membership::Key,
        response: election::VoteResponse,
        now_ms: u64,
    ) -> Result<Tally, Error> {
        Ok(self.election.receive_vote_key(key, response, now_ms)?)
    }
    /// Confirmed durable election and volatile timer diagnostics.
    pub fn state(&self) -> State {
        self.election.state()
    }
    /// Number of synchronized journal snapshots, including initialization.
    pub fn durable_states(&self) -> usize {
        self.election.durable_states()
    }
    /// Synchronize a trusted newer wire epoch without voting or asserting a leader.
    ///
    /// Recovery/replication owners reconcile externally confirmed durable log
    /// epochs through this transition before advancing the log summary. Raw
    /// unvalidated peer bytes must go through the fenced RPC handler instead.
    pub fn adopt_epoch(&mut self, epoch: i32, now_ms: u64) -> Result<bool, Error> {
        Ok(self.election.adopt_term(core_term(epoch)?, now_ms)?)
    }
    /// Replication-owner-only reconciliation after validating its durable WAL.
    pub(crate) fn reconcile_durable_log(
        &mut self,
        expected: LogPosition,
        replacement: LogPosition,
        committed_end: u64,
    ) -> Result<(), Error> {
        self.election
            .reconcile_durable_log(expected, replacement, committed_end)?;
        Ok(())
    }
    /// Replication-owner fencing; a leader result is never a commit lease.
    pub(crate) fn lose_quorum(&mut self, now_ms: u64) -> Result<(), Error> {
        self.election.lose_quorum(now_ms)?;
        Ok(())
    }
    /// Fence a typed trusted replication leader through the same durable epoch/vote core.
    pub(crate) fn observe_replication_leader(
        &mut self,
        leader: u32,
        term: u64,
        now_ms: u64,
    ) -> Result<bool, Error> {
        if !(1..=MAX_TERM).contains(&term) {
            return Err(Error::EpochOverflow);
        }
        Ok(self.election.observe_leader(leader, term, now_ms)?)
    }

    /// Advance the summary of externally synchronized metadata log records.
    ///
    /// Kafka's end offset is the number of records, not a last-record index.
    /// Empty logs require epoch0/end0; nonempty epoch0 is supported through term1.
    /// This does not append/replicate content or confer commit authority.
    pub fn advance_log(&mut self, last_epoch: i32, end_offset: i64) -> Result<(), Error> {
        self.election
            .advance_log(log_position(last_epoch, end_offset)?)?;
        Ok(())
    }
    /// Process one transport-bounded payload, returning its response header/body.
    ///
    /// Unimplemented controller versions and malformed bytes fail closed.
    /// ApiVersions alone uses its pinned special v0/error35 fallback.
    /// Semantic cluster/partition/member/log/epoch failures use pinned
    /// top-level or partition error fields. No state mutates before full parsing
    /// and the supported static-profile checks succeed.
    pub fn respond(&mut self, input: &[u8], now_ms: u64) -> Result<Vec<u8>, Error> {
        self.respond_into(input, now_ms, Vec::new())
    }

    /// Like [`Self::respond`], writing the body into `spare` when it fits.
    fn respond_into(
        &mut self,
        input: &[u8],
        now_ms: u64,
        spare: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        if self.election.membership_context().is_some() {
            return Err(Error::UnsupportedVersion);
        }
        let key = i16::from_be_bytes(
            input
                .get(..2)
                .ok_or(protocol::Error::Truncated)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        );
        if key == 18 {
            return Ok(ApiVersionsHandler::with_advertised(
                self.config.protocol_limits,
                &CONTROLLER_API_VERSIONS,
            )
            .respond(input)?);
        }
        if !matches!(key, 52..=54) {
            return Err(protocol::Error::UnimplementedApi(key).into());
        }
        let version = i16::from_be_bytes(
            input
                .get(2..4)
                .ok_or(protocol::Error::Truncated)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        );
        let header_version = if key == 52 || version >= 1 { 2 } else { 1 };
        let (header, body) =
            RequestHeader::parse(input, header_version, self.config.protocol_limits)?;
        if version != 0 {
            return Err(Error::UnsupportedVersion);
        }
        let request = parse_request(key, body, &self.config)?;
        if request.cluster.is_some_and(|c| c != self.config.cluster_id) {
            return response_into(
                key,
                header.correlation_id,
                CLUSTER_MISMATCH,
                None,
                &self.config,
                self.state(),
                spare,
            );
        }
        if !request.partition_matches {
            return response_into(
                key,
                header.correlation_id,
                INVALID_REQUEST,
                None,
                &self.config,
                self.state(),
                spare,
            );
        }
        let error = self.apply(key, &request, now_ms)?;
        response_into(
            key,
            header.correlation_id,
            0,
            Some(error),
            &self.config,
            self.state(),
            spare,
        )
    }
    fn apply(&mut self, key: i16, r: &Request<'_>, now: u64) -> Result<(i16, bool), Error> {
        let current = wire_epoch(self.state().persistent.term)?;
        // Apache validates the log coordinates before stale-epoch fencing.
        if key == 52
            && (r.last_epoch < 0
                || r.last_offset < 0
                || r.last_epoch >= r.epoch
                || (r.last_offset == 0 && r.last_epoch != 0))
        {
            return Ok((INVALID_REQUEST, false));
        }
        if r.epoch < current {
            return Ok((FENCED_EPOCH, false));
        }
        if r.epoch < 0
            || r.node < 0
            || !self
                .config
                .voters
                .contains(&u32::try_from(r.node).map_err(|_| Error::UnexpectedResponse)?)
        {
            return Ok((INVALID_REQUEST, false));
        }
        let node = u32::try_from(r.node).map_err(|_| Error::UnexpectedResponse)?;
        let term = core_term(r.epoch)?;
        if key == 52 {
            let reply = self.election.request_vote(
                election::VoteRequest {
                    term,
                    candidate: node,
                    log: log_position(r.last_epoch, r.last_offset)?,
                },
                now,
            )?;
            return Ok((0, reply.granted));
        }
        if key == 54 {
            if node == self.config.local_id {
                return Ok((INVALID_REQUEST, false));
            }
            for (i, id) in r.successors[..r.successor_count].iter().enumerate() {
                if *id < 0
                    || !self
                        .config
                        .voters
                        .contains(&u32::try_from(*id).map_err(|_| Error::UnexpectedResponse)?)
                    || r.successors[..i].contains(id)
                {
                    return Ok((INVALID_REQUEST, false));
                }
            }
        }
        match self.election.observe_leader(node, term, now) {
            Ok(true) => {}
            Ok(false) => return Ok((FENCED_EPOCH, false)),
            Err(election::Error::ConflictingLeader | election::Error::NotElected) => {
                return Ok((INVALID_REQUEST, false))
            }
            Err(e) => return Err(e.into()),
        }
        if key == 54 {
            let local = i32::try_from(self.config.local_id).map_err(|_| Error::InvalidConfig)?;
            let rank = r.successors[..r.successor_count]
                .iter()
                .position(|id| *id == local);
            let backoff =
                successor_backoff(self.config.election_backoff_max_ms, r.successor_count, rank)?;
            self.election.end_epoch_backoff(u64::from(backoff), now)?;
        }
        Ok((0, false))
    }
    /// Trigger a due synchronized candidacy and encode its Vote0 request.
    ///
    /// The caller sends this payload only to trusted configured peers and tracks
    /// correlation IDs. Crossing signed32-bit epoch range fails before mutation.
    pub fn tick(&mut self, now_ms: u64, correlation_id: i32) -> Result<Option<Vec<u8>>, Error> {
        if self.election.membership_context().is_some() {
            return Err(Error::UnsupportedVersion);
        }
        if self.state().persistent.term >= MAX_TERM
            && self.state().role != Role::Leader
            && now_ms >= self.state().deadline_ms
        {
            return Err(Error::EpochOverflow);
        }
        match self.election.tick(now_ms)? {
            Tick::Idle => Ok(None),
            Tick::Campaign(vote) => Ok(Some(vote_request(&self.config, vote, correlation_id)?)),
        }
    }
    /// Validate a correlated Vote0 peer reply before counting distinct grants.
    ///
    /// Error74 can carry a newer fenced epoch; other semantic errors do not
    /// count. Known leaders are validated against fixed membership and the
    /// existing same-term leader fence before relinquishing candidacy.
    pub fn receive_vote(
        &mut self,
        peer_id: u32,
        correlation_id: i32,
        input: &[u8],
        now_ms: u64,
    ) -> Result<Tally, Error> {
        if self.election.membership_context().is_some() {
            return Err(Error::UnsupportedVersion);
        }
        if !self.config.voters.contains(&peer_id)
            || peer_id == self.config.local_id
            || input.len() > self.config.protocol_limits.max_request_bytes()
        {
            return Err(Error::UnexpectedResponse);
        }
        let mut reader = Reader::new(input, true, self.config.protocol_limits);
        if reader.i32()? != correlation_id {
            return Err(Error::UnexpectedResponse);
        }
        reader.tags()?;
        let top = reader.i16()?;
        if top != 0 {
            return Err(Error::PeerError(top));
        }
        if reader.count()? != 1
            || reader.string()? != self.config.topic
            || reader.count()? != 1
            || reader.i32()? != self.config.partition
        {
            return Err(Error::UnexpectedResponse);
        }
        let error = reader.i16()?;
        let leader = reader.i32()?;
        let epoch = reader.i32()?;
        let granted = match reader.byte()? {
            0 => false,
            1 => true,
            _ => return Err(Error::UnexpectedResponse),
        };
        reader.tags()?;
        reader.tags()?;
        reader.tags()?;
        reader.finish()?;
        if !matches!(error, 0 | FENCED_EPOCH) || (error != 0 && granted) {
            return Err(Error::PeerError(error));
        }
        let term = core_term(epoch)?;
        if leader != -1
            && (leader < 0
                || !self
                    .config
                    .voters
                    .contains(&u32::try_from(leader).map_err(|_| Error::UnexpectedResponse)?))
        {
            return Err(Error::UnexpectedResponse);
        }
        if granted
            && leader >= 0
            && u32::try_from(leader).map_err(|_| Error::UnexpectedResponse)? != self.config.local_id
        {
            return Err(Error::UnexpectedResponse);
        }
        let previous_term = self.state().persistent.term;
        if leader >= 0 && term >= self.state().persistent.term {
            let node = u32::try_from(leader).map_err(|_| Error::UnexpectedResponse)?;
            self.election.observe_leader(node, term, now_ms)?;
        }
        let tally = self.election.receive_vote(
            election::VoteResponse {
                term,
                voter: peer_id,
                candidate: self.config.local_id,
                granted,
            },
            now_ms,
        )?;
        Ok(if leader >= 0 && term > previous_term {
            Tally::SteppedDown
        } else {
            tally
        })
    }
}

fn check_state(state: State) -> Result<(), Error> {
    if state.persistent.term > MAX_TERM
        || state.persistent.log.term > MAX_TERM
        || state.persistent.log.index > i64::MAX as u64
    {
        return Err(Error::EpochOverflow);
    }
    Ok(())
}
fn core_term(epoch: i32) -> Result<u64, Error> {
    u64::try_from(epoch)
        .map_err(|_| Error::EpochOverflow)?
        .checked_add(1)
        .ok_or(Error::EpochOverflow)
}
fn wire_epoch(term: u64) -> Result<i32, Error> {
    i32::try_from(term.checked_sub(1).ok_or(Error::EpochOverflow)?)
        .map_err(|_| Error::EpochOverflow)
}
fn log_position(epoch: i32, offset: i64) -> Result<LogPosition, Error> {
    if offset == 0 && epoch == 0 {
        return Ok(LogPosition::default());
    }
    if offset <= 0 {
        return Err(Error::EpochOverflow);
    }
    Ok(LogPosition::new(
        core_term(epoch)?,
        u64::try_from(offset).map_err(|_| Error::EpochOverflow)?,
    )?)
}

/// Exact Apache ranked backoff for the supported zero-to31-successor profile.
///
/// Rank0 and an empty list are immediate; an absent local voter in a nonempty
/// list gets the configured maximum, matching Apache's position0 special case.
/// Distinct/member validation belongs to the controller before state mutation.
pub fn successor_backoff(maximum_ms: u32, count: usize, rank: Option<usize>) -> Result<u32, Error> {
    if count > MAX_SUCCESSORS
        || !(1..=600_000).contains(&maximum_ms)
        || rank.is_some_and(|r| r >= count)
    {
        return Err(Error::InvalidConfig);
    }
    if count == 0 {
        return Ok(0);
    }
    match rank {
        Some(0) => Ok(0),
        None => Ok(maximum_ms),
        Some(r) => {
            let base = maximum_ms >> (count - 1);
            Ok((base << (r - 1)).min(maximum_ms))
        }
    }
}

/// One EndQuorum preferred successor.
///
/// `directory` is absent on version 0 and required to match on version 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreferredCandidate {
    /// Candidate replica id.
    pub id: i32,
    /// Candidate directory. `None` is the version 0 id-only entry.
    pub directory: Option<[u8; 16]>,
}

/// Reject an EndQuorum successor list the classic handler would not apply.
///
/// The resigning leader cannot be the local voter. A negative or repeated
/// successor id is illegal. No rank is returned with the refusal.
fn successor_list_refusal(
    leader_id: i32,
    local_id: i32,
    preferred: &[PreferredCandidate],
) -> Option<PreVoteRefusal> {
    if leader_id == local_id {
        return Some(PreVoteRefusal::IllegalEpoch);
    }
    for (index, candidate) in preferred.iter().enumerate() {
        if candidate.id < 0
            || preferred
                .iter()
                .take(index)
                .any(|earlier| earlier.id == candidate.id)
        {
            return Some(PreVoteRefusal::IllegalEpoch);
        }
    }
    None
}

/// Rank of the local voter in a preferred-candidate list.
///
/// A version 1 entry whose directory does not match is not this voter, even
/// when the id matches. The result is an input to [`successor_backoff`]; it
/// does not persist a vote or epoch.
pub fn preferred_rank(
    candidates: &[PreferredCandidate],
    local_id: i32,
    local_directory: [u8; 16],
) -> Result<Option<usize>, Error> {
    if candidates.len() > MAX_SUCCESSORS || local_directory == [0; 16] {
        return Err(Error::InvalidConfig);
    }
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.id != local_id {
            continue;
        }
        if let Some(directory) = candidate.directory {
            if directory != local_directory {
                return Ok(None);
            }
        }
        return Ok(Some(index));
    }
    Ok(None)
}

struct Request<'a> {
    cluster: Option<&'a str>,
    partition_matches: bool,
    node: i32,
    epoch: i32,
    last_epoch: i32,
    last_offset: i64,
    successors: [i32; MAX_SUCCESSORS],
    successor_count: usize,
}
fn parse_request<'a>(key: i16, body: &'a [u8], config: &Config) -> Result<Request<'a>, Error> {
    let flexible = key == 52;
    let mut reader = Reader::new(body, flexible, config.protocol_limits);
    let mut result = Request {
        cluster: reader.nullable_string()?,
        partition_matches: true,
        node: -1,
        epoch: -1,
        last_epoch: 0,
        last_offset: 0,
        successors: [0; MAX_SUCCESSORS],
        successor_count: 0,
    };
    let topics = reader.count()?;
    result.partition_matches &= topics == 1;
    for _ in 0..topics {
        result.partition_matches &= reader.string()? == config.topic;
        let partitions = reader.count()?;
        result.partition_matches &= partitions == 1;
        for _ in 0..partitions {
            result.partition_matches &= reader.i32()? == config.partition;
            if key == 52 {
                result.epoch = reader.i32()?;
                result.node = reader.i32()?;
                result.last_epoch = reader.i32()?;
                result.last_offset = reader.i64()?;
            } else {
                result.node = reader.i32()?;
                result.epoch = reader.i32()?;
            }
            if key == 54 {
                result.successor_count = reader.count()?;
                if result.successor_count > MAX_SUCCESSORS {
                    return Err(protocol::Error::InvalidLength.into());
                }
                for id in &mut result.successors[..result.successor_count] {
                    *id = reader.i32()?;
                }
            }
            if flexible {
                reader.tags()?;
            }
        }
        if flexible {
            reader.tags()?;
        }
    }
    if flexible {
        reader.tags()?;
    }
    reader.finish()?;
    Ok(result)
}

struct Reader<'a> {
    input: &'a [u8],
    flexible: bool,
    limits: protocol::Limits,
}
impl<'a> Reader<'a> {
    fn new(input: &'a [u8], flexible: bool, limits: protocol::Limits) -> Self {
        Self {
            input,
            flexible,
            limits,
        }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let bytes = self.input.get(..n).ok_or(protocol::Error::Truncated)?;
        self.input = &self.input[n..];
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16, Error> {
        Ok(i16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn varint(&mut self) -> Result<u32, Error> {
        let mut value = 0;
        for shift in (0..35).step_by(7) {
            let b = self.byte()?;
            if shift == 28 && b > 15 {
                return Err(protocol::Error::InvalidVarint.into());
            }
            value |= u32::from(b & 127) << shift;
            if b & 128 == 0 {
                return Ok(value);
            }
        }
        Err(protocol::Error::InvalidVarint.into())
    }
    fn length(&mut self) -> Result<i64, Error> {
        if self.flexible {
            Ok(i64::from(self.varint()?) - 1)
        } else {
            Ok(i64::from(self.i16()?))
        }
    }
    fn text(&mut self, n: i64) -> Result<&'a str, Error> {
        let n = usize::try_from(n).map_err(|_| protocol::Error::InvalidLength)?;
        if n > 32767 {
            return Err(protocol::Error::InvalidLength.into());
        }
        Ok(std::str::from_utf8(self.take(n)?).map_err(|_| protocol::Error::InvalidUtf8)?)
    }
    fn string(&mut self) -> Result<&'a str, Error> {
        let n = self.length()?;
        self.text(n)
    }
    fn nullable_string(&mut self) -> Result<Option<&'a str>, Error> {
        let n = self.length()?;
        if n == -1 {
            Ok(None)
        } else {
            Ok(Some(self.text(n)?))
        }
    }
    fn count(&mut self) -> Result<usize, Error> {
        let n = if self.flexible {
            i64::from(self.varint()?) - 1
        } else {
            i64::from(self.i32()?)
        };
        let n = usize::try_from(n).map_err(|_| protocol::Error::InvalidLength)?;
        if n > MAX_ARRAY {
            return Err(protocol::Error::InvalidLength.into());
        }
        Ok(n)
    }
    fn tags(&mut self) -> Result<(), Error> {
        let count = usize::try_from(self.varint()?).map_err(|_| protocol::Error::TooManyTags)?;
        if count > self.limits.max_tagged_fields() {
            return Err(protocol::Error::TooManyTags.into());
        }
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varint()?;
            if previous.is_some_and(|p| p >= tag) {
                return Err(protocol::Error::InvalidTagOrder.into());
            }
            previous = Some(tag);
            let n = usize::try_from(self.varint()?).map_err(|_| protocol::Error::InvalidLength)?;
            self.take(n)?;
        }
        Ok(())
    }
    fn finish(self) -> Result<(), Error> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(protocol::Error::TrailingBytes.into())
        }
    }
}

fn allocate(n: usize) -> Result<Vec<u8>, Error> {
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| Error::Allocation)?;
    Ok(v)
}
fn varint(out: &mut Vec<u8>, mut value: u32) {
    while value > 127 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
fn string(out: &mut Vec<u8>, value: &str, flexible: bool) -> Result<(), Error> {
    if flexible {
        varint(
            out,
            u32::try_from(value.len()).map_err(|_| Error::InvalidConfig)? + 1,
        );
    } else {
        out.extend_from_slice(
            &i16::try_from(value.len())
                .map_err(|_| Error::InvalidConfig)?
                .to_be_bytes(),
        );
    }
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
#[cfg(test)]
fn response(
    key: i16,
    correlation: i32,
    top: i16,
    partition_error: Option<(i16, bool)>,
    config: &Config,
    state: State,
) -> Result<Vec<u8>, Error> {
    response_into(
        key,
        correlation,
        top,
        partition_error,
        config,
        state,
        Vec::new(),
    )
}

fn response_into(
    key: i16,
    correlation: i32,
    top: i16,
    partition_error: Option<(i16, bool)>,
    config: &Config,
    state: State,
    mut out: Vec<u8>,
) -> Result<Vec<u8>, Error> {
    let flexible = key == 52;
    out.clear();
    let need = config.topic.len().saturating_add(64);
    let extra = need.saturating_sub(out.capacity());
    if extra > 0 {
        out.try_reserve_exact(extra)
            .map_err(|_| Error::Allocation)?;
    }
    out.extend_from_slice(&correlation.to_be_bytes());
    if flexible {
        out.push(0);
    }
    out.extend_from_slice(&top.to_be_bytes());
    let count = u8::from(partition_error.is_some());
    if flexible {
        varint(&mut out, u32::from(count) + 1);
    } else {
        out.extend_from_slice(&i32::from(count).to_be_bytes());
    }
    if let Some((error, granted)) = partition_error {
        string(&mut out, &config.topic, flexible)?;
        if flexible {
            out.push(2);
        } else {
            out.extend_from_slice(&1_i32.to_be_bytes());
        }
        out.extend_from_slice(&config.partition.to_be_bytes());
        out.extend_from_slice(&error.to_be_bytes());
        let leader = state
            .leader
            .map(i32::try_from)
            .transpose()
            .map_err(|_| Error::EpochOverflow)?
            .unwrap_or(-1);
        out.extend_from_slice(&leader.to_be_bytes());
        out.extend_from_slice(&wire_epoch(state.persistent.term)?.to_be_bytes());
        if flexible {
            out.push(u8::from(granted));
            out.extend_from_slice(&[0, 0]);
        }
    }
    if flexible {
        out.push(0);
    }
    Ok(out)
}
fn vote_request(
    config: &Config,
    vote: election::VoteRequest,
    correlation: i32,
) -> Result<Vec<u8>, Error> {
    let mut out = allocate(config.cluster_id.len() + config.topic.len() + 64)?;
    out.extend_from_slice(&52_i16.to_be_bytes());
    out.extend_from_slice(&0_i16.to_be_bytes());
    out.extend_from_slice(&correlation.to_be_bytes());
    out.extend_from_slice(&(-1_i16).to_be_bytes());
    out.push(0);
    string(&mut out, &config.cluster_id, true)?;
    out.push(2);
    string(&mut out, &config.topic, true)?;
    out.push(2);
    out.extend_from_slice(&config.partition.to_be_bytes());
    out.extend_from_slice(&wire_epoch(vote.term)?.to_be_bytes());
    out.extend_from_slice(
        &i32::try_from(vote.candidate)
            .map_err(|_| Error::EpochOverflow)?
            .to_be_bytes(),
    );
    let log_epoch = if vote.log.index == 0 {
        0
    } else {
        wire_epoch(vote.log.term)?
    };
    out.extend_from_slice(&log_epoch.to_be_bytes());
    out.extend_from_slice(
        &i64::try_from(vote.log.index)
            .map_err(|_| Error::EpochOverflow)?
            .to_be_bytes(),
    );
    out.extend_from_slice(&[0, 0, 0]);
    Ok(out)
}

/// One Vote partition body for versions 0, 1, or 2.
///
/// Version 1 adds directory ids. Version 2 adds `pre_vote`. Versions above 0
/// are not advertised in [`CONTROLLER_API_VERSIONS`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteBody {
    /// Vote API version, 0, 1, or 2.
    pub version: i16,
    /// Cluster id. `None` is the nullable default.
    pub cluster_id: Option<String>,
    /// Receiving voter. Version 0 stores `-1`.
    pub voter_id: i32,
    /// Metadata topic.
    pub topic: String,
    /// Metadata partition.
    pub partition: i32,
    /// Candidate epoch.
    pub replica_epoch: i32,
    /// Candidate replica id.
    pub replica_id: i32,
    /// Candidate directory. Required on versions 1 and 2.
    pub replica_directory: Option<[u8; 16]>,
    /// Receiving voter directory. Required on versions 1 and 2.
    pub voter_directory: Option<[u8; 16]>,
    /// Epoch of the candidate's last metadata record.
    pub last_offset_epoch: i32,
    /// Candidate log end offset.
    pub last_offset: i64,
    /// Version 2 pre-vote. True means the decision must not be persisted.
    pub pre_vote: bool,
}

/// What a decoded Vote body is allowed to do to stable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteDisposition {
    /// Version 0 durable vote. The existing handler owns persistence.
    DurableV0,
    /// Pre-vote granted. The caller must not write a term or vote.
    PreVoteGranted,
    /// A prospective vote was rejected as an unsupported version.
    ///
    /// Apache starts a standard election immediately. This disposition does
    /// not persist a term or a vote, and it is not a grant.
    FallBackToStandardVote,
    /// Refused. The caller must not write a term or vote.
    Refused(PreVoteRefusal),
    /// Directory-qualified durable vote. Parsed, not advertised, not persisted here.
    NotAdvertised,
}

/// Encode one flexible Vote body for `version` 0, 1, or 2.
pub fn encode_vote_body(body: &VoteBody) -> Result<Vec<u8>, Error> {
    if !(0..=2).contains(&body.version) {
        return Err(Error::InvalidConfig);
    }
    if body.version == 0
        && (body.replica_directory.is_some() || body.pre_vote || body.voter_id != -1)
    {
        return Err(Error::InvalidConfig);
    }
    if body.version >= 1 && (body.replica_directory.is_none() || body.voter_directory.is_none()) {
        return Err(Error::InvalidConfig);
    }
    if body.version < 2 && body.pre_vote {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(body.topic.len() + 96)?;
    match &body.cluster_id {
        Some(cluster) => string(&mut out, cluster, true)?,
        None => out.push(0),
    }
    if body.version >= 1 {
        out.extend_from_slice(&body.voter_id.to_be_bytes());
    }
    out.push(2);
    string(&mut out, &body.topic, true)?;
    out.push(2);
    out.extend_from_slice(&body.partition.to_be_bytes());
    out.extend_from_slice(&body.replica_epoch.to_be_bytes());
    out.extend_from_slice(&body.replica_id.to_be_bytes());
    if body.version >= 1 {
        out.extend_from_slice(
            body.replica_directory
                .ok_or(Error::InvalidConfig)?
                .as_slice(),
        );
        out.extend_from_slice(body.voter_directory.ok_or(Error::InvalidConfig)?.as_slice());
    }
    out.extend_from_slice(&body.last_offset_epoch.to_be_bytes());
    out.extend_from_slice(&body.last_offset.to_be_bytes());
    if body.version >= 2 {
        out.push(u8::from(body.pre_vote));
    }
    out.extend_from_slice(&[0, 0, 0]);
    Ok(out)
}

/// Decode one flexible Vote body encoded by [`encode_vote_body`].
pub fn decode_vote_body(bytes: &[u8], version: i16) -> Result<VoteBody, Error> {
    if !(0..=2).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let cluster_id = reader.nullable_string()?.map(str::to_owned);
    let voter_id = if version >= 1 { reader.i32()? } else { -1 };
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let partition = reader.i32()?;
    let replica_epoch = reader.i32()?;
    let replica_id = reader.i32()?;
    let (replica_directory, voter_directory) = if version >= 1 {
        (
            Some(
                reader
                    .take(16)?
                    .try_into()
                    .map_err(|_| Error::InvalidConfig)?,
            ),
            Some(
                reader
                    .take(16)?
                    .try_into()
                    .map_err(|_| Error::InvalidConfig)?,
            ),
        )
    } else {
        (None, None)
    };
    let last_offset_epoch = reader.i32()?;
    let last_offset = reader.i64()?;
    let pre_vote = if version >= 2 {
        reader.byte()? == 1
    } else {
        false
    };
    reader.tags()?;
    reader.tags()?;
    reader.tags()?;
    reader.finish()?;
    Ok(VoteBody {
        version,
        cluster_id,
        voter_id,
        topic,
        partition,
        replica_epoch,
        replica_id,
        replica_directory,
        voter_directory,
        last_offset_epoch,
        last_offset,
        pre_vote,
    })
}

/// The one metadata log a KRaft request is allowed to name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataLog<'a> {
    /// Stable cluster id. A request may omit its cluster id.
    pub cluster_id: &'a str,
    /// Metadata topic.
    pub topic: &'a str,
    /// Metadata partition.
    pub partition: i32,
}

/// Volatile role used when deciding an unadvertised vote.
///
/// A follower with no leader is unattached. Naming a leader means this
/// replica is following one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteQuorum {
    /// Current election role.
    pub role: election::Role,
    /// Known leader for the local term.
    pub leader: Option<election::NodeId>,
}

impl VoteQuorum {
    /// Follower that has not named a leader.
    pub const fn unattached() -> Self {
        Self {
            role: election::Role::Follower,
            leader: None,
        }
    }
}

/// Apply a decoded Vote body without persisting a pre-vote.
///
/// `expected_replica` and `expected_voter` are the directory ids that must
/// match on versions 1 and 2. The returned disposition never asks the caller
/// to write `persistent`.
pub fn decide_vote_body(
    persistent: &election::PersistentState,
    quorum: VoteQuorum,
    metadata: MetadataLog<'_>,
    expected_replica: [u8; 16],
    local_voter_id: i32,
    expected_voter: [u8; 16],
    body: &VoteBody,
) -> VoteDisposition {
    if body.version == 0 {
        return VoteDisposition::DurableV0;
    }
    if let Some(refusal) = metadata_refusal(
        metadata,
        body.cluster_id.as_deref(),
        &body.topic,
        body.partition,
    ) {
        return VoteDisposition::Refused(refusal);
    }
    let Some(replica_directory) = body.replica_directory else {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    };
    if replica_directory != expected_replica {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    }
    // Apache `KafkaRaftClient.handleVoteRequest` returns INVALID_REQUEST
    // before any quorum transition: a negative end, a standard vote whose
    // last offset epoch is not strictly older than the candidate epoch, or a
    // pre-vote whose last offset epoch is newer than the candidate epoch.
    if vote_offset_epoch_illegal(body) {
        return VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch);
    }
    // Apache `validateVoterOnlyRequest` fences an older or negative candidate
    // epoch and rejects a negative replica id before any grant. A higher epoch
    // would become unattached in Apache; this path does not persist that bump.
    if let Some(refusal) = voter_request_refusal(persistent, body) {
        return VoteDisposition::Refused(refusal);
    }
    // Apache checks the receiving replica after the candidate epoch. A
    // negative voter id is untargeted. A different id, or a non-zero
    // directory that is not this replica, is `INVALID_VOTER_KEY`.
    if let Some(refusal) = addressed_voter_refusal(
        local_voter_id,
        expected_voter,
        body.voter_id,
        body.voter_directory,
    ) {
        return VoteDisposition::Refused(refusal);
    }
    if let Some(refusal) = established_quorum_refusal(quorum, persistent, body) {
        return VoteDisposition::Refused(refusal);
    }
    if !body.pre_vote {
        // A grant would persist `voted_for`. Report it as unadvertised and
        // refuse the cases Apache would answer with `voteGranted = false`.
        if let Some(refusal) = standard_vote_grant_refusal(persistent, body) {
            return VoteDisposition::Refused(refusal);
        }
        return VoteDisposition::NotAdvertised;
    }
    let log = if body.last_offset == 0 {
        election::LogPosition { term: 0, index: 0 }
    } else {
        election::LogPosition {
            term: u64::try_from(body.last_offset_epoch.max(0))
                .unwrap_or(0)
                .saturating_add(1),
            index: u64::try_from(body.last_offset).unwrap_or(0),
        }
    };
    match evaluate_prevote(
        persistent,
        expected_replica,
        DirectoryPreVote {
            term: u64::try_from(body.replica_epoch.max(0))
                .unwrap_or(0)
                .saturating_add(1),
            candidate: u32::try_from(body.replica_id).unwrap_or(0),
            directory: replica_directory,
            log,
        },
    ) {
        Ok(true) => VoteDisposition::PreVoteGranted,
        Err(error) => VoteDisposition::Refused(error),
        Ok(false) => VoteDisposition::Refused(PreVoteRefusal::StaleLog),
    }
}

/// One leader address from VoteResponse versions 1 and 2.
///
/// The endpoint is tagged field 0. Those versions are not advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteNodeEndpoint {
    /// Broker id named by this endpoint.
    pub node_id: i32,
    /// Hostname. Empty is not a usable leader address.
    pub host: String,
    /// Port.
    pub port: u16,
}

/// One VoteResponse body for versions 0, 1, or 2.
///
/// Version 0 matches the existing controller response. Versions 1 and 2 add
/// leader endpoints and are not advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponseBody {
    /// Vote response version, 0, 1, or 2.
    pub version: i16,
    /// Top-level error code.
    pub error_code: i16,
    /// Metadata topic.
    pub topic: String,
    /// Metadata partition.
    pub partition: i32,
    /// Partition error code.
    pub partition_error: i16,
    /// Current leader, or `-1` when unknown.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Whether the vote was granted.
    pub vote_granted: bool,
    /// Leader endpoints. Empty on version 0.
    pub endpoints: Vec<VoteNodeEndpoint>,
}

/// Encode a flexible VoteResponse body for version 0, 1, or 2.
pub fn encode_vote_response(body: &VoteResponseBody) -> Result<Vec<u8>, Error> {
    if !(0..=2).contains(&body.version) || body.endpoints.len() > MAX_ARRAY {
        return Err(Error::InvalidConfig);
    }
    if body.version == 0 && !body.endpoints.is_empty() {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(body.topic.len() + 64)?;
    out.extend_from_slice(&body.error_code.to_be_bytes());
    write_count(&mut out, 1, true)?;
    string(&mut out, &body.topic, true)?;
    write_count(&mut out, 1, true)?;
    out.extend_from_slice(&body.partition.to_be_bytes());
    out.extend_from_slice(&body.partition_error.to_be_bytes());
    out.extend_from_slice(&body.leader_id.to_be_bytes());
    out.extend_from_slice(&body.leader_epoch.to_be_bytes());
    out.push(u8::from(body.vote_granted));
    out.extend_from_slice(&[0, 0]);
    if body.endpoints.is_empty() {
        out.push(0);
        return Ok(out);
    }
    let mut payload = Vec::new();
    write_count(
        &mut payload,
        u32::try_from(body.endpoints.len()).map_err(|_| Error::InvalidConfig)?,
        true,
    )?;
    for endpoint in &body.endpoints {
        payload.extend_from_slice(&endpoint.node_id.to_be_bytes());
        string(&mut payload, &endpoint.host, true)?;
        payload.extend_from_slice(&endpoint.port.to_be_bytes());
        payload.push(0);
    }
    varint(&mut out, 1);
    varint(&mut out, 0);
    varint(
        &mut out,
        u32::try_from(payload.len()).map_err(|_| Error::InvalidConfig)?,
    );
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode a body produced by [`encode_vote_response`].
pub fn decode_vote_response(bytes: &[u8], version: i16) -> Result<VoteResponseBody, Error> {
    if !(0..=2).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let error_code = reader.i16()?;
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let partition = reader.i32()?;
    let partition_error = reader.i16()?;
    let leader_id = reader.i32()?;
    let leader_epoch = reader.i32()?;
    let granted = reader.byte()?;
    if granted > 1 {
        return Err(Error::InvalidConfig);
    }
    reader.tags()?;
    reader.tags()?;
    let endpoints = if version == 0 {
        reader.tags()?;
        Vec::new()
    } else {
        read_vote_endpoints(&mut reader)?
    };
    reader.finish()?;
    Ok(VoteResponseBody {
        version,
        error_code,
        topic,
        partition,
        partition_error,
        leader_id,
        leader_epoch,
        vote_granted: granted == 1,
        endpoints,
    })
}

/// Accept a VoteResponse without changing `persistent`.
///
/// Version 0 stays on the existing handler. A version 1 or 2 body is not
/// applied: a single non-empty endpoint naming `leader_id` is only
/// `NotAdvertised`, and any other endpoint list is refused.
pub fn decide_vote_response(
    persistent: &election::PersistentState,
    prospective: bool,
    body: &VoteResponseBody,
) -> VoteDisposition {
    let _ = persistent;
    if prospective && body.error_code == UNSUPPORTED_VERSION {
        return VoteDisposition::FallBackToStandardVote;
    }
    if body.version == 0 {
        return if body.endpoints.is_empty() {
            VoteDisposition::DurableV0
        } else {
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        };
    }
    if !(1..=2).contains(&body.version) {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    }
    if body.leader_id < 0 {
        return if body.endpoints.is_empty() {
            VoteDisposition::NotAdvertised
        } else {
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        };
    }
    match body.endpoints.as_slice() {
        [endpoint] if endpoint.node_id == body.leader_id && !endpoint.host.is_empty() => {
            VoteDisposition::NotAdvertised
        }
        _ => VoteDisposition::Refused(PreVoteRefusal::StaleDirectory),
    }
}

fn read_vote_endpoints(reader: &mut Reader<'_>) -> Result<Vec<VoteNodeEndpoint>, Error> {
    let count = reader.varint()?;
    if count == 0 {
        return Ok(Vec::new());
    }
    if count != 1 || reader.varint()? != 0 {
        return Err(Error::InvalidConfig);
    }
    let length = usize::try_from(reader.varint()?).map_err(|_| Error::InvalidConfig)?;
    let payload = reader.take(length)?;
    let mut nested = Reader::new(payload, true, protocol::Limits::default());
    let n = nested.count()?;
    let mut endpoints = Vec::new();
    endpoints
        .try_reserve_exact(n)
        .map_err(|_| Error::Allocation)?;
    for _ in 0..n {
        let node_id = nested.i32()?;
        let host = nested.string()?.to_owned();
        let port = u16::from_be_bytes(
            nested
                .take(2)?
                .try_into()
                .map_err(|_| Error::InvalidConfig)?,
        );
        nested.tags()?;
        endpoints.push(VoteNodeEndpoint {
            node_id,
            host,
            port,
        });
    }
    nested.finish()?;
    Ok(endpoints)
}

/// BeginQuorumEpoch and EndQuorumEpoch share this response body.
///
/// Version 0 is classic and matches the controller handler. Version 1 is
/// flexible, carries leader endpoints, and is not advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochResponseBody {
    /// Response version, 0 or 1.
    pub version: i16,
    /// Top-level error code.
    pub error_code: i16,
    /// Metadata topic.
    pub topic: String,
    /// Metadata partition.
    pub partition: i32,
    /// Partition error code.
    pub partition_error: i16,
    /// Current leader, or `-1` when unknown.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Leader endpoints. Empty on version 0.
    pub endpoints: Vec<VoteNodeEndpoint>,
}

/// Encode a BeginQuorumEpoch or EndQuorumEpoch response body.
pub fn encode_epoch_response(body: &EpochResponseBody) -> Result<Vec<u8>, Error> {
    if !(0..=1).contains(&body.version) || body.endpoints.len() > MAX_ARRAY {
        return Err(Error::InvalidConfig);
    }
    if body.version == 0 && !body.endpoints.is_empty() {
        return Err(Error::InvalidConfig);
    }
    let flexible = body.version >= 1;
    let mut out = allocate(body.topic.len() + 48)?;
    out.extend_from_slice(&body.error_code.to_be_bytes());
    write_count(&mut out, 1, flexible)?;
    string(&mut out, &body.topic, flexible)?;
    write_count(&mut out, 1, flexible)?;
    out.extend_from_slice(&body.partition.to_be_bytes());
    out.extend_from_slice(&body.partition_error.to_be_bytes());
    out.extend_from_slice(&body.leader_id.to_be_bytes());
    out.extend_from_slice(&body.leader_epoch.to_be_bytes());
    if !flexible {
        return Ok(out);
    }
    out.extend_from_slice(&[0, 0]);
    write_tagged_endpoints(&mut out, &body.endpoints)?;
    Ok(out)
}

/// Decode a body produced by [`encode_epoch_response`].
pub fn decode_epoch_response(bytes: &[u8], version: i16) -> Result<EpochResponseBody, Error> {
    if !(0..=1).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let flexible = version >= 1;
    let mut reader = Reader::new(bytes, flexible, protocol::Limits::default());
    let error_code = reader.i16()?;
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let partition = reader.i32()?;
    let partition_error = reader.i16()?;
    let leader_id = reader.i32()?;
    let leader_epoch = reader.i32()?;
    let endpoints = if flexible {
        reader.tags()?;
        reader.tags()?;
        read_vote_endpoints(&mut reader)?
    } else {
        Vec::new()
    };
    reader.finish()?;
    Ok(EpochResponseBody {
        version,
        error_code,
        topic,
        partition,
        partition_error,
        leader_id,
        leader_epoch,
        endpoints,
    })
}

/// Accept an epoch response without changing `persistent`.
///
/// Version 0 stays on the existing handler. Version 1 is not applied: one
/// non-empty endpoint naming `leader_id` is only `NotAdvertised`.
pub fn decide_epoch_response(
    persistent: &election::PersistentState,
    body: &EpochResponseBody,
) -> VoteDisposition {
    let _ = persistent;
    if body.version == 0 {
        return if body.endpoints.is_empty() {
            VoteDisposition::DurableV0
        } else {
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        };
    }
    if body.version != 1 {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    }
    if body.leader_id < 0 {
        return if body.endpoints.is_empty() {
            VoteDisposition::NotAdvertised
        } else {
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        };
    }
    match body.endpoints.as_slice() {
        [endpoint] if endpoint.node_id == body.leader_id && !endpoint.host.is_empty() => {
            VoteDisposition::NotAdvertised
        }
        _ => VoteDisposition::Refused(PreVoteRefusal::StaleDirectory),
    }
}

fn write_tagged_endpoints(out: &mut Vec<u8>, endpoints: &[VoteNodeEndpoint]) -> Result<(), Error> {
    if endpoints.is_empty() {
        out.push(0);
        return Ok(());
    }
    let mut payload = Vec::new();
    write_count(
        &mut payload,
        u32::try_from(endpoints.len()).map_err(|_| Error::InvalidConfig)?,
        true,
    )?;
    for endpoint in endpoints {
        payload.extend_from_slice(&endpoint.node_id.to_be_bytes());
        string(&mut payload, &endpoint.host, true)?;
        payload.extend_from_slice(&endpoint.port.to_be_bytes());
        payload.push(0);
    }
    varint(out, 1);
    varint(out, 0);
    varint(
        out,
        u32::try_from(payload.len()).map_err(|_| Error::InvalidConfig)?,
    );
    out.extend_from_slice(&payload);
    Ok(())
}

/// RemoveRaftVoter version 0. The api is not advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveVoterBody {
    /// Cluster id.
    pub cluster_id: Option<String>,
    /// Voter replica id.
    pub voter_id: i32,
    /// Voter directory id.
    pub voter_directory: [u8; 16],
}

/// AddRaftVoter versions 0 and 1. Neither version is advertised.
///
/// Version 1 carries `ack_when_committed`. Version 0 implies `true` and rejects
/// an explicit false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddVoterBody {
    /// AddRaftVoter version, 0 or 1.
    pub version: i16,
    /// Cluster id.
    pub cluster_id: Option<String>,
    /// Request timeout in milliseconds.
    pub timeout_ms: i32,
    /// Voter replica id.
    pub voter_id: i32,
    /// Voter directory id.
    pub voter_directory: [u8; 16],
    /// Endpoints for the voter.
    pub listeners: Vec<LeaderEndpoint>,
    /// When false, a caller would return after a local append. This decoder
    /// still does not append.
    pub ack_when_committed: bool,
}

/// UpdateRaftVoter version 0. The api is not advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateVoterBody {
    /// Cluster id.
    pub cluster_id: Option<String>,
    /// Leader epoch the caller believes is current.
    pub current_leader_epoch: i32,
    /// Voter replica id.
    pub voter_id: i32,
    /// Voter directory id.
    pub voter_directory: [u8; 16],
    /// Endpoints for the voter.
    pub listeners: Vec<LeaderEndpoint>,
    /// Minimum KRaft protocol version the replica supports.
    pub kraft_min: i16,
    /// Maximum KRaft protocol version the replica supports.
    pub kraft_max: i16,
}

/// Encode a flexible RemoveRaftVoter body.
pub fn encode_remove_voter(body: &RemoveVoterBody) -> Result<Vec<u8>, Error> {
    let mut out = allocate(48)?;
    write_nullable(&mut out, body.cluster_id.as_deref(), true)?;
    out.extend_from_slice(&body.voter_id.to_be_bytes());
    out.extend_from_slice(&body.voter_directory);
    out.push(0);
    Ok(out)
}

/// Decode a body produced by [`encode_remove_voter`].
pub fn decode_remove_voter(bytes: &[u8]) -> Result<RemoveVoterBody, Error> {
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let cluster_id = reader.nullable_string()?.map(str::to_owned);
    let voter_id = reader.i32()?;
    let voter_directory = reader
        .take(16)?
        .try_into()
        .map_err(|_| Error::InvalidConfig)?;
    reader.tags()?;
    reader.finish()?;
    Ok(RemoveVoterBody {
        cluster_id,
        voter_id,
        voter_directory,
    })
}

/// Accept a removal without changing `persistent` or the voter set.
pub fn decide_remove_voter(
    persistent: &election::PersistentState,
    role: election::Role,
    metadata: MetadataLog<'_>,
    voter_id: i32,
    expected_directory: [u8; 16],
    body: &RemoveVoterBody,
) -> VoteDisposition {
    if let Some(refusal) = inconsistent_cluster(metadata.cluster_id, body.cluster_id.as_deref()) {
        return VoteDisposition::Refused(refusal);
    }
    if let Some(refusal) = leader_only_refusal(role) {
        return VoteDisposition::Refused(refusal);
    }
    if let Some(refusal) = negative_voter_id(body.voter_id) {
        return VoteDisposition::Refused(refusal);
    }
    directory_change(
        persistent,
        voter_id,
        expected_directory,
        body.voter_id,
        body.voter_directory,
        true,
    )
}

/// Encode a flexible AddRaftVoter body for version 0 or 1.
pub fn encode_add_voter(body: &AddVoterBody) -> Result<Vec<u8>, Error> {
    if !(0..=1).contains(&body.version) || (body.version == 0 && !body.ack_when_committed) {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(64)?;
    write_nullable(&mut out, body.cluster_id.as_deref(), true)?;
    out.extend_from_slice(&body.timeout_ms.to_be_bytes());
    out.extend_from_slice(&body.voter_id.to_be_bytes());
    out.extend_from_slice(&body.voter_directory);
    write_listeners(&mut out, &body.listeners)?;
    if body.version >= 1 {
        out.push(u8::from(body.ack_when_committed));
    }
    out.push(0);
    Ok(out)
}

/// Decode a body produced by [`encode_add_voter`].
pub fn decode_add_voter(bytes: &[u8], version: i16) -> Result<AddVoterBody, Error> {
    if !(0..=1).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let cluster_id = reader.nullable_string()?.map(str::to_owned);
    let timeout_ms = reader.i32()?;
    let voter_id = reader.i32()?;
    let voter_directory = reader
        .take(16)?
        .try_into()
        .map_err(|_| Error::InvalidConfig)?;
    let listeners = read_listeners(&mut reader)?;
    let ack_when_committed = if version >= 1 {
        let byte = reader.byte()?;
        if byte > 1 {
            return Err(Error::InvalidConfig);
        }
        byte == 1
    } else {
        true
    };
    reader.tags()?;
    reader.finish()?;
    Ok(AddVoterBody {
        version,
        cluster_id,
        timeout_ms,
        voter_id,
        voter_directory,
        listeners,
        ack_when_committed,
    })
}

/// Accept an add without changing `persistent` or the voter set.
///
/// The default `listener` must be present. `ack_when_committed` false is still
/// not appended.
pub fn decide_add_voter(
    persistent: &election::PersistentState,
    role: election::Role,
    metadata: MetadataLog<'_>,
    voter_id: i32,
    expected_directory: [u8; 16],
    listener: &str,
    body: &AddVoterBody,
) -> VoteDisposition {
    if let Some(refusal) = inconsistent_cluster(metadata.cluster_id, body.cluster_id.as_deref()) {
        return VoteDisposition::Refused(refusal);
    }
    if let Some(refusal) = leader_only_refusal(role) {
        return VoteDisposition::Refused(refusal);
    }
    if let Some(refusal) = negative_voter_id(body.voter_id) {
        return VoteDisposition::Refused(refusal);
    }
    directory_change(
        persistent,
        voter_id,
        expected_directory,
        body.voter_id,
        body.voter_directory,
        serves_listener(&body.listeners, listener),
    )
}

/// Encode a flexible UpdateRaftVoter body.
pub fn encode_update_voter(body: &UpdateVoterBody) -> Result<Vec<u8>, Error> {
    if body.kraft_min < 0 || body.kraft_max < body.kraft_min {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(64)?;
    write_nullable(&mut out, body.cluster_id.as_deref(), true)?;
    out.extend_from_slice(&body.current_leader_epoch.to_be_bytes());
    out.extend_from_slice(&body.voter_id.to_be_bytes());
    out.extend_from_slice(&body.voter_directory);
    write_listeners(&mut out, &body.listeners)?;
    out.extend_from_slice(&body.kraft_min.to_be_bytes());
    out.extend_from_slice(&body.kraft_max.to_be_bytes());
    out.extend_from_slice(&[0, 0]);
    Ok(out)
}

/// Decode a body produced by [`encode_update_voter`].
pub fn decode_update_voter(bytes: &[u8]) -> Result<UpdateVoterBody, Error> {
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let cluster_id = reader.nullable_string()?.map(str::to_owned);
    let current_leader_epoch = reader.i32()?;
    let voter_id = reader.i32()?;
    let voter_directory = reader
        .take(16)?
        .try_into()
        .map_err(|_| Error::InvalidConfig)?;
    let listeners = read_listeners(&mut reader)?;
    let kraft_min = reader.i16()?;
    let kraft_max = reader.i16()?;
    reader.tags()?;
    reader.tags()?;
    reader.finish()?;
    Ok(UpdateVoterBody {
        cluster_id,
        current_leader_epoch,
        voter_id,
        voter_directory,
        listeners,
        kraft_min,
        kraft_max,
    })
}

/// Accept an update without changing `persistent` or the voter set.
pub fn decide_update_voter(
    persistent: &election::PersistentState,
    role: election::Role,
    metadata: MetadataLog<'_>,
    voter_id: i32,
    expected_directory: [u8; 16],
    listener: &str,
    body: &UpdateVoterBody,
) -> VoteDisposition {
    if let Some(refusal) = inconsistent_cluster(metadata.cluster_id, body.cluster_id.as_deref()) {
        return VoteDisposition::Refused(refusal);
    }
    if let Some(refusal) = negative_voter_id(body.voter_id) {
        return VoteDisposition::Refused(refusal);
    }
    // Apache `handleUpdateVoterRequest` checks `validateLeaderOnlyRequest`
    // before the voter key. A mismatched epoch does not change listeners.
    if let Some(refusal) = leader_epoch_refusal(persistent, body.current_leader_epoch) {
        return VoteDisposition::Refused(refusal);
    }
    if let Some(refusal) = leader_only_refusal(role) {
        return VoteDisposition::Refused(refusal);
    }
    let disposition = directory_change(
        persistent,
        voter_id,
        expected_directory,
        body.voter_id,
        body.voter_directory,
        serves_listener(&body.listeners, listener),
    );
    if disposition != VoteDisposition::NotAdvertised {
        return disposition;
    }
    // Apache `handleUpdateVoterRequest` rejects a negative or inverted KRaft
    // range with `INVALID_REQUEST` after the voter and listener checks. The
    // refusal does not change listeners.
    if let Some(refusal) = kraft_range_refusal(body.kraft_min, body.kraft_max) {
        return VoteDisposition::Refused(refusal);
    }
    // This profile finalizes kraft.version at 1. A range that cannot speak
    // that level is not applied.
    if body.kraft_min > 1 || body.kraft_max < 1 {
        return VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch);
    }
    disposition
}

/// AddRaftVoter or RemoveRaftVoter response.
///
/// A zero error code is not a committed membership change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoterChangeResponse {
    /// Api 80 or 81.
    pub api_key: i16,
    /// Response version. Add allows 0 and 1. Remove allows 0.
    pub version: i16,
    /// Throttle time in milliseconds.
    pub throttle_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Error message, when present.
    pub error_message: Option<String>,
}

/// Encode an AddRaftVoter or RemoveRaftVoter response body.
pub fn encode_voter_change_response(body: &VoterChangeResponse) -> Result<Vec<u8>, Error> {
    let allowed = match body.api_key {
        80 => (0..=1).contains(&body.version),
        81 => body.version == 0,
        _ => false,
    };
    if !allowed {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(32 + body.error_message.as_ref().map_or(0, String::len))?;
    out.extend_from_slice(&body.throttle_ms.to_be_bytes());
    out.extend_from_slice(&body.error_code.to_be_bytes());
    write_nullable(&mut out, body.error_message.as_deref(), true)?;
    out.push(0);
    Ok(out)
}

/// Decode a body produced by [`encode_voter_change_response`].
pub fn decode_voter_change_response(
    bytes: &[u8],
    api_key: i16,
    version: i16,
) -> Result<VoterChangeResponse, Error> {
    let allowed = match api_key {
        80 => (0..=1).contains(&version),
        81 => version == 0,
        _ => false,
    };
    if !allowed {
        return Err(Error::InvalidConfig);
    }
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let throttle_ms = reader.i32()?;
    let error_code = reader.i16()?;
    let error_message = reader.nullable_string()?.map(str::to_owned);
    reader.tags()?;
    reader.finish()?;
    Ok(VoterChangeResponse {
        api_key,
        version,
        throttle_ms,
        error_code,
        error_message,
    })
}

/// A decoded voter-change response is never applied to `persistent`.
pub fn decide_voter_change_response(
    persistent: &election::PersistentState,
    body: &VoterChangeResponse,
) -> VoteDisposition {
    let _ = (persistent, body);
    VoteDisposition::NotAdvertised
}

/// UpdateRaftVoter response. The current leader is tagged field 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateVoterResponse {
    /// Throttle time in milliseconds.
    pub throttle_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Whether tagged field 0 was present.
    pub leader_present: bool,
    /// Leader id, or `-1` when the tag is absent.
    pub leader_id: i32,
    /// Leader epoch, or `-1` when the tag is absent.
    pub leader_epoch: i32,
    /// Leader hostname.
    pub host: String,
    /// Leader port.
    pub port: i32,
}

/// Encode an UpdateRaftVoter response body.
pub fn encode_update_voter_response(body: &UpdateVoterResponse) -> Result<Vec<u8>, Error> {
    let mut out = allocate(48 + body.host.len())?;
    out.extend_from_slice(&body.throttle_ms.to_be_bytes());
    out.extend_from_slice(&body.error_code.to_be_bytes());
    if body.leader_present {
        let mut payload = Vec::new();
        payload.extend_from_slice(&body.leader_id.to_be_bytes());
        payload.extend_from_slice(&body.leader_epoch.to_be_bytes());
        string(&mut payload, &body.host, true)?;
        payload.extend_from_slice(&body.port.to_be_bytes());
        payload.push(0);
        varint(&mut out, 1);
        varint(&mut out, 0);
        varint(
            &mut out,
            u32::try_from(payload.len()).map_err(|_| Error::InvalidConfig)?,
        );
        out.extend_from_slice(&payload);
    } else {
        out.push(0);
    }
    Ok(out)
}

/// Decode a body produced by [`encode_update_voter_response`].
pub fn decode_update_voter_response(bytes: &[u8]) -> Result<UpdateVoterResponse, Error> {
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let throttle_ms = reader.i32()?;
    let error_code = reader.i16()?;
    let count = reader.varint()?;
    let (leader_present, leader_id, leader_epoch, host, port) = if count == 0 {
        (false, -1, -1, String::new(), 0)
    } else if count == 1 && reader.varint()? == 0 {
        let length = usize::try_from(reader.varint()?).map_err(|_| Error::InvalidConfig)?;
        let payload = reader.take(length)?;
        let mut nested = Reader::new(payload, true, protocol::Limits::default());
        let leader_id = nested.i32()?;
        let leader_epoch = nested.i32()?;
        let host = nested.string()?.to_owned();
        let port = nested.i32()?;
        nested.tags()?;
        nested.finish()?;
        (true, leader_id, leader_epoch, host, port)
    } else {
        return Err(Error::InvalidConfig);
    };
    reader.finish()?;
    Ok(UpdateVoterResponse {
        throttle_ms,
        error_code,
        leader_present,
        leader_id,
        leader_epoch,
        host,
        port,
    })
}

/// Accept an update response without changing `persistent` or the voter set.
///
/// A present leader with an empty host or a nonpositive port is refused.
pub fn decide_update_voter_response(
    persistent: &election::PersistentState,
    body: &UpdateVoterResponse,
) -> VoteDisposition {
    let _ = persistent;
    if body.leader_present && body.leader_id >= 0 && (body.host.is_empty() || body.port <= 0) {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    }
    VoteDisposition::NotAdvertised
}

/// Apache accepts an add or update only when the default listener is present.
///
/// Listener names are compared without case, matching `ListenerName.normalised`.
/// Refuse a removal that would leave no voter.
pub fn last_voter_refusal(voter_count: usize) -> Option<PreVoteRefusal> {
    if voter_count <= 1 {
        Some(PreVoteRefusal::LastVoter)
    } else {
        None
    }
}

fn serves_listener(listeners: &[LeaderEndpoint], required: &str) -> bool {
    if required.is_empty() {
        return false;
    }
    listeners
        .iter()
        .any(|endpoint| !endpoint.host.is_empty() && endpoint.name.eq_ignore_ascii_case(required))
}

fn leader_only_refusal(role: election::Role) -> Option<PreVoteRefusal> {
    if role == election::Role::Leader {
        None
    } else {
        Some(PreVoteRefusal::NotLeader)
    }
}

fn kraft_range_refusal(min: i16, max: i16) -> Option<PreVoteRefusal> {
    if min < 0 || max < 0 || max < min {
        Some(PreVoteRefusal::IllegalEpoch)
    } else {
        None
    }
}

fn negative_voter_id(voter_id: i32) -> Option<PreVoteRefusal> {
    if voter_id < 0 {
        Some(PreVoteRefusal::IllegalEpoch)
    } else {
        None
    }
}

fn directory_change(
    persistent: &election::PersistentState,
    voter_id: i32,
    expected_directory: [u8; 16],
    actual_id: i32,
    actual_directory: [u8; 16],
    usable: bool,
) -> VoteDisposition {
    let _ = persistent;
    if usable
        && actual_id == voter_id
        && actual_directory == expected_directory
        && actual_directory != [0; 16]
    {
        VoteDisposition::NotAdvertised
    } else {
        VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
    }
}

fn write_listeners(out: &mut Vec<u8>, listeners: &[LeaderEndpoint]) -> Result<(), Error> {
    if listeners.len() > MAX_ARRAY {
        return Err(Error::InvalidConfig);
    }
    write_count(
        out,
        u32::try_from(listeners.len()).map_err(|_| Error::InvalidConfig)?,
        true,
    )?;
    for listener in listeners {
        string(out, &listener.name, true)?;
        string(out, &listener.host, true)?;
        out.extend_from_slice(&listener.port.to_be_bytes());
        out.push(0);
    }
    Ok(())
}

fn read_listeners(reader: &mut Reader<'_>) -> Result<Vec<LeaderEndpoint>, Error> {
    let n = reader.count()?;
    let mut listeners = Vec::new();
    listeners
        .try_reserve_exact(n)
        .map_err(|_| Error::Allocation)?;
    for _ in 0..n {
        let name = reader.string()?.to_owned();
        let host = reader.string()?.to_owned();
        let port = u16::from_be_bytes(
            reader
                .take(2)?
                .try_into()
                .map_err(|_| Error::InvalidConfig)?,
        );
        reader.tags()?;
        listeners.push(LeaderEndpoint { name, host, port });
    }
    Ok(listeners)
}

/// One voter inside a Kafka `VotersRecord`.
///
/// This is the upstream flexible record, not the local `PLVOTR01` configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaVoter {
    /// Replica id.
    pub id: i32,
    /// Directory id. Zero is not a usable identity.
    pub directory: [u8; 16],
    /// Endpoints for this voter.
    pub endpoints: Vec<LeaderEndpoint>,
    /// Minimum KRaft protocol version.
    pub kraft_min: i16,
    /// Maximum KRaft protocol version.
    pub kraft_max: i16,
}

/// Kafka `VotersRecord` version 0.
///
/// Decoding it does not replace the live voter set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaVotersRecord {
    /// Record version. Only 0 is defined.
    pub version: i16,
    /// Voters in the record.
    pub voters: Vec<KafkaVoter>,
}

/// Encode a flexible Kafka `VotersRecord`.
pub fn encode_voters_record(record: &KafkaVotersRecord) -> Result<Vec<u8>, Error> {
    if record.version != 0 || record.voters.is_empty() || record.voters.len() > MAX_ARRAY {
        return Err(Error::InvalidConfig);
    }
    let mut seen = Vec::new();
    let mut out = allocate(64)?;
    out.extend_from_slice(&record.version.to_be_bytes());
    write_count(
        &mut out,
        u32::try_from(record.voters.len()).map_err(|_| Error::InvalidConfig)?,
        true,
    )?;
    for voter in &record.voters {
        if voter.kraft_min < 0 || voter.kraft_max < voter.kraft_min || seen.contains(&voter.id) {
            return Err(Error::InvalidConfig);
        }
        seen.push(voter.id);
        out.extend_from_slice(&voter.id.to_be_bytes());
        out.extend_from_slice(&voter.directory);
        write_listeners(&mut out, &voter.endpoints)?;
        out.extend_from_slice(&voter.kraft_min.to_be_bytes());
        out.extend_from_slice(&voter.kraft_max.to_be_bytes());
        out.extend_from_slice(&[0, 0]);
    }
    out.push(0);
    Ok(out)
}

/// Decode a body produced by [`encode_voters_record`].
pub fn decode_voters_record(bytes: &[u8]) -> Result<KafkaVotersRecord, Error> {
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let version = reader.i16()?;
    if version != 0 {
        return Err(Error::InvalidConfig);
    }
    let n = reader.count()?;
    if n == 0 {
        return Err(Error::InvalidConfig);
    }
    let mut voters = Vec::new();
    voters.try_reserve_exact(n).map_err(|_| Error::Allocation)?;
    for _ in 0..n {
        let id = reader.i32()?;
        if voters.iter().any(|voter: &KafkaVoter| voter.id == id) {
            return Err(Error::InvalidConfig);
        }
        let directory = reader
            .take(16)?
            .try_into()
            .map_err(|_| Error::InvalidConfig)?;
        let endpoints = read_listeners(&mut reader)?;
        let kraft_min = reader.i16()?;
        let kraft_max = reader.i16()?;
        reader.tags()?;
        reader.tags()?;
        voters.push(KafkaVoter {
            id,
            directory,
            endpoints,
            kraft_min,
            kraft_max,
        });
    }
    reader.tags()?;
    reader.finish()?;
    Ok(KafkaVotersRecord { version, voters })
}

/// Compare a `VotersRecord` with the live identities without changing them.
///
/// A directory that disagrees with a live id is refused. An add or removal is
/// only `NotAdvertised` and must not be applied by the caller.
pub fn decide_voters_record(
    persistent: &election::PersistentState,
    current: &[(i32, [u8; 16])],
    record: &KafkaVotersRecord,
) -> VoteDisposition {
    let _ = persistent;
    if record.version != 0 {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    }
    for voter in &record.voters {
        if voter.directory == [0; 16] || voter.endpoints.is_empty() {
            return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
        }
        if current
            .iter()
            .any(|(id, directory)| *id == voter.id && *directory != voter.directory)
        {
            return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
        }
    }
    VoteDisposition::NotAdvertised
}

/// One replica row in a DescribeQuorum response.
///
/// `directory` is present only on version 2. Fetch and caught-up timestamps
/// are present on versions 1 and 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumReplica {
    /// Replica id.
    pub replica_id: i32,
    /// Replica directory. Required on version 2.
    pub directory: Option<[u8; 16]>,
    /// Last known log end offset.
    pub log_end_offset: i64,
    /// Last fetch timestamp, or `-1` when unknown or on version 0.
    pub last_fetch_timestamp: i64,
    /// Last caught-up timestamp, or `-1` when unknown or on version 0.
    pub last_caught_up_timestamp: i64,
}

/// One controller node in DescribeQuorum version 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumNode {
    /// Node id.
    pub node_id: i32,
    /// Listeners for this node.
    pub listeners: Vec<LeaderEndpoint>,
}

/// One DescribeQuorum response body for versions 0, 1, or 2.
///
/// Version 2 adds error messages, replica directories, and nodes. None of
/// these versions are advertised on the controller listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumBody {
    /// DescribeQuorum version, 0, 1, or 2.
    pub version: i16,
    /// Top-level error code.
    pub error_code: i16,
    /// Top-level error message. Version 2 only.
    pub error_message: Option<String>,
    /// Metadata topic.
    pub topic: String,
    /// Metadata partition.
    pub partition: i32,
    /// Partition error code.
    pub partition_error: i16,
    /// Partition error message. Version 2 only.
    pub partition_error_message: Option<String>,
    /// Current leader, or `-1` when unknown.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// High watermark.
    pub high_watermark: i64,
    /// Current voters.
    pub voters: Vec<QuorumReplica>,
    /// Observers.
    pub observers: Vec<QuorumReplica>,
    /// Nodes. Version 2 only.
    pub nodes: Vec<QuorumNode>,
}

/// Encode a flexible DescribeQuorum body for version 0, 1, or 2.
pub fn encode_describe_quorum(body: &DescribeQuorumBody) -> Result<Vec<u8>, Error> {
    if !(0..=2).contains(&body.version)
        || body.voters.len() > MAX_ARRAY
        || body.observers.len() > MAX_ARRAY
        || body.nodes.len() > MAX_ARRAY
    {
        return Err(Error::InvalidConfig);
    }
    if body.version < 2
        && (body.error_message.is_some()
            || body.partition_error_message.is_some()
            || !body.nodes.is_empty())
    {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(body.topic.len() + 96)?;
    out.extend_from_slice(&body.error_code.to_be_bytes());
    if body.version >= 2 {
        write_nullable(&mut out, body.error_message.as_deref(), true)?;
    }
    write_count(&mut out, 1, true)?;
    string(&mut out, &body.topic, true)?;
    write_count(&mut out, 1, true)?;
    out.extend_from_slice(&body.partition.to_be_bytes());
    out.extend_from_slice(&body.partition_error.to_be_bytes());
    if body.version >= 2 {
        write_nullable(&mut out, body.partition_error_message.as_deref(), true)?;
    }
    out.extend_from_slice(&body.leader_id.to_be_bytes());
    out.extend_from_slice(&body.leader_epoch.to_be_bytes());
    out.extend_from_slice(&body.high_watermark.to_be_bytes());
    write_replicas(&mut out, body.version, &body.voters)?;
    write_replicas(&mut out, body.version, &body.observers)?;
    out.extend_from_slice(&[0, 0]);
    if body.version >= 2 {
        write_quorum_nodes(&mut out, &body.nodes)?;
    }
    out.push(0);
    Ok(out)
}

/// Decode a body produced by [`encode_describe_quorum`].
pub fn decode_describe_quorum(bytes: &[u8], version: i16) -> Result<DescribeQuorumBody, Error> {
    if !(0..=2).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let error_code = reader.i16()?;
    let error_message = if version >= 2 {
        reader.nullable_string()?.map(str::to_owned)
    } else {
        None
    };
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let partition = reader.i32()?;
    let partition_error = reader.i16()?;
    let partition_error_message = if version >= 2 {
        reader.nullable_string()?.map(str::to_owned)
    } else {
        None
    };
    let leader_id = reader.i32()?;
    let leader_epoch = reader.i32()?;
    let high_watermark = reader.i64()?;
    let voters = read_replicas(&mut reader, version)?;
    let observers = read_replicas(&mut reader, version)?;
    reader.tags()?;
    reader.tags()?;
    let nodes = if version >= 2 {
        read_quorum_nodes(&mut reader)?
    } else {
        Vec::new()
    };
    reader.tags()?;
    reader.finish()?;
    Ok(DescribeQuorumBody {
        version,
        error_code,
        error_message,
        topic,
        partition,
        partition_error,
        partition_error_message,
        leader_id,
        leader_epoch,
        high_watermark,
        voters,
        observers,
        nodes,
    })
}

/// One DescribeQuorum request. Versions 0 through 2 share this body.
///
/// The request is not advertised, and decoding it does not write quorum state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumRequestBody {
    /// DescribeQuorum version, 0, 1, or 2.
    pub version: i16,
    /// Topic name.
    pub topic: String,
    /// Partition indexes.
    pub partitions: Vec<i32>,
}

/// Encode a flexible DescribeQuorum request.
pub fn encode_describe_quorum_request(body: &DescribeQuorumRequestBody) -> Result<Vec<u8>, Error> {
    if !(0..=2).contains(&body.version)
        || body.partitions.is_empty()
        || body.partitions.len() > MAX_ARRAY
        || body
            .partitions
            .iter()
            .enumerate()
            .any(|(index, partition)| {
                *partition < 0 || body.partitions[..index].contains(partition)
            })
    {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(body.topic.len() + 16 + body.partitions.len() * 4)?;
    write_count(&mut out, 1, true)?;
    string(&mut out, &body.topic, true)?;
    write_count(
        &mut out,
        u32::try_from(body.partitions.len()).map_err(|_| Error::InvalidConfig)?,
        true,
    )?;
    for partition in &body.partitions {
        out.extend_from_slice(&partition.to_be_bytes());
        out.push(0);
    }
    // Topic suffix, then the request suffix. Apache writes both.
    out.extend_from_slice(&[0, 0]);
    Ok(out)
}

/// Decode a body produced by [`encode_describe_quorum_request`].
pub fn decode_describe_quorum_request(
    bytes: &[u8],
    version: i16,
) -> Result<DescribeQuorumRequestBody, Error> {
    if !(0..=2).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    let n = reader.count()?;
    if n == 0 {
        return Err(Error::InvalidConfig);
    }
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(n)
        .map_err(|_| Error::Allocation)?;
    for _ in 0..n {
        let partition = reader.i32()?;
        if partition < 0 || partitions.contains(&partition) {
            return Err(Error::InvalidConfig);
        }
        partitions.push(partition);
        reader.tags()?;
    }
    reader.tags()?;
    reader.tags()?;
    reader.finish()?;
    Ok(DescribeQuorumRequestBody {
        version,
        topic,
        partitions,
    })
}

/// Accept a DescribeQuorum request without changing `persistent`.
///
/// A topic or partition other than the configured metadata log is refused.
/// A match is `NotAdvertised` and must not be turned into a stored response.
pub fn decide_describe_quorum_request(
    persistent: &election::PersistentState,
    role: election::Role,
    topic: &str,
    partition: i32,
    body: &DescribeQuorumRequestBody,
) -> VoteDisposition {
    let _ = persistent;
    if body.topic != topic || !body.partitions.contains(&partition) {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    }
    if let Some(refusal) = leader_only_refusal(role) {
        return VoteDisposition::Refused(refusal);
    }
    VoteDisposition::NotAdvertised
}

/// Accept a DescribeQuorum body without changing `persistent`.
///
/// Version 2 is refused when the named replica's directory does not match.
/// The body is not applied, and api 55 stays off the controller listener.
pub fn decide_describe_quorum(
    persistent: &election::PersistentState,
    replica_id: i32,
    expected_directory: [u8; 16],
    body: &DescribeQuorumBody,
) -> VoteDisposition {
    let _ = persistent;
    if body.version < 2 {
        return VoteDisposition::NotAdvertised;
    }
    match body
        .voters
        .iter()
        .find(|replica| replica.replica_id == replica_id)
        .and_then(|replica| replica.directory)
    {
        Some(directory) if directory == expected_directory && directory != [0; 16] => {
            VoteDisposition::NotAdvertised
        }
        _ => VoteDisposition::Refused(PreVoteRefusal::StaleDirectory),
    }
}

fn write_replicas(
    out: &mut Vec<u8>,
    version: i16,
    replicas: &[QuorumReplica],
) -> Result<(), Error> {
    write_count(
        out,
        u32::try_from(replicas.len()).map_err(|_| Error::InvalidConfig)?,
        true,
    )?;
    for replica in replicas {
        if version < 2 && replica.directory.is_some() {
            return Err(Error::InvalidConfig);
        }
        if version >= 2 && replica.directory.is_none() {
            return Err(Error::InvalidConfig);
        }
        if version == 0
            && (replica.last_fetch_timestamp != -1 || replica.last_caught_up_timestamp != -1)
        {
            return Err(Error::InvalidConfig);
        }
        out.extend_from_slice(&replica.replica_id.to_be_bytes());
        if let Some(directory) = replica.directory {
            out.extend_from_slice(&directory);
        }
        out.extend_from_slice(&replica.log_end_offset.to_be_bytes());
        if version >= 1 {
            out.extend_from_slice(&replica.last_fetch_timestamp.to_be_bytes());
            out.extend_from_slice(&replica.last_caught_up_timestamp.to_be_bytes());
        }
        out.push(0);
    }
    Ok(())
}

fn read_replicas(reader: &mut Reader<'_>, version: i16) -> Result<Vec<QuorumReplica>, Error> {
    let n = reader.count()?;
    let mut replicas = Vec::new();
    replicas
        .try_reserve_exact(n)
        .map_err(|_| Error::Allocation)?;
    for _ in 0..n {
        let replica_id = reader.i32()?;
        let directory = if version >= 2 {
            Some(
                reader
                    .take(16)?
                    .try_into()
                    .map_err(|_| Error::InvalidConfig)?,
            )
        } else {
            None
        };
        let log_end_offset = reader.i64()?;
        let (last_fetch_timestamp, last_caught_up_timestamp) = if version >= 1 {
            (reader.i64()?, reader.i64()?)
        } else {
            (-1, -1)
        };
        reader.tags()?;
        replicas.push(QuorumReplica {
            replica_id,
            directory,
            log_end_offset,
            last_fetch_timestamp,
            last_caught_up_timestamp,
        });
    }
    Ok(replicas)
}

fn write_quorum_nodes(out: &mut Vec<u8>, nodes: &[QuorumNode]) -> Result<(), Error> {
    write_count(
        out,
        u32::try_from(nodes.len()).map_err(|_| Error::InvalidConfig)?,
        true,
    )?;
    for node in nodes {
        if node.listeners.len() > MAX_ARRAY {
            return Err(Error::InvalidConfig);
        }
        out.extend_from_slice(&node.node_id.to_be_bytes());
        write_count(
            out,
            u32::try_from(node.listeners.len()).map_err(|_| Error::InvalidConfig)?,
            true,
        )?;
        for listener in &node.listeners {
            string(out, &listener.name, true)?;
            string(out, &listener.host, true)?;
            out.extend_from_slice(&listener.port.to_be_bytes());
            out.push(0);
        }
        out.push(0);
    }
    Ok(())
}

fn read_quorum_nodes(reader: &mut Reader<'_>) -> Result<Vec<QuorumNode>, Error> {
    let n = reader.count()?;
    let mut nodes = Vec::new();
    nodes.try_reserve_exact(n).map_err(|_| Error::Allocation)?;
    for _ in 0..n {
        let node_id = reader.i32()?;
        let listeners_n = reader.count()?;
        let mut listeners = Vec::new();
        listeners
            .try_reserve_exact(listeners_n)
            .map_err(|_| Error::Allocation)?;
        for _ in 0..listeners_n {
            let name = reader.string()?.to_owned();
            let host = reader.string()?.to_owned();
            let port = u16::from_be_bytes(
                reader
                    .take(2)?
                    .try_into()
                    .map_err(|_| Error::InvalidConfig)?,
            );
            reader.tags()?;
            listeners.push(LeaderEndpoint { name, host, port });
        }
        reader.tags()?;
        nodes.push(QuorumNode { node_id, listeners });
    }
    Ok(nodes)
}

/// One leader endpoint from BeginQuorumEpoch version 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEndpoint {
    /// Endpoint name.
    pub name: String,
    /// Hostname.
    pub host: String,
    /// Port.
    pub port: u16,
}

/// One BeginQuorumEpoch body for versions 0 or 1.
///
/// Version 0 is classic. Version 1 is flexible and adds the voter directory
/// and leader endpoints. Version 1 is not advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginEpochBody {
    /// BeginQuorumEpoch version, 0 or 1.
    pub version: i16,
    /// Cluster id.
    pub cluster_id: Option<String>,
    /// Receiving voter. Version 0 stores `-1`.
    pub voter_id: i32,
    /// Metadata topic.
    pub topic: String,
    /// Metadata partition.
    pub partition: i32,
    /// Receiving voter directory. Required on version 1.
    pub voter_directory: Option<[u8; 16]>,
    /// Elected leader id.
    pub leader_id: i32,
    /// Elected leader epoch.
    pub leader_epoch: i32,
    /// Leader endpoints. Empty on version 0.
    pub leader_endpoints: Vec<LeaderEndpoint>,
}

/// Encode a BeginQuorumEpoch body. Version 0 is classic; version 1 is flexible.
pub fn encode_begin_body(body: &BeginEpochBody) -> Result<Vec<u8>, Error> {
    if body.version == 0 {
        if body.voter_directory.is_some()
            || !body.leader_endpoints.is_empty()
            || body.voter_id != -1
        {
            return Err(Error::InvalidConfig);
        }
    } else if body.version != 1 || body.voter_directory.is_none() {
        return Err(Error::InvalidConfig);
    }
    let flexible = body.version >= 1;
    let mut out = allocate(body.topic.len() + 64)?;
    write_nullable(&mut out, body.cluster_id.as_deref(), flexible)?;
    if flexible {
        out.extend_from_slice(&body.voter_id.to_be_bytes());
    }
    write_count(&mut out, 1, flexible)?;
    string(&mut out, &body.topic, flexible)?;
    write_count(&mut out, 1, flexible)?;
    out.extend_from_slice(&body.partition.to_be_bytes());
    if flexible {
        out.extend_from_slice(body.voter_directory.ok_or(Error::InvalidConfig)?.as_slice());
    }
    out.extend_from_slice(&body.leader_id.to_be_bytes());
    out.extend_from_slice(&body.leader_epoch.to_be_bytes());
    if flexible {
        out.push(0);
        out.push(0);
        write_count(
            &mut out,
            u32::try_from(body.leader_endpoints.len()).map_err(|_| Error::InvalidConfig)?,
            flexible,
        )?;
        for endpoint in &body.leader_endpoints {
            string(&mut out, &endpoint.name, true)?;
            string(&mut out, &endpoint.host, true)?;
            out.extend_from_slice(&endpoint.port.to_be_bytes());
            out.push(0);
        }
        out.push(0);
    }
    Ok(out)
}

/// Decode a body produced by [`encode_begin_body`].
pub fn decode_begin_body(bytes: &[u8], version: i16) -> Result<BeginEpochBody, Error> {
    if !(0..=1).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let flexible = version >= 1;
    let mut reader = Reader::new(bytes, flexible, protocol::Limits::default());
    let cluster_id = reader.nullable_string()?.map(str::to_owned);
    let voter_id = if flexible { reader.i32()? } else { -1 };
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let partition = reader.i32()?;
    let voter_directory = if flexible {
        Some(
            reader
                .take(16)?
                .try_into()
                .map_err(|_| Error::InvalidConfig)?,
        )
    } else {
        None
    };
    let leader_id = reader.i32()?;
    let leader_epoch = reader.i32()?;
    let mut leader_endpoints = Vec::new();
    if flexible {
        reader.tags()?;
        reader.tags()?;
        let count = reader.count()?;
        for _ in 0..count {
            leader_endpoints.push(LeaderEndpoint {
                name: reader.string()?.to_owned(),
                host: reader.string()?.to_owned(),
                port: u16::from_be_bytes(
                    reader
                        .take(2)?
                        .try_into()
                        .map_err(|_| Error::InvalidConfig)?,
                ),
            });
            reader.tags()?;
        }
        reader.tags()?;
    }
    reader.finish()?;
    Ok(BeginEpochBody {
        version,
        cluster_id,
        voter_id,
        topic,
        partition,
        voter_directory,
        leader_id,
        leader_epoch,
        leader_endpoints,
    })
}

/// Accept a BeginQuorumEpoch body without changing `persistent`.
///
/// Version 0 is the classic handler input. Version 1 addressed to another
/// replica is refused. A matching version 1 body is parsed and not advertised.
pub fn decide_begin_body(
    persistent: &election::PersistentState,
    metadata: MetadataLog<'_>,
    local_voter_id: i32,
    expected_directory: [u8; 16],
    body: &BeginEpochBody,
) -> VoteDisposition {
    if body.version == 0 {
        return VoteDisposition::DurableV0;
    }
    if let Some(refusal) = metadata_refusal(
        metadata,
        body.cluster_id.as_deref(),
        &body.topic,
        body.partition,
    ) {
        return VoteDisposition::Refused(refusal);
    }
    // Apache `validateVoterOnlyRequest` fences an older epoch before the
    // voter-key check, and does not write the new leader.
    if quorum_epoch_fenced(persistent, body.leader_epoch) {
        return VoteDisposition::Refused(PreVoteRefusal::StaleTerm);
    }
    if body.leader_epoch < 0 || body.leader_id < 0 {
        return VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch);
    }
    // Apache `isValidVoterKey` runs after the epoch fence and does not write
    // the new leader. A zero directory is the id-only key.
    if let Some(refusal) = addressed_voter_refusal(
        local_voter_id,
        expected_directory,
        body.voter_id,
        body.voter_directory,
    ) {
        return VoteDisposition::Refused(refusal);
    }
    VoteDisposition::NotAdvertised
}

/// One EndQuorumEpoch body for versions 0 or 1.
///
/// Version 0 lists preferred successor ids. Version 1 lists preferred
/// candidates with directory ids and is not advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndEpochBody {
    /// EndQuorumEpoch version, 0 or 1.
    pub version: i16,
    /// Cluster id.
    pub cluster_id: Option<String>,
    /// Metadata topic.
    pub topic: String,
    /// Metadata partition.
    pub partition: i32,
    /// Resigning leader id.
    pub leader_id: i32,
    /// Resigning leader epoch.
    pub leader_epoch: i32,
    /// Preferred successors. Version 0 entries omit directories.
    pub preferred: Vec<PreferredCandidate>,
    /// Leader endpoints. Empty on version 0.
    pub leader_endpoints: Vec<LeaderEndpoint>,
}

/// Encode an EndQuorumEpoch body. Version 0 is classic; version 1 is flexible.
pub fn encode_end_body(body: &EndEpochBody) -> Result<Vec<u8>, Error> {
    if body.preferred.len() > MAX_SUCCESSORS {
        return Err(Error::InvalidConfig);
    }
    if body.version == 0 {
        if body
            .preferred
            .iter()
            .any(|candidate| candidate.directory.is_some())
            || !body.leader_endpoints.is_empty()
        {
            return Err(Error::InvalidConfig);
        }
    } else if body.version != 1
        || body
            .preferred
            .iter()
            .any(|candidate| candidate.directory.is_none())
    {
        return Err(Error::InvalidConfig);
    }
    let flexible = body.version >= 1;
    let mut out = allocate(body.topic.len() + 96)?;
    write_nullable(&mut out, body.cluster_id.as_deref(), flexible)?;
    write_count(&mut out, 1, flexible)?;
    string(&mut out, &body.topic, flexible)?;
    write_count(&mut out, 1, flexible)?;
    out.extend_from_slice(&body.partition.to_be_bytes());
    out.extend_from_slice(&body.leader_id.to_be_bytes());
    out.extend_from_slice(&body.leader_epoch.to_be_bytes());
    write_count(
        &mut out,
        u32::try_from(body.preferred.len()).map_err(|_| Error::InvalidConfig)?,
        flexible,
    )?;
    for candidate in &body.preferred {
        out.extend_from_slice(&candidate.id.to_be_bytes());
        if flexible {
            out.extend_from_slice(candidate.directory.ok_or(Error::InvalidConfig)?.as_slice());
            out.push(0);
        }
    }
    if flexible {
        out.push(0);
        out.push(0);
        write_count(
            &mut out,
            u32::try_from(body.leader_endpoints.len()).map_err(|_| Error::InvalidConfig)?,
            true,
        )?;
        for endpoint in &body.leader_endpoints {
            string(&mut out, &endpoint.name, true)?;
            string(&mut out, &endpoint.host, true)?;
            out.extend_from_slice(&endpoint.port.to_be_bytes());
            out.push(0);
        }
        out.push(0);
    }
    Ok(out)
}

/// Decode a body produced by [`encode_end_body`].
pub fn decode_end_body(bytes: &[u8], version: i16) -> Result<EndEpochBody, Error> {
    if !(0..=1).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let flexible = version >= 1;
    let mut reader = Reader::new(bytes, flexible, protocol::Limits::default());
    let cluster_id = reader.nullable_string()?.map(str::to_owned);
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let partition = reader.i32()?;
    let leader_id = reader.i32()?;
    let leader_epoch = reader.i32()?;
    let preferred_count = reader.count()?;
    if preferred_count > MAX_SUCCESSORS {
        return Err(Error::InvalidConfig);
    }
    let mut preferred = Vec::new();
    for _ in 0..preferred_count {
        let id = reader.i32()?;
        let directory = if flexible {
            let bytes = reader.take(16)?;
            let mut directory = [0u8; 16];
            directory.copy_from_slice(bytes);
            reader.tags()?;
            Some(directory)
        } else {
            None
        };
        preferred.push(PreferredCandidate { id, directory });
    }
    let mut leader_endpoints = Vec::new();
    if flexible {
        reader.tags()?;
        reader.tags()?;
        let count = reader.count()?;
        for _ in 0..count {
            leader_endpoints.push(LeaderEndpoint {
                name: reader.string()?.to_owned(),
                host: reader.string()?.to_owned(),
                port: u16::from_be_bytes(
                    reader
                        .take(2)?
                        .try_into()
                        .map_err(|_| Error::InvalidConfig)?,
                ),
            });
            reader.tags()?;
        }
        reader.tags()?;
    }
    reader.finish()?;
    Ok(EndEpochBody {
        version,
        cluster_id,
        topic,
        partition,
        leader_id,
        leader_epoch,
        preferred,
        leader_endpoints,
    })
}

/// Rank the local voter in an EndQuorumEpoch body without changing `persistent`.
///
/// Version 0 remains the classic handler input. Version 1 is ranked only when
/// the directory matches and is not advertised.
pub fn decide_end_body(
    persistent: &election::PersistentState,
    metadata: MetadataLog<'_>,
    local_id: i32,
    local_directory: [u8; 16],
    body: &EndEpochBody,
) -> Result<(VoteDisposition, Option<usize>), Error> {
    if body.version != 0 {
        if let Some(refusal) = metadata_refusal(
            metadata,
            body.cluster_id.as_deref(),
            &body.topic,
            body.partition,
        ) {
            return Ok((VoteDisposition::Refused(refusal), None));
        }
    }
    if body.version != 0 && quorum_epoch_fenced(persistent, body.leader_epoch) {
        return Ok((VoteDisposition::Refused(PreVoteRefusal::StaleTerm), None));
    }
    if body.version != 0 && (body.leader_epoch < 0 || body.leader_id < 0) {
        return Ok((VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch), None));
    }
    if body.version != 0 {
        if let Some(refusal) = successor_list_refusal(body.leader_id, local_id, &body.preferred) {
            return Ok((VoteDisposition::Refused(refusal), None));
        }
    }
    let rank = preferred_rank(&body.preferred, local_id, local_directory)?;
    let disposition = if body.version == 0 {
        VoteDisposition::DurableV0
    } else if rank.is_none()
        && body.preferred.iter().any(|candidate| {
            candidate.id == local_id && candidate.directory != Some(local_directory)
        })
    {
        VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
    } else {
        VoteDisposition::NotAdvertised
    };
    Ok((disposition, rank))
}

/// One FetchSnapshot partition request.
///
/// Version 0 has no follower directory. Version 1 carries it as tagged field 0
/// and is not advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchSnapshotBody {
    /// FetchSnapshot version, 0 or 1.
    pub version: i16,
    /// Cluster id, carried as request tagged field 0 when present.
    pub cluster_id: Option<String>,
    /// Follower replica id.
    pub replica_id: i32,
    /// Maximum snapshot bytes.
    pub max_bytes: i32,
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Leader epoch the follower believes is current.
    pub current_leader_epoch: i32,
    /// Snapshot end offset.
    pub end_offset: i64,
    /// Snapshot epoch.
    pub epoch: i32,
    /// Byte position to resume from.
    pub position: i64,
    /// Follower directory. Required on version 1.
    pub replica_directory: Option<[u8; 16]>,
}

/// Encode a flexible FetchSnapshot body for version 0 or 1.
pub fn encode_fetch_snapshot_body(body: &FetchSnapshotBody) -> Result<Vec<u8>, Error> {
    if body.version == 0 && body.replica_directory.is_some() {
        return Err(Error::InvalidConfig);
    }
    if (body.version == 1 && body.replica_directory.is_none()) || !(0..=1).contains(&body.version) {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(body.topic.len() + 80)?;
    out.extend_from_slice(&body.replica_id.to_be_bytes());
    out.extend_from_slice(&body.max_bytes.to_be_bytes());
    write_count(&mut out, 1, true)?;
    string(&mut out, &body.topic, true)?;
    write_count(&mut out, 1, true)?;
    out.extend_from_slice(&body.partition.to_be_bytes());
    out.extend_from_slice(&body.current_leader_epoch.to_be_bytes());
    out.extend_from_slice(&body.end_offset.to_be_bytes());
    out.extend_from_slice(&body.epoch.to_be_bytes());
    out.push(0);
    out.extend_from_slice(&body.position.to_be_bytes());
    if let Some(directory) = body.replica_directory {
        varint(&mut out, 1);
        varint(&mut out, 0);
        varint(&mut out, 16);
        out.extend_from_slice(&directory);
    } else {
        out.push(0);
    }
    out.push(0);
    match &body.cluster_id {
        Some(cluster) => {
            varint(&mut out, 1);
            varint(&mut out, 0);
            let mut payload = Vec::new();
            string(&mut payload, cluster, true)?;
            varint(
                &mut out,
                u32::try_from(payload.len()).map_err(|_| Error::InvalidConfig)?,
            );
            out.extend_from_slice(&payload);
        }
        None => out.push(0),
    }
    Ok(out)
}

/// Decode a body produced by [`encode_fetch_snapshot_body`].
pub fn decode_fetch_snapshot_body(bytes: &[u8], version: i16) -> Result<FetchSnapshotBody, Error> {
    if !(0..=1).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let replica_id = reader.i32()?;
    let max_bytes = reader.i32()?;
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let partition = reader.i32()?;
    let current_leader_epoch = reader.i32()?;
    let end_offset = reader.i64()?;
    let epoch = reader.i32()?;
    reader.tags()?;
    let position = reader.i64()?;
    let replica_directory = if version >= 1 {
        Some(read_directory_tag(&mut reader)?)
    } else {
        reader.tags()?;
        None
    };
    reader.tags()?;
    let cluster_id = read_cluster_tag(&mut reader)?;
    reader.finish()?;
    Ok(FetchSnapshotBody {
        version,
        cluster_id,
        replica_id,
        max_bytes,
        topic,
        partition,
        current_leader_epoch,
        end_offset,
        epoch,
        position,
        replica_directory,
    })
}

/// Accept a FetchSnapshot body without changing `persistent`.
///
/// Version 1 with a mismatched follower directory is refused. Neither version
/// is advertised on the controller listener.
pub fn decide_fetch_snapshot_body(
    persistent: &election::PersistentState,
    role: election::Role,
    metadata: MetadataLog<'_>,
    expected_directory: [u8; 16],
    body: &FetchSnapshotBody,
) -> VoteDisposition {
    if let Some(refusal) = metadata_refusal(
        metadata,
        body.cluster_id.as_deref(),
        &body.topic,
        body.partition,
    ) {
        return VoteDisposition::Refused(refusal);
    }
    // Apache `validateLeaderOnlyRequest` requires the follower's leader epoch
    // to equal the local epoch. An older epoch is fenced and a newer one is
    // unknown. Neither result installs snapshot bytes.
    if let Some(refusal) = leader_epoch_refusal(persistent, body.current_leader_epoch) {
        return VoteDisposition::Refused(refusal);
    }
    if let Some(refusal) = leader_only_refusal(role) {
        return VoteDisposition::Refused(refusal);
    }
    // Apache returns `POSITION_OUT_OF_RANGE` before serving snapshot bytes
    // when the resume position is negative.
    if body.position < 0 {
        return VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch);
    }
    if body.version == 0 {
        return VoteDisposition::NotAdvertised;
    }
    match body.replica_directory {
        Some(directory) if directory == expected_directory && directory != [0; 16] => {
            VoteDisposition::NotAdvertised
        }
        _ => VoteDisposition::Refused(PreVoteRefusal::StaleDirectory),
    }
}

fn read_directory_tag(reader: &mut Reader<'_>) -> Result<[u8; 16], Error> {
    let count = reader.varint()?;
    if count != 1 {
        return Err(Error::InvalidConfig);
    }
    if reader.varint()? != 0 {
        return Err(Error::InvalidConfig);
    }
    let length = reader.varint()?;
    if length != 16 {
        return Err(Error::InvalidConfig);
    }
    reader
        .take(16)?
        .try_into()
        .map_err(|_| Error::InvalidConfig)
}

fn read_cluster_tag(reader: &mut Reader<'_>) -> Result<Option<String>, Error> {
    let count = reader.varint()?;
    if count == 0 {
        return Ok(None);
    }
    if count != 1 || reader.varint()? != 0 {
        return Err(Error::InvalidConfig);
    }
    let length = usize::try_from(reader.varint()?).map_err(|_| Error::InvalidConfig)?;
    let payload = reader.take(length)?;
    let mut nested = Reader::new(payload, true, protocol::Limits::default());
    let cluster = nested.nullable_string()?.map(str::to_owned);
    nested.finish()?;
    Ok(cluster)
}

const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;

/// Leader named by a FetchSnapshot response partition tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLeader {
    /// Leader id, or `-1` when unknown.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
}

/// One FetchSnapshot response body for versions 0 and 1.
///
/// Version 1 carries leader endpoints. Neither version is advertised, and the
/// unaligned bytes are not installed as a local snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchSnapshotResponseBody {
    /// FetchSnapshot version, 0 or 1.
    pub version: i16,
    /// Throttle time in milliseconds.
    pub throttle_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Partition error code.
    pub partition_error: i16,
    /// Snapshot end offset.
    pub end_offset: i64,
    /// Snapshot epoch.
    pub epoch: i32,
    /// Current leader from partition tag 0, when present.
    pub leader: Option<SnapshotLeader>,
    /// Total snapshot size.
    pub size: i64,
    /// Starting byte position.
    pub position: i64,
    /// Unaligned snapshot bytes. `None` is the null records field.
    pub records: Option<Vec<u8>>,
    /// Leader endpoints. Empty on version 0.
    pub endpoints: Vec<VoteNodeEndpoint>,
}

/// Encode a flexible FetchSnapshot response body.
pub fn encode_fetch_snapshot_response(body: &FetchSnapshotResponseBody) -> Result<Vec<u8>, Error> {
    if !(0..=1).contains(&body.version) || body.endpoints.len() > MAX_ARRAY {
        return Err(Error::InvalidConfig);
    }
    if body.version == 0 && !body.endpoints.is_empty() {
        return Err(Error::InvalidConfig);
    }
    if body
        .records
        .as_ref()
        .is_some_and(|bytes| bytes.len() > MAX_SNAPSHOT_BYTES)
    {
        return Err(Error::InvalidConfig);
    }
    let mut out = allocate(body.topic.len() + body.records.as_ref().map_or(0, Vec::len) + 80)?;
    out.extend_from_slice(&body.throttle_ms.to_be_bytes());
    out.extend_from_slice(&body.error_code.to_be_bytes());
    write_count(&mut out, 1, true)?;
    string(&mut out, &body.topic, true)?;
    write_count(&mut out, 1, true)?;
    out.extend_from_slice(&body.partition.to_be_bytes());
    out.extend_from_slice(&body.partition_error.to_be_bytes());
    out.extend_from_slice(&body.end_offset.to_be_bytes());
    out.extend_from_slice(&body.epoch.to_be_bytes());
    out.push(0);
    out.extend_from_slice(&body.size.to_be_bytes());
    out.extend_from_slice(&body.position.to_be_bytes());
    write_compact_bytes(&mut out, body.records.as_deref())?;
    match body.leader {
        Some(leader) => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&leader.leader_id.to_be_bytes());
            payload.extend_from_slice(&leader.leader_epoch.to_be_bytes());
            payload.push(0);
            varint(&mut out, 1);
            varint(&mut out, 0);
            varint(
                &mut out,
                u32::try_from(payload.len()).map_err(|_| Error::InvalidConfig)?,
            );
            out.extend_from_slice(&payload);
        }
        None => out.push(0),
    }
    out.push(0);
    if body.version == 0 {
        out.push(0);
    } else {
        write_tagged_endpoints(&mut out, &body.endpoints)?;
    }
    Ok(out)
}

/// Decode a body produced by [`encode_fetch_snapshot_response`].
pub fn decode_fetch_snapshot_response(
    bytes: &[u8],
    version: i16,
) -> Result<FetchSnapshotResponseBody, Error> {
    if !(0..=1).contains(&version) {
        return Err(Error::InvalidConfig);
    }
    let mut reader = Reader::new(bytes, true, protocol::Limits::default());
    let throttle_ms = reader.i32()?;
    let error_code = reader.i16()?;
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let topic = reader.string()?.to_owned();
    if reader.count()? != 1 {
        return Err(Error::InvalidConfig);
    }
    let partition = reader.i32()?;
    let partition_error = reader.i16()?;
    let end_offset = reader.i64()?;
    let epoch = reader.i32()?;
    reader.tags()?;
    let size = reader.i64()?;
    let position = reader.i64()?;
    let records = read_compact_bytes(&mut reader)?;
    let leader = read_snapshot_leader(&mut reader)?;
    reader.tags()?;
    let endpoints = if version == 0 {
        reader.tags()?;
        Vec::new()
    } else {
        read_vote_endpoints(&mut reader)?
    };
    reader.finish()?;
    Ok(FetchSnapshotResponseBody {
        version,
        throttle_ms,
        error_code,
        topic,
        partition,
        partition_error,
        end_offset,
        epoch,
        leader,
        size,
        position,
        records,
        endpoints,
    })
}

/// Accept a FetchSnapshot response without installing its bytes.
///
/// Version 1 requires one endpoint naming the tagged leader. The caller must
/// not treat `NotAdvertised` as permission to publish a snapshot.
pub fn decide_fetch_snapshot_response(
    persistent: &election::PersistentState,
    body: &FetchSnapshotResponseBody,
) -> VoteDisposition {
    let _ = persistent;
    if !(0..=1).contains(&body.version) {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    }
    let leader_id = body.leader.map(|leader| leader.leader_id).unwrap_or(-1);
    if body.version == 0 || leader_id < 0 {
        return if body.endpoints.is_empty() {
            VoteDisposition::NotAdvertised
        } else {
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        };
    }
    match body.endpoints.as_slice() {
        [endpoint] if endpoint.node_id == leader_id && !endpoint.host.is_empty() => {
            VoteDisposition::NotAdvertised
        }
        _ => VoteDisposition::Refused(PreVoteRefusal::StaleDirectory),
    }
}

fn write_compact_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) -> Result<(), Error> {
    let Some(bytes) = bytes else {
        out.push(0);
        return Ok(());
    };
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(Error::InvalidConfig);
    }
    varint(
        out,
        u32::try_from(bytes.len()).map_err(|_| Error::InvalidConfig)? + 1,
    );
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_compact_bytes(reader: &mut Reader<'_>) -> Result<Option<Vec<u8>>, Error> {
    let n = i64::from(reader.varint()?) - 1;
    if n == -1 {
        return Ok(None);
    }
    if n < 0 || n > i64::try_from(MAX_SNAPSHOT_BYTES).unwrap_or(0) {
        return Err(Error::InvalidConfig);
    }
    let len = usize::try_from(n).map_err(|_| Error::InvalidConfig)?;
    Ok(Some(reader.take(len)?.to_vec()))
}

fn read_snapshot_leader(reader: &mut Reader<'_>) -> Result<Option<SnapshotLeader>, Error> {
    let count = reader.varint()?;
    if count == 0 {
        return Ok(None);
    }
    if count != 1 || reader.varint()? != 0 {
        return Err(Error::InvalidConfig);
    }
    let length = usize::try_from(reader.varint()?).map_err(|_| Error::InvalidConfig)?;
    let payload = reader.take(length)?;
    let mut nested = Reader::new(payload, true, protocol::Limits::default());
    let leader_id = nested.i32()?;
    let leader_epoch = nested.i32()?;
    nested.tags()?;
    nested.finish()?;
    Ok(Some(SnapshotLeader {
        leader_id,
        leader_epoch,
    }))
}

fn write_nullable(out: &mut Vec<u8>, value: Option<&str>, flexible: bool) -> Result<(), Error> {
    match value {
        Some(text) => string(out, text, flexible),
        None if flexible => {
            out.push(0);
            Ok(())
        }
        None => {
            out.extend_from_slice(&(-1_i16).to_be_bytes());
            Ok(())
        }
    }
}

fn write_count(out: &mut Vec<u8>, count: u32, flexible: bool) -> Result<(), Error> {
    if flexible {
        varint(out, count.saturating_add(1));
    } else {
        out.extend_from_slice(
            &i32::try_from(count)
                .map_err(|_| Error::InvalidConfig)?
                .to_be_bytes(),
        );
    }
    Ok(())
}

type ControllerReply = Result<(Vec<u8>, Vec<u8>), Error>;

struct ControllerJob {
    request: Vec<u8>,
    spare: Vec<u8>,
    reply: oneshot::Sender<ControllerReply>,
}

enum Command {
    Request(ControllerJob),
    Stop,
}
/// Async transport handler with one joined bounded durable election actor.
pub struct ControllerHandler {
    sender: mpsc::Sender<Command>,
    stopping: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
    config: Config,
}
impl ControllerHandler {
    /// Open and synchronize storage on a blocking thread before returning ready.
    pub async fn open(path: impl AsRef<Path>, config: Config) -> Result<Self, Error> {
        config.validate()?;
        let path = path.as_ref().to_owned();
        let local_config = config.clone();
        let controller =
            tokio::task::spawn_blocking(move || Controller::open(path, local_config, 0))
                .await
                .map_err(|_| Error::Actor)??;
        let (sender, mut receiver) = mpsc::channel(config.max_queued_requests);
        let stopping = Arc::new(AtomicBool::new(false));
        let local_stop = stopping.clone();
        let start = Instant::now();
        let join = tokio::task::spawn_blocking(move || {
            let mut controller = controller;
            while let Some(command) = receiver.blocking_recv() {
                if local_stop.load(Ordering::Acquire) {
                    break;
                }
                match command {
                    Command::Stop => break,
                    Command::Request(job) => {
                        let ControllerJob {
                            request,
                            spare,
                            reply,
                        } = job;
                        if reply.is_closed() {
                            continue;
                        }
                        let result = u64::try_from(start.elapsed().as_millis())
                            .map_err(|_| Error::EpochOverflow)
                            .and_then(|now| controller.respond_into(&request, now, spare))
                            .map(|response| (response, request));
                        drop(reply.send(result));
                    }
                }
            }
        });
        Ok(Self {
            sender,
            stopping,
            join: Mutex::new(Some(join)),
            config,
        })
    }
    /// Reject admission, discard pending work and join the one owned actor.
    ///
    /// A synchronization already in progress completes with an ambiguous client
    /// outcome if canceled. No network listener or outbound peer task is owned.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.stopping.store(true, Ordering::Release);
        drop(self.sender.try_send(Command::Stop));
        let mut join = self.join.lock().await;
        if let Some(task) = join.as_mut() {
            task.await.map_err(|_| Error::Actor)?;
        }
        *join = None;
        Ok(())
    }
}
impl ControllerHandler {
    async fn submit(&self, request: Vec<u8>, spare: Vec<u8>) -> ControllerReply {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if request.len() > self.config.protocol_limits.max_request_bytes() {
            return Err(protocol::Error::RequestTooLarge.into());
        }
        let (sender, receiver) = oneshot::channel();
        self.sender
            .try_send(Command::Request(ControllerJob {
                request,
                spare,
                reply: sender,
            }))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => Error::Stopped,
            })?;
        receiver.await.map_err(|_| Error::Actor)?
    }
}
impl Handler for ControllerHandler {
    type Error = Error;
    async fn handle(&self, request: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        let (response, _) = self.submit(request, Vec::new()).await?;
        Ok(Some(response))
    }

    async fn exchange_request(
        &self,
        _peer: &Peer,
        request: Vec<u8>,
        spare: Vec<u8>,
    ) -> Result<ExchangeOutcome, Error> {
        let (response, request) = self.submit(request, spare).await?;
        Ok(ExchangeOutcome {
            response: Some(response),
            request,
            spare: Vec::new(),
        })
    }
}
impl Drop for ControllerHandler {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        drop(self.sender.try_send(Command::Stop));
    }
}

/// A directory-qualified pre-vote. Granting it does not persist a term or vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryPreVote {
    /// Candidate term. It is not written to stable state.
    pub term: u64,
    /// Candidate id.
    pub candidate: election::NodeId,
    /// Candidate directory identity. The zero id is refused.
    pub directory: [u8; 16],
    /// Candidate durable log summary.
    pub log: election::LogPosition,
}

/// Why a pre-vote was not granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreVoteRefusal {
    /// The directory does not match the expected identity.
    StaleDirectory,
    /// The request named a cluster id other than this node's.
    ///
    /// A missing cluster id is accepted, matching Apache `hasValidClusterId`.
    InconsistentCluster,
    /// The request names a topic or partition other than this metadata log.
    ///
    /// Apache returns `INVALID_REQUEST` for Vote, BeginQuorumEpoch, and
    /// EndQuorumEpoch. The check runs after the cluster id and before the
    /// directory.
    InvalidTopic,
    /// The candidate term is older than the stable term.
    StaleTerm,
    /// The candidate log is behind the stable log.
    StaleLog,
    /// This epoch already recorded a vote for a different replica.
    ///
    /// A standard vote at the local epoch is refused. A higher epoch is not
    /// this case: adopting it would clear the prior vote, and that write stays
    /// unadvertised.
    AlreadyVoted,
    /// This replica will not grant a same-epoch vote.
    ///
    /// A leader rejects every same-epoch vote. A candidate rejects a standard
    /// vote. A follower who already names a leader rejects both, matching
    /// Apache `FollowerState` after it has fetched. A higher epoch is not this
    /// case: Apache becomes unattached before the grant check, and this path
    /// does not persist that transition.
    HasLeader,
    /// The replica is not the leader.
    ///
    /// Apache `validateLeaderOnlyRequest` returns `NOT_LEADER_OR_FOLLOWER`
    /// after the epoch matches. DescribeQuorum uses the same check after the
    /// topic matches. The refusal does not change membership or serve bytes.
    NotLeader,
    /// Removing this voter would leave the quorum empty.
    ///
    /// Apache `VoterSet.removeVoter` keeps a one-voter set. The membership
    /// record is unchanged.
    LastVoter,
    /// The candidate's last offset epoch or end offset is illegal.
    ///
    /// A negative end is refused. Offset 0 is legal only at epoch 0. A
    /// standard vote must name a last offset epoch strictly older than the
    /// candidate epoch. A pre-vote may name the same epoch, but not a newer one.
    IllegalEpoch,
    /// The request is addressed to a different replica.
    ///
    /// Apache `isValidVoterKey` returns `INVALID_VOTER_KEY` (125). A negative
    /// voter id is untargeted and is not this refusal. A zero directory is the
    /// id-only key and is compared by id alone.
    InvalidVoterKey,
}

/// Whether Apache would reject this Vote body as `INVALID_REQUEST`.
/// Apache fences a Begin/End epoch older than the local quorum epoch.
///
/// Internal term 0 has no wire epoch yet, so nothing is older than it.
fn quorum_epoch_fenced(persistent: &election::PersistentState, leader_epoch: i32) -> bool {
    wire_epoch(persistent.term).is_ok_and(|current| leader_epoch < current)
}

/// Apache `validateLeaderOnlyRequest`: the caller's leader epoch must equal
/// the local epoch. Older is [`PreVoteRefusal::StaleTerm`]. Newer or negative
/// is [`PreVoteRefusal::IllegalEpoch`].
fn leader_epoch_refusal(
    persistent: &election::PersistentState,
    epoch: i32,
) -> Option<PreVoteRefusal> {
    if epoch < 0 {
        return Some(PreVoteRefusal::IllegalEpoch);
    }
    if quorum_epoch_fenced(persistent, epoch) {
        return Some(PreVoteRefusal::StaleTerm);
    }
    if wire_epoch(persistent.term).is_ok_and(|current| epoch > current) {
        return Some(PreVoteRefusal::IllegalEpoch);
    }
    None
}

fn metadata_refusal(
    metadata: MetadataLog<'_>,
    cluster_id: Option<&str>,
    topic: &str,
    partition: i32,
) -> Option<PreVoteRefusal> {
    if let Some(refusal) = inconsistent_cluster(metadata.cluster_id, cluster_id) {
        return Some(refusal);
    }
    if topic != metadata.topic || partition != metadata.partition {
        return Some(PreVoteRefusal::InvalidTopic);
    }
    None
}

fn inconsistent_cluster(expected: &str, presented: Option<&str>) -> Option<PreVoteRefusal> {
    match presented {
        None => None,
        Some(id) if id == expected => None,
        Some(_) => Some(PreVoteRefusal::InconsistentCluster),
    }
}

/// Apache `isValidVoterKey` for Vote and BeginQuorumEpoch.
///
/// A negative voter id is untargeted and accepted. A non-negative id must be
/// this replica. A zero directory is `ReplicaKey.NO_DIRECTORY_ID` and is not
/// compared. Any other directory must be this replica's directory.
fn addressed_voter_refusal(
    local_id: i32,
    local_directory: [u8; 16],
    voter_id: i32,
    voter_directory: Option<[u8; 16]>,
) -> Option<PreVoteRefusal> {
    if voter_id < 0 {
        return None;
    }
    if voter_id != local_id {
        return Some(PreVoteRefusal::InvalidVoterKey);
    }
    if let Some(directory) = voter_directory {
        if directory != [0; 16] && directory != local_directory {
            return Some(PreVoteRefusal::InvalidVoterKey);
        }
    }
    None
}

/// Same-epoch votes a leader, candidate, or following replica will not grant.
fn established_quorum_refusal(
    quorum: VoteQuorum,
    persistent: &election::PersistentState,
    body: &VoteBody,
) -> Option<PreVoteRefusal> {
    let same_epoch = wire_epoch(persistent.term).is_ok_and(|current| body.replica_epoch == current);
    if !same_epoch {
        return None;
    }
    let blocked = match quorum.role {
        election::Role::Leader => true,
        election::Role::Candidate => !body.pre_vote,
        election::Role::Follower => quorum.leader.is_some(),
    };
    blocked.then_some(PreVoteRefusal::HasLeader)
}

fn voter_request_refusal(
    persistent: &election::PersistentState,
    body: &VoteBody,
) -> Option<PreVoteRefusal> {
    if body.replica_id < 0 {
        return Some(PreVoteRefusal::IllegalEpoch);
    }
    if body.replica_epoch < 0 || quorum_epoch_fenced(persistent, body.replica_epoch) {
        return Some(PreVoteRefusal::StaleTerm);
    }
    None
}

fn candidate_log(body: &VoteBody) -> Result<election::LogPosition, PreVoteRefusal> {
    log_position(body.last_offset_epoch, body.last_offset).map_err(|_| PreVoteRefusal::IllegalEpoch)
}

/// Apache `unattachedOrProspectiveCanGrantVote` for a standard vote, without
/// writing the vote a grant would persist.
fn standard_vote_grant_refusal(
    persistent: &election::PersistentState,
    body: &VoteBody,
) -> Option<PreVoteRefusal> {
    let log = match candidate_log(body) {
        Ok(log) => log,
        Err(refusal) => return Some(refusal),
    };
    if log < persistent.log {
        return Some(PreVoteRefusal::StaleLog);
    }
    let same_epoch = wire_epoch(persistent.term).is_ok_and(|current| body.replica_epoch == current);
    if same_epoch
        && persistent
            .voted_for
            .is_some_and(|prior| i32::try_from(prior).ok() != Some(body.replica_id))
    {
        return Some(PreVoteRefusal::AlreadyVoted);
    }
    None
}

fn vote_offset_epoch_illegal(body: &VoteBody) -> bool {
    if body.last_offset < 0 || body.last_offset_epoch < 0 {
        return true;
    }
    // Offset 0 is only the empty log at epoch 0. A non-zero epoch at offset 0
    // is the same invalid coordinate the v0 Vote handler rejects. Treating it
    // as an empty log would grant a pre-vote the durable path cannot represent.
    if body.last_offset == 0 && body.last_offset_epoch != 0 {
        return true;
    }
    if body.pre_vote {
        body.last_offset_epoch > body.replica_epoch
    } else {
        body.last_offset_epoch >= body.replica_epoch
    }
}

/// Decide a pre-vote without changing `persistent`.
///
/// The stable term and vote stay with the caller. A pre-vote may name the
/// same log epoch as the candidate epoch, and it may not name a newer one.
/// An empty log is only `(0, 0)`. Wire versions above the advertised Vote v0
/// profile are not selected by [`negotiate`].
pub fn evaluate_prevote(
    persistent: &election::PersistentState,
    expected_directory: [u8; 16],
    request: DirectoryPreVote,
) -> Result<bool, PreVoteRefusal> {
    if request.directory == [0; 16] || request.directory != expected_directory {
        return Err(PreVoteRefusal::StaleDirectory);
    }
    if request.term < persistent.term {
        return Err(PreVoteRefusal::StaleTerm);
    }
    if (request.log.term == 0) != (request.log.index == 0) || request.log.term > request.term {
        return Err(PreVoteRefusal::IllegalEpoch);
    }
    if request.log < persistent.log {
        return Err(PreVoteRefusal::StaleLog);
    }
    Ok(true)
}

#[cfg(test)]
mod response_buffer_tests {
    use super::*;

    fn state() -> State {
        State {
            persistent: election::PersistentState {
                term: 1,
                voted_for: None,
                log: election::LogPosition { term: 0, index: 0 },
            },
            role: Role::Follower,
            leader: None,
            deadline_ms: 0,
            granted_votes: 0,
            poisoned: false,
        }
    }

    #[test]
    fn a_controller_response_reuses_its_buffer() {
        let config =
            Config::new(1, vec![1], "cluster".into()).unwrap_or_else(|error| panic!("{error:?}"));
        let mut spare = Vec::new();
        spare
            .try_reserve_exact(128)
            .unwrap_or_else(|error| panic!("{error}"));
        let capacity = spare.capacity();
        let first = response_into(53, 7, 0, None, &config, state(), spare)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(first.capacity() >= capacity);
        assert!(!first.is_empty());
        let kept = first.capacity();
        let second = response_into(53, 8, 0, None, &config, state(), first)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(second.capacity() >= kept);
        assert!(!second.is_empty());
    }
}

#[cfg(test)]
mod prevote_tests {
    use super::*;

    fn metadata_log() -> MetadataLog<'static> {
        MetadataLog {
            cluster_id: "cluster-1",
            topic: "__cluster_metadata",
            partition: 0,
        }
    }

    fn state(term: u64) -> election::PersistentState {
        election::PersistentState {
            term,
            voted_for: Some(7),
            log: election::LogPosition { term: 1, index: 1 },
        }
    }

    #[test]
    fn prevote_does_not_change_the_stable_term_or_vote() {
        let persistent = state(4);
        let before = persistent;
        let directory = [9u8; 16];
        let granted = evaluate_prevote(
            &persistent,
            directory,
            DirectoryPreVote {
                term: 5,
                candidate: 3,
                directory,
                log: persistent.log,
            },
        )
        .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(granted);
        assert_eq!(persistent, before);
        assert_eq!(
            negotiate(
                &[ApiVersion {
                    api_key: 52,
                    min_version: 0,
                    max_version: 2,
                }],
                52,
            ),
            Some(0)
        );
    }

    #[test]
    fn stale_directory_or_term_or_log_is_refused() {
        let persistent = state(4);
        let directory = [9u8; 16];
        assert_eq!(
            evaluate_prevote(
                &persistent,
                directory,
                DirectoryPreVote {
                    term: 5,
                    candidate: 3,
                    directory: [1u8; 16],
                    log: persistent.log,
                },
            ),
            Err(PreVoteRefusal::StaleDirectory)
        );
        assert_eq!(
            evaluate_prevote(
                &persistent,
                directory,
                DirectoryPreVote {
                    term: 3,
                    candidate: 3,
                    directory,
                    log: persistent.log,
                },
            ),
            Err(PreVoteRefusal::StaleTerm)
        );
        assert_eq!(
            evaluate_prevote(
                &persistent,
                directory,
                DirectoryPreVote {
                    term: 5,
                    candidate: 3,
                    directory,
                    log: election::LogPosition { term: 0, index: 0 },
                },
            ),
            Err(PreVoteRefusal::StaleLog)
        );
        assert_eq!(persistent.voted_for, Some(7));
        assert_eq!(persistent.term, 4);
        let same_epoch = evaluate_prevote(
            &persistent,
            directory,
            DirectoryPreVote {
                term: 5,
                candidate: 3,
                directory,
                log: election::LogPosition { term: 5, index: 2 },
            },
        )
        .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(same_epoch);
        assert_eq!(
            evaluate_prevote(
                &persistent,
                directory,
                DirectoryPreVote {
                    term: 5,
                    candidate: 3,
                    directory,
                    log: election::LogPosition { term: 6, index: 2 },
                },
            ),
            Err(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(
            evaluate_prevote(
                &persistent,
                directory,
                DirectoryPreVote {
                    term: 5,
                    candidate: 3,
                    directory,
                    log: election::LogPosition { term: 0, index: 2 },
                },
            ),
            Err(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(persistent, state(4));
    }

    fn sample_body(version: i16, replica: [u8; 16], voter: [u8; 16], pre_vote: bool) -> VoteBody {
        VoteBody {
            version,
            cluster_id: Some("cluster-1".to_owned()),
            voter_id: if version == 0 { -1 } else { 7 },
            topic: "__cluster_metadata".to_owned(),
            partition: 0,
            replica_epoch: 5,
            replica_id: 3,
            replica_directory: (version >= 1).then_some(replica),
            voter_directory: (version >= 1).then_some(voter),
            last_offset_epoch: 1,
            last_offset: 1,
            pre_vote,
        }
    }

    #[test]
    fn vote_v0_matches_the_existing_request_and_v2_prevote_is_not_persisted() {
        let config = Config::new(1, vec![1, 2], "cluster-1".to_owned())
            .unwrap_or_else(|error| panic!("{error:?}"));
        let vote = election::VoteRequest {
            term: 2,
            candidate: 3,
            log: election::LogPosition { term: 1, index: 1 },
        };
        let framed = vote_request(&config, vote, 9).unwrap_or_else(|error| panic!("{error:?}"));
        let encoded = encode_vote_body(&VoteBody {
            version: 0,
            cluster_id: Some("cluster-1".to_owned()),
            voter_id: -1,
            topic: config.topic.clone(),
            partition: config.partition,
            replica_epoch: wire_epoch(vote.term).unwrap_or_else(|error| panic!("{error:?}")),
            replica_id: 3,
            replica_directory: None,
            voter_directory: None,
            last_offset_epoch: wire_epoch(vote.log.term)
                .unwrap_or_else(|error| panic!("{error:?}")),
            last_offset: 1,
            pre_vote: false,
        })
        .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(&framed[11..], encoded.as_slice());
        let replica = [4u8; 16];
        let voter = [5u8; 16];
        let body = sample_body(2, replica, voter, true);
        let bytes = encode_vote_body(&body).unwrap_or_else(|error| panic!("{error:?}"));
        let decoded = decode_vote_body(&bytes, 2).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(decoded.pre_vote);
        let persistent = state(4);
        let before = persistent;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &decoded
            ),
            VoteDisposition::PreVoteGranted
        );
        assert_eq!(persistent, before);
        let mut stale = decoded.clone();
        stale.replica_directory = Some([1u8; 16]);
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &stale
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        assert_eq!(persistent, before);
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &sample_body(1, replica, voter, false)
            ),
            VoteDisposition::NotAdvertised
        );
        assert_eq!(CONTROLLER_API_VERSIONS[1].max_version, 0);
        let mut ahead = decoded;
        ahead.last_offset_epoch = ahead.replica_epoch.saturating_add(1);
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &ahead
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut negative = sample_body(2, replica, voter, true);
        negative.last_offset = -1;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &negative
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut same_epoch = sample_body(1, replica, voter, false);
        same_epoch.last_offset_epoch = same_epoch.replica_epoch;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &same_epoch
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(persistent, before);
    }

    #[test]
    fn prevote_offset_zero_with_nonzero_epoch_is_not_an_empty_log() {
        let persistent = election::PersistentState::default();
        let before = persistent;
        let replica = [4u8; 16];
        let voter = [5u8; 16];
        let mut body = sample_body(2, replica, voter, true);
        body.replica_epoch = 5;
        body.last_offset = 0;
        body.last_offset_epoch = 3;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &body
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(persistent, before);
    }

    #[test]
    fn standard_directory_vote_refuses_a_losing_candidate_without_persisting() {
        let persistent = state(4);
        let before = persistent;
        let replica = [4u8; 16];
        let voter = [5u8; 16];
        let mut older = sample_body(1, replica, voter, false);
        older.replica_epoch = 2;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &older
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleTerm)
        );
        let mut negative_id = sample_body(2, replica, voter, true);
        negative_id.replica_id = -1;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &negative_id
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut negative_epoch = sample_body(2, replica, voter, true);
        negative_epoch.replica_epoch = -1;
        negative_epoch.last_offset_epoch = 0;
        negative_epoch.last_offset = 0;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &negative_epoch
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut behind = sample_body(1, replica, voter, false);
        behind.last_offset_epoch = 0;
        behind.last_offset = 0;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &behind
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleLog)
        );
        let mut same_epoch = sample_body(1, replica, voter, false);
        same_epoch.replica_epoch = 3;
        same_epoch.last_offset_epoch = 1;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &same_epoch
            ),
            VoteDisposition::Refused(PreVoteRefusal::AlreadyVoted)
        );
        let mut repeat = same_epoch;
        repeat.replica_id = 7;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &repeat
            ),
            VoteDisposition::NotAdvertised
        );
        assert_eq!(persistent, before);
        assert_eq!(CONTROLLER_API_VERSIONS[1].max_version, 0);
    }

    #[test]
    fn a_vote_for_another_replica_is_not_a_grant() {
        let persistent = state(4);
        let before = persistent;
        let replica = [4u8; 16];
        let voter = [5u8; 16];
        let mut misaddressed = sample_body(2, replica, voter, true);
        misaddressed.voter_id = 9;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &misaddressed
            ),
            VoteDisposition::Refused(PreVoteRefusal::InvalidVoterKey)
        );
        let mut illegal = misaddressed.clone();
        illegal.last_offset = -1;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &illegal
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut id_only = sample_body(2, replica, voter, true);
        id_only.voter_directory = Some([0u8; 16]);
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &id_only
            ),
            VoteDisposition::PreVoteGranted
        );
        let mut untargeted = sample_body(2, replica, voter, true);
        untargeted.voter_id = -1;
        untargeted.voter_directory = Some([1u8; 16]);
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &untargeted
            ),
            VoteDisposition::PreVoteGranted
        );
        let mut standard = sample_body(1, replica, voter, false);
        standard.voter_id = 4;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &standard
            ),
            VoteDisposition::Refused(PreVoteRefusal::InvalidVoterKey)
        );
        let begin = BeginEpochBody {
            version: 1,
            cluster_id: Some("cluster-1".to_owned()),
            voter_id: 4,
            topic: "__cluster_metadata".to_owned(),
            partition: 0,
            voter_directory: Some(voter),
            leader_id: 3,
            leader_epoch: 5,
            leader_endpoints: Vec::new(),
        };
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 7, voter, &begin),
            VoteDisposition::Refused(PreVoteRefusal::InvalidVoterKey)
        );
        let mut older = begin;
        older.leader_epoch = 1;
        older.voter_id = 9;
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 7, voter, &older),
            VoteDisposition::Refused(PreVoteRefusal::StaleTerm)
        );
        let mut id_only_begin = older;
        id_only_begin.voter_id = 7;
        id_only_begin.leader_epoch = 5;
        id_only_begin.voter_directory = Some([0u8; 16]);
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 7, voter, &id_only_begin),
            VoteDisposition::NotAdvertised
        );
        let mut untargeted_begin = id_only_begin;
        untargeted_begin.voter_id = -1;
        untargeted_begin.voter_directory = Some([1u8; 16]);
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 7, voter, &untargeted_begin),
            VoteDisposition::NotAdvertised
        );
        assert_eq!(persistent, before);
        assert_eq!(CONTROLLER_API_VERSIONS[1].max_version, 0);
    }

    #[test]
    fn a_follower_with_a_leader_does_not_grant_the_same_epoch() {
        let persistent = state(4);
        let before = persistent;
        let replica = [4u8; 16];
        let voter = [5u8; 16];
        let mut prevote = sample_body(2, replica, voter, true);
        prevote.replica_epoch = 3;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum {
                    role: election::Role::Follower,
                    leader: Some(1),
                },
                metadata_log(),
                replica,
                7,
                voter,
                &prevote
            ),
            VoteDisposition::Refused(PreVoteRefusal::HasLeader)
        );
        let mut higher = prevote.clone();
        higher.replica_epoch = 5;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum {
                    role: election::Role::Follower,
                    leader: Some(1),
                },
                metadata_log(),
                replica,
                7,
                voter,
                &higher
            ),
            VoteDisposition::PreVoteGranted
        );
        let mut standard = sample_body(1, replica, voter, false);
        standard.replica_epoch = 3;
        standard.replica_id = 7;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum {
                    role: election::Role::Leader,
                    leader: Some(7),
                },
                metadata_log(),
                replica,
                7,
                voter,
                &standard
            ),
            VoteDisposition::Refused(PreVoteRefusal::HasLeader)
        );
        let mut candidate = sample_body(2, replica, voter, true);
        candidate.replica_epoch = 3;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum {
                    role: election::Role::Candidate,
                    leader: None,
                },
                metadata_log(),
                replica,
                7,
                voter,
                &candidate
            ),
            VoteDisposition::PreVoteGranted
        );
        let mut candidate_standard = candidate;
        candidate_standard.pre_vote = false;
        candidate_standard.version = 1;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum {
                    role: election::Role::Candidate,
                    leader: None,
                },
                metadata_log(),
                replica,
                7,
                voter,
                &candidate_standard
            ),
            VoteDisposition::Refused(PreVoteRefusal::HasLeader)
        );
        assert_eq!(persistent, before);
        assert_eq!(CONTROLLER_API_VERSIONS[1].max_version, 0);
    }

    #[test]
    fn a_foreign_cluster_id_is_refused_before_directory_or_epoch() {
        let persistent = state(4);
        let before = persistent;
        let replica = [4u8; 16];
        let voter = [5u8; 16];
        let mut foreign = sample_body(2, replica, voter, true);
        foreign.cluster_id = Some("other".to_owned());
        foreign.replica_directory = Some([1u8; 16]);
        foreign.replica_epoch = 1;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &foreign
            ),
            VoteDisposition::Refused(PreVoteRefusal::InconsistentCluster)
        );
        let mut absent = sample_body(2, replica, voter, true);
        absent.cluster_id = None;
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &absent
            ),
            VoteDisposition::PreVoteGranted
        );
        let begin = BeginEpochBody {
            version: 1,
            cluster_id: Some("other".to_owned()),
            voter_id: 7,
            topic: "__cluster_metadata".to_owned(),
            partition: 0,
            voter_directory: Some([1u8; 16]),
            leader_id: 4,
            leader_epoch: 1,
            leader_endpoints: Vec::new(),
        };
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 7, voter, &begin),
            VoteDisposition::Refused(PreVoteRefusal::InconsistentCluster)
        );
        let end = EndEpochBody {
            version: 1,
            cluster_id: Some("other".to_owned()),
            topic: "__cluster_metadata".to_owned(),
            partition: 0,
            leader_id: 4,
            leader_epoch: 1,
            preferred: Vec::new(),
            leader_endpoints: Vec::new(),
        };
        let (disposition, rank) = decide_end_body(&persistent, metadata_log(), 7, voter, &end)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            disposition,
            VoteDisposition::Refused(PreVoteRefusal::InconsistentCluster)
        );
        assert_eq!(rank, None);
        let mut wrong_topic = sample_body(2, replica, voter, true);
        wrong_topic.topic = "orders".to_owned();
        wrong_topic.replica_directory = Some([1u8; 16]);
        assert_eq!(
            decide_vote_body(
                &persistent,
                VoteQuorum::unattached(),
                metadata_log(),
                replica,
                7,
                voter,
                &wrong_topic
            ),
            VoteDisposition::Refused(PreVoteRefusal::InvalidTopic)
        );
        let mut wrong_partition = begin;
        wrong_partition.cluster_id = Some("cluster-1".to_owned());
        wrong_partition.voter_directory = Some(voter);
        wrong_partition.partition = 4;
        wrong_partition.leader_epoch = 2;
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 7, voter, &wrong_partition),
            VoteDisposition::Refused(PreVoteRefusal::InvalidTopic)
        );
        let mut snapshot = FetchSnapshotBody {
            version: 1,
            cluster_id: Some("cluster-1".to_owned()),
            replica_id: 3,
            max_bytes: 1024,
            topic: "orders".to_owned(),
            partition: 0,
            current_leader_epoch: 3,
            end_offset: 10,
            epoch: 2,
            position: 0,
            replica_directory: Some(voter),
        };
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                voter,
                &snapshot
            ),
            VoteDisposition::Refused(PreVoteRefusal::InvalidTopic)
        );
        snapshot.topic = "__cluster_metadata".to_owned();
        snapshot.cluster_id = Some("other".to_owned());
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                voter,
                &snapshot
            ),
            VoteDisposition::Refused(PreVoteRefusal::InconsistentCluster)
        );
        assert_eq!(persistent, before);
    }

    #[test]
    fn end_rejects_a_self_or_duplicate_successor_list() {
        let persistent = state(4);
        let before = persistent;
        let directory = [8u8; 16];
        let mut body = EndEpochBody {
            version: 1,
            cluster_id: Some("cluster-1".to_owned()),
            topic: "__cluster_metadata".to_owned(),
            partition: 0,
            leader_id: 7,
            leader_epoch: 3,
            preferred: vec![PreferredCandidate {
                id: 7,
                directory: Some(directory),
            }],
            leader_endpoints: Vec::new(),
        };
        let (disposition, rank) = decide_end_body(&persistent, metadata_log(), 7, directory, &body)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            disposition,
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(rank, None);
        body.leader_id = 4;
        body.preferred = vec![
            PreferredCandidate {
                id: 4,
                directory: None,
            },
            PreferredCandidate {
                id: 4,
                directory: Some(directory),
            },
        ];
        let (disposition, rank) = decide_end_body(&persistent, metadata_log(), 7, directory, &body)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            disposition,
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(rank, None);
        body.preferred = vec![PreferredCandidate {
            id: -1,
            directory: None,
        }];
        let (disposition, rank) = decide_end_body(&persistent, metadata_log(), 7, directory, &body)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            disposition,
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(rank, None);
        body.preferred = vec![PreferredCandidate {
            id: 7,
            directory: Some(directory),
        }];
        let (disposition, rank) = decide_end_body(&persistent, metadata_log(), 7, directory, &body)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(disposition, VoteDisposition::NotAdvertised);
        assert_eq!(rank, Some(0));
        assert_eq!(persistent, before);
        assert_eq!(CONTROLLER_API_VERSIONS[3].max_version, 0);
    }

    #[test]
    fn a_stale_successor_directory_is_not_rank_zero() {
        let directory = [8u8; 16];
        let version0 = [
            PreferredCandidate {
                id: 4,
                directory: None,
            },
            PreferredCandidate {
                id: 7,
                directory: None,
            },
        ];
        assert_eq!(
            preferred_rank(&version0, 7, directory).unwrap_or_else(|error| panic!("{error:?}")),
            Some(1)
        );
        let stale = [PreferredCandidate {
            id: 7,
            directory: Some([1u8; 16]),
        }];
        assert_eq!(
            preferred_rank(&stale, 7, directory).unwrap_or_else(|error| panic!("{error:?}")),
            None
        );
        assert_eq!(
            successor_backoff(1000, stale.len(), None).unwrap_or_else(|error| panic!("{error:?}")),
            1000
        );
        let matched = [PreferredCandidate {
            id: 7,
            directory: Some(directory),
        }];
        assert_eq!(
            preferred_rank(&matched, 7, directory).unwrap_or_else(|error| panic!("{error:?}")),
            Some(0)
        );
        assert_eq!(
            successor_backoff(1000, matched.len(), Some(0))
                .unwrap_or_else(|error| panic!("{error:?}")),
            0
        );
    }

    #[test]
    fn begin_v0_parses_and_v1_directory_mismatch_does_not_change_state() {
        let config = Config::new(1, vec![1], "cluster-1".to_owned())
            .unwrap_or_else(|error| panic!("{error:?}"));
        let classic = BeginEpochBody {
            version: 0,
            cluster_id: Some(config.cluster_id.clone()),
            voter_id: -1,
            topic: config.topic.clone(),
            partition: config.partition,
            voter_directory: None,
            leader_id: 4,
            leader_epoch: 3,
            leader_endpoints: Vec::new(),
        };
        let bytes = encode_begin_body(&classic).unwrap_or_else(|error| panic!("{error:?}"));
        let parsed = parse_request(53, &bytes, &config).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(parsed.node, 4);
        assert_eq!(parsed.epoch, 3);
        let directory = [6u8; 16];
        let modern = BeginEpochBody {
            version: 1,
            voter_id: 1,
            voter_directory: Some(directory),
            leader_endpoints: vec![LeaderEndpoint {
                name: "plaintext".to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 9093,
            }],
            ..classic
        };
        let encoded = encode_begin_body(&modern).unwrap_or_else(|error| panic!("{error:?}"));
        let decoded = decode_begin_body(&encoded, 1).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(decoded.leader_endpoints.len(), 1);
        assert_eq!(decoded.leader_endpoints[0].port, 9093);
        let persistent = state(4);
        let before = persistent;
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 1, directory, &decoded),
            VoteDisposition::NotAdvertised
        );
        let mut stale = decoded.clone();
        stale.voter_directory = Some([2u8; 16]);
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 1, directory, &stale),
            VoteDisposition::Refused(PreVoteRefusal::InvalidVoterKey)
        );
        assert_eq!(persistent, before);
        let mut older = decoded;
        older.leader_epoch = 2;
        assert_eq!(
            decide_begin_body(&persistent, metadata_log(), 1, directory, &older),
            VoteDisposition::Refused(PreVoteRefusal::StaleTerm)
        );
        assert_eq!(persistent, before);
        assert_eq!(CONTROLLER_API_VERSIONS[2].api_key, 53);
        assert_eq!(CONTROLLER_API_VERSIONS[2].max_version, 0);
    }

    #[test]
    fn end_v0_parses_and_v1_stale_directory_is_not_a_successor() {
        let config = Config::new(7, vec![4, 7], "cluster-1".to_owned())
            .unwrap_or_else(|error| panic!("{error:?}"));
        let classic = EndEpochBody {
            version: 0,
            cluster_id: Some(config.cluster_id.clone()),
            topic: config.topic.clone(),
            partition: config.partition,
            leader_id: 4,
            leader_epoch: 2,
            preferred: vec![
                PreferredCandidate {
                    id: 4,
                    directory: None,
                },
                PreferredCandidate {
                    id: 7,
                    directory: None,
                },
            ],
            leader_endpoints: Vec::new(),
        };
        let bytes = encode_end_body(&classic).unwrap_or_else(|error| panic!("{error:?}"));
        let parsed = parse_request(54, &bytes, &config).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(parsed.node, 4);
        assert_eq!(parsed.epoch, 2);
        assert_eq!(parsed.successor_count, 2);
        assert_eq!(parsed.successors[1], 7);
        let directory = [8u8; 16];
        let mut modern = EndEpochBody {
            version: 1,
            preferred: vec![PreferredCandidate {
                id: 7,
                directory: Some([1u8; 16]),
            }],
            leader_endpoints: vec![LeaderEndpoint {
                name: "plaintext".to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 9093,
            }],
            ..classic
        };
        modern.leader_epoch = 3;
        let encoded = encode_end_body(&modern).unwrap_or_else(|error| panic!("{error:?}"));
        let decoded = decode_end_body(&encoded, 1).unwrap_or_else(|error| panic!("{error:?}"));
        let persistent = state(4);
        let before = persistent;
        let (disposition, rank) =
            decide_end_body(&persistent, metadata_log(), 7, directory, &decoded)
                .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            disposition,
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        assert_eq!(rank, None);
        let mut older = decoded;
        older.leader_epoch = 2;
        let (disposition, rank) =
            decide_end_body(&persistent, metadata_log(), 7, directory, &older)
                .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            disposition,
            VoteDisposition::Refused(PreVoteRefusal::StaleTerm)
        );
        assert_eq!(rank, None);
        assert_eq!(persistent, before);
        assert_eq!(CONTROLLER_API_VERSIONS[3].api_key, 54);
        assert_eq!(CONTROLLER_API_VERSIONS[3].max_version, 0);
    }

    #[test]
    fn fetch_snapshot_v1_directory_mismatch_is_not_advertised() {
        let directory = [7u8; 16];
        let body = FetchSnapshotBody {
            version: 1,
            cluster_id: Some("cluster-1".to_owned()),
            replica_id: 3,
            max_bytes: 1024,
            topic: "__cluster_metadata".to_owned(),
            partition: 0,
            current_leader_epoch: 3,
            end_offset: 10,
            epoch: 2,
            position: 0,
            replica_directory: Some(directory),
        };
        let bytes = encode_fetch_snapshot_body(&body).unwrap_or_else(|error| panic!("{error:?}"));
        let decoded =
            decode_fetch_snapshot_body(&bytes, 1).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(decoded.cluster_id.as_deref(), Some("cluster-1"));
        assert_eq!(decoded.end_offset, 10);
        let persistent = state(4);
        let before = persistent;
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                directory,
                &decoded
            ),
            VoteDisposition::NotAdvertised
        );
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Follower,
                metadata_log(),
                directory,
                &decoded
            ),
            VoteDisposition::Refused(PreVoteRefusal::NotLeader)
        );
        let mut stale = decoded.clone();
        stale.replica_directory = Some([1u8; 16]);
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                directory,
                &stale
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        let mut older = decoded.clone();
        older.current_leader_epoch = 2;
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                directory,
                &older
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleTerm)
        );
        let mut resume = decoded.clone();
        resume.position = -1;
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                directory,
                &resume
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut fenced_resume = resume;
        fenced_resume.current_leader_epoch = 2;
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                directory,
                &fenced_resume
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleTerm)
        );
        let mut newer = decoded;
        newer.current_leader_epoch = 4;
        assert_eq!(
            decide_fetch_snapshot_body(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                directory,
                &newer
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(persistent, before);
        assert!(CONTROLLER_API_VERSIONS.iter().all(|api| api.api_key != 59));
        assert_eq!(
            negotiate(
                &[ApiVersion {
                    api_key: 59,
                    min_version: 0,
                    max_version: 1
                }],
                59
            ),
            None
        );
    }

    #[test]
    fn vote_response_v0_matches_the_handler_and_v1_endpoint_is_not_applied() {
        let config = Config::new(1, vec![1, 2], "cluster-1".to_owned())
            .unwrap_or_else(|error| panic!("{error:?}"));
        let election = election::State {
            persistent: state(4),
            role: election::Role::Follower,
            leader: Some(4),
            deadline_ms: 0,
            granted_votes: 0,
            poisoned: false,
        };
        let framed = response(52, 9, 0, Some((0, true)), &config, election)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let classic = VoteResponseBody {
            version: 0,
            error_code: 0,
            topic: config.topic.clone(),
            partition: config.partition,
            partition_error: 0,
            leader_id: 4,
            leader_epoch: wire_epoch(4).unwrap_or_else(|error| panic!("{error:?}")),
            vote_granted: true,
            endpoints: Vec::new(),
        };
        let encoded = encode_vote_response(&classic).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(&framed[5..], encoded.as_slice());
        let modern = VoteResponseBody {
            version: 1,
            endpoints: vec![VoteNodeEndpoint {
                node_id: 4,
                host: "127.0.0.1".to_owned(),
                port: 9093,
            }],
            ..classic
        };
        let bytes = encode_vote_response(&modern).unwrap_or_else(|error| panic!("{error:?}"));
        let decoded = decode_vote_response(&bytes, 1).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(decoded.endpoints.len(), 1);
        assert_eq!(decoded.endpoints[0].port, 9093);
        let persistent = state(4);
        let before = persistent;
        assert_eq!(
            decide_vote_response(&persistent, false, &decoded),
            VoteDisposition::NotAdvertised
        );
        assert_eq!(
            decide_vote_response(&persistent, true, &decoded),
            VoteDisposition::NotAdvertised
        );
        let mut unsupported = decoded.clone();
        unsupported.error_code = UNSUPPORTED_VERSION;
        assert_eq!(
            decide_vote_response(&persistent, true, &unsupported),
            VoteDisposition::FallBackToStandardVote
        );
        assert_eq!(
            decide_vote_response(&persistent, false, &unsupported),
            VoteDisposition::NotAdvertised
        );
        let mut classic_unsupported = decoded.clone();
        classic_unsupported.version = 0;
        classic_unsupported.error_code = UNSUPPORTED_VERSION;
        classic_unsupported.endpoints.clear();
        assert_eq!(
            decide_vote_response(&persistent, true, &classic_unsupported),
            VoteDisposition::FallBackToStandardVote
        );
        assert_eq!(
            decide_vote_response(&persistent, false, &classic_unsupported),
            VoteDisposition::DurableV0
        );
        let mut stale = decoded;
        stale.endpoints[0].node_id = 9;
        assert_eq!(
            decide_vote_response(&persistent, false, &stale),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        assert_eq!(persistent, before);
        assert_eq!(CONTROLLER_API_VERSIONS[1].api_key, 52);
        assert_eq!(CONTROLLER_API_VERSIONS[1].max_version, 0);
        assert_eq!(
            negotiate(
                &[ApiVersion {
                    api_key: 52,
                    min_version: 0,
                    max_version: 2
                }],
                52
            ),
            Some(0)
        );
    }

    #[test]
    fn describe_quorum_v2_directory_mismatch_is_not_advertised() {
        let directory = [8u8; 16];
        let replica = QuorumReplica {
            replica_id: 7,
            directory: Some(directory),
            log_end_offset: 10,
            last_fetch_timestamp: 1,
            last_caught_up_timestamp: 2,
        };
        let body = DescribeQuorumBody {
            version: 2,
            error_code: 0,
            error_message: None,
            topic: "__cluster_metadata".to_owned(),
            partition: 0,
            partition_error: 0,
            partition_error_message: None,
            leader_id: 7,
            leader_epoch: 3,
            high_watermark: 9,
            voters: vec![replica],
            observers: Vec::new(),
            nodes: vec![QuorumNode {
                node_id: 7,
                listeners: vec![LeaderEndpoint {
                    name: "plaintext".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 9093,
                }],
            }],
        };
        let bytes = encode_describe_quorum(&body).unwrap_or_else(|error| panic!("{error:?}"));
        let decoded = decode_describe_quorum(&bytes, 2).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(decoded.high_watermark, 9);
        assert_eq!(decoded.voters[0].directory, Some(directory));
        assert_eq!(decoded.nodes[0].listeners[0].port, 9093);
        let persistent = state(4);
        let before = persistent;
        assert_eq!(
            decide_describe_quorum(&persistent, 7, directory, &decoded),
            VoteDisposition::NotAdvertised
        );
        let mut stale = decoded;
        stale.voters[0].directory = Some([1u8; 16]);
        assert_eq!(
            decide_describe_quorum(&persistent, 7, directory, &stale),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        assert_eq!(persistent, before);
        let request = DescribeQuorumRequestBody {
            version: 2,
            topic: "__cluster_metadata".to_owned(),
            partitions: vec![0],
        };
        assert_eq!(
            decide_describe_quorum_request(
                &persistent,
                election::Role::Leader,
                "__cluster_metadata",
                0,
                &request
            ),
            VoteDisposition::NotAdvertised
        );
        assert_eq!(
            decide_describe_quorum_request(
                &persistent,
                election::Role::Follower,
                "__cluster_metadata",
                0,
                &request
            ),
            VoteDisposition::Refused(PreVoteRefusal::NotLeader)
        );
        let mut other = request;
        other.topic = "orders".to_owned();
        assert_eq!(
            decide_describe_quorum_request(
                &persistent,
                election::Role::Follower,
                "__cluster_metadata",
                0,
                &other
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        assert!(CONTROLLER_API_VERSIONS.iter().all(|api| api.api_key != 55));
    }

    #[test]
    fn epoch_response_v0_matches_the_handler_and_v1_endpoint_is_not_applied() {
        let config = Config::new(1, vec![1, 2], "cluster-1".to_owned())
            .unwrap_or_else(|error| panic!("{error:?}"));
        let election = election::State {
            persistent: state(4),
            role: election::Role::Follower,
            leader: Some(4),
            deadline_ms: 0,
            granted_votes: 0,
            poisoned: false,
        };
        let classic = EpochResponseBody {
            version: 0,
            error_code: 0,
            topic: config.topic.clone(),
            partition: config.partition,
            partition_error: 0,
            leader_id: 4,
            leader_epoch: wire_epoch(4).unwrap_or_else(|error| panic!("{error:?}")),
            endpoints: Vec::new(),
        };
        for key in [53_i16, 54] {
            let framed = response(key, 9, 0, Some((0, false)), &config, election)
                .unwrap_or_else(|error| panic!("{error:?}"));
            let encoded =
                encode_epoch_response(&classic).unwrap_or_else(|error| panic!("{error:?}"));
            assert_eq!(&framed[4..], encoded.as_slice());
        }
        let modern = EpochResponseBody {
            version: 1,
            endpoints: vec![VoteNodeEndpoint {
                node_id: 4,
                host: "127.0.0.1".to_owned(),
                port: 9093,
            }],
            ..classic
        };
        let bytes = encode_epoch_response(&modern).unwrap_or_else(|error| panic!("{error:?}"));
        let decoded = decode_epoch_response(&bytes, 1).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(decoded.endpoints[0].port, 9093);
        let persistent = state(4);
        let before = persistent;
        assert_eq!(
            decide_epoch_response(&persistent, &decoded),
            VoteDisposition::NotAdvertised
        );
        let mut stale = decoded;
        stale.endpoints[0].node_id = 9;
        assert_eq!(
            decide_epoch_response(&persistent, &stale),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        assert_eq!(persistent, before);
        assert_eq!(CONTROLLER_API_VERSIONS[2].max_version, 0);
        assert_eq!(CONTROLLER_API_VERSIONS[3].max_version, 0);
    }

    #[test]
    fn voter_directory_changes_are_parsed_and_not_applied() {
        let directory = [6u8; 16];
        let listener = LeaderEndpoint {
            name: "plaintext".to_owned(),
            host: "127.0.0.1".to_owned(),
            port: 9093,
        };
        let remove = RemoveVoterBody {
            cluster_id: Some("cluster-1".to_owned()),
            voter_id: 7,
            voter_directory: directory,
        };
        let removed = decode_remove_voter(
            &encode_remove_voter(&remove).unwrap_or_else(|error| panic!("{error:?}")),
        )
        .unwrap_or_else(|error| panic!("{error:?}"));
        let persistent = state(4);
        let before = persistent;
        assert_eq!(
            decide_remove_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                &removed
            ),
            VoteDisposition::NotAdvertised
        );
        let mut stale_remove = removed;
        stale_remove.voter_directory = [1u8; 16];
        assert_eq!(
            decide_remove_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                &stale_remove
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        let add = AddVoterBody {
            version: 0,
            cluster_id: Some("cluster-1".to_owned()),
            timeout_ms: 1000,
            voter_id: 7,
            voter_directory: directory,
            listeners: vec![listener.clone()],
            ack_when_committed: true,
        };
        let classic = encode_add_voter(&add).unwrap_or_else(|error| panic!("{error:?}"));
        let delayed = AddVoterBody {
            version: 1,
            ack_when_committed: false,
            ..add
        };
        let modern = encode_add_voter(&delayed).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(modern.len(), classic.len() + 1);
        let decoded = decode_add_voter(&modern, 1).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(!decoded.ack_when_committed);
        assert_eq!(
            decide_add_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &decoded
            ),
            VoteDisposition::NotAdvertised
        );
        let mut other_listener = decoded.clone();
        other_listener.listeners[0].name = "OTHER".to_owned();
        assert_eq!(
            decide_add_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &other_listener
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        let mut upper = decoded.clone();
        upper.listeners[0].name = "PLAINTEXT".to_owned();
        assert_eq!(
            decide_add_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &upper
            ),
            VoteDisposition::NotAdvertised
        );
        assert_eq!(
            decide_add_voter(
                &persistent,
                election::Role::Follower,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &decoded
            ),
            VoteDisposition::Refused(PreVoteRefusal::NotLeader)
        );
        let update = UpdateVoterBody {
            cluster_id: Some("cluster-1".to_owned()),
            current_leader_epoch: 3,
            voter_id: 7,
            voter_directory: directory,
            listeners: vec![listener],
            kraft_min: 0,
            kraft_max: 1,
        };
        let updated = decode_update_voter(
            &encode_update_voter(&update).unwrap_or_else(|error| panic!("{error:?}")),
        )
        .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(updated.kraft_max, 1);
        assert_eq!(updated.listeners[0].port, 9093);
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &updated
            ),
            VoteDisposition::NotAdvertised
        );
        let mut above = updated.clone();
        above.kraft_min = 2;
        above.kraft_max = 2;
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &above
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut below = updated.clone();
        below.kraft_max = 0;
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &below
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Follower,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &updated
            ),
            VoteDisposition::Refused(PreVoteRefusal::NotLeader)
        );
        let mut inverted = updated.clone();
        inverted.kraft_min = 2;
        inverted.kraft_max = 1;
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &inverted
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut negative_range = updated.clone();
        negative_range.kraft_max = -1;
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &negative_range
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut fenced_range = inverted;
        fenced_range.current_leader_epoch = 2;
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &fenced_range
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleTerm)
        );
        let mut older = updated.clone();
        older.current_leader_epoch = 2;
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &older
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleTerm)
        );
        let mut newer = updated.clone();
        newer.current_leader_epoch = 4;
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &newer
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        let mut bare = updated;
        bare.listeners.clear();
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &bare
            ),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        let mut foreign_remove = stale_remove;
        foreign_remove.cluster_id = Some("other".to_owned());
        assert_eq!(
            decide_remove_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                &foreign_remove
            ),
            VoteDisposition::Refused(PreVoteRefusal::InconsistentCluster)
        );
        let mut foreign_update = older;
        foreign_update.cluster_id = Some("other".to_owned());
        assert_eq!(
            decide_update_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &foreign_update
            ),
            VoteDisposition::Refused(PreVoteRefusal::InconsistentCluster)
        );
        let mut negative = decoded;
        negative.voter_id = -1;
        assert_eq!(
            decide_add_voter(
                &persistent,
                election::Role::Leader,
                metadata_log(),
                7,
                directory,
                "plaintext",
                &negative
            ),
            VoteDisposition::Refused(PreVoteRefusal::IllegalEpoch)
        );
        assert_eq!(persistent, before);
        assert!(CONTROLLER_API_VERSIONS
            .iter()
            .all(|api| !matches!(api.api_key, 80..=82)));
        assert_eq!(last_voter_refusal(1), Some(PreVoteRefusal::LastVoter));
        assert_eq!(last_voter_refusal(0), Some(PreVoteRefusal::LastVoter));
        assert_eq!(last_voter_refusal(2), None);
    }
}
