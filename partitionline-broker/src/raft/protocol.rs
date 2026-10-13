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
    transport::Handler,
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
            return response(
                key,
                header.correlation_id,
                CLUSTER_MISMATCH,
                None,
                &self.config,
                self.state(),
            );
        }
        if !request.partition_matches {
            return response(
                key,
                header.correlation_id,
                INVALID_REQUEST,
                None,
                &self.config,
                self.state(),
            );
        }
        let error = self.apply(key, &request, now_ms)?;
        response(
            key,
            header.correlation_id,
            0,
            Some(error),
            &self.config,
            self.state(),
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
fn response(
    key: i16,
    correlation: i32,
    top: i16,
    partition_error: Option<(i16, bool)>,
    config: &Config,
    state: State,
) -> Result<Vec<u8>, Error> {
    let flexible = key == 52;
    let mut out = allocate(config.topic.len() + 64)?;
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

/// Apply a decoded Vote body without persisting a pre-vote.
///
/// `expected_replica` and `expected_voter` are the directory ids that must
/// match on versions 1 and 2. The returned disposition never asks the caller
/// to write `persistent`.
pub fn decide_vote_body(
    persistent: &election::PersistentState,
    expected_replica: [u8; 16],
    expected_voter: [u8; 16],
    body: &VoteBody,
) -> VoteDisposition {
    if body.version == 0 {
        return VoteDisposition::DurableV0;
    }
    let Some(replica_directory) = body.replica_directory else {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    };
    let Some(voter_directory) = body.voter_directory else {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    };
    if replica_directory != expected_replica || voter_directory != expected_voter {
        return VoteDisposition::Refused(PreVoteRefusal::StaleDirectory);
    }
    if !body.pre_vote {
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
    body: &VoteResponseBody,
) -> VoteDisposition {
    let _ = persistent;
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
    voter_id: i32,
    expected_directory: [u8; 16],
    body: &RemoveVoterBody,
) -> VoteDisposition {
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
/// An empty listener list is refused. `ack_when_committed` false is still not
/// appended.
pub fn decide_add_voter(
    persistent: &election::PersistentState,
    voter_id: i32,
    expected_directory: [u8; 16],
    body: &AddVoterBody,
) -> VoteDisposition {
    directory_change(
        persistent,
        voter_id,
        expected_directory,
        body.voter_id,
        body.voter_directory,
        !body.listeners.is_empty()
            && body
                .listeners
                .iter()
                .all(|listener| !listener.host.is_empty()),
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
    voter_id: i32,
    expected_directory: [u8; 16],
    body: &UpdateVoterBody,
) -> VoteDisposition {
    directory_change(
        persistent,
        voter_id,
        expected_directory,
        body.voter_id,
        body.voter_directory,
        !body.listeners.is_empty()
            && body
                .listeners
                .iter()
                .all(|listener| !listener.host.is_empty()),
    )
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
/// Version 0 is the classic handler input. Version 1 with a mismatched
/// directory is refused. A matching version 1 body is parsed and not advertised.
pub fn decide_begin_body(
    persistent: &election::PersistentState,
    expected_directory: [u8; 16],
    body: &BeginEpochBody,
) -> VoteDisposition {
    let _ = persistent;
    if body.version == 0 {
        return VoteDisposition::DurableV0;
    }
    match body.voter_directory {
        Some(directory) if directory == expected_directory && directory != [0; 16] => {
            VoteDisposition::NotAdvertised
        }
        _ => VoteDisposition::Refused(PreVoteRefusal::StaleDirectory),
    }
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
    local_id: i32,
    local_directory: [u8; 16],
    body: &EndEpochBody,
) -> Result<(VoteDisposition, Option<usize>), Error> {
    let _ = persistent;
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
    expected_directory: [u8; 16],
    body: &FetchSnapshotBody,
) -> VoteDisposition {
    let _ = persistent;
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

enum Command {
    Request(Vec<u8>, oneshot::Sender<Result<Vec<u8>, Error>>),
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
                    Command::Request(request, reply) => {
                        if reply.is_closed() {
                            continue;
                        }
                        let result = u64::try_from(start.elapsed().as_millis())
                            .map_err(|_| Error::EpochOverflow)
                            .and_then(|now| controller.respond(&request, now));
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
impl Handler for ControllerHandler {
    type Error = Error;
    async fn handle(&self, request: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if request.len() > self.config.protocol_limits.max_request_bytes() {
            return Err(protocol::Error::RequestTooLarge.into());
        }
        let (sender, receiver) = oneshot::channel();
        self.sender
            .try_send(Command::Request(request, sender))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => Error::Stopped,
            })?;
        Ok(Some(receiver.await.map_err(|_| Error::Actor)??))
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
    /// The candidate term is older than the stable term.
    StaleTerm,
    /// The candidate log is behind the stable log.
    StaleLog,
}

/// Decide a pre-vote without changing `persistent`.
///
/// The stable term and vote stay with the caller. Wire versions above the
/// advertised Vote v0 profile are not selected by [`negotiate`].
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
    if request.log < persistent.log {
        return Err(PreVoteRefusal::StaleLog);
    }
    Ok(true)
}

#[cfg(test)]
mod prevote_tests {
    use super::*;

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
            decide_vote_body(&persistent, replica, voter, &decoded),
            VoteDisposition::PreVoteGranted
        );
        assert_eq!(persistent, before);
        let mut stale = decoded.clone();
        stale.replica_directory = Some([1u8; 16]);
        assert_eq!(
            decide_vote_body(&persistent, replica, voter, &stale),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        assert_eq!(persistent, before);
        assert_eq!(
            decide_vote_body(
                &persistent,
                replica,
                voter,
                &sample_body(1, replica, voter, false)
            ),
            VoteDisposition::NotAdvertised
        );
        assert_eq!(CONTROLLER_API_VERSIONS[1].max_version, 0);
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
            decide_begin_body(&persistent, directory, &decoded),
            VoteDisposition::NotAdvertised
        );
        let mut stale = decoded;
        stale.voter_directory = Some([2u8; 16]);
        assert_eq!(
            decide_begin_body(&persistent, directory, &stale),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
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
        let modern = EndEpochBody {
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
        let encoded = encode_end_body(&modern).unwrap_or_else(|error| panic!("{error:?}"));
        let decoded = decode_end_body(&encoded, 1).unwrap_or_else(|error| panic!("{error:?}"));
        let persistent = state(4);
        let before = persistent;
        let (disposition, rank) = decide_end_body(&persistent, 7, directory, &decoded)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            disposition,
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
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
            current_leader_epoch: 2,
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
            decide_fetch_snapshot_body(&persistent, directory, &decoded),
            VoteDisposition::NotAdvertised
        );
        let mut stale = decoded;
        stale.replica_directory = Some([1u8; 16]);
        assert_eq!(
            decide_fetch_snapshot_body(&persistent, directory, &stale),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
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
            decide_vote_response(&persistent, &decoded),
            VoteDisposition::NotAdvertised
        );
        let mut stale = decoded;
        stale.endpoints[0].node_id = 9;
        assert_eq!(
            decide_vote_response(&persistent, &stale),
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
            decide_remove_voter(&persistent, 7, directory, &removed),
            VoteDisposition::NotAdvertised
        );
        let mut stale_remove = removed;
        stale_remove.voter_directory = [1u8; 16];
        assert_eq!(
            decide_remove_voter(&persistent, 7, directory, &stale_remove),
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
            decide_add_voter(&persistent, 7, directory, &decoded),
            VoteDisposition::NotAdvertised
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
            decide_update_voter(&persistent, 7, directory, &updated),
            VoteDisposition::NotAdvertised
        );
        let mut bare = updated;
        bare.listeners.clear();
        assert_eq!(
            decide_update_voter(&persistent, 7, directory, &bare),
            VoteDisposition::Refused(PreVoteRefusal::StaleDirectory)
        );
        assert_eq!(persistent, before);
        assert!(CONTROLLER_API_VERSIONS
            .iter()
            .all(|api| !matches!(api.api_key, 80..=82)));
    }
}
