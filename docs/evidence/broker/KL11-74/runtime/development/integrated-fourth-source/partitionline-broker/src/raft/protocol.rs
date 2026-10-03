//! Fixed trusted-member controller RPCs: Vote52/0, Begin53/0 and End54/0.
//!
//! This is one configured metadata partition, not replication or full KRaft.
//! Directory identities, pre-votes and dynamic membership are unadvertised.
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
