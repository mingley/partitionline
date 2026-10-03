//! Owned autonomous replication over explicitly configured trusted numeric peers.
//!
//! Private framing does not authenticate a network, discover directories, or
//! advertise native Kafka KRaft APIs. The durable Node keeps its known-leader
//! configuration gate. Rust-managed envelopes are bounded; OS socket buffers,
//! allocator overhead and total process RSS are not represented by that bound.

use super::{
    election::{LogPosition, Role, Timeouts},
    membership::{Bootstrap, FeatureRequest, Key, Voter, MAX_CONFIGURATION_BYTES},
    peer_codec::{self, Frame, Group, Hello, Message},
    protocol,
    replication::{self, ChangeReceipt, DynamicSnapshotRequest, Node, Record, State},
    snapshot,
};
#[cfg(test)]
use std::path::Path;
use std::{
    fmt, io,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch, Notify, OwnedSemaphorePermit, Semaphore},
    task::{JoinHandle, JoinSet},
    time::{Instant, MissedTickBehavior},
};

const MANAGED_CEILING: usize = 512 * 1024 * 1024;

#[cfg(not(test))]
type AdmissionPermit = OwnedSemaphorePermit;
#[cfg(test)]
type AdmissionPermit = TestPermit;

#[cfg(test)]
#[derive(Default)]
struct TestGauge {
    current: AtomicUsize,
    peak: AtomicUsize,
}
#[cfg(test)]
#[derive(Default)]
struct TestMetrics {
    // Constructed RAII owners: client, charged bytes, network task, socket,
    // pending/executing command envelope. This is not allocator or RSS usage.
    gauges: [TestGauge; 5],
    workers_spawned: AtomicUsize,
    workers_joined: AtomicUsize,
    clock: std::sync::OnceLock<std::time::Instant>,
}
#[cfg(test)]
impl TestMetrics {
    fn json(&self) -> Result<String> {
        use std::fmt::Write;
        let mut out = String::new();
        out.try_reserve_exact(1024).map_err(|_| Error::Allocation)?;
        out.push_str("{\"names\":[\"client_admission_owners\",\"charged_transport_owner_bytes\",\"network_task_owners\",\"socket_owners\",\"command_envelope_owners\"],\"gauges\":[");
        for (index, g) in self.gauges.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            // A constructor may be preempted between its current increment
            // and peak publication. This observed current value is itself a
            // genuine high-water observation; publish it before emitting both.
            let current = g.current.load(Ordering::Acquire);
            let peak = g.peak.fetch_max(current, Ordering::AcqRel).max(current);
            write!(out, "{{\"current\":{},\"peak\":{}}}", current, peak)
                .map_err(|_| Error::Peer)?;
        }
        out.push_str("]}");
        Ok(out)
    }
}
#[cfg(test)]
struct TestCounter {
    metrics: Arc<TestMetrics>,
    gauge: usize,
    amount: usize,
}
#[cfg(test)]
impl TestCounter {
    fn new(metrics: Arc<TestMetrics>, gauge: usize, amount: usize) -> Self {
        let value = metrics.gauges[gauge]
            .current
            .fetch_add(amount, Ordering::AcqRel)
            + amount;
        metrics.gauges[gauge]
            .peak
            .fetch_max(value, Ordering::AcqRel);
        Self {
            metrics,
            gauge,
            amount,
        }
    }
}
#[cfg(test)]
impl Drop for TestCounter {
    fn drop(&mut self) {
        self.metrics.gauges[self.gauge]
            .current
            .fetch_sub(self.amount, Ordering::AcqRel);
    }
}
#[cfg(test)]
struct TestPermit {
    // Rust drops fields in declaration order. Publish the logical decrement
    // before returning semaphore capacity to the next constructed owner.
    _counter: TestCounter,
    _permit: OwnedSemaphorePermit,
}
#[cfg(test)]
impl TestPermit {
    fn new(
        permit: OwnedSemaphorePermit,
        metrics: Arc<TestMetrics>,
        gauge: usize,
        amount: usize,
    ) -> Self {
        Self {
            _counter: TestCounter::new(metrics, gauge, amount),
            _permit: permit,
        }
    }
}
type Result<T> = std::result::Result<T, Error>;

/// Explicit bounded runtime failure; a timeout after admission can be ambiguous.
#[derive(Debug)]
pub enum Error {
    /// Configuration or a checked resource envelope is invalid.
    InvalidConfig,
    /// Runtime admissions have stopped.
    Closed,
    /// An absolute deadline expired; already started storage can still finish.
    Deadline,
    /// Recovery failed; shutdown retains the owner for joined cleanup.
    Startup,
    /// Private bytes or identities do not match their configured session.
    Peer,
    /// The requested position is currently absent or has another term.
    PositionMismatch,
    /// A bounded allocation failed.
    Allocation,
    /// Socket or local filesystem failure.
    Io(io::Error),
    /// Durable Node rejection or ambiguous storage failure.
    Node(replication::Error),
    /// An owned task panicked or was otherwise interrupted.
    Join(tokio::task::JoinError),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "private peer runtime: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<replication::Error> for Error {
    fn from(e: replication::Error) -> Self {
        Self::Node(e)
    }
}
impl From<peer_codec::Error> for Error {
    fn from(_: peer_codec::Error) -> Self {
        Self::Peer
    }
}

/// One immutable numeric route and the exact expected replica descriptor.
#[derive(Debug, Clone)]
pub struct Peer {
    voter: Voter,
    address: SocketAddr,
}
impl Peer {
    /// Configure a full directory-qualified peer; port zero cannot be dialed.
    pub fn new(voter: Voter, address: SocketAddr) -> Result<Self> {
        if address.port() == 0 || address.ip().is_unspecified() {
            return Err(Error::InvalidConfig);
        }
        Ok(Self { voter, address })
    }
    /// Exact configured peer identity.
    pub fn key(&self) -> Key {
        self.voter.key()
    }
    /// Configured numeric endpoint; no DNS worker is created.
    pub fn address(&self) -> SocketAddr {
        self.address
    }
}
/// Trusted bounded storage paths, never derived from wire fields.
#[derive(Debug, Clone)]
pub struct Paths {
    wal: PathBuf,
    election: PathBuf,
    images: PathBuf,
}
impl Paths {
    /// Configure distinct paths, each at most4096 encoded bytes.
    pub fn new(wal: PathBuf, election: PathBuf, images: PathBuf) -> Result<Self> {
        if [&wal, &election, &images]
            .iter()
            .any(|p| p.as_os_str().is_empty() || p.as_os_str().as_encoded_bytes().len() > 4096)
            || wal == election
            || wal == images
            || election == images
        {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            wal,
            election,
            images,
        })
    }
}
/// Exact storage numbers used for both Node construction and runtime accounting.
#[derive(Debug, Clone, Copy)]
pub struct StorageLimits {
    /// Maximum payload of one opaque or Voters record.
    pub record_bytes: usize,
    /// Bounded Node append/WAL chunk.
    pub chunk_bytes: usize,
    /// Maximum retained live positions.
    pub live_entries: usize,
    /// Maximum retained opaque payload bytes.
    pub live_bytes: usize,
    /// Maximum durable operations.
    pub operations: usize,
    /// Maximum content WAL bytes.
    pub wal_bytes: u64,
    /// Maximum committed fetch output.
    pub fetch_bytes: usize,
    /// Existing bounded image decode/disk/generation/chunk limits.
    pub images: snapshot::Limits,
}
impl StorageLimits {
    /// Recommended finite runtime profile. Image chunks are2MiB so their
    /// private envelope fits Limits::default(); the image/live byte ceilings
    /// retain the existing core defaults. Constructor errors propagate.
    pub fn standard() -> Result<Self> {
        Ok(Self {
            record_bytes: 1024 * 1024,
            chunk_bytes: 2 * 1024 * 1024,
            live_entries: 4096,
            live_bytes: 64 * 1024 * 1024,
            operations: 65536,
            wal_bytes: 256 * 1024 * 1024,
            fetch_bytes: 2 * 1024 * 1024,
            images: snapshot::Limits::new(
                65 * 1024 * 1024,
                4096,
                64 * 1024 * 1024,
                1024 * 1024,
                2 * 1024 * 1024,
                8,
                32,
            )
            .map_err(|_| Error::InvalidConfig)?,
        })
    }
    fn node(self) -> Result<replication::Limits> {
        Ok(replication::Limits::new(
            self.record_bytes,
            self.chunk_bytes,
            self.live_entries,
            self.live_bytes,
            self.operations,
            self.wal_bytes,
            self.fetch_bytes,
        )?)
    }
}
/// Finite tasks, messages, command admissions and exact monotonic timer settings.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Active incoming sessions, including incomplete Hello admissions; zero derives max(peers,1).
    pub incoming: usize,
    /// Public queued/in-flight/completed-unconsumed command slots.
    pub client_slots: usize,
    /// Maximum private payload, excluding TCP length prefix.
    pub frame_bytes: usize,
    /// Election minimum milliseconds, assigned directly to the durable owner.
    pub election_min_ms: u64,
    /// Election maximum milliseconds, assigned directly to the durable owner.
    pub election_max_ms: u64,
    /// Current-term quorum interval and Node transfer lifetime.
    pub quorum_ms: u64,
    /// Coalesced autonomous timer interval.
    pub tick_ms: u64,
    /// Per-peer heartbeat/retry interval.
    pub heartbeat_ms: u64,
    /// Absolute TCP connect budget, bounded by an RPC deadline.
    pub connect_ms: u64,
    /// Whole private RPC/Hello read/write budget.
    pub rpc_ms: u64,
    /// Whole image stream budget; progress never renews it.
    pub transfer_ms: u64,
    /// Public admission and response deadline.
    pub command_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            incoming: 0,
            client_slots: 8,
            frame_bytes: 2 * 1024 * 1024 + 4096,
            election_min_ms: 1000,
            election_max_ms: 2000,
            quorum_ms: 3000,
            tick_ms: 20,
            heartbeat_ms: 100,
            connect_ms: 500,
            rpc_ms: 1000,
            transfer_ms: 2000,
            command_ms: 5000,
        }
    }
}
/// Immutable fixed routes combined with existing durable dynamic-directory semantics.
#[derive(Debug, Clone)]
pub struct Config {
    controller: protocol::Config,
    bootstrap: Bootstrap,
    peers: Vec<Peer>,
    storage: StorageLimits,
    limits: Limits,
    group: Group,
    charge: usize,
    pool: usize,
    managed: usize,
}
impl Config {
    /// Validate exact genesis/routes and all positive byte/task/deadline ceilings.
    pub fn new(
        mut controller: protocol::Config,
        bootstrap: Bootstrap,
        peers: Vec<Peer>,
        storage: StorageLimits,
        mut limits: Limits,
    ) -> Result<Self> {
        let p = peers.len();
        let local = bootstrap.local().key();
        if limits.incoming == 0 {
            limits.incoming = p.max(1);
        }
        if p > 64
            || controller.local_id != local.id
            || !(1..=65).contains(&limits.incoming)
            || !(1..=64).contains(&limits.client_slots)
            || !(1024..=4 * 1024 * 1024).contains(&limits.frame_bytes)
            || limits.incoming > p + 1
            || storage
                .chunk_bytes
                .checked_add(256)
                .is_none_or(|n| n > limits.frame_bytes)
            || storage.images.chunk_bytes() > limits.frame_bytes - 256
            || peers
                .iter()
                .enumerate()
                .any(|(i, v)| v.key() == local || peers[..i].iter().any(|a| a.key() == v.key()))
        {
            return Err(Error::InvalidConfig);
        }
        for v in bootstrap.genesis().voters() {
            if v.key() != local && !peers.iter().any(|p| &p.voter == v) {
                return Err(Error::InvalidConfig);
            }
        }
        let local_endpoint = bootstrap
            .local()
            .endpoint(bootstrap.listener())
            .ok_or(Error::InvalidConfig)?;
        let local_ip = local_endpoint
            .host()
            .parse::<IpAddr>()
            .map_err(|_| Error::InvalidConfig)?;
        if local_ip.is_unspecified() {
            return Err(Error::InvalidConfig);
        }
        for peer in &peers {
            let ep = peer
                .voter
                .endpoint(bootstrap.listener())
                .ok_or(Error::InvalidConfig)?;
            let ip = ep
                .host()
                .parse::<IpAddr>()
                .map_err(|_| Error::InvalidConfig)?;
            if SocketAddr::new(ip, ep.port()) != peer.address {
                return Err(Error::InvalidConfig);
            }
        }
        for n in [
            limits.quorum_ms,
            limits.tick_ms,
            limits.heartbeat_ms,
            limits.connect_ms,
            limits.rpc_ms,
            limits.transfer_ms,
            limits.command_ms,
        ] {
            if !(1..=600000).contains(&n) {
                return Err(Error::InvalidConfig);
            }
        }
        if limits.tick_ms > limits.heartbeat_ms
            || limits
                .heartbeat_ms
                .checked_mul(3)
                .ok_or(Error::InvalidConfig)?
                > limits.quorum_ms
            || limits.connect_ms > limits.rpc_ms
            || limits.rpc_ms > limits.quorum_ms
            || limits.transfer_ms > limits.quorum_ms
        {
            return Err(Error::InvalidConfig);
        }
        controller.election_timeouts =
            Timeouts::new(limits.election_min_ms, limits.election_max_ms)
                .map_err(|_| Error::InvalidConfig)?;
        controller.validate().map_err(|_| Error::InvalidConfig)?;
        storage.node()?;
        if storage.images.records() > storage.live_entries
            || storage.images.payload_bytes()
                > u64::try_from(storage.live_bytes).map_err(|_| Error::InvalidConfig)?
            || storage.images.record_bytes() > storage.record_bytes
        {
            return Err(Error::InvalidConfig);
        }
        let genesis = bootstrap
            .genesis()
            .encode()
            .map_err(|_| Error::InvalidConfig)?;
        if genesis
            .len()
            .checked_add(600)
            .is_none_or(|n| n > limits.frame_bytes)
        {
            return Err(Error::InvalidConfig);
        }
        let table = storage
            .live_entries
            .checked_mul(std::mem::size_of::<Record>())
            .ok_or(Error::InvalidConfig)?;
        let charge = checked_sum(&[
            product(limits.frame_bytes, 3)?,
            table,
            std::mem::size_of::<Message>(),
        ])?;
        let network_slots = checked_sum(&[p, limits.incoming])?;
        let pool = product(network_slots, charge)?;
        let command_envelope = checked_sum(&[
            limits
                .frame_bytes
                .max(storage.fetch_bytes)
                .max(controller.protocol_limits.max_request_bytes()),
            table,
            std::mem::size_of::<Command>(),
            std::mem::size_of::<Reply>(),
        ])?;
        let command_slots = checked_sum(&[product(limits.client_slots, 2)?, 1])?;
        let owner_slots = checked_sum(&[p, limits.incoming, limits.client_slots, 1])?;
        let managed = checked_sum(&[
            product(checked_sum(&[storage.live_bytes, table])?, 2)?,
            usize::try_from(storage.images.decoded_bytes()).map_err(|_| Error::InvalidConfig)?,
            32 * 1024 * 1024,
            product(MAX_CONFIGURATION_BYTES, 16)?,
            pool,
            product(command_slots, command_envelope)?,
            product(owner_slots, 4096)?,
        ])?;
        if managed > MANAGED_CEILING || pool > u32::MAX as usize {
            return Err(Error::InvalidConfig);
        }
        let group = Group {
            cluster: controller.cluster_id.clone(),
            topic: controller.topic.clone(),
            partition: u32::try_from(controller.partition).map_err(|_| Error::InvalidConfig)?,
            genesis,
        };
        Ok(Self {
            controller,
            bootstrap,
            peers,
            storage,
            limits,
            group,
            charge,
            pool,
            managed,
        })
    }
    /// Conservative configured Rust-managed byte envelope; not RSS or OS buffers.
    pub fn managed_bytes(&self) -> usize {
        self.managed
    }
    /// Maximum supervisor/storage/network tasks owned by this runtime.
    pub fn task_bound(&self) -> usize {
        2 + self.peers.len() + self.limits.incoming
    }
}
fn checked_sum(values: &[usize]) -> Result<usize> {
    values
        .iter()
        .try_fold(0usize, |a, v| a.checked_add(*v).ok_or(Error::InvalidConfig))
}
fn product(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right).ok_or(Error::InvalidConfig)
}

/// Current ownership diagnostics, not a read lease or durability certificate.
#[derive(Debug, Clone, Copy)]
pub struct Diagnostics {
    /// Active network workers/sessions.
    pub network_tasks: usize,
    /// Currently owned connected/dialing sockets; listener adds one while running.
    pub sockets: usize,
    /// Charged live transport bytes.
    pub transport_bytes: usize,
    /// Current public admission slots.
    pub client_slots: usize,
    /// True after stop was requested.
    pub stopping: bool,
}
struct Control {
    stopped: AtomicBool,
    notify: Notify,
    commands: mpsc::Sender<Envelope>,
    clients: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    startup: watch::Receiver<u8>,
    tasks: AtomicUsize,
    sockets: AtomicUsize,
    #[cfg(test)]
    metrics: Arc<TestMetrics>,
    #[cfg(test)]
    capture_root: Option<PathBuf>,
    config: Arc<Config>,
}
impl Control {
    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.clients.close();
        self.notify.notify_waiters();
    }
    async fn stopping(&self) {
        loop {
            let notified = self.notify.notified();
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
    async fn call(
        &self,
        command: Command,
        permit: Option<AdmissionPermit>,
        transport: Option<Arc<AdmissionPermit>>,
        deadline: Instant,
    ) -> Result<Reply> {
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        let (tx, rx) = oneshot::channel();
        let started = Arc::new(AtomicBool::new(false));
        let envelope = Envelope {
            command,
            reply: tx,
            permit,
            transport,
            started,
            deadline: Some(deadline),
            #[cfg(test)]
            delivery_gate: None,
            #[cfg(test)]
            _counter: Some(TestCounter::new(self.metrics.clone(), 4, 1)),
        };
        tokio::time::timeout_at(deadline, self.commands.send(envelope))
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Closed)?;
        let delivered = tokio::time::timeout_at(deadline, rx)
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Closed)?;
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        delivered.result
    }
    async fn internal(&self, command: Command) -> Result<Reply> {
        // A network worker keeps at most one such call; the owner channel includes
        // one reserved position per worker and a separate timer/stop position.
        self.call(
            command,
            None,
            None,
            Instant::now() + Duration::from_millis(self.config.limits.command_ms),
        )
        .await
    }
    // Cleanup and Stop have bounded admission memory, but deliberately no
    // response timeout. The supervisor retains and awaits every such future;
    // a delayed blocking storage operation must finish before joined shutdown.
    // Failure is channel/owner loss, never silently skipped expired admission.
    async fn reliable(&self, command: Command) -> Result<Reply> {
        let (tx, rx) = oneshot::channel();
        let envelope = Envelope {
            command,
            reply: tx,
            permit: None,
            transport: None,
            started: Arc::new(AtomicBool::new(false)),
            deadline: None,
            #[cfg(test)]
            delivery_gate: None,
            #[cfg(test)]
            _counter: Some(TestCounter::new(self.metrics.clone(), 4, 1)),
        };
        self.commands
            .send(envelope)
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?.result
    }
    async fn cancel_job(&self, job: Job) -> Result<Reply> {
        self.reliable(job.cancellation()).await
    }
    async fn reserve_frame(&self, deadline: Instant) -> Result<Arc<AdmissionPermit>> {
        tokio::time::timeout_at(
            deadline,
            self.bytes
                .clone()
                .acquire_many_owned(self.config.charge as u32),
        )
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|_| Error::Closed)
        .map(|permit| {
            #[cfg(test)]
            let permit = TestPermit::new(permit, self.metrics.clone(), 1, self.config.charge);
            Arc::new(permit)
        })
    }
    #[cfg(test)]
    fn lifecycle(
        &self,
        stage: &str,
        spawned: usize,
        joined: usize,
        owner_joined: bool,
        supervisor_joined: bool,
        listener_closed: bool,
    ) -> Result<()> {
        use std::io::Write;
        let Some(root) = &self.capture_root else {
            return Ok(());
        };
        if !matches!(stage, "workers-owner-joined" | "supervisor-joined") {
            return Err(Error::InvalidConfig);
        }
        let now_ms = u64::try_from(
            self.metrics
                .clock
                .get()
                .ok_or(Error::InvalidConfig)?
                .elapsed()
                .as_millis(),
        )
        .map_err(|_| Error::InvalidConfig)?;
        let capacity = checked_sum(&[
            self.config.limits.client_slots,
            self.config.peers.len(),
            self.config.limits.incoming,
            1,
        ])?;
        let bounds = [
            self.config.limits.client_slots,
            self.config.pool,
            checked_sum(&[self.config.peers.len(), self.config.limits.incoming])?,
            checked_sum(&[self.config.peers.len(), self.config.limits.incoming])?,
            checked_sum(&[product(capacity, 2)?, 1])?,
        ];
        for (gauge, bound) in self.metrics.gauges.iter().zip(bounds) {
            if gauge.current.load(Ordering::Acquire) != 0
                || gauge.peak.load(Ordering::Acquire) > bound
            {
                return Err(Error::InvalidConfig);
            }
        }
        if spawned != joined {
            return Err(Error::InvalidConfig);
        }
        std::fs::create_dir_all(root)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join(format!("lifecycle-{stage}.json")))?;
        let text = format!("{{\"schema_version\":1,\"capture_revision\":2,\"pid\":{},\"stage\":\"{stage}\",\"now_ms\":{now_ms},\"clock_basis\":\"same exclusive owner process-local monotonic epoch\",\"source_sha\":\"{}\",\"local_id\":{},\"directory\":\"{}\",\"network_workers_spawned\":{spawned},\"network_workers_joined\":{joined},\"owner_joined\":{owner_joined},\"supervisor_joined\":{supervisor_joined},\"listener_closed\":{listener_closed},\"resource_owners\":{},\"stopping\":{},\"available_client_permits\":{},\"available_transport_permits\":{}}}\n", std::process::id(), std::env::var("PL_PEER_RUNTIME_SOURCE_SHA").map_err(|_| Error::InvalidConfig)?, self.config.bootstrap.local().key().id, TestTrace::hex(&self.config.bootstrap.local().key().directory)?, self.metrics.json()?, self.stopped.load(Ordering::Acquire), self.clients.available_permits(), self.bytes.available_permits());
        if text.len() > 8192 {
            return Err(Error::InvalidConfig);
        }
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    }
}
struct Envelope {
    command: Command,
    reply: oneshot::Sender<Delivered>,
    permit: Option<AdmissionPermit>,
    transport: Option<Arc<AdmissionPermit>>,
    started: Arc<AtomicBool>,
    deadline: Option<Instant>,
    #[cfg(test)]
    delivery_gate: Option<DeliveryGate>,
    #[cfg(test)]
    _counter: Option<TestCounter>,
}
#[cfg(test)]
struct DeliveryGate {
    ready: oneshot::Sender<(Vec<replication::DynamicVoteRequest>, State)>,
    release: std::sync::mpsc::Receiver<()>,
}
struct Delivered {
    result: Result<Reply>,
    _permit: Option<AdmissionPermit>,
    _transport: Option<Arc<AdmissionPermit>>,
}
enum Command {
    Tick,
    State,
    Voters,
    Propose(Vec<u8>),
    Position(LogPosition),
    Fetch(u64, usize, usize),
    Checkpoint([u8; 16]),
    Prepare(Key),
    Inbound(Key, Message),
    Acknowledge(Message),
    Timeout(Key, u64, bool),
    CancelVote(replication::DynamicVoteRequest),
    Chunk(DynamicSnapshotRequest),
    Add(Key),
    Addition(Key),
    Remove(Key),
    Change,
    Disconnected(Key),
    Stop,
}
enum Reply {
    Done,
    State(State),
    Voters(super::membership::Voters),
    Position(LogPosition),
    Visible(bool),
    Records(Vec<Record>),
    Descriptor(snapshot::Descriptor),
    Job(Option<Job>),
    Message(Message),
    Votes(Vec<Job>),
    Change(ChangeReceipt),
    Addition(Option<ChangeReceipt>),
}
#[derive(Debug)]
enum Job {
    Vote(replication::DynamicVoteRequest),
    Feature(FeatureRequest),
    Append(replication::DynamicRequest),
    Image(DynamicSnapshotRequest),
}
impl Job {
    fn peer(&self) -> Key {
        match self {
            Self::Vote(q) => q.context.peer,
            Self::Feature(q) => q.peer,
            Self::Append(q) => q.context.peer,
            Self::Image(q) => q.context.peer,
        }
    }
    fn sequence(&self) -> u64 {
        match self {
            Self::Vote(q) => q.sequence,
            Self::Feature(q) => q.sequence,
            Self::Append(q) => q.request.sequence,
            Self::Image(q) => q.request.sequence,
        }
    }
    fn cancellation(&self) -> Command {
        match self {
            Self::Vote(q) => Command::CancelVote(*q),
            _ => Command::Timeout(self.peer(), self.sequence(), matches!(self, Self::Image(_))),
        }
    }
}

/// Exclusive lifecycle owner; successful shutdown joins every subordinate task.
pub struct Runtime {
    control: Arc<Control>,
    supervisor: Option<JoinHandle<Result<State>>>,
    finished: Option<State>,
}
/// Cloneable bounded command handle; it cannot borrow storage or join handles.
#[derive(Clone)]
pub struct Handle {
    control: Arc<Control>,
}
impl Runtime {
    /// Start owned recovery/tasks using an already bound private listener.
    /// Cancellation of wait_ready does not detach the storage owner. A Tokio
    /// runtime must be present; startup itself performs no filesystem I/O.
    pub fn start(listener: TcpListener, config: Config, paths: Paths) -> Result<Self> {
        tokio::runtime::Handle::try_current().map_err(|_| Error::InvalidConfig)?;
        // The caller owns the listener, possibly behind an explicitly configured
        // fixed numeric forwarder. Advertised route != local bind is supported;
        // neither SocketAddr nor private Hello is represented as authentication.
        let bound = listener.local_addr()?;
        if bound.port() == 0 {
            return Err(Error::InvalidConfig);
        }
        let config = Arc::new(config);
        let capacity = checked_sum(&[
            config.limits.client_slots,
            config.peers.len(),
            config.limits.incoming,
            1,
        ])?;
        let (tx, rx) = mpsc::channel(capacity);
        let (ready_tx, ready_rx) = watch::channel(0);
        let control = Arc::new(Control {
            stopped: AtomicBool::new(false),
            notify: Notify::new(),
            commands: tx,
            clients: Arc::new(Semaphore::new(config.limits.client_slots)),
            bytes: Arc::new(Semaphore::new(config.pool)),
            startup: ready_rx,
            tasks: AtomicUsize::new(0),
            sockets: AtomicUsize::new(0),
            #[cfg(test)]
            metrics: Arc::new(TestMetrics::default()),
            #[cfg(test)]
            capture_root: TestTrace::capture_root(&paths, &config)?,
            config,
        });
        let owner_control = control.clone();
        let start = std::time::Instant::now();
        let owner = tokio::task::spawn_blocking(move || {
            run_owner(owner_control, paths, rx, ready_tx, start)
        });
        let supervisor = tokio::spawn(supervise(listener, control.clone(), owner));
        Ok(Self {
            control,
            supervisor: Some(supervisor),
            finished: None,
        })
    }
    /// Observe confirmed startup without taking ownership away from recovery.
    pub async fn wait_ready(&self) -> Result<()> {
        let deadline =
            Instant::now() + Duration::from_millis(self.control.config.limits.command_ms);
        let mut state = self.control.startup.clone();
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            let value = *state.borrow_and_update();
            match value {
                1 => return Ok(()),
                2 => return Err(Error::Startup),
                _ => {}
            }
            tokio::time::timeout_at(deadline, state.changed())
                .await
                .map_err(|_| Error::Deadline)?
                .map_err(|_| Error::Startup)?;
        }
    }
    /// Return a bounded admission handle.
    pub fn handle(&self) -> Handle {
        Handle {
            control: self.control.clone(),
        }
    }
    /// Stop admission and join listener/network/storage owners. Cancellation
    /// leaves the supervisor handle in place so this call can be retried.
    pub async fn shutdown(&mut self) -> Result<State> {
        self.control.stop();
        if let Some(state) = self.finished {
            return Ok(state);
        }
        let handle = self.supervisor.as_mut().ok_or(Error::Closed)?;
        let result = handle.await;
        self.supervisor.take();
        let result = result.map_err(Error::Join)?;
        let state = result?;
        #[cfg(test)]
        self.control.lifecycle(
            "supervisor-joined",
            self.control.metrics.workers_spawned.load(Ordering::Acquire),
            self.control.metrics.workers_joined.load(Ordering::Acquire),
            true,
            true,
            true,
        )?;
        self.finished = Some(state);
        Ok(state)
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.control.stop();
    }
}
impl Handle {
    async fn call(&self, command: Command) -> Result<Reply> {
        self.call_until(
            command,
            Instant::now() + Duration::from_millis(self.control.config.limits.command_ms),
        )
        .await
    }
    async fn call_until(&self, command: Command, outer: Instant) -> Result<Reply> {
        if Instant::now() >= outer {
            return Err(Error::Deadline);
        }
        if self.control.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if *self.control.startup.borrow() != 1 {
            return Err(Error::Startup);
        }
        let deadline = outer
            .min(Instant::now() + Duration::from_millis(self.control.config.limits.command_ms));
        let permit =
            tokio::time::timeout_at(deadline, self.control.clients.clone().acquire_owned())
                .await
                .map_err(|_| Error::Deadline)?
                .map_err(|_| Error::Closed)?;
        #[cfg(test)]
        let permit = TestPermit::new(permit, self.control.metrics.clone(), 0, 1);
        self.control
            .call(command, Some(permit), None, deadline)
            .await
    }
    /// Get current confirmed state; a leader role alone is not a read lease.
    pub async fn state(&self) -> Result<State> {
        match self.call(Command::State).await? {
            Reply::State(v) => Ok(v),
            _ => Err(Error::Peer),
        }
    }
    /// Get the actual current canonical voter configuration.
    pub async fn voters(&self) -> Result<super::membership::Voters> {
        match self.call(Command::Voters).await? {
            Reply::Voters(v) => Ok(v),
            _ => Err(Error::Peer),
        }
    }
    /// Synchronize opaque bytes locally; returned position is not a commit ACK.
    pub async fn propose(&self, bytes: Vec<u8>) -> Result<LogPosition> {
        if bytes.is_empty() || bytes.len() > self.control.config.storage.record_bytes {
            return Err(Error::InvalidConfig);
        }
        match self.call(Command::Propose(bytes)).await? {
            Reply::Position(v) => Ok(v),
            _ => Err(Error::Peer),
        }
    }
    /// Wait for this exact term/index; an absent/replaced current position fails.
    pub async fn wait_committed(&self, position: LogPosition) -> Result<()> {
        let deadline =
            Instant::now() + Duration::from_millis(self.control.config.limits.command_ms);
        loop {
            match self
                .call_until(Command::Position(position), deadline)
                .await?
            {
                Reply::Visible(true) => return Ok(()),
                Reply::Visible(false) => {}
                _ => return Err(Error::Peer),
            }
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            tokio::time::sleep_until(
                deadline.min(
                    Instant::now() + Duration::from_millis(self.control.config.limits.tick_ms),
                ),
            )
            .await;
        }
    }
    /// Fetch only synchronized committed records within configured output bounds.
    pub async fn fetch_committed(
        &self,
        from: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<Record>> {
        let l = self.control.config.storage;
        if count > l.live_entries || bytes > l.fetch_bytes {
            return Err(Error::InvalidConfig);
        }
        match self.call(Command::Fetch(from, count, bytes)).await? {
            Reply::Records(v) => Ok(v),
            _ => Err(Error::Peer),
        }
    }
    /// Select a full-prefix image through the existing authoritative Install WAL.
    pub async fn checkpoint(&self, generation: [u8; 16]) -> Result<snapshot::Descriptor> {
        match self.call(Command::Checkpoint(generation)).await? {
            Reply::Descriptor(v) => Ok(v),
            _ => Err(Error::Peer),
        }
    }
    /// Negotiate/catch up one configured observer and append its safe voter change.
    /// Completion returns ChangeReceipt; committed=false remains uncommitted.
    pub async fn add_voter(&self, key: Key) -> Result<ChangeReceipt> {
        let deadline = Instant::now() + Duration::from_millis(self.control.config.limits.quorum_ms);
        self.call_until(Command::Add(key), deadline).await?;
        loop {
            match self.call_until(Command::Addition(key), deadline).await? {
                Reply::Addition(Some(v)) => return Ok(v),
                Reply::Addition(None) => {}
                _ => return Err(Error::Peer),
            }
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            tokio::time::sleep_until(
                deadline.min(
                    Instant::now() + Duration::from_millis(self.control.config.limits.tick_ms),
                ),
            )
            .await;
        }
    }
    /// Append one configured voter removal; new-set majority remains authoritative.
    pub async fn remove_voter(&self, key: Key) -> Result<ChangeReceipt> {
        match self.call(Command::Remove(key)).await? {
            Reply::Change(v) => Ok(v),
            _ => Err(Error::Peer),
        }
    }
    /// Confirm whether the latest active configuration position is committed.
    pub async fn change_status(&self) -> Result<ChangeReceipt> {
        match self.call(Command::Change).await? {
            Reply::Change(v) => Ok(v),
            _ => Err(Error::Peer),
        }
    }
    /// Current finite ownership counters, excluding the listening socket.
    pub fn diagnostics(&self) -> Diagnostics {
        let c = &self.control;
        Diagnostics {
            network_tasks: c.tasks.load(Ordering::Acquire),
            sockets: c.sockets.load(Ordering::Acquire),
            transport_bytes: c.config.pool - c.bytes.available_permits(),
            client_slots: c.config.limits.client_slots - c.clients.available_permits(),
            stopping: c.stopped.load(Ordering::Acquire),
        }
    }
}

struct Addition {
    key: Key,
    request: FeatureRequest,
    sent: bool,
    negotiated: bool,
    deadline: u64,
    receipt: Option<ChangeReceipt>,
    failed: bool,
}
struct Owner {
    node: Node,
    config: Arc<Config>,
    matches: Vec<(Key, u64, bool)>,
    addition: Option<Addition>,
    incoming: Option<(Key, DynamicSnapshotRequest)>,
    term: u64,
    #[cfg(test)]
    metrics: Arc<TestMetrics>,
    #[cfg(test)]
    cleanup: Option<String>,
}
impl Owner {
    fn execute(&mut self, command: Command, now: u64) -> Result<Reply> {
        #[cfg(test)]
        {
            self.cleanup = None;
        }
        match command {
            Command::Tick => {
                self.node.poll(now)?;
                let votes = self.node.campaign_dynamic(now)?;
                let s = self.node.state();
                if self.term != s.election.persistent.term || s.election.role != Role::Leader {
                    for v in &mut self.matches {
                        v.1 = 0;
                        v.2 = false;
                    }
                    self.term = s.election.persistent.term;
                }
                if s.election.role == Role::Leader && s.active_term.is_none() {
                    self.node.activate_leader(now)?;
                }
                if let Some(a) = &mut self.addition {
                    if a.receipt.is_none() && !a.failed {
                        if now >= a.deadline {
                            let _ = self.node.cancel_addition(a.key);
                            a.failed = true;
                        } else if a.negotiated {
                            match self.node.add_voter(a.key, now) {
                                Ok(r) => a.receipt = Some(r),
                                Err(replication::Error::MembershipNotReady) => {}
                                Err(_) => a.failed = true,
                            }
                        }
                    }
                }
                Ok(Reply::Votes(votes.into_iter().map(Job::Vote).collect()))
            }
            Command::State => Ok(Reply::State(self.node.state())),
            Command::Voters => Ok(Reply::Voters(self.node.voters()?.clone())),
            Command::Propose(bytes) => {
                let index = self.node.propose(&bytes, now)?;
                let term = self.node.term_at(index)?.ok_or(Error::Peer)?;
                Ok(Reply::Position(LogPosition { term, index }))
            }
            Command::Position(p) => {
                if self.node.term_at(p.index)? != Some(p.term) {
                    return Err(Error::PositionMismatch);
                }
                Ok(Reply::Visible(self.node.state().committed_end >= p.index))
            }
            Command::Fetch(from, count, bytes) => Ok(Reply::Records(
                self.node.fetch_committed(from, count, bytes)?,
            )),
            Command::Checkpoint(generation) => {
                Ok(Reply::Descriptor(self.node.checkpoint(generation, now)?))
            }
            Command::Prepare(key) => {
                if self.node.state().election.role != Role::Leader {
                    return Ok(Reply::Job(None));
                }
                if let Some(a) = &mut self.addition {
                    if a.key == key && !a.failed && a.receipt.is_none() && !a.negotiated {
                        if !a.sent {
                            a.sent = true;
                            return Ok(Reply::Job(Some(Job::Feature(a.request))));
                        }
                        return Ok(Reply::Job(None));
                    }
                }
                let active = self.node.voters()?.contains(key)
                    || self
                        .addition
                        .as_ref()
                        .is_some_and(|a| a.key == key && a.negotiated && !a.failed);
                if !active {
                    return Ok(Reply::Job(None));
                }
                let matched = self
                    .matches
                    .iter()
                    .find(|(k, _, _)| *k == key)
                    .map_or(0, |(_, n, _)| *n);
                if self
                    .node
                    .selected_snapshot()?
                    .is_some_and(|d| d.base.index > matched)
                    && !self.matches.iter().any(|(k, _, skip)| *k == key && *skip)
                {
                    match self.node.prepare_dynamic_snapshot(key, now) {
                        Ok(q) => return Ok(Reply::Job(Some(Job::Image(q)))),
                        Err(replication::Error::Busy) => return Ok(Reply::Job(None)),
                        Err(e) => return Err(e.into()),
                    }
                }
                match self.node.prepare_dynamic(key, now) {
                    Ok(q) => Ok(Reply::Job(Some(Job::Append(q)))),
                    Err(replication::Error::Busy) => Ok(Reply::Job(None)),
                    Err(e) => Err(e.into()),
                }
            }
            Command::Inbound(source, message) => {
                let context = message.context().ok_or(Error::Peer)?;
                if context.leader != source || context.peer != self.config.bootstrap.local().key() {
                    return Err(Error::Peer);
                }
                let reply = match message {
                    Message::Vote(q) => Message::VoteReply(self.node.receive_dynamic_vote(q, now)?),
                    Message::Feature(q) => {
                        Message::FeatureReply(self.node.receive_feature_probe(q, now)?)
                    }
                    Message::Append(q) => Message::AppendReply(self.node.receive_dynamic(&q, now)?),
                    Message::Begin(q) => {
                        self.node.begin_dynamic_snapshot(q, now)?;
                        self.incoming = Some((source, q));
                        Message::Begun(q)
                    }
                    Message::Chunk {
                        offer,
                        offset,
                        bytes,
                    } => {
                        if self.incoming != Some((source, offer)) {
                            return Err(Error::Peer);
                        }
                        self.node
                            .receive_dynamic_snapshot_chunk(offer, offset, &bytes, now)?;
                        Message::Chunked {
                            offer,
                            offset,
                            length: bytes.len() as u32,
                        }
                    }
                    Message::Finish(q) => {
                        if self.incoming != Some((source, q)) {
                            return Err(Error::Peer);
                        }
                        let r = self.node.finish_dynamic_snapshot(q, now)?;
                        self.incoming = None;
                        Message::Finished(r)
                    }
                    _ => return Err(Error::Peer),
                };
                Ok(Reply::Message(reply))
            }
            Command::Acknowledge(message) => {
                match message {
                    Message::VoteReply(r) => {
                        self.node.acknowledge_dynamic_vote(r, now)?;
                    }
                    Message::FeatureReply(r) => {
                        let expected = self
                            .config
                            .peers
                            .iter()
                            .find(|p| p.key() == r.request.peer)
                            .ok_or(Error::Peer)?;
                        if r.voter != expected.voter {
                            return Err(Error::Peer);
                        }
                        self.node.acknowledge_feature_probe(r.clone(), now)?;
                        let a = self
                            .addition
                            .as_mut()
                            .filter(|a| a.request == r.request)
                            .ok_or(Error::Peer)?;
                        a.negotiated = true;
                    }
                    Message::AppendReply(r) => {
                        self.node.acknowledge_dynamic(r, now)?;
                        if r.response.success {
                            self.matched(r.context.peer, r.response.matched.index);
                        }
                    }
                    Message::Finished(r) => {
                        self.node.acknowledge_dynamic_snapshot(r, now)?;
                        self.matched(r.context.peer, r.response.response.matched.index);
                    }
                    _ => return Err(Error::Peer),
                }
                Ok(Reply::Done)
            }
            Command::Timeout(key, sequence, image) => {
                let released = self.node.timeout_dynamic(key, sequence).is_ok();
                if image && released {
                    if let Some(v) = self.matches.iter_mut().find(|(k, _, _)| *k == key) {
                        v.2 = true;
                    }
                }
                #[cfg(test)]
                let mut feature_released = "null";
                if let Some(a) = &mut self.addition {
                    if a.key == key && a.request.sequence == sequence && !a.negotiated {
                        let result = self.node.timeout_feature_probe(a.request);
                        #[cfg(test)]
                        {
                            feature_released = if result.is_ok() { "true" } else { "false" };
                        }
                        drop(result);
                        a.failed = true;
                    }
                }
                #[cfg(test)]
                {
                    self.cleanup = Some(format!("{{\"append_or_image_released\":{released},\"feature_released\":{feature_released},\"image_fallback_set\":{}}}", image && released));
                }
                Ok(Reply::Done)
            }
            Command::CancelVote(request) => {
                self.node.timeout_dynamic_vote(request)?;
                Ok(Reply::Done)
            }
            Command::Chunk(q) => {
                let c = self.node.dynamic_snapshot_chunk(q, now)?;
                Ok(Reply::Message(Message::Chunk {
                    offer: q,
                    offset: c.offset,
                    bytes: c.bytes,
                }))
            }
            Command::Add(key) => {
                if self
                    .addition
                    .as_ref()
                    .is_some_and(|a| a.receipt.is_none() && !a.failed)
                {
                    return Err(replication::Error::Busy.into());
                }
                let voter = self
                    .config
                    .peers
                    .iter()
                    .find(|p| p.key() == key)
                    .ok_or(Error::Peer)?
                    .voter
                    .clone();
                let request = self.node.probe_addition(voter, now)?;
                self.addition = Some(Addition {
                    key,
                    request,
                    sent: false,
                    negotiated: false,
                    deadline: now
                        .checked_add(self.config.limits.quorum_ms)
                        .ok_or(Error::InvalidConfig)?,
                    receipt: None,
                    failed: false,
                });
                Ok(Reply::Done)
            }
            Command::Addition(key) => {
                let a = self
                    .addition
                    .as_ref()
                    .filter(|a| a.key == key)
                    .ok_or(Error::Peer)?;
                if a.failed {
                    return Err(Error::Deadline);
                }
                Ok(Reply::Addition(a.receipt))
            }
            Command::Remove(key) => Ok(Reply::Change(self.node.remove_voter(key, now)?)),
            Command::Change => Ok(Reply::Change(self.node.change_status()?)),
            Command::Disconnected(source) => {
                if let Some((_, _offer)) = self.incoming.filter(|(key, _)| *key == source) {
                    #[cfg(test)]
                    {
                        self.cleanup = Some(format!(
                            "{{\"aborted_dispatch_offer\":{{\"kind\":30,\"body_hex\":\"{}\"}}}}",
                            TestTrace::body(
                                &Message::Begin(_offer),
                                self.config.limits.frame_bytes
                            )?
                        ));
                    }
                    self.node.abort_snapshot()?;
                    self.incoming = None;
                }
                Ok(Reply::Done)
            }
            Command::Stop => {
                self.node.abort_snapshot()?;
                Ok(Reply::Done)
            }
        }
    }
    fn matched(&mut self, key: Key, index: u64) {
        if let Some(v) = self.matches.iter_mut().find(|(k, _, _)| *k == key) {
            v.1 = v.1.max(index);
            v.2 = false;
        }
    }
}
fn run_owner(
    control: Arc<Control>,
    paths: Paths,
    mut receiver: mpsc::Receiver<Envelope>,
    startup: watch::Sender<u8>,
    start: std::time::Instant,
) -> Result<State> {
    let c = &control.config;
    #[cfg(test)]
    if control.metrics.clock.set(start).is_err() {
        return Err(Error::InvalidConfig);
    }
    let mut config = replication::Config::new(c.controller.clone());
    config.limits = c.storage.node()?;
    config.max_queued_requests = c.limits.client_slots;
    config.quorum_timeout_ms = c.limits.quorum_ms;
    let opened = (|| {
        let identity = snapshot::Identity::dynamic(
            c.group.cluster.clone(),
            c.group.topic.clone(),
            c.group.partition,
            c.bootstrap.genesis().clone(),
        )
        .map_err(replication::Error::from)?;
        let store = snapshot::Store::open(&paths.images, identity, c.storage.images)
            .map_err(replication::Error::from)?;
        Ok::<_, Error>(Node::open_dynamic(
            &paths.wal,
            &paths.election,
            config,
            c.bootstrap.clone(),
            Some(store),
            0,
        )?)
    })();
    let node = match opened {
        Ok(node) => node,
        Err(e) => {
            let _ = startup.send(2);
            control.stop();
            return Err(e);
        }
    };
    let mut matches = Vec::new();
    matches
        .try_reserve_exact(c.peers.len())
        .map_err(|_| Error::Allocation)?;
    matches.extend(c.peers.iter().map(|p| (p.key(), 0, false)));
    let mut owner = Owner {
        node,
        config: c.clone(),
        matches,
        addition: None,
        incoming: None,
        term: 0,
        #[cfg(test)]
        metrics: control.metrics.clone(),
        #[cfg(test)]
        cleanup: None,
    };
    #[cfg(test)]
    let mut trace = TestTrace::open(&paths, c, receiver.max_capacity())?;
    let _ = startup.send(1);
    while let Some(envelope) = receiver.blocking_recv() {
        let Envelope {
            command,
            reply,
            permit,
            transport,
            started,
            deadline,
            #[cfg(test)]
            delivery_gate,
            #[cfg(test)]
            _counter,
        } = envelope;
        let stop = matches!(command, Command::Stop);
        let internal = matches!(
            command,
            Command::Timeout(..)
                | Command::CancelVote(..)
                | Command::Disconnected(..)
                | Command::Stop
        );
        if !internal && reply.is_closed() {
            continue;
        }
        if !internal && deadline.is_some_and(|d| Instant::now() >= d) {
            let _ = reply.send(Delivered {
                result: Err(Error::Deadline),
                _permit: permit,
                _transport: transport,
            });
            continue;
        }
        if control.stopped.load(Ordering::Acquire) && !internal {
            let _ = reply.send(Delivered {
                result: Err(Error::Closed),
                _permit: permit,
                _transport: transport,
            });
            continue;
        }
        started.store(true, Ordering::Release);
        let now = u64::try_from(start.elapsed().as_millis()).map_err(|_| Error::InvalidConfig)?;
        #[cfg(test)]
        let captured = TestTrace::input(&command, c.limits.frame_bytes)?;
        let result = owner.execute(command, now);
        // Tests pause only after the actual Node operation has completed. This
        // cannot inject a grant, alter Node state, or expose production handles.
        #[cfg(test)]
        if let Some(gate) = delivery_gate {
            let requests = match &result {
                Ok(Reply::Votes(jobs)) => jobs
                    .iter()
                    .filter_map(|j| if let Job::Vote(q) = j { Some(*q) } else { None })
                    .collect(),
                _ => Vec::new(),
            };
            let _ = gate.ready.send((requests, owner.node.state()));
            gate.release.recv().map_err(|_| Error::Closed)?;
        }
        #[cfg(test)]
        if let Some(trace) = &mut trace {
            trace.record(&owner, captured, &result, now, &paths)?;
        }
        if let Err(delivery) = reply.send(Delivered {
            result,
            _permit: permit,
            _transport: transport,
        }) {
            match delivery.result {
                Ok(Reply::Job(Some(job))) => {
                    cancel_owner_job(
                        &mut owner,
                        job,
                        now,
                        #[cfg(test)]
                        &mut trace,
                        #[cfg(test)]
                        &paths,
                    )?;
                }
                Ok(Reply::Votes(jobs)) => {
                    for job in jobs {
                        cancel_owner_job(
                            &mut owner,
                            job,
                            now,
                            #[cfg(test)]
                            &mut trace,
                            #[cfg(test)]
                            &paths,
                        )?;
                    }
                }
                _ => {}
            }
        }
        if stop {
            break;
        }
    }
    owner.node.abort_snapshot()?;
    // Dropping the sole Node closes journals/readers after its last synchronous
    // operation. Store::drop removes any staged partial generation.
    Ok(owner.node.state())
}
fn cancel_owner_job(
    owner: &mut Owner,
    job: Job,
    now: u64,
    #[cfg(test)] trace: &mut Option<TestTrace>,
    #[cfg(test)] paths: &Paths,
) -> Result<()> {
    let command = job.cancellation();
    #[cfg(test)]
    let captured = TestTrace::input(&command, owner.config.limits.frame_bytes)?;
    let result = owner.execute(command, now);
    #[cfg(test)]
    if let Some(trace) = trace {
        trace.record(owner, captured, &result, now, paths)?;
    }
    // A previously acknowledged/stepped-down exact request is already absent;
    // rejecting its cancellation does not create a fabricated grant/contact.
    drop(result);
    Ok(())
}

async fn supervise(
    listener: TcpListener,
    control: Arc<Control>,
    owner: JoinHandle<Result<State>>,
) -> Result<State> {
    let mut startup = control.startup.clone();
    while *startup.borrow() != 1 {
        if *startup.borrow() == 2 || control.stopped.load(Ordering::Acquire) {
            break;
        }
        tokio::select! {r=startup.changed()=>{if r.is_err(){break;}},_ = control.stopping()=>break}
    }
    let mut workers = JoinSet::new();
    let mut routes = Vec::new();
    let mut failure = None;
    #[cfg(test)]
    let (mut spawned, mut joined) = (0usize, 0usize);
    let incoming = Arc::new(Semaphore::new(control.config.limits.incoming));
    let identities: Arc<Vec<AtomicBool>> = Arc::new(
        control
            .config
            .peers
            .iter()
            .map(|_| AtomicBool::new(false))
            .collect(),
    );
    if *startup.borrow() == 1 && !control.stopped.load(Ordering::Acquire) {
        if routes
            .try_reserve_exact(control.config.peers.len())
            .is_err()
        {
            control.stop();
            failure = Some(Error::Allocation);
        }
        for peer in control
            .config
            .peers
            .iter()
            .filter(|_| !control.stopped.load(Ordering::Acquire))
        {
            let (tx, rx) = mpsc::channel(1);
            routes.push((peer.key(), tx));
            let c = control.clone();
            let p = peer.clone();
            workers.spawn(async move {
                outbound(c, p, rx).await;
            });
            #[cfg(test)]
            {
                spawned += 1;
            }
        }
        let mut tick = tokio::time::interval(Duration::from_millis(control.config.limits.tick_ms));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while !control.stopped.load(Ordering::Acquire) {
            tokio::select! {
                _ = control.stopping()=>break,
                _ = tick.tick()=>{
                    match control.internal(Command::Tick).await {
                        Ok(Reply::Votes(jobs))=>dispatch_votes(&control,&routes,jobs).await,
                        Ok(_)=>{},Err(_)=>{control.stop();break;}
                    }
                }
                accepted=listener.accept()=>{
                    match accepted {
                        Ok((socket,_))=>{
                            if let Ok(permit)=incoming.clone().try_acquire_owned(){let c=control.clone();let keys=identities.clone();workers.spawn(async move{serve(c,socket,permit,keys).await;});
                                #[cfg(test)] { spawned += 1; }}
                            else{drop(socket);}
                        }
                        Err(_)=>{control.stop();break;}
                    }
                }
                result=workers.join_next(),if !workers.is_empty()=>{if let Some(result)=result {
                    #[cfg(test)] { joined += 1; }
                    if let Err(e)=result{failure=Some(Error::Join(e));control.stop();break;}
                }}
            }
        }
    }
    control.stop();
    drop(listener);
    drop(routes);
    while let Some(result) = workers.join_next().await {
        #[cfg(test)]
        {
            joined += 1;
        }
        if let Err(e) = result {
            if failure.is_none() {
                failure = Some(Error::Join(e));
            }
        }
    }
    let stopped = control.reliable(Command::Stop).await;
    let state = owner.await.map_err(Error::Join)??;
    stopped?;
    if let Some(error) = failure {
        return Err(error);
    }
    #[cfg(test)]
    {
        control
            .metrics
            .workers_spawned
            .store(spawned, Ordering::Release);
        control
            .metrics
            .workers_joined
            .store(joined, Ordering::Release);
        control.lifecycle("workers-owner-joined", spawned, joined, true, false, true)?;
    }
    Ok(state)
}
async fn dispatch_votes(control: &Control, routes: &[(Key, mpsc::Sender<Job>)], jobs: Vec<Job>) {
    for job in jobs {
        match routes.iter().find(|(k, _)| *k == job.peer()) {
            Some((_, tx)) => {
                if let Err(error) = tx.try_send(job) {
                    let _ = control.cancel_job(error.into_inner()).await;
                }
            }
            None => {
                let _ = control.cancel_job(job).await;
            }
        }
    }
}
struct Counter {
    control: Arc<Control>,
    socket: bool,
    #[cfg(test)]
    _counter: TestCounter,
}
impl Counter {
    fn task(c: &Arc<Control>) -> Self {
        c.tasks.fetch_add(1, Ordering::AcqRel);
        Self {
            control: c.clone(),
            socket: false,
            #[cfg(test)]
            _counter: TestCounter::new(c.metrics.clone(), 2, 1),
        }
    }
    fn socket(c: &Arc<Control>) -> Self {
        c.sockets.fetch_add(1, Ordering::AcqRel);
        Self {
            control: c.clone(),
            socket: true,
            #[cfg(test)]
            _counter: TestCounter::new(c.metrics.clone(), 3, 1),
        }
    }
}
impl Drop for Counter {
    fn drop(&mut self) {
        if self.socket {
            self.control.sockets.fetch_sub(1, Ordering::AcqRel);
        } else {
            self.control.tasks.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
async fn read_frame(
    socket: &mut TcpStream,
    limit: usize,
    max_records: usize,
    deadline: Instant,
) -> Result<Frame> {
    tokio::time::timeout_at(deadline, async {
        let length = socket.read_u32().await? as usize;
        if !(28..=limit).contains(&length) {
            return Err(Error::Peer);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| Error::Allocation)?;
        bytes.resize(length, 0);
        socket.read_exact(&mut bytes).await?;
        Ok(peer_codec::decode(&bytes, limit, max_records)?)
    })
    .await
    .map_err(|_| Error::Deadline)?
}
async fn write_frame(
    socket: &mut TcpStream,
    frame: &Frame,
    limit: usize,
    deadline: Instant,
) -> Result<()> {
    let bytes = peer_codec::encode(frame, limit)?;
    tokio::time::timeout_at(deadline, async {
        socket.write_u32(bytes.len() as u32).await?;
        socket.write_all(&bytes).await?;
        Ok::<_, Error>(())
    })
    .await
    .map_err(|_| Error::Deadline)?
}
async fn hello(
    socket: &mut TcpStream,
    control: &Control,
    peer: Key,
    deadline: Instant,
) -> Result<()> {
    let h = Hello {
        source: control.config.bootstrap.local().key(),
        target: peer,
        group: control.config.group.clone(),
    };
    write_frame(
        socket,
        &Frame {
            rpc: 0,
            message: Message::Hello(h.clone()),
        },
        control.config.limits.frame_bytes,
        deadline,
    )
    .await?;
    let response = read_frame(
        socket,
        control.config.limits.frame_bytes,
        control.config.storage.live_entries,
        deadline,
    )
    .await?;
    if response.rpc != 0 || response.message != Message::HelloAck(h) {
        return Err(Error::Peer);
    }
    Ok(())
}
async fn rpc(
    socket: &mut TcpStream,
    message: Message,
    id: &mut u64,
    control: &Control,
    deadline: Instant,
) -> Result<Message> {
    *id = id.checked_add(1).ok_or(Error::Peer)?;
    let frame = Frame { rpc: *id, message };
    write_frame(socket, &frame, control.config.limits.frame_bytes, deadline).await?;
    let response = read_frame(
        socket,
        control.config.limits.frame_bytes,
        control.config.storage.live_entries,
        deadline,
    )
    .await?;
    if response.rpc != frame.rpc || !frame.message.matches_reply(&response.message) {
        return Err(Error::Peer);
    }
    Ok(response.message)
}
async fn outbound(control: Arc<Control>, peer: Peer, mut jobs: mpsc::Receiver<Job>) {
    let _task = Counter::task(&control);
    let mut socket: Option<(TcpStream, Counter, u64)> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(control.config.limits.heartbeat_ms));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let admission = Instant::now() + Duration::from_millis(control.config.limits.rpc_ms);
        let reservation =
            tokio::select! {_ = control.stopping()=>break,r=control.reserve_frame(admission)=>r};
        let Ok(reservation) = reservation else {
            continue;
        };
        let job = tokio::select! {
            _ = control.stopping()=>break,
            value=jobs.recv()=>match value{Some(j)=>Some(j),None=>break},
            _ = tick.tick()=>match control.call(Command::Prepare(peer.key()),None,Some(reservation.clone()),admission).await{Ok(Reply::Job(j))=>j,_=>None},
        };
        let Some(job) = job else {
            continue;
        };
        let cancellation = job.cancellation();
        let deadline = Instant::now()
            + Duration::from_millis(if matches!(job, Job::Image(_)) {
                control.config.limits.transfer_ms
            } else {
                control.config.limits.rpc_ms
            });
        if socket.is_none() {
            let counter = Counter::socket(&control);
            let connect_deadline = deadline
                .min(Instant::now() + Duration::from_millis(control.config.limits.connect_ms));
            match tokio::time::timeout_at(connect_deadline, TcpStream::connect(peer.address)).await
            {
                Ok(Ok(mut s)) => {
                    if hello(&mut s, &control, peer.key(), deadline).await.is_ok() {
                        socket = Some((s, counter, 0));
                    }
                }
                _ => {}
            }
        }
        let result = if let Some((s, _, id)) = &mut socket {
            tokio::select! {
                _ = control.stopping()=>Err(Error::Closed),
                result=send_job(s,&control,job,id,deadline,&reservation)=>result,
            }
        } else {
            Err(Error::Peer)
        };
        match result {
            Ok(response) => {
                if control
                    .call(
                        Command::Acknowledge(response),
                        None,
                        Some(reservation.clone()),
                        deadline,
                    )
                    .await
                    .is_err()
                {
                    socket.take();
                    let _ = control.reliable(cancellation).await;
                }
            }
            Err(_) => {
                socket.take();
                let _ = control.reliable(cancellation).await;
            }
        }
    }
    drop(socket);
    jobs.close();
    while let Some(job) = jobs.recv().await {
        let _ = control.cancel_job(job).await;
    }
}
async fn send_job(
    socket: &mut TcpStream,
    control: &Control,
    job: Job,
    id: &mut u64,
    deadline: Instant,
    reservation: &Arc<AdmissionPermit>,
) -> Result<Message> {
    match job {
        Job::Vote(q) => rpc(socket, Message::Vote(q), id, control, deadline).await,
        Job::Feature(q) => rpc(socket, Message::Feature(q), id, control, deadline).await,
        Job::Append(q) => rpc(socket, Message::Append(q), id, control, deadline).await,
        Job::Image(offer) => {
            rpc(socket, Message::Begin(offer), id, control, deadline).await?;
            let mut offset = 0;
            while offset < offer.request.descriptor.bytes {
                let reply = control
                    .call(
                        Command::Chunk(offer),
                        None,
                        Some(reservation.clone()),
                        deadline,
                    )
                    .await?;
                let Reply::Message(Message::Chunk {
                    offer: q,
                    offset: o,
                    bytes,
                }) = reply
                else {
                    return Err(Error::Peer);
                };
                if q != offer || o != offset || bytes.is_empty() {
                    return Err(Error::Peer);
                }
                offset = offset.checked_add(bytes.len() as u64).ok_or(Error::Peer)?;
                if offset > offer.request.descriptor.bytes {
                    return Err(Error::Peer);
                }
                rpc(
                    socket,
                    Message::Chunk {
                        offer,
                        offset: o,
                        bytes,
                    },
                    id,
                    control,
                    deadline,
                )
                .await?;
            }
            rpc(socket, Message::Finish(offer), id, control, deadline).await
        }
    }
}
struct IdentitySlot {
    slots: Arc<Vec<AtomicBool>>,
    index: usize,
}
impl Drop for IdentitySlot {
    fn drop(&mut self) {
        self.slots[self.index].store(false, Ordering::Release);
    }
}
async fn serve(
    control: Arc<Control>,
    mut socket: TcpStream,
    _permit: OwnedSemaphorePermit,
    slots: Arc<Vec<AtomicBool>>,
) {
    let _task = Counter::task(&control);
    let _socket = Counter::socket(&control);
    let deadline = Instant::now() + Duration::from_millis(control.config.limits.rpc_ms);
    let reservation = control.reserve_frame(deadline).await;
    let Ok(reservation) = reservation else {
        return;
    };
    let first = tokio::select! {_ = control.stopping()=>return,r=read_frame(&mut socket,control.config.limits.frame_bytes,control.config.storage.live_entries,deadline)=>r};
    let Ok(Frame {
        rpc: 0,
        message: Message::Hello(h),
    }) = first
    else {
        return;
    };
    if h.group != control.config.group || h.target != control.config.bootstrap.local().key() {
        return;
    }
    let Some(index) = control
        .config
        .peers
        .iter()
        .position(|p| p.key() == h.source)
    else {
        return;
    };
    if slots[index]
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let _identity = IdentitySlot { slots, index };
    let source = h.source;
    if write_frame(
        &mut socket,
        &Frame {
            rpc: 0,
            message: Message::HelloAck(h),
        },
        control.config.limits.frame_bytes,
        deadline,
    )
    .await
    .is_err()
    {
        return;
    }
    drop(reservation);
    let mut last_rpc = 0;
    loop {
        let deadline = Instant::now() + Duration::from_millis(control.config.limits.rpc_ms);
        let reservation =
            tokio::select! {_ = control.stopping()=>break,r=control.reserve_frame(deadline)=>r};
        let Ok(reservation) = reservation else {
            break;
        };
        let frame = tokio::select! {_ = control.stopping()=>break,r=read_frame(&mut socket,control.config.limits.frame_bytes,control.config.storage.live_entries,deadline)=>r};
        let Ok(frame) = frame else {
            break;
        };
        if frame.rpc <= last_rpc || !frame.message.request() {
            break;
        }
        last_rpc = frame.rpc;
        let kind = frame.message.kind();
        let response = match control
            .call(
                Command::Inbound(source, frame.message),
                None,
                Some(reservation.clone()),
                deadline,
            )
            .await
        {
            Ok(Reply::Message(message)) => message,
            _ => Message::Failure {
                request_kind: kind,
                code: 1,
            },
        };
        if control.stopped.load(Ordering::Acquire) || Instant::now() >= deadline {
            break;
        }
        if write_frame(
            &mut socket,
            &Frame {
                rpc: last_rpc,
                message: response,
            },
            control.config.limits.frame_bytes,
            deadline,
        )
        .await
        .is_err()
        {
            break;
        }
    }
    let _ = control.reliable(Command::Disconnected(source)).await;
}

// Owner-local test evidence; no production mutable-storage or fault API.
// Its additional bounded temporary encoding/hex envelope is separate from the
// production transport counter and must be recorded in test resource receipts.
#[cfg(test)]
struct TestTrace {
    root: PathBuf,
    file: std::fs::File,
    ordinal: u64,
    disk_bytes: u64,
    last: (usize, usize, bool),
}
#[cfg(test)]
impl TestTrace {
    fn capture_root(paths: &Paths, c: &Config) -> Result<Option<PathBuf>> {
        let Some(base) = std::env::var_os("PL_PEER_RUNTIME_CAPTURE_DIR") else {
            return Ok(None);
        };
        if base.as_encoded_bytes().len() > 4096 {
            return Err(Error::InvalidConfig);
        }
        let case = paths
            .wal
            .parent()
            .and_then(Path::file_name)
            .and_then(|n| n.to_str())
            .ok_or(Error::InvalidConfig)?;
        Ok(Some(PathBuf::from(base).join(format!(
            "{}-{case}-{}",
            std::process::id(),
            c.bootstrap.local().key().id
        ))))
    }
    fn key(key: Key) -> Result<String> {
        Ok(format!(
            "{{\"id\":{},\"directory\":\"{}\"}}",
            key.id,
            Self::hex(&key.directory)?
        ))
    }
    fn descriptor(d: snapshot::Descriptor) -> Result<String> {
        Ok(format!("{{\"generation\":\"{}\",\"base_term\":{},\"base_index\":{},\"records\":{},\"payload_bytes\":{},\"bytes\":{},\"checksum\":{}}}", Self::hex(&d.generation)?, d.base.term, d.base.index, d.records, d.payload_bytes, d.bytes, d.checksum))
    }
    fn open(paths: &Paths, c: &Config, command_queue_capacity: usize) -> Result<Option<Self>> {
        use std::io::Write;
        let Some(base) = std::env::var_os("PL_PEER_RUNTIME_CAPTURE_DIR") else {
            return Ok(None);
        };
        let extra = checked_sum(&[
            product(c.limits.frame_bytes, 6)?,
            product(MAX_CONFIGURATION_BYTES, 2)?,
        ])?;
        if checked_sum(&[c.managed, extra])? > MANAGED_CEILING {
            return Err(Error::InvalidConfig);
        }
        if base.as_encoded_bytes().len() > 4096 {
            return Err(Error::InvalidConfig);
        }
        let local = c.bootstrap.local().key();
        let root = Self::capture_root(paths, c)?.ok_or(Error::InvalidConfig)?;
        std::fs::create_dir_all(&root)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("owner.jsonl"))?;
        let source = std::env::var("PL_PEER_RUNTIME_SOURCE_SHA")
            .unwrap_or_else(|_| "unqualified-WORK-draft".into());
        if source.len() > 128
            || source
                .bytes()
                .any(|b| !b.is_ascii_alphanumeric() && b != b'-')
        {
            return Err(Error::InvalidConfig);
        }
        writeln!(file,concat!(
            "{{\"schema_version\":1,\"capture_revision\":2,\"source_sha\":\"{source}\",\"local_id\":{local_id},\"directory\":\"{directory}\",\"genesis_hex\":\"{genesis}\",",
            "\"clock_basis\":\"process-local monotonic Instant; restart begins a new clock epoch\",\"pid\":{pid},",
            "\"trace_layer\":\"exclusive Node owner; typed bodies are not TCP packet captures\",",
            "\"runtime_settings\":{{\"frame_bytes\":{frame_bytes},\"record_bytes\":{record_bytes},\"chunk_bytes\":{chunk_bytes},",
            "\"live_entries\":{live_entries},\"live_bytes\":{live_bytes},\"operations\":{operations},\"wal_bytes\":{wal_bytes},\"fetch_bytes\":{fetch_bytes},",
            "\"image_bytes\":{image_bytes},\"image_records\":{image_records},\"image_record_bytes\":{image_record_bytes},\"image_payload_bytes\":{image_payload_bytes},",
            "\"image_chunk_bytes\":{image_chunk_bytes},\"image_decoded_bytes\":{image_decoded_bytes},\"image_generations\":{image_generations},\"image_disk_bytes\":{image_disk_bytes},",
            "\"peer_count\":{peer_count},\"incoming_slots\":{incoming_slots},\"client_slots\":{client_slots},\"command_queue_capacity\":{command_queue_capacity},",
            "\"peer_mailbox_slots\":1,\"dns_admissions\":0,\"canonical_configuration_max_bytes\":{configuration_max},\"task_bound\":{task_bound},\"socket_bound_with_listener\":{socket_bound},",
            "\"quorum_ms\":{quorum_ms},\"quorum_timeout_ms\":{quorum_ms},\"election_min_ms\":{election_min_ms},\"election_max_ms\":{election_max_ms},",
            "\"tick_ms\":{tick_ms},\"heartbeat_ms\":{heartbeat_ms},\"connect_ms\":{connect_ms},\"rpc_ms\":{rpc_ms},\"transfer_ms\":{transfer_ms},\"command_ms\":{command_ms},",
            "\"transport_charge_bytes\":{charge},\"transport_pool_bytes\":{pool},\"managed_bytes\":{managed},\"managed_ceiling_bytes\":{ceiling},\"extra_test_encode_hex_bytes\":{extra},\"test_resource_observation\":\"constructed RAII ownership high-water counters; excludes acquired-but-unpublished permit window, allocator, RSS and OS buffers\",\"command_owner_bound\":{command_owner_bound},\"test_counter_fixed_bytes\":{test_counter_fixed_bytes}}}}}"
        ),source=source,local_id=local.id,directory=Self::hex(&local.directory)?,genesis=Self::hex(&c.group.genesis)?,pid=std::process::id(),
            frame_bytes=c.limits.frame_bytes,record_bytes=c.storage.record_bytes,chunk_bytes=c.storage.chunk_bytes,
            live_entries=c.storage.live_entries,live_bytes=c.storage.live_bytes,operations=c.storage.operations,wal_bytes=c.storage.wal_bytes,fetch_bytes=c.storage.fetch_bytes,
            image_bytes=c.storage.images.image_bytes(),image_records=c.storage.images.records(),image_record_bytes=c.storage.images.record_bytes(),image_payload_bytes=c.storage.images.payload_bytes(),
            image_chunk_bytes=c.storage.images.chunk_bytes(),image_decoded_bytes=c.storage.images.decoded_bytes(),image_generations=c.storage.images.generations(),image_disk_bytes=c.storage.images.disk_bytes(),
            peer_count=c.peers.len(),incoming_slots=c.limits.incoming,client_slots=c.limits.client_slots,command_queue_capacity=command_queue_capacity,
            configuration_max=MAX_CONFIGURATION_BYTES,task_bound=c.task_bound(),socket_bound=checked_sum(&[1,c.peers.len(),c.limits.incoming])?,
            quorum_ms=c.limits.quorum_ms,election_min_ms=c.limits.election_min_ms,election_max_ms=c.limits.election_max_ms,
            tick_ms=c.limits.tick_ms,heartbeat_ms=c.limits.heartbeat_ms,connect_ms=c.limits.connect_ms,rpc_ms=c.limits.rpc_ms,transfer_ms=c.limits.transfer_ms,command_ms=c.limits.command_ms,
            charge=c.charge,pool=c.pool,managed=c.managed,ceiling=MANAGED_CEILING,extra=extra,
            command_owner_bound=checked_sum(&[product(command_queue_capacity,2)?,1])?,test_counter_fixed_bytes=std::mem::size_of::<TestMetrics>())?;
        file.sync_all()?;
        let disk_bytes = file.metadata()?.len();
        Ok(Some(Self {
            root,
            file,
            ordinal: 0,
            disk_bytes,
            last: (0, 0, false),
        }))
    }
    fn hex(bytes: &[u8]) -> Result<String> {
        use std::fmt::Write;
        let mut out = String::new();
        out.try_reserve_exact(bytes.len().checked_mul(2).ok_or(Error::InvalidConfig)?)
            .map_err(|_| Error::Allocation)?;
        for b in bytes {
            write!(out, "{b:02x}").map_err(|_| Error::Peer)?;
        }
        Ok(out)
    }
    fn body(message: &Message, limit: usize) -> Result<String> {
        let bytes = peer_codec::encode(
            &Frame {
                rpc: 1,
                message: message.clone(),
            },
            limit,
        )?;
        Self::hex(&bytes[24..bytes.len() - 4])
    }
    fn input(
        command: &Command,
        limit: usize,
    ) -> Result<(String, Option<(u8, String)>, Option<String>)> {
        let name = match command {
            Command::Tick => "tick",
            Command::State => "state",
            Command::Voters => "voters",
            Command::Propose(_) => "propose",
            Command::Position(_) => "position",
            Command::Fetch(..) => "fetch",
            Command::Checkpoint(_) => "checkpoint",
            Command::Prepare(_) => "prepare",
            Command::Inbound(..) => "receive",
            Command::Acknowledge(_) => "ack",
            Command::Timeout(..) => "timeout",
            Command::CancelVote(_) => "cancel-vote",
            Command::Chunk(_) => "image-read",
            Command::Add(_) => "add",
            Command::Addition(_) => "addition-status",
            Command::Remove(_) => "remove",
            Command::Change => "change-status",
            Command::Disconnected(_) => "disconnect",
            Command::Stop => "stop",
        }
        .to_owned();
        let typed = match command {
            Command::Inbound(_, m) | Command::Acknowledge(m) => {
                Some((m.kind(), Self::body(m, limit)?))
            }
            Command::CancelVote(q) => Some((10, Self::body(&Message::Vote(*q), limit)?)),
            _ => None,
        };
        let target = match command {
            Command::Timeout(peer, sequence, image) => Some(format!(
                "{{\"peer\":{},\"sequence\":{sequence},\"image\":{image}}}",
                Self::key(*peer)?
            )),
            Command::Disconnected(peer) => Some(format!("{{\"peer\":{}}}", Self::key(*peer)?)),
            Command::Prepare(peer) | Command::Add(peer) | Command::Remove(peer) => {
                Some(format!("{{\"peer\":{}}}", Self::key(*peer)?))
            }
            _ => None,
        };
        Ok((name, typed, target))
    }
    fn record(
        &mut self,
        owner: &Owner,
        input: (String, Option<(u8, String)>, Option<String>),
        result: &Result<Reply>,
        now: u64,
        paths: &Paths,
    ) -> Result<()> {
        use std::io::Write;
        let s = owner.node.state();
        let counts = (s.wal_durable_ops, s.election_durable_states, s.poisoned);
        let interesting = counts != self.last
            || matches!(
                input.0.as_str(),
                "prepare"
                    | "receive"
                    | "ack"
                    | "timeout"
                    | "cancel-vote"
                    | "disconnect"
                    | "image-read"
                    | "add"
                    | "remove"
                    | "stop"
            );
        if !interesting {
            return Ok(());
        }
        if self.ordinal >= 50000 {
            return Err(Error::InvalidConfig);
        }
        let mut generated = Vec::new();
        if let Ok(reply) = result {
            match reply {
                Reply::Job(Some(j)) => generated.push(match j {
                    Job::Vote(q) => Message::Vote(*q),
                    Job::Feature(q) => Message::Feature(*q),
                    Job::Append(q) => Message::Append(q.clone()),
                    Job::Image(q) => Message::Begin(*q),
                }),
                Reply::Message(m) => generated.push(m.clone()),
                Reply::Votes(jobs) => {
                    for j in jobs {
                        if let Job::Vote(q) = j {
                            generated.push(Message::Vote(*q));
                        }
                    }
                }
                _ => {}
            }
        }
        let command = input.0;
        let target = input.2.unwrap_or_else(|| "null".into());
        let input = match input.1 {
            Some((kind, body)) => format!("{{\"kind\":{kind},\"body_hex\":\"{body}\"}}"),
            None => "null".into(),
        };
        let mut output = String::new();
        for m in generated {
            if !output.is_empty() {
                output.push(',');
            }
            output.push_str(&format!(
                "{{\"kind\":{},\"body_hex\":\"{}\"}}",
                m.kind(),
                Self::body(&m, owner.config.limits.frame_bytes)?
            ));
        }
        let voters = match owner.node.voters() {
            Ok(v) => format!("\"{}\"", Self::hex(&v.encode().map_err(|_| Error::Peer)?)?),
            Err(_) => "null".into(),
        };
        let (vote_available, vote) = match owner.node.voted_directory() {
            Ok(v) => (
                true,
                v.map_or_else(
                    || Ok("null".to_owned()),
                    |k| {
                        Ok::<_, Error>(format!(
                            "{{\"id\":{},\"directory\":\"{}\"}}",
                            k.id,
                            Self::hex(&k.directory)?
                        ))
                    },
                )?,
            ),
            Err(_) => (false, "null".into()),
        };
        let selected = owner
            .node
            .selected_snapshot()
            .ok()
            .flatten()
            .map(Self::descriptor)
            .transpose()?
            .unwrap_or_else(|| "null".into());
        let cleanup = owner.cleanup.as_deref().unwrap_or("null");
        let resources = owner.metrics.json()?;
        let row=format!("{{\"ordinal\":{},\"now_ms\":{now},\"command\":\"{command}\",\"target\":{target},\"cleanup\":{cleanup},\"selected_snapshot\":{selected},\"resource_owners\":{resources},\"input\":{input},\"typed_output\":[{output}],\"result_ok\":{},\"term\":{},\"role\":\"{:?}\",\"ready\":{},\"poisoned\":{},\"last_term\":{},\"last_index\":{},\"committed_end\":{},\"voted_directory_available\":{vote_available},\"voted_for\":{vote},\"configuration_hex\":{voters},\"wal_ops\":{},\"election_states\":{}}}\n",self.ordinal,result.is_ok(),s.election.persistent.term,s.election.role,s.ready,s.poisoned,s.last_position.term,s.last_position.index,s.committed_end,s.wal_durable_ops,s.election_durable_states);
        self.charge(row.len() as u64)?;
        self.file.write_all(row.as_bytes())?;
        self.file.sync_all()?;
        if counts != self.last {
            let root = self.root.join(format!("checkpoint-{}", self.ordinal));
            std::fs::create_dir(&root)?;
            for (source, name) in [
                (&paths.wal, "metadata.wal"),
                (&paths.election, "election.wal"),
            ] {
                self.charge(std::fs::metadata(source)?.len())?;
                std::fs::copy(source, root.join(name))?;
                std::fs::File::open(root.join(name))?.sync_all()?;
            }
            let images = root.join("images");
            std::fs::create_dir(&images)?;
            for entry in std::fs::read_dir(&paths.images)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    self.charge(entry.metadata()?.len())?;
                    let destination = images.join(entry.file_name());
                    std::fs::copy(entry.path(), &destination)?;
                    std::fs::File::open(destination)?.sync_all()?;
                }
            }
            std::fs::File::open(images)?.sync_all()?;
            std::fs::File::open(root)?.sync_all()?;
            self.last = counts;
        }
        self.ordinal += 1;
        Ok(())
    }
    fn charge(&mut self, n: u64) -> Result<()> {
        self.disk_bytes = self.disk_bytes.checked_add(n).ok_or(Error::InvalidConfig)?;
        if self.disk_bytes > 16 * 1024 * 1024 {
            Err(Error::InvalidConfig)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate as partitionline_broker;
    // Run the public TCP scenarios in the unit artifact as well, enabling the
    // owner-local raw/WAL hook without adding a production fault surface.
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/common/raft_runtime.rs"
    ));
}

#[cfg(test)]
mod ownership_tests {
    use super::super::membership::{Endpoint, Voters};
    use super::*;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
    struct LocalOwner {
        control: Arc<Control>,
        join: Option<std::thread::JoinHandle<Result<State>>>,
        root: PathBuf,
    }
    impl LocalOwner {
        async fn open(local: u32, count: u32, capacity: usize) -> TestResult<Self> {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "partitionline76-owner-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&root)?;
            let voters = (0..count)
                .map(|id| {
                    Ok(Voter::new(
                        Key::new(id, [(id + 1) as u8; 16])?,
                        vec![Endpoint::new(
                            "CONTROLLER".into(),
                            "127.0.0.1".into(),
                            6000 + id as u16,
                        )?],
                        0,
                        1,
                    )?)
                })
                .collect::<TestResult<Vec<_>>>()?;
            let local_voter = voters.get(local as usize).ok_or("local voter")?.clone();
            let genesis = Voters::new(0, LogPosition::default(), 1, voters.clone())?;
            let bootstrap = Bootstrap::new(local_voter, genesis, "CONTROLLER".into())?;
            let peers = voters
                .into_iter()
                .filter(|v| v.key().id != local)
                .map(|v| {
                    let address =
                        SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 6000 + v.key().id as u16);
                    Ok(Peer::new(v, address)?)
                })
                .collect::<TestResult<Vec<_>>>()?;
            let controller =
                protocol::Config::new(local, vec![local], "owner-control-test".into())?;
            let config = Arc::new(Config::new(
                controller,
                bootstrap,
                peers,
                StorageLimits::standard()?,
                Limits {
                    election_min_ms: 1,
                    election_max_ms: 1,
                    command_ms: 20,
                    ..Limits::default()
                },
            )?);
            let (tx, rx) = mpsc::channel(capacity);
            let (ready_tx, ready_rx) = watch::channel(0);
            let paths = Paths::new(
                root.join("metadata.wal"),
                root.join("election.wal"),
                root.join("images"),
            )?;
            let control = Arc::new(Control {
                stopped: AtomicBool::new(false),
                notify: Notify::new(),
                commands: tx,
                clients: Arc::new(Semaphore::new(config.limits.client_slots)),
                bytes: Arc::new(Semaphore::new(config.pool)),
                startup: ready_rx,
                tasks: AtomicUsize::new(0),
                sockets: AtomicUsize::new(0),
                metrics: Arc::new(TestMetrics::default()),
                capture_root: TestTrace::capture_root(&paths, &config)?,
                config,
            });
            let child_control = control.clone();
            let join = std::thread::spawn(move || {
                run_owner(
                    child_control,
                    paths,
                    rx,
                    ready_tx,
                    std::time::Instant::now(),
                )
            });
            let mut ready = control.startup.clone();
            while *ready.borrow() != 1 {
                if *ready.borrow() == 2 {
                    return Err("owner startup".into());
                }
                ready.changed().await?;
            }
            Ok(Self {
                control,
                join: Some(join),
                root,
            })
        }
        async fn state(&self) -> TestResult<State> {
            match self.control.internal(Command::State).await? {
                Reply::State(s) => Ok(s),
                _ => Err("state reply".into()),
            }
        }
        fn assert_joined(&self) -> TestResult {
            use std::io::Write;
            // This owner helper never starts a listener, supervisor or network
            // worker. Assert its actual constructed owners after thread join;
            // its manually sized channel differs from the public runtime.
            let capacity = self.control.commands.max_capacity();
            let envelope_bound = checked_sum(&[product(capacity, 2)?, 1])?;
            let bounds = [
                self.control.config.limits.client_slots,
                self.control.config.pool,
                0,
                0,
                envelope_bound,
            ];
            for (gauge, bound) in self.control.metrics.gauges.iter().zip(bounds) {
                assert_eq!(gauge.current.load(Ordering::Acquire), 0);
                assert!(gauge.peak.load(Ordering::Acquire) <= bound);
            }
            assert_eq!(self.control.tasks.load(Ordering::Acquire), 0);
            assert_eq!(self.control.sockets.load(Ordering::Acquire), 0);
            assert_eq!(self.control.metrics.workers_spawned.load(Ordering::Acquire), 0);
            assert_eq!(self.control.metrics.workers_joined.load(Ordering::Acquire), 0);
            assert!(self.control.stopped.load(Ordering::Acquire));
            assert_eq!(
                self.control.clients.available_permits(),
                self.control.config.limits.client_slots
            );
            assert_eq!(
                self.control.bytes.available_permits(),
                self.control.config.pool
            );
            let Some(root) = &self.control.capture_root else {
                return Ok(());
            };
            let now_ms = u64::try_from(
                self.control
                    .metrics
                    .clock
                    .get()
                    .ok_or("owner clock")?
                    .elapsed()
                    .as_millis(),
            )?;
            let text = format!(
                "{{\"schema_version\":1,\"capture_revision\":2,\"scope\":\"direct blocking owner without network workers or supervisor\",\"pid\":{},\"stage\":\"direct-owner-joined\",\"now_ms\":{now_ms},\"clock_basis\":\"same exclusive owner process-local monotonic epoch\",\"source_sha\":\"{}\",\"local_id\":{},\"directory\":\"{}\",\"owner_joined\":true,\"supervisor_joined\":null,\"listener_closed\":null,\"network_workers_spawned\":0,\"network_workers_joined\":0,\"actual_mpsc_capacity\":{capacity},\"command_envelope_owner_bound\":{envelope_bound},\"resource_owners\":{},\"stopping\":true,\"available_client_permits\":{},\"available_transport_permits\":{}}}\n",
                std::process::id(),
                std::env::var("PL_PEER_RUNTIME_SOURCE_SHA")?,
                self.control.config.bootstrap.local().key().id,
                TestTrace::hex(&self.control.config.bootstrap.local().key().directory)?,
                self.control.metrics.json()?,
                self.control.clients.available_permits(),
                self.control.bytes.available_permits()
            );
            assert!(text.len() <= 8192);
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(root.join("lifecycle-direct-owner-joined.json"))?;
            file.write_all(text.as_bytes())?;
            file.sync_all()?;
            std::fs::File::open(root)?.sync_all()?;
            Ok(())
        }
        async fn finish(mut self) -> TestResult {
            self.control.stop();
            self.control.reliable(Command::Stop).await?;
            let joined = self
                .join
                .take()
                .ok_or("owner join already taken")?
                .join()
                .map_err(|_| "owner panic")?;
            joined?;
            self.assert_joined()?;
            std::fs::remove_dir_all(&self.root)?;
            Ok(())
        }
    }
    impl Drop for LocalOwner {
        fn drop(&mut self) {
            self.control.stop();
            if let Some(owner) = self.join.take() {
                let control = self.control.clone();
                let cleanup = std::thread::spawn(move || {
                    let (tx, _rx) = oneshot::channel();
                    let _ = control.commands.blocking_send(Envelope {
                        command: Command::Stop,
                        reply: tx,
                        permit: None,
                        transport: None,
                        started: Arc::new(AtomicBool::new(false)),
                        deadline: None,
                        delivery_gate: None,
                        _counter: None,
                    });
                    let _ = owner.join();
                });
                let _ = cleanup.join();
            }
        }
    }
    fn gate(
        command: Command,
    ) -> (
        Envelope,
        oneshot::Receiver<Delivered>,
        oneshot::Receiver<(Vec<replication::DynamicVoteRequest>, State)>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (reply, rx) = oneshot::channel();
        let (ready, entered) = oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let envelope = Envelope {
            command,
            reply,
            permit: None,
            transport: None,
            started: Arc::new(AtomicBool::new(false)),
            deadline: Some(Instant::now() + Duration::from_secs(2)),
            delivery_gate: Some(DeliveryGate {
                ready,
                release: held,
            }),
            _counter: None,
        };
        (envelope, rx, entered, release)
    }
    async fn grant(
        owner: &LocalOwner,
        request: replication::DynamicVoteRequest,
    ) -> TestResult<Message> {
        match owner
            .control
            .internal(Command::Inbound(
                request.context.leader,
                Message::Vote(request),
            ))
            .await?
        {
            Reply::Message(m @ Message::VoteReply(_)) => Ok(m),
            _ => Err("actual vote response".into()),
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_vote_vector_and_full_route_release_every_exact_correlation() -> TestResult {
        let source = LocalOwner::open(0, 5, 16).await?;
        tokio::time::sleep(Duration::from_millis(3)).await;
        let (envelope, reply, entered, release) = gate(Command::Tick);
        source
            .control
            .commands
            .send(envelope)
            .await
            .map_err(|_| "gate admission")?;
        let (requests, generated_state) = entered.await?;
        assert_eq!(requests.len(), 4);
        // The real candidacy/WAL has completed, then the whole reply receiver
        // disappears. Owner delivery cleanup must cover all four requests.
        drop(reply);
        release.send(())?;
        let after = source.state().await?;
        assert_eq!(
            after.election.persistent,
            generated_state.election.persistent
        );
        assert_eq!(after.wal_durable_ops, generated_state.wal_durable_ops);
        assert_eq!(
            after.election_durable_states,
            generated_state.election_durable_states
        );
        let mut peers = Vec::new();
        for id in 1..5 {
            peers.push(LocalOwner::open(id, 5, 16).await?);
        }
        for request in &requests {
            let response = grant(&peers[request.context.peer.id as usize - 1], *request).await?;
            assert!(
                source
                    .control
                    .internal(Command::Acknowledge(response))
                    .await
                    .is_err(),
                "abandoned vector grant cannot remain correlated"
            );
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
        let newer = match source.control.internal(Command::Tick).await? {
            Reply::Votes(jobs) => jobs,
            _ => return Err("campaign reply".into()),
        };
        assert_eq!(newer.len(), 4);
        let new_requests = newer
            .iter()
            .map(|j| match j {
                Job::Vote(q) => Ok(*q),
                _ => Err("vote job"),
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut routes = Vec::new();
        let mut receivers = Vec::new();
        // Old jobs are genuinely still queued. The fresh campaign's route
        // send now fails on capacity; it must cancel the fresh correlations.
        for q in &requests {
            let (tx, rx) = mpsc::channel(1);
            tx.try_send(Job::Vote(*q)).map_err(|_| "fill route")?;
            routes.push((q.context.peer, tx));
            receivers.push(rx);
        }
        dispatch_votes(&source.control, &routes, newer).await;
        for request in new_requests {
            let response = grant(&peers[request.context.peer.id as usize - 1], request).await?;
            assert!(
                source
                    .control
                    .internal(Command::Acknowledge(response))
                    .await
                    .is_err(),
                "failed route grant cannot remain correlated"
            );
        }
        drop(routes);
        drop(receivers);
        for peer in peers {
            peer.finish().await?;
        }
        source.finish().await
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reliable_stop_waits_for_saturated_owner_even_after_command_deadline() -> TestResult {
        let source = LocalOwner::open(0, 1, 1).await?;
        let (held, held_reply, entered, release) = gate(Command::State);
        source
            .control
            .commands
            .send(held)
            .await
            .map_err(|_| "held admission")?;
        entered.await?;
        let (queued, queued_reply) = oneshot::channel();
        source
            .control
            .commands
            .send(Envelope {
                command: Command::State,
                reply: queued,
                permit: None,
                transport: None,
                started: Arc::new(AtomicBool::new(false)),
                deadline: Some(Instant::now() + Duration::from_millis(1)),
                delivery_gate: None,
                _counter: None,
            })
            .await
            .map_err(|_| "fill owner queue")?;
        source.control.stop();
        let stop_control = source.control.clone();
        let mut stop = tokio::spawn(async move { stop_control.reliable(Command::Stop).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut stop)
                .await
                .is_err(),
            "stop retains admission beyond ordinary20ms budget"
        );
        release.send(())?;
        drop(held_reply);
        drop(queued_reply);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), &mut stop).await???,
            Reply::Done
        ));
        let mut source = source;
        source
            .join
            .take()
            .ok_or("owner join")?
            .join()
            .map_err(|_| "owner panic")??;
        source.assert_joined()?;
        std::fs::remove_dir_all(&source.root)?;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_outer_budget_cannot_admit_a_genuine_leader_write() -> TestResult {
        let source = LocalOwner::open(0, 5, 16).await?;
        tokio::time::sleep(Duration::from_millis(3)).await;
        let requests = match source.control.internal(Command::Tick).await? {
            Reply::Votes(jobs) => jobs,
            _ => return Err("campaign reply".into()),
        };
        let mut peers = Vec::new();
        for job in requests.into_iter().take(2) {
            let Job::Vote(request) = job else {
                return Err("vote job".into());
            };
            let peer = LocalOwner::open(request.context.peer.id, 5, 16).await?;
            source
                .control
                .internal(Command::Acknowledge(grant(&peer, request).await?))
                .await?;
            peers.push(peer);
        }
        source.control.internal(Command::Tick).await?;
        let before = source.state().await?;
        assert_eq!(before.election.role, Role::Leader);
        assert_eq!(before.active_term, Some(before.election.persistent.term));
        let handle = Handle {
            control: source.control.clone(),
        };
        let expired = Instant::now() - Duration::from_millis(1);
        assert!(matches!(
            handle
                .call_until(Command::Propose(b"expired outer budget".to_vec()), expired)
                .await,
            Err(Error::Deadline)
        ));
        let after = source.state().await?;
        assert_eq!(after.last_position, before.last_position);
        assert_eq!(after.wal_durable_ops, before.wal_durable_ops);
        assert_eq!(
            after.election_durable_states,
            before.election_durable_states
        );
        // A real proposal with a live budget confirms that the old assertion
        // was about expiry, not an inactive/invalid writer or malformed input.
        assert!(matches!(
            handle
                .call(Command::Propose(b"live budget".to_vec()))
                .await?,
            Reply::Position(_)
        ));
        assert!(source.state().await?.last_position.index > before.last_position.index);
        for peer in peers {
            peer.finish().await?;
        }
        source.finish().await
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn configured_route_does_not_authorize_unknown_configuration_leader_over_tcp(
    ) -> TestResult {
        let mut listeners = Vec::new();
        let mut addresses = Vec::new();
        for _ in 0..4 {
            let l = TcpListener::bind("127.0.0.1:0").await?;
            addresses.push(l.local_addr()?);
            listeners.push(l);
        }
        let voters = addresses
            .iter()
            .enumerate()
            .map(|(id, address)| {
                Ok(Voter::new(
                    Key::new(id as u32, [(id + 1) as u8; 16])?,
                    vec![Endpoint::new(
                        "CONTROLLER".into(),
                        address.ip().to_string(),
                        address.port(),
                    )?],
                    0,
                    1,
                )?)
            })
            .collect::<TestResult<Vec<_>>>()?;
        let genesis = Voters::new(0, LogPosition::default(), 1, voters[..3].to_vec())?;
        let bootstrap = Bootstrap::new(voters[0].clone(), genesis, "CONTROLLER".into())?;
        let peers = voters
            .iter()
            .enumerate()
            .skip(1)
            .map(|(id, v)| Ok(Peer::new(v.clone(), addresses[id])?))
            .collect::<TestResult<Vec<_>>>()?;
        // Long elections isolate the incoming request from unrelated timer
        // transitions; this is an actual local TCP/Node rejection test.
        let config = Config::new(
            protocol::Config::new(0, vec![0], "unknown-directory-test".into())?,
            bootstrap,
            peers,
            StorageLimits::standard()?,
            Limits {
                election_min_ms: 600000,
                election_max_ms: 600000,
                ..Limits::default()
            },
        )?;
        let group = config.group.clone();
        let bound = config.limits.frame_bytes;
        let root =
            std::env::temp_dir().join(format!("partitionline76-unknown-{}", std::process::id()));
        std::fs::create_dir(&root)?;
        let listener = listeners.remove(0);
        let address = listener.local_addr()?;
        let mut runtime = Runtime::start(
            listener,
            config,
            Paths::new(
                root.join("metadata.wal"),
                root.join("election.wal"),
                root.join("images"),
            )?,
        )?;
        runtime.wait_ready().await?;
        let handle = runtime.handle();
        let before = handle.state().await?;
        let mut socket = TcpStream::connect(address).await?;
        let deadline = Instant::now() + Duration::from_secs(2);
        let hello = Hello {
            source: voters[3].key(),
            target: voters[0].key(),
            group,
        };
        write_frame(
            &mut socket,
            &Frame {
                rpc: 0,
                message: Message::Hello(hello.clone()),
            },
            bound,
            deadline,
        )
        .await?;
        let response = read_frame(&mut socket, bound, 4096, deadline).await?;
        assert_eq!(response.message, Message::HelloAck(hello));
        let request = replication::DynamicVoteRequest {
            context: replication::Context {
                leader: voters[3].key(),
                peer: voters[0].key(),
                configuration_epoch: 0,
            },
            sequence: 1,
            request: super::super::election::VoteRequest {
                term: 2,
                candidate: 3,
                log: LogPosition::default(),
            },
        };
        write_frame(
            &mut socket,
            &Frame {
                rpc: 1,
                message: Message::Vote(request),
            },
            bound,
            deadline,
        )
        .await?;
        let response = read_frame(&mut socket, bound, 4096, deadline).await?;
        assert_eq!(response.rpc, 1);
        assert!(matches!(
            response.message,
            Message::Failure {
                request_kind: 10,
                code: 1
            }
        ));
        let after = handle.state().await?;
        assert_eq!(after.election.persistent, before.election.persistent);
        assert_eq!(after.wal_durable_ops, before.wal_durable_ops);
        assert_eq!(
            after.election_durable_states,
            before.election_durable_states
        );
        assert_eq!(after.last_position, before.last_position);
        assert_eq!(after.committed_end, before.committed_end);
        drop(socket);
        runtime.shutdown().await?;
        std::fs::remove_dir_all(root)?;
        drop(listeners);
        Ok(())
    }
    async fn owner_append_exchange(source: &LocalOwner, peer: &LocalOwner, key: Key) -> TestResult {
        let request = match source.control.reliable(Command::Prepare(key)).await? {
            Reply::Job(Some(Job::Append(q))) => q,
            _ => return Err("actual prepared Append required".into()),
        };
        let response = match peer
            .control
            .reliable(Command::Inbound(
                request.context.leader,
                Message::Append(request),
            ))
            .await?
        {
            Reply::Message(m @ Message::AppendReply(_)) => m,
            _ => return Err("actual follower Append reply".into()),
        };
        source
            .control
            .reliable(Command::Acknowledge(response))
            .await?;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exact_append_timeout_rejects_real_late_ack_and_cannot_cancel_newer_request(
    ) -> TestResult {
        use std::io::Read;
        fn bounded(path: &Path) -> TestResult<Vec<u8>> {
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(8 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 8 * 1024 * 1024 {
                return Err("actual control journal bound".into());
            }
            Ok(bytes)
        }
        let source = LocalOwner::open(0, 3, 16).await?;
        let peer = LocalOwner::open(1, 3, 16).await?;
        let second = LocalOwner::open(2, 3, 16).await?;
        tokio::time::sleep(Duration::from_millis(3)).await;
        let jobs = match source.control.reliable(Command::Tick).await? {
            Reply::Votes(v) => v,
            _ => return Err("actual campaign".into()),
        };
        assert_eq!(jobs.len(), 2);
        for job in jobs {
            let Job::Vote(q) = job else {
                return Err("actual vote job".into());
            };
            let voter = if q.context.peer.id == 1 {
                &peer
            } else {
                &second
            };
            let response = grant(voter, q).await?;
            source
                .control
                .reliable(Command::Acknowledge(response))
                .await?;
        }
        source.control.reliable(Command::Tick).await?;
        let key = peer.control.config.bootstrap.local().key();
        for _ in 0..2 {
            owner_append_exchange(&source, &peer, key).await?;
        }
        assert!(source.state().await?.committed_end > 0);
        source
            .control
            .reliable(Command::Propose(
                b"synchronized real response abandoned before consume".to_vec(),
            ))
            .await?;
        let request = match source.control.reliable(Command::Prepare(key)).await? {
            Reply::Job(Some(Job::Append(q))) => q,
            _ => return Err("actual prepared data request".into()),
        };
        let response = match peer
            .control
            .reliable(Command::Inbound(
                request.context.leader,
                Message::Append(request.clone()),
            ))
            .await?
        {
            Reply::Message(m @ Message::AppendReply(_)) => m,
            _ => return Err("actual synchronized data reply".into()),
        };
        assert!(matches!(response,Message::AppendReply(r) if r.response.success));
        let old_state = source.state().await?;
        let old_wal = bounded(&source.root.join("metadata.wal"))?;
        let old_election = bounded(&source.root.join("election.wal"))?;
        // A real correct response exists, but its exact correlation is released
        // before consumption. No denial or durable state is fabricated.
        source
            .control
            .reliable(Command::Timeout(key, request.request.sequence, false))
            .await?;
        assert!(source
            .control
            .reliable(Command::Acknowledge(response))
            .await
            .is_err());
        let after = source.state().await?;
        assert_eq!(after.election.persistent, old_state.election.persistent);
        assert_eq!(after.wal_durable_ops, old_state.wal_durable_ops);
        assert_eq!(
            after.election_durable_states,
            old_state.election_durable_states
        );
        assert_eq!(bounded(&source.root.join("metadata.wal"))?, old_wal);
        assert_eq!(bounded(&source.root.join("election.wal"))?, old_election);
        let newer = match source.control.reliable(Command::Prepare(key)).await? {
            Reply::Job(Some(Job::Append(q))) => q,
            _ => return Err("newer retry request".into()),
        };
        assert!(newer.request.sequence > request.request.sequence);
        let newer_response = match peer
            .control
            .reliable(Command::Inbound(
                newer.context.leader,
                Message::Append(newer.clone()),
            ))
            .await?
        {
            Reply::Message(m @ Message::AppendReply(_)) => m,
            _ => return Err("newer actual receipt".into()),
        };
        // Current API returns Done even when this old timeout is rejected by
        // Node. The capture outcome must say released=false; the newer ACK works.
        source
            .control
            .reliable(Command::Timeout(key, request.request.sequence, false))
            .await?;
        source
            .control
            .reliable(Command::Acknowledge(newer_response))
            .await?;
        assert_eq!(
            source.state().await?.committed_end,
            newer
                .request
                .entries
                .last()
                .map_or(newer.request.previous.index, |r| r.index)
        );
        second.finish().await?;
        peer.finish().await?;
        source.finish().await
    }
}
