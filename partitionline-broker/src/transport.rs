//! Bounded asynchronous length-prefixed transport, without Kafka API handlers.
//!
//! Each connection reads one signed big-endian length and request, awaits one
//! handler, and writes its optional response before reading the next request. Pipelined
//! responses therefore remain in request order without an application queue.
//! Connections and concurrent handler futures have separate explicit caps.
//! Read and write deadlines cover both prefix and body without resetting on
//! progress; handler deadlines include waiting for its concurrency permit.
//!
//! [`Transport::shutdown`] stops admission, drops sockets/handler futures and
//! joins every owned connection task. Cancelling that await leaves its join
//! handle available for retry. Dropping the transport requests the same cleanup;
//! call `shutdown` to await its completion. Handlers must yield to the executor
//! and must not block in polling or destructors. The transport cannot preempt
//! synchronous blocking work or manage tasks independently spawned by a handler.
//!
//! Limits bound transport-owned request buffers, response wire sizes and work
//! admission, not arbitrary handler allocation, OS socket buffers or process
//! RSS. Optional TLS/mTLS uses the same admission cap and joined shutdown,
//! without plaintext fallback. This module provides no Kafka API validation,
//! application authorization,
//! storage, broker readiness or production qualification.

use std::{future::Future, io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch, Semaphore},
    task::{JoinError, JoinHandle, JoinSet},
    time::{timeout_at, Instant},
};

/// Connection metadata supplied to handlers, without implying authorization.
#[derive(Debug, Clone)]
pub struct Peer {
    address: SocketAddr,
    #[cfg(feature = "tls")]
    tls: Option<crate::security::tls::VerifiedPeer>,
}

impl Peer {
    /// Remote TCP address; not an authenticated principal.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Verified TLS handshake metadata, absent for the explicit plaintext listener.
    #[cfg(feature = "tls")]
    pub fn tls(&self) -> Option<&crate::security::tls::VerifiedPeer> {
        self.tls.as_ref()
    }
}

enum Security {
    Plaintext,
    #[cfg(feature = "tls")]
    Tls(crate::security::tls::Acceptor),
}

enum Session {
    Plaintext,
    #[cfg(feature = "tls")]
    Tls {
        snapshot: Arc<crate::security::tls::Snapshot>,
        deadline: Instant,
    },
}

impl Security {
    fn admit(&self) -> Session {
        match self {
            Self::Plaintext => Session::Plaintext,
            #[cfg(feature = "tls")]
            Self::Tls(acceptor) => {
                let snapshot = acceptor.snapshot();
                Session::Tls {
                    deadline: Instant::now() + snapshot.limits.handshake_timeout,
                    snapshot,
                }
            }
        }
    }
}

/// Validated admission, frame and absolute operation deadline configuration.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    max_connections: usize,
    max_handlers: usize,
    max_request_bytes: usize,
    max_response_bytes: usize,
    read_timeout: Duration,
    handler_timeout: Duration,
    write_timeout: Duration,
}

impl Config {
    /// Configure positive bounds, rejecting invalid values before starting I/O.
    ///
    /// Connections are 1–65,536, handlers are 1–connections, request/response
    /// payloads are 1 byte–64 MiB, and deadlines are positive through 24 hours.
    /// Frame-byte limits exclude the four-byte length prefix. An empty payload
    /// is valid at this framing layer; future Kafka handlers validate headers.
    pub fn new(
        max_connections: usize,
        max_handlers: usize,
        max_request_bytes: usize,
        max_response_bytes: usize,
        read_timeout: Duration,
        handler_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<Self, Error> {
        for (field, valid) in [
            ("max_connections", (1..=65_536).contains(&max_connections)),
            (
                "max_handlers",
                (1..=max_connections).contains(&max_handlers),
            ),
            (
                "max_request_bytes",
                (1..=64 * 1024 * 1024).contains(&max_request_bytes),
            ),
            (
                "max_response_bytes",
                (1..=64 * 1024 * 1024).contains(&max_response_bytes),
            ),
            ("read_timeout", valid_timeout(read_timeout)),
            ("handler_timeout", valid_timeout(handler_timeout)),
            ("write_timeout", valid_timeout(write_timeout)),
        ] {
            if !valid {
                return Err(Error::InvalidConfig(field));
            }
        }
        Ok(Self {
            max_connections,
            max_handlers,
            max_request_bytes,
            max_response_bytes,
            read_timeout,
            handler_timeout,
            write_timeout,
        })
    }

    /// Maximum active owned connection tasks.
    pub fn max_connections(self) -> usize {
        self.max_connections
    }
    /// Maximum concurrent handler futures, independently of connected clients.
    pub fn max_handlers(self) -> usize {
        self.max_handlers
    }
    /// Maximum request payload bytes, excluding its four-byte prefix.
    pub fn max_request_bytes(self) -> usize {
        self.max_request_bytes
    }
    /// Maximum response payload bytes, excluding its four-byte prefix.
    pub fn max_response_bytes(self) -> usize {
        self.max_response_bytes
    }
    /// Absolute read deadline per frame, including prefix and payload.
    pub fn read_timeout(self) -> Duration {
        self.read_timeout
    }
    /// Absolute handler deadline, including concurrency-permit acquisition.
    pub fn handler_timeout(self) -> Duration {
        self.handler_timeout
    }
    /// Absolute response write deadline, including prefix and payload.
    pub fn write_timeout(self) -> Duration {
        self.write_timeout
    }
}

fn valid_timeout(duration: Duration) -> bool {
    !duration.is_zero() && duration <= Duration::from_secs(24 * 60 * 60)
}

impl Default for Config {
    /// 64 connections, 32 handlers, 8 MiB payloads, 10s/30s/10s deadlines.
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_handlers: 32,
            max_request_bytes: 8 * 1024 * 1024,
            max_response_bytes: 8 * 1024 * 1024,
            read_timeout: Duration::from_secs(10),
            handler_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(10),
        }
    }
}

/// Asynchronous request interface; response lengths are checked before writing.
///
/// `Some(payload)` sends one framed response. `None` sends no bytes and keeps
/// the connection usable, for protocols such as Kafka Produce with `acks=0`.
/// Implementations must bound their own allocations/work, yield to Tokio and
/// avoid blocking destructors. Errors close only the affected connection.
/// No background task is created for a handler call by this transport.
pub trait Handler: Send + Sync {
    /// Application-specific handler failure, never exposed as a success response.
    type Error: Send;
    /// Handle one owned bounded request; the future can await storage or other I/O.
    fn handle(
        &self,
        request: Vec<u8>,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;

    /// Handle with connection metadata; defaults to [`Handler::handle`].
    ///
    /// Handlers needing verified client identity can override this method.
    /// Authentication metadata alone makes no authorization decision.
    fn handle_with_peer(
        &self,
        _peer: &Peer,
        request: Vec<u8>,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send {
        self.handle(request)
    }
}

impl<F, Fut, E> Handler for F
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<Vec<u8>>, E>> + Send,
    E: Send,
{
    type Error = E;
    fn handle(&self, request: Vec<u8>) -> impl Future<Output = Result<Option<Vec<u8>>, E>> + Send {
        self(request)
    }
}

/// Configuration, listener or owned runner failure.
#[derive(Debug)]
pub enum Error {
    /// Named configuration field is outside its supported positive bound.
    InvalidConfig(&'static str),
    /// Listener bind or local-address lookup failed.
    Bind(io::Error),
    /// Listener failed; all accepted connection tasks are still cleaned up.
    Accept(io::Error),
    /// The runner task failed; a normal joined shutdown was not established.
    Join(JoinError),
    /// A previously failed runner has already been consumed.
    AlreadyStopped,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(field) => write!(f, "invalid transport configuration: {field}"),
            Self::Bind(error) => write!(f, "transport listener: {error}"),
            Self::Accept(error) => write!(f, "transport accept: {error}"),
            Self::Join(error) => write!(f, "transport runner: {error}"),
            Self::AlreadyStopped => f.write_str("transport runner already stopped"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind(error) | Self::Accept(error) => Some(error),
            Self::Join(error) => Some(error),
            _ => None,
        }
    }
}

/// Final saturated counters after every accepted connection task has joined.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// Accepted and spawned connection tasks.
    pub accepted_connections: u64,
    /// Accepted sockets immediately dropped because the connection cap was full.
    pub overload_rejections: u64,
    /// Completed/joined connection tasks, including failures and cancellation.
    pub joined_connections: u64,
    /// Maximum simultaneous owned connection tasks observed by admission.
    pub peak_connections: usize,
    /// Negative or over-budget request length prefixes.
    pub invalid_lengths: u64,
    /// Responses rejected before writing because their payload exceeded its cap.
    pub oversized_responses: u64,
    /// Absolute prefix/body read deadlines exceeded.
    pub read_deadlines: u64,
    /// Absolute permit/handler deadlines exceeded.
    pub handler_deadlines: u64,
    /// Absolute response prefix/body write deadlines exceeded.
    pub write_deadlines: u64,
    /// Handler calls returned an application error.
    pub handler_errors: u64,
    /// Peer EOFs, including truncated frames.
    pub peer_closes: u64,
    /// Other socket I/O or bounded reservation failures.
    pub io_errors: u64,
    /// Worker tasks terminated unexpectedly, such as through a handler panic.
    pub worker_failures: u64,
    /// Connections stopped by the transport shutdown signal.
    pub shutdown_connections: u64,
    /// TLS handshake, verification or peer bound failures (zero for plaintext).
    pub tls_handshake_errors: u64,
    /// Absolute TLS handshake deadlines exceeded (zero for plaintext).
    pub tls_handshake_deadlines: u64,
}

enum Exit {
    InvalidLength,
    OversizedResponse,
    ReadDeadline,
    HandlerDeadline,
    WriteDeadline,
    HandlerError,
    PeerClosed,
    Io,
    Shutdown,
    #[cfg(feature = "tls")]
    TlsError,
    #[cfg(feature = "tls")]
    TlsDeadline,
}

impl Report {
    fn joined(&mut self, result: Result<Exit, JoinError>) {
        self.joined_connections = self.joined_connections.saturating_add(1);
        let counter = match result {
            Ok(Exit::InvalidLength) => &mut self.invalid_lengths,
            Ok(Exit::OversizedResponse) => &mut self.oversized_responses,
            Ok(Exit::ReadDeadline) => &mut self.read_deadlines,
            Ok(Exit::HandlerDeadline) => &mut self.handler_deadlines,
            Ok(Exit::WriteDeadline) => &mut self.write_deadlines,
            Ok(Exit::HandlerError) => &mut self.handler_errors,
            Ok(Exit::PeerClosed) => &mut self.peer_closes,
            Ok(Exit::Io) => &mut self.io_errors,
            Ok(Exit::Shutdown) => &mut self.shutdown_connections,
            #[cfg(feature = "tls")]
            Ok(Exit::TlsError) => &mut self.tls_handshake_errors,
            #[cfg(feature = "tls")]
            Ok(Exit::TlsDeadline) => &mut self.tls_handshake_deadlines,
            Err(_) => &mut self.worker_failures,
        };
        *counter = counter.saturating_add(1);
    }
}

/// Owned listener runner and cancellation-safe joined shutdown handle.
pub struct Transport {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    runner: Option<JoinHandle<Result<Report, Error>>>,
    report: Option<Report>,
}

impl Transport {
    /// Bind a TCP listener and start bounded admission on the current Tokio runtime.
    pub async fn bind<H: Handler + 'static>(
        addr: SocketAddr,
        config: Config,
        handler: Arc<H>,
    ) -> Result<Self, Error> {
        Self::bind_with(addr, config, handler, Security::Plaintext).await
    }

    /// Bind a TLS-only listener with bounded, cancellable handshakes.
    ///
    /// Handshakes occupy the same connection cap as established sessions.
    /// Identity/trust is captured at admission; rotation affects later sockets.
    /// No plaintext fallback or protocol sniffing is performed.
    #[cfg(feature = "tls")]
    pub async fn bind_tls<H: Handler + 'static>(
        addr: SocketAddr,
        config: Config,
        handler: Arc<H>,
        acceptor: crate::security::tls::Acceptor,
    ) -> Result<Self, Error> {
        Self::bind_with(addr, config, handler, Security::Tls(acceptor)).await
    }

    async fn bind_with<H: Handler + 'static>(
        addr: SocketAddr,
        config: Config,
        handler: Arc<H>,
        security: Security,
    ) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await.map_err(Error::Bind)?;
        let addr = listener.local_addr().map_err(Error::Bind)?;
        let (shutdown, signal) = oneshot::channel();
        let runner = tokio::spawn(run(listener, config, handler, signal, security));
        Ok(Self {
            addr,
            shutdown: Some(shutdown),
            runner: Some(runner),
            report: None,
        })
    }

    /// Bound socket address, including the assigned port when binding port zero.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop admission, cancel every connection and await all owned tasks.
    ///
    /// Safe to cancel and retry this await. Repeated successful calls return the
    /// same final report. Cancellation drops async handler futures rather than
    /// waiting for them to finish; blocking handler code cannot be preempted.
    pub async fn shutdown(&mut self) -> Result<Report, Error> {
        if let Some(report) = self.report {
            return Ok(report);
        }
        if let Some(signal) = self.shutdown.take() {
            let _ = signal.send(());
        }
        let runner = self.runner.as_mut().ok_or(Error::AlreadyStopped)?;
        let result = runner.await.map_err(Error::Join);
        let _ = self.runner.take();
        let report = result??;
        self.report = Some(report);
        Ok(report)
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        if let Some(signal) = self.shutdown.take() {
            let _ = signal.send(());
        }
        // Detaching this runner allows it to complete its owned JoinSet drain.
        // Explicit shutdown is the API for observing that drain's completion.
    }
}

async fn run<H: Handler + 'static>(
    listener: TcpListener,
    config: Config,
    handler: Arc<H>,
    mut signal: oneshot::Receiver<()>,
    security: Security,
) -> Result<Report, Error> {
    let handlers = Arc::new(Semaphore::new(config.max_handlers));
    let (stop, _) = watch::channel(false);
    let mut workers = JoinSet::new();
    let mut report = Report::default();
    let failure = loop {
        tokio::select! {
            biased;
            _ = &mut signal => break None,
            Some(result) = workers.join_next(), if !workers.is_empty() => report.joined(result),
            accepted = listener.accept() => {
                let (socket, address) = match accepted {
                    Ok(socket) => socket,
                    Err(error) => break Some(Error::Accept(error)),
                };
                if workers.len() >= config.max_connections {
                    report.overload_rejections = report.overload_rejections.saturating_add(1);
                    drop(socket);
                    continue;
                }
                report.accepted_connections = report.accepted_connections.saturating_add(1);
                let peer = Peer { address, #[cfg(feature = "tls")] tls: None };
                workers.spawn(connection(socket, config, handler.clone(), handlers.clone(), stop.subscribe(), peer, security.admit()));
                report.peak_connections = report.peak_connections.max(workers.len());
            }
        }
    };
    drop(listener);
    let _ = stop.send(true);
    while let Some(result) = workers.join_next().await {
        report.joined(result);
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(report),
    }
}

async fn cancelled(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow() {
        return;
    }
    let _ = stop.changed().await;
}

async fn connection<H: Handler>(
    socket: TcpStream,
    config: Config,
    handler: Arc<H>,
    handlers: Arc<Semaphore>,
    mut stop: watch::Receiver<bool>,
    peer: Peer,
    session: Session,
) -> Exit {
    tokio::select! {
        biased;
        _ = cancelled(&mut stop) => Exit::Shutdown,
        result = establish(socket, config, handler, handlers, peer, session) => result,
    }
}

async fn establish<H: Handler>(
    socket: TcpStream,
    config: Config,
    handler: Arc<H>,
    handlers: Arc<Semaphore>,
    peer: Peer,
    session: Session,
) -> Exit {
    match session {
        Session::Plaintext => exchange(socket, config, handler, handlers, peer).await,
        #[cfg(feature = "tls")]
        Session::Tls { snapshot, deadline } => {
            let stream = match timeout_at(
                deadline,
                tokio_rustls::TlsAcceptor::from(snapshot.server.clone()).accept(socket),
            )
            .await
            {
                Ok(Ok(stream)) => stream,
                Ok(Err(_)) => return Exit::TlsError,
                Err(_) => return Exit::TlsDeadline,
            };
            let mut peer = peer;
            peer.tls = match crate::security::tls::VerifiedPeer::new(
                &snapshot,
                stream.get_ref().1.peer_certificates(),
            ) {
                Ok(verified) => Some(verified),
                Err(_) => return Exit::TlsError,
            };
            exchange(stream, config, handler, handlers, peer).await
        }
    }
}

fn read_error(error: io::Error) -> Exit {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        Exit::PeerClosed
    } else {
        Exit::Io
    }
}

async fn read_frame<S: AsyncRead + Unpin>(socket: &mut S, config: Config) -> Result<Vec<u8>, Exit> {
    let deadline = Instant::now() + config.read_timeout;
    timeout_at(deadline, async {
        let mut prefix = [0; 4];
        let _ = socket.read_exact(&mut prefix).await.map_err(read_error)?;
        let length =
            usize::try_from(i32::from_be_bytes(prefix)).map_err(|_| Exit::InvalidLength)?;
        if length > config.max_request_bytes {
            return Err(Exit::InvalidLength);
        }
        // No allocation based on the peer's length occurs before this check.
        let mut payload = Vec::new();
        payload.try_reserve_exact(length).map_err(|_| Exit::Io)?;
        payload.resize(length, 0);
        let _ = socket.read_exact(&mut payload).await.map_err(read_error)?;
        Ok(payload)
    })
    .await
    .map_err(|_| Exit::ReadDeadline)?
}

async fn exchange<H: Handler, S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: S,
    config: Config,
    handler: Arc<H>,
    handlers: Arc<Semaphore>,
    peer: Peer,
) -> Exit {
    loop {
        let request = match read_frame(&mut socket, config).await {
            Ok(request) => request,
            Err(exit) => return exit,
        };
        let deadline = Instant::now() + config.handler_timeout;
        let response = match timeout_at(deadline, async {
            let permit = handlers.acquire().await.map_err(|_| Exit::Shutdown)?;
            let response = handler
                .handle_with_peer(&peer, request)
                .await
                .map_err(|_| Exit::HandlerError);
            drop(permit);
            response
        })
        .await
        {
            Ok(Ok(Some(response))) => response,
            Ok(Ok(None)) => continue,
            Ok(Err(exit)) => return exit,
            Err(_) => return Exit::HandlerDeadline,
        };
        if response.len() > config.max_response_bytes {
            return Exit::OversizedResponse;
        }
        let length = match i32::try_from(response.len()) {
            Ok(length) => length,
            Err(_) => return Exit::OversizedResponse,
        };
        let deadline = Instant::now() + config.write_timeout;
        match timeout_at(deadline, async {
            socket.write_all(&length.to_be_bytes()).await?;
            socket.write_all(&response).await
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Exit::Io,
            Err(_) => return Exit::WriteDeadline,
        }
    }
}
