//! TCP and TLS broker connections.

use std::collections::VecDeque;
use std::fmt;
use std::future::{poll_fn, Future};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::sync::Once;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::{timeout, timeout_at, Instant as TokioInstant};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::error::{Error, Result};
use crate::protocol::header::{
    decode_response_header, encode_request_header_fields, RequestHeader, ResponseHeader,
};

/// Max Kafka response frame (100 MiB). Larger is treated as a protocol error.
pub const MAX_FRAME: i32 = 100 * 1024 * 1024;

/// Java `SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID`.
pub const MAX_RESERVED_CORRELATION_ID: i32 = i32::MAX;

/// Java `SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID`.
pub const MIN_RESERVED_CORRELATION_ID: i32 = i32::MAX - 7;

/// Java `SaslClientAuthenticator.isReserved`.
#[must_use]
pub const fn is_reserved_correlation_id(correlation_id: i32) -> bool {
    correlation_id >= MIN_RESERVED_CORRELATION_ID
}

/// Java `NetworkClient.nextCorrelationId`.
///
/// Skips [`MIN_RESERVED_CORRELATION_ID`] through
/// [`MAX_RESERVED_CORRELATION_ID`] (SASL reauth). Hitting that range jumps
/// to `MAX_RESERVED_CORRELATION_ID + 1`, which wraps to `i32::MIN`.
#[must_use]
pub fn next_correlation_id(correlation: &mut i32) -> i32 {
    if is_reserved_correlation_id(*correlation) {
        *correlation = MAX_RESERVED_CORRELATION_ID.wrapping_add(1);
    }
    let issued = *correlation;
    *correlation = correlation.wrapping_add(1);
    issued
}

/// Java `SaslClientAuthenticator.nextCorrelationId`.
///
/// Issues ids in [`MIN_RESERVED_CORRELATION_ID`] through
/// [`MAX_RESERVED_CORRELATION_ID`]. A field outside that range (including
/// the start value `0` and wrap to `i32::MIN` after `i32::MAX`) jumps to
/// [`MIN_RESERVED_CORRELATION_ID`].
#[must_use]
pub fn next_sasl_correlation_id(correlation: &mut i32) -> i32 {
    if !is_reserved_correlation_id(*correlation) {
        *correlation = MIN_RESERVED_CORRELATION_ID;
    }
    let issued = *correlation;
    *correlation = correlation.wrapping_add(1);
    issued
}

/// Java `NetworkClient.parseResponse` correlation-id check.
///
/// [`RequestHeader::check_correlation`] is Java
/// `AbstractResponse.parseResponse` (`CorrelationIdMismatchException`).
/// When the request id is reserved for SASL and the response id is not,
/// Java wraps that as `SchemaException`: the body belongs to some other
/// in-flight Kafka request.
pub fn check_parse_response_correlation(
    request: &RequestHeader,
    response: &ResponseHeader,
) -> Result<()> {
    if request.correlation_id() == response.correlation_id() {
        return Ok(());
    }
    if is_reserved_correlation_id(request.correlation_id())
        && !is_reserved_correlation_id(response.correlation_id())
    {
        return Err(Error::protocol(format!(
            "The response is unrelated to Sasl request since its correlation id is {} and the reserved range for Sasl request is [ {},{}]",
            response.correlation_id(),
            MIN_RESERVED_CORRELATION_ID,
            MAX_RESERVED_CORRELATION_ID
        )));
    }
    request.check_correlation(response)
}

/// An absolute deadline for network operations.
///
/// Shared remaining-time contract for request write, header/body read,
/// and partial-frame progress.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    instant: TokioInstant,
}

impl Deadline {
    /// Create a deadline expiring after `timeout` from now.
    #[must_use]
    pub fn from_timeout(timeout: Duration) -> Self {
        Self {
            instant: TokioInstant::now() + timeout,
        }
    }

    /// Create a deadline at an explicit [`TokioInstant`].
    #[must_use]
    pub fn at(instant: TokioInstant) -> Self {
        Self { instant }
    }

    /// Create a deadline from a standard [`std::time::Instant`].
    #[must_use]
    pub fn from_std(instant: Instant) -> Self {
        Self {
            instant: TokioInstant::from_std(instant),
        }
    }

    /// The target [`TokioInstant`].
    #[must_use]
    pub fn target(&self) -> TokioInstant {
        self.instant
    }

    /// Check if this deadline has passed.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        TokioInstant::now() >= self.instant
    }

    /// Check if expired, returning [`Error::Timeout`] if so.
    pub fn check_expired(&self) -> Result<()> {
        if self.is_expired() {
            Err(Error::Timeout)
        } else {
            Ok(())
        }
    }

    /// Remaining duration until the deadline, or [`Error::Timeout`] if expired.
    pub fn remaining(&self) -> Result<Duration> {
        let now = TokioInstant::now();
        if now >= self.instant {
            Err(Error::Timeout)
        } else {
            Ok(self.instant - now)
        }
    }

    /// Run `future` bounded by this deadline. Returns [`Error::Timeout`] if
    /// the deadline is reached before `future` resolves.
    pub async fn run<F, T>(&self, future: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        self.check_expired()?;
        match timeout_at(self.instant, future).await {
            Ok(res) => res,
            Err(_) => Err(Error::Timeout),
        }
    }

    /// Run an I/O future returning `std::io::Result<T>` bounded by this deadline.
    pub async fn run_io<F, T>(&self, future: F) -> Result<T>
    where
        F: std::future::Future<Output = std::io::Result<T>>,
    {
        self.check_expired()?;
        match timeout_at(self.instant, future).await {
            Ok(Ok(val)) => Ok(val),
            Ok(Err(e)) => Err(Error::from(e)),
            Err(_) => Err(Error::Timeout),
        }
    }
}

struct CloseOnDrop {
    closed: Arc<AtomicBool>,
    completed: bool,
}

impl CloseOnDrop {
    fn new(closed: Arc<AtomicBool>) -> Self {
        Self {
            closed,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if !self.completed {
            self.closed.store(true, Ordering::SeqCst);
        }
    }
}

/// Grow `read_buf` once to the known frame size so a 16MiB Fetch does not
/// memcpy through the 8KiB → 16KiB → … doubling path.
pub(crate) fn reserve_frame(buf: &mut BytesMut, total: usize) {
    if let Some(need) = total.checked_sub(buf.len()) {
        if need > 0 {
            buf.reserve(need);
        }
    }
}

/// rustls client settings. No OpenSSL.
///
/// [`Debug`] never prints PEM bytes; the private key is always `<redacted>`
/// (KL-06).
#[derive(Clone, Default)]
pub struct TlsConfig {
    /// PEM CA bundle. If `None`, Mozilla webpki-roots are used.
    pub ca_pem: Option<Vec<u8>>,
    /// Client certificate PEM for mTLS.
    pub client_cert_pem: Option<Vec<u8>>,
    /// Client private key PEM for mTLS.
    pub client_key_pem: Option<Vec<u8>>,
    /// SNI and certificate hostname. Defaults to the bootstrap host (no port).
    pub server_name: Option<String>,
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig")
            .field(
                "ca_pem",
                &self
                    .ca_pem
                    .as_ref()
                    .map(|p| format!("<pem {} bytes>", p.len())),
            )
            .field(
                "client_cert_pem",
                &self
                    .client_cert_pem
                    .as_ref()
                    .map(|p| format!("<pem {} bytes>", p.len())),
            )
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("server_name", &self.server_name)
            .finish()
    }
}

impl TlsConfig {
    /// Trust this CA PEM bundle instead of Mozilla roots.
    #[must_use]
    pub fn ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(pem.into());
        self
    }

    /// SNI / certificate hostname, for example `localhost`.
    #[must_use]
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Client certificate and key for mTLS.
    #[must_use]
    pub fn client_identity(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_cert_pem = Some(cert_pem.into());
        self.client_key_pem = Some(key_pem.into());
        self
    }
}

fn ensure_crypto() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        drop(rustls::crypto::ring::default_provider().install_default());
    });
}

fn is_bootstrap_scheme_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'%' | b'.' | b'_')
}

fn is_bootstrap_host_char(b: u8) -> bool {
    is_bootstrap_scheme_char(b) || b == b':'
}

fn rest_after_optional_scheme(address: &str) -> &str {
    match address.find("://") {
        Some(i) => {
            let scheme = address.get(..i).unwrap_or("");
            if scheme.bytes().all(is_bootstrap_scheme_char) {
                match i.checked_add(3).and_then(|end| address.get(end..)) {
                    Some(rest) => rest,
                    None => address,
                }
            } else {
                address
            }
        }
        None => address,
    }
}

fn host_from_prefix(prefix: &str) -> Option<&str> {
    let mut host = prefix;
    if let Some(inner) = host.strip_prefix('[') {
        host = inner;
    }
    if let Some(inner) = host.strip_suffix(']') {
        host = inner;
    }
    host.bytes().all(is_bootstrap_host_char).then_some(host)
}

fn parse_host_port(address: &str) -> Option<(&str, i32)> {
    let rest = rest_after_optional_scheme(address);
    let (prefix, port_str) = rest.rsplit_once(':')?;
    if port_str.is_empty() || !port_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let port = port_str.parse().ok()?;
    let host = host_from_prefix(prefix)?;
    Some((host, port))
}

/// Java `Utils.getHost` (`None` is Java `null`).
///
/// Parses `host:port`, bracketed IPv6 plus port, and an optional scheme
/// (`PLAINTEXT://`). Invalid characters are `None`.
#[must_use]
pub fn get_host(address: &str) -> Option<&str> {
    parse_host_port(address).map(|(h, _)| h)
}

/// Java `Utils.getPort` (`None` is Java `null`).
///
/// A non-digit port (including a leading minus) is `None`. A digit string
/// that does not fit in `i32` is also `None` (Java `Integer.parseInt`
/// throws).
#[must_use]
pub fn get_port(address: &str) -> Option<i32> {
    parse_host_port(address).map(|(_, p)| p)
}

/// Java `Utils.validHostPattern`.
#[must_use]
pub fn valid_host_pattern(address: &str) -> bool {
    address.bytes().all(is_bootstrap_host_char)
}

/// Java `Utils.formatAddress` (IPv6 host is wrapped in brackets).
#[must_use]
pub fn format_address(host: &str, port: i32) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Java `ClientUtils.parseAndValidateAddresses` without DNS lookup.
///
/// Empty input is `no bootstrap servers`. Blank entries are skipped. A
/// non-empty url that [`get_host`] / [`get_port`] cannot parse is
/// `Invalid url in bootstrap.servers: {url}`. A port that does not fit
/// `u16` is `Invalid port in bootstrap.servers: {url}`. If nothing remains
/// after skipping blanks, `No resolvable bootstrap urls given in
/// bootstrap.servers`. Does not resolve hosts (`Unknown host in
/// bootstrap.servers`).
pub fn parse_and_validate_addresses(urls: &[String]) -> Result<Vec<String>> {
    if urls.is_empty() {
        return Err(Error::protocol("no bootstrap servers"));
    }
    let mut addresses = Vec::new();
    for url in urls {
        if url.is_empty() {
            continue;
        }
        addresses.push(parse_bootstrap_url(url)?);
    }
    if addresses.is_empty() {
        return Err(Error::protocol(
            "No resolvable bootstrap urls given in bootstrap.servers",
        ));
    }
    Ok(addresses)
}

fn parse_bootstrap_url(url: &str) -> Result<String> {
    let (host, port) = match (get_host(url), get_port(url)) {
        (Some(host), Some(port)) => (host, port),
        _ => {
            return Err(Error::protocol(format!(
                "Invalid url in bootstrap.servers: {url}"
            )));
        }
    };
    if u16::try_from(port).is_err() {
        return Err(Error::protocol(format!(
            "Invalid port in bootstrap.servers: {url}"
        )));
    }
    Ok(format_address(host, port))
}

fn host_of(addr: &str) -> &str {
    if let Some(host) = get_host(addr) {
        return host;
    }
    if let Some(rest) = addr.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(addr);
    }
    addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr)
}

fn certs_from_pem(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::protocol(format!("tls cert pem: {e}")))
}

fn key_from_pem(pem: &[u8]) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|e| Error::protocol(format!("tls key pem: {e}")))
}

fn root_store(tls: &TlsConfig) -> Result<RootCertStore> {
    let mut root = RootCertStore::empty();
    if let Some(pem) = &tls.ca_pem {
        for c in certs_from_pem(pem)? {
            root.add(c)
                .map_err(|e| Error::protocol(format!("tls ca: {e}")))?;
        }
    } else {
        root.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    if root.is_empty() {
        return Err(Error::protocol("tls: empty CA store"));
    }
    Ok(root)
}

pub(crate) async fn wrap_tls(
    tcp: TcpStream,
    addr: &str,
    tls: &TlsConfig,
) -> Result<TlsStream<TcpStream>> {
    ensure_crypto();
    let root = root_store(tls)?;
    let builder = ClientConfig::builder().with_root_certificates(root);
    let config = match (&tls.client_cert_pem, &tls.client_key_pem) {
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(certs_from_pem(cert)?, key_from_pem(key)?)
            .map_err(|e| Error::protocol(format!("tls client cert: {e}")))?,
        _ => builder.with_no_client_auth(),
    };
    let connector = TlsConnector::from(Arc::new(config));
    let name = tls
        .server_name
        .clone()
        .unwrap_or_else(|| host_of(addr).to_string());
    let server_name =
        ServerName::try_from(name).map_err(|e| Error::protocol(format!("tls server name: {e}")))?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(Error::from)
}

async fn write_all_pump(
    stream: &mut ConnIo,
    read_buf: &mut BytesMut,
    bytes: &[u8],
) -> io::Result<()> {
    let mut pos = 0usize;
    poll_fn(|cx| loop {
        let remaining = match bytes.get(pos..) {
            Some(r) => r,
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "write pos past end",
                )));
            }
        };
        match Pin::new(&mut *stream).poll_write(cx, remaining) {
            Poll::Ready(Ok(0)) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "broker write returned 0",
                )));
            }
            Poll::Ready(Ok(n)) => {
                pos += n;
                if pos >= bytes.len() {
                    return Poll::Ready(Ok(()));
                }
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => {
                let mut tmp = [0u8; 8192];
                let mut rb = ReadBuf::new(&mut tmp);
                match Pin::new(&mut *stream).poll_read(cx, &mut rb) {
                    Poll::Ready(Ok(())) => {
                        let filled = rb.filled();
                        if filled.is_empty() {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "broker closed connection",
                            )));
                        }
                        read_buf.extend_from_slice(filled);
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    })
    .await
}

enum ConnIo {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for ConnIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnIo::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            ConnIo::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ConnIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ConnIo::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            ConnIo::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnIo::Tcp(s) => Pin::new(s).poll_flush(cx),
            ConnIo::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnIo::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            ConnIo::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// One TCP or TLS connection to a Kafka broker.
pub struct BrokerConn {
    stream: ConnIo,
    read_buf: BytesMut,
    write_buf: BytesMut,
    next_correlation: i32,
    sasl_correlation: i32,
    client_id: String,
    addr: String,
    last_io: Instant,
    closed: Arc<AtomicBool>,
    /// Admin-only. Producer and consumer sockets leave this `None` so
    /// [`Self::send`] plus [`Self::read_response`] is not double-counted.
    stats: Option<Arc<crate::metrics::AdminTracker>>,
    /// Produce version negotiated on data sockets (`-1` unset).
    /// This crate picks 3–12 from ApiVersions.
    pub(crate) produce_version: i16,
    /// OffsetCommit version negotiated on coordinator sockets (`0` unset).
    /// Classic consumer groups pick 2–9 from ApiVersions. Kafka 4.0
    /// removed v0–v1, so `0` is not a spoken version.
    pub(crate) offset_commit_version: i16,
    /// OffsetFetch version negotiated on coordinator sockets (`0` unset).
    /// Classic consumer groups pick 5–9 from ApiVersions.
    pub(crate) offset_fetch_version: i16,
    /// Heartbeat version negotiated on coordinator sockets (`-1` unset).
    /// Classic consumer groups pick 0–4 from ApiVersions. `0` is a spoken
    /// version, so it cannot mean unset.
    pub(crate) heartbeat_version: i16,
    /// SyncGroup version negotiated on coordinator sockets (`-1` unset).
    /// Classic consumer groups pick 0–5 from ApiVersions. `0` is a spoken
    /// version, so it cannot mean unset.
    pub(crate) sync_group_version: i16,
    /// JoinGroup version negotiated on coordinator sockets (`0` unset).
    /// Classic consumer groups pick 2–9 from ApiVersions. Kafka 4.0
    /// removed v0–v1, so `0` is not a spoken version.
    pub(crate) join_group_version: i16,
    /// LeaveGroup version negotiated on coordinator sockets (`-1` unset).
    /// Classic consumer groups pick 0–5 from ApiVersions. `0` is a spoken
    /// version, so it cannot mean unset.
    pub(crate) leave_group_version: i16,
    /// ConsumerGroupHeartbeat version negotiated on coordinator sockets
    /// (`-1` unset). KIP-848 groups pick 0–1 from ApiVersions. `0` is a
    /// spoken version, so it cannot mean unset.
    pub(crate) consumer_group_heartbeat_version: i16,
    /// ShareGroupHeartbeat version negotiated on coordinator sockets
    /// (`-1` unset). KIP-932 groups pick 0–1 from ApiVersions. `0` is a
    /// spoken version, so it cannot mean unset.
    pub(crate) share_group_heartbeat_version: i16,
    /// SaslHandshake version negotiated after ApiVersions (`-1` unset).
    /// This crate picks 0–1. `0` is a spoken version, so it cannot mean
    /// unset.
    pub(crate) sasl_handshake_version: i16,
    /// SaslAuthenticate version negotiated after ApiVersions (`-1` unset).
    /// This crate picks 0–2. `0` is a spoken version, so it cannot mean
    /// unset.
    pub(crate) sasl_authenticate_version: i16,
    /// Broker session lifetime in milliseconds, if negotiated via SaslAuthenticate v1+ (KIP-368).
    pub(crate) session_lifetime_ms: Option<i64>,
    /// Instant when SASL authentication completed.
    pub(crate) authenticated_at: Option<Instant>,
    /// Absolute instant when the broker SASL session expires (`authenticated_at + session_lifetime_ms`).
    pub(crate) sasl_session_expires_at: Option<Instant>,
    /// Instant at which proactive reauthentication should start (85% of session lifetime).
    pub(crate) reauth_at: Option<Instant>,
    /// In-flight application request count on this connection.
    pub(crate) in_flight: usize,
}

impl BrokerConn {
    /// Connect without TLS.
    pub async fn connect(addr: &str, client_id: &str, connect_timeout: Duration) -> Result<Self> {
        Self::connect_tls(addr, client_id, connect_timeout, None).await
    }

    /// Try each bootstrap address until one connects.
    pub async fn connect_tls_any(
        addrs: &[String],
        client_id: &str,
        connect_timeout: Duration,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let addrs = parse_and_validate_addresses(addrs)?;
        let mut last = Error::protocol("all bootstrap servers failed");
        for addr in &addrs {
            match Self::connect_tls(addr, client_id, connect_timeout, tls).await {
                Ok(conn) => return Ok(conn),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    /// Connect, optionally with rustls.
    pub async fn connect_tls(
        addr: &str,
        client_id: &str,
        connect_timeout: Duration,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let tcp = timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout)??;
        tcp.set_nodelay(true)?;
        let stream = if let Some(tls) = tls {
            ConnIo::Tls(Box::new(wrap_tls(tcp, addr, tls).await?))
        } else {
            ConnIo::Tcp(tcp)
        };
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(8 * 1024),
            write_buf: BytesMut::with_capacity(16 * 1024),
            next_correlation: 1,
            sasl_correlation: 0,
            client_id: client_id.to_string(),
            addr: addr.to_string(),
            last_io: Instant::now(),
            closed: Arc::new(AtomicBool::new(false)),
            stats: None,
            produce_version: -1,
            offset_commit_version: 0,
            offset_fetch_version: 0,
            heartbeat_version: -1,
            sync_group_version: -1,
            join_group_version: 0,
            leave_group_version: -1,
            consumer_group_heartbeat_version: -1,
            share_group_heartbeat_version: -1,
            sasl_handshake_version: -1,
            sasl_authenticate_version: -1,
            session_lifetime_ms: None,
            authenticated_at: None,
            sasl_session_expires_at: None,
            reauth_at: None,
            in_flight: 0,
        })
    }

    /// Kafka `client.id` on this connection.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Negotiated Produce version on this broker connection (`-1` unset).
    #[must_use]
    pub fn produce_version(&self) -> i16 {
        self.produce_version
    }

    /// Set negotiated Produce version on this broker connection.
    pub fn set_produce_version(&mut self, version: i16) {
        self.produce_version = version;
    }

    /// Next request correlation id.
    ///
    /// Java `NetworkClient.nextCorrelationId` ([`next_correlation_id`]).
    pub fn next_correlation(&mut self) -> i32 {
        next_correlation_id(&mut self.next_correlation)
    }

    /// Next SASL request correlation id.
    ///
    /// Java `SaslClientAuthenticator.nextCorrelationId`
    /// ([`next_sasl_correlation_id`]). Handshake and authenticate use this
    /// reserved range so a delayed SASL response cannot be parsed as a
    /// Kafka response.
    pub fn next_sasl_correlation(&mut self) -> i32 {
        next_sasl_correlation_id(&mut self.sasl_correlation)
    }

    /// Whether this connection is closed, failed, or desynchronized.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Mark this connection as permanently closed/failed.
    pub fn close(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.in_flight = 0;
    }

    /// Broker session lifetime in milliseconds, if negotiated via SaslAuthenticate v1+ (KIP-368).
    #[must_use]
    pub fn session_lifetime_ms(&self) -> Option<i64> {
        self.session_lifetime_ms
    }

    /// Instant when SASL authentication completed.
    #[must_use]
    pub fn authenticated_at(&self) -> Option<Instant> {
        self.authenticated_at
    }

    /// Absolute instant when the broker SASL session expires.
    #[must_use]
    pub fn session_expiry(&self) -> Option<Instant> {
        self.sasl_session_expires_at
    }

    /// Instant at which proactive reauthentication should start (85% of session lifetime).
    #[must_use]
    pub fn reauth_at(&self) -> Option<Instant> {
        self.reauth_at
    }

    /// Check if the session has expired at the given instant.
    #[must_use]
    pub fn is_session_expired_at(&self, now: Instant) -> bool {
        self.sasl_session_expires_at.is_some_and(|exp| now >= exp)
    }

    /// Check if the session has expired now.
    #[must_use]
    pub fn is_session_expired(&self) -> bool {
        self.is_session_expired_at(Instant::now())
    }

    /// Check if reauthentication is needed at the given instant (85% threshold reached).
    #[must_use]
    pub fn needs_reauth_at(&self, now: Instant) -> bool {
        self.reauth_at.is_some_and(|r| now >= r)
    }

    /// Check if reauthentication is needed now.
    #[must_use]
    pub fn needs_reauth(&self) -> bool {
        self.needs_reauth_at(Instant::now())
    }

    /// Number of in-flight application requests on this connection.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Whether this connection can safely execute SASL reauthentication without interleaving with in-flight requests.
    #[must_use]
    pub fn can_reauthenticate(&self) -> bool {
        self.in_flight == 0 && !self.is_closed()
    }

    /// Record broker `session_lifetime_ms` at a specific instant.
    pub fn record_sasl_session_lifetime_at(&mut self, session_lifetime_ms: i64, now: Instant) {
        if session_lifetime_ms > 0 {
            let Ok(lifetime_ms) = u64::try_from(session_lifetime_ms) else {
                self.clear_sasl_session();
                return;
            };
            let lifetime = Duration::from_millis(lifetime_ms);
            let reauth_millis = lifetime_ms.saturating_mul(85) / 100;
            let reauth_duration = Duration::from_millis(reauth_millis);
            self.session_lifetime_ms = Some(session_lifetime_ms);
            self.authenticated_at = Some(now);
            self.sasl_session_expires_at = Some(now + lifetime);
            self.reauth_at = Some(now + reauth_duration);
        } else {
            self.clear_sasl_session();
        }
    }

    /// Record broker `session_lifetime_ms` from a successful SaslAuthenticate.
    pub fn record_sasl_session_lifetime(&mut self, session_lifetime_ms: i64) {
        self.record_sasl_session_lifetime_at(session_lifetime_ms, Instant::now());
    }

    /// Clear SASL session lifetime state.
    pub fn clear_sasl_session(&mut self) {
        self.session_lifetime_ms = None;
        self.authenticated_at = None;
        self.sasl_session_expires_at = None;
        self.reauth_at = None;
    }

    /// Convert this connection into a pipelined connection manager.
    #[must_use]
    pub fn into_pipeline(self) -> BrokerPipeline {
        BrokerPipeline::new(self)
    }

    /// Convert this connection into a pipelined connection manager with custom max in-flight.
    #[must_use]
    pub fn into_pipeline_with_max_in_flight(self, max_in_flight: usize) -> BrokerPipeline {
        BrokerPipeline::with_max_in_flight(self, max_in_flight)
    }

    /// Write `bytes` bounded by `deadline` or fail with [`Error::Timeout`].
    ///
    /// If write fails, times out, or is cancelled, marks this connection
    /// permanently closed so no partial frame can corrupt subsequent use.
    pub async fn write_all_deadline(&mut self, bytes: &[u8], deadline: Deadline) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let mut guard = CloseOnDrop::new(self.closed.clone());
        deadline
            .run_io(write_all_pump(&mut self.stream, &mut self.read_buf, bytes))
            .await?;
        guard.complete();
        self.touch();
        Ok(())
    }

    /// Write `bytes` or fail with [`Error::Timeout`].
    pub async fn write_all_timeout(
        &mut self,
        bytes: &[u8],
        request_timeout: Duration,
    ) -> Result<()> {
        self.write_all_deadline(bytes, Deadline::from_timeout(request_timeout))
            .await
    }

    async fn read_frame_deadline_internal(
        &mut self,
        deadline: Deadline,
        close_on_drop: bool,
    ) -> Result<Bytes> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let mut guard = if close_on_drop {
            Some(CloseOnDrop::new(self.closed.clone()))
        } else {
            None
        };
        loop {
            if self.read_buf.len() >= 4 {
                let prefix = self
                    .read_buf
                    .get(..4)
                    .ok_or_else(|| Error::protocol("short frame prefix"))?;
                let size = i32::from_be_bytes(
                    prefix
                        .try_into()
                        .map_err(|_| Error::protocol("short frame prefix"))?,
                );
                if !(0..=MAX_FRAME).contains(&size) {
                    if let Some(ref mut g) = guard {
                        g.complete();
                    }
                    self.close();
                    return Err(Error::protocol(format!("invalid frame size {size}")));
                }
                let total = 4 + crate::protocol::buf::usize_from_i32(size)?;
                if self.read_buf.len() >= total {
                    let mut frame = self.read_buf.split_to(total);
                    drop(frame.split_to(4));
                    if let Some(ref mut g) = guard {
                        g.complete();
                    }
                    return Ok(frame.freeze());
                }
                reserve_frame(&mut self.read_buf, total);
            }
            let n = match deadline
                .run_io(self.stream.read_buf(&mut self.read_buf))
                .await
            {
                Ok(n) => n,
                Err(e) => {
                    self.close();
                    return Err(e);
                }
            };
            if n == 0 {
                self.close();
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "broker closed connection",
                )));
            }
        }
    }

    /// Read one response frame and check the correlation id bounded by `deadline`.
    pub async fn read_response_deadline(
        &mut self,
        api_key: i16,
        api_version: i16,
        correlation: i32,
        deadline: Deadline,
    ) -> Result<Bytes> {
        self.read_response_deadline_internal(api_key, api_version, correlation, deadline, true)
            .await
    }

    pub(crate) async fn read_response_deadline_internal(
        &mut self,
        api_key: i16,
        api_version: i16,
        correlation: i32,
        deadline: Deadline,
        close_on_drop: bool,
    ) -> Result<Bytes> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let mut guard = if close_on_drop {
            Some(CloseOnDrop::new(self.closed.clone()))
        } else {
            None
        };
        let frame = self
            .read_frame_deadline_internal(deadline, close_on_drop)
            .await?;
        let mut cur = frame;
        let header = decode_response_header(&mut cur, api_key, api_version)?;
        if header.correlation_id != correlation {
            if let Some(ref mut g) = guard {
                g.complete();
            }
            self.close();
            check_parse_response_correlation(
                &RequestHeader {
                    api_key,
                    api_version,
                    correlation_id: correlation,
                    client_id: Some(self.client_id.clone()),
                },
                &header,
            )?;
        }
        if let Some(ref mut g) = guard {
            g.complete();
        }
        if !is_reserved_correlation_id(correlation) {
            self.in_flight = self.in_flight.saturating_sub(1);
        }
        self.touch();
        Ok(cur)
    }

    /// Read one response frame and check the correlation id.
    pub async fn read_response(
        &mut self,
        api_key: i16,
        api_version: i16,
        correlation: i32,
        request_timeout: Duration,
    ) -> Result<Bytes> {
        self.read_response_deadline(
            api_key,
            api_version,
            correlation,
            Deadline::from_timeout(request_timeout),
        )
        .await
    }

    /// Broker `host:port` used to open this connection.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Kafka `connections.max.idle.ms`. Zero never expires.
    #[must_use]
    pub fn idle_expired(&self, max_idle: Duration) -> bool {
        self.is_closed() || crate::config::connection_idle_expired(self.last_io.elapsed(), max_idle)
    }

    fn touch(&mut self) {
        self.last_io = Instant::now();
    }

    /// Count this socket's [`Self::roundtrip`] on an Admin tracker.
    pub(crate) fn set_stats(&mut self, stats: Arc<crate::metrics::AdminTracker>) {
        self.stats = Some(stats);
    }

    async fn write_request_deadline(
        &mut self,
        api_key: i16,
        api_version: i16,
        correlation: i32,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        deadline: Deadline,
    ) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        self.write_buf.clear();
        self.write_buf.put_i32(0);
        encode_request_header_fields(
            &mut self.write_buf,
            api_key,
            api_version,
            correlation,
            Some(self.client_id.as_str()),
        )?;
        encode_body(&mut self.write_buf)?;
        let size = crate::protocol::buf::i32_from_usize(self.write_buf.len().saturating_sub(4))?;
        let slot = self
            .write_buf
            .get_mut(..4)
            .ok_or_else(|| Error::protocol("short length prefix"))?;
        slot.copy_from_slice(&size.to_be_bytes());
        let payload = self.write_buf.split();
        self.write_all_deadline(&payload, deadline).await?;
        Ok(())
    }

    /// Encode and write one request bounded by `deadline`. Returns the correlation id.
    pub async fn send_deadline(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        deadline: Deadline,
    ) -> Result<i32> {
        if self.is_session_expired() {
            return Err(Error::protocol(
                "broker SASL session lifetime expired; connection must be reauthenticated or closed",
            ));
        }
        let correlation = self.next_correlation();
        self.write_request_deadline(api_key, api_version, correlation, encode_body, deadline)
            .await?;
        self.in_flight += 1;
        Ok(correlation)
    }

    /// Encode and write one request. Returns the correlation id.
    pub async fn send(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        request_timeout: Duration,
    ) -> Result<i32> {
        self.send_deadline(
            api_key,
            api_version,
            encode_body,
            Deadline::from_timeout(request_timeout),
        )
        .await
    }

    /// Write a request and read its response bounded by a single absolute `deadline`.
    pub async fn roundtrip_deadline(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        deadline: Deadline,
    ) -> Result<Bytes> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let started = Instant::now();
        let mut guard = CloseOnDrop::new(self.closed.clone());
        let result = deadline
            .run(async {
                let correlation = self
                    .send_deadline(api_key, api_version, encode_body, deadline)
                    .await?;
                self.read_response_deadline(api_key, api_version, correlation, deadline)
                    .await
            })
            .await;
        if let Some(stats) = &self.stats {
            stats.record(started.elapsed(), result.is_ok());
        }
        if result.is_ok() {
            guard.complete();
        }
        result
    }

    /// Write a request and read its response.
    ///
    /// Uses a single shared absolute deadline across request write,
    /// response header/body read, and partial-frame progress so delayed
    /// peers cannot consume two independent full RPC budgets.
    pub async fn roundtrip(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        request_timeout: Duration,
    ) -> Result<Bytes> {
        let deadline = Deadline::from_timeout(request_timeout);
        self.roundtrip_deadline(api_key, api_version, encode_body, deadline)
            .await
    }

    /// Set negotiated SASL Handshake and Authenticate API versions.
    pub fn set_sasl_versions(&mut self, handshake: i16, authenticate: i16) {
        self.sasl_handshake_version = handshake;
        self.sasl_authenticate_version = authenticate;
    }

    /// Set negotiated SASL Authenticate API version.
    pub fn set_sasl_authenticate_version(&mut self, version: i16) {
        self.sasl_authenticate_version = version;
    }

    /// Negotiated SASL Handshake API version (-1 if unset).
    #[must_use]
    pub fn sasl_handshake_version(&self) -> i16 {
        self.sasl_handshake_version
    }

    /// Negotiated SASL Authenticate API version (-1 if unset).
    #[must_use]
    pub fn sasl_authenticate_version(&self) -> i16 {
        self.sasl_authenticate_version
    }

    /// Write a SASL request and read its response bounded by a single absolute `deadline`.
    pub async fn roundtrip_sasl_deadline(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        deadline: Deadline,
    ) -> Result<Bytes> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if self.in_flight > 0 {
            return Err(Error::protocol(
                "cannot send SASL request while application requests are in-flight; pipeline must be quiesced first",
            ));
        }
        let started = Instant::now();
        let mut guard = CloseOnDrop::new(self.closed.clone());
        let correlation = self.next_sasl_correlation();
        let result = deadline
            .run(async {
                self.write_request_deadline(
                    api_key,
                    api_version,
                    correlation,
                    encode_body,
                    deadline,
                )
                .await?;
                self.read_response_deadline(api_key, api_version, correlation, deadline)
                    .await
            })
            .await;
        if let Some(stats) = &self.stats {
            stats.record(started.elapsed(), result.is_ok());
        }
        if result.is_ok() {
            guard.complete();
        }
        result
    }

    /// Write a SASL request and read its response.
    ///
    /// Java `SaslClientAuthenticator.nextRequestHeader`: correlation ids
    /// come from [`next_sasl_correlation_id`], not
    /// [`next_correlation_id`].
    pub async fn roundtrip_sasl(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        request_timeout: Duration,
    ) -> Result<Bytes> {
        let deadline = Deadline::from_timeout(request_timeout);
        self.roundtrip_sasl_deadline(api_key, api_version, encode_body, deadline)
            .await
    }
}

/// State of a pipelined broker connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineState {
    /// Active and processing application requests.
    Active,
    /// Quiescing: waiting for all in-flight application requests on the wire to complete.
    /// New requests are queued in memory and held.
    Quiescing,
    /// Reauthenticating: mid-connection SASL reauthentication is executing on the wire.
    /// In-flight application request count is 0. New requests are queued in memory.
    Reauthenticating,
    /// Connection has been closed, disconnected, shut down, or session expired.
    Closed,
}

impl PipelineState {
    fn to_u8(self) -> u8 {
        match self {
            Self::Active => 0,
            Self::Quiescing => 1,
            Self::Reauthenticating => 2,
            Self::Closed => 3,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Active,
            1 => Self::Quiescing,
            2 => Self::Reauthenticating,
            _ => Self::Closed,
        }
    }
}

/// A status snapshot of a [`BrokerPipeline`].
#[derive(Clone, Debug)]
pub struct PipelineStatus {
    /// Current state of the pipeline.
    pub state: PipelineState,
    /// Number of application requests currently in flight on the wire.
    pub in_flight: usize,
    /// Number of accepted application requests waiting in the queue.
    pub queued: usize,
    /// Broker session lifetime in milliseconds, if negotiated via SaslAuthenticate v1+.
    pub session_lifetime_ms: Option<i64>,
    /// Instant when SASL authentication completed.
    pub authenticated_at: Option<Instant>,
    /// Absolute instant when the broker SASL session expires.
    pub session_expiry: Option<Instant>,
    /// Instant at which proactive reauthentication should start (85% threshold).
    pub reauth_at: Option<Instant>,
    /// Whether the broker SASL session has expired.
    pub is_session_expired: bool,
    /// Whether the connection needs reauthentication.
    pub needs_reauth: bool,
    /// Whether the underlying connection is closed.
    pub is_closed: bool,
}

type ReauthFn = Box<
    dyn for<'a> FnOnce(
            &'a mut BrokerConn,
            Duration,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>
        + Send,
>;

type EncodeBody = Box<dyn FnOnce(&mut BytesMut) -> Result<()> + Send>;

enum PipelineCommand {
    Request {
        api_key: i16,
        api_version: i16,
        encode_body: EncodeBody,
        tx: tokio::sync::oneshot::Sender<Result<Bytes>>,
        deadline: Deadline,
    },
    Quiesce {
        tx: tokio::sync::oneshot::Sender<Result<()>>,
        deadline: Deadline,
    },
    Resume {
        tx: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Reauthenticate {
        reauth: ReauthFn,
        tx: tokio::sync::oneshot::Sender<Result<()>>,
        timeout: Duration,
    },
    Status {
        tx: tokio::sync::oneshot::Sender<PipelineStatus>,
    },
    Shutdown {
        tx: tokio::sync::oneshot::Sender<()>,
    },
}

struct InFlightItem {
    correlation_id: i32,
    api_key: i16,
    api_version: i16,
    tx: tokio::sync::oneshot::Sender<Result<Bytes>>,
    deadline: Deadline,
}

struct QueuedItem {
    api_key: i16,
    api_version: i16,
    encode_body: EncodeBody,
    tx: tokio::sync::oneshot::Sender<Result<Bytes>>,
    deadline: Deadline,
}

fn fail_command(cmd: PipelineCommand, err: &Error) {
    match cmd {
        PipelineCommand::Request { tx, .. } => {
            drop(tx.send(Err(err.clone())));
        }
        PipelineCommand::Quiesce { tx, .. } => {
            drop(tx.send(Err(err.clone())));
        }
        PipelineCommand::Resume { tx } => {
            drop(tx.send(Err(err.clone())));
        }
        PipelineCommand::Reauthenticate { tx, .. } => {
            drop(tx.send(Err(err.clone())));
        }
        PipelineCommand::Status { tx } => {
            drop(tx.send(PipelineStatus {
                state: PipelineState::Closed,
                in_flight: 0,
                queued: 0,
                session_lifetime_ms: None,
                authenticated_at: None,
                session_expiry: None,
                reauth_at: None,
                is_session_expired: false,
                needs_reauth: false,
                is_closed: true,
            }));
        }
        PipelineCommand::Shutdown { tx } => match tx.send(()) {
            Ok(()) | Err(()) => (),
        },
    }
}

fn fail_all_requests(
    in_flight: &mut VecDeque<InFlightItem>,
    queued: &mut VecDeque<QueuedItem>,
    quiesce_waiters: &mut Vec<tokio::sync::oneshot::Sender<Result<()>>>,
    err: &Error,
) {
    while let Some(item) = in_flight.pop_front() {
        drop(item.tx.send(Err(err.clone())));
    }
    while let Some(item) = queued.pop_front() {
        drop(item.tx.send(Err(err.clone())));
    }
    for tx in quiesce_waiters.drain(..) {
        drop(tx.send(Err(err.clone())));
    }
}

fn make_status(
    conn: &BrokerConn,
    state: PipelineState,
    in_flight: usize,
    queued: usize,
) -> PipelineStatus {
    PipelineStatus {
        state: if conn.is_closed() {
            PipelineState::Closed
        } else {
            state
        },
        in_flight,
        queued,
        session_lifetime_ms: conn.session_lifetime_ms(),
        authenticated_at: conn.authenticated_at(),
        session_expiry: conn.session_expiry(),
        reauth_at: conn.reauth_at(),
        is_session_expired: conn.is_session_expired(),
        needs_reauth: conn.needs_reauth(),
        is_closed: conn.is_closed(),
    }
}

async fn run_pipeline_loop(
    mut conn: BrokerConn,
    max_in_flight: usize,
    mut cmd_rx: tokio::sync::mpsc::Receiver<PipelineCommand>,
    shared_state: Arc<AtomicU8>,
) {
    let mut in_flight: VecDeque<InFlightItem> = VecDeque::new();
    let mut queued: VecDeque<QueuedItem> = VecDeque::new();
    let mut quiesce_waiters: Vec<tokio::sync::oneshot::Sender<Result<()>>> = Vec::new();
    let mut state = PipelineState::Active;
    shared_state.store(state.to_u8(), Ordering::SeqCst);

    loop {
        while state == PipelineState::Active
            && in_flight.len() < max_in_flight
            && !queued.is_empty()
            && !conn.is_closed()
        {
            if conn.is_session_expired() {
                conn.close();
                state = PipelineState::Closed;
                shared_state.store(state.to_u8(), Ordering::SeqCst);
                let err = Error::protocol(
                    "broker SASL session lifetime expired; connection must be reauthenticated or closed",
                );
                fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &err);
                break;
            }
            let Some(item) = queued.pop_front() else {
                break;
            };
            if item.deadline.is_expired() {
                drop(item.tx.send(Err(Error::Timeout)));
                continue;
            }
            match conn
                .send_deadline(
                    item.api_key,
                    item.api_version,
                    item.encode_body,
                    item.deadline,
                )
                .await
            {
                Ok(correlation_id) => {
                    in_flight.push_back(InFlightItem {
                        correlation_id,
                        api_key: item.api_key,
                        api_version: item.api_version,
                        tx: item.tx,
                        deadline: item.deadline,
                    });
                }
                Err(e) => {
                    conn.close();
                    state = PipelineState::Closed;
                    shared_state.store(state.to_u8(), Ordering::SeqCst);
                    drop(item.tx.send(Err(e.clone())));
                    fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &e);
                    break;
                }
            }
        }

        if state == PipelineState::Closed {
            while let Ok(cmd) = cmd_rx.try_recv() {
                fail_command(cmd, &Error::Closed);
            }
            break;
        }

        tokio::select! {
            read_res = async {
                if let Some(front) = in_flight.front() {
                    conn.read_response_deadline_internal(
                        front.api_key,
                        front.api_version,
                        front.correlation_id,
                        front.deadline,
                        false,
                    ).await
                } else {
                    std::future::pending::<Result<Bytes>>().await
                }
            } => {
                match read_res {
                    Ok(body) => {
                        if let Some(item) = in_flight.pop_front() {
                            drop(item.tx.send(Ok(body)));
                        }
                        if in_flight.is_empty() && state == PipelineState::Quiescing {
                            for tx in quiesce_waiters.drain(..) {
                                drop(tx.send(Ok(())));
                            }
                        }
                    }
                    Err(e) => {
                        conn.close();
                        state = PipelineState::Closed;
                        shared_state.store(state.to_u8(), Ordering::SeqCst);
                        fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &e);
                    }
                }
            }
            cmd_opt = cmd_rx.recv() => {
                let Some(cmd) = cmd_opt else {
                    conn.close();
                    state = PipelineState::Closed;
                    shared_state.store(state.to_u8(), Ordering::SeqCst);
                    break;
                };
                match cmd {
                    PipelineCommand::Request {
                        api_key,
                        api_version,
                        encode_body,
                        tx,
                        deadline,
                    } => {
                        if conn.is_closed() || state == PipelineState::Closed {
                            drop(tx.send(Err(Error::Closed)));
                        } else if conn.is_session_expired() {
                            conn.close();
                            state = PipelineState::Closed;
                            shared_state.store(state.to_u8(), Ordering::SeqCst);
                            let err = Error::protocol(
                                "broker SASL session lifetime expired; connection must be reauthenticated or closed",
                            );
                            drop(tx.send(Err(err.clone())));
                            fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &err);
                        } else if state == PipelineState::Active && in_flight.len() < max_in_flight {
                            if deadline.is_expired() {
                                drop(tx.send(Err(Error::Timeout)));
                            } else {
                                match conn.send_deadline(api_key, api_version, encode_body, deadline).await {
                                    Ok(correlation_id) => {
                                        in_flight.push_back(InFlightItem {
                                            correlation_id,
                                            api_key,
                                            api_version,
                                            tx,
                                            deadline,
                                        });
                                    }
                                    Err(e) => {
                                        conn.close();
                                        state = PipelineState::Closed;
                                        shared_state.store(state.to_u8(), Ordering::SeqCst);
                                        drop(tx.send(Err(e.clone())));
                                        fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &e);
                                    }
                                }
                            }
                        } else {
                            queued.push_back(QueuedItem {
                                api_key,
                                api_version,
                                encode_body,
                                tx,
                                deadline,
                            });
                        }
                    }
                    PipelineCommand::Quiesce { tx, deadline } => {
                        if conn.is_closed() || state == PipelineState::Closed {
                            drop(tx.send(Err(Error::Closed)));
                        } else if deadline.is_expired() {
                            drop(tx.send(Err(Error::Timeout)));
                        } else if in_flight.is_empty() {
                            state = PipelineState::Quiescing;
                            shared_state.store(state.to_u8(), Ordering::SeqCst);
                            drop(tx.send(Ok(())));
                        } else {
                            state = PipelineState::Quiescing;
                            shared_state.store(state.to_u8(), Ordering::SeqCst);
                            quiesce_waiters.push(tx);
                        }
                    }
                    PipelineCommand::Resume { tx } => {
                        if conn.is_closed() || state == PipelineState::Closed {
                            drop(tx.send(Err(Error::Closed)));
                        } else {
                            state = PipelineState::Active;
                            shared_state.store(state.to_u8(), Ordering::SeqCst);
                            drop(tx.send(Ok(())));
                        }
                    }
                    PipelineCommand::Reauthenticate { reauth, tx, timeout } => {
                        if conn.is_closed() || state == PipelineState::Closed {
                            drop(tx.send(Err(Error::Closed)));
                            continue;
                        }
                        state = PipelineState::Quiescing;
                        shared_state.store(state.to_u8(), Ordering::SeqCst);
                        let mut drain_err = None;
                        while let Some(front) = in_flight.front() {
                            match conn.read_response_deadline_internal(
                                front.api_key,
                                front.api_version,
                                front.correlation_id,
                                front.deadline,
                                false,
                            ).await {
                                Ok(body) => {
                                    if let Some(item) = in_flight.pop_front() {
                                        drop(item.tx.send(Ok(body)));
                                    }
                                }
                                Err(e) => {
                                    drain_err = Some(e);
                                    break;
                                }
                            }
                            while let Ok(pending_cmd) = cmd_rx.try_recv() {
                                match pending_cmd {
                                    PipelineCommand::Request { api_key, api_version, encode_body, tx: req_tx, deadline } => {
                                        queued.push_back(QueuedItem { api_key, api_version, encode_body, tx: req_tx, deadline });
                                    }
                                    PipelineCommand::Status { tx: stat_tx } => {
                                        drop(stat_tx.send(make_status(&conn, state, in_flight.len(), queued.len())));
                                    }
                                    PipelineCommand::Shutdown { tx: shut_tx } => {
                                        conn.close();
                                        state = PipelineState::Closed;
                                        shared_state.store(state.to_u8(), Ordering::SeqCst);
                                        fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &Error::Closed);
                                        match shut_tx.send(()) {
                                            Ok(()) | Err(()) => (),
                                        }
                                        return;
                                    }
                                    PipelineCommand::Quiesce { tx: qtx, .. } => {
                                        quiesce_waiters.push(qtx);
                                    }
                                    PipelineCommand::Resume { tx: rtx } => {
                                        drop(rtx.send(Err(Error::protocol("cannot resume while reauthenticating"))));
                                    }
                                    PipelineCommand::Reauthenticate { tx: dtx, .. } => {
                                        drop(dtx.send(Err(Error::protocol("reauthentication already in progress"))));
                                    }
                                }
                            }
                        }

                        if let Some(e) = drain_err {
                            conn.close();
                            state = PipelineState::Closed;
                            shared_state.store(state.to_u8(), Ordering::SeqCst);
                            drop(tx.send(Err(e.clone())));
                            fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &e);
                            continue;
                        }

                        for qtx in quiesce_waiters.drain(..) {
                            drop(qtx.send(Ok(())));
                        }

                        state = PipelineState::Reauthenticating;
                        shared_state.store(state.to_u8(), Ordering::SeqCst);
                        let reauth_res = reauth(&mut conn, timeout).await;
                        match reauth_res {
                            Ok(()) => {
                                state = PipelineState::Active;
                                shared_state.store(state.to_u8(), Ordering::SeqCst);
                                drop(tx.send(Ok(())));
                            }
                            Err(e) => {
                                conn.close();
                                state = PipelineState::Closed;
                                shared_state.store(state.to_u8(), Ordering::SeqCst);
                                drop(tx.send(Err(e.clone())));
                                fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &e);
                            }
                        }
                    }
                    PipelineCommand::Status { tx } => {
                        drop(tx.send(make_status(&conn, state, in_flight.len(), queued.len())));
                    }
                    PipelineCommand::Shutdown { tx } => {
                        conn.close();
                        state = PipelineState::Closed;
                        shared_state.store(state.to_u8(), Ordering::SeqCst);
                        fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &Error::Closed);
                        match tx.send(()) {
                            Ok(()) | Err(()) => (),
                        }
                        break;
                    }
                }
            }
        }
    }
}

/// Pipelined connection manager supporting non-interleaved mid-connection SASL reauthentication.
///
/// Ensures that when session reauthentication is triggered:
/// 1. Pipelined application traffic is quiesced (all in-flight wire requests drain).
/// 2. Any new application requests submitted during quiescing or reauthentication are queued
///    in memory and accepted (no accepted work is lost).
/// 3. Reauthentication executes on the quiet wire using reserved SASL correlation IDs.
///    Application and SASL correlation IDs are never mixed.
/// 4. Upon successful reauth, pipelined traffic resumes and queued requests are dispatched.
/// 5. Upon failure, disconnect, or expiry, all accepted and in-flight work fails cleanly.
#[derive(Clone)]
pub struct BrokerPipeline {
    cmd_tx: tokio::sync::mpsc::Sender<PipelineCommand>,
    closed: Arc<AtomicBool>,
    shared_state: Arc<AtomicU8>,
}

impl fmt::Debug for BrokerPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerPipeline")
            .field("state", &self.state())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl BrokerPipeline {
    /// Create a new pipelined connection wrapper with default max in-flight (5).
    #[must_use]
    pub fn new(conn: BrokerConn) -> Self {
        Self::with_max_in_flight(conn, 5)
    }

    /// Create a new pipelined connection wrapper with specified max in-flight requests.
    #[must_use]
    pub fn with_max_in_flight(conn: BrokerConn, max_in_flight: usize) -> Self {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(128);
        let closed = conn.closed.clone();
        let shared_state = Arc::new(AtomicU8::new(PipelineState::Active.to_u8()));
        drop(tokio::spawn(run_pipeline_loop(
            conn,
            max_in_flight.max(1),
            cmd_rx,
            shared_state.clone(),
        )));
        Self {
            cmd_tx,
            closed,
            shared_state,
        }
    }

    /// Whether this pipeline's underlying connection is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst) || self.state() == PipelineState::Closed
    }

    /// Current pipeline state.
    #[must_use]
    pub fn state(&self) -> PipelineState {
        PipelineState::from_u8(self.shared_state.load(Ordering::SeqCst))
    }

    /// Submit an application request to the pipeline.
    ///
    /// If the pipeline is currently quiescing or reauthenticating, the request is
    /// accepted into the queue and will be dispatched once reauthentication succeeds
    /// and the pipeline resumes. Accepted work is never lost.
    pub async fn request<F>(
        &self,
        api_key: i16,
        api_version: i16,
        encode_body: F,
        timeout: Duration,
    ) -> Result<Bytes>
    where
        F: FnOnce(&mut BytesMut) -> Result<()> + Send + 'static,
    {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let deadline = Deadline::from_timeout(timeout);
        let cmd = PipelineCommand::Request {
            api_key,
            api_version,
            encode_body: Box::new(encode_body),
            tx,
            deadline,
        };
        self.cmd_tx.send(cmd).await.map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Quiesce the pipeline: stop sending new requests and wait until all in-flight
    /// requests on the wire have received their responses.
    pub async fn quiesce(&self, timeout: Duration) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let deadline = Deadline::from_timeout(timeout);
        self.cmd_tx
            .send(PipelineCommand::Quiesce { tx, deadline })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Resume the pipeline after a manual quiesce.
    pub async fn resume(&self) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(PipelineCommand::Resume { tx })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Execute mid-connection reauthentication.
    ///
    /// Automatically quiesces in-flight application requests before invoking `reauth`,
    /// executes `reauth` on the quiet wire using reserved SASL correlation IDs, and
    /// resumes the pipeline upon success. Any application requests accepted while
    /// quiescing or reauthenticating are queued and dispatched upon resumption.
    pub async fn reauthenticate<F>(&self, reauth: F, timeout: Duration) -> Result<()>
    where
        F: for<'a> FnOnce(
                &'a mut BrokerConn,
                Duration,
            ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>
            + Send
            + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let boxed_reauth: ReauthFn = Box::new(reauth);
        self.cmd_tx
            .send(PipelineCommand::Reauthenticate {
                reauth: boxed_reauth,
                tx,
                timeout,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Mid-connection OAUTHBEARER reauthentication with access token.
    pub async fn reauthenticate_oauthbearer_token(
        &self,
        token: &str,
        timeout: Duration,
    ) -> Result<()> {
        let token = token.to_string();
        self.reauthenticate(
            move |conn, timeout| {
                Box::pin(async move {
                    crate::protocol::sasl::reauthenticate_oauthbearer_token(conn, &token, timeout)
                        .await
                })
            },
            timeout,
        )
        .await
    }

    /// Mid-connection OAUTHBEARER reauthentication with token provider.
    pub async fn reauthenticate_with_token_provider<
        P: crate::protocol::oidc::TokenProvider + ?Sized,
    >(
        &self,
        provider: &P,
        timeout: Duration,
    ) -> Result<()> {
        let token = provider.token(timeout).await?;
        self.reauthenticate_oauthbearer_token(&token, timeout).await
    }

    /// Mid-connection PLAIN reauthentication.
    pub async fn reauthenticate_plain(
        &self,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<()> {
        let user = user.to_string();
        let pass = pass.to_string();
        self.reauthenticate(
            move |conn, timeout| {
                Box::pin(async move {
                    crate::protocol::sasl::reauthenticate_plain(conn, &user, &pass, timeout).await
                })
            },
            timeout,
        )
        .await
    }

    /// Mid-connection SCRAM reauthentication.
    pub async fn reauthenticate_scram(
        &self,
        alg: crate::protocol::scram::ScramAlg,
        user: &str,
        pass: &str,
        timeout: Duration,
    ) -> Result<()> {
        let user = user.to_string();
        let pass = pass.to_string();
        self.reauthenticate(
            move |conn, timeout| {
                Box::pin(async move {
                    crate::protocol::sasl::reauthenticate_scram(conn, alg, &user, &pass, timeout)
                        .await
                })
            },
            timeout,
        )
        .await
    }

    /// Query the pipeline status.
    pub async fn status(&self) -> Result<PipelineStatus> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(PipelineCommand::Status { tx })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }

    /// Number of in-flight application requests currently on the wire.
    pub async fn in_flight_count(&self) -> Result<usize> {
        Ok(self.status().await?.in_flight)
    }

    /// Number of accepted application requests waiting in the queue.
    pub async fn queued_count(&self) -> Result<usize> {
        Ok(self.status().await?.queued)
    }

    /// Whether the pipeline is currently quiesced with 0 in-flight requests.
    pub async fn is_quiesced(&self) -> Result<bool> {
        let st = self.status().await?;
        Ok(st.state == PipelineState::Quiescing && st.in_flight == 0)
    }

    /// Broker session lifetime in milliseconds.
    pub async fn session_lifetime_ms(&self) -> Result<Option<i64>> {
        Ok(self.status().await?.session_lifetime_ms)
    }

    /// Whether the broker SASL session has expired.
    pub async fn is_session_expired(&self) -> Result<bool> {
        Ok(self.status().await?.is_session_expired)
    }

    /// Whether proactive reauthentication threshold has been reached.
    pub async fn needs_reauth(&self) -> Result<bool> {
        Ok(self.status().await?.needs_reauth)
    }

    /// Gracefully shutdown the pipeline and close the connection.
    pub async fn shutdown(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .cmd_tx
            .send(PipelineCommand::Shutdown { tx })
            .await
            .is_ok()
        {
            match rx.await {
                Ok(()) | Err(_) => (),
            }
        }
    }
}

/// Install rustls `ring` as the process crypto provider. Idempotent.
pub fn install_crypto_provider() {
    ensure_crypto();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_frame_grows_once_to_known_size() {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&[0, 0, 0, 16]);
        reserve_frame(&mut buf, 4 + 1_000_000);
        assert!(buf.capacity() >= 1_000_004);
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn reserve_frame_is_noop_when_already_filled() {
        let mut buf = BytesMut::with_capacity(32);
        buf.extend_from_slice(&[0u8; 16]);
        let cap = buf.capacity();
        reserve_frame(&mut buf, 8);
        assert_eq!(buf.capacity(), cap);
        assert_eq!(buf.len(), 16);
    }

    #[test]
    fn reserved_correlation_ids_match_java() {
        assert_eq!(MAX_RESERVED_CORRELATION_ID, i32::MAX);
        assert_eq!(MIN_RESERVED_CORRELATION_ID, i32::MAX - 7);
        assert!(!is_reserved_correlation_id(i32::MAX - 8));
        assert!(is_reserved_correlation_id(MIN_RESERVED_CORRELATION_ID));
        assert!(is_reserved_correlation_id(MAX_RESERVED_CORRELATION_ID));
        assert!(!is_reserved_correlation_id(i32::MIN));
        assert!(!is_reserved_correlation_id(0));
        assert!(!is_reserved_correlation_id(1));
    }

    #[test]
    fn next_correlation_id_skips_sasl_reserved_range() {
        // NetworkClientTest.testCorrelationId: 100 ids, none reserved.
        let mut correlation = 0i32;
        let mut ids = Vec::new();
        for _ in 0..100 {
            ids.push(next_correlation_id(&mut correlation));
        }
        assert_eq!(ids.len(), 100);
        let mut unique = ids.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 100);
        for id in ids {
            assert!(
                !is_reserved_correlation_id(id),
                "reserved correlation id {id}"
            );
            assert!(id < MIN_RESERVED_CORRELATION_ID);
        }

        let mut wrap = MIN_RESERVED_CORRELATION_ID - 1;
        assert_eq!(next_correlation_id(&mut wrap), i32::MAX - 8);
        assert_eq!(next_correlation_id(&mut wrap), i32::MIN);
        assert_eq!(next_correlation_id(&mut wrap), i32::MIN + 1);
    }

    #[test]
    fn next_sasl_correlation_id_uses_reserved_range() {
        // SaslClientAuthenticator.nextCorrelationId: field starts at 0.
        let mut correlation = 0i32;
        assert_eq!(
            next_sasl_correlation_id(&mut correlation),
            MIN_RESERVED_CORRELATION_ID
        );
        assert_eq!(
            next_sasl_correlation_id(&mut correlation),
            MIN_RESERVED_CORRELATION_ID + 1
        );

        let mut cycling = MIN_RESERVED_CORRELATION_ID;
        let mut issued = Vec::new();
        for _ in 0..8 {
            issued.push(next_sasl_correlation_id(&mut cycling));
        }
        assert_eq!(
            issued,
            (MIN_RESERVED_CORRELATION_ID..=MAX_RESERVED_CORRELATION_ID).collect::<Vec<_>>()
        );
        assert_eq!(cycling, i32::MIN);
        // After Integer.MAX_VALUE the field wraps to MIN_VALUE, which is
        // not reserved, so the next id resets to MIN_RESERVED.
        assert_eq!(
            next_sasl_correlation_id(&mut cycling),
            MIN_RESERVED_CORRELATION_ID
        );
        assert!(!is_reserved_correlation_id(i32::MIN));
    }

    #[test]
    fn parse_response_wraps_unrelated_sasl_correlation() {
        let sasl = RequestHeader {
            api_key: crate::protocol::api_keys::SASL_HANDSHAKE,
            api_version: 1,
            correlation_id: MIN_RESERVED_CORRELATION_ID,
            client_id: Some("client".into()),
        };
        let kafka = ResponseHeader { correlation_id: 1 };
        let err = check_parse_response_correlation(&sasl, &kafka).unwrap_err();
        let want = format!(
            "The response is unrelated to Sasl request since its correlation id is 1 and the reserved range for Sasl request is [ {MIN_RESERVED_CORRELATION_ID},{MAX_RESERVED_CORRELATION_ID}]"
        );
        assert!(err.to_string().contains(&want), "{err}");

        let other_sasl = ResponseHeader {
            correlation_id: MAX_RESERVED_CORRELATION_ID,
        };
        let mismatch = check_parse_response_correlation(&sasl, &other_sasl).unwrap_err();
        assert!(
            mismatch.to_string().contains("Correlation id for response"),
            "{mismatch}"
        );

        let kafka_req = RequestHeader {
            api_key: crate::protocol::api_keys::PRODUCE,
            api_version: 9,
            correlation_id: 1,
            client_id: Some("client".into()),
        };
        let kafka_resp = ResponseHeader { correlation_id: 2 };
        let kafka_err = check_parse_response_correlation(&kafka_req, &kafka_resp).unwrap_err();
        assert!(
            kafka_err
                .to_string()
                .contains("Correlation id for response"),
            "{kafka_err}"
        );
        check_parse_response_correlation(&kafka_req, &ResponseHeader { correlation_id: 1 })
            .unwrap();
    }

    #[test]
    fn utils_host_port_match_java() {
        for protocol in ["PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"] {
            assert_eq!(
                get_host(&format!("{protocol}://mydomain.com:8080")),
                Some("mydomain.com")
            );
            assert_eq!(
                get_host(&format!("{protocol}://MyDomain.com:8080")),
                Some("MyDomain.com")
            );
            assert_eq!(
                get_host(&format!("{protocol}://My_Domain.com:8080")),
                Some("My_Domain.com")
            );
            assert_eq!(get_host(&format!("{protocol}://[::1]:1234")), Some("::1"));
            assert_eq!(
                get_host(&format!(
                    "{protocol}://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678"
                )),
                Some("2001:db8:85a3:8d3:1319:8a2e:370:7348")
            );
            assert_eq!(
                get_host(&format!(
                    "{protocol}://[2001:DB8:85A3:8D3:1319:8A2E:370:7348]:5678"
                )),
                Some("2001:DB8:85A3:8D3:1319:8A2E:370:7348")
            );
            assert_eq!(
                get_host(&format!("{protocol}://[fe80::b1da:69ca:57f7:63d8%3]:5678")),
                Some("fe80::b1da:69ca:57f7:63d8%3")
            );
            assert_eq!(get_host(&format!("{protocol}://mydo)main.com:8080")), None);
            assert_eq!(get_host(&format!("{protocol}://mydo(main.com:8080")), None);
        }
        assert_eq!(get_host("127.0.0.1:8000"), Some("127.0.0.1"));
        assert_eq!(get_host("[::1]:1234"), Some("::1"));
        assert_eq!(get_host("ho)st:9092"), None);
        assert_eq!(get_port("127.0.0.1:8000"), Some(8000));
        assert_eq!(get_port("mydomain.com:8080"), Some(8080));
        assert_eq!(get_port("[::1]:1234"), Some(1234));
        assert_eq!(
            get_port("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678"),
            Some(5678)
        );
        assert_eq!(get_port("[fe80::b1da:69ca:57f7:63d8%3]:5678"), Some(5678));
        assert_eq!(get_port("host:-92"), None);
        assert_eq!(get_port("host:-9-2"), None);
        assert_eq!(get_port("host:92-"), None);
        assert_eq!(get_port("host:9-2"), None);
        assert!(valid_host_pattern("127.0.0.1"));
        assert!(valid_host_pattern("mydomain.com"));
        assert!(valid_host_pattern("My_Domain.com"));
        assert!(valid_host_pattern("::1"));
        assert_eq!(format_address("127.0.0.1", 8000), "127.0.0.1:8000");
        assert_eq!(format_address("mydomain.com", 8080), "mydomain.com:8080");
        assert_eq!(format_address("::1", 1234), "[::1]:1234");
        assert_eq!(
            format_address("2001:db8:85a3:8d3:1319:8a2e:370:7348", 5678),
            "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678"
        );
        assert_eq!(host_of("127.0.0.1:9092"), "127.0.0.1");
        assert_eq!(host_of("[::1]:9092"), "::1");
        assert_eq!(host_of("PLAINTEXT://broker.local:9093"), "broker.local");
    }

    #[test]
    fn parse_and_validate_addresses_match_java() {
        let empty: Vec<String> = Vec::new();
        assert!(parse_and_validate_addresses(&empty)
            .unwrap_err()
            .to_string()
            .contains("no bootstrap servers"));
        let blanks = vec![String::new(), String::new()];
        assert!(parse_and_validate_addresses(&blanks)
            .unwrap_err()
            .to_string()
            .contains("No resolvable bootstrap urls given in bootstrap.servers"));
        let bad = vec!["ho)st:9092".to_string()];
        assert!(parse_and_validate_addresses(&bad)
            .unwrap_err()
            .to_string()
            .contains("Invalid url in bootstrap.servers: ho)st:9092"));
        let port = vec!["host:70000".to_string()];
        assert!(parse_and_validate_addresses(&port)
            .unwrap_err()
            .to_string()
            .contains("Invalid port in bootstrap.servers: host:70000"));
        let scheme = vec!["PLAINTEXT://127.0.0.1:9092".to_string()];
        assert_eq!(
            parse_and_validate_addresses(&scheme).unwrap(),
            vec!["127.0.0.1:9092".to_string()]
        );
        let v6 = vec!["[::1]:9092".to_string()];
        assert_eq!(
            parse_and_validate_addresses(&v6).unwrap(),
            vec!["[::1]:9092".to_string()]
        );
    }

    #[test]
    fn deadline_contract_tracks_remaining_and_expiration() {
        let deadline = Deadline::from_timeout(Duration::from_millis(200));
        assert!(!deadline.is_expired());
        assert!(deadline.check_expired().is_ok());
        let rem = deadline.remaining().unwrap();
        assert!(rem > Duration::ZERO && rem <= Duration::from_millis(200));

        let expired = Deadline::at(TokioInstant::now() - Duration::from_millis(10));
        assert!(expired.is_expired());
        assert!(matches!(expired.check_expired(), Err(Error::Timeout)));
        assert!(matches!(expired.remaining(), Err(Error::Timeout)));
    }

    #[tokio::test]
    async fn deadline_run_io_enforces_budget() {
        let deadline = Deadline::from_timeout(Duration::from_millis(30));
        let res = deadline
            .run_io(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<(), std::io::Error>(())
            })
            .await;
        assert!(matches!(res, Err(Error::Timeout)));
    }
}
