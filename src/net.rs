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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
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
            Err(_) => {
                // Dropping the bounded future only schedules cancellation of
                // dials it still owns. Yield until those sockets close.
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                Err(Error::Timeout)
            }
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
            Err(_) => {
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                Err(Error::Timeout)
            }
        }
    }
}

struct CloseOnDrop {
    closed: Arc<AtomicBool>,
    /// Wakes the idle watch. Dropping a read only sets the flag; the watch
    /// is parked on the socket and will not shut it down until it is woken.
    idle_arm: Arc<tokio::sync::Notify>,
    completed: bool,
}

impl CloseOnDrop {
    fn new(closed: Arc<AtomicBool>, idle_arm: Arc<tokio::sync::Notify>) -> Self {
        Self {
            closed,
            idle_arm,
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
            self.idle_arm.notify_one();
        }
    }
}

/// Wake the idle watch if a stream lock is dropped before it is released.
///
/// A read replaces the watch's socket registration. Cancelling that read
/// drops the lock without [`BrokerConn::release_stream`], so the watch
/// stays parked and a later close never becomes a FIN.
struct Rearm {
    arm: Arc<tokio::sync::Notify>,
    armed: bool,
}

impl Rearm {
    fn arm(arm: Arc<tokio::sync::Notify>) -> Self {
        Self { arm, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for Rearm {
    fn drop(&mut self) {
        if self.armed {
            self.arm.notify_one();
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

/// After a frame consumes the read buffer, keep room for another of the same size.
///
/// [`BytesMut::split_to`] leaves no spare capacity when the frame filled the
/// allocation. The next read would otherwise start at the small default and
/// copy the prefix into a new buffer once the length is known.
pub(crate) fn restore_read_capacity(buf: &mut BytesMut, frame_len: usize) {
    if buf.capacity() == 0 && frame_len > 0 {
        reserve_frame(buf, frame_len);
    }
}

/// Bytes pulled into `read_buf` on one stalled-write read when the next
/// frame length is not known yet. A known short frame is filled directly.
const STALLED_WRITE_READ_CHUNK: usize = 8 * 1024;

/// How many bytes to append while a write waits, and the absolute `read_buf`
/// length to reserve when that read finishes a length-prefixed frame.
///
/// Complete frames already buffered are skipped so a later frame is still
/// read. An illegal length is not reserved; the framed reader rejects it.
fn stalled_write_read(read_buf: &BytesMut) -> (usize, Option<usize>) {
    let (budget, frame_end, _) = stalled_write_read_from(read_buf, 0);
    (budget, frame_end)
}

/// Like [`stalled_write_read`], continuing from `scanned` so a stalled write
/// does not walk every complete frame again after each chunk.
fn stalled_write_read_from(
    read_buf: &BytesMut,
    mut offset: usize,
) -> (usize, Option<usize>, usize) {
    let bytes = read_buf.as_ref();
    if offset > bytes.len() {
        offset = 0;
    }
    while bytes.len().saturating_sub(offset) >= 4 {
        let Some(prefix) = bytes.get(offset..offset.saturating_add(4)) else {
            break;
        };
        let Ok(raw) = <[u8; 4]>::try_from(prefix) else {
            break;
        };
        let size = i32::from_be_bytes(raw);
        if !(0..=MAX_FRAME).contains(&size) {
            return (STALLED_WRITE_READ_CHUNK, None, offset);
        }
        let Ok(size) = usize::try_from(size) else {
            return (STALLED_WRITE_READ_CHUNK, None, offset);
        };
        let Some(total) = 4usize.checked_add(size) else {
            return (STALLED_WRITE_READ_CHUNK, None, offset);
        };
        let Some(frame_end) = offset.checked_add(total) else {
            return (STALLED_WRITE_READ_CHUNK, None, offset);
        };
        if bytes.len() < frame_end {
            return (frame_end - bytes.len(), Some(frame_end), offset);
        }
        offset = frame_end;
    }
    (STALLED_WRITE_READ_CHUNK, None, offset)
}

/// A stalled write is also the socket's reader. One max frame is enough for
/// the response that was already on the wire. More than that, or a length
/// this connection will not accept, is a peer that is not reading.
enum StalledScan {
    Reject,
    /// More complete frames are buffered. Yield before walking the rest.
    Yield,
    Continue,
}

fn advance_stalled_scan(read_buf: &[u8], scanned: &mut usize) -> StalledScan {
    let cap = usize::try_from(MAX_FRAME).unwrap_or(usize::MAX);
    if read_buf.len() > cap {
        return StalledScan::Reject;
    }
    if *scanned > read_buf.len() {
        *scanned = 0;
    }
    match scan_frame_prefix(read_buf, *scanned, 64 * 1024) {
        PrefixScan::Rejected => StalledScan::Reject,
        PrefixScan::Rest(at) => {
            *scanned = at;
            StalledScan::Continue
        }
        PrefixScan::Partial(at) => {
            *scanned = at;
            StalledScan::Yield
        }
    }
}

/// True when `buf` contains a length this connection will not accept.
///
/// Complete frames are skipped. A length after an empty frame is still
/// rejected, and a short tail of the next frame is not.
enum PrefixScan {
    Rejected,
    /// Walked through `at`. Anything after that is a short tail or empty.
    Rest(usize),
    /// Walked through `at` and more complete frames are still buffered.
    Partial(usize),
}

/// Walk complete frames from `start`, stopping after `budget` bytes.
///
/// A long run of empty frames is still legal. Scanning all of it in one
/// turn keeps the caller from running until the walk finishes.
fn scan_frame_prefix(buf: &[u8], start: usize, budget: usize) -> PrefixScan {
    let mut offset = start.min(buf.len());
    let limit = offset.saturating_add(budget.max(4));
    while buf.len().saturating_sub(offset) >= 4 {
        if offset >= limit {
            return PrefixScan::Partial(offset);
        }
        let Some(prefix) = buf.get(offset..offset.saturating_add(4)) else {
            return PrefixScan::Rejected;
        };
        let Ok(raw) = <[u8; 4]>::try_from(prefix) else {
            return PrefixScan::Rejected;
        };
        let size = i32::from_be_bytes(raw);
        if !(0..=MAX_FRAME).contains(&size) {
            return PrefixScan::Rejected;
        }
        let Ok(size) = usize::try_from(size) else {
            return PrefixScan::Rejected;
        };
        let Some(total) = 4usize.checked_add(size) else {
            return PrefixScan::Rejected;
        };
        let Some(frame_end) = offset.checked_add(total) else {
            return PrefixScan::Rejected;
        };
        if buf.len() < frame_end {
            return PrefixScan::Rest(offset);
        }
        offset = frame_end;
    }
    PrefixScan::Rest(offset)
}

/// Whether `buf` contains a rejected length, yielding between slices.
///
/// A long run of empty frames is legal. Walking all of it before the first
/// await keeps the caller from running until the walk finishes.
async fn prefix_rejected(buf: &[u8]) -> bool {
    let mut scanned = 0usize;
    loop {
        if scanned > buf.len() {
            scanned = 0;
        }
        match scan_frame_prefix(buf, scanned, 64 * 1024) {
            PrefixScan::Rejected => return true,
            PrefixScan::Rest(_) => return false,
            PrefixScan::Partial(at) => {
                scanned = at;
                tokio::task::yield_now().await;
            }
        }
    }
}

/// True when `buf` holds one full frame, or a length the framed reader will reject.
fn inbound_satisfied(buf: &BytesMut) -> bool {
    let Some(prefix) = buf.get(..4) else {
        return false;
    };
    let Ok(raw) = <[u8; 4]>::try_from(prefix) else {
        return false;
    };
    let size = i32::from_be_bytes(raw);
    if !(0..=MAX_FRAME).contains(&size) {
        return true;
    }
    let Ok(size) = usize::try_from(size) else {
        return true;
    };
    buf.len() >= 4usize.saturating_add(size)
}

/// rustls client settings. No OpenSSL.
///
/// [`Debug`] never prints PEM bytes; the private key is always `<redacted>`
/// (KL-06).
///
/// Trust and identity are fixed per client: every connection a client opens
/// uses this config as built. Rotate roots or client certificates by
/// constructing a client with the replacement [`TlsConfig`]; connections
/// established before the rotation are unaffected because TLS
/// authenticates once at handshake (KL06-05). The handshake itself is
/// bounded by the configured connect timeout; a failed handshake never
/// falls back to plaintext.
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

async fn write_all_pump<S>(stream: &mut S, read_buf: &mut BytesMut, bytes: &[u8]) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut pos = 0usize;
    // Complete frames already parsed. The next stall read continues here.
    let mut scanned = 0usize;
    poll_fn(|cx| {
        // A length already buffered is not waiting on another read. The
        // peer is not reading, so the write must not sit until its deadline.
        // A long run of empty frames is walked in slices so that deadline
        // can run before the rest of the buffer is scanned.
        match advance_stalled_scan(read_buf, &mut scanned) {
            StalledScan::Reject => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stalled write buffered more than one max frame",
                )));
            }
            StalledScan::Yield => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            StalledScan::Continue => {}
        }
        let Some(remaining) = bytes.get(pos..) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write pos past end",
            )));
        };
        // One ready socket can accept the whole request. Offer one chunk so
        // the write deadline runs before the rest is copied.
        let offer = remaining.len().min(STALLED_WRITE_READ_CHUNK);
        let Some(chunk) = remaining.get(..offer) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write pos past end",
            )));
        };
        match Pin::new(&mut *stream).poll_write(cx, chunk) {
            Poll::Ready(Ok(0)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "broker write returned 0",
            ))),
            Poll::Ready(Ok(n)) => {
                pos += n;
                if pos >= bytes.len() {
                    Poll::Ready(Ok(()))
                } else {
                    // A socket that accepts one byte at a time keeps this poll
                    // running, so the write deadline cannot stop it.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                // Read straight into `read_buf` spare capacity. The framed
                // reader and a stalled TLS handshake both need those bytes,
                // and a stack buffer would copy them once on the way in.
                let (budget, frame_end, next) = stalled_write_read_from(read_buf, scanned);
                scanned = next;
                if let Some(end) = frame_end {
                    reserve_frame(read_buf, end);
                } else {
                    read_buf.reserve(budget);
                }
                // A known frame stays reserved. One read still stops after a
                // chunk so the write deadline runs before the rest is copied.
                let step = budget.min(STALLED_WRITE_READ_CHUNK);
                let mut limited = (&mut *read_buf).limit(step);
                // `read_buf` only advances `read_buf` after a ready read, so
                // dropping this future on Pending does not lose bytes.
                let mut read = std::pin::pin!(stream.read_buf(&mut limited));
                match read.as_mut().poll(cx) {
                    Poll::Ready(Ok(0)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "broker closed connection",
                    ))),
                    Poll::Ready(Ok(_)) => {
                        // Ready data keeps this poll going, so the write
                        // deadline never gets another turn. Stop after one
                        // max frame instead of buffering the peer without a
                        // limit.
                        match advance_stalled_scan(read_buf, &mut scanned) {
                            StalledScan::Reject => {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "stalled write buffered more than one max frame",
                                )));
                            }
                            StalledScan::Yield => {
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                            StalledScan::Continue => {}
                        }
                        // The bytes stay buffered. Yield so the write deadline
                        // can stop a peer that keeps the read ready.
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    })
    .await
}

enum ConnIo {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
    /// Test double that returns a fixed body in small ready chunks.
    #[cfg(test)]
    Script(ScriptIo),
}

/// One ready chunk, then a pause, so a read loop that never yields can be timed.
#[cfg(test)]
struct ScriptIo {
    body: Vec<u8>,
    pos: usize,
    chunk: usize,
    pause: Duration,
}

#[cfg(test)]
impl AsyncRead for ScriptIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pos >= self.body.len() || buf.remaining() == 0 {
            return Poll::Pending;
        }
        let n = self
            .chunk
            .min(self.body.len() - self.pos)
            .min(buf.remaining());
        let end = self.pos + n;
        let Some(chunk) = self.body.get(self.pos..end) else {
            return Poll::Pending;
        };
        buf.put_slice(chunk);
        self.pos = end;
        // A large ready read spends time in proportion to the bytes copied.
        // One pause covers one kibibyte, so a single poll that fills a whole
        // frame outruns a short deadline.
        let units = u32::try_from(n.div_ceil(1024)).unwrap_or(u32::MAX);
        std::thread::sleep(self.pause.saturating_mul(units));
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
impl AsyncWrite for ScriptIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !buf.is_empty() {
            let units = u32::try_from(buf.len().div_ceil(1024)).unwrap_or(u32::MAX);
            std::thread::sleep(self.pause.saturating_mul(units));
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
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
            #[cfg(test)]
            ConnIo::Script(s) => Pin::new(s).poll_read(cx, buf),
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
            #[cfg(test)]
            ConnIo::Script(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnIo::Tcp(s) => Pin::new(s).poll_flush(cx),
            ConnIo::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            #[cfg(test)]
            ConnIo::Script(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ConnIo::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            ConnIo::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            #[cfg(test)]
            ConnIo::Script(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

enum IdleRead {
    Eof,
    Busy,
    Data(usize),
    Shut,
}

/// Discard bytes still arriving after this side has stopped writing.
///
/// Shutdown only closes the write half. A peer blocked in its own write stays
/// there until those bytes are read or the socket is dropped.
async fn discard_peer_write<S: AsyncRead + Unpin>(io: &mut S) {
    let mut discard = [0u8; 8 * 1024];
    loop {
        let n = poll_fn(|cx| {
            let mut read = ReadBuf::new(&mut discard);
            match Pin::new(&mut *io).poll_read(cx, &mut read) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(_)) => Poll::Ready(0),
                Poll::Ready(Ok(())) => Poll::Ready(read.filled().len()),
            }
        })
        .await;
        if n == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
}

/// Notice a broker that stops writing while the owner is not reading.
///
/// The owner holds the stream lock for the whole read or write, so this
/// watch only runs when the connection is idle. The socket wakes the watch,
/// so a close is not delayed until the next timer. Bytes it does read stay
/// in `prefetch` for the next framed read.
fn spawn_idle_watch(
    stream: Arc<tokio::sync::Mutex<ConnIo>>,
    prefetch: Arc<parking_lot::Mutex<BytesMut>>,
    closed: Arc<AtomicBool>,
    arm: Arc<tokio::sync::Notify>,
) -> tokio::task::AbortHandle {
    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        // Yield often enough that a ready flood cannot hold the runtime, and
        // rarely enough that a full unsolicited frame still reaches its cap.
        let mut since_yield = 0usize;
        // Resume point for the prefix walk. A full rescan of a long run of
        // empty frames would hold this task until the buffer is capped.
        let mut scanned = 0usize;
        loop {
            if closed.load(Ordering::SeqCst) {
                if let Ok(mut io) = stream.try_lock() {
                    drop(poll_fn(|cx| Pin::new(&mut *io).poll_shutdown(cx)).await);
                    // The peer can still be writing. Discard those bytes so
                    // that write can finish and the peer can observe this
                    // shutdown.
                    discard_peer_write(&mut *io).await;
                    arm.notify_one();
                    return;
                }
                // The owner still holds the socket. Leaving now would drop
                // the drain, and the peer would stay blocked in its write.
                tokio::select! {
                    biased;
                    _ = arm.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                }
                continue;
            }
            tokio::select! {
                biased;
                // A request replaced this watch's socket registration. Poll
                // again now that the owner has released the stream.
                _ = arm.notified() => continue,
                seen = poll_fn(|cx| {
                    let Ok(mut io) = stream.try_lock() else {
                        return Poll::Ready(IdleRead::Busy);
                    };
                    if closed.load(Ordering::SeqCst) {
                        return Poll::Ready(IdleRead::Shut);
                    }
                    let mut read = ReadBuf::new(&mut buf);
                    match Pin::new(&mut *io).poll_read(cx, &mut read) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Err(_)) => Poll::Ready(IdleRead::Eof),
                        Poll::Ready(Ok(())) => {
                            let filled = read.filled().len();
                            if filled == 0 {
                                Poll::Ready(IdleRead::Eof)
                            } else if let Some(chunk) = buf.get(..filled) {
                                prefetch.lock().extend_from_slice(chunk);
                                Poll::Ready(IdleRead::Data(filled))
                            } else {
                                Poll::Ready(IdleRead::Data(0))
                            }
                        }
                    }
                }) => {
                    match seen {
                        IdleRead::Busy => {
                            // The owner holds the stream. A close during that
                            // read must not wait out the retry sleep.
                            tokio::select! {
                                biased;
                                _ = arm.notified() => {}
                                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                            }
                        }
                        IdleRead::Data(n) => {
                            // An idle socket has no request in flight. More
                            // than one max frame is not a response waiting
                            // for the owner. Shut the socket down instead of
                            // buffering the peer's write without a limit.
                            loop {
                                let scan = {
                                    let buffered = prefetch.lock();
                                    if scanned > buffered.len() {
                                        scanned = 0;
                                    }
                                    let too_long = buffered.len()
                                        > usize::try_from(MAX_FRAME).unwrap_or(usize::MAX);
                                    let scan = scan_frame_prefix(&buffered, scanned, 64 * 1024);
                                    (too_long, scan)
                                };
                                let (too_long, scan) = scan;
                                if too_long || matches!(scan, PrefixScan::Rejected) {
                                    closed.store(true, Ordering::SeqCst);
                                    if let Ok(mut io) = stream.try_lock() {
                                        drop(poll_fn(|cx| {
                                            Pin::new(&mut *io).poll_shutdown(cx)
                                        })
                                        .await);
                                        discard_peer_write(&mut *io).await;
                                    }
                                    arm.notify_one();
                                    return;
                                }
                                match scan {
                                    PrefixScan::Rejected => return,
                                    PrefixScan::Rest(at) => {
                                        scanned = at;
                                        break;
                                    }
                                    PrefixScan::Partial(at) => {
                                        scanned = at;
                                        // More complete frames are buffered.
                                        // Yield before walking the rest so a
                                        // caller beside this watch can run.
                                        tokio::task::yield_now().await;
                                    }
                                }
                            }
                            // A ready flood never leaves this read, so a caller
                            // beside the watch cannot run until the cap is reached.
                            since_yield = since_yield.saturating_add(n);
                            if since_yield >= 8 * 1024 {
                                since_yield = 0;
                                tokio::task::yield_now().await;
                            }
                        }
                        IdleRead::Shut => continue,
                        IdleRead::Eof => {
                            closed.store(true, Ordering::SeqCst);
                            if let Ok(mut io) = stream.try_lock() {
                                drop(poll_fn(|cx| Pin::new(&mut *io).poll_shutdown(cx)).await);
                            }
                            // Shut the write half down before waking a heartbeat
                            // that already has bytes buffered. That waiter is
                            // not sitting in a socket read, and it drops the
                            // connection when this returns.
                            arm.notify_one();
                            return;
                        }
                    }
                }
            }
        }
    })
    .abort_handle()
}

/// One TCP or TLS connection to a Kafka broker.
pub struct BrokerConn {
    stream: Arc<tokio::sync::Mutex<ConnIo>>,
    /// Bytes an idle watch read so the owner can finish the frame.
    prefetch: Arc<parking_lot::Mutex<BytesMut>>,
    /// Stops the idle watch when this connection is dropped.
    idle_watch: tokio::task::AbortHandle,
    /// Wakes the idle watch after a request releases the stream.
    idle_arm: Arc<tokio::sync::Notify>,
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
    /// This crate picks 3–13 from ApiVersions.
    pub(crate) produce_version: i16,
    /// Fetch version negotiated on data sockets (`-1` unset).
    /// This crate picks 4–18 from ApiVersions (KL05-12).
    pub(crate) fetch_version: i16,
    /// OffsetCommit version negotiated on coordinator sockets (`0` unset).
    /// Classic consumer groups pick 2–10 from ApiVersions. v10 uses topic
    /// UUIDs. Kafka 4.0 removed v0–v1, so `0` is not a spoken version.
    pub(crate) offset_commit_version: i16,
    /// OffsetFetch version negotiated on coordinator sockets (`0` unset).
    /// Classic consumer groups pick 1–10 from ApiVersions. v10 uses topic
    /// UUIDs. `0` is not a spoken version.
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
    /// FindCoordinator version negotiated on coordinator sockets (`-1` unset).
    /// This crate picks 0–6. `0` is a spoken version, so it cannot mean unset.
    pub(crate) find_coordinator_version: i16,
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

impl Drop for BrokerConn {
    fn drop(&mut self) {
        self.idle_watch.abort();
    }
}

impl BrokerConn {
    /// Move bytes the idle watch already read into the frame buffer.
    ///
    /// Callers hold the stream lock, which is the same order the watch uses
    /// when it appends those bytes.
    /// Drop a stream lock and wake the idle watch so it can register again.
    fn release_stream(&self, stream: tokio::sync::MutexGuard<'_, ConnIo>) {
        drop(stream);
        self.idle_arm.notify_one();
    }

    fn drain_prefetch(&mut self) {
        let mut extra = self.prefetch.lock();
        if extra.is_empty() {
            return;
        }
        let bytes = extra.split();
        self.read_buf.extend_from_slice(&bytes);
    }

    /// Connect without TLS.
    pub async fn connect(addr: &str, client_id: &str, connect_timeout: Duration) -> Result<Self> {
        Self::connect_tls(addr, client_id, connect_timeout, None).await
    }

    /// Cancel connects that lost the race and wait until they stop.
    ///
    /// Abort only schedules cancellation. On a current-thread runtime a
    /// connected socket stays open until this caller yields.
    async fn finish_aborted_connects<T: 'static>(mut set: tokio::task::JoinSet<T>) {
        set.abort_all();
        let mut extra = 0u32;
        while !set.is_empty() {
            tokio::select! {
                biased;
                joined = set.join_next() => {
                    if joined.is_none() {
                        break;
                    }
                }
                () = tokio::task::yield_now() => {
                    extra = extra.saturating_add(1);
                    if extra >= 8 {
                        break;
                    }
                }
            }
        }
        // Dropping a finished connect closes its socket. A few more turns let
        // the peer observe that close before this race returns.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    /// Try each bootstrap address until one connects.
    pub async fn connect_tls_any(
        addrs: &[String],
        client_id: &str,
        connect_timeout: Duration,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let addrs = parse_and_validate_addresses(addrs)?;
        if addrs.len() <= 1 {
            let Some(addr) = addrs.first() else {
                return Err(Error::protocol("all bootstrap servers failed"));
            };
            return Self::connect_tls(addr, client_id, connect_timeout, tls).await;
        }
        const PARALLELISM: usize = 8;
        let mut waiting = addrs.into_iter();
        let mut inflight = tokio::task::JoinSet::new();
        let mut last = Error::protocol("all bootstrap servers failed");
        loop {
            while inflight.len() < PARALLELISM {
                let Some(addr) = waiting.next() else {
                    break;
                };
                let client_id = client_id.to_string();
                let tls = tls.cloned();
                let _ = inflight.spawn(async move {
                    BrokerConn::connect_tls(&addr, &client_id, connect_timeout, tls.as_ref()).await
                });
            }
            if inflight.is_empty() {
                break;
            }
            match inflight.join_next().await {
                Some(Ok(Ok(conn))) => {
                    Self::finish_aborted_connects(inflight).await;
                    return Ok(conn);
                }
                Some(Ok(Err(error))) => last = error,
                Some(Err(error)) => {
                    last = Error::protocol(format!("bootstrap handshake: {error}"));
                }
                None => break,
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
            // Bound the handshake like the TCP connect: a peer that accepts
            // TCP but never handshakes must surface Timeout, never hang the
            // caller or fall back to plaintext (KL06-05).
            //
            // The timeout drops the accepted socket while this call is still
            // in the same poll. Yield so the peer observes that close before
            // Timeout is returned.
            let handshake = timeout(connect_timeout, wrap_tls(tcp, addr, tls)).await;
            let hs = match handshake {
                Ok(result) => result,
                Err(_) => Err(Error::Timeout),
            };
            match hs {
                Ok(hs) => ConnIo::Tls(Box::new(hs)),
                Err(error) => {
                    for _ in 0..8 {
                        tokio::task::yield_now().await;
                    }
                    return Err(error);
                }
            }
        } else {
            ConnIo::Tcp(tcp)
        };
        let stream = Arc::new(tokio::sync::Mutex::new(stream));
        let prefetch = Arc::new(parking_lot::Mutex::new(BytesMut::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let idle_arm = Arc::new(tokio::sync::Notify::new());
        let idle_watch = spawn_idle_watch(
            Arc::clone(&stream),
            Arc::clone(&prefetch),
            Arc::clone(&closed),
            Arc::clone(&idle_arm),
        );
        Ok(Self {
            stream,
            prefetch,
            idle_watch,
            idle_arm,
            read_buf: BytesMut::with_capacity(8 * 1024),
            write_buf: BytesMut::with_capacity(16 * 1024),
            next_correlation: 1,
            sasl_correlation: 0,
            client_id: client_id.to_string(),
            addr: addr.to_string(),
            last_io: Instant::now(),
            closed,
            stats: None,
            produce_version: -1,
            fetch_version: -1,
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
            find_coordinator_version: -1,
            session_lifetime_ms: None,
            authenticated_at: None,
            sasl_session_expires_at: None,
            reauth_at: None,
            in_flight: 0,
        })
    }

    /// A connection whose reads return `body` in ready `chunk`s.
    ///
    /// The idle watch is not started. Each chunk pauses, so a read loop that
    /// stays inside one poll can be told apart from one that yields.
    #[cfg(test)]
    fn script_conn(body: Vec<u8>, chunk: usize, pause: Duration) -> Self {
        let stream = Arc::new(tokio::sync::Mutex::new(ConnIo::Script(ScriptIo {
            body,
            pos: 0,
            chunk,
            pause,
        })));
        let prefetch = Arc::new(parking_lot::Mutex::new(BytesMut::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let idle_arm = Arc::new(tokio::sync::Notify::new());
        let parked = tokio::spawn(std::future::pending::<()>());
        let idle_watch = parked.abort_handle();
        parked.abort();
        Self {
            stream,
            prefetch,
            idle_watch,
            idle_arm,
            read_buf: BytesMut::with_capacity(8 * 1024),
            write_buf: BytesMut::with_capacity(16 * 1024),
            next_correlation: 1,
            sasl_correlation: 0,
            client_id: "script".to_string(),
            addr: "script".to_string(),
            last_io: Instant::now(),
            closed,
            stats: None,
            produce_version: -1,
            fetch_version: -1,
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
            find_coordinator_version: -1,
            session_lifetime_ms: None,
            authenticated_at: None,
            sasl_session_expires_at: None,
            reauth_at: None,
            in_flight: 0,
        }
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

    /// Negotiated Fetch version on this broker connection (`-1` unset).
    #[must_use]
    pub fn fetch_version(&self) -> i16 {
        self.fetch_version
    }

    /// Set negotiated Fetch version on this broker connection.
    pub fn set_fetch_version(&mut self, version: i16) {
        self.fetch_version = version;
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

    /// Whether a response byte is already buffered for the next read.
    ///
    /// [`Self::inbound_ready`] returns immediately in that case. A heartbeat
    /// that still wants the later close has to wait on [`Self::wait_until_closed`].
    pub(crate) fn has_queued_response(&self) -> bool {
        !self.read_buf.is_empty() || !self.prefetch.lock().is_empty()
    }

    /// Mark this connection as permanently closed/failed.
    pub fn close(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.in_flight = 0;
        // The idle watch is parked on the socket. Wake it so the flag
        // becomes a FIN instead of waiting for the broker to write.
        self.idle_arm.notify_one();
    }

    /// Wait until the idle watch has marked this connection closed.
    ///
    /// A caller that already has response bytes buffered cannot sit in
    /// [`Self::inbound_ready`] — that returns immediately — so it waits here
    /// for the peer to finish closing.
    pub(crate) async fn wait_until_closed(&self) {
        loop {
            if self.is_closed() {
                return;
            }
            self.idle_arm.notified().await;
        }
    }

    /// Wait until the peer closes, then shut this socket down.
    ///
    /// A buffered response does not finish the wait. Polling
    /// [`Self::inbound_ready`] again would return immediately and, on a
    /// current-thread runtime, spin without ever yielding. The later close
    /// would then leave the peer blocked in its read.
    pub(crate) async fn peer_left(&mut self) -> Result<()> {
        if !self.has_queued_response() {
            match self.inbound_ready().await {
                Err(error) => {
                    if !self.is_closed() {
                        self.close();
                    }
                    self.release_if_closed().await;
                    return Err(error);
                }
                Ok(()) if self.is_closed() => {
                    self.release_if_closed().await;
                    return Err(Error::Closed);
                }
                Ok(()) => {}
            }
        }
        let rejected = prefix_rejected(self.read_buf.as_ref()).await || {
            let mut scanned = 0usize;
            loop {
                let scan = {
                    let buffered = self.prefetch.lock();
                    if scanned > buffered.len() {
                        scanned = 0;
                    }
                    scan_frame_prefix(&buffered, scanned, 64 * 1024)
                };
                match scan {
                    PrefixScan::Rejected => break true,
                    PrefixScan::Rest(_) => break false,
                    PrefixScan::Partial(at) => {
                        scanned = at;
                        tokio::task::yield_now().await;
                    }
                }
            }
        };
        if rejected {
            if !self.is_closed() {
                self.close();
            }
            self.release_if_closed().await;
            return Err(Error::protocol("invalid frame size"));
        }
        self.wait_until_closed().await;
        if !self.is_closed() {
            self.close();
        }
        self.release_if_closed().await;
        Err(Error::Closed)
    }

    /// Send FIN for a connection that is already marked closed.
    ///
    /// [`Self::close`] only sets the flag. The broker stays in its read until
    /// this shutdown or the socket is dropped.
    pub(crate) async fn release_if_closed(&mut self) {
        if !self.is_closed() {
            return;
        }
        let mut rearm = Rearm::arm(Arc::clone(&self.idle_arm));
        let stream = Arc::clone(&self.stream);
        let mut stream = stream.lock().await;
        drop(stream.shutdown().await);
        self.release_stream(stream);
        rearm.disarm();
        // Shutdown only schedules the FIN. Yield so the peer observes it
        // before the caller continues.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
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
        let mut guard = CloseOnDrop::new(self.closed.clone(), Arc::clone(&self.idle_arm));
        let mut rearm = Rearm::arm(Arc::clone(&self.idle_arm));
        let stream = Arc::clone(&self.stream);
        let mut stream = stream.lock().await;
        self.drain_prefetch();
        let wrote = deadline
            .run_io(write_all_pump(&mut *stream, &mut self.read_buf, bytes))
            .await;
        self.release_stream(stream);
        rearm.disarm();
        if let Err(error) = wrote {
            if !self.is_closed() {
                self.close();
            }
            // The idle watch is not running while this call still holds the
            // caller. Shut the socket down here so the broker is gone before
            // the timeout returns.
            self.release_if_closed().await;
            return Err(error);
        }
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

    async fn shutdown_if_prefix_rejected(&mut self) -> Result<()> {
        if !prefix_rejected(self.read_buf.as_ref()).await {
            return Ok(());
        }
        if !self.is_closed() {
            self.close();
        }
        self.release_if_closed().await;
        Err(Error::protocol("invalid frame size"))
    }

    /// Wait until at least one response byte is buffered.
    ///
    /// Bytes already queued on the socket are taken straight into the frame
    /// buffer, up through one complete frame. Cancellation leaves the socket
    /// open. The following framed read consumes what this call stored.
    pub(crate) async fn inbound_ready(&mut self) -> Result<()> {
        if inbound_satisfied(&self.read_buf) || !self.read_buf.is_empty() {
            self.shutdown_if_prefix_rejected().await?;
            return Ok(());
        }
        let mut rearm = Rearm::arm(Arc::clone(&self.idle_arm));
        let stream = Arc::clone(&self.stream);
        let mut stream = stream.lock().await;
        // The idle watch may have stored the response and then observed the
        // broker's FIN. Those bytes are still this read's data.
        self.drain_prefetch();
        if inbound_satisfied(&self.read_buf) || !self.read_buf.is_empty() {
            self.release_stream(stream);
            rearm.disarm();
            self.shutdown_if_prefix_rejected().await?;
            return Ok(());
        }
        if self.is_closed() {
            self.release_stream(stream);
            rearm.disarm();
            return Err(Error::Closed);
        }
        let result = poll_fn(|cx| {
            if self.is_closed() {
                return Poll::Ready(Err(Error::Closed));
            }
            loop {
                if inbound_satisfied(&self.read_buf) {
                    return Poll::Ready(Ok(()));
                }
                let (budget, frame_end) = stalled_write_read(&self.read_buf);
                if let Some(end) = frame_end {
                    reserve_frame(&mut self.read_buf, end);
                } else {
                    self.read_buf.reserve(budget);
                }
                let polled = {
                    // A known frame stays reserved. One read still stops after a
                    // chunk so a deadline runs before the rest is copied.
                    let step = budget.min(STALLED_WRITE_READ_CHUNK);
                    let mut limited = (&mut self.read_buf).limit(step);
                    let mut read = std::pin::pin!(stream.read_buf(&mut limited));
                    read.as_mut().poll(cx)
                };
                match polled {
                    Poll::Pending => {
                        return if self.read_buf.is_empty() {
                            Poll::Pending
                        } else {
                            Poll::Ready(Ok(()))
                        };
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(Error::from(error))),
                    Poll::Ready(Ok(0)) => {
                        // Keep any bytes already read. The peer is still gone,
                        // so the caller must be able to shut the socket down
                        // instead of treating the short buffer as a live wait.
                        self.close();
                        if self.read_buf.is_empty() {
                            return Poll::Ready(Err(Error::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "broker closed connection",
                            ))));
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Ok(_)) => {
                        // A peer that already has the frame queued stays Ready.
                        // Yield so a deadline beside this read can stop it
                        // before the rest of the frame is copied.
                        if !inbound_satisfied(&self.read_buf) {
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                }
            }
        })
        .await;
        self.release_stream(stream);
        rearm.disarm();
        self.shutdown_if_prefix_rejected().await?;
        if self.is_closed() {
            self.release_if_closed().await;
        }
        result
    }

    #[cfg(test)]
    fn buffered_len(&self) -> usize {
        self.read_buf.len()
    }

    async fn read_frame_deadline_internal(
        &mut self,
        deadline: Deadline,
        close_on_drop: bool,
    ) -> Result<Bytes> {
        let mut guard = if close_on_drop {
            Some(CloseOnDrop::new(
                self.closed.clone(),
                Arc::clone(&self.idle_arm),
            ))
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
                    self.release_if_closed().await;
                    return Err(Error::protocol(format!("invalid frame size {size}")));
                }
                let total = 4 + crate::protocol::buf::usize_from_i32(size)?;
                if self.read_buf.len() >= total {
                    let mut frame = self.read_buf.split_to(total);
                    drop(frame.split_to(4));
                    restore_read_capacity(&mut self.read_buf, total);
                    if let Some(ref mut g) = guard {
                        g.complete();
                    }
                    return Ok(frame.freeze());
                }
                reserve_frame(&mut self.read_buf, total);
            }
            let buffered = self.read_buf.len();
            let mut rearm = Rearm::arm(Arc::clone(&self.idle_arm));
            let stream = Arc::clone(&self.stream);
            let mut stream = stream.lock().await;
            self.drain_prefetch();
            if self.read_buf.len() > buffered {
                self.release_stream(stream);
                rearm.disarm();
                continue;
            }
            if self.is_closed() && self.read_buf.is_empty() {
                self.release_stream(stream);
                rearm.disarm();
                return Err(Error::Closed);
            }
            // A known frame is reserved in full. Each read still stops after
            // one chunk so this deadline is polled before the rest is copied.
            let n = match deadline
                .run_io(async {
                    let mut limited = (&mut self.read_buf).limit(STALLED_WRITE_READ_CHUNK);
                    stream.read_buf(&mut limited).await
                })
                .await
            {
                Ok(n) => n,
                Err(e) => {
                    self.release_stream(stream);
                    rearm.disarm();
                    self.close();
                    self.release_if_closed().await;
                    return Err(e);
                }
            };
            self.release_stream(stream);
            rearm.disarm();
            if n == 0 {
                self.close();
                self.release_if_closed().await;
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
            Some(CloseOnDrop::new(
                self.closed.clone(),
                Arc::clone(&self.idle_arm),
            ))
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
            self.release_if_closed().await;
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
        // `split` would leave only the spare tail and drop the grown buffer.
        // Take it for the write, then clear and put the same allocation back.
        let mut payload = std::mem::take(&mut self.write_buf);
        let result = self.write_all_deadline(&payload, deadline).await;
        payload.clear();
        self.write_buf = payload;
        result?;
        Ok(())
    }

    #[cfg(test)]
    fn write_capacity(&self) -> usize {
        self.write_buf.capacity()
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
        let mut guard = CloseOnDrop::new(self.closed.clone(), Arc::clone(&self.idle_arm));
        let correlation = self
            .send_deadline(api_key, api_version, encode_body, deadline)
            .await?;
        let result = self
            .read_response_deadline(api_key, api_version, correlation, deadline)
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
        let mut guard = CloseOnDrop::new(self.closed.clone(), Arc::clone(&self.idle_arm));
        let correlation = self.next_sasl_correlation();
        self.write_request_deadline(api_key, api_version, correlation, encode_body, deadline)
            .await?;
        let result = self
            .read_response_deadline(api_key, api_version, correlation, deadline)
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

async fn retire_pipeline_conn(conn: &mut BrokerConn) {
    if !conn.is_closed() {
        conn.close();
    }
    // Callers are told after this returns. The broker has to be gone by then,
    // not merely marked closed for the idle watch.
    conn.release_if_closed().await;
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
                retire_pipeline_conn(&mut conn).await;
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
                    retire_pipeline_conn(&mut conn).await;
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
                } else if !conn.is_closed() {
                    // Nothing is in flight. A broker that stops writing must
                    // not stay blocked in its read until the next request.
                    // A queued response is not that close. The next request
                    // still uses this socket, and the command channel has to
                    // stay reachable while the byte sits in the buffer.
                    let _left = conn.peer_left().await;
                    Err(Error::Closed)
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
                        retire_pipeline_conn(&mut conn).await;
                        state = PipelineState::Closed;
                        shared_state.store(state.to_u8(), Ordering::SeqCst);
                        fail_all_requests(&mut in_flight, &mut queued, &mut quiesce_waiters, &e);
                    }
                }
            }
            cmd_opt = cmd_rx.recv() => {
                let Some(cmd) = cmd_opt else {
                    retire_pipeline_conn(&mut conn).await;
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
                            retire_pipeline_conn(&mut conn).await;
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
                                        retire_pipeline_conn(&mut conn).await;
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
                                        retire_pipeline_conn(&mut conn).await;
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
                            retire_pipeline_conn(&mut conn).await;
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
                                retire_pipeline_conn(&mut conn).await;
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
                        retire_pipeline_conn(&mut conn).await;
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
    task: Arc<tokio::task::JoinHandle<()>>,
}

impl Drop for BrokerPipeline {
    fn drop(&mut self) {
        // The loop can be inside a socket write, where it does not watch the
        // command channel. Dropping the last handle aborts that write.
        // Dropping a `JoinHandle` alone would detach it.
        if Arc::strong_count(&self.task) == 1 {
            self.task.abort();
        }
    }
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
        let task = Arc::new(tokio::spawn(run_pipeline_loop(
            conn,
            max_in_flight.max(1),
            cmd_rx,
            shared_state.clone(),
        )));
        Self {
            cmd_tx,
            closed,
            shared_state,
            task,
        }
    }

    /// Test hook: the connection loop task.
    #[doc(hidden)]
    #[must_use]
    pub fn test_task(&self) -> Arc<tokio::task::JoinHandle<()>> {
        Arc::clone(&self.task)
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
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
    use tokio::net::TcpListener;

    #[tokio::test(flavor = "current_thread")]
    async fn a_run_of_empty_frames_does_not_hold_the_caller() {
        let body = vec![0u8; 1024];
        let stream = Arc::new(tokio::sync::Mutex::new(ConnIo::Script(ScriptIo {
            body,
            pos: 0,
            chunk: usize::MAX,
            pause: Duration::ZERO,
        })));
        let mut queued = BytesMut::new();
        queued.resize(16 * 1024 * 1024, 0);
        let prefetch = Arc::new(parking_lot::Mutex::new(queued));
        let closed = Arc::new(AtomicBool::new(false));
        let arm = Arc::new(tokio::sync::Notify::new());
        let watch = spawn_idle_watch(
            Arc::clone(&stream),
            Arc::clone(&prefetch),
            Arc::clone(&closed),
            Arc::clone(&arm),
        );
        let started = std::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let elapsed = started.elapsed();
        watch.abort();
        assert!(
            elapsed < Duration::from_millis(80),
            "scanning empty frames held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_idle_flood_does_not_hold_the_caller() {
        let body = vec![5u8; 64 * 1024];
        let stream = Arc::new(tokio::sync::Mutex::new(ConnIo::Script(ScriptIo {
            body,
            pos: 0,
            chunk: usize::MAX,
            pause: Duration::from_millis(2),
        })));
        let prefetch = Arc::new(parking_lot::Mutex::new(BytesMut::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let arm = Arc::new(tokio::sync::Notify::new());
        let watch = spawn_idle_watch(
            Arc::clone(&stream),
            Arc::clone(&prefetch),
            Arc::clone(&closed),
            Arc::clone(&arm),
        );
        let started = std::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let elapsed = started.elapsed();
        watch.abort();
        assert!(
            elapsed < Duration::from_millis(80),
            "a ready idle flood held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_frame_stops_for_a_deadline() {
        let body_len = 32 * 1024;
        let mut body = Vec::with_capacity(4 + body_len);
        body.extend_from_slice(&(i32::try_from(body_len).unwrap()).to_be_bytes());
        body.resize(4 + body_len, 1);
        let mut conn = BrokerConn::script_conn(body, 1024, Duration::from_millis(4));
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_millis(40), conn.inbound_ready()).await;
        let elapsed = started.elapsed();
        assert!(
            result.is_err(),
            "a ready frame finished in {elapsed:?} without the deadline"
        );
        assert!(
            elapsed < Duration::from_millis(90),
            "a ready frame held the deadline for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_greedy_ready_frame_stops_for_a_deadline() {
        let body_len = 64 * 1024;
        let mut body = Vec::with_capacity(4 + body_len);
        body.extend_from_slice(&i32::try_from(body_len).unwrap().to_be_bytes());
        body.resize(4 + body_len, 1);
        let mut conn = BrokerConn::script_conn(body, usize::MAX, Duration::from_millis(2));
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_millis(40), conn.inbound_ready()).await;
        let elapsed = started.elapsed();
        assert!(
            result.is_err(),
            "a greedy ready frame finished in {elapsed:?} without the deadline"
        );
        assert!(
            elapsed < Duration::from_millis(80),
            "a greedy ready frame held the deadline for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_response_stops_for_a_deadline() {
        let body_len = 64 * 1024;
        let mut body = Vec::with_capacity(4 + body_len);
        body.extend_from_slice(&i32::try_from(body_len).unwrap().to_be_bytes());
        body.resize(4 + body_len, 1);
        let mut conn = BrokerConn::script_conn(body, usize::MAX, Duration::from_millis(2));
        let started = std::time::Instant::now();
        let result = conn
            .read_frame_deadline_internal(Deadline::from_timeout(Duration::from_millis(40)), false)
            .await;
        let elapsed = started.elapsed();
        assert!(
            matches!(result, Err(Error::Timeout)),
            "a ready response finished in {elapsed:?} without the deadline"
        );
        assert!(
            elapsed < Duration::from_millis(80),
            "a ready response held the deadline for {elapsed:?}"
        );
    }

    struct PendingIo;

    impl AsyncRead for PendingIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for PendingIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_run_of_empty_frames_does_not_hold_inbound_ready() {
        let mut conn = BrokerConn::script_conn(Vec::new(), 1, Duration::ZERO);
        conn.read_buf.resize(16 * 1024 * 1024, 0);
        let started = std::time::Instant::now();
        tokio::select! {
            _ = conn.inbound_ready() => {}
            _ = tokio::time::sleep(Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(80),
            "scanning empty frames held the read for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_run_of_empty_frames_does_not_hold_the_write() {
        let mut buffered = BytesMut::new();
        buffered.resize(16 * 1024 * 1024, 0);
        let mut socket = PendingIo;
        let started = std::time::Instant::now();
        tokio::select! {
            _ = write_all_pump(&mut socket, &mut buffered, &[1u8; 64]) => {}
            _ = tokio::time::sleep(Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(80),
            "scanning empty frames held the write for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_write_stops_for_a_deadline() {
        let mut conn = BrokerConn::script_conn(Vec::new(), usize::MAX, Duration::from_millis(2));
        let payload = vec![1u8; 64 * 1024];
        let started = std::time::Instant::now();
        let result = conn
            .write_all_deadline(&payload, Deadline::from_timeout(Duration::from_millis(40)))
            .await;
        let elapsed = started.elapsed();
        assert!(
            matches!(result, Err(Error::Timeout)),
            "a ready write finished in {elapsed:?} without the deadline"
        );
        assert!(
            elapsed < Duration::from_millis(80),
            "a ready write held the deadline for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connect_any_finishes_a_losing_connect() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let closed = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let mut addrs = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..2 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(listener.local_addr().unwrap().to_string());
            let flag = Arc::clone(&closed);
            let saw = Arc::clone(&accepted);
            peers.push(tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                let _accepted = saw.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 8];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _closed = flag.fetch_add(1, Ordering::SeqCst);
            }));
        }
        let conn = BrokerConn::connect_tls_any(&addrs, "race", Duration::from_secs(2), None)
            .await
            .unwrap();
        assert_eq!(
            (
                accepted.load(Ordering::SeqCst),
                closed.load(Ordering::SeqCst)
            ),
            (2, 1),
            "the losing connect was still open"
        );
        drop(conn);
        for peer in peers {
            peer.abort();
        }
    }

    /// A TLS handshake timeout closes the accepted socket before the call returns.
    #[tokio::test(flavor = "current_thread")]
    async fn tls_handshake_timeout_closes_the_socket() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let open_flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 64];
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let tls = TlsConfig::default().server_name("localhost");
        let result =
            BrokerConn::connect_tls(&addr, "tls-stop", Duration::from_millis(200), Some(&tls))
                .await;
        assert!(
            matches!(result, Err(Error::Timeout)),
            "a silent peer should time out the handshake"
        );
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "tls handshake left the accepted socket blocked in a read"
        );
        peer.abort();
    }

    #[test]
    fn a_full_frame_split_reserves_the_next_read() {
        let mut buf = BytesMut::new();
        assert_eq!(buf.capacity(), 0);
        restore_read_capacity(&mut buf, 32);
        assert!(buf.capacity() >= 32);
        let kept = buf.capacity();
        restore_read_capacity(&mut buf, 32);
        assert_eq!(buf.capacity(), kept);

        let mut filled = BytesMut::with_capacity(8);
        filled.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let taken = filled.len();
        let frame = filled.split_to(taken);
        let spare = filled.capacity();
        restore_read_capacity(&mut filled, taken);
        if spare == 0 {
            assert!(filled.capacity() >= taken);
        } else {
            assert_eq!(filled.capacity(), spare);
        }
        assert!(filled.is_empty());
        drop(frame);
    }

    #[test]
    fn reserve_frame_grows_once_to_known_size() {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&[0, 0, 0, 16]);
        reserve_frame(&mut buf, 4 + 1_000_000);
        assert!(buf.capacity() >= 1_000_004);
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn stalled_write_read_fills_only_the_open_frame() {
        let mut buf = BytesMut::new();
        let (budget, end) = stalled_write_read(&buf);
        assert_eq!(budget, STALLED_WRITE_READ_CHUNK);
        assert!(end.is_none());

        buf.extend_from_slice(&[0, 1]);
        let (budget, end) = stalled_write_read(&buf);
        assert_eq!(budget, STALLED_WRITE_READ_CHUNK);
        assert!(end.is_none());

        let mut framed = BytesMut::new();
        framed.extend_from_slice(&3i32.to_be_bytes());
        framed.extend_from_slice(b"one");
        let (budget, end) = stalled_write_read(&framed);
        assert_eq!(budget, STALLED_WRITE_READ_CHUNK);
        assert!(end.is_none());

        let at = framed.len();
        framed.extend_from_slice(&1i32.to_be_bytes());
        let (budget, end) = stalled_write_read(&framed);
        assert_eq!(budget, 1);
        assert_eq!(end, Some(at + 5));

        let mut illegal = BytesMut::new();
        illegal.extend_from_slice(&i32::MAX.to_be_bytes());
        let (budget, end) = stalled_write_read(&illegal);
        assert_eq!(budget, STALLED_WRITE_READ_CHUNK);
        assert!(end.is_none());
    }

    struct Script {
        inbound: Vec<u8>,
        pos: usize,
        chunk: usize,
        reads: u32,
    }

    impl AsyncRead for Script {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.reads = this.reads.saturating_add(1);
            if this.pos >= this.inbound.len() {
                return Poll::Ready(Ok(()));
            }
            let end = this.pos.saturating_add(this.chunk).min(this.inbound.len());
            if let Some(slice) = this.inbound.get(this.pos..end) {
                buf.put_slice(slice);
                this.pos = end;
            }
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for Script {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let drained = self.reads > 0 && self.pos >= self.inbound.len();
            if !drained {
                return Poll::Pending;
            }
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct SlowAccept;

    impl AsyncRead for SlowAccept {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for SlowAccept {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_millis(3) {}
            Poll::Ready(Ok(1))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_deadline_stops_a_ready_partial_write() {
        let payload = vec![1u8; 40];
        let mut read_buf = BytesMut::new();
        let started = std::time::Instant::now();
        let result = Deadline::from_timeout(Duration::from_millis(30))
            .run_io(write_all_pump(&mut SlowAccept, &mut read_buf, &payload))
            .await;
        let elapsed = started.elapsed();
        let deadline = matches!(result, Err(Error::Timeout));
        assert!(
            deadline && elapsed < Duration::from_millis(80),
            "a ready partial write held the deadline: {elapsed:?} deadline {deadline}"
        );
    }

    /// Writes stay pending. Reads fill every spare byte of the next frame.
    struct GreedyStall {
        body: Vec<u8>,
        pos: usize,
    }

    impl AsyncRead for GreedyStall {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.pos >= this.body.len() || buf.remaining() == 0 {
                return Poll::Pending;
            }
            let n = (this.body.len() - this.pos).min(buf.remaining());
            let end = this.pos + n;
            let Some(chunk) = this.body.get(this.pos..end) else {
                return Poll::Pending;
            };
            buf.put_slice(chunk);
            this.pos = end;
            let units = u32::try_from(n.div_ceil(1024)).unwrap_or(u32::MAX);
            std::thread::sleep(Duration::from_millis(2).saturating_mul(units));
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for GreedyStall {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_deadline_stops_a_greedy_stalled_read() {
        let body_len = 64 * 1024;
        let mut body = Vec::with_capacity(4 + body_len);
        body.extend_from_slice(&i32::try_from(body_len).unwrap().to_be_bytes());
        body.resize(4 + body_len, 2);
        let mut socket = GreedyStall { body, pos: 0 };
        let mut read_buf = BytesMut::new();
        let started = std::time::Instant::now();
        let result = Deadline::from_timeout(Duration::from_millis(40))
            .run_io(write_all_pump(&mut socket, &mut read_buf, b"request-bytes"))
            .await;
        let elapsed = started.elapsed();
        let deadline = matches!(result, Err(Error::Timeout));
        assert!(
            deadline && elapsed < Duration::from_millis(80),
            "a greedy stalled read held the write deadline: {elapsed:?} deadline {deadline}"
        );
    }

    struct IllegalAfterFrame {
        sent: bool,
    }

    impl AsyncRead for IllegalAfterFrame {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.sent {
                return Poll::Pending;
            }
            let dst = buf.initialize_unfilled();
            if dst.len() < 8 {
                return Poll::Pending;
            }
            dst[..4].fill(0);
            dst[4..8].copy_from_slice(&i32::MAX.to_be_bytes());
            buf.advance(8);
            this.sent = true;
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for IllegalAfterFrame {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_illegal_length_after_a_frame_does_not_wait_out_the_write() {
        let payload = vec![1u8; 32];
        let mut read_buf = BytesMut::new();
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            Deadline::from_timeout(Duration::from_secs(5)).run_io(write_all_pump(
                &mut IllegalAfterFrame { sent: false },
                &mut read_buf,
                &payload,
            )),
        )
        .await;
        let elapsed = started.elapsed();
        assert!(
            matches!(result, Ok(Err(_))),
            "an illegal length after a frame waited out the write: {elapsed:?}"
        );
    }

    /// The illegal length is already buffered. The peer accepts no more
    /// writes and sends nothing else.
    struct StuckWrite;

    impl AsyncRead for StuckWrite {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for StuckWrite {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_illegal_length_already_buffered_does_not_wait_out_the_write() {
        let payload = vec![1u8; 32];
        let mut read_buf = BytesMut::new();
        read_buf.extend_from_slice(&0i32.to_be_bytes());
        read_buf.extend_from_slice(&i32::MAX.to_be_bytes());
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            Deadline::from_timeout(Duration::from_secs(5)).run_io(write_all_pump(
                &mut StuckWrite,
                &mut read_buf,
                &payload,
            )),
        )
        .await;
        let elapsed = started.elapsed();
        assert!(
            matches!(result, Ok(Err(_))),
            "an illegal length already buffered waited out the write: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn stalled_write_keeps_a_large_response_without_copying_through_a_stack_buffer() {
        let body = vec![7u8; 20_000];
        let mut inbound = Vec::new();
        inbound.extend_from_slice(&i32::try_from(body.len()).unwrap().to_be_bytes());
        inbound.extend_from_slice(&body);
        let mut stream = Script {
            inbound: inbound.clone(),
            pos: 0,
            chunk: 100,
            reads: 0,
        };
        let mut read_buf = BytesMut::new();
        write_all_pump(&mut stream, &mut read_buf, b"request")
            .await
            .unwrap();
        assert_eq!(&read_buf[..], &inbound[..]);
        assert!(read_buf.capacity() >= inbound.len());
    }

    #[tokio::test]
    async fn repeated_requests_reuse_the_write_buffer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for _ in 0..2 {
                let mut prefix = [0u8; 4];
                let _ = socket.read_exact(&mut prefix).await.unwrap();
                let size = usize::try_from(i32::from_be_bytes(prefix)).unwrap();
                let mut body = vec![0u8; size];
                let _ = socket.read_exact(&mut body).await.unwrap();
                let correlation = i32::from_be_bytes(body[4..8].try_into().unwrap());
                let mut response = Vec::new();
                response.extend_from_slice(&4i32.to_be_bytes());
                response.extend_from_slice(&correlation.to_be_bytes());
                socket.write_all(&response).await.unwrap();
            }
        });
        let mut conn = BrokerConn::connect(&addr, "reuse", Duration::from_secs(2))
            .await
            .unwrap();
        let body = vec![0u8; 20_000];
        let first = body.clone();
        let _ = conn
            .roundtrip(
                crate::protocol::api_keys::API_VERSIONS,
                0,
                move |buf| {
                    buf.extend_from_slice(&first);
                    Ok(())
                },
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        let after_first = conn.write_capacity();
        let second = body.clone();
        let _ = conn
            .roundtrip(
                crate::protocol::api_keys::API_VERSIONS,
                0,
                move |buf| {
                    buf.extend_from_slice(&second);
                    Ok(())
                },
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert!(after_first >= body.len());
        assert_eq!(conn.write_capacity(), after_first);
        drop(server);
    }

    #[tokio::test]
    async fn stalled_write_eof_before_any_byte_is_an_error() {
        let mut stream = Script {
            inbound: Vec::new(),
            pos: 0,
            chunk: 8,
            reads: 0,
        };
        let mut read_buf = BytesMut::new();
        let error = write_all_pump(&mut stream, &mut read_buf, b"request")
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(read_buf.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inbound_ready_closes_when_the_peer_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });
        let mut conn = BrokerConn::connect(&addr, "eof", Duration::from_secs(2))
            .await
            .unwrap();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let result = conn.inbound_ready().await;
        assert!(
            result.is_err(),
            "a closed peer should not look like a queued response"
        );
        assert!(
            conn.is_closed(),
            "a closed peer should close the connection"
        );
        server.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inbound_ready_shutdowns_when_the_peer_stops_writing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let mut conn = BrokerConn::connect(&addr, "eof-fin", Duration::from_secs(2))
            .await
            .unwrap();
        let result = conn.inbound_ready().await;
        assert!(result.is_err(), "peer write shutdown should surface");
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after its write shutdown: {saw:?}"
        );
    }

    #[tokio::test]
    async fn inbound_ready_keeps_a_queued_ack_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let body = vec![9u8; 4096];
        let mut frame = Vec::new();
        let payload_len = 4 + body.len();
        frame.extend_from_slice(&i32::try_from(payload_len).unwrap().to_be_bytes());
        frame.extend_from_slice(&7i32.to_be_bytes());
        frame.extend_from_slice(&body);
        let queued = frame.clone();
        let (wrote, wrote_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.write_all(&queued).await.unwrap();
            let _ = wrote.send(());
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let mut conn = BrokerConn::connect(&addr, "ack", Duration::from_secs(2))
            .await
            .unwrap();
        wrote_rx.await.unwrap();
        conn.inbound_ready().await.unwrap();
        assert!(
            conn.buffered_len() >= frame.len(),
            "queued ack was not taken into the frame buffer: {} of {}",
            conn.buffered_len(),
            frame.len()
        );
        let got = conn
            .read_response(
                crate::protocol::api_keys::API_VERSIONS,
                0,
                7,
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(&got[..], &body[..]);
        drop(server);
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

    #[tokio::test(flavor = "current_thread")]
    async fn read_timeout_shuts_the_broker_down() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut conn = BrokerConn::connect(&addr, "timeout", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let result = conn
            .roundtrip(
                crate::protocol::api_keys::API_VERSIONS,
                0,
                |_buf| Ok(()),
                Duration::from_millis(200),
            )
            .await;
        assert!(
            matches!(result, Err(Error::Timeout)),
            "the read finished instead of timing out"
        );
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a read timeout left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_timeout_shuts_the_broker_down() {
        use std::io::Read;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut peer = socket.into_std().unwrap();
            peer.set_nonblocking(false).unwrap();
            let _thread = std::thread::spawn(move || {
                let _ = ready_tx.send(());
                let _ = gate_rx.recv();
                let mut buf = [0u8; 8];
                loop {
                    match peer.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _ = done_tx.send(());
            });
        });
        let mut conn = BrokerConn::connect(&addr, "write-timeout", Duration::from_secs(2))
            .await
            .unwrap();
        let started = std::time::Instant::now();
        loop {
            match ready_rx.try_recv() {
                Ok(()) => break,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    if started.elapsed() >= Duration::from_secs(1) {
                        panic!("broker thread did not start");
                    }
                    tokio::task::yield_now().await;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("broker thread dropped the start signal");
                }
            }
        }
        let payload = vec![1u8; 8 * 1024 * 1024];
        let result = conn
            .write_all_timeout(&payload, Duration::from_millis(200))
            .await;
        assert!(
            matches!(result, Err(Error::Timeout)),
            "the write finished instead of timing out"
        );
        let _ = gate_tx.send(());
        assert!(
            done_rx.recv_timeout(Duration::from_millis(300)).is_ok(),
            "a write timeout left the broker blocked in a read"
        );
        accepted.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_expired_pipeline_lets_the_broker_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut conn = BrokerConn::connect(&addr, "pipeline", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        conn.record_sasl_session_lifetime_at(1, Instant::now() - Duration::from_secs(60));
        let pipeline = BrokerPipeline::new(conn);
        let result = pipeline
            .request(
                crate::protocol::api_keys::API_VERSIONS,
                0,
                |_buf| Ok(()),
                Duration::from_secs(2),
            )
            .await;
        assert!(result.is_err(), "an expired session still sent the request");
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "an expired pipeline left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_watch_keeps_bytes_that_arrive_before_the_peer_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.write_all(&[0xAB]).await.unwrap();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let mut conn =
            BrokerConn::connect(&addr.to_string(), "idle-prefetch", Duration::from_secs(2))
                .await
                .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let ready = conn.inbound_ready().await;
        assert!(
            ready.is_ok(),
            "a response that arrived before the close was dropped: {ready:?} closed {}",
            conn.is_closed()
        );
        assert!(conn.buffered_len() > 0, "the response byte was not kept");
        let saw = accepted.await.unwrap();
        assert!(matches!(saw, Ok(Ok(0))), "peer stayed in the read: {saw:?}");
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_illegal_length_does_not_leave_the_peer_blocked() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.write_all(&i32::MAX.to_be_bytes()).await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let mut conn = BrokerConn::connect(&addr, "bad-len", Duration::from_secs(2))
            .await
            .unwrap();
        let queued = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if conn.has_queued_response() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(queued.is_ok(), "the illegal length never became visible");
        let noticed = tokio::time::timeout(Duration::from_millis(200), conn.peer_left()).await;
        assert!(noticed.is_ok(), "an illegal length left the wait parked");
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "an illegal length left the peer blocked in its read: {saw:?}"
        );
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inbound_ready_shuts_an_illegal_length_down() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            go_rx.await.unwrap();
            writer.write_all(&i32::MAX.to_be_bytes()).await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let mut conn = BrokerConn::connect(&addr, "ready-bad-len", Duration::from_secs(2))
            .await
            .unwrap();
        let result = {
            let reading = conn.inbound_ready();
            tokio::pin!(reading);
            tokio::time::sleep(Duration::from_millis(30)).await;
            go_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_millis(200), &mut reading).await
        };
        assert!(result.is_ok(), "the illegal length never reached the read");
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "an illegal length read left the peer blocked: {saw:?}"
        );
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_shuts_the_socket_while_the_idle_watch_is_parked() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = socket.into_split();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let mut conn = BrokerConn::connect(&addr.to_string(), "flag-close", Duration::from_secs(2))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        conn.close();
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after the connection was marked closed: {saw:?}"
        );
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_byte_does_not_spin_through_a_later_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.write_all(&[0x01]).await.unwrap();
            gate_rx.await.unwrap();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let mut conn = BrokerConn::connect(&addr.to_string(), "spin-close", Duration::from_secs(2))
            .await
            .unwrap();
        let queued = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if conn.has_queued_response() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(queued.is_ok(), "the queued byte never became visible");
        gate_tx.send(()).unwrap();
        let noticed = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                tokio::select! {
                    biased;
                    _ = conn.peer_left(), if !conn.is_closed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(50)) => break,
                }
            }
        })
        .await;
        assert!(
            noticed.is_ok(),
            "a queued byte spun the wait so the later close was never observed"
        );
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after it closed a socket that already had a queued byte: {saw:?}"
        );
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_read_rearms_the_idle_watch() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            gate_rx.await.unwrap();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let mut conn =
            BrokerConn::connect(&addr.to_string(), "cancel-read", Duration::from_secs(2))
                .await
                .unwrap();
        // Let the idle watch register before this read replaces that registration.
        tokio::time::sleep(Duration::from_millis(50)).await;
        {
            let mut ready = std::pin::pin!(conn.inbound_ready());
            tokio::select! {
                result = &mut ready => panic!("the read finished before it was cancelled: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(30)) => {}
            }
        }
        gate_tx.send(()).unwrap();
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after a cancelled read stole the idle watch: {saw:?}"
        );
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_watch_sees_a_close_after_a_finished_read() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (byte_tx, byte_rx) = tokio::sync::oneshot::channel::<()>();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            byte_rx.await.unwrap();
            writer.write_all(&[0x01]).await.unwrap();
            gate_rx.await.unwrap();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let mut conn =
            BrokerConn::connect(&addr.to_string(), "idle-after-read", Duration::from_secs(2))
                .await
                .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        byte_tx.send(()).unwrap();
        conn.inbound_ready().await.unwrap();
        gate_tx.send(()).unwrap();
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after a close that followed a finished read: {saw:?}"
        );
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_watch_sees_a_close_that_arrives_after_the_first_poll() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            gate_rx.await.unwrap();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let conn = BrokerConn::connect(&addr.to_string(), "idle-late", Duration::from_secs(2))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        gate_tx.send(()).unwrap();
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after a late write shutdown: {saw:?}"
        );
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closing_an_idle_socket_lets_the_peer_finish_writing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            gate_rx.await.unwrap();
            let chunk = vec![1u8; 64 * 1024];
            let write = async {
                for _ in 0..64 {
                    writer.write_all(&chunk).await?;
                }
                Ok::<(), std::io::Error>(())
            };
            let wrote = tokio::time::timeout(Duration::from_millis(300), write).await;
            let saw =
                tokio::time::timeout(Duration::from_millis(300), reader.read(&mut [0u8; 8])).await;
            (wrote, saw)
        });
        let mut conn = BrokerConn::connect(
            &addr.to_string(),
            "idle-close-write",
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        conn.close();
        conn.release_if_closed().await;
        gate_tx.send(()).unwrap();
        let (wrote, saw) = accepted.await.unwrap();
        assert!(
            wrote.is_ok(),
            "closing an idle socket left the peer blocked in its write: {wrote:?}"
        );
        assert!(
            matches!(saw, Ok(Ok(0))),
            "closing an idle socket left the peer blocked in its read: {saw:?}"
        );
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_idle_flood_lets_the_peer_finish_writing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            let chunk = vec![0xabu8; 64 * 1024];
            let write = async {
                for _ in 0..64 {
                    writer.write_all(&chunk).await?;
                }
                Ok::<(), std::io::Error>(())
            };
            let wrote = tokio::time::timeout(Duration::from_millis(300), write).await;
            let saw =
                tokio::time::timeout(Duration::from_millis(300), reader.read(&mut [0u8; 8])).await;
            (wrote, saw)
        });
        let conn = BrokerConn::connect(
            &addr.to_string(),
            "idle-flood-write",
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let (wrote, saw) = accepted.await.unwrap();
        assert!(
            wrote.is_ok(),
            "an idle flood left the peer blocked in its write: {wrote:?}"
        );
        assert!(
            matches!(saw, Ok(Ok(0))),
            "an idle flood left the peer blocked in its read: {saw:?}"
        );
        assert!(conn.is_closed(), "an idle flood left the connection open");
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_watch_stops_a_flood_of_unsolicited_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            let chunk = vec![0xabu8; 64 * 1024];
            let limit = usize::try_from(MAX_FRAME).unwrap_or(usize::MAX) + chunk.len();
            let mut sent = 0usize;
            while sent < limit {
                if writer.write_all(&chunk).await.is_err() {
                    break;
                }
                sent += chunk.len();
            }
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let conn = BrokerConn::connect(&addr.to_string(), "idle-flood", Duration::from_secs(2))
            .await
            .unwrap();
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "an idle flood left the peer blocked in its read: {saw:?}"
        );
        assert!(conn.is_closed(), "an idle flood left the connection open");
        drop(conn);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_byte_does_not_close_an_idle_pipeline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.write_all(&[0x01]).await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let conn = BrokerConn::connect(&addr.to_string(), "pipe-byte", Duration::from_secs(2))
            .await
            .unwrap();
        let pipeline = BrokerPipeline::new(conn);
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Err(_)),
            "a queued byte closed an idle pipeline: {saw:?}"
        );
        assert!(
            !pipeline.is_closed(),
            "a queued byte marked the pipeline closed"
        );
        drop(pipeline);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_pipeline_shuts_down_when_the_broker_stops_writing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let conn = BrokerConn::connect(&addr.to_string(), "pipeline-idle", Duration::from_secs(2))
            .await
            .unwrap();
        let pipeline = BrokerPipeline::new(conn);
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after its write shutdown: {saw:?}"
        );
        drop(pipeline);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_write_stops_a_flood_of_empty_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (read_tx, read_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            let chunk = vec![0u8; 64 * 1024];
            let _flood = tokio::spawn(async move {
                loop {
                    if writer.write_all(&chunk).await.is_err() {
                        break;
                    }
                }
            });
            let _started = read_rx.await;
            let mut buf = [0u8; 8 * 1024];
            tokio::time::timeout(Duration::from_millis(300), async {
                loop {
                    match reader.read(&mut buf).await {
                        Ok(0) | Err(_) => return Ok::<usize, std::io::Error>(0),
                        Ok(_) => continue,
                    }
                }
            })
            .await
        });
        let mut conn = BrokerConn::connect(&addr, "stall-flood", Duration::from_secs(2))
            .await
            .unwrap();
        // Larger than the largest TCP window, so the write stays blocked
        // while the peer sends and does not read.
        let payload = vec![1u8; 8 * 1024 * 1024];
        let started = std::time::Instant::now();
        let result = conn
            .write_all_deadline(&payload, Deadline::from_timeout(Duration::from_millis(200)))
            .await;
        assert!(
            result.is_err(),
            "a flood of empty frames let the stalled write finish"
        );
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "a flood of empty frames kept the stalled write running: {:?}",
            started.elapsed()
        );
        read_tx.send(()).unwrap();
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "a flood of empty frames left the peer blocked in its read: {saw:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_a_pipeline_stops_a_blocked_write() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hold = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
            drop(sock);
        });
        let conn = BrokerConn::connect(&addr.to_string(), "pipeline-drop", Duration::from_secs(2))
            .await
            .unwrap();
        let pipeline = BrokerPipeline::with_max_in_flight(conn, 1);
        let watch = pipeline.test_task().abort_handle();
        let sender = pipeline.clone();
        let request = tokio::spawn(async move {
            let body = vec![0u8; 8 * 1024 * 1024];
            let _result = sender
                .request(
                    18,
                    0,
                    move |buf| {
                        buf.extend_from_slice(&body);
                        Ok(())
                    },
                    Duration::from_secs(30),
                )
                .await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!watch.is_finished(), "the pipeline should still be writing");
        drop(pipeline);
        request.abort();
        let started = std::time::Instant::now();
        let mut turns = 0u32;
        while !watch.is_finished() && turns < 8 {
            turns = turns.saturating_add(1);
            tokio::task::yield_now().await;
        }
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "drop waited for the blocked write: {:?}",
            started.elapsed()
        );
        assert!(
            watch.is_finished(),
            "dropping the pipeline left its connection loop running"
        );
        hold.abort();
    }
}
