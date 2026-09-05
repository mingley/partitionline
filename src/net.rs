//! TCP and TLS broker connections.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
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
use tokio::time::timeout;
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
    /// Admin-only. Producer and consumer sockets leave this `None` so
    /// [`Self::send`] plus [`Self::read_response`] is not double-counted.
    stats: Option<Arc<crate::metrics::AdminTracker>>,
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
    /// Absolute instant when the broker SASL session should be treated as
    /// expired, from SaslAuthenticate `session_lifetime_ms` (v1+). `None`
    /// when the broker omitted a positive lifetime (v0 or `0`). Recorded for
    /// reconnect / mid-connection reauth (KL-06). Prefer live-socket
    /// `SaslAuthenticate` via `should_reconnect_after_reauth` before dropping.
    pub(crate) sasl_session_expires_at: Option<Instant>,
    /// Absolute instant when the last OIDC access token should be treated as
    /// expired, from IdP `expires_in`. `None` when the IdP omitted
    /// `expires_in`. Reconnect re-fetches; mid-connection reauth via SaslAuthenticate when possible.
    pub(crate) oidc_token_expires_at: Option<Instant>,
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
            stats: None,
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
            sasl_session_expires_at: None,
            oidc_token_expires_at: None,
        })
    }

    /// Record broker `session_lifetime_ms` from a successful SaslAuthenticate.
    ///
    /// `ms <= 0` clears the deadline (v0 responses and brokers that send `0`).
    pub(crate) fn record_sasl_session_lifetime(&mut self, session_lifetime_ms: i64) {
        self.sasl_session_expires_at = u64::try_from(session_lifetime_ms).ok().and_then(|ms| {
            if ms == 0 {
                None
            } else {
                Some(Instant::now() + Duration::from_millis(ms))
            }
        });
    }

    /// Record IdP access-token expiry from a successful OIDC fetch.
    pub(crate) fn record_oidc_token_expiry(&mut self, expires_at: Option<Instant>) {
        self.oidc_token_expires_at = expires_at;
    }

    /// Kafka `client.id` on this connection.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
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

    /// Write `bytes` or fail with [`Error::Timeout`].
    pub async fn write_all_timeout(
        &mut self,
        bytes: &[u8],
        request_timeout: Duration,
    ) -> Result<()> {
        timeout(
            request_timeout,
            write_all_pump(&mut self.stream, &mut self.read_buf, bytes),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        self.touch();
        Ok(())
    }

    /// Read one response frame and check the correlation id.
    pub async fn read_response(
        &mut self,
        api_key: i16,
        api_version: i16,
        correlation: i32,
        request_timeout: Duration,
    ) -> Result<Bytes> {
        let frame = timeout(request_timeout, self.read_frame())
            .await
            .map_err(|_| Error::Timeout)??;
        let mut cur = frame;
        let header = decode_response_header(&mut cur, api_key, api_version)?;
        if header.correlation_id != correlation {
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
        self.touch();
        Ok(cur)
    }

    /// Broker `host:port` used to open this connection.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Kafka `connections.max.idle.ms`. Zero never expires.
    #[must_use]
    pub(crate) fn idle_expired(&self, max_idle: Duration) -> bool {
        crate::config::connection_idle_expired(self.last_io.elapsed(), max_idle)
    }

    /// True when a recorded SASL session or OIDC token lifetime is within `skew`.
    ///
    /// Missing lifetimes (`None`) do **not** invent a reconnect deadline.
    /// Prefer [`crate::protocol::sasl::should_reconnect_after_reauth`] so
    /// mid-connection `SaslAuthenticate` (KIP-368) can refresh without
    /// dropping the socket; fall back to full reconnect on reauth failure.
    #[must_use]
    pub(crate) fn auth_lifetime_expired(&self, skew: Duration) -> bool {
        auth_lifetimes_need_refresh(
            self.sasl_session_expires_at,
            self.oidc_token_expires_at,
            skew,
        )
    }

    /// Idle budget exhausted (Kafka `connections.max.idle.ms`).
    ///
    /// Auth-lifetime refresh is handled by
    /// [`crate::protocol::sasl::should_reconnect_after_reauth`], which calls
    /// this for the idle check and prefers mid-connection `SaslAuthenticate`
    /// when only SASL/OIDC lifetime is near expiry.
    #[must_use]
    pub(crate) fn should_reconnect(&self, max_idle: Duration) -> bool {
        self.idle_expired(max_idle)
    }

    fn touch(&mut self) {
        self.last_io = Instant::now();
    }

    /// Count this socket's [`Self::roundtrip`] on an Admin tracker.
    pub(crate) fn set_stats(&mut self, stats: Arc<crate::metrics::AdminTracker>) {
        self.stats = Some(stats);
    }

    /// Encode and write one request. Returns the correlation id.
    pub async fn send(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        request_timeout: Duration,
    ) -> Result<i32> {
        let correlation = self.next_correlation();
        self.write_request(
            api_key,
            api_version,
            correlation,
            encode_body,
            request_timeout,
        )
        .await?;
        Ok(correlation)
    }

    async fn write_request(
        &mut self,
        api_key: i16,
        api_version: i16,
        correlation: i32,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        request_timeout: Duration,
    ) -> Result<()> {
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
        self.write_all_timeout(&payload, request_timeout).await?;
        Ok(())
    }

    /// Write a request and read its response.
    pub async fn roundtrip(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        request_timeout: Duration,
    ) -> Result<Bytes> {
        let started = Instant::now();
        let result = async {
            let correlation = self
                .send(api_key, api_version, encode_body, request_timeout)
                .await?;
            self.read_response(api_key, api_version, correlation, request_timeout)
                .await
        }
        .await;
        if let Some(stats) = &self.stats {
            stats.record(started.elapsed(), result.is_ok());
        }
        result
    }

    /// Write a SASL request and read its response.
    ///
    /// Java `SaslClientAuthenticator.nextRequestHeader`: correlation ids
    /// come from [`next_sasl_correlation_id`], not
    /// [`next_correlation_id`].
    pub(crate) async fn roundtrip_sasl(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        request_timeout: Duration,
    ) -> Result<Bytes> {
        let started = Instant::now();
        let result = async {
            let correlation = self.next_sasl_correlation();
            self.write_request(
                api_key,
                api_version,
                correlation,
                encode_body,
                request_timeout,
            )
            .await?;
            self.read_response(api_key, api_version, correlation, request_timeout)
                .await
        }
        .await;
        if let Some(stats) = &self.stats {
            stats.record(started.elapsed(), result.is_ok());
        }
        result
    }

    async fn read_frame(&mut self) -> Result<Bytes> {
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
                    return Err(Error::protocol(format!("invalid frame size {size}")));
                }
                let total = 4 + crate::protocol::buf::usize_from_i32(size)?;
                if self.read_buf.len() >= total {
                    let mut frame = self.read_buf.split_to(total);
                    drop(frame.split_to(4));
                    return Ok(frame.freeze());
                }
                reserve_frame(&mut self.read_buf, total);
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "broker closed connection",
                )));
            }
        }
    }
}

/// Install rustls `ring` as the process crypto provider. Idempotent.
pub fn install_crypto_provider() {
    ensure_crypto();
}

/// True when either recorded auth deadline is within `skew` of now.
///
/// `None` deadlines never force refresh (no invented lifetime). Used by
/// [`BrokerConn::auth_lifetime_expired`] before recycling a socket.
#[must_use]
pub(crate) fn auth_lifetimes_need_refresh(
    sasl_session_expires_at: Option<Instant>,
    oidc_token_expires_at: Option<Instant>,
    skew: Duration,
) -> bool {
    use crate::protocol::oidc::token_needs_refresh;
    token_needs_refresh(sasl_session_expires_at, skew)
        || token_needs_refresh(oidc_token_expires_at, skew)
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
    fn auth_lifetimes_need_refresh_respects_skew_and_none() {
        let skew = Duration::from_secs(60);
        assert!(!auth_lifetimes_need_refresh(None, None, skew));
        let far = Some(Instant::now() + Duration::from_secs(600));
        assert!(!auth_lifetimes_need_refresh(far, None, skew));
        assert!(!auth_lifetimes_need_refresh(None, far, skew));
        let near = Some(Instant::now() + Duration::from_secs(30));
        assert!(auth_lifetimes_need_refresh(near, None, skew));
        assert!(auth_lifetimes_need_refresh(None, near, skew));
        let past = Some(Instant::now() - Duration::from_secs(1));
        assert!(auth_lifetimes_need_refresh(past, far, skew));
    }
}
