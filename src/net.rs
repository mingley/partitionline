//! TCP and TLS broker connections.

use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Once;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::error::{Error, Result};
use crate::protocol::header::{decode_response_header, encode_request_header_fields};

/// Max Kafka response frame (100 MiB). Larger is treated as a protocol error.
pub const MAX_FRAME: i32 = 100 * 1024 * 1024;

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
#[derive(Debug, Clone, Default)]
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

fn host_of(addr: &str) -> &str {
    if let Some(rest) = addr.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(addr);
    }
    addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr)
}

fn certs_from_pem(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut &*pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::protocol(format!("tls cert pem: {e}")))
}

fn key_from_pem(pem: &[u8]) -> Result<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut &*pem)
        .map_err(|e| Error::protocol(format!("tls key pem: {e}")))?
        .ok_or_else(|| Error::protocol("tls: no private key in pem"))
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
    client_id: String,
    addr: String,
    last_io: Instant,
    /// Admin-only. Producer and consumer sockets leave this `None` so
    /// [`Self::send`] plus [`Self::read_response`] is not double-counted.
    stats: Option<Arc<crate::metrics::AdminTracker>>,
    /// OffsetCommit version negotiated on coordinator sockets (`0` unset).
    /// Classic consumer groups pick 7–9 from ApiVersions.
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
    /// Classic consumer groups pick 5–9 from ApiVersions.
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
        if addrs.is_empty() {
            return Err(Error::protocol("no bootstrap servers"));
        }
        let mut last = Error::protocol("all bootstrap servers failed");
        for addr in addrs {
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
        })
    }

    /// Kafka `client.id` on this connection.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Next request correlation id.
    pub fn next_correlation(&mut self) -> i32 {
        let c = self.next_correlation;
        self.next_correlation = self.next_correlation.wrapping_add(1);
        c
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
            return Err(Error::protocol(format!(
                "correlation mismatch: sent {correlation}, got {}",
                header.correlation_id
            )));
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
        Ok(correlation)
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
}
