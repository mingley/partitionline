use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Once;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::error::{Error, Result};
use crate::protocol::header::{decode_response_header, encode_request_header_fields};

pub const MAX_FRAME: i32 = 100 * 1024 * 1024;

/// rustls client settings. No OpenSSL.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// PEM CA bundle. If `None`, Mozilla webpki-roots are used.
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    /// SNI and certificate hostname. Defaults to the bootstrap host (no port).
    pub server_name: Option<String>,
}

fn ensure_crypto() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
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

async fn wrap_tls(tcp: TcpStream, addr: &str, tls: &TlsConfig) -> Result<TlsStream<TcpStream>> {
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

pub struct BrokerConn {
    stream: ConnIo,
    read_buf: BytesMut,
    write_buf: BytesMut,
    next_correlation: i32,
    client_id: String,
    addr: String,
}

impl BrokerConn {
    pub async fn connect(addr: &str, client_id: &str, connect_timeout: Duration) -> Result<Self> {
        Self::connect_tls(addr, client_id, connect_timeout, None).await
    }

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
        })
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn next_correlation(&mut self) -> i32 {
        let c = self.next_correlation;
        self.next_correlation = self.next_correlation.wrapping_add(1);
        c
    }

    pub async fn write_all_timeout(
        &mut self,
        bytes: &[u8],
        request_timeout: Duration,
    ) -> Result<()> {
        timeout(request_timeout, self.stream.write_all(bytes))
            .await
            .map_err(|_| Error::Timeout)??;
        Ok(())
    }

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
        Ok(cur)
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub async fn send(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut),
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
        );
        encode_body(&mut self.write_buf);
        let size = (self.write_buf.len() - 4) as i32;
        self.write_buf[0..4].copy_from_slice(&size.to_be_bytes());
        timeout(request_timeout, self.stream.write_all(&self.write_buf))
            .await
            .map_err(|_| Error::Timeout)??;
        Ok(correlation)
    }

    pub async fn roundtrip(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut),
        request_timeout: Duration,
    ) -> Result<Bytes> {
        let correlation = self
            .send(api_key, api_version, encode_body, request_timeout)
            .await?;
        self.read_response(api_key, api_version, correlation, request_timeout)
            .await
    }

    async fn read_frame(&mut self) -> Result<Bytes> {
        loop {
            if self.read_buf.len() >= 4 {
                let size = i32::from_be_bytes(self.read_buf[0..4].try_into().unwrap());
                if !(0..=MAX_FRAME).contains(&size) {
                    return Err(Error::protocol(format!("invalid frame size {size}")));
                }
                let total = 4 + size as usize;
                if self.read_buf.len() >= total {
                    let mut frame = self.read_buf.split_to(total);
                    let _ = frame.split_to(4);
                    return Ok(frame.freeze());
                }
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
