//! RFC 6749 client_credentials token fetch for SASL OAUTHBEARER.
//!
//! HTTP/1.1 POST over `tokio::net::TcpStream`. `https://` uses rustls.

use std::fmt;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::error::{Error, Result};

const MAX_RESPONSE: usize = 64 * 1024;
const FORM_BODY: &str = "grant_type=client_credentials";

/// OIDC-style client credentials used to POST for an access token.
///
/// [`Debug`] redacts [`Self::client_secret`] (KL-06).
#[derive(Clone)]
pub struct OidcConfig {
    /// Token endpoint, `http://` or `https://host:port/path`.
    pub token_url: String,
    /// OAuth client id (`client_id`).
    pub client_id: String,
    /// OAuth client secret (`client_secret`).
    pub client_secret: String,
    /// TLS for `https://` token URLs. `None` uses webpki-roots.
    pub tls: Option<crate::net::TlsConfig>,
}

impl fmt::Debug for OidcConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OidcConfig")
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .field("tls", &self.tls)
            .finish()
    }
}

impl OidcConfig {
    /// Token URL plus client id and secret. `https://` uses Mozilla roots
    /// by default; call [`OidcConfig::tls()`] for a custom CA or mTLS.
    pub fn new(
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        Self {
            token_url: token_url.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            tls: None,
        }
    }

    /// rustls config for `https://` token URLs (custom CA or mTLS).
    #[must_use]
    pub fn tls(mut self, tls: crate::net::TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}

struct HttpUrl {
    https: bool,
    host: String,
    port: u16,
    path: String,
}

/// POST `grant_type=client_credentials` and return `access_token`.
pub async fn fetch_client_credentials_token(
    cfg: &OidcConfig,
    request_timeout: Duration,
) -> Result<String> {
    let url = parse_http_url(&cfg.token_url)?;
    let deadline = Instant::now() + request_timeout;
    let addr = connect_addr(&url.host, url.port);
    let left = time_left(deadline)?;
    let mut stream = match timeout(left, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Err(Error::Timeout),
    };
    let default_port = if url.https { 443 } else { 80 };
    let host_header = host_header(&url.host, url.port, default_port);
    let auth = basic_auth(&cfg.client_id, &cfg.client_secret);
    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Authorization: Basic {auth}\r\n\
         Content-Type: application/x-www-form-urlencoded\r\n\
         Accept: application/json\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n\
         {FORM_BODY}",
        path = url.path,
        host = host_header,
        auth = auth,
        len = FORM_BODY.len(),
    );
    let (status, body) = if url.https {
        let tls = cfg.tls.clone().unwrap_or_default();
        let left = time_left(deadline)?;
        let mut tls_stream = match timeout(left, crate::net::wrap_tls(stream, &addr, &tls)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(Error::Timeout),
        };
        token_http_roundtrip(&mut tls_stream, req.as_bytes(), deadline).await?
    } else {
        token_http_roundtrip(&mut stream, req.as_bytes(), deadline).await?
    };
    let text = String::from_utf8_lossy(&body);
    if status != 200 {
        return Err(Error::protocol(format!(
            "oidc token endpoint HTTP {status}: {text}"
        )));
    }
    access_token_from_json(&text)
}

fn time_left(deadline: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or(Error::Timeout)
}

fn connect_addr(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn host_header(host: &str, port: u16, default_port: u16) -> String {
    if host.contains(':') {
        if port == default_port {
            format!("[{host}]")
        } else {
            format!("[{host}]:{port}")
        }
    } else if port == default_port {
        host.to_string()
    } else {
        format!("{host}:{port}")
    }
}

fn parse_http_url(url: &str) -> Result<HttpUrl> {
    let (https, rest, default_port) = if let Some(rest) = url.strip_prefix("https://") {
        (true, rest, 443)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (false, rest, 80)
    } else {
        return Err(Error::protocol(
            "oidc token_url must start with http:// or https://",
        ));
    };
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    if authority.is_empty() {
        return Err(Error::protocol("oidc token_url missing host"));
    }
    let (host, port) = parse_authority(authority, default_port)?;
    if host.is_empty() {
        return Err(Error::protocol("oidc token_url missing host"));
    }
    Ok(HttpUrl {
        https,
        host,
        port,
        path,
    })
}

fn parse_authority(authority: &str, default_port: u16) -> Result<(String, u16)> {
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .ok_or_else(|| Error::protocol("oidc token_url IPv6 host"))?;
        let port = match after.strip_prefix(':') {
            Some(p) if !p.is_empty() => parse_port(p)?,
            Some(_) => return Err(Error::protocol("oidc token_url empty port")),
            None if after.is_empty() => default_port,
            None => return Err(Error::protocol("oidc token_url IPv6 host")),
        };
        return Ok((host.to_string(), port));
    }
    if let Some((host, port)) = authority.rsplit_once(':') {
        if host.is_empty() {
            return Err(Error::protocol("oidc token_url missing host"));
        }
        return Ok((host.to_string(), parse_port(port)?));
    }
    Ok((authority.to_string(), default_port))
}

fn parse_port(s: &str) -> Result<u16> {
    s.parse()
        .map_err(|_| Error::protocol("oidc token_url port"))
}

fn form_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(char::from(*b));
            }
            other => {
                out.push('%');
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                let hi = usize::from(other >> 4);
                let lo = usize::from(other & 0x0f);
                if let (Some(&h), Some(&l)) = (HEX.get(hi), HEX.get(lo)) {
                    out.push(char::from(h));
                    out.push(char::from(l));
                }
            }
        }
    }
    out
}

fn basic_auth(client_id: &str, client_secret: &str) -> String {
    let raw = format!("{}:{}", form_encode(client_id), form_encode(client_secret));
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw.as_bytes())
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn parse_status(head: &[u8]) -> Result<u16> {
    let text = std::str::from_utf8(head).map_err(|_| Error::protocol("oidc headers not utf8"))?;
    let line = text
        .split("\r\n")
        .next()
        .ok_or_else(|| Error::protocol("oidc empty status"))?;
    let code = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| Error::protocol("oidc status"))?;
    code.parse()
        .map_err(|_| Error::protocol("oidc status code"))
}

fn parse_content_length(head: &[u8]) -> Result<Option<usize>> {
    let text = std::str::from_utf8(head).map_err(|_| Error::protocol("oidc headers not utf8"))?;
    for line in text.split("\r\n") {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case("content-length") {
            let n = v
                .trim()
                .parse::<usize>()
                .map_err(|_| Error::protocol("oidc content-length"))?;
            return Ok(Some(n));
        }
    }
    Ok(None)
}

async fn token_http_roundtrip<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    stream: &mut S,
    req: &[u8],
    deadline: Instant,
) -> Result<(u16, Vec<u8>)> {
    let left = time_left(deadline)?;
    match timeout(left, stream.write_all(req)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Err(Error::Timeout),
    }
    read_http_response(stream, deadline).await
}

async fn read_http_response<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    deadline: Instant,
) -> Result<(u16, Vec<u8>)> {
    let mut buf = Vec::new();
    loop {
        if buf.len() > MAX_RESPONSE {
            return Err(Error::protocol("oidc token response too large"));
        }
        if let Some(end) = find_header_end(&buf) {
            if let Some(n) = parse_content_length(buf.get(..end).unwrap_or(&[]))? {
                if buf.len().saturating_sub(end) >= n {
                    break;
                }
            }
        }
        let left = time_left(deadline)?;
        let mut tmp = [0u8; 2048];
        let n = match timeout(left, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Err(Error::Timeout),
        };
        let chunk = tmp
            .get(..n)
            .ok_or_else(|| Error::protocol("oidc token read"))?;
        buf.extend_from_slice(chunk);
    }
    let end =
        find_header_end(&buf).ok_or_else(|| Error::protocol("oidc token response headers"))?;
    let head = buf
        .get(..end)
        .ok_or_else(|| Error::protocol("oidc truncated headers"))?;
    let status = parse_status(head)?;
    let body = match parse_content_length(head)? {
        Some(n) => buf.get(end..end.saturating_add(n)).unwrap_or(&[]).to_vec(),
        None => buf.get(end..).unwrap_or(&[]).to_vec(),
    };
    Ok((status, body))
}

pub(crate) fn access_token_from_json(json: &str) -> Result<String> {
    let rest = json
        .split_once("\"access_token\"")
        .map(|(_, r)| r)
        .ok_or_else(|| Error::protocol("oidc response missing access_token"))?;
    let rest = rest.trim_start();
    let rest = rest
        .strip_prefix(':')
        .ok_or_else(|| Error::protocol("oidc access_token"))?;
    let rest = rest.trim_start();
    let rest = rest
        .strip_prefix('"')
        .ok_or_else(|| Error::protocol("oidc access_token not a string"))?;
    let val = rest
        .split('"')
        .next()
        .ok_or_else(|| Error::protocol("oidc truncated access_token"))?;
    if val.is_empty() {
        return Err(Error::protocol("oidc empty access_token"));
    }
    Ok(val.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    #[test]
    fn parse_ipv4_and_ipv6_urls() {
        let u = parse_http_url("http://127.0.0.1:8080/oauth/token").unwrap();
        assert_eq!(u.host, "127.0.0.1");
        assert_eq!(u.port, 8080);
        assert_eq!(u.path, "/oauth/token");
        let u = parse_http_url("http://localhost/token").unwrap();
        assert_eq!(u.host, "localhost");
        assert_eq!(u.port, 80);
        assert_eq!(u.path, "/token");
        let u = parse_http_url("http://[::1]:9/x").unwrap();
        assert_eq!(u.host, "::1");
        assert_eq!(u.port, 9);
        let u = parse_http_url("https://example.com/token").unwrap();
        assert!(u.https);
        assert_eq!(u.host, "example.com");
        assert_eq!(u.port, 443);
    }

    #[test]
    fn access_token_json_space_after_colon() {
        assert_eq!(
            access_token_from_json("{\"access_token\": \"abc\",\"token_type\":\"Bearer\"}")
                .unwrap(),
            "abc"
        );
        assert_eq!(
            access_token_from_json("{\"token_type\":\"Bearer\",\"access_token\":\"xyz\"}").unwrap(),
            "xyz"
        );
    }

    #[tokio::test]
    async fn fetch_token_from_http_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(buf.get(..n).unwrap_or(&[]));
            assert!(req.contains("grant_type=client_credentials"));
            assert!(req.contains("Authorization: Basic "));
            let body = "{\"access_token\":\"tok-1\",\"token_type\":\"Bearer\"}";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        }));
        let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", "csecret");
        let token = fetch_client_credentials_token(&cfg, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(token, "tok-1");
    }

    #[tokio::test]
    async fn fetch_token_rejects_http_401() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let _n = sock.read(&mut buf).await.unwrap();
            let body = "{\"error\":\"invalid_client\"}";
            let resp = format!(
                "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        }));
        let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", "bad");
        let err = fetch_client_credentials_token(&cfg, Duration::from_secs(5))
            .await
            .unwrap_err();
        match err {
            Error::Protocol(m) => assert!(m.contains("401"), "{m}"),
            other => panic!("expected protocol 401, got {other}"),
        }
    }
}
