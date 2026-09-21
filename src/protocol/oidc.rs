//! RFC 6749 client_credentials token fetch for SASL OAUTHBEARER.
//!
//! HTTP/1.1 POST over `tokio::net::TcpStream`. `https://` uses rustls.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
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

/// Parsed OIDC token response metadata.
///
/// Redacts [`Self::access_token`] in [`fmt::Debug`] to prevent credential leakage.
#[derive(Clone, PartialEq, Eq)]
pub struct OidcTokenResponse {
    /// The access token string issued by the authorization server.
    pub access_token: String,
    /// The type of the token issued (e.g. "Bearer").
    pub token_type: String,
    /// The lifetime in seconds of the access token, if provided.
    pub expires_in: Option<u64>,
}

impl fmt::Debug for OidcTokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OidcTokenResponse")
            .field("access_token", &"<redacted>")
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .finish()
    }
}

impl OidcTokenResponse {
    /// Create a new parsed token response.
    #[must_use]
    pub fn new(
        access_token: impl Into<String>,
        token_type: impl Into<String>,
        expires_in: Option<u64>,
    ) -> Self {
        Self {
            access_token: access_token.into(),
            token_type: token_type.into(),
            expires_in,
        }
    }
}

struct HttpUrl {
    https: bool,
    host: String,
    port: u16,
    path: String,
}

/// Max attempts for a single [`fetch_client_credentials_token`] call (1 + retries).
const OIDC_FETCH_ATTEMPTS: u32 = 3;
/// Initial backoff between transient IdP failures (doubles each retry).
const OIDC_RETRY_BACKOFF_START: Duration = Duration::from_millis(20);

/// POST `grant_type=client_credentials` and return `access_token`.
///
/// Transient IdP failures (HTTP 5xx, I/O, timeout) are retried up to three
/// attempts within `request_timeout`. HTTP 4xx fails immediately
/// (no credential hammering). This is bounded reconnect-time recovery, **not**
/// mid-connection token refresh / `expires_in` handling (KL-06 still open).
///
/// Preserves the public one-shot acquisition path. See
/// [`fetch_client_credentials_token_response`] for full token metadata.
pub async fn fetch_client_credentials_token(
    cfg: &OidcConfig,
    request_timeout: Duration,
) -> Result<String> {
    fetch_client_credentials_token_response(cfg, request_timeout)
        .await
        .map(|resp| resp.access_token)
}

/// POST `grant_type=client_credentials` and return parsed [`OidcTokenResponse`].
///
/// Transient IdP failures (HTTP 5xx, I/O, timeout) are retried up to three
/// attempts within `request_timeout`. HTTP 4xx fails immediately
/// (no credential hammering).
pub async fn fetch_client_credentials_token_response(
    cfg: &OidcConfig,
    request_timeout: Duration,
) -> Result<OidcTokenResponse> {
    let deadline = Instant::now() + request_timeout;
    let mut backoff = OIDC_RETRY_BACKOFF_START;
    let mut last_err = None;
    for attempt in 1..=OIDC_FETCH_ATTEMPTS {
        match fetch_client_credentials_token_response_once(cfg, deadline).await {
            Ok(resp) => return Ok(resp),
            Err(err) if attempt < OIDC_FETCH_ATTEMPTS && is_transient_oidc_error(&err) => {
                last_err = Some(err);
                let sleep_for = match time_left(deadline) {
                    Ok(left) => backoff.min(left),
                    Err(_) => Duration::ZERO,
                };
                if sleep_for.is_zero() {
                    break;
                }
                tokio::time::sleep(sleep_for).await;
                backoff = backoff.saturating_mul(2);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_err.unwrap_or(Error::Timeout))
}

fn is_transient_oidc_error(err: &Error) -> bool {
    match err {
        Error::Timeout | Error::Io(_) => true,
        Error::Protocol(m) => oidc_http_status(m).is_some_and(|s| (500..600).contains(&s)),
        _ => false,
    }
}

fn oidc_http_status(msg: &str) -> Option<u16> {
    const PREFIX: &str = "oidc token endpoint HTTP ";
    msg.strip_prefix(PREFIX)?.parse().ok()
}

async fn fetch_client_credentials_token_response_once(
    cfg: &OidcConfig,
    deadline: Instant,
) -> Result<OidcTokenResponse> {
    let url = parse_http_url(&cfg.token_url)?;
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
    if status != 200 {
        // Do not embed IdP response bodies in Error — they can echo client_secret,
        // tokens, or other credential-adjacent material (KL-06 error hygiene).
        return Err(Error::protocol(format!(
            "oidc token endpoint HTTP {status}"
        )));
    }
    let text =
        std::str::from_utf8(&body).map_err(|_| Error::protocol("oidc token response not utf8"))?;
    parse_oidc_token_response(text)
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
    let mut cl = None;
    for line in text.split("\r\n") {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case("content-length") {
            let n = v
                .trim()
                .parse::<usize>()
                .map_err(|_| Error::protocol("oidc content-length"))?;
            if let Some(prev) = cl {
                if prev != n {
                    return Err(Error::protocol("oidc conflicting content-length"));
                }
            } else {
                cl = Some(n);
            }
        }
    }
    Ok(cl)
}

fn is_chunked_transfer_encoding(head: &[u8]) -> Result<bool> {
    let text = std::str::from_utf8(head).map_err(|_| Error::protocol("oidc headers not utf8"))?;
    for line in text.split("\r\n") {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case("transfer-encoding") {
            for part in v.split(',') {
                if part.trim().eq_ignore_ascii_case("chunked") {
                    return Ok(true);
                }
            }
            return Err(Error::protocol("oidc unsupported transfer-encoding"));
        }
    }
    Ok(false)
}

fn try_decode_chunked_body(raw: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut cursor = 0usize;
    let mut decoded = Vec::new();
    loop {
        let remaining = raw.get(cursor..).unwrap_or(&[]);
        let Some(pos) = remaining.windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let line_bytes = remaining.get(..pos).unwrap_or(&[]);
        let line_str = std::str::from_utf8(line_bytes)
            .map_err(|_| Error::protocol("oidc malformed chunked body"))?;
        let size_str = match line_str.split_once(';') {
            Some((s, _)) => s.trim(),
            None => line_str.trim(),
        };
        if size_str.is_empty() || !size_str.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::protocol("oidc malformed chunked body"));
        }
        let chunk_size = usize::from_str_radix(size_str, 16)
            .map_err(|_| Error::protocol("oidc malformed chunked body"))?;

        cursor = cursor.saturating_add(pos).saturating_add(2);

        if chunk_size == 0 {
            let trailer_bytes = raw.get(cursor..).unwrap_or(&[]);
            if trailer_bytes.starts_with(b"\r\n") {
                return Ok(Some(decoded));
            }
            if let Some(_trailer_end) = trailer_bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                return Ok(Some(decoded));
            }
            return Ok(None);
        }

        if chunk_size > MAX_RESPONSE || decoded.len().saturating_add(chunk_size) > MAX_RESPONSE {
            return Err(Error::protocol("oidc token response too large"));
        }

        let chunk_end = cursor.saturating_add(chunk_size);
        let crlf_end = chunk_end.saturating_add(2);
        if raw.len() < crlf_end {
            return Ok(None);
        }

        let chunk_data = raw
            .get(cursor..chunk_end)
            .ok_or_else(|| Error::protocol("oidc token read"))?;
        let chunk_crlf = raw
            .get(chunk_end..crlf_end)
            .ok_or_else(|| Error::protocol("oidc token read"))?;
        if chunk_crlf != b"\r\n" {
            return Err(Error::protocol("oidc malformed chunked body"));
        }

        decoded.extend_from_slice(chunk_data);
        cursor = crlf_end;
    }
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
    let end = loop {
        if let Some(end) = find_header_end(&buf) {
            break end;
        }
        if buf.len() > MAX_RESPONSE {
            return Err(Error::protocol("oidc token response too large"));
        }
        let left = time_left(deadline)?;
        let mut tmp = [0u8; 2048];
        let n = match timeout(left, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => return Err(Error::protocol("oidc truncated headers")),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Err(Error::Timeout),
        };
        let chunk = tmp
            .get(..n)
            .ok_or_else(|| Error::protocol("oidc token read"))?;
        buf.extend_from_slice(chunk);
    };

    let head = buf
        .get(..end)
        .ok_or_else(|| Error::protocol("oidc truncated headers"))?;
    let status = parse_status(head)?;

    if is_chunked_transfer_encoding(head)? {
        loop {
            if buf.len() > MAX_RESPONSE {
                return Err(Error::protocol("oidc token response too large"));
            }
            let raw_chunked = buf.get(end..).unwrap_or(&[]);
            if let Some(body) = try_decode_chunked_body(raw_chunked)? {
                if body.len() > MAX_RESPONSE {
                    return Err(Error::protocol("oidc token response too large"));
                }
                return Ok((status, body));
            }
            let left = time_left(deadline)?;
            let mut tmp = [0u8; 2048];
            let n = match timeout(left, stream.read(&mut tmp)).await {
                Ok(Ok(0)) => return Err(Error::protocol("oidc truncated chunked body")),
                Ok(Ok(n)) => n,
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => return Err(Error::Timeout),
            };
            let chunk = tmp
                .get(..n)
                .ok_or_else(|| Error::protocol("oidc token read"))?;
            buf.extend_from_slice(chunk);
        }
    }

    if let Some(content_len) = parse_content_length(head)? {
        if content_len > MAX_RESPONSE {
            return Err(Error::protocol("oidc token response too large"));
        }
        while buf.len().saturating_sub(end) < content_len {
            if buf.len() > MAX_RESPONSE {
                return Err(Error::protocol("oidc token response too large"));
            }
            let left = time_left(deadline)?;
            let mut tmp = [0u8; 2048];
            let n = match timeout(left, stream.read(&mut tmp)).await {
                Ok(Ok(0)) => return Err(Error::protocol("oidc truncated response body")),
                Ok(Ok(n)) => n,
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => return Err(Error::Timeout),
            };
            let chunk = tmp
                .get(..n)
                .ok_or_else(|| Error::protocol("oidc token read"))?;
            buf.extend_from_slice(chunk);
        }
        let body_end = end.saturating_add(content_len);
        let body = buf
            .get(end..body_end)
            .ok_or_else(|| Error::protocol("oidc truncated response body"))?
            .to_vec();
        return Ok((status, body));
    }

    // Neither chunked nor Content-Length: read until EOF (e.g. Connection: close)
    loop {
        if buf.len() > MAX_RESPONSE {
            return Err(Error::protocol("oidc token response too large"));
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
    if buf.len() > MAX_RESPONSE {
        return Err(Error::protocol("oidc token response too large"));
    }
    let body = buf.get(end..).unwrap_or(&[]).to_vec();
    Ok((status, body))
}

/// Parse an OIDC token endpoint JSON response into [`OidcTokenResponse`].
///
/// Validates required fields (`access_token`, `token_type`), parses optional `expires_in`,
/// handles JSON escape sequences, and enforces size bounds without exposing response bodies
/// in errors.
pub fn parse_oidc_token_response(json: &str) -> Result<OidcTokenResponse> {
    if json.len() > MAX_RESPONSE {
        return Err(Error::protocol("oidc token response too large"));
    }
    let mut parser = JsonParser::new(json);
    let val = parser.parse()?;
    let entries = match val {
        JsonValue::Object(entries) => entries,
        _ => return Err(Error::protocol("oidc response not a json object")),
    };

    let mut access_token = None;
    let mut token_type = None;
    let mut expires_in = None;
    let mut seen_keys = std::collections::HashSet::new();

    for (key, val) in entries {
        if !seen_keys.insert(key.clone()) {
            return Err(Error::protocol("oidc duplicate json key"));
        }
        match key.as_str() {
            "access_token" => match val {
                JsonValue::String(s) => {
                    if s.is_empty() {
                        return Err(Error::protocol("oidc empty access_token"));
                    }
                    access_token = Some(s);
                }
                _ => return Err(Error::protocol("oidc access_token not a string")),
            },
            "token_type" => match val {
                JsonValue::String(s) => {
                    if s.is_empty() {
                        return Err(Error::protocol("oidc empty token_type"));
                    }
                    if !s.eq_ignore_ascii_case("Bearer") {
                        return Err(Error::protocol("oidc unsupported token_type"));
                    }
                    token_type = Some(s);
                }
                _ => return Err(Error::protocol("oidc token_type not a string")),
            },
            "expires_in" => match val {
                JsonValue::Number(n) => {
                    let secs = n
                        .as_u64
                        .ok_or_else(|| Error::protocol("oidc invalid expires_in"))?;
                    expires_in = Some(secs);
                }
                _ => return Err(Error::protocol("oidc invalid expires_in")),
            },
            _ => {
                // Ignore unrecognized extension fields
            }
        }
    }

    let access_token =
        access_token.ok_or_else(|| Error::protocol("oidc response missing access_token"))?;
    let token_type =
        token_type.ok_or_else(|| Error::protocol("oidc response missing token_type"))?;

    Ok(OidcTokenResponse {
        access_token,
        token_type,
        expires_in,
    })
}

/// Extract access token string from JSON response.
///
/// Preserves the public helper interface.
pub fn access_token_from_json(json: &str) -> Result<String> {
    parse_oidc_token_response(json).map(|resp| resp.access_token)
}

#[derive(Debug, PartialEq)]
enum JsonValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

#[derive(Debug, PartialEq)]
struct JsonNumber {
    as_u64: Option<u64>,
}

struct JsonParser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    depth: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().peekable(),
            depth: 0,
        }
    }

    fn parse(&mut self) -> Result<JsonValue> {
        self.skip_whitespace();
        let val = self.parse_value()?;
        self.skip_whitespace();
        if self.chars.next().is_some() {
            return Err(Error::protocol("oidc malformed json"));
        }
        Ok(val)
    }

    fn skip_whitespace(&mut self) {
        while let Some(&c) = self.chars.peek() {
            if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
                let _ = self.chars.next();
            } else {
                break;
            }
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue> {
        self.skip_whitespace();
        let val = match self.chars.peek().copied() {
            Some('{') => self.parse_object()?,
            Some('[') => self.parse_array()?,
            Some('"') => JsonValue::String(self.parse_string()?),
            Some('t' | 'f') => JsonValue::Bool(self.parse_bool()?),
            Some('n') => {
                self.parse_null()?;
                JsonValue::Null
            }
            Some('-' | '0'..='9') => JsonValue::Number(self.parse_number()?),
            _ => return Err(Error::protocol("oidc malformed json")),
        };
        Ok(val)
    }

    fn parse_object(&mut self) -> Result<JsonValue> {
        if self.depth > 32 {
            return Err(Error::protocol("oidc json depth limit"));
        }
        self.depth += 1;
        let _ = self.chars.next();
        let mut entries = Vec::new();
        self.skip_whitespace();
        if let Some('}') = self.chars.peek().copied() {
            let _ = self.chars.next();
            self.depth -= 1;
            return Ok(JsonValue::Object(entries));
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            self.skip_whitespace();
            match self.chars.next() {
                Some(':') => {}
                _ => return Err(Error::protocol("oidc malformed json")),
            }
            let val = self.parse_value()?;
            entries.push((key, val));
            self.skip_whitespace();
            match self.chars.peek().copied() {
                Some(',') => {
                    let _ = self.chars.next();
                    self.skip_whitespace();
                    if let Some('}') = self.chars.peek().copied() {
                        return Err(Error::protocol("oidc malformed json"));
                    }
                }
                Some('}') => {
                    let _ = self.chars.next();
                    break;
                }
                _ => return Err(Error::protocol("oidc malformed json")),
            }
        }
        self.depth -= 1;
        Ok(JsonValue::Object(entries))
    }

    fn parse_array(&mut self) -> Result<JsonValue> {
        if self.depth > 32 {
            return Err(Error::protocol("oidc json depth limit"));
        }
        self.depth += 1;
        let _ = self.chars.next();
        let mut items = Vec::new();
        self.skip_whitespace();
        if let Some(']') = self.chars.peek().copied() {
            let _ = self.chars.next();
            self.depth -= 1;
            return Ok(JsonValue::Array(items));
        }
        loop {
            let val = self.parse_value()?;
            items.push(val);
            self.skip_whitespace();
            match self.chars.peek().copied() {
                Some(',') => {
                    let _ = self.chars.next();
                    self.skip_whitespace();
                    if let Some(']') = self.chars.peek().copied() {
                        return Err(Error::protocol("oidc malformed json"));
                    }
                }
                Some(']') => {
                    let _ = self.chars.next();
                    break;
                }
                _ => return Err(Error::protocol("oidc malformed json")),
            }
        }
        self.depth -= 1;
        Ok(JsonValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String> {
        match self.chars.next() {
            Some('"') => {}
            _ => return Err(Error::protocol("oidc malformed json")),
        }
        let mut out = String::new();
        loop {
            if out.len() > MAX_RESPONSE {
                return Err(Error::protocol("oidc token response too large"));
            }
            let c = self
                .chars
                .next()
                .ok_or_else(|| Error::protocol("oidc malformed json"))?;
            match c {
                '"' => return Ok(out),
                '\\' => {
                    let esc = self
                        .chars
                        .next()
                        .ok_or_else(|| Error::protocol("oidc malformed json"))?;
                    match esc {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\x08'),
                        'f' => out.push('\x0c'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let cp = self.parse_hex_4()?;
                            if (0xd800..=0xdbff).contains(&cp) {
                                match self.chars.next() {
                                    Some('\\') => {}
                                    _ => return Err(Error::protocol("oidc malformed json")),
                                }
                                match self.chars.next() {
                                    Some('u') => {}
                                    _ => return Err(Error::protocol("oidc malformed json")),
                                }
                                let low = self.parse_hex_4()?;
                                if !(0xdc00..=0xdfff).contains(&low) {
                                    return Err(Error::protocol("oidc malformed json"));
                                }
                                let scalar = 0x10000 + ((cp - 0xd800) << 10) + (low - 0xdc00);
                                let ch = char::from_u32(scalar)
                                    .ok_or_else(|| Error::protocol("oidc malformed json"))?;
                                out.push(ch);
                            } else if (0xdc00..=0xdfff).contains(&cp) {
                                return Err(Error::protocol("oidc malformed json"));
                            } else {
                                let ch = char::from_u32(cp)
                                    .ok_or_else(|| Error::protocol("oidc malformed json"))?;
                                out.push(ch);
                            }
                        }
                        _ => return Err(Error::protocol("oidc malformed json")),
                    }
                }
                c if u32::from(c) < 0x20 => {
                    return Err(Error::protocol("oidc malformed json"));
                }
                other => {
                    out.push(other);
                }
            }
        }
    }

    fn parse_hex_4(&mut self) -> Result<u32> {
        let mut val = 0u32;
        for _ in 0..4 {
            let c = self
                .chars
                .next()
                .ok_or_else(|| Error::protocol("oidc malformed json"))?;
            let digit = c
                .to_digit(16)
                .ok_or_else(|| Error::protocol("oidc malformed json"))?;
            val = (val << 4) | digit;
        }
        Ok(val)
    }

    fn parse_bool(&mut self) -> Result<bool> {
        if self.consume_literal("true") {
            Ok(true)
        } else if self.consume_literal("false") {
            Ok(false)
        } else {
            Err(Error::protocol("oidc malformed json"))
        }
    }

    fn parse_null(&mut self) -> Result<()> {
        if self.consume_literal("null") {
            Ok(())
        } else {
            Err(Error::protocol("oidc malformed json"))
        }
    }

    fn consume_literal(&mut self, expected: &str) -> bool {
        let mut matched = 0;
        for exp_ch in expected.chars() {
            if let Some(&c) = self.chars.peek() {
                if c == exp_ch {
                    let _ = self.chars.next();
                    matched += 1;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        matched == expected.chars().count()
    }

    fn parse_number(&mut self) -> Result<JsonNumber> {
        let mut digits = String::new();
        let mut is_negative = false;
        let mut has_fraction_or_exp = false;

        if let Some(&'-') = self.chars.peek() {
            is_negative = true;
            let _ = self.chars.next();
        }

        match self.chars.peek().copied() {
            Some('0') => {
                digits.push('0');
                let _ = self.chars.next();
                if let Some(&c) = self.chars.peek() {
                    if c.is_ascii_digit() {
                        return Err(Error::protocol("oidc malformed json"));
                    }
                }
            }
            Some('1'..='9') => {
                while let Some(&c) = self.chars.peek() {
                    if c.is_ascii_digit() {
                        digits.push(c);
                        let _ = self.chars.next();
                    } else {
                        break;
                    }
                }
            }
            _ => return Err(Error::protocol("oidc malformed json")),
        }

        if let Some(&'.') = self.chars.peek() {
            has_fraction_or_exp = true;
            let _ = self.chars.next();
            let mut frac_digits = 0;
            while let Some(&c) = self.chars.peek() {
                if c.is_ascii_digit() {
                    let _ = self.chars.next();
                    frac_digits += 1;
                } else {
                    break;
                }
            }
            if frac_digits == 0 {
                return Err(Error::protocol("oidc malformed json"));
            }
        }

        if let Some(&c) = self.chars.peek() {
            if c == 'e' || c == 'E' {
                has_fraction_or_exp = true;
                let _ = self.chars.next();
                if let Some(&sign) = self.chars.peek() {
                    if sign == '+' || sign == '-' {
                        let _ = self.chars.next();
                    }
                }
                let mut exp_digits = 0;
                while let Some(&d) = self.chars.peek() {
                    if d.is_ascii_digit() {
                        let _ = self.chars.next();
                        exp_digits += 1;
                    } else {
                        break;
                    }
                }
                if exp_digits == 0 {
                    return Err(Error::protocol("oidc malformed json"));
                }
            }
        }

        let as_u64 = if !is_negative && !has_fraction_or_exp {
            digits.parse::<u64>().ok()
        } else {
            None
        };

        Ok(JsonNumber { as_u64 })
    }
}

/// Token metadata returned from OIDC token endpoint or custom provider.
///
/// Redacts [`Self::token`] in [`fmt::Debug`] to prevent credential leakage (KL-06).
#[derive(Clone, PartialEq, Eq)]
pub struct TokenData {
    /// Access token string issued by the authorization server.
    pub token: String,
    /// Absolute expiration timestamp.
    pub expires_at: Instant,
}

impl fmt::Debug for TokenData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenData")
            .field("token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl TokenData {
    /// Create a new token data record.
    #[must_use]
    pub fn new(token: impl Into<String>, expires_at: Instant) -> Self {
        Self {
            token: token.into(),
            expires_at,
        }
    }

    /// Access the raw token string.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Access the expiration instant.
    #[must_use]
    pub fn expires_at(&self) -> Instant {
        self.expires_at
    }
}

/// Extended configuration options for OIDC proactive refresh and caching behavior.
#[derive(Clone, Debug)]
pub struct OidcRefreshConfig {
    /// Clock skew allowance (default: 60s, or 10% of lifetime for lifetimes < 120s).
    pub clock_skew: Duration,
    /// Proactive refresh ratio (default: 0.80).
    pub refresh_ratio: f64,
    /// Minimum headroom buffer before expiry (default: 60s).
    pub min_buffer: Duration,
    /// Maximum jitter subtracted from proactive refresh point (default: 30s).
    pub max_jitter: Duration,
    /// Request timeout for token acquisition and refresh calls (default: 10s).
    pub request_timeout: Duration,
}

impl Default for OidcRefreshConfig {
    fn default() -> Self {
        Self {
            clock_skew: Duration::from_secs(60),
            refresh_ratio: 0.80,
            min_buffer: Duration::from_secs(60),
            max_jitter: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl OidcRefreshConfig {
    /// Create default refresh configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set clock skew allowance.
    #[must_use]
    pub fn clock_skew(mut self, skew: Duration) -> Self {
        self.clock_skew = skew;
        self
    }

    /// Set proactive refresh ratio.
    #[must_use]
    pub fn refresh_ratio(mut self, ratio: f64) -> Self {
        self.refresh_ratio = ratio;
        self
    }

    /// Set minimum buffer headroom before expiry.
    #[must_use]
    pub fn min_buffer(mut self, buffer: Duration) -> Self {
        self.min_buffer = buffer;
        self
    }

    /// Set maximum jitter.
    #[must_use]
    pub fn max_jitter(mut self, jitter: Duration) -> Self {
        self.max_jitter = jitter;
        self
    }

    /// Set request timeout.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

/// Compute effective clock skew allowance for a token lifetime.
#[must_use]
pub fn compute_skew(lifetime: Duration, base_skew: Duration) -> Duration {
    if lifetime < Duration::from_secs(120) {
        lifetime.mul_f64(0.10)
    } else {
        base_skew.min(lifetime)
    }
}

/// Compute minimum buffer headroom for a token lifetime.
#[must_use]
pub fn compute_buffer(lifetime: Duration, base_min_buffer: Duration) -> Duration {
    base_min_buffer.min(lifetime.mul_f64(0.20))
}

/// Compute maximum proactive jitter for a token lifetime.
#[must_use]
pub fn compute_max_jitter(lifetime: Duration, base_max_jitter: Duration) -> Duration {
    base_max_jitter.min(lifetime.mul_f64(0.10))
}

/// Compute proactive refresh point before jitter.
///
/// Follows Section 6.1 of docs/auth-refresh.md:
/// `T_refresh = T_issued + max(0.80 * T_lifetime, T_lifetime - Delta_skew - Delta_buffer)`
#[must_use]
pub fn compute_refresh_point(
    issued: Instant,
    lifetime: Duration,
    config: &OidcRefreshConfig,
) -> Instant {
    let skew = compute_skew(lifetime, config.clock_skew);
    let buffer = compute_buffer(lifetime, config.min_buffer);
    let ratio_offset = lifetime.mul_f64(config.refresh_ratio);
    let headroom_offset = lifetime.saturating_sub(skew).saturating_sub(buffer);
    let offset = ratio_offset.max(headroom_offset);
    issued.checked_add(offset).unwrap_or(issued)
}

/// Compute scheduled proactive refresh instant with jitter subtracted.
#[must_use]
pub fn compute_scheduled_refresh(
    issued: Instant,
    lifetime: Duration,
    config: &OidcRefreshConfig,
    jitter: Duration,
) -> Instant {
    let refresh_pt = compute_refresh_point(issued, lifetime, config);
    let max_j = compute_max_jitter(lifetime, config.max_jitter);
    let actual_j = jitter.min(max_j);
    refresh_pt
        .checked_sub(actual_j)
        .unwrap_or(issued)
        .max(issued)
}

/// Compute outage retry delay following Section 6.3 of docs/auth-refresh.md.
///
/// Delay is exponential doubling starting at 1s up to 30s with jitter,
/// clamped to `(T_expiry - Delta_skew - T_now) * 0.5`.
/// Returns `None` if the token has reached or passed `T_expiry - Delta_skew`.
#[must_use]
pub fn compute_retry_delay(
    attempt: u32,
    now: Instant,
    expires_at: Instant,
    skew: Duration,
    jitter_factor: f64,
) -> Option<Duration> {
    let hard_limit = expires_at.checked_sub(skew)?;
    let remaining = hard_limit.checked_duration_since(now)?;
    if remaining.is_zero() {
        return None;
    }
    let exp_secs = 1u64
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(30)
        .min(30);
    let base_delay = Duration::from_secs(exp_secs);
    let multiplier = 0.80 + (jitter_factor.clamp(0.0, 1.0) * 0.40);
    let delay_with_jitter = base_delay.mul_f64(multiplier);
    let ceiling = remaining.mul_f64(0.50);
    Some(
        delay_with_jitter
            .min(ceiling)
            .max(Duration::from_millis(10)),
    )
}

/// Token lifecycle states following Section 4.2 of docs/auth-refresh.md.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenLifecycleState {
    /// No token has been acquired yet.
    Uninitialized,
    /// Token acquisition is in-flight. Connection attempts wait on this task.
    Acquiring,
    /// Token is valid and fresh (`T_now < T_refresh`).
    Active,
    /// Token is still valid, but proactive refresh is executing in the background.
    Refreshing,
    /// Background refresh failed with a transient error, but existing token is still valid.
    Degraded,
    /// Token validity has lapsed (`T_now >= T_expiry - Delta_skew`).
    Expired,
    /// Terminal failure (e.g. HTTP 401/403 or invalid credentials).
    FailedClosed,
}

/// Time provider trait for deterministic token refresh and expiry testing.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Return the current instant.
    fn now(&self) -> Instant;

    /// Asynchronously sleep for the given duration.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Standard system clock using [`Instant::now()`] and [`tokio::time::sleep`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// Controllable mock clock for deterministic testing of refresh, expiry, skew, and timeouts.
#[derive(Debug)]
pub struct MockClock {
    now: parking_lot::Mutex<Instant>,
    waiters: parking_lot::Mutex<Vec<(Instant, tokio::sync::oneshot::Sender<()>)>>,
}

impl MockClock {
    /// Create a new mock clock initialized to the given instant.
    #[must_use]
    pub fn new(start: Instant) -> Self {
        Self {
            now: parking_lot::Mutex::new(start),
            waiters: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Advance the mock clock and wake any timers that have expired.
    pub fn advance(&self, duration: Duration) {
        let current = {
            let mut now = self.now.lock();
            *now = now.checked_add(duration).unwrap_or(*now);
            *now
        };
        self.wake_expired(current);
    }

    /// Set the mock clock to a specific instant and wake expired timers.
    pub fn set(&self, instant: Instant) {
        {
            let mut now = self.now.lock();
            *now = instant;
        }
        self.wake_expired(instant);
    }

    fn wake_expired(&self, current: Instant) {
        let mut waiters = self.waiters.lock();
        let mut i = 0;
        while i < waiters.len() {
            if let Some(entry) = waiters.get(i) {
                if entry.0 <= current {
                    let (_, tx) = waiters.swap_remove(i);
                    match tx.send(()) {
                        Ok(()) | Err(()) => {}
                    }
                    continue;
                }
            }
            i += 1;
        }
    }

    /// Count of currently pending sleep waiters.
    #[must_use]
    pub fn pending_waiters(&self) -> usize {
        self.waiters.lock().len()
    }
}

impl Clock for MockClock {
    fn now(&self) -> Instant {
        *self.now.lock()
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let current = *self.now.lock();
        if duration.is_zero() {
            return Box::pin(std::future::ready(()));
        }
        let deadline = current.checked_add(duration).unwrap_or(current);
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut waiters = self.waiters.lock();
            let now = *self.now.lock();
            if deadline <= now {
                return Box::pin(std::future::ready(()));
            }
            waiters.push((deadline, tx));
        }
        Box::pin(async move {
            drop(rx.await);
        })
    }
}

/// Jitter source for refresh scheduling.
pub trait JitterSource: Send + Sync + fmt::Debug {
    /// Produce jitter duration within `[0, max_jitter]`.
    fn jitter(&self, max_jitter: Duration) -> Duration;
}

/// Random jitter generator.
#[derive(Clone, Copy, Debug, Default)]
pub struct RandomJitter;

impl JitterSource for RandomJitter {
    fn jitter(&self, max_jitter: Duration) -> Duration {
        if max_jitter.is_zero() {
            return Duration::ZERO;
        }
        let mut b = [0u8; 8];
        if getrandom::getrandom(&mut b).is_ok() {
            let n = u64::from_ne_bytes(b);
            let nanos = max_jitter.as_nanos();
            if nanos > 0 {
                let j = (n as u128) % nanos;
                let j_u64 = u64::try_from(j).unwrap_or(0);
                return Duration::from_nanos(j_u64);
            }
        }
        Duration::ZERO
    }
}

/// Deterministic zero jitter.
#[derive(Clone, Copy, Debug, Default)]
pub struct ZeroJitter;

impl JitterSource for ZeroJitter {
    fn jitter(&self, _max_jitter: Duration) -> Duration {
        Duration::ZERO
    }
}

/// Deterministic fixed jitter.
#[derive(Clone, Copy, Debug)]
pub struct FixedJitter(pub Duration);

impl JitterSource for FixedJitter {
    fn jitter(&self, max_jitter: Duration) -> Duration {
        self.0.min(max_jitter)
    }
}

/// Trait for fetching token responses from an IdP.
pub trait TokenFetcher: Send + Sync + fmt::Debug {
    /// Fetch token response with the given timeout deadline.
    fn fetch<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<OidcTokenResponse>> + Send + 'a>>;
}

/// Token fetcher using HTTP client credentials over TCP/TLS.
#[derive(Clone, Debug)]
pub struct OidcHttpFetcher {
    config: OidcConfig,
}

impl OidcHttpFetcher {
    /// Create a new HTTP token fetcher.
    #[must_use]
    pub fn new(config: OidcConfig) -> Self {
        Self { config }
    }
}

impl TokenFetcher for OidcHttpFetcher {
    fn fetch<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<OidcTokenResponse>> + Send + 'a>> {
        Box::pin(fetch_client_credentials_token_response(
            &self.config,
            timeout,
        ))
    }
}

/// Closure-based mock token fetcher for testing.
pub struct MockTokenFetcher<F> {
    fetch_fn: F,
}

impl<F> fmt::Debug for MockTokenFetcher<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockTokenFetcher").finish()
    }
}

impl<F> MockTokenFetcher<F>
where
    F: Fn(Duration) -> Pin<Box<dyn Future<Output = Result<OidcTokenResponse>> + Send>>
        + Send
        + Sync,
{
    /// Create a new mock fetcher with the provided closure.
    #[must_use]
    pub fn new(fetch_fn: F) -> Self {
        Self { fetch_fn }
    }
}

impl<F> TokenFetcher for MockTokenFetcher<F>
where
    F: Fn(Duration) -> Pin<Box<dyn Future<Output = Result<OidcTokenResponse>> + Send>>
        + Send
        + Sync,
{
    fn fetch<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<OidcTokenResponse>> + Send + 'a>> {
        (self.fetch_fn)(timeout)
    }
}

#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    issued_at: Instant,
    expires_at: Instant,
    lifetime: Duration,
}

impl CachedToken {
    fn is_valid(&self, now: Instant, skew: Duration) -> bool {
        now.checked_add(skew)
            .is_some_and(|threshold| threshold < self.expires_at)
    }
}

struct ManagerState {
    cached: Option<CachedToken>,
    state: TokenLifecycleState,
    terminal_error: Option<String>,
    token_epoch: u64,
    in_flight_rx: Option<tokio::sync::watch::Receiver<Option<Result<TokenData>>>>,
    in_flight_tx: Option<tokio::sync::watch::Sender<Option<Result<TokenData>>>>,
}

enum RefreshAction {
    ScheduleNext(Option<(Instant, u64)>),
    Retry(Duration),
    Stop,
}

struct OidcTokenManagerInner {
    fetcher: Arc<dyn TokenFetcher>,
    config: OidcRefreshConfig,
    clock: Arc<dyn Clock>,
    jitter: Arc<dyn JitterSource>,
    state: parking_lot::RwLock<ManagerState>,
    refresh_abort_handle: parking_lot::Mutex<Option<tokio::task::AbortHandle>>,
}

impl Drop for OidcTokenManagerInner {
    fn drop(&mut self) {
        if let Some(handle) = self.refresh_abort_handle.lock().take() {
            handle.abort();
        }
    }
}

impl OidcTokenManagerInner {
    async fn execute_acquisition(self: Arc<Self>) {
        let timeout_duration = self.config.request_timeout;
        let res = self.fetcher.fetch(timeout_duration).await;
        let now = self.clock.now();

        let (result_to_send, scheduled_opt, tx) = {
            let mut state = self.state.write();
            let tx = state.in_flight_tx.take();
            let _ = state.in_flight_rx.take();

            match res {
                Ok(token_resp) => {
                    let lifetime = token_resp
                        .expires_in
                        .map(Duration::from_secs)
                        .unwrap_or(Duration::from_secs(3600 * 24 * 365));
                    let expires_at = now.checked_add(lifetime).unwrap_or(now);
                    let cached = CachedToken {
                        token: token_resp.access_token.clone(),
                        issued_at: now,
                        expires_at,
                        lifetime,
                    };
                    state.cached = Some(cached);
                    state.state = TokenLifecycleState::Active;
                    state.terminal_error = None;
                    state.token_epoch = state.token_epoch.wrapping_add(1);
                    let epoch = state.token_epoch;

                    let token_data = TokenData::new(token_resp.access_token, expires_at);
                    let scheduled = if token_resp.expires_in.is_some() {
                        let jitter_dur = self.jitter.jitter(self.config.max_jitter);
                        let sched_inst =
                            compute_scheduled_refresh(now, lifetime, &self.config, jitter_dur);
                        Some((sched_inst, epoch))
                    } else {
                        None
                    };
                    (Ok(token_data), scheduled, tx)
                }
                Err(err) => {
                    let is_transient = is_transient_oidc_error(&err);
                    if !is_transient {
                        state.state = TokenLifecycleState::FailedClosed;
                        state.terminal_error = Some(err.to_string());
                    } else if state.cached.is_none() {
                        state.state = TokenLifecycleState::Uninitialized;
                    }
                    (Err(err), None, tx)
                }
            }
        };

        if let Some(tx) = tx {
            drop(tx.send(Some(result_to_send)));
        }

        if let Some((sched_inst, epoch)) = scheduled_opt {
            self.schedule_refresh(sched_inst, epoch);
        }
    }

    fn schedule_refresh(self: &Arc<Self>, scheduled: Instant, epoch: u64) {
        let weak_self = Arc::downgrade(self);
        let abort_handle = tokio::spawn(async move {
            Self::run_refresh_loop_scheduled(weak_self, scheduled, epoch).await;
        })
        .abort_handle();

        let mut handle_guard = self.refresh_abort_handle.lock();
        if let Some(prev) = handle_guard.replace(abort_handle) {
            prev.abort();
        }
    }

    async fn run_refresh_loop_scheduled(weak_self: Weak<Self>, scheduled: Instant, epoch: u64) {
        let (clock, delay) = {
            let Some(inner) = weak_self.upgrade() else {
                return;
            };
            let now = inner.clock.now();
            let delay = scheduled.saturating_duration_since(now);
            (Arc::clone(&inner.clock), delay)
        };
        clock.sleep(delay).await;

        Self::run_refresh_loop(weak_self, epoch).await;
    }

    async fn run_refresh_loop(weak_self: Weak<Self>, epoch: u64) {
        let Some(inner) = weak_self.upgrade() else {
            return;
        };
        {
            let mut state = inner.state.write();
            if state.token_epoch != epoch {
                return;
            }
            state.state = TokenLifecycleState::Refreshing;
        }

        let mut attempt = 1u32;
        loop {
            let fetch_timeout = inner.config.request_timeout;
            let res = inner.fetcher.fetch(fetch_timeout).await;
            let now = inner.clock.now();

            let Some(inner) = weak_self.upgrade() else {
                return;
            };
            let action = {
                let mut state = inner.state.write();
                if state.token_epoch != epoch {
                    return;
                }
                match res {
                    Ok(resp) => {
                        let lifetime = resp
                            .expires_in
                            .map(Duration::from_secs)
                            .unwrap_or(Duration::from_secs(3600 * 24 * 365));
                        let expires_at = now.checked_add(lifetime).unwrap_or(now);
                        state.cached = Some(CachedToken {
                            token: resp.access_token,
                            issued_at: now,
                            expires_at,
                            lifetime,
                        });
                        state.state = TokenLifecycleState::Active;
                        state.terminal_error = None;
                        state.token_epoch = state.token_epoch.wrapping_add(1);
                        let new_epoch = state.token_epoch;

                        let next_sched = if resp.expires_in.is_some() {
                            let j = inner.jitter.jitter(inner.config.max_jitter);
                            let sched_inst =
                                compute_scheduled_refresh(now, lifetime, &inner.config, j);
                            Some((sched_inst, new_epoch))
                        } else {
                            None
                        };
                        RefreshAction::ScheduleNext(next_sched)
                    }
                    Err(err) => {
                        let is_transient = is_transient_oidc_error(&err);
                        if !is_transient {
                            state.cached = None;
                            state.state = TokenLifecycleState::FailedClosed;
                            state.terminal_error = Some(err.to_string());
                            RefreshAction::Stop
                        } else {
                            state.state = TokenLifecycleState::Degraded;
                            if let Some(ref cached) = state.cached {
                                let skew = compute_skew(cached.lifetime, inner.config.clock_skew);
                                if !cached.is_valid(now, skew) {
                                    state.cached = None;
                                    state.state = TokenLifecycleState::Expired;
                                    RefreshAction::Stop
                                } else {
                                    let jitter_factor = 0.50;
                                    match compute_retry_delay(
                                        attempt,
                                        now,
                                        cached.expires_at,
                                        skew,
                                        jitter_factor,
                                    ) {
                                        Some(retry_delay) => RefreshAction::Retry(retry_delay),
                                        None => {
                                            state.cached = None;
                                            state.state = TokenLifecycleState::Expired;
                                            RefreshAction::Stop
                                        }
                                    }
                                }
                            } else {
                                state.state = TokenLifecycleState::Expired;
                                RefreshAction::Stop
                            }
                        }
                    }
                }
            };

            match action {
                RefreshAction::ScheduleNext(Some((next_sched, new_epoch))) => {
                    inner.schedule_refresh(next_sched, new_epoch);
                    break;
                }
                RefreshAction::ScheduleNext(None) => {
                    *inner.refresh_abort_handle.lock() = None;
                    break;
                }
                RefreshAction::Stop => {
                    *inner.refresh_abort_handle.lock() = None;
                    break;
                }
                RefreshAction::Retry(delay) => {
                    attempt = attempt.saturating_add(1);
                    inner.clock.sleep(delay).await;
                }
            }
        }
    }
}

/// Bounded OIDC token cache and proactive refresh owner.
///
/// Manages token lifecycle, single-flight coalescing to prevent stampedes,
/// proactive background refresh, and deterministic clock support.
#[derive(Clone)]
pub struct OidcTokenManager {
    inner: Arc<OidcTokenManagerInner>,
}

impl fmt::Debug for OidcTokenManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OidcTokenManager")
            .field("state", &self.state())
            .field("config", &self.inner.config)
            .finish()
    }
}

impl OidcTokenManager {
    /// Create a new token manager for the given OIDC configuration with default refresh settings and system clock.
    #[must_use]
    pub fn new(config: OidcConfig) -> Self {
        let refresh_config = OidcRefreshConfig::default();
        let fetcher = Arc::new(OidcHttpFetcher::new(config));
        let clock = Arc::new(SystemClock);
        let jitter = Arc::new(RandomJitter);
        Self::with_fetcher(fetcher, refresh_config, clock, jitter)
    }

    /// Create a token manager with custom refresh configuration.
    #[must_use]
    pub fn with_refresh_config(config: OidcConfig, refresh_config: OidcRefreshConfig) -> Self {
        let fetcher = Arc::new(OidcHttpFetcher::new(config));
        let clock = Arc::new(SystemClock);
        let jitter = Arc::new(RandomJitter);
        Self::with_fetcher(fetcher, refresh_config, clock, jitter)
    }

    /// Create a token manager with custom refresh configuration, clock, and jitter source.
    #[must_use]
    pub fn with_clock_and_jitter(
        config: OidcConfig,
        refresh_config: OidcRefreshConfig,
        clock: Arc<dyn Clock>,
        jitter: Arc<dyn JitterSource>,
    ) -> Self {
        let fetcher = Arc::new(OidcHttpFetcher::new(config));
        Self::with_fetcher(fetcher, refresh_config, clock, jitter)
    }

    /// Create a token manager with a custom token fetcher (e.g. for deterministic unit testing).
    #[must_use]
    pub fn with_fetcher(
        fetcher: Arc<dyn TokenFetcher>,
        refresh_config: OidcRefreshConfig,
        clock: Arc<dyn Clock>,
        jitter: Arc<dyn JitterSource>,
    ) -> Self {
        let inner = Arc::new(OidcTokenManagerInner {
            fetcher,
            config: refresh_config,
            clock,
            jitter,
            state: parking_lot::RwLock::new(ManagerState {
                cached: None,
                state: TokenLifecycleState::Uninitialized,
                terminal_error: None,
                token_epoch: 0,
                in_flight_rx: None,
                in_flight_tx: None,
            }),
            refresh_abort_handle: parking_lot::Mutex::new(None),
        });
        Self { inner }
    }

    /// Retrieve a valid token, acquiring one synchronously if uninitialized or expired,
    /// or returning the cached token while refreshing proactively in the background.
    pub async fn token(&self, timeout: Duration) -> Result<String> {
        self.token_data(timeout).await.map(|td| td.token)
    }

    /// Retrieve a valid [`TokenData`] record.
    pub async fn token_data(&self, timeout_dur: Duration) -> Result<TokenData> {
        let deadline = Instant::now() + timeout_dur;

        // 1. Fast read path: Check cache under read lock
        {
            let state = self.inner.state.read();
            if state.state == TokenLifecycleState::FailedClosed {
                let msg = state
                    .terminal_error
                    .as_deref()
                    .unwrap_or("oidc authentication failed closed");
                return Err(Error::protocol(msg));
            }
            if let Some(ref cached) = state.cached {
                let skew = compute_skew(cached.lifetime, self.inner.config.clock_skew);
                let now = self.inner.clock.now();
                if cached.is_valid(now, skew) {
                    let refresh_pt = compute_refresh_point(
                        cached.issued_at,
                        cached.lifetime,
                        &self.inner.config,
                    );
                    let should_refresh =
                        now >= refresh_pt && state.state == TokenLifecycleState::Active;
                    let result = TokenData::new(cached.token.clone(), cached.expires_at);
                    drop(state);
                    if should_refresh {
                        self.maybe_trigger_proactive_refresh();
                    }
                    return Ok(result);
                }
            }
        }

        // 2. Slow path: Acquire write lock
        let (rx, is_leader) = {
            let mut state = self.inner.state.write();
            if state.state == TokenLifecycleState::FailedClosed {
                let msg = state
                    .terminal_error
                    .as_deref()
                    .unwrap_or("oidc authentication failed closed");
                return Err(Error::protocol(msg));
            }
            if let Some(ref cached) = state.cached {
                let skew = compute_skew(cached.lifetime, self.inner.config.clock_skew);
                let now = self.inner.clock.now();
                if cached.is_valid(now, skew) {
                    return Ok(TokenData::new(cached.token.clone(), cached.expires_at));
                }
            }
            state.cached = None;
            if state.state != TokenLifecycleState::Acquiring {
                state.state = TokenLifecycleState::Acquiring;
            }
            if let Some(ref existing_rx) = state.in_flight_rx {
                (existing_rx.clone(), false)
            } else {
                let (tx, rx) = tokio::sync::watch::channel(None);
                state.in_flight_rx = Some(rx.clone());
                state.in_flight_tx = Some(tx);
                (rx, true)
            }
        };

        if is_leader {
            let inner = Arc::clone(&self.inner);
            drop(tokio::spawn(async move {
                inner.execute_acquisition().await;
            }));
        }

        let time_remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        self.wait_for_in_flight(rx, time_remaining).await
    }

    fn maybe_trigger_proactive_refresh(&self) {
        let mut handle_guard = self.inner.refresh_abort_handle.lock();
        if handle_guard.is_some() {
            return;
        }
        let epoch = self.inner.state.read().token_epoch;
        let weak_self = Arc::downgrade(&self.inner);
        let abort_handle = tokio::spawn(async move {
            OidcTokenManagerInner::run_refresh_loop(weak_self, epoch).await;
        })
        .abort_handle();
        *handle_guard = Some(abort_handle);
    }

    async fn wait_for_in_flight(
        &self,
        mut rx: tokio::sync::watch::Receiver<Option<Result<TokenData>>>,
        timeout_duration: Duration,
    ) -> Result<TokenData> {
        if let Some(res) = rx.borrow().as_ref() {
            return res.clone();
        }
        let wait_fut = async {
            while rx.changed().await.is_ok() {
                if let Some(res) = rx.borrow().as_ref() {
                    return res.clone();
                }
            }
            Err(Error::protocol("oidc token acquisition task dropped"))
        };
        match timeout(timeout_duration, wait_fut).await {
            Ok(res) => res,
            Err(_) => Err(Error::Timeout),
        }
    }

    /// Inspect currently cached token data without blocking or fetching.
    #[must_use]
    pub fn cached_token(&self) -> Option<TokenData> {
        let state = self.inner.state.read();
        state
            .cached
            .as_ref()
            .map(|c| TokenData::new(c.token.clone(), c.expires_at))
    }

    /// Current token lifecycle state.
    #[must_use]
    pub fn state(&self) -> TokenLifecycleState {
        self.inner.state.read().state
    }

    /// Reference to the clock used by this manager.
    #[must_use]
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.inner.clock
    }

    /// Reference to the refresh configuration.
    #[must_use]
    pub fn refresh_config(&self) -> &OidcRefreshConfig {
        &self.inner.config
    }
}

/// Pluggable asynchronous token provider trait.
pub trait TokenProvider: Send + Sync {
    /// Retrieve a valid token, refreshing proactively if near expiration.
    fn token<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>>;

    /// Retrieve a valid [`TokenData`] record with expiration metadata.
    fn token_data<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<TokenData>> + Send + 'a>> {
        let _ = timeout;
        Box::pin(async {
            Err(Error::Unsupported(
                "token_data not implemented by provider".into(),
            ))
        })
    }
}

impl TokenProvider for OidcTokenManager {
    fn token<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> {
        Box::pin(self.token(timeout))
    }

    fn token_data<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<TokenData>> + Send + 'a>> {
        Box::pin(self.token_data(timeout))
    }
}

impl TokenProvider for Arc<OidcTokenManager> {
    fn token<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> {
        Box::pin(self.as_ref().token(timeout))
    }

    fn token_data<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<TokenData>> + Send + 'a>> {
        Box::pin(self.as_ref().token_data(timeout))
    }
}

/// Type alias for [`OidcTokenManager`].
pub type OidcTokenProvider = OidcTokenManager;

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
            Error::Protocol(m) => {
                assert!(m.contains("401"), "{m}");
                assert!(
                    !m.contains("invalid_client"),
                    "OIDC Error must not embed IdP response body: {m}"
                );
                assert_eq!(m, "oidc token endpoint HTTP 401");
            }
            other => panic!("expected protocol 401, got {other}"),
        }
    }

    #[tokio::test]
    async fn fetch_token_rejects_http_503_fail_closed() {
        // Persistent IdP 503: bounded retries then fail closed (status-only Protocol).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(tokio::spawn(async move {
            for _ in 0..OIDC_FETCH_ATTEMPTS {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 1024];
                let _n = sock.read(&mut buf).await.unwrap();
                let body = "{\"error\":\"server_error\",\"error_description\":\"try again\"}";
                let resp = format!(
                    "HTTP/1.1 503 Service Unavailable\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            }
        }));
        let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", "csecret");
        let err = fetch_client_credentials_token(&cfg, Duration::from_secs(5))
            .await
            .unwrap_err();
        match err {
            Error::Protocol(m) => {
                assert_eq!(m, "oidc token endpoint HTTP 503");
                assert!(
                    !m.contains("server_error") && !m.contains("try again"),
                    "OIDC Error must not embed IdP outage body: {m}"
                );
            }
            other => panic!("expected protocol 503, got {other}"),
        }
    }

    #[tokio::test]
    async fn fetch_token_retries_transient_503_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        let hits = Arc::new(AtomicU32::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits_srv = hits.clone();
        drop(tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 1024];
                let _n = sock.read(&mut buf).await.unwrap();
                let n = hits_srv.fetch_add(1, Ordering::SeqCst) + 1;
                let (status, body) = if n == 1 {
                    (
                        "503 Service Unavailable",
                        "{\"error\":\"temporarily_unavailable\"}",
                    )
                } else {
                    (
                        "200 OK",
                        "{\"access_token\":\"tok-after-retry\",\"token_type\":\"Bearer\"}",
                    )
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            }
        }));
        let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", "csecret");
        let token = fetch_client_credentials_token(&cfg, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(token, "tok-after-retry");
        assert!(
            hits.load(Ordering::SeqCst) >= 2,
            "expected at least one retry after 503"
        );
    }

    #[tokio::test]
    async fn fetch_token_does_not_retry_http_401() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        let hits = Arc::new(AtomicU32::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits_srv = hits.clone();
        drop(tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = hits_srv.fetch_add(1, Ordering::SeqCst);
            let mut buf = vec![0u8; 1024];
            let _n = sock.read(&mut buf).await.unwrap();
            let body = "{\"error\":\"invalid_client\"}";
            let resp = format!(
                "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            // A second accept would hang the test if the client retried.
        }));
        let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", "bad");
        let err = fetch_client_credentials_token(&cfg, Duration::from_secs(5))
            .await
            .unwrap_err();
        match err {
            Error::Protocol(m) => assert_eq!(m, "oidc token endpoint HTTP 401"),
            other => panic!("expected protocol 401, got {other}"),
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1, "401 must not be retried");
    }

    #[tokio::test]
    async fn fetch_token_hang_times_out_fail_closed() {
        // Silent IdP hang: request_timeout must surface Error::Timeout (no hang forever).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let _n = sock.read(&mut buf).await.unwrap();
            // Accept the request then never respond within the client timeout.
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(sock.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await);
        }));
        let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", "csecret");
        let err = fetch_client_credentials_token(&cfg, Duration::from_millis(80))
            .await
            .unwrap_err();
        match err {
            Error::Timeout => {}
            other => panic!("expected Timeout on IdP hang, got {other}"),
        }
    }

    #[test]
    fn parse_token_response_success_all_fields() {
        let json = "{\"access_token\":\"tok-123\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
        let resp = parse_oidc_token_response(json).unwrap();
        assert_eq!(resp.access_token, "tok-123");
        assert_eq!(resp.token_type, "Bearer");
        assert_eq!(resp.expires_in, Some(3600));
    }

    #[test]
    fn parse_token_response_escaped_json() {
        let json = "{\"access_token\":\"tok\\\"escaped\\\\slashes\\/newlines\\n\\r\\ttabs\\b\\f\\u0041\\uD83D\\uDE00\",\"token_type\":\"bearer\",\"expires_in\":0}";
        let resp = parse_oidc_token_response(json).unwrap();
        assert_eq!(
            resp.access_token,
            "tok\"escaped\\slashes/newlines\n\r\ttabs\x08\x0cA\u{1F600}"
        );
        assert_eq!(resp.token_type, "bearer");
        assert_eq!(resp.expires_in, Some(0));
    }

    #[test]
    fn parse_token_response_missing_expires_in() {
        let json = "{\"access_token\":\"tok-no-exp\",\"token_type\":\"Bearer\"}";
        let resp = parse_oidc_token_response(json).unwrap();
        assert_eq!(resp.access_token, "tok-no-exp");
        assert_eq!(resp.token_type, "Bearer");
        assert_eq!(resp.expires_in, None);
    }

    #[test]
    fn parse_token_response_bearer_case_insensitive() {
        for tt in ["Bearer", "bearer", "BEARER", "BeArEr"] {
            let json = format!("{{\"access_token\":\"tok\",\"token_type\":\"{tt}\"}}");
            let resp = parse_oidc_token_response(&json).unwrap();
            assert_eq!(resp.token_type, tt);
        }
    }

    #[test]
    fn parse_token_response_ignores_extra_fields() {
        let json = "{\
            \"access_token\":\"tok-extra\",\
            \"token_type\":\"Bearer\",\
            \"scope\":\"read write\",\
            \"refresh_token\":\"ref-123\",\
            \"active\":true,\
            \"nested\":{\"k\":[1,2,null]}\
        }";
        let resp = parse_oidc_token_response(json).unwrap();
        assert_eq!(resp.access_token, "tok-extra");
        assert_eq!(resp.token_type, "Bearer");
    }

    #[test]
    fn parse_token_response_missing_required_fields() {
        let no_token = "{\"token_type\":\"Bearer\",\"expires_in\":3600}";
        match parse_oidc_token_response(no_token).unwrap_err() {
            Error::Protocol(m) => assert_eq!(m, "oidc response missing access_token"),
            other => panic!("expected missing access_token, got {other:?}"),
        }

        let no_type = "{\"access_token\":\"tok\",\"expires_in\":3600}";
        match parse_oidc_token_response(no_type).unwrap_err() {
            Error::Protocol(m) => assert_eq!(m, "oidc response missing token_type"),
            other => panic!("expected missing token_type, got {other:?}"),
        }
    }

    #[test]
    fn parse_token_response_empty_fields_fail() {
        let empty_token = "{\"access_token\":\"\",\"token_type\":\"Bearer\"}";
        match parse_oidc_token_response(empty_token).unwrap_err() {
            Error::Protocol(m) => assert_eq!(m, "oidc empty access_token"),
            other => panic!("expected empty access_token error, got {other:?}"),
        }

        let empty_type = "{\"access_token\":\"tok\",\"token_type\":\"\"}";
        match parse_oidc_token_response(empty_type).unwrap_err() {
            Error::Protocol(m) => assert_eq!(m, "oidc empty token_type"),
            other => panic!("expected empty token_type error, got {other:?}"),
        }
    }

    #[test]
    fn parse_token_response_wrong_type_fields() {
        let cases = [
            (
                "{\"access_token\":123,\"token_type\":\"Bearer\"}",
                "oidc access_token not a string",
            ),
            (
                "{\"access_token\":true,\"token_type\":\"Bearer\"}",
                "oidc access_token not a string",
            ),
            (
                "{\"access_token\":null,\"token_type\":\"Bearer\"}",
                "oidc access_token not a string",
            ),
            (
                "{\"access_token\":[],\"token_type\":\"Bearer\"}",
                "oidc access_token not a string",
            ),
            (
                "{\"access_token\":{},\"token_type\":\"Bearer\"}",
                "oidc access_token not a string",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":123}",
                "oidc token_type not a string",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":false}",
                "oidc token_type not a string",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":[\"Bearer\"]}",
                "oidc token_type not a string",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"mac\"}",
                "oidc unsupported token_type",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"dpop\"}",
                "oidc unsupported token_type",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":\"3600\"}",
                "oidc invalid expires_in",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":true}",
                "oidc invalid expires_in",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":null}",
                "oidc invalid expires_in",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":-10}",
                "oidc invalid expires_in",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":3600.5}",
                "oidc invalid expires_in",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":1e3}",
                "oidc invalid expires_in",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":[3600]}",
                "oidc invalid expires_in",
            ),
            (
                "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":{}}",
                "oidc invalid expires_in",
            ),
        ];
        for (json, expected_err) in cases {
            match parse_oidc_token_response(json).unwrap_err() {
                Error::Protocol(m) => assert_eq!(m, expected_err, "failed for {json}"),
                other => panic!("expected {expected_err}, got {other:?} for {json}"),
            }
        }
    }

    #[test]
    fn parse_token_response_malformed_and_truncated() {
        let cases = [
            "",
            "   ",
            "{",
            "{\"access_token\":",
            "{\"access_token\":\"tok\"",
            "{\"access_token\":\"tok\",",
            "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",}",
            "{\"access_token\":\"tok\",\"token_type\":\"Bearer\"} extra",
            "{\"access_token\":\"tok\\u12\",\"token_type\":\"Bearer\"}",
            "{\"access_token\":\"tok\\uD800\",\"token_type\":\"Bearer\"}",
            "{\"access_token\":\"tok\\uD800\\u0041\",\"token_type\":\"Bearer\"}",
            "{\"access_token\":\"tok\\uDC00\",\"token_type\":\"Bearer\"}",
            "{\"access_token\":\"tok\nunescaped\",\"token_type\":\"Bearer\"}",
            "{\"access_token\":0123,\"token_type\":\"Bearer\"}",
            "{\"access_token\":\"tok\",\"expires_in\":1.}",
            "{\"access_token\":\"tok\",\"expires_in\":1e}",
        ];
        for json in cases {
            assert!(
                parse_oidc_token_response(json).is_err(),
                "malformed JSON should be rejected: {json}"
            );
        }

        let non_objects = [
            "[\"access_token\",\"tok\"]",
            "\"string-only\"",
            "12345",
            "true",
            "null",
        ];
        for json in non_objects {
            match parse_oidc_token_response(json).unwrap_err() {
                Error::Protocol(m) => assert_eq!(m, "oidc response not a json object"),
                other => panic!("expected not a json object, got {other:?} for {json}"),
            }
        }
    }

    #[test]
    fn parse_token_response_duplicate_keys() {
        let cases = [
            "{\"access_token\":\"tok1\",\"access_token\":\"tok2\",\"token_type\":\"Bearer\"}",
            "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"token_type\":\"Bearer\"}",
            "{\"access_token\":\"tok\",\"token_type\":\"Bearer\",\"expires_in\":3600,\"expires_in\":7200}",
        ];
        for json in cases {
            match parse_oidc_token_response(json).unwrap_err() {
                Error::Protocol(m) => assert_eq!(m, "oidc duplicate json key"),
                other => panic!("expected duplicate key error, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_token_response_size_limits() {
        let oversized = format!(
            "{{\"access_token\":\"{}\",\"token_type\":\"Bearer\"}}",
            "a".repeat(MAX_RESPONSE + 1)
        );
        match parse_oidc_token_response(&oversized).unwrap_err() {
            Error::Protocol(m) => assert_eq!(m, "oidc token response too large"),
            other => panic!("expected token response too large, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_http_response_chunked_decoding() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let resp = b"HTTP/1.1 200 OK\r\n\
                     Transfer-Encoding: chunked\r\n\
                     Content-Type: application/json\r\n\
                     Connection: close\r\n\
                     \r\n\
                     4\r\n\
                     Wiki\r\n\
                     5\r\n\
                     pedia\r\n\
                     0\r\n\
                     \r\n";
        server.write_all(resp).await.unwrap();
        drop(server);

        let (status, body) =
            read_http_response(&mut client, Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"Wikipedia");
    }

    #[tokio::test]
    async fn read_http_response_chunked_with_extensions_and_trailers() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let resp = b"HTTP/1.1 200 OK\r\n\
                     Transfer-Encoding: chunked\r\n\
                     Connection: close\r\n\
                     \r\n\
                     4;ext=foo;bar=baz\r\n\
                     Wiki\r\n\
                     5\r\n\
                     pedia\r\n\
                     0\r\n\
                     Expires: never\r\n\
                     X-Checksum: 1234\r\n\
                     \r\n";
        server.write_all(resp).await.unwrap();
        drop(server);

        let (status, body) =
            read_http_response(&mut client, Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"Wikipedia");
    }

    #[tokio::test]
    async fn read_http_response_chunked_truncated_fails() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let resp = b"HTTP/1.1 200 OK\r\n\
                     Transfer-Encoding: chunked\r\n\
                     Connection: close\r\n\
                     \r\n\
                     10\r\n\
                     short";
        server.write_all(resp).await.unwrap();
        drop(server);

        let err = read_http_response(&mut client, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap_err();
        match err {
            Error::Protocol(m) => assert_eq!(m, "oidc truncated chunked body"),
            other => panic!("expected truncated chunked body, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_http_response_chunked_malformed_fails() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let resp = b"HTTP/1.1 200 OK\r\n\
                     Transfer-Encoding: chunked\r\n\
                     Connection: close\r\n\
                     \r\n\
                     ZZ\r\n\
                     data\r\n\
                     0\r\n\
                     \r\n";
        server.write_all(resp).await.unwrap();
        drop(server);

        let err = read_http_response(&mut client, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap_err();
        match err {
            Error::Protocol(m) => assert_eq!(m, "oidc malformed chunked body"),
            other => panic!("expected malformed chunked body, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_http_response_truncated_content_length() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let resp = b"HTTP/1.1 200 OK\r\n\
                     Content-Length: 100\r\n\
                     Connection: close\r\n\
                     \r\n\
                     partially-sent-body";
        server.write_all(resp).await.unwrap();
        drop(server);

        let err = read_http_response(&mut client, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap_err();
        match err {
            Error::Protocol(m) => assert_eq!(m, "oidc truncated response body"),
            other => panic!("expected truncated response body, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_http_response_oversized_content_length() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_RESPONSE + 100
        );
        server.write_all(resp.as_bytes()).await.unwrap();
        drop(server);

        let err = read_http_response(&mut client, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap_err();
        match err {
            Error::Protocol(m) => assert_eq!(m, "oidc token response too large"),
            other => panic!("expected token response too large, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_http_response_truncated_headers() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        server
            .write_all(b"HTTP/1.1 200 OK\r\nIncomplete-Header: 1")
            .await
            .unwrap();
        drop(server);

        let err = read_http_response(&mut client, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap_err();
        match err {
            Error::Protocol(m) => assert_eq!(m, "oidc truncated headers"),
            other => panic!("expected truncated headers, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_token_response_chunked_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 1024];
                let _n = sock.read(&mut buf).await.unwrap();
                let c1 = "{\"access_token\":\"chunked-tok-1\",";
                let c2 = "\"token_type\":\"Bearer\",\"expires_in\":1800}";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Transfer-Encoding: chunked\r\n\
                     Content-Type: application/json\r\n\
                     Connection: close\r\n\
                     \r\n\
                     {:x}\r\n{}\r\n\
                     {:x}\r\n{}\r\n\
                     0\r\n\r\n",
                    c1.len(),
                    c1,
                    c2.len(),
                    c2
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            }
        }));

        let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", "csecret");
        let resp = fetch_client_credentials_token_response(&cfg, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(resp.access_token, "chunked-tok-1");
        assert_eq!(resp.token_type, "Bearer");
        assert_eq!(resp.expires_in, Some(1800));

        // One-shot method also preserves public path
        let token = fetch_client_credentials_token(&cfg, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(token, "chunked-tok-1");
    }

    #[test]
    fn compute_skew_rules() {
        let base = Duration::from_secs(60);
        // Under 120s: 10% of lifetime
        assert_eq!(
            compute_skew(Duration::from_secs(100), base),
            Duration::from_secs(10)
        );
        assert_eq!(
            compute_skew(Duration::from_secs(50), base),
            Duration::from_secs(5)
        );
        // Over or equal 120s: base skew clamped to lifetime
        assert_eq!(
            compute_skew(Duration::from_secs(120), base),
            Duration::from_secs(60)
        );
        assert_eq!(
            compute_skew(Duration::from_secs(3600), base),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn compute_buffer_and_refresh_point() {
        let config = OidcRefreshConfig::default();
        let issued = Instant::now();

        // 3600s token: skew = 60s, buffer = 60s
        // 0.80 * 3600 = 2880s
        // 3600 - 60 - 60 = 3480s
        // max(2880, 3480) = 3480s
        let pt = compute_refresh_point(issued, Duration::from_secs(3600), &config);
        assert_eq!(pt, issued + Duration::from_secs(3480));

        // Short token (60s): skew = 6s, buffer = 12s
        // 0.80 * 60 = 48s
        // 60 - 6 - 12 = 42s
        // max(48, 42) = 48s
        let pt_short = compute_refresh_point(issued, Duration::from_secs(60), &config);
        assert_eq!(pt_short, issued + Duration::from_secs(48));
    }

    #[test]
    fn compute_jitter_and_scheduled_refresh() {
        let config = OidcRefreshConfig::default();
        let issued = Instant::now();
        let lifetime = Duration::from_secs(3600);

        // Max jitter is min(30s, 0.10 * 3600 = 360s) = 30s
        assert_eq!(
            compute_max_jitter(lifetime, config.max_jitter),
            Duration::from_secs(30)
        );

        // Jitter of 10s subtracted from refresh point (3480s - 10s = 3470s)
        let sched = compute_scheduled_refresh(issued, lifetime, &config, Duration::from_secs(10));
        assert_eq!(sched, issued + Duration::from_secs(3470));

        // Jitter exceeding max_jitter clamped to max_jitter (3480s - 30s = 3450s)
        let sched_clamped =
            compute_scheduled_refresh(issued, lifetime, &config, Duration::from_secs(50));
        assert_eq!(sched_clamped, issued + Duration::from_secs(3450));
    }

    #[test]
    fn compute_retry_delay_exponential_and_clamping() {
        let now = Instant::now();
        let expires_at = now + Duration::from_secs(100);
        let skew = Duration::from_secs(20);
        // remaining = (100 - 20) = 80s
        // ceiling = 80 * 0.5 = 40s

        // attempt 1: base = 1s, factor = 0.5 -> 1.0 multiplier -> 1s
        let d1 = compute_retry_delay(1, now, expires_at, skew, 0.5).unwrap();
        assert_eq!(d1, Duration::from_secs(1));

        // attempt 2: base = 2s, factor = 0.5 -> 2s
        let d2 = compute_retry_delay(2, now, expires_at, skew, 0.5).unwrap();
        assert_eq!(d2, Duration::from_secs(2));

        // attempt 5: base = 16s, factor = 0.5 -> 16s
        let d5 = compute_retry_delay(5, now, expires_at, skew, 0.5).unwrap();
        assert_eq!(d5, Duration::from_secs(16));

        // attempt 6: base = 30s (capped at 30s), factor = 0.5 -> 30s
        let d6 = compute_retry_delay(6, now, expires_at, skew, 0.5).unwrap();
        assert_eq!(d6, Duration::from_secs(30));

        // When remaining is small (e.g. 10s), ceiling is 5s, which clamps 30s -> 5s
        let near_expiry = now + Duration::from_secs(30);
        // remaining = (30 - 20) = 10s, ceiling = 5s
        let d_clamped = compute_retry_delay(6, now, near_expiry, skew, 0.5).unwrap();
        assert_eq!(d_clamped, Duration::from_secs(5));

        // Past expiration ceiling: now + skew >= expires_at -> None
        let expired = now + Duration::from_secs(15);
        assert!(compute_retry_delay(1, now, expired, skew, 0.5).is_none());
    }

    #[test]
    fn token_data_debug_redacts_token() {
        let td = TokenData::new(
            "secret-token-value-kl06",
            Instant::now() + Duration::from_secs(3600),
        );
        let dbg = format!("{td:?}");
        assert!(
            !dbg.contains("secret-token-value-kl06"),
            "TokenData Debug leaked token: {dbg}"
        );
        assert!(
            dbg.contains("<redacted>"),
            "TokenData Debug must contain <redacted>: {dbg}"
        );
        assert_eq!(td.token(), "secret-token-value-kl06");
    }

    #[tokio::test]
    async fn mock_clock_advance_wakes_waiters() {
        let start = Instant::now();
        let clock = Arc::new(MockClock::new(start));

        let clock_clone = clock.clone();
        let handle = tokio::spawn(async move {
            clock_clone.sleep(Duration::from_secs(100)).await;
            true
        });

        tokio::task::yield_now().await;
        assert_eq!(clock.pending_waiters(), 1);

        // Advance only 50s: waiter should still be pending
        clock.advance(Duration::from_secs(50));
        tokio::task::yield_now().await;
        assert_eq!(clock.pending_waiters(), 1);
        assert!(!handle.is_finished());

        // Advance another 50s: reaches 100s, wakes waiter
        clock.advance(Duration::from_secs(50));
        tokio::task::yield_now().await;
        let finished = handle.await.unwrap();
        assert!(finished);
        assert_eq!(clock.pending_waiters(), 0);
    }

    #[test]
    fn fixed_and_zero_jitter_behavior() {
        let zero = ZeroJitter;
        assert_eq!(zero.jitter(Duration::from_secs(30)), Duration::ZERO);

        let fixed = FixedJitter(Duration::from_secs(15));
        assert_eq!(
            fixed.jitter(Duration::from_secs(30)),
            Duration::from_secs(15)
        );
        assert_eq!(
            fixed.jitter(Duration::from_secs(10)),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn token_manager_debug_redacts_tokens() {
        let cfg = OidcConfig::new("https://idp.example/token", "cid", "super-secret-key");
        let mgr = OidcTokenManager::new(cfg);
        let dbg = format!("{mgr:?}");
        assert!(
            !dbg.contains("super-secret-key"),
            "manager Debug leaked secret: {dbg}"
        );
        assert!(dbg.contains("OidcTokenManager"), "manager Debug: {dbg}");
    }
}
