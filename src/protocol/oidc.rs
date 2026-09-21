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
}
