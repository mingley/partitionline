//! Bounded Schema Registry lookup client (companion crate only).
//!
//! [`RegistryClient`] performs read-only lookups against a
//! Confluent-compatible Schema Registry: fetch a schema by global id
//! ([`RegistryClient::get_schema_by_id`]), fetch a subject/version
//! ([`RegistryClient::get_schema_by_subject`]), and resolve the schema
//! references either call returns
//! ([`RegistryClient::fetch_references`]).
//!
//! This module deliberately offers **no** registration, mutation, delete,
//! or compatibility-check API, and it never depends on the core
//! `partitionline` client crate. Transport is a small `GET`-only HTTP/1.1
//! client over `tokio` + `rustls` with its own minimal JSON reader (no
//! `serde`, no `url` crate), mirroring the core crate's OIDC token fetch.
//!
//! # Transport
//!
//! One TCP connection (plus a TLS handshake for `https://`) per attempt,
//! `Connection: close`, no keep-alive pooling, no redirect following, no
//! request body. Responses may use `Content-Length`, `chunked`
//! `Transfer-Encoding`, or close-delimited framing, all capped by
//! [`RegistryClientConfig::max_body_bytes`].
//!
//! # TLS
//!
//! `https://` base URLs use `rustls` with Mozilla webpki roots (or a custom
//! PEM CA bundle via [`RegistryClientConfig::ca_pem`]) and full hostname
//! verification. There is no insecure-verifier escape hatch anywhere in
//! this module.
//!
//! # Authentication
//!
//! [`RegistryAuth`] selects no auth, HTTP Basic, or bearer tokens.
//! Credentials travel only in the `Authorization` header, are rejected in
//! `base_url` userinfo at construction, and are redacted from every
//! [`fmt::Debug`] impl and [`RegistryError`] message here.
//!
//! # Timeouts and retries
//!
//! * `connect_timeout` bounds each TCP connect and each TLS handshake.
//! * `request_timeout` is an overall deadline per public call, including
//!   retries and backoff. When it expires the call fails with
//!   [`RegistryError::Timeout`].
//! * Retried while attempts and the deadline remain: HTTP 429 (honoring
//!   `Retry-After` delay-seconds, capped at the backoff maximum), HTTP
//!   500/502/503/504, and transient transport/timeout failures of an
//!   attempt. Backoff starts at `backoff_initial`, doubles per retry, and
//!   is capped at `backoff_max`.
//! * Never retried: 401/403/404, other 3xx/4xx/5xx statuses, TLS failures,
//!   malformed or oversize responses, and configuration errors.
//! * Attempt counts are bounded by `max_attempts` (1 through 8).
//!
//! # Errors
//!
//! Every failure surfaces as the typed [`RegistryError`]. Messages carry
//! statuses, endpoint descriptions, and registry `error_code`s only;
//! response bodies, URLs, and credentials are never embedded.

use std::fmt;
use std::sync::Arc;
use std::sync::Once;
use std::time::{Duration, Instant};

use base64::Engine as _;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

/// Default per-connect/per-handshake bound.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Default overall deadline per public call, including retries.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Default attempts per call (1 initial + 2 retries).
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Hard ceiling for [`RegistryClientConfig::max_attempts`].
const MAX_ATTEMPTS_LIMIT: u32 = 8;
/// Default first backoff between retries.
const DEFAULT_BACKOFF_INITIAL: Duration = Duration::from_millis(50);
/// Default backoff ceiling (also caps honored `Retry-After` delays).
const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(2);
/// Default cap for response bodies.
const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;
/// Smallest accepted [`RegistryClientConfig::max_body_bytes`].
const MIN_BODY_LIMIT: usize = 1024;
/// Largest accepted [`RegistryClientConfig::max_body_bytes`].
const MAX_BODY_LIMIT: usize = 64 * 1024 * 1024;
/// Default cap for [`RegistryClient::fetch_references`] batch size.
const DEFAULT_MAX_REFERENCES: usize = 64;
/// Largest accepted `max_references`.
const MAX_REFERENCES_LIMIT: usize = 1024;
/// Fixed cap for HTTP response headers (not configurable).
const MAX_HEADER_BYTES: usize = 64 * 1024;
/// Socket read quantum.
const READ_CHUNK: usize = 8192;
/// Fixed cap for JSON nesting depth.
const MAX_JSON_DEPTH: usize = 32;
/// Accept header sent on every lookup.
const SCHEMA_REGISTRY_ACCEPT: &str = "application/vnd.schemaregistry.v1+json";

fn ensure_crypto() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        drop(rustls::crypto::ring::default_provider().install_default());
    });
}

/// Credentials for Schema Registry lookups.
///
/// `Debug` never prints the secret material; see the redaction tests.
#[derive(Clone, Default)]
pub enum RegistryAuth {
    /// No `Authorization` header.
    #[default]
    None,
    /// HTTP Basic: `base64(username:password)` (standard encoding; the
    /// username must not contain `:`).
    Basic {
        /// Registry username (non-empty, no `:` or control characters).
        username: String,
        /// Registry password (no control characters).
        password: String,
    },
    /// HTTP bearer token (non-empty, no whitespace/control characters).
    Bearer {
        /// Opaque token sent as `Authorization: Bearer <token>`.
        token: String,
    },
}

impl fmt::Debug for RegistryAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Basic { .. } => write!(f, "Basic(<redacted>)"),
            Self::Bearer { .. } => write!(f, "Bearer(<redacted>)"),
        }
    }
}

impl RegistryAuth {
    /// HTTP Basic credentials.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Basic {
            username: username.into(),
            password: password.into(),
        }
    }

    /// HTTP bearer token.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::Bearer {
            token: token.into(),
        }
    }

    fn validate(&self) -> Result<(), RegistryError> {
        match self {
            Self::None => Ok(()),
            Self::Basic { username, password } => {
                if username.is_empty() {
                    return Err(RegistryError::invalid_config(
                        "basic auth username must be non-empty",
                    ));
                }
                if username.contains(':') {
                    return Err(RegistryError::invalid_config(
                        "basic auth username must not contain ':'",
                    ));
                }
                if username.chars().any(is_bad_auth_char) || password.chars().any(is_bad_auth_char)
                {
                    return Err(RegistryError::invalid_config(
                        "basic auth credentials must not contain control characters",
                    ));
                }
                Ok(())
            }
            Self::Bearer { token } => {
                if token.is_empty() {
                    return Err(RegistryError::invalid_config(
                        "bearer token must be non-empty",
                    ));
                }
                if token
                    .chars()
                    .any(|c| c.is_whitespace() || is_bad_auth_char(c))
                {
                    return Err(RegistryError::invalid_config(
                        "bearer token must not contain whitespace or control characters",
                    ));
                }
                Ok(())
            }
        }
    }
}

fn is_bad_auth_char(c: char) -> bool {
    c.is_control()
}

/// Builder for [`RegistryClient`].
///
/// `Debug` redacts credentials via [`RegistryAuth`]'s redacted form.
#[derive(Clone)]
pub struct RegistryClientConfig {
    base_url: String,
    auth: RegistryAuth,
    connect_timeout: Duration,
    request_timeout: Duration,
    max_attempts: u32,
    backoff_initial: Duration,
    backoff_max: Duration,
    max_body_bytes: usize,
    max_references: usize,
    ca_pem: Option<Vec<u8>>,
}

impl fmt::Debug for RegistryClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryClientConfig")
            .field("base_url", &self.base_url)
            .field("auth", &self.auth)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("max_attempts", &self.max_attempts)
            .field("backoff_initial", &self.backoff_initial)
            .field("backoff_max", &self.backoff_max)
            .field("max_body_bytes", &self.max_body_bytes)
            .field("max_references", &self.max_references)
            .field("ca_override", &self.ca_pem.is_some())
            .finish()
    }
}

impl RegistryClientConfig {
    /// Start a config for `base_url` (`http://` or `https://`, optional path
    /// prefix, no userinfo). Defaults: 5s connect / 10s request timeouts, 3
    /// attempts, 50ms-2s backoff, 1 MiB bodies, 64 references, no auth.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            auth: RegistryAuth::None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            backoff_initial: DEFAULT_BACKOFF_INITIAL,
            backoff_max: DEFAULT_BACKOFF_MAX,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_references: DEFAULT_MAX_REFERENCES,
            ca_pem: None,
        }
    }

    /// Credentials for the `Authorization` header.
    #[must_use]
    pub fn auth(mut self, auth: RegistryAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Bound for each TCP connect and each TLS handshake (non-zero).
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Overall deadline per public call, including retries (non-zero).
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Attempts per call, 1 through 8 (default 3).
    #[must_use]
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// Retry backoff: first delay and ceiling (both non-zero, initial not
    /// above max). Also caps honored `Retry-After` delays.
    #[must_use]
    pub fn retry_backoff(mut self, initial: Duration, max: Duration) -> Self {
        self.backoff_initial = initial;
        self.backoff_max = max;
        self
    }

    /// Cap for response bodies, 1 KiB through 64 MiB (default 1 MiB).
    #[must_use]
    pub fn max_body_bytes(mut self, limit: usize) -> Self {
        self.max_body_bytes = limit;
        self
    }

    /// Cap for [`RegistryClient::fetch_references`] batch size, 1 through
    /// 1024 (default 64).
    #[must_use]
    pub fn max_references(mut self, limit: usize) -> Self {
        self.max_references = limit;
        self
    }

    /// PEM CA bundle replacing Mozilla webpki roots for `https://`.
    #[must_use]
    pub fn ca_pem(mut self, pem: Vec<u8>) -> Self {
        self.ca_pem = Some(pem);
        self
    }
}

/// A schema fetched by global id (`GET /schemas/ids/{id}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    /// Global schema id from the request path.
    pub id: u32,
    /// Schema text (Avro JSON, Protobuf source, or JSON Schema).
    pub schema: String,
    /// Declared type (`AVRO`, `PROTOBUF`, `JSON`); `None` when the registry
    /// omits it (Avro is the implicit default).
    pub schema_type: Option<String>,
    /// Named references to other subject/versions (empty when absent).
    pub references: Vec<SchemaReference>,
}

/// A schema fetched by subject/version
/// (`GET /subjects/{subject}/versions/{version}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedSchema {
    /// Subject from the registry response.
    pub subject: String,
    /// Resolved version from the registry response.
    pub version: u32,
    /// Global schema id from the registry response.
    pub id: u32,
    /// Schema text.
    pub schema: String,
    /// Declared type; `None` when the registry omits it.
    pub schema_type: Option<String>,
    /// Named references to other subject/versions (empty when absent).
    pub references: Vec<SchemaReference>,
}

/// One named reference from a schema to another subject/version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaReference {
    /// Reference name as used inside the schema text.
    pub name: String,
    /// Referenced subject.
    pub subject: String,
    /// Referenced version.
    pub version: u32,
}

/// Version selector for [`RegistryClient::get_schema_by_subject`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaVersion {
    /// The registry's `latest` version.
    Latest,
    /// A pinned numeric version.
    Pinned(u32),
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latest => write!(f, "latest"),
            Self::Pinned(v) => write!(f, "{v}"),
        }
    }
}

/// Typed lookup failures.
///
/// Messages carry statuses, endpoint descriptions, and registry
/// `error_code`s only. Response bodies, request URLs, and credentials are
/// never embedded; see the redaction tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// HTTP 404: the id, subject, or version does not exist.
    NotFound {
        /// What was looked up, e.g. `schema id 42`.
        lookup: String,
        /// Registry `error_code` (40401/40402/40403) when the error body
        /// parsed; `None` otherwise.
        error_code: Option<i64>,
    },
    /// HTTP 401: missing or rejected credentials. Not retried.
    Unauthorized,
    /// HTTP 403: authenticated but forbidden. Not retried.
    Forbidden,
    /// HTTP 429 on every attempt (attempts exhausted or deadline expired
    /// first — see [`RegistryError::Timeout`).
    RateLimited {
        /// Attempts made before giving up.
        attempts: u32,
    },
    /// HTTP 500/502/503/504 on the final attempt.
    Server {
        /// Status of the final attempt.
        status: u16,
    },
    /// Any other status (3xx — redirects are never followed — and
    /// unlisted 4xx/5xx). Not retried.
    UnexpectedStatus {
        /// Observed status code.
        status: u16,
    },
    /// The 200 body was not UTF-8, not JSON, or not the expected shape.
    Malformed {
        /// Which field or JSON rule failed; never body text.
        what: String,
    },
    /// Headers or body exceeded the configured size bound.
    TooLarge {
        /// Bound that was exceeded, in bytes.
        limit: usize,
    },
    /// The request deadline expired (connect, handshake, read, or retry
    /// backoff ran out of time).
    Timeout,
    /// TLS handshake or certificate verification failed. Not retried.
    Tls(String),
    /// TCP connect/read/write failed after retries.
    Transport(String),
    /// Rejected configuration (URL, credentials, or limits).
    InvalidConfig(String),
}

impl RegistryError {
    fn invalid_config(msg: &str) -> Self {
        Self::InvalidConfig(msg.to_string())
    }

    fn malformed(what: &str) -> Self {
        Self::Malformed {
            what: what.to_string(),
        }
    }

    fn transport(err: std::io::Error) -> Self {
        Self::Transport(err.to_string())
    }

    fn tls(err: std::io::Error) -> Self {
        Self::Tls(err.to_string())
    }

    fn not_found(lookup: &str, body: &[u8]) -> Self {
        Self::NotFound {
            lookup: lookup.to_string(),
            error_code: registry_error_code(body),
        }
    }

    /// Retryable within the attempt and deadline budget: per-attempt
    /// transport/timeout failures only. TLS, auth, mapping, size, JSON,
    /// and config failures are deterministic and never retried.
    fn is_transient(&self) -> bool {
        matches!(self, Self::Timeout | Self::Transport(_))
    }
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { lookup, error_code } => {
                write!(f, "schema registry lookup {lookup} not found")?;
                if let Some(code) = error_code {
                    write!(f, " (error_code {code})")?;
                }
                Ok(())
            }
            Self::Unauthorized => write!(f, "schema registry request unauthorized (HTTP 401)"),
            Self::Forbidden => write!(f, "schema registry request forbidden (HTTP 403)"),
            Self::RateLimited { attempts } => write!(
                f,
                "schema registry rate limited (HTTP 429) after {attempts} attempts"
            ),
            Self::Server { status } => {
                write!(
                    f,
                    "schema registry server error (HTTP {status}) after retries"
                )
            }
            Self::UnexpectedStatus { status } => {
                write!(f, "schema registry unexpected HTTP status {status}")
            }
            Self::Malformed { what } => write!(f, "schema registry malformed response: {what}"),
            Self::TooLarge { limit } => write!(
                f,
                "schema registry response exceeds size limit ({limit} bytes)"
            ),
            Self::Timeout => write!(f, "schema registry request timed out"),
            Self::Tls(detail) => write!(f, "schema registry TLS error: {detail}"),
            Self::Transport(detail) => write!(f, "schema registry transport error: {detail}"),
            Self::InvalidConfig(detail) => {
                write!(f, "schema registry invalid configuration: {detail}")
            }
        }
    }
}

impl std::error::Error for RegistryError {}

/// Validated `base_url` (userinfo-free by construction).
#[derive(Debug, Clone)]
struct ParsedBase {
    /// Original URL, shown in `Debug`; never carries userinfo.
    base_url: String,
    https: bool,
    host: String,
    port: u16,
    /// Path prefix: empty or starting with `/`, never trailing `/`.
    prefix: String,
    default_port: u16,
}

/// Bounded read-only Schema Registry client.
///
/// Build with [`RegistryClientConfig`]; construction validates the URL,
/// credentials, and limits and pre-builds the `rustls` config, so it
/// performs no I/O. `Debug` redacts credentials.
#[derive(Clone)]
pub struct RegistryClient {
    base: ParsedBase,
    auth: RegistryAuth,
    tls: Arc<ClientConfig>,
    connect_timeout: Duration,
    request_timeout: Duration,
    max_attempts: u32,
    backoff_initial: Duration,
    backoff_max: Duration,
    max_body_bytes: usize,
    max_references: usize,
}

impl fmt::Debug for RegistryClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryClient")
            .field("base_url", &self.base.base_url)
            .field("auth", &self.auth)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("max_attempts", &self.max_attempts)
            .field("max_body_bytes", &self.max_body_bytes)
            .field("max_references", &self.max_references)
            .finish()
    }
}

impl RegistryClient {
    /// Build a client from `config` (validates; no I/O).
    pub fn new(config: RegistryClientConfig) -> Result<Self, RegistryError> {
        config.auth.validate()?;
        let base = parse_base_url(&config.base_url)?;
        if config.connect_timeout.is_zero() || config.request_timeout.is_zero() {
            return Err(RegistryError::invalid_config("timeouts must be non-zero"));
        }
        if config.max_attempts == 0 || config.max_attempts > MAX_ATTEMPTS_LIMIT {
            return Err(RegistryError::invalid_config(
                "max_attempts must be 1 through 8",
            ));
        }
        if config.backoff_initial.is_zero() || config.backoff_max.is_zero() {
            return Err(RegistryError::invalid_config(
                "retry backoff bounds must be non-zero",
            ));
        }
        if config.backoff_initial > config.backoff_max {
            return Err(RegistryError::invalid_config(
                "retry backoff initial must not exceed max",
            ));
        }
        if config.max_body_bytes < MIN_BODY_LIMIT || config.max_body_bytes > MAX_BODY_LIMIT {
            return Err(RegistryError::invalid_config(
                "max_body_bytes must be 1 KiB through 64 MiB",
            ));
        }
        if config.max_references == 0 || config.max_references > MAX_REFERENCES_LIMIT {
            return Err(RegistryError::invalid_config(
                "max_references must be 1 through 1024",
            ));
        }
        if base.https {
            // Fail fast (no I/O) when the host can never verify as a TLS name.
            ServerName::try_from(base.host.clone()).map_err(|_| {
                RegistryError::invalid_config("host is not a valid TLS server name")
            })?;
        }
        let tls = tls_config(config.ca_pem.as_deref())?;
        Ok(Self {
            base,
            auth: config.auth,
            tls,
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_attempts: config.max_attempts,
            backoff_initial: config.backoff_initial,
            backoff_max: config.backoff_max,
            max_body_bytes: config.max_body_bytes,
            max_references: config.max_references,
        })
    }

    /// The configured base URL (userinfo-free by construction).
    pub fn base_url(&self) -> &str {
        &self.base.base_url
    }

    /// Fetch a schema by global id (`GET /schemas/ids/{id}`).
    pub async fn get_schema_by_id(&self, id: u32) -> Result<RegisteredSchema, RegistryError> {
        let path = format!("{prefix}/schemas/ids/{id}", prefix = self.base.prefix);
        let lookup = format!("schema id {id}");
        let body = self.get_200_body(&path, &lookup).await?;
        parse_registered_schema(id, &body)
    }

    /// Fetch a subject/version (`GET /subjects/{subject}/versions/{version}`).
    ///
    /// The subject is percent-encoded as one path segment.
    pub async fn get_schema_by_subject(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<VersionedSchema, RegistryError> {
        let encoded = percent_encode_segment(subject);
        let path = format!(
            "{prefix}/subjects/{encoded}/versions/{version}",
            prefix = self.base.prefix,
        );
        let lookup = format!("subject '{subject}' version {version}");
        let body = self.get_200_body(&path, &lookup).await?;
        parse_versioned_schema(&body)
    }

    /// Fetch every schema in `references` (one `GET` per entry, in order).
    ///
    /// Each fetch gets its own full `request_timeout`; the batch is bounded
    /// by `max_references`. The first failure aborts the batch.
    pub async fn fetch_references(
        &self,
        references: &[SchemaReference],
    ) -> Result<Vec<VersionedSchema>, RegistryError> {
        if references.len() > self.max_references {
            return Err(RegistryError::malformed(
                "reference list exceeds max_references",
            ));
        }
        let mut out = Vec::with_capacity(references.len());
        for reference in references {
            let schema = self
                .get_schema_by_subject(
                    reference.subject.as_str(),
                    SchemaVersion::Pinned(reference.version),
                )
                .await?;
            out.push(schema);
        }
        Ok(out)
    }

    /// `GET` with retries; returns the 200 body or a terminal error.
    async fn get_200_body(&self, path: &str, lookup: &str) -> Result<Vec<u8>, RegistryError> {
        let deadline = Instant::now() + self.request_timeout;
        let mut backoff = self.backoff_initial;
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            match self.get_once(path, deadline).await {
                Ok(resp) => match resp.status {
                    200 => return Ok(resp.body),
                    404 => return Err(RegistryError::not_found(lookup, &resp.body)),
                    401 => return Err(RegistryError::Unauthorized),
                    403 => return Err(RegistryError::Forbidden),
                    429 => {
                        if attempt >= self.max_attempts {
                            return Err(RegistryError::RateLimited { attempts: attempt });
                        }
                        let delay = resp
                            .retry_after_secs
                            .map(Duration::from_secs)
                            .unwrap_or(backoff)
                            .min(self.backoff_max);
                        if !sleep_within(deadline, delay).await {
                            return Err(RegistryError::Timeout);
                        }
                        backoff = backoff.saturating_mul(2).min(self.backoff_max);
                    }
                    500 | 502 | 503 | 504 => {
                        if attempt >= self.max_attempts {
                            return Err(RegistryError::Server {
                                status: resp.status,
                            });
                        }
                        if !sleep_within(deadline, backoff).await {
                            return Err(RegistryError::Timeout);
                        }
                        backoff = backoff.saturating_mul(2).min(self.backoff_max);
                    }
                    status => return Err(RegistryError::UnexpectedStatus { status }),
                },
                Err(err) if err.is_transient() && attempt < self.max_attempts => {
                    if !sleep_within(deadline, backoff).await {
                        return Err(RegistryError::Timeout);
                    }
                    backoff = backoff.saturating_mul(2).min(self.backoff_max);
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// One `GET` attempt: connect, optional TLS handshake, roundtrip.
    async fn get_once(&self, path: &str, deadline: Instant) -> Result<HttpResponse, RegistryError> {
        let left = time_left(deadline).ok_or(RegistryError::Timeout)?;
        let addr = connect_addr(&self.base.host, self.base.port);
        let stream = match timeout(left.min(self.connect_timeout), TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => return Err(RegistryError::transport(err)),
            Err(_) => return Err(RegistryError::Timeout),
        };
        // Contains the Authorization header; never logged.
        let request = self.request_bytes(path);
        if self.base.https {
            let left = time_left(deadline).ok_or(RegistryError::Timeout)?;
            let name = ServerName::try_from(self.base.host.clone()).map_err(|_| {
                RegistryError::invalid_config("host is not a valid TLS server name")
            })?;
            let connector = TlsConnector::from(self.tls.clone());
            let mut tls = match timeout(
                left.min(self.connect_timeout),
                connector.connect(name, stream),
            )
            .await
            {
                Ok(Ok(tls)) => tls,
                Ok(Err(err)) => return Err(RegistryError::tls(err)),
                Err(_) => return Err(RegistryError::Timeout),
            };
            http_roundtrip(&mut tls, request.as_bytes(), deadline, self.max_body_bytes).await
        } else {
            let mut plain = stream;
            http_roundtrip(
                &mut plain,
                request.as_bytes(),
                deadline,
                self.max_body_bytes,
            )
            .await
        }
    }

    /// Render the `GET` request bytes (holds secrets; never logged).
    fn request_bytes(&self, path: &str) -> String {
        let host = host_header(&self.base.host, self.base.port, self.base.default_port);
        let mut req =
            format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nAccept: {SCHEMA_REGISTRY_ACCEPT}\r\n");
        match &self.auth {
            RegistryAuth::None => {}
            RegistryAuth::Basic { username, password } => {
                let raw = format!("{username}:{password}");
                let encoded = base64::engine::general_purpose::STANDARD.encode(raw.as_bytes());
                req.push_str("Authorization: Basic ");
                req.push_str(&encoded);
                req.push_str("\r\n");
            }
            RegistryAuth::Bearer { token } => {
                req.push_str("Authorization: Bearer ");
                req.push_str(token);
                req.push_str("\r\n");
            }
        }
        req.push_str("Connection: close\r\n\r\n");
        req
    }
}

/// Sleep `delay` unless the deadline expires first.
async fn sleep_within(deadline: Instant, delay: Duration) -> bool {
    let Some(left) = deadline.checked_duration_since(Instant::now()) else {
        return false;
    };
    if left.is_zero() {
        return false;
    }
    tokio::time::sleep(delay.min(left)).await;
    true
}

fn time_left(deadline: Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
}

fn tls_config(ca_pem: Option<&[u8]>) -> Result<Arc<ClientConfig>, RegistryError> {
    ensure_crypto();
    let mut roots = RootCertStore::empty();
    match ca_pem {
        Some(pem) => {
            let certs = CertificateDer::pem_slice_iter(pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| {
                    RegistryError::invalid_config("ca_pem does not parse as PEM certificates")
                })?;
            for cert in certs {
                roots.add(cert).map_err(|_| {
                    RegistryError::invalid_config("ca_pem holds an unusable CA certificate")
                })?;
            }
        }
        None => {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
    }
    if roots.is_empty() {
        return Err(RegistryError::invalid_config("TLS CA store is empty"));
    }
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

fn parse_base_url(url: &str) -> Result<ParsedBase, RegistryError> {
    let (https, rest, default_port) = if let Some(rest) = url.strip_prefix("https://") {
        (true, rest, 443)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (false, rest, 80)
    } else {
        return Err(RegistryError::invalid_config(
            "base_url must start with http:// or https://",
        ));
    };
    let (authority, path) = match rest.split_once('/') {
        Some((authority, tail)) => (authority, format!("/{tail}")),
        None => (rest, String::new()),
    };
    if authority.is_empty() {
        return Err(RegistryError::invalid_config("base_url is missing a host"));
    }
    if authority.contains('@') {
        return Err(RegistryError::invalid_config(
            "base_url must not contain userinfo; pass credentials via RegistryAuth",
        ));
    }
    let (host, port) = parse_authority(authority, default_port)?;
    if host.is_empty() || host.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(RegistryError::invalid_config(
            "base_url holds an invalid host",
        ));
    }
    if path.contains(['?', '#']) || path.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(RegistryError::invalid_config(
            "base_url path must not hold a query, fragment, or whitespace",
        ));
    }
    let prefix = path.trim_end_matches('/').to_string();
    Ok(ParsedBase {
        base_url: url.to_string(),
        https,
        host,
        port,
        prefix,
        default_port,
    })
}

fn parse_authority(authority: &str, default_port: u16) -> Result<(String, u16), RegistryError> {
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .ok_or_else(|| RegistryError::invalid_config("base_url holds a bad IPv6 host"))?;
        let port = match after.strip_prefix(':') {
            Some(port) if !port.is_empty() => parse_port(port)?,
            Some(_) => {
                return Err(RegistryError::invalid_config(
                    "base_url holds an empty port",
                ));
            }
            None if after.is_empty() => default_port,
            None => {
                return Err(RegistryError::invalid_config(
                    "base_url holds a bad IPv6 host",
                ));
            }
        };
        return Ok((host.to_string(), port));
    }
    if let Some((host, port)) = authority.rsplit_once(':') {
        if host.is_empty() {
            return Err(RegistryError::invalid_config("base_url is missing a host"));
        }
        return Ok((host.to_string(), parse_port(port)?));
    }
    Ok((authority.to_string(), default_port))
}

fn parse_port(text: &str) -> Result<u16, RegistryError> {
    text.parse()
        .map_err(|_| RegistryError::invalid_config("base_url holds an invalid port"))
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

/// Percent-encode one path segment (unreserved chars pass through).
fn percent_encode_segment(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(char::from(*byte));
            }
            other => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
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

/// Parsed HTTP response head plus bounded body.
struct HttpResponse {
    status: u16,
    retry_after_secs: Option<u64>,
    body: Vec<u8>,
}

async fn http_roundtrip<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    stream: &mut S,
    request: &[u8],
    deadline: Instant,
    max_body: usize,
) -> Result<HttpResponse, RegistryError> {
    let left = time_left(deadline).ok_or(RegistryError::Timeout)?;
    match timeout(left, stream.write_all(request)).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return Err(RegistryError::transport(err)),
        Err(_) => return Err(RegistryError::Timeout),
    }
    read_http_response(stream, deadline, max_body).await
}

async fn read_http_response<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    deadline: Instant,
    max_body: usize,
) -> Result<HttpResponse, RegistryError> {
    let mut buf = Vec::new();
    let end = loop {
        if let Some(end) = find_header_end(&buf) {
            break end;
        }
        if buf.len() > MAX_HEADER_BYTES {
            return Err(RegistryError::TooLarge {
                limit: MAX_HEADER_BYTES,
            });
        }
        let left = time_left(deadline).ok_or(RegistryError::Timeout)?;
        let mut tmp = [0u8; READ_CHUNK];
        let n = match timeout(left, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => {
                return Err(RegistryError::malformed("truncated response headers"));
            }
            Ok(Ok(n)) => n,
            Ok(Err(err)) => return Err(RegistryError::transport(err)),
            Err(_) => return Err(RegistryError::Timeout),
        };
        let chunk = tmp
            .get(..n)
            .ok_or_else(|| RegistryError::malformed("short socket read"))?;
        buf.extend_from_slice(chunk);
    };

    let head = buf
        .get(..end)
        .ok_or_else(|| RegistryError::malformed("truncated response headers"))?;
    let status = parse_status(head)?;
    let retry_after_secs = parse_retry_after(head);

    if is_chunked(head)? {
        return read_chunked_body(stream, &mut buf, end, status, deadline, max_body).await;
    }
    if let Some(content_len) = parse_content_length(head)? {
        if content_len > max_body {
            return Err(RegistryError::TooLarge { limit: max_body });
        }
        while buf.len().saturating_sub(end) < content_len {
            if buf.len() > max_body.saturating_add(MAX_HEADER_BYTES) {
                return Err(RegistryError::TooLarge { limit: max_body });
            }
            let left = time_left(deadline).ok_or(RegistryError::Timeout)?;
            let mut tmp = [0u8; READ_CHUNK];
            let n = match timeout(left, stream.read(&mut tmp)).await {
                Ok(Ok(0)) => {
                    return Err(RegistryError::malformed("truncated response body"));
                }
                Ok(Ok(n)) => n,
                Ok(Err(err)) => return Err(RegistryError::transport(err)),
                Err(_) => return Err(RegistryError::Timeout),
            };
            let chunk = tmp
                .get(..n)
                .ok_or_else(|| RegistryError::malformed("short socket read"))?;
            buf.extend_from_slice(chunk);
        }
        let body_end = end.saturating_add(content_len);
        let body = buf
            .get(end..body_end)
            .ok_or_else(|| RegistryError::malformed("truncated response body"))?
            .to_vec();
        return Ok(HttpResponse {
            status,
            retry_after_secs,
            body,
        });
    }

    // Neither chunked nor Content-Length: read until close.
    loop {
        if buf.len().saturating_sub(end) > max_body {
            return Err(RegistryError::TooLarge { limit: max_body });
        }
        let left = time_left(deadline).ok_or(RegistryError::Timeout)?;
        let mut tmp = [0u8; READ_CHUNK];
        let n = match timeout(left, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(err)) => return Err(RegistryError::transport(err)),
            Err(_) => return Err(RegistryError::Timeout),
        };
        let chunk = tmp
            .get(..n)
            .ok_or_else(|| RegistryError::malformed("short socket read"))?;
        buf.extend_from_slice(chunk);
    }
    let body = buf
        .get(end..)
        .ok_or_else(|| RegistryError::malformed("truncated response body"))?
        .to_vec();
    Ok(HttpResponse {
        status,
        retry_after_secs,
        body,
    })
}

async fn read_chunked_body<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    end: usize,
    status: u16,
    deadline: Instant,
    max_body: usize,
) -> Result<HttpResponse, RegistryError> {
    loop {
        if buf.len() > max_body.saturating_add(MAX_HEADER_BYTES) {
            return Err(RegistryError::TooLarge { limit: max_body });
        }
        let raw = buf.get(end..).unwrap_or(&[]);
        if let Some(body) = try_decode_chunked_body(raw, max_body)? {
            return Ok(HttpResponse {
                status,
                retry_after_secs: None,
                body,
            });
        }
        let left = time_left(deadline).ok_or(RegistryError::Timeout)?;
        let mut tmp = [0u8; READ_CHUNK];
        let n = match timeout(left, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => {
                return Err(RegistryError::malformed("truncated chunked body"));
            }
            Ok(Ok(n)) => n,
            Ok(Err(err)) => return Err(RegistryError::transport(err)),
            Err(_) => return Err(RegistryError::Timeout),
        };
        let chunk = tmp
            .get(..n)
            .ok_or_else(|| RegistryError::malformed("short socket read"))?;
        buf.extend_from_slice(chunk);
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i.saturating_add(4))
}

fn head_text(head: &[u8]) -> Result<&str, RegistryError> {
    std::str::from_utf8(head).map_err(|_| RegistryError::malformed("response headers are not utf8"))
}

fn parse_status(head: &[u8]) -> Result<u16, RegistryError> {
    let text = head_text(head)?;
    let line = text
        .split("\r\n")
        .next()
        .ok_or_else(|| RegistryError::malformed("empty status line"))?;
    let mut parts = line.split_whitespace();
    let version = parts
        .next()
        .ok_or_else(|| RegistryError::malformed("bad status line"))?;
    if !version.starts_with("HTTP/") {
        return Err(RegistryError::malformed("bad status line"));
    }
    let code = parts
        .next()
        .ok_or_else(|| RegistryError::malformed("bad status line"))?;
    code.parse()
        .map_err(|_| RegistryError::malformed("bad status code"))
}

fn parse_content_length(head: &[u8]) -> Result<Option<usize>, RegistryError> {
    let text = head_text(head)?;
    let mut len = None;
    for line in text.split("\r\n") {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.eq_ignore_ascii_case("content-length") {
            let n = value
                .trim()
                .parse::<usize>()
                .map_err(|_| RegistryError::malformed("bad content-length"))?;
            if let Some(prev) = len {
                if prev != n {
                    return Err(RegistryError::malformed("conflicting content-length"));
                }
            } else {
                len = Some(n);
            }
        }
    }
    Ok(len)
}

fn is_chunked(head: &[u8]) -> Result<bool, RegistryError> {
    let text = head_text(head)?;
    for line in text.split("\r\n") {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.eq_ignore_ascii_case("transfer-encoding") {
            for part in value.split(',') {
                if part.trim().eq_ignore_ascii_case("chunked") {
                    return Ok(true);
                }
            }
            return Err(RegistryError::malformed("unsupported transfer-encoding"));
        }
    }
    Ok(false)
}

/// `Retry-After` delay-seconds; HTTP-date and bad values yield `None`
/// (the caller falls back to exponential backoff).
fn parse_retry_after(head: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(head).ok()?;
    for line in text.split("\r\n") {
        let (key, value) = line.split_once(':')?;
        if key.eq_ignore_ascii_case("retry-after") {
            let trimmed = value.trim();
            if trimmed.is_empty() || !trimmed.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            return trimmed.parse::<u64>().ok();
        }
    }
    None
}

/// Decode one possibly-partial chunked body: `Ok(None)` means more bytes
/// are needed. Mirrors the core crate's strict chunked reader.
fn try_decode_chunked_body(raw: &[u8], max_body: usize) -> Result<Option<Vec<u8>>, RegistryError> {
    let mut cursor = 0usize;
    let mut decoded = Vec::new();
    loop {
        let remaining = raw.get(cursor..).unwrap_or(&[]);
        let Some(pos) = remaining.windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let line = remaining.get(..pos).unwrap_or(&[]);
        let line_str =
            std::str::from_utf8(line).map_err(|_| RegistryError::malformed("bad chunk size"))?;
        let size_str = match line_str.split_once(';') {
            Some((size, _)) => size.trim(),
            None => line_str.trim(),
        };
        if size_str.is_empty() || !size_str.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(RegistryError::malformed("bad chunk size"));
        }
        let chunk_size = usize::from_str_radix(size_str, 16)
            .map_err(|_| RegistryError::malformed("bad chunk size"))?;
        cursor = cursor.saturating_add(pos).saturating_add(2);
        if chunk_size == 0 {
            let trailer = raw.get(cursor..).unwrap_or(&[]);
            if trailer.starts_with(b"\r\n") {
                return Ok(Some(decoded));
            }
            if trailer.windows(4).any(|w| w == b"\r\n\r\n") {
                return Ok(Some(decoded));
            }
            return Ok(None);
        }
        if chunk_size > max_body || decoded.len().saturating_add(chunk_size) > max_body {
            return Err(RegistryError::TooLarge { limit: max_body });
        }
        let chunk_end = cursor.saturating_add(chunk_size);
        let crlf_end = chunk_end.saturating_add(2);
        if raw.len() < crlf_end {
            return Ok(None);
        }
        let data = raw
            .get(cursor..chunk_end)
            .ok_or_else(|| RegistryError::malformed("truncated chunk"))?;
        let crlf = raw
            .get(chunk_end..crlf_end)
            .ok_or_else(|| RegistryError::malformed("truncated chunk"))?;
        if crlf != b"\r\n" {
            return Err(RegistryError::malformed("bad chunk framing"));
        }
        decoded.extend_from_slice(data);
        cursor = crlf_end;
    }
}

/// Minimal JSON value for registry responses (no `serde` dependency).
#[derive(Debug, Clone)]
enum JsonValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    /// Short shape name for mismatch errors. Never echoes string or
    /// container contents (bodies can be large and must stay out of errors).
    fn describe(&self) -> String {
        match self {
            Self::Null => "null".to_string(),
            Self::Bool(value) => format!("boolean {value}"),
            Self::Int(value) => format!("integer {value}"),
            Self::Float(value) => format!("number {value}"),
            Self::Str(text) => format!("string of {} bytes", text.len()),
            Self::Array(items) => format!("array of {} items", items.len()),
            Self::Object(entries) => format!("object with {} keys", entries.len()),
        }
    }
}

/// Strict small JSON reader: duplicate keys, trailing data, bad escapes,
/// and over-deep nesting are all errors.
struct JsonParser<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            buf: text.as_bytes(),
            pos: 0,
        }
    }

    fn parse(&mut self) -> Result<JsonValue, RegistryError> {
        let value = self.parse_value(0)?;
        self.skip_ws();
        if self.pos != self.buf.len() {
            return Err(RegistryError::malformed("json trailing data"));
        }
        Ok(value)
    }

    fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos = self.pos.saturating_add(1);
        }
    }

    fn parse_value(&mut self, depth: usize) -> Result<JsonValue, RegistryError> {
        if depth > MAX_JSON_DEPTH {
            return Err(RegistryError::malformed("json nesting too deep"));
        }
        self.skip_ws();
        match self.peek() {
            Some(b'n') => self.parse_literal("null", JsonValue::Null),
            Some(b't') => self.parse_literal("true", JsonValue::Bool(true)),
            Some(b'f') => self.parse_literal("false", JsonValue::Bool(false)),
            Some(b'"') => Ok(JsonValue::Str(self.parse_string()?)),
            Some(b'[') => {
                self.pos = self.pos.saturating_add(1);
                self.parse_array(depth)
            }
            Some(b'{') => {
                self.pos = self.pos.saturating_add(1);
                self.parse_object(depth)
            }
            Some(b'-') | Some(b'0'..=b'9') => self.parse_number(),
            _ => Err(RegistryError::malformed("json unexpected value")),
        }
    }

    fn parse_literal(&mut self, word: &str, value: JsonValue) -> Result<JsonValue, RegistryError> {
        let end = self.pos.saturating_add(word.len());
        if self.buf.get(self.pos..end) != Some(word.as_bytes()) {
            return Err(RegistryError::malformed("json bad literal"));
        }
        self.pos = end;
        Ok(value)
    }

    fn parse_string(&mut self) -> Result<String, RegistryError> {
        // Caller peeked the opening quote.
        self.pos = self.pos.saturating_add(1);
        let mut out: Vec<u8> = Vec::new();
        loop {
            let byte = self
                .peek()
                .ok_or_else(|| RegistryError::malformed("json unterminated string"))?;
            match byte {
                b'"' => {
                    self.pos = self.pos.saturating_add(1);
                    return String::from_utf8(out)
                        .map_err(|_| RegistryError::malformed("json bad string escape"));
                }
                b'\\' => {
                    self.pos = self.pos.saturating_add(1);
                    self.parse_escape(&mut out)?;
                }
                0x00..=0x1f => {
                    return Err(RegistryError::malformed("json control character in string"));
                }
                _ => {
                    out.push(byte);
                    self.pos = self.pos.saturating_add(1);
                }
            }
        }
    }

    fn parse_escape(&mut self, out: &mut Vec<u8>) -> Result<(), RegistryError> {
        let bad = || RegistryError::malformed("json bad string escape");
        let byte = self.peek().ok_or_else(bad)?;
        self.pos = self.pos.saturating_add(1);
        match byte {
            b'"' => out.push(b'"'),
            b'\\' => out.push(b'\\'),
            b'/' => out.push(b'/'),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'u' => {
                let high = self.parse_hex4().ok_or_else(bad)?;
                if (0xd800..0xdc00).contains(&high) {
                    // High surrogate: must be followed by `\uDC00-\uDFFF`.
                    if self.buf.get(self.pos..self.pos.saturating_add(2)) != Some(b"\\u".as_slice())
                    {
                        return Err(bad());
                    }
                    self.pos = self.pos.saturating_add(2);
                    let low = self.parse_hex4().ok_or_else(bad)?;
                    if !(0xdc00..0xe000).contains(&low) {
                        return Err(bad());
                    }
                    let code = 0x1_0000u32
                        .saturating_add((high - 0xd800) << 10)
                        .saturating_add(low - 0xdc00);
                    let ch = char::from_u32(code).ok_or_else(bad)?;
                    let mut tmp = [0u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                } else if (0xdc00..0xe000).contains(&high) {
                    return Err(bad());
                } else {
                    let ch = char::from_u32(high).ok_or_else(bad)?;
                    let mut tmp = [0u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                }
            }
            _ => return Err(bad()),
        }
        Ok(())
    }

    fn parse_hex4(&mut self) -> Option<u32> {
        let end = self.pos.saturating_add(4);
        let digits = self.buf.get(self.pos..end)?;
        let mut value: u32 = 0;
        for byte in digits {
            value = value
                .saturating_mul(16)
                .saturating_add(char::from(*byte).to_digit(16)?);
        }
        self.pos = end;
        Some(value)
    }

    fn parse_number(&mut self) -> Result<JsonValue, RegistryError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos = self.pos.saturating_add(1);
        }
        match self.peek() {
            Some(b'0') => {
                self.pos = self.pos.saturating_add(1);
            }
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.pos = self.pos.saturating_add(1);
                }
            }
            _ => return Err(RegistryError::malformed("json bad number")),
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.pos = self.pos.saturating_add(1);
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(RegistryError::malformed("json bad number"));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos = self.pos.saturating_add(1);
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            is_float = true;
            self.pos = self.pos.saturating_add(1);
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos = self.pos.saturating_add(1);
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(RegistryError::malformed("json bad number"));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos = self.pos.saturating_add(1);
            }
        }
        let text = self
            .buf
            .get(start..self.pos)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .ok_or_else(|| RegistryError::malformed("json bad number"))?;
        if is_float {
            let value = text
                .parse::<f64>()
                .map_err(|_| RegistryError::malformed("json bad number"))?;
            if !value.is_finite() {
                return Err(RegistryError::malformed("json number out of range"));
            }
            Ok(JsonValue::Float(value))
        } else {
            text.parse::<i64>()
                .map(JsonValue::Int)
                .map_err(|_| RegistryError::malformed("json number out of range"))
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<JsonValue, RegistryError> {
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos = self.pos.saturating_add(1);
            return Ok(JsonValue::Array(items));
        }
        loop {
            items.push(self.parse_value(depth.saturating_add(1))?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos = self.pos.saturating_add(1);
                }
                Some(b']') => {
                    self.pos = self.pos.saturating_add(1);
                    return Ok(JsonValue::Array(items));
                }
                _ => return Err(RegistryError::malformed("json bad array")),
            }
        }
    }

    fn parse_object(&mut self, depth: usize) -> Result<JsonValue, RegistryError> {
        let mut entries: Vec<(String, JsonValue)> = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos = self.pos.saturating_add(1);
            return Ok(JsonValue::Object(entries));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err(RegistryError::malformed("json object key not a string"));
            }
            let key = self.parse_string()?;
            self.skip_ws();
            if self.peek() != Some(b':') {
                return Err(RegistryError::malformed("json object missing colon"));
            }
            self.pos = self.pos.saturating_add(1);
            let value = self.parse_value(depth.saturating_add(1))?;
            if entries.iter().any(|(known, _)| known == &key) {
                return Err(RegistryError::malformed("json duplicate key"));
            }
            entries.push((key, value));
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos = self.pos.saturating_add(1);
                }
                Some(b'}') => {
                    self.pos = self.pos.saturating_add(1);
                    return Ok(JsonValue::Object(entries));
                }
                _ => return Err(RegistryError::malformed("json bad object")),
            }
        }
    }
}

fn object_entries<'a>(
    what: &str,
    value: &'a JsonValue,
) -> Result<&'a [(String, JsonValue)], RegistryError> {
    match value {
        JsonValue::Object(entries) => Ok(entries),
        _ => Err(RegistryError::malformed(
            format!(
                "{what}: response is not a json object (found {})",
                value.describe()
            )
            .as_str(),
        )),
    }
}

fn required_field<'a>(
    what: &str,
    entries: &'a [(String, JsonValue)],
    key: &str,
) -> Result<&'a JsonValue, RegistryError> {
    entries
        .iter()
        .find(|(known, _)| known == key)
        .map(|(_, value)| value)
        .ok_or_else(|| RegistryError::malformed(format!("{what}: missing '{key}'").as_str()))
}

fn optional_field<'a>(entries: &'a [(String, JsonValue)], key: &str) -> Option<&'a JsonValue> {
    entries
        .iter()
        .find(|(known, _)| known == key)
        .map(|(_, value)| value)
}

fn as_str(what: &str, key: &str, value: &JsonValue) -> Result<String, RegistryError> {
    match value {
        JsonValue::Str(text) => Ok(text.clone()),
        _ => Err(RegistryError::malformed(
            format!(
                "{what}: '{key}' is not a string (found {})",
                value.describe()
            )
            .as_str(),
        )),
    }
}

fn as_opt_str(what: &str, key: &str, value: &JsonValue) -> Result<Option<String>, RegistryError> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Str(text) => Ok(Some(text.clone())),
        _ => Err(RegistryError::malformed(
            format!(
                "{what}: '{key}' is not a string or null (found {})",
                value.describe()
            )
            .as_str(),
        )),
    }
}

fn as_u32(what: &str, key: &str, value: &JsonValue) -> Result<u32, RegistryError> {
    match value {
        JsonValue::Int(n) => u32::try_from(*n).map_err(|_| {
            RegistryError::malformed(
                format!(
                    "{what}: '{key}' is out of range (found {})",
                    value.describe()
                )
                .as_str(),
            )
        }),
        _ => Err(RegistryError::malformed(
            format!(
                "{what}: '{key}' is not an integer (found {})",
                value.describe()
            )
            .as_str(),
        )),
    }
}

fn parse_registered_schema(id: u32, body: &[u8]) -> Result<RegisteredSchema, RegistryError> {
    let what = "schemas/ids response";
    let text =
        std::str::from_utf8(body).map_err(|_| RegistryError::malformed("response is not utf8"))?;
    let value = JsonParser::new(text).parse()?;
    let entries = object_entries(what, &value)?;
    let schema = as_str(what, "schema", required_field(what, entries, "schema")?)?;
    let schema_type = optional_field(entries, "schemaType")
        .map(|value| as_opt_str(what, "schemaType", value))
        .transpose()?
        .flatten();
    let references = parse_references(what, optional_field(entries, "references"))?;
    Ok(RegisteredSchema {
        id,
        schema,
        schema_type,
        references,
    })
}

fn parse_versioned_schema(body: &[u8]) -> Result<VersionedSchema, RegistryError> {
    let what = "subjects/versions response";
    let text =
        std::str::from_utf8(body).map_err(|_| RegistryError::malformed("response is not utf8"))?;
    let value = JsonParser::new(text).parse()?;
    let entries = object_entries(what, &value)?;
    let subject = as_str(what, "subject", required_field(what, entries, "subject")?)?;
    let version = as_u32(what, "version", required_field(what, entries, "version")?)?;
    let id = as_u32(what, "id", required_field(what, entries, "id")?)?;
    let schema = as_str(what, "schema", required_field(what, entries, "schema")?)?;
    let schema_type = optional_field(entries, "schemaType")
        .map(|value| as_opt_str(what, "schemaType", value))
        .transpose()?
        .flatten();
    let references = parse_references(what, optional_field(entries, "references"))?;
    Ok(VersionedSchema {
        subject,
        version,
        id,
        schema,
        schema_type,
        references,
    })
}

fn parse_references(
    what: &str,
    value: Option<&JsonValue>,
) -> Result<Vec<SchemaReference>, RegistryError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let items = match value {
        JsonValue::Array(items) => items,
        _ => {
            return Err(RegistryError::malformed(
                format!(
                    "{what}: 'references' is not an array (found {})",
                    value.describe()
                )
                .as_str(),
            ));
        }
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let entries = object_entries(format!("{what}: reference").as_str(), item)?;
        let name = as_str(
            what,
            "references[].name",
            required_field(what, entries, "name")?,
        )?;
        let subject = as_str(
            what,
            "references[].subject",
            required_field(what, entries, "subject")?,
        )?;
        let version = as_u32(
            what,
            "references[].version",
            required_field(what, entries, "version")?,
        )?;
        out.push(SchemaReference {
            name,
            subject,
            version,
        });
    }
    Ok(out)
}

/// Best-effort registry `error_code` from an error body; never fails (status
/// mapping already decided the outcome, so body problems yield `None`).
fn registry_error_code(body: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(body).ok()?;
    let value = JsonParser::new(text).parse().ok()?;
    let entries = match &value {
        JsonValue::Object(entries) => entries,
        _ => return None,
    };
    match optional_field(entries, "error_code")? {
        JsonValue::Int(code) => Some(*code),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    const SECRET_PASSWORD: &str = "s3cr3t-pw-9z";
    const SECRET_TOKEN: &str = "tok-abc-xyz-789";

    fn client_for(url: &str) -> RegistryClient {
        RegistryClient::new(RegistryClientConfig::new(url)).unwrap()
    }

    #[test]
    fn base_url_accepts_http_https_forms() {
        for (url, https, host, port, prefix) in [
            ("http://reg:8081", false, "reg", 8081, ""),
            ("https://reg", true, "reg", 443, ""),
            ("http://reg", false, "reg", 80, ""),
            (
                "http://127.0.0.1:8081/pfx",
                false,
                "127.0.0.1",
                8081,
                "/pfx",
            ),
            (
                "http://127.0.0.1:8081/pfx/",
                false,
                "127.0.0.1",
                8081,
                "/pfx",
            ),
            ("http://[::1]:8081", false, "::1", 8081, ""),
            ("https://[::1]", true, "::1", 443, ""),
            ("http://reg:1/a/b", false, "reg", 1, "/a/b"),
        ] {
            let base = parse_base_url(url).unwrap();
            assert_eq!(base.https, https, "{url}");
            assert_eq!(base.host, host, "{url}");
            assert_eq!(base.port, port, "{url}");
            assert_eq!(base.prefix, prefix, "{url}");
        }
    }

    #[test]
    fn base_url_rejects_bad_input() {
        let userinfo = format!("https://alice:{SECRET_PASSWORD}@reg:8081");
        for url in [
            "ftp://reg",
            "reg:8081",
            "",
            "http://",
            "http:///path",
            "http://user:pass@reg",
            userinfo.as_str(),
            "http://reg:notaport",
            "http://reg:",
            "http://reg:99999",
            "http://[::1",
            "http://[::1]:",
            "http://[::1]x",
            "http://re g/",
            "http://reg/a?b=c",
            "http://reg/a#b",
            "http://reg/a b",
        ] {
            let err = parse_base_url(url).unwrap_err();
            assert!(
                matches!(err, RegistryError::InvalidConfig(_)),
                "{url}: {err:?}"
            );
            // The rejected URL is never echoed: it may carry a password.
            let shown = format!("{err}");
            assert!(!shown.contains("user:pass"), "{url}: {shown}");
            assert!(!shown.contains("alice"), "{url}: {shown}");
            assert!(!shown.contains(SECRET_PASSWORD), "{url}: {shown}");
            if !url.is_empty() {
                assert!(!shown.contains(url), "{url}: {shown}");
            }
        }
    }

    #[test]
    fn config_rejects_bad_limits() {
        for build in [
            RegistryClientConfig::new("http://r").connect_timeout(Duration::ZERO),
            RegistryClientConfig::new("http://r").request_timeout(Duration::ZERO),
            RegistryClientConfig::new("http://r").max_attempts(0),
            RegistryClientConfig::new("http://r").max_attempts(9),
            RegistryClientConfig::new("http://r")
                .retry_backoff(Duration::ZERO, Duration::from_secs(1)),
            RegistryClientConfig::new("http://r")
                .retry_backoff(Duration::from_secs(2), Duration::from_secs(1)),
            RegistryClientConfig::new("http://r").max_body_bytes(512),
            RegistryClientConfig::new("http://r").max_body_bytes(128 * 1024 * 1024),
            RegistryClientConfig::new("http://r").max_references(0),
            RegistryClientConfig::new("http://r").max_references(2048),
        ] {
            let err = RegistryClient::new(build).unwrap_err();
            assert!(matches!(err, RegistryError::InvalidConfig(_)), "{err:?}");
        }
        // Boundary values are accepted.
        for build in [
            RegistryClientConfig::new("http://r").max_attempts(1),
            RegistryClientConfig::new("http://r").max_attempts(8),
            RegistryClientConfig::new("http://r").max_body_bytes(1024),
            RegistryClientConfig::new("http://r").max_body_bytes(64 * 1024 * 1024),
            RegistryClientConfig::new("http://r").max_references(1),
            RegistryClientConfig::new("http://r").max_references(1024),
        ] {
            RegistryClient::new(build).unwrap();
        }
    }

    #[test]
    fn auth_validation_rejects_bad_credentials() {
        for auth in [
            RegistryAuth::basic("", "pw"),
            RegistryAuth::basic("al:ice", "pw"),
            RegistryAuth::basic("alice", "p\tw"),
            RegistryAuth::basic("ali\nce", "pw"),
            RegistryAuth::basic("alice", format!("{SECRET_PASSWORD}\n")),
            RegistryAuth::bearer(""),
            RegistryAuth::bearer("tok en"),
            RegistryAuth::bearer("tok\nen"),
            RegistryAuth::bearer(format!("xx {SECRET_TOKEN}")),
        ] {
            let err =
                RegistryClient::new(RegistryClientConfig::new("http://r").auth(auth)).unwrap_err();
            assert!(matches!(err, RegistryError::InvalidConfig(_)), "{err:?}");
            let shown = format!("{err}");
            assert!(!shown.contains("alice"), "{shown}");
            assert!(!shown.contains("tok en"), "{shown}");
            assert!(!shown.contains(SECRET_PASSWORD), "{shown}");
            assert!(!shown.contains(SECRET_TOKEN), "{shown}");
        }
        assert!(RegistryAuth::basic("alice", "p:ss").validate().is_ok());
        assert!(RegistryAuth::bearer("tok-1").validate().is_ok());
    }

    #[test]
    fn debug_impls_redact_secrets() {
        let basic = RegistryClient::new(
            RegistryClientConfig::new("http://reg:8081")
                .auth(RegistryAuth::basic("alice-x", SECRET_PASSWORD)),
        )
        .unwrap();
        let bearer = RegistryClient::new(
            RegistryClientConfig::new("https://reg").auth(RegistryAuth::bearer(SECRET_TOKEN)),
        )
        .unwrap();
        for client in [&basic, &bearer] {
            let shown = format!("{client:?}");
            assert!(shown.contains("<redacted>"), "{shown}");
            assert!(!shown.contains(SECRET_PASSWORD), "{shown}");
            assert!(!shown.contains(SECRET_TOKEN), "{shown}");
            assert!(!shown.contains("alice-x"), "{shown}");
        }
        let config = RegistryClientConfig::new("http://reg")
            .auth(RegistryAuth::bearer(SECRET_TOKEN))
            .ca_pem(b"fake".to_vec());
        let shown = format!("{config:?}");
        assert!(!shown.contains(SECRET_TOKEN), "{shown}");
        assert!(shown.contains("ca_override"), "{shown}");
        let auth_shown = format!("{:?}", RegistryAuth::bearer(SECRET_TOKEN));
        assert_eq!(auth_shown, "Bearer(<redacted>)");
    }

    #[test]
    fn request_bytes_carry_auth_but_never_leak_into_errors() {
        let basic = RegistryClient::new(
            RegistryClientConfig::new("http://reg:8081").auth(RegistryAuth::basic("al", "pw")),
        )
        .unwrap();
        let req = basic.request_bytes("/schemas/ids/1");
        // base64("al:pw") == "YWw6cHc="
        assert!(req.contains("Authorization: Basic YWw6cHc=\r\n"), "{req}");
        assert!(req.starts_with("GET /schemas/ids/1 HTTP/1.1\r\n"), "{req}");
        assert!(req.contains(SCHEMA_REGISTRY_ACCEPT), "{req}");

        let bearer = RegistryClient::new(
            RegistryClientConfig::new("http://reg").auth(RegistryAuth::bearer("t-1")),
        )
        .unwrap();
        let req = bearer.request_bytes("/x");
        assert!(req.contains("Authorization: Bearer t-1\r\n"), "{req}");

        let anon = client_for("http://reg");
        assert!(!anon.request_bytes("/x").contains("Authorization"));
    }

    #[test]
    fn percent_encoding_covers_reserved_chars() {
        assert_eq!(percent_encode_segment("user-value"), "user-value");
        assert_eq!(percent_encode_segment("a/b c"), "a%2Fb%20c");
        assert_eq!(percent_encode_segment("a+b?c=d&e"), "a%2Bb%3Fc%3Dd%26e");
        assert_eq!(percent_encode_segment("100%"), "100%25");
        assert_eq!(percent_encode_segment("~._-"), "~._-");
    }

    #[test]
    fn schema_version_display() {
        assert_eq!(SchemaVersion::Latest.to_string(), "latest");
        assert_eq!(SchemaVersion::Pinned(3).to_string(), "3");
    }

    #[test]
    fn parse_schema_by_id_with_references() {
        let body = br#"{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"User\",\"doc\":\"caf\u00e9 \ud83d\ude00\"}","references":[{"name":"Address","subject":"address-value","version":3},{"name":"N","subject":"n","version":1}]}"#;
        let schema = parse_registered_schema(100, body).unwrap();
        assert_eq!(schema.id, 100);
        assert_eq!(schema.schema_type.as_deref(), Some("AVRO"));
        assert!(
            schema.schema.contains("caf\u{e9} \u{1f600}"),
            "{}",
            schema.schema
        );
        assert_eq!(
            schema.references,
            vec![
                SchemaReference {
                    name: "Address".to_string(),
                    subject: "address-value".to_string(),
                    version: 3,
                },
                SchemaReference {
                    name: "N".to_string(),
                    subject: "n".to_string(),
                    version: 1,
                },
            ]
        );
    }

    #[test]
    fn parse_subject_version_optional_fields() {
        let body = br#"{"subject":"s","version":2,"id":9,"schema":"{}"}"#;
        let schema = parse_versioned_schema(body).unwrap();
        assert_eq!(schema.subject, "s");
        assert_eq!(schema.version, 2);
        assert_eq!(schema.id, 9);
        assert_eq!(schema.schema_type, None);
        assert!(schema.references.is_empty());

        // Explicit nulls behave like absent fields.
        let body = br#"{"subject":"s","version":2,"id":9,"schema":"{}","schemaType":null}"#;
        let schema = parse_versioned_schema(body).unwrap();
        assert_eq!(schema.schema_type, None);

        // Unknown fields are ignored for forward compatibility.
        let body = br#"{"subject":"s","version":2,"id":9,"schema":"{}","future":true}"#;
        parse_versioned_schema(body).unwrap();
    }

    #[test]
    fn malformed_json_shapes_rejected() {
        let deep = format!("{{\"a\":{}1{}}}", "[".repeat(40), "]".repeat(40));
        let cases: Vec<Vec<u8>> = vec![
            b"{".to_vec(),
            b"not json".to_vec(),
            b"[1,2]".to_vec(),
            b"{\"schema\":42}".to_vec(),
            b"{\"schema\":\"x\",\"schema\":\"y\"}".to_vec(),
            b"{\"schema\":\"x\"} trailing".to_vec(),
            b"{\"schema\":\"\\ud800\"}".to_vec(),
            b"{\"schema\":\"\\udc00\"}".to_vec(),
            b"{\"schema\":\"\\x\"}".to_vec(),
            b"{\"schema\":\"a\x01b\"}".to_vec(),
            b"{\"schema\":01}".to_vec(),
            b"{\"schema\":\"x\",\"references\":{}}".to_vec(),
            b"{\"schema\":\"x\",\"references\":[42]}".to_vec(),
            b"{\"schema\":\"x\",\"references\":[{\"name\":\"n\"}]}".to_vec(),
            b"{\"schema\":\"x\",\"references\":[{\"name\":\"n\",\"subject\":\"s\",\"version\":-1}]}"
                .to_vec(),
            b"{\"schema\":\"x\",\"references\":[{\"name\":\"n\",\"subject\":\"s\",\"version\":1.5}]}"
                .to_vec(),
            deep.into_bytes(),
            vec![0xff, 0xfe],
        ];
        for body in &cases {
            let err = parse_registered_schema(1, body).unwrap_err();
            assert!(matches!(err, RegistryError::Malformed { .. }), "{err:?}");
            // Malformed messages name the rule, never the body bytes.
            assert!(!format!("{err}").contains("ud800"), "{err}");
        }
        for body in [
            br#"{"subject":"s","version":1,"id":1}"#.as_slice(),
            br#"{"subject":"s","version":1,"id":99999999999999999999999,"schema":"x"}"#.as_slice(),
            br#"{"subject":"s","version":"1","id":1,"schema":"x"}"#.as_slice(),
            br#"{"subject":"s","version":1,"id":1,"schema":"x","schemaType":7}"#.as_slice(),
        ] {
            let err = parse_versioned_schema(body).unwrap_err();
            assert!(matches!(err, RegistryError::Malformed { .. }), "{err:?}");
        }
    }

    #[test]
    fn registry_error_code_extraction() {
        assert_eq!(
            registry_error_code(br#"{"error_code":40403,"message":"Schema not found"}"#),
            Some(40403)
        );
        assert_eq!(registry_error_code(b"not json"), None);
        assert_eq!(registry_error_code(b"[1]"), None);
        assert_eq!(registry_error_code(br#"{"error_code":"x"}"#), None);
        assert_eq!(registry_error_code(br#"{}"#), None);
    }

    #[test]
    fn error_display_names_statuses_without_bodies() {
        let err = RegistryError::not_found("schema id 7", b"body-marker-zzz");
        assert_eq!(
            err,
            RegistryError::NotFound {
                lookup: "schema id 7".to_string(),
                error_code: None,
            }
        );
        assert_eq!(
            format!("{err}"),
            "schema registry lookup schema id 7 not found"
        );
        let err = RegistryError::not_found("schema id 7", br#"{"error_code":40403}"#);
        assert_eq!(
            format!("{err}"),
            "schema registry lookup schema id 7 not found (error_code 40403)"
        );
        assert_eq!(
            RegistryError::RateLimited { attempts: 3 }.to_string(),
            "schema registry rate limited (HTTP 429) after 3 attempts"
        );
        // Debug mirrors the same safe fields.
        let shown = format!("{err:?}");
        assert!(shown.contains("40403"), "{shown}");
        assert!(!shown.contains("body-marker"), "{shown}");
    }

    // ---------- loopback mock servers ----------

    #[derive(Debug)]
    struct RecordedRequest {
        method: String,
        path: String,
        /// Whether the `Authorization` header equaled the expected value.
        /// The header value itself is never stored.
        auth_ok: bool,
    }

    #[derive(Clone)]
    struct MockResponse {
        status: u16,
        reason: &'static str,
        extra_headers: Vec<(String, String)>,
        body: Vec<u8>,
        chunked: bool,
        close_delimited: bool,
    }

    impl MockResponse {
        fn json(status: u16, body: &str) -> Self {
            let reason = match status {
                200 => "OK",
                302 => "Found",
                400 => "Bad Request",
                401 => "Unauthorized",
                403 => "Forbidden",
                404 => "Not Found",
                429 => "Too Many Requests",
                500 => "Internal Server Error",
                502 => "Bad Gateway",
                503 => "Service Unavailable",
                504 => "Gateway Timeout",
                _ => "Error",
            };
            Self {
                status,
                reason,
                extra_headers: vec![(
                    "Content-Type".to_string(),
                    "application/vnd.schemaregistry.v1+json".to_string(),
                )],
                body: body.as_bytes().to_vec(),
                chunked: false,
                close_delimited: false,
            }
        }

        fn header(mut self, key: &str, value: &str) -> Self {
            self.extra_headers
                .push((key.to_string(), value.to_string()));
            self
        }

        fn chunked(mut self) -> Self {
            self.chunked = true;
            self
        }

        fn close_delimited(mut self) -> Self {
            self.close_delimited = true;
            self
        }

        fn render(&self) -> Vec<u8> {
            let mut head = format!("HTTP/1.1 {} {}\r\n", self.status, self.reason);
            for (key, value) in &self.extra_headers {
                head.push_str(&format!("{key}: {value}\r\n"));
            }
            if self.chunked {
                head.push_str("Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
                let mut bytes = head.into_bytes();
                let mid = self.body.len() / 2;
                for part in [&self.body[..mid], &self.body[mid..]] {
                    bytes.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
                    bytes.extend_from_slice(part);
                    bytes.extend_from_slice(b"\r\n");
                }
                bytes.extend_from_slice(b"0\r\n\r\n");
                bytes
            } else {
                if !self.close_delimited {
                    head.push_str(&format!("Content-Length: {}\r\n", self.body.len()));
                }
                head.push_str("Connection: close\r\n\r\n");
                let mut bytes = head.into_bytes();
                bytes.extend_from_slice(&self.body);
                bytes
            }
        }
    }

    struct MockScript {
        responses: Vec<MockResponse>,
        next: usize,
    }

    struct MockServer {
        base_url: String,
        recorded: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl MockServer {
        async fn hits(&self) -> usize {
            self.recorded.lock().await.len()
        }

        async fn paths(&self) -> Vec<String> {
            self.recorded
                .lock()
                .await
                .iter()
                .map(|r| r.path.clone())
                .collect()
        }
    }

    /// Scripted plain-HTTP mock: connection N gets scripted response N (the
    /// last response repeats). `expect_auth` is the exact expected
    /// `Authorization` header value, or `None` to require its absence.
    async fn start_mock(script: Vec<MockResponse>, expect_auth: Option<String>) -> MockServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(MockScript {
            responses: script,
            next: 0,
        }));
        tokio::spawn({
            let recorded = Arc::clone(&recorded);
            let script = Arc::clone(&script);
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    let recorded = recorded.clone();
                    let script = script.clone();
                    let expect_auth = expect_auth.clone();
                    tokio::spawn(async move {
                        handle_mock_conn(stream, recorded, script, expect_auth).await;
                    });
                }
            }
        });
        MockServer {
            base_url: format!("http://127.0.0.1:{port}"),
            recorded,
        }
    }

    /// Split a recorded request into method, path, and auth header value.
    fn parse_mock_request(buf: &[u8]) -> (String, String, Option<String>) {
        let text = String::from_utf8_lossy(buf);
        let mut lines = text.split("\r\n");
        let request_line = lines.next().unwrap_or("");
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let path = parts.next().unwrap_or("").to_string();
        let mut saw_auth = None;
        for line in lines {
            if let Some((key, value)) = line.split_once(':') {
                if key.eq_ignore_ascii_case("authorization") {
                    saw_auth = Some(value.trim().to_string());
                }
            }
        }
        (method, path, saw_auth)
    }

    async fn handle_mock_conn(
        stream: TcpStream,
        recorded: Arc<Mutex<Vec<RecordedRequest>>>,
        script: Arc<Mutex<MockScript>>,
        expect_auth: Option<String>,
    ) {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        let (mut rd, mut wr) = stream.into_split();
        loop {
            if find_header_end(&buf).is_some() || buf.len() > 65_536 {
                break;
            }
            match tokio::time::timeout(Duration::from_secs(5), rd.read(&mut tmp)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                _ => break,
            }
        }
        let (method, path, saw_auth) = parse_mock_request(&buf);
        // Compare without storing: the harness keeps only the verdict.
        let auth_ok = match (&expect_auth, &saw_auth) {
            (None, None) => true,
            (Some(expected), Some(seen)) => expected == seen,
            _ => false,
        };
        recorded.lock().await.push(RecordedRequest {
            method,
            path,
            auth_ok,
        });
        let response = {
            let mut script = script.lock().await;
            let idx = script.next.min(script.responses.len().saturating_sub(1));
            script.next = script.next.saturating_add(1);
            script.responses[idx].render()
        };
        let _ = wr.write_all(&response).await;
        let _ = wr.shutdown().await;
    }

    fn fast_client(server: &MockServer) -> RegistryClient {
        RegistryClient::new(
            RegistryClientConfig::new(server.base_url.clone())
                .connect_timeout(Duration::from_secs(2))
                .request_timeout(Duration::from_secs(5))
                .retry_backoff(Duration::from_millis(5), Duration::from_millis(20)),
        )
        .unwrap()
    }

    const BY_ID_100: &str = r#"{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"User\"}","references":[{"name":"Address","subject":"address-value","version":3}]}"#;
    const ADDRESS_V3: &str = r#"{"subject":"address-value","version":3,"id":55,"schema":"{\"type\":\"record\",\"name\":\"Address\"}"}"#;
    const USER_LATEST: &str = r#"{"subject":"user-value","version":7,"id":100,"schema":"{}","schemaType":"AVRO","references":[{"name":"A","subject":"a-value","version":1},{"name":"B","subject":"b-value","version":2}]}"#;

    #[tokio::test]
    async fn get_schema_by_id_success() {
        let server = start_mock(vec![MockResponse::json(200, BY_ID_100)], None).await;
        let schema = fast_client(&server).get_schema_by_id(100).await.unwrap();
        assert_eq!(schema.id, 100);
        assert_eq!(schema.schema_type.as_deref(), Some("AVRO"));
        assert_eq!(schema.references.len(), 1);
        assert_eq!(schema.references[0].subject, "address-value");
        assert_eq!(server.hits().await, 1);
        assert_eq!(server.paths().await, vec!["/schemas/ids/100".to_string()]);
        assert!(server.recorded.lock().await[0].auth_ok);
        assert_eq!(server.recorded.lock().await[0].method, "GET");
    }

    #[tokio::test]
    async fn get_schema_by_subject_latest_and_pinned() {
        let server = start_mock(
            vec![
                MockResponse::json(200, USER_LATEST),
                MockResponse::json(200, ADDRESS_V3),
            ],
            None,
        )
        .await;
        let client = fast_client(&server);
        let latest = client
            .get_schema_by_subject("user-value", SchemaVersion::Latest)
            .await
            .unwrap();
        assert_eq!(latest.subject, "user-value");
        assert_eq!(latest.version, 7);
        assert_eq!(latest.references.len(), 2);
        let pinned = client
            .get_schema_by_subject("address-value", SchemaVersion::Pinned(3))
            .await
            .unwrap();
        assert_eq!(pinned.id, 55);
        assert_eq!(
            server.paths().await,
            vec![
                "/subjects/user-value/versions/latest".to_string(),
                "/subjects/address-value/versions/3".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn subject_path_is_percent_encoded() {
        let server = start_mock(vec![MockResponse::json(200, USER_LATEST)], None).await;
        fast_client(&server)
            .get_schema_by_subject("a/b c+d", SchemaVersion::Latest)
            .await
            .unwrap();
        assert_eq!(
            server.paths().await,
            vec!["/subjects/a%2Fb%20c%2Bd/versions/latest".to_string()]
        );
    }

    #[tokio::test]
    async fn base_path_prefix_is_used() {
        let inner = start_mock(vec![MockResponse::json(200, BY_ID_100)], None).await;
        let prefixed = format!("{}/pfx/", inner.base_url);
        let client = RegistryClient::new(RegistryClientConfig::new(prefixed)).unwrap();
        client.get_schema_by_id(7).await.unwrap();
        assert_eq!(inner.paths().await, vec!["/pfx/schemas/ids/7".to_string()]);
    }

    #[tokio::test]
    async fn fetch_references_fetches_each_in_order() {
        let server = start_mock(
            vec![
                MockResponse::json(200, USER_LATEST),
                MockResponse::json(
                    200,
                    r#"{"subject":"a-value","version":1,"id":11,"schema":"a"}"#,
                ),
                MockResponse::json(
                    200,
                    r#"{"subject":"b-value","version":2,"id":22,"schema":"b"}"#,
                ),
            ],
            None,
        )
        .await;
        let client = fast_client(&server);
        let root = client
            .get_schema_by_subject("user-value", SchemaVersion::Latest)
            .await
            .unwrap();
        let resolved = client.fetch_references(&root.references).await.unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].subject, "a-value");
        assert_eq!(resolved[1].id, 22);
        assert_eq!(
            server.paths().await,
            vec![
                "/subjects/user-value/versions/latest".to_string(),
                "/subjects/a-value/versions/1".to_string(),
                "/subjects/b-value/versions/2".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn fetch_references_respects_cap_before_any_fetch() {
        let server = start_mock(vec![MockResponse::json(200, USER_LATEST)], None).await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(server.base_url.clone()).max_references(1),
        )
        .unwrap();
        let refs = vec![
            SchemaReference {
                name: "A".to_string(),
                subject: "a-value".to_string(),
                version: 1,
            },
            SchemaReference {
                name: "B".to_string(),
                subject: "b-value".to_string(),
                version: 2,
            },
        ];
        let err = client.fetch_references(&refs).await.unwrap_err();
        assert!(matches!(err, RegistryError::Malformed { .. }), "{err:?}");
        assert_eq!(server.hits().await, 0);
    }

    #[tokio::test]
    async fn not_found_maps_with_error_code() {
        for (body, code) in [
            (
                r#"{"error_code":40403,"message":"Schema not found"}"#,
                Some(40403),
            ),
            (
                r#"{"error_code":40401,"message":"Subject not found"}"#,
                Some(40401),
            ),
            ("<html>proxy 404</html>", None),
        ] {
            let server = start_mock(vec![MockResponse::json(404, body)], None).await;
            let err = fast_client(&server).get_schema_by_id(9).await.unwrap_err();
            assert_eq!(
                err,
                RegistryError::NotFound {
                    lookup: "schema id 9".to_string(),
                    error_code: code,
                },
                "{body}"
            );
            assert_eq!(server.hits().await, 1, "404 must not retry");
            let shown = format!("{err}");
            assert!(!shown.contains("proxy"), "{shown}");
        }
    }

    #[tokio::test]
    async fn unauthorized_and_forbidden_do_not_retry() {
        for (status, expected) in [
            (401, RegistryError::Unauthorized),
            (403, RegistryError::Forbidden),
        ] {
            let server = start_mock(
                vec![MockResponse::json(status, r#"{"error_code":401}"#)],
                None,
            )
            .await;
            let err = fast_client(&server)
                .get_schema_by_subject("s", SchemaVersion::Latest)
                .await
                .unwrap_err();
            assert_eq!(err, expected);
            assert_eq!(server.hits().await, 1);
        }
    }

    #[tokio::test]
    async fn rate_limit_retries_then_succeeds() {
        let server = start_mock(
            vec![
                MockResponse::json(429, r#"{"error_code":429}"#).header("Retry-After", "0"),
                MockResponse::json(200, BY_ID_100),
            ],
            None,
        )
        .await;
        let schema = fast_client(&server).get_schema_by_id(100).await.unwrap();
        assert_eq!(schema.id, 100);
        assert_eq!(server.hits().await, 2);
    }

    #[tokio::test]
    async fn rate_limit_exhaustion_reports_attempts() {
        let server = start_mock(vec![MockResponse::json(429, r#"{"error_code":429}"#)], None).await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(server.base_url.clone())
                .max_attempts(3)
                .retry_backoff(Duration::from_millis(5), Duration::from_millis(10))
                .request_timeout(Duration::from_secs(10)),
        )
        .unwrap();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert_eq!(err, RegistryError::RateLimited { attempts: 3 });
        assert_eq!(server.hits().await, 3);
    }

    #[tokio::test]
    async fn retry_after_is_capped_at_backoff_max() {
        let server = start_mock(
            vec![
                MockResponse::json(429, "{}").header("Retry-After", "3600"),
                MockResponse::json(200, BY_ID_100),
            ],
            None,
        )
        .await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(server.base_url.clone())
                .retry_backoff(Duration::from_millis(10), Duration::from_millis(50))
                .request_timeout(Duration::from_secs(10)),
        )
        .unwrap();
        let started = Instant::now();
        client.get_schema_by_id(100).await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "Retry-After: 3600 must be capped"
        );
        assert_eq!(server.hits().await, 2);
    }

    #[tokio::test]
    async fn server_errors_retry_then_fail() {
        let server = start_mock(
            vec![
                MockResponse::json(503, "busy"),
                MockResponse::json(502, "bad gw"),
                MockResponse::json(200, BY_ID_100),
            ],
            None,
        )
        .await;
        fast_client(&server).get_schema_by_id(100).await.unwrap();
        assert_eq!(server.hits().await, 3);

        let server = start_mock(vec![MockResponse::json(500, "boom")], None).await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(server.base_url.clone())
                .max_attempts(2)
                .retry_backoff(Duration::from_millis(5), Duration::from_millis(10)),
        )
        .unwrap();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert_eq!(err, RegistryError::Server { status: 500 });
        assert_eq!(server.hits().await, 2);
        assert!(!format!("{err}").contains("boom"));
    }

    #[tokio::test]
    async fn unexpected_status_not_retried() {
        for status in [302, 400, 405, 501] {
            let server = start_mock(vec![MockResponse::json(status, "x")], None).await;
            let err = fast_client(&server).get_schema_by_id(1).await.unwrap_err();
            assert_eq!(err, RegistryError::UnexpectedStatus { status });
            assert_eq!(server.hits().await, 1, "status {status} must not retry");
        }
    }

    #[tokio::test]
    async fn malformed_body_maps_and_redacts() {
        let evil = format!("not-json {{{SECRET_TOKEN}}} {{{SECRET_PASSWORD}}}");
        let server = start_mock(vec![MockResponse::json(200, &evil)], None).await;
        let err = fast_client(&server).get_schema_by_id(1).await.unwrap_err();
        assert!(matches!(err, RegistryError::Malformed { .. }), "{err:?}");
        assert_eq!(server.hits().await, 1, "malformed must not retry");
        for shown in [format!("{err}"), format!("{err:?}")] {
            assert!(!shown.contains(SECRET_TOKEN), "{shown}");
            assert!(!shown.contains(SECRET_PASSWORD), "{shown}");
        }
        // Valid JSON with the wrong shape is also Malformed.
        let server = start_mock(vec![MockResponse::json(200, r#"{"id":1}"#)], None).await;
        let err = fast_client(&server).get_schema_by_id(1).await.unwrap_err();
        assert!(matches!(err, RegistryError::Malformed { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn oversize_body_rejected() {
        // Lying Content-Length far above the cap: rejected without reading.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let mut total = Vec::new();
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                total.extend_from_slice(&buf[..n]);
                if find_header_end(&total).is_some() {
                    break;
                }
            }
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                100 * 1024 * 1024
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            // Trickle far less than promised, then hold: the client must
            // fail fast on the declared length instead of waiting.
            stream.write_all(b"junk").await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let client = RegistryClient::new(
            RegistryClientConfig::new(format!("http://127.0.0.1:{port}"))
                .max_body_bytes(8192)
                .request_timeout(Duration::from_secs(5)),
        )
        .unwrap();
        let started = Instant::now();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert_eq!(err, RegistryError::TooLarge { limit: 8192 });
        assert!(started.elapsed() < Duration::from_secs(4));

        // Close-delimited body past the cap is also rejected.
        let big = "z".repeat(16 * 1024);
        let server = start_mock(vec![MockResponse::json(200, &big).close_delimited()], None).await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(server.base_url.clone()).max_body_bytes(8192),
        )
        .unwrap();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert_eq!(err, RegistryError::TooLarge { limit: 8192 });
    }

    #[tokio::test]
    async fn chunked_response_supported() {
        let server = start_mock(vec![MockResponse::json(200, BY_ID_100).chunked()], None).await;
        let schema = fast_client(&server).get_schema_by_id(100).await.unwrap();
        assert_eq!(schema.id, 100);
        assert_eq!(schema.references.len(), 1);
    }

    #[tokio::test]
    async fn close_delimited_response_supported() {
        let server = start_mock(
            vec![MockResponse::json(200, BY_ID_100).close_delimited()],
            None,
        )
        .await;
        let schema = fast_client(&server).get_schema_by_id(100).await.unwrap();
        assert_eq!(schema.id, 100);
    }

    #[tokio::test]
    async fn timeout_when_server_hangs() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let client = RegistryClient::new(
            RegistryClientConfig::new(format!("http://127.0.0.1:{port}"))
                .request_timeout(Duration::from_millis(200))
                .connect_timeout(Duration::from_secs(2))
                .max_attempts(1),
        )
        .unwrap();
        let started = Instant::now();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert_eq!(err, RegistryError::Timeout);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn transport_error_on_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client = RegistryClient::new(
            RegistryClientConfig::new(format!("http://127.0.0.1:{port}"))
                .request_timeout(Duration::from_secs(5))
                .max_attempts(1),
        )
        .unwrap();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert!(matches!(err, RegistryError::Transport(_)), "{err:?}");
    }

    #[tokio::test]
    async fn auth_headers_sent() {
        // Basic: base64("alice:pw-1").
        let basic_value = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(b"alice:pw-1")
        );
        let server = start_mock(vec![MockResponse::json(200, BY_ID_100)], Some(basic_value)).await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(server.base_url.clone())
                .auth(RegistryAuth::basic("alice", "pw-1")),
        )
        .unwrap();
        client.get_schema_by_id(100).await.unwrap();
        assert!(server.recorded.lock().await[0].auth_ok);

        // Bearer [REDACTED] through verbatim.
        let server = start_mock(
            vec![MockResponse::json(200, BY_ID_100)],
            Some(format!("Bearer {SECRET_TOKEN}")),
        )
        .await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(server.base_url.clone())
                .auth(RegistryAuth::bearer(SECRET_TOKEN)),
        )
        .unwrap();
        client.get_schema_by_id(100).await.unwrap();
        assert!(server.recorded.lock().await[0].auth_ok);

        // Anonymous lookups send no Authorization header.
        let server = start_mock(vec![MockResponse::json(200, BY_ID_100)], None).await;
        fast_client(&server).get_schema_by_id(100).await.unwrap();
        assert!(server.recorded.lock().await[0].auth_ok);
    }

    #[tokio::test]
    async fn error_body_echoing_credential_is_redacted() {
        let token = SECRET_TOKEN.to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 1024];
            loop {
                let n = stream.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if find_header_end(&buf).is_some() {
                    break;
                }
            }
            // Hostile registry: reflect the Authorization header into an
            // error body. The client must still not surface it.
            let text = String::from_utf8_lossy(&buf);
            let mut echoed = String::new();
            for line in text.split("\r\n") {
                if let Some(value) = line.strip_prefix("Authorization:") {
                    echoed = value.trim().to_string();
                }
            }
            assert!(echoed.contains(token.as_str()));
            let body = format!("{{\"error_code\":500,\"message\":\"fail {echoed}\"}}");
            let head = format!(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body.as_bytes()).await.unwrap();
        });
        let client = RegistryClient::new(
            RegistryClientConfig::new(format!("http://127.0.0.1:{port}"))
                .auth(RegistryAuth::bearer(SECRET_TOKEN))
                .max_attempts(1),
        )
        .unwrap();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert_eq!(err, RegistryError::Server { status: 500 });
        for shown in [format!("{err}"), format!("{err:?}")] {
            assert!(!shown.contains(SECRET_TOKEN), "{shown}");
            assert!(!shown.contains("Bearer"), "{shown}");
        }
    }

    // ---------- TLS fixtures ----------
    //
    // Private CA + server certificate generated once with the openssl CLI
    // (RSA-2048, SAN IP:127.0.0.1 + DNS:localhost, valid 2026-09-22
    // through 2056-09-14) and embedded so tests need no openssl at
    // runtime. Test-only trust: never valid outside loopback tests.

    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIDDTCCAfWgAwIBAgIULJklwQcGGSXwkdKcxoVJooJsHQIwDQYJKoZIhvcNAQEL\nBQAwFTETMBEGA1UEAwwKcGwtdGVzdC1jYTAgFw0yNjA5MjIxODMzMTVaGA8yMDU2\nMDkxNDE4MzMxNVowFTETMBEGA1UEAwwKcGwtdGVzdC1jYTCCASIwDQYJKoZIhvcN\nAQEBBQADggEPADCCAQoCggEBANHuHvd46GHeLcH8tTjiMiyefhfpwagzpV+XT7Bl\nIOeqKcCVt2uCt3eDDz8Sj149MZm0RzxfLl6y9KwjOyXNvxAOgOTKJTeb5c6GxyxJ\nlPpILFgDQmX0I6nU6IxOjZEwFi/N6DZCc2YLK1Y1J7ffI1pFjR3kqoYE/5yDPo8O\nKbt03HMpBsHb+FmT0SnPWT/KyMkS4TYAQ180j0x04e1GcIDdOq5Qt/YqcguoH1hs\nkuFS5acMgDE2OFrrJr4n+fkNZShSp4IY3SKd8t+mB8403qTmUxrZ8C4QBCoKSypw\nsdwMRDXGaNN1aALWa5tiEGmyf+E3uOnlitbsM7e9kXBMtDcCAwEAAaNTMFEwHQYD\nVR0OBBYEFN2aUKLet7Z/BMCSHlIR8UddyOMVMB8GA1UdIwQYMBaAFN2aUKLet7Z/\nBMCSHlIR8UddyOMVMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEB\nAAOUUPbh8BDwE9p7m21D1w/J8yyCTbo7T5QfgqM4suLMc8xOunozdaXAeCuI/nec\niYZXeEpxQw77K7ekg1SXezgX50Dl5L8gGd/nI4b1ogU+9H2fsrY5opIOMAIDavfp\nZ5Bq7Ob9KQCTFKRjJPV4wl266IczdwSkOhTr9UJGAk6440zBgKqzci/TmRmd7pu9\nevU0TRw7liVZbePaF3n4+AMhdbe66SUKzZAAUO6Y2wqZwUgzouuqmowYIx16YL9n\n0GYDwuI0nBx9LSITYokYCOl/zYpqHH309PtB73gQDuVUrJnGZzviZLQxzqb61B82\nf8QJLsvfP8jTJryUIPZECxs=\n-----END CERTIFICATE-----\n";

    const TEST_SERVER_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIDRjCCAi6gAwIBAgIUF2iM1H4ZYTaXd9tym0xP4zXrYqIwDQYJKoZIhvcNAQEL\nBQAwFTETMBEGA1UEAwwKcGwtdGVzdC1jYTAgFw0yNjA5MjIxODMzMTVaGA8yMDU2\nMDkxNDE4MzMxNVowFDESMBAGA1UEAwwJMTI3LjAuMC4xMIIBIjANBgkqhkiG9w0B\nAQEFAAOCAQ8AMIIBCgKCAQEA2mNrSM+UJ0k4A8xb88L7LKu6Jxv9CyrfQO2GOkQh\nBW9jWvd7L/oaZtoQ16FwoaLHeWfxfGorDZ2/3XFQ2R2MRH99gib20A7CFTDDp9ye\nL5szIqqceXFXf/Yh789FucRcrIOU123dlOtqV2ynqZYo7otRQ2DoNtCf3sVPPW5D\nr/Olc8ymZo5lRTHAGniDf3enJ0F/cnM9H7GuWiqwfo6r/UQrBnpZB2XJYJKhxxuN\njqLttLoVFuluGlgJFcrCwJjyfe006TtlXRaOQ7UnYgR4Fj7HZoXOaHQx0DBgTc01\nbyZ/1FyR4EwjKGpvOICl6QVD0zglhTamdwBp5HWACFeBKwIDAQABo4GMMIGJMBoG\nA1UdEQQTMBGHBH8AAAGCCWxvY2FsaG9zdDAJBgNVHRMEAjAAMAsGA1UdDwQEAwIF\noDATBgNVHSUEDDAKBggrBgEFBQcDATAdBgNVHQ4EFgQUJfYHxzqlgTLS5F6dHJXX\n4EaeK8kwHwYDVR0jBBgwFoAU3ZpQot63tn8EwJIeUhHxR13I4xUwDQYJKoZIhvcN\nAQELBQADggEBAIE/4PZNUatwCVlxIOEo1WklBR+HjLKqbMoWXF9qwYRJsxnTJt4b\nIf7gjASeIgzRcEdRdwG0aBq55edMr76E17z4WNogbXBrDGMfc4ufu3lNXfveU7xk\nWJcekNIGsR/yln+aPFN1jv7gDflvnuTN0eL9jhouBufWr+MAFW0jSgfX4CyaCMQu\nOz7QP1IOOc9SnO8wU0PCfZoudxeou6nl9ybQCIh/d4GcREWsJPwF3rapR07wyUe4\nEXBehJrExhTGLomd03q5ngx5Vu0gzE6bliCwe3RWitCEbU+qSrNrCMW97udCWhfn\nWpMuG8YbKx0+P46u/jAt5CtYS2JGHG27mWg=\n-----END CERTIFICATE-----\n";

    const TEST_SERVER_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDaY2tIz5QnSTgD\nzFvzwvssq7onG/0LKt9A7YY6RCEFb2Na93sv+hpm2hDXoXChosd5Z/F8aisNnb/d\ncVDZHYxEf32CJvbQDsIVMMOn3J4vmzMiqpx5cVd/9iHvz0W5xFysg5TXbd2U62pX\nbKeplijui1FDYOg20J/exU89bkOv86VzzKZmjmVFMcAaeIN/d6cnQX9ycz0fsa5a\nKrB+jqv9RCsGelkHZclgkqHHG42Oou20uhUW6W4aWAkVysLAmPJ97TTpO2VdFo5D\ntSdiBHgWPsdmhc5odDHQMGBNzTVvJn/UXJHgTCMoam84gKXpBUPTOCWFNqZ3AGnk\ndYAIV4ErAgMBAAECggEAJnAfPvaCPhPuDwUWHiZwbSFgp2eOtzt5hgUIfhLluP4s\n/6LVhNFBel7hXgKlP13WPDEmWN6a60+bqI88SqqfuKKz5YeUI3SlhnNJzK7RDNIF\nQxHCbsGbRAN/X6UcwaClKxwRw4Ur3f09f1u5eujaFXph+DkDRjqcGOqjks1ojnxB\nj7CXhyH8y9qLEX6ppf6X/VDlEBKJy7Y3R/nhkalaSuH36WkDztQfotT7O9LWPUUo\nl8T7rcpWr0varfwP0Kwaq6wcSLGTb81XPAY0t6EMd8+kueBIYItSMy5AP7df5mtq\ndiosX+YmEnyhloJ/AAYpZ2KsD3A7UNaXK4Ec0ydT4QKBgQD28tUSXRnHHesoYNDv\n4IcN/4unm6zGdIQtV3NfT7djTu/Cy/qrfMSpaRh/9i0n+sbQEmv8skhu4hv0nxw9\nYOoEkPY+h0qgoNNFyc1Hc2BOzfcwERneFNupbDx4YxQSRvvkZNjdvfoCDDpN35JA\nosqhCsJy6lmisvAvfMUkMgqQuwKBgQDiZJnDIyTbRwEyJUD5Rh3lK1SVZJPG5Fj1\naQvAKOUW1F0AIid/vOvF3OAynu2Yr7nO070vY/DFexGrRVRanPX8HLUX9yY6tOv5\nSEWO+X+9Z1kLnXuwvxtj6ml/sqB3hK7y15v4nv3F7QL3FpJCgg8EOMdK6k8YBt4M\ngdOMz9bCUQKBgQDJNyQWSnXuoJoz1G9qhXCGH2sTru0g51+r8k23o6Sx7me+OaaO\nhKNZxqCH43b31Iaak+gZhssuTl6o+9xuxsDn55Y9bM+KAoEjpEL3rTMUAw8ew1Bo\nfGZfrim3jkOUgPJOLz3lsB49/Oik+z6YHA0vGy1FpV5UC6lZiDi6PWwOcQKBgQDe\nYsXsOsSEpb4V/SRS+T56lFLVIWRMdpiwEU0aqNFI2Li2XdaBExpjVbHh594rI0sZ\nUUNAnyKvSlIz9LmE/TRhP+3gKcYi2wAF8qlpZcrGShPdZghPuZp1Tpntd5FLdknI\ngGVVFxDf8Q79mu13aXzIv+F8xKeHSY+rp4gghTVH0QKBgQDeWZ+FVvUucT/3iHFD\n6fgFvqBK0SuZtTvBfVSLD+9FjcYaaHTMLdvpMXOBJoTYV9VmsoK5is/zlltuR9go\n0mN/xrqQR167S2hkLFbPjQLGpA7V0gCkot2UCBTx0XWcBtF4pIOGzKqVrViWQHOx\nhDgPxkEWuBq/LMDzB9u80H1rQA==\n-----END PRIVATE KEY-----\n";

    async fn start_tls_mock(
        script: Vec<MockResponse>,
    ) -> (String, Arc<Mutex<Vec<RecordedRequest>>>) {
        ensure_crypto();
        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(TEST_SERVER_PEM.as_bytes())
                .collect::<Result<_, _>>()
                .unwrap();
        let key =
            rustls::pki_types::PrivateKeyDer::from_pem_slice(TEST_SERVER_KEY.as_bytes()).unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(MockScript {
            responses: script,
            next: 0,
        }));
        tokio::spawn({
            let recorded = Arc::clone(&recorded);
            let script = Arc::clone(&script);
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    let Ok(tls) = acceptor.accept(stream).await else {
                        continue;
                    };
                    let recorded = recorded.clone();
                    let script = script.clone();
                    tokio::spawn(async move {
                        handle_mock_tls_conn(tls, recorded, script).await;
                    });
                }
            }
        });
        (format!("https://127.0.0.1:{port}"), recorded)
    }

    async fn handle_mock_tls_conn<S>(
        mut stream: S,
        recorded: Arc<Mutex<Vec<RecordedRequest>>>,
        script: Arc<Mutex<MockScript>>,
    ) where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            if find_header_end(&buf).is_some() || buf.len() > 65_536 {
                break;
            }
            match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut tmp)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                _ => break,
            }
        }
        let (method, path, _) = parse_mock_request(&buf);
        recorded.lock().await.push(RecordedRequest {
            method,
            path,
            auth_ok: true,
        });
        let response = {
            let mut script = script.lock().await;
            let idx = script.next.min(script.responses.len().saturating_sub(1));
            script.next = script.next.saturating_add(1);
            script.responses[idx].render()
        };
        let _ = stream.write_all(&response).await;
        let _ = stream.shutdown().await;
    }

    #[tokio::test]
    async fn https_with_private_ca_succeeds() {
        let (base_url, recorded) = start_tls_mock(vec![MockResponse::json(200, BY_ID_100)]).await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(base_url)
                .ca_pem(TEST_CA_PEM.as_bytes().to_vec())
                .request_timeout(Duration::from_secs(10)),
        )
        .unwrap();
        let schema = client.get_schema_by_id(100).await.unwrap();
        assert_eq!(schema.id, 100);
        assert_eq!(schema.references.len(), 1);
        let recorded = recorded.lock().await;
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].path, "/schemas/ids/100");
    }

    #[tokio::test]
    async fn https_with_default_roots_rejects_private_ca() {
        let (base_url, recorded) = start_tls_mock(vec![MockResponse::json(200, BY_ID_100)]).await;
        let client = RegistryClient::new(
            RegistryClientConfig::new(base_url)
                .request_timeout(Duration::from_secs(10))
                .max_attempts(1),
        )
        .unwrap();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert!(matches!(err, RegistryError::Tls(_)), "{err:?}");
        // The handshake failed before any request was served.
        assert_eq!(recorded.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn https_to_plain_http_fails_without_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut tmp = [0u8; 4096];
            let _ = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut tmp)).await;
            let body = MockResponse::json(200, BY_ID_100).render();
            let _ = stream.write_all(&body).await;
        });
        let client = RegistryClient::new(
            RegistryClientConfig::new(format!("https://127.0.0.1:{port}"))
                .request_timeout(Duration::from_secs(10))
                .max_attempts(1),
        )
        .unwrap();
        let err = client.get_schema_by_id(1).await.unwrap_err();
        assert!(matches!(err, RegistryError::Tls(_)), "{err:?}");
    }

    #[test]
    fn invalid_ca_pem_rejected_at_construction() {
        for pem in [
            b"not pem at all".to_vec(),
            Vec::new(),
            TEST_SERVER_KEY.as_bytes().to_vec(),
        ] {
            let err = RegistryClient::new(RegistryClientConfig::new("https://reg").ca_pem(pem))
                .unwrap_err();
            assert!(matches!(err, RegistryError::InvalidConfig(_)), "{err:?}");
        }
        // The test CA parses as a usable store.
        RegistryClient::new(
            RegistryClientConfig::new("https://reg").ca_pem(TEST_CA_PEM.as_bytes().to_vec()),
        )
        .unwrap();
    }

    #[test]
    fn https_rejects_invalid_tls_server_name_at_construction() {
        // Over the 255-byte DNS-name ceiling: can never verify.
        let long = format!("https://{}.example", "a".repeat(300));
        let err = RegistryClient::new(RegistryClientConfig::new(long)).unwrap_err();
        assert!(matches!(err, RegistryError::InvalidConfig(_)), "{err:?}");
        // Plain HTTP needs no TLS name, so construction succeeds.
        RegistryClient::new(RegistryClientConfig::new("http://my_host")).unwrap();
    }
}
