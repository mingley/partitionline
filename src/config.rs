//! Shared client configuration types.
//!
//! These are the knobs you set before connecting. Field-by-field mutation on
//! [`ProducerConfig`](crate::ProducerConfig) still works; the builders here are
//! the shorter path.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::net::TlsConfig;
use crate::protocol::oidc::OidcConfig;

/// Kafka `retry.backoff.ms` default (Java and librdkafka: 100).
pub(crate) const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Kafka `retry.backoff.max.ms` default (Java and librdkafka: 1000).
pub(crate) const DEFAULT_RETRY_BACKOFF_MAX: Duration = Duration::from_millis(1000);
/// Kafka `reconnect.backoff.ms` default (Java: 50).
pub(crate) const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_millis(50);
/// Kafka `reconnect.backoff.max.ms` default (Java: 1000).
pub(crate) const DEFAULT_RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(1000);
/// Kafka `connections.max.idle.ms` default (Java: 9 minutes).
pub(crate) const DEFAULT_CONNECTIONS_MAX_IDLE: Duration = Duration::from_millis(9 * 60 * 1000);

/// True when a socket has been unused for at least `max_idle`.
///
/// A zero `max_idle` never expires (this crate; Java 0 would close immediately).
pub(crate) fn connection_idle_expired(elapsed: Duration, max_idle: Duration) -> bool {
    !max_idle.is_zero() && elapsed >= max_idle
}

/// Exponential delay for retry attempt `n` (0-based): `base * 2^n`, capped at `max`.
///
/// A zero `base` disables the wait (immediate retry). No jitter (Java adds up
/// to 20%). `max` is raised to `base` when the caller sets it lower.
pub(crate) fn retry_backoff_delay(base: Duration, max: Duration, attempt: u32) -> Duration {
    if base.is_zero() {
        return Duration::ZERO;
    }
    let cap = max.max(base);
    let shift = attempt.min(16);
    base.saturating_mul(1u32 << shift).min(cap)
}

/// Sleep [`retry_backoff_delay`], not past `deadline`.
pub(crate) async fn sleep_retry_backoff(
    base: Duration,
    max: Duration,
    attempt: u32,
    deadline: Instant,
) {
    let delay = retry_backoff_delay(base, max, attempt);
    if delay.is_zero() {
        return;
    }
    let now = Instant::now();
    if now >= deadline {
        return;
    }
    tokio::time::sleep(delay.min(deadline.saturating_duration_since(now))).await;
}

/// Wait after `fails` unsuccessful TCP/handshake attempts for one broker.
///
/// `fails == 0` is the first connect (no wait). After that this is
/// [`retry_backoff_delay`] with `attempt = fails - 1` (50ms, 100ms, …).
pub(crate) fn reconnect_backoff_delay(base: Duration, max: Duration, fails: u32) -> Duration {
    match fails {
        0 => Duration::ZERO,
        n => retry_backoff_delay(base, max, n - 1),
    }
}

/// Sleep [`reconnect_backoff_delay`].
pub(crate) async fn sleep_reconnect_backoff(base: Duration, max: Duration, fails: u32) {
    let delay = reconnect_backoff_delay(base, max, fails);
    if delay.is_zero() {
        return;
    }
    tokio::time::sleep(delay).await;
}

/// Count one failed connect for `node`. Returns the new count.
pub(crate) fn bump_reconnect_fails(map: &mut HashMap<i32, u32>, node: i32) -> u32 {
    let slot = map.entry(node).or_insert(0);
    *slot = slot.saturating_add(1);
    *slot
}

/// Broker acknowledgements the producer waits for.
///
/// Stored on [`crate::ProducerConfig::acks`] as `i16` so existing `acks = 1`
/// code keeps compiling. Prefer this enum at call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Acks {
    /// Fire and forget. The broker does not send a Produce response.
    None,
    /// Wait for the partition leader. Kafka `acks=1`. This is the default.
    #[default]
    Leader,
    /// Wait for the in-sync replicas. Kafka `acks=-1` / `acks=all`.
    All,
}

impl Acks {
    /// Kafka `acks` field.
    #[must_use]
    pub fn as_i16(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Leader => 1,
            Self::All => -1,
        }
    }

    /// Parse a Kafka `acks` value. Unknown numbers return `None`.
    #[must_use]
    pub fn from_i16(v: i16) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Leader),
            -1 => Some(Self::All),
            _ => None,
        }
    }
}

impl From<Acks> for i16 {
    fn from(acks: Acks) -> Self {
        acks.as_i16()
    }
}

/// Fetch isolation. Matches Kafka `isolation.level`.
///
/// [`Display`] is Java `IsolationLevel.toString` (`read_uncommitted`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i8)]
pub enum IsolationLevel {
    /// Return every record, including aborted transactions.
    #[default]
    ReadUncommitted = 0,
    /// Skip records from aborted transactions. Wait for the last stable offset.
    ReadCommitted = 1,
}

impl IsolationLevel {
    /// Java `IsolationLevel.toString` (`read_uncommitted`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadUncommitted => "read_uncommitted",
            Self::ReadCommitted => "read_committed",
        }
    }

    /// Java `IsolationLevel.id` (Fetch / ListOffsets wire byte).
    #[must_use]
    pub fn id(self) -> i8 {
        self.as_i8()
    }

    /// Wire value sent on Fetch and ListOffsets.
    #[must_use]
    pub fn as_i8(self) -> i8 {
        self as i8
    }

    /// Java `IsolationLevel.forId`. Unknown values return `None`.
    #[must_use]
    pub fn from_id(id: i8) -> Option<Self> {
        Self::from_i8(id)
    }

    /// Parse a Kafka isolation byte. Unknown values return `None`.
    #[must_use]
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::ReadUncommitted),
            1 => Some(Self::ReadCommitted),
            _ => None,
        }
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Java `org.apache.kafka.common.security.auth.SecurityProtocol`.
///
/// Channel type: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
/// [`Display`] is Java `SecurityProtocol.toString` / the `name` field
/// (`PLAINTEXT`). This crate still configures TLS via [`TlsConfig`] and
/// SASL via [`Sasl`]; the enum is the Java id/name mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum SecurityProtocol {
    /// Unauthenticated, unencrypted (`PLAINTEXT`).
    Plaintext = 0,
    /// TLS (`SSL`).
    Ssl = 1,
    /// SASL without TLS (`SASL_PLAINTEXT`).
    SaslPlaintext = 2,
    /// SASL over TLS (`SASL_SSL`).
    SaslSsl = 3,
}

impl SecurityProtocol {
    /// Java `SecurityProtocol.id`.
    #[must_use]
    pub const fn id(self) -> i16 {
        self as i16
    }

    /// Java `SecurityProtocol.name` (enum constant name).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Plaintext => "PLAINTEXT",
            Self::Ssl => "SSL",
            Self::SaslPlaintext => "SASL_PLAINTEXT",
            Self::SaslSsl => "SASL_SSL",
        }
    }

    /// Java `SecurityProtocol.forId`. Unknown ids are `None` (Java `null`).
    #[must_use]
    pub const fn from_id(id: i16) -> Option<Self> {
        match id {
            0 => Some(Self::Plaintext),
            1 => Some(Self::Ssl),
            2 => Some(Self::SaslPlaintext),
            3 => Some(Self::SaslSsl),
            _ => None,
        }
    }

    /// Java `SecurityProtocol.forName` (`toUpperCase`; unknown is
    /// [`Error::protocol`], Java `IllegalArgumentException` from `valueOf`).
    pub fn from_name(name: &str) -> Result<Self> {
        if name.eq_ignore_ascii_case("PLAINTEXT") {
            Ok(Self::Plaintext)
        } else if name.eq_ignore_ascii_case("SSL") {
            Ok(Self::Ssl)
        } else if name.eq_ignore_ascii_case("SASL_PLAINTEXT") {
            Ok(Self::SaslPlaintext)
        } else if name.eq_ignore_ascii_case("SASL_SSL") {
            Ok(Self::SaslSsl)
        } else {
            Err(Error::protocol(format!(
                "No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.{}",
                name.to_ascii_uppercase()
            )))
        }
    }

    /// Java `SecurityProtocol.names` (declaration order).
    #[must_use]
    pub const fn names() -> &'static [&'static str] {
        &["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
    }
}

impl fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Java `org.apache.kafka.common.network.ListenerName`.
///
/// [`Display`] is Java `ListenerName.toString` (`ListenerName(PLAINTEXT)`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListenerName {
    value: String,
}

impl ListenerName {
    /// Java `new ListenerName(String)` (`requireNonNull` on the value).
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    /// Java `ListenerName.forSecurityProtocol` (the protocol `name` field).
    #[must_use]
    pub fn for_security_protocol(security_protocol: SecurityProtocol) -> Self {
        Self::new(security_protocol.name())
    }

    /// Java `ListenerName.normalised` (`toUpperCase`; blank is
    /// [`Error::protocol`], Java `ConfigException`).
    pub fn normalised(value: &str) -> Result<Self> {
        if crate::protocol::buf::is_blank(Some(value)) {
            return Err(Error::protocol(
                "The provided listener name is null or empty string",
            ));
        }
        Ok(Self::new(value.to_uppercase()))
    }

    /// Java `ListenerName.value`.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Java `ListenerName.configPrefix` (`listener.name.{value}.` in lower case).
    #[must_use]
    pub fn config_prefix(&self) -> String {
        format!("listener.name.{}.", self.value.to_lowercase())
    }

    /// Java `ListenerName.saslMechanismConfigPrefix`.
    #[must_use]
    pub fn sasl_mechanism_config_prefix(&self, sasl_mechanism: &str) -> String {
        format!(
            "{}{}",
            self.config_prefix(),
            Self::sasl_mechanism_prefix(sasl_mechanism)
        )
    }

    /// Java `ListenerName.saslMechanismPrefix`.
    #[must_use]
    pub fn sasl_mechanism_prefix(sasl_mechanism: &str) -> String {
        format!("{}.", sasl_mechanism.to_lowercase())
    }
}

impl fmt::Display for ListenerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ListenerName({})", self.value)
    }
}

/// Java `org.apache.kafka.common.Endpoint`.
///
/// Broker listener endpoint. Java `listenerName` may be null on clients
/// (`None` here). [`Display`] is Java `Endpoint.toString`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    listener_name: Option<String>,
    security_protocol: SecurityProtocol,
    host: Option<String>,
    port: i32,
}

impl Endpoint {
    /// Java `new Endpoint(String, SecurityProtocol, String, int)`.
    ///
    /// `None` is Java null for listener name and host.
    #[must_use]
    pub fn new(
        listener_name: Option<String>,
        security_protocol: SecurityProtocol,
        host: Option<String>,
        port: i32,
    ) -> Self {
        Self {
            listener_name,
            security_protocol,
            host,
            port,
        }
    }

    /// Java `Endpoint.listenerName` (`Optional.ofNullable`).
    #[must_use]
    pub fn listener_name(&self) -> Option<&str> {
        self.listener_name.as_deref()
    }

    /// Java `Endpoint.securityProtocol`.
    #[must_use]
    pub fn security_protocol(&self) -> SecurityProtocol {
        self.security_protocol
    }

    /// Java `Endpoint.host`.
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    /// Java `Endpoint.port`.
    #[must_use]
    pub fn port(&self) -> i32 {
        self.port
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Endpoint(listenerName='{}', securityProtocol={}, host='{}', port={})",
            self.listener_name.as_deref().unwrap_or("null"),
            self.security_protocol,
            self.host.as_deref().unwrap_or("null"),
            self.port
        )
    }
}

/// Where a group member starts when OffsetFetch returns no committed offset.
///
/// Kafka `auto.offset.reset`. Java defaults to [`Self::Latest`]. This crate
/// defaults to [`Self::Earliest`] so a new group reads the existing log.
///
/// [`Display`] is Java `OffsetResetStrategy.toString` (`earliest`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AutoOffsetReset {
    /// Log start (offset `0` when the broker has no committed offset).
    #[default]
    Earliest,
    /// High watermark (`ListOffsets` latest).
    Latest,
    /// Fail join / rebalance if there is no committed offset.
    None,
}

impl AutoOffsetReset {
    /// Java `OffsetResetStrategy.toString` / `AutoOffsetResetStrategy.name`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::None => "none",
        }
    }
}

impl fmt::Display for AutoOffsetReset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// SASL mechanism. Set at most one.
///
/// ```
/// use partitionline::{ProducerConfig, Sasl};
///
/// let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
///     .sasl(Sasl::scram_sha256("alice", "secret"));
/// ```
#[derive(Debug, Clone)]
pub enum Sasl {
    /// SASL PLAIN (`username`, `password`).
    Plain {
        /// SASL username.
        username: String,
        /// SASL password.
        password: String,
    },
    /// RFC 5802 SCRAM-SHA-256.
    ScramSha256 {
        /// SASL username.
        username: String,
        /// SASL password.
        password: String,
    },
    /// RFC 5802 SCRAM-SHA-512.
    ScramSha512 {
        /// SASL username.
        username: String,
        /// SASL password.
        password: String,
    },
    /// Unsecured OAUTHBEARER JWT (`alg=none`), principal claim.
    OauthBearer {
        /// JWT `sub` / principal.
        principal: String,
    },
    /// RFC 6749 `client_credentials` token URL, then OAUTHBEARER.
    Oidc(OidcConfig),
}

impl Sasl {
    /// SASL PLAIN.
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Plain {
            username: username.into(),
            password: password.into(),
        }
    }

    /// SCRAM-SHA-256.
    pub fn scram_sha256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::ScramSha256 {
            username: username.into(),
            password: password.into(),
        }
    }

    /// SCRAM-SHA-512.
    pub fn scram_sha512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::ScramSha512 {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Unsecured OAUTHBEARER JWT for `principal`.
    pub fn oauthbearer(principal: impl Into<String>) -> Self {
        Self::OauthBearer {
            principal: principal.into(),
        }
    }

    /// OIDC client-credentials token endpoint.
    #[must_use]
    pub fn oidc(cfg: OidcConfig) -> Self {
        Self::Oidc(cfg)
    }

    pub(crate) fn apply_to(
        self,
        plain: &mut Option<(String, String)>,
        scram: &mut Option<(String, String)>,
        scram512: &mut Option<(String, String)>,
        oauth: &mut Option<String>,
        oidc: &mut Option<OidcConfig>,
    ) {
        *plain = None;
        *scram = None;
        *scram512 = None;
        *oauth = None;
        *oidc = None;
        match self {
            Self::Plain { username, password } => *plain = Some((username, password)),
            Self::ScramSha256 { username, password } => *scram = Some((username, password)),
            Self::ScramSha512 { username, password } => *scram512 = Some((username, password)),
            Self::OauthBearer { principal } => *oauth = Some(principal),
            Self::Oidc(cfg) => *oidc = Some(cfg),
        }
    }
}

/// TLS helpers shared by producer, consumer, and admin configs.
pub(crate) fn apply_tls(slot: &mut Option<TlsConfig>, tls: TlsConfig) {
    *slot = Some(tls);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn acks_roundtrip() {
        assert_eq!(Acks::None.as_i16(), 0);
        assert_eq!(Acks::Leader.as_i16(), 1);
        assert_eq!(Acks::All.as_i16(), -1);
        assert_eq!(Acks::from_i16(1), Some(Acks::Leader));
        assert_eq!(Acks::from_i16(2), None);
        assert_eq!(Acks::default(), Acks::Leader);
        assert_eq!(i16::from(Acks::All), -1);
        assert_eq!(i16::from(Acks::Leader), 1);
    }

    #[test]
    fn isolation_roundtrip() {
        assert_eq!(IsolationLevel::ReadUncommitted.as_i8(), 0);
        assert_eq!(IsolationLevel::ReadCommitted.as_i8(), 1);
        assert_eq!(IsolationLevel::ReadUncommitted.id(), 0);
        assert_eq!(IsolationLevel::ReadCommitted.id(), 1);
        assert_eq!(
            IsolationLevel::from_i8(1),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            IsolationLevel::from_id(1),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(IsolationLevel::from_i8(9), None);
        assert_eq!(IsolationLevel::from_id(9), None);
        assert_eq!(
            IsolationLevel::ReadUncommitted.to_string(),
            "read_uncommitted"
        );
        assert_eq!(IsolationLevel::ReadCommitted.to_string(), "read_committed");
        assert_eq!(IsolationLevel::ReadUncommitted.as_str(), "read_uncommitted");
    }

    #[test]
    fn security_protocol_matches_java() {
        assert_eq!(SecurityProtocol::Plaintext.id(), 0);
        assert_eq!(SecurityProtocol::Ssl.id(), 1);
        assert_eq!(SecurityProtocol::SaslPlaintext.id(), 2);
        assert_eq!(SecurityProtocol::SaslSsl.id(), 3);
        assert_eq!(SecurityProtocol::Plaintext.name(), "PLAINTEXT");
        assert_eq!(SecurityProtocol::Ssl.name(), "SSL");
        assert_eq!(SecurityProtocol::SaslPlaintext.name(), "SASL_PLAINTEXT");
        assert_eq!(SecurityProtocol::SaslSsl.name(), "SASL_SSL");
        assert_eq!(SecurityProtocol::Plaintext.to_string(), "PLAINTEXT");
        assert_eq!(SecurityProtocol::Ssl.to_string(), "SSL");
        assert_eq!(
            SecurityProtocol::SaslPlaintext.to_string(),
            "SASL_PLAINTEXT"
        );
        assert_eq!(SecurityProtocol::SaslSsl.to_string(), "SASL_SSL");
        assert_eq!(
            SecurityProtocol::from_id(0),
            Some(SecurityProtocol::Plaintext)
        );
        assert_eq!(SecurityProtocol::from_id(1), Some(SecurityProtocol::Ssl));
        assert_eq!(
            SecurityProtocol::from_id(2),
            Some(SecurityProtocol::SaslPlaintext)
        );
        assert_eq!(
            SecurityProtocol::from_id(3),
            Some(SecurityProtocol::SaslSsl)
        );
        assert_eq!(SecurityProtocol::from_id(4), None);
        assert_eq!(SecurityProtocol::from_id(-1), None);
        assert_eq!(
            SecurityProtocol::from_name("PLAINTEXT").unwrap(),
            SecurityProtocol::Plaintext
        );
        assert_eq!(
            SecurityProtocol::from_name("plaintext").unwrap(),
            SecurityProtocol::Plaintext
        );
        assert_eq!(
            SecurityProtocol::from_name("Ssl").unwrap(),
            SecurityProtocol::Ssl
        );
        assert_eq!(
            SecurityProtocol::from_name("sasl_plaintext").unwrap(),
            SecurityProtocol::SaslPlaintext
        );
        assert_eq!(
            SecurityProtocol::from_name("SASL_SSL").unwrap(),
            SecurityProtocol::SaslSsl
        );
        let unknown = SecurityProtocol::from_name("FOO").unwrap_err();
        assert!(
            unknown.to_string().contains(
                "No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.FOO"
            ),
            "got {unknown}"
        );
        let lower = SecurityProtocol::from_name("foo").unwrap_err();
        assert!(
            lower.to_string().contains("SecurityProtocol.FOO"),
            "Java valueOf uses the uppercased name, got {lower}"
        );
        assert_eq!(
            SecurityProtocol::names(),
            ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
        );
    }

    #[test]
    fn listener_name_matches_java() {
        let plaintext = ListenerName::for_security_protocol(SecurityProtocol::Plaintext);
        assert_eq!(plaintext.value(), "PLAINTEXT");
        assert_eq!(plaintext.to_string(), "ListenerName(PLAINTEXT)");
        assert_eq!(plaintext.config_prefix(), "listener.name.plaintext.");
        assert_eq!(
            plaintext.sasl_mechanism_config_prefix("PLAIN"),
            "listener.name.plaintext.plain."
        );
        assert_eq!(ListenerName::sasl_mechanism_prefix("PLAIN"), "plain.");
        assert_eq!(
            ListenerName::sasl_mechanism_prefix("SCRAM-SHA-256"),
            "scram-sha-256."
        );
        let client = ListenerName::normalised("client").unwrap();
        assert_eq!(client.value(), "CLIENT");
        assert_eq!(client.config_prefix(), "listener.name.client.");
        let mixed = ListenerName::normalised("Internal").unwrap();
        assert_eq!(mixed.value(), "INTERNAL");
        let kept = ListenerName::new("plain");
        assert_eq!(kept.value(), "plain");
        assert_eq!(kept.to_string(), "ListenerName(plain)");
        let empty = ListenerName::normalised("").unwrap_err();
        assert!(
            empty
                .to_string()
                .contains("The provided listener name is null or empty string"),
            "got {empty}"
        );
        let spaces = ListenerName::normalised("  \t").unwrap_err();
        assert!(
            spaces
                .to_string()
                .contains("The provided listener name is null or empty string"),
            "got {spaces}"
        );
        let nbsp = ListenerName::normalised("\u{00a0}").unwrap();
        assert_eq!(nbsp.value(), "\u{00a0}");
    }

    #[test]
    fn endpoint_matches_java() {
        let ep = Endpoint::new(
            Some("CLIENT".into()),
            SecurityProtocol::Plaintext,
            Some("localhost".into()),
            9092,
        );
        assert_eq!(ep.listener_name(), Some("CLIENT"));
        assert_eq!(ep.security_protocol(), SecurityProtocol::Plaintext);
        assert_eq!(ep.host(), Some("localhost"));
        assert_eq!(ep.port(), 9092);
        assert_eq!(
            ep.to_string(),
            "Endpoint(listenerName='CLIENT', securityProtocol=PLAINTEXT, host='localhost', port=9092)"
        );
        let none = Endpoint::new(None, SecurityProtocol::Ssl, None, 9093);
        assert_eq!(none.listener_name(), None);
        assert_eq!(none.host(), None);
        assert_eq!(
            none.to_string(),
            "Endpoint(listenerName='null', securityProtocol=SSL, host='null', port=9093)"
        );
        let sasl = Endpoint::new(
            Some("INTERNAL".into()),
            SecurityProtocol::SaslSsl,
            Some("broker.local".into()),
            9094,
        );
        assert_eq!(
            sasl.to_string(),
            "Endpoint(listenerName='INTERNAL', securityProtocol=SASL_SSL, host='broker.local', port=9094)"
        );
    }

    #[test]
    fn auto_offset_reset_default_is_earliest() {
        assert_eq!(AutoOffsetReset::default(), AutoOffsetReset::Earliest);
        assert_eq!(AutoOffsetReset::Earliest.to_string(), "earliest");
        assert_eq!(AutoOffsetReset::Latest.to_string(), "latest");
        assert_eq!(AutoOffsetReset::None.to_string(), "none");
        assert_eq!(AutoOffsetReset::Earliest.as_str(), "earliest");
    }

    #[test]
    fn retry_backoff_delay_doubles_then_caps() {
        let base = Duration::from_millis(100);
        let max = Duration::from_millis(1000);
        assert_eq!(retry_backoff_delay(base, max, 0), base);
        assert_eq!(
            retry_backoff_delay(base, max, 1),
            Duration::from_millis(200)
        );
        assert_eq!(
            retry_backoff_delay(base, max, 2),
            Duration::from_millis(400)
        );
        assert_eq!(
            retry_backoff_delay(base, max, 3),
            Duration::from_millis(800)
        );
        assert_eq!(retry_backoff_delay(base, max, 4), max);
        assert_eq!(retry_backoff_delay(base, max, 20), max);
        assert_eq!(retry_backoff_delay(Duration::ZERO, max, 5), Duration::ZERO);
        assert_eq!(
            retry_backoff_delay(base, Duration::from_millis(50), 3),
            base,
            "max below base still waits at least base"
        );
    }

    #[test]
    fn reconnect_backoff_delay_skips_first_then_doubles() {
        let base = Duration::from_millis(50);
        let max = Duration::from_millis(1000);
        assert_eq!(reconnect_backoff_delay(base, max, 0), Duration::ZERO);
        assert_eq!(reconnect_backoff_delay(base, max, 1), base);
        assert_eq!(
            reconnect_backoff_delay(base, max, 2),
            Duration::from_millis(100)
        );
        assert_eq!(
            reconnect_backoff_delay(base, max, 3),
            Duration::from_millis(200)
        );
        assert_eq!(reconnect_backoff_delay(base, max, 20), max);
        assert_eq!(
            reconnect_backoff_delay(Duration::ZERO, max, 4),
            Duration::ZERO
        );
        let mut fails = HashMap::new();
        assert_eq!(bump_reconnect_fails(&mut fails, 1), 1);
        assert_eq!(bump_reconnect_fails(&mut fails, 1), 2);
        assert_eq!(fails.get(&1).copied(), Some(2));
    }

    #[test]
    fn connection_idle_expired_zero_never_closes() {
        assert!(!connection_idle_expired(
            Duration::from_secs(3600),
            Duration::ZERO
        ));
        assert!(!connection_idle_expired(
            Duration::from_millis(29),
            Duration::from_millis(30)
        ));
        assert!(connection_idle_expired(
            Duration::from_millis(30),
            Duration::from_millis(30)
        ));
    }

    #[test]
    fn sasl_apply_clears_other_mechanisms() {
        let mut plain = Some(("old".into(), "x".into()));
        let mut scram = Some(("s".into(), "y".into()));
        let mut scram512 = None;
        let mut oauth = Some("p".into());
        let mut oidc = None;
        Sasl::scram_sha256("alice", "secret").apply_to(
            &mut plain,
            &mut scram,
            &mut scram512,
            &mut oauth,
            &mut oidc,
        );
        assert!(plain.is_none());
        assert_eq!(scram, Some(("alice".into(), "secret".into())));
        assert!(scram512.is_none());
        assert!(oauth.is_none());
        assert!(oidc.is_none());
    }
}
