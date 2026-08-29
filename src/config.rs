//! Shared client configuration types.
//!
//! These are the knobs you set before connecting. Field-by-field mutation on
//! [`ProducerConfig`](crate::ProducerConfig) still works; the builders here are
//! the shorter path.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

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

    /// Wire value sent on Fetch and ListOffsets.
    #[must_use]
    pub fn as_i8(self) -> i8 {
        self as i8
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
        assert_eq!(
            IsolationLevel::from_i8(1),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(IsolationLevel::from_i8(9), None);
        assert_eq!(
            IsolationLevel::ReadUncommitted.to_string(),
            "read_uncommitted"
        );
        assert_eq!(IsolationLevel::ReadCommitted.to_string(), "read_committed");
        assert_eq!(IsolationLevel::ReadUncommitted.as_str(), "read_uncommitted");
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
