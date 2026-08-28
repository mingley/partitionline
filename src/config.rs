//! Shared client configuration types.
//!
//! These are the knobs you set before connecting. Field-by-field mutation on
//! [`ProducerConfig`](crate::ProducerConfig) still works; the builders here are
//! the shorter path.

use crate::net::TlsConfig;
use crate::protocol::oidc::OidcConfig;

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

/// Fetch isolation. Matches Kafka `isolation.level`.
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

/// Where a group member starts when OffsetFetch returns no committed offset.
///
/// Kafka `auto.offset.reset`. Java defaults to [`Self::Latest`]. This crate
/// defaults to [`Self::Earliest`] so a new group reads the existing log.
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

    #[test]
    fn acks_roundtrip() {
        assert_eq!(Acks::None.as_i16(), 0);
        assert_eq!(Acks::Leader.as_i16(), 1);
        assert_eq!(Acks::All.as_i16(), -1);
        assert_eq!(Acks::from_i16(1), Some(Acks::Leader));
        assert_eq!(Acks::from_i16(2), None);
        assert_eq!(Acks::default(), Acks::Leader);
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
    }

    #[test]
    fn auto_offset_reset_default_is_earliest() {
        assert_eq!(AutoOffsetReset::default(), AutoOffsetReset::Earliest);
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
