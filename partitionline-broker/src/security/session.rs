//! Kafka SASL connection policy and credential administration.
//!
//! State belongs to one actual socket. Application requests become dispatchable
//! only after a successful proof. PLAIN requires the TLS profile; plaintext
//! SCRAM is a separate explicit profile. Optional OAUTHBEARER requires TLS and
//! a managed HTTPS authority; finite token/key/revocation leases govern the
//! entire socket lifetime. Credential and issuer/subject admin allowlists are
//! separate authority families. Admin operations require an exact
//! principal allowlist and use only the durable store. Reauthentication is an
//! explicit listener opt-in; default Authenticate v1/v2 lifetime remains zero.
//!
//! Original wire frames are zeroizing transport buffers. Imported SaltedPassword
//! copies use Secret; neither Debug nor errors include credentials or messages.
//! Caller-selected advertisements must match the installed application handler.

use super::{
    credentials::{self, Change, Store},
    sasl::{self, Algorithm, Identity, PlainSession, ScramSession, Secret},
};
use crate::protocol::{ApiVersion, ApiVersionsHandler, RequestHeader};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

/// Negotiation plus actually wired SASL and durable credential APIs.
pub const SASL_API_VERSIONS: [ApiVersion; 5] = [
    ApiVersion {
        api_key: 17,
        min_version: 0,
        max_version: 1,
    },
    ApiVersion {
        api_key: 18,
        min_version: 0,
        max_version: 4,
    },
    ApiVersion {
        api_key: 36,
        min_version: 0,
        max_version: 2,
    },
    ApiVersion {
        api_key: 50,
        min_version: 0,
        max_version: 0,
    },
    ApiVersion {
        api_key: 51,
        min_version: 0,
        max_version: 0,
    },
];
/// SASL profile composed with the actual MetadataHandler's four API ranges.
/// Select this only when that handler is installed behind authentication.
pub const SASL_METADATA_API_VERSIONS: [ApiVersion; 8] = [
    ApiVersion {
        api_key: 3,
        min_version: 0,
        max_version: 13,
    },
    SASL_API_VERSIONS[0],
    SASL_API_VERSIONS[1],
    ApiVersion {
        api_key: 19,
        min_version: 2,
        max_version: 4,
    },
    ApiVersion {
        api_key: 20,
        min_version: 1,
        max_version: 6,
    },
    SASL_API_VERSIONS[2],
    SASL_API_VERSIONS[3],
    SASL_API_VERSIONS[4],
];
/// TLS-only OAuth listener: no credential administration is installed.
#[cfg(feature = "oidc")]
pub const OIDC_API_VERSIONS: [ApiVersion; 3] = [
    SASL_API_VERSIONS[0],
    SASL_API_VERSIONS[1],
    SASL_API_VERSIONS[2],
];
/// OAuth listener composed with the actual MetadataHandler's four API ranges.
#[cfg(feature = "oidc")]
pub const OIDC_METADATA_API_VERSIONS: [ApiVersion; 6] = [
    SASL_METADATA_API_VERSIONS[0],
    OIDC_API_VERSIONS[0],
    OIDC_API_VERSIONS[1],
    SASL_METADATA_API_VERSIONS[3],
    SASL_METADATA_API_VERSIONS[4],
    OIDC_API_VERSIONS[2],
];
/// Exact issuer/subject administrator authority; never a credential name alias.
#[cfg(feature = "oidc")]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct OAuthAdministrator {
    /// Exact validated HTTPS issuer.
    pub issuer: String,
    /// Exact validated subject within that issuer.
    pub subject: String,
}
#[cfg(feature = "oidc")]
impl OAuthAdministrator {
    fn valid(&self) -> bool {
        if self.subject.is_empty()
            || self.subject.len() > 1024
            || self.subject.chars().any(char::is_control)
            || self.issuer.len() > 2048
        {
            return false;
        }
        let Ok(uri) = self.issuer.parse::<::http::Uri>() else {
            return false;
        };
        let (Some(host), Some(authority)) = (uri.host(), uri.authority()) else {
            return false;
        };
        let Some(suffix) = authority.as_str().get(host.len()..) else {
            return false;
        };
        uri.scheme_str() == Some("https")
            && !host.is_empty()
            && host.len() <= 256
            && !authority.as_str().contains('@')
            && (suffix.is_empty() || (suffix.starts_with(':') && uri.port_u16().is_some()))
            && uri.port_u16() != Some(0)
            && uri.query().is_none()
            && !self.issuer.contains('#')
    }
}
#[cfg(feature = "oidc")]
impl fmt::Debug for OAuthAdministrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OAuthAdministrator { [REDACTED] }")
    }
}
/// Validated per-socket control, parsing and deadline ceilings.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Absolute time from socket admission through authentication, at most 60s.
    /// Each opt-in renewal gets one deadline bounded by this duration and the
    /// previous session expiry; authentication rounds never restart it.
    pub preauth_timeout: Duration,
    /// Total authentication wire bytes, including length prefixes; 1KiB through
    /// 1MiB. A renewal resets this budget once, including its Handshake request.
    pub preauth_bytes: usize,
    /// Maximum one preauth/control payload, 256 bytes through 64KiB.
    /// Post-proof buffers use the transport allocation cap; known control APIs
    /// enforce this smaller logical cap before parsing, copies or work.
    pub frame_bytes: usize,
    /// Total requests/tokens per authentication exchange, 2 through 32.
    pub control_rounds: usize,
    /// Maximum users/changes in an admin frame, 1 through 1024.
    pub admin_entries: usize,
    /// Maximum tags in each header/body/struct block, 0 through 128.
    pub tagged_fields: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            preauth_timeout: Duration::from_secs(10),
            preauth_bytes: 65536,
            frame_bytes: 16384,
            control_rounds: 8,
            admin_entries: 64,
            tagged_fields: 32,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if self.preauth_timeout.is_zero()
            || self.preauth_timeout > Duration::from_secs(60)
            || !(1024..=1024 * 1024).contains(&self.preauth_bytes)
            || !(256..=65536).contains(&self.frame_bytes)
            || self.frame_bytes + 4 > self.preauth_bytes
            || !(2..=32).contains(&self.control_rounds)
            || !(1..=1024).contains(&self.admin_entries)
            || self.tagged_fields > 128
        {
            return Err(Error::InvalidProfile);
        }
        Ok(())
    }
}
/// Redacted profile or connection failure, with no attacker-controlled text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Invalid limits, principals or advertised APIs.
    InvalidProfile,
    /// Malformed, unsupported or incompletely consumed Kafka wire message.
    Malformed,
    /// Absolute deadline, cumulative bytes or rounds exhausted.
    Budget,
    /// Stopped or poisoned credential service.
    Unavailable,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SASL connection: {self:?}")
    }
}
impl std::error::Error for Error {}
struct Configuration {
    store: Option<Store>,
    administrators: HashSet<String>,
    limits: Limits,
    advertised: &'static [ApiVersion],
    tls: bool,
    reauthentication: Option<Duration>,
    #[cfg(feature = "oidc")]
    oidc: Option<super::oidc::Service>,
    #[cfg(feature = "oidc")]
    oauth_administrators: HashSet<OAuthAdministrator>,
}
impl Configuration {
    fn store(&self) -> Result<&Store, Error> {
        self.store.as_ref().ok_or(Error::Unavailable)
    }
    fn healthy(&self) -> bool {
        self.store.as_ref().is_none_or(Store::is_healthy)
    }
    fn message_bytes(&self) -> usize {
        #[cfg(feature = "oidc")]
        if self.oidc.is_some() {
            return self.limits.frame_bytes;
        }
        self.store.as_ref().map_or(self.limits.frame_bytes, |s| {
            s.mechanism_limits().message_bytes
        })
    }
    fn authorized(&self, identity: &Identity) -> bool {
        match identity.authority() {
            sasl::Authority::Credential => self.administrators.contains(identity.name()),
            #[cfg(feature = "oidc")]
            sasl::Authority::Oidc { issuer } => {
                self.oauth_administrators.contains(&OAuthAdministrator {
                    issuer: issuer.clone(),
                    subject: identity.name().to_owned(),
                })
            }
        }
    }
}
/// Immutable listener profile. Default advertisements contain the five SASL APIs.
#[derive(Clone)]
pub struct Profile(Arc<Configuration>);
impl fmt::Debug for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Profile { [REDACTED] }")
    }
}
impl Profile {
    /// TLS-only PLAIN/SCRAM profile with an explicit administrator allowlist.
    pub fn tls(store: Store, administrators: Vec<String>, limits: Limits) -> Result<Self, Error> {
        Self::new(store, administrators, limits, true)
    }
    /// Explicit plaintext SCRAM-only profile; PLAIN is never offered or accepted.
    pub fn scram_plaintext(
        store: Store,
        administrators: Vec<String>,
        limits: Limits,
    ) -> Result<Self, Error> {
        Self::new(store, administrators, limits, false)
    }
    fn new(
        store: Store,
        administrators: Vec<String>,
        limits: Limits,
        tls: bool,
    ) -> Result<Self, Error> {
        limits.validate()?;
        if !store.is_healthy()
            || administrators.len() > 128
            || administrators
                .iter()
                .any(|p| !store.mechanism_limits().identity(p))
        {
            return Err(Error::InvalidProfile);
        }
        let count = administrators.len();
        let administrators: HashSet<_> = administrators.into_iter().collect();
        if count != administrators.len() {
            return Err(Error::InvalidProfile);
        }
        Ok(Self(Arc::new(Configuration {
            store: Some(store),
            administrators,
            limits,
            advertised: &SASL_API_VERSIONS,
            tls,
            reauthentication: None,
            #[cfg(feature = "oidc")]
            oidc: None,
            #[cfg(feature = "oidc")]
            oauth_administrators: HashSet::new(),
        })))
    }
    /// TLS-only OAUTHBEARER listener backed by an actual managed authority.
    /// No credential APIs or credential mechanisms are installed.
    /// Reauthentication requires the explicit listener policy below; default
    /// clients reconnect after their finite authority lease expires.
    #[cfg(feature = "oidc")]
    pub fn oidc_tls(service: super::oidc::Service, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self(Arc::new(Configuration {
            store: None,
            administrators: HashSet::new(),
            limits,
            advertised: &OIDC_API_VERSIONS,
            tls: true,
            reauthentication: None,
            oidc: Some(service),
            oauth_administrators: HashSet::new(),
        })))
    }
    /// Add managed OAuth to a TLS credential profile with a separate exact
    /// issuer/subject admin allowlist. An empty allowlist denies OAuth admins.
    #[cfg(feature = "oidc")]
    pub fn with_oidc(
        self,
        service: super::oidc::Service,
        administrators: Vec<OAuthAdministrator>,
    ) -> Result<Self, Error> {
        if !self.0.tls || administrators.len() > 128 || administrators.iter().any(|a| !a.valid()) {
            return Err(Error::InvalidProfile);
        }
        let count = administrators.len();
        let administrators: HashSet<_> = administrators.into_iter().collect();
        if count != administrators.len() {
            return Err(Error::InvalidProfile);
        }
        Ok(Self(Arc::new(Configuration {
            store: self.0.store.clone(),
            administrators: self.0.administrators.clone(),
            limits: self.0.limits,
            advertised: self.0.advertised,
            tls: true,
            reauthentication: self.0.reauthentication,
            oidc: Some(service),
            oauth_administrators: administrators,
        })))
    }
    /// Select the complete API allowlist actually installed on this listener.
    /// All installed SASL ranges must occur exactly once; OAuth-only profiles
    /// require three ranges and exclude credential admin APIs. Ranges are sorted/unique.
    /// Additional APIs are dispatchable only after proof; inventory is no proof
    /// of support. The caller is responsible for matching its installed handler.
    pub fn with_advertised(self, advertised: &'static [ApiVersion]) -> Result<Self, Error> {
        if advertised.len() > 128
            || advertised.windows(2).any(|w| w[0].api_key >= w[1].api_key)
            || advertised
                .iter()
                .any(|a| a.min_version < 0 || a.min_version > a.max_version)
            || (if self.0.store.is_some() {
                &SASL_API_VERSIONS[..]
            } else {
                #[cfg(feature = "oidc")]
                {
                    &OIDC_API_VERSIONS[..]
                }
                #[cfg(not(feature = "oidc"))]
                {
                    &SASL_API_VERSIONS[..]
                }
            })
            .iter()
            .any(|a| advertised.iter().find(|b| b.api_key == a.api_key) != Some(a))
            || (self.0.store.is_none() && advertised.iter().any(|a| matches!(a.api_key, 50 | 51)))
        {
            return Err(Error::InvalidProfile);
        }
        Ok(Self(Arc::new(Configuration {
            store: self.0.store.clone(),
            administrators: self.0.administrators.clone(),
            limits: self.0.limits,
            advertised,
            tls: self.0.tls,
            reauthentication: self.0.reauthentication,
            #[cfg(feature = "oidc")]
            oidc: self.0.oidc.clone(),
            #[cfg(feature = "oidc")]
            oauth_administrators: self.0.oauth_administrators.clone(),
        })))
    }
    pub(crate) fn requires_tls(&self) -> bool {
        self.0.tls
    }
    /// Opt into Kafka Handshake1/Authenticate1-2 connection renewal. The maximum
    /// lifetime is an exact whole millisecond from 1ms through 24 hours. OAuth
    /// additionally caps it at the current token/key/revocation lease. Initial
    /// legacy/authenticate0 clients retain lifetime zero and cannot renew.
    /// Renewal permits fresh credential generations, never another mechanism,
    /// principal, or authority. The transport drains each previous reply before
    /// the handshake and forbids application dispatch during authentication.
    pub fn with_reauthentication(self, lifetime: Duration) -> Result<Self, Error> {
        if lifetime < Duration::from_millis(1)
            || lifetime > Duration::from_secs(24 * 60 * 60)
            || !lifetime.subsec_nanos().is_multiple_of(1_000_000)
        {
            return Err(Error::InvalidProfile);
        }
        Ok(Self(Arc::new(Configuration {
            store: self.0.store.clone(),
            administrators: self.0.administrators.clone(),
            limits: self.0.limits,
            advertised: self.0.advertised,
            tls: self.0.tls,
            reauthentication: Some(lifetime),
            #[cfg(feature = "oidc")]
            oidc: self.0.oidc.clone(),
            #[cfg(feature = "oidc")]
            oauth_administrators: self.0.oauth_administrators.clone(),
        })))
    }
    pub(crate) fn admit(&self) -> Session {
        Session {
            profile: self.clone(),
            state: State::Handshake,
            deadline: Instant::now() + self.0.limits.preauth_timeout,
            bytes: 0,
            rounds: 0,
            mechanism: None,
            session_deadline: None,
            renewal: None,
            #[cfg(feature = "oidc")]
            lease: None,
        }
    }
}
enum Mechanism {
    #[cfg(feature = "oidc")]
    OAuth,
    Plain(PlainSession),
    Scram {
        session: ScramSession,
        challenged: bool,
    },
}
enum State {
    Handshake,
    Negotiated {
        mechanism: Mechanism,
        legacy: bool,
    },
    Authenticated(Arc<Identity>),
    #[cfg(feature = "oidc")]
    OAuthFailureAck {
        legacy: bool,
    },
    Failed,
}
struct Renewal {
    identity: Arc<Identity>,
    deadline: Instant,
    #[cfg(feature = "oidc")]
    lease: Option<super::oidc::Lease>,
}
pub(crate) struct Session {
    profile: Profile,
    state: State,
    deadline: Instant,
    bytes: usize,
    rounds: usize,
    mechanism: Option<&'static str>,
    session_deadline: Option<Instant>,
    renewal: Option<Renewal>,
    #[cfg(feature = "oidc")]
    lease: Option<super::oidc::Lease>,
}
pub(crate) enum Decision {
    Reply { bytes: Vec<u8>, close: bool },
    Dispatch,
}
impl Session {
    #[cfg(feature = "oidc")]
    pub(crate) fn lease(&self) -> Option<super::oidc::Lease> {
        // Retain the previous authority through successful proof response
        // writing; cancellation never transfers that authority early.
        self.renewal
            .as_ref()
            .map_or_else(|| self.lease.clone(), |r| r.lease.clone())
    }
    pub(crate) fn identity(&self) -> Option<Arc<Identity>> {
        if self.renewal.is_some() {
            return None;
        }
        match &self.state {
            State::Authenticated(identity) => Some(identity.clone()),
            _ => None,
        }
    }
    pub(crate) fn preauth_deadline(&self) -> Option<Instant> {
        if let Some(renewal) = &self.renewal {
            return Some(renewal.deadline);
        }
        if matches!(self.state, State::Authenticated(_)) {
            None
        } else {
            Some(self.deadline)
        }
    }
    pub(crate) fn operation_deadline(&self) -> Option<Instant> {
        match (self.preauth_deadline(), self.session_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
    /// Charge renewal admission wait to the same deadline as its proof rounds.
    /// This is captured once immediately after the complete frame is received,
    /// before waiting for the global handler slot or validating the handshake.
    pub(crate) fn request_deadline(&self, request: &[u8], received: Instant) -> Option<Instant> {
        let deadline = self.operation_deadline();
        if matches!(self.state, State::Authenticated(_))
            && self.session_deadline.is_some()
            && request.get(..2) == Some(&17i16.to_be_bytes()[..])
        {
            let renewal = received + self.profile.0.limits.preauth_timeout;
            Some(deadline.map_or(renewal, |deadline| deadline.min(renewal)))
        } else {
            deadline
        }
    }
    /// Commit renewal authority only after its successful final reply is written.
    pub(crate) fn response_written(&mut self) -> Result<(), Error> {
        if self
            .operation_deadline()
            .is_some_and(|d| Instant::now() >= d)
        {
            self.state = State::Failed;
            return Err(Error::Budget);
        }
        #[cfg(feature = "oidc")]
        if self.lease().is_some_and(|l| l.failure().is_some())
            || self.lease.as_ref().is_some_and(|l| l.failure().is_some())
        {
            self.state = State::Failed;
            return Err(Error::Unavailable);
        }
        if matches!(self.state, State::Authenticated(_)) {
            self.renewal = None;
        }
        Ok(())
    }
    fn charge(&mut self, request: &[u8]) -> Result<(), Error> {
        self.rounds = self.rounds.checked_add(1).ok_or(Error::Budget)?;
        self.bytes = self
            .bytes
            .checked_add(request.len().checked_add(4).ok_or(Error::Budget)?)
            .ok_or(Error::Budget)?;
        if Instant::now() >= self.deadline
            || self.rounds > self.profile.0.limits.control_rounds
            || self.bytes > self.profile.0.limits.preauth_bytes
        {
            self.state = State::Failed;
            return Err(Error::Budget);
        }
        Ok(())
    }
    fn begin_renewal(&mut self, request: &[u8], received: Instant) -> Result<(), Error> {
        let State::Authenticated(identity) = &self.state else {
            return Err(Error::Malformed);
        };
        let expires = self.session_deadline.ok_or(Error::Malformed)?;
        let now = Instant::now();
        if now >= expires || self.profile.0.reauthentication.is_none() {
            return Err(Error::Budget);
        }
        let deadline = expires.min(received + self.profile.0.limits.preauth_timeout);
        self.renewal = Some(Renewal {
            identity: identity.clone(),
            deadline,
            #[cfg(feature = "oidc")]
            lease: self.lease.clone(),
        });
        self.deadline = deadline;
        self.bytes = 0;
        self.rounds = 0;
        self.state = State::Handshake;
        self.charge(request)
    }
    fn complete(&mut self, identity: Identity, framed: Option<(i32, i16)>) -> Result<i64, Error> {
        let now = Instant::now();
        if now >= self.deadline {
            return Err(Error::Budget);
        }
        if let Some(renewal) = &self.renewal {
            if now >= renewal.deadline
                || identity.name() != renewal.identity.name()
                || identity.authority() != renewal.identity.authority()
            {
                return Err(Error::Malformed);
            }
        }
        let lifetime = match (self.profile.0.reauthentication, framed) {
            (Some(maximum), Some((_, 1 | 2))) => {
                #[allow(unused_mut)]
                let mut duration = maximum;
                #[cfg(feature = "oidc")]
                if let Some(lease) = &self.lease {
                    if lease.failure().is_some() {
                        return Err(Error::Unavailable);
                    }
                    duration = duration.min(lease.deadline().saturating_duration_since(now));
                }
                let millis = i64::try_from(duration.as_millis()).map_err(|_| Error::Budget)?;
                if millis == 0 {
                    return Err(Error::Budget);
                }
                self.session_deadline = Some(
                    now + Duration::from_millis(u64::try_from(millis).map_err(|_| Error::Budget)?),
                );
                millis
            }
            _ => {
                self.session_deadline = None;
                0
            }
        };
        self.state = State::Authenticated(Arc::new(identity));
        Ok(lifetime)
    }
    pub(crate) fn frame_limit(&self, configured: usize) -> usize {
        if self.preauth_deadline().is_some() {
            configured.min(self.profile.0.limits.frame_bytes).min(
                self.profile
                    .0
                    .limits
                    .preauth_bytes
                    .saturating_sub(self.bytes + 4),
            )
        } else {
            configured
        }
    }
    pub(crate) async fn handle(
        &mut self,
        request: &[u8],
        received: Instant,
    ) -> Result<Decision, Error> {
        if matches!(self.state, State::Failed) {
            return Err(Error::Malformed);
        }
        if !self.profile.0.healthy() {
            self.state = State::Failed;
            return Err(Error::Unavailable);
        }
        if self
            .operation_deadline()
            .is_some_and(|d| Instant::now() >= d)
        {
            self.state = State::Failed;
            return Err(Error::Budget);
        }
        if self.preauth_deadline().is_some() {
            self.charge(request)?;
        }
        #[cfg(feature = "oidc")]
        if self.lease().is_some_and(|lease| lease.failure().is_some()) {
            self.state = State::Failed;
            return Err(Error::Unavailable);
        }
        #[cfg(feature = "oidc")]
        if matches!(self.state, State::OAuthFailureAck { legacy: true }) {
            return self.authenticate(request, None).await;
        }
        if matches!(self.state, State::Negotiated { legacy: true, .. }) {
            return self.authenticate(request, None).await;
        }
        let key = i16::from_be_bytes(
            request
                .get(..2)
                .ok_or(Error::Malformed)?
                .try_into()
                .map_err(|_| Error::Malformed)?,
        );
        let version = i16::from_be_bytes(
            request
                .get(2..4)
                .ok_or(Error::Malformed)?
                .try_into()
                .map_err(|_| Error::Malformed)?,
        );
        // Authenticated application frames retain the caller's transport cap.
        // Enforce the smaller control cap before header parsing/field copies or
        // work admission; original post-proof allocation is still transport-capped.
        if matches!(key, 17 | 36 | 50 | 51) && request.len() > self.profile.0.limits.frame_bytes {
            self.state = State::Failed;
            return Err(Error::Budget);
        }
        #[cfg(feature = "oidc")]
        if matches!(self.state, State::OAuthFailureAck { .. }) && key != 36 {
            self.state = State::Failed;
            return Err(Error::Malformed);
        }
        if key == 18 {
            if self.renewal.is_some() {
                self.state = State::Failed;
                return Err(Error::Malformed);
            }
            let limits = crate::protocol::Limits::new(
                self.profile.0.limits.frame_bytes,
                self.profile.0.limits.tagged_fields,
            )
            .map_err(|_| Error::InvalidProfile)?;
            let response = ApiVersionsHandler::with_advertised(limits, self.profile.0.advertised)
                .respond(request)
                .map_err(|_| Error::Malformed)?;
            return Ok(Decision::Reply {
                bytes: response,
                close: false,
            });
        }
        let flexible = (key == 36 && version >= 2) || matches!(key, 50 | 51);
        let limits =
            crate::protocol::Limits::new(request.len().max(1), self.profile.0.limits.tagged_fields)
                .map_err(|_| Error::Malformed)?;
        let (header, body) = RequestHeader::parse(request, if flexible { 2 } else { 1 }, limits)
            .map_err(|_| Error::Malformed)?;
        match key {
            17 if matches!(version, 0 | 1) => {
                let mut reader = Reader::new(body, self.profile.0.limits);
                let mechanism = reader.classic_string(32)?;
                reader.finish()?;
                let mechanism_name = match mechanism {
                    "PLAIN" => Some("PLAIN"),
                    "SCRAM-SHA-256" => Some("SCRAM-SHA-256"),
                    "SCRAM-SHA-512" => Some("SCRAM-SHA-512"),
                    #[cfg(feature = "oidc")]
                    "OAUTHBEARER" => Some("OAUTHBEARER"),
                    _ => None,
                };
                let mut error = 0;
                if matches!(self.state, State::Authenticated(_)) {
                    if version != 1 || self.session_deadline.is_none() {
                        error = 34;
                    } else if self.mechanism != Some(mechanism) {
                        error = if mechanism_name.is_none() { 33 } else { 58 };
                    } else if self.begin_renewal(request, received).is_err() {
                        error = 58;
                    }
                }
                if error != 0 || !matches!(self.state, State::Handshake) {
                    if error == 0 {
                        error = 34;
                    }
                } else {
                    let admitted = match mechanism {
                        #[cfg(feature = "oidc")]
                        "OAUTHBEARER" if self.profile.0.oidc.is_some() && self.profile.0.tls => {
                            Ok(Mechanism::OAuth)
                        }
                        "PLAIN" if self.profile.0.tls && self.profile.0.store.is_some() => {
                            self.profile.0.store()?.begin_plain().map(Mechanism::Plain)
                        }
                        "SCRAM-SHA-256" if self.profile.0.store.is_some() => self
                            .profile
                            .0
                            .store()?
                            .scram(Algorithm::Sha256)
                            .map(|session| Mechanism::Scram {
                                session,
                                challenged: false,
                            }),
                        "SCRAM-SHA-512" if self.profile.0.store.is_some() => self
                            .profile
                            .0
                            .store()?
                            .scram(Algorithm::Sha512)
                            .map(|session| Mechanism::Scram {
                                session,
                                challenged: false,
                            }),
                        _ => {
                            error = 33;
                            Err(sasl::Error::InvalidMessage)
                        }
                    };
                    match admitted {
                        Ok(mechanism) => {
                            self.mechanism = mechanism_name;
                            self.state = State::Negotiated {
                                mechanism,
                                legacy: version == 0,
                            }
                        }
                        Err(_) if error == 0 => error = 58,
                        Err(_) => {}
                    }
                }
                if error != 0 {
                    self.state = State::Failed;
                }
                Ok(Decision::Reply {
                    bytes: self.handshake_response(header.correlation_id, error),
                    close: error != 0,
                })
            }
            36 if (0..=2).contains(&version) => {
                if self.renewal.is_some() && version == 0 {
                    self.state = State::Failed;
                    return Ok(Decision::Reply {
                        bytes: authenticate_response(header.correlation_id, version, 34, &[]),
                        close: true,
                    });
                }
                let mut reader = Reader::new(body, self.profile.0.limits);
                let message = reader.bytes(flexible, self.profile.0.message_bytes())?;
                if flexible {
                    reader.tags()?;
                }
                reader.finish()?;
                self.authenticate(message, Some((header.correlation_id, version)))
                    .await
            }
            50 | 51 if version == 0 => {
                if self.renewal.is_some()
                    || !matches!(self.state, State::Authenticated(_))
                    || self.profile.0.store.is_none()
                {
                    self.state = State::Failed;
                    return Err(Error::Malformed);
                }
                let authorized = self
                    .identity()
                    .is_some_and(|i| self.profile.0.authorized(&i));
                let response = if key == 50 {
                    self.describe(header.correlation_id, body, authorized)
                        .await?
                } else {
                    self.alter(header.correlation_id, body, authorized).await?
                };
                Ok(Decision::Reply {
                    bytes: response,
                    close: !self.profile.0.healthy(),
                })
            }
            _ => {
                if self.renewal.is_some() || !matches!(self.state, State::Authenticated(_)) {
                    self.state = State::Failed;
                    return Err(Error::Malformed);
                }
                if !self
                    .profile
                    .0
                    .advertised
                    .iter()
                    .any(|a| a.api_key == key && (a.min_version..=a.max_version).contains(&version))
                {
                    return Err(Error::Malformed);
                }
                Ok(Decision::Dispatch)
            }
        }
    }
    fn handshake_response(&self, correlation: i32, error: i16) -> Vec<u8> {
        let mut names = Vec::new();
        if self.profile.0.store.is_some() {
            if self.profile.0.tls {
                names.push("PLAIN");
            }
            names.extend(["SCRAM-SHA-256", "SCRAM-SHA-512"]);
        }
        #[cfg(feature = "oidc")]
        if self.profile.0.oidc.is_some() {
            names.push("OAUTHBEARER");
        }
        handshake_response_names(correlation, error, &names)
    }
    async fn authenticate(
        &mut self,
        message: &[u8],
        framed: Option<(i32, i16)>,
    ) -> Result<Decision, Error> {
        if message.len() > self.profile.0.message_bytes() {
            self.state = State::Failed;
            return Err(Error::Budget);
        }
        let state = std::mem::replace(&mut self.state, State::Failed);
        #[cfg(feature = "oidc")]
        match state {
            State::Negotiated {
                mechanism: Mechanism::OAuth,
                legacy,
            } => {
                return self.oauth(message, framed, legacy).await;
            }
            State::OAuthFailureAck { .. } => {
                if message != [1] {
                    return Err(Error::Malformed);
                }
                return match framed {
                    Some((correlation, version)) => Ok(Decision::Reply {
                        bytes: authenticate_response(correlation, version, 58, &[]),
                        close: true,
                    }),
                    None => Err(Error::Malformed),
                };
            }
            _ => {}
        }
        let mut complete = None;
        let result = match state {
            State::Negotiated {
                mechanism: Mechanism::Plain(session),
                ..
            } => session
                .finish(Secret::new(message.to_vec()))
                .await
                .map(|identity| {
                    complete = Some(identity);
                    Vec::new()
                }),
            State::Negotiated {
                mechanism:
                    Mechanism::Scram {
                        mut session,
                        challenged: false,
                    },
                legacy,
            } => match session.challenge(message) {
                Ok(response) => {
                    self.state = State::Negotiated {
                        mechanism: Mechanism::Scram {
                            session,
                            challenged: true,
                        },
                        legacy,
                    };
                    Ok(response)
                }
                Err(error) => Err(error),
            },
            State::Negotiated {
                mechanism:
                    Mechanism::Scram {
                        session,
                        challenged: true,
                    },
                ..
            } => session
                .finish(Secret::new(message.to_vec()))
                .await
                .map(|result| {
                    complete = Some(result.identity);
                    result.message
                }),
            _ => {
                if let Some((correlation, version)) = framed {
                    return Ok(Decision::Reply {
                        bytes: authenticate_response(correlation, version, 34, &[]),
                        close: true,
                    });
                }
                return Err(Error::Malformed);
            }
        };
        let mut success = result.is_ok() && self.profile.0.healthy();
        let mut lifetime = 0;
        if success {
            if let Some(identity) = complete {
                match self.complete(identity, framed) {
                    Ok(value) => lifetime = value,
                    Err(_) => success = false,
                }
            }
        }
        if !success {
            self.state = State::Failed;
        }
        match framed {
            Some((correlation, version)) => Ok(Decision::Reply {
                bytes: authenticate_response_lifetime(
                    correlation,
                    version,
                    if success { 0 } else { 58 },
                    if success {
                        result.as_ref().map_or(&[], Vec::as_slice)
                    } else {
                        &[]
                    },
                    lifetime,
                ),
                close: !success,
            }),
            None if success => Ok(Decision::Reply {
                bytes: result.map_err(|_| Error::Unavailable)?,
                close: false,
            }),
            None => Err(Error::Malformed),
        }
    }
    #[cfg(feature = "oidc")]
    async fn oauth(
        &mut self,
        message: &[u8],
        framed: Option<(i32, i16)>,
        legacy: bool,
    ) -> Result<Decision, Error> {
        let result = async {
            let initial = super::oidc::sasl::initial(message)?;
            let service = self
                .profile
                .0
                .oidc
                .as_ref()
                .ok_or(super::oidc::Error::Unavailable)?;
            let lease = service.validate(initial.token).await?;
            if initial
                .authorization
                .as_ref()
                .is_some_and(|a| a != lease.subject())
            {
                return Err(super::oidc::Error::Authentication);
            }
            if let Some(error) = lease.failure() {
                return Err(error);
            }
            if !self.profile.0.healthy() {
                return Err(super::oidc::Error::Unavailable);
            }
            Ok(lease)
        }
        .await;
        let mut lifetime = 0;
        let result = result.and_then(|lease| {
            let identity = Identity::from_oidc(&lease);
            self.lease = Some(lease);
            self.complete(identity, framed)
                .map(|value| {
                    lifetime = value;
                })
                .map_err(|_| super::oidc::Error::Authentication)
        });
        let bytes = match result {
            Ok(()) => Vec::new(),
            Err(_) => {
                self.state = State::OAuthFailureAck { legacy };
                b"{\"status\":\"invalid_token\"}".to_vec()
            }
        };
        Ok(Decision::Reply {
            bytes: framed.map_or_else(
                || bytes.clone(),
                |(correlation, version)| {
                    authenticate_response_lifetime(correlation, version, 0, &bytes, lifetime)
                },
            ),
            close: false,
        })
    }
    async fn describe(
        &self,
        correlation: i32,
        body: &[u8],
        authorized: bool,
    ) -> Result<Vec<u8>, Error> {
        let mut reader = Reader::new(body, self.profile.0.limits);
        let count = reader.count(true)?;
        let mut users = Vec::new();
        for _ in 0..count.unwrap_or(0) {
            users.push(
                reader
                    .compact_string(self.profile.0.store()?.mechanism_limits().identity_bytes)?
                    .to_owned(),
            );
            reader.tags()?;
        }
        reader.tags()?;
        reader.finish()?;
        let mut output = response_header(correlation, true);
        output.extend_from_slice(&0i32.to_be_bytes());
        if !authorized {
            output.extend_from_slice(&31i16.to_be_bytes());
            output.push(0);
            output.push(1);
            output.push(0);
            return Ok(output);
        }
        let all = users.is_empty();
        let infos = self
            .profile
            .0
            .store()?
            .describe(if all { None } else { Some(users.clone()) })
            .await;
        let (infos, error) = match infos {
            Ok(infos) => (infos, 0),
            Err(error) => (Vec::new(), storage_code(error)),
        };
        output.extend_from_slice(&error.to_be_bytes());
        output.push(u8::from(error == 0));
        let mut by_user: HashMap<String, Vec<_>> = HashMap::new();
        for info in infos {
            by_user
                .entry(info.user().to_owned())
                .or_default()
                .push((info.algorithm(), info.iterations()));
        }
        if all {
            users = by_user.keys().cloned().collect();
            users.sort();
        }
        let mut seen = HashSet::new();
        let mut duplicates = HashSet::new();
        let mut unique_users = Vec::with_capacity(users.len());
        for user in users {
            if seen.insert(user.clone()) {
                unique_users.push(user);
            } else {
                duplicates.insert(user);
            }
        }
        varint(&mut output, unique_users.len() + 1);
        for user in unique_users {
            compact_string(&mut output, &user);
            let duplicate = duplicates.contains(&user);
            let entries = by_user.get(&user);
            let error: i16 = if duplicate {
                92
            } else if entries.is_none() {
                91
            } else {
                0
            };
            output.extend_from_slice(&error.to_be_bytes());
            output.push(u8::from(error == 0));
            varint(
                &mut output,
                if error == 0 {
                    entries.map_or(1, |entries| entries.len() + 1)
                } else {
                    1
                },
            );
            if error == 0 {
                for (algorithm, iterations) in entries.into_iter().flatten() {
                    output.push(algorithm_code(*algorithm));
                    output.extend_from_slice(&(*iterations as i32).to_be_bytes());
                    output.push(0);
                }
            }
            output.push(0);
        }
        output.push(0);
        Ok(output)
    }
    async fn alter(
        &self,
        correlation: i32,
        body: &[u8],
        authorized: bool,
    ) -> Result<Vec<u8>, Error> {
        let limits = self.profile.0.store()?.mechanism_limits();
        let mut reader = Reader::new(body, self.profile.0.limits);
        let mut mutations: Vec<Mutation> = Vec::new();
        let mut count = 0;
        for upsert in [false, true] {
            let entries = reader.count(false)?.ok_or(Error::Malformed)?;
            count += entries;
            if count > self.profile.0.limits.admin_entries {
                return Err(Error::Budget);
            }
            for _ in 0..entries {
                let user = reader.compact_string(limits.identity_bytes)?.to_owned();
                let mechanism = reader.byte()?;
                let algorithm = decode_algorithm(mechanism);
                let (change, invalid) = if upsert {
                    let iterations = reader.i32()?;
                    let salt = reader.bytes(true, limits.salt_bytes)?.to_vec();
                    let salted_password = Secret::new(reader.bytes(true, 64)?.to_vec());
                    let invalid = iterations < 4096
                        || iterations > limits.iterations as i32
                        || salt.len() < 16
                        || algorithm.is_some_and(|a| salted_password.len() != a.size());
                    (
                        algorithm.map(|algorithm| Change::Upsert {
                            algorithm,
                            salt,
                            iterations: iterations as u32,
                            salted_password,
                        }),
                        invalid,
                    )
                } else {
                    (algorithm.map(Change::Delete), false)
                };
                reader.tags()?;
                let index = mutations.iter().position(|m| m.user == user);
                let mutation = if let Some(index) = index {
                    &mut mutations[index]
                } else {
                    mutations.push(Mutation {
                        user: user.clone(),
                        changes: Vec::new(),
                        error: 0,
                    });
                    mutations.last_mut().ok_or(Error::Malformed)?
                };
                if !limits.identity(&user) || invalid {
                    mutation.error = 93;
                }
                if algorithm.is_none() {
                    mutation.error = 33;
                }
                if index.is_some() {
                    mutation.error = 92;
                }
                if let Some(change) = change {
                    mutation.changes.push(change);
                }
            }
        }
        reader.tags()?;
        reader.finish_alter()?;
        let mut output = response_header(correlation, true);
        output.extend_from_slice(&0i32.to_be_bytes());
        varint(&mut output, mutations.len() + 1);
        for mutation in mutations {
            let error = if !authorized {
                31
            } else if mutation.error != 0 {
                mutation.error
            } else {
                match self
                    .profile
                    .0
                    .store()?
                    .mutate(mutation.user.clone(), mutation.changes)
                    .await
                {
                    Ok(_) => 0,
                    Err(error) => storage_code(error),
                }
            };
            compact_string(&mut output, &mutation.user);
            output.extend_from_slice(&error.to_be_bytes());
            output.push(u8::from(error == 0));
            output.push(0);
        }
        output.push(0);
        Ok(output)
    }
}
struct Mutation {
    user: String,
    changes: Vec<Change>,
    error: i16,
}
fn storage_code(error: credentials::Error) -> i16 {
    match error {
        credentials::Error::Invalid => 93,
        credentials::Error::Duplicate => 92,
        credentials::Error::NotFound => 91,
        credentials::Error::Busy => 89,
        credentials::Error::Budget => 89,
        _ => -1,
    }
}
fn algorithm_code(algorithm: Algorithm) -> u8 {
    match algorithm {
        Algorithm::Sha256 => 1,
        Algorithm::Sha512 => 2,
    }
}
fn decode_algorithm(value: u8) -> Option<Algorithm> {
    match value {
        1 => Some(Algorithm::Sha256),
        2 => Some(Algorithm::Sha512),
        _ => None,
    }
}
fn response_header(correlation: i32, flexible: bool) -> Vec<u8> {
    let mut bytes = correlation.to_be_bytes().to_vec();
    if flexible {
        bytes.push(0);
    }
    bytes
}
#[cfg(test)]
fn handshake_response(correlation: i32, error: i16, tls: bool) -> Vec<u8> {
    handshake_response_names(
        correlation,
        error,
        if tls {
            &["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
        } else {
            &["SCRAM-SHA-256", "SCRAM-SHA-512"]
        },
    )
}
fn handshake_response_names(correlation: i32, error: i16, names: &[&str]) -> Vec<u8> {
    let mut bytes = response_header(correlation, false);
    bytes.extend_from_slice(&error.to_be_bytes());
    bytes.extend_from_slice(&(names.len() as i32).to_be_bytes());
    for name in names {
        bytes.extend_from_slice(&(name.len() as i16).to_be_bytes());
        bytes.extend_from_slice(name.as_bytes());
    }
    bytes
}
fn authenticate_response(correlation: i32, version: i16, error: i16, message: &[u8]) -> Vec<u8> {
    authenticate_response_lifetime(correlation, version, error, message, 0)
}
fn authenticate_response_lifetime(
    correlation: i32,
    version: i16,
    error: i16,
    message: &[u8],
    lifetime: i64,
) -> Vec<u8> {
    let flexible = version == 2;
    let mut bytes = response_header(correlation, flexible);
    bytes.extend_from_slice(&error.to_be_bytes());
    if flexible {
        bytes.push(u8::from(error == 0));
        varint(&mut bytes, message.len() + 1);
    } else {
        bytes.extend_from_slice(&(if error == 0 { 0i16 } else { -1i16 }).to_be_bytes());
        bytes.extend_from_slice(&(message.len() as i32).to_be_bytes());
    }
    bytes.extend_from_slice(message);
    if version >= 1 {
        bytes.extend_from_slice(&(if error == 0 { lifetime } else { 0 }).to_be_bytes());
    }
    if flexible {
        bytes.push(0);
    }
    bytes
}
fn varint(output: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 127) as u8;
        value >>= 7;
        output.push(byte | if value > 0 { 128 } else { 0 });
        if value == 0 {
            break;
        }
    }
}
fn compact_string(output: &mut Vec<u8>, value: &str) {
    varint(output, value.len() + 1);
    output.extend_from_slice(value.as_bytes());
}
struct Reader<'a> {
    remaining: &'a [u8],
    limits: Limits,
}
impl<'a> Reader<'a> {
    fn new(remaining: &'a [u8], limits: Limits) -> Self {
        Self { remaining, limits }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let value = self.remaining.get(..length).ok_or(Error::Malformed)?;
        self.remaining = &self.remaining[length..];
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Error::Malformed)?,
        ))
    }
    fn varint(&mut self) -> Result<usize, Error> {
        let mut value = 0u32;
        for shift in [0, 7, 14, 21, 28] {
            let byte = self.byte()?;
            if shift == 28 && byte > 15 {
                return Err(Error::Malformed);
            }
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value as usize);
            }
        }
        Err(Error::Malformed)
    }
    fn classic_string(&mut self, max: usize) -> Result<&'a str, Error> {
        let length = i16::from_be_bytes(self.take(2)?.try_into().map_err(|_| Error::Malformed)?);
        let length = usize::try_from(length).map_err(|_| Error::Malformed)?;
        self.string(length, max)
    }
    fn compact_string(&mut self, max: usize) -> Result<&'a str, Error> {
        let length = self.varint()?.checked_sub(1).ok_or(Error::Malformed)?;
        self.string(length, max)
    }
    fn string(&mut self, length: usize, max: usize) -> Result<&'a str, Error> {
        if length > max {
            return Err(Error::Budget);
        }
        std::str::from_utf8(self.take(length)?).map_err(|_| Error::Malformed)
    }
    fn bytes(&mut self, flexible: bool, max: usize) -> Result<&'a [u8], Error> {
        let length = if flexible {
            self.varint()?.checked_sub(1).ok_or(Error::Malformed)?
        } else {
            usize::try_from(self.i32()?).map_err(|_| Error::Malformed)?
        };
        if length > max {
            return Err(Error::Budget);
        }
        self.take(length)
    }
    fn count(&mut self, nullable: bool) -> Result<Option<usize>, Error> {
        let count = self.varint()?;
        if count == 0 {
            return if nullable {
                Ok(None)
            } else {
                Err(Error::Malformed)
            };
        }
        let count = count - 1;
        if count > self.limits.admin_entries {
            return Err(Error::Budget);
        }
        Ok(Some(count))
    }
    fn tags(&mut self) -> Result<(), Error> {
        let count = self.varint()?;
        if count > self.limits.tagged_fields {
            return Err(Error::Budget);
        }
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varint()?;
            if previous.is_some_and(|p| p >= tag) {
                return Err(Error::Malformed);
            }
            previous = Some(tag);
            let length = self.varint()?;
            self.take(length)?;
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), Error> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(Error::Malformed)
        }
    }
    fn finish_alter(&mut self) -> Result<(), Error> {
        // Genuine librdkafka2.15 Alter51v0 writes canonical body tags and its
        // FLEXVER finalizer appends one more empty tag block. Consume only that
        // exact single-byte dialect after the complete canonical body. All
        // other tails, and every other API, retain strict whole-input parsing.
        if self.remaining == [0] {
            self.remaining = &[];
        }
        self.finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct TestPath(PathBuf);
    impl TestPath {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "partitionline-sasl-codec-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }
    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    fn hex(value: &str) -> Vec<u8> {
        (0..value.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
            .collect()
    }
    fn i16(reader: &mut Reader<'_>) -> i16 {
        i16::from_be_bytes(reader.take(2).unwrap().try_into().unwrap())
    }
    fn nullable_string(reader: &mut Reader<'_>, flex: bool) {
        let length = if flex {
            reader.varint().unwrap().checked_sub(1)
        } else {
            usize::try_from(i16(reader)).ok()
        };
        if let Some(length) = length {
            let value = reader.take(length).unwrap();
            std::str::from_utf8(value).unwrap();
        }
    }
    fn validate_request(key: i16, version: i16, body: &[u8]) -> Result<(), Error> {
        let mut reader = Reader::new(body, Limits::default());
        match key {
            17 => {
                reader.classic_string(32)?;
            }
            36 => {
                reader.bytes(version == 2, 8192)?;
                if version == 2 {
                    reader.tags()?;
                }
            }
            50 => {
                let count = reader.count(true)?;
                for _ in 0..count.unwrap_or(0) {
                    reader.compact_string(256)?;
                    reader.tags()?;
                }
                reader.tags()?;
            }
            51 => {
                for upsert in [false, true] {
                    let count = reader.count(false)?.ok_or(Error::Malformed)?;
                    for _ in 0..count {
                        reader.compact_string(256)?;
                        reader.byte()?;
                        if upsert {
                            reader.i32()?;
                            reader.bytes(true, 64)?;
                            reader.bytes(true, 64)?;
                        }
                        reader.tags()?;
                    }
                }
                reader.tags()?;
            }
            _ => return Err(Error::Malformed),
        }
        if key == 51 {
            reader.finish_alter()
        } else {
            reader.finish()
        }
    }
    #[test]
    fn all_authentic_apache_wire_fields_headers_lengths_and_response_goldens() {
        let rows = include_str!("../../tests/fixtures/sasl-wire/apache-wire.tsv");
        let mut counts = [0usize; 2];
        for line in rows.lines().skip(1) {
            let c: Vec<_> = line.split('\t').collect();
            assert_eq!(c.len(), 10);
            let key: i16 = c[2].parse().unwrap();
            let version: i16 = c[3].parse().unwrap();
            let header_version: i16 = c[4].parse().unwrap();
            let correlation: i32 = c[5].parse().unwrap();
            let body = hex(c[7]);
            let payload = hex(c[8]);
            let frame = hex(c[9]);
            assert_eq!(&frame[4..], payload);
            assert_eq!(
                i32::from_be_bytes(frame[..4].try_into().unwrap()) as usize,
                payload.len()
            );
            if c[0] == "request" {
                counts[0] += 1;
                let (header, rest) = RequestHeader::parse(
                    &payload,
                    header_version,
                    crate::protocol::Limits::default(),
                )
                .unwrap();
                assert_eq!(
                    (header.api_key, header.api_version, header.correlation_id),
                    (key, version, correlation)
                );
                assert_eq!(rest, body);
                validate_request(key, version, &body).unwrap();
                for length in 0..body.len() {
                    assert!(
                        validate_request(key, version, &body[..length]).is_err(),
                        "truncated {} at{}",
                        c[1],
                        length
                    );
                }
                let mut trailing = body.clone();
                trailing.push(0);
                if key == 51 {
                    assert_eq!(validate_request(key, version, &trailing), Ok(()));
                    trailing.push(0);
                }
                assert_eq!(
                    validate_request(key, version, &trailing),
                    Err(Error::Malformed)
                );
            } else {
                counts[1] += 1;
                assert_eq!(&payload[..4], &correlation.to_be_bytes());
                let offset = if header_version == 1 {
                    assert_eq!(payload[4], 0);
                    5
                } else {
                    4
                };
                assert_eq!(&payload[offset..], body);
                let mut reader = Reader::new(&body, Limits::default());
                match key {
                    17 => {
                        let error = i16(&mut reader);
                        let count = reader.i32().unwrap();
                        for _ in 0..count {
                            reader.classic_string(32).unwrap();
                        }
                        if error == 0 {
                            assert_eq!(
                                handshake_response(correlation, 0, c[1] == "handshake-tls-enabled"),
                                payload
                            );
                        }
                    }
                    36 => {
                        let error = i16(&mut reader);
                        nullable_string(&mut reader, version == 2);
                        let message = reader.bytes(version == 2, 8192).unwrap();
                        if version >= 1 {
                            assert_eq!(reader.take(8).unwrap(), &0i64.to_be_bytes());
                        }
                        if version == 2 {
                            reader.tags().unwrap();
                        }
                        if error == 0 {
                            assert_eq!(
                                authenticate_response(correlation, version, 0, message),
                                payload
                            );
                        }
                    }
                    50 => {
                        reader.i32().unwrap();
                        i16(&mut reader);
                        nullable_string(&mut reader, true);
                        let count = reader.count(false).unwrap().unwrap();
                        for _ in 0..count {
                            reader.compact_string(256).unwrap();
                            i16(&mut reader);
                            nullable_string(&mut reader, true);
                            let count = reader.count(false).unwrap().unwrap();
                            for _ in 0..count {
                                reader.byte().unwrap();
                                reader.i32().unwrap();
                                reader.tags().unwrap();
                            }
                            reader.tags().unwrap();
                        }
                        reader.tags().unwrap();
                    }
                    51 => {
                        reader.i32().unwrap();
                        let count = reader.count(false).unwrap().unwrap();
                        for _ in 0..count {
                            reader.compact_string(256).unwrap();
                            i16(&mut reader);
                            nullable_string(&mut reader, true);
                            reader.tags().unwrap();
                        }
                        reader.tags().unwrap();
                    }
                    _ => panic!("unknown independent fixture API"),
                }
                reader.finish().unwrap();
            }
        }
        assert_eq!(counts, [41, 38]);
    }
    fn upsert(algorithm: Algorithm, iterations: u32) -> Change {
        use pbkdf2::pbkdf2_hmac;
        use sha2::{Sha256, Sha512};
        let salt = (0..16).collect::<Vec<u8>>();
        let mut salted = vec![0; algorithm.size()];
        match algorithm {
            Algorithm::Sha256 => pbkdf2_hmac::<Sha256>(b"pencil", &salt, iterations, &mut salted),
            Algorithm::Sha512 => pbkdf2_hmac::<Sha512>(b"pencil", &salt, iterations, &mut salted),
        }
        Change::Upsert {
            algorithm,
            salt,
            iterations,
            salted_password: Secret::new(salted),
        }
    }
    fn renewal_request(key: i16, version: i16, correlation: i32, body: &[u8]) -> Vec<u8> {
        let mut request = Vec::new();
        request.extend_from_slice(&key.to_be_bytes());
        request.extend_from_slice(&version.to_be_bytes());
        request.extend_from_slice(&correlation.to_be_bytes());
        request.extend_from_slice(&(-1i16).to_be_bytes());
        if version == 2 {
            request.push(0);
        }
        request.extend_from_slice(body);
        request
    }
    fn plain_handshake() -> Vec<u8> {
        renewal_request(17, 1, 9, b"\0\x05PLAIN")
    }
    fn plain_proof() -> Vec<u8> {
        let message = b"\0user\0pencil";
        let mut body = (message.len() as i32).to_be_bytes().to_vec();
        body.extend_from_slice(message);
        renewal_request(36, 1, 10, &body)
    }
    async fn complete_plain(session: &mut Session) -> Vec<u8> {
        assert!(matches!(
            session
                .handle(&plain_handshake(), Instant::now())
                .await
                .unwrap(),
            Decision::Reply { close: false, .. }
        ));
        session.response_written().unwrap();
        match session
            .handle(&plain_proof(), Instant::now())
            .await
            .unwrap()
        {
            Decision::Reply {
                bytes,
                close: false,
            } => bytes,
            _ => panic!("valid PLAIN proof must produce its final reply"),
        }
    }
    #[test]
    fn renewal_lifetime_wire_layout_matches_literal_independent_vectors() {
        for (version, expected) in [
            (0, "000000090000000000000002aabb"),
            (1, "000000090000000000000002aabb00000000000004d2"),
            (2, "000000090000000103aabb00000000000004d200"),
        ] {
            assert_eq!(
                authenticate_response_lifetime(9, version, 0, &[0xaa, 0xbb], 1234),
                hex(expected)
            );
        }
        for version in 0..=2 {
            assert_eq!(
                authenticate_response_lifetime(9, version, 58, &[], 1234),
                authenticate_response(9, version, 58, &[])
            );
        }
    }
    #[tokio::test]
    async fn renewal_policy_accepts_only_positive_whole_milliseconds_through_one_day() {
        let path = TestPath::new();
        let (store, _) = Store::open(&path.0, credentials::Limits::default())
            .await
            .unwrap();
        let profile = Profile::tls(store.clone(), vec![], Limits::default()).unwrap();
        for duration in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_micros(1500),
            Duration::from_millis(86_400_001),
        ] {
            assert!(matches!(
                profile.clone().with_reauthentication(duration),
                Err(Error::InvalidProfile)
            ));
        }
        for duration in [Duration::from_millis(1), Duration::from_millis(86_400_000)] {
            assert!(profile.clone().with_reauthentication(duration).is_ok());
        }
        assert!(profile.admit().session_deadline.is_none());
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn renewal_authority_is_pending_until_final_reply_write_and_cannot_outlive_old_expiry() {
        let path = TestPath::new();
        let (store, _) = Store::open(&path.0, credentials::Limits::default())
            .await
            .unwrap();
        store
            .mutate("user".into(), vec![upsert(Algorithm::Sha256, 4096)])
            .await
            .unwrap();
        let profile = Profile::tls(store.clone(), vec![], Limits::default())
            .unwrap()
            .with_advertised(&SASL_METADATA_API_VERSIONS)
            .unwrap()
            .with_reauthentication(Duration::from_secs(2))
            .unwrap();
        let mut session = profile.admit();
        complete_plain(&mut session).await;
        session.response_written().unwrap();
        let previous_generation = session.identity().unwrap().generation();
        store
            .mutate("user".into(), vec![upsert(Algorithm::Sha256, 4096)])
            .await
            .unwrap();
        let previous_expiry = Instant::now() + Duration::from_secs(1);
        session.session_deadline = Some(previous_expiry);
        complete_plain(&mut session).await;
        assert!(session.identity().is_none());
        assert_eq!(session.operation_deadline(), Some(previous_expiry));
        assert!(session.session_deadline.unwrap() > previous_expiry);
        session.response_written().unwrap();
        assert!(session.renewal.is_none());
        assert!(session.identity().unwrap().generation() > previous_generation);
        // A proof completed before expiry is still not usable if its final reply
        // write reaches the old boundary. No application identity is exposed.
        complete_plain(&mut session).await;
        session.renewal.as_mut().unwrap().deadline = Instant::now();
        assert_eq!(session.response_written(), Err(Error::Budget));
        assert!(session.identity().is_none());
        assert!(matches!(session.state, State::Failed));
        let mut session = profile.admit();
        complete_plain(&mut session).await;
        session.response_written().unwrap();
        complete_plain(&mut session).await;
        let app = renewal_request(3, 0, 11, &(-1i32).to_be_bytes());
        assert!(matches!(
            session.handle(&app, Instant::now()).await,
            Err(Error::Malformed)
        ));
        assert!(session.identity().is_none());
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn renewal_budgets_reset_once_include_handshake_and_keep_absolute_deadline() {
        let path = TestPath::new();
        let (store, _) = Store::open(&path.0, credentials::Limits::default())
            .await
            .unwrap();
        store
            .mutate("user".into(), vec![upsert(Algorithm::Sha256, 4096)])
            .await
            .unwrap();
        let limits = Limits {
            preauth_bytes: 1024,
            frame_bytes: 256,
            control_rounds: 2,
            ..Limits::default()
        };
        let profile = Profile::tls(store.clone(), vec![], limits)
            .unwrap()
            .with_reauthentication(Duration::from_secs(2))
            .unwrap();
        let mut session = profile.admit();
        complete_plain(&mut session).await;
        session.response_written().unwrap();
        session
            .begin_renewal(&plain_handshake(), Instant::now())
            .unwrap();
        let deadline = session.deadline;
        assert_eq!(session.rounds, 1);
        assert_eq!(session.bytes, plain_handshake().len() + 4);
        session.charge(b"second-token").unwrap();
        assert_eq!(session.deadline, deadline);
        assert_eq!(session.charge(b"third-token"), Err(Error::Budget));
        assert!(matches!(session.state, State::Failed));
        let mut session = profile.admit();
        complete_plain(&mut session).await;
        session.response_written().unwrap();
        session
            .begin_renewal(&plain_handshake(), Instant::now())
            .unwrap();
        let remaining = 1024 - session.bytes;
        assert_eq!(session.charge(&vec![0; remaining - 3]), Err(Error::Budget));
        assert!(session.bytes > 1024);
        assert!(matches!(session.state, State::Failed));
        let mut session = profile.admit();
        complete_plain(&mut session).await;
        session.response_written().unwrap();
        let received = Instant::now() - Duration::from_secs(11);
        let request = plain_handshake();
        assert_eq!(
            session.request_deadline(&request, received),
            Some(received + limits.preauth_timeout)
        );
        assert!(matches!(
            session.handle(&request, received).await.unwrap(),
            Decision::Reply { close: true, .. }
        ));
        assert!(matches!(session.state, State::Failed));
        assert!(session.identity().is_none());
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn admin_runtime_encoders_match_apache_success_and_all_truncated_mutations_are_uncommitted(
    ) {
        let path = TestPath::new();
        let (store, _) = Store::open(&path.0, credentials::Limits::default())
            .await
            .unwrap();
        store
            .mutate(
                "user".into(),
                vec![
                    upsert(Algorithm::Sha256, 4096),
                    upsert(Algorithm::Sha512, 8192),
                ],
            )
            .await
            .unwrap();
        let profile = Profile::tls(store.clone(), vec!["admin".into()], Limits::default()).unwrap();
        let session = profile.admit();
        let rows: Vec<Vec<_>> = include_str!("../../tests/fixtures/sasl-wire/apache-wire.tsv")
            .lines()
            .skip(1)
            .map(|line| line.split('\t').collect())
            .collect();
        let metadata = rows
            .iter()
            .find(|c| c[1] == "describe-metadata-only")
            .unwrap();
        let correlation = metadata[5].parse().unwrap();
        // One user in the same independent metadata-only golden, with actual
        // actor verifiers; no keys or salt can occur in this response schema.
        let mut describe = Vec::new();
        varint(&mut describe, 2);
        compact_string(&mut describe, "user");
        describe.extend_from_slice(&[0, 0]);
        assert_eq!(
            session
                .describe(correlation, &describe, true)
                .await
                .unwrap(),
            hex(metadata[8])
        );
        for request in rows.iter().filter(|c| c[0] == "request" && c[2] == "51") {
            let body = hex(request[7]);
            let correlation = request[5].parse().unwrap();
            for length in 0..body.len() {
                assert!(session
                    .alter(correlation, &body[..length], true)
                    .await
                    .is_err());
            }
            assert_eq!(store.describe(None).await.unwrap().len(), 2);
        }
        let request = rows
            .iter()
            .find(|c| c[0] == "request" && c[1] == "alter-upsert256")
            .unwrap();
        let response = rows.iter().find(|c| c[1] == "alter-success").unwrap();
        assert_eq!(
            session
                .alter(response[5].parse().unwrap(), &hex(request[7]), true)
                .await
                .unwrap(),
            hex(response[8])
        );
        let duplicate = rows
            .iter()
            .find(|c| c[0] == "request" && c[1] == "describe-duplicate")
            .unwrap();
        let response = session
            .describe(duplicate[5].parse().unwrap(), &hex(duplicate[7]), true)
            .await
            .unwrap();
        let mut reader = Reader::new(&response[5..], Limits::default());
        reader.i32().unwrap();
        assert_eq!(i16(&mut reader), 0);
        nullable_string(&mut reader, true);
        assert_eq!(reader.count(false).unwrap(), Some(1));
        assert_eq!(reader.compact_string(256).unwrap(), "user");
        assert_eq!(i16(&mut reader), 92);
        let both = rows
            .iter()
            .find(|c| c[0] == "request" && c[1] == "alter-both")
            .unwrap();
        let response = session
            .alter(both[5].parse().unwrap(), &hex(both[7]), true)
            .await
            .unwrap();
        let mut reader = Reader::new(&response[5..], Limits::default());
        reader.i32().unwrap();
        assert_eq!(reader.count(false).unwrap(), Some(1));
        assert_eq!(reader.compact_string(256).unwrap(), "user");
        assert_eq!(i16(&mut reader), 92);
        store.shutdown().await.unwrap();
        let (store, recovery) = Store::open(&path.0, credentials::Limits::default())
            .await
            .unwrap();
        assert_eq!(recovery.recovered_entries, 2);
        store.shutdown().await.unwrap();
    }
    #[test]
    fn malformed_varint_tag_order_count_and_null_sentinels_fail_boundedly() {
        assert_eq!(
            Reader::new(&[0xff; 5], Limits::default()).varint(),
            Err(Error::Malformed)
        );
        assert_eq!(
            Reader::new(&[2, 1, 0, 1, 0], Limits::default()).tags(),
            Err(Error::Malformed)
        );
        assert_eq!(
            Reader::new(&[0], Limits::default()).count(false),
            Err(Error::Malformed)
        );
        assert_eq!(
            Reader::new(&[0], Limits::default()).compact_string(256),
            Err(Error::Malformed)
        );
        assert_eq!(
            Reader::new(&[130, 1], Limits::default()).count(false),
            Err(Error::Budget)
        );
    }
}
