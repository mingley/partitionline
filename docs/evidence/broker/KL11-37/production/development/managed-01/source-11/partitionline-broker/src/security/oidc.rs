//! Bounded signed access-token validation and OIDC authority management.
//!
//! The finite pinned-JWKS verifier is a signed-JWT foundation. It does not
//! discover an issuer, refresh keys or establish provider revocation. Managed
//! HTTPS authorities and authenticated socket leases are separate layers.
//! Owned bearer inputs are zeroizing; diagnostics never contain token material.

mod cache;
mod http;
mod introspection;
mod jwt;
pub use cache::{Config, KeySource, Lease, RuntimeLimits, Service};
pub use http::{HttpLimits, HttpsTrust};
pub use introspection::Introspection;

use super::sasl::Secret;
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{Mutex, Notify, Semaphore},
    time::Instant,
};

/// Explicitly supported public-key JWS signature algorithms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Algorithm {
    /// RSA PKCS#1 v1.5 SHA-256, with a 2048–4096-bit modulus.
    Rs256,
    /// P-256 ECDSA SHA-256, with the fixed-width JOSE R || S signature.
    Es256,
}
impl Algorithm {
    fn name(self) -> &'static str {
        match self {
            Self::Rs256 => "RS256",
            Self::Es256 => "ES256",
        }
    }
}

/// Signed discriminator that distinguishes an access token from an ID token.
#[derive(Clone, Debug)]
pub enum AccessToken {
    /// Require protected JOSE typ to equal at+jwt.
    AtJwt,
    /// Require an exact provider-specific signed string claim, for example
    /// token_use=access. JOSE typ, when present, must be JWT or at+jwt.
    Claim {
        /// Exact signed claim name; registered identity/time claims are disallowed.
        name: String,
        /// Exact expected signed string value identifying an access token.
        value: String,
    },
}

/// Validated input, work and lifetime ceilings. Inputs never choose these bounds.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum compact JWT bytes, 512 through 32 KiB.
    pub token_bytes: usize,
    /// Maximum decoded protected header bytes, 128 through 8 KiB.
    pub header_bytes: usize,
    /// Maximum one JSON document, 1 through 256 KiB.
    pub document_bytes: usize,
    /// Maximum JSON containers nested within a document, 2 through 32.
    pub json_depth: usize,
    /// Maximum public keys in one JWKS, 1 through 64.
    pub keys: usize,
    /// Maximum UTF-8 bytes in one key identifier, 1 through 256.
    pub kid_bytes: usize,
    /// Maximum UTF-8 bytes in a subject or token identifier, 1 through 1024.
    pub identity_bytes: usize,
    /// Maximum accepted/configured audience entries, 1 through 32.
    pub audiences: usize,
    /// Maximum signed exp - iat lifetime, positive through 24 hours.
    pub token_lifetime: Duration,
    /// Permitted future iat/nbf clock skew, zero through 60 seconds.
    /// Expiration is never extended by skew.
    pub clock_skew: Duration,
    /// Maximum finite authority freshness, positive through 15 minutes.
    pub authority_freshness: Duration,
    /// Maximum admitted verification operations, 1 through 256.
    pub admissions: usize,
    /// Maximum actual blocking cryptographic workers, 1 through 16.
    pub workers: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            token_bytes: 8192,
            header_bytes: 2048,
            document_bytes: 65536,
            json_depth: 16,
            keys: 16,
            kid_bytes: 128,
            identity_bytes: 256,
            audiences: 8,
            token_lifetime: Duration::from_secs(3600),
            clock_skew: Duration::from_secs(5),
            authority_freshness: Duration::from_secs(300),
            admissions: 32,
            workers: 2,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if !(512..=32768).contains(&self.token_bytes)
            || !(128..=8192).contains(&self.header_bytes)
            || !(1024..=262144).contains(&self.document_bytes)
            || !(2..=32).contains(&self.json_depth)
            || !(1..=64).contains(&self.keys)
            || !(1..=256).contains(&self.kid_bytes)
            || !(1..=1024).contains(&self.identity_bytes)
            || !(1..=32).contains(&self.audiences)
            || self.token_lifetime.is_zero()
            || self.token_lifetime > Duration::from_secs(86400)
            || self.clock_skew > Duration::from_secs(60)
            || self.authority_freshness.is_zero()
            || self.authority_freshness > Duration::from_secs(900)
            || !(1..=256).contains(&self.admissions)
            || !(1..=16).contains(&self.workers)
            || self.workers > self.admissions
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Explicit trusted policy; token claims cannot supply issuer, keys or audiences.
#[derive(Clone)]
pub struct Policy {
    /// Exact configured HTTPS issuer, at most 2048 bytes.
    pub issuer: String,
    /// Expected audiences; at least one must be present in the signed claim.
    pub audiences: Vec<String>,
    /// Explicit allowlist, with at most the two supported algorithms.
    pub algorithms: Vec<Algorithm>,
    /// Required signed access-token discriminator.
    pub access_token: AccessToken,
    /// Input, cryptographic work and lifetime bounds.
    pub limits: Limits,
}
impl fmt::Debug for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Policy { [REDACTED] }")
    }
}
impl Policy {
    fn validate(&self) -> Result<(), Error> {
        self.limits.validate()?;
        let uri: ::http::Uri = self
            .issuer
            .parse()
            .map_err(|_| Error::InvalidConfiguration)?;
        let authority = uri.authority().ok_or(Error::InvalidConfiguration)?;
        let host = uri.host().ok_or(Error::InvalidConfiguration)?;
        let suffix = authority
            .as_str()
            .get(host.len()..)
            .ok_or(Error::InvalidConfiguration)?;
        if self.issuer.len() > 2048
            || uri.scheme_str() != Some("https")
            || authority.as_str().contains('@')
            || host.is_empty()
            || host.len() > 256
            || (!suffix.is_empty() && (!suffix.starts_with(':') || uri.port_u16().is_none()))
            || uri.port_u16() == Some(0)
            || uri.query().is_some()
            || self.issuer.contains('#')
            || self.audiences.is_empty()
            || self.audiences.len() > self.limits.audiences
            || self
                .audiences
                .iter()
                .any(|a| !text(a, 2048) || self.audiences.iter().filter(|b| *b == a).count() != 1)
            || self.algorithms.is_empty()
            || self.algorithms.len() > 2
            || self
                .algorithms
                .iter()
                .any(|a| self.algorithms.iter().filter(|b| *b == a).count() != 1)
        {
            return Err(Error::InvalidConfiguration);
        }
        if let AccessToken::Claim { name, value } = &self.access_token {
            if !text(name, 128)
                || !text(value, 256)
                || matches!(
                    name.as_str(),
                    "iss" | "sub" | "aud" | "iat" | "exp" | "nbf" | "jti"
                )
            {
                return Err(Error::InvalidConfiguration);
            }
        }
        Ok(())
    }
}
fn text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

/// Redacted typed failure; no source URL, principal, token, key or secret appears.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// Trusted configuration or authority material violates its bounds/policy.
    InvalidConfiguration,
    /// Input is malformed, duplicated, out of bounds or unsupported.
    Malformed,
    /// Signature or a required signed claim failed validation.
    Authentication,
    /// Hard token or authority lease expired; expiration has no skew grace.
    Expired,
    /// Explicit revocation authority reports inactive or revoked.
    Revoked,
    /// Bounded admission/worker/authority capacity is occupied.
    Busy,
    /// Stopped, unavailable or failed authority; never extends a prior lease.
    Unavailable,
    /// A caller or service cancelled admitted work.
    Cancelled,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OIDC: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Identity and finite deadline produced only after cryptographic validation.
/// This foundation result does not assert provider revocation or managed OIDC.
#[derive(Clone)]
pub struct Verified {
    issuer: String,
    subject: String,
    audiences: Vec<String>,
    token_id: Option<String>,
    key_id: String,
    expires_at: SystemTime,
    token_deadline: Instant,
    deadline: Instant,
}
impl fmt::Debug for Verified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Verified { [REDACTED] }")
    }
}
impl Verified {
    /// Exact authenticated issuer; authorization must include this authority.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }
    /// Signed subject scoped to the authenticated issuer.
    pub fn subject(&self) -> &str {
        &self.subject
    }
    /// Signed token identifier, when present.
    pub fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }
    /// Public signing-key identifier used for this proof.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }
    /// Signed wall-clock expiration, with no grace extension.
    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }
    /// Monotonic minimum of token expiration and finite pinned-key freshness.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

struct Work {
    registration: Mutex<()>,
    stopping: AtomicBool,
    outstanding: AtomicUsize,
    changed: Notify,
    admissions: Arc<Semaphore>,
    workers: Arc<Semaphore>,
}
impl Work {
    fn new(limits: Limits) -> Self {
        Self {
            registration: Mutex::new(()),
            stopping: AtomicBool::new(false),
            outstanding: AtomicUsize::new(0),
            changed: Notify::new(),
            admissions: Arc::new(Semaphore::new(limits.admissions)),
            workers: Arc::new(Semaphore::new(limits.workers)),
        }
    }
    async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        job: impl FnOnce() -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Unavailable);
        }
        let admission = self
            .admissions
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let worker = self
            .workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        // Serialize registration with shutdown. Holding an admission alone does
        // not prevent shutdown from observing zero before a closure is counted.
        {
            let _registration = self.registration.lock().await;
            if self.stopping.load(Ordering::Acquire) {
                return Err(Error::Unavailable);
            }
            self.outstanding.fetch_add(1, Ordering::AcqRel);
        }
        let guard = WorkGuard(self.clone());
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        let stopping = self.clone();
        tokio::task::spawn_blocking(move || {
            // Drop the accounting guard last, after both actual work permits.
            // Shutdown observing zero must not race permit/secret cleanup.
            let _guard = guard;
            let _admission = admission;
            let _worker = worker;
            if cancelled.load(Ordering::Acquire) || stopping.stopping.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            let result = job();
            if cancelled.load(Ordering::Acquire) || stopping.stopping.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            result
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }
    async fn shutdown(self: &Arc<Self>) -> Result<(), Error> {
        {
            let _registration = self.registration.lock().await;
            self.stopping.store(true, Ordering::Release);
            self.admissions.close();
            self.workers.close();
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.outstanding.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            changed.await;
        }
    }
}
struct WorkGuard(Arc<Work>);
impl Drop for WorkGuard {
    fn drop(&mut self) {
        self.0.outstanding.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Finite immutable pinned-JWKS signed-JWT verifier, with bounded blocking work.
/// It is not the managed HTTPS discovery/key/revocation profile.
#[derive(Clone)]
pub struct PinnedVerifier {
    policy: Arc<Policy>,
    keys: Arc<jwt::KeySet>,
    freshness: Instant,
    work: Arc<Work>,
}
impl fmt::Debug for PinnedVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PinnedVerifier { [REDACTED] }")
    }
}
impl PinnedVerifier {
    /// Parse bounded public JWKS and capture a finite monotonic freshness lease.
    /// The trusted caller supplies its public keys; tokens cannot select a URL.
    pub fn new(policy: Policy, jwks: &[u8], valid_for: Duration) -> Result<Self, Error> {
        policy.validate()?;
        if valid_for.is_zero() || valid_for > policy.limits.authority_freshness {
            return Err(Error::InvalidConfiguration);
        }
        let keys = jwt::KeySet::parse(jwks, &policy)?;
        let freshness = Instant::now()
            .checked_add(valid_for)
            .ok_or(Error::InvalidConfiguration)?;
        let work = Arc::new(Work {
            registration: Mutex::new(()),
            stopping: AtomicBool::new(false),
            outstanding: AtomicUsize::new(0),
            changed: Notify::new(),
            admissions: Arc::new(Semaphore::new(policy.limits.admissions)),
            workers: Arc::new(Semaphore::new(policy.limits.workers)),
        });
        Ok(Self {
            policy: Arc::new(policy),
            keys: Arc::new(keys),
            freshness,
            work,
        })
    }
    /// Verify an owned zeroizing bearer token off Tokio's async worker threads.
    /// Cancellation retains both permits until the actual blocking work exits.
    pub async fn verify(&self, token: Secret) -> Result<Verified, Error> {
        if token.len() > self.policy.limits.token_bytes {
            return Err(Error::Malformed);
        }
        let policy = self.policy.clone();
        let keys = self.keys.clone();
        let freshness = self.freshness;
        self.run(move || {
            jwt::verify(
                &policy,
                &keys,
                token.as_bytes(),
                SystemTime::now(),
                freshness,
            )
        })
        .await
    }
    async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        self.work.run(job).await
    }
    /// Close admissions and join all actual cryptographic work, including work
    /// whose caller future was cancelled. Repeating/cancelling shutdown is safe.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.work.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn verifier() -> PinnedVerifier {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        // Public structural P-256 material suffices for work-lifecycle tests;
        // no claim of an authentic signature is made by this helper.
        let jwks = json!({"keys":[{"kid":"capacity-test","kty":"EC","crv":"P-256","x":URL_SAFE_NO_PAD.encode([1u8;32]),"y":URL_SAFE_NO_PAD.encode([2u8;32])}]});
        PinnedVerifier::new(
            Policy {
                issuer: "https://issuer.example".to_owned(),
                audiences: vec!["partitionline".to_owned()],
                algorithms: vec![Algorithm::Es256],
                access_token: AccessToken::AtJwt,
                limits: Limits {
                    workers: 1,
                    admissions: 1,
                    ..Limits::default()
                },
            },
            &serde_json::to_vec(&jwks).unwrap(),
            Duration::from_secs(60),
        )
        .unwrap()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_retains_actual_worker_and_shutdown_joins_it() {
        let verifier = verifier();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let other = verifier.clone();
        let caller = tokio::spawn(async move {
            other
                .run(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(verifier.work.outstanding.load(Ordering::Acquire), 1);
        assert_eq!(verifier.work.admissions.available_permits(), 0);
        assert_eq!(verifier.work.workers.available_permits(), 0);
        assert_eq!(verifier.run(|| Ok(())).await.unwrap_err(), Error::Busy);
        let other = verifier.clone();
        let shutdown = tokio::spawn(async move { other.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        release_tx.send(()).unwrap();
        shutdown.await.unwrap().unwrap();
        assert_eq!(verifier.work.outstanding.load(Ordering::Acquire), 0);
        assert!(verifier.work.workers.is_closed());
        assert_eq!(
            verifier.run(|| Ok(())).await.unwrap_err(),
            Error::Unavailable
        );
        verifier.shutdown().await.unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_shutdown_leaves_actual_work_joinable() {
        let verifier = verifier();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let other = verifier.clone();
        let caller = tokio::spawn(async move {
            other
                .run(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        let other = verifier.clone();
        let shutdown = tokio::spawn(async move { other.shutdown().await });
        while !verifier.work.stopping.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        shutdown.abort();
        assert!(shutdown.await.unwrap_err().is_cancelled());
        release_tx.send(()).unwrap();
        assert_eq!(caller.await.unwrap().unwrap_err(), Error::Cancelled);
        verifier.shutdown().await.unwrap();
        assert_eq!(verifier.work.outstanding.load(Ordering::Acquire), 0);
    }
    #[test]
    fn configuration_defaults_and_authorities_are_bounded() {
        let verifier = verifier();
        let mut policy = (*verifier.policy).clone();
        policy.issuer = "http://issuer.example".to_owned();
        assert_eq!(policy.validate().unwrap_err(), Error::InvalidConfiguration);
        policy.issuer = "https://user@issuer.example".to_owned();
        assert!(policy.validate().is_err());
        policy.issuer = "https://issuer.example?keys=attacker".to_owned();
        assert!(policy.validate().is_err());
        policy.issuer = "https://issuer.example".to_owned();
        policy.algorithms.push(Algorithm::Es256);
        assert!(policy.validate().is_err());
        policy.algorithms.pop();
        policy.access_token = AccessToken::Claim {
            name: "sub".to_owned(),
            value: "admin".to_owned(),
        };
        assert!(policy.validate().is_err());
        let limits = Limits {
            workers: 3,
            admissions: 2,
            ..Limits::default()
        };
        assert!(limits.validate().is_err());
        assert!(format!("{verifier:?}").contains("REDACTED"));
        assert!(!format!("{verifier:?}").contains("issuer.example"));
    }
}
