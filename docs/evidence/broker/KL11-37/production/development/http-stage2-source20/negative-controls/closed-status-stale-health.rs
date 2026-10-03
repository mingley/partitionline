use super::{
    http::{Client, Endpoint, HttpsTrust},
    introspection::{Authority, Introspection},
    jwt, Error, PinnedVerifier, Policy, Verified, Work,
};
use crate::security::sasl::Secret;
use serde_json::Value;
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{mpsc, oneshot, watch, Mutex, OwnedSemaphorePermit, RwLock, Semaphore},
    task::{JoinHandle, JoinSet},
    time::{Instant, MissedTickBehavior},
};

/// Trusted signing-authority location; no URL can come from an unverified token.
pub enum KeySource {
    /// Actual issuer discovery, with an explicit JWKS HTTPS origin allowlist.
    Discovery {
        /// One through eight exact HTTPS origins permitted by configuration.
        allowed_jwks_origins: Vec<String>,
    },
    /// Explicit configured HTTPS JWKS, without discovery. Signature/issuer/time
    /// and online revocation still apply; this profile does not assert discovery.
    Jwks {
        /// Exact configured HTTPS JWKS URL.
        uri: String,
    },
}
impl fmt::Debug for KeySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KeySource { [REDACTED] }")
    }
}
/// Manager, refresh and active-token ceilings shared across listener sockets.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeLimits {
    /// Maximum active authenticated leases, 1 through 1024.
    pub active_leases: usize,
    /// Maximum concurrent validations/queued authority requests, 1 through 256.
    pub validations: usize,
    /// Proactive signing-authority refresh interval, at least 50 ms and at most
    /// half key freshness.
    pub refresh_interval: Duration,
    /// Global unknown-kid refresh cooldown, 1 through 60 seconds.
    /// No per-attacker-kid unbounded cache is retained.
    pub unknown_kid_cooldown: Duration,
    /// Maximum one positive provider revocation lease, 100 ms through 5 seconds.
    pub revocation_lease: Duration,
    /// Proactive revocation refresh interval, at least 50 ms and at most half
    /// the positive lease.
    pub revocation_refresh: Duration,
}
impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            active_leases: 256,
            validations: 32,
            refresh_interval: Duration::from_secs(30),
            unknown_kid_cooldown: Duration::from_secs(5),
            revocation_lease: Duration::from_secs(5),
            revocation_refresh: Duration::from_secs(1),
        }
    }
}
impl RuntimeLimits {
    fn validate(self, policy: &Policy) -> Result<(), Error> {
        if !(1..=1024).contains(&self.active_leases)
            || !(1..=256).contains(&self.validations)
            || self.refresh_interval < Duration::from_millis(50)
            || self.refresh_interval > policy.limits.authority_freshness / 2
            || self.unknown_kid_cooldown < Duration::from_secs(1)
            || self.unknown_kid_cooldown > Duration::from_secs(60)
            || self.revocation_lease < Duration::from_millis(100)
            || self.revocation_lease > Duration::from_secs(5)
            || self.revocation_refresh < Duration::from_millis(50)
            || self.revocation_refresh > self.revocation_lease / 2
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(())
    }
}
/// Explicit real HTTPS signing and revocation authorities.
/// Managed validation requires online RFC7662 authority; a static JWKS alone
/// remains the separate PinnedVerifier signed-JWT foundation.
pub struct Config {
    /// Trusted signed access-token policy.
    pub policy: Policy,
    /// HTTPS discovery or explicit JWKS source.
    pub keys: KeySource,
    /// Explicit authenticated HTTPS revocation authority.
    pub introspection: Introspection,
    /// Bounded owned refresh/validation/session state.
    pub runtime: RuntimeLimits,
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Config { [REDACTED] }")
    }
}
enum Source {
    Discovery {
        endpoint: Endpoint,
        origins: Vec<String>,
    },
    Jwks(Endpoint),
}
impl Source {
    fn new(source: KeySource, policy: &Policy) -> Result<Self, Error> {
        match source {
            KeySource::Jwks { uri } => Ok(Self::Jwks(Endpoint::parse(&uri)?)),
            KeySource::Discovery {
                allowed_jwks_origins,
            } => {
                if !(1..=8).contains(&allowed_jwks_origins.len()) {
                    return Err(Error::InvalidConfiguration);
                }
                let mut origins = Vec::new();
                for origin in allowed_jwks_origins {
                    let endpoint = Endpoint::parse(&origin)?;
                    if !matches!(endpoint.uri().path(), "" | "/")
                        || endpoint.uri().query().is_some()
                        || origins.iter().any(|o| o == endpoint.origin())
                    {
                        return Err(Error::InvalidConfiguration);
                    }
                    origins.push(endpoint.origin().to_owned());
                }
                let url = format!(
                    "{}/.well-known/openid-configuration",
                    policy.issuer.trim_end_matches('/')
                );
                Ok(Self::Discovery {
                    endpoint: Endpoint::parse(&url)?,
                    origins,
                })
            }
        }
    }
    async fn fetch(
        &self,
        client: &Client,
        policy: &Policy,
        generation: u64,
        previous: Option<&Publication>,
    ) -> Result<Arc<Publication>, Error> {
        let started = Instant::now();
        let endpoint = match self {
            Self::Jwks(endpoint) => endpoint.clone(),
            Self::Discovery { endpoint, origins } => {
                let bytes = client.get(endpoint).await?;
                let value = jwt::parse_json(
                    &bytes,
                    policy.limits.document_bytes,
                    policy.limits.json_depth,
                )?;
                let object = value.as_object().ok_or(Error::Malformed)?;
                if object.get("issuer").and_then(Value::as_str) != Some(policy.issuer.as_str()) {
                    return Err(Error::Authentication);
                }
                let uri = object
                    .get("jwks_uri")
                    .and_then(Value::as_str)
                    .ok_or(Error::Malformed)?;
                let jwks = Endpoint::parse(uri)?;
                if !origins.iter().any(|o| o == jwks.origin()) {
                    return Err(Error::Authentication);
                }
                jwks
            }
        };
        let bytes = client.get(&endpoint).await?;
        let keys = Arc::new(jwt::KeySet::parse_authority(&bytes, policy)?);
        let fresh_until = started
            .checked_add(policy.limits.authority_freshness)
            .ok_or(Error::InvalidConfiguration)?;
        if Instant::now() >= fresh_until {
            return Err(Error::Expired);
        }
        Ok(Arc::new(Publication::new(
            keys,
            generation,
            fresh_until,
            previous,
        )))
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
struct KeyIdentity {
    fingerprint: [u8; 32],
    epoch: u64,
}
struct Publication {
    keys: Arc<jwt::KeySet>,
    generation: u64,
    fresh_until: Instant,
    identities: HashMap<String, KeyIdentity>,
}
impl Publication {
    fn new(
        keys: Arc<jwt::KeySet>,
        generation: u64,
        fresh_until: Instant,
        previous: Option<&Self>,
    ) -> Self {
        let identities = keys
            .fingerprints()
            .map(|(kid, fingerprint)| {
                let epoch = previous
                    .and_then(|p| p.identity(kid))
                    .filter(|old| old.fingerprint == fingerprint)
                    .map_or(generation, |old| old.epoch);
                (kid.to_owned(), KeyIdentity { fingerprint, epoch })
            })
            .collect();
        Self {
            keys,
            generation,
            fresh_until,
            identities,
        }
    }
    fn identity(&self, kid: &str) -> Option<KeyIdentity> {
        self.identities.get(kid).copied()
    }
}
type LeaseFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
enum Command {
    Unknown {
        kid: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Lease(LeaseFuture),
}
struct State {
    policy: Arc<Policy>,
    runtime: RuntimeLimits,
    source: Source,
    http: Client,
    revocation: Authority,
    publication: RwLock<Arc<Publication>>,
    published: watch::Sender<Arc<Publication>>,
    stop: watch::Sender<bool>,
    validations: Arc<Semaphore>,
    leases: Arc<Semaphore>,
    crypto: Arc<Work>,
    tx: mpsc::Sender<Command>,
    join: Mutex<Option<JoinHandle<()>>>,
}
struct Handle(Arc<State>);
impl Drop for Handle {
    fn drop(&mut self) {
        self.0.stop.send_replace(true);
    }
}
/// Owned bounded HTTPS authority manager and online token-lease validator.
/// Discovery mode executes real HTTPS discovery and JWKS requests; direct-JWKS
/// mode is explicitly configured. Both require online revocation introspection.
#[derive(Clone)]
pub struct Service(Arc<Handle>);
impl fmt::Debug for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Service { [REDACTED] }")
    }
}
impl Service {
    /// Validate configuration and initial HTTPS authority before publishing any
    /// handle. Initial discovery/JWKS outage fails closed; no empty cache fallback.
    pub async fn start(config: Config, trust: HttpsTrust) -> Result<Self, Error> {
        config.policy.validate()?;
        config.runtime.validate(&config.policy)?;
        let source = Source::new(config.keys, &config.policy)?;
        let revocation = Authority::new(config.introspection)?;
        let http = Client::new(trust)?;
        let publication = match source.fetch(&http, &config.policy, 1, None).await {
            Ok(publication) => publication,
            Err(error) => {
                // A failed startup still joins actual admitted resolver work.
                // Kernel name resolution is not forcibly cancellable.
                http.shutdown().await?;
                return Err(error);
            }
        };
        let policy = Arc::new(config.policy);
        let work = Arc::new(Work::new(policy.limits));
        let (published, _) = watch::channel(publication.clone());
        let (stop, _) = watch::channel(false);
        let (tx, rx) = mpsc::channel(config.runtime.validations);
        let state = Arc::new(State {
            policy,
            runtime: config.runtime,
            source,
            http,
            revocation,
            publication: RwLock::new(publication),
            published,
            stop,
            validations: Arc::new(Semaphore::new(config.runtime.validations)),
            leases: Arc::new(Semaphore::new(config.runtime.active_leases)),
            crypto: work,
            tx,
            join: Mutex::new(None),
        });
        let task = tokio::spawn(manager(state.clone(), rx));
        *state.join.lock().await = Some(task);
        Ok(Self(Arc::new(Handle(state))))
    }
    /// Validate a zeroizing token and obtain an online-revocation socket lease.
    /// No token-selected URL or per-token global cache is created.
    pub async fn validate(&self, token: Secret) -> Result<Lease, Error> {
        let state = &self.0 .0;
        if *state.stop.borrow() {
            return Err(Error::Unavailable);
        }
        let _validation = state
            .validations
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let lease_permit = state
            .leases
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let kid = jwt::peek_kid(token.as_bytes(), &state.policy)?;
        let mut publication = state.publication.read().await.clone();
        if Instant::now() >= publication.fresh_until {
            return Err(Error::Expired);
        }
        if !publication.keys.contains(&kid) {
            let (reply, received) = oneshot::channel();
            state
                .tx
                .try_send(Command::Unknown { kid, reply })
                .map_err(|_| Error::Busy)?;
            let mut stop = state.stop.subscribe();
            tokio::select! { biased; _=stop.changed()=>return Err(Error::Unavailable), r=received=>r.map_err(|_| Error::Unavailable)?? }
            publication = state.publication.read().await.clone();
        }
        let verifier = PinnedVerifier {
            policy: state.policy.clone(),
            keys: publication.keys.clone(),
            freshness: publication.fresh_until,
            work: state.crypto.clone(),
        };
        let verified = verifier
            .verify(Secret::new(token.as_bytes().to_vec()))
            .await?;
        let identity = publication
            .identity(verified.key_id())
            .ok_or(Error::Authentication)?;
        let mut stop = state.stop.subscribe();
        if *stop.borrow() {
            return Err(Error::Unavailable);
        }
        let revocation = tokio::select! { biased; _=stop.changed()=>return Err(Error::Unavailable), r=state.revocation.check(&state.http,&state.policy,token.as_bytes(),&verified,state.runtime.revocation_lease)=>r? };
        // Authority may rotate while proof/introspection is in flight. A newly
        // authenticated socket must not publish a removed/replaced old key.
        let current = state.publication.read().await.clone();
        if current.identity(verified.key_id()) != Some(identity) {
            return Err(Error::Revoked);
        }
        let until = verified
            .token_deadline
            .min(current.fresh_until)
            .min(revocation);
        if Instant::now() >= until || *state.stop.borrow() {
            return Err(Error::Expired);
        }
        let (updates, receiver) = watch::channel(Status {
            deadline: until,
            failure: None,
        });
        let (cancel, cancelled) = watch::channel(false);
        let lease = Lease(Arc::new(LeaseHandle {
            verified,
            generation: publication.generation,
            authority: state.published.subscribe(),
            identity,
            terminal: AtomicU8::new(0),
            receiver,
            cancel,
        }));
        let future = lease_task(
            state.clone(),
            token,
            lease.0.verified.clone(),
            identity,
            updates,
            cancelled,
            lease_permit,
        );
        state
            .tx
            .try_send(Command::Lease(Box::pin(future)))
            .map_err(|_| Error::Busy)?;
        Ok(lease)
    }
    /// Stop admissions, invalidate leases and join refresh/lease/actual crypto
    /// workers. The handle remains joinable if this future is cancelled.
    pub async fn shutdown(&self) -> Result<(), Error> {
        let state = &self.0 .0;
        state.stop.send_replace(true);
        state.validations.close();
        state.leases.close();
        let mut join = state.join.lock().await;
        if let Some(handle) = join.as_mut() {
            handle.await.map_err(|_| Error::Unavailable)?;
        }
        *join = None;
        drop(join);
        state.crypto.shutdown().await?;
        state.http.shutdown().await
    }
}
#[derive(Clone, Copy)]
struct Status {
    deadline: Instant,
    failure: Option<Error>,
}
struct LeaseHandle {
    verified: Verified,
    generation: u64,
    authority: watch::Receiver<Arc<Publication>>,
    identity: KeyIdentity,
    terminal: AtomicU8,
    receiver: watch::Receiver<Status>,
    cancel: watch::Sender<bool>,
}
impl Drop for LeaseHandle {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}
fn encode_terminal(error: Error) -> u8 {
    match error {
        Error::InvalidConfiguration => 1,
        Error::Malformed => 2,
        Error::Authentication => 3,
        Error::Expired => 4,
        Error::Revoked => 5,
        Error::Busy => 6,
        Error::Unavailable => 7,
        Error::Cancelled => 8,
    }
}
fn decode_terminal(code: u8) -> Option<Error> {
    match code {
        1 => Some(Error::InvalidConfiguration),
        2 => Some(Error::Malformed),
        3 => Some(Error::Authentication),
        4 => Some(Error::Expired),
        5 => Some(Error::Revoked),
        6 => Some(Error::Busy),
        7 => Some(Error::Unavailable),
        8 => Some(Error::Cancelled),
        _ => None,
    }
}
/// Socket-owned issuer/subject lease. The bearer exists only in a bounded owned
/// refresh task; dropping the last lease cancels that task and zeroizes its token.
#[derive(Clone)]
pub struct Lease(Arc<LeaseHandle>);
impl fmt::Debug for Lease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Lease { [REDACTED] }")
    }
}
impl Lease {
    /// Authenticated issuer, mandatory for authorization authority matching.
    pub fn issuer(&self) -> &str {
        self.0.verified.issuer()
    }
    /// Authenticated subject scoped to that issuer.
    pub fn subject(&self) -> &str {
        self.0.verified.subject()
    }
    /// Verified authority generation captured by this authentication.
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    /// Current minimum token/key/revocation deadline; no outage extends it.
    pub fn deadline(&self) -> Instant {
        self.0.receiver.borrow().deadline
    }
    /// Current rejection reason, including a hard expiration observed locally.
    pub fn failure(&self) -> Option<Error> {
        if let Some(terminal) = decode_terminal(self.0.terminal.load(Ordering::Acquire)) {
            return Some(terminal);
        }
        let status = *self.0.receiver.borrow();
        let failure = status.failure.or_else(|| {
            // A queued lease may be dropped during manager shutdown before its
            // task starts. Closed status ownership is terminal, even without a
            // final watch value, and cannot leave the public health check green.
            if self.0.authority.borrow().identity(self.0.verified.key_id()) != Some(self.0.identity)
            {
                return Some(Error::Revoked);
            }
            (Instant::now() >= status.deadline || SystemTime::now() >= self.0.verified.expires_at())
                .then_some(Error::Expired)
        });
        if let Some(failure) = failure {
            let code = encode_terminal(failure);
            let first = self
                .0
                .terminal
                .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire)
                .map_or_else(|earlier| earlier, |_| code);
            return decode_terminal(first);
        }
        None
    }
    /// Wait until terminal revocation, authority failure, token expiry or service
    /// shutdown. Transport races this against read/permit/handler/write futures.
    pub async fn invalidated(&self) -> Error {
        let mut receiver = self.0.receiver.clone();
        let mut authority = self.0.authority.clone();
        loop {
            if let Some(failure) = self.failure() {
                return failure;
            }
            let deadline = receiver.borrow_and_update().deadline;
            tokio::select! {
                _=tokio::time::sleep_until(deadline)=>{},
                result=receiver.changed()=>if result.is_err() { return Error::Unavailable; },
                result=authority.changed()=>if result.is_err() { return Error::Unavailable; },
            }
        }
    }
}
async fn refresh(state: &State) -> Result<(), Error> {
    let previous = state.publication.read().await.clone();
    let generation = previous
        .generation
        .checked_add(1)
        .ok_or(Error::Unavailable)?;
    let publication = state
        .source
        .fetch(&state.http, &state.policy, generation, Some(&previous))
        .await?;
    *state.publication.write().await = publication.clone();
    state.published.send_replace(publication);
    Ok(())
}
async fn manager(state: Arc<State>, mut commands: mpsc::Receiver<Command>) {
    let mut stop = state.stop.subscribe();
    let mut interval = tokio::time::interval(state.runtime.refresh_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    let mut next_unknown = Instant::now();
    let mut leases = JoinSet::new();
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _=stop.changed()=>break,
            Some(_)=leases.join_next(), if !leases.is_empty()=>{},
            _=interval.tick()=>{
                tokio::select! { biased; _=stop.changed()=>break, _=refresh(&state)=>{} }
                // Schedule from completion. A fetch slower than its period must
                // leave a real interval for queued lease tasks/commands instead
                // of repeatedly winning the biased select with overdue ticks.
                interval.reset();
                // Errors retain only the original finite publication freshness.
            }
            command=commands.recv()=>match command {
                Some(Command::Lease(future))=>{ leases.spawn(future); }
                Some(Command::Unknown { kid, reply })=>{
                    let known = state.publication.read().await.keys.contains(&kid);
                    let now = Instant::now();
                    let result = if known { Ok(()) } else if now < next_unknown { Err(Error::Authentication) } else {
                        next_unknown = now + state.runtime.unknown_kid_cooldown;
                        tokio::select! { biased; _=stop.changed()=>break, result=refresh(&state)=>result }
                    };
                    let _=reply.send(result);
                }
                None=>break,
            }
        }
    }
    state.stop.send_replace(true);
    commands.close();
    while let Ok(command) = commands.try_recv() {
        if let Command::Unknown { reply, .. } = command {
            let _ = reply.send(Err(Error::Unavailable));
        }
    }
    while leases.join_next().await.is_some() {}
    // Last-handle Drop also owns a real joined shutdown path. No background
    // lease/HTTP/resolver/crypto work remains merely because nobody awaited the
    // public shutdown method; admitted OS resolver calls must still finish.
    let _ = state.crypto.shutdown().await;
    let _ = state.http.shutdown().await;
}
async fn lease_task(
    state: Arc<State>,
    token: Secret,
    verified: Verified,
    identity: KeyIdentity,
    updates: watch::Sender<Status>,
    mut cancelled: watch::Receiver<bool>,
    _permit: OwnedSemaphorePermit,
) {
    let mut stop = state.stop.subscribe();
    let mut published = state.published.subscribe();
    let mut refresh_at = Instant::now() + state.runtime.revocation_refresh;
    let terminal = 'lifecycle: loop {
        let status = *updates.borrow();
        if *stop.borrow() {
            break Error::Unavailable;
        }
        if *cancelled.borrow() {
            break Error::Cancelled;
        }
        // A publication can also occur between validate's final check and this
        // task subscribing; inspect the current generation before waiting for
        // its next watch notification.
        if published.borrow().identity(verified.key_id()) != Some(identity) {
            break Error::Revoked;
        }
        if Instant::now() >= status.deadline || SystemTime::now() >= verified.expires_at() {
            break Error::Expired;
        }
        tokio::select! {
            biased;
            _=stop.changed()=>break Error::Unavailable,
            _=cancelled.changed()=>break Error::Cancelled,
            _=tokio::time::sleep_until(status.deadline)=>break Error::Expired,
            changed=published.changed()=>{
                if changed.is_err() { break Error::Unavailable; }
                let current = published.borrow_and_update().clone();
                if current.identity(verified.key_id()) != Some(identity) { break Error::Revoked; }
                // A new key generation cannot independently extend revocation.
            }
            _=tokio::time::sleep_until(refresh_at)=>{
                let request=state.revocation.check(&state.http,&state.policy,token.as_bytes(),&verified,state.runtime.revocation_lease);
                tokio::pin!(request);
                let result = loop {
                    tokio::select! {
                        biased;
                        _=stop.changed()=>break 'lifecycle Error::Unavailable,
                        _=cancelled.changed()=>break 'lifecycle Error::Cancelled,
                        _=tokio::time::sleep_until(status.deadline)=>break 'lifecycle Error::Expired,
                        changed=published.changed()=>{
                            if changed.is_err() {break 'lifecycle Error::Unavailable;}
                            let current=published.borrow_and_update().clone();
                            if current.identity(verified.key_id())!=Some(identity) {break 'lifecycle Error::Revoked;}
                        }
                        result=&mut request=>break result,
                    }
                };
                match result {
                    Ok(revocation)=>{
                        let current = published.borrow_and_update().clone();
                        if current.identity(verified.key_id()) != Some(identity) { break Error::Revoked; }
                        let until = revocation.min(current.fresh_until).min(verified.token_deadline);
                        if Instant::now() >= status.deadline || Instant::now() >= until { break Error::Expired; }
                        updates.send_replace(Status { deadline: until, failure: None });
                        refresh_at = Instant::now()+state.runtime.revocation_refresh;
                    }
                    // Preserve the old finite lease through a transient outage,
                    // retry within it; failure never publishes a new deadline.
                    Err(Error::Unavailable | Error::Busy)=>{ refresh_at = (Instant::now()+state.runtime.revocation_refresh).min(status.deadline); }
                    Err(error)=>break error,
                }
            }
        }
    };
    let status = *updates.borrow();
    updates.send_replace(Status {
        deadline: status.deadline,
        failure: Some(terminal),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use serde_json::json;

    fn publication() -> (Arc<Publication>, Arc<jwt::KeySet>, Policy) {
        let policy = Policy {
            issuer: "https://issuer.example".to_owned(),
            audiences: vec!["partitionline".to_owned()],
            algorithms: vec![super::super::Algorithm::Es256],
            access_token: super::super::AccessToken::AtJwt,
            limits: super::super::Limits::default(),
        };
        // Public structural points test cache identity, without a proof claim.
        let bytes = serde_json::to_vec(&json!({"keys":[{"kid":"key","kty":"EC","crv":"P-256","x":URL_SAFE_NO_PAD.encode([1u8;32]),"y":URL_SAFE_NO_PAD.encode([2u8;32])}]})).unwrap();
        let keys = Arc::new(jwt::KeySet::parse_authority(&bytes, &policy).unwrap());
        let publication = Arc::new(Publication::new(
            keys.clone(),
            1,
            Instant::now() + Duration::from_secs(60),
            None,
        ));
        (publication, keys, policy)
    }
    fn lease(
        publication: Arc<Publication>,
        deadline: Instant,
    ) -> (
        Lease,
        watch::Sender<Status>,
        watch::Sender<Arc<Publication>>,
    ) {
        let identity = publication.identity("key").unwrap();
        let (authority, receiver) = watch::channel(publication);
        let (updates, status) = watch::channel(Status {
            deadline,
            failure: None,
        });
        let (cancel, _) = watch::channel(false);
        let lease = Lease(Arc::new(LeaseHandle {
            verified: Verified {
                issuer: "https://issuer.example".to_owned(),
                subject: "user".to_owned(),
                audiences: vec!["partitionline".to_owned()],
                token_id: None,
                key_id: "key".to_owned(),
                expires_at: SystemTime::now() + Duration::from_secs(60),
                token_deadline: Instant::now() + Duration::from_secs(60),
                deadline,
            },
            generation: 1,
            authority: receiver,
            identity,
            terminal: AtomicU8::new(0),
            receiver: status,
            cancel,
        }));
        (lease, updates, authority)
    }
    #[test]
    fn bounded_key_epochs_preserve_refresh_and_removal_even_when_watch_coalesces() {
        let (initial, keys, policy) = publication();
        let continuous = Arc::new(Publication::new(
            keys.clone(),
            2,
            initial.fresh_until,
            Some(&initial),
        ));
        assert!(continuous.identity("key") == initial.identity("key"));
        let empty = Arc::new(jwt::KeySet::parse_authority(br#"{"keys":[]}"#, &policy).unwrap());
        let removed = Arc::new(Publication::new(
            empty,
            3,
            initial.fresh_until,
            Some(&continuous),
        ));
        assert!(removed.identities.is_empty());
        let restored = Arc::new(Publication::new(
            keys,
            4,
            initial.fresh_until,
            Some(&removed),
        ));
        assert!(restored.identity("key") != initial.identity("key"));
        assert_eq!(restored.identities.len(), 1);
        // Keep the status authority alive: this fixture exercises key removal,
        // while a closed status channel independently invalidates the lease.
        let (lease, _updates, published) = lease(initial.clone(), initial.fresh_until);
        published.send_replace(removed);
        published.send_replace(restored);
        // No lease task runs between these two valid authority publications.
        assert_eq!(lease.failure(), Some(Error::Revoked));
        published.send_replace(initial);
        assert_eq!(lease.failure(), Some(Error::Revoked));
    }
    #[test]
    fn an_observed_deadline_is_terminal_even_if_a_later_status_is_restored() {
        let (publication, _, _) = publication();
        let (lease, updates, _) = lease(publication, Instant::now() - Duration::from_secs(1));
        assert_eq!(lease.failure(), Some(Error::Expired));
        updates.send_replace(Status {
            deadline: Instant::now() + Duration::from_secs(30),
            failure: None,
        });
        assert_eq!(lease.failure(), Some(Error::Expired));
    }
    #[test]
    fn runtime_configuration_cannot_create_unbounded_refresh_or_lease_admission() {
        let (_, _, policy) = publication();
        assert!(RuntimeLimits::default().validate(&policy).is_ok());
        for invalid in [
            RuntimeLimits {
                refresh_interval: Duration::from_nanos(1),
                ..RuntimeLimits::default()
            },
            RuntimeLimits {
                revocation_refresh: Duration::from_nanos(1),
                ..RuntimeLimits::default()
            },
            RuntimeLimits {
                revocation_lease: Duration::from_secs(6),
                ..RuntimeLimits::default()
            },
            RuntimeLimits {
                active_leases: 1025,
                ..RuntimeLimits::default()
            },
            RuntimeLimits {
                validations: 257,
                ..RuntimeLimits::default()
            },
        ] {
            assert_eq!(invalid.validate(&policy), Err(Error::InvalidConfiguration));
        }
    }
}
