use super::{
    http::{Client, Endpoint, HttpsTrust},
    introspection::{Authority, Introspection},
    jwt, Error, PinnedVerifier, Policy, Verified, Work,
};
use crate::security::sasl::Secret;
use serde_json::Value;
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
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
    /// Proactive signing-authority refresh interval, at most half key freshness.
    pub refresh_interval: Duration,
    /// Global unknown-kid refresh cooldown, 1 through 60 seconds.
    /// No per-attacker-kid unbounded cache is retained.
    pub unknown_kid_cooldown: Duration,
    /// Maximum one positive provider revocation lease, positive through 5 seconds.
    pub revocation_lease: Duration,
    /// Proactive revocation refresh interval, at most half the positive lease.
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
            || self.refresh_interval.is_zero()
            || self.refresh_interval > policy.limits.authority_freshness / 2
            || self.unknown_kid_cooldown < Duration::from_secs(1)
            || self.unknown_kid_cooldown > Duration::from_secs(60)
            || self.revocation_lease.is_zero()
            || self.revocation_lease > Duration::from_secs(5)
            || self.revocation_refresh.is_zero()
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
        let keys = Arc::new(jwt::KeySet::parse(&bytes, policy)?);
        let fresh_until = started
            .checked_add(policy.limits.authority_freshness)
            .ok_or(Error::InvalidConfiguration)?;
        if Instant::now() >= fresh_until {
            return Err(Error::Expired);
        }
        Ok(Arc::new(Publication {
            keys,
            generation,
            fresh_until,
        }))
    }
}
struct Publication {
    keys: Arc<jwt::KeySet>,
    generation: u64,
    fresh_until: Instant,
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
        let publication = match source.fetch(&http, &config.policy, 1).await {
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
        let fingerprint = publication
            .keys
            .fingerprint(verified.key_id())
            .ok_or(Error::Authentication)?;
        let mut stop = state.stop.subscribe();
        if *stop.borrow() {
            return Err(Error::Unavailable);
        }
        let revocation = tokio::select! { biased; _=stop.changed()=>return Err(Error::Unavailable), r=state.revocation.check(&state.http,&state.policy,token.as_bytes(),&verified,state.runtime.revocation_lease)=>r? };
        let until = verified
            .token_deadline
            .min(publication.fresh_until)
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
            receiver,
            cancel,
        }));
        let future = lease_task(
            state.clone(),
            token,
            lease.0.verified.clone(),
            fingerprint,
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
    receiver: watch::Receiver<Status>,
    cancel: watch::Sender<bool>,
}
impl Drop for LeaseHandle {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
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
        let status = *self.0.receiver.borrow();
        status.failure.or_else(|| {
            (Instant::now() >= status.deadline || SystemTime::now() >= self.0.verified.expires_at())
                .then_some(Error::Expired)
        })
    }
    /// Wait until terminal revocation, authority failure, token expiry or service
    /// shutdown. Transport races this against read/permit/handler/write futures.
    pub async fn invalidated(&self) -> Error {
        let mut receiver = self.0.receiver.clone();
        loop {
            if let Some(failure) = self.failure() {
                return failure;
            }
            let deadline = receiver.borrow_and_update().deadline;
            tokio::select! {
                _=tokio::time::sleep_until(deadline)=>{},
                result=receiver.changed()=>if result.is_err() { return Error::Unavailable; },
            }
        }
    }
}
async fn refresh(state: &State) -> Result<(), Error> {
    let generation = state
        .publication
        .read()
        .await
        .generation
        .checked_add(1)
        .ok_or(Error::Unavailable)?;
    let publication = state
        .source
        .fetch(&state.http, &state.policy, generation)
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
    fingerprint: [u8; 32],
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
                if current.keys.fingerprint(verified.key_id()) != Some(fingerprint) { break Error::Revoked; }
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
                            if current.keys.fingerprint(verified.key_id())!=Some(fingerprint) {break 'lifecycle Error::Revoked;}
                        }
                        result=&mut request=>break result,
                    }
                };
                match result {
                    Ok(revocation)=>{
                        let current = published.borrow_and_update().clone();
                        if current.keys.fingerprint(verified.key_id()) != Some(fingerprint) { break Error::Revoked; }
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
