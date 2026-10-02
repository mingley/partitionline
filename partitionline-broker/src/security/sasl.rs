//! Bounded PLAIN and SCRAM server mechanism primitives, separate from Kafka I/O.
//!
//! A session captures an immutable credential generation. Rotation affects new
//! admissions; admitted SCRAM sessions retain their captured verifier. Only
//! salt, iterations, StoredKey and ServerKey are exported for persistence.
//! Passwords, salted passwords and ClientKeys are owned zeroizing temporaries.
//! Password bytes are UTF-8 without SASLprep, matching Kafka's ScramFormatter.
//! SCRAM usernames are ASCII, matching Kafka's SASLNAME parser; PLAIN identities
//! can be UTF-8. RFC coverage is limited to the pinned vectors and AuthMessage
//! construction, without a general SASLprep conformance claim.
//! Callers remain responsible for any copies they made before transferring input.
//!
//! This implements non-PLUS SCRAM with GS2 `n`, empty/self authorization identity,
//! exact channel-binding header verification, no mandatory extensions and no
//! duplicate attributes. These stricter checks intentionally reject some messages
//! Apache's parser accepts. No transport, Kafka SASL dispatch or session-lifetime
//! reauthentication policy is provided here.

use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use hmac::{
    digest::{CtOutput, Output},
    Hmac, KeyInit, Mac,
};
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256, Sha512};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use zeroize::{Zeroize, Zeroizing};

/// Fixed categories; neither Display nor Debug contains input or credential data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Invalid configured bound or imported verifier.
    InvalidCredential,
    /// Malformed or unsupported mechanism message.
    InvalidMessage,
    /// Incorrect credentials, proof, nonce or authorization identity.
    AuthenticationFailed,
    /// Authentication/worker admission or short credential access is busy.
    Busy,
    /// Caller cancelled its work or the blocking worker failed.
    Cancelled,
    /// OS cryptographic randomness is unavailable.
    Randomness,
    /// Credential generation counter cannot be advanced.
    Unavailable,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidCredential => "invalid SASL credential or bound",
            Self::InvalidMessage => "invalid SASL message",
            Self::AuthenticationFailed => "SASL authentication failed",
            Self::Busy => "SASL work admission exhausted",
            Self::Cancelled => "SASL work cancelled",
            Self::Randomness => "SASL randomness unavailable",
            Self::Unavailable => "SASL credential service unavailable",
        })
    }
}
impl std::error::Error for Error {}

/// Positive resource ceilings validated before parsing, copying or expensive work.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum one message, at most 64 KiB.
    pub message_bytes: usize,
    /// Maximum UTF-8 identity, at most 1024 bytes.
    pub identity_bytes: usize,
    /// Maximum password, at most 16 KiB.
    pub password_bytes: usize,
    /// Maximum salt, 32 through 1024 bytes; imported salts may have 16 bytes.
    pub salt_bytes: usize,
    /// Maximum combined nonce, 64 through 4096 bytes.
    pub nonce_bytes: usize,
    /// Maximum PBKDF2 iterations, 4096 through 1,000,000.
    pub iterations: u32,
    /// Maximum stored identity/algorithm entries, at most 4096.
    pub credentials: usize,
    /// Maximum admitted operations/sessions, at most 256.
    pub admissions: usize,
    /// Maximum queued or running blocking workers, at most 64.
    pub workers: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            message_bytes: 8192,
            identity_bytes: 256,
            password_bytes: 4096,
            salt_bytes: 64,
            nonce_bytes: 256,
            iterations: 16384,
            credentials: 1024,
            admissions: 32,
            workers: 4,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if !(1..=65536).contains(&self.message_bytes)
            || !(1..=1024).contains(&self.identity_bytes)
            || !(1..=16384).contains(&self.password_bytes)
            || !(32..=1024).contains(&self.salt_bytes)
            || !(64..=4096).contains(&self.nonce_bytes)
            || !(4096..=1_000_000).contains(&self.iterations)
            || !(1..=4096).contains(&self.credentials)
            || !(1..=256).contains(&self.admissions)
            || !(1..=64).contains(&self.workers)
        {
            return Err(Error::InvalidCredential);
        }
        Ok(())
    }
    fn identity(self, identity: &str) -> bool {
        !identity.is_empty()
            && identity.len() <= self.identity_bytes
            && !identity.chars().any(char::is_control)
    }
    fn password(self, password: &[u8]) -> bool {
        !password.is_empty()
            && password.len() <= self.password_bytes
            && std::str::from_utf8(password).is_ok()
    }
}

/// Supported Kafka non-PLUS SCRAM hashes.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Algorithm {
    /// SCRAM-SHA-256.
    Sha256,
    /// SCRAM-SHA-512.
    Sha512,
}
impl Algorithm {
    /// Exact mechanism name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
    fn size(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }
    fn hash(self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => Sha256::digest(bytes).to_vec(),
            Self::Sha512 => Sha512::digest(bytes).to_vec(),
        }
    }
    fn mac(self, key: &[u8], bytes: &[u8]) -> Result<Zeroizing<Vec<u8>>, Error> {
        match self {
            Self::Sha256 => {
                let mut mac =
                    Hmac::<Sha256>::new_from_slice(key).map_err(|_| Error::InvalidCredential)?;
                mac.update(bytes);
                let output = mac.finalize();
                Ok(Zeroizing::new(output.as_bytes().to_vec()))
            }
            Self::Sha512 => {
                let mut mac =
                    Hmac::<Sha512>::new_from_slice(key).map_err(|_| Error::InvalidCredential)?;
                mac.update(bytes);
                let output = mac.finalize();
                Ok(Zeroizing::new(output.as_bytes().to_vec()))
            }
        }
    }
    // RustCrypto CtOutput::Eq uses ctutils::CtEq, not ordinary byte equality.
    fn equal(self, a: &[u8], b: &[u8]) -> bool {
        match self {
            Self::Sha256 => match (Output::<Sha256>::try_from(a), Output::<Sha256>::try_from(b)) {
                (Ok(a), Ok(b)) => CtOutput::<Sha256>::new(a) == CtOutput::<Sha256>::new(b),
                _ => false,
            },
            Self::Sha512 => match (Output::<Sha512>::try_from(a), Output::<Sha512>::try_from(b)) {
                (Ok(a), Ok(b)) => CtOutput::<Sha512>::new(a) == CtOutput::<Sha512>::new(b),
                _ => false,
            },
        }
    }
}

/// Owned ephemeral input, zeroized on drop; Debug never exposes its contents.
pub struct Secret(Zeroizing<Vec<u8>>);
impl Secret {
    /// Transfer ownership; bounds are checked by the receiving operation.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }
}
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret { [REDACTED] }")
    }
}

/// The only persisted credential form. No password, SaltedPassword or ClientKey.
pub struct CredentialForm {
    /// SCRAM hash algorithm.
    pub algorithm: Algorithm,
    /// Public salt bytes.
    pub salt: Vec<u8>,
    /// PBKDF2 work count.
    pub iterations: u32,
    /// Hash of ClientKey, exactly the algorithm's hash size.
    pub stored_key: Vec<u8>,
    /// ServerKey, exactly the algorithm's hash size.
    pub server_key: Vec<u8>,
}
impl fmt::Debug for CredentialForm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialForm { [REDACTED] }")
    }
}
/// Validated verifier, with zeroizing key ownership and redacted diagnostics.
#[derive(Clone)]
pub struct Credential {
    algorithm: Algorithm,
    salt: Vec<u8>,
    iterations: u32,
    stored_key: Zeroizing<Vec<u8>>,
    server_key: Zeroizing<Vec<u8>>,
}
impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credential { [REDACTED] }")
    }
}
impl Credential {
    /// Validate an imported salted verifier under the configured bounds.
    pub fn from_form(form: CredentialForm, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        if !(16..=limits.salt_bytes).contains(&form.salt.len())
            || !(4096..=limits.iterations).contains(&form.iterations)
            || form.stored_key.len() != form.algorithm.size()
            || form.server_key.len() != form.algorithm.size()
        {
            return Err(Error::InvalidCredential);
        }
        Ok(Self {
            algorithm: form.algorithm,
            salt: form.salt,
            iterations: form.iterations,
            stored_key: Zeroizing::new(form.stored_key),
            server_key: Zeroizing::new(form.server_key),
        })
    }
    /// Export only salted verifier fields for caller-owned persistence.
    pub fn form(&self) -> CredentialForm {
        CredentialForm {
            algorithm: self.algorithm,
            salt: self.salt.clone(),
            iterations: self.iterations,
            stored_key: self.stored_key.to_vec(),
            server_key: self.server_key.to_vec(),
        }
    }
}
fn derive(
    algorithm: Algorithm,
    password: &[u8],
    salt: Vec<u8>,
    iterations: u32,
) -> Result<Credential, Error> {
    let mut salted = Zeroizing::new(vec![0; algorithm.size()]);
    match algorithm {
        Algorithm::Sha256 => pbkdf2_hmac::<Sha256>(password, &salt, iterations, &mut salted),
        Algorithm::Sha512 => pbkdf2_hmac::<Sha512>(password, &salt, iterations, &mut salted),
    }
    let client_key = algorithm.mac(&salted, b"Client Key")?;
    let stored_key = Zeroizing::new(algorithm.hash(&client_key));
    let server_key = algorithm.mac(&salted, b"Server Key")?;
    Ok(Credential {
        algorithm,
        salt,
        iterations,
        stored_key,
        server_key,
    })
}

struct Snapshot {
    generation: u64,
    records: HashMap<(String, Algorithm), Arc<Credential>>,
}
struct Inner {
    limits: Limits,
    snapshot: RwLock<Arc<Snapshot>>,
    admissions: Arc<Semaphore>,
    workers: Arc<Semaphore>,
}
/// Shared immutable credential generations and bounded expensive-work pools.
#[derive(Clone)]
pub struct Service(Arc<Inner>);
impl fmt::Debug for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Service { [REDACTED] }")
    }
}
/// Current active admission and queued/running blocking-worker counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCounts {
    /// Admitted derivations, PLAIN operations and SCRAM sessions.
    pub admissions: usize,
    /// Blocking jobs whose permits remain held through actual completion.
    pub workers: usize,
}
struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
impl Service {
    /// Construct empty verifier storage with validated hard ceilings.
    pub fn new(limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self(Arc::new(Inner {
            limits,
            snapshot: RwLock::new(Arc::new(Snapshot {
                generation: 0,
                records: HashMap::new(),
            })),
            admissions: Arc::new(Semaphore::new(limits.admissions)),
            workers: Arc::new(Semaphore::new(limits.workers)),
        })))
    }
    /// Replace all salted verifiers atomically; existing sessions retain their generation.
    pub fn replace(&self, records: Vec<(String, Credential)>) -> Result<u64, Error> {
        if records.len() > self.0.limits.credentials {
            return Err(Error::InvalidCredential);
        }
        let mut map = HashMap::with_capacity(records.len());
        for (identity, credential) in records {
            if !self.0.limits.identity(&identity) {
                return Err(Error::InvalidCredential);
            }
            let credential = Credential::from_form(credential.form(), self.0.limits)?;
            if map
                .insert((identity, credential.algorithm), Arc::new(credential))
                .is_some()
            {
                return Err(Error::InvalidCredential);
            }
        }
        let mut snapshot = self.0.snapshot.try_write().map_err(|_| Error::Busy)?;
        let generation = snapshot
            .generation
            .checked_add(1)
            .ok_or(Error::Unavailable)?;
        *snapshot = Arc::new(Snapshot {
            generation,
            records: map,
        });
        Ok(generation)
    }
    /// Observe bounds without exposing credential material.
    pub fn work_counts(&self) -> WorkCounts {
        WorkCounts {
            admissions: self.0.limits.admissions - self.0.admissions.available_permits(),
            workers: self.0.limits.workers - self.0.workers.available_permits(),
        }
    }
    fn admission(&self) -> Result<OwnedSemaphorePermit, Error> {
        self.0
            .admissions
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)
    }
    fn snapshot(&self) -> Result<Arc<Snapshot>, Error> {
        Ok(self.0.snapshot.try_read().map_err(|_| Error::Busy)?.clone())
    }
    async fn work<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let permit = self
            .0
            .workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel_on_drop = CancelOnDrop(cancelled.clone());
        tokio::task::spawn_blocking(move || {
            let _work_permit = permit;
            if cancelled.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            let result = job();
            if cancelled.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            result
        })
        .await
        .map_err(|_| Error::Cancelled)?
    }
    /// Derive a new salted verifier outside Tokio; never automatically persists it.
    ///
    /// Cancellation frees admission immediately. An already-running PBKDF2 cannot
    /// be preempted; its bounded worker permit and zeroizing password remain owned
    /// until completion, and the cancelled caller cannot receive success.
    pub async fn derive(
        &self,
        algorithm: Algorithm,
        password: Secret,
        iterations: u32,
    ) -> Result<Credential, Error> {
        if !self.0.limits.password(&password.0)
            || !(4096..=self.0.limits.iterations).contains(&iterations)
        {
            return Err(Error::InvalidCredential);
        }
        let _admission = self.admission()?;
        self.work(move || {
            let mut salt = vec![0; 32];
            getrandom::fill(&mut salt).map_err(|_| Error::Randomness)?;
            derive(algorithm, &password.0, salt, iterations)
        })
        .await
    }
    /// Authenticate one owned PLAIN message against salted SCRAM verifiers.
    ///
    /// Authzid is empty or equals authcid; use verified TLS at later integration.
    /// This primitive does not decide which connection is authorized to use PLAIN.
    pub async fn plain(&self, message: Secret) -> Result<Identity, Error> {
        if message.0.len() > self.0.limits.message_bytes {
            return Err(Error::InvalidMessage);
        }
        let _admission = self.admission()?;
        let snapshot = self.snapshot()?;
        let limits = self.0.limits;
        self.work(move || {
            let parts: Vec<_> = message.0.split(|b| *b == 0).take(4).collect();
            if parts.len() != 3 {
                return Err(Error::InvalidMessage);
            }
            let authz = std::str::from_utf8(parts[0]).map_err(|_| Error::InvalidMessage)?;
            let name = std::str::from_utf8(parts[1]).map_err(|_| Error::InvalidMessage)?;
            let password = parts[2];
            if !limits.identity(name)
                || (!authz.is_empty() && !limits.identity(authz))
                || !limits.password(password)
            {
                return Err(Error::InvalidMessage);
            }
            if !authz.is_empty() && authz != name {
                return Err(Error::AuthenticationFailed);
            }
            let credential = snapshot
                .records
                .get(&(name.to_owned(), Algorithm::Sha256))
                .or_else(|| snapshot.records.get(&(name.to_owned(), Algorithm::Sha512)))
                .ok_or(Error::AuthenticationFailed)?;
            let candidate = derive(
                credential.algorithm,
                password,
                credential.salt.clone(),
                credential.iterations,
            )?;
            if !credential
                .algorithm
                .equal(&candidate.stored_key, &credential.stored_key)
            {
                return Err(Error::AuthenticationFailed);
            }
            Ok(Identity {
                name: name.to_owned(),
                generation: snapshot.generation,
            })
        })
        .await
    }
    /// Admit a one-shot SCRAM session capturing the current credential generation.
    pub fn scram(&self, algorithm: Algorithm) -> Result<ScramSession, Error> {
        let admission = self.admission()?;
        Ok(ScramSession {
            service: self.clone(),
            snapshot: self.snapshot()?,
            algorithm,
            _admission: admission,
            challenge: None,
            started: false,
        })
    }
}

/// Authenticated identity returned only after successful verification.
pub struct Identity {
    name: String,
    generation: u64,
}
impl Identity {
    /// Authenticated, decoded UTF-8 principal.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Immutable credential generation captured by the operation.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Identity { [REDACTED] }")
    }
}
/// Successful SCRAM final response and its authenticated identity.
pub struct ScramResult {
    /// SASL server-final bytes; contains the verified server signature.
    pub message: Vec<u8>,
    /// Identity established by this one-shot exchange.
    pub identity: Identity,
}
impl fmt::Debug for ScramResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScramResult { [REDACTED] }")
    }
}
struct Challenge {
    identity: String,
    credential: Arc<Credential>,
    gs2: String,
    first_bare: String,
    server_first: String,
    nonce: String,
}
/// One admitted SCRAM exchange; errors are terminal, and final consumes the session.
pub struct ScramSession {
    service: Service,
    snapshot: Arc<Snapshot>,
    algorithm: Algorithm,
    _admission: OwnedSemaphorePermit,
    challenge: Option<Challenge>,
    started: bool,
}
impl fmt::Debug for ScramSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScramSession { [REDACTED] }")
    }
}
impl ScramSession {
    /// Parse bounded client-first and issue a cryptographically random server challenge.
    pub fn challenge(&mut self, message: &[u8]) -> Result<Vec<u8>, Error> {
        if self.started {
            self.challenge = None;
            return Err(Error::InvalidMessage);
        }
        let mut random = [0; 32];
        if getrandom::fill(&mut random).is_err() {
            self.started = true;
            return Err(Error::Randomness);
        }
        self.challenge_with_nonce(message, &URL_SAFE_NO_PAD.encode(random))
    }
    fn challenge_with_nonce(&mut self, message: &[u8], suffix: &str) -> Result<Vec<u8>, Error> {
        if self.started {
            self.challenge = None;
            return Err(Error::InvalidMessage);
        }
        self.started = true;
        let limits = self.service.0.limits;
        let message = text(message, limits)?;
        let mut header = message.splitn(3, ',');
        if header.next() != Some("n") {
            return Err(Error::InvalidMessage);
        }
        let authz = header.next().ok_or(Error::InvalidMessage)?;
        let bare = header.next().ok_or(Error::InvalidMessage)?;
        let fields = attributes(bare)?;
        if fields.first().map(|v| v.0) != Some('n') || fields.get(1).map(|v| v.0) != Some('r') {
            return Err(Error::InvalidMessage);
        }
        let name = decode_name(field(&fields, 'n')?)?;
        if !name.is_ascii() || !limits.identity(&name) {
            return Err(Error::InvalidMessage);
        }
        if !authz.is_empty() {
            let authorized = decode_name(authz.strip_prefix("a=").ok_or(Error::InvalidMessage)?)?;
            if !limits.identity(&authorized) || authorized != name {
                return Err(Error::AuthenticationFailed);
            }
        }
        let client_nonce = field(&fields, 'r')?;
        if !nonce(client_nonce)
            || client_nonce.len().saturating_add(suffix.len()) > limits.nonce_bytes
        {
            return Err(Error::InvalidMessage);
        }
        let credential = self
            .snapshot
            .records
            .get(&(name.clone(), self.algorithm))
            .ok_or(Error::AuthenticationFailed)?
            .clone();
        let nonce = format!("{client_nonce}{suffix}");
        let server_first = format!(
            "r={nonce},s={},i={}",
            STANDARD.encode(&credential.salt),
            credential.iterations
        );
        if server_first.len() > limits.message_bytes {
            return Err(Error::InvalidMessage);
        }
        let reply = server_first.as_bytes().to_vec();
        self.challenge = Some(Challenge {
            identity: name,
            credential,
            gs2: format!("n,{authz},"),
            first_bare: bare.to_owned(),
            server_first,
            nonce,
        });
        Ok(reply)
    }
    /// Verify final proof outside Tokio and consume the session, including on failure.
    pub async fn finish(mut self, message: Secret) -> Result<ScramResult, Error> {
        let challenge = self.challenge.take().ok_or(Error::InvalidMessage)?;
        let limits = self.service.0.limits;
        let generation = self.snapshot.generation;
        if message.0.len() > limits.message_bytes {
            return Err(Error::InvalidMessage);
        }
        self.service
            .work(move || verify_final(challenge, &message.0, limits, generation))
            .await
    }
}
fn text(bytes: &[u8], limits: Limits) -> Result<&str, Error> {
    if bytes.is_empty() || bytes.len() > limits.message_bytes {
        return Err(Error::InvalidMessage);
    }
    let value = std::str::from_utf8(bytes).map_err(|_| Error::InvalidMessage)?;
    if value.chars().any(char::is_control) {
        return Err(Error::InvalidMessage);
    }
    Ok(value)
}
fn nonce(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| (33..=126).contains(&b) && b != b',')
}
fn attributes(message: &str) -> Result<Vec<(char, &str)>, Error> {
    let mut fields = Vec::new();
    for part in message.split(',') {
        let (key, value) = part.split_once('=').ok_or(Error::InvalidMessage)?;
        let mut key_chars = key.chars();
        let key = key_chars.next().ok_or(Error::InvalidMessage)?;
        if key_chars.next().is_some()
            || !key.is_ascii_alphabetic()
            || key == 'm'
            || value.is_empty()
            || fields.len() >= 16
            || fields.iter().any(|(prior, _)| *prior == key)
        {
            return Err(Error::InvalidMessage);
        }
        fields.push((key, value));
    }
    Ok(fields)
}
fn field<'a>(fields: &[(char, &'a str)], key: char) -> Result<&'a str, Error> {
    fields
        .iter()
        .find(|(k, _)| *k == key)
        .map(|v| v.1)
        .ok_or(Error::InvalidMessage)
}
fn decode_name(value: &str) -> Result<String, Error> {
    let mut out = String::new();
    let mut rest = value;
    while let Some(index) = rest.find('=') {
        out.push_str(&rest[..index]);
        rest = &rest[index..];
        if let Some(tail) = rest.strip_prefix("=2C") {
            out.push(',');
            rest = tail;
        } else if let Some(tail) = rest.strip_prefix("=3D") {
            out.push('=');
            rest = tail;
        } else {
            return Err(Error::InvalidMessage);
        }
    }
    out.push_str(rest);
    Ok(out)
}
fn verify_final(
    challenge: Challenge,
    message: &[u8],
    limits: Limits,
    generation: u64,
) -> Result<ScramResult, Error> {
    let message = text(message, limits)?;
    let fields = attributes(message)?;
    if fields.first().map(|v| v.0) != Some('c')
        || fields.get(1).map(|v| v.0) != Some('r')
        || fields.last().map(|v| v.0) != Some('p')
    {
        return Err(Error::InvalidMessage);
    }
    if field(&fields, 'c')? != STANDARD.encode(challenge.gs2.as_bytes())
        || field(&fields, 'r')? != challenge.nonce
    {
        return Err(Error::AuthenticationFailed);
    }
    let proof = Zeroizing::new(
        STANDARD
            .decode(field(&fields, 'p')?)
            .map_err(|_| Error::InvalidMessage)?,
    );
    let algorithm = challenge.credential.algorithm;
    if proof.len() != algorithm.size() {
        return Err(Error::AuthenticationFailed);
    }
    let (without_proof, _) = message.rsplit_once(",p=").ok_or(Error::InvalidMessage)?;
    let auth = format!(
        "{},{},{}",
        challenge.first_bare, challenge.server_first, without_proof
    );
    let signature = algorithm.mac(&challenge.credential.stored_key, auth.as_bytes())?;
    let mut client_key = Zeroizing::new(vec![0; algorithm.size()]);
    for ((out, p), s) in client_key
        .iter_mut()
        .zip(proof.iter())
        .zip(signature.iter())
    {
        *out = *p ^ *s;
    }
    let mut candidate = algorithm.hash(&client_key);
    let valid = algorithm.equal(&candidate, &challenge.credential.stored_key);
    candidate.zeroize();
    if !valid {
        return Err(Error::AuthenticationFailed);
    }
    let server_signature = algorithm.mac(&challenge.credential.server_key, auth.as_bytes())?;
    Ok(ScramResult {
        message: format!("v={}", STANDARD.encode(server_signature.as_slice())).into_bytes(),
        identity: Identity {
            name: challenge.identity,
            generation,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    fn hex(value: &str) -> TestResult<Vec<u8>> {
        if value.len() % 2 != 0 {
            return Err("odd fixture hex length".into());
        }
        (0..value.len())
            .step_by(2)
            .map(|index| Ok(u8::from_str_radix(&value[index..index + 2], 16)?))
            .collect()
    }

    async fn replay_scram(data: &str) -> TestResult<usize> {
        let mut count = 0;
        for line in data.lines().skip(1) {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 12);
            let name = columns[0];
            let algorithm = match columns[1] {
                "SCRAM-SHA-256" => Algorithm::Sha256,
                "SCRAM-SHA-512" => Algorithm::Sha512,
                _ => return Err("unknown fixture mechanism".into()),
            };
            let iterations: u32 = columns[4].parse()?;
            let salt = hex(columns[3])?;
            let expected_stored = hex(columns[9])?;
            let expected_server = hex(columns[10])?;
            let password = Secret::new(hex(columns[2])?);
            // Byte-match actual Apache/RFC verifiers, independently of the proof.
            // The low/high-iteration fixtures remain bounded and are not admitted.
            let derived = derive(algorithm, &password.0, salt.clone(), iterations)?;
            assert_eq!(derived.stored_key.as_slice(), expected_stored, "{name}");
            assert_eq!(derived.server_key.as_slice(), expected_server, "{name}");
            let credential = Credential::from_form(
                CredentialForm {
                    algorithm,
                    salt,
                    iterations,
                    stored_key: expected_stored,
                    server_key: expected_server,
                },
                Limits::default(),
            );
            let expected = columns[11] == "true" && !name.starts_with("hardening-");
            if let Err(error) = &credential {
                assert_eq!(*error, Error::InvalidCredential, "{name}");
                assert!(!expected, "{name}");
                count += 1;
                continue;
            }
            let credential = credential?;
            let service = Service::new(Limits::default())?;
            service.replace(vec![
                ("user".into(), credential.clone()),
                ("escape,user=ok".into(), credential.clone()),
                ("用户".into(), credential),
            ])?;
            let first = hex(columns[5])?;
            let server = hex(columns[6])?;
            let first_text = std::str::from_utf8(&first)?;
            let client_nonce = first_text.split(',').find_map(|p| p.strip_prefix("r="));
            let server_text = std::str::from_utf8(&server)?;
            let server_nonce = server_text.split(',').find_map(|p| p.strip_prefix("r="));
            let suffix = match (client_nonce, server_nonce) {
                (Some(client), Some(server)) => server
                    .strip_prefix(client)
                    .ok_or("fixture nonce mismatch")?,
                _ => "public-fixture-suffix",
            };
            let mut session = service.scram(algorithm)?;
            match session.challenge_with_nonce(&first, suffix) {
                Ok(challenge) => {
                    assert_eq!(challenge, server, "{name}");
                    let result = session.finish(Secret::new(hex(columns[7])?)).await;
                    assert_eq!(result.is_ok(), expected, "{name}");
                    if let Ok(result) = result {
                        assert_eq!(result.message, hex(columns[8])?, "{name}");
                        let identity = match name {
                            "canonical-escaped" => "escape,user=ok",
                            "canonical-utf8" => "用户",
                            _ => "user",
                        };
                        assert_eq!(result.identity.name(), identity, "{name}");
                    }
                }
                Err(_) => {
                    assert!(!expected, "{name}");
                    // A malformed first message cannot be retried into success.
                    assert!(session
                        .challenge_with_nonce(b"n,,n=user,r=valid", suffix)
                        .is_err());
                    assert!(session.finish(Secret::new(hex(columns[7])?)).await.is_err());
                }
            }
            assert_eq!(
                service.work_counts(),
                WorkCounts {
                    admissions: 0,
                    workers: 0
                }
            );
            count += 1;
        }
        Ok(count)
    }

    #[tokio::test]
    async fn authentic_apache_and_literal_rfc_scram_proofs() -> TestResult {
        assert_eq!(
            replay_scram(include_str!("../../tests/fixtures/sasl/apache-scram.tsv")).await?,
            48
        );
        assert_eq!(
            replay_scram(include_str!(
                "../../tests/fixtures/sasl/rfc-scram-sha256.tsv"
            ))
            .await?,
            1
        );
        assert_eq!(
            replay_scram(include_str!(
                "../../tests/fixtures/sasl/rfc-scram-extensions.tsv"
            ))
            .await?,
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn contended_credential_access_is_bounded_and_releases_admission() -> TestResult {
        let service = Service::new(Limits::default())?;
        let writing = service.0.snapshot.try_write()?;
        assert!(matches!(service.scram(Algorithm::Sha256), Err(Error::Busy)));
        assert!(matches!(
            service.plain(Secret::new(b"\0user\0pencil".to_vec())).await,
            Err(Error::Busy)
        ));
        assert_eq!(
            service.work_counts(),
            WorkCounts {
                admissions: 0,
                workers: 0
            }
        );
        drop(writing);
        let reading = service.0.snapshot.try_read()?;
        assert_eq!(service.replace(Vec::new()), Err(Error::Busy));
        assert_eq!(reading.generation, 0);
        drop(reading);
        assert_eq!(service.replace(Vec::new())?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn authentic_apache_plain_decisions() -> TestResult {
        let service = Service::new(Limits::default())?;
        let credential = service
            .derive(Algorithm::Sha256, Secret::new(b"pencil".to_vec()), 4096)
            .await?;
        let utf8 = service
            .derive(
                Algorithm::Sha256,
                Secret::new("päss💫".as_bytes().to_vec()),
                4096,
            )
            .await?;
        service.replace(vec![
            ("user".into(), credential.clone()),
            ("escape,user=ok".into(), credential),
            ("用户".into(), utf8),
        ])?;
        let mut count = 0;
        for line in include_str!("../../tests/fixtures/sasl/apache-plain.tsv")
            .lines()
            .skip(1)
        {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 4);
            let result = service.plain(Secret::new(hex(columns[1])?)).await;
            assert_eq!(result.is_ok(), columns[2] == "true", "{}", columns[0]);
            if let Ok(identity) = result {
                assert_eq!(
                    identity.name().as_bytes(),
                    hex(columns[3])?,
                    "{}",
                    columns[0]
                );
            }
            count += 1;
        }
        assert_eq!(count, 12);
        Ok(())
    }

    #[tokio::test]
    async fn apache_first_message_parser_scope_and_stricter_policy() -> TestResult {
        let service = Service::new(Limits::default())?;
        let credential = service
            .derive(Algorithm::Sha256, Secret::new(b"pencil".to_vec()), 4096)
            .await?;
        service.replace(vec![
            ("user".into(), credential.clone()),
            ("escape,user=ok".into(), credential),
        ])?;
        let mut count = 0;
        for line in include_str!("../../tests/fixtures/sasl/apache-messages.tsv")
            .lines()
            .skip(1)
        {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 4);
            // Server-first/final are peer/client parsing scope; full server outputs
            // are byte-checked in the transcript suite, not parsed by this server.
            if columns[1] != "client-first" {
                continue;
            }
            let mut session = service.scram(Algorithm::Sha256)?;
            let result = session.challenge_with_nonce(&hex(columns[2])?, "fixture-suffix");
            assert_eq!(
                result.is_ok(),
                columns[3] == "true" && !columns[0].starts_with("hardening-"),
                "{}",
                columns[0]
            );
            count += 1;
        }
        assert_eq!(count, 12);
        Ok(())
    }

    #[test]
    fn constant_time_api_rejects_first_middle_last_and_wrong_length() {
        for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
            let expected = vec![0x55; algorithm.size()];
            assert!(algorithm.equal(&expected, &expected));
            for index in [0, algorithm.size() / 2, algorithm.size() - 1] {
                let mut changed = expected.clone();
                changed[index] ^= 1;
                assert!(!algorithm.equal(&changed, &expected));
                assert!(!algorithm.equal(&expected, &changed));
            }
            assert!(!algorithm.equal(&expected[..expected.len() - 1], &expected));
            assert!(!algorithm.equal(&expected, &[]));
        }
    }

    async fn wait_counts(service: &Service, wanted: WorkCounts) -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), async {
            while service.work_counts() != wanted {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_releases_admission_and_running_worker_remains_bounded() -> TestResult {
        let service = Service::new(Limits {
            admissions: 1,
            workers: 1,
            ..Limits::default()
        })?;
        let owner = service.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let executor = std::thread::current().id();
        let operation = tokio::spawn(async move {
            let _admission = owner.admission()?;
            owner
                .work(move || {
                    let worker = std::thread::current().id();
                    let _ = started_tx.send(worker);
                    release_rx
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(|_| Error::Cancelled)?;
                    Ok(())
                })
                .await
        });
        assert_ne!(started_rx.await?, executor);
        assert_eq!(
            service.work_counts(),
            WorkCounts {
                admissions: 1,
                workers: 1
            }
        );
        assert!(matches!(service.scram(Algorithm::Sha256), Err(Error::Busy)));
        operation.abort();
        assert!(operation.await.is_err());
        assert_eq!(
            service.work_counts(),
            WorkCounts {
                admissions: 0,
                workers: 1
            }
        );
        assert!(matches!(
            service
                .derive(Algorithm::Sha256, Secret::new(b"pencil".to_vec()), 4096)
                .await,
            Err(Error::Busy)
        ));
        assert_eq!(
            service.work_counts(),
            WorkCounts {
                admissions: 0,
                workers: 1
            }
        );
        release_tx.send(())?;
        wait_counts(
            &service,
            WorkCounts {
                admissions: 0,
                workers: 0,
            },
        )
        .await?;
        // Subsequent work succeeds only after the actual worker released its permit.
        service
            .derive(Algorithm::Sha256, Secret::new(b"pencil".to_vec()), 4096)
            .await?;
        Ok(())
    }

    #[test]
    fn cancelled_queued_work_does_not_execute_after_pool_unblocks() -> TestResult {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?;
        runtime.block_on(async {
            let service = Service::new(Limits {
                admissions: 1,
                workers: 1,
                ..Limits::default()
            })?;
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let occupying = tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                release_rx.recv_timeout(Duration::from_secs(5))
            });
            started_rx.await?;
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let call_counter = calls.clone();
            let owner = service.clone();
            let operation = tokio::spawn(async move {
                let _admission = owner.admission()?;
                owner
                    .work(move || {
                        call_counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            });
            wait_counts(
                &service,
                WorkCounts {
                    admissions: 1,
                    workers: 1,
                },
            )
            .await?;
            operation.abort();
            assert!(operation.await.is_err());
            assert_eq!(
                service.work_counts(),
                WorkCounts {
                    admissions: 0,
                    workers: 1
                }
            );
            assert!(matches!(
                service
                    .derive(Algorithm::Sha512, Secret::new(b"pencil".to_vec()), 4096)
                    .await,
                Err(Error::Busy)
            ));
            release_tx.send(())?;
            occupying.await??;
            wait_counts(
                &service,
                WorkCounts {
                    admissions: 0,
                    workers: 0,
                },
            )
            .await?;
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            Ok(())
        })
    }
}
