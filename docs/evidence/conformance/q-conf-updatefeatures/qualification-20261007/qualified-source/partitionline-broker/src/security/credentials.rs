//! Bounded, serialized durable SASL verifier storage.
//!
//! Only salt, iterations, StoredKey and ServerKey enter the journal. Imported
//! SaltedPassword is an owned zeroizing input, converted on the blocking actor.
//! A complete per-user mutation is synchronized before its immutable generation
//! is published. The generation write guard is held only on that blocking actor;
//! new authentication fails with Busy during synchronization instead of waiting.
//! Queued cancelled mutations are skipped. Once the actor starts a mutation it
//! completes despite caller cancellation; a lost acknowledgement is ambiguous.
//!
//! Paths are trusted exclusive configuration. Unix files must be regular, not
//! symlinks and inaccessible to group/other users. This does not provide hostile
//! directory race protection or cross-process locking. Journal CRC detects
//! corruption, not tampering. Storage I/O/corruption poisons authentication until
//! reopening. Non-Unix platforms are rejected until equivalent ACLs are provided.

use super::sasl::{self, Algorithm, Credential, CredentialForm, PublicationError, Secret, Service};
use crate::journal::{self, Journal};
use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, Semaphore};

const MAGIC: &[u8; 8] = b"PLSASL01";
const RECORD_BYTES: usize = 4096;
type Records = HashMap<(String, Algorithm), Credential>;

/// Fixed redacted storage/admin failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Invalid user, verifier, mutation or resource configuration.
    Invalid,
    /// A mutation repeats an algorithm for the same user.
    Duplicate,
    /// A requested deletion has no existing verifier.
    NotFound,
    /// Bounded actor admission or credential publication is busy.
    Busy,
    /// A journal, replay or credential capacity would be exceeded.
    Budget,
    /// Complete journal damage or invalid stored verifier data.
    Corrupt,
    /// Stopped, poisoned or otherwise unavailable storage.
    Unavailable,
    /// Credential file is a symlink, nonregular or accessible to other users.
    InsecureFile,
    /// The platform lacks the enforced credential-file permission policy.
    UnsupportedPlatform,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "credential store: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Positive retained-storage, replay and admitted-request ceilings.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Mechanism limits, including total identity/algorithm entries.
    pub sasl: sasl::Limits,
    /// Queued plus running requests, at most 256.
    pub requests: usize,
    /// Retained journal mutation entries, at most one million.
    pub operations: usize,
    /// Journal bytes including headers, 128 bytes through one GiB.
    pub journal_bytes: u64,
    /// Total replayed payload bytes, 4096 through 128 MiB.
    pub replay_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            sasl: sasl::Limits::default(),
            requests: 32,
            operations: 4096,
            journal_bytes: 16 * 1024 * 1024,
            replay_bytes: 16 * 1024 * 1024,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<journal::Limits, Error> {
        Service::new(self.sasl).map_err(|_| Error::Invalid)?;
        if !(1..=256).contains(&self.requests)
            || !(1..=1_000_000).contains(&self.operations)
            || !(128..=1 << 30).contains(&self.journal_bytes)
            || !(RECORD_BYTES..=128 * 1024 * 1024).contains(&self.replay_bytes)
        {
            return Err(Error::Invalid);
        }
        journal::Limits::new(
            RECORD_BYTES,
            self.journal_bytes,
            self.operations,
            RECORD_BYTES + std::mem::size_of::<journal::Entry>(),
        )
        .map_err(|_| Error::Invalid)
    }
}

/// One algorithm change in an atomic per-user mutation; Debug is redacted.
pub enum Change {
    /// Import Kafka AlterUserScramCredentials' transient SaltedPassword.
    Upsert {
        /// Supported SCRAM hash.
        algorithm: Algorithm,
        /// Public salt, at least 16 bytes and no more than the configured bound.
        salt: Vec<u8>,
        /// PBKDF2 iteration count, 4096 through the configured bound.
        iterations: u32,
        /// Exactly one hash output; zeroized after verifier derivation.
        salted_password: Secret,
    },
    /// Delete an existing algorithm for this user.
    Delete(Algorithm),
}
impl fmt::Debug for Change {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Change { [REDACTED] }")
    }
}
impl Change {
    fn algorithm(&self) -> Algorithm {
        match self {
            Self::Upsert { algorithm, .. } | Self::Delete(algorithm) => *algorithm,
        }
    }
}
/// Describe output containing no keys, salt or password material.
pub struct CredentialInfo {
    user: String,
    algorithm: Algorithm,
    iterations: u32,
}
impl CredentialInfo {
    /// Principal that owns this verifier.
    pub fn user(&self) -> &str {
        &self.user
    }
    /// SCRAM hash algorithm.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }
    /// Configured PBKDF2 iterations.
    pub fn iterations(&self) -> u32 {
        self.iterations
    }
}
impl fmt::Debug for CredentialInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialInfo { [REDACTED] }")
    }
}
struct Status {
    healthy: AtomicBool,
    stopping: AtomicBool,
}
struct Shared {
    tx: mpsc::Sender<Command>,
    admissions: Arc<Semaphore>,
    status: Arc<Status>,
    service: Service,
    limits: Limits,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
/// Cloneable bounded store; the mutable mechanism service never escapes it.
#[derive(Clone)]
pub struct Store(Arc<Shared>);
impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Store { [REDACTED] }")
    }
}
struct Cancellation(Arc<AtomicBool>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct Request<T> {
    value: T,
    cancelled: Arc<AtomicBool>,
    _permit: OwnedSemaphorePermit,
}
type Reply<T> = oneshot::Sender<Result<T, Error>>;
type Mutation = (String, Vec<Change>, Reply<u64>);
type Query = (Option<Vec<String>>, Reply<Vec<CredentialInfo>>);
enum Command {
    Mutate(Request<Mutation>),
    Describe(Request<Query>),
    Wake,
}
struct ActorExit {
    status: Arc<Status>,
    service: Service,
}
impl Drop for ActorExit {
    fn drop(&mut self) {
        self.status.healthy.store(false, Ordering::Release);
        self.service.disable();
    }
}
impl Store {
    /// Open/recover on a dedicated bounded blocking actor. A repaired incomplete
    /// journal tail is reported; complete corruption never produces a store.
    pub async fn open(
        path: impl AsRef<Path>,
        limits: Limits,
    ) -> Result<(Self, journal::Recovery), Error> {
        Self::open_inner(
            path.as_ref().to_path_buf(),
            limits,
            #[cfg(test)]
            None,
        )
        .await
    }
    async fn open_inner(
        path: PathBuf,
        limits: Limits,
        #[cfg(test)] hook: Option<CommitHook>,
    ) -> Result<(Self, journal::Recovery), Error> {
        let journal_limits = limits.validate()?;
        let service = Service::new(limits.sasl).map_err(|_| Error::Invalid)?;
        let status = Arc::new(Status {
            healthy: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
        });
        let (tx, rx) = mpsc::channel(limits.requests);
        let (ready_tx, ready_rx) = oneshot::channel();
        let actor_service = service.clone();
        let actor_status = status.clone();
        let join = tokio::task::spawn_blocking(move || {
            let _exit = ActorExit {
                status: actor_status.clone(),
                service: actor_service.clone(),
            };
            match Actor::open(
                path,
                limits,
                journal_limits,
                actor_service,
                actor_status,
                #[cfg(test)]
                hook,
            ) {
                Ok((mut actor, recovery)) => {
                    actor.status.healthy.store(true, Ordering::Release);
                    if ready_tx.send(Ok(recovery)).is_ok() {
                        actor.run(rx);
                    }
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
            }
        });
        let recovery = ready_rx.await.map_err(|_| Error::Unavailable)??;
        Ok((
            Self(Arc::new(Shared {
                tx,
                admissions: Arc::new(Semaphore::new(limits.requests)),
                status,
                service,
                limits,
                join: Mutex::new(Some(join)),
            })),
            recovery,
        ))
    }
    /// Whether new operations can be admitted. In-flight poisoned operations
    /// must also check this before dispatching authenticated application work.
    pub fn is_healthy(&self) -> bool {
        self.0.status.healthy.load(Ordering::Acquire)
            && !self.0.status.stopping.load(Ordering::Acquire)
    }
    fn admission(&self) -> Result<OwnedSemaphorePermit, Error> {
        if !self.is_healthy() {
            return Err(Error::Unavailable);
        }
        self.0
            .admissions
            .clone()
            .try_acquire_owned()
            .map_err(|e| match e {
                tokio::sync::TryAcquireError::Closed => Error::Unavailable,
                tokio::sync::TryAcquireError::NoPermits => Error::Busy,
            })
    }
    /// Commit all changes for one user atomically. A started mutation finishes
    /// after caller cancellation; reopening resolves a lost acknowledgement.
    pub async fn mutate(&self, user: String, changes: Vec<Change>) -> Result<u64, Error> {
        validate_changes(&user, &changes, self.0.limits)?;
        let permit = self.admission()?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancellation = Cancellation(cancelled.clone());
        let (reply, response) = oneshot::channel();
        let command = Command::Mutate(Request {
            value: (user, changes, reply),
            cancelled,
            _permit: permit,
        });
        self.0.tx.try_send(command).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Busy,
            mpsc::error::TrySendError::Closed(_) => Error::Unavailable,
        })?;
        response.await.map_err(|_| Error::Unavailable)?
    }
    /// Describe all users (`None`) or a bounded list. Empty results contain no
    /// unknown-user placeholder; wire handlers supply per-user error metadata.
    pub async fn describe(&self, users: Option<Vec<String>>) -> Result<Vec<CredentialInfo>, Error> {
        if let Some(users) = &users {
            if users.len() > self.0.limits.sasl.credentials
                || users.iter().any(|u| !self.0.limits.sasl.identity(u))
            {
                return Err(Error::Invalid);
            }
        }
        let permit = self.admission()?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancellation = Cancellation(cancelled.clone());
        let (reply, response) = oneshot::channel();
        self.0
            .tx
            .try_send(Command::Describe(Request {
                value: (users, reply),
                cancelled,
                _permit: permit,
            }))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => Error::Unavailable,
            })?;
        response.await.map_err(|_| Error::Unavailable)?
    }
    /// Authenticate against this store without exposing a mutable service.
    pub async fn plain(&self, message: Secret) -> Result<sasl::Identity, sasl::Error> {
        if !self.is_healthy() {
            return Err(sasl::Error::Unavailable);
        }
        let identity = self.0.service.plain(message).await?;
        if !self.is_healthy() {
            return Err(sasl::Error::Unavailable);
        }
        Ok(identity)
    }
    /// Capture one immutable generation for a SCRAM exchange.
    pub fn scram(&self, algorithm: Algorithm) -> Result<sasl::ScramSession, sasl::Error> {
        if !self.is_healthy() {
            return Err(sasl::Error::Unavailable);
        }
        self.0.service.scram(algorithm)
    }
    /// Capture one generation for a negotiated PLAIN exchange. Callers must
    /// check store health again before accepting a completed identity.
    pub fn begin_plain(&self) -> Result<sasl::PlainSession, sasl::Error> {
        if !self.is_healthy() {
            return Err(sasl::Error::Unavailable);
        }
        self.0.service.begin_plain()
    }
    /// Validated mechanism ceilings used by listener parsing before copying.
    pub fn mechanism_limits(&self) -> sasl::Limits {
        self.0.limits.sasl
    }
    /// Stop admissions, finish any started mutation and discard queued work.
    /// The join handle remains available if this shutdown future is cancelled.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.0.status.stopping.store(true, Ordering::Release);
        self.0.admissions.close();
        let _ = self.0.tx.try_send(Command::Wake);
        let mut join = self.0.join.lock().await;
        if let Some(handle) = join.as_mut() {
            handle.await.map_err(|_| Error::Unavailable)?;
        }
        *join = None;
        Ok(())
    }
}
fn validate_changes(user: &str, changes: &[Change], limits: Limits) -> Result<(), Error> {
    if !limits.sasl.identity(user) || !(1..=2).contains(&changes.len()) {
        return Err(Error::Invalid);
    }
    if changes.len() == 2 && changes[0].algorithm() == changes[1].algorithm() {
        return Err(Error::Duplicate);
    }
    for change in changes {
        if let Change::Upsert {
            algorithm,
            salt,
            iterations,
            salted_password,
        } = change
        {
            if !(16..=limits.sasl.salt_bytes).contains(&salt.len())
                || !(4096..=limits.sasl.iterations).contains(iterations)
                || salted_password.len() != algorithm.size()
            {
                return Err(Error::Invalid);
            }
        }
    }
    Ok(())
}
#[cfg(test)]
type CommitHook =
    Box<dyn FnMut(&mut Journal, &[u8]) -> Result<journal::Append, journal::Error> + Send>;
struct Actor {
    journal: Journal,
    records: Records,
    replay_bytes: usize,
    limits: Limits,
    service: Service,
    status: Arc<Status>,
    #[cfg(test)]
    commit: Option<CommitHook>,
}
impl Actor {
    fn open(
        path: PathBuf,
        limits: Limits,
        journal_limits: journal::Limits,
        service: Service,
        status: Arc<Status>,
        #[cfg(test)] hook: Option<CommitHook>,
    ) -> Result<(Self, journal::Recovery), Error> {
        secure_file(&path)?;
        let (mut journal, recovery) =
            Journal::open(path, 0, journal_limits).map_err(storage_error)?;
        let mut records = Records::new();
        let mut replay_bytes = 0usize;
        for offset in 0..recovery.next_offset {
            let entries = journal
                .fetch(
                    offset,
                    1,
                    RECORD_BYTES + std::mem::size_of::<journal::Entry>(),
                )
                .map_err(storage_error)?;
            let entry = entries.into_iter().next().ok_or(Error::Corrupt)?;
            if entry.record_count != 1 || entry.first_offset != offset {
                return Err(Error::Corrupt);
            }
            replay_bytes = replay_bytes
                .checked_add(entry.payload.len())
                .filter(|n| *n <= limits.replay_bytes)
                .ok_or(Error::Budget)?;
            decode_apply(&entry.payload, &mut records, limits)?;
        }
        service
            .replace(export(&records))
            .map_err(|_| Error::Corrupt)?;
        Ok((
            Self {
                journal,
                records,
                replay_bytes,
                limits,
                service,
                status,
                #[cfg(test)]
                commit: hook,
            },
            recovery,
        ))
    }
    fn run(&mut self, mut rx: mpsc::Receiver<Command>) {
        while let Some(command) = rx.blocking_recv() {
            if self.status.stopping.load(Ordering::Acquire) {
                break;
            }
            match command {
                Command::Wake => {}
                Command::Mutate(request) => {
                    if request.cancelled.load(Ordering::Acquire) {
                        continue;
                    }
                    let (user, changes, reply) = request.value;
                    let result = if self.status.healthy.load(Ordering::Acquire) {
                        self.mutate(user, changes)
                    } else {
                        Err(Error::Unavailable)
                    };
                    let _ = reply.send(result);
                }
                Command::Describe(request) => {
                    if request.cancelled.load(Ordering::Acquire) {
                        continue;
                    }
                    let (users, reply) = request.value;
                    let result = if self.status.healthy.load(Ordering::Acquire) {
                        let mut rows: Vec<_> = self
                            .records
                            .iter()
                            .filter(|((user, _), _)| {
                                users.as_ref().is_none_or(|users| users.contains(user))
                            })
                            .map(|((user, algorithm), credential)| CredentialInfo {
                                user: user.clone(),
                                algorithm: *algorithm,
                                iterations: credential.iterations(),
                            })
                            .collect();
                        rows.sort_by(|a, b| {
                            a.user
                                .cmp(&b.user)
                                .then_with(|| code(a.algorithm).cmp(&code(b.algorithm)))
                        });
                        Ok(rows)
                    } else {
                        Err(Error::Unavailable)
                    };
                    let _ = reply.send(result);
                }
            }
        }
    }
    fn mutate(&mut self, user: String, changes: Vec<Change>) -> Result<u64, Error> {
        let mut next = self.records.clone();
        let mut stored = Vec::with_capacity(changes.len());
        for change in changes {
            let algorithm = change.algorithm();
            match change {
                Change::Delete(_) => {
                    if next.remove(&(user.clone(), algorithm)).is_none() {
                        return Err(Error::NotFound);
                    }
                    stored.push((algorithm, None));
                }
                Change::Upsert {
                    salt,
                    iterations,
                    salted_password,
                    ..
                } => {
                    let credential = Credential::from_salted_password(
                        algorithm,
                        salt,
                        iterations,
                        salted_password,
                        self.limits.sasl,
                    )
                    .map_err(|_| Error::Invalid)?;
                    stored.push((algorithm, Some(credential.form())));
                    next.insert((user.clone(), algorithm), credential);
                }
            }
        }
        if next.len() > self.limits.sasl.credentials {
            return Err(Error::Budget);
        }
        let payload = encode(&user, stored);
        let replay_bytes = self
            .replay_bytes
            .checked_add(payload.len())
            .filter(|n| *n <= self.limits.replay_bytes)
            .ok_or(Error::Budget)?;
        let result = self.service.publish_after(export(&next), || {
            #[cfg(test)]
            if let Some(commit) = self.commit.as_mut() {
                return commit(&mut self.journal, &payload).map(|_| ());
            }
            self.journal.append(1, &payload).map(|_| ())
        });
        match result {
            Ok(generation) => {
                self.records = next;
                self.replay_bytes = replay_bytes;
                Ok(generation)
            }
            Err(PublicationError::Validation(sasl::Error::Busy)) => Err(Error::Busy),
            Err(PublicationError::Validation(_)) => {
                self.poison();
                Err(Error::Unavailable)
            }
            Err(PublicationError::Commit(error)) => {
                let result = storage_error(error);
                if result != Error::Budget {
                    self.poison();
                }
                Err(result)
            }
        }
    }
    fn poison(&self) {
        self.status.healthy.store(false, Ordering::Release);
        self.service.disable();
    }
}
fn export(records: &Records) -> Vec<(String, Credential)> {
    records
        .iter()
        .map(|((user, _), credential)| (user.clone(), credential.clone()))
        .collect()
}
fn storage_error(error: journal::Error) -> Error {
    match error {
        journal::Error::Corrupt { .. } | journal::Error::BaseOffsetMismatch => Error::Corrupt,
        journal::Error::FileBudgetExceeded
        | journal::Error::IndexBudgetExceeded
        | journal::Error::EntryTooLarge
        | journal::Error::FetchBudgetExceeded => Error::Budget,
        _ => Error::Unavailable,
    }
}
fn code(algorithm: Algorithm) -> u8 {
    match algorithm {
        Algorithm::Sha256 => 1,
        Algorithm::Sha512 => 2,
    }
}
fn algorithm(code: u8) -> Result<Algorithm, Error> {
    match code {
        1 => Ok(Algorithm::Sha256),
        2 => Ok(Algorithm::Sha512),
        _ => Err(Error::Corrupt),
    }
}
fn encode(user: &str, changes: Vec<(Algorithm, Option<CredentialForm>)>) -> Vec<u8> {
    let mut output = Vec::with_capacity(RECORD_BYTES);
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&(user.len() as u16).to_be_bytes());
    output.extend_from_slice(user.as_bytes());
    output.push(changes.len() as u8);
    for (algorithm, form) in changes {
        output.push(code(algorithm));
        output.push(u8::from(form.is_some()));
        if let Some(form) = form {
            output.extend_from_slice(&(form.salt.len() as u16).to_be_bytes());
            output.extend_from_slice(&form.salt);
            output.extend_from_slice(&form.iterations.to_be_bytes());
            output.extend_from_slice(&form.stored_key);
            output.extend_from_slice(&form.server_key);
        }
    }
    output
}
struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}
impl<'a> Decoder<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let end = self
            .cursor
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(Error::Corrupt)?;
        let result = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(result)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<usize, Error> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]) as usize)
    }
}
fn decode_apply(payload: &[u8], records: &mut Records, limits: Limits) -> Result<(), Error> {
    let mut decoder = Decoder {
        bytes: payload,
        cursor: 0,
    };
    if decoder.take(8)? != MAGIC {
        return Err(Error::Corrupt);
    }
    let length = decoder.u16()?;
    if !(1..=limits.sasl.identity_bytes).contains(&length) {
        return Err(Error::Corrupt);
    }
    let user = std::str::from_utf8(decoder.take(length)?)
        .map_err(|_| Error::Corrupt)?
        .to_owned();
    if !limits.sasl.identity(&user) {
        return Err(Error::Corrupt);
    }
    let count = decoder.byte()?;
    if !(1..=2).contains(&count) {
        return Err(Error::Corrupt);
    }
    let mut previous = None;
    for _ in 0..count {
        let algorithm = algorithm(decoder.byte()?)?;
        if previous == Some(algorithm) {
            return Err(Error::Corrupt);
        }
        previous = Some(algorithm);
        match decoder.byte()? {
            0 => {
                if records.remove(&(user.clone(), algorithm)).is_none() {
                    return Err(Error::Corrupt);
                }
            }
            1 => {
                let length = decoder.u16()?;
                if !(16..=limits.sasl.salt_bytes).contains(&length) {
                    return Err(Error::Corrupt);
                }
                let salt = decoder.take(length)?.to_vec();
                let bytes = decoder.take(4)?;
                let iterations = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                let stored_key = decoder.take(algorithm.size())?.to_vec();
                let server_key = decoder.take(algorithm.size())?.to_vec();
                let credential = Credential::from_form(
                    CredentialForm {
                        algorithm,
                        salt,
                        iterations,
                        stored_key,
                        server_key,
                    },
                    limits.sasl,
                )
                .map_err(|_| Error::Corrupt)?;
                records.insert((user.clone(), algorithm), credential);
            }
            _ => return Err(Error::Corrupt),
        }
    }
    if decoder.cursor != payload.len() || records.len() > limits.sasl.credentials {
        return Err(Error::Corrupt);
    }
    Ok(())
}
#[cfg(unix)]
fn secure_file(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o077 != 0 {
                return Err(Error::InsecureFile);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .map_err(|_| Error::Unavailable)?;
        }
        Err(_) => return Err(Error::Unavailable),
    }
    Ok(())
}
#[cfg(not(unix))]
fn secure_file(_: &Path) -> Result<(), Error> {
    Err(Error::UnsupportedPlatform)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use pbkdf2::pbkdf2_hmac;
    use sha2::Sha256;
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct TestPath(PathBuf);
    impl TestPath {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "partitionline-credential-fault-{}-{}",
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
    fn change(password: &str) -> Change {
        let salt = b"public-actor-fixture-salt".to_vec();
        let mut salted = vec![0; 32];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, 4096, &mut salted);
        Change::Upsert {
            algorithm: Algorithm::Sha256,
            salt,
            iterations: 4096,
            salted_password: Secret::new(salted),
        }
    }
    fn message(password: &str) -> Secret {
        Secret::new(format!("\0user\0{password}").into_bytes())
    }
    async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(10), future)
            .await
            .expect("bounded test deadline")
    }
    async fn wait_admissions(store: &Store, expected: usize) {
        bounded(async {
            while store.0.admissions.available_permits() != store.0.limits.requests - expected {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_started_commit_survives_cancellation_queued_cancel_skips_and_auth_is_busy() {
        let path = TestPath::new();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut entered_tx = Some(entered_tx);
        let mut count = 0;
        let hook: CommitHook = Box::new(move |journal, payload| {
            count += 1;
            if count == 2 {
                entered_tx.take().unwrap().send(()).unwrap();
                release_rx.recv().unwrap();
            }
            journal.append(1, payload)
        });
        let (store, _) = Store::open_inner(
            path.0.clone(),
            Limits {
                requests: 2,
                ..Limits::default()
            },
            Some(hook),
        )
        .await
        .unwrap();
        let old_generation = store
            .mutate("user".into(), vec![change("old")])
            .await
            .unwrap();
        let mut captured = store.scram(Algorithm::Sha256).unwrap();
        let first = captured
            .challenge(b"n,,n=user,r=public-client-nonce")
            .unwrap();
        assert!(std::str::from_utf8(&first).unwrap().contains("i=4096"));
        let started = tokio::spawn({
            let store = store.clone();
            async move { store.mutate("user".into(), vec![change("new")]).await }
        });
        bounded(entered_rx).await.unwrap();
        assert_eq!(
            store.plain(message("old")).await.err(),
            Some(sasl::Error::Busy)
        );
        assert_eq!(
            store.scram(Algorithm::Sha256).err(),
            Some(sasl::Error::Busy)
        );
        let queued = tokio::spawn({
            let store = store.clone();
            async move { store.mutate("queued".into(), vec![change("never")]).await }
        });
        wait_admissions(&store, 2).await;
        assert_eq!(store.describe(None).await.err(), Some(Error::Busy));
        queued.abort();
        bounded(queued).await.unwrap_err();
        started.abort();
        bounded(started).await.unwrap_err();
        // Cancellation keeps bounded storage ownership until the actual actor
        // processes/discards each request, rather than freeing running permits.
        assert_eq!(store.0.admissions.available_permits(), 0);
        release_tx.send(()).unwrap();
        wait_admissions(&store, 0).await;
        let identity = store.plain(message("new")).await.unwrap();
        assert_eq!(identity.generation(), old_generation + 1);
        assert_eq!(
            store.plain(message("old")).await.err(),
            Some(sasl::Error::AuthenticationFailed)
        );
        assert_eq!(store.describe(None).await.unwrap().len(), 1);
        drop(captured);
        store.shutdown().await.unwrap();
        let (store, recovery) = Store::open(&path.0, Limits::default()).await.unwrap();
        assert_eq!(recovery.recovered_entries, 2);
        store.plain(message("new")).await.unwrap();
        assert!(store
            .describe(Some(vec!["queued".into()]))
            .await
            .unwrap()
            .is_empty());
        store.shutdown().await.unwrap();
    }
    async fn injected_failure(after_append: bool) {
        let path = TestPath::new();
        let hook: CommitHook = Box::new(move |journal, payload| {
            if after_append {
                journal.append(1, payload)?;
            }
            Err(journal::Error::Io(std::io::Error::other(
                "injected ambiguous storage failure",
            )))
        });
        let (store, _) = Store::open_inner(path.0.clone(), Limits::default(), Some(hook))
            .await
            .unwrap();
        assert_eq!(
            store.mutate("user".into(), vec![change("pencil")]).await,
            Err(Error::Unavailable)
        );
        assert!(!store.is_healthy());
        assert_eq!(
            store.plain(message("pencil")).await.err(),
            Some(sasl::Error::Unavailable)
        );
        assert_eq!(
            store.scram(Algorithm::Sha256).err(),
            Some(sasl::Error::Unavailable)
        );
        assert_eq!(store.describe(None).await.err(), Some(Error::Unavailable));
        store.shutdown().await.unwrap();
        let (store, recovery) = Store::open(&path.0, Limits::default()).await.unwrap();
        assert_eq!(recovery.recovered_entries, usize::from(after_append));
        if after_append {
            store.plain(message("pencil")).await.unwrap();
        } else {
            assert_eq!(
                store.plain(message("pencil")).await.err(),
                Some(sasl::Error::AuthenticationFailed)
            );
        }
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn failed_storage_before_append_has_no_ack_or_published_generation() {
        injected_failure(false).await;
    }
    #[tokio::test]
    async fn ambiguous_failure_after_complete_append_poison_disables_auth_until_recovery() {
        injected_failure(true).await;
    }
    #[test]
    fn corrupt_payload_and_duplicate_changes_cannot_become_a_generation() {
        let limits = Limits::default();
        let service = Service::new(limits.sasl).unwrap();
        let credential = Credential::from_salted_password(
            Algorithm::Sha256,
            vec![7; 16],
            4096,
            Secret::new(vec![9; 32]),
            limits.sasl,
        )
        .unwrap();
        let valid = encode("user", vec![(Algorithm::Sha256, Some(credential.form()))]);
        for length in 0..valid.len() {
            assert!(decode_apply(&valid[..length], &mut Records::new(), limits).is_err());
        }
        let mut trailing = valid.clone();
        trailing.push(0);
        assert_eq!(
            decode_apply(&trailing, &mut Records::new(), limits),
            Err(Error::Corrupt)
        );
        let duplicate = encode(
            "user",
            vec![
                (Algorithm::Sha256, Some(credential.form())),
                (Algorithm::Sha256, Some(credential.form())),
            ],
        );
        assert_eq!(
            decode_apply(&duplicate, &mut Records::new(), limits),
            Err(Error::Corrupt)
        );
        assert_eq!(
            service
                .scram(Algorithm::Sha256)
                .unwrap()
                .challenge(b"n,,n=user,r=abc")
                .err(),
            Some(sasl::Error::AuthenticationFailed)
        );
    }
}
