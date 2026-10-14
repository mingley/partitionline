//! Durable KRaft broker registration, heartbeats, and epoch fencing.
//!
//! The typed session follows the stable BrokerRegistration v0–v4 and
//! BrokerHeartbeat v0–v1 fields from the pinned 4.1.2/4.2.1 schemas:
//! broker id, cluster id, incarnation, listeners, features, rack, the v1
//! ZooKeeper-migration flag, v2 log directories, and the v3 previous broker
//! epoch. Heartbeat v1 records `LastTransactionTimestampMs`. Version 4 of
//! registration is admitted with that same field set; the 4.3.1 schema hash
//! differs and heartbeat v2 is rejected. This module does not speak the
//! flexible controller RPC and does not qualify a production cluster.
//!
//! Each confirmed registration, fence, and heartbeat is one checksummed
//! journal record, published only after `sync_data`. A failed append poisons
//! the handle. Restart replays only complete records. An expired or fenced
//! epoch cannot authorize a write or a controller mutation, and a stale
//! heartbeat does not refresh or unfence the live session.

use crate::journal::{self, Journal};
use std::collections::BTreeMap;
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLBREG01";
const MAX_TEXT: usize = 249;
const MAX_LISTENERS: usize = 16;
const MAX_FEATURES: usize = 32;
const MAX_LOG_DIRS: usize = 16;

/// Kafka `STALE_BROKER_EPOCH`.
pub const STALE_BROKER_EPOCH: i16 = 77;
/// Kafka `DUPLICATE_BROKER_REGISTRATION`.
pub const DUPLICATE_BROKER_REGISTRATION: i16 = 101;
/// Kafka `BROKER_ID_NOT_REGISTERED`.
pub const BROKER_ID_NOT_REGISTERED: i16 = 102;
/// Kafka `INCONSISTENT_CLUSTER_ID`.
pub const INCONSISTENT_CLUSTER_ID: i16 = 104;
/// Kafka `INVALID_REGISTRATION`.
pub const INVALID_REGISTRATION: i16 = 119;

/// One listener advertised by a registering broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listener {
    /// Nonempty listener name.
    pub name: String,
    /// Nonempty host. No URI or port suffix is accepted.
    pub host: String,
    /// Nonzero port.
    pub port: u16,
    /// Kafka `SecurityProtocol` code: 0 plaintext, 1 SSL, 2 SASL_PLAINTEXT, 3 SASL_SSL.
    pub security_protocol: i16,
}

/// One supported finalized feature range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Feature {
    /// Nonempty feature name.
    pub name: String,
    /// Inclusive minimum supported version.
    pub min_version: i16,
    /// Inclusive maximum supported version.
    pub max_version: i16,
}

/// BrokerRegistration fields for one admitted version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    /// Request version, 0 through 4.
    pub version: i16,
    /// Nonnegative broker id.
    pub broker_id: i32,
    /// New process incarnation. The all-zero id is rejected.
    pub incarnation_id: [u8; 16],
    /// At least one listener, with unique names.
    pub listeners: Vec<Listener>,
    /// Finalized feature ranges. Empty is allowed.
    pub features: Vec<Feature>,
    /// Optional rack. Empty is rejected; absence is `None`.
    pub rack: Option<String>,
    /// v1 `IsMigratingZkBroker`. Must be false below v1.
    pub migrating_zk: bool,
    /// v2 log directory ids. Must be empty below v2.
    pub log_dirs: Vec<[u8; 16]>,
    /// v3 `PreviousBrokerEpoch`, or -1 when the broker claims none.
    /// Versions below 3 can only send -1.
    pub previous_broker_epoch: i64,
    /// Caller clock in milliseconds, used as the first contact time.
    pub now_ms: u64,
}

/// BrokerHeartbeat fields for version 0 or 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    /// Heartbeat version, 0 or 1.
    pub version: i16,
    /// Registered broker id.
    pub broker_id: i32,
    /// Epoch returned by the registration this process still holds.
    pub broker_epoch: i64,
    /// Broker's current metadata log offset. Recorded, not used as catch-up proof.
    pub metadata_offset: i64,
    /// Ask the controller to fence this registration.
    pub want_fence: bool,
    /// Ask the controller to shut this broker down.
    pub want_shutdown: bool,
    /// v1 `LastTransactionTimestampMs`. Versions below 1 can only send -1.
    pub last_transaction_timestamp_ms: i64,
    /// Caller clock in milliseconds.
    pub now_ms: u64,
}

/// Confirmed registration epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registered {
    /// Epoch that must be presented by later heartbeats and writes.
    pub broker_epoch: i64,
}

/// Heartbeat outcome. `is_caught_up` stays false: this registry has no metadata
/// high-watermark quorum to compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatResult {
    /// Kafka error code. Zero means the epoch was recognized.
    pub error_code: i16,
    /// The named epoch is not an active writer.
    pub is_fenced: bool,
    /// Shutdown was requested and retained for this epoch.
    pub should_shut_down: bool,
    /// Always false until a later card owns metadata catch-up.
    pub is_caught_up: bool,
}

/// Registration bounds and the journal budget that stores them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_brokers: usize,
    max_records: usize,
    session_timeout_ms: u64,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a positive broker cap up to 10,000, a record cap up to one
    /// million, and a positive session timeout.
    ///
    /// The journal must accept one maximum registration record and must be
    /// able to index `max_records` entries.
    pub fn new(
        max_brokers: usize,
        max_records: usize,
        session_timeout_ms: u64,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=10_000).contains(&max_brokers)
            || !(1..=1_000_000).contains(&max_records)
            || session_timeout_ms == 0
            || journal.max_entry_bytes() < max_record_bytes()
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_brokers,
            max_records,
            session_timeout_ms,
            journal,
        })
    }

    /// Brokers retained at once.
    pub fn max_brokers(self) -> usize {
        self.max_brokers
    }
    /// Confirmed journal records, including heartbeats and fences.
    pub fn max_records(self) -> usize {
        self.max_records
    }
    /// Silence after which an active broker is fenced.
    pub fn session_timeout_ms(self) -> u64 {
        self.session_timeout_ms
    }
}

/// Why a registration call or authorization failed.
#[derive(Debug)]
pub enum Error {
    /// The version cannot represent the supplied fields, or it is outside 0–4 / 0–1.
    UnsupportedVersion,
    /// The broker id, incarnation, listeners, or epoch claim is not usable.
    InvalidRegistration,
    /// The cluster id does not match the registry.
    InconsistentCluster,
    /// This broker id already has a live registration with another incarnation.
    DuplicateRegistration,
    /// The previous or presented epoch is not the current one.
    StaleEpoch,
    /// The broker id has no confirmed registration.
    NotRegistered,
    /// The epoch is fenced or its session timed out.
    Fenced,
    /// Another confirmed record would exceed the configured budget.
    Budget,
    /// A failed append left this handle unable to confirm further mutations.
    Poisoned,
    /// The supplied limits cannot store a maximum registration.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a broker-visible refusal. Storage and budget failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::UnsupportedVersion | Self::InvalidRegistration | Self::InvalidLimits => {
                INVALID_REGISTRATION
            }
            Self::InconsistentCluster => INCONSISTENT_CLUSTER_ID,
            Self::DuplicateRegistration => DUPLICATE_BROKER_REGISTRATION,
            Self::StaleEpoch => STALE_BROKER_EPOCH,
            Self::NotRegistered => BROKER_ID_NOT_REGISTERED,
            Self::Fenced => STALE_BROKER_EPOCH,
            Self::Budget | Self::Poisoned | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Session {
    incarnation_id: [u8; 16],
    epoch: i64,
    listeners: Vec<Listener>,
    features: Vec<Feature>,
    rack: Option<String>,
    migrating_zk: bool,
    log_dirs: Vec<[u8; 16]>,
    fenced: bool,
    shutdown: bool,
    last_heartbeat_ms: u64,
    metadata_offset: i64,
    last_transaction_timestamp_ms: i64,
}

/// Durable registration registry for one cluster id.
pub struct Registry {
    cluster_id: String,
    limits: Limits,
    sessions: BTreeMap<i32, Session>,
    journal: Journal,
    records: usize,
    poisoned: bool,
}

impl Registry {
    /// Open or recover the registration journal for `cluster_id`.
    pub fn open(
        path: impl AsRef<Path>,
        cluster_id: impl Into<String>,
        limits: Limits,
    ) -> Result<Self, Error> {
        let cluster_id = cluster_id.into();
        validate_cluster_id(&cluster_id)?;
        let (mut journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut sessions = BTreeMap::new();
        let mut records = 0usize;
        let mut offset = journal.base_offset();
        loop {
            let batch = journal.fetch(offset, 64, limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRegistration);
                }
                let (id, session) = decode_session(&entry.payload, &cluster_id)?;
                sessions.insert(id, session);
                records = records.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(u64::from(entry.record_count))
                    .ok_or(Error::InvalidRegistration)?;
            }
        }
        if sessions.len() > limits.max_brokers || records > limits.max_records {
            return Err(Error::Budget);
        }
        Ok(Self {
            cluster_id,
            limits,
            sessions,
            journal,
            records,
            poisoned: false,
        })
    }

    /// Register or replace one broker and return the epoch writers must present.
    pub fn register(&mut self, request: &Registration) -> Result<Registered, Error> {
        self.ready()?;
        validate_registration(request)?;
        if let Some(existing) = self.sessions.get(&request.broker_id) {
            if request.previous_broker_epoch == -1 {
                if existing.incarnation_id == request.incarnation_id
                    && !existing.fenced
                    && same_contact(existing, request)
                {
                    return Ok(Registered {
                        broker_epoch: existing.epoch,
                    });
                }
                if !existing.fenced {
                    return Err(Error::DuplicateRegistration);
                }
            } else if request.previous_broker_epoch != existing.epoch {
                return Err(Error::StaleEpoch);
            } else if existing.incarnation_id == request.incarnation_id
                && !existing.fenced
                && same_contact(existing, request)
            {
                return Ok(Registered {
                    broker_epoch: existing.epoch,
                });
            }
        } else if request.previous_broker_epoch != -1 {
            return Err(Error::StaleEpoch);
        }
        if self.sessions.len() >= self.limits.max_brokers
            && !self.sessions.contains_key(&request.broker_id)
        {
            return Err(Error::Budget);
        }
        let epoch = match self.sessions.get(&request.broker_id) {
            Some(existing) => existing.epoch.checked_add(1).ok_or(Error::Budget)?,
            None => 0,
        };
        let session = Session {
            incarnation_id: request.incarnation_id,
            epoch,
            listeners: request.listeners.clone(),
            features: request.features.clone(),
            rack: request.rack.clone(),
            migrating_zk: request.migrating_zk,
            log_dirs: request.log_dirs.clone(),
            fenced: false,
            shutdown: false,
            last_heartbeat_ms: request.now_ms,
            metadata_offset: 0,
            last_transaction_timestamp_ms: -1,
        };
        self.persist(request.broker_id, &session)?;
        Ok(Registered {
            broker_epoch: epoch,
        })
    }

    /// Record a heartbeat. A stale epoch is refused and does not change the live session.
    ///
    /// An older clock does not move the session backward, so a late heartbeat
    /// cannot make a fresh broker look silent.
    pub fn heartbeat(&mut self, request: &Heartbeat) -> Result<HeartbeatResult, Error> {
        self.ready()?;
        if !(0..=1).contains(&request.version) {
            return Err(Error::UnsupportedVersion);
        }
        if request.version < 1 && request.last_transaction_timestamp_ms != -1 {
            return Err(Error::UnsupportedVersion);
        }
        let Some(existing) = self.sessions.get(&request.broker_id).cloned() else {
            return Ok(HeartbeatResult {
                error_code: BROKER_ID_NOT_REGISTERED,
                is_fenced: true,
                should_shut_down: false,
                is_caught_up: false,
            });
        };
        if request.broker_epoch != existing.epoch {
            return Ok(HeartbeatResult {
                error_code: STALE_BROKER_EPOCH,
                is_fenced: true,
                should_shut_down: false,
                is_caught_up: false,
            });
        }
        let mut session = existing;
        if request.want_fence {
            session.fenced = true;
        }
        if request.want_shutdown {
            session.shutdown = true;
        }
        if !session.fenced && request.now_ms >= session.last_heartbeat_ms {
            session.last_heartbeat_ms = request.now_ms;
        }
        session.metadata_offset = request.metadata_offset;
        if request.version >= 1 {
            session.last_transaction_timestamp_ms = request.last_transaction_timestamp_ms;
        }
        self.persist(request.broker_id, &session)?;
        Ok(HeartbeatResult {
            error_code: 0,
            is_fenced: session.fenced,
            should_shut_down: session.shutdown,
            is_caught_up: false,
        })
    }

    /// Fence sessions that have been silent longer than the configured timeout.
    pub fn expire(&mut self, now_ms: u64) -> Result<usize, Error> {
        self.ready()?;
        let timeout = self.limits.session_timeout_ms;
        let due: Vec<i32> = self
            .sessions
            .iter()
            .filter(|(_, session)| {
                !session.fenced && silent(session.last_heartbeat_ms, now_ms, timeout)
            })
            .map(|(id, _)| *id)
            .collect();
        let count = due.len();
        for id in due {
            let Some(mut session) = self.sessions.get(&id).cloned() else {
                continue;
            };
            session.fenced = true;
            self.persist(id, &session)?;
        }
        Ok(count)
    }

    /// Authorize a data write from a broker that still holds the active epoch.
    pub fn authorize_write(
        &mut self,
        broker_id: i32,
        epoch: i64,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.authorize(broker_id, epoch, now_ms)
    }

    /// Authorize a controller mutation from a broker that still holds the active epoch.
    pub fn authorize_controller(
        &mut self,
        broker_id: i32,
        epoch: i64,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.authorize(broker_id, epoch, now_ms)
    }

    /// Current confirmed epoch, if this broker has registered.
    pub fn epoch(&self, broker_id: i32) -> Option<i64> {
        self.sessions.get(&broker_id).map(|session| session.epoch)
    }

    /// Whether the current epoch is fenced.
    pub fn is_fenced(&self, broker_id: i32) -> Option<bool> {
        self.sessions.get(&broker_id).map(|session| session.fenced)
    }

    fn authorize(&mut self, broker_id: i32, epoch: i64, now_ms: u64) -> Result<(), Error> {
        self.expire(now_ms)?;
        let session = self.sessions.get(&broker_id).ok_or(Error::NotRegistered)?;
        if epoch != session.epoch {
            return Err(Error::StaleEpoch);
        }
        if session.fenced || session.shutdown {
            return Err(Error::Fenced);
        }
        Ok(())
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&mut self, broker_id: i32, session: &Session) -> Result<(), Error> {
        if self.records >= self.limits.max_records {
            return Err(Error::Budget);
        }
        let payload = encode_session(&self.cluster_id, broker_id, session)?;
        match self.journal.append(1, &payload) {
            Ok(_) => {
                self.records = self.records.saturating_add(1);
                self.sessions.insert(broker_id, session.clone());
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(Error::Storage(error))
            }
        }
    }
}

fn silent(last_ms: u64, now_ms: u64, timeout_ms: u64) -> bool {
    now_ms.saturating_sub(last_ms) > timeout_ms
}

fn same_contact(session: &Session, request: &Registration) -> bool {
    session.listeners == request.listeners
        && session.features == request.features
        && session.rack == request.rack
        && session.migrating_zk == request.migrating_zk
        && session.log_dirs == request.log_dirs
}

fn validate_cluster_id(cluster_id: &str) -> Result<(), Error> {
    if cluster_id.is_empty()
        || cluster_id.len() > MAX_TEXT
        || cluster_id.chars().any(|c| c.is_control())
    {
        return Err(Error::InconsistentCluster);
    }
    Ok(())
}

fn validate_registration(request: &Registration) -> Result<(), Error> {
    if !(0..=4).contains(&request.version) {
        return Err(Error::UnsupportedVersion);
    }
    if request.version < 1 && request.migrating_zk {
        return Err(Error::UnsupportedVersion);
    }
    if request.version < 2 && !request.log_dirs.is_empty() {
        return Err(Error::UnsupportedVersion);
    }
    if request.version < 3 && request.previous_broker_epoch != -1 {
        return Err(Error::UnsupportedVersion);
    }
    if request.broker_id < 0 || request.previous_broker_epoch < -1 {
        return Err(Error::InvalidRegistration);
    }
    if request.incarnation_id == [0; 16] {
        return Err(Error::InvalidRegistration);
    }
    if request.listeners.is_empty() || request.listeners.len() > MAX_LISTENERS {
        return Err(Error::InvalidRegistration);
    }
    let mut names = BTreeMap::<&str, ()>::new();
    for listener in &request.listeners {
        if listener.name.is_empty()
            || listener.host.is_empty()
            || listener.port == 0
            || !(0..=3).contains(&listener.security_protocol)
            || listener.name.len() > MAX_TEXT
            || listener.host.len() > MAX_TEXT
            || listener.name.chars().any(|c| c.is_control())
            || listener.host.chars().any(|c| c.is_control())
            || names.insert(listener.name.as_str(), ()).is_some()
        {
            return Err(Error::InvalidRegistration);
        }
    }
    if request.features.len() > MAX_FEATURES || request.log_dirs.len() > MAX_LOG_DIRS {
        return Err(Error::InvalidRegistration);
    }
    let mut features = BTreeMap::<&str, ()>::new();
    for feature in &request.features {
        if feature.name.is_empty()
            || feature.name.len() > MAX_TEXT
            || feature.min_version < 0
            || feature.max_version < feature.min_version
            || features.insert(feature.name.as_str(), ()).is_some()
        {
            return Err(Error::InvalidRegistration);
        }
    }
    if request.log_dirs.iter().any(|id| {
        *id == [0; 16] || request.log_dirs.iter().filter(|other| *other == id).count() > 1
    }) {
        return Err(Error::InvalidRegistration);
    }
    match &request.rack {
        Some(rack)
            if rack.is_empty() || rack.len() > MAX_TEXT || rack.chars().any(|c| c.is_control()) =>
        {
            Err(Error::InvalidRegistration)
        }
        _ => Ok(()),
    }
}

fn max_record_bytes() -> usize {
    RECORD_MAGIC.len()
        + 4
        + 8
        + 16
        + 1
        + 1
        + 8
        + 8
        + 8
        + 2
        + MAX_TEXT
        + 1
        + 2
        + MAX_TEXT
        + 2
        + MAX_LISTENERS * (2 + MAX_TEXT + 2 + MAX_TEXT + 2 + 2)
        + 2
        + MAX_FEATURES * (2 + MAX_TEXT + 2 + 2)
        + 2
        + MAX_LOG_DIRS * 16
}

fn encode_session(cluster_id: &str, broker_id: i32, session: &Session) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::with_capacity(256);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.extend_from_slice(&broker_id.to_be_bytes());
    bytes.extend_from_slice(&session.epoch.to_be_bytes());
    bytes.extend_from_slice(&session.incarnation_id);
    bytes.push(u8::from(session.fenced));
    bytes.push(u8::from(session.shutdown));
    bytes.extend_from_slice(&session.last_heartbeat_ms.to_be_bytes());
    bytes.extend_from_slice(&session.metadata_offset.to_be_bytes());
    bytes.extend_from_slice(&session.last_transaction_timestamp_ms.to_be_bytes());
    put_str(&mut bytes, cluster_id)?;
    match &session.rack {
        Some(rack) => {
            bytes.push(1);
            put_str(&mut bytes, rack)?;
        }
        None => bytes.push(0),
    }
    put_count(&mut bytes, session.listeners.len())?;
    for listener in &session.listeners {
        put_str(&mut bytes, &listener.name)?;
        put_str(&mut bytes, &listener.host)?;
        bytes.extend_from_slice(&listener.port.to_be_bytes());
        bytes.extend_from_slice(&listener.security_protocol.to_be_bytes());
    }
    put_count(&mut bytes, session.features.len())?;
    for feature in &session.features {
        put_str(&mut bytes, &feature.name)?;
        bytes.extend_from_slice(&feature.min_version.to_be_bytes());
        bytes.extend_from_slice(&feature.max_version.to_be_bytes());
    }
    put_count(&mut bytes, session.log_dirs.len())?;
    for id in &session.log_dirs {
        bytes.extend_from_slice(id);
    }
    bytes.push(u8::from(session.migrating_zk));
    Ok(bytes)
}

fn decode_session(bytes: &[u8], expected_cluster: &str) -> Result<(i32, Session), Error> {
    let mut input = bytes;
    let magic = take(&mut input, 8)?;
    if magic != RECORD_MAGIC {
        return Err(Error::InvalidRegistration);
    }
    let broker_id = i32::from_be_bytes(
        take(&mut input, 4)?
            .try_into()
            .map_err(|_| Error::InvalidRegistration)?,
    );
    let epoch = i64::from_be_bytes(
        take(&mut input, 8)?
            .try_into()
            .map_err(|_| Error::InvalidRegistration)?,
    );
    let mut incarnation_id = [0u8; 16];
    incarnation_id.copy_from_slice(take(&mut input, 16)?);
    let fenced = take(&mut input, 1)?[0] == 1;
    let shutdown = take(&mut input, 1)?[0] == 1;
    let last_heartbeat_ms = u64::from_be_bytes(
        take(&mut input, 8)?
            .try_into()
            .map_err(|_| Error::InvalidRegistration)?,
    );
    let metadata_offset = i64::from_be_bytes(
        take(&mut input, 8)?
            .try_into()
            .map_err(|_| Error::InvalidRegistration)?,
    );
    let last_transaction_timestamp_ms = i64::from_be_bytes(
        take(&mut input, 8)?
            .try_into()
            .map_err(|_| Error::InvalidRegistration)?,
    );
    let cluster_id = take_str(&mut input)?;
    if cluster_id != expected_cluster {
        return Err(Error::InconsistentCluster);
    }
    let rack = if take(&mut input, 1)?[0] == 1 {
        Some(take_str(&mut input)?)
    } else {
        None
    };
    let listener_count = take_count(&mut input)?;
    let mut listeners = Vec::with_capacity(listener_count);
    for _ in 0..listener_count {
        listeners.push(Listener {
            name: take_str(&mut input)?,
            host: take_str(&mut input)?,
            port: u16::from_be_bytes(
                take(&mut input, 2)?
                    .try_into()
                    .map_err(|_| Error::InvalidRegistration)?,
            ),
            security_protocol: i16::from_be_bytes(
                take(&mut input, 2)?
                    .try_into()
                    .map_err(|_| Error::InvalidRegistration)?,
            ),
        });
    }
    let feature_count = take_count(&mut input)?;
    let mut features = Vec::with_capacity(feature_count);
    for _ in 0..feature_count {
        features.push(Feature {
            name: take_str(&mut input)?,
            min_version: i16::from_be_bytes(
                take(&mut input, 2)?
                    .try_into()
                    .map_err(|_| Error::InvalidRegistration)?,
            ),
            max_version: i16::from_be_bytes(
                take(&mut input, 2)?
                    .try_into()
                    .map_err(|_| Error::InvalidRegistration)?,
            ),
        });
    }
    let dir_count = take_count(&mut input)?;
    let mut log_dirs = Vec::with_capacity(dir_count);
    for _ in 0..dir_count {
        let mut id = [0u8; 16];
        id.copy_from_slice(take(&mut input, 16)?);
        log_dirs.push(id);
    }
    let migrating_zk = take(&mut input, 1)?[0] == 1;
    if !input.is_empty() {
        return Err(Error::InvalidRegistration);
    }
    Ok((
        broker_id,
        Session {
            incarnation_id,
            epoch,
            listeners,
            features,
            rack,
            migrating_zk,
            log_dirs,
            fenced,
            shutdown,
            last_heartbeat_ms,
            metadata_offset,
            last_transaction_timestamp_ms,
        },
    ))
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidRegistration)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_count(bytes: &mut Vec<u8>, count: usize) -> Result<(), Error> {
    let len = u16::try_from(count).map_err(|_| Error::InvalidRegistration)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidRegistration);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_count(input: &mut &[u8]) -> Result<usize, Error> {
    let bytes = take(input, 2)?;
    Ok(usize::from(u16::from_be_bytes(
        bytes.try_into().map_err(|_| Error::InvalidRegistration)?,
    )))
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = take_count(input)?;
    let bytes = take(input, len)?;
    let text = std::str::from_utf8(bytes).map_err(|_| Error::InvalidRegistration)?;
    if text.chars().any(|c| c.is_control()) {
        return Err(Error::InvalidRegistration);
    }
    Ok(text.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(max_record_bytes(), 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, 100, 1_000, journal).unwrap_or_else(|_| panic!("registry limits"))
    }

    fn registry(name: &str) -> (Registry, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-reg-{name}-{nanos}"));
        let registry = Registry::open(&path, "cluster-410", limits())
            .unwrap_or_else(|error| panic!("{error:?}"));
        (registry, path)
    }

    fn registration(version: i16, incarnation: u8, previous: i64) -> Registration {
        Registration {
            version,
            broker_id: 3,
            incarnation_id: [incarnation; 16],
            listeners: vec![Listener {
                name: "PLAINTEXT".into(),
                host: "127.0.0.1".into(),
                port: 9092,
                security_protocol: 0,
            }],
            features: vec![Feature {
                name: "metadata.version".into(),
                min_version: 1,
                max_version: 7,
            }],
            rack: None,
            migrating_zk: false,
            log_dirs: vec![],
            previous_broker_epoch: previous,
            now_ms: 0,
        }
    }

    #[test]
    fn first_registration_starts_at_epoch_zero_and_replays() {
        let (mut registry, path) = registry("first");
        let registered = registry
            .register(&registration(4, 2, -1))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(registered.broker_epoch, 0);
        registry
            .authorize_write(3, 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        registry
            .authorize_controller(3, 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        drop(registry);
        let restored = Registry::open(&path, "cluster-410", limits())
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.epoch(3), Some(0));
        assert_eq!(restored.is_fenced(3), Some(false));
    }

    #[test]
    fn stale_epoch_does_not_replace_or_refresh_the_live_session() {
        let (mut registry, _) = registry("stale");
        registry
            .register(&registration(3, 2, -1))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let stale = registry.register(&registration(3, 9, 4));
        assert!(matches!(stale, Err(Error::StaleEpoch)));
        assert_eq!(registry.epoch(3), Some(0));
        let beat = registry
            .heartbeat(&Heartbeat {
                version: 1,
                broker_id: 3,
                broker_epoch: 4,
                metadata_offset: 10,
                want_fence: false,
                want_shutdown: false,
                last_transaction_timestamp_ms: 5,
                now_ms: 50,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(beat.error_code, STALE_BROKER_EPOCH);
        assert!(beat.is_fenced);
        registry
            .authorize_write(3, 0, 50)
            .unwrap_or_else(|error| panic!("{error:?}"));
    }

    #[test]
    fn expired_and_replaced_epochs_cannot_write_or_mutate() {
        let (mut registry, _) = registry("expire");
        registry
            .register(&registration(3, 2, -1))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(registry.expire(1_001).unwrap_or(99), 1);
        assert!(matches!(
            registry.authorize_write(3, 0, 1_001),
            Err(Error::Fenced)
        ));
        assert!(matches!(
            registry.authorize_controller(3, 0, 1_001),
            Err(Error::Fenced)
        ));
        let mut replacement = registration(3, 9, 0);
        replacement.now_ms = 1_002;
        replacement.log_dirs = vec![[7; 16]];
        let registered = registry
            .register(&replacement)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(registered.broker_epoch, 1);
        assert!(matches!(
            registry.authorize_write(3, 0, 1_002),
            Err(Error::StaleEpoch)
        ));
        registry
            .authorize_controller(3, 1, 1_002)
            .unwrap_or_else(|error| panic!("{error:?}"));
    }

    #[test]
    fn older_versions_reject_fields_they_cannot_express() {
        let (mut registry, _) = registry("version");
        let mut request = registration(0, 2, -1);
        request.migrating_zk = true;
        assert!(matches!(
            registry.register(&request),
            Err(Error::UnsupportedVersion)
        ));
        request.migrating_zk = false;
        request.log_dirs = vec![[1; 16]];
        request.version = 1;
        assert!(matches!(
            registry.register(&request),
            Err(Error::UnsupportedVersion)
        ));
        request.log_dirs.clear();
        request.previous_broker_epoch = 0;
        request.version = 2;
        assert!(matches!(
            registry.register(&request),
            Err(Error::UnsupportedVersion)
        ));
        assert_eq!(registry.epoch(3), None);
    }

    #[test]
    fn an_older_heartbeat_does_not_shorten_the_session() {
        let (mut registry, path) = registry("clock");
        let mut request = registration(4, 2, -1);
        request.now_ms = 5_000;
        registry
            .register(&request)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let beat = registry
            .heartbeat(&Heartbeat {
                version: 0,
                broker_id: 3,
                broker_epoch: 0,
                metadata_offset: 1,
                want_fence: false,
                want_shutdown: false,
                last_transaction_timestamp_ms: -1,
                now_ms: 1_000,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(beat.error_code, 0);
        assert!(!beat.is_fenced);
        drop(registry);
        let mut registry = Registry::open(&path, "cluster-410", limits())
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(registry.expire(6_000).unwrap_or(99), 0);
        assert_eq!(registry.is_fenced(3), Some(false));
        assert_eq!(registry.expire(6_001).unwrap_or(99), 1);
        assert_eq!(registry.is_fenced(3), Some(true));
    }
}
