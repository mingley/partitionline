//! Cluster, replica, log-directory, feature, and quorum administration.
//!
//! Each admin key has an explicit disposition. Spoken versions are 0 and 1.
//! Anything else, including the rest of the broker API matrix, returns
//! unsupported and does not change state. Mutations require the current
//! controller epoch. An invalid log-directory batch stores nothing. Reopen
//! restores the last committed snapshot.
//!
//! This is not the Kafka admin RPC and it does not advertise success for an
//! API it does not implement.

use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLADM001";

/// Kafka `STALE_CONTROLLER_EPOCH`.
pub const STALE_CONTROLLER_EPOCH: i16 = 11;
/// Kafka `UNSUPPORTED_VERSION`.
pub const UNSUPPORTED_VERSION: i16 = 35;
/// Kafka `INVALID_REPLICA_ASSIGNMENT`.
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
/// Kafka `LOG_DIR_NOT_FOUND`.
pub const LOG_DIR_NOT_FOUND: i16 = 57;
/// Kafka `FEATURE_UPDATE_FAILED`.
pub const FEATURE_UPDATE_FAILED: i16 = 96;

/// Kafka `ALTER_REPLICA_LOG_DIRS`.
pub const ALTER_REPLICA_LOG_DIRS: i16 = 34;
/// Kafka `DESCRIBE_LOG_DIRS`.
pub const DESCRIBE_LOG_DIRS: i16 = 35;
/// Kafka `ALTER_PARTITION_REASSIGNMENTS`.
pub const ALTER_PARTITION_REASSIGNMENTS: i16 = 45;
/// Kafka `LIST_PARTITION_REASSIGNMENTS`.
pub const LIST_PARTITION_REASSIGNMENTS: i16 = 46;
/// Kafka `DESCRIBE_QUORUM`.
pub const DESCRIBE_QUORUM: i16 = 55;
/// Kafka `UPDATE_FEATURES`.
pub const UPDATE_FEATURES: i16 = 57;
/// Kafka `DESCRIBE_CLUSTER`.
pub const DESCRIBE_CLUSTER: i16 = 60;
/// Kafka `ADD_RAFT_VOTER`.
pub const ADD_RAFT_VOTER: i16 = 80;
/// Kafka `REMOVE_RAFT_VOTER`.
pub const REMOVE_RAFT_VOTER: i16 = 81;

/// Whether an API key is implemented by this admin surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Versions `min..=max` are spoken. Other versions are unsupported.
    Local {
        /// Lowest spoken version.
        min: i16,
        /// Highest spoken version.
        max: i16,
    },
    /// The key is not implemented. Calls fail and do not mutate.
    Unsupported,
}

/// Disposition for one API key. Local admin keys speak versions 0 and 1.
#[must_use]
pub fn disposition(api_key: i16) -> Disposition {
    match api_key {
        ALTER_REPLICA_LOG_DIRS
        | DESCRIBE_LOG_DIRS
        | ALTER_PARTITION_REASSIGNMENTS
        | LIST_PARTITION_REASSIGNMENTS
        | DESCRIBE_QUORUM
        | UPDATE_FEATURES
        | DESCRIBE_CLUSTER
        | ADD_RAFT_VOTER
        | REMOVE_RAFT_VOTER => Disposition::Local { min: 0, max: 1 },
        _ => Disposition::Unsupported,
    }
}

/// Why an admin call was refused.
#[derive(Debug)]
pub enum Error {
    /// The API or version is not spoken.
    Unsupported,
    /// The controller epoch is not current.
    StaleEpoch,
    /// The replica set or leader is not usable.
    InvalidAssignment,
    /// A log directory is not registered.
    LogDirNotFound,
    /// A feature level would move backwards.
    FeatureDowngrade,
    /// The name, version, or voter set is not usable.
    InvalidRequest,
    /// A failed append left this admin log unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the snapshot.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a caller-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        match self {
            Self::Unsupported => Some(UNSUPPORTED_VERSION),
            Self::StaleEpoch => Some(STALE_CONTROLLER_EPOCH),
            Self::InvalidAssignment => Some(INVALID_REPLICA_ASSIGNMENT),
            Self::LogDirNotFound => Some(LOG_DIR_NOT_FOUND),
            Self::FeatureDowngrade => Some(FEATURE_UPDATE_FAILED),
            Self::InvalidRequest | Self::Poisoned | Self::InvalidLimits | Self::Storage(_) => None,
        }
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Admin bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_replicas: usize,
    max_dirs: usize,
    max_features: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive replica, directory, and feature caps.
    pub fn new(
        max_replicas: usize,
        max_dirs: usize,
        max_features: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_replicas)
            || !(1..=1_000).contains(&max_dirs)
            || !(1..=1_000).contains(&max_features)
            || journal.max_entry_bytes() < 1024
            || journal.max_index_entries() < max_replicas
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_replicas,
            max_dirs,
            max_features,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TopicPartition {
    topic: String,
    partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Replica {
    replicas: Vec<i32>,
    leader: i32,
    log_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    cluster_id: String,
    controller_epoch: i64,
    dirs: BTreeSet<String>,
    replicas: BTreeMap<TopicPartition, Replica>,
    features: BTreeMap<String, i16>,
    voters: BTreeSet<i32>,
}

/// One partition assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment<'a> {
    /// Topic.
    pub topic: &'a str,
    /// Partition.
    pub partition: i32,
    /// Replica broker ids. The leader must be included.
    pub replicas: &'a [i32],
    /// Leader broker.
    pub leader: i32,
    /// Registered log directory.
    pub log_dir: &'a str,
}

/// One log-directory move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirMove {
    /// Topic.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Broker whose replica moves.
    pub broker: i32,
    /// Registered log directory.
    pub directory: String,
}

/// Durable admin snapshot.
pub struct Admin {
    limits: Limits,
    state: State,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Admin {
    /// Open or recover admin state. The initial voter is stored at epoch 0.
    pub fn open(
        path: impl AsRef<Path>,
        limits: Limits,
        cluster_id: &str,
        initial_voter: i32,
    ) -> Result<Self, Error> {
        if !valid_name(cluster_id) || initial_voter <= 0 {
            return Err(Error::InvalidRequest);
        }
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut admin = Self {
            limits,
            state: State {
                cluster_id: cluster_id.to_owned(),
                controller_epoch: 0,
                dirs: BTreeSet::new(),
                replicas: BTreeMap::new(),
                features: BTreeMap::new(),
                voters: BTreeSet::from([initial_voter]),
            },
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut cursor = admin.journal.base_offset();
        let mut restored = false;
        loop {
            let batch = admin
                .journal
                .fetch(cursor, 32, admin.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                let decoded = decode(&entry.payload)?;
                accept_admin(&admin.state, &decoded, admin.limits)?;
                admin.state = decoded;
                restored = true;
                admin.stored = admin.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        if !restored {
            let state = admin.state.clone();
            admin.commit(state)?;
        }
        Ok(admin)
    }

    /// Refuse an unsupported API or version without mutating.
    pub fn reject(&self, api_key: i16, version: i16) -> Result<(), Error> {
        spoken(api_key, version)
    }

    /// Cluster id and controller epoch.
    pub fn describe_cluster(&self, version: i16) -> Result<(&str, i64), Error> {
        spoken(DESCRIBE_CLUSTER, version)?;
        Ok((&self.state.cluster_id, self.state.controller_epoch))
    }

    /// Advance the controller epoch. Older epochs cannot mutate.
    pub fn advance(&mut self, version: i16, epoch: i64) -> Result<(), Error> {
        self.ready()?;
        spoken(DESCRIBE_CLUSTER, version)?;
        if epoch <= self.state.controller_epoch {
            return Err(Error::StaleEpoch);
        }
        let mut next = self.state.clone();
        next.controller_epoch = epoch;
        self.commit(next)
    }

    /// Register a log directory.
    pub fn register_log_dir(
        &mut self,
        version: i16,
        controller_epoch: i64,
        directory: &str,
    ) -> Result<(), Error> {
        self.ready()?;
        self.authorize(DESCRIBE_LOG_DIRS, version, controller_epoch)?;
        if !valid_name(directory) {
            return Err(Error::InvalidRequest);
        }
        if self.state.dirs.len() >= self.limits.max_dirs && !self.state.dirs.contains(directory) {
            return Err(Error::InvalidLimits);
        }
        let mut next = self.state.clone();
        next.dirs.insert(directory.to_owned());
        self.commit(next)
    }

    /// Replace a partition assignment. The leader must be one of the replicas.
    pub fn alter_assignment(
        &mut self,
        version: i16,
        controller_epoch: i64,
        assignment: Assignment<'_>,
    ) -> Result<(), Error> {
        self.ready()?;
        self.authorize(ALTER_PARTITION_REASSIGNMENTS, version, controller_epoch)?;
        if !valid_name(assignment.topic)
            || assignment.partition < 0
            || !self.state.dirs.contains(assignment.log_dir)
        {
            return Err(if self.state.dirs.contains(assignment.log_dir) {
                Error::InvalidRequest
            } else {
                Error::LogDirNotFound
            });
        }
        if !valid_replicas(assignment.replicas, assignment.leader) {
            return Err(Error::InvalidAssignment);
        }
        let key = TopicPartition {
            topic: assignment.topic.to_owned(),
            partition: assignment.partition,
        };
        if self.state.replicas.len() >= self.limits.max_replicas
            && !self.state.replicas.contains_key(&key)
        {
            return Err(Error::InvalidLimits);
        }
        let mut next = self.state.clone();
        next.replicas.insert(
            key,
            Replica {
                replicas: assignment.replicas.to_vec(),
                leader: assignment.leader,
                log_dir: assignment.log_dir.to_owned(),
            },
        );
        self.commit(next)
    }

    /// Move replicas onto registered directories, or store nothing.
    pub fn alter_log_dirs(
        &mut self,
        version: i16,
        controller_epoch: i64,
        moves: &[LogDirMove],
    ) -> Result<(), Error> {
        self.ready()?;
        self.authorize(ALTER_REPLICA_LOG_DIRS, version, controller_epoch)?;
        if moves.is_empty() {
            return Err(Error::InvalidRequest);
        }
        let mut next = self.state.clone();
        for move_dir in moves {
            if !next.dirs.contains(&move_dir.directory) {
                return Err(Error::LogDirNotFound);
            }
            let Some(replica) = next.replicas.get_mut(&TopicPartition {
                topic: move_dir.topic.clone(),
                partition: move_dir.partition,
            }) else {
                return Err(Error::InvalidRequest);
            };
            if !replica.replicas.contains(&move_dir.broker) {
                return Err(Error::InvalidAssignment);
            }
            replica.log_dir = move_dir.directory.clone();
        }
        self.commit(next)
    }

    /// Log directory for one replica.
    pub fn describe_log_dir(
        &self,
        version: i16,
        topic: &str,
        partition: i32,
    ) -> Result<&str, Error> {
        spoken(DESCRIBE_LOG_DIRS, version)?;
        self.state
            .replicas
            .get(&TopicPartition {
                topic: topic.to_owned(),
                partition,
            })
            .map(|replica| replica.log_dir.as_str())
            .ok_or(Error::InvalidRequest)
    }

    /// Raise a finalized feature level. A lower level is refused.
    pub fn update_feature(
        &mut self,
        version: i16,
        controller_epoch: i64,
        name: &str,
        level: i16,
    ) -> Result<(), Error> {
        self.ready()?;
        self.authorize(UPDATE_FEATURES, version, controller_epoch)?;
        if !valid_name(name) || level < 0 {
            return Err(Error::InvalidRequest);
        }
        if self
            .state
            .features
            .get(name)
            .is_some_and(|current| level < *current)
        {
            return Err(Error::FeatureDowngrade);
        }
        if self.state.features.len() >= self.limits.max_features
            && !self.state.features.contains_key(name)
        {
            return Err(Error::InvalidLimits);
        }
        let mut next = self.state.clone();
        next.features.insert(name.to_owned(), level);
        self.commit(next)
    }

    /// Finalized level for a feature.
    pub fn feature_level(&self, version: i16, name: &str) -> Result<i16, Error> {
        spoken(UPDATE_FEATURES, version)?;
        self.state
            .features
            .get(name)
            .copied()
            .ok_or(Error::InvalidRequest)
    }

    /// Add a quorum voter.
    pub fn add_voter(
        &mut self,
        version: i16,
        controller_epoch: i64,
        voter: i32,
    ) -> Result<(), Error> {
        self.ready()?;
        self.authorize(ADD_RAFT_VOTER, version, controller_epoch)?;
        if voter <= 0 || self.state.voters.contains(&voter) {
            return Err(Error::InvalidRequest);
        }
        let mut next = self.state.clone();
        next.voters.insert(voter);
        self.commit(next)
    }

    /// Remove a quorum voter. The last voter cannot be removed.
    pub fn remove_voter(
        &mut self,
        version: i16,
        controller_epoch: i64,
        voter: i32,
    ) -> Result<(), Error> {
        self.ready()?;
        self.authorize(REMOVE_RAFT_VOTER, version, controller_epoch)?;
        if !self.state.voters.contains(&voter) || self.state.voters.len() == 1 {
            return Err(Error::InvalidRequest);
        }
        let mut next = self.state.clone();
        next.voters.remove(&voter);
        self.commit(next)
    }

    /// Current voters and controller epoch.
    pub fn describe_quorum(&self, version: i16) -> Result<(Vec<i32>, i64), Error> {
        spoken(DESCRIBE_QUORUM, version)?;
        Ok((
            self.state.voters.iter().copied().collect(),
            self.state.controller_epoch,
        ))
    }

    fn authorize(&self, api_key: i16, version: i16, controller_epoch: i64) -> Result<(), Error> {
        spoken(api_key, version)?;
        if controller_epoch != self.state.controller_epoch {
            Err(Error::StaleEpoch)
        } else {
            Ok(())
        }
    }

    fn commit(&mut self, state: State) -> Result<(), Error> {
        let payload = encode(&state)?;
        if payload.len() > self.limits.journal.max_entry_bytes() {
            return Err(Error::InvalidLimits);
        }
        self.persist(&payload)?;
        self.state = state;
        Ok(())
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        if self.stored >= self.limits.max_replicas.saturating_mul(8) {
            return Err(Error::InvalidLimits);
        }
        match self.journal.append(1, payload) {
            Ok(_) => {
                self.stored = self.stored.saturating_add(1);
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error.into())
            }
        }
    }
}

fn spoken(api_key: i16, version: i16) -> Result<(), Error> {
    match disposition(api_key) {
        Disposition::Local { min, max } if (min..=max).contains(&version) => Ok(()),
        Disposition::Local { .. } | Disposition::Unsupported => Err(Error::Unsupported),
    }
}

fn valid_name(text: &str) -> bool {
    !text.is_empty() && text.len() <= 249 && !text.chars().any(char::is_control)
}

fn valid_replicas(replicas: &[i32], leader: i32) -> bool {
    if replicas.is_empty() || !replicas.contains(&leader) {
        return false;
    }
    let mut seen = BTreeSet::new();
    replicas
        .iter()
        .all(|replica| *replica > 0 && seen.insert(*replica))
}

fn encode(state: &State) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    put_str(&mut bytes, &state.cluster_id)?;
    bytes.extend_from_slice(&state.controller_epoch.to_be_bytes());
    put_u32(
        &mut bytes,
        u32::try_from(state.dirs.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for directory in &state.dirs {
        put_str(&mut bytes, directory)?;
    }
    put_u32(
        &mut bytes,
        u32::try_from(state.replicas.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for (key, replica) in &state.replicas {
        put_str(&mut bytes, &key.topic)?;
        bytes.extend_from_slice(&key.partition.to_be_bytes());
        put_u32(
            &mut bytes,
            u32::try_from(replica.replicas.len()).map_err(|_| Error::InvalidRequest)?,
        )?;
        for broker in &replica.replicas {
            bytes.extend_from_slice(&broker.to_be_bytes());
        }
        bytes.extend_from_slice(&replica.leader.to_be_bytes());
        put_str(&mut bytes, &replica.log_dir)?;
    }
    put_u32(
        &mut bytes,
        u32::try_from(state.features.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for (name, level) in &state.features {
        put_str(&mut bytes, name)?;
        bytes.extend_from_slice(&level.to_be_bytes());
    }
    put_u32(
        &mut bytes,
        u32::try_from(state.voters.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for voter in &state.voters {
        bytes.extend_from_slice(&voter.to_be_bytes());
    }
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<State, Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let cluster_id = take_str(&mut bytes)?;
    let controller_epoch = take_i64(&mut bytes)?;
    let dir_count = take_u32(&mut bytes)?;
    let mut dirs = BTreeSet::new();
    for _ in 0..dir_count {
        let directory = take_str(&mut bytes)?;
        if !valid_name(&directory) || !dirs.insert(directory) {
            return Err(Error::InvalidRequest);
        }
    }
    let replica_count = take_u32(&mut bytes)?;
    let mut replicas = BTreeMap::new();
    for _ in 0..replica_count {
        let topic = take_str(&mut bytes)?;
        let partition = take_i32(&mut bytes)?;
        let broker_count = take_u32(&mut bytes)?;
        let mut brokers = Vec::new();
        for _ in 0..broker_count {
            brokers.push(take_i32(&mut bytes)?);
        }
        let leader = take_i32(&mut bytes)?;
        let log_dir = take_str(&mut bytes)?;
        if replicas
            .insert(
                TopicPartition { topic, partition },
                Replica {
                    replicas: brokers,
                    leader,
                    log_dir,
                },
            )
            .is_some()
        {
            return Err(Error::InvalidRequest);
        }
    }
    let feature_count = take_u32(&mut bytes)?;
    let mut features = BTreeMap::new();
    for _ in 0..feature_count {
        let name = take_str(&mut bytes)?;
        let level = take_i16(&mut bytes)?;
        if !valid_name(&name) || level < 0 || features.insert(name, level).is_some() {
            return Err(Error::InvalidRequest);
        }
    }
    let voter_count = take_u32(&mut bytes)?;
    let mut voters = BTreeSet::new();
    for _ in 0..voter_count {
        let voter = take_i32(&mut bytes)?;
        if voter <= 0 || !voters.insert(voter) {
            return Err(Error::InvalidRequest);
        }
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    let state = State {
        cluster_id,
        controller_epoch,
        dirs,
        replicas,
        features,
        voters,
    };
    validate_recovered(&state)?;
    Ok(state)
}

fn accept_admin(previous: &State, next: &State, limits: Limits) -> Result<(), Error> {
    if next.dirs.len() > limits.max_dirs
        || next.replicas.len() > limits.max_replicas
        || next.features.len() > limits.max_features
    {
        return Err(Error::InvalidLimits);
    }
    if next.cluster_id != previous.cluster_id {
        return Err(Error::InvalidRequest);
    }
    let epoch_changed = next.controller_epoch != previous.controller_epoch;
    let dirs_changed = next.dirs != previous.dirs;
    let replicas_changed = next.replicas != previous.replicas;
    let features_changed = next.features != previous.features;
    let voters_changed = next.voters != previous.voters;
    let changes = usize::from(epoch_changed)
        + usize::from(dirs_changed)
        + usize::from(replicas_changed)
        + usize::from(features_changed)
        + usize::from(voters_changed);
    if changes == 0 {
        return Ok(());
    }
    if changes != 1 {
        return Err(Error::InvalidRequest);
    }
    if epoch_changed {
        return if next.controller_epoch > previous.controller_epoch {
            Ok(())
        } else {
            Err(Error::InvalidRequest)
        };
    }
    if dirs_changed {
        return if next.dirs.len() == previous.dirs.len().saturating_add(1)
            && previous.dirs.is_subset(&next.dirs)
        {
            Ok(())
        } else {
            Err(Error::InvalidRequest)
        };
    }
    if voters_changed {
        let added = next.voters.len() == previous.voters.len().saturating_add(1)
            && previous.voters.is_subset(&next.voters);
        let removed = previous.voters.len() == next.voters.len().saturating_add(1)
            && next.voters.is_subset(&previous.voters)
            && !next.voters.is_empty();
        return if added || removed {
            Ok(())
        } else {
            Err(Error::InvalidRequest)
        };
    }
    if features_changed {
        return feature_step(&previous.features, &next.features);
    }
    replica_step(&previous.replicas, &next.replicas)
}

fn feature_step(
    previous: &BTreeMap<String, i16>,
    next: &BTreeMap<String, i16>,
) -> Result<(), Error> {
    if next.len() == previous.len().saturating_add(1)
        && previous
            .iter()
            .all(|(name, level)| next.get(name) == Some(level))
    {
        return Ok(());
    }
    if next.len() != previous.len() {
        return Err(Error::InvalidRequest);
    }
    let mut raised = 0usize;
    for (name, level) in previous {
        match next.get(name) {
            Some(next_level) if next_level == level => {}
            Some(next_level) if *next_level > *level => raised += 1,
            _ => return Err(Error::InvalidRequest),
        }
    }
    if raised == 1 {
        Ok(())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn replica_step(
    previous: &BTreeMap<TopicPartition, Replica>,
    next: &BTreeMap<TopicPartition, Replica>,
) -> Result<(), Error> {
    if next.len() == previous.len().saturating_add(1) {
        return if previous
            .iter()
            .all(|(key, replica)| next.get(key) == Some(replica))
        {
            Ok(())
        } else {
            Err(Error::InvalidRequest)
        };
    }
    if next.len() != previous.len() {
        return Err(Error::InvalidRequest);
    }
    let mut changed = 0usize;
    let mut structural = 0usize;
    for (key, old) in previous {
        let Some(new) = next.get(key) else {
            return Err(Error::InvalidRequest);
        };
        if old == new {
            continue;
        }
        changed += 1;
        if old.replicas != new.replicas || old.leader != new.leader {
            structural += 1;
        }
    }
    if structural == 0 || (structural == 1 && changed == 1) {
        Ok(())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn validate_recovered(state: &State) -> Result<(), Error> {
    if !valid_name(&state.cluster_id) || state.controller_epoch < 0 || state.voters.is_empty() {
        return Err(Error::InvalidRequest);
    }
    for (key, replica) in &state.replicas {
        if !valid_name(&key.topic)
            || key.partition < 0
            || !state.dirs.contains(&replica.log_dir)
            || !valid_replicas(&replica.replicas, replica.leader)
        {
            return Err(Error::InvalidRequest);
        }
    }
    Ok(())
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) -> Result<(), Error> {
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidRequest);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ));
    String::from_utf8(take(input, len)?.to_vec()).map_err(|_| Error::InvalidRequest)
}

fn take_i64(input: &mut &[u8]) -> Result<i64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(i64::from_be_bytes(value))
}

fn take_i32(input: &mut &[u8]) -> Result<i32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(i32::from_be_bytes(value))
}

fn take_i16(input: &mut &[u8]) -> Result<i16, Error> {
    let mut value = [0u8; 2];
    value.copy_from_slice(take(input, 2)?);
    Ok(i16::from_be_bytes(value))
}

fn take_u32(input: &mut &[u8]) -> Result<u32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(u32::from_be_bytes(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(8192, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(16, 8, 8, journal).unwrap_or_else(|_| panic!("admin limits"))
    }

    fn open(name: &str) -> (Admin, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-admin-{name}-{nanos}"));
        let admin = Admin::open(&path, limits(), "cluster-1", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        (admin, path)
    }

    #[test]
    fn an_admin_snapshot_cannot_change_epoch_and_voters_together() {
        let (admin, _) = open("jump");
        let mut voters = admin.state.voters.clone();
        voters.insert(2);
        let mut next = admin.state.clone();
        next.controller_epoch = 4;
        next.voters = voters;
        assert!(matches!(
            accept_admin(&admin.state, &next, admin.limits),
            Err(Error::InvalidRequest)
        ));
        assert_eq!(admin.state.controller_epoch, 0);
        assert_eq!(admin.state.voters.len(), 1);
    }

    #[test]
    fn a_leader_outside_the_replica_set_is_not_recovered() {
        let mut dirs = BTreeSet::new();
        dirs.insert("data".to_owned());
        let mut replicas = BTreeMap::new();
        replicas.insert(
            TopicPartition {
                topic: "orders".to_owned(),
                partition: 0,
            },
            Replica {
                replicas: vec![1],
                leader: 2,
                log_dir: "data".to_owned(),
            },
        );
        let mut voters = BTreeSet::new();
        voters.insert(1);
        let state = State {
            cluster_id: "cluster-1".to_owned(),
            controller_epoch: 0,
            dirs,
            replicas,
            features: BTreeMap::new(),
            voters,
        };
        let encoded = encode(&state).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(decode(&encoded), Err(Error::InvalidRequest)));
    }

    #[test]
    fn admin_dispositions_and_failover_keep_validated_mutations() {
        for api in [0, 1, 19, 20, 32] {
            assert_eq!(disposition(api), Disposition::Unsupported);
            let (admin, _) = open("reject");
            assert!(matches!(admin.reject(api, 0), Err(Error::Unsupported)));
        }
        for api in [
            ALTER_REPLICA_LOG_DIRS,
            DESCRIBE_LOG_DIRS,
            ALTER_PARTITION_REASSIGNMENTS,
            LIST_PARTITION_REASSIGNMENTS,
            DESCRIBE_QUORUM,
            UPDATE_FEATURES,
            DESCRIBE_CLUSTER,
            ADD_RAFT_VOTER,
            REMOVE_RAFT_VOTER,
        ] {
            assert_eq!(disposition(api), Disposition::Local { min: 0, max: 1 });
            let (admin, _) = open("version");
            assert!(matches!(admin.reject(api, 2), Err(Error::Unsupported)));
            assert!(admin.reject(api, 0).is_ok());
            assert!(admin.reject(api, 1).is_ok());
        }

        let (mut admin, path) = open("mutate");
        let (cluster, epoch) = admin
            .describe_cluster(0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(cluster, "cluster-1");
        assert_eq!(epoch, 0);
        admin
            .register_log_dir(0, 0, "data-a")
            .unwrap_or_else(|error| panic!("{error:?}"));
        admin
            .register_log_dir(1, 0, "data-b")
            .unwrap_or_else(|error| panic!("{error:?}"));
        admin
            .alter_assignment(
                0,
                0,
                Assignment {
                    topic: "orders",
                    partition: 0,
                    replicas: &[1, 2],
                    leader: 1,
                    log_dir: "data-a",
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            admin.alter_log_dirs(
                1,
                0,
                &[
                    LogDirMove {
                        topic: "orders".to_owned(),
                        partition: 0,
                        broker: 1,
                        directory: "missing".to_owned(),
                    },
                    LogDirMove {
                        topic: "orders".to_owned(),
                        partition: 0,
                        broker: 2,
                        directory: "data-b".to_owned(),
                    },
                ],
            ),
            Err(Error::LogDirNotFound)
        ));
        assert_eq!(
            admin
                .describe_log_dir(0, "orders", 0)
                .unwrap_or_else(|error| panic!("{error:?}")),
            "data-a"
        );
        admin
            .alter_log_dirs(
                1,
                0,
                &[LogDirMove {
                    topic: "orders".to_owned(),
                    partition: 0,
                    broker: 1,
                    directory: "data-b".to_owned(),
                }],
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        admin
            .update_feature(0, 0, "metadata.version", 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            admin.update_feature(1, 0, "metadata.version", 1),
            Err(Error::FeatureDowngrade)
        ));
        assert_eq!(
            admin
                .feature_level(0, "metadata.version")
                .unwrap_or_else(|error| panic!("{error:?}")),
            2
        );
        admin
            .add_voter(0, 0, 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        admin
            .remove_voter(1, 0, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            admin.remove_voter(0, 0, 2),
            Err(Error::InvalidRequest)
        ));
        assert!(matches!(
            admin.alter_assignment(
                2,
                0,
                Assignment {
                    topic: "orders",
                    partition: 0,
                    replicas: &[1],
                    leader: 1,
                    log_dir: "data-b",
                },
            ),
            Err(Error::Unsupported)
        ));
        assert!(matches!(admin.add_voter(0, 9, 3), Err(Error::StaleEpoch)));
        let (voters, _) = admin
            .describe_quorum(1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(voters, vec![2]);
        drop(admin);
        let restored = Admin::open(&path, limits(), "cluster-1", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            restored
                .describe_log_dir(1, "orders", 0)
                .unwrap_or_else(|error| panic!("{error:?}")),
            "data-b"
        );
        assert_eq!(
            restored
                .feature_level(0, "metadata.version")
                .unwrap_or_else(|error| panic!("{error:?}")),
            2
        );
        let (voters, epoch) = restored
            .describe_quorum(0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(voters, vec![2]);
        assert_eq!(epoch, 0);
        let (cluster, _) = restored
            .describe_cluster(1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(cluster, "cluster-1");
    }
}
