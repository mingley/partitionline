//! Durable classic consumer-group Join and Sync.
//!
//! The first joiner is the leader. A new member or a session timeout starts a
//! new generation and clears assignments. Sync succeeds only when every current
//! member has joined that generation. The leader's assignment must cover each
//! member at most once and must not give the same partition to two members.
//! A stale generation is `ILLEGAL_GENERATION`. A stale coordinator epoch is
//! `NOT_COORDINATOR`.
//!
//! Successful transitions are journaled, so failover reopens the same
//! generation and assignment. This is not the JoinGroup or SyncGroup RPC.

use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLGRP001";

/// Kafka `NOT_COORDINATOR`.
pub const NOT_COORDINATOR: i16 = 16;
/// Kafka `ILLEGAL_GENERATION`.
pub const ILLEGAL_GENERATION: i16 = 22;
/// Kafka `INCONSISTENT_GROUP_PROTOCOL`.
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
/// Kafka `UNKNOWN_MEMBER_ID`.
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Kafka `REBALANCE_IN_PROGRESS`.
pub const REBALANCE_IN_PROGRESS: i16 = 27;

/// Result of a join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    /// Assigned member id.
    pub member_id: String,
    /// Generation the member must sync.
    pub generation: i32,
    /// Leader member id.
    pub leader_id: String,
}

/// Why a group operation was refused.
#[derive(Debug)]
pub enum Error {
    /// The coordinator epoch is not current.
    NotCoordinator,
    /// The generation does not match the group.
    IllegalGeneration,
    /// The protocol does not match the group.
    InconsistentProtocol,
    /// The member id is not in the group.
    UnknownMember,
    /// Not every member has joined this generation.
    RebalanceInProgress,
    /// The group id, member, or assignment is not usable.
    InvalidRequest,
    /// A failed append left this coordinator unable to confirm further changes.
    Poisoned,
    /// The limits cannot store the group.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a member-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::NotCoordinator => NOT_COORDINATOR,
            Self::IllegalGeneration => ILLEGAL_GENERATION,
            Self::InconsistentProtocol => INCONSISTENT_GROUP_PROTOCOL,
            Self::UnknownMember => UNKNOWN_MEMBER_ID,
            Self::RebalanceInProgress => REBALANCE_IN_PROGRESS,
            Self::InvalidRequest | Self::InvalidLimits => return None,
            Self::Poisoned | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Group bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_members: usize,
    session_timeout_ms: u64,
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a positive member cap and session timeout.
    pub fn new(
        max_members: usize,
        session_timeout_ms: u64,
        max_records: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=1_000).contains(&max_members)
            || session_timeout_ms == 0
            || !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < 4096
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_members,
            session_timeout_ms,
            max_records,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Member {
    joined_generation: i32,
    last_seen_ms: u64,
    assignment: Vec<i32>,
    synced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Group {
    protocol: String,
    generation: i32,
    leader: String,
    members: BTreeMap<String, Member>,
}

/// Durable classic group coordinator.
pub struct Groups {
    limits: Limits,
    epoch: i64,
    next_member: u64,
    groups: BTreeMap<String, Group>,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Groups {
    /// Open or recover group state. The coordinator epoch starts at 0.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut groups = Self {
            limits,
            epoch: 0,
            next_member: 1,
            groups: BTreeMap::new(),
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut offset = groups.journal.base_offset();
        loop {
            let batch =
                groups
                    .journal
                    .fetch(offset, 16, groups.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                groups.apply(&entry.payload)?;
                groups.stored = groups.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(groups)
    }

    /// Current coordinator epoch.
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Current generation, if the group exists.
    pub fn generation(&self, group: &str) -> Option<i32> {
        self.groups.get(group).map(|group| group.generation)
    }

    /// Partitions assigned to a member in the stable generation.
    pub fn assignment(&self, group: &str, member: &str) -> Option<&[i32]> {
        self.groups
            .get(group)
            .and_then(|group| group.members.get(member))
            .filter(|member| member.synced)
            .map(|member| member.assignment.as_slice())
    }

    /// Move the coordinator lineage forward.
    pub fn advance(&mut self, epoch: i64) -> Result<i64, Error> {
        self.ready()?;
        if epoch <= self.epoch {
            return Err(Error::NotCoordinator);
        }
        self.epoch = epoch;
        self.persist_all()?;
        Ok(epoch)
    }

    /// Join `group` with `protocol`. An empty `member_id` allocates one.
    pub fn join(
        &mut self,
        group_id: &str,
        member_id: &str,
        protocol: &str,
        coordinator_epoch: i64,
        now_ms: u64,
    ) -> Result<Join, Error> {
        self.ready()?;
        self.require_coordinator(coordinator_epoch)?;
        validate_name(group_id)?;
        validate_name(protocol)?;
        let fresh = member_id.is_empty();
        let member_id = if fresh {
            let id = format!("m-{}", self.next_member);
            self.next_member = self.next_member.saturating_add(1);
            id
        } else {
            validate_name(member_id)?;
            member_id.to_owned()
        };
        if !fresh
            && self
                .groups
                .get(group_id)
                .and_then(|group| group.members.get(&member_id))
                .is_none()
        {
            return Err(Error::UnknownMember);
        }
        let group = self.groups.entry(group_id.to_owned()).or_insert(Group {
            protocol: protocol.to_owned(),
            generation: 0,
            leader: String::new(),
            members: BTreeMap::new(),
        });
        if group.protocol != protocol {
            return Err(Error::InconsistentProtocol);
        }
        if !fresh && !group.members.contains_key(&member_id) {
            return Err(Error::UnknownMember);
        }
        if group.members.len() >= self.limits.max_members && !group.members.contains_key(&member_id)
        {
            return Err(Error::InvalidLimits);
        }
        let known = group.members.contains_key(&member_id);
        if !known || group.members.values().any(|member| member.synced) {
            group.generation = group.generation.saturating_add(1);
            for member in group.members.values_mut() {
                member.assignment.clear();
                member.synced = false;
                member.joined_generation = 0;
            }
        }
        group.members.insert(
            member_id.clone(),
            Member {
                joined_generation: group.generation,
                last_seen_ms: now_ms,
                assignment: Vec::new(),
                synced: false,
            },
        );
        if group.leader.is_empty() || !group.members.contains_key(&group.leader) {
            group.leader = member_id.clone();
        }
        let join = Join {
            member_id,
            generation: group.generation,
            leader_id: group.leader.clone(),
        };
        self.persist_all()?;
        Ok(join)
    }

    /// Publish the leader assignment or accept it as a follower.
    ///
    /// Followers pass an empty assignment. The leader's map must name every
    /// current member and must not repeat a partition.
    pub fn sync(
        &mut self,
        group_id: &str,
        member_id: &str,
        generation: i32,
        coordinator_epoch: i64,
        assignment: &BTreeMap<String, Vec<i32>>,
    ) -> Result<Vec<i32>, Error> {
        self.ready()?;
        self.require_coordinator(coordinator_epoch)?;
        let Some(group) = self.groups.get(group_id) else {
            return Err(Error::UnknownMember);
        };
        if generation != group.generation {
            return Err(Error::IllegalGeneration);
        }
        if !group.members.contains_key(member_id) {
            return Err(Error::UnknownMember);
        }
        if group
            .members
            .values()
            .any(|member| member.joined_generation != generation)
        {
            return Err(Error::RebalanceInProgress);
        }
        if member_id == group.leader {
            validate_assignment(group, assignment)?;
        } else if !assignment.is_empty() {
            return Err(Error::InvalidRequest);
        }
        if member_id == group.leader {
            let group = self.groups.get_mut(group_id).ok_or(Error::UnknownMember)?;
            for (id, partitions) in assignment {
                let member = group.members.get_mut(id).ok_or(Error::UnknownMember)?;
                member.assignment = partitions.clone();
            }
        }
        let group = self.groups.get_mut(group_id).ok_or(Error::UnknownMember)?;
        let member = group
            .members
            .get_mut(member_id)
            .ok_or(Error::UnknownMember)?;
        member.synced = true;
        let owned = member.assignment.clone();
        self.persist_all()?;
        Ok(owned)
    }

    /// Remove members that have been silent longer than the session timeout.
    pub fn expire(&mut self, now_ms: u64) -> Result<usize, Error> {
        self.ready()?;
        let timeout = self.limits.session_timeout_ms;
        let mut removed = 0usize;
        for group in self.groups.values_mut() {
            let stale: Vec<String> = group
                .members
                .iter()
                .filter(|(_, member)| now_ms.saturating_sub(member.last_seen_ms) > timeout)
                .map(|(id, _)| id.clone())
                .collect();
            if stale.is_empty() {
                continue;
            }
            for id in stale {
                group.members.remove(&id);
                removed = removed.saturating_add(1);
            }
            group.generation = group.generation.saturating_add(1);
            for member in group.members.values_mut() {
                member.assignment.clear();
                member.synced = false;
                member.joined_generation = 0;
            }
            if !group.members.contains_key(&group.leader) {
                group.leader = group.members.keys().next().cloned().unwrap_or_default();
            }
        }
        if removed > 0 {
            self.persist_all()?;
        }
        Ok(removed)
    }

    fn require_coordinator(&self, epoch: i64) -> Result<(), Error> {
        if epoch == self.epoch {
            Ok(())
        } else {
            Err(Error::NotCoordinator)
        }
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist_all(&mut self) -> Result<(), Error> {
        let payload = encode(self.epoch, self.next_member, &self.groups)?;
        if self.stored >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        match self.journal.append(1, &payload) {
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

    fn apply(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let (epoch, next_member, groups) = decode(bytes)?;
        if epoch < self.epoch {
            return Err(Error::InvalidRequest);
        }
        self.epoch = epoch;
        self.next_member = next_member;
        self.groups = groups;
        Ok(())
    }
}

fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty() || name.len() > 249 || name.chars().any(|c| c.is_control()) {
        Err(Error::InvalidRequest)
    } else {
        Ok(())
    }
}

fn validate_assignment(
    group: &Group,
    assignment: &BTreeMap<String, Vec<i32>>,
) -> Result<(), Error> {
    if assignment.len() != group.members.len() {
        return Err(Error::InvalidRequest);
    }
    let mut seen = BTreeSet::new();
    for (member, partitions) in assignment {
        if !group.members.contains_key(member) {
            return Err(Error::UnknownMember);
        }
        for partition in partitions {
            if *partition < 0 || !seen.insert(*partition) {
                return Err(Error::InvalidRequest);
            }
        }
    }
    Ok(())
}

fn encode(
    epoch: i64,
    next_member: u64,
    groups: &BTreeMap<String, Group>,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.extend_from_slice(&epoch.to_be_bytes());
    bytes.extend_from_slice(&next_member.to_be_bytes());
    put_u16(&mut bytes, groups.len())?;
    for (id, group) in groups {
        put_str(&mut bytes, id)?;
        put_str(&mut bytes, &group.protocol)?;
        bytes.extend_from_slice(&group.generation.to_be_bytes());
        put_str(&mut bytes, &group.leader)?;
        put_u16(&mut bytes, group.members.len())?;
        for (member_id, member) in &group.members {
            put_str(&mut bytes, member_id)?;
            bytes.extend_from_slice(&member.joined_generation.to_be_bytes());
            bytes.extend_from_slice(&member.last_seen_ms.to_be_bytes());
            bytes.push(u8::from(member.synced));
            put_u16(&mut bytes, member.assignment.len())?;
            for partition in &member.assignment {
                bytes.extend_from_slice(&partition.to_be_bytes());
            }
        }
    }
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<(i64, u64, BTreeMap<String, Group>), Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let epoch = take_i64(&mut bytes)?;
    let next_member = take_u64(&mut bytes)?;
    let group_count = take_u16(&mut bytes)?;
    let mut groups = BTreeMap::new();
    for _ in 0..group_count {
        let id = take_str(&mut bytes)?;
        let protocol = take_str(&mut bytes)?;
        let generation = take_i32(&mut bytes)?;
        let leader = take_str(&mut bytes)?;
        let member_count = take_u16(&mut bytes)?;
        let mut members = BTreeMap::new();
        for _ in 0..member_count {
            let member_id = take_str(&mut bytes)?;
            let joined_generation = take_i32(&mut bytes)?;
            let last_seen_ms = take_u64(&mut bytes)?;
            let synced = take_u8(&mut bytes)? == 1;
            let assignment_len = take_u16(&mut bytes)?;
            let mut assignment = Vec::with_capacity(assignment_len);
            for _ in 0..assignment_len {
                assignment.push(take_i32(&mut bytes)?);
            }
            members.insert(
                member_id,
                Member {
                    joined_generation,
                    last_seen_ms,
                    assignment,
                    synced,
                },
            );
        }
        groups.insert(
            id,
            Group {
                protocol,
                generation,
                leader,
                members,
            },
        );
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok((epoch, next_member, groups))
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    put_u16(bytes, value.len())?;
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u16(bytes: &mut Vec<u8>, value: usize) -> Result<(), Error> {
    let value = u16::try_from(value).map_err(|_| Error::InvalidRequest)?;
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

fn take_u16(input: &mut &[u8]) -> Result<usize, Error> {
    Ok(usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    )))
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = take_u16(input)?;
    let bytes = take(input, len)?;
    std::str::from_utf8(bytes)
        .map(|text| text.to_owned())
        .map_err(|_| Error::InvalidRequest)
}

fn take_i64(input: &mut &[u8]) -> Result<i64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(i64::from_be_bytes(value))
}

fn take_u64(input: &mut &[u8]) -> Result<u64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(u64::from_be_bytes(value))
}

fn take_i32(input: &mut &[u8]) -> Result<i32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(i32::from_be_bytes(value))
}

fn take_u8(input: &mut &[u8]) -> Result<u8, Error> {
    Ok(take(input, 1)?[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(4096, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, 1_000, 32, journal).unwrap_or_else(|_| panic!("group limits"))
    }

    fn open(name: &str) -> (Groups, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-group-{name}-{nanos}"));
        let groups = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (groups, path)
    }

    #[test]
    fn two_members_receive_disjoint_assignments_that_survive_failover() {
        let (mut groups, path) = open("join");
        let leader = groups
            .join("orders", "", "range", 0, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let follower = groups
            .join("orders", "", "range", 0, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let leader = groups
            .join("orders", &leader.member_id, "range", 0, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(leader.generation, follower.generation);
        assert_eq!(follower.leader_id, leader.member_id);
        let mut assignment = BTreeMap::new();
        assignment.insert(leader.member_id.clone(), vec![0, 1]);
        assignment.insert(follower.member_id.clone(), vec![2]);
        assert!(matches!(
            groups.sync(
                "orders",
                &follower.member_id,
                leader.generation,
                0,
                &assignment
            ),
            Err(Error::InvalidRequest)
        ));
        let owned = groups
            .sync(
                "orders",
                &leader.member_id,
                leader.generation,
                0,
                &assignment,
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(owned, vec![0, 1]);
        let followed = groups
            .sync(
                "orders",
                &follower.member_id,
                follower.generation,
                0,
                &BTreeMap::new(),
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(followed, vec![2]);
        groups
            .advance(1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            groups.sync(
                "orders",
                &leader.member_id,
                leader.generation,
                0,
                &assignment
            ),
            Err(Error::NotCoordinator)
        ));
        drop(groups);
        let restored = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(restored.epoch(), 1);
        assert_eq!(
            restored.assignment("orders", &leader.member_id),
            Some(&[0, 1][..])
        );
        assert_eq!(
            restored.assignment("orders", &follower.member_id),
            Some(&[2][..])
        );
    }

    #[test]
    fn timeout_revokes_ownership_and_a_stale_generation_cannot_sync() {
        let (mut groups, _) = open("timeout");
        let leader = groups
            .join("orders", "", "range", 0, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let follower = groups
            .join("orders", "", "range", 0, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_001).unwrap_or(99), 1);
        assert!(matches!(
            groups.sync(
                "orders",
                &leader.member_id,
                leader.generation,
                0,
                &BTreeMap::new()
            ),
            Err(Error::IllegalGeneration)
        ));
        assert_eq!(groups.generation("orders"), Some(follower.generation + 1));
        assert!(groups.assignment("orders", &leader.member_id).is_none());
        assert!(matches!(
            groups.join("orders", "ghost", "range", 0, 2_000),
            Err(Error::UnknownMember)
        ));
    }
}
