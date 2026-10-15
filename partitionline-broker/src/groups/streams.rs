//! Streams-group membership for heartbeat version 0.
//!
//! The client supplies the member id. Epoch 0 joins with a topology epoch and
//! task count. Epoch -1 leaves. Epoch -2 replaces the member id of the same
//! static instance. An older endpoint epoch does not replace a newer one.
//! Active tasks are range-assigned. A second member receives
//! the other member's active tasks as standby, so one task is never active on
//! two members. A different topology epoch stores nothing. Silence past the
//! session timeout drops the member and reassigns the rest. Reopen restores
//! the assignment epoch.
//!
//! Kafka 4.1.2 advertises this heartbeat as version 0 only. This is not the
//! StreamsGroupHeartbeat or StreamsGroupDescribe RPC.

use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLSTR001";

/// Join epoch from `StreamsGroupHeartbeatRequest`.
pub const JOIN_GROUP_MEMBER_EPOCH: i32 = 0;
/// Leave epoch from `StreamsGroupHeartbeatRequest`.
pub const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;
/// Static rejoin epoch from `StreamsGroupHeartbeatRequest`.
pub const STATIC_REJOIN_MEMBER_EPOCH: i32 = -2;
/// The only spoken heartbeat version in Kafka 4.1.2.
pub const SPOKEN_VERSION: i16 = 0;
/// Kafka `UNKNOWN_MEMBER_ID`.
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Kafka `INCONSISTENT_GROUP_PROTOCOL`.
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
/// Kafka `FENCED_MEMBER_EPOCH`.
pub const FENCED_MEMBER_EPOCH: i16 = 110;
/// Kafka `FENCED_INSTANCE_ID`.
pub const FENCED_INSTANCE_ID: i16 = 82;
/// Kafka `STALE_MEMBER_EPOCH`.
pub const STALE_MEMBER_EPOCH: i16 = 113;

/// One streams-group heartbeat.
#[derive(Debug, Clone, Copy)]
pub struct Heartbeat<'a> {
    /// Must be [`SPOKEN_VERSION`].
    pub version: i16,
    /// Group id.
    pub group_id: &'a str,
    /// Client-generated member id.
    pub member_id: &'a str,
    /// Last issued epoch, or a join, leave, or static-rejoin sentinel.
    pub member_epoch: i32,
    /// Coordinator clock for this heartbeat. Older values do not shorten the session.
    pub now_ms: u64,
    /// Static instance id, required for [`STATIC_REJOIN_MEMBER_EPOCH`].
    pub instance_id: Option<&'a str>,
    /// Latest endpoint epoch this client received.
    pub endpoint_epoch: i32,
    /// Topology epoch. Required on join.
    pub topology_epoch: Option<i32>,
    /// Task count. Required on join and must match the group's topology.
    pub task_count: Option<i32>,
}

/// Assigned tasks for one member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResult {
    /// Member id.
    pub member_id: String,
    /// Epoch the next heartbeat must present.
    pub member_epoch: i32,
    /// Group epoch.
    pub group_epoch: i32,
    /// Epoch of the target task assignment.
    pub assignment_epoch: i32,
    /// Endpoint epoch echoed to the member.
    pub endpoint_epoch: i32,
    /// Active task ids.
    pub active: Vec<i32>,
    /// Standby task ids. Disjoint from `active`.
    pub standby: Vec<i32>,
}

/// One described member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberView {
    /// Member id.
    pub member_id: String,
    /// Current epoch.
    pub member_epoch: i32,
    /// Active tasks.
    pub active: Vec<i32>,
    /// Standby tasks.
    pub standby: Vec<i32>,
}

/// Describe view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// Topology epoch fixed by the first join.
    pub topology_epoch: i32,
    /// Task count fixed by the first join.
    pub task_count: i32,
    /// Members.
    pub members: Vec<MemberView>,
}

/// Why a streams-group operation was refused.
#[derive(Debug)]
pub enum Error {
    /// The topology epoch or task count does not match the group.
    Inconsistent,
    /// The member id is not in the group.
    UnknownMember,
    /// The member epoch is older than the issued epoch.
    Fenced,
    /// The static instance id does not match the joined member.
    FencedInstance,
    /// The member epoch skipped ahead of the issued epoch.
    Stale,
    /// The group, version, or topology is not usable.
    InvalidRequest,
    /// A failed append left this coordinator unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the group.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a member-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        match self {
            Self::Inconsistent => Some(INCONSISTENT_GROUP_PROTOCOL),
            Self::UnknownMember => Some(UNKNOWN_MEMBER_ID),
            Self::Fenced => Some(FENCED_MEMBER_EPOCH),
            Self::FencedInstance => Some(FENCED_INSTANCE_ID),
            Self::Stale => Some(STALE_MEMBER_EPOCH),
            Self::InvalidRequest | Self::Poisoned | Self::InvalidLimits | Self::Storage(_) => None,
        }
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Streams-group bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_members: usize,
    max_tasks: i32,
    session_timeout_ms: u64,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive member, task, and session caps.
    pub fn new(
        max_members: usize,
        max_tasks: i32,
        session_timeout_ms: u64,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=10_000).contains(&max_members)
            || !(1..=100_000).contains(&max_tasks)
            || session_timeout_ms == 0
            || journal.max_entry_bytes() < 1024
            || journal.max_index_entries() < max_members
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_members,
            max_tasks,
            session_timeout_ms,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Member {
    epoch: i32,
    last_seen_ms: u64,
    acked: bool,
    instance: Option<String>,
    endpoint_epoch: i32,
    active: Vec<i32>,
    standby: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Group {
    epoch: i32,
    assignment_epoch: i32,
    topology_epoch: i32,
    task_count: i32,
    members: BTreeMap<String, Member>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    groups: BTreeMap<String, Group>,
}

/// Durable streams-group coordinator.
pub struct Groups {
    limits: Limits,
    state: State,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Groups {
    /// Open or recover streams groups.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut groups = Self {
            limits,
            state: State {
                groups: BTreeMap::new(),
            },
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut cursor = groups.journal.base_offset();
        loop {
            let batch =
                groups
                    .journal
                    .fetch(cursor, 32, groups.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                groups.state = decode(&entry.payload)?;
                groups.stored = groups.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(groups)
    }

    /// Join, refresh, leave, or replace a static member.
    pub fn heartbeat(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        self.ready()?;
        if request.version != SPOKEN_VERSION
            || !valid_name(request.group_id)
            || !valid_name(request.member_id)
        {
            return Err(Error::InvalidRequest);
        }
        if request.member_epoch == LEAVE_GROUP_MEMBER_EPOCH {
            return self.leave(request);
        }
        if request.member_epoch == STATIC_REJOIN_MEMBER_EPOCH {
            return self.rejoin(request);
        }
        let known = self
            .state
            .groups
            .get(request.group_id)
            .is_some_and(|group| group.members.contains_key(request.member_id));
        if !known {
            if request.member_epoch != JOIN_GROUP_MEMBER_EPOCH {
                return Err(Error::UnknownMember);
            }
            return self.join(request);
        }
        self.refresh(request)
    }

    /// Current topology, epochs, and task assignment.
    pub fn describe(&self, group_id: &str) -> Option<Description> {
        let group = self.state.groups.get(group_id)?;
        let mut members = Vec::new();
        for (member_id, member) in &group.members {
            members.push(MemberView {
                member_id: member_id.clone(),
                member_epoch: member.epoch,
                active: member.active.clone(),
                standby: member.standby.clone(),
            });
        }
        Some(Description {
            group_epoch: group.epoch,
            assignment_epoch: group.assignment_epoch,
            topology_epoch: group.topology_epoch,
            task_count: group.task_count,
            members,
        })
    }

    /// Remove members silent longer than the session timeout and reassign the rest.
    pub fn expire(&mut self, now_ms: u64) -> Result<usize, Error> {
        self.ready()?;
        let timeout = self.limits.session_timeout_ms;
        let mut next = self.state.clone();
        let mut removed = 0usize;
        let ids: Vec<String> = next.groups.keys().cloned().collect();
        for id in ids {
            let Some(group) = next.groups.get_mut(&id) else {
                continue;
            };
            let stale: Vec<String> = group
                .members
                .iter()
                .filter(|(_, member)| now_ms.saturating_sub(member.last_seen_ms) > timeout)
                .map(|(member_id, _)| member_id.clone())
                .collect();
            if stale.is_empty() {
                continue;
            }
            for member_id in stale {
                group.members.remove(&member_id);
                removed = removed.saturating_add(1);
            }
            if group.members.is_empty() {
                next.groups.remove(&id);
            } else if let Some(group) = next.groups.get_mut(&id) {
                reassign(group)?;
            }
        }
        if removed > 0 {
            self.commit(next)?;
        }
        Ok(removed)
    }

    fn join(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        let (topology, tasks) = topology_of(request, self.limits.max_tasks)?;
        let instance = instance_of(request.instance_id)?;
        let mut next = self.state.clone();
        let group = next
            .groups
            .entry(request.group_id.to_owned())
            .or_insert(Group {
                epoch: -1,
                assignment_epoch: -1,
                topology_epoch: topology,
                task_count: tasks,
                members: BTreeMap::new(),
            });
        if group.members.is_empty() {
            group.topology_epoch = topology;
            group.task_count = tasks;
        } else if group.topology_epoch != topology || group.task_count != tasks {
            return Err(Error::Inconsistent);
        }
        if group.members.contains_key(request.member_id) {
            return Err(Error::Fenced);
        }
        if group.members.len() >= self.limits.max_members {
            return Err(Error::InvalidLimits);
        }
        group.members.insert(
            request.member_id.to_owned(),
            Member {
                epoch: 0,
                last_seen_ms: request.now_ms,
                acked: true,
                instance,
                endpoint_epoch: request.endpoint_epoch,
                active: Vec::new(),
                standby: Vec::new(),
            },
        );
        group.epoch = advance_epoch(group.epoch)?;
        reassign(group)?;
        if let Some(member) = group.members.get_mut(request.member_id) {
            member.acked = true;
            member.epoch = 0;
        }
        let result = result_from(group, request.member_id)?;
        self.commit(next)?;
        Ok(result)
    }

    fn refresh(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        if let (Some(topology), Some(tasks)) = (request.topology_epoch, request.task_count) {
            let group = self
                .state
                .groups
                .get(request.group_id)
                .ok_or(Error::UnknownMember)?;
            if group.topology_epoch != topology || group.task_count != tasks {
                return Err(Error::Inconsistent);
            }
        }
        let mut next = self.state.clone();
        let Some(group) = next.groups.get_mut(request.group_id) else {
            return Err(Error::UnknownMember);
        };
        let Some(member) = group.members.get_mut(request.member_id) else {
            return Err(Error::UnknownMember);
        };
        if request.member_epoch < member.epoch {
            return Err(Error::Fenced);
        }
        if request.member_epoch > member.epoch {
            return Err(Error::Stale);
        }
        if member.instance.as_deref() != request.instance_id {
            return Err(Error::FencedInstance);
        }
        member.last_seen_ms = later_seen(member.last_seen_ms, request.now_ms);
        if request.endpoint_epoch > member.endpoint_epoch {
            member.endpoint_epoch = request.endpoint_epoch;
        }
        reassign(group)?;
        if let Some(member) = group.members.get_mut(request.member_id) {
            if !member.acked {
                member.epoch = advance_epoch(member.epoch)?;
                member.acked = true;
            }
        }
        let result = result_from(group, request.member_id)?;
        self.commit(next)?;
        Ok(result)
    }

    fn leave(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        let mut next = self.state.clone();
        let (epoch, assignment_epoch, empty) = {
            let Some(group) = next.groups.get_mut(request.group_id) else {
                return Err(Error::UnknownMember);
            };
            let Some(member) = group.members.get(request.member_id) else {
                return Err(Error::UnknownMember);
            };
            if member.instance.as_deref() != request.instance_id {
                return Err(Error::FencedInstance);
            }
            group.members.remove(request.member_id);
            group.epoch = advance_epoch(group.epoch)?;
            let empty = group.members.is_empty();
            if !empty {
                reassign(group)?;
            }
            (group.epoch, group.assignment_epoch, empty)
        };
        if empty {
            next.groups.remove(request.group_id);
        }
        let result = HeartbeatResult {
            member_id: request.member_id.to_owned(),
            member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
            group_epoch: epoch,
            assignment_epoch,
            endpoint_epoch: request.endpoint_epoch,
            active: Vec::new(),
            standby: Vec::new(),
        };
        self.commit(next)?;
        Ok(result)
    }

    fn rejoin(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        let Some(instance) = instance_of(request.instance_id)? else {
            return Err(Error::InvalidRequest);
        };
        let mut next = self.state.clone();
        let Some(group) = next.groups.get_mut(request.group_id) else {
            return Err(Error::UnknownMember);
        };
        let previous = group
            .members
            .iter()
            .find(|(_, member)| member.instance.as_ref() == Some(&instance))
            .map(|(id, _)| id.clone());
        let Some(previous) = previous else {
            return Err(Error::UnknownMember);
        };
        if request.member_id != previous && group.members.contains_key(request.member_id) {
            return Err(Error::InvalidRequest);
        }
        let (endpoint, seen) = group
            .members
            .get(&previous)
            .map(|member| (member.endpoint_epoch, member.last_seen_ms))
            .unwrap_or((request.endpoint_epoch, request.now_ms));
        let endpoint = endpoint.max(request.endpoint_epoch);
        group.members.remove(&previous);
        group.members.insert(
            request.member_id.to_owned(),
            Member {
                epoch: 0,
                last_seen_ms: later_seen(seen, request.now_ms),
                acked: true,
                instance: Some(instance),
                endpoint_epoch: endpoint,
                active: Vec::new(),
                standby: Vec::new(),
            },
        );
        group.epoch = advance_epoch(group.epoch)?;
        reassign(group)?;
        if let Some(member) = group.members.get_mut(request.member_id) {
            member.acked = true;
            member.epoch = 0;
        }
        let result = result_from(group, request.member_id)?;
        self.commit(next)?;
        Ok(result)
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
        if self.stored >= self.limits.max_members.saturating_mul(8) {
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

fn topology_of(request: Heartbeat<'_>, max_tasks: i32) -> Result<(i32, i32), Error> {
    match (request.topology_epoch, request.task_count) {
        (Some(epoch), Some(tasks)) if epoch >= 0 && (1..=max_tasks).contains(&tasks) => {
            Ok((epoch, tasks))
        }
        _ => Err(Error::InvalidRequest),
    }
}

fn instance_of(instance: Option<&str>) -> Result<Option<String>, Error> {
    match instance {
        None => Ok(None),
        Some(value) if valid_name(value) => Ok(Some(value.to_owned())),
        Some(_) => Err(Error::InvalidRequest),
    }
}

fn valid_name(text: &str) -> bool {
    !text.is_empty() && text.len() <= 249 && !text.chars().any(char::is_control)
}

fn advance_epoch(epoch: i32) -> Result<i32, Error> {
    epoch.checked_add(1).ok_or(Error::InvalidRequest)
}

fn reassign(group: &mut Group) -> Result<(), Error> {
    let previous: BTreeMap<String, (Vec<i32>, Vec<i32>)> = group
        .members
        .iter()
        .map(|(id, member)| (id.clone(), (member.active.clone(), member.standby.clone())))
        .collect();
    let ids: Vec<String> = group.members.keys().cloned().collect();
    let count = usize::try_from(group.task_count).unwrap_or(0);
    let mut active: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    if !ids.is_empty() {
        let base = count / ids.len();
        let extra = count % ids.len();
        let mut next = 0i32;
        for (index, member_id) in ids.iter().enumerate() {
            let share = base + usize::from(index < extra);
            let mut owned = Vec::new();
            for _ in 0..share {
                owned.push(next);
                next = next.saturating_add(1);
            }
            active.insert(member_id.clone(), owned);
        }
    }
    for (index, member_id) in ids.iter().enumerate() {
        let standby = if ids.len() > 1 {
            let donor = &ids[(index + 1) % ids.len()];
            active.get(donor).cloned().unwrap_or_default()
        } else {
            Vec::new()
        };
        if let Some(member) = group.members.get_mut(member_id) {
            member.active = active.get(member_id).cloned().unwrap_or_default();
            member.standby = standby;
        }
    }
    let mut changed = false;
    for (id, member) in &mut group.members {
        let now = (member.active.clone(), member.standby.clone());
        if previous.get(id) != Some(&now) {
            member.acked = false;
            changed = true;
        }
    }
    if changed {
        group.assignment_epoch = advance_epoch(group.assignment_epoch)?;
    }
    Ok(())
}

fn later_seen(current: u64, now_ms: u64) -> u64 {
    current.max(now_ms)
}

fn result_from(group: &Group, member_id: &str) -> Result<HeartbeatResult, Error> {
    let member = group.members.get(member_id).ok_or(Error::UnknownMember)?;
    Ok(HeartbeatResult {
        member_id: member_id.to_owned(),
        member_epoch: member.epoch,
        group_epoch: group.epoch,
        assignment_epoch: group.assignment_epoch,
        endpoint_epoch: member.endpoint_epoch,
        active: member.active.clone(),
        standby: member.standby.clone(),
    })
}

fn encode(state: &State) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    put_u32(
        &mut bytes,
        u32::try_from(state.groups.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for (name, group) in &state.groups {
        put_str(&mut bytes, name)?;
        bytes.extend_from_slice(&group.epoch.to_be_bytes());
        bytes.extend_from_slice(&group.assignment_epoch.to_be_bytes());
        bytes.extend_from_slice(&group.topology_epoch.to_be_bytes());
        bytes.extend_from_slice(&group.task_count.to_be_bytes());
        put_u32(
            &mut bytes,
            u32::try_from(group.members.len()).map_err(|_| Error::InvalidRequest)?,
        )?;
        for (member_id, member) in &group.members {
            put_str(&mut bytes, member_id)?;
            bytes.extend_from_slice(&member.epoch.to_be_bytes());
            bytes.extend_from_slice(&member.last_seen_ms.to_be_bytes());
            bytes.push(u8::from(member.acked));
            bytes.extend_from_slice(&member.endpoint_epoch.to_be_bytes());
            match &member.instance {
                Some(instance) => {
                    bytes.push(1);
                    put_str(&mut bytes, instance)?;
                }
                None => bytes.push(0),
            }
            put_ids(&mut bytes, &member.active)?;
            put_ids(&mut bytes, &member.standby)?;
        }
    }
    Ok(bytes)
}

fn put_ids(bytes: &mut Vec<u8>, ids: &[i32]) -> Result<(), Error> {
    put_u32(
        bytes,
        u32::try_from(ids.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for id in ids {
        bytes.extend_from_slice(&id.to_be_bytes());
    }
    Ok(())
}

fn decode(mut bytes: &[u8]) -> Result<State, Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let mut state = State {
        groups: BTreeMap::new(),
    };
    let group_count = take_u32(&mut bytes)?;
    for _ in 0..group_count {
        let name = take_str(&mut bytes)?;
        let epoch = take_i32(&mut bytes)?;
        let assignment_epoch = take_i32(&mut bytes)?;
        let topology_epoch = take_i32(&mut bytes)?;
        let task_count = take_i32(&mut bytes)?;
        let member_count = take_u32(&mut bytes)?;
        let mut members = BTreeMap::new();
        for _ in 0..member_count {
            let member_id = take_str(&mut bytes)?;
            let member_epoch = take_i32(&mut bytes)?;
            let last_seen_ms = take_u64(&mut bytes)?;
            let acked_byte = take_u8(&mut bytes)?;
            let endpoint_epoch = take_i32(&mut bytes)?;
            let instance_flag = take_u8(&mut bytes)?;
            if acked_byte > 1 || instance_flag > 1 {
                return Err(Error::InvalidRequest);
            }
            let instance = if instance_flag == 1 {
                Some(take_str(&mut bytes)?)
            } else {
                None
            };
            if members
                .insert(
                    member_id,
                    Member {
                        epoch: member_epoch,
                        last_seen_ms,
                        acked: acked_byte == 1,
                        instance,
                        endpoint_epoch,
                        active: take_ids(&mut bytes)?,
                        standby: take_ids(&mut bytes)?,
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidRequest);
            }
        }
        let group = Group {
            epoch,
            assignment_epoch,
            topology_epoch,
            task_count,
            members,
        };
        validate_recovered(&name, &group)?;
        if state.groups.insert(name, group).is_some() {
            return Err(Error::InvalidRequest);
        }
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(state)
}

fn validate_recovered(name: &str, group: &Group) -> Result<(), Error> {
    if !valid_name(name)
        || group.members.is_empty()
        || group.topology_epoch < 0
        || group.task_count < 1
    {
        return Err(Error::InvalidRequest);
    }
    let mut active = BTreeSet::new();
    for (member_id, member) in &group.members {
        if !valid_name(member_id) || member.instance.as_ref().is_some_and(|id| !valid_name(id)) {
            return Err(Error::InvalidRequest);
        }
        let mut owned = BTreeSet::new();
        for task in &member.active {
            if *task < 0
                || *task >= group.task_count
                || !active.insert(*task)
                || !owned.insert(*task)
            {
                return Err(Error::InvalidRequest);
            }
        }
        for task in &member.standby {
            if *task < 0 || *task >= group.task_count || owned.contains(task) {
                return Err(Error::InvalidRequest);
            }
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

fn take_ids(input: &mut &[u8]) -> Result<Vec<i32>, Error> {
    let count = take_u32(input)?;
    let mut ids = Vec::new();
    for _ in 0..count {
        let id = take_i32(input)?;
        if id < 0 || ids.contains(&id) {
            return Err(Error::InvalidRequest);
        }
        ids.push(id);
    }
    Ok(ids)
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

fn take_u32(input: &mut &[u8]) -> Result<u32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(u32::from_be_bytes(value))
}

fn take_u8(input: &mut &[u8]) -> Result<u8, Error> {
    Ok(take(input, 1)?[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet as Set;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(8192, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(16, 64, 1_000, journal).unwrap_or_else(|_| panic!("streams limits"))
    }

    fn open(name: &str) -> (Groups, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-streams-{name}-{nanos}"));
        let groups = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (groups, path)
    }

    fn join<'a>(id: &'a str, instance: Option<&'a str>) -> Heartbeat<'a> {
        Heartbeat {
            version: SPOKEN_VERSION,
            group_id: "app",
            member_id: id,
            member_epoch: JOIN_GROUP_MEMBER_EPOCH,
            now_ms: 0,
            instance_id: instance,
            endpoint_epoch: 1,
            topology_epoch: Some(3),
            task_count: Some(4),
        }
    }

    #[test]
    fn a_task_past_the_count_is_not_recovered() {
        let mut members = std::collections::BTreeMap::new();
        members.insert(
            "m-1".to_owned(),
            Member {
                epoch: 0,
                last_seen_ms: 0,
                acked: true,
                instance: None,
                endpoint_epoch: 0,
                active: vec![1],
                standby: Vec::new(),
            },
        );
        let mut groups = std::collections::BTreeMap::new();
        groups.insert(
            "g".to_owned(),
            Group {
                epoch: 0,
                assignment_epoch: 0,
                topology_epoch: 1,
                task_count: 1,
                members,
            },
        );
        let state = State { groups };
        let encoded = encode(&state).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(decode(&encoded), Err(Error::InvalidRequest)));
    }

    #[test]
    fn session_timeout_removes_a_silent_streams_member() {
        let (mut groups, _) = open("streams-session");
        let _joined = groups
            .heartbeat(join("a", Some("i-1")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_001).unwrap_or(99), 1);
        assert!(groups.describe("app").is_none());
        let joined = groups
            .heartbeat(join("a", Some("i-1")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let pinged = groups
            .heartbeat(Heartbeat {
                member_epoch: joined.member_epoch,
                now_ms: 1_000,
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_001).unwrap_or(99), 0);
        groups
            .heartbeat(Heartbeat {
                member_epoch: pinged.member_epoch,
                now_ms: 1,
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_500).unwrap_or(99), 0);
        assert_eq!(groups.expire(2_001).unwrap_or(99), 1);
        assert!(groups.describe("app").is_none());
        let joined = groups
            .heartbeat(join("a", Some("i-1")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        groups
            .heartbeat(Heartbeat {
                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                member_id: &joined.member_id,
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(groups.describe("app").is_none());
    }

    #[test]
    fn streams_tasks_stay_disjoint_and_a_bad_topology_stores_nothing() {
        let (mut groups, path) = open("streams");
        assert!(matches!(
            groups.heartbeat(Heartbeat {
                version: 1,
                ..join("a", None)
            }),
            Err(Error::InvalidRequest)
        ));
        let first = groups
            .heartbeat(join("a", Some("i-1")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(first.member_epoch, 0);
        assert_eq!(first.active, vec![0, 1, 2, 3]);
        assert!(first.standby.is_empty());
        let mut other = join("b", None);
        other.topology_epoch = Some(4);
        assert!(matches!(groups.heartbeat(other), Err(Error::Inconsistent)));
        assert_eq!(
            groups
                .describe("app")
                .unwrap_or_else(|| panic!("describe"))
                .members
                .len(),
            1
        );
        let second = groups
            .heartbeat(join("b", None))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(second.assignment_epoch > first.assignment_epoch);
        assert!(matches!(
            groups.heartbeat(Heartbeat {
                member_id: "b",
                member_epoch: STATIC_REJOIN_MEMBER_EPOCH,
                topology_epoch: None,
                task_count: None,
                ..join("b", Some("i-1"))
            }),
            Err(Error::InvalidRequest)
        ));
        assert_eq!(
            groups
                .describe("app")
                .unwrap_or_else(|| panic!("describe"))
                .members
                .len(),
            2
        );
        let described = groups.describe("app").unwrap_or_else(|| panic!("describe"));
        let mut active = Set::new();
        assert_eq!(described.topology_epoch, 3);
        assert_eq!(described.task_count, 4);
        for member in &described.members {
            assert!(member
                .active
                .iter()
                .all(|task| member.standby.iter().all(|standby| standby != task)));
            for task in &member.active {
                assert!(active.insert(*task));
            }
        }
        assert_eq!(active, Set::from([0, 1, 2, 3]));
        let refreshed = groups
            .heartbeat(Heartbeat {
                member_epoch: 0,
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(refreshed.member_epoch, 1);
        assert!(matches!(
            groups.heartbeat(Heartbeat {
                member_epoch: refreshed.member_epoch,
                instance_id: Some("i-2"),
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-2"))
            }),
            Err(Error::FencedInstance)
        ));
        assert!(matches!(
            groups.heartbeat(Heartbeat {
                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                instance_id: Some("i-2"),
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-2"))
            }),
            Err(Error::FencedInstance)
        ));
        assert!(groups
            .describe("app")
            .is_some_and(|view| { view.members.iter().any(|member| member.member_id == "a") }));
        assert!(matches!(
            groups.heartbeat(Heartbeat {
                member_epoch: 0,
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-1"))
            }),
            Err(Error::Fenced)
        ));
        let replaced = groups
            .heartbeat(Heartbeat {
                member_id: "a2",
                member_epoch: STATIC_REJOIN_MEMBER_EPOCH,
                topology_epoch: None,
                task_count: None,
                ..join("a2", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(replaced.member_id, "a2");
        assert_eq!(replaced.member_epoch, 0);
        let described = groups.describe("app").unwrap_or_else(|| panic!("describe"));
        assert!(described
            .members
            .iter()
            .all(|member| member.member_id != "a"));
        let mut active = Set::new();
        for member in &described.members {
            for task in &member.active {
                assert!(active.insert(*task));
            }
        }
        assert_eq!(active.len(), 4);
        drop(groups);
        let restored = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let again = restored
            .describe("app")
            .unwrap_or_else(|| panic!("restored"));
        assert_eq!(again.assignment_epoch, described.assignment_epoch);
        assert_eq!(again.members.len(), described.members.len());
    }

    #[test]
    fn an_older_endpoint_epoch_does_not_replace_a_newer_one() {
        let (mut groups, path) = open("streams-endpoint");
        let joined = groups
            .heartbeat(join("a", Some("i-1")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(joined.endpoint_epoch, 1);
        let advanced = groups
            .heartbeat(Heartbeat {
                member_epoch: joined.member_epoch,
                endpoint_epoch: 5,
                now_ms: 10,
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(advanced.endpoint_epoch, 5);
        let older = groups
            .heartbeat(Heartbeat {
                member_epoch: advanced.member_epoch,
                endpoint_epoch: 2,
                now_ms: 11,
                topology_epoch: None,
                task_count: None,
                ..join("a", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(older.endpoint_epoch, 5);
        let replaced = groups
            .heartbeat(Heartbeat {
                member_id: "a2",
                member_epoch: STATIC_REJOIN_MEMBER_EPOCH,
                endpoint_epoch: 4,
                topology_epoch: None,
                task_count: None,
                ..join("a2", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(replaced.endpoint_epoch, 5);
        let newer = groups
            .heartbeat(Heartbeat {
                member_id: "a3",
                member_epoch: STATIC_REJOIN_MEMBER_EPOCH,
                endpoint_epoch: 9,
                topology_epoch: None,
                task_count: None,
                ..join("a3", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(newer.endpoint_epoch, 9);
        drop(groups);
        let mut restored =
            Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let echoed = restored
            .heartbeat(Heartbeat {
                member_id: "a3",
                member_epoch: newer.member_epoch,
                endpoint_epoch: 1,
                now_ms: 12,
                topology_epoch: None,
                task_count: None,
                ..join("a3", Some("i-1"))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(echoed.endpoint_epoch, 9);
    }

    #[test]
    fn a_group_epoch_at_i32_max_does_not_accept_another_member() {
        let (mut groups, _) = open("epoch-max");
        let joined = groups
            .heartbeat(join("a", Some("i-1")))
            .unwrap_or_else(|error| panic!("{error:?}"));
        groups
            .state
            .groups
            .get_mut("app")
            .unwrap_or_else(|| panic!("group"))
            .epoch = i32::MAX;
        assert!(matches!(
            groups.heartbeat(join("b", Some("i-2"))),
            Err(Error::InvalidRequest)
        ));
        let described = groups.describe("app").unwrap_or_else(|| panic!("describe"));
        assert_eq!(described.group_epoch, i32::MAX);
        assert_eq!(described.members.len(), 1);
        assert_eq!(described.members[0].member_id, joined.member_id);
    }
}
