//! KIP-848 style consumer-group heartbeat and server-side range assignment.
//!
//! The broker owns the assignment. Heartbeat versions 0 and 1 are accepted, and
//! the group negotiates the minimum. A member epoch moves forward only when
//! that member is shown a new assignment. A stale or skipped epoch is fenced.
//! Leaving a member, or replacing a static instance, keeps each partition owned
//! by one member. The journal restores the same epochs after a coordinator
//! restart; an older coordinator epoch cannot mutate the group.
//!
//! This is not the ConsumerGroupHeartbeat or ConsumerGroupDescribe RPC, and it
//! is not pinned to Apache response bytes.

use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLCNS001";

/// Kafka `NOT_COORDINATOR`.
pub const NOT_COORDINATOR: i16 = 16;
/// Kafka `UNKNOWN_MEMBER_ID`.
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Kafka `FENCED_MEMBER_EPOCH`.
pub const FENCED_MEMBER_EPOCH: i16 = 110;
/// Kafka `FENCED_INSTANCE_ID`.
pub const FENCED_INSTANCE_ID: i16 = 82;
/// Kafka `UNSUPPORTED_ASSIGNOR`.
pub const UNSUPPORTED_ASSIGNOR: i16 = 112;
/// Kafka `STALE_MEMBER_EPOCH`.
pub const STALE_MEMBER_EPOCH: i16 = 113;

/// One consumer heartbeat.
#[derive(Debug, Clone, Copy)]
pub struct Heartbeat<'a> {
    /// Coordinator epoch that must match the restored leader.
    pub coordinator_epoch: i64,
    /// Coordinator clock for this heartbeat. Older values do not shorten the session.
    pub now_ms: u64,
    /// Group id.
    pub group: &'a str,
    /// Empty to join. `member_epoch` of `-1` leaves.
    pub member_id: &'a str,
    /// Last epoch returned to this member.
    pub member_epoch: i32,
    /// Static instance id, if any.
    pub instance_id: Option<&'a str>,
    /// Heartbeat version, 0 or 1.
    pub version: i16,
    /// Server assignor. Only `range` is implemented.
    pub assignor: &'a str,
    /// Replacement subscription. `None` keeps the previous subscription.
    ///
    /// A join must name topics. An empty list replaces the subscription with
    /// nothing.
    pub topics: Option<&'a [&'a str]>,
}

/// Assignment returned by a heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assigned {
    /// Member id, allocated on join.
    pub member_id: String,
    /// Epoch the next heartbeat must present.
    pub member_epoch: i32,
    /// Group epoch.
    pub group_epoch: i32,
    /// Minimum heartbeat version of the current members.
    pub version: i16,
    /// Owned partitions by topic.
    pub assignment: BTreeMap<String, Vec<i32>>,
}

/// A member visible to describe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberView {
    /// Member id.
    pub member_id: String,
    /// Current epoch.
    pub member_epoch: i32,
    /// Owned partitions by topic.
    pub assignment: BTreeMap<String, Vec<i32>>,
}

/// Group metadata visible to describe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupView {
    /// Group epoch.
    pub epoch: i32,
    /// Negotiated heartbeat version.
    pub version: i16,
    /// Members and their target assignments.
    pub members: Vec<MemberView>,
}

/// Why a modern-group operation was refused.
#[derive(Debug)]
pub enum Error {
    /// The coordinator epoch does not match this process.
    NotCoordinator,
    /// The member id is not in the group.
    UnknownMember,
    /// The member epoch is older than the epoch last issued.
    Fenced,
    /// The static instance id does not match the joined member.
    FencedInstance,
    /// The member epoch skipped ahead of the issued epoch.
    Stale,
    /// The assignor is not the server range assignor.
    UnsupportedAssignor,
    /// The group, topic, version, or epoch is not usable.
    InvalidRequest,
    /// A failed append left this coordinator unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the groups.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a member-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        match self {
            Self::NotCoordinator => Some(NOT_COORDINATOR),
            Self::UnknownMember => Some(UNKNOWN_MEMBER_ID),
            Self::Fenced => Some(FENCED_MEMBER_EPOCH),
            Self::FencedInstance => Some(FENCED_INSTANCE_ID),
            Self::Stale => Some(STALE_MEMBER_EPOCH),
            Self::UnsupportedAssignor => Some(UNSUPPORTED_ASSIGNOR),
            Self::InvalidRequest | Self::Poisoned | Self::InvalidLimits | Self::Storage(_) => None,
        }
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
    max_topics: usize,
    session_timeout_ms: u64,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive member and topic caps.
    pub fn new(
        max_members: usize,
        max_topics: usize,
        session_timeout_ms: u64,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=10_000).contains(&max_members)
            || !(1..=10_000).contains(&max_topics)
            || session_timeout_ms == 0
            || journal.max_entry_bytes() < 1024
            || journal.max_index_entries() < max_members
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_members,
            max_topics,
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
    version: i16,
    topics: BTreeSet<String>,
    assignment: BTreeMap<String, Vec<i32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Group {
    epoch: i32,
    version: i16,
    members: BTreeMap<String, Member>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    coordinator_epoch: i64,
    next_member: u64,
    topics: BTreeMap<String, i32>,
    groups: BTreeMap<String, Group>,
}

impl State {
    fn new() -> Self {
        Self {
            coordinator_epoch: 0,
            next_member: 1,
            topics: BTreeMap::new(),
            groups: BTreeMap::new(),
        }
    }
}

/// Durable modern consumer-group coordinator.
pub struct Groups {
    limits: Limits,
    state: State,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Groups {
    /// Open or recover the coordinator.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut groups = Self {
            limits,
            state: State::new(),
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
                let decoded = decode(&entry.payload)?;
                accept_consumer_state(&groups.state, &decoded)?;
                groups.state = decoded;
                groups.stored = groups.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(groups)
    }

    /// Declare a topic's partition count. Existing subscriptions are reassigned.
    pub fn set_topic(&mut self, topic: &str, partitions: i32) -> Result<(), Error> {
        self.ready()?;
        if !valid_topic(topic) || !(1..=1024).contains(&partitions) {
            return Err(Error::InvalidRequest);
        }
        if !self.state.topics.contains_key(topic)
            && self.state.topics.len() >= self.limits.max_topics
        {
            return Err(Error::InvalidLimits);
        }
        let mut next = self.state.clone();
        next.topics.insert(topic.to_owned(), partitions);
        recompute_all(&mut next);
        self.commit(next)
    }

    /// Move the coordinator epoch forward. Older epochs cannot heartbeat.
    pub fn advance(&mut self, epoch: i64) -> Result<(), Error> {
        self.ready()?;
        if epoch <= self.state.coordinator_epoch {
            return Err(Error::InvalidRequest);
        }
        let mut next = self.state.clone();
        next.coordinator_epoch = epoch;
        self.commit(next)
    }

    /// Join, refresh, or leave. The returned epoch is required on the next call.
    pub fn heartbeat(&mut self, request: Heartbeat<'_>) -> Result<Assigned, Error> {
        self.ready()?;
        if request.coordinator_epoch != self.state.coordinator_epoch {
            return Err(Error::NotCoordinator);
        }
        if request.version != 0 && request.version != 1 {
            return Err(Error::InvalidRequest);
        }
        if request.assignor != "range" {
            return Err(Error::UnsupportedAssignor);
        }
        if !valid_name(request.group) {
            return Err(Error::InvalidRequest);
        }
        if request.member_epoch == -1 {
            return self.leave(request);
        }
        if request.member_id.is_empty() {
            return self.join(request);
        }
        self.refresh(request)
    }

    /// Current membership and target assignment.
    pub fn describe(&self, group: &str) -> Option<GroupView> {
        let group = self.state.groups.get(group)?;
        let mut members = Vec::new();
        for (member_id, member) in &group.members {
            members.push(MemberView {
                member_id: member_id.clone(),
                member_epoch: member.epoch,
                assignment: member.assignment.clone(),
            });
        }
        Some(GroupView {
            epoch: group.epoch,
            version: group.version,
            members,
        })
    }

    /// Remove members silent longer than the session timeout and reassign the rest.
    pub fn expire(&mut self, now_ms: u64) -> Result<usize, Error> {
        self.ready()?;
        let timeout = self.limits.session_timeout_ms;
        let mut next = self.state.clone();
        let topics = next.topics.clone();
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
                finish_group_with(group, &topics);
            }
        }
        if removed > 0 {
            self.commit(next)?;
        }
        Ok(removed)
    }

    fn join(&mut self, request: Heartbeat<'_>) -> Result<Assigned, Error> {
        if request.member_epoch != 0 {
            return Err(Error::InvalidRequest);
        }
        let Some(names) = request.topics else {
            return Err(Error::InvalidRequest);
        };
        let topics = topic_set(names, &self.state.topics)?;
        let instance = instance_of(request.instance_id)?;
        let mut next = self.state.clone();
        let member_number = next.next_member;
        next.next_member = next.next_member.saturating_add(1);
        let topic_counts = next.topics.clone();
        let group = next
            .groups
            .entry(request.group.to_owned())
            .or_insert(Group {
                epoch: -1,
                version: request.version,
                members: BTreeMap::new(),
            });
        if let Some(instance) = &instance {
            let previous = group
                .members
                .iter()
                .find(|(_, member)| member.instance.as_ref() == Some(instance))
                .map(|(id, _)| id.clone());
            if let Some(previous) = previous {
                group.members.remove(&previous);
            }
        }
        if group.members.len() >= self.limits.max_members {
            return Err(Error::InvalidLimits);
        }
        let member_id = format!("m-{member_number}");
        group.members.insert(
            member_id.clone(),
            Member {
                epoch: 0,
                last_seen_ms: request.now_ms,
                acked: true,
                instance,
                version: request.version,
                topics,
                assignment: BTreeMap::new(),
            },
        );
        group.epoch = advance_epoch(group.epoch)?;
        finish_group_with(group, &topic_counts);
        let assigned = assigned_from(group, &member_id)?;
        self.commit(next)?;
        Ok(assigned)
    }

    fn refresh(&mut self, request: Heartbeat<'_>) -> Result<Assigned, Error> {
        if self.steady_refresh(&request) {
            return self.commit_seen(request);
        }
        let replacement = match request.topics {
            Some(names) => Some(topic_set(names, &self.state.topics)?),
            None => None,
        };
        let instance = instance_of(request.instance_id)?;
        let mut next = self.state.clone();
        let topic_counts = next.topics.clone();
        let Some(group) = next.groups.get_mut(request.group) else {
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
        if member.instance != instance {
            return Err(Error::FencedInstance);
        }
        member.last_seen_ms = later_seen(member.last_seen_ms, request.now_ms);
        let changed = replacement
            .as_ref()
            .is_some_and(|topics| member.topics != *topics)
            || member.version != request.version;
        if let Some(topics) = replacement {
            member.topics = topics;
        }
        member.version = request.version;
        if changed {
            group.epoch = advance_epoch(group.epoch)?;
        }
        finish_group_with(group, &topic_counts);
        let member = group
            .members
            .get_mut(request.member_id)
            .ok_or(Error::UnknownMember)?;
        if !member.acked {
            member.epoch = advance_epoch(member.epoch)?;
            member.acked = true;
        }
        let assigned = assigned_from(group, request.member_id)?;
        self.commit(next)?;
        Ok(assigned)
    }

    fn leave(&mut self, request: Heartbeat<'_>) -> Result<Assigned, Error> {
        if request.member_id.is_empty() {
            return Err(Error::UnknownMember);
        }
        let mut next = self.state.clone();
        let topic_counts = next.topics.clone();
        let (epoch, version, empty) = {
            let Some(group) = next.groups.get_mut(request.group) else {
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
            let epoch = group.epoch;
            let version = group.version;
            let empty = group.members.is_empty();
            if !empty {
                finish_group_with(group, &topic_counts);
            }
            (epoch, version, empty)
        };
        if empty {
            next.groups.remove(request.group);
        }
        self.commit(next)?;
        Ok(Assigned {
            member_id: request.member_id.to_owned(),
            member_epoch: -1,
            group_epoch: epoch,
            version,
            assignment: BTreeMap::new(),
        })
    }

    /// The subscription, version, and instance already match the stored member.
    fn steady_refresh(&self, request: &Heartbeat<'_>) -> bool {
        let Some(group) = self.state.groups.get(request.group) else {
            return false;
        };
        let Some(member) = group.members.get(request.member_id) else {
            return false;
        };
        if request.member_epoch != member.epoch {
            return false;
        }
        match request.instance_id {
            None if member.instance.is_some() => return false,
            Some(value) if !valid_name(value) || member.instance.as_deref() != Some(value) => {
                return false
            }
            _ => {}
        }
        let topics_same = match request.topics {
            None => true,
            Some(names) => same_topic_list(names, &member.topics),
        };
        if !topics_same || member.version != request.version {
            return false;
        }
        let min_version = group
            .members
            .values()
            .map(|member| member.version)
            .min()
            .unwrap_or(0);
        group.version == min_version
    }

    /// Persist a heartbeat that only moves the session clock or acks the current assignment.
    fn commit_seen(&mut self, request: Heartbeat<'_>) -> Result<Assigned, Error> {
        let saved = {
            let group = self
                .state
                .groups
                .get_mut(request.group)
                .ok_or(Error::UnknownMember)?;
            let member = group
                .members
                .get_mut(request.member_id)
                .ok_or(Error::UnknownMember)?;
            let saved = (member.last_seen_ms, member.epoch, member.acked);
            member.last_seen_ms = later_seen(saved.0, request.now_ms);
            if !saved.2 {
                match advance_epoch(saved.1) {
                    Ok(epoch) => {
                        member.epoch = epoch;
                        member.acked = true;
                    }
                    Err(error) => {
                        member.last_seen_ms = saved.0;
                        return Err(error);
                    }
                }
            }
            saved
        };
        if let Err(error) = self.commit_current() {
            self.restore_member(request.group, request.member_id, saved);
            return Err(error);
        }
        let group = self
            .state
            .groups
            .get(request.group)
            .ok_or(Error::UnknownMember)?;
        assigned_from(group, request.member_id)
    }

    fn commit_current(&mut self) -> Result<(), Error> {
        let payload = encode(&self.state)?;
        if payload.len() > self.limits.journal.max_entry_bytes() {
            return Err(Error::InvalidLimits);
        }
        self.persist(&payload)
    }

    fn restore_member(&mut self, group: &str, member_id: &str, saved: (u64, i32, bool)) {
        let Some(group) = self.state.groups.get_mut(group) else {
            return;
        };
        let Some(member) = group.members.get_mut(member_id) else {
            return;
        };
        member.last_seen_ms = saved.0;
        member.epoch = saved.1;
        member.acked = saved.2;
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

fn recompute_all(state: &mut State) {
    let topics = state.topics.clone();
    for group in state.groups.values_mut() {
        apply_topics(group, &topics);
    }
}

fn apply_topics(group: &mut Group, topics: &BTreeMap<String, i32>) {
    let previous: BTreeMap<String, BTreeMap<String, Vec<i32>>> = group
        .members
        .iter()
        .map(|(id, member)| (id.clone(), member.assignment.clone()))
        .collect();
    for member in group.members.values_mut() {
        member.assignment.clear();
    }
    let names: BTreeSet<String> = group
        .members
        .values()
        .flat_map(|member| member.topics.iter().cloned())
        .collect();
    for topic in names {
        let Some(count) = topics.get(&topic).copied() else {
            continue;
        };
        let subscribers: Vec<String> = group
            .members
            .iter()
            .filter(|(_, member)| member.topics.contains(&topic))
            .map(|(id, _)| id.clone())
            .collect();
        assign_topic(group, &topic, count, &subscribers);
    }
    for (id, member) in &mut group.members {
        if previous.get(id) != Some(&member.assignment) {
            member.acked = false;
        }
    }
}

fn assign_topic(group: &mut Group, topic: &str, count: i32, subscribers: &[String]) {
    if subscribers.is_empty() || count <= 0 {
        return;
    }
    let count = usize::try_from(count).unwrap_or(0);
    let base = count / subscribers.len();
    let extra = count % subscribers.len();
    let mut next = 0i32;
    for (index, member_id) in subscribers.iter().enumerate() {
        let share = base + usize::from(index < extra);
        let mut owned = Vec::new();
        for _ in 0..share {
            owned.push(next);
            next = next.saturating_add(1);
        }
        if let Some(member) = group.members.get_mut(member_id) {
            member.assignment.insert(topic.to_owned(), owned);
        }
    }
}

fn accept_consumer_state(previous: &State, next: &State) -> Result<(), Error> {
    if next.coordinator_epoch < previous.coordinator_epoch {
        return Err(Error::InvalidRequest);
    }
    if next.next_member != previous.next_member
        && previous.next_member.checked_add(1) != Some(next.next_member)
    {
        return Err(Error::InvalidRequest);
    }
    if previous
        .topics
        .keys()
        .any(|topic| !next.topics.contains_key(topic))
    {
        return Err(Error::InvalidRequest);
    }
    for (id, group) in &next.groups {
        match previous.groups.get(id) {
            None => {
                if group.epoch != 0 {
                    return Err(Error::InvalidRequest);
                }
            }
            Some(stored) => {
                if group.epoch != stored.epoch && stored.epoch.checked_add(1) != Some(group.epoch) {
                    return Err(Error::InvalidRequest);
                }
                for (member_id, member) in &group.members {
                    if let Some(stored_member) = stored.members.get(member_id) {
                        if member.epoch != stored_member.epoch
                            && stored_member.epoch.checked_add(1) != Some(member.epoch)
                        {
                            return Err(Error::InvalidRequest);
                        }
                    } else if member.epoch != 0 {
                        return Err(Error::InvalidRequest);
                    }
                }
            }
        }
    }
    let mut allocated = None;
    if next.next_member != previous.next_member {
        allocated = Some(format!("m-{}", previous.next_member));
    }
    let mut found = allocated.is_none();
    for (id, group) in &next.groups {
        for member_id in group.members.keys() {
            let known = previous
                .groups
                .get(id)
                .is_some_and(|stored| stored.members.contains_key(member_id));
            if known {
                continue;
            }
            if allocated.as_deref() != Some(member_id.as_str()) || found {
                return Err(Error::InvalidRequest);
            }
            found = true;
        }
    }
    if !found {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}

fn advance_epoch(epoch: i32) -> Result<i32, Error> {
    epoch.checked_add(1).ok_or(Error::InvalidRequest)
}

fn finish_group_with(group: &mut Group, topics: &BTreeMap<String, i32>) {
    apply_topics(group, topics);
    group.version = group
        .members
        .values()
        .map(|member| member.version)
        .min()
        .unwrap_or(0);
}

fn later_seen(current: u64, now_ms: u64) -> u64 {
    current.max(now_ms)
}

fn claim_partition<'a>(
    seen: &mut HashMap<&'a str, HashSet<i32>>,
    topic: &'a str,
    partition: i32,
) -> bool {
    if let Some(parts) = seen.get_mut(topic) {
        return parts.insert(partition);
    }
    let mut parts = HashSet::new();
    let _ = parts.insert(partition);
    let _ = seen.insert(topic, parts);
    true
}

fn same_topic_list(names: &[&str], owned: &BTreeSet<String>) -> bool {
    if names.len() != owned.len() {
        return false;
    }
    if names.len() <= 8 {
        return small_topic_list(names, owned);
    }
    let mut seen = HashSet::with_capacity(names.len());
    for name in names {
        if !seen.insert(*name) {
            return false;
        }
    }
    owned.iter().all(|stored| seen.contains(stored.as_str()))
}

fn small_topic_list(names: &[&str], owned: &BTreeSet<String>) -> bool {
    let mut matched = 0usize;
    for stored in owned {
        let mut hits = 0usize;
        for name in names {
            if *name == stored.as_str() {
                hits = hits.saturating_add(1);
            }
        }
        if hits != 1 {
            return false;
        }
        matched = matched.saturating_add(1);
    }
    matched == names.len()
}

fn topic_set(topics: &[&str], known: &BTreeMap<String, i32>) -> Result<BTreeSet<String>, Error> {
    let mut set = BTreeSet::new();
    for topic in topics {
        if !valid_topic(topic) || !known.contains_key(*topic) || !set.insert((*topic).to_owned()) {
            return Err(Error::InvalidRequest);
        }
    }
    Ok(set)
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

fn valid_topic(topic: &str) -> bool {
    crate::catalog::validate_topic_name(topic).is_ok()
}

fn assigned_from(group: &Group, member_id: &str) -> Result<Assigned, Error> {
    let member = group.members.get(member_id).ok_or(Error::UnknownMember)?;
    Ok(Assigned {
        member_id: member_id.to_owned(),
        member_epoch: member.epoch,
        group_epoch: group.epoch,
        version: group.version,
        assignment: member.assignment.clone(),
    })
}

fn encode(state: &State) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.extend_from_slice(&state.coordinator_epoch.to_be_bytes());
    bytes.extend_from_slice(&state.next_member.to_be_bytes());
    put_u32(
        &mut bytes,
        u32::try_from(state.topics.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for (topic, partitions) in &state.topics {
        put_str(&mut bytes, topic)?;
        bytes.extend_from_slice(&partitions.to_be_bytes());
    }
    put_u32(
        &mut bytes,
        u32::try_from(state.groups.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for (name, group) in &state.groups {
        put_str(&mut bytes, name)?;
        bytes.extend_from_slice(&group.epoch.to_be_bytes());
        bytes.extend_from_slice(&group.version.to_be_bytes());
        put_u32(
            &mut bytes,
            u32::try_from(group.members.len()).map_err(|_| Error::InvalidRequest)?,
        )?;
        for (member_id, member) in &group.members {
            put_str(&mut bytes, member_id)?;
            bytes.extend_from_slice(&member.epoch.to_be_bytes());
            bytes.extend_from_slice(&member.last_seen_ms.to_be_bytes());
            bytes.push(u8::from(member.acked));
            bytes.extend_from_slice(&member.version.to_be_bytes());
            match &member.instance {
                Some(instance) => {
                    bytes.push(1);
                    put_str(&mut bytes, instance)?;
                }
                None => bytes.push(0),
            }
            put_u32(
                &mut bytes,
                u32::try_from(member.topics.len()).map_err(|_| Error::InvalidRequest)?,
            )?;
            for topic in &member.topics {
                put_str(&mut bytes, topic)?;
            }
            put_u32(
                &mut bytes,
                u32::try_from(member.assignment.len()).map_err(|_| Error::InvalidRequest)?,
            )?;
            for (topic, partitions) in &member.assignment {
                put_str(&mut bytes, topic)?;
                put_u32(
                    &mut bytes,
                    u32::try_from(partitions.len()).map_err(|_| Error::InvalidRequest)?,
                )?;
                for partition in partitions {
                    bytes.extend_from_slice(&partition.to_be_bytes());
                }
            }
        }
    }
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<State, Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let mut state = State {
        coordinator_epoch: take_i64(&mut bytes)?,
        next_member: take_u64(&mut bytes)?,
        topics: BTreeMap::new(),
        groups: BTreeMap::new(),
    };
    let topic_count = take_u32(&mut bytes)?;
    for _ in 0..topic_count {
        let topic = take_str(&mut bytes)?;
        let count = take_i32(&mut bytes)?;
        if !valid_topic(&topic)
            || !(1..=1024).contains(&count)
            || state.topics.insert(topic, count).is_some()
        {
            return Err(Error::InvalidRequest);
        }
    }
    let group_count = take_u32(&mut bytes)?;
    for _ in 0..group_count {
        let name = take_str(&mut bytes)?;
        let epoch = take_i32(&mut bytes)?;
        let version = take_i16(&mut bytes)?;
        let member_count = take_u32(&mut bytes)?;
        let mut members = BTreeMap::new();
        for _ in 0..member_count {
            let member_id = take_str(&mut bytes)?;
            let member_epoch = take_i32(&mut bytes)?;
            let last_seen_ms = take_u64(&mut bytes)?;
            let acked_byte = take_u8(&mut bytes)?;
            let member_version = take_i16(&mut bytes)?;
            let instance_flag = take_u8(&mut bytes)?;
            if acked_byte > 1 || instance_flag > 1 {
                return Err(Error::InvalidRequest);
            }
            let instance = if instance_flag == 1 {
                Some(take_str(&mut bytes)?)
            } else {
                None
            };
            let topic_count = take_u32(&mut bytes)?;
            let mut topics = BTreeSet::new();
            for _ in 0..topic_count {
                topics.insert(take_str(&mut bytes)?);
            }
            let assignment_count = take_u32(&mut bytes)?;
            let mut assignment = BTreeMap::new();
            for _ in 0..assignment_count {
                let topic = take_str(&mut bytes)?;
                let partition_count = take_u32(&mut bytes)?;
                let mut partitions = Vec::new();
                for _ in 0..partition_count {
                    partitions.push(take_i32(&mut bytes)?);
                }
                if assignment.insert(topic, partitions).is_some() {
                    return Err(Error::InvalidRequest);
                }
            }
            if members
                .insert(
                    member_id,
                    Member {
                        epoch: member_epoch,
                        last_seen_ms,
                        acked: acked_byte == 1,
                        instance,
                        version: member_version,
                        topics,
                        assignment,
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidRequest);
            }
        }
        if state
            .groups
            .insert(
                name,
                Group {
                    epoch,
                    version,
                    members,
                },
            )
            .is_some()
        {
            return Err(Error::InvalidRequest);
        }
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    validate_decoded(&state)?;
    Ok(state)
}

fn validate_decoded(state: &State) -> Result<(), Error> {
    for (name, group) in &state.groups {
        if !valid_name(name) || group.members.is_empty() || !(0..=1).contains(&group.version) {
            return Err(Error::InvalidRequest);
        }
        let mut seen: HashMap<&str, HashSet<i32>> = HashMap::new();
        for (member_id, member) in &group.members {
            if !valid_name(member_id) || !(0..=1).contains(&member.version) {
                return Err(Error::InvalidRequest);
            }
            if member.instance.as_ref().is_some_and(|id| !valid_name(id)) {
                return Err(Error::InvalidRequest);
            }
            for topic in &member.topics {
                if !state.topics.contains_key(topic) {
                    return Err(Error::InvalidRequest);
                }
            }
            for (topic, partitions) in &member.assignment {
                let Some(count) = member
                    .topics
                    .contains(topic)
                    .then(|| state.topics.get(topic).copied())
                    .flatten()
                else {
                    return Err(Error::InvalidRequest);
                };
                for partition in partitions {
                    if *partition < 0
                        || *partition >= count
                        || !claim_partition(&mut seen, topic, *partition)
                    {
                        return Err(Error::InvalidRequest);
                    }
                }
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
    let bytes = take(input, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::InvalidRequest)
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

fn take_u32(input: &mut &[u8]) -> Result<u32, Error> {
    let mut value = [0u8; 4];
    value.copy_from_slice(take(input, 4)?);
    Ok(u32::from_be_bytes(value))
}

fn take_i16(input: &mut &[u8]) -> Result<i16, Error> {
    let mut value = [0u8; 2];
    value.copy_from_slice(take(input, 2)?);
    Ok(i16::from_be_bytes(value))
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
        Limits::new(16, 8, 1_000, journal).unwrap_or_else(|_| panic!("group limits"))
    }

    fn open(name: &str) -> (Groups, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-cns-{name}-{nanos}"));
        let groups = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (groups, path)
    }

    fn beat<'a>(
        group: &'a str,
        member: &'a str,
        epoch: i32,
        version: i16,
        topics: &'a [&'a str],
    ) -> Heartbeat<'a> {
        Heartbeat {
            coordinator_epoch: 0,
            now_ms: 0,
            group,
            member_id: member,
            member_epoch: epoch,
            instance_id: None,
            version,
            assignor: "range",
            topics: Some(topics),
        }
    }

    fn owners(view: &GroupView) -> Set<(String, i32)> {
        let mut owned = Set::new();
        for member in &view.members {
            for (topic, partitions) in &member.assignment {
                for partition in partitions {
                    assert!(
                        owned.insert((topic.clone(), *partition)),
                        "overlapping owner"
                    );
                }
            }
        }
        owned
    }

    #[test]
    fn a_topic_list_matches_in_any_order() {
        let short: Set<String> = ["orders", "payments"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert!(same_topic_list(&["payments", "orders"], &short));
        assert!(!same_topic_list(&["orders", "orders"], &short));
        let owned: Set<String> = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let names = ["i", "h", "g", "f", "e", "d", "c", "b", "a"];
        assert!(same_topic_list(&names, &owned));
        let mut repeated = names;
        repeated[0] = "a";
        assert!(!same_topic_list(&repeated, &owned));
        assert!(!same_topic_list(&["a", "b"], &owned));
    }

    #[test]
    fn a_partition_claim_is_once_per_topic_name() {
        let mut seen = HashMap::new();
        assert!(claim_partition(&mut seen, "orders", 0));
        assert!(claim_partition(&mut seen, "orders", 1));
        assert!(!claim_partition(&mut seen, "orders", 0));
        assert!(claim_partition(&mut seen, "payments", 0));
    }

    #[test]
    fn a_group_epoch_jump_is_not_recovered() {
        let (groups, _) = open("jump");
        let mut members = BTreeMap::new();
        members.insert(
            "m-1".to_owned(),
            Member {
                epoch: 0,
                last_seen_ms: 0,
                acked: true,
                instance: None,
                version: 0,
                topics: BTreeSet::new(),
                assignment: BTreeMap::new(),
            },
        );
        let mut stored = BTreeMap::new();
        stored.insert(
            "g".to_owned(),
            Group {
                epoch: 4,
                version: 0,
                members,
            },
        );
        let state = State {
            coordinator_epoch: 0,
            next_member: 2,
            topics: BTreeMap::new(),
            groups: stored,
        };
        assert!(matches!(
            accept_consumer_state(&groups.state, &state),
            Err(Error::InvalidRequest)
        ));
        assert!(groups.state.groups.is_empty());
        assert_eq!(groups.state.next_member, 1);
    }

    #[test]
    fn a_negative_assignment_is_not_recovered() {
        let mut topics = BTreeSet::new();
        topics.insert("orders".to_owned());
        let mut assignment = BTreeMap::new();
        assignment.insert("orders".to_owned(), vec![-1]);
        let mut members = BTreeMap::new();
        members.insert(
            "m-1".to_owned(),
            Member {
                epoch: 0,
                last_seen_ms: 0,
                acked: true,
                instance: None,
                version: 0,
                topics,
                assignment,
            },
        );
        let mut groups = BTreeMap::new();
        groups.insert(
            "g".to_owned(),
            Group {
                epoch: 0,
                version: 0,
                members,
            },
        );
        let mut topic_counts = BTreeMap::new();
        topic_counts.insert("orders".to_owned(), 1);
        let state = State {
            coordinator_epoch: 0,
            next_member: 2,
            topics: topic_counts,
            groups,
        };
        let encoded = encode(&state).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(decode(&encoded), Err(Error::InvalidRequest)));
    }

    #[test]
    fn session_timeout_removes_a_silent_member() {
        let (mut groups, _) = open("session");
        groups
            .set_topic("orders", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let _joined = groups
            .heartbeat(beat("orders-g", "", 0, 1, &["orders"]))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_001).unwrap_or(99), 1);
        assert!(groups.describe("orders-g").is_none());
        let joined = groups
            .heartbeat(beat("orders-g", "", 0, 1, &["orders"]))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut ping = beat(
            "orders-g",
            &joined.member_id,
            joined.member_epoch,
            1,
            &["orders"],
        );
        ping.now_ms = 1_000;
        let pinged = groups
            .heartbeat(ping)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_001).unwrap_or(99), 0);
        let mut older = ping;
        older.member_epoch = pinged.member_epoch;
        older.now_ms = 1;
        groups
            .heartbeat(older)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_500).unwrap_or(99), 0);
        assert_eq!(groups.expire(2_001).unwrap_or(99), 1);
        assert!(groups.describe("orders-g").is_none());
        let joined = groups
            .heartbeat(beat("orders-g", "", 0, 1, &["orders"]))
            .unwrap_or_else(|error| panic!("{error:?}"));
        groups
            .heartbeat(Heartbeat {
                member_epoch: -1,
                member_id: &joined.member_id,
                ..beat("orders-g", &joined.member_id, -1, 1, &["orders"])
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(groups.describe("orders-g").is_none());
    }

    #[test]
    fn unchanged_subscription_keeps_the_assignment() {
        let (mut groups, _path) = open("unchanged");
        groups
            .set_topic("orders", 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let joined = groups
            .heartbeat(beat("orders-g", "", 0, 1, &["orders"]))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut missing = beat("orders-g", "", 0, 1, &["orders"]);
        missing.topics = None;
        missing.group = "other-g";
        assert!(matches!(
            groups.heartbeat(missing),
            Err(Error::InvalidRequest)
        ));
        let acked = groups
            .heartbeat(Heartbeat {
                topics: None,
                ..beat(
                    "orders-g",
                    &joined.member_id,
                    joined.member_epoch,
                    1,
                    &["orders"],
                )
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            acked.assignment.get("orders").map(Vec::as_slice),
            Some(&[0, 1][..])
        );
        let steady = groups
            .heartbeat(Heartbeat {
                topics: None,
                ..beat(
                    "orders-g",
                    &joined.member_id,
                    acked.member_epoch,
                    1,
                    &["orders"],
                )
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(steady.member_epoch, acked.member_epoch);
        assert_eq!(steady.assignment, acked.assignment);
        let repeated = groups
            .heartbeat(beat(
                "orders-g",
                &joined.member_id,
                steady.member_epoch,
                1,
                &["orders"],
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(repeated.member_epoch, steady.member_epoch);
        assert_eq!(repeated.assignment, steady.assignment);
        let cleared = groups
            .heartbeat(beat(
                "orders-g",
                &joined.member_id,
                acked.member_epoch,
                1,
                &[],
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(cleared.assignment.is_empty());
    }

    #[test]
    fn range_assignment_fences_epochs_and_survives_coordinator_failover() {
        let (mut groups, path) = open("modern");
        groups
            .set_topic("orders", 4)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let alice = groups
            .heartbeat(beat("orders-g", "", 0, 1, &["orders"]))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            alice.assignment.get("orders").map(Vec::as_slice),
            Some(&[0, 1, 2, 3][..])
        );
        let bob = groups
            .heartbeat(beat("orders-g", "", 0, 0, &["orders"]))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(bob.version, 0);
        assert_eq!(
            bob.assignment.get("orders").map(Vec::as_slice),
            Some(&[2, 3][..])
        );
        let alice = groups
            .heartbeat(beat(
                "orders-g",
                &alice.member_id,
                alice.member_epoch,
                1,
                &["orders"],
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(alice.member_epoch, 1);
        assert_eq!(
            alice.assignment.get("orders").map(Vec::as_slice),
            Some(&[0, 1][..])
        );
        assert!(matches!(
            groups.heartbeat(beat("orders-g", &alice.member_id, 0, 1, &["orders"])),
            Err(Error::Fenced)
        ));
        assert!(matches!(
            groups.heartbeat(beat("orders-g", &alice.member_id, 9, 1, &["orders"])),
            Err(Error::Stale)
        ));
        let mut bad_version = beat("orders-g", "", 0, 2, &["orders"]);
        bad_version.version = 2;
        assert!(matches!(
            groups.heartbeat(bad_version),
            Err(Error::InvalidRequest)
        ));
        let mut bad_assignor = beat("orders-g", "", 0, 1, &["orders"]);
        bad_assignor.assignor = "sticky";
        assert!(matches!(
            groups.heartbeat(bad_assignor),
            Err(Error::UnsupportedAssignor)
        ));
        groups
            .heartbeat(Heartbeat {
                member_epoch: -1,
                member_id: &bob.member_id,
                ..beat("orders-g", &bob.member_id, -1, 0, &["orders"])
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        let view = groups
            .describe("orders-g")
            .unwrap_or_else(|| panic!("group"));
        assert_eq!(view.version, 1);
        assert_eq!(owners(&view).len(), 4);
        assert!(view
            .members
            .iter()
            .all(|member| member.member_id != bob.member_id));
        drop(groups);
        let mut groups = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let restored = groups
            .describe("orders-g")
            .unwrap_or_else(|| panic!("restored"));
        assert_eq!(owners(&restored), owners(&view));
        groups
            .advance(1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            groups.heartbeat(beat(
                "orders-g",
                &alice.member_id,
                alice.member_epoch,
                1,
                &["orders"]
            )),
            Err(Error::NotCoordinator)
        ));
        let mut current = beat(
            "orders-g",
            &alice.member_id,
            alice.member_epoch,
            1,
            &["orders"],
        );
        current.coordinator_epoch = 1;
        let refreshed = groups
            .heartbeat(current)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            refreshed.assignment.get("orders").map(Vec::as_slice),
            Some(&[0, 1, 2, 3][..])
        );
        let mut static_join = beat("orders-g", "", 0, 1, &["orders"]);
        static_join.coordinator_epoch = 1;
        static_join.instance_id = Some("i-1");
        let first = groups
            .heartbeat(static_join)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut replaced = static_join;
        replaced.member_id = "";
        let second = groups
            .heartbeat(replaced)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_ne!(first.member_id, second.member_id);
        let view = groups
            .describe("orders-g")
            .unwrap_or_else(|| panic!("group"));
        assert_eq!(
            view.members
                .iter()
                .filter(|member| member.member_id == first.member_id)
                .count(),
            0
        );
        assert!(owners(&view).len() <= 4);
        assert_eq!(owners(&view).len(), 4);
        let mut wrong_instance = beat(
            "orders-g",
            &second.member_id,
            second.member_epoch,
            1,
            &["orders"],
        );
        wrong_instance.coordinator_epoch = 1;
        wrong_instance.instance_id = Some("i-2");
        assert!(matches!(
            groups.heartbeat(wrong_instance),
            Err(Error::FencedInstance)
        ));
        let mut wrong_leave = beat("orders-g", &second.member_id, -1, 1, &["orders"]);
        wrong_leave.coordinator_epoch = 1;
        wrong_leave.member_epoch = -1;
        wrong_leave.instance_id = Some("i-2");
        assert!(matches!(
            groups.heartbeat(wrong_leave),
            Err(Error::FencedInstance)
        ));
        assert!(groups.describe("orders-g").is_some_and(|view| {
            view.members
                .iter()
                .any(|member| member.member_id == second.member_id)
        }));
        let mut leave = beat("orders-g", &second.member_id, -1, 1, &["orders"]);
        leave.coordinator_epoch = 1;
        leave.member_epoch = -1;
        leave.instance_id = Some("i-1");
        groups
            .heartbeat(leave)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(groups.describe("orders-g").is_some_and(|view| {
            view.members
                .iter()
                .all(|member| member.member_id != second.member_id)
        }));
    }

    #[test]
    fn a_group_epoch_at_i32_max_does_not_accept_another_member() {
        let (mut groups, _) = open("epoch-max");
        groups
            .set_topic("orders", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let joined = groups
            .heartbeat(beat("orders-g", "", 0, 1, &["orders"]))
            .unwrap_or_else(|error| panic!("{error:?}"));
        groups
            .state
            .groups
            .get_mut("orders-g")
            .unwrap_or_else(|| panic!("group"))
            .epoch = i32::MAX;
        assert!(matches!(
            groups.heartbeat(beat("orders-g", "", 0, 1, &["orders"])),
            Err(Error::InvalidRequest)
        ));
        let view = groups
            .describe("orders-g")
            .unwrap_or_else(|| panic!("describe"));
        assert_eq!(view.epoch, i32::MAX);
        assert_eq!(view.members.len(), 1);
        assert_eq!(view.members[0].member_id, joined.member_id);
    }

    #[test]
    fn an_illegal_topic_name_is_not_declared() {
        let (mut groups, _) = open("bad-topic");
        assert!(matches!(
            groups.set_topic("bad name", 1),
            Err(Error::InvalidRequest)
        ));
        assert!(matches!(
            groups.set_topic(".", 1),
            Err(Error::InvalidRequest)
        ));
        assert!(groups.state.topics.is_empty());
        groups
            .set_topic("orders", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(groups.state.topics.contains_key("orders"));
        assert!(matches!(
            groups.heartbeat(beat("g", "", 0, 1, &["bad name"])),
            Err(Error::InvalidRequest)
        ));
    }
}
