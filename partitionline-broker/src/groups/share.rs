//! KIP-932 share-group membership.
//!
//! Heartbeat versions 0 and 1 use the same membership fields. Join is member
//! epoch 0 and leave is member epoch -1. The broker owns the range assignment.
//! A member presents the epoch it was last given; an older epoch is fenced and
//! a skipped epoch is stale. A heartbeat that changes the member's version
//! bumps the group epoch. Describe reports the group epoch and the
//! assignment epoch. Reopen restores both.
//!
//! This is not the ShareGroupHeartbeat or ShareGroupDescribe RPC.

use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// Partition assignment shared by a stored member and a heartbeat response.
///
/// A changed heartbeat clones this pointer. The map is copied only when the
/// assignment is rebuilt.
pub type AssignmentMap = Arc<BTreeMap<String, Vec<i32>>>;

const RECORD_MAGIC: &[u8; 8] = b"PLSHR001";

/// Java `ShareGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH`.
pub const JOIN_GROUP_MEMBER_EPOCH: i32 = 0;
/// Java `ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
pub const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;
/// Kafka `UNKNOWN_MEMBER_ID`.
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Kafka `FENCED_MEMBER_EPOCH`.
pub const FENCED_MEMBER_EPOCH: i16 = 110;
/// Kafka `STALE_MEMBER_EPOCH`.
pub const STALE_MEMBER_EPOCH: i16 = 113;

/// One share-group heartbeat.
#[derive(Debug, Clone, Copy)]
pub struct Heartbeat<'a> {
    /// Spoken heartbeat version, 0 or 1.
    pub version: i16,
    /// Group id.
    pub group_id: &'a str,
    /// Client member id. Empty allocates one on join.
    pub member_id: &'a str,
    /// Last issued epoch, [`JOIN_GROUP_MEMBER_EPOCH`], or [`LEAVE_GROUP_MEMBER_EPOCH`].
    pub member_epoch: i32,
    /// Coordinator clock for this heartbeat. Older values do not shorten the session.
    pub now_ms: u64,
    /// Replacement subscription. `None` keeps the previous subscription.
    pub subscribed_topics: Option<&'a [&'a str]>,
}

/// Heartbeat result. `assignment` is `None` when it did not change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResult {
    /// Member id.
    pub member_id: String,
    /// Epoch the next heartbeat must present.
    pub member_epoch: i32,
    /// Group epoch.
    pub group_epoch: i32,
    /// Epoch of the current target assignment.
    pub assignment_epoch: i32,
    /// Negotiated heartbeat version.
    pub version: i16,
    /// Heartbeat interval, in milliseconds.
    pub heartbeat_interval_ms: i32,
    /// New assignment, if it changed.
    pub assignment: Option<AssignmentMap>,
}

/// One described member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberView {
    /// Member id.
    pub member_id: String,
    /// Current epoch.
    pub member_epoch: i32,
    /// Owned partitions.
    pub assignment: AssignmentMap,
}

/// Describe view for one share group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// Minimum heartbeat version of the current members.
    pub version: i16,
    /// Members.
    pub members: Vec<MemberView>,
}

/// Why a share-group operation was refused.
#[derive(Debug)]
pub enum Error {
    /// The member id is not in the group.
    UnknownMember,
    /// The member epoch is older than the issued epoch.
    Fenced,
    /// The member epoch skipped ahead of the issued epoch.
    Stale,
    /// The group, topic, or version is not usable.
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
            Self::UnknownMember => Some(UNKNOWN_MEMBER_ID),
            Self::Fenced => Some(FENCED_MEMBER_EPOCH),
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

/// Share-group bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_members: usize,
    max_topics: usize,
    heartbeat_interval_ms: i32,
    session_timeout_ms: u64,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive member, topic, and heartbeat-interval caps.
    pub fn new(
        max_members: usize,
        max_topics: usize,
        heartbeat_interval_ms: i32,
        session_timeout_ms: u64,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=10_000).contains(&max_members)
            || !(1..=10_000).contains(&max_topics)
            || !(1..=300_000).contains(&heartbeat_interval_ms)
            || session_timeout_ms == 0
            || journal.max_entry_bytes() < 1024
            || journal.max_index_entries() < max_members
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_members,
            max_topics,
            heartbeat_interval_ms,
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
    version: i16,
    topics: BTreeSet<String>,
    assignment: AssignmentMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Group {
    epoch: i32,
    assignment_epoch: i32,
    version: i16,
    members: BTreeMap<String, Member>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    next_member: u64,
    topics: BTreeMap<String, i32>,
    groups: BTreeMap<String, Group>,
}

/// Durable share-group coordinator.
pub struct Groups {
    limits: Limits,
    state: State,
    journal: Journal,
    stored: usize,
    poisoned: bool,
    /// Reused journal image. Cleared and rewritten on each commit.
    encoded: Vec<u8>,
}

impl Groups {
    /// Open or recover share groups.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut groups = Self {
            limits,
            state: State {
                next_member: 1,
                topics: BTreeMap::new(),
                groups: BTreeMap::new(),
            },
            journal,
            stored: 0,
            poisoned: false,
            encoded: Vec::new(),
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
                accept_share_group_state(&groups.state, &decoded)?;
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

    /// Declare a topic's partition count and reassign subscribers.
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
        if let Some(count) = next.topics.get_mut(topic) {
            *count = partitions;
        } else {
            let _ = next.topics.insert(topic.to_owned(), partitions);
        }
        {
            let State { topics, groups, .. } = &mut next;
            for group in groups.values_mut() {
                reassign(group, topics)?;
            }
        }
        self.commit(next)
    }

    /// Join, refresh, or leave.
    pub fn heartbeat(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        self.ready()?;
        if !(0..=1).contains(&request.version) || !valid_name(request.group_id) {
            return Err(Error::InvalidRequest);
        }
        if request.member_epoch == LEAVE_GROUP_MEMBER_EPOCH {
            return self.leave(request);
        }
        let known = self
            .state
            .groups
            .get(request.group_id)
            .is_some_and(|group| group.members.contains_key(request.member_id));
        if request.member_id.is_empty() || !known {
            if request.member_epoch != JOIN_GROUP_MEMBER_EPOCH {
                return Err(if request.member_id.is_empty() {
                    Error::InvalidRequest
                } else {
                    Error::UnknownMember
                });
            }
            return self.join(request);
        }
        self.refresh(request)
    }

    /// Current epochs, negotiated version, and assignments.
    pub fn describe(&self, group_id: &str) -> Option<Description> {
        let group = self.state.groups.get(group_id)?;
        let mut members = Vec::new();
        for (member_id, member) in &group.members {
            members.push(MemberView {
                member_id: member_id.clone(),
                member_epoch: member.epoch,
                assignment: Arc::clone(&member.assignment),
            });
        }
        Some(Description {
            group_epoch: group.epoch,
            assignment_epoch: group.assignment_epoch,
            version: group.version,
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
            let State { topics, groups, .. } = &mut next;
            let Some(group) = groups.get_mut(&id) else {
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
                groups.remove(&id);
            } else if let Some(group) = groups.get_mut(&id) {
                reassign(group, topics)?;
            }
        }
        if removed > 0 {
            self.commit(next)?;
        }
        Ok(removed)
    }

    fn join(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        let Some(names) = request.subscribed_topics else {
            return Err(Error::InvalidRequest);
        };
        let topics = topic_set(names, &self.state.topics)?;
        let mut next = self.state.clone();
        let member_number = next.next_member;
        if request.member_id.is_empty() {
            next.next_member = next.next_member.saturating_add(1);
        }
        let result = {
            let State {
                topics: counts,
                groups,
                ..
            } = &mut next;
            let group = super::occupy(groups, request.group_id, || Group {
                epoch: -1,
                assignment_epoch: -1,
                version: request.version,
                members: BTreeMap::new(),
            })
            .ok_or(Error::InvalidRequest)?;
            let member_id = if request.member_id.is_empty() {
                format!("s-{member_number}")
            } else if group.members.contains_key(request.member_id) {
                return Err(Error::Fenced);
            } else {
                request.member_id.to_owned()
            };
            if group.members.len() >= self.limits.max_members {
                return Err(Error::InvalidLimits);
            }
            group.members.insert(
                member_id.clone(),
                Member {
                    epoch: 0,
                    last_seen_ms: request.now_ms,
                    acked: true,
                    version: request.version,
                    topics,
                    assignment: Arc::new(BTreeMap::new()),
                },
            );
            group.epoch = advance_epoch(group.epoch)?;
            reassign(group, counts)?;
            if let Some(member) = group.members.get_mut(&member_id) {
                member.acked = true;
                member.epoch = 0;
            }
            result_from(group, &member_id, self.limits.heartbeat_interval_ms, true)?
        };
        self.commit(next)?;
        Ok(result)
    }

    fn refresh(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        if self.steady_refresh(&request) {
            return self.commit_seen(request);
        }
        let mut next = self.state.clone();
        let result = {
            let State {
                topics: counts,
                groups,
                ..
            } = &mut next;
            let Some(group) = groups.get_mut(request.group_id) else {
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
            member.last_seen_ms = later_seen(member.last_seen_ms, request.now_ms);
            if let Some(names) = request.subscribed_topics {
                let topics = topic_set(names, counts)?;
                if member.topics != topics {
                    member.topics = topics;
                    group.epoch = advance_epoch(group.epoch)?;
                }
            }
            if member.version != request.version {
                group.epoch = advance_epoch(group.epoch)?;
            }
            member.version = request.version;
            reassign(group, counts)?;
            let changed = if let Some(member) = group.members.get_mut(request.member_id) {
                let changed = !member.acked;
                if changed {
                    member.epoch = advance_epoch(member.epoch)?;
                    member.acked = true;
                }
                changed
            } else {
                return Err(Error::UnknownMember);
            };
            result_from(
                group,
                request.member_id,
                self.limits.heartbeat_interval_ms,
                changed,
            )?
        };
        self.commit(next)?;
        Ok(result)
    }

    fn leave(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        if request.member_id.is_empty() {
            return Err(Error::UnknownMember);
        }
        let mut next = self.state.clone();
        let (epoch, assignment_epoch, version, empty) = {
            let State {
                topics: counts,
                groups,
                ..
            } = &mut next;
            let Some(group) = groups.get_mut(request.group_id) else {
                return Err(Error::UnknownMember);
            };
            if group.members.remove(request.member_id).is_none() {
                return Err(Error::UnknownMember);
            }
            group.epoch = advance_epoch(group.epoch)?;
            let empty = group.members.is_empty();
            if !empty {
                reassign(group, counts)?;
            }
            (group.epoch, group.assignment_epoch, group.version, empty)
        };
        if empty {
            next.groups.remove(request.group_id);
        }
        let result = HeartbeatResult {
            member_id: request.member_id.to_owned(),
            member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
            group_epoch: epoch,
            assignment_epoch,
            version,
            heartbeat_interval_ms: self.limits.heartbeat_interval_ms,
            assignment: Some(Arc::new(BTreeMap::new())),
        };
        self.commit(next)?;
        Ok(result)
    }

    fn steady_refresh(&self, request: &Heartbeat<'_>) -> bool {
        let Some(group) = self.state.groups.get(request.group_id) else {
            return false;
        };
        let Some(member) = group.members.get(request.member_id) else {
            return false;
        };
        if request.member_epoch != member.epoch || member.version != request.version {
            return false;
        }
        let topics_same = match request.subscribed_topics {
            None => true,
            Some(names) => same_topic_list(names, &member.topics),
        };
        if !topics_same {
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

    fn commit_seen(&mut self, request: Heartbeat<'_>) -> Result<HeartbeatResult, Error> {
        let (saved, changed) = {
            let group = self
                .state
                .groups
                .get_mut(request.group_id)
                .ok_or(Error::UnknownMember)?;
            let member = group
                .members
                .get_mut(request.member_id)
                .ok_or(Error::UnknownMember)?;
            let saved = (member.last_seen_ms, member.epoch, member.acked);
            let changed = !member.acked;
            member.last_seen_ms = later_seen(saved.0, request.now_ms);
            if changed {
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
            (saved, changed)
        };
        if let Err(error) = self.commit_current() {
            self.restore_member(request.group_id, request.member_id, saved);
            return Err(error);
        }
        let group = self
            .state
            .groups
            .get(request.group_id)
            .ok_or(Error::UnknownMember)?;
        result_from(
            group,
            request.member_id,
            self.limits.heartbeat_interval_ms,
            changed,
        )
    }

    fn commit_current(&mut self) -> Result<(), Error> {
        let mut encoded = std::mem::take(&mut self.encoded);
        let encoded_result = encode_into(&self.state, &mut encoded);
        self.encoded = encoded;
        encoded_result?;
        self.persist_encoded()
    }

    fn restore_member(&mut self, group_id: &str, member_id: &str, saved: (u64, i32, bool)) {
        let Some(group) = self.state.groups.get_mut(group_id) else {
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
        encode_into(&state, &mut self.encoded)?;
        self.persist_encoded()?;
        self.state = state;
        Ok(())
    }

    fn persist_encoded(&mut self) -> Result<(), Error> {
        if self.encoded.len() > self.limits.journal.max_entry_bytes() {
            return Err(Error::InvalidLimits);
        }
        let payload = std::mem::take(&mut self.encoded);
        let result = self.persist(&payload);
        self.encoded = payload;
        result
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

fn within_steps(previous: i32, next: i32, steps: u32) -> bool {
    let mut current = previous;
    for _ in 0..=steps {
        if current == next {
            return true;
        }
        match current.checked_add(1) {
            Some(advanced) => current = advanced,
            None => return false,
        }
    }
    false
}

fn accept_share_group_state(previous: &State, next: &State) -> Result<(), Error> {
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
                if group.epoch != 0 || !within_steps(-1, group.assignment_epoch, 1) {
                    return Err(Error::InvalidRequest);
                }
            }
            Some(stored) => {
                if !within_steps(stored.epoch, group.epoch, 2)
                    || !within_steps(stored.assignment_epoch, group.assignment_epoch, 1)
                {
                    return Err(Error::InvalidRequest);
                }
                for (member_id, member) in &group.members {
                    if let Some(stored_member) = stored.members.get(member_id) {
                        if !within_steps(stored_member.epoch, member.epoch, 1) {
                            return Err(Error::InvalidRequest);
                        }
                    } else if member.epoch != 0 {
                        return Err(Error::InvalidRequest);
                    }
                }
            }
        }
    }
    let mut added = 0usize;
    for (id, group) in &next.groups {
        for member_id in group.members.keys() {
            let known = previous
                .groups
                .get(id)
                .is_some_and(|stored| stored.members.contains_key(member_id));
            if !known {
                added += 1;
                if next.next_member != previous.next_member
                    && member_id != &format!("s-{}", previous.next_member)
                {
                    return Err(Error::InvalidRequest);
                }
            }
        }
    }
    if added > 1 || (next.next_member != previous.next_member && added != 1) {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}

fn advance_epoch(epoch: i32) -> Result<i32, Error> {
    epoch.checked_add(1).ok_or(Error::InvalidRequest)
}

fn reassign(group: &mut Group, topics: &BTreeMap<String, i32>) -> Result<(), Error> {
    let previous: BTreeMap<String, AssignmentMap> = group
        .members
        .iter()
        .map(|(id, member)| (id.clone(), Arc::clone(&member.assignment)))
        .collect();
    for member in group.members.values_mut() {
        member.assignment = Arc::new(BTreeMap::new());
    }
    for (topic, count) in topics {
        if *count <= 0 {
            continue;
        }
        let subscribers: Vec<String> = group
            .members
            .iter()
            .filter(|(_, member)| member.topics.contains(topic))
            .map(|(id, _)| id.clone())
            .collect();
        assign(group, topic, *count, &subscribers);
    }
    let mut changed = false;
    for (id, member) in &mut group.members {
        if let Some(prior) = previous
            .get(id)
            .filter(|prior| *prior == &member.assignment)
        {
            member.assignment = Arc::clone(prior);
        } else {
            member.acked = false;
            changed = true;
        }
    }
    if changed {
        group.assignment_epoch = advance_epoch(group.assignment_epoch)?;
    }
    group.version = group
        .members
        .values()
        .map(|member| member.version)
        .min()
        .unwrap_or(0);
    Ok(())
}

fn assign(group: &mut Group, topic: &str, count: i32, subscribers: &[String]) {
    if subscribers.is_empty() {
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
            let _ = Arc::make_mut(&mut member.assignment).insert(topic.to_owned(), owned);
        }
    }
}

fn result_from(
    group: &Group,
    member_id: &str,
    interval: i32,
    changed: bool,
) -> Result<HeartbeatResult, Error> {
    let member = group.members.get(member_id).ok_or(Error::UnknownMember)?;
    Ok(HeartbeatResult {
        member_id: member_id.to_owned(),
        member_epoch: member.epoch,
        group_epoch: group.epoch,
        assignment_epoch: group.assignment_epoch,
        version: group.version,
        heartbeat_interval_ms: interval,
        assignment: changed.then(|| Arc::clone(&member.assignment)),
    })
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

fn valid_name(text: &str) -> bool {
    !text.is_empty() && text.len() <= 249 && !text.chars().any(char::is_control)
}

fn valid_topic(topic: &str) -> bool {
    crate::catalog::validate_topic_name(topic).is_ok()
}

#[cfg(test)]
fn encode(state: &State) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    encode_into(state, &mut bytes)?;
    Ok(bytes)
}

fn encode_into(state: &State, bytes: &mut Vec<u8>) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.extend_from_slice(&state.next_member.to_be_bytes());
    put_u32(
        bytes,
        u32::try_from(state.topics.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for (topic, partitions) in &state.topics {
        put_str(bytes, topic)?;
        bytes.extend_from_slice(&partitions.to_be_bytes());
    }
    put_u32(
        bytes,
        u32::try_from(state.groups.len()).map_err(|_| Error::InvalidRequest)?,
    )?;
    for (name, group) in &state.groups {
        put_str(bytes, name)?;
        bytes.extend_from_slice(&group.epoch.to_be_bytes());
        bytes.extend_from_slice(&group.assignment_epoch.to_be_bytes());
        bytes.extend_from_slice(&group.version.to_be_bytes());
        put_u32(
            bytes,
            u32::try_from(group.members.len()).map_err(|_| Error::InvalidRequest)?,
        )?;
        for (member_id, member) in &group.members {
            put_str(bytes, member_id)?;
            bytes.extend_from_slice(&member.epoch.to_be_bytes());
            bytes.extend_from_slice(&member.last_seen_ms.to_be_bytes());
            bytes.push(u8::from(member.acked));
            bytes.extend_from_slice(&member.version.to_be_bytes());
            put_u32(
                bytes,
                u32::try_from(member.topics.len()).map_err(|_| Error::InvalidRequest)?,
            )?;
            for topic in &member.topics {
                put_str(bytes, topic)?;
            }
            put_u32(
                bytes,
                u32::try_from(member.assignment.len()).map_err(|_| Error::InvalidRequest)?,
            )?;
            for (topic, partitions) in member.assignment.iter() {
                put_str(bytes, topic)?;
                put_u32(
                    bytes,
                    u32::try_from(partitions.len()).map_err(|_| Error::InvalidRequest)?,
                )?;
                for partition in partitions {
                    bytes.extend_from_slice(&partition.to_be_bytes());
                }
            }
        }
    }
    Ok(())
}

fn decode(mut bytes: &[u8]) -> Result<State, Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let mut state = State {
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
        let assignment_epoch = take_i32(&mut bytes)?;
        let version = take_i16(&mut bytes)?;
        let member_count = take_u32(&mut bytes)?;
        let mut members = BTreeMap::new();
        for _ in 0..member_count {
            let member_id = take_str(&mut bytes)?;
            let member_epoch = take_i32(&mut bytes)?;
            let last_seen_ms = take_u64(&mut bytes)?;
            let acked_byte = take_u8(&mut bytes)?;
            if acked_byte > 1 {
                return Err(Error::InvalidRequest);
            }
            let member_version = take_i16(&mut bytes)?;
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
                        version: member_version,
                        topics,
                        assignment: Arc::new(assignment),
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
                    assignment_epoch,
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
            for topic in &member.topics {
                if !state.topics.contains_key(topic) {
                    return Err(Error::InvalidRequest);
                }
            }
            for (topic, partitions) in member.assignment.iter() {
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
    String::from_utf8(take(input, len)?.to_vec()).map_err(|_| Error::InvalidRequest)
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
        Limits::new(16, 8, 5_000, 1_000, journal).unwrap_or_else(|_| panic!("share limits"))
    }

    fn open(name: &str) -> (Groups, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-share-{name}-{nanos}"));
        let groups = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (groups, path)
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
    fn a_share_epoch_jump_is_not_recovered() {
        let (groups, _) = open("jump");
        let mut members = BTreeMap::new();
        members.insert(
            "s-1".to_owned(),
            Member {
                epoch: 0,
                last_seen_ms: 0,
                acked: true,
                version: 0,
                topics: BTreeSet::new(),
                assignment: Arc::new(BTreeMap::new()),
            },
        );
        let mut stored = BTreeMap::new();
        stored.insert(
            "g".to_owned(),
            Group {
                epoch: 4,
                assignment_epoch: 0,
                version: 0,
                members,
            },
        );
        let state = State {
            next_member: 2,
            topics: BTreeMap::new(),
            groups: stored,
        };
        assert!(matches!(
            accept_share_group_state(&groups.state, &state),
            Err(Error::InvalidRequest)
        ));
        assert!(groups.state.groups.is_empty());
    }

    #[test]
    fn a_partition_past_the_topic_count_is_not_recovered() {
        let mut topics = BTreeSet::new();
        topics.insert("orders".to_owned());
        let mut assignment = BTreeMap::new();
        assignment.insert("orders".to_owned(), vec![1]);
        let mut members = BTreeMap::new();
        members.insert(
            "m-1".to_owned(),
            Member {
                epoch: 0,
                last_seen_ms: 0,
                acked: true,
                version: 0,
                topics,
                assignment: Arc::new(assignment),
            },
        );
        let mut groups = BTreeMap::new();
        groups.insert(
            "g".to_owned(),
            Group {
                epoch: 0,
                assignment_epoch: 0,
                version: 0,
                members,
            },
        );
        let mut topic_counts = BTreeMap::new();
        topic_counts.insert("orders".to_owned(), 1);
        let state = State {
            next_member: 2,
            topics: topic_counts,
            groups,
        };
        let encoded = encode(&state).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(decode(&encoded), Err(Error::InvalidRequest)));
    }

    #[test]
    fn session_timeout_removes_a_silent_share_member() {
        let (mut groups, _) = open("share-session");
        groups
            .set_topic("orders", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let _joined = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: Some(&["orders"]),
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_001).unwrap_or(99), 1);
        assert!(groups.describe("g").is_none());
        let joined = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: Some(&["orders"]),
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        let pinged = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: &joined.member_id,
                member_epoch: joined.member_epoch,
                now_ms: 1_000,
                subscribed_topics: None,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_001).unwrap_or(99), 0);
        groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: &pinged.member_id,
                member_epoch: pinged.member_epoch,
                now_ms: 1,
                subscribed_topics: None,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(groups.expire(1_500).unwrap_or(99), 0);
        assert_eq!(groups.expire(2_001).unwrap_or(99), 1);
        assert!(groups.describe("g").is_none());
        let joined = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: Some(&["orders"]),
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: &joined.member_id,
                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: None,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(groups.describe("g").is_none());
    }

    #[test]
    fn share_versions_assignment_epoch_and_fence_survive_restart() {
        let (mut groups, path) = open("share");
        groups
            .set_topic("orders", 4)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            groups.heartbeat(Heartbeat {
                version: 2,
                group_id: "g",
                member_id: "a",
                member_epoch: 0,
                now_ms: 0,
                subscribed_topics: Some(&["orders"]),
            }),
            Err(Error::InvalidRequest)
        ));
        let alice = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: Some(&["orders"]),
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(alice.member_epoch, 0);
        assert_eq!(alice.heartbeat_interval_ms, 5_000);
        assert_eq!(
            alice
                .assignment
                .as_ref()
                .and_then(|map| map.get("orders"))
                .map(Vec::as_slice),
            Some(&[0, 1, 2, 3][..])
        );
        let bob = groups
            .heartbeat(Heartbeat {
                version: 0,
                group_id: "g",
                member_id: "bob",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: Some(&["orders"]),
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(bob.version, 0);
        assert!(bob.assignment_epoch > alice.assignment_epoch);
        let alice = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: 0,
                now_ms: 0,
                subscribed_topics: None,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(alice.member_epoch, 1);
        assert_eq!(
            alice
                .assignment
                .as_ref()
                .and_then(|map| map.get("orders"))
                .map(Vec::as_slice),
            Some(&[0, 1][..])
        );
        let described = groups.describe("g").unwrap_or_else(|| panic!("describe"));
        let stored = described
            .members
            .iter()
            .find(|member| member.member_id == "alice")
            .unwrap_or_else(|| panic!("alice"));
        let shared = alice
            .assignment
            .as_ref()
            .unwrap_or_else(|| panic!("assignment"));
        assert!(Arc::ptr_eq(shared, &stored.assignment));
        let encoded_capacity = groups.encoded.capacity();
        assert!(encoded_capacity > 0);
        let steady = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: alice.member_epoch,
                now_ms: 5,
                subscribed_topics: Some(&["orders"]),
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(steady.member_epoch, alice.member_epoch);
        assert!(steady.assignment.is_none());
        assert!(groups.encoded.capacity() >= encoded_capacity);
        assert!(matches!(
            groups.heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: 0,
                now_ms: 0,
                subscribed_topics: None,
            }),
            Err(Error::Fenced)
        ));
        groups
            .heartbeat(Heartbeat {
                version: 0,
                group_id: "g",
                member_id: "bob",
                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: None,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        let described = groups.describe("g").unwrap_or_else(|| panic!("describe"));
        assert_eq!(described.version, 1);
        let mut owned = Set::new();
        for member in &described.members {
            for (topic, partitions) in member.assignment.iter() {
                for partition in partitions {
                    assert!(owned.insert((topic.clone(), *partition)));
                }
            }
        }
        assert_eq!(owned.len(), 4);
        drop(groups);
        let restored = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let again = restored.describe("g").unwrap_or_else(|| panic!("restored"));
        assert_eq!(again.group_epoch, described.group_epoch);
        assert_eq!(again.assignment_epoch, described.assignment_epoch);
        assert_eq!(again.members.len(), 1);
    }

    #[test]
    fn a_share_member_version_change_bumps_the_group_epoch() {
        let (mut groups, path) = open("share-version");
        groups
            .set_topic("orders", 2)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let alice = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: Some(&["orders"]),
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(alice.version, 1);
        let lowered = groups
            .heartbeat(Heartbeat {
                version: 0,
                group_id: "g",
                member_id: &alice.member_id,
                member_epoch: alice.member_epoch,
                now_ms: 1,
                subscribed_topics: None,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(lowered.group_epoch > alice.group_epoch);
        assert_eq!(lowered.version, 0);
        let again = groups
            .heartbeat(Heartbeat {
                version: 0,
                group_id: "g",
                member_id: &alice.member_id,
                member_epoch: lowered.member_epoch,
                now_ms: 2,
                subscribed_topics: None,
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(again.group_epoch, lowered.group_epoch);
        drop(groups);
        let restored = Groups::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let described = restored.describe("g").unwrap_or_else(|| panic!("describe"));
        assert_eq!(described.version, 0);
        assert_eq!(described.group_epoch, lowered.group_epoch);
    }

    #[test]
    fn a_group_epoch_at_i32_max_does_not_accept_another_member() {
        let (mut groups, _) = open("epoch-max");
        groups
            .set_topic("orders", 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let joined = groups
            .heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: Some(&["orders"]),
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
        groups
            .state
            .groups
            .get_mut("g")
            .unwrap_or_else(|| panic!("group"))
            .epoch = i32::MAX;
        assert!(matches!(
            groups.heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "bob",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 1,
                subscribed_topics: Some(&["orders"]),
            }),
            Err(Error::InvalidRequest)
        ));
        let described = groups.describe("g").unwrap_or_else(|| panic!("describe"));
        assert_eq!(described.group_epoch, i32::MAX);
        assert_eq!(described.members.len(), 1);
        assert_eq!(described.members[0].member_id, joined.member_id);
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
            groups.heartbeat(Heartbeat {
                version: 1,
                group_id: "g",
                member_id: "alice",
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                now_ms: 0,
                subscribed_topics: Some(&["bad name"]),
            }),
            Err(Error::InvalidRequest)
        ));
    }
}
