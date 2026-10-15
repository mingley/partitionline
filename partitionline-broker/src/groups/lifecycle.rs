//! Group session, leave, and cooperative revocation.
//!
//! A heartbeat refreshes a member within the session timeout. Silence longer
//! than that timeout, or a graceful leave, revokes the member's partitions.
//! A static instance that rejoins replaces the previous member id and keeps
//! the same partitions, so ownership does not overlap. An offset commit is
//! accepted only for a partition the member still owns and has marked
//! delivered. Cooperative revoke drops undelivered partitions before a commit
//! can succeed.
//!
//! Transitions are journaled. This is not the Heartbeat or OffsetCommit RPC.

use crate::journal::{self, Journal};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLGLIF01";

/// Kafka `UNKNOWN_MEMBER_ID`.
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Kafka `REBALANCE_IN_PROGRESS`.
pub const REBALANCE_IN_PROGRESS: i16 = 27;
/// Kafka `FENCED_INSTANCE_ID`.
pub const FENCED_INSTANCE_ID: i16 = 82;

/// Why a lifecycle operation was refused.
#[derive(Debug)]
pub enum Error {
    /// The member is not in the group.
    UnknownMember,
    /// The static instance id belongs to another live member.
    FencedInstance,
    /// The partition is revoked or was never delivered.
    Undelivered,
    /// The member, instance, or partition is not usable.
    InvalidRequest,
    /// A failed append left this group unable to confirm further changes.
    Poisoned,
    /// The limits cannot store the group.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a member-visible refusal. Undelivered commits have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::UnknownMember => UNKNOWN_MEMBER_ID,
            Self::FencedInstance => FENCED_INSTANCE_ID,
            Self::Undelivered
            | Self::InvalidRequest
            | Self::InvalidLimits
            | Self::Poisoned
            | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Session and rebalance budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_members: usize,
    session_timeout_ms: u64,
    rebalance_timeout_ms: u64,
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive member, session, and rebalance budgets.
    pub fn new(
        max_members: usize,
        session_timeout_ms: u64,
        rebalance_timeout_ms: u64,
        max_records: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=1_000).contains(&max_members)
            || session_timeout_ms == 0
            || rebalance_timeout_ms == 0
            || !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < 4096
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_members,
            session_timeout_ms,
            rebalance_timeout_ms,
            max_records,
            journal,
        })
    }

    /// Silence after which a member is expired.
    pub fn session_timeout_ms(self) -> u64 {
        self.session_timeout_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Member {
    instance_id: Option<String>,
    owned: BTreeSet<i32>,
    delivered: BTreeSet<i32>,
    last_heartbeat_ms: u64,
    rebalance_deadline_ms: Option<u64>,
}

/// Durable membership lifecycle for one group.
pub struct Lifecycle {
    limits: Limits,
    members: BTreeMap<String, Member>,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Lifecycle {
    /// Open or recover a group lifecycle.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut life = Self {
            limits,
            members: BTreeMap::new(),
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut offset = life.journal.base_offset();
        loop {
            let batch = life
                .journal
                .fetch(offset, 16, life.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                let decoded = decode(&entry.payload)?;
                accept_lifecycle(&life.members, &decoded, life.limits)?;
                life.members = decoded;
                life.stored = life.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(life)
    }

    /// Partitions currently owned by `member`.
    pub fn owned(&self, member: &str) -> Option<&BTreeSet<i32>> {
        self.members.get(member).map(|member| &member.owned)
    }

    /// Add a member, or replace the previous member id of the same static instance.
    pub fn join(
        &mut self,
        member_id: &str,
        instance_id: Option<&str>,
        partitions: &[i32],
        now_ms: u64,
    ) -> Result<(), Error> {
        self.ready()?;
        validate_name(member_id)?;
        if let Some(instance) = instance_id {
            validate_name(instance)?;
            let previous = self
                .members
                .iter()
                .find(|(_, member)| member.instance_id.as_deref() == Some(instance))
                .map(|(id, member)| (id.clone(), member.clone()));
            if let Some((old_id, mut replacement)) = previous {
                if old_id != member_id {
                    if self.members.contains_key(member_id) {
                        return Err(Error::InvalidRequest);
                    }
                    if now_ms >= replacement.last_heartbeat_ms {
                        replacement.last_heartbeat_ms = now_ms;
                        replacement.rebalance_deadline_ms = None;
                    }
                    self.members.remove(&old_id);
                    self.members.insert(member_id.to_owned(), replacement);
                    self.persist()?;
                } else if now_ms >= replacement.last_heartbeat_ms {
                    let Some(member) = self.members.get_mut(&old_id) else {
                        return Err(Error::UnknownMember);
                    };
                    member.last_heartbeat_ms = now_ms;
                    member.rebalance_deadline_ms = None;
                    self.persist()?;
                }
                return Ok(());
            }
        }
        if let Some(existing) = self.members.get(member_id) {
            let mismatch = match (existing.instance_id.as_deref(), instance_id) {
                (Some(current), Some(requested)) => current != requested,
                (Some(_), None) | (None, Some(_)) => true,
                (None, None) => false,
            };
            return Err(if mismatch {
                Error::FencedInstance
            } else {
                Error::InvalidRequest
            });
        }
        if self.members.len() >= self.limits.max_members {
            return Err(Error::InvalidLimits);
        }
        if partitions.iter().any(|partition| *partition < 0) {
            return Err(Error::InvalidRequest);
        }
        let mut owned = BTreeSet::new();
        for partition in partitions {
            if self
                .members
                .values()
                .any(|member| member.owned.contains(partition))
            {
                return Err(Error::InvalidRequest);
            }
            owned.insert(*partition);
        }
        self.members.insert(
            member_id.to_owned(),
            Member {
                instance_id: instance_id.map(str::to_owned),
                owned,
                delivered: BTreeSet::new(),
                last_heartbeat_ms: now_ms,
                rebalance_deadline_ms: None,
            },
        );
        self.persist()?;
        Ok(())
    }

    /// Refresh the session timer. An older clock does not shorten the session.
    pub fn heartbeat(&mut self, member_id: &str, now_ms: u64) -> Result<(), Error> {
        self.ready()?;
        let Some(member) = self.members.get_mut(member_id) else {
            return Err(Error::UnknownMember);
        };
        if now_ms < member.last_heartbeat_ms {
            return Ok(());
        }
        member.last_heartbeat_ms = now_ms;
        member.rebalance_deadline_ms = None;
        self.persist()?;
        Ok(())
    }

    /// Remove a member immediately and return the partitions that were revoked.
    pub fn leave(&mut self, member_id: &str) -> Result<Vec<i32>, Error> {
        self.ready()?;
        let Some(member) = self.members.remove(member_id) else {
            return Err(Error::UnknownMember);
        };
        let revoked: Vec<i32> = member.owned.into_iter().collect();
        self.persist()?;
        Ok(revoked)
    }

    /// Revoke partitions for a cooperative rebalance. Undelivered ones cannot be committed.
    pub fn revoke(
        &mut self,
        member_id: &str,
        partitions: &[i32],
        now_ms: u64,
    ) -> Result<(), Error> {
        self.ready()?;
        if partitions.iter().any(|partition| *partition < 0) {
            return Err(Error::InvalidRequest);
        }
        let Some(member) = self.members.get_mut(member_id) else {
            return Err(Error::UnknownMember);
        };
        let mut removed = false;
        for partition in partitions {
            if member.owned.remove(partition) {
                removed = true;
            }
            member.delivered.remove(partition);
        }
        if !removed {
            return Ok(());
        }
        let clock = now_ms.max(member.last_heartbeat_ms);
        let deadline = clock.saturating_add(self.limits.rebalance_timeout_ms);
        member.rebalance_deadline_ms = Some(
            member
                .rebalance_deadline_ms
                .map_or(deadline, |current| current.max(deadline)),
        );
        self.persist()?;
        Ok(())
    }

    /// Record that the member received records for a partition it owns.
    pub fn mark_delivered(&mut self, member_id: &str, partition: i32) -> Result<(), Error> {
        self.ready()?;
        let Some(member) = self.members.get_mut(member_id) else {
            return Err(Error::UnknownMember);
        };
        if !member.owned.contains(&partition) {
            return Err(Error::Undelivered);
        }
        member.delivered.insert(partition);
        self.persist()?;
        Ok(())
    }

    /// Commit an offset only after the partition was delivered and is still owned.
    pub fn commit(&self, member_id: &str, partition: i32) -> Result<(), Error> {
        let Some(member) = self.members.get(member_id) else {
            return Err(Error::UnknownMember);
        };
        if !member.owned.contains(&partition) || !member.delivered.contains(&partition) {
            return Err(Error::Undelivered);
        }
        Ok(())
    }

    /// Drop members that missed their session or rebalance budget.
    pub fn expire(&mut self, now_ms: u64) -> Result<Vec<String>, Error> {
        self.ready()?;
        let session = self.limits.session_timeout_ms;
        let stale: Vec<String> = self
            .members
            .iter()
            .filter(|(_, member)| {
                now_ms.saturating_sub(member.last_heartbeat_ms) > session
                    || member
                        .rebalance_deadline_ms
                        .is_some_and(|deadline| now_ms > deadline)
            })
            .map(|(id, _)| id.clone())
            .collect();
        if stale.is_empty() {
            return Ok(stale);
        }
        for id in &stale {
            self.members.remove(id);
        }
        self.persist()?;
        Ok(stale)
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&mut self) -> Result<(), Error> {
        if self.stored >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        let payload = encode(&self.members)?;
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
}

fn accept_lifecycle(
    previous: &BTreeMap<String, Member>,
    next: &BTreeMap<String, Member>,
    limits: Limits,
) -> Result<(), Error> {
    if next.len() > limits.max_members {
        return Err(Error::InvalidLimits);
    }
    let mut added = None;
    let mut removed = Vec::new();
    let mut changed = None;
    let mut extra = false;
    let mut left = previous.iter();
    let mut right = next.iter();
    let mut previous_item = left.next();
    let mut next_item = right.next();
    while previous_item.is_some() || next_item.is_some() {
        match (previous_item, next_item) {
            (Some((left_id, old)), Some((right_id, new))) if left_id == right_id => {
                if old != new {
                    if changed.is_some() {
                        extra = true;
                    }
                    changed = Some((old, new));
                }
                previous_item = left.next();
                next_item = right.next();
            }
            (Some((left_id, _)), Some((right_id, new))) if *left_id > *right_id => {
                if added.is_some() {
                    extra = true;
                }
                added = Some(new);
                next_item = right.next();
            }
            (Some((_, old)), _) => {
                removed.push(old);
                previous_item = left.next();
            }
            (None, Some((_, new))) => {
                if added.is_some() {
                    extra = true;
                }
                added = Some(new);
                next_item = right.next();
            }
            (None, None) => break,
        }
    }
    if extra {
        return Err(Error::InvalidRequest);
    }
    match (added, removed.len(), changed) {
        (None, 0, None) => Ok(()),
        (Some(member), 0, None) => joined_member(member),
        (None, removed, None) if removed > 0 => Ok(()),
        (Some(member), 1, None) => replaced_member(removed[0], member),
        (None, 0, Some((old, new))) => updated_member(old, new, limits),
        _ => Err(Error::InvalidRequest),
    }
}

fn joined_member(member: &Member) -> Result<(), Error> {
    if member.delivered.is_empty() && member.rebalance_deadline_ms.is_none() {
        Ok(())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn replaced_member(old: &Member, new: &Member) -> Result<(), Error> {
    if old.instance_id.is_some()
        && old.instance_id == new.instance_id
        && old.owned == new.owned
        && old.delivered == new.delivered
        && new.last_heartbeat_ms >= old.last_heartbeat_ms
        && new.rebalance_deadline_ms.is_none()
    {
        Ok(())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn updated_member(old: &Member, new: &Member, limits: Limits) -> Result<(), Error> {
    if old.instance_id != new.instance_id || new.last_heartbeat_ms < old.last_heartbeat_ms {
        return Err(Error::InvalidRequest);
    }
    let heartbeat = new.owned == old.owned
        && new.delivered == old.delivered
        && new.rebalance_deadline_ms.is_none();
    let delivered = new.owned == old.owned
        && new.last_heartbeat_ms == old.last_heartbeat_ms
        && new.rebalance_deadline_ms == old.rebalance_deadline_ms
        && old.delivered.is_subset(&new.delivered)
        && new.delivered != old.delivered;
    let minimum = old
        .last_heartbeat_ms
        .saturating_add(limits.rebalance_timeout_ms);
    let floor = old.rebalance_deadline_ms.unwrap_or(0).max(minimum);
    let revoked = new.last_heartbeat_ms == old.last_heartbeat_ms
        && new.owned.is_subset(&old.owned)
        && new.owned != old.owned
        && new.delivered.is_subset(&old.delivered)
        && new
            .rebalance_deadline_ms
            .is_some_and(|deadline| deadline >= floor);
    if heartbeat || delivered || revoked {
        Ok(())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty() || name.len() > 249 || name.chars().any(|c| c.is_control()) {
        Err(Error::InvalidRequest)
    } else {
        Ok(())
    }
}

fn encode(members: &BTreeMap<String, Member>) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    let count = u16::try_from(members.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (id, member) in members {
        put_str(&mut bytes, id)?;
        match &member.instance_id {
            Some(instance) => {
                bytes.push(1);
                put_str(&mut bytes, instance)?;
            }
            None => bytes.push(0),
        }
        bytes.extend_from_slice(&member.last_heartbeat_ms.to_be_bytes());
        match member.rebalance_deadline_ms {
            Some(deadline) => {
                bytes.push(1);
                bytes.extend_from_slice(&deadline.to_be_bytes());
            }
            None => bytes.push(0),
        }
        put_set(&mut bytes, &member.owned)?;
        put_set(&mut bytes, &member.delivered)?;
    }
    Ok(bytes)
}

fn decode(mut bytes: &[u8]) -> Result<BTreeMap<String, Member>, Error> {
    if bytes.len() < RECORD_MAGIC.len() || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    bytes = &bytes[RECORD_MAGIC.len()..];
    let count = usize::from(u16::from_be_bytes(
        take(&mut bytes, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ));
    let mut members = BTreeMap::new();
    for _ in 0..count {
        let id = take_str(&mut bytes)?;
        validate_name(&id)?;
        let instance_flag = take(&mut bytes, 1)?[0];
        if instance_flag > 1 {
            return Err(Error::InvalidRequest);
        }
        let instance_id = if instance_flag == 1 {
            Some(take_str(&mut bytes)?)
        } else {
            None
        };
        if let Some(instance) = &instance_id {
            validate_name(instance)?;
        }
        let last_heartbeat_ms = take_u64(&mut bytes)?;
        let deadline_flag = take(&mut bytes, 1)?[0];
        if deadline_flag > 1 {
            return Err(Error::InvalidRequest);
        }
        let rebalance_deadline_ms = if deadline_flag == 1 {
            Some(take_u64(&mut bytes)?)
        } else {
            None
        };
        let owned = take_set(&mut bytes)?;
        let delivered = take_set(&mut bytes)?;
        if delivered.iter().any(|partition| !owned.contains(partition)) {
            return Err(Error::InvalidRequest);
        }
        if members
            .insert(
                id,
                Member {
                    instance_id,
                    owned,
                    delivered,
                    last_heartbeat_ms,
                    rebalance_deadline_ms,
                },
            )
            .is_some()
        {
            return Err(Error::InvalidRequest);
        }
    }
    let mut seen = BTreeSet::new();
    for member in members.values() {
        for partition in &member.owned {
            if !seen.insert(*partition) {
                return Err(Error::InvalidRequest);
            }
        }
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(members)
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_set(bytes: &mut Vec<u8>, values: &BTreeSet<i32>) -> Result<(), Error> {
    let len = u16::try_from(values.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    for value in values {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
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
    std::str::from_utf8(bytes)
        .map(|text| text.to_owned())
        .map_err(|_| Error::InvalidRequest)
}

fn take_u64(input: &mut &[u8]) -> Result<u64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(u64::from_be_bytes(value))
}

fn take_set(input: &mut &[u8]) -> Result<BTreeSet<i32>, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ));
    let mut values = BTreeSet::new();
    for _ in 0..len {
        let mut raw = [0u8; 4];
        raw.copy_from_slice(take(input, 4)?);
        let value = i32::from_be_bytes(raw);
        if value < 0 || !values.insert(value) {
            return Err(Error::InvalidRequest);
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(4096, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(8, 1_000, 500, 32, journal).unwrap_or_else(|_| panic!("lifecycle limits"))
    }

    fn open(name: &str) -> (Lifecycle, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-life-{name}-{nanos}"));
        let life = Lifecycle::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (life, path)
    }

    #[test]
    fn a_join_that_is_already_delivered_is_not_recovered() {
        let (life, _) = open("jump");
        let mut members = BTreeMap::new();
        let mut delivered = BTreeSet::new();
        delivered.insert(0);
        members.insert(
            "a".to_owned(),
            Member {
                instance_id: None,
                owned: delivered.clone(),
                delivered,
                last_heartbeat_ms: 0,
                rebalance_deadline_ms: None,
            },
        );
        assert!(matches!(
            accept_lifecycle(&life.members, &members, life.limits),
            Err(Error::InvalidRequest)
        ));
        assert!(life.members.is_empty());
    }

    #[test]
    fn a_delivered_partition_that_is_not_owned_is_not_recovered() {
        let mut members = BTreeMap::new();
        members.insert(
            "m-1".to_owned(),
            Member {
                instance_id: None,
                owned: BTreeSet::from([0]),
                delivered: BTreeSet::from([1]),
                last_heartbeat_ms: 0,
                rebalance_deadline_ms: None,
            },
        );
        let encoded = encode(&members).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(decode(&encoded), Err(Error::InvalidRequest)));
        let mut members = BTreeMap::new();
        members.insert(
            "m-1".to_owned(),
            Member {
                instance_id: None,
                owned: BTreeSet::from([-1]),
                delivered: BTreeSet::new(),
                last_heartbeat_ms: 0,
                rebalance_deadline_ms: None,
            },
        );
        let encoded = encode(&members).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(decode(&encoded), Err(Error::InvalidRequest)));
    }

    #[test]
    fn leave_and_expiration_revoke_within_the_session_budget() {
        let (mut life, _) = open("session");
        life.join("a", None, &[0, 1], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            life.join("a", None, &[3], 10),
            Err(Error::InvalidRequest)
        ));
        assert!(matches!(
            life.join("a", Some("i-2"), &[3], 10),
            Err(Error::FencedInstance)
        ));
        assert_eq!(Error::FencedInstance.code(), Some(FENCED_INSTANCE_ID));
        assert_eq!(life.owned("a").map(|set| set.contains(&0)), Some(true));
        life.join("b", None, &[2], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.heartbeat("a", 500)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.heartbeat("b", 500)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.heartbeat("a", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(life
            .expire(1_001)
            .unwrap_or_else(|error| panic!("{error:?}"))
            .is_empty());
        let left = life.leave("b").unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(left, vec![2]);
        assert!(life.owned("b").is_none());
        let expired = life
            .expire(1_501)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(expired, vec!["a".to_owned()]);
        assert!(life.owned("a").is_none());
    }

    #[test]
    fn an_older_revoke_does_not_expire_a_fresh_session() {
        let (mut life, _) = open("revoke-clock");
        life.join("a", None, &[0, 1], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.heartbeat("a", 5_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            life.revoke("a", &[-1], 5_000),
            Err(Error::InvalidRequest)
        ));
        life.revoke("a", &[9], 5_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(life
            .expire(5_501)
            .unwrap_or_else(|error| panic!("{error:?}"))
            .is_empty());
        life.revoke("a", &[1], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(life.owned("a").map(|set| set.contains(&1)), Some(false));
        assert!(life
            .expire(5_001)
            .unwrap_or_else(|error| panic!("{error:?}"))
            .is_empty());
        assert_eq!(
            life.expire(5_501)
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec!["a".to_owned()]
        );
    }

    #[test]
    fn a_same_id_static_rejoin_refreshes_a_newer_session() {
        let (mut life, _) = open("static-clock");
        life.join("a", Some("i-1"), &[0, 1], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.heartbeat("a", 5_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.revoke("a", &[1], 5_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.join("a", Some("i-1"), &[], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            life.expire(5_501)
                .unwrap_or_else(|error| panic!("{error:?}")),
            vec!["a".to_owned()]
        );
        life.join("b", Some("i-2"), &[0, 1], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.heartbeat("b", 5_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.revoke("b", &[1], 5_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.join("b", Some("i-2"), &[], 5_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(life
            .expire(5_501)
            .unwrap_or_else(|error| panic!("{error:?}"))
            .is_empty());
        assert_eq!(life.owned("b").map(|set| set.contains(&0)), Some(true));
    }

    #[test]
    fn static_rejoin_and_cooperative_revoke_do_not_commit_undelivered_records() {
        let (mut life, path) = open("static");
        life.join("old", Some("instance-a"), &[0, 1], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.join("other", Some("instance-b"), &[2], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            life.join("other", Some("instance-a"), &[], 5),
            Err(Error::InvalidRequest)
        ));
        assert_eq!(life.owned("other").map(|set| set.contains(&2)), Some(true));
        assert!(life.owned("old").is_some());
        life.mark_delivered("old", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(life.commit("old", 1), Err(Error::Undelivered)));
        life.commit("old", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.revoke("old", &[1], 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(life.commit("old", 1), Err(Error::Undelivered)));
        life.join("new", Some("instance-a"), &[], 20)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(life.owned("old").is_none());
        assert_eq!(life.owned("new").map(|set| set.contains(&0)), Some(true));
        assert_eq!(life.owned("new").map(|set| set.contains(&1)), Some(false));
        life.commit("new", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        drop(life);
        let restored = Lifecycle::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(restored.commit("new", 1), Err(Error::Undelivered)));
        restored
            .commit("new", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
}
