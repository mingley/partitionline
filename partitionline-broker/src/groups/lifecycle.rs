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
const CHECKPOINT_MAGIC: &[u8; 8] = b"PLGLCP01";

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
    /// A persistence failure requires reopening before reads or writes.
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
    /// Reused journal image. Cleared and rewritten on each commit.
    encoded: Vec<u8>,
}

impl Lifecycle {
    /// Open or recover a group lifecycle.
    ///
    /// The adjacent `<path>.checkpoint.tmp` name is reserved for atomic
    /// checkpoint publication and must not be used by other applications.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut life = Self {
            limits,
            members: BTreeMap::new(),
            journal,
            stored: 0,
            poisoned: false,
            encoded: Vec::new(),
        };
        let mut offset = life.journal.base_offset();
        loop {
            let batch = life.journal.fetch(
                offset,
                16.min(life.limits.journal.max_index_entries()),
                life.limits.journal.max_fetch_bytes(),
            )?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                let checkpoint = entry.payload.get(..8) == Some(CHECKPOINT_MAGIC.as_slice());
                let decoded = decode(&entry.payload)?;
                validate_lifecycle_state(&decoded, life.limits)?;
                if checkpoint {
                    if entry.first_offset != life.journal.base_offset() || life.stored != 0 {
                        return Err(Error::InvalidRequest);
                    }
                } else {
                    accept_lifecycle(&life.members, &decoded, life.limits)?;
                }
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
    pub fn owned(&self, member: &str) -> Result<Option<&BTreeSet<i32>>, Error> {
        self.ready()?;
        Ok(self.members.get(member).map(|member| &member.owned))
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
        if overlaps_existing(&self.members, partitions) {
            return Err(Error::InvalidRequest);
        }
        let mut owned = BTreeSet::new();
        for partition in partitions {
            owned.insert(*partition);
        }
        if owned.len() > usize::from(u16::MAX) {
            return Err(Error::InvalidLimits);
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
        self.ready()?;
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
        let mut payload = std::mem::take(&mut self.encoded);
        let result = (|| {
            let length = encoded_len(&self.members)?;
            if length > self.limits.journal.max_entry_bytes()
                || length
                    .checked_add(std::mem::size_of::<journal::Entry>())
                    .is_none_or(|size| size > self.limits.journal.max_fetch_bytes())
            {
                return Err(Error::InvalidLimits);
            }
            payload.clear();
            payload
                .try_reserve_exact(length)
                .map_err(|_| Error::InvalidLimits)?;
            encode_into(&self.members, &mut payload)?;
            debug_assert_eq!(payload.len(), length);
            let rollover = self.stored >= self.limits.max_records
                || !self.journal.can_append_snapshot(payload.len());
            if rollover {
                payload[..8].copy_from_slice(CHECKPOINT_MAGIC);
                let result = self.journal.replace_with_snapshot(&payload);
                payload[..8].copy_from_slice(RECORD_MAGIC);
                result?;
                self.stored = 1;
            } else {
                self.journal.append(1, &payload)?;
                self.stored = self.stored.saturating_add(1);
            }
            Ok(())
        })();
        self.encoded = payload;
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
}

fn validate_lifecycle_state(
    members: &BTreeMap<String, Member>,
    limits: Limits,
) -> Result<(), Error> {
    if members.len() > limits.max_members {
        return Err(Error::InvalidLimits);
    }
    let mut instances = BTreeSet::new();
    for member in members.values() {
        if member
            .instance_id
            .as_ref()
            .is_some_and(|id| !instances.insert(id.as_str()))
            || member
                .rebalance_deadline_ms
                .is_some_and(|deadline| deadline < member.last_heartbeat_ms)
        {
            return Err(Error::InvalidRequest);
        }
    }
    Ok(())
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

fn overlaps_existing(members: &BTreeMap<String, Member>, partitions: &[i32]) -> bool {
    let mut taken = BTreeSet::new();
    for member in members.values() {
        taken.extend(member.owned.iter().copied());
    }
    partitions.iter().any(|partition| taken.contains(partition))
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
        && (new.rebalance_deadline_ms.is_none()
            || (new.last_heartbeat_ms == old.last_heartbeat_ms
                && new.rebalance_deadline_ms == old.rebalance_deadline_ms))
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

#[cfg(test)]
fn encode(members: &BTreeMap<String, Member>) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    encode_into(members, &mut bytes)?;
    Ok(bytes)
}

fn encoded_len(members: &BTreeMap<String, Member>) -> Result<usize, Error> {
    let _ = u16::try_from(members.len()).map_err(|_| Error::InvalidRequest)?;
    let mut len = RECORD_MAGIC.len() + 2;
    for (id, member) in members {
        let _ = u16::try_from(id.len()).map_err(|_| Error::InvalidRequest)?;
        let instance_len = if let Some(instance) = &member.instance_id {
            let _ = u16::try_from(instance.len()).map_err(|_| Error::InvalidRequest)?;
            2 + instance.len()
        } else {
            0
        };
        let _ = u16::try_from(member.owned.len()).map_err(|_| Error::InvalidRequest)?;
        let _ = u16::try_from(member.delivered.len()).map_err(|_| Error::InvalidRequest)?;
        let owned_bytes = member
            .owned
            .len()
            .checked_mul(4)
            .ok_or(Error::InvalidLimits)?;
        let delivered_bytes = member
            .delivered
            .len()
            .checked_mul(4)
            .ok_or(Error::InvalidLimits)?;
        let deadline_bytes = if member.rebalance_deadline_ms.is_some() {
            8
        } else {
            0
        };
        len = len
            .checked_add(2 + id.len() + 1 + instance_len + 8 + 1 + deadline_bytes + 2 + 2)
            .and_then(|len| len.checked_add(owned_bytes))
            .and_then(|len| len.checked_add(delivered_bytes))
            .ok_or(Error::InvalidLimits)?;
    }
    Ok(len)
}

fn encode_into(members: &BTreeMap<String, Member>, bytes: &mut Vec<u8>) -> Result<(), Error> {
    bytes.clear();
    bytes.extend_from_slice(RECORD_MAGIC);
    let count = u16::try_from(members.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (id, member) in members {
        put_str(bytes, id)?;
        match &member.instance_id {
            Some(instance) => {
                bytes.push(1);
                put_str(bytes, instance)?;
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
        put_set(bytes, &member.owned)?;
        put_set(bytes, &member.delivered)?;
    }
    Ok(())
}

fn decode(mut bytes: &[u8]) -> Result<BTreeMap<String, Member>, Error> {
    if bytes.len() < RECORD_MAGIC.len()
        || (&bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC
            && &bytes[..RECORD_MAGIC.len()] != CHECKPOINT_MAGIC)
    {
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
    if len > input.len() / 4 {
        return Err(Error::InvalidRequest);
    }
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
    fn checking_many_owned_partitions_does_not_hold_the_caller() {
        // 1_000 members and 20_000 new partitions: scanning every member for
        // each partition held the caller 1.28s.
        let mut members = BTreeMap::new();
        for member in 0..1_000 {
            let mut owned = BTreeSet::new();
            owned.insert(member);
            members.insert(
                format!("m{member}"),
                Member {
                    instance_id: None,
                    owned,
                    delivered: BTreeSet::new(),
                    last_heartbeat_ms: 0,
                    rebalance_deadline_ms: None,
                },
            );
        }
        let partitions: Vec<i32> = (1_000..21_000).collect();
        let started = std::time::Instant::now();
        let overlap = overlaps_existing(&members, &partitions);
        let elapsed = started.elapsed();
        assert!(!overlap);
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "checking owned partitions held the caller for {elapsed:?}"
        );
        assert!(overlaps_existing(&members, &[0, 20_000]));
        assert!(!overlaps_existing(&members, &[20_000]));
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
        assert_eq!(
            life.owned("a").unwrap().map(|set| set.contains(&0)),
            Some(true)
        );
        life.join("b", None, &[2], 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        life.heartbeat("a", 500)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let encoded_capacity = life.encoded.capacity();
        assert!(encoded_capacity > 0);
        life.heartbeat("b", 500)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(life.encoded.capacity() >= encoded_capacity);
        life.heartbeat("a", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(life
            .expire(1_001)
            .unwrap_or_else(|error| panic!("{error:?}"))
            .is_empty());
        let left = life.leave("b").unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(left, vec![2]);
        assert!(life.owned("b").unwrap().is_none());
        let expired = life
            .expire(1_501)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(expired, vec!["a".to_owned()]);
        assert!(life.owned("a").unwrap().is_none());
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
        assert_eq!(
            life.owned("a").unwrap().map(|set| set.contains(&1)),
            Some(false)
        );
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
        assert_eq!(
            life.owned("b").unwrap().map(|set| set.contains(&0)),
            Some(true)
        );
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
        assert_eq!(
            life.owned("other").unwrap().map(|set| set.contains(&2)),
            Some(true)
        );
        assert!(life.owned("old").unwrap().is_some());
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
        assert!(life.owned("old").unwrap().is_none());
        assert_eq!(
            life.owned("new").unwrap().map(|set| set.contains(&0)),
            Some(true)
        );
        assert_eq!(
            life.owned("new").unwrap().map(|set| set.contains(&1)),
            Some(false)
        );
        life.commit("new", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        drop(life);
        let restored = Lifecycle::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(restored.commit("new", 1), Err(Error::Undelivered)));
        restored
            .commit("new", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
    #[test]
    fn static_replacement_with_older_clock_preserves_rebalance_deadline_on_recovery() {
        for max_records in [1, 32] {
            let journal = journal::Limits::new(4096, 1 << 20, 32, 1 << 20).unwrap();
            let limits = Limits::new(8, 1_000, 500, max_records, journal).unwrap();
            let (_, path) = open("static-older-clock");
            let mut life = Lifecycle::open(&path, limits).unwrap();
            life.join("old", Some("instance"), &[0, 1], 100).unwrap();
            life.mark_delivered("old", 0).unwrap();
            life.revoke("old", &[1], 100).unwrap();
            life.join("new", Some("instance"), &[], 0).unwrap();
            drop(life);
            let mut life = Lifecycle::open(&path, limits).unwrap();
            assert!(life.owned("old").unwrap().is_none());
            assert_eq!(life.owned("new").unwrap().unwrap(), &BTreeSet::from([0]));
            life.commit("new", 0).unwrap();
            assert!(life.expire(600).unwrap().is_empty());
            assert_eq!(life.expire(601).unwrap(), vec!["new".to_owned()]);
            drop(life);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn lifecycle_checkpoint_preserves_ownership_and_delivery_across_reopens() {
        for (file_bytes, index_entries, max_records) in
            [(1 << 20, 2, 2), (1 << 20, 100, 2), (512, 100, 100)]
        {
            let journal = journal::Limits::new(4096, file_bytes, index_entries, 1 << 20).unwrap();
            let limits = Limits::new(8, 1_000, 500, max_records, journal).unwrap();
            let (_, path) = open("lifecycle-checkpoint");
            let mut life = Lifecycle::open(&path, limits).unwrap();
            life.join("old", Some("static-a"), &[0, 1], 0).unwrap();
            life.join("other", None, &[2], 0).unwrap();
            life.mark_delivered("old", 0).unwrap();
            life.mark_delivered("other", 2).unwrap();
            life.revoke("old", &[1], 0).unwrap();
            life.join("new", Some("static-a"), &[], 1).unwrap();
            for now in 2..=26 {
                life.heartbeat("new", now).unwrap();
                life.heartbeat("other", now).unwrap();
                assert!(life.journal.entry_count() <= index_entries.min(max_records));
                assert!(life.journal.file_bytes() <= file_bytes);
                if now % 5 == 1 {
                    drop(life);
                    life = Lifecycle::open(&path, limits).unwrap();
                    assert!(life.owned("old").unwrap().is_none());
                    assert_eq!(life.owned("new").unwrap().unwrap(), &BTreeSet::from([0]));
                    life.commit("new", 0).unwrap();
                    life.commit("other", 2).unwrap();
                    assert!(matches!(life.commit("new", 1), Err(Error::Undelivered)));
                }
            }
            life.leave("other").unwrap();
            drop(life);
            let life = Lifecycle::open(&path, limits).unwrap();
            assert!(life.owned("other").unwrap().is_none());
            life.commit("new", 0).unwrap();
            drop(life);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn checkpoint_recovery_rejects_late_or_invalid_state() {
        let (mut life, path) = open("checkpoint-position");
        life.join("one", Some("i1"), &[0], 100).unwrap();
        let mut image = encode(&life.members).unwrap();
        image[..8].copy_from_slice(CHECKPOINT_MAGIC);
        life.journal.append(1, &image).unwrap();
        drop(life);
        assert!(matches!(
            Lifecycle::open(&path, limits()),
            Err(Error::InvalidRequest)
        ));
        let _ = std::fs::remove_file(path);
        for invalid in ["instance", "deadline", "delivery"] {
            let (mut life, path) = open("checkpoint-invalid");
            life.join("one", Some("i1"), &[0], 100).unwrap();
            life.join("two", Some("i2"), &[1], 100).unwrap();
            match invalid {
                "instance" => {
                    life.members.get_mut("two").unwrap().instance_id = Some("i1".to_owned())
                }
                "deadline" => life.members.get_mut("one").unwrap().rebalance_deadline_ms = Some(99),
                "delivery" => {
                    life.members.get_mut("one").unwrap().delivered.insert(1);
                }
                _ => unreachable!(),
            }
            let mut image = encode(&life.members).unwrap();
            image[..8].copy_from_slice(CHECKPOINT_MAGIC);
            life.journal.replace_with_snapshot(&image).unwrap();
            drop(life);
            assert!(
                matches!(Lifecycle::open(&path, limits()), Err(Error::InvalidRequest)),
                "{invalid}"
            );
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn failed_delivery_checkpoint_blocks_commit_and_ownership_reads() {
        for phase in ["prepared", "renamed"] {
            let journal = journal::Limits::new(4096, 1 << 20, 1, 1 << 20).unwrap();
            let limits = Limits::new(8, 1_000, 500, 1, journal).unwrap();
            let (_, path) = open("lifecycle-checkpoint-failure");
            let mut life = Lifecycle::open(&path, limits).unwrap();
            life.join("one", None, &[0], 0).unwrap();
            journal::fail_checkpoint_once_at(phase);
            assert!(matches!(
                life.mark_delivered("one", 0),
                Err(Error::Storage(_))
            ));
            assert!(matches!(life.owned("one"), Err(Error::Poisoned)));
            assert!(matches!(life.commit("one", 0), Err(Error::Poisoned)));
            assert!(matches!(life.heartbeat("one", 1), Err(Error::Poisoned)));
            drop(life);
            let mut life = Lifecycle::open(&path, limits).unwrap();
            assert_eq!(life.commit("one", 0).is_ok(), phase == "renamed");
            life.mark_delivered("one", 0).unwrap();
            drop(life);
            let life = Lifecycle::open(&path, limits).unwrap();
            life.commit("one", 0).unwrap();
            drop(life);
            let _ = std::fs::remove_file(format!("{}.checkpoint.tmp", path.display()));
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn failed_revocation_checkpoint_blocks_commit_until_recovery() {
        for phase in ["prepared", "renamed"] {
            let journal = journal::Limits::new(4096, 1 << 20, 1, 1 << 20).unwrap();
            let limits = Limits::new(8, 1_000, 500, 1, journal).unwrap();
            let (_, path) = open("lifecycle-revoke-failure");
            let mut life = Lifecycle::open(&path, limits).unwrap();
            life.join("one", None, &[0], 0).unwrap();
            life.mark_delivered("one", 0).unwrap();
            journal::fail_checkpoint_once_at(phase);
            assert!(matches!(
                life.revoke("one", &[0], 1),
                Err(Error::Storage(_))
            ));
            assert!(matches!(life.owned("one"), Err(Error::Poisoned)));
            assert!(matches!(life.commit("one", 0), Err(Error::Poisoned)));
            drop(life);
            let life = Lifecycle::open(&path, limits).unwrap();
            assert_eq!(life.commit("one", 0).is_ok(), phase == "prepared");
            drop(life);
            let _ = std::fs::remove_file(format!("{}.checkpoint.tmp", path.display()));
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn oversized_snapshot_does_not_grow_output_buffer_or_expose_mutation() {
        let journal = journal::Limits::new(4096, 1 << 20, 1, 1 << 20).unwrap();
        let limits = Limits::new(8, 1_000, 500, 1, journal).unwrap();
        let (_, path) = open("lifecycle-oversize");
        let mut life = Lifecycle::open(&path, limits).unwrap();
        life.join("seed", None, &[], 0).unwrap();
        let old_capacity = life.encoded.capacity();
        let partitions: Vec<i32> = (0..1_024).collect();
        assert!(matches!(
            life.join("huge", None, &partitions, 0),
            Err(Error::InvalidLimits)
        ));
        assert_eq!(life.encoded.capacity(), old_capacity);
        assert!(matches!(life.owned("huge"), Err(Error::Poisoned)));
        drop(life);
        let life = Lifecycle::open(&path, limits).unwrap();
        assert!(life.owned("huge").unwrap().is_none());
        assert!(life.owned("seed").unwrap().is_some());
        drop(life);
        let _ = std::fs::remove_file(path);
    }
}
