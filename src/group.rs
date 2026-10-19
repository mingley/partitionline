//! Consumer-group join / sync / heartbeat / commit.
mod offsets;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::watch;

use crate::config::{AutoOffsetReset, IsolationLevel};
use crate::consumer::{
    duration_millis_i32, Consumer, ConsumerConfig, ConsumerRecords, LeaderDialFlight,
    OffsetAndMetadata, TopicPartition,
};
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{ApiVersion, ApiVersionsResponse};
use crate::protocol::api_keys::{
    pick_version, CONSUMER_GROUP_HEARTBEAT, FIND_COORDINATOR, HEARTBEAT, JOIN_GROUP, LEAVE_GROUP,
    OFFSET_COMMIT, OFFSET_FETCH, SHARE_GROUP_HEARTBEAT, SYNC_GROUP,
};
use crate::protocol::cgheartbeat::{
    decode_consumer_group_heartbeat_response_cooperative, encode_consumer_group_heartbeat_request,
    encode_consumer_group_heartbeat_request_cooperative, ConsumerGroupHeartbeatRequest,
    TopicPartitions,
};
use crate::protocol::group::{
    decode_assignment_cooperative, decode_find_coordinator_response, decode_heartbeat_response,
    decode_join_group_response_cooperative, decode_leave_group_response_version_cooperative,
    decode_offset_commit_response, decode_offset_commit_response_cooperative,
    decode_offset_fetch_response, decode_offset_fetch_response_cooperative,
    decode_sync_group_response, encode_find_coordinator_request_typed, encode_heartbeat_request,
    encode_join_group_protocols_request, encode_leave_group_request_members,
    encode_offset_commit_request, encode_offset_commit_request_cooperative,
    encode_offset_fetch_request, encode_offset_fetch_request_cooperative,
    encode_sync_group_request, encode_sync_group_request_cooperative,
    encode_tp_assignment_cooperative, BorrowedOffsetPartition, BorrowedOffsetTopic,
    ConsumerProtocol, ConsumerProtocolSubscription, FetchedOffset, FetchedOffsetTopic,
    JoinGroupProtocol, JoinGroupProtocolsRequest, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupMember, OffsetCommitRequestData, OffsetFetchRequestData, OffsetFetchTopic,
    OffsetPartition, OffsetTopic, SyncGroupRequest, Topic, COORDINATOR_GROUP,
    DEFAULT_RETENTION_TIME,
};
use crate::protocol::sasl;

pub use crate::protocol::group::CoordinatorType;

pub(crate) type TopicMatch = Arc<dyn Fn(&str) -> bool + Send + Sync>;

type AsyncOffsetCommitCallback =
    Box<dyn FnOnce(Result<Vec<(TopicPartition, OffsetAndMetadata)>>) + Send>;
type PendingAsyncCommit = (
    Vec<(TopicPartition, OffsetAndMetadata)>,
    Option<AsyncOffsetCommitCallback>,
    DeliveredTopicIds,
);
type DeliveredTopicIds = HashMap<Arc<str>, HashMap<i32, [u8; 16]>>;

/// Java default JoinGroup `Reason` for [`ConsumerGroup::enforce_rebalance`] (KIP-800).
pub const DEFAULT_ENFORCE_REBALANCE_REASON: &str = "rebalance enforced by user";

/// Java LeaveGroup `Reason` on [`ConsumerGroup::leave`] / [`ConsumerGroup::close`] (KIP-800).
pub const LEAVE_GROUP_REASON_CLOSED: &str = "the consumer is being closed";

/// Java LeaveGroup `Reason` on [`ConsumerGroup::unsubscribe`] (KIP-800).
pub const LEAVE_GROUP_REASON_UNSUBSCRIBED: &str = "the consumer unsubscribed from all topics";

/// Java LeaveGroup `Reason` when `max.poll.interval.ms` expires (KIP-800).
pub const LEAVE_GROUP_REASON_POLL_TIMEOUT: &str = "consumer poll timeout has expired.";

/// JoinGroup / LeaveGroup Reason is a STRING truncated to 255 characters (KIP-800).
pub(crate) fn truncate_group_reason(reason: &str) -> String {
    JoinGroupRequest::maybe_truncate_reason(reason)
}

/// Split `partitions` across sorted `members` (Java range assignor).
pub fn assign_range(members: &[String], partitions: &[i32]) -> HashMap<String, Vec<i32>> {
    let members: Vec<&str> = members.iter().map(String::as_str).collect();
    assign_range_names(&members, partitions)
}

fn assign_range_names(members: &[&str], partitions: &[i32]) -> HashMap<String, Vec<i32>> {
    let mut order = members.to_vec();
    order.sort_unstable();
    order.dedup();
    let mut partitions: Vec<i32> = partitions.iter().copied().filter(|p| *p >= 0).collect();
    partitions.sort_unstable();
    partitions.dedup();
    let mut out: HashMap<String, Vec<i32>> = HashMap::new();
    for member in &order {
        let _ = out.insert((*member).to_owned(), Vec::new());
    }
    let n = order.len();
    if n == 0 {
        return out;
    }
    let np = partitions.len();
    let base = np / n;
    let extra = np % n;
    let mut idx = 0usize;
    for (i, member) in order.iter().enumerate() {
        let take = base + usize::from(i < extra);
        for _ in 0..take {
            if let Some(p) = partitions.get(idx) {
                if let Some(slot) = out.get_mut(*member) {
                    slot.push(*p);
                }
            }
            idx = idx.saturating_add(1);
        }
    }
    out
}

/// Keep previous assignments when still valid; fill the rest (sticky).
pub fn assign_sticky(
    members: &[String],
    partitions: &[i32],
    prev: &HashMap<String, Vec<i32>>,
) -> HashMap<String, Vec<i32>> {
    let members: Vec<&str> = members.iter().map(String::as_str).collect();
    assign_sticky_from(
        &members,
        partitions,
        prev.iter()
            .map(|(member, parts)| (member.as_str(), parts.as_slice())),
    )
}

fn assign_sticky_from<'a>(
    members: &[&str],
    partitions: &[i32],
    prev: impl IntoIterator<Item = (&'a str, &'a [i32])>,
) -> HashMap<String, Vec<i32>> {
    let member_set: std::collections::HashSet<&str> = members.iter().copied().collect();
    let part_set: std::collections::HashSet<i32> =
        partitions.iter().copied().filter(|p| *p >= 0).collect();
    let mut out: HashMap<String, Vec<i32>> = HashMap::new();
    for m in members {
        let _ = out.insert((*m).to_owned(), Vec::new());
    }
    let mut used = std::collections::HashSet::new();
    for (m, parts) in prev {
        if !member_set.contains(m) {
            continue;
        }
        for p in parts {
            if part_set.contains(p) && used.insert(*p) {
                if let Some(slot) = out.get_mut(m) {
                    slot.push(*p);
                }
            }
        }
    }
    let mut remaining: Vec<i32> = part_set
        .iter()
        .copied()
        .filter(|p| !used.contains(p))
        .collect();
    remaining.sort();
    let mut load: BTreeSet<(usize, &str)> = members
        .iter()
        .copied()
        .map(|member| (out.get(member).map(Vec::len).unwrap_or(0), member))
        .collect();
    for partition in remaining {
        let Some(&(count, member)) = load.iter().next() else {
            break;
        };
        let _ = load.remove(&(count, member));
        if let Some(slot) = out.get_mut(member) {
            slot.push(partition);
        }
        let _ = load.insert((count.saturating_add(1), member));
    }
    rebalance_counts(&mut out);
    out
}

/// How many sticky members to prepare before yielding.
fn sticky_member_chunk() -> usize {
    8 * 1024
}

fn rebalance_counts<T: Clone>(out: &mut HashMap<String, Vec<T>>) {
    let mut load: BTreeSet<(usize, String)> = out
        .iter()
        .map(|(member, parts)| (parts.len(), member.clone()))
        .collect();
    while let Some((max_n, max_id)) = load
        .iter()
        .next_back()
        .map(|(count, member)| (*count, member.clone()))
    {
        let Some((min_n, min_id)) = load
            .iter()
            .next()
            .map(|(count, member)| (*count, member.clone()))
        else {
            break;
        };
        if max_id == min_id || max_n <= min_n.saturating_add(1) {
            break;
        }
        let Some(moved) = out.get_mut(&max_id).and_then(Vec::pop) else {
            break;
        };
        if let Some(slot) = out.get_mut(&min_id) {
            slot.push(moved);
        }
        let _ = load.remove(&(max_n, max_id.clone()));
        let _ = load.remove(&(min_n, min_id.clone()));
        let _ = load.insert((max_n.saturating_sub(1), max_id));
        let _ = load.insert((min_n.saturating_add(1), min_id));
    }
}

async fn rebalance_counts_cooperative<T: Clone>(out: &mut HashMap<String, Vec<T>>) {
    let mut load: BTreeSet<(usize, String)> = BTreeSet::new();
    let mut walked = 0usize;
    for (member, parts) in out.iter() {
        let _ = load.insert((parts.len(), member.clone()));
        walked = walked.saturating_add(1);
        if walked == sticky_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    while let Some((max_n, max_id)) = load
        .iter()
        .next_back()
        .map(|(count, member)| (*count, member.clone()))
    {
        let Some((min_n, min_id)) = load
            .iter()
            .next()
            .map(|(count, member)| (*count, member.clone()))
        else {
            break;
        };
        if max_id == min_id || max_n <= min_n.saturating_add(1) {
            break;
        }
        let Some(moved) = out.get_mut(&max_id).and_then(Vec::pop) else {
            break;
        };
        if let Some(slot) = out.get_mut(&min_id) {
            slot.push(moved);
        }
        let _ = load.remove(&(max_n, max_id.clone()));
        let _ = load.remove(&(min_n, min_id.clone()));
        let _ = load.insert((max_n.saturating_sub(1), max_id));
        let _ = load.insert((min_n.saturating_add(1), min_id));
        walked = walked.saturating_add(1);
        if walked == sticky_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
}

/// Sticky assignment that does not give a partition to a new owner until the
/// current owner has revoked it (KIP-429 cooperative-sticky).
pub fn assign_cooperative_sticky(
    members: &[String],
    partitions: &[i32],
    prev: &HashMap<String, Vec<i32>>,
) -> HashMap<String, Vec<i32>> {
    cooperative_filter(assign_sticky(members, partitions, prev), prev)
}

fn cooperative_filter<T: Eq + std::hash::Hash>(
    computed: HashMap<String, Vec<T>>,
    prev: &HashMap<String, Vec<T>>,
) -> HashMap<String, Vec<T>> {
    let mut owner: HashMap<&T, &str> = HashMap::new();
    for (member, partitions) in prev {
        for partition in partitions {
            if let std::collections::hash_map::Entry::Vacant(slot) = owner.entry(partition) {
                let _ = slot.insert(member.as_str());
            }
        }
    }
    let mut out: HashMap<String, Vec<T>> = HashMap::new();
    for (member, owned) in computed {
        let mut kept = Vec::with_capacity(owned.len());
        for partition in owned {
            let revoked = owner
                .get(&partition)
                .is_some_and(|current| *current != member.as_str());
            if !revoked {
                kept.push(partition);
            }
        }
        let _ = out.insert(member, kept);
    }
    out
}

/// Range-assign each topic independently across every member, then concatenate.
pub fn assign_range_topics(
    members: &[String],
    topics: &[(String, Vec<i32>)],
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for member in members {
        let _ = out.insert(member.clone(), Vec::new());
    }
    for (topic, parts) in topics {
        for (member, ps) in assign_range(members, parts) {
            if let Some(slot) = out.get_mut(&member) {
                for p in ps {
                    slot.push((topic.clone(), p));
                }
            }
        }
    }
    out
}

/// Sticky-assign each topic independently across every member, then concatenate.
pub fn assign_sticky_topics(
    members: &[String],
    topics: &[(String, Vec<i32>)],
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for member in members {
        let _ = out.insert(member.clone(), Vec::new());
    }
    let names: Vec<&str> = members.iter().map(String::as_str).collect();
    let previous = previous_partitions_by_topic(prev);
    for (topic, parts) in topics {
        let prev_for_topic = sticky_previous(previous.get(topic.as_str()));
        for (member, ps) in assign_sticky_from(&names, parts, prev_for_topic) {
            if let Some(slot) = out.get_mut(&member) {
                for p in ps {
                    slot.push((topic.clone(), p));
                }
            }
        }
    }
    out
}

/// Range-assign each topic among the members subscribed to it (Java RangeAssignor).
pub fn assign_range_subscribed(
    member_subs: &[(String, Vec<String>)],
    topics: &[(String, Vec<i32>)],
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for (m, _) in member_subs {
        let _ = out.insert(m.clone(), Vec::new());
    }
    let members_by_topic = members_by_topic(member_subs);
    for (topic, parts) in topics {
        let members = members_by_topic
            .get(topic.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for (member, ps) in assign_range_names(members, parts) {
            if let Some(slot) = out.get_mut(&member) {
                for p in ps {
                    slot.push((topic.clone(), p));
                }
            }
        }
    }
    out
}

/// [`assign_range_subscribed`], yielding while a long partition list is assigned.
pub async fn assign_range_subscribed_cooperative(
    member_subs: &[(String, Vec<String>)],
    topics: &[(String, Vec<i32>)],
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out = open_assignment_members(member_subs).await;
    let members_by_topic = members_by_topic_cooperative(member_subs).await;
    for (topic, parts) in topics {
        let members = members_by_topic
            .get(topic.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let assigned = assign_range_names_cooperative(members, parts).await;
        for (member, ps) in assigned {
            if let Some(slot) = out.get_mut(&member) {
                let mut walked = 0usize;
                for partition in ps {
                    slot.push((topic.clone(), partition));
                    walked = walked.saturating_add(1);
                    if walked == 8 * 1024 {
                        walked = 0;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
    }
    out
}

fn range_member_chunk() -> usize {
    8 * 1024
}

async fn sort_member_names_cooperative(names: &mut Vec<&str>) {
    let chunk = range_member_chunk();
    if names.len() <= 1 {
        return;
    }
    let mut start = 0usize;
    while start < names.len() {
        let end = start.saturating_add(chunk).min(names.len());
        if let Some(part) = names.get_mut(start..end) {
            part.sort_unstable();
        }
        start = end;
        if start < names.len() {
            tokio::task::yield_now().await;
        }
    }
    let mut width = chunk;
    while width < names.len() {
        let mut merged = Vec::with_capacity(names.len());
        let mut start = 0usize;
        let mut written = 0usize;
        while start < names.len() {
            let mid = start.saturating_add(width).min(names.len());
            let end = mid.saturating_add(width).min(names.len());
            let mut left = start;
            let mut right = mid;
            while left < mid && right < end {
                let take_right = names
                    .get(right)
                    .zip(names.get(left))
                    .is_some_and(|(right_name, left_name)| right_name < left_name);
                if take_right {
                    if let Some(name) = names.get(right).copied() {
                        merged.push(name);
                    }
                    right = right.saturating_add(1);
                } else if let Some(name) = names.get(left).copied() {
                    merged.push(name);
                    left = left.saturating_add(1);
                } else {
                    break;
                }
                written = written.saturating_add(1);
                if written == chunk {
                    written = 0;
                    tokio::task::yield_now().await;
                }
            }
            while left < mid {
                if let Some(name) = names.get(left).copied() {
                    merged.push(name);
                }
                left = left.saturating_add(1);
                written = written.saturating_add(1);
                if written == chunk {
                    written = 0;
                    tokio::task::yield_now().await;
                }
            }
            while right < end {
                if let Some(name) = names.get(right).copied() {
                    merged.push(name);
                }
                right = right.saturating_add(1);
                written = written.saturating_add(1);
                if written == chunk {
                    written = 0;
                    tokio::task::yield_now().await;
                }
            }
            start = end;
        }
        *names = merged;
        width = width.saturating_mul(2);
        if width < names.len() {
            tokio::task::yield_now().await;
        }
    }
}

async fn dedup_member_names_cooperative(order: &mut Vec<&str>) {
    if order.len() <= 1 {
        return;
    }
    let mut write = 1usize;
    let mut read = 1usize;
    let mut walked = 0usize;
    while read < order.len() {
        let same = order.get(read) == order.get(write.saturating_sub(1));
        if !same {
            if let (Some(name), Some(slot)) = (order.get(read).copied(), order.get_mut(write)) {
                *slot = name;
            }
            write = write.saturating_add(1);
        }
        read = read.saturating_add(1);
        walked = walked.saturating_add(1);
        if walked == range_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    order.truncate(write);
}

/// Sort range members, drop duplicate names, and assign partitions.
///
/// The walk pauses every 8,192 members. Without that pause, ordering
/// 2,000,000 members held the caller for 3.42 seconds.
///
/// Each member id is still copied. Duplicate names are still dropped.
async fn assign_range_names_cooperative(
    members: &[&str],
    partitions: &[i32],
) -> HashMap<String, Vec<i32>> {
    let mut order = Vec::with_capacity(members.len());
    let mut walked = 0usize;
    for member in members {
        order.push(*member);
        walked = walked.saturating_add(1);
        if walked == range_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    sort_member_names_cooperative(&mut order).await;
    dedup_member_names_cooperative(&mut order).await;
    let mut sorted = Vec::new();
    let mut walked = 0usize;
    for partition in partitions
        .iter()
        .copied()
        .filter(|partition| *partition >= 0)
    {
        sorted.push(partition);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    sorted.sort_unstable();
    sorted.dedup();
    let mut out: HashMap<String, Vec<i32>> = HashMap::new();
    walked = 0;
    for member in &order {
        let _ = out.insert((*member).to_owned(), Vec::new());
        walked = walked.saturating_add(1);
        if walked == range_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    let n = order.len();
    if n == 0 {
        return out;
    }
    let np = sorted.len();
    let base = np / n;
    let extra = np % n;
    let mut idx = 0usize;
    walked = 0;
    for (i, member) in order.iter().enumerate() {
        let take = base + usize::from(i < extra);
        for _ in 0..take {
            if let Some(partition) = sorted.get(idx) {
                if let Some(slot) = out.get_mut(*member) {
                    slot.push(*partition);
                }
            }
            idx = idx.saturating_add(1);
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    out
}

/// `(member_id, owned topic-partitions, generation_id)` for
/// [`resolve_sticky_owned_partitions`].
type StickyOwnedClaim = (String, Vec<(String, i32)>, i32);

#[derive(Clone, Copy)]
struct StickyOwner<'a> {
    member: &'a str,
    generation: i32,
}

/// Owned partitions for one member, in the order they were claimed.
///
/// A revoked partition is cleared in place so later claims stay after the
/// partitions that were kept.
struct OwnedParts {
    items: Vec<Option<(String, i32)>>,
    index: HashMap<String, HashMap<i32, usize>>,
}

impl OwnedParts {
    fn new() -> Self {
        Self {
            items: Vec::new(),
            index: HashMap::new(),
        }
    }

    fn push(&mut self, topic: &str, partition: i32) {
        let slot = self.items.len();
        if let Some(parts) = self.index.get_mut(topic) {
            let _ = parts.insert(partition, slot);
        } else {
            let mut parts = HashMap::new();
            let _ = parts.insert(partition, slot);
            let _ = self.index.insert(topic.to_owned(), parts);
        }
        self.items.push(Some((topic.to_owned(), partition)));
    }

    fn revoke(&mut self, topic: &str, partition: i32) {
        let Some(slot) = self
            .index
            .get_mut(topic)
            .and_then(|parts| parts.remove(&partition))
        else {
            return;
        };
        if self.index.get(topic).is_some_and(|parts| parts.is_empty()) {
            let _ = self.index.remove(topic);
        }
        if let Some(item) = self.items.get_mut(slot) {
            *item = None;
        }
    }

    fn into_vec(self) -> Vec<(String, i32)> {
        self.items.into_iter().flatten().collect()
    }
}

fn remember_sticky_owner<'a>(
    owner: &mut HashMap<&'a str, HashMap<i32, StickyOwner<'a>>>,
    topic: &'a str,
    partition: i32,
    claim: StickyOwner<'a>,
) {
    if let Some(parts) = owner.get_mut(topic) {
        let _ = parts.insert(partition, claim);
        return;
    }
    let mut parts = HashMap::new();
    let _ = parts.insert(partition, claim);
    let _ = owner.insert(topic, parts);
}

/// Resolve overlapping owned partitions the way Java
/// `AbstractStickyAssignor.allSubscriptionsEqual` does.
///
/// Each claim is `(member_id, owned topic-partitions, generation_id)`.
/// The higher [`ConsumerProtocolSubscription::generation_id`] keeps the
/// partition. The same generation revokes it from both members
/// (KAFKA-13081). [`ConsumerProtocolSubscription::DEFAULT_GENERATION`] is
/// unknown.
pub fn resolve_sticky_owned_partitions(
    claims: &[StickyOwnedClaim],
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out: HashMap<String, OwnedParts> = HashMap::new();
    for (member, _, _) in claims {
        if !out.contains_key(member.as_str()) {
            let _ = out.insert(member.clone(), OwnedParts::new());
        }
    }
    let mut owner: HashMap<&str, HashMap<i32, StickyOwner<'_>>> = HashMap::new();
    for (member, owned, generation) in claims {
        for (topic, partition) in owned {
            let current = owner
                .get(topic.as_str())
                .and_then(|parts| parts.get(partition))
                .copied();
            let claim = StickyOwner {
                member: member.as_str(),
                generation: *generation,
            };
            match current {
                None => {
                    if let Some(slot) = out.get_mut(member.as_str()) {
                        slot.push(topic, *partition);
                    }
                    remember_sticky_owner(&mut owner, topic.as_str(), *partition, claim);
                }
                Some(StickyOwner {
                    member: other,
                    generation: other_generation,
                }) => match generation.cmp(&other_generation) {
                    std::cmp::Ordering::Equal => {
                        // A member repeating its own claim keeps the partition.
                        // The same generation from a different member revokes
                        // it from both (KAFKA-13081).
                        if other != member.as_str() {
                            if let Some(other_slot) = out.get_mut(other) {
                                other_slot.revoke(topic, *partition);
                            }
                            remember_sticky_owner(&mut owner, topic.as_str(), *partition, claim);
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        if let Some(other_slot) = out.get_mut(other) {
                            other_slot.revoke(topic, *partition);
                        }
                        if let Some(slot) = out.get_mut(member.as_str()) {
                            slot.push(topic, *partition);
                        }
                        remember_sticky_owner(&mut owner, topic.as_str(), *partition, claim);
                    }
                    std::cmp::Ordering::Less => {}
                },
            }
        }
    }
    out.into_iter()
        .map(|(member, parts)| (member, parts.into_vec()))
        .collect()
}

/// [`resolve_sticky_owned_partitions`], yielding while a long claim list is resolved.
pub async fn resolve_sticky_owned_partitions_cooperative(
    claims: &[StickyOwnedClaim],
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out: HashMap<String, OwnedParts> = HashMap::new();
    for (member, _, _) in claims {
        if !out.contains_key(member.as_str()) {
            let _ = out.insert(member.clone(), OwnedParts::new());
        }
    }
    let mut owner: HashMap<&str, HashMap<i32, StickyOwner<'_>>> = HashMap::new();
    let mut walked = 0usize;
    for (member, owned, generation) in claims {
        for (topic, partition) in owned {
            let current = owner
                .get(topic.as_str())
                .and_then(|parts| parts.get(partition))
                .copied();
            let claim = StickyOwner {
                member: member.as_str(),
                generation: *generation,
            };
            match current {
                None => {
                    if let Some(slot) = out.get_mut(member.as_str()) {
                        slot.push(topic, *partition);
                    }
                    remember_sticky_owner(&mut owner, topic.as_str(), *partition, claim);
                }
                Some(StickyOwner {
                    member: other,
                    generation: other_generation,
                }) => match generation.cmp(&other_generation) {
                    std::cmp::Ordering::Equal => {
                        if other != member.as_str() {
                            if let Some(other_slot) = out.get_mut(other) {
                                other_slot.revoke(topic, *partition);
                            }
                            remember_sticky_owner(&mut owner, topic.as_str(), *partition, claim);
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        if let Some(other_slot) = out.get_mut(other) {
                            other_slot.revoke(topic, *partition);
                        }
                        if let Some(slot) = out.get_mut(member.as_str()) {
                            slot.push(topic, *partition);
                        }
                        remember_sticky_owner(&mut owner, topic.as_str(), *partition, claim);
                    }
                    std::cmp::Ordering::Less => {}
                },
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    out.into_iter()
        .map(|(member, parts)| (member, parts.into_vec()))
        .collect()
}

/// Sticky-assign each topic among the members subscribed to it.
pub fn assign_sticky_subscribed(
    member_subs: &[(String, Vec<String>)],
    topics: &[(String, Vec<i32>)],
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for (m, _) in member_subs {
        let _ = out.insert(m.clone(), Vec::new());
    }
    let members_by_topic = members_by_topic(member_subs);
    let previous = previous_partitions_by_topic(prev);
    for (topic, parts) in topics {
        let members = members_by_topic
            .get(topic.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let prev_for_topic = sticky_previous(previous.get(topic.as_str()));
        for (member, ps) in assign_sticky_from(members, parts, prev_for_topic) {
            if let Some(slot) = out.get_mut(&member) {
                for p in ps {
                    slot.push((topic.clone(), p));
                }
            }
        }
    }
    out
}

/// [`assign_sticky_subscribed`], yielding while a long partition list is assigned.
pub async fn assign_sticky_subscribed_cooperative(
    member_subs: &[(String, Vec<String>)],
    topics: &[(String, Vec<i32>)],
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out = open_assignment_members(member_subs).await;
    let members_by_topic = members_by_topic_cooperative(member_subs).await;
    let previous = previous_partitions_by_topic_cooperative(prev).await;
    for (topic, parts) in topics {
        let members = members_by_topic
            .get(topic.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let prev_for_topic = sticky_previous(previous.get(topic.as_str()));
        let assigned = assign_sticky_from_cooperative(members, parts, prev_for_topic).await;
        for (member, ps) in assigned {
            if let Some(slot) = out.get_mut(&member) {
                let mut walked = 0usize;
                for p in ps {
                    slot.push((topic.clone(), p));
                    walked = walked.saturating_add(1);
                    if walked == 8 * 1024 {
                        walked = 0;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
    }
    out
}

/// Assign partitions with the sticky assignor.
///
/// Member setup pauses every 8,192 members. Without that pause, preparing
/// 2,000,000 members held the caller for 10.91 seconds.
///
/// Each member id is still copied. A member is stored once.
async fn assign_sticky_from_cooperative<'a>(
    members: &[&str],
    partitions: &[i32],
    prev: impl IntoIterator<Item = (&'a str, &'a [i32])>,
) -> HashMap<String, Vec<i32>> {
    let mut member_set = std::collections::HashSet::new();
    let mut walked = 0usize;
    for member in members {
        let _ = member_set.insert(*member);
        walked = walked.saturating_add(1);
        if walked == sticky_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    let mut part_set: std::collections::HashSet<i32> = std::collections::HashSet::new();
    walked = 0;
    for partition in partitions
        .iter()
        .copied()
        .filter(|partition| *partition >= 0)
    {
        let _ = part_set.insert(partition);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    let mut out: HashMap<String, Vec<i32>> = HashMap::new();
    walked = 0;
    for member in members {
        let _ = out.insert((*member).to_owned(), Vec::new());
        walked = walked.saturating_add(1);
        if walked == sticky_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    let mut used = std::collections::HashSet::new();
    walked = 0;
    for (member, parts) in prev {
        if !member_set.contains(member) {
            continue;
        }
        for partition in parts {
            if part_set.contains(partition) && used.insert(*partition) {
                if let Some(slot) = out.get_mut(member) {
                    slot.push(*partition);
                }
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    let mut remaining = Vec::new();
    walked = 0;
    for partition in &part_set {
        if !used.contains(partition) {
            remaining.push(*partition);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    remaining.sort();
    let mut load: BTreeSet<(usize, &str)> = BTreeSet::new();
    walked = 0;
    for member in members {
        let member = *member;
        let _ = load.insert((out.get(member).map(Vec::len).unwrap_or(0), member));
        walked = walked.saturating_add(1);
        if walked == sticky_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    walked = 0;
    for partition in remaining {
        let Some(&(count, member)) = load.iter().next() else {
            break;
        };
        let _ = load.remove(&(count, member));
        if let Some(slot) = out.get_mut(member) {
            slot.push(partition);
        }
        let _ = load.insert((count.saturating_add(1), member));
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    rebalance_counts_cooperative(&mut out).await;
    out
}

/// Cooperative-sticky across several topics (KIP-429).
pub fn assign_cooperative_sticky_subscribed(
    member_subs: &[(String, Vec<String>)],
    topics: &[(String, Vec<i32>)],
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<String, Vec<(String, i32)>> {
    cooperative_filter(assign_sticky_subscribed(member_subs, topics, prev), prev)
}

/// [`assign_cooperative_sticky_subscribed`], yielding while partitions are assigned.
pub async fn assign_cooperative_sticky_subscribed_cooperative(
    member_subs: &[(String, Vec<String>)],
    topics: &[(String, Vec<i32>)],
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<String, Vec<(String, i32)>> {
    let computed = assign_sticky_subscribed_cooperative(member_subs, topics, prev).await;
    cooperative_filter_cooperative(computed, prev).await
}

async fn cooperative_filter_cooperative<T: Eq + std::hash::Hash>(
    computed: HashMap<String, Vec<T>>,
    prev: &HashMap<String, Vec<T>>,
) -> HashMap<String, Vec<T>> {
    let mut owner: HashMap<&T, &str> = HashMap::new();
    let mut walked = 0usize;
    for (member, partitions) in prev {
        for partition in partitions {
            if let std::collections::hash_map::Entry::Vacant(slot) = owner.entry(partition) {
                let _ = slot.insert(member.as_str());
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    let mut out: HashMap<String, Vec<T>> = HashMap::new();
    for (member, owned) in computed {
        let mut kept = Vec::with_capacity(owned.len());
        for partition in owned {
            let revoked = owner
                .get(&partition)
                .is_some_and(|current| *current != member.as_str());
            if !revoked {
                kept.push(partition);
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
        let _ = out.insert(member, kept);
    }
    out
}

fn members_by_topic(member_subs: &[(String, Vec<String>)]) -> HashMap<&str, Vec<&str>> {
    let mut out: HashMap<&str, Vec<&str>> = HashMap::new();
    for (id, subs) in member_subs {
        let mut seen = HashSet::new();
        for topic in subs {
            if !seen.insert(topic.as_str()) {
                continue;
            }
            if let Some(members) = out.get_mut(topic.as_str()) {
                members.push(id.as_str());
            } else {
                let _ = out.insert(topic.as_str(), vec![id.as_str()]);
            }
        }
    }
    out
}

fn subscription_index_chunk() -> usize {
    8 * 1024
}

fn assignment_member_chunk() -> usize {
    8 * 1024
}

/// Open one empty assignment list per member.
///
/// The walk pauses every 8,192 members. Without that pause, opening
/// 2,000,000 members held the caller for 2.32 seconds.
///
/// Each member id is still copied.
async fn open_assignment_members(
    member_subs: &[(String, Vec<String>)],
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out = HashMap::new();
    let mut walked = 0usize;
    for (member, _) in member_subs {
        let _ = out.insert(member.clone(), Vec::new());
        walked = walked.saturating_add(1);
        if walked == assignment_member_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    out
}

/// Index each subscribed topic to the members that named it.
///
/// The walk pauses every 8,192 subscriptions. Without that pause, indexing
/// one member's 2,000,000 subscriptions held the caller for 4.43 seconds.
///
/// A repeated topic on the same member is stored once. The first member that
/// names a topic stays first in that topic's list.
async fn members_by_topic_cooperative(
    member_subs: &[(String, Vec<String>)],
) -> HashMap<&str, Vec<&str>> {
    let mut out: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut members_walked = 0usize;
    for (id, subs) in member_subs {
        let mut seen = HashSet::new();
        let mut walked = 0usize;
        for topic in subs {
            walked = walked.saturating_add(1);
            if seen.insert(topic.as_str()) {
                if let Some(members) = out.get_mut(topic.as_str()) {
                    members.push(id.as_str());
                } else {
                    let _ = out.insert(topic.as_str(), vec![id.as_str()]);
                }
            }
            if walked == subscription_index_chunk() {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
        members_walked = members_walked.saturating_add(1);
        if members_walked == assignment_member_chunk() {
            members_walked = 0;
            tokio::task::yield_now().await;
        }
    }
    out
}

fn previous_partitions_by_topic(
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<&str, HashMap<&str, Vec<i32>>> {
    let mut out: HashMap<&str, HashMap<&str, Vec<i32>>> = HashMap::new();
    for (member, owned) in prev {
        for (topic, partition) in owned {
            if let Some(owners) = out.get_mut(topic.as_str()) {
                owners.entry(member.as_str()).or_default().push(*partition);
            } else {
                let mut owners = HashMap::new();
                let _ = owners.insert(member.as_str(), vec![*partition]);
                let _ = out.insert(topic.as_str(), owners);
            }
        }
    }
    out
}

fn previous_index_chunk() -> usize {
    8 * 1024
}

/// Index previously owned partitions by topic and member.
///
/// The walk pauses every 8,192 partitions. Without that pause, indexing
/// 2,000,000 partitions held the caller for 4.49 seconds.
///
/// Every partition stays under the member that owned it.
async fn previous_partitions_by_topic_cooperative(
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<&str, HashMap<&str, Vec<i32>>> {
    let mut out: HashMap<&str, HashMap<&str, Vec<i32>>> = HashMap::new();
    for (member, owned) in prev {
        let mut walked = 0usize;
        for (topic, partition) in owned {
            if let Some(owners) = out.get_mut(topic.as_str()) {
                owners.entry(member.as_str()).or_default().push(*partition);
            } else {
                let mut owners = HashMap::new();
                let _ = owners.insert(member.as_str(), vec![*partition]);
                let _ = out.insert(topic.as_str(), owners);
            }
            walked = walked.saturating_add(1);
            if walked == previous_index_chunk() {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    out
}

fn sticky_previous<'a>(
    owners: Option<&'a HashMap<&'a str, Vec<i32>>>,
) -> impl Iterator<Item = (&'a str, &'a [i32])> {
    owners.into_iter().flat_map(|owners| {
        owners
            .iter()
            .map(|(member, parts)| (*member, parts.as_slice()))
    })
}

/// This member's identity in a consumer group (Java `ConsumerGroupMetadata`).
///
/// [`std::fmt::Display`] is Java `ConsumerGroupMetadata.toString` (`GroupMetadata(...)`).
/// Missing [`Self::group_instance_id`] prints as Java `Optional.orElse("")`.
/// [`Self::new`] uses [`Self::UNKNOWN_GENERATION_ID`] /
/// [`Self::UNKNOWN_MEMBER_ID`] (Java `JoinGroupRequest`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMetadata {
    /// Kafka `group.id`.
    pub group_id: String,
    /// Classic generation, or KIP-848 member epoch.
    pub generation_id: i32,
    /// Member id (coordinator-assigned on ConsumerGroupHeartbeat v0;
    /// client-generated on v1, KIP-1082).
    pub member_id: String,
    /// Kafka `group.instance.id`, if static membership is set.
    pub group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Java `JoinGroupRequest.UNKNOWN_GENERATION_ID`. Same sentinel as
    /// [`crate::protocol::group::JoinGroupRequest::UNKNOWN_GENERATION_ID`].
    pub const UNKNOWN_GENERATION_ID: i32 = JoinGroupRequest::UNKNOWN_GENERATION_ID;
    /// Java `JoinGroupRequest.UNKNOWN_MEMBER_ID`. Same sentinel as
    /// [`crate::protocol::group::JoinGroupRequest::UNKNOWN_MEMBER_ID`].
    pub const UNKNOWN_MEMBER_ID: &'static str = JoinGroupRequest::UNKNOWN_MEMBER_ID;

    /// Java `ConsumerGroupMetadata(String)` (`generationId`
    /// [`Self::UNKNOWN_GENERATION_ID`], `memberId`
    /// [`Self::UNKNOWN_MEMBER_ID`], empty `groupInstanceId`).
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id: Self::UNKNOWN_GENERATION_ID,
            member_id: Self::UNKNOWN_MEMBER_ID.into(),
            group_instance_id: None,
        }
    }

    /// Java `ConsumerGroupMetadata.groupId`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        self.group_id.as_str()
    }

    /// Java `ConsumerGroupMetadata.generationId`.
    #[must_use]
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// Java `ConsumerGroupMetadata.memberId`.
    #[must_use]
    pub fn member_id(&self) -> &str {
        self.member_id.as_str()
    }

    /// Java `ConsumerGroupMetadata.groupInstanceId`.
    #[must_use]
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }
}

impl fmt::Display for ConsumerGroupMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GroupMetadata(groupId = {}, generationId = {}, memberId = {}, groupInstanceId = {})",
            self.group_id,
            self.generation_id,
            self.member_id,
            self.group_instance_id.as_deref().unwrap_or("")
        )
    }
}

/// Java `org.apache.kafka.clients.consumer.GroupProtocol` (`group.protocol`).
///
/// [`std::fmt::Display`] is Java `GroupProtocol.toString` (`CLASSIC`). [`Self::of`] is
/// Java `GroupProtocol.of` (unknown is `None`; Java throws).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupProtocol {
    /// Java `CLASSIC` (JoinGroup / SyncGroup).
    Classic,
    /// Java `CONSUMER` (KIP-848 ConsumerGroupHeartbeat).
    Consumer,
}

impl GroupProtocol {
    /// Java `GroupProtocol.name` (`CLASSIC` / `CONSUMER`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Classic => "CLASSIC",
            Self::Consumer => "CONSUMER",
        }
    }

    /// Java `GroupProtocol.of` (case-insensitive; unknown is `None`).
    #[must_use]
    pub fn of(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("classic") {
            Some(Self::Classic)
        } else if name.eq_ignore_ascii_case("consumer") {
            Some(Self::Consumer)
        } else {
            None
        }
    }
}

impl fmt::Display for GroupProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Classic or KIP-848 consumer group member.
pub struct ConsumerGroup {
    consumer: Consumer,
    coord: BrokerConn,
    cfg: ConsumerConfig,
    group_id: String,
    member_id: String,
    generation_id: i32,
    topics: Vec<String>,
    /// Java `subscribe(Pattern)`: re-list cluster topics on poll.
    topic_match: Option<TopicMatch>,
    last_match_refresh: Instant,
    protocol: String,
    /// Assignors advertised on JoinGroup (Java `partition.assignment.strategy`).
    assignors: Vec<String>,
    kip848: bool,
    prev_assignment: HashMap<String, Vec<(String, i32)>>,
    hb_err: Arc<AtomicI16>,
    hb_generation: Arc<AtomicI32>,
    /// Assignment from a later ConsumerGroupHeartbeat; applied on `poll` / `commit`.
    hb_assignment: Arc<parking_lot::Mutex<Option<Vec<TopicPartitions>>>>,
    /// Last applied assignment, sent once on the next heartbeat (KIP-848 ack).
    hb_ack: Arc<parking_lot::Mutex<Option<Vec<TopicPartitions>>>>,
    /// Member id shared with the KIP-848 heartbeat task, which adopts
    /// broker-rotated ids into it; the classic loop keeps its spawn-time
    /// clone. The poll path copies this into `member_id` before committing
    /// or leaving (KL03-15).
    hb_member_id: Arc<parking_lot::Mutex<String>>,
    /// Topics the KIP-848 heartbeat task sends when it rejoins at epoch 0.
    hb_topics: Arc<parking_lot::Mutex<Vec<String>>>,
    hb_interval_ms: Arc<AtomicI32>,
    hb_deadline: Arc<parking_lot::Mutex<Option<Instant>>>,
    hb_stop: watch::Sender<bool>,
    /// Background heartbeat task. [`Self::leave`] waits for it to stop.
    hb_task: Option<Arc<tokio::task::JoinHandle<()>>>,
    last_auto_commit: Instant,
    last_delivered_positions: Option<Vec<(Arc<str>, i32, i64)>>,
    last_delivered_topic_ids: DeliveredTopicIds,
    /// Previous v10 OffsetCommit body, refilled on the next commit.
    commit_v10: OffsetCommitRequestData,
    /// Previous v10 OffsetFetch body, refilled on the next fetch.
    fetch_v10: OffsetFetchRequestData,
    /// Previous classic OffsetCommit topics, refilled on the next commit.
    classic_commit_topics: Vec<OffsetTopic>,
    /// Previous classic OffsetFetch topics, refilled on the next fetch.
    classic_fetch_topics: Vec<OffsetFetchTopic>,
    /// Topic ids from the previous v10 OffsetFetch, reused for the response names.
    fetch_names: HashMap<[u8; 16], String>,
    last_poll: Arc<parking_lot::Mutex<Option<Instant>>>,
    /// Heartbeat thread left the group after `max.poll.interval.ms`.
    left_max_poll: Arc<AtomicBool>,
    m_rebalances: AtomicU64,
    m_commits_ok: AtomicU64,
    m_commits_failed: AtomicU64,
    /// Next [`poll`](Self::poll) must rejoin (Java `enforceRebalance`).
    rebalance_needed: bool,
    /// JoinGroup v8+ Reason for the next rejoin (KIP-800).
    rebalance_reason: Option<String>,
    /// Java `commitAsync`: OffsetCommit sent on the next poll / leave.
    pending_async_commits: Vec<PendingAsyncCommit>,
    /// Coordinators this member already contacted. A later commit reuses them.
    prior_coords: HashMap<String, KnownCoord>,
}

impl Drop for ConsumerGroup {
    fn drop(&mut self) {
        // Leave waits for this task. Dropping the group without leave only
        // drops the handle, which would detach a heartbeat stuck in a broker
        // read. Abort it so the next poll of the runtime stops that call.
        if let Some(task) = self.hb_task.take() {
            task.abort();
        }
    }
}

impl ConsumerGroup {
    /// Join with the Java range assignor. One topic.
    ///
    /// An empty `group_id` is Java `InvalidGroupIdException`
    /// (`The configured group.id should not be an empty string or whitespace.`).
    pub async fn join(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        Self::join_topics(cfg, group_id, std::iter::once(topic)).await
    }

    /// Join with the Java range assignor. Several topics, assigned independently.
    pub async fn join_topics(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        Self::join_with_protocol(cfg, group_id, topics, "range").await
    }

    /// Join with the sticky assignor. One topic.
    pub async fn join_sticky(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        Self::join_sticky_topics(cfg, group_id, std::iter::once(topic)).await
    }

    /// Join with the sticky assignor. Several topics, assigned independently.
    pub async fn join_sticky_topics(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        Self::join_with_protocol(cfg, group_id, topics, "sticky").await
    }

    /// Join with cooperative-sticky (KIP-429). One topic.
    ///
    /// Partitions move only after the previous owner has revoked them, so two
    /// members never fetch the same partition during a rebalance.
    pub async fn join_cooperative_sticky(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        Self::join_cooperative_sticky_topics(cfg, group_id, std::iter::once(topic)).await
    }

    /// Join with cooperative-sticky (KIP-429). Several topics.
    pub async fn join_cooperative_sticky_topics(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        Self::join_with_protocol(cfg, group_id, topics, "cooperative-sticky").await
    }

    async fn join_with_protocol(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
        protocol: &str,
    ) -> Result<Self> {
        let group_id = group_id.into();
        reject_java_empty_group_id(&group_id)?;
        let topics = collect_topics(topics)?;
        Self::join_with_protocol_list(cfg, group_id, topics, vec![protocol.to_string()], None).await
    }

    /// Join with several assignors (Java `partition.assignment.strategy`).
    ///
    /// JoinGroup sends Protocols of N in this order. The broker picks the
    /// first protocol every member supports; this client then assigns with
    /// that name. Empty `assignors` is Java `IllegalStateException`
    /// (`Must configure at least one partition assigner class name`).
    pub async fn join_with_assignors(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
        assignors: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        Self::join_with_assignors_topics(cfg, group_id, std::iter::once(topic), assignors).await
    }

    /// [`Self::join_with_assignors`] for several topics.
    pub async fn join_with_assignors_topics(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
        assignors: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let group_id = group_id.into();
        reject_java_empty_group_id(&group_id)?;
        let names: Vec<String> = assignors.into_iter().map(Into::into).collect();
        if names.is_empty() {
            return Err(reject_java_no_assignors());
        }
        Self::join_with_protocol_list(cfg, group_id, collect_topics(topics)?, names, None).await
    }

    async fn join_with_protocol_list(
        cfg: ConsumerConfig,
        group_id: String,
        topics: Vec<String>,
        assignors: Vec<String>,
        topic_match: Option<TopicMatch>,
    ) -> Result<Self> {
        reject_java_empty_group_id(&group_id)?;
        let mut cfg = cfg;
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        let protocol = assignors
            .first()
            .cloned()
            .ok_or_else(reject_java_no_assignors)?;
        let started = Instant::now()
            .checked_add(cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        let (mut consumer, coord) = connect_member_and_coordinator(
            &cfg,
            &group_id,
            &topics,
            topic_match.is_some(),
            started,
        )
        .await?;
        let coord = match coord {
            Ok((conn, keys)) => {
                consumer.remember_broker_versions(conn.addr(), keys);
                conn
            }
            Err(error) => return Err(error),
        };

        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_generation = Arc::new(AtomicI32::new(0));
        let hb_assignment = Arc::new(parking_lot::Mutex::new(None));
        let hb_ack = Arc::new(parking_lot::Mutex::new(None));
        let hb_member_id = Arc::new(parking_lot::Mutex::new(String::new()));
        let hb_topics = Arc::new(parking_lot::Mutex::new(topics.clone()));
        let hb_interval_ms = Arc::new(AtomicI32::new(duration_millis_i32(cfg.heartbeat_interval)));
        let hb_deadline = Arc::new(parking_lot::Mutex::new(None));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            consumer,
            coord,
            cfg: cfg.clone(),
            group_id,
            member_id: String::new(),
            generation_id: ConsumerProtocolSubscription::DEFAULT_GENERATION,
            topics,
            topic_match,
            last_match_refresh: Instant::now(),
            protocol,
            assignors,
            kip848: false,
            prev_assignment: HashMap::new(),
            hb_err,
            hb_generation,
            hb_assignment,
            hb_ack,
            hb_member_id,
            hb_topics,
            hb_interval_ms,
            hb_deadline,
            hb_stop,
            hb_task: None,
            last_auto_commit: Instant::now(),
            last_delivered_positions: None,
            last_delivered_topic_ids: HashMap::new(),
            commit_v10: OffsetCommitRequestData::default(),
            fetch_v10: OffsetFetchRequestData::default(),
            classic_commit_topics: Vec::new(),
            classic_fetch_topics: Vec::new(),
            fetch_names: HashMap::new(),
            last_poll: Arc::new(parking_lot::Mutex::new(None)),
            left_max_poll: Arc::new(AtomicBool::new(false)),
            m_rebalances: AtomicU64::new(0),
            m_commits_ok: AtomicU64::new(0),
            m_commits_failed: AtomicU64::new(0),
            rebalance_needed: false,
            rebalance_reason: None,
            pending_async_commits: Vec::new(),
            prior_coords: HashMap::new(),
        };
        if g.topic_match.is_some() {
            g.topics = g.matching_topic_names_within(started).await?;
            g.last_match_refresh = Instant::now();
        }
        g.rejoin_within(started).await?;
        g.hb_task = Some(g.spawn_heartbeat(hb_rx));
        Ok(g)
    }

    /// Join with the range assignor and a topic predicate (Java `subscribe(Pattern)`).
    ///
    /// Cluster topics for which `matches` is true become the subscription.
    /// Names starting with `__` are skipped (Java `exclude.internal.topics`).
    /// [`Self::poll`] re-lists Metadata when [`ConsumerConfig::metadata_max_age`]
    /// has elapsed (every poll when that age is zero).
    pub async fn join_matching(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::join_with_protocol_list(
            cfg,
            group_id.into(),
            Vec::new(),
            vec!["range".into()],
            Some(Arc::new(matches)),
        )
        .await
    }

    /// [`Self::join_sticky`] with a topic predicate (Java `subscribe(Pattern)`).
    pub async fn join_sticky_matching(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::join_with_protocol_list(
            cfg,
            group_id.into(),
            Vec::new(),
            vec!["sticky".into()],
            Some(Arc::new(matches)),
        )
        .await
    }

    /// [`Self::join_cooperative_sticky`] with a topic predicate
    /// (Java `subscribe(Pattern)`).
    pub async fn join_cooperative_sticky_matching(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::join_with_protocol_list(
            cfg,
            group_id.into(),
            Vec::new(),
            vec!["cooperative-sticky".into()],
            Some(Arc::new(matches)),
        )
        .await
    }

    /// KIP-848 `group.protocol=consumer`. One topic.
    ///
    /// An empty `group_id` uses the same Java `InvalidGroupIdException` as
    /// [`Self::join`].
    pub async fn join_consumer(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        Self::join_consumer_topics(cfg, group_id, std::iter::once(topic)).await
    }

    /// KIP-848 `group.protocol=consumer`. Several topics.
    pub async fn join_consumer_topics(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let group_id = group_id.into();
        reject_java_empty_group_id(&group_id)?;
        let topics = collect_topics(topics)?;
        Self::join_consumer_list(cfg, group_id, topics, None).await
    }

    /// [`Self::join_consumer`] with a topic predicate (Java `subscribe(Pattern)`).
    pub async fn join_consumer_matching(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::join_consumer_list(cfg, group_id.into(), Vec::new(), Some(Arc::new(matches))).await
    }

    async fn join_consumer_list(
        cfg: ConsumerConfig,
        group_id: String,
        topics: Vec<String>,
        topic_match: Option<TopicMatch>,
    ) -> Result<Self> {
        reject_java_empty_group_id(&group_id)?;
        let mut cfg = cfg;
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        let started = Instant::now()
            .checked_add(cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        let (mut consumer, coord) = connect_member_and_coordinator(
            &cfg,
            &group_id,
            &topics,
            topic_match.is_some(),
            started,
        )
        .await?;
        let coord = match coord {
            Ok((conn, keys)) => {
                consumer.remember_broker_versions(conn.addr(), keys);
                conn
            }
            Err(error) => return Err(error),
        };
        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_generation = Arc::new(AtomicI32::new(
            ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
        ));
        let hb_assignment = Arc::new(parking_lot::Mutex::new(None));
        let hb_ack = Arc::new(parking_lot::Mutex::new(None));
        let hb_member_id = Arc::new(parking_lot::Mutex::new(String::new()));
        let hb_topics = Arc::new(parking_lot::Mutex::new(topics.clone()));
        let hb_interval_ms = Arc::new(AtomicI32::new(0));
        let hb_deadline = Arc::new(parking_lot::Mutex::new(None));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            consumer,
            coord,
            cfg: cfg.clone(),
            group_id,
            member_id: String::new(),
            generation_id: ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            topics,
            topic_match,
            last_match_refresh: Instant::now(),
            protocol: "consumer".into(),
            assignors: Vec::new(),
            kip848: true,
            prev_assignment: HashMap::new(),
            hb_err,
            hb_generation,
            hb_assignment,
            hb_ack,
            hb_member_id,
            hb_topics,
            hb_interval_ms,
            hb_deadline,
            hb_stop,
            hb_task: None,
            last_auto_commit: Instant::now(),
            last_delivered_positions: None,
            last_delivered_topic_ids: HashMap::new(),
            commit_v10: OffsetCommitRequestData::default(),
            fetch_v10: OffsetFetchRequestData::default(),
            classic_commit_topics: Vec::new(),
            classic_fetch_topics: Vec::new(),
            fetch_names: HashMap::new(),
            last_poll: Arc::new(parking_lot::Mutex::new(None)),
            left_max_poll: Arc::new(AtomicBool::new(false)),
            m_rebalances: AtomicU64::new(0),
            m_commits_ok: AtomicU64::new(0),
            m_commits_failed: AtomicU64::new(0),
            rebalance_needed: false,
            rebalance_reason: None,
            pending_async_commits: Vec::new(),
            prior_coords: HashMap::new(),
        };
        if g.topic_match.is_some() {
            g.topics = g.matching_topic_names_within(started).await?;
            g.last_match_refresh = Instant::now();
        }
        g.heartbeat_join_within(started).await?;
        g.hb_task = Some(g.spawn_heartbeat_consumer(hb_rx));
        Ok(g)
    }

    /// Test hook: the background heartbeat task, if one has been started.
    #[doc(hidden)]
    pub fn test_heartbeat_task(&self) -> Option<Arc<tokio::task::JoinHandle<()>>> {
        self.hb_task.as_ref().map(Arc::clone)
    }

    /// Subscribed topic names, in join order.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Subscribed topic names. Same as [`Self::topics`] (Java `subscription`).
    #[must_use]
    pub fn subscription(&self) -> &[String] {
        self.topics()
    }

    /// Java `group.protocol` in effect ([`GroupProtocol`]).
    ///
    /// Classic JoinGroup members are [`GroupProtocol::Classic`].
    /// [`Self::join_consumer`] members are [`GroupProtocol::Consumer`].
    #[must_use]
    pub fn group_protocol(&self) -> GroupProtocol {
        if self.kip848 {
            GroupProtocol::Consumer
        } else {
            GroupProtocol::Classic
        }
    }

    /// For KIP-848 consumer groups, returns the broker-directed heartbeat interval.
    /// For classic groups, returns the configured [`ConsumerConfig::heartbeat_interval`].
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        if self.kip848 {
            let ms = self.hb_interval_ms.load(Ordering::SeqCst);
            if ms > 0 {
                if let Ok(ms_u64) = u64::try_from(ms) {
                    return Duration::from_millis(ms_u64);
                }
            }
        }
        self.cfg.heartbeat_interval
    }

    /// The recorded next heartbeat deadline computed from the broker interval,
    /// or `None` if no heartbeat is currently scheduled or the member has left.
    #[must_use]
    pub fn next_heartbeat_deadline(&self) -> Option<Instant> {
        *self.hb_deadline.lock()
    }

    /// Whether the heartbeat task has left the group due to `max.poll.interval.ms`.
    #[must_use]
    pub fn is_left_max_poll(&self) -> bool {
        self.left_max_poll.load(Ordering::SeqCst)
    }

    /// Assigned partitions (Java `assignment`). Offsets are [`Self::positions`].
    #[must_use]
    pub fn assignment(&self) -> Vec<TopicPartition> {
        self.consumer.assignment()
    }

    /// Same as [`Self::assignment`].
    #[must_use]
    pub fn assigned_partitions(&self) -> Vec<TopicPartition> {
        self.assignment()
    }

    /// Assigned partitions with their next consumable offsets (delivered positions).
    ///
    /// If records are buffered in the consumer (e.g. via [`ConsumerConfig::max_poll_records`]),
    /// each partition reports its next undelivered record offset rather than the
    /// ahead-of-delivery broker fetch cursor. A partition waiting to be placed
    /// in the log is omitted.
    #[must_use]
    pub fn positions(&self) -> Vec<(TopicPartition, i64)> {
        self.consumer.positions()
    }

    /// Broker fetch cursor (next offset to request from the broker) for an assigned partition.
    ///
    /// This may be ahead of [`Self::position`] when records are buffered in the consumer
    /// under [`ConsumerConfig::max_poll_records`]. A cursor below zero is waiting to be
    /// placed in the log and is not returned.
    pub fn fetch_cursor(&self, topic: &str, partition: i32) -> Result<i64> {
        self.consumer.fetch_cursor(topic, partition)
    }

    /// Assigned partitions with their current broker fetch cursors.
    #[must_use]
    pub fn fetch_cursors(&self) -> Vec<(TopicPartition, i64)> {
        self.consumer.fetch_cursors()
    }

    /// Kafka `group.id`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Kafka member id assigned by the coordinator.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Classic generation, or KIP-848 member epoch.
    #[must_use]
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// Snapshot of group id, member id, generation, and instance id.
    #[must_use]
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        ConsumerGroupMetadata {
            group_id: self.group_id.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: self.cfg.group_instance_id.clone(),
        }
    }

    /// Stop fetching these assigned partitions until [`resume`](Self::resume).
    ///
    /// Pause is stored on the consumer, so it survives rebalance.
    pub fn pause(&mut self, partitions: impl IntoIterator<Item = impl Into<TopicPartition>>) {
        self.consumer.pause(partitions);
    }

    /// Undo [`pause`](Self::pause) for these partitions.
    pub fn resume(&mut self, partitions: impl IntoIterator<Item = impl Into<TopicPartition>>) {
        self.consumer.resume(partitions);
    }

    /// Assigned partitions that [`poll`](Self::poll) currently skips.
    pub fn paused(&self) -> Vec<TopicPartition> {
        self.consumer.paused()
    }

    /// Next consumable offset (delivered position) for an assigned partition.
    ///
    /// If records have been fetched and buffered (for example under
    /// [`ConsumerConfig::max_poll_records`]), this returns the offset of the
    /// next undelivered record. If all fetched records have been delivered,
    /// this matches the broker fetch cursor.
    ///
    /// An unassigned partition is Java `IllegalStateException`
    /// (`You can only check the position for partitions assigned to this consumer.`).
    pub fn position(&self, topic: &str, partition: i32) -> Result<i64> {
        self.consumer.position(topic, partition)
    }

    /// [`Self::position`] for a [`TopicPartition`].
    pub fn position_of(&self, partition: impl Into<TopicPartition>) -> Result<i64> {
        self.consumer.position_of(partition)
    }

    /// Set the next fetch offset for an assigned partition (Java
    /// `seek(TopicPartition, long)`).
    ///
    /// A negative offset is Java `IllegalArgumentException` (`seek offset
    /// must not be a negative number`). An unassigned partition is Java
    /// `IllegalStateException` (`No current assignment for partition`).
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.consumer.seek(topic, partition, offset)?;
        if self.last_delivered_positions.is_some() {
            self.snapshot_delivered_positions();
        }
        Ok(())
    }

    /// [`Self::seek`] for a [`TopicPartition`].
    pub fn seek_to(&mut self, partition: impl Into<TopicPartition>, offset: i64) -> Result<()> {
        self.consumer.seek_to(partition, offset)?;
        if self.last_delivered_positions.is_some() {
            self.snapshot_delivered_positions();
        }
        Ok(())
    }

    /// Seek using [`OffsetAndMetadata`] (Java `seek(TopicPartition, OffsetAndMetadata)`).
    ///
    /// A negative offset and an unassigned partition use the same Java
    /// messages as [`Self::seek`]. The leader epoch is sent as Fetch
    /// `LastFetchedEpoch`. The metadata string is ignored.
    pub fn seek_with_metadata(
        &mut self,
        partition: impl Into<TopicPartition>,
        offset: impl Into<OffsetAndMetadata>,
    ) -> Result<()> {
        self.consumer.seek_with_metadata(partition, offset)?;
        if self.last_delivered_positions.is_some() {
            self.snapshot_delivered_positions();
        }
        Ok(())
    }

    /// Seek every assigned partition to the log start (Java `seekToBeginning`).
    pub async fn seek_to_beginning(&mut self) -> Result<()> {
        self.consumer.seek_to_beginning().await
    }

    /// Seek these assigned partitions to the log start.
    pub async fn seek_to_beginning_of(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<()> {
        self.consumer.seek_to_beginning_of(partitions).await
    }

    /// Seek every assigned partition to the high watermark (Java `seekToEnd`).
    pub async fn seek_to_end(&mut self) -> Result<()> {
        self.consumer.seek_to_end().await
    }

    /// Seek these assigned partitions to the high watermark.
    pub async fn seek_to_end_of(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<()> {
        self.consumer.seek_to_end_of(partitions).await
    }

    /// High watermark minus position (Java `currentLag`).
    ///
    /// An unassigned partition is Java `IllegalStateException`
    /// (`No current assignment for partition`).
    pub async fn current_lag(
        &mut self,
        partition: impl Into<TopicPartition>,
    ) -> Result<Option<i64>> {
        self.consumer.current_lag(partition).await
    }

    /// [`Self::current_lag`] with a one-shot timeout for the ListOffsets RPC.
    ///
    /// An unassigned partition is the same Java `IllegalStateException` as
    /// [`Self::current_lag`].
    pub async fn current_lag_timeout(
        &mut self,
        partition: impl Into<TopicPartition>,
        timeout: Duration,
    ) -> Result<Option<i64>> {
        self.consumer.current_lag_timeout(partition, timeout).await
    }

    /// Last committed offsets for the current assignment (`OffsetFetch`).
    ///
    /// Partitions with no committed offset return offset `-1`.
    /// Waits up to [`ConsumerConfig::request_timeout`].
    pub async fn committed(&mut self) -> Result<Vec<(TopicPartition, OffsetAndMetadata)>> {
        let assigned = self.consumer.assignment_cooperative().await;
        self.committed_for(assigned).await
    }

    /// [`Self::committed`] with a one-shot timeout (Java `committed(Duration)`).
    pub async fn committed_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, OffsetAndMetadata)>> {
        let assigned = self.consumer.assignment_cooperative().await;
        self.committed_for_timeout(assigned, timeout).await
    }

    /// Last committed offsets for these partitions (`OffsetFetch`).
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`].
    pub async fn committed_for(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<Vec<(TopicPartition, OffsetAndMetadata)>> {
        let timeout = self.cfg.request_timeout;
        self.committed_for_timeout(partitions, timeout).await
    }

    /// [`Self::committed_for`] with a one-shot timeout.
    pub async fn committed_for_timeout(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, OffsetAndMetadata)>> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        let started = Instant::now();
        if self.kip848 {
            self.apply_pending_assignment().await?;
        }
        let partitions = crate::offsets::collect_bounded_partitions(partitions)?;
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let mut topics = std::mem::take(&mut self.classic_fetch_topics);
        refill_topic_partitions_cooperative(&mut topics, &partitions).await;
        let fetched = self
            .offset_fetch(&topics, timeout.saturating_sub(started.elapsed()))
            .await;
        self.classic_fetch_topics = topics;
        let fetched = fetched?;
        check_fetched_offsets(&fetched)?;
        let index = index_fetched_offsets_cooperative(&fetched).await;
        let mut listed = Vec::with_capacity(partitions.len());
        let mut walked = 0usize;
        for tp in partitions {
            let md = index
                .get(&(tp.topic.as_str(), tp.partition))
                .copied()
                .map(offset_and_metadata_from)
                .unwrap_or_else(|| OffsetAndMetadata::new(-1));
            listed.push((tp, md));
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
        Ok(listed)
    }

    async fn offset_fetch(
        &mut self,
        topics: &[OffsetFetchTopic],
        timeout: Duration,
    ) -> Result<Vec<FetchedOffsetTopic>> {
        if self.coord.offset_fetch_version == 10 {
            return self.fetch_offsets_v10(topics, timeout).await;
        }
        let require_stable = self.cfg.isolation_level == IsolationLevel::ReadCommitted;
        let (member_id, member_epoch) = if self.kip848 {
            (Some(self.member_id.as_str()), self.generation_id)
        } else {
            (None, -1)
        };
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        let prepared_version = negotiated_coord_version(&self.coord, OFFSET_FETCH).ok();
        let mut payload = BytesMut::new();
        if let Some(version) = prepared_version {
            encode_offset_fetch_request_cooperative(
                &mut payload,
                version,
                &self.group_id,
                member_id,
                member_epoch,
                require_stable,
                Some(topics),
            )
            .await?;
        }
        let (version, body) = coord_roundtrip_with_bootstrap(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            |version, buf| {
                if prepared_version == Some(version) {
                    buf.extend_from_slice(&payload);
                    Ok(())
                } else {
                    encode_offset_fetch_request(
                        buf,
                        version,
                        &self.group_id,
                        member_id,
                        member_epoch,
                        require_stable,
                        Some(topics),
                    )
                }
            },
            &bootstrap_keys,
            &address_keys,
            &mut self.prior_coords,
            timeout,
        )
        .await?;
        decode_offset_fetch_response_cooperative(&mut body.clone(), version)
            .await
            .map_err(|e| group_decode_context(e, "OffsetFetch", version, body.len()))
    }

    /// Fetch the current assignment. Rejoins on a classic rebalance.
    ///
    /// Returns [`ConsumerRecords`], which indexes like a slice of
    /// [`crate::FetchedRecord`].
    ///
    /// When [`ConsumerConfig::enable_auto_commit`] is on and the interval has
    /// elapsed, commits previously delivered records. The batch about to be
    /// returned is not committed until a subsequent poll (KL03-07). Leave/close/unsubscribe
    /// do **not** auto-commit positions (KL-02); only this poll-interval path and
    /// explicit `commit*` APIs store offsets.
    ///
    /// Returns [`Error::MaxPollInterval`] if this member did not poll within
    /// [`ConsumerConfig::max_poll_interval`]. The heartbeat thread also leaves
    /// the group when that happens.
    ///
    /// Not subscribed is Java `IllegalStateException` (`Consumer is not
    /// subscribed to any topics or assigned any partitions`).
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn poll(&mut self) -> Result<ConsumerRecords> {
        self.poll_fetch(None).await
    }

    /// Poll with a one-shot `fetch.max.wait.ms` (Java `poll(Duration)`).
    ///
    /// Not subscribed is the same Java `IllegalStateException` as [`Self::poll`].
    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<ConsumerRecords> {
        self.poll_fetch(Some(timeout)).await
    }

    async fn poll_fetch(&mut self, wait: Option<Duration>) -> Result<ConsumerRecords> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        if self.left_max_poll.load(Ordering::SeqCst) {
            return Err(Error::MaxPollInterval);
        }
        self.check_max_poll_interval()?;
        if self.topics.is_empty() && self.topic_match.is_none() {
            return Err(crate::consumer::reject_java_no_subscription_or_assignment());
        }
        self.maybe_refresh_matching().await?;
        self.reject_unsupported_heartbeat()?;
        let force = std::mem::replace(&mut self.rebalance_needed, false);
        if self.kip848 {
            self.check_kip848_membership().await?;
            self.apply_pending_assignment().await?;
            if force {
                self.rebalance_reason = None;
                self.heartbeat_join().await?;
            }
        } else {
            let code = self.hb_err.load(Ordering::SeqCst);
            if code == error::FENCED_INSTANCE_ID {
                return Err(Error::broker(code, "Heartbeat"));
            }
            if force || matches!(classic_heartbeat(code), ClassicHeartbeat::Rejoin) {
                self.rejoin().await?;
            } else if matches!(classic_heartbeat(code), ClassicHeartbeat::Failed) {
                return Err(Error::broker(code, "Heartbeat"));
            }
        }
        let flush_deadline = Instant::now() + self.cfg.request_timeout;
        self.flush_async_commits(flush_deadline).await;
        let recs = self.fetch_while_interval_commits(wait).await?;
        if !recs.is_empty()
            && self.cfg.enable_auto_commit
            && self.cfg.auto_commit_interval.is_zero()
        {
            if let Err(error) = self.commit().await {
                self.consumer.restore_fetched(recs).await;
                return Err(error);
            }
        }
        if !recs.is_empty() {
            self.snapshot_delivered_positions();
        }
        Ok(self.consumer.deliver_consumed(recs))
    }

    /// Fetch counters for the underlying consumer.
    #[must_use]
    pub fn metrics(&self) -> crate::ConsumerMetrics {
        self.consumer.metrics()
    }

    /// Rebalance and offset-commit counts since join. No group or member labels.
    #[must_use]
    pub fn outcomes(&self) -> crate::metrics::GroupOutcomes {
        crate::metrics::GroupOutcomes {
            rebalances: self.m_rebalances.load(Ordering::Relaxed),
            commits_succeeded: self.m_commits_ok.load(Ordering::Relaxed),
            commits_failed: self.m_commits_failed.load(Ordering::Relaxed),
        }
    }

    fn note_commit(&self, ok: bool) {
        let counter = if ok {
            &self.m_commits_ok
        } else {
            &self.m_commits_failed
        };
        let _ = counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Java `clientInstanceId` (KIP-714). Delegates to [`crate::Consumer::client_instance_id`].
    ///
    /// Returns [`crate::Uuid`] (Java `Uuid`).
    pub async fn client_instance_id(&mut self) -> Result<crate::Uuid> {
        self.consumer.client_instance_id().await
    }

    /// [`Self::client_instance_id`] with a one-shot timeout (Java
    /// `clientInstanceId(Duration)`).
    pub async fn client_instance_id_timeout(&mut self, timeout: Duration) -> Result<crate::Uuid> {
        self.consumer.client_instance_id_timeout(timeout).await
    }

    /// Interrupt [`Self::poll`]. See [`crate::Consumer::wakeup`].
    pub fn wakeup(&self) {
        self.consumer.wakeup();
    }

    /// Cloneable handle for [`Self::wakeup`] from another task.
    #[must_use]
    pub fn wakeup_handle(&self) -> crate::WakeupHandle {
        self.consumer.wakeup_handle()
    }

    /// Metadata for `topic` (Java `partitionsFor`: leader, replicas, ISR, offline replicas, leader epoch).
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::partitions_for_timeout`].
    pub async fn partitions_for(
        &mut self,
        topic: impl Into<String>,
    ) -> Result<Vec<crate::PartitionInfo>> {
        self.consumer.partitions_for(topic).await
    }

    /// [`Self::partitions_for`] with a one-shot timeout (Java `partitionsFor(String, Duration)`).
    pub async fn partitions_for_timeout(
        &mut self,
        topic: impl Into<String>,
        timeout: Duration,
    ) -> Result<Vec<crate::PartitionInfo>> {
        self.consumer.partitions_for_timeout(topic, timeout).await
    }

    /// Cluster Metadata for every topic (Java `listTopics`).
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::list_topics_timeout`].
    pub async fn list_topics(&mut self) -> Result<Vec<crate::PartitionInfo>> {
        self.consumer.list_topics().await
    }

    /// [`Self::list_topics`] with a one-shot timeout (Java `listTopics(Duration)`).
    pub async fn list_topics_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<crate::PartitionInfo>> {
        self.consumer.list_topics_timeout(timeout).await
    }

    /// Log-start offset for each partition.
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::beginning_offsets_timeout`].
    pub async fn beginning_offsets(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        self.consumer.beginning_offsets(partitions).await
    }

    /// [`Self::beginning_offsets`] with a one-shot timeout
    /// (Java `beginningOffsets(Collection, Duration)`).
    pub async fn beginning_offsets_timeout(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        self.consumer
            .beginning_offsets_timeout(partitions, timeout)
            .await
    }

    /// High-watermark offset for each partition.
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::end_offsets_timeout`].
    pub async fn end_offsets(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        self.consumer.end_offsets(partitions).await
    }

    /// [`Self::end_offsets`] with a one-shot timeout
    /// (Java `endOffsets(Collection, Duration)`).
    pub async fn end_offsets_timeout(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        self.consumer.end_offsets_timeout(partitions, timeout).await
    }

    /// ListOffsets timestamp: `EARLIEST_TIMESTAMP` (-2), `LATEST_TIMESTAMP` (-1),
    /// `MAX_TIMESTAMP` (-3), `EARLIEST_LOCAL_TIMESTAMP` (-4),
    /// `LATEST_TIERED_TIMESTAMP` (-5), or ms.
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::list_offsets_timeout`].
    pub async fn list_offsets(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64> {
        self.consumer
            .list_offsets(topic, partition, timestamp)
            .await
    }

    /// [`Self::list_offsets`] with a one-shot timeout.
    pub async fn list_offsets_timeout(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        timestamp: i64,
        timeout: Duration,
    ) -> Result<i64> {
        self.consumer
            .list_offsets_timeout(topic, partition, timestamp, timeout)
            .await
    }

    /// [`Self::list_offsets`] for a [`TopicPartition`].
    pub async fn list_offset(
        &mut self,
        partition: impl Into<TopicPartition>,
        timestamp: i64,
    ) -> Result<i64> {
        self.consumer.list_offset(partition, timestamp).await
    }

    /// [`Self::list_offset`] with a one-shot timeout.
    pub async fn list_offset_timeout(
        &mut self,
        partition: impl Into<TopicPartition>,
        timestamp: i64,
        timeout: Duration,
    ) -> Result<i64> {
        self.consumer
            .list_offset_timeout(partition, timestamp, timeout)
            .await
    }

    /// First offset at or after each timestamp (Java `offsetsForTimes`).
    ///
    /// A negative timestamp is Java `IllegalArgumentException`
    /// (`The target time cannot be negative`).
    /// [`crate::OffsetAndTimestamp::leader_epoch`] is Java `getLeaderEpoch`.
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::offsets_for_times_timeout`].
    pub async fn offsets_for_times(
        &mut self,
        queries: impl IntoIterator<Item = (impl Into<TopicPartition>, i64)>,
    ) -> Result<Vec<(TopicPartition, Option<crate::OffsetAndTimestamp>)>> {
        self.consumer.offsets_for_times(queries).await
    }

    /// [`Self::offsets_for_times`] with a one-shot timeout
    /// (Java `offsetsForTimes(Map, Duration)`).
    pub async fn offsets_for_times_timeout(
        &mut self,
        queries: impl IntoIterator<Item = (impl Into<TopicPartition>, i64)>,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, Option<crate::OffsetAndTimestamp>)>> {
        self.consumer
            .offsets_for_times_timeout(queries, timeout)
            .await
    }

    /// Ask the coordinator to rebalance on the next [`Self::poll`] (Java `enforceRebalance`).
    ///
    /// Classic groups rejoin and send JoinGroup v8+ Reason
    /// [`DEFAULT_ENFORCE_REBALANCE_REASON`]. KIP-848 members send a join
    /// heartbeat (no Reason field). For a custom reason, use
    /// [`Self::enforce_rebalance_with`].
    pub fn enforce_rebalance(&mut self) {
        self.enforce_rebalance_with(DEFAULT_ENFORCE_REBALANCE_REASON);
    }

    /// [`Self::enforce_rebalance`] with a JoinGroup v8+ Reason (Java
    /// `enforceRebalance(String)`).
    ///
    /// Empty reason uses [`DEFAULT_ENFORCE_REBALANCE_REASON`]. The string is
    /// truncated to 255 characters (KIP-800).
    pub fn enforce_rebalance_with(&mut self, reason: impl Into<String>) {
        self.rebalance_needed = true;
        let reason = reason.into();
        self.rebalance_reason = Some(if reason.is_empty() {
            DEFAULT_ENFORCE_REBALANCE_REASON.to_string()
        } else {
            truncate_group_reason(&reason)
        });
    }

    /// Commit the delivered positions for the current assignment
    /// (Java `commitSync()` with no arguments).
    ///
    /// Commits each assigned partition's current delivered position (the next
    /// consumable offset), not the ahead-of-delivery broker fetch cursor. When
    /// [`ConsumerConfig::max_poll_records`] leaves fetched records buffered in
    /// the consumer, undelivered records remain uncommitted so they are available
    /// on restart or rejoin. The committed leader epoch is the epoch of the next
    /// undelivered record when one is buffered, otherwise the epoch of the
    /// fetched records, and the metadata leader epoch only when neither is known.
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. To commit only
    /// partitions from the last poll, pass [`ConsumerRecords::next_offsets`]
    /// to [`Self::commit_with_metadata`]. For a one-shot timeout, use
    /// [`Self::commit_timeout`].
    pub async fn commit(&mut self) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.commit_timeout(timeout).await
    }

    /// [`Self::commit`] with a one-shot timeout (Java `commitSync(Duration)`).
    pub async fn commit_timeout(&mut self, timeout: Duration) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        self.reject_sticky_classic_heartbeat()?;
        let started = Instant::now();
        if self.kip848 {
            self.check_kip848_membership().await?;
            self.apply_pending_assignment().await?;
        }
        let offsets = crate::offsets::collect_bounded_commits(
            self.consumer.delivered_commit_offsets_cooperative().await,
        )?;
        self.commit_prepared_offsets(&offsets, timeout.saturating_sub(started.elapsed()))
            .await?;
        self.snapshot_delivered_positions();
        self.last_auto_commit = Instant::now();
        Ok(())
    }

    /// Commit these offsets (`OffsetCommit`).
    ///
    /// Each item is a [`TopicPartition`] (or anything that converts to one)
    /// and the next fetch offset. Leader epoch is taken from Metadata. For
    /// epoch plus a metadata string, use [`Self::commit_with_metadata`].
    /// Waits up to [`ConsumerConfig::request_timeout`].
    pub async fn commit_offsets(
        &mut self,
        offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, i64)>,
    ) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.commit_offsets_timeout(offsets, timeout).await
    }

    /// [`Self::commit_offsets`] with a one-shot timeout.
    pub async fn commit_offsets_timeout(
        &mut self,
        offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, i64)>,
        timeout: Duration,
    ) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        let started = Instant::now();
        let mut epochs = crate::cluster::EpochList::new();
        let items =
            crate::offsets::collect_bounded_commits(offsets.into_iter().map(|(tp, offset)| {
                let tp = tp.into();
                let epoch = epochs.epoch(self.consumer.cluster(), &tp.topic, tp.partition);
                (
                    tp,
                    OffsetAndMetadata::from_wire(offset, epoch, String::new()),
                )
            }))?;
        self.commit_with_metadata_timeout(items, timeout.saturating_sub(started.elapsed()))
            .await
    }

    /// Commit offsets with optional leader epoch and user metadata.
    ///
    /// Pass [`ConsumerRecords::next_offsets`] to match Java
    /// `commitSync(records.nextOffsets())`. That commits only partitions
    /// present in the batch. [`Self::commit`] commits every assigned
    /// partition's current position. Waits up to
    /// [`ConsumerConfig::request_timeout`]. For a one-shot timeout, use
    /// [`Self::commit_with_metadata_timeout`].
    pub async fn commit_with_metadata(
        &mut self,
        offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, impl Into<OffsetAndMetadata>)>,
    ) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.commit_with_metadata_timeout(offsets, timeout).await
    }

    /// [`Self::commit_with_metadata`] with a one-shot timeout
    /// (Java `commitSync(Map, Duration)`).
    ///
    /// A pending [`crate::Consumer::wakeup`] fails this call once, before any
    /// OffsetCommit is sent.
    pub async fn commit_with_metadata_timeout(
        &mut self,
        offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, impl Into<OffsetAndMetadata>)>,
        timeout: Duration,
    ) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        self.commit_metadata_offsets(offsets, timeout).await
    }

    async fn commit_shared_positions(
        &mut self,
        positions: &[(Arc<str>, i32, i64)],
        timeout: Duration,
    ) -> Result<()> {
        if positions.is_empty() {
            return Ok(());
        }
        let result = if self.coord.offset_commit_version == 10 {
            self.commit_shared_v10(positions, timeout).await
        } else {
            let mut topics = std::mem::take(&mut self.classic_commit_topics);
            refill_shared_offset_topics_cooperative(&mut topics, positions, |topic, partition| {
                self.consumer.position_epoch(topic, partition)
            })
            .await;
            if topics.is_empty() {
                self.classic_commit_topics = topics;
                return Ok(());
            }
            let committed = self.commit_classic(&[], &topics, timeout).await;
            self.classic_commit_topics = topics;
            committed
        };
        self.note_commit(result.is_ok());
        result
    }

    async fn commit_prepared_offsets(
        &mut self,
        offsets: &[(TopicPartition, OffsetAndMetadata)],
        timeout: Duration,
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        let result = if self.coord.offset_commit_version == 10 {
            self.commit_offsets_v10(offsets, timeout, None).await
        } else {
            let mut topics = std::mem::take(&mut self.classic_commit_topics);
            refill_offset_topics_cooperative(&mut topics, offsets).await;
            let committed = self.commit_classic(offsets, &topics, timeout).await;
            self.classic_commit_topics = topics;
            committed
        };
        self.note_commit(result.is_ok());
        result
    }

    async fn commit_metadata_offsets(
        &mut self,
        offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, impl Into<OffsetAndMetadata>)>,
        timeout: Duration,
    ) -> Result<()> {
        let started = Instant::now();
        let offsets = crate::offsets::collect_bounded_commits(offsets)?;
        self.commit_prepared_offsets(&offsets, timeout.saturating_sub(started.elapsed()))
            .await
    }

    async fn commit_classic(
        &mut self,
        offsets: &[(TopicPartition, OffsetAndMetadata)],
        topics: &[OffsetTopic],
        timeout: Duration,
    ) -> Result<()> {
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        let prepared = negotiated_coord_version(&self.coord, OFFSET_COMMIT).ok();
        let mut encoded = bytes::BytesMut::new();
        if let Some(version) = prepared {
            encode_offset_commit_request_cooperative(
                &mut encoded,
                version,
                &self.group_id,
                self.generation_id,
                &self.member_id,
                self.cfg.group_instance_id.as_deref(),
                DEFAULT_RETENTION_TIME,
                topics,
            )
            .await?;
        }
        let (version, body) = coord_roundtrip_with_bootstrap(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            OFFSET_COMMIT,
            |version, buf| {
                if prepared == Some(version) {
                    buf.extend_from_slice(&encoded);
                    Ok(())
                } else {
                    encode_offset_commit_request(
                        buf,
                        version,
                        &self.group_id,
                        self.generation_id,
                        &self.member_id,
                        self.cfg.group_instance_id.as_deref(),
                        DEFAULT_RETENTION_TIME,
                        topics,
                    )
                }
            },
            &bootstrap_keys,
            &address_keys,
            &mut self.prior_coords,
            timeout,
        )
        .await?;
        let err = decode_offset_commit_response_cooperative(&mut body.clone(), version).await?;
        if err != 0 {
            return Err(Error::broker(err, "OffsetCommit"));
        }
        self.cfg.interceptors.on_commit(offsets);
        Ok(())
    }

    /// Queue an OffsetCommit of the current assignment (Java `commitAsync()`).
    ///
    /// Snapshots delivered positions now, committing only records delivered to
    /// the application and leaving buffered-but-undelivered records uncommitted.
    /// The RPC is sent on the next [`Self::poll`], [`Self::leave`], [`Self::close`],
    /// or [`Self::unsubscribe`]. Does not spawn a task. Failures are not returned from
    /// poll; use [`Self::commit_async_with`] for a callback.
    pub fn commit_async(&mut self) {
        let offsets = self.assigned_commit_offsets();
        self.queue_async_commit(offsets, None);
    }

    /// Java `commitAsync(OffsetCommitCallback)`.
    ///
    /// `callback` runs on the next poll / leave with the snapshotted delivered
    /// positions or the OffsetCommit error. Poll still returns the fetch result.
    pub fn commit_async_with<F>(&mut self, callback: F)
    where
        F: FnOnce(Result<Vec<(TopicPartition, OffsetAndMetadata)>>) + Send + 'static,
    {
        let offsets = self.assigned_commit_offsets();
        self.queue_async_commit(offsets, Some(Box::new(callback)));
    }

    /// Queue these offsets (Java `commitAsync(Map, null)`).
    ///
    /// Same send timing as [`Self::commit_async`]. For a callback, use
    /// [`Self::commit_with_metadata_async_with`].
    pub fn commit_with_metadata_async(
        &mut self,
        offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, impl Into<OffsetAndMetadata>)>,
    ) {
        let offsets = collect_commit_offsets(offsets);
        self.queue_async_commit(offsets, None);
    }

    /// Java `commitAsync(Map, OffsetCommitCallback)`.
    pub fn commit_with_metadata_async_with<F>(
        &mut self,
        offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, impl Into<OffsetAndMetadata>)>,
        callback: F,
    ) where
        F: FnOnce(Result<Vec<(TopicPartition, OffsetAndMetadata)>>) + Send + 'static,
    {
        let offsets = collect_commit_offsets(offsets);
        self.queue_async_commit(offsets, Some(Box::new(callback)));
    }

    fn assigned_commit_offsets(&self) -> Vec<(TopicPartition, OffsetAndMetadata)> {
        self.consumer.delivered_commit_offsets()
    }

    async fn flush_async_commits(&mut self, deadline: Instant) {
        let pending = std::mem::take(&mut self.pending_async_commits);
        for (offsets, callback, ids) in pending {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                if let Some(callback) = callback {
                    callback(Err(Error::Timeout));
                }
                continue;
            }
            let metadata_ids = self.consumer.topic_name_ids();
            let send = if self.coord.offset_commit_version == 10 {
                let sent = self.commit_offsets_v10(&offsets, timeout, Some(&ids)).await;
                self.note_commit(sent.is_ok());
                sent
            } else if offsets.iter().any(|(tp, _)| {
                nested_topic_id(&ids, &tp.topic, tp.partition)
                    != self
                        .consumer
                        .assigned_topic_id(&tp.topic, tp.partition)
                        .or_else(|| metadata_ids.get(tp.topic.as_str()).copied())
                        .filter(|id| *id != [0; 16])
            }) {
                self.note_commit(false);
                Err(Error::protocol(
                    "queued offset identity differs from the current assignment",
                ))
            } else if callback.is_some() {
                self.commit_metadata_offsets(offsets.clone(), timeout).await
            } else {
                let sent = self.commit_metadata_offsets(offsets, timeout).await;
                if let Err(err) = sent {
                    let _err = err;
                }
                continue;
            };
            if let Some(callback) = callback {
                callback(send.map(|()| offsets));
            } else if let Err(err) = send {
                let _err = err;
            }
        }
    }

    /// Fetch while a due interval commit is in flight.
    ///
    /// The commit uses the group coordinator. The fetch uses the leader
    /// sockets, so one slow commit does not hold the records. A failed
    /// commit puts any fetched batch back in front of the buffer.
    async fn fetch_while_interval_commits(
        &mut self,
        wait: Option<Duration>,
    ) -> Result<ConsumerRecords> {
        if !self.cfg.enable_auto_commit
            || self.cfg.auto_commit_interval.is_zero()
            || self.last_auto_commit.elapsed() < self.cfg.auto_commit_interval
            || self.coord.offset_commit_version == 10
            || !self.cfg.interceptors.is_empty()
        {
            self.maybe_auto_commit_interval().await?;
            return self.fetch_for_poll(wait).await;
        }
        let Some(mut to_commit) = self.last_delivered_positions.clone() else {
            return self.fetch_for_poll(wait).await;
        };
        to_commit.retain(|(topic, partition, offset)| {
            position_is_committable(*offset)
                && self.consumer.is_assigned(topic, *partition)
                && self
                    .last_delivered_topic_ids
                    .get(topic.as_ref())
                    .and_then(|parts| parts.get(partition))
                    .copied()
                    == self.consumer.assigned_topic_id(topic, *partition)
        });
        if to_commit.is_empty() {
            return self.fetch_for_poll(wait).await;
        }
        let timeout = self.cfg.request_timeout;
        let mut topics = std::mem::take(&mut self.classic_commit_topics);
        refill_shared_offset_topics_cooperative(&mut topics, &to_commit, |topic, partition| {
            self.consumer.position_epoch(topic, partition)
        })
        .await;
        if topics.is_empty() {
            self.classic_commit_topics = topics;
            return self.fetch_for_poll(wait).await;
        }
        let generation_id = self.generation_id;
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let instance = self.cfg.group_instance_id.clone();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        let coord = &mut self.coord;
        let cfg = &self.cfg;
        let consumer = &mut self.consumer;
        let prior = &mut self.prior_coords;
        let prepared = negotiated_coord_version(coord, OFFSET_COMMIT).ok();
        let mut encoded = bytes::BytesMut::new();
        if let Some(version) = prepared {
            encode_offset_commit_request_cooperative(
                &mut encoded,
                version,
                &group_id,
                generation_id,
                &member_id,
                instance.as_deref(),
                DEFAULT_RETENTION_TIME,
                &topics,
            )
            .await?;
        }
        let (committed, fetched) = tokio::join!(
            coord_roundtrip_with_bootstrap(
                coord,
                cfg,
                &group_id,
                COORDINATOR_GROUP,
                OFFSET_COMMIT,
                |version, buf| {
                    if prepared == Some(version) {
                        buf.extend_from_slice(&encoded);
                        Ok(())
                    } else {
                        encode_offset_commit_request(
                            buf,
                            version,
                            &group_id,
                            generation_id,
                            &member_id,
                            instance.as_deref(),
                            DEFAULT_RETENTION_TIME,
                            &topics,
                        )
                    }
                },
                &bootstrap_keys,
                &address_keys,
                prior,
                timeout,
            ),
            async {
                match wait {
                    Some(timeout) => consumer.fetch_allow_unassigned_timeout(timeout).await,
                    None => consumer.fetch_allow_unassigned().await,
                }
            },
        );
        self.classic_commit_topics = topics;
        let (version, body) = match committed {
            Ok(committed) => committed,
            Err(error) => {
                if let Ok(recs) = fetched {
                    self.consumer.restore_fetched(recs).await;
                }
                return Err(error);
            }
        };
        let err = match decode_offset_commit_response_cooperative(&mut body.clone(), version).await
        {
            Ok(err) => err,
            Err(error) => {
                if let Ok(recs) = fetched {
                    self.consumer.restore_fetched(recs).await;
                }
                return Err(error);
            }
        };
        self.note_commit(err == 0);
        if err != 0 {
            if let Ok(recs) = fetched {
                self.consumer.restore_fetched(recs).await;
            }
            return Err(Error::broker(err, "OffsetCommit"));
        }
        self.last_auto_commit = Instant::now();
        fetched
    }

    async fn fetch_for_poll(&mut self, wait: Option<Duration>) -> Result<ConsumerRecords> {
        match wait {
            Some(timeout) => self.consumer.fetch_allow_unassigned_timeout(timeout).await,
            None => self.consumer.fetch_allow_unassigned().await,
        }
    }

    async fn maybe_auto_commit_interval(&mut self) -> Result<()> {
        if !self.cfg.enable_auto_commit || self.cfg.auto_commit_interval.is_zero() {
            return Ok(());
        }
        if self.last_auto_commit.elapsed() < self.cfg.auto_commit_interval {
            return Ok(());
        }
        let Some(mut to_commit) = self.last_delivered_positions.clone() else {
            return Ok(());
        };
        to_commit.retain(|(topic, partition, offset)| {
            position_is_committable(*offset)
                && self.consumer.is_assigned(topic, *partition)
                && self
                    .last_delivered_topic_ids
                    .get(topic.as_ref())
                    .and_then(|parts| parts.get(partition))
                    .copied()
                    == self.consumer.assigned_topic_id(topic, *partition)
        });
        if !to_commit.is_empty() {
            let timeout = self.cfg.request_timeout;
            if self.cfg.interceptors.is_empty() {
                self.commit_shared_positions(&to_commit, timeout).await?;
            } else {
                let mut owned = Vec::with_capacity(to_commit.len());
                let mut walked = 0usize;
                for (topic, partition, offset) in &to_commit {
                    let epoch = self.consumer.position_epoch(topic, *partition);
                    owned.push((
                        TopicPartition::new(topic.as_ref(), *partition),
                        OffsetAndMetadata::from_wire(*offset, epoch, String::new()),
                    ));
                    walked = walked.saturating_add(1);
                    if walked == 8 * 1024 {
                        walked = 0;
                        tokio::task::yield_now().await;
                    }
                }
                self.commit_with_metadata_timeout(owned, timeout).await?;
            }
            self.last_auto_commit = Instant::now();
        }
        Ok(())
    }

    fn check_max_poll_interval(&self) -> Result<()> {
        let interval = self.cfg.max_poll_interval;
        let mut last = self.last_poll.lock();
        if !interval.is_zero() {
            if let Some(t) = *last {
                if t.elapsed() > interval {
                    return Err(Error::MaxPollInterval);
                }
            }
        }
        *last = Some(Instant::now());
        Ok(())
    }

    /// Leave the group and drop the subscription (Java `unsubscribe`).
    ///
    /// Heartbeats stop, fetch sessions are retired, and the assignment is
    /// cleared. Classic LeaveGroup v5 sends
    /// [`LEAVE_GROUP_REASON_UNSUBSCRIBED`]. [`Self::subscribe`] joins again
    /// with a new topic list. [`Self::leave`] after this is a no-op.
    ///
    /// Flushes queued `commitAsync` work only — does **not** auto-commit
    /// positions when [`ConsumerConfig::enable_auto_commit`] is on (KL-02).
    pub async fn unsubscribe(&mut self) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        let deadline = Instant::now() + self.cfg.request_timeout;
        self.hb_stop.send(true).unwrap_or(());
        let heartbeat = self.hb_task.take();
        if self.member_id.is_empty() {
            self.topics.clear();
            self.publish_hb_topics().await;
            self.topic_match = None;
            self.consumer.clear_assignment();
            if let Some(heartbeat) = heartbeat {
                finish_heartbeat_task(heartbeat, deadline).await;
            }
            return Ok(());
        }
        self.flush_async_commits(deadline).await;
        // KL-02: do not auto-commit on unsubscribe — poll-interval auto-commit
        // and explicit commit* are the only OffsetCommit paths for positions.
        *self.hb_deadline.lock() = None;
        let revoked = self.consumer.assignment_cooperative().await;
        if self.kip848 {
            self.adopt_kip848_member_id();
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        let kip848 = self.kip848;
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        let coord = &mut self.coord;
        let cfg = &self.cfg;
        let consumer = &mut self.consumer;
        let prior = &mut self.prior_coords;
        let (left, _) = tokio::join!(
            Self::leave_joined_member(
                coord,
                cfg,
                &group_id,
                &member_id,
                kip848,
                LEAVE_GROUP_REASON_UNSUBSCRIBED,
                &bootstrap_keys,
                &address_keys,
                prior,
                deadline,
            ),
            consumer.close_fetch_sessions(timeout),
        );
        left?;
        if !revoked.is_empty() {
            self.cfg.rebalance.call(&revoked, &[]);
        }
        self.consumer.clear_assignment();
        self.topics.clear();
        self.publish_hb_topics().await;
        self.topic_match = None;
        self.member_id.clear();
        self.generation_id = 0;
        self.hb_generation.store(0, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        *self.hb_assignment.lock() = None;
        *self.hb_ack.lock() = None;
        self.prev_assignment.clear();
        if let Some(heartbeat) = heartbeat {
            finish_heartbeat_task(heartbeat, deadline).await;
        }
        Ok(())
    }

    /// Replace the subscription and (re)join (Java `subscribe`).
    ///
    /// If this member is already in the group, the coordinator is not left;
    /// a rejoin uses the new topic list. After [`Self::unsubscribe`], this
    /// starts a new heartbeat loop. Drops a [`Self::subscribe_matching`]
    /// predicate, if one was set.
    pub async fn subscribe(
        &mut self,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<()> {
        self.topic_match = None;
        let topics = collect_topics(topics)?;
        self.apply_subscription(topics).await
    }

    /// Subscribe to cluster topics for which `matches` is true
    /// (Java `subscribe(Pattern)`).
    ///
    /// Names starting with `__` are skipped (Java `exclude.internal.topics`).
    /// [`Self::poll`] re-lists Metadata when [`ConsumerConfig::metadata_max_age`]
    /// has elapsed (every poll when that age is zero). [`Self::subscribe`]
    /// with an explicit list drops the predicate.
    pub async fn subscribe_matching(
        &mut self,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        self.topic_match = Some(Arc::new(matches));
        let topics = self.matching_topic_names_within(deadline).await?;
        self.last_match_refresh = Instant::now();
        self.apply_subscription_within(topics, deadline).await
    }

    async fn apply_subscription(&mut self, topics: Vec<String>) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        self.apply_subscription_within(topics, deadline).await
    }

    async fn apply_subscription_within(
        &mut self,
        topics: Vec<String>,
        deadline: Instant,
    ) -> Result<()> {
        if self.consumer.take_wakeup() {
            return Err(Error::Wakeup);
        }
        if topics == self.topics && !self.member_id.is_empty() {
            return Ok(());
        }
        let rejoining = !self.member_id.is_empty();
        if rejoining {
            if self.cfg.enable_auto_commit {
                let to_commit = self.consumer.delivered_commit_offsets_cooperative().await;
                if !to_commit.is_empty() {
                    let timeout = deadline.saturating_duration_since(Instant::now());
                    if timeout.is_zero() {
                        return Err(Error::Timeout);
                    }
                    let consumer = &self.consumer;
                    let stale = !crate::consumer::topics_metadata_fresh(
                        topics.iter().map(String::as_str),
                        |topic| consumer.topic_metadata_fresh(topic),
                    )
                    .await;
                    if stale {
                        self.commit_offsets_while_refreshing(&to_commit, &topics, timeout)
                            .await?;
                    } else {
                        self.commit_metadata_offsets(to_commit, timeout).await?;
                    }
                }
            }
            let revoked = self.consumer.assignment_cooperative().await;
            self.consumer.clear_assignment();
            if !revoked.is_empty() {
                self.cfg.rebalance.call(&revoked, &[]);
            }
            self.prev_assignment.clear();
        }
        self.topics = topics;
        self.publish_hb_topics().await;
        self.left_max_poll.store(false, Ordering::SeqCst);
        if rejoining {
            if self.kip848 {
                self.heartbeat_join_within(deadline).await?;
            } else {
                self.rejoin_within(deadline).await?;
            }
            return Ok(());
        }
        let (hb_stop, hb_rx) = watch::channel(false);
        self.hb_stop = hb_stop;
        if self.kip848 {
            self.heartbeat_join_within(deadline).await?;
            self.hb_task = Some(self.spawn_heartbeat_consumer(hb_rx));
        } else {
            self.rejoin_within(deadline).await?;
            self.hb_task = Some(self.spawn_heartbeat(hb_rx));
        }
        Ok(())
    }

    async fn matching_topic_names_within(&mut self, deadline: Instant) -> Result<Vec<String>> {
        let Some(pred) = self.topic_match.clone() else {
            return Ok(clone_names(&self.topics).await);
        };
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let infos = self.consumer.list_topics_timeout(timeout).await?;
        Ok(
            filter_matching_topics_cooperative(infos.iter().map(|i| i.topic.as_str()), |n| pred(n))
                .await,
        )
    }

    async fn maybe_refresh_matching(&mut self) -> Result<()> {
        if self.topic_match.is_none() {
            return Ok(());
        }
        let age = self.cfg.metadata_max_age;
        if !age.is_zero() && self.last_match_refresh.elapsed() < age {
            return Ok(());
        }
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        let topics = self.matching_topic_names_within(deadline).await?;
        self.last_match_refresh = Instant::now();
        self.apply_subscription_within(topics, deadline).await
    }

    /// Leave the group (`LeaveGroup` or KIP-848 heartbeat leave).
    ///
    /// Classic LeaveGroup v5 sends [`LEAVE_GROUP_REASON_CLOSED`]. KIP-848
    /// sends [`ConsumerGroupHeartbeatRequest::leave_group_epoch`] (static
    /// members send [`ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH`]).
    ///
    /// Flushes queued `commitAsync` work, then leaves. Does **not** auto-commit
    /// positions even when [`ConsumerConfig::enable_auto_commit`] is on (KL-02:
    /// close must not commit polled-but-unprocessed records). Use poll-interval
    /// auto-commit or an explicit `commit*` before leave if you want stored offsets.
    pub async fn leave(self) -> Result<()> {
        let deadline = Instant::now() + self.cfg.request_timeout;
        self.leave_within(deadline).await
    }

    async fn leave_within(mut self, deadline: Instant) -> Result<()> {
        self.hb_stop.send(true).unwrap_or(());
        let heartbeat = self.hb_task.take();
        if self.member_id.is_empty() {
            *self.hb_deadline.lock() = None;
            self.consumer.close_interceptors();
            self.consumer
                .close_fetch_sessions(deadline.saturating_duration_since(Instant::now()))
                .await;
            if let Some(heartbeat) = heartbeat {
                finish_heartbeat_task(heartbeat, deadline).await;
            }
            // The member socket drops with this group. Yield so the broker
            // observes that close before leave returns.
            drop(self);
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            return Ok(());
        }
        self.flush_async_commits(deadline).await;
        // KL-02: never auto-commit positions on leave/close.
        *self.hb_deadline.lock() = None;
        if self.kip848 {
            self.adopt_kip848_member_id();
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        let kip848 = self.kip848;
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        let coord = &mut self.coord;
        let cfg = &self.cfg;
        let consumer = &mut self.consumer;
        let prior = &mut self.prior_coords;
        let (out, _) = tokio::join!(
            Self::leave_joined_member(
                coord,
                cfg,
                &group_id,
                &member_id,
                kip848,
                LEAVE_GROUP_REASON_CLOSED,
                &bootstrap_keys,
                &address_keys,
                prior,
                deadline,
            ),
            consumer.close_fetch_sessions(timeout),
        );
        self.consumer.close_interceptors();
        if let Some(heartbeat) = heartbeat {
            finish_heartbeat_task(heartbeat, deadline).await;
        }
        // Member and coordinator sockets drop with this group. Yield so the
        // broker observes that close before leave returns.
        drop(self);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        out
    }

    /// Leave the group. Same as [`Self::leave`] (no position auto-commit on close).
    pub async fn close(self) -> Result<()> {
        self.leave().await
    }

    /// Leave the group, waiting up to `timeout` (Java `close(Duration)`).
    ///
    /// [`Self::leave`] / [`Self::close`] wait up to
    /// [`ConsumerConfig::request_timeout`] for the coordinator. A shorter
    /// `timeout` returns [`Error::Timeout`] if leave does not finish in time.
    pub async fn close_timeout(self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::protocol("close deadline overflow"))?;
        self.leave_within(deadline).await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "leave carries the coordinator versions learned earlier"
    )]
    async fn leave_joined_member(
        coord: &mut BrokerConn,
        cfg: &ConsumerConfig,
        group_id: &str,
        member_id: &str,
        kip848: bool,
        reason: &str,
        bootstrap_keys: &[ApiVersion],
        address_keys: &HashMap<String, Vec<ApiVersion>>,
        prior: &mut HashMap<String, KnownCoord>,
        deadline: Instant,
    ) -> Result<()> {
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        if kip848 {
            let req = ConsumerGroupHeartbeatRequest {
                group_id: group_id.to_string(),
                member_id: member_id.to_string(),
                member_epoch: ConsumerGroupHeartbeatRequest::leave_group_epoch(
                    cfg.group_instance_id.as_deref(),
                ),
                instance_id: cfg.group_instance_id.clone(),
                rack_id: cfg.rack.clone(),
                rebalance_timeout_ms: ConsumerGroupHeartbeatRequest::UNCHANGED_REBALANCE_TIMEOUT_MS,
                subscribed_topic_names: None,
                subscribed_topic_regex: None,
                server_assignor: None,
                topic_partitions: None,
            };
            let prepared = negotiated_coord_version(coord, CONSUMER_GROUP_HEARTBEAT).ok();
            let mut encoded = bytes::BytesMut::new();
            if let Some(version) = prepared {
                encode_consumer_group_heartbeat_request_cooperative(&mut encoded, version, &req)
                    .await?;
            }
            let (version, body) = coord_roundtrip_with_bootstrap(
                coord,
                cfg,
                group_id,
                COORDINATOR_GROUP,
                CONSUMER_GROUP_HEARTBEAT,
                |version, buf| {
                    if prepared == Some(version) {
                        buf.extend_from_slice(&encoded);
                        Ok(())
                    } else {
                        encode_consumer_group_heartbeat_request(buf, version, &req)
                    }
                },
                bootstrap_keys,
                address_keys,
                prior,
                timeout,
            )
            .await?;
            let resp =
                decode_consumer_group_heartbeat_response_cooperative(&mut body.clone(), version)
                    .await?;
            if resp.error_code != 0 {
                return Err(Error::broker(
                    resp.error_code,
                    "ConsumerGroupHeartbeat leave",
                ));
            }
            return Ok(());
        }
        let members = [LeaveGroupMember {
            member_id: member_id.to_string(),
            group_instance_id: cfg.group_instance_id.clone(),
            reason: Some(truncate_group_reason(reason)),
        }];
        let (version, body) = coord_roundtrip_with_bootstrap(
            coord,
            cfg,
            group_id,
            COORDINATOR_GROUP,
            LEAVE_GROUP,
            |version, buf| encode_leave_group_request_members(buf, version, group_id, &members),
            bootstrap_keys,
            address_keys,
            prior,
            timeout,
        )
        .await?;
        let (err, results, ..) =
            decode_leave_group_response_version_cooperative(&mut body.clone(), version).await?;
        if err != 0 {
            return Err(Error::broker(err, "LeaveGroup"));
        }
        if let Some(m) = results.first() {
            if m.error_code != 0 {
                return Err(Error::broker(m.error_code, "LeaveGroup"));
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), fields(protocol = %self.protocol)))]
    async fn rejoin(&mut self) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        self.rejoin_within(deadline).await
    }

    async fn rejoin_within(&mut self, deadline: Instant) -> Result<()> {
        let rejoining = !self.member_id.is_empty();
        for _ in 0..8 {
            let revoked = self.rejoin_once(deadline).await?;
            if self.protocol != "cooperative-sticky" || revoked.is_empty() {
                if rejoining {
                    let _ = self.m_rebalances.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
        }
        Err(Error::protocol("cooperative-sticky did not settle"))
    }

    async fn join_protocol_entries(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let owned = self.consumer.assignment_pairs_cooperative().await;
        let names = if self.assignors.is_empty() {
            vec![self.protocol.clone()]
        } else {
            self.assignors.clone()
        };
        let metadata =
            ConsumerProtocol::serialize_subscription_cooperative(&ConsumerProtocolSubscription {
                topics: clone_names(&self.topics).await,
                owned_partitions: owned,
                generation_id: self.generation_id,
                rack_id: self.cfg.rack.clone(),
            })
            .await?;
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            out.push((name, metadata.clone()));
        }
        Ok(out)
    }

    async fn rejoin_once(&mut self, deadline: Instant) -> Result<Vec<(String, i32)>> {
        let entries = self.join_protocol_entries().await?;
        let protocols: Vec<JoinGroupProtocol<'_>> = entries
            .iter()
            .map(|(n, m)| JoinGroupProtocol::new(n, m))
            .collect();
        let reason = self.rebalance_reason.take();
        let session_timeout_ms = self.cfg.session_timeout_ms;
        let rebalance_timeout_ms = if self.cfg.max_poll_interval.is_zero() {
            i32::MAX
        } else {
            duration_millis_i32(self.cfg.max_poll_interval)
        };
        // Metadata for this member's topics overlaps the first JoinGroup.
        // Other members' topics are still loaded after the response.
        let mut metadata_prefetch: Option<Result<()>> = None;
        let (error, generation, protocol, leader, assigned_id, skip_assignment, members, ..) = loop {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let consumer = &self.consumer;
            let prefetch = metadata_prefetch.is_none()
                && !crate::consumer::topics_metadata_fresh(
                    self.topics.iter().map(String::as_str),
                    |topic| consumer.topic_metadata_fresh(topic),
                )
                .await;
            let (version, body) = if prefetch {
                let bootstrap_keys = self.consumer.copy_api_keys();
                let address_keys = self.consumer.copy_learned_broker_keys();
                let group_id = self.group_id.as_str();
                let member_id = self.member_id.as_str();
                let instance = self.cfg.group_instance_id.as_deref();
                let reason = reason.as_deref();
                let coord = &mut self.coord;
                let cfg = &self.cfg;
                let consumer = &mut self.consumer;
                let topics = &self.topics;
                let prior = &mut self.prior_coords;
                let refresh = consumer.refresh_metadata_timeout(Some(topics), timeout);
                let rpc = coord_roundtrip_with_bootstrap(
                    coord,
                    cfg,
                    group_id,
                    COORDINATOR_GROUP,
                    JOIN_GROUP,
                    |version, buf| {
                        encode_join_group_protocols_request(
                            buf,
                            version,
                            &JoinGroupProtocolsRequest {
                                group_id,
                                session_timeout_ms,
                                rebalance_timeout_ms,
                                member_id,
                                group_instance_id: instance,
                                protocol_type: ConsumerProtocol::PROTOCOL_TYPE,
                                protocols: &protocols,
                                reason,
                            },
                        )
                    },
                    &bootstrap_keys,
                    &address_keys,
                    prior,
                    timeout,
                );
                let (refresh, rpc) = tokio::join!(refresh, rpc);
                metadata_prefetch = Some(refresh);
                rpc?
            } else {
                let bootstrap_keys = self.consumer.copy_api_keys();
                let address_keys = self.consumer.copy_learned_broker_keys();
                coord_roundtrip_with_bootstrap(
                    &mut self.coord,
                    &self.cfg,
                    &self.group_id,
                    COORDINATOR_GROUP,
                    JOIN_GROUP,
                    |version, buf| {
                        encode_join_group_protocols_request(
                            buf,
                            version,
                            &JoinGroupProtocolsRequest {
                                group_id: &self.group_id,
                                session_timeout_ms,
                                rebalance_timeout_ms,
                                member_id: &self.member_id,
                                group_instance_id: self.cfg.group_instance_id.as_deref(),
                                protocol_type: ConsumerProtocol::PROTOCOL_TYPE,
                                protocols: &protocols,
                                reason: reason.as_deref(),
                            },
                        )
                    },
                    &bootstrap_keys,
                    &address_keys,
                    &mut self.prior_coords,
                    timeout,
                )
                .await?
            };
            let decoded = decode_join_group_response_cooperative(&mut body.clone(), version)
                .await
                .map_err(|e| group_decode_context(e, "JoinGroup", version, body.len()))?;
            if decoded.0 == error::MEMBER_ID_REQUIRED
                && self.member_id == JoinGroupRequest::UNKNOWN_MEMBER_ID
                && !decoded.4.is_empty()
            {
                self.member_id = decoded.4;
                continue;
            }
            break decoded;
        };
        if error != 0 {
            return Err(Error::broker(error, "JoinGroup"));
        }
        if let Some(Err(error)) = metadata_prefetch {
            return Err(error);
        }
        self.member_id = assigned_id;
        self.generation_id = generation;
        if !protocol.is_empty() {
            self.protocol = protocol;
        }

        let mut member_subs: Vec<(String, Vec<String>)> = Vec::with_capacity(members.len());
        let mut owned_claims: Vec<StickyOwnedClaim> = Vec::with_capacity(members.len());
        let mut topic_set = clone_names(&self.topics).await;
        let mut seen_topics = HashSet::new();
        remember_existing_topics(&mut seen_topics, &topic_set).await;
        for m in &members {
            let sub =
                match ConsumerProtocol::deserialize_subscription_cooperative(&m.metadata).await {
                    Ok(s) if !s.topics.is_empty() => s,
                    Ok(s) => ConsumerProtocolSubscription {
                        topics: clone_names(&self.topics).await,
                        owned_partitions: s.owned_partitions,
                        generation_id: s.generation_id,
                        rack_id: s.rack_id,
                    },
                    Err(_) => ConsumerProtocolSubscription::new(clone_names(&self.topics).await),
                };
            extend_unique_topics(&mut topic_set, &mut seen_topics, &sub.topics).await;
            member_subs.push((m.member_id.clone(), sub.topics));
            owned_claims.push((m.member_id.clone(), sub.owned_partitions, sub.generation_id));
        }
        let owned_prev = resolve_sticky_owned_partitions_cooperative(&owned_claims).await;
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        // A rejoin of topics already in the metadata image does not ask again.
        // Any missing or stale name refreshes the whole set: a subset response
        // would replace the image `partition_ids` reads.
        let mut by_topic = Vec::with_capacity(topic_set.len());
        let consumer = &self.consumer;
        let mut loaded =
            crate::consumer::topics_metadata_fresh(topic_set.iter().map(String::as_str), |topic| {
                consumer.topic_metadata_fresh(topic)
            })
            .await;
        if loaded {
            match self.consumer.partition_ids_all(&topic_set).await {
                Ok(lists) => {
                    for (topic, ids) in topic_set.iter().zip(lists) {
                        by_topic.push((topic.clone(), ids));
                    }
                }
                Err(_) => loaded = false,
            }
        }
        if !loaded {
            self.consumer
                .refresh_metadata_timeout(Some(&topic_set), timeout)
                .await?;
            by_topic.clear();
            let lists = self.consumer.partition_ids_all(&topic_set).await?;
            for (topic, ids) in topic_set.iter().zip(lists) {
                by_topic.push((topic.clone(), ids));
            }
        }
        let assigning = JoinGroupResponse::is_leader(&self.member_id, &leader) && !skip_assignment;
        let assignments = if assigning {
            let map = match self.protocol.as_str() {
                "cooperative-sticky" => {
                    assign_cooperative_sticky_subscribed_cooperative(
                        &member_subs,
                        &by_topic,
                        &owned_prev,
                    )
                    .await
                }
                "sticky" => {
                    assign_sticky_subscribed_cooperative(&member_subs, &by_topic, &owned_prev).await
                }
                _ => assign_range_subscribed_cooperative(&member_subs, &by_topic).await,
            };
            self.prev_assignment = map.clone();
            let mut assignments = Vec::with_capacity(map.len());
            for (id, tps) in map {
                assignments.push((id, encode_tp_assignment_cooperative(&tps).await?));
            }
            assignments
        } else {
            Vec::new()
        };
        // The assigning leader already chose its partitions. Dial those
        // brokers while SyncGroup runs so offset fetch and the next poll
        // do not pay the handshake after the coordinator answers.
        let mut leaders = Vec::new();
        let mut seen_leaders = HashSet::new();
        if assigning {
            if let Some(parts) = self.prev_assignment.get(&self.member_id) {
                remember_partition_leaders(
                    self.consumer.cluster(),
                    parts,
                    &mut leaders,
                    &mut seen_leaders,
                )
                .await;
            }
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        let mut flight = if leaders.is_empty() {
            None
        } else {
            Some(self.consumer.start_leader_dials(&leaders, deadline).await)
        };
        let prepared_version = negotiated_coord_version(&self.coord, SYNC_GROUP).ok();
        let sync_request = SyncGroupRequest {
            group_id: &self.group_id,
            generation_id: self.generation_id,
            member_id: &self.member_id,
            group_instance_id: self.cfg.group_instance_id.as_deref(),
            protocol_type: ConsumerProtocol::PROTOCOL_TYPE,
            protocol_name: &self.protocol,
            assignments: &assignments,
        };
        let mut payload = BytesMut::new();
        if let Some(version) = prepared_version {
            encode_sync_group_request_cooperative(&mut payload, version, &sync_request).await?;
        }
        let sync_fut = coord_roundtrip_with_bootstrap(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            SYNC_GROUP,
            |version, buf| {
                if prepared_version == Some(version) {
                    buf.extend_from_slice(&payload);
                    Ok(())
                } else {
                    encode_sync_group_request(buf, version, &sync_request)
                }
            },
            &bootstrap_keys,
            &address_keys,
            &mut self.prior_coords,
            timeout,
        );
        let synced = if let Some(flight) = flight.as_mut() {
            let (synced, ()) = tokio::join!(sync_fut, flight);
            synced
        } else {
            sync_fut.await
        };
        if let Some(flight) = flight {
            flight.store(&mut self.consumer);
        }
        let (version, body) = synced?;
        let (err, assignment, ..) = decode_sync_group_response(&mut body.clone(), version)
            .map_err(|e| group_decode_context(e, "SyncGroup", version, body.len()))?;
        if err != 0 {
            return Err(Error::broker(err, "SyncGroup"));
        }
        let assigned = decode_assignment_cooperative(&assignment)
            .await
            .map_err(|e| {
                group_decode_context(e, "ConsumerProtocol assignment", version, assignment.len())
            })?;
        let wanted = expand_topic_partitions_cooperative(assigned).await;
        let revoked = self.assign_committed(&wanted, deadline, false).await?;
        self.hb_generation
            .store(self.generation_id, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(revoked)
    }

    async fn heartbeat_join(&mut self) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(self.cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        self.heartbeat_join_within(deadline).await
    }

    async fn heartbeat_join_within(&mut self, deadline: Instant) -> Result<()> {
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let version = spoken_consumer_group_heartbeat(self.coord.consumer_group_heartbeat_version)?;
        if version >= ConsumerGroupHeartbeatRequest::CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION
            && self.member_id.is_empty()
        {
            self.member_id = new_kip848_member_id()?;
        }
        let req = ConsumerGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            instance_id: self.cfg.group_instance_id.clone(),
            rack_id: self.cfg.rack.clone(),
            rebalance_timeout_ms: if self.cfg.max_poll_interval.is_zero() {
                i32::MAX
            } else {
                duration_millis_i32(self.cfg.max_poll_interval)
            },
            subscribed_topic_names: Some(clone_names(&self.topics).await),
            subscribed_topic_regex: None,
            server_assignor: None,
            // Broker rejects null TopicPartitions on join ("must be empty when
            // (re-)joining"); send an empty array instead of null/unchanged.
            topic_partitions: Some(Vec::new()),
        };
        // Topic names are already on the heartbeat. Metadata only has to be
        // ready when the assignment is resolved, so a stale image loads while
        // the coordinator join is in flight.
        let consumer = &self.consumer;
        let stale = !crate::consumer::topics_metadata_fresh(
            self.topics.iter().map(String::as_str),
            |topic| consumer.topic_metadata_fresh(topic),
        )
        .await;
        let mut metadata_refresh = None;
        let (version, body) = if stale {
            let bootstrap_keys = self.consumer.copy_api_keys();
            let address_keys = self.consumer.copy_learned_broker_keys();
            let group_id = self.group_id.as_str();
            let coord = &mut self.coord;
            let cfg = &self.cfg;
            let consumer = &mut self.consumer;
            let topics = &self.topics;
            let prior = &mut self.prior_coords;
            let refresh = consumer.refresh_metadata_timeout(Some(topics), timeout);
            let prepared = negotiated_coord_version(coord, CONSUMER_GROUP_HEARTBEAT).ok();
            let mut encoded = bytes::BytesMut::new();
            if let Some(version) = prepared {
                encode_consumer_group_heartbeat_request_cooperative(&mut encoded, version, &req)
                    .await?;
            }
            let rpc = coord_roundtrip_with_bootstrap(
                coord,
                cfg,
                group_id,
                COORDINATOR_GROUP,
                CONSUMER_GROUP_HEARTBEAT,
                |version, buf| {
                    if prepared == Some(version) {
                        buf.extend_from_slice(&encoded);
                        Ok(())
                    } else {
                        encode_consumer_group_heartbeat_request(buf, version, &req)
                    }
                },
                &bootstrap_keys,
                &address_keys,
                prior,
                timeout,
            );
            let (refresh, rpc) = tokio::join!(refresh, rpc);
            metadata_refresh = Some(refresh);
            rpc?
        } else {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let bootstrap_keys = self.consumer.copy_api_keys();
            let address_keys = self.consumer.copy_learned_broker_keys();
            let prepared = negotiated_coord_version(&self.coord, CONSUMER_GROUP_HEARTBEAT).ok();
            let mut encoded = bytes::BytesMut::new();
            if let Some(version) = prepared {
                encode_consumer_group_heartbeat_request_cooperative(&mut encoded, version, &req)
                    .await?;
            }
            coord_roundtrip_with_bootstrap(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
                CONSUMER_GROUP_HEARTBEAT,
                |version, buf| {
                    if prepared == Some(version) {
                        buf.extend_from_slice(&encoded);
                        Ok(())
                    } else {
                        encode_consumer_group_heartbeat_request(buf, version, &req)
                    }
                },
                &bootstrap_keys,
                &address_keys,
                &mut self.prior_coords,
                timeout,
            )
            .await?
        };
        let resp = decode_consumer_group_heartbeat_response_cooperative(&mut body.clone(), version)
            .await?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ConsumerGroupHeartbeat"));
        }
        if resp.heartbeat_interval_ms <= 0 {
            return Err(Error::protocol(format!(
                "invalid ConsumerGroupHeartbeat heartbeat_interval_ms: {}",
                resp.heartbeat_interval_ms
            )));
        }
        self.hb_interval_ms
            .store(resp.heartbeat_interval_ms, Ordering::SeqCst);
        let interval =
            Duration::from_millis(u64::try_from(resp.heartbeat_interval_ms).unwrap_or(0));
        let next_heartbeat = Instant::now() + interval;
        *self.hb_deadline.lock() = Some(next_heartbeat);
        if let Some(id) = resp.member_id {
            self.member_id = id;
        }
        // The join response is authoritative: publish the member id for the
        // heartbeat task and drop any pre-join pending assignment so it can
        // never clobber the fresh join below (KL03-15).
        *self.hb_member_id.lock() = self.member_id.clone();
        let _ = self.hb_assignment.lock().take();
        self.generation_id = resp.member_epoch;
        if let Some(Err(error)) = metadata_refresh {
            self.hb_generation
                .store(self.generation_id, Ordering::SeqCst);
            self.hb_err.store(0, Ordering::SeqCst);
            return Err(error);
        }
        let assignment = resp.assignment.unwrap_or_default();
        let names = self.consumer.topic_id_names();
        if (self.coord.offset_commit_version == 10 || self.coord.offset_fetch_version == 10)
            && assignment_has_unknown_topic_id(&assignment, names).await
        {
            return Err(Error::protocol(
                "group assignment contains an unknown topic UUID",
            ));
        }
        let wanted = wanted_from_kip848(&self.topics, names, &assignment).await;
        let _ = self.assign_committed(&wanted, deadline, false).await?;
        *self.hb_ack.lock() = Some(assignment);
        self.hb_generation
            .store(self.generation_id, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(())
    }

    async fn publish_hb_topics(&self) {
        *self.hb_topics.lock() = clone_names(&self.topics).await;
    }

    fn reject_unsupported_heartbeat(&self) -> Result<()> {
        if !self.kip848 && self.hb_err.load(Ordering::SeqCst) == error::UNSUPPORTED_VERSION {
            return Err(Error::broker(error::UNSUPPORTED_VERSION, "Heartbeat"));
        }
        Ok(())
    }

    fn reject_sticky_classic_heartbeat(&self) -> Result<()> {
        if self.kip848 {
            return Ok(());
        }
        let code = self.hb_err.load(Ordering::SeqCst);
        if code != 0 {
            return Err(Error::broker(code, "Heartbeat"));
        }
        Ok(())
    }

    /// Reconcile the poll path with the KIP-848 heartbeat loop (KL03-15).
    ///
    /// A fenced epoch or unknown member id means this membership is dead:
    /// rejoin before fetching or committing anything so no RPC uses stale
    /// membership metadata. Any other sticky heartbeat error (auth, the
    /// malformed-response sentinel, ...) surfaces instead of spinning on a
    /// dead membership. Transport blips never land in `hb_err`: the loop
    /// retries those silently.
    async fn check_kip848_membership(&mut self) -> Result<()> {
        match self.hb_err.load(Ordering::SeqCst) {
            0 => Ok(()),
            error::FENCED_MEMBER_EPOCH | error::UNKNOWN_MEMBER_ID => {
                self.rebalance_reason = None;
                self.heartbeat_join().await
            }
            code => Err(Error::broker(code, "ConsumerGroupHeartbeat")),
        }
    }

    /// Copy a broker-rotated member id from the heartbeat task (KL03-15).
    fn adopt_kip848_member_id(&mut self) {
        if self.kip848 {
            copy_member_id(&mut self.member_id, &self.hb_member_id.lock());
        }
    }

    async fn apply_pending_assignment(&mut self) -> Result<()> {
        self.adopt_kip848_member_id();
        let assignment = {
            let pending = self.hb_assignment.lock();
            let Some(assignment) = pending.as_ref() else {
                return Ok(());
            };
            assignment.clone()
        };
        let deadline = Instant::now() + self.cfg.request_timeout;
        let missing_topic =
            assignment_has_unknown_topic_id(&assignment, self.consumer.topic_id_names()).await;
        let consumer = &self.consumer;
        let stale = !crate::consumer::topics_metadata_fresh(
            self.topics.iter().map(String::as_str),
            |topic| consumer.topic_metadata_fresh(topic),
        )
        .await;
        // Topic ids have to be known before the assignment can be named.
        // A stale image can reload while OffsetFetch runs.
        let overlap_metadata = stale && !missing_topic;
        if missing_topic {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            self.consumer
                .refresh_metadata_timeout(Some(&self.topics), timeout)
                .await?;
        }
        let names = self.consumer.topic_id_names();
        if (self.coord.offset_commit_version == 10 || self.coord.offset_fetch_version == 10)
            && assignment_has_unknown_topic_id(&assignment, names).await
        {
            return Err(Error::protocol(
                "group assignment contains an unknown topic UUID",
            ));
        }
        let wanted = wanted_from_kip848(&self.topics, names, &assignment).await;
        if !kip848_assignment_can_apply(&wanted, &assignment) {
            return Ok(());
        }
        let _revoked = self
            .assign_committed(&wanted, deadline, overlap_metadata)
            .await?;
        {
            let mut pending = self.hb_assignment.lock();
            if pending.as_ref() == Some(&assignment) {
                *pending = None;
            }
        }
        *self.hb_ack.lock() = Some(assignment);
        self.generation_id = self.hb_generation.load(Ordering::SeqCst);
        Ok(())
    }

    async fn keep_leader_flight(&mut self, flight: LeaderDialFlight, deadline: Instant) {
        flight.finish(&mut self.consumer, deadline).await;
    }

    async fn assign_committed(
        &mut self,
        wanted: &[(String, i32)],
        deadline: Instant,
        overlap_metadata: bool,
    ) -> Result<Vec<(String, i32)>> {
        let identities = clone_topic_ids(self.consumer.topic_name_ids()).await;
        let all_current = self.consumer.assigned_offsets().to_vec();
        let mut current: HashMap<String, HashMap<i32, i64>> = HashMap::new();
        let mut walked = 0usize;
        for (topic, partition, offset) in &all_current {
            if self.consumer.assigned_topic_id(topic.as_ref(), *partition)
                == identities
                    .get(topic.as_ref())
                    .copied()
                    .filter(|id| *id != [0; 16])
            {
                remember_i64(&mut current, topic.as_ref(), *partition, *offset);
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
        let mut prev: HashMap<String, HashSet<i32>> = HashMap::new();
        remember_current_partitions(&mut prev, &all_current).await;
        let mut next: HashMap<String, HashSet<i32>> = HashMap::new();
        remember_wanted_partitions(&mut next, wanted).await;
        let mut revoked = Vec::new();
        walked = 0;
        for (topic, parts) in &prev {
            for partition in parts {
                let still_wanted = next.get(topic).is_some_and(|kept| kept.contains(partition));
                let same_identity = nested_i64(&current, topic, *partition).is_some();
                if !still_wanted || !same_identity {
                    revoked.push((topic.clone(), *partition));
                }
                walked = walked.saturating_add(1);
                if walked == 8 * 1024 {
                    walked = 0;
                    tokio::task::yield_now().await;
                }
            }
        }
        // KL03-14: with auto-commit on, commit the revoked partitions'
        // delivered positions before the new assignment drops their pending
        // records, so the next owner resumes after delivered work instead of
        // replaying it. Retained partitions keep their positions and pending
        // records untouched; the poll-interval commit covers them. A commit
        // failure propagates: the rebalance reports the error rather than
        // silently handing over uncommitted work.
        if !revoked.is_empty() && self.cfg.enable_auto_commit {
            // Membership uses the names `revoked` already owns. A
            // `(String, i32)` set would copy each name again on insert and
            // once more for every position that asks.
            let revoked_parts = index_revoked(&revoked).await;
            let positions = self.consumer.positions_cooperative().await;
            let to_commit = revoked_commits(
                positions,
                |topic, partition| self.consumer.position_epoch(topic, partition),
                |tp, offset| {
                    position_is_committable(offset)
                        && revoked_contains(&revoked_parts, &tp.topic, tp.partition)
                },
            )
            .await;
            if !to_commit.is_empty() {
                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    return Err(Error::Timeout);
                }
                self.commit_metadata_offsets(to_commit, timeout).await?;
            }
        }
        let mut kept_epochs: HashMap<String, HashMap<i32, i32>> = HashMap::new();
        walked = 0;
        for (topic, partition) in wanted {
            if nested_i64(&current, topic, *partition).is_some() {
                remember_i32(
                    &mut kept_epochs,
                    topic,
                    *partition,
                    self.consumer.last_fetched_epoch(topic, *partition),
                );
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
        let mut added = Vec::new();
        walked = 0;
        for (topic, partition) in wanted {
            if nested_i64(&current, topic, *partition).is_none() {
                added.push((topic.clone(), *partition));
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
        let reset = self.cfg.auto_offset_reset;
        let mut warm_leaders = false;
        let fetched = if added.is_empty() {
            if overlap_metadata {
                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    return Err(Error::Timeout);
                }
                self.consumer
                    .refresh_metadata_timeout(Some(&self.topics), timeout)
                    .await?;
            }
            Vec::new()
        } else {
            let mut topics = std::mem::take(&mut self.classic_fetch_topics);
            refill_offset_fetch_topics_cooperative(&mut topics, &added).await;
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                self.classic_fetch_topics = topics;
                return Err(Error::Timeout);
            }
            let overlap_dials =
                self.coord.offset_fetch_version != 10 && !matches!(reset, AutoOffsetReset::None);
            if !overlap_dials {
                if overlap_metadata {
                    self.consumer
                        .refresh_metadata_timeout(Some(&self.topics), timeout)
                        .await?;
                }
                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    self.classic_fetch_topics = topics;
                    return Err(Error::Timeout);
                }
                let fetched = self.offset_fetch(&topics, timeout).await;
                self.classic_fetch_topics = topics;
                fetched?
            } else if overlap_metadata {
                let mut leaders = Vec::new();
                let mut seen_leaders = HashSet::new();
                remember_partition_leaders(
                    self.consumer.cluster(),
                    &added,
                    &mut leaders,
                    &mut seen_leaders,
                )
                .await;
                let mut dials = self.consumer.start_leader_dials(&leaders, deadline).await;
                let require_stable = self.cfg.isolation_level == IsolationLevel::ReadCommitted;
                let member_epoch = if self.kip848 { self.generation_id } else { -1 };
                let member_id = if self.kip848 {
                    Some(self.member_id.clone())
                } else {
                    None
                };
                let group_id = self.group_id.clone();
                let bootstrap_keys = self.consumer.copy_api_keys();
                let address_keys = self.consumer.copy_learned_broker_keys();
                let prepared_version = negotiated_coord_version(&self.coord, OFFSET_FETCH).ok();
                let mut payload = BytesMut::new();
                if let Some(version) = prepared_version {
                    encode_offset_fetch_request_cooperative(
                        &mut payload,
                        version,
                        &group_id,
                        member_id.as_deref(),
                        member_epoch,
                        require_stable,
                        Some(&topics),
                    )
                    .await?;
                }
                let fetched = {
                    let refresh_fut = self
                        .consumer
                        .refresh_metadata_timeout(Some(&self.topics), timeout);
                    let fetch_fut = coord_roundtrip_with_bootstrap(
                        &mut self.coord,
                        &self.cfg,
                        &group_id,
                        COORDINATOR_GROUP,
                        OFFSET_FETCH,
                        |version, buf| {
                            if prepared_version == Some(version) {
                                buf.extend_from_slice(&payload);
                                Ok(())
                            } else {
                                encode_offset_fetch_request(
                                    buf,
                                    version,
                                    &group_id,
                                    member_id.as_deref(),
                                    member_epoch,
                                    require_stable,
                                    Some(&topics),
                                )
                            }
                        },
                        &bootstrap_keys,
                        &address_keys,
                        &mut self.prior_coords,
                        timeout,
                    );
                    let (fetched_rpc, metadata_refresh) = {
                        tokio::pin!(refresh_fut);
                        tokio::pin!(fetch_fut);
                        let mut metadata_refresh = None;
                        let fetched_rpc = loop {
                            tokio::select! {
                                biased;
                                result = &mut fetch_fut => break result,
                                result = &mut refresh_fut, if metadata_refresh.is_none() => {
                                    metadata_refresh = Some(result);
                                }
                                _ = &mut dials, if !warm_leaders => {
                                    warm_leaders = true;
                                }
                            }
                        };
                        if metadata_refresh.is_none() {
                            metadata_refresh = Some((&mut refresh_fut).await);
                        }
                        (fetched_rpc, metadata_refresh)
                    };
                    let (version, body) = match fetched_rpc {
                        Ok(pair) => pair,
                        Err(error) => {
                            self.classic_fetch_topics = topics;
                            self.keep_leader_flight(dials, deadline).await;
                            return Err(error);
                        }
                    };
                    let fetched =
                        match decode_offset_fetch_response_cooperative(&mut body.clone(), version)
                            .await
                        {
                            Ok(fetched) => fetched,
                            Err(error) => {
                                self.classic_fetch_topics = topics;
                                self.keep_leader_flight(dials, deadline).await;
                                return Err(group_decode_context(
                                    error,
                                    "OffsetFetch",
                                    version,
                                    body.len(),
                                ));
                            }
                        };
                    if let Some(Err(error)) = metadata_refresh {
                        self.classic_fetch_topics = topics;
                        self.keep_leader_flight(dials, deadline).await;
                        return Err(error);
                    }
                    let committed = index_fetched_offsets_cooperative(&fetched).await;
                    let mut needs_reset = false;
                    let mut walked = 0usize;
                    for (topic, partition) in &added {
                        let offset = committed
                            .get(&(topic.as_str(), *partition))
                            .copied()
                            .map(|part| part.offset)
                            .unwrap_or(OffsetAndMetadata::INVALID_OFFSET);
                        if offset < 0 {
                            needs_reset = true;
                            break;
                        }
                        walked = walked.saturating_add(1);
                        if walked == 8 * 1024 {
                            walked = 0;
                            tokio::task::yield_now().await;
                        }
                    }
                    if needs_reset && !warm_leaders {
                        (&mut dials).await;
                    }
                    self.keep_leader_flight(dials, deadline).await;
                    fetched
                };
                self.classic_fetch_topics = topics;
                fetched
            } else {
                let mut leaders = Vec::new();
                let mut seen_leaders = HashSet::new();
                remember_partition_leaders(
                    self.consumer.cluster(),
                    &added,
                    &mut leaders,
                    &mut seen_leaders,
                )
                .await;
                let mut dials = self.consumer.start_leader_dials(&leaders, deadline).await;
                let require_stable = self.cfg.isolation_level == IsolationLevel::ReadCommitted;
                let member_epoch = if self.kip848 { self.generation_id } else { -1 };
                let member_id = if self.kip848 {
                    Some(self.member_id.clone())
                } else {
                    None
                };
                let group_id = self.group_id.clone();
                let bootstrap_keys = self.consumer.copy_api_keys();
                let address_keys = self.consumer.copy_learned_broker_keys();
                let prepared_version = negotiated_coord_version(&self.coord, OFFSET_FETCH).ok();
                let mut payload = BytesMut::new();
                if let Some(version) = prepared_version {
                    encode_offset_fetch_request_cooperative(
                        &mut payload,
                        version,
                        &group_id,
                        member_id.as_deref(),
                        member_epoch,
                        require_stable,
                        Some(&topics),
                    )
                    .await?;
                }
                let fetched = {
                    let fetch_fut = coord_roundtrip_with_bootstrap(
                        &mut self.coord,
                        &self.cfg,
                        &group_id,
                        COORDINATOR_GROUP,
                        OFFSET_FETCH,
                        |version, buf| {
                            if prepared_version == Some(version) {
                                buf.extend_from_slice(&payload);
                                Ok(())
                            } else {
                                encode_offset_fetch_request(
                                    buf,
                                    version,
                                    &group_id,
                                    member_id.as_deref(),
                                    member_epoch,
                                    require_stable,
                                    Some(&topics),
                                )
                            }
                        },
                        &bootstrap_keys,
                        &address_keys,
                        &mut self.prior_coords,
                        timeout,
                    );
                    let fetched_rpc = {
                        tokio::pin!(fetch_fut);
                        tokio::select! {
                            biased;
                            result = &mut fetch_fut => result,
                            () = &mut dials, if !warm_leaders => {
                                warm_leaders = true;
                                (&mut fetch_fut).await
                            }
                        }
                    };
                    let (version, body) = match fetched_rpc {
                        Ok(pair) => pair,
                        Err(error) => {
                            self.classic_fetch_topics = topics;
                            self.keep_leader_flight(dials, deadline).await;
                            return Err(error);
                        }
                    };
                    let fetched =
                        match decode_offset_fetch_response_cooperative(&mut body.clone(), version)
                            .await
                        {
                            Ok(fetched) => fetched,
                            Err(error) => {
                                self.classic_fetch_topics = topics;
                                self.keep_leader_flight(dials, deadline).await;
                                return Err(group_decode_context(
                                    error,
                                    "OffsetFetch",
                                    version,
                                    body.len(),
                                ));
                            }
                        };
                    let committed = index_fetched_offsets_cooperative(&fetched).await;
                    let mut needs_reset = false;
                    let mut walked = 0usize;
                    for (topic, partition) in &added {
                        let offset = committed
                            .get(&(topic.as_str(), *partition))
                            .copied()
                            .map(|part| part.offset)
                            .unwrap_or(OffsetAndMetadata::INVALID_OFFSET);
                        if offset < 0 {
                            needs_reset = true;
                            break;
                        }
                        walked = walked.saturating_add(1);
                        if walked == 8 * 1024 {
                            walked = 0;
                            tokio::task::yield_now().await;
                        }
                    }
                    if needs_reset && !warm_leaders {
                        (&mut dials).await;
                    }
                    self.keep_leader_flight(dials, deadline).await;
                    fetched
                };
                self.classic_fetch_topics = topics;
                fetched
            }
        };
        check_fetched_offsets(&fetched)?;
        let committed = index_fetched_offsets_cooperative(&fetched).await;
        let mut starts: Vec<Option<(String, i32, i64)>> = Vec::with_capacity(wanted.len());
        let mut reset_slots = Vec::new();
        let mut walked_commits = 0usize;
        for (topic, part) in wanted {
            if let Some(offset) = nested_i64(&current, topic, *part) {
                starts.push(Some((topic.clone(), *part, offset)));
            } else {
                let offset = committed
                    .get(&(topic.as_str(), *part))
                    .copied()
                    .map(|part| part.offset)
                    .unwrap_or(OffsetAndMetadata::INVALID_OFFSET);
                if offset >= 0 {
                    starts.push(Some((topic.clone(), *part, offset)));
                } else if matches!(reset, AutoOffsetReset::None) {
                    return Err(Error::protocol(format!(
                        "no committed offset for {topic}-{part}"
                    )));
                } else {
                    reset_slots.push(starts.len());
                    starts.push(None);
                }
            }
            walked_commits = walked_commits.saturating_add(1);
            if walked_commits == 8 * 1024 {
                walked_commits = 0;
                tokio::task::yield_now().await;
            }
        }
        if !reset_slots.is_empty() {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let mut partitions = Vec::with_capacity(reset_slots.len());
            let mut walked_names = 0usize;
            for index in &reset_slots {
                let Some((topic, part)) = wanted.get(*index) else {
                    return Err(Error::protocol("ListOffsets returned no reset offset"));
                };
                partitions.push(TopicPartition::new(topic.as_str(), *part));
                walked_names = walked_names.saturating_add(1);
                if walked_names == 8 * 1024 {
                    walked_names = 0;
                    tokio::task::yield_now().await;
                }
            }
            let listed = match reset {
                AutoOffsetReset::Earliest => {
                    self.consumer
                        .beginning_offsets_timeout(partitions, timeout)
                        .await?
                }
                AutoOffsetReset::Latest => {
                    self.consumer
                        .end_offsets_timeout(partitions, timeout)
                        .await?
                }
                AutoOffsetReset::None => {
                    return Err(Error::protocol("no committed offset"));
                }
            };
            if listed.len() != reset_slots.len() {
                return Err(Error::protocol("ListOffsets returned no reset offset"));
            }
            apply_reset_offsets(&mut starts, &reset_slots, listed).await?;
        }
        let starts = flatten_reset_starts(starts).await;
        self.consumer.assign_all(&starts).await?;
        if assignment_topic_ids_changed(
            wanted,
            |topic, partition| self.consumer.assigned_topic_id(topic, partition),
            &identities,
        )
        .await
        {
            self.consumer.clear_assignment();
            return Err(Error::protocol(
                "topic identity changed while assigning committed offsets",
            ));
        }
        let mut walked_epochs = 0usize;
        for (topic, part) in wanted {
            if let Some(epoch) = nested_i32(&kept_epochs, topic, *part) {
                self.consumer.set_last_fetched_epoch(topic, *part, epoch);
            } else if let Some(found) = committed.get(&(topic.as_str(), *part)).copied() {
                self.consumer
                    .set_last_fetched_epoch(topic, *part, found.leader_epoch);
            }
            walked_epochs = walked_epochs.saturating_add(1);
            if walked_epochs == 8 * 1024 {
                walked_epochs = 0;
                tokio::task::yield_now().await;
            }
        }
        let mut added_tps = Vec::new();
        let mut walked_added = 0usize;
        for (topic, parts) in &next {
            for partition in parts {
                if nested_i64(&current, topic, *partition).is_none() {
                    added_tps.push((topic.clone(), *partition));
                }
                walked_added = walked_added.saturating_add(1);
                if walked_added == 8 * 1024 {
                    walked_added = 0;
                    tokio::task::yield_now().await;
                }
            }
        }
        if !revoked.is_empty() || !added_tps.is_empty() {
            self.cfg.rebalance.call(
                &TopicPartition::list_from(&revoked),
                &TopicPartition::list_from(&added_tps),
            );
        }
        Ok(revoked)
    }

    fn spawn_heartbeat_consumer(
        &self,
        mut stop: watch::Receiver<bool>,
    ) -> Arc<tokio::task::JoinHandle<()>> {
        let group_id = self.group_id.clone();
        let hb_member_id = self.hb_member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_generation = self.hb_generation.clone();
        let hb_assignment = self.hb_assignment.clone();
        let hb_ack = self.hb_ack.clone();
        let hb_topics = self.hb_topics.clone();
        let hb_interval_ms = self.hb_interval_ms.clone();
        let hb_deadline = self.hb_deadline.clone();
        let last_poll = self.last_poll.clone();
        let left_max_poll = self.left_max_poll.clone();
        let cfg = self.cfg.clone();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        Arc::new(tokio::spawn(async move {
            let mut conn: Option<BrokerConn> = None;
            let mut remembered: Option<KnownCoord> = None;
            let mut prior_coords = HashMap::new();
            let init_ms = hb_interval_ms.load(Ordering::SeqCst);
            let mut current_interval = if let Ok(ms_u64) = u64::try_from(init_ms) {
                if ms_u64 > 0 {
                    Duration::from_millis(ms_u64)
                } else {
                    cfg.heartbeat_interval.max(Duration::from_millis(1))
                }
            } else {
                cfg.heartbeat_interval.max(Duration::from_millis(1))
            };
            // Copy first. parking_lot mutexes are not reentrant, and this task
            // starts only after join has stored a deadline.
            let existing_deadline = *hb_deadline.lock();
            let mut next_hb_deadline = if let Some(d) = existing_deadline {
                d
            } else {
                let d = Instant::now() + current_interval;
                *hb_deadline.lock() = Some(d);
                d
            };
            let mut member_id = hb_member_id.lock().clone();
            let mut heartbeat = ConsumerGroupHeartbeatRequest {
                group_id: group_id.clone(),
                member_id: member_id.clone(),
                member_epoch: 0,
                instance_id: cfg.group_instance_id.clone(),
                rack_id: cfg.rack.clone(),
                rebalance_timeout_ms: ConsumerGroupHeartbeatRequest::UNCHANGED_REBALANCE_TIMEOUT_MS,
                subscribed_topic_names: None,
                subscribed_topic_regex: None,
                server_assignor: None,
                topic_partitions: None,
            };
            loop {
                let now = Instant::now();
                if let Some(extern_deadline) = *hb_deadline.lock() {
                    if extern_deadline > now && extern_deadline != next_hb_deadline {
                        next_hb_deadline = extern_deadline;
                        let latest_ms = hb_interval_ms.load(Ordering::SeqCst);
                        if let Ok(ms_u64) = u64::try_from(latest_ms) {
                            if ms_u64 > 0 {
                                current_interval = Duration::from_millis(ms_u64);
                            }
                        }
                    }
                }
                let mut wake_deadline = next_hb_deadline;
                if !cfg.max_poll_interval.is_zero() {
                    if let Some(t) = *last_poll.lock() {
                        let poll_deadline = t + cfg.max_poll_interval;
                        if poll_deadline < wake_deadline {
                            wake_deadline = poll_deadline;
                        }
                    }
                }
                let sleep_duration = wake_deadline.saturating_duration_since(Instant::now());
                enum Wake {
                    Stop,
                    Again,
                    Beat,
                }
                let wake = tokio::select! {
                    res = stop.changed() => {
                        if res.is_err() || *stop.borrow() {
                            Wake::Stop
                        } else {
                            Wake::Again
                        }
                    }
                    _ = tokio::time::sleep(sleep_duration) => Wake::Beat,
                    _ = coordinator_lost(&mut conn) => {
                        retire_group_conn(&mut conn).await;
                        next_hb_deadline = Instant::now();
                        Wake::Beat
                    }
                };
                match wake {
                    Wake::Stop => {
                        *hb_deadline.lock() = None;
                        break;
                    }
                    Wake::Again => continue,
                    Wake::Beat => {}
                }
                // Re-read the shared id every tick: the broker may
                // rotate it mid-session (KL03-15). An unchanged id
                // keeps the existing string.
                copy_member_id(&mut member_id, &hb_member_id.lock());
                if leave_if_max_poll(
                    &cfg,
                    &group_id,
                    &member_id,
                    true,
                    &last_poll,
                    &left_max_poll,
                    &mut conn,
                    &bootstrap_keys,
                    &mut remembered,
                    &mut prior_coords,
                )
                .await
                {
                    *hb_deadline.lock() = None;
                    break;
                }
                if Instant::now() < next_hb_deadline {
                    continue;
                }
                let deadline = Instant::now() + cfg.request_timeout;
                if conn.is_none() {
                    if let Some(known) = remembered.take() {
                        conn = Box::pin(rediscover_known_coord(
                            &cfg,
                            &group_id,
                            COORDINATOR_GROUP,
                            &known,
                            &mut prior_coords,
                        ))
                        .await
                        .ok();
                    }
                }
                reopen_idle_coordinator(
                    &mut conn,
                    &cfg,
                    &group_id,
                    deadline,
                    &bootstrap_keys,
                    &address_keys,
                    &mut prior_coords,
                )
                .await;
                let Some(c) = conn.as_mut() else {
                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                    next_hb_deadline = Instant::now() + retry_delay;
                    *hb_deadline.lock() = Some(next_hb_deadline);
                    continue;
                };
                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                    next_hb_deadline = Instant::now() + retry_delay;
                    *hb_deadline.lock() = Some(next_hb_deadline);
                    continue;
                }
                let epoch = hb_generation.load(Ordering::SeqCst);
                let joining = epoch == ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH;
                let rebalance_timeout_ms = if joining {
                    if cfg.max_poll_interval.is_zero() {
                        i32::MAX
                    } else {
                        duration_millis_i32(cfg.max_poll_interval)
                    }
                } else {
                    ConsumerGroupHeartbeatRequest::UNCHANGED_REBALANCE_TIMEOUT_MS
                };
                let version = c.consumer_group_heartbeat_version;
                if spoken_consumer_group_heartbeat(version).is_err() {
                    retire_group_conn(&mut conn).await;
                    hb_err.store(error::UNSUPPORTED_VERSION, Ordering::SeqCst);
                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                    next_hb_deadline = Instant::now() + retry_delay;
                    *hb_deadline.lock() = Some(next_hb_deadline);
                    continue;
                }
                copy_member_id(&mut heartbeat.member_id, &member_id);
                heartbeat.member_epoch = epoch;
                heartbeat.rebalance_timeout_ms = rebalance_timeout_ms;
                if joining {
                    reuse_topic_names_locked(&mut heartbeat.subscribed_topic_names, &hb_topics)
                        .await;
                    if heartbeat
                        .topic_partitions
                        .as_ref()
                        .is_none_or(|parts| !parts.is_empty())
                    {
                        heartbeat.topic_partitions = Some(Vec::new());
                    }
                } else {
                    heartbeat.subscribed_topic_names = None;
                    reuse_topic_partitions_locked(&mut heartbeat.topic_partitions, &hb_ack).await;
                }
                let mut encoded = bytes::BytesMut::new();
                if encode_consumer_group_heartbeat_request_cooperative(
                    &mut encoded,
                    version,
                    &heartbeat,
                )
                .await
                .is_err()
                {
                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                    next_hb_deadline = Instant::now() + retry_delay;
                    *hb_deadline.lock() = Some(next_hb_deadline);
                    continue;
                }
                let res = c
                    .roundtrip(
                        CONSUMER_GROUP_HEARTBEAT,
                        version,
                        |buf| {
                            buf.extend_from_slice(&encoded);
                            Ok(())
                        },
                        timeout,
                    )
                    .await;
                match res {
                    Ok(body) => {
                        if let Ok(resp) = decode_consumer_group_heartbeat_response_cooperative(
                            &mut body.clone(),
                            version,
                        )
                        .await
                        {
                            if error::coordinator_retriable(resp.error_code) {
                                if let Some(current) = conn.as_ref() {
                                    remembered = Some(
                                        KnownCoord::capture(current)
                                            .with_bootstrap_keys(&bootstrap_keys)
                                            .with_address_keys(&address_keys),
                                    );
                                }
                                retire_group_conn(&mut conn).await;
                                let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                                next_hb_deadline = Instant::now() + retry_delay;
                                *hb_deadline.lock() = Some(next_hb_deadline);
                            } else {
                                hb_err.store(resp.error_code, Ordering::SeqCst);
                                hb_generation.store(
                                    kip848_heartbeat_epoch(
                                        resp.error_code,
                                        resp.member_epoch,
                                        hb_generation.load(Ordering::SeqCst),
                                    ),
                                    Ordering::SeqCst,
                                );
                                // The broker owns member ids; adopt a
                                // rotated id for later heartbeats and
                                // the poll path (KL03-15).
                                if let Some(id) = resp.member_id.as_deref() {
                                    if id != member_id {
                                        copy_member_id(&mut member_id, id);
                                        copy_member_id(&mut heartbeat.member_id, id);
                                        copy_member_id(&mut hb_member_id.lock(), &member_id);
                                    }
                                }
                                if resp.error_code == 0 {
                                    if let Some(assignment) = resp.assignment {
                                        *hb_assignment.lock() = Some(assignment);
                                        *hb_ack.lock() = None;
                                    } else {
                                        *hb_ack.lock() = None;
                                    }
                                    if resp.heartbeat_interval_ms <= 0 {
                                        hb_err.store(error::INVALID_REQUEST, Ordering::SeqCst);
                                        next_hb_deadline = Instant::now() + current_interval;
                                        *hb_deadline.lock() = Some(next_hb_deadline);
                                    } else if let Ok(ms_u64) =
                                        u64::try_from(resp.heartbeat_interval_ms)
                                    {
                                        current_interval = Duration::from_millis(ms_u64);
                                        hb_interval_ms
                                            .store(resp.heartbeat_interval_ms, Ordering::SeqCst);
                                        next_hb_deadline = Instant::now() + current_interval;
                                        *hb_deadline.lock() = Some(next_hb_deadline);
                                    } else {
                                        next_hb_deadline = Instant::now() + current_interval;
                                        *hb_deadline.lock() = Some(next_hb_deadline);
                                    }
                                } else {
                                    next_hb_deadline = Instant::now() + current_interval;
                                    *hb_deadline.lock() = Some(next_hb_deadline);
                                }
                            }
                        } else {
                            // Undecodable bytes are a broker protocol
                            // violation, not a blip: mark it so poll
                            // surfaces it instead of spinning
                            // silently. Transport failures below stay
                            // silent-retry by design (KL03-15).
                            if let Some(current) = conn.as_ref() {
                                remembered = Some(
                                    KnownCoord::capture(current)
                                        .with_bootstrap_keys(&bootstrap_keys)
                                        .with_address_keys(&address_keys),
                                );
                            }
                            hb_err.store(error::INVALID_REQUEST, Ordering::SeqCst);
                            retire_group_conn(&mut conn).await;
                            let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                            next_hb_deadline = Instant::now() + retry_delay;
                            *hb_deadline.lock() = Some(next_hb_deadline);
                        }
                    }
                    Err(_) => {
                        if let Some(current) = conn.as_ref() {
                            remembered = Some(
                                KnownCoord::capture(current)
                                    .with_bootstrap_keys(&bootstrap_keys)
                                    .with_address_keys(&address_keys),
                            );
                        }
                        retire_group_conn(&mut conn).await;
                        let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                        next_hb_deadline = Instant::now() + retry_delay;
                        *hb_deadline.lock() = Some(next_hb_deadline);
                    }
                }
            }
        }))
    }

    fn spawn_heartbeat(&self, mut stop: watch::Receiver<bool>) -> Arc<tokio::task::JoinHandle<()>> {
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_generation = self.hb_generation.clone();
        let last_poll = self.last_poll.clone();
        let left_max_poll = self.left_max_poll.clone();
        let cfg = self.cfg.clone();
        let bootstrap_keys = self.consumer.copy_api_keys();
        let address_keys = self.consumer.copy_learned_broker_keys();
        Arc::new(tokio::spawn(async move {
            let mut conn: Option<BrokerConn> = None;
            let mut remembered: Option<KnownCoord> = None;
            let mut prior_coords = HashMap::new();
            let mut tick =
                tokio::time::interval(cfg.heartbeat_interval.max(Duration::from_millis(1)));
            loop {
                enum Wake {
                    Stop,
                    Again,
                    Beat,
                }
                let wake = tokio::select! {
                    res = stop.changed() => {
                        if res.is_err() || *stop.borrow() {
                            Wake::Stop
                        } else {
                            Wake::Again
                        }
                    }
                    _ = tick.tick() => Wake::Beat,
                    _ = coordinator_lost(&mut conn) => {
                        retire_group_conn(&mut conn).await;
                        Wake::Beat
                    }
                };
                match wake {
                    Wake::Stop => break,
                    Wake::Again => continue,
                    Wake::Beat => {}
                }
                if leave_if_max_poll(
                    &cfg,
                    &group_id,
                    &member_id,
                    false,
                    &last_poll,
                    &left_max_poll,
                    &mut conn,
                    &bootstrap_keys,
                    &mut remembered,
                    &mut prior_coords,
                )
                .await
                {
                    break;
                }
                let deadline = Instant::now() + cfg.request_timeout;
                if conn.is_none() {
                    if let Some(known) = remembered.take() {
                        conn = Box::pin(rediscover_known_coord(
                            &cfg,
                            &group_id,
                            COORDINATOR_GROUP,
                            &known,
                            &mut prior_coords,
                        ))
                        .await
                        .ok();
                    }
                }
                reopen_idle_coordinator(
                    &mut conn,
                    &cfg,
                    &group_id,
                    deadline,
                    &bootstrap_keys,
                    &address_keys,
                    &mut prior_coords,
                )
                .await;
                let Some(c) = conn.as_mut() else {
                    continue;
                };
                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    continue;
                }
                let generation = hb_generation.load(Ordering::SeqCst);
                let Ok(version) = spoken_heartbeat(c.heartbeat_version) else {
                    // A coordinator that joined us and then omits Heartbeat
                    // cannot keep the member alive. Drop the socket and
                    // surface the miss; the next successful heartbeat
                    // stores error 0 and clears it.
                    retire_group_conn(&mut conn).await;
                    hb_err.store(error::UNSUPPORTED_VERSION, Ordering::SeqCst);
                    continue;
                };
                let res = c
                    .roundtrip(
                        HEARTBEAT,
                        version,
                        |buf| {
                            encode_heartbeat_request(
                                buf,
                                version,
                                &group_id,
                                generation,
                                &member_id,
                                cfg.group_instance_id.as_deref(),
                            )
                        },
                        timeout,
                    )
                    .await;
                match res {
                    Ok(body) => {
                        if let Ok((err, ..)) = decode_heartbeat_response(&mut body.clone(), version)
                        {
                            if error::coordinator_retriable(err) {
                                if let Some(current) = conn.as_ref() {
                                    remembered = Some(
                                        KnownCoord::capture(current)
                                            .with_bootstrap_keys(&bootstrap_keys)
                                            .with_address_keys(&address_keys),
                                    );
                                }
                                retire_group_conn(&mut conn).await;
                            } else {
                                hb_err.store(err, Ordering::SeqCst);
                            }
                        }
                    }
                    Err(_) => {
                        if let Some(current) = conn.as_ref() {
                            remembered = Some(
                                KnownCoord::capture(current)
                                    .with_bootstrap_keys(&bootstrap_keys)
                                    .with_address_keys(&address_keys),
                            );
                        }
                        retire_group_conn(&mut conn).await;
                    }
                }
            }
        }))
    }
}

/// Stop a heartbeat task that has been signalled, then abort it if it is still running.
async fn finish_heartbeat_task(task: Arc<tokio::task::JoinHandle<()>>, deadline: Instant) {
    while !task.is_finished() && Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    if !task.is_finished() {
        task.abort();
    }
    let mut turns = 0u32;
    while !task.is_finished() && turns < 8 {
        turns = turns.saturating_add(1);
        tokio::task::yield_now().await;
    }
    // The abort drops the coordinator when the task is polled. The idle watch
    // then has to run before the broker sees that close.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

pub(crate) fn copy_member_id(dest: &mut String, source: &str) {
    if dest.as_str() != source {
        dest.clear();
        dest.push_str(source);
    }
}

#[cfg(test)]
fn reuse_topic_names(dest: &mut Option<Vec<String>>, topics: &[String]) {
    let same = dest
        .as_ref()
        .is_some_and(|names| names.as_slice() == topics);
    if !same {
        *dest = Some(topics.to_vec());
    }
}

fn reused_name_chunk() -> usize {
    8 * 1024
}

/// Copy subscribed topic names into the heartbeat when they changed.
///
/// The copy pauses every 8,192 names. Without that pause, copying 4,000,000
/// names held the caller for 404 milliseconds.
///
/// An unchanged list keeps its buffer. Each changed name is still copied.
pub(crate) async fn reuse_topic_names_locked(
    dest: &mut Option<Vec<String>>,
    topics: &parking_lot::Mutex<Vec<String>>,
) {
    let snapshot = {
        let mut guard = topics.lock();
        std::mem::take(&mut *guard)
    };
    reuse_topic_names_from(dest, &snapshot).await;
    let mut guard = topics.lock();
    if guard.is_empty() {
        *guard = snapshot;
    }
}

pub(crate) async fn reuse_topic_names_from(dest: &mut Option<Vec<String>>, topics: &[String]) {
    let unchanged = match dest.as_ref() {
        Some(names) => names_match(names, topics).await,
        None => false,
    };
    if !unchanged {
        *dest = Some(copy_reused_names(topics).await);
    }
}

async fn names_match(left: &[String], right: &[String]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0usize;
    while index < left.len() {
        let end = index.saturating_add(reused_name_chunk()).min(left.len());
        if left.get(index..end) != right.get(index..end) {
            return false;
        }
        index = end;
        if index < left.len() {
            tokio::task::yield_now().await;
        }
    }
    true
}

async fn copy_reused_names(topics: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut walked = 0usize;
    for name in topics {
        out.push(name.clone());
        walked = walked.saturating_add(1);
        if walked == reused_name_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    out
}

#[cfg(test)]
fn reuse_topic_partitions(
    dest: &mut Option<Vec<TopicPartitions>>,
    next: &Option<Vec<TopicPartitions>>,
) {
    if dest.as_ref() != next.as_ref() {
        *dest = next.clone();
    }
}

fn reused_partition_chunk() -> usize {
    8 * 1024
}

/// Copy the heartbeat assignment when it changed.
///
/// The copy pauses every 8,192 topics. Without that pause, copying
/// 2,000,000 topics held the caller for 276 milliseconds.
///
/// An unchanged assignment keeps its buffer. Each changed topic is still
/// copied.
async fn reuse_topic_partitions_locked(
    dest: &mut Option<Vec<TopicPartitions>>,
    next: &parking_lot::Mutex<Option<Vec<TopicPartitions>>>,
) {
    let snapshot = {
        let mut guard = next.lock();
        std::mem::take(&mut *guard)
    };
    let unchanged = match (dest.as_ref(), snapshot.as_ref()) {
        (Some(current), Some(next)) => topic_partitions_match(current, next).await,
        (None, None) => true,
        _ => false,
    };
    if !unchanged {
        *dest = copy_topic_partitions(&snapshot).await;
    }
    let mut guard = next.lock();
    if guard.is_none() {
        *guard = snapshot;
    }
}

async fn topic_partitions_match(left: &[TopicPartitions], right: &[TopicPartitions]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0usize;
    while index < left.len() {
        let end = index
            .saturating_add(reused_partition_chunk())
            .min(left.len());
        if left.get(index..end) != right.get(index..end) {
            return false;
        }
        index = end;
        if index < left.len() {
            tokio::task::yield_now().await;
        }
    }
    true
}

async fn copy_topic_partitions(
    next: &Option<Vec<TopicPartitions>>,
) -> Option<Vec<TopicPartitions>> {
    let Some(next) = next else {
        return None;
    };
    let mut out = Vec::new();
    let mut walked = 0usize;
    for item in next {
        out.push(item.clone());
        walked = walked.saturating_add(1);
        if walked == reused_partition_chunk() {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    Some(out)
}

#[cfg(test)]
mod member_id_tests {
    use super::copy_member_id;

    #[test]
    fn an_unchanged_member_id_keeps_its_buffer() {
        let mut dest = String::from("member-1");
        let ptr = dest.as_ptr();
        let capacity = dest.capacity();
        copy_member_id(&mut dest, "member-1");
        assert_eq!(dest.as_ptr(), ptr);
        assert!(dest.capacity() >= capacity);
        copy_member_id(&mut dest, "member-2");
        assert_eq!(dest, "member-2");
    }

    #[test]
    fn an_unchanged_join_subscription_keeps_its_buffer() {
        let topics = vec!["orders".to_string()];
        let mut names = Some(topics.clone());
        let ptr = names.as_ref().map(Vec::as_ptr);
        super::reuse_topic_names(&mut names, &topics);
        assert_eq!(names.as_ref().map(Vec::as_ptr), ptr);
        let changed = ["payments".to_string()];
        super::reuse_topic_names(&mut names, &changed);
        assert_eq!(names.as_deref(), Some(changed.as_slice()));
    }

    #[test]
    fn an_unchanged_heartbeat_assignment_keeps_its_buffer() {
        let owned = vec![super::TopicPartitions {
            topic_id: [1; 16],
            partitions: vec![0, 1],
        }];
        let mut dest = Some(owned.clone());
        let ptr = dest.as_ref().map(Vec::as_ptr);
        super::reuse_topic_partitions(&mut dest, &Some(owned));
        assert_eq!(dest.as_ref().map(Vec::as_ptr), ptr);
        let changed = Some(vec![super::TopicPartitions {
            topic_id: [2; 16],
            partitions: vec![3],
        }]);
        super::reuse_topic_partitions(&mut dest, &changed);
        assert_eq!(dest, changed);
        super::reuse_topic_partitions(&mut dest, &None);
        assert!(dest.is_none());
    }
}

async fn leave_coordinator(
    cfg: &ConsumerConfig,
    group_id: &str,
    deadline: Instant,
    bootstrap_keys: &[ApiVersion],
    prior: &HashMap<String, KnownCoord>,
) -> Option<BrokerConn> {
    let addr = cfg.bootstrap.first()?;
    if bootstrap_keys.is_empty() {
        return discover_coord_within(cfg, group_id, COORDINATOR_GROUP, deadline)
            .await
            .ok();
    }
    let bound = crate::net::Deadline::from_std(deadline);
    let (mut hop, version, _) = open_coord_with_keys(cfg, addr, bootstrap_keys).await.ok()?;
    let Some(coord_addr) =
        coordinator_address_of(&mut hop, version, group_id, COORDINATOR_GROUP, bound)
            .await
            .ok()
    else {
        retire_lookup_conn(hop).await;
        return None;
    };
    if coord_addr == hop.addr() {
        return Some(hop);
    }
    let opened = if let Some(known) = prior.get(&coord_addr) {
        connect_known_coord(cfg, known).await.ok()
    } else {
        open_coord_with_find_version(cfg, &coord_addr)
            .await
            .ok()
            .map(|(conn, _, _)| conn)
    };
    retire_lookup_conn(hop).await;
    opened
}

#[expect(
    clippy::too_many_arguments,
    reason = "max poll leave carries the open coordinator and its learned versions"
)]
async fn leave_if_max_poll(
    cfg: &ConsumerConfig,
    group_id: &str,
    member_id: &str,
    kip848: bool,
    last_poll: &parking_lot::Mutex<Option<Instant>>,
    left: &AtomicBool,
    conn: &mut Option<BrokerConn>,
    bootstrap_keys: &[ApiVersion],
    remembered: &mut Option<KnownCoord>,
    prior: &mut HashMap<String, KnownCoord>,
) -> bool {
    if left.load(Ordering::SeqCst) {
        return true;
    }
    if cfg.max_poll_interval.is_zero() {
        return false;
    }
    let expired = last_poll
        .lock()
        .is_some_and(|t| t.elapsed() > cfg.max_poll_interval);
    if !expired {
        return false;
    }
    left.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + cfg.request_timeout;
    if conn.is_none() {
        if let Some(known) = remembered.take() {
            *conn = Box::pin(rediscover_known_coord(
                cfg,
                group_id,
                COORDINATOR_GROUP,
                &known,
                prior,
            ))
            .await
            .ok();
        }
    }
    if conn.is_none() {
        *conn = leave_coordinator(cfg, group_id, deadline, bootstrap_keys, prior).await;
    }
    if let Some(c) = conn.as_mut() {
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return true;
        }
        if kip848 {
            let version = c.consumer_group_heartbeat_version;
            if spoken_consumer_group_heartbeat(version).is_ok() {
                let req = ConsumerGroupHeartbeatRequest {
                    group_id: group_id.to_string(),
                    member_id: member_id.to_string(),
                    member_epoch: ConsumerGroupHeartbeatRequest::leave_group_epoch(
                        cfg.group_instance_id.as_deref(),
                    ),
                    instance_id: cfg.group_instance_id.clone(),
                    rack_id: cfg.rack.clone(),
                    rebalance_timeout_ms:
                        ConsumerGroupHeartbeatRequest::UNCHANGED_REBALANCE_TIMEOUT_MS,
                    subscribed_topic_names: None,
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    topic_partitions: None,
                };
                let mut encoded = bytes::BytesMut::new();
                if encode_consumer_group_heartbeat_request_cooperative(&mut encoded, version, &req)
                    .await
                    .is_ok()
                {
                    drop(
                        c.roundtrip(
                            CONSUMER_GROUP_HEARTBEAT,
                            version,
                            |buf| {
                                buf.extend_from_slice(&encoded);
                                Ok(())
                            },
                            timeout,
                        )
                        .await,
                    );
                }
            }
        } else if let Ok(version) = spoken_leave_group(c.leave_group_version) {
            let gid = group_id.to_string();
            let members = [LeaveGroupMember {
                member_id: member_id.to_string(),
                group_instance_id: cfg.group_instance_id.clone(),
                reason: Some(LEAVE_GROUP_REASON_POLL_TIMEOUT.into()),
            }];
            drop(
                c.roundtrip(
                    LEAVE_GROUP,
                    version,
                    |buf| encode_leave_group_request_members(buf, version, &gid, &members),
                    timeout,
                )
                .await,
            );
        }
    }
    true
}

fn spoken_offset_fetch(version: i16) -> Result<i16> {
    if (1..=9).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support OffsetFetch v1-9".into(),
        ))
    }
}

fn spoken_offset_commit(version: i16) -> Result<i16> {
    if (2..=9).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support OffsetCommit v2-9".into(),
        ))
    }
}

fn spoken_join_group(version: i16) -> Result<i16> {
    if (2..=9).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support JoinGroup v2-9".into(),
        ))
    }
}

fn spoken_sync_group(version: i16) -> Result<i16> {
    if (0..=5).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support SyncGroup v0-5".into(),
        ))
    }
}

enum ClassicHeartbeat {
    Ready,
    Rejoin,
    Failed,
}

fn kip848_heartbeat_epoch(code: i16, response_epoch: i32, current: i32) -> i32 {
    if matches!(code, error::FENCED_MEMBER_EPOCH | error::UNKNOWN_MEMBER_ID) {
        ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
    } else if code == error::STALE_MEMBER_EPOCH {
        // The coordinator's epoch, including 0. A negative response epoch
        // cannot be applied, so the next heartbeat rejoins.
        if response_epoch >= 0 {
            response_epoch
        } else {
            ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        }
    } else if response_epoch > 0 {
        response_epoch
    } else {
        current
    }
}

fn classic_heartbeat(code: i16) -> ClassicHeartbeat {
    match code {
        0 => ClassicHeartbeat::Ready,
        error::REBALANCE_IN_PROGRESS | error::ILLEGAL_GENERATION | error::UNKNOWN_MEMBER_ID => {
            ClassicHeartbeat::Rejoin
        }
        _ => ClassicHeartbeat::Failed,
    }
}

fn spoken_heartbeat(version: i16) -> Result<i16> {
    if (0..=4).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support Heartbeat v0-4".into(),
        ))
    }
}

fn spoken_leave_group(version: i16) -> Result<i16> {
    if (0..=5).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support LeaveGroup v0-5".into(),
        ))
    }
}

fn spoken_consumer_group_heartbeat(version: i16) -> Result<i16> {
    if (0..=1).contains(&version) {
        Ok(version)
    } else {
        Err(Error::Unsupported(
            "broker does not support ConsumerGroupHeartbeat v0-1".into(),
        ))
    }
}

fn new_kip848_member_id() -> Result<String> {
    let mut raw = [0u8; 16];
    getrandom::fill(&mut raw).map_err(|_| Error::protocol("consumer member id rng"))?;
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        raw,
    ))
}

/// True when an assigned topic id no longer matches the snapshot.
///
/// The walk pauses every 8,192 partitions. Without that pause, comparing
/// 2,000,000 assigned topic ids held the caller for 2.21 seconds.
///
/// The first changed id stops the walk. A zero snapshot id is ignored.
async fn assignment_topic_ids_changed(
    wanted: &[(String, i32)],
    mut assigned_id: impl FnMut(&str, i32) -> Option<[u8; 16]>,
    identities: &HashMap<Arc<str>, [u8; 16]>,
) -> bool {
    let mut walked = 0usize;
    for (topic, partition) in wanted {
        if assigned_id(topic, *partition)
            != identities
                .get(topic.as_str())
                .copied()
                .filter(|id| *id != [0; 16])
        {
            return true;
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    false
}

/// Snapshot of topic name to id.
///
/// The walk pauses every 8,192 topics. Without that pause, cloning
/// 2,000,000 topic ids held the caller for 480 milliseconds.
///
/// Each cached name stays on its existing allocation.
async fn clone_topic_ids(ids: &HashMap<Arc<str>, [u8; 16]>) -> HashMap<Arc<str>, [u8; 16]> {
    let mut out = HashMap::with_capacity(ids.len());
    let mut walked = 0usize;
    for (name, id) in ids {
        let _stored = out.insert(Arc::clone(name), *id);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    out
}

/// True when a KIP-848 assignment names a topic id metadata has not indexed.
///
/// The walk pauses every 8,192 topics. Without that pause, scanning
/// 2,000,000 assignment topic ids held the caller for 697 milliseconds.
///
/// The first unknown id stops the walk.
async fn assignment_has_unknown_topic_id<V>(
    assignment: &[TopicPartitions],
    names: &HashMap<[u8; 16], V>,
) -> bool {
    let mut walked = 0usize;
    for topic in assignment {
        if !names.contains_key(&topic.topic_id) {
            return true;
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    false
}

/// Topic partitions named by a KIP-848 assignment.
///
/// The walk pauses every 8,192 partitions. Without that pause, opening
/// 2,000,000 assigned partitions held the caller for 1.13 seconds.
///
/// Each topic name is copied. A negative partition is skipped. A repeated
/// partition is stored once. One subscription can name an id metadata has not
/// indexed yet. A zero id is not attached to the first name when several
/// topics are subscribed.
async fn wanted_from_kip848<I: AsRef<str>>(
    subscribed: &[String],
    id_to_name: &HashMap<[u8; 16], I>,
    assignment: &[TopicPartitions],
) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    let mut seen: HashMap<&str, HashSet<i32>> = HashMap::new();
    let mut walked = 0usize;
    for tp in assignment {
        let name = id_to_name
            .get(&tp.topic_id)
            .map(|name| name.as_ref())
            .or_else(|| {
                if subscribed.len() == 1 {
                    subscribed.first().map(String::as_str)
                } else {
                    None
                }
            });
        let Some(name) = name else {
            continue;
        };
        if !seen.contains_key(name) {
            let _ = seen.insert(name, HashSet::new());
        }
        for p in &tp.partitions {
            let fresh = *p >= 0 && seen.get_mut(name).is_some_and(|parts| parts.insert(*p));
            if fresh {
                out.push((name.to_owned(), *p));
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    out
}

/// An empty assignment revokes. A non-empty assignment that names no
/// partition stays pending so a later metadata refresh can apply it.
fn kip848_assignment_can_apply(wanted: &[(String, i32)], assignment: &[TopicPartitions]) -> bool {
    if !wanted.is_empty() || assignment.is_empty() {
        return true;
    }
    !assignment
        .iter()
        .any(|topic| topic.partitions.iter().any(|partition| *partition >= 0))
}

/// Java `ClassicKafkaConsumer` / `AsyncKafkaConsumer` constructor when
/// `group.id` is an empty string.
fn reject_java_empty_group_id(group_id: &str) -> Result<()> {
    if group_id.is_empty() {
        return Err(Error::protocol(
            "The configured group.id should not be an empty string or whitespace.",
        ));
    }
    Ok(())
}

/// Java `ClassicKafkaConsumer.throwIfNoAssignorsConfigured`.
fn reject_java_no_assignors() -> Error {
    Error::protocol(
        "Must configure at least one partition assigner class name to partition.assignment.strategy configuration property",
    )
}

pub(crate) fn collect_topics(
    topics: impl IntoIterator<Item = impl Into<String>>,
) -> Result<Vec<String>> {
    let mut raw = Vec::new();
    for topic in topics {
        let topic = topic.into();
        Topic::validate(&topic)?;
        raw.push(topic);
    }
    let mut seen: HashSet<&str> = HashSet::new();
    let mut keep = Vec::with_capacity(raw.len());
    for topic in &raw {
        keep.push(seen.insert(topic.as_str()));
    }
    let mut out = Vec::new();
    for (topic, kept) in raw.into_iter().zip(keep) {
        if kept {
            out.push(topic);
        }
    }
    if out.is_empty() {
        return Err(Error::protocol("no topics"));
    }
    Ok(out)
}

/// Copy subscribed topic names.
///
/// The walk pauses every 8,192 names. Without that pause, copying 2,000,000
/// names held the caller for 150 milliseconds.
///
/// Each topic name is still copied.
pub(crate) async fn clone_names(names: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(names.len());
    let mut walked = 0usize;
    for name in names {
        out.push(name.clone());
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    out
}

fn matching_topic_chunk() -> usize {
    8 * 1024
}

/// Keep topic names that match a subscription, then sort them.
///
/// The walk pauses every 8,192 names. Without that pause, filtering
/// 2,000,000 names held the caller for 2.87 seconds.
///
/// Each kept name is still stored. Empty names and internal topics are
/// still skipped.
pub(crate) async fn filter_matching_topics_cooperative(
    topics: impl IntoIterator<Item = impl AsRef<str>>,
    pred: impl Fn(&str) -> bool,
) -> Vec<String> {
    let mut names = HashSet::new();
    let mut walked = 0usize;
    let chunk = matching_topic_chunk();
    for topic in topics {
        let topic = topic.as_ref();
        if !(topic.is_empty() || topic.starts_with("__") || Topic::validate(topic).is_err())
            && pred(topic)
        {
            let _ = names.insert(topic.to_string());
        }
        walked = walked.saturating_add(1);
        if walked == chunk {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    let mut names: Vec<String> = names.into_iter().collect();
    sort_names_cooperative(&mut names).await;
    names
}

async fn sort_names_cooperative(names: &mut Vec<String>) {
    let chunk = matching_topic_chunk();
    if names.len() <= 1 {
        return;
    }
    let mut start = 0usize;
    while start < names.len() {
        let end = start.saturating_add(chunk).min(names.len());
        if let Some(part) = names.get_mut(start..end) {
            part.sort();
        }
        start = end;
        if start < names.len() {
            tokio::task::yield_now().await;
        }
    }
    let mut width = chunk;
    while width < names.len() {
        let mut merged = Vec::with_capacity(names.len());
        let mut start = 0usize;
        let mut written = 0usize;
        while start < names.len() {
            let mid = start.saturating_add(width).min(names.len());
            let end = mid.saturating_add(width).min(names.len());
            let mut left = start;
            let mut right = mid;
            while left < mid && right < end {
                let take_right = names
                    .get(right)
                    .zip(names.get(left))
                    .is_some_and(|(right_name, left_name)| right_name < left_name);
                if take_right {
                    merged.push(names.get_mut(right).map(std::mem::take).unwrap_or_default());
                    right = right.saturating_add(1);
                } else {
                    merged.push(names.get_mut(left).map(std::mem::take).unwrap_or_default());
                    left = left.saturating_add(1);
                }
                written = written.saturating_add(1);
                if written == chunk {
                    written = 0;
                    tokio::task::yield_now().await;
                }
            }
            while left < mid {
                merged.push(names.get_mut(left).map(std::mem::take).unwrap_or_default());
                left = left.saturating_add(1);
                written = written.saturating_add(1);
                if written == chunk {
                    written = 0;
                    tokio::task::yield_now().await;
                }
            }
            while right < end {
                merged.push(names.get_mut(right).map(std::mem::take).unwrap_or_default());
                right = right.saturating_add(1);
                written = written.saturating_add(1);
                if written == chunk {
                    written = 0;
                    tokio::task::yield_now().await;
                }
            }
            start = end;
        }
        *names = merged;
        width = width.saturating_mul(2);
        if width < names.len() {
            tokio::task::yield_now().await;
        }
    }
}

#[cfg(test)]
pub(crate) fn filter_matching_topics(
    topics: impl IntoIterator<Item = impl AsRef<str>>,
    pred: impl Fn(&str) -> bool,
) -> Vec<String> {
    let mut names = HashSet::new();
    for topic in topics {
        let topic = topic.as_ref();
        // Pattern subscription only keeps names this client can assign.
        // Empty names and internal topics are already excluded; `.`, `..`,
        // and other illegal names are excluded here too.
        if topic.is_empty() || topic.starts_with("__") || Topic::validate(topic).is_err() {
            continue;
        }
        if pred(topic) {
            let _ = names.insert(topic.to_string());
        }
    }
    let mut names: Vec<String> = names.into_iter().collect();
    names.sort();
    names
}

fn collect_commit_offsets(
    offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, impl Into<OffsetAndMetadata>)>,
) -> Vec<(TopicPartition, OffsetAndMetadata)> {
    offsets
        .into_iter()
        .map(|(tp, md)| (tp.into(), md.into()))
        .collect()
}

/// Write each ListOffsets reset into its assignment slot.
///
/// The walk pauses every 8,192 offsets. Without that pause, applying
/// 4,000,000 reset offsets held the caller for 342 milliseconds.
///
/// A negative broker offset is still an error. Each reset stays in the slot
/// it was reserved for.
async fn apply_reset_offsets(
    starts: &mut [Option<(String, i32, i64)>],
    reset_slots: &[usize],
    listed: Vec<(TopicPartition, i64)>,
) -> Result<()> {
    let mut walked = 0usize;
    for (slot, (tp, offset)) in reset_slots.iter().copied().zip(listed) {
        let start = crate::consumer::reset_position(offset).ok_or_else(|| {
            Error::protocol(format!(
                "ListOffsets returned no reset offset for {}-{}",
                tp.topic, tp.partition
            ))
        })?;
        let Some(entry) = starts.get_mut(slot) else {
            return Err(Error::protocol(format!(
                "ListOffsets returned no reset offset for {}-{}",
                tp.topic, tp.partition
            )));
        };
        *entry = Some((tp.topic, tp.partition, start));
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

/// Drop assignment slots that are still waiting for a reset.
///
/// The walk pauses every 8,192 slots. Without that pause, flattening
/// 4,000,000 reset slots held the caller for 263 milliseconds.
///
/// Finished resets stay in order. A slot that is still empty is dropped.
async fn flatten_reset_starts(starts: Vec<Option<(String, i32, i64)>>) -> Vec<(String, i32, i64)> {
    let mut out = Vec::with_capacity(starts.len());
    let mut walked = 0usize;
    for start in starts {
        if let Some(start) = start {
            out.push(start);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    out
}

/// A negative position is the reset marker after a topic is recreated.
/// It is not an offset that can be committed.
///
/// Commits for positions that `include` still wants.
///
/// The walk pauses every 8,192 positions. Without that pause, collecting
/// 2,000,000 revoked commits held the caller for 338 milliseconds.
///
/// A position that is not included is dropped. Kept positions stay in order.
async fn revoked_commits(
    positions: Vec<(TopicPartition, i64)>,
    mut epoch_of: impl FnMut(&str, i32) -> i32,
    mut include: impl FnMut(&TopicPartition, i64) -> bool,
) -> Vec<(TopicPartition, OffsetAndMetadata)> {
    let mut out = Vec::new();
    let mut walked = 0usize;
    for (tp, offset) in positions {
        if include(&tp, offset) {
            let epoch = epoch_of(&tp.topic, tp.partition);
            out.push((
                tp,
                OffsetAndMetadata::from_wire(offset, epoch, String::new()),
            ));
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    out
}

fn position_is_committable(offset: i64) -> bool {
    offset >= 0
}

#[cfg(test)]
fn group_shared_offset_topics(
    items: &[(Arc<str>, i32, i64)],
    epoch_of: impl Fn(&str, i32) -> i32,
) -> Vec<OffsetTopic> {
    let mut topics = Vec::new();
    refill_shared_offset_topics(&mut topics, items, epoch_of);
    topics
}

#[cfg(test)]
fn refill_shared_offset_topics(
    topics: &mut Vec<OffsetTopic>,
    items: &[(Arc<str>, i32, i64)],
    epoch_of: impl Fn(&str, i32) -> i32,
) {
    let mut used = 0usize;
    let mut slots: HashMap<&str, usize> = HashMap::new();
    for (topic, partition, offset) in items {
        let part = OffsetPartition {
            partition: *partition,
            offset: *offset,
            leader_epoch: epoch_of(topic.as_ref(), *partition),
            metadata: String::new(),
        };
        if let Some(&index) = slots.get(topic.as_ref()) {
            if let Some(slot) = topics.get_mut(index) {
                slot.partitions.push(part);
            }
            continue;
        }
        if let Some(slot) = topics.get_mut(used) {
            if slot.topic != topic.as_ref() {
                slot.topic.clear();
                slot.topic.push_str(topic.as_ref());
            }
            slot.partitions.clear();
            slot.partitions.push(part);
        } else {
            topics.push(OffsetTopic {
                topic: topic.as_ref().to_owned(),
                partitions: vec![part],
            });
        }
        let _ = slots.insert(topic.as_ref(), used);
        used = used.saturating_add(1);
    }
    topics.truncate(used);
}

async fn refill_shared_offset_topics_cooperative(
    topics: &mut Vec<OffsetTopic>,
    items: &[(Arc<str>, i32, i64)],
    epoch_of: impl Fn(&str, i32) -> i32,
) {
    let mut used = 0usize;
    let mut slots: HashMap<&str, usize> = HashMap::new();
    let mut walked = 0usize;
    for (topic, partition, offset) in items {
        let part = OffsetPartition {
            partition: *partition,
            offset: *offset,
            leader_epoch: epoch_of(topic.as_ref(), *partition),
            metadata: String::new(),
        };
        if let Some(&index) = slots.get(topic.as_ref()) {
            if let Some(slot) = topics.get_mut(index) {
                slot.partitions.push(part);
            }
        } else {
            if let Some(slot) = topics.get_mut(used) {
                if slot.topic != topic.as_ref() {
                    slot.topic.clear();
                    slot.topic.push_str(topic.as_ref());
                }
                slot.partitions.clear();
                slot.partitions.push(part);
            } else {
                topics.push(OffsetTopic {
                    topic: topic.as_ref().to_owned(),
                    partitions: vec![part],
                });
            }
            let _ = slots.insert(topic.as_ref(), used);
            used = used.saturating_add(1);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    topics.truncate(used);
}

#[cfg(test)]
pub(crate) fn group_offset_topics(
    items: &[(TopicPartition, OffsetAndMetadata)],
) -> Vec<OffsetTopic> {
    let mut topics = Vec::new();
    refill_offset_topics(&mut topics, items);
    topics
}

pub(crate) async fn group_offset_topics_cooperative(
    items: &[(TopicPartition, OffsetAndMetadata)],
) -> Vec<OffsetTopic> {
    let mut topics = Vec::new();
    refill_offset_topics_cooperative(&mut topics, items).await;
    topics
}

fn place_borrowed_offset<'a>(
    groups: &mut Vec<BorrowedOffsetTopic<'a>>,
    seen: &mut Vec<HashSet<i32>>,
    slots: &mut HashMap<&'a str, usize>,
    topic: &'a str,
    part: BorrowedOffsetPartition<'a>,
) {
    if let Some(&slot) = slots.get(topic) {
        let Some(known) = seen.get_mut(slot) else {
            return;
        };
        if !known.insert(part.partition) {
            return;
        }
        let Some(group) = groups.get_mut(slot) else {
            return;
        };
        group.partitions.push(part);
        return;
    }
    let slot = groups.len();
    let mut known = HashSet::new();
    let _ = known.insert(part.partition);
    seen.push(known);
    let _ = slots.insert(topic, slot);
    groups.push(BorrowedOffsetTopic {
        topic,
        partitions: vec![part],
    });
}

#[cfg(test)]
pub(crate) fn group_borrowed_offset_topics(
    items: &[(TopicPartition, OffsetAndMetadata)],
) -> Vec<BorrowedOffsetTopic<'_>> {
    let mut groups = Vec::new();
    let mut seen = Vec::new();
    let mut slots = HashMap::new();
    for (tp, md) in items {
        place_borrowed_offset(
            &mut groups,
            &mut seen,
            &mut slots,
            tp.topic.as_str(),
            BorrowedOffsetPartition {
                partition: tp.partition,
                offset: md.offset,
                leader_epoch: md.wire_epoch(),
                metadata: md.metadata.as_str(),
            },
        );
    }
    groups
}

pub(crate) async fn group_borrowed_offset_topics_cooperative(
    items: &[(TopicPartition, OffsetAndMetadata)],
) -> Vec<BorrowedOffsetTopic<'_>> {
    let mut groups = Vec::new();
    let mut seen = Vec::new();
    let mut slots = HashMap::new();
    let mut walked = 0usize;
    for (tp, md) in items {
        place_borrowed_offset(
            &mut groups,
            &mut seen,
            &mut slots,
            tp.topic.as_str(),
            BorrowedOffsetPartition {
                partition: tp.partition,
                offset: md.offset,
                leader_epoch: md.wire_epoch(),
                metadata: md.metadata.as_str(),
            },
        );
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    groups
}

fn place_offset_partition<'a>(
    topics: &mut Vec<OffsetTopic>,
    seen: &mut Vec<HashSet<i32>>,
    slots: &mut HashMap<&'a str, usize>,
    used: &mut usize,
    topic: &'a str,
    part: OffsetPartition,
) {
    if let Some(&index) = slots.get(topic) {
        let Some(known) = seen.get_mut(index) else {
            return;
        };
        if !known.insert(part.partition) {
            return;
        }
        let Some(slot) = topics.get_mut(index) else {
            return;
        };
        slot.partitions.push(part);
        return;
    }
    let partition = part.partition;
    if let Some(slot) = topics.get_mut(*used) {
        if slot.topic != topic {
            slot.topic.clear();
            slot.topic.push_str(topic);
        }
        slot.partitions.clear();
        slot.partitions.push(part);
    } else {
        topics.push(OffsetTopic {
            topic: topic.to_owned(),
            partitions: vec![part],
        });
    }
    if let Some(known) = seen.get_mut(*used) {
        known.clear();
        let _ = known.insert(partition);
    } else {
        let mut known = HashSet::new();
        let _ = known.insert(partition);
        seen.push(known);
    }
    let _ = slots.insert(topic, *used);
    *used = used.saturating_add(1);
}

#[cfg(test)]
fn refill_offset_topics(
    topics: &mut Vec<OffsetTopic>,
    items: &[(TopicPartition, OffsetAndMetadata)],
) {
    let mut used = 0usize;
    let mut seen = Vec::new();
    let mut slots = HashMap::new();
    for (tp, md) in items {
        place_offset_partition(
            topics,
            &mut seen,
            &mut slots,
            &mut used,
            tp.topic.as_str(),
            OffsetPartition {
                partition: tp.partition,
                offset: md.offset,
                leader_epoch: md.wire_epoch(),
                metadata: md.metadata.clone(),
            },
        );
    }
    topics.truncate(used);
}

async fn refill_offset_topics_cooperative(
    topics: &mut Vec<OffsetTopic>,
    items: &[(TopicPartition, OffsetAndMetadata)],
) {
    let mut used = 0usize;
    let mut seen = Vec::new();
    let mut slots = HashMap::new();
    let mut walked = 0usize;
    for (tp, md) in items {
        place_offset_partition(
            topics,
            &mut seen,
            &mut slots,
            &mut used,
            tp.topic.as_str(),
            OffsetPartition {
                partition: tp.partition,
                offset: md.offset,
                leader_epoch: md.wire_epoch(),
                metadata: md.metadata.clone(),
            },
        );
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    topics.truncate(used);
}

#[cfg(test)]
pub(crate) fn group_offset_fetch_topics<'a>(
    wanted: impl IntoIterator<Item = (&'a str, i32)>,
) -> Vec<OffsetFetchTopic> {
    let mut topics = Vec::new();
    refill_offset_fetch_topics(&mut topics, wanted);
    topics
}

fn place_borrowed_fetch_topic<'a>(
    groups: &mut Vec<(&'a str, Vec<i32>)>,
    slots: &mut HashMap<&'a str, usize>,
    seen: &mut Vec<HashSet<i32>>,
    topic: &'a str,
    part: i32,
) {
    if let Some(&slot) = slots.get(topic) {
        let fresh = seen.get_mut(slot).is_some_and(|known| known.insert(part));
        if !fresh {
            return;
        }
        if let Some((_, parts)) = groups.get_mut(slot) {
            parts.push(part);
        }
        return;
    }
    let slot = groups.len();
    let mut known = HashSet::new();
    let _ = known.insert(part);
    seen.push(known);
    let _ = slots.insert(topic, slot);
    groups.push((topic, vec![part]));
}

#[cfg(test)]
pub(crate) fn group_borrowed_offset_fetch_topics<'a>(
    wanted: impl IntoIterator<Item = (&'a str, i32)>,
) -> Vec<(&'a str, Vec<i32>)> {
    let mut groups = Vec::new();
    let mut slots = HashMap::new();
    let mut seen = Vec::new();
    for (topic, part) in wanted {
        place_borrowed_fetch_topic(&mut groups, &mut slots, &mut seen, topic, part);
    }
    groups
}

pub(crate) async fn group_borrowed_offset_fetch_topics_cooperative(
    wanted: &[TopicPartition],
) -> Vec<(&str, Vec<i32>)> {
    let mut groups = Vec::new();
    let mut slots = HashMap::new();
    let mut seen = Vec::new();
    let mut walked = 0usize;
    for tp in wanted {
        place_borrowed_fetch_topic(
            &mut groups,
            &mut slots,
            &mut seen,
            tp.topic.as_str(),
            tp.partition,
        );
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    groups
}

fn place_fetched_partition<'a>(
    topics: &mut Vec<OffsetFetchTopic>,
    seen: &mut Vec<HashSet<i32>>,
    slots: &mut HashMap<&'a str, usize>,
    used: &mut usize,
    topic: &'a str,
    part: i32,
) {
    if let Some(&index) = slots.get(topic) {
        let Some(slot) = topics.get_mut(index) else {
            return;
        };
        let Some(known) = seen.get_mut(index) else {
            return;
        };
        if !known.insert(part) {
            return;
        }
        slot.partitions.push(part);
        return;
    }
    if let Some(slot) = topics.get_mut(*used) {
        if slot.topic != topic {
            slot.topic.clear();
            slot.topic.push_str(topic);
        }
        slot.partitions.clear();
        slot.partitions.push(part);
    } else {
        topics.push(OffsetFetchTopic {
            topic: topic.to_owned(),
            partitions: vec![part],
        });
    }
    if let Some(known) = seen.get_mut(*used) {
        known.clear();
        let _ = known.insert(part);
    } else {
        let mut known = HashSet::new();
        let _ = known.insert(part);
        seen.push(known);
    }
    let _ = slots.insert(topic, *used);
    *used = used.saturating_add(1);
}

#[cfg(test)]
fn refill_offset_fetch_topics<'a>(
    topics: &mut Vec<OffsetFetchTopic>,
    wanted: impl IntoIterator<Item = (&'a str, i32)>,
) {
    let mut used = 0usize;
    let mut seen = Vec::new();
    let mut slots = HashMap::new();
    for (topic, part) in wanted {
        place_fetched_partition(topics, &mut seen, &mut slots, &mut used, topic, part);
    }
    topics.truncate(used);
}

async fn refill_offset_fetch_topics_cooperative(
    topics: &mut Vec<OffsetFetchTopic>,
    wanted: &[(String, i32)],
) {
    let mut used = 0usize;
    let mut seen = Vec::new();
    let mut slots = HashMap::new();
    let mut walked = 0usize;
    for (topic, part) in wanted {
        place_fetched_partition(topics, &mut seen, &mut slots, &mut used, topic, *part);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    topics.truncate(used);
}

async fn refill_topic_partitions_cooperative(
    topics: &mut Vec<OffsetFetchTopic>,
    wanted: &[TopicPartition],
) {
    let mut used = 0usize;
    let mut seen = Vec::new();
    let mut slots = HashMap::new();
    let mut walked = 0usize;
    for tp in wanted {
        place_fetched_partition(
            topics,
            &mut seen,
            &mut slots,
            &mut used,
            tp.topic.as_str(),
            tp.partition,
        );
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    topics.truncate(used);
}

pub(crate) async fn group_offset_fetch_topics_cooperative(
    wanted: &[TopicPartition],
) -> Vec<OffsetFetchTopic> {
    let mut topics = Vec::new();
    refill_topic_partitions_cooperative(&mut topics, wanted).await;
    topics
}

async fn index_revoked(revoked: &[(String, i32)]) -> HashMap<&str, HashSet<i32>> {
    let mut parts: HashMap<&str, HashSet<i32>> = HashMap::new();
    let mut index = 0usize;
    let mut walked = 0usize;
    while index < revoked.len() {
        let Some((topic, partition)) = revoked.get(index) else {
            break;
        };
        if let Some(slot) = parts.get_mut(topic.as_str()) {
            let _ = slot.insert(*partition);
        } else {
            let mut slot = HashSet::new();
            let _ = slot.insert(*partition);
            let _ = parts.insert(topic.as_str(), slot);
        }
        index = index.saturating_add(1);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    parts
}

fn revoked_contains(index: &HashMap<&str, HashSet<i32>>, topic: &str, partition: i32) -> bool {
    index
        .get(topic)
        .is_some_and(|parts| parts.contains(&partition))
}

async fn remember_current_partitions(
    map: &mut HashMap<String, HashSet<i32>>,
    parts: &[(Arc<str>, i32, i64)],
) {
    let mut walked = 0usize;
    for (topic, partition, _) in parts {
        remember_partition(map, topic.as_ref(), *partition);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
}

async fn remember_wanted_partitions(
    map: &mut HashMap<String, HashSet<i32>>,
    parts: &[(String, i32)],
) {
    let mut walked = 0usize;
    for (topic, partition) in parts {
        remember_partition(map, topic, *partition);
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
}

fn remember_partition(map: &mut HashMap<String, HashSet<i32>>, topic: &str, partition: i32) {
    if let Some(parts) = map.get_mut(topic) {
        let _ = parts.insert(partition);
        return;
    }
    let mut parts = HashSet::new();
    let _ = parts.insert(partition);
    let _ = map.insert(topic.to_owned(), parts);
}

/// Dial targets for an assignment. A missing leader is skipped.
///
/// Remembering 2,000,000 assignment leaders held the caller for 200ms.
/// A known leader is still stored once.
async fn remember_partition_leaders<T: AsRef<str>>(
    cluster: &crate::cluster::Cluster,
    parts: &[(T, i32)],
    leaders: &mut Vec<i32>,
    seen: &mut HashSet<i32>,
) {
    let mut walked = 0usize;
    for (topic, partition) in parts {
        if let Ok(node) = cluster.leader_id(topic.as_ref(), *partition) {
            crate::consumer::Consumer::remember_node(leaders, seen, node);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
}

fn remember_i64(
    map: &mut HashMap<String, HashMap<i32, i64>>,
    topic: &str,
    partition: i32,
    value: i64,
) {
    if let Some(parts) = map.get_mut(topic) {
        let _ = parts.insert(partition, value);
        return;
    }
    let mut parts = HashMap::new();
    let _ = parts.insert(partition, value);
    let _ = map.insert(topic.to_owned(), parts);
}

fn remember_i32(
    map: &mut HashMap<String, HashMap<i32, i32>>,
    topic: &str,
    partition: i32,
    value: i32,
) {
    if let Some(parts) = map.get_mut(topic) {
        let _ = parts.insert(partition, value);
        return;
    }
    let mut parts = HashMap::new();
    let _ = parts.insert(partition, value);
    let _ = map.insert(topic.to_owned(), parts);
}

fn nested_i64(
    map: &HashMap<String, HashMap<i32, i64>>,
    topic: &str,
    partition: i32,
) -> Option<i64> {
    map.get(topic)?.get(&partition).copied()
}

fn nested_i32(
    map: &HashMap<String, HashMap<i32, i32>>,
    topic: &str,
    partition: i32,
) -> Option<i32> {
    map.get(topic)?.get(&partition).copied()
}

fn remember_topic_id(map: &mut DeliveredTopicIds, topic: &str, partition: i32, id: [u8; 16]) {
    if let Some(parts) = map.get_mut(topic) {
        let _ = parts.insert(partition, id);
        return;
    }
    let mut parts = HashMap::new();
    let _ = parts.insert(partition, id);
    let _ = map.insert(Arc::from(topic), parts);
}

fn clear_delivered_topic_ids(map: &mut DeliveredTopicIds) {
    for parts in map.values_mut() {
        parts.clear();
    }
}

fn remember_shared_topic_id(
    map: &mut DeliveredTopicIds,
    topic: &Arc<str>,
    partition: i32,
    id: [u8; 16],
) {
    if let Some(parts) = map.get_mut(topic.as_ref()) {
        let _ = parts.insert(partition, id);
        return;
    }
    let mut parts = HashMap::new();
    let _ = parts.insert(partition, id);
    let _ = map.insert(Arc::clone(topic), parts);
}

fn nested_topic_id(map: &DeliveredTopicIds, topic: &str, partition: i32) -> Option<[u8; 16]> {
    map.get(topic)?.get(&partition).copied()
}

pub(crate) fn check_fetched_offsets(fetched: &[FetchedOffsetTopic]) -> Result<()> {
    for topic in fetched {
        for partition in &topic.partitions {
            if partition.has_error() {
                return Err(Error::broker(
                    partition.error_code,
                    format!("OffsetFetch {}-{}", topic.topic, partition.partition),
                ));
            }
        }
    }
    Ok(())
}

/// The last fetched commit for `topic`/`partition`.
///
/// A later copy of the same topic replaces an earlier partition, matching the
/// map used by [`committed_offset_map`].
pub(crate) async fn index_fetched_offsets_cooperative(
    fetched: &[FetchedOffsetTopic],
) -> HashMap<(&str, i32), &FetchedOffset> {
    let mut index = HashMap::new();
    let mut walked = 0usize;
    for topic in fetched {
        // A later topic with the same name replaces earlier hits. Within one
        // topic, the first partition wins, matching `fetched_committed`.
        let mut seen_here = HashSet::new();
        for partition in &topic.partitions {
            if seen_here.insert(partition.partition) {
                let _ = index.insert((topic.topic.as_str(), partition.partition), partition);
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    index
}

#[cfg(test)]
pub(crate) fn fetched_committed<'a>(
    fetched: &'a [FetchedOffsetTopic],
    topic: &str,
    partition: i32,
) -> Option<&'a FetchedOffset> {
    let mut found = None;
    for item in fetched {
        if item.topic != topic {
            continue;
        }
        if let Some(part) = item
            .partitions
            .iter()
            .find(|part| part.partition == partition)
        {
            found = Some(part);
        }
    }
    found
}

pub(crate) fn offset_and_metadata_from(part: &FetchedOffset) -> OffsetAndMetadata {
    OffsetAndMetadata::from_wire(part.offset, part.leader_epoch, part.metadata.clone())
}

#[cfg(test)]
fn insert_committed(
    parts: &mut HashMap<i32, OffsetAndMetadata>,
    topic: &FetchedOffsetTopic,
) -> Result<()> {
    for partition in &topic.partitions {
        if partition.has_error() {
            return Err(Error::broker(
                partition.error_code,
                format!("OffsetFetch {}-{}", topic.topic, partition.partition),
            ));
        }
        let _ = parts.insert(
            partition.partition,
            OffsetAndMetadata::from_wire(
                partition.offset,
                partition.leader_epoch,
                partition.metadata.clone(),
            ),
        );
    }
    Ok(())
}

async fn insert_committed_cooperative(
    parts: &mut HashMap<i32, OffsetAndMetadata>,
    topic: &FetchedOffsetTopic,
) -> Result<()> {
    let mut walked = 0usize;
    for partition in &topic.partitions {
        if partition.has_error() {
            return Err(Error::broker(
                partition.error_code,
                format!("OffsetFetch {}-{}", topic.topic, partition.partition),
            ));
        }
        let _ = parts.insert(
            partition.partition,
            OffsetAndMetadata::from_wire(
                partition.offset,
                partition.leader_epoch,
                partition.metadata.clone(),
            ),
        );
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

/// One topic per entry.
///
/// Building a map of 2,000,000 topics held the caller for 4561ms. A topic name is still stored.
pub(crate) async fn committed_offset_map_cooperative(
    fetched: &[FetchedOffsetTopic],
) -> Result<HashMap<String, HashMap<i32, OffsetAndMetadata>>> {
    let mut map: HashMap<String, HashMap<i32, OffsetAndMetadata>> = HashMap::new();
    let mut walked = 0usize;
    for topic in fetched {
        if map.contains_key(topic.topic.as_str()) {
            let parts = map
                .get_mut(topic.topic.as_str())
                .ok_or_else(|| Error::protocol("missing committed topic"))?;
            insert_committed_cooperative(parts, topic).await?;
        } else {
            let mut parts = HashMap::new();
            insert_committed_cooperative(&mut parts, topic).await?;
            let _ = map.insert(topic.topic.clone(), parts);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
    Ok(map)
}

#[cfg(test)]
pub(crate) fn committed_offset_map(
    fetched: &[FetchedOffsetTopic],
) -> Result<HashMap<String, HashMap<i32, OffsetAndMetadata>>> {
    let mut map: HashMap<String, HashMap<i32, OffsetAndMetadata>> = HashMap::new();
    for topic in fetched {
        if let Some(parts) = map.get_mut(topic.topic.as_str()) {
            insert_committed(parts, topic)?;
            continue;
        }
        let mut parts = HashMap::new();
        insert_committed(&mut parts, topic)?;
        let _ = map.insert(topic.topic.clone(), parts);
    }
    Ok(map)
}

#[cfg(test)]
pub(crate) fn committed_offset<'a>(
    map: &'a HashMap<String, HashMap<i32, OffsetAndMetadata>>,
    topic: &str,
    partition: i32,
) -> Option<&'a OffsetAndMetadata> {
    map.get(topic)?.get(&partition)
}

#[cfg(test)]
fn remember_unique_topic(topics: &mut Vec<String>, topic: &str) {
    if topics.iter().any(|name| name == topic) {
        return;
    }
    topics.push(topic.to_owned());
}

async fn remember_existing_topics(seen: &mut HashSet<String>, topics: &[String]) {
    let mut walked = 0usize;
    for name in topics {
        let _ = seen.insert(name.clone());
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
}

async fn extend_unique_topics(
    topics: &mut Vec<String>,
    seen: &mut HashSet<String>,
    incoming: &[String],
) {
    let mut walked = 0usize;
    for name in incoming {
        if !seen.contains(name.as_str()) {
            let owned = name.clone();
            let _ = seen.insert(owned.clone());
            topics.push(owned);
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
    }
}

/// One owned topic becomes one name per partition. The last partition keeps it.
#[cfg(test)]
fn expand_topic_partitions(topics: Vec<(String, Vec<i32>)>) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    for (mut topic, partitions) in topics {
        let count = partitions.len();
        for (index, partition) in partitions.into_iter().enumerate() {
            let name = if index.saturating_add(1) == count {
                std::mem::take(&mut topic)
            } else {
                topic.clone()
            };
            out.push((name, partition));
        }
    }
    out
}

async fn expand_topic_partitions_cooperative(
    topics: Vec<(String, Vec<i32>)>,
) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    let mut walked = 0usize;
    for (mut topic, partitions) in topics {
        let count = partitions.len();
        for (index, partition) in partitions.into_iter().enumerate() {
            let name = if index.saturating_add(1) == count {
                std::mem::take(&mut topic)
            } else {
                topic.clone()
            };
            out.push((name, partition));
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    out
}

#[cfg(test)]
pub(crate) fn committed_offset_pairs(
    map: HashMap<String, HashMap<i32, OffsetAndMetadata>>,
) -> Vec<(TopicPartition, OffsetAndMetadata)> {
    let mut out = Vec::new();
    for (mut topic, parts) in map {
        let count = parts.len();
        for (index, (partition, md)) in parts.into_iter().enumerate() {
            let name = if index.saturating_add(1) == count {
                std::mem::take(&mut topic)
            } else {
                topic.clone()
            };
            out.push((TopicPartition::new(name, partition), md));
        }
    }
    out
}

pub(crate) async fn committed_offset_pairs_cooperative(
    map: HashMap<String, HashMap<i32, OffsetAndMetadata>>,
) -> Vec<(TopicPartition, OffsetAndMetadata)> {
    let mut out = Vec::new();
    let mut walked = 0usize;
    for (mut topic, parts) in map {
        let count = parts.len();
        for (index, (partition, md)) in parts.into_iter().enumerate() {
            let name = if index.saturating_add(1) == count {
                std::mem::take(&mut topic)
            } else {
                topic.clone()
            };
            out.push((TopicPartition::new(name, partition), md));
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    out
}

pub(crate) fn commit_position_epoch(
    pending: Option<i32>,
    last_fetched: i32,
    metadata_epoch: i32,
) -> i32 {
    if let Some(epoch) = pending.filter(|epoch| *epoch >= 0) {
        return epoch;
    }
    if last_fetched >= 0 {
        return last_fetched;
    }
    metadata_epoch
}

#[cfg(test)]
fn committed_starts(
    wanted: &[(String, i32)],
    fetched: &[FetchedOffsetTopic],
    reset: AutoOffsetReset,
) -> Result<Vec<(String, i32, i64)>> {
    check_fetched_offsets(fetched)?;
    let mut out = Vec::with_capacity(wanted.len());
    for (topic, part) in wanted {
        let committed = fetched_committed(fetched, topic, *part)
            .map(|part| part.offset)
            .unwrap_or(OffsetAndMetadata::INVALID_OFFSET);
        let start = if committed >= 0 {
            committed
        } else {
            match reset {
                AutoOffsetReset::Earliest | AutoOffsetReset::Latest => {
                    return Err(Error::protocol(
                        "auto.offset.reset needs ListOffsets at assign",
                    ));
                }
                AutoOffsetReset::None => {
                    return Err(Error::protocol(format!(
                        "no committed offset for {topic}-{part}"
                    )));
                }
            }
        };
        out.push((topic.clone(), *part, start));
    }
    Ok(out)
}

// Preserve error categories while identifying a malformed response without
// retaining broker addresses, group identities, or wire payloads.
fn group_decode_context(error: Error, api: &str, version: i16, length: usize) -> Error {
    match error {
        Error::Protocol(message) => Error::protocol(format!(
            "{api} v{version} response ({length} bytes): {message}"
        )),
        other => other,
    }
}

fn peek_error_code(body: &[u8]) -> Option<i16> {
    if body.len() >= 6 {
        let b4 = *body.get(4)?;
        let b5 = *body.get(5)?;
        Some(i16::from_be_bytes([b4, b5]))
    } else if body.len() >= 2 {
        let b0 = *body.first()?;
        let b1 = *body.get(1)?;
        Some(i16::from_be_bytes([b0, b1]))
    } else {
        None
    }
}

/// Race coordinator handshakes and return once the losers have stopped.
///
/// A later join continues with group RPCs, which hides a handshake that was
/// only aborted.
#[doc(hidden)]
pub async fn test_race_coordinator(cfg: &ConsumerConfig, group_id: &str) -> Result<()> {
    let _conn = discover_coord(cfg, group_id, COORDINATOR_GROUP).await?;
    Ok(())
}

pub(crate) async fn discover_coord(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
) -> Result<BrokerConn> {
    let deadline = Instant::now()
        .checked_add(cfg.request_timeout)
        .ok_or(Error::Timeout)?;
    discover_coord_within(cfg, group_id, key_type, deadline).await
}

/// Find a coordinator using only the time still left on `deadline`.
/// Open the member socket and find the group coordinator together.
///
/// The member dial is required first when both fail. A coordinator error is
/// returned only after the member socket has connected.
pub(crate) async fn connect_member_and_coordinator(
    cfg: &ConsumerConfig,
    group_id: &str,
    topics: &[String],
    prefetch_all: bool,
    deadline: Instant,
) -> Result<(Consumer, Result<(BrokerConn, Vec<ApiVersion>)>)> {
    let started = async {
    let member = Consumer::connect_within(cfg.clone(), deadline);
    let coordinator = async {
        discover_coord_with_versions(cfg, group_id, COORDINATOR_GROUP, deadline)
            .await
            .map(|(conn, versions)| (conn, versions.api_keys))
    };
    tokio::pin!(member);
    tokio::pin!(coordinator);
    let mut opened_member = None;
    let mut opened_coordinator = None;
    let mut coordinator_error = None;
    loop {
        if opened_member.is_some() {
            break;
        }
        tokio::select! {
            biased;
            result = &mut member, if opened_member.is_none() => {
                opened_member = Some(result?);
            }
            result = &mut coordinator, if opened_coordinator.is_none() && coordinator_error.is_none() => {
                match result {
                    Ok(coord) => opened_coordinator = Some(coord),
                    Err(error) => coordinator_error = Some(error),
                }
            }
        }
    }
    let mut consumer = opened_member.ok_or_else(|| Error::protocol("member connection"))?;
    if !topics.is_empty() || prefetch_all {
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let refresh = if topics.is_empty() {
            consumer.refresh_metadata_timeout(None, timeout)
        } else {
            consumer.refresh_metadata_timeout(Some(topics), timeout)
        };
        tokio::pin!(refresh);
        let mut metadata = None;
        loop {
            if metadata.is_some() && (opened_coordinator.is_some() || coordinator_error.is_some()) {
                break;
            }
            tokio::select! {
                biased;
                result = &mut refresh, if metadata.is_none() => {
                    metadata = Some(result);
                }
                result = &mut coordinator, if opened_coordinator.is_none() && coordinator_error.is_none() => {
                    match result {
                        Ok(coord) => opened_coordinator = Some(coord),
                        Err(error) => coordinator_error = Some(error),
                    }
                }
            }
        }
        if coordinator_error.is_none() {
            metadata.ok_or_else(|| Error::protocol("member metadata"))??;
        }
    } else {
        loop {
            if opened_coordinator.is_some() || coordinator_error.is_some() {
                break;
            }
            tokio::select! {
                biased;
                result = &mut coordinator, if opened_coordinator.is_none() && coordinator_error.is_none() => {
                    match result {
                        Ok(coord) => opened_coordinator = Some(coord),
                        Err(error) => coordinator_error = Some(error),
                    }
                }
            }
        }
    }
    if let Some(error) = coordinator_error {
        return Ok((consumer, Err(error)));
    }
    Ok((
        consumer,
        Ok(opened_coordinator.ok_or_else(|| Error::protocol("coordinator connection"))?),
    ))
    }
    .await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    started
}

/// Wait until the coordinator closes `conn`.
///
/// A response that is already buffered does not finish this wait. The
/// heartbeat interval stays armed beside it and reads those bytes. The wait
/// ends when the peer later closes, after the idle watch has shut the
/// socket down.
pub(crate) async fn coordinator_lost(conn: &mut Option<BrokerConn>) -> bool {
    let Some(current) = conn.as_mut() else {
        return std::future::pending::<bool>().await;
    };
    if !current.has_queued_response() {
        match current.inbound_ready().await {
            Err(_) => return true,
            Ok(()) if current.is_closed() => return true,
            Ok(()) => {}
        }
    }
    current.wait_until_closed().await;
    true
}

async fn retire_group_conn(conn: &mut Option<BrokerConn>) {
    if let Some(mut current) = conn.take() {
        current.close();
        current.release_if_closed().await;
    }
}

/// Shut a lookup socket down before another connection replaces it.
///
/// Dropping the connection aborts its idle watch, so the broker stays in its
/// read until that watch is scheduled.
async fn retire_lookup_conn(mut conn: BrokerConn) {
    conn.close();
    conn.release_if_closed().await;
}

/// Reconnect an idle coordinator while its address is checked again.
///
/// The first connection still uses a normal lookup. An idle socket keeps its
/// address and is dialed during that lookup.
pub(crate) async fn reopen_idle_coordinator(
    conn: &mut Option<BrokerConn>,
    cfg: &ConsumerConfig,
    group_id: &str,
    deadline: Instant,
    bootstrap_keys: &[ApiVersion],
    address_keys: &HashMap<String, Vec<ApiVersion>>,
    prior: &mut HashMap<String, KnownCoord>,
) {
    let known = if conn
        .as_ref()
        .is_some_and(|c| c.idle_expired(cfg.connections_max_idle))
    {
        let snap = conn.as_ref().map(|conn| {
            KnownCoord::capture(conn)
                .with_bootstrap_keys(bootstrap_keys)
                .with_address_keys(address_keys)
        });
        retire_group_conn(conn).await;
        snap
    } else {
        None
    };
    if conn.is_some() {
        return;
    }
    *conn = if let Some(known) = known {
        Box::pin(rediscover_known_coord(
            cfg,
            group_id,
            COORDINATOR_GROUP,
            &known,
            prior,
        ))
        .await
        .ok()
    } else if !prior.is_empty() && !bootstrap_keys.is_empty() {
        leave_coordinator(cfg, group_id, deadline, bootstrap_keys, prior).await
    } else {
        discover_coord_within(cfg, group_id, COORDINATOR_GROUP, deadline)
            .await
            .ok()
    };
}

pub(crate) async fn discover_coord_within(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: Instant,
) -> Result<BrokerConn> {
    discover_coord_with_versions(cfg, group_id, key_type, deadline)
        .await
        .map(|(conn, _)| conn)
}

pub(crate) async fn discover_coord_with_versions(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: Instant,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
    let discovered = async {
        if cfg.bootstrap.len() > 16 || group_id.len() > i16::MAX as usize {
            return Err(Error::protocol(
                "bootstrap/group key exceeds discovery limit",
            ));
        }
        let mut attempt = 0u32;
        loop {
            if attempt >= 8 {
                return Err(Error::protocol("coordinator discovery retry limit"));
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let result = discover_coord_once(cfg, group_id, key_type, deadline).await;
            match result {
                Err(error)
                    if matches!(error, Error::Io(_) | Error::Timeout)
                        || error
                            .broker_code()
                            .is_some_and(error::coordinator_retriable) =>
                {
                    crate::config::sleep_retry_backoff(
                        cfg.retry_backoff.max(Duration::from_millis(1)),
                        cfg.retry_backoff_max.max(Duration::from_millis(1)),
                        attempt,
                        deadline,
                    )
                    .await;
                    attempt = attempt.saturating_add(1);
                }
                result => return result,
            }
        }
    }
    .await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    discovered
}

async fn discover_coord_once(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: Instant,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
    let deadline = crate::net::Deadline::from_std(deadline);
    if cfg.bootstrap.len() > 1 {
        return discover_coord_raced(cfg, group_id, key_type, deadline, None, &HashMap::new())
            .await;
    }
    let mut last = Error::protocol("find coordinator failed");
    for addr in &cfg.bootstrap {
        match probe_bootstrap(cfg, addr, group_id, key_type, deadline).await {
            Ok(found) => return Ok(found),
            Err(error) => {
                let fatal = error
                    .broker_code()
                    .is_some_and(|code| !error::coordinator_retriable(code));
                last = error;
                if fatal {
                    return Err(last);
                }
            }
        }
    }
    Err(last)
}

/// How to open one broker while a coordinator is rediscovered.
enum RememberedDial {
    /// Stamp versions already learned for this address.
    Known(KnownCoord),
    /// Apply a stored ApiVersions list. No new handshake.
    Keys(Vec<ApiVersion>),
    /// First contact. Negotiate ApiVersions.
    Negotiate,
}

fn remembered_dial(
    addr: &str,
    cfg: &ConsumerConfig,
    known: Option<&KnownCoord>,
    prior: &HashMap<String, KnownCoord>,
) -> RememberedDial {
    let Some(known) = known else {
        return RememberedDial::Negotiate;
    };
    if addr == known.addr && known.find_coordinator_version >= 0 {
        return RememberedDial::Known(known.clone());
    }
    if let Some(previous) = prior.get(addr) {
        if previous.find_coordinator_version >= 0 {
            return RememberedDial::Known(previous.clone());
        }
    }
    if let Some(keys) = known.address_keys.get(addr).filter(|keys| !keys.is_empty()) {
        return RememberedDial::Keys(keys.clone());
    }
    if cfg
        .bootstrap
        .first()
        .is_some_and(|bootstrap| bootstrap == addr)
        && !known.bootstrap_keys.is_empty()
    {
        return RememberedDial::Keys(known.bootstrap_keys.clone());
    }
    RememberedDial::Negotiate
}

async fn open_remembered_dial(
    cfg: &ConsumerConfig,
    addr: &str,
    dial: RememberedDial,
) -> Result<CoordHandshake> {
    match dial {
        RememberedDial::Known(known) => {
            let conn = connect_known_coord(cfg, &known).await?;
            let version = conn.find_coordinator_version;
            Ok((
                conn,
                version,
                remembered_coord_versions(&known.bootstrap_keys),
            ))
        }
        RememberedDial::Keys(keys) => open_coord_with_keys(cfg, addr, &keys).await,
        RememberedDial::Negotiate => open_coord_with_find_version(cfg, addr).await,
    }
}

async fn finish_aborted_group_handshakes(
    mut set: tokio::task::JoinSet<(usize, Result<CoordHandshake>)>,
) {
    set.abort_all();
    let mut extra = 0u32;
    while !set.is_empty() {
        tokio::select! {
            biased;
            joined = set.join_next() => {
                let Some(joined) = joined else {
                    break;
                };
                if let Ok((_, Ok((conn, _, _)))) = joined {
                    retire_lookup_conn(conn).await;
                }
            }
            () = tokio::task::yield_now() => {
                extra = extra.saturating_add(1);
                if extra >= 8 {
                    break;
                }
            }
        }
    }
    // A cancelled group handshake drops its socket when the task is polled.
    // The idle watch then has to run before that broker sees the close.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

async fn discover_coord_raced(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: crate::net::Deadline,
    known: Option<&KnownCoord>,
    prior: &HashMap<String, KnownCoord>,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
    let mut inflight = tokio::task::JoinSet::<(usize, Result<CoordHandshake>)>::new();
    for (index, addr) in cfg.bootstrap.iter().enumerate() {
        let cfg_task = cfg.clone();
        let addr_task = addr.clone();
        let dial = remembered_dial(addr, cfg, known, prior);
        let _ = inflight.spawn(async move {
            let result = match bounded_coord_config(&cfg_task, deadline) {
                Ok(bounded) => open_remembered_dial(&bounded, &addr_task, dial).await,
                Err(error) => Err(error),
            };
            (index, result)
        });
    }
    let mut ready: Vec<Option<Result<CoordHandshake>>> = Vec::new();
    ready.resize_with(inflight.len(), || None);
    let mut last = Error::protocol("find coordinator failed");
    while !inflight.is_empty() {
        if deadline.remaining().is_err() {
            retire_ready_lookups(&mut ready).await;
            finish_aborted_group_handshakes(inflight).await;
            return Err(Error::Timeout);
        }
        let rest = deadline.remaining().unwrap_or(Duration::ZERO);
        let joined = tokio::select! {
            joined = inflight.join_next() => joined,
            _ = tokio::time::sleep(rest) => {
                retire_ready_lookups(&mut ready).await;
                finish_aborted_group_handshakes(inflight).await;
                return Err(Error::Timeout);
            }
        };
        let Some(Ok((index, result))) = joined else {
            break;
        };
        if let Some(slot) = ready.get_mut(index) {
            *slot = Some(result);
        }
        // A fast terminal error on an earlier bootstrap must be observed
        // before a later broker is asked. Handshakes that finish together
        // are gathered before that choice.
        let gather = deadline
            .remaining()
            .unwrap_or(Duration::ZERO)
            .min(Duration::from_millis(5));
        let gather_done = tokio::time::sleep(gather);
        tokio::pin!(gather_done);
        loop {
            tokio::select! {
                biased;
                joined = inflight.join_next(), if !inflight.is_empty() => {
                    if let Some(Ok((index, result))) = joined {
                        if let Some(slot) = ready.get_mut(index) {
                            *slot = Some(result);
                        }
                    }
                }
                _ = &mut gather_done => break,
            }
        }
        let mut cursor = 0usize;
        while cursor < ready.len() {
            match ready.get(cursor).and_then(|slot| slot.as_ref()) {
                None => {
                    let later =
                        ready
                            .iter()
                            .enumerate()
                            .skip(cursor + 1)
                            .find_map(|(index, slot)| match slot {
                                Some(Ok(_)) => Some(index),
                                _ => None,
                            });
                    let Some(index) = later else {
                        break;
                    };
                    match ask_ready_coordinator(
                        cfg, group_id, key_type, deadline, &mut ready, index, known, prior,
                    )
                    .await
                    {
                        CoordAsk::Found(found) => {
                            retire_ready_lookups(&mut ready).await;
                            finish_aborted_group_handshakes(inflight).await;
                            return Ok(*found);
                        }
                        CoordAsk::Fatal(error) => {
                            retire_ready_lookups(&mut ready).await;
                            finish_aborted_group_handshakes(inflight).await;
                            return Err(error);
                        }
                        CoordAsk::Retry(error) => last = error,
                    }
                    break;
                }
                Some(_) => {
                    match ask_ready_coordinator(
                        cfg, group_id, key_type, deadline, &mut ready, cursor, known, prior,
                    )
                    .await
                    {
                        CoordAsk::Found(found) => {
                            retire_ready_lookups(&mut ready).await;
                            finish_aborted_group_handshakes(inflight).await;
                            return Ok(*found);
                        }
                        CoordAsk::Fatal(error) => {
                            retire_ready_lookups(&mut ready).await;
                            finish_aborted_group_handshakes(inflight).await;
                            return Err(error);
                        }
                        CoordAsk::Retry(error) => {
                            last = error;
                            cursor = cursor.saturating_add(1);
                        }
                    }
                }
            }
        }
    }
    retire_ready_lookups(&mut ready).await;
    finish_aborted_group_handshakes(inflight).await;
    if deadline.remaining().is_err() {
        return Err(Error::Timeout);
    }
    Err(last)
}

type CoordHandshake = (BrokerConn, i16, crate::protocol::api::ApiVersionsResponse);

async fn retire_ready_lookups(ready: &mut [Option<Result<CoordHandshake>>]) {
    for slot in ready {
        if let Some(Ok((conn, _, _))) = slot.take() {
            retire_lookup_conn(conn).await;
        }
    }
}

enum CoordAsk {
    Found(Box<(BrokerConn, crate::protocol::api::ApiVersionsResponse)>),
    Fatal(Error),
    Retry(Error),
}

#[expect(
    clippy::too_many_arguments,
    reason = "rediscovery carries the coordinator versions learned on earlier visits"
)]
async fn ask_ready_coordinator(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: crate::net::Deadline,
    ready: &mut [Option<Result<CoordHandshake>>],
    index: usize,
    known: Option<&KnownCoord>,
    prior: &HashMap<String, KnownCoord>,
) -> CoordAsk {
    let Some(slot) = ready.get_mut(index).and_then(Option::take) else {
        return CoordAsk::Retry(Error::protocol("missing coordinator handshake"));
    };
    let (hop, version, versions) = match slot {
        Ok(handshake) => handshake,
        Err(error) => return CoordAsk::Retry(error),
    };
    let asked = find_coordinator_on(
        cfg,
        hop,
        version,
        versions,
        group_id,
        key_type,
        deadline,
        Some(ready),
        known,
        prior,
    )
    .await;
    match asked {
        Ok(found) => CoordAsk::Found(Box::new(found)),
        Err(error)
            if error
                .broker_code()
                .is_some_and(|code| !error::coordinator_retriable(code)) =>
        {
            CoordAsk::Fatal(error)
        }
        Err(error) => CoordAsk::Retry(error),
    }
}

async fn probe_bootstrap(
    cfg: &ConsumerConfig,
    addr: &str,
    group_id: &str,
    key_type: i8,
    deadline: crate::net::Deadline,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
    let bounded = bounded_coord_config(cfg, deadline)?;
    let (hop, version, versions) = open_coord_with_find_version(&bounded, addr).await?;
    find_coordinator_on(
        cfg,
        hop,
        version,
        versions,
        group_id,
        key_type,
        deadline,
        None,
        None,
        &HashMap::new(),
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "coordinator lookup can reuse a bootstrap handshake that is already open"
)]
async fn find_coordinator_on(
    cfg: &ConsumerConfig,
    mut hop: BrokerConn,
    version: i16,
    versions: crate::protocol::api::ApiVersionsResponse,
    group_id: &str,
    key_type: i8,
    deadline: crate::net::Deadline,
    ready: Option<&mut [Option<Result<CoordHandshake>>]>,
    known: Option<&KnownCoord>,
    prior: &HashMap<String, KnownCoord>,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
    let rest = match deadline.remaining() {
        Ok(rest) => rest,
        Err(error) => {
            retire_lookup_conn(hop).await;
            return Err(error);
        }
    };
    let body = match hop
        .roundtrip(
            FIND_COORDINATOR,
            version,
            |buf| encode_find_coordinator_request_typed(buf, version, group_id, key_type),
            rest,
        )
        .await
    {
        Ok(body) => body,
        Err(error) => {
            retire_lookup_conn(hop).await;
            return Err(error);
        }
    };
    let (err, _node, host, port) =
        match decode_find_coordinator_response(&mut body.clone(), version) {
            Ok(decoded) => decoded,
            Err(error) => {
                let error = group_decode_context(error, "FindCoordinator", version, body.len());
                retire_lookup_conn(hop).await;
                return Err(error);
            }
        };
    if err != 0 {
        let error = Err(Error::broker(err, "FindCoordinator"));
        retire_lookup_conn(hop).await;
        return error;
    }
    let Some(coord_addr) = crate::cluster::dial_address(&host, port) else {
        let error = Err(Error::protocol("invalid coordinator address"));
        retire_lookup_conn(hop).await;
        return error;
    };
    if coord_addr == hop.addr() {
        return Ok((hop, versions));
    }
    if let Some(ready) = ready {
        if let Some((conn, _, versions)) = take_ready_handshake(ready, &coord_addr) {
            retire_lookup_conn(hop).await;
            return Ok((conn, versions));
        }
    }
    let bounded = bounded_coord_config(cfg, deadline)?;
    let opened = open_remembered_dial(
        &bounded,
        &coord_addr,
        remembered_dial(&coord_addr, cfg, known, prior),
    )
    .await;
    retire_lookup_conn(hop).await;
    let (conn, _, versions) = opened?;
    Ok((conn, versions))
}

/// Take a bootstrap handshake whose address is already the coordinator.
fn take_ready_handshake(
    ready: &mut [Option<Result<CoordHandshake>>],
    addr: &str,
) -> Option<CoordHandshake> {
    let index = ready.iter().position(|slot| {
        slot.as_ref()
            .and_then(|result| result.as_ref().ok())
            .is_some_and(|(conn, _, _)| conn.addr() == addr)
    })?;
    ready.get_mut(index)?.take()?.ok()
}

/// Open a new TCP socket to a coordinator whose versions were already learned.
///
/// Idle expiry and a dropped connection still need a new socket. The broker
/// address did not change, so ApiVersions would only repeat the previous handshake.
pub(crate) async fn reopen_known_coord(
    cfg: &ConsumerConfig,
    previous: &BrokerConn,
) -> Result<BrokerConn> {
    connect_known_coord(cfg, &KnownCoord::capture(previous)).await
}

/// Versions already learned for one coordinator address.
#[derive(Clone)]
pub(crate) struct KnownCoord {
    addr: String,
    offset_commit_version: i16,
    offset_fetch_version: i16,
    heartbeat_version: i16,
    sync_group_version: i16,
    join_group_version: i16,
    leave_group_version: i16,
    consumer_group_heartbeat_version: i16,
    share_group_heartbeat_version: i16,
    sasl_handshake_version: i16,
    sasl_authenticate_version: i16,
    find_coordinator_version: i16,
    /// ApiVersions for the bootstrap broker, used when this coordinator is elsewhere.
    bootstrap_keys: Vec<ApiVersion>,
    /// ApiVersions already learned for other broker addresses, usually the
    /// other bootstrap servers from startup.
    address_keys: HashMap<String, Vec<ApiVersion>>,
}

impl KnownCoord {
    /// Copy the negotiated versions off `conn`.
    pub(crate) fn capture(conn: &BrokerConn) -> Self {
        Self {
            addr: conn.addr().to_string(),
            offset_commit_version: conn.offset_commit_version,
            offset_fetch_version: conn.offset_fetch_version,
            heartbeat_version: conn.heartbeat_version,
            sync_group_version: conn.sync_group_version,
            join_group_version: conn.join_group_version,
            leave_group_version: conn.leave_group_version,
            consumer_group_heartbeat_version: conn.consumer_group_heartbeat_version,
            share_group_heartbeat_version: conn.share_group_heartbeat_version,
            sasl_handshake_version: conn.sasl_handshake_version,
            sasl_authenticate_version: conn.sasl_authenticate_version,
            find_coordinator_version: conn.find_coordinator_version,
            bootstrap_keys: Vec::new(),
            address_keys: HashMap::new(),
        }
    }

    pub(crate) fn with_bootstrap_keys(mut self, keys: &[ApiVersion]) -> Self {
        self.bootstrap_keys = keys.to_vec();
        self
    }

    pub(crate) fn with_address_keys(mut self, keys: &HashMap<String, Vec<ApiVersion>>) -> Self {
        self.address_keys = keys.clone();
        self
    }

    fn apply(&self, conn: &mut BrokerConn) {
        conn.offset_commit_version = self.offset_commit_version;
        conn.offset_fetch_version = self.offset_fetch_version;
        conn.heartbeat_version = self.heartbeat_version;
        conn.sync_group_version = self.sync_group_version;
        conn.join_group_version = self.join_group_version;
        conn.leave_group_version = self.leave_group_version;
        conn.consumer_group_heartbeat_version = self.consumer_group_heartbeat_version;
        conn.share_group_heartbeat_version = self.share_group_heartbeat_version;
        conn.sasl_handshake_version = self.sasl_handshake_version;
        conn.sasl_authenticate_version = self.sasl_authenticate_version;
        conn.find_coordinator_version = self.find_coordinator_version;
    }
}

/// Dial `known.addr` and stamp the versions already learned there.
pub(crate) async fn connect_known_coord(
    cfg: &ConsumerConfig,
    known: &KnownCoord,
) -> Result<BrokerConn> {
    let opened = async {
        let deadline = Instant::now()
            .checked_add(cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        let bound = crate::net::Deadline::from_std(deadline);
        let mut conn = BrokerConn::connect_tls(
            &known.addr,
            &cfg.client_id,
            cfg.connect_timeout.min(bound.remaining()?),
            cfg.tls.as_ref(),
        )
        .await?;
        known.apply(&mut conn);
        sasl::authenticate(
            &mut conn,
            cfg.sasl_plain.as_ref(),
            cfg.sasl_scram.as_ref(),
            cfg.sasl_scram_sha512.as_ref(),
            cfg.sasl_oauthbearer.as_deref(),
            cfg.sasl_oauthbearer_oidc.as_ref(),
            bound.remaining()?,
        )
        .await?;
        Ok(conn)
    }
    .await;
    if opened.is_err() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
    opened
}

/// Find the coordinator again while the previous address is dialed.
///
/// A coordinator that stays put reuses that handshake. A moved coordinator
/// is dialed after the lookup.
pub(crate) async fn rediscover_known_coord(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    known: &KnownCoord,
    prior: &mut HashMap<String, KnownCoord>,
) -> Result<BrokerConn> {
    let _stored = prior.insert(known.addr.clone(), known.clone());
    if known.find_coordinator_version < 0 {
        // Heap-allocate the full handshake. Inlining it here makes the
        // caller's state machine large enough to overflow the stack.
        return Box::pin(rediscover_coord(cfg, group_id, key_type, &known.addr)).await;
    }
    if cfg.bootstrap.len() != 1 {
        let deadline = Instant::now()
            .checked_add(cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        let deadline = crate::net::Deadline::from_std(deadline);
        let bounded = bounded_coord_config(cfg, deadline)?;
        let (conn, _) = Box::pin(discover_coord_raced(
            &bounded,
            group_id,
            key_type,
            deadline,
            Some(known),
            prior,
        ))
        .await?;
        return Ok(conn);
    }
    let deadline = Instant::now()
        .checked_add(cfg.request_timeout)
        .ok_or(Error::Timeout)?;
    let bounded = bounded_coord_config(cfg, crate::net::Deadline::from_std(deadline))?;
    // The lookup and the previous address are dialed together. The lookup
    // future has to finish, including a broker error, before this returns,
    // and the speculative socket has to be gone by then.
    let discovered = async {
        let speculative = connect_known_coord(&bounded, known);
        let located = locate_known_coordinator(&bounded, group_id, key_type, deadline, known);
        tokio::pin!(speculative);
        tokio::pin!(located);
        let addr = tokio::select! {
            biased;
            addr = &mut located => addr?,
            opened = &mut speculative => {
                let addr = located.await?;
                if addr == known.addr {
                    return opened;
                }
                return open_discovered_coord(&bounded, &addr, known, prior).await;
            }
        };
        if addr == known.addr {
            return speculative.await;
        }
        open_discovered_coord(&bounded, &addr, known, prior).await
    }
    .await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    discovered
}

/// Dial a coordinator found by lookup.
///
/// The bootstrap broker already completed ApiVersions at startup. A
/// coordinator that returns there reuses those versions.
async fn open_discovered_coord(
    cfg: &ConsumerConfig,
    addr: &str,
    known: &KnownCoord,
    prior: &HashMap<String, KnownCoord>,
) -> Result<BrokerConn> {
    if cfg
        .bootstrap
        .first()
        .is_some_and(|bootstrap| bootstrap == addr)
        && !known.bootstrap_keys.is_empty()
    {
        let (conn, _, _) = open_coord_with_keys(cfg, addr, &known.bootstrap_keys).await?;
        return Ok(conn);
    }
    if let Some(keys) = known.address_keys.get(addr).filter(|keys| !keys.is_empty()) {
        let (conn, _, _) = open_coord_with_keys(cfg, addr, keys).await?;
        return Ok(conn);
    }
    if let Some(previous) = prior.get(addr) {
        return connect_known_coord(cfg, previous).await;
    }
    let (conn, _, _) = open_coord_with_find_version(cfg, addr).await?;
    Ok(conn)
}

async fn locate_known_coordinator(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: Instant,
    known: &KnownCoord,
) -> Result<String> {
    let bootstrap = cfg
        .bootstrap
        .first()
        .ok_or_else(|| Error::protocol("find coordinator failed"))?;
    if bootstrap != &known.addr {
        if known.bootstrap_keys.is_empty() {
            return locate_coordinator_address(cfg, group_id, key_type, deadline).await;
        }
        let deadline = crate::net::Deadline::from_std(deadline);
        let bounded = bounded_coord_config(cfg, deadline)?;
        let (mut hop, version, _) =
            open_coord_with_keys(&bounded, bootstrap, &known.bootstrap_keys).await?;
        let address = coordinator_address_of(&mut hop, version, group_id, key_type, deadline).await;
        retire_lookup_conn(hop).await;
        return address;
    }
    let deadline = crate::net::Deadline::from_std(deadline);
    let bounded = bounded_coord_config(cfg, deadline)?;
    let mut hop = connect_known_coord(&bounded, known).await?;
    let version = known.find_coordinator_version;
    let body = hop
        .roundtrip(
            FIND_COORDINATOR,
            version,
            |buf| encode_find_coordinator_request_typed(buf, version, group_id, key_type),
            deadline.remaining()?,
        )
        .await?;
    let (err, _node, host, port) = decode_find_coordinator_response(&mut body.clone(), version)
        .map_err(|e| group_decode_context(e, "FindCoordinator", version, body.len()))?;
    if err != 0 {
        let error = Err(Error::broker(err, "FindCoordinator"));
        retire_lookup_conn(hop).await;
        return error;
    }
    let address = crate::cluster::dial_address(&host, port)
        .ok_or_else(|| Error::protocol("invalid coordinator address"));
    retire_lookup_conn(hop).await;
    address
}

async fn rediscover_coord(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    previous: &str,
) -> Result<BrokerConn> {
    if cfg.bootstrap.len() != 1 {
        return discover_coord(cfg, group_id, key_type).await;
    }
    let deadline = Instant::now()
        .checked_add(cfg.request_timeout)
        .ok_or(Error::Timeout)?;
    let bounded = bounded_coord_config(cfg, crate::net::Deadline::from_std(deadline))?;
    let discovered = async {
        let speculative = open_coord_with_find_version(&bounded, previous);
        let located = locate_coordinator_address(&bounded, group_id, key_type, deadline);
        tokio::pin!(speculative);
        tokio::pin!(located);
        let addr = tokio::select! {
            biased;
            addr = &mut located => addr?,
            opened = &mut speculative => {
                let addr = located.await?;
                if addr == previous {
                    return Ok(opened?.0);
                }
                let next = open_coord_with_find_version(&bounded, &addr).await;
                if let Ok((conn, _, _)) = opened {
                    retire_lookup_conn(conn).await;
                }
                return Ok(next?.0);
            }
        };
        if addr == previous {
            return Ok(speculative.await?.0);
        }
        Ok(open_coord_with_find_version(&bounded, &addr).await?.0)
    }
    .await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    discovered
}

async fn locate_coordinator_address(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: Instant,
) -> Result<String> {
    let deadline = crate::net::Deadline::from_std(deadline);
    let bounded = bounded_coord_config(cfg, deadline)?;
    let Some(addr) = cfg.bootstrap.first() else {
        return Err(Error::protocol("find coordinator failed"));
    };
    let (mut hop, version, _) = open_coord_with_find_version(&bounded, addr).await?;
    let address = coordinator_address_of(&mut hop, version, group_id, key_type, deadline).await;
    retire_lookup_conn(hop).await;
    address
}

async fn coordinator_address_of(
    hop: &mut BrokerConn,
    version: i16,
    group_id: &str,
    key_type: i8,
    deadline: crate::net::Deadline,
) -> Result<String> {
    let body = hop
        .roundtrip(
            FIND_COORDINATOR,
            version,
            |buf| encode_find_coordinator_request_typed(buf, version, group_id, key_type),
            deadline.remaining()?,
        )
        .await?;
    let (err, _node, host, port) = decode_find_coordinator_response(&mut body.clone(), version)
        .map_err(|e| group_decode_context(e, "FindCoordinator", version, body.len()))?;
    if err != 0 {
        return Err(Error::broker(err, "FindCoordinator"));
    }
    crate::cluster::dial_address(&host, port)
        .ok_or_else(|| Error::protocol("invalid coordinator address"))
}

fn remembered_coord_versions(keys: &[ApiVersion]) -> ApiVersionsResponse {
    ApiVersionsResponse {
        error_code: 0,
        api_keys: keys.to_vec(),
        throttle_time_ms: 0,
        supported_features: Vec::new(),
        finalized_features_epoch: None,
        finalized_features: Vec::new(),
        zk_migration_ready: false,
    }
}

fn stamp_coord_versions(conn: &mut BrokerConn, resp: &ApiVersionsResponse) -> Result<i16> {
    let version = resp
        .api_version(FIND_COORDINATOR)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 6))
        .ok_or_else(|| Error::Unsupported("broker does not support FindCoordinator v0-6".into()))?;
    conn.offset_commit_version = resp
        .api_version(OFFSET_COMMIT)
        .and_then(|v| pick_version(v.min_version, v.max_version, 2, 10))
        .unwrap_or(0);
    conn.offset_fetch_version = resp
        .api_version(OFFSET_FETCH)
        .and_then(|v| pick_version(v.min_version, v.max_version, 1, 10))
        .unwrap_or(0);
    conn.heartbeat_version = resp
        .api_version(HEARTBEAT)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 4))
        .unwrap_or(-1);
    conn.sync_group_version = resp
        .api_version(SYNC_GROUP)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 5))
        .unwrap_or(-1);
    conn.join_group_version = resp
        .api_version(JOIN_GROUP)
        .and_then(|v| pick_version(v.min_version, v.max_version, 2, 9))
        .unwrap_or(0);
    conn.leave_group_version = resp
        .api_version(LEAVE_GROUP)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 5))
        .unwrap_or(-1);
    conn.consumer_group_heartbeat_version = resp
        .api_version(CONSUMER_GROUP_HEARTBEAT)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
        .unwrap_or(-1);
    conn.share_group_heartbeat_version = resp
        .api_version(SHARE_GROUP_HEARTBEAT)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
        .unwrap_or(-1);
    conn.find_coordinator_version = version;
    sasl::apply_api_keys(conn, &resp.api_keys);
    Ok(version)
}

async fn open_coord_socket(
    cfg: &ConsumerConfig,
    addr: &str,
) -> Result<(BrokerConn, crate::net::Deadline)> {
    let deadline = Instant::now()
        .checked_add(cfg.request_timeout)
        .ok_or(Error::Timeout)?;
    let bound = crate::net::Deadline::from_std(deadline);
    let conn = BrokerConn::connect_tls(
        addr,
        &cfg.client_id,
        cfg.connect_timeout.min(bound.remaining()?),
        cfg.tls.as_ref(),
    )
    .await?;
    Ok((conn, bound))
}

async fn authenticate_coord(
    conn: &mut BrokerConn,
    cfg: &ConsumerConfig,
    bound: crate::net::Deadline,
) -> Result<()> {
    sasl::authenticate(
        conn,
        cfg.sasl_plain.as_ref(),
        cfg.sasl_scram.as_ref(),
        cfg.sasl_scram_sha512.as_ref(),
        cfg.sasl_oauthbearer.as_deref(),
        cfg.sasl_oauthbearer_oidc.as_ref(),
        bound.remaining()?,
    )
    .await
}

async fn open_coord_with_keys(
    cfg: &ConsumerConfig,
    addr: &str,
    keys: &[ApiVersion],
) -> Result<(BrokerConn, i16, ApiVersionsResponse)> {
    let opened = async {
        let (mut conn, bound) = open_coord_socket(cfg, addr).await?;
        let resp = remembered_coord_versions(keys);
        let version = stamp_coord_versions(&mut conn, &resp)?;
        authenticate_coord(&mut conn, cfg, bound).await?;
        Ok((conn, version, resp))
    }
    .await;
    if opened.is_err() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
    opened
}

async fn open_coord_with_find_version(
    cfg: &ConsumerConfig,
    addr: &str,
) -> Result<(BrokerConn, i16, ApiVersionsResponse)> {
    let opened = async {
        let (mut conn, bound) = open_coord_socket(cfg, addr).await?;
        let resp =
            crate::protocol::api::negotiate_api_versions(&mut conn, bound.remaining()?).await?;
        let version = stamp_coord_versions(&mut conn, &resp)?;
        authenticate_coord(&mut conn, cfg, bound).await?;
        Ok((conn, version, resp))
    }
    .await;
    // A failed handshake drops its socket in this call. Yield so the broker
    // observes that close before the error returns. A successful dial keeps
    // the socket and does not wait.
    if opened.is_err() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
    opened
}

#[expect(
    clippy::too_many_arguments,
    reason = "coordinator RPC carries the learned bootstrap versions"
)]
pub(crate) async fn coord_roundtrip_with_bootstrap(
    coord: &mut BrokerConn,
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    api_key: i16,
    mut encode_body: impl FnMut(i16, &mut BytesMut) -> Result<()>,
    bootstrap_keys: &[ApiVersion],
    address_keys: &HashMap<String, Vec<ApiVersion>>,
    prior: &mut HashMap<String, KnownCoord>,
    request_timeout: Duration,
) -> Result<(i16, Bytes)> {
    let deadline = crate::net::Deadline::from_std(
        Instant::now()
            .checked_add(request_timeout)
            .ok_or(Error::Timeout)?,
    );
    if api_key == OFFSET_COMMIT || api_key == OFFSET_FETCH {
        let version = negotiated_coord_version(coord, api_key)?;
        let body = offsets::coord_offsets_legacy(
            coord,
            cfg,
            group_id,
            key_type,
            api_key,
            version,
            |buf| encode_body(version, buf),
            bootstrap_keys,
            address_keys,
            prior,
            deadline.remaining()?,
        )
        .await?;
        return Ok((version, body));
    }
    if coord.idle_expired(cfg.connections_max_idle) {
        let bounded = bounded_coord_config(cfg, deadline)?;
        let replacement = reopen_known_coord(&bounded, coord).await?;
        let previous = std::mem::replace(coord, replacement);
        retire_lookup_conn(previous).await;
    }
    let version = negotiated_coord_version(coord, api_key)?;
    let body = match coord
        .roundtrip_deadline(api_key, version, |buf| encode_body(version, buf), deadline)
        .await
    {
        Ok(body) => body,
        Err(e) if e.is_retriable() => {
            let bounded = bounded_coord_config(cfg, deadline)?;
            let replacement = reopen_known_coord(&bounded, coord).await?;
            let previous = std::mem::replace(coord, replacement);
            retire_lookup_conn(previous).await;
            let version = negotiated_coord_version(coord, api_key)?;
            let body = coord
                .roundtrip_deadline(api_key, version, |buf| encode_body(version, buf), deadline)
                .await?;
            return finish_coord_roundtrip(
                coord,
                cfg,
                group_id,
                key_type,
                api_key,
                version,
                body,
                &mut encode_body,
                bootstrap_keys,
                address_keys,
                prior,
                deadline,
            )
            .await;
        }
        Err(e) => return Err(e),
    };
    finish_coord_roundtrip(
        coord,
        cfg,
        group_id,
        key_type,
        api_key,
        version,
        body,
        &mut encode_body,
        bootstrap_keys,
        address_keys,
        prior,
        deadline,
    )
    .await
}

fn bounded_coord_config(
    cfg: &ConsumerConfig,
    deadline: crate::net::Deadline,
) -> Result<ConsumerConfig> {
    let remaining = deadline.remaining()?;
    let mut bounded = cfg.clone();
    bounded.request_timeout = remaining;
    bounded.connect_timeout = cfg.connect_timeout.min(remaining);
    Ok(bounded)
}

#[expect(
    clippy::too_many_arguments,
    reason = "coordinator rediscovery carries startup addresses and earlier visits"
)]
async fn finish_coord_roundtrip(
    coord: &mut BrokerConn,
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    api_key: i16,
    version: i16,
    body: Bytes,
    encode_body: &mut impl FnMut(i16, &mut BytesMut) -> Result<()>,
    bootstrap_keys: &[ApiVersion],
    address_keys: &HashMap<String, Vec<ApiVersion>>,
    prior: &mut HashMap<String, KnownCoord>,
    deadline: crate::net::Deadline,
) -> Result<(i16, Bytes)> {
    let coordinator = if api_key == OFFSET_FETCH {
        offset_fetch_error(version, &body).await
    } else if api_key == OFFSET_COMMIT {
        offset_commit_error(version, &body).await
    } else {
        coordinator_error(api_key, version, &body)
    };
    if coordinator.is_some_and(error::coordinator_retriable) {
        let bounded = bounded_coord_config(cfg, deadline)?;
        let known = KnownCoord::capture(coord)
            .with_bootstrap_keys(bootstrap_keys)
            .with_address_keys(address_keys);
        let replacement = Box::pin(rediscover_known_coord(
            &bounded, group_id, key_type, &known, prior,
        ))
        .await?;
        let previous = std::mem::replace(coord, replacement);
        retire_lookup_conn(previous).await;
        let version = negotiated_coord_version(coord, api_key)?;
        let body = coord
            .roundtrip_deadline(api_key, version, |buf| encode_body(version, buf), deadline)
            .await?;
        Ok((version, body))
    } else {
        Ok((version, body))
    }
}

pub(crate) fn negotiated_coord_version(coord: &BrokerConn, api_key: i16) -> Result<i16> {
    match api_key {
        OFFSET_FETCH => spoken_offset_fetch(coord.offset_fetch_version),
        OFFSET_COMMIT => spoken_offset_commit(coord.offset_commit_version),
        JOIN_GROUP => spoken_join_group(coord.join_group_version),
        SYNC_GROUP => spoken_sync_group(coord.sync_group_version),
        HEARTBEAT => spoken_heartbeat(coord.heartbeat_version),
        LEAVE_GROUP => spoken_leave_group(coord.leave_group_version),
        CONSUMER_GROUP_HEARTBEAT => {
            spoken_consumer_group_heartbeat(coord.consumer_group_heartbeat_version)
        }
        SHARE_GROUP_HEARTBEAT => {
            if (0..=1).contains(&coord.share_group_heartbeat_version) {
                Ok(coord.share_group_heartbeat_version)
            } else {
                Err(Error::Unsupported(
                    "broker does not support ShareGroupHeartbeat v0-1".into(),
                ))
            }
        }
        _ => Err(Error::protocol(format!(
            "coordinator api {api_key} has no negotiated version"
        ))),
    }
}

/// OffsetCommit / OffsetFetch put coordinator errors on each partition (and
/// OffsetFetch also at the tail). Bytes 4–5 are the topic-array length, so a
/// throttle-then-i16 peek misses 14/15/16 and treats a recoverable code as fatal.
async fn offset_commit_error(version: i16, body: &[u8]) -> Option<i16> {
    match decode_offset_commit_response_cooperative(&mut { body }, version).await {
        Ok(0) => None,
        Ok(code) => Some(code),
        Err(_) => None,
    }
}

async fn offset_fetch_error(version: i16, body: &[u8]) -> Option<i16> {
    match decode_offset_fetch_response_cooperative(&mut { body }, version).await {
        Err(Error::Broker { code, .. }) => Some(code),
        Ok(topics) => topics
            .iter()
            .flat_map(|topic| topic.partitions.iter())
            .map(|partition| partition.error_code)
            .find(|code| *code != 0),
        Err(_) => None,
    }
}

fn coordinator_error(api_key: i16, api_version: i16, body: &[u8]) -> Option<i16> {
    match api_key {
        OFFSET_COMMIT => match decode_offset_commit_response(&mut { body }, api_version) {
            Ok(0) => None,
            Ok(code) => Some(code),
            Err(_) => None,
        },
        OFFSET_FETCH => match decode_offset_fetch_response(&mut { body }, api_version) {
            Err(Error::Broker { code, .. }) => Some(code),
            Ok(topics) => topics
                .iter()
                .flat_map(|t| t.partitions.iter())
                .map(|p| p.error_code)
                .find(|code| *code != 0),
            Err(_) => None,
        },
        _ => peek_error_code(body),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::{OffsetAndMetadata, TopicPartition};
    use crate::net::BrokerConn;
    use crate::protocol::group::{ConsumerProtocolSubscription, FetchedOffset, JoinGroupRequest};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test(flavor = "current_thread")]
    async fn remembering_many_assignment_leaders_does_not_hold_the_caller() {
        let parts: Vec<(&str, i32)> = (0..2_000_000).map(|index| ("orders", index)).collect();
        let cluster = crate::cluster::Cluster::default();
        let mut leaders = Vec::new();
        let mut seen = HashSet::new();
        let started = std::time::Instant::now();
        tokio::select! {
            () = remember_partition_leaders(&cluster, &parts, &mut leaders, &mut seen) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "remembering assignment leaders held the caller for {:?}",
            started.elapsed()
        );
        let mut known = crate::cluster::Cluster::default();
        let mut image = crate::protocol::api::MetadataResponse::blank();
        image.brokers = vec![
            crate::protocol::api::Broker::new(4, "127.0.0.1", 9092, None),
            crate::protocol::api::Broker::new(5, "10.0.0.8", 9093, None),
        ];
        image.topics = vec![
            crate::protocol::api::TopicMetadata::new(
                0,
                "orders",
                false,
                vec![crate::protocol::api::PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 4,
                    leader_epoch: 1,
                    replica_nodes: Vec::new(),
                    isr_nodes: Vec::new(),
                    offline_replicas: Vec::new(),
                }],
            ),
            crate::protocol::api::TopicMetadata::new(
                0,
                "payments",
                false,
                vec![crate::protocol::api::PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 5,
                    leader_epoch: 1,
                    replica_nodes: Vec::new(),
                    isr_nodes: Vec::new(),
                    offline_replicas: Vec::new(),
                }],
            ),
        ];
        known.apply_image(&image, 1).await;
        let sample = [
            ("orders", 0),
            ("orders", 0),
            ("payments", 0),
            ("missing", 0),
        ];
        let mut found = Vec::new();
        let mut found_seen = HashSet::new();
        remember_partition_leaders(&known, &sample, &mut found, &mut found_seen).await;
        assert_eq!(found, vec![4, 5]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn decoding_many_join_members_does_not_hold_the_caller() {
        let members: Vec<crate::protocol::group::JoinMember> = (0..200_000)
            .map(|index| crate::protocol::group::JoinMember {
                member_id: format!("member-{index}"),
                group_instance_id: Some(format!("instance-{index}")),
                metadata: b"sub".to_vec(),
            })
            .collect();
        let mut body = bytes::BytesMut::new();
        crate::protocol::group::encode_join_group_response(
            &mut body, 6, 0, 1, "range", "member-0", "member-0", &members,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let started = std::time::Instant::now();
        tokio::select! {
            decoded = decode_join_group_response_cooperative(&mut body, 6) => {
                let decoded = decoded.unwrap_or_else(|error| panic!("{error}"));
                assert_eq!(decoded.0, 0);
                assert_eq!(decoded.6.len(), 200_000);
                assert_eq!(decoded.6.first().map(|item| item.member_id.as_str()), Some("member-0"));
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "decoding join members held the caller for {elapsed:?}"
        );
        let sample = [crate::protocol::group::JoinMember {
            member_id: "member-a".to_string(),
            group_instance_id: Some("instance-a".to_string()),
            metadata: b"sub".to_vec(),
        }];
        let mut encoded = bytes::BytesMut::new();
        crate::protocol::group::encode_join_group_response(
            &mut encoded,
            6,
            7,
            3,
            "range",
            "leader",
            "member-a",
            &sample,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let mut once = encoded.clone();
        let mut paced = encoded;
        let sync = crate::protocol::group::decode_join_group_response(&mut once, 6)
            .unwrap_or_else(|error| panic!("{error}"));
        let coop = decode_join_group_response_cooperative(&mut paced, 6)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(sync.0, coop.0);
        assert_eq!(sync.1, coop.1);
        assert_eq!(sync.2, coop.2);
        assert_eq!(sync.3, coop.3);
        assert_eq!(sync.4, coop.4);
        assert_eq!(sync.5, coop.5);
        assert_eq!(sync.7, coop.7);
        assert_eq!(sync.8, coop.8);
        assert_eq!(sync.6.len(), coop.6.len());
        assert_eq!(
            coop.6.first().map(|item| item.member_id.as_str()),
            Some("member-a")
        );
        assert_eq!(
            coop.6
                .first()
                .and_then(|item| item.group_instance_id.as_deref()),
            Some("instance-a")
        );
        assert_eq!(
            coop.6.first().map(|item| item.metadata.as_slice()),
            Some(&b"sub"[..])
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn decoding_many_heartbeat_assignments_does_not_hold_the_caller() {
        let assignment: Vec<crate::protocol::cgheartbeat::TopicPartitions> = (0..1_000_000)
            .map(|index: usize| {
                let mut topic_id = [0u8; 16];
                topic_id[..8].copy_from_slice(&index.to_le_bytes());
                crate::protocol::cgheartbeat::TopicPartitions {
                    topic_id,
                    partitions: vec![index as i32],
                }
            })
            .collect();
        let resp = crate::protocol::cgheartbeat::ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("member-0".to_string()),
            member_epoch: 1,
            heartbeat_interval_ms: 1000,
            assignment: Some(assignment),
        };
        let mut body = bytes::BytesMut::new();
        crate::protocol::cgheartbeat::encode_consumer_group_heartbeat_response(&mut body, 1, &resp)
            .unwrap_or_else(|error| panic!("{error}"));
        let started = std::time::Instant::now();
        tokio::select! {
            decoded = decode_consumer_group_heartbeat_response_cooperative(&mut body, 1) => {
                let decoded = decoded.unwrap_or_else(|error| panic!("{error}"));
                assert_eq!(decoded.throttle_time_ms, 0);
                assert_eq!(decoded.assignment.as_ref().map(Vec::len), Some(1_000_000));
                assert_eq!(
                    decoded.assignment.as_ref().and_then(|items| items.first()).and_then(|item| item.partitions.first()).copied(),
                    Some(0)
                );
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "decoding heartbeat assignments held the caller for {elapsed:?}"
        );
        let sample = crate::protocol::cgheartbeat::ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 4,
            error_code: 7,
            error_message: Some("busy".to_string()),
            member_id: Some("member-a".to_string()),
            member_epoch: 2,
            heartbeat_interval_ms: 500,
            assignment: Some(vec![crate::protocol::cgheartbeat::TopicPartitions {
                topic_id: [0x11; 16],
                partitions: vec![9],
            }]),
        };
        let mut encoded = bytes::BytesMut::new();
        crate::protocol::cgheartbeat::encode_consumer_group_heartbeat_response(
            &mut encoded,
            1,
            &sample,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let mut once = encoded.clone();
        let mut paced = encoded;
        let sync =
            crate::protocol::cgheartbeat::decode_consumer_group_heartbeat_response(&mut once, 1)
                .unwrap_or_else(|error| panic!("{error}"));
        let coop = decode_consumer_group_heartbeat_response_cooperative(&mut paced, 1)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(sync, coop);
        assert_eq!(coop.error_message.as_deref(), Some("busy"));
        assert_eq!(
            coop.assignment
                .as_ref()
                .and_then(|items| items.first())
                .map(|item| item.topic_id),
            Some([0x11; 16])
        );
    }

    fn offset_map(count: i32) -> HashMap<String, HashMap<i32, OffsetAndMetadata>> {
        let mut parts = HashMap::new();
        for index in 0..count {
            let _ = parts.insert(index, OffsetAndMetadata::new(i64::from(index)));
        }
        let mut map = HashMap::new();
        let _ = map.insert("orders".to_string(), parts);
        map
    }

    fn fetched_orders(count: i32) -> crate::protocol::group::FetchedOffsetTopic {
        use crate::protocol::group::{FetchedOffset, FetchedOffsetTopic};
        FetchedOffsetTopic {
            topic: "orders".into(),
            partitions: (0..count)
                .map(|index| FetchedOffset {
                    partition: index,
                    offset: i64::from(index),
                    leader_epoch: -1,
                    metadata: String::new(),
                    error_code: 0,
                })
                .collect(),
        }
    }

    fn commit_items(count: i32) -> Vec<(TopicPartition, OffsetAndMetadata)> {
        (0..count)
            .map(|index| {
                (
                    TopicPartition::new("orders", index),
                    OffsetAndMetadata::new(i64::from(index)),
                )
            })
            .collect()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_many_fetched_offsets_does_not_hold_the_caller() {
        // 20_000 repeated scans held the caller 497ms. Indexing 500_000
        // offsets still has to pause.
        use crate::protocol::group::{FetchedOffset, FetchedOffsetTopic};
        let fetched = [FetchedOffsetTopic {
            topic: "orders".into(),
            partitions: (0..500_000)
                .map(|index| FetchedOffset {
                    partition: index,
                    offset: i64::from(index),
                    leader_epoch: -1,
                    metadata: String::new(),
                    error_code: 0,
                })
                .collect(),
        }];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = index_fetched_offsets_cooperative(&fetched) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "indexing fetched offsets held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexed_fetched_offsets_match_one_shot() {
        use crate::protocol::group::{FetchedOffset, FetchedOffsetTopic};
        let fetched = [FetchedOffsetTopic {
            topic: "orders".into(),
            partitions: vec![
                FetchedOffset {
                    partition: 0,
                    offset: 4,
                    leader_epoch: 1,
                    metadata: String::new(),
                    error_code: 0,
                },
                FetchedOffset {
                    partition: 0,
                    offset: 9,
                    leader_epoch: 2,
                    metadata: String::new(),
                    error_code: 0,
                },
            ],
        }];
        let index = index_fetched_offsets_cooperative(&fetched).await;
        assert_eq!(
            index.get(&("orders", 0)).copied().map(|part| part.offset),
            fetched_committed(&fetched, "orders", 0).map(|part| part.offset)
        );
        assert!(index.get(&("orders", 3)).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn grouping_many_borrowed_offset_fetches_does_not_hold_the_caller() {
        // 20_000 partitions: scanning the growing list held the caller 725ms.
        // A longer walk still has to pause after membership is a set.
        let wanted: Vec<TopicPartition> = (0..500_000)
            .map(|index| TopicPartition::new("orders", index))
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = group_borrowed_offset_fetch_topics_cooperative(&wanted) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "grouping borrowed offset fetches held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_borrowed_offset_fetch_matches_one_shot() {
        let wanted = vec![
            TopicPartition::new("orders", 0),
            TopicPartition::new("orders", 0),
            TopicPartition::new("orders", 1),
            TopicPartition::new("payments", 4),
        ];
        let one = group_borrowed_offset_fetch_topics(
            wanted.iter().map(|tp| (tp.topic.as_str(), tp.partition)),
        );
        let two = group_borrowed_offset_fetch_topics_cooperative(&wanted).await;
        assert_eq!(one.len(), two.len());
        assert_eq!(one[0].1, two[0].1);
        assert_eq!(one[1].0, two[1].0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refilling_many_shared_offsets_does_not_hold_the_caller() {
        // 1_500_000 partitions: placing them held the caller 130ms.
        let topic: Arc<str> = Arc::from("orders");
        let items: Vec<(Arc<str>, i32, i64)> = (0..1_500_000)
            .map(|index| (Arc::clone(&topic), index, 1))
            .collect();
        let mut topics = Vec::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = refill_shared_offset_topics_cooperative(&mut topics, &items, |_, _| 0) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "refilling shared offsets held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_shared_offset_refill_matches_one_shot() {
        let topic: Arc<str> = Arc::from("orders");
        let items = vec![
            (Arc::clone(&topic), 1, 4),
            (Arc::clone(&topic), 0, 2),
            (Arc::from("payments"), 3, 9),
        ];
        let mut one = Vec::new();
        refill_shared_offset_topics(&mut one, &items, |_, _| 7);
        let mut two = Vec::new();
        refill_shared_offset_topics_cooperative(&mut two, &items, |_, _| 7).await;
        assert_eq!(one.len(), two.len());
        assert_eq!(one[0].partitions.len(), two[0].partitions.len());
        assert_eq!(one[0].partitions[0].offset, two[0].partitions[0].offset);
        assert_eq!(one[0].partitions[0].leader_epoch, 7);
        assert_eq!(one[1].topic, two[1].topic);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refilling_many_offset_topics_does_not_hold_the_caller() {
        // 8_192 topics: scanning earlier names held the caller 407ms.
        let items: Vec<(TopicPartition, OffsetAndMetadata)> = (0..8_192)
            .map(|index| {
                (
                    TopicPartition::new(format!("t{index}"), 0),
                    OffsetAndMetadata::new(1),
                )
            })
            .collect();
        let mut topics = Vec::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = refill_offset_topics_cooperative(&mut topics, &items) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert_eq!(topics.len(), 8_192);
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "refilling offset topics held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refilling_many_offset_commit_partitions_does_not_hold_the_caller() {
        // 20_000 partitions: scanning the growing list held the caller 559ms.
        // A longer walk still has to pause after membership is a set.
        let items = commit_items(500_000);
        let mut topics = Vec::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = refill_offset_topics_cooperative(&mut topics, &items) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "refilling offset-commit partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_offset_commit_refill_matches_one_shot() {
        let items = vec![
            (TopicPartition::new("orders", 0), OffsetAndMetadata::new(1)),
            (TopicPartition::new("orders", 0), OffsetAndMetadata::new(8)),
            (TopicPartition::new("orders", 1), OffsetAndMetadata::new(2)),
            (
                TopicPartition::new("payments", 4),
                OffsetAndMetadata::new(3),
            ),
        ];
        let one = group_offset_topics(&items);
        let two = group_offset_topics_cooperative(&items).await;
        assert_eq!(one.len(), two.len());
        assert_eq!(one[0].partitions.len(), 2);
        assert_eq!(one[0].partitions[0].offset, two[0].partitions[0].offset);
        assert_eq!(
            one[0].partitions[1].partition,
            two[0].partitions[1].partition
        );
        assert_eq!(one[1].topic, two[1].topic);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refilling_many_offset_fetch_partitions_does_not_hold_the_caller() {
        // 20_000 partitions: scanning the growing list held the caller 691ms.
        // 1_500_000 hash inserts of the same shape held it for 777ms, so the
        // long walk also pauses.
        let wanted: Vec<(String, i32)> = (0..1_500_000)
            .map(|index| ("orders".into(), index))
            .collect();
        let mut topics = Vec::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = refill_offset_fetch_topics_cooperative(&mut topics, &wanted) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "refilling offset-fetch partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_offset_fetch_refill_matches_one_shot() {
        let wanted = vec![
            ("orders".to_string(), 0),
            ("orders".to_string(), 0),
            ("orders".to_string(), 1),
            ("payments".to_string(), 4),
        ];
        let mut one = Vec::new();
        refill_offset_fetch_topics(
            &mut one,
            wanted
                .iter()
                .map(|(topic, partition)| (topic.as_str(), *partition)),
        );
        let mut two = Vec::new();
        refill_offset_fetch_topics_cooperative(&mut two, &wanted).await;
        assert_eq!(one.len(), two.len());
        assert_eq!(one[0].topic, two[0].topic);
        assert_eq!(one[0].partitions, two[0].partitions);
        assert_eq!(one[1].partitions, two[1].partitions);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn building_a_large_offset_map_does_not_hold_the_caller() {
        // 1_500_000 offsets: inserting them held the caller 792ms.
        let fetched = [fetched_orders(1_500_000)];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = committed_offset_map_cooperative(&fetched) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "building a committed offset map held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn building_many_offset_topics_does_not_hold_the_caller() {
        use crate::protocol::group::{FetchedOffset, FetchedOffsetTopic};
        let fetched: Vec<FetchedOffsetTopic> = (0..2_000_000)
            .map(|index| FetchedOffsetTopic {
                topic: format!("topic-{index}"),
                partitions: vec![FetchedOffset {
                    partition: 0,
                    offset: 1,
                    leader_epoch: -1,
                    metadata: String::new(),
                    error_code: 0,
                }],
            })
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = committed_offset_map_cooperative(&fetched) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "building committed offsets for many topics held the caller for {:?}",
            started.elapsed()
        );
        let sample = [
            FetchedOffsetTopic {
                topic: "orders".into(),
                partitions: vec![FetchedOffset {
                    partition: 0,
                    offset: 4,
                    leader_epoch: -1,
                    metadata: String::new(),
                    error_code: 0,
                }],
            },
            FetchedOffsetTopic {
                topic: "orders".into(),
                partitions: vec![FetchedOffset {
                    partition: 1,
                    offset: 8,
                    leader_epoch: -1,
                    metadata: String::new(),
                    error_code: 0,
                }],
            },
            FetchedOffsetTopic {
                topic: "payments".into(),
                partitions: vec![FetchedOffset {
                    partition: 2,
                    offset: 3,
                    leader_epoch: -1,
                    metadata: String::new(),
                    error_code: 0,
                }],
            },
        ];
        let map = committed_offset_map_cooperative(&sample).await.unwrap();
        assert_eq!(
            map.get("orders")
                .and_then(|parts| parts.get(&0))
                .map(|md| md.offset),
            Some(4)
        );
        assert_eq!(
            map.get("orders")
                .and_then(|parts| parts.get(&1))
                .map(|md| md.offset),
            Some(8)
        );
        assert_eq!(
            map.get("payments")
                .and_then(|parts| parts.get(&2))
                .map(|md| md.offset),
            Some(3)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_many_wanted_partitions_does_not_hold_the_caller() {
        // 1_500_000 partitions: inserting them held the caller 748ms.
        let wanted: Vec<(String, i32)> = (0..1_500_000)
            .map(|index| ("orders".to_string(), index))
            .collect();
        let mut next = HashMap::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = remember_wanted_partitions(&mut next, &wanted) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "indexing wanted partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_offset_map_matches_one_shot() {
        let fetched = [fetched_orders(3)];
        let one = committed_offset_map(&fetched).unwrap();
        let two = committed_offset_map_cooperative(&fetched).await.unwrap();
        assert_eq!(
            one.get("orders")
                .and_then(|parts| parts.get(&1))
                .map(|md| md.offset),
            Some(1)
        );
        assert_eq!(
            two.get("orders")
                .and_then(|parts| parts.get(&2))
                .map(|md| md.offset),
            Some(2)
        );
        assert_eq!(
            one.get("orders").map(HashMap::len),
            two.get("orders").map(HashMap::len)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expanding_many_assigned_partitions_does_not_hold_the_caller() {
        // 1_500_000 partitions: cloning the topic name held the caller 115ms.
        let topics = vec![("orders".to_string(), (0..1_500_000).collect::<Vec<i32>>())];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = expand_topic_partitions_cooperative(topics) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "expanding assigned partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pairing_many_committed_offsets_does_not_hold_the_caller() {
        // 1_500_000 offsets: cloning the topic name held the caller 241ms.
        let map = offset_map(1_500_000);
        let started = std::time::Instant::now();
        tokio::select! {
            _ = committed_offset_pairs_cooperative(map) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "pairing committed offsets held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_partition_expansion_matches_one_shot() {
        let topics = vec![
            ("orders".to_string(), vec![1, 0, 2]),
            ("payments".to_string(), vec![4]),
        ];
        let one = expand_topic_partitions(topics.clone());
        let two = expand_topic_partitions_cooperative(topics).await;
        assert_eq!(one, two);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_committed_pairs_match_one_shot() {
        let one = committed_offset_pairs(offset_map(3));
        let two = committed_offset_pairs_cooperative(offset_map(3)).await;
        let mut one_keys: Vec<_> = one
            .iter()
            .map(|(tp, md)| (tp.topic.clone(), tp.partition, md.offset))
            .collect();
        let mut two_keys: Vec<_> = two
            .iter()
            .map(|(tp, md)| (tp.topic.clone(), tp.partition, md.offset))
            .collect();
        one_keys.sort();
        two_keys.sort();
        assert_eq!(one_keys, two_keys);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn assigning_many_cooperative_sticky_partitions_does_not_hold_the_caller() {
        let parts: Vec<i32> = (0..1_000_000).collect();
        let subs = vec![("member".to_string(), vec!["orders".to_string()])];
        let topics = vec![("orders".to_string(), parts)];
        let prev = HashMap::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assign_cooperative_sticky_subscribed_cooperative(&subs, &topics, &prev) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "assigning cooperative-sticky partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn assigning_many_range_partitions_does_not_hold_the_caller() {
        let parts: Vec<i32> = (0..1_000_000).collect();
        let subs = vec![("member".to_string(), vec!["orders".to_string()])];
        let topics = vec![("orders".to_string(), parts)];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assign_range_subscribed_cooperative(&subs, &topics) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "assigning range partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rebalancing_many_sticky_members_does_not_hold_the_caller() {
        // 2_048 members and 8_192 partitions already owned by one member:
        // scanning every member for each move held the caller 1.66s.
        let members: Vec<String> = (0..2_048).map(|index| format!("m{index:04}")).collect();
        let member_refs: Vec<&str> = members.iter().map(String::as_str).collect();
        let partitions: Vec<i32> = (0..8_192).collect();
        let prev = [("m0000", partitions.as_slice())];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assign_sticky_from_cooperative(&member_refs, &partitions, prev) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "rebalancing sticky members held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn balancing_many_sticky_members_does_not_hold_the_caller() {
        // 2_048 members and 8_192 partitions: scanning every member for each
        // partition held the caller 401ms.
        let members: Vec<String> = (0..2_048).map(|index| format!("m{index:04}")).collect();
        let member_refs: Vec<&str> = members.iter().map(String::as_str).collect();
        let partitions: Vec<i32> = (0..8_192).collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assign_sticky_from_cooperative(&member_refs, &partitions, std::iter::empty()) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "balancing sticky members held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn assigning_many_sticky_partitions_does_not_hold_the_caller() {
        let parts: Vec<i32> = (0..1_000_000).collect();
        let subs = vec![("member".to_string(), vec!["orders".to_string()])];
        let topics = vec![("orders".to_string(), parts)];
        let prev = HashMap::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assign_sticky_subscribed_cooperative(&subs, &topics, &prev) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "assigning sticky partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn revoking_many_owned_partitions_does_not_hold_the_caller() {
        // 8_192 partitions: scanning one member's claims for each revoke held the caller 685ms.
        let owned: Vec<(String, i32)> = (0..8_192)
            .map(|index| ("orders".to_string(), index))
            .collect();
        let claims = vec![
            ("a".to_string(), owned.clone(), 1),
            ("b".to_string(), owned, 1),
        ];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = resolve_sticky_owned_partitions_cooperative(&claims) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "revoking owned partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolving_many_owned_partitions_does_not_hold_the_caller() {
        let mut owned = Vec::with_capacity(1_000_000);
        for index in 0..1_000_000 {
            owned.push(("orders".to_string(), index));
        }
        let claims = vec![("member".to_string(), owned, 1)];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = resolve_sticky_owned_partitions_cooperative(&claims) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "resolving owned partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_raced_group_lookup_lets_the_other_broker_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_addr = first.local_addr().unwrap().to_string();
        let second_addr = second.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let first_flag = Arc::clone(&open);
        let second_flag = Arc::clone(&open);
        let mut find_body = bytes::BytesMut::new();
        crate::protocol::group::encode_find_coordinator_response(
            &mut find_body,
            1,
            1,
            "127.0.0.1",
            1,
            "group",
        )
        .unwrap();
        find_body[4..6].copy_from_slice(&42i16.to_be_bytes());
        let find_body = find_body.freeze();
        let first_peer = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let Ok((mut socket, _)) = first.accept().await else {
                return;
            };
            let _started = first_flag.fetch_add(1, Ordering::SeqCst);
            let mut size_buf = [0u8; 4];
            if socket.read_exact(&mut size_buf).await.is_ok() {
                let size = i32::from_be_bytes(size_buf).max(0) as usize;
                let mut request = vec![0u8; size];
                if socket.read_exact(&mut request).await.is_ok() {
                    if let Some(bytes) = request.get(4..8) {
                        let correlation = i32::from_be_bytes([
                            bytes.first().copied().unwrap_or(0),
                            bytes.get(1).copied().unwrap_or(0),
                            bytes.get(2).copied().unwrap_or(0),
                            bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut response = Vec::new();
                        let len = i32::try_from(4 + find_body.len()).unwrap_or(0);
                        response.extend_from_slice(&len.to_be_bytes());
                        response.extend_from_slice(&correlation.to_be_bytes());
                        response.extend_from_slice(&find_body);
                        let _wrote = socket.write_all(&response).await;
                    }
                }
            }
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = first_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let second_peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = second.accept().await else {
                return;
            };
            let _started = second_flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = second_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let cfg =
            crate::consumer::ConsumerConfig::bootstrap([first_addr.clone(), second_addr.clone()]);
        let keys = vec![ApiVersion {
            api_key: FIND_COORDINATOR,
            min_version: 1,
            max_version: 1,
        }];
        let mut address_keys = HashMap::new();
        let _stored = address_keys.insert(first_addr.clone(), keys.clone());
        let _stored = address_keys.insert(second_addr, keys.clone());
        let known = KnownCoord {
            addr: first_addr,
            offset_commit_version: 0,
            offset_fetch_version: 0,
            heartbeat_version: -1,
            sync_group_version: -1,
            join_group_version: 0,
            leave_group_version: -1,
            consumer_group_heartbeat_version: -1,
            share_group_heartbeat_version: -1,
            sasl_handshake_version: -1,
            sasl_authenticate_version: -1,
            find_coordinator_version: 1,
            bootstrap_keys: keys,
            address_keys,
        };
        let _found = discover_coord_raced(
            &cfg,
            "group",
            COORDINATOR_GROUP,
            crate::net::Deadline::from_timeout(Duration::from_secs(2)),
            Some(&known),
            &HashMap::new(),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a raced group lookup left a broker blocked in a read"
        );
        first_peer.abort();
        second_peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_bad_coordinator_reply_lets_the_broker_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut size_buf = [0u8; 4];
            if socket.read_exact(&mut size_buf).await.is_ok() {
                let size = i32::from_be_bytes(size_buf).max(0) as usize;
                let mut request = vec![0u8; size];
                if socket.read_exact(&mut request).await.is_ok() {
                    if let Some(bytes) = request.get(4..8) {
                        let correlation = i32::from_be_bytes([
                            bytes.first().copied().unwrap_or(0),
                            bytes.get(1).copied().unwrap_or(0),
                            bytes.get(2).copied().unwrap_or(0),
                            bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut response = Vec::with_capacity(8);
                        response.extend_from_slice(&4i32.to_be_bytes());
                        response.extend_from_slice(&correlation.to_be_bytes());
                        let _wrote = socket.write_all(&response).await;
                    }
                }
            }
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "bad-coord", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let cfg = crate::consumer::ConsumerConfig::bootstrap([addr]);
        let versions = crate::protocol::api::ApiVersionsResponse {
            error_code: 0,
            api_keys: Vec::new(),
            throttle_time_ms: 0,
            supported_features: Vec::new(),
            finalized_features_epoch: None,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
        };
        let _found = find_coordinator_on(
            &cfg,
            conn,
            1,
            versions,
            "group",
            COORDINATOR_GROUP,
            crate::net::Deadline::from_timeout(Duration::from_secs(2)),
            None,
            None,
            &HashMap::new(),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a bad coordinator reply left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacing_a_lookup_socket_lets_the_broker_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "lookup-replace", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        retire_lookup_conn(conn).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "replacing a lookup socket left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retiring_a_coordinator_lets_the_broker_leave() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "group-coord", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut held = Some(conn);
        retire_group_conn(&mut held).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "retiring a coordinator left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_bytes_do_not_block_the_heartbeat_interval() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (_reader, mut writer) = socket.into_split();
            writer.write_all(&[0x01]).await.unwrap();
            // Stay open so the client is not simply observing a close.
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let conn = BrokerConn::connect(&addr.to_string(), "queued-hb", Duration::from_secs(2))
            .await
            .unwrap();
        let mut conn = Some(conn);
        let queued = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if conn.as_ref().is_some_and(BrokerConn::has_queued_response) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            queued.is_ok(),
            "queued bytes never became visible to the heartbeat"
        );
        {
            let mut waiting = std::pin::pin!(coordinator_lost(&mut conn));
            tokio::select! {
                _ = &mut waiting => panic!("queued bytes ended the heartbeat wait"),
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
        assert!(
            conn.is_some(),
            "queued bytes dropped the coordinator socket"
        );
        drop(conn);
        drop(accepted);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_after_queued_bytes_shuts_the_heartbeat_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = socket.into_split();
            writer.write_all(&[0x01]).await.unwrap();
            gate_rx.await.unwrap();
            writer.shutdown().await.unwrap();
            let mut buf = [0u8; 8];
            tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await
        });
        let conn = BrokerConn::connect(&addr.to_string(), "queued-close", Duration::from_secs(2))
            .await
            .unwrap();
        let mut conn = Some(conn);
        let queued = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if conn.as_ref().is_some_and(BrokerConn::has_queued_response) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            queued.is_ok(),
            "queued bytes never became visible to the heartbeat"
        );
        {
            let mut waiting = std::pin::pin!(coordinator_lost(&mut conn));
            tokio::select! {
                _ = &mut waiting => panic!("closed before the peer shut its write half"),
                _ = tokio::time::sleep(Duration::from_millis(30)) => {}
            }
            gate_tx.send(()).unwrap();
            let noticed = tokio::time::timeout(Duration::from_millis(300), waiting).await;
            assert!(
                noticed.is_ok(),
                "a close after queued bytes was not noticed before the heartbeat interval"
            );
        }
        let saw = accepted.await.unwrap();
        assert!(
            matches!(saw, Ok(Ok(0))),
            "peer stayed in the read after it closed a socket that already had queued bytes: {saw:?}"
        );
        drop(conn);
    }

    #[test]
    fn an_assignment_expands_without_dropping_the_last_name() {
        let orders = "orders-topic-name-is-long".to_string();
        let orders_ptr = orders.as_ptr();
        let expanded = expand_topic_partitions(vec![(orders, vec![1, 0])]);
        assert_eq!(
            expanded
                .first()
                .map(|(topic, partition)| (topic.as_str(), *partition)),
            Some(("orders-topic-name-is-long", 1))
        );
        assert_eq!(
            expanded
                .get(1)
                .map(|(topic, partition)| (topic.as_ptr(), *partition)),
            Some((orders_ptr, 0))
        );
        let mut topics = vec!["orders".to_string()];
        remember_unique_topic(&mut topics, "orders");
        remember_unique_topic(&mut topics, "payments-topic-name-is-long");
        let payments_ptr = topics.get(1).map(|topic| topic.as_ptr());
        remember_unique_topic(&mut topics, "payments-topic-name-is-long");
        assert_eq!(
            topics.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["orders", "payments-topic-name-is-long"]
        );
        assert_eq!(topics.get(1).map(|topic| topic.as_ptr()), payments_ptr);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn extending_many_joined_topics_does_not_hold_the_caller() {
        // Scanning the joined topic list is the same scan that held assignment
        // grouping for 1.2s. Extending 1_000_000 names without a pause held the
        // caller 1.1s.
        let incoming: Vec<String> = (0..1_000_000)
            .map(|index| format!("topic-{index}"))
            .collect();
        let mut topics = Vec::new();
        let mut seen = HashSet::new();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = extend_unique_topics(&mut topics, &mut seen, &incoming) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "extending joined topics held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn joined_topics_keep_the_first_name() {
        let mut topics = vec!["orders".to_string()];
        let mut seen = HashSet::new();
        remember_existing_topics(&mut seen, &topics).await;
        extend_unique_topics(
            &mut topics,
            &mut seen,
            &[
                "orders".to_string(),
                "payments".to_string(),
                "orders".to_string(),
            ],
        )
        .await;
        assert_eq!(
            topics.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["orders", "payments"]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_many_revoked_partitions_does_not_hold_the_caller() {
        // 2_000_000 partitions: indexing the revoked set held the caller 1.30s.
        let revoked: Vec<(String, i32)> = (0..2_000_000)
            .map(|index| ("orders".to_owned(), index))
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            index = index_revoked(&revoked) => {
                assert_eq!(index.get("orders").map(HashSet::len), Some(2_000_000));
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "indexing revoked partitions held the caller for {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn revoked_partitions_match_by_borrowed_topic_name() {
        let revoked = vec![
            ("orders".to_owned(), 0),
            ("orders".to_owned(), 2),
            ("payments".to_owned(), 1),
            ("orders".to_owned(), 0),
        ];
        let index = index_revoked(&revoked).await;
        assert!(revoked_contains(&index, "orders", 0));
        assert!(revoked_contains(&index, "orders", 2));
        assert!(revoked_contains(&index, "payments", 1));
        assert!(!revoked_contains(&index, "orders", 1));
        assert!(!revoked_contains(&index, "payments", 0));
        assert!(!revoked_contains(&index, "other", 0));
        assert_eq!(index.get("orders").map(HashSet::len), Some(2));
    }

    #[test]
    fn committed_offsets_keep_the_topic_allocation() {
        let orders = "orders-topic-name-is-long".to_string();
        let orders_ptr = orders.as_ptr();
        let mut parts = HashMap::new();
        let _ = parts.insert(0, OffsetAndMetadata::new(1));
        let _ = parts.insert(2, OffsetAndMetadata::new(4));
        let mut map = HashMap::new();
        let _ = map.insert(orders, parts);
        let pairs = committed_offset_pairs(map);
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().any(|(tp, _)| tp.topic.as_ptr() == orders_ptr));
        assert!(pairs.iter().all(|(tp, md)| {
            tp.topic == "orders-topic-name-is-long" && (md.offset == 1 || md.offset == 4)
        }));
        let payments = TopicPartition::new("payments", 1);
        assert_ne!(payments.topic.as_ptr(), orders_ptr);
    }

    #[test]
    fn a_commit_uses_the_fetched_epoch_ahead_of_metadata() {
        assert_eq!(commit_position_epoch(Some(1), 4, 9), 1);
        assert_eq!(commit_position_epoch(None, 4, 9), 4);
        assert_eq!(commit_position_epoch(None, -1, 9), 9);
        assert_eq!(commit_position_epoch(Some(-1), -1, 9), 9);
    }

    #[test]
    fn consumer_group_metadata_getters_match_java() {
        let md = ConsumerGroupMetadata {
            group_id: "g".into(),
            generation_id: 4,
            member_id: "m".into(),
            group_instance_id: Some("i".into()),
        };
        assert_eq!(md.group_id(), "g");
        assert_eq!(md.generation_id(), 4);
        assert_eq!(md.member_id(), "m");
        assert_eq!(md.group_instance_id(), Some("i"));
        assert_eq!(
            md.to_string(),
            "GroupMetadata(groupId = g, generationId = 4, memberId = m, groupInstanceId = i)"
        );
        let classic = ConsumerGroupMetadata {
            group_id: "g".into(),
            generation_id: 1,
            member_id: "m".into(),
            group_instance_id: None,
        };
        assert!(classic.group_instance_id().is_none());
        assert_eq!(
            classic.to_string(),
            "GroupMetadata(groupId = g, generationId = 1, memberId = m, groupInstanceId = )"
        );
        let unknown = ConsumerGroupMetadata::new("g");
        assert_eq!(ConsumerGroupMetadata::UNKNOWN_GENERATION_ID, -1);
        assert_eq!(ConsumerGroupMetadata::UNKNOWN_MEMBER_ID, "");
        assert_eq!(
            ConsumerGroupMetadata::UNKNOWN_GENERATION_ID,
            JoinGroupRequest::UNKNOWN_GENERATION_ID
        );
        assert_eq!(
            ConsumerGroupMetadata::UNKNOWN_MEMBER_ID,
            JoinGroupRequest::UNKNOWN_MEMBER_ID
        );
        assert_eq!(unknown.group_id(), "g");
        assert_eq!(
            unknown.generation_id(),
            ConsumerGroupMetadata::UNKNOWN_GENERATION_ID
        );
        assert_eq!(
            unknown.member_id(),
            ConsumerGroupMetadata::UNKNOWN_MEMBER_ID
        );
        assert!(unknown.group_instance_id().is_none());
        assert_eq!(
            unknown.to_string(),
            "GroupMetadata(groupId = g, generationId = -1, memberId = , groupInstanceId = )"
        );
    }

    #[test]
    fn group_protocol_matches_java() {
        assert_eq!(GroupProtocol::Classic.as_str(), "CLASSIC");
        assert_eq!(GroupProtocol::Consumer.as_str(), "CONSUMER");
        assert_eq!(GroupProtocol::Classic.to_string(), "CLASSIC");
        assert_eq!(GroupProtocol::Consumer.to_string(), "CONSUMER");
        assert_eq!(GroupProtocol::of("classic"), Some(GroupProtocol::Classic));
        assert_eq!(GroupProtocol::of("CONSUMER"), Some(GroupProtocol::Consumer));
        assert!(GroupProtocol::of("share").is_none());
        assert!(GroupProtocol::of("nope").is_none());
    }

    #[test]
    fn range_splits_all_partitions_without_overlap() {
        let members = vec!["b".into(), "a".into()];
        let parts = vec![0, 1, 2, 3];
        let map = assign_range(&members, &parts);
        let mut union: Vec<i32> = map.values().flatten().copied().collect();
        union.sort();
        assert_eq!(union, parts);
        let a = map.get("a").cloned().unwrap_or_default();
        let b = map.get("b").cloned().unwrap_or_default();
        assert!(a.iter().all(|p| !b.contains(p)));
        assert_eq!(a, vec![0, 1]);
        assert_eq!(b, vec![2, 3]);
        let deduped = assign_range(&["a".into(), "a".into(), "b".into()], &[0, 0, 1, -1]);
        assert_eq!(deduped.get("a").cloned().unwrap_or_default(), vec![0]);
        assert_eq!(deduped.get("b").cloned().unwrap_or_default(), vec![1]);
    }

    #[test]
    fn sticky_does_not_assign_one_partition_twice() {
        let members = vec!["a".into(), "b".into()];
        let parts = vec![0, 0, 1, -1];
        let map = assign_sticky(&members, &parts, &HashMap::new());
        let mut union: Vec<i32> = map.values().flatten().copied().collect();
        union.sort_unstable();
        assert_eq!(union, vec![0, 1]);
        let a = map.get("a").cloned().unwrap_or_default();
        let b = map.get("b").cloned().unwrap_or_default();
        assert!(a.iter().all(|p| !b.contains(p)));
    }

    #[test]
    fn sticky_owned_repeat_by_the_same_member_keeps_the_partition() {
        let claims = vec![(
            "a".into(),
            vec![("t".into(), 0), ("t".into(), 0), ("t".into(), 1)],
            4,
        )];
        let got = resolve_sticky_owned_partitions(&claims);
        assert_eq!(
            got.get("a").cloned().unwrap_or_default(),
            vec![("t".into(), 0), ("t".into(), 1)]
        );
    }

    #[test]
    fn sticky_owned_higher_generation_keeps_partition() {
        let claims = vec![
            (
                "a".into(),
                vec![("t".into(), 0), ("t".into(), 1)],
                ConsumerProtocolSubscription::DEFAULT_GENERATION,
            ),
            ("b".into(), vec![("t".into(), 0), ("t".into(), 2)], 3),
        ];
        let got = resolve_sticky_owned_partitions(&claims);
        let cooperative = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(resolve_sticky_owned_partitions_cooperative(&claims));
        assert_eq!(cooperative, got);
        assert_eq!(
            got.get("a").cloned().unwrap_or_default(),
            vec![("t".into(), 1)]
        );
        assert_eq!(
            got.get("b").cloned().unwrap_or_default(),
            vec![("t".into(), 0), ("t".into(), 2)]
        );
    }

    #[test]
    fn sticky_owned_same_generation_revokes_from_both() {
        let gen = 4;
        let claims = vec![
            ("a".into(), vec![("t".into(), 0)], gen),
            ("b".into(), vec![("t".into(), 0)], gen),
        ];
        let got = resolve_sticky_owned_partitions(&claims);
        assert!(got.get("a").cloned().unwrap_or_default().is_empty());
        assert!(got.get("b").cloned().unwrap_or_default().is_empty());
    }

    #[test]
    fn sticky_owned_later_higher_generation_takes_doubly_claimed() {
        let claims = vec![
            ("a".into(), vec![("t".into(), 0)], 5),
            ("b".into(), vec![("t".into(), 0)], 5),
            ("c".into(), vec![("t".into(), 0)], 6),
        ];
        let got = resolve_sticky_owned_partitions(&claims);
        assert!(got.get("a").cloned().unwrap_or_default().is_empty());
        assert!(got.get("b").cloned().unwrap_or_default().is_empty());
        assert_eq!(
            got.get("c").cloned().unwrap_or_default(),
            vec![("t".into(), 0)]
        );
    }

    #[test]
    fn sticky_keeps_previous_when_still_valid() {
        let members = vec!["a".into(), "b".into()];
        let parts = vec![0, 1, 2, 3];
        let mut prev = HashMap::new();
        let _ = prev.insert("a".into(), vec![2, 3]);
        let _ = prev.insert("b".into(), vec![0, 1]);
        let map = assign_sticky(&members, &parts, &prev);
        assert_eq!(map.get("a").cloned().unwrap_or_default(), vec![2, 3]);
        assert_eq!(map.get("b").cloned().unwrap_or_default(), vec![0, 1]);
    }

    #[test]
    fn sticky_rebalances_when_a_member_joins() {
        let members = vec!["a".into(), "b".into()];
        let parts = vec![0, 1, 2, 3];
        let mut prev = HashMap::new();
        let _ = prev.insert("a".into(), vec![0, 1, 2, 3]);
        let map = assign_sticky(&members, &parts, &prev);
        let a = map.get("a").cloned().unwrap_or_default();
        let b = map.get("b").cloned().unwrap_or_default();
        assert_eq!(a.len(), 2);
        assert_eq!(b.len(), 2);
        let mut union = a;
        union.extend(b);
        union.sort();
        assert_eq!(union, parts);
    }

    #[test]
    fn cooperative_sticky_holds_owned_until_revoke() {
        let members = vec!["a".into(), "b".into()];
        let parts = vec![0, 1, 2, 3];
        let mut prev = HashMap::new();
        let _ = prev.insert("a".into(), vec![0, 1, 2, 3]);
        let first = assign_cooperative_sticky(&members, &parts, &prev);
        assert_eq!(first.get("a").map(Vec::len), Some(2));
        assert_eq!(first.get("b").map(Vec::len), Some(0));
        let mut prev2 = HashMap::new();
        let _ = prev2.insert("a".into(), first.get("a").cloned().unwrap_or_default());
        let _ = prev2.insert("b".into(), Vec::new());
        let second = assign_cooperative_sticky(&members, &parts, &prev2);
        let a = second.get("a").cloned().unwrap_or_default();
        let b = second.get("b").cloned().unwrap_or_default();
        assert_eq!(a.len(), 2);
        assert_eq!(b.len(), 2);
        assert!(a.iter().all(|p| !b.contains(p)));
    }

    #[test]
    fn a_reset_position_is_not_committed() {
        assert!(position_is_committable(0));
        assert!(position_is_committable(4));
        assert!(!position_is_committable(-1));
    }

    #[test]
    fn a_delivered_topic_id_map_keeps_its_buckets() {
        let topic: Arc<str> = Arc::from("topic-name-longer-than-twenty-three");
        let stored = Arc::as_ptr(&topic);
        let other: Arc<str> = Arc::from("another-topic-name-longer-than-twenty-three");
        let mut ids = HashMap::new();
        remember_shared_topic_id(&mut ids, &topic, 0, [1u8; 16]);
        remember_shared_topic_id(&mut ids, &topic, 1, [1u8; 16]);
        remember_shared_topic_id(&mut ids, &other, 0, [2u8; 16]);
        let buckets = ids.capacity();
        let inner = ids.get(topic.as_ref()).map(HashMap::capacity);
        clear_delivered_topic_ids(&mut ids);
        remember_shared_topic_id(&mut ids, &topic, 0, [3u8; 16]);
        ids.retain(|_, parts| !parts.is_empty());
        assert!(ids.capacity() >= buckets);
        assert!(ids.get(topic.as_ref()).map(HashMap::capacity) >= inner);
        assert_eq!(
            ids.get_key_value(topic.as_ref())
                .map(|(name, _)| Arc::as_ptr(name)),
            Some(stored)
        );
        assert_eq!(nested_topic_id(&ids, topic.as_ref(), 0), Some([3u8; 16]));
        assert_eq!(nested_topic_id(&ids, topic.as_ref(), 1), None);
        assert_eq!(nested_topic_id(&ids, other.as_ref(), 0), None);
    }

    #[test]
    fn a_delivered_topic_id_keeps_the_assignment_name() {
        let topic: Arc<str> = Arc::from("topic-name-longer-than-twenty-three");
        let stored = Arc::as_ptr(&topic);
        let mut ids = HashMap::new();
        remember_shared_topic_id(&mut ids, &topic, 0, [1u8; 16]);
        remember_shared_topic_id(&mut ids, &topic, 1, [1u8; 16]);
        assert_eq!(
            ids.get_key_value(topic.as_ref())
                .map(|(name, _)| Arc::as_ptr(name)),
            Some(stored)
        );
        assert_eq!(nested_topic_id(&ids, topic.as_ref(), 1), Some([1u8; 16]));
        let copy: Arc<str> = Arc::from(topic.as_ref());
        remember_shared_topic_id(&mut ids, &copy, 2, [1u8; 16]);
        assert_eq!(
            ids.get_key_value(topic.as_ref())
                .map(|(name, _)| Arc::as_ptr(name)),
            Some(stored)
        );
    }

    #[test]
    fn shared_offset_topics_keep_one_name_for_every_partition() {
        let topic: Arc<str> = Arc::from("topic-name-longer-than-twenty-three");
        let stored = Arc::as_ptr(&topic);
        let items = [
            (Arc::clone(&topic), 0, 4),
            (Arc::clone(&topic), 1, 9),
            (
                Arc::<str>::from("topic-name-longer-than-twenty-three"),
                2,
                1,
            ),
        ];
        let topics = group_shared_offset_topics(&items, |_, _| 3);
        assert_eq!(topics.len(), 1);
        let mut again = topics;
        let name = again
            .first()
            .map(|topic| topic.topic.as_ptr())
            .unwrap_or(std::ptr::null());
        let capacity = again
            .first()
            .map(|topic| topic.partitions.capacity())
            .unwrap_or(0);
        refill_shared_offset_topics(&mut again, &items, |_, _| 3);
        assert_eq!(again.first().map(|topic| topic.topic.as_ptr()), Some(name));
        assert!(again
            .first()
            .is_some_and(|topic| topic.partitions.capacity() >= capacity));
        let topics = again;
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic.as_str(), topic.as_ref());
        assert_eq!(topics[0].partitions.len(), 3);
        assert_eq!(topics[0].partitions[1].offset, 9);
        assert_eq!(topics[0].partitions[1].leader_epoch, 3);
        assert_eq!(Arc::as_ptr(&items[0].0), stored);
        assert_eq!(Arc::as_ptr(&items[1].0), stored);
    }

    #[test]
    fn group_offset_topics_collapses_partitions() {
        let assigned = vec![
            (TopicPartition::new("t", 1), OffsetAndMetadata::new(4)),
            (TopicPartition::new("t", 0), OffsetAndMetadata::new(2)),
            (TopicPartition::new("u", 0), OffsetAndMetadata::new(9)),
        ];
        let topics = group_offset_topics(&assigned);
        assert_eq!(topics.len(), 2);
        let t = topics.iter().find(|x| x.topic == "t").unwrap();
        assert_eq!(t.partitions.len(), 2);
        let u = topics.iter().find(|x| x.topic == "u").unwrap();
        assert_eq!(u.partitions[0].offset, 9);
        let repeated = vec![
            (TopicPartition::new("t", 0), OffsetAndMetadata::new(1)),
            (TopicPartition::new("t", 0), OffsetAndMetadata::new(8)),
        ];
        let mut topics = group_offset_topics(&repeated);
        assert_eq!(topics[0].partitions.len(), 1);
        assert_eq!(topics[0].partitions[0].offset, 1);
        let borrowed = group_borrowed_offset_topics(&repeated);
        assert_eq!(borrowed.len(), 1);
        assert_eq!(borrowed[0].topic, "t");
        assert_eq!(borrowed[0].partitions.len(), 1);
        assert_eq!(borrowed[0].partitions[0].offset, 1);
        let name = topics[0].topic.as_ptr();
        refill_offset_topics(&mut topics, &repeated);
        assert_eq!(topics[0].topic.as_ptr(), name);
        assert_eq!(topics[0].partitions[0].offset, 1);
        let borrowed_fetch = group_borrowed_offset_fetch_topics([("t", 0), ("t", 0), ("t", 1)]);
        assert_eq!(borrowed_fetch.len(), 1);
        assert_eq!(borrowed_fetch[0].0, "t");
        assert_eq!(borrowed_fetch[0].1, vec![0, 1]);
        let mut fetched = group_offset_fetch_topics([("t", 0), ("t", 0), ("t", 1)]);
        assert_eq!(fetched[0].partitions, vec![0, 1]);
        let fetch_name = fetched[0].topic.as_ptr();
        refill_offset_fetch_topics(&mut fetched, [("t", 0), ("t", 0), ("t", 1)]);
        assert_eq!(fetched[0].topic.as_ptr(), fetch_name);
        assert_eq!(fetched[0].partitions, vec![0, 1]);
    }

    #[test]
    fn kip848_fenced_epoch_restarts_at_join() {
        assert_eq!(
            kip848_heartbeat_epoch(error::FENCED_MEMBER_EPOCH, 8, 8),
            ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        );
        assert_eq!(
            kip848_heartbeat_epoch(error::UNKNOWN_MEMBER_ID, 8, 8),
            ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        );
        assert_eq!(kip848_heartbeat_epoch(0, 6, 4), 6);
        assert_eq!(kip848_heartbeat_epoch(0, 0, 4), 4);
        assert_eq!(kip848_heartbeat_epoch(error::STALE_MEMBER_EPOCH, 2, 5), 2);
        assert_eq!(kip848_heartbeat_epoch(error::STALE_MEMBER_EPOCH, 0, 5), 0);
        assert_eq!(
            kip848_heartbeat_epoch(error::STALE_MEMBER_EPOCH, -1, 5),
            ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
        );
    }

    #[test]
    fn classic_heartbeat_rejoins_a_lost_generation_and_fails_a_fence() {
        assert!(matches!(classic_heartbeat(0), ClassicHeartbeat::Ready));
        assert!(matches!(
            classic_heartbeat(error::REBALANCE_IN_PROGRESS),
            ClassicHeartbeat::Rejoin
        ));
        assert!(matches!(
            classic_heartbeat(error::ILLEGAL_GENERATION),
            ClassicHeartbeat::Rejoin
        ));
        assert!(matches!(
            classic_heartbeat(error::UNKNOWN_MEMBER_ID),
            ClassicHeartbeat::Rejoin
        ));
        assert!(matches!(
            classic_heartbeat(error::FENCED_INSTANCE_ID),
            ClassicHeartbeat::Failed
        ));
        assert!(matches!(
            classic_heartbeat(error::UNSUPPORTED_VERSION),
            ClassicHeartbeat::Failed
        ));
    }

    #[test]
    fn coordinator_retriable_covers_load_and_move() {
        assert!(error::coordinator_retriable(
            error::COORDINATOR_LOAD_IN_PROGRESS
        ));
        assert!(error::coordinator_retriable(
            error::COORDINATOR_NOT_AVAILABLE
        ));
        assert!(error::coordinator_retriable(error::NOT_COORDINATOR));
        assert!(!error::coordinator_retriable(0));
        assert!(!error::coordinator_retriable(error::INVALID_TXN_STATE));
        assert!(Error::broker(error::COORDINATOR_LOAD_IN_PROGRESS, "x").is_retriable());
        assert!(Error::broker(error::COORDINATOR_NOT_AVAILABLE, "x").is_retriable());
    }

    #[test]
    fn offset_commit_load_in_progress_is_not_at_byte_four() {
        use crate::protocol::api_keys::OFFSET_COMMIT;
        use crate::protocol::group::{encode_offset_commit_response, OffsetPartition, OffsetTopic};
        let topics = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition::new(0, 1)],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_response(&mut buf, 7, &topics, error::COORDINATOR_LOAD_IN_PROGRESS)
            .unwrap();
        assert_ne!(
            peek_error_code(&buf),
            Some(error::COORDINATOR_LOAD_IN_PROGRESS),
            "throttle + topic-array length must not look like error 14"
        );
        assert_eq!(
            coordinator_error(OFFSET_COMMIT, 7, &buf),
            Some(error::COORDINATOR_LOAD_IN_PROGRESS)
        );
    }

    #[test]
    fn offset_commit_not_coordinator_is_not_at_byte_four() {
        use crate::protocol::api_keys::OFFSET_COMMIT;
        use crate::protocol::group::{encode_offset_commit_response, OffsetPartition, OffsetTopic};
        let topics = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition::new(0, 1)],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_response(&mut buf, 7, &topics, error::NOT_COORDINATOR).unwrap();
        assert_ne!(
            peek_error_code(&buf),
            Some(error::NOT_COORDINATOR),
            "throttle + topic-array length must not look like error 16"
        );
        assert_eq!(
            coordinator_error(OFFSET_COMMIT, 7, &buf),
            Some(error::NOT_COORDINATOR)
        );
    }

    #[test]
    fn offset_fetch_not_coordinator_is_not_at_byte_four() {
        use crate::protocol::api_keys::OFFSET_FETCH;
        use crate::protocol::group::{
            encode_offset_fetch_response, FetchedOffset, FetchedOffsetTopic,
        };
        let topics = vec![FetchedOffsetTopic {
            topic: "t".into(),
            partitions: vec![FetchedOffset::new(0, -1, error::NOT_COORDINATOR)],
        }];
        let mut buf = BytesMut::new();
        encode_offset_fetch_response(&mut buf, 5, "g", &topics, 0).unwrap();
        assert_ne!(
            peek_error_code(&buf),
            Some(error::NOT_COORDINATOR),
            "throttle + topic-array length must not look like error 16"
        );
        assert_eq!(
            coordinator_error(OFFSET_FETCH, 5, &buf),
            Some(error::NOT_COORDINATOR)
        );
    }

    #[tokio::test]
    async fn kip848_wanted_maps_topic_id_to_subscribed_name() {
        let assignment = vec![TopicPartitions {
            topic_id: [9u8; 16],
            partitions: vec![1, 3],
        }];
        assert_eq!(
            wanted_from_kip848(
                &["orders".into()],
                &HashMap::<[u8; 16], String>::new(),
                &assignment,
            )
            .await,
            vec![("orders".into(), 1), ("orders".into(), 3)]
        );
        let mut ids: HashMap<[u8; 16], String> = HashMap::new();
        let _ = ids.insert([1u8; 16], "payments".into());
        let multi = vec![
            TopicPartitions {
                topic_id: [1u8; 16],
                partitions: vec![0],
            },
            TopicPartitions {
                topic_id: [2u8; 16],
                partitions: vec![1],
            },
        ];
        assert_eq!(
            wanted_from_kip848(&["orders".into(), "payments".into()], &ids, &multi).await,
            vec![("payments".into(), 0)]
        );
        let messy = vec![TopicPartitions {
            topic_id: [1u8; 16],
            partitions: vec![0, -1, 0, 2],
        }];
        assert_eq!(
            wanted_from_kip848(&["payments".into()], &ids, &messy).await,
            vec![("payments".into(), 0), ("payments".into(), 2)]
        );
        let repeated = vec![
            TopicPartitions {
                topic_id: [1u8; 16],
                partitions: vec![0, 2],
            },
            TopicPartitions {
                topic_id: [1u8; 16],
                partitions: vec![2, 4],
            },
        ];
        assert_eq!(
            wanted_from_kip848(&["payments".into()], &ids, &repeated).await,
            vec![
                ("payments".into(), 0),
                ("payments".into(), 2),
                ("payments".into(), 4)
            ]
        );
        let unknown = vec![TopicPartitions {
            topic_id: [9u8; 16],
            partitions: vec![1],
        }];
        let unresolved =
            wanted_from_kip848(&["orders".into(), "payments".into()], &ids, &unknown).await;
        assert!(unresolved.is_empty());
        assert!(!kip848_assignment_can_apply(&unresolved, &unknown));
        assert!(kip848_assignment_can_apply(&[], &[]));
        let zero = vec![TopicPartitions {
            topic_id: [0u8; 16],
            partitions: vec![0, 1],
        }];
        assert!(
            wanted_from_kip848(&["orders".into(), "payments".into()], &ids, &zero)
                .await
                .is_empty()
        );
        assert_eq!(
            wanted_from_kip848(
                &["orders".into()],
                &HashMap::<[u8; 16], String>::new(),
                &zero,
            )
            .await,
            vec![("orders".into(), 0), ("orders".into(), 1)]
        );
    }

    #[test]
    fn range_topics_assigns_each_topic_independently() {
        let members = vec!["a".into(), "b".into()];
        let topics = vec![("orders".into(), vec![0, 1]), ("payments".into(), vec![0])];
        let map = assign_range_topics(&members, &topics);
        let a = map.get("a").cloned().unwrap_or_default();
        let b = map.get("b").cloned().unwrap_or_default();
        let mut all = a;
        all.extend(b);
        all.sort();
        assert_eq!(
            all,
            vec![
                ("orders".into(), 0),
                ("orders".into(), 1),
                ("payments".into(), 0),
            ]
        );
    }

    #[test]
    fn sticky_subscribed_keeps_a_previous_owner_and_one_subscription() {
        let subs = vec![
            ("a".into(), vec!["orders".into(), "orders".into()]),
            ("b".into(), vec!["orders".into(), "payments".into()]),
        ];
        let topics = vec![("orders".into(), vec![0, 1]), ("payments".into(), vec![0])];
        let mut prev = HashMap::new();
        let _ = prev.insert("a".into(), vec![("orders".into(), 1)]);
        let map = assign_sticky_subscribed(&subs, &topics, &prev);
        let cooperative = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(assign_sticky_subscribed_cooperative(&subs, &topics, &prev));
        assert_eq!(cooperative, map);
        let mut a = map.get("a").cloned().unwrap_or_default();
        let mut b = map.get("b").cloned().unwrap_or_default();
        a.sort();
        b.sort();
        assert!(a.contains(&("orders".into(), 1)));
        assert_eq!(a.len() + b.len(), 3);
        assert!(b.iter().any(|owned| owned.0 == "payments"));
        assert!(!a.iter().any(|owned| owned.0 == "payments"));
    }

    #[test]
    fn range_subscribed_keeps_members_on_their_topics() {
        let subs = vec![
            ("a".into(), vec!["orders".into()]),
            ("b".into(), vec!["payments".into()]),
        ];
        let topics = vec![
            ("orders".into(), vec![0, 1]),
            ("payments".into(), vec![0, 1]),
        ];
        let map = assign_range_subscribed(&subs, &topics);
        let mut a = map.get("a").cloned().unwrap_or_default();
        let mut b = map.get("b").cloned().unwrap_or_default();
        a.sort();
        b.sort();
        assert_eq!(a, vec![("orders".into(), 0), ("orders".into(), 1)]);
        assert_eq!(b, vec![("payments".into(), 0), ("payments".into(), 1)]);
    }

    #[test]
    fn committed_offsets_reuse_a_repeated_topic() {
        let fetched = [
            FetchedOffsetTopic {
                topic: "orders".into(),
                partitions: vec![FetchedOffset::new(0, 4, 0)],
            },
            FetchedOffsetTopic {
                topic: "orders".into(),
                partitions: vec![FetchedOffset::new(1, 9, 0), FetchedOffset::new(0, 8, 0)],
            },
        ];
        let map = committed_offset_map(&fetched).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(
            committed_offset(&map, "orders", 1).map(|md| md.offset),
            Some(9)
        );
        assert_eq!(
            committed_offset(&map, "orders", 0).map(|md| md.offset),
            Some(8)
        );
        assert_eq!(
            fetched_committed(&fetched, "orders", 0).map(|part| part.offset),
            Some(8)
        );
        assert_eq!(
            fetched_committed(&fetched, "orders", 1).map(|part| part.offset),
            Some(9)
        );
        let failed = [FetchedOffsetTopic {
            topic: "orders".into(),
            partitions: vec![FetchedOffset::new(0, 4, 1)],
        }];
        assert!(check_fetched_offsets(&failed).is_err());
        assert!(committed_offset_map(&failed).is_err());
    }

    #[test]
    fn committed_starts_uses_fetched_or_zero() {
        let wanted = vec![("t".into(), 0), ("t".into(), 1)];
        let fetched = vec![FetchedOffsetTopic {
            topic: "t".into(),
            partitions: vec![FetchedOffset::new(0, 5, 0)],
        }];
        let starts =
            committed_starts(&[("t".into(), 0)], &fetched, AutoOffsetReset::Earliest).unwrap();
        assert_eq!(starts, vec![("t".into(), 0, 5)]);
        assert_eq!(OffsetAndMetadata::INVALID_OFFSET, -1);
        assert!(committed_starts(&wanted, &fetched, AutoOffsetReset::None).is_err());
        assert!(committed_starts(&wanted, &fetched, AutoOffsetReset::Earliest).is_err());
        assert!(committed_starts(&wanted, &fetched, AutoOffsetReset::Latest).is_err());
    }

    #[test]
    fn collect_topics_keeps_the_first_copy() {
        let topics = collect_topics(["orders", "payments", "orders"]).unwrap();
        assert_eq!(topics, vec!["orders".to_string(), "payments".to_string()]);
        assert!(collect_topics(Vec::<String>::new()).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn applying_many_reset_offsets_does_not_hold_the_caller() {
        let listed: Vec<(TopicPartition, i64)> = (0..4_000_000)
            .map(|partition| (TopicPartition::new("orders", partition), 4))
            .collect();
        let reset_slots: Vec<usize> = (0..4_000_000).collect();
        let starts: Vec<Option<(String, i32, i64)>> = vec![None; 4_000_000];
        let started = std::time::Instant::now();
        let worker = tokio::spawn(async move {
            let mut starts = starts;
            let _applied = apply_reset_offsets(&mut starts, &reset_slots, listed).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        worker.abort();
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "applying reset offsets held the caller for {elapsed:?}"
        );
        let mut sample = vec![None, None];
        apply_reset_offsets(
            &mut sample,
            &[1],
            vec![(TopicPartition::new("orders", 3), 9)],
        )
        .await
        .expect("reset");
        let flat = flatten_reset_starts(sample).await;
        assert_eq!(flat, vec![("orders".to_string(), 3, 9)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flattening_many_reset_starts_does_not_hold_the_caller() {
        let starts: Vec<Option<(String, i32, i64)>> = (0..4_000_000)
            .map(|partition| Some(("orders".to_string(), partition, 4)))
            .collect();
        let started = std::time::Instant::now();
        let worker = tokio::spawn(async move {
            let _flat = flatten_reset_starts(starts).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        worker.abort();
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "flattening reset starts held the caller for {elapsed:?}"
        );
        let flat = flatten_reset_starts(vec![None, Some(("orders".to_string(), 3, 9))]).await;
        assert_eq!(flat, vec![("orders".to_string(), 3, 9)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collecting_many_revoked_commits_does_not_hold_the_caller() {
        let positions: Vec<(TopicPartition, i64)> = (0..2_000_000)
            .map(|partition| (TopicPartition::new("orders", partition), 1))
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = revoked_commits(positions, |_topic, _partition| 4, |_tp, offset| offset >= 0) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "collecting revoked commits held the caller for {elapsed:?}"
        );
        let sample = vec![
            (TopicPartition::new("orders", 0), 3),
            (TopicPartition::new("orders", 1), -1),
        ];
        let commits =
            revoked_commits(sample, |_topic, _partition| 7, |_tp, offset| offset >= 0).await;
        assert_eq!(commits.len(), 1);
        assert_eq!(
            commits
                .first()
                .map(|(tp, offset)| (tp.partition, offset.offset)),
            Some((0, 3))
        );
        assert_eq!(
            commits.first().map(|(_, offset)| offset.leader_epoch),
            Some(Some(7))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn comparing_many_assigned_topic_ids_does_not_hold_the_caller() {
        let mut assigned_ids: HashMap<Arc<str>, HashMap<i32, [u8; 16]>> = HashMap::new();
        let mut identities: HashMap<Arc<str>, [u8; 16]> = HashMap::new();
        let mut wanted = Vec::with_capacity(2_000_000);
        for index in 0..2_000_000 {
            let name: Arc<str> = Arc::from(format!("topic-{index}"));
            let mut parts = HashMap::new();
            let _stored = parts.insert(0, [1u8; 16]);
            let _stored = assigned_ids.insert(Arc::clone(&name), parts);
            let _stored = identities.insert(Arc::clone(&name), [1u8; 16]);
            wanted.push((name.to_string(), 0));
        }
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assignment_topic_ids_changed(
                &wanted,
                |topic, partition| {
                    assigned_ids
                        .get(topic)
                        .and_then(|parts| parts.get(&partition))
                        .copied()
                },
                &identities,
            ) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "comparing assigned topic ids held the caller for {elapsed:?}"
        );
        let identities = HashMap::from([
            (Arc::<str>::from("orders"), [1u8; 16]),
            (Arc::<str>::from("payments"), [1u8; 16]),
        ]);
        let wanted = [("orders".to_string(), 0), ("payments".to_string(), 1)];
        let mut calls = 0usize;
        let changed = assignment_topic_ids_changed(
            &wanted,
            |topic, _partition| {
                calls = calls.saturating_add(1);
                if topic == "orders" {
                    Some([2u8; 16])
                } else {
                    Some([1u8; 16])
                }
            },
            &identities,
        )
        .await;
        assert!(changed);
        assert_eq!(calls, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cloning_many_topic_ids_does_not_hold_the_caller() {
        let identities: HashMap<Arc<str>, [u8; 16]> = (0..2_000_000)
            .map(|index| {
                let mut id = [0u8; 16];
                id[..8].copy_from_slice(&(index as u64).to_le_bytes());
                (Arc::from(format!("topic-{index}")), id)
            })
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = clone_topic_ids(&identities) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "cloning topic ids held the caller for {elapsed:?}"
        );
        let name: Arc<str> = Arc::from("orders-topic-name-longer-than-inline");
        let one = HashMap::from([(Arc::clone(&name), [1u8; 16])]);
        let cloned = clone_topic_ids(&one).await;
        assert_eq!(cloned.get(name.as_ref()).copied(), Some([1u8; 16]));
        assert_eq!(
            cloned
                .get_key_value(name.as_ref())
                .map(|(key, _)| Arc::as_ptr(key)),
            Some(Arc::as_ptr(&name))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scanning_many_assignment_topic_ids_does_not_hold_the_caller() {
        let mut names = HashMap::new();
        let assignment: Vec<TopicPartitions> = (0..2_000_000)
            .map(|index| {
                let mut topic_id = [0u8; 16];
                topic_id[..8].copy_from_slice(&(index as u64).to_le_bytes());
                let _stored = names.insert(topic_id, ());
                TopicPartitions {
                    topic_id,
                    partitions: Vec::new(),
                }
            })
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assignment_has_unknown_topic_id(&assignment, &names) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "scanning assignment topic ids held the caller for {elapsed:?}"
        );
        let known = HashMap::from([([1u8; 16], ())]);
        assert!(
            !assignment_has_unknown_topic_id(
                &[TopicPartitions {
                    topic_id: [1u8; 16],
                    partitions: vec![0],
                }],
                &known,
            )
            .await
        );
        assert!(
            assignment_has_unknown_topic_id(
                &[TopicPartitions {
                    topic_id: [2u8; 16],
                    partitions: vec![0],
                }],
                &known,
            )
            .await
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn opening_many_kip848_partitions_does_not_hold_the_caller() {
        let mut topic_id = [0u8; 16];
        topic_id[0] = 1;
        let ids = HashMap::from([(topic_id, "orders".to_string())]);
        let assignment = vec![TopicPartitions {
            topic_id,
            partitions: (0..2_000_000).collect(),
        }];
        let subscribed = vec!["orders".to_string()];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = wanted_from_kip848(&subscribed, &ids, &assignment) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "opening a group assignment held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_many_subscribed_members_does_not_hold_the_caller() {
        let topics: Vec<String> = (0..2_000_000)
            .map(|index| format!("topic-{index}"))
            .collect();
        let members = [("member-1".to_string(), topics)];
        let started = std::time::Instant::now();
        tokio::select! {
            _ = members_by_topic_cooperative(&members) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "indexing subscribed members held the caller for {:?}",
            started.elapsed()
        );
        let sample = [
            (
                "m1".to_string(),
                vec![
                    "orders".to_string(),
                    "orders".to_string(),
                    "payments".to_string(),
                ],
            ),
            ("m2".to_string(), vec!["orders".to_string()]),
        ];
        assert_eq!(
            members_by_topic_cooperative(&sample).await,
            members_by_topic(&sample)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn indexing_many_previous_partitions_does_not_hold_the_caller() {
        let owned: Vec<(String, i32)> = (0..2_000_000)
            .map(|index| (format!("topic-{index}"), index))
            .collect();
        let mut prev = HashMap::new();
        let _ = prev.insert("member-1".to_string(), owned);
        let started = std::time::Instant::now();
        tokio::select! {
            _ = previous_partitions_by_topic_cooperative(&prev) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "indexing previous partitions held the caller for {:?}",
            started.elapsed()
        );
        let mut sample = HashMap::new();
        let _ = sample.insert(
            "m1".to_string(),
            vec![
                ("orders".to_string(), 0),
                ("orders".to_string(), 1),
                ("payments".to_string(), 4),
            ],
        );
        let _ = sample.insert("m2".to_string(), vec![("orders".to_string(), 2)]);
        assert_eq!(
            previous_partitions_by_topic_cooperative(&sample).await,
            previous_partitions_by_topic(&sample)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn opening_many_assignment_members_does_not_hold_the_caller() {
        let members: Vec<(String, Vec<String>)> = (0..2_000_000)
            .map(|index| (format!("member-{index}"), Vec::new()))
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assign_range_subscribed_cooperative(&members, &[]) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "opening assignment members held the caller for {:?}",
            started.elapsed()
        );
        let sample = [
            ("m1".to_string(), Vec::new()),
            ("m2".to_string(), Vec::new()),
        ];
        let assigned = assign_range_subscribed_cooperative(&sample, &[]).await;
        assert_eq!(assigned.len(), 2);
        assert_eq!(assigned.get("m1").map(Vec::len), Some(0));
        assert_eq!(assigned.get("m2").map(Vec::len), Some(0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preparing_many_sticky_members_does_not_hold_the_caller() {
        let names: Vec<String> = (0..2_000_000)
            .map(|index| format!("member-{index}"))
            .collect();
        let members: Vec<&str> = names.iter().map(String::as_str).collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assign_sticky_from_cooperative(&members, &[], std::iter::empty()) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "preparing sticky members held the caller for {:?}",
            started.elapsed()
        );
        let members = ["b", "a"];
        let partitions = [2, 0, 1, -1];
        let prev = [("b", &[0i32][..])];
        assert_eq!(
            assign_sticky_from_cooperative(&members, &partitions, prev).await,
            assign_sticky_from(&members, &partitions, prev)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordering_many_range_members_does_not_hold_the_caller() {
        let names: Vec<String> = (0..2_000_000)
            .map(|index| format!("member-{index}"))
            .collect();
        let members: Vec<&str> = names.iter().map(String::as_str).collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = assign_range_names_cooperative(&members, &[]) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "ordering range members held the caller for {:?}",
            started.elapsed()
        );
        let members = ["b", "a", "b", "a"];
        let partitions = [2, 0, 1, -1, 0];
        assert_eq!(
            assign_range_names_cooperative(&members, &partitions).await,
            assign_range_names(&members, &partitions)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reusing_many_topic_partitions_does_not_hold_the_caller() {
        let owned: Vec<TopicPartitions> = (0..2_000_000)
            .map(|index| {
                let mut topic_id = [0u8; 16];
                topic_id[..8].copy_from_slice(&(index as u64).to_le_bytes());
                TopicPartitions {
                    topic_id,
                    partitions: vec![index],
                }
            })
            .collect();
        let stored = parking_lot::Mutex::new(Some(owned));
        let mut dest = None;
        let started = std::time::Instant::now();
        let task = tokio::spawn(async move {
            reuse_topic_partitions_locked(&mut dest, &stored).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let elapsed = started.elapsed();
        task.abort();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "reusing topic partitions held the caller for {elapsed:?}"
        );
        let sample = parking_lot::Mutex::new(Some(vec![TopicPartitions {
            topic_id: [1; 16],
            partitions: vec![0, 1],
        }]));
        let mut kept = Some(vec![TopicPartitions {
            topic_id: [1; 16],
            partitions: vec![0, 1],
        }]);
        let ptr = kept.as_ref().map(Vec::as_ptr);
        reuse_topic_partitions_locked(&mut kept, &sample).await;
        assert_eq!(kept.as_ref().map(Vec::as_ptr), ptr);
        *sample.lock() = None;
        reuse_topic_partitions_locked(&mut kept, &sample).await;
        assert!(kept.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reusing_many_topic_names_does_not_hold_the_caller() {
        let topics: Vec<String> = (0..4_000_000)
            .map(|index| format!("topic-{index}"))
            .collect();
        let stored = parking_lot::Mutex::new(topics);
        let mut names = None;
        let started = std::time::Instant::now();
        let task = tokio::spawn(async move {
            reuse_topic_names_locked(&mut names, &stored).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let elapsed = started.elapsed();
        task.abort();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "reusing topic names held the caller for {elapsed:?}"
        );
        let sample = parking_lot::Mutex::new(vec!["orders".to_string()]);
        let mut kept = Some(vec!["orders".to_string()]);
        let ptr = kept.as_ref().map(Vec::as_ptr);
        reuse_topic_names_locked(&mut kept, &sample).await;
        assert_eq!(kept.as_ref().map(Vec::as_ptr), ptr);
        *sample.lock() = vec!["payments".to_string()];
        reuse_topic_names_locked(&mut kept, &sample).await;
        assert_eq!(kept.as_deref(), Some(["payments".to_string()].as_slice()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cloning_many_subscribed_topics_does_not_hold_the_caller() {
        let topics: Vec<String> = (0..2_000_000)
            .map(|index| format!("topic-{index}"))
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = clone_names(&topics) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "cloning subscribed topics held the caller for {:?}",
            started.elapsed()
        );
        let sample = [
            "orders".to_string(),
            "payments".to_string(),
            "orders".to_string(),
        ];
        assert_eq!(clone_names(&sample).await, sample);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn filtering_many_matching_topics_does_not_hold_the_caller() {
        let names: Vec<String> = (0..2_000_000)
            .map(|index| format!("topic-{index}"))
            .collect();
        let started = std::time::Instant::now();
        tokio::select! {
            _ = filter_matching_topics_cooperative(names.iter(), |_| true) => {}
            () = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(80),
            "filtering matching topics held the caller for {:?}",
            started.elapsed()
        );
        let sample = ["b", "a", "b", "__hidden", "", "a"];
        let matched = filter_matching_topics_cooperative(sample, |_| true).await;
        assert_eq!(matched, filter_matching_topics(sample, |_| true));
    }

    #[test]
    fn filter_matching_topics_skips_internal_and_sorts() {
        let names = filter_matching_topics(
            [
                "z-a",
                "__consumer_offsets",
                "a-1",
                "a-1",
                "",
                "b-1",
                "bad name",
                ".",
                "..",
            ],
            |n| {
                n.starts_with("a-")
                    || n.starts_with("z-")
                    || n == "bad name"
                    || n == "."
                    || n == ".."
            },
        );
        assert_eq!(names, vec!["a-1".to_string(), "z-a".to_string()]);
    }
}

#[cfg(test)]
mod rediscover_stop_tests {
    use super::{
        connect_known_coord, open_coord_with_find_version, open_coord_with_keys,
        rediscover_known_coord, KnownCoord,
    };
    use crate::protocol::group::COORDINATOR_GROUP;
    use crate::ConsumerConfig;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn remembered_keys_sasl_timeout_closes_the_socket() {
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::{FIND_COORDINATOR, SASL_AUTHENTICATE, SASL_HANDSHAKE};
        use std::sync::atomic::AtomicUsize;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let open_flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 64];
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut cfg = ConsumerConfig::bootstrap([addr.clone()]);
        cfg.request_timeout = Duration::from_millis(200);
        cfg.connect_timeout = Duration::from_millis(200);
        cfg.max_wait_ms = 10;
        cfg.sasl_plain = Some(("user".into(), "pass".into()));
        let keys = [
            ApiVersion {
                api_key: FIND_COORDINATOR,
                min_version: 0,
                max_version: 6,
            },
            ApiVersion {
                api_key: SASL_HANDSHAKE,
                min_version: 0,
                max_version: 1,
            },
            ApiVersion {
                api_key: SASL_AUTHENTICATE,
                min_version: 0,
                max_version: 2,
            },
        ];
        let result = open_coord_with_keys(&cfg, &addr, &keys).await;
        assert!(matches!(result, Err(crate::Error::Timeout)));
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "remembered coordinator sasl left the socket open"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remembered_sasl_timeout_closes_the_socket() {
        use std::sync::atomic::AtomicUsize;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let open_flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 64];
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut cfg = ConsumerConfig::bootstrap([addr.clone()]);
        cfg.request_timeout = Duration::from_millis(200);
        cfg.connect_timeout = Duration::from_millis(200);
        cfg.max_wait_ms = 10;
        cfg.sasl_plain = Some(("user".into(), "pass".into()));
        let known = KnownCoord {
            addr: addr.clone(),
            offset_commit_version: 2,
            offset_fetch_version: 1,
            heartbeat_version: 0,
            sync_group_version: 0,
            join_group_version: 2,
            leave_group_version: 0,
            consumer_group_heartbeat_version: -1,
            share_group_heartbeat_version: -1,
            sasl_handshake_version: 1,
            sasl_authenticate_version: 2,
            find_coordinator_version: 1,
            bootstrap_keys: Vec::new(),
            address_keys: HashMap::new(),
        };
        let result = connect_known_coord(&cfg, &known).await;
        assert!(matches!(result, Err(crate::Error::Timeout)));
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "coordinator sasl left the socket open"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinator_handshake_timeout_closes_the_socket() {
        use std::sync::atomic::AtomicUsize;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let open_flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 64];
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut cfg = ConsumerConfig::bootstrap([addr.clone()]);
        cfg.request_timeout = Duration::from_millis(200);
        cfg.connect_timeout = Duration::from_millis(200);
        let result = open_coord_with_find_version(&cfg, &addr).await;
        assert!(
            matches!(result, Err(crate::Error::Timeout)),
            "a silent coordinator should time out the handshake"
        );
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "coordinator handshake left the socket blocked in a broker read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_lookup_finishes_the_speculative_coordinator_dial() {
        use tokio::io::AsyncReadExt;

        let lookup = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let lookup_addr = lookup.local_addr().unwrap().to_string();
        let lookup_peer = tokio::spawn(async move {
            let (mut sock, _) = lookup.accept().await.unwrap();
            let mut buf = [0u8; 64];
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let slow = listener.local_addr().unwrap().to_string();
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let peer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            flag.store(true, Ordering::SeqCst);
        });
        let mut cfg = ConsumerConfig::bootstrap([lookup_addr]);
        cfg.request_timeout = Duration::from_millis(200);
        cfg.connect_timeout = Duration::from_millis(200);
        let known = KnownCoord {
            addr: slow,
            offset_commit_version: 9,
            offset_fetch_version: 9,
            heartbeat_version: 4,
            sync_group_version: 5,
            join_group_version: 9,
            leave_group_version: 5,
            consumer_group_heartbeat_version: 1,
            share_group_heartbeat_version: 1,
            sasl_handshake_version: -1,
            sasl_authenticate_version: -1,
            find_coordinator_version: 1,
            bootstrap_keys: Vec::new(),
            address_keys: HashMap::new(),
        };
        let mut prior = HashMap::new();
        let result = rediscover_known_coord(&cfg, "g", COORDINATOR_GROUP, &known, &mut prior).await;
        assert!(result.is_err(), "the refused lookup should fail");
        assert!(
            dropped.load(Ordering::SeqCst),
            "lookup left the speculative coordinator dial blocked in a broker read"
        );
        peer.abort();
        lookup_peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_member_connect_finishes_the_coordinator_dial() {
        use super::connect_member_and_coordinator;
        use std::sync::atomic::AtomicUsize;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let open_flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let _started = open_flag.fetch_add(1, Ordering::SeqCst);
                let open_flag = Arc::clone(&open_flag);
                let _reader = tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => continue,
                        }
                    }
                    let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
        let mut cfg = ConsumerConfig::bootstrap([addr]);
        cfg.max_wait_ms = 10;
        cfg.request_timeout = Duration::from_millis(200);
        cfg.connect_timeout = Duration::from_millis(200);
        let result = connect_member_and_coordinator(
            &cfg,
            "g",
            &[],
            false,
            std::time::Instant::now() + Duration::from_millis(200),
        )
        .await;
        assert!(result.is_err(), "the stalled member connect should fail");
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "member setup left the coordinator dial blocked in a broker read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinator_discovery_timeout_closes_the_bootstrap_socket() {
        use super::discover_coord_within;
        use std::sync::atomic::AtomicUsize;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let open_flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let _started = open_flag.fetch_add(1, Ordering::SeqCst);
                let open_flag = Arc::clone(&open_flag);
                let _reader = tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => continue,
                        }
                    }
                    let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
        let mut cfg = ConsumerConfig::bootstrap([addr]);
        cfg.max_wait_ms = 10;
        cfg.request_timeout = Duration::from_millis(200);
        cfg.connect_timeout = Duration::from_millis(200);
        cfg.retry_backoff = Duration::ZERO;
        cfg.retry_backoff_max = Duration::ZERO;
        let result = discover_coord_within(
            &cfg,
            "g",
            COORDINATOR_GROUP,
            std::time::Instant::now() + Duration::from_millis(200),
        )
        .await;
        assert!(result.is_err(), "discovery should time out");
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "discovery left the bootstrap socket blocked in a broker read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacing_an_idle_coordinator_lets_the_broker_leave() {
        use super::coord_roundtrip_with_bootstrap;
        use crate::net::BrokerConn;
        use crate::protocol::api_keys::HEARTBEAT;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&flag);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                } else {
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                    });
                }
            }
        });
        let mut conn = BrokerConn::connect(&addr, "idle-coord", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        let cfg = ConsumerConfig::default().connections_max_idle(Duration::from_nanos(1));
        assert!(
            conn.idle_expired(cfg.connections_max_idle),
            "coordinator socket was not idle"
        );
        let mut prior = HashMap::new();
        let _sent = coord_roundtrip_with_bootstrap(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            HEARTBEAT,
            |_version, _buf| Ok(()),
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "replacing an idle coordinator left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacing_an_idle_offset_coordinator_lets_the_broker_leave() {
        use super::offsets::coord_offsets_v10;
        use crate::net::BrokerConn;
        use crate::protocol::api_keys::OFFSET_FETCH;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&flag);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                } else {
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                    });
                }
            }
        });
        let mut conn = BrokerConn::connect(&addr, "idle-offset", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        let cfg = ConsumerConfig::default().connections_max_idle(Duration::from_nanos(1));
        let mut prior = HashMap::new();
        let _fetched = coord_offsets_v10(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            |_buf| Ok(()),
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "replacing an idle offset coordinator left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave() {
        use super::offsets::coord_offsets_legacy;
        use crate::net::BrokerConn;
        use crate::protocol::api_keys::OFFSET_FETCH;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&flag);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                } else {
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                    });
                }
            }
        });
        let mut conn = BrokerConn::connect(&addr, "idle-legacy-offset", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        let cfg = ConsumerConfig::default().connections_max_idle(Duration::from_nanos(1));
        let mut prior = HashMap::new();
        let _fetched = coord_offsets_legacy(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            8,
            |_buf| Ok(()),
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "replacing an idle legacy offset coordinator left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_retriable_coordinator_error_lets_the_broker_leave() {
        use super::coord_roundtrip_with_bootstrap;
        use crate::net::BrokerConn;
        use crate::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
        use crate::protocol::api_keys::{API_VERSIONS, FIND_COORDINATOR, HEARTBEAT};
        use crate::protocol::group::encode_find_coordinator_response;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&flag);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut size_buf = [0u8; 4];
                        if socket.read_exact(&mut size_buf).await.is_ok() {
                            let size = i32::from_be_bytes(size_buf).max(0) as usize;
                            let mut request = vec![0u8; size];
                            if socket.read_exact(&mut request).await.is_ok() {
                                if let Some(bytes) = request.get(4..8) {
                                    let correlation = i32::from_be_bytes([
                                        bytes.first().copied().unwrap_or(0),
                                        bytes.get(1).copied().unwrap_or(0),
                                        bytes.get(2).copied().unwrap_or(0),
                                        bytes.get(3).copied().unwrap_or(0),
                                    ]);
                                    let mut response = Vec::new();
                                    response.extend_from_slice(&6i32.to_be_bytes());
                                    response.extend_from_slice(&correlation.to_be_bytes());
                                    response.extend_from_slice(&16i16.to_be_bytes());
                                    let _wrote = socket.write_all(&response).await;
                                }
                            }
                        }
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                    continue;
                }
                let port = i32::from(addr.port());
                let _reader = tokio::spawn(async move {
                    loop {
                        let mut size_buf = [0u8; 4];
                        if socket.read_exact(&mut size_buf).await.is_err() {
                            return;
                        }
                        let size = i32::from_be_bytes(size_buf).max(0) as usize;
                        let mut request = vec![0u8; size];
                        if socket.read_exact(&mut request).await.is_err() {
                            return;
                        }
                        let Some(api_bytes) = request.get(0..2) else {
                            return;
                        };
                        let api = i16::from_be_bytes([
                            api_bytes.first().copied().unwrap_or(0),
                            api_bytes.get(1).copied().unwrap_or(0),
                        ]);
                        let Some(corr_bytes) = request.get(4..8) else {
                            return;
                        };
                        let correlation = i32::from_be_bytes([
                            corr_bytes.first().copied().unwrap_or(0),
                            corr_bytes.get(1).copied().unwrap_or(0),
                            corr_bytes.get(2).copied().unwrap_or(0),
                            corr_bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut response = Vec::new();
                        if api == API_VERSIONS {
                            let mut body = bytes::BytesMut::new();
                            let encoded = encode_api_versions_response(
                                &mut body,
                                4,
                                &ApiVersionsResponse {
                                    error_code: 0,
                                    api_keys: vec![ApiVersion {
                                        api_key: FIND_COORDINATOR,
                                        min_version: 1,
                                        max_version: 1,
                                    }],
                                    throttle_time_ms: 0,
                                    supported_features: Vec::new(),
                                    finalized_features_epoch: None,
                                    finalized_features: Vec::new(),
                                    zk_migration_ready: false,
                                },
                            );
                            if encoded.is_err() {
                                return;
                            }
                            let len = i32::try_from(4 + body.len()).unwrap_or(0);
                            response.extend_from_slice(&len.to_be_bytes());
                            response.extend_from_slice(&correlation.to_be_bytes());
                            response.extend_from_slice(&body);
                        } else if api == FIND_COORDINATOR {
                            let mut body = bytes::BytesMut::new();
                            let encoded = encode_find_coordinator_response(
                                &mut body,
                                1,
                                1,
                                "127.0.0.1",
                                port,
                                "group",
                            );
                            if encoded.is_err() {
                                return;
                            }
                            let len = i32::try_from(4 + body.len()).unwrap_or(0);
                            response.extend_from_slice(&len.to_be_bytes());
                            response.extend_from_slice(&correlation.to_be_bytes());
                            response.extend_from_slice(&body);
                        } else {
                            return;
                        }
                        if socket.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let mut conn =
            BrokerConn::connect(&addr.to_string(), "retriable-coord", Duration::from_secs(2))
                .await
                .unwrap();
        conn.heartbeat_version = 0;
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut cfg = ConsumerConfig::bootstrap([addr.to_string()]);
        cfg.connections_max_idle = Duration::ZERO;
        cfg.request_timeout = Duration::from_secs(2);
        cfg.connect_timeout = Duration::from_secs(2);
        cfg.max_wait_ms = 10;
        let mut prior = HashMap::new();
        let _sent = coord_roundtrip_with_bootstrap(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            HEARTBEAT,
            |_version, _buf| Ok(()),
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a retriable coordinator error left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_coordinator_encode_lets_the_broker_leave() {
        use super::coord_roundtrip_with_bootstrap;
        use crate::net::BrokerConn;
        use crate::protocol::api_keys::HEARTBEAT;
        use std::io::ErrorKind;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&flag);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                } else {
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                    });
                }
            }
        });
        let mut conn = BrokerConn::connect(&addr, "encode-coord", Duration::from_secs(2))
            .await
            .unwrap();
        conn.heartbeat_version = 0;
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut cfg = ConsumerConfig::bootstrap([addr]);
        cfg.connections_max_idle = Duration::ZERO;
        cfg.request_timeout = Duration::from_secs(2);
        cfg.connect_timeout = Duration::from_secs(2);
        cfg.max_wait_ms = 10;
        let mut prior = HashMap::new();
        let _sent = coord_roundtrip_with_bootstrap(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            HEARTBEAT,
            |_version, _buf| {
                Err(crate::Error::Io(std::io::Error::new(
                    ErrorKind::ConnectionReset,
                    "coordinator encode failed",
                )))
            },
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a failed coordinator encode left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_offset_encode_lets_the_broker_leave() {
        use super::offsets::coord_offsets_legacy;
        use crate::net::BrokerConn;
        use crate::protocol::api_keys::OFFSET_FETCH;
        use std::io::ErrorKind;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        async fn count_first(listener: tokio::net::TcpListener, open: Arc<AtomicUsize>) {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&open);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                } else {
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                    });
                }
            }
        }

        let fail = || {
            Err(crate::Error::Io(std::io::Error::new(
                ErrorKind::ConnectionReset,
                "offset encode failed",
            )))
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let peer = tokio::spawn(count_first(listener, Arc::clone(&open)));
        let mut conn = BrokerConn::connect(&addr, "encode-offset", Duration::from_secs(2))
            .await
            .unwrap();
        conn.offset_fetch_version = 2;
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut cfg = ConsumerConfig::bootstrap([addr]);
        cfg.connections_max_idle = Duration::ZERO;
        cfg.request_timeout = Duration::from_secs(2);
        cfg.connect_timeout = Duration::from_secs(2);
        cfg.max_wait_ms = 10;
        let mut prior = HashMap::new();
        let _fetched = coord_offsets_legacy(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            2,
            |_buf| fail(),
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a failed offset encode left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_v10_offset_encode_lets_the_broker_leave() {
        use super::offsets::coord_offsets_v10;
        use crate::net::BrokerConn;
        use crate::protocol::api_keys::OFFSET_FETCH;
        use std::io::ErrorKind;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&flag);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                } else {
                    let _reader = tokio::spawn(async move {
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                    });
                }
            }
        });
        let mut conn = BrokerConn::connect(&addr, "encode-offset-v10", Duration::from_secs(2))
            .await
            .unwrap();
        conn.offset_fetch_version = 10;
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut cfg = ConsumerConfig::bootstrap([addr]);
        cfg.connections_max_idle = Duration::ZERO;
        cfg.request_timeout = Duration::from_secs(2);
        cfg.connect_timeout = Duration::from_secs(2);
        cfg.max_wait_ms = 10;
        let mut prior = HashMap::new();
        let _fetched = coord_offsets_v10(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            |_buf| {
                Err(crate::Error::Io(std::io::Error::new(
                    ErrorKind::ConnectionReset,
                    "v10 offset encode failed",
                )))
            },
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a failed v10 offset encode left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_retriable_offset_error_lets_the_broker_leave() {
        use super::offsets::coord_offsets_legacy;
        use crate::net::BrokerConn;
        use crate::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
        use crate::protocol::api_keys::{API_VERSIONS, FIND_COORDINATOR, OFFSET_FETCH};
        use crate::protocol::group::encode_find_coordinator_response;
        use crate::protocol::group::encode_offset_fetch_response;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&flag);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut size_buf = [0u8; 4];
                        if socket.read_exact(&mut size_buf).await.is_ok() {
                            let size = i32::from_be_bytes(size_buf).max(0) as usize;
                            let mut request = vec![0u8; size];
                            if socket.read_exact(&mut request).await.is_ok() {
                                if let Some(bytes) = request.get(4..8) {
                                    let correlation = i32::from_be_bytes([
                                        bytes.first().copied().unwrap_or(0),
                                        bytes.get(1).copied().unwrap_or(0),
                                        bytes.get(2).copied().unwrap_or(0),
                                        bytes.get(3).copied().unwrap_or(0),
                                    ]);
                                    let mut body = bytes::BytesMut::new();
                                    let encoded = encode_offset_fetch_response(
                                        &mut body,
                                        2,
                                        "group",
                                        &[],
                                        16,
                                    );
                                    if encoded.is_ok() {
                                        let mut response = Vec::new();
                                        let len = i32::try_from(4 + body.len()).unwrap_or(0);
                                        response.extend_from_slice(&len.to_be_bytes());
                                        response.extend_from_slice(&correlation.to_be_bytes());
                                        response.extend_from_slice(&body);
                                        let _wrote = socket.write_all(&response).await;
                                    }
                                }
                            }
                        }
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                    continue;
                }
                let port = i32::from(addr.port());
                let _reader = tokio::spawn(async move {
                    loop {
                        let mut size_buf = [0u8; 4];
                        if socket.read_exact(&mut size_buf).await.is_err() {
                            return;
                        }
                        let size = i32::from_be_bytes(size_buf).max(0) as usize;
                        let mut request = vec![0u8; size];
                        if socket.read_exact(&mut request).await.is_err() {
                            return;
                        }
                        let Some(api_bytes) = request.get(0..2) else {
                            return;
                        };
                        let api = i16::from_be_bytes([
                            api_bytes.first().copied().unwrap_or(0),
                            api_bytes.get(1).copied().unwrap_or(0),
                        ]);
                        let Some(corr_bytes) = request.get(4..8) else {
                            return;
                        };
                        let correlation = i32::from_be_bytes([
                            corr_bytes.first().copied().unwrap_or(0),
                            corr_bytes.get(1).copied().unwrap_or(0),
                            corr_bytes.get(2).copied().unwrap_or(0),
                            corr_bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut response = Vec::new();
                        if api == API_VERSIONS {
                            let mut body = bytes::BytesMut::new();
                            let encoded = encode_api_versions_response(
                                &mut body,
                                4,
                                &ApiVersionsResponse {
                                    error_code: 0,
                                    api_keys: vec![ApiVersion {
                                        api_key: FIND_COORDINATOR,
                                        min_version: 1,
                                        max_version: 1,
                                    }],
                                    throttle_time_ms: 0,
                                    supported_features: Vec::new(),
                                    finalized_features_epoch: None,
                                    finalized_features: Vec::new(),
                                    zk_migration_ready: false,
                                },
                            );
                            if encoded.is_err() {
                                return;
                            }
                            let len = i32::try_from(4 + body.len()).unwrap_or(0);
                            response.extend_from_slice(&len.to_be_bytes());
                            response.extend_from_slice(&correlation.to_be_bytes());
                            response.extend_from_slice(&body);
                        } else if api == FIND_COORDINATOR {
                            let mut body = bytes::BytesMut::new();
                            let encoded = encode_find_coordinator_response(
                                &mut body,
                                1,
                                1,
                                "127.0.0.1",
                                port,
                                "group",
                            );
                            if encoded.is_err() {
                                return;
                            }
                            let len = i32::try_from(4 + body.len()).unwrap_or(0);
                            response.extend_from_slice(&len.to_be_bytes());
                            response.extend_from_slice(&correlation.to_be_bytes());
                            response.extend_from_slice(&body);
                        } else {
                            return;
                        }
                        if socket.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let mut conn = BrokerConn::connect(
            &addr.to_string(),
            "retriable-offset",
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        conn.offset_fetch_version = 2;
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut cfg = ConsumerConfig::bootstrap([addr.to_string()]);
        cfg.connections_max_idle = Duration::ZERO;
        cfg.request_timeout = Duration::from_secs(2);
        cfg.connect_timeout = Duration::from_secs(2);
        cfg.max_wait_ms = 10;
        let mut prior = HashMap::new();
        let _fetched = coord_offsets_legacy(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            2,
            |_buf| Ok(()),
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a retriable offset error left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_retriable_v10_offset_error_lets_the_broker_leave() {
        use super::offsets::coord_offsets_v10;
        use crate::net::BrokerConn;
        use crate::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
        use crate::protocol::api_keys::{API_VERSIONS, FIND_COORDINATOR, OFFSET_FETCH};
        use crate::protocol::group::{
            encode_find_coordinator_response, encode_offset_fetch_response_data,
            OffsetFetchGroupResponseData, OffsetFetchResponseData, OffsetLimits,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    let flag = Arc::clone(&flag);
                    let _started = flag.fetch_add(1, Ordering::SeqCst);
                    let _reader = tokio::spawn(async move {
                        let mut size_buf = [0u8; 4];
                        if socket.read_exact(&mut size_buf).await.is_ok() {
                            let size = i32::from_be_bytes(size_buf).max(0) as usize;
                            let mut request = vec![0u8; size];
                            if socket.read_exact(&mut request).await.is_ok() {
                                if let Some(bytes) = request.get(4..8) {
                                    let correlation = i32::from_be_bytes([
                                        bytes.first().copied().unwrap_or(0),
                                        bytes.get(1).copied().unwrap_or(0),
                                        bytes.get(2).copied().unwrap_or(0),
                                        bytes.get(3).copied().unwrap_or(0),
                                    ]);
                                    let encoded = encode_offset_fetch_response_data(
                                        &OffsetFetchResponseData {
                                            groups: vec![OffsetFetchGroupResponseData {
                                                group_id: "group".to_string(),
                                                error_code: 16,
                                                ..OffsetFetchGroupResponseData::default()
                                            }],
                                            ..OffsetFetchResponseData::default()
                                        },
                                        10,
                                        OffsetLimits::default(),
                                    );
                                    if let Ok(body) = encoded {
                                        let mut response = Vec::new();
                                        let len = i32::try_from(5 + body.len()).unwrap_or(0);
                                        response.extend_from_slice(&len.to_be_bytes());
                                        response.extend_from_slice(&correlation.to_be_bytes());
                                        response.push(0);
                                        response.extend_from_slice(&body);
                                        let _wrote = socket.write_all(&response).await;
                                    }
                                }
                            }
                        }
                        let mut buf = [0u8; 8];
                        loop {
                            match socket.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => continue,
                            }
                        }
                        let _left = flag.fetch_sub(1, Ordering::SeqCst);
                    });
                    continue;
                }
                let port = i32::from(addr.port());
                let _reader = tokio::spawn(async move {
                    loop {
                        let mut size_buf = [0u8; 4];
                        if socket.read_exact(&mut size_buf).await.is_err() {
                            return;
                        }
                        let size = i32::from_be_bytes(size_buf).max(0) as usize;
                        let mut request = vec![0u8; size];
                        if socket.read_exact(&mut request).await.is_err() {
                            return;
                        }
                        let Some(api_bytes) = request.get(0..2) else {
                            return;
                        };
                        let api = i16::from_be_bytes([
                            api_bytes.first().copied().unwrap_or(0),
                            api_bytes.get(1).copied().unwrap_or(0),
                        ]);
                        let Some(corr_bytes) = request.get(4..8) else {
                            return;
                        };
                        let correlation = i32::from_be_bytes([
                            corr_bytes.first().copied().unwrap_or(0),
                            corr_bytes.get(1).copied().unwrap_or(0),
                            corr_bytes.get(2).copied().unwrap_or(0),
                            corr_bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut response = Vec::new();
                        if api == API_VERSIONS {
                            let mut body = bytes::BytesMut::new();
                            let encoded = encode_api_versions_response(
                                &mut body,
                                4,
                                &ApiVersionsResponse {
                                    error_code: 0,
                                    api_keys: vec![ApiVersion {
                                        api_key: FIND_COORDINATOR,
                                        min_version: 1,
                                        max_version: 1,
                                    }],
                                    throttle_time_ms: 0,
                                    supported_features: Vec::new(),
                                    finalized_features_epoch: None,
                                    finalized_features: Vec::new(),
                                    zk_migration_ready: false,
                                },
                            );
                            if encoded.is_err() {
                                return;
                            }
                            let len = i32::try_from(4 + body.len()).unwrap_or(0);
                            response.extend_from_slice(&len.to_be_bytes());
                            response.extend_from_slice(&correlation.to_be_bytes());
                            response.extend_from_slice(&body);
                        } else if api == FIND_COORDINATOR {
                            let mut body = bytes::BytesMut::new();
                            let encoded = encode_find_coordinator_response(
                                &mut body,
                                1,
                                1,
                                "127.0.0.1",
                                port,
                                "group",
                            );
                            if encoded.is_err() {
                                return;
                            }
                            let len = i32::try_from(4 + body.len()).unwrap_or(0);
                            response.extend_from_slice(&len.to_be_bytes());
                            response.extend_from_slice(&correlation.to_be_bytes());
                            response.extend_from_slice(&body);
                        } else {
                            return;
                        }
                        if socket.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let mut conn = BrokerConn::connect(
            &addr.to_string(),
            "retriable-offset-v10",
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        conn.offset_fetch_version = 10;
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut cfg = ConsumerConfig::bootstrap([addr.to_string()]);
        cfg.connections_max_idle = Duration::ZERO;
        cfg.request_timeout = Duration::from_secs(2);
        cfg.connect_timeout = Duration::from_secs(2);
        cfg.max_wait_ms = 10;
        let mut prior = HashMap::new();
        let _fetched = coord_offsets_v10(
            &mut conn,
            &cfg,
            "group",
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            |_buf| Ok(()),
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a retriable v10 offset error left the previous broker blocked in a read"
        );
        drop(conn);
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopping_a_finished_group_handshake_lets_the_broker_leave() {
        use super::finish_aborted_group_handshakes;
        use crate::net::BrokerConn;
        use crate::protocol::api::ApiVersionsResponse;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "group-handshake", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let versions = ApiVersionsResponse {
            error_code: 0,
            api_keys: Vec::new(),
            throttle_time_ms: 0,
            supported_features: Vec::new(),
            finalized_features_epoch: None,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
        };
        let mut set = tokio::task::JoinSet::<(
            usize,
            Result<(BrokerConn, i16, ApiVersionsResponse), crate::Error>,
        )>::new();
        let _ = set.spawn(async move { (0usize, Ok((conn, 1i16, versions))) });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        finish_aborted_group_handshakes(set).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "stopping a finished group handshake left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopping_a_group_heartbeat_lets_the_broker_leave() {
        use super::finish_heartbeat_task;
        use crate::net::BrokerConn;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "group-heartbeat", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let task = Arc::new(tokio::spawn(async move {
            let _hold = conn;
            std::future::pending::<()>().await;
        }));
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        finish_heartbeat_task(task, std::time::Instant::now()).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "stopping a group heartbeat left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_aborted_group_handshake_lets_the_broker_leave() {
        use super::{finish_aborted_group_handshakes, CoordHandshake};
        use crate::net::BrokerConn;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "abort-group", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut inflight = tokio::task::JoinSet::new();
        let _spawned = inflight.spawn(async move {
            let _hold = conn;
            std::future::pending::<(usize, crate::Result<CoordHandshake>)>().await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        finish_aborted_group_handshakes(inflight).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "an aborted group handshake left the broker blocked in a read"
        );
        peer.abort();
    }
}
