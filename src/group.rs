//! Consumer-group join / sync / heartbeat / commit.
mod offsets;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::watch;

use crate::config::{AutoOffsetReset, IsolationLevel};
use crate::consumer::{
    duration_millis_i32, Consumer, ConsumerConfig, ConsumerRecords, OffsetAndMetadata,
    TopicPartition,
};
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{ApiVersion, ApiVersionsResponse};
use crate::protocol::api_keys::{
    pick_version, CONSUMER_GROUP_HEARTBEAT, FIND_COORDINATOR, HEARTBEAT, JOIN_GROUP, LEAVE_GROUP,
    OFFSET_COMMIT, OFFSET_FETCH, SHARE_GROUP_HEARTBEAT, SYNC_GROUP,
};
use crate::protocol::cgheartbeat::{
    decode_consumer_group_heartbeat_response, encode_consumer_group_heartbeat_request,
    ConsumerGroupHeartbeatRequest, TopicPartitions,
};
use crate::protocol::group::{
    decode_assignment, decode_find_coordinator_response, decode_heartbeat_response,
    decode_join_group_response, decode_leave_group_response_version, decode_offset_commit_response,
    decode_offset_fetch_response, decode_sync_group_response,
    encode_find_coordinator_request_typed, encode_heartbeat_request,
    encode_join_group_protocols_request, encode_leave_group_request_members,
    encode_offset_commit_request, encode_offset_fetch_request, encode_sync_group_request,
    encode_tp_assignment, BorrowedOffsetPartition, BorrowedOffsetTopic, ConsumerProtocol,
    ConsumerProtocolSubscription, FetchedOffset, FetchedOffsetTopic, JoinGroupProtocol,
    JoinGroupProtocolsRequest, JoinGroupRequest, JoinGroupResponse, LeaveGroupMember,
    OffsetCommitRequestData, OffsetFetchRequestData, OffsetFetchTopic, OffsetPartition,
    OffsetTopic, SyncGroupRequest, Topic, COORDINATOR_GROUP, DEFAULT_RETENTION_TIME,
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
    for p in remaining {
        let target = out
            .iter()
            .min_by(|(left_member, left_parts), (right_member, right_parts)| {
                left_parts
                    .len()
                    .cmp(&right_parts.len())
                    .then_with(|| left_member.as_str().cmp(right_member.as_str()))
            })
            .map(|(member, _)| (*member).clone());
        if let Some(m) = target {
            if let Some(slot) = out.get_mut(&m) {
                slot.push(p);
            }
        }
    }
    rebalance_counts(&mut out);
    out
}

/// Move partitions until every member has `floor(n/m)` or `ceil(n/m)`.
fn rebalance_counts<T: Clone>(out: &mut HashMap<String, Vec<T>>) {
    loop {
        let max_id = out
            .iter()
            .max_by_key(|(m, v)| (v.len(), (*m).as_str()))
            .map(|(m, _)| m.clone());
        let min_id = out
            .iter()
            .min_by_key(|(m, v)| (v.len(), (*m).as_str()))
            .map(|(m, _)| m.clone());
        let (Some(max_id), Some(min_id)) = (max_id, min_id) else {
            break;
        };
        if max_id == min_id {
            break;
        }
        let max_n = out.get(&max_id).map(Vec::len).unwrap_or(0);
        let min_n = out.get(&min_id).map(Vec::len).unwrap_or(0);
        if max_n <= min_n.saturating_add(1) {
            break;
        }
        let Some(tp) = out.get_mut(&max_id).and_then(Vec::pop) else {
            break;
        };
        if let Some(slot) = out.get_mut(&min_id) {
            slot.push(tp);
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

/// `(member_id, owned topic-partitions, generation_id)` for
/// [`resolve_sticky_owned_partitions`].
type StickyOwnedClaim = (String, Vec<(String, i32)>, i32);

#[derive(Clone, Copy)]
struct StickyOwner<'a> {
    member: &'a str,
    generation: i32,
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
    let mut out: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for (member, _, _) in claims {
        if !out.contains_key(member.as_str()) {
            let _ = out.insert(member.clone(), Vec::new());
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
                        slot.push((topic.clone(), *partition));
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
                                other_slot
                                    .retain(|(name, part)| name != topic || *part != *partition);
                            }
                            remember_sticky_owner(&mut owner, topic.as_str(), *partition, claim);
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        if let Some(other_slot) = out.get_mut(other) {
                            other_slot.retain(|(name, part)| name != topic || *part != *partition);
                        }
                        if let Some(slot) = out.get_mut(member.as_str()) {
                            slot.push((topic.clone(), *partition));
                        }
                        remember_sticky_owner(&mut owner, topic.as_str(), *partition, claim);
                    }
                    std::cmp::Ordering::Less => {}
                },
            }
        }
    }
    out
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

/// Cooperative-sticky across several topics (KIP-429).
pub fn assign_cooperative_sticky_subscribed(
    member_subs: &[(String, Vec<String>)],
    topics: &[(String, Vec<i32>)],
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<String, Vec<(String, i32)>> {
    cooperative_filter(assign_sticky_subscribed(member_subs, topics, prev), prev)
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
        let (consumer, coord) = connect_member_and_coordinator(
            &cfg,
            &group_id,
            &topics,
            topic_match.is_some(),
            started,
        )
        .await?;
        let coord = coord?;

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
        g.spawn_heartbeat(hb_rx);
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
        let (consumer, coord) = connect_member_and_coordinator(
            &cfg,
            &group_id,
            &topics,
            topic_match.is_some(),
            started,
        )
        .await?;
        let coord = coord?;
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
        g.spawn_heartbeat_consumer(hb_rx);
        Ok(g)
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
        let assigned = self.assignment();
        self.committed_for(assigned).await
    }

    /// [`Self::committed`] with a one-shot timeout (Java `committed(Duration)`).
    pub async fn committed_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, OffsetAndMetadata)>> {
        let assigned = self.assignment();
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
        refill_offset_fetch_topics(
            &mut topics,
            partitions
                .iter()
                .map(|tp| (tp.topic.as_str(), tp.partition)),
        );
        let fetched = self
            .offset_fetch(&topics, timeout.saturating_sub(started.elapsed()))
            .await;
        self.classic_fetch_topics = topics;
        let fetched = fetched?;
        check_fetched_offsets(&fetched)?;
        Ok(partitions
            .into_iter()
            .map(|tp| {
                let md = fetched_committed(&fetched, &tp.topic, tp.partition)
                    .map(offset_and_metadata_from)
                    .unwrap_or_else(|| OffsetAndMetadata::new(-1));
                (tp, md)
            })
            .collect())
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
        let (version, body) = coord_roundtrip_with_bootstrap(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            |version, buf| {
                encode_offset_fetch_request(
                    buf,
                    version,
                    &self.group_id,
                    member_id,
                    member_epoch,
                    require_stable,
                    Some(topics),
                )
            },
            &bootstrap_keys,
            &mut self.prior_coords,
            timeout,
        )
        .await?;
        decode_offset_fetch_response(&mut body.clone(), version)
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
                self.consumer.restore_fetched(recs);
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
        let offsets = crate::offsets::collect_bounded_commits(self.assigned_commit_offsets())?;
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
            refill_shared_offset_topics(&mut topics, positions, |topic, partition| {
                self.consumer.position_epoch(topic, partition)
            });
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
            refill_offset_topics(&mut topics, offsets);
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
        let (version, body) = coord_roundtrip_with_bootstrap(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            OFFSET_COMMIT,
            |version, buf| {
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
            },
            &bootstrap_keys,
            &mut self.prior_coords,
            timeout,
        )
        .await?;
        let err = decode_offset_commit_response(&mut body.clone(), version)?;
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
        refill_shared_offset_topics(&mut topics, &to_commit, |topic, partition| {
            self.consumer.position_epoch(topic, partition)
        });
        if topics.is_empty() {
            self.classic_commit_topics = topics;
            return self.fetch_for_poll(wait).await;
        }
        let generation_id = self.generation_id;
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let instance = self.cfg.group_instance_id.clone();
        let coord = &mut self.coord;
        let cfg = &self.cfg;
        let consumer = &mut self.consumer;
        let prior = &mut self.prior_coords;
        let (committed, fetched) = tokio::join!(
            coord_roundtrip(
                coord,
                cfg,
                &group_id,
                COORDINATOR_GROUP,
                OFFSET_COMMIT,
                |version, buf| {
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
                },
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
                    self.consumer.restore_fetched(recs);
                }
                return Err(error);
            }
        };
        let err = match decode_offset_commit_response(&mut body.clone(), version) {
            Ok(err) => err,
            Err(error) => {
                if let Ok(recs) = fetched {
                    self.consumer.restore_fetched(recs);
                }
                return Err(error);
            }
        };
        self.note_commit(err == 0);
        if err != 0 {
            if let Ok(recs) = fetched {
                self.consumer.restore_fetched(recs);
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
                let owned: Vec<(TopicPartition, OffsetAndMetadata)> = to_commit
                    .iter()
                    .map(|(topic, partition, offset)| {
                        let epoch = self.consumer.position_epoch(topic, *partition);
                        (
                            TopicPartition::new(topic.as_ref(), *partition),
                            OffsetAndMetadata::from_wire(*offset, epoch, String::new()),
                        )
                    })
                    .collect();
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
        if self.member_id.is_empty() {
            self.topics.clear();
            self.publish_hb_topics();
            self.topic_match = None;
            self.consumer.clear_assignment();
            return Ok(());
        }
        let deadline = Instant::now() + self.cfg.request_timeout;
        self.flush_async_commits(deadline).await;
        // KL-02: do not auto-commit on unsubscribe — poll-interval auto-commit
        // and explicit commit* are the only OffsetCommit paths for positions.
        self.hb_stop.send(true).unwrap_or(());
        *self.hb_deadline.lock() = None;
        let revoked = self.assignment();
        if self.kip848 {
            self.adopt_kip848_member_id();
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        let kip848 = self.kip848;
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
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
        self.publish_hb_topics();
        self.topic_match = None;
        self.member_id.clear();
        self.generation_id = 0;
        self.hb_generation.store(0, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        *self.hb_assignment.lock() = None;
        *self.hb_ack.lock() = None;
        self.prev_assignment.clear();
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
                let to_commit = self.assigned_commit_offsets();
                if !to_commit.is_empty() {
                    let timeout = deadline.saturating_duration_since(Instant::now());
                    if timeout.is_zero() {
                        return Err(Error::Timeout);
                    }
                    let stale = topics
                        .iter()
                        .any(|topic| !self.consumer.topic_metadata_fresh(topic));
                    if stale {
                        self.commit_offsets_while_refreshing(&to_commit, &topics, timeout)
                            .await?;
                    } else {
                        self.commit_metadata_offsets(to_commit, timeout).await?;
                    }
                }
            }
            let revoked = self.assignment();
            self.consumer.clear_assignment();
            if !revoked.is_empty() {
                self.cfg.rebalance.call(&revoked, &[]);
            }
            self.prev_assignment.clear();
        }
        self.topics = topics;
        self.publish_hb_topics();
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
            self.spawn_heartbeat_consumer(hb_rx);
        } else {
            self.rejoin_within(deadline).await?;
            self.spawn_heartbeat(hb_rx);
        }
        Ok(())
    }

    async fn matching_topic_names_within(&mut self, deadline: Instant) -> Result<Vec<String>> {
        let Some(pred) = self.topic_match.clone() else {
            return Ok(self.topics.clone());
        };
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let infos = self.consumer.list_topics_timeout(timeout).await?;
        Ok(filter_matching_topics(
            infos.iter().map(|i| i.topic.as_str()),
            |n| pred(n),
        ))
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
        if self.member_id.is_empty() {
            self.hb_stop.send(true).unwrap_or(());
            *self.hb_deadline.lock() = None;
            self.consumer.close_interceptors();
            self.consumer
                .close_fetch_sessions(deadline.saturating_duration_since(Instant::now()))
                .await;
            return Ok(());
        }
        self.flush_async_commits(deadline).await;
        // KL-02: never auto-commit positions on leave/close.
        self.hb_stop.send(true).unwrap_or(());
        *self.hb_deadline.lock() = None;
        if self.kip848 {
            self.adopt_kip848_member_id();
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        let kip848 = self.kip848;
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
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
                prior,
                deadline,
            ),
            consumer.close_fetch_sessions(timeout),
        );
        self.consumer.close_interceptors();
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
            let (version, body) = coord_roundtrip(
                coord,
                cfg,
                group_id,
                COORDINATOR_GROUP,
                CONSUMER_GROUP_HEARTBEAT,
                |version, buf| encode_consumer_group_heartbeat_request(buf, version, &req),
                prior,
                timeout,
            )
            .await?;
            let resp = decode_consumer_group_heartbeat_response(&mut body.clone(), version)?;
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
        let (version, body) = coord_roundtrip(
            coord,
            cfg,
            group_id,
            COORDINATOR_GROUP,
            LEAVE_GROUP,
            |version, buf| encode_leave_group_request_members(buf, version, group_id, &members),
            prior,
            timeout,
        )
        .await?;
        let (err, results, ..) = decode_leave_group_response_version(&mut body.clone(), version)?;
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

    fn join_protocol_entries(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let owned: Vec<(String, i32)> = self.assignment().into_iter().map(Into::into).collect();
        let names = if self.assignors.is_empty() {
            vec![self.protocol.clone()]
        } else {
            self.assignors.clone()
        };
        let metadata = ConsumerProtocol::serialize_subscription(&ConsumerProtocolSubscription {
            topics: self.topics.clone(),
            owned_partitions: owned,
            generation_id: self.generation_id,
            rack_id: self.cfg.rack.clone(),
        })?;
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            out.push((name, metadata.clone()));
        }
        Ok(out)
    }

    async fn rejoin_once(&mut self, deadline: Instant) -> Result<Vec<(String, i32)>> {
        let entries = self.join_protocol_entries()?;
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
            let prefetch = metadata_prefetch.is_none()
                && self
                    .topics
                    .iter()
                    .any(|topic| !self.consumer.topic_metadata_fresh(topic));
            let (version, body) = if prefetch {
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
                let rpc = coord_roundtrip(
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
                    prior,
                    timeout,
                );
                let (refresh, rpc) = tokio::join!(refresh, rpc);
                metadata_prefetch = Some(refresh);
                rpc?
            } else {
                coord_roundtrip(
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
                    &mut self.prior_coords,
                    timeout,
                )
                .await?
            };
            let decoded = decode_join_group_response(&mut body.clone(), version)
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
        let mut topic_set = self.topics.clone();
        for m in &members {
            let sub = match ConsumerProtocol::deserialize_subscription(&m.metadata) {
                Ok(s) if !s.topics.is_empty() => s,
                Ok(s) => ConsumerProtocolSubscription {
                    topics: self.topics.clone(),
                    owned_partitions: s.owned_partitions,
                    generation_id: s.generation_id,
                    rack_id: s.rack_id,
                },
                Err(_) => ConsumerProtocolSubscription::new(self.topics.clone()),
            };
            for t in &sub.topics {
                remember_unique_topic(&mut topic_set, t);
            }
            member_subs.push((m.member_id.clone(), sub.topics));
            owned_claims.push((m.member_id.clone(), sub.owned_partitions, sub.generation_id));
        }
        let owned_prev = resolve_sticky_owned_partitions(&owned_claims);
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        // A rejoin of topics already in the metadata image does not ask again.
        // Any missing or stale name refreshes the whole set: a subset response
        // would replace the image `partition_ids` reads.
        let mut by_topic = Vec::with_capacity(topic_set.len());
        let mut loaded = true;
        for topic in &topic_set {
            if !self.consumer.topic_metadata_fresh(topic) {
                loaded = false;
                break;
            }
            match self.consumer.partition_ids(topic) {
                Ok(ids) => by_topic.push((topic.clone(), ids)),
                Err(_) => {
                    loaded = false;
                    break;
                }
            }
        }
        if !loaded {
            self.consumer
                .refresh_metadata_timeout(Some(&topic_set), timeout)
                .await?;
            by_topic.clear();
            for topic in &topic_set {
                by_topic.push((topic.clone(), self.consumer.partition_ids(topic)?));
            }
        }
        let assigning = JoinGroupResponse::is_leader(&self.member_id, &leader) && !skip_assignment;
        let assignments = if assigning {
            let map = match self.protocol.as_str() {
                "cooperative-sticky" => {
                    assign_cooperative_sticky_subscribed(&member_subs, &by_topic, &owned_prev)
                }
                "sticky" => assign_sticky_subscribed(&member_subs, &by_topic, &owned_prev),
                _ => assign_range_subscribed(&member_subs, &by_topic),
            };
            self.prev_assignment = map.clone();
            map.into_iter()
                .map(|(id, tps)| Ok((id, encode_tp_assignment(&tps)?)))
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        // The assigning leader already chose its partitions. Dial those
        // brokers while SyncGroup runs so offset fetch and the next poll
        // do not pay the handshake after the coordinator answers.
        let mut leaders = Vec::new();
        if assigning {
            if let Some(parts) = self.prev_assignment.get(&self.member_id) {
                for (topic, partition) in parts {
                    let Ok(node) = self.consumer.cluster().leader_id(topic, *partition) else {
                        continue;
                    };
                    if !leaders.contains(&node) {
                        leaders.push(node);
                    }
                }
            }
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let mut flight = if leaders.is_empty() {
            None
        } else {
            Some(self.consumer.start_leader_dials(&leaders, deadline))
        };
        let sync_fut = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            SYNC_GROUP,
            |version, buf| {
                encode_sync_group_request(
                    buf,
                    version,
                    &SyncGroupRequest {
                        group_id: &self.group_id,
                        generation_id: self.generation_id,
                        member_id: &self.member_id,
                        group_instance_id: self.cfg.group_instance_id.as_deref(),
                        protocol_type: ConsumerProtocol::PROTOCOL_TYPE,
                        protocol_name: &self.protocol,
                        assignments: &assignments,
                    },
                )
            },
            &mut self.prior_coords,
            timeout,
        );
        let synced = if let Some(flight) = flight.as_mut() {
            let (synced, ()) = tokio::join!(sync_fut, flight);
            synced
        } else {
            sync_fut.await
        };
        let (version, body) = synced?;
        let (err, assignment, ..) = decode_sync_group_response(&mut body.clone(), version)
            .map_err(|e| group_decode_context(e, "SyncGroup", version, body.len()))?;
        if err != 0 {
            return Err(Error::broker(err, "SyncGroup"));
        }
        if let Some(flight) = flight {
            flight.store(&mut self.consumer);
        }
        let assigned = decode_assignment(&assignment).map_err(|e| {
            group_decode_context(e, "ConsumerProtocol assignment", version, assignment.len())
        })?;
        let wanted = expand_topic_partitions(assigned);
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
            subscribed_topic_names: Some(self.topics.clone()),
            subscribed_topic_regex: None,
            server_assignor: None,
            // Broker rejects null TopicPartitions on join ("must be empty when
            // (re-)joining"); send an empty array instead of null/unchanged.
            topic_partitions: Some(Vec::new()),
        };
        // Topic names are already on the heartbeat. Metadata only has to be
        // ready when the assignment is resolved, so a stale image loads while
        // the coordinator join is in flight.
        let stale = self
            .topics
            .iter()
            .any(|topic| !self.consumer.topic_metadata_fresh(topic));
        let mut metadata_refresh = None;
        let (version, body) = if stale {
            let group_id = self.group_id.as_str();
            let coord = &mut self.coord;
            let cfg = &self.cfg;
            let consumer = &mut self.consumer;
            let topics = &self.topics;
            let prior = &mut self.prior_coords;
            let refresh = consumer.refresh_metadata_timeout(Some(topics), timeout);
            let rpc = coord_roundtrip(
                coord,
                cfg,
                group_id,
                COORDINATOR_GROUP,
                CONSUMER_GROUP_HEARTBEAT,
                |version, buf| encode_consumer_group_heartbeat_request(buf, version, &req),
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
            coord_roundtrip(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
                CONSUMER_GROUP_HEARTBEAT,
                |version, buf| encode_consumer_group_heartbeat_request(buf, version, &req),
                &mut self.prior_coords,
                timeout,
            )
            .await?
        };
        let resp = decode_consumer_group_heartbeat_response(&mut body.clone(), version)?;
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
            && assignment
                .iter()
                .any(|topic| !names.contains_key(&topic.topic_id))
        {
            return Err(Error::protocol(
                "group assignment contains an unknown topic UUID",
            ));
        }
        let wanted = wanted_from_kip848(&self.topics, names, &assignment);
        let _ = self.assign_committed(&wanted, deadline, false).await?;
        *self.hb_ack.lock() = Some(assignment);
        self.hb_generation
            .store(self.generation_id, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn publish_hb_topics(&self) {
        *self.hb_topics.lock() = self.topics.clone();
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
        let missing_topic = {
            let names = self.consumer.topic_id_names();
            assignment
                .iter()
                .any(|topic| !names.contains_key(&topic.topic_id))
        };
        let stale = self
            .topics
            .iter()
            .any(|topic| !self.consumer.topic_metadata_fresh(topic));
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
            && assignment
                .iter()
                .any(|topic| !names.contains_key(&topic.topic_id))
        {
            return Err(Error::protocol(
                "group assignment contains an unknown topic UUID",
            ));
        }
        let wanted = wanted_from_kip848(&self.topics, names, &assignment);
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

    async fn assign_committed(
        &mut self,
        wanted: &[(String, i32)],
        deadline: Instant,
        overlap_metadata: bool,
    ) -> Result<Vec<(String, i32)>> {
        let identities = self.consumer.topic_name_ids().clone();
        let all_current = self.consumer.assigned_offsets().to_vec();
        let mut current: HashMap<String, HashMap<i32, i64>> = HashMap::new();
        for (topic, partition, offset) in &all_current {
            if self.consumer.assigned_topic_id(topic.as_ref(), *partition)
                != identities
                    .get(topic.as_ref())
                    .copied()
                    .filter(|id| *id != [0; 16])
            {
                continue;
            }
            remember_i64(&mut current, topic.as_ref(), *partition, *offset);
        }
        let mut prev: HashMap<String, HashSet<i32>> = HashMap::new();
        for (topic, partition, _) in &all_current {
            remember_partition(&mut prev, topic.as_ref(), *partition);
        }
        let mut next: HashMap<String, HashSet<i32>> = HashMap::new();
        for (topic, partition) in wanted {
            remember_partition(&mut next, topic, *partition);
        }
        let mut revoked = Vec::new();
        for (topic, parts) in &prev {
            for partition in parts {
                let still_wanted = next.get(topic).is_some_and(|kept| kept.contains(partition));
                let same_identity = nested_i64(&current, topic, *partition).is_some();
                if !still_wanted || !same_identity {
                    revoked.push((topic.clone(), *partition));
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
            let revoked_parts = index_revoked(&revoked);
            let to_commit: Vec<(TopicPartition, OffsetAndMetadata)> = self
                .consumer
                .positions()
                .into_iter()
                .filter(|(tp, offset)| {
                    position_is_committable(*offset)
                        && revoked_contains(&revoked_parts, &tp.topic, tp.partition)
                })
                .map(|(tp, offset)| {
                    let epoch = self.consumer.position_epoch(&tp.topic, tp.partition);
                    (
                        tp,
                        OffsetAndMetadata::from_wire(offset, epoch, String::new()),
                    )
                })
                .collect();
            if !to_commit.is_empty() {
                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    return Err(Error::Timeout);
                }
                self.commit_metadata_offsets(to_commit, timeout).await?;
            }
        }
        let mut kept_epochs: HashMap<String, HashMap<i32, i32>> = HashMap::new();
        for (topic, partition) in wanted {
            if nested_i64(&current, topic, *partition).is_none() {
                continue;
            }
            remember_i32(
                &mut kept_epochs,
                topic,
                *partition,
                self.consumer.last_fetched_epoch(topic, *partition),
            );
        }
        let added: Vec<(String, i32)> = wanted
            .iter()
            .filter(|(topic, partition)| nested_i64(&current, topic, *partition).is_none())
            .cloned()
            .collect();
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
            refill_offset_fetch_topics(
                &mut topics,
                added
                    .iter()
                    .map(|(topic, partition)| (topic.as_str(), *partition)),
            );
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
                for (topic, partition) in &added {
                    let Ok(node) = self.consumer.cluster().leader_id(topic, *partition) else {
                        continue;
                    };
                    if !leaders.contains(&node) {
                        leaders.push(node);
                    }
                }
                let mut dials = self.consumer.start_leader_dials(&leaders, deadline);
                let require_stable = self.cfg.isolation_level == IsolationLevel::ReadCommitted;
                let member_epoch = if self.kip848 { self.generation_id } else { -1 };
                let member_id = if self.kip848 {
                    Some(self.member_id.clone())
                } else {
                    None
                };
                let group_id = self.group_id.clone();
                let fetched = {
                    let refresh_fut = self
                        .consumer
                        .refresh_metadata_timeout(Some(&self.topics), timeout);
                    let fetch_fut = coord_roundtrip(
                        &mut self.coord,
                        &self.cfg,
                        &group_id,
                        COORDINATOR_GROUP,
                        OFFSET_FETCH,
                        |version, buf| {
                            encode_offset_fetch_request(
                                buf,
                                version,
                                &group_id,
                                member_id.as_deref(),
                                member_epoch,
                                require_stable,
                                Some(&topics),
                            )
                        },
                        &mut self.prior_coords,
                        timeout,
                    );
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
                    let (version, body) = fetched_rpc?;
                    let fetched = decode_offset_fetch_response(&mut body.clone(), version)
                        .map_err(|e| group_decode_context(e, "OffsetFetch", version, body.len()))?;
                    if let Some(Err(error)) = metadata_refresh {
                        return Err(error);
                    }
                    let needs_reset = added.iter().any(|(topic, partition)| {
                        fetched_committed(&fetched, topic, *partition)
                            .map(|part| part.offset)
                            .unwrap_or(OffsetAndMetadata::INVALID_OFFSET)
                            < 0
                    });
                    if needs_reset && !warm_leaders {
                        (&mut dials).await;
                        warm_leaders = true;
                    }
                    fetched
                };
                if warm_leaders {
                    dials.store(&mut self.consumer);
                }
                self.classic_fetch_topics = topics;
                fetched
            } else {
                let mut leaders = Vec::new();
                for (topic, partition) in &added {
                    let Ok(node) = self.consumer.cluster().leader_id(topic, *partition) else {
                        continue;
                    };
                    if !leaders.contains(&node) {
                        leaders.push(node);
                    }
                }
                let require_stable = self.cfg.isolation_level == IsolationLevel::ReadCommitted;
                let member_epoch = if self.kip848 { self.generation_id } else { -1 };
                let member_id = if self.kip848 {
                    Some(self.member_id.clone())
                } else {
                    None
                };
                let group_id = self.group_id.clone();
                let coord = &mut self.coord;
                let cfg = &self.cfg;
                let consumer = &mut self.consumer;
                let prior = &mut self.prior_coords;
                let fetched = {
                    let fetch_fut = coord_roundtrip(
                        coord,
                        cfg,
                        &group_id,
                        COORDINATOR_GROUP,
                        OFFSET_FETCH,
                        |version, buf| {
                            encode_offset_fetch_request(
                                buf,
                                version,
                                &group_id,
                                member_id.as_deref(),
                                member_epoch,
                                require_stable,
                                Some(&topics),
                            )
                        },
                        prior,
                        timeout,
                    );
                    let warm_fut = consumer.warm_leader_conns(&leaders, deadline);
                    tokio::pin!(fetch_fut);
                    tokio::pin!(warm_fut);
                    let fetched_rpc = tokio::select! {
                        biased;
                        result = &mut fetch_fut => result,
                        _ = &mut warm_fut => {
                            warm_leaders = true;
                            (&mut fetch_fut).await
                        }
                    };
                    let (version, body) = fetched_rpc?;
                    let fetched = decode_offset_fetch_response(&mut body.clone(), version)
                        .map_err(|e| group_decode_context(e, "OffsetFetch", version, body.len()))?;
                    let needs_reset = added.iter().any(|(topic, partition)| {
                        fetched_committed(&fetched, topic, *partition)
                            .map(|part| part.offset)
                            .unwrap_or(OffsetAndMetadata::INVALID_OFFSET)
                            < 0
                    });
                    if needs_reset && !warm_leaders {
                        warm_fut.await;
                    }
                    fetched
                };
                self.classic_fetch_topics = topics;
                fetched
            }
        };
        check_fetched_offsets(&fetched)?;
        let mut starts: Vec<Option<(String, i32, i64)>> = Vec::with_capacity(wanted.len());
        let mut reset_slots = Vec::new();
        for (topic, part) in wanted {
            if let Some(offset) = nested_i64(&current, topic, *part) {
                starts.push(Some((topic.clone(), *part, offset)));
                continue;
            }
            let committed = fetched_committed(&fetched, topic, *part)
                .map(|part| part.offset)
                .unwrap_or(OffsetAndMetadata::INVALID_OFFSET);
            if committed >= 0 {
                starts.push(Some((topic.clone(), *part, committed)));
                continue;
            }
            if matches!(reset, AutoOffsetReset::None) {
                return Err(Error::protocol(format!(
                    "no committed offset for {topic}-{part}"
                )));
            }
            reset_slots.push(starts.len());
            starts.push(None);
        }
        if !reset_slots.is_empty() {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let mut partitions = Vec::with_capacity(reset_slots.len());
            for index in &reset_slots {
                let Some((topic, part)) = wanted.get(*index) else {
                    return Err(Error::protocol("ListOffsets returned no reset offset"));
                };
                partitions.push(TopicPartition::new(topic.as_str(), *part));
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
            }
        }
        let starts: Vec<(String, i32, i64)> = starts.into_iter().flatten().collect();
        self.consumer.assign_all(&starts).await?;
        if wanted.iter().any(|(topic, partition)| {
            self.consumer.assigned_topic_id(topic, *partition)
                != identities
                    .get(topic.as_str())
                    .copied()
                    .filter(|id| *id != [0; 16])
        }) {
            self.consumer.clear_assignment();
            return Err(Error::protocol(
                "topic identity changed while assigning committed offsets",
            ));
        }
        for (topic, part) in wanted {
            if let Some(epoch) = nested_i32(&kept_epochs, topic, *part) {
                self.consumer.set_last_fetched_epoch(topic, *part, epoch);
            } else if let Some(found) = fetched_committed(&fetched, topic, *part) {
                self.consumer
                    .set_last_fetched_epoch(topic, *part, found.leader_epoch);
            }
        }
        let added_tps: Vec<(String, i32)> = next
            .iter()
            .flat_map(|(topic, parts)| {
                parts
                    .iter()
                    .filter(|partition| nested_i64(&current, topic, **partition).is_none())
                    .map(|partition| (topic.clone(), *partition))
            })
            .collect();
        if !revoked.is_empty() || !added_tps.is_empty() {
            self.cfg.rebalance.call(
                &TopicPartition::list_from(&revoked),
                &TopicPartition::list_from(&added_tps),
            );
        }
        Ok(revoked)
    }

    fn spawn_heartbeat_consumer(&self, mut stop: watch::Receiver<bool>) {
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
        drop(tokio::spawn(async move {
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
                tokio::select! {
                    res = stop.changed() => {
                        if res.is_err() || *stop.borrow() {
                            *hb_deadline.lock() = None;
                            break;
                        }
                    }
                    _ = tokio::time::sleep(sleep_duration) => {
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
                        let joining =
                            epoch == ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH;
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
                            conn = None;
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
                            reuse_topic_names(
                                &mut heartbeat.subscribed_topic_names,
                                &hb_topics.lock(),
                            );
                            if heartbeat
                                .topic_partitions
                                .as_ref()
                                .is_none_or(|parts| !parts.is_empty())
                            {
                                heartbeat.topic_partitions = Some(Vec::new());
                            }
                        } else {
                            heartbeat.subscribed_topic_names = None;
                            reuse_topic_partitions(
                                &mut heartbeat.topic_partitions,
                                &hb_ack.lock(),
                            );
                        }
                        let res = c
                            .roundtrip(
                                CONSUMER_GROUP_HEARTBEAT,
                                version,
                                |buf| {
                                    encode_consumer_group_heartbeat_request(
                                        buf,
                                        version,
                                        &heartbeat,
                                    )
                                },
                                timeout,
                            )
                            .await;
                        match res {
                            Ok(body) => {
                                if let Ok(resp) = decode_consumer_group_heartbeat_response(
                                    &mut body.clone(),
                                    version,
                                ) {
                                    if error::coordinator_retriable(resp.error_code) {
                                        if let Some(current) = conn.as_ref() {
                                            remembered = Some(
                                                KnownCoord::capture(current)
                                                    .with_bootstrap_keys(&bootstrap_keys),
                                            );
                                        }
                                        conn = None;
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
                                            } else if let Ok(ms_u64) = u64::try_from(resp.heartbeat_interval_ms) {
                                                current_interval = Duration::from_millis(ms_u64);
                                                hb_interval_ms.store(resp.heartbeat_interval_ms, Ordering::SeqCst);
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
                                                .with_bootstrap_keys(&bootstrap_keys),
                                        );
                                    }
                                    hb_err.store(error::INVALID_REQUEST, Ordering::SeqCst);
                                    conn = None;
                                    let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                                    next_hb_deadline = Instant::now() + retry_delay;
                                    *hb_deadline.lock() = Some(next_hb_deadline);
                                }
                            }
                            Err(_) => {
                                if let Some(current) = conn.as_ref() {
                                    remembered = Some(
                                        KnownCoord::capture(current)
                                            .with_bootstrap_keys(&bootstrap_keys),
                                    );
                                }
                                conn = None;
                                let retry_delay = cfg.retry_backoff.max(Duration::from_millis(50));
                                next_hb_deadline = Instant::now() + retry_delay;
                                *hb_deadline.lock() = Some(next_hb_deadline);
                            }
                        }
                    }
                }
            }
        }));
    }

    fn spawn_heartbeat(&self, mut stop: watch::Receiver<bool>) {
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_generation = self.hb_generation.clone();
        let last_poll = self.last_poll.clone();
        let left_max_poll = self.left_max_poll.clone();
        let cfg = self.cfg.clone();
        let bootstrap_keys = self.consumer.copy_api_keys();
        drop(tokio::spawn(async move {
            let mut conn: Option<BrokerConn> = None;
            let mut remembered: Option<KnownCoord> = None;
            let mut prior_coords = HashMap::new();
            let mut tick =
                tokio::time::interval(cfg.heartbeat_interval.max(Duration::from_millis(1)));
            loop {
                tokio::select! {
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                    _ = tick.tick() => {
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
                            conn = None;
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
                                if let Ok((err, ..)) =
                                    decode_heartbeat_response(&mut body.clone(), version)
                                {
                                    if error::coordinator_retriable(err) {
                                        if let Some(current) = conn.as_ref() {
                                            remembered = Some(
                                                KnownCoord::capture(current)
                                                    .with_bootstrap_keys(&bootstrap_keys),
                                            );
                                        }
                                        conn = None;
                                    } else {
                                        hb_err.store(err, Ordering::SeqCst);
                                    }
                                }
                            }
                            Err(_) => {
                                if let Some(current) = conn.as_ref() {
                                    remembered = Some(
                                        KnownCoord::capture(current)
                                            .with_bootstrap_keys(&bootstrap_keys),
                                    );
                                }
                                conn = None;
                            }
                        }
                    }
                }
            }
        }));
    }
}

pub(crate) fn copy_member_id(dest: &mut String, source: &str) {
    if dest.as_str() != source {
        dest.clear();
        dest.push_str(source);
    }
}

fn reuse_topic_names(dest: &mut Option<Vec<String>>, topics: &[String]) {
    let same = dest
        .as_ref()
        .is_some_and(|names| names.as_slice() == topics);
    if !same {
        *dest = Some(topics.to_vec());
    }
}

fn reuse_topic_partitions(
    dest: &mut Option<Vec<TopicPartitions>>,
    next: &Option<Vec<TopicPartitions>>,
) {
    if dest.as_ref() != next.as_ref() {
        *dest = next.clone();
    }
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
) -> Option<BrokerConn> {
    let addr = cfg.bootstrap.first()?;
    if bootstrap_keys.is_empty() {
        return discover_coord_within(cfg, group_id, COORDINATOR_GROUP, deadline)
            .await
            .ok();
    }
    let bound = crate::net::Deadline::from_std(deadline);
    let (mut hop, version, _) = open_coord_with_keys(cfg, addr, bootstrap_keys).await.ok()?;
    let coord_addr = coordinator_address_of(&mut hop, version, group_id, COORDINATOR_GROUP, bound)
        .await
        .ok()?;
    if coord_addr == hop.addr() {
        return Some(hop);
    }
    open_coord_with_find_version(cfg, &coord_addr)
        .await
        .ok()
        .map(|(conn, _, _)| conn)
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
        *conn = leave_coordinator(cfg, group_id, deadline, bootstrap_keys).await;
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
                drop(
                    c.roundtrip(
                        CONSUMER_GROUP_HEARTBEAT,
                        version,
                        |buf| encode_consumer_group_heartbeat_request(buf, version, &req),
                        timeout,
                    )
                    .await,
                );
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

fn wanted_from_kip848<I: AsRef<str>>(
    subscribed: &[String],
    id_to_name: &HashMap<[u8; 16], I>,
    assignment: &[TopicPartitions],
) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    let mut seen: HashMap<&str, HashSet<i32>> = HashMap::new();
    for tp in assignment {
        let name = id_to_name
            .get(&tp.topic_id)
            .map(|name| name.as_ref())
            .or_else(|| {
                // One subscription can name an id metadata has not indexed yet.
                // A zero id is not a topic, so it must not be attached to the
                // first name when several topics are subscribed.
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
        let Some(parts) = seen.get_mut(name) else {
            continue;
        };
        for p in &tp.partitions {
            if *p < 0 || !parts.insert(*p) {
                continue;
            }
            out.push((name.to_owned(), *p));
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

/// A negative position is the reset marker after a topic is recreated.
/// It is not an offset that can be committed.
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

fn refill_shared_offset_topics(
    topics: &mut Vec<OffsetTopic>,
    items: &[(Arc<str>, i32, i64)],
    epoch_of: impl Fn(&str, i32) -> i32,
) {
    let mut used = 0usize;
    for (topic, partition, offset) in items {
        let part = OffsetPartition {
            partition: *partition,
            offset: *offset,
            leader_epoch: epoch_of(topic.as_ref(), *partition),
            metadata: String::new(),
        };
        let existing = topics
            .iter()
            .take(used)
            .position(|slot| slot.topic == topic.as_ref());
        if let Some(index) = existing {
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
        used = used.saturating_add(1);
    }
    topics.truncate(used);
}

pub(crate) fn group_offset_topics(
    items: &[(TopicPartition, OffsetAndMetadata)],
) -> Vec<OffsetTopic> {
    let mut topics = Vec::new();
    refill_offset_topics(&mut topics, items);
    topics
}

pub(crate) fn group_borrowed_offset_topics(
    items: &[(TopicPartition, OffsetAndMetadata)],
) -> Vec<BorrowedOffsetTopic<'_>> {
    let mut groups: Vec<BorrowedOffsetTopic<'_>> = Vec::new();
    let mut slots: HashMap<&str, usize> = HashMap::new();
    for (tp, md) in items {
        let part = BorrowedOffsetPartition {
            partition: tp.partition,
            offset: md.offset,
            leader_epoch: md.wire_epoch(),
            metadata: md.metadata.as_str(),
        };
        let topic = tp.topic.as_str();
        if let Some(&slot) = slots.get(topic) {
            if let Some(group) = groups.get_mut(slot) {
                if group
                    .partitions
                    .iter()
                    .any(|saved| saved.partition == tp.partition)
                {
                    continue;
                }
                group.partitions.push(part);
                continue;
            }
        }
        let slot = groups.len();
        let _ = slots.insert(topic, slot);
        groups.push(BorrowedOffsetTopic {
            topic,
            partitions: vec![part],
        });
    }
    groups
}

fn refill_offset_topics(
    topics: &mut Vec<OffsetTopic>,
    items: &[(TopicPartition, OffsetAndMetadata)],
) {
    let mut used = 0usize;
    for (tp, md) in items {
        let part = OffsetPartition {
            partition: tp.partition,
            offset: md.offset,
            leader_epoch: md.wire_epoch(),
            metadata: md.metadata.clone(),
        };
        let existing = topics
            .iter()
            .take(used)
            .position(|slot| slot.topic == tp.topic);
        if let Some(index) = existing {
            let Some(slot) = topics.get_mut(index) else {
                continue;
            };
            if slot
                .partitions
                .iter()
                .any(|saved| saved.partition == tp.partition)
            {
                continue;
            }
            slot.partitions.push(part);
            continue;
        }
        if let Some(slot) = topics.get_mut(used) {
            if slot.topic != tp.topic {
                slot.topic.clear();
                slot.topic.push_str(&tp.topic);
            }
            slot.partitions.clear();
            slot.partitions.push(part);
        } else {
            topics.push(OffsetTopic {
                topic: tp.topic.clone(),
                partitions: vec![part],
            });
        }
        used = used.saturating_add(1);
    }
    topics.truncate(used);
}

pub(crate) fn group_offset_fetch_topics<'a>(
    wanted: impl IntoIterator<Item = (&'a str, i32)>,
) -> Vec<OffsetFetchTopic> {
    let mut topics = Vec::new();
    refill_offset_fetch_topics(&mut topics, wanted);
    topics
}

pub(crate) fn group_borrowed_offset_fetch_topics<'a>(
    wanted: impl IntoIterator<Item = (&'a str, i32)>,
) -> Vec<(&'a str, Vec<i32>)> {
    let mut groups: Vec<(&str, Vec<i32>)> = Vec::new();
    let mut slots: HashMap<&str, usize> = HashMap::new();
    for (topic, part) in wanted {
        if let Some(&slot) = slots.get(topic) {
            if let Some((_, parts)) = groups.get_mut(slot) {
                if !parts.contains(&part) {
                    parts.push(part);
                }
                continue;
            }
        }
        let slot = groups.len();
        let _ = slots.insert(topic, slot);
        groups.push((topic, vec![part]));
    }
    groups
}

fn refill_offset_fetch_topics<'a>(
    topics: &mut Vec<OffsetFetchTopic>,
    wanted: impl IntoIterator<Item = (&'a str, i32)>,
) {
    let mut used = 0usize;
    for (topic, part) in wanted {
        let existing = topics
            .iter()
            .take(used)
            .position(|slot| slot.topic == topic);
        if let Some(index) = existing {
            let Some(slot) = topics.get_mut(index) else {
                continue;
            };
            if slot.partitions.contains(&part) {
                continue;
            }
            slot.partitions.push(part);
            continue;
        }
        if let Some(slot) = topics.get_mut(used) {
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
        used = used.saturating_add(1);
    }
    topics.truncate(used);
}

fn index_revoked(revoked: &[(String, i32)]) -> HashMap<&str, HashSet<i32>> {
    let mut parts: HashMap<&str, HashSet<i32>> = HashMap::new();
    for (topic, partition) in revoked {
        if let Some(slot) = parts.get_mut(topic.as_str()) {
            let _ = slot.insert(*partition);
            continue;
        }
        let mut slot = HashSet::new();
        let _ = slot.insert(*partition);
        let _ = parts.insert(topic.as_str(), slot);
    }
    parts
}

fn revoked_contains(index: &HashMap<&str, HashSet<i32>>, topic: &str, partition: i32) -> bool {
    index
        .get(topic)
        .is_some_and(|parts| parts.contains(&partition))
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

fn remember_unique_topic(topics: &mut Vec<String>, topic: &str) {
    if topics.iter().any(|name| name == topic) {
        return;
    }
    topics.push(topic.to_owned());
}

/// One owned topic becomes one name per partition. The last partition keeps it.
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
) -> Result<(Consumer, Result<BrokerConn>)> {
    let member = Consumer::connect_within(cfg.clone(), deadline);
    let coordinator = discover_coord_within(cfg, group_id, COORDINATOR_GROUP, deadline);
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
    prior: &mut HashMap<String, KnownCoord>,
) {
    let known = if conn
        .as_ref()
        .is_some_and(|c| c.idle_expired(cfg.connections_max_idle))
    {
        let snap = conn
            .as_ref()
            .map(|conn| KnownCoord::capture(conn).with_bootstrap_keys(bootstrap_keys));
        *conn = None;
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

async fn discover_coord_with_versions(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: Instant,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
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

async fn discover_coord_once(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: Instant,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
    let deadline = crate::net::Deadline::from_std(deadline);
    if cfg.bootstrap.len() > 1 {
        return discover_coord_raced(cfg, group_id, key_type, deadline).await;
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

async fn discover_coord_raced(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: crate::net::Deadline,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
    type Handshake = (BrokerConn, i16, crate::protocol::api::ApiVersionsResponse);
    let mut inflight = tokio::task::JoinSet::<(usize, Result<Handshake>)>::new();
    for (index, addr) in cfg.bootstrap.iter().enumerate() {
        let cfg = cfg.clone();
        let addr = addr.clone();
        let _ = inflight.spawn(async move {
            let result = match bounded_coord_config(&cfg, deadline) {
                Ok(bounded) => open_coord_with_find_version(&bounded, &addr).await,
                Err(error) => Err(error),
            };
            (index, result)
        });
    }
    let mut ready: Vec<Option<Result<Handshake>>> = Vec::new();
    ready.resize_with(inflight.len(), || None);
    let mut last = Error::protocol("find coordinator failed");
    while !inflight.is_empty() {
        if deadline.remaining().is_err() {
            inflight.abort_all();
            return Err(Error::Timeout);
        }
        let rest = deadline.remaining().unwrap_or(Duration::ZERO);
        let joined = tokio::select! {
            joined = inflight.join_next() => joined,
            _ = tokio::time::sleep(rest) => {
                inflight.abort_all();
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
                        cfg, group_id, key_type, deadline, &mut ready, index,
                    )
                    .await
                    {
                        CoordAsk::Found(found) => {
                            inflight.abort_all();
                            return Ok(*found);
                        }
                        CoordAsk::Fatal(error) => {
                            inflight.abort_all();
                            return Err(error);
                        }
                        CoordAsk::Retry(error) => last = error,
                    }
                    break;
                }
                Some(_) => {
                    match ask_ready_coordinator(
                        cfg, group_id, key_type, deadline, &mut ready, cursor,
                    )
                    .await
                    {
                        CoordAsk::Found(found) => {
                            inflight.abort_all();
                            return Ok(*found);
                        }
                        CoordAsk::Fatal(error) => {
                            inflight.abort_all();
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
    inflight.abort_all();
    if deadline.remaining().is_err() {
        return Err(Error::Timeout);
    }
    Err(last)
}

type CoordHandshake = (BrokerConn, i16, crate::protocol::api::ApiVersionsResponse);

enum CoordAsk {
    Found(Box<(BrokerConn, crate::protocol::api::ApiVersionsResponse)>),
    Fatal(Error),
    Retry(Error),
}

async fn ask_ready_coordinator(
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    deadline: crate::net::Deadline,
    ready: &mut [Option<Result<CoordHandshake>>],
    index: usize,
) -> CoordAsk {
    let Some(slot) = ready.get_mut(index).and_then(Option::take) else {
        return CoordAsk::Retry(Error::protocol("missing coordinator handshake"));
    };
    let (hop, version, versions) = match slot {
        Ok(handshake) => handshake,
        Err(error) => return CoordAsk::Retry(error),
    };
    let asked =
        find_coordinator_on(cfg, hop, version, versions, group_id, key_type, deadline).await;
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
    find_coordinator_on(cfg, hop, version, versions, group_id, key_type, deadline).await
}

async fn find_coordinator_on(
    cfg: &ConsumerConfig,
    mut hop: BrokerConn,
    version: i16,
    versions: crate::protocol::api::ApiVersionsResponse,
    group_id: &str,
    key_type: i8,
    deadline: crate::net::Deadline,
) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
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
    let Some(coord_addr) = crate::cluster::dial_address(&host, port) else {
        return Err(Error::protocol("invalid coordinator address"));
    };
    if coord_addr == hop.addr() {
        return Ok((hop, versions));
    }
    let bounded = bounded_coord_config(cfg, deadline)?;
    let (conn, _, versions) = open_coord_with_find_version(&bounded, &coord_addr).await?;
    Ok((conn, versions))
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
        }
    }

    pub(crate) fn with_bootstrap_keys(mut self, keys: &[ApiVersion]) -> Self {
        self.bootstrap_keys = keys.to_vec();
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
    if known.find_coordinator_version < 0 || cfg.bootstrap.len() != 1 {
        // Heap-allocate the full handshake. Inlining it here makes the
        // caller's state machine large enough to overflow the stack.
        return Box::pin(rediscover_coord(cfg, group_id, key_type, &known.addr)).await;
    }
    let deadline = Instant::now()
        .checked_add(cfg.request_timeout)
        .ok_or(Error::Timeout)?;
    let bounded = bounded_coord_config(cfg, crate::net::Deadline::from_std(deadline))?;
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
        return coordinator_address_of(&mut hop, version, group_id, key_type, deadline).await;
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
        return Err(Error::broker(err, "FindCoordinator"));
    }
    crate::cluster::dial_address(&host, port)
        .ok_or_else(|| Error::protocol("invalid coordinator address"))
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
            return Ok(open_coord_with_find_version(&bounded, &addr).await?.0);
        }
    };
    if addr == previous {
        return Ok(speculative.await?.0);
    }
    Ok(open_coord_with_find_version(&bounded, &addr).await?.0)
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
    coordinator_address_of(&mut hop, version, group_id, key_type, deadline).await
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
    let (mut conn, bound) = open_coord_socket(cfg, addr).await?;
    let resp = remembered_coord_versions(keys);
    let version = stamp_coord_versions(&mut conn, &resp)?;
    authenticate_coord(&mut conn, cfg, bound).await?;
    Ok((conn, version, resp))
}

async fn open_coord_with_find_version(
    cfg: &ConsumerConfig,
    addr: &str,
) -> Result<(BrokerConn, i16, ApiVersionsResponse)> {
    let (mut conn, bound) = open_coord_socket(cfg, addr).await?;
    let resp = crate::protocol::api::negotiate_api_versions(&mut conn, bound.remaining()?).await?;
    let version = stamp_coord_versions(&mut conn, &resp)?;
    authenticate_coord(&mut conn, cfg, bound).await?;
    Ok((conn, version, resp))
}

#[expect(
    clippy::too_many_arguments,
    reason = "coordinator RPC carries versions learned from earlier coordinators"
)]
pub(crate) async fn coord_roundtrip(
    coord: &mut BrokerConn,
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    api_key: i16,
    encode_body: impl FnMut(i16, &mut BytesMut) -> Result<()>,
    prior: &mut HashMap<String, KnownCoord>,
    request_timeout: Duration,
) -> Result<(i16, Bytes)> {
    coord_roundtrip_with_bootstrap(
        coord,
        cfg,
        group_id,
        key_type,
        api_key,
        encode_body,
        &[],
        prior,
        request_timeout,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "coordinator RPC carries the learned bootstrap versions"
)]
async fn coord_roundtrip_with_bootstrap(
    coord: &mut BrokerConn,
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    api_key: i16,
    mut encode_body: impl FnMut(i16, &mut BytesMut) -> Result<()>,
    bootstrap_keys: &[ApiVersion],
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
            prior,
            deadline.remaining()?,
        )
        .await?;
        return Ok((version, body));
    }
    if coord.idle_expired(cfg.connections_max_idle) {
        let bounded = bounded_coord_config(cfg, deadline)?;
        *coord = reopen_known_coord(&bounded, coord).await?;
    }
    let version = negotiated_coord_version(coord, api_key)?;
    let body = match coord
        .roundtrip_deadline(api_key, version, |buf| encode_body(version, buf), deadline)
        .await
    {
        Ok(body) => body,
        Err(e) if e.is_retriable() => {
            let bounded = bounded_coord_config(cfg, deadline)?;
            *coord = reopen_known_coord(&bounded, coord).await?;
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
    reason = "coord roundtrip is one wire call plus rediscovery identity"
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
    deadline: crate::net::Deadline,
) -> Result<(i16, Bytes)> {
    if coordinator_error(api_key, version, &body).is_some_and(error::coordinator_retriable) {
        let bounded = bounded_coord_config(cfg, deadline)?;
        let known = KnownCoord::capture(coord);
        let mut prior = HashMap::new();
        *coord = Box::pin(rediscover_known_coord(
            &bounded, group_id, key_type, &known, &mut prior,
        ))
        .await?;
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
    use crate::protocol::group::{ConsumerProtocolSubscription, FetchedOffset, JoinGroupRequest};
    use std::collections::HashMap;
    use std::sync::Arc;

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

    #[test]
    fn revoked_partitions_match_by_borrowed_topic_name() {
        let revoked = vec![
            ("orders".to_owned(), 0),
            ("orders".to_owned(), 2),
            ("payments".to_owned(), 1),
            ("orders".to_owned(), 0),
        ];
        let index = index_revoked(&revoked);
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

    #[test]
    fn kip848_wanted_maps_topic_id_to_subscribed_name() {
        let assignment = vec![TopicPartitions {
            topic_id: [9u8; 16],
            partitions: vec![1, 3],
        }];
        assert_eq!(
            wanted_from_kip848(
                &["orders".into()],
                &HashMap::<[u8; 16], String>::new(),
                &assignment,
            ),
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
            wanted_from_kip848(&["orders".into(), "payments".into()], &ids, &multi),
            vec![("payments".into(), 0)]
        );
        let messy = vec![TopicPartitions {
            topic_id: [1u8; 16],
            partitions: vec![0, -1, 0, 2],
        }];
        assert_eq!(
            wanted_from_kip848(&["payments".into()], &ids, &messy),
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
            wanted_from_kip848(&["payments".into()], &ids, &repeated),
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
        let unresolved = wanted_from_kip848(&["orders".into(), "payments".into()], &ids, &unknown);
        assert!(unresolved.is_empty());
        assert!(!kip848_assignment_can_apply(&unresolved, &unknown));
        assert!(kip848_assignment_can_apply(&[], &[]));
        let zero = vec![TopicPartitions {
            topic_id: [0u8; 16],
            partitions: vec![0, 1],
        }];
        assert!(wanted_from_kip848(&["orders".into(), "payments".into()], &ids, &zero).is_empty());
        assert_eq!(
            wanted_from_kip848(
                &["orders".into()],
                &HashMap::<[u8; 16], String>::new(),
                &zero,
            ),
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
