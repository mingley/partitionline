//! Consumer-group join / sync / heartbeat / commit.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::watch;

use crate::config::{AutoOffsetReset, IsolationLevel};
use crate::consumer::{
    Consumer, ConsumerConfig, ConsumerRecords, OffsetAndMetadata, TopicPartition,
};
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
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
    decode_offset_fetch_response, decode_subscription_owned, decode_sync_group_response,
    encode_find_coordinator_request_typed, encode_heartbeat_request,
    encode_join_group_protocols_request, encode_leave_group_request_members,
    encode_offset_commit_request, encode_offset_fetch_request, encode_subscription,
    encode_subscription_owned, encode_sync_group_request, encode_tp_assignment, ConsumerProtocol,
    FetchedOffsetTopic, JoinGroupProtocol, JoinGroupProtocolsRequest, JoinGroupRequest,
    LeaveGroupMember, OffsetFetchTopic, OffsetPartition, OffsetTopic, SyncGroupRequest,
    COORDINATOR_GROUP,
};
use crate::protocol::sasl;

pub use crate::protocol::group::CoordinatorType;

pub(crate) type TopicMatch = Arc<dyn Fn(&str) -> bool + Send + Sync>;

type AsyncOffsetCommitCallback =
    Box<dyn FnOnce(Result<Vec<(TopicPartition, OffsetAndMetadata)>>) + Send>;
type PendingAsyncCommit = (
    Vec<(TopicPartition, OffsetAndMetadata)>,
    Option<AsyncOffsetCommitCallback>,
);

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
    let mut members: Vec<String> = members.to_vec();
    members.sort();
    let mut partitions: Vec<i32> = partitions.to_vec();
    partitions.sort();
    let mut out: HashMap<String, Vec<i32>> = HashMap::new();
    for m in &members {
        let _ = out.insert(m.clone(), Vec::new());
    }
    let n = members.len();
    if n == 0 {
        return out;
    }
    let np = partitions.len();
    let base = np / n;
    let extra = np % n;
    let mut idx = 0usize;
    for (i, m) in members.iter().enumerate() {
        let take = base + usize::from(i < extra);
        for _ in 0..take {
            if let Some(p) = partitions.get(idx) {
                if let Some(slot) = out.get_mut(m) {
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
    let member_set: std::collections::HashSet<&str> = members.iter().map(String::as_str).collect();
    let part_set: std::collections::HashSet<i32> = partitions.iter().copied().collect();
    let mut out: HashMap<String, Vec<i32>> = HashMap::new();
    for m in members {
        let _ = out.insert(m.clone(), Vec::new());
    }
    let mut used = std::collections::HashSet::new();
    for (m, parts) in prev {
        if !member_set.contains(m.as_str()) {
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
    let mut remaining: Vec<i32> = partitions
        .iter()
        .copied()
        .filter(|p| !used.contains(p))
        .collect();
    remaining.sort();
    for p in remaining {
        let target = out
            .iter()
            .min_by_key(|(m, v)| (v.len(), (*m).clone()))
            .map(|(m, _)| m.clone());
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

fn cooperative_filter<T: Clone + Eq + std::hash::Hash>(
    computed: HashMap<String, Vec<T>>,
    prev: &HashMap<String, Vec<T>>,
) -> HashMap<String, Vec<T>> {
    let mut owner: HashMap<T, String> = HashMap::new();
    for (m, tps) in prev {
        for tp in tps {
            if !owner.contains_key(tp) {
                let _ = owner.insert(tp.clone(), m.clone());
            }
        }
    }
    let mut out: HashMap<String, Vec<T>> = HashMap::new();
    for (m, tps) in computed {
        let slot = out.entry(m.clone()).or_default();
        for tp in tps {
            match owner.get(&tp) {
                Some(o) if o != &m => {}
                _ => slot.push(tp),
            }
        }
    }
    out
}

/// Range-assign each topic independently across every member, then concatenate.
pub fn assign_range_topics(
    members: &[String],
    topics: &[(String, Vec<i32>)],
) -> HashMap<String, Vec<(String, i32)>> {
    assign_range_subscribed(&all_subscribed(members, topics), topics)
}

/// Sticky-assign each topic independently across every member, then concatenate.
pub fn assign_sticky_topics(
    members: &[String],
    topics: &[(String, Vec<i32>)],
    prev: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<String, Vec<(String, i32)>> {
    assign_sticky_subscribed(&all_subscribed(members, topics), topics, prev)
}

fn all_subscribed(members: &[String], topics: &[(String, Vec<i32>)]) -> Vec<(String, Vec<String>)> {
    let names: Vec<String> = topics.iter().map(|(t, _)| t.clone()).collect();
    members.iter().map(|m| (m.clone(), names.clone())).collect()
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
    for (topic, parts) in topics {
        let members = members_for_topic(member_subs, topic);
        for (member, ps) in assign_range(&members, parts) {
            if let Some(slot) = out.get_mut(&member) {
                for p in ps {
                    slot.push((topic.clone(), p));
                }
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
    for (topic, parts) in topics {
        let members = members_for_topic(member_subs, topic);
        let prev_for_topic: HashMap<String, Vec<i32>> = prev
            .iter()
            .map(|(m, tps)| {
                (
                    m.clone(),
                    tps.iter()
                        .filter(|(t, _)| t == topic)
                        .map(|(_, p)| *p)
                        .collect(),
                )
            })
            .collect();
        for (member, ps) in assign_sticky(&members, parts, &prev_for_topic) {
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

fn members_for_topic(member_subs: &[(String, Vec<String>)], topic: &str) -> Vec<String> {
    member_subs
        .iter()
        .filter(|(_, subs)| subs.iter().any(|t| t == topic))
        .map(|(id, _)| id.clone())
        .collect()
}

/// This member's identity in a consumer group (Java `ConsumerGroupMetadata`).
///
/// [`Display`] is Java `ConsumerGroupMetadata.toString` (`GroupMetadata(...)`).
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
/// [`Display`] is Java `GroupProtocol.toString` (`CLASSIC`). [`Self::of`] is
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
    hb_stop: watch::Sender<bool>,
    last_auto_commit: Instant,
    last_poll: Arc<parking_lot::Mutex<Option<Instant>>>,
    /// Heartbeat thread left the group after `max.poll.interval.ms`.
    left_max_poll: Arc<AtomicBool>,
    /// Next [`poll`](Self::poll) must rejoin (Java `enforceRebalance`).
    rebalance_needed: bool,
    /// JoinGroup v8+ Reason for the next rejoin (KIP-800).
    rebalance_reason: Option<String>,
    /// Java `commitAsync`: OffsetCommit sent on the next poll / leave.
    pending_async_commits: Vec<PendingAsyncCommit>,
}

impl ConsumerGroup {
    /// Join with the Java range assignor. One topic.
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
        let topics = collect_topics(topics)?;
        Self::join_with_protocol_list(cfg, group_id, topics, vec![protocol.to_string()], None).await
    }

    /// Join with several assignors (Java `partition.assignment.strategy`).
    ///
    /// JoinGroup sends Protocols of N in this order. The broker picks the
    /// first protocol every member supports; this client then assigns with
    /// that name. Empty `assignors` is a protocol error.
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
        let names: Vec<String> = assignors.into_iter().map(Into::into).collect();
        if names.is_empty() {
            return Err(Error::protocol("JoinGroup Protocols is empty"));
        }
        Self::join_with_protocol_list(cfg, group_id.into(), collect_topics(topics)?, names, None)
            .await
    }

    async fn join_with_protocol_list(
        cfg: ConsumerConfig,
        group_id: String,
        topics: Vec<String>,
        assignors: Vec<String>,
        topic_match: Option<TopicMatch>,
    ) -> Result<Self> {
        let protocol = assignors
            .first()
            .cloned()
            .ok_or_else(|| Error::protocol("JoinGroup Protocols is empty"))?;
        let consumer = Consumer::new(cfg.clone()).await?;
        let coord = discover_coord(&cfg, &group_id, COORDINATOR_GROUP).await?;

        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_generation = Arc::new(AtomicI32::new(0));
        let hb_assignment = Arc::new(parking_lot::Mutex::new(None));
        let hb_ack = Arc::new(parking_lot::Mutex::new(None));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            consumer,
            coord,
            cfg: cfg.clone(),
            group_id,
            member_id: String::new(),
            generation_id: 0,
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
            hb_stop,
            last_auto_commit: Instant::now(),
            last_poll: Arc::new(parking_lot::Mutex::new(None)),
            left_max_poll: Arc::new(AtomicBool::new(false)),
            rebalance_needed: false,
            rebalance_reason: None,
            pending_async_commits: Vec::new(),
        };
        if g.topic_match.is_some() {
            g.topics = g.matching_topic_names().await?;
            g.last_match_refresh = Instant::now();
        }
        g.rejoin().await?;
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
        let consumer = Consumer::new(cfg.clone()).await?;
        let coord = discover_coord(&cfg, &group_id, COORDINATOR_GROUP).await?;
        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_generation = Arc::new(AtomicI32::new(0));
        let hb_assignment = Arc::new(parking_lot::Mutex::new(None));
        let hb_ack = Arc::new(parking_lot::Mutex::new(None));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            consumer,
            coord,
            cfg: cfg.clone(),
            group_id,
            member_id: String::new(),
            generation_id: 0,
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
            hb_stop,
            last_auto_commit: Instant::now(),
            last_poll: Arc::new(parking_lot::Mutex::new(None)),
            left_max_poll: Arc::new(AtomicBool::new(false)),
            rebalance_needed: false,
            rebalance_reason: None,
            pending_async_commits: Vec::new(),
        };
        if g.topic_match.is_some() {
            g.topics = g.matching_topic_names().await?;
            g.last_match_refresh = Instant::now();
        }
        g.heartbeat_join().await?;
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

    /// Assigned partitions with their next fetch offsets.
    #[must_use]
    pub fn positions(&self) -> Vec<(TopicPartition, i64)> {
        self.consumer.positions()
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

    /// Next fetch offset for an assigned partition.
    pub fn position(&self, topic: &str, partition: i32) -> Result<i64> {
        self.consumer.position(topic, partition)
    }

    /// [`Self::position`] for a [`TopicPartition`].
    pub fn position_of(&self, partition: impl Into<TopicPartition>) -> Result<i64> {
        self.consumer.position_of(partition)
    }

    /// Set the next fetch offset for an assigned partition (Java
    /// `seek(TopicPartition, long)`).
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.consumer.seek(topic, partition, offset)
    }

    /// [`Self::seek`] for a [`TopicPartition`].
    pub fn seek_to(&mut self, partition: impl Into<TopicPartition>, offset: i64) -> Result<()> {
        self.consumer.seek_to(partition, offset)
    }

    /// Seek using [`OffsetAndMetadata`] (Java `seek(TopicPartition, OffsetAndMetadata)`).
    ///
    /// The leader epoch is sent as Fetch `LastFetchedEpoch`. The metadata
    /// string is ignored.
    pub fn seek_with_metadata(
        &mut self,
        partition: impl Into<TopicPartition>,
        offset: impl Into<OffsetAndMetadata>,
    ) -> Result<()> {
        self.consumer.seek_with_metadata(partition, offset)
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
    pub async fn current_lag(
        &mut self,
        partition: impl Into<TopicPartition>,
    ) -> Result<Option<i64>> {
        self.consumer.current_lag(partition).await
    }

    /// [`Self::current_lag`] with a one-shot timeout for the ListOffsets RPC.
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
        if self.kip848 {
            self.apply_pending_assignment().await?;
        }
        let partitions: Vec<TopicPartition> = partitions.into_iter().map(Into::into).collect();
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: Vec<(String, i32)> = partitions
            .iter()
            .map(|tp| (tp.topic.clone(), tp.partition))
            .collect();
        let topics = group_offset_fetch_topics(&wanted);
        let fetched = self.offset_fetch(&topics, timeout).await?;
        let map = committed_offset_map(&fetched)?;
        Ok(partitions
            .iter()
            .map(|tp| {
                let md = map
                    .get(&(tp.topic.clone(), tp.partition))
                    .cloned()
                    .unwrap_or_else(|| OffsetAndMetadata::new(-1));
                (tp.clone(), md)
            })
            .collect())
    }

    async fn offset_fetch(
        &mut self,
        topics: &[OffsetFetchTopic],
        timeout: Duration,
    ) -> Result<Vec<FetchedOffsetTopic>> {
        let version = spoken_offset_fetch(self.coord.offset_fetch_version)?;
        let require_stable = self.cfg.isolation_level == IsolationLevel::ReadCommitted;
        let (member_id, member_epoch) = if self.kip848 {
            (Some(self.member_id.as_str()), self.generation_id)
        } else {
            (None, -1)
        };
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            version,
            |buf| {
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
            timeout,
        )
        .await?;
        decode_offset_fetch_response(&mut body.clone(), version)
    }

    /// Fetch the current assignment. Rejoins on a classic rebalance.
    ///
    /// Returns [`ConsumerRecords`], which indexes like a slice of
    /// [`crate::FetchedRecord`].
    ///
    /// When [`ConsumerConfig::enable_auto_commit`] is on and the interval has
    /// elapsed, commits after a successful fetch.
    ///
    /// Returns [`Error::MaxPollInterval`] if this member did not poll within
    /// [`ConsumerConfig::max_poll_interval`]. The heartbeat thread also leaves
    /// the group when that happens.
    pub async fn poll(&mut self) -> Result<ConsumerRecords> {
        self.poll_fetch(None).await
    }

    /// Poll with a one-shot `fetch.max.wait.ms` (Java `poll(Duration)`).
    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<ConsumerRecords> {
        self.poll_fetch(Some(timeout)).await
    }

    async fn poll_fetch(&mut self, wait: Option<Duration>) -> Result<ConsumerRecords> {
        if self.left_max_poll.load(Ordering::SeqCst) {
            return Err(Error::MaxPollInterval);
        }
        self.check_max_poll_interval()?;
        self.maybe_refresh_matching().await?;
        let force = std::mem::replace(&mut self.rebalance_needed, false);
        if self.kip848 {
            self.apply_pending_assignment().await?;
            if force {
                self.rebalance_reason = None;
                self.heartbeat_join().await?;
            }
        } else if force || self.hb_err.load(Ordering::SeqCst) == error::REBALANCE_IN_PROGRESS {
            self.rejoin().await?;
        }
        self.flush_async_commits().await;
        let recs = match wait {
            Some(t) => self.consumer.fetch_timeout(t).await?,
            None => self.consumer.fetch().await?,
        };
        self.maybe_auto_commit().await?;
        Ok(recs)
    }

    /// Fetch counters for the underlying consumer.
    #[must_use]
    pub fn metrics(&self) -> crate::ConsumerMetrics {
        self.consumer.metrics()
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

    /// Commit the next fetch offsets for the current assignment
    /// (Java `commitSync()` with no arguments).
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
        if self.kip848 {
            self.apply_pending_assignment().await?;
        }
        let assigned = self.consumer.assigned_offsets().to_vec();
        self.commit_offsets_timeout(
            assigned
                .into_iter()
                .map(|(topic, partition, offset)| (TopicPartition::new(topic, partition), offset)),
            timeout,
        )
        .await?;
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
        let items: Vec<(TopicPartition, OffsetAndMetadata)> = offsets
            .into_iter()
            .map(|(tp, offset)| {
                let tp = tp.into();
                let epoch = self.consumer.leader_epoch(&tp.topic, tp.partition);
                (
                    tp,
                    OffsetAndMetadata::from_wire(offset, epoch, String::new()),
                )
            })
            .collect();
        self.commit_with_metadata_timeout(items, timeout).await
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
    pub async fn commit_with_metadata_timeout(
        &mut self,
        offsets: impl IntoIterator<Item = (impl Into<TopicPartition>, impl Into<OffsetAndMetadata>)>,
        timeout: Duration,
    ) -> Result<()> {
        let offsets: Vec<(TopicPartition, OffsetAndMetadata)> = offsets
            .into_iter()
            .map(|(tp, md)| (tp.into(), md.into()))
            .collect();
        let topics = group_offset_topics(&offsets);
        if topics.is_empty() {
            return Ok(());
        }
        let version = spoken_offset_commit(self.coord.offset_commit_version)?;
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            OFFSET_COMMIT,
            version,
            |buf| {
                encode_offset_commit_request(
                    buf,
                    version,
                    &self.group_id,
                    self.generation_id,
                    &self.member_id,
                    self.cfg.group_instance_id.as_deref(),
                    &topics,
                )
            },
            timeout,
        )
        .await?;
        let err = decode_offset_commit_response(&mut body.clone(), version)?;
        if err != 0 {
            return Err(Error::broker(err, "OffsetCommit"));
        }
        self.cfg.interceptors.on_commit(&offsets);
        Ok(())
    }

    /// Queue an OffsetCommit of the current assignment (Java `commitAsync()`).
    ///
    /// Snapshots positions now. The RPC is sent on the next [`Self::poll`],
    /// [`Self::leave`], [`Self::close`], or [`Self::unsubscribe`]. Does not
    /// spawn a task. Failures are not returned from poll; use
    /// [`Self::commit_async_with`] for a callback.
    pub fn commit_async(&mut self) {
        let offsets = self.assigned_commit_offsets();
        self.pending_async_commits.push((offsets, None));
    }

    /// Java `commitAsync(OffsetCommitCallback)`.
    ///
    /// `callback` runs on the next poll / leave with the snapshotted offsets
    /// or the OffsetCommit error. Poll still returns the fetch result.
    pub fn commit_async_with<F>(&mut self, callback: F)
    where
        F: FnOnce(Result<Vec<(TopicPartition, OffsetAndMetadata)>>) + Send + 'static,
    {
        let offsets = self.assigned_commit_offsets();
        self.pending_async_commits
            .push((offsets, Some(Box::new(callback))));
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
        self.pending_async_commits.push((offsets, None));
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
        self.pending_async_commits
            .push((offsets, Some(Box::new(callback))));
    }

    fn assigned_commit_offsets(&self) -> Vec<(TopicPartition, OffsetAndMetadata)> {
        self.consumer
            .assigned_offsets()
            .iter()
            .map(|(topic, partition, offset)| {
                let epoch = self.consumer.leader_epoch(topic, *partition);
                (
                    TopicPartition::new(topic.clone(), *partition),
                    OffsetAndMetadata::from_wire(*offset, epoch, String::new()),
                )
            })
            .collect()
    }

    async fn flush_async_commits(&mut self) {
        let pending = std::mem::take(&mut self.pending_async_commits);
        for (offsets, callback) in pending {
            let send = self
                .commit_with_metadata_timeout(offsets.clone(), self.cfg.request_timeout)
                .await;
            if let Some(callback) = callback {
                callback(send.map(|()| offsets));
            } else if let Err(err) = send {
                let _err = err;
            }
        }
    }

    async fn maybe_auto_commit(&mut self) -> Result<()> {
        if !self.cfg.enable_auto_commit {
            return Ok(());
        }
        if self.last_auto_commit.elapsed() < self.cfg.auto_commit_interval {
            return Ok(());
        }
        self.commit().await
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
    /// Heartbeats stop and the assignment is cleared. Classic LeaveGroup v5
    /// sends [`LEAVE_GROUP_REASON_UNSUBSCRIBED`]. [`Self::subscribe`] joins
    /// again with a new topic list. [`Self::leave`] after this is a no-op.
    pub async fn unsubscribe(&mut self) -> Result<()> {
        if self.member_id.is_empty() {
            self.topics.clear();
            self.topic_match = None;
            self.consumer.clear_assignment();
            return Ok(());
        }
        self.flush_async_commits().await;
        if self.cfg.enable_auto_commit {
            self.commit().await?;
        }
        self.hb_stop.send(true).unwrap_or(());
        let revoked = self.assignment();
        self.leave_coordinator(LEAVE_GROUP_REASON_UNSUBSCRIBED)
            .await?;
        if !revoked.is_empty() {
            self.cfg.rebalance.call(&revoked, &[]);
        }
        self.consumer.clear_assignment();
        self.topics.clear();
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
        self.topic_match = Some(Arc::new(matches));
        let topics = self.matching_topic_names().await?;
        self.last_match_refresh = Instant::now();
        self.apply_subscription(topics).await
    }

    async fn apply_subscription(&mut self, topics: Vec<String>) -> Result<()> {
        if topics == self.topics && !self.member_id.is_empty() {
            return Ok(());
        }
        let rejoining = !self.member_id.is_empty();
        if rejoining {
            let revoked = self.assignment();
            self.consumer.clear_assignment();
            if !revoked.is_empty() {
                self.cfg.rebalance.call(&revoked, &[]);
            }
            self.prev_assignment.clear();
        }
        self.topics = topics;
        self.left_max_poll.store(false, Ordering::SeqCst);
        if rejoining {
            if self.kip848 {
                self.heartbeat_join().await?;
            } else {
                self.rejoin().await?;
            }
            return Ok(());
        }
        let (hb_stop, hb_rx) = watch::channel(false);
        self.hb_stop = hb_stop;
        if self.kip848 {
            self.heartbeat_join().await?;
            self.spawn_heartbeat_consumer(hb_rx);
        } else {
            self.rejoin().await?;
            self.spawn_heartbeat(hb_rx);
        }
        Ok(())
    }

    async fn matching_topic_names(&mut self) -> Result<Vec<String>> {
        let Some(pred) = self.topic_match.clone() else {
            return Ok(self.topics.clone());
        };
        let infos = self.consumer.list_topics().await?;
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
        let topics = self.matching_topic_names().await?;
        self.last_match_refresh = Instant::now();
        self.apply_subscription(topics).await
    }

    /// Leave the group (`LeaveGroup` or KIP-848 epoch `-1`).
    ///
    /// Classic LeaveGroup v5 sends [`LEAVE_GROUP_REASON_CLOSED`].
    pub async fn leave(mut self) -> Result<()> {
        if self.member_id.is_empty() {
            self.hb_stop.send(true).unwrap_or(());
            self.consumer.close_interceptors();
            return Ok(());
        }
        self.flush_async_commits().await;
        if self.cfg.enable_auto_commit {
            self.commit().await?;
        }
        self.hb_stop.send(true).unwrap_or(());
        let out = self.leave_coordinator(LEAVE_GROUP_REASON_CLOSED).await;
        self.consumer.close_interceptors();
        out
    }

    /// Leave the group. Same as [`Self::leave`].
    pub async fn close(self) -> Result<()> {
        self.leave().await
    }

    /// Leave the group, waiting up to `timeout` (Java `close(Duration)`).
    ///
    /// [`Self::leave`] / [`Self::close`] wait up to
    /// [`ConsumerConfig::request_timeout`] for the coordinator. A shorter
    /// `timeout` returns [`Error::Timeout`] if leave does not finish in time.
    pub async fn close_timeout(self, timeout: Duration) -> Result<()> {
        match tokio::time::timeout(timeout, self.leave()).await {
            Ok(out) => out,
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn leave_coordinator(&mut self, reason: &str) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        if self.kip848 {
            let version =
                spoken_consumer_group_heartbeat(self.coord.consumer_group_heartbeat_version)?;
            let req = ConsumerGroupHeartbeatRequest {
                group_id: self.group_id.clone(),
                member_id: self.member_id.clone(),
                member_epoch: -1,
                instance_id: self.cfg.group_instance_id.clone(),
                rack_id: self.cfg.rack.clone(),
                subscribed_topic_names: None,
                subscribed_topic_regex: None,
                topic_partitions: None,
            };
            let body = coord_roundtrip(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
                CONSUMER_GROUP_HEARTBEAT,
                version,
                |buf| encode_consumer_group_heartbeat_request(buf, version, &req),
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
        let version = spoken_leave_group(self.coord.leave_group_version)?;
        let members = [LeaveGroupMember {
            member_id: self.member_id.clone(),
            group_instance_id: self.cfg.group_instance_id.clone(),
            reason: Some(truncate_group_reason(reason)),
        }];
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            LEAVE_GROUP,
            version,
            |buf| encode_leave_group_request_members(buf, version, &self.group_id, &members),
            timeout,
        )
        .await?;
        let (err, results) = decode_leave_group_response_version(&mut body.clone(), version)?;
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

    async fn rejoin(&mut self) -> Result<()> {
        for _ in 0..8 {
            let revoked = self.rejoin_once().await?;
            if self.protocol != "cooperative-sticky" || revoked.is_empty() {
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
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let metadata = if name == "range" {
                encode_subscription(&self.topics)?
            } else {
                encode_subscription_owned(&self.topics, &owned)?
            };
            out.push((name, metadata));
        }
        Ok(out)
    }

    async fn rejoin_once(&mut self) -> Result<Vec<(String, i32)>> {
        let timeout = self.cfg.request_timeout;
        let entries = self.join_protocol_entries()?;
        let protocols: Vec<JoinGroupProtocol<'_>> = entries
            .iter()
            .map(|(n, m)| JoinGroupProtocol::new(n, m))
            .collect();
        let version = spoken_join_group(self.coord.join_group_version)?;
        let reason = self.rebalance_reason.take();
        let (error, generation, protocol, leader, assigned_id, skip_assignment, members) = loop {
            let body = coord_roundtrip(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
                JOIN_GROUP,
                version,
                |buf| {
                    encode_join_group_protocols_request(
                        buf,
                        version,
                        &JoinGroupProtocolsRequest {
                            group_id: &self.group_id,
                            session_timeout_ms: self.cfg.session_timeout_ms,
                            member_id: &self.member_id,
                            group_instance_id: self.cfg.group_instance_id.as_deref(),
                            protocol_type: ConsumerProtocol::PROTOCOL_TYPE,
                            protocols: &protocols,
                            reason: reason.as_deref(),
                        },
                    )
                },
                timeout,
            )
            .await?;
            let decoded = decode_join_group_response(&mut body.clone(), version)?;
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
        self.member_id = assigned_id;
        self.generation_id = generation;
        if !protocol.is_empty() {
            self.protocol = protocol;
        }

        let mut member_subs: Vec<(String, Vec<String>)> = Vec::with_capacity(members.len());
        let mut owned_prev = self.prev_assignment.clone();
        let mut topic_set = self.topics.clone();
        for m in &members {
            let (subs, owned) = match decode_subscription_owned(&m.metadata) {
                Ok((t, o)) if !t.is_empty() => (t, o),
                Ok((_, o)) => (self.topics.clone(), o),
                Err(_) => (self.topics.clone(), Vec::new()),
            };
            for t in &subs {
                if !topic_set.iter().any(|x| x == t) {
                    topic_set.push(t.clone());
                }
            }
            let _ = owned_prev.insert(m.member_id.clone(), owned);
            member_subs.push((m.member_id.clone(), subs));
        }
        self.consumer.refresh_topics(&topic_set).await?;
        let mut by_topic = Vec::with_capacity(topic_set.len());
        for topic in &topic_set {
            by_topic.push((topic.clone(), self.consumer.partition_ids(topic).await?));
        }
        let assignments = if leader == self.member_id && !skip_assignment {
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
        let version = spoken_sync_group(self.coord.sync_group_version)?;
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            SYNC_GROUP,
            version,
            |buf| {
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
            timeout,
        )
        .await?;
        let (err, assignment) = decode_sync_group_response(&mut body.clone(), version)?;
        if err != 0 {
            return Err(Error::broker(err, "SyncGroup"));
        }
        let assigned = decode_assignment(&assignment)?;
        let wanted: Vec<(String, i32)> = assigned
            .into_iter()
            .flat_map(|(t, ps)| ps.into_iter().map(move |p| (t.clone(), p)))
            .collect();
        let revoked = self.assign_committed(&wanted).await?;
        self.hb_generation
            .store(self.generation_id, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(revoked)
    }

    async fn heartbeat_join(&mut self) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.consumer.refresh_topics(&self.topics).await?;
        let version = spoken_consumer_group_heartbeat(self.coord.consumer_group_heartbeat_version)?;
        if version >= 1 && self.member_id.is_empty() {
            self.member_id = new_kip848_member_id()?;
        }
        let req = ConsumerGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: 0,
            instance_id: self.cfg.group_instance_id.clone(),
            rack_id: self.cfg.rack.clone(),
            subscribed_topic_names: Some(self.topics.clone()),
            subscribed_topic_regex: None,
            topic_partitions: None,
        };
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            CONSUMER_GROUP_HEARTBEAT,
            version,
            |buf| encode_consumer_group_heartbeat_request(buf, version, &req),
            timeout,
        )
        .await?;
        let resp = decode_consumer_group_heartbeat_response(&mut body.clone(), version)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ConsumerGroupHeartbeat"));
        }
        if let Some(id) = resp.member_id {
            self.member_id = id;
        }
        self.generation_id = resp.member_epoch;
        let assignment = resp.assignment.unwrap_or_default();
        let wanted = wanted_from_kip848(&self.topics, &self.consumer.topic_id_names(), &assignment);
        let _ = self.assign_committed(&wanted).await?;
        *self.hb_ack.lock() = Some(assignment);
        self.hb_generation
            .store(self.generation_id, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(())
    }

    async fn apply_pending_assignment(&mut self) -> Result<()> {
        let pending = self.hb_assignment.lock().take();
        let Some(assignment) = pending else {
            return Ok(());
        };
        self.consumer.refresh_topics(&self.topics).await?;
        let wanted = wanted_from_kip848(&self.topics, &self.consumer.topic_id_names(), &assignment);
        let _ = self.assign_committed(&wanted).await?;
        *self.hb_ack.lock() = Some(assignment);
        self.generation_id = self.hb_generation.load(Ordering::SeqCst);
        Ok(())
    }

    async fn assign_committed(&mut self, wanted: &[(String, i32)]) -> Result<Vec<(String, i32)>> {
        let current: HashMap<(String, i32), i64> = self
            .consumer
            .assigned_offsets()
            .iter()
            .map(|(t, p, o)| ((t.clone(), *p), *o))
            .collect();
        let kept_epochs: HashMap<(String, i32), i32> = wanted
            .iter()
            .filter(|(t, p)| current.contains_key(&(t.clone(), *p)))
            .map(|(t, p)| ((t.clone(), *p), self.consumer.last_fetched_epoch(t, *p)))
            .collect();
        let added: Vec<(String, i32)> = wanted
            .iter()
            .filter(|(t, p)| !current.contains_key(&(t.clone(), *p)))
            .cloned()
            .collect();
        let fetched = if added.is_empty() {
            Vec::new()
        } else {
            let topics = group_offset_fetch_topics(&added);
            let timeout = self.cfg.request_timeout;
            self.offset_fetch(&topics, timeout).await?
        };
        let map = committed_offset_map(&fetched)?;
        let reset = self.cfg.auto_offset_reset;
        let mut starts: Vec<(String, i32, i64)> = Vec::with_capacity(wanted.len());
        for (topic, part) in wanted {
            let key = (topic.clone(), *part);
            if let Some(o) = current.get(&key) {
                starts.push((topic.clone(), *part, *o));
                continue;
            }
            let committed = map.get(&key).map(|m| m.offset).unwrap_or(-1);
            let start = if committed >= 0 {
                committed
            } else {
                match reset {
                    AutoOffsetReset::Earliest => 0,
                    AutoOffsetReset::Latest => {
                        self.consumer.ensure_topic_metadata(topic).await?;
                        self.consumer
                            .list_offsets(topic.clone(), *part, crate::LATEST_TIMESTAMP)
                            .await?
                    }
                    AutoOffsetReset::None => {
                        return Err(Error::protocol(format!(
                            "no committed offset for {topic}-{part}"
                        )));
                    }
                }
            };
            starts.push((topic.clone(), *part, start));
        }
        self.consumer.assign_all(&starts).await?;
        for (topic, part) in wanted {
            let key = (topic.clone(), *part);
            if let Some(epoch) = kept_epochs.get(&key) {
                self.consumer.set_last_fetched_epoch(topic, *part, *epoch);
            } else if let Some(md) = map.get(&key) {
                self.consumer
                    .set_last_fetched_epoch(topic, *part, md.wire_epoch());
            }
        }
        let prev: HashSet<(String, i32)> = current.keys().cloned().collect();
        let next: HashSet<(String, i32)> = wanted.iter().cloned().collect();
        let revoked: Vec<(String, i32)> = prev.difference(&next).cloned().collect();
        let added_tps: Vec<(String, i32)> = next.difference(&prev).cloned().collect();
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
        let member_id = self.member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_generation = self.hb_generation.clone();
        let hb_assignment = self.hb_assignment.clone();
        let hb_ack = self.hb_ack.clone();
        let last_poll = self.last_poll.clone();
        let left_max_poll = self.left_max_poll.clone();
        let cfg = self.cfg.clone();
        drop(tokio::spawn(async move {
            let mut conn: Option<BrokerConn> = None;
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
                            true,
                            &last_poll,
                            &left_max_poll,
                        )
                        .await
                        {
                            break;
                        }
                        if conn
                            .as_ref()
                            .is_some_and(|c| c.idle_expired(cfg.connections_max_idle))
                        {
                            conn = None;
                        }
                        if conn.is_none() {
                            conn = discover_coord(&cfg, &group_id, COORDINATOR_GROUP).await.ok();
                        }
                        let Some(c) = conn.as_mut() else {
                            continue;
                        };
                        let timeout = cfg.request_timeout;
                        let epoch = hb_generation.load(Ordering::SeqCst);
                        let topic_partitions = hb_ack.lock().clone();
                        let version = c.consumer_group_heartbeat_version;
                        if spoken_consumer_group_heartbeat(version).is_err() {
                            conn = None;
                            continue;
                        }
                        let req = ConsumerGroupHeartbeatRequest {
                            group_id: group_id.clone(),
                            member_id: member_id.clone(),
                            member_epoch: epoch,
                            instance_id: cfg.group_instance_id.clone(),
                            rack_id: cfg.rack.clone(),
                            subscribed_topic_names: None,
                            subscribed_topic_regex: None,
                            topic_partitions,
                        };
                        let res = c
                            .roundtrip(
                                CONSUMER_GROUP_HEARTBEAT,
                                version,
                                |buf| encode_consumer_group_heartbeat_request(buf, version, &req),
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
                                        conn = None;
                                    } else {
                                        hb_err.store(resp.error_code, Ordering::SeqCst);
                                        if resp.member_epoch > 0 {
                                            hb_generation.store(resp.member_epoch, Ordering::SeqCst);
                                        }
                                        if resp.error_code == 0 {
                                            if let Some(assignment) = resp.assignment {
                                                *hb_assignment.lock() = Some(assignment);
                                                *hb_ack.lock() = None;
                                            } else {
                                                *hb_ack.lock() = None;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                conn = None;
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
        drop(tokio::spawn(async move {
            let mut conn: Option<BrokerConn> = None;
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
                        )
                        .await
                        {
                            break;
                        }
                        if conn
                            .as_ref()
                            .is_some_and(|c| c.idle_expired(cfg.connections_max_idle))
                        {
                            conn = None;
                        }
                        if conn.is_none() {
                            conn = discover_coord(&cfg, &group_id, COORDINATOR_GROUP).await.ok();
                        }
                        let Some(c) = conn.as_mut() else {
                            continue;
                        };
                        let timeout = cfg.request_timeout;
                        let gid = group_id.clone();
                        let mid = member_id.clone();
                        let instance = cfg.group_instance_id.clone();
                        let generation = hb_generation.load(Ordering::SeqCst);
                        let Ok(version) = spoken_heartbeat(c.heartbeat_version) else {
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
                                        &gid,
                                        generation,
                                        &mid,
                                        instance.as_deref(),
                                    )
                                },
                                timeout,
                            )
                            .await;
                        match res {
                            Ok(body) => {
                                if let Ok(err) =
                                    decode_heartbeat_response(&mut body.clone(), version)
                                {
                                    if error::coordinator_retriable(err) {
                                        conn = None;
                                    } else {
                                        hb_err.store(err, Ordering::SeqCst);
                                    }
                                }
                            }
                            Err(_) => {
                                conn = None;
                            }
                        }
                    }
                }
            }
        }));
    }
}

async fn leave_if_max_poll(
    cfg: &ConsumerConfig,
    group_id: &str,
    member_id: &str,
    kip848: bool,
    last_poll: &parking_lot::Mutex<Option<Instant>>,
    left: &AtomicBool,
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
    if let Ok(mut c) = discover_coord(cfg, group_id, COORDINATOR_GROUP).await {
        let timeout = cfg.request_timeout;
        if kip848 {
            let version = c.consumer_group_heartbeat_version;
            if spoken_consumer_group_heartbeat(version).is_ok() {
                let req = ConsumerGroupHeartbeatRequest {
                    group_id: group_id.to_string(),
                    member_id: member_id.to_string(),
                    member_epoch: -1,
                    instance_id: cfg.group_instance_id.clone(),
                    rack_id: cfg.rack.clone(),
                    subscribed_topic_names: None,
                    subscribed_topic_regex: None,
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
    getrandom::getrandom(&mut raw).map_err(|_| Error::protocol("consumer member id rng"))?;
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        raw,
    ))
}

fn wanted_from_kip848(
    subscribed: &[String],
    id_to_name: &HashMap<[u8; 16], String>,
    assignment: &[TopicPartitions],
) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    for tp in assignment {
        let name = id_to_name.get(&tp.topic_id).cloned().or_else(|| {
            if subscribed.len() == 1 || tp.topic_id == [0u8; 16] {
                subscribed.first().cloned()
            } else {
                None
            }
        });
        let Some(name) = name else {
            continue;
        };
        for p in &tp.partitions {
            out.push((name.clone(), *p));
        }
    }
    out
}

pub(crate) fn collect_topics(
    topics: impl IntoIterator<Item = impl Into<String>>,
) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for topic in topics {
        let topic = topic.into();
        if topic.is_empty() {
            return Err(Error::protocol("empty topic name"));
        }
        if !out.iter().any(|t| t == &topic) {
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
    let mut names = Vec::new();
    for topic in topics {
        let topic = topic.as_ref();
        if topic.is_empty() || topic.starts_with("__") {
            continue;
        }
        if pred(topic) && !names.iter().any(|n| n == topic) {
            names.push(topic.to_string());
        }
    }
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

pub(crate) fn group_offset_topics(
    items: &[(TopicPartition, OffsetAndMetadata)],
) -> Vec<OffsetTopic> {
    let mut by_topic: HashMap<String, Vec<OffsetPartition>> = HashMap::new();
    for (tp, md) in items {
        by_topic
            .entry(tp.topic.clone())
            .or_default()
            .push(OffsetPartition {
                partition: tp.partition,
                offset: md.offset,
                leader_epoch: md.wire_epoch(),
                metadata: md.metadata.clone(),
            });
    }
    by_topic
        .into_iter()
        .map(|(topic, partitions)| OffsetTopic { topic, partitions })
        .collect()
}

pub(crate) fn group_offset_fetch_topics(wanted: &[(String, i32)]) -> Vec<OffsetFetchTopic> {
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for (topic, part) in wanted {
        by_topic.entry(topic.clone()).or_default().push(*part);
    }
    by_topic
        .into_iter()
        .map(|(topic, partitions)| OffsetFetchTopic { topic, partitions })
        .collect()
}

pub(crate) fn committed_offset_map(
    fetched: &[FetchedOffsetTopic],
) -> Result<HashMap<(String, i32), OffsetAndMetadata>> {
    let mut map = HashMap::new();
    for t in fetched {
        for p in &t.partitions {
            if p.has_error() {
                return Err(Error::broker(
                    p.error_code,
                    format!("OffsetFetch {}-{}", t.topic, p.partition),
                ));
            }
            let _ = map.insert(
                (t.topic.clone(), p.partition),
                OffsetAndMetadata::from_wire(p.offset, p.leader_epoch, p.metadata.clone()),
            );
        }
    }
    Ok(map)
}

#[cfg(test)]
fn committed_starts(
    wanted: &[(String, i32)],
    fetched: &[FetchedOffsetTopic],
    reset: AutoOffsetReset,
) -> Result<Vec<(String, i32, i64)>> {
    let map = committed_offset_map(fetched)?;
    let mut out = Vec::with_capacity(wanted.len());
    for (topic, part) in wanted {
        let committed = map
            .get(&(topic.clone(), *part))
            .map(|m| m.offset)
            .unwrap_or(-1);
        let start = if committed >= 0 {
            committed
        } else {
            match reset {
                AutoOffsetReset::Earliest => 0,
                AutoOffsetReset::Latest => {
                    return Err(Error::protocol(
                        "auto.offset.reset=latest needs ListOffsets at assign",
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
    let timeout = cfg.request_timeout;
    let mut last = Error::protocol("find coordinator failed");
    // FindCoordinator 14/15 is one pass of the bootstrap list; try again.
    for _ in 0..3 {
        for addr in &cfg.bootstrap {
            let (mut hop, version) = match open_coord_with_find_version(cfg, addr).await {
                Ok(v) => v,
                Err(e) => {
                    last = e;
                    continue;
                }
            };
            let body = match hop
                .roundtrip(
                    FIND_COORDINATOR,
                    version,
                    |buf| encode_find_coordinator_request_typed(buf, version, group_id, key_type),
                    timeout,
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    last = e;
                    continue;
                }
            };
            let (err, _node, host, port) =
                decode_find_coordinator_response(&mut body.clone(), version)?;
            if err != 0 {
                last = Error::broker(err, "FindCoordinator");
                continue;
            }
            let coord_addr = format!("{host}:{port}");
            if coord_addr == hop.addr() {
                return Ok(hop);
            }
            return open_coord(cfg, &coord_addr).await;
        }
        match &last {
            Error::Broker { code, .. } if error::coordinator_retriable(*code) => {}
            _ => break,
        }
    }
    Err(last)
}

pub(crate) async fn open_coord(cfg: &ConsumerConfig, addr: &str) -> Result<BrokerConn> {
    Ok(open_coord_with_find_version(cfg, addr).await?.0)
}

async fn open_coord_with_find_version(
    cfg: &ConsumerConfig,
    addr: &str,
) -> Result<(BrokerConn, i16)> {
    let mut conn =
        BrokerConn::connect_tls(addr, &cfg.client_id, cfg.connect_timeout, cfg.tls.as_ref())
            .await?;
    let resp = crate::protocol::api::negotiate_api_versions(&mut conn, cfg.request_timeout).await?;
    let version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == FIND_COORDINATOR)
        .and_then(|v| pick_version(v.min_version, v.max_version, 1, 6))
        .ok_or_else(|| Error::Unsupported("broker does not support FindCoordinator v1-6".into()))?;
    conn.offset_commit_version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == OFFSET_COMMIT)
        .and_then(|v| pick_version(v.min_version, v.max_version, 2, 9))
        .unwrap_or(0);
    conn.offset_fetch_version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == OFFSET_FETCH)
        .and_then(|v| pick_version(v.min_version, v.max_version, 1, 9))
        .unwrap_or(0);
    conn.heartbeat_version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == HEARTBEAT)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 4))
        .unwrap_or(-1);
    conn.sync_group_version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == SYNC_GROUP)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 5))
        .unwrap_or(-1);
    conn.join_group_version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == JOIN_GROUP)
        .and_then(|v| pick_version(v.min_version, v.max_version, 2, 9))
        .unwrap_or(0);
    conn.leave_group_version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == LEAVE_GROUP)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 5))
        .unwrap_or(-1);
    conn.consumer_group_heartbeat_version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == CONSUMER_GROUP_HEARTBEAT)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
        .unwrap_or(-1);
    conn.share_group_heartbeat_version = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == SHARE_GROUP_HEARTBEAT)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
        .unwrap_or(-1);
    sasl::apply_api_keys(&mut conn, &resp.api_keys);
    sasl::authenticate(
        &mut conn,
        cfg.sasl_plain.as_ref(),
        cfg.sasl_scram.as_ref(),
        cfg.sasl_scram_sha512.as_ref(),
        cfg.sasl_oauthbearer.as_deref(),
        cfg.sasl_oauthbearer_oidc.as_ref(),
        cfg.request_timeout,
    )
    .await?;
    Ok((conn, version))
}

#[expect(
    clippy::too_many_arguments,
    reason = "coord roundtrip is one wire call plus rediscovery identity"
)]
pub(crate) async fn coord_roundtrip(
    coord: &mut BrokerConn,
    cfg: &ConsumerConfig,
    group_id: &str,
    key_type: i8,
    api_key: i16,
    api_version: i16,
    encode_body: impl Fn(&mut BytesMut) -> Result<()>,
    request_timeout: Duration,
) -> Result<Bytes> {
    if coord.idle_expired(cfg.connections_max_idle) {
        *coord = open_coord(cfg, coord.addr()).await?;
    }
    let body = match coord
        .roundtrip(
            api_key,
            api_version,
            |buf| encode_body(buf),
            request_timeout,
        )
        .await
    {
        Ok(body) => body,
        Err(e) if e.is_retriable() => {
            *coord = open_coord(cfg, coord.addr()).await?;
            coord
                .roundtrip(
                    api_key,
                    api_version,
                    |buf| encode_body(buf),
                    request_timeout,
                )
                .await?
        }
        Err(e) => return Err(e),
    };
    if coordinator_error(api_key, api_version, &body).is_some_and(error::coordinator_retriable) {
        *coord = discover_coord(cfg, group_id, key_type).await?;
        coord
            .roundtrip(
                api_key,
                api_version,
                |buf| encode_body(buf),
                request_timeout,
            )
            .await
    } else {
        Ok(body)
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
    use crate::protocol::group::{FetchedOffset, JoinGroupRequest};
    use std::collections::HashMap;

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
            wanted_from_kip848(&["orders".into()], &HashMap::new(), &assignment),
            vec![("orders".into(), 1), ("orders".into(), 3)]
        );
        let mut ids = HashMap::new();
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
    fn committed_starts_uses_fetched_or_zero() {
        let wanted = vec![("t".into(), 0), ("t".into(), 1)];
        let fetched = vec![FetchedOffsetTopic {
            topic: "t".into(),
            partitions: vec![FetchedOffset::new(0, 5, 0)],
        }];
        let starts = committed_starts(&wanted, &fetched, AutoOffsetReset::Earliest).unwrap();
        assert_eq!(starts, vec![("t".into(), 0, 5), ("t".into(), 1, 0)]);
        assert!(committed_starts(&wanted, &fetched, AutoOffsetReset::None).is_err());
    }

    #[test]
    fn filter_matching_topics_skips_internal_and_sorts() {
        let names = filter_matching_topics(
            ["z-a", "__consumer_offsets", "a-1", "a-1", "", "b-1"],
            |n| n.starts_with("a-") || n.starts_with("z-"),
        );
        assert_eq!(names, vec!["a-1".to_string(), "z-a".to_string()]);
    }
}
