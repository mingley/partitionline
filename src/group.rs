#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::watch;

use crate::config::AutoOffsetReset;
use crate::consumer::{Consumer, ConsumerConfig, FetchedRecord};
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{decode_api_versions_response, encode_api_versions_request};
use crate::protocol::api_keys::{
    API_VERSIONS, CONSUMER_GROUP_HEARTBEAT, FIND_COORDINATOR, HEARTBEAT, JOIN_GROUP, LEAVE_GROUP,
    OFFSET_COMMIT, OFFSET_FETCH, SYNC_GROUP,
};
use crate::protocol::cgheartbeat::{
    decode_consumer_group_heartbeat_response, encode_consumer_group_heartbeat_request,
    ConsumerGroupHeartbeatRequest, TopicPartitions,
};
use crate::protocol::group::{
    decode_assignment, decode_find_coordinator_response, decode_heartbeat_response,
    decode_join_group_response, decode_leave_group_response, decode_offset_commit_response,
    decode_offset_fetch_response, decode_subscription, decode_sync_group_response,
    encode_find_coordinator_request_typed, encode_heartbeat_request, encode_join_group_request,
    encode_leave_group_request, encode_offset_commit_request, encode_offset_fetch_request,
    encode_subscription, encode_sync_group_request, encode_tp_assignment, FetchedOffsetTopic,
    JoinGroupRequest, OffsetFetchTopic, OffsetPartition, OffsetTopic, COORDINATOR_GROUP,
};
use crate::protocol::sasl;

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

fn members_for_topic(member_subs: &[(String, Vec<String>)], topic: &str) -> Vec<String> {
    member_subs
        .iter()
        .filter(|(_, subs)| subs.iter().any(|t| t == topic))
        .map(|(id, _)| id.clone())
        .collect()
}

pub struct ConsumerGroup {
    consumer: Consumer,
    coord: BrokerConn,
    cfg: ConsumerConfig,
    group_id: String,
    member_id: String,
    generation_id: i32,
    topics: Vec<String>,
    protocol: String,
    kip848: bool,
    prev_assignment: HashMap<String, Vec<(String, i32)>>,
    hb_err: Arc<AtomicI16>,
    hb_generation: Arc<AtomicI32>,
    /// Assignment from a later ConsumerGroupHeartbeat; applied on `poll` / `commit`.
    hb_assignment: Arc<parking_lot::Mutex<Option<Vec<TopicPartitions>>>>,
    /// Last applied assignment, sent once on the next heartbeat (KIP-848 ack).
    hb_ack: Arc<parking_lot::Mutex<Option<Vec<TopicPartitions>>>>,
    hb_stop: watch::Sender<bool>,
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

    async fn join_with_protocol(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
        protocol: &str,
    ) -> Result<Self> {
        let group_id = group_id.into();
        let topics = collect_topics(topics)?;
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
            protocol: protocol.to_string(),
            kip848: false,
            prev_assignment: HashMap::new(),
            hb_err,
            hb_generation,
            hb_assignment,
            hb_ack,
            hb_stop,
        };
        g.rejoin().await?;
        g.spawn_heartbeat(hb_rx);
        Ok(g)
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
            protocol: "consumer".into(),
            kip848: true,
            prev_assignment: HashMap::new(),
            hb_err,
            hb_generation,
            hb_assignment,
            hb_ack,
            hb_stop,
        };
        g.heartbeat_join().await?;
        g.spawn_heartbeat_consumer(hb_rx);
        Ok(g)
    }

    /// Subscribed topic names, in join order.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    pub fn assignment(&self) -> Vec<(String, i32)> {
        self.consumer
            .assignment()
            .iter()
            .map(|(t, p, _)| (t.clone(), *p))
            .collect()
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Stop fetching these assigned partitions until [`resume`](Self::resume).
    ///
    /// Pause is stored on the consumer, so it survives rebalance.
    pub fn pause(&mut self, partitions: &[(String, i32)]) {
        self.consumer.pause(partitions);
    }

    /// Undo [`pause`](Self::pause) for these partitions.
    pub fn resume(&mut self, partitions: &[(String, i32)]) {
        self.consumer.resume(partitions);
    }

    /// Assigned partitions that [`poll`](Self::poll) currently skips.
    pub fn paused(&self) -> Vec<(String, i32)> {
        self.consumer.paused()
    }

    /// Next fetch offset for an assigned partition.
    pub fn position(&self, topic: &str, partition: i32) -> Result<i64> {
        self.consumer.position(topic, partition)
    }

    /// Last committed offsets for the current assignment (`OffsetFetch`).
    ///
    /// Partitions with no committed offset return `-1`.
    pub async fn committed(&mut self) -> Result<Vec<(String, i32, i64)>> {
        if self.kip848 {
            self.apply_pending_assignment().await?;
        }
        let assigned: Vec<(String, i32)> = self.assignment();
        if assigned.is_empty() {
            return Ok(Vec::new());
        }
        let topics = group_offset_fetch_topics(&assigned);
        let timeout = Duration::from_secs(30);
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            OFFSET_FETCH,
            5,
            |buf| encode_offset_fetch_request(buf, &self.group_id, &topics),
            timeout,
        )
        .await?;
        let fetched = decode_offset_fetch_response(&mut body.clone())?;
        let map = committed_offset_map(&fetched)?;
        Ok(assigned
            .into_iter()
            .map(|(t, p)| {
                let off = map.get(&(t.clone(), p)).copied().unwrap_or(-1);
                (t, p, off)
            })
            .collect())
    }

    /// Fetch the current assignment. Rejoins on a classic rebalance.
    pub async fn poll(&mut self) -> Result<Vec<FetchedRecord>> {
        if self.kip848 {
            self.apply_pending_assignment().await?;
        } else if self.hb_err.load(Ordering::SeqCst) == error::REBALANCE_IN_PROGRESS {
            self.rejoin().await?;
        }
        self.consumer.fetch().await
    }

    /// Commit the next fetch offsets for the current assignment.
    pub async fn commit(&mut self) -> Result<()> {
        if self.kip848 {
            self.apply_pending_assignment().await?;
        }
        let assigned = self.consumer.assignment().to_vec();
        let topics = group_offset_topics(&assigned);
        if topics.is_empty() {
            return Ok(());
        }
        let timeout = Duration::from_secs(30);
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            OFFSET_COMMIT,
            7,
            |buf| {
                encode_offset_commit_request(
                    buf,
                    &self.group_id,
                    self.generation_id,
                    &self.member_id,
                    &topics,
                )
            },
            timeout,
        )
        .await?;
        let err = decode_offset_commit_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "OffsetCommit"));
        }
        Ok(())
    }

    pub async fn leave(mut self) -> Result<()> {
        self.hb_stop.send(true).unwrap_or(());
        let timeout = Duration::from_secs(30);
        if self.kip848 {
            let req = ConsumerGroupHeartbeatRequest {
                group_id: self.group_id.clone(),
                member_id: self.member_id.clone(),
                member_epoch: -1,
                instance_id: self.cfg.group_instance_id.clone(),
                rack_id: self.cfg.rack.clone(),
                subscribed_topic_names: None,
                topic_partitions: None,
            };
            let body = coord_roundtrip(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
                CONSUMER_GROUP_HEARTBEAT,
                0,
                |buf| encode_consumer_group_heartbeat_request(buf, &req),
                timeout,
            )
            .await?;
            let resp = decode_consumer_group_heartbeat_response(&mut body.clone())?;
            if resp.error_code != 0 {
                return Err(Error::broker(
                    resp.error_code,
                    "ConsumerGroupHeartbeat leave",
                ));
            }
            return Ok(());
        }
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            LEAVE_GROUP,
            0,
            |buf| encode_leave_group_request(buf, &self.group_id, &self.member_id),
            timeout,
        )
        .await?;
        let err = decode_leave_group_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "LeaveGroup"));
        }
        Ok(())
    }

    async fn rejoin(&mut self) -> Result<()> {
        let timeout = Duration::from_secs(30);
        let metadata = encode_subscription(&self.topics)?;
        if self.member_id.is_empty() {
            let body = coord_roundtrip(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
                JOIN_GROUP,
                5,
                |buf| {
                    encode_join_group_request(
                        buf,
                        &JoinGroupRequest {
                            group_id: &self.group_id,
                            session_timeout_ms: 10_000,
                            member_id: "",
                            group_instance_id: self.cfg.group_instance_id.as_deref(),
                            protocol_type: "consumer",
                            protocol_name: &self.protocol,
                            metadata: &metadata,
                        },
                    )
                },
                timeout,
            )
            .await?;
            let (error, _, _, _, assigned_id, _) = decode_join_group_response(&mut body.clone())?;
            self.member_id = assigned_id;
            if error != 0 && error != error::MEMBER_ID_REQUIRED {
                return Err(Error::broker(error, "JoinGroup"));
            }
        }
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            JOIN_GROUP,
            5,
            |buf| {
                encode_join_group_request(
                    buf,
                    &JoinGroupRequest {
                        group_id: &self.group_id,
                        session_timeout_ms: 10_000,
                        member_id: &self.member_id,
                        group_instance_id: self.cfg.group_instance_id.as_deref(),
                        protocol_type: "consumer",
                        protocol_name: &self.protocol,
                        metadata: &metadata,
                    },
                )
            },
            timeout,
        )
        .await?;
        let (error, generation, protocol, leader, assigned_id, members) =
            decode_join_group_response(&mut body.clone())?;
        if error != 0 {
            return Err(Error::broker(error, "JoinGroup"));
        }
        self.member_id = assigned_id;
        self.generation_id = generation;
        let _ = protocol;

        let mut member_subs: Vec<(String, Vec<String>)> = Vec::with_capacity(members.len());
        let mut topic_set = self.topics.clone();
        for m in &members {
            let subs = match decode_subscription(&m.metadata) {
                Ok(t) if !t.is_empty() => t,
                _ => self.topics.clone(),
            };
            for t in &subs {
                if !topic_set.iter().any(|x| x == t) {
                    topic_set.push(t.clone());
                }
            }
            member_subs.push((m.member_id.clone(), subs));
        }
        self.consumer.refresh_topics(&topic_set).await?;
        let mut by_topic = Vec::with_capacity(topic_set.len());
        for topic in &topic_set {
            by_topic.push((topic.clone(), self.consumer.partition_ids(topic).await?));
        }
        let assignments = if leader == self.member_id {
            let map = if self.protocol == "sticky" {
                assign_sticky_subscribed(&member_subs, &by_topic, &self.prev_assignment)
            } else {
                assign_range_subscribed(&member_subs, &by_topic)
            };
            self.prev_assignment = map.clone();
            map.into_iter()
                .map(|(id, tps)| Ok((id, encode_tp_assignment(&tps)?)))
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            SYNC_GROUP,
            3,
            |buf| {
                encode_sync_group_request(
                    buf,
                    &self.group_id,
                    self.generation_id,
                    &self.member_id,
                    &assignments,
                )
            },
            timeout,
        )
        .await?;
        let (err, assignment) = decode_sync_group_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "SyncGroup"));
        }
        let assigned = decode_assignment(&assignment)?;
        let wanted: Vec<(String, i32)> = assigned
            .into_iter()
            .flat_map(|(t, ps)| ps.into_iter().map(move |p| (t.clone(), p)))
            .collect();
        self.assign_committed(&wanted).await?;
        self.hb_generation
            .store(self.generation_id, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(())
    }

    async fn heartbeat_join(&mut self) -> Result<()> {
        let timeout = Duration::from_secs(30);
        self.consumer.refresh_topics(&self.topics).await?;
        let req = ConsumerGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: 0,
            instance_id: self.cfg.group_instance_id.clone(),
            rack_id: self.cfg.rack.clone(),
            subscribed_topic_names: Some(self.topics.clone()),
            topic_partitions: None,
        };
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_GROUP,
            CONSUMER_GROUP_HEARTBEAT,
            0,
            |buf| encode_consumer_group_heartbeat_request(buf, &req),
            timeout,
        )
        .await?;
        let resp = decode_consumer_group_heartbeat_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ConsumerGroupHeartbeat"));
        }
        if let Some(id) = resp.member_id {
            self.member_id = id;
        }
        self.generation_id = resp.member_epoch;
        let assignment = resp.assignment.unwrap_or_default();
        let wanted = wanted_from_kip848(&self.topics, &self.consumer.topic_id_names(), &assignment);
        self.assign_committed(&wanted).await?;
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
        self.assign_committed(&wanted).await?;
        *self.hb_ack.lock() = Some(assignment);
        self.generation_id = self.hb_generation.load(Ordering::SeqCst);
        Ok(())
    }

    async fn assign_committed(&mut self, wanted: &[(String, i32)]) -> Result<()> {
        let current: HashMap<(String, i32), i64> = self
            .consumer
            .assignment()
            .iter()
            .map(|(t, p, o)| ((t.clone(), *p), *o))
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
            let timeout = Duration::from_secs(30);
            let body = coord_roundtrip(
                &mut self.coord,
                &self.cfg,
                &self.group_id,
                COORDINATOR_GROUP,
                OFFSET_FETCH,
                5,
                |buf| encode_offset_fetch_request(buf, &self.group_id, &topics),
                timeout,
            )
            .await?;
            decode_offset_fetch_response(&mut body.clone())?
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
            let committed = map.get(&key).copied().unwrap_or(-1);
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
        Ok(())
    }

    fn spawn_heartbeat_consumer(&self, mut stop: watch::Receiver<bool>) {
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_generation = self.hb_generation.clone();
        let hb_assignment = self.hb_assignment.clone();
        let hb_ack = self.hb_ack.clone();
        let cfg = self.cfg.clone();
        drop(tokio::spawn(async move {
            let mut conn: Option<BrokerConn> = None;
            let mut tick = tokio::time::interval(Duration::from_millis(150));
            loop {
                tokio::select! {
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                    _ = tick.tick() => {
                        if conn.is_none() {
                            conn = discover_coord(&cfg, &group_id, COORDINATOR_GROUP).await.ok();
                        }
                        let Some(c) = conn.as_mut() else {
                            continue;
                        };
                        let timeout = Duration::from_secs(10);
                        let epoch = hb_generation.load(Ordering::SeqCst);
                        let topic_partitions = hb_ack.lock().clone();
                        let req = ConsumerGroupHeartbeatRequest {
                            group_id: group_id.clone(),
                            member_id: member_id.clone(),
                            member_epoch: epoch,
                            instance_id: cfg.group_instance_id.clone(),
                            rack_id: cfg.rack.clone(),
                            subscribed_topic_names: None,
                            topic_partitions,
                        };
                        let res = c
                            .roundtrip(
                                CONSUMER_GROUP_HEARTBEAT,
                                0,
                                |buf| encode_consumer_group_heartbeat_request(buf, &req),
                                timeout,
                            )
                            .await;
                        match res {
                            Ok(body) => {
                                if let Ok(resp) =
                                    decode_consumer_group_heartbeat_response(&mut body.clone())
                                {
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
        let cfg = self.cfg.clone();
        drop(tokio::spawn(async move {
            let mut conn: Option<BrokerConn> = None;
            let mut tick = tokio::time::interval(Duration::from_millis(150));
            loop {
                tokio::select! {
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                    _ = tick.tick() => {
                        if conn.is_none() {
                            conn = discover_coord(&cfg, &group_id, COORDINATOR_GROUP).await.ok();
                        }
                        let Some(c) = conn.as_mut() else {
                            continue;
                        };
                        let timeout = Duration::from_secs(10);
                        let gid = group_id.clone();
                        let mid = member_id.clone();
                        let instance = cfg.group_instance_id.clone();
                        let generation = hb_generation.load(Ordering::SeqCst);
                        let res = c
                            .roundtrip(
                                HEARTBEAT,
                                3,
                                |buf| {
                                    encode_heartbeat_request(
                                        buf,
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
                                if let Ok(err) = decode_heartbeat_response(&mut body.clone()) {
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

fn group_offset_topics(assigned: &[(String, i32, i64)]) -> Vec<OffsetTopic> {
    let mut by_topic: HashMap<String, Vec<OffsetPartition>> = HashMap::new();
    for (topic, part, next) in assigned {
        by_topic
            .entry(topic.clone())
            .or_default()
            .push(OffsetPartition {
                partition: *part,
                offset: *next,
            });
    }
    by_topic
        .into_iter()
        .map(|(topic, partitions)| OffsetTopic { topic, partitions })
        .collect()
}

fn group_offset_fetch_topics(wanted: &[(String, i32)]) -> Vec<OffsetFetchTopic> {
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for (topic, part) in wanted {
        by_topic.entry(topic.clone()).or_default().push(*part);
    }
    by_topic
        .into_iter()
        .map(|(topic, partitions)| OffsetFetchTopic { topic, partitions })
        .collect()
}

fn committed_offset_map(fetched: &[FetchedOffsetTopic]) -> Result<HashMap<(String, i32), i64>> {
    let mut map = HashMap::new();
    for t in fetched {
        for p in &t.partitions {
            if p.error_code != 0 {
                return Err(Error::broker(
                    p.error_code,
                    format!("OffsetFetch {}-{}", t.topic, p.partition),
                ));
            }
            let _ = map.insert((t.topic.clone(), p.partition), p.offset);
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
        let committed = map.get(&(topic.clone(), *part)).copied().unwrap_or(-1);
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
            let mut hop = match open_coord(cfg, addr).await {
                Ok(c) => c,
                Err(e) => {
                    last = e;
                    continue;
                }
            };
            let body = match hop
                .roundtrip(
                    FIND_COORDINATOR,
                    2,
                    |buf| encode_find_coordinator_request_typed(buf, group_id, key_type),
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
            let (err, _node, host, port) = decode_find_coordinator_response(&mut body.clone())?;
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
    let mut conn =
        BrokerConn::connect_tls(addr, &cfg.client_id, cfg.connect_timeout, cfg.tls.as_ref())
            .await?;
    let body = conn
        .roundtrip(
            API_VERSIONS,
            3,
            |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
            cfg.request_timeout,
        )
        .await?;
    let resp = decode_api_versions_response(&mut body.clone(), 3)?;
    if resp.error_code != 0 {
        return Err(Error::broker(resp.error_code, "ApiVersions"));
    }
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
    Ok(conn)
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
    if coordinator_error(api_key, &body).is_some_and(error::coordinator_retriable) {
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
fn coordinator_error(api_key: i16, body: &[u8]) -> Option<i16> {
    match api_key {
        OFFSET_COMMIT => match decode_offset_commit_response(&mut { body }) {
            Ok(0) => None,
            Ok(code) => Some(code),
            Err(_) => None,
        },
        OFFSET_FETCH => match decode_offset_fetch_response(&mut { body }) {
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
    use crate::protocol::group::FetchedOffset;
    use std::collections::HashMap;

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
    fn group_offset_topics_collapses_partitions() {
        let assigned = vec![("t".into(), 1, 4), ("t".into(), 0, 2), ("u".into(), 0, 9)];
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
            partitions: vec![OffsetPartition {
                partition: 0,
                offset: 1,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_response(&mut buf, &topics, error::COORDINATOR_LOAD_IN_PROGRESS)
            .unwrap();
        assert_ne!(
            peek_error_code(&buf),
            Some(error::COORDINATOR_LOAD_IN_PROGRESS),
            "throttle + topic-array length must not look like error 14"
        );
        assert_eq!(
            coordinator_error(OFFSET_COMMIT, &buf),
            Some(error::COORDINATOR_LOAD_IN_PROGRESS)
        );
    }

    #[test]
    fn offset_commit_not_coordinator_is_not_at_byte_four() {
        use crate::protocol::api_keys::OFFSET_COMMIT;
        use crate::protocol::group::{encode_offset_commit_response, OffsetPartition, OffsetTopic};
        let topics = vec![OffsetTopic {
            topic: "t".into(),
            partitions: vec![OffsetPartition {
                partition: 0,
                offset: 1,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_offset_commit_response(&mut buf, &topics, error::NOT_COORDINATOR).unwrap();
        assert_ne!(
            peek_error_code(&buf),
            Some(error::NOT_COORDINATOR),
            "throttle + topic-array length must not look like error 16"
        );
        assert_eq!(
            coordinator_error(OFFSET_COMMIT, &buf),
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
            partitions: vec![FetchedOffset {
                partition: 0,
                offset: -1,
                error_code: error::NOT_COORDINATOR,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_offset_fetch_response(&mut buf, &topics).unwrap();
        assert_ne!(
            peek_error_code(&buf),
            Some(error::NOT_COORDINATOR),
            "throttle + topic-array length must not look like error 16"
        );
        assert_eq!(
            coordinator_error(OFFSET_FETCH, &buf),
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
            partitions: vec![FetchedOffset {
                partition: 0,
                offset: 5,
                error_code: 0,
            }],
        }];
        let starts = committed_starts(&wanted, &fetched, AutoOffsetReset::Earliest).unwrap();
        assert_eq!(starts, vec![("t".into(), 0, 5), ("t".into(), 1, 0)]);
        assert!(committed_starts(&wanted, &fetched, AutoOffsetReset::None).is_err());
    }
}
