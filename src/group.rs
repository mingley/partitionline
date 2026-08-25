#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::consumer::{Consumer, ConsumerConfig, FetchedRecord};
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{decode_api_versions_response, encode_api_versions_request};
use crate::protocol::api_keys::{
    API_VERSIONS, FIND_COORDINATOR, HEARTBEAT, JOIN_GROUP, LEAVE_GROUP, OFFSET_COMMIT,
    OFFSET_FETCH, SYNC_GROUP,
};
use crate::protocol::group::{
    decode_assignment, decode_find_coordinator_response, decode_heartbeat_response,
    decode_join_group_response, decode_leave_group_response, decode_offset_commit_response,
    decode_offset_fetch_response, decode_sync_group_response, encode_assignment,
    encode_find_coordinator_request, encode_heartbeat_request, encode_join_group_request,
    encode_leave_group_request, encode_offset_commit_request, encode_offset_fetch_request,
    encode_subscription, encode_sync_group_request,
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

pub struct ConsumerGroup {
    consumer: Consumer,
    coord: BrokerConn,
    group_id: String,
    member_id: String,
    generation_id: i32,
    topic: String,
    protocol: String,
    prev_assignment: HashMap<String, Vec<i32>>,
    hb_err: Arc<AtomicI16>,
    hb_generation: Arc<std::sync::atomic::AtomicI32>,
    hb_stop: watch::Sender<bool>,
}

impl ConsumerGroup {
    pub async fn join(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        Self::join_with_protocol(cfg, group_id, topic, "range").await
    }

    pub async fn join_sticky(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        Self::join_with_protocol(cfg, group_id, topic, "sticky").await
    }

    async fn join_with_protocol(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
        protocol: &str,
    ) -> Result<Self> {
        let group_id = group_id.into();
        let topic = topic.into();
        let mut consumer = Consumer::new(cfg.clone()).await?;
        let timeout = Duration::from_secs(30);
        let mut coord = open_coord(&cfg, consumer.conn_mut().addr()).await?;

        let body = coord
            .roundtrip(
                FIND_COORDINATOR,
                2,
                |buf| encode_find_coordinator_request(buf, &group_id),
                timeout,
            )
            .await?;
        let (err, _node, host, port) = decode_find_coordinator_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "FindCoordinator"));
        }
        let coord_addr = format!("{host}:{port}");
        if coord_addr != coord.addr() {
            coord = open_coord(&cfg, &coord_addr).await?;
        }

        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_generation = Arc::new(AtomicI32::new(0));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            consumer,
            coord,
            group_id,
            member_id: String::new(),
            generation_id: 0,
            topic,
            protocol: protocol.to_string(),
            prev_assignment: HashMap::new(),
            hb_err,
            hb_generation,
            hb_stop,
        };
        g.rejoin().await?;
        g.spawn_heartbeat(hb_rx);
        Ok(g)
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

    pub async fn poll(&mut self) -> Result<Vec<FetchedRecord>> {
        if self.hb_err.load(Ordering::SeqCst) == error::REBALANCE_IN_PROGRESS {
            self.rejoin().await?;
        }
        self.consumer.fetch().await
    }

    pub async fn commit(&mut self) -> Result<()> {
        let timeout = Duration::from_secs(30);
        let assigned = self.consumer.assignment().to_vec();
        for (topic, part, next) in assigned {
            let body = self
                .coord
                .roundtrip(
                    OFFSET_COMMIT,
                    7,
                    |buf| {
                        encode_offset_commit_request(
                            buf,
                            &self.group_id,
                            self.generation_id,
                            &self.member_id,
                            &topic,
                            part,
                            next,
                        )
                    },
                    timeout,
                )
                .await?;
            let err = decode_offset_commit_response(&mut body.clone())?;
            if err != 0 {
                return Err(Error::broker(err, "OffsetCommit"));
            }
        }
        Ok(())
    }

    pub async fn leave(mut self) -> Result<()> {
        self.hb_stop.send(true).unwrap_or(());
        let timeout = Duration::from_secs(30);
        let body = self
            .coord
            .roundtrip(
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
        let metadata = encode_subscription(std::slice::from_ref(&self.topic))?;
        if self.member_id.is_empty() {
            let body = self
                .coord
                .roundtrip(
                    JOIN_GROUP,
                    5,
                    |buf| {
                        encode_join_group_request(
                            buf,
                            &self.group_id,
                            10_000,
                            "",
                            "consumer",
                            &self.protocol,
                            &metadata,
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
        let body = self
            .coord
            .roundtrip(
                JOIN_GROUP,
                5,
                |buf| {
                    encode_join_group_request(
                        buf,
                        &self.group_id,
                        10_000,
                        &self.member_id,
                        "consumer",
                        &self.protocol,
                        &metadata,
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

        let parts = self.consumer.partition_ids(&self.topic).await?;
        let member_ids: Vec<String> = members.iter().map(|m| m.member_id.clone()).collect();
        let assignments = if leader == self.member_id {
            let map = if self.protocol == "sticky" {
                assign_sticky(&member_ids, &parts, &self.prev_assignment)
            } else {
                assign_range(&member_ids, &parts)
            };
            self.prev_assignment = map.clone();
            map.into_iter()
                .map(|(id, ps)| Ok((id, encode_assignment(&self.topic, &ps)?)))
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let body = self
            .coord
            .roundtrip(
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
        self.consumer.clear_assignment();
        for (t, ps) in assigned {
            for p in ps {
                let body = self
                    .coord
                    .roundtrip(
                        OFFSET_FETCH,
                        5,
                        |buf| encode_offset_fetch_request(buf, &self.group_id, &t, p),
                        timeout,
                    )
                    .await?;
                let committed = decode_offset_fetch_response(&mut body.clone()).unwrap_or(-1);
                let start = if committed < 0 { 0 } else { committed };
                self.consumer.assign(t.clone(), p, start).await?;
            }
        }
        self.hb_generation
            .store(self.generation_id, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn spawn_heartbeat(&self, mut stop: watch::Receiver<bool>) {
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_generation = self.hb_generation.clone();
        let addr = self.coord.addr().to_string();
        drop(tokio::spawn(async move {
            let Ok(mut conn) =
                BrokerConn::connect(&addr, "partitionline-hb", Duration::from_secs(10)).await
            else {
                return;
            };
            let mut tick = tokio::time::interval(Duration::from_millis(150));
            loop {
                tokio::select! {
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                    _ = tick.tick() => {
                        let timeout = Duration::from_secs(10);
                        let gid = group_id.clone();
                        let mid = member_id.clone();
                        let generation = hb_generation.load(Ordering::SeqCst);
                        let res = conn
                            .roundtrip(
                                HEARTBEAT,
                                3,
                                |buf| encode_heartbeat_request(buf, &gid, generation, &mid),
                                timeout,
                            )
                            .await;
                        match res {
                            Ok(body) => {
                                if let Ok(err) = decode_heartbeat_response(&mut body.clone()) {
                                    hb_err.store(err, Ordering::SeqCst);
                                }
                            }
                            Err(_) => {
                                hb_err.store(error::REQUEST_TIMED_OUT, Ordering::SeqCst);
                            }
                        }
                    }
                }
            }
        }));
    }
}

async fn open_coord(cfg: &ConsumerConfig, addr: &str) -> Result<BrokerConn> {
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
        cfg.request_timeout,
    )
    .await?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
