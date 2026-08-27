//! Share groups (KIP-932): queue-style consumption with per-record ack.

#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::watch;

use crate::consumer::{Consumer, ConsumerConfig};
use crate::error::{self, Error, Result};
use crate::group::{coord_roundtrip, discover_coord};
use crate::net::BrokerConn;
use crate::protocol::api_keys::{SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_HEARTBEAT};
use crate::protocol::group::COORDINATOR_SHARE;
use crate::protocol::share::{
    decode_share_acknowledge_response, decode_share_fetch_response,
    decode_share_group_heartbeat_response, encode_share_acknowledge_request,
    encode_share_fetch_request, encode_share_group_heartbeat_request, AcknowledgementBatch,
    ShareFetchPartition, ShareFetchTopic, ShareGroupHeartbeatRequest, ACK_ACCEPT, ACK_REJECT,
    ACK_RELEASE,
};

pub use crate::protocol::share::{
    ACK_ACCEPT as SHARE_ACK_ACCEPT, ACK_REJECT as SHARE_ACK_REJECT,
    ACK_RELEASE as SHARE_ACK_RELEASE,
};

#[derive(Debug, Clone)]
pub struct ShareRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub delivery_count: i16,
}

pub struct ShareGroup {
    consumer: Consumer,
    coord: BrokerConn,
    cfg: ConsumerConfig,
    group_id: String,
    member_id: String,
    member_epoch: i32,
    topic: String,
    topic_id: [u8; 16],
    partitions: Vec<i32>,
    /// Share session epoch per share-partition leader (KIP-932).
    share_epochs: HashMap<i32, i32>,
    hb_err: Arc<AtomicI16>,
    hb_epoch: Arc<AtomicI32>,
    hb_stop: watch::Sender<bool>,
}

fn new_member_id() -> Result<String> {
    let mut raw = [0u8; 8];
    getrandom::getrandom(&mut raw).map_err(|_| Error::protocol("share member id rng"))?;
    let mut hex = String::with_capacity(16);
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for b in raw {
        let hi = usize::from(b >> 4);
        let lo = usize::from(b & 0x0f);
        if let (Some(&h), Some(&l)) = (DIGITS.get(hi), DIGITS.get(lo)) {
            hex.push(char::from(h));
            hex.push(char::from(l));
        }
    }
    Ok(format!("s-{hex}"))
}

impl ShareGroup {
    pub async fn join(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        let group_id = group_id.into();
        let topic = topic.into();
        let consumer = Consumer::new(cfg.clone()).await?;
        let coord = discover_coord(&cfg, &group_id, COORDINATOR_SHARE).await?;
        let member_id = new_member_id()?;
        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_epoch = Arc::new(AtomicI32::new(0));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            consumer,
            coord,
            cfg: cfg.clone(),
            group_id,
            member_id,
            member_epoch: 0,
            topic,
            topic_id: [0u8; 16],
            partitions: Vec::new(),
            share_epochs: HashMap::new(),
            hb_err,
            hb_epoch,
            hb_stop,
        };
        g.heartbeat_join().await?;
        g.spawn_heartbeat(hb_rx);
        Ok(g)
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn assignment(&self) -> Vec<(String, i32)> {
        self.partitions
            .iter()
            .map(|p| (self.topic.clone(), *p))
            .collect()
    }

    async fn heartbeat_join(&mut self) -> Result<()> {
        let timeout = Duration::from_secs(30);
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![self.topic.clone()]),
        };
        let body = self
            .coord
            .roundtrip(
                SHARE_GROUP_HEARTBEAT,
                1,
                |buf| encode_share_group_heartbeat_request(buf, &req),
                timeout,
            )
            .await?;
        let resp = decode_share_group_heartbeat_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ShareGroupHeartbeat"));
        }
        if let Some(id) = resp.member_id {
            if !id.is_empty() {
                self.member_id = id;
            }
        }
        self.member_epoch = resp.member_epoch;
        self.partitions.clear();
        if let Some(assigned) = resp.assignment {
            for tp in assigned {
                self.topic_id = tp.topic_id;
                self.partitions.extend(tp.partitions);
            }
        }
        if self.partitions.is_empty() {
            self.partitions.push(0);
        }
        self.hb_epoch.store(self.member_epoch, Ordering::SeqCst);
        self.hb_err.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub async fn poll(&mut self) -> Result<Vec<ShareRecord>> {
        let hb = self.hb_err.load(Ordering::SeqCst);
        if hb != 0 {
            return Err(Error::broker(hb, "ShareGroupHeartbeat"));
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.poll_leaders().await {
                Ok(recs) => return Ok(recs),
                Err(e) if share_leader_retriable(&e) => {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    self.consumer.invalidate_topic(&self.topic);
                    self.consumer.refresh_topic_metadata(&self.topic).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn accept(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, ACK_ACCEPT).await
    }

    pub async fn release(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, ACK_RELEASE).await
    }

    pub async fn reject(&mut self, recs: &[ShareRecord]) -> Result<()> {
        self.acknowledge(recs, ACK_REJECT).await
    }

    fn session_epoch(&self, node: i32) -> i32 {
        self.share_epochs.get(&node).copied().unwrap_or(0)
    }

    fn advance_node_epoch(&mut self, node: i32) {
        let next = self.session_epoch(node).saturating_add(1);
        let _ = self.share_epochs.insert(node, next);
    }

    fn reset_node_session(&mut self, node: i32) {
        let _ = self.share_epochs.remove(&node);
        self.consumer.drop_node(node);
    }

    async fn leaders_of(&mut self, parts: &[i32]) -> Result<HashMap<i32, Vec<i32>>> {
        self.consumer.ensure_topic_metadata(&self.topic).await?;
        let mut by_leader: HashMap<i32, Vec<i32>> = HashMap::new();
        for p in parts {
            let (node, _) = self.consumer.leader_of(&self.topic, *p)?;
            by_leader.entry(node).or_default().push(*p);
        }
        Ok(by_leader)
    }

    async fn poll_leaders(&mut self) -> Result<Vec<ShareRecord>> {
        let assigned = self.partitions.clone();
        let by_leader = self.leaders_of(&assigned).await?;
        let timeout = Duration::from_secs(30);
        let max_wait = 10i32;
        let mut out = Vec::new();
        for (node, parts) in by_leader {
            let epoch = self.session_epoch(node);
            let topics = vec![ShareFetchTopic {
                topic_id: self.topic_id,
                partitions: parts
                    .iter()
                    .map(|p| ShareFetchPartition {
                        partition: *p,
                        acknowledgements: Vec::new(),
                    })
                    .collect(),
            }];
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_FETCH,
                    1,
                    |buf| {
                        encode_share_fetch_request(
                            buf,
                            &self.group_id,
                            &self.member_id,
                            epoch,
                            max_wait,
                            1,
                            1_048_576,
                            16,
                            &topics,
                        )
                    },
                    timeout,
                )
                .await;
            let mut body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    self.reset_node_session(node);
                    return Err(e);
                }
                Err(e) => return Err(e),
            };
            let fetched = match decode_share_fetch_response(&mut body) {
                Ok(f) => f,
                Err(e) => {
                    if share_session_reset(&e) || share_leader_retriable(&e) {
                        self.reset_node_session(node);
                    }
                    return Err(e);
                }
            };
            for topic in &fetched {
                for part in &topic.partitions {
                    if part.error_code != 0 {
                        let e = Error::broker(part.error_code, "ShareFetch");
                        if share_leader_retriable(&e) || share_session_reset(&e) {
                            self.reset_node_session(node);
                        }
                        return Err(e);
                    }
                }
            }
            self.advance_node_epoch(node);
            for topic in fetched {
                for part in topic.partitions {
                    for batch in part.records {
                        for rec in batch.records {
                            let delivery = part
                                .acquired
                                .iter()
                                .find(|a| {
                                    rec.offset >= a.first_offset && rec.offset <= a.last_offset
                                })
                                .map(|a| a.delivery_count)
                                .unwrap_or(1);
                            out.push(ShareRecord {
                                topic: self.topic.clone(),
                                partition: part.partition,
                                offset: rec.offset,
                                timestamp: rec.timestamp,
                                key: rec.key,
                                value: rec.value,
                                delivery_count: delivery,
                            });
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    async fn acknowledge(&mut self, recs: &[ShareRecord], ack: i8) -> Result<()> {
        if recs.is_empty() {
            return Ok(());
        }
        let partitions = acknowledgement_batches(recs, ack);
        if partitions.is_empty() {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.acknowledge_leaders(&partitions).await {
                Ok(()) => return Ok(()),
                Err(e) if share_leader_retriable(&e) => {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    self.consumer.invalidate_topic(&self.topic);
                    self.consumer.refresh_topic_metadata(&self.topic).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn acknowledge_leaders(
        &mut self,
        partitions: &[(i32, Vec<AcknowledgementBatch>)],
    ) -> Result<()> {
        let parts: Vec<i32> = partitions.iter().map(|(p, _)| *p).collect();
        let by_leader = self.leaders_of(&parts).await?;
        let timeout = Duration::from_secs(30);
        for (node, node_parts) in by_leader {
            let epoch = self.session_epoch(node);
            if epoch <= 0 {
                return Err(Error::protocol(
                    "ShareAcknowledge requires an open share session (poll first)",
                ));
            }
            let batches: Vec<(i32, Vec<AcknowledgementBatch>)> = partitions
                .iter()
                .filter(|(p, _)| node_parts.contains(p))
                .map(|(p, b)| (*p, b.clone()))
                .collect();
            let topic_id = self.topic_id;
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_ACKNOWLEDGE,
                    1,
                    |buf| {
                        encode_share_acknowledge_request(
                            buf,
                            &self.group_id,
                            &self.member_id,
                            epoch,
                            topic_id,
                            &batches,
                        )
                    },
                    timeout,
                )
                .await;
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    self.reset_node_session(node);
                    return Err(e);
                }
                Err(e) => return Err(e),
            };
            let err = decode_share_acknowledge_response(&mut body.clone())?;
            if err != 0 {
                let e = Error::broker(err, "ShareAcknowledge");
                if share_leader_retriable(&e) || share_session_reset(&e) {
                    self.reset_node_session(node);
                }
                return Err(e);
            }
            self.advance_node_epoch(node);
        }
        Ok(())
    }

    async fn close_share_session(&mut self) -> Result<()> {
        let open: Vec<i32> = self
            .share_epochs
            .iter()
            .filter(|(_, e)| **e > 0)
            .map(|(n, _)| *n)
            .collect();
        if open.is_empty() {
            return Ok(());
        }
        let timeout = Duration::from_secs(30);
        let topic_id = self.topic_id;
        let mut last = Ok(());
        for node in open {
            let body = self
                .consumer
                .roundtrip_node(
                    node,
                    SHARE_ACKNOWLEDGE,
                    1,
                    |buf| {
                        encode_share_acknowledge_request(
                            buf,
                            &self.group_id,
                            &self.member_id,
                            -1,
                            topic_id,
                            &[],
                        )
                    },
                    timeout,
                )
                .await;
            let err = match body {
                Ok(body) => decode_share_acknowledge_response(&mut body.clone())?,
                Err(_) => error::SHARE_SESSION_NOT_FOUND,
            };
            let _ = self.share_epochs.remove(&node);
            if err != 0 && err != error::SHARE_SESSION_NOT_FOUND {
                last = Err(Error::broker(err, "ShareAcknowledge close"));
            }
        }
        last
    }

    pub async fn leave(mut self) -> Result<()> {
        self.hb_stop.send(true).unwrap_or(());
        let timeout = Duration::from_secs(30);
        self.close_share_session().await?;
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: -1,
            subscribed_topic_names: None,
        };
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_SHARE,
            SHARE_GROUP_HEARTBEAT,
            1,
            |buf| encode_share_group_heartbeat_request(buf, &req),
            timeout,
        )
        .await?;
        let resp = decode_share_group_heartbeat_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ShareGroupHeartbeat leave"));
        }
        Ok(())
    }

    fn spawn_heartbeat(&self, mut stop: watch::Receiver<bool>) {
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_epoch = self.hb_epoch.clone();
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
                            conn = discover_coord(&cfg, &group_id, COORDINATOR_SHARE).await.ok();
                        }
                        let Some(c) = conn.as_mut() else {
                            continue;
                        };
                        let epoch = hb_epoch.load(Ordering::SeqCst);
                        let req = ShareGroupHeartbeatRequest {
                            group_id: group_id.clone(),
                            member_id: member_id.clone(),
                            member_epoch: epoch,
                            subscribed_topic_names: None,
                        };
                        let res = c
                            .roundtrip(
                                SHARE_GROUP_HEARTBEAT,
                                1,
                                |buf| encode_share_group_heartbeat_request(buf, &req),
                                Duration::from_secs(10),
                            )
                            .await;
                        match res {
                            Ok(body) => {
                                if let Ok(resp) =
                                    decode_share_group_heartbeat_response(&mut body.clone())
                                {
                                    if crate::error::coordinator_retriable(resp.error_code) {
                                        conn = None;
                                    } else {
                                        hb_err.store(resp.error_code, Ordering::SeqCst);
                                        if resp.member_epoch > 0 {
                                            hb_epoch.store(resp.member_epoch, Ordering::SeqCst);
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
}

fn share_leader_retriable(e: &Error) -> bool {
    match e {
        Error::NoLeader { .. } => true,
        Error::Broker { code, .. } => matches!(
            *code,
            error::NOT_LEADER_OR_FOLLOWER
                | error::LEADER_NOT_AVAILABLE
                | error::UNKNOWN_TOPIC_OR_PARTITION
        ),
        Error::Io(_) | Error::Timeout => true,
        _ => false,
    }
}

fn share_session_reset(e: &Error) -> bool {
    matches!(
        e,
        Error::Broker {
            code: error::SHARE_SESSION_NOT_FOUND | error::INVALID_SHARE_SESSION_EPOCH,
            ..
        }
    )
}

/// Collapse records into KIP-932 acknowledgement batches.
///
/// Contiguous offsets with the same type become one batch with a single
/// `AcknowledgeType` (applies to the whole range). Gaps start a new batch.
fn acknowledgement_batches(recs: &[ShareRecord], ack: i8) -> Vec<(i32, Vec<AcknowledgementBatch>)> {
    let mut by_part: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
    for rec in recs {
        by_part.entry(rec.partition).or_default().push(rec.offset);
    }
    let mut out = Vec::with_capacity(by_part.len());
    for (partition, mut offs) in by_part {
        offs.sort_unstable();
        offs.dedup();
        let mut batches = Vec::new();
        let mut range: Option<(i64, i64)> = None;
        for off in offs {
            range = match range {
                None => Some((off, off)),
                Some((s, p)) if off == p.saturating_add(1) => Some((s, off)),
                Some((s, p)) => {
                    batches.push(AcknowledgementBatch {
                        first_offset: s,
                        last_offset: p,
                        types: vec![ack],
                    });
                    Some((off, off))
                }
            };
        }
        if let Some((s, p)) = range {
            batches.push(AcknowledgementBatch {
                first_offset: s,
                last_offset: p,
                types: vec![ack],
            });
        }
        if !batches.is_empty() {
            out.push((partition, batches));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(partition: i32, offset: i64) -> ShareRecord {
        ShareRecord {
            topic: "t".into(),
            partition,
            offset,
            timestamp: 0,
            key: None,
            value: None,
            delivery_count: 1,
        }
    }

    #[test]
    fn acknowledgement_batches_collapses_contiguous_offsets() {
        let recs = [rec(0, 1), rec(0, 3), rec(0, 2), rec(1, 9)];
        let batches = acknowledgement_batches(&recs, ACK_ACCEPT);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].0, 0);
        assert_eq!(batches[0].1.len(), 1);
        assert_eq!(batches[0].1[0].first_offset, 1);
        assert_eq!(batches[0].1[0].last_offset, 3);
        assert_eq!(batches[0].1[0].types, vec![ACK_ACCEPT]);
        assert_eq!(batches[1].0, 1);
        assert_eq!(batches[1].1[0].first_offset, 9);
        assert_eq!(batches[1].1[0].last_offset, 9);
    }

    #[test]
    fn share_leader_retriable_is_not_leader_or_missing() {
        assert!(share_leader_retriable(&Error::broker(
            error::NOT_LEADER_OR_FOLLOWER,
            "x"
        )));
        assert!(share_leader_retriable(&Error::NoLeader {
            topic: "t".into(),
            partition: 0,
        }));
        assert!(!share_leader_retriable(&Error::broker(
            error::INVALID_RECORD_STATE,
            "x"
        )));
        assert!(share_session_reset(&Error::broker(
            error::INVALID_SHARE_SESSION_EPOCH,
            "x"
        )));
    }

    #[test]
    fn acknowledgement_batches_splits_on_gap() {
        let recs = [rec(0, 1), rec(0, 4)];
        let batches = acknowledgement_batches(&recs, ACK_REJECT);
        assert_eq!(batches[0].1.len(), 2);
        assert_eq!(batches[0].1[0].first_offset, 1);
        assert_eq!(batches[0].1[0].last_offset, 1);
        assert_eq!(batches[0].1[1].first_offset, 4);
        assert_eq!(batches[0].1[1].types, vec![ACK_REJECT]);
    }
}
