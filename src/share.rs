//! Share groups (KIP-932): queue-style consumption with per-record ack.

#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::watch;

use crate::consumer::ConsumerConfig;
use crate::error::{Error, Result};
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
    coord: BrokerConn,
    cfg: ConsumerConfig,
    group_id: String,
    member_id: String,
    member_epoch: i32,
    topic: String,
    topic_id: [u8; 16],
    partitions: Vec<i32>,
    share_session_epoch: i32,
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
        let coord = discover_coord(&cfg, &group_id, COORDINATOR_SHARE).await?;
        let member_id = new_member_id()?;
        let hb_err = Arc::new(AtomicI16::new(0));
        let hb_epoch = Arc::new(AtomicI32::new(0));
        let (hb_stop, hb_rx) = watch::channel(false);
        let mut g = Self {
            coord,
            cfg: cfg.clone(),
            group_id,
            member_id,
            member_epoch: 0,
            topic,
            topic_id: [0u8; 16],
            partitions: Vec::new(),
            share_session_epoch: 0,
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
        let timeout = Duration::from_secs(30);
        let max_wait = 10i32;
        let topics = vec![ShareFetchTopic {
            topic_id: self.topic_id,
            partitions: self
                .partitions
                .iter()
                .map(|p| ShareFetchPartition {
                    partition: *p,
                    acknowledgements: Vec::new(),
                })
                .collect(),
        }];
        let mut body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_SHARE,
            SHARE_FETCH,
            1,
            |buf| {
                encode_share_fetch_request(
                    buf,
                    &self.group_id,
                    &self.member_id,
                    self.share_session_epoch,
                    max_wait,
                    1,
                    1_048_576,
                    16,
                    &topics,
                )
            },
            timeout,
        )
        .await?;
        let fetched = decode_share_fetch_response(&mut body)?;
        self.advance_share_epoch();
        let mut out = Vec::new();
        for topic in fetched {
            for part in topic.partitions {
                if part.error_code != 0 {
                    return Err(Error::broker(part.error_code, "ShareFetch"));
                }
                for batch in part.records {
                    for rec in batch.records {
                        let delivery = part
                            .acquired
                            .iter()
                            .find(|a| rec.offset >= a.first_offset && rec.offset <= a.last_offset)
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
        Ok(out)
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

    fn advance_share_epoch(&mut self) {
        if self.share_session_epoch < 0 {
            return;
        }
        self.share_session_epoch = self.share_session_epoch.saturating_add(1);
    }

    async fn acknowledge(&mut self, recs: &[ShareRecord], ack: i8) -> Result<()> {
        if recs.is_empty() {
            return Ok(());
        }
        let partitions = acknowledgement_batches(recs, ack);
        if partitions.is_empty() {
            return Ok(());
        }
        if self.share_session_epoch <= 0 {
            return Err(Error::protocol(
                "ShareAcknowledge requires an open share session (poll first)",
            ));
        }
        let timeout = Duration::from_secs(30);
        let epoch = self.share_session_epoch;
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_SHARE,
            SHARE_ACKNOWLEDGE,
            1,
            |buf| {
                encode_share_acknowledge_request(
                    buf,
                    &self.group_id,
                    &self.member_id,
                    epoch,
                    self.topic_id,
                    &partitions,
                )
            },
            timeout,
        )
        .await?;
        let err = decode_share_acknowledge_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "ShareAcknowledge"));
        }
        self.advance_share_epoch();
        Ok(())
    }

    async fn close_share_session(&mut self) -> Result<()> {
        if self.share_session_epoch <= 0 {
            return Ok(());
        }
        let timeout = Duration::from_secs(30);
        let body = coord_roundtrip(
            &mut self.coord,
            &self.cfg,
            &self.group_id,
            COORDINATOR_SHARE,
            SHARE_ACKNOWLEDGE,
            1,
            |buf| {
                encode_share_acknowledge_request(
                    buf,
                    &self.group_id,
                    &self.member_id,
                    -1,
                    self.topic_id,
                    &[],
                )
            },
            timeout,
        )
        .await?;
        let err = decode_share_acknowledge_response(&mut body.clone())?;
        self.share_session_epoch = 0;
        if err != 0 && err != crate::error::SHARE_SESSION_NOT_FOUND {
            return Err(Error::broker(err, "ShareAcknowledge close"));
        }
        Ok(())
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
                                    if resp.error_code == crate::error::NOT_COORDINATOR {
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
