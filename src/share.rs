//! Share groups (KIP-932): queue-style consumption with per-record ack.

#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::sync::atomic::{AtomicI16, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::watch;

use crate::consumer::{Consumer, ConsumerConfig};
use crate::error::{Error, Result};
use crate::group::open_coord;
use crate::net::BrokerConn;
use crate::protocol::api_keys::{
    FIND_COORDINATOR, SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_HEARTBEAT,
};
use crate::protocol::group::{
    decode_find_coordinator_response, encode_find_coordinator_request_typed, COORDINATOR_SHARE,
};
use crate::protocol::share::{
    decode_share_acknowledge_response, decode_share_fetch_response,
    decode_share_group_heartbeat_response, encode_share_acknowledge_request,
    encode_share_fetch_request, encode_share_group_heartbeat_request, AcknowledgementBatch,
    ShareFetchPartition, ShareFetchTopic, ShareGroupHeartbeatRequest, ACK_ACCEPT, ACK_RELEASE,
};

pub use crate::protocol::share::{
    ACK_ACCEPT as SHARE_ACK_ACCEPT, ACK_RELEASE as SHARE_ACK_RELEASE,
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
        let mut consumer = Consumer::new(cfg.clone()).await?;
        let timeout = Duration::from_secs(30);
        let mut coord = open_coord(&cfg, consumer.conn_mut().addr()).await?;
        let body = coord
            .roundtrip(
                FIND_COORDINATOR,
                2,
                |buf| encode_find_coordinator_request_typed(buf, &group_id, COORDINATOR_SHARE),
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
        let body = self
            .coord
            .roundtrip(
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
        if self.share_session_epoch == 0 {
            self.share_session_epoch = 1;
        } else {
            self.share_session_epoch = self.share_session_epoch.saturating_add(1);
        }
        let fetched = decode_share_fetch_response(&mut body.clone())?;
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

    async fn acknowledge(&mut self, recs: &[ShareRecord], ack: i8) -> Result<()> {
        if recs.is_empty() {
            return Ok(());
        }
        let timeout = Duration::from_secs(30);
        for rec in recs {
            let body = self
                .coord
                .roundtrip(
                    SHARE_ACKNOWLEDGE,
                    1,
                    |buf| {
                        encode_share_acknowledge_request(
                            buf,
                            &self.group_id,
                            &self.member_id,
                            self.share_session_epoch,
                            self.topic_id,
                            rec.partition,
                            &[AcknowledgementBatch {
                                first_offset: rec.offset,
                                last_offset: rec.offset,
                                types: vec![ack],
                            }],
                        )
                    },
                    timeout,
                )
                .await?;
            let err = decode_share_acknowledge_response(&mut body.clone())?;
            if err != 0 {
                return Err(Error::broker(err, "ShareAcknowledge"));
            }
        }
        Ok(())
    }

    pub async fn leave(mut self) -> Result<()> {
        self.hb_stop.send(true).unwrap_or(());
        let timeout = Duration::from_secs(30);
        let req = ShareGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: -1,
            subscribed_topic_names: None,
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
            return Err(Error::broker(resp.error_code, "ShareGroupHeartbeat leave"));
        }
        Ok(())
    }

    fn spawn_heartbeat(&self, mut stop: watch::Receiver<bool>) {
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let hb_err = self.hb_err.clone();
        let hb_epoch = self.hb_epoch.clone();
        let addr = self.coord.addr().to_string();
        let cfg = self.cfg.clone();
        drop(tokio::spawn(async move {
            let Ok(mut conn) = open_coord(&cfg, &addr).await else {
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
                        let epoch = hb_epoch.load(Ordering::SeqCst);
                        let req = ShareGroupHeartbeatRequest {
                            group_id: group_id.clone(),
                            member_id: member_id.clone(),
                            member_epoch: epoch,
                            subscribed_topic_names: None,
                        };
                        let res = conn
                            .roundtrip(
                                SHARE_GROUP_HEARTBEAT,
                                1,
                                |buf| encode_share_group_heartbeat_request(buf, &req),
                                Duration::from_secs(10),
                            )
                            .await;
                        if let Ok(body) = res {
                            if let Ok(resp) =
                                decode_share_group_heartbeat_response(&mut body.clone())
                            {
                                hb_err.store(resp.error_code, Ordering::SeqCst);
                                if resp.member_epoch > 0 {
                                    hb_epoch.store(resp.member_epoch, Ordering::SeqCst);
                                }
                            }
                        }
                    }
                }
            }
        }));
    }
}
