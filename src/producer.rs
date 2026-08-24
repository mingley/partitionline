use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::partitioner::{partition_for_key, to_positive};
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, decode_produce_response,
    encode_api_versions_request, encode_metadata_request, encode_produce_request, ApiVersion,
    MetadataResponse, ProducePartitionData, ProduceTopicData,
};
use crate::protocol::api_keys::{pick_version, API_VERSIONS, METADATA, PRODUCE};
use crate::protocol::records::{Header as RecordHeader, Record, RecordBatch};

#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub bootstrap: Vec<String>,
    pub client_id: String,
    pub acks: i16,
    pub linger: Duration,
    pub batch_records: usize,
    pub batch_bytes: usize,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub allow_auto_topic_creation: bool,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap: vec!["127.0.0.1:9092".into()],
            client_id: "partitionline".into(),
            acks: 1,
            linger: Duration::from_millis(5),
            batch_records: 10_000,
            batch_bytes: 1_000_000,
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            allow_auto_topic_creation: false,
        }
    }
}

impl ProducerConfig {
    pub fn bootstrap<S: Into<String>>(servers: impl IntoIterator<Item = S>) -> Self {
        Self {
            bootstrap: servers.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProduceRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: Option<i64>,
    pub headers: Vec<RecordHeader>,
}

impl ProduceRecord {
    pub fn to(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: None,
            timestamp: None,
            headers: Vec::new(),
        }
    }

    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Clone)]
pub struct Producer {
    tx: mpsc::UnboundedSender<Cmd>,
}

enum Cmd {
    Send(ProduceRecord, oneshot::Sender<Result<RecordMetadata>>),
    Flush(oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<Result<()>>),
}

struct Pending {
    rec: ProduceRecord,
    tx: oneshot::Sender<Result<RecordMetadata>>,
}

struct Actor {
    cfg: ProducerConfig,
    rx: mpsc::UnboundedReceiver<Cmd>,
    conns: HashMap<String, BrokerConn>,
    versions: HashMap<i16, ApiVersion>,
    produce_version: i16,
    metadata_version: i16,
    metadata: Option<MetadataResponse>,
    rr: HashMap<String, i32>,
    batches: HashMap<(String, i32), Vec<Pending>>,
    batch_bytes: HashMap<(String, i32), usize>,
}

impl Producer {
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(ProducerConfig::bootstrap([bootstrap.into()])).await
    }

    pub async fn new(cfg: ProducerConfig) -> Result<Self> {
        if cfg.bootstrap.is_empty() {
            return Err(Error::protocol("no bootstrap servers"));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let mut actor = Actor {
            cfg,
            rx,
            conns: HashMap::new(),
            versions: HashMap::new(),
            produce_version: 3,
            metadata_version: 1,
            metadata: None,
            rr: HashMap::new(),
            batches: HashMap::new(),
            batch_bytes: HashMap::new(),
        };
        actor.handshake().await?;
        tokio::spawn(actor.run());
        Ok(Self { tx })
    }

    pub async fn send(&self, rec: ProduceRecord) -> Result<RecordMetadata> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Send(rec, tx))
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Cmd::Flush(tx)).map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    pub async fn close(self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Cmd::Close(tx)).map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }
}

impl Actor {
    async fn handshake(&mut self) -> Result<()> {
        let addr = self.cfg.bootstrap[0].clone();
        let mut conn =
            BrokerConn::connect(&addr, &self.cfg.client_id, self.cfg.connect_timeout).await?;
        let body = conn
            .roundtrip(
                API_VERSIONS,
                3,
                |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
                self.cfg.request_timeout,
            )
            .await?;
        let resp = decode_api_versions_response(&mut body.clone(), 3)?;
        if resp.error_code != 0 && resp.error_code != error::UNSUPPORTED_VERSION {
            return Err(Error::broker(resp.error_code, "ApiVersions"));
        }
        for api in &resp.api_keys {
            self.versions.insert(api.api_key, api.clone());
        }
        self.produce_version = self
            .pick(PRODUCE, 3, 9)
            .ok_or_else(|| Error::Unsupported("broker does not support Produce v3-9".into()))?;
        self.metadata_version = self
            .pick(METADATA, 1, 12)
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        self.conns.insert(addr, conn);
        Ok(())
    }

    fn pick(&self, api_key: i16, client_min: i16, client_max: i16) -> Option<i16> {
        self.versions
            .get(&api_key)
            .and_then(|v| pick_version(v.min_version, v.max_version, client_min, client_max))
    }

    async fn run(mut self) {
        loop {
            let cmd = if self.batches.is_empty() {
                self.rx.recv().await
            } else {
                tokio::select! {
                    cmd = self.rx.recv() => cmd,
                    _ = tokio::time::sleep(self.cfg.linger) => {
                        let _ = self.flush_all().await;
                        continue;
                    }
                }
            };
            let Some(cmd) = cmd else { break };
            match cmd {
                Cmd::Send(rec, tx) => {
                    self.enqueue(rec, tx);
                    if self.cfg.linger.is_zero() || self.batch_full() {
                        let _ = self.flush_all().await;
                    }
                }
                Cmd::Flush(tx) => {
                    let _ = tx.send(self.flush_all().await);
                }
                Cmd::Close(tx) => {
                    let r = self.flush_all().await;
                    let _ = tx.send(r);
                    break;
                }
            }
        }
    }

    fn batch_full(&self) -> bool {
        self.batches
            .values()
            .any(|v| v.len() >= self.cfg.batch_records)
            || self
                .batch_bytes
                .values()
                .any(|b| *b >= self.cfg.batch_bytes)
    }

    fn enqueue(&mut self, rec: ProduceRecord, tx: oneshot::Sender<Result<RecordMetadata>>) {
        let estimate = rec.key.as_ref().map(|b| b.len()).unwrap_or(0)
            + rec.value.as_ref().map(|b| b.len()).unwrap_or(0)
            + 64;
        let key = (rec.topic.clone(), rec.partition.unwrap_or(-1));
        self.batch_bytes
            .entry(key.clone())
            .and_modify(|b| *b += estimate)
            .or_insert(estimate);
        self.batches
            .entry(key)
            .or_default()
            .push(Pending { rec, tx });
    }

    async fn flush_all(&mut self) -> Result<()> {
        if self.batches.is_empty() {
            return Ok(());
        }
        let batches = std::mem::take(&mut self.batches);
        self.batch_bytes.clear();

        // Group by assigned partition.
        let mut assigned: HashMap<(String, i32), Vec<Pending>> = HashMap::new();
        for (key, pendings) in batches {
            for p in pendings {
                match self.assign_partition(&p.rec).await {
                    Ok(part) => assigned
                        .entry((p.rec.topic.clone(), part))
                        .or_default()
                        .push(p),
                    Err(e) => {
                        let _ = p.tx.send(Err(e));
                    }
                }
            }
            let _ = key;
        }

        // Group by leader.
        let mut by_leader: HashMap<i32, Vec<(String, i32, Vec<Pending>)>> = HashMap::new();
        for ((topic, part), pendings) in assigned {
            match self.leader_id(&topic, part) {
                Ok(leader) => by_leader
                    .entry(leader)
                    .or_default()
                    .push((topic, part, pendings)),
                Err(e) => {
                    for p in pendings {
                        let _ = p.tx.send(Err(clone_err(&e)));
                    }
                }
            }
        }

        for (leader, groups) in by_leader {
            self.produce_to_leader(leader, groups).await?;
        }
        Ok(())
    }

    async fn assign_partition(&mut self, rec: &ProduceRecord) -> Result<i32> {
        if let Some(p) = rec.partition {
            return Ok(p);
        }
        self.ensure_topic(&rec.topic).await?;
        let n = self.partition_count(&rec.topic)?;
        if n <= 0 {
            return Err(Error::UnknownTopic(rec.topic.clone()));
        }
        if let Some(key) = &rec.key {
            return Ok(partition_for_key(key, n));
        }
        let next = self.rr.entry(rec.topic.clone()).or_insert(0);
        let p = to_positive(*next) % n;
        *next += 1;
        Ok(p)
    }

    fn partition_count(&self, topic: &str) -> Result<i32> {
        let md = self
            .metadata
            .as_ref()
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
        let t = md
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic))
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
        if t.error_code != 0 {
            return Err(Error::broker(t.error_code, topic));
        }
        Ok(t.partitions.len() as i32)
    }

    fn leader_id(&self, topic: &str, partition: i32) -> Result<i32> {
        let md = self
            .metadata
            .as_ref()
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
        let t = md
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic))
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
        let p = t
            .partitions
            .iter()
            .find(|p| p.partition_index == partition)
            .ok_or_else(|| Error::NoLeader {
                topic: topic.to_string(),
                partition,
            })?;
        if p.error_code != 0 {
            return Err(Error::broker(p.error_code, format!("{topic}-{partition}")));
        }
        if p.leader_id < 0 {
            return Err(Error::NoLeader {
                topic: topic.to_string(),
                partition,
            });
        }
        Ok(p.leader_id)
    }

    fn broker_addr(&self, node_id: i32) -> Result<String> {
        let md = self
            .metadata
            .as_ref()
            .ok_or_else(|| Error::protocol("no metadata"))?;
        let b = md
            .brokers
            .iter()
            .find(|b| b.node_id == node_id)
            .ok_or_else(|| Error::protocol(format!("unknown broker {node_id}")))?;
        Ok(format!("{}:{}", b.host, b.port))
    }

    async fn ensure_topic(&mut self, topic: &str) -> Result<()> {
        let have = self
            .metadata
            .as_ref()
            .and_then(|m| m.topics.iter().find(|t| t.name.as_deref() == Some(topic)))
            .is_some();
        if have {
            return Ok(());
        }
        self.refresh_metadata(Some(&[topic.to_string()])).await
    }

    async fn refresh_metadata(&mut self, topics: Option<&[String]>) -> Result<()> {
        let addr = self.cfg.bootstrap[0].clone();
        let version = self.metadata_version;
        let allow = self.cfg.allow_auto_topic_creation;
        let request_timeout = self.cfg.request_timeout;
        let body = self
            .conn(&addr)
            .await?
            .roundtrip(
                METADATA,
                version,
                |buf| encode_metadata_request(buf, version, topics, allow),
                request_timeout,
            )
            .await?;
        let resp = decode_metadata_response(&mut body.clone(), version)?;
        self.metadata = Some(resp);
        Ok(())
    }

    async fn conn(&mut self, addr: &str) -> Result<&mut BrokerConn> {
        if !self.conns.contains_key(addr) {
            let conn =
                BrokerConn::connect(addr, &self.cfg.client_id, self.cfg.connect_timeout).await?;
            self.conns.insert(addr.to_string(), conn);
        }
        Ok(self.conns.get_mut(addr).unwrap())
    }

    async fn produce_to_leader(
        &mut self,
        leader: i32,
        groups: Vec<(String, i32, Vec<Pending>)>,
    ) -> Result<()> {
        let addr = match self.broker_addr(leader) {
            Ok(a) => a,
            Err(_) => self.cfg.bootstrap[0].clone(),
        };
        let now = now_ms();
        let mut wire: Vec<ProduceTopicData> = Vec::new();
        // Keep pending aligned with wire order.
        let mut pending_by_tp: Vec<((String, i32), Vec<Pending>)> = Vec::new();
        let mut by_topic: HashMap<String, Vec<(i32, Vec<Pending>)>> = HashMap::new();
        for (topic, part, pendings) in groups {
            by_topic.entry(topic).or_default().push((part, pendings));
        }
        for (topic, parts) in by_topic {
            let mut pdata = Vec::new();
            for (part, pendings) in parts {
                let records: Vec<Record> = pendings
                    .iter()
                    .map(|p| Record {
                        timestamp: p.rec.timestamp.unwrap_or(now),
                        key: p.rec.key.clone(),
                        value: p.rec.value.clone(),
                        headers: p.rec.headers.clone(),
                    })
                    .collect();
                pdata.push(ProducePartitionData {
                    index: part,
                    records: RecordBatch::from_records(records),
                });
                pending_by_tp.push(((topic.clone(), part), pendings));
            }
            wire.push(ProduceTopicData {
                topic,
                partitions: pdata,
            });
        }

        let version = self.produce_version;
        let acks = self.cfg.acks;
        let request_timeout = self.cfg.request_timeout;
        let timeout_ms = request_timeout.as_millis() as i32;
        if let Err(e) = self.conn(&addr).await {
            fail_all(pending_by_tp, e);
            return Ok(());
        }
        let conn = self.conns.get_mut(&addr).expect("conn inserted");
        if acks == 0 {
            // Kafka sends no Produce response when acks=0.
            if let Err(e) = conn
                .send(
                    PRODUCE,
                    version,
                    |buf| encode_produce_request(buf, version, None, acks, timeout_ms, &wire),
                    request_timeout,
                )
                .await
            {
                fail_all(pending_by_tp, e);
                return Ok(());
            }
            for ((topic, part), pendings) in pending_by_tp {
                for p in pendings {
                    let _ = p.tx.send(Ok(RecordMetadata {
                        topic: topic.clone(),
                        partition: part,
                        offset: -1,
                    }));
                }
            }
            return Ok(());
        }

        let body = match conn
            .roundtrip(
                PRODUCE,
                version,
                |buf| encode_produce_request(buf, version, None, acks, timeout_ms, &wire),
                request_timeout,
            )
            .await
        {
            Ok(b) => b,
            Err(e) => {
                fail_all(pending_by_tp, e);
                return Ok(());
            }
        };

        let responses = match decode_produce_response(&mut body.clone(), version) {
            Ok(r) => r,
            Err(e) => {
                fail_all(pending_by_tp, e);
                return Ok(());
            }
        };
        for ((topic, part), pendings) in pending_by_tp {
            let found = responses
                .iter()
                .find(|r| r.topic == topic && r.partition == part);
            match found {
                None => fail_pendings(pendings, Error::protocol("missing produce response")),
                Some(r) if r.error_code != 0 => {
                    fail_pendings(
                        pendings,
                        Error::broker(r.error_code, format!("{topic}-{part}")),
                    );
                }
                Some(r) => {
                    for (i, p) in pendings.into_iter().enumerate() {
                        let _ = p.tx.send(Ok(RecordMetadata {
                            topic: topic.clone(),
                            partition: part,
                            offset: r.base_offset + i as i64,
                        }));
                    }
                }
            }
        }
        Ok(())
    }
}

fn fail_all(groups: Vec<((String, i32), Vec<Pending>)>, err: Error) {
    for (_, pendings) in groups {
        fail_pendings(pendings, clone_err(&err));
    }
}

fn fail_pendings(pendings: Vec<Pending>, err: Error) {
    for p in pendings {
        let _ = p.tx.send(Err(clone_err(&err)));
    }
}

fn clone_err(err: &Error) -> Error {
    match err {
        Error::Io(e) => Error::Io(std::io::Error::new(e.kind(), e.to_string())),
        Error::Protocol(m) => Error::Protocol(m.clone()),
        Error::Broker { code, message } => Error::Broker {
            code: *code,
            message: message.clone(),
        },
        Error::UnknownTopic(t) => Error::UnknownTopic(t.clone()),
        Error::NoLeader { topic, partition } => Error::NoLeader {
            topic: topic.clone(),
            partition: *partition,
        },
        Error::Unsupported(m) => Error::Unsupported(m.clone()),
        Error::Closed => Error::Closed,
        Error::Timeout => Error::Timeout,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
