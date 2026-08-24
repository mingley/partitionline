use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::partitioner::{partition_for_key, to_positive};
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, decode_produce_response,
    encode_api_versions_request, encode_metadata_request, ApiVersion,
};
use crate::protocol::api_keys::{pick_version, API_VERSIONS, METADATA, PRODUCE};
use crate::protocol::header::encode_request_header_fields;
use crate::protocol::records::{
    write_record_batch, BatchHeader, Compression, EncodeRecord, Header as RecordHeader,
};

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
    pub compression: Compression,
    pub sasl_plain: Option<(String, String)>,
    pub connections: usize,
    pub max_in_flight: usize,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap: vec!["127.0.0.1:9092".into()],
            client_id: "partitionline".into(),
            acks: 1,
            linger: Duration::from_millis(5),
            batch_records: 32_768,
            batch_bytes: 1_000_000,
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            allow_auto_topic_creation: false,
            compression: Compression::None,
            sasl_plain: None,
            connections: 8,
            max_in_flight: 16,
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
    pub topic: Arc<str>,
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: Option<i64>,
    pub headers: Vec<RecordHeader>,
}

impl ProduceRecord {
    pub fn to(topic: impl Into<Arc<str>>) -> Self {
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

struct Pending {
    rec: ProduceRecord,
    tx: Option<oneshot::Sender<Result<RecordMetadata>>>,
}

enum Ctrl {
    Flush(oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<Result<()>>),
}

struct WorkerHandle {
    data: mpsc::Sender<Pending>,
    ctrl: mpsc::Sender<Ctrl>,
}

struct TopicCache {
    fast_name: OnceLock<Arc<str>>,
    fast_n: AtomicI32,
    rest: std::sync::Mutex<HashMap<Arc<str>, i32>>,
}

impl TopicCache {
    fn new() -> Self {
        Self {
            fast_name: OnceLock::new(),
            fast_n: AtomicI32::new(0),
            rest: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn lookup(&self, topic: &str) -> Option<i32> {
        if let Some(name) = self.fast_name.get() {
            if name.as_ref() == topic {
                let n = self.fast_n.load(Ordering::Acquire);
                if n > 0 {
                    return Some(n);
                }
            }
        }
        self.rest.lock().ok()?.get(topic).copied()
    }

    fn insert(&self, topic: Arc<str>, n: i32) {
        if self.fast_name.get().is_none() {
            let _ = self.fast_name.set(topic.clone());
            self.fast_n.store(n, Ordering::Release);
        }
        if let Ok(mut g) = self.rest.lock() {
            g.insert(topic, n);
        }
    }
}

struct Shared {
    cfg: ProducerConfig,
    cache: TopicCache,
    meta: Mutex<BrokerConn>,
    metadata_version: i16,
    produce_version: i16,
    rr: AtomicI32,
}

#[derive(Clone)]
pub struct Producer {
    inner: Arc<Inner>,
}

struct Inner {
    workers: Vec<WorkerHandle>,
    shared: Arc<Shared>,
}

impl Producer {
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(ProducerConfig::bootstrap([bootstrap.into()])).await
    }

    pub async fn new(cfg: ProducerConfig) -> Result<Self> {
        if cfg.bootstrap.is_empty() {
            return Err(Error::protocol("no bootstrap servers"));
        }
        let addr = cfg.bootstrap[0].clone();
        let mut meta = BrokerConn::connect(&addr, &cfg.client_id, cfg.connect_timeout).await?;
        let body = meta
            .roundtrip(
                API_VERSIONS,
                3,
                |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
                cfg.request_timeout,
            )
            .await?;
        let resp = decode_api_versions_response(&mut body.clone(), 3)?;
        if resp.error_code != 0 && resp.error_code != error::UNSUPPORTED_VERSION {
            return Err(Error::broker(resp.error_code, "ApiVersions"));
        }
        let mut versions = HashMap::new();
        for api in &resp.api_keys {
            versions.insert(api.api_key, api.clone());
        }
        if let Some((u, p)) = cfg.sasl_plain.clone() {
            crate::protocol::sasl::authenticate_plain(&mut meta, &u, &p, cfg.request_timeout)
                .await?;
        }
        let produce_version = pick(&versions, PRODUCE, 3, 8)
            .ok_or_else(|| Error::Unsupported("broker does not support Produce v3-8".into()))?;
        let metadata_version = pick(&versions, METADATA, 1, 12)
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;

        let n_conn = cfg.connections.max(1);
        let cap = (100_000 / n_conn).max(4_096);
        let shared = Arc::new(Shared {
            cfg: cfg.clone(),
            cache: TopicCache::new(),
            meta: Mutex::new(meta),
            metadata_version,
            produce_version,
            rr: AtomicI32::new(0),
        });

        let mut workers = Vec::with_capacity(n_conn);
        for _ in 0..n_conn {
            let conn = open_conn(&addr, &cfg).await?;
            let (data_tx, data_rx) = mpsc::channel(cap);
            let (ctrl_tx, ctrl_rx) = mpsc::channel(16);
            let worker = Worker {
                conn,
                data: data_rx,
                ctrl: ctrl_rx,
                shared: shared.clone(),
                write_buf: BytesMut::with_capacity(2 * 1024 * 1024),
                pending: Vec::with_capacity(cfg.batch_records.min(8192)),
                in_flight: VecDeque::new(),
                part_rr: 0,
            };
            tokio::spawn(worker.run());
            workers.push(WorkerHandle {
                data: data_tx,
                ctrl: ctrl_tx,
            });
        }

        Ok(Self {
            inner: Arc::new(Inner { workers, shared }),
        })
    }

    fn assign_cached(&self, rec: &mut ProduceRecord) -> usize {
        let n_workers = self.inner.workers.len();
        if rec.partition.is_none() {
            if let Some(np) = self.inner.shared.cache.lookup(rec.topic.as_ref()) {
                rec.partition = Some(if let Some(k) = &rec.key {
                    partition_for_key(k, np)
                } else {
                    to_positive(self.inner.shared.rr.fetch_add(1, Ordering::Relaxed)) % np
                });
            }
        }
        match rec.partition {
            Some(p) => (p as usize) % n_workers,
            None => {
                if rec.key.is_some() {
                    0
                } else {
                    to_positive(self.inner.shared.rr.fetch_add(1, Ordering::Relaxed)) as usize
                        % n_workers
                }
            }
        }
    }

    pub async fn send(&self, rec: ProduceRecord) -> Result<RecordMetadata> {
        let (tx, rx) = oneshot::channel();
        let mut rec = rec;
        let i = self.assign_cached(&mut rec);
        self.inner.workers[i]
            .data
            .send(Pending { rec, tx: Some(tx) })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Enqueue without a per-record future. Delivery is observed on `flush`.
    pub fn try_send(&self, rec: ProduceRecord) -> Result<()> {
        let mut rec = rec;
        let i = self.assign_cached(&mut rec);
        self.inner.workers[i]
            .data
            .try_send(Pending { rec, tx: None })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    pub async fn flush(&self) -> Result<()> {
        let mut rxs = Vec::with_capacity(self.inner.workers.len());
        for w in &self.inner.workers {
            let (tx, rx) = oneshot::channel();
            w.ctrl
                .send(Ctrl::Flush(tx))
                .await
                .map_err(|_| Error::Closed)?;
            rxs.push(rx);
        }
        for rx in rxs {
            rx.await.map_err(|_| Error::Closed)??;
        }
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        let mut rxs = Vec::with_capacity(self.inner.workers.len());
        for w in &self.inner.workers {
            let (tx, rx) = oneshot::channel();
            let _ = w.ctrl.send(Ctrl::Close(tx)).await;
            rxs.push(rx);
        }
        for rx in rxs {
            let _ = rx.await;
        }
        Ok(())
    }
}

fn pick(
    versions: &HashMap<i16, ApiVersion>,
    api_key: i16,
    client_min: i16,
    client_max: i16,
) -> Option<i16> {
    versions
        .get(&api_key)
        .and_then(|v| pick_version(v.min_version, v.max_version, client_min, client_max))
}

async fn open_conn(addr: &str, cfg: &ProducerConfig) -> Result<BrokerConn> {
    let mut conn = BrokerConn::connect(addr, &cfg.client_id, cfg.connect_timeout).await?;
    let _ = conn
        .roundtrip(
            API_VERSIONS,
            3,
            |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
            cfg.request_timeout,
        )
        .await?;
    if let Some((u, p)) = &cfg.sasl_plain {
        crate::protocol::sasl::authenticate_plain(&mut conn, u, p, cfg.request_timeout).await?;
    }
    Ok(conn)
}

async fn partitions_for(shared: &Shared, topic: &Arc<str>) -> Result<i32> {
    if let Some(n) = shared.cache.lookup(topic) {
        return Ok(n);
    }
    let mut conn = shared.meta.lock().await;
    let version = shared.metadata_version;
    let allow = shared.cfg.allow_auto_topic_creation;
    let timeout = shared.cfg.request_timeout;
    let topics = [topic.to_string()];
    let body = conn
        .roundtrip(
            METADATA,
            version,
            |buf| encode_metadata_request(buf, version, Some(&topics), allow),
            timeout,
        )
        .await?;
    drop(conn);
    let resp = decode_metadata_response(&mut body.clone(), version)?;
    let t = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic.as_ref()))
        .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
    if t.error_code != 0 {
        return Err(Error::broker(t.error_code, topic.to_string()));
    }
    let n = t.partitions.len() as i32;
    if n <= 0 {
        return Err(Error::UnknownTopic(topic.to_string()));
    }
    shared.cache.insert(topic.clone(), n);
    Ok(n)
}

struct Worker {
    conn: BrokerConn,
    data: mpsc::Receiver<Pending>,
    ctrl: mpsc::Receiver<Ctrl>,
    shared: Arc<Shared>,
    write_buf: BytesMut,
    pending: Vec<Pending>,
    in_flight: VecDeque<InFlight>,
    part_rr: i32,
}

struct InFlight {
    correlation: i32,
    groups: Vec<(Arc<str>, i32, Vec<Pending>)>,
}

impl Worker {
    fn pull_ready(&mut self) {
        while let Ok(p) = self.data.try_recv() {
            self.pending.push(p);
        }
    }

    fn linger_expired(&self, start: Option<Instant>) -> bool {
        let linger = self.shared.cfg.linger;
        if self.pending.is_empty() {
            return false;
        }
        if linger.is_zero() {
            return true;
        }
        start.map(|s| s.elapsed() >= linger).unwrap_or(false)
    }

    fn can_fire(&self, linger_start: Option<Instant>) -> bool {
        if self.pending.is_empty() {
            return false;
        }
        if self.in_flight.len() >= self.shared.cfg.max_in_flight {
            return false;
        }
        batch_ready(
            &self.pending,
            self.shared.cfg.batch_records,
            self.shared.cfg.batch_bytes,
        ) || self.linger_expired(linger_start)
    }

    async fn run(mut self) {
        let mut linger_start: Option<Instant> = None;
        loop {
            self.pull_ready();
            if linger_start.is_none() && !self.pending.is_empty() {
                linger_start = Some(Instant::now());
            }
            if self.can_fire(linger_start) {
                if let Err(e) = self.fire().await {
                    fail_pendings(std::mem::take(&mut self.pending), e);
                }
                linger_start = if self.pending.is_empty() {
                    None
                } else {
                    Some(Instant::now())
                };
                continue;
            }

            if self.in_flight.len() >= self.shared.cfg.max_in_flight
                || (self.pending.is_empty() && !self.in_flight.is_empty())
            {
                if let Err(e) = self.wait_one().await {
                    fail_inflight(&mut self.in_flight, e);
                }
                continue;
            }

            let rec_limit = self.shared.cfg.batch_records;
            let room = rec_limit.saturating_sub(self.pending.len()).max(1);
            let linger = self.shared.cfg.linger;
            let wait_linger =
                linger_start.filter(|_| !linger.is_zero() && !self.pending.is_empty());

            tokio::select! {
                biased;
                n = self.data.recv_many(&mut self.pending, room) => {
                    if n == 0 {
                        self.pull_ready();
                        let _ = self.drain_inflight().await;
                        break;
                    }
                    if linger_start.is_none() {
                        linger_start = Some(Instant::now());
                    }
                }
                c = self.ctrl.recv() => {
                    match c {
                        None => {
                            self.pull_ready();
                            let _ = self.drain_inflight().await;
                            break;
                        }
                        Some(c) => {
                            let close = matches!(&c, Ctrl::Close(_));
                            self.pull_ready();
                            self.drain_inflight().await;
                            match c {
                                Ctrl::Flush(tx) | Ctrl::Close(tx) => {
                                    let _ = tx.send(Ok(()));
                                }
                            }
                            linger_start = None;
                            if close {
                                break;
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(
                    wait_linger
                        .map(|s| linger.saturating_sub(s.elapsed()))
                        .unwrap_or(Duration::from_secs(86400)),
                ), if wait_linger.is_some() => {}
            }
        }
    }

    async fn fire(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let n = take_count(
            &self.pending,
            self.shared.cfg.batch_records,
            self.shared.cfg.batch_bytes,
        );
        let mut batch: Vec<Pending> = self.pending.drain(..n).collect();
        let now = now_ms();
        let mut topic_np: HashMap<Arc<str>, i32> = HashMap::new();
        for p in &mut batch {
            if p.rec.partition.is_none() {
                let np = if let Some(n) = topic_np.get(&p.rec.topic) {
                    *n
                } else if let Some(n) = self.shared.cache.lookup(p.rec.topic.as_ref()) {
                    topic_np.insert(p.rec.topic.clone(), n);
                    n
                } else {
                    let n = partitions_for(&self.shared, &p.rec.topic).await?;
                    topic_np.insert(p.rec.topic.clone(), n);
                    n
                };
                p.rec.partition = Some(if let Some(k) = &p.rec.key {
                    partition_for_key(k, np)
                } else {
                    let v = self.part_rr;
                    self.part_rr = self.part_rr.wrapping_add(1);
                    to_positive(v) % np
                });
            }
        }
        let groups = group_pending(batch);
        if groups.is_empty() {
            return Ok(());
        }

        let version = self.shared.produce_version;
        let acks = self.shared.cfg.acks;
        let timeout_ms = self.shared.cfg.request_timeout.as_millis() as i32;
        let compression = self.shared.cfg.compression;
        self.write_buf.clear();
        self.write_buf.put_i32(0);
        let correlation = self.conn.next_correlation();
        encode_request_header_fields(
            &mut self.write_buf,
            PRODUCE,
            version,
            correlation,
            Some(self.conn.client_id()),
        );
        encode_produce_body(
            &mut self.write_buf,
            version,
            acks,
            timeout_ms,
            &groups,
            compression,
            now,
        )?;
        let size = (self.write_buf.len() - 4) as i32;
        self.write_buf[0..4].copy_from_slice(&size.to_be_bytes());
        self.conn
            .write_all_timeout(&self.write_buf, self.shared.cfg.request_timeout)
            .await?;

        if acks == 0 {
            complete_acks0(groups);
            return Ok(());
        }
        self.in_flight.push_back(InFlight {
            correlation,
            groups,
        });
        Ok(())
    }

    async fn wait_one(&mut self) -> Result<()> {
        let Some(inf) = self.in_flight.pop_front() else {
            return Ok(());
        };
        let version = self.shared.produce_version;
        let body = match self
            .conn
            .read_response(
                PRODUCE,
                version,
                inf.correlation,
                self.shared.cfg.request_timeout,
            )
            .await
        {
            Ok(b) => b,
            Err(e) => {
                fail_groups(inf.groups, e);
                return Ok(());
            }
        };
        let responses = match decode_produce_response(&mut body.clone(), version) {
            Ok(r) => r,
            Err(e) => {
                fail_groups(inf.groups, e);
                return Ok(());
            }
        };
        for (topic, part, pendings) in inf.groups {
            let found = responses
                .iter()
                .find(|r| r.topic.as_str() == topic.as_ref() && r.partition == part);
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
                        if let Some(tx) = p.tx {
                            let _ = tx.send(Ok(RecordMetadata {
                                topic: topic.to_string(),
                                partition: part,
                                offset: r.base_offset + i as i64,
                            }));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn drain_inflight(&mut self) {
        while !self.pending.is_empty() || !self.in_flight.is_empty() {
            if !self.pending.is_empty() {
                while self.in_flight.len() >= self.shared.cfg.max_in_flight {
                    if let Err(e) = self.wait_one().await {
                        fail_inflight(&mut self.in_flight, e);
                    }
                }
                if let Err(e) = self.fire().await {
                    fail_pendings(std::mem::take(&mut self.pending), e);
                }
            } else if let Err(e) = self.wait_one().await {
                fail_inflight(&mut self.in_flight, e);
            }
        }
    }
}

fn group_pending(batch: Vec<Pending>) -> Vec<(Arc<str>, i32, Vec<Pending>)> {
    if batch.is_empty() {
        return Vec::new();
    }
    let topic0 = batch[0].rec.topic.clone();
    let part0 = batch[0].rec.partition.unwrap_or(-1);
    let homogeneous = batch
        .iter()
        .all(|p| p.rec.partition == Some(part0) && p.rec.topic.as_ref() == topic0.as_ref());
    if homogeneous {
        return vec![(topic0, part0, batch)];
    }
    let mut assigned: HashMap<(Arc<str>, i32), Vec<Pending>> = HashMap::new();
    for p in batch {
        assigned
            .entry((p.rec.topic.clone(), p.rec.partition.unwrap_or(-1)))
            .or_default()
            .push(p);
    }
    assigned.into_iter().map(|((t, p), v)| (t, p, v)).collect()
}

fn batch_ready(pending: &[Pending], rec_limit: usize, byte_limit: usize) -> bool {
    if pending.len() >= rec_limit {
        return true;
    }
    let mut bytes = 0usize;
    for p in pending {
        bytes += estimate(p);
        if bytes >= byte_limit {
            return true;
        }
    }
    false
}

fn take_count(pending: &[Pending], rec_limit: usize, byte_limit: usize) -> usize {
    let mut n = 0;
    let mut bytes = 0usize;
    for p in pending.iter().take(rec_limit) {
        bytes += estimate(p);
        n += 1;
        if bytes >= byte_limit {
            break;
        }
    }
    n.max(1).min(pending.len())
}

fn estimate(p: &Pending) -> usize {
    p.rec.key.as_ref().map(|b| b.len()).unwrap_or(0)
        + p.rec.value.as_ref().map(|b| b.len()).unwrap_or(0)
        + 64
}

fn encode_produce_body(
    buf: &mut BytesMut,
    version: i16,
    acks: i16,
    timeout_ms: i32,
    groups: &[(Arc<str>, i32, Vec<Pending>)],
    compression: Compression,
    now: i64,
) -> Result<()> {
    let flexible = version >= 9;
    if version >= 3 {
        crate::protocol::buf::put_string(buf, flexible, None);
    }
    buf.put_i16(acks);
    buf.put_i32(timeout_ms);
    let mut topics: Vec<&Arc<str>> = Vec::new();
    for (t, _, _) in groups {
        if !topics.iter().any(|x| x.as_ref() == t.as_ref()) {
            topics.push(t);
        }
    }
    crate::protocol::buf::put_array_len(buf, flexible, Some(topics.len()));
    for topic in topics {
        crate::protocol::buf::put_string(buf, flexible, Some(topic.as_ref()));
        let idxs: Vec<usize> = groups
            .iter()
            .enumerate()
            .filter(|(_, (t, _, _))| t.as_ref() == topic.as_ref())
            .map(|(i, _)| i)
            .collect();
        crate::protocol::buf::put_array_len(buf, flexible, Some(idxs.len()));
        for i in idxs {
            let (_, partition, pendings) = &groups[i];
            buf.put_i32(*partition);
            if flexible {
                let mut recs = BytesMut::new();
                encode_pendings(&mut recs, pendings, compression, now)?;
                crate::protocol::buf::put_bytes(buf, true, Some(&recs));
                crate::protocol::buf::put_empty_tagged_fields(buf);
            } else {
                let len_pos = buf.len();
                buf.put_i32(0);
                encode_pendings(buf, pendings, compression, now)?;
                let rec_len = (buf.len() - len_pos - 4) as i32;
                buf[len_pos..len_pos + 4].copy_from_slice(&rec_len.to_be_bytes());
            }
        }
        if flexible {
            crate::protocol::buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        crate::protocol::buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

fn encode_pendings(
    buf: &mut BytesMut,
    pendings: &[Pending],
    compression: Compression,
    now: i64,
) -> Result<()> {
    let base_ts = pendings
        .first()
        .and_then(|p| p.rec.timestamp)
        .unwrap_or(now);
    let max_ts = pendings
        .iter()
        .map(|p| p.rec.timestamp.unwrap_or(now))
        .max()
        .unwrap_or(base_ts);
    write_record_batch(
        buf,
        &BatchHeader {
            attributes: compression as i16,
            base_timestamp: base_ts,
            max_timestamp: max_ts,
            count: pendings.len() as i32,
            ..BatchHeader::default()
        },
        pendings.iter().map(|p| EncodeRecord {
            timestamp: p.rec.timestamp.unwrap_or(now),
            key: p.rec.key.as_deref(),
            value: p.rec.value.as_deref(),
            headers: &p.rec.headers,
        }),
    )
}

fn complete_acks0(groups: Vec<(Arc<str>, i32, Vec<Pending>)>) {
    for (topic, part, pendings) in groups {
        for p in pendings {
            if let Some(tx) = p.tx {
                let _ = tx.send(Ok(RecordMetadata {
                    topic: topic.to_string(),
                    partition: part,
                    offset: -1,
                }));
            }
        }
    }
}

fn fail_inflight(in_flight: &mut VecDeque<InFlight>, err: Error) {
    while let Some(inf) = in_flight.pop_front() {
        fail_groups(inf.groups, clone_err(&err));
    }
}

fn fail_groups(groups: Vec<(Arc<str>, i32, Vec<Pending>)>, err: Error) {
    for (_, _, pendings) in groups {
        fail_pendings(pendings, clone_err(&err));
    }
}

fn fail_pendings(pendings: Vec<Pending>, err: Error) {
    for p in pendings {
        if let Some(tx) = p.tx {
            let _ = tx.send(Err(clone_err(&err)));
        }
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
        Error::UnknownTopic(t) => Error::UnknownTopic(t.to_string()),
        Error::NoLeader { topic, partition } => Error::NoLeader {
            topic: topic.clone(),
            partition: *partition,
        },
        Error::Unsupported(m) => Error::Unsupported(m.clone()),
        Error::Closed => Error::Closed,
        Error::Timeout => Error::Timeout,
        Error::QueueFull => Error::QueueFull,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
