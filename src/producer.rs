#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, TlsConfig};
use crate::partitioner::{partition_for_key, to_positive};
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, decode_produce_response,
    encode_api_versions_request, encode_metadata_request, ApiVersion,
};
use crate::protocol::api_keys::{
    pick_version, ADD_OFFSETS_TO_TXN, ADD_PARTITIONS_TO_TXN, API_VERSIONS, END_TXN,
    INIT_PRODUCER_ID, METADATA, PRODUCE, TXN_OFFSET_COMMIT,
};
use crate::protocol::header::encode_request_header_fields;
use crate::protocol::idem::{decode_init_producer_id_response, encode_init_producer_id_request};
use crate::protocol::records::{
    write_record_batch, BatchHeader, Compression, EncodeRecord, Header as RecordHeader,
};
use crate::protocol::txn::{
    decode_add_offsets_to_txn_response, decode_add_partitions_to_txn_response,
    decode_end_txn_response, decode_txn_offset_commit_response, encode_add_offsets_to_txn_request,
    encode_add_partitions_to_txn_request, encode_end_txn_request, encode_txn_offset_commit_request,
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
    pub sasl_scram: Option<(String, String)>,
    pub sasl_scram_sha512: Option<(String, String)>,
    pub sasl_oauthbearer: Option<String>,
    pub sasl_oauthbearer_oidc: Option<crate::OidcConfig>,
    pub connections: usize,
    pub max_in_flight: usize,
    pub enable_idempotence: bool,
    pub transactional_id: Option<String>,
    pub tls: Option<TlsConfig>,
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
            sasl_scram: None,
            sasl_scram_sha512: None,
            sasl_oauthbearer: None,
            sasl_oauthbearer_oidc: None,
            connections: 8,
            max_in_flight: 16,
            enable_idempotence: false,
            transactional_id: None,
            tls: None,
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
    seq: Option<i32>,
    deadline: Instant,
}

enum Ctrl {
    Flush(oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<Result<()>>),
}

#[derive(Clone)]
struct WorkerHandle {
    data: mpsc::Sender<Pending>,
    ctrl: mpsc::Sender<Ctrl>,
}

struct Shared {
    cfg: ProducerConfig,
    cluster: parking_lot::Mutex<Cluster>,
    meta: Mutex<BrokerConn>,
    metadata_version: i16,
    produce_version: i16,
    rr: AtomicI32,
    producer_id: i64,
    producer_epoch: i16,
    seqs: parking_lot::Mutex<HashMap<(Arc<str>, i32), i32>>,
    cache_nudge: Notify,
    meta_tx: mpsc::Sender<Arc<str>>,
    connect_tx: mpsc::Sender<i32>,
    retry_tx: mpsc::Sender<Pending>,
    last_meta_err: parking_lot::Mutex<Option<Error>>,
    nodes: parking_lot::Mutex<HashMap<i32, Vec<WorkerHandle>>>,
    retries_out: AtomicUsize,
    in_txn: AtomicBool,
    txn_partitions: parking_lot::Mutex<HashSet<(Arc<str>, i32)>>,
}

#[derive(Clone)]
pub struct Producer {
    inner: Arc<Inner>,
}

struct Inner {
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
        let mut meta = BrokerConn::connect_tls_any(
            &cfg.bootstrap,
            &cfg.client_id,
            cfg.connect_timeout,
            cfg.tls.as_ref(),
        )
        .await?;
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
            let _prev = versions.insert(api.api_key, api.clone());
        }
        crate::protocol::sasl::authenticate(
            &mut meta,
            cfg.sasl_plain.as_ref(),
            cfg.sasl_scram.as_ref(),
            cfg.sasl_scram_sha512.as_ref(),
            cfg.sasl_oauthbearer.as_deref(),
            cfg.sasl_oauthbearer_oidc.as_ref(),
            cfg.request_timeout,
        )
        .await?;
        let mut cfg = cfg;
        if cfg.transactional_id.is_some() {
            cfg.enable_idempotence = true;
        }
        if cfg.enable_idempotence {
            cfg.acks = -1;
            cfg.max_in_flight = cfg.max_in_flight.min(5);
        }
        let produce_version = pick(&versions, PRODUCE, 3, 8)
            .ok_or_else(|| Error::Unsupported("broker does not support Produce v3-8".into()))?;
        let metadata_version = pick(&versions, METADATA, 1, 12)
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;

        let mut producer_id = -1i64;
        let mut producer_epoch = -1i16;
        if cfg.enable_idempotence {
            let ipid_version = pick(&versions, INIT_PRODUCER_ID, 0, 1).ok_or_else(|| {
                Error::Unsupported("broker does not support InitProducerId".into())
            })?;
            let txn_id = cfg.transactional_id.clone();
            let body = meta
                .roundtrip(
                    INIT_PRODUCER_ID,
                    ipid_version,
                    |buf| encode_init_producer_id_request(buf, ipid_version, txn_id.as_deref()),
                    cfg.request_timeout,
                )
                .await?;
            let (err, pid, epoch) =
                decode_init_producer_id_response(&mut body.clone(), ipid_version)?;
            if err != 0 {
                return Err(Error::broker(err, "InitProducerId"));
            }
            if pid < 0 {
                return Err(Error::protocol("InitProducerId returned producer_id=-1"));
            }
            producer_id = pid;
            producer_epoch = epoch;
        }

        let n_conn = cfg.connections.max(1);
        let cap = (100_000 / n_conn).max(4_096);
        let (meta_tx, meta_rx) = mpsc::channel(8);
        let (connect_tx, connect_rx) = mpsc::channel(16);
        let (retry_tx, retry_rx) = mpsc::channel(cap.max(1024));
        let shared = Arc::new(Shared {
            cfg: cfg.clone(),
            cluster: parking_lot::Mutex::new(Cluster::default()),
            meta: Mutex::new(meta),
            metadata_version,
            produce_version,
            rr: AtomicI32::new(0),
            producer_id,
            producer_epoch,
            seqs: parking_lot::Mutex::new(HashMap::new()),
            cache_nudge: Notify::new(),
            meta_tx,
            connect_tx,
            retry_tx,
            last_meta_err: parking_lot::Mutex::new(None),
            nodes: parking_lot::Mutex::new(HashMap::new()),
            retries_out: AtomicUsize::new(0),
            in_txn: AtomicBool::new(false),
            txn_partitions: parking_lot::Mutex::new(HashSet::new()),
        });
        let weak = Arc::downgrade(&shared);
        drop(tokio::spawn(async move {
            let mut meta_rx = meta_rx;
            while let Some(topic) = meta_rx.recv().await {
                let Some(shared) = weak.upgrade() else {
                    break;
                };
                if shared
                    .cluster
                    .lock()
                    .partition_count(topic.as_ref())
                    .is_some()
                {
                    shared.cache_nudge.notify_waiters();
                    continue;
                }
                match partitions_for(&shared, &topic).await {
                    Ok(_) => {
                        *shared.last_meta_err.lock() = None;
                    }
                    Err(e) => {
                        *shared.last_meta_err.lock() = Some(clone_err(&e));
                    }
                }
                shared.cache_nudge.notify_waiters();
            }
        }));
        let weak = Arc::downgrade(&shared);
        drop(tokio::spawn(async move {
            connect_loop(weak, connect_rx, cap).await;
        }));
        let weak = Arc::downgrade(&shared);
        drop(tokio::spawn(async move {
            retry_loop(weak, retry_rx).await;
        }));

        Ok(Self {
            inner: Arc::new(Inner { shared }),
        })
    }

    fn apply_cached_partition(&self, rec: &mut ProduceRecord) -> bool {
        if rec.partition.is_some() {
            return true;
        }
        let Some(np) = self
            .inner
            .shared
            .cluster
            .lock()
            .partition_count(rec.topic.as_ref())
        else {
            return false;
        };
        rec.partition = Some(pick_part(rec, np, &self.inner.shared.rr));
        true
    }

    fn worker_for(&self, rec: &ProduceRecord) -> Option<WorkerHandle> {
        let p = rec.partition?;
        let cluster = self.inner.shared.cluster.lock();
        let (node, _) = cluster.leader(rec.topic.as_ref(), p).ok()?;
        drop(cluster);
        try_nudge_node(&self.inner.shared.connect_tx, node);
        let nodes = self.inner.shared.nodes.lock();
        let workers = nodes.get(&node)?;
        if workers.is_empty() {
            return None;
        }
        let i = usize::try_from(p).unwrap_or(0) % workers.len();
        workers.get(i).cloned()
    }

    fn nudge_topic(&self, rec: &ProduceRecord) {
        drop(self.inner.shared.meta_tx.try_send(rec.topic.clone()));
        if let Some(p) = rec.partition {
            if let Ok((node, _)) = self
                .inner
                .shared
                .cluster
                .lock()
                .leader(rec.topic.as_ref(), p)
            {
                try_nudge_node(&self.inner.shared.connect_tx, node);
            }
        }
    }

    async fn ensure_ready(&self, rec: &mut ProduceRecord) -> Result<()> {
        let deadline = Instant::now() + self.inner.shared.cfg.request_timeout;
        loop {
            if let Some(e) = peek_meta_err(&self.inner.shared) {
                return Err(e);
            }
            let _ = self.apply_cached_partition(rec);
            if self.worker_for(rec).is_some() {
                return Ok(());
            }
            self.nudge_topic(rec);
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let rest = deadline.saturating_duration_since(Instant::now());
            let notified = self.inner.shared.cache_nudge.notified();
            tokio::pin!(notified);
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(rest) => return Err(Error::Timeout),
            }
        }
    }

    pub async fn send(&self, rec: ProduceRecord) -> Result<RecordMetadata> {
        let (tx, rx) = oneshot::channel();
        let mut rec = rec;
        self.ensure_ready(&mut rec).await?;
        let w = self.worker_for(&rec).ok_or(Error::Closed)?;
        let deadline = Instant::now() + self.inner.shared.cfg.request_timeout;
        w.data
            .send(Pending {
                rec,
                tx: Some(tx),
                seq: None,
                deadline,
            })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Enqueue without a per-record future. Delivery is observed on `flush`.
    ///
    /// Returns `QueueFull` until metadata and a connection to the partition
    /// leader are ready. Call again; `send` waits instead. Records are never
    /// queued without a partition, so each partition is pinned to one TCP
    /// connection on its current leader.
    pub fn try_send(&self, rec: ProduceRecord) -> Result<()> {
        let mut rec = rec;
        let _ = self.apply_cached_partition(&mut rec);
        let Some(w) = self.worker_for(&rec) else {
            self.nudge_topic(&rec);
            return Err(Error::QueueFull);
        };
        let deadline = Instant::now() + self.inner.shared.cfg.request_timeout;
        w.data
            .try_send(Pending {
                rec,
                tx: None,
                seq: None,
                deadline,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    async fn flush_workers(&self) -> Result<()> {
        let workers: Vec<WorkerHandle> = self
            .inner
            .shared
            .nodes
            .lock()
            .values()
            .flatten()
            .cloned()
            .collect();
        if workers.is_empty() {
            return Ok(());
        }
        let mut rxs = Vec::with_capacity(workers.len());
        for w in &workers {
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

    pub async fn begin_transaction(&self) -> Result<()> {
        if self.inner.shared.cfg.transactional_id.is_none() {
            return Err(Error::protocol("transactional.id is not set"));
        }
        self.inner.shared.in_txn.store(true, Ordering::SeqCst);
        self.inner.shared.txn_partitions.lock().clear();
        Ok(())
    }

    pub async fn commit_transaction(&self) -> Result<()> {
        self.flush().await?;
        self.end_txn(true).await
    }

    pub async fn abort_transaction(&self) -> Result<()> {
        self.flush().await?;
        self.end_txn(false).await
    }

    pub async fn send_offsets_to_transaction(
        &self,
        group_id: &str,
        offsets: &[(String, i32, i64)],
    ) -> Result<()> {
        let Some(tid) = self.inner.shared.cfg.transactional_id.clone() else {
            return Err(Error::protocol("transactional.id is not set"));
        };
        if !self.inner.shared.in_txn.load(Ordering::SeqCst) {
            return Err(Error::protocol("no transaction in progress"));
        }
        let timeout = self.inner.shared.cfg.request_timeout;
        let pid = self.inner.shared.producer_id;
        let epoch = self.inner.shared.producer_epoch;
        {
            let mut meta = self.inner.shared.meta.lock().await;
            let body = meta
                .roundtrip(
                    ADD_OFFSETS_TO_TXN,
                    0,
                    |buf| encode_add_offsets_to_txn_request(buf, &tid, pid, epoch, group_id),
                    timeout,
                )
                .await?;
            let err = decode_add_offsets_to_txn_response(&mut body.clone())?;
            if err != 0 {
                return Err(Error::broker(err, "AddOffsetsToTxn"));
            }
        }
        let mut meta = self.inner.shared.meta.lock().await;
        for (topic, part, off) in offsets {
            let body = meta
                .roundtrip(
                    TXN_OFFSET_COMMIT,
                    0,
                    |buf| {
                        encode_txn_offset_commit_request(
                            buf, &tid, group_id, pid, epoch, topic, *part, *off,
                        )
                    },
                    timeout,
                )
                .await?;
            let err = decode_txn_offset_commit_response(&mut body.clone())?;
            if err != 0 {
                return Err(Error::broker(err, "TxnOffsetCommit"));
            }
        }
        Ok(())
    }

    async fn end_txn(&self, committed: bool) -> Result<()> {
        let Some(tid) = self.inner.shared.cfg.transactional_id.clone() else {
            return Err(Error::protocol("transactional.id is not set"));
        };
        let timeout = self.inner.shared.cfg.request_timeout;
        let pid = self.inner.shared.producer_id;
        let epoch = self.inner.shared.producer_epoch;
        let mut meta = self.inner.shared.meta.lock().await;
        let body = meta
            .roundtrip(
                END_TXN,
                0,
                |buf| encode_end_txn_request(buf, &tid, pid, epoch, committed),
                timeout,
            )
            .await?;
        let err = decode_end_txn_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "EndTxn"));
        }
        self.inner.shared.in_txn.store(false, Ordering::SeqCst);
        self.inner.shared.txn_partitions.lock().clear();
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        let deadline = Instant::now() + self.inner.shared.cfg.request_timeout;
        loop {
            self.flush_workers().await?;
            if self.inner.shared.retries_out.load(Ordering::SeqCst) == 0 {
                self.flush_workers().await?;
                if self.inner.shared.retries_out.load(Ordering::SeqCst) == 0 {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let notified = self.inner.shared.cache_nudge.notified();
            tokio::pin!(notified);
            let rest = deadline.saturating_duration_since(Instant::now());
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(rest.min(Duration::from_millis(5))) => {}
            }
        }
    }

    pub async fn close(self) -> Result<()> {
        let workers: Vec<WorkerHandle> = self
            .inner
            .shared
            .nodes
            .lock()
            .values()
            .flatten()
            .cloned()
            .collect();
        let mut rxs = Vec::with_capacity(workers.len());
        for w in &workers {
            let (tx, rx) = oneshot::channel();
            drop(w.ctrl.send(Ctrl::Close(tx)).await);
            rxs.push(rx);
        }
        for rx in rxs {
            drop(rx.await);
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
    let mut conn =
        BrokerConn::connect_tls(addr, &cfg.client_id, cfg.connect_timeout, cfg.tls.as_ref())
            .await?;
    let _versions = conn
        .roundtrip(
            API_VERSIONS,
            3,
            |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
            cfg.request_timeout,
        )
        .await?;
    crate::protocol::sasl::authenticate(
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

async fn partitions_for(shared: &Shared, topic: &Arc<str>) -> Result<i32> {
    if let Some(n) = shared.cluster.lock().partition_count(topic) {
        nudge_leaders(shared, topic);
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
    let n = crate::protocol::buf::i32_from_usize(t.partitions.len())?;
    if n <= 0 {
        return Err(Error::UnknownTopic(topic.to_string()));
    }
    {
        let mut cluster = shared.cluster.lock();
        cluster.apply(&resp);
    }
    nudge_leaders(shared, topic);
    Ok(n)
}

fn try_nudge_node(tx: &mpsc::Sender<i32>, node: i32) {
    tx.try_send(node).unwrap_or(());
}

fn nudge_leaders(shared: &Shared, topic: &str) {
    let cluster = shared.cluster.lock();
    if let Some(leaders) = cluster.leaders.get(topic) {
        for node in leaders {
            if *node >= 0 {
                try_nudge_node(&shared.connect_tx, *node);
            }
        }
    }
}

async fn connect_loop(weak: std::sync::Weak<Shared>, mut rx: mpsc::Receiver<i32>, cap: usize) {
    while let Some(node) = rx.recv().await {
        let Some(shared) = weak.upgrade() else {
            break;
        };
        if shared
            .nodes
            .lock()
            .get(&node)
            .is_some_and(|w| !w.is_empty())
        {
            shared.cache_nudge.notify_waiters();
            continue;
        }
        let addr = shared.cluster.lock().brokers.get(&node).cloned();
        let Some(addr) = addr else {
            continue;
        };
        {
            let mut nodes = shared.nodes.lock();
            let _ = nodes.entry(node).or_insert_with(Vec::new);
        }
        match spawn_node_workers(&shared, node, &addr, cap).await {
            Ok(workers) => {
                let _prev = shared.nodes.lock().insert(node, workers);
                *shared.last_meta_err.lock() = None;
            }
            Err(e) => {
                let _ = shared.nodes.lock().remove(&node);
                *shared.last_meta_err.lock() = Some(clone_err(&e));
            }
        }
        shared.cache_nudge.notify_waiters();
    }
}

async fn spawn_node_workers(
    shared: &Arc<Shared>,
    node: i32,
    addr: &str,
    cap: usize,
) -> Result<Vec<WorkerHandle>> {
    let n_conn = shared.cfg.connections.max(1);
    let mut workers = Vec::with_capacity(n_conn);
    for _ in 0..n_conn {
        let conn = open_conn(addr, &shared.cfg).await?;
        let (data_tx, data_rx) = mpsc::channel(cap);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(16);
        let worker = Worker {
            node_id: node,
            conn,
            data: data_rx,
            ctrl: ctrl_rx,
            shared: shared.clone(),
            write_buf: BytesMut::with_capacity(2 * 1024 * 1024),
            pending: Vec::with_capacity(shared.cfg.batch_records.min(8192)),
            in_flight: VecDeque::new(),
            fail: None,
        };
        drop(tokio::spawn(worker.run()));
        workers.push(WorkerHandle {
            data: data_tx,
            ctrl: ctrl_tx,
        });
    }
    Ok(workers)
}

async fn retry_loop(weak: std::sync::Weak<Shared>, mut rx: mpsc::Receiver<Pending>) {
    while let Some(p) = rx.recv().await {
        if let Some(shared) = weak.upgrade() {
            retry_one(&shared, p).await;
            let _ = shared.retries_out.fetch_sub(1, Ordering::SeqCst);
            shared.cache_nudge.notify_waiters();
        }
    }
}

async fn retry_one(shared: &Arc<Shared>, mut p: Pending) {
    if Instant::now() >= p.deadline {
        fail_pendings(vec![p], Error::Timeout);
        return;
    }
    shared.cluster.lock().invalidate_topic(p.rec.topic.as_ref());
    if let Err(e) = partitions_for(shared, &p.rec.topic).await {
        fail_pendings(vec![p], e);
        return;
    }
    if p.rec.partition.is_none() {
        if let Some(np) = shared.cluster.lock().partition_count(p.rec.topic.as_ref()) {
            p.rec.partition = Some(pick_part(&p.rec, np, &shared.rr));
        }
    }
    let Some(part) = p.rec.partition else {
        fail_pendings(vec![p], Error::protocol("retry without partition"));
        return;
    };
    let leader = shared.cluster.lock().leader(p.rec.topic.as_ref(), part);
    let Ok((node, _)) = leader else {
        let topic = p.rec.topic.to_string();
        fail_pendings(
            vec![p],
            Error::NoLeader {
                topic,
                partition: part,
            },
        );
        return;
    };
    try_nudge_node(&shared.connect_tx, node);
    let deadline = p.deadline;
    loop {
        if Instant::now() >= deadline {
            fail_pendings(vec![p], Error::Timeout);
            return;
        }
        let handle = {
            let nodes = shared.nodes.lock();
            nodes.get(&node).and_then(|ws| {
                if ws.is_empty() {
                    None
                } else {
                    let i = usize::try_from(part).unwrap_or(0) % ws.len();
                    ws.get(i).cloned()
                }
            })
        };
        if let Some(w) = handle {
            if w.data.send(p).await.is_err() {
                return;
            }
            return;
        }
        let notified = shared.cache_nudge.notified();
        tokio::pin!(notified);
        let rest = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(rest) => {
                fail_pendings(vec![p], Error::Timeout);
                return;
            }
        }
    }
}

struct Worker {
    node_id: i32,
    conn: BrokerConn,
    data: mpsc::Receiver<Pending>,
    ctrl: mpsc::Receiver<Ctrl>,
    shared: Arc<Shared>,
    write_buf: BytesMut,
    pending: Vec<Pending>,
    in_flight: VecDeque<InFlight>,
    fail: Option<Error>,
}

struct InFlight {
    correlation: i32,
    groups: Vec<(Arc<str>, i32, Vec<Pending>)>,
}

impl Worker {
    fn note_fail(&mut self, err: Error) {
        if self.fail.is_none() {
            self.fail = Some(err);
        }
    }

    fn take_fail(&mut self) -> Result<()> {
        match self.fail.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

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
                    fail_pendings(std::mem::take(&mut self.pending), clone_err(&e));
                    self.note_fail(e);
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
                    fail_inflight(&mut self.in_flight, clone_err(&e));
                    self.note_fail(e);
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
                        self.drain_inflight().await;
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
                            self.drain_inflight().await;
                            break;
                        }
                        Some(c) => {
                            let close = matches!(&c, Ctrl::Close(_));
                            self.pull_ready();
                            self.drain_inflight().await;
                            match c {
                                Ctrl::Flush(tx) | Ctrl::Close(tx) => {
                                    drop(tx.send(self.take_fail()));
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
        let batch: Vec<Pending> = self.pending.drain(..n).collect();
        if let Some(p) = batch.iter().find(|p| p.rec.partition.is_none()) {
            let e = Error::protocol(format!("produce without partition topic={}", p.rec.topic));
            fail_pendings(batch, clone_err(&e));
            return Err(e);
        }
        let now = now_ms();
        let mut groups = group_pending(batch);
        if groups.is_empty() {
            return Ok(());
        }
        assign_sequences(&mut groups, self.shared.producer_id, &self.shared.seqs);
        self.add_txn_partitions(&groups).await?;
        let transactional_id = if self.shared.in_txn.load(Ordering::SeqCst) {
            self.shared.cfg.transactional_id.as_deref()
        } else {
            None
        };

        let version = self.shared.produce_version;
        let acks = self.shared.cfg.acks;
        let timeout_ms =
            i32::try_from(self.shared.cfg.request_timeout.as_millis()).unwrap_or(i32::MAX);
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
        )?;
        encode_produce_body(
            &mut self.write_buf,
            version,
            acks,
            timeout_ms,
            &groups,
            compression,
            now,
            self.shared.producer_id,
            self.shared.producer_epoch,
            transactional_id,
        )?;
        let size = crate::protocol::buf::i32_from_usize(self.write_buf.len().saturating_sub(4))?;
        crate::protocol::buf::patch_i32(&mut self.write_buf, 0, size)?;
        if let Err(e) = self
            .conn
            .write_all_timeout(&self.write_buf, self.shared.cfg.request_timeout)
            .await
        {
            if e.is_retriable() {
                let _ = self.shared.nodes.lock().remove(&self.node_id);
                self.requeue(groups);
                return Ok(());
            }
            fail_groups(groups, clone_err(&e));
            return Err(e);
        }

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
                if e.is_retriable() {
                    let _ = self.shared.nodes.lock().remove(&self.node_id);
                    self.requeue(inf.groups);
                    return Ok(());
                }
                fail_groups(inf.groups, clone_err(&e));
                return Err(e);
            }
        };
        let responses = match decode_produce_response(&mut body.clone(), version) {
            Ok(r) => r,
            Err(e) => {
                fail_groups(inf.groups, clone_err(&e));
                return Err(e);
            }
        };
        let mut first_err: Option<Error> = None;
        for (topic, part, pendings) in inf.groups {
            let found = responses
                .iter()
                .find(|r| r.topic.as_str() == topic.as_ref() && r.partition == part);
            match found {
                None => {
                    let e = Error::protocol("missing produce response");
                    fail_pendings(pendings, clone_err(&e));
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Some(r) if r.error_code != 0 => {
                    let e = Error::broker(r.error_code, format!("{topic}-{part}"));
                    if e.is_retriable() {
                        self.shared.cluster.lock().invalidate_topic(topic.as_ref());
                        drop(self.shared.meta_tx.try_send(topic.clone()));
                        self.requeue_pendings(pendings);
                    } else {
                        fail_pendings(pendings, clone_err(&e));
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
                Some(r) => {
                    for (i, p) in pendings.into_iter().enumerate() {
                        if let Some(tx) = p.tx {
                            drop(tx.send(Ok(RecordMetadata {
                                topic: topic.to_string(),
                                partition: part,
                                offset: r.base_offset + i64::try_from(i).unwrap_or(0),
                            })));
                        }
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn requeue(&self, groups: Vec<(Arc<str>, i32, Vec<Pending>)>) {
        for (_, _, pendings) in groups {
            self.requeue_pendings(pendings);
        }
    }

    async fn add_txn_partitions(&self, groups: &[(Arc<str>, i32, Vec<Pending>)]) -> Result<()> {
        let Some(tid) = self.shared.cfg.transactional_id.clone() else {
            return Ok(());
        };
        if !self.shared.in_txn.load(Ordering::SeqCst) {
            return Err(Error::protocol("produce outside a transaction"));
        }
        let mut added: Vec<(Arc<str>, i32)> = Vec::new();
        {
            let mut set = self.shared.txn_partitions.lock();
            for (topic, part, _) in groups {
                if set.insert((topic.clone(), *part)) {
                    added.push((topic.clone(), *part));
                }
            }
        }
        if added.is_empty() {
            return Ok(());
        }
        let timeout = self.shared.cfg.request_timeout;
        let pid = self.shared.producer_id;
        let epoch = self.shared.producer_epoch;
        let mut meta = self.shared.meta.lock().await;
        for (topic, part) in added {
            let body = meta
                .roundtrip(
                    ADD_PARTITIONS_TO_TXN,
                    0,
                    |buf| {
                        encode_add_partitions_to_txn_request(
                            buf,
                            &tid,
                            pid,
                            epoch,
                            topic.as_ref(),
                            part,
                        )
                    },
                    timeout,
                )
                .await?;
            let err = decode_add_partitions_to_txn_response(&mut body.clone())?;
            if err != 0 {
                return Err(Error::broker(err, "AddPartitionsToTxn"));
            }
        }
        Ok(())
    }

    fn requeue_pendings(&self, pendings: Vec<Pending>) {
        for p in pendings {
            let _ = self.shared.retries_out.fetch_add(1, Ordering::SeqCst);
            if self.shared.retry_tx.try_send(p).is_err() {
                let _ = self.shared.retries_out.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    async fn drain_inflight(&mut self) {
        while !self.pending.is_empty() || !self.in_flight.is_empty() {
            if !self.pending.is_empty() {
                while self.in_flight.len() >= self.shared.cfg.max_in_flight {
                    if let Err(e) = self.wait_one().await {
                        fail_inflight(&mut self.in_flight, clone_err(&e));
                        self.note_fail(e);
                    }
                }
                if let Err(e) = self.fire().await {
                    fail_pendings(std::mem::take(&mut self.pending), clone_err(&e));
                    self.note_fail(e);
                }
            } else if let Err(e) = self.wait_one().await {
                fail_inflight(&mut self.in_flight, clone_err(&e));
                self.note_fail(e);
            }
        }
    }
}

fn group_pending(batch: Vec<Pending>) -> Vec<(Arc<str>, i32, Vec<Pending>)> {
    if batch.is_empty() {
        return Vec::new();
    }
    let Some(first) = batch.first() else {
        return Vec::new();
    };
    let topic0 = first.rec.topic.clone();
    let part0 = first.rec.partition.unwrap_or(-1);
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

fn assign_sequences(
    groups: &mut [(Arc<str>, i32, Vec<Pending>)],
    producer_id: i64,
    seqs: &parking_lot::Mutex<HashMap<(Arc<str>, i32), i32>>,
) {
    if producer_id < 0 {
        return;
    }
    for (topic, partition, pendings) in groups.iter_mut() {
        if pendings.iter().all(|p| p.seq.is_some()) {
            continue;
        }
        let base = next_sequence(seqs, producer_id, topic, *partition, pendings.len());
        for (i, p) in pendings.iter_mut().enumerate() {
            if p.seq.is_none() {
                p.seq = Some(base.saturating_add(i32::try_from(i).unwrap_or(0)));
            }
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "produce body needs pid, epoch, seq, and batch knobs together"
)]
fn encode_produce_body(
    buf: &mut BytesMut,
    version: i16,
    acks: i16,
    timeout_ms: i32,
    groups: &[(Arc<str>, i32, Vec<Pending>)],
    compression: Compression,
    now: i64,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: Option<&str>,
) -> Result<()> {
    let flexible = version >= 9;
    let transactional = transactional_id.is_some();
    if version >= 3 {
        crate::protocol::buf::put_string(buf, flexible, transactional_id)?;
    }
    buf.put_i16(acks);
    buf.put_i32(timeout_ms);
    let mut topics: Vec<&Arc<str>> = Vec::new();
    for (t, _, _) in groups {
        if !topics.iter().any(|x| x.as_ref() == t.as_ref()) {
            topics.push(t);
        }
    }
    crate::protocol::buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for topic in topics {
        crate::protocol::buf::put_string(buf, flexible, Some(topic.as_ref()))?;
        let idxs: Vec<usize> = groups
            .iter()
            .enumerate()
            .filter(|(_, (t, _, _))| t.as_ref() == topic.as_ref())
            .map(|(i, _)| i)
            .collect();
        crate::protocol::buf::put_array_len(buf, flexible, Some(idxs.len()))?;
        for i in idxs {
            let Some((_, partition, pendings)) = groups.get(i) else {
                continue;
            };
            buf.put_i32(*partition);
            let base_sequence = pendings
                .first()
                .and_then(|p| p.seq)
                .unwrap_or(if producer_id < 0 { -1 } else { 0 });
            if flexible {
                let mut recs = BytesMut::new();
                encode_pendings(
                    &mut recs,
                    pendings,
                    compression,
                    now,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                    transactional,
                )?;
                crate::protocol::buf::put_bytes(buf, true, Some(&recs))?;
                crate::protocol::buf::put_empty_tagged_fields(buf);
            } else {
                let len_pos = buf.len();
                buf.put_i32(0);
                encode_pendings(
                    buf,
                    pendings,
                    compression,
                    now,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                    transactional,
                )?;
                let rec_len =
                    crate::protocol::buf::i32_from_usize(buf.len().saturating_sub(len_pos + 4))?;
                crate::protocol::buf::patch_i32(buf, len_pos, rec_len)?;
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

fn next_sequence(
    seqs: &parking_lot::Mutex<HashMap<(Arc<str>, i32), i32>>,
    producer_id: i64,
    topic: &Arc<str>,
    partition: i32,
    count: usize,
) -> i32 {
    if producer_id < 0 {
        return -1;
    }
    let mut g = seqs.lock();
    let e = g.entry((topic.clone(), partition)).or_insert(0);
    let base = *e;
    *e = e.saturating_add(i32::try_from(count).unwrap_or(i32::MAX));
    base
}

#[expect(
    clippy::too_many_arguments,
    reason = "record batch header and payload knobs travel together"
)]
fn encode_pendings(
    buf: &mut BytesMut,
    pendings: &[Pending],
    compression: Compression,
    now: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    transactional: bool,
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
            attributes: (compression as i16)
                | if transactional {
                    crate::protocol::records::ATTR_TRANSACTIONAL
                } else {
                    0
                },
            base_timestamp: base_ts,
            max_timestamp: max_ts,
            count: crate::protocol::buf::i32_from_usize(pendings.len())?,
            producer_id,
            producer_epoch,
            base_sequence,
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
                drop(tx.send(Ok(RecordMetadata {
                    topic: topic.to_string(),
                    partition: part,
                    offset: -1,
                })));
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
            drop(tx.send(Err(clone_err(&err))));
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

fn pick_part(rec: &ProduceRecord, np: i32, rr: &AtomicI32) -> i32 {
    if let Some(p) = rec.partition {
        return p;
    }
    if let Some(k) = &rec.key {
        partition_for_key(k, np)
    } else {
        to_positive(rr.fetch_add(1, Ordering::Relaxed)) % np
    }
}

fn peek_meta_err(shared: &Shared) -> Option<Error> {
    shared.last_meta_err.lock().as_ref().map(clone_err)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
