use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, TlsConfig};
use crate::partitioner::{to_positive, Partitioner, PartitionerBox};
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, decode_produce_response,
    encode_api_versions_request, encode_metadata_request, ApiVersion,
};
use crate::protocol::api_keys::{
    pick_version, ADD_OFFSETS_TO_TXN, ADD_PARTITIONS_TO_TXN, API_VERSIONS, END_TXN,
    FIND_COORDINATOR, GET_TELEMETRY_SUBSCRIPTIONS, INIT_PRODUCER_ID, METADATA, PRODUCE,
    TXN_OFFSET_COMMIT,
};
use crate::protocol::group::{
    decode_find_coordinator_response, encode_find_coordinator_request_typed, COORDINATOR_GROUP,
    COORDINATOR_TRANSACTION,
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
    TxnOffsetPartition, TxnOffsetTopic, TxnPartitionsTopic,
};

/// Produce settings. Prefer the chainable builders; raw fields remain writable.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Bootstrap brokers, `host:port`.
    pub bootstrap: Vec<String>,
    /// Kafka `client.id`.
    pub client_id: String,
    /// Kafka `acks` (`0`, `1`, or `-1`). Prefer [`Self::acks`] with [`crate::Acks`].
    pub acks: i16,
    /// How long to wait for a batch to fill.
    pub linger: Duration,
    /// Max records in one Produce batch.
    pub batch_records: usize,
    /// Max bytes in one Produce batch.
    pub batch_bytes: usize,
    /// Per-request timeout (produce, metadata, init pid).
    pub request_timeout: Duration,
    /// Kafka `delivery.timeout.ms`. Time from queue until ack or timeout,
    /// including retries. Default 30s (this crate; Java defaults to 120s).
    pub delivery_timeout: Duration,
    /// Kafka `max.block.ms`. How long [`crate::Producer::send`] waits for
    /// metadata and a leader connection. Default 30s (this crate; Java
    /// defaults to 60s). [`crate::Producer::try_send`] does not wait.
    pub max_block: Duration,
    /// Kafka `retry.backoff.ms`. Wait after a retriable Produce failure
    /// before the next attempt. Default 100ms (Java / librdkafka). Zero
    /// retries immediately. Grows as `base * 2^n` up to
    /// [`Self::retry_backoff_max`].
    pub retry_backoff: Duration,
    /// Kafka `retry.backoff.max.ms`. Cap on [`Self::retry_backoff`]
    /// exponential growth. Default 1s.
    pub retry_backoff_max: Duration,
    /// Kafka `metadata.max.age.ms`. Refresh cached Metadata after this age.
    /// Default 5 minutes (Java). Zero refreshes on every lookup.
    /// [`crate::Producer::try_send`] still uses a stale cache and nudges a
    /// background refresh; [`crate::Producer::send`] waits for a fresh copy.
    pub metadata_max_age: Duration,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Kafka `reconnect.backoff.ms`. Wait after a failed TCP/TLS/SASL
    /// connect to a broker before the next attempt. Default 50ms (Java).
    /// Zero retries immediately. Grows as `base * 2^n` up to
    /// [`Self::reconnect_backoff_max`]. Distinct from [`Self::retry_backoff`]
    /// (Produce RPC retries).
    pub reconnect_backoff: Duration,
    /// Kafka `reconnect.backoff.max.ms`. Cap on [`Self::reconnect_backoff`]
    /// exponential growth. Default 1s (Java).
    pub reconnect_backoff_max: Duration,
    /// Kafka `allow.auto.create.topics` on Metadata.
    pub allow_auto_topic_creation: bool,
    /// Record batch compression.
    pub compression: Compression,
    /// SASL PLAIN `(username, password)`.
    pub sasl_plain: Option<(String, String)>,
    /// SASL SCRAM-SHA-256 `(username, password)`.
    pub sasl_scram: Option<(String, String)>,
    /// SASL SCRAM-SHA-512 `(username, password)`.
    pub sasl_scram_sha512: Option<(String, String)>,
    /// Unsecured OAUTHBEARER principal.
    pub sasl_oauthbearer: Option<String>,
    /// OIDC client-credentials, then OAUTHBEARER.
    pub sasl_oauthbearer_oidc: Option<crate::OidcConfig>,
    /// TCP connections per leader. Idempotent produce uses one per partition.
    pub connections: usize,
    /// Pipelined Produce requests per connection. Capped at 5 when idempotent.
    pub max_in_flight: usize,
    /// Kafka `enable.idempotence`.
    pub enable_idempotence: bool,
    /// Kafka `transactional.id`. Implies idempotence.
    pub transactional_id: Option<String>,
    /// Kafka `transaction.timeout.ms` on InitProducerId. Default 60s (Java).
    ///
    /// The transaction coordinator aborts the txn if the producer does not
    /// finish within this timeout. Must not exceed the broker's
    /// `transaction.max.timeout.ms`.
    pub transaction_timeout: Duration,
    /// rustls. `None` is plain TCP.
    pub tls: Option<TlsConfig>,
    /// How records without an explicit partition are mapped.
    pub partitioner: PartitionerBox,
    /// Produce interceptors. Empty is a no-op.
    pub interceptors: crate::interceptor::ProducerInterceptors,
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
            delivery_timeout: Duration::from_secs(30),
            max_block: Duration::from_secs(30),
            retry_backoff: crate::config::DEFAULT_RETRY_BACKOFF,
            retry_backoff_max: crate::config::DEFAULT_RETRY_BACKOFF_MAX,
            metadata_max_age: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(10),
            reconnect_backoff: crate::config::DEFAULT_RECONNECT_BACKOFF,
            reconnect_backoff_max: crate::config::DEFAULT_RECONNECT_BACKOFF_MAX,
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
            transaction_timeout: Duration::from_secs(60),
            tls: None,
            partitioner: PartitionerBox::default(),
            interceptors: crate::interceptor::ProducerInterceptors::default(),
        }
    }
}

impl ProducerConfig {
    /// Bootstrap brokers, for example `["127.0.0.1:9092"]`.
    pub fn bootstrap<S: Into<String>>(servers: impl IntoIterator<Item = S>) -> Self {
        Self {
            bootstrap: servers.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// Kafka `client.id`.
    #[must_use]
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    /// Acknowledgements. Prefer this over writing [`Self::acks`] as a raw `i16`.
    #[must_use]
    pub fn acks(mut self, acks: crate::Acks) -> Self {
        self.acks = acks.as_i16();
        self
    }

    /// How long to wait for a batch to fill before sending.
    #[must_use]
    pub fn linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    /// Max records in one Produce batch.
    #[must_use]
    pub fn batch_records(mut self, n: usize) -> Self {
        self.batch_records = n;
        self
    }

    /// Max bytes in one Produce batch.
    #[must_use]
    pub fn batch_bytes(mut self, n: usize) -> Self {
        self.batch_bytes = n;
        self
    }

    /// Record batch compression.
    #[must_use]
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// TCP connections per leader. Idempotent produce uses one per partition.
    #[must_use]
    pub fn connections(mut self, n: usize) -> Self {
        self.connections = n.max(1);
        self
    }

    /// Pipelined Produce requests per connection. Capped at 5 when idempotent.
    #[must_use]
    pub fn max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n.max(1);
        self
    }

    /// `enable.idempotence`. Also forces `acks=all` and `max_in_flight ≤ 5`.
    #[must_use]
    pub fn idempotent(mut self, on: bool) -> Self {
        self.enable_idempotence = on;
        self
    }

    /// `transactional.id`. Implies idempotence.
    #[must_use]
    pub fn transactional_id(mut self, id: impl Into<String>) -> Self {
        self.transactional_id = Some(id.into());
        self
    }

    /// Kafka `transaction.timeout.ms` on InitProducerId. Default 60s (Java).
    #[must_use]
    pub fn transaction_timeout(mut self, timeout: Duration) -> Self {
        self.transaction_timeout = timeout;
        self
    }

    /// Replace the default murmur2 / round-robin partitioner.
    #[must_use]
    pub fn partitioner(mut self, p: impl Partitioner) -> Self {
        self.partitioner = PartitionerBox::new(p);
        self
    }

    /// Append a produce interceptor. They run in insertion order.
    #[must_use]
    pub fn interceptor(mut self, i: impl crate::interceptor::ProducerInterceptor) -> Self {
        self.interceptors.push(i);
        self
    }

    /// SASL. Replaces any previously set mechanism.
    #[must_use]
    pub fn sasl(mut self, sasl: crate::Sasl) -> Self {
        sasl.apply_to(
            &mut self.sasl_plain,
            &mut self.sasl_scram,
            &mut self.sasl_scram_sha512,
            &mut self.sasl_oauthbearer,
            &mut self.sasl_oauthbearer_oidc,
        );
        self
    }

    /// rustls. No OpenSSL.
    #[must_use]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        crate::config::apply_tls(&mut self.tls, tls);
        self
    }

    /// Per-request timeout (produce, metadata, init pid).
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Kafka `delivery.timeout.ms`. Time from queue until ack or [`crate::Error::Timeout`].
    ///
    /// Default 30s. Java `delivery.timeout.ms` defaults to 120s. Per-RPC waits
    /// still use [`Self::request_timeout`].
    #[must_use]
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.delivery_timeout = timeout;
        self
    }

    /// Kafka `max.block.ms`. How long [`crate::Producer::send`] waits for metadata.
    ///
    /// Default 30s. Java `max.block.ms` defaults to 60s. [`crate::Producer::try_send`]
    /// returns [`crate::Error::QueueFull`] instead of waiting.
    #[must_use]
    pub fn max_block(mut self, timeout: Duration) -> Self {
        self.max_block = timeout;
        self
    }

    /// Kafka `retry.backoff.ms`. Wait after a retriable Produce before retrying.
    ///
    /// Default 100ms. Zero retries immediately. Combined with
    /// [`Self::retry_backoff_max`] this is exponential (`base * 2^n`), no jitter.
    #[must_use]
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }

    /// Kafka `retry.backoff.max.ms`. Cap on exponential produce retry waits.
    ///
    /// Default 1s. Raised to [`Self::retry_backoff`] when set lower.
    #[must_use]
    pub fn retry_backoff_max(mut self, backoff: Duration) -> Self {
        self.retry_backoff_max = backoff;
        self
    }

    /// Kafka `metadata.max.age.ms`. Refresh cached Metadata after this age.
    ///
    /// Default 5 minutes (Java). Zero refreshes on every lookup.
    #[must_use]
    pub fn metadata_max_age(mut self, max_age: Duration) -> Self {
        self.metadata_max_age = max_age;
        self
    }

    /// TCP connect timeout.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Kafka `reconnect.backoff.ms`. Wait after a failed broker connect.
    ///
    /// Default 50ms (Java). Zero retries immediately. Combined with
    /// [`Self::reconnect_backoff_max`] this is exponential (`base * 2^n`),
    /// no jitter. Bootstrap `connect_tls_any` does not wait between hosts.
    #[must_use]
    pub fn reconnect_backoff(mut self, backoff: Duration) -> Self {
        self.reconnect_backoff = backoff;
        self
    }

    /// Kafka `reconnect.backoff.max.ms`. Cap on exponential reconnect waits.
    ///
    /// Default 1s (Java). Raised to [`Self::reconnect_backoff`] when set lower.
    #[must_use]
    pub fn reconnect_backoff_max(mut self, backoff: Duration) -> Self {
        self.reconnect_backoff_max = backoff;
        self
    }

    /// Kafka `allow.auto.create.topics` on Metadata.
    #[must_use]
    pub fn allow_auto_create_topics(mut self, allow: bool) -> Self {
        self.allow_auto_topic_creation = allow;
        self
    }
}

/// One record to produce.
#[derive(Debug, Clone)]
pub struct ProduceRecord {
    /// Topic name.
    pub topic: Arc<str>,
    /// Explicit partition. `None` uses the [`Partitioner`].
    pub partition: Option<i32>,
    /// Optional key (murmur2 when the default partitioner is used).
    pub key: Option<Bytes>,
    /// Optional value.
    pub value: Option<Bytes>,
    /// Timestamp in milliseconds since the Unix epoch. `None` uses the producer clock.
    pub timestamp: Option<i64>,
    /// Record headers.
    pub headers: Vec<RecordHeader>,
}

impl ProduceRecord {
    /// Start a record for `topic`.
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

    /// Set the key. The default partitioner hashes it with murmur2.
    #[must_use]
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the value.
    #[must_use]
    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Pin the partition. Skips the [`Partitioner`].
    #[must_use]
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Record timestamp in milliseconds since the Unix epoch.
    ///
    /// `None` (the default) uses the producer clock when the batch is written.
    #[must_use]
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Append one header. Call more than once for several headers.
    #[must_use]
    pub fn header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(RecordHeader::new(key, value));
        self
    }

    /// Append a header with a null value (Java `RecordHeader(key, null)`).
    #[must_use]
    pub fn null_header(mut self, key: impl Into<String>) -> Self {
        self.headers.push(RecordHeader::null(key));
        self
    }

    /// Replace all headers.
    #[must_use]
    pub fn headers(mut self, headers: Vec<RecordHeader>) -> Self {
        self.headers = headers;
        self
    }
}

/// Broker acknowledgement for a produced record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    /// Topic name.
    pub topic: String,
    /// Partition the record was written to.
    pub partition: i32,
    /// Assigned offset, or `-1` when `acks=0`.
    pub offset: i64,
}

struct Pending {
    rec: ProduceRecord,
    tx: Option<oneshot::Sender<Result<RecordMetadata>>>,
    seq: Option<i32>,
    deadline: Instant,
    queued_at: Instant,
    /// Failed Produce attempts so far. The first retry sleeps
    /// [`ProducerConfig::retry_backoff`].
    retry: u32,
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

struct FastRoute {
    topic: Arc<str>,
    np: i32,
    handles: Vec<WorkerHandle>,
}

struct Shared {
    cfg: ProducerConfig,
    cluster: parking_lot::Mutex<Cluster>,
    meta: Mutex<BrokerConn>,
    /// Transaction coordinator. `None` when `transactional.id` is unset.
    txn: Mutex<Option<BrokerConn>>,
    metadata_version: i16,
    produce_version: i16,
    add_partitions_version: i16,
    txn_offset_version: i16,
    telemetry_version: Option<i16>,
    client_instance_id: parking_lot::Mutex<Option<[u8; 16]>>,
    partitioner: Arc<dyn Partitioner>,
    producer_id: i64,
    producer_epoch: i16,
    seqs: parking_lot::Mutex<HashMap<(Arc<str>, i32), i32>>,
    cache_nudge: Notify,
    meta_tx: mpsc::Sender<Arc<str>>,
    connect_tx: mpsc::Sender<i32>,
    retry_tx: mpsc::Sender<Pending>,
    last_meta_err: parking_lot::Mutex<Option<Error>>,
    nodes: parking_lot::Mutex<HashMap<i32, Vec<WorkerHandle>>>,
    reconnect_fails: parking_lot::Mutex<HashMap<i32, u32>>,
    retries_out: AtomicUsize,
    in_txn: AtomicBool,
    txn_partitions: parking_lot::Mutex<HashSet<(Arc<str>, i32)>>,
    txn_added: parking_lot::Mutex<HashSet<(Arc<str>, i32)>>,
    fast: parking_lot::Mutex<Option<FastRoute>>,
    m_queued: AtomicU64,
    m_acked: AtomicU64,
    m_errors: AtomicU64,
    m_bytes: AtomicU64,
    ack_latency: crate::metrics::LatencyTracker,
    interceptors: crate::interceptor::ProducerInterceptors,
}

/// Produce client: queue records, batch, and wait for offsets.
#[derive(Clone)]
pub struct Producer {
    inner: Arc<Inner>,
}

struct Inner {
    shared: Arc<Shared>,
}

impl Shared {
    fn note_queued_n(&self, n: u64, bytes: u64) {
        let _ = self.m_queued.fetch_add(n, Ordering::Relaxed);
        let _ = self.m_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn note_acked(&self, n: u64) {
        let _ = self.m_acked.fetch_add(n, Ordering::Relaxed);
    }

    fn note_ack_latency(&self, queued_at: Instant) {
        self.ack_latency.record(queued_at.elapsed());
    }

    fn note_errors(&self, n: u64) {
        let _ = self.m_errors.fetch_add(n, Ordering::Relaxed);
    }
}

fn rec_bytes(rec: &ProduceRecord) -> u64 {
    let k = rec.key.as_ref().map(bytes::Bytes::len).unwrap_or(0);
    let v = rec.value.as_ref().map(bytes::Bytes::len).unwrap_or(0);
    u64::try_from(k.saturating_add(v)).unwrap_or(u64::MAX)
}

impl Producer {
    /// Connect with default config to one bootstrap server.
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(ProducerConfig::bootstrap([bootstrap.into()])).await
    }

    /// Connect using `cfg`. Negotiates ApiVersions, optional SASL/TLS, and
    /// `InitProducerId` when idempotent or transactional.
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
        let (add_partitions_version, txn_offset_version) = if cfg.transactional_id.is_some() {
            let add_p = pick(&versions, ADD_PARTITIONS_TO_TXN, 0, 1).ok_or_else(|| {
                Error::Unsupported("broker does not support AddPartitionsToTxn".into())
            })?;
            let toc = pick(&versions, TXN_OFFSET_COMMIT, 0, 2).ok_or_else(|| {
                Error::Unsupported("broker does not support TxnOffsetCommit".into())
            })?;
            (add_p, toc)
        } else {
            (0, 0)
        };

        let mut producer_id = -1i64;
        let mut producer_epoch = -1i16;
        let mut txn = if let Some(tid) = cfg.transactional_id.as_deref() {
            Some(discover_typed_coord(&cfg, tid, COORDINATOR_TRANSACTION).await?)
        } else {
            None
        };
        if cfg.enable_idempotence {
            let ipid_version = pick(&versions, INIT_PRODUCER_ID, 0, 1).ok_or_else(|| {
                Error::Unsupported("broker does not support InitProducerId".into())
            })?;
            let body = init_producer_id_roundtrip(&cfg, &mut txn, &mut meta, ipid_version).await?;
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
            txn: Mutex::new(txn),
            metadata_version,
            produce_version,
            add_partitions_version,
            txn_offset_version,
            telemetry_version: pick(&versions, GET_TELEMETRY_SUBSCRIPTIONS, 0, 0),
            client_instance_id: parking_lot::Mutex::new(None),
            partitioner: cfg.partitioner.arc(),
            producer_id,
            producer_epoch,
            seqs: parking_lot::Mutex::new(HashMap::new()),
            cache_nudge: Notify::new(),
            meta_tx,
            connect_tx,
            retry_tx,
            last_meta_err: parking_lot::Mutex::new(None),
            nodes: parking_lot::Mutex::new(HashMap::new()),
            reconnect_fails: parking_lot::Mutex::new(HashMap::new()),
            retries_out: AtomicUsize::new(0),
            in_txn: AtomicBool::new(false),
            txn_partitions: parking_lot::Mutex::new(HashSet::new()),
            txn_added: parking_lot::Mutex::new(HashSet::new()),
            fast: parking_lot::Mutex::new(None),
            m_queued: AtomicU64::new(0),
            m_acked: AtomicU64::new(0),
            m_errors: AtomicU64::new(0),
            m_bytes: AtomicU64::new(0),
            ack_latency: crate::metrics::LatencyTracker::new(),
            interceptors: cfg.interceptors.clone(),
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
                    .topic_fresh(topic.as_ref(), shared.cfg.metadata_max_age)
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
        rec.partition = Some(pick_part(rec, np, self.inner.shared.partitioner.as_ref()));
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
        let deadline = Instant::now() + self.inner.shared.cfg.max_block;
        loop {
            if let Some(e) = peek_meta_err(&self.inner.shared) {
                return Err(e);
            }
            let fresh = self
                .inner
                .shared
                .cluster
                .lock()
                .topic_fresh(rec.topic.as_ref(), self.inner.shared.cfg.metadata_max_age);
            if !fresh {
                let _ = partitions_for(&self.inner.shared, &rec.topic).await?;
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

    /// Queue one record and wait for its offset.
    ///
    /// This waits for the Produce response of *this* record before returning.
    /// A loop of `send().await` therefore cannot pipeline. For many records
    /// use [`Self::send_all`] (offsets) or [`Self::try_send`] plus
    /// [`Self::flush`] (throughput).
    pub async fn send(&self, rec: ProduceRecord) -> Result<RecordMetadata> {
        let mut out = self.send_all(std::iter::once(rec)).await?;
        out.pop().ok_or_else(|| Error::protocol("send_all empty"))
    }

    /// Queue every record, then wait for every offset.
    ///
    /// Records are handed to workers as soon as metadata is ready, so batches
    /// fill while later records are still being partitioned. Empty input
    /// returns an empty vec.
    pub async fn send_all(
        &self,
        recs: impl IntoIterator<Item = ProduceRecord>,
    ) -> Result<Vec<RecordMetadata>> {
        let recs: Vec<ProduceRecord> = recs.into_iter().collect();
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        let mut rxs = Vec::with_capacity(recs.len());
        for rec in recs {
            let (tx, rx) = oneshot::channel();
            let mut rec = self.inner.shared.interceptors.on_send(rec);
            self.ensure_ready(&mut rec).await?;
            let w = self.worker_for(&rec).ok_or(Error::Closed)?;
            let now = Instant::now();
            let deadline = now + self.inner.shared.cfg.delivery_timeout;
            let bytes = rec_bytes(&rec);
            w.data
                .send(Pending {
                    rec,
                    tx: Some(tx),
                    seq: None,
                    deadline,
                    queued_at: now,
                    retry: 0,
                })
                .await
                .map_err(|_| Error::Closed)?;
            self.inner.shared.note_queued_n(1, bytes);
            rxs.push(rx);
        }
        let mut out = Vec::with_capacity(rxs.len());
        for rx in rxs {
            out.push(rx.await.map_err(|_| Error::Closed)??);
        }
        Ok(out)
    }

    fn fast_route(&self, rec: &ProduceRecord) -> Option<(i32, WorkerHandle)> {
        let fast = self.inner.shared.fast.lock();
        let f = fast.as_ref()?;
        if f.topic != rec.topic || f.handles.is_empty() || f.np <= 0 {
            return None;
        }
        let p = match rec.partition {
            Some(p) => p,
            None => pick_part(rec, f.np, self.inner.shared.partitioner.as_ref()),
        };
        if p < 0 || p >= f.np {
            return None;
        }
        let i = usize::try_from(p).ok()?;
        let w = f.handles.get(i)?.clone();
        Some((p, w))
    }

    fn remember_fast(&self, rec: &ProduceRecord) {
        let Some(np) = self
            .inner
            .shared
            .cluster
            .lock()
            .partition_count(rec.topic.as_ref())
        else {
            return;
        };
        if np <= 0 {
            return;
        }
        let n = usize::try_from(np).unwrap_or(0);
        let mut handles = Vec::with_capacity(n);
        for i in 0..np {
            let probe = ProduceRecord {
                topic: rec.topic.clone(),
                partition: Some(i),
                key: None,
                value: None,
                timestamp: None,
                headers: Vec::new(),
            };
            let Some(w) = self.worker_for(&probe) else {
                return;
            };
            handles.push(w);
        }
        *self.inner.shared.fast.lock() = Some(FastRoute {
            topic: rec.topic.clone(),
            np,
            handles,
        });
    }

    /// Enqueue without a per-record future. Delivery is observed on [`Self::flush`].
    ///
    /// Returns [`Error::QueueFull`] until metadata and a connection to the
    /// partition leader are ready. Call again; [`Self::send`] waits up to
    /// [`ProducerConfig::max_block`].
    /// Records are never queued without a partition, so each partition is
    /// pinned to one TCP connection on its current leader.
    pub fn try_send(&self, rec: ProduceRecord) -> Result<()> {
        let mut rec = self.inner.shared.interceptors.on_send(rec);
        let w = if let Some((p, w)) = self.fast_route(&rec) {
            rec.partition = Some(p);
            w
        } else {
            let _ = self.apply_cached_partition(&mut rec);
            let Some(w) = self.worker_for(&rec) else {
                self.nudge_topic(&rec);
                return Err(Error::QueueFull);
            };
            self.remember_fast(&rec);
            w
        };
        let now = Instant::now();
        let deadline = now + self.inner.shared.cfg.delivery_timeout;
        let bytes = rec_bytes(&rec);
        w.data
            .try_send(Pending {
                rec,
                tx: None,
                seq: None,
                deadline,
                queued_at: now,
                retry: 0,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?;
        self.inner.shared.note_queued_n(1, bytes);
        Ok(())
    }

    /// Produce counters and ack latency since connect (min/mean/max and p50/p99).
    #[must_use]
    pub fn metrics(&self) -> crate::ProducerMetrics {
        crate::ProducerMetrics {
            records_queued: self.inner.shared.m_queued.load(Ordering::Relaxed),
            records_acked: self.inner.shared.m_acked.load(Ordering::Relaxed),
            produce_errors: self.inner.shared.m_errors.load(Ordering::Relaxed),
            bytes_queued: self.inner.shared.m_bytes.load(Ordering::Relaxed),
            ack_latency: self.inner.shared.ack_latency.snapshot(),
        }
    }

    /// Java `clientInstanceId` (KIP-714 GetTelemetrySubscriptions).
    ///
    /// The first call sends a zero UUID; the broker assigns one. Later calls
    /// return the cached id without another round-trip.
    pub async fn client_instance_id(&self) -> Result<[u8; 16]> {
        if let Some(id) = *self.inner.shared.client_instance_id.lock() {
            return Ok(id);
        }
        let version = self.inner.shared.telemetry_version.ok_or_else(|| {
            Error::Unsupported("broker does not support GetTelemetrySubscriptions".into())
        })?;
        let timeout = self.inner.shared.cfg.request_timeout;
        let mut meta = self.inner.shared.meta.lock().await;
        let id =
            crate::admin::fetch_client_instance_id(&mut meta, version, timeout, [0; 16]).await?;
        drop(meta);
        *self.inner.shared.client_instance_id.lock() = Some(id);
        Ok(id)
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

    /// Java `initTransactions`. [`Self::new`] already runs `InitProducerId`
    /// when [`ProducerConfig::transactional_id`] is set. Safe to call again.
    pub async fn init_transactions(&self) -> Result<()> {
        if self.inner.shared.cfg.transactional_id.is_none() {
            return Err(Error::protocol("transactional.id is not set"));
        }
        if self.inner.shared.producer_id < 0 {
            return Err(Error::protocol("InitProducerId did not run"));
        }
        Ok(())
    }

    /// Start a transaction. Requires [`ProducerConfig::transactional_id`].
    pub async fn begin_transaction(&self) -> Result<()> {
        if self.inner.shared.cfg.transactional_id.is_none() {
            return Err(Error::protocol("transactional.id is not set"));
        }
        self.inner.shared.in_txn.store(true, Ordering::SeqCst);
        self.inner.shared.txn_partitions.lock().clear();
        self.inner.shared.txn_added.lock().clear();
        Ok(())
    }

    /// Flush, then commit the current transaction (`EndTxn` commit).
    pub async fn commit_transaction(&self) -> Result<()> {
        self.flush().await?;
        self.end_txn(true).await
    }

    /// Flush, then abort the current transaction (`EndTxn` abort).
    pub async fn abort_transaction(&self) -> Result<()> {
        self.flush().await?;
        self.end_txn(false).await
    }

    /// Send these offsets to the transaction coordinator (`AddOffsetsToTxn`
    /// then `TxnOffsetCommit`).
    ///
    /// Each item is a [`crate::TopicPartition`] (or anything that converts
    /// to one) and the next fetch offset. For epoch plus a metadata string,
    /// use [`Self::send_offsets_with_metadata`].
    pub async fn send_offsets_to_transaction(
        &self,
        group_id: &str,
        offsets: impl IntoIterator<Item = (impl Into<crate::TopicPartition>, i64)>,
    ) -> Result<()> {
        let items: Vec<(crate::TopicPartition, crate::OffsetAndMetadata)> = offsets
            .into_iter()
            .map(|(tp, o)| (tp.into(), crate::OffsetAndMetadata::new(o)))
            .collect();
        self.send_offsets_with_metadata(group_id, items).await
    }

    /// [`Self::send_offsets_to_transaction`] with leader epoch and metadata.
    pub async fn send_offsets_with_metadata(
        &self,
        group_id: &str,
        offsets: impl IntoIterator<
            Item = (
                impl Into<crate::TopicPartition>,
                impl Into<crate::OffsetAndMetadata>,
            ),
        >,
    ) -> Result<()> {
        let offsets: Vec<(crate::TopicPartition, crate::OffsetAndMetadata)> = offsets
            .into_iter()
            .map(|(tp, md)| (tp.into(), md.into()))
            .collect();
        let Some(tid) = self.inner.shared.cfg.transactional_id.clone() else {
            return Err(Error::protocol("transactional.id is not set"));
        };
        if !self.inner.shared.in_txn.load(Ordering::SeqCst) {
            return Err(Error::protocol("no transaction in progress"));
        }
        let timeout = self.inner.shared.cfg.request_timeout;
        let pid = self.inner.shared.producer_id;
        let epoch = self.inner.shared.producer_epoch;
        let body = txn_roundtrip(
            &self.inner.shared,
            ADD_OFFSETS_TO_TXN,
            0,
            |buf| encode_add_offsets_to_txn_request(buf, &tid, pid, epoch, group_id),
            timeout,
            |body| decode_add_offsets_to_txn_response(&mut { body }),
        )
        .await?;
        let err = decode_add_offsets_to_txn_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "AddOffsetsToTxn"));
        }
        let mut topics: Vec<String> = Vec::new();
        for (tp, _) in &offsets {
            if !topics.iter().any(|t| t == &tp.topic) {
                topics.push(tp.topic.clone());
            }
        }
        for topic in &topics {
            let name = Arc::<str>::from(topic.as_str());
            if partitions_for(&self.inner.shared, &name).await? <= 0 {
                return Err(Error::UnknownTopic(topic.clone()));
            }
        }
        let version = self.inner.shared.txn_offset_version;
        let grouped = {
            let cluster = self.inner.shared.cluster.lock();
            group_txn_offsets(&offsets, |topic, part| cluster.leader_epoch(topic, part))
        };
        let body = group_coord_roundtrip(
            &self.inner.shared.cfg,
            group_id,
            TXN_OFFSET_COMMIT,
            version,
            |buf| {
                encode_txn_offset_commit_request(buf, version, &tid, group_id, pid, epoch, &grouped)
            },
            timeout,
            |body| decode_txn_offset_commit_response(&mut { body }),
        )
        .await?;
        let err = decode_txn_offset_commit_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "TxnOffsetCommit"));
        }
        Ok(())
    }

    /// [`Self::send_offsets_with_metadata`] using a group's identity.
    pub async fn send_offsets_for_group(
        &self,
        group: &crate::ConsumerGroupMetadata,
        offsets: impl IntoIterator<
            Item = (
                impl Into<crate::TopicPartition>,
                impl Into<crate::OffsetAndMetadata>,
            ),
        >,
    ) -> Result<()> {
        self.send_offsets_with_metadata(&group.group_id, offsets)
            .await
    }

    async fn end_txn(&self, committed: bool) -> Result<()> {
        let Some(tid) = self.inner.shared.cfg.transactional_id.clone() else {
            return Err(Error::protocol("transactional.id is not set"));
        };
        let timeout = self.inner.shared.cfg.request_timeout;
        let pid = self.inner.shared.producer_id;
        let epoch = self.inner.shared.producer_epoch;
        let body = txn_roundtrip(
            &self.inner.shared,
            END_TXN,
            0,
            |buf| encode_end_txn_request(buf, &tid, pid, epoch, committed),
            timeout,
            |body| decode_end_txn_response(&mut { body }),
        )
        .await?;
        let err = decode_end_txn_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "EndTxn"));
        }
        self.inner.shared.in_txn.store(false, Ordering::SeqCst);
        self.inner.shared.txn_partitions.lock().clear();
        self.inner.shared.txn_added.lock().clear();
        Ok(())
    }

    /// Wait until queued records are acked (or a broker error). `try_send` Ok
    /// only means queued.
    pub async fn flush(&self) -> Result<()> {
        self.flush_until(Instant::now() + self.inner.shared.cfg.request_timeout)
            .await
    }

    /// [`Self::flush`] with a caller-chosen deadline (Java `flush` + timeout).
    pub async fn flush_timeout(&self, timeout: Duration) -> Result<()> {
        self.flush_until(Instant::now() + timeout).await
    }

    async fn flush_until(&self, deadline: Instant) -> Result<()> {
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

    /// Flush, then stop workers. Further sends return [`Error::Closed`].
    pub async fn close(self) -> Result<()> {
        self.shutdown_workers().await;
        self.inner.shared.interceptors.close();
        Ok(())
    }

    /// Flush for up to `timeout`, then stop workers (Java `close(Duration)`).
    ///
    /// A flush timeout is returned after the producer is closed.
    pub async fn close_timeout(self, timeout: Duration) -> Result<()> {
        let flush = self.flush_timeout(timeout).await;
        self.shutdown_workers().await;
        self.inner.shared.interceptors.close();
        flush
    }

    async fn shutdown_workers(&self) {
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
    }

    /// Partition metadata for `topic` (Java `partitionsFor`: leader, replicas, ISR, offline replicas, leader epoch).
    pub async fn partitions_for(
        &self,
        topic: impl Into<String>,
    ) -> Result<Vec<crate::PartitionInfo>> {
        let topic = topic.into();
        let mut conn = self.inner.shared.meta.lock().await;
        let version = self.inner.shared.metadata_version;
        let allow = self.inner.shared.cfg.allow_auto_topic_creation;
        let timeout = self.inner.shared.cfg.request_timeout;
        let topics = [topic.clone()];
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
        {
            let mut cluster = self.inner.shared.cluster.lock();
            cluster.apply(&resp);
        }
        let infos = crate::consumer::partition_infos_from(&resp, Some(topic.as_str()))?;
        if infos.is_empty() {
            return Err(Error::UnknownTopic(topic));
        }
        Ok(infos)
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

async fn discover_typed_coord(cfg: &ProducerConfig, key: &str, key_type: i8) -> Result<BrokerConn> {
    let timeout = cfg.request_timeout;
    let mut last = Error::protocol("find coordinator failed");
    // FindCoordinator 14/15 is one pass of the bootstrap list; try again.
    for _ in 0..3 {
        for addr in &cfg.bootstrap {
            let mut hop = match open_conn(addr, cfg).await {
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
                    |buf| encode_find_coordinator_request_typed(buf, key, key_type),
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
            return open_conn(&coord_addr, cfg).await;
        }
        match &last {
            Error::Broker { code, .. } if error::coordinator_retriable(*code) => {}
            _ => break,
        }
    }
    Err(last)
}

async fn init_producer_id_roundtrip(
    cfg: &ProducerConfig,
    txn: &mut Option<BrokerConn>,
    meta: &mut BrokerConn,
    version: i16,
) -> Result<Bytes> {
    let txn_id = cfg.transactional_id.clone();
    let timeout = cfg.request_timeout;
    let txn_timeout_ms = i32::try_from(cfg.transaction_timeout.as_millis())
        .unwrap_or(i32::MAX)
        .max(0);
    let first = {
        let conn = txn.as_mut().unwrap_or(meta);
        conn.roundtrip(
            INIT_PRODUCER_ID,
            version,
            |buf| encode_init_producer_id_request(buf, version, txn_id.as_deref(), txn_timeout_ms),
            timeout,
        )
        .await
    };
    let Some(tid) = txn_id else {
        return first;
    };
    match first {
        Ok(body) => {
            let err = decode_init_producer_id_response(&mut body.clone(), version)?.0;
            if !error::coordinator_retriable(err) {
                return Ok(body);
            }
        }
        Err(e) if e.is_retriable() => {}
        Err(e) => return Err(e),
    }
    let new = discover_typed_coord(cfg, &tid, COORDINATOR_TRANSACTION).await?;
    *txn = Some(new);
    let conn = txn
        .as_mut()
        .ok_or_else(|| Error::protocol("no transaction coordinator"))?;
    conn.roundtrip(
        INIT_PRODUCER_ID,
        version,
        |buf| encode_init_producer_id_request(buf, version, Some(tid.as_str()), txn_timeout_ms),
        timeout,
    )
    .await
}

async fn txn_roundtrip(
    shared: &Shared,
    api_key: i16,
    api_version: i16,
    encode_body: impl Fn(&mut BytesMut) -> Result<()>,
    request_timeout: Duration,
    error_of: impl Fn(&[u8]) -> Result<i16>,
) -> Result<Bytes> {
    let tid = shared
        .cfg
        .transactional_id
        .clone()
        .ok_or_else(|| Error::protocol("transactional.id is not set"))?;
    let first = {
        let mut guard = shared.txn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| Error::protocol("no transaction coordinator"))?;
        conn.roundtrip(
            api_key,
            api_version,
            |buf| encode_body(buf),
            request_timeout,
        )
        .await
    };
    match first {
        Ok(body) if !error::coordinator_retriable(error_of(&body)?) => return Ok(body),
        Ok(_) => {}
        Err(e) if e.is_retriable() => {}
        Err(e) => return Err(e),
    }
    let new = discover_typed_coord(&shared.cfg, &tid, COORDINATOR_TRANSACTION).await?;
    let mut guard = shared.txn.lock().await;
    *guard = Some(new);
    let conn = guard
        .as_mut()
        .ok_or_else(|| Error::protocol("no transaction coordinator"))?;
    conn.roundtrip(
        api_key,
        api_version,
        |buf| encode_body(buf),
        request_timeout,
    )
    .await
}

async fn group_coord_roundtrip(
    cfg: &ProducerConfig,
    group_id: &str,
    api_key: i16,
    api_version: i16,
    encode_body: impl Fn(&mut BytesMut) -> Result<()>,
    request_timeout: Duration,
    error_of: impl Fn(&[u8]) -> Result<i16>,
) -> Result<Bytes> {
    let mut coord = discover_typed_coord(cfg, group_id, COORDINATOR_GROUP).await?;
    let body = coord
        .roundtrip(
            api_key,
            api_version,
            |buf| encode_body(buf),
            request_timeout,
        )
        .await?;
    if !error::coordinator_retriable(error_of(&body)?) {
        return Ok(body);
    }
    coord = discover_typed_coord(cfg, group_id, COORDINATOR_GROUP).await?;
    coord
        .roundtrip(
            api_key,
            api_version,
            |buf| encode_body(buf),
            request_timeout,
        )
        .await
}

async fn partitions_for(shared: &Shared, topic: &Arc<str>) -> Result<i32> {
    // Drop the parking_lot guard before `nudge_leaders`. An `if let` on
    // `cluster.lock().partition_count(...)` keeps the guard alive through the
    // body (edition 2021 temporary scope) and deadlocks the non-reentrant mutex.
    let cached = {
        let cluster = shared.cluster.lock();
        match cluster.partition_count(topic) {
            Some(n) if cluster.topic_fresh(topic, shared.cfg.metadata_max_age) => Some(n),
            _ => None,
        }
    };
    if let Some(n) = cached {
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
    drop_fast_topic(shared, topic);
    nudge_leaders(shared, topic);
    Ok(n)
}

fn drop_fast_topic(shared: &Shared, topic: &str) {
    let mut fast = shared.fast.lock();
    if fast.as_ref().is_some_and(|f| f.topic.as_ref() == topic) {
        *fast = None;
    }
}

fn invalidate_cached_topic(shared: &Shared, topic: &str) {
    shared.cluster.lock().invalidate_topic(topic);
    drop_fast_topic(shared, topic);
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
                let _ = shared.reconnect_fails.lock().remove(&node);
                let _prev = shared.nodes.lock().insert(node, workers);
                *shared.last_meta_err.lock() = None;
                shared.cache_nudge.notify_waiters();
            }
            Err(e) => {
                let _ = shared.nodes.lock().remove(&node);
                let fails =
                    crate::config::bump_reconnect_fails(&mut shared.reconnect_fails.lock(), node);
                if e.is_retriable() {
                    let delay = crate::config::reconnect_backoff_delay(
                        shared.cfg.reconnect_backoff,
                        shared.cfg.reconnect_backoff_max,
                        fails,
                    );
                    if delay.is_zero() {
                        try_nudge_node(&shared.connect_tx, node);
                        shared.cache_nudge.notify_waiters();
                    } else {
                        let weak = weak.clone();
                        drop(tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            if let Some(shared) = weak.upgrade() {
                                try_nudge_node(&shared.connect_tx, node);
                            }
                        }));
                    }
                } else {
                    *shared.last_meta_err.lock() = Some(clone_err(&e));
                    shared.cache_nudge.notify_waiters();
                }
            }
        }
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
        fail_pendings(shared, vec![p], Error::Timeout);
        return;
    }
    crate::config::sleep_retry_backoff(
        shared.cfg.retry_backoff,
        shared.cfg.retry_backoff_max,
        p.retry.saturating_sub(1),
        p.deadline,
    )
    .await;
    if Instant::now() >= p.deadline {
        fail_pendings(shared, vec![p], Error::Timeout);
        return;
    }
    invalidate_cached_topic(shared, p.rec.topic.as_ref());
    if let Err(e) = partitions_for(shared, &p.rec.topic).await {
        fail_pendings(shared, vec![p], e);
        return;
    }
    if p.rec.partition.is_none() {
        if let Some(np) = shared.cluster.lock().partition_count(p.rec.topic.as_ref()) {
            p.rec.partition = Some(pick_part(&p.rec, np, shared.partitioner.as_ref()));
        }
    }
    let Some(part) = p.rec.partition else {
        fail_pendings(shared, vec![p], Error::protocol("retry without partition"));
        return;
    };
    let leader = shared.cluster.lock().leader(p.rec.topic.as_ref(), part);
    let Ok((node, _)) = leader else {
        let topic = p.rec.topic.to_string();
        fail_pendings(
            shared,
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
            fail_pendings(shared, vec![p], Error::Timeout);
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
                fail_pendings(shared, vec![p], Error::Timeout);
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
                    fail_pendings(
                        &self.shared,
                        std::mem::take(&mut self.pending),
                        clone_err(&e),
                    );
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
                    fail_inflight(&self.shared, &mut self.in_flight, clone_err(&e));
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
            fail_pendings(&self.shared, batch, clone_err(&e));
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
            fail_groups(&self.shared, groups, clone_err(&e));
            return Err(e);
        }

        if acks == 0 {
            complete_acks0(&self.shared, groups);
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
                fail_groups(&self.shared, inf.groups, clone_err(&e));
                return Err(e);
            }
        };
        let mut body = body;
        let responses = match decode_produce_response(&mut body, version) {
            Ok(r) => r,
            Err(e) => {
                fail_groups(&self.shared, inf.groups, clone_err(&e));
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
                    fail_pendings(&self.shared, pendings, clone_err(&e));
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Some(r) if r.error_code != 0 => {
                    let e = Error::broker(r.error_code, format!("{topic}-{part}"));
                    if e.is_retriable() {
                        invalidate_cached_topic(&self.shared, topic.as_ref());
                        drop(self.shared.meta_tx.try_send(topic.clone()));
                        self.requeue_pendings(pendings);
                    } else {
                        fail_pendings(&self.shared, pendings, clone_err(&e));
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
                Some(r) => {
                    let n = u64::try_from(pendings.len()).unwrap_or(u64::MAX);
                    self.shared.note_acked(n);
                    for (i, p) in pendings.into_iter().enumerate() {
                        self.shared.note_ack_latency(p.queued_at);
                        let md = RecordMetadata {
                            topic: topic.to_string(),
                            partition: part,
                            offset: r.base_offset + i64::try_from(i).unwrap_or(0),
                        };
                        self.shared.interceptors.on_ack(&md);
                        if let Some(tx) = p.tx {
                            drop(tx.send(Ok(md)));
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

    fn requeue(&mut self, groups: Vec<(Arc<str>, i32, Vec<Pending>)>) {
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
        {
            let mut set = self.shared.txn_partitions.lock();
            for (topic, part, _) in groups {
                let _ = set.insert((topic.clone(), *part));
            }
        }
        let timeout = self.shared.cfg.request_timeout;
        let pid = self.shared.producer_id;
        let epoch = self.shared.producer_epoch;
        let version = self.shared.add_partitions_version;
        let added: Vec<(Arc<str>, i32)> = {
            let wanted = self.shared.txn_partitions.lock();
            let sent = self.shared.txn_added.lock();
            wanted
                .iter()
                .filter(|k| !sent.contains(*k))
                .cloned()
                .collect()
        };
        if added.is_empty() {
            return Ok(());
        }
        let topics = group_txn_partitions(&added);
        let body = txn_roundtrip(
            &self.shared,
            ADD_PARTITIONS_TO_TXN,
            version,
            |buf| encode_add_partitions_to_txn_request(buf, &tid, pid, epoch, &topics),
            timeout,
            |body| decode_add_partitions_to_txn_response(&mut { body }),
        )
        .await?;
        let err = decode_add_partitions_to_txn_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "AddPartitionsToTxn"));
        }
        {
            let mut sent = self.shared.txn_added.lock();
            for k in added {
                let _ = sent.insert(k);
            }
        }
        Ok(())
    }

    fn requeue_pendings(&mut self, pendings: Vec<Pending>) {
        for mut p in pendings {
            p.retry = p.retry.saturating_add(1);
            let _ = self.shared.retries_out.fetch_add(1, Ordering::SeqCst);
            match self.shared.retry_tx.try_send(p) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(p)) => {
                    let _ = self.shared.retries_out.fetch_sub(1, Ordering::SeqCst);
                    fail_pendings(&self.shared, vec![p], Error::QueueFull);
                    self.note_fail(Error::QueueFull);
                }
                Err(mpsc::error::TrySendError::Closed(p)) => {
                    let _ = self.shared.retries_out.fetch_sub(1, Ordering::SeqCst);
                    fail_pendings(&self.shared, vec![p], Error::Closed);
                    self.note_fail(Error::Closed);
                }
            }
        }
    }

    async fn drain_inflight(&mut self) {
        while !self.pending.is_empty() || !self.in_flight.is_empty() {
            if !self.pending.is_empty() {
                while self.in_flight.len() >= self.shared.cfg.max_in_flight {
                    if let Err(e) = self.wait_one().await {
                        fail_inflight(&self.shared, &mut self.in_flight, clone_err(&e));
                        self.note_fail(e);
                    }
                }
                if let Err(e) = self.fire().await {
                    fail_pendings(
                        &self.shared,
                        std::mem::take(&mut self.pending),
                        clone_err(&e),
                    );
                    self.note_fail(e);
                }
            } else if let Err(e) = self.wait_one().await {
                fail_inflight(&self.shared, &mut self.in_flight, clone_err(&e));
                self.note_fail(e);
            }
        }
    }
}

fn group_txn_partitions(parts: &[(Arc<str>, i32)]) -> Vec<TxnPartitionsTopic> {
    let mut topics: Vec<TxnPartitionsTopic> = Vec::new();
    for (topic, part) in parts {
        match topics.iter_mut().find(|t| t.topic == topic.as_ref()) {
            Some(slot) => slot.partitions.push(*part),
            None => topics.push(TxnPartitionsTopic {
                topic: topic.to_string(),
                partitions: vec![*part],
            }),
        }
    }
    topics
}

fn group_txn_offsets(
    offsets: &[(crate::TopicPartition, crate::OffsetAndMetadata)],
    epoch_of: impl Fn(&str, i32) -> i32,
) -> Vec<TxnOffsetTopic> {
    let mut topics: Vec<TxnOffsetTopic> = Vec::new();
    for (tp, md) in offsets {
        let leader_epoch = md
            .leader_epoch
            .unwrap_or_else(|| epoch_of(&tp.topic, tp.partition));
        let partition = TxnOffsetPartition {
            partition: tp.partition,
            offset: md.offset,
            leader_epoch,
            metadata: md.metadata.clone(),
        };
        match topics.iter_mut().find(|t| t.topic == tp.topic) {
            Some(slot) => slot.partitions.push(partition),
            None => topics.push(TxnOffsetTopic {
                topic: tp.topic.clone(),
                partitions: vec![partition],
            }),
        }
    }
    topics
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

fn complete_acks0(shared: &Shared, groups: Vec<(Arc<str>, i32, Vec<Pending>)>) {
    let mut n = 0u64;
    for (topic, part, pendings) in groups {
        for p in pendings {
            n = n.saturating_add(1);
            shared.note_ack_latency(p.queued_at);
            let md = RecordMetadata {
                topic: topic.to_string(),
                partition: part,
                offset: -1,
            };
            shared.interceptors.on_ack(&md);
            if let Some(tx) = p.tx {
                drop(tx.send(Ok(md)));
            }
        }
    }
    shared.note_acked(n);
}

fn fail_inflight(shared: &Shared, in_flight: &mut VecDeque<InFlight>, err: Error) {
    while let Some(inf) = in_flight.pop_front() {
        fail_groups(shared, inf.groups, clone_err(&err));
    }
}

fn fail_groups(shared: &Shared, groups: Vec<(Arc<str>, i32, Vec<Pending>)>, err: Error) {
    for (_, _, pendings) in groups {
        fail_pendings(shared, pendings, clone_err(&err));
    }
}

fn fail_pendings(shared: &Shared, pendings: Vec<Pending>, err: Error) {
    let n = u64::try_from(pendings.len()).unwrap_or(u64::MAX);
    shared.note_errors(n);
    for p in pendings {
        shared.interceptors.on_error(&err);
        if let Some(tx) = p.tx {
            drop(tx.send(Err(clone_err(&err))));
        }
    }
}

fn clone_err(err: &Error) -> Error {
    err.clone()
}

fn pick_part(rec: &ProduceRecord, np: i32, partitioner: &dyn Partitioner) -> i32 {
    if let Some(p) = rec.partition {
        return p;
    }
    if np <= 0 {
        return 0;
    }
    let p = partitioner.partition(rec.topic.as_ref(), rec.key.as_deref(), np);
    if (0..np).contains(&p) {
        p
    } else {
        to_positive(p) % np
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
