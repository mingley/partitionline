//! Fetch client with manual partition assignment.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::watch;

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, encode_api_versions_request,
    encode_metadata_request, ApiVersion, MetadataResponse,
};
use crate::protocol::api_keys::{
    pick_version, API_VERSIONS, FETCH, LIST_OFFSETS, METADATA, OFFSET_FOR_LEADER_EPOCH,
};
use crate::protocol::epoch::{
    decode_offset_for_leader_epoch_response, encode_offset_for_leader_epoch_request,
};
use crate::protocol::fetch::{
    decode_fetch_response, encode_fetch_request, FetchPartition, FetchTopic,
};
use crate::protocol::offsets::{decode_list_offsets_response, encode_list_offsets_request};
use crate::protocol::records::Header;
use crate::protocol::sasl;

type RebalanceFn = dyn Fn(&[(String, i32)], &[(String, i32)]) + Send + Sync;

/// Called as `(revoked, assigned)` after a consumer-group assignment change.
///
/// Set with [`ConsumerConfig::on_rebalance`]. The first join reports an empty
/// revoked set.
#[derive(Clone, Default)]
pub struct RebalanceListener(Option<Arc<RebalanceFn>>);

impl RebalanceListener {
    /// Wrap a callback.
    pub fn from_fn(f: impl Fn(&[(String, i32)], &[(String, i32)]) + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(f)))
    }

    pub(crate) fn call(&self, revoked: &[(String, i32)], assigned: &[(String, i32)]) {
        if let Some(f) = &self.0 {
            f(revoked, assigned);
        }
    }
}

impl fmt::Debug for RebalanceListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "RebalanceListener"
        } else {
            "None"
        })
    }
}

/// Fetch and group-member settings.
///
/// Prefer the chainable builders. Raw fields remain writable.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Bootstrap brokers, `host:port`.
    pub bootstrap: Vec<String>,
    /// Kafka `client.id`.
    pub client_id: String,
    /// Per-request timeout (fetch, metadata, offsets, group RPCs that use this config).
    pub request_timeout: Duration,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
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
    /// rustls. `None` is plain TCP.
    pub tls: Option<crate::net::TlsConfig>,
    /// `fetch.max.wait.ms`.
    pub max_wait_ms: i32,
    /// `fetch.min.bytes`.
    pub min_bytes: i32,
    /// `fetch.max.bytes` / `max.partition.fetch.bytes`. Default 16 MiB.
    pub max_bytes: i32,
    /// 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED.
    pub isolation_level: i8,
    /// Client rack for fetch-from-follower (KIP-392). Empty means leader only.
    pub rack: Option<String>,
    /// Kafka `group.instance.id`. Static membership for classic and KIP-848 groups.
    pub group_instance_id: Option<String>,
    /// Kafka `auto.offset.reset` for group members with no committed offset.
    pub auto_offset_reset: crate::AutoOffsetReset,
    /// Cap on records returned from one [`Consumer::fetch`] / [`crate::ConsumerGroup::poll`].
    ///
    /// `None` (the default) returns every record from the Fetch round.
    pub max_poll_records: Option<usize>,
    /// Kafka `session.timeout.ms` on classic JoinGroup. Default 10 seconds.
    pub session_timeout_ms: i32,
    /// How often the group member heartbeats. Default 150 ms (faster than Java's 3 s).
    pub heartbeat_interval: Duration,
    /// Optional `(revoked, assigned)` callback after a group assignment change.
    pub rebalance: RebalanceListener,
    /// Kafka `enable.auto.commit`. Off by default (Java defaults to on).
    pub enable_auto_commit: bool,
    /// Kafka `auto.commit.interval.ms`. Used when [`Self::enable_auto_commit`] is on.
    ///
    /// A zero interval commits after every [`crate::ConsumerGroup::poll`].
    pub auto_commit_interval: Duration,
    /// Kafka `max.poll.interval.ms`. Zero means no limit. Default 5 minutes.
    ///
    /// The next [`crate::ConsumerGroup::poll`] errors with
    /// [`crate::Error::MaxPollInterval`] if exceeded. The heartbeat thread
    /// also leaves the group (classic `LeaveGroup` or KIP-848 epoch `-1`).
    pub max_poll_interval: Duration,
    /// Fetch interceptors. Empty is a no-op.
    pub interceptors: crate::interceptor::ConsumerInterceptors,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap: vec!["127.0.0.1:9092".into()],
            client_id: "partitionline".into(),
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            sasl_plain: None,
            sasl_scram: None,
            sasl_scram_sha512: None,
            sasl_oauthbearer: None,
            sasl_oauthbearer_oidc: None,
            tls: None,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 16_777_216,
            isolation_level: 0,
            rack: None,
            group_instance_id: None,
            auto_offset_reset: crate::AutoOffsetReset::Earliest,
            max_poll_records: None,
            session_timeout_ms: 10_000,
            heartbeat_interval: Duration::from_millis(150),
            rebalance: RebalanceListener::default(),
            enable_auto_commit: false,
            auto_commit_interval: Duration::from_secs(5),
            max_poll_interval: Duration::from_secs(300),
            interceptors: crate::interceptor::ConsumerInterceptors::default(),
        }
    }
}

impl ConsumerConfig {
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

    /// `fetch.max.wait.ms`.
    #[must_use]
    pub fn max_wait_ms(mut self, ms: i32) -> Self {
        self.max_wait_ms = ms;
        self
    }

    /// `fetch.min.bytes`.
    #[must_use]
    pub fn min_bytes(mut self, n: i32) -> Self {
        self.min_bytes = n;
        self
    }

    /// Cap for the Fetch request and each partition (`fetch.max.bytes` /
    /// `max.partition.fetch.bytes`). Default 16 MiB.
    #[must_use]
    pub fn max_bytes(mut self, n: i32) -> Self {
        self.max_bytes = n;
        self
    }

    /// `isolation.level`.
    #[must_use]
    pub fn isolation(mut self, level: crate::IsolationLevel) -> Self {
        self.isolation_level = level.as_i8();
        self
    }

    /// `client.rack` for fetch-from-follower (KIP-392).
    #[must_use]
    pub fn rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = Some(rack.into());
        self
    }

    /// `group.instance.id` (static membership).
    #[must_use]
    pub fn group_instance_id(mut self, id: impl Into<String>) -> Self {
        self.group_instance_id = Some(id.into());
        self
    }

    /// `auto.offset.reset` when a group member has no committed offset.
    #[must_use]
    pub fn auto_offset_reset(mut self, reset: crate::AutoOffsetReset) -> Self {
        self.auto_offset_reset = reset;
        self
    }

    /// Kafka `max.poll.records`. `None` means no cap.
    #[must_use]
    pub fn max_poll_records(mut self, n: usize) -> Self {
        self.max_poll_records = Some(n);
        self
    }

    /// Kafka `session.timeout.ms` on classic JoinGroup.
    #[must_use]
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        let ms = timeout.as_millis();
        self.session_timeout_ms = i32::try_from(ms).unwrap_or(i32::MAX);
        self
    }

    /// How often this member heartbeats while in a group.
    #[must_use]
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Called as `(revoked, assigned)` after a group assignment change.
    #[must_use]
    pub fn on_rebalance(
        mut self,
        f: impl Fn(&[(String, i32)], &[(String, i32)]) + Send + Sync + 'static,
    ) -> Self {
        self.rebalance = RebalanceListener::from_fn(f);
        self
    }

    /// Kafka `enable.auto.commit`. Off by default.
    #[must_use]
    pub fn auto_commit(mut self, on: bool) -> Self {
        self.enable_auto_commit = on;
        self
    }

    /// Kafka `auto.commit.interval.ms`. Zero commits after every poll.
    #[must_use]
    pub fn auto_commit_interval(mut self, interval: Duration) -> Self {
        self.auto_commit_interval = interval;
        self
    }

    /// Kafka `max.poll.interval.ms`. Zero means no limit.
    #[must_use]
    pub fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.max_poll_interval = interval;
        self
    }

    /// Append a fetch interceptor.
    #[must_use]
    pub fn interceptor(mut self, i: impl crate::interceptor::ConsumerInterceptor) -> Self {
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
    pub fn tls(mut self, tls: crate::net::TlsConfig) -> Self {
        crate::config::apply_tls(&mut self.tls, tls);
        self
    }

    /// Per-request timeout.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

/// One record from Fetch.
#[derive(Debug, Clone)]
pub struct FetchedRecord {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// Optional key.
    pub key: Option<Bytes>,
    /// Optional value.
    pub value: Option<Bytes>,
    /// Record headers.
    pub headers: Vec<Header>,
}

/// A topic name and partition index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
}

impl TopicPartition {
    /// `topic` plus `partition`.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl From<(String, i32)> for TopicPartition {
    fn from((topic, partition): (String, i32)) -> Self {
        Self { topic, partition }
    }
}

impl From<(&str, i32)> for TopicPartition {
    fn from((topic, partition): (&str, i32)) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl From<TopicPartition> for (String, i32) {
    fn from(tp: TopicPartition) -> Self {
        (tp.topic, tp.partition)
    }
}

/// Offset plus the matching record timestamp from ListOffsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetAndTimestamp {
    /// Log offset, or `-1` when the broker has no match.
    pub offset: i64,
    /// Record timestamp in milliseconds since the Unix epoch, or `-1`.
    pub timestamp: i64,
}

/// One partition from Metadata: leader, replicas, and ISR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Leader broker id, or `-1` if unknown.
    pub leader: i32,
    /// Replica broker ids.
    pub replicas: Vec<i32>,
    /// In-sync replica broker ids.
    pub isr: Vec<i32>,
}

/// Manual-assignment fetch client.
pub struct Consumer {
    cfg: ConsumerConfig,
    conn: BrokerConn,
    versions: HashMap<i16, ApiVersion>,
    fetch_version: i16,
    metadata_version: i16,
    metadata: Option<MetadataResponse>,
    cluster: Cluster,
    conns: HashMap<i32, BrokerConn>,
    assigned: Vec<(String, i32, i64)>,
    preferred: HashMap<(String, i32), i32>,
    paused: HashSet<(String, i32)>,
    pending: VecDeque<FetchedRecord>,
    m_fetch_rounds: AtomicU64,
    m_records: AtomicU64,
    m_bytes: AtomicU64,
    m_errors: AtomicU64,
    wakeup: Arc<AtomicBool>,
    wakeup_tx: watch::Sender<bool>,
}

/// Thread-safe handle that interrupts [`Consumer::fetch`] / group `poll`.
///
/// [`Consumer`] is not `Sync`. Clone this handle onto another task for shutdown.
#[derive(Clone)]
pub struct WakeupHandle {
    flag: Arc<AtomicBool>,
    tx: watch::Sender<bool>,
}

impl WakeupHandle {
    /// Make the next (or in-flight) fetch return [`Error::Wakeup`].
    pub fn wakeup(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.tx.send(true).unwrap_or(());
    }
}

impl Consumer {
    /// Connect with default config to one bootstrap server.
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(ConsumerConfig::bootstrap([bootstrap.into()])).await
    }

    /// Connect using `cfg`. Negotiates ApiVersions and optional SASL/TLS.
    pub async fn new(cfg: ConsumerConfig) -> Result<Self> {
        if cfg.bootstrap.is_empty() {
            return Err(Error::protocol("no bootstrap servers"));
        }
        let mut conn = BrokerConn::connect_tls_any(
            &cfg.bootstrap,
            &cfg.client_id,
            cfg.connect_timeout,
            cfg.tls.as_ref(),
        )
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
        let mut versions = HashMap::new();
        for api in resp.api_keys {
            let _prev = versions.insert(api.api_key, api);
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
        let fetch_version = versions
            .get(&FETCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 4, 11))
            .ok_or_else(|| Error::Unsupported("broker does not support Fetch v4-11".into()))?;
        let metadata_version = versions
            .get(&METADATA)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 12))
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        Ok(Self {
            cfg,
            conn,
            versions,
            fetch_version,
            metadata_version,
            metadata: None,
            cluster: Cluster::default(),
            conns: HashMap::new(),
            assigned: Vec::new(),
            preferred: HashMap::new(),
            paused: HashSet::new(),
            pending: VecDeque::new(),
            m_fetch_rounds: AtomicU64::new(0),
            m_records: AtomicU64::new(0),
            m_bytes: AtomicU64::new(0),
            m_errors: AtomicU64::new(0),
            wakeup: Arc::new(AtomicBool::new(false)),
            wakeup_tx: watch::channel(false).0,
        })
    }

    /// Assign one partition at `offset`. Replaces a previous offset for the same pair.
    pub async fn assign(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let topic = topic.into();
        self.refresh_metadata(Some(std::slice::from_ref(&topic)))
            .await?;
        self.drop_pending_for(&topic, partition);
        self.assigned
            .retain(|(t, p, _)| !(t == &topic && *p == partition));
        self.assigned.push((topic, partition, offset));
        Ok(())
    }

    /// Assign every partition of `topic` at `offset` (from metadata).
    pub async fn assign_topic(&mut self, topic: impl Into<String>, offset: i64) -> Result<()> {
        let topic = topic.into();
        self.refresh_metadata(Some(std::slice::from_ref(&topic)))
            .await?;
        let (error_code, parts): (i16, Vec<i32>) = {
            let tmd = self
                .metadata
                .as_ref()
                .and_then(|md| {
                    md.topics
                        .iter()
                        .find(|t| t.name.as_deref() == Some(topic.as_str()))
                })
                .ok_or_else(|| Error::UnknownTopic(topic.clone()))?;
            (
                tmd.error_code,
                tmd.partitions.iter().map(|p| p.partition_index).collect(),
            )
        };
        if error_code != 0 {
            return Err(Error::broker(error_code, topic));
        }
        if parts.is_empty() {
            return Err(Error::UnknownTopic(topic));
        }
        self.assigned.retain(|(t, _, _)| t != &topic);
        for p in parts {
            self.drop_pending_for(&topic, p);
            self.assigned.push((topic.clone(), p, offset));
        }
        Ok(())
    }

    /// Assigned `(topic, partition, next_offset)` triples.
    pub fn assignment(&self) -> &[(String, i32, i64)] {
        &self.assigned
    }

    /// Next fetch offset for an assigned partition.
    pub fn position(&self, topic: &str, partition: i32) -> Result<i64> {
        self.assigned
            .iter()
            .find(|(t, p, _)| t == topic && *p == partition)
            .map(|(_, _, o)| *o)
            .ok_or_else(|| Error::protocol(format!("no position for {topic}-{partition}")))
    }

    /// Stop fetching these assigned partitions until [`resume`](Self::resume).
    ///
    /// Pause is stored on the consumer, so it survives group rebalance. Fetch
    /// skips a partition only while it is both assigned and paused. Records
    /// already buffered for a paused partition are held until resume.
    pub fn pause(&mut self, partitions: &[(String, i32)]) {
        self.paused.extend(partitions.iter().cloned());
    }

    /// Undo [`pause`](Self::pause) for these partitions.
    pub fn resume(&mut self, partitions: &[(String, i32)]) {
        for tp in partitions {
            let _removed = self.paused.remove(tp);
        }
    }

    /// Assigned partitions that [`fetch`](Self::fetch) currently skips.
    pub fn paused(&self) -> Vec<(String, i32)> {
        self.assigned
            .iter()
            .filter(|(t, p, _)| self.paused.contains(&(t.clone(), *p)))
            .map(|(t, p, _)| (t.clone(), *p))
            .collect()
    }

    pub(crate) fn clear_assignment(&mut self) {
        self.assigned.clear();
        self.pending.clear();
    }

    /// Replace the assignment. One Metadata refresh for the topic set.
    pub(crate) async fn assign_all(&mut self, starts: &[(String, i32, i64)]) -> Result<()> {
        self.clear_assignment();
        if starts.is_empty() {
            return Ok(());
        }
        let mut topics: Vec<String> = Vec::new();
        for (topic, _, _) in starts {
            if !topics.iter().any(|t| t == topic) {
                topics.push(topic.clone());
            }
        }
        self.refresh_metadata(Some(&topics)).await?;
        self.assigned.extend(starts.iter().cloned());
        self.retain_pending_assigned();
        Ok(())
    }

    pub(crate) async fn partition_ids(&mut self, topic: &str) -> Result<Vec<i32>> {
        let topics = [topic.to_string()];
        self.refresh_metadata(Some(&topics)).await?;
        let tmd = self
            .metadata
            .as_ref()
            .and_then(|md| md.topics.iter().find(|t| t.name.as_deref() == Some(topic)))
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
        if tmd.error_code != 0 {
            return Err(Error::broker(tmd.error_code, topic.to_string()));
        }
        Ok(tmd.partitions.iter().map(|p| p.partition_index).collect())
    }

    /// Set the next fetch offset without a ListOffsets call.
    pub fn advance(&mut self, topic: &str, partition: i32, next_offset: i64) {
        if let Some(slot) = self
            .assigned
            .iter_mut()
            .find(|(t, p, _)| t == topic && *p == partition)
        {
            slot.2 = next_offset;
        }
    }

    async fn reconnect_bootstrap(&mut self) -> Result<()> {
        self.conns.clear();
        let addr = self.conn.addr().to_string();
        let mut conn = BrokerConn::connect_tls(
            &addr,
            &self.cfg.client_id,
            self.cfg.connect_timeout,
            self.cfg.tls.as_ref(),
        )
        .await?;
        let body = conn
            .roundtrip(
                API_VERSIONS,
                3,
                |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
                self.cfg.request_timeout,
            )
            .await?;
        let resp = decode_api_versions_response(&mut body.clone(), 3)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ApiVersions"));
        }
        sasl::authenticate(
            &mut conn,
            self.cfg.sasl_plain.as_ref(),
            self.cfg.sasl_scram.as_ref(),
            self.cfg.sasl_scram_sha512.as_ref(),
            self.cfg.sasl_oauthbearer.as_deref(),
            self.cfg.sasl_oauthbearer_oidc.as_ref(),
            self.cfg.request_timeout,
        )
        .await?;
        self.conn = conn;
        Ok(())
    }

    async fn refresh_metadata(&mut self, topics: Option<&[String]>) -> Result<()> {
        let version = self.metadata_version;
        let timeout = self.cfg.request_timeout;
        let body = match self
            .conn
            .roundtrip(
                METADATA,
                version,
                |buf| encode_metadata_request(buf, version, topics, false),
                timeout,
            )
            .await
        {
            Ok(b) => b,
            Err(e) if e.is_retriable() => {
                self.reconnect_bootstrap().await?;
                self.conn
                    .roundtrip(
                        METADATA,
                        version,
                        |buf| encode_metadata_request(buf, version, topics, false),
                        timeout,
                    )
                    .await?
            }
            Err(e) => return Err(e),
        };
        let md = decode_metadata_response(&mut body.clone(), version)?;
        self.cluster.apply(&md);
        self.metadata = Some(md);
        Ok(())
    }

    pub(crate) async fn ensure_topic_metadata(&mut self, topic: &str) -> Result<()> {
        if self.cluster.partition_count(topic).is_some() {
            return Ok(());
        }
        self.refresh_topic_metadata(topic).await
    }

    pub(crate) async fn refresh_topics(&mut self, topics: &[String]) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        self.refresh_metadata(Some(topics)).await
    }

    pub(crate) async fn refresh_topic_metadata(&mut self, topic: &str) -> Result<()> {
        let topics = [topic.to_string()];
        self.refresh_metadata(Some(&topics)).await
    }

    pub(crate) fn topic_id_names(&self) -> HashMap<[u8; 16], String> {
        let mut out = HashMap::new();
        let Some(md) = &self.metadata else {
            return out;
        };
        for t in &md.topics {
            if t.topic_id == [0u8; 16] {
                continue;
            }
            if let Some(name) = &t.name {
                let _ = out.insert(t.topic_id, name.clone());
            }
        }
        out
    }

    pub(crate) fn leader_of(&self, topic: &str, partition: i32) -> Result<(i32, String)> {
        self.cluster.leader(topic, partition)
    }

    pub(crate) fn invalidate_topic(&mut self, topic: &str) {
        self.cluster.invalidate_topic(topic);
    }

    pub(crate) fn drop_node(&mut self, node: i32) {
        let _ = self.conns.remove(&node);
    }

    pub(crate) async fn roundtrip_node(
        &mut self,
        node: i32,
        api_key: i16,
        api_version: i16,
        encode_body: impl Fn(&mut bytes::BytesMut) -> Result<()>,
        timeout: Duration,
    ) -> Result<Bytes> {
        self.connect_node(node).await?;
        let conn = self
            .conns
            .get_mut(&node)
            .ok_or_else(|| Error::protocol("missing node conn"))?;
        conn.roundtrip(api_key, api_version, encode_body, timeout)
            .await
    }

    async fn connect_node(&mut self, node: i32) -> Result<()> {
        if self.conns.contains_key(&node) {
            return Ok(());
        }
        let addr = self
            .cluster
            .brokers
            .get(&node)
            .cloned()
            .ok_or_else(|| Error::protocol(format!("unknown broker {node}")))?;
        let mut conn = BrokerConn::connect_tls(
            &addr,
            &self.cfg.client_id,
            self.cfg.connect_timeout,
            self.cfg.tls.as_ref(),
        )
        .await?;
        let _versions = conn
            .roundtrip(
                API_VERSIONS,
                3,
                |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
                self.cfg.request_timeout,
            )
            .await?;
        sasl::authenticate(
            &mut conn,
            self.cfg.sasl_plain.as_ref(),
            self.cfg.sasl_scram.as_ref(),
            self.cfg.sasl_scram_sha512.as_ref(),
            self.cfg.sasl_oauthbearer.as_deref(),
            self.cfg.sasl_oauthbearer_oidc.as_ref(),
            self.cfg.request_timeout,
        )
        .await?;
        let _prev = self.conns.insert(node, conn);
        Ok(())
    }

    async fn recover_leader_epoch(&mut self, topic: &str, partition: i32) -> Result<()> {
        let version = self
            .versions
            .get(&OFFSET_FOR_LEADER_EPOCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support OffsetForLeaderEpoch".into())
            })?;
        // Preferred replica may have returned the fence; OffsetForLeaderEpoch is leader-only.
        // Refresh Metadata first so `current_leader_epoch` is not the value that just fenced us.
        let _ = self.preferred.remove(&(topic.to_string(), partition));
        let deadline = Instant::now() + self.cfg.request_timeout;
        {
            let topics = [topic.to_string()];
            self.refresh_metadata(Some(&topics)).await?;
        }
        loop {
            if self.cluster.leader(topic, partition).is_err() {
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(topic, partition)?;
            self.connect_node(node).await?;
            let current = self.cluster.leader_epoch(topic, partition);
            let timeout = self.cfg.request_timeout;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing epoch conn"))?;
                conn.roundtrip(
                    OFFSET_FOR_LEADER_EPOCH,
                    version,
                    |buf| {
                        encode_offset_for_leader_epoch_request(
                            buf, version, topic, partition, current, current,
                        )
                    },
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (err, epoch, end_offset) =
                decode_offset_for_leader_epoch_response(&mut body.clone(), version)?;
            if err == 0 {
                self.cluster.set_leader_epoch(topic, partition, epoch);
                let assigned = self
                    .assigned
                    .iter()
                    .find(|(t, p, _)| t == topic && *p == partition)
                    .map(|(_, _, o)| *o);
                if let Some(off) = assigned {
                    if off > end_offset {
                        self.advance(topic, partition, end_offset);
                    }
                }
                return Ok(());
            }
            let e = Error::broker(err, format!("OffsetForLeaderEpoch {topic}-{partition}"));
            let fence = err == error::FENCED_LEADER_EPOCH || err == error::UNKNOWN_LEADER_EPOCH;
            if e.is_retriable() || fence {
                // NOT_LEADER_OR_FOLLOWER (6) / fence: Metadata, then the new leader/epoch.
                self.cluster.invalidate_topic(topic);
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Err(e);
        }
    }

    /// Fetch one round from every assigned partition that is not paused.
    ///
    /// Empty if nothing is assigned, or every assigned partition is paused.
    /// Partitions that share a leader go in one Fetch. Distinct leaders are
    /// fetched at the same time.
    ///
    /// When [`ConsumerConfig::max_poll_records`] is set, extra records from
    /// the Fetch stay buffered and are returned on the next call.
    pub async fn fetch(&mut self) -> Result<Vec<FetchedRecord>> {
        if self.take_wakeup() {
            return Err(Error::Wakeup);
        }
        let result = self.fetch_assigned().await;
        match result {
            Ok(recs) => {
                let _ = self.m_fetch_rounds.fetch_add(1, Ordering::Relaxed);
                let n = u64::try_from(recs.len()).unwrap_or(u64::MAX);
                let _ = self.m_records.fetch_add(n, Ordering::Relaxed);
                let bytes: u64 = recs.iter().map(fetched_bytes).fold(0, u64::saturating_add);
                let _ = self.m_bytes.fetch_add(bytes, Ordering::Relaxed);
                Ok(self.cfg.interceptors.on_consume(recs))
            }
            Err(Error::Wakeup) => {
                let _ = self.take_wakeup();
                Err(Error::Wakeup)
            }
            Err(e) => {
                let _ = self.m_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Fetch counters since connect.
    #[must_use]
    pub fn metrics(&self) -> crate::ConsumerMetrics {
        crate::ConsumerMetrics {
            fetch_rounds: self.m_fetch_rounds.load(Ordering::Relaxed),
            records_fetched: self.m_records.load(Ordering::Relaxed),
            bytes_fetched: self.m_bytes.load(Ordering::Relaxed),
            fetch_errors: self.m_errors.load(Ordering::Relaxed),
        }
    }

    /// Interrupt [`Self::fetch`] (and group `poll` that calls it).
    ///
    /// Safe to call while fetch is running on this task. From another task,
    /// use [`Self::wakeup_handle`].
    pub fn wakeup(&self) {
        self.wakeup.store(true, Ordering::SeqCst);
        self.wakeup_tx.send(true).unwrap_or(());
    }

    /// Cloneable handle for [`Self::wakeup`] from another task.
    #[must_use]
    pub fn wakeup_handle(&self) -> WakeupHandle {
        WakeupHandle {
            flag: Arc::clone(&self.wakeup),
            tx: self.wakeup_tx.clone(),
        }
    }

    /// Drop fetch connections. The consumer is then gone (same as `Producer::close`).
    pub async fn close(mut self) -> Result<()> {
        self.conns.clear();
        Ok(())
    }

    pub(crate) fn take_wakeup(&self) -> bool {
        let was = self.wakeup.swap(false, Ordering::SeqCst);
        if was {
            self.wakeup_tx.send(false).unwrap_or(());
        }
        was
    }

    fn woken(&self) -> bool {
        self.wakeup.load(Ordering::SeqCst)
    }

    async fn fetch_assigned(&mut self) -> Result<Vec<FetchedRecord>> {
        if let Some(ready) = self.take_ready() {
            return Ok(ready);
        }
        if self.assigned.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + self.cfg.request_timeout;
        loop {
            if self.woken() {
                return Err(Error::Wakeup);
            }
            if self.cluster.leaders.is_empty() {
                let topics: Vec<String> = self.assigned.iter().map(|(t, _, _)| t.clone()).collect();
                self.refresh_metadata(Some(&topics)).await?;
            }
            let mut by_leader: HashMap<i32, HashMap<String, Vec<FetchPartition>>> = HashMap::new();
            let mut missing_leader = false;
            for (topic, part, offset) in &self.assigned {
                if self.paused.contains(&(topic.clone(), *part)) {
                    continue;
                }
                let node = if self.cfg.rack.is_some() {
                    self.preferred.get(&(topic.clone(), *part)).copied()
                } else {
                    None
                };
                let node = match node {
                    Some(n) => Some(n),
                    None => self.cluster.leader(topic, *part).ok().map(|(n, _)| n),
                };
                match node {
                    Some(node) => {
                        by_leader
                            .entry(node)
                            .or_default()
                            .entry(topic.clone())
                            .or_default()
                            .push(FetchPartition {
                                partition: *part,
                                current_leader_epoch: self.cluster.leader_epoch(topic, *part),
                                fetch_offset: *offset,
                                partition_max_bytes: self.cfg.max_bytes,
                            });
                    }
                    None => {
                        missing_leader = true;
                        break;
                    }
                }
            }
            if missing_leader {
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                for (t, _, _) in &self.assigned {
                    self.cluster.invalidate_topic(t);
                }
                let topics: Vec<String> = self.assigned.iter().map(|(t, _, _)| t.clone()).collect();
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            let bodies = self.fetch_from_leaders(by_leader).await?;
            let mut out = Vec::new();
            let mut retry = false;
            for (node, body) in bodies {
                let mut body = match body {
                    Ok(b) => b,
                    Err(e) if e.is_retriable() => {
                        let _ = self.conns.remove(&node);
                        retry = true;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                if self.apply_fetch_body(node, &mut body, &mut out).await? {
                    retry = true;
                }
            }
            if retry {
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                let topics: Vec<String> = self.assigned.iter().map(|(t, _, _)| t.clone()).collect();
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Ok(self.finish_fetch(out));
        }
    }

    async fn fetch_from_leaders(
        &mut self,
        by_leader: HashMap<i32, HashMap<String, Vec<FetchPartition>>>,
    ) -> Result<Vec<(i32, Result<Bytes>)>> {
        if self.woken() {
            self.conns.clear();
            return Err(Error::Wakeup);
        }
        let mut rx = self.wakeup_tx.subscribe();
        tokio::select! {
            biased;
            result = rx.wait_for(|on| *on) => {
                drop(result);
                self.conns.clear();
                Err(Error::Wakeup)
            }
            result = self.fetch_from_leaders_io(by_leader) => {
                result
            }
        }
    }

    async fn fetch_from_leaders_io(
        &mut self,
        mut by_leader: HashMap<i32, HashMap<String, Vec<FetchPartition>>>,
    ) -> Result<Vec<(i32, Result<Bytes>)>> {
        let mut nodes: Vec<i32> = by_leader.keys().copied().collect();
        nodes.sort_unstable();
        for node in &nodes {
            self.connect_node(*node).await?;
        }
        let max_wait = self.cfg.max_wait_ms;
        let min_bytes = self.cfg.min_bytes;
        let max_bytes = self.cfg.max_bytes;
        let isolation_level = self.cfg.isolation_level;
        let timeout = self.cfg.request_timeout;
        let fetch_version = self.fetch_version;
        let rack = self.cfg.rack.clone();
        if nodes.len() <= 1 {
            let mut out = Vec::with_capacity(nodes.len());
            for node in nodes {
                let Some(by_topic) = by_leader.remove(&node) else {
                    continue;
                };
                let topics = fetch_topics(by_topic);
                let body = {
                    let conn = self
                        .conns
                        .get_mut(&node)
                        .ok_or_else(|| Error::protocol("missing fetch conn"))?;
                    conn.roundtrip(
                        FETCH,
                        fetch_version,
                        |buf| {
                            encode_fetch_request(
                                buf,
                                max_wait,
                                min_bytes,
                                max_bytes,
                                isolation_level,
                                &topics,
                                rack.as_deref(),
                            )
                        },
                        timeout,
                    )
                    .await
                };
                out.push((node, body));
            }
            return Ok(out);
        }
        let mut set = tokio::task::JoinSet::new();
        for node in nodes {
            let Some(by_topic) = by_leader.remove(&node) else {
                continue;
            };
            let topics = fetch_topics(by_topic);
            let mut conn = self
                .conns
                .remove(&node)
                .ok_or_else(|| Error::protocol("missing fetch conn"))?;
            let rack = rack.clone();
            let _ = set.spawn(async move {
                let result = conn
                    .roundtrip(
                        FETCH,
                        fetch_version,
                        |buf| {
                            encode_fetch_request(
                                buf,
                                max_wait,
                                min_bytes,
                                max_bytes,
                                isolation_level,
                                &topics,
                                rack.as_deref(),
                            )
                        },
                        timeout,
                    )
                    .await;
                (node, conn, result)
            });
        }
        let mut out = Vec::new();
        while let Some(joined) = set.join_next().await {
            let (node, conn, result) =
                joined.map_err(|e| Error::protocol(format!("fetch task: {e}")))?;
            let _ = self.conns.insert(node, conn);
            out.push((node, result));
        }
        out.sort_by_key(|(n, _)| *n);
        Ok(out)
    }

    async fn apply_fetch_body(
        &mut self,
        node: i32,
        body: &mut Bytes,
        out: &mut Vec<FetchedRecord>,
    ) -> Result<bool> {
        let fetched = decode_fetch_response(body)?;
        let mut retry = false;
        for topic in fetched {
            for part in topic.partitions {
                if part.preferred_read_replica >= 0
                    && self.cfg.rack.is_some()
                    && part.preferred_read_replica != node
                {
                    let _prev = self.preferred.insert(
                        (topic.topic.clone(), part.partition),
                        part.preferred_read_replica,
                    );
                    retry = true;
                    continue;
                }
                if part.error_code == error::OFFSET_OUT_OF_RANGE {
                    self.advance(&topic.topic, part.partition, part.log_start_offset);
                    continue;
                }
                if part.error_code == error::FENCED_LEADER_EPOCH
                    || part.error_code == error::UNKNOWN_LEADER_EPOCH
                {
                    self.recover_leader_epoch(&topic.topic, part.partition)
                        .await?;
                    retry = true;
                    continue;
                }
                if part.error_code != 0 {
                    let e = Error::broker(
                        part.error_code,
                        format!("{}-{}", topic.topic, part.partition),
                    );
                    if e.is_retriable() {
                        self.cluster.invalidate_topic(&topic.topic);
                        let _ = self.conns.remove(&node);
                        retry = true;
                        continue;
                    }
                    return Err(e);
                }
                let mut next = None;
                let isolation = self.cfg.isolation_level;
                for batch in part.records {
                    if batch.attributes & crate::protocol::records::ATTR_CONTROL != 0 {
                        if let Some(last) = batch.records.last() {
                            next = Some(last.offset + 1);
                        }
                        continue;
                    }
                    for rec in batch.records {
                        let offset = rec.offset;
                        if isolation == 1 && offset >= part.last_stable_offset {
                            break;
                        }
                        next = Some(offset + 1);
                        if isolation == 1 {
                            let aborted = part
                                .aborted_transactions
                                .iter()
                                .any(|(pid, first)| batch.producer_id == *pid && offset >= *first);
                            if aborted {
                                continue;
                            }
                        }
                        out.push(FetchedRecord {
                            topic: topic.topic.clone(),
                            partition: part.partition,
                            offset,
                            timestamp: rec.timestamp,
                            key: rec.key,
                            value: rec.value,
                            headers: rec.headers,
                        });
                    }
                }
                if let Some(n) = next {
                    self.advance(&topic.topic, part.partition, n);
                }
            }
        }
        Ok(retry)
    }

    /// Negotiated ApiVersions for this connection.
    pub fn versions(&self) -> &HashMap<i16, ApiVersion> {
        &self.versions
    }

    #[expect(
        dead_code,
        reason = "callers that already hold a Consumer use this to hop FindCoordinator"
    )]
    pub(crate) fn conn_mut(&mut self) -> &mut BrokerConn {
        &mut self.conn
    }

    /// ListOffsets timestamp: `EARLIEST_TIMESTAMP` (-2), `LATEST_TIMESTAMP` (-1), or ms.
    pub async fn list_offsets(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64> {
        let topic = topic.into();
        Ok(self.list_offset_at(&topic, partition, timestamp).await?.0)
    }

    async fn list_offset_at(
        &mut self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<(i64, i64)> {
        let deadline = Instant::now() + self.cfg.request_timeout;
        loop {
            if self.cluster.leader(topic, partition).is_err() {
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(topic, partition)?;
            self.connect_node(node).await?;
            let version = self
                .versions
                .get(&LIST_OFFSETS)
                .and_then(|v| pick_version(v.min_version, v.max_version, 1, 5))
                .ok_or_else(|| Error::Unsupported("broker does not support ListOffsets".into()))?;
            let isolation = self.cfg.isolation_level;
            let timeout = self.cfg.request_timeout;
            let current_leader_epoch = self.cluster.leader_epoch(topic, partition);
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing list_offsets conn"))?;
                conn.roundtrip(
                    LIST_OFFSETS,
                    version,
                    |buf| {
                        encode_list_offsets_request(
                            buf,
                            version,
                            isolation,
                            topic,
                            partition,
                            current_leader_epoch,
                            timestamp,
                        )
                    },
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            match decode_list_offsets_response(&mut body.clone(), version) {
                Ok((_err, ts, offset)) => return Ok((offset, ts)),
                Err(e)
                    if matches!(
                        &e,
                        Error::Broker {
                            code: error::FENCED_LEADER_EPOCH | error::UNKNOWN_LEADER_EPOCH,
                            ..
                        }
                    ) =>
                {
                    self.recover_leader_epoch(topic, partition).await?;
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) if e.is_retriable() => {
                    // NOT_LEADER_OR_FOLLOWER (6) and friends: Metadata, then the new leader.
                    self.cluster.invalidate_topic(topic);
                    let _ = self.conns.remove(&node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    let topics = [topic.to_string()];
                    self.refresh_metadata(Some(&topics)).await?;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Set the next fetch offset for an assigned partition.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        if let Some(slot) = self
            .assigned
            .iter_mut()
            .find(|(t, p, _)| t == topic && *p == partition)
        {
            slot.2 = offset;
            self.drop_pending_for(topic, partition);
            return Ok(());
        }
        Err(Error::protocol(format!(
            "seek of unassigned {topic}-{partition}"
        )))
    }

    /// Seek every assigned partition to the log start (`ListOffsets` earliest).
    pub async fn seek_to_beginning(&mut self) -> Result<()> {
        self.seek_assigned(crate::EARLIEST_TIMESTAMP).await
    }

    /// Seek every assigned partition to the high watermark (`ListOffsets` latest).
    pub async fn seek_to_end(&mut self) -> Result<()> {
        self.seek_assigned(crate::LATEST_TIMESTAMP).await
    }

    async fn seek_assigned(&mut self, timestamp: i64) -> Result<()> {
        let assigned: Vec<(String, i32)> = self
            .assigned
            .iter()
            .map(|(t, p, _)| (t.clone(), *p))
            .collect();
        for (topic, partition) in assigned {
            let offset = self
                .list_offsets(topic.clone(), partition, timestamp)
                .await?;
            self.seek(&topic, partition, offset)?;
        }
        Ok(())
    }

    /// Partition metadata for `topic` (leader, replicas, ISR).
    pub async fn partitions_for(&mut self, topic: impl Into<String>) -> Result<Vec<PartitionInfo>> {
        let topic = topic.into();
        self.refresh_metadata(Some(std::slice::from_ref(&topic)))
            .await?;
        let tmd = self
            .metadata
            .as_ref()
            .and_then(|md| {
                md.topics
                    .iter()
                    .find(|t| t.name.as_deref() == Some(topic.as_str()))
            })
            .ok_or_else(|| Error::UnknownTopic(topic.clone()))?;
        if tmd.error_code != 0 {
            return Err(Error::broker(tmd.error_code, topic));
        }
        Ok(tmd
            .partitions
            .iter()
            .map(|p| PartitionInfo {
                topic: topic.clone(),
                partition: p.partition_index,
                leader: p.leader_id,
                replicas: p.replica_nodes.clone(),
                isr: p.isr_nodes.clone(),
            })
            .collect())
    }

    /// Log-start offset for each partition (`ListOffsets` earliest).
    pub async fn beginning_offsets(
        &mut self,
        partitions: &[(String, i32)],
    ) -> Result<Vec<(String, i32, i64)>> {
        self.offsets_at(partitions, crate::EARLIEST_TIMESTAMP).await
    }

    /// High-watermark offset for each partition (`ListOffsets` latest).
    pub async fn end_offsets(
        &mut self,
        partitions: &[(String, i32)],
    ) -> Result<Vec<(String, i32, i64)>> {
        self.offsets_at(partitions, crate::LATEST_TIMESTAMP).await
    }

    /// First offset at or after each timestamp (Java `offsetsForTimes`).
    ///
    /// Partitions with no matching record return `None`.
    pub async fn offsets_for_times(
        &mut self,
        queries: &[(TopicPartition, i64)],
    ) -> Result<Vec<(TopicPartition, Option<OffsetAndTimestamp>)>> {
        let mut out = Vec::with_capacity(queries.len());
        for (tp, timestamp) in queries {
            let (offset, ts) = self
                .list_offset_at(&tp.topic, tp.partition, *timestamp)
                .await?;
            let found = if offset < 0 {
                None
            } else {
                Some(OffsetAndTimestamp {
                    offset,
                    timestamp: ts,
                })
            };
            out.push((tp.clone(), found));
        }
        Ok(out)
    }

    async fn offsets_at(
        &mut self,
        partitions: &[(String, i32)],
        timestamp: i64,
    ) -> Result<Vec<(String, i32, i64)>> {
        let mut out = Vec::with_capacity(partitions.len());
        for (topic, partition) in partitions {
            let offset = self
                .list_offsets(topic.clone(), *partition, timestamp)
                .await?;
            out.push((topic.clone(), *partition, offset));
        }
        Ok(out)
    }

    fn drop_pending_for(&mut self, topic: &str, partition: i32) {
        self.pending
            .retain(|r| !(r.topic == topic && r.partition == partition));
    }

    fn retain_pending_assigned(&mut self) {
        let assigned: HashSet<(String, i32)> = self
            .assigned
            .iter()
            .map(|(t, p, _)| (t.clone(), *p))
            .collect();
        self.pending
            .retain(|r| assigned.contains(&(r.topic.clone(), r.partition)));
    }

    fn take_ready(&mut self) -> Option<Vec<FetchedRecord>> {
        if self.pending.is_empty() {
            return None;
        }
        let drained = self.drain_pending();
        if drained.is_empty() {
            None
        } else {
            Some(drained)
        }
    }

    fn finish_fetch(&mut self, recs: Vec<FetchedRecord>) -> Vec<FetchedRecord> {
        if self.pending.is_empty() && self.cfg.max_poll_records.is_none() && self.paused.is_empty()
        {
            return recs;
        }
        self.pending.extend(recs);
        self.drain_pending()
    }

    fn drain_pending(&mut self) -> Vec<FetchedRecord> {
        let cap = self
            .cfg
            .max_poll_records
            .filter(|n| *n > 0)
            .unwrap_or(usize::MAX);
        let mut out = Vec::new();
        let mut kept = VecDeque::new();
        while let Some(rec) = self.pending.pop_front() {
            if self.paused.contains(&(rec.topic.clone(), rec.partition)) || out.len() >= cap {
                kept.push_back(rec);
                continue;
            }
            out.push(rec);
        }
        self.pending = kept;
        out
    }
}

fn fetch_topics(by_topic: HashMap<String, Vec<FetchPartition>>) -> Vec<FetchTopic> {
    by_topic
        .into_iter()
        .map(|(topic, partitions)| FetchTopic { topic, partitions })
        .collect()
}

fn fetched_bytes(rec: &FetchedRecord) -> u64 {
    let k = rec.key.as_ref().map(Bytes::len).unwrap_or(0);
    let v = rec.value.as_ref().map(Bytes::len).unwrap_or(0);
    u64::try_from(k.saturating_add(v)).unwrap_or(u64::MAX)
}
