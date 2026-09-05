//! Fetch client with manual partition assignment.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::watch;

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{
    decode_metadata_response, encode_metadata_request, ApiVersion, MetadataResponse,
    PartitionMetadata,
};
use crate::protocol::api_keys::{
    pick_version, FETCH, GET_TELEMETRY_SUBSCRIPTIONS, LIST_OFFSETS, METADATA,
    OFFSET_FOR_LEADER_EPOCH,
};
use crate::protocol::epoch::{
    decode_offset_for_leader_epoch_topics_response, encode_offset_for_leader_epoch_topics_request,
    OffsetForLeaderPartition, OffsetForLeaderTopic, OffsetForLeaderTopicResult,
};
use crate::protocol::fetch::{
    decode_fetch_response, encode_fetch_request, FetchPartition, FetchTopic,
    INVALID_LOG_START_OFFSET,
};
use crate::protocol::group::Topic;
use crate::protocol::offsets::{decode_list_offsets_response, encode_list_offsets_request};
use crate::protocol::records::{
    write_java_optional, write_java_optional_bytes, write_java_record_headers, Header,
    TimestampType,
};
use crate::protocol::sasl;

type RebalanceFn = dyn Fn(&[TopicPartition], &[TopicPartition]) + Send + Sync;

/// Called as `(revoked, assigned)` after a consumer-group assignment change.
///
/// Set with [`ConsumerConfig::on_rebalance`]. The first join reports an empty
/// revoked set.
#[derive(Clone, Default)]
pub struct RebalanceListener(Option<Arc<RebalanceFn>>);

impl RebalanceListener {
    /// Wrap a callback.
    pub fn from_fn(
        f: impl Fn(&[TopicPartition], &[TopicPartition]) + Send + Sync + 'static,
    ) -> Self {
        Self(Some(Arc::new(f)))
    }

    pub(crate) fn call(&self, revoked: &[TopicPartition], assigned: &[TopicPartition]) {
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
#[derive(Clone)]
pub struct ConsumerConfig {
    /// Bootstrap brokers, `host:port`.
    pub bootstrap: Vec<String>,
    /// Kafka `client.id`.
    pub client_id: String,
    /// Per-request timeout (fetch, metadata, offsets, group and share RPCs).
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
    /// Kafka `fetch.max.bytes`. Default 16 MiB. [`Self::max_bytes()`] sets
    /// this and [`Self::max_partition_fetch_bytes`] together.
    pub max_bytes: i32,
    /// Kafka `max.partition.fetch.bytes`. Default 16 MiB (Java defaults to
    /// 1 MiB). Independent of [`Self::max_bytes`] unless set via
    /// [`Self::max_bytes()`].
    pub max_partition_fetch_bytes: i32,
    /// Kafka `isolation.level`.
    pub isolation_level: crate::IsolationLevel,
    /// Kafka `client.rack`. Fetch-from-follower (KIP-392) and group heartbeats
    /// (ConsumerGroupHeartbeat / ShareGroupHeartbeat RackId). Empty means leader only.
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
    /// Classic JoinGroup `RebalanceTimeoutMs` is this interval (Java
    /// `ClassicKafkaConsumer` / `max.poll.interval.ms`). KIP-848
    /// ConsumerGroupHeartbeat join sends the same value; later heartbeats
    /// send [`crate::protocol::cgheartbeat::ConsumerGroupHeartbeatRequest::UNCHANGED_REBALANCE_TIMEOUT_MS`].
    /// Zero sends [`i32::MAX`]. The next [`crate::ConsumerGroup::poll`] errors with
    /// [`crate::Error::MaxPollInterval`] if exceeded. The heartbeat thread
    /// also leaves the group (classic `LeaveGroup` or KIP-848 epoch `-1`).
    pub max_poll_interval: Duration,
    /// Kafka `retry.backoff.ms`. Wait after a retriable Fetch error before
    /// the next attempt in the same [`Consumer::fetch`]. Default 100ms.
    /// Zero retries immediately. Preferred-replica redirects do not wait.
    /// Grows as `base * 2^n` up to [`Self::retry_backoff_max`].
    pub retry_backoff: Duration,
    /// Kafka `retry.backoff.max.ms`. Cap on [`Self::retry_backoff`]
    /// exponential growth. Default 1s.
    pub retry_backoff_max: Duration,
    /// Kafka `metadata.max.age.ms`. Refresh cached Metadata after this age.
    /// Default 5 minutes (Java). Zero refreshes on every lookup.
    pub metadata_max_age: Duration,
    /// Kafka `reconnect.backoff.ms`. Wait after a failed TCP/TLS/SASL
    /// connect to a broker before the next attempt. Default 50ms (Java).
    /// Zero retries immediately. Grows as `base * 2^n` up to
    /// [`Self::reconnect_backoff_max`]. Distinct from [`Self::retry_backoff`]
    /// (Fetch RPC retries).
    pub reconnect_backoff: Duration,
    /// Kafka `reconnect.backoff.max.ms`. Cap on [`Self::reconnect_backoff`]
    /// exponential growth. Default 1s (Java).
    pub reconnect_backoff_max: Duration,
    /// Kafka `connections.max.idle.ms`. Close a broker TCP connection that
    /// has been unused for this long and reconnect on the next RPC. Default
    /// 9 minutes (Java). Zero never closes for idle.
    pub connections_max_idle: Duration,
    /// Kafka `allow.auto.create.topics` on Metadata. Default `false` (Java
    /// consumer defaults to `true`). When `true`, a Metadata request for a
    /// missing topic may create it on brokers that allow auto-create.
    pub allow_auto_topic_creation: bool,
    /// Fetch interceptors. Empty is a no-op.
    pub interceptors: crate::interceptor::ConsumerInterceptors,
}

impl fmt::Debug for ConsumerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerConfig")
            .field("bootstrap", &self.bootstrap)
            .field("client_id", &self.client_id)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field(
                "sasl_plain",
                &crate::config::RedactedUserPass(&self.sasl_plain),
            )
            .field(
                "sasl_scram",
                &crate::config::RedactedUserPass(&self.sasl_scram),
            )
            .field(
                "sasl_scram_sha512",
                &crate::config::RedactedUserPass(&self.sasl_scram_sha512),
            )
            .field("sasl_oauthbearer", &self.sasl_oauthbearer)
            .field("sasl_oauthbearer_oidc", &self.sasl_oauthbearer_oidc)
            .field("tls", &self.tls)
            .field("max_wait_ms", &self.max_wait_ms)
            .field("min_bytes", &self.min_bytes)
            .field("max_bytes", &self.max_bytes)
            .field("max_partition_fetch_bytes", &self.max_partition_fetch_bytes)
            .field("isolation_level", &self.isolation_level)
            .field("rack", &self.rack)
            .field("group_instance_id", &self.group_instance_id)
            .field("auto_offset_reset", &self.auto_offset_reset)
            .field("max_poll_records", &self.max_poll_records)
            .field("session_timeout_ms", &self.session_timeout_ms)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("rebalance", &self.rebalance)
            .field("enable_auto_commit", &self.enable_auto_commit)
            .field("auto_commit_interval", &self.auto_commit_interval)
            .field("max_poll_interval", &self.max_poll_interval)
            .field("retry_backoff", &self.retry_backoff)
            .field("retry_backoff_max", &self.retry_backoff_max)
            .field("metadata_max_age", &self.metadata_max_age)
            .field("reconnect_backoff", &self.reconnect_backoff)
            .field("reconnect_backoff_max", &self.reconnect_backoff_max)
            .field("connections_max_idle", &self.connections_max_idle)
            .field("allow_auto_topic_creation", &self.allow_auto_topic_creation)
            .field("interceptors", &self.interceptors)
            .finish()
    }
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
            max_partition_fetch_bytes: 16_777_216,
            isolation_level: crate::IsolationLevel::ReadUncommitted,
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
            retry_backoff: crate::config::DEFAULT_RETRY_BACKOFF,
            retry_backoff_max: crate::config::DEFAULT_RETRY_BACKOFF_MAX,
            metadata_max_age: Duration::from_secs(300),
            reconnect_backoff: crate::config::DEFAULT_RECONNECT_BACKOFF,
            reconnect_backoff_max: crate::config::DEFAULT_RECONNECT_BACKOFF_MAX,
            connections_max_idle: crate::config::DEFAULT_CONNECTIONS_MAX_IDLE,
            allow_auto_topic_creation: false,
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

    /// Kafka `fetch.max.bytes` and `max.partition.fetch.bytes` (same value).
    ///
    /// Default 16 MiB for both. Java defaults are 50 MiB / 1 MiB. For
    /// independent caps, use [`Self::fetch_max_bytes`] and
    /// [`Self::max_partition_fetch_bytes`].
    #[must_use]
    pub fn max_bytes(mut self, n: i32) -> Self {
        self.max_bytes = n;
        self.max_partition_fetch_bytes = n;
        self
    }

    /// Kafka `fetch.max.bytes` only (request-level Fetch cap).
    #[must_use]
    pub fn fetch_max_bytes(mut self, n: i32) -> Self {
        self.max_bytes = n;
        self
    }

    /// Kafka `max.partition.fetch.bytes` only (per-partition Fetch cap).
    #[must_use]
    pub fn max_partition_fetch_bytes(mut self, n: i32) -> Self {
        self.max_partition_fetch_bytes = n;
        self
    }

    /// `isolation.level`.
    #[must_use]
    pub fn isolation(mut self, level: crate::IsolationLevel) -> Self {
        self.isolation_level = level;
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
        f: impl Fn(&[TopicPartition], &[TopicPartition]) + Send + Sync + 'static,
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

    /// Kafka `retry.backoff.ms`. Wait after a retriable Fetch before retrying.
    ///
    /// Default 100ms. Zero retries immediately. Preferred-replica redirects
    /// (KIP-392) do not wait. Combined with [`Self::retry_backoff_max`] this
    /// is exponential (`base * 2^n`), no jitter.
    #[must_use]
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }

    /// Kafka `retry.backoff.max.ms`. Cap on exponential fetch retry waits.
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

    /// Kafka `reconnect.backoff.ms`. Wait after a failed broker connect.
    ///
    /// Default 50ms (Java). Zero retries immediately. Combined with
    /// [`Self::reconnect_backoff_max`] this is exponential (`base * 2^n`),
    /// no jitter. Preferred-replica redirects and Fetch RPC retries still
    /// use [`Self::retry_backoff`].
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

    /// Kafka `connections.max.idle.ms`. Close unused broker TCP connections.
    ///
    /// Default 9 minutes (Java). Zero never closes for idle. The next Fetch
    /// reconnects.
    #[must_use]
    pub fn connections_max_idle(mut self, idle: Duration) -> Self {
        self.connections_max_idle = idle;
        self
    }

    /// Kafka `allow.auto.create.topics` on Metadata.
    ///
    /// Default `false` (this crate; Java consumer defaults to `true`).
    #[must_use]
    pub fn allow_auto_create_topics(mut self, allow: bool) -> Self {
        self.allow_auto_topic_creation = allow;
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

    /// TCP connect timeout.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
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
    /// Java `ConsumerRecord.timestampType`.
    pub timestamp_type: TimestampType,
    /// Optional key.
    pub key: Option<Bytes>,
    /// Optional value.
    pub value: Option<Bytes>,
    /// Record headers.
    pub headers: Vec<Header>,
    /// Partition leader epoch from the record batch (Java `leaderEpoch`).
    ///
    /// `None` when the wire value is [`crate::RecordBatch::NO_PARTITION_LEADER_EPOCH`]
    /// or otherwise negative.
    pub leader_epoch: Option<i32>,
}

impl FetchedRecord {
    /// Java `ConsumerRecord.NO_TIMESTAMP`.
    pub const NO_TIMESTAMP: i64 = crate::RecordBatch::NO_TIMESTAMP;
    /// Java `ConsumerRecord.NULL_SIZE`.
    pub const NULL_SIZE: i32 = -1;

    /// Topic and partition of this record.
    #[must_use]
    pub fn topic_partition(&self) -> TopicPartition {
        TopicPartition::new(self.topic.clone(), self.partition)
    }

    /// Serialized key size in bytes, or [`Self::NULL_SIZE`] if there is no key
    /// (Java `serializedKeySize`).
    #[must_use]
    pub fn serialized_key_size(&self) -> i32 {
        serialized_bytes_size(self.key.as_ref())
    }

    /// Serialized value size in bytes, or [`Self::NULL_SIZE`] if there is no value
    /// (Java `serializedValueSize`).
    #[must_use]
    pub fn serialized_value_size(&self) -> i32 {
        serialized_bytes_size(self.value.as_ref())
    }

    /// Java `ConsumerRecord.topic`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Java `ConsumerRecord.partition`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Java `ConsumerRecord.offset`.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Java `ConsumerRecord.timestamp`.
    #[must_use]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Java `ConsumerRecord.timestampType`.
    #[must_use]
    pub fn timestamp_type(&self) -> TimestampType {
        self.timestamp_type
    }

    /// Java `ConsumerRecord.key`.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Java `ConsumerRecord.value`.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Java `ConsumerRecord.headers`.
    #[must_use]
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    /// Java `Headers.lastHeader`.
    #[must_use]
    pub fn last_header(&self, key: &str) -> Option<&Header> {
        Header::last_in(&self.headers, key)
    }

    /// Java `Headers.headers(String)`.
    pub fn headers_for_key<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a Header> + 'a {
        Header::for_key(&self.headers, key)
    }

    /// Java `ConsumerRecord.leaderEpoch`.
    #[must_use]
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}

impl fmt::Display for FetchedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConsumerRecord(topic = {}, partition = {}, leaderEpoch = ",
            self.topic, self.partition
        )?;
        write_java_optional(f, self.leader_epoch)?;
        write!(
            f,
            ", offset = {}, {} = {}, deliveryCount = null, serialized key size = {}, serialized value size = {}, headers = ",
            self.offset,
            self.timestamp_type,
            self.timestamp,
            self.serialized_key_size(),
            self.serialized_value_size()
        )?;
        write_java_record_headers(f, &self.headers, true)?;
        f.write_str(", key = ")?;
        write_java_optional_bytes(f, self.key.as_deref())?;
        f.write_str(", value = ")?;
        write_java_optional_bytes(f, self.value.as_deref())?;
        f.write_str(")")
    }
}

fn serialized_bytes_size(bytes: Option<&Bytes>) -> i32 {
    bytes
        .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
        .unwrap_or(FetchedRecord::NULL_SIZE)
}

/// Records from one fetch or poll (Java `ConsumerRecords`).
///
/// Indexes and iterates like a slice of [`FetchedRecord`]. [`Self::empty`] /
/// [`Self::is_empty`] / [`Self::partitions`] / [`Self::records`] /
/// [`Self::next_offsets`] match Java `empty` / `isEmpty` / `partitions` /
/// `records(TopicPartition)` / `nextOffsets`.
#[derive(Debug, Clone, Default)]
pub struct ConsumerRecords {
    records: Vec<FetchedRecord>,
}

impl ConsumerRecords {
    /// Java `ConsumerRecords.empty`.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Java `ConsumerRecords.isEmpty`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Number of records (Java `count`). Same as slice `len` via [`Deref`].
    #[must_use]
    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// Distinct partitions in this batch, in first-seen order.
    #[must_use]
    pub fn partitions(&self) -> Vec<TopicPartition> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for rec in &self.records {
            let tp = rec.topic_partition();
            if seen.insert(tp.clone()) {
                out.push(tp);
            }
        }
        out
    }

    /// Records for this partition (Java `records(TopicPartition)`).
    pub fn records(
        &self,
        partition: impl Into<TopicPartition>,
    ) -> impl Iterator<Item = &FetchedRecord> {
        let tp = partition.into();
        self.records
            .iter()
            .filter(move |r| r.topic == tp.topic && r.partition == tp.partition)
    }

    /// Records for this topic name (Java `records(String)`).
    pub fn records_for_topic<'a>(
        &'a self,
        topic: &'a str,
    ) -> impl Iterator<Item = &'a FetchedRecord> {
        self.records.iter().filter(move |r| r.topic == topic)
    }

    /// Next offset to consume per partition (Java `nextOffsets`).
    ///
    /// For each partition that has at least one record, this is the last
    /// record's offset plus one, with that record's leader epoch and
    /// [`OffsetAndMetadata::NO_METADATA`]. Partitions appear in first-seen
    /// order.
    #[must_use]
    pub fn next_offsets(&self) -> Vec<(TopicPartition, OffsetAndMetadata)> {
        let mut last = HashMap::new();
        let mut order = Vec::new();
        for rec in &self.records {
            let tp = rec.topic_partition();
            if last.insert(tp.clone(), rec).is_none() {
                order.push(tp);
            }
        }
        order
            .into_iter()
            .filter_map(|tp| {
                last.remove(&tp).map(|rec| {
                    let mut md = OffsetAndMetadata::new(rec.offset.saturating_add(1));
                    if let Some(epoch) = rec.leader_epoch {
                        md = md.with_leader_epoch(epoch);
                    }
                    (tp, md)
                })
            })
            .collect()
    }
}

impl Deref for ConsumerRecords {
    type Target = [FetchedRecord];

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl AsRef<[FetchedRecord]> for ConsumerRecords {
    fn as_ref(&self) -> &[FetchedRecord] {
        &self.records
    }
}

impl From<Vec<FetchedRecord>> for ConsumerRecords {
    fn from(records: Vec<FetchedRecord>) -> Self {
        Self { records }
    }
}

impl IntoIterator for ConsumerRecords {
    type Item = FetchedRecord;
    type IntoIter = std::vec::IntoIter<FetchedRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<'a> IntoIterator for &'a ConsumerRecords {
    type Item = &'a FetchedRecord;
    type IntoIter = std::slice::Iter<'a, FetchedRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
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

    /// Java `Topic.CLUSTER_METADATA_TOPIC_PARTITION` (`__cluster_metadata`-0).
    #[must_use]
    pub fn cluster_metadata() -> Self {
        Self::new(Topic::CLUSTER_METADATA_TOPIC_NAME, 0)
    }

    /// Java `TopicPartition.topic`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Java `TopicPartition.partition`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub(crate) fn list_from(pairs: &[(String, i32)]) -> Vec<Self> {
        pairs.iter().map(Self::from).collect()
    }
}

impl From<(String, i32)> for TopicPartition {
    fn from((topic, partition): (String, i32)) -> Self {
        Self { topic, partition }
    }
}

impl From<&(String, i32)> for TopicPartition {
    fn from((topic, partition): &(String, i32)) -> Self {
        Self {
            topic: topic.clone(),
            partition: *partition,
        }
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

impl From<&(&str, i32)> for TopicPartition {
    fn from((topic, partition): &(&str, i32)) -> Self {
        Self {
            topic: (*topic).into(),
            partition: *partition,
        }
    }
}

impl From<&TopicPartition> for TopicPartition {
    fn from(tp: &TopicPartition) -> Self {
        tp.clone()
    }
}

impl From<TopicPartition> for (String, i32) {
    fn from(tp: TopicPartition) -> Self {
        (tp.topic, tp.partition)
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

/// Topic id plus topic-partition (Java `TopicIdPartition`).
///
/// [`std::fmt::Display`] is Java `TopicIdPartition.toString` (`topicId:topic-partition`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicIdPartition {
    /// Topic id.
    pub topic_id: crate::Uuid,
    /// Topic name and partition index.
    pub topic_partition: TopicPartition,
}

impl TopicIdPartition {
    /// Java `TopicIdPartition(Uuid, TopicPartition)`.
    #[must_use]
    pub fn new(topic_id: crate::Uuid, topic_partition: impl Into<TopicPartition>) -> Self {
        Self {
            topic_id,
            topic_partition: topic_partition.into(),
        }
    }

    /// Java `TopicIdPartition(Uuid, int, String)`.
    #[must_use]
    pub fn from_topic(topic_id: crate::Uuid, partition: i32, topic: impl Into<String>) -> Self {
        Self::new(topic_id, TopicPartition::new(topic, partition))
    }

    /// Java `TopicIdPartition.topicId`.
    #[must_use]
    pub fn topic_id(&self) -> crate::Uuid {
        self.topic_id
    }

    /// Java `TopicIdPartition.topic`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic_partition.topic()
    }

    /// Java `TopicIdPartition.partition`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.topic_partition.partition()
    }

    /// Java `TopicIdPartition.topicPartition`.
    #[must_use]
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }
}

impl fmt::Display for TopicIdPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}-{}", self.topic_id, self.topic(), self.partition())
    }
}

/// Committed offset plus optional leader epoch and user metadata.
///
/// Java `OffsetAndMetadata`. The epoch is `None` when the wire value is
/// [`crate::RecordBatch::NO_PARTITION_LEADER_EPOCH`] or otherwise negative
/// (Java `leaderEpoch()` is empty when the stored epoch is `null` or
/// negative).
/// [`Self::new`] uses [`Self::NO_METADATA`] (Java `OffsetFetchResponse.NO_METADATA`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndMetadata {
    /// Next fetch offset (the committed offset).
    pub offset: i64,
    /// Broker leader epoch, or `None` when unknown.
    pub leader_epoch: Option<i32>,
    /// Caller-supplied metadata string (Kafka `committed.metadata`).
    pub metadata: String,
}

impl OffsetAndMetadata {
    /// Java `OffsetFetchResponse.NO_METADATA` (empty string; Java
    /// `OffsetAndMetadata` stores this when the constructor metadata is
    /// null). Same sentinel as [`crate::protocol::group::FetchedOffset::NO_METADATA`].
    pub const NO_METADATA: &'static str = crate::protocol::group::FetchedOffset::NO_METADATA;

    /// Java `OffsetFetchResponse.INVALID_OFFSET`. Same sentinel as
    /// [`crate::protocol::group::FetchedOffset::INVALID_OFFSET`].
    pub const INVALID_OFFSET: i64 = crate::protocol::group::FetchedOffset::INVALID_OFFSET;

    /// Offset only: unknown epoch, [`Self::NO_METADATA`].
    #[must_use]
    pub fn new(offset: i64) -> Self {
        Self {
            offset,
            leader_epoch: None,
            metadata: Self::NO_METADATA.into(),
        }
    }

    /// Offset plus a metadata string.
    #[must_use]
    pub fn with_metadata(offset: i64, metadata: impl Into<String>) -> Self {
        Self {
            offset,
            leader_epoch: None,
            metadata: metadata.into(),
        }
    }

    /// Set the leader epoch. Negative values become `None`.
    #[must_use]
    pub fn with_leader_epoch(mut self, epoch: i32) -> Self {
        self.leader_epoch = (epoch >= 0).then_some(epoch);
        self
    }

    pub(crate) fn from_wire(offset: i64, leader_epoch: i32, metadata: String) -> Self {
        Self {
            offset,
            leader_epoch: (leader_epoch >= 0).then_some(leader_epoch),
            metadata,
        }
    }

    pub(crate) fn wire_epoch(&self) -> i32 {
        self.leader_epoch
            .unwrap_or(crate::RecordBatch::NO_PARTITION_LEADER_EPOCH)
    }

    /// Java `OffsetAndMetadata.offset`.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Java `OffsetAndMetadata.leaderEpoch` (`None` is Java empty `Optional`).
    #[must_use]
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Java `OffsetAndMetadata.metadata`.
    #[must_use]
    pub fn metadata(&self) -> &str {
        self.metadata.as_str()
    }
}

impl fmt::Display for OffsetAndMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OffsetAndMetadata{{offset={}, leaderEpoch=", self.offset)?;
        write_optional_i32(f, self.leader_epoch)?;
        write!(f, ", metadata='{}'}}", self.metadata)
    }
}

fn write_optional_i32(f: &mut fmt::Formatter<'_>, v: Option<i32>) -> fmt::Result {
    match v {
        Some(n) => write!(f, "{n}"),
        None => f.write_str("null"),
    }
}

impl From<i64> for OffsetAndMetadata {
    fn from(offset: i64) -> Self {
        Self::new(offset)
    }
}

/// Offset plus the matching record timestamp from ListOffsets.
///
/// Java `OffsetAndTimestamp`. [`Self::leader_epoch`] is Java `getLeaderEpoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetAndTimestamp {
    /// Log offset, or `-1` when the broker has no match.
    pub offset: i64,
    /// Record timestamp in milliseconds since the Unix epoch, or `-1`.
    pub timestamp: i64,
    /// Leader epoch from ListOffsets v4+, or `None` when unknown.
    pub leader_epoch: Option<i32>,
}

impl OffsetAndTimestamp {
    /// Java `ListOffsetsResponse.UNKNOWN_OFFSET`.
    pub const UNKNOWN_OFFSET: i64 = -1;
    /// Java `ListOffsetsResponse.UNKNOWN_TIMESTAMP`.
    pub const UNKNOWN_TIMESTAMP: i64 = -1;

    /// Offset and timestamp with an unknown leader epoch.
    #[must_use]
    pub fn new(offset: i64, timestamp: i64) -> Self {
        Self {
            offset,
            timestamp,
            leader_epoch: None,
        }
    }

    /// Set the leader epoch. Negative values become `None`.
    #[must_use]
    pub fn with_leader_epoch(mut self, epoch: i32) -> Self {
        self.leader_epoch = (epoch >= 0).then_some(epoch);
        self
    }

    /// Java `OffsetAndTimestamp.offset`.
    #[must_use]
    pub fn offset(self) -> i64 {
        self.offset
    }

    /// Java `OffsetAndTimestamp.timestamp`.
    #[must_use]
    pub fn timestamp(self) -> i64 {
        self.timestamp
    }

    /// Java `OffsetAndTimestamp.getLeaderEpoch`.
    #[must_use]
    pub fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }
}

impl fmt::Display for OffsetAndTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(timestamp={}, leaderEpoch=", self.timestamp)?;
        write_optional_i32(f, self.leader_epoch)?;
        write!(f, ", offset={})", self.offset)
    }
}

/// One partition from Metadata: leader, replicas, ISR, and offline replicas.
///
/// Java `PartitionInfo`. [`Self::offline_replicas`] is Java `offlineReplicas`.
/// [`Self::leader_epoch`] is Metadata v7+
/// ([`crate::RecordBatch::NO_PARTITION_LEADER_EPOCH`] when unknown).
/// [`Self::from_partition_metadata`] is Java `MetadataResponse.toPartitionInfo`
/// (broker ids, not `Node`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Leader broker id, or [`MetadataResponse::NO_LEADER_ID`].
    pub leader: i32,
    /// Leader epoch from Metadata v7+, or
    /// [`crate::RecordBatch::NO_PARTITION_LEADER_EPOCH`].
    pub leader_epoch: i32,
    /// Replica broker ids.
    pub replicas: Vec<i32>,
    /// In-sync replica broker ids.
    pub isr: Vec<i32>,
    /// Offline replica broker ids (Java `offlineReplicas`).
    pub offline_replicas: Vec<i32>,
}

impl PartitionInfo {
    /// Java `MetadataResponse.toPartitionInfo` (broker ids, not `Node`).
    ///
    /// Java `PartitionMetadata` carries the topic name; this type stores it as
    /// a field, so callers pass `topic`. Also copies Metadata `leader_epoch`
    /// (Java `PartitionInfo` has no epoch).
    #[must_use]
    pub fn from_partition_metadata(topic: impl Into<String>, p: &PartitionMetadata) -> Self {
        Self {
            topic: topic.into(),
            partition: p.partition_index,
            leader: p.leader_id,
            leader_epoch: p.leader_epoch,
            replicas: p.replica_nodes.clone(),
            isr: p.isr_nodes.clone(),
            offline_replicas: p.offline_replicas.clone(),
        }
    }

    /// Java `PartitionInfo.topic`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Java `PartitionInfo.partition`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Java `PartitionInfo.leader` (broker id, or
    /// [`MetadataResponse::NO_LEADER_ID`]).
    #[must_use]
    pub fn leader(&self) -> i32 {
        self.leader
    }

    /// Metadata v7+ leader epoch
    /// ([`crate::RecordBatch::NO_PARTITION_LEADER_EPOCH`] when unknown).
    #[must_use]
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Java `PartitionInfo.replicas`.
    #[must_use]
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// Java `PartitionInfo.inSyncReplicas`.
    #[must_use]
    pub fn isr(&self) -> &[i32] {
        &self.isr
    }

    /// Java `PartitionInfo.offlineReplicas`.
    #[must_use]
    pub fn offline_replicas(&self) -> &[i32] {
        &self.offline_replicas
    }
}

impl fmt::Display for PartitionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Partition(topic = {}, partition = {}, leader = ",
            self.topic, self.partition
        )?;
        if self.leader < 0 {
            f.write_str("none")?;
        } else {
            write!(f, "{}", self.leader)?;
        }
        f.write_str(", replicas = ")?;
        write_broker_id_list(f, &self.replicas)?;
        f.write_str(", isr = ")?;
        write_broker_id_list(f, &self.isr)?;
        f.write_str(", offlineReplicas = ")?;
        write_broker_id_list(f, &self.offline_replicas)?;
        f.write_str(")")
    }
}

fn write_broker_id_list(f: &mut fmt::Formatter<'_>, ids: &[i32]) -> fmt::Result {
    f.write_str("[")?;
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            f.write_str(",")?;
        }
        write!(f, "{id}")?;
    }
    f.write_str("]")
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
    /// Last consumed record-batch leader epoch (Fetch v12+ `LastFetchedEpoch`).
    last_fetched_epochs: HashMap<(String, i32), i32>,
    preferred: HashMap<(String, i32), i32>,
    paused: HashSet<(String, i32)>,
    pending: VecDeque<FetchedRecord>,
    m_fetch_rounds: AtomicU64,
    m_records: AtomicU64,
    m_bytes: AtomicU64,
    m_errors: AtomicU64,
    m_heartbeat_ok: Arc<AtomicU64>,
    m_heartbeat_fail: Arc<AtomicU64>,
    wakeup: Arc<AtomicBool>,
    wakeup_tx: watch::Sender<bool>,
    telemetry_version: Option<i16>,
    client_instance_id: Option<[u8; 16]>,
    m_fetch_latency: crate::metrics::LatencyTracker,
    topic_metrics: HashMap<String, crate::metrics::FetchTopicTracker>,
    reconnect_fails: HashMap<i32, u32>,
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
        let mut cfg = cfg;
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        let mut conn = BrokerConn::connect_tls_any(
            &cfg.bootstrap,
            &cfg.client_id,
            cfg.connect_timeout,
            cfg.tls.as_ref(),
        )
        .await?;
        let resp =
            crate::protocol::api::negotiate_api_versions(&mut conn, cfg.request_timeout).await?;
        sasl::apply_api_keys(&mut conn, &resp.api_keys);
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
            .and_then(|v| pick_version(v.min_version, v.max_version, 4, 17))
            .ok_or_else(|| Error::Unsupported("broker does not support Fetch v4-17".into()))?;
        let metadata_version = versions
            .get(&METADATA)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 13))
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        let telemetry_version = versions
            .get(&GET_TELEMETRY_SUBSCRIPTIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
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
            last_fetched_epochs: HashMap::new(),
            preferred: HashMap::new(),
            paused: HashSet::new(),
            pending: VecDeque::new(),
            m_fetch_rounds: AtomicU64::new(0),
            m_records: AtomicU64::new(0),
            m_bytes: AtomicU64::new(0),
            m_errors: AtomicU64::new(0),
            m_heartbeat_ok: Arc::new(AtomicU64::new(0)),
            m_heartbeat_fail: Arc::new(AtomicU64::new(0)),
            wakeup: Arc::new(AtomicBool::new(false)),
            wakeup_tx: watch::channel(false).0,
            telemetry_version,
            client_instance_id: None,
            m_fetch_latency: crate::metrics::LatencyTracker::new(),
            topic_metrics: HashMap::new(),
            reconnect_fails: HashMap::new(),
        })
    }

    /// Assign one partition at `offset`. Replaces a previous offset for the same pair.
    ///
    /// Java `assign` calls [`crate::protocol::group::Topic::validate`] on the topic name.
    pub async fn assign(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let topic = topic.into();
        Topic::validate(&topic)?;
        self.refresh_metadata(Some(std::slice::from_ref(&topic)))
            .await?;
        self.drop_pending_for(&topic, partition);
        self.assigned
            .retain(|(t, p, _)| !(t == &topic && *p == partition));
        self.set_last_fetched_epoch(
            &topic,
            partition,
            crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
        );
        self.assigned.push((topic, partition, offset));
        Ok(())
    }

    /// Replace the assignment with these `(partition, offset)` pairs.
    pub async fn assign_many(
        &mut self,
        starts: impl IntoIterator<Item = (impl Into<TopicPartition>, i64)>,
    ) -> Result<()> {
        let triples: Vec<(String, i32, i64)> = starts
            .into_iter()
            .map(|(tp, offset)| {
                let tp = tp.into();
                (tp.topic, tp.partition, offset)
            })
            .collect();
        self.assign_all(&triples).await
    }

    /// Replace the assignment (Java `assign(Collection)`).
    ///
    /// Offsets come from [`ConsumerConfig::auto_offset_reset`] via ListOffsets
    /// (`earliest` or `latest`). [`crate::AutoOffsetReset::None`] is an error
    /// (a manual consumer has no committed offsets). An empty list drops the
    /// assignment ([`Self::unassign`]). Each topic name is checked with
    /// [`crate::protocol::group::Topic::validate`].
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::assign_partitions_timeout`].
    pub async fn assign_partitions(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.assign_partitions_timeout(partitions, timeout).await
    }

    /// [`Self::assign_partitions`] with a one-shot timeout for ListOffsets.
    pub async fn assign_partitions_timeout(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
        timeout: Duration,
    ) -> Result<()> {
        let tps: Vec<TopicPartition> = partitions.into_iter().map(Into::into).collect();
        if tps.is_empty() {
            self.unassign();
            return Ok(());
        }
        for tp in &tps {
            Topic::validate(&tp.topic)?;
        }
        let timestamp = match self.cfg.auto_offset_reset {
            crate::AutoOffsetReset::Earliest => crate::EARLIEST_TIMESTAMP,
            crate::AutoOffsetReset::Latest => crate::LATEST_TIMESTAMP,
            crate::AutoOffsetReset::None => {
                return Err(Error::protocol(
                    "auto.offset.reset=none: no offset for manual assign",
                ));
            }
        };
        let starts = self.offsets_at(tps, timestamp, timeout).await?;
        let triples: Vec<(String, i32, i64)> = starts
            .into_iter()
            .map(|(tp, offset)| (tp.topic, tp.partition, offset))
            .collect();
        self.assign_all(&triples).await
    }

    /// Drop every assigned partition (Java `unsubscribe` for a manual consumer).
    pub fn unassign(&mut self) {
        self.clear_assignment();
    }

    /// Assign every partition of `topic` at `offset` (from metadata).
    ///
    /// Java `assign` calls [`crate::protocol::group::Topic::validate`] on the topic name.
    pub async fn assign_topic(&mut self, topic: impl Into<String>, offset: i64) -> Result<()> {
        let topic = topic.into();
        Topic::validate(&topic)?;
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
            self.set_last_fetched_epoch(&topic, p, crate::RecordBatch::NO_PARTITION_LEADER_EPOCH);
            self.assigned.push((topic.clone(), p, offset));
        }
        Ok(())
    }

    /// Assigned partitions (Java `assignment`). Offsets are [`Self::positions`].
    #[must_use]
    pub fn assignment(&self) -> Vec<TopicPartition> {
        self.assigned
            .iter()
            .map(|(t, p, _)| TopicPartition::new(t.clone(), *p))
            .collect()
    }

    /// Same as [`Self::assignment`].
    #[must_use]
    pub fn assigned_partitions(&self) -> Vec<TopicPartition> {
        self.assignment()
    }

    /// Assigned partitions with their next fetch offsets.
    #[must_use]
    pub fn positions(&self) -> Vec<(TopicPartition, i64)> {
        self.assigned
            .iter()
            .map(|(t, p, o)| (TopicPartition::new(t.clone(), *p), *o))
            .collect()
    }

    pub(crate) fn assigned_offsets(&self) -> &[(String, i32, i64)] {
        &self.assigned
    }

    pub(crate) fn leader_epoch(&self, topic: &str, partition: i32) -> i32 {
        self.cluster.leader_epoch(topic, partition)
    }

    pub(crate) fn last_fetched_epoch(&self, topic: &str, partition: i32) -> i32 {
        self.last_fetched_epochs
            .get(&(topic.to_string(), partition))
            .copied()
            .unwrap_or(crate::RecordBatch::NO_PARTITION_LEADER_EPOCH)
    }

    pub(crate) fn set_last_fetched_epoch(&mut self, topic: &str, partition: i32, epoch: i32) {
        if epoch >= 0 {
            let _prev = self
                .last_fetched_epochs
                .insert((topic.to_string(), partition), epoch);
        } else {
            let _removed = self
                .last_fetched_epochs
                .remove(&(topic.to_string(), partition));
        }
    }

    /// Next fetch offset for an assigned partition.
    ///
    /// An unassigned partition is Java `IllegalStateException`
    /// (`You can only check the position for partitions assigned to this consumer.`).
    pub fn position(&self, topic: &str, partition: i32) -> Result<i64> {
        self.assigned
            .iter()
            .find(|(t, p, _)| t == topic && *p == partition)
            .map(|(_, _, o)| *o)
            .ok_or_else(reject_java_position_unassigned)
    }

    /// [`Self::position`] for a [`TopicPartition`].
    pub fn position_of(&self, partition: impl Into<TopicPartition>) -> Result<i64> {
        let tp = partition.into();
        self.position(&tp.topic, tp.partition)
    }

    /// Stop fetching these assigned partitions until [`resume`](Self::resume).
    ///
    /// Pause is stored on the consumer, so it survives group rebalance. Fetch
    /// skips a partition only while it is both assigned and paused. Records
    /// already buffered for a paused partition are held until resume.
    pub fn pause(&mut self, partitions: impl IntoIterator<Item = impl Into<TopicPartition>>) {
        for p in partitions {
            let tp = p.into();
            let _inserted = self.paused.insert((tp.topic, tp.partition));
        }
    }

    /// Undo [`pause`](Self::pause) for these partitions.
    pub fn resume(&mut self, partitions: impl IntoIterator<Item = impl Into<TopicPartition>>) {
        for p in partitions {
            let tp = p.into();
            let _removed = self.paused.remove(&(tp.topic, tp.partition));
        }
    }

    /// Assigned partitions that [`fetch`](Self::fetch) currently skips.
    pub fn paused(&self) -> Vec<TopicPartition> {
        self.assigned
            .iter()
            .filter(|(t, p, _)| self.paused.contains(&(t.clone(), *p)))
            .map(|(t, p, _)| TopicPartition::new(t.clone(), *p))
            .collect()
    }

    pub(crate) fn clear_assignment(&mut self) {
        self.assigned.clear();
        self.pending.clear();
        self.last_fetched_epochs.clear();
    }

    /// Replace the assignment. One Metadata refresh for the topic set.
    pub(crate) async fn assign_all(&mut self, starts: &[(String, i32, i64)]) -> Result<()> {
        self.clear_assignment();
        if starts.is_empty() {
            return Ok(());
        }
        let mut topics: Vec<String> = Vec::new();
        for (topic, _, _) in starts {
            Topic::validate(topic)?;
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
    pub(crate) fn advance(&mut self, topic: &str, partition: i32, next_offset: i64) {
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
        let resp =
            crate::protocol::api::negotiate_api_versions(&mut conn, self.cfg.request_timeout)
                .await?;
        sasl::apply_api_keys(&mut conn, &resp.api_keys);
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
        self.refresh_metadata_timeout(topics, self.cfg.request_timeout)
            .await
    }

    async fn refresh_metadata_timeout(
        &mut self,
        topics: Option<&[String]>,
        timeout: Duration,
    ) -> Result<()> {
        if self.conn.idle_expired(self.cfg.connections_max_idle) {
            let addr = self.conn.addr().to_string();
            self.conn = self.open_node_conn(&addr).await?;
        }
        let version = self.metadata_version;
        let allow = self.cfg.allow_auto_topic_creation;
        let body = match self
            .conn
            .roundtrip(
                METADATA,
                version,
                |buf| encode_metadata_request(buf, version, topics, allow),
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
                        |buf| encode_metadata_request(buf, version, topics, allow),
                        timeout,
                    )
                    .await?
            }
            Err(e) => return Err(e),
        };
        let md = decode_metadata_response(&mut body.clone(), version)?;
        md.check()?;
        self.cluster.apply(&md, version);
        self.metadata = Some(md);
        Ok(())
    }

    pub(crate) async fn ensure_topic_metadata(&mut self, topic: &str) -> Result<()> {
        if self.cluster.topic_fresh(topic, self.cfg.metadata_max_age) {
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

    pub(crate) fn topic_name_ids(&self) -> HashMap<String, [u8; 16]> {
        let mut out = HashMap::new();
        let Some(md) = &self.metadata else {
            return out;
        };
        for t in &md.topics {
            if let Some(name) = &t.name {
                let _ = out.insert(name.clone(), t.topic_id);
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
        if self
            .conns
            .get(&node)
            .is_some_and(|c| c.idle_expired(self.cfg.connections_max_idle))
        {
            let _ = self.conns.remove(&node);
        }
        if self.conns.contains_key(&node) {
            return Ok(());
        }
        let addr = self
            .cluster
            .brokers
            .get(&node)
            .cloned()
            .ok_or_else(|| Error::protocol(format!("unknown broker {node}")))?;
        let deadline = Instant::now() + self.cfg.request_timeout;
        loop {
            if self.woken() {
                return Err(Error::Wakeup);
            }
            let fails = self.reconnect_fails.get(&node).copied().unwrap_or(0);
            self.sleep_reconnect_backoff(fails, deadline).await?;
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            match self.open_node_conn(&addr).await {
                Ok(conn) => {
                    let _ = self.reconnect_fails.remove(&node);
                    let _prev = self.conns.insert(node, conn);
                    return Ok(());
                }
                Err(e) if e.is_retriable() => {
                    let _fails =
                        crate::config::bump_reconnect_fails(&mut self.reconnect_fails, node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                }
                Err(e) => {
                    let _fails =
                        crate::config::bump_reconnect_fails(&mut self.reconnect_fails, node);
                    return Err(e);
                }
            }
        }
    }

    async fn open_node_conn(&self, addr: &str) -> Result<crate::net::BrokerConn> {
        let mut conn = BrokerConn::connect_tls(
            addr,
            &self.cfg.client_id,
            self.cfg.connect_timeout,
            self.cfg.tls.as_ref(),
        )
        .await?;
        let versions_resp =
            crate::protocol::api::negotiate_api_versions(&mut conn, self.cfg.request_timeout)
                .await?;
        sasl::apply_api_keys(&mut conn, &versions_resp.api_keys);
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
        Ok(conn)
    }

    /// Wait [`ConsumerConfig::reconnect_backoff`] unless a wakeup arrives.
    async fn sleep_reconnect_backoff(&self, fails: u32, deadline: Instant) -> Result<()> {
        if self.woken() {
            return Err(Error::Wakeup);
        }
        let delay = crate::config::reconnect_backoff_delay(
            self.cfg.reconnect_backoff,
            self.cfg.reconnect_backoff_max,
            fails,
        );
        if delay.is_zero() {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        let rest = delay.min(deadline.saturating_duration_since(now));
        let mut rx = self.wakeup_tx.subscribe();
        tokio::select! {
            biased;
            result = rx.wait_for(|on| *on) => {
                drop(result);
                Err(Error::Wakeup)
            }
            _ = tokio::time::sleep(rest) => Ok(())
        }
    }

    async fn recover_leader_epoch(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.recover_leader_epochs(&[(topic.to_string(), partition)])
            .await
    }

    async fn recover_leader_epochs(&mut self, coords: &[(String, i32)]) -> Result<()> {
        if coords.is_empty() {
            return Ok(());
        }
        let version = self
            .versions
            .get(&OFFSET_FOR_LEADER_EPOCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 4))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support OffsetForLeaderEpoch v0-4".into())
            })?;
        // Preferred replica may have returned the fence; OffsetForLeaderEpoch is leader-only.
        // Refresh Metadata first so `current_leader_epoch` is not the value that just fenced us.
        for (topic, partition) in coords {
            let _ = self.preferred.remove(&(topic.clone(), *partition));
        }
        let deadline = Instant::now() + self.cfg.request_timeout;
        let mut names: Vec<String> = coords.iter().map(|(t, _)| t.clone()).collect();
        names.sort();
        names.dedup();
        self.refresh_metadata(Some(&names)).await?;
        let mut remaining: Vec<(String, i32)> = coords.to_vec();
        remaining.sort();
        remaining.dedup();
        loop {
            if remaining.is_empty() {
                return Ok(());
            }
            let mut missing_leader = false;
            for (topic, partition) in &remaining {
                if self.cluster.leader(topic, *partition).is_err() {
                    missing_leader = true;
                    break;
                }
            }
            if missing_leader {
                self.refresh_metadata(Some(&names)).await?;
            }
            let mut by_leader: HashMap<i32, Vec<(String, i32)>> = HashMap::new();
            let mut retry = Vec::new();
            for (topic, partition) in remaining {
                match self.cluster.leader(&topic, partition) {
                    Ok((node, _)) => by_leader.entry(node).or_default().push((topic, partition)),
                    Err(_) => retry.push((topic, partition)),
                }
            }
            let mut nodes: Vec<i32> = by_leader.keys().copied().collect();
            nodes.sort_unstable();
            for node in nodes {
                let Some(parts) = by_leader.remove(&node) else {
                    continue;
                };
                self.connect_node(node).await?;
                let topics = offset_for_leader_epoch_topics(&self.cluster, &parts);
                let timeout = self.cfg.request_timeout;
                let body = {
                    let conn = self
                        .conns
                        .get_mut(&node)
                        .ok_or_else(|| Error::protocol("missing epoch conn"))?;
                    conn.roundtrip(
                        OFFSET_FOR_LEADER_EPOCH,
                        version,
                        |buf| encode_offset_for_leader_epoch_topics_request(buf, version, &topics),
                        timeout,
                    )
                    .await
                };
                let body = match body {
                    Ok(b) => b,
                    Err(e) if e.is_retriable() => {
                        let _ = self.conns.remove(&node);
                        retry.extend(parts);
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                match decode_offset_for_leader_epoch_topics_response(&mut body.clone(), version) {
                    Ok((got, ..)) => {
                        let more = self.apply_epoch_end_offsets(&parts, &got)?;
                        if !more.is_empty() {
                            let _ = self.conns.remove(&node);
                            retry.extend(more);
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        let _ = self.conns.remove(&node);
                        retry.extend(parts);
                    }
                    Err(e) => return Err(e),
                }
            }
            remaining = retry;
            remaining.sort();
            remaining.dedup();
            if remaining.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let mut retry_names: Vec<String> = remaining.iter().map(|(t, _)| t.clone()).collect();
            retry_names.sort();
            retry_names.dedup();
            self.refresh_metadata(Some(&retry_names)).await?;
        }
    }

    fn apply_epoch_end_offsets(
        &mut self,
        requested: &[(String, i32)],
        topics: &[OffsetForLeaderTopicResult],
    ) -> Result<Vec<(String, i32)>> {
        let mut seen = HashSet::new();
        let mut retry = Vec::new();
        for t in topics {
            for p in &t.partitions {
                let _ = seen.insert((t.topic.clone(), p.partition));
                if p.error_code == 0 {
                    self.cluster
                        .set_leader_epoch(&t.topic, p.partition, p.leader_epoch);
                    let assigned = self
                        .assigned
                        .iter()
                        .find(|(name, part, _)| name == &t.topic && *part == p.partition)
                        .map(|(_, _, o)| *o);
                    if let Some(off) = assigned {
                        if off > p.end_offset {
                            self.advance(&t.topic, p.partition, p.end_offset);
                            self.set_last_fetched_epoch(&t.topic, p.partition, p.leader_epoch);
                        }
                    }
                    continue;
                }
                let e = Error::broker(
                    p.error_code,
                    format!("OffsetForLeaderEpoch {}-{}", t.topic, p.partition),
                );
                let fence = p.error_code == error::FENCED_LEADER_EPOCH
                    || p.error_code == error::UNKNOWN_LEADER_EPOCH;
                if e.is_retriable() || fence {
                    self.cluster.invalidate_topic(&t.topic);
                    retry.push((t.topic.clone(), p.partition));
                    continue;
                }
                return Err(e);
            }
        }
        for coord in requested {
            if !seen.contains(coord) {
                retry.push(coord.clone());
            }
        }
        Ok(retry)
    }

    /// Fetch one round from every assigned partition that is not paused.
    ///
    /// Returns [`ConsumerRecords`], which indexes like a slice of
    /// [`FetchedRecord`]. Empty when every assigned partition is paused.
    /// Nothing assigned is Java `IllegalStateException` (`Consumer is not
    /// subscribed to any topics or assigned any partitions`).
    /// Partitions that share a leader go in one Fetch. Distinct leaders are
    /// fetched at the same time.
    ///
    /// When [`ConsumerConfig::max_poll_records`] is set, extra records from
    /// the Fetch stay buffered and are returned on the next call.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn fetch(&mut self) -> Result<ConsumerRecords> {
        self.fetch_records(true).await
    }

    /// [`Self::fetch`] for a subscribed [`crate::ConsumerGroup`] (assignment
    /// may still be empty while the coordinator is joining).
    pub(crate) async fn fetch_allow_unassigned(&mut self) -> Result<ConsumerRecords> {
        self.fetch_records(false).await
    }

    async fn fetch_records(&mut self, require_assignment: bool) -> Result<ConsumerRecords> {
        if self.take_wakeup() {
            return Err(Error::Wakeup);
        }
        if require_assignment && self.assigned.is_empty() {
            return Err(reject_java_no_subscription_or_assignment());
        }
        let started = Instant::now();
        let result = self.fetch_assigned().await;
        match result {
            Ok(recs) => {
                let elapsed = started.elapsed();
                self.m_fetch_latency.record(elapsed);
                let _ = self.m_fetch_rounds.fetch_add(1, Ordering::Relaxed);
                let n = u64::try_from(recs.len()).unwrap_or(u64::MAX);
                let _ = self.m_records.fetch_add(n, Ordering::Relaxed);
                let bytes: u64 = recs.iter().map(fetched_bytes).fold(0, u64::saturating_add);
                let _ = self.m_bytes.fetch_add(bytes, Ordering::Relaxed);
                crate::metrics::accumulate_fetch_topics(
                    &mut self.topic_metrics,
                    recs.iter().map(|r| (r.topic.as_str(), fetched_bytes(r))),
                    elapsed,
                );
                Ok(ConsumerRecords::from(
                    self.cfg.interceptors.on_consume(recs),
                ))
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

    /// Fetch with a one-shot `fetch.max.wait.ms` (Java `poll(Duration)`).
    ///
    /// [`ConsumerConfig::max_wait_ms`] is restored afterwards. Nothing
    /// assigned is the same Java `IllegalStateException` as [`Self::fetch`].
    pub async fn fetch_timeout(&mut self, timeout: Duration) -> Result<ConsumerRecords> {
        self.fetch_records_timeout(timeout, true).await
    }

    /// [`Self::fetch_timeout`] for a subscribed [`crate::ConsumerGroup`].
    pub(crate) async fn fetch_allow_unassigned_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<ConsumerRecords> {
        self.fetch_records_timeout(timeout, false).await
    }

    async fn fetch_records_timeout(
        &mut self,
        timeout: Duration,
        require_assignment: bool,
    ) -> Result<ConsumerRecords> {
        let prev = self.cfg.max_wait_ms;
        self.cfg.max_wait_ms = duration_millis_i32(timeout);
        let out = self.fetch_records(require_assignment).await;
        self.cfg.max_wait_ms = prev;
        out
    }

    /// Fetch counters and round latency since connect (min/mean/max and p50/p99).
    ///
    /// [`crate::ConsumerMetrics::topics`] is one row per topic that returned at least
    /// one record.
    #[must_use]
    pub fn metrics(&self) -> crate::ConsumerMetrics {
        crate::ConsumerMetrics {
            fetch_rounds: self.m_fetch_rounds.load(Ordering::Relaxed),
            records_fetched: self.m_records.load(Ordering::Relaxed),
            bytes_fetched: self.m_bytes.load(Ordering::Relaxed),
            fetch_errors: self.m_errors.load(Ordering::Relaxed),
            fetch_latency: self.m_fetch_latency.snapshot(),
            topics: crate::metrics::snapshot_fetch_topics(&self.topic_metrics),
            heartbeat_ok: self.m_heartbeat_ok.load(Ordering::Relaxed),
            heartbeat_fail: self.m_heartbeat_fail.load(Ordering::Relaxed),
        }
    }

    /// Shared Heartbeat counters for the group heartbeat task.
    pub(crate) fn heartbeat_counters(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        (Arc::clone(&self.m_heartbeat_ok), Arc::clone(&self.m_heartbeat_fail))
    }

    /// Java `clientInstanceId` (KIP-714 GetTelemetrySubscriptions).
    ///
    /// Returns [`crate::Uuid`] (Java `Uuid`). The first call sends a zero
    /// UUID; the broker assigns one. Later calls return the cached id
    /// without another round-trip. Waits up to
    /// [`ConsumerConfig::request_timeout`]. For a one-shot timeout, use
    /// [`Self::client_instance_id_timeout`].
    pub async fn client_instance_id(&mut self) -> Result<crate::Uuid> {
        let timeout = self.cfg.request_timeout;
        self.client_instance_id_timeout(timeout).await
    }

    /// [`Self::client_instance_id`] with a one-shot timeout (Java
    /// `clientInstanceId(Duration)`).
    ///
    /// `timeout` is the GetTelemetrySubscriptions RPC deadline. Cached after
    /// the first successful call; later calls ignore `timeout`.
    pub async fn client_instance_id_timeout(&mut self, timeout: Duration) -> Result<crate::Uuid> {
        if let Some(id) = self.client_instance_id {
            return Ok(crate::Uuid::from_bytes(id));
        }
        let version = self.telemetry_version.ok_or_else(|| {
            Error::Unsupported("broker does not support GetTelemetrySubscriptions".into())
        })?;
        if self.conn.idle_expired(self.cfg.connections_max_idle) {
            let addr = self.conn.addr().to_string();
            self.conn = self.open_node_conn(&addr).await?;
        }
        let id = crate::admin::fetch_client_instance_id(&mut self.conn, version, timeout, [0; 16])
            .await?;
        self.client_instance_id = Some(id);
        Ok(crate::Uuid::from_bytes(id))
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
        self.cfg.interceptors.close();
        self.conns.clear();
        Ok(())
    }

    /// Drop fetch connections (Java `close(Duration)`).
    ///
    /// A manual consumer has no LeaveGroup RPC; this is the same as
    /// [`Self::close`]. Group and share members use
    /// [`crate::ConsumerGroup::close_timeout`] /
    /// [`crate::ShareGroup::close_timeout`].
    pub async fn close_timeout(self, _timeout: Duration) -> Result<()> {
        self.close().await
    }

    pub(crate) fn close_interceptors(&self) {
        self.cfg.interceptors.close();
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

    /// Wait [`ConsumerConfig::retry_backoff`] (exponential) unless a wakeup arrives.
    pub(crate) async fn sleep_retry_backoff(&self, attempt: u32, deadline: Instant) -> Result<()> {
        if self.woken() {
            return Err(Error::Wakeup);
        }
        let delay = crate::config::retry_backoff_delay(
            self.cfg.retry_backoff,
            self.cfg.retry_backoff_max,
            attempt,
        );
        if delay.is_zero() {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        let rest = delay.min(deadline.saturating_duration_since(now));
        let mut rx = self.wakeup_tx.subscribe();
        tokio::select! {
            biased;
            result = rx.wait_for(|on| *on) => {
                drop(result);
                Err(Error::Wakeup)
            }
            _ = tokio::time::sleep(rest) => Ok(())
        }
    }

    async fn fetch_assigned(&mut self) -> Result<Vec<FetchedRecord>> {
        if let Some(ready) = self.take_ready() {
            return Ok(ready);
        }
        if self.assigned.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + self.cfg.request_timeout;
        let mut attempt = 0u32;
        loop {
            if self.woken() {
                return Err(Error::Wakeup);
            }
            let mut topics = Vec::new();
            let mut seen = HashSet::new();
            for (t, _, _) in &self.assigned {
                if seen.insert(t.clone()) {
                    topics.push(t.clone());
                }
            }
            if topics
                .iter()
                .any(|t| !self.cluster.topic_fresh(t, self.cfg.metadata_max_age))
            {
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
                                last_fetched_epoch: self.last_fetched_epoch(topic, *part),
                                log_start_offset: INVALID_LOG_START_OFFSET,
                                partition_max_bytes: self.cfg.max_partition_fetch_bytes,
                                replica_directory_id: [0; 16],
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
                self.sleep_retry_backoff(attempt, deadline).await?;
                attempt = attempt.saturating_add(1);
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
            let mut retry = FetchRetry::None;
            let mut fenced = Vec::new();
            for (node, body) in bodies {
                let mut body = match body {
                    Ok(b) => b,
                    Err(e) if e.is_retriable() => {
                        let _ = self.conns.remove(&node);
                        retry = retry.merge(FetchRetry::Backoff);
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                retry =
                    retry.merge(self.apply_fetch_body(node, &mut body, &mut out, &mut fenced)?);
            }
            if !fenced.is_empty() {
                fenced.sort();
                fenced.dedup();
                self.recover_leader_epochs(&fenced).await?;
                retry = retry.merge(FetchRetry::Backoff);
            }
            if retry.should_retry() {
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                if retry.needs_backoff() {
                    self.sleep_retry_backoff(attempt, deadline).await?;
                    attempt = attempt.saturating_add(1);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    let topics: Vec<String> =
                        self.assigned.iter().map(|(t, _, _)| t.clone()).collect();
                    self.refresh_metadata(Some(&topics)).await?;
                }
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
        let isolation_level = self.cfg.isolation_level.as_i8();
        let timeout = self.cfg.request_timeout;
        let fetch_version = self.fetch_version;
        let rack = self.cfg.rack.clone();
        let name_ids = self.topic_name_ids();
        if nodes.len() <= 1 {
            let mut out = Vec::with_capacity(nodes.len());
            for node in nodes {
                let Some(by_topic) = by_leader.remove(&node) else {
                    continue;
                };
                let topics = fetch_topics(by_topic, &name_ids);
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
                                fetch_version,
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
            let topics = fetch_topics(by_topic, &name_ids);
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
                                fetch_version,
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

    fn apply_fetch_body(
        &mut self,
        node: i32,
        body: &mut Bytes,
        out: &mut Vec<FetchedRecord>,
        fenced: &mut Vec<(String, i32)>,
    ) -> Result<FetchRetry> {
        let (fetched, endpoints, ..) = decode_fetch_response(body, self.fetch_version)?;
        self.cluster.apply_node_endpoints(&endpoints);
        let id_names = self.topic_id_names();
        let mut retry = FetchRetry::None;
        for topic in fetched {
            let name = if !topic.topic.is_empty() {
                topic.topic
            } else if let Some(n) = id_names.get(&topic.topic_id) {
                n.clone()
            } else {
                continue;
            };
            for part in topic.partitions {
                if self.cfg.rack.is_some() {
                    if let Some(replica) = part.preferred_read_replica() {
                        if replica != node {
                            let _prev = self
                                .preferred
                                .insert((name.clone(), part.partition), replica);
                            retry = retry.merge(FetchRetry::Redirect);
                            continue;
                        }
                    }
                }
                if part.error_code == error::OFFSET_OUT_OF_RANGE {
                    self.advance(&name, part.partition, part.log_start_offset);
                    self.set_last_fetched_epoch(
                        &name,
                        part.partition,
                        crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    );
                    continue;
                }
                if part.error_code == error::FENCED_LEADER_EPOCH
                    || part.error_code == error::UNKNOWN_LEADER_EPOCH
                {
                    fenced.push((name.clone(), part.partition));
                    continue;
                }
                if part.error_code != 0 {
                    let e = Error::broker(part.error_code, format!("{}-{}", name, part.partition));
                    if e.is_retriable() {
                        let applied = part.current_leader_id >= 0
                            && self.cluster.apply_current_leader(
                                &name,
                                part.partition,
                                part.current_leader_id,
                                part.current_leader_epoch,
                            );
                        if applied {
                            let _ = self.conns.remove(&node);
                            retry = retry.merge(FetchRetry::Redirect);
                        } else {
                            self.cluster.invalidate_topic(&name);
                            let _ = self.conns.remove(&node);
                            retry = retry.merge(FetchRetry::Backoff);
                        }
                        continue;
                    }
                    return Err(e);
                }
                if part.is_diverging_epoch() && part.diverging_end_offset >= 0 {
                    self.advance(&name, part.partition, part.diverging_end_offset);
                    self.set_last_fetched_epoch(&name, part.partition, part.diverging_epoch);
                    self.drop_pending_for(&name, part.partition);
                    retry = retry.merge(FetchRetry::Redirect);
                    continue;
                }
                let mut next = None;
                let mut last_epoch = crate::RecordBatch::NO_PARTITION_LEADER_EPOCH;
                let isolation = self.cfg.isolation_level;
                for batch in part.records {
                    if batch.attributes & crate::protocol::records::ATTR_CONTROL != 0 {
                        if let Some(last) = batch.records.last() {
                            next = Some(last.offset + 1);
                            last_epoch = batch.partition_leader_epoch;
                        }
                        continue;
                    }
                    let timestamp_type = batch.timestamp_type();
                    for rec in batch.records {
                        let offset = rec.offset;
                        if isolation == crate::IsolationLevel::ReadCommitted
                            && offset >= part.last_stable_offset
                        {
                            break;
                        }
                        next = Some(offset + 1);
                        last_epoch = batch.partition_leader_epoch;
                        if isolation == crate::IsolationLevel::ReadCommitted {
                            let aborted = part
                                .aborted_transactions
                                .iter()
                                .any(|(pid, first)| batch.producer_id == *pid && offset >= *first);
                            if aborted {
                                continue;
                            }
                        }
                        out.push(FetchedRecord {
                            topic: name.clone(),
                            partition: part.partition,
                            offset,
                            timestamp: rec.timestamp,
                            timestamp_type,
                            key: rec.key,
                            value: rec.value,
                            headers: rec.headers,
                            leader_epoch: (batch.partition_leader_epoch >= 0)
                                .then_some(batch.partition_leader_epoch),
                        });
                    }
                }
                if let Some(n) = next {
                    self.advance(&name, part.partition, n);
                    self.set_last_fetched_epoch(&name, part.partition, last_epoch);
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

    /// ListOffsets timestamp: [`crate::EARLIEST_TIMESTAMP`] (`-2`),
    /// [`crate::LATEST_TIMESTAMP`] (`-1`), [`crate::MAX_TIMESTAMP`] (`-3`),
    /// [`crate::EARLIEST_LOCAL_TIMESTAMP`] (`-4`),
    /// [`crate::LATEST_TIERED_TIMESTAMP`] (`-5`), or milliseconds.
    ///
    /// Negotiates ListOffsets v1–v10 (v6–v10 flexible; v10 TimeoutMs). Waits up to
    /// [`ConsumerConfig::request_timeout`]. For a one-shot timeout, use
    /// [`Self::list_offsets_timeout`].
    pub async fn list_offsets(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64> {
        let timeout = self.cfg.request_timeout;
        self.list_offsets_timeout(topic, partition, timestamp, timeout)
            .await
    }

    /// [`Self::list_offsets`] with a one-shot timeout.
    pub async fn list_offsets_timeout(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        timestamp: i64,
        timeout: Duration,
    ) -> Result<i64> {
        let topic = topic.into();
        Ok(self
            .list_offset_at(&topic, partition, timestamp, timeout)
            .await?
            .0)
    }

    /// [`Self::list_offsets`] for a [`TopicPartition`].
    pub async fn list_offset(
        &mut self,
        partition: impl Into<TopicPartition>,
        timestamp: i64,
    ) -> Result<i64> {
        let timeout = self.cfg.request_timeout;
        self.list_offset_timeout(partition, timestamp, timeout)
            .await
    }

    /// [`Self::list_offset`] with a one-shot timeout.
    pub async fn list_offset_timeout(
        &mut self,
        partition: impl Into<TopicPartition>,
        timestamp: i64,
        timeout: Duration,
    ) -> Result<i64> {
        let tp = partition.into();
        self.list_offsets_timeout(tp.topic, tp.partition, timestamp, timeout)
            .await
    }

    async fn list_offset_at(
        &mut self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        timeout: Duration,
    ) -> Result<(i64, i64, i32)> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.cluster.leader(topic, partition).is_err() {
                let topics = [topic.to_string()];
                self.refresh_metadata_timeout(Some(&topics), timeout)
                    .await?;
            }
            let (node, _) = self.cluster.leader(topic, partition)?;
            self.connect_node(node).await?;
            let version = self
                .versions
                .get(&LIST_OFFSETS)
                .and_then(|v| pick_version(v.min_version, v.max_version, 1, 10))
                .ok_or_else(|| Error::Unsupported("broker does not support ListOffsets".into()))?;
            let isolation = self.cfg.isolation_level.as_i8();
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
                            duration_millis_i32(timeout),
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
                Ok(got) => return Ok((got.offset, got.timestamp, got.leader_epoch)),
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
                    self.refresh_metadata_timeout(Some(&topics), timeout)
                        .await?;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Set the next fetch offset for an assigned partition (Java
    /// `seek(TopicPartition, long)`).
    ///
    /// A negative offset is Java `IllegalArgumentException` (`seek offset
    /// must not be a negative number`). An unassigned partition is Java
    /// `IllegalStateException` (`No current assignment for partition`).
    /// Clears Fetch `LastFetchedEpoch` (KIP-320). To keep a leader epoch,
    /// use [`Self::seek_with_metadata`].
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.seek_with_epoch(
            topic,
            partition,
            offset,
            crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
        )
    }

    /// [`Self::seek`] for a [`TopicPartition`].
    pub fn seek_to(&mut self, partition: impl Into<TopicPartition>, offset: i64) -> Result<()> {
        let tp = partition.into();
        self.seek(&tp.topic, tp.partition, offset)
    }

    /// Seek using [`OffsetAndMetadata`] (Java `seek(TopicPartition, OffsetAndMetadata)`).
    ///
    /// The offset is the next fetch position. A negative offset and an
    /// unassigned partition use the same Java messages as [`Self::seek`].
    /// The leader epoch is sent as Fetch `LastFetchedEpoch` (KIP-320).
    /// Unknown epoch (`None`) clears it, matching Java `Optional.empty()`.
    /// The metadata string is ignored (Java does the same).
    pub fn seek_with_metadata(
        &mut self,
        partition: impl Into<TopicPartition>,
        offset: impl Into<OffsetAndMetadata>,
    ) -> Result<()> {
        let tp = partition.into();
        let md = offset.into();
        self.seek_with_epoch(&tp.topic, tp.partition, md.offset, md.wire_epoch())
    }

    fn seek_with_epoch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        last_fetched_epoch: i32,
    ) -> Result<()> {
        if offset < 0 {
            return Err(Error::protocol("seek offset must not be a negative number"));
        }
        if let Some(slot) = self
            .assigned
            .iter_mut()
            .find(|(t, p, _)| t == topic && *p == partition)
        {
            slot.2 = offset;
            self.set_last_fetched_epoch(topic, partition, last_fetched_epoch);
            self.drop_pending_for(topic, partition);
            return Ok(());
        }
        Err(reject_java_no_current_assignment(topic, partition))
    }

    /// Seek every assigned partition to the log start (`ListOffsets` earliest).
    pub async fn seek_to_beginning(&mut self) -> Result<()> {
        self.seek_assigned(crate::EARLIEST_TIMESTAMP).await
    }

    /// Seek these assigned partitions to the log start (Java `seekToBeginning`).
    pub async fn seek_to_beginning_of(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<()> {
        self.seek_partitions(crate::EARLIEST_TIMESTAMP, partitions)
            .await
    }

    /// Seek every assigned partition to the high watermark (`ListOffsets` latest).
    pub async fn seek_to_end(&mut self) -> Result<()> {
        self.seek_assigned(crate::LATEST_TIMESTAMP).await
    }

    /// Seek these assigned partitions to the high watermark (Java `seekToEnd`).
    pub async fn seek_to_end_of(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<()> {
        self.seek_partitions(crate::LATEST_TIMESTAMP, partitions)
            .await
    }

    async fn seek_assigned(&mut self, timestamp: i64) -> Result<()> {
        let assigned: Vec<(String, i32)> = self
            .assigned
            .iter()
            .map(|(t, p, _)| (t.clone(), *p))
            .collect();
        self.seek_listed(timestamp, assigned).await
    }

    async fn seek_partitions(
        &mut self,
        timestamp: i64,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<()> {
        let listed: Vec<(String, i32)> = partitions
            .into_iter()
            .map(|p| {
                let tp = p.into();
                (tp.topic, tp.partition)
            })
            .collect();
        self.seek_listed(timestamp, listed).await
    }

    async fn seek_listed(&mut self, timestamp: i64, partitions: Vec<(String, i32)>) -> Result<()> {
        for (topic, partition) in partitions {
            let offset = self
                .list_offsets(topic.clone(), partition, timestamp)
                .await?;
            self.seek(&topic, partition, offset)?;
        }
        Ok(())
    }

    /// Partition metadata for `topic` (Java `partitionsFor`: leader, replicas, ISR, offline replicas, leader epoch).
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::partitions_for_timeout`].
    pub async fn partitions_for(&mut self, topic: impl Into<String>) -> Result<Vec<PartitionInfo>> {
        let timeout = self.cfg.request_timeout;
        self.partitions_for_timeout(topic, timeout).await
    }

    /// [`Self::partitions_for`] with a one-shot timeout (Java `partitionsFor(String, Duration)`).
    pub async fn partitions_for_timeout(
        &mut self,
        topic: impl Into<String>,
        timeout: Duration,
    ) -> Result<Vec<PartitionInfo>> {
        let topic = topic.into();
        self.refresh_metadata_timeout(Some(std::slice::from_ref(&topic)), timeout)
            .await?;
        let infos = self.partition_infos(Some(topic.as_str()))?;
        if infos.is_empty() {
            return Err(Error::UnknownTopic(topic));
        }
        Ok(infos)
    }

    /// Cluster Metadata for every topic (Java `listTopics`).
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::list_topics_timeout`].
    pub async fn list_topics(&mut self) -> Result<Vec<PartitionInfo>> {
        let timeout = self.cfg.request_timeout;
        self.list_topics_timeout(timeout).await
    }

    /// [`Self::list_topics`] with a one-shot timeout (Java `listTopics(Duration)`).
    pub async fn list_topics_timeout(&mut self, timeout: Duration) -> Result<Vec<PartitionInfo>> {
        self.refresh_metadata_timeout(None, timeout).await?;
        self.partition_infos(None)
    }

    fn partition_infos(&self, only: Option<&str>) -> Result<Vec<PartitionInfo>> {
        match &self.metadata {
            Some(md) => partition_infos_from(md, only),
            None => Ok(Vec::new()),
        }
    }

    /// Log-start offset for each partition (`ListOffsets` earliest).
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::beginning_offsets_timeout`].
    pub async fn beginning_offsets(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        let timeout = self.cfg.request_timeout;
        self.beginning_offsets_timeout(partitions, timeout).await
    }

    /// [`Self::beginning_offsets`] with a one-shot timeout
    /// (Java `beginningOffsets(Collection, Duration)`).
    pub async fn beginning_offsets_timeout(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        self.offsets_at(partitions, crate::EARLIEST_TIMESTAMP, timeout)
            .await
    }

    /// High-watermark offset for each partition (`ListOffsets` latest).
    ///
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::end_offsets_timeout`].
    pub async fn end_offsets(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        let timeout = self.cfg.request_timeout;
        self.end_offsets_timeout(partitions, timeout).await
    }

    /// [`Self::end_offsets`] with a one-shot timeout
    /// (Java `endOffsets(Collection, Duration)`).
    pub async fn end_offsets_timeout(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        self.offsets_at(partitions, crate::LATEST_TIMESTAMP, timeout)
            .await
    }

    /// High watermark minus position (Java `currentLag`).
    ///
    /// `None` when the high watermark is unknown (`-1`). An unassigned
    /// partition is Java `IllegalStateException` (`No current assignment
    /// for partition`).
    pub async fn current_lag(
        &mut self,
        partition: impl Into<TopicPartition>,
    ) -> Result<Option<i64>> {
        let timeout = self.cfg.request_timeout;
        self.current_lag_timeout(partition, timeout).await
    }

    /// [`Self::current_lag`] with a one-shot timeout for the ListOffsets RPC.
    ///
    /// An unassigned partition is the same Java `IllegalStateException` as
    /// [`Self::current_lag`].
    pub async fn current_lag_timeout(
        &mut self,
        partition: impl Into<TopicPartition>,
        timeout: Duration,
    ) -> Result<Option<i64>> {
        let tp = partition.into();
        let pos = self
            .assigned
            .iter()
            .find(|(t, p, _)| t == &tp.topic && *p == tp.partition)
            .map(|(_, _, o)| *o)
            .ok_or_else(|| reject_java_no_current_assignment(&tp.topic, tp.partition))?;
        let hw = self
            .list_offsets_timeout(
                tp.topic.clone(),
                tp.partition,
                crate::LATEST_TIMESTAMP,
                timeout,
            )
            .await?;
        if hw < 0 {
            Ok(None)
        } else {
            Ok(Some(hw - pos))
        }
    }

    /// First offset at or after each timestamp (Java `offsetsForTimes`).
    ///
    /// A negative timestamp is Java `IllegalArgumentException`
    /// (`The target time cannot be negative`). Use [`Self::beginning_offsets`]
    /// / [`Self::end_offsets`] (or [`Self::list_offsets`] with
    /// [`crate::EARLIEST_TIMESTAMP`] / [`crate::LATEST_TIMESTAMP`]) for
    /// those sentinels. Partitions with no matching record return `None`.
    /// [`OffsetAndTimestamp::leader_epoch`] is Java `getLeaderEpoch`.
    /// Waits up to [`ConsumerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::offsets_for_times_timeout`].
    pub async fn offsets_for_times(
        &mut self,
        queries: impl IntoIterator<Item = (impl Into<TopicPartition>, i64)>,
    ) -> Result<Vec<(TopicPartition, Option<OffsetAndTimestamp>)>> {
        let timeout = self.cfg.request_timeout;
        self.offsets_for_times_timeout(queries, timeout).await
    }

    /// [`Self::offsets_for_times`] with a one-shot timeout
    /// (Java `offsetsForTimes(Map, Duration)`).
    pub async fn offsets_for_times_timeout(
        &mut self,
        queries: impl IntoIterator<Item = (impl Into<TopicPartition>, i64)>,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, Option<OffsetAndTimestamp>)>> {
        let queries: Vec<(TopicPartition, i64)> = queries
            .into_iter()
            .map(|(tp, timestamp)| (tp.into(), timestamp))
            .collect();
        for (tp, timestamp) in &queries {
            if *timestamp < 0 {
                return Err(Error::protocol(format!(
                    "The target time for partition {tp} is {timestamp}. The target time cannot be negative."
                )));
            }
        }
        let mut out = Vec::new();
        for (tp, timestamp) in queries {
            let (offset, ts, epoch) = self
                .list_offset_at(&tp.topic, tp.partition, timestamp, timeout)
                .await?;
            let found = if offset < 0 {
                None
            } else {
                Some(OffsetAndTimestamp {
                    offset,
                    timestamp: ts,
                    leader_epoch: (epoch >= 0).then_some(epoch),
                })
            };
            out.push((tp, found));
        }
        Ok(out)
    }

    async fn offsets_at(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
        timestamp: i64,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartition, i64)>> {
        let mut out = Vec::new();
        for p in partitions {
            let tp = p.into();
            let offset = self
                .list_offsets_timeout(tp.topic.clone(), tp.partition, timestamp, timeout)
                .await?;
            out.push((tp, offset));
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FetchRetry {
    None,
    /// Preferred replica (KIP-392), applied CurrentLeader, or DivergingEpoch
    /// seek. Retry Fetch immediately without Metadata.
    Redirect,
    /// Retriable broker / IO error. Wait `retry.backoff.ms`.
    Backoff,
}

impl FetchRetry {
    fn should_retry(self) -> bool {
        self != Self::None
    }

    fn needs_backoff(self) -> bool {
        self == Self::Backoff
    }

    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Backoff, _) | (_, Self::Backoff) => Self::Backoff,
            (Self::Redirect, _) | (_, Self::Redirect) => Self::Redirect,
            (Self::None, Self::None) => Self::None,
        }
    }
}

fn fetch_topics(
    by_topic: HashMap<String, Vec<FetchPartition>>,
    name_ids: &HashMap<String, [u8; 16]>,
) -> Vec<FetchTopic> {
    by_topic
        .into_iter()
        .map(|(topic, partitions)| FetchTopic {
            topic_id: name_ids.get(&topic).copied().unwrap_or([0u8; 16]),
            topic,
            partitions,
        })
        .collect()
}

fn offset_for_leader_epoch_topics(
    cluster: &Cluster,
    parts: &[(String, i32)],
) -> Vec<OffsetForLeaderTopic> {
    let mut by_topic: HashMap<String, Vec<OffsetForLeaderPartition>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for (topic, partition) in parts {
        let current = cluster.leader_epoch(topic, *partition);
        let part = OffsetForLeaderPartition::new(*partition, current, current);
        match by_topic.entry(topic.clone()) {
            Entry::Vacant(v) => {
                order.push(topic.clone());
                let _ = v.insert(vec![part]);
            }
            Entry::Occupied(mut o) => o.get_mut().push(part),
        }
    }
    order
        .into_iter()
        .map(|name| {
            let mut partitions = by_topic.remove(&name).unwrap_or_default();
            partitions.sort_by_key(|p| p.partition);
            OffsetForLeaderTopic::new(name, partitions)
        })
        .collect()
}

fn fetched_bytes(rec: &FetchedRecord) -> u64 {
    let k = rec.key.as_ref().map(Bytes::len).unwrap_or(0);
    let v = rec.value.as_ref().map(Bytes::len).unwrap_or(0);
    u64::try_from(k.saturating_add(v)).unwrap_or(u64::MAX)
}

pub(crate) fn duration_millis_i32(d: Duration) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(i32::MAX).max(0)
}

/// Java `KafkaConsumer.poll` when `SubscriptionState.hasNoSubscriptionOrUserAssignment`.
pub(crate) fn reject_java_no_subscription_or_assignment() -> Error {
    Error::protocol("Consumer is not subscribed to any topics or assigned any partitions")
}

/// Java `KafkaConsumer.position` when the partition is not assigned.
fn reject_java_position_unassigned() -> Error {
    Error::protocol("You can only check the position for partitions assigned to this consumer.")
}

/// Java `SubscriptionState.assignedState` (seek / `currentLag`).
fn reject_java_no_current_assignment(topic: &str, partition: i32) -> Error {
    Error::protocol(format!(
        "No current assignment for partition {topic}-{partition}"
    ))
}

pub(crate) fn partition_infos_from(
    md: &MetadataResponse,
    only: Option<&str>,
) -> Result<Vec<PartitionInfo>> {
    let mut out = Vec::new();
    for tmd in &md.topics {
        let Some(name) = tmd.name.as_ref() else {
            continue;
        };
        if let Some(only) = only {
            if name != only {
                continue;
            }
        }
        if tmd.error_code != 0 {
            return Err(Error::broker(tmd.error_code, name.clone()));
        }
        for p in &tmd.partitions {
            out.push(PartitionInfo::from_partition_metadata(name.as_str(), p));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(topic: &str, partition: i32, offset: i64) -> FetchedRecord {
        FetchedRecord {
            topic: topic.into(),
            partition,
            offset,
            timestamp: 0,
            timestamp_type: TimestampType::CreateTime,
            key: None,
            value: None,
            headers: Vec::new(),
            leader_epoch: None,
        }
    }

    #[test]
    fn consumer_records_partitions_and_filters() {
        let mut last_a0 = rec("a", 0, 3);
        last_a0.leader_epoch = Some(7);
        let recs = ConsumerRecords::from(vec![
            rec("a", 0, 1),
            rec("a", 1, 2),
            last_a0,
            rec("b", 0, 4),
        ]);
        assert_eq!(recs.count(), 4);
        assert_eq!(recs.len(), 4);
        assert!(!recs.is_empty());
        assert!(ConsumerRecords::empty().is_empty());
        assert_eq!(ConsumerRecords::empty().count(), 0);
        assert_eq!(
            recs.partitions(),
            vec![
                TopicPartition::new("a", 0),
                TopicPartition::new("a", 1),
                TopicPartition::new("b", 0),
            ]
        );
        let a0: Vec<_> = recs.records(("a", 0)).map(|r| r.offset).collect();
        assert_eq!(a0, vec![1, 3]);
        assert_eq!(recs.records_for_topic("a").count(), 3);
        assert_eq!(recs.records_for_topic("missing").count(), 0);
        let via_ref: Vec<_> = (&recs).into_iter().map(|r| r.offset).collect();
        assert_eq!(via_ref, vec![1, 2, 3, 4]);
        assert_eq!(
            recs.next_offsets(),
            vec![
                (
                    TopicPartition::new("a", 0),
                    OffsetAndMetadata::new(4).with_leader_epoch(7)
                ),
                (TopicPartition::new("a", 1), OffsetAndMetadata::new(3),),
                (TopicPartition::new("b", 0), OffsetAndMetadata::new(5),),
            ]
        );
    }

    #[test]
    fn java_core_type_getters_match_fields() {
        let tp = TopicPartition::new("events", 3);
        assert_eq!(tp.topic(), "events");
        assert_eq!(tp.partition(), 3);
        let cluster_md = TopicPartition::cluster_metadata();
        assert_eq!(cluster_md.topic(), Topic::CLUSTER_METADATA_TOPIC_NAME);
        assert_eq!(cluster_md.partition(), 0);
        assert_eq!(cluster_md.to_string(), "__cluster_metadata-0");
        let tid = TopicIdPartition::from_topic(crate::Uuid::ONE, 3, "events");
        assert_eq!(tid.topic_id(), crate::Uuid::ONE);
        assert_eq!(tid.topic(), "events");
        assert_eq!(tid.partition(), 3);
        assert_eq!(tid.topic_partition(), &tp);
        assert_eq!(
            TopicIdPartition::new(crate::Uuid::ONE, tp.clone()).to_string(),
            "AAAAAAAAAAAAAAAAAAAAAQ:events-3"
        );
        assert_eq!(tid.to_string(), "AAAAAAAAAAAAAAAAAAAAAQ:events-3");
        let committed = OffsetAndMetadata::with_metadata(9, "meta").with_leader_epoch(2);
        assert_eq!(committed.offset(), 9);
        assert_eq!(committed.leader_epoch(), Some(2));
        assert_eq!(committed.metadata(), "meta");
        assert_eq!(
            committed.to_string(),
            "OffsetAndMetadata{offset=9, leaderEpoch=2, metadata='meta'}"
        );
        assert_eq!(
            OffsetAndMetadata::new(1).to_string(),
            "OffsetAndMetadata{offset=1, leaderEpoch=null, metadata=''}"
        );
        assert_eq!(OffsetAndMetadata::NO_METADATA, "");
        assert_eq!(OffsetAndMetadata::INVALID_OFFSET, -1);
        assert_eq!(
            OffsetAndMetadata::new(1).metadata(),
            OffsetAndMetadata::NO_METADATA
        );
        let listed = OffsetAndTimestamp::new(5, 1_700_000_000_000).with_leader_epoch(4);
        assert_eq!(listed.offset(), 5);
        assert_eq!(listed.timestamp(), 1_700_000_000_000);
        assert_eq!(listed.leader_epoch(), Some(4));
        assert_eq!(
            listed.to_string(),
            "(timestamp=1700000000000, leaderEpoch=4, offset=5)"
        );
        assert_eq!(
            OffsetAndTimestamp::new(1, 2).to_string(),
            "(timestamp=2, leaderEpoch=null, offset=1)"
        );
        assert_eq!(OffsetAndTimestamp::UNKNOWN_OFFSET, -1);
        assert_eq!(OffsetAndTimestamp::UNKNOWN_TIMESTAMP, -1);
        let info = PartitionInfo {
            topic: "t".into(),
            partition: 1,
            leader: 2,
            leader_epoch: 8,
            replicas: vec![2, 3],
            isr: vec![2],
            offline_replicas: vec![3],
        };
        assert_eq!(info.topic(), "t");
        assert_eq!(info.partition(), 1);
        assert_eq!(info.leader(), 2);
        assert_eq!(info.leader_epoch(), 8);
        assert_eq!(info.replicas(), &[2, 3]);
        assert_eq!(info.isr(), &[2]);
        assert_eq!(info.offline_replicas(), &[3]);
        assert_eq!(
            info.to_string(),
            "Partition(topic = t, partition = 1, leader = 2, replicas = [2,3], isr = [2], offlineReplicas = [3])"
        );
        let md = PartitionMetadata {
            error_code: 0,
            partition_index: 1,
            leader_id: 2,
            leader_epoch: 8,
            replica_nodes: vec![2, 3],
            isr_nodes: vec![2],
            offline_replicas: vec![3],
        };
        assert_eq!(PartitionInfo::from_partition_metadata("t", &md), info);
        assert_eq!(
            PartitionInfo::from_partition_metadata("t", &md.without_leader_epoch()).leader_epoch(),
            crate::RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        let no_leader = PartitionInfo {
            topic: "t".into(),
            partition: 0,
            leader: MetadataResponse::NO_LEADER_ID,
            leader_epoch: crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
            replicas: Vec::new(),
            isr: Vec::new(),
            offline_replicas: Vec::new(),
        };
        assert_eq!(no_leader.leader(), MetadataResponse::NO_LEADER_ID);
        assert_eq!(
            no_leader.leader_epoch(),
            crate::RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(
            no_leader.to_string(),
            "Partition(topic = t, partition = 0, leader = none, replicas = [], isr = [], offlineReplicas = [])"
        );
        let rec = rec("t", 0, 11);
        assert_eq!(rec.topic(), "t");
        assert_eq!(rec.partition(), 0);
        assert_eq!(rec.offset(), 11);
        assert_eq!(rec.timestamp(), 0);
        assert_eq!(rec.timestamp_type(), TimestampType::CreateTime);
        assert!(rec.key().is_none());
        assert!(rec.value().is_none());
        assert!(rec.headers().is_empty());
        assert!(rec.last_header("k").is_none());
        assert_eq!(rec.headers_for_key("k").count(), 0);
        assert!(rec.leader_epoch().is_none());
        assert_eq!(
            FetchedRecord::NO_TIMESTAMP,
            crate::RecordBatch::NO_TIMESTAMP
        );
        assert_eq!(FetchedRecord::NULL_SIZE, -1);
        assert_eq!(rec.serialized_key_size(), FetchedRecord::NULL_SIZE);
        assert_eq!(rec.serialized_value_size(), FetchedRecord::NULL_SIZE);
        assert_eq!(
            rec.to_string(),
            "ConsumerRecord(topic = t, partition = 0, leaderEpoch = null, offset = 11, CreateTime = 0, deliveryCount = null, serialized key size = -1, serialized value size = -1, headers = RecordHeaders(headers = [], isReadOnly = true), key = null, value = null)"
        );
    }
}
