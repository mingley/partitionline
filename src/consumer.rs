//! Fetch client with manual partition assignment.

use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
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
    decode_metadata_response, encode_metadata_request, ApiVersion, ApiVersionsResponse,
    MetadataResponse, PartitionMetadata,
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
    decode_fetch_response_raw, encode_fetch_request_with_forgotten, FetchMetadata, FetchPartition,
    FetchTopic, FetchedTopic, ForgottenTopic, FETCH_CRATE_MAX_VERSION, INVALID_LOG_START_OFFSET,
};
use crate::protocol::group::Topic;
use crate::protocol::offsets::{decode_list_offsets_topics_response, encode_list_offsets_request};
use crate::protocol::records::{
    split_record_batches, write_java_optional, write_java_optional_bytes,
    write_java_record_headers, EncodedBatch, Header, TimestampType,
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
    /// Kafka `buffer.memory` equivalent for consumer fetch buffering.
    ///
    /// Upper bound on prefetched record bytes (key + value + headers) across all
    /// partitions and brokers. When records in the pending queue reach or exceed
    /// this limit, the consumer stops prefetching more bytes from brokers until
    /// application consumption drains the buffer below the budget.
    ///
    /// Default 32 MiB (matching producer `buffer_memory`). Zero means no aggregate cap.
    pub buffer_memory: usize,
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
            .field("buffer_memory", &self.buffer_memory)
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
            buffer_memory: 32 * 1024 * 1024,
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

    /// Kafka `max.poll.records`. `None` means no cap. `0` is rejected when
    /// the consumer starts; Java requires at least one record.
    #[must_use]
    pub fn max_poll_records(mut self, n: usize) -> Self {
        self.max_poll_records = Some(n);
        self
    }

    /// Kafka `buffer.memory` equivalent for consumer fetch buffering.
    ///
    /// Upper bound on prefetched record bytes (key + value + headers) across all
    /// partitions and brokers. Default 32 MiB (matching producer `buffer_memory`).
    /// Zero means no aggregate cap.
    #[must_use]
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = bytes;
        self
    }

    /// Fetch buffer budget alias for [`Self::buffer_memory`].
    #[must_use]
    pub fn fetch_buffer_bytes(mut self, bytes: usize) -> Self {
        self.buffer_memory = bytes;
        self
    }

    /// Fetch buffer memory alias for [`Self::buffer_memory`].
    #[must_use]
    pub fn fetch_buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = bytes;
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
    ///
    /// When [`Self::auto_commit`] is on, the revoked partitions' delivered
    /// positions are committed before this callback runs, and their buffered
    /// records are dropped. Retained partitions keep their positions and
    /// buffered records. With auto-commit off, nothing is committed for the
    /// revoked partitions; commit explicitly before the transfer if the next
    /// owner must not replay delivered work.
    #[must_use]
    pub fn on_rebalance(
        mut self,
        f: impl Fn(&[TopicPartition], &[TopicPartition]) + Send + Sync + 'static,
    ) -> Self {
        self.rebalance = RebalanceListener::from_fn(f);
        self
    }

    /// Kafka `enable.auto.commit`. Off by default.
    ///
    /// On covers the poll-interval commit of delivered positions and the
    /// rebalance commit of revoked partitions' delivered positions (see
    /// [`Self::on_rebalance`]). `leave`, `close` and `unsubscribe` never
    /// auto-commit.
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

/// Topic name stored beside the record when it fits in 23 bytes.
///
/// Kafka topic names are at most 249 bytes. Names that fit inline clone
/// without a heap allocation. Longer names share one heap allocation.
#[derive(Clone, Debug)]
pub enum TopicName {
    /// UTF-8 bytes and their length, at most 23.
    Inline([u8; 23], u8),
    /// Topic longer than 23 bytes.
    Heap(Arc<str>),
}

impl TopicName {
    /// Borrow the topic as UTF-8.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Inline(buf, len) => {
                let end = usize::from(*len);
                buf.get(..end)
                    .and_then(|bytes| std::str::from_utf8(bytes).ok())
                    .unwrap_or("")
            }
            Self::Heap(topic) => topic.as_ref(),
        }
    }
}

impl From<&str> for TopicName {
    fn from(value: &str) -> Self {
        let bytes = value.as_bytes();
        if let Some(len) = u8::try_from(bytes.len()).ok().filter(|len| *len <= 23) {
            let mut buf = [0u8; 23];
            if let Some(slot) = buf.get_mut(..bytes.len()) {
                slot.copy_from_slice(bytes);
            }
            Self::Inline(buf, len)
        } else {
            Self::Heap(Arc::from(value))
        }
    }
}

impl From<String> for TopicName {
    fn from(value: String) -> Self {
        if value.len() <= 23 {
            Self::from(value.as_str())
        } else {
            Self::Heap(Arc::from(value))
        }
    }
}

impl From<&String> for TopicName {
    fn from(value: &String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<TopicName> for String {
    fn from(value: TopicName) -> Self {
        match value {
            TopicName::Heap(topic) => topic.as_ref().to_owned(),
            inline => inline.as_str().to_owned(),
        }
    }
}

impl std::ops::Deref for TopicName {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq for TopicName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for TopicName {}

impl PartialOrd for TopicName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopicName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl std::hash::Hash for TopicName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl PartialEq<str> for TopicName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for TopicName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for TopicName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<TopicName> for String {
    fn eq(&self, other: &TopicName) -> bool {
        self.as_str() == other.as_str()
    }
}

/// Key or value bytes. Values up to 24 bytes live in the record.
const RECORD_BYTES_INLINE: usize = 24;

/// Bytes stored inline when they fit in 24 bytes, otherwise a shared buffer.
#[derive(Clone, Debug)]
pub enum RecordBytes {
    /// Inline payload and its length.
    Short([u8; RECORD_BYTES_INLINE], u8),
    /// Longer payload, shared with the fetched batch.
    Long(Bytes),
}

impl RecordBytes {
    /// Borrow the bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Short(buf, len) => buf.get(..usize::from(*len)).unwrap_or(&[]),
            Self::Long(bytes) => bytes.as_ref(),
        }
    }
}

impl std::ops::Deref for RecordBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<&[u8]> for RecordBytes {
    fn from(value: &[u8]) -> Self {
        if value.len() <= RECORD_BYTES_INLINE {
            let mut buf = [0u8; RECORD_BYTES_INLINE];
            if let Some(slot) = buf.get_mut(..value.len()) {
                slot.copy_from_slice(value);
            }
            Self::Short(buf, u8::try_from(value.len()).unwrap_or(0))
        } else {
            Self::Long(Bytes::copy_from_slice(value))
        }
    }
}

impl From<Bytes> for RecordBytes {
    fn from(value: Bytes) -> Self {
        if value.len() <= RECORD_BYTES_INLINE {
            Self::from(value.as_ref())
        } else {
            Self::Long(value)
        }
    }
}

impl PartialEq for RecordBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for RecordBytes {}

impl PartialEq<Bytes> for RecordBytes {
    fn eq(&self, other: &Bytes) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl PartialEq<RecordBytes> for Bytes {
    fn eq(&self, other: &RecordBytes) -> bool {
        self.as_ref() == other.as_slice()
    }
}

/// One record from Fetch.
#[derive(Debug, Clone)]
pub struct FetchedRecord {
    /// Topic name.
    pub topic: TopicName,
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// Java `ConsumerRecord.timestampType`.
    pub timestamp_type: TimestampType,
    /// Optional key.
    pub key: Option<RecordBytes>,
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
        serialized_bytes_size(self.key.as_ref().map(|key| key.len()))
    }

    /// Serialized value size in bytes, or [`Self::NULL_SIZE`] if there is no value
    /// (Java `serializedValueSize`).
    #[must_use]
    pub fn serialized_value_size(&self) -> i32 {
        serialized_bytes_size(self.value.as_ref().map(Bytes::len))
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

fn serialized_bytes_size(len: Option<usize>) -> i32 {
    len.map(|n| i32::try_from(n).unwrap_or(i32::MAX))
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
            if seen.insert((rec.topic.as_str(), rec.partition)) {
                out.push(TopicPartition::new(rec.topic.clone(), rec.partition));
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
    /// order. A record at [`i64::MAX`] has no next offset, so that partition
    /// is omitted.
    #[must_use]
    pub fn next_offsets(&self) -> Vec<(TopicPartition, OffsetAndMetadata)> {
        let mut last: HashMap<(&str, i32), &FetchedRecord> = HashMap::new();
        let mut order = Vec::new();
        for rec in &self.records {
            let key = (rec.topic.as_str(), rec.partition);
            if last.insert(key, rec).is_none() {
                order.push(key);
            }
        }
        order
            .into_iter()
            .filter_map(|key| {
                last.remove(&key).and_then(|rec| {
                    let next = exclusive_fetch_offset(rec.offset)?;
                    let mut md = OffsetAndMetadata::new(next);
                    if let Some(epoch) = rec.leader_epoch {
                        md = md.with_leader_epoch(epoch);
                    }
                    Some((TopicPartition::new(rec.topic.clone(), rec.partition), md))
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

/// Partition state indexed by borrowed topic names. A topic is owned only when
/// its first partition is inserted, and empty topic buckets are removed.
#[derive(Clone)]
struct PartitionMap<V> {
    topics: HashMap<String, HashMap<i32, V>>,
}

impl<V> PartitionMap<V> {
    fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    fn get(&self, topic: &str, partition: i32) -> Option<&V> {
        self.topics.get(topic)?.get(&partition)
    }

    fn get_mut(&mut self, topic: &str, partition: i32) -> Option<&mut V> {
        self.topics.get_mut(topic)?.get_mut(&partition)
    }

    fn contains(&self, topic: &str, partition: i32) -> bool {
        self.get(topic, partition).is_some()
    }

    fn insert(&mut self, topic: &str, partition: i32, value: V) -> Option<V> {
        if let Some(partitions) = self.topics.get_mut(topic) {
            return partitions.insert(partition, value);
        }
        let mut partitions = HashMap::new();
        let previous = partitions.insert(partition, value);
        let _previous_topic = self.topics.insert(topic.to_owned(), partitions);
        previous
    }

    fn remove(&mut self, topic: &str, partition: i32) -> Option<V> {
        let partitions = self.topics.get_mut(topic)?;
        let removed = partitions.remove(&partition);
        if partitions.is_empty() {
            let _removed_topic = self.topics.remove(topic);
        }
        removed
    }

    fn clear(&mut self) {
        self.topics.clear();
    }

    fn entries(&self) -> Vec<(String, i32)> {
        let mut out = Vec::new();
        for (topic, partitions) in &self.topics {
            for partition in partitions.keys() {
                out.push((topic.clone(), *partition));
            }
        }
        out
    }

    fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }
}

/// Framed batches from one fetch response that the memory budget did not decode.
struct HeldFetch {
    last_stable_offset: i64,
    batches: VecDeque<EncodedBatch>,
}

/// A contiguous arrival run. Separate runs of the same partition can straddle
/// other partitions, so queue grouping alone cannot preserve delivery order.
struct PendingRun {
    topic: TopicName,
    partition: i32,
    count: usize,
}

struct PendingRecords {
    partitions: PartitionMap<VecDeque<FetchedRecord>>,
    order: VecDeque<PendingRun>,
}

impl PendingRecords {
    fn new() -> Self {
        Self {
            partitions: PartitionMap::new(),
            order: VecDeque::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    fn clear(&mut self) {
        self.partitions.clear();
        self.order.clear();
    }

    fn prepend(&mut self, records: Vec<FetchedRecord>) {
        if records.is_empty() {
            return;
        }
        let mut runs: VecDeque<PendingRun> = VecDeque::new();
        for rec in records.into_iter().rev() {
            if let Some(run) = runs
                .front_mut()
                .filter(|run| run.topic == rec.topic && run.partition == rec.partition)
            {
                run.count += 1;
            } else {
                runs.push_front(PendingRun {
                    topic: rec.topic.clone(),
                    partition: rec.partition,
                    count: 1,
                });
            }
            if let Some(queue) = self.partitions.get_mut(&rec.topic, rec.partition) {
                queue.push_front(rec);
            } else {
                let topic = rec.topic.clone();
                let _previous =
                    self.partitions
                        .insert(&topic, rec.partition, VecDeque::from([rec]));
            }
        }
        while let Some(run) = runs.pop_back() {
            self.order.push_front(run);
        }
    }

    fn extend(&mut self, records: Vec<FetchedRecord>) {
        for rec in records {
            if let Some(run) = self
                .order
                .back_mut()
                .filter(|run| run.topic == rec.topic && run.partition == rec.partition)
            {
                run.count += 1;
            } else {
                self.order.push_back(PendingRun {
                    topic: rec.topic.clone(),
                    partition: rec.partition,
                    count: 1,
                });
            }
            if let Some(queue) = self.partitions.get_mut(&rec.topic, rec.partition) {
                queue.push_back(rec);
            } else {
                let topic = rec.topic.clone();
                let _previous =
                    self.partitions
                        .insert(&topic, rec.partition, VecDeque::from([rec]));
            }
        }
    }

    fn drop_for(&mut self, topic: &str, partition: i32) -> usize {
        let removed = self.partitions.remove(topic, partition);
        self.order
            .retain(|run| run.topic != topic || run.partition != partition);
        removed.map_or(0, |queue| {
            queue
                .iter()
                .map(record_bytes)
                .fold(0, usize::saturating_add)
        })
    }

    fn retain_assigned(&mut self, assigned: &PartitionMap<()>) -> usize {
        let mut removed_bytes = 0usize;
        self.partitions.topics.retain(|topic, partitions| {
            partitions.retain(|partition, records| {
                if assigned.contains(topic, *partition) {
                    true
                } else {
                    removed_bytes = records
                        .iter()
                        .map(record_bytes)
                        .fold(removed_bytes, usize::saturating_add);
                    false
                }
            });
            !partitions.is_empty()
        });
        self.order
            .retain(|run| assigned.contains(&run.topic, run.partition));
        removed_bytes
    }

    fn drain(
        &mut self,
        paused: &PartitionMap<()>,
        cap: usize,
        buffered_bytes: &mut usize,
    ) -> Vec<FetchedRecord> {
        let mut out = Vec::new();
        let mut index = 0;
        while out.len() < cap {
            let Some(run) = self.order.get_mut(index) else {
                break;
            };
            if paused.contains(&run.topic, run.partition) {
                index += 1;
                continue;
            }
            if let Some(queue) = self.partitions.get_mut(&run.topic, run.partition) {
                let count = run.count.min(cap - out.len());
                for _ in 0..count {
                    if let Some(rec) = queue.pop_front() {
                        *buffered_bytes = buffered_bytes.saturating_sub(record_bytes(&rec));
                        out.push(rec);
                        run.count -= 1;
                    }
                }
                if queue.is_empty() {
                    let _removed = self.partitions.remove(&run.topic, run.partition);
                }
            }
            if run.count == 0 {
                let _removed = self.order.remove(index);
            } else {
                break;
            }
        }
        out
    }
}

/// Partition-local cursor over aborted starts. ABORT markers retire active
/// intervals, including those whose data and marker arrive in separate replies.
#[derive(Default)]
struct AbortedIntervals {
    pending: BinaryHeap<std::cmp::Reverse<(i64, i64)>>,
    active: HashMap<i64, i64>,
    completed: HashSet<(i64, i64)>,
}

impl AbortedIntervals {
    fn extend(&mut self, intervals: &[(i64, i64)]) {
        for &(pid, first) in intervals {
            if !self.completed.contains(&(pid, first))
                && self.active.get(&pid) != Some(&first)
                && !self.pending.iter().any(|entry| entry.0 == (first, pid))
            {
                self.pending.push(std::cmp::Reverse((first, pid)));
            }
        }
    }

    fn advance(&mut self, last: i64) {
        while let Some(&std::cmp::Reverse((first, pid))) = self.pending.peek() {
            if first > last {
                break;
            }
            let _pending = self.pending.pop();
            let _previous = self.active.insert(pid, first);
        }
    }

    fn abort(&mut self, pid: i64) {
        if let Some(first) = self.active.remove(&pid) {
            let _previous = self.completed.insert((pid, first));
        }
    }
}

/// Manual-assignment fetch client.
pub struct Consumer {
    cfg: ConsumerConfig,
    conn: BrokerConn,
    versions: HashMap<i16, ApiVersion>,
    metadata_version: i16,
    metadata: Option<MetadataResponse>,
    cluster: Cluster,
    conns: HashMap<i32, BrokerConn>,
    /// ListOffsets min/max negotiated for each dedicated leader socket.
    list_offsets_ranges: HashMap<i32, (i16, i16)>,
    /// OffsetForLeaderEpoch min/max negotiated for each dedicated leader socket.
    leader_epoch_ranges: HashMap<i32, (i16, i16)>,
    fetch_throttles: HashMap<i32, Instant>,
    fetch_sessions: HashMap<i32, BrokerFetchSession>,
    throttle_metrics: crate::metrics::ThrottleTracker,
    assigned: Vec<(Arc<str>, i32, i64)>,
    /// Positions in `assigned`. Cleared when membership changes.
    cursor_index: Option<CursorIndex>,
    /// Topic identity captured when each partition was assigned. Metadata
    /// refreshes must not bind delivered offsets to a recreated topic.
    assigned_topic_ids: HashMap<Arc<str>, HashMap<i32, [u8; 16]>>,
    /// Last consumed record-batch leader epoch (Fetch v12+ `LastFetchedEpoch`).
    last_fetched_epochs: PartitionMap<i32>,
    preferred: PartitionMap<i32>,
    paused: PartitionMap<()>,
    pending: PendingRecords,
    /// Framed batches already fetched that did not fit in `buffer_memory`.
    /// Decoded on a later poll instead of being requested again.
    retained: PartitionMap<HeldFetch>,
    retained_bytes: usize,
    buffered_bytes: usize,
    aborted: PartitionMap<AbortedIntervals>,
    m_fetch_rounds: AtomicU64,
    m_records: AtomicU64,
    m_bytes: AtomicU64,
    m_errors: AtomicU64,
    m_reconnects: AtomicU64,
    wakeup: Arc<AtomicBool>,
    wakeup_tx: watch::Sender<bool>,
    telemetry_version: Option<i16>,
    client_instance_id: Option<[u8; 16]>,
    m_fetch_latency: crate::metrics::LatencyTracker,
    topic_metrics: HashMap<Arc<str>, crate::metrics::FetchTopicTracker>,
    /// Key/value bytes already summed for records currently in `out`, by topic.
    ///
    /// Used when `fetch` returns that vector unchanged, so metrics do not walk
    /// every record again. Cleared when a later filter returns a subset.
    fetch_return_totals: HashMap<Arc<str>, (u64, u64)>,
    reconnect_fails: HashMap<i32, u32>,
    /// Leaders whose Fetch was sent and not yet applied. A multi-leader round
    /// returns after [`MULTI_LEADER_COLLECTION_WINDOW`] once any peer has
    /// responded, and these tasks finish on a later `fetch`.
    inflight_fetches: Option<tokio::task::JoinSet<(FetchReply, BrokerConn)>>,
    inflight_nodes: HashSet<i32>,
}

/// After one leader in a multi-leader round has responded, wait this long for
/// the others. A slower leader stays in flight and is applied on a later fetch.
pub(crate) const MULTI_LEADER_COLLECTION_WINDOW: Duration = Duration::from_millis(10);

/// How long to wait for the other leaders after the first fetch reply.
///
/// The window never extends past the fetch deadline.
pub(crate) fn multi_leader_grace(elapsed: Duration, until_deadline: Duration) -> Duration {
    MULTI_LEADER_COLLECTION_WINDOW
        .saturating_sub(elapsed)
        .min(until_deadline)
}

/// Preserve each peer's completion clock while collecting a concurrent round.
struct FetchReply {
    node: i32,
    version: i16,
    received_at: Instant,
    body: Result<Bytes>,
}

/// Aborts parked leader tasks if this round is cancelled before they are
/// stored back on the consumer.
struct InflightFetches {
    set: Option<tokio::task::JoinSet<(FetchReply, BrokerConn)>>,
    pending: HashSet<i32>,
}

impl Drop for InflightFetches {
    fn drop(&mut self) {
        if let Some(mut set) = self.set.take() {
            set.abort_all();
        }
    }
}

impl InflightFetches {
    fn park(mut self, consumer: &mut Consumer) {
        let set = self.set.take().unwrap_or_default();
        let pending = std::mem::take(&mut self.pending);
        if set.is_empty() {
            consumer.inflight_fetches = None;
            consumer.inflight_nodes.clear();
        } else {
            consumer.inflight_fetches = Some(set);
            consumer.inflight_nodes = pending;
        }
    }
}

/// Take fetch replies that have already finished. Does not wait.
fn reap_fetch_replies(
    set: &mut tokio::task::JoinSet<(FetchReply, BrokerConn)>,
    conns: &mut HashMap<i32, BrokerConn>,
) -> Result<Vec<FetchReply>> {
    let mut ready = Vec::new();
    while let Some(joined) = set.try_join_next() {
        let (reply, conn) =
            joined.map_err(|error| Error::protocol(format!("fetch task: {error}")))?;
        let _ = conns.insert(reply.node, conn);
        ready.push(reply);
    }
    ready.sort_by_key(|reply| reply.node);
    Ok(ready)
}

struct FetchBody {
    /// Partition metadata; `records` stay empty (see `batches`).
    topics: Vec<FetchedTopic>,
    /// Framed, CRC-checked batches per `[topic][partition]`, decoded only
    /// when applied so batches past the buffer budget are never inflated.
    batches: Vec<Vec<Vec<EncodedBatch>>>,
    endpoints: Vec<crate::protocol::api::NodeEndpoint>,
}

struct DecodedFetchReply {
    node: i32,
    body: Result<FetchBody>,
    recovery: Option<FetchRetry>,
}

type SessionParts = HashMap<i32, ([u8; 16], FetchPartition)>;

/// Position of each assigned partition in [`Consumer::assigned`].
type CursorIndex = HashMap<Arc<str>, HashMap<i32, usize>>;

/// A broker's cached request map is bounded by this consumer's active assignment.
/// Preparing marks the map uncertain until a response arrives, so cancellation
/// cannot reuse an epoch whose request may already have reached the broker.
struct BrokerFetchSession {
    version: Option<i16>,
    metadata: FetchMetadata,
    /// One topic name is shared by every partition of that topic.
    partitions: HashMap<Arc<str>, SessionParts>,
    awaiting_response: bool,
}

impl Default for BrokerFetchSession {
    fn default() -> Self {
        Self {
            version: None,
            metadata: FetchMetadata::INITIAL,
            partitions: HashMap::new(),
            awaiting_response: false,
        }
    }
}

struct SessionFetchRequest {
    metadata: FetchMetadata,
    topics: Vec<FetchTopic>,
    forgotten: Vec<ForgottenTopic>,
}

impl BrokerFetchSession {
    fn reset(&mut self) {
        self.metadata = self.metadata.next_close_existing_attempt_new();
        self.awaiting_response = false;
    }

    fn validate_partitions(&self, topics: &[FetchedTopic], version: i16, full: bool) -> Result<()> {
        let expected: HashSet<_> = self
            .partitions
            .iter()
            .flat_map(|(name, parts)| {
                parts
                    .keys()
                    .map(move |partition| (name.as_ref(), *partition))
            })
            .collect();
        let ids: HashMap<_, _> = self
            .partitions
            .iter()
            .flat_map(|(name, parts)| parts.values().map(move |(id, _)| (*id, name.as_ref())))
            .collect();
        let mut seen = HashSet::new();
        for topic in topics {
            let name = if version >= 13 {
                ids.get(&topic.topic_id).copied().ok_or_else(|| {
                    Error::protocol("Fetch session response has an unexpected topic ID")
                })?
            } else {
                topic.topic.as_str()
            };
            for partition in &topic.partitions {
                let key = (name, partition.partition);
                if !expected.contains(&key) || !seen.insert(key) {
                    return Err(Error::protocol(
                        "Fetch session response has an extra or duplicate partition",
                    ));
                }
            }
        }
        if full && seen != expected {
            return Err(Error::protocol(
                "Fetch full session response is missing partitions",
            ));
        }
        Ok(())
    }

    fn prepare(&mut self, topics: Vec<FetchTopic>, version: i16) -> SessionFetchRequest {
        if version < 7 {
            *self = Self::default();
            return SessionFetchRequest {
                metadata: FetchMetadata::LEGACY,
                topics,
                forgotten: Vec::new(),
            };
        }
        if self.awaiting_response || self.version != Some(version) {
            self.reset();
        }
        self.version = Some(version);
        let mut next: HashMap<Arc<str>, SessionParts> = HashMap::new();
        let mut changed: HashMap<String, Vec<FetchPartition>> = HashMap::new();
        let mut ids: HashMap<Arc<str>, [u8; 16]> = HashMap::new();
        for topic in &topics {
            let name = next
                .get_key_value(topic.topic.as_str())
                .or_else(|| self.partitions.get_key_value(topic.topic.as_str()))
                .map(|(existing, _)| Arc::clone(existing))
                .unwrap_or_else(|| Arc::<str>::from(topic.topic.as_str()));
            let _ = ids.insert(Arc::clone(&name), topic.topic_id);
            let slot = next.entry(name).or_default();
            for partition in &topic.partitions {
                let value = (topic.topic_id, partition.clone());
                let unchanged = self
                    .partitions
                    .get(topic.topic.as_str())
                    .and_then(|parts| parts.get(&partition.partition))
                    == Some(&value);
                if !self.metadata.is_full() && !unchanged {
                    if let Some(changed_slot) = changed.get_mut(topic.topic.as_str()) {
                        changed_slot.push(partition.clone());
                    } else {
                        let _ = changed.insert(topic.topic.clone(), vec![partition.clone()]);
                    }
                }
                let _ = slot.insert(partition.partition, value);
            }
        }
        let mut forgotten: HashMap<(Arc<str>, [u8; 16]), Vec<i32>> = HashMap::new();
        if !self.metadata.is_full() {
            for (topic, parts) in &self.partitions {
                for (partition, (id, _)) in parts {
                    if next
                        .get(topic.as_ref())
                        .and_then(|stored| stored.get(partition))
                        .is_none_or(|(next_id, _)| next_id != id)
                    {
                        forgotten
                            .entry((Arc::clone(topic), *id))
                            .or_default()
                            .push(*partition);
                    }
                }
            }
        }
        let topics = if self.metadata.is_full() {
            topics
        } else {
            fetch_topics(changed, &ids)
        };
        self.partitions = next;
        self.awaiting_response = true;
        let mut forgotten: Vec<_> = forgotten
            .into_iter()
            .map(|((topic, topic_id), mut partitions)| {
                partitions.sort_unstable();
                ForgottenTopic {
                    topic: topic.to_string(),
                    topic_id,
                    partitions,
                }
            })
            .collect();
        forgotten.sort_by(|a, b| (&a.topic, a.topic_id).cmp(&(&b.topic, b.topic_id)));
        SessionFetchRequest {
            metadata: self.metadata,
            topics,
            forgotten,
        }
    }

    fn response(&mut self, body: &Bytes, version: i16) -> Result<()> {
        if version < 7 {
            return Ok(());
        }
        let mut header = body.as_ref();
        let result = (|| {
            let _throttle = crate::protocol::buf::get_i32(&mut header)?;
            let code = crate::protocol::buf::get_i16(&mut header)?;
            let id = crate::protocol::buf::get_i32(&mut header)?;
            if code != 0 {
                if code == error::FETCH_SESSION_ID_NOT_FOUND {
                    self.metadata = FetchMetadata::INITIAL;
                }
                return Err(Error::broker(code, "Fetch session"));
            }
            if id < 0 || (!self.metadata.is_full() && id != 0 && id != self.metadata.session_id()) {
                return Err(Error::protocol(
                    "Fetch response has an invalid session identity",
                ));
            }
            self.metadata = if id == FetchMetadata::INVALID_SESSION_ID {
                FetchMetadata::INITIAL
            } else if self.metadata.is_full() {
                FetchMetadata::new_incremental(id)
            } else {
                self.metadata.next_incremental()
            };
            self.awaiting_response = false;
            Ok(())
        })();
        if result.is_err() {
            self.reset();
        }
        result
    }
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
        let _previous = self.tx.send_replace(true);
    }
}

/// Send Fetch epoch `-1` on `conn` and leave the socket with that response read.
async fn send_fetch_session_close(
    conn: &mut crate::net::BrokerConn,
    session: &BrokerFetchSession,
    throttle_until: Option<Instant>,
    wakeup_tx: &watch::Sender<bool>,
    cfg: &ConsumerConfig,
    deadline: Instant,
) -> Result<()> {
    if Instant::now() >= deadline || conn.fetch_version() < 7 {
        return Ok(());
    }
    let version = conn.fetch_version();
    let mut wakeup = wakeup_tx.subscribe();
    if let Some(until) = throttle_until {
        // Terminal requests still obey the broker's quota. Drop the close
        // when the mute outlives the remaining retirement budget.
        if until >= deadline {
            return Ok(());
        }
        tokio::select! {
            biased;
            result = wakeup.wait_for(|on| *on) => {
                drop(result);
                return Err(Error::Wakeup);
            }
            _ = tokio::time::sleep(until.saturating_duration_since(Instant::now())) => {}
        }
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Ok(());
    }
    let metadata = session.metadata.next_close_existing();
    let rack = cfg.rack.clone();
    let isolation = cfg.isolation_level.as_i8();
    let max_bytes = cfg.max_bytes;
    tokio::select! {
        biased;
        result = wakeup.wait_for(|on| *on) => {
            drop(result);
            Err(Error::Wakeup)
        }
        result = conn.roundtrip(FETCH, version, |buf| {
            encode_fetch_request_with_forgotten(
                buf,
                version,
                0,
                1,
                max_bytes,
                isolation,
                &[],
                rack.as_deref(),
                metadata,
                &[],
            )
        }, remaining) => {
            drop(result);
            Ok(())
        }
    }
}

fn startup_remaining(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(Error::Timeout)
    } else {
        Ok(remaining)
    }
}

impl Consumer {
    /// Connect with default config to one bootstrap server.
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(ConsumerConfig::bootstrap([bootstrap.into()])).await
    }

    /// Connect using `cfg`. Negotiates ApiVersions and optional SASL/TLS.
    pub async fn new(cfg: ConsumerConfig) -> Result<Self> {
        let deadline = Instant::now()
            .checked_add(cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        Self::connect_within(cfg, deadline).await
    }

    /// Connect using only the time still left on `deadline`.
    async fn open_one_bootstrap(
        cfg: &ConsumerConfig,
        addr: &str,
        deadline: Instant,
    ) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
        let mut conn = BrokerConn::connect_tls(
            addr,
            &cfg.client_id,
            cfg.connect_timeout.min(startup_remaining(deadline)?),
            cfg.tls.as_ref(),
        )
        .await?;
        let resp =
            crate::protocol::api::negotiate_api_versions(&mut conn, startup_remaining(deadline)?)
                .await?;
        sasl::apply_api_keys(&mut conn, &resp.api_keys);
        sasl::authenticate(
            &mut conn,
            cfg.sasl_plain.as_ref(),
            cfg.sasl_scram.as_ref(),
            cfg.sasl_scram_sha512.as_ref(),
            cfg.sasl_oauthbearer.as_deref(),
            cfg.sasl_oauthbearer_oidc.as_ref(),
            startup_remaining(deadline)?,
        )
        .await?;
        Ok((conn, resp))
    }

    async fn open_bootstrap(
        cfg: &ConsumerConfig,
        deadline: Instant,
    ) -> Result<(BrokerConn, crate::protocol::api::ApiVersionsResponse)> {
        if cfg.bootstrap.len() <= 1 {
            let addr = cfg
                .bootstrap
                .first()
                .ok_or_else(|| Error::protocol("all bootstrap servers failed"))?;
            return Self::open_one_bootstrap(cfg, addr, deadline).await;
        }
        let mut inflight = tokio::task::JoinSet::new();
        for addr in &cfg.bootstrap {
            let cfg = cfg.clone();
            let addr = addr.clone();
            let _ = inflight
                .spawn(async move { Consumer::open_one_bootstrap(&cfg, &addr, deadline).await });
        }
        let mut last = Error::protocol("all bootstrap servers failed");
        while let Some(joined) = inflight.join_next().await {
            match joined {
                Ok(Ok(opened)) => {
                    inflight.abort_all();
                    return Ok(opened);
                }
                Ok(Err(error)) => last = error,
                Err(error) => last = Error::protocol(format!("bootstrap handshake: {error}")),
            }
        }
        Err(last)
    }

    pub(crate) async fn connect_within(cfg: ConsumerConfig, deadline: Instant) -> Result<Self> {
        crate::net::Deadline::from_std(deadline)
            .run(Self::new_until(cfg, deadline))
            .await
    }

    async fn new_until(cfg: ConsumerConfig, deadline: Instant) -> Result<Self> {
        if cfg.bootstrap.len() > 16 || cfg.client_id.len() > i16::MAX as usize {
            return Err(Error::protocol(
                "bootstrap/client ID exceeds discovery limit",
            ));
        }
        if cfg.max_poll_records == Some(0) {
            return Err(Error::protocol("max.poll.records must be at least 1"));
        }
        let Ok(session_ms) = u128::try_from(cfg.session_timeout_ms) else {
            return Err(Error::protocol("session.timeout.ms must be at least 1"));
        };
        if session_ms == 0 {
            return Err(Error::protocol("session.timeout.ms must be at least 1"));
        }
        if cfg.heartbeat_interval.as_millis() >= session_ms {
            return Err(Error::protocol(
                "heartbeat.interval.ms must be lower than session.timeout.ms",
            ));
        }
        let mut cfg = cfg;
        for (field, value) in [
            ("max_wait_ms", cfg.max_wait_ms),
            ("min_bytes", cfg.min_bytes),
            ("max_bytes", cfg.max_bytes),
            ("max_partition_fetch_bytes", cfg.max_partition_fetch_bytes),
        ] {
            if value < 0 {
                return Err(Error::protocol(format!("{field} must be nonnegative")));
            }
        }
        let Ok(wait_ms) = u32::try_from(cfg.max_wait_ms) else {
            return Err(Error::protocol("max_wait_ms must be nonnegative"));
        };
        if u128::from(wait_ms) >= cfg.request_timeout.as_millis() {
            return Err(Error::protocol(
                "request.timeout.ms must be greater than fetch.max.wait.ms",
            ));
        }
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        let (mut conn, resp) = Self::open_bootstrap(&cfg, deadline).await?;
        let mut versions = HashMap::new();
        for api in resp.api_keys {
            let _prev = versions.insert(api.api_key, api);
        }
        // Best-effort: each leader connection negotiates its own Fetch
        // version in `open_node_conn`; the fetch path fails per-peer when a
        // leader cannot speak Fetch v4-18 (KL03-22, mirrors KL03-11).
        if let Some(fv) = versions
            .get(&FETCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 4, FETCH_CRATE_MAX_VERSION))
        {
            conn.set_fetch_version(fv);
        }
        let metadata_version = versions
            .get(&METADATA)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 13))
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        let telemetry_version = versions
            .get(&GET_TELEMETRY_SUBSCRIPTIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        Ok(Self {
            cfg,
            conn,
            versions,
            metadata_version,
            metadata: None,
            cluster: Cluster::default(),
            conns: HashMap::new(),
            list_offsets_ranges: HashMap::new(),
            leader_epoch_ranges: HashMap::new(),
            fetch_throttles: HashMap::new(),
            fetch_sessions: HashMap::new(),
            throttle_metrics: crate::metrics::ThrottleTracker::default(),
            assigned: Vec::new(),
            cursor_index: None,
            assigned_topic_ids: HashMap::new(),
            last_fetched_epochs: PartitionMap::new(),
            preferred: PartitionMap::new(),
            paused: PartitionMap::new(),
            pending: PendingRecords::new(),
            retained: PartitionMap::new(),
            retained_bytes: 0,
            buffered_bytes: 0,
            aborted: PartitionMap::new(),
            m_fetch_rounds: AtomicU64::new(0),
            m_records: AtomicU64::new(0),
            m_bytes: AtomicU64::new(0),
            m_errors: AtomicU64::new(0),
            m_reconnects: AtomicU64::new(0),
            wakeup: Arc::new(AtomicBool::new(false)),
            wakeup_tx: watch::channel(false).0,
            telemetry_version,
            client_instance_id: None,
            m_fetch_latency: crate::metrics::LatencyTracker::new(),
            topic_metrics: HashMap::new(),
            fetch_return_totals: HashMap::new(),
            reconnect_fails: HashMap::new(),
            inflight_fetches: None,
            inflight_nodes: HashSet::new(),
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
        let topic_name = topic.into();
        require_partition(partition)?;
        require_fetch_offset(offset)?;
        Topic::validate(&topic_name)?;
        self.reject_pending_wakeup()?;
        self.abort_inflight_fetches();
        self.refresh_metadata(Some(std::slice::from_ref(&topic_name)))
            .await?;
        self.drop_pending_for(&topic_name, partition);
        self.assigned
            .retain(|(t, p, _)| !(t.as_ref() == topic_name.as_str() && *p == partition));
        self.set_last_fetched_epoch(
            &topic_name,
            partition,
            crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
        );
        let topic: Arc<str> = Arc::from(topic_name);
        self.assigned.push((Arc::clone(&topic), partition, offset));
        self.cursor_index = None;
        if let Some(parts) = self.assigned_topic_ids.get_mut(topic.as_ref()) {
            let _previous = parts.remove(&partition);
        }
        self.capture_assigned_topic_ids();
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
        require_fetch_offset(offset)?;
        Topic::validate(&topic)?;
        self.reject_pending_wakeup()?;
        self.refresh_metadata(Some(std::slice::from_ref(&topic)))
            .await?;
        let Some(md) = self.metadata.as_ref() else {
            return Err(Error::UnknownTopic(topic));
        };
        let entries: Vec<(i16, Vec<i32>)> = md
            .topics
            .iter()
            .filter(|tmd| tmd.name.as_deref() == Some(topic.as_str()))
            .map(|tmd| {
                (
                    tmd.error_code,
                    tmd.partitions
                        .iter()
                        .map(|partition| partition.partition_index)
                        .collect(),
                )
            })
            .collect();
        let parts = match listed_partition_indexes(
            entries.iter().map(|(error, ids)| (*error, ids.as_slice())),
        ) {
            ListedPartitions::Indexes(indexes) if !indexes.is_empty() => indexes,
            ListedPartitions::Indexes(_) | ListedPartitions::Missing => {
                return Err(Error::UnknownTopic(topic));
            }
            ListedPartitions::Error(code) => return Err(Error::broker(code, topic)),
        };
        self.assigned
            .retain(|(t, _, _)| t.as_ref() != topic.as_str());
        let _removed = self.assigned_topic_ids.remove(topic.as_str());
        let name: Arc<str> = Arc::from(topic.as_str());
        for p in parts {
            self.drop_pending_for(&topic, p);
            self.set_last_fetched_epoch(&topic, p, crate::RecordBatch::NO_PARTITION_LEADER_EPOCH);
            self.assigned.push((Arc::clone(&name), p, offset));
        }
        self.cursor_index = None;
        self.capture_assigned_topic_ids();
        Ok(())
    }

    /// Assigned partitions (Java `assignment`). Offsets are [`Self::positions`].
    #[must_use]
    pub fn assignment(&self) -> Vec<TopicPartition> {
        self.assigned
            .iter()
            .map(|(t, p, _)| TopicPartition::new(t.as_ref(), *p))
            .collect()
    }

    /// Same as [`Self::assignment`].
    #[must_use]
    pub fn assigned_partitions(&self) -> Vec<TopicPartition> {
        self.assignment()
    }

    /// Assigned partitions with their next consumable offsets (delivered positions).
    ///
    /// When [`ConsumerConfig::max_poll_records`] leaves fetched records buffered,
    /// each partition reports its next undelivered record offset rather than the
    /// ahead-of-delivery broker fetch cursor, matching Java `position(TopicPartition)`.
    /// A partition waiting to be placed in the log is omitted.
    #[must_use]
    pub fn positions(&self) -> Vec<(TopicPartition, i64)> {
        self.assigned
            .iter()
            .filter_map(|(t, p, o)| {
                let pos = self.delivered_position(t, *p, *o);
                defined_position(pos).map(|pos| (TopicPartition::new(t.as_ref(), *p), pos))
            })
            .collect()
    }

    /// Broker fetch cursor (next offset to request from the broker) for an assigned partition.
    ///
    /// This may be ahead of [`Self::position`] when records are buffered in the consumer
    /// under [`ConsumerConfig::max_poll_records`]. A cursor below zero is waiting to be
    /// placed in the log and is not returned.
    pub fn fetch_cursor(&self, topic: &str, partition: i32) -> Result<i64> {
        let offset = self
            .assigned
            .iter()
            .find(|(t, p, _)| t.as_ref() == topic && *p == partition)
            .map(|(_, _, offset)| *offset)
            .ok_or_else(reject_java_position_unassigned)?;
        defined_position(offset).ok_or_else(|| {
            Error::protocol(format!(
                "fetch cursor for {topic}-{partition} is not in the log"
            ))
        })
    }

    /// Assigned partitions with their current broker fetch cursors.
    ///
    /// A partition waiting to be placed in the log is omitted.
    #[must_use]
    pub fn fetch_cursors(&self) -> Vec<(TopicPartition, i64)> {
        self.assigned
            .iter()
            .filter_map(|(t, p, o)| {
                defined_position(*o).map(|offset| (TopicPartition::new(t.as_ref(), *p), offset))
            })
            .collect()
    }

    /// Record bytes (keys, values, and headers) currently buffered in
    /// the pending-record queue.
    ///
    /// Process RSS is not bounded by this value.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    fn delivered_position(&self, topic: &str, partition: i32, fetch_cursor: i64) -> i64 {
        self.pending
            .partitions
            .get(topic, partition)
            .and_then(|records| records.front())
            .map_or(fetch_cursor, |record| record.offset)
    }

    pub(crate) fn assigned_offsets(&self) -> &[(Arc<str>, i32, i64)] {
        &self.assigned
    }

    pub(crate) fn leader_epoch(&self, topic: &str, partition: i32) -> i32 {
        self.cluster.leader_epoch(topic, partition)
    }

    /// Leader epoch for the next undelivered record, else the fetch cursor's epoch.
    pub(crate) fn position_epoch(&self, topic: &str, partition: i32) -> i32 {
        let pending = self
            .pending
            .partitions
            .get(topic, partition)
            .and_then(|records| records.front())
            .and_then(|record| record.leader_epoch);
        crate::group::commit_position_epoch(
            pending,
            self.last_fetched_epoch(topic, partition),
            self.leader_epoch(topic, partition),
        )
    }

    pub(crate) fn last_fetched_epoch(&self, topic: &str, partition: i32) -> i32 {
        self.last_fetched_epochs
            .get(topic, partition)
            .copied()
            .unwrap_or(crate::RecordBatch::NO_PARTITION_LEADER_EPOCH)
    }

    pub(crate) fn set_last_fetched_epoch(&mut self, topic: &str, partition: i32, epoch: i32) {
        if epoch >= 0 {
            let _prev = self.last_fetched_epochs.insert(topic, partition, epoch);
        } else {
            let _removed = self.last_fetched_epochs.remove(topic, partition);
        }
    }

    /// Next consumable offset (delivered position) for an assigned partition.
    ///
    /// If records have been fetched and buffered (for example under
    /// [`ConsumerConfig::max_poll_records`]), this returns the offset of the
    /// next undelivered record. If all fetched records have been delivered,
    /// this matches the broker fetch cursor.
    ///
    /// An unassigned partition is Java `IllegalStateException`
    /// (`You can only check the position for partitions assigned to this consumer.`).
    /// A position below zero is waiting to be placed in the log and is not returned.
    pub fn position(&self, topic: &str, partition: i32) -> Result<i64> {
        let fetch_offset = self
            .assigned
            .iter()
            .find(|(t, p, _)| t.as_ref() == topic && *p == partition)
            .map(|(_, _, o)| *o)
            .ok_or_else(reject_java_position_unassigned)?;
        let position = self.delivered_position(topic, partition, fetch_offset);
        defined_position(position).ok_or_else(|| {
            Error::protocol(format!(
                "position for {topic}-{partition} is not in the log"
            ))
        })
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
            let _inserted = self.paused.insert(&tp.topic, tp.partition, ());
        }
    }

    /// Undo [`pause`](Self::pause) for these partitions.
    pub fn resume(&mut self, partitions: impl IntoIterator<Item = impl Into<TopicPartition>>) {
        for p in partitions {
            let tp = p.into();
            let _removed = self.paused.remove(&tp.topic, tp.partition);
        }
    }

    /// Assigned partitions that [`fetch`](Self::fetch) currently skips.
    pub fn paused(&self) -> Vec<TopicPartition> {
        self.assigned
            .iter()
            .filter(|(t, p, _)| self.paused.contains(t.as_ref(), *p))
            .map(|(t, p, _)| TopicPartition::new(t.as_ref(), *p))
            .collect()
    }

    pub(crate) fn clear_assignment(&mut self) {
        self.abort_inflight_fetches();
        self.assigned.clear();
        self.cursor_index = None;
        self.assigned_topic_ids.clear();
        self.pending.clear();
        self.retained.clear();
        self.retained_bytes = 0;
        self.buffered_bytes = 0;
        self.last_fetched_epochs.clear();
        self.aborted.clear();
    }

    /// Replace the assignment. One Metadata refresh for the topic set.
    ///
    /// A failed refresh leaves the previous assignment, fetch epochs, and
    /// aborted-transaction cursors in place. An empty list still clears them.
    pub(crate) async fn assign_all(&mut self, starts: &[(String, i32, i64)]) -> Result<()> {
        self.reject_pending_wakeup()?;
        self.abort_inflight_fetches();
        if starts.len() > crate::protocol::group::OffsetLimits::default().array_elements {
            return Err(Error::protocol("too many assigned partitions"));
        }
        let mut seen = HashSet::new();
        for (topic, partition, offset) in starts {
            require_partition(*partition)?;
            require_fetch_offset(*offset)?;
            Topic::validate(topic)?;
            if !seen.insert((topic.clone(), *partition)) {
                return Err(Error::protocol("duplicate assigned partition"));
            }
        }
        let old_ids = std::mem::take(&mut self.assigned_topic_ids);
        let mut old_assigned = PartitionMap::new();
        for (topic, partition, offset) in &self.assigned {
            let _previous = old_assigned.insert(topic, *partition, *offset);
        }
        let previous_assigned = std::mem::take(&mut self.assigned);
        self.cursor_index = None;
        let previous_epochs = std::mem::replace(&mut self.last_fetched_epochs, PartitionMap::new());
        let previous_aborted = std::mem::replace(&mut self.aborted, PartitionMap::new());
        if starts.is_empty() {
            self.pending.clear();
            self.retained.clear();
            self.retained_bytes = 0;
            self.buffered_bytes = 0;
            return Ok(());
        }
        let mut topics: Vec<String> = Vec::new();
        for (topic, _, _) in starts {
            Topic::validate(topic)?;
            if !topics.iter().any(|t| t == topic) {
                topics.push(topic.clone());
            }
        }
        if let Err(error) = self.refresh_metadata(Some(&topics)).await {
            self.assigned = previous_assigned;
            self.assigned_topic_ids = old_ids;
            self.last_fetched_epochs = previous_epochs;
            self.aborted = previous_aborted;
            return Err(error);
        }
        let new_ids = self.topic_name_ids();
        let mut names: HashMap<&str, Arc<str>> = HashMap::new();
        self.assigned
            .extend(starts.iter().map(|(topic, part, offset)| {
                let name = if let Some(existing) = names.get(topic.as_str()) {
                    Arc::clone(existing)
                } else {
                    let name = Arc::<str>::from(topic.as_str());
                    let _previous = names.insert(topic.as_str(), Arc::clone(&name));
                    name
                };
                (name, *part, *offset)
            }));
        for (topic, part, offset) in starts {
            if let Some(&old_offset) = old_assigned.get(topic, *part) {
                if old_offset != *offset
                    || old_ids
                        .get(topic.as_str())
                        .and_then(|parts| parts.get(part))
                        != new_ids.get(topic.as_str())
                {
                    self.drop_pending_for(topic, *part);
                }
            } else {
                self.drop_pending_for(topic, *part);
            }
        }
        self.retain_pending_assigned();
        self.capture_assigned_topic_ids();
        Ok(())
    }

    fn capture_assigned_topic_ids(&mut self) {
        let ids = self.topic_name_ids();
        self.assigned_topic_ids.retain(|topic, parts| {
            parts.retain(|partition, _| {
                self.assigned.iter().any(|(assigned, part, _)| {
                    assigned.as_ref() == topic.as_ref() && part == partition
                })
            });
            !parts.is_empty()
                && self
                    .assigned
                    .iter()
                    .any(|(assigned, _, _)| assigned.as_ref() == topic.as_ref())
        });
        for (topic, partition, _) in &self.assigned {
            if let Some(id) = ids.get(topic.as_ref()).filter(|id| **id != [0; 16]) {
                let _previous = self
                    .assigned_topic_ids
                    .entry(Arc::clone(topic))
                    .or_default()
                    .entry(*partition)
                    .or_insert(*id);
            }
        }
    }

    pub(crate) fn assigned_topic_id(&self, topic: &str, partition: i32) -> Option<[u8; 16]> {
        self.assigned_topic_ids
            .get(topic)
            .and_then(|parts| parts.get(&partition))
            .copied()
    }

    pub(crate) async fn partition_ids(&mut self, topic: &str) -> Result<Vec<i32>> {
        let topics = [topic.to_string()];
        self.refresh_metadata(Some(&topics)).await?;
        let Some(md) = self.metadata.as_ref() else {
            return Err(Error::UnknownTopic(topic.to_string()));
        };
        let entries: Vec<(i16, Vec<i32>)> = md
            .topics
            .iter()
            .filter(|tmd| tmd.name.as_deref() == Some(topic))
            .map(|tmd| {
                (
                    tmd.error_code,
                    tmd.partitions
                        .iter()
                        .map(|partition| partition.partition_index)
                        .collect(),
                )
            })
            .collect();
        match listed_partition_indexes(entries.iter().map(|(error, ids)| (*error, ids.as_slice())))
        {
            ListedPartitions::Indexes(indexes) => Ok(indexes),
            ListedPartitions::Error(code) => Err(Error::broker(code, topic.to_string())),
            ListedPartitions::Missing => Err(Error::UnknownTopic(topic.to_string())),
        }
    }

    /// Set the next fetch offset without a ListOffsets call.
    pub(crate) fn advance(&mut self, topic: &str, partition: i32, next_offset: i64) {
        if let Some(slot) = self
            .assigned
            .iter_mut()
            .find(|(t, p, _)| t.as_ref() == topic && *p == partition)
        {
            slot.2 = next_offset;
        }
    }

    fn ensure_cursor_index(&mut self) {
        if self.cursor_index.is_some() {
            return;
        }
        let mut index: CursorIndex = HashMap::new();
        for (slot, (topic, partition, _)) in self.assigned.iter().enumerate() {
            if let Some(parts) = index.get_mut(topic.as_ref()) {
                let _ = parts.insert(*partition, slot);
            } else {
                let mut parts = HashMap::new();
                let _ = parts.insert(*partition, slot);
                let _ = index.insert(Arc::clone(topic), parts);
            }
        }
        self.cursor_index = Some(index);
    }

    fn cursor_at(&self, topic: &str, partition: i32) -> Option<i64> {
        let slot = self
            .cursor_index
            .as_ref()?
            .get(topic)?
            .get(&partition)
            .copied()?;
        self.assigned.get(slot).and_then(|(name, part, offset)| {
            (name.as_ref() == topic && *part == partition).then_some(*offset)
        })
    }

    fn advance_indexed(&mut self, topic: &str, partition: i32, next_offset: i64) {
        let slot = self.cursor_index.as_ref().and_then(|index| {
            index
                .get(topic)
                .and_then(|parts| parts.get(&partition))
                .copied()
        });
        let Some(slot) = slot else {
            return;
        };
        if let Some(entry) = self.assigned.get_mut(slot) {
            if entry.0.as_ref() == topic && entry.1 == partition {
                entry.2 = next_offset;
            }
        }
    }

    async fn reconnect_bootstrap(&mut self, deadline: Instant) -> Result<()> {
        self.conns.clear();
        let addr = self.conn.addr().to_string();
        let mut conn = BrokerConn::connect_tls(
            &addr,
            &self.cfg.client_id,
            self.cfg.connect_timeout.min(startup_remaining(deadline)?),
            self.cfg.tls.as_ref(),
        )
        .await?;
        let resp =
            crate::protocol::api::negotiate_api_versions(&mut conn, startup_remaining(deadline)?)
                .await?;
        sasl::apply_api_keys(&mut conn, &resp.api_keys);
        sasl::authenticate(
            &mut conn,
            self.cfg.sasl_plain.as_ref(),
            self.cfg.sasl_scram.as_ref(),
            self.cfg.sasl_scram_sha512.as_ref(),
            self.cfg.sasl_oauthbearer.as_deref(),
            self.cfg.sasl_oauthbearer_oidc.as_ref(),
            startup_remaining(deadline)?,
        )
        .await?;
        self.metadata_version = resp
            .api_version(METADATA)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 13))
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        if let Some(version) = resp
            .api_version(FETCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 4, FETCH_CRATE_MAX_VERSION))
        {
            conn.set_fetch_version(version);
        }
        self.versions = resp
            .api_keys
            .into_iter()
            .map(|range| (range.api_key, range))
            .collect();
        self.conn = conn;
        self.list_offsets_ranges.clear();
        self.leader_epoch_ranges.clear();
        // Dedicated sockets were dropped above, and this bootstrap socket is new.
        // An incremental fetch session belongs to the previous connection.
        for session in self.fetch_sessions.values_mut() {
            session.reset();
        }
        Ok(())
    }

    async fn refresh_metadata(&mut self, topics: Option<&[String]>) -> Result<()> {
        self.refresh_metadata_timeout(topics, self.cfg.request_timeout)
            .await
    }

    /// Refresh metadata for `topics`, bounded by one `timeout`.
    pub(crate) async fn refresh_metadata_timeout(
        &mut self,
        topics: Option<&[String]>,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now().checked_add(timeout).ok_or(Error::Timeout)?;
        self.refresh_metadata_until(topics, deadline).await
    }

    async fn refresh_metadata_until(
        &mut self,
        topics: Option<&[String]>,
        deadline: Instant,
    ) -> Result<()> {
        if self.metadata_version == 0 {
            let mut encoded = bytes::BytesMut::new();
            let allow = self.cfg.allow_auto_topic_creation || topics.is_none();
            encode_metadata_request(&mut encoded, 0, topics, allow)?;
        }
        if self.conn.idle_expired(self.cfg.connections_max_idle) {
            self.reconnect_bootstrap(deadline).await?;
        }
        let mut version = self.metadata_version;
        let allow = self.cfg.allow_auto_topic_creation || version < 4 && topics.is_none();
        let body = match self
            .conn
            .roundtrip(
                METADATA,
                version,
                |buf| encode_metadata_request(buf, version, topics, allow),
                startup_remaining(deadline)?,
            )
            .await
        {
            Ok(b) => b,
            Err(e) if e.is_retriable() => {
                self.reconnect_bootstrap(deadline).await?;
                version = self.metadata_version;
                let allow = self.cfg.allow_auto_topic_creation || version < 4 && topics.is_none();
                self.conn
                    .roundtrip(
                        METADATA,
                        version,
                        |buf| encode_metadata_request(buf, version, topics, allow),
                        startup_remaining(deadline)?,
                    )
                    .await?
            }
            Err(e) => return Err(e),
        };
        if version >= 10
            && [
                crate::protocol::api_keys::OFFSET_COMMIT,
                crate::protocol::api_keys::OFFSET_FETCH,
            ]
            .iter()
            .any(|api| {
                self.versions
                    .get(api)
                    .is_some_and(|range| range.min_version <= 10 && range.max_version >= 10)
            })
        {
            crate::protocol::group::validate_offset_metadata_bounds(
                &body,
                version,
                crate::protocol::group::OffsetLimits::default(),
            )?;
        }
        let md = decode_metadata_response(&mut body.clone(), version)?;
        md.check()?;
        let previous_epochs = self.cluster.leader_epochs.clone();
        self.cluster.apply(&md, version);
        for (topic, partition) in preferred_partitions_to_drop(
            &self.preferred.entries(),
            &previous_epochs,
            &self.cluster.leader_epochs,
        ) {
            let _ = self.preferred.remove(&topic, partition);
        }
        self.metadata = Some(md);
        self.adopt_recreated_topics();
        Ok(())
    }

    /// A topic id change means the name was deleted and created again.
    ///
    /// The captured assignment id and delivered position stay in place so a
    /// commit still names the log the application read. Buffered records from
    /// the deleted topic are dropped. Fetch refuses the mismatched id, and the
    /// next group assignment resolves the new log. ListOffsets is not called
    /// here: that lookup refreshes metadata and would recurse.
    fn adopt_recreated_topics(&mut self) {
        let changed = recreated_topic_partitions(
            &self.assigned,
            &self.assigned_topic_ids,
            &self.topic_name_ids(),
        );
        for (topic, partition, _) in changed {
            self.drop_pending_for(&topic, partition);
            let _removed = self.preferred.remove(&topic, partition);
            self.set_last_fetched_epoch(
                &topic,
                partition,
                crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
            );
        }
    }

    pub(crate) async fn ensure_topic_metadata_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<()> {
        if self.cluster.topic_fresh(topic, self.cfg.metadata_max_age) {
            return Ok(());
        }
        let topics = [topic.to_string()];
        self.refresh_metadata_timeout(Some(&topics), timeout).await
    }

    pub(crate) fn topic_id_names(&self) -> HashMap<[u8; 16], String> {
        self.topic_identities().1
    }

    pub(crate) fn topic_name_ids(&self) -> HashMap<String, [u8; 16]> {
        self.topic_identities().0
    }

    /// Topic ids after Metadata merge. A cached id wins over a later zero id
    /// in the same response, matching [`Cluster::apply`].
    fn topic_identities(&self) -> (HashMap<String, [u8; 16]>, HashMap<[u8; 16], String>) {
        let Some(md) = &self.metadata else {
            return (HashMap::new(), HashMap::new());
        };
        resolved_topic_identities(
            md.topics
                .iter()
                .filter_map(|topic| topic.name.as_deref().map(|name| (name, topic.topic_id))),
            &self.cluster.topic_ids,
        )
    }

    pub(crate) fn leader_of(&self, topic: &str, partition: i32) -> Result<(i32, String)> {
        self.cluster.leader(topic, partition)
    }

    /// Store Produce/Fetch/Share NodeEndpoints before a CurrentLeader patch.
    pub(crate) fn apply_node_endpoints(
        &mut self,
        endpoints: &[crate::protocol::api::NodeEndpoint],
    ) {
        self.cluster.apply_node_endpoints(endpoints);
    }

    /// Patch one partition leader from a share response. A negative id and an
    /// epoch older than the cached leader epoch leave the cache unchanged.
    pub(crate) fn apply_share_leader(
        &mut self,
        topic: &str,
        partition: i32,
        leader_id: i32,
        leader_epoch: i32,
    ) -> bool {
        leader_id >= 0
            && self
                .cluster
                .apply_current_leader(topic, partition, leader_id, leader_epoch)
    }

    pub(crate) fn invalidate_topic(&mut self, topic: &str) {
        self.cluster.invalidate_topic(topic);
    }

    pub(crate) fn drop_node(&mut self, node: i32) {
        let _ = self.conns.remove(&node);
    }

    async fn connect_node(&mut self, node: i32, deadline: Instant) -> Result<()> {
        self.connect_node_inner(node, true, deadline).await
    }

    /// Connection that speaks to `node`: the dedicated socket, or the bootstrap
    /// socket when one-node fetch is already using it.
    fn broker_conn_mut(&mut self, node: i32) -> Result<&mut crate::net::BrokerConn> {
        if self.conns.contains_key(&node) {
            return self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing broker conn"));
        }
        let same_broker = self
            .cluster
            .brokers
            .get(&node)
            .is_some_and(|addr| addr == self.conn.addr());
        if same_broker && !self.conn.is_closed() {
            return Ok(&mut self.conn);
        }
        Err(Error::protocol("missing broker conn"))
    }

    fn reset_fetch_sessions_using_bootstrap(&mut self) {
        let bootstrap = self.conn.addr().to_string();
        let nodes: Vec<i32> = self
            .cluster
            .brokers
            .iter()
            .filter(|(_, addr)| *addr == &bootstrap)
            .map(|(node, _)| *node)
            .collect();
        for node in nodes {
            if self.conns.contains_key(&node) {
                continue;
            }
            if let Some(session) = self.fetch_sessions.get_mut(&node) {
                session.reset();
            }
        }
    }

    fn drop_broker_conn(&mut self, node: i32) {
        let used_bootstrap = !self.conns.contains_key(&node)
            && self
                .cluster
                .brokers
                .get(&node)
                .is_some_and(|addr| addr == self.conn.addr());
        let _ = self.conns.remove(&node);
        let _ = self.list_offsets_ranges.remove(&node);
        let _ = self.leader_epoch_ranges.remove(&node);
        if used_bootstrap {
            self.conn.close();
        }
    }

    async fn connect_node_inner(
        &mut self,
        node: i32,
        reuse_bootstrap: bool,
        deadline: Instant,
    ) -> Result<()> {
        if self
            .conns
            .get(&node)
            .is_some_and(|c| c.idle_expired(self.cfg.connections_max_idle))
        {
            let _ = self.conns.remove(&node);
            let _ = self.list_offsets_ranges.remove(&node);
            let _ = self.leader_epoch_ranges.remove(&node);
            if let Some(session) = self.fetch_sessions.get_mut(&node) {
                session.reset();
            }
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
        // The one-node leader is the bootstrap socket, which is not stored in
        // `conns`. Resetting its fetch session on every poll forces a full
        // partition list. Keep the session while that socket stays live.
        if reuse_bootstrap && self.conn.addr() == addr && self.conn.fetch_version() >= 0 {
            if self.conn.idle_expired(self.cfg.connections_max_idle) {
                if let Some(session) = self.fetch_sessions.get_mut(&node) {
                    session.reset();
                }
                let (conn, versions) = self.open_node_conn_until(&addr, deadline).await?;
                self.versions = versions
                    .api_keys
                    .into_iter()
                    .map(|api| (api.api_key, api))
                    .collect();
                self.conn = conn;
            }
            return Ok(());
        }
        if let Some(session) = self.fetch_sessions.get_mut(&node) {
            session.reset();
        }
        loop {
            if self.woken() {
                return Err(Error::Wakeup);
            }
            let fails = self.reconnect_fails.get(&node).copied().unwrap_or(0);
            self.sleep_reconnect_backoff(fails, deadline).await?;
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            match self.open_node_conn_until(&addr, deadline).await {
                Ok((conn, versions)) => {
                    let _ = self.reconnect_fails.remove(&node);
                    self.remember_opened_conn(node, conn, &versions);
                    return Ok(());
                }
                Err(e) if e.is_retriable() => {
                    self.note_reconnect_failure(node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                }
                Err(e) => {
                    self.note_reconnect_failure(node);
                    return Err(e);
                }
            }
        }
    }

    /// Dial, negotiate, and authenticate inside `deadline`.
    async fn open_node_conn_until(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<(BrokerConn, ApiVersionsResponse)> {
        Self::open_fetch_conn(
            addr,
            &self.cfg.client_id,
            self.cfg.connect_timeout,
            self.cfg.tls.as_ref(),
            self.cfg.sasl_plain.as_ref(),
            self.cfg.sasl_scram.as_ref(),
            self.cfg.sasl_scram_sha512.as_ref(),
            self.cfg.sasl_oauthbearer.as_deref(),
            self.cfg.sasl_oauthbearer_oidc.as_ref(),
            deadline,
        )
        .await
    }

    /// How many leader handshakes a multi-broker fetch may open at once.
    const FETCH_DIAL_PARALLELISM: usize = 8;

    #[expect(
        clippy::too_many_arguments,
        reason = "a leader dial carries the shared SASL and TLS settings"
    )]
    pub(crate) async fn open_fetch_conn(
        addr: &str,
        client_id: &str,
        connect_timeout: Duration,
        tls: Option<&crate::net::TlsConfig>,
        sasl_plain: Option<&(String, String)>,
        sasl_scram: Option<&(String, String)>,
        sasl_scram_sha512: Option<&(String, String)>,
        sasl_oauthbearer: Option<&str>,
        sasl_oauthbearer_oidc: Option<&crate::OidcConfig>,
        deadline: Instant,
    ) -> Result<(BrokerConn, ApiVersionsResponse)> {
        let mut conn = BrokerConn::connect_tls(
            addr,
            client_id,
            connect_timeout.min(startup_remaining(deadline)?),
            tls,
        )
        .await?;
        let versions_resp =
            crate::protocol::api::negotiate_api_versions(&mut conn, startup_remaining(deadline)?)
                .await?;
        if let Some(fv) = versions_resp
            .api_version(FETCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 4, FETCH_CRATE_MAX_VERSION))
        {
            conn.set_fetch_version(fv);
        }
        sasl::apply_api_keys(&mut conn, &versions_resp.api_keys);
        sasl::authenticate(
            &mut conn,
            sasl_plain,
            sasl_scram,
            sasl_scram_sha512,
            sasl_oauthbearer,
            sasl_oauthbearer_oidc,
            startup_remaining(deadline)?,
        )
        .await?;
        Ok((conn, versions_resp))
    }

    async fn connect_parallel_fetch_nodes(
        &mut self,
        nodes: Vec<i32>,
        by_leader: &mut HashMap<i32, HashMap<Arc<str>, Vec<FetchPartition>>>,
        deadline: Instant,
    ) -> Result<Vec<i32>> {
        let mut queue = nodes;
        let mut connected = Vec::new();
        let mut dials = Vec::new();
        while let Some(node) = queue.pop() {
            if connected.contains(&node) || dials.iter().any(|(id, _)| *id == node) {
                continue;
            }
            if Instant::now() >= deadline {
                break;
            }
            if is_preferred_only_target(&self.preferred, &self.cluster, node, by_leader) {
                match self.connect_preferred_once(node, deadline).await {
                    Ok(()) => connected.push(node),
                    Err(error) => {
                        if !reroute_unreachable_preferred(
                            &mut self.preferred,
                            &self.cluster,
                            node,
                            by_leader,
                        ) {
                            if matches!(error, Error::Timeout) {
                                break;
                            }
                            return Err(Error::Timeout);
                        }
                        for leader in by_leader.keys().copied() {
                            if !connected.contains(&leader)
                                && !queue.contains(&leader)
                                && dials.iter().all(|(id, _)| *id != leader)
                            {
                                queue.push(leader);
                            }
                        }
                    }
                }
                continue;
            }
            if self
                .conns
                .get(&node)
                .is_some_and(|conn| conn.idle_expired(self.cfg.connections_max_idle))
            {
                let _ = self.conns.remove(&node);
                let _ = self.list_offsets_ranges.remove(&node);
                let _ = self.leader_epoch_ranges.remove(&node);
                if let Some(session) = self.fetch_sessions.get_mut(&node) {
                    session.reset();
                }
            }
            if self.conns.contains_key(&node) {
                connected.push(node);
                continue;
            }
            let Some(addr) = self.cluster.brokers.get(&node).cloned() else {
                return Err(Error::protocol(format!("unknown broker {node}")));
            };
            if let Some(session) = self.fetch_sessions.get_mut(&node) {
                session.reset();
            }
            dials.push((node, addr));
        }
        if dials.is_empty() {
            if connected.is_empty() {
                return Err(Error::Timeout);
            }
            return Ok(connected);
        }
        let client_id = self.cfg.client_id.clone();
        let connect_timeout = self.cfg.connect_timeout;
        let tls = self.cfg.tls.clone();
        let sasl_plain = self.cfg.sasl_plain.clone();
        let sasl_scram = self.cfg.sasl_scram.clone();
        let sasl_scram_sha512 = self.cfg.sasl_scram_sha512.clone();
        let sasl_oauthbearer = self.cfg.sasl_oauthbearer.clone();
        let sasl_oauthbearer_oidc = self.cfg.sasl_oauthbearer_oidc.clone();
        let mut waiting = dials.into_iter();
        let mut inflight = tokio::task::JoinSet::new();
        let spawn = |inflight: &mut tokio::task::JoinSet<(
            i32,
            Result<(BrokerConn, ApiVersionsResponse)>,
        )>,
                     node: i32,
                     addr: String| {
            let client_id = client_id.clone();
            let tls = tls.clone();
            let sasl_plain = sasl_plain.clone();
            let sasl_scram = sasl_scram.clone();
            let sasl_scram_sha512 = sasl_scram_sha512.clone();
            let sasl_oauthbearer = sasl_oauthbearer.clone();
            let sasl_oauthbearer_oidc = sasl_oauthbearer_oidc.clone();
            let _ = inflight.spawn(async move {
                let result = Consumer::open_fetch_conn(
                    &addr,
                    &client_id,
                    connect_timeout,
                    tls.as_ref(),
                    sasl_plain.as_ref(),
                    sasl_scram.as_ref(),
                    sasl_scram_sha512.as_ref(),
                    sasl_oauthbearer.as_deref(),
                    sasl_oauthbearer_oidc.as_ref(),
                    deadline,
                )
                .await;
                (node, result)
            });
        };
        while inflight.len() < Self::FETCH_DIAL_PARALLELISM {
            let Some((node, addr)) = waiting.next() else {
                break;
            };
            spawn(&mut inflight, node, addr);
        }
        let mut retry = Vec::new();
        while !inflight.is_empty() {
            if self.woken() {
                inflight.abort_all();
                return Err(Error::Wakeup);
            }
            if Instant::now() >= deadline {
                inflight.abort_all();
                break;
            }
            let rest = deadline.saturating_duration_since(Instant::now());
            let joined = tokio::select! {
                joined = inflight.join_next() => joined,
                _ = tokio::time::sleep(rest) => {
                    inflight.abort_all();
                    break;
                }
            };
            let Some(joined) = joined else {
                break;
            };
            let (node, result) =
                joined.map_err(|error| Error::protocol(format!("fetch dial task: {error}")))?;
            match result {
                Ok((conn, versions)) => {
                    let _ = self.reconnect_fails.remove(&node);
                    self.remember_opened_conn(node, conn, &versions);
                    connected.push(node);
                }
                Err(error) if error.is_retriable() || matches!(error, Error::Timeout) => {
                    self.note_reconnect_failure(node);
                    retry.push(node);
                }
                Err(error) => {
                    inflight.abort_all();
                    return Err(error);
                }
            }
            while inflight.len() < Self::FETCH_DIAL_PARALLELISM {
                let Some((node, addr)) = waiting.next() else {
                    break;
                };
                spawn(&mut inflight, node, addr);
            }
        }
        for node in retry {
            if Instant::now() >= deadline {
                break;
            }
            match self.connect_node_inner(node, false, deadline).await {
                Ok(()) => connected.push(node),
                Err(Error::Timeout) => break,
                Err(error) if error.is_retriable() => {}
                Err(error) => return Err(error),
            }
        }
        if connected.is_empty() {
            return Err(Error::Timeout);
        }
        Ok(connected)
    }

    /// One dial for a rack replica. A refusal must leave time to fetch from the leader.
    async fn connect_preferred_once(&mut self, node: i32, deadline: Instant) -> Result<()> {
        if self
            .conns
            .get(&node)
            .is_some_and(|conn| conn.idle_expired(self.cfg.connections_max_idle))
        {
            let _ = self.conns.remove(&node);
            let _ = self.list_offsets_ranges.remove(&node);
            let _ = self.leader_epoch_ranges.remove(&node);
            if let Some(session) = self.fetch_sessions.get_mut(&node) {
                session.reset();
            }
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
        if let Some(session) = self.fetch_sessions.get_mut(&node) {
            session.reset();
        }
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        match self.open_node_conn_until(&addr, deadline).await {
            Ok((conn, versions)) => {
                let _ = self.reconnect_fails.remove(&node);
                self.remember_opened_conn(node, conn, &versions);
                Ok(())
            }
            Err(error) => {
                self.note_reconnect_failure(node);
                Err(error)
            }
        }
    }

    fn remember_opened_conn(
        &mut self,
        node: i32,
        conn: BrokerConn,
        versions: &ApiVersionsResponse,
    ) {
        Self::store_api_range(
            &mut self.list_offsets_ranges,
            node,
            versions.api_version(LIST_OFFSETS),
        );
        Self::store_api_range(
            &mut self.leader_epoch_ranges,
            node,
            versions.api_version(OFFSET_FOR_LEADER_EPOCH),
        );
        let _previous = self.conns.insert(node, conn);
    }

    fn store_api_range(
        ranges: &mut HashMap<i32, (i16, i16)>,
        node: i32,
        version: Option<&ApiVersion>,
    ) {
        match version {
            Some(range) => {
                let _previous = ranges.insert(node, (range.min_version, range.max_version));
            }
            None => {
                let _previous = ranges.remove(&node);
            }
        }
    }

    fn note_reconnect_failure(&mut self, node: i32) {
        let _fails = crate::config::bump_reconnect_fails(&mut self.reconnect_fails, node);
        let _ = self.m_reconnects.fetch_add(1, Ordering::Relaxed);
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

    async fn recover_leader_epoch(
        &mut self,
        topic: &str,
        partition: i32,
        deadline: Instant,
    ) -> Result<()> {
        self.recover_leader_epochs(&[(topic.to_string(), partition)], deadline)
            .await
    }

    fn offset_for_leader_epoch_version(&self, node: i32) -> Result<i16> {
        let uses_bootstrap = !self.conns.contains_key(&node)
            && self
                .cluster
                .brokers
                .get(&node)
                .is_some_and(|addr| addr == self.conn.addr());
        let version = if uses_bootstrap {
            self.versions
                .get(&OFFSET_FOR_LEADER_EPOCH)
                .and_then(|v| pick_version(v.min_version, v.max_version, 0, 4))
        } else {
            self.leader_epoch_ranges
                .get(&node)
                .copied()
                .and_then(|(min_version, max_version)| pick_version(min_version, max_version, 0, 4))
        };
        version.ok_or_else(|| {
            Error::Unsupported("broker does not support OffsetForLeaderEpoch v0-4".into())
        })
    }

    async fn recover_leader_epochs(
        &mut self,
        coords: &[(String, i32)],
        deadline: Instant,
    ) -> Result<()> {
        if coords.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        // Preferred replica may have returned the fence; OffsetForLeaderEpoch is leader-only.
        // Refresh Metadata first so `current_leader_epoch` is not the value that just fenced us.
        for (topic, partition) in coords {
            let _ = self.preferred.remove(topic, *partition);
        }
        let mut names: Vec<String> = coords.iter().map(|(t, _)| t.clone()).collect();
        names.sort();
        names.dedup();
        self.refresh_metadata_timeout(
            Some(&names),
            remaining_offset_timeout(deadline, Instant::now()),
        )
        .await?;
        let mut remaining: Vec<(String, i32)> = coords.to_vec();
        remaining.sort();
        remaining.dedup();
        loop {
            if remaining.is_empty() {
                return Ok(());
            }
            let mut missing_leader = false;
            for (topic, partition) in &remaining {
                if self.cluster.leader_id(topic, *partition).is_err() {
                    missing_leader = true;
                    break;
                }
            }
            if missing_leader {
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                self.refresh_metadata_timeout(
                    Some(&names),
                    remaining_offset_timeout(deadline, Instant::now()),
                )
                .await?;
            }
            let mut by_leader: HashMap<i32, Vec<(String, i32)>> = HashMap::new();
            let mut retry = Vec::new();
            for (topic, partition) in remaining {
                match self.cluster.leader_id(&topic, partition) {
                    Ok(node) => by_leader.entry(node).or_default().push((topic, partition)),
                    Err(_) => retry.push((topic, partition)),
                }
            }
            let mut nodes: Vec<i32> = by_leader.keys().copied().collect();
            nodes.sort_unstable();
            for node in nodes {
                let Some(parts) = by_leader.remove(&node) else {
                    continue;
                };
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                self.connect_node(node, deadline).await?;
                let version = self.offset_for_leader_epoch_version(node)?;
                let topics = offset_for_leader_epoch_topics(
                    &self.cluster,
                    &self.last_fetched_epochs,
                    &parts,
                );
                let timeout = remaining_offset_timeout(deadline, Instant::now());
                let body = {
                    let conn = self.broker_conn_mut(node)?;
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
                        self.drop_broker_conn(node);
                        retry.extend(parts);
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                match decode_offset_for_leader_epoch_topics_response(&mut body.clone(), version) {
                    Ok((got, ..)) => {
                        let more = self.apply_epoch_end_offsets(&parts, &got)?;
                        if !more.is_empty() {
                            self.drop_broker_conn(node);
                            retry.extend(more);
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        self.drop_broker_conn(node);
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
            self.refresh_metadata_timeout(
                Some(&retry_names),
                remaining_offset_timeout(deadline, Instant::now()),
            )
            .await?;
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
                        .find(|(name, part, _)| {
                            name.as_ref() == t.topic.as_str() && *part == p.partition
                        })
                        .map(|(_, _, o)| *o);
                    if let Some(off) = assigned {
                        if let Some(next) = truncated_fetch_offset(off, p.end_offset) {
                            self.advance(&t.topic, p.partition, next);
                            self.set_last_fetched_epoch(&t.topic, p.partition, p.leader_epoch);
                            self.drop_pending_for(&t.topic, p.partition);
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
        self.fetch_records(true, self.cfg.max_wait_ms, true).await
    }

    /// [`Self::fetch`] for a subscribed [`crate::ConsumerGroup`] (assignment
    /// may still be empty while the coordinator is joining).
    ///
    /// Fetch interceptors run when the group returns the records, after a
    /// zero-interval auto-commit has succeeded.
    pub(crate) async fn fetch_allow_unassigned(&mut self) -> Result<ConsumerRecords> {
        self.fetch_records(false, self.cfg.max_wait_ms, false).await
    }

    async fn fetch_records(
        &mut self,
        require_assignment: bool,
        max_wait_ms: i32,
        intercept: bool,
    ) -> Result<ConsumerRecords> {
        if self.take_wakeup() {
            return Err(Error::Wakeup);
        }
        if require_assignment && self.assigned.is_empty() {
            return Err(reject_java_no_subscription_or_assignment());
        }
        let started = Instant::now();
        let result = self.fetch_assigned(max_wait_ms).await;
        match result {
            Ok(recs) => {
                let elapsed = started.elapsed();
                self.m_fetch_latency.record(elapsed);
                let _ = self.m_fetch_rounds.fetch_add(1, Ordering::Relaxed);
                let n = u64::try_from(recs.len()).unwrap_or(u64::MAX);
                let _ = self.m_records.fetch_add(n, Ordering::Relaxed);
                let staged = std::mem::take(&mut self.fetch_return_totals);
                let staged_records = staged
                    .values()
                    .fold(0u64, |n, (count, _)| n.saturating_add(*count));
                let bytes = if staged_records == u64::try_from(recs.len()).unwrap_or(u64::MAX) {
                    let mut bytes = 0u64;
                    for (topic, (n, topic_bytes)) in staged {
                        bytes = bytes.saturating_add(topic_bytes);
                        if let Some(tracker) = self.topic_metrics.get_mut(topic.as_ref()) {
                            tracker.note_fetched(n, topic_bytes, elapsed);
                        } else {
                            let tracker = crate::metrics::FetchTopicTracker::new();
                            tracker.note_fetched(n, topic_bytes, elapsed);
                            let _ = self.topic_metrics.insert(topic, tracker);
                        }
                    }
                    bytes
                } else {
                    let bytes = recs.iter().map(fetched_bytes).fold(0, u64::saturating_add);
                    crate::metrics::accumulate_fetch_topics(
                        &mut self.topic_metrics,
                        recs.iter().map(|r| (r.topic.as_str(), fetched_bytes(r))),
                        elapsed,
                    );
                    bytes
                };
                let _ = self.m_bytes.fetch_add(bytes, Ordering::Relaxed);
                let recs = if intercept {
                    self.cfg.interceptors.on_consume(recs)
                } else {
                    recs
                };
                Ok(ConsumerRecords::from(recs))
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
    /// A cancelled one-shot future leaves [`ConsumerConfig::max_wait_ms`] intact. Nothing
    /// assigned is the same Java `IllegalStateException` as [`Self::fetch`].
    pub async fn fetch_timeout(&mut self, timeout: Duration) -> Result<ConsumerRecords> {
        self.fetch_records_timeout(timeout, true, true).await
    }

    /// [`Self::fetch_timeout`] for a subscribed [`crate::ConsumerGroup`].
    pub(crate) async fn fetch_allow_unassigned_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<ConsumerRecords> {
        self.fetch_records_timeout(timeout, false, false).await
    }

    async fn fetch_records_timeout(
        &mut self,
        timeout: Duration,
        require_assignment: bool,
        intercept: bool,
    ) -> Result<ConsumerRecords> {
        // Carry the one-shot value through the future instead of mutating cfg:
        // cancellation during a quota wait must not change later poll settings.
        self.fetch_records(require_assignment, duration_millis_i32(timeout), intercept)
            .await
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
            reconnects: self.m_reconnects.load(Ordering::Relaxed),
            fetch_latency: self.m_fetch_latency.snapshot(),
            throttle: self.throttle_metrics.snapshot(),
            topics: crate::metrics::snapshot_fetch_topics(&self.topic_metrics),
        }
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
        self.reject_pending_wakeup()?;
        let version = self.telemetry_version.ok_or_else(|| {
            Error::Unsupported("broker does not support GetTelemetrySubscriptions".into())
        })?;
        let deadline = Instant::now().checked_add(timeout).ok_or(Error::Timeout)?;
        if self.conn.idle_expired(self.cfg.connections_max_idle) {
            let addr = self.conn.addr().to_string();
            let (conn, versions) = self.open_node_conn_until(&addr, deadline).await?;
            self.versions = versions
                .api_keys
                .into_iter()
                .map(|api| (api.api_key, api))
                .collect();
            self.conn = conn;
            // The new socket does not own the previous connection's fetch session.
            self.reset_fetch_sessions_using_bootstrap();
        }
        let id = crate::admin::fetch_client_instance_id(
            &mut self.conn,
            version,
            startup_remaining(deadline)?,
            [0; 16],
        )
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
        let _previous = self.wakeup_tx.send_replace(true);
    }

    /// Cloneable handle for [`Self::wakeup`] from another task.
    #[must_use]
    pub fn wakeup_handle(&self) -> WakeupHandle {
        WakeupHandle {
            flag: Arc::clone(&self.wakeup),
            tx: self.wakeup_tx.clone(),
        }
    }

    /// Close known Fetch sessions within the request timeout, then drop connections.
    /// Session retirement is best effort and does not commit offsets.
    pub async fn close(self) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.close_timeout(timeout).await
    }

    /// Best-effort terminal Fetch requests, bounded by one total caller budget.
    ///
    /// A zero budget drops connections immediately. A manual consumer has no
    /// LeaveGroup RPC; group and share members use
    /// [`crate::ConsumerGroup::close_timeout`] /
    /// [`crate::ShareGroup::close_timeout`].
    pub async fn close_timeout(mut self, timeout: Duration) -> Result<()> {
        self.cfg.interceptors.close();
        self.close_fetch_sessions(timeout).await;
        self.conns.clear();
        Ok(())
    }

    fn abort_inflight_fetches(&mut self) {
        if let Some(mut set) = self.inflight_fetches.take() {
            set.abort_all();
        }
        self.inflight_nodes.clear();
    }

    pub(crate) async fn close_fetch_sessions(&mut self, timeout: Duration) {
        self.abort_inflight_fetches();
        if !timeout.is_zero() {
            if let Some(deadline) = Instant::now().checked_add(timeout) {
                let nodes = self.fetch_sessions.keys().copied().collect();
                let _closed = self.retire_fetch_sessions(nodes, deadline).await;
            }
        }
        self.fetch_sessions.clear();
    }

    async fn retire_fetch_sessions(
        &mut self,
        mut nodes: Vec<i32>,
        deadline: Instant,
    ) -> Result<()> {
        nodes.sort_unstable();
        for node in nodes {
            let Some(session) = self.fetch_sessions.remove(&node) else {
                continue;
            };
            if session.metadata.session_id() == FetchMetadata::INVALID_SESSION_ID {
                continue;
            }
            let throttle_until = self.fetch_throttles.get(&node).copied();
            let uses_bootstrap = !self.conns.contains_key(&node)
                && self
                    .cluster
                    .brokers
                    .get(&node)
                    .is_some_and(|addr| addr == self.conn.addr());
            if uses_bootstrap {
                // One-node fetch keeps this leader on the bootstrap socket.
                send_fetch_session_close(
                    &mut self.conn,
                    &session,
                    throttle_until,
                    &self.wakeup_tx,
                    &self.cfg,
                    deadline,
                )
                .await?;
                continue;
            }
            let Some(mut conn) = self.conns.remove(&node) else {
                continue;
            };
            send_fetch_session_close(
                &mut conn,
                &session,
                throttle_until,
                &self.wakeup_tx,
                &self.cfg,
                deadline,
            )
            .await?;
            // The dedicated socket is dropped with its close response. An
            // interrupted roundtrip must not be reused with an unread frame.
        }
        Ok(())
    }

    pub(crate) fn close_interceptors(&self) {
        self.cfg.interceptors.close();
    }

    /// Run fetch interceptors once, when a group poll actually returns records.
    pub(crate) fn deliver_consumed(&self, records: ConsumerRecords) -> ConsumerRecords {
        ConsumerRecords::from(
            self.cfg
                .interceptors
                .on_consume(records.into_iter().collect()),
        )
    }

    /// Put a poll batch back in front of any records still buffered.
    ///
    /// Used when a zero-interval auto-commit fails after the fetch cursor has
    /// already moved. The next poll returns this batch again.
    pub(crate) fn restore_fetched(&mut self, records: ConsumerRecords) {
        let records: Vec<FetchedRecord> = records.into_iter().collect();
        for rec in &records {
            self.buffered_bytes = self.buffered_bytes.saturating_add(record_bytes(rec));
        }
        self.pending.prepend(records);
    }

    pub(crate) fn take_wakeup(&self) -> bool {
        let was = self.wakeup.swap(false, Ordering::SeqCst);
        if was {
            let _previous = self.wakeup_tx.send_replace(false);
        }
        was
    }

    fn reject_pending_wakeup(&self) -> Result<()> {
        if self.take_wakeup() {
            Err(Error::Wakeup)
        } else {
            Ok(())
        }
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

    async fn fetch_assigned(&mut self, max_wait_ms: i32) -> Result<Vec<FetchedRecord>> {
        if let Some(ready) = self.take_ready() {
            self.fetch_return_totals.clear();
            return Ok(ready);
        }
        if self.assigned.is_empty() {
            return Ok(Vec::new());
        }
        if self.cfg.buffer_memory > 0 && self.buffered_bytes >= self.cfg.buffer_memory {
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let deadline = started + self.cfg.request_timeout;
        let poll_deadline =
            started + Duration::from_millis(u64::try_from(max_wait_ms).unwrap_or(0));
        let initial_assigned = self.assigned.clone();
        let initial_epochs = self.last_fetched_epochs.clone();
        match self
            .fetch_assigned_inner(deadline, poll_deadline, max_wait_ms)
            .await
        {
            Ok(records) => Ok(records),
            Err(e) => {
                self.assigned = initial_assigned;
                self.last_fetched_epochs = initial_epochs;
                Err(e)
            }
        }
    }

    async fn fetch_assigned_inner(
        &mut self,
        deadline: Instant,
        poll_deadline: Instant,
        max_wait_ms: i32,
    ) -> Result<Vec<FetchedRecord>> {
        let mut attempt = 0u32;
        let mut out = Vec::new();
        let mut out_bytes = 0usize;
        let mut completed = PartitionMap::new();
        self.fetch_return_totals.clear();
        loop {
            if self.woken() {
                return Err(Error::Wakeup);
            }
            if let Some(body) = self.detach_unpaused_retained() {
                let mut fenced = Vec::new();
                let mut need_offsets = Vec::new();
                let mut budget_reached = false;
                let _retry = self.apply_fetch_body(
                    -1,
                    body,
                    &mut out,
                    &mut out_bytes,
                    &mut fenced,
                    &mut need_offsets,
                    &mut completed,
                    &mut budget_reached,
                )?;
                if budget_reached {
                    return Ok(self.finish_fetch(out));
                }
            }
            let metadata_stale = self.assigned.iter().any(|(t, p, _)| {
                !completed.contains(t, *p)
                    && !self
                        .cluster
                        .topic_fresh(t.as_ref(), self.cfg.metadata_max_age)
            });
            if metadata_stale {
                let mut topics = Vec::new();
                let mut seen = HashSet::new();
                for (t, p, _) in &self.assigned {
                    if completed.contains(t, *p) {
                        continue;
                    }
                    if seen.insert(t.as_ref()) {
                        topics.push(t.as_ref().to_owned());
                    }
                }
                if Instant::now() >= deadline {
                    return self.fetched_or_timeout(out, Error::Timeout);
                }
                if let Err(error) = self
                    .refresh_metadata_timeout(
                        Some(&topics),
                        remaining_offset_timeout(deadline, Instant::now()),
                    )
                    .await
                {
                    // A later metadata refresh must not drop records already read.
                    return self.fetched_or_timeout(out, error);
                }
            }
            let mut by_leader: HashMap<i32, HashMap<Arc<str>, Vec<FetchPartition>>> =
                HashMap::new();
            let mut missing_leader = false;
            for (topic, part, offset) in &self.assigned {
                if self.paused.contains(topic, *part) {
                    continue;
                }
                if completed.contains(topic, *part) || self.retained.contains(topic, *part) {
                    continue;
                }
                let preferred = if self.cfg.rack.is_some() {
                    self.preferred.get(topic, *part).copied()
                } else {
                    None
                };
                let leader = self.cluster.leader_id(topic, *part).ok();
                let preferred_known =
                    preferred.is_some_and(|node| self.cluster.brokers.contains_key(&node));
                let node = fetch_target(preferred, preferred_known, leader);
                match node {
                    Some(node) => {
                        let planned = FetchPartition {
                            partition: *part,
                            current_leader_epoch: self.cluster.leader_epoch(topic, *part),
                            fetch_offset: *offset,
                            last_fetched_epoch: self.last_fetched_epoch(topic, *part),
                            log_start_offset: INVALID_LOG_START_OFFSET,
                            partition_max_bytes: self.cfg.max_partition_fetch_bytes,
                            replica_directory_id: [0; 16],
                        };
                        let topics = by_leader.entry(node).or_default();
                        if let Some(parts) = topics.get_mut(topic.as_ref()) {
                            parts.push(planned);
                        } else {
                            let _ = topics.insert(Arc::clone(topic), vec![planned]);
                        }
                    }
                    None => {
                        missing_leader = true;
                        break;
                    }
                }
            }
            if missing_leader {
                if Instant::now() >= deadline {
                    return if !out.is_empty() {
                        Ok(self.finish_fetch(out))
                    } else {
                        Err(Error::Timeout)
                    };
                }
                self.sleep_retry_backoff(attempt, deadline).await?;
                attempt = attempt.saturating_add(1);
                if Instant::now() >= deadline {
                    return if !out.is_empty() {
                        Ok(self.finish_fetch(out))
                    } else {
                        Err(Error::Timeout)
                    };
                }
                for (t, _, _) in &self.assigned {
                    self.cluster.invalidate_topic(t);
                }
                let topics: Vec<String> = self
                    .assigned
                    .iter()
                    .map(|(t, _, _)| t.as_ref().to_owned())
                    .collect();
                if let Err(error) = self
                    .refresh_metadata_timeout(
                        Some(&topics),
                        remaining_offset_timeout(deadline, Instant::now()),
                    )
                    .await
                {
                    return self.fetched_or_timeout(out, error);
                }
                continue;
            }
            let retired = self
                .fetch_sessions
                .keys()
                .filter(|node| !by_leader.contains_key(node) && !self.inflight_nodes.contains(node))
                .copied()
                .collect();
            self.retire_fetch_sessions(retired, deadline.min(poll_deadline))
                .await?;
            if by_leader.is_empty() {
                return Ok(self.finish_fetch(out));
            }
            if !self.fetch_throttles.is_empty() {
                let now = Instant::now();
                self.fetch_throttles.retain(|_, until| *until > now);
                let mut earliest: Option<Instant> = None;
                by_leader.retain(|node, _| {
                    if let Some(until) = self.fetch_throttles.get(node) {
                        earliest = Some(earliest.map_or(*until, |old| old.min(*until)));
                        false
                    } else {
                        true
                    }
                });
                if by_leader.is_empty() {
                    // Deliver previously accumulated work before waiting for
                    // any muted peer; no fetch/position advance for that peer.
                    if !out.is_empty() {
                        return Ok(self.finish_fetch(out));
                    }
                    if now >= deadline {
                        return Err(Error::Timeout);
                    }
                    if now >= poll_deadline {
                        return Ok(self.finish_fetch(out));
                    }
                    if let Some(until) = earliest {
                        let wake = until.min(deadline).min(poll_deadline);
                        let mut rx = self.wakeup_tx.subscribe();
                        tokio::select! {
                            biased;
                            result = rx.wait_for(|on| *on) => {
                                drop(result);
                                return Err(Error::Wakeup);
                            }
                            _ = tokio::time::sleep(wake.saturating_duration_since(now)) => {}
                        }
                        continue;
                    }
                }
            }
            let wait = max_wait_ms.min(duration_millis_i32(
                poll_deadline.saturating_duration_since(Instant::now()),
            ));
            let bodies = match self.fetch_from_leaders(by_leader, deadline, wait).await {
                Ok(bodies) => bodies,
                // Dialing or waiting out the deadline must not roll back records
                // this poll already took from an earlier leader.
                Err(error) => return self.fetched_or_timeout(out, error),
            };
            // ThrottleTimeMs is the first INT32 on every negotiated Fetch
            // version. Observe every completed response's header before the
            // record budget can discard another peer's body. Normal decoding
            // retains responsibility for truncated/invalid response errors.
            for reply in &bodies {
                if reply.version < 8 {
                    continue;
                }
                let Ok(body) = &reply.body else { continue };
                let mut header = body.as_ref();
                if let Ok(millis) = crate::protocol::buf::get_i32(&mut header) {
                    self.observe_fetch_throttle(reply.node, millis, reply.received_at);
                }
            }
            // Decode each envelope once, before any record application. Validate
            // every session even when the record budget will discard its body.
            let bodies: Vec<_> = bodies
                .into_iter()
                .map(|reply| self.decode_fetch_reply(reply))
                .collect();
            let mut retry = FetchRetry::None;
            let mut fenced = Vec::new();
            let mut need_offsets = Vec::new();
            let mut budget_reached = false;
            for reply in bodies {
                let node = reply.node;
                let body = match reply.body {
                    Ok(b) => b,
                    Err(_) if reply.recovery.is_some() => {
                        if let Some(recovery) = reply.recovery {
                            retry = retry.merge(recovery);
                        }
                        continue;
                    }
                    Err(e) if e.is_retriable() => {
                        let used_bootstrap = !self.conns.contains_key(&node)
                            && self
                                .cluster
                                .brokers
                                .get(&node)
                                .is_some_and(|addr| addr == self.conn.addr());
                        let _ = self.conns.remove(&node);
                        if used_bootstrap {
                            self.conn.close();
                        }
                        retry = retry.merge(FetchRetry::Backoff);
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                let applied = self.apply_fetch_body(
                    node,
                    body,
                    &mut out,
                    &mut out_bytes,
                    &mut fenced,
                    &mut need_offsets,
                    &mut completed,
                    &mut budget_reached,
                );
                if applied.is_err() {
                    if let Some(session) = self.fetch_sessions.get_mut(&node) {
                        session.reset();
                    }
                }
                retry = retry.merge(applied?);
                if budget_reached {
                    break;
                }
            }
            if !need_offsets.is_empty() {
                for (topic, partition, timestamp) in need_offsets {
                    let offset = match self
                        .list_offsets_timeout(
                            topic.clone(),
                            partition,
                            timestamp,
                            remaining_offset_timeout(deadline, Instant::now()),
                        )
                        .await
                    {
                        Ok(offset) => offset,
                        // Records already fetched stay delivered. Partitions
                        // still out of range are placed on the next poll.
                        Err(Error::Timeout) if !out.is_empty() => {
                            return Ok(self.finish_fetch(out));
                        }
                        Err(error) => return Err(error),
                    };
                    let Some(offset) = reset_position(offset) else {
                        return Err(Error::protocol(format!(
                            "ListOffsets returned no reset offset for {topic}-{partition}"
                        )));
                    };
                    self.advance(&topic, partition, offset);
                    self.set_last_fetched_epoch(
                        &topic,
                        partition,
                        crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    );
                    self.drop_pending_for(&topic, partition);
                    let _ = completed.insert(&topic, partition, ());
                }
            }
            if !fenced.is_empty() {
                fenced.sort();
                fenced.dedup();
                let recovery = self.recover_leader_epochs(&fenced, deadline).await;
                for (topic, partition) in &fenced {
                    if let Ok(cursor) = self.fetch_cursor(topic, *partition) {
                        out_bytes = out_bytes.saturating_sub(drop_records_at_or_after(
                            &mut out, topic, *partition, cursor,
                        ));
                    }
                }
                match recovery {
                    Ok(()) => retry = retry.merge(FetchRetry::Backoff),
                    // Keep records that are still below the recovered cursor.
                    // A timeout must not roll the successful fetches back.
                    Err(Error::Timeout) if !out.is_empty() => {
                        return Ok(self.finish_fetch(out));
                    }
                    Err(error) => return Err(error),
                }
            }
            if budget_reached {
                return Ok(self.finish_fetch(out));
            }
            let all_done = self
                .assigned
                .iter()
                .all(|(t, p, _)| self.paused.contains(t, *p) || completed.contains(t, *p));
            if !all_done && retry.should_retry() {
                if Instant::now() >= deadline {
                    return if !out.is_empty() {
                        Ok(self.finish_fetch(out))
                    } else {
                        Err(Error::Timeout)
                    };
                }
                if retry.needs_backoff() {
                    self.sleep_retry_backoff(attempt, deadline).await?;
                    attempt = attempt.saturating_add(1);
                    if Instant::now() >= deadline {
                        return if !out.is_empty() {
                            Ok(self.finish_fetch(out))
                        } else {
                            Err(Error::Timeout)
                        };
                    }
                    if retry == FetchRetry::Backoff {
                        let topics: Vec<String> = self
                            .assigned
                            .iter()
                            .map(|(t, _, _)| t.as_ref().to_owned())
                            .collect();
                        if let Err(error) = self
                            .refresh_metadata_timeout(
                                Some(&topics),
                                remaining_offset_timeout(deadline, Instant::now()),
                            )
                            .await
                        {
                            return self.fetched_or_timeout(out, error);
                        }
                    }
                }
                continue;
            }
            return Ok(self.finish_fetch(out));
        }
    }

    /// Deliver records already taken when a later step times out.
    ///
    /// Any other error still fails the fetch, and the caller rolls the cursors
    /// back. A timeout with nothing in hand is a timeout.
    fn fetched_or_timeout(
        &mut self,
        out: Vec<FetchedRecord>,
        error: Error,
    ) -> Result<Vec<FetchedRecord>> {
        if matches!(error, Error::Timeout) && !out.is_empty() {
            Ok(self.finish_fetch(out))
        } else {
            Err(error)
        }
    }

    /// Replies that finished while this poll was dialing or out of time.
    ///
    /// Brokers that are still running stay parked. Nothing ready is a timeout.
    fn ready_fetches_or_timeout(&mut self) -> Result<Vec<FetchReply>> {
        let Some(mut set) = self.inflight_fetches.take() else {
            return Err(Error::Timeout);
        };
        let ready = match reap_fetch_replies(&mut set, &mut self.conns) {
            Ok(ready) => ready,
            Err(error) => {
                self.inflight_fetches = Some(set);
                return Err(error);
            }
        };
        for reply in &ready {
            let _ = self.inflight_nodes.remove(&reply.node);
        }
        if set.is_empty() {
            self.inflight_nodes.clear();
        } else {
            self.inflight_fetches = Some(set);
        }
        if ready.is_empty() {
            Err(Error::Timeout)
        } else {
            Ok(ready)
        }
    }

    async fn fetch_from_leaders(
        &mut self,
        by_leader: HashMap<i32, HashMap<Arc<str>, Vec<FetchPartition>>>,
        deadline: Instant,
        max_wait: i32,
    ) -> Result<Vec<FetchReply>> {
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
            result = self.fetch_from_leaders_io(by_leader, deadline, max_wait) => {
                result
            }
        }
    }

    async fn fetch_from_leaders_io(
        &mut self,
        mut by_leader: HashMap<i32, HashMap<Arc<str>, Vec<FetchPartition>>>,
        deadline: Instant,
        max_wait: i32,
    ) -> Result<Vec<FetchReply>> {
        let mut nodes: Vec<i32> = by_leader.keys().copied().collect();
        nodes.sort_unstable();
        let min_bytes = self.cfg.min_bytes;
        let max_bytes = if self.cfg.buffer_memory > 0 {
            let headroom = self.cfg.buffer_memory.saturating_sub(self.buffered_bytes);
            if self.buffered_bytes == 0 {
                self.cfg.max_bytes
            } else {
                self.cfg
                    .max_bytes
                    .min(i32::try_from(headroom).unwrap_or(i32::MAX))
                    .max(1)
            }
        } else {
            self.cfg.max_bytes
        };
        let isolation_level = self.cfg.isolation_level.as_i8();
        if deadline.saturating_duration_since(Instant::now()).is_zero() {
            return self.ready_fetches_or_timeout();
        }
        let rack = self.cfg.rack.clone();
        let mut name_ids = self.topic_name_ids();
        for by_topic in by_leader.values() {
            for (topic, partitions) in by_topic {
                let current = name_ids.get(topic.as_ref()).copied();
                let mut bound = None;
                for partition in partitions {
                    if let Some(id) = self.assigned_topic_id(topic.as_ref(), partition.partition) {
                        if current.is_some_and(|current| current != id)
                            || bound.is_some_and(|bound| bound != id)
                        {
                            return Err(Error::broker(
                                crate::error::UNKNOWN_TOPIC_ID,
                                "assigned topic identity changed",
                            ));
                        }
                        bound = Some(id);
                    }
                }
                if let Some(id) = bound.filter(|_| current.is_none()) {
                    let _previous = name_ids.insert(topic.to_string(), id);
                }
            }
        }
        let collect_parallel = nodes.len() > 1 || self.inflight_fetches.is_some();
        let mut queue = nodes;
        let mut connected = Vec::new();
        if collect_parallel {
            connected = match self
                .connect_parallel_fetch_nodes(queue, &mut by_leader, deadline)
                .await
            {
                Ok(nodes) => nodes,
                Err(Error::Timeout) => return self.ready_fetches_or_timeout(),
                Err(error) => return Err(error),
            };
        } else {
            while let Some(node) = queue.pop() {
                if connected.contains(&node) {
                    continue;
                }
                let rest = deadline.saturating_duration_since(Instant::now());
                if rest.is_zero() {
                    if connected.is_empty() {
                        return self.ready_fetches_or_timeout();
                    }
                    break;
                }
                let preferred_only =
                    is_preferred_only_target(&self.preferred, &self.cluster, node, &by_leader);
                let connected_ok = if preferred_only {
                    match self.connect_preferred_once(node, deadline).await {
                        Ok(()) => true,
                        Err(error) => {
                            if !reroute_unreachable_preferred(
                                &mut self.preferred,
                                &self.cluster,
                                node,
                                &mut by_leader,
                            ) {
                                if matches!(error, Error::Timeout) {
                                    return self.ready_fetches_or_timeout();
                                }
                                return Err(Error::Timeout);
                            }
                            false
                        }
                    }
                } else {
                    match self
                        .connect_node_inner(node, !collect_parallel, deadline)
                        .await
                    {
                        Ok(()) => true,
                        Err(Error::Timeout) => {
                            if connected.is_empty() {
                                return self.ready_fetches_or_timeout();
                            }
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                };
                if connected_ok {
                    connected.push(node);
                    continue;
                }
                for leader in by_leader.keys().copied() {
                    if !connected.contains(&leader) && !queue.contains(&leader) {
                        queue.push(leader);
                    }
                }
            }
        }
        let nodes: Vec<i32> = connected;
        // Dialing spent part of the deadline. The broker wait and the RPC
        // timeout are the time still left, not the time that remained before
        // the first connection.
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return self.ready_fetches_or_timeout();
        }
        let max_wait = max_wait.min(duration_millis_i32(timeout));
        if !collect_parallel {
            let mut out = Vec::with_capacity(nodes.len());
            for node in nodes {
                let timeout = deadline.saturating_duration_since(Instant::now());
                if timeout.is_zero() {
                    if !out.is_empty() {
                        return Ok(out);
                    }
                    return self.ready_fetches_or_timeout();
                }
                let max_wait = max_wait.min(duration_millis_i32(timeout));
                let Some(by_topic) = by_leader.remove(&node) else {
                    continue;
                };
                let topics = fetch_topics(by_topic, &name_ids);
                let reuse_bootstrap = !self.conns.contains_key(&node)
                    && self
                        .cluster
                        .brokers
                        .get(&node)
                        .is_some_and(|addr| addr == self.conn.addr())
                    && self.conn.fetch_version() >= 0;
                let (fetch_version, body) = {
                    // Each leader speaks its own negotiated Fetch version; a
                    // newer bootstrap never forces its schema on an older
                    // leader (KL03-22, mirrors KL03-11). The same broker's
                    // bootstrap connection already negotiated that schema.
                    let fetch_version = if reuse_bootstrap {
                        self.conn.fetch_version()
                    } else {
                        self.conns
                            .get(&node)
                            .ok_or_else(|| Error::protocol("missing fetch conn"))?
                            .fetch_version()
                    };
                    if fetch_version < 0 {
                        return Err(Error::Unsupported(format!(
                            "broker at {} does not support Fetch v4-17",
                            if reuse_bootstrap {
                                self.conn.addr()
                            } else {
                                self.conns.get(&node).map(|conn| conn.addr()).unwrap_or("")
                            }
                        )));
                    }
                    let request = self
                        .fetch_sessions
                        .entry(node)
                        .or_default()
                        .prepare(topics, fetch_version);
                    let encode = |buf: &mut bytes::BytesMut| {
                        encode_fetch_request_with_forgotten(
                            buf,
                            fetch_version,
                            max_wait,
                            min_bytes,
                            max_bytes,
                            isolation_level,
                            &request.topics,
                            rack.as_deref(),
                            request.metadata,
                            &request.forgotten,
                        )
                    };
                    let body = if reuse_bootstrap {
                        self.conn
                            .roundtrip(FETCH, fetch_version, encode, timeout)
                            .await
                    } else {
                        let conn = self
                            .conns
                            .get_mut(&node)
                            .ok_or_else(|| Error::protocol("missing fetch conn"))?;
                        conn.roundtrip(FETCH, fetch_version, encode, timeout).await
                    };
                    (fetch_version, body)
                };
                out.push(FetchReply {
                    node,
                    version: fetch_version,
                    received_at: Instant::now(),
                    body,
                });
            }
            return Ok(out);
        }
        let mut inflight = InflightFetches {
            set: Some(self.inflight_fetches.take().unwrap_or_default()),
            pending: std::mem::take(&mut self.inflight_nodes),
        };
        for node in nodes {
            if inflight.pending.contains(&node) {
                continue;
            }
            let Some(by_topic) = by_leader.remove(&node) else {
                continue;
            };
            let topics = fetch_topics(by_topic, &name_ids);
            let fetch_version = {
                let conn = self
                    .conns
                    .get(&node)
                    .ok_or_else(|| Error::protocol("missing fetch conn"))?;
                if conn.fetch_version() < 0 {
                    return Err(Error::Unsupported(format!(
                        "broker at {} does not support Fetch v4-17",
                        conn.addr()
                    )));
                }
                conn.fetch_version()
            };
            let mut conn = self
                .conns
                .remove(&node)
                .ok_or_else(|| Error::protocol("missing fetch conn"))?;
            let request = self
                .fetch_sessions
                .entry(node)
                .or_default()
                .prepare(topics, fetch_version);
            let rack = rack.clone();
            let _ = inflight.pending.insert(node);
            let Some(set) = inflight.set.as_mut() else {
                return Err(Error::protocol("inflight fetch set missing"));
            };
            let _ = set.spawn(async move {
                let result = conn
                    .roundtrip(
                        FETCH,
                        fetch_version,
                        |buf| {
                            encode_fetch_request_with_forgotten(
                                buf,
                                fetch_version,
                                max_wait,
                                min_bytes,
                                max_bytes,
                                isolation_level,
                                &request.topics,
                                rack.as_deref(),
                                request.metadata,
                                &request.forgotten,
                            )
                        },
                        timeout,
                    )
                    .await;
                (
                    FetchReply {
                        node,
                        version: fetch_version,
                        received_at: Instant::now(),
                        body: result,
                    },
                    conn,
                )
            });
        }
        let mut out = Vec::new();
        let mut first_reply: Option<Instant> = None;
        while let Some(set) = inflight.set.as_mut().filter(|set| !set.is_empty()) {
            let joined = if let Some(started) = first_reply {
                let grace = multi_leader_grace(
                    started.elapsed(),
                    deadline.saturating_duration_since(Instant::now()),
                );
                if grace.is_zero() {
                    break;
                }
                tokio::select! {
                    biased;
                    joined = set.join_next() => joined,
                    () = tokio::time::sleep(grace) => break,
                }
            } else {
                let rest = deadline.saturating_duration_since(Instant::now());
                if rest.is_zero() {
                    break;
                }
                tokio::select! {
                    biased;
                    joined = set.join_next() => joined,
                    () = tokio::time::sleep(rest) => break,
                }
            };
            let Some(joined) = joined else {
                break;
            };
            let (reply, conn) = joined.map_err(|e| Error::protocol(format!("fetch task: {e}")))?;
            let _ = inflight.pending.remove(&reply.node);
            let _ = self.conns.insert(reply.node, conn);
            if first_reply.is_none() {
                first_reply = Some(reply.received_at);
            }
            out.push(reply);
        }
        // A reply can land in the instant the deadline ends the wait. Take it
        // before parking the brokers that are still running.
        let reaped = if let Some(set) = inflight.set.as_mut() {
            reap_fetch_replies(set, &mut self.conns)
        } else {
            Ok(Vec::new())
        };
        match reaped {
            Ok(ready) => {
                for reply in &ready {
                    let _ = inflight.pending.remove(&reply.node);
                }
                out.extend(ready);
            }
            Err(error) => {
                inflight.park(self);
                return Err(error);
            }
        }
        inflight.park(self);
        if out.is_empty() && Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        out.sort_by_key(|reply| reply.node);
        Ok(out)
    }

    fn observe_fetch_throttle(&mut self, node: i32, millis: i32, received_at: Instant) {
        self.throttle_metrics.observe(millis);
        let Ok(millis) = u64::try_from(millis) else {
            return;
        };
        if millis == 0 {
            return;
        }
        if let Some(until) = received_at.checked_add(Duration::from_millis(millis)) {
            // A fast peer's quota can expire while another response is pending;
            // processing the aggregate result must not restart that interval.
            if until > Instant::now() {
                let previous = self.fetch_throttles.entry(node).or_insert(until);
                *previous = (*previous).max(until);
            }
        }
    }

    fn decode_fetch_reply(&mut self, reply: FetchReply) -> DecodedFetchReply {
        let mut recovery = None;
        let session = self.fetch_sessions.get_mut(&reply.node);
        let mut session = session;
        let body = (|| {
            let mut bytes = reply.body?;
            let full = session.as_ref().is_some_and(|s| s.metadata.is_full());
            if let Some(session) = session.as_mut() {
                if let Err(error) = session.response(&bytes, reply.version) {
                    if let Error::Broker { code, .. } = &error {
                        recovery = match *code {
                            error::FETCH_SESSION_ID_NOT_FOUND
                            | error::INVALID_FETCH_SESSION_EPOCH => Some(FetchRetry::Session),
                            error::FETCH_SESSION_TOPIC_ID_ERROR => Some(FetchRetry::Backoff),
                            _ => None,
                        };
                    }
                    return Err(error);
                }
            }
            let (topics, raw, endpoints, _, _, throttle) =
                decode_fetch_response_raw(&mut bytes, reply.version)?;
            let batches = raw
                .into_iter()
                .map(|partitions| {
                    partitions
                        .into_iter()
                        .map(split_record_batches)
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            if reply.version >= 7 {
                if let Some(session) = session.as_mut() {
                    // Apache treats an empty throttled full response as a
                    // declined session. The quota scheduler owns the next wait.
                    if full && throttle > 0 && topics.is_empty() {
                        session.metadata = FetchMetadata::INITIAL;
                    } else if let Err(error) =
                        session.validate_partitions(&topics, reply.version, full)
                    {
                        if full {
                            session.metadata = FetchMetadata::INITIAL;
                        }
                        recovery = Some(FetchRetry::Session);
                        return Err(error);
                    }
                }
            }
            Ok(FetchBody {
                topics,
                batches,
                endpoints,
            })
        })();
        if body.is_err() {
            if let Some(session) = session {
                session.reset();
            }
        }
        DecodedFetchReply {
            node: reply.node,
            body,
            recovery,
        }
    }

    fn reset_fetched_offset(
        &mut self,
        name: &str,
        partition: i32,
        log_start: i64,
        high_watermark: i64,
        completed: &mut PartitionMap<()>,
        need_offsets: &mut Vec<(String, i32, i64)>,
    ) -> Result<()> {
        if log_boundary_conflict(log_start, high_watermark) {
            return Err(Error::protocol(format!(
                "Fetch {name}-{partition} log start {log_start} is past high watermark {high_watermark}"
            )));
        }
        match self.cfg.auto_offset_reset {
            crate::AutoOffsetReset::None => Err(Error::broker(
                error::OFFSET_OUT_OF_RANGE,
                format!("{name}-{partition}"),
            )),
            crate::AutoOffsetReset::Earliest => {
                if log_start >= 0 {
                    self.advance_indexed(name, partition, log_start);
                    self.set_last_fetched_epoch(
                        name,
                        partition,
                        crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    );
                    self.drop_pending_for(name, partition);
                    let _ = completed.insert(name, partition, ());
                } else {
                    need_offsets.push((name.to_owned(), partition, crate::EARLIEST_TIMESTAMP));
                }
                Ok(())
            }
            crate::AutoOffsetReset::Latest => {
                if high_watermark >= 0 {
                    self.advance_indexed(name, partition, high_watermark);
                    self.set_last_fetched_epoch(
                        name,
                        partition,
                        crate::RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    );
                    self.drop_pending_for(name, partition);
                    let _ = completed.insert(name, partition, ());
                } else {
                    need_offsets.push((name.to_owned(), partition, crate::LATEST_TIMESTAMP));
                }
                Ok(())
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "private helper accumulates records, byte budgets, error recovery, and completed partitions across leaders"
    )]
    fn apply_fetch_body(
        &mut self,
        node: i32,
        body: FetchBody,
        out: &mut Vec<FetchedRecord>,
        out_bytes: &mut usize,
        fenced: &mut Vec<(String, i32)>,
        need_offsets: &mut Vec<(String, i32, i64)>,
        completed: &mut PartitionMap<()>,
        budget_reached: &mut bool,
    ) -> Result<FetchRetry> {
        let FetchBody {
            topics: fetched,
            batches,
            endpoints,
        } = body;
        let declared = batches
            .iter()
            .flatten()
            .flatten()
            .fold(0usize, |sum, batch| {
                sum.saturating_add(batch.reserve_hint())
            });
        out.reserve(declared);
        self.cluster.apply_node_endpoints(&endpoints);
        let id_names = self.topic_id_names();
        self.ensure_cursor_index();
        let mut retry = FetchRetry::None;
        // A partition listed twice must not rewind the cursor or deliver twice.
        let mut seen_partitions: HashMap<Arc<str>, HashSet<i32>> = HashMap::new();
        for (topic, topic_batches) in fetched.into_iter().zip(batches) {
            if *budget_reached {
                let Some(name) = fetch_topic_name(&topic.topic, topic.topic_id, &id_names)? else {
                    continue;
                };
                let seen_name = Arc::<str>::from(name.as_str());
                for (part, part_batches) in topic.partitions.into_iter().zip(topic_batches) {
                    if !take_fetch_partition(&mut seen_partitions, &seen_name, part.partition) {
                        continue;
                    }
                    if self.cursor_at(&name, part.partition).is_none() {
                        continue;
                    }
                    if part.error_code == 0 {
                        self.retain_held(
                            &name,
                            part.partition,
                            part.last_stable_offset,
                            part_batches,
                        );
                    }
                }
                continue;
            }
            let Some(name) = fetch_topic_name(&topic.topic, topic.topic_id, &id_names)? else {
                continue;
            };
            let seen_name = Arc::<str>::from(name.as_str());
            let topic_name = TopicName::from(name.as_str());
            for (part, part_batches) in topic.partitions.into_iter().zip(topic_batches) {
                if !take_fetch_partition(&mut seen_partitions, &seen_name, part.partition) {
                    continue;
                }
                if self.cursor_at(&name, part.partition).is_none() {
                    continue;
                }
                if *budget_reached {
                    if part.error_code == 0 {
                        self.retain_held(
                            &name,
                            part.partition,
                            part.last_stable_offset,
                            part_batches,
                        );
                    }
                    continue;
                }
                if self.cfg.rack.is_some() {
                    if let Some(replica) = part.preferred_read_replica() {
                        if replica != node {
                            let _prev = self.preferred.insert(&name, part.partition, replica);
                            retry = retry.merge(FetchRetry::Redirect);
                            continue;
                        }
                    }
                }
                if part.error_code == error::OFFSET_OUT_OF_RANGE {
                    let is_leader =
                        self.cluster.leader_id(&name, part.partition).ok() == Some(node);
                    if self.preferred.remove(&name, part.partition).is_some() || !is_leader {
                        let _ = self.preferred.remove(&name, part.partition);
                        retry = retry.merge(FetchRetry::Redirect);
                        continue;
                    }
                    self.reset_fetched_offset(
                        &name,
                        part.partition,
                        part.log_start_offset,
                        part.high_watermark,
                        completed,
                        need_offsets,
                    )?;
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
                        // The follower chosen for the previous leader epoch must
                        // not keep serving this partition after the leader moves.
                        let _ = self.preferred.remove(&name, part.partition);
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
                    self.advance_indexed(&name, part.partition, part.diverging_end_offset);
                    self.set_last_fetched_epoch(&name, part.partition, part.diverging_epoch);
                    self.drop_pending_for(&name, part.partition);
                    retry = retry.merge(FetchRetry::Redirect);
                    continue;
                }
                let req_offset = self.cursor_at(&name, part.partition).unwrap_or(0);
                if position_past_high_watermark(req_offset, part.high_watermark)
                    || position_before_log_start(req_offset, part.log_start_offset)
                {
                    let is_leader =
                        self.cluster.leader_id(&name, part.partition).ok() == Some(node);
                    if self.preferred.remove(&name, part.partition).is_some() || !is_leader {
                        let _ = self.preferred.remove(&name, part.partition);
                        retry = retry.merge(FetchRetry::Redirect);
                        continue;
                    }
                    self.reset_fetched_offset(
                        &name,
                        part.partition,
                        part.log_start_offset,
                        part.high_watermark,
                        completed,
                        need_offsets,
                    )?;
                    continue;
                }
                let isolation = self.cfg.isolation_level;
                let mut aborted = if isolation == crate::IsolationLevel::ReadCommitted {
                    if !self.aborted.contains(&name, part.partition) {
                        let _previous =
                            self.aborted
                                .insert(&name, part.partition, AbortedIntervals::default());
                    }
                    self.aborted.get_mut(&name, part.partition)
                } else {
                    None
                };
                let has_txn_headers = part_batches
                    .iter()
                    .any(|b| b.is_transactional() || b.is_control_batch())
                    || aborted
                        .as_ref()
                        .is_some_and(|state| !state.active.is_empty());

                if let Some(state) = aborted.as_deref_mut() {
                    state.extend(&part.aborted_transactions);
                }

                let mut next: Option<i64> = None;
                let mut last_epoch = crate::RecordBatch::NO_PARTITION_LEADER_EPOCH;
                let mut reached_lso = false;

                let mut encoded_iter = part_batches.into_iter();
                while let Some(encoded) = encoded_iter.next() {
                    let header_end = encoded.header_exclusive_end();
                    let is_control = encoded.is_control_batch();
                    let is_first_batch = out.is_empty() && self.pending.is_empty();
                    let budget = self.cfg.buffer_memory;
                    // Already over budget: keep the framed batch and decode it
                    // after the application drains pending records.
                    if budget > 0
                        && !is_first_batch
                        && !is_control
                        && self.buffered_bytes.saturating_add(*out_bytes) > budget
                    {
                        self.retain_held(
                            &name,
                            part.partition,
                            part.last_stable_offset,
                            std::iter::once(encoded).chain(encoded_iter),
                        );
                        *budget_reached = true;
                        break;
                    }
                    if !is_control
                        && !encoded.is_transactional()
                        && isolation != crate::IsolationLevel::ReadCommitted
                    {
                        let mark = out.len();
                        let mark_bytes = *out_bytes;
                        let mut added = 0usize;
                        let mut added_records = 0u64;
                        // Last record in the batch, including ones below
                        // `req_offset`. A compacted gap can make that offset
                        // plus one larger than `base + count`.
                        let mut last_record_off = None;
                        // Highest `offset + 1` among records that are delivered.
                        let mut included_next: Option<i64> = None;
                        let mut cursor_overflow = false;
                        let visited = encoded.try_visit_short_keys(|rec| {
                            last_record_off = Some(rec.offset);
                            if rec.offset < req_offset
                                || !record_is_visible(rec.offset, part.high_watermark)
                                || !record_is_retained(rec.offset, part.log_start_offset)
                            {
                                return;
                            }
                            let Some(advanced) = exclusive_fetch_offset(rec.offset) else {
                                cursor_overflow = true;
                                return;
                            };
                            let key_len = rec.key.as_ref().map_or(0, |(_, len)| usize::from(*len));
                            let val_len = rec.value.as_ref().map_or(0, Bytes::len);
                            added = added.saturating_add(key_len.saturating_add(val_len));
                            added_records = added_records.saturating_add(1);
                            included_next = Some(
                                included_next.map_or(advanced, |current| current.max(advanced)),
                            );
                            out.push(FetchedRecord {
                                topic: topic_name.clone(),
                                partition: part.partition,
                                offset: rec.offset,
                                timestamp: rec.timestamp,
                                timestamp_type: TimestampType::CreateTime,
                                key: rec.key.map(|(bytes, len)| RecordBytes::Short(bytes, len)),
                                value: rec.value,
                                headers: Vec::new(),
                                leader_epoch: None,
                            });
                        })?;
                        if cursor_overflow {
                            return Err(Error::protocol("fetch cursor overflow"));
                        }
                        if let Some(meta) = visited {
                            let over_budget = budget > 0
                                && !is_first_batch
                                && mark_bytes.saturating_add(added) > budget;
                            if over_budget {
                                out.truncate(mark);
                                *out_bytes = mark_bytes;
                                self.retain_held(
                                    &name,
                                    part.partition,
                                    part.last_stable_offset,
                                    std::iter::once(encoded).chain(encoded_iter),
                                );
                                *budget_reached = true;
                                break;
                            }
                            let timestamp_type = TimestampType::from_attributes(meta.attributes);
                            let epoch = (meta.partition_leader_epoch >= 0)
                                .then_some(meta.partition_leader_epoch);
                            if let Some(tail) = out.get_mut(mark..) {
                                for record in tail {
                                    record.timestamp_type = timestamp_type;
                                    if timestamp_type == TimestampType::LogAppendTime {
                                        record.timestamp = meta.max_timestamp;
                                    }
                                    record.leader_epoch = epoch;
                                }
                            }
                            *out_bytes = mark_bytes.saturating_add(added);
                            if added_records > 0 {
                                let added_bytes = u64::try_from(added).unwrap_or(u64::MAX);
                                if let Some(totals) =
                                    self.fetch_return_totals.get_mut(name.as_str())
                                {
                                    totals.0 = totals.0.saturating_add(added_records);
                                    totals.1 = totals.1.saturating_add(added_bytes);
                                } else {
                                    let _previous = self.fetch_return_totals.insert(
                                        Arc::clone(&seen_name),
                                        (added_records, added_bytes),
                                    );
                                }
                            }
                            let batch_next = fetch_end_after(header_end, last_record_off)?;
                            if let Some(advanced) = included_next {
                                let advanced = cap_fetch_cursor(advanced, part.high_watermark);
                                next = Some(next.map_or(advanced, |c| c.max(advanced)));
                                last_epoch = meta.partition_leader_epoch;
                            }
                            if batch_next >= req_offset {
                                next = Some(next.map_or(
                                    cap_fetch_cursor(batch_next, part.high_watermark),
                                    |c| c.max(cap_fetch_cursor(batch_next, part.high_watermark)),
                                ));
                                last_epoch = meta.partition_leader_epoch;
                            }
                            continue;
                        }
                        out.truncate(mark);
                        *out_bytes = mark_bytes;
                    }
                    let batch = encoded.decode_shared()?;
                    if budget > 0 && !is_first_batch && !is_control {
                        let batch_bytes = batch_records_bytes(&batch.records);
                        let current_total = self.buffered_bytes.saturating_add(*out_bytes);
                        if current_total.saturating_add(batch_bytes) > budget {
                            self.retain_held(
                                &name,
                                part.partition,
                                part.last_stable_offset,
                                std::iter::once(encoded).chain(encoded_iter),
                            );
                            *budget_reached = true;
                            break;
                        }
                    }

                    if isolation == crate::IsolationLevel::ReadCommitted
                        && part.last_stable_offset >= 0
                        && batch.base_offset >= part.last_stable_offset
                    {
                        break;
                    }

                    let batch_last = batch.last_offset();
                    let batch_next = fetch_end_after(
                        header_end,
                        batch.records.last().map(|record| record.offset),
                    )?;

                    if let Some(state) = aborted.as_deref_mut() {
                        state.advance(batch_last);

                        if batch.is_control_batch() {
                            if is_abort_marker(&batch) {
                                state.abort(batch.producer_id);
                            }
                            if batch_next >= req_offset {
                                next = Some(next.map_or(
                                    cap_fetch_cursor(batch_next, part.high_watermark),
                                    |c| c.max(cap_fetch_cursor(batch_next, part.high_watermark)),
                                ));
                                last_epoch = batch.partition_leader_epoch;
                            }
                            continue;
                        }

                        if has_txn_headers && batch.is_transactional() {
                            if let Some(&first) = state.active.get(&batch.producer_id) {
                                if batch.base_offset >= first {
                                    if batch_next >= req_offset {
                                        next = Some(next.map_or(
                                            cap_fetch_cursor(batch_next, part.high_watermark),
                                            |c| {
                                                c.max(cap_fetch_cursor(
                                                    batch_next,
                                                    part.high_watermark,
                                                ))
                                            },
                                        ));
                                        last_epoch = batch.partition_leader_epoch;
                                    }
                                    continue;
                                }
                            }
                        }
                    } else if batch.is_control_batch() {
                        if batch_next >= req_offset {
                            next =
                                Some(next.map_or(
                                    cap_fetch_cursor(batch_next, part.high_watermark),
                                    |c| c.max(cap_fetch_cursor(batch_next, part.high_watermark)),
                                ));
                            last_epoch = batch.partition_leader_epoch;
                        }
                        continue;
                    }

                    let is_transactional = batch.is_transactional();
                    let timestamp_type = batch.timestamp_type();
                    let log_append_timestamp = (timestamp_type == TimestampType::LogAppendTime)
                        .then_some(batch.max_timestamp());
                    let mut batch_records = 0u64;
                    let mut batch_bytes = 0u64;
                    // No transaction state changes within one decoded data batch.
                    let abort_from = aborted.as_ref().and_then(|state| {
                        if !has_txn_headers || is_transactional {
                            state.active.get(&batch.producer_id).copied()
                        } else {
                            None
                        }
                    });
                    for rec in batch.records {
                        let offset = rec.offset;
                        if isolation == crate::IsolationLevel::ReadCommitted
                            && part.last_stable_offset >= 0
                            && offset >= part.last_stable_offset
                        {
                            reached_lso = true;
                            break;
                        }
                        if !record_is_visible(offset, part.high_watermark) {
                            break;
                        }
                        if offset >= req_offset {
                            let advanced = cap_fetch_cursor(
                                exclusive_fetch_offset(offset)
                                    .ok_or_else(|| Error::protocol("fetch cursor overflow"))?,
                                part.high_watermark,
                            );
                            next = Some(next.map_or(advanced, |c| c.max(advanced)));
                            last_epoch = batch.partition_leader_epoch;
                        } else {
                            continue;
                        }
                        if !record_is_retained(offset, part.log_start_offset) {
                            continue;
                        }
                        if isolation == crate::IsolationLevel::ReadCommitted
                            && abort_from.is_some_and(|first| offset >= first)
                        {
                            continue;
                        }
                        let rec_b = single_record_bytes(&rec);
                        *out_bytes = out_bytes.saturating_add(rec_b);
                        batch_records = batch_records.saturating_add(1);
                        batch_bytes = batch_bytes.saturating_add(
                            u64::try_from(
                                rec.key
                                    .as_ref()
                                    .map_or(0, Bytes::len)
                                    .saturating_add(rec.value.as_ref().map_or(0, Bytes::len)),
                            )
                            .unwrap_or(u64::MAX),
                        );
                        let timestamp = log_append_timestamp.unwrap_or(rec.timestamp);
                        out.push(FetchedRecord {
                            topic: topic_name.clone(),
                            partition: part.partition,
                            offset,
                            timestamp,
                            timestamp_type,
                            key: rec.key.map(RecordBytes::from),
                            value: rec.value,
                            headers: rec.headers,
                            leader_epoch: (batch.partition_leader_epoch >= 0)
                                .then_some(batch.partition_leader_epoch),
                        });
                    }
                    if batch_records > 0 {
                        if let Some(totals) = self.fetch_return_totals.get_mut(name.as_str()) {
                            totals.0 = totals.0.saturating_add(batch_records);
                            totals.1 = totals.1.saturating_add(batch_bytes);
                        } else {
                            let _previous = self
                                .fetch_return_totals
                                .insert(Arc::clone(&seen_name), (batch_records, batch_bytes));
                        }
                    }
                    if reached_lso {
                        break;
                    }
                    if batch_next >= req_offset {
                        next = Some(
                            next.map_or(cap_fetch_cursor(batch_next, part.high_watermark), |c| {
                                c.max(cap_fetch_cursor(batch_next, part.high_watermark))
                            }),
                        );
                        last_epoch = batch.partition_leader_epoch;
                    }
                }
                if let Some(n) = next {
                    self.advance_indexed(&name, part.partition, n);
                    self.set_last_fetched_epoch(&name, part.partition, last_epoch);
                    let _ = completed.insert(&name, part.partition, ());
                } else if !*budget_reached {
                    let _ = completed.insert(&name, part.partition, ());
                }
                if *budget_reached {
                    break;
                }
            }
            if *budget_reached {
                break;
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
    /// [`crate::LATEST_TIERED_TIMESTAMP`] (`-5`),
    /// [`crate::EARLIEST_PENDING_UPLOAD_TIMESTAMP`] (`-6`, requires v11), or milliseconds.
    ///
    /// Negotiates ListOffsets v1–v11 (v6+ flexible; v10+ TimeoutMs). Waits up to
    /// [`ConsumerConfig::request_timeout`]. Each attempt refreshes the selected
    /// leader's range (one ApiVersions RPC); unsupported selector/isolation
    /// combinations fail explicitly. Metadata, connection, lookup and retries
    /// share that budget. For a one-shot timeout, use [`Self::list_offsets_timeout`].
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
        self.reject_pending_wakeup()?;
        require_list_offset_target(topic, partition)?;
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::protocol("ListOffsets deadline overflow"))?;
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            if self.cluster.leader_id(topic, partition).is_err() {
                let topics = [topic.to_string()];
                self.refresh_metadata_timeout(
                    Some(&topics),
                    deadline.saturating_duration_since(Instant::now()),
                )
                .await?;
            }
            let node = self.cluster.leader_id(topic, partition)?;
            self.connect_node(node, deadline).await?;
            let isolation = self.cfg.isolation_level.as_i8();
            let minimum =
                crate::protocol::offsets::minimum_version_for_timestamp(timestamp, isolation);
            let uses_bootstrap = !self.conns.contains_key(&node)
                && self
                    .cluster
                    .brokers
                    .get(&node)
                    .is_some_and(|addr| addr == self.conn.addr());
            let version = if uses_bootstrap {
                    self.versions.get(&LIST_OFFSETS).and_then(|v| {
                        pick_version(v.min_version, v.max_version, minimum, 11)
                    })
                } else if let Some((min_version, max_version)) =
                    self.list_offsets_ranges.get(&node).copied()
                {
                    pick_version(min_version, max_version, minimum, 11)
                } else {
                    // `connect_node` already stored the leader handshake. A missing
                    // range means that handshake omitted ListOffsets.
                    None
                }
                .ok_or_else(|| {
                    Error::Unsupported(format!(
                        "broker does not support requested ListOffsets selector/isolation (requires v{minimum}+)"
                    ))
                })?;
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let current_leader_epoch = self.cluster.leader_epoch(topic, partition);
            let body = {
                let conn = self.broker_conn_mut(node)?;
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
                    self.drop_broker_conn(node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let decoded = (|| {
                let mut cursor = body.as_ref();
                let (topics, _) = decode_list_offsets_topics_response(&mut cursor, version)?;
                if !cursor.is_empty() {
                    return Err(Error::protocol("trailing ListOffsets response bytes"));
                }
                let mut matching = topics
                    .into_iter()
                    .filter(|t| t.name == topic)
                    .flat_map(|t| t.partitions)
                    .filter(|p| p.partition_index == partition);
                let result = matching
                    .next()
                    .ok_or_else(|| Error::protocol("ListOffsets missing requested partition"))?;
                if matching.next().is_some() {
                    return Err(Error::protocol("duplicate ListOffsets result"));
                }
                if result.error_code != 0 {
                    return Err(Error::broker(result.error_code, "ListOffsets"));
                }
                Ok(result)
            })();
            match decoded {
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
                    self.recover_leader_epoch(topic, partition, deadline)
                        .await?;
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) if e.is_retriable() => {
                    // NOT_LEADER_OR_FOLLOWER (6) and friends: Metadata, then the new leader.
                    self.cluster.invalidate_topic(topic);
                    self.drop_broker_conn(node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    let topics = [topic.to_string()];
                    self.refresh_metadata_timeout(
                        Some(&topics),
                        deadline.saturating_duration_since(Instant::now()),
                    )
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
        require_partition(partition)?;
        require_fetch_offset(offset)?;
        // The in-flight request still carries the pre-seek offset.
        self.abort_inflight_fetches();
        if let Some(slot) = self
            .assigned
            .iter_mut()
            .find(|(t, p, _)| t.as_ref() == topic && *p == partition)
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
            .map(|(t, p, _)| (t.as_ref().to_owned(), *p))
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
        require_unique_offset_partitions(
            partitions
                .iter()
                .map(|(topic, partition)| (topic.as_str(), *partition)),
        )?;
        let deadline = offset_lookup_deadline(self.cfg.request_timeout)?;
        for (topic, partition) in partitions {
            let offset = self
                .list_offsets_timeout(
                    topic.clone(),
                    partition,
                    timestamp,
                    remaining_offset_timeout(deadline, Instant::now()),
                )
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
        self.reject_pending_wakeup()?;
        Topic::validate(&topic)?;
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
        self.reject_pending_wakeup()?;
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

    /// High watermark minus the delivered position (Java `currentLag`).
    ///
    /// Buffered records still count as lag. A position past the high
    /// watermark reports `0`. `None` when the high watermark is unknown
    /// (`-1`). An unassigned partition is Java `IllegalStateException`
    /// (`No current assignment for partition`).
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
        let fetch_cursor = self
            .assigned
            .iter()
            .find(|(t, p, _)| t.as_ref() == tp.topic.as_str() && *p == tp.partition)
            .map(|(_, _, o)| *o)
            .ok_or_else(|| reject_java_no_current_assignment(&tp.topic, tp.partition))?;
        let pos = self.delivered_position(&tp.topic, tp.partition, fetch_cursor);
        let hw = self
            .list_offsets_timeout(
                tp.topic.clone(),
                tp.partition,
                crate::LATEST_TIMESTAMP,
                timeout,
            )
            .await?;
        Ok(partition_lag(hw, pos))
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
        require_unique_offset_partitions(
            queries
                .iter()
                .map(|(tp, _)| (tp.topic.as_str(), tp.partition)),
        )?;
        let deadline = offset_lookup_deadline(timeout)?;
        let mut out = Vec::new();
        for (tp, timestamp) in queries {
            let (offset, ts, epoch) = self
                .list_offset_at(
                    &tp.topic,
                    tp.partition,
                    timestamp,
                    remaining_offset_timeout(deadline, Instant::now()),
                )
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
        let partitions: Vec<TopicPartition> = partitions.into_iter().map(Into::into).collect();
        require_unique_offset_partitions(
            partitions
                .iter()
                .map(|tp| (tp.topic.as_str(), tp.partition)),
        )?;
        let deadline = offset_lookup_deadline(timeout)?;
        let mut out = Vec::new();
        for tp in partitions {
            let offset = self
                .list_offsets_timeout(
                    tp.topic.clone(),
                    tp.partition,
                    timestamp,
                    remaining_offset_timeout(deadline, Instant::now()),
                )
                .await?;
            out.push((tp, offset));
        }
        Ok(out)
    }

    fn drop_pending_for(&mut self, topic: &str, partition: i32) {
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_sub(self.pending.drop_for(topic, partition));
        self.drop_retained_for(topic, partition);
        let _state = self.aborted.remove(topic, partition);
    }

    fn retain_pending_assigned(&mut self) {
        let mut assigned = PartitionMap::new();
        for (topic, partition, _) in &self.assigned {
            let _previous = assigned.insert(topic, *partition, ());
        }
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_sub(self.pending.retain_assigned(&assigned));
        self.retain_held_assigned(&assigned);
    }

    fn drop_retained_for(&mut self, topic: &str, partition: i32) {
        let Some(held) = self.retained.remove(topic, partition) else {
            return;
        };
        for batch in held.batches {
            self.retained_bytes = self.retained_bytes.saturating_sub(batch.framed_len());
        }
    }

    fn retain_held_assigned(&mut self, assigned: &PartitionMap<()>) {
        let topics = std::mem::take(&mut self.retained.topics);
        for (topic, partitions) in topics {
            for (partition, held) in partitions {
                if assigned.contains(&topic, partition) {
                    let _previous = self.retained.insert(&topic, partition, held);
                } else {
                    for batch in held.batches {
                        self.retained_bytes =
                            self.retained_bytes.saturating_sub(batch.framed_len());
                    }
                }
            }
        }
    }

    /// Keep framed batches so a later poll can decode them without another fetch.
    ///
    /// `buffer_memory == 0` keeps every batch. Otherwise retained framed bytes
    /// stay within `max(buffer_memory, max_bytes)`, except one batch may be
    /// kept when the partition has nothing held yet so that batch is not
    /// downloaded again. Decoded payload remains under `buffer_memory`.
    fn retain_held(
        &mut self,
        topic: &str,
        partition: i32,
        last_stable_offset: i64,
        batches: impl IntoIterator<Item = EncodedBatch>,
    ) {
        let incoming: Vec<EncodedBatch> = batches.into_iter().collect();
        if incoming.is_empty() {
            return;
        }
        if self.retained.get(topic, partition).is_none() {
            let _previous = self.retained.insert(
                topic,
                partition,
                HeldFetch {
                    last_stable_offset,
                    batches: VecDeque::new(),
                },
            );
        }
        let Some(held) = self.retained.get_mut(topic, partition) else {
            return;
        };
        held.last_stable_offset = last_stable_offset;
        // Decoded records stay within buffer_memory. Framed batches already
        // fetched are kept up to the larger of that budget and max_bytes,
        // which is the response the broker was asked to send.
        let cap = if self.cfg.buffer_memory == 0 {
            0
        } else {
            self.cfg
                .buffer_memory
                .max(usize::try_from(self.cfg.max_bytes.max(0)).unwrap_or(usize::MAX))
        };
        for batch in incoming {
            let size = batch.framed_len();
            let fits = cap == 0 || self.retained_bytes.saturating_add(size) <= cap;
            if !fits && !held.batches.is_empty() {
                break;
            }
            self.retained_bytes = self.retained_bytes.saturating_add(size);
            held.batches.push_back(batch);
            if !fits {
                break;
            }
        }
        let empty = held.batches.is_empty();
        if empty {
            self.drop_retained_for(topic, partition);
        }
    }

    /// Move unpaused held batches into a fetch body. Paused partitions stay held.
    fn detach_unpaused_retained(&mut self) -> Option<FetchBody> {
        if self.retained.is_empty() {
            return None;
        }
        let taken = std::mem::replace(&mut self.retained, PartitionMap::new());
        let mut topics = Vec::new();
        let mut all_batches = Vec::new();
        for (topic, partitions) in taken.topics {
            let mut parts = Vec::new();
            let mut topic_batches = Vec::new();
            for (partition, held) in partitions {
                if self.paused.contains(&topic, partition) {
                    let _previous = self.retained.insert(&topic, partition, held);
                    continue;
                }
                if held.batches.is_empty() {
                    continue;
                }
                let bytes = held
                    .batches
                    .iter()
                    .map(EncodedBatch::framed_len)
                    .fold(0usize, usize::saturating_add);
                self.retained_bytes = self.retained_bytes.saturating_sub(bytes);
                let mut part =
                    crate::protocol::fetch::FetchedPartition::partition_response(partition, 0);
                part.last_stable_offset = held.last_stable_offset;
                parts.push(part);
                topic_batches.push(held.batches.into_iter().collect());
            }
            if !parts.is_empty() {
                topics.push(crate::protocol::fetch::FetchedTopic {
                    topic,
                    topic_id: [0; 16],
                    partitions: parts,
                });
                all_batches.push(topic_batches);
            }
        }
        if topics.is_empty() {
            None
        } else {
            Some(FetchBody {
                topics,
                batches: all_batches,
                endpoints: Vec::new(),
            })
        }
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
        for rec in &recs {
            self.buffered_bytes = self.buffered_bytes.saturating_add(record_bytes(rec));
        }
        self.pending.extend(recs);
        self.drain_pending()
    }

    fn drain_pending(&mut self) -> Vec<FetchedRecord> {
        let cap = self.cfg.max_poll_records.unwrap_or(usize::MAX);
        self.pending
            .drain(&self.paused, cap, &mut self.buffered_bytes)
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
    /// Reset a session within the request budget, keeping its healthy socket.
    Session,
}

impl FetchRetry {
    fn should_retry(self) -> bool {
        self != Self::None
    }

    fn needs_backoff(self) -> bool {
        matches!(self, Self::Backoff | Self::Session)
    }

    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Backoff, _) | (_, Self::Backoff) => Self::Backoff,
            (Self::Session, _) | (_, Self::Session) => Self::Session,
            (Self::Redirect, _) | (_, Self::Redirect) => Self::Redirect,
            (Self::None, Self::None) => Self::None,
        }
    }
}

fn is_abort_marker(batch: &crate::protocol::records::RecordBatch) -> bool {
    if !batch.is_control_batch() {
        return false;
    }
    batch
        .records
        .first()
        .and_then(|r| r.key.as_ref())
        .is_some_and(|k| {
            crate::protocol::records::ControlRecordType::parse(k)
                .is_ok_and(|t| t == crate::protocol::records::ControlRecordType::Abort)
        })
}

fn fetch_topics<T, I>(
    by_topic: HashMap<T, Vec<FetchPartition>>,
    name_ids: &HashMap<I, [u8; 16]>,
) -> Vec<FetchTopic>
where
    T: std::hash::Hash + Eq + AsRef<str>,
    I: std::hash::Hash + Eq + std::borrow::Borrow<str>,
{
    by_topic
        .into_iter()
        .map(|(topic, partitions)| FetchTopic {
            topic_id: name_ids.get(topic.as_ref()).copied().unwrap_or([0u8; 16]),
            topic: topic.as_ref().to_owned(),
            partitions,
        })
        .collect()
}

/// Next fetch offset when `position` sits past the epoch end.
///
/// [`crate::protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH_OFFSET`] is not a
/// log position. A successful lookup with that sentinel must not rewind.
/// Java `SubscriptionState.partitionLag`: unknown watermark is empty, and a
/// position past the high watermark is zero rather than negative.
/// A successful Fetch whose position is past the high watermark was truncated
/// without an out-of-range error. The high watermark itself is still readable.
fn position_past_high_watermark(position: i64, high_watermark: i64) -> bool {
    high_watermark >= 0 && position > high_watermark
}

/// The requested offset was deleted when it is below a known log start.
fn position_before_log_start(position: i64, log_start: i64) -> bool {
    log_start >= 0 && position < log_start
}

/// A log start past the high watermark cannot be used to reset a fetch.
fn log_boundary_conflict(log_start: i64, high_watermark: i64) -> bool {
    log_start >= 0 && high_watermark >= 0 && log_start > high_watermark
}

/// A record is visible when it is strictly below a known high watermark.
fn record_is_visible(offset: i64, high_watermark: i64) -> bool {
    high_watermark < 0 || offset < high_watermark
}

/// A record remains in the log when it is at or after a known log start.
fn record_is_retained(offset: i64, log_start: i64) -> bool {
    log_start < 0 || offset >= log_start
}

/// Offset after `offset`. [`i64::MAX`] has no exclusive end.
fn exclusive_fetch_offset(offset: i64) -> Option<i64> {
    offset.checked_add(1)
}

/// Fetch cursor after a batch.
///
/// The end is the later of the last record's exclusive offset and the
/// header's exclusive end (`baseOffset + lastOffsetDelta + 1`). Compaction
/// keeps that header end after it deletes the tail, so a cursor that stopped
/// on the last remaining record would fetch the same batch again. A header
/// end that does not fit must not fail the fetch when the last record still
/// has an end.
fn fetch_end_after(header_end: Option<i64>, last_record: Option<i64>) -> Result<i64> {
    let from_record = match last_record {
        Some(offset) => Some(
            exclusive_fetch_offset(offset)
                .ok_or_else(|| Error::protocol("fetch cursor overflow"))?,
        ),
        None => None,
    };
    match (from_record, header_end) {
        (Some(record_end), Some(header_end)) => Ok(record_end.max(header_end)),
        (Some(record_end), None) => Ok(record_end),
        (None, Some(header_end)) => Ok(header_end),
        (None, None) => Err(Error::protocol("fetch cursor overflow")),
    }
}

/// Fetch cursor after a decoded batch that has no header `lastOffsetDelta`.
///
/// The count end stands in for that header. The live fetch path reads the
/// header end from the framed batch instead.
#[cfg(test)]
fn batch_fetch_end(batch: &crate::RecordBatch) -> Result<i64> {
    let count_end = i64::try_from(batch.records.len())
        .ok()
        .and_then(|count| batch.base_offset.checked_add(count));
    fetch_end_after(count_end, batch.records.last().map(|record| record.offset))
}

/// Do not advance a fetch cursor to or past a known high watermark.
fn cap_fetch_cursor(next: i64, high_watermark: i64) -> i64 {
    if high_watermark >= 0 {
        next.min(high_watermark)
    } else {
        next
    }
}

/// Partitions whose stored topic id disagrees with the latest Metadata id.
///
/// A missing stored id is the first time the assignment learns the id, not a
/// recreation. A zero metadata id does not replace a stored id.
/// Name-to-id and id-to-name maps for one Metadata response.
///
/// `cached` is the leader cache's topic id. It replaces a zero id written by
/// a later copy of the same name. A zero id does not erase an id already
/// taken from an earlier copy.
fn resolved_topic_identities<'a>(
    topics: impl IntoIterator<Item = (&'a str, [u8; 16])>,
    cached: &HashMap<String, [u8; 16]>,
) -> (HashMap<String, [u8; 16]>, HashMap<[u8; 16], String>) {
    let mut by_name = HashMap::new();
    let mut by_id = HashMap::new();
    let mut ambiguous = HashSet::new();
    for (name, metadata_id) in topics {
        let id = cached
            .get(name)
            .copied()
            .filter(|id| *id != [0; 16])
            .unwrap_or(metadata_id);
        if id == [0; 16] && by_name.get(name).is_some_and(|current| *current != [0; 16]) {
            continue;
        }
        let _previous = by_name.insert(name.to_owned(), id);
        if id == [0; 16] || ambiguous.contains(&id) {
            continue;
        }
        match by_id.get(&id).map(String::as_str) {
            Some(existing) if existing != name => {
                let _removed = by_id.remove(&id);
                let _inserted = ambiguous.insert(id);
            }
            Some(_) => {}
            None => {
                let _previous = by_id.insert(id, name.to_owned());
            }
        }
    }
    (by_name, by_id)
}

/// Name to use for one Fetch topic.
///
/// A non-zero topic id selects the subscribed name. A response name that
/// disagrees with that id is rejected. An unknown id does not borrow another
/// topic's name; an empty response name is then skipped.
fn fetch_topic_name(
    response_name: &str,
    topic_id: [u8; 16],
    id_names: &HashMap<[u8; 16], String>,
) -> Result<Option<String>> {
    if topic_id != [0; 16] {
        if let Some(mapped) = id_names.get(&topic_id) {
            if !response_name.is_empty() && response_name != mapped.as_str() {
                return Err(Error::protocol(
                    "fetch topic id does not match the topic name",
                ));
            }
            return Ok(Some(mapped.clone()));
        }
    }
    if response_name.is_empty() {
        Ok(None)
    } else {
        Ok(Some(response_name.to_owned()))
    }
}

/// First copy of a fetch partition wins. A later copy is ignored.
///
/// The topic name is stored once. Later partitions of that topic only record
/// the partition index.
fn take_fetch_partition(
    seen: &mut HashMap<Arc<str>, HashSet<i32>>,
    topic: &Arc<str>,
    partition: i32,
) -> bool {
    if let Some(parts) = seen.get_mut(topic.as_ref()) {
        return parts.insert(partition);
    }
    let mut parts = HashSet::new();
    let inserted = parts.insert(partition);
    let _ = seen.insert(Arc::clone(topic), parts);
    inserted
}

/// Partition indexes for one topic across every copy in a Metadata response.
///
/// A later successful copy supplies indexes even when an earlier copy has a
/// topic error. Negative indexes are omitted. The result is sorted and unique.
enum ListedPartitions {
    Missing,
    Error(i16),
    Indexes(Vec<i32>),
}

fn listed_partition_indexes<'a>(
    entries: impl IntoIterator<Item = (i16, &'a [i32])>,
) -> ListedPartitions {
    let mut saw = false;
    let mut error = None;
    let mut indexes = Vec::new();
    for (error_code, partitions) in entries {
        saw = true;
        if error_code != 0 {
            if error.is_none() {
                error = Some(error_code);
            }
            continue;
        }
        error = None;
        indexes.extend(partitions.iter().copied().filter(|index| *index >= 0));
    }
    if !saw {
        return ListedPartitions::Missing;
    }
    if indexes.is_empty() {
        if let Some(code) = error {
            return ListedPartitions::Error(code);
        }
    }
    indexes.sort_unstable();
    indexes.dedup();
    ListedPartitions::Indexes(indexes)
}

fn recreated_topic_partitions(
    assigned: &[(Arc<str>, i32, i64)],
    assigned_ids: &HashMap<Arc<str>, HashMap<i32, [u8; 16]>>,
    metadata_ids: &HashMap<String, [u8; 16]>,
) -> Vec<(String, i32, [u8; 16])> {
    let mut changed = Vec::new();
    for (topic, partition, _) in assigned {
        let Some(id) = metadata_ids
            .get(topic.as_ref())
            .copied()
            .filter(|id| *id != [0; 16])
        else {
            continue;
        };
        if assigned_ids
            .get(topic.as_ref())
            .and_then(|parts| parts.get(partition))
            .is_some_and(|current| *current != id)
        {
            changed.push((topic.to_string(), *partition, id));
        }
    }
    changed
}

/// A negative offset is the reset marker, not a consumable position.
fn defined_position(offset: i64) -> Option<i64> {
    if offset < 0 {
        None
    } else {
        Some(offset)
    }
}

fn partition_lag(high_watermark: i64, position: i64) -> Option<i64> {
    if high_watermark < 0 || position < 0 {
        None
    } else {
        Some(high_watermark.saturating_sub(position).max(0))
    }
}

fn require_list_offset_target(topic: &str, partition: i32) -> Result<()> {
    Topic::validate(topic)?;
    require_partition(partition)
}

fn require_unique_offset_partitions<'a>(
    partitions: impl IntoIterator<Item = (&'a str, i32)>,
) -> Result<()> {
    let mut seen = HashSet::new();
    for (topic, partition) in partitions {
        require_list_offset_target(topic, partition)?;
        if !seen.insert((topic.to_owned(), partition)) {
            return Err(Error::protocol("duplicate offset partition"));
        }
    }
    Ok(())
}

fn require_partition(partition: i32) -> Result<()> {
    if partition < 0 {
        Err(Error::protocol(
            "partition index must not be a negative number",
        ))
    } else {
        Ok(())
    }
}

fn require_fetch_offset(offset: i64) -> Result<()> {
    if offset < 0 {
        Err(Error::protocol("seek offset must not be a negative number"))
    } else {
        Ok(())
    }
}

/// One deadline for every partition in a ListOffsets batch.
///
/// Java `beginningOffsets`, `endOffsets`, and `offsetsForTimes` spend the
/// caller timeout once. A later partition sees only the time still left.
pub(crate) fn offset_lookup_deadline(timeout: Duration) -> Result<Instant> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::protocol("ListOffsets deadline overflow"))
}

pub(crate) fn remaining_offset_timeout(deadline: Instant, now: Instant) -> Duration {
    deadline.saturating_duration_since(now)
}

/// A ListOffsets reset position. Offset `-1` means the broker has no offset.
pub(crate) fn reset_position(offset: i64) -> Option<i64> {
    (offset >= 0).then_some(offset)
}

fn drop_records_at_or_after(
    records: &mut Vec<FetchedRecord>,
    topic: &str,
    partition: i32,
    cursor: i64,
) -> usize {
    let mut removed = 0usize;
    records.retain(|record| {
        let keep = record.topic.as_str() != topic
            || record.partition != partition
            || record.offset < cursor;
        if !keep {
            removed = removed.saturating_add(record_bytes(record));
        }
        keep
    });
    removed
}

fn truncated_fetch_offset(position: i64, end_offset: i64) -> Option<i64> {
    if end_offset >= 0 && position > end_offset {
        Some(end_offset)
    } else {
        None
    }
}

fn offset_for_leader_epoch_topics(
    cluster: &Cluster,
    last_fetched: &PartitionMap<i32>,
    parts: &[(String, i32)],
) -> Vec<OffsetForLeaderTopic> {
    let mut by_topic: HashMap<String, Vec<OffsetForLeaderPartition>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for (topic, partition) in parts {
        let current = cluster.leader_epoch(topic, *partition);
        // KIP-320: current_leader_epoch fences the leader. The epoch to resolve
        // is the one already fetched. An unknown fetch epoch keeps the current
        // epoch so a broker does not treat -1 as older than the log.
        let looked_up = last_fetched
            .get(topic, *partition)
            .copied()
            .filter(|epoch| *epoch >= 0)
            .unwrap_or(current);
        let part = OffsetForLeaderPartition::new(*partition, current, looked_up);
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

fn record_bytes(rec: &FetchedRecord) -> usize {
    let k = rec.key.as_ref().map_or(0, |key| key.len());
    let v = rec.value.as_ref().map_or(0, Bytes::len);
    let h: usize = rec
        .headers
        .iter()
        .map(|h| {
            h.key
                .len()
                .saturating_add(h.value.as_ref().map_or(0, Bytes::len))
        })
        .fold(0, usize::saturating_add);
    k.saturating_add(v).saturating_add(h)
}

fn single_record_bytes(rec: &crate::protocol::records::Record) -> usize {
    let k = rec.key.as_ref().map_or(0, Bytes::len);
    let v = rec.value.as_ref().map_or(0, Bytes::len);
    let h: usize = rec
        .headers
        .iter()
        .map(|h| {
            h.key
                .len()
                .saturating_add(h.value.as_ref().map_or(0, Bytes::len))
        })
        .fold(0, usize::saturating_add);
    k.saturating_add(v).saturating_add(h)
}

fn batch_records_bytes(records: &[crate::protocol::records::Record]) -> usize {
    records
        .iter()
        .map(single_record_bytes)
        .fold(0, usize::saturating_add)
}

fn fetched_bytes(rec: &FetchedRecord) -> u64 {
    let k = rec.key.as_ref().map(|key| key.len()).unwrap_or(0);
    let v = rec.value.as_ref().map(Bytes::len).unwrap_or(0);
    u64::try_from(k.saturating_add(v)).unwrap_or(u64::MAX)
}

fn epoch_at(epochs: &HashMap<String, Vec<i32>>, topic: &str, partition: i32) -> i32 {
    usize::try_from(partition)
        .ok()
        .and_then(|index| {
            epochs
                .get(topic)
                .and_then(|values| values.get(index).copied())
        })
        .unwrap_or(crate::RecordBatch::NO_PARTITION_LEADER_EPOCH)
}

/// True when every partition on `node` is a rack hint for a different leader.
fn is_preferred_only_target<T>(
    preferred: &PartitionMap<i32>,
    cluster: &Cluster,
    node: i32,
    by_leader: &HashMap<i32, HashMap<T, Vec<FetchPartition>>>,
) -> bool
where
    T: std::hash::Hash + Eq + AsRef<str>,
{
    let Some(by_topic) = by_leader.get(&node) else {
        return false;
    };
    let mut saw_partition = false;
    for (topic, parts) in by_topic {
        for part in parts {
            saw_partition = true;
            if preferred.get(topic.as_ref(), part.partition).copied() != Some(node) {
                return false;
            }
            match cluster.leader_id(topic.as_ref(), part.partition) {
                Ok(leader) if leader != node => {}
                _ => return false,
            }
        }
    }
    saw_partition
}

/// Move partitions off a preferred replica that could not be dialed.
///
/// The hint is dropped and the partitions return to their metadata leaders.
/// A node that is itself the leader is left unchanged so the caller reports
/// the connection failure.
fn reroute_unreachable_preferred<T>(
    preferred: &mut PartitionMap<i32>,
    cluster: &Cluster,
    node: i32,
    by_leader: &mut HashMap<i32, HashMap<T, Vec<FetchPartition>>>,
) -> bool
where
    T: std::hash::Hash + Eq + Clone + AsRef<str>,
{
    if !is_preferred_only_target(preferred, cluster, node, by_leader) {
        return false;
    }
    let Some(by_topic) = by_leader.remove(&node) else {
        return false;
    };
    for (topic, parts) in by_topic {
        for part in parts {
            let leader = cluster
                .leader_id(topic.as_ref(), part.partition)
                .unwrap_or(node);
            if leader != node {
                let _removed = preferred.remove(topic.as_ref(), part.partition);
            }
            by_leader
                .entry(leader)
                .or_default()
                .entry(topic.clone())
                .or_default()
                .push(part);
        }
    }
    true
}

/// Use a rack-local replica only when its address is already cached.
///
/// Fetch can name a preferred replica before Metadata or NodeEndpoints have
/// supplied a dialable host. That id stays remembered, and this fetch uses the
/// leader until the address arrives.
fn fetch_target(preferred: Option<i32>, preferred_known: bool, leader: Option<i32>) -> Option<i32> {
    match preferred {
        Some(node) if preferred_known => Some(node),
        _ => leader,
    }
}

/// Preferred replicas belong to a leader epoch. Drop one when that epoch changes.
fn preferred_partitions_to_drop(
    preferred: &[(String, i32)],
    previous: &HashMap<String, Vec<i32>>,
    current: &HashMap<String, Vec<i32>>,
) -> Vec<(String, i32)> {
    preferred
        .iter()
        .filter(|(topic, partition)| {
            epoch_at(previous, topic, *partition) != epoch_at(current, topic, *partition)
        })
        .cloned()
        .collect()
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
    let mut order = Vec::new();
    let mut parts: HashMap<String, Vec<PartitionMetadata>> = HashMap::new();
    let mut topic_error: HashMap<String, i16> = HashMap::new();
    let mut first_error: Option<(String, i16)> = None;
    for tmd in &md.topics {
        let Some(name) = tmd.name.as_ref() else {
            continue;
        };
        if only.is_some_and(|wanted| name != wanted) {
            continue;
        }
        if tmd.error_code != 0 {
            if first_error.is_none() {
                first_error = Some((name.clone(), tmd.error_code));
            }
            let _ = topic_error.entry(name.clone()).or_insert(tmd.error_code);
            continue;
        }
        if !parts.contains_key(name) {
            order.push(name.clone());
        }
        let slot = parts.entry(name.clone()).or_default();
        for partition in &tmd.partitions {
            if partition.partition_index < 0 {
                continue;
            }
            if slot
                .iter()
                .any(|existing| existing.partition_index == partition.partition_index)
            {
                continue;
            }
            slot.push(partition.clone());
        }
    }
    if let Some(wanted) = only {
        if !parts.contains_key(wanted) {
            if let Some(code) = topic_error.get(wanted) {
                return Err(Error::broker(*code, wanted.to_string()));
            }
            return Ok(Vec::new());
        }
    } else if parts.is_empty() {
        if let Some((name, code)) = first_error {
            return Err(Error::broker(code, name));
        }
    }
    let mut out = Vec::new();
    for name in order {
        if let Some(partitions) = parts.get(&name) {
            for partition in partitions {
                out.push(PartitionInfo::from_partition_metadata(&name, partition));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_topic(id: [u8; 16], offset: i64) -> FetchTopic {
        FetchTopic {
            topic: "t".into(),
            topic_id: id,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 1,
                fetch_offset: offset,
                last_fetched_epoch: -1,
                log_start_offset: -1,
                partition_max_bytes: 1024,
                replica_directory_id: [0; 16],
            }],
        }
    }

    #[test]
    fn fetch_session_reuses_the_topic_name() {
        let mut session = BrokerFetchSession::default();
        let _ = session.prepare(vec![session_topic([1; 16], 0)], 7);
        let first = Arc::clone(
            session
                .partitions
                .get_key_value("t")
                .map(|(name, _)| name)
                .unwrap(),
        );
        let _ = session.prepare(vec![session_topic([1; 16], 4)], 7);
        let second = session
            .partitions
            .get_key_value("t")
            .map(|(name, _)| name)
            .unwrap();
        assert!(Arc::ptr_eq(&first, second));
    }

    fn session_header(code: i16, id: i32) -> Bytes {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&code.to_be_bytes());
        bytes.extend_from_slice(&id.to_be_bytes());
        Bytes::from(bytes)
    }

    fn fetched_at(topic: &str, partition: i32, offset: i64) -> FetchedRecord {
        FetchedRecord {
            topic: TopicName::from(topic),
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
    fn pending_prepend_keeps_returned_records_ahead_of_the_buffer() {
        let mut pending = PendingRecords::new();
        pending.extend(vec![fetched_at("t", 0, 1), fetched_at("t", 1, 4)]);
        pending.prepend(vec![fetched_at("t", 0, 0), fetched_at("t", 1, 3)]);
        let mut bytes = 0usize;
        let drained = pending.drain(&PartitionMap::new(), 10, &mut bytes);
        assert_eq!(
            drained
                .iter()
                .map(|record| (record.partition, record.offset))
                .collect::<Vec<_>>(),
            vec![(0, 0), (1, 3), (0, 1), (1, 4)]
        );
    }

    #[test]
    fn a_later_topic_copy_is_listed_when_the_first_has_an_error() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let partition = |index: i32, leader: i32| PartitionMetadata {
            error_code: 0,
            partition_index: index,
            leader_id: leader,
            leader_epoch: 1,
            replica_nodes: vec![leader],
            isr_nodes: vec![leader],
            offline_replicas: Vec::new(),
        };
        let topic = |name: &str, error_code: i16, parts: Vec<PartitionMetadata>| TopicMetadata {
            error_code,
            name: Some(name.into()),
            topic_id: [0; 16],
            is_internal: false,
            partitions: parts,
            topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        };
        let md = MetadataResponse {
            throttle_time_ms: 0,
            brokers: Vec::new(),
            cluster_id: None,
            controller_id: 1,
            topics: vec![
                topic("t", 5, Vec::new()),
                topic(
                    "t",
                    0,
                    vec![partition(1, 2), partition(0, 1), partition(0, 9)],
                ),
                topic("u", 3, Vec::new()),
                topic("v", 0, vec![partition(0, 1)]),
            ],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let listed = partition_infos_from(&md, None).unwrap();
        let topics: Vec<_> = listed.iter().map(|info| info.partition()).collect();
        assert_eq!(topics, vec![1, 0, 0]);
        assert_eq!(listed[0].topic(), "t");
        assert_eq!(listed[1].topic(), "t");
        assert_eq!(listed[1].leader(), 1);
        assert_eq!(listed[2].topic(), "v");
        assert!(partition_infos_from(&md, Some("u")).is_err());
        assert!(partition_infos_from(&md, Some("missing"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_later_metadata_copy_supplies_partition_indexes() {
        let none: &[(i16, &[i32])] = &[];
        let empty: &[i32] = &[];
        assert!(matches!(
            listed_partition_indexes(none.iter().copied()),
            ListedPartitions::Missing
        ));
        assert!(matches!(
            listed_partition_indexes([(3, empty)]),
            ListedPartitions::Error(3)
        ));
        assert!(matches!(
            listed_partition_indexes([(5, empty), (0, &[1, 0, 0, -1][..])]),
            ListedPartitions::Indexes(ref indexes) if indexes == &[0, 1]
        ));
        assert!(matches!(
            listed_partition_indexes([(0, &[0, 1][..]), (5, empty)]),
            ListedPartitions::Indexes(ref indexes) if indexes == &[0, 1]
        ));
    }

    #[test]
    fn a_repeated_fetch_partition_is_applied_once() {
        let topic_t = Arc::<str>::from("t");
        let topic_u = Arc::<str>::from("u");
        let mut seen = HashMap::new();
        assert!(take_fetch_partition(&mut seen, &topic_t, 0));
        assert!(!take_fetch_partition(&mut seen, &topic_t, 0));
        assert!(take_fetch_partition(&mut seen, &topic_t, 1));
        assert!(take_fetch_partition(&mut seen, &topic_u, 0));
        assert!(Arc::ptr_eq(
            seen.get_key_value("t").map(|(name, _)| name).unwrap(),
            &topic_t
        ));
    }

    #[test]
    fn a_topic_id_shared_by_two_names_is_not_resolved() {
        let (by_name, by_id) = resolved_topic_identities(
            [("orders", [7u8; 16]), ("trades", [7u8; 16])],
            &HashMap::new(),
        );
        assert_eq!(by_name.get("orders").copied(), Some([7u8; 16]));
        assert_eq!(by_name.get("trades").copied(), Some([7u8; 16]));
        assert!(by_id.get(&[7u8; 16]).is_none());
        let named = fetch_topic_name("", [7u8; 16], &by_id);
        assert!(named.is_ok());
        if let Ok(name) = named {
            assert!(name.is_none());
        }
        let agreed = fetch_topic_name(
            "",
            [1u8; 16],
            &HashMap::from([([1u8; 16], "orders".to_owned())]),
        );
        assert!(agreed.is_ok());
        if let Ok(name) = agreed {
            assert_eq!(name.as_deref(), Some("orders"));
        }
        assert!(fetch_topic_name(
            "trades",
            [1u8; 16],
            &HashMap::from([([1u8; 16], "orders".to_owned())])
        )
        .is_err());
        let plain = fetch_topic_name("orders", [0u8; 16], &HashMap::new());
        assert!(plain.is_ok());
        if let Ok(name) = plain {
            assert_eq!(name.as_deref(), Some("orders"));
        }
    }

    #[test]
    fn a_later_zero_topic_id_does_not_erase_the_cached_id() {
        let cached = HashMap::from([("t".to_string(), [1u8; 16])]);
        let (by_name, by_id) = resolved_topic_identities(
            [("t", [1u8; 16]), ("t", [0u8; 16]), ("u", [2u8; 16])],
            &cached,
        );
        assert_eq!(by_name.get("t").copied(), Some([1u8; 16]));
        assert_eq!(by_name.get("u").copied(), Some([2u8; 16]));
        assert_eq!(by_id.get(&[1u8; 16]).map(String::as_str), Some("t"));
        let (by_name, _) =
            resolved_topic_identities([("t", [4u8; 16]), ("t", [0u8; 16])], &HashMap::new());
        assert_eq!(
            by_name.get("t").copied(),
            Some([4u8; 16]),
            "a zero copy must not erase an earlier id in the same response"
        );
    }

    #[test]
    fn a_changed_topic_id_is_a_new_log() {
        let assigned = vec![(Arc::<str>::from("t"), 0, 4), (Arc::<str>::from("u"), 1, 0)];
        let mut assigned_ids: HashMap<Arc<str>, HashMap<i32, [u8; 16]>> = HashMap::new();
        let _ = assigned_ids
            .entry(Arc::from("t"))
            .or_default()
            .insert(0, [1u8; 16]);
        let _ = assigned_ids
            .entry(Arc::from("u"))
            .or_default()
            .insert(1, [4u8; 16]);
        let mut metadata_ids = HashMap::new();
        let _ = metadata_ids.insert("t".to_string(), [2u8; 16]);
        let _ = metadata_ids.insert("u".to_string(), [4u8; 16]);
        assert_eq!(
            recreated_topic_partitions(&assigned, &assigned_ids, &metadata_ids),
            vec![("t".to_string(), 0, [2u8; 16])]
        );
        let _ = metadata_ids.insert("t".to_string(), [0u8; 16]);
        assert!(recreated_topic_partitions(&assigned, &assigned_ids, &metadata_ids).is_empty());
        assigned_ids.clear();
        let _ = metadata_ids.insert("t".to_string(), [2u8; 16]);
        assert!(
            recreated_topic_partitions(&assigned, &assigned_ids, &metadata_ids).is_empty(),
            "the first stored id is not a recreation"
        );
    }

    #[test]
    fn a_negative_fetch_offset_is_rejected() {
        assert!(require_fetch_offset(-1).is_err());
        assert!(require_fetch_offset(0).is_ok());
        assert!(require_partition(-1).is_err());
        assert!(require_partition(0).is_ok());
    }

    #[test]
    fn a_position_past_the_high_watermark_is_truncated() {
        assert!(!position_past_high_watermark(2, 2));
        assert!(position_past_high_watermark(3, 2));
        assert!(!position_past_high_watermark(3, -1));
        assert!(position_before_log_start(1, 2));
        assert!(!position_before_log_start(2, 2));
        assert!(!position_before_log_start(0, -1));
        assert!(log_boundary_conflict(5, 2));
        assert!(!log_boundary_conflict(2, 2));
        assert!(!log_boundary_conflict(1, 4));
        assert!(!log_boundary_conflict(-1, 4));
        assert!(record_is_visible(1, 2));
        assert!(!record_is_visible(2, 2));
        assert!(record_is_visible(9, -1));
        assert!(!record_is_retained(1, 2));
        assert!(record_is_retained(2, 2));
        assert!(record_is_retained(0, -1));
        assert_eq!(cap_fetch_cursor(5, 2), 2);
        assert_eq!(cap_fetch_cursor(1, 2), 1);
        assert_eq!(cap_fetch_cursor(5, -1), 5);
    }

    #[test]
    fn lag_counts_buffered_records_and_does_not_go_negative() {
        assert_eq!(partition_lag(5, 2), Some(3));
        assert_eq!(partition_lag(5, 5), Some(0));
        assert_eq!(partition_lag(2, 5), Some(0));
        assert_eq!(partition_lag(-1, 0), None);
        assert_eq!(partition_lag(5, -1), None);
        assert_eq!(defined_position(0), Some(0));
        assert_eq!(defined_position(4), Some(4));
        assert_eq!(defined_position(-1), None);
    }

    #[test]
    fn an_unknown_list_offset_is_not_a_reset_position() {
        assert_eq!(reset_position(0), Some(0));
        assert_eq!(reset_position(4), Some(4));
        assert_eq!(reset_position(-1), None);
    }

    #[test]
    fn truncation_drops_records_at_the_new_cursor() {
        let mut records = vec![
            fetched_at("t", 0, 1),
            fetched_at("t", 0, 4),
            fetched_at("u", 0, 4),
        ];
        let dropped = records.get(1).map(record_bytes);
        let removed = drop_records_at_or_after(&mut records, "t", 0, 4);
        assert_eq!(Some(removed), dropped);
        let kept: Vec<_> = records
            .iter()
            .map(|record| (record.topic.as_str(), record.offset))
            .collect();
        assert_eq!(kept, vec![("t", 1), ("u", 4)]);
    }

    #[test]
    fn undefined_epoch_end_does_not_rewind_the_fetch_position() {
        assert_eq!(truncated_fetch_offset(10, 4), Some(4));
        assert_eq!(truncated_fetch_offset(4, 4), None);
        assert_eq!(truncated_fetch_offset(0, 0), None);
        assert_eq!(
            truncated_fetch_offset(
                3,
                crate::protocol::epoch::EpochEndOffset::UNDEFINED_EPOCH_OFFSET
            ),
            None
        );
    }

    #[test]
    fn offset_for_leader_epoch_resolves_the_fetched_epoch() {
        let mut cluster = Cluster::default();
        let _ = cluster.leader_epochs.insert("t".into(), vec![4, 6]);
        let mut fetched = PartitionMap::new();
        let _ = fetched.insert("t", 0, 2);
        let topics =
            offset_for_leader_epoch_topics(&cluster, &fetched, &[("t".into(), 0), ("t".into(), 1)]);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].partitions[0].partition, 0);
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 4);
        assert_eq!(topics[0].partitions[0].leader_epoch, 2);
        assert_eq!(topics[0].partitions[1].partition, 1);
        assert_eq!(topics[0].partitions[1].current_leader_epoch, 6);
        assert_eq!(topics[0].partitions[1].leader_epoch, 6);
    }

    #[test]
    fn an_unreachable_preferred_replica_returns_to_the_leader() {
        use crate::protocol::api::NodeEndpoint;
        let mut cluster = Cluster::default();
        cluster.apply_node_endpoints(&[
            NodeEndpoint::new(1, "10.0.0.1", 9092, None),
            NodeEndpoint::new(7, "10.0.0.7", 9092, None),
        ]);
        assert!(cluster.apply_current_leader("t", 0, 1, 4));
        let mut preferred = PartitionMap::new();
        let _ = preferred.insert("t", 0, 7);
        let part = FetchPartition {
            partition: 0,
            current_leader_epoch: 4,
            fetch_offset: 3,
            last_fetched_epoch: -1,
            log_start_offset: INVALID_LOG_START_OFFSET,
            partition_max_bytes: 1,
            replica_directory_id: [0; 16],
        };
        let mut by_leader: HashMap<i32, HashMap<String, Vec<FetchPartition>>> = HashMap::new();
        by_leader
            .entry(7)
            .or_default()
            .entry("t".to_owned())
            .or_default()
            .push(part.clone());
        assert!(is_preferred_only_target(
            &preferred, &cluster, 7, &by_leader
        ));
        assert!(reroute_unreachable_preferred(
            &mut preferred,
            &cluster,
            7,
            &mut by_leader
        ));
        assert!(preferred.get("t", 0).is_none());
        assert!(by_leader.get(&7).is_none());
        assert_eq!(
            by_leader
                .get(&1)
                .and_then(|topics| topics.get("t"))
                .and_then(|parts| parts.first())
                .map(|part| part.fetch_offset),
            Some(3)
        );
        by_leader
            .entry(1)
            .or_default()
            .entry("t".to_owned())
            .or_default()
            .push(part);
        let _ = preferred.insert("t", 0, 1);
        assert!(!is_preferred_only_target(
            &preferred, &cluster, 1, &by_leader
        ));
        assert!(!reroute_unreachable_preferred(
            &mut preferred,
            &cluster,
            1,
            &mut by_leader
        ));
        assert_eq!(preferred.get("t", 0).copied(), Some(1));
    }

    #[test]
    fn an_unknown_preferred_replica_uses_the_leader() {
        assert_eq!(fetch_target(Some(7), false, Some(1)), Some(1));
        assert_eq!(fetch_target(Some(7), true, Some(1)), Some(7));
        assert_eq!(fetch_target(None, false, Some(1)), Some(1));
        assert_eq!(fetch_target(Some(7), false, None), None);
    }

    #[test]
    fn a_leader_epoch_change_drops_the_preferred_replica() {
        let mut previous = HashMap::new();
        let _ = previous.insert("t".into(), vec![4, 4]);
        let _ = previous.insert("u".into(), vec![2]);
        let mut current = HashMap::new();
        let _ = current.insert("t".into(), vec![4, 9]);
        let preferred = vec![("t".into(), 0), ("t".into(), 1), ("u".into(), 0)];
        let dropped = preferred_partitions_to_drop(&preferred, &previous, &current);
        assert_eq!(dropped, vec![("t".into(), 1), ("u".into(), 0)]);
        let kept = preferred_partitions_to_drop(&[("t".into(), 0)], &previous, &current);
        assert!(kept.is_empty());
    }

    #[test]
    fn fetch_session_abandoned_request_requires_full_map_and_closes_known_session() {
        let topic = session_topic([1; 16], 0);
        let mut session = BrokerFetchSession::default();
        let first = session.prepare(vec![topic.clone()], 17);
        assert_eq!(first.metadata, FetchMetadata::INITIAL);
        session.response(&session_header(0, 91), 17).unwrap();
        let second = session.prepare(vec![topic.clone()], 17);
        assert_eq!(second.metadata, FetchMetadata::new(91, 1));
        assert!(second.topics.is_empty());
        // No response was observed; an outer future may have been canceled.
        let third = session.prepare(vec![topic], 17);
        assert_eq!(third.metadata, FetchMetadata::new(91, 0));
        assert_eq!(third.topics.len(), 1);
        assert!(third.forgotten.is_empty());
    }

    #[test]
    fn fetch_session_topic_id_replacement_forgets_old_identity() {
        let mut session = BrokerFetchSession::default();
        let _ = session.prepare(vec![session_topic([1; 16], 0)], 17);
        session.response(&session_header(0, 91), 17).unwrap();
        let request = session.prepare(vec![session_topic([2; 16], 0)], 17);
        assert_eq!(request.topics[0].topic_id, [2; 16]);
        assert_eq!(request.forgotten[0].topic_id, [1; 16]);
        assert_eq!(request.forgotten[0].partitions, [0]);
    }

    #[test]
    fn fetch_session_failure_or_malformed_header_resets_before_partition_application() {
        for code in [
            error::FETCH_SESSION_ID_NOT_FOUND,
            error::INVALID_FETCH_SESSION_EPOCH,
            error::FETCH_SESSION_TOPIC_ID_ERROR,
        ] {
            let mut session = BrokerFetchSession::default();
            let _ = session.prepare(vec![session_topic([1; 16], 0)], 17);
            session.response(&session_header(0, 91), 17).unwrap();
            let _ = session.prepare(vec![session_topic([1; 16], 0)], 17);
            assert!(
                matches!(session.response(&session_header(code, 0), 17), Err(Error::Broker { code: got, .. }) if got == code)
            );
            let request = session.prepare(vec![session_topic([1; 16], 0)], 17);
            assert!(request.metadata.is_full());
            assert_eq!(request.topics.len(), 1);
            if code == error::FETCH_SESSION_ID_NOT_FOUND {
                assert_eq!(request.metadata, FetchMetadata::INITIAL);
            } else {
                assert_eq!(request.metadata, FetchMetadata::new(91, 0));
            }
        }
        let mut session = BrokerFetchSession::default();
        let _ = session.prepare(vec![session_topic([1; 16], 0)], 17);
        assert!(session.response(&Bytes::from_static(&[0; 6]), 17).is_err());
        assert!(session
            .prepare(vec![session_topic([1; 16], 0)], 17)
            .metadata
            .is_full());
    }

    #[test]
    fn fetch_session_decline_version_change_and_epoch_wrap() {
        let mut session = BrokerFetchSession::default();
        let _ = session.prepare(vec![session_topic([1; 16], 0)], 17);
        session.response(&session_header(0, 0), 17).unwrap();
        assert_eq!(
            session
                .prepare(vec![session_topic([1; 16], 0)], 17)
                .topics
                .len(),
            1
        );
        session.response(&session_header(0, 91), 17).unwrap();
        session.metadata = FetchMetadata::new(91, i32::MAX);
        let _ = session.prepare(vec![session_topic([1; 16], 0)], 17);
        session.response(&session_header(0, 91), 17).unwrap();
        assert_eq!(session.metadata, FetchMetadata::new(91, 1));
        let changed = session.prepare(vec![session_topic([1; 16], 0)], 12);
        assert!(changed.metadata.is_full());
        assert_eq!(changed.topics.len(), 1);
        let legacy = session.prepare(vec![session_topic([1; 16], 0)], 6);
        assert_eq!(legacy.metadata, FetchMetadata::LEGACY);
        assert_eq!(legacy.topics.len(), 1);
        assert!(session.partitions.is_empty());
    }

    #[test]
    fn topic_name_inlines_short_names_and_heaps_long_ones() {
        let short = TopicName::from("nb-fetch");
        assert_eq!(short.as_str(), "nb-fetch");
        assert!(matches!(short, TopicName::Inline(_, _)));
        let cloned = short.clone();
        assert_eq!(cloned, short);
        assert_eq!(cloned, "nb-fetch");
        assert_eq!(String::from("nb-fetch"), cloned);
        let long = "topic-name-longer-than-twenty-three";
        let heaped = TopicName::from(long);
        let cloned = heaped.clone();
        assert!(matches!(
            (&heaped, &cloned),
            (TopicName::Heap(left), TopicName::Heap(right)) if Arc::ptr_eq(left, right)
        ));
        assert_eq!(heaped.as_str(), long);
        assert_eq!(String::from(heaped.as_str()), long);
    }

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
        let mut later = rec("a", 0, 8);
        later.leader_epoch = Some(9);
        let interleaved = ConsumerRecords::from(vec![rec("a", 0, 1), rec("b", 0, 4), later]);
        assert_eq!(
            interleaved.partitions(),
            vec![TopicPartition::new("a", 0), TopicPartition::new("b", 0)]
        );
        assert_eq!(
            interleaved.next_offsets(),
            vec![
                (
                    TopicPartition::new("a", 0),
                    OffsetAndMetadata::new(9).with_leader_epoch(9)
                ),
                (TopicPartition::new("b", 0), OffsetAndMetadata::new(5)),
            ]
        );
    }

    #[test]
    fn a_multi_leader_fetch_does_not_wait_past_the_deadline() {
        let window = Duration::from_millis(10);
        assert_eq!(
            multi_leader_grace(Duration::ZERO, Duration::from_secs(1)),
            window
        );
        assert_eq!(
            multi_leader_grace(Duration::from_millis(4), Duration::from_millis(20)),
            Duration::from_millis(6)
        );
        assert_eq!(
            multi_leader_grace(Duration::from_millis(4), Duration::from_millis(3)),
            Duration::from_millis(3)
        );
        assert_eq!(
            multi_leader_grace(window, Duration::from_secs(1)),
            Duration::ZERO
        );
        assert_eq!(
            multi_leader_grace(Duration::ZERO, Duration::ZERO),
            Duration::ZERO
        );
    }

    #[test]
    fn a_shared_list_offsets_deadline_keeps_only_the_time_left() {
        let start = Instant::now();
        let deadline = start + Duration::from_millis(100);
        assert_eq!(
            remaining_offset_timeout(deadline, start),
            Duration::from_millis(100)
        );
        assert_eq!(
            remaining_offset_timeout(deadline, start + Duration::from_millis(40)),
            Duration::from_millis(60)
        );
        assert_eq!(
            remaining_offset_timeout(deadline, start + Duration::from_millis(100)),
            Duration::ZERO
        );
        assert_eq!(
            remaining_offset_timeout(deadline, start + Duration::from_millis(150)),
            Duration::ZERO
        );
    }

    #[test]
    fn list_offsets_rejects_an_invalid_topic_before_a_request() {
        let err = require_list_offset_target("bad name", 0)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Topic name is invalid"), "{err}");
        assert!(require_list_offset_target("", 0).is_err());
        assert!(require_list_offset_target("orders", -1).is_err());
        assert!(require_list_offset_target("orders", 0).is_ok());
        assert!(require_unique_offset_partitions([("orders", 0), ("orders", 1)]).is_ok());
        let duplicate = require_unique_offset_partitions([("orders", 0), ("orders", 0)])
            .unwrap_err()
            .to_string();
        assert!(
            duplicate.contains("duplicate offset partition"),
            "{duplicate}"
        );
    }

    #[test]
    fn a_saturated_batch_count_does_not_jump_the_fetch_cursor() {
        let record = |offset| crate::protocol::records::Record {
            offset,
            timestamp: 0,
            key: None,
            value: None,
            headers: Vec::new(),
        };
        let mut batch = crate::RecordBatch::from_records(vec![record(0), record(0), record(0)]);
        batch.base_offset = i64::MAX - 1;
        for item in &mut batch.records {
            item.offset = i64::MAX - 2;
        }
        assert_eq!(batch_fetch_end(&batch).unwrap(), i64::MAX - 1);
        assert_eq!(
            fetch_end_after(None, Some(i64::MAX - 2)).unwrap(),
            i64::MAX - 1
        );
        assert_eq!(fetch_end_after(Some(11), Some(1)).unwrap(), 11);
        assert_eq!(fetch_end_after(Some(2), Some(5)).unwrap(), 6);
        let mut normal = crate::RecordBatch::from_records(vec![record(0), record(0)]);
        normal.records[0].offset = 0;
        normal.records[1].offset = 4;
        assert_eq!(batch_fetch_end(&normal).unwrap(), 5);
    }

    #[test]
    fn a_record_at_i64_max_has_no_next_offset() {
        assert_eq!(exclusive_fetch_offset(10), Some(11));
        assert_eq!(exclusive_fetch_offset(i64::MAX), None);
        let recs = ConsumerRecords::from(vec![rec("a", 0, i64::MAX), rec("b", 0, 4)]);
        assert_eq!(
            recs.next_offsets(),
            vec![(TopicPartition::new("b", 0), OffsetAndMetadata::new(5))]
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
