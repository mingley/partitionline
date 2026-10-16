use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, TlsConfig};
use crate::partitioner::{
    partition_for_key, to_positive, Partitioner, PartitionerBox, StickyState, StickyTag,
};
use crate::protocol::api::{
    decode_metadata_response_into, decode_produce_acks_into, encode_metadata_request,
    encode_metadata_request_names, ApiVersion, ApiVersionsResponse, MetadataResponse, ProduceAck,
    ProduceRequest,
};
use crate::protocol::api_keys::{
    pick_version, ADD_OFFSETS_TO_TXN, ADD_PARTITIONS_TO_TXN, END_TXN, FIND_COORDINATOR,
    GET_TELEMETRY_SUBSCRIPTIONS, INIT_PRODUCER_ID, METADATA, PRODUCE, TXN_OFFSET_COMMIT,
};
use crate::protocol::group::{
    decode_find_coordinator_response, encode_find_coordinator_request_typed, Topic,
    COORDINATOR_GROUP, COORDINATOR_TRANSACTION,
};
use crate::protocol::header::encode_request_header_fields;
use crate::protocol::idem::{decode_init_producer_id_response, encode_init_producer_id_request};
use crate::protocol::records::{
    record_size_upper_bound, write_java_optional, write_java_optional_bytes,
    write_java_record_headers, write_record_batch_scratch, BatchHeader, CompressScratch,
    Compression, EncodeRecord, Header as RecordHeader, RecordBatch, Records,
};
use crate::protocol::txn::{
    can_handle_abortable_error, classify_add_offsets_error, classify_add_partitions_error,
    classify_end_txn_error, classify_produce_error, classify_txn_offset_commit_error,
    client_epoch_bump_capable, decode_add_offsets_to_txn_response,
    decode_add_partitions_to_txn_response, decode_end_txn_response,
    decode_txn_offset_commit_response, encode_add_offsets_to_txn_request,
    encode_add_partitions_to_txn_borrowed, encode_end_txn_request,
    encode_txn_offset_commit_borrowed, BorrowedTxnOffsetPartition, BorrowedTxnOffsetTopic,
    BorrowedTxnTopic, EndTxnRequest, TransactionResult, TxnErrorDisposition, TxnOffsetCommitMember,
    TxnOffsetCommitRequest,
};

/// Pre-send failure injection point for testing buffer ownership and resource contracts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreSendFault {
    /// No pre-send fault injection.
    None,
    /// Inject a transaction-partition error during `add_txn_partitions`.
    TxnPartition,
    /// Inject a serialization/encoding error during produce body encoding.
    Encode,
    /// Inject a non-retriable socket write failure.
    Write,
    /// Inject a retriable socket write failure.
    WriteRetriable,
}

/// Produce settings. Prefer the chainable builders; raw fields remain writable.
#[derive(Clone)]
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
    /// Kafka `buffer.memory`. Visible key, value and header bytes of accepted
    /// records not yet acknowledged. Admission compacts shared or oversized
    /// backing buffers; exact unique allocations may be reused. Allocator and
    /// record-object overhead, encoding scratch and socket buffers are separate.
    /// Default 32 MiB (Java). Zero means no client-side cap (the
    /// per-connection channel still bounds how many records sit in memory).
    /// [`crate::Producer::send`] waits up to [`Self::max_block`];
    /// [`crate::Producer::try_send`] returns [`crate::Error::QueueFull`].
    /// A single record whose
    /// [`crate::protocol::records::Records::estimate_size_in_bytes_upper_bound`]
    /// is larger than this returns [`crate::Error::RecordTooLarge`] (Java
    /// `ensureValidRecordSize` `buffer.memory` check) without waiting.
    pub buffer_memory: usize,
    /// Kafka `max.request.size`. Java `KafkaProducer.ensureValidRecordSize`
    /// compares [`crate::protocol::records::Records::estimate_size_in_bytes_upper_bound`]
    /// to this, then to [`Self::buffer_memory`].
    /// Produce batches are also capped at
    /// `min(batch_bytes, max_request_size)` when both are non-zero. Default
    /// 1 MiB (Java). Zero means no extra cap ([`Self::batch_bytes`] still
    /// applies). Oversized records return [`crate::Error::RecordTooLarge`].
    pub max_request_size: usize,
    /// Per-request timeout (produce, metadata, init pid).
    pub request_timeout: Duration,
    /// Kafka `delivery.timeout.ms`. Time from queue until ack or timeout,
    /// including retries. Default 30s (this crate; Java defaults to 120s).
    pub delivery_timeout: Duration,
    /// Kafka `max.block.ms`. How long [`crate::Producer::send`] waits for
    /// metadata, a leader connection, and [`Self::buffer_memory`]. Default
    /// 30s (this crate; Java defaults to 60s). [`crate::Producer::try_send`]
    /// does not wait.
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
    /// Kafka `connections.max.idle.ms`. Close a broker TCP connection that
    /// has been unused for this long and reconnect on the next RPC. Default
    /// 9 minutes (Java). Zero never closes for idle.
    pub connections_max_idle: Duration,
    /// Kafka `allow.auto.create.topics` on Metadata.
    pub allow_auto_topic_creation: bool,
    /// Record batch compression.
    pub compression: Compression,
    /// Validated zstd compression level (default 3); requires the `zstd` feature.
    #[cfg(feature = "zstd")]
    pub zstd_level: crate::protocol::records::ZstdLevel,
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
    /// Test-only pre-send fault injection hook at the actual ownership transition.
    #[doc(hidden)]
    pub pre_send_fault: Option<(PreSendFault, Option<u32>)>,
}

impl fmt::Debug for ProducerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut config = f.debug_struct("ProducerConfig");
        #[cfg(feature = "zstd")]
        let _ = config.field("zstd_level", &self.zstd_level);
        config
            .field("bootstrap", &self.bootstrap)
            .field("client_id", &self.client_id)
            .field("acks", &self.acks)
            .field("linger", &self.linger)
            .field("batch_records", &self.batch_records)
            .field("batch_bytes", &self.batch_bytes)
            .field("buffer_memory", &self.buffer_memory)
            .field("max_request_size", &self.max_request_size)
            .field("request_timeout", &self.request_timeout)
            .field("delivery_timeout", &self.delivery_timeout)
            .field("max_block", &self.max_block)
            .field("retry_backoff", &self.retry_backoff)
            .field("retry_backoff_max", &self.retry_backoff_max)
            .field("metadata_max_age", &self.metadata_max_age)
            .field("connect_timeout", &self.connect_timeout)
            .field("reconnect_backoff", &self.reconnect_backoff)
            .field("reconnect_backoff_max", &self.reconnect_backoff_max)
            .field("connections_max_idle", &self.connections_max_idle)
            .field("allow_auto_topic_creation", &self.allow_auto_topic_creation)
            .field("compression", &self.compression)
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
            .field("connections", &self.connections)
            .field("max_in_flight", &self.max_in_flight)
            .field("enable_idempotence", &self.enable_idempotence)
            .field("transactional_id", &self.transactional_id)
            .field("transaction_timeout", &self.transaction_timeout)
            .field("tls", &self.tls)
            .field("partitioner", &self.partitioner)
            .field("interceptors", &self.interceptors)
            .field("pre_send_fault", &self.pre_send_fault)
            .finish()
    }
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
            buffer_memory: 32 * 1024 * 1024,
            max_request_size: 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            delivery_timeout: Duration::from_secs(30),
            max_block: Duration::from_secs(30),
            retry_backoff: crate::config::DEFAULT_RETRY_BACKOFF,
            retry_backoff_max: crate::config::DEFAULT_RETRY_BACKOFF_MAX,
            metadata_max_age: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(10),
            reconnect_backoff: crate::config::DEFAULT_RECONNECT_BACKOFF,
            reconnect_backoff_max: crate::config::DEFAULT_RECONNECT_BACKOFF_MAX,
            connections_max_idle: crate::config::DEFAULT_CONNECTIONS_MAX_IDLE,
            allow_auto_topic_creation: false,
            compression: Compression::None,
            #[cfg(feature = "zstd")]
            zstd_level: crate::protocol::records::ZstdLevel::default(),
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
            pre_send_fault: None,
        }
    }
}

impl ProducerConfig {
    /// Test hook: inject pre-send failures at the actual ownership transition.
    #[doc(hidden)]
    #[must_use]
    pub fn pre_send_fault(mut self, fault: PreSendFault) -> Self {
        self.pre_send_fault = Some((fault, Some(1)));
        self
    }

    /// Test hook: inject pre-send failures for the next `n` batches.
    #[doc(hidden)]
    #[must_use]
    pub fn pre_send_fault_times(mut self, fault: PreSendFault, n: u32) -> Self {
        self.pre_send_fault = Some((fault, Some(n)));
        self
    }

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

    /// Kafka `buffer.memory`. Key plus value bytes queued and not yet acked.
    ///
    /// Default 32 MiB (Java). Zero means no client-side cap. A record whose
    /// Java `estimateSizeInBytesUpperBound` is larger than this returns
    /// [`crate::Error::RecordTooLarge`] without waiting.
    #[must_use]
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = bytes;
        self
    }

    /// Kafka `max.request.size`. Java `ensureValidRecordSize` compares
    /// [`crate::protocol::records::Records::estimate_size_in_bytes_upper_bound`]
    /// to this, then to [`Self::buffer_memory`].
    ///
    /// Default 1 MiB (Java). Zero means no extra cap. A record larger than
    /// this returns [`crate::Error::RecordTooLarge`] from `send` / `try_send`
    /// without waiting for [`Self::max_block`].
    #[must_use]
    pub fn max_request_size(mut self, bytes: usize) -> Self {
        self.max_request_size = bytes;
        self
    }

    /// Record batch compression.
    #[must_use]
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Select a validated zstd encoder level (1 through 19, default 3).
    /// This does not change the selected compression codec.
    #[cfg(feature = "zstd")]
    #[must_use]
    pub fn zstd_level(mut self, level: crate::protocol::records::ZstdLevel) -> Self {
        self.zstd_level = level;
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

    /// Kafka `max.block.ms`. How long [`crate::Producer::send`] waits for metadata
    /// and [`Self::buffer_memory`].
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
    /// no jitter. Startup handshakes do not wait between hosts.
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
    /// Default 9 minutes (Java). Zero never closes for idle. The next Produce
    /// reconnects.
    #[must_use]
    pub fn connections_max_idle(mut self, idle: Duration) -> Self {
        self.connections_max_idle = idle;
        self
    }

    /// Kafka `allow.auto.create.topics` on Metadata.
    #[must_use]
    pub fn allow_auto_create_topics(mut self, allow: bool) -> Self {
        self.allow_auto_topic_creation = allow;
        self
    }

    fn produce_batch_bytes(&self) -> usize {
        match (self.batch_bytes, self.max_request_size) {
            (b, 0) => b,
            (0, m) => m,
            (b, m) => b.min(m),
        }
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

    /// Java `ProducerRecord.topic`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_ref()
    }

    /// Java `Headers.lastHeader`.
    #[must_use]
    pub fn last_header(&self, key: &str) -> Option<&RecordHeader> {
        RecordHeader::last_in(&self.headers, key)
    }

    /// Java `Headers.headers(String)`.
    pub fn headers_for_key<'a>(
        &'a self,
        key: &'a str,
    ) -> impl Iterator<Item = &'a RecordHeader> + 'a {
        RecordHeader::for_key(&self.headers, key)
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
    ///
    /// Java `ProducerRecord` constructor rejects a negative partition
    /// (`Invalid partition`). [`Producer::send`] / [`Producer::try_send`]
    /// enforce that.
    #[must_use]
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Record timestamp in milliseconds since the Unix epoch.
    ///
    /// `None` (the default) uses the producer clock when the batch is written.
    /// Java `ProducerRecord` constructor rejects a negative timestamp
    /// (`Invalid timestamp`). [`Producer::send`] / [`Producer::try_send`]
    /// enforce that.
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

impl fmt::Display for ProduceRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProducerRecord(topic={}, partition=", self.topic)?;
        write_java_optional(f, self.partition)?;
        f.write_str(", headers=")?;
        write_java_record_headers(f, &self.headers, false)?;
        f.write_str(", key=")?;
        write_java_optional_bytes(f, self.key.as_deref())?;
        f.write_str(", value=")?;
        write_java_optional_bytes(f, self.value.as_deref())?;
        f.write_str(", timestamp=")?;
        write_java_optional(f, self.timestamp)?;
        f.write_str(")")
    }
}

/// Broker acknowledgement for a produced record.
///
/// Java `RecordMetadata`. [`Self::new`] is Java
/// `RecordMetadata(TopicPartition, long, int, long, int, int)` (`baseOffset`
/// [`Self::INVALID_OFFSET`] keeps offset `-1` and ignores `batchIndex`).
/// [`Self::has_offset`] is Java `hasOffset` (`offset` is not
/// [`Self::INVALID_OFFSET`]).
/// [`Self::has_timestamp`] is Java `hasTimestamp` (`timestamp` is not
/// [`RecordBatch::NO_TIMESTAMP`]). [`Self::serialized_key_size`] /
/// [`Self::serialized_value_size`] are `-1` when the key or value is null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    /// Topic name, shared with the produced record.
    pub topic: Arc<str>,
    /// Partition the record was written to.
    pub partition: i32,
    /// Assigned offset, or `-1` when `acks=0`.
    pub offset: i64,
    /// Java `RecordMetadata.timestamp` ([`RecordBatch::NO_TIMESTAMP`] when
    /// unknown). Log-append time from the Produce response when the broker
    /// sends it; otherwise the produce timestamp (or the producer clock).
    pub timestamp: i64,
    /// Java `serializedKeySize` (`-1` if there is no key).
    pub serialized_key_size: i32,
    /// Java `serializedValueSize` (`-1` if there is no value).
    pub serialized_value_size: i32,
}

impl RecordMetadata {
    /// Java `RecordMetadata.UNKNOWN_PARTITION`.
    pub const UNKNOWN_PARTITION: i32 = -1;
    /// Java `ProduceResponse.INVALID_OFFSET`. Same sentinel as
    /// [`crate::protocol::api::ProducePartitionResponse::INVALID_OFFSET`].
    pub const INVALID_OFFSET: i64 = crate::protocol::api::ProducePartitionResponse::INVALID_OFFSET;

    /// Java `RecordMetadata(TopicPartition, long, int, long, int, int)`.
    ///
    /// Offset is `base_offset + batch_index` unless `base_offset` is
    /// [`Self::INVALID_OFFSET`] (`-1`), in which case the index is ignored
    /// (Java `baseOffset == -1 ? baseOffset : baseOffset + batchIndex`).
    #[must_use]
    pub fn new(
        topic_partition: impl Into<crate::TopicPartition>,
        base_offset: i64,
        batch_index: i32,
        timestamp: i64,
        serialized_key_size: i32,
        serialized_value_size: i32,
    ) -> Self {
        let tp = topic_partition.into();
        let offset = if base_offset == Self::INVALID_OFFSET {
            base_offset
        } else {
            base_offset + i64::from(batch_index)
        };
        Self {
            topic: Arc::from(tp.topic),
            partition: tp.partition,
            offset,
            timestamp,
            serialized_key_size,
            serialized_value_size,
        }
    }

    /// Java `RecordMetadata.topic`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_ref()
    }

    /// Java `RecordMetadata.partition`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Java `RecordMetadata.offset` (`-1` when `acks=0`).
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Java `RecordMetadata.hasOffset`.
    #[must_use]
    pub fn has_offset(&self) -> bool {
        self.offset != Self::INVALID_OFFSET
    }

    /// Java `RecordMetadata.timestamp` ([`RecordBatch::NO_TIMESTAMP`] when
    /// [`Self::has_timestamp`] is false).
    #[must_use]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Java `RecordMetadata.hasTimestamp`.
    #[must_use]
    pub fn has_timestamp(&self) -> bool {
        self.timestamp != RecordBatch::NO_TIMESTAMP
    }

    /// Java `RecordMetadata.serializedKeySize` (`-1` if there is no key).
    #[must_use]
    pub fn serialized_key_size(&self) -> i32 {
        self.serialized_key_size
    }

    /// Java `RecordMetadata.serializedValueSize` (`-1` if there is no value).
    #[must_use]
    pub fn serialized_value_size(&self) -> i32 {
        self.serialized_value_size
    }

    /// Topic and partition of this ack (Java `TopicPartition` inside
    /// `RecordMetadata`).
    #[must_use]
    pub fn topic_partition(&self) -> crate::TopicPartition {
        crate::TopicPartition::new(self.topic.as_ref(), self.partition)
    }
}

impl fmt::Display for RecordMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}@{}", self.topic, self.partition, self.offset)
    }
}

/// A producer-wide record-slot bound, independent of payload byte accounting.
/// Fresh per Producer, including when cloned configs share a partitioner Arc.
struct StickyRecordBudget {
    limit: usize,
    used: AtomicUsize,
    nudge: Notify,
}

impl StickyRecordBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit: limit.clamp(1, 100_000),
            used: AtomicUsize::new(0),
            nudge: Notify::new(),
        }
    }
    fn acquire(self: &Arc<Self>) -> Option<StickyRecordPermit> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            if used >= self.limit {
                return None;
            }
            match self.used.compare_exchange_weak(
                used,
                used + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(StickyRecordPermit {
                        budget: Arc::clone(self),
                    })
                }
                Err(current) => used = current,
            }
        }
    }
}

/// Non-Clone: one reservation moves with Pending through every owner and retry.
struct StickyRecordPermit {
    budget: Arc<StickyRecordBudget>,
}
impl Drop for StickyRecordPermit {
    fn drop(&mut self) {
        let _ = self.budget.used.fetch_sub(1, Ordering::AcqRel);
        self.budget.nudge.notify_waiters();
    }
}

struct Pending {
    sticky_record: Option<StickyRecordPermit>,
    sticky: Option<StickyTag>,
    rec: ProduceRecord,
    tx: Option<oneshot::Sender<Result<RecordMetadata>>>,
    seq: Option<i32>,
    batch_base_seq: Option<i32>,
    batch_len: usize,
    /// [`Shared::epoch_gen`] observed when the sequence was assigned. A
    /// mismatch means a producer-epoch bump (or identity renewal) happened
    /// while this batch was retained, so the stale sequence must be
    /// re-assigned before the next Produce (KL03-09).
    epoch_gen: u64,
    deadline: Instant,
    queued_at: Instant,
    /// Failed Produce attempts so far. The first retry sleeps
    /// [`ProducerConfig::retry_backoff`].
    retry: u32,
    /// Produce v10+ CurrentLeader already patched cluster metadata.
    skip_meta_refresh: bool,
    retry_after: Instant,
}

enum Ctrl {
    Flush(oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<Result<()>>),
    /// Redial an idle leader socket. Ignored when a produce is already in flight.
    Reconnect,
}

#[derive(Clone)]
struct WorkerHandle {
    id: Arc<()>,
    data: mpsc::Sender<Pending>,
    ctrl: mpsc::Sender<Ctrl>,
    task: Arc<tokio::task::JoinHandle<()>>,
}

/// Topics kept with a ready partition-to-connection map.
const FAST_ROUTE_TOPICS: usize = 8;

struct FastRoute {
    np: i32,
    handles: Vec<Arc<WorkerHandle>>,
    tracker: Arc<crate::metrics::ProduceTopicTracker>,
    /// Metadata time captured when this route was built.
    fetched_at: Option<Instant>,
}

#[derive(Default)]
struct FastRoutes {
    order: VecDeque<Arc<str>>,
    routes: HashMap<Arc<str>, FastRoute>,
}

impl FastRoutes {
    fn clear(&mut self) {
        self.order.clear();
        self.routes.clear();
    }
}

/// One broker's client quota, shared by all its connection slots. The atomic
/// fast path avoids a clock read and mutex when the broker has never throttled
/// or its interval has expired. Keep an outstanding interval across reconnects.
#[derive(Default)]
struct BrokerThrottle {
    active: AtomicBool,
    until: parking_lot::Mutex<Option<Instant>>,
}

impl BrokerThrottle {
    fn remaining(&self) -> Option<Duration> {
        if !self.active.load(Ordering::Acquire) {
            return None;
        }
        let mut until = self.until.lock();
        let rest = until.map(|end| end.saturating_duration_since(Instant::now()));
        if rest.is_some_and(|rest| !rest.is_zero()) {
            rest
        } else {
            *until = None;
            self.active.store(false, Ordering::Release);
            None
        }
    }

    fn observe(&self, millis: i32) {
        // Zero and negative values never cancel another connection's quota.
        let Ok(millis) = u64::try_from(millis) else {
            return;
        };
        if millis == 0 {
            return;
        }
        let Some(end) = Instant::now().checked_add(Duration::from_millis(millis)) else {
            return;
        };
        let mut until = self.until.lock();
        *until = Some(until.map_or(end, |old| old.max(end)));
        self.active.store(true, Ordering::Release);
    }
}

// Keep the exact coordinator negotiation beside its connection, so a moved
// coordinator cannot inherit an InitProducerId version from the old socket.
struct CoordinatorConnection {
    conn: BrokerConn,
    init_producer_id_version: Option<i16>,
}

/// Idempotent sequence numbers, keyed by topic so a steady partition does not
/// copy the topic name on every batch.
type SequenceMap = HashMap<Arc<str>, HashMap<i32, i32>>;

struct Shared {
    cfg: ProducerConfig,
    cluster: parking_lot::Mutex<Cluster>,
    meta: Mutex<BrokerConn>,
    /// Transaction coordinator. `None` when `transactional.id` is unset.
    txn: Mutex<Option<CoordinatorConnection>>,
    metadata_version: i16,
    /// Last metadata image. The next refresh refills it.
    metadata_image: parking_lot::Mutex<MetadataResponse>,
    /// Fixed at initialization: finalized transaction.version >= 2 and EndTxn v5.
    transaction_v2: bool,
    add_partitions_version: i16,
    add_offsets_version: i16,
    end_txn_version: i16,
    txn_offset_version: i16,
    find_coord_version: Option<i16>,
    init_producer_id_version: i16,
    telemetry_version: Option<i16>,
    client_instance_id: parking_lot::Mutex<Option<[u8; 16]>>,
    partitioner: Arc<dyn Partitioner>,
    /// Present only for the explicit opt-in; mutable history is per producer.
    sticky: Option<parking_lot::Mutex<StickyState>>,
    sticky_records: Option<Arc<StickyRecordBudget>>,
    producer_id: AtomicI64,
    producer_epoch: AtomicI16,
    /// Generation of the shared idempotent identity. Incremented on every
    /// local epoch bump and on every producer-identity renewal, so retained
    /// batches (pending, in-flight, retry queue, any worker or partition)
    /// can detect that their assigned sequence belongs to a superseded
    /// epoch and must be re-assigned (KL03-09).
    epoch_gen: AtomicU64,
    /// After an abortable transactional produce failure, abort re-inits with
    /// the last producer id and epoch (KIP-360). Set only when
    /// [`client_epoch_bump_capable`] holds; transaction V2 (`EndTxn` v5+)
    /// carries the bumped identity in the `EndTxn` response instead. Never a
    /// local invention: the replacement identity always comes from the
    /// broker (`InitProducerId` response). `INVALID_PRODUCER_ID_MAPPING` on
    /// Produce is fatal (pinned Java `maybeTransitionToErrorState`) and never
    /// sets this flag.
    epoch_bump_required: AtomicBool,
    /// Java `TransactionManager.State.FATAL_ERROR`: fencing, authorization, or
    /// identity errors (KL03-10). Terminal: close and recreate the producer.
    txn_fatal: AtomicBool,
    /// Java `TransactionManager.State.ABORTABLE_ERROR`: the open transaction
    /// must be aborted before any other transactional operation (KL03-10).
    /// Cleared by a successful abort.
    txn_abort_required: AtomicBool,
    /// First fatal/abortable transactional error (Java `lastError`).
    /// `begin`/`commit`/`send_offsets` fail with it while set; `abort` fails
    /// with it only when [`Self::txn_fatal`] is set.
    txn_error: parking_lot::Mutex<Option<Error>>,
    seqs: parking_lot::Mutex<SequenceMap>,
    cache_nudge: Notify,
    buffer_nudge: Notify,
    meta_tx: mpsc::Sender<Arc<str>>,
    connect_tx: mpsc::Sender<(i32, usize)>,
    retry_tx: mpsc::Sender<Pending>,
    meta_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    connect_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    retry_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    last_meta_err: parking_lot::Mutex<Option<Error>>,
    /// Set when [`Self::last_meta_err`] holds an error. The produce fast path
    /// loads this before taking the mutex.
    meta_err_set: AtomicBool,
    nodes: parking_lot::Mutex<HashMap<i32, Vec<Option<WorkerHandle>>>>,
    broker_throttles: parking_lot::Mutex<HashMap<i32, Arc<BrokerThrottle>>>,
    throttle_metrics: crate::metrics::ThrottleTracker,
    reconnect_fails: parking_lot::Mutex<HashMap<i32, u32>>,
    /// (Broker, slot) pairs with a connect or reconnect-backoff in flight.
    reconnect_busy: parking_lot::Mutex<HashSet<(i32, usize)>>,
    retries_out: AtomicUsize,
    in_txn: AtomicBool,
    txn_partitions: parking_lot::Mutex<TxnPartSet>,
    txn_added: parking_lot::Mutex<TxnPartSet>,
    fast: parking_lot::RwLock<FastRoutes>,
    m_queued: AtomicU64,
    m_acked: AtomicU64,
    m_errors: AtomicU64,
    m_bytes: AtomicU64,
    buffered_bytes: AtomicU64,
    /// Set by [`Producer::close`] / [`Producer::close_timeout`] so clones cannot
    /// respawn workers after shutdown (KL-02 durable Closed outcome).
    closed: AtomicBool,
    ack_latency: crate::metrics::LatencyTracker,
    interceptors: crate::interceptor::ProducerInterceptors,
    topics: parking_lot::Mutex<HashMap<Arc<str>, Arc<crate::metrics::ProduceTopicTracker>>>,
    pub(crate) pre_send_fault: parking_lot::Mutex<Option<(PreSendFault, Option<u32>)>>,
}

/// Produce client: queue records, batch, and wait for offsets.
#[derive(Clone)]
pub struct Producer {
    inner: Arc<Inner>,
}

struct Inner {
    shared: Arc<Shared>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.buffer_nudge.notify_waiters();
        self.shared.cache_nudge.notify_waiters();

        let workers: Vec<WorkerHandle> = self
            .shared
            .nodes
            .lock()
            .values()
            .flatten()
            .flatten()
            .cloned()
            .collect();
        for w in &workers {
            w.task.abort();
        }
        self.shared.nodes.lock().clear();
        self.shared.fast.write().clear();

        if let Some(h) = self.shared.meta_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.shared.connect_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.shared.retry_task.lock().take() {
            h.abort();
        }

        self.shared.clear_sticky();
        self.shared.interceptors.close();
    }
}

/// Per-record queue-to-ack latency against one batch ack timestamp
/// (no clock read; definition preserved: ack time minus queued_at).
fn ack_latency_ns(ack: Instant, queued_at: Instant) -> u64 {
    u64::try_from(ack.saturating_duration_since(queued_at).as_nanos()).unwrap_or(u64::MAX)
}

/// Owns a reserved payload budget until the real enqueue transfers ownership.
/// Dropping an async admission future before enqueue releases its reservation.
struct BufferReservation<'a> {
    shared: &'a Shared,
    bytes: u64,
    active: bool,
}
impl Drop for BufferReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.shared.release_buffer(self.bytes);
        }
    }
}

impl Shared {
    fn clear_sticky(&self) {
        if let Some(state) = &self.sticky {
            state.lock().clear();
        }
        if let Some(records) = &self.sticky_records {
            records.nudge.notify_waiters();
        }
    }

    fn retire_sticky(&self, pending: &[Pending], failed: bool) {
        let Some(state) = &self.sticky else {
            return;
        };
        // Lock order is always metadata -> policy; neither lock crosses an await.
        let cluster = self.cluster.lock();
        let mut state = state.lock();
        for p in pending {
            if let Some(tag) = p.sticky {
                if failed && p.seq.is_some() {
                    state.cancel(tag);
                }
                state.release(tag, 1, &cluster);
            }
        }
        drop(state);
        drop(cluster);
        self.cache_nudge.notify_waiters();
    }
    fn broker_throttle(&self, node: i32) -> Arc<BrokerThrottle> {
        let mut map = self.broker_throttles.lock();
        // Retain only live workers and unexpired intervals, including those
        // whose last socket disconnected. No history per record or response.
        map.retain(|_, throttle| Arc::strong_count(throttle) > 1 || throttle.remaining().is_some());
        Arc::clone(map.entry(node).or_default())
    }

    fn topic_tracker(&self, topic: &Arc<str>) -> Arc<crate::metrics::ProduceTopicTracker> {
        let mut map = self.topics.lock();
        if let Some(tracker) = map.get(topic.as_ref()) {
            return Arc::clone(tracker);
        }
        let tracker = Arc::new(crate::metrics::ProduceTopicTracker::new());
        let _ = map.insert(Arc::clone(topic), Arc::clone(&tracker));
        tracker
    }

    fn note_queued_totals(&self, n: u64, bytes: u64) {
        let _ = self.m_queued.fetch_add(n, Ordering::Relaxed);
        let _ = self.m_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Apply EndTxn v5 ProducerId / ProducerEpoch. Ignores
    /// [`RecordBatch::NO_PRODUCER_ID`] (JSON default / NOT_COORDINATOR).
    /// Clears per-partition sequences when the identity changes (Java
    /// resets sequences on epoch bump).
    fn apply_end_txn_identity(&self, version: i16, producer_id: i64, producer_epoch: i16) {
        if version <= EndTxnRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
            || producer_id <= RecordBatch::NO_PRODUCER_ID
        {
            return;
        }
        let old_pid = self.producer_id.load(Ordering::SeqCst);
        let old_epoch = self.producer_epoch.load(Ordering::SeqCst);
        if producer_id != old_pid || producer_epoch != old_epoch {
            self.seqs.lock().clear();
            let _ = self.epoch_gen.fetch_add(1, Ordering::SeqCst);
        }
        self.producer_id.store(producer_id, Ordering::SeqCst);
        self.producer_epoch.store(producer_epoch, Ordering::SeqCst);
    }

    /// Local epoch bump for the idempotent producer (KIP-360). Clears
    /// per-partition sequences and advances [`Self::epoch_gen`] so every
    /// retained batch re-takes its sequence under the new epoch instead of
    /// reusing a stale one. At `i16::MAX` no bump is possible; the caller
    /// must renew the producer identity instead (KL03-09).
    ///
    /// Nontransactional only: the transactional fencing path never bumps
    /// locally. Transactional recovery identities always come from the broker
    /// (`InitProducerId` re-init or `EndTxn` v5+ response); retries reuse the
    /// broker-authorized identity and never invent one.
    fn bump_idempotent_epoch(&self) {
        let epoch = self.producer_epoch.load(Ordering::SeqCst);
        if epoch <= RecordBatch::NO_PRODUCER_EPOCH || epoch == i16::MAX {
            return;
        }
        self.producer_epoch
            .store(epoch.saturating_add(1), Ordering::SeqCst);
        self.seqs.lock().clear();
        let _ = self.epoch_gen.fetch_add(1, Ordering::SeqCst);
    }

    /// Java `TransactionManager.maybeFailWithError` for `begin`/`commit`/
    /// `send_offsets`: fail with the remembered error while fatal or
    /// abort-required (KL03-10).
    fn fail_on_txn_error(&self) -> Result<()> {
        if self.txn_fatal.load(Ordering::SeqCst) || self.txn_abort_required.load(Ordering::SeqCst) {
            if let Some(e) = self.txn_error.lock().clone() {
                return Err(e);
            }
            return Err(Error::protocol("transaction in error state"));
        }
        Ok(())
    }

    /// Java `beginAbort` guard: abort is the recovery path out of
    /// abort-required, but a fenced producer cannot even abort (KL03-10).
    fn fail_on_txn_fatal(&self) -> Result<()> {
        if self.txn_fatal.load(Ordering::SeqCst) {
            if let Some(e) = self.txn_error.lock().clone() {
                return Err(e);
            }
            return Err(Error::protocol("transactional producer fenced"));
        }
        Ok(())
    }

    /// Record a fatal transactional error (Java `transitionToFatalError`).
    /// The first error is retained as the root cause; the flag is terminal.
    fn set_txn_fatal(&self, err: Error) {
        if !self.txn_fatal.swap(true, Ordering::SeqCst) && self.txn_error.lock().is_none() {
            *self.txn_error.lock() = Some(err);
        }
    }

    /// Record an abortable transactional error (Java
    /// `transitionToAbortableError`). Ignored once fatal: fatal wins.
    fn set_txn_abortable(&self, err: Error) {
        if self.txn_fatal.load(Ordering::SeqCst) {
            return;
        }
        if !self.txn_abort_required.swap(true, Ordering::SeqCst) && self.txn_error.lock().is_none()
        {
            *self.txn_error.lock() = Some(err);
        }
    }

    /// Java `resetTransactionState` (success path): a completed abort clears
    /// the abort-required state so the next transaction can begin. Never
    /// clears fatal.
    fn clear_txn_abortable(&self) {
        self.txn_abort_required.store(false, Ordering::SeqCst);
        if !self.txn_fatal.load(Ordering::SeqCst) {
            *self.txn_error.lock() = None;
        }
    }

    /// Java `TransactionManager.canHandleAbortableError` for the negotiated
    /// coordinator versions.
    fn txn_can_handle_abortable(&self) -> bool {
        can_handle_abortable_error(self.init_producer_id_version, self.end_txn_version)
    }

    /// Record a terminal [`TxnErrorDisposition`]. A retriable code only
    /// reaches here with an exhausted retry budget, which degrades to
    /// abortable (Java turns exhausted retries into `abortable` on the
    /// Produce path; aborting and retrying the whole transaction is safe
    /// for every transactional RPC).
    fn apply_txn_disposition(&self, disposition: TxnErrorDisposition, err: Error) {
        match disposition {
            TxnErrorDisposition::Fatal => self.set_txn_fatal(err),
            TxnErrorDisposition::Abortable | TxnErrorDisposition::Retriable => {
                self.set_txn_abortable(err);
            }
        }
    }

    fn note_queued_n(&self, topic: &Arc<str>, n: u64, bytes: u64) {
        self.note_queued_totals(n, bytes);
        self.topic_tracker(topic).note_queued(n, bytes);
    }

    /// True when two topic handles name the same metrics row.
    fn same_tracked_topic(cached: &Arc<str>, topic: &Arc<str>) -> bool {
        Arc::ptr_eq(cached, topic) || cached.as_ref() == topic.as_ref()
    }

    fn topic_tracker_cached(
        &self,
        cache: &mut Option<(Arc<str>, Arc<crate::metrics::ProduceTopicTracker>)>,
        topic: &Arc<str>,
    ) -> Arc<crate::metrics::ProduceTopicTracker> {
        if let Some((cached, tracker)) = cache.as_ref() {
            if Self::same_tracked_topic(cached, topic) {
                return Arc::clone(tracker);
            }
        }
        let tracker = self.topic_tracker(topic);
        *cache = Some((Arc::clone(topic), Arc::clone(&tracker)));
        tracker
    }

    fn note_acked_on(&self, tracker: &crate::metrics::ProduceTopicTracker, n: u64) {
        let _ = self.m_acked.fetch_add(n, Ordering::Relaxed);
        tracker.note_acked(n);
    }

    /// Batch ack-latency accounting for a tracker already resolved.
    /// No-op for empty batches. One sample is recorded per call.
    fn note_ack_latency_on(
        &self,
        tracker: &crate::metrics::ProduceTopicTracker,
        b: crate::metrics::BatchLatency,
    ) {
        if b.n == 0 {
            return;
        }
        self.ack_latency.record_batch(b);
        tracker.note_ack_latency_batch(b);
    }

    fn note_errors(&self, topic: &Arc<str>, n: u64) {
        let _ = self.m_errors.fetch_add(n, Ordering::Relaxed);
        self.topic_tracker(topic).note_errors(n);
    }

    fn try_reserve_buffer(&self, bytes: u64) -> bool {
        let cap = self.cfg.buffer_memory;
        if cap == 0 {
            // Unlimited capacity still owns a counted reservation: failed
            // admission, cancellation and delivery each release it once.
            let _ = self.buffered_bytes.fetch_add(bytes, Ordering::Relaxed);
            return true;
        }
        let cap = u64::try_from(cap).unwrap_or(u64::MAX);
        if bytes == 0 {
            return self.buffered_bytes.load(Ordering::Relaxed) < cap;
        }
        let prev = self.buffered_bytes.fetch_add(bytes, Ordering::Relaxed);
        if prev.saturating_add(bytes) > cap {
            let _ = self.buffered_bytes.fetch_sub(bytes, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    fn release_buffer(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self.buffered_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.buffer_nudge.notify_waiters();
    }
}

fn rec_bytes(rec: &ProduceRecord) -> u64 {
    let k = rec
        .key
        .as_ref()
        .map(|b| u64::try_from(b.len()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let v = rec
        .value
        .as_ref()
        .map(|b| u64::try_from(b.len()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let h: u64 = rec
        .headers
        .iter()
        .map(|h| {
            let k_len = u64::try_from(h.key.len()).unwrap_or(u64::MAX);
            let v_len = h
                .value
                .as_ref()
                .map(|b| u64::try_from(b.len()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            k_len.saturating_add(v_len)
        })
        .fold(0, u64::saturating_add);
    k.saturating_add(v).saturating_add(h)
}

// Admission owns buffers sized to the visible payload. Converting through Vec
// exposes the whole allocation, including a prefix hidden by Bytes::slice.
// Shared or custom owners copy; an exact, unique allocation can be reused.
fn compact_payload(bytes: Bytes) -> Bytes {
    let visible: Vec<u8> = bytes.into();
    Bytes::from(visible.into_boxed_slice())
}

fn compact_record_backing(rec: &mut ProduceRecord) {
    rec.key = rec.key.take().map(compact_payload);
    rec.value = rec.value.take().map(compact_payload);
    let headers = std::mem::take(&mut rec.headers);
    rec.headers = headers.into_boxed_slice().into_vec();
    for header in &mut rec.headers {
        if header.key.capacity() != header.key.len() {
            header.key = std::mem::take(&mut header.key)
                .into_boxed_str()
                .into_string();
        }
        header.value = header.value.take().map(compact_payload);
    }
}

fn reject_java_producer_record(rec: &ProduceRecord) -> Result<()> {
    if let Some(timestamp) = rec.timestamp {
        if timestamp < 0 {
            return Err(Error::protocol(format!(
                "Invalid timestamp: {timestamp}. Timestamp should always be non-negative or null."
            )));
        }
    }
    if let Some(partition) = rec.partition {
        if partition < 0 {
            return Err(Error::protocol(format!(
                "Invalid partition: {partition}. Partition number should always be non-negative or null."
            )));
        }
    }
    Topic::validate(&rec.topic)?;
    Ok(())
}

/// Java `KafkaProducer.throwIfInvalidGroupMetadata`.
fn reject_java_group_metadata(group: &crate::ConsumerGroupMetadata) -> Result<()> {
    if group.generation_id > 0 && group.member_id == crate::ConsumerGroupMetadata::UNKNOWN_MEMBER_ID
    {
        return Err(Error::protocol(format!(
            "Passed in group metadata {group} has generationId > 0 but the member.id is unknown"
        )));
    }
    Ok(())
}

/// Java `KafkaProducer.throwIfNoTransactionManager`.
fn reject_java_no_transaction_manager() -> Error {
    Error::protocol(
        "Cannot use transactional methods without enabling transactions by setting the transactional.id configuration property",
    )
}

fn reject_oversized(cfg: &ProducerConfig, rec: &ProduceRecord) -> Result<u64> {
    reject_java_producer_record(rec)?;
    let bytes = rec_bytes(rec);
    let max_request = if cfg.max_request_size == 0 {
        None
    } else {
        Some(u64::try_from(cfg.max_request_size).unwrap_or(u64::MAX))
    };
    let buffer_memory = if cfg.buffer_memory == 0 {
        None
    } else {
        Some(u64::try_from(cfg.buffer_memory).unwrap_or(u64::MAX))
    };
    if max_request.is_none() && buffer_memory.is_none() {
        return Ok(bytes);
    }
    let serialized = Records::estimate_size_in_bytes_upper_bound(
        rec.key.as_deref(),
        rec.value.as_deref(),
        &rec.headers,
    )?;
    let size = u64::try_from(serialized).unwrap_or(u64::MAX);
    // Java `KafkaProducer.ensureValidRecordSize`: max.request.size first.
    if let Some(cap) = max_request {
        if size > cap {
            return Err(Error::record_too_large_max_request_size(size, cap));
        }
    }
    if let Some(cap) = buffer_memory {
        if size > cap {
            return Err(Error::record_too_large_buffer_memory(size, cap));
        }
    }
    Ok(bytes)
}

fn pendings_bytes(pendings: &[Pending]) -> u64 {
    pendings
        .iter()
        .map(|p| rec_bytes(&p.rec))
        .fold(0, u64::saturating_add)
}

impl Producer {
    /// Connect with default config to one bootstrap server.
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(ProducerConfig::bootstrap([bootstrap.into()])).await
    }

    /// Connect using `cfg`. Negotiates ApiVersions, optional SASL/TLS, and
    /// `InitProducerId` when idempotent or transactional.
    /// FindCoordinator is required only when a transactional id is configured.
    ///
    /// Several bootstraps race the handshake. The first one to finish is
    /// kept, so a peer that accepts TCP and then stalls cannot hold a
    /// reachable broker for the whole connect timeout.
    pub async fn new(cfg: ProducerConfig) -> Result<Self> {
        let deadline = Instant::now()
            .checked_add(cfg.request_timeout)
            .ok_or(Error::Timeout)?;
        crate::net::Deadline::from_std(deadline)
            .run(Self::new_until(cfg, deadline))
            .await
    }

    /// TCP, ApiVersions, and SASL on one bootstrap, bounded by `deadline`.
    async fn open_one_bootstrap(
        cfg: &ProducerConfig,
        addr: &str,
        deadline: Instant,
    ) -> Result<(BrokerConn, ApiVersionsResponse)> {
        let mut conn = BrokerConn::connect_tls(
            addr,
            &cfg.client_id,
            cfg.connect_timeout.min(timeout_remaining(deadline)?),
            cfg.tls.as_ref(),
        )
        .await?;
        let resp =
            crate::protocol::api::negotiate_api_versions(&mut conn, timeout_remaining(deadline)?)
                .await?;
        crate::protocol::sasl::apply_api_keys(&mut conn, &resp.api_keys);
        crate::protocol::sasl::authenticate(
            &mut conn,
            cfg.sasl_plain.as_ref(),
            cfg.sasl_scram.as_ref(),
            cfg.sasl_scram_sha512.as_ref(),
            cfg.sasl_oauthbearer.as_deref(),
            cfg.sasl_oauthbearer_oidc.as_ref(),
            timeout_remaining(deadline)?,
        )
        .await?;
        Ok((conn, resp))
    }

    /// Handshake every bootstrap together and keep the first full success.
    async fn open_bootstrap(
        cfg: &ProducerConfig,
        deadline: Instant,
    ) -> Result<(BrokerConn, ApiVersionsResponse)> {
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
                .spawn(async move { Producer::open_one_bootstrap(&cfg, &addr, deadline).await });
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

    async fn new_until(cfg: ProducerConfig, deadline: Instant) -> Result<Self> {
        if cfg.bootstrap.len() > 16 || cfg.client_id.len() > i16::MAX as usize {
            return Err(Error::protocol(
                "bootstrap/client ID exceeds discovery limit",
            ));
        }
        let mut cfg = cfg;
        if !matches!(cfg.acks, -1..=1) {
            return Err(Error::protocol("acks must be -1, 0, or 1"));
        }
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        let (mut meta, resp) = Self::open_bootstrap(&cfg, deadline).await?;
        let mut versions = HashMap::new();
        for api in &resp.api_keys {
            let _prev = versions.insert(api.api_key, api.clone());
        }
        let mut cfg = cfg;
        if cfg.transactional_id.is_some() {
            cfg.enable_idempotence = true;
        }
        if cfg.enable_idempotence {
            cfg.acks = -1;
            cfg.max_in_flight = cfg.max_in_flight.min(5);
        }
        let find_coord_version = pick(&versions, FIND_COORDINATOR, 0, 6);
        if cfg.transactional_id.is_some() {
            let _version = required_find_coordinator(find_coord_version)?;
        }
        if let Some(pv) = pick(&versions, PRODUCE, 3, 13) {
            meta.set_produce_version(pv);
        }
        let metadata_version = pick(&versions, METADATA, 0, 13)
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        let transaction_v2 =
            resp.finalized_features.iter().any(|feature| {
                feature.name == "transaction.version" && feature.max_version_level >= 2
            }) && pick(&versions, END_TXN, 0, 5) == Some(5);
        let transaction_max = if transaction_v2 { 5 } else { 4 };
        let (add_partitions_version, add_offsets_version, end_txn_version, txn_offset_version) =
            if cfg.transactional_id.is_some() {
                let add_p = pick(&versions, ADD_PARTITIONS_TO_TXN, 0, 3).ok_or_else(|| {
                    Error::Unsupported("broker does not support AddPartitionsToTxn".into())
                })?;
                let add_o = pick(&versions, ADD_OFFSETS_TO_TXN, 0, 4).ok_or_else(|| {
                    Error::Unsupported("broker does not support AddOffsetsToTxn".into())
                })?;
                let end = pick(&versions, END_TXN, 0, transaction_max)
                    .ok_or_else(|| Error::Unsupported("broker does not support EndTxn".into()))?;
                let toc =
                    pick(&versions, TXN_OFFSET_COMMIT, 0, transaction_max).ok_or_else(|| {
                        Error::Unsupported("broker does not support TxnOffsetCommit".into())
                    })?;
                (add_p, add_o, end, toc)
            } else {
                (0, 0, 0, 0)
            };

        let mut producer_id = RecordBatch::NO_PRODUCER_ID;
        let mut producer_epoch = RecordBatch::NO_PRODUCER_EPOCH;
        let mut init_producer_id_version = 0i16;
        // Coordinator discovery and producer-ID allocation continue the
        // startup deadline. Rediscovery and backoff cannot renew it.
        let mut txn = if let Some(tid) = cfg.transactional_id.as_deref() {
            Some(
                discover_within(
                    &cfg,
                    tid,
                    COORDINATOR_TRANSACTION,
                    required_find_coordinator(find_coord_version)?,
                    deadline,
                )
                .await?,
            )
        } else {
            None
        };
        if cfg.enable_idempotence {
            let ipid_version = pick(&versions, INIT_PRODUCER_ID, 0, 6).ok_or_else(|| {
                Error::Unsupported("broker does not support InitProducerId".into())
            })?;
            let (body, ipid_version) = init_producer_id_roundtrip(
                &cfg,
                &mut txn,
                Some(&mut meta),
                ipid_version,
                find_coord_version,
                (RecordBatch::NO_PRODUCER_ID, RecordBatch::NO_PRODUCER_EPOCH),
                deadline,
            )
            .await?;
            init_producer_id_version = ipid_version;
            let (err, pid, epoch, ..) =
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
        let sticky_policy = cfg.partitioner.arc().unkeyed_batching_policy();
        let shared = Arc::new(Shared {
            cfg: cfg.clone(),
            cluster: parking_lot::Mutex::new(Cluster::default()),
            meta: Mutex::new(meta),
            txn: Mutex::new(txn),
            metadata_version,
            metadata_image: parking_lot::Mutex::new(MetadataResponse::blank()),
            transaction_v2,
            add_partitions_version,
            add_offsets_version,
            end_txn_version,
            txn_offset_version,
            find_coord_version,
            init_producer_id_version,
            telemetry_version: pick(&versions, GET_TELEMETRY_SUBSCRIPTIONS, 0, 0),
            client_instance_id: parking_lot::Mutex::new(None),
            partitioner: cfg.partitioner.arc(),
            sticky: sticky_policy.map(|policy| {
                parking_lot::Mutex::new(StickyState::new(
                    policy,
                    cfg.produce_batch_bytes(),
                    cfg.batch_records,
                ))
            }),
            sticky_records: sticky_policy
                .map(|policy| Arc::new(StickyRecordBudget::new(policy.max_pending_records))),
            producer_id: AtomicI64::new(producer_id),
            producer_epoch: AtomicI16::new(producer_epoch),
            epoch_gen: AtomicU64::new(0),
            epoch_bump_required: AtomicBool::new(false),
            txn_fatal: AtomicBool::new(false),
            txn_abort_required: AtomicBool::new(false),
            txn_error: parking_lot::Mutex::new(None),
            seqs: parking_lot::Mutex::new(HashMap::new()),
            cache_nudge: Notify::new(),
            buffer_nudge: Notify::new(),
            meta_tx,
            connect_tx,
            retry_tx,
            meta_task: parking_lot::Mutex::new(None),
            connect_task: parking_lot::Mutex::new(None),
            retry_task: parking_lot::Mutex::new(None),
            last_meta_err: parking_lot::Mutex::new(None),
            meta_err_set: AtomicBool::new(false),
            nodes: parking_lot::Mutex::new(HashMap::new()),
            broker_throttles: parking_lot::Mutex::new(HashMap::new()),
            throttle_metrics: crate::metrics::ThrottleTracker::default(),
            reconnect_fails: parking_lot::Mutex::new(HashMap::new()),
            reconnect_busy: parking_lot::Mutex::new(HashSet::new()),
            retries_out: AtomicUsize::new(0),
            in_txn: AtomicBool::new(false),
            txn_partitions: parking_lot::Mutex::new(HashMap::new()),
            txn_added: parking_lot::Mutex::new(HashMap::new()),
            fast: parking_lot::RwLock::new(FastRoutes::default()),
            m_queued: AtomicU64::new(0),
            m_acked: AtomicU64::new(0),
            m_errors: AtomicU64::new(0),
            m_bytes: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            ack_latency: crate::metrics::LatencyTracker::new(),
            interceptors: cfg.interceptors.clone(),
            topics: parking_lot::Mutex::new(HashMap::new()),
            pre_send_fault: parking_lot::Mutex::new(cfg.pre_send_fault),
        });
        let weak = Arc::downgrade(&shared);
        let meta_handle = tokio::spawn(async move {
            let mut meta_rx = meta_rx;
            let mut queued = Vec::new();
            while let Some(topic) = meta_rx.recv().await {
                let Some(shared) = weak.upgrade() else {
                    break;
                };
                if shared.closed.load(Ordering::SeqCst) {
                    break;
                }
                queued.clear();
                queued.push(topic);
                while let Ok(topic) = meta_rx.try_recv() {
                    queued.push(topic);
                }
                match refresh_queued_topics(&shared, &queued).await {
                    None => {}
                    Some(Ok(())) => publish_meta_err(&shared, None),
                    Some(Err(error)) => publish_meta_err(&shared, Some(clone_err(&error))),
                }
                shared.cache_nudge.notify_waiters();
            }
        });
        let weak = Arc::downgrade(&shared);
        let connect_handle = tokio::spawn(async move {
            connect_loop(weak, connect_rx, cap).await;
        });
        let weak = Arc::downgrade(&shared);
        let retry_handle = tokio::spawn(async move {
            retry_loop(weak, retry_rx).await;
        });

        *shared.meta_task.lock() = Some(meta_handle);
        *shared.connect_task.lock() = Some(connect_handle);
        *shared.retry_task.lock() = Some(retry_handle);

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
        let node = cluster.leader_id(rec.topic.as_ref(), p).ok()?;
        drop(cluster);
        let n_conn = self.inner.shared.cfg.connections.max(1);
        let slot = usize::try_from(p).unwrap_or(0) % n_conn;
        try_nudge_slot(&self.inner.shared.connect_tx, node, slot);
        let nodes = self.inner.shared.nodes.lock();
        let slots = nodes.get(&node)?;
        slots.get(slot).cloned().flatten()
    }

    fn nudge_topic(&self, rec: &ProduceRecord) {
        drop(self.inner.shared.meta_tx.try_send(rec.topic.clone()));
        if let Some(p) = rec.partition {
            if let Ok(node) = self
                .inner
                .shared
                .cluster
                .lock()
                .leader_id(rec.topic.as_ref(), p)
            {
                let n_conn = self.inner.shared.cfg.connections.max(1);
                let slot = usize::try_from(p).unwrap_or(0) % n_conn;
                try_nudge_slot(&self.inner.shared.connect_tx, node, slot);
            }
        }
    }

    async fn ensure_ready(&self, rec: &mut ProduceRecord, deadline: Instant) -> Result<()> {
        loop {
            if self.inner.shared.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed);
            }
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
                let rest = deadline.saturating_duration_since(Instant::now());
                if rest.is_zero() {
                    return Err(Error::Timeout);
                }
                self.reconnect_cached_leaders(std::iter::once(&*rec));
                let timeout = self.inner.shared.cfg.request_timeout.min(rest);
                let _ = partitions_for_timeout(&self.inner.shared, &rec.topic, timeout).await?;
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
                _ = tokio::time::sleep(rest.min(Duration::from_millis(5))) => {}
            }
        }
    }

    async fn wait_buffer(&self, bytes: u64, deadline: Instant) -> Result<()> {
        loop {
            if self.inner.shared.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed);
            }
            if self.inner.shared.try_reserve_buffer(bytes) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let rest = deadline.saturating_duration_since(Instant::now());
            let notified = self.inner.shared.buffer_nudge.notified();
            tokio::pin!(notified);
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(rest.min(Duration::from_millis(5))) => {}
            }
        }
    }

    /// Queue one record and wait for its offset.
    ///
    /// This waits for the Produce response of *this* record before returning.
    /// A loop of `send().await` therefore cannot pipeline. For many records
    /// use [`Self::send_all`] (offsets) or [`Self::try_send`] plus
    /// [`Self::flush`] (throughput).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, rec), fields(topic = %rec.topic))
    )]
    /// Wait for one record's broker ack (`RecordMetadata`) or a terminal error.
    ///
    /// **Cancellation:** dropping this future does **not** dequeue or guarantee the
    /// record was never written. Once accepted into `buffer_memory`, delivery may
    /// continue; the caller outcome is **ambiguous** until `flush`/`close` (or
    /// another observe path) settles it. See `docs/guide.md` (Produce cancellation).
    pub async fn send(&self, rec: ProduceRecord) -> Result<RecordMetadata> {
        self.ensure_send_open()?;
        let rx = self.enqueue_one(rec).await?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Queue every record, then wait for every offset.
    ///
    /// Records that already have metadata are handed to workers before a
    /// later topic is resolved, so batches fill while that lookup runs.
    /// Topics that still need metadata share one request. Empty input
    /// returns an empty vec.
    pub async fn send_all(
        &self,
        recs: impl IntoIterator<Item = ProduceRecord>,
    ) -> Result<Vec<RecordMetadata>> {
        self.ensure_send_open()?;
        let recs: Vec<ProduceRecord> = recs.into_iter().collect();
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        let mut pending: VecDeque<ProduceRecord> = recs.into();
        let mut rxs = Vec::with_capacity(pending.len());
        let mut failed: Vec<(Arc<str>, Error)> = Vec::new();
        while let Some(rec) = pending.pop_front() {
            if let Some(error) = prepared_topic_error(&failed, &rec.topic) {
                return Err(error);
            }
            if topic_needs_metadata(&self.inner.shared, &rec.topic) {
                let mut topics = vec![Arc::clone(&rec.topic)];
                for later in &pending {
                    if topic_needs_metadata(&self.inner.shared, &later.topic)
                        && topics
                            .iter()
                            .all(|topic| topic.as_ref() != later.topic.as_ref())
                    {
                        topics.push(Arc::clone(&later.topic));
                    }
                }
                if topics.len() > 1 {
                    let budget = self.inner.shared.cfg.max_block;
                    if budget.is_zero() {
                        return Err(Error::Timeout);
                    }
                    let timeout = self.inner.shared.cfg.request_timeout.min(budget);
                    self.reconnect_cached_leaders(std::iter::once(&rec).chain(pending.iter()));
                    let requested =
                        refresh_topics_within(&self.inner.shared, &topics, timeout).await?;
                    for topic in &requested {
                        if prepared_topic_error(&failed, topic).is_some() {
                            continue;
                        }
                        if let Some(error) = unfresh_topic_error(&self.inner.shared, topic) {
                            failed.push((Arc::clone(topic), error));
                        }
                    }
                    if let Some(error) = prepared_topic_error(&failed, &rec.topic) {
                        return Err(error);
                    }
                }
            }
            // Start every known leader handshake before this record waits
            // for its own worker, so a later leader is already dialing.
            self.nudge_known_leaders(std::iter::once(&rec).chain(pending.iter()));
            rxs.push(self.enqueue_one(rec).await?);
        }
        let mut out = Vec::with_capacity(rxs.len());
        for rx in rxs {
            out.push(rx.await.map_err(|_| Error::Closed)??);
        }
        Ok(out)
    }

    /// Redial idle leader sockets while a metadata refresh is still running.
    fn reconnect_cached_leaders<'a>(&self, recs: impl Iterator<Item = &'a ProduceRecord>) {
        if self.inner.shared.sticky.is_some() {
            return;
        }
        let shared = &self.inner.shared;
        let n_conn = shared.cfg.connections.max(1);
        let mut slots = HashSet::new();
        {
            let cluster = shared.cluster.lock();
            for rec in recs {
                let Some(np) = cluster.partition_count(rec.topic.as_ref()) else {
                    continue;
                };
                if np <= 0 {
                    continue;
                }
                let partition = match rec.partition {
                    Some(partition) => partition,
                    None => pick_part(rec, np, shared.partitioner.as_ref()),
                };
                let Ok(node) = cluster.leader_id(rec.topic.as_ref(), partition) else {
                    continue;
                };
                let slot = usize::try_from(partition).unwrap_or(0) % n_conn;
                let _ = slots.insert((node, slot));
            }
        }
        let nodes = shared.nodes.lock();
        for (node, slot) in slots {
            let Some(handle) = nodes
                .get(&node)
                .and_then(|slots| slots.get(slot))
                .and_then(|handle| handle.as_ref())
            else {
                continue;
            };
            handle.ctrl.try_send(Ctrl::Reconnect).unwrap_or(());
        }
    }

    /// Nudge a connection for every record whose leader is already known.
    fn nudge_known_leaders<'a>(&self, recs: impl Iterator<Item = &'a ProduceRecord>) {
        if self.inner.shared.sticky.is_some() {
            return;
        }
        let shared = &self.inner.shared;
        let n_conn = shared.cfg.connections.max(1);
        let max_age = shared.cfg.metadata_max_age;
        let mut slots = HashSet::new();
        {
            let cluster = shared.cluster.lock();
            for rec in recs {
                if !cluster.topic_fresh(rec.topic.as_ref(), max_age) {
                    continue;
                }
                let Some(np) = cluster.partition_count(rec.topic.as_ref()) else {
                    continue;
                };
                if np <= 0 {
                    continue;
                }
                let partition = match rec.partition {
                    Some(partition) => partition,
                    None => pick_part(rec, np, shared.partitioner.as_ref()),
                };
                let Ok(node) = cluster.leader_id(rec.topic.as_ref(), partition) else {
                    continue;
                };
                let slot = usize::try_from(partition).unwrap_or(0) % n_conn;
                let _ = slots.insert((node, slot));
            }
        }
        for (node, slot) in slots {
            try_nudge_slot(&shared.connect_tx, node, slot);
        }
    }

    fn ensure_send_open(&self) -> Result<()> {
        if self.inner.shared.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        // A fenced producer fails sends immediately instead of emitting
        // records the broker must reject. Abort-required does *not* block
        // sends: the failed send already reported its error, and later sends
        // in the still-open broker transaction stay legal (they cannot commit
        // until the abort-required state clears via `abort_transaction`).
        // This differs from Java `maybeAddPartition`, which also blocks sends
        // while abort-required; the pinned buffer-ownership contract requires
        // a one-time `AddPartitionsToTxn` failure to not poison later sends.
        if self.inner.shared.cfg.transactional_id.is_some() {
            self.inner.shared.fail_on_txn_fatal()?;
        }
        Ok(())
    }

    async fn enqueue_one(
        &self,
        rec: ProduceRecord,
    ) -> Result<oneshot::Receiver<Result<RecordMetadata>>> {
        let (tx, rx) = oneshot::channel();
        let mut rec = self.inner.shared.interceptors.on_send(rec);
        let bytes = reject_oversized(&self.inner.shared.cfg, &rec)?;
        let block_deadline = Instant::now() + self.inner.shared.cfg.max_block;
        if self.inner.shared.sticky.is_some() {
            self.enqueue_sticky(rec, Some(tx), bytes, block_deadline)
                .await?;
            return Ok(rx);
        }
        let fast = self.fresh_fast_route(&rec);
        let (w, tracker) = if let Some((partition, handle, tracker)) = fast {
            rec.partition = Some(partition);
            (handle, Some(tracker))
        } else {
            self.ensure_ready(&mut rec, block_deadline).await?;
            let handle = self.worker_for(&rec).ok_or(Error::Closed)?;
            self.remember_fast(&rec);
            (Arc::new(handle), None)
        };
        self.wait_buffer(bytes, block_deadline).await?;
        let mut reservation = BufferReservation {
            shared: &self.inner.shared,
            bytes,
            active: true,
        };
        if self.inner.shared.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        compact_record_backing(&mut rec);
        let now = Instant::now();
        let deadline = now + self.inner.shared.cfg.delivery_timeout;
        let topic = tracker.is_none().then(|| Arc::clone(&rec.topic));
        let rest_block = block_deadline.saturating_duration_since(now);
        if rest_block.is_zero() {
            return Err(Error::Timeout);
        }
        let send_res = tokio::time::timeout(
            rest_block,
            w.data.send(Pending {
                sticky_record: None,
                sticky: None,
                rec,
                tx: Some(tx),
                seq: None,
                batch_base_seq: None,
                batch_len: 1,
                epoch_gen: self.inner.shared.epoch_gen.load(Ordering::SeqCst),
                deadline,
                queued_at: now,
                retry: 0,
                skip_meta_refresh: false,
                retry_after: now,
            }),
        )
        .await;
        match send_res {
            Ok(Ok(())) => reservation.active = false,
            Ok(Err(_)) => {
                return Err(Error::Closed);
            }
            Err(_) => {
                return Err(Error::Timeout);
            }
        }
        if let Some(tracker) = tracker {
            self.inner.shared.note_queued_totals(1, bytes);
            tracker.note_queued(1, bytes);
        } else if let Some(topic) = topic {
            self.inner.shared.note_queued_n(&topic, 1, bytes);
        }
        Ok(rx)
    }

    fn new_pending(
        &self,
        rec: ProduceRecord,
        tx: Option<oneshot::Sender<Result<RecordMetadata>>>,
        now: Instant,
    ) -> Pending {
        Pending {
            sticky_record: None,
            sticky: None,
            rec,
            tx,
            seq: None,
            batch_base_seq: None,
            batch_len: 1,
            epoch_gen: self.inner.shared.epoch_gen.load(Ordering::SeqCst),
            deadline: now + self.inner.shared.cfg.delivery_timeout,
            queued_at: now,
            retry: 0,
            skip_meta_refresh: false,
            retry_after: now,
        }
    }

    /// The one accounting linearization point. Both route and plan are pure until
    /// the actual try_send succeeds. No callback, network operation, or await is
    /// performed while metadata/policy locks are held.
    fn sticky_enqueue(
        &self,
        pending_slot: &mut Option<Pending>,
        unkeyed: bool,
        block_deadline: Option<Instant>,
    ) -> Result<()> {
        let Some(pending) = pending_slot.as_mut() else {
            return Err(Error::Closed);
        };
        let shared = &self.inner.shared;
        let Some(policy) = &shared.sticky else {
            return Err(Error::Closed);
        };
        let cluster = shared.cluster.lock();
        let mut policy = policy.lock();
        if shared.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        let Some(np) = cluster.partition_count(&pending.rec.topic) else {
            return Err(Error::QueueFull);
        };
        let route = if unkeyed {
            let Some(route) = policy.route(&pending.rec.topic, &cluster) else {
                return Err(Error::QueueFull);
            };
            debug_assert!(policy.route_current(&pending.rec.topic, route, &cluster));
            Some(route)
        } else {
            None
        };
        let partition = route.map_or_else(
            || {
                pending.rec.partition.unwrap_or_else(|| {
                    partition_for_key(pending.rec.key.as_deref().unwrap_or_default(), np)
                })
            },
            |route| route.partition,
        );
        pending.rec.partition = Some(partition);
        let Ok(node) = cluster.leader_id(&pending.rec.topic, partition) else {
            return Err(Error::QueueFull);
        };
        let slot = usize::try_from(partition).unwrap_or(0) % shared.cfg.connections.max(1);
        let worker = shared
            .nodes
            .lock()
            .get(&node)
            .and_then(|slots| slots.get(slot))
            .cloned()
            .flatten();
        let Some(worker) = worker else {
            return Err(Error::QueueFull);
        };
        let topic = Arc::clone(&pending.rec.topic);
        let mut plan = policy.plan(&topic, partition, &worker.id, estimate(pending), &cluster);
        plan.next_rng = route.and_then(|route| route.rng);
        let now = Instant::now();
        if block_deadline.is_some_and(|deadline| now >= deadline) {
            return Err(Error::Timeout);
        }
        pending.queued_at = now;
        pending.retry_after = now;
        pending.deadline = now + shared.cfg.delivery_timeout;
        pending.sticky = Some(plan.tag);
        let Some(pending) = pending_slot.take() else {
            return Err(Error::Closed);
        };
        match worker.data.try_send(pending) {
            Ok(()) => {
                policy.commit(&topic, partition, &worker.id, unkeyed, plan, &cluster);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(mut pending)) => {
                pending.sticky = None;
                *pending_slot = Some(pending);
                Err(Error::QueueFull)
            }
            Err(mpsc::error::TrySendError::Closed(mut pending)) => {
                pending.sticky = None;
                *pending_slot = Some(pending);
                Err(Error::Closed)
            }
        }
    }

    async fn wait_sticky_record(&self, deadline: Instant) -> Result<StickyRecordPermit> {
        let shared = &self.inner.shared;
        let records = shared.sticky_records.as_ref().ok_or(Error::Closed)?;
        loop {
            if shared.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::Timeout);
            }
            if let Some(permit) = records.acquire() {
                return Ok(permit);
            }
            let notified = records.nudge.notified();
            tokio::pin!(notified);
            tokio::select! { _ = notified => {}, _ = tokio::time::sleep(deadline.saturating_duration_since(now).min(Duration::from_millis(5))) => {} }
        }
    }

    async fn enqueue_sticky(
        &self,
        rec: ProduceRecord,
        tx: Option<oneshot::Sender<Result<RecordMetadata>>>,
        bytes: u64,
        deadline: Instant,
    ) -> Result<()> {
        let shared = &self.inner.shared;
        let unkeyed = rec.partition.is_none() && rec.key.is_none();
        let topic = Arc::clone(&rec.topic);
        let mut pending = Some(self.new_pending(rec, tx, Instant::now()));
        // Metadata waits consume the same absolute max_block budget as admission.
        if !shared
            .cluster
            .lock()
            .topic_fresh(&topic, shared.cfg.metadata_max_age)
        {
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                return Err(Error::Timeout);
            }
            let _ = partitions_for_timeout(shared, &topic, shared.cfg.request_timeout.min(rest))
                .await?;
        }
        let record_permit = self.wait_sticky_record(deadline).await?;
        pending.as_mut().ok_or(Error::Closed)?.sticky_record = Some(record_permit);
        self.wait_buffer(bytes, deadline).await?;
        let mut reservation = BufferReservation {
            shared,
            bytes,
            active: true,
        };
        compact_record_backing(&mut pending.as_mut().ok_or(Error::Closed)?.rec);
        loop {
            if shared.closed.load(Ordering::SeqCst) {
                return Err(Error::Closed);
            }
            if let Some(err) = peek_meta_err(shared) {
                return Err(err);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::Timeout);
            }
            let current = pending.as_mut().ok_or(Error::Closed)?;
            current.queued_at = now;
            current.retry_after = now;
            current.deadline = now + shared.cfg.delivery_timeout;
            match self.sticky_enqueue(&mut pending, unkeyed, Some(deadline)) {
                Ok(()) => {
                    reservation.active = false;
                    shared.note_queued_n(&topic, 1, bytes);
                    return Ok(());
                }
                Err(Error::QueueFull) => {
                    if let Some(current) = &pending {
                        self.nudge_topic(&current.rec);
                    }
                }
                Err(err) => return Err(err),
            }
            // Capacity/readiness notifications and the bounded fallback wake are
            // outside the policy lock. Cancellation drops the reserved budget.
            let notified = shared.cache_nudge.notified();
            tokio::pin!(notified);
            let rest = deadline.saturating_duration_since(Instant::now());
            tokio::select! { _ = notified => {}, _ = tokio::time::sleep(rest.min(Duration::from_millis(5))) => {} }
        }
    }

    /// Test hook: bounded topic/cohort/text state, pressure admissions, and total
    /// admitted unkeyed conservative packed bytes (including new-cohort overhead).
    #[doc(hidden)]
    pub fn __test_sticky_state(&self) -> Option<(usize, usize, usize, u64, u64)> {
        self.inner
            .shared
            .sticky
            .as_ref()
            .map(|state| state.lock().counts())
    }

    /// Test hook: currently reserved initial-admission slots plus accepted
    /// records, and the hard normalized record capacity. Includes byte-empty
    /// records and slots held by a pre-enqueue future; retries do not reacquire.
    #[doc(hidden)]
    pub fn __test_sticky_records(&self) -> Option<(usize, usize)> {
        self.inner
            .shared
            .sticky_records
            .as_ref()
            .map(|records| (records.used.load(Ordering::Acquire), records.limit))
    }

    fn fast_route(
        &self,
        rec: &ProduceRecord,
    ) -> Option<(
        i32,
        Arc<WorkerHandle>,
        Arc<crate::metrics::ProduceTopicTracker>,
    )> {
        let fast = self.inner.shared.fast.read();
        let f = fast.routes.get(rec.topic.as_ref())?;
        if f.handles.is_empty() || f.np <= 0 {
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
        let w = Arc::clone(f.handles.get(i)?);
        Some((p, w, Arc::clone(&f.tracker)))
    }

    /// A cached route whose metadata is still inside `metadata_max_age`.
    ///
    /// A recorded metadata error still goes through the slow path so the
    /// caller sees it before another record is queued.
    fn fresh_fast_route(
        &self,
        rec: &ProduceRecord,
    ) -> Option<(
        i32,
        Arc<WorkerHandle>,
        Arc<crate::metrics::ProduceTopicTracker>,
    )> {
        if peek_meta_err(&self.inner.shared).is_some() {
            return None;
        }
        let max_age = self.inner.shared.cfg.metadata_max_age;
        if max_age.is_zero() {
            return None;
        }
        let fast = self.inner.shared.fast.read();
        let f = fast.routes.get(rec.topic.as_ref())?;
        let fresh = f
            .fetched_at
            .is_some_and(|at| Instant::now().saturating_duration_since(at) < max_age);
        if !fresh || f.handles.is_empty() || f.np <= 0 {
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
        let w = Arc::clone(f.handles.get(i)?);
        Some((p, w, Arc::clone(&f.tracker)))
    }

    fn remember_fast(&self, rec: &ProduceRecord) {
        let (np, fetched_at, leader_nodes, complete) = {
            let cluster = self.inner.shared.cluster.lock();
            let Some(np) = cluster.partition_count(rec.topic.as_ref()) else {
                return;
            };
            if np <= 0 {
                return;
            }
            let Ok(n) = usize::try_from(np) else {
                return;
            };
            let leaders = cluster.leader_ids(rec.topic.as_ref());
            let mut leader_nodes = Vec::with_capacity(n);
            for partition in 0..np {
                let Some(idx) = usize::try_from(partition).ok() else {
                    break;
                };
                let Some(node) = leaders.get(idx).copied().filter(|id| *id >= 0) else {
                    break;
                };
                if !cluster.brokers.contains_key(&node) {
                    break;
                }
                leader_nodes.push(node);
            }
            let complete = leader_nodes.len() == n;
            (
                np,
                cluster.topic_fetched_at(rec.topic.as_ref()),
                leader_nodes,
                complete,
            )
        };
        let n_conn = self.inner.shared.cfg.connections.max(1);
        let mut handles = Vec::with_capacity(leader_nodes.len());
        {
            let nodes = self.inner.shared.nodes.lock();
            for (partition, node) in leader_nodes.into_iter().enumerate() {
                let slot = partition % n_conn;
                try_nudge_slot(&self.inner.shared.connect_tx, node, slot);
                let Some(handle) = nodes
                    .get(&node)
                    .and_then(|slots| slots.get(slot))
                    .cloned()
                    .flatten()
                else {
                    return;
                };
                handles.push(Arc::new(handle));
            }
        }
        if !complete {
            return;
        }
        let tracker = self.inner.shared.topic_tracker(&rec.topic);
        let route = FastRoute {
            np,
            handles,
            tracker,
            fetched_at,
        };
        let mut fast = self.inner.shared.fast.write();
        if fast.routes.contains_key(rec.topic.as_ref()) {
            if let Some(slot) = fast.routes.get_mut(rec.topic.as_ref()) {
                *slot = route;
            }
            return;
        }
        while fast.order.len() >= FAST_ROUTE_TOPICS {
            let Some(old) = fast.order.pop_front() else {
                break;
            };
            let _ = fast.routes.remove(old.as_ref());
        }
        fast.order.push_back(Arc::clone(&rec.topic));
        let _ = fast.routes.insert(Arc::clone(&rec.topic), route);
    }

    /// Enqueue without a per-record future. Delivery is observed on [`Self::flush`].
    ///
    /// Returns [`Error::QueueFull`] until metadata and a connection to the
    /// partition leader are ready, or when [`ProducerConfig::buffer_memory`]
    /// is full. Call again; [`Self::send`] waits up to
    /// [`ProducerConfig::max_block`].
    /// A record whose Java `estimateSizeInBytesUpperBound` is larger than
    /// [`ProducerConfig::max_request_size`] or [`ProducerConfig::buffer_memory`]
    /// returns [`Error::RecordTooLarge`] without waiting (Java
    /// `RecordTooLargeException` / `ensureValidRecordSize`).
    /// Records are never queued without a partition, so each partition is
    /// pinned to one TCP connection on its current leader.
    pub fn try_send(&self, rec: ProduceRecord) -> Result<()> {
        if self.inner.shared.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        // Same fencing rule as [`Self::send_all`]: fatal blocks, abortable
        // does not (see the comment there).
        if self.inner.shared.cfg.transactional_id.is_some() {
            self.inner.shared.fail_on_txn_fatal()?;
        }
        let mut rec = self.inner.shared.interceptors.on_send(rec);
        let bytes = reject_oversized(&self.inner.shared.cfg, &rec)?;
        if self.inner.shared.sticky.is_some() {
            let record_permit = self
                .inner
                .shared
                .sticky_records
                .as_ref()
                .and_then(StickyRecordBudget::acquire)
                .ok_or(Error::QueueFull)?;
            if !self.inner.shared.try_reserve_buffer(bytes) {
                return Err(Error::QueueFull);
            }
            let mut reservation = BufferReservation {
                shared: &self.inner.shared,
                bytes,
                active: true,
            };
            compact_record_backing(&mut rec);
            let unkeyed = rec.partition.is_none() && rec.key.is_none();
            let now = Instant::now();
            let topic = Arc::clone(&rec.topic);
            let mut current = self.new_pending(rec, None, now);
            current.sticky_record = Some(record_permit);
            let mut pending = Some(current);
            match self.sticky_enqueue(&mut pending, unkeyed, None) {
                Ok(()) => {
                    reservation.active = false;
                    self.inner.shared.note_queued_n(&topic, 1, bytes);
                    return Ok(());
                }
                Err(err) => {
                    if matches!(err, Error::QueueFull) {
                        if let Some(current) = &pending {
                            self.nudge_topic(&current.rec);
                        }
                    }
                    return Err(err);
                }
            }
        }
        let (w, tracker) = if let Some((p, w, tracker)) = self.fast_route(&rec) {
            rec.partition = Some(p);
            (w, Some(tracker))
        } else {
            let _ = self.apply_cached_partition(&mut rec);
            let Some(w) = self.worker_for(&rec) else {
                self.nudge_topic(&rec);
                return Err(Error::QueueFull);
            };
            self.remember_fast(&rec);
            (Arc::new(w), None)
        };
        let now = Instant::now();
        let deadline = now + self.inner.shared.cfg.delivery_timeout;
        let topic = tracker.is_none().then(|| Arc::clone(&rec.topic));
        if !self.inner.shared.try_reserve_buffer(bytes) {
            return Err(Error::QueueFull);
        }
        let mut reservation = BufferReservation {
            shared: &self.inner.shared,
            bytes,
            active: true,
        };
        compact_record_backing(&mut rec);
        if let Err(e) = w.data.try_send(Pending {
            sticky_record: None,
            sticky: None,
            rec,
            tx: None,
            seq: None,
            batch_base_seq: None,
            batch_len: 1,
            epoch_gen: self.inner.shared.epoch_gen.load(Ordering::SeqCst),
            deadline,
            queued_at: now,
            retry: 0,
            skip_meta_refresh: false,
            retry_after: now,
        }) {
            return Err(match e {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            });
        }
        reservation.active = false;
        if let Some(tracker) = tracker {
            self.inner.shared.note_queued_totals(1, bytes);
            tracker.note_queued(1, bytes);
        } else if let Some(topic) = topic {
            self.inner.shared.note_queued_n(&topic, 1, bytes);
        }
        Ok(())
    }

    /// Test hook: inject a pre-send fault for the next drained batch.
    #[doc(hidden)]
    pub fn inject_pre_send_fault(&self, fault: PreSendFault) {
        *self.inner.shared.pre_send_fault.lock() = Some((fault, Some(1)));
    }

    /// Test hook: inject a pre-send fault for the next `n` drained batches.
    #[doc(hidden)]
    pub fn inject_pre_send_fault_times(&self, fault: PreSendFault, n: u32) {
        *self.inner.shared.pre_send_fault.lock() = Some((fault, Some(n)));
    }

    /// Count of retries currently in flight.
    #[must_use]
    pub fn retries_in_flight(&self) -> usize {
        self.inner.shared.retries_out.load(Ordering::SeqCst)
    }

    /// Test hook: read the current idempotent producer id (KL03-09).
    #[doc(hidden)]
    pub fn __test_producer_id(&self) -> i64 {
        self.inner.shared.producer_id.load(Ordering::SeqCst)
    }

    /// Test hook: read the current idempotent producer epoch (KL03-09).
    #[doc(hidden)]
    pub fn __test_producer_epoch(&self) -> i16 {
        self.inner.shared.producer_epoch.load(Ordering::SeqCst)
    }

    /// Test hook: force the idempotent producer epoch, e.g. to `i16::MAX`
    /// to exercise epoch-exhaustion renewal (KL03-09).
    #[doc(hidden)]
    pub fn __test_set_producer_epoch(&self, epoch: i16) {
        self.inner
            .shared
            .producer_epoch
            .store(epoch, Ordering::SeqCst);
    }

    /// Test hook: inspect worker task handles to verify task termination.
    #[doc(hidden)]
    pub fn test_worker_tasks(&self) -> Vec<Arc<tokio::task::JoinHandle<()>>> {
        self.inner
            .shared
            .nodes
            .lock()
            .values()
            .flatten()
            .flatten()
            .map(|w| Arc::clone(&w.task))
            .collect()
    }

    /// Produce counters and ack latency since connect (min/mean/max and p50/p99).
    ///
    /// [`crate::ProducerMetrics::topics`] is one row per topic that queued, acked, or
    /// failed at least one record.
    #[must_use]
    pub fn metrics(&self) -> crate::ProducerMetrics {
        crate::ProducerMetrics {
            records_queued: self.inner.shared.m_queued.load(Ordering::Relaxed),
            records_acked: self.inner.shared.m_acked.load(Ordering::Relaxed),
            produce_errors: self.inner.shared.m_errors.load(Ordering::Relaxed),
            bytes_queued: self.inner.shared.m_bytes.load(Ordering::Relaxed),
            bytes_buffered: self.inner.shared.buffered_bytes.load(Ordering::Relaxed),
            ack_latency: self.inner.shared.ack_latency.snapshot(),
            throttle: self.inner.shared.throttle_metrics.snapshot(),
            topics: crate::metrics::snapshot_produce_topics(&self.inner.shared.topics.lock()),
        }
    }

    /// Java `clientInstanceId` (KIP-714 GetTelemetrySubscriptions).
    ///
    /// Returns [`crate::Uuid`] (Java `Uuid`). The first call sends a zero
    /// UUID; the broker assigns one. Later calls return the cached id
    /// without another round-trip. Waits up to
    /// [`ProducerConfig::request_timeout`]. For a one-shot timeout, use
    /// [`Self::client_instance_id_timeout`].
    pub async fn client_instance_id(&self) -> Result<crate::Uuid> {
        let timeout = self.inner.shared.cfg.request_timeout;
        self.client_instance_id_timeout(timeout).await
    }

    /// [`Self::client_instance_id`] with a one-shot timeout (Java
    /// `clientInstanceId(Duration)`).
    ///
    /// `timeout` is the GetTelemetrySubscriptions RPC deadline. Cached after
    /// the first successful call; later calls ignore `timeout`.
    pub async fn client_instance_id_timeout(&self, timeout: Duration) -> Result<crate::Uuid> {
        if let Some(id) = *self.inner.shared.client_instance_id.lock() {
            return Ok(crate::Uuid::from_bytes(id));
        }
        let version = self.inner.shared.telemetry_version.ok_or_else(|| {
            Error::Unsupported("broker does not support GetTelemetrySubscriptions".into())
        })?;
        let mut meta = self.inner.shared.meta.lock().await;
        let id =
            crate::admin::fetch_client_instance_id(&mut meta, version, timeout, [0; 16]).await?;
        drop(meta);
        *self.inner.shared.client_instance_id.lock() = Some(id);
        Ok(crate::Uuid::from_bytes(id))
    }

    async fn flush_workers(&self, deadline: Instant) -> Result<()> {
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        let workers: Vec<WorkerHandle> = self
            .inner
            .shared
            .nodes
            .lock()
            .values()
            .flatten()
            .flatten()
            .cloned()
            .collect();
        if workers.is_empty() {
            return Ok(());
        }
        let mut rxs = Vec::with_capacity(workers.len());
        for w in &workers {
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                return Err(Error::Timeout);
            }
            let (tx, rx) = oneshot::channel();
            match tokio::time::timeout(rest, w.ctrl.send(Ctrl::Flush(tx))).await {
                Ok(Ok(())) => rxs.push(rx),
                Ok(Err(_)) => return Err(Error::Closed),
                Err(_) => return Err(Error::Timeout),
            }
        }
        for rx in rxs {
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                return Err(Error::Timeout);
            }
            match tokio::time::timeout(rest, rx).await {
                Ok(Ok(result)) => result?,
                Ok(Err(_)) => return Err(Error::Closed),
                Err(_) => return Err(Error::Timeout),
            }
        }
        Ok(())
    }

    /// Java `initTransactions`. [`Self::new`] already runs `InitProducerId`
    /// when [`ProducerConfig::transactional_id`] is set. Safe to call again.
    ///
    /// Missing `transactional.id` is Java `IllegalStateException`
    /// (`Cannot use transactional methods without enabling transactions`).
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn init_transactions(&self) -> Result<()> {
        if self.inner.shared.cfg.transactional_id.is_none() {
            return Err(reject_java_no_transaction_manager());
        }
        if self.inner.shared.producer_id.load(Ordering::SeqCst) < 0 {
            return Err(Error::protocol("InitProducerId did not run"));
        }
        Ok(())
    }

    /// Start a transaction. Requires [`ProducerConfig::transactional_id`].
    ///
    /// Missing `transactional.id` is the same Java message as
    /// [`Self::init_transactions`].
    ///
    /// Fails with the remembered error while the producer is fenced (fatal)
    /// or the previous transaction still needs an abort (Java
    /// `maybeFailWithError`): abort first, then begin again. A fenced
    /// producer never recovers; close it and create a replacement with the
    /// same `transactional.id`.
    ///
    /// Exactly-once scope: one transaction atomically commits its produced
    /// records together with the consumed input offsets sent via
    /// [`Self::send_offsets_to_transaction`]. Both land in a single broker
    /// outcome (commit or abort) and can never be split across outcomes.
    /// External side effects (database writes, RPCs, files) are *not* part
    /// of the transaction: this client cannot roll back anything outside the
    /// broker, so perform side effects only after
    /// [`Self::commit_transaction`] succeeds, and design them to tolerate
    /// duplicates when a commit outcome is ambiguous (timeout) and the
    /// application retries.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn begin_transaction(&self) -> Result<()> {
        if self.inner.shared.cfg.transactional_id.is_none() {
            return Err(reject_java_no_transaction_manager());
        }
        self.inner.shared.fail_on_txn_error()?;
        self.inner.shared.in_txn.store(true, Ordering::SeqCst);
        self.inner.shared.txn_partitions.lock().clear();
        self.inner.shared.txn_added.lock().clear();
        Ok(())
    }

    /// Flush, then commit the current transaction (`EndTxn`
    /// [`TransactionResult::Commit`]).
    ///
    /// Fails without sending `EndTxn` when there is no open transaction, when
    /// the producer is fenced, or when the transaction is abort-required
    /// (Java `maybeFailWithError`): a failed commit never invents a partial
    /// outcome, so produced records and consumed offsets stay in one broker
    /// transaction until [`Self::abort_transaction`] runs.
    ///
    /// A transport timeout here is ambiguous: the broker may still have
    /// committed. The transaction stays open and `EndTxn` is idempotent, so
    /// retrying the commit is safe and can never commit the same records and
    /// offsets twice in different outcomes. See [`Self::begin_transaction`]
    /// for the external-side-effect limits this implies.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn commit_transaction(&self) -> Result<()> {
        if self.inner.shared.cfg.transactional_id.is_none() {
            return Err(reject_java_no_transaction_manager());
        }
        self.inner.shared.fail_on_txn_error()?;
        if !self.inner.shared.in_txn.load(Ordering::SeqCst) {
            return Err(Error::protocol("no transaction in progress"));
        }
        self.flush().await?;
        self.end_txn(TransactionResult::Commit.id()).await
    }

    /// Drain in-flight Produce, then abort (`EndTxn`
    /// [`TransactionResult::Abort`]).
    ///
    /// Abort is the recovery path out of every abort-required error, so it is
    /// allowed while abort-required but still refused once the producer is
    /// fenced (Java `beginAbort`). Aborting with no open transaction is a
    /// programming error and fails without sending `EndTxn`.
    ///
    /// After an abortable transactional failure, classic coordinators
    /// (`EndTxn` below v5) re-init with the last producer id and epoch
    /// (KIP-360); the replacement identity always comes from that broker
    /// response, never from a local bump. `EndTxn` v5 already returns the
    /// bumped identity, so no re-init follows. `INVALID_PRODUCER_ID_MAPPING`
    /// on Produce is fatal (pinned Java `maybeTransitionToErrorState`), not
    /// bump-recoverable.
    ///
    /// A Produce that already completed [`Self::send`] with a broker error
    /// does not fail abort: Java still EndTxn-aborts, then optionally re-inits.
    /// [`Self::commit_transaction`] still fails on that error without
    /// sending `EndTxn`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn abort_transaction(&self) -> Result<()> {
        if self.inner.shared.cfg.transactional_id.is_none() {
            return Err(reject_java_no_transaction_manager());
        }
        self.inner.shared.fail_on_txn_fatal()?;
        if !self.inner.shared.in_txn.load(Ordering::SeqCst) {
            return Err(Error::protocol("no transaction in progress"));
        }
        self.drain_before_abort().await?;
        self.end_txn(TransactionResult::Abort.id()).await?;
        self.inner.shared.clear_txn_abortable();
        if let Err(e) = self.maybe_bump_epoch_after_abort().await {
            // The transaction aborted, but the producer identity cannot be
            // renewed: without a broker-authorized replacement identity the
            // producer must not transact again (Java effectively fences here:
            // the bump `InitProducerId` fails out of `INITIALIZING`).
            self.inner.shared.set_txn_fatal(clone_err(&e));
            return Err(e);
        }
        Ok(())
    }

    /// Wait for in-flight Produce the same way [`Self::flush`] does. Delivery
    /// errors already completed the caller's `send()` and must not block
    /// EndTxn. Closed / timeout still fail so abort does not race inflight.
    async fn drain_before_abort(&self) -> Result<()> {
        match self.flush().await {
            Ok(()) => Ok(()),
            Err(e) if matches!(e, Error::Closed | Error::Timeout) => Err(e),
            Err(_) => Ok(()),
        }
    }

    async fn maybe_bump_epoch_after_abort(&self) -> Result<()> {
        if !self
            .inner
            .shared
            .epoch_bump_required
            .swap(false, Ordering::SeqCst)
        {
            return Ok(());
        }
        bump_producer_epoch(&self.inner.shared).await
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
        self.send_offsets_inner(group_id, &TxnOffsetCommitMember::unknown(), offsets)
            .await
    }

    /// [`Self::send_offsets_with_metadata`] using a group's identity.
    ///
    /// Java `sendOffsetsToTransaction` calls `throwIfInvalidGroupMetadata`
    /// (`generationId` greater than 0 with unknown `member.id`).
    /// TxnOffsetCommit v3+ sends `generation.id`, `member.id`, and
    /// `group.instance.id` from [`crate::ConsumerGroupMetadata`].
    /// Brokers below request v3 return [`crate::Error::Unsupported`] when that
    /// identity is set (Java `TxnOffsetCommitRequest.Builder.groupMetadataSet`).
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
        reject_java_group_metadata(group)?;
        let offsets: Vec<(crate::TopicPartition, crate::OffsetAndMetadata)> = offsets
            .into_iter()
            .map(|(tp, md)| (tp.into(), md.into()))
            .collect();
        let member = TxnOffsetCommitMember {
            generation_id: group.generation_id,
            member_id: group.member_id.clone(),
            group_instance_id: group.group_instance_id.clone(),
        };
        self.send_offsets_inner(&group.group_id, &member, offsets)
            .await
    }

    async fn send_offsets_inner(
        &self,
        group_id: &str,
        member: &TxnOffsetCommitMember,
        offsets: Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>,
    ) -> Result<()> {
        let Some(tid) = self.inner.shared.cfg.transactional_id.clone() else {
            return Err(reject_java_no_transaction_manager());
        };
        // Java `sendOffsetsToTransaction`: fail while fenced or
        // abort-required, then require an open transaction, so consumed
        // offsets can only join the single outcome of the current broker
        // transaction and never leak into another one.
        self.inner.shared.fail_on_txn_error()?;
        if !self.inner.shared.in_txn.load(Ordering::SeqCst) {
            return Err(Error::protocol("no transaction in progress"));
        }
        let timeout = self.inner.shared.cfg.request_timeout;
        let deadline = Instant::now().checked_add(timeout).ok_or(Error::Timeout)?;
        let pid = self.inner.shared.producer_id.load(Ordering::SeqCst);
        let epoch = self.inner.shared.producer_epoch.load(Ordering::SeqCst);
        let version = self.inner.shared.txn_offset_version;
        // TxnOffsetCommit v5 is transaction V2 (KIP-890 Part 2): the
        // group coordinator also performs AddOffsetsToTxn. Skip that
        // RPC when the broker advertised v5 (Java `isTransactionV2Enabled`).
        let load_topics = async {
            let topics = unique_offset_topics(&offsets);
            if topics.len() > 1 {
                let names: Vec<Arc<str>> = topics.iter().copied().map(Arc::<str>::from).collect();
                let requested =
                    refresh_topics_within(&self.inner.shared, &names, timeout_remaining(deadline)?)
                        .await?;
                for topic in topics {
                    let n = if requested.iter().any(|name| name.as_ref() == topic) {
                        partition_count_from_image(&self.inner.shared, topic)?
                    } else if let Some(n) = fresh_partition_count(&self.inner.shared, topic) {
                        n
                    } else {
                        let name = Arc::<str>::from(topic);
                        partitions_for_timeout(
                            &self.inner.shared,
                            &name,
                            timeout_remaining(deadline)?,
                        )
                        .await?
                    };
                    if n <= 0 {
                        return Err(Error::UnknownTopic(topic.to_owned()));
                    }
                }
            } else {
                for topic in topics {
                    let name = Arc::<str>::from(topic);
                    if partitions_for_timeout(
                        &self.inner.shared,
                        &name,
                        timeout_remaining(deadline)?,
                    )
                    .await?
                        <= 0
                    {
                        return Err(Error::UnknownTopic(topic.to_owned()));
                    }
                }
            }
            Ok(())
        };
        if version <= TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 {
            // AddOffsetsToTxn uses the transaction coordinator. Topic metadata
            // uses the metadata socket, so both fit in this deadline.
            let added = async {
                let add_offsets_version = self.inner.shared.add_offsets_version;
                let mut attempt = 0u32;
                loop {
                    let body = txn_roundtrip(
                        &self.inner.shared,
                        ADD_OFFSETS_TO_TXN,
                        add_offsets_version,
                        |buf| {
                            encode_add_offsets_to_txn_request(
                                buf,
                                add_offsets_version,
                                &tid,
                                pid,
                                epoch,
                                group_id,
                            )
                        },
                        timeout_remaining(deadline)?,
                        |body| {
                            Ok(decode_add_offsets_to_txn_response(
                                &mut { body },
                                add_offsets_version,
                            )?
                            .0)
                        },
                    )
                    .await?;
                    let (err, ..) =
                        decode_add_offsets_to_txn_response(&mut body.clone(), add_offsets_version)?;
                    if err == 0 {
                        break;
                    }
                    // Java `AddOffsetsToTxnHandler`: retry retriable codes (the
                    // RPC is idempotent), otherwise fence or require an abort.
                    // An exhausted retry budget degrades to abortable: aborting
                    // and retrying the whole transaction is always safe.
                    let can_handle = self.inner.shared.txn_can_handle_abortable();
                    if classify_add_offsets_error(err, can_handle) == TxnErrorDisposition::Retriable
                        && Instant::now() < deadline
                    {
                        attempt += 1;
                        txn_retry_sleep(&self.inner.shared, attempt, deadline).await;
                        if Instant::now() < deadline {
                            continue;
                        }
                    }
                    let e = Error::broker(err, "AddOffsetsToTxn");
                    self.inner.shared.apply_txn_disposition(
                        classify_add_offsets_error(err, can_handle),
                        clone_err(&e),
                    );
                    return Err(e);
                }
                Ok(())
            };
            let (added, loaded) = tokio::join!(added, load_topics);
            added?;
            loaded?;
        } else {
            load_topics.await?;
        }
        let grouped = {
            let cluster = self.inner.shared.cluster.lock();
            group_txn_offsets(&offsets, &cluster)
        };
        let mut attempt = 0u32;
        loop {
            let body = group_coord_roundtrip(
                &self.inner.shared.cfg,
                group_id,
                TXN_OFFSET_COMMIT,
                version,
                self.inner.shared.find_coord_version,
                |buf| {
                    encode_txn_offset_commit_borrowed(
                        buf, version, &tid, group_id, pid, epoch, member, &grouped,
                    )
                },
                timeout_remaining(deadline)?,
                |body| decode_txn_offset_commit_response(&mut { body }, version),
            )
            .await?;
            let err = decode_txn_offset_commit_response(&mut body.clone(), version)?;
            if err == 0 {
                break;
            }
            // Java `TxnOffsetCommitHandler`: retry retriable codes, otherwise
            // fence or require an abort (see the `AddOffsetsToTxn` loop above
            // for the exhausted-budget rule).
            if classify_txn_offset_commit_error(err) == TxnErrorDisposition::Retriable
                && Instant::now() < deadline
            {
                attempt += 1;
                txn_retry_sleep(&self.inner.shared, attempt, deadline).await;
                if Instant::now() < deadline {
                    continue;
                }
            }
            let e = Error::broker(err, "TxnOffsetCommit");
            self.inner
                .shared
                .apply_txn_disposition(classify_txn_offset_commit_error(err), clone_err(&e));
            return Err(e);
        }
        Ok(())
    }

    async fn end_txn(&self, committed: bool) -> Result<()> {
        let Some(tid) = self.inner.shared.cfg.transactional_id.clone() else {
            return Err(reject_java_no_transaction_manager());
        };
        let timeout = self.inner.shared.cfg.request_timeout;
        let pid = self.inner.shared.producer_id.load(Ordering::SeqCst);
        let epoch = self.inner.shared.producer_epoch.load(Ordering::SeqCst);
        let end_txn_version = self.inner.shared.end_txn_version;
        let is_abort = !committed;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let (new_pid, new_epoch) = loop {
            // A transport error here is ambiguous (the broker may have
            // applied `EndTxn` anyway). It sets no sticky state: the
            // transaction stays open and the caller retries the same
            // idempotent `EndTxn`, which can never split one transaction
            // into two outcomes.
            let body = txn_roundtrip(
                &self.inner.shared,
                END_TXN,
                end_txn_version,
                |buf| encode_end_txn_request(buf, end_txn_version, &tid, pid, epoch, committed),
                timeout_remaining(deadline)?,
                |body| Ok(decode_end_txn_response(&mut { body }, end_txn_version)?.0),
            )
            .await?;
            let (err, new_pid, new_epoch, ..) =
                decode_end_txn_response(&mut body.clone(), end_txn_version)?;
            if err == 0 {
                break (new_pid, new_epoch);
            }
            // Java `EndTxnHandler`: retry retriable codes (an `EndTxn`
            // commit is idempotent), otherwise fence or require an abort
            // (see the `AddOffsetsToTxn` loop for the exhausted-budget rule).
            let can_handle = self.inner.shared.txn_can_handle_abortable();
            if classify_end_txn_error(err, is_abort, can_handle) == TxnErrorDisposition::Retriable
                && Instant::now() < deadline
            {
                attempt += 1;
                txn_retry_sleep(&self.inner.shared, attempt, deadline).await;
                if Instant::now() < deadline {
                    continue;
                }
            }
            let e = Error::broker(err, "EndTxn");
            self.inner.shared.apply_txn_disposition(
                classify_end_txn_error(err, is_abort, can_handle),
                clone_err(&e),
            );
            return Err(e);
        };
        self.inner
            .shared
            .apply_end_txn_identity(end_txn_version, new_pid, new_epoch);
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
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            self.flush_workers(deadline).await?;
            if self.inner.shared.retries_out.load(Ordering::SeqCst) == 0 {
                self.flush_workers(deadline).await?;
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

    /// Flush, then stop workers. Further sends on **any** clone return [`Error::Closed`].
    ///
    /// Prefer an explicit `close` over dropping the last handle: drop alone does
    /// not wait for in-flight produce outcomes (see the guide cancellation table).
    pub async fn close(self) -> Result<()> {
        let timeout = self
            .inner
            .shared
            .cfg
            .delivery_timeout
            .max(self.inner.shared.cfg.request_timeout);
        let deadline = Instant::now() + timeout;
        let flush = self.flush_until(deadline).await;
        self.shutdown_workers(Some(deadline)).await;
        self.inner.shared.interceptors.close();
        match flush {
            Err(Error::Timeout) => Err(Error::Timeout),
            _ => Ok(()),
        }
    }

    /// Flush for up to `timeout`, then stop workers (Java `close(Duration)`).
    ///
    /// A flush timeout is returned after the producer is closed.
    pub async fn close_timeout(self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let flush = self.flush_until(deadline).await;
        self.shutdown_workers(Some(deadline)).await;
        self.inner.shared.interceptors.close();
        match flush {
            Err(Error::Timeout) => Err(Error::Timeout),
            _ => Ok(()),
        }
    }

    async fn shutdown_workers(&self, deadline: Option<Instant>) {
        // Refuse new enqueue before workers drain so clones observe Closed.
        self.inner.shared.closed.store(true, Ordering::SeqCst);
        self.inner.shared.buffer_nudge.notify_waiters();
        self.inner.shared.cache_nudge.notify_waiters();

        let workers: Vec<WorkerHandle> = self
            .inner
            .shared
            .nodes
            .lock()
            .values()
            .flatten()
            .flatten()
            .cloned()
            .collect();
        let mut rxs = Vec::with_capacity(workers.len());
        for w in &workers {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                break;
            }
            let (tx, rx) = oneshot::channel();
            let send = w.ctrl.send(Ctrl::Close(tx));
            let sent = if let Some(deadline) = deadline {
                let rest = deadline.saturating_duration_since(Instant::now());
                if rest.is_zero() {
                    break;
                }
                tokio::time::timeout(rest, send).await.ok()
            } else {
                Some(send.await)
            };
            if sent.is_some() {
                rxs.push(rx);
            }
        }

        for rx in rxs {
            if let Some(deadline) = deadline {
                let rest = deadline.saturating_duration_since(Instant::now());
                if rest.is_zero() {
                    break;
                }
                drop(tokio::time::timeout(rest, rx).await);
            } else {
                drop(rx.await);
            }
        }

        for w in &workers {
            w.task.abort();
        }
        self.inner.shared.nodes.lock().clear();
        self.inner.shared.fast.write().clear();

        if let Some(h) = self.inner.shared.meta_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.inner.shared.connect_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.inner.shared.retry_task.lock().take() {
            h.abort();
        }
        self.inner.shared.clear_sticky();
    }

    /// Partition metadata for `topic` (Java `partitionsFor`: leader, replicas, ISR, offline replicas, leader epoch).
    ///
    /// Waits up to [`ProducerConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::partitions_for_timeout`].
    pub async fn partitions_for(
        &self,
        topic: impl Into<String>,
    ) -> Result<Vec<crate::PartitionInfo>> {
        let timeout = self.inner.shared.cfg.request_timeout;
        self.partitions_for_timeout(topic, timeout).await
    }

    /// [`Self::partitions_for`] with a one-shot timeout (Java `partitionsFor(String, Duration)`).
    pub async fn partitions_for_timeout(
        &self,
        topic: impl Into<String>,
        timeout: Duration,
    ) -> Result<Vec<crate::PartitionInfo>> {
        let topic = topic.into();
        let mut conn = self.inner.shared.meta.lock().await;
        let version = self.inner.shared.metadata_version;
        let allow = self.inner.shared.cfg.allow_auto_topic_creation;
        let body = conn
            .roundtrip(
                METADATA,
                version,
                |buf| {
                    encode_metadata_request(buf, version, Some(std::slice::from_ref(&topic)), allow)
                },
                timeout,
            )
            .await?;
        drop(conn);
        let mut body = body;
        decode_shared_metadata(&self.inner.shared, &mut body, version)?;
        let infos = {
            let image = self.inner.shared.metadata_image.lock();
            crate::consumer::partition_infos_from(&image, Some(topic.as_str()))?
        };
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

async fn open_conn(addr: &str, cfg: &ProducerConfig, transaction_v2: bool) -> Result<BrokerConn> {
    Ok(open_conn_with_versions(addr, cfg, transaction_v2).await?.0)
}

async fn open_conn_with_versions(
    addr: &str,
    cfg: &ProducerConfig,
    transaction_v2: bool,
) -> Result<(BrokerConn, ApiVersionsResponse)> {
    let deadline = Instant::now()
        .checked_add(cfg.request_timeout)
        .ok_or(Error::Timeout)?;
    let mut conn = BrokerConn::connect_tls(
        addr,
        &cfg.client_id,
        cfg.connect_timeout.min(timeout_remaining(deadline)?),
        cfg.tls.as_ref(),
    )
    .await?;
    let versions_resp =
        crate::protocol::api::negotiate_api_versions(&mut conn, timeout_remaining(deadline)?)
            .await?;
    // Topic identity requires Metadata10+, and transactionV1 must retain the
    // v11 ceiling: v12/13 imply transactionV2 on the partition leader.
    // A supported EndTxn range alone does not enable the finalized feature.
    let mut produce_max = if versions_resp
        .api_version(METADATA)
        .is_some_and(|v| v.max_version >= 10)
    {
        13
    } else {
        12
    };
    if cfg.transactional_id.is_some()
        && (!transaction_v2
            || versions_resp
                .api_version(END_TXN)
                .is_none_or(|v| v.max_version < 5))
    {
        produce_max = ProduceRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2;
    }
    if let Some(pv) = versions_resp
        .api_version(PRODUCE)
        .and_then(|v| pick_version(v.min_version, v.max_version, 3, produce_max))
    {
        conn.set_produce_version(pv);
    }
    crate::protocol::sasl::apply_api_keys(&mut conn, &versions_resp.api_keys);
    crate::protocol::sasl::authenticate(
        &mut conn,
        cfg.sasl_plain.as_ref(),
        cfg.sasl_scram.as_ref(),
        cfg.sasl_scram_sha512.as_ref(),
        cfg.sasl_oauthbearer.as_deref(),
        cfg.sasl_oauthbearer_oidc.as_ref(),
        timeout_remaining(deadline)?,
    )
    .await?;
    Ok((conn, versions_resp))
}

async fn discover_typed_coord(
    cfg: &ProducerConfig,
    key: &str,
    key_type: i8,
    version: i16,
    deadline: Instant,
) -> Result<CoordinatorConnection> {
    let mut attempt = 0u32;
    loop {
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        let result = discover_typed_coord_once(cfg, key, key_type, version, deadline).await;
        match result {
            Err(error)
                if error
                    .broker_code()
                    .is_some_and(error::coordinator_retriable) =>
            {
                crate::config::sleep_retry_backoff(
                    cfg.retry_backoff.max(Duration::from_millis(1)),
                    cfg.retry_backoff_max.max(Duration::from_millis(1)),
                    attempt,
                    deadline,
                )
                .await;
                attempt = attempt.saturating_add(1);
            }
            result => return result,
        }
    }
}

async fn discover_typed_coord_once(
    cfg: &ProducerConfig,
    key: &str,
    key_type: i8,
    version: i16,
    deadline: Instant,
) -> Result<CoordinatorConnection> {
    if cfg.bootstrap.len() > 1 {
        return discover_typed_coord_raced(cfg, key, key_type, version, deadline).await;
    }
    let mut last = Error::protocol("find coordinator failed");
    for addr in &cfg.bootstrap {
        let bounded = bounded_producer_config(cfg, deadline)?;
        let (hop, hop_versions) = match open_conn_with_versions(addr, &bounded, false).await {
            Ok(c) => c,
            Err(e) => {
                last = e;
                continue;
            }
        };
        match find_producer_coordinator(cfg, hop, hop_versions, key, key_type, version, deadline)
            .await
        {
            HopFind::Found(found) => return Ok(*found),
            HopFind::Continue(error) => last = error,
            HopFind::Stop(error) => return Err(error),
        }
    }
    Err(last)
}

type ProducerHandshake = (BrokerConn, ApiVersionsResponse);

enum HopFind {
    Found(Box<CoordinatorConnection>),
    Continue(Error),
    Stop(Error),
}

enum ProducerCoordAsk {
    Found(Box<CoordinatorConnection>),
    Fatal(Error),
    Retry(Error),
}

async fn discover_typed_coord_raced(
    cfg: &ProducerConfig,
    key: &str,
    key_type: i8,
    version: i16,
    deadline: Instant,
) -> Result<CoordinatorConnection> {
    let mut inflight = tokio::task::JoinSet::<(usize, Result<ProducerHandshake>)>::new();
    for (index, addr) in cfg.bootstrap.iter().enumerate() {
        let cfg = cfg.clone();
        let addr = addr.clone();
        let _ = inflight.spawn(async move {
            let result = match bounded_producer_config(&cfg, deadline) {
                Ok(bounded) => open_conn_with_versions(&addr, &bounded, false).await,
                Err(error) => Err(error),
            };
            (index, result)
        });
    }
    let mut ready: Vec<Option<Result<ProducerHandshake>>> = Vec::new();
    ready.resize_with(inflight.len(), || None);
    let mut last = Error::protocol("find coordinator failed");
    while !inflight.is_empty() {
        if Instant::now() >= deadline {
            inflight.abort_all();
            return Err(Error::Timeout);
        }
        let rest = timeout_remaining(deadline).unwrap_or(Duration::ZERO);
        let joined = tokio::select! {
            joined = inflight.join_next() => joined,
            () = tokio::time::sleep(rest) => {
                inflight.abort_all();
                return Err(Error::Timeout);
            }
        };
        let Some(Ok((index, result))) = joined else {
            break;
        };
        if let Some(slot) = ready.get_mut(index) {
            *slot = Some(result);
        }
        // Handshakes that finish together are gathered so an earlier
        // terminal FindCoordinator error is seen before a later broker
        // is asked. A handshake that is still running does not block a
        // later broker that is already up.
        let gather = timeout_remaining(deadline)
            .unwrap_or(Duration::ZERO)
            .min(Duration::from_millis(5));
        let gather_done = tokio::time::sleep(gather);
        tokio::pin!(gather_done);
        loop {
            tokio::select! {
                biased;
                joined = inflight.join_next(), if !inflight.is_empty() => {
                    if let Some(Ok((index, result))) = joined {
                        if let Some(slot) = ready.get_mut(index) {
                            *slot = Some(result);
                        }
                    }
                }
                () = &mut gather_done => break,
            }
        }
        let mut cursor = 0usize;
        while cursor < ready.len() {
            match ready.get(cursor).and_then(|slot| slot.as_ref()) {
                None => {
                    let later =
                        ready
                            .iter()
                            .enumerate()
                            .skip(cursor + 1)
                            .find_map(|(index, slot)| match slot {
                                Some(Ok(_)) => Some(index),
                                _ => None,
                            });
                    let Some(index) = later else {
                        break;
                    };
                    match ask_ready_producer_coordinator(
                        cfg, key, key_type, version, deadline, &mut ready, index,
                    )
                    .await
                    {
                        ProducerCoordAsk::Found(found) => {
                            inflight.abort_all();
                            return Ok(*found);
                        }
                        ProducerCoordAsk::Fatal(error) => {
                            inflight.abort_all();
                            return Err(error);
                        }
                        ProducerCoordAsk::Retry(error) => last = error,
                    }
                    break;
                }
                Some(_) => {
                    match ask_ready_producer_coordinator(
                        cfg, key, key_type, version, deadline, &mut ready, cursor,
                    )
                    .await
                    {
                        ProducerCoordAsk::Found(found) => {
                            inflight.abort_all();
                            return Ok(*found);
                        }
                        ProducerCoordAsk::Fatal(error) => {
                            inflight.abort_all();
                            return Err(error);
                        }
                        ProducerCoordAsk::Retry(error) => {
                            last = error;
                            cursor = cursor.saturating_add(1);
                        }
                    }
                }
            }
        }
    }
    inflight.abort_all();
    if Instant::now() >= deadline {
        return Err(Error::Timeout);
    }
    Err(last)
}

async fn ask_ready_producer_coordinator(
    cfg: &ProducerConfig,
    key: &str,
    key_type: i8,
    version: i16,
    deadline: Instant,
    ready: &mut [Option<Result<ProducerHandshake>>],
    index: usize,
) -> ProducerCoordAsk {
    let Some(slot) = ready.get_mut(index).and_then(Option::take) else {
        return ProducerCoordAsk::Retry(Error::protocol("missing coordinator handshake"));
    };
    let (hop, versions) = match slot {
        Ok(handshake) => handshake,
        Err(error) => return ProducerCoordAsk::Retry(error),
    };
    match find_producer_coordinator(cfg, hop, versions, key, key_type, version, deadline).await {
        HopFind::Found(found) => ProducerCoordAsk::Found(found),
        HopFind::Stop(error) => ProducerCoordAsk::Fatal(error),
        HopFind::Continue(error) => ProducerCoordAsk::Retry(error),
    }
}

async fn find_producer_coordinator(
    cfg: &ProducerConfig,
    mut hop: BrokerConn,
    hop_versions: ApiVersionsResponse,
    key: &str,
    key_type: i8,
    version: i16,
    deadline: Instant,
) -> HopFind {
    let rest = match timeout_remaining(deadline) {
        Ok(rest) => rest,
        Err(error) => return HopFind::Stop(error),
    };
    let body = match hop
        .roundtrip(
            FIND_COORDINATOR,
            version,
            |buf| encode_find_coordinator_request_typed(buf, version, key, key_type),
            rest,
        )
        .await
    {
        Ok(body) => body,
        Err(error) => return HopFind::Continue(error),
    };
    let (err, _node, host, port) =
        match decode_find_coordinator_response(&mut body.clone(), version) {
            Ok(decoded) => decoded,
            Err(error) => return HopFind::Stop(error),
        };
    if err != 0 {
        let error = Error::broker(err, "FindCoordinator");
        if error::coordinator_retriable(err) {
            return HopFind::Continue(error);
        }
        return HopFind::Stop(error);
    }
    let coord_addr = crate::net::format_address(&host, port);
    if coord_addr == hop.addr() {
        return HopFind::Found(Box::new(coordinator_connection(hop, &hop_versions)));
    }
    let bounded = match bounded_producer_config(cfg, deadline) {
        Ok(bounded) => bounded,
        Err(error) => return HopFind::Stop(error),
    };
    match open_conn_with_versions(&coord_addr, &bounded, false).await {
        Ok((conn, versions)) => HopFind::Found(Box::new(coordinator_connection(conn, &versions))),
        Err(error) => HopFind::Stop(error),
    }
}

fn coordinator_connection(
    conn: BrokerConn,
    versions: &ApiVersionsResponse,
) -> CoordinatorConnection {
    let init_producer_id_version = versions
        .api_version(INIT_PRODUCER_ID)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 6));
    CoordinatorConnection {
        conn,
        init_producer_id_version,
    }
}

fn required_find_coordinator(version: Option<i16>) -> Result<i16> {
    version.filter(|version| *version >= 1).ok_or_else(|| {
        Error::Unsupported("transaction discovery requires FindCoordinator v1-6".into())
    })
}

async fn init_producer_id_roundtrip(
    cfg: &ProducerConfig,
    txn: &mut Option<CoordinatorConnection>,
    mut meta: Option<&mut BrokerConn>,
    version: i16,
    find_coord_version: Option<i16>,
    identity: (i64, i16),
    deadline: Instant,
) -> Result<(Bytes, i16)> {
    let txn_id = cfg.transactional_id.clone();
    let txn_timeout_ms = i32::try_from(cfg.transaction_timeout.as_millis())
        .unwrap_or(i32::MAX)
        .max(0);
    let (producer_id, producer_epoch) = identity;
    let mut attempt = 0u32;
    let mut rediscover = false;
    let mut transport_retry_used = false;
    loop {
        if rediscover {
            let tid = txn_id
                .as_deref()
                .ok_or_else(|| Error::protocol("no transactional id"))?;
            let new = discover_within(
                cfg,
                tid,
                COORDINATOR_TRANSACTION,
                required_find_coordinator(find_coord_version)?,
                deadline,
            )
            .await?;
            *txn = Some(new);
        }
        let timeout = timeout_remaining(deadline)?;
        let (conn, version) = match txn.as_mut() {
            Some(coord) => (
                &mut coord.conn,
                coord.init_producer_id_version.ok_or_else(|| {
                    Error::Unsupported("coordinator does not support InitProducerId v0-6".into())
                })?,
            ),
            None => (
                meta.as_deref_mut()
                    .ok_or_else(|| Error::protocol("missing initialization connection"))?,
                version,
            ),
        };
        if txn_id.is_some() && producer_id >= 0 && version < 3 {
            return Err(Error::Unsupported(
                "coordinator does not support InitProducerId epoch recovery v3+".into(),
            ));
        }
        let result = conn
            .roundtrip(
                INIT_PRODUCER_ID,
                version,
                |buf| {
                    encode_init_producer_id_request(
                        buf,
                        version,
                        txn_id.as_deref(),
                        txn_timeout_ms,
                        producer_id,
                        producer_epoch,
                    )
                },
                timeout,
            )
            .await;
        match result {
            Ok(body) => {
                let err = decode_init_producer_id_response(&mut body.clone(), version)?.0;
                if !error::coordinator_retriable(err) {
                    return Ok((body, version));
                }
                // Loading can recover on the current connection. Missing/moved
                // transactional coordinators require rediscovery before retry.
                rediscover = txn_id.is_some() && err != error::COORDINATOR_LOAD_IN_PROGRESS;
            }
            // Preserve the existing one transport retry for transactions. Typed
            // startup retries do not replay application Produce requests.
            Err(e)
                if txn_id.is_some()
                    && !transport_retry_used
                    && (e.is_retriable() || matches!(e, Error::Closed)) =>
            {
                transport_retry_used = true;
                rediscover = true;
            }
            Err(e) => return Err(e),
        }
        crate::config::sleep_retry_backoff(
            cfg.retry_backoff.max(Duration::from_millis(1)),
            cfg.retry_backoff_max.max(Duration::from_millis(1)),
            attempt,
            deadline,
        )
        .await;
        attempt = attempt.saturating_add(1);
    }
}

/// KIP-360 epoch bump: InitProducerId with the last producer id and epoch.
/// Skipped when InitProducerId is below v3 or EndTxn v5 already bumped.
async fn bump_producer_epoch(shared: &Shared) -> Result<()> {
    let version = shared.init_producer_id_version;
    if version < 3
        || shared.end_txn_version > EndTxnRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
    {
        return Ok(());
    }
    let pid = shared.producer_id.load(Ordering::SeqCst);
    let epoch = shared.producer_epoch.load(Ordering::SeqCst);
    if pid < 0 {
        return Ok(());
    }
    let deadline = Instant::now()
        .checked_add(shared.cfg.request_timeout)
        .ok_or(Error::Timeout)?;
    let mut txn = shared.txn.lock().await;
    let (body, version) = init_producer_id_roundtrip(
        &shared.cfg,
        &mut txn,
        None,
        version,
        shared.find_coord_version,
        (pid, epoch),
        deadline,
    )
    .await?;
    let (err, new_pid, new_epoch, ..) =
        decode_init_producer_id_response(&mut body.clone(), version)?;
    if err != 0 {
        return Err(Error::broker(err, "InitProducerId"));
    }
    if new_pid < 0 {
        return Err(Error::protocol("InitProducerId returned producer_id=-1"));
    }
    if new_pid != pid || new_epoch != epoch {
        shared.seqs.lock().clear();
        let _ = shared.epoch_gen.fetch_add(1, Ordering::SeqCst);
    }
    shared.producer_id.store(new_pid, Ordering::SeqCst);
    shared.producer_epoch.store(new_epoch, Ordering::SeqCst);
    Ok(())
}

/// Nontransactional epoch-exhaustion recovery (KL03-09): at `i16::MAX` no
/// local epoch bump is possible, so request a brand-new producer identity
/// with `InitProducerId(-1, -1)` and restart sequences at zero under it.
/// This mirrors the pinned Java `TransactionManager` producer-ID reset rule;
/// the transactional KIP-360 bump above is a separate path and is untouched.
async fn renew_nontransactional_identity(shared: &Shared) -> Result<()> {
    let version = shared.init_producer_id_version;
    let deadline = Instant::now()
        .checked_add(shared.cfg.request_timeout)
        .ok_or(Error::Timeout)?;
    let mut txn: Option<CoordinatorConnection> = None;
    let mut meta = shared.meta.lock().await;
    let (body, version) = init_producer_id_roundtrip(
        &shared.cfg,
        &mut txn,
        Some(&mut meta),
        version,
        shared.find_coord_version,
        (RecordBatch::NO_PRODUCER_ID, RecordBatch::NO_PRODUCER_EPOCH),
        deadline,
    )
    .await?;
    let (err, new_pid, new_epoch, ..) =
        decode_init_producer_id_response(&mut body.clone(), version)?;
    if err != 0 {
        return Err(Error::broker(err, "InitProducerId"));
    }
    if new_pid < 0 {
        return Err(Error::protocol("InitProducerId returned producer_id=-1"));
    }
    shared.seqs.lock().clear();
    shared.producer_id.store(new_pid, Ordering::SeqCst);
    shared.producer_epoch.store(new_epoch, Ordering::SeqCst);
    let _ = shared.epoch_gen.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

/// Bounded Java `reenqueue` for transactional RPCs: backoff between retries
/// of a retriable broker code. Retries never invent identities: every
/// attempt reuses the broker-authorized producer id and epoch.
async fn txn_retry_sleep(shared: &Shared, attempt: u32, deadline: Instant) {
    crate::config::sleep_retry_backoff(
        shared.cfg.retry_backoff,
        shared.cfg.retry_backoff_max,
        attempt,
        deadline,
    )
    .await;
}

fn timeout_remaining(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(Error::Timeout)
    } else {
        Ok(remaining)
    }
}

fn bounded_producer_config(cfg: &ProducerConfig, deadline: Instant) -> Result<ProducerConfig> {
    let remaining = timeout_remaining(deadline)?;
    let mut bounded = cfg.clone();
    bounded.request_timeout = remaining;
    bounded.connect_timeout = cfg.connect_timeout.min(remaining);
    Ok(bounded)
}

async fn discover_within(
    cfg: &ProducerConfig,
    key: &str,
    key_type: i8,
    version: i16,
    deadline: Instant,
) -> Result<CoordinatorConnection> {
    discover_typed_coord(cfg, key, key_type, version, deadline).await
}

async fn txn_roundtrip(
    shared: &Shared,
    api_key: i16,
    api_version: i16,
    encode_body: impl Fn(&mut BytesMut) -> Result<()>,
    request_timeout: Duration,
    error_of: impl Fn(&[u8]) -> Result<i16>,
) -> Result<Bytes> {
    let deadline = Instant::now() + request_timeout;
    let tid = shared
        .cfg
        .transactional_id
        .clone()
        .ok_or_else(reject_java_no_transaction_manager)?;
    let first = {
        let remaining = timeout_remaining(deadline)?;
        let mut guard = shared.txn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| Error::protocol("no transaction coordinator"))?;
        conn.conn
            .roundtrip(api_key, api_version, |buf| encode_body(buf), remaining)
            .await
    };
    match first {
        Ok(body) if !error::coordinator_retriable(error_of(&body)?) => return Ok(body),
        Ok(_) => {}
        // `Closed` is a dead socket, not a dead producer: rediscover and
        // retry once like any other transport failure (KL03-10). Without
        // this, one broker-side close bricks the coordinator channel.
        Err(e) if e.is_retriable() || matches!(e, Error::Closed) => {}
        Err(e) => return Err(e),
    }
    let new = discover_within(
        &shared.cfg,
        &tid,
        COORDINATOR_TRANSACTION,
        required_find_coordinator(shared.find_coord_version)?,
        deadline,
    )
    .await?;
    let remaining = timeout_remaining(deadline)?;
    let mut guard = shared.txn.lock().await;
    *guard = Some(new);
    let conn = guard
        .as_mut()
        .ok_or_else(|| Error::protocol("no transaction coordinator"))?;
    conn.conn
        .roundtrip(api_key, api_version, |buf| encode_body(buf), remaining)
        .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "group coord roundtrip is one wire call plus rediscovery identity"
)]
async fn group_coord_roundtrip(
    cfg: &ProducerConfig,
    group_id: &str,
    api_key: i16,
    api_version: i16,
    find_coord_version: Option<i16>,
    encode_body: impl Fn(&mut BytesMut) -> Result<()>,
    request_timeout: Duration,
    error_of: impl Fn(&[u8]) -> Result<i16>,
) -> Result<Bytes> {
    let deadline = Instant::now() + request_timeout;
    let find_coord_version = required_find_coordinator(find_coord_version)?;
    let mut coord = discover_within(
        cfg,
        group_id,
        COORDINATOR_GROUP,
        find_coord_version,
        deadline,
    )
    .await?;
    let body = coord
        .conn
        .roundtrip(
            api_key,
            api_version,
            |buf| encode_body(buf),
            timeout_remaining(deadline)?,
        )
        .await?;
    if !error::coordinator_retriable(error_of(&body)?) {
        return Ok(body);
    }
    coord = discover_within(
        cfg,
        group_id,
        COORDINATOR_GROUP,
        find_coord_version,
        deadline,
    )
    .await?;
    coord
        .conn
        .roundtrip(
            api_key,
            api_version,
            |buf| encode_body(buf),
            timeout_remaining(deadline)?,
        )
        .await
}

fn decode_shared_metadata(shared: &Shared, body: &mut bytes::Bytes, version: i16) -> Result<()> {
    let mut image = shared.metadata_image.lock();
    decode_metadata_response_into(body, version, &mut image)?;
    image.check()?;
    shared.cluster.lock().apply(&image, version);
    Ok(())
}

fn topic_needs_metadata(shared: &Shared, topic: &Arc<str>) -> bool {
    if shared.cfg.metadata_max_age.is_zero() || peek_meta_err(shared).is_some() {
        return false;
    }
    let cluster = shared.cluster.lock();
    !(cluster.partition_count(topic.as_ref()).is_some()
        && cluster.topic_fresh(topic.as_ref(), shared.cfg.metadata_max_age))
}

fn prepared_topic_error(failed: &[(Arc<str>, Error)], topic: &Arc<str>) -> Option<Error> {
    failed
        .iter()
        .find(|(name, _)| name.as_ref() == topic.as_ref())
        .map(|(_, error)| clone_err(error))
}

/// Broker error for a topic that a shared metadata response did not make fresh.
fn unfresh_topic_error(shared: &Shared, topic: &Arc<str>) -> Option<Error> {
    let ready = {
        let cluster = shared.cluster.lock();
        cluster.partition_count(topic.as_ref()).is_some()
            && cluster.topic_fresh(topic.as_ref(), shared.cfg.metadata_max_age)
    };
    if ready {
        return None;
    }
    let image = shared.metadata_image.lock();
    match image
        .topics
        .iter()
        .find(|item| item.name.as_deref() == Some(topic.as_ref()))
    {
        Some(found) if found.error_code != 0 => {
            Some(Error::broker(found.error_code, topic.to_string()))
        }
        Some(found) if !found.partitions.is_empty() => None,
        _ => Some(Error::UnknownTopic(topic.to_string())),
    }
}

/// Refresh every stale topic that was already waiting.
///
/// `None` means each topic was still fresh, so a previous metadata error
/// stays published. A later topic's broker error replaces an earlier one,
/// matching a queue that refreshed those topics one after another.
async fn refresh_queued_topics(shared: &Shared, topics: &[Arc<str>]) -> Option<Result<()>> {
    let requested = match refresh_topics_within(shared, topics, shared.cfg.request_timeout).await {
        Ok(names) if names.is_empty() => return None,
        Ok(names) => names,
        Err(error) => return Some(Err(error)),
    };
    let mut last = Ok(());
    {
        let image = shared.metadata_image.lock();
        for topic in &requested {
            let Some(found) = image
                .topics
                .iter()
                .find(|item| item.name.as_deref() == Some(topic.as_ref()))
            else {
                last = Err(Error::UnknownTopic(topic.to_string()));
                continue;
            };
            if found.error_code != 0 {
                last = Err(Error::broker(found.error_code, topic.to_string()));
                continue;
            }
            if found.partitions.is_empty() {
                last = Err(Error::UnknownTopic(topic.to_string()));
                continue;
            }
            last = Ok(());
        }
    }
    Some(last)
}

/// Fetch every stale name in `topics` with one Metadata request.
///
/// The returned names are the topics that were requested. An empty vector
/// means each topic was already fresh. Per-topic broker errors stay in the
/// metadata image; only timeout, transport, and decode failures return `Err`.
async fn refresh_topics_within(
    shared: &Shared,
    topics: &[Arc<str>],
    timeout: Duration,
) -> Result<Vec<Arc<str>>> {
    let mut requested = Vec::new();
    for topic in topics {
        let fresh = {
            let cluster = shared.cluster.lock();
            cluster.partition_count(topic.as_ref()).is_some()
                && cluster.topic_fresh(topic.as_ref(), shared.cfg.metadata_max_age)
        };
        if fresh
            || requested
                .iter()
                .any(|name: &Arc<str>| name.as_ref() == topic.as_ref())
        {
            continue;
        }
        requested.push(Arc::clone(topic));
    }
    if requested.is_empty() {
        return Ok(requested);
    }
    if timeout.is_zero() {
        return Err(Error::Timeout);
    }
    let deadline = crate::net::Deadline::from_timeout(timeout);
    let mut conn = match tokio::time::timeout(timeout, shared.meta.lock()).await {
        Ok(guard) => guard,
        Err(_) => return Err(Error::Timeout),
    };
    let version = shared.metadata_version;
    let allow = shared.cfg.allow_auto_topic_creation;
    let names: Vec<&str> = requested.iter().map(|topic| topic.as_ref()).collect();
    let body = conn
        .roundtrip_deadline(
            METADATA,
            version,
            |buf| encode_metadata_request_names(buf, version, Some(&names), allow),
            deadline,
        )
        .await?;
    drop(conn);
    let mut body = body;
    decode_shared_metadata(shared, &mut body, version)?;
    let mut succeeded = Vec::new();
    {
        let image = shared.metadata_image.lock();
        for topic in &requested {
            let Some(found) = image
                .topics
                .iter()
                .find(|item| item.name.as_deref() == Some(topic.as_ref()))
            else {
                continue;
            };
            if found.error_code != 0 || found.partitions.is_empty() {
                continue;
            }
            succeeded.push(topic.as_ref());
        }
    }
    for topic in succeeded {
        drop_fast_topic(shared, topic);
    }
    Ok(requested)
}

async fn partitions_for_timeout(
    shared: &Shared,
    topic: &Arc<str>,
    timeout: Duration,
) -> Result<i32> {
    // Scope the guard: the freshness check must release the non-reentrant
    // cluster mutex before any downstream work (edition 2021 temporary
    // scope would otherwise hold it through the body).
    let cached = {
        let cluster = shared.cluster.lock();
        match cluster.partition_count(topic) {
            Some(n) if cluster.topic_fresh(topic, shared.cfg.metadata_max_age) => Some(n),
            _ => None,
        }
    };
    if let Some(n) = cached {
        return Ok(n);
    }
    if timeout.is_zero() {
        return Err(Error::Timeout);
    }
    let deadline = crate::net::Deadline::from_timeout(timeout);
    let mut conn = match tokio::time::timeout(timeout, shared.meta.lock()).await {
        Ok(guard) => guard,
        Err(_) => return Err(Error::Timeout),
    };
    let version = shared.metadata_version;
    let allow = shared.cfg.allow_auto_topic_creation;
    let names = [topic.as_ref()];
    let body = conn
        .roundtrip_deadline(
            METADATA,
            version,
            |buf| encode_metadata_request_names(buf, version, Some(&names), allow),
            deadline,
        )
        .await?;
    drop(conn);
    let mut body = body;
    decode_shared_metadata(shared, &mut body, version)?;
    let n = partition_count_from_image(shared, topic.as_ref())?;
    drop_fast_topic(shared, topic);
    Ok(n)
}

fn fresh_partition_count(shared: &Shared, topic: &str) -> Option<i32> {
    let cluster = shared.cluster.lock();
    match cluster.partition_count(topic) {
        Some(n) if cluster.topic_fresh(topic, shared.cfg.metadata_max_age) => Some(n),
        _ => None,
    }
}

fn partition_count_from_image(shared: &Shared, topic: &str) -> Result<i32> {
    let image = shared.metadata_image.lock();
    let found = image
        .topics
        .iter()
        .find(|item| item.name.as_deref() == Some(topic))
        .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
    if found.error_code != 0 {
        return Err(Error::broker(found.error_code, topic.to_string()));
    }
    let n = crate::protocol::buf::i32_from_usize(found.partitions.len())?;
    if n <= 0 {
        return Err(Error::UnknownTopic(topic.to_string()));
    }
    Ok(n)
}

fn drop_fast_topic(shared: &Shared, topic: &str) {
    let mut fast = shared.fast.write();
    if let Some(pos) = fast.order.iter().position(|name| name.as_ref() == topic) {
        let _ = fast.order.remove(pos);
    }
    let _ = fast.routes.remove(topic);
}

fn invalidate_cached_topic(shared: &Shared, topic: &str) {
    shared.cluster.lock().invalidate_topic(topic);
    drop_fast_topic(shared, topic);
}

fn try_nudge_slot(tx: &mpsc::Sender<(i32, usize)>, node: i32, slot: usize) {
    tx.try_send((node, slot)).unwrap_or(());
}

/// KL09-25: drop a dead node's slots and re-nudge exactly the slots that
/// were live, so a reconnect restores the previously demanded set and no more.
fn remove_node_nudge_live(shared: &Shared, node: i32) {
    let live: Vec<usize> = shared
        .nodes
        .lock()
        .remove(&node)
        .map(|slots| {
            slots
                .iter()
                .enumerate()
                .filter_map(|(i, s)| s.as_ref().map(|_| i))
                .collect()
        })
        .unwrap_or_default();
    for slot in live {
        try_nudge_slot(&shared.connect_tx, node, slot);
    }
}

async fn connect_loop(
    weak: std::sync::Weak<Shared>,
    mut rx: mpsc::Receiver<(i32, usize)>,
    cap: usize,
) {
    let mut inflight = tokio::task::JoinSet::<(i32, usize, Result<WorkerHandle>)>::new();
    let mut rx_open = true;
    loop {
        if !rx_open && inflight.is_empty() {
            break;
        }
        tokio::select! {
            biased;
            joined = inflight.join_next(), if !inflight.is_empty() => {
                let Some(joined) = joined else {
                    continue;
                };
                let Ok((node, slot, result)) = joined else {
                    continue;
                };
                let Some(shared) = weak.upgrade() else {
                    inflight.abort_all();
                    break;
                };
                finish_slot_connect(&shared, &weak, node, slot, result);
            }
            incoming = rx.recv(), if rx_open && inflight.len() < 8 => {
                let Some((node, slot)) = incoming else {
                    rx_open = false;
                    continue;
                };
                let Some(shared) = weak.upgrade() else {
                    inflight.abort_all();
                    break;
                };
                if shared.closed.load(Ordering::SeqCst) {
                    inflight.abort_all();
                    break;
                }
                let Some(addr) = prepare_slot_dial(&shared, node, slot) else {
                    continue;
                };
                let shared = Arc::clone(&shared);
                let _ = inflight.spawn(async move {
                    let result = spawn_slot_worker(&shared, node, &addr, cap).await;
                    (node, slot, result)
                });
            }
        }
    }
}

fn prepare_slot_dial(shared: &Shared, node: i32, slot: usize) -> Option<String> {
    let n_conn = shared.cfg.connections.max(1);
    if slot >= n_conn {
        return None;
    }
    if shared
        .nodes
        .lock()
        .get(&node)
        .and_then(|slots| slots.get(slot))
        .is_some_and(|slot| slot.is_some())
    {
        shared.cache_nudge.notify_waiters();
        return None;
    }
    let addr = shared.cluster.lock().brokers.get(&node).cloned()?;
    {
        let mut busy = shared.reconnect_busy.lock();
        if !busy.insert((node, slot)) {
            return None;
        }
    }
    {
        let mut nodes = shared.nodes.lock();
        let _ = nodes.entry(node).or_insert_with(|| vec![None; n_conn]);
    }
    Some(addr)
}

fn finish_slot_connect(
    shared: &Shared,
    weak: &std::sync::Weak<Shared>,
    node: i32,
    slot: usize,
    result: Result<WorkerHandle>,
) {
    match result {
        Ok(handle) => {
            let _ = shared.reconnect_busy.lock().remove(&(node, slot));
            let _ = shared.reconnect_fails.lock().remove(&node);
            if let Some(slots) = shared.nodes.lock().get_mut(&node) {
                if let Some(s) = slots.get_mut(slot) {
                    *s = Some(handle);
                }
            }
            publish_meta_err(shared, None);
            shared.cache_nudge.notify_waiters();
        }
        Err(e) => {
            // KL09-25: the entry stays (other slots may be live); only
            // this slot remains vacant until the backoff re-nudge.
            let fails =
                crate::config::bump_reconnect_fails(&mut shared.reconnect_fails.lock(), node);
            if e.is_retriable() {
                let delay = crate::config::reconnect_backoff_delay(
                    shared.cfg.reconnect_backoff,
                    shared.cfg.reconnect_backoff_max,
                    fails,
                );
                let weak = weak.clone();
                drop(tokio::spawn(async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    if let Some(shared) = weak.upgrade() {
                        let _ = shared.reconnect_busy.lock().remove(&(node, slot));
                        try_nudge_slot(&shared.connect_tx, node, slot);
                    }
                }));
            } else {
                let _ = shared.reconnect_busy.lock().remove(&(node, slot));
                publish_meta_err(shared, Some(clone_err(&e)));
                shared.cache_nudge.notify_waiters();
            }
        }
    }
}

/// KL09-25: open one connection slot on demand instead of all `n_conn`
/// workers up front, so a 1-partition producer holds 1 worker socket.
async fn spawn_slot_worker(
    shared: &Arc<Shared>,
    node: i32,
    addr: &str,
    cap: usize,
) -> Result<WorkerHandle> {
    if shared.closed.load(Ordering::SeqCst) {
        return Err(Error::Closed);
    }
    let conn = open_conn(addr, &shared.cfg, shared.transaction_v2).await?;
    if conn.produce_version() < 0 {
        return Err(Error::Unsupported(format!(
            "broker at {addr} does not support usable Produce v3-13"
        )));
    }
    if shared.closed.load(Ordering::SeqCst) {
        return Err(Error::Closed);
    }
    let (data_tx, data_rx) = mpsc::channel(cap);
    let (ctrl_tx, ctrl_rx) = mpsc::channel(16);
    // KL09-33: compress scratch caps track the effective batch byte cap so
    // retained buffers stay proportional to the batches being encoded.
    let batch_cap = shared.cfg.produce_batch_bytes();
    let batch_cap = if batch_cap == 0 {
        1024 * 1024
    } else {
        batch_cap
    };
    let id = Arc::new(());
    let worker = Worker {
        id: Arc::clone(&id),
        node_id: node,
        throttle: shared.broker_throttle(node),
        conn,
        data: data_rx,
        ctrl: ctrl_rx,
        shared: shared.clone(),
        write_buf: BytesMut::with_capacity(2 * 1024 * 1024),
        pending: Vec::with_capacity(shared.cfg.batch_records.min(8192)),
        spare_batch: Vec::new(),
        pending_estimate: 0,
        earliest_deadline: None,
        in_flight: VecDeque::new(),
        fail: None,
        compress_scratch: CompressScratch::with_caps(batch_cap, batch_cap),
        group_scratch: ProduceGroupScratch::default(),
        ack_index: AckIndex::default(),
        ack_slots: Vec::new(),
        spare_acks: Vec::new(),
        ack_requested: HashMap::new(),
        ack_seen: HashMap::new(),
        ack_tracker: None,
    };
    let handle = tokio::spawn(worker.run());
    Ok(WorkerHandle {
        id,
        data: data_tx,
        ctrl: ctrl_tx,
        task: Arc::new(handle),
    })
}

async fn retry_loop(weak: std::sync::Weak<Shared>, mut rx: mpsc::Receiver<Pending>) {
    struct RxDrain<'a> {
        weak: &'a std::sync::Weak<Shared>,
        rx: &'a mut mpsc::Receiver<Pending>,
    }
    impl Drop for RxDrain<'_> {
        fn drop(&mut self) {
            if let Some(shared) = self.weak.upgrade() {
                while let Ok(p) = self.rx.try_recv() {
                    let _ = shared.retries_out.fetch_sub(1, Ordering::SeqCst);
                    fail_pendings(&shared, vec![p], Error::Timeout);
                }
            }
        }
    }
    let drainer = RxDrain {
        weak: &weak,
        rx: &mut rx,
    };
    while let Some(p) = drainer.rx.recv().await {
        let Some(shared) = weak.upgrade() else {
            break;
        };
        let mut batch = vec![p];
        while let Ok(next) = drainer.rx.try_recv() {
            batch.push(next);
        }
        if shared.closed.load(Ordering::SeqCst) {
            let _ = shared.retries_out.fetch_sub(batch.len(), Ordering::SeqCst);
            fail_pendings(&shared, batch, Error::Timeout);
            shared.cache_nudge.notify_waiters();
            continue;
        }
        let mut topics = Vec::new();
        for pending in &batch {
            if pending.skip_meta_refresh {
                continue;
            }
            if topics
                .iter()
                .any(|topic: &Arc<str>| topic.as_ref() == pending.rec.topic.as_ref())
            {
                continue;
            }
            topics.push(Arc::clone(&pending.rec.topic));
        }
        let refreshed = if topics.is_empty() {
            true
        } else {
            for topic in &topics {
                invalidate_cached_topic(&shared, topic);
            }
            match refresh_queued_topics(&shared, &topics).await {
                None | Some(Ok(())) => true,
                Some(Err(error)) => {
                    publish_meta_err(&shared, Some(clone_err(&error)));
                    false
                }
            }
        };
        if refreshed {
            for pending in &mut batch {
                pending.skip_meta_refresh = true;
            }
            // The sockets start while each record still waits for retry_after.
            // Nothing is sent until that per-record deadline.
            nudge_retry_leaders(&shared, &batch);
        }
        for pending in batch {
            retry_one(&shared, pending).await;
            let _ = shared.retries_out.fetch_sub(1, Ordering::SeqCst);
            shared.cache_nudge.notify_waiters();
        }
    }
}

struct RetryGuard<'a> {
    shared: &'a Arc<Shared>,
    p: Option<Pending>,
}

impl Drop for RetryGuard<'_> {
    fn drop(&mut self) {
        if let Some(p) = self.p.take() {
            fail_pendings(self.shared, vec![p], Error::Timeout);
        }
    }
}

/// Open a leader socket for every retried partition whose leader is known.
fn nudge_retry_leaders(shared: &Shared, batch: &[Pending]) {
    if shared.sticky.is_some() {
        return;
    }
    let n_conn = shared.cfg.connections.max(1);
    let mut slots = HashSet::new();
    {
        let cluster = shared.cluster.lock();
        for pending in batch {
            let Some(part) = pending.rec.partition else {
                continue;
            };
            let Ok(node) = cluster.leader_id(pending.rec.topic.as_ref(), part) else {
                continue;
            };
            let slot = usize::try_from(part).unwrap_or(0) % n_conn;
            let _ = slots.insert((node, slot));
        }
    }
    for (node, slot) in slots {
        try_nudge_slot(&shared.connect_tx, node, slot);
    }
}

async fn retry_one(shared: &Arc<Shared>, p: Pending) {
    let mut guard = RetryGuard { shared, p: Some(p) };
    let Some(p) = guard.p.as_mut() else {
        return;
    };
    if shared.closed.load(Ordering::SeqCst) || Instant::now() >= p.deadline {
        return;
    }
    let now = Instant::now();
    if now < p.retry_after {
        let wait = p.retry_after.saturating_duration_since(now);
        let rest = p.deadline.saturating_duration_since(now);
        tokio::time::sleep(wait.min(rest)).await;
    }
    if shared.closed.load(Ordering::SeqCst) || Instant::now() >= p.deadline {
        return;
    }
    let skip_meta = p.skip_meta_refresh;
    p.skip_meta_refresh = false;
    let cached_node = if skip_meta {
        match p.rec.partition {
            Some(part) => shared
                .cluster
                .lock()
                .leader_id(p.rec.topic.as_ref(), part)
                .ok(),
            None => None,
        }
    } else {
        None
    };
    let need_meta = cached_node.is_none();
    if need_meta {
        invalidate_cached_topic(shared, p.rec.topic.as_ref());
        let rest = p.deadline.saturating_duration_since(Instant::now());
        if rest.is_zero() {
            return;
        }
        let meta_timeout = shared.cfg.request_timeout.min(rest);
        if partitions_for_timeout(shared, &p.rec.topic, meta_timeout)
            .await
            .is_err()
        {
            return;
        }
        if shared.closed.load(Ordering::SeqCst) {
            return;
        }
    }
    if shared.sticky.is_some() {
        retry_sticky_one(shared, &mut guard).await;
        return;
    }
    if p.rec.partition.is_none() {
        if let Some(np) = shared.cluster.lock().partition_count(p.rec.topic.as_ref()) {
            p.rec.partition = Some(pick_part(&p.rec, np, shared.partitioner.as_ref()));
        }
    }
    let Some(part) = p.rec.partition else {
        return;
    };
    let node = if let Some(node) = cached_node {
        node
    } else {
        let Ok(node) = shared.cluster.lock().leader_id(p.rec.topic.as_ref(), part) else {
            return;
        };
        node
    };
    let n_conn = shared.cfg.connections.max(1);
    let slot = usize::try_from(part).unwrap_or(0) % n_conn;
    try_nudge_slot(&shared.connect_tx, node, slot);
    let deadline = p.deadline;
    loop {
        if shared.closed.load(Ordering::SeqCst) || Instant::now() >= deadline {
            return;
        }
        let handle = {
            let nodes = shared.nodes.lock();
            nodes
                .get(&node)
                .and_then(|s| s.get(slot).cloned().flatten())
        };
        if let Some(w) = handle {
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() {
                return;
            }
            match tokio::time::timeout(rest, w.data.reserve()).await {
                Ok(Ok(permit)) => {
                    if let Some(p) = guard.p.take() {
                        permit.send(p);
                    }
                    return;
                }
                Ok(Err(_)) => {
                    if let Some(p) = guard.p.take() {
                        fail_pendings(shared, vec![p], Error::Closed);
                    }
                    return;
                }
                Err(_) => {
                    return;
                }
            }
        }
        let notified = shared.cache_nudge.notified();
        tokio::pin!(notified);
        let rest = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(rest.min(Duration::from_millis(10))) => {
                if Instant::now() >= deadline || shared.closed.load(Ordering::SeqCst) {
                    return;
                }
            }
        }
    }
}

/// Retry records stay pinned to their admitted partition and never enter the
/// accounting path again. Rebinding a cohort wakes displaced workers so a
/// metadata change between member transfers cannot strand half of a cohort.
async fn retry_sticky_one(shared: &Arc<Shared>, guard: &mut RetryGuard<'_>) {
    loop {
        let Some(pending) = guard.p.as_ref() else {
            return;
        };
        let deadline = pending.deadline;
        if shared.closed.load(Ordering::SeqCst) || Instant::now() >= deadline {
            return;
        }
        let Some(partition) = pending.rec.partition else {
            return;
        };
        let topic = Arc::clone(&pending.rec.topic);
        let outcome = {
            let cluster = shared.cluster.lock();
            let Some(policy) = &shared.sticky else {
                return;
            };
            let mut policy = policy.lock();
            if guard
                .p
                .as_ref()
                .and_then(|p| p.sticky)
                .is_some_and(|tag| policy.cancelled(tag))
            {
                return;
            }
            if let Ok(node) = cluster.leader_id(&topic, partition) {
                let slot = usize::try_from(partition).unwrap_or(0) % shared.cfg.connections.max(1);
                try_nudge_slot(&shared.connect_tx, node, slot);
                let worker = shared
                    .nodes
                    .lock()
                    .get(&node)
                    .and_then(|slots| slots.get(slot))
                    .cloned()
                    .flatten();
                if let Some(worker) = worker {
                    if let Some(pending) = guard.p.take() {
                        let tag = pending.sticky;
                        match worker.data.try_send(pending) {
                            Ok(()) => {
                                if let Some(tag) = tag {
                                    policy.rebind(tag, &worker.id);
                                }
                                Some(Ok(()))
                            }
                            Err(mpsc::error::TrySendError::Full(pending)) => {
                                guard.p = Some(pending);
                                None
                            }
                            Err(mpsc::error::TrySendError::Closed(pending)) => {
                                guard.p = Some(pending);
                                Some(Err(()))
                            }
                        }
                    } else {
                        Some(Ok(()))
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        match outcome {
            Some(Ok(())) => {
                shared.cache_nudge.notify_waiters();
                return;
            }
            Some(Err(())) => {
                if let Some(pending) = guard.p.take() {
                    fail_pendings(shared, vec![pending], Error::Closed);
                }
                return;
            }
            None => {}
        }
        let notified = shared.cache_nudge.notified();
        tokio::pin!(notified);
        let rest = deadline.saturating_duration_since(Instant::now());
        tokio::select! { _ = notified => {}, _ = tokio::time::sleep(rest.min(Duration::from_millis(10))) => {} }
    }
}

struct Worker {
    id: Arc<()>,
    node_id: i32,
    throttle: Arc<BrokerThrottle>,
    conn: BrokerConn,
    data: mpsc::Receiver<Pending>,
    ctrl: mpsc::Receiver<Ctrl>,
    shared: Arc<Shared>,
    write_buf: BytesMut,
    pending: Vec<Pending>,
    /// Empty record batch returned after an acknowledgement. The next drain refills it.
    spare_batch: Vec<Pending>,
    /// Sum of [`estimate`] for `pending`. Steady-state batching reads this
    /// instead of scanning the queue.
    pending_estimate: usize,
    /// Soonest delivery deadline in `pending`.
    earliest_deadline: Option<Instant>,
    in_flight: VecDeque<InFlight>,
    fail: Option<Error>,
    compress_scratch: CompressScratch,
    /// Reused partition map and topic index for the next Produce body.
    group_scratch: ProduceGroupScratch,
    /// Reused partition index for the next Produce acknowledgement.
    ack_index: AckIndex,
    /// Reused response-slot list for the next Produce acknowledgement.
    ack_slots: Vec<Option<usize>>,
    /// Reused Produce acknowledgement rows.
    spare_acks: Vec<crate::protocol::api::ProduceAck>,
    /// Partitions requested by the in-flight Produce. Inner sets are cleared per acknowledgement.
    ack_requested: HashMap<Arc<str>, HashSet<i32>>,
    /// Partitions already accepted in that acknowledgement.
    ack_seen: HashMap<Arc<str>, HashSet<i32>>,
    /// Topic metrics from the previous acknowledgement on this connection.
    ack_tracker: Option<(Arc<str>, Arc<crate::metrics::ProduceTopicTracker>)>,
}

#[derive(Default)]
struct ProduceGroupScratch {
    buckets: ProduceBuckets,
    /// First group index of each topic, in first-seen order.
    topics: Vec<usize>,
    /// Group indexes for each topic, parallel to `topics`.
    parts: Vec<Vec<usize>>,
    /// Partition slots for a one-topic batch. Cleared on each use.
    partition_slots: HashMap<i32, usize>,
    /// First-seen partitions for a one-topic batch. Cleared on each use.
    partition_order: Vec<i32>,
    /// Record counts copied out while a mixed batch sizes its partition lists.
    record_counts: Vec<usize>,
    /// Emptied partition lists from a previous split batch.
    spare_parts: Vec<Vec<Pending>>,
    /// Group list from the previous split batch.
    spare_groups: Vec<(Arc<str>, i32, Vec<Pending>)>,
    /// Partition-slot maps from the previous mixed batch.
    spare_slots: Vec<HashMap<i32, usize>>,
    /// Per-topic partition orders from the previous mixed batch.
    spare_orders: Vec<Vec<(i32, Vec<Pending>)>>,
    /// Topic order from the previous mixed batch.
    spare_topic_order: Vec<Arc<str>>,
    /// Topic name to the group-list slot. Cleared on each mixed batch.
    topic_slots: HashMap<Arc<str>, usize>,
    /// First topic id for each name. Cleared when a multi-topic v13 body is encoded.
    topic_ids: HashMap<Arc<str>, [u8; 16]>,
    /// Sequence base already packed for each topic partition. Cleared per take.
    batch_bases: HashMap<Arc<str>, HashMap<i32, Option<i32>>>,
}

struct PendingParts {
    order: Vec<(i32, Vec<Pending>)>,
    slots: HashMap<i32, usize>,
}

type ProduceBuckets = HashMap<Arc<str>, PendingParts>;

#[derive(Default)]
struct AckIndex {
    topics: HashMap<Arc<str>, HashMap<i32, (usize, u64)>>,
    generation: u64,
    /// Topic id to the first sent name. Cleared when a multi-topic v13 response is matched.
    by_uuid: HashMap<[u8; 16], Arc<str>>,
}

impl AckIndex {
    fn fill(&mut self, groups: &[(Arc<str>, i32, Vec<Pending>)]) {
        self.generation = self.generation.wrapping_add(1);
        let generation = self.generation;
        let one_topic = groups.first().is_some_and(|(topic, _, _)| {
            groups.iter().all(|(name, _, _)| Arc::ptr_eq(name, topic))
        });
        if one_topic {
            if let Some(topic) = groups.first().map(|(topic, _, _)| Arc::clone(topic)) {
                if !self.topics.contains_key(topic.as_ref()) {
                    let _ = self.topics.insert(Arc::clone(&topic), HashMap::new());
                }
                if let Some(parts) = self.topics.get_mut(topic.as_ref()) {
                    for (slot, (_, part, _)) in groups.iter().enumerate() {
                        match parts.get_mut(part) {
                            Some(entry) if entry.1 == generation => {}
                            Some(entry) => *entry = (slot, generation),
                            None => {
                                let _ = parts.insert(*part, (slot, generation));
                            }
                        }
                    }
                }
            }
        } else {
            for (slot, (topic, part, _)) in groups.iter().enumerate() {
                if let Some(parts) = self.topics.get_mut(topic.as_ref()) {
                    match parts.get_mut(part) {
                        Some(entry) if entry.1 == generation => {}
                        Some(entry) => *entry = (slot, generation),
                        None => {
                            let _ = parts.insert(*part, (slot, generation));
                        }
                    }
                    continue;
                }
                let mut parts = HashMap::new();
                let _ = parts.insert(*part, (slot, generation));
                let _ = self.topics.insert(Arc::clone(topic), parts);
            }
        }
        self.topics.retain(|_, parts| {
            parts.retain(|_, entry| entry.1 == generation);
            !parts.is_empty()
        });
    }

    fn get(&self, topic: &str, partition: i32) -> Option<usize> {
        let (slot, generation) = self.topics.get(topic)?.get(&partition)?;
        (*generation == self.generation).then_some(*slot)
    }
}

struct InFlight {
    correlation: i32,
    version: i16,
    // Snapshot of identities actually sent; latest metadata may describe a
    // recreated topic when an older response arrives.
    topic_ids: ProduceTopicIds,
    groups: ProduceGroups,
}

/// Topic ids carried with one Produce request.
///
/// One topic stays inline. Several topics own a vector.
enum ProduceTopicIds {
    Empty,
    One((Arc<str>, [u8; 16])),
    Many(Vec<(Arc<str>, [u8; 16])>),
}

impl ProduceTopicIds {
    fn as_slice(&self) -> &[(Arc<str>, [u8; 16])] {
        match self {
            Self::Empty => &[],
            Self::One(id) => std::slice::from_ref(id),
            Self::Many(ids) => ids,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 1,
            Self::Many(ids) => ids.len(),
        }
    }

    fn first(&self) -> Option<&(Arc<str>, [u8; 16])> {
        self.as_slice().first()
    }

    fn from_vec(ids: Vec<(Arc<str>, [u8; 16])>) -> Self {
        match ids.len() {
            0 => Self::Empty,
            1 => match ids.into_iter().next() {
                Some(id) => Self::One(id),
                None => Self::Empty,
            },
            _ => Self::Many(ids),
        }
    }
}

struct InFlightGuard {
    shared: Arc<Shared>,
    inf: Option<InFlight>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Some(inf) = self.inf.take() {
            fail_groups(&self.shared, inf.groups, Error::Timeout);
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        while let Ok(p) = self.data.try_recv() {
            self.pending.push(p);
        }
        if let Some(policy) = &self.shared.sticky {
            let mut policy = policy.lock();
            for p in &self.pending {
                if let Some(tag) = p.sticky {
                    policy.seal(tag);
                }
            }
        }
        if !self.shared.closed.load(Ordering::SeqCst) {
            while let Some(inf) = self.in_flight.pop_front() {
                self.requeue(inf.groups);
            }
            if !self.pending.is_empty() {
                let pendings = std::mem::take(&mut self.pending);
                self.requeue_pendings(pendings);
            }
        } else {
            if !self.in_flight.is_empty() {
                self.fail_kept_inflight(Error::Timeout);
            }
            if !self.pending.is_empty() {
                let pendings = std::mem::take(&mut self.pending);
                for p in pendings {
                    let err = if p.retry > 0 {
                        Error::Timeout
                    } else {
                        Error::Closed
                    };
                    fail_pendings(&self.shared, vec![p], err);
                }
            }
        }
        while let Ok(c) = self.ctrl.try_recv() {
            match c {
                Ctrl::Flush(tx) | Ctrl::Close(tx) => {
                    drop(tx.send(self.take_fail()));
                }
                Ctrl::Reconnect => {}
            }
        }
    }
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
            self.push_pending(p);
        }
    }

    fn note_deadline(&mut self, deadline: Instant) {
        self.earliest_deadline = Some(match self.earliest_deadline {
            Some(current) if current <= deadline => current,
            _ => deadline,
        });
    }

    fn push_pending(&mut self, pending: Pending) {
        self.pending_estimate = self.pending_estimate.saturating_add(estimate(&pending));
        self.note_deadline(pending.deadline);
        self.pending.push(pending);
    }

    fn account_pending_from(&mut self, from: usize) {
        let Some(added) = self.pending.get(from..) else {
            return;
        };
        let mut extra = 0usize;
        let mut earliest = self.earliest_deadline;
        for pending in added {
            extra = extra.saturating_add(estimate(pending));
            earliest = Some(match earliest {
                Some(current) if current <= pending.deadline => current,
                _ => pending.deadline,
            });
        }
        self.pending_estimate = self.pending_estimate.saturating_add(extra);
        self.earliest_deadline = earliest;
    }

    fn take_all_pending(&mut self) -> Vec<Pending> {
        self.pending_estimate = 0;
        self.earliest_deadline = None;
        std::mem::take(&mut self.pending)
    }

    fn queued_batch_ready(&self) -> bool {
        if let Some(first) = self.pending.first() {
            if let Some(base) = first.batch_base_seq {
                let count = self
                    .pending
                    .iter()
                    .take_while(|pending| pending.batch_base_seq == Some(base))
                    .count();
                return count >= first.batch_len;
            }
        }
        self.pending.len() >= self.shared.cfg.batch_records
            || self.pending_estimate >= self.shared.cfg.produce_batch_bytes()
    }

    fn forget_deadline(&mut self, deadline: Instant) {
        if self.earliest_deadline == Some(deadline) {
            self.earliest_deadline = self.pending.iter().map(|pending| pending.deadline).min();
        }
    }

    fn drain_pending(&mut self, n: usize) -> Vec<Pending> {
        let batch = fill_spare(&mut self.spare_batch, &mut self.pending, n);
        let mut refresh_deadline = false;
        for pending in &batch {
            self.pending_estimate = self.pending_estimate.saturating_sub(estimate(pending));
            if self.earliest_deadline == Some(pending.deadline) {
                refresh_deadline = true;
            }
        }
        if refresh_deadline {
            self.earliest_deadline = self.pending.iter().map(|pending| pending.deadline).min();
        }
        batch
    }

    fn recompute_pending_estimate(&mut self) {
        let mut total = 0usize;
        let mut earliest = None;
        for pending in &self.pending {
            total = total.saturating_add(estimate(pending));
            earliest = Some(match earliest {
                Some(current) if current <= pending.deadline => current,
                _ => pending.deadline,
            });
        }
        self.pending_estimate = total;
        self.earliest_deadline = earliest;
    }

    fn sticky_complete(&self) -> bool {
        let Some(policy) = &self.shared.sticky else {
            return true;
        };
        let Some(tag) = self.pending.first().and_then(|p| p.sticky) else {
            return true;
        };
        if tag.id == 0 {
            return true;
        }
        let policy = policy.lock();
        policy.cancelled(tag)
            || (policy.owned_by(tag, &self.id)
                && self
                    .pending
                    .iter()
                    .filter(|p| p.sticky.is_some_and(|member| member.id == tag.id))
                    .count()
                    >= policy.members(tag))
    }

    fn requeue_displaced_sticky(&mut self) {
        let shared = Arc::clone(&self.shared);
        let Some(policy) = &shared.sticky else {
            return;
        };
        let policy = policy.lock();
        let mut retained = Vec::with_capacity(self.pending.len());
        let mut displaced = Vec::new();
        for p in std::mem::take(&mut self.pending) {
            if p.sticky
                .is_some_and(|tag| !policy.cancelled(tag) && !policy.owned_by(tag, &self.id))
            {
                displaced.push(p);
            } else {
                retained.push(p);
            }
        }
        self.pending = retained;
        self.recompute_pending_estimate();
        drop(policy);
        if !displaced.is_empty() {
            self.requeue_pendings(displaced);
        }
    }

    async fn take_sticky_batch(&mut self) -> Vec<Pending> {
        let shared = Arc::clone(&self.shared);
        loop {
            self.pull_ready();
            self.purge_expired_pending();
            self.requeue_displaced_sticky();
            if self.pending.is_empty() {
                return Vec::new();
            }
            {
                let cluster = shared.cluster.lock();
                let Some(policy) = &shared.sticky else {
                    return Vec::new();
                };
                let mut policy = policy.lock();
                // Every committed admission is now visible. Holding the policy
                // lock prevents extending a tail while its actual drain seals it.
                self.pull_ready();
                let Some(tag) = self.pending.first().and_then(|p| p.sticky) else {
                    return Vec::new();
                };
                let count = if tag.id == 0 {
                    1
                } else {
                    self.pending
                        .iter()
                        .filter(|p| p.sticky.is_some_and(|member| member.id == tag.id))
                        .count()
                };
                if policy.cancelled(tag)
                    || (policy.owned_by(tag, &self.id) && count >= policy.members(tag))
                {
                    let cancelled = policy.cancelled(tag);
                    policy.drain(tag, &cluster);
                    if tag.id == 0 {
                        let Some(pending) = self.pending.drain(..1).next() else {
                            return Vec::new();
                        };
                        self.pending_estimate =
                            self.pending_estimate.saturating_sub(estimate(&pending));
                        self.forget_deadline(pending.deadline);
                        return vec![pending];
                    }
                    let mut batch = Vec::with_capacity(count);
                    let mut retained = Vec::with_capacity(self.pending.len().saturating_sub(count));
                    for p in std::mem::take(&mut self.pending) {
                        if p.sticky.is_some_and(|member| member.id == tag.id) {
                            batch.push(p);
                        } else {
                            retained.push(p);
                        }
                    }
                    self.pending = retained;
                    self.recompute_pending_estimate();
                    if cancelled {
                        drop(policy);
                        drop(cluster);
                        self.fail_kept_batch(batch, Error::Timeout);
                        continue;
                    }
                    batch.sort_by_key(|p| p.sticky.map_or(0, |tag| tag.index));
                    return batch;
                }
            }
            // A retry may be in transit or expire in another actor. Its terminal
            // callback shrinks membership and wakes this wait. No busy linger loop.
            let notified = shared.cache_nudge.notified();
            tokio::pin!(notified);
            let rest = self
                .earliest_pending_deadline()
                .map_or(Duration::from_millis(10), |deadline| {
                    deadline.saturating_duration_since(Instant::now())
                });
            tokio::select! {
                n = self.data.recv_many(&mut self.pending, shared.cfg.batch_records.max(1)) => {
                    if n == 0 { return Vec::new(); }
                    self.account_pending_from(self.pending.len().saturating_sub(n));
                }
                _ = notified => {}
                _ = tokio::time::sleep(rest.min(Duration::from_millis(10))) => {}
            }
        }
    }

    fn record_expired(&self, pending: &Pending, now: Instant) -> bool {
        now >= pending.deadline
            || pending.sticky.is_some_and(|tag| {
                self.shared
                    .sticky
                    .as_ref()
                    .is_some_and(|policy| policy.lock().cancelled(tag))
            })
    }

    fn purge_expired_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let now = Instant::now();
        let Some(first) = self
            .pending
            .iter()
            .position(|pending| self.record_expired(pending, now))
        else {
            return;
        };
        // One pass from the first expired record. Survivors stay in order.
        let mut expired = Vec::new();
        for pending in self.pending.split_off(first) {
            if self.record_expired(&pending, now) {
                expired.push(pending);
            } else {
                self.pending.push(pending);
            }
        }
        if !expired.is_empty() {
            self.recompute_pending_estimate();
            self.fail_kept_batch(expired, Error::Timeout);
        }
    }

    fn earliest_pending_deadline(&self) -> Option<Instant> {
        self.earliest_deadline
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
        if self.in_flight.len() >= self.shared.cfg.max_in_flight
            || self.throttle.remaining().is_some()
        {
            return false;
        }
        if !self.sticky_complete() {
            return false;
        }
        if let Some(first) = self
            .pending
            .first()
            .filter(|_| self.shared.sticky.is_none())
        {
            if let Some(base) = first.batch_base_seq {
                let count = self
                    .pending
                    .iter()
                    .take_while(|p| p.batch_base_seq == Some(base))
                    .count();
                return count >= first.batch_len;
            }
        }
        // Zero linger is ready as soon as a record is queued. Checking it
        // first skips a size scan of the whole pending queue.
        self.linger_expired(linger_start) || self.queued_batch_ready()
    }

    async fn run(mut self) {
        let mut linger_start: Option<Instant> = None;
        // One timer for linger and delivery deadlines. Resetting it avoids a
        // new sleep registration on every wait.
        let mut linger_timer = std::pin::pin!(tokio::time::sleep(Duration::ZERO));
        loop {
            if self.shared.closed.load(Ordering::SeqCst)
                && self.pending.is_empty()
                && self.in_flight.is_empty()
            {
                break;
            }
            if self.conn.is_closed() && self.pending.is_empty() && self.in_flight.is_empty() {
                break;
            }
            self.pull_ready();
            self.purge_expired_pending();
            self.requeue_displaced_sticky();
            if self.pending.is_empty() {
                linger_start = None;
            } else if linger_start.is_none() {
                linger_start = Some(Instant::now());
            }
            if self.can_fire(linger_start) {
                if let Err(e) = self.fire().await {
                    self.note_fail(e);
                }
                self.purge_expired_pending();
                linger_start = if self.pending.is_empty() {
                    None
                } else {
                    Some(Instant::now())
                };
                continue;
            }

            if self.in_flight.len() >= self.shared.cfg.max_in_flight
                || (!self.in_flight.is_empty()
                    && (self.pending.is_empty() || self.throttle.remaining().is_some()))
            {
                if let Err(e) = self.wait_one().await {
                    self.fail_kept_inflight(clone_err(&e));
                    self.note_fail(e);
                }
                continue;
            }

            let rec_limit = self.shared.cfg.batch_records;
            let room = rec_limit.saturating_sub(self.pending.len()).max(1);
            let linger = self.shared.cfg.linger;
            let wait_linger =
                linger_start.filter(|_| !linger.is_zero() && !self.pending.is_empty());

            let now = Instant::now();
            // A completed linger must not spin while a broker is muted. The
            // original delivery deadline still wakes us to expire queued work.
            let next_wake = if !self.sticky_complete() {
                self.earliest_pending_deadline()
                    .map(|deadline| deadline.saturating_duration_since(now))
            } else if let Some(rest) = self.throttle.remaining() {
                self.earliest_pending_deadline()
                    .map(|dl| rest.min(dl.saturating_duration_since(now)))
            } else {
                match (wait_linger, self.earliest_pending_deadline()) {
                    (Some(s), Some(dl)) => Some(
                        linger
                            .saturating_sub(s.elapsed())
                            .min(dl.saturating_duration_since(now)),
                    ),
                    (Some(s), None) => Some(linger.saturating_sub(s.elapsed())),
                    (None, Some(dl)) => Some(dl.saturating_duration_since(now)),
                    (None, None) => None,
                }
            };

            let wake_shared = Arc::clone(&self.shared);
            if let Some(delay) = next_wake {
                linger_timer
                    .as_mut()
                    .reset(tokio::time::Instant::now() + delay);
            }
            tokio::select! {
                biased;
                _ = wake_shared.cache_nudge.notified(), if wake_shared.sticky.is_some() => {}
                n = self.data.recv_many(&mut self.pending, room) => {
                    self.account_pending_from(self.pending.len().saturating_sub(n));
                    if n == 0 {
                        self.pull_ready();
                        self.drain_inflight().await;
                        break;
                    }
                    self.purge_expired_pending();
                    if linger_start.is_none() && !self.pending.is_empty() {
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
                        Some(Ctrl::Reconnect) => {
                            self.reconnect_if_idle().await;
                        }
                        Some(c) => {
                            let close = matches!(&c, Ctrl::Close(_));
                            self.pull_ready();
                            self.drain_inflight().await;
                            match c {
                                Ctrl::Flush(tx) | Ctrl::Close(tx) => {
                                    drop(tx.send(self.take_fail()));
                                }
                                Ctrl::Reconnect => {}
                            }
                            linger_start = None;
                            if close {
                                break;
                            }
                        }
                    }
                }
                _ = linger_timer.as_mut(), if next_wake.is_some() => {
                    self.purge_expired_pending();
                }
                ready = self.conn.inbound_ready(), if !self.in_flight.is_empty() && !self.conn.is_closed() => {
                    let read = match ready {
                        Ok(()) => self.wait_one().await,
                        Err(error) => Err(error),
                    };
                    if let Err(error) = read {
                        self.fail_kept_inflight(clone_err(&error));
                        self.note_fail(error);
                    }
                }
            }
        }
    }

    async fn reconnect_if_idle(&mut self) {
        if !self.in_flight.is_empty()
            || self.conn.is_closed()
            || !self.conn.idle_expired(self.shared.cfg.connections_max_idle)
        {
            return;
        }
        let addr = self.conn.addr().to_string();
        if let Ok(conn) = open_conn(&addr, &self.shared.cfg, self.shared.transaction_v2).await {
            if conn.produce_version() >= 0 {
                self.conn = conn;
            }
        }
    }

    async fn fire(&mut self) -> Result<()> {
        // Flush/close call fire directly, so they must also respect the quota.
        // Existing responses remain readable while new sends are muted. Sleep
        // is cancellation-safe: shutdown aborts workers at its original budget.
        loop {
            self.purge_expired_pending();
            if self.pending.is_empty() {
                return Ok(());
            }
            let Some(rest) = self.throttle.remaining() else {
                break;
            };
            if !self.in_flight.is_empty() {
                if let Err(e) = self.wait_one().await {
                    self.fail_kept_inflight(clone_err(&e));
                    return Err(e);
                }
                continue;
            }
            let rest = self.earliest_pending_deadline().map_or(rest, |dl| {
                rest.min(dl.saturating_duration_since(Instant::now()))
            });
            tokio::time::sleep(rest).await;
        }
        self.purge_expired_pending();
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.conn.is_closed() {
            remove_node_nudge_live(&self.shared, self.node_id);
            let pending = self.take_all_pending();
            self.requeue_pendings(pending);
            while let Some(remaining) = self.in_flight.pop_front() {
                self.requeue(remaining.groups);
            }
            return Ok(());
        }
        if self.in_flight.is_empty() && self.conn.idle_expired(self.shared.cfg.connections_max_idle)
        {
            let addr = self.conn.addr().to_string();
            match open_conn(&addr, &self.shared.cfg, self.shared.transaction_v2).await {
                Ok(c) if c.produce_version() >= 0 => self.conn = c,
                Ok(_) => {
                    let e = Error::Unsupported(format!(
                        "broker at {addr} does not support usable Produce v3-13"
                    ));
                    let pending = self.take_all_pending();
                    self.fail_kept_batch(pending, clone_err(&e));
                    self.note_fail(e.clone());
                    return Err(e);
                }
                Err(e) => {
                    let pending = self.take_all_pending();
                    if e.is_retriable() {
                        let _ = self.shared.nodes.lock().remove(&self.node_id);
                        self.requeue_pendings(pending);
                        return Ok(());
                    }
                    self.fail_kept_batch(pending, clone_err(&e));
                    self.note_fail(e.clone());
                    return Err(e);
                }
            }
        }
        let batch: Vec<Pending> = if self.shared.sticky.is_some() {
            self.take_sticky_batch().await
        } else {
            let n = take_count_in(
                &self.pending,
                self.shared.cfg.batch_records,
                self.shared.cfg.produce_batch_bytes(),
                &mut self.group_scratch.batch_bases,
            );
            self.drain_pending(n)
        };
        if let Some(p) = batch.iter().find(|p| p.rec.partition.is_none()) {
            let e = Error::protocol(format!("produce without partition topic={}", p.rec.topic));
            self.fail_kept_batch(batch, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        let batch_deadline = batch
            .iter()
            .map(|p| p.deadline)
            .min()
            .unwrap_or_else(|| Instant::now() + self.shared.cfg.delivery_timeout);
        let now = now_ms();
        let (mut groups, shell) = group_pending(batch, &mut self.group_scratch);
        self.park_batch(shell);
        if groups.is_empty() {
            return Ok(());
        }
        let version = self.conn.produce_version();
        let topic_ids = if version >= 13 {
            let identities = {
                let shared_topic = groups.first().and_then(|(topic, _, _)| {
                    let shared = groups.iter().all(|(name, _, _)| Arc::ptr_eq(name, topic));
                    shared.then(|| Arc::clone(topic))
                });
                if let Some(topic) = shared_topic {
                    let admitted_id = groups
                        .first()
                        .and_then(|(_, _, pendings)| pendings.first())
                        .and_then(|pending| pending.sticky)
                        .and_then(|tag| tag.identity);
                    let id = admitted_id.or_else(|| {
                        self.shared
                            .cluster
                            .lock()
                            .topic_ids
                            .get(topic.as_ref())
                            .copied()
                            .filter(|id| *id != [0; 16])
                    });
                    match id {
                        Some(id) => Ok(ProduceTopicIds::One((topic, id))),
                        None => Err(Error::Unsupported(format!(
                            "Produce v13 missing nonzero Metadata topic ID for {topic}"
                        ))),
                    }
                } else {
                    let cluster = self.shared.cluster.lock();
                    let resolved = groups.iter().map(|(topic, _, pendings)| {
                        let admitted_id = pendings
                            .first()
                            .and_then(|pending| pending.sticky)
                            .and_then(|tag| tag.identity);
                        let id = admitted_id.or_else(|| {
                            cluster
                                .topic_ids
                                .get(topic.as_ref())
                                .copied()
                                .filter(|id| *id != [0; 16])
                        });
                        (topic, id)
                    });
                    collect_produce_topic_ids(resolved)
                        .map(ProduceTopicIds::from_vec)
                        .map_err(|topic| {
                            Error::Unsupported(format!(
                                "Produce v13 missing nonzero Metadata topic ID for {topic}"
                            ))
                        })
                }
            };
            match identities {
                Ok(ids) => ids,
                Err(error) => {
                    self.fail_kept_groups(groups, clone_err(&error));
                    self.note_fail(error.clone());
                    return Err(error);
                }
            }
        } else {
            ProduceTopicIds::Empty
        };
        let producer_id = self.shared.producer_id.load(Ordering::SeqCst);
        let producer_epoch = self.shared.producer_epoch.load(Ordering::SeqCst);
        let epoch_gen = self.shared.epoch_gen.load(Ordering::SeqCst);
        assign_sequences(&mut groups, producer_id, &self.shared.seqs, epoch_gen);

        let fault = {
            let mut g = self.shared.pre_send_fault.lock();
            match *g {
                None => None,
                Some((f, None)) => {
                    *g = None;
                    Some(f)
                }
                Some((f, Some(1))) => {
                    *g = None;
                    Some(f)
                }
                Some((f, Some(count))) => {
                    *g = Some((f, Some(count - 1)));
                    Some(f)
                }
            }
        };
        if fault == Some(PreSendFault::TxnPartition) {
            let e = Error::broker(
                crate::error::OPERATION_NOT_ATTEMPTED,
                "injected transaction partition failure",
            );
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        if let Err(e) = self.add_txn_partitions(&groups).await {
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        let transactional_id = if self.shared.in_txn.load(Ordering::SeqCst) {
            self.shared.cfg.transactional_id.as_deref()
        } else {
            None
        };

        let version = self.conn.produce_version();
        let acks = self.shared.cfg.acks;
        let timeout_ms =
            i32::try_from(self.shared.cfg.request_timeout.as_millis()).unwrap_or(i32::MAX);
        let compression = self.shared.cfg.compression;
        #[cfg(feature = "zstd")]
        self.compress_scratch
            .set_zstd_level(self.shared.cfg.zstd_level);
        #[cfg(feature = "zstd")]
        self.compress_scratch
            .set_zstd_max_batch_bytes(self.shared.cfg.max_request_size);
        self.write_buf.clear();
        self.write_buf.put_i32(0);
        let correlation = self.conn.next_correlation();

        if fault == Some(PreSendFault::Encode) {
            let e = Error::protocol("injected produce encode failure");
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        if let Err(e) = encode_request_header_fields(
            &mut self.write_buf,
            PRODUCE,
            version,
            correlation,
            Some(self.conn.client_id()),
        ) {
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        if let Err(e) = encode_produce_body(
            &mut self.write_buf,
            version,
            acks,
            timeout_ms,
            &groups,
            topic_ids.as_slice(),
            compression,
            now,
            producer_id,
            producer_epoch,
            transactional_id,
            &mut self.compress_scratch,
            &mut self.group_scratch,
        ) {
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        let size =
            match crate::protocol::buf::i32_from_usize(self.write_buf.len().saturating_sub(4)) {
                Ok(s) => s,
                Err(e) => {
                    rollback_sequences(&groups, producer_id, &self.shared.seqs);
                    self.fail_kept_groups(groups, clone_err(&e));
                    self.note_fail(e.clone());
                    return Err(e);
                }
            };
        if let Err(e) = crate::protocol::buf::patch_i32(&mut self.write_buf, 0, size) {
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }

        if fault == Some(PreSendFault::Write) {
            let e = Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected socket write failure",
            ));
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        if fault == Some(PreSendFault::WriteRetriable) {
            let _ = self.shared.nodes.lock().remove(&self.node_id);
            self.requeue(groups);
            return Ok(());
        }
        let now_inst = Instant::now();
        if now_inst >= batch_deadline {
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, Error::Timeout);
            return Ok(());
        }
        let write_timeout = self
            .shared
            .cfg
            .request_timeout
            .min(batch_deadline.saturating_duration_since(now_inst));
        if let Err(e) = self
            .conn
            .write_all_timeout(&self.write_buf, write_timeout)
            .await
        {
            if e.is_retriable() {
                remove_node_nudge_live(&self.shared, self.node_id);
                self.requeue(groups);
                while let Some(remaining) = self.in_flight.pop_front() {
                    self.requeue(remaining.groups);
                }
                let pending = self.take_all_pending();
                self.requeue_pendings(pending);
                return Ok(());
            }
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            self.fail_kept_groups(groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }

        if acks == 0 {
            complete_acks0(&self.shared, groups, &mut self.ack_tracker);
            return Ok(());
        }
        self.in_flight.push_back(InFlight {
            correlation,
            version,
            topic_ids,
            groups,
        });
        Ok(())
    }

    async fn wait_one(&mut self) -> Result<()> {
        let Some(inf) = self.in_flight.pop_front() else {
            return Ok(());
        };
        let mut guard = InFlightGuard {
            shared: Arc::clone(&self.shared),
            inf: Some(inf),
        };
        let (correlation, version, batch_deadline) = match guard.inf.as_ref() {
            Some(inf) => {
                let dl = inf
                    .groups
                    .iter()
                    .flat_map(|(_, _, ps)| ps.iter())
                    .map(|p| p.deadline)
                    .min()
                    .unwrap_or_else(|| Instant::now() + self.shared.cfg.delivery_timeout);
                (inf.correlation, inf.version, dl)
            }
            None => (
                0,
                self.conn.produce_version(),
                Instant::now() + self.shared.cfg.delivery_timeout,
            ),
        };
        let now = Instant::now();
        let timeout = if now >= batch_deadline {
            Duration::ZERO
        } else {
            self.shared
                .cfg
                .request_timeout
                .min(batch_deadline.saturating_duration_since(now))
        };
        let body = match self
            .conn
            .read_response(PRODUCE, version, correlation, timeout)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                if let Some(inf) = guard.inf.take() {
                    if e.is_retriable() {
                        remove_node_nudge_live(&self.shared, self.node_id);
                        self.requeue(inf.groups);
                        while let Some(remaining) = self.in_flight.pop_front() {
                            self.requeue(remaining.groups);
                        }
                        let pending = self.take_all_pending();
                        self.requeue_pendings(pending);
                        return Ok(());
                    }
                    self.fail_kept_groups(inf.groups, clone_err(&e));
                }
                return Err(e);
            }
        };
        let mut body = body;
        if version >= 13 {
            if let Some(inf) = guard.inf.as_ref() {
                if inf.topic_ids.len() > 1 {
                    fill_ids_by_uuid(inf.topic_ids.as_slice(), &mut self.ack_index.by_uuid);
                }
            }
        }
        let by_uuid = &self.ack_index.by_uuid;
        let requested = &mut self.ack_requested;
        let seen = &mut self.ack_seen;
        let mut acks = std::mem::take(&mut self.spare_acks);
        let decoded = decode_produce_acks_into(&mut body, version, &mut acks).and_then(
            |(endpoints, throttle)| {
                if version >= 13 {
                    let inf = guard.inf.as_ref().ok_or_else(|| {
                        Error::protocol("missing Produce request identity snapshot")
                    })?;
                    let absent = || {
                        Error::protocol("Produce v13 response UUID is absent from sent snapshot")
                    };
                    let unsolicited = || {
                        Error::protocol("Produce v13 unsolicited or duplicate partition response")
                    };
                    if let Some((name, sent)) =
                        inf.topic_ids.first().filter(|_| inf.topic_ids.len() == 1)
                    {
                        if let [(topic, partition, _)] = inf.groups.as_slice() {
                            if Arc::ptr_eq(topic, name) || topic.as_ref() == name.as_ref() {
                                sole_partition_acks(*partition, sent, &acks)?;
                            } else if !acks.is_empty() {
                                return Err(unsolicited());
                            }
                        } else {
                            fill_ack_topics(&inf.groups, Some(name), requested);
                            clear_ack_parts(seen);
                            for ack in &acks {
                                if *sent == [0; 16]
                                    || ack.topic_id == [0; 16]
                                    || ack.topic_id != *sent
                                {
                                    return Err(absent());
                                }
                                let known = requested
                                    .get(name.as_ref())
                                    .is_some_and(|parts| parts.contains(&ack.partition));
                                if !known || !ack_partition_is_new(seen, name, ack.partition) {
                                    return Err(unsolicited());
                                }
                            }
                        }
                    } else {
                        solicited_v13_acks(&inf.groups, &acks, by_uuid, requested, seen)?;
                    }
                }
                Ok((endpoints, throttle))
            },
        );
        let (endpoints, throttle_ms) = match decoded {
            Ok(r) => r,
            Err(e) => {
                self.spare_acks = acks;
                if let Some(inf) = guard.inf.take() {
                    self.fail_kept_groups(inf.groups, clone_err(&e));
                }
                return Err(e);
            }
        };
        let responses = acks;
        // Apache ProduceResponse.shouldClientThrottle: older versions already
        // wait at the server. Publish the quota before any delivery completes.
        if version >= 6 {
            self.shared.throttle_metrics.observe(throttle_ms);
            self.throttle.observe(throttle_ms);
        }
        let Some(inf) = guard.inf.take() else {
            return Ok(());
        };
        if !endpoints.is_empty() {
            self.shared.cluster.lock().apply_node_endpoints(&endpoints);
        }
        let mut slots = std::mem::take(&mut self.ack_slots);
        slots.clear();
        if inf.groups.len() == 1 {
            let found = if let Some((topic, part, _)) = inf.groups.first() {
                match first_matching_ack(
                    version,
                    &responses,
                    topic,
                    *part,
                    inf.topic_ids.as_slice(),
                    &self.ack_index.by_uuid,
                ) {
                    Ok(found) => found,
                    Err(error) => {
                        self.ack_slots = slots;
                        self.spare_acks = responses;
                        self.fail_kept_groups(inf.groups, clone_err(&error));
                        return Err(error);
                    }
                }
            } else {
                None
            };
            slots.push(found);
        } else {
            self.ack_index.fill(&inf.groups);
            slots.resize_with(inf.groups.len(), || None);
            let ack_ids =
                (version >= 13 && inf.topic_ids.len() > 1).then_some(&self.ack_index.by_uuid);
            for (ack_at, ack) in responses.iter().enumerate() {
                let topic = if version >= 13 {
                    name_for_topic_id(inf.topic_ids.as_slice(), ack_ids, ack.topic_id)
                } else {
                    std::str::from_utf8(&ack.topic).ok()
                };
                let Some(topic) = topic else {
                    if version < 13 {
                        let e = Error::protocol("produce response topic is not utf-8");
                        self.ack_slots = slots;
                        self.spare_acks = responses;
                        self.fail_kept_groups(inf.groups, clone_err(&e));
                        return Err(e);
                    }
                    continue;
                };
                let Some(slot) = self.ack_index.get(topic, ack.partition) else {
                    continue;
                };
                if let Some(entry) = slots.get_mut(slot) {
                    if entry.is_none() {
                        *entry = Some(ack_at);
                    }
                }
            }
        }
        let mut first_err: Option<Error> = None;
        let OpenedProduceGroups { one, mut shell } = inf.groups.into_one_many();
        {
            let mut grouped = one.into_iter().chain(shell.drain(..));
            for (found_at, (topic, part, pendings)) in slots.drain(..).zip(&mut grouped) {
                let found = found_at.and_then(|at| responses.get(at));
                match found {
                    None => {
                        let e = Error::protocol("missing produce response");
                        self.fail_kept_batch(pendings, clone_err(&e));
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                    Some(r) if r.error_code != 0 => {
                        let e = Error::broker(r.error_code, format!("{topic}-{part}"));
                        // UNKNOWN_TOPIC_ID refresh is producer-private; it does
                        // not change the public Error retry category contract.
                        if e.is_retriable() || r.error_code == error::UNKNOWN_TOPIC_ID {
                            let applied = r.error_code != error::UNKNOWN_TOPIC_ID
                                && r.current_leader_id >= 0
                                && self.shared.cluster.lock().apply_current_leader(
                                    topic.as_ref(),
                                    part,
                                    r.current_leader_id,
                                    r.current_leader_epoch,
                                );
                            let mut pendings = pendings;
                            if applied {
                                drop_fast_topic(&self.shared, topic.as_ref());
                                let n_conn = self.shared.cfg.connections.max(1);
                                let slot = usize::try_from(part).unwrap_or(0) % n_conn;
                                try_nudge_slot(&self.shared.connect_tx, r.current_leader_id, slot);
                                for p in &mut pendings {
                                    p.skip_meta_refresh = true;
                                }
                            } else {
                                invalidate_cached_topic(&self.shared, topic.as_ref());
                                // No meta_tx nudge: the requeued batch's
                                // first pending owns the refresh (KL09-28),
                                // so a nudge here would be a duplicate RPC
                                // per faulted batch.
                            }
                            self.requeue_pendings(pendings);
                        } else if r.error_code == error::UNKNOWN_PRODUCER_ID
                            && self.shared.cfg.transactional_id.is_none()
                            && self.shared.producer_id.load(Ordering::SeqCst) >= 0
                        {
                            // Nontransactional idempotent recovery (KL03-09):
                            // a local epoch bump while the epoch can advance,
                            // a brand-new producer identity once it is
                            // exhausted. The transactional fencing path below
                            // is untouched: this never serves as fencing
                            // recovery.
                            if self.shared.producer_epoch.load(Ordering::SeqCst) == i16::MAX {
                                match renew_nontransactional_identity(&self.shared).await {
                                    Ok(()) => {
                                        let mut pendings = pendings;
                                        for p in &mut pendings {
                                            p.skip_meta_refresh = true;
                                        }
                                        self.requeue_pendings(pendings);
                                    }
                                    Err(renew_err) => {
                                        self.fail_kept_batch(pendings, clone_err(&renew_err));
                                        if first_err.is_none() {
                                            first_err = Some(renew_err);
                                        }
                                    }
                                }
                            } else {
                                self.shared.bump_idempotent_epoch();
                                let mut pendings = pendings;
                                for p in &mut pendings {
                                    p.skip_meta_refresh = true;
                                }
                                self.requeue_pendings(pendings);
                            }
                        } else if self.shared.cfg.transactional_id.is_some() {
                            // Java `maybeTransitionToErrorState` (KL03-10):
                            // fencing/authorization/identity errors fence the
                            // producer (fatal); every other failed batch makes
                            // the open transaction abort-required. Retriable
                            // codes were requeued above. The epoch-bump flag is
                            // Java `clientSideEpochBumpRequired`: only when the
                            // client bump path exists (classic coordinators);
                            // transaction V2 bumps server-side in `EndTxn`.
                            match classify_produce_error(r.error_code) {
                                TxnErrorDisposition::Fatal => {
                                    self.shared.set_txn_fatal(clone_err(&e));
                                }
                                TxnErrorDisposition::Abortable | TxnErrorDisposition::Retriable => {
                                    self.shared.set_txn_abortable(clone_err(&e));
                                    if client_epoch_bump_capable(
                                        self.shared.init_producer_id_version,
                                        self.shared.end_txn_version,
                                    ) {
                                        self.shared
                                            .epoch_bump_required
                                            .store(true, Ordering::SeqCst);
                                    }
                                }
                            }
                            self.fail_kept_batch(pendings, clone_err(&e));
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                        } else {
                            self.fail_kept_batch(pendings, clone_err(&e));
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                        }
                    }
                    Some(r) => {
                        let mut pendings = pendings;
                        let n = u64::try_from(pendings.len()).unwrap_or(u64::MAX);
                        self.shared.retire_sticky(&pendings, false);
                        self.shared.release_buffer(pendings_bytes(&pendings));
                        let tracker = self
                            .shared
                            .topic_tracker_cached(&mut self.ack_tracker, &topic);
                        self.shared.note_acked_on(&tracker, n);
                        // KL09-18: one clock read per batch; per-record
                        // latencies fold locally (no atomics, no lookups).
                        let ack = Instant::now();
                        let sample_ns = pendings
                            .first()
                            .map(|p| ack_latency_ns(ack, p.queued_at))
                            .unwrap_or(0);
                        let mut lat_n = 0u64;
                        let mut lat_sum = 0u64;
                        let mut lat_min = u64::MAX;
                        let mut lat_max = 0u64;
                        let mut stamped = None;
                        for (i, p) in pendings.drain(..).enumerate() {
                            let ns = ack_latency_ns(ack, p.queued_at);
                            lat_n += 1;
                            lat_sum = lat_sum.saturating_add(ns);
                            lat_min = lat_min.min(ns);
                            lat_max = lat_max.max(ns);
                            // KL09-16: build ack metadata only when a
                            // caller oneshot or an interceptor consumes it.
                            if p.tx.is_none() && self.shared.interceptors.is_empty() {
                                continue;
                            }
                            let batch_index = i32::try_from(i).unwrap_or(i32::MAX);
                            let now = *stamped.get_or_insert_with(now_ms);
                            let md = record_metadata(
                                &topic,
                                part,
                                r.base_offset,
                                batch_index,
                                &p.rec,
                                r.log_append_time_ms,
                                now,
                            );
                            self.shared.interceptors.on_ack(&md);
                            if let Some(tx) = p.tx {
                                drop(tx.send(Ok(md)));
                            }
                        }
                        self.shared.note_ack_latency_on(
                            &tracker,
                            crate::metrics::BatchLatency {
                                n: lat_n,
                                sum_nanos: lat_sum,
                                min_nanos: lat_min,
                                max_nanos: lat_max,
                                sample_ns,
                            },
                        );
                        self.park_batch(pendings);
                    }
                }
            }
        }
        self.ack_slots = slots;
        self.spare_acks = responses;
        self.stash_group_shell(shell);
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn requeue(&mut self, groups: ProduceGroups) {
        let OpenedProduceGroups { one, mut shell } = groups.into_one_many();
        if let Some((_, _, pendings)) = one {
            self.requeue_pendings(pendings);
        }
        for (_, _, pendings) in shell.drain(..) {
            self.requeue_pendings(pendings);
        }
        self.stash_group_shell(shell);
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
                remember_txn_part(&mut set, topic, *part);
            }
        }
        // Produce v12 is transaction V2 (KIP-890 Part 2): Produce also
        // performs AddPartitionsToTxn. Skip that RPC only for this leader.
        // Do not record the skip in the shared set: a later v3–v11 leader
        // must still send AddPartitionsToTxn after leader movement.
        if ProduceRequest::is_transaction_v2_requested(self.conn.produce_version()) {
            return Ok(());
        }
        let timeout = self.shared.cfg.request_timeout;
        let pid = self.shared.producer_id.load(Ordering::SeqCst);
        let epoch = self.shared.producer_epoch.load(Ordering::SeqCst);
        let version = self.shared.add_partitions_version;
        let added: Vec<(Arc<str>, i32)> = {
            let wanted = self.shared.txn_partitions.lock();
            let sent = self.shared.txn_added.lock();
            let mut added = Vec::new();
            for (topic, parts) in wanted.iter() {
                for part in parts {
                    if !txn_part_present(&sent, topic, *part) {
                        added.push((Arc::clone(topic), *part));
                    }
                }
            }
            added
        };
        if added.is_empty() {
            return Ok(());
        }
        let topics = group_txn_partitions(&added);
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            // A transport error sets no sticky state: adding partitions is
            // idempotent, so the failed send is simply retried by the caller
            // (or the transaction is aborted).
            let remaining = match timeout_remaining(deadline) {
                Ok(remaining) => remaining,
                Err(error) => {
                    let mut set = self.shared.txn_partitions.lock();
                    for (topic, part) in &added {
                        forget_txn_part(&mut set, topic, *part);
                    }
                    return Err(error);
                }
            };
            let body = match txn_roundtrip(
                &self.shared,
                ADD_PARTITIONS_TO_TXN,
                version,
                |buf| {
                    encode_add_partitions_to_txn_borrowed(buf, version, &tid, pid, epoch, &topics)
                },
                remaining,
                |body| decode_add_partitions_to_txn_response(&mut { body }, version),
            )
            .await
            {
                Ok(b) => b,
                Err(e) => {
                    let mut set = self.shared.txn_partitions.lock();
                    for (topic, part) in &added {
                        forget_txn_part(&mut set, topic, *part);
                    }
                    return Err(e);
                }
            };
            let err = match decode_add_partitions_to_txn_response(&mut body.clone(), version) {
                Ok(e) => e,
                Err(e) => {
                    let mut set = self.shared.txn_partitions.lock();
                    for (topic, part) in &added {
                        forget_txn_part(&mut set, topic, *part);
                    }
                    return Err(e);
                }
            };
            if err == 0 {
                break;
            }
            // Java `AddPartitionsToTxnHandler`: retry `CONCURRENT_TRANSACTIONS`
            // and other retriable codes, otherwise fence or require an abort
            // (see the `AddOffsetsToTxn` loop for the exhausted-budget rule).
            let can_handle = self.shared.txn_can_handle_abortable();
            if classify_add_partitions_error(err, can_handle) == TxnErrorDisposition::Retriable
                && Instant::now() < deadline
            {
                attempt += 1;
                txn_retry_sleep(&self.shared, attempt, deadline).await;
                if Instant::now() < deadline {
                    continue;
                }
            }
            let mut set = self.shared.txn_partitions.lock();
            for (topic, part) in &added {
                forget_txn_part(&mut set, topic, *part);
            }
            drop(set);
            let e = Error::broker(err, "AddPartitionsToTxn");
            self.shared.apply_txn_disposition(
                classify_add_partitions_error(err, can_handle),
                clone_err(&e),
            );
            return Err(e);
        }
        {
            let mut sent = self.shared.txn_added.lock();
            for (topic, part) in added {
                remember_txn_part_owned(&mut sent, topic, part);
            }
        }
        Ok(())
    }

    fn park_batch(&mut self, mut batch: Vec<Pending>) {
        batch.clear();
        if batch.capacity() == 0 {
            return;
        }
        if batch.capacity() > self.spare_batch.capacity() {
            let previous = std::mem::replace(&mut self.spare_batch, batch);
            self.stash_part_list(previous);
            return;
        }
        self.stash_part_list(batch);
    }

    fn stash_group_shell(&mut self, mut groups: Vec<(Arc<str>, i32, Vec<Pending>)>) {
        groups.clear();
        if groups.capacity() > self.group_scratch.spare_groups.capacity() {
            self.group_scratch.spare_groups = groups;
        }
    }

    fn stash_part_list(&mut self, batch: Vec<Pending>) {
        if batch.capacity() == 0 || self.group_scratch.spare_parts.len() >= 64 {
            return;
        }
        self.group_scratch.spare_parts.push(batch);
    }

    /// Fail every record and keep the batch allocation for the next send.
    fn fail_kept_batch(&mut self, batch: Vec<Pending>, err: Error) {
        let empty = fail_kept_pendings(&self.shared, batch, err);
        self.park_batch(empty);
    }

    /// Fail grouped records and keep the largest emptied batch list.
    fn fail_kept_groups(&mut self, groups: ProduceGroups, err: Error) {
        let OpenedProduceGroups { one, mut shell } = groups.into_one_many();
        let mut best = Vec::new();
        if let Some((_, _, pendings)) = one {
            best = fail_kept_pendings(&self.shared, pendings, clone_err(&err));
        }
        for (_, _, pendings) in shell.drain(..) {
            let empty = fail_kept_pendings(&self.shared, pendings, clone_err(&err));
            if empty.capacity() > best.capacity() {
                best = empty;
            }
        }
        self.park_batch(best);
        self.stash_group_shell(shell);
    }

    /// Fail every in-flight batch and keep the largest emptied list.
    fn fail_kept_inflight(&mut self, err: Error) {
        let mut best = Vec::new();
        let mut best_shell = Vec::new();
        while let Some(inflight) = self.in_flight.pop_front() {
            let OpenedProduceGroups { one, mut shell } = inflight.groups.into_one_many();
            if let Some((_, _, pendings)) = one {
                let empty = fail_kept_pendings(&self.shared, pendings, clone_err(&err));
                if empty.capacity() > best.capacity() {
                    best = empty;
                }
            }
            for (_, _, pendings) in shell.drain(..) {
                let empty = fail_kept_pendings(&self.shared, pendings, clone_err(&err));
                if empty.capacity() > best.capacity() {
                    best = empty;
                }
            }
            if shell.capacity() > best_shell.capacity() {
                best_shell = shell;
            }
        }
        self.park_batch(best);
        self.stash_group_shell(best_shell);
    }

    fn requeue_pendings(&mut self, mut pendings: Vec<Pending>) {
        if let Some(policy) = &self.shared.sticky {
            let mut policy = policy.lock();
            for p in &pendings {
                if let Some(tag) = p.sticky {
                    policy.seal(tag);
                }
            }
        }
        let now = Instant::now();
        // KL09-28: coalesce per topic — one backoff deadline and one
        // metadata refresh per retained topic-batch, not per record.
        // The retry loop drains sequentially, so the first pending's
        // refresh lands before the followers check the cached leader;
        // a follower that still finds no leader refreshes as a
        // fallback (e.g. the first's refresh failed).
        let (group_of, groups) = retry_topic_groups(&pendings);
        let mut firsts = vec![usize::MAX; groups.len()];
        for (i, &gi) in group_of.iter().enumerate() {
            if let Some(first) = firsts.get_mut(gi) {
                *first = (*first).min(i);
            }
        }
        for (i, (mut p, gi)) in pendings.drain(..).zip(group_of).enumerate() {
            p.retry = p.retry.saturating_add(1);
            if now >= p.deadline {
                fail_pendings(&self.shared, vec![p], Error::Timeout);
                continue;
            }
            // Group slots exist by construction; the fallback treats a
            // missing slot as its own group (refresh, own backoff).
            let (max_retry, first) = match (groups.get(gi), firsts.get(gi)) {
                (Some((_, max_retry)), Some(first)) => (*max_retry, *first),
                _ => (p.retry, i),
            };
            if i != first {
                p.skip_meta_refresh = true;
            }
            // Shared deadline from the group's max retry count: at
            // least as patient as any member's own backoff.
            let delay = crate::config::retry_backoff_delay(
                self.shared.cfg.retry_backoff,
                self.shared.cfg.retry_backoff_max,
                max_retry.saturating_sub(1),
            );
            p.retry_after = now + delay;
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
        self.park_batch(pendings);
    }

    async fn drain_inflight(&mut self) {
        while !self.pending.is_empty() || !self.in_flight.is_empty() {
            self.purge_expired_pending();
            if !self.pending.is_empty() {
                while self.in_flight.len() >= self.shared.cfg.max_in_flight {
                    if let Err(e) = self.wait_one().await {
                        self.fail_kept_inflight(clone_err(&e));
                        self.note_fail(e);
                    }
                }
                if let Err(e) = self.fire().await {
                    self.note_fail(e);
                }
            } else if let Err(e) = self.wait_one().await {
                self.fail_kept_inflight(clone_err(&e));
                self.note_fail(e);
            }
        }
    }
}

type TxnPartSet = HashMap<Arc<str>, HashSet<i32>>;

fn remember_txn_part(map: &mut TxnPartSet, topic: &Arc<str>, partition: i32) {
    if let Some(parts) = map.get_mut(topic.as_ref()) {
        let _ = parts.insert(partition);
        return;
    }
    let mut parts = HashSet::new();
    let _ = parts.insert(partition);
    let _ = map.insert(Arc::clone(topic), parts);
}

fn remember_txn_part_owned(map: &mut TxnPartSet, topic: Arc<str>, partition: i32) {
    if let Some(parts) = map.get_mut(topic.as_ref()) {
        let _ = parts.insert(partition);
        return;
    }
    let mut parts = HashSet::new();
    let _ = parts.insert(partition);
    let _ = map.insert(topic, parts);
}

fn txn_part_present(map: &TxnPartSet, topic: &Arc<str>, partition: i32) -> bool {
    map.get(topic.as_ref())
        .is_some_and(|parts| parts.contains(&partition))
}

fn forget_txn_part(map: &mut TxnPartSet, topic: &Arc<str>, partition: i32) {
    let empty = if let Some(parts) = map.get_mut(topic.as_ref()) {
        let _ = parts.remove(&partition);
        parts.is_empty()
    } else {
        return;
    };
    if empty {
        let _ = map.remove(topic.as_ref());
    }
}

fn retry_topic_groups(pendings: &[Pending]) -> (Vec<usize>, Vec<(Arc<str>, u32)>) {
    let mut group_of = Vec::with_capacity(pendings.len());
    let mut groups: Vec<(Arc<str>, u32)> = Vec::new();
    let mut slots: HashMap<&str, usize> = HashMap::new();
    for pending in pendings {
        let incremented = pending.retry.saturating_add(1);
        if let Some(&group) = slots.get(pending.rec.topic.as_ref()) {
            if let Some(slot) = groups.get_mut(group) {
                slot.1 = slot.1.max(incremented);
            }
            group_of.push(group);
            continue;
        }
        let group = groups.len();
        let _ = slots.insert(pending.rec.topic.as_ref(), group);
        group_of.push(group);
        groups.push((Arc::clone(&pending.rec.topic), incremented));
    }
    (group_of, groups)
}

fn group_txn_partitions(parts: &[(Arc<str>, i32)]) -> Vec<BorrowedTxnTopic<'_>> {
    let mut topics: Vec<BorrowedTxnTopic<'_>> = Vec::new();
    let mut slots: HashMap<&str, usize> = HashMap::new();
    for (topic, part) in parts {
        if let Some(&slot) = slots.get(topic.as_ref()) {
            if let Some(group) = topics.get_mut(slot) {
                group.partitions.push(*part);
                continue;
            }
        }
        let slot = topics.len();
        let _ = slots.insert(topic.as_ref(), slot);
        topics.push(BorrowedTxnTopic {
            topic: topic.as_ref(),
            partitions: vec![*part],
        });
    }
    topics
}

fn unique_offset_topics(
    offsets: &[(crate::TopicPartition, crate::OffsetAndMetadata)],
) -> Vec<&str> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut topics = Vec::new();
    for (tp, _) in offsets {
        if seen.insert(tp.topic.as_str()) {
            topics.push(tp.topic.as_str());
        }
    }
    topics
}

fn group_txn_offsets<'a>(
    offsets: &'a [(crate::TopicPartition, crate::OffsetAndMetadata)],
    cluster: &crate::cluster::Cluster,
) -> Vec<BorrowedTxnOffsetTopic<'a>> {
    let mut topics: Vec<BorrowedTxnOffsetTopic<'a>> = Vec::new();
    let mut slots: HashMap<&str, usize> = HashMap::new();
    let mut epochs = crate::cluster::EpochList::new();
    for (tp, md) in offsets {
        let leader_epoch = md
            .leader_epoch
            .unwrap_or_else(|| epochs.epoch(cluster, &tp.topic, tp.partition));
        let partition = BorrowedTxnOffsetPartition {
            partition: tp.partition,
            offset: md.offset,
            leader_epoch,
            metadata: md.metadata.as_str(),
        };
        if let Some(&slot) = slots.get(tp.topic.as_str()) {
            if let Some(group) = topics.get_mut(slot) {
                group.partitions.push(partition);
                continue;
            }
        }
        let slot = topics.len();
        let _ = slots.insert(tp.topic.as_str(), slot);
        topics.push(BorrowedTxnOffsetTopic {
            topic: tp.topic.as_str(),
            partitions: vec![partition],
        });
    }
    topics
}

fn fill_spare(spare: &mut Vec<Pending>, pending: &mut Vec<Pending>, n: usize) -> Vec<Pending> {
    let mut batch = std::mem::take(spare);
    batch.clear();
    batch.extend(pending.drain(..n));
    batch
}

#[cfg(test)]
fn park_spare(spare: &mut Vec<Pending>, mut batch: Vec<Pending>) {
    batch.clear();
    if batch.capacity() > spare.capacity() {
        *spare = batch;
    }
}

fn take_group_shell(scratch: &mut ProduceGroupScratch) -> Vec<(Arc<str>, i32, Vec<Pending>)> {
    let mut groups = std::mem::take(&mut scratch.spare_groups);
    groups.clear();
    groups
}

fn take_part_list(scratch: &mut ProduceGroupScratch) -> Vec<Pending> {
    let mut part = scratch.spare_parts.pop().unwrap_or_default();
    part.clear();
    part
}

fn group_pending(
    mut batch: Vec<Pending>,
    scratch: &mut ProduceGroupScratch,
) -> (ProduceGroups, Vec<Pending>) {
    if batch.is_empty() {
        return (ProduceGroups::Empty, batch);
    }
    let (topic0, part0) = {
        let Some(first) = batch.first() else {
            return (ProduceGroups::Empty, batch);
        };
        let topic0 = Arc::clone(&first.rec.topic);
        let part0 = first
            .rec
            .partition
            .unwrap_or(RecordMetadata::UNKNOWN_PARTITION);
        (topic0, part0)
    };
    let same_topic =
        |topic: &Arc<str>| Arc::ptr_eq(topic, &topic0) || topic.as_ref() == topic0.as_ref();
    // One pass classifies the batch. A second topic stops the scan; a single
    // topic does not walk the records again just to learn that.
    let mut one_partition = true;
    let mut one_topic = true;
    for pending in batch.iter().skip(1) {
        if pending.rec.partition != Some(part0) {
            one_partition = false;
        }
        if !same_topic(&pending.rec.topic) {
            one_topic = false;
            break;
        }
    }
    if one_partition && one_topic {
        return (ProduceGroups::One((topic0, part0, batch)), Vec::new());
    }
    if one_topic {
        let mut groups = take_group_shell(scratch);
        scratch.partition_slots.clear();
        scratch.partition_order.clear();
        for pending in &batch {
            let partition = pending
                .rec
                .partition
                .unwrap_or(RecordMetadata::UNKNOWN_PARTITION);
            if let Some(count) = scratch.partition_slots.get_mut(&partition) {
                *count = count.saturating_add(1);
            } else {
                scratch.partition_order.push(partition);
                let _inserted = scratch.partition_slots.insert(partition, 1);
            }
        }
        reserve_len(&mut groups, scratch.partition_order.len());
        let partitions = scratch.partition_order.len();
        for index in 0..partitions {
            let Some(partition) = scratch.partition_order.get(index).copied() else {
                continue;
            };
            let count = scratch
                .partition_slots
                .get(&partition)
                .copied()
                .unwrap_or(0);
            let mut part = take_part_list(scratch);
            reserve_len(&mut part, count);
            groups.push((Arc::clone(&topic0), partition, part));
        }
        scratch.partition_order.clear();
        scratch.partition_slots.clear();
        for (index, (_, partition, _)) in groups.iter().enumerate() {
            let _inserted = scratch.partition_slots.insert(*partition, index);
        }
        for pending in batch.drain(..) {
            let partition = pending
                .rec
                .partition
                .unwrap_or(RecordMetadata::UNKNOWN_PARTITION);
            let slot = scratch.partition_slots.get(&partition).copied();
            if let Some((_, _, records)) = slot.and_then(|slot| groups.get_mut(slot)) {
                records.push(pending);
                continue;
            }
            let slot = groups.len();
            let _inserted = scratch.partition_slots.insert(partition, slot);
            let mut part = take_part_list(scratch);
            part.push(pending);
            groups.push((Arc::clone(&topic0), partition, part));
        }
        return (ProduceGroups::Many(groups), batch);
    }
    let mut spare_parts = std::mem::take(&mut scratch.spare_parts);
    let mut spare_slots = std::mem::take(&mut scratch.spare_slots);
    let mut spare_orders = std::mem::take(&mut scratch.spare_orders);
    let mut order = std::mem::take(&mut scratch.spare_topic_order);
    let mut counts = std::mem::take(&mut scratch.record_counts);
    order.clear();
    {
        let buckets = &mut scratch.buckets;
        for pending in &batch {
            let partition = pending
                .rec
                .partition
                .unwrap_or(RecordMetadata::UNKNOWN_PARTITION);
            if let Some(groups) = buckets.get_mut(pending.rec.topic.as_ref()) {
                if let Some(count) = groups.slots.get_mut(&partition) {
                    *count = count.saturating_add(1);
                    continue;
                }
                let _inserted = groups.slots.insert(partition, 1);
                let mut part = spare_parts.pop().unwrap_or_default();
                part.clear();
                groups.order.push((partition, part));
                continue;
            }
            let topic = Arc::clone(&pending.rec.topic);
            order.push(Arc::clone(&topic));
            let mut slots = spare_slots.pop().unwrap_or_default();
            slots.clear();
            let _inserted = slots.insert(partition, 1);
            let mut part = spare_parts.pop().unwrap_or_default();
            part.clear();
            let mut topic_order = spare_orders.pop().unwrap_or_default();
            topic_order.clear();
            topic_order.push((partition, part));
            let _inserted = buckets.insert(
                topic,
                PendingParts {
                    order: topic_order,
                    slots,
                },
            );
        }
        for topic in &order {
            let Some(groups) = buckets.get_mut(topic.as_ref()) else {
                continue;
            };
            counts.clear();
            reserve_len(&mut counts, groups.order.len());
            for (partition, _) in &groups.order {
                counts.push(groups.slots.get(partition).copied().unwrap_or(0));
            }
            for ((_, records), count) in groups.order.iter_mut().zip(counts.iter().copied()) {
                reserve_len(records, count);
            }
            for (index, (partition, _)) in groups.order.iter().enumerate() {
                let _inserted = groups.slots.insert(*partition, index);
            }
        }
        for pending in batch.drain(..) {
            let partition = pending
                .rec
                .partition
                .unwrap_or(RecordMetadata::UNKNOWN_PARTITION);
            if let Some(groups) = buckets.get_mut(pending.rec.topic.as_ref()) {
                if let Some(&slot) = groups.slots.get(&partition) {
                    if let Some((_, records)) = groups.order.get_mut(slot) {
                        records.push(pending);
                        continue;
                    }
                }
                let _inserted = groups.slots.insert(partition, groups.order.len());
                let mut part = spare_parts.pop().unwrap_or_default();
                part.clear();
                part.push(pending);
                groups.order.push((partition, part));
                continue;
            }
            let topic = Arc::clone(&pending.rec.topic);
            order.push(Arc::clone(&topic));
            let mut slots = spare_slots.pop().unwrap_or_default();
            slots.clear();
            let _inserted = slots.insert(partition, 0);
            let mut part = spare_parts.pop().unwrap_or_default();
            part.clear();
            part.push(pending);
            let mut topic_order = spare_orders.pop().unwrap_or_default();
            topic_order.clear();
            topic_order.push((partition, part));
            let _inserted = buckets.insert(
                topic,
                PendingParts {
                    order: topic_order,
                    slots,
                },
            );
        }
    }
    scratch.record_counts = counts;
    let mut total = 0usize;
    for topic in &order {
        total = total.saturating_add(
            scratch
                .buckets
                .get(topic.as_ref())
                .map(|groups| groups.order.len())
                .unwrap_or(0),
        );
    }
    let mut out = take_group_shell(scratch);
    reserve_len(&mut out, total);
    for topic in order.drain(..) {
        let Some(mut groups) = scratch.buckets.remove(topic.as_ref()) else {
            continue;
        };
        for (partition, pending) in groups.order.drain(..) {
            out.push((Arc::clone(&topic), partition, pending));
        }
        groups.slots.clear();
        if spare_slots.len() < 64 {
            spare_slots.push(groups.slots);
        }
        groups.order.clear();
        if spare_orders.len() < 64 {
            spare_orders.push(groups.order);
        }
    }
    scratch.spare_parts = spare_parts;
    scratch.spare_slots = spare_slots;
    scratch.spare_orders = spare_orders;
    scratch.spare_topic_order = order;
    (ProduceGroups::from_vec(out), batch)
}

/// Partition groups for one Produce request.
///
/// One partition stays inline. Several partitions own a vector.
enum ProduceGroups {
    Empty,
    One((Arc<str>, i32, Vec<Pending>)),
    Many(Vec<(Arc<str>, i32, Vec<Pending>)>),
}

/// A produce group list opened for acknowledgement or failure.
///
/// One partition does not allocate the shell.
struct OpenedProduceGroups {
    one: Option<(Arc<str>, i32, Vec<Pending>)>,
    shell: Vec<(Arc<str>, i32, Vec<Pending>)>,
}

impl ProduceGroups {
    fn as_slice(&self) -> &[(Arc<str>, i32, Vec<Pending>)] {
        match self {
            Self::Empty => &[],
            Self::One(group) => std::slice::from_ref(group),
            Self::Many(groups) => groups,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [(Arc<str>, i32, Vec<Pending>)] {
        match self {
            Self::Empty => &mut [],
            Self::One(group) => std::slice::from_mut(group),
            Self::Many(groups) => groups,
        }
    }

    /// One group stays beside an empty shell. Several groups keep their vector.
    fn into_one_many(self) -> OpenedProduceGroups {
        match self {
            Self::Empty => OpenedProduceGroups {
                one: None,
                shell: Vec::new(),
            },
            Self::One(group) => OpenedProduceGroups {
                one: Some(group),
                shell: Vec::new(),
            },
            Self::Many(groups) => OpenedProduceGroups {
                one: None,
                shell: groups,
            },
        }
    }

    fn from_vec(groups: Vec<(Arc<str>, i32, Vec<Pending>)>) -> Self {
        match groups.len() {
            0 => Self::Empty,
            1 => match groups.into_iter().next() {
                Some(group) => Self::One(group),
                None => Self::Empty,
            },
            _ => Self::Many(groups),
        }
    }
}

impl std::ops::Deref for ProduceGroups {
    type Target = [(Arc<str>, i32, Vec<Pending>)];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::ops::DerefMut for ProduceGroups {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl IntoIterator for ProduceGroups {
    type Item = (Arc<str>, i32, Vec<Pending>);
    type IntoIter = ProduceGroupsIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Empty => ProduceGroupsIntoIter {
                one: None,
                many: Vec::new().into_iter(),
            },
            Self::One(group) => ProduceGroupsIntoIter {
                one: Some(group),
                many: Vec::new().into_iter(),
            },
            Self::Many(groups) => ProduceGroupsIntoIter {
                one: None,
                many: groups.into_iter(),
            },
        }
    }
}

struct ProduceGroupsIntoIter {
    one: Option<(Arc<str>, i32, Vec<Pending>)>,
    many: std::vec::IntoIter<(Arc<str>, i32, Vec<Pending>)>,
}

impl Iterator for ProduceGroupsIntoIter {
    type Item = (Arc<str>, i32, Vec<Pending>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(group) = self.one.take() {
            return Some(group);
        }
        self.many.next()
    }
}

#[cfg(test)]
fn take_count(pending: &[Pending], rec_limit: usize, byte_limit: usize) -> usize {
    let mut bases = HashMap::new();
    take_count_in(pending, rec_limit, byte_limit, &mut bases)
}

fn take_count_in(
    pending: &[Pending],
    rec_limit: usize,
    byte_limit: usize,
    bases: &mut HashMap<Arc<str>, HashMap<i32, Option<i32>>>,
) -> usize {
    let group_overhead = usize::try_from(RecordBatch::RECORD_BATCH_OVERHEAD).unwrap_or(0);
    if let Some(first) = pending.first() {
        let topic = first.rec.topic.as_ref();
        let part = first.rec.partition.unwrap_or(0);
        let base = first.batch_base_seq;
        let mut n = 0usize;
        let mut bytes = 0usize;
        let mut one_group = true;
        for p in pending.iter().take(rec_limit) {
            if p.rec.partition.unwrap_or(0) != part || p.rec.topic.as_ref() != topic {
                one_group = false;
                break;
            }
            if p.batch_base_seq != base {
                break;
            }
            let add = estimate(p).saturating_add(if n == 0 { group_overhead } else { 0 });
            if n > 0 && bytes.saturating_add(add) > byte_limit {
                break;
            }
            bytes = bytes.saturating_add(add);
            n += 1;
            if bytes >= byte_limit {
                break;
            }
        }
        if one_group {
            return n.max(1).min(pending.len());
        }
    }
    let mut n = 0;
    let mut bytes = 0usize;
    for parts in bases.values_mut() {
        parts.clear();
    }
    for p in pending.iter().take(rec_limit) {
        let partition = p.rec.partition.unwrap_or(0);
        let new_group = if let Some(parts) = bases.get(p.rec.topic.as_ref()) {
            match parts.get(&partition) {
                Some(&expected_base) => {
                    if p.batch_base_seq != expected_base {
                        break;
                    }
                    false
                }
                None => true,
            }
        } else {
            true
        };
        // KL09-14: strict bound — stop before the record that would cross
        // it, counting one batch overhead per partition group. The take
        // still advances by at least one record: an unsplittable single
        // keeps Java parity by exceeding `batch_bytes` alone.
        let add = estimate(p).saturating_add(if new_group { group_overhead } else { 0 });
        if n > 0 && bytes.saturating_add(add) > byte_limit {
            break;
        }
        if new_group {
            if let Some(parts) = bases.get_mut(p.rec.topic.as_ref()) {
                let _ = parts.insert(partition, p.batch_base_seq);
            } else {
                let mut parts = HashMap::new();
                let _ = parts.insert(partition, p.batch_base_seq);
                let _ = bases.insert(Arc::clone(&p.rec.topic), parts);
            }
        }
        bytes = bytes.saturating_add(add);
        n += 1;
        if bytes >= byte_limit {
            break;
        }
    }
    bases.retain(|_, parts| !parts.is_empty());
    n.max(1).min(pending.len())
}

/// Upper-bound wire size of one queued record for batch packing (KL09-14).
///
/// Uses the same [`record_size_upper_bound`] (key, value, headers,
/// `MAX_RECORD_OVERHEAD`) as the queue-time oversize check, so packing
/// sees what the encoder will write. `take_count` adds one
/// [`RecordBatch::RECORD_BATCH_OVERHEAD`] per partition group. The running
/// queue total used for readiness omits that overhead; firing early is
/// harmless because the take enforces the bound. Overflow fails closed to `usize::MAX`
/// (record travels alone); queue-time rejection makes it unreachable.
fn estimate(p: &Pending) -> usize {
    let key = p.rec.key.as_deref();
    let value = p.rec.value.as_deref();
    record_size_upper_bound(key, value, &p.rec.headers)
        .ok()
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(usize::MAX)
}

fn refresh_sequence_epoch(pendings: &mut [Pending], epoch_gen: u64) {
    // A retained batch whose sequence was assigned under a superseded
    // epoch (shared epoch bump or identity renewal while it was
    // pending, in flight, or queued for retry) must drop the stale
    // assignment and re-take a sequence under the current epoch.
    // Sending the old base sequence with the new epoch would fail with
    // OUT_OF_ORDER_SEQUENCE_NUMBER; reusing seq zero twice on one
    // partition would silently dedup away a record.
    for pending in pendings {
        if pending.epoch_gen != epoch_gen {
            pending.seq = None;
            pending.batch_base_seq = None;
            pending.batch_len = 1;
            pending.epoch_gen = epoch_gen;
        }
    }
}

fn stamp_sequences(pendings: &mut [Pending], base: i32) {
    let count = pendings.len();
    for (index, pending) in pendings.iter_mut().enumerate() {
        if pending.seq.is_none() {
            pending.seq = Some(base.saturating_add(i32::try_from(index).unwrap_or(0)));
            pending.batch_base_seq = Some(base);
            pending.batch_len = count;
        }
    }
}

fn assign_sequences(
    groups: &mut [(Arc<str>, i32, Vec<Pending>)],
    producer_id: i64,
    seqs: &parking_lot::Mutex<SequenceMap>,
    epoch_gen: u64,
) {
    if producer_id <= RecordBatch::NO_PRODUCER_ID {
        return;
    }
    let mut map = seqs.lock();
    let one_topic = groups
        .first()
        .is_some_and(|(topic, _, _)| groups.iter().all(|(name, _, _)| Arc::ptr_eq(name, topic)));
    if one_topic {
        let Some(topic) = groups.first().map(|(topic, _, _)| Arc::clone(topic)) else {
            return;
        };
        if !map.contains_key(topic.as_ref()) {
            let _ = map.insert(Arc::clone(&topic), HashMap::new());
        }
        let Some(parts) = map.get_mut(topic.as_ref()) else {
            return;
        };
        for (_, partition, pendings) in groups.iter_mut() {
            refresh_sequence_epoch(pendings, epoch_gen);
            if pendings.iter().all(|pending| pending.seq.is_some()) {
                continue;
            }
            let base = sequence_in(parts, *partition, pendings.len());
            stamp_sequences(pendings, base);
        }
        return;
    }
    for (topic, partition, pendings) in groups.iter_mut() {
        refresh_sequence_epoch(pendings, epoch_gen);
        if pendings.iter().all(|pending| pending.seq.is_some()) {
            continue;
        }
        let base = sequence_of(&mut map, topic, *partition, pendings.len());
        stamp_sequences(pendings, base);
    }
}

fn rewind_sequence(parts: &mut HashMap<i32, i32>, partition: i32, pendings: &[Pending]) {
    let Some(first) = pendings.first() else {
        return;
    };
    let Some(base) = first.seq else {
        return;
    };
    let count = i32::try_from(pendings.len()).unwrap_or(0);
    if let Some(curr) = parts.get_mut(&partition) {
        if *curr == base.saturating_add(count) {
            *curr = base;
        }
    }
}

fn rollback_sequences(
    groups: &[(Arc<str>, i32, Vec<Pending>)],
    producer_id: i64,
    seqs: &parking_lot::Mutex<SequenceMap>,
) {
    if producer_id <= RecordBatch::NO_PRODUCER_ID {
        return;
    }
    let mut g = seqs.lock();
    // A produce batch is one topic. Load that partition map once.
    let one_topic = groups
        .first()
        .is_some_and(|(topic, _, _)| groups.iter().all(|(name, _, _)| Arc::ptr_eq(name, topic)));
    if one_topic {
        let Some(topic) = groups.first().map(|(topic, _, _)| topic.as_ref()) else {
            return;
        };
        let Some(parts) = g.get_mut(topic) else {
            return;
        };
        for (_, partition, pendings) in groups {
            rewind_sequence(parts, *partition, pendings);
        }
        return;
    }
    for (topic, partition, pendings) in groups {
        let Some(parts) = g.get_mut(topic.as_ref()) else {
            continue;
        };
        rewind_sequence(parts, *partition, pendings);
    }
}

type ProduceTopicId = (Arc<str>, [u8; 16]);

/// First id for each topic name. A repeated name keeps that first id. A
/// later topic that reuses an id already kept is an ambiguous UUID. `None`
/// is a missing id and stops the walk. A provided all-zero id is kept: only
/// the metadata lookup treats zero as absent.
fn collect_produce_topic_ids<'a, I>(topics: I) -> std::result::Result<Vec<ProduceTopicId>, String>
where
    I: IntoIterator<Item = (&'a Arc<str>, Option<[u8; 16]>)>,
{
    let mut identities: Vec<ProduceTopicId> = Vec::new();
    let mut seen_names: HashSet<&str> = HashSet::new();
    let mut seen_ids: HashSet<[u8; 16]> = HashSet::new();
    for (topic, id) in topics {
        if !seen_names.insert(topic.as_ref()) {
            continue;
        }
        let Some(id) = id else {
            return Err(topic.to_string());
        };
        if !seen_ids.insert(id) {
            return Err(format!("{topic} (ambiguous UUID)"));
        }
        identities.push((Arc::clone(topic), id));
    }
    Ok(identities)
}

/// First topic id for each name. A later copy of the same name is ignored,
/// including when the first id is all zeros.
fn fill_topic_ids(ids: &[(Arc<str>, [u8; 16])], index: &mut HashMap<Arc<str>, [u8; 16]>) {
    index.clear();
    for (name, id) in ids {
        if !index.contains_key(name.as_ref()) {
            let _ = index.insert(Arc::clone(name), *id);
        }
    }
}

fn topic_id_for<'a>(
    ids: &'a [(Arc<str>, [u8; 16])],
    index: Option<&'a HashMap<Arc<str>, [u8; 16]>>,
    topic: &Arc<str>,
) -> Option<&'a [u8; 16]> {
    let id = if ids.len() == 1 {
        ids.first().map(|(_, id)| id)
    } else {
        index?.get(topic.as_ref())
    };
    id.filter(|id| **id != [0; 16])
}

/// First name for each non-zero topic id.
fn fill_ids_by_uuid(ids: &[(Arc<str>, [u8; 16])], index: &mut HashMap<[u8; 16], Arc<str>>) {
    index.clear();
    for (name, id) in ids {
        if *id != [0; 16] && !index.contains_key(id) {
            let _ = index.insert(*id, Arc::clone(name));
        }
    }
}

/// First acknowledgement for one sent partition.
///
/// A name-based response with a non-UTF-8 topic is an error. A later copy of
/// the same partition is ignored, matching the indexed acknowledgement path.
fn first_matching_ack(
    version: i16,
    acks: &[ProduceAck],
    topic: &Arc<str>,
    partition: i32,
    topic_ids: &[(Arc<str>, [u8; 16])],
    by_uuid: &HashMap<[u8; 16], Arc<str>>,
) -> Result<Option<usize>> {
    for (ack_at, ack) in acks.iter().enumerate() {
        let name = if version >= 13 {
            let index = (topic_ids.len() > 1).then_some(by_uuid);
            name_for_topic_id(topic_ids, index, ack.topic_id)
        } else {
            Some(
                std::str::from_utf8(&ack.topic)
                    .map_err(|_| Error::protocol("produce response topic is not utf-8"))?,
            )
        };
        if ack.partition == partition && name.is_some_and(|name| name == topic.as_ref()) {
            return Ok(Some(ack_at));
        }
    }
    Ok(None)
}

fn name_for_topic_id<'a>(
    ids: &'a [(Arc<str>, [u8; 16])],
    index: Option<&'a HashMap<[u8; 16], Arc<str>>>,
    id: [u8; 16],
) -> Option<&'a str> {
    if id == [0; 16] {
        return None;
    }
    if ids.len() == 1 {
        return ids
            .first()
            .and_then(|(name, sent)| (*sent == id).then_some(name.as_ref()));
    }
    index?.get(&id).map(|name| name.as_ref())
}

fn sole_partition_acks(partition: i32, sent: &[u8; 16], acks: &[ProduceAck]) -> Result<()> {
    let mut matched = false;
    for ack in acks {
        if *sent == [0; 16] || ack.topic_id == [0; 16] || ack.topic_id != *sent {
            return Err(Error::protocol(
                "Produce v13 response UUID is absent from sent snapshot",
            ));
        }
        if ack.partition != partition || matched {
            return Err(Error::protocol(
                "Produce v13 unsolicited or duplicate partition response",
            ));
        }
        matched = true;
    }
    Ok(())
}

fn clear_ack_parts(parts: &mut HashMap<Arc<str>, HashSet<i32>>) {
    for partitions in parts.values_mut() {
        partitions.clear();
    }
}

fn fill_ack_topics(
    groups: &[(Arc<str>, i32, Vec<Pending>)],
    name: Option<&Arc<str>>,
    requested: &mut HashMap<Arc<str>, HashSet<i32>>,
) {
    clear_ack_parts(requested);
    for (topic, partition, _) in groups {
        if let Some(name) = name {
            if !Arc::ptr_eq(topic, name) && topic.as_ref() != name.as_ref() {
                continue;
            }
        }
        if let Some(parts) = requested.get_mut(topic.as_ref()) {
            let _ = parts.insert(*partition);
            continue;
        }
        let mut parts = HashSet::new();
        let _ = parts.insert(*partition);
        let _ = requested.insert(Arc::clone(topic), parts);
    }
}

fn ack_partition_is_new(
    seen: &mut HashMap<Arc<str>, HashSet<i32>>,
    topic: &Arc<str>,
    partition: i32,
) -> bool {
    if let Some(parts) = seen.get_mut(topic.as_ref()) {
        return parts.insert(partition);
    }
    let mut parts = HashSet::new();
    let _ = parts.insert(partition);
    let _ = seen.insert(Arc::clone(topic), parts);
    true
}

fn solicited_v13_acks(
    groups: &[(Arc<str>, i32, Vec<Pending>)],
    acks: &[ProduceAck],
    by_id: &HashMap<[u8; 16], Arc<str>>,
    requested: &mut HashMap<Arc<str>, HashSet<i32>>,
    seen: &mut HashMap<Arc<str>, HashSet<i32>>,
) -> Result<()> {
    fill_ack_topics(groups, None, requested);
    clear_ack_parts(seen);
    for ack in acks {
        let Some(name) = by_id.get(&ack.topic_id) else {
            return Err(Error::protocol(
                "Produce v13 response UUID is absent from sent snapshot",
            ));
        };
        let known = requested
            .get(name.as_ref())
            .is_some_and(|parts| parts.contains(&ack.partition));
        if !known || !ack_partition_is_new(seen, name, ack.partition) {
            return Err(Error::protocol(
                "Produce v13 unsolicited or duplicate partition response",
            ));
        }
    }
    Ok(())
}

fn reserve_len<T>(items: &mut Vec<T>, count: usize) {
    if items.capacity() < count {
        items.reserve(count - items.capacity());
    }
}

fn index_produce_groups(
    groups: &[(Arc<str>, i32, Vec<Pending>)],
    scratch: &mut ProduceGroupScratch,
) {
    scratch.topics.clear();
    for parts in &mut scratch.parts {
        parts.clear();
    }
    // Steady-state batches share one topic allocation, so the name map is unused.
    // The pointer check and the index fill are the same walk.
    if let Some((first, _, _)) = groups.first() {
        if scratch.parts.is_empty() {
            scratch.parts.push(Vec::new());
        }
        let mut same = true;
        if let Some(parts) = scratch.parts.first_mut() {
            reserve_len(parts, groups.len());
            for (index, (topic, _, _)) in groups.iter().enumerate() {
                if !Arc::ptr_eq(topic, first) {
                    same = false;
                    break;
                }
                parts.push(index);
            }
            if !same {
                parts.clear();
            }
        }
        if same {
            scratch.topics.push(0);
            return;
        }
    }
    scratch.topic_slots.clear();
    reserve_len(&mut scratch.topics, groups.len());
    for (index, (topic, _, _)) in groups.iter().enumerate() {
        if let Some(&slot) = scratch.topic_slots.get(topic.as_ref()) {
            if let Some(parts) = scratch.parts.get_mut(slot) {
                parts.push(index);
            }
            continue;
        }
        let slot = scratch.topics.len();
        scratch.topics.push(index);
        if scratch.parts.len() == slot {
            scratch.parts.push(Vec::new());
        }
        if let Some(parts) = scratch.parts.get_mut(slot) {
            parts.push(index);
        }
        let _ = scratch.topic_slots.insert(Arc::clone(topic), slot);
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
    topic_ids: &[(Arc<str>, [u8; 16])],
    compression: Compression,
    now: i64,
    producer_id: i64,
    producer_epoch: i16,
    transactional_id: Option<&str>,
    scratch: &mut CompressScratch,
    groups_scratch: &mut ProduceGroupScratch,
) -> Result<()> {
    if version < 7 && compression.id() == 4 {
        return Err(Error::Unsupported(
            "zstd produce requires Produce v7 or newer".into(),
        ));
    }
    // v9–v12 share this compact request layout (v10+ CurrentLeader is
    // response-only; v12 transaction V2 is Produce-does-AddPartitionsToTxn).
    // v13 substitutes the retained UUID for each compact topic name.
    // Must stay in sync with the identity-aware and name-based protocol helpers.
    let flexible = version >= 9;
    let transactional = transactional_id.is_some();
    if version >= 3 {
        crate::protocol::buf::put_string(buf, flexible, transactional_id)?;
    }
    buf.put_i16(acks);
    buf.put_i32(timeout_ms);
    index_produce_groups(groups, groups_scratch);
    crate::protocol::buf::put_array_len(buf, flexible, Some(groups_scratch.topics.len()))?;
    if version >= 13 && topic_ids.len() > 1 {
        fill_topic_ids(topic_ids, &mut groups_scratch.topic_ids);
    }
    let topic_count = groups_scratch.topics.len();
    for topic_slot in 0..topic_count {
        let Some(group_at) = groups_scratch.topics.get(topic_slot).copied() else {
            continue;
        };
        let Some((topic, _, _)) = groups.get(group_at) else {
            continue;
        };
        if version >= 13 {
            let indexed = (topic_ids.len() > 1).then_some(&groups_scratch.topic_ids);
            let id = topic_id_for(topic_ids, indexed, topic).ok_or_else(|| {
                Error::Unsupported(format!("Produce v13 missing topic ID for {topic}"))
            })?;
            buf.extend_from_slice(id);
        } else {
            crate::protocol::buf::put_string(buf, flexible, Some(topic.as_ref()))?;
        }
        let part_count = groups_scratch
            .parts
            .get(topic_slot)
            .map(Vec::len)
            .unwrap_or(0);
        crate::protocol::buf::put_array_len(buf, flexible, Some(part_count))?;
        for part_slot in 0..part_count {
            let Some(i) = groups_scratch
                .parts
                .get(topic_slot)
                .and_then(|parts| parts.get(part_slot))
                .copied()
            else {
                continue;
            };
            let Some((_, partition, pendings)) = groups.get(i) else {
                continue;
            };
            buf.put_i32(*partition);
            let base_sequence = pendings.first().and_then(|p| p.seq).unwrap_or(
                if producer_id <= RecordBatch::NO_PRODUCER_ID {
                    RecordBatch::NO_SEQUENCE
                } else {
                    0
                },
            );
            if flexible {
                let mark = buf.len();
                if let Err(error) = encode_flexible_partition(
                    buf,
                    pendings,
                    compression,
                    now,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                    transactional,
                    scratch,
                ) {
                    buf.truncate(mark);
                    return Err(error);
                }
                crate::protocol::buf::put_empty_tagged_fields(buf);
            } else {
                let mark = buf.len();
                if let Err(error) = encode_classic_partition(
                    buf,
                    pendings,
                    compression,
                    now,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                    transactional,
                    scratch,
                ) {
                    buf.truncate(mark);
                    return Err(error);
                }
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

#[expect(
    clippy::too_many_arguments,
    reason = "flexible partition encode shares the produce batch knobs"
)]
fn encode_flexible_partition(
    buf: &mut BytesMut,
    pendings: &[Pending],
    compression: Compression,
    now: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    transactional: bool,
    scratch: &mut CompressScratch,
) -> Result<()> {
    if compression == Compression::None {
        let header = pending_batch_header(
            pendings,
            compression,
            now,
            producer_id,
            producer_epoch,
            base_sequence,
            transactional,
        )?;
        let batch_len = crate::protocol::records::uncompressed_batch_len(
            &header,
            pendings.iter().map(|pending| EncodeRecord {
                timestamp: pending.rec.timestamp.unwrap_or(now),
                key: pending.rec.key.as_deref(),
                value: pending.rec.value.as_deref(),
                headers: &pending.rec.headers,
            }),
        )?;
        let compact = crate::protocol::buf::u32_from_usize(batch_len)?
            .checked_add(1)
            .ok_or_else(|| Error::protocol("compact bytes overflow"))?;
        reserve_produce_tail(
            buf,
            crate::protocol::buf::unsigned_varint_size(compact).saturating_add(batch_len),
        );
        crate::protocol::buf::put_unsigned_varint(buf, compact);
        let start = buf.len();
        encode_pendings(buf, pendings, &header, now, scratch)?;
        if buf.len().saturating_sub(start) != batch_len {
            return Err(Error::protocol("uncompressed batch length mismatch"));
        }
        return Ok(());
    }
    let header = pending_batch_header(
        pendings,
        compression,
        now,
        producer_id,
        producer_epoch,
        base_sequence,
        transactional,
    )?;
    reserve_compressed_section(&header, pendings, now, scratch)?;
    let batch_len = crate::protocol::records::pack_compressed_batch(
        &header,
        pendings.iter().map(|pending| EncodeRecord {
            timestamp: pending.rec.timestamp.unwrap_or(now),
            key: pending.rec.key.as_deref(),
            value: pending.rec.value.as_deref(),
            headers: &pending.rec.headers,
        }),
        scratch,
    )?;
    let compact = crate::protocol::buf::u32_from_usize(batch_len)?
        .checked_add(1)
        .ok_or_else(|| Error::protocol("compact bytes overflow"))?;
    reserve_produce_tail(
        buf,
        crate::protocol::buf::unsigned_varint_size(compact).saturating_add(batch_len),
    );
    crate::protocol::buf::put_unsigned_varint(buf, compact);
    let start = buf.len();
    crate::protocol::records::write_packed_batch(buf, &header, scratch)?;
    if buf.len().saturating_sub(start) != batch_len {
        return Err(Error::protocol("compressed batch length mismatch"));
    }
    scratch.shrink_to_caps();
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "classic partition encode shares the produce batch knobs"
)]
fn encode_classic_partition(
    buf: &mut BytesMut,
    pendings: &[Pending],
    compression: Compression,
    now: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    transactional: bool,
    scratch: &mut CompressScratch,
) -> Result<()> {
    let header = pending_batch_header(
        pendings,
        compression,
        now,
        producer_id,
        producer_epoch,
        base_sequence,
        transactional,
    )?;
    let records = || {
        pendings.iter().map(|pending| EncodeRecord {
            timestamp: pending.rec.timestamp.unwrap_or(now),
            key: pending.rec.key.as_deref(),
            value: pending.rec.value.as_deref(),
            headers: &pending.rec.headers,
        })
    };
    let batch_len = if compression == Compression::None {
        crate::protocol::records::uncompressed_batch_len(&header, records())?
    } else {
        reserve_compressed_section(&header, pendings, now, scratch)?;
        crate::protocol::records::pack_compressed_batch(&header, records(), scratch)?
    };
    let wire_len = crate::protocol::buf::i32_from_usize(batch_len)?;
    reserve_produce_tail(buf, 4usize.saturating_add(batch_len));
    buf.put_i32(wire_len);
    let start = buf.len();
    if compression == Compression::None {
        encode_pendings(buf, pendings, &header, now, scratch)?;
    } else {
        crate::protocol::records::write_packed_batch(buf, &header, scratch)?;
        scratch.shrink_to_caps();
    }
    if buf.len().saturating_sub(start) != batch_len {
        return Err(Error::protocol("classic batch length mismatch"));
    }
    Ok(())
}

fn pending_batch_header(
    pendings: &[Pending],
    compression: Compression,
    now: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    transactional: bool,
) -> Result<BatchHeader> {
    let base_ts = pendings
        .first()
        .and_then(|pending| pending.rec.timestamp)
        .unwrap_or(now);
    let max_ts = pendings
        .iter()
        .map(|pending| pending.rec.timestamp.unwrap_or(now))
        .max()
        .unwrap_or(base_ts);
    Ok(BatchHeader {
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
    })
}

fn sequence_in(parts: &mut HashMap<i32, i32>, partition: i32, count: usize) -> i32 {
    let added = i32::try_from(count).unwrap_or(i32::MAX);
    if let Some(slot) = parts.get_mut(&partition) {
        let base = *slot;
        *slot = slot.saturating_add(added);
        return base;
    }
    let _ = parts.insert(partition, added);
    0
}

fn sequence_of(map: &mut SequenceMap, topic: &Arc<str>, partition: i32, count: usize) -> i32 {
    if let Some(parts) = map.get_mut(topic.as_ref()) {
        return sequence_in(parts, partition, count);
    }
    let added = i32::try_from(count).unwrap_or(i32::MAX);
    let mut parts = HashMap::new();
    let _ = parts.insert(partition, added);
    let _ = map.insert(Arc::clone(topic), parts);
    0
}

fn reserve_produce_tail(buf: &mut BytesMut, extra: usize) {
    let needed = buf.len().saturating_add(extra);
    if buf.capacity() < needed {
        buf.reserve(needed - buf.capacity());
    }
}

fn reserve_compressed_section(
    header: &BatchHeader,
    pendings: &[Pending],
    now: i64,
    scratch: &mut CompressScratch,
) -> Result<()> {
    if Compression::from_attributes(header.attributes)? == Compression::None {
        return Ok(());
    }
    let section = crate::protocol::records::uncompressed_section_len(
        header,
        pendings.iter().map(|pending| EncodeRecord {
            timestamp: pending.rec.timestamp.unwrap_or(now),
            key: pending.rec.key.as_deref(),
            value: pending.rec.value.as_deref(),
            headers: &pending.rec.headers,
        }),
    )?;
    scratch.reserve_section(section);
    Ok(())
}

fn encode_pendings(
    buf: &mut BytesMut,
    pendings: &[Pending],
    header: &BatchHeader,
    now: i64,
    scratch: &mut CompressScratch,
) -> Result<()> {
    reserve_compressed_section(header, pendings, now, scratch)?;
    write_record_batch_scratch(
        buf,
        header,
        pendings.iter().map(|p| EncodeRecord {
            timestamp: p.rec.timestamp.unwrap_or(now),
            key: p.rec.key.as_deref(),
            value: p.rec.value.as_deref(),
            headers: &p.rec.headers,
        }),
        scratch,
    )
}

fn complete_acks0(
    shared: &Shared,
    groups: impl IntoIterator<Item = (Arc<str>, i32, Vec<Pending>)>,
    ack_tracker: &mut Option<(Arc<str>, Arc<crate::metrics::ProduceTopicTracker>)>,
) {
    for (topic, part, pendings) in groups {
        shared.retire_sticky(&pendings, false);
        shared.release_buffer(pendings_bytes(&pendings));
        let n = u64::try_from(pendings.len()).unwrap_or(u64::MAX);
        let tracker = shared.topic_tracker_cached(ack_tracker, &topic);
        shared.note_acked_on(&tracker, n);
        // KL09-18: one clock read per batch; per-record latencies
        // fold locally (no atomics, no lookups).
        let ack = Instant::now();
        let sample_ns = pendings
            .first()
            .map(|p| ack_latency_ns(ack, p.queued_at))
            .unwrap_or(0);
        let mut lat_n = 0u64;
        let mut lat_sum = 0u64;
        let mut lat_min = u64::MAX;
        let mut lat_max = 0u64;
        let mut stamped = None;
        for p in pendings {
            let ns = ack_latency_ns(ack, p.queued_at);
            lat_n += 1;
            lat_sum = lat_sum.saturating_add(ns);
            lat_min = lat_min.min(ns);
            lat_max = lat_max.max(ns);
            // KL09-16: build ack metadata only when a caller oneshot
            // or an interceptor consumes it.
            if p.tx.is_none() && shared.interceptors.is_empty() {
                continue;
            }
            let now = *stamped.get_or_insert_with(now_ms);
            let md = record_metadata(
                &topic,
                part,
                RecordMetadata::INVALID_OFFSET,
                0,
                &p.rec,
                RecordBatch::NO_TIMESTAMP,
                now,
            );
            shared.interceptors.on_ack(&md);
            if let Some(tx) = p.tx {
                drop(tx.send(Ok(md)));
            }
        }
        shared.note_ack_latency_on(
            &tracker,
            crate::metrics::BatchLatency {
                n: lat_n,
                sum_nanos: lat_sum,
                min_nanos: lat_min,
                max_nanos: lat_max,
                sample_ns,
            },
        );
    }
}

fn fail_groups(
    shared: &Shared,
    groups: impl IntoIterator<Item = (Arc<str>, i32, Vec<Pending>)>,
    err: Error,
) {
    for (_, _, pendings) in groups {
        fail_pendings(shared, pendings, clone_err(&err));
    }
}

fn fail_pendings(shared: &Shared, pendings: Vec<Pending>, err: Error) {
    let _empty = fail_kept_pendings(shared, pendings, err);
}

/// Fail every record and return the emptied batch so its allocation can be reused.
fn fail_kept_pendings(shared: &Shared, mut pendings: Vec<Pending>, err: Error) -> Vec<Pending> {
    shared.retire_sticky(&pendings, true);
    shared.release_buffer(pendings_bytes(&pendings));
    let one_topic = pendings.first().is_some_and(|first| {
        pendings
            .iter()
            .all(|pending| Arc::ptr_eq(&pending.rec.topic, &first.rec.topic))
    });
    if one_topic {
        if let Some(first) = pendings.first() {
            let n = u64::try_from(pendings.len()).unwrap_or(u64::MAX);
            shared.note_errors(&first.rec.topic, n);
        }
    }
    for pending in pendings.drain(..) {
        if !one_topic {
            shared.note_errors(&pending.rec.topic, 1);
        }
        shared.interceptors.on_error(&err);
        if let Some(tx) = pending.tx {
            drop(tx.send(Err(clone_err(&err))));
        }
    }
    pendings
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

fn publish_meta_err(shared: &Shared, err: Option<Error>) {
    let mut slot = shared.last_meta_err.lock();
    let present = err.is_some();
    *slot = err;
    shared.meta_err_set.store(present, Ordering::Release);
}

fn peek_meta_err(shared: &Shared) -> Option<Error> {
    if !shared.meta_err_set.load(Ordering::Acquire) {
        return None;
    }
    shared.last_meta_err.lock().as_ref().map(clone_err)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn serialized_bytes_size(bytes: Option<&Bytes>) -> i32 {
    bytes
        .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
        .unwrap_or(-1)
}

fn record_metadata(
    topic: &Arc<str>,
    partition: i32,
    base_offset: i64,
    batch_index: i32,
    rec: &ProduceRecord,
    log_append_time_ms: i64,
    now_ms: i64,
) -> RecordMetadata {
    let timestamp = if log_append_time_ms >= 0 {
        log_append_time_ms
    } else {
        rec.timestamp.unwrap_or(now_ms)
    };
    let offset = if base_offset == RecordMetadata::INVALID_OFFSET {
        base_offset
    } else {
        base_offset + i64::from(batch_index)
    };
    RecordMetadata {
        topic: Arc::clone(topic),
        partition,
        offset,
        timestamp,
        serialized_key_size: serialized_bytes_size(rec.key.as_ref()),
        serialized_value_size: serialized_bytes_size(rec.value.as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queued(topic: &str, partition: Option<i32>) -> Pending {
        let now = Instant::now();
        let mut rec = ProduceRecord::to(topic);
        if let Some(partition) = partition {
            rec = rec.partition(partition);
        }
        Pending {
            sticky_record: None,
            sticky: None,
            rec,
            tx: None,
            seq: None,
            batch_base_seq: None,
            batch_len: 1,
            epoch_gen: 0,
            deadline: now,
            queued_at: now,
            retry: 0,
            skip_meta_refresh: false,
            retry_after: now,
        }
    }

    #[test]
    fn an_uncompressed_produce_sizes_its_batch_once() {
        let topic = Arc::<str>::from("orders");
        let pendings: Vec<_> = (0..32)
            .map(|_| {
                let mut pending = queued("orders", Some(0));
                pending.rec.topic = Arc::clone(&topic);
                pending.rec.key = Some(Bytes::from_static(b"key"));
                pending.rec.value = Some(Bytes::from(vec![b'v'; 64]));
                pending
            })
            .collect();
        let groups = [(Arc::clone(&topic), 0, pendings)];
        let mut scratch = CompressScratch::default();
        let mut grouped = ProduceGroupScratch::default();
        for version in [3_i16, 9] {
            let mut buf = BytesMut::new();
            encode_produce_body(
                &mut buf,
                version,
                1,
                1_000,
                &groups,
                &[],
                Compression::None,
                5,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                None,
                &mut scratch,
                &mut grouped,
            )
            .unwrap();
            let len = buf.len();
            let capacity = buf.capacity();
            assert!(capacity >= len);
            assert!(capacity < 64 * 1024);
            buf.clear();
            encode_produce_body(
                &mut buf,
                version,
                1,
                1_000,
                &groups,
                &[],
                Compression::None,
                5,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                None,
                &mut scratch,
                &mut grouped,
            )
            .unwrap();
            assert_eq!(buf.len(), len);
            assert_eq!(buf.capacity(), capacity);
        }
    }

    #[test]
    fn a_classic_gzip_produce_sizes_its_batch_once() {
        let topic = Arc::<str>::from("orders");
        let pendings: Vec<_> = (0..8)
            .map(|_| {
                let mut pending = queued("orders", Some(0));
                pending.rec.topic = Arc::clone(&topic);
                pending.rec.value = Some(Bytes::from(vec![b'v'; 32]));
                pending
            })
            .collect();
        let groups = [(topic, 0, pendings)];
        let mut scratch = CompressScratch::default();
        let mut grouped = ProduceGroupScratch::default();
        let mut buf = BytesMut::new();
        encode_produce_body(
            &mut buf,
            3,
            1,
            1_000,
            &groups,
            &[],
            Compression::Gzip,
            5,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            None,
            &mut scratch,
            &mut grouped,
        )
        .unwrap();
        let len = buf.len();
        let capacity = buf.capacity();
        assert!(capacity >= len);
        assert!(capacity < 64 * 1024);
        buf.clear();
        encode_produce_body(
            &mut buf,
            3,
            1,
            1_000,
            &groups,
            &[],
            Compression::Gzip,
            5,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            None,
            &mut scratch,
            &mut grouped,
        )
        .unwrap();
        assert_eq!(buf.len(), len);
        assert_eq!(buf.capacity(), capacity);
    }

    #[test]
    fn uncompressed_produce_writes_the_sized_batch_header() {
        let pending = queued("orders", Some(0));
        let groups = [(Arc::clone(&pending.rec.topic), 0, vec![pending])];
        let mut scratch = CompressScratch::default();
        let mut grouped = ProduceGroupScratch::default();
        for version in [3_i16, 9] {
            let mut buf = BytesMut::new();
            encode_produce_body(
                &mut buf,
                version,
                1,
                1_000,
                &groups,
                &[],
                Compression::None,
                5,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
                None,
                &mut scratch,
                &mut grouped,
            )
            .unwrap();
            assert!(buf.len() > 12, "v{version} produce body");
        }
    }

    #[test]
    fn retry_groups_share_a_topic_and_keep_the_larger_attempt() {
        let mut orders = queued("orders", Some(0));
        let mut payments = queued("payments", Some(1));
        let mut orders_again = queued("orders", Some(2));
        orders.retry = 0;
        payments.retry = 2;
        orders_again.retry = 5;
        let (group_of, groups) = retry_topic_groups(&[orders, payments, orders_again]);
        assert_eq!(group_of, vec![0, 1, 0]);
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .first()
                .map(|(topic, retry)| (topic.as_ref(), *retry)),
            Some(("orders", 6))
        );
        assert_eq!(
            groups.get(1).map(|(topic, retry)| (topic.as_ref(), *retry)),
            Some(("payments", 3))
        );
    }

    #[test]
    fn transactional_offsets_group_by_first_seen_topic() {
        let offsets = [
            (
                crate::TopicPartition::new("orders", 1),
                crate::OffsetAndMetadata::from_wire(4, 7, "a".into()),
            ),
            (
                crate::TopicPartition::new("payments", 0),
                crate::OffsetAndMetadata::from_wire(1, 2, String::new()),
            ),
            (
                crate::TopicPartition::new("orders", 0),
                crate::OffsetAndMetadata::from_wire(9, 7, "b".into()),
            ),
        ];
        let names = unique_offset_topics(&offsets);
        assert_eq!(names.first().copied(), Some("orders"));
        assert_eq!(names.get(1).copied(), Some("payments"));
        assert_eq!(names.len(), 2);
        let grouped = group_txn_offsets(&offsets, &crate::cluster::Cluster::default());
        assert_eq!(grouped.first().map(|topic| topic.topic), Some("orders"));
        assert_eq!(
            grouped.first().map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| {
                        (
                            partition.partition,
                            partition.offset,
                            partition.leader_epoch,
                        )
                    })
                    .collect::<Vec<_>>()
            }),
            Some(vec![(1, 4, 7), (0, 9, 7)])
        );
        assert_eq!(grouped.get(1).map(|topic| topic.topic), Some("payments"));
        assert_eq!(
            grouped
                .get(1)
                .and_then(|topic| topic.partitions.first())
                .map(|partition| { (partition.partition, partition.offset, partition.metadata,) }),
            Some((0, 1, ""))
        );
    }

    #[test]
    fn produce_topic_ids_keep_the_first_name_and_reject_a_shared_uuid() {
        let orders = Arc::<str>::from("orders");
        let orders_again = Arc::<str>::from("orders");
        let payments = Arc::<str>::from("payments");
        let id_a = [1u8; 16];
        let id_b = [2u8; 16];
        let kept = collect_produce_topic_ids([
            (&orders, Some(id_a)),
            (&orders_again, Some(id_b)),
            (&payments, Some(id_b)),
        ]);
        assert_eq!(kept.as_ref().map(Vec::len), Ok(2));
        assert_eq!(
            kept.as_ref()
                .ok()
                .and_then(|ids| ids.first())
                .map(|(topic, id)| (topic.as_ref(), *id)),
            Some(("orders", id_a))
        );
        assert_eq!(
            kept.as_ref()
                .ok()
                .and_then(|ids| ids.get(1))
                .map(|(topic, id)| (topic.as_ref(), *id)),
            Some(("payments", id_b))
        );
        assert_eq!(
            collect_produce_topic_ids([(&orders, Some(id_a)), (&payments, Some(id_a))])
                .as_ref()
                .map_err(String::as_str),
            Err("payments (ambiguous UUID)")
        );
        assert_eq!(
            collect_produce_topic_ids([(&payments, None)])
                .as_ref()
                .map_err(String::as_str),
            Err("payments")
        );
        assert_eq!(
            collect_produce_topic_ids([(&orders, Some([0; 16]))])
                .as_ref()
                .map(Vec::len),
            Ok(1)
        );
    }

    #[test]
    fn transactional_partitions_share_one_topic_entry() {
        let mut set = TxnPartSet::new();
        let orders = Arc::<str>::from("orders");
        let orders_again = Arc::<str>::from("orders");
        remember_txn_part(&mut set, &orders, 0);
        remember_txn_part(&mut set, &orders_again, 1);
        remember_txn_part(&mut set, &orders, 0);
        assert_eq!(set.len(), 1);
        assert!(txn_part_present(&set, &orders_again, 0));
        assert!(txn_part_present(&set, &orders, 1));
        assert!(!txn_part_present(&set, &orders, 2));
        let payments = Arc::<str>::from("payments");
        remember_txn_part_owned(&mut set, Arc::clone(&payments), 3);
        assert_eq!(set.len(), 2);
        assert!(txn_part_present(&set, &payments, 3));
        forget_txn_part(&mut set, &orders, 0);
        assert!(!txn_part_present(&set, &orders, 0));
        assert!(txn_part_present(&set, &orders, 1));
        forget_txn_part(&mut set, &orders_again, 1);
        assert!(set.get("orders").is_none());
        assert!(txn_part_present(&set, &payments, 3));
        let parts = [
            (Arc::clone(&orders), 1),
            (Arc::clone(&payments), 0),
            (Arc::clone(&orders), 0),
        ];
        let grouped = group_txn_partitions(&parts);
        assert_eq!(grouped.first().map(|topic| topic.topic), Some("orders"));
        assert_eq!(
            grouped.first().map(|topic| topic.partitions.as_slice()),
            Some(&[1, 0][..])
        );
        assert_eq!(grouped.get(1).map(|topic| topic.topic), Some("payments"));
        assert_eq!(
            grouped.get(1).map(|topic| topic.partitions.as_slice()),
            Some(&[0][..])
        );
    }

    #[test]
    fn take_count_charges_one_group_overhead_for_one_partition() {
        let same = vec![queued("t", Some(0)), queued("t", Some(0))];
        let mixed = vec![queued("t", Some(0)), queued("t", Some(1))];
        let overhead = usize::try_from(RecordBatch::RECORD_BATCH_OVERHEAD).unwrap_or(0);
        let first = estimate(&same[0]);
        let second = estimate(&same[1]);
        let one = first.saturating_add(overhead);
        let two_same = one.saturating_add(second);
        let two_groups = two_same.saturating_add(overhead);
        assert_eq!(take_count(&same, 10, one), 1);
        assert_eq!(take_count(&same, 10, two_same), 2);
        assert_eq!(take_count(&mixed, 10, two_same), 1);
        assert_eq!(take_count(&mixed, 10, two_groups), 2);
        let mut split = same;
        split[0].batch_base_seq = Some(1);
        split[1].batch_base_seq = Some(2);
        assert_eq!(take_count(&split, 10, usize::MAX), 1);
        assert_eq!(take_count(&[], 10, usize::MAX), 0);
        let mut bases = HashMap::new();
        let mixed_topics = vec![
            queued("t", Some(0)),
            queued("t", Some(1)),
            queued("u", Some(0)),
        ];
        assert_eq!(take_count_in(&mixed_topics, 10, usize::MAX, &mut bases), 3);
        assert_eq!(bases.len(), 2);
        assert_eq!(take_count_in(&mixed_topics, 10, usize::MAX, &mut bases), 3);
        let mut same_base = vec![queued("t", Some(0)), queued("t", Some(0))];
        same_base[0].batch_base_seq = Some(4);
        same_base[1].batch_base_seq = Some(4);
        assert_eq!(take_count_in(&same_base, 10, usize::MAX, &mut bases), 2);
    }

    #[test]
    fn one_topic_produce_groups_share_its_allocation() {
        let mut scratch = ProduceGroupScratch::default();
        let mut batch = vec![
            queued("orders", Some(1)),
            queued("orders", Some(0)),
            queued("orders", Some(1)),
        ];
        let orders = Arc::clone(&batch[0].rec.topic);
        batch[1].rec.topic = Arc::clone(&orders);
        batch[2].rec.topic = Arc::<str>::from("orders");
        let (groups, shell) = group_pending(batch, &mut scratch);
        assert!(shell.is_empty());
        assert!(shell.capacity() >= 3);
        assert_eq!(groups.len(), 2);
        assert_eq!((groups[0].1, groups[0].2.len()), (1, 2));
        assert_eq!((groups[1].1, groups[1].2.len()), (0, 1));
        assert!(groups
            .iter()
            .all(|(topic, _, _)| Arc::ptr_eq(topic, &orders)));
        assert!(scratch.buckets.is_empty());

        let (one, shell) = group_pending(
            vec![queued("orders", Some(0)), queued("orders", Some(0))],
            &mut scratch,
        );
        assert!(shell.is_empty());
        assert_eq!(shell.capacity(), 0);
        let mut spare = Vec::new();
        let mut queued_batch = vec![queued("orders", Some(0)), queued("orders", Some(0))];
        let drained = fill_spare(&mut spare, &mut queued_batch, 2);
        assert!(queued_batch.is_empty());
        assert_eq!(drained.len(), 2);
        let batch_capacity = drained.capacity();
        assert!(batch_capacity > 0);
        park_spare(&mut spare, drained);
        assert!(spare.capacity() >= batch_capacity);
        let mut next = vec![queued("orders", Some(0)), queued("orders", Some(0))];
        let again = fill_spare(&mut spare, &mut next, 2);
        assert!(again.capacity() >= batch_capacity);
        assert!(spare.is_empty());

        assert!(matches!(one, ProduceGroups::One(_)));
        assert_eq!(one.len(), 1);
        assert_eq!(
            one.first().map(|(_, part, pending)| (*part, pending.len())),
            Some((0, 2))
        );
    }

    #[test]
    fn a_one_partition_produce_group_has_no_shell() {
        let pending = queued("topic-name-longer-than-twenty-three", Some(0));
        let topic = Arc::clone(&pending.rec.topic);
        let groups = ProduceGroups::One((topic, 0, vec![pending]));
        let OpenedProduceGroups { one, shell } = groups.into_one_many();
        assert!(one.is_some());
        assert_eq!(shell.capacity(), 0);
        let many = ProduceGroups::Many(vec![
            (Arc::from("orders"), 0, Vec::new()),
            (Arc::from("orders"), 1, Vec::new()),
        ]);
        let OpenedProduceGroups { one, shell } = many.into_one_many();
        assert!(one.is_none());
        assert!(shell.capacity() >= 2);
    }

    #[test]
    fn a_one_topic_produce_sizes_each_partition_list_once() {
        let mut scratch = ProduceGroupScratch::default();
        let batch = vec![
            queued("orders", Some(1)),
            queued("orders", Some(0)),
            queued("orders", Some(1)),
            queued("orders", Some(0)),
        ];
        let (groups, _) = group_pending(batch, &mut scratch);
        let ProduceGroups::Many(groups) = groups else {
            panic!("expected several partitions");
        };
        assert_eq!(groups.len(), 2);
        let first = groups
            .first()
            .map(|(_, _, records)| records.capacity())
            .unwrap_or(0);
        let second = groups
            .get(1)
            .map(|(_, _, records)| records.capacity())
            .unwrap_or(0);
        assert!(first >= 2);
        assert!(second >= 2);
        assert_eq!(
            groups
                .first()
                .map(|(_, partition, records)| (*partition, records.len())),
            Some((1, 2))
        );
        assert_eq!(
            groups
                .get(1)
                .map(|(_, partition, records)| (*partition, records.len())),
            Some((0, 2))
        );
        for (_, _, mut records) in groups {
            records.clear();
            scratch.spare_parts.push(records);
        }
        let (again, _) = group_pending(
            vec![
                queued("orders", Some(1)),
                queued("orders", Some(0)),
                queued("orders", Some(1)),
                queued("orders", Some(0)),
            ],
            &mut scratch,
        );
        let ProduceGroups::Many(again) = again else {
            panic!("expected several partitions");
        };
        assert_eq!(
            again.first().map(|(_, _, records)| records.capacity()),
            Some(first)
        );
        assert_eq!(
            again.get(1).map(|(_, _, records)| records.capacity()),
            Some(second)
        );
    }

    #[test]
    fn mixed_produce_groups_share_the_first_topic_name() {
        let mut scratch = ProduceGroupScratch::default();
        let mut batch = vec![
            queued("orders", Some(1)),
            queued("orders", Some(0)),
            queued("orders", Some(1)),
            queued("payments", Some(0)),
        ];
        let orders = Arc::clone(&batch[0].rec.topic);
        batch[2].rec.topic = Arc::clone(&orders);
        let payments = Arc::clone(&batch[3].rec.topic);
        let (groups, shell) = group_pending(batch, &mut scratch);
        assert!(shell.is_empty());
        assert!(shell.capacity() >= 4);
        assert_eq!(groups.len(), 3);
        assert_eq!(
            (groups[0].0.as_ref(), groups[0].1, groups[0].2.len()),
            ("orders", 1, 2)
        );
        assert!(Arc::ptr_eq(&groups[0].0, &orders));
        assert_eq!(
            (groups[1].0.as_ref(), groups[1].1, groups[1].2.len()),
            ("orders", 0, 1)
        );
        assert!(Arc::ptr_eq(&groups[1].0, &orders));
        assert_eq!(
            (groups[2].0.as_ref(), groups[2].1, groups[2].2.len()),
            ("payments", 0, 1)
        );
        assert!(Arc::ptr_eq(&groups[2].0, &payments));
        assert!(groups
            .first()
            .is_some_and(|(_, _, records)| records.capacity() >= 2));
        assert!(groups
            .get(1)
            .is_some_and(|(_, _, records)| records.capacity() >= 1));
        assert!(groups
            .get(2)
            .is_some_and(|(_, _, records)| records.capacity() >= 1));
        assert!(scratch.buckets.is_empty());

        let mut indexed = ProduceGroupScratch::default();
        index_produce_groups(&groups, &mut indexed);
        assert_eq!(indexed.topics, vec![0, 2]);
        assert!(indexed.topics.capacity() >= groups.len());
        assert_eq!(indexed.parts.first().map(Vec::as_slice), Some(&[0, 1][..]));
        assert_eq!(indexed.parts.get(1).map(Vec::as_slice), Some(&[2][..]));
        let topic_capacity = indexed.topics.capacity();
        let part_capacity = indexed.parts.first().map(Vec::capacity).unwrap_or(0);
        assert!(part_capacity >= groups.len());
        index_produce_groups(&groups, &mut indexed);
        assert_eq!(indexed.topics.capacity(), topic_capacity);
        assert_eq!(
            indexed.parts.first().map(Vec::capacity),
            Some(part_capacity)
        );

        let shared = vec![
            (Arc::clone(&orders), 1, Vec::new()),
            (Arc::clone(&orders), 0, Vec::new()),
            (Arc::clone(&orders), 2, Vec::new()),
        ];
        index_produce_groups(&shared, &mut indexed);
        assert_eq!(indexed.topics, vec![0]);
        assert_eq!(
            indexed.parts.first().map(Vec::as_slice),
            Some(&[0, 1, 2][..])
        );
        let shared_capacity = indexed.parts.first().map(Vec::capacity).unwrap_or(0);
        assert!(shared_capacity >= shared.len());
        index_produce_groups(&shared, &mut indexed);
        assert_eq!(
            indexed.parts.first().map(Vec::capacity),
            Some(shared_capacity)
        );

        let alias = Arc::<str>::from("orders");
        let aliased = vec![(Arc::clone(&orders), 0, Vec::new()), (alias, 3, Vec::new())];
        index_produce_groups(&aliased, &mut indexed);
        assert_eq!(indexed.topics, vec![0]);
        assert_eq!(indexed.parts.first().map(Vec::as_slice), Some(&[0, 1][..]));

        index_produce_groups(&[], &mut indexed);
        assert!(indexed.topics.is_empty());
        assert_eq!(indexed.parts.first().map(Vec::len), Some(0));

        let (again, shell) = group_pending(
            vec![queued("payments", Some(1)), queued("orders", Some(0))],
            &mut scratch,
        );
        assert!(shell.is_empty());
        assert!(shell.capacity() >= 2);
        assert_eq!(again.len(), 2);
        let mut reused = ProduceGroupScratch::default();
        reused.spare_parts.push(Vec::with_capacity(8));
        let (split, _) = group_pending(
            vec![queued("orders", Some(0)), queued("orders", Some(1))],
            &mut reused,
        );
        assert!(reused.spare_parts.is_empty());
        assert!(split
            .first()
            .is_some_and(|(_, partition, pending)| *partition == 0 && pending.capacity() >= 8));
        let mut grouped = ProduceGroupScratch::default();
        let mut shell = Vec::with_capacity(4);
        shell.push((Arc::<str>::from("old"), 0, Vec::new()));
        shell.clear();
        grouped.spare_groups = shell;
        let (groups, _) = group_pending(
            vec![queued("orders", Some(0)), queued("orders", Some(1))],
            &mut grouped,
        );
        assert!(grouped.spare_groups.is_empty());
        assert!(matches!(
            &groups,
            ProduceGroups::Many(items) if items.capacity() >= 4
        ));
        let mut mixed = ProduceGroupScratch::default();
        mixed.spare_slots.push(HashMap::with_capacity(8));
        mixed.spare_orders.push(Vec::with_capacity(4));
        let _ = group_pending(
            vec![queued("orders", Some(0)), queued("payments", Some(1))],
            &mut mixed,
        );
        assert_eq!(mixed.spare_slots.len(), 2);
        assert!(mixed.spare_slots.iter().any(|slots| slots.capacity() >= 8));
        assert_eq!(mixed.spare_orders.len(), 2);
        assert!(mixed.spare_orders.iter().any(|order| order.capacity() >= 4));
        assert!(mixed.spare_topic_order.capacity() >= 2);
        assert!(mixed.buckets.is_empty());
        assert_eq!((again[0].0.as_ref(), again[0].1), ("payments", 1));
        assert_eq!((again[1].0.as_ref(), again[1].1), ("orders", 0));
    }

    #[test]
    fn one_produce_topic_id_stays_inline() {
        let orders = Arc::<str>::from("orders");
        let one = ProduceTopicIds::One((Arc::clone(&orders), [1; 16]));
        assert!(matches!(one, ProduceTopicIds::One(_)));
        assert_eq!(one.len(), 1);
        assert_eq!(
            one.first().map(|(topic, id)| (topic.as_ref(), *id)),
            Some(("orders", [1; 16]))
        );
        let folded = ProduceTopicIds::from_vec(vec![(orders, [1; 16])]);
        assert!(matches!(folded, ProduceTopicIds::One(_)));
        assert!(matches!(
            ProduceTopicIds::from_vec(Vec::new()),
            ProduceTopicIds::Empty
        ));
        let many = ProduceTopicIds::from_vec(vec![
            (Arc::<str>::from("orders"), [1; 16]),
            (Arc::<str>::from("payments"), [2; 16]),
        ]);
        assert!(matches!(many, ProduceTopicIds::Many(_)));
        assert_eq!(many.len(), 2);
    }

    #[test]
    fn produce_topic_ids_keep_the_first_copy() {
        let orders = Arc::<str>::from("orders");
        let payments = Arc::<str>::from("payments");
        let ids = vec![
            (Arc::clone(&orders), [1; 16]),
            (Arc::clone(&payments), [2; 16]),
            (Arc::clone(&orders), [9; 16]),
            (Arc::<str>::from("zero"), [0; 16]),
        ];
        let mut by_name = HashMap::new();
        fill_topic_ids(&ids, &mut by_name);
        let name_capacity = by_name.capacity();
        assert!(name_capacity > 0);
        fill_topic_ids(&ids, &mut by_name);
        assert!(by_name.capacity() >= name_capacity);
        assert_eq!(
            topic_id_for(&ids, Some(&by_name), &orders).copied(),
            Some([1; 16])
        );
        assert_eq!(
            topic_id_for(&ids, Some(&by_name), &payments).copied(),
            Some([2; 16])
        );
        let zero = Arc::<str>::from("zero");
        assert_eq!(topic_id_for(&ids, Some(&by_name), &zero), None);
        let missing = Arc::<str>::from("missing");
        assert_eq!(topic_id_for(&ids, Some(&by_name), &missing), None);
        let one = vec![(Arc::clone(&orders), [4; 16])];
        assert_eq!(topic_id_for(&one, None, &orders).copied(), Some([4; 16]));
        let only_zero = vec![(Arc::clone(&payments), [0; 16])];
        assert_eq!(topic_id_for(&only_zero, None, &payments), None);

        let mut by_uuid = HashMap::new();
        fill_ids_by_uuid(&ids, &mut by_uuid);
        let uuid_capacity = by_uuid.capacity();
        assert!(uuid_capacity > 0);
        fill_ids_by_uuid(&ids, &mut by_uuid);
        assert!(by_uuid.capacity() >= uuid_capacity);
        assert_eq!(
            name_for_topic_id(&ids, Some(&by_uuid), [2; 16]),
            Some("payments")
        );
        assert_eq!(
            name_for_topic_id(&ids, Some(&by_uuid), [1; 16]),
            Some("orders")
        );
        assert_eq!(name_for_topic_id(&ids, Some(&by_uuid), [0; 16]), None);
        assert_eq!(
            name_for_topic_id(&ids, Some(&by_uuid), [9; 16]),
            Some("orders")
        );
        assert_eq!(name_for_topic_id(&one, None, [4; 16]), Some("orders"));
        assert_eq!(name_for_topic_id(&one, None, [1; 16]), None);
        let shared_id = vec![
            (Arc::clone(&orders), [7; 16]),
            (Arc::clone(&payments), [7; 16]),
        ];
        let mut shared = HashMap::new();
        fill_ids_by_uuid(&shared_id, &mut shared);
        assert_eq!(
            name_for_topic_id(&shared_id, Some(&shared), [7; 16]),
            Some("orders")
        );
    }

    #[test]
    fn produce_ack_partition_sets_are_reused() {
        let orders = Arc::<str>::from("orders");
        let payments = Arc::<str>::from("payments");
        let sent = [4u8; 16];
        let ack = |partition| ProduceAck {
            topic: Bytes::new(),
            topic_id: sent,
            partition,
            error_code: 0,
            base_offset: 0,
            log_append_time_ms: -1,
            current_leader_id: -1,
            current_leader_epoch: -1,
        };
        sole_partition_acks(0, &sent, &[ack(0)]).unwrap();
        assert!(sole_partition_acks(0, &sent, &[ack(0), ack(0)]).is_err());
        assert!(sole_partition_acks(0, &sent, &[ack(1)]).is_err());

        let groups = vec![
            (Arc::clone(&orders), 0, Vec::new()),
            (Arc::clone(&orders), 1, Vec::new()),
            (Arc::clone(&payments), 0, Vec::new()),
        ];
        let mut requested = HashMap::new();
        fill_ack_topics(&groups, None, &mut requested);
        let orders_capacity = requested
            .get(orders.as_ref())
            .map(HashSet::capacity)
            .unwrap_or(0);
        assert!(orders_capacity > 0);
        let only_orders = vec![
            (Arc::clone(&orders), 0, Vec::new()),
            (Arc::clone(&orders), 1, Vec::new()),
        ];
        fill_ack_topics(&only_orders, Some(&orders), &mut requested);
        let parts = requested.get(orders.as_ref()).unwrap();
        assert!(parts.capacity() >= orders_capacity);
        assert!(parts.contains(&0) && parts.contains(&1));
        assert!(requested
            .get(payments.as_ref())
            .is_none_or(|parts| parts.is_empty()));
    }

    #[test]
    fn a_single_partition_ack_matches_without_the_topic_index() {
        let orders = Arc::<str>::from("orders");
        let ack = |partition| ProduceAck {
            topic: Bytes::from_static(b"orders"),
            topic_id: [0; 16],
            partition,
            error_code: 0,
            base_offset: 8,
            log_append_time_ms: -1,
            current_leader_id: -1,
            current_leader_epoch: -1,
        };
        let ids = HashMap::new();
        let matched = first_matching_ack(9, &[ack(4), ack(3)], &orders, 3, &[], &ids).unwrap();
        assert_eq!(matched, Some(1));
        let again = Arc::<str>::from("orders");
        assert!(Shared::same_tracked_topic(&orders, &again));
        assert!(!Arc::ptr_eq(&orders, &again));
        let missing = first_matching_ack(9, &[], &orders, 3, &[], &ids).unwrap();
        assert_eq!(missing, None);
        let bad = ProduceAck {
            topic: Bytes::from_static(b"\xff"),
            topic_id: [0; 16],
            partition: 3,
            error_code: 0,
            base_offset: 0,
            log_append_time_ms: -1,
            current_leader_id: -1,
            current_leader_epoch: -1,
        };
        assert!(first_matching_ack(9, &[bad], &orders, 3, &[], &ids).is_err());
    }

    #[test]
    fn compact_payload_preserves_bytes_and_reuses_an_exact_unique_allocation() {
        let original = Bytes::from(vec![b'v'; 64]);
        let pointer = original.as_ptr();
        let compact = compact_payload(original);
        assert_eq!(compact.as_ptr(), pointer);
        let actual: Vec<u8> = compact.into();
        assert_eq!(actual, vec![b'v'; 64]);
        assert_eq!(actual.capacity(), actual.len());
    }

    #[test]
    fn compact_payload_drops_hidden_prefix_and_excess_tail_capacity() {
        let large = Bytes::from(vec![b'v'; 4 * 1024 * 1024]);
        let tail = large.slice(large.len() - 64..);
        drop(large);
        let actual: Vec<u8> = compact_payload(tail).into();
        assert_eq!(actual, vec![b'v'; 64]);
        assert_eq!(actual.capacity(), 64);
        let mut excessive = Vec::with_capacity(4 * 1024 * 1024);
        excessive.extend_from_slice(b"small");
        let actual: Vec<u8> = compact_payload(Bytes::from(excessive)).into();
        assert_eq!(actual, b"small");
        assert_eq!(actual.capacity(), 5);
    }

    #[test]
    fn compact_record_bounds_header_string_and_vector_capacity_preserving_nulls() {
        let mut headers = Vec::with_capacity(4096);
        let mut key = String::with_capacity(4 * 1024 * 1024);
        key.push_str("世界");
        headers.push(RecordHeader { key, value: None });
        headers.push(RecordHeader::new("empty", Bytes::new()));
        let mut record = ProduceRecord::to("t").value(Bytes::new()).headers(headers);
        compact_record_backing(&mut record);
        assert!(record.key.is_none());
        assert_eq!(record.value, Some(Bytes::new()));
        assert_eq!(record.headers.capacity(), 2);
        assert_eq!(record.headers[0].key, "世界");
        assert_eq!(record.headers[0].key.capacity(), "世界".len());
        assert!(record.headers[0].value.is_none());
        assert_eq!(record.headers[1].value, Some(Bytes::new()));
    }

    #[test]
    fn broker_throttle_extends_without_zero_or_negative_unmute() {
        let throttle = BrokerThrottle::default();
        assert_eq!(throttle.remaining(), None);
        throttle.observe(10_000);
        let first = *throttle.until.lock();
        for millis in [0, -1, i32::MIN, 1] {
            throttle.observe(millis);
        }
        assert_eq!(*throttle.until.lock(), first);
        throttle.observe(i32::MAX);
        assert!(throttle.remaining().unwrap() > Duration::from_secs(24 * 86400));
        *throttle.until.lock() = Some(Instant::now());
        assert_eq!(throttle.remaining(), None);
        assert!(!throttle.active.load(Ordering::Acquire));
        assert_eq!(*throttle.until.lock(), None);
    }

    #[test]
    fn ack_index_keeps_the_first_slot_and_drops_the_previous_response() {
        let topic = Arc::<str>::from("t");
        let groups = vec![
            (Arc::clone(&topic), 0, Vec::new()),
            (Arc::clone(&topic), 0, Vec::new()),
            (Arc::clone(&topic), 1, Vec::new()),
        ];
        let mut index = AckIndex::default();
        index.fill(&groups);
        assert_eq!(index.get("t", 0), Some(0));
        assert_eq!(index.get("t", 1), Some(2));
        let next = vec![(Arc::clone(&topic), 1, Vec::new())];
        index.fill(&next);
        assert_eq!(index.get("t", 1), Some(0));
        assert_eq!(index.get("t", 0), None);
        assert_eq!(index.topics.get("t").map(|parts| parts.len()), Some(1));
        assert!(Arc::ptr_eq(
            index
                .topics
                .get_key_value("t")
                .map(|(name, _)| name)
                .unwrap(),
            &topic
        ));
    }

    #[test]
    fn record_metadata_shares_the_produce_topic() {
        let rec = ProduceRecord::to("events");
        let md = record_metadata(&rec.topic, 1, 4, 2, &rec, -1, 5);
        assert!(Arc::ptr_eq(&md.topic, &rec.topic));
        assert_eq!(md.offset(), 6);
        assert_eq!(md.topic(), "events");
    }

    #[test]
    fn record_metadata_getters_match_java() {
        let md = RecordMetadata {
            topic: "events".into(),
            partition: 2,
            offset: 9,
            timestamp: 1_700_000_000_000,
            serialized_key_size: 3,
            serialized_value_size: 5,
        };
        assert_eq!(md.topic(), "events");
        assert_eq!(md.partition(), 2);
        assert_eq!(md.offset(), 9);
        assert!(md.has_offset());
        assert_eq!(md.timestamp(), 1_700_000_000_000);
        assert!(md.has_timestamp());
        assert_eq!(md.serialized_key_size(), 3);
        assert_eq!(md.serialized_value_size(), 5);
        assert_eq!(
            md.topic_partition(),
            crate::TopicPartition::new("events", 2)
        );
        assert_eq!(md.to_string(), "events-2@9");
        assert_eq!(RecordMetadata::UNKNOWN_PARTITION, -1);
        assert_eq!(RecordMetadata::INVALID_OFFSET, -1);
        assert_eq!(
            RecordMetadata::INVALID_OFFSET,
            crate::protocol::api::ProducePartitionResponse::INVALID_OFFSET
        );
        let acks0 = RecordMetadata {
            topic: "events".into(),
            partition: 0,
            offset: RecordMetadata::INVALID_OFFSET,
            timestamp: RecordBatch::NO_TIMESTAMP,
            serialized_key_size: -1,
            serialized_value_size: -1,
        };
        assert!(!acks0.has_offset());
        assert!(!acks0.has_timestamp());
        assert_eq!(acks0.to_string(), "events-0@-1");
        assert_eq!(
            ProduceRecord::to("t").to_string(),
            "ProducerRecord(topic=t, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=null, value=null, timestamp=null)"
        );
    }

    #[test]
    fn record_metadata_constructor_matches_java() {
        let tp = crate::TopicPartition::new("events", 2);
        let md = RecordMetadata::new(&tp, 10, 3, 1_700_000_000_000, 3, 5);
        assert_eq!(md.topic(), "events");
        assert_eq!(md.partition(), 2);
        assert_eq!(md.offset(), 13);
        assert!(md.has_offset());
        assert_eq!(md.timestamp(), 1_700_000_000_000);
        assert_eq!(md.serialized_key_size(), 3);
        assert_eq!(md.serialized_value_size(), 5);
        assert_eq!(md.to_string(), "events-2@13");

        let first = RecordMetadata::new(&tp, 10, 0, 1, -1, 4);
        assert_eq!(first.offset(), 10);
        assert_eq!(first.serialized_key_size(), -1);

        let unknown = RecordMetadata::new(
            &tp,
            RecordMetadata::INVALID_OFFSET,
            7,
            RecordBatch::NO_TIMESTAMP,
            -1,
            -1,
        );
        assert_eq!(unknown.offset(), RecordMetadata::INVALID_OFFSET);
        assert!(!unknown.has_offset());
        assert!(!unknown.has_timestamp());
        assert_eq!(unknown.to_string(), "events-2@-1");

        let subtracted = RecordMetadata::new(&tp, 10, -1, 0, 0, 0);
        assert_eq!(subtracted.offset(), 9);

        let zero_base = RecordMetadata::new(&tp, 0, 4, 0, 0, 0);
        assert_eq!(zero_base.offset(), 4);
    }

    #[test]
    fn produce_record_constructor_checks_match_java() {
        let part = reject_java_producer_record(&ProduceRecord::to("t").partition(-1))
            .unwrap_err()
            .to_string();
        assert!(
            part.contains(
                "Invalid partition: -1. Partition number should always be non-negative or null."
            ),
            "{part}"
        );
        let ts = reject_java_producer_record(&ProduceRecord::to("t").timestamp(-1))
            .unwrap_err()
            .to_string();
        assert!(
            ts.contains("Invalid timestamp: -1. Timestamp should always be non-negative or null."),
            "{ts}"
        );
        reject_java_producer_record(&ProduceRecord::to("t").partition(0).timestamp(0)).unwrap();
        reject_java_producer_record(&ProduceRecord::to("t")).unwrap();
        let empty = reject_java_producer_record(&ProduceRecord::to(""))
            .unwrap_err()
            .to_string();
        assert!(
            empty.contains("Topic name is invalid: the empty string is not allowed"),
            "{empty}"
        );
    }

    #[test]
    fn send_offsets_group_metadata_checks_match_java() {
        let bad = crate::ConsumerGroupMetadata {
            group_id: "g".into(),
            generation_id: 1,
            member_id: crate::ConsumerGroupMetadata::UNKNOWN_MEMBER_ID.into(),
            group_instance_id: None,
        };
        let err = reject_java_group_metadata(&bad).unwrap_err().to_string();
        assert!(
            err.contains(
                "Passed in group metadata GroupMetadata(groupId = g, generationId = 1, memberId = , groupInstanceId = ) has generationId > 0 but the member.id is unknown"
            ),
            "{err}"
        );
        reject_java_group_metadata(&crate::ConsumerGroupMetadata::new("g")).unwrap();
        reject_java_group_metadata(&crate::ConsumerGroupMetadata {
            group_id: "g".into(),
            generation_id: 1,
            member_id: "m".into(),
            group_instance_id: None,
        })
        .unwrap();
        reject_java_group_metadata(&crate::ConsumerGroupMetadata {
            group_id: "g".into(),
            generation_id: 0,
            member_id: crate::ConsumerGroupMetadata::UNKNOWN_MEMBER_ID.into(),
            group_instance_id: None,
        })
        .unwrap();
    }

    #[test]
    fn transactional_methods_without_transactional_id_match_java() {
        let err = reject_java_no_transaction_manager().to_string();
        assert!(
            err.contains(
                "Cannot use transactional methods without enabling transactions by setting the transactional.id configuration property"
            ),
            "{err}"
        );
    }
}
