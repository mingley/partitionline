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
use crate::partitioner::{to_positive, Partitioner, PartitionerBox};
use crate::protocol::api::{
    decode_metadata_response, decode_produce_response, encode_metadata_request, ApiVersion,
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
    write_java_optional, write_java_optional_bytes, write_java_record_headers, write_record_batch,
    BatchHeader, Compression, EncodeRecord, Header as RecordHeader, RecordBatch, Records,
};
use crate::protocol::txn::{
    can_handle_abortable_error, classify_add_offsets_error, classify_add_partitions_error,
    classify_end_txn_error, classify_produce_error, classify_txn_offset_commit_error,
    client_epoch_bump_capable, decode_add_offsets_to_txn_response,
    decode_add_partitions_to_txn_response, decode_end_txn_response,
    decode_txn_offset_commit_response, encode_add_offsets_to_txn_request,
    encode_add_partitions_to_txn_request, encode_end_txn_request, encode_txn_offset_commit_request,
    EndTxnRequest, TransactionResult, TxnErrorDisposition, TxnOffsetCommitMember,
    TxnOffsetCommitRequest, TxnOffsetPartition, TxnOffsetTopic, TxnPartitionsTopic,
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
    /// Kafka `buffer.memory`. Key plus value bytes of records queued and not
    /// yet acked. Default 32 MiB (Java). Zero means no client-side cap (the
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
        f.debug_struct("ProducerConfig")
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
    /// Topic name.
    pub topic: String,
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
            topic: tp.topic,
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
        self.topic.as_str()
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
        crate::TopicPartition::new(self.topic.clone(), self.partition)
    }
}

impl fmt::Display for RecordMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}@{}", self.topic, self.partition, self.offset)
    }
}

struct Pending {
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
}

#[derive(Clone)]
struct WorkerHandle {
    data: mpsc::Sender<Pending>,
    ctrl: mpsc::Sender<Ctrl>,
    task: Arc<tokio::task::JoinHandle<()>>,
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
    add_partitions_version: i16,
    add_offsets_version: i16,
    end_txn_version: i16,
    txn_offset_version: i16,
    find_coord_version: i16,
    init_producer_id_version: i16,
    telemetry_version: Option<i16>,
    client_instance_id: parking_lot::Mutex<Option<[u8; 16]>>,
    partitioner: Arc<dyn Partitioner>,
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
    seqs: parking_lot::Mutex<HashMap<(Arc<str>, i32), i32>>,
    cache_nudge: Notify,
    buffer_nudge: Notify,
    meta_tx: mpsc::Sender<Arc<str>>,
    connect_tx: mpsc::Sender<i32>,
    retry_tx: mpsc::Sender<Pending>,
    meta_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    connect_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    retry_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    last_meta_err: parking_lot::Mutex<Option<Error>>,
    nodes: parking_lot::Mutex<HashMap<i32, Vec<WorkerHandle>>>,
    reconnect_fails: parking_lot::Mutex<HashMap<i32, u32>>,
    /// Brokers with a connect or reconnect-backoff in flight.
    reconnect_busy: parking_lot::Mutex<HashSet<i32>>,
    retries_out: AtomicUsize,
    in_txn: AtomicBool,
    txn_partitions: parking_lot::Mutex<HashSet<(Arc<str>, i32)>>,
    txn_added: parking_lot::Mutex<HashSet<(Arc<str>, i32)>>,
    fast: parking_lot::Mutex<Option<FastRoute>>,
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
            .cloned()
            .collect();
        for w in &workers {
            w.task.abort();
        }
        self.shared.nodes.lock().clear();
        *self.shared.fast.lock() = None;

        if let Some(h) = self.shared.meta_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.shared.connect_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.shared.retry_task.lock().take() {
            h.abort();
        }

        self.shared.interceptors.close();
    }
}

impl Shared {
    fn topic_tracker(&self, topic: &Arc<str>) -> Arc<crate::metrics::ProduceTopicTracker> {
        let mut map = self.topics.lock();
        map.entry(Arc::clone(topic))
            .or_insert_with(|| Arc::new(crate::metrics::ProduceTopicTracker::new()))
            .clone()
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
        let _ = self.m_queued.fetch_add(n, Ordering::Relaxed);
        let _ = self.m_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.topic_tracker(topic).note_queued(n, bytes);
    }

    fn note_acked(&self, topic: &Arc<str>, n: u64) {
        let _ = self.m_acked.fetch_add(n, Ordering::Relaxed);
        self.topic_tracker(topic).note_acked(n);
    }

    fn note_ack_latency(&self, topic: &Arc<str>, queued_at: Instant) {
        let d = queued_at.elapsed();
        self.ack_latency.record(d);
        self.topic_tracker(topic).note_ack_latency(d);
    }

    fn note_errors(&self, topic: &Arc<str>, n: u64) {
        let _ = self.m_errors.fetch_add(n, Ordering::Relaxed);
        self.topic_tracker(topic).note_errors(n);
    }

    fn try_reserve_buffer(&self, bytes: u64) -> bool {
        let cap = self.cfg.buffer_memory;
        if cap == 0 {
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
    pub async fn new(cfg: ProducerConfig) -> Result<Self> {
        let mut cfg = cfg;
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        let mut meta = BrokerConn::connect_tls_any(
            &cfg.bootstrap,
            &cfg.client_id,
            cfg.connect_timeout,
            cfg.tls.as_ref(),
        )
        .await?;
        let resp =
            crate::protocol::api::negotiate_api_versions(&mut meta, cfg.request_timeout).await?;
        let mut versions = HashMap::new();
        for api in &resp.api_keys {
            let _prev = versions.insert(api.api_key, api.clone());
        }
        crate::protocol::sasl::apply_api_keys(&mut meta, &resp.api_keys);
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
        let find_coord_version = pick(&versions, FIND_COORDINATOR, 1, 6).ok_or_else(|| {
            Error::Unsupported("broker does not support FindCoordinator v1-6".into())
        })?;
        if let Some(pv) = pick(&versions, PRODUCE, 3, 12) {
            meta.set_produce_version(pv);
        }
        let metadata_version = pick(&versions, METADATA, 1, 13)
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        let (add_partitions_version, add_offsets_version, end_txn_version, txn_offset_version) =
            if cfg.transactional_id.is_some() {
                let add_p = pick(&versions, ADD_PARTITIONS_TO_TXN, 0, 3).ok_or_else(|| {
                    Error::Unsupported("broker does not support AddPartitionsToTxn".into())
                })?;
                let add_o = pick(&versions, ADD_OFFSETS_TO_TXN, 0, 4).ok_or_else(|| {
                    Error::Unsupported("broker does not support AddOffsetsToTxn".into())
                })?;
                let end = pick(&versions, END_TXN, 0, 5)
                    .ok_or_else(|| Error::Unsupported("broker does not support EndTxn".into()))?;
                let toc = pick(&versions, TXN_OFFSET_COMMIT, 0, 5).ok_or_else(|| {
                    Error::Unsupported("broker does not support TxnOffsetCommit".into())
                })?;
                (add_p, add_o, end, toc)
            } else {
                (0, 0, 0, 0)
            };

        let mut producer_id = RecordBatch::NO_PRODUCER_ID;
        let mut producer_epoch = RecordBatch::NO_PRODUCER_EPOCH;
        let mut init_producer_id_version = 0i16;
        let mut txn = if let Some(tid) = cfg.transactional_id.as_deref() {
            Some(
                discover_typed_coord(&cfg, tid, COORDINATOR_TRANSACTION, find_coord_version)
                    .await?,
            )
        } else {
            None
        };
        if cfg.enable_idempotence {
            let ipid_version = pick(&versions, INIT_PRODUCER_ID, 0, 5).ok_or_else(|| {
                Error::Unsupported("broker does not support InitProducerId".into())
            })?;
            init_producer_id_version = ipid_version;
            let body = init_producer_id_roundtrip(
                &cfg,
                &mut txn,
                &mut meta,
                ipid_version,
                find_coord_version,
                (RecordBatch::NO_PRODUCER_ID, RecordBatch::NO_PRODUCER_EPOCH),
            )
            .await?;
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
        let shared = Arc::new(Shared {
            cfg: cfg.clone(),
            cluster: parking_lot::Mutex::new(Cluster::default()),
            meta: Mutex::new(meta),
            txn: Mutex::new(txn),
            metadata_version,
            add_partitions_version,
            add_offsets_version,
            end_txn_version,
            txn_offset_version,
            find_coord_version,
            init_producer_id_version,
            telemetry_version: pick(&versions, GET_TELEMETRY_SUBSCRIPTIONS, 0, 0),
            client_instance_id: parking_lot::Mutex::new(None),
            partitioner: cfg.partitioner.arc(),
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
            nodes: parking_lot::Mutex::new(HashMap::new()),
            reconnect_fails: parking_lot::Mutex::new(HashMap::new()),
            reconnect_busy: parking_lot::Mutex::new(HashSet::new()),
            retries_out: AtomicUsize::new(0),
            in_txn: AtomicBool::new(false),
            txn_partitions: parking_lot::Mutex::new(HashSet::new()),
            txn_added: parking_lot::Mutex::new(HashSet::new()),
            fast: parking_lot::Mutex::new(None),
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
            while let Some(topic) = meta_rx.recv().await {
                let Some(shared) = weak.upgrade() else {
                    break;
                };
                if shared.closed.load(Ordering::SeqCst) {
                    break;
                }
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
        let recs: Vec<ProduceRecord> = recs.into_iter().collect();
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        let mut rxs = Vec::with_capacity(recs.len());
        for rec in recs {
            let (tx, rx) = oneshot::channel();
            let mut rec = self.inner.shared.interceptors.on_send(rec);
            let bytes = reject_oversized(&self.inner.shared.cfg, &rec)?;
            let block_deadline = Instant::now() + self.inner.shared.cfg.max_block;
            self.ensure_ready(&mut rec, block_deadline).await?;
            let w = self.worker_for(&rec).ok_or(Error::Closed)?;
            self.wait_buffer(bytes, block_deadline).await?;
            if self.inner.shared.closed.load(Ordering::SeqCst) {
                self.inner.shared.release_buffer(bytes);
                return Err(Error::Closed);
            }
            let now = Instant::now();
            let deadline = now + self.inner.shared.cfg.delivery_timeout;
            let topic = rec.topic.clone();
            let rest_block = block_deadline.saturating_duration_since(now);
            if rest_block.is_zero() {
                self.inner.shared.release_buffer(bytes);
                return Err(Error::Timeout);
            }
            let send_res = tokio::time::timeout(
                rest_block,
                w.data.send(Pending {
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
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    self.inner.shared.release_buffer(bytes);
                    return Err(Error::Closed);
                }
                Err(_) => {
                    self.inner.shared.release_buffer(bytes);
                    return Err(Error::Timeout);
                }
            }
            self.inner.shared.note_queued_n(&topic, 1, bytes);
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
        let topic = rec.topic.clone();
        if !self.inner.shared.try_reserve_buffer(bytes) {
            return Err(Error::QueueFull);
        }
        if let Err(e) = w.data.try_send(Pending {
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
            self.inner.shared.release_buffer(bytes);
            return Err(match e {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            });
        }
        self.inner.shared.note_queued_n(&topic, 1, bytes);
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
        let pid = self.inner.shared.producer_id.load(Ordering::SeqCst);
        let epoch = self.inner.shared.producer_epoch.load(Ordering::SeqCst);
        let version = self.inner.shared.txn_offset_version;
        // TxnOffsetCommit v5 is transaction V2 (KIP-890 Part 2): the
        // group coordinator also performs AddOffsetsToTxn. Skip that
        // RPC when the broker advertised v5 (Java `isTransactionV2Enabled`).
        if version <= TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 {
            let add_offsets_version = self.inner.shared.add_offsets_version;
            let start = Instant::now();
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
                    timeout,
                    |body| {
                        Ok(
                            decode_add_offsets_to_txn_response(&mut { body }, add_offsets_version)?
                                .0,
                        )
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
                    && start.elapsed() < timeout
                {
                    attempt += 1;
                    txn_retry_sleep(&self.inner.shared, attempt).await;
                    continue;
                }
                let e = Error::broker(err, "AddOffsetsToTxn");
                self.inner.shared.apply_txn_disposition(
                    classify_add_offsets_error(err, can_handle),
                    clone_err(&e),
                );
                return Err(e);
            }
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
        let grouped = {
            let cluster = self.inner.shared.cluster.lock();
            group_txn_offsets(&offsets, |topic, part| cluster.leader_epoch(topic, part))
        };
        let start = Instant::now();
        let mut attempt = 0u32;
        loop {
            let body = group_coord_roundtrip(
                &self.inner.shared.cfg,
                group_id,
                TXN_OFFSET_COMMIT,
                version,
                self.inner.shared.find_coord_version,
                |buf| {
                    encode_txn_offset_commit_request(
                        buf, version, &tid, group_id, pid, epoch, member, &grouped,
                    )
                },
                timeout,
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
                && start.elapsed() < timeout
            {
                attempt += 1;
                txn_retry_sleep(&self.inner.shared, attempt).await;
                continue;
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
        let start = Instant::now();
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
                timeout,
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
                && start.elapsed() < timeout
            {
                attempt += 1;
                txn_retry_sleep(&self.inner.shared, attempt).await;
                continue;
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
            let rest = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(rest, self.flush_workers()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(Error::Timeout),
            }
            if self.inner.shared.retries_out.load(Ordering::SeqCst) == 0 {
                let rest = deadline.saturating_duration_since(Instant::now());
                match tokio::time::timeout(rest, self.flush_workers()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => return Err(e),
                    Err(_) => return Err(Error::Timeout),
                }
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
            .cloned()
            .collect();
        let mut rxs = Vec::with_capacity(workers.len());
        for w in &workers {
            let (tx, rx) = oneshot::channel();
            drop(w.ctrl.send(Ctrl::Close(tx)).await);
            rxs.push(rx);
        }

        let wait_drain = async {
            for rx in rxs {
                drop(rx.await);
            }
        };

        if let Some(d) = deadline {
            let rest = d.saturating_duration_since(Instant::now());
            drop(tokio::time::timeout(rest, wait_drain).await);
        } else {
            wait_drain.await;
        }

        for w in &workers {
            w.task.abort();
        }
        self.inner.shared.nodes.lock().clear();
        *self.inner.shared.fast.lock() = None;

        if let Some(h) = self.inner.shared.meta_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.inner.shared.connect_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.inner.shared.retry_task.lock().take() {
            h.abort();
        }
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
        resp.check()?;
        {
            let mut cluster = self.inner.shared.cluster.lock();
            cluster.apply(&resp, version);
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
    let versions_resp =
        crate::protocol::api::negotiate_api_versions(&mut conn, cfg.request_timeout).await?;
    if let Some(pv) = versions_resp
        .api_version(PRODUCE)
        .and_then(|v| pick_version(v.min_version, v.max_version, 3, 12))
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
        cfg.request_timeout,
    )
    .await?;
    Ok(conn)
}

async fn discover_typed_coord(
    cfg: &ProducerConfig,
    key: &str,
    key_type: i8,
    version: i16,
) -> Result<BrokerConn> {
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
                    version,
                    |buf| encode_find_coordinator_request_typed(buf, version, key, key_type),
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
            let (err, _node, host, port) =
                decode_find_coordinator_response(&mut body.clone(), version)?;
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
    find_coord_version: i16,
    identity: (i64, i16),
) -> Result<Bytes> {
    let txn_id = cfg.transactional_id.clone();
    let timeout = cfg.request_timeout;
    let txn_timeout_ms = i32::try_from(cfg.transaction_timeout.as_millis())
        .unwrap_or(i32::MAX)
        .max(0);
    let (producer_id, producer_epoch) = identity;
    let first = {
        let conn = txn.as_mut().unwrap_or(meta);
        conn.roundtrip(
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
        // `Closed` is a dead socket, not a dead producer: rediscover and
        // retry once like any other transport failure (KL03-10). Without
        // this, one broker-side close bricks the coordinator channel.
        Err(e) if e.is_retriable() || matches!(e, Error::Closed) => {}
        Err(e) => return Err(e),
    }
    let new = discover_typed_coord(cfg, &tid, COORDINATOR_TRANSACTION, find_coord_version).await?;
    *txn = Some(new);
    let conn = txn
        .as_mut()
        .ok_or_else(|| Error::protocol("no transaction coordinator"))?;
    conn.roundtrip(
        INIT_PRODUCER_ID,
        version,
        |buf| {
            encode_init_producer_id_request(
                buf,
                version,
                Some(tid.as_str()),
                txn_timeout_ms,
                producer_id,
                producer_epoch,
            )
        },
        timeout,
    )
    .await
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
    let tid = shared
        .cfg
        .transactional_id
        .clone()
        .ok_or_else(reject_java_no_transaction_manager)?;
    let timeout = shared.cfg.request_timeout;
    let txn_timeout_ms = i32::try_from(shared.cfg.transaction_timeout.as_millis())
        .unwrap_or(i32::MAX)
        .max(0);
    let body = txn_roundtrip(
        shared,
        INIT_PRODUCER_ID,
        version,
        |buf| {
            encode_init_producer_id_request(
                buf,
                version,
                Some(tid.as_str()),
                txn_timeout_ms,
                pid,
                epoch,
            )
        },
        timeout,
        |body| Ok(decode_init_producer_id_response(&mut { body }, version)?.0),
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
    let mut txn: Option<BrokerConn> = None;
    let mut meta = shared.meta.lock().await;
    let body = init_producer_id_roundtrip(
        &shared.cfg,
        &mut txn,
        &mut meta,
        version,
        shared.find_coord_version,
        (RecordBatch::NO_PRODUCER_ID, RecordBatch::NO_PRODUCER_EPOCH),
    )
    .await?;
    drop(meta);
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
async fn txn_retry_sleep(shared: &Shared, attempt: u32) {
    tokio::time::sleep(crate::config::retry_backoff_delay(
        shared.cfg.retry_backoff,
        shared.cfg.retry_backoff_max,
        attempt,
    ))
    .await;
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
        .ok_or_else(reject_java_no_transaction_manager)?;
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
        // `Closed` is a dead socket, not a dead producer: rediscover and
        // retry once like any other transport failure (KL03-10). Without
        // this, one broker-side close bricks the coordinator channel.
        Err(e) if e.is_retriable() || matches!(e, Error::Closed) => {}
        Err(e) => return Err(e),
    }
    let new = discover_typed_coord(
        &shared.cfg,
        &tid,
        COORDINATOR_TRANSACTION,
        shared.find_coord_version,
    )
    .await?;
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

#[expect(
    clippy::too_many_arguments,
    reason = "group coord roundtrip is one wire call plus rediscovery identity"
)]
async fn group_coord_roundtrip(
    cfg: &ProducerConfig,
    group_id: &str,
    api_key: i16,
    api_version: i16,
    find_coord_version: i16,
    encode_body: impl Fn(&mut BytesMut) -> Result<()>,
    request_timeout: Duration,
    error_of: impl Fn(&[u8]) -> Result<i16>,
) -> Result<Bytes> {
    let mut coord =
        discover_typed_coord(cfg, group_id, COORDINATOR_GROUP, find_coord_version).await?;
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
    coord = discover_typed_coord(cfg, group_id, COORDINATOR_GROUP, find_coord_version).await?;
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
    partitions_for_timeout(shared, topic, shared.cfg.request_timeout).await
}

async fn partitions_for_timeout(
    shared: &Shared,
    topic: &Arc<str>,
    timeout: Duration,
) -> Result<i32> {
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
    let topics = [topic.to_string()];
    let body = conn
        .roundtrip_deadline(
            METADATA,
            version,
            |buf| encode_metadata_request(buf, version, Some(&topics), allow),
            deadline,
        )
        .await?;
    drop(conn);
    let resp = decode_metadata_response(&mut body.clone(), version)?;
    resp.check()?;
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
        cluster.apply(&resp, version);
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
        if shared.closed.load(Ordering::SeqCst) {
            break;
        }
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
            let mut busy = shared.reconnect_busy.lock();
            if !busy.insert(node) {
                continue;
            }
        }
        {
            let mut nodes = shared.nodes.lock();
            let _ = nodes.entry(node).or_insert_with(Vec::new);
        }
        match spawn_node_workers(&shared, node, &addr, cap).await {
            Ok(workers) => {
                let _ = shared.reconnect_busy.lock().remove(&node);
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
                    let weak = weak.clone();
                    drop(tokio::spawn(async move {
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        if let Some(shared) = weak.upgrade() {
                            let _ = shared.reconnect_busy.lock().remove(&node);
                            try_nudge_node(&shared.connect_tx, node);
                        }
                    }));
                } else {
                    let _ = shared.reconnect_busy.lock().remove(&node);
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
    if shared.closed.load(Ordering::SeqCst) {
        return Err(Error::Closed);
    }
    let n_conn = shared.cfg.connections.max(1);
    let mut workers: Vec<WorkerHandle> = Vec::with_capacity(n_conn);
    for _ in 0..n_conn {
        if shared.closed.load(Ordering::SeqCst) {
            for w in &workers {
                w.task.abort();
            }
            return Err(Error::Closed);
        }
        let conn = open_conn(addr, &shared.cfg).await?;
        if conn.produce_version() < 0 {
            for w in &workers {
                w.task.abort();
            }
            return Err(Error::Unsupported(format!(
                "broker at {addr} does not support Produce v3-12"
            )));
        }
        if shared.closed.load(Ordering::SeqCst) {
            for w in &workers {
                w.task.abort();
            }
            return Err(Error::Closed);
        }
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
        let handle = tokio::spawn(worker.run());
        workers.push(WorkerHandle {
            data: data_tx,
            ctrl: ctrl_tx,
            task: Arc::new(handle),
        });
    }
    Ok(workers)
}

async fn retry_loop(weak: std::sync::Weak<Shared>, mut rx: mpsc::Receiver<Pending>) {
    struct RxDrain<'a> {
        weak: &'a std::sync::Weak<Shared>,
        rx: &'a mut mpsc::Receiver<Pending>,
    }
    impl<'a> Drop for RxDrain<'a> {
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
        if shared.closed.load(Ordering::SeqCst) {
            let _ = shared.retries_out.fetch_sub(1, Ordering::SeqCst);
            fail_pendings(&shared, vec![p], Error::Timeout);
            shared.cache_nudge.notify_waiters();
            continue;
        }
        retry_one(&shared, p).await;
        let _ = shared.retries_out.fetch_sub(1, Ordering::SeqCst);
        shared.cache_nudge.notify_waiters();
    }
}

struct RetryGuard<'a> {
    shared: &'a Arc<Shared>,
    p: Option<Pending>,
}

impl<'a> Drop for RetryGuard<'a> {
    fn drop(&mut self) {
        if let Some(p) = self.p.take() {
            fail_pendings(self.shared, vec![p], Error::Timeout);
        }
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
    let need_meta = if skip_meta {
        match p.rec.partition {
            Some(part) => shared
                .cluster
                .lock()
                .leader(p.rec.topic.as_ref(), part)
                .is_err(),
            None => true,
        }
    } else {
        true
    };
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
    if p.rec.partition.is_none() {
        if let Some(np) = shared.cluster.lock().partition_count(p.rec.topic.as_ref()) {
            p.rec.partition = Some(pick_part(&p.rec, np, shared.partitioner.as_ref()));
        }
    }
    let Some(part) = p.rec.partition else {
        return;
    };
    let leader = shared.cluster.lock().leader(p.rec.topic.as_ref(), part);
    let Ok((node, _)) = leader else {
        return;
    };
    try_nudge_node(&shared.connect_tx, node);
    let deadline = p.deadline;
    loop {
        if shared.closed.load(Ordering::SeqCst) || Instant::now() >= deadline {
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
    version: i16,
    groups: Vec<(Arc<str>, i32, Vec<Pending>)>,
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
                fail_inflight(&self.shared, &mut self.in_flight, Error::Timeout);
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
            self.pending.push(p);
        }
    }

    fn purge_expired_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut i = 0;
        let mut expired = Vec::new();
        while i < self.pending.len() {
            let is_expired = self.pending.get(i).is_some_and(|p| now >= p.deadline);
            if is_expired {
                expired.push(self.pending.remove(i));
            } else {
                i += 1;
            }
        }
        if !expired.is_empty() {
            fail_pendings(&self.shared, expired, Error::Timeout);
        }
    }

    fn earliest_pending_deadline(&self) -> Option<Instant> {
        self.pending.iter().map(|p| p.deadline).min()
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
        if let Some(first) = self.pending.first() {
            if let Some(base) = first.batch_base_seq {
                let count = self
                    .pending
                    .iter()
                    .take_while(|p| p.batch_base_seq == Some(base))
                    .count();
                return count >= first.batch_len;
            }
        }
        batch_ready(
            &self.pending,
            self.shared.cfg.batch_records,
            self.shared.cfg.produce_batch_bytes(),
        ) || self.linger_expired(linger_start)
    }

    async fn run(mut self) {
        let mut linger_start: Option<Instant> = None;
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

            let now = Instant::now();
            let next_wake = match (wait_linger, self.earliest_pending_deadline()) {
                (Some(s), Some(dl)) => {
                    let linger_rest = linger.saturating_sub(s.elapsed());
                    let dl_rest = dl.saturating_duration_since(now);
                    Some(linger_rest.min(dl_rest))
                }
                (Some(s), None) => Some(linger.saturating_sub(s.elapsed())),
                (None, Some(dl)) => Some(dl.saturating_duration_since(now)),
                (None, None) => None,
            };

            tokio::select! {
                biased;
                n = self.data.recv_many(&mut self.pending, room) => {
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
                    next_wake.unwrap_or(Duration::from_secs(86400)),
                ), if next_wake.is_some() => {
                    self.purge_expired_pending();
                }
            }
        }
    }

    async fn fire(&mut self) -> Result<()> {
        self.purge_expired_pending();
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.conn.is_closed() {
            let _ = self.shared.nodes.lock().remove(&self.node_id);
            try_nudge_node(&self.shared.connect_tx, self.node_id);
            let pending = std::mem::take(&mut self.pending);
            self.requeue_pendings(pending);
            while let Some(remaining) = self.in_flight.pop_front() {
                self.requeue(remaining.groups);
            }
            return Ok(());
        }
        if self.in_flight.is_empty() && self.conn.idle_expired(self.shared.cfg.connections_max_idle)
        {
            let addr = self.conn.addr().to_string();
            match open_conn(&addr, &self.shared.cfg).await {
                Ok(c) if c.produce_version() >= 0 => self.conn = c,
                Ok(_) => {
                    let e = Error::Unsupported(format!(
                        "broker at {addr} does not support Produce v3-12"
                    ));
                    let pending = std::mem::take(&mut self.pending);
                    fail_pendings(&self.shared, pending, clone_err(&e));
                    self.note_fail(e.clone());
                    return Err(e);
                }
                Err(e) => {
                    let pending = std::mem::take(&mut self.pending);
                    if e.is_retriable() {
                        let _ = self.shared.nodes.lock().remove(&self.node_id);
                        self.requeue_pendings(pending);
                        return Ok(());
                    }
                    fail_pendings(&self.shared, pending, clone_err(&e));
                    self.note_fail(e.clone());
                    return Err(e);
                }
            }
        }
        let n = take_count(
            &self.pending,
            self.shared.cfg.batch_records,
            self.shared.cfg.produce_batch_bytes(),
        );
        let batch: Vec<Pending> = self.pending.drain(..n).collect();
        if let Some(p) = batch.iter().find(|p| p.rec.partition.is_none()) {
            let e = Error::protocol(format!("produce without partition topic={}", p.rec.topic));
            fail_pendings(&self.shared, batch, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        let batch_deadline = batch
            .iter()
            .map(|p| p.deadline)
            .min()
            .unwrap_or_else(|| Instant::now() + self.shared.cfg.delivery_timeout);
        let now = now_ms();
        let mut groups = group_pending(batch);
        if groups.is_empty() {
            return Ok(());
        }
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
            fail_groups(&self.shared, groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        if let Err(e) = self.add_txn_partitions(&groups).await {
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            fail_groups(&self.shared, groups, clone_err(&e));
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
        self.write_buf.clear();
        self.write_buf.put_i32(0);
        let correlation = self.conn.next_correlation();

        if fault == Some(PreSendFault::Encode) {
            let e = Error::protocol("injected produce encode failure");
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            fail_groups(&self.shared, groups, clone_err(&e));
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
            fail_groups(&self.shared, groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        if let Err(e) = encode_produce_body(
            &mut self.write_buf,
            version,
            acks,
            timeout_ms,
            &groups,
            compression,
            now,
            producer_id,
            producer_epoch,
            transactional_id,
        ) {
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            fail_groups(&self.shared, groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }
        let size =
            match crate::protocol::buf::i32_from_usize(self.write_buf.len().saturating_sub(4)) {
                Ok(s) => s,
                Err(e) => {
                    rollback_sequences(&groups, producer_id, &self.shared.seqs);
                    fail_groups(&self.shared, groups, clone_err(&e));
                    self.note_fail(e.clone());
                    return Err(e);
                }
            };
        if let Err(e) = crate::protocol::buf::patch_i32(&mut self.write_buf, 0, size) {
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            fail_groups(&self.shared, groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }

        if fault == Some(PreSendFault::Write) {
            let e = Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected socket write failure",
            ));
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            fail_groups(&self.shared, groups, clone_err(&e));
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
            fail_groups(&self.shared, groups, Error::Timeout);
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
                let _ = self.shared.nodes.lock().remove(&self.node_id);
                try_nudge_node(&self.shared.connect_tx, self.node_id);
                self.requeue(groups);
                while let Some(remaining) = self.in_flight.pop_front() {
                    self.requeue(remaining.groups);
                }
                let pending = std::mem::take(&mut self.pending);
                self.requeue_pendings(pending);
                return Ok(());
            }
            rollback_sequences(&groups, producer_id, &self.shared.seqs);
            fail_groups(&self.shared, groups, clone_err(&e));
            self.note_fail(e.clone());
            return Err(e);
        }

        if acks == 0 {
            complete_acks0(&self.shared, groups);
            return Ok(());
        }
        self.in_flight.push_back(InFlight {
            correlation,
            version,
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
                        let _ = self.shared.nodes.lock().remove(&self.node_id);
                        try_nudge_node(&self.shared.connect_tx, self.node_id);
                        self.requeue(inf.groups);
                        while let Some(remaining) = self.in_flight.pop_front() {
                            self.requeue(remaining.groups);
                        }
                        let pending = std::mem::take(&mut self.pending);
                        self.requeue_pendings(pending);
                        return Ok(());
                    }
                    fail_groups(&self.shared, inf.groups, clone_err(&e));
                }
                return Err(e);
            }
        };
        let mut body = body;
        let (responses, endpoints, ..) = match decode_produce_response(&mut body, version) {
            Ok(r) => r,
            Err(e) => {
                if let Some(inf) = guard.inf.take() {
                    fail_groups(&self.shared, inf.groups, clone_err(&e));
                }
                return Err(e);
            }
        };
        let Some(inf) = guard.inf.take() else {
            return Ok(());
        };
        self.shared.cluster.lock().apply_node_endpoints(&endpoints);
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
                        let applied = r.current_leader_id >= 0
                            && self.shared.cluster.lock().apply_current_leader(
                                topic.as_ref(),
                                part,
                                r.current_leader_id,
                                r.current_leader_epoch,
                            );
                        let mut pendings = pendings;
                        if applied {
                            drop_fast_topic(&self.shared, topic.as_ref());
                            try_nudge_node(&self.shared.connect_tx, r.current_leader_id);
                            for p in &mut pendings {
                                p.skip_meta_refresh = true;
                            }
                        } else {
                            invalidate_cached_topic(&self.shared, topic.as_ref());
                            drop(self.shared.meta_tx.try_send(topic.clone()));
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
                                    fail_pendings(&self.shared, pendings, clone_err(&renew_err));
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
                        fail_pendings(&self.shared, pendings, clone_err(&e));
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    } else {
                        fail_pendings(&self.shared, pendings, clone_err(&e));
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
                Some(r) => {
                    let n = u64::try_from(pendings.len()).unwrap_or(u64::MAX);
                    self.shared.release_buffer(pendings_bytes(&pendings));
                    self.shared.note_acked(&topic, n);
                    for (i, p) in pendings.into_iter().enumerate() {
                        self.shared.note_ack_latency(&topic, p.queued_at);
                        let batch_index = i32::try_from(i).unwrap_or(i32::MAX);
                        let md = record_metadata(
                            &topic,
                            part,
                            r.base_offset,
                            batch_index,
                            &p.rec,
                            r.log_append_time_ms,
                        );
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
        let start = Instant::now();
        let mut attempt = 0u32;
        loop {
            // A transport error sets no sticky state: adding partitions is
            // idempotent, so the failed send is simply retried by the caller
            // (or the transaction is aborted).
            let body = match txn_roundtrip(
                &self.shared,
                ADD_PARTITIONS_TO_TXN,
                version,
                |buf| encode_add_partitions_to_txn_request(buf, version, &tid, pid, epoch, &topics),
                timeout,
                |body| decode_add_partitions_to_txn_response(&mut { body }, version),
            )
            .await
            {
                Ok(b) => b,
                Err(e) => {
                    let mut set = self.shared.txn_partitions.lock();
                    for k in &added {
                        let _ = set.remove(k);
                    }
                    return Err(e);
                }
            };
            let err = match decode_add_partitions_to_txn_response(&mut body.clone(), version) {
                Ok(e) => e,
                Err(e) => {
                    let mut set = self.shared.txn_partitions.lock();
                    for k in &added {
                        let _ = set.remove(k);
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
                && start.elapsed() < timeout
            {
                attempt += 1;
                txn_retry_sleep(&self.shared, attempt).await;
                continue;
            }
            let mut set = self.shared.txn_partitions.lock();
            for k in &added {
                let _ = set.remove(k);
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
            for k in added {
                let _ = sent.insert(k);
            }
        }
        Ok(())
    }

    fn requeue_pendings(&mut self, pendings: Vec<Pending>) {
        let now = Instant::now();
        for mut p in pendings {
            p.retry = p.retry.saturating_add(1);
            if now >= p.deadline {
                fail_pendings(&self.shared, vec![p], Error::Timeout);
                continue;
            }
            let delay = crate::config::retry_backoff_delay(
                self.shared.cfg.retry_backoff,
                self.shared.cfg.retry_backoff_max,
                p.retry.saturating_sub(1),
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
    }

    async fn drain_inflight(&mut self) {
        while !self.pending.is_empty() || !self.in_flight.is_empty() {
            self.purge_expired_pending();
            if !self.pending.is_empty() {
                while self.in_flight.len() >= self.shared.cfg.max_in_flight {
                    if let Err(e) = self.wait_one().await {
                        fail_inflight(&self.shared, &mut self.in_flight, clone_err(&e));
                        self.note_fail(e);
                    }
                }
                if let Err(e) = self.fire().await {
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
    let part0 = first
        .rec
        .partition
        .unwrap_or(RecordMetadata::UNKNOWN_PARTITION);
    let homogeneous = batch
        .iter()
        .all(|p| p.rec.partition == Some(part0) && p.rec.topic.as_ref() == topic0.as_ref());
    if homogeneous {
        return vec![(topic0, part0, batch)];
    }
    let mut assigned: HashMap<(Arc<str>, i32), Vec<Pending>> = HashMap::new();
    for p in batch {
        assigned
            .entry((
                p.rec.topic.clone(),
                p.rec.partition.unwrap_or(RecordMetadata::UNKNOWN_PARTITION),
            ))
            .or_default()
            .push(p);
    }
    assigned.into_iter().map(|((t, p), v)| (t, p, v)).collect()
}

fn batch_ready(pending: &[Pending], rec_limit: usize, byte_limit: usize) -> bool {
    if let Some(first) = pending.first() {
        if let Some(base) = first.batch_base_seq {
            let count = pending
                .iter()
                .take_while(|p| p.batch_base_seq == Some(base))
                .count();
            return count >= first.batch_len;
        }
    }
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
    let mut batch_bases: HashMap<(Arc<str>, i32), Option<i32>> = HashMap::new();
    for p in pending.iter().take(rec_limit) {
        let key = (p.rec.topic.clone(), p.rec.partition.unwrap_or(0));
        match batch_bases.get(&key) {
            Some(&expected_base) => {
                if p.batch_base_seq != expected_base {
                    break;
                }
            }
            None => {
                let _ = batch_bases.insert(key, p.batch_base_seq);
            }
        }
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
    epoch_gen: u64,
) {
    if producer_id <= RecordBatch::NO_PRODUCER_ID {
        return;
    }
    for (topic, partition, pendings) in groups.iter_mut() {
        // A retained batch whose sequence was assigned under a superseded
        // epoch (shared epoch bump or identity renewal while it was
        // pending, in flight, or queued for retry) must drop the stale
        // assignment and re-take a sequence under the current epoch.
        // Sending the old base sequence with the new epoch would fail with
        // OUT_OF_ORDER_SEQUENCE_NUMBER; reusing seq zero twice on one
        // partition would silently dedup away a record.
        for p in pendings.iter_mut() {
            if p.epoch_gen != epoch_gen {
                p.seq = None;
                p.batch_base_seq = None;
                p.batch_len = 1;
                p.epoch_gen = epoch_gen;
            }
        }
        if pendings.iter().all(|p| p.seq.is_some()) {
            continue;
        }
        let base = next_sequence(seqs, producer_id, topic, *partition, pendings.len());
        let count = pendings.len();
        for (i, p) in pendings.iter_mut().enumerate() {
            if p.seq.is_none() {
                p.seq = Some(base.saturating_add(i32::try_from(i).unwrap_or(0)));
                p.batch_base_seq = Some(base);
                p.batch_len = count;
            }
        }
    }
}

fn rollback_sequences(
    groups: &[(Arc<str>, i32, Vec<Pending>)],
    producer_id: i64,
    seqs: &parking_lot::Mutex<HashMap<(Arc<str>, i32), i32>>,
) {
    if producer_id <= RecordBatch::NO_PRODUCER_ID {
        return;
    }
    let mut g = seqs.lock();
    for (topic, partition, pendings) in groups {
        let Some(first) = pendings.first() else {
            continue;
        };
        let Some(base) = first.seq else {
            continue;
        };
        let count = i32::try_from(pendings.len()).unwrap_or(0);
        if let Some(curr) = g.get_mut(&(topic.clone(), *partition)) {
            if *curr == base.saturating_add(count) {
                *curr = base;
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
    // v9–v12 share this compact request layout (v10+ CurrentLeader is
    // response-only; v12 transaction V2 is Produce-does-AddPartitionsToTxn).
    // Must stay in sync with `encode_produce_request`.
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
            let base_sequence = pendings.first().and_then(|p| p.seq).unwrap_or(
                if producer_id <= RecordBatch::NO_PRODUCER_ID {
                    RecordBatch::NO_SEQUENCE
                } else {
                    0
                },
            );
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
    if producer_id <= RecordBatch::NO_PRODUCER_ID {
        return RecordBatch::NO_SEQUENCE;
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
    for (topic, part, pendings) in groups {
        shared.release_buffer(pendings_bytes(&pendings));
        let n = u64::try_from(pendings.len()).unwrap_or(u64::MAX);
        shared.note_acked(&topic, n);
        for p in pendings {
            shared.note_ack_latency(&topic, p.queued_at);
            let md = record_metadata(
                &topic,
                part,
                RecordMetadata::INVALID_OFFSET,
                0,
                &p.rec,
                RecordBatch::NO_TIMESTAMP,
            );
            shared.interceptors.on_ack(&md);
            if let Some(tx) = p.tx {
                drop(tx.send(Ok(md)));
            }
        }
    }
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
    shared.release_buffer(pendings_bytes(&pendings));
    for p in pendings {
        shared.note_errors(&p.rec.topic, 1);
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

fn serialized_bytes_size(bytes: Option<&Bytes>) -> i32 {
    bytes
        .map(|b| i32::try_from(b.len()).unwrap_or(i32::MAX))
        .unwrap_or(-1)
}

fn record_metadata(
    topic: &str,
    partition: i32,
    base_offset: i64,
    batch_index: i32,
    rec: &ProduceRecord,
    log_append_time_ms: i64,
) -> RecordMetadata {
    let timestamp = if log_append_time_ms >= 0 {
        log_append_time_ms
    } else {
        rec.timestamp.unwrap_or_else(now_ms)
    };
    RecordMetadata::new(
        crate::TopicPartition::new(topic, partition),
        base_offset,
        batch_index,
        timestamp,
        serialized_bytes_size(rec.key.as_ref()),
        serialized_bytes_size(rec.value.as_ref()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
