//! A Kafka client written in Rust. No C, no librdkafka.
//!
//! # Produce
//!
//! ```no_run
//! # async fn example() -> partitionline::Result<()> {
//! use partitionline::{ProduceRecord, Producer};
//!
//! let producer = Producer::connect("127.0.0.1:9092").await?;
//! let md = producer
//!     .send(ProduceRecord::to("events").value(&b"hello"[..]))
//!     .await?;
//! println!("{}-{}@{}", md.topic, md.partition, md.offset);
//! producer.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! For many records, [`Producer::send_all`] waits for every offset after
//! queuing, and [`Producer::try_send`] plus [`Producer::flush`] is the
//! throughput path (see `examples/bench_produce.rs`).
//! [`Producer::metrics`] is a snapshot of queued / acked / error counts
//! plus produce-ack latency min/mean/max and p50/p99 (last 1024 samples),
//! with per-topic rows on [`ProducerMetrics::topics`].
//! [`Producer::client_instance_id`] is Java `clientInstanceId` (KIP-714).
//!
//! # Fetch
//!
//! ```no_run
//! # async fn example() -> partitionline::Result<()> {
//! use partitionline::Consumer;
//!
//! let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
//! consumer.assign("events", 0, 0).await?;
//! let recs = consumer.fetch().await?;
//! # let _ = recs;
//! # Ok(())
//! # }
//! ```
//!
//! [`Consumer::assign_topic`] assigns every partition. [`Consumer::seek`] /
//! [`Consumer::seek_to`] / [`Consumer::seek_to_beginning`] /
//! [`Consumer::seek_to_end`] / [`Consumer::seek_to_beginning_of`] /
//! [`Consumer::seek_to_end_of`] move the
//! next fetch offset. [`Consumer::pause`] / [`Consumer::resume`] skip
//! partitions without dropping the assignment. [`Consumer::fetch`] talks to
//! every partition leader in parallel. [`ConsumerConfig::max_bytes`] sets
//! both `fetch.max.bytes` and `max.partition.fetch.bytes`;
//! [`ConsumerConfig::fetch_max_bytes`] /
//! [`ConsumerConfig::max_partition_fetch_bytes`] set them independently.
//! [`Consumer::partitions_for`] /
//! [`Producer::partitions_for`] return Metadata (leader, replicas, ISR,
//! [`PartitionInfo::offline_replicas`], [`PartitionInfo::leader_epoch`]).
//! [`Consumer::wakeup`] interrupts fetch
//! (clone [`WakeupHandle`] for another task).
//! [`Consumer::client_instance_id`] is Java `clientInstanceId` (KIP-714).
//! [`Consumer::offsets_for_times`] is Java `offsetsForTimes`
//! ([`OffsetAndTimestamp::leader_epoch`] is Java `getLeaderEpoch`).
//! [`FetchedRecord::leader_epoch`] is the record-batch partition leader epoch.
//! [`FetchedRecord::serialized_key_size`] / [`FetchedRecord::serialized_value_size`]
//! match Java `serializedKeySize` / `serializedValueSize`.
//! [`Admin::create_partitions`] takes [`NewPartitions`].
//! [`Admin::incremental_alter_configs`] / [`Admin::alter_configs`] take
//! [`ConfigResource`] / [`ConfigResourceType`].
//! [`Consumer::current_lag`] is Java `currentLag`.
//! [`Consumer::list_topics`] is cluster Metadata. [`Consumer::assign_many`]
//! / [`Consumer::assign_partitions`] / [`Consumer::unassign`] replace or
//! drop a manual assignment ([`Consumer::assign_partitions`] is Java
//! `assign(Collection)` and uses [`ConsumerConfig::auto_offset_reset`]).
//! [`Consumer::beginning_offsets`] / [`Consumer::end_offsets`] take
//! [`TopicPartition`]. [`Consumer::list_offset`] is ListOffsets for one
//! partition. [`Consumer::assignment`] is Java `assignment`
//! ([`Consumer::assigned_partitions`] is the same list; [`Consumer::positions`]
//! pairs each partition with its next fetch offset).
//! [`Consumer::fetch`] / [`ConsumerGroup::poll`] return [`ConsumerRecords`]
//! (Java `count` / `partitions` / `records` / `nextOffsets`).
//! [`ShareGroup::poll`] returns [`ShareRecords`]. [`Consumer::fetch_timeout`] /
//! [`ConsumerGroup::poll_timeout`] / [`ShareGroup::poll_timeout`] are Java
//! `poll(Duration)`. [`ConsumerGroup::committed_timeout`] is Java
//! `committed(Duration)`. [`ConsumerGroup::commit_timeout`] is Java
//! `commitSync(Duration)`. [`Consumer::partitions_for_timeout`] /
//! [`Consumer::list_topics_timeout`] / [`Consumer::beginning_offsets_timeout`] /
//! [`Consumer::end_offsets_timeout`] / [`Consumer::offsets_for_times_timeout`]
//! are Java `partitionsFor` / `listTopics` / `beginningOffsets` / `endOffsets` /
//! `offsetsForTimes` with a `Duration`.
//! [`ConsumerGroup::commit_offsets`] takes [`TopicPartition`] (or anything
//! that converts to one) plus the next fetch offset.
//! [`ConsumerGroup::commit_with_metadata`] takes
//! [`ConsumerRecords::next_offsets`] (Java `commitSync(records.nextOffsets())`).
//! [`Admin::delete_records`] / [`Admin::describe_producers`] /
//! [`Admin::list_offsets`] / [`Admin::delete_offsets`] /
//! [`Admin::list_consumer_group_offsets`] /
//! [`Admin::alter_consumer_group_offsets`] take [`TopicPartition`].
//! [`Admin::list_offsets`] is Java `listOffsets` ([`OffsetAndTimestamp`]).
//! [`Admin::fence_producers`] is Java `fenceProducers` ([`FencedProducer`]).
//! [`Admin::remove_members_from_consumer_group`] is Java
//! `removeMembersFromConsumerGroup` ([`MemberToRemove`]).
//! [`Admin::describe_features`] is Java `describeFeatures`
//! ([`FeatureMetadata`]; ApiVersions v3 tagged fields).
//! [`AclBinding::allow_topic`] / [`AclResourceType`] / [`AclOperation`] /
//! [`AclPermission`] cover CreateAcls / DescribeAcls / DeleteAcls.
//! [`Producer::init_transactions`] / [`Producer::flush_timeout`] /
//! [`Producer::close_timeout`] match Java. [`Consumer::close_timeout`]
//! drops fetch connections (Java `close(Duration)`; no LeaveGroup).
//! [`ConsumerGroup::close_timeout`] / [`ShareGroup::close_timeout`] cap
//! `leave`.
//! [`ProducerConfig::interceptor`] / [`ConsumerConfig::interceptor`] observe
//! or rewrite records (`close` / [`ConsumerInterceptor::on_commit`]).
//!
//! # Groups
//!
//! [`ConsumerGroup::join`] is classic range, [`ConsumerGroup::join_sticky`]
//! is sticky, [`ConsumerGroup::join_cooperative_sticky`] is KIP-429, and
//! [`ConsumerGroup::join_consumer`] is KIP-848. Each has a
//! `_topics` variant for several topics. [`ConsumerGroup::join_matching`] /
//! [`ConsumerGroup::join_sticky_matching`] /
//! [`ConsumerGroup::join_cooperative_sticky_matching`] /
//! [`ConsumerGroup::join_consumer_matching`] are Java `subscribe(Pattern)`
//! at join (range, sticky, cooperative-sticky, KIP-848).
//! [`ConsumerConfig::group_instance_id`] is static membership.
//! [`ConsumerConfig::auto_offset_reset`] is used when OffsetFetch has no
//! committed offset. [`ShareGroup`] is KIP-932 (`join` / [`ShareGroup::join_topics`] /
//! [`ShareGroup::join_matching`] / [`ShareGroup::subscribe`] /
//! [`ShareGroup::subscribe_matching`] / [`ShareGroup::unsubscribe`]).
//! [`ConsumerGroup::commit_with_metadata`] sends [`OffsetAndMetadata`]
//! (leader epoch and a metadata string). [`ConsumerGroup::commit_timeout`] /
//! [`ConsumerGroup::commit_with_metadata_timeout`] are Java
//! `commitSync(Duration)`. [`ConsumerGroup::enforce_rebalance`]
//! rejoins on the next poll. [`ConsumerConfig::on_rebalance`] receives
//! [`TopicPartition`] slices. [`ConsumerGroup::subscribe`] /
//! [`ConsumerGroup::subscribe_matching`] / [`ConsumerGroup::unsubscribe`]
//! change the topic list without dropping the handle.
//! [`ConsumerGroup::group_metadata`] is Java `ConsumerGroupMetadata`.
//! [`Producer::send_offsets_with_metadata`] / [`Producer::send_offsets_for_group`]
//! commit transactional offsets with epoch and metadata.
//! [`Producer::send_offsets_to_transaction`] takes [`TopicPartition`].
//! [`Admin::close`] drops the admin connection.
//!
//! # Configure
//!
//! ```no_run
//! use std::time::Duration;
//! use partitionline::{Acks, Compression, IsolationLevel, ProducerConfig, Sasl};
//!
//! let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
//!     .acks(Acks::All)
//!     .linger(Duration::from_millis(5))
//!     .compression(Compression::Lz4)
//!     .sasl(Sasl::scram_sha256("alice", "secret"));
//!
//! let _iso = IsolationLevel::ReadCommitted;
//! ```
//!
//! TLS is [`TlsConfig`] on the same builders (rustls, no OpenSSL).
//! [`ProducerConfig::delivery_timeout`] is Kafka `delivery.timeout.ms`
//! (default 30s; Java defaults to 120s). [`ProducerConfig::max_block`] is
//! Kafka `max.block.ms` (how long `send` waits for metadata and
//! [`ProducerConfig::buffer_memory`]; default 30s, Java 60s).
//! [`ProducerConfig::buffer_memory`] is Kafka `buffer.memory` (queued
//! key-plus-value bytes not yet acked; default 32 MiB, Java; zero is no
//! client-side cap). [`ProducerConfig::max_request_size`] is Kafka
//! `max.request.size` (key-plus-value bytes of one record; default 1 MiB,
//! Java; zero is no extra cap; oversized records return
//! [`Error::RecordTooLarge`]). [`ProducerConfig::retry_backoff`] /
//! [`ProducerConfig::retry_backoff_max`] are Kafka `retry.backoff.ms` /
//! `retry.backoff.max.ms` (exponential wait after a retriable Produce;
//! default 100ms / 1s). [`ConsumerConfig::retry_backoff`] is the same pair
//! for retriable Fetch (preferred-replica redirects do not wait).
//! [`ProducerConfig::reconnect_backoff`] /
//! [`ProducerConfig::reconnect_backoff_max`] are Kafka `reconnect.backoff.ms` /
//! `reconnect.backoff.max.ms` (exponential wait after a failed broker TCP
//! connect; default 50ms / 1s, same as Java). The same pair is on
//! [`ConsumerConfig`] and [`AdminConfig`].
//! [`ProducerConfig::connections_max_idle`] / [`ConsumerConfig::connections_max_idle`] /
//! [`AdminConfig::connections_max_idle`] are Kafka `connections.max.idle.ms`
//! (close unused broker TCP connections; default 9 minutes, Java; zero never
//! closes for idle). Admin bootstrap RPCs and group/share coordinator sockets
//! reconnect after the same idle.
//! [`AdminConfig::retry_backoff`] / [`AdminConfig::retry_backoff_max`] are
//! Kafka `retry.backoff.ms` / `retry.backoff.max.ms` on admin RPCs
//! (`NOT_CONTROLLER`, coordinator moves, retriable IO; default 100ms / 1s).
//! [`ProducerConfig::transaction_timeout`] is Kafka `transaction.timeout.ms`
//! on InitProducerId (default 60s, same as Java).
//! [`ProducerConfig::metadata_max_age`] / [`ConsumerConfig::metadata_max_age`]
//! are Kafka `metadata.max.age.ms` (default 5 minutes; zero refreshes every
//! lookup).
//! [`ProducerConfig::allow_auto_create_topics`] /
//! [`ConsumerConfig::allow_auto_create_topics`] are Kafka
//! `allow.auto.create.topics` (this crate defaults to `false`; Java consumer
//! defaults to `true`).
//! [`ConsumerConfig::isolation`] is [`IsolationLevel`].
//! [`ConfigResourceType`] / [`ScramMechanism`] type admin config
//! resources and user SCRAM.
//!
//! # Admin
//!
//! [`Admin`] covers topics, partitions, configs, ACLs, groups, transactions,
//! quotas, telemetry, log dirs, and delegation tokens. See the [`admin`]
//! module. Still missing versus librdkafka: zstd and Kerberos (C libraries)
//! and Schema Registry. Tracker: `docs/gaps.md`.

#![forbid(unsafe_code)]

/// Admin client: topics, partitions, configs, ACLs, and the rest of Kafka admin.
pub mod admin;
pub(crate) mod cluster;
/// Shared config: [`Acks`], [`IsolationLevel`], [`Sasl`].
pub mod config;
/// Fetch client with manual partition assignment.
pub mod consumer;
/// Kafka and client error types.
pub mod error;
/// Consumer-group join / sync / heartbeat / commit.
pub mod group;
/// Produce and fetch interceptors.
pub mod interceptor;
/// Client counters, latency min/mean/max plus p50/p99, and per-topic rows: [`ProducerMetrics`], [`ConsumerMetrics`], [`ShareMetrics`].
pub mod metrics;
/// TCP and TLS broker connections.
pub mod net;
/// Kafka murmur2 partitioner.
pub mod partitioner;
/// Produce client.
pub mod producer;
/// Kafka protocol codecs. Public so integration tests can speak the wire.
pub mod protocol;
/// Share groups (KIP-932).
pub mod share;

pub use admin::{
    AclBinding, AclOperation, AclPermission, AclResourceType, ActiveProducer, Admin, AdminConfig,
    AlterConfig, AlterReplicaLogDirsDirectory, AlterReplicaLogDirsRequest,
    AlterReplicaLogDirsResponse, AlterReplicaLogDirsResponsePartition,
    AlterReplicaLogDirsResponseTopic, AlterReplicaLogDirsTopic, AlterShareGroupOffsetsPartition,
    AlterShareGroupOffsetsTopic, AlteredShareGroupOffsets, AlteredShareGroupOffsetsPartition,
    AlteredShareGroupOffsetsTopic, AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition,
    AssignReplicasToDirsRequest, AssignReplicasToDirsResponse,
    AssignReplicasToDirsResponseDirectory, AssignReplicasToDirsResponsePartition,
    AssignReplicasToDirsResponseTopic, AssignReplicasToDirsTopic, ClientQuotaAlteration,
    ClientQuotaAlterationResult, ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilterComponent,
    ClientQuotaOp, ClientQuotaValue, ClusterDescription, ConfigEntry, ConfigResource,
    ConfigResourceType, ConsumerGroupAssignment, ConsumerGroupMember, ConsumerGroupTopicPartitions,
    CreatableRenewer, CreateDelegationTokenRequest, CreateDelegationTokenResponse,
    DeletableGroupResult, DeleteShareGroupOffsetsTopic, DeletedShareGroupOffsets,
    DeletedShareGroupOffsetsTopic, DescribableLogDirTopic, DescribeDelegationTokenOwner,
    DescribeDelegationTokenRequest, DescribeDelegationTokenResponse, DescribeLogDirsPartition,
    DescribeLogDirsRequest, DescribeLogDirsResponse, DescribeLogDirsResult, DescribeLogDirsTopic,
    DescribeProducersPartition, DescribeShareGroupOffsetsGroup, DescribeShareGroupOffsetsTopic,
    DescribeTopicPartitionsResponse, DescribeUserScramCredentialsResult, DescribedConsumerGroup,
    DescribedDelegationToken, DescribedDelegationTokenRenewer, DescribedGroup,
    DescribedGroupMember, DescribedShareGroup, DescribedShareGroupOffsets,
    DescribedShareGroupOffsetsPartition, DescribedShareGroupOffsetsTopic, DescribedTopicPartition,
    DescribedTopicPartitions, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse,
    FeatureMetadata, FeatureUpdate, FeatureUpdateResult, FencedProducer, FinalizedVersionRange,
    GetTelemetrySubscriptionsResponse, ListedConfigResource, ListedGroup, MemberToRemove,
    NewPartitions, NewTopic, OffsetDeleteResult, OngoingReassignment, PartitionReassignment,
    ProducerIdBlock, PushTelemetryResponse, ReassignmentResult, RemovedMember,
    RenewDelegationTokenRequest, RenewDelegationTokenResponse, ScramCredentialInfo, ScramMechanism,
    ShareGroupAssignment, ShareGroupMember, ShareGroupTopicPartitions, SupportedVersionRange,
    TopicPartitionCursor, TransactionListing, TransactionState, TransactionTopic,
    UserScramCredentialDeletion, UserScramCredentialResult, UserScramCredentialUpsertion,
    ALTER_CONFIG_DELETE, ALTER_CONFIG_SET, AUTHORIZED_OPERATIONS_OMITTED, CONFIG_RESOURCE_BROKER,
    CONFIG_RESOURCE_BROKER_LOGGER, CONFIG_RESOURCE_CLIENT_METRICS, CONFIG_RESOURCE_GROUP,
    CONFIG_RESOURCE_TOPIC, QUOTA_MATCH_ANY, QUOTA_MATCH_DEFAULT, QUOTA_MATCH_EXACT, SCRAM_SHA_256,
    SCRAM_SHA_512,
};
pub use config::{Acks, AutoOffsetReset, IsolationLevel, Sasl};
pub use consumer::{
    Consumer, ConsumerConfig, ConsumerRecords, FetchedRecord, OffsetAndMetadata,
    OffsetAndTimestamp, PartitionInfo, RebalanceListener, TopicPartition, WakeupHandle,
};
pub use error::{Error, Result};
pub use group::{ConsumerGroup, ConsumerGroupMetadata};
pub use interceptor::{ConsumerInterceptor, ProducerInterceptor};
pub use metrics::{
    ConsumerMetrics, LatencyStats, ProducerMetrics, ShareMetrics, TopicFetchMetrics,
    TopicProduceMetrics,
};
pub use net::TlsConfig;
pub use partitioner::{
    murmur2, partition_for_key, DefaultPartitioner, Partitioner, PartitionerBox,
};
pub use producer::{ProduceRecord, Producer, ProducerConfig, RecordMetadata};
pub use protocol::acl::{ACL_OPERATION_ALL, ACL_PERMISSION_ALLOW, ACL_RESOURCE_TOPIC};
pub use protocol::admin::{DescribeConfigsResult, TopicResult};
pub use protocol::offsets::{EARLIEST_TIMESTAMP, LATEST_TIMESTAMP};
pub use protocol::oidc::OidcConfig;
pub use protocol::records::{Compression, Header, Record, RecordBatch};
pub use share::{
    ShareGroup, ShareRecord, ShareRecords, SHARE_ACK_ACCEPT, SHARE_ACK_REJECT, SHARE_ACK_RELEASE,
};

/// Software name sent in ApiVersions v3+.
pub const CLIENT_NAME: &str = "partitionline";
/// Crate version sent in ApiVersions v3+.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
