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
//! [`Producer::metrics`] is a snapshot of queued / acked / error counts.
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
//! [`Consumer::assign_topic`] assigns every partition. [`Consumer::seek`],
//! [`Consumer::seek_to_beginning`], and [`Consumer::seek_to_end`] move the
//! next fetch offset. [`Consumer::pause`] / [`Consumer::resume`] skip
//! partitions without dropping the assignment. [`Consumer::fetch`] talks to
//! every partition leader in parallel. [`Consumer::partitions_for`] /
//! [`Producer::partitions_for`] return Metadata (leader, replicas, ISR).
//! [`Consumer::wakeup`] interrupts fetch
//! (clone [`WakeupHandle`] for another task).
//! [`Consumer::offsets_for_times`] is Java `offsetsForTimes`.
//! [`Consumer::current_lag`] is Java `currentLag`.
//! [`Consumer::list_topics`] is cluster Metadata. [`Consumer::assign_many`]
//! / [`Consumer::unassign`] replace or drop a manual assignment.
//! [`Consumer::fetch_timeout`] / [`ConsumerGroup::poll_timeout`] /
//! [`ShareGroup::poll_timeout`] are Java `poll(Duration)`.
//! [`ProducerConfig::interceptor`] / [`ConsumerConfig::interceptor`] observe
//! or rewrite records (`close` / [`ConsumerInterceptor::on_commit`]).
//!
//! # Groups
//!
//! [`ConsumerGroup::join`] is classic range, [`ConsumerGroup::join_sticky`]
//! is sticky, [`ConsumerGroup::join_cooperative_sticky`] is KIP-429, and
//! [`ConsumerGroup::join_consumer`] is KIP-848. Each has a
//! `_topics` variant for several topics.
//! [`ConsumerConfig::group_instance_id`] is static membership.
//! [`ConsumerConfig::auto_offset_reset`] is used when OffsetFetch has no
//! committed offset. [`ShareGroup`] is KIP-932 (`join` / [`ShareGroup::join_topics`] /
//! [`ShareGroup::subscribe`] / [`ShareGroup::unsubscribe`]).
//! [`ConsumerGroup::commit_with_metadata`] sends [`OffsetAndMetadata`]
//! (leader epoch and a metadata string). [`ConsumerGroup::enforce_rebalance`]
//! rejoins on the next poll. [`ConsumerGroup::subscribe`] /
//! [`ConsumerGroup::unsubscribe`] change the topic list without dropping
//! the handle. [`ConsumerGroup::group_metadata`] is Java `ConsumerGroupMetadata`.
//! [`Producer::send_offsets_with_metadata`] / [`Producer::send_offsets_for_group`]
//! commit transactional offsets with epoch and metadata.
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
/// Client counters: [`ProducerMetrics`], [`ConsumerMetrics`], [`ShareMetrics`].
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
    AclBinding, ActiveProducer, Admin, AdminConfig, AlterConfig, AlterReplicaLogDirsDirectory,
    AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, AlterReplicaLogDirsResponsePartition,
    AlterReplicaLogDirsResponseTopic, AlterReplicaLogDirsTopic, AlterShareGroupOffsetsPartition,
    AlterShareGroupOffsetsTopic, AlteredShareGroupOffsets, AlteredShareGroupOffsetsPartition,
    AlteredShareGroupOffsetsTopic, AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition,
    AssignReplicasToDirsRequest, AssignReplicasToDirsResponse,
    AssignReplicasToDirsResponseDirectory, AssignReplicasToDirsResponsePartition,
    AssignReplicasToDirsResponseTopic, AssignReplicasToDirsTopic, ClientQuotaAlteration,
    ClientQuotaAlterationResult, ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilterComponent,
    ClientQuotaOp, ClientQuotaValue, ClusterDescription, ConfigEntry, ConfigResource,
    ConsumerGroupAssignment, ConsumerGroupMember, ConsumerGroupTopicPartitions, CreatableRenewer,
    CreateDelegationTokenRequest, CreateDelegationTokenResponse, DeletableGroupResult,
    DeleteShareGroupOffsetsTopic, DeletedShareGroupOffsets, DeletedShareGroupOffsetsTopic,
    DescribableLogDirTopic, DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, DescribeLogDirsPartition, DescribeLogDirsRequest,
    DescribeLogDirsResponse, DescribeLogDirsResult, DescribeLogDirsTopic,
    DescribeProducersPartition, DescribeShareGroupOffsetsGroup, DescribeShareGroupOffsetsTopic,
    DescribeTopicPartitionsResponse, DescribeUserScramCredentialsResult, DescribedConsumerGroup,
    DescribedDelegationToken, DescribedDelegationTokenRenewer, DescribedGroup,
    DescribedGroupMember, DescribedShareGroup, DescribedShareGroupOffsets,
    DescribedShareGroupOffsetsPartition, DescribedShareGroupOffsetsTopic, DescribedTopicPartition,
    DescribedTopicPartitions, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse,
    FeatureUpdate, FeatureUpdateResult, GetTelemetrySubscriptionsResponse, ListedConfigResource,
    ListedGroup, NewTopic, OffsetDeleteResult, OngoingReassignment, PartitionReassignment,
    ProducerIdBlock, PushTelemetryResponse, ReassignmentResult, RenewDelegationTokenRequest,
    RenewDelegationTokenResponse, ScramCredentialInfo, ShareGroupAssignment, ShareGroupMember,
    ShareGroupTopicPartitions, TopicPartitionCursor, TransactionListing, TransactionState,
    TransactionTopic, UserScramCredentialDeletion, UserScramCredentialResult,
    UserScramCredentialUpsertion, ALTER_CONFIG_DELETE, ALTER_CONFIG_SET,
    AUTHORIZED_OPERATIONS_OMITTED, CONFIG_RESOURCE_BROKER, CONFIG_RESOURCE_BROKER_LOGGER,
    CONFIG_RESOURCE_CLIENT_METRICS, CONFIG_RESOURCE_GROUP, CONFIG_RESOURCE_TOPIC, QUOTA_MATCH_ANY,
    QUOTA_MATCH_DEFAULT, QUOTA_MATCH_EXACT, SCRAM_SHA_256, SCRAM_SHA_512,
};
pub use config::{Acks, AutoOffsetReset, IsolationLevel, Sasl};
pub use consumer::{
    Consumer, ConsumerConfig, FetchedRecord, OffsetAndMetadata, OffsetAndTimestamp, PartitionInfo,
    RebalanceListener, TopicPartition, WakeupHandle,
};
pub use error::{Error, Result};
pub use group::{ConsumerGroup, ConsumerGroupMetadata};
pub use interceptor::{ConsumerInterceptor, ProducerInterceptor};
pub use metrics::{ConsumerMetrics, ProducerMetrics, ShareMetrics};
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
pub use share::{ShareGroup, ShareRecord, SHARE_ACK_ACCEPT, SHARE_ACK_REJECT, SHARE_ACK_RELEASE};

/// Software name sent in ApiVersions v3+.
pub const CLIENT_NAME: &str = "partitionline";
/// Crate version sent in ApiVersions v3+.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
