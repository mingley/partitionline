//! A Kafka client written in Rust. No C, no librdkafka.
//!
//! Send and fetch records, join a consumer group, gzip, snappy, lz4, SASL PLAIN,
//! SASL SCRAM-SHA-256, SASL SCRAM-SHA-512, SASL OAUTHBEARER (unsecured JWT or
//! OIDC client_credentials http/https token URL), TLS (rustls), fetch from
//! follower, KIP-848 groups, share groups,
//! idempotent and transactional produce, ListOffsets/seek, and admin
//! (topics, partitions, configs, ACLs, DeleteRecords, OffsetDelete,
//! AlterPartitionReassignments, ListPartitionReassignments, UpdateFeatures,
//! AlterUserScramCredentials, DescribeUserScramCredentials,
//! AlterClientQuotas, DescribeClientQuotas, DescribeProducers,
//! AllocateProducerIds, DescribeTransactions, ListTransactions,
//! UnregisterBroker, ConsumerGroupDescribe, DescribeGroups, ListGroups,
//! DeleteGroups, ShareGroupDescribe, DescribeShareGroupOffsets,
//! AlterShareGroupOffsets, DeleteShareGroupOffsets, DescribeTopicPartitions,
//! ListConfigResources, GetTelemetrySubscriptions, PushTelemetry,
//! AssignReplicasToDirs, AlterReplicaLogDirs, DescribeCluster).
//! See the crate README and `docs/gaps.md` for what is still missing.

#![forbid(unsafe_code)]

/// Admin client: CreateTopics, DeleteTopics, DescribeConfigs.
pub mod admin;
pub(crate) mod cluster;
/// Fetch client with manual partition assignment.
pub mod consumer;
/// Kafka and client error types.
pub mod error;
/// Consumer-group join / sync / heartbeat / commit.
pub mod group;
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
    ConsumerGroupAssignment, ConsumerGroupMember, ConsumerGroupTopicPartitions,
    DeletableGroupResult, DeleteShareGroupOffsetsTopic, DeletedShareGroupOffsets,
    DeletedShareGroupOffsetsTopic, DescribeProducersPartition, DescribeShareGroupOffsetsGroup,
    DescribeShareGroupOffsetsTopic, DescribeTopicPartitionsResponse,
    DescribeUserScramCredentialsResult, DescribedConsumerGroup, DescribedGroup,
    DescribedGroupMember, DescribedShareGroup, DescribedShareGroupOffsets,
    DescribedShareGroupOffsetsPartition, DescribedShareGroupOffsetsTopic, DescribedTopicPartition,
    DescribedTopicPartitions, FeatureUpdate, FeatureUpdateResult,
    GetTelemetrySubscriptionsResponse, ListedConfigResource, ListedGroup, NewTopic,
    OffsetDeleteResult, OngoingReassignment, PartitionReassignment, ProducerIdBlock,
    PushTelemetryResponse, ReassignmentResult, ScramCredentialInfo, ShareGroupAssignment,
    ShareGroupMember, ShareGroupTopicPartitions, TopicPartitionCursor, TransactionListing,
    TransactionState, TransactionTopic, UserScramCredentialDeletion, UserScramCredentialResult,
    UserScramCredentialUpsertion, ALTER_CONFIG_DELETE, ALTER_CONFIG_SET,
    AUTHORIZED_OPERATIONS_OMITTED, CONFIG_RESOURCE_BROKER, CONFIG_RESOURCE_BROKER_LOGGER,
    CONFIG_RESOURCE_CLIENT_METRICS, CONFIG_RESOURCE_GROUP, CONFIG_RESOURCE_TOPIC, QUOTA_MATCH_ANY,
    QUOTA_MATCH_DEFAULT, QUOTA_MATCH_EXACT, SCRAM_SHA_256, SCRAM_SHA_512,
};
pub use consumer::{Consumer, ConsumerConfig, FetchedRecord};
pub use error::{Error, Result};
pub use group::ConsumerGroup;
pub use net::TlsConfig;
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
