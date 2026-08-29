//! Kafka admin client: topics, configs, ACLs, groups, and cluster operations.
//!
//! [`Admin::connect`] / [`Admin::new`] negotiate ApiVersions. Methods that
//! must land on the controller retry on `NOT_CONTROLLER`. Group and
//! transaction methods retry on coordinator errors.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, TlsConfig};
use crate::protocol::acl::{
    decode_create_acls_response, decode_delete_acls_filter_results, decode_describe_acls_response,
    encode_create_acls_request, encode_delete_acls_request, encode_describe_acls_request,
};
use crate::protocol::admin::{
    decode_allocate_producer_ids_response, decode_alter_client_quotas_response,
    decode_alter_configs_resource_results, decode_alter_partition_reassignments_response,
    decode_alter_replica_log_dirs_response, decode_alter_share_group_offsets_response,
    decode_alter_user_scram_credentials_response, decode_assign_replicas_to_dirs_response,
    decode_consumer_group_describe_response, decode_create_delegation_token_response,
    decode_create_partitions_response, decode_create_topics_response,
    decode_delete_groups_response, decode_delete_records_topics_response,
    decode_delete_share_group_offsets_response, decode_delete_topics_response,
    decode_describe_client_quotas_response, decode_describe_cluster_response,
    decode_describe_configs_response, decode_describe_delegation_token_response,
    decode_describe_groups_response, decode_describe_log_dirs_response,
    decode_describe_producers_response, decode_describe_share_group_offsets_response,
    decode_describe_topic_partitions_response, decode_describe_transactions_response,
    decode_describe_user_scram_credentials_response, decode_expire_delegation_token_response,
    decode_get_telemetry_subscriptions_response, decode_incremental_alter_configs_resource_results,
    decode_list_config_resources_response, decode_list_groups_response,
    decode_list_partition_reassignments_response, decode_list_transactions_response,
    decode_push_telemetry_response, decode_renew_delegation_token_response,
    decode_share_group_describe_response, decode_unregister_broker_response,
    decode_update_features_response, encode_allocate_producer_ids_request,
    encode_alter_client_quotas_request, encode_alter_configs_resources_request,
    encode_alter_partition_reassignments_request, encode_alter_replica_log_dirs_request,
    encode_alter_share_group_offsets_request, encode_alter_user_scram_credentials_request,
    encode_assign_replicas_to_dirs_request, encode_consumer_group_describe_request,
    encode_create_delegation_token_request, encode_create_partitions_request,
    encode_create_topics_request, encode_delete_groups_request,
    encode_delete_records_topics_request, encode_delete_share_group_offsets_request,
    encode_delete_topics_states_request, encode_describe_client_quotas_request,
    encode_describe_cluster_request, encode_describe_configs_request,
    encode_describe_delegation_token_request, encode_describe_groups_request,
    encode_describe_log_dirs_request, encode_describe_producers_topics_request,
    encode_describe_share_group_offsets_request, encode_describe_topic_partitions_request,
    encode_describe_transactions_request, encode_describe_user_scram_credentials_request,
    encode_expire_delegation_token_request, encode_get_telemetry_subscriptions_request,
    encode_incremental_alter_configs_resources_request, encode_list_config_resources_request,
    encode_list_groups_request, encode_list_partition_reassignments_request,
    encode_list_transactions_request, encode_push_telemetry_request,
    encode_renew_delegation_token_request, encode_share_group_describe_request,
    encode_unregister_broker_request, encode_update_features_request, AlterConfigsResource,
    AlterableResource, CreatableTopic, CreatePartitionsTopic, CreateTopicsRequest,
    DeleteRecordsPartition, DeleteRecordsTopic, DeleteTopicState, DescribeConfigsResource,
    DescribeConfigsResult, DescribeProducersTopicRequest, FeatureUpdateKey, ListReassignmentTopic,
    ReassignablePartition, ReassignableTopic, ReplicaAssignment, ScramCredentialDeletion,
    ScramCredentialUpsertion, TopicConfig, TopicResult, RESOURCE_BROKER, RESOURCE_BROKER_LOGGER,
    RESOURCE_CLIENT_METRICS, RESOURCE_GROUP, RESOURCE_TOPIC,
};
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, encode_api_versions_request,
    encode_metadata_request_topics, ApiVersion, MetadataRequestTopic, MetadataResponse,
};
use crate::protocol::api_keys::{
    pick_version, ALLOCATE_PRODUCER_IDS, ALTER_CLIENT_QUOTAS, ALTER_CONFIGS,
    ALTER_PARTITION_REASSIGNMENTS, ALTER_REPLICA_LOG_DIRS, ALTER_SHARE_GROUP_OFFSETS,
    ALTER_USER_SCRAM_CREDENTIALS, API_VERSIONS, ASSIGN_REPLICAS_TO_DIRS, CONSUMER_GROUP_DESCRIBE,
    CREATE_ACLS, CREATE_DELEGATION_TOKEN, CREATE_PARTITIONS, CREATE_TOPICS, DELETE_ACLS,
    DELETE_GROUPS, DELETE_RECORDS, DELETE_SHARE_GROUP_OFFSETS, DELETE_TOPICS, DESCRIBE_ACLS,
    DESCRIBE_CLIENT_QUOTAS, DESCRIBE_CLUSTER, DESCRIBE_CONFIGS, DESCRIBE_DELEGATION_TOKEN,
    DESCRIBE_GROUPS, DESCRIBE_LOG_DIRS, DESCRIBE_PRODUCERS, DESCRIBE_SHARE_GROUP_OFFSETS,
    DESCRIBE_TOPIC_PARTITIONS, DESCRIBE_TRANSACTIONS, DESCRIBE_USER_SCRAM_CREDENTIALS,
    EXPIRE_DELEGATION_TOKEN, FIND_COORDINATOR, GET_TELEMETRY_SUBSCRIPTIONS,
    INCREMENTAL_ALTER_CONFIGS, INIT_PRODUCER_ID, LEAVE_GROUP, LIST_CONFIG_RESOURCES, LIST_GROUPS,
    LIST_OFFSETS, LIST_PARTITION_REASSIGNMENTS, LIST_TRANSACTIONS, METADATA, OFFSET_COMMIT,
    OFFSET_DELETE, OFFSET_FETCH, PUSH_TELEMETRY, RENEW_DELEGATION_TOKEN, SHARE_GROUP_DESCRIBE,
    UNREGISTER_BROKER, UPDATE_FEATURES, WRITE_TXN_MARKERS,
};
use crate::protocol::group::{
    decode_find_coordinator_response, decode_find_coordinator_response_coordinators,
    decode_leave_group_response_version, decode_offset_commit_response,
    decode_offset_delete_response, decode_offset_fetch_groups_response,
    decode_offset_fetch_response, encode_find_coordinator_request_keys,
    encode_find_coordinator_request_typed, encode_leave_group_request_members,
    encode_offset_commit_request, encode_offset_delete_request, encode_offset_fetch_groups_request,
    encode_offset_fetch_request, LeaveGroupMember, OffsetDeleteTopic, OffsetFetchGroup,
    COORDINATOR_GROUP, COORDINATOR_TRANSACTION,
};
use crate::protocol::idem::{decode_init_producer_id_response, encode_init_producer_id_request};
use crate::protocol::offsets::{
    decode_list_offsets_topics_response, encode_list_offsets_topics_request,
    ListOffsetsPartitionRequest, ListOffsetsResponsePartition, ListOffsetsTopicRequest,
};
use crate::protocol::sasl;
use crate::protocol::txn::{
    decode_write_txn_markers_response, encode_write_txn_markers_request, WritableTxnMarker,
    WritableTxnMarkerTopic,
};

pub use crate::protocol::acl::{
    AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation,
    AclPatternType, AclPermission, AclResourceType, DeletedAclsFilterResult, ResourcePattern,
    ResourcePatternFilter,
};
pub use crate::protocol::admin::{
    ActiveProducer, AlterConfig, AlterConfigOp, AlterConfigOpType, AlterConfigsResourceResult,
    AlterReplicaLogDirsDirectory, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse,
    AlterReplicaLogDirsResponsePartition, AlterReplicaLogDirsResponseTopic,
    AlterReplicaLogDirsTopic, AlterShareGroupOffsetsPartition, AlterShareGroupOffsetsTopic,
    AlteredShareGroupOffsets, AlteredShareGroupOffsetsPartition, AlteredShareGroupOffsetsTopic,
    AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition, AssignReplicasToDirsRequest,
    AssignReplicasToDirsResponse, AssignReplicasToDirsResponseDirectory,
    AssignReplicasToDirsResponsePartition, AssignReplicasToDirsResponseTopic,
    AssignReplicasToDirsTopic, ClientQuotaAlteration, ClientQuotaAlterationResult,
    ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilter, ClientQuotaFilterComponent,
    ClientQuotaOp, ClientQuotaValue, ClusterDescription, Config, ConfigEntry, ConfigSource,
    ConfigSynonym, ConfigType, ConsumerGroupAssignment, ConsumerGroupMember,
    ConsumerGroupTopicPartitions, CreatableRenewer, CreateDelegationTokenRequest,
    CreateDelegationTokenResponse, DeletableGroupResult, DeleteShareGroupOffsetsTopic,
    DeletedShareGroupOffsets, DeletedShareGroupOffsetsTopic, DescribableLogDirTopic,
    DescribeClusterBroker, DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, DescribeLogDirsPartition, DescribeLogDirsRequest,
    DescribeLogDirsResponse, DescribeLogDirsResult, DescribeLogDirsTopic,
    DescribeProducersPartition, DescribeProducersTopic, DescribeShareGroupOffsetsGroup,
    DescribeShareGroupOffsetsTopic, DescribeTopicPartitionsResponse,
    DescribeUserScramCredentialsResult, DescribedConsumerGroup, DescribedDelegationToken,
    DescribedDelegationTokenRenewer, DescribedGroup, DescribedGroupMember, DescribedShareGroup,
    DescribedShareGroupOffsets, DescribedShareGroupOffsetsPartition,
    DescribedShareGroupOffsetsTopic, DescribedTopicPartition, DescribedTopicPartitions,
    EndpointType, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse,
    GetTelemetrySubscriptionsResponse, GroupState, GroupType, ListedConfigResource, ListedGroup,
    PushTelemetryRequest, PushTelemetryResponse, RenewDelegationTokenRequest,
    RenewDelegationTokenResponse, ScramCredentialInfo, ScramMechanism, ShareGroupAssignment,
    ShareGroupMember, ShareGroupTopicPartitions, TopicPartitionCursor, TransactionListing,
    TransactionState, TransactionTopic, UpgradeType, ALTER_CONFIG_APPEND, ALTER_CONFIG_DELETE,
    ALTER_CONFIG_SET, ALTER_CONFIG_SUBTRACT, AUTHORIZED_OPERATIONS_OMITTED, CONFIG_SOURCE_DEFAULT,
    CONFIG_SOURCE_DYNAMIC_BROKER, CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER,
    CONFIG_SOURCE_DYNAMIC_CLIENT_METRICS, CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
    CONFIG_SOURCE_DYNAMIC_GROUP, CONFIG_SOURCE_DYNAMIC_TOPIC, CONFIG_SOURCE_STATIC_BROKER,
    CONFIG_SOURCE_UNKNOWN, CONFIG_TYPE_BOOLEAN, CONFIG_TYPE_CLASS, CONFIG_TYPE_DOUBLE,
    CONFIG_TYPE_INT, CONFIG_TYPE_LIST, CONFIG_TYPE_LONG, CONFIG_TYPE_PASSWORD, CONFIG_TYPE_SHORT,
    CONFIG_TYPE_STRING, CONFIG_TYPE_UNKNOWN, ENDPOINT_TYPE_BROKERS, ENDPOINT_TYPE_CONTROLLERS,
    QUOTA_MATCH_ANY, QUOTA_MATCH_DEFAULT, QUOTA_MATCH_EXACT,
    RESOURCE_BROKER as CONFIG_RESOURCE_BROKER,
    RESOURCE_BROKER_LOGGER as CONFIG_RESOURCE_BROKER_LOGGER,
    RESOURCE_CLIENT_METRICS as CONFIG_RESOURCE_CLIENT_METRICS,
    RESOURCE_GROUP as CONFIG_RESOURCE_GROUP, RESOURCE_TOPIC as CONFIG_RESOURCE_TOPIC,
    SCRAM_SHA_256, SCRAM_SHA_512, SCRAM_UNKNOWN, UNKNOWN_VOLUME_BYTES, UPGRADE_TYPE_SAFE_DOWNGRADE,
    UPGRADE_TYPE_UNSAFE_DOWNGRADE, UPGRADE_TYPE_UPGRADE,
};
pub use crate::protocol::group::OffsetDeleteResult;

/// Bootstrap, identity, SASL, and TLS for [`Admin`].
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Bootstrap brokers, `host:port`.
    pub bootstrap: Vec<String>,
    /// Kafka `client.id`.
    pub client_id: String,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Kafka `reconnect.backoff.ms`. Wait after a failed TCP/TLS/SASL
    /// connect to a broker before the next attempt. Default 50ms (Java).
    /// Zero retries immediately. Grows as `base * 2^n` up to
    /// [`Self::reconnect_backoff_max`].
    pub reconnect_backoff: Duration,
    /// Kafka `reconnect.backoff.max.ms`. Cap on [`Self::reconnect_backoff`]
    /// exponential growth. Default 1s (Java).
    pub reconnect_backoff_max: Duration,
    /// Kafka `connections.max.idle.ms`. Close a broker TCP connection that
    /// has been unused for this long and reconnect on the next RPC. Default
    /// 9 minutes (Java). Zero never closes for idle.
    pub connections_max_idle: Duration,
    /// Kafka `retry.backoff.ms`. Wait after a retriable admin error
    /// (`NOT_CONTROLLER`, coordinator moves, IO) before the next attempt.
    /// Default 100ms (Java / librdkafka). Zero retries immediately.
    /// Grows as `base * 2^n` up to [`Self::retry_backoff_max`]. Distinct
    /// from [`Self::reconnect_backoff`] (TCP/handshake failures).
    pub retry_backoff: Duration,
    /// Kafka `retry.backoff.max.ms`. Cap on [`Self::retry_backoff`]
    /// exponential growth. Default 1s.
    pub retry_backoff_max: Duration,
    /// SASL PLAIN `(username, password)`.
    pub sasl_plain: Option<(String, String)>,
    /// SASL SCRAM-SHA-256 `(username, password)`.
    pub sasl_scram: Option<(String, String)>,
    /// SASL SCRAM-SHA-512 `(username, password)`.
    pub sasl_scram_sha512: Option<(String, String)>,
    /// Unsecured OAUTHBEARER token.
    pub sasl_oauthbearer: Option<String>,
    /// OIDC client-credentials token endpoint.
    pub sasl_oauthbearer_oidc: Option<crate::OidcConfig>,
    /// rustls. No OpenSSL.
    pub tls: Option<TlsConfig>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            bootstrap: vec!["127.0.0.1:9092".into()],
            client_id: "partitionline".into(),
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            reconnect_backoff: crate::config::DEFAULT_RECONNECT_BACKOFF,
            reconnect_backoff_max: crate::config::DEFAULT_RECONNECT_BACKOFF_MAX,
            connections_max_idle: crate::config::DEFAULT_CONNECTIONS_MAX_IDLE,
            retry_backoff: crate::config::DEFAULT_RETRY_BACKOFF,
            retry_backoff_max: crate::config::DEFAULT_RETRY_BACKOFF_MAX,
            sasl_plain: None,
            sasl_scram: None,
            sasl_scram_sha512: None,
            sasl_oauthbearer: None,
            sasl_oauthbearer_oidc: None,
            tls: None,
        }
    }
}

impl AdminConfig {
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

    /// Kafka `reconnect.backoff.ms`. Wait after a failed broker connect.
    ///
    /// Default 50ms (Java). Zero retries immediately. Combined with
    /// [`Self::reconnect_backoff_max`] this is exponential (`base * 2^n`),
    /// no jitter.
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
    /// Default 9 minutes (Java). Zero never closes for idle. The next admin
    /// RPC reconnects.
    #[must_use]
    pub fn connections_max_idle(mut self, idle: Duration) -> Self {
        self.connections_max_idle = idle;
        self
    }

    /// Kafka `retry.backoff.ms`. Wait after a retriable admin error.
    ///
    /// Default 100ms. Zero retries immediately. Combined with
    /// [`Self::retry_backoff_max`] this is exponential (`base * 2^n`),
    /// no jitter. Failed TCP/handshake still uses [`Self::reconnect_backoff`].
    #[must_use]
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }

    /// Kafka `retry.backoff.max.ms`. Cap on exponential admin retry waits.
    ///
    /// Default 1s. Raised to [`Self::retry_backoff`] when set lower.
    #[must_use]
    pub fn retry_backoff_max(mut self, backoff: Duration) -> Self {
        self.retry_backoff_max = backoff;
        self
    }
}

/// Topic to create (`CreateTopics`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    /// Topic name.
    pub name: String,
    /// Partition count, or `-1` for broker `num.partitions` (KIP-464) or
    /// when `assignments` is set (Java `NewTopic(String, Map)`).
    pub num_partitions: i32,
    /// Replication factor, or `-1` for broker `default.replication.factor`
    /// (KIP-464) or when `assignments` is set (Java `NewTopic(String, Map)`).
    pub replication_factor: i16,
    /// Manual replica assignments `(partition_index, broker_ids)`.
    ///
    /// Empty is Java `NewTopic(String, int, short)` (broker assigns).
    /// Non-empty is Java `NewTopic(String, Map<Integer, List<Integer>>)`.
    pub assignments: Vec<(i32, Vec<i32>)>,
    /// Optional topic configs `(name, value)`.
    pub configs: Vec<(String, Option<String>)>,
}

impl NewTopic {
    /// `name` with partition count and replication factor.
    ///
    /// Assignments are empty; the broker places replicas. `-1` on either
    /// count is Java `Optional.empty()` (KIP-464 broker default).
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            assignments: Vec::new(),
            configs: Vec::new(),
        }
    }

    /// Java `NewTopic(String, Optional.empty(), Optional.empty())` (KIP-464).
    ///
    /// Sends NumPartitions `-1` and ReplicationFactor `-1` with an empty
    /// Assignments array so the broker uses `num.partitions` /
    /// `default.replication.factor`.
    #[must_use]
    pub fn broker_defaults(name: impl Into<String>) -> Self {
        Self::new(name, -1, -1)
    }

    /// Java `NewTopic(String, Map<Integer, List<Integer>>)`.
    ///
    /// `num_partitions` and `replication_factor` are `-1`. Each pair is
    /// partition index → replica broker ids.
    #[must_use]
    pub fn with_assignments<A, B>(name: impl Into<String>, assignments: A) -> Self
    where
        A: IntoIterator<Item = (i32, B)>,
        B: IntoIterator<Item = i32>,
    {
        Self {
            name: name.into(),
            num_partitions: -1,
            replication_factor: -1,
            assignments: assignments
                .into_iter()
                .map(|(partition, brokers)| (partition, brokers.into_iter().collect()))
                .collect(),
            configs: Vec::new(),
        }
    }

    /// Topic config `(name, value)`.
    #[must_use]
    pub fn config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((name.into(), Some(value.into())));
        self
    }

    /// Java `NewTopic.configs(Map)`.
    ///
    /// Replaces any configs set by [`Self::config`]. Each pair is
    /// `(name, value)`.
    #[must_use]
    pub fn configs<I, K, V>(mut self, configs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.configs = configs
            .into_iter()
            .map(|(k, v)| (k.into(), Some(v.into())))
            .collect();
        self
    }

    /// Java `NewTopic.name`.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `NewTopic.numPartitions` (`-1` is broker default / assignments).
    #[must_use]
    pub fn num_partitions(&self) -> i32 {
        self.num_partitions
    }

    /// Java `NewTopic.replicationFactor` (`-1` is broker default / assignments).
    #[must_use]
    pub fn replication_factor(&self) -> i16 {
        self.replication_factor
    }

    /// Java `NewTopic.replicasAssignments` (`None` is Java `null`).
    #[must_use]
    pub fn replicas_assignments(&self) -> Option<&[(i32, Vec<i32>)]> {
        if self.assignments.is_empty() {
            None
        } else {
            Some(self.assignments.as_slice())
        }
    }
}

/// Java `RecordsToDelete` for [`Admin::delete_records`].
///
/// Converts to the DeleteRecords Offset INT64: records strictly before
/// this offset are deleted; records at or after it are kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordsToDelete {
    offset: i64,
}

impl RecordsToDelete {
    /// Java `RecordsToDelete.beforeOffset(long)`.
    #[must_use]
    pub const fn before_offset(offset: i64) -> Self {
        Self { offset }
    }

    /// DeleteRecords Offset INT64 (Java `RecordsToDelete.beforeOffset()`).
    #[must_use]
    pub const fn offset(self) -> i64 {
        self.offset
    }
}

impl From<RecordsToDelete> for i64 {
    fn from(records: RecordsToDelete) -> Self {
        records.offset
    }
}

/// Java `DeletedRecords` for [`Admin::delete_records`].
///
/// `low_watermark` is Java `lowWatermark()`. `error_code` is the
/// per-partition DeleteRecords ErrorCode (Java surfaces non-zero via
/// the future).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletedRecords {
    /// New log start offset after the delete.
    pub low_watermark: i64,
    /// Per-partition ErrorCode (`0` is success).
    pub error_code: i16,
}

impl DeletedRecords {
    /// Java `DeletedRecords(long)` (`error_code` 0).
    #[must_use]
    pub const fn new(low_watermark: i64) -> Self {
        Self {
            low_watermark,
            error_code: 0,
        }
    }

    /// Low watermark plus per-partition ErrorCode.
    #[must_use]
    pub const fn with_error_code(low_watermark: i64, error_code: i16) -> Self {
        Self {
            low_watermark,
            error_code,
        }
    }

    /// Java `DeletedRecords.lowWatermark`.
    #[must_use]
    pub const fn low_watermark(self) -> i64 {
        self.low_watermark
    }

    /// Per-partition DeleteRecords ErrorCode.
    #[must_use]
    pub const fn error_code(self) -> i16 {
        self.error_code
    }
}

impl From<(i64, i16)> for DeletedRecords {
    fn from((low_watermark, error_code): (i64, i16)) -> Self {
        Self {
            low_watermark,
            error_code,
        }
    }
}

impl From<DeletedRecords> for (i64, i16) {
    fn from(deleted: DeletedRecords) -> Self {
        (deleted.low_watermark, deleted.error_code)
    }
}

/// Kafka `org.apache.kafka.common.Uuid` (16 bytes; `toString` is base64url).
///
/// Wire TopicId / ClientInstanceId stay `[u8; 16]`. [`Self::from_string`] /
/// [`Display`] match Java `fromString` / `toString` (`URL_SAFE` without
/// padding). Kafka 4.1 Raft voter APIs are not spoken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Uuid([u8; 16]);

impl Uuid {
    /// Java `Uuid.ZERO_UUID` (`0, 0`).
    pub const ZERO: Self = Self([0; 16]);

    /// Java `Uuid.ONE_UUID` (`0, 1`). Also [`Self::METADATA_TOPIC_ID`].
    pub const ONE: Self = Self([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    /// Java `Uuid.METADATA_TOPIC_ID` (KRaft metadata topic).
    pub const METADATA_TOPIC_ID: Self = Self::ONE;

    /// Wrap a 16-byte Kafka UUID (big-endian most then least).
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// 16-byte Kafka UUID.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    /// 16-byte Kafka UUID.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Java `Uuid(long, long)` (`mostSignificantBits`, `leastSignificantBits`).
    #[must_use]
    pub fn from_parts(most: i64, least: i64) -> Self {
        let mut bytes = [0u8; 16];
        if let Some(hi) = bytes.first_chunk_mut::<8>() {
            *hi = most.to_be_bytes();
        }
        if let Some(lo) = bytes.last_chunk_mut::<8>() {
            *lo = least.to_be_bytes();
        }
        Self(bytes)
    }

    /// Java `getMostSignificantBits`.
    #[must_use]
    pub fn most_significant_bits(self) -> i64 {
        match self.0.first_chunk::<8>() {
            Some(hi) => i64::from_be_bytes(*hi),
            None => 0,
        }
    }

    /// Java `getLeastSignificantBits`.
    #[must_use]
    pub fn least_significant_bits(self) -> i64 {
        match self.0.last_chunk::<8>() {
            Some(lo) => i64::from_be_bytes(*lo),
            None => 0,
        }
    }

    /// Java `Uuid.fromString` (base64url; optional padding).
    pub fn from_string(s: &str) -> Result<Self> {
        if s.len() > 24 {
            let prefix = s.get(..24).unwrap_or(s);
            return Err(Error::protocol(format!(
                "Uuid string with prefix `{prefix}` is too long to be decoded as a base64 UUID"
            )));
        }
        let decoded =
            match base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, s) {
                Ok(b) => b,
                Err(_) => base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE, s)
                    .map_err(|_| {
                        Error::protocol(format!("Uuid string `{s}` is not a base64url UUID"))
                    })?,
            };
        let n = decoded.len();
        let bytes = <[u8; 16]>::try_from(decoded).map_err(|_| {
            Error::protocol(format!(
                "Uuid string `{s}` decoded as {n} bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID"
            ))
        })?;
        Ok(Self(bytes))
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            self.0.as_slice(),
        ))
    }
}

impl From<[u8; 16]> for Uuid {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<Uuid> for [u8; 16] {
    fn from(id: Uuid) -> Self {
        id.0
    }
}

impl std::str::FromStr for Uuid {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_string(s)
    }
}

impl PartialOrd for Uuid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Uuid {
    fn cmp(&self, other: &Self) -> Ordering {
        // Java `Uuid.compareTo` compares the two longs as signed values.
        self.most_significant_bits()
            .cmp(&other.most_significant_bits())
            .then(
                self.least_significant_bits()
                    .cmp(&other.least_significant_bits()),
            )
    }
}

/// Java `org.apache.kafka.common.TopicCollection` (`ofTopicNames` /
/// `ofTopicIds`).
///
/// [`Admin::describe_topics_for`] / [`Admin::delete_topics_for`] dispatch
/// names to DescribeTopicPartitions (Metadata fallback) and ids to
/// Metadata v10+ / DeleteTopics v6. Existing
/// [`Admin::describe_topics_by_id`] / [`Admin::delete_topics_by_id`] keep
/// `&[[u8; 16]]` so `describe_topics_by_id(&[])` still infers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicCollection {
    /// Java `TopicCollection.TopicNameCollection`.
    Names(Vec<String>),
    /// Java `TopicCollection.TopicIdCollection`.
    Ids(Vec<Uuid>),
}

impl TopicCollection {
    /// Java `TopicCollection.ofTopicNames`.
    #[must_use]
    pub fn of_topic_names(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Names(names.into_iter().map(Into::into).collect())
    }

    /// Java `TopicCollection.ofTopicIds`.
    ///
    /// `ids` items are [`Uuid`] or `[u8; 16]` ([`From`]). Empty `&[]`
    /// does not infer; use [`Self::Ids`] with `Vec::new` or
    /// `Vec::<Uuid>::new()`.
    #[must_use]
    pub fn of_topic_ids<I, Id>(ids: I) -> Self
    where
        I: IntoIterator<Item = Id>,
        Id: Into<Uuid>,
    {
        Self::Ids(ids.into_iter().map(Into::into).collect())
    }

    /// Java `TopicNameCollection.topicNames` (`None` when this is ids).
    #[must_use]
    pub fn topic_names(&self) -> Option<&[String]> {
        match self {
            Self::Names(names) => Some(names.as_slice()),
            Self::Ids(_) => None,
        }
    }

    /// Java `TopicIdCollection.topicIds` (`None` when this is names).
    #[must_use]
    pub fn topic_ids(&self) -> Option<&[Uuid]> {
        match self {
            Self::Ids(ids) => Some(ids.as_slice()),
            Self::Names(_) => None,
        }
    }
}

/// One topic from [`Admin::list_topics`] (Java `TopicListing`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicListing {
    /// Topic name.
    pub name: String,
    /// Topic id (Metadata v10+), or zeros.
    pub topic_id: [u8; 16],
    /// Internal topic (for example `__consumer_offsets`).
    pub is_internal: bool,
}

impl TopicListing {
    /// Topic `name` with optional id and internal flag.
    #[must_use]
    pub fn new(name: impl Into<String>, topic_id: impl Into<[u8; 16]>, is_internal: bool) -> Self {
        Self {
            name: name.into(),
            topic_id: topic_id.into(),
            is_internal,
        }
    }

    /// Java `TopicListing.name`.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `TopicListing.topicId`.
    #[must_use]
    pub fn topic_id(&self) -> Uuid {
        Uuid::from_bytes(self.topic_id)
    }

    /// Java `TopicListing.isInternal`.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        self.is_internal
    }
}

/// One group from [`Admin::describe_consumer_groups`] (Java
/// `ConsumerGroupDescription`).
///
/// Kafka 4.0 `DescribeConsumerGroupsHandler` sends ConsumerGroupDescribe
/// (api 69) first. Per-group [`crate::error::UNSUPPORTED_VERSION`] (35) or
/// [`crate::error::GROUP_ID_NOT_FOUND`] (69), a broker that does not
/// advertise api 69, or an RPC-level [`Error::Unsupported`], fall back to
/// DescribeGroups (api 15).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerGroupDescription {
    /// New consumer protocol (KIP-848) via ConsumerGroupDescribe.
    Consumer(DescribedConsumerGroup),
    /// Classic protocol via DescribeGroups.
    Classic(DescribedGroup),
}

impl ConsumerGroupDescription {
    /// Kafka `group.id`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        match self {
            Self::Consumer(g) => g.group_id.as_str(),
            Self::Classic(g) => g.group_id.as_str(),
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        match self {
            Self::Consumer(g) => g.error_code,
            Self::Classic(g) => g.error_code,
        }
    }

    /// Group state name from the broker.
    #[must_use]
    pub fn group_state(&self) -> &str {
        match self {
            Self::Consumer(g) => g.group_state.as_str(),
            Self::Classic(g) => g.group_state.as_str(),
        }
    }

    /// Bitfield of authorized operations, or [`AUTHORIZED_OPERATIONS_OMITTED`].
    #[must_use]
    pub fn authorized_operations(&self) -> i32 {
        match self {
            Self::Consumer(g) => g.authorized_operations,
            Self::Classic(g) => g.authorized_operations,
        }
    }

    /// `true` when this came from ConsumerGroupDescribe (api 69).
    #[must_use]
    pub fn is_consumer_protocol(&self) -> bool {
        matches!(self, Self::Consumer(_))
    }

    /// Java `ConsumerGroupDescription.isSimpleConsumerGroup`.
    ///
    /// [`DescribeConsumerGroupsHandler`](https://github.com/apache/kafka/blob/4.0.0/clients/src/main/java/org/apache/kafka/clients/admin/internals/DescribeConsumerGroupsHandler.java)
    /// is `false` for api 69 and empty classic `ProtocolType` for api 15.
    #[must_use]
    pub fn is_simple_consumer_group(&self) -> bool {
        match self {
            Self::Consumer(_) => false,
            Self::Classic(g) => g.is_simple_consumer_group(),
        }
    }

    /// Java `ConsumerGroupDescription.partitionAssignor`.
    #[must_use]
    pub fn partition_assignor(&self) -> &str {
        match self {
            Self::Consumer(g) => g.assignor_name(),
            Self::Classic(g) => g.protocol_data(),
        }
    }

    /// Java `ConsumerGroupDescription.type`.
    #[must_use]
    pub fn group_type(&self) -> GroupType {
        match self {
            Self::Consumer(_) => GroupType::Consumer,
            Self::Classic(_) => GroupType::Classic,
        }
    }

    /// Java `ConsumerGroupDescription.groupEpoch` (empty for CLASSIC groups).
    #[must_use]
    pub fn group_epoch(&self) -> Option<i32> {
        match self {
            Self::Consumer(g) => Some(g.group_epoch()),
            Self::Classic(_) => None,
        }
    }

    /// Java `ConsumerGroupDescription.targetAssignmentEpoch` (empty for CLASSIC groups).
    #[must_use]
    pub fn target_assignment_epoch(&self) -> Option<i32> {
        match self {
            Self::Consumer(g) => Some(g.assignment_epoch()),
            Self::Classic(_) => None,
        }
    }
}

/// One topic from [`Admin::describe_topics`] (Java `TopicDescription`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDescription {
    /// Topic name (empty when a TopicId describe returns no name).
    pub name: String,
    /// Topic id (Metadata v10+), or zeros.
    pub topic_id: [u8; 16],
    /// Internal topic.
    pub is_internal: bool,
    /// Per-topic Metadata error (`0` is success).
    pub error_code: i16,
    /// Partitions (empty when [`Self::error_code`] is not `0`).
    pub partitions: Vec<crate::PartitionInfo>,
    /// Topic authorized operations (Metadata v8+), or
    /// [`AUTHORIZED_OPERATIONS_OMITTED`] when not requested.
    pub authorized_operations: i32,
}

impl TopicDescription {
    /// Topic `name` with id, internal flag, error, and partitions.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        topic_id: impl Into<[u8; 16]>,
        is_internal: bool,
        error_code: i16,
        partitions: Vec<crate::PartitionInfo>,
    ) -> Self {
        Self {
            name: name.into(),
            topic_id: topic_id.into(),
            is_internal,
            error_code,
            partitions,
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    /// Java `TopicDescription.name`.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `TopicDescription.topicId`.
    #[must_use]
    pub fn topic_id(&self) -> Uuid {
        Uuid::from_bytes(self.topic_id)
    }

    /// Java `TopicDescription.isInternal`.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        self.is_internal
    }

    /// Per-topic Metadata error (`0` is success). Java `TopicDescription`
    /// throws instead of storing this.
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Java `TopicDescription.partitions`.
    #[must_use]
    pub fn partitions(&self) -> &[crate::PartitionInfo] {
        &self.partitions
    }

    /// Java `TopicDescription.authorizedOperations` as the Metadata
    /// bitfield, or [`AUTHORIZED_OPERATIONS_OMITTED`].
    #[must_use]
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }
}

impl TopicResult {
    /// Java `CreateTopicsResult.topicId` (KIP-516). Zero when the broker
    /// omitted TopicId (CreateTopics below v7 / DeleteTopics below v6, or
    /// an error).
    #[must_use]
    pub fn topic_id(&self) -> Uuid {
        Uuid::from_bytes(self.topic_id)
    }
}

impl GetTelemetrySubscriptionsResponse {
    /// Java `clientInstanceId` (KIP-714). Wire field stays `[u8; 16]`.
    #[must_use]
    pub fn client_instance_id(&self) -> Uuid {
        Uuid::from_bytes(self.client_instance_id)
    }
}

/// One replica: topic, partition, and broker id (Java `TopicPartitionReplica`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartitionReplica {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Broker that hosts this replica.
    pub broker_id: i32,
}

impl TopicPartitionReplica {
    /// Topic `topic`, partition `partition`, on `broker_id`.
    #[must_use]
    pub fn new(topic: impl Into<String>, partition: i32, broker_id: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            broker_id,
        }
    }

    /// Java `TopicPartitionReplica.topic`.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Java `TopicPartitionReplica.partition`.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Java `TopicPartitionReplica.brokerId`.
    #[must_use]
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }
}

/// Log directory for one replica (Java `ReplicaLogDirInfo`).
///
/// Missing current or future dirs are [`None`]. Unknown lag is `-1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLogDirInfo {
    /// Current log directory, when the replica is online there.
    pub current_log_dir: Option<String>,
    /// Offset lag in [`Self::current_log_dir`] (`-1` when unknown).
    pub current_offset_lag: i64,
    /// Future log directory (AlterReplicaLogDirs in progress).
    pub future_log_dir: Option<String>,
    /// Offset lag in [`Self::future_log_dir`] (`-1` when unknown).
    pub future_offset_lag: i64,
}

impl ReplicaLogDirInfo {
    /// Current and future log dirs with their offset lags.
    #[must_use]
    pub fn new(
        current_log_dir: Option<String>,
        current_offset_lag: i64,
        future_log_dir: Option<String>,
        future_offset_lag: i64,
    ) -> Self {
        Self {
            current_log_dir,
            current_offset_lag,
            future_log_dir,
            future_offset_lag,
        }
    }

    /// No dirs known (Java `ReplicaLogDirInfo()`). Lags are `-1`.
    #[must_use]
    pub fn unknown() -> Self {
        Self::new(None, -1, None, -1)
    }

    /// Java `ReplicaLogDirInfo.currentLogDir`.
    #[must_use]
    pub fn current_log_dir(&self) -> Option<&str> {
        self.current_log_dir.as_deref()
    }

    /// Java `ReplicaLogDirInfo.currentOffsetLag`.
    #[must_use]
    pub fn current_offset_lag(&self) -> i64 {
        self.current_offset_lag
    }

    /// Java `ReplicaLogDirInfo.futureLogDir`.
    #[must_use]
    pub fn future_log_dir(&self) -> Option<&str> {
        self.future_log_dir.as_deref()
    }

    /// Java `ReplicaLogDirInfo.futureOffsetLag`.
    #[must_use]
    pub fn future_offset_lag(&self) -> i64 {
        self.future_offset_lag
    }
}

/// Increase a topic's partition count (`CreatePartitions`). Java `NewPartitions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPartitions {
    /// Topic name.
    pub name: String,
    /// Total partition count after the increase (not a delta).
    pub total_count: i32,
    /// Replica assignments for the new partitions (Java
    /// `NewPartitions.increaseTo(int, List<List<Integer>>)`).
    ///
    /// `None` is a null Assignments array: the broker assigns replicas
    /// (Java `increaseTo(int)`).
    pub assignments: Option<Vec<Vec<i32>>>,
}

impl NewPartitions {
    /// Set `name` to `total_count` partitions (Java `NewPartitions.increaseTo`).
    ///
    /// Assignments are null; the broker picks replicas.
    #[must_use]
    pub fn increase_to(name: impl Into<String>, total_count: i32) -> Self {
        Self {
            name: name.into(),
            total_count,
            assignments: None,
        }
    }

    /// Java `NewPartitions.increaseTo(int, List<List<Integer>>)`.
    ///
    /// Each inner list is the replica broker ids for one new partition.
    #[must_use]
    pub fn with_assignments(
        mut self,
        assignments: impl IntoIterator<Item = impl IntoIterator<Item = i32>>,
    ) -> Self {
        self.assignments = Some(
            assignments
                .into_iter()
                .map(|brokers| brokers.into_iter().collect())
                .collect(),
        );
        self
    }

    /// Topic name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `NewPartitions.totalCount`.
    #[must_use]
    pub fn total_count(&self) -> i32 {
        self.total_count
    }

    /// Java `NewPartitions.assignments` (`None` is Java `null`).
    #[must_use]
    pub fn assignments(&self) -> Option<&[Vec<i32>]> {
        self.assignments.as_deref()
    }
}

/// Kafka config resource type (`ConfigResource.Type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum ConfigResourceType {
    /// Topic.
    Topic = RESOURCE_TOPIC,
    /// Broker (`broker.id` as the name).
    Broker = RESOURCE_BROKER,
    /// Broker logger (KIP-1142).
    BrokerLogger = RESOURCE_BROKER_LOGGER,
    /// Client metrics (KIP-714 / KIP-1142).
    ClientMetrics = RESOURCE_CLIENT_METRICS,
    /// Consumer group.
    Group = RESOURCE_GROUP,
}

impl From<ConfigResourceType> for i8 {
    fn from(ty: ConfigResourceType) -> Self {
        ty as i8
    }
}

impl ConfigResourceType {
    /// Wire id to [`Self`]. Unknown ids are [`None`].
    #[must_use]
    pub const fn from_id(id: i8) -> Option<Self> {
        match id {
            RESOURCE_TOPIC => Some(Self::Topic),
            RESOURCE_BROKER => Some(Self::Broker),
            RESOURCE_BROKER_LOGGER => Some(Self::BrokerLogger),
            RESOURCE_CLIENT_METRICS => Some(Self::ClientMetrics),
            RESOURCE_GROUP => Some(Self::Group),
            _ => None,
        }
    }
}

/// Resource for DescribeConfigs / IncrementalAlterConfigs / AlterConfigs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResource {
    /// Kafka resource type (`CONFIG_RESOURCE_TOPIC`, [`ConfigResourceType::Topic`], …).
    pub resource_type: i8,
    /// Resource name (topic name, or broker id as a string).
    pub name: String,
    /// Config keys to fetch; `None` means all.
    pub keys: Option<Vec<String>>,
}

impl ConfigResource {
    /// Resource of this type. Fetches every config key unless [`Self::keys`] is set.
    #[must_use]
    pub fn of(ty: ConfigResourceType, name: impl Into<String>) -> Self {
        Self {
            resource_type: i8::from(ty),
            name: name.into(),
            keys: None,
        }
    }

    /// Topic resource. Fetches every config key unless [`Self::keys`] is set.
    #[must_use]
    pub fn topic(name: impl Into<String>) -> Self {
        Self::of(ConfigResourceType::Topic, name)
    }

    /// Broker resource (`broker.id` as the name).
    #[must_use]
    pub fn broker(id: i32) -> Self {
        Self::of(ConfigResourceType::Broker, id.to_string())
    }

    /// Consumer-group resource.
    #[must_use]
    pub fn group(name: impl Into<String>) -> Self {
        Self::of(ConfigResourceType::Group, name)
    }

    /// Restrict DescribeConfigs to these keys.
    #[must_use]
    pub fn keys(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.keys = Some(keys.into_iter().map(Into::into).collect());
        self
    }

    /// Java `ConfigResource.name`.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `ConfigResource.type`.
    #[must_use]
    pub fn resource_type(&self) -> Option<ConfigResourceType> {
        ConfigResourceType::from_id(self.resource_type)
    }
}

impl ListedConfigResource {
    /// Java `ConfigResource.name` (`listConfigResources` item).
    #[must_use]
    pub fn name(&self) -> &str {
        self.resource_name.as_str()
    }

    /// Java `ConfigResource.type`. Unknown wire ids are [`None`].
    #[must_use]
    pub fn resource_type(&self) -> Option<ConfigResourceType> {
        ConfigResourceType::from_id(self.resource_type)
    }

    /// This listing as a [`ConfigResource`] with no key filter (Java
    /// `listConfigResources` returns `ConfigResource`).
    #[must_use]
    pub fn to_config_resource(&self) -> ConfigResource {
        ConfigResource {
            resource_type: self.resource_type,
            name: self.resource_name.clone(),
            keys: None,
        }
    }
}

impl From<ListedConfigResource> for ConfigResource {
    fn from(listed: ListedConfigResource) -> Self {
        Self {
            resource_type: listed.resource_type,
            name: listed.resource_name,
            keys: None,
        }
    }
}

/// One resource plus ops for [`Admin::incremental_alter_configs_for`]
/// (Java `incrementalAlterConfigs(Map)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResourceUpdate {
    /// Resource to alter.
    pub resource: ConfigResource,
    /// Incremental ops (`AlterConfig::set` / `delete` / `append` / `subtract`).
    pub configs: Vec<AlterConfig>,
}

impl ConfigResourceUpdate {
    /// `resource` with these ops.
    #[must_use]
    pub fn new(resource: ConfigResource, configs: impl IntoIterator<Item = AlterConfig>) -> Self {
        Self {
            resource,
            configs: configs.into_iter().collect(),
        }
    }
}

/// One resource plus replacement entries for [`Admin::alter_configs_for`]
/// (Java `alterConfigs(Map)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigReplacement {
    /// Resource to replace configs on.
    pub resource: ConfigResource,
    /// Replacement name/value pairs (value `None` clears the key).
    pub configs: Vec<(String, Option<String>)>,
}

impl ConfigReplacement {
    /// `resource` with these entries.
    #[must_use]
    pub fn new(
        resource: ConfigResource,
        configs: impl IntoIterator<Item = (String, Option<String>)>,
    ) -> Self {
        Self {
            resource,
            configs: configs.into_iter().collect(),
        }
    }

    /// Java `alterConfigs(Map)` value: this resource plus [`Config::entries`].
    #[must_use]
    pub fn from_config(resource: ConfigResource, config: &Config) -> Self {
        Self {
            resource,
            configs: config
                .entries()
                .iter()
                .map(|e| (e.name.clone(), e.value.clone()))
                .collect(),
        }
    }
}

/// Java `NewPartitionReassignment` for
/// [`Admin::alter_partition_reassignments_for`].
///
/// [`Self::new`] rejects an empty replica list (Java throws
/// `IllegalArgumentException`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPartitionReassignment {
    /// Target replica broker ids (Java `targetReplicas`).
    pub target_replicas: Vec<i32>,
}

impl NewPartitionReassignment {
    /// Java `NewPartitionReassignment(List)`. Empty replicas is an error.
    pub fn new(target_replicas: impl IntoIterator<Item = i32>) -> Result<Self> {
        let target_replicas: Vec<i32> = target_replicas.into_iter().collect();
        if target_replicas.is_empty() {
            return Err(Error::protocol(
                "Cannot create a new partition reassignment without any replicas",
            ));
        }
        Ok(Self { target_replicas })
    }

    /// Java `NewPartitionReassignment.targetReplicas`.
    #[must_use]
    pub fn target_replicas(&self) -> &[i32] {
        &self.target_replicas
    }
}

/// One partition in `Admin::alter_partition_reassignments`.
///
/// `replicas = None` cancels a pending reassignment (KIP-455).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignment {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Target replica broker ids, or `None` to cancel a pending move.
    pub replicas: Option<Vec<i32>>,
}

impl PartitionReassignment {
    /// Move `partition` onto `replicas`.
    #[must_use]
    pub fn assign(
        partition: impl Into<crate::TopicPartition>,
        replicas: impl IntoIterator<Item = i32>,
    ) -> Self {
        let tp = partition.into();
        Self {
            topic: tp.topic,
            partition: tp.partition,
            replicas: Some(replicas.into_iter().collect()),
        }
    }

    /// Cancel a pending reassignment for this partition.
    #[must_use]
    pub fn cancel(partition: impl Into<crate::TopicPartition>) -> Self {
        let tp = partition.into();
        Self {
            topic: tp.topic,
            partition: tp.partition,
            replicas: None,
        }
    }

    /// Java `alterPartitionReassignments(Map)` value: `Some` assigns,
    /// `None` cancels (Java `Optional.empty()`).
    #[must_use]
    pub fn from_new(
        partition: impl Into<crate::TopicPartition>,
        assignment: Option<NewPartitionReassignment>,
    ) -> Self {
        match assignment {
            Some(n) => Self::assign(partition, n.target_replicas),
            None => Self::cancel(partition),
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Partition index.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Target replicas, or `None` to cancel.
    #[must_use]
    pub fn replicas(&self) -> Option<&[i32]> {
        self.replicas.as_deref()
    }
}

/// Flattened per-partition result of AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignmentResult {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Per-partition error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl ReassignmentResult {
    /// Topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Partition index.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Per-partition error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// Flattened ongoing reassignment from ListPartitionReassignments
/// (Java `PartitionReassignment`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingReassignment {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Replica set after the move completes.
    pub replicas: Vec<i32>,
    /// Brokers being added.
    pub adding_replicas: Vec<i32>,
    /// Brokers being removed.
    pub removing_replicas: Vec<i32>,
}

impl OngoingReassignment {
    /// Topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Partition index.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Java `PartitionReassignment.replicas`.
    #[must_use]
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// Java `PartitionReassignment.addingReplicas`.
    #[must_use]
    pub fn adding_replicas(&self) -> &[i32] {
        &self.adding_replicas
    }

    /// Java `PartitionReassignment.removingReplicas`.
    #[must_use]
    pub fn removing_replicas(&self) -> &[i32] {
        &self.removing_replicas
    }
}

/// One finalized-feature update for `Admin::update_features`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdate {
    /// Feature name (for example `metadata.version`).
    pub name: String,
    /// Target finalized version.
    pub max_version_level: i16,
    /// When true, allow a downgrade (v0 `AllowDowngrade`; Java maps this
    /// to safe downgrade).
    pub allow_downgrade: bool,
    /// Upgrade type (v1+). [`UPGRADE_TYPE_UPGRADE`] / safe / unsafe.
    pub upgrade_type: i8,
}

impl FeatureUpdate {
    /// Feature `name` at `max_version_level` (upgrade only).
    #[must_use]
    pub fn new(name: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            name: name.into(),
            max_version_level,
            allow_downgrade: false,
            upgrade_type: UPGRADE_TYPE_UPGRADE,
        }
    }

    /// Allow a downgrade of this feature (Java `FeatureUpdate(short, true)`
    /// → safe downgrade). v0 sends `AllowDowngrade`. v1+ sends
    /// [`UPGRADE_TYPE_SAFE_DOWNGRADE`].
    #[must_use]
    pub fn allow_downgrade(mut self, allow: bool) -> Self {
        self.allow_downgrade = allow;
        self.upgrade_type = if allow {
            UPGRADE_TYPE_SAFE_DOWNGRADE
        } else {
            UPGRADE_TYPE_UPGRADE
        };
        self
    }

    /// Set v1+ [`UpgradeType`] (`1` upgrade, `2` safe, `3` unsafe).
    /// v0 sends `AllowDowngrade` when the type is not upgrade-only.
    #[must_use]
    pub fn upgrade_type(mut self, upgrade_type: impl Into<i8>) -> Self {
        let upgrade_type = upgrade_type.into();
        self.upgrade_type = upgrade_type;
        self.allow_downgrade = upgrade_type != UPGRADE_TYPE_UPGRADE;
        self
    }

    /// Feature name (Java map key for `updateFeatures`).
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `FeatureUpdate.maxVersionLevel`.
    #[must_use]
    pub fn max_version_level(&self) -> i16 {
        self.max_version_level
    }
}

/// Per-feature result of UpdateFeatures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateResult {
    /// Feature name.
    pub name: String,
    /// Per-feature error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl FeatureUpdateResult {
    /// Feature name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Per-feature error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// Supported version range from [`Admin::describe_features`] (Java `SupportedVersionRange`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedVersionRange {
    /// Feature name (for example `metadata.version`).
    pub name: String,
    /// Lowest version the broker supports.
    pub min_version: i16,
    /// Highest version the broker supports.
    pub max_version: i16,
}

impl SupportedVersionRange {
    /// Feature `name` supported from `min_version` through `max_version`.
    #[must_use]
    pub fn new(name: impl Into<String>, min_version: i16, max_version: i16) -> Self {
        Self {
            name: name.into(),
            min_version,
            max_version,
        }
    }

    /// Feature name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `SupportedVersionRange.min`.
    #[must_use]
    pub fn min_version(&self) -> i16 {
        self.min_version
    }

    /// Java `SupportedVersionRange.max`.
    #[must_use]
    pub fn max_version(&self) -> i16 {
        self.max_version
    }
}

/// Finalized version range from [`Admin::describe_features`] (Java `FinalizedVersionRange`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedVersionRange {
    /// Feature name (for example `metadata.version`).
    pub name: String,
    /// Lowest finalized version.
    pub min_version_level: i16,
    /// Highest finalized version.
    pub max_version_level: i16,
}

impl FinalizedVersionRange {
    /// Feature `name` finalized from `min_version_level` through `max_version_level`.
    #[must_use]
    pub fn new(name: impl Into<String>, min_version_level: i16, max_version_level: i16) -> Self {
        Self {
            name: name.into(),
            min_version_level,
            max_version_level,
        }
    }

    /// Feature name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `FinalizedVersionRange.minVersionLevel`.
    #[must_use]
    pub fn min_version_level(&self) -> i16 {
        self.min_version_level
    }

    /// Java `FinalizedVersionRange.maxVersionLevel`.
    #[must_use]
    pub fn max_version_level(&self) -> i16 {
        self.max_version_level
    }
}

/// Cluster feature metadata from [`Admin::describe_features`] (Java `FeatureMetadata`).
///
/// There is no DescribeFeatures api key. Java and this client re-issue
/// ApiVersions v3–v4 and read KIP-482 tagged fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureMetadata {
    /// Features the broker supports (`supportedFeatures`).
    pub supported_features: Vec<SupportedVersionRange>,
    /// Finalized features (`finalizedFeatures`).
    pub finalized_features: Vec<FinalizedVersionRange>,
    /// Monotonic finalized-features epoch. `None` when the broker sends `-1`.
    pub finalized_features_epoch: Option<i64>,
    /// ApiVersions tagged field 3 (`zkMigrationReady`, KIP-866).
    pub zk_migration_ready: bool,
}

impl FeatureMetadata {
    /// Java `FeatureMetadata.supportedFeatures`.
    #[must_use]
    pub fn supported_features(&self) -> &[SupportedVersionRange] {
        &self.supported_features
    }

    /// Java `FeatureMetadata.finalizedFeatures`.
    #[must_use]
    pub fn finalized_features(&self) -> &[FinalizedVersionRange] {
        &self.finalized_features
    }

    /// Java `FeatureMetadata.finalizedFeaturesEpoch`.
    #[must_use]
    pub fn finalized_features_epoch(&self) -> Option<i64> {
        self.finalized_features_epoch
    }

    /// ApiVersions tagged field 3 (`zkMigrationReady`).
    #[must_use]
    pub fn zk_migration_ready(&self) -> bool {
        self.zk_migration_ready
    }
}

/// One SCRAM credential to remove for `Admin::alter_user_scram_credentials`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScramCredentialDeletion {
    /// User name.
    pub name: String,
    /// `SCRAM_SHA_256` / [`crate::ScramMechanism::Sha256`], or SHA-512.
    pub mechanism: i8,
}

impl UserScramCredentialDeletion {
    /// Delete this user's credential for `mechanism`.
    #[must_use]
    pub fn new(name: impl Into<String>, mechanism: impl Into<i8>) -> Self {
        Self {
            name: name.into(),
            mechanism: mechanism.into(),
        }
    }

    /// Java `UserScramCredentialAlteration.user`.
    #[must_use]
    pub fn user(&self) -> &str {
        self.name.as_str()
    }

    /// Java `UserScramCredentialDeletion.mechanism`.
    #[must_use]
    pub fn mechanism(&self) -> ScramMechanism {
        ScramMechanism::from_id(self.mechanism)
    }
}

/// One SCRAM credential to insert or replace.
///
/// Callers supply dummy `salt` / `salted_password` bytes. This crate does
/// not hash a password or keep a credential store. `Debug` redacts those
/// fields.
#[derive(Clone, PartialEq, Eq)]
pub struct UserScramCredentialUpsertion {
    /// User name.
    pub name: String,
    /// `SCRAM_SHA_256` / [`crate::ScramMechanism::Sha256`], or SHA-512.
    pub mechanism: i8,
    /// PBKDF2 iteration count.
    pub iterations: i32,
    /// Salt bytes. Redacted in `Debug`.
    pub salt: Vec<u8>,
    /// Salted password bytes. Redacted in `Debug`.
    pub salted_password: Vec<u8>,
}

impl UserScramCredentialUpsertion {
    /// Insert or replace this user's SCRAM credential.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        mechanism: impl Into<i8>,
        iterations: i32,
        salt: impl Into<Vec<u8>>,
        salted_password: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            name: name.into(),
            mechanism: mechanism.into(),
            iterations,
            salt: salt.into(),
            salted_password: salted_password.into(),
        }
    }

    /// Java `UserScramCredentialAlteration.user`.
    #[must_use]
    pub fn user(&self) -> &str {
        self.name.as_str()
    }

    /// Java `UserScramCredentialUpsertion.credentialInfo`.
    #[must_use]
    pub fn credential_info(&self) -> ScramCredentialInfo {
        ScramCredentialInfo::new(self.mechanism, self.iterations)
    }

    /// Java `UserScramCredentialUpsertion.salt`.
    #[must_use]
    pub fn salt(&self) -> &[u8] {
        &self.salt
    }
}

impl std::fmt::Debug for UserScramCredentialUpsertion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserScramCredentialUpsertion")
            .field("name", &self.name)
            .field("mechanism", &self.mechanism)
            .field("iterations", &self.iterations)
            .field("salt", &"<redacted>")
            .field("salted_password", &"<redacted>")
            .finish()
    }
}

/// Java `UserScramCredentialAlteration` for
/// [`Admin::alter_user_scram_credentials_with`].
///
/// Java `alterUserScramCredentials(List)` is a mixed list of
/// [`UserScramCredentialDeletion`] and [`UserScramCredentialUpsertion`].
/// The wire request still splits Deletions then Upsertions.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum UserScramCredentialAlteration {
    /// Java `UserScramCredentialDeletion`.
    Deletion(UserScramCredentialDeletion),
    /// Java `UserScramCredentialUpsertion`.
    Upsertion(UserScramCredentialUpsertion),
}

impl UserScramCredentialAlteration {
    /// Java `UserScramCredentialAlteration.user()`.
    #[must_use]
    pub fn user(&self) -> &str {
        match self {
            Self::Deletion(d) => d.name.as_str(),
            Self::Upsertion(u) => u.name.as_str(),
        }
    }
}

impl From<UserScramCredentialDeletion> for UserScramCredentialAlteration {
    fn from(deletion: UserScramCredentialDeletion) -> Self {
        Self::Deletion(deletion)
    }
}

impl From<UserScramCredentialUpsertion> for UserScramCredentialAlteration {
    fn from(upsertion: UserScramCredentialUpsertion) -> Self {
        Self::Upsertion(upsertion)
    }
}

/// Per-user result of AlterUserScramCredentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScramCredentialResult {
    /// User name.
    pub user: String,
    /// Per-user error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl UserScramCredentialResult {
    /// User name.
    #[must_use]
    pub fn user(&self) -> &str {
        self.user.as_str()
    }

    /// Per-user error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// PID block from `Admin::allocate_producer_ids` (AllocateProducerIds api 67).
///
/// Fixture broker id/epoch only. This is not a live cluster PID allocator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerIdBlock {
    /// First producer id in the allocated block.
    pub producer_id_start: i64,
    /// Number of ids in the block.
    pub producer_id_len: i32,
}

impl ProducerIdBlock {
    /// First producer id in the allocated block.
    #[must_use]
    pub fn producer_id_start(&self) -> i64 {
        self.producer_id_start
    }

    /// Number of ids in the block.
    #[must_use]
    pub fn producer_id_len(&self) -> i32 {
        self.producer_id_len
    }
}

/// One transactional.id fenced by [`Admin::fence_producers`].
///
/// Java `FenceProducersResult`: [`Self::producer_id`] is `producerId`,
/// [`Self::epoch`] is `epoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencedProducer {
    /// Kafka `transactional.id`.
    pub transactional_id: String,
    /// Producer id after InitProducerId.
    pub producer_id: i64,
    /// Producer epoch after InitProducerId.
    pub epoch: i16,
}

impl FencedProducer {
    /// Kafka `transactional.id`.
    #[must_use]
    pub fn transactional_id(&self) -> &str {
        self.transactional_id.as_str()
    }

    /// Java `FenceProducersResult.producerId`.
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Java `FenceProducersResult.epochId`.
    #[must_use]
    pub fn epoch(&self) -> i16 {
        self.epoch
    }
}

/// Spec for [`Admin::abort_transaction`] (Java `AbortTransactionSpec`).
///
/// Sends WriteTxnMarkers (api 27) with `transactionResult=false` (ABORT)
/// to the Metadata partition leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortTransactionSpec {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Producer id that owns the open transaction.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Transaction coordinator epoch.
    pub coordinator_epoch: i32,
}

impl AbortTransactionSpec {
    /// Abort the open transaction of `producer_id` on `partition`.
    #[must_use]
    pub fn new(
        partition: impl Into<crate::TopicPartition>,
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i32,
    ) -> Self {
        let tp = partition.into();
        Self {
            topic: tp.topic,
            partition: tp.partition,
            producer_id,
            producer_epoch,
            coordinator_epoch,
        }
    }

    /// Java `AbortTransactionSpec.topicPartition`.
    #[must_use]
    pub fn topic_partition(&self) -> crate::TopicPartition {
        crate::TopicPartition::new(self.topic.clone(), self.partition)
    }

    /// Java `AbortTransactionSpec.producerId`.
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Java `AbortTransactionSpec.producerEpoch`.
    #[must_use]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Java `AbortTransactionSpec.coordinatorEpoch`.
    #[must_use]
    pub fn coordinator_epoch(&self) -> i32 {
        self.coordinator_epoch
    }
}

/// Java `KafkaAdminClient.DEFAULT_LEAVE_GROUP_REASON` (KIP-800).
pub const DEFAULT_LEAVE_GROUP_REASON: &str = "member was removed by an admin";

/// LeaveGroup v5 Reason: empty/null → default; otherwise KIP-800 truncate.
fn admin_leave_reason(reason: Option<&str>) -> String {
    match reason {
        None | Some("") => DEFAULT_LEAVE_GROUP_REASON.to_string(),
        Some(r) => crate::group::truncate_group_reason(r),
    }
}

/// One static member for [`Admin::remove_members_from_consumer_group`].
///
/// Java `MemberToRemove`. Identified by Kafka `group.instance.id` (KIP-345).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberToRemove {
    /// Kafka `group.instance.id`.
    pub group_instance_id: String,
}

impl MemberToRemove {
    /// Remove the static member with this `group.instance.id`.
    #[must_use]
    pub fn new(group_instance_id: impl Into<String>) -> Self {
        Self {
            group_instance_id: group_instance_id.into(),
        }
    }

    /// Java `MemberToRemove.groupInstanceId`.
    #[must_use]
    pub fn group_instance_id(&self) -> &str {
        self.group_instance_id.as_str()
    }
}

impl From<&str> for MemberToRemove {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

impl From<String> for MemberToRemove {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

/// One group's topic-partition filter (Java `ListConsumerGroupOffsetsSpec`).
///
/// `partitions: None` is every committed partition (OffsetFetch null
/// Topics). Empty `partitions` is a no-op for that group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListConsumerGroupOffsetsSpec {
    /// Topic-partitions to fetch. `None` is all committed partitions.
    pub partitions: Option<Vec<crate::TopicPartition>>,
}

impl ListConsumerGroupOffsetsSpec {
    /// Every committed partition (Java null `topicPartitions`).
    #[must_use]
    pub fn all() -> Self {
        Self { partitions: None }
    }

    /// Named topic-partitions (Java `ListConsumerGroupOffsetsSpec.topicPartitions`).
    #[must_use]
    pub fn topic_partitions(
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
    ) -> Self {
        Self {
            partitions: Some(partitions.into_iter().map(Into::into).collect()),
        }
    }

    /// Java `ListConsumerGroupOffsetsSpec.topicPartitions` (`None` is all).
    #[must_use]
    pub fn partitions(&self) -> Option<&[crate::TopicPartition]> {
        self.partitions.as_deref()
    }
}

/// Per-member result of [`Admin::remove_members_from_consumer_group`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedMember {
    /// Kafka member id from the LeaveGroup response (empty when unknown).
    pub member_id: String,
    /// Kafka `group.instance.id`, when present.
    pub group_instance_id: Option<String>,
    /// Per-member error code (`0` is success).
    pub error_code: i16,
}

impl RemovedMember {
    /// Kafka member id from the LeaveGroup response.
    #[must_use]
    pub fn member_id(&self) -> &str {
        self.member_id.as_str()
    }

    /// Kafka `group.instance.id`, when present.
    #[must_use]
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Per-member error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }
}

/// Kafka admin client: topics, configs, ACLs, groups, and cluster operations.
pub struct Admin {
    cfg: AdminConfig,
    conn: BrokerConn,
    versions: HashMap<i16, ApiVersion>,
    create_version: i16,
    delete_version: i16,
    describe_version: i16,
    partitions_version: i16,
    alter_version: Option<i16>,
    legacy_alter_version: i16,
    delete_records_version: i16,
    describe_producers_version: Option<i16>,
    describe_cluster_version: Option<i16>,
    create_acls_version: i16,
    describe_acls_version: i16,
    delete_acls_version: i16,
    metadata_version: i16,
    find_coord_version: i16,
    offset_delete_version: Option<i16>,
    reassign_version: Option<i16>,
    list_reassign_version: Option<i16>,
    update_features_version: Option<i16>,
    alter_user_scram_version: Option<i16>,
    describe_user_scram_version: Option<i16>,
    unregister_broker_version: Option<i16>,
    describe_client_quotas_version: Option<i16>,
    alter_client_quotas_version: Option<i16>,
    allocate_producer_ids_version: Option<i16>,
    describe_transactions_version: Option<i16>,
    list_transactions_version: Option<i16>,
    consumer_group_describe_version: Option<i16>,
    describe_groups_version: i16,
    list_groups_version: i16,
    delete_groups_version: i16,
    share_group_describe_version: Option<i16>,
    describe_share_group_offsets_version: Option<i16>,
    alter_share_group_offsets_version: Option<i16>,
    delete_share_group_offsets_version: Option<i16>,
    describe_topic_partitions_version: Option<i16>,
    list_config_resources_version: Option<i16>,
    get_telemetry_subscriptions_version: Option<i16>,
    /// Cached KIP-714 client instance UUID (`None` until first fetch).
    cached_client_instance_id: Option<[u8; 16]>,
    push_telemetry_version: Option<i16>,
    assign_replicas_to_dirs_version: Option<i16>,
    alter_replica_log_dirs_version: Option<i16>,
    describe_log_dirs_version: Option<i16>,
    create_delegation_token_version: Option<i16>,
    renew_delegation_token_version: Option<i16>,
    expire_delegation_token_version: Option<i16>,
    describe_delegation_token_version: Option<i16>,
    cluster: Cluster,
    conns: HashMap<i32, BrokerConn>,
    reconnect_fails: HashMap<i32, u32>,
    group_coord: Option<(String, i32)>,
    group_coords: HashMap<String, i32>,
    txn_coord: Option<(String, i32)>,
    txn_coords: HashMap<String, i32>,
    stats: Arc<crate::metrics::AdminTracker>,
}

pub(crate) async fn fetch_client_instance_id(
    conn: &mut BrokerConn,
    version: i16,
    timeout: Duration,
    known: [u8; 16],
) -> Result<[u8; 16]> {
    let body = conn
        .roundtrip(
            GET_TELEMETRY_SUBSCRIPTIONS,
            version,
            |buf| encode_get_telemetry_subscriptions_request(buf, &known),
            timeout,
        )
        .await?;
    let resp = decode_get_telemetry_subscriptions_response(&mut body.clone())?;
    if resp.error_code != 0 {
        return Err(Error::broker(resp.error_code, "GetTelemetrySubscriptions"));
    }
    Ok(resp.client_instance_id)
}

fn offset_fetch_topics_for_spec(
    spec: &ListConsumerGroupOffsetsSpec,
) -> Option<Vec<crate::protocol::group::OffsetFetchTopic>> {
    spec.partitions.as_ref().map(|ps| {
        let wanted: Vec<(String, i32)> = ps
            .iter()
            .map(|tp| (tp.topic.clone(), tp.partition))
            .collect();
        crate::group::group_offset_fetch_topics(&wanted)
    })
}

fn listed_group_offsets(
    spec: &ListConsumerGroupOffsetsSpec,
    fetched: &[crate::protocol::group::FetchedOffsetTopic],
) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>> {
    let map = crate::group::committed_offset_map(fetched)?;
    match &spec.partitions {
        None => Ok(map
            .into_iter()
            .map(|((topic, partition), md)| (crate::TopicPartition::new(topic, partition), md))
            .collect()),
        Some(ps) => Ok(ps
            .iter()
            .map(|tp| {
                let md = map
                    .get(&(tp.topic.clone(), tp.partition))
                    .cloned()
                    .unwrap_or_else(|| crate::OffsetAndMetadata::new(-1));
                (tp.clone(), md)
            })
            .collect()),
    }
}

fn delete_records_topics(
    records: &[(crate::TopicPartition, i64)],
    idxs: &[usize],
) -> Vec<DeleteRecordsTopic> {
    let mut by_topic: HashMap<String, Vec<DeleteRecordsPartition>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for &i in idxs {
        let Some((tp, offset)) = records.get(i) else {
            continue;
        };
        match by_topic.entry(tp.topic.clone()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                order.push(tp.topic.clone());
                let _ = slot.insert(vec![DeleteRecordsPartition {
                    partition: tp.partition,
                    offset: *offset,
                }]);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                slot.get_mut().push(DeleteRecordsPartition {
                    partition: tp.partition,
                    offset: *offset,
                });
            }
        }
    }
    order
        .into_iter()
        .filter_map(|topic| {
            by_topic
                .remove(&topic)
                .map(|partitions| DeleteRecordsTopic { topic, partitions })
        })
        .collect()
}

fn describe_producers_topics(
    partitions: &[crate::TopicPartition],
    idxs: &[usize],
) -> Vec<DescribeProducersTopicRequest> {
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for &i in idxs {
        let Some(tp) = partitions.get(i) else {
            continue;
        };
        match by_topic.entry(tp.topic.clone()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                order.push(tp.topic.clone());
                let _ = slot.insert(vec![tp.partition]);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                slot.get_mut().push(tp.partition);
            }
        }
    }
    order
        .into_iter()
        .filter_map(|name| {
            by_topic
                .remove(&name)
                .map(|partition_indexes| DescribeProducersTopicRequest {
                    name,
                    partition_indexes,
                })
        })
        .collect()
}

type DescribeProducersNodeOutcome = (Vec<(usize, DescribeProducersPartition)>, Vec<usize>);

fn creatable_from_new(topics: &[NewTopic]) -> Vec<CreatableTopic> {
    topics
        .iter()
        .map(|t| CreatableTopic {
            name: t.name.clone(),
            num_partitions: t.num_partitions,
            replication_factor: t.replication_factor,
            assignments: t
                .assignments
                .iter()
                .map(|(partition_index, broker_ids)| ReplicaAssignment {
                    partition_index: *partition_index,
                    broker_ids: broker_ids.clone(),
                })
                .collect(),
            configs: t
                .configs
                .iter()
                .map(|(n, v)| TopicConfig {
                    name: n.clone(),
                    value: v.clone(),
                })
                .collect(),
        })
        .collect()
}

fn delete_state_matches(topic: &DeleteTopicState, result: &TopicResult) -> bool {
    match &topic.name {
        Some(name) => name == &result.name,
        None => topic.topic_id != [0; 16] && topic.topic_id == result.topic_id,
    }
}

fn partitions_from_new(topics: &[NewPartitions]) -> Vec<CreatePartitionsTopic> {
    topics
        .iter()
        .map(|t| CreatePartitionsTopic {
            name: t.name.clone(),
            count: t.total_count,
            assignments: t.assignments.clone(),
        })
        .collect()
}

impl Admin {
    /// Connect with default config to one bootstrap server.
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(AdminConfig::bootstrap([bootstrap.into()])).await
    }

    /// Connect using `cfg`. Negotiates ApiVersions and optional SASL/TLS.
    ///
    /// DescribeTopicPartitions, ConsumerGroupDescribe, ShareGroupDescribe,
    /// the share-offset RPCs, AllocateProducerIds, ListConfigResources,
    /// GetTelemetrySubscriptions, PushTelemetry, AssignReplicasToDirs,
    /// UnregisterBroker, DescribeProducers, DescribeCluster,
    /// UpdateFeatures, DescribeClientQuotas, AlterClientQuotas,
    /// AlterUserScramCredentials, DescribeUserScramCredentials,
    /// AlterReplicaLogDirs, DescribeLogDirs, the delegation-token APIs,
    /// DescribeTransactions, ListTransactions, AlterPartitionReassignments,
    /// ListPartitionReassignments, OffsetDelete, and IncrementalAlterConfigs
    /// are optional at connect. Missing APIs fail on the method with
    /// [`Error::Unsupported`].
    pub async fn new(cfg: AdminConfig) -> Result<Self> {
        if cfg.bootstrap.is_empty() {
            return Err(Error::protocol("no bootstrap servers"));
        }
        let stats = Arc::new(crate::metrics::AdminTracker::default());
        let mut conn = BrokerConn::connect_tls_any(
            &cfg.bootstrap,
            &cfg.client_id,
            cfg.connect_timeout,
            cfg.tls.as_ref(),
        )
        .await?;
        conn.set_stats(Arc::clone(&stats));
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
        let create_version = versions
            .get(&CREATE_TOPICS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 7))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support CreateTopics v0-7".into())
            })?;
        let delete_version = versions
            .get(&DELETE_TOPICS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 6))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DeleteTopics v0-6".into())
            })?;
        let describe_version = versions
            .get(&DESCRIBE_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 4))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeConfigs v0-4".into())
            })?;
        let partitions_version = versions
            .get(&CREATE_PARTITIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 3))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support CreatePartitions v0-3".into())
            })?;
        let alter_version = versions
            .get(&INCREMENTAL_ALTER_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1));
        let legacy_alter_version = versions
            .get(&ALTER_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support AlterConfigs v0-2".into())
            })?;
        let delete_records_version = versions
            .get(&DELETE_RECORDS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DeleteRecords v0-2".into())
            })?;
        let describe_producers_version = versions
            .get(&DESCRIBE_PRODUCERS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let describe_cluster_version = versions
            .get(&DESCRIBE_CLUSTER)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 2));
        let create_acls_version = versions
            .get(&CREATE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 3))
            .ok_or_else(|| Error::Unsupported("broker does not support CreateAcls v0-3".into()))?;
        let describe_acls_version = versions
            .get(&DESCRIBE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 3))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeAcls v0-3".into())
            })?;
        let delete_acls_version = versions
            .get(&DELETE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 3))
            .ok_or_else(|| Error::Unsupported("broker does not support DeleteAcls v0-3".into()))?;
        let metadata_version = versions
            .get(&METADATA)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 13))
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        let find_coord_version = versions
            .get(&FIND_COORDINATOR)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 6))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support FindCoordinator v1-6".into())
            })?;
        let offset_delete_version = versions
            .get(&OFFSET_DELETE)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let reassign_version = versions
            .get(&ALTER_PARTITION_REASSIGNMENTS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let list_reassign_version = versions
            .get(&LIST_PARTITION_REASSIGNMENTS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let update_features_version = versions
            .get(&UPDATE_FEATURES)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 2));
        let alter_user_scram_version = versions
            .get(&ALTER_USER_SCRAM_CREDENTIALS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let describe_user_scram_version = versions
            .get(&DESCRIBE_USER_SCRAM_CREDENTIALS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let unregister_broker_version = versions
            .get(&UNREGISTER_BROKER)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let describe_client_quotas_version = versions
            .get(&DESCRIBE_CLIENT_QUOTAS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1));
        let alter_client_quotas_version = versions
            .get(&ALTER_CLIENT_QUOTAS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1));
        let allocate_producer_ids_version = versions
            .get(&ALLOCATE_PRODUCER_IDS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let describe_transactions_version = versions
            .get(&DESCRIBE_TRANSACTIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let list_transactions_version = versions
            .get(&LIST_TRANSACTIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1));
        let consumer_group_describe_version = versions
            .get(&CONSUMER_GROUP_DESCRIBE)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1));
        let describe_groups_version = versions
            .get(&DESCRIBE_GROUPS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 6))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeGroups v0-6".into())
            })?;
        let list_groups_version = versions
            .get(&LIST_GROUPS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 5))
            .ok_or_else(|| Error::Unsupported("broker does not support ListGroups v0-5".into()))?;
        let delete_groups_version = versions
            .get(&DELETE_GROUPS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DeleteGroups v0-2".into())
            })?;
        let share_group_describe_version = versions
            .get(&SHARE_GROUP_DESCRIBE)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1));
        let describe_share_group_offsets_version = versions
            .get(&DESCRIBE_SHARE_GROUP_OFFSETS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let alter_share_group_offsets_version = versions
            .get(&ALTER_SHARE_GROUP_OFFSETS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let delete_share_group_offsets_version = versions
            .get(&DELETE_SHARE_GROUP_OFFSETS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let describe_topic_partitions_version = versions
            .get(&DESCRIBE_TOPIC_PARTITIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let list_config_resources_version = versions
            .get(&LIST_CONFIG_RESOURCES)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1));
        let get_telemetry_subscriptions_version = versions
            .get(&GET_TELEMETRY_SUBSCRIPTIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let push_telemetry_version = versions
            .get(&PUSH_TELEMETRY)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let assign_replicas_to_dirs_version = versions
            .get(&ASSIGN_REPLICAS_TO_DIRS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0));
        let alter_replica_log_dirs_version = versions
            .get(&ALTER_REPLICA_LOG_DIRS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 2));
        let describe_log_dirs_version = versions
            .get(&DESCRIBE_LOG_DIRS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 4));
        let create_delegation_token_version = versions
            .get(&CREATE_DELEGATION_TOKEN)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 3));
        let renew_delegation_token_version = versions
            .get(&RENEW_DELEGATION_TOKEN)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 2));
        let expire_delegation_token_version = versions
            .get(&EXPIRE_DELEGATION_TOKEN)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 2));
        let describe_delegation_token_version = versions
            .get(&DESCRIBE_DELEGATION_TOKEN)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 3));
        Ok(Self {
            cfg,
            conn,
            versions,
            create_version,
            delete_version,
            describe_version,
            partitions_version,
            alter_version,
            legacy_alter_version,
            delete_records_version,
            describe_producers_version,
            describe_cluster_version,
            create_acls_version,
            describe_acls_version,
            delete_acls_version,
            metadata_version,
            find_coord_version,
            offset_delete_version,
            reassign_version,
            list_reassign_version,
            update_features_version,
            alter_user_scram_version,
            describe_user_scram_version,
            unregister_broker_version,
            describe_client_quotas_version,
            alter_client_quotas_version,
            allocate_producer_ids_version,
            describe_transactions_version,
            list_transactions_version,
            consumer_group_describe_version,
            describe_groups_version,
            list_groups_version,
            delete_groups_version,
            share_group_describe_version,
            describe_share_group_offsets_version,
            alter_share_group_offsets_version,
            delete_share_group_offsets_version,
            describe_topic_partitions_version,
            list_config_resources_version,
            get_telemetry_subscriptions_version,
            cached_client_instance_id: None,
            push_telemetry_version,
            assign_replicas_to_dirs_version,
            alter_replica_log_dirs_version,
            describe_log_dirs_version,
            create_delegation_token_version,
            renew_delegation_token_version,
            expire_delegation_token_version,
            describe_delegation_token_version,
            cluster: Cluster::default(),
            conns: HashMap::new(),
            reconnect_fails: HashMap::new(),
            group_coord: None,
            group_coords: HashMap::new(),
            txn_coord: None,
            txn_coords: HashMap::new(),
            stats,
        })
    }

    /// Negotiated ApiVersions for this connection.
    #[must_use]
    pub fn versions(&self) -> &HashMap<i16, ApiVersion> {
        &self.versions
    }

    /// Admin RPC counters and round-trip latency since connect.
    ///
    /// Java `Admin.metrics()` is Kafka's live metric map. This snapshot
    /// counts every Admin [`BrokerConn::roundtrip`]. `errors` is I/O,
    /// timeout, and protocol failure — not a decoded broker `error_code`
    /// on a valid body. `connections` is the bootstrap socket plus
    /// per-node sockets.
    #[must_use]
    pub fn metrics(&self) -> crate::AdminMetrics {
        self.stats.snapshot(1 + self.conns.len() as u64)
    }

    /// Drop the admin connection.
    pub async fn close(self) -> Result<()> {
        Ok(())
    }

    /// [`Self::close`] with a timeout (Java `close(Duration)`).
    ///
    /// Admin has no LeaveGroup; the duration is unused, same as
    /// [`crate::Consumer::close_timeout`].
    pub async fn close_timeout(self, _timeout: Duration) -> Result<()> {
        self.close().await
    }

    /// Java `clientInstanceId` (KIP-714 GetTelemetrySubscriptions).
    ///
    /// Returns [`Uuid`] (Java `Uuid`). The first call sends a zero UUID;
    /// the broker assigns one. Later calls return the cached id without
    /// another round-trip. Waits up to [`AdminConfig::request_timeout`].
    /// For a one-shot timeout, use [`Self::client_instance_id_timeout`].
    pub async fn client_instance_id(&mut self) -> Result<Uuid> {
        let timeout = self.cfg.request_timeout;
        self.client_instance_id_timeout(timeout).await
    }

    /// [`Self::client_instance_id`] with a one-shot timeout (Java
    /// `clientInstanceId(Duration)`).
    ///
    /// `timeout` is the GetTelemetrySubscriptions RPC deadline. Cached after
    /// the first successful call; later calls ignore `timeout`.
    pub async fn client_instance_id_timeout(&mut self, timeout: Duration) -> Result<Uuid> {
        if let Some(id) = self.cached_client_instance_id {
            return Ok(Uuid::from_bytes(id));
        }
        self.ensure_bootstrap().await?;
        let version = self.get_telemetry_subscriptions_version.ok_or_else(|| {
            Error::Unsupported("broker does not support GetTelemetrySubscriptions".into())
        })?;
        let id = fetch_client_instance_id(&mut self.conn, version, timeout, [0; 16]).await?;
        self.cached_client_instance_id = Some(id);
        Ok(Uuid::from_bytes(id))
    }

    /// Create topics (`CreateTopics`).
    ///
    /// Negotiates v0–v7 (v5+ flexible; v5 returns NumPartitions /
    /// ReplicationFactor / Configs, KIP-525; v7 TopicId, KIP-516).
    /// Kafka 4.0 `validVersions` is `2-7`. v8+ is not spoken.
    /// `timeout_ms` is CreateTopics TimeoutMs. The RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot timeout that
    /// drives both the RPC deadline and TimeoutMs, use
    /// [`Self::create_topics_timeout`].
    /// Java `CreateTopicsOptions.retryOnQuotaViolation` defaults to
    /// `true` (KIP-599); use [`Self::create_topics_with_quota_retry`]
    /// to disable.
    pub async fn create_topics(
        &mut self,
        topics: &[NewTopic],
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout = self.cfg.request_timeout;
        self.create_topics_with(topics, timeout_ms, validate_only, timeout, true)
            .await
    }

    /// [`Self::create_topics`] with a one-shot timeout (Java
    /// `CreateTopicsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and CreateTopics TimeoutMs.
    pub async fn create_topics_timeout(
        &mut self,
        topics: &[NewTopic],
        timeout: Duration,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.create_topics_with(topics, timeout_ms, validate_only, timeout, true)
            .await
    }

    /// [`Self::create_topics`] with Java `CreateTopicsOptions.retryOnQuotaViolation`.
    ///
    /// [`Self::create_topics`] defaults this to `true` (KIP-599). When
    /// true, topics that return `THROTTLING_QUOTA_EXCEEDED` (89) are
    /// retried alone until the RPC deadline.
    pub async fn create_topics_with_quota_retry(
        &mut self,
        topics: &[NewTopic],
        timeout_ms: i32,
        validate_only: bool,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout = self.cfg.request_timeout;
        self.create_topics_with(
            topics,
            timeout_ms,
            validate_only,
            timeout,
            retry_on_quota_violation,
        )
        .await
    }

    /// [`Self::create_topics_with_quota_retry`] with a one-shot timeout
    /// (Java `CreateTopicsOptions.timeoutMs` + `retryOnQuotaViolation`).
    ///
    /// `timeout` is the RPC deadline and CreateTopics TimeoutMs.
    pub async fn create_topics_timeout_with_quota_retry(
        &mut self,
        topics: &[NewTopic],
        timeout: Duration,
        validate_only: bool,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.create_topics_with(
            topics,
            timeout_ms,
            validate_only,
            timeout,
            retry_on_quota_violation,
        )
        .await
    }

    async fn create_topics_with(
        &mut self,
        topics: &[NewTopic],
        timeout_ms: i32,
        validate_only: bool,
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let mut pending: Vec<NewTopic> = topics.to_vec();
        let mut finished: HashMap<String, TopicResult> = HashMap::new();
        let version = self.create_version;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let req = CreateTopicsRequest {
                topics: creatable_from_new(&pending),
                timeout_ms,
                validate_only,
            };
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing create_topics conn"))?;
                conn.roundtrip(
                    CREATE_TOPICS,
                    version,
                    |buf| encode_create_topics_request(buf, version, &req),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results = decode_create_topics_response(&mut body.clone(), version)?;
            if results
                .iter()
                .any(|r| r.error_code == error::NOT_CONTROLLER)
            {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            let mut next_pending = Vec::new();
            for r in results {
                if retry_on_quota_violation && r.error_code == error::THROTTLING_QUOTA_EXCEEDED {
                    if let Some(t) = pending.iter().find(|t| t.name == r.name) {
                        next_pending.push(t.clone());
                    }
                } else {
                    let name = r.name.clone();
                    let _prev = finished.insert(name, r);
                }
            }
            if next_pending.is_empty() {
                let mut out = Vec::with_capacity(topics.len());
                for t in topics {
                    let r = finished.remove(&t.name).ok_or_else(|| {
                        Error::protocol(format!("missing create_topics result for {}", t.name))
                    })?;
                    out.push(r);
                }
                return Ok(out);
            }
            pending = next_pending;
            self.wait_retry(&mut attempt, deadline).await?;
        }
    }

    /// Delete topics (`DeleteTopics`).
    ///
    /// Negotiates v0–v6 (v4+ flexible; v5 ErrorMessage, KIP-599; v6
    /// Topics of Name + TopicId, KIP-516). Name-based deletes send a
    /// zero UUID at v6 (Java `deleteTopics(Collection<String>)`).
    /// For TopicId deletes (Java `TopicCollection.ofTopicIds`), use
    /// [`Self::delete_topics_by_id`].
    /// Kafka 4.0 `validVersions` is `1-6`. v7+ is not spoken.
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    /// `timeout_ms` is DeleteTopics TimeoutMs. The RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot timeout that
    /// drives both the RPC deadline and TimeoutMs, use
    /// [`Self::delete_topics_timeout`].
    /// Java `DeleteTopicsOptions.retryOnQuotaViolation` defaults to
    /// `true` (KIP-599); use [`Self::delete_topics_with_quota_retry`]
    /// to disable.
    pub async fn delete_topics(
        &mut self,
        names: &[impl AsRef<str>],
        timeout_ms: i32,
    ) -> Result<Vec<TopicResult>> {
        let names: Vec<String> = names.iter().map(|n| n.as_ref().to_string()).collect();
        let timeout = self.cfg.request_timeout;
        self.delete_topics_with(names, timeout_ms, timeout, true)
            .await
    }

    /// [`Self::delete_topics`] with a one-shot timeout (Java
    /// `DeleteTopicsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and DeleteTopics TimeoutMs.
    pub async fn delete_topics_timeout(
        &mut self,
        names: &[impl AsRef<str>],
        timeout: Duration,
    ) -> Result<Vec<TopicResult>> {
        let names: Vec<String> = names.iter().map(|n| n.as_ref().to_string()).collect();
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.delete_topics_with(names, timeout_ms, timeout, true)
            .await
    }

    /// [`Self::delete_topics`] with Java `DeleteTopicsOptions.retryOnQuotaViolation`.
    ///
    /// [`Self::delete_topics`] defaults this to `true` (KIP-599). When
    /// true, topics that return `THROTTLING_QUOTA_EXCEEDED` (89) are
    /// retried alone until the RPC deadline.
    pub async fn delete_topics_with_quota_retry(
        &mut self,
        names: &[impl AsRef<str>],
        timeout_ms: i32,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let names: Vec<String> = names.iter().map(|n| n.as_ref().to_string()).collect();
        let timeout = self.cfg.request_timeout;
        self.delete_topics_with(names, timeout_ms, timeout, retry_on_quota_violation)
            .await
    }

    /// [`Self::delete_topics_with_quota_retry`] with a one-shot timeout
    /// (Java `DeleteTopicsOptions.timeoutMs` + `retryOnQuotaViolation`).
    ///
    /// `timeout` is the RPC deadline and DeleteTopics TimeoutMs.
    pub async fn delete_topics_timeout_with_quota_retry(
        &mut self,
        names: &[impl AsRef<str>],
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let names: Vec<String> = names.iter().map(|n| n.as_ref().to_string()).collect();
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.delete_topics_with(names, timeout_ms, timeout, retry_on_quota_violation)
            .await
    }

    /// Delete topics by TopicId (Java `deleteTopics(TopicCollection.ofTopicIds)`).
    ///
    /// DeleteTopics v6 sends Topics of null Name + TopicId. Brokers that
    /// only speak v0–v5 return [`Error::Unsupported`]. Empty `ids` is a
    /// no-op. Unknown ids return `UNKNOWN_TOPIC_ID` (100) per topic.
    /// `timeout_ms` is DeleteTopics TimeoutMs. The RPC deadline is
    /// [`AdminConfig::request_timeout`]. See
    /// [`Self::delete_topics_by_id_timeout`].
    /// Java `DeleteTopicsOptions.retryOnQuotaViolation` defaults to
    /// `true` (KIP-599); use [`Self::delete_topics_by_id_with_quota_retry`]
    /// to disable.
    pub async fn delete_topics_by_id(
        &mut self,
        ids: &[[u8; 16]],
        timeout_ms: i32,
    ) -> Result<Vec<TopicResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_topics_by_id_with(ids, timeout_ms, timeout, true)
            .await
    }

    /// [`Self::delete_topics_by_id`] with a one-shot timeout (Java
    /// `DeleteTopicsOptions.timeoutMs`).
    pub async fn delete_topics_by_id_timeout(
        &mut self,
        ids: &[[u8; 16]],
        timeout: Duration,
    ) -> Result<Vec<TopicResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.delete_topics_by_id_with(ids, timeout_ms, timeout, true)
            .await
    }

    /// [`Self::delete_topics_by_id`] with Java
    /// `DeleteTopicsOptions.retryOnQuotaViolation`.
    ///
    /// [`Self::delete_topics_by_id`] defaults this to `true` (KIP-599).
    /// When true, topics that return `THROTTLING_QUOTA_EXCEEDED` (89)
    /// are retried alone until the RPC deadline.
    pub async fn delete_topics_by_id_with_quota_retry(
        &mut self,
        ids: &[[u8; 16]],
        timeout_ms: i32,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_topics_by_id_with(ids, timeout_ms, timeout, retry_on_quota_violation)
            .await
    }

    /// [`Self::delete_topics_by_id_with_quota_retry`] with a one-shot
    /// timeout (Java `DeleteTopicsOptions.timeoutMs` +
    /// `retryOnQuotaViolation`).
    ///
    /// `timeout` is the RPC deadline and DeleteTopics TimeoutMs.
    pub async fn delete_topics_by_id_timeout_with_quota_retry(
        &mut self,
        ids: &[[u8; 16]],
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.delete_topics_by_id_with(ids, timeout_ms, timeout, retry_on_quota_violation)
            .await
    }

    /// Java `deleteTopics(TopicCollection)`.
    ///
    /// [`TopicCollection::Names`] is [`Self::delete_topics`].
    /// [`TopicCollection::Ids`] is [`Self::delete_topics_by_id`].
    /// Empty collections are a no-op. `timeout_ms` is DeleteTopics
    /// TimeoutMs. The RPC deadline is [`AdminConfig::request_timeout`].
    /// Java `DeleteTopicsOptions.retryOnQuotaViolation` defaults to
    /// `true` (KIP-599); use [`Self::delete_topics_for_with_quota_retry`]
    /// to disable.
    pub async fn delete_topics_for(
        &mut self,
        topics: &TopicCollection,
        timeout_ms: i32,
    ) -> Result<Vec<TopicResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_topics_for_inner(topics, timeout_ms, timeout, true)
            .await
    }

    /// [`Self::delete_topics_for`] with a one-shot timeout (Java
    /// `DeleteTopicsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and DeleteTopics TimeoutMs.
    pub async fn delete_topics_for_timeout(
        &mut self,
        topics: &TopicCollection,
        timeout: Duration,
    ) -> Result<Vec<TopicResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.delete_topics_for_inner(topics, timeout_ms, timeout, true)
            .await
    }

    /// [`Self::delete_topics_for`] with Java
    /// `DeleteTopicsOptions.retryOnQuotaViolation`.
    pub async fn delete_topics_for_with_quota_retry(
        &mut self,
        topics: &TopicCollection,
        timeout_ms: i32,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_topics_for_inner(topics, timeout_ms, timeout, retry_on_quota_violation)
            .await
    }

    /// [`Self::delete_topics_for_with_quota_retry`] with a one-shot
    /// timeout (Java `DeleteTopicsOptions.timeoutMs` +
    /// `retryOnQuotaViolation`).
    pub async fn delete_topics_for_timeout_with_quota_retry(
        &mut self,
        topics: &TopicCollection,
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.delete_topics_for_inner(topics, timeout_ms, timeout, retry_on_quota_violation)
            .await
    }

    async fn delete_topics_for_inner(
        &mut self,
        topics: &TopicCollection,
        timeout_ms: i32,
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        match topics {
            TopicCollection::Names(names) => {
                self.delete_topics_with(
                    names.clone(),
                    timeout_ms,
                    timeout,
                    retry_on_quota_violation,
                )
                .await
            }
            TopicCollection::Ids(ids) => {
                let ids: Vec<[u8; 16]> = ids.iter().copied().map(Uuid::to_bytes).collect();
                self.delete_topics_by_id_with(&ids, timeout_ms, timeout, retry_on_quota_violation)
                    .await
            }
        }
    }

    async fn delete_topics_by_id_with(
        &mut self,
        ids: &[[u8; 16]],
        timeout_ms: i32,
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        if self.delete_version < 6 {
            return Err(Error::Unsupported(
                "broker does not support DeleteTopics v6 topic IDs".into(),
            ));
        }
        let topics: Vec<DeleteTopicState> =
            ids.iter().copied().map(DeleteTopicState::by_id).collect();
        self.delete_topics_states_with(topics, timeout_ms, timeout, retry_on_quota_violation)
            .await
    }

    async fn delete_topics_with(
        &mut self,
        names: Vec<String>,
        timeout_ms: i32,
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let topics: Vec<DeleteTopicState> =
            names.into_iter().map(DeleteTopicState::by_name).collect();
        self.delete_topics_states_with(topics, timeout_ms, timeout, retry_on_quota_violation)
            .await
    }

    async fn delete_topics_states_with(
        &mut self,
        topics: Vec<DeleteTopicState>,
        timeout_ms: i32,
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let original = topics;
        let mut pending = original.clone();
        let mut finished_names: HashMap<String, TopicResult> = HashMap::new();
        let mut finished_ids: HashMap<[u8; 16], TopicResult> = HashMap::new();
        let version = self.delete_version;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing delete_topics conn"))?;
                conn.roundtrip(
                    DELETE_TOPICS,
                    version,
                    |buf| encode_delete_topics_states_request(buf, version, &pending, timeout_ms),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results = decode_delete_topics_response(&mut body.clone(), version)?;
            if results
                .iter()
                .any(|r| r.error_code == error::NOT_CONTROLLER)
            {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            let mut next_pending = Vec::new();
            for r in results {
                let matched = pending.iter().find(|t| delete_state_matches(t, &r));
                if retry_on_quota_violation && r.error_code == error::THROTTLING_QUOTA_EXCEEDED {
                    if let Some(t) = matched {
                        next_pending.push(t.clone());
                    }
                } else if let Some(t) = matched {
                    if let Some(n) = &t.name {
                        let _prev = finished_names.insert(n.clone(), r);
                    } else {
                        let _prev = finished_ids.insert(t.topic_id, r);
                    }
                }
            }
            if next_pending.is_empty() {
                let mut out = Vec::with_capacity(original.len());
                for t in &original {
                    let r = if let Some(n) = &t.name {
                        finished_names.remove(n)
                    } else {
                        finished_ids.remove(&t.topic_id)
                    }
                    .ok_or_else(|| {
                        Error::protocol(format!(
                            "missing delete_topics result for {}",
                            t.name.as_deref().unwrap_or("topic id")
                        ))
                    })?;
                    out.push(r);
                }
                return Ok(out);
            }
            pending = next_pending;
            self.wait_retry(&mut attempt, deadline).await?;
        }
    }

    /// Cluster topics (Java `Admin.listTopics`).
    ///
    /// Sends Metadata with a null topic array (all topics) on the
    /// bootstrap connection. Includes internal topics;
    /// [`TopicListing::is_internal`] is Metadata `IsInternal`.
    /// Metadata has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For Java
    /// `ListTopicsOptions.listInternal`, use [`Self::list_topics_with`].
    /// For a one-shot RPC deadline, use [`Self::list_topics_timeout`].
    pub async fn list_topics(&mut self) -> Result<Vec<TopicListing>> {
        let timeout = self.cfg.request_timeout;
        self.list_topics_with_timeout(true, timeout).await
    }

    /// [`Self::list_topics`] filtered by Java `ListTopicsOptions.listInternal`.
    ///
    /// Metadata still returns every topic; this crate drops rows with
    /// `IsInternal` when `list_internal` is false. [`Self::list_topics`]
    /// keeps internals (Java's default `listInternal` is false).
    pub async fn list_topics_with(&mut self, list_internal: bool) -> Result<Vec<TopicListing>> {
        let timeout = self.cfg.request_timeout;
        self.list_topics_with_timeout(list_internal, timeout).await
    }

    /// [`Self::list_topics`] with a one-shot RPC deadline (Java
    /// `ListTopicsOptions.timeoutMs`).
    ///
    /// Metadata has no TimeoutMs. Includes internal topics, matching
    /// [`Self::list_topics`].
    pub async fn list_topics_timeout(&mut self, timeout: Duration) -> Result<Vec<TopicListing>> {
        self.list_topics_with_timeout(true, timeout).await
    }

    /// [`Self::list_topics`] with Java `ListTopicsOptions.listInternal` and
    /// `timeoutMs`.
    ///
    /// Metadata has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn list_topics_with_timeout(
        &mut self,
        list_internal: bool,
        timeout: Duration,
    ) -> Result<Vec<TopicListing>> {
        let md = self
            .fetch_metadata_request_with(None, false, timeout)
            .await?;
        Ok(topic_listings_from(&md, list_internal))
    }

    /// Topic partition layouts (Java `Admin.describeTopics`).
    ///
    /// Sends DescribeTopicPartitions (api 75, KIP-966) when the broker
    /// advertises it (Java `TopicCollection.ofTopicNames`). Otherwise
    /// Metadata for the named topics (Java fallback before KIP-966).
    /// `ResponsePartitionLimit` is 2000; a `NextCursor` is followed until
    /// the broker is done. Empty input is a no-op (no RPC). Per-topic
    /// errors live on [`TopicDescription::error_code`];
    /// [`TopicDescription::partitions`] is filled only when that code is `0`.
    /// DescribeTopicPartitions has no IncludeTopicAuthorizedOperations
    /// request flag; [`TopicDescription::authorized_operations`] is
    /// [`AUTHORIZED_OPERATIONS_OMITTED`] unless
    /// [`Self::describe_topics_with`] is used. For TopicId describes
    /// (Java `TopicCollection.ofTopicIds`), use [`Self::describe_topics_by_id`].
    /// DescribeTopicPartitions and Metadata have no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::describe_topics_timeout`].
    pub async fn describe_topics(
        &mut self,
        topics: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Vec<TopicDescription>> {
        self.describe_topics_with(topics, false).await
    }

    /// [`Self::describe_topics`] with a one-shot RPC deadline (Java
    /// `DescribeTopicsOptions.timeoutMs`).
    pub async fn describe_topics_timeout(
        &mut self,
        topics: impl IntoIterator<Item = impl AsRef<str>>,
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        self.describe_topics_with_timeout(topics, false, timeout)
            .await
    }

    /// [`Self::describe_topics`] with Java
    /// `DescribeTopicsOptions.includeAuthorizedOperations`.
    ///
    /// DescribeTopicPartitions always returns TopicAuthorizedOperations.
    /// When `include_authorized_operations` is false, this crate stores
    /// [`AUTHORIZED_OPERATIONS_OMITTED`] (Java Metadata path omits the
    /// request flag; DTP has no equivalent field). The Metadata fallback
    /// sends IncludeTopicAuthorizedOperations when the flag is true.
    pub async fn describe_topics_with(
        &mut self,
        topics: impl IntoIterator<Item = impl AsRef<str>>,
        include_authorized_operations: bool,
    ) -> Result<Vec<TopicDescription>> {
        let timeout = self.cfg.request_timeout;
        self.describe_topics_with_timeout(topics, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::describe_topics_with`] with Java
    /// `DescribeTopicsOptions.timeoutMs`.
    ///
    /// DescribeTopicPartitions and Metadata have no TimeoutMs; `timeout`
    /// is the RPC deadline (and the DTP NextCursor loop budget).
    /// `ResponsePartitionLimit` is 2000 (Java
    /// `DescribeTopicsOptions.partitionSizeLimitPerResponse` default).
    /// For a custom limit, use
    /// [`Self::describe_topics_with_partition_limit_timeout`].
    pub async fn describe_topics_with_timeout(
        &mut self,
        topics: impl IntoIterator<Item = impl AsRef<str>>,
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        self.describe_topics_with_partition_limit_timeout(
            topics,
            include_authorized_operations,
            DESCRIBE_TOPIC_PARTITIONS_LIMIT,
            timeout,
        )
        .await
    }

    /// [`Self::describe_topics_with`] with Java
    /// `DescribeTopicsOptions.partitionSizeLimitPerResponse`.
    ///
    /// `partition_size_limit` is DescribeTopicPartitions
    /// `ResponsePartitionLimit` (Java default 2000). The broker may cap
    /// it at `max.request.partition.size.limit`. Ignored on the Metadata
    /// fallback when api 75 is not advertised. Topic-id describes
    /// ([`Self::describe_topics_by_id`]) stay on Metadata (KAFKA-19628).
    pub async fn describe_topics_with_partition_limit(
        &mut self,
        topics: impl IntoIterator<Item = impl AsRef<str>>,
        include_authorized_operations: bool,
        partition_size_limit: i32,
    ) -> Result<Vec<TopicDescription>> {
        let timeout = self.cfg.request_timeout;
        self.describe_topics_with_partition_limit_timeout(
            topics,
            include_authorized_operations,
            partition_size_limit,
            timeout,
        )
        .await
    }

    /// [`Self::describe_topics_with_partition_limit`] with Java
    /// `DescribeTopicsOptions.timeoutMs`.
    ///
    /// DescribeTopicPartitions and Metadata have no TimeoutMs; `timeout`
    /// is the RPC deadline (and the DTP NextCursor loop budget).
    pub async fn describe_topics_with_partition_limit_timeout(
        &mut self,
        topics: impl IntoIterator<Item = impl AsRef<str>>,
        include_authorized_operations: bool,
        partition_size_limit: i32,
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        let names: Vec<String> = topics.into_iter().map(|s| s.as_ref().to_string()).collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }
        if self.describe_topic_partitions_version.is_some() {
            self.describe_topics_dtp(
                &names,
                include_authorized_operations,
                partition_size_limit,
                timeout,
            )
            .await
        } else {
            self.describe_topics_metadata(&names, include_authorized_operations, timeout)
                .await
        }
    }

    /// Describe topics by TopicId (Java `describeTopics(TopicCollection.ofTopicIds)`).
    ///
    /// Metadata v10+ sends Topics of null Name + TopicId.
    /// `AllowAutoTopicCreation` is false. Brokers that only speak v1–v9
    /// return [`Error::Unsupported`]. Empty `ids` is a no-op. Unknown
    /// ids return `UNKNOWN_TOPIC_ID` (100) per topic with an empty name.
    /// See [`Self::describe_topics_by_id_with`]. Metadata has no TimeoutMs;
    /// the RPC deadline is [`AdminConfig::request_timeout`]. For a
    /// one-shot deadline, use [`Self::describe_topics_by_id_timeout`].
    pub async fn describe_topics_by_id(
        &mut self,
        ids: &[[u8; 16]],
    ) -> Result<Vec<TopicDescription>> {
        self.describe_topics_by_id_with(ids, false).await
    }

    /// [`Self::describe_topics_by_id`] with Java
    /// `DescribeTopicsOptions.includeAuthorizedOperations`.
    ///
    /// Metadata v10+ sends IncludeTopicAuthorizedOperations (the flag
    /// exists from v8; TopicId requires v10). Metadata has no TimeoutMs;
    /// the RPC deadline is [`AdminConfig::request_timeout`]. For a
    /// one-shot deadline, use [`Self::describe_topics_by_id_with_timeout`].
    pub async fn describe_topics_by_id_with(
        &mut self,
        ids: &[[u8; 16]],
        include_authorized_operations: bool,
    ) -> Result<Vec<TopicDescription>> {
        let timeout = self.cfg.request_timeout;
        self.describe_topics_by_id_with_timeout(ids, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::describe_topics_by_id`] with a one-shot RPC deadline (Java
    /// `DescribeTopicsOptions.timeoutMs`).
    pub async fn describe_topics_by_id_timeout(
        &mut self,
        ids: &[[u8; 16]],
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        self.describe_topics_by_id_with_timeout(ids, false, timeout)
            .await
    }

    /// [`Self::describe_topics_by_id_with`] with Java
    /// `DescribeTopicsOptions.timeoutMs`.
    ///
    /// Metadata has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_topics_by_id_with_timeout(
        &mut self,
        ids: &[[u8; 16]],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        if self.metadata_version < 10 {
            return Err(Error::Unsupported(
                "broker does not support Metadata v10 topic IDs".into(),
            ));
        }
        let topics: Vec<MetadataRequestTopic> = ids
            .iter()
            .copied()
            .map(MetadataRequestTopic::by_id)
            .collect();
        let md = self
            .fetch_metadata_request_with(Some(&topics), include_authorized_operations, timeout)
            .await?;
        Ok(topic_descriptions_including_unnamed(&md))
    }

    /// Java `describeTopics(TopicCollection)`.
    ///
    /// [`TopicCollection::Names`] is [`Self::describe_topics`]
    /// (DescribeTopicPartitions, Metadata fallback).
    /// [`TopicCollection::Ids`] is [`Self::describe_topics_by_id`]
    /// (Metadata v10+). Empty collections are a no-op.
    /// DescribeTopicPartitions and Metadata have no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::describe_topics_for_timeout`].
    pub async fn describe_topics_for(
        &mut self,
        topics: &TopicCollection,
    ) -> Result<Vec<TopicDescription>> {
        self.describe_topics_for_with(topics, false).await
    }

    /// [`Self::describe_topics_for`] with a one-shot RPC deadline (Java
    /// `DescribeTopicsOptions.timeoutMs`).
    pub async fn describe_topics_for_timeout(
        &mut self,
        topics: &TopicCollection,
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        self.describe_topics_for_with_timeout(topics, false, timeout)
            .await
    }

    /// [`Self::describe_topics_for`] with Java
    /// `DescribeTopicsOptions.includeAuthorizedOperations`.
    pub async fn describe_topics_for_with(
        &mut self,
        topics: &TopicCollection,
        include_authorized_operations: bool,
    ) -> Result<Vec<TopicDescription>> {
        let timeout = self.cfg.request_timeout;
        self.describe_topics_for_with_timeout(topics, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::describe_topics_for_with`] with Java
    /// `DescribeTopicsOptions.timeoutMs`.
    ///
    /// DescribeTopicPartitions and Metadata have no TimeoutMs; `timeout`
    /// is the RPC deadline (and the DTP NextCursor loop budget).
    pub async fn describe_topics_for_with_timeout(
        &mut self,
        topics: &TopicCollection,
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        match topics {
            TopicCollection::Names(names) => {
                self.describe_topics_with_timeout(names, include_authorized_operations, timeout)
                    .await
            }
            TopicCollection::Ids(ids) => {
                let ids: Vec<[u8; 16]> = ids.iter().copied().map(Uuid::to_bytes).collect();
                self.describe_topics_by_id_with_timeout(
                    &ids,
                    include_authorized_operations,
                    timeout,
                )
                .await
            }
        }
    }

    /// Describe broker or topic configs (`DescribeConfigs`).
    ///
    /// Negotiates v0–v4 (v1 IncludeSynonyms / ConfigSource / Synonyms;
    /// v3 IncludeDocumentation / ConfigType, KIP-226; v4 flexible).
    /// Kafka 4.0 `validVersions` is `1-4`. v5+ is not spoken.
    /// Documentation is omitted (`false`); see
    /// [`Self::describe_configs_with_documentation`]. DescribeConfigs has
    /// no TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use [`Self::describe_configs_timeout`].
    pub async fn describe_configs(
        &mut self,
        resources: &[ConfigResource],
        include_synonyms: bool,
    ) -> Result<Vec<DescribeConfigsResult>> {
        self.describe_configs_with_documentation(resources, include_synonyms, false)
            .await
    }

    /// [`Self::describe_configs`] with a one-shot RPC deadline (Java
    /// `DescribeConfigsOptions.timeoutMs`).
    ///
    /// DescribeConfigs has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_configs_timeout(
        &mut self,
        resources: &[ConfigResource],
        include_synonyms: bool,
        timeout: Duration,
    ) -> Result<Vec<DescribeConfigsResult>> {
        self.describe_configs_with_documentation_timeout(
            resources,
            include_synonyms,
            false,
            timeout,
        )
        .await
    }

    /// DescribeConfigs with documentation (Java `describeConfigs` plus
    /// `DescribeConfigsOptions.includeDocumentation`).
    ///
    /// v3+ sends IncludeDocumentation. v0–v2 omit the field even when
    /// `include_documentation` is set. DescribeConfigs has no TimeoutMs;
    /// the RPC deadline is [`AdminConfig::request_timeout`]. For a
    /// one-shot deadline, use
    /// [`Self::describe_configs_with_documentation_timeout`].
    pub async fn describe_configs_with_documentation(
        &mut self,
        resources: &[ConfigResource],
        include_synonyms: bool,
        include_documentation: bool,
    ) -> Result<Vec<DescribeConfigsResult>> {
        let timeout = self.cfg.request_timeout;
        self.describe_configs_with_documentation_timeout(
            resources,
            include_synonyms,
            include_documentation,
            timeout,
        )
        .await
    }

    /// [`Self::describe_configs_with_documentation`] with Java
    /// `DescribeConfigsOptions.timeoutMs`.
    ///
    /// DescribeConfigs has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_configs_with_documentation_timeout(
        &mut self,
        resources: &[ConfigResource],
        include_synonyms: bool,
        include_documentation: bool,
        timeout: Duration,
    ) -> Result<Vec<DescribeConfigsResult>> {
        let req: Vec<DescribeConfigsResource> = resources
            .iter()
            .map(|r| DescribeConfigsResource {
                resource_type: r.resource_type,
                name: r.name.clone(),
                keys: r.keys.clone(),
            })
            .collect();
        let version = self.describe_version;
        let body = self
            .roundtrip_bootstrap(
                DESCRIBE_CONFIGS,
                version,
                |buf| {
                    encode_describe_configs_request(
                        buf,
                        version,
                        &req,
                        include_synonyms,
                        include_documentation,
                    )
                },
                timeout,
            )
            .await?;
        decode_describe_configs_response(&mut body.clone(), version)
    }

    /// Increase partition count (`CreatePartitions`).
    ///
    /// Negotiates v0–v3 (v0–v1 classic; v2+ flexible; v3 KIP-599
    /// THROTTLING_QUOTA_EXCEEDED). Kafka 4.0 `validVersions` is `0-3`.
    /// v4+ is not spoken. [`NewPartitions::total_count`] is the new
    /// total, not a delta. Lands on the Metadata controller.
    /// `NOT_CONTROLLER` (41) refreshes Metadata and retries.
    /// `timeout_ms` is CreatePartitions TimeoutMs. The RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot timeout that
    /// drives both the RPC deadline and TimeoutMs, use
    /// [`Self::create_partitions_timeout`].
    /// Java `CreatePartitionsOptions.retryOnQuotaViolation` defaults to
    /// `true` (KIP-599); use [`Self::create_partitions_with_quota_retry`]
    /// to disable.
    pub async fn create_partitions(
        &mut self,
        topics: &[NewPartitions],
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout = self.cfg.request_timeout;
        self.create_partitions_with(topics, timeout_ms, validate_only, timeout, true)
            .await
    }

    /// [`Self::create_partitions`] with a one-shot timeout (Java
    /// `CreatePartitionsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and CreatePartitions TimeoutMs.
    pub async fn create_partitions_timeout(
        &mut self,
        topics: &[NewPartitions],
        timeout: Duration,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.create_partitions_with(topics, timeout_ms, validate_only, timeout, true)
            .await
    }

    /// [`Self::create_partitions`] with Java `CreatePartitionsOptions.retryOnQuotaViolation`.
    ///
    /// [`Self::create_partitions`] defaults this to `true` (KIP-599). When
    /// true, topics that return `THROTTLING_QUOTA_EXCEEDED` (89) are
    /// retried alone until the RPC deadline.
    pub async fn create_partitions_with_quota_retry(
        &mut self,
        topics: &[NewPartitions],
        timeout_ms: i32,
        validate_only: bool,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout = self.cfg.request_timeout;
        self.create_partitions_with(
            topics,
            timeout_ms,
            validate_only,
            timeout,
            retry_on_quota_violation,
        )
        .await
    }

    /// [`Self::create_partitions_with_quota_retry`] with a one-shot timeout
    /// (Java `CreatePartitionsOptions.timeoutMs` + `retryOnQuotaViolation`).
    ///
    /// `timeout` is the RPC deadline and CreatePartitions TimeoutMs.
    pub async fn create_partitions_timeout_with_quota_retry(
        &mut self,
        topics: &[NewPartitions],
        timeout: Duration,
        validate_only: bool,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.create_partitions_with(
            topics,
            timeout_ms,
            validate_only,
            timeout,
            retry_on_quota_violation,
        )
        .await
    }

    async fn create_partitions_with(
        &mut self,
        topics: &[NewPartitions],
        timeout_ms: i32,
        validate_only: bool,
        timeout: Duration,
        retry_on_quota_violation: bool,
    ) -> Result<Vec<TopicResult>> {
        let mut pending: Vec<NewPartitions> = topics.to_vec();
        let mut finished: HashMap<String, TopicResult> = HashMap::new();
        let version = self.partitions_version;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let encoded = partitions_from_new(&pending);
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing create_partitions conn"))?;
                conn.roundtrip(
                    CREATE_PARTITIONS,
                    version,
                    |buf| {
                        encode_create_partitions_request(
                            buf,
                            version,
                            &encoded,
                            timeout_ms,
                            validate_only,
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
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results = decode_create_partitions_response(&mut body.clone(), version)?;
            if results
                .iter()
                .any(|r| r.error_code == error::NOT_CONTROLLER)
            {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            let mut next_pending = Vec::new();
            for r in results {
                if retry_on_quota_violation && r.error_code == error::THROTTLING_QUOTA_EXCEEDED {
                    if let Some(t) = pending.iter().find(|t| t.name == r.name) {
                        next_pending.push(t.clone());
                    }
                } else {
                    let name = r.name.clone();
                    let _prev = finished.insert(name, r);
                }
            }
            if next_pending.is_empty() {
                let mut out = Vec::with_capacity(topics.len());
                for t in topics {
                    let r = finished.remove(&t.name).ok_or_else(|| {
                        Error::protocol(format!("missing create_partitions result for {}", t.name))
                    })?;
                    out.push(r);
                }
                return Ok(out);
            }
            pending = next_pending;
            self.wait_retry(&mut attempt, deadline).await?;
        }
    }

    /// Alter configs incrementally (`IncrementalAlterConfigs`).
    ///
    /// Negotiates v0–v1 (v0 classic; v1 flexible). Kafka 4.0
    /// `validVersions` is `0-1`. v2+ is not spoken.
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    /// Returns the first resource's error code. For several resources in
    /// one RPC (Java `incrementalAlterConfigs(Map)`), use
    /// [`Self::incremental_alter_configs_for`]. IncrementalAlterConfigs
    /// has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::incremental_alter_configs_timeout`]. Optional at
    /// [`Self::new`] (Kafka 2.3+ / KIP-339); a broker that omits api 44
    /// returns [`Error::Unsupported`].
    pub async fn incremental_alter_configs(
        &mut self,
        resource: &ConfigResource,
        configs: &[AlterConfig],
        validate_only: bool,
    ) -> Result<i16> {
        let results = self
            .incremental_alter_configs_for(
                &[ConfigResourceUpdate::new(
                    resource.clone(),
                    configs.iter().cloned(),
                )],
                validate_only,
            )
            .await?;
        Ok(results.first().map(|r| r.error_code).unwrap_or(0))
    }

    /// [`Self::incremental_alter_configs`] with a one-shot RPC deadline
    /// (Java `AlterConfigsOptions.timeoutMs`).
    ///
    /// IncrementalAlterConfigs has no TimeoutMs; `timeout` is the RPC
    /// deadline and the `NOT_CONTROLLER` retry budget.
    pub async fn incremental_alter_configs_timeout(
        &mut self,
        resource: &ConfigResource,
        configs: &[AlterConfig],
        timeout: Duration,
        validate_only: bool,
    ) -> Result<i16> {
        let results = self
            .incremental_alter_configs_for_timeout(
                &[ConfigResourceUpdate::new(
                    resource.clone(),
                    configs.iter().cloned(),
                )],
                timeout,
                validate_only,
            )
            .await?;
        Ok(results.first().map(|r| r.error_code).unwrap_or(0))
    }

    /// [`Self::incremental_alter_configs`] for several resources (Java
    /// `incrementalAlterConfigs(Map)`; IncrementalAlterConfigs Resources of N).
    ///
    /// Empty `updates` is a no-op.
    pub async fn incremental_alter_configs_for(
        &mut self,
        updates: &[ConfigResourceUpdate],
        validate_only: bool,
    ) -> Result<Vec<AlterConfigsResourceResult>> {
        let timeout = self.cfg.request_timeout;
        self.incremental_alter_configs_for_timeout(updates, timeout, validate_only)
            .await
    }

    /// [`Self::incremental_alter_configs_for`] with Java
    /// `AlterConfigsOptions.timeoutMs`.
    ///
    /// Empty `updates` is a no-op. IncrementalAlterConfigs has no
    /// TimeoutMs; `timeout` is the RPC deadline and the `NOT_CONTROLLER`
    /// retry budget.
    pub async fn incremental_alter_configs_for_timeout(
        &mut self,
        updates: &[ConfigResourceUpdate],
        timeout: Duration,
        validate_only: bool,
    ) -> Result<Vec<AlterConfigsResourceResult>> {
        if updates.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.alter_version.ok_or_else(|| {
            Error::Unsupported("broker does not support IncrementalAlterConfigs v0-1".into())
        })?;
        let resources: Vec<AlterableResource> = updates
            .iter()
            .map(|u| AlterableResource {
                resource_type: u.resource.resource_type,
                name: u.resource.name.clone(),
                configs: u.configs.clone(),
            })
            .collect();
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing incremental_alter_configs conn"))?;
                conn.roundtrip(
                    INCREMENTAL_ALTER_CONFIGS,
                    version,
                    |buf| {
                        encode_incremental_alter_configs_resources_request(
                            buf,
                            version,
                            &resources,
                            validate_only,
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
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results =
                decode_incremental_alter_configs_resource_results(&mut body.clone(), version)?;
            if results
                .iter()
                .any(|r| r.error_code == error::NOT_CONTROLLER)
            {
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(results);
        }
    }

    /// Create ACL bindings (`CreateAcls`).
    ///
    /// Negotiates v0–v3 (v0–v1 classic; v2+ flexible). v1 adds
    /// ResourcePatternType (LITERAL unless [`AclBinding::pattern_type`]
    /// is set). v3 is the same layout (user resource type). Kafka 4.0
    /// `validVersions` is `1-3`. v4+ is not spoken.
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller. CreateAcls has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use [`Self::create_acls_timeout`].
    pub async fn create_acls(&mut self, acls: &[AclBinding]) -> Result<Vec<i16>> {
        let timeout = self.cfg.request_timeout;
        self.create_acls_timeout(acls, timeout).await
    }

    /// [`Self::create_acls`] with a one-shot RPC deadline (Java
    /// `CreateAclsOptions.timeoutMs`).
    ///
    /// CreateAcls has no TimeoutMs; `timeout` is the RPC deadline and
    /// the `NOT_CONTROLLER` retry budget.
    pub async fn create_acls_timeout(
        &mut self,
        acls: &[AclBinding],
        timeout: Duration,
    ) -> Result<Vec<i16>> {
        let version = self.create_acls_version;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let acls = acls.to_vec();
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing create_acls conn"))?;
                conn.roundtrip(
                    CREATE_ACLS,
                    version,
                    |buf| encode_create_acls_request(buf, version, &acls),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results = decode_create_acls_response(&mut body.clone(), version)?;
            if results.contains(&error::NOT_CONTROLLER) {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(results);
        }
    }

    /// Alter partition replicas (AlterPartitionReassignments api 45).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    /// `timeout_ms` is AlterPartitionReassignments TimeoutMs. The RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// timeout that drives both the RPC deadline and TimeoutMs, use
    /// [`Self::alter_partition_reassignments_timeout`]. Optional at
    /// [`Self::new`] (Kafka 2.4+ / KIP-455); a broker that omits api 45
    /// returns [`Error::Unsupported`].
    pub async fn alter_partition_reassignments(
        &mut self,
        assignments: &[PartitionReassignment],
        timeout_ms: i32,
    ) -> Result<Vec<ReassignmentResult>> {
        let timeout = self.cfg.request_timeout;
        self.alter_partition_reassignments_with(assignments, timeout_ms, timeout)
            .await
    }

    /// [`Self::alter_partition_reassignments`] with a one-shot timeout
    /// (Java `AlterPartitionReassignmentsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and AlterPartitionReassignments
    /// TimeoutMs.
    pub async fn alter_partition_reassignments_timeout(
        &mut self,
        assignments: &[PartitionReassignment],
        timeout: Duration,
    ) -> Result<Vec<ReassignmentResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.alter_partition_reassignments_with(assignments, timeout_ms, timeout)
            .await
    }

    /// Java `alterPartitionReassignments(Map)` ([`NewPartitionReassignment`];
    /// `None` cancels).
    ///
    /// `timeout_ms` is AlterPartitionReassignments TimeoutMs. The RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// timeout that drives both, use
    /// [`Self::alter_partition_reassignments_for_timeout`].
    pub async fn alter_partition_reassignments_for<I, Tp>(
        &mut self,
        assignments: I,
        timeout_ms: i32,
    ) -> Result<Vec<ReassignmentResult>>
    where
        I: IntoIterator<Item = (Tp, Option<NewPartitionReassignment>)>,
        Tp: Into<crate::TopicPartition>,
    {
        let collected: Vec<PartitionReassignment> = assignments
            .into_iter()
            .map(|(tp, assignment)| PartitionReassignment::from_new(tp, assignment))
            .collect();
        self.alter_partition_reassignments(&collected, timeout_ms)
            .await
    }

    /// [`Self::alter_partition_reassignments_for`] with a one-shot timeout
    /// (Java `AlterPartitionReassignmentsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and AlterPartitionReassignments
    /// TimeoutMs.
    pub async fn alter_partition_reassignments_for_timeout<I, Tp>(
        &mut self,
        assignments: I,
        timeout: Duration,
    ) -> Result<Vec<ReassignmentResult>>
    where
        I: IntoIterator<Item = (Tp, Option<NewPartitionReassignment>)>,
        Tp: Into<crate::TopicPartition>,
    {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        let collected: Vec<PartitionReassignment> = assignments
            .into_iter()
            .map(|(tp, assignment)| PartitionReassignment::from_new(tp, assignment))
            .collect();
        self.alter_partition_reassignments_with(&collected, timeout_ms, timeout)
            .await
    }

    async fn alter_partition_reassignments_with(
        &mut self,
        assignments: &[PartitionReassignment],
        timeout_ms: i32,
        timeout: Duration,
    ) -> Result<Vec<ReassignmentResult>> {
        let topics = group_reassignments(assignments);
        let version = self.reassign_version.ok_or_else(|| {
            Error::Unsupported("broker does not support AlterPartitionReassignments".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing alter_partition_reassignments conn"))?;
                conn.roundtrip(
                    ALTER_PARTITION_REASSIGNMENTS,
                    version,
                    |buf| encode_alter_partition_reassignments_request(buf, timeout_ms, &topics),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_alter_partition_reassignments_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER
                || resp.results.iter().any(|t| {
                    t.partitions
                        .iter()
                        .any(|p| p.error_code == error::NOT_CONTROLLER)
                })
            {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(
                    resp.error_code,
                    "AlterPartitionReassignments",
                ));
            }
            return Ok(flatten_reassignment_results(&resp.results));
        }
    }

    /// List ongoing replica moves (ListPartitionReassignments api 46).
    ///
    /// `partitions = None` lists every ongoing reassignment. Lands on the
    /// Metadata controller. `NOT_CONTROLLER` (41) refreshes Metadata and
    /// retries on the new controller.
    /// `timeout_ms` is ListPartitionReassignments TimeoutMs. The RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// timeout that drives both the RPC deadline and TimeoutMs, use
    /// [`Self::list_partition_reassignments_timeout`]. Optional at
    /// [`Self::new`] (Kafka 2.4+ / KIP-455); a broker that omits api 46
    /// returns [`Error::Unsupported`].
    pub async fn list_partition_reassignments(
        &mut self,
        partitions: Option<&[crate::TopicPartition]>,
        timeout_ms: i32,
    ) -> Result<Vec<OngoingReassignment>> {
        let timeout = self.cfg.request_timeout;
        self.list_partition_reassignments_with(partitions, timeout_ms, timeout)
            .await
    }

    /// [`Self::list_partition_reassignments`] with a one-shot timeout
    /// (Java `ListPartitionReassignmentsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and ListPartitionReassignments
    /// TimeoutMs.
    pub async fn list_partition_reassignments_timeout(
        &mut self,
        partitions: Option<&[crate::TopicPartition]>,
        timeout: Duration,
    ) -> Result<Vec<OngoingReassignment>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.list_partition_reassignments_with(partitions, timeout_ms, timeout)
            .await
    }

    /// List every ongoing replica move (Java
    /// `Admin.listPartitionReassignments()`).
    ///
    /// Same wire as [`Self::list_partition_reassignments`] with `topics`
    /// null. TimeoutMs is [`AdminConfig::request_timeout`].
    pub async fn list_partition_reassignments_all(&mut self) -> Result<Vec<OngoingReassignment>> {
        let timeout = self.cfg.request_timeout;
        self.list_partition_reassignments_all_timeout(timeout).await
    }

    /// [`Self::list_partition_reassignments_all`] with a one-shot timeout
    /// (Java `ListPartitionReassignmentsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and ListPartitionReassignments
    /// TimeoutMs.
    pub async fn list_partition_reassignments_all_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<OngoingReassignment>> {
        self.list_partition_reassignments_timeout(None, timeout)
            .await
    }

    /// List ongoing replica moves for `partitions` (Java
    /// `Admin.listPartitionReassignments(Set)`).
    ///
    /// TimeoutMs is [`AdminConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::list_partition_reassignments_timeout`].
    pub async fn list_partition_reassignments_for(
        &mut self,
        partitions: &[crate::TopicPartition],
    ) -> Result<Vec<OngoingReassignment>> {
        let timeout = self.cfg.request_timeout;
        self.list_partition_reassignments_timeout(Some(partitions), timeout)
            .await
    }

    async fn list_partition_reassignments_with(
        &mut self,
        partitions: Option<&[crate::TopicPartition]>,
        timeout_ms: i32,
        timeout: Duration,
    ) -> Result<Vec<OngoingReassignment>> {
        let topics = partitions.map(group_list_reassignments);
        let version = self.list_reassign_version.ok_or_else(|| {
            Error::Unsupported("broker does not support ListPartitionReassignments".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing list_partition_reassignments conn"))?;
                conn.roundtrip(
                    LIST_PARTITION_REASSIGNMENTS,
                    version,
                    |buf| {
                        encode_list_partition_reassignments_request(
                            buf,
                            timeout_ms,
                            topics.as_deref(),
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
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_list_partition_reassignments_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "ListPartitionReassignments"));
            }
            return Ok(flatten_list_reassignments(&resp.topics));
        }
    }

    /// Update finalized feature versions (UpdateFeatures api 57).
    ///
    /// Negotiates v0–v2 (flexible from v0). v0 sends `AllowDowngrade`.
    /// v1+ sends `UpgradeType` and `ValidateOnly` false. v2 omits
    /// per-feature Results; this method synthesizes success rows from
    /// the request when the top-level error is `0`. Kafka 4.0
    /// `validVersions` is `0-2`. v3+ is not spoken. Lands on the
    /// Metadata controller. `NOT_CONTROLLER` (41) refreshes Metadata
    /// and retries. `timeout_ms` is UpdateFeatures TimeoutMs. The RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// timeout that drives both the RPC deadline and TimeoutMs, use
    /// [`Self::update_features_timeout`]. See
    /// [`Self::update_features_with`] for Java
    /// `UpdateFeaturesOptions.validateOnly`. Optional at [`Self::new`]
    /// (Kafka 2.7+ / KIP-584); a broker that omits api 57 returns
    /// [`Error::Unsupported`].
    pub async fn update_features(
        &mut self,
        updates: &[FeatureUpdate],
        timeout_ms: i32,
    ) -> Result<Vec<FeatureUpdateResult>> {
        let timeout = self.cfg.request_timeout;
        self.update_features_inner(updates, timeout_ms, false, timeout)
            .await
    }

    /// [`Self::update_features`] with a one-shot timeout (Java
    /// `UpdateFeaturesOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and UpdateFeatures TimeoutMs.
    pub async fn update_features_timeout(
        &mut self,
        updates: &[FeatureUpdate],
        timeout: Duration,
    ) -> Result<Vec<FeatureUpdateResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.update_features_inner(updates, timeout_ms, false, timeout)
            .await
    }

    /// UpdateFeatures with validate-only (Java `updateFeatures` plus
    /// `UpdateFeaturesOptions.validateOnly`).
    ///
    /// v1+ sends `ValidateOnly`. v0 omits the field even when set.
    /// `timeout_ms` is UpdateFeatures TimeoutMs. The RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot timeout that
    /// drives both, use [`Self::update_features_with_timeout`].
    pub async fn update_features_with(
        &mut self,
        updates: &[FeatureUpdate],
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Vec<FeatureUpdateResult>> {
        let timeout = self.cfg.request_timeout;
        self.update_features_inner(updates, timeout_ms, validate_only, timeout)
            .await
    }

    /// [`Self::update_features_with`] with a one-shot timeout (Java
    /// `UpdateFeaturesOptions.timeoutMs` plus `validateOnly`).
    ///
    /// `timeout` is the RPC deadline and UpdateFeatures TimeoutMs.
    pub async fn update_features_with_timeout(
        &mut self,
        updates: &[FeatureUpdate],
        timeout: Duration,
        validate_only: bool,
    ) -> Result<Vec<FeatureUpdateResult>> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.update_features_inner(updates, timeout_ms, validate_only, timeout)
            .await
    }

    async fn update_features_inner(
        &mut self,
        updates: &[FeatureUpdate],
        timeout_ms: i32,
        validate_only: bool,
        timeout: Duration,
    ) -> Result<Vec<FeatureUpdateResult>> {
        let keys: Vec<FeatureUpdateKey> = updates
            .iter()
            .map(|u| FeatureUpdateKey {
                name: u.name.clone(),
                max_version_level: u.max_version_level,
                allow_downgrade: u.allow_downgrade,
                upgrade_type: u.upgrade_type,
            })
            .collect();
        let version = self.update_features_version.ok_or_else(|| {
            Error::Unsupported("broker does not support UpdateFeatures v0-2".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing update_features conn"))?;
                conn.roundtrip(
                    UPDATE_FEATURES,
                    version,
                    |buf| {
                        encode_update_features_request(
                            buf,
                            version,
                            timeout_ms,
                            &keys,
                            validate_only,
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
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_update_features_response(&mut body.clone(), version)?;
            if resp.error_code == error::NOT_CONTROLLER
                || resp
                    .results
                    .iter()
                    .any(|r| r.error_code == error::NOT_CONTROLLER)
            {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "UpdateFeatures"));
            }
            if version >= 2 {
                return Ok(keys
                    .iter()
                    .map(|k| FeatureUpdateResult {
                        name: k.name.clone(),
                        error_code: 0,
                        error_message: None,
                    })
                    .collect());
            }
            return Ok(resp
                .results
                .into_iter()
                .map(|r| FeatureUpdateResult {
                    name: r.name,
                    error_code: r.error_code,
                    error_message: r.error_message,
                })
                .collect());
        }
    }

    /// Supported and finalized features (Java `describeFeatures`).
    ///
    /// There is no DescribeFeatures api key. This re-issues ApiVersions v3–v4
    /// on the bootstrap connection and reads KIP-482 tagged fields
    /// (`supportedFeatures`, `finalizedFeaturesEpoch`, `finalizedFeatures`,
    /// `zkMigrationReady`). v4 includes SupportedFeatures with MinVersion 0
    /// (KAFKA-17011). ApiVersions has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_features_timeout`].
    pub async fn describe_features(&mut self) -> Result<FeatureMetadata> {
        let timeout = self.cfg.request_timeout;
        self.describe_features_timeout(timeout).await
    }

    /// [`Self::describe_features`] with a one-shot RPC deadline (Java
    /// `DescribeFeaturesOptions.timeoutMs`).
    ///
    /// There is no DescribeFeatures api key; this re-issues ApiVersions
    /// v3–v4. ApiVersions has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_features_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<FeatureMetadata> {
        let version = self
            .versions
            .get(&API_VERSIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 3, 4))
            .unwrap_or(3);
        let body = self
            .roundtrip_bootstrap(
                API_VERSIONS,
                version,
                |buf| encode_api_versions_request(buf, version, "partitionline", "0.1.0"),
                timeout,
            )
            .await?;
        let resp = decode_api_versions_response(&mut body.clone(), version)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ApiVersions"));
        }
        Ok(FeatureMetadata {
            supported_features: resp
                .supported_features
                .into_iter()
                .map(|f| SupportedVersionRange {
                    name: f.name,
                    min_version: f.min_version,
                    max_version: f.max_version,
                })
                .collect(),
            finalized_features: resp
                .finalized_features
                .into_iter()
                .map(|f| FinalizedVersionRange {
                    name: f.name,
                    min_version_level: f.min_version_level,
                    max_version_level: f.max_version_level,
                })
                .collect(),
            finalized_features_epoch: resp.finalized_features_epoch,
            zk_migration_ready: resp.zk_migration_ready,
        })
    }

    /// Upsert or delete user SCRAM credentials (AlterUserScramCredentials
    /// api 51).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller. AlterUserScramCredentials
    /// has no TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use
    /// [`Self::alter_user_scram_credentials_timeout`]. Optional at
    /// [`Self::new`] (Kafka 2.7+ / KIP-554); a broker that omits api 51
    /// returns [`Error::Unsupported`]. Java
    /// `alterUserScramCredentials(List)` is
    /// [`Self::alter_user_scram_credentials_with`].
    pub async fn alter_user_scram_credentials(
        &mut self,
        deletions: &[UserScramCredentialDeletion],
        upsertions: &[UserScramCredentialUpsertion],
    ) -> Result<Vec<UserScramCredentialResult>> {
        let timeout = self.cfg.request_timeout;
        self.alter_user_scram_credentials_timeout(deletions, upsertions, timeout)
            .await
    }

    /// Java `alterUserScramCredentials(List)` of [`UserScramCredentialAlteration`].
    ///
    /// The wire request still splits Deletions then Upsertions. AlterUserScramCredentials
    /// has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::alter_user_scram_credentials_with_timeout`].
    pub async fn alter_user_scram_credentials_with<A>(
        &mut self,
        alterations: impl IntoIterator<Item = A>,
    ) -> Result<Vec<UserScramCredentialResult>>
    where
        A: Into<UserScramCredentialAlteration>,
    {
        let timeout = self.cfg.request_timeout;
        self.alter_user_scram_credentials_with_timeout(alterations, timeout)
            .await
    }

    /// [`Self::alter_user_scram_credentials_with`] with a one-shot RPC
    /// deadline (Java `AlterUserScramCredentialsOptions.timeoutMs`).
    ///
    /// AlterUserScramCredentials has no TimeoutMs; `timeout` is the RPC
    /// deadline and the `NOT_CONTROLLER` retry budget.
    pub async fn alter_user_scram_credentials_with_timeout<A>(
        &mut self,
        alterations: impl IntoIterator<Item = A>,
        timeout: Duration,
    ) -> Result<Vec<UserScramCredentialResult>>
    where
        A: Into<UserScramCredentialAlteration>,
    {
        let mut deletions = Vec::new();
        let mut upsertions = Vec::new();
        for item in alterations {
            match item.into() {
                UserScramCredentialAlteration::Deletion(d) => deletions.push(d),
                UserScramCredentialAlteration::Upsertion(u) => upsertions.push(u),
            }
        }
        self.alter_user_scram_credentials_timeout(&deletions, &upsertions, timeout)
            .await
    }

    /// [`Self::alter_user_scram_credentials`] with a one-shot RPC deadline
    /// (Java `AlterUserScramCredentialsOptions.timeoutMs`).
    ///
    /// AlterUserScramCredentials has no TimeoutMs; `timeout` is the RPC
    /// deadline and the `NOT_CONTROLLER` retry budget.
    pub async fn alter_user_scram_credentials_timeout(
        &mut self,
        deletions: &[UserScramCredentialDeletion],
        upsertions: &[UserScramCredentialUpsertion],
        timeout: Duration,
    ) -> Result<Vec<UserScramCredentialResult>> {
        let deletions: Vec<ScramCredentialDeletion> = deletions
            .iter()
            .map(|d| ScramCredentialDeletion {
                name: d.name.clone(),
                mechanism: d.mechanism,
            })
            .collect();
        let upsertions: Vec<ScramCredentialUpsertion> = upsertions
            .iter()
            .map(|u| ScramCredentialUpsertion {
                name: u.name.clone(),
                mechanism: u.mechanism,
                iterations: u.iterations,
                salt: u.salt.clone(),
                salted_password: u.salted_password.clone(),
            })
            .collect();
        let version = self.alter_user_scram_version.ok_or_else(|| {
            Error::Unsupported("broker does not support AlterUserScramCredentials".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing alter_user_scram_credentials conn"))?;
                conn.roundtrip(
                    ALTER_USER_SCRAM_CREDENTIALS,
                    version,
                    |buf| encode_alter_user_scram_credentials_request(buf, &deletions, &upsertions),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results = decode_alter_user_scram_credentials_response(&mut body.clone())?;
            if results
                .iter()
                .any(|r| r.error_code == error::NOT_CONTROLLER)
            {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(results
                .into_iter()
                .map(|r| UserScramCredentialResult {
                    user: r.user,
                    error_code: r.error_code,
                    error_message: r.error_message,
                })
                .collect());
        }
    }

    /// Describe user SCRAM credentials (DescribeUserScramCredentials
    /// api 50).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller. Top-level `error_code`
    /// (bytes 4–5), not a first-result field. Empty `users` sends a null
    /// Users array (Java `describeUserScramCredentials()` / empty list)
    /// and describes every user. DescribeUserScramCredentials has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use
    /// [`Self::describe_user_scram_credentials_timeout`]. Optional at
    /// [`Self::new`] (Kafka 2.7+ / KIP-554); a broker that omits api 50
    /// returns [`Error::Unsupported`].
    pub async fn describe_user_scram_credentials(
        &mut self,
        users: &[&str],
    ) -> Result<Vec<DescribeUserScramCredentialsResult>> {
        let timeout = self.cfg.request_timeout;
        self.describe_user_scram_credentials_timeout(users, timeout)
            .await
    }

    /// [`Self::describe_user_scram_credentials`] with a one-shot RPC
    /// deadline (Java `DescribeUserScramCredentialsOptions.timeoutMs`).
    ///
    /// DescribeUserScramCredentials has no TimeoutMs; `timeout` is the
    /// RPC deadline and the `NOT_CONTROLLER` retry budget.
    pub async fn describe_user_scram_credentials_timeout(
        &mut self,
        users: &[&str],
        timeout: Duration,
    ) -> Result<Vec<DescribeUserScramCredentialsResult>> {
        let users: Vec<String> = users.iter().map(|s| (*s).to_string()).collect();
        let users_wire: Option<&[String]> = if users.is_empty() {
            None
        } else {
            Some(users.as_slice())
        };
        let version = self.describe_user_scram_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DescribeUserScramCredentials".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self.conns.get_mut(&node).ok_or_else(|| {
                    Error::protocol("missing describe_user_scram_credentials conn")
                })?;
                conn.roundtrip(
                    DESCRIBE_USER_SCRAM_CREDENTIALS,
                    version,
                    |buf| encode_describe_user_scram_credentials_request(buf, users_wire),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_describe_user_scram_credentials_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(
                    resp.error_code,
                    "DescribeUserScramCredentials",
                ));
            }
            return Ok(resp.results);
        }
    }

    /// Describe every user's SCRAM credentials (Java
    /// `Admin.describeUserScramCredentials()`).
    ///
    /// Same wire as [`Self::describe_user_scram_credentials`] with an
    /// empty user list (Users null).
    pub async fn describe_user_scram_credentials_all(
        &mut self,
    ) -> Result<Vec<DescribeUserScramCredentialsResult>> {
        let timeout = self.cfg.request_timeout;
        self.describe_user_scram_credentials_all_timeout(timeout)
            .await
    }

    /// [`Self::describe_user_scram_credentials_all`] with a one-shot RPC
    /// deadline (Java `DescribeUserScramCredentialsOptions.timeoutMs`).
    pub async fn describe_user_scram_credentials_all_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<DescribeUserScramCredentialsResult>> {
        self.describe_user_scram_credentials_timeout(&[], timeout)
            .await
    }

    /// Unregister a broker (UnregisterBroker api 64, KIP-500).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller. Top-level `error_code`
    /// (bytes 4–5), after throttle. Fixture broker id only; this is not
    /// a live KRaft unregistration. Optional at [`Self::new`] (Java
    /// `@InterfaceStability.Unstable`); a broker that omits api 64
    /// returns [`Error::Unsupported`]. UnregisterBroker has no TimeoutMs;
    /// the RPC deadline is [`AdminConfig::request_timeout`]. For a
    /// one-shot deadline, use [`Self::unregister_broker_timeout`].
    pub async fn unregister_broker(&mut self, broker_id: i32) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.unregister_broker_timeout(broker_id, timeout).await
    }

    /// [`Self::unregister_broker`] with a one-shot RPC deadline (Java
    /// `UnregisterBrokerOptions.timeoutMs`).
    ///
    /// UnregisterBroker has no TimeoutMs; `timeout` is the RPC deadline
    /// and the `NOT_CONTROLLER` retry budget.
    pub async fn unregister_broker_timeout(
        &mut self,
        broker_id: i32,
        timeout: Duration,
    ) -> Result<()> {
        let version = self
            .unregister_broker_version
            .ok_or_else(|| Error::Unsupported("broker does not support UnregisterBroker".into()))?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing unregister_broker conn"))?;
                conn.roundtrip(
                    UNREGISTER_BROKER,
                    version,
                    |buf| encode_unregister_broker_request(buf, broker_id),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_unregister_broker_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "UnregisterBroker"));
            }
            return Ok(());
        }
    }

    /// Describe client quotas (DescribeClientQuotas api 48, KIP-219).
    ///
    /// Lands on the connected broker (bootstrap is fine). Negotiates
    /// DescribeClientQuotas v0–v1 (Kafka 4.0 `validVersions` `0-1`;
    /// v0 classic, v1 flexible). Official Apache
    /// JSON listeners are `broker` only. This is not a controller hop:
    /// there is no Metadata `controller_id` lookup and no
    /// `NOT_CONTROLLER` (41) retry. Top-level `error_code` is the INT16
    /// at bytes 4–5, after throttle. DescribeClientQuotas has no TimeoutMs;
    /// the RPC deadline is [`AdminConfig::request_timeout`]. For a
    /// one-shot deadline, use [`Self::describe_client_quotas_timeout`].
    /// Optional at [`Self::new`] (Kafka 2.6+ / KIP-219); a broker that
    /// omits api 48 returns [`Error::Unsupported`]. See
    /// [`Self::describe_client_quotas_all`] for Java
    /// `ClientQuotaFilter.all()`, or [`Self::describe_client_quotas_with`]
    /// for [`ClientQuotaFilter::contains`] / [`ClientQuotaFilter::contains_only`].
    pub async fn describe_client_quotas(
        &mut self,
        components: &[ClientQuotaFilterComponent],
        strict: bool,
    ) -> Result<Vec<ClientQuotaEntry>> {
        let timeout = self.cfg.request_timeout;
        self.describe_client_quotas_timeout(components, strict, timeout)
            .await
    }

    /// [`Self::describe_client_quotas`] with a one-shot RPC deadline (Java
    /// `DescribeClientQuotasOptions.timeoutMs`).
    ///
    /// DescribeClientQuotas has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_client_quotas_timeout(
        &mut self,
        components: &[ClientQuotaFilterComponent],
        strict: bool,
        timeout: Duration,
    ) -> Result<Vec<ClientQuotaEntry>> {
        let components = components.to_vec();
        let version = self.describe_client_quotas_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DescribeClientQuotas v0-1".into())
        })?;
        let body = self
            .roundtrip_bootstrap(
                DESCRIBE_CLIENT_QUOTAS,
                version,
                |buf| encode_describe_client_quotas_request(buf, version, &components, strict),
                timeout,
            )
            .await?;
        let resp = decode_describe_client_quotas_response(&mut body.clone(), version)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "DescribeClientQuotas"));
        }
        Ok(resp.entries.unwrap_or_default())
    }

    /// [`Self::describe_client_quotas`] with a Java `ClientQuotaFilter`.
    ///
    /// DescribeClientQuotas has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_client_quotas_with_timeout`].
    pub async fn describe_client_quotas_with(
        &mut self,
        filter: &ClientQuotaFilter,
    ) -> Result<Vec<ClientQuotaEntry>> {
        self.describe_client_quotas(filter.components(), filter.strict())
            .await
    }

    /// [`Self::describe_client_quotas_with`] with a one-shot RPC deadline
    /// (Java `DescribeClientQuotasOptions.timeoutMs`).
    ///
    /// DescribeClientQuotas has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_client_quotas_with_timeout(
        &mut self,
        filter: &ClientQuotaFilter,
        timeout: Duration,
    ) -> Result<Vec<ClientQuotaEntry>> {
        self.describe_client_quotas_timeout(filter.components(), filter.strict(), timeout)
            .await
    }

    /// Describe every client quota (Java `ClientQuotaFilter.all()`).
    ///
    /// Same wire as [`Self::describe_client_quotas_with`] with
    /// [`ClientQuotaFilter::all`].
    pub async fn describe_client_quotas_all(&mut self) -> Result<Vec<ClientQuotaEntry>> {
        self.describe_client_quotas_with(&ClientQuotaFilter::all())
            .await
    }

    /// [`Self::describe_client_quotas_all`] with a one-shot RPC deadline
    /// (Java `DescribeClientQuotasOptions.timeoutMs`).
    pub async fn describe_client_quotas_all_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ClientQuotaEntry>> {
        self.describe_client_quotas_with_timeout(&ClientQuotaFilter::all(), timeout)
            .await
    }

    /// Upsert or delete client quotas (AlterClientQuotas api 49).
    ///
    /// Lands on the Metadata controller. Negotiates AlterClientQuotas
    /// v0–v1 (Kafka 4.0 `validVersions` `0-1`; v0 classic, v1 flexible).
    /// `NOT_CONTROLLER` (41) refreshes Metadata and retries on the new
    /// controller. AlterClientQuotas has no TimeoutMs; the RPC deadline
    /// is [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::alter_client_quotas_timeout`]. Optional at [`Self::new`]
    /// (Kafka 2.6+ / KIP-219); a broker that omits api 49 returns
    /// [`Error::Unsupported`].
    pub async fn alter_client_quotas(
        &mut self,
        entries: &[ClientQuotaAlteration],
        validate_only: bool,
    ) -> Result<Vec<ClientQuotaAlterationResult>> {
        let timeout = self.cfg.request_timeout;
        self.alter_client_quotas_timeout(entries, timeout, validate_only)
            .await
    }

    /// [`Self::alter_client_quotas`] with a one-shot RPC deadline (Java
    /// `AlterClientQuotasOptions.timeoutMs`).
    ///
    /// AlterClientQuotas has no TimeoutMs; `timeout` is the RPC deadline
    /// and the `NOT_CONTROLLER` retry budget.
    pub async fn alter_client_quotas_timeout(
        &mut self,
        entries: &[ClientQuotaAlteration],
        timeout: Duration,
        validate_only: bool,
    ) -> Result<Vec<ClientQuotaAlterationResult>> {
        let entries = entries.to_vec();
        let version = self.alter_client_quotas_version.ok_or_else(|| {
            Error::Unsupported("broker does not support AlterClientQuotas v0-1".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing alter_client_quotas conn"))?;
                conn.roundtrip(
                    ALTER_CLIENT_QUOTAS,
                    version,
                    |buf| encode_alter_client_quotas_request(buf, version, &entries, validate_only),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results = decode_alter_client_quotas_response(&mut body.clone(), version)?;
            if results
                .iter()
                .any(|r| r.error_code == error::NOT_CONTROLLER)
            {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(results);
        }
    }

    /// Allocate a producer-id block (AllocateProducerIds api 67).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller. `broker_id` /
    /// `broker_epoch` are the requesting broker's fixture identity.
    /// AllocateProducerIds has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::allocate_producer_ids_timeout`].
    pub async fn allocate_producer_ids(
        &mut self,
        broker_id: i32,
        broker_epoch: i64,
    ) -> Result<ProducerIdBlock> {
        let timeout = self.cfg.request_timeout;
        self.allocate_producer_ids_timeout(broker_id, broker_epoch, timeout)
            .await
    }

    /// [`Self::allocate_producer_ids`] with a one-shot RPC deadline.
    ///
    /// AllocateProducerIds has no TimeoutMs; `timeout` is the RPC
    /// deadline and the `NOT_CONTROLLER` retry budget. Java `Admin`
    /// has no `allocateProducerIds`; this overload is crate-first.
    pub async fn allocate_producer_ids_timeout(
        &mut self,
        broker_id: i32,
        broker_epoch: i64,
        timeout: Duration,
    ) -> Result<ProducerIdBlock> {
        let version = self.allocate_producer_ids_version.ok_or_else(|| {
            Error::Unsupported("broker does not support AllocateProducerIds".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing allocate_producer_ids conn"))?;
                conn.roundtrip(
                    ALLOCATE_PRODUCER_IDS,
                    version,
                    |buf| encode_allocate_producer_ids_request(buf, broker_id, broker_epoch),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_allocate_producer_ids_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "AllocateProducerIds"));
            }
            return Ok(ProducerIdBlock {
                producer_id_start: resp.producer_id_start,
                producer_id_len: resp.producer_id_len,
            });
        }
    }

    /// Fence transactional producers (Java `Admin.fenceProducers`).
    ///
    /// InitProducerId v0–v5 on each `transactional.id`'s transaction
    /// coordinator (`FindCoordinator` `key_type=1`). Empty
    /// `transactional_ids` returns an empty list. Coordinator load /
    /// move errors refresh and retry. [`AdminConfig::request_timeout`]
    /// is the RPC deadline and is sent as `transaction.timeout.ms`. For
    /// a one-shot timeout, use [`Self::fence_producers_timeout`]. Java
    /// `forceTerminateTransaction` is [`Self::force_terminate_transaction`].
    pub async fn fence_producers(
        &mut self,
        transactional_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Vec<FencedProducer>> {
        let timeout = self.cfg.request_timeout;
        self.fence_producers_timeout(transactional_ids, timeout)
            .await
    }

    /// [`Self::fence_producers`] with a one-shot timeout (Java
    /// `fenceProducers` plus `FenceProducersOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and InitProducerId
    /// `transaction.timeout.ms`.
    pub async fn fence_producers_timeout(
        &mut self,
        transactional_ids: impl IntoIterator<Item = impl Into<String>>,
        timeout: Duration,
    ) -> Result<Vec<FencedProducer>> {
        let mut out = Vec::new();
        for id in transactional_ids {
            let transactional_id = id.into();
            let (producer_id, epoch) = self.fence_one(&transactional_id, timeout).await?;
            out.push(FencedProducer {
                transactional_id,
                producer_id,
                epoch,
            });
        }
        Ok(out)
    }

    /// Force-terminate a transactional id (Java
    /// `Admin.forceTerminateTransaction`).
    ///
    /// Same wire as [`Self::fence_producers`] for one id: InitProducerId
    /// on the transaction coordinator (`FindCoordinator` `key_type=1`).
    /// Java's `forceTerminateTransaction` calls `fenceProducers` with a
    /// singleton set. Waits up to [`AdminConfig::request_timeout`]. For
    /// a one-shot timeout, use [`Self::force_terminate_transaction_timeout`].
    pub async fn force_terminate_transaction(
        &mut self,
        transactional_id: impl Into<String>,
    ) -> Result<FencedProducer> {
        let timeout = self.cfg.request_timeout;
        self.force_terminate_transaction_timeout(transactional_id, timeout)
            .await
    }

    /// [`Self::force_terminate_transaction`] with a one-shot timeout
    /// (Java `forceTerminateTransaction` plus
    /// `FenceProducersOptions.timeoutMs`).
    pub async fn force_terminate_transaction_timeout(
        &mut self,
        transactional_id: impl Into<String>,
        timeout: Duration,
    ) -> Result<FencedProducer> {
        let transactional_id = transactional_id.into();
        let (producer_id, epoch) = self.fence_one(&transactional_id, timeout).await?;
        Ok(FencedProducer {
            transactional_id,
            producer_id,
            epoch,
        })
    }

    async fn fence_one(&mut self, transactional_id: &str, timeout: Duration) -> Result<(i64, i16)> {
        let version = self
            .versions
            .get(&INIT_PRODUCER_ID)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 5))
            .ok_or_else(|| Error::Unsupported("broker does not support InitProducerId".into()))?;
        let txn_timeout_ms = crate::consumer::duration_millis_i32(timeout);
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let stale = self
                .txn_coord
                .as_ref()
                .is_none_or(|(k, _)| k != transactional_id);
            if stale {
                let node = self.discover_txn_coord(transactional_id).await?;
                self.txn_coord = Some((transactional_id.to_string(), node));
            }
            let node = self
                .txn_coord
                .as_ref()
                .map(|(_, n)| *n)
                .ok_or_else(|| Error::protocol("missing transaction coordinator"))?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing fence_producers conn"))?;
                conn.roundtrip(
                    INIT_PRODUCER_ID,
                    version,
                    |buf| {
                        encode_init_producer_id_request(
                            buf,
                            version,
                            Some(transactional_id),
                            txn_timeout_ms,
                            -1,
                            -1,
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
                    self.txn_coord = None;
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (err, producer_id, epoch) =
                decode_init_producer_id_response(&mut body.clone(), version)?;
            if err == 0 {
                return Ok((producer_id, epoch));
            }
            if error::coordinator_retriable(err) {
                self.txn_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Err(Error::broker(err, "InitProducerId"));
        }
    }

    /// Force-abort an open transaction on a partition (Java `abortTransaction`).
    ///
    /// Sends WriteTxnMarkers (api 27) v0 (classic) or v1 (flexible;
    /// Kafka 4.0 baseline) with `transactionResult=false` to the
    /// Metadata partition leader. `NOT_LEADER_OR_FOLLOWER` and
    /// fenced/unknown leader epochs refresh Metadata and retry. This is
    /// not a controller hop and not a transaction-coordinator hop.
    /// v2 `TransactionVersion` (KIP-1228) is not spoken. WriteTxnMarkers
    /// has no TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use [`Self::abort_transaction_timeout`].
    pub async fn abort_transaction(&mut self, spec: AbortTransactionSpec) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.abort_transaction_timeout(spec, timeout).await
    }

    /// [`Self::abort_transaction`] with a one-shot RPC deadline (Java
    /// `AbortTransactionOptions.timeoutMs`).
    ///
    /// WriteTxnMarkers has no TimeoutMs; `timeout` is the RPC deadline
    /// and the `NOT_LEADER_OR_FOLLOWER` retry budget.
    pub async fn abort_transaction_timeout(
        &mut self,
        spec: AbortTransactionSpec,
        timeout: Duration,
    ) -> Result<()> {
        let version = self
            .versions
            .get(&WRITE_TXN_MARKERS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| Error::Unsupported("broker does not support WriteTxnMarkers".into()))?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let marker = WritableTxnMarker {
            producer_id: spec.producer_id,
            producer_epoch: spec.producer_epoch,
            transaction_result: false,
            topics: vec![WritableTxnMarkerTopic {
                name: spec.topic.clone(),
                partitions: vec![spec.partition],
            }],
            coordinator_epoch: spec.coordinator_epoch,
        };
        loop {
            if self.cluster.leader(&spec.topic, spec.partition).is_err() {
                let topics = [spec.topic.clone()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(&spec.topic, spec.partition)?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing abort_transaction conn"))?;
                conn.roundtrip(
                    WRITE_TXN_MARKERS,
                    version,
                    |buf| {
                        encode_write_txn_markers_request(
                            buf,
                            version,
                            std::slice::from_ref(&marker),
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
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_write_txn_markers_response(&mut body.clone(), version)?;
            let error_code = resp
                .iter()
                .flat_map(|m| m.topics.iter())
                .flat_map(|t| t.partitions.iter())
                .map(|p| p.error_code)
                .find(|&c| c != 0)
                .unwrap_or(0);
            if error_code == 0 {
                return Ok(());
            }
            let e = Error::broker(error_code, "WriteTxnMarkers");
            if matches!(
                error_code,
                error::FENCED_LEADER_EPOCH | error::UNKNOWN_LEADER_EPOCH
            ) || e.is_retriable()
            {
                self.cluster.invalidate_topic(&spec.topic);
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                let topics = [spec.topic.clone()];
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Err(e);
        }
    }

    /// Describe transactional.id state (DescribeTransactions api 65).
    ///
    /// Lands on the transaction coordinator (`FindCoordinator`
    /// `key_type=1`). `COORDINATOR_LOAD_IN_PROGRESS` /
    /// `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. This is not `NOT_CONTROLLER` (41).
    /// FindCoordinator v4+ CoordinatorKeys array of N (KIP-699): one
    /// FindCoordinator per retry for uncached transactional ids.
    /// DescribeTransactions is one RPC per coordinator. Brokers that
    /// only speak FindCoordinator v1–v3 get one FindCoordinator per
    /// uncached id. Empty input is a no-op. Optional at [`Self::new`]
    /// (Kafka 2.5+ / KIP-573); a broker that omits api 65 returns
    /// [`Error::Unsupported`]. DescribeTransactions has no TimeoutMs;
    /// the RPC deadline is [`AdminConfig::request_timeout`]. For a
    /// one-shot deadline, use [`Self::describe_transactions_timeout`].
    pub async fn describe_transactions(
        &mut self,
        transactional_ids: &[&str],
    ) -> Result<Vec<TransactionState>> {
        let timeout = self.cfg.request_timeout;
        self.describe_transactions_timeout(transactional_ids, timeout)
            .await
    }

    /// [`Self::describe_transactions`] with a one-shot RPC deadline (Java
    /// `DescribeTransactionsOptions.timeoutMs`).
    ///
    /// DescribeTransactions has no TimeoutMs; `timeout` is the RPC
    /// deadline and the coordinator retry budget.
    pub async fn describe_transactions_timeout(
        &mut self,
        transactional_ids: &[&str],
        timeout: Duration,
    ) -> Result<Vec<TransactionState>> {
        let ids: Vec<String> = transactional_ids.iter().map(|s| (*s).to_string()).collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.describe_transactions_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DescribeTransactions".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let mut out: Vec<Option<TransactionState>> = vec![None; ids.len()];
        let mut pending: Vec<usize> = (0..ids.len()).collect();
        loop {
            let by_node = self.txn_coord_nodes(&ids, &pending).await?;
            let mut nodes: Vec<i32> = by_node.keys().copied().collect();
            nodes.sort_unstable();
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.get(&node).cloned().unwrap_or_default();
                match self
                    .describe_transactions_on_node(node, &ids, &idxs, version, timeout)
                    .await
                {
                    Ok(done) => {
                        for (i, t) in done {
                            if error::coordinator_retriable(t.error_code) {
                                self.invalidate_txn_coord_idxs(&ids, &[i], node);
                                still.push(i);
                            } else if let Some(slot) = out.get_mut(i) {
                                *slot = Some(t);
                            }
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        self.invalidate_txn_coord_idxs(&ids, &idxs, node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
        }
        out.into_iter()
            .zip(ids)
            .map(|(t, id)| {
                t.ok_or_else(|| Error::protocol(format!("DescribeTransactions missing {id}")))
            })
            .collect()
    }

    /// List transactional.id state (ListTransactions api 66).
    ///
    /// Lands on the transaction coordinator (`FindCoordinator`
    /// `key_type=1`). `COORDINATOR_LOAD_IN_PROGRESS` /
    /// `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. This is not `NOT_CONTROLLER` (41). Top-level
    /// `error_code` (bytes 4–5), not a first-result field.
    /// Duration is unfiltered (`-1`). See
    /// [`Self::list_transactions_with_duration`] for Java
    /// `ListTransactionsOptions.filterOnDuration`. Optional at
    /// [`Self::new`] (Kafka 2.5+ / KIP-573); a broker that omits api 66
    /// returns [`Error::Unsupported`]. ListTransactions has no TimeoutMs;
    /// the RPC deadline is [`AdminConfig::request_timeout`]. For a
    /// one-shot deadline, use [`Self::list_transactions_timeout`].
    pub async fn list_transactions(
        &mut self,
        state_filters: &[&str],
        producer_id_filters: &[i64],
    ) -> Result<Vec<TransactionListing>> {
        let timeout = self.cfg.request_timeout;
        self.list_transactions_timeout(state_filters, producer_id_filters, timeout)
            .await
    }

    /// [`Self::list_transactions`] with a one-shot RPC deadline (Java
    /// `ListTransactionsOptions.timeoutMs`).
    ///
    /// ListTransactions has no TimeoutMs; `timeout` is the RPC deadline
    /// and the coordinator retry budget. DurationFilter stays `-1`
    /// (unfiltered). See [`Self::list_transactions_with_duration_timeout`]
    /// for `filterOnDuration` plus deadline.
    pub async fn list_transactions_timeout(
        &mut self,
        state_filters: &[&str],
        producer_id_filters: &[i64],
        timeout: Duration,
    ) -> Result<Vec<TransactionListing>> {
        self.list_transactions_with_duration_timeout(
            state_filters,
            producer_id_filters,
            -1,
            timeout,
        )
        .await
    }

    /// List every transaction (Java `Admin.listTransactions()`).
    ///
    /// Same wire as [`Self::list_transactions`] with empty state and
    /// producer-id filters and DurationFilter `-1` (Java
    /// `ListTransactionsOptions` default).
    pub async fn list_transactions_all(&mut self) -> Result<Vec<TransactionListing>> {
        self.list_transactions(&[], &[]).await
    }

    /// [`Self::list_transactions_all`] with a one-shot RPC deadline (Java
    /// `ListTransactionsOptions.timeoutMs`).
    pub async fn list_transactions_all_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<TransactionListing>> {
        self.list_transactions_timeout(&[], &[], timeout).await
    }

    /// ListTransactions with a duration filter (Java `listTransactions`
    /// plus `ListTransactionsOptions.filterOnDuration`).
    ///
    /// `duration_ms < 0` means no duration filter (Java default `-1`).
    /// v1 sends `DurationFilter` INT64 (KIP-994). v0 omits the field
    /// even when `duration_ms` is set. Kafka 4.0 `validVersions` is
    /// `0-1`. This crate speaks 0–1. v2 TransactionalIdPattern is not
    /// spoken. ListTransactions has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::list_transactions_with_duration_timeout`].
    pub async fn list_transactions_with_duration(
        &mut self,
        state_filters: &[&str],
        producer_id_filters: &[i64],
        duration_ms: i64,
    ) -> Result<Vec<TransactionListing>> {
        let timeout = self.cfg.request_timeout;
        self.list_transactions_with_duration_timeout(
            state_filters,
            producer_id_filters,
            duration_ms,
            timeout,
        )
        .await
    }

    /// [`Self::list_transactions_with_duration`] with a one-shot RPC
    /// deadline (Java `ListTransactionsOptions.filterOnDuration` and
    /// `timeoutMs`).
    ///
    /// ListTransactions has no TimeoutMs; `timeout` is the RPC deadline
    /// and the coordinator retry budget. `duration_ms` is DurationFilter
    /// (v1), not TimeoutMs.
    pub async fn list_transactions_with_duration_timeout(
        &mut self,
        state_filters: &[&str],
        producer_id_filters: &[i64],
        duration_ms: i64,
        timeout: Duration,
    ) -> Result<Vec<TransactionListing>> {
        let states: Vec<String> = state_filters.iter().map(|s| (*s).to_string()).collect();
        let pids = producer_id_filters.to_vec();
        // ListTransactions has no transactional.id; FindCoordinator still
        // needs a key. Empty string is the no-id lookup used here.
        const COORD_KEY: &str = "";
        let version = self
            .list_transactions_version
            .ok_or_else(|| Error::Unsupported("broker does not support ListTransactions".into()))?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let stale = self.txn_coord.as_ref().is_none_or(|(k, _)| k != COORD_KEY);
            if stale {
                let node = self.discover_txn_coord(COORD_KEY).await?;
                self.txn_coord = Some((COORD_KEY.to_string(), node));
            }
            let node = self
                .txn_coord
                .as_ref()
                .map(|(_, n)| *n)
                .ok_or_else(|| Error::protocol("missing transaction coordinator"))?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing list_transactions conn"))?;
                conn.roundtrip(
                    LIST_TRANSACTIONS,
                    version,
                    |buf| {
                        encode_list_transactions_request(buf, version, &states, &pids, duration_ms)
                    },
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.txn_coord = None;
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_list_transactions_response(&mut body.clone(), version)?;
            if error::coordinator_retriable(resp.error_code) {
                // 14/15/16: FindCoordinator, then the new txn coordinator.
                self.txn_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "ListTransactions"));
            }
            return Ok(resp.transaction_states);
        }
    }

    /// Describe ACL bindings (`DescribeAcls`) matching `resource_type`.
    ///
    /// Negotiates v0–v3 (v0–v1 classic; v2+ flexible). v1+ sends
    /// PatternTypeFilter ANY. Operation and Permission are ANY (Java
    /// `AccessControlEntryFilter.ANY`). Kafka 4.0 `validVersions` is
    /// `1-3`. v4+ is not spoken.
    ///
    /// `resource_type` is [`crate::AclResourceType`] or a protocol `i8`
    /// (`ACL_RESOURCE_TOPIC`, …). For principal / host / name / operation
    /// filters, use [`Self::describe_acls_with`]. DescribeAcls has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use [`Self::describe_acls_timeout`].
    pub async fn describe_acls(&mut self, resource_type: impl Into<i8>) -> Result<Vec<AclBinding>> {
        self.describe_acls_with(&AclBindingFilter::resource_type(resource_type))
            .await
    }

    /// [`Self::describe_acls`] with a one-shot RPC deadline (Java
    /// `DescribeAclsOptions.timeoutMs`).
    pub async fn describe_acls_timeout(
        &mut self,
        resource_type: impl Into<i8>,
        timeout: Duration,
    ) -> Result<Vec<AclBinding>> {
        self.describe_acls_with_timeout(&AclBindingFilter::resource_type(resource_type), timeout)
            .await
    }

    /// [`Self::describe_acls`] with a Java `AclBindingFilter`.
    ///
    /// DescribeAcls has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_acls_with_timeout`].
    pub async fn describe_acls_with(
        &mut self,
        filter: &AclBindingFilter,
    ) -> Result<Vec<AclBinding>> {
        let timeout = self.cfg.request_timeout;
        self.describe_acls_with_timeout(filter, timeout).await
    }

    /// [`Self::describe_acls_with`] with Java `DescribeAclsOptions.timeoutMs`.
    ///
    /// DescribeAcls has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_acls_with_timeout(
        &mut self,
        filter: &AclBindingFilter,
        timeout: Duration,
    ) -> Result<Vec<AclBinding>> {
        let version = self.describe_acls_version;
        let body = self
            .roundtrip_bootstrap(
                DESCRIBE_ACLS,
                version,
                |buf| encode_describe_acls_request(buf, version, filter),
                timeout,
            )
            .await?;
        decode_describe_acls_response(&mut body.clone(), version)
    }

    /// Describe every ACL (Java `describeAcls(AclBindingFilter.ANY)`).
    ///
    /// Same wire as [`Self::describe_acls_with`] with
    /// [`AclBindingFilter::any`]. DescribeAcls has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::describe_acls_any_timeout`].
    pub async fn describe_acls_any(&mut self) -> Result<Vec<AclBinding>> {
        self.describe_acls_with(&AclBindingFilter::any()).await
    }

    /// [`Self::describe_acls_any`] with a one-shot RPC deadline (Java
    /// `DescribeAclsOptions.timeoutMs`).
    ///
    /// DescribeAcls has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_acls_any_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<AclBinding>> {
        self.describe_acls_with_timeout(&AclBindingFilter::any(), timeout)
            .await
    }

    /// Replace configs (`AlterConfigs`, legacy api 33).
    ///
    /// Negotiates v0–v2 (v0–v1 classic; v2 flexible). v1 response adds
    /// ThrottleTimeMs (KIP-219). Kafka 4.0 `validVersions` is `0-2`.
    /// v3+ is not spoken.
    ///
    /// Prefer [`Self::incremental_alter_configs`] on modern brokers.
    /// Returns the first resource's error code. For several resources in
    /// one RPC (Java `alterConfigs(Map)`), use [`Self::alter_configs_for`].
    /// AlterConfigs has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::alter_configs_timeout`].
    pub async fn alter_configs(
        &mut self,
        resource: &ConfigResource,
        configs: &[(String, Option<String>)],
        validate_only: bool,
    ) -> Result<i16> {
        let results = self
            .alter_configs_for(
                &[ConfigReplacement::new(
                    resource.clone(),
                    configs.iter().cloned(),
                )],
                validate_only,
            )
            .await?;
        Ok(results.first().map(|r| r.error_code).unwrap_or(0))
    }

    /// [`Self::alter_configs`] with a one-shot RPC deadline (Java
    /// `AlterConfigsOptions.timeoutMs`).
    ///
    /// AlterConfigs has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn alter_configs_timeout(
        &mut self,
        resource: &ConfigResource,
        configs: &[(String, Option<String>)],
        timeout: Duration,
        validate_only: bool,
    ) -> Result<i16> {
        let results = self
            .alter_configs_for_timeout(
                &[ConfigReplacement::new(
                    resource.clone(),
                    configs.iter().cloned(),
                )],
                timeout,
                validate_only,
            )
            .await?;
        Ok(results.first().map(|r| r.error_code).unwrap_or(0))
    }

    /// [`Self::alter_configs`] with a Java `Config` (`alterConfigs(Map)` one
    /// resource).
    ///
    /// AlterConfigs has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::alter_configs_with_timeout`].
    pub async fn alter_configs_with(
        &mut self,
        resource: &ConfigResource,
        config: &Config,
        validate_only: bool,
    ) -> Result<i16> {
        let results = self
            .alter_configs_for(
                &[ConfigReplacement::from_config(resource.clone(), config)],
                validate_only,
            )
            .await?;
        Ok(results.first().map(|r| r.error_code).unwrap_or(0))
    }

    /// [`Self::alter_configs_with`] with a one-shot RPC deadline (Java
    /// `AlterConfigsOptions.timeoutMs`).
    ///
    /// AlterConfigs has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn alter_configs_with_timeout(
        &mut self,
        resource: &ConfigResource,
        config: &Config,
        timeout: Duration,
        validate_only: bool,
    ) -> Result<i16> {
        let results = self
            .alter_configs_for_timeout(
                &[ConfigReplacement::from_config(resource.clone(), config)],
                timeout,
                validate_only,
            )
            .await?;
        Ok(results.first().map(|r| r.error_code).unwrap_or(0))
    }

    /// [`Self::alter_configs`] for several resources (Java `alterConfigs(Map)`;
    /// AlterConfigs Resources of N).
    ///
    /// Empty `updates` is a no-op.
    pub async fn alter_configs_for(
        &mut self,
        updates: &[ConfigReplacement],
        validate_only: bool,
    ) -> Result<Vec<AlterConfigsResourceResult>> {
        let timeout = self.cfg.request_timeout;
        self.alter_configs_for_timeout(updates, timeout, validate_only)
            .await
    }

    /// [`Self::alter_configs_for`] with Java `AlterConfigsOptions.timeoutMs`.
    ///
    /// Empty `updates` is a no-op. AlterConfigs has no TimeoutMs;
    /// `timeout` is the RPC deadline.
    pub async fn alter_configs_for_timeout(
        &mut self,
        updates: &[ConfigReplacement],
        timeout: Duration,
        validate_only: bool,
    ) -> Result<Vec<AlterConfigsResourceResult>> {
        if updates.is_empty() {
            return Ok(Vec::new());
        }
        let resources: Vec<AlterConfigsResource> = updates
            .iter()
            .map(|u| AlterConfigsResource {
                resource_type: u.resource.resource_type,
                name: u.resource.name.clone(),
                configs: u
                    .configs
                    .iter()
                    .map(|(n, v)| TopicConfig {
                        name: n.clone(),
                        value: v.clone(),
                    })
                    .collect(),
            })
            .collect();
        let version = self.legacy_alter_version;
        let body = self
            .roundtrip_bootstrap(
                ALTER_CONFIGS,
                version,
                |buf| {
                    encode_alter_configs_resources_request(buf, version, &resources, validate_only)
                },
                timeout,
            )
            .await?;
        decode_alter_configs_resource_results(&mut body.clone(), version)
    }

    async fn fetch_metadata(&mut self, topics: Option<&[String]>) -> Result<MetadataResponse> {
        self.fetch_metadata_with(topics, false).await
    }

    async fn fetch_metadata_with(
        &mut self,
        topics: Option<&[String]>,
        include_topic_authorized_operations: bool,
    ) -> Result<MetadataResponse> {
        let owned = topics.map(|names| {
            names
                .iter()
                .map(|name| MetadataRequestTopic::by_name(name.clone()))
                .collect::<Vec<_>>()
        });
        let timeout = self.cfg.request_timeout;
        self.fetch_metadata_request_with(
            owned.as_deref(),
            include_topic_authorized_operations,
            timeout,
        )
        .await
    }

    async fn fetch_metadata_request_with(
        &mut self,
        topics: Option<&[MetadataRequestTopic]>,
        include_topic_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<MetadataResponse> {
        let version = self.metadata_version;
        let body = self
            .roundtrip_bootstrap(
                METADATA,
                version,
                |buf| {
                    encode_metadata_request_topics(
                        buf,
                        version,
                        topics,
                        false,
                        include_topic_authorized_operations,
                    )
                },
                timeout,
            )
            .await?;
        let md = decode_metadata_response(&mut body.clone(), version)?;
        md.check()?;
        self.cluster.apply(&md);
        Ok(md)
    }

    async fn refresh_metadata(&mut self, topics: Option<&[String]>) -> Result<()> {
        self.fetch_metadata(topics).await.map(|_| ())
    }

    /// Wait [`AdminConfig::retry_backoff`] (exponential) or [`Error::Timeout`].
    async fn wait_retry(&self, attempt: &mut u32, deadline: Instant) -> Result<()> {
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        crate::config::sleep_retry_backoff(
            self.cfg.retry_backoff,
            self.cfg.retry_backoff_max,
            *attempt,
            deadline,
        )
        .await;
        *attempt = attempt.saturating_add(1);
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        Ok(())
    }

    async fn ensure_bootstrap(&mut self) -> Result<()> {
        if !self.conn.idle_expired(self.cfg.connections_max_idle) {
            return Ok(());
        }
        let addr = self.conn.addr().to_string();
        self.conn = self.open_node_conn(&addr).await?;
        Ok(())
    }

    /// Reconnect the bootstrap socket if it has been idle, then round-trip.
    async fn roundtrip_bootstrap(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut) -> Result<()>,
        timeout: Duration,
    ) -> Result<Bytes> {
        self.ensure_bootstrap().await?;
        self.conn
            .roundtrip(api_key, api_version, encode_body, timeout)
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
            let fails = self.reconnect_fails.get(&node).copied().unwrap_or(0);
            crate::config::sleep_reconnect_backoff(
                self.cfg.reconnect_backoff,
                self.cfg.reconnect_backoff_max,
                fails,
            )
            .await;
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

    async fn open_node_conn(&self, addr: &str) -> Result<BrokerConn> {
        let mut conn = BrokerConn::connect_tls(
            addr,
            &self.cfg.client_id,
            self.cfg.connect_timeout,
            self.cfg.tls.as_ref(),
        )
        .await?;
        conn.set_stats(Arc::clone(&self.stats));
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

    /// Delete records before `offset` (`DeleteRecords`).
    ///
    /// Negotiates v0–v2 (v0–v1 classic; v2 flexible). v1 response adds
    /// ThrottleTimeMs (KIP-219). Kafka 4.0 `validVersions` is `0-2`.
    /// v3+ is not spoken.
    ///
    /// `offset` is [`RecordsToDelete`] or INT64 (Java
    /// `RecordsToDelete.beforeOffset(long)`). Lands on the Metadata
    /// partition leader. `NOT_LEADER_OR_FOLLOWER` (6) and other
    /// retriable codes refresh Metadata and retry on the new leader.
    /// Returns [`DeletedRecords`] (`lowWatermark` plus per-partition
    /// ErrorCode). `timeout_ms` is
    /// DeleteRecords TimeoutMs. The RPC deadline is
    /// [`AdminConfig::request_timeout`]. For several partitions, use
    /// [`Self::delete_records_for`]. For a one-shot timeout that drives
    /// both the RPC deadline and TimeoutMs, use
    /// [`Self::delete_records_timeout`].
    pub async fn delete_records(
        &mut self,
        partition: impl Into<crate::TopicPartition>,
        offset: impl Into<i64>,
        timeout_ms: i32,
    ) -> Result<DeletedRecords> {
        let timeout = self.cfg.request_timeout;
        self.delete_records_one(partition.into(), offset.into(), timeout_ms, timeout)
            .await
            .map(DeletedRecords::from)
    }

    /// [`Self::delete_records`] with a one-shot timeout (Java
    /// `DeleteRecordsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and DeleteRecords TimeoutMs.
    pub async fn delete_records_timeout(
        &mut self,
        partition: impl Into<crate::TopicPartition>,
        offset: impl Into<i64>,
        timeout: Duration,
    ) -> Result<DeletedRecords> {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        self.delete_records_one(partition.into(), offset.into(), timeout_ms, timeout)
            .await
            .map(DeletedRecords::from)
    }

    /// Delete records on several partitions (Java `deleteRecords(Map)`).
    ///
    /// Each item is a [`crate::TopicPartition`] and an offset
    /// ([`RecordsToDelete`] or INT64). One DeleteRecords RPC per
    /// Metadata partition leader. Empty input is a no-op. Returns
    /// [`DeletedRecords`] per partition. TimeoutMs and
    /// the RPC deadline are [`AdminConfig::request_timeout`]. For a
    /// one-shot timeout, use [`Self::delete_records_for_timeout`].
    pub async fn delete_records_for<Tp, Off>(
        &mut self,
        records: impl IntoIterator<Item = (Tp, Off)>,
    ) -> Result<Vec<(crate::TopicPartition, DeletedRecords)>>
    where
        Tp: Into<crate::TopicPartition>,
        Off: Into<i64>,
    {
        let timeout = self.cfg.request_timeout;
        self.delete_records_for_timeout(records, timeout).await
    }

    /// [`Self::delete_records_for`] with a one-shot timeout (Java
    /// `deleteRecords` plus `DeleteRecordsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and DeleteRecords TimeoutMs.
    pub async fn delete_records_for_timeout<Tp, Off>(
        &mut self,
        records: impl IntoIterator<Item = (Tp, Off)>,
        timeout: Duration,
    ) -> Result<Vec<(crate::TopicPartition, DeletedRecords)>>
    where
        Tp: Into<crate::TopicPartition>,
        Off: Into<i64>,
    {
        let timeout_ms = crate::consumer::duration_millis_i32(timeout);
        let raw = self
            .delete_records_for_with(records, timeout_ms, timeout)
            .await?;
        Ok(raw
            .into_iter()
            .map(|(tp, low, err)| (tp, DeletedRecords::with_error_code(low, err)))
            .collect())
    }

    async fn delete_records_one(
        &mut self,
        tp: crate::TopicPartition,
        offset: i64,
        timeout_ms: i32,
        timeout: Duration,
    ) -> Result<(i64, i16)> {
        let mut out = self
            .delete_records_for_with([(tp, offset)], timeout_ms, timeout)
            .await?;
        match out.pop() {
            Some((_, low, err)) => Ok((low, err)),
            None => Err(Error::protocol("missing DeleteRecords result")),
        }
    }

    async fn delete_records_for_with<Tp, Off>(
        &mut self,
        records: impl IntoIterator<Item = (Tp, Off)>,
        timeout_ms: i32,
        timeout: Duration,
    ) -> Result<Vec<(crate::TopicPartition, i64, i16)>>
    where
        Tp: Into<crate::TopicPartition>,
        Off: Into<i64>,
    {
        let records: Vec<(crate::TopicPartition, i64)> = records
            .into_iter()
            .map(|(tp, off)| (tp.into(), off.into()))
            .collect();
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.delete_records_version;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let mut out: Vec<Option<(i64, i16)>> = vec![None; records.len()];
        let mut pending: Vec<usize> = (0..records.len()).collect();
        loop {
            if pending.is_empty() {
                break;
            }
            let mut need: Vec<String> = Vec::new();
            for &i in &pending {
                let Some((tp, _)) = records.get(i) else {
                    continue;
                };
                if self.cluster.leader(&tp.topic, tp.partition).is_err()
                    && !need.iter().any(|t| t == &tp.topic)
                {
                    need.push(tp.topic.clone());
                }
            }
            if !need.is_empty() {
                self.refresh_metadata(Some(&need)).await?;
            }
            let mut by_node: HashMap<i32, Vec<usize>> = HashMap::new();
            let mut nodes: Vec<i32> = Vec::new();
            for &i in &pending {
                let (tp, _) = records
                    .get(i)
                    .ok_or_else(|| Error::protocol("missing DeleteRecords query"))?;
                let (node, _) = self.cluster.leader(&tp.topic, tp.partition)?;
                match by_node.entry(node) {
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        nodes.push(node);
                        let _ = slot.insert(vec![i]);
                    }
                    std::collections::hash_map::Entry::Occupied(mut slot) => {
                        slot.get_mut().push(i);
                    }
                }
            }
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.remove(&node).unwrap_or_default();
                match self
                    .delete_records_on_node(node, version, timeout_ms, &records, &idxs, timeout)
                    .await
                {
                    Ok((done, retry)) => {
                        for (i, low, err) in done {
                            if let Some(slot) = out.get_mut(i) {
                                *slot = Some((low, err));
                            }
                        }
                        still.extend(retry);
                    }
                    Err(e) if e.is_retriable() => {
                        let _ = self.conns.remove(&node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
            for &i in &pending {
                if let Some((tp, _)) = records.get(i) {
                    self.cluster.invalidate_topic(&tp.topic);
                }
            }
            let topics: Vec<String> = {
                let mut t = Vec::new();
                for &i in &pending {
                    if let Some((tp, _)) = records.get(i) {
                        if !t.iter().any(|n| n == &tp.topic) {
                            t.push(tp.topic.clone());
                        }
                    }
                }
                t
            };
            if !topics.is_empty() {
                self.refresh_metadata(Some(&topics)).await?;
            }
        }
        out.into_iter()
            .zip(records)
            .map(|(got, (tp, _))| {
                got.map(|(low, err)| (tp, low, err))
                    .ok_or_else(|| Error::protocol("DeleteRecords missing result"))
            })
            .collect()
    }

    async fn delete_records_on_node(
        &mut self,
        node: i32,
        version: i16,
        timeout_ms: i32,
        records: &[(crate::TopicPartition, i64)],
        idxs: &[usize],
        timeout: Duration,
    ) -> Result<(Vec<(usize, i64, i16)>, Vec<usize>)> {
        let topics = delete_records_topics(records, idxs);
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing delete_records conn"))?;
            conn.roundtrip(
                DELETE_RECORDS,
                version,
                |buf| encode_delete_records_topics_request(buf, version, &topics, timeout_ms),
                timeout,
            )
            .await
        }?;
        let resp = decode_delete_records_topics_response(&mut body.clone(), version)?;
        let mut by_key: HashMap<(String, i32), VecDeque<(i64, i16)>> = HashMap::new();
        for t in resp {
            for p in t.partitions {
                by_key
                    .entry((t.topic.clone(), p.partition))
                    .or_default()
                    .push_back((p.low_watermark, p.error_code));
            }
        }
        let mut done = Vec::new();
        let mut retry = Vec::new();
        for &i in idxs {
            let (tp, _) = records
                .get(i)
                .ok_or_else(|| Error::protocol("missing DeleteRecords query"))?;
            let (low, err) = by_key
                .get_mut(&(tp.topic.clone(), tp.partition))
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| {
                    Error::protocol(format!(
                        "DeleteRecords missing {}-{}",
                        tp.topic, tp.partition
                    ))
                })?;
            if err == 0 {
                done.push((i, low, err));
                continue;
            }
            let e = Error::broker(err, format!("{}-{}", tp.topic, tp.partition));
            if e.is_retriable() {
                self.cluster.invalidate_topic(&tp.topic);
                let _ = self.conns.remove(&node);
                retry.push(i);
            } else {
                done.push((i, low, err));
            }
        }
        Ok((done, retry))
    }

    /// ListOffsets for these partitions (Java `Admin.listOffsets`).
    ///
    /// Isolation is read-uncommitted. See
    /// [`Self::list_offsets_with_isolation`] for Java
    /// `ListOffsetsOptions.isolationLevel`. Waits up to
    /// [`AdminConfig::request_timeout`]. For a one-shot timeout, use
    /// [`Self::list_offsets_timeout`]. Each timestamp is
    /// [`crate::OffsetSpec`] or INT64 (see
    /// [`Self::list_offsets_with_isolation`]).
    pub async fn list_offsets<Tp, Ts>(
        &mut self,
        queries: impl IntoIterator<Item = (Tp, Ts)>,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndTimestamp)>>
    where
        Tp: Into<crate::TopicPartition>,
        Ts: Into<i64>,
    {
        let timeout = self.cfg.request_timeout;
        self.list_offsets_timeout(queries, timeout).await
    }

    /// [`Self::list_offsets`] with a one-shot timeout (Java `listOffsets`
    /// plus `ListOffsetsOptions.timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and ListOffsets v10 `TimeoutMs`.
    pub async fn list_offsets_timeout<Tp, Ts>(
        &mut self,
        queries: impl IntoIterator<Item = (Tp, Ts)>,
        timeout: Duration,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndTimestamp)>>
    where
        Tp: Into<crate::TopicPartition>,
        Ts: Into<i64>,
    {
        self.list_offsets_with_isolation_timeout(
            queries,
            crate::IsolationLevel::ReadUncommitted,
            timeout,
        )
        .await
    }

    /// ListOffsets with isolation (Java `listOffsets` +
    /// `ListOffsetsOptions.isolationLevel`).
    ///
    /// Each item is a [`crate::TopicPartition`] and a timestamp
    /// ([`crate::OffsetSpec`] or INT64): [`crate::OffsetSpec::earliest`]
    /// / [`crate::EARLIEST_TIMESTAMP`] (`-2`), [`crate::OffsetSpec::latest`]
    /// / [`crate::LATEST_TIMESTAMP`] (`-1`), [`crate::OffsetSpec::max_timestamp`]
    /// / [`crate::MAX_TIMESTAMP`] (`-3`),
    /// [`crate::OffsetSpec::earliest_local`] /
    /// [`crate::EARLIEST_LOCAL_TIMESTAMP`] (`-4`),
    /// [`crate::OffsetSpec::latest_tiered`] /
    /// [`crate::LATEST_TIERED_TIMESTAMP`] (`-5`), or milliseconds since
    /// the Unix epoch. One ListOffsets
    /// RPC per Metadata partition leader (duplicate partitions keep
    /// separate timestamps). `NOT_LEADER_OR_FOLLOWER` refreshes
    /// Metadata and retries.
    /// [`crate::OffsetAndTimestamp::leader_epoch`] is ListOffsets v4+.
    /// v1–v5 are classic; v6–v10 are flexible. v10 `TimeoutMs` is
    /// [`AdminConfig::request_timeout`]. For a one-shot timeout, use
    /// [`Self::list_offsets_with_isolation_timeout`]. Empty input is a no-op.
    pub async fn list_offsets_with_isolation<Tp, Ts>(
        &mut self,
        queries: impl IntoIterator<Item = (Tp, Ts)>,
        isolation: crate::IsolationLevel,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndTimestamp)>>
    where
        Tp: Into<crate::TopicPartition>,
        Ts: Into<i64>,
    {
        let timeout = self.cfg.request_timeout;
        self.list_offsets_with_isolation_timeout(queries, isolation, timeout)
            .await
    }

    /// [`Self::list_offsets_with_isolation`] with a one-shot timeout (Java
    /// `listOffsets` plus `ListOffsetsOptions.isolationLevel` and
    /// `timeoutMs`).
    ///
    /// `timeout` is the RPC deadline and ListOffsets v10 `TimeoutMs`.
    pub async fn list_offsets_with_isolation_timeout<Tp, Ts>(
        &mut self,
        queries: impl IntoIterator<Item = (Tp, Ts)>,
        isolation: crate::IsolationLevel,
        timeout: Duration,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndTimestamp)>>
    where
        Tp: Into<crate::TopicPartition>,
        Ts: Into<i64>,
    {
        let queries: Vec<(crate::TopicPartition, i64)> = queries
            .into_iter()
            .map(|(tp, ts)| (tp.into(), ts.into()))
            .collect();
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let version = self
            .versions
            .get(&LIST_OFFSETS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 10))
            .ok_or_else(|| Error::Unsupported("broker does not support ListOffsets".into()))?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let isolation = isolation.as_i8();
        let mut out: Vec<Option<crate::OffsetAndTimestamp>> = vec![None; queries.len()];
        let mut pending: Vec<usize> = (0..queries.len()).collect();
        loop {
            if pending.is_empty() {
                break;
            }
            let mut need: Vec<String> = Vec::new();
            for &i in &pending {
                let Some((tp, _)) = queries.get(i) else {
                    continue;
                };
                if self.cluster.leader(&tp.topic, tp.partition).is_err()
                    && !need.iter().any(|t| t == &tp.topic)
                {
                    need.push(tp.topic.clone());
                }
            }
            if !need.is_empty() {
                self.refresh_metadata(Some(&need)).await?;
            }
            let mut by_node: HashMap<i32, Vec<usize>> = HashMap::new();
            let mut nodes: Vec<i32> = Vec::new();
            for &i in &pending {
                let (tp, _) = queries
                    .get(i)
                    .ok_or_else(|| Error::protocol("missing ListOffsets query"))?;
                let (node, _) = self.cluster.leader(&tp.topic, tp.partition)?;
                match by_node.entry(node) {
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        nodes.push(node);
                        let _ = slot.insert(vec![i]);
                    }
                    std::collections::hash_map::Entry::Occupied(mut slot) => {
                        slot.get_mut().push(i);
                    }
                }
            }
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.remove(&node).unwrap_or_default();
                match self
                    .list_offsets_on_node(node, version, isolation, &queries, &idxs, timeout)
                    .await
                {
                    Ok((done, retry)) => {
                        for (i, ot) in done {
                            if let Some(slot) = out.get_mut(i) {
                                *slot = Some(ot);
                            }
                        }
                        still.extend(retry);
                    }
                    Err(e) if e.is_retriable() => {
                        let _ = self.conns.remove(&node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
            for &i in &pending {
                if let Some((tp, _)) = queries.get(i) {
                    self.cluster.invalidate_topic(&tp.topic);
                }
            }
            let topics: Vec<String> = {
                let mut t = Vec::new();
                for &i in &pending {
                    if let Some((tp, _)) = queries.get(i) {
                        if !t.iter().any(|n| n == &tp.topic) {
                            t.push(tp.topic.clone());
                        }
                    }
                }
                t
            };
            if !topics.is_empty() {
                self.refresh_metadata(Some(&topics)).await?;
            }
        }
        out.into_iter()
            .zip(queries)
            .map(|(ot, (tp, _))| {
                ot.map(|ot| (tp, ot))
                    .ok_or_else(|| Error::protocol("ListOffsets missing result"))
            })
            .collect()
    }

    async fn list_offsets_on_node(
        &mut self,
        node: i32,
        version: i16,
        isolation: i8,
        queries: &[(crate::TopicPartition, i64)],
        idxs: &[usize],
        timeout: Duration,
    ) -> Result<(Vec<(usize, crate::OffsetAndTimestamp)>, Vec<usize>)> {
        let topics = list_offset_topic_requests(queries, idxs, &self.cluster);
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing list_offsets conn"))?;
            conn.roundtrip(
                LIST_OFFSETS,
                version,
                |buf| {
                    encode_list_offsets_topics_request(
                        buf,
                        version,
                        isolation,
                        &topics,
                        crate::consumer::duration_millis_i32(timeout),
                    )
                },
                timeout,
            )
            .await
        }?;
        let resp = decode_list_offsets_topics_response(&mut body.clone(), version)?;
        let mut by_key: HashMap<(String, i32), VecDeque<ListOffsetsResponsePartition>> =
            HashMap::new();
        for t in resp {
            for p in t.partitions {
                by_key
                    .entry((t.name.clone(), p.partition_index))
                    .or_default()
                    .push_back(p);
            }
        }
        let mut done = Vec::new();
        let mut retry = Vec::new();
        for &i in idxs {
            let (tp, _) = queries
                .get(i)
                .ok_or_else(|| Error::protocol("missing ListOffsets query"))?;
            let part = by_key
                .get_mut(&(tp.topic.clone(), tp.partition))
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| {
                    Error::protocol(format!("ListOffsets missing {}-{}", tp.topic, tp.partition))
                })?;
            if part.error_code == 0 {
                done.push((
                    i,
                    crate::OffsetAndTimestamp::new(part.offset, part.timestamp)
                        .with_leader_epoch(part.leader_epoch),
                ));
                continue;
            }
            let e = Error::broker(part.error_code, format!("{}-{}", tp.topic, tp.partition));
            if matches!(
                part.error_code,
                error::FENCED_LEADER_EPOCH | error::UNKNOWN_LEADER_EPOCH
            ) || e.is_retriable()
            {
                self.cluster.invalidate_topic(&tp.topic);
                let _ = self.conns.remove(&node);
                retry.push(i);
            } else {
                return Err(e);
            }
        }
        Ok((done, retry))
    }

    /// Describe active producers on a partition (DescribeProducers api 61,
    /// KIP-360).
    ///
    /// Lands on the Metadata partition leader (same class as
    /// DeleteRecords / ListOffsets / OffsetForLeaderEpoch). Official
    /// Apache JSON listeners are `broker` only. This is not a
    /// controller hop and not a transaction-coordinator hop: there is
    /// no Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41)
    /// retry, and no FindCoordinator / `NOT_COORDINATOR` (16) retry.
    /// `NOT_LEADER_OR_FOLLOWER` (6) and other `is_retriable` broker
    /// codes refresh Metadata and retry on the new leader. ErrorCode
    /// is per-partition (bytes 12–13 on leftover-empty fixture topic
    /// `"t"` partition `0`), not top-level after throttle.
    /// For several partitions in one RPC per leader (Java
    /// `describeProducers(Collection)`), use [`Self::describe_producers_for`].
    /// Optional at [`Self::new`] (Kafka 2.8+ / KIP-664); a broker that
    /// omits api 61 returns [`Error::Unsupported`]. DescribeProducers
    /// has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_producers_timeout`].
    pub async fn describe_producers(
        &mut self,
        partition: impl Into<crate::TopicPartition>,
    ) -> Result<DescribeProducersPartition> {
        let timeout = self.cfg.request_timeout;
        self.describe_producers_timeout(partition, timeout).await
    }

    /// [`Self::describe_producers`] with a one-shot RPC deadline (Java
    /// `DescribeProducersOptions.timeoutMs`).
    ///
    /// DescribeProducers has no TimeoutMs; `timeout` is the RPC deadline
    /// and the `NOT_LEADER_OR_FOLLOWER` retry budget.
    pub async fn describe_producers_timeout(
        &mut self,
        partition: impl Into<crate::TopicPartition>,
        timeout: Duration,
    ) -> Result<DescribeProducersPartition> {
        let topics = self
            .describe_producers_for_timeout([partition.into()], timeout)
            .await?;
        topics
            .into_iter()
            .next()
            .and_then(|t| t.partitions.into_iter().next())
            .ok_or_else(|| Error::protocol("empty DescribeProducers response"))
    }

    /// [`Self::describe_producers`] for several partitions (Java
    /// `describeProducers(Collection)`; DescribeProducers Topics of N).
    ///
    /// Groups by Metadata leader and sends one RPC per leader. Empty
    /// `partitions` is a no-op. To pin every partition to one broker
    /// (Java `DescribeProducersOptions.brokerId`), use
    /// [`Self::describe_producers_for_on_broker`]. DescribeProducers has
    /// no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_producers_for_timeout`].
    pub async fn describe_producers_for(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
    ) -> Result<Vec<DescribeProducersTopic>> {
        let timeout = self.cfg.request_timeout;
        self.describe_producers_for_with(partitions, timeout, None)
            .await
    }

    /// [`Self::describe_producers_for`] with a one-shot RPC deadline (Java
    /// `describeProducers` plus `DescribeProducersOptions.timeoutMs`).
    ///
    /// DescribeProducers has no TimeoutMs; `timeout` is the RPC deadline
    /// and the `NOT_LEADER_OR_FOLLOWER` retry budget.
    pub async fn describe_producers_for_timeout(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<DescribeProducersTopic>> {
        self.describe_producers_for_with(partitions, timeout, None)
            .await
    }

    /// [`Self::describe_producers_for`] pinned to one broker (Java
    /// `DescribeProducersOptions.brokerId`).
    ///
    /// Sends one DescribeProducers RPC to `broker_id` for every
    /// partition. `NOT_LEADER_OR_FOLLOWER` (6) is returned on that
    /// partition and is not retried on the Metadata leader.
    pub async fn describe_producers_for_on_broker(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
        broker_id: i32,
    ) -> Result<Vec<DescribeProducersTopic>> {
        let timeout = self.cfg.request_timeout;
        self.describe_producers_for_with(partitions, timeout, Some(broker_id))
            .await
    }

    /// [`Self::describe_producers_for_on_broker`] with a one-shot RPC
    /// deadline (Java `DescribeProducersOptions.brokerId` + `timeoutMs`).
    ///
    /// DescribeProducers has no TimeoutMs; `timeout` is the RPC deadline.
    /// `NOT_LEADER_OR_FOLLOWER` is not retried onto another broker.
    pub async fn describe_producers_for_on_broker_timeout(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
        broker_id: i32,
        timeout: Duration,
    ) -> Result<Vec<DescribeProducersTopic>> {
        self.describe_producers_for_with(partitions, timeout, Some(broker_id))
            .await
    }

    async fn describe_producers_for_with(
        &mut self,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
        timeout: Duration,
        broker_id: Option<i32>,
    ) -> Result<Vec<DescribeProducersTopic>> {
        let partitions: Vec<crate::TopicPartition> =
            partitions.into_iter().map(Into::into).collect();
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.describe_producers_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DescribeProducers".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let mut out: Vec<Option<DescribeProducersPartition>> = vec![None; partitions.len()];
        let mut pending: Vec<usize> = (0..partitions.len()).collect();
        let pin_broker = broker_id.is_some();
        loop {
            if pending.is_empty() {
                break;
            }
            if !pin_broker {
                let mut need: Vec<String> = Vec::new();
                for &i in &pending {
                    let Some(tp) = partitions.get(i) else {
                        continue;
                    };
                    if self.cluster.leader(&tp.topic, tp.partition).is_err()
                        && !need.iter().any(|t| t == &tp.topic)
                    {
                        need.push(tp.topic.clone());
                    }
                }
                if !need.is_empty() {
                    self.refresh_metadata(Some(&need)).await?;
                }
            }
            let mut by_node: HashMap<i32, Vec<usize>> = HashMap::new();
            let mut nodes: Vec<i32> = Vec::new();
            if let Some(id) = broker_id {
                let _prev = by_node.insert(id, pending.clone());
                nodes.push(id);
            } else {
                for &i in &pending {
                    let tp = partitions
                        .get(i)
                        .ok_or_else(|| Error::protocol("missing DescribeProducers query"))?;
                    let (node, _) = self.cluster.leader(&tp.topic, tp.partition)?;
                    match by_node.entry(node) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            nodes.push(node);
                            let _ = slot.insert(vec![i]);
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            slot.get_mut().push(i);
                        }
                    }
                }
            }
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.remove(&node).unwrap_or_default();
                match self
                    .describe_producers_on_node(
                        node,
                        version,
                        &partitions,
                        &idxs,
                        timeout,
                        pin_broker,
                    )
                    .await
                {
                    Ok((done, retry)) => {
                        for (i, part) in done {
                            if let Some(slot) = out.get_mut(i) {
                                *slot = Some(part);
                            }
                        }
                        still.extend(retry);
                    }
                    Err(e) if e.is_retriable() => {
                        let _ = self.conns.remove(&node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
            if pin_broker {
                continue;
            }
            for &i in &pending {
                if let Some(tp) = partitions.get(i) {
                    self.cluster.invalidate_topic(&tp.topic);
                }
            }
            let topics: Vec<String> = {
                let mut t = Vec::new();
                for &i in &pending {
                    if let Some(tp) = partitions.get(i) {
                        if !t.iter().any(|n| n == &tp.topic) {
                            t.push(tp.topic.clone());
                        }
                    }
                }
                t
            };
            if !topics.is_empty() {
                self.refresh_metadata(Some(&topics)).await?;
            }
        }
        let mut grouped: Vec<DescribeProducersTopic> = Vec::new();
        for (tp, part) in partitions.into_iter().zip(out) {
            let part = part.ok_or_else(|| Error::protocol("DescribeProducers missing result"))?;
            match grouped.last_mut() {
                Some(topic) if topic.name == tp.topic => topic.partitions.push(part),
                _ => grouped.push(DescribeProducersTopic::new(tp.topic, vec![part])),
            }
        }
        Ok(grouped)
    }

    async fn describe_producers_on_node(
        &mut self,
        node: i32,
        version: i16,
        partitions: &[crate::TopicPartition],
        idxs: &[usize],
        timeout: Duration,
        pin_broker: bool,
    ) -> Result<DescribeProducersNodeOutcome> {
        let topics = describe_producers_topics(partitions, idxs);
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing describe_producers conn"))?;
            conn.roundtrip(
                DESCRIBE_PRODUCERS,
                version,
                |buf| encode_describe_producers_topics_request(buf, &topics),
                timeout,
            )
            .await
        }?;
        let resp = decode_describe_producers_response(&mut body.clone())?;
        let mut by_tp: HashMap<(String, i32), DescribeProducersPartition> = HashMap::new();
        for topic in resp.topics {
            for part in topic.partitions {
                let _ = by_tp.insert((topic.name.clone(), part.partition_index), part);
            }
        }
        let mut done = Vec::new();
        let mut retry = Vec::new();
        for &i in idxs {
            let tp = partitions
                .get(i)
                .ok_or_else(|| Error::protocol("missing DescribeProducers query"))?;
            let part = by_tp
                .remove(&(tp.topic.clone(), tp.partition))
                .ok_or_else(|| {
                    Error::protocol(format!(
                        "DescribeProducers missing {}-{}",
                        tp.topic, tp.partition
                    ))
                })?;
            if part.error_code == 0 {
                done.push((i, part));
                continue;
            }
            let e = Error::broker(part.error_code, format!("{}-{}", tp.topic, tp.partition));
            if !pin_broker && e.is_retriable() {
                retry.push(i);
            } else {
                done.push((i, part));
            }
        }
        Ok((done, retry))
    }

    /// Brokers, controller, and cluster id (`DescribeCluster`).
    ///
    /// Negotiates v0–v2 (flexible from v0). v1 EndpointType is brokers
    /// (KIP-919). v2 omits fenced brokers (`IncludeFencedBrokers` false).
    /// Kafka 4.0 `validVersions` is `0-2`. v3+ is not spoken. See
    /// [`Self::describe_cluster_with`] for Java `DescribeClusterOptions`.
    /// Optional at [`Self::new`] (Kafka 2.8+ / KIP-700); a broker that
    /// omits api 60 returns [`Error::Unsupported`]. DescribeCluster has
    /// no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_cluster_timeout`].
    pub async fn describe_cluster(&mut self) -> Result<ClusterDescription> {
        self.describe_cluster_with(false, ENDPOINT_TYPE_BROKERS, false)
            .await
    }

    /// [`Self::describe_cluster`] with a one-shot RPC deadline (Java
    /// `DescribeClusterOptions.timeoutMs`).
    ///
    /// DescribeCluster has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_cluster_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<ClusterDescription> {
        self.describe_cluster_with_timeout(false, ENDPOINT_TYPE_BROKERS, false, timeout)
            .await
    }

    /// DescribeCluster with authorized operations, endpoint type, and
    /// fenced brokers (Java `describeCluster` plus
    /// `DescribeClusterOptions`).
    ///
    /// `endpoint_type` is [`EndpointType`] or a protocol `i8` (`1` brokers,
    /// `2` controllers). v1+ sends EndpointType. v2 sends
    /// IncludeFencedBrokers. v0 omits both even when set. DescribeCluster
    /// has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_cluster_with_timeout`].
    pub async fn describe_cluster_with(
        &mut self,
        include_authorized_operations: bool,
        endpoint_type: impl Into<i8>,
        include_fenced_brokers: bool,
    ) -> Result<ClusterDescription> {
        let timeout = self.cfg.request_timeout;
        self.describe_cluster_with_timeout(
            include_authorized_operations,
            endpoint_type,
            include_fenced_brokers,
            timeout,
        )
        .await
    }

    /// [`Self::describe_cluster_with`] with Java
    /// `DescribeClusterOptions.timeoutMs`.
    ///
    /// DescribeCluster has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_cluster_with_timeout(
        &mut self,
        include_authorized_operations: bool,
        endpoint_type: impl Into<i8>,
        include_fenced_brokers: bool,
        timeout: Duration,
    ) -> Result<ClusterDescription> {
        let endpoint_type = endpoint_type.into();
        let version = self.describe_cluster_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DescribeCluster v0-2".into())
        })?;
        let body = self
            .roundtrip_bootstrap(
                DESCRIBE_CLUSTER,
                version,
                |buf| {
                    encode_describe_cluster_request(
                        buf,
                        version,
                        include_authorized_operations,
                        endpoint_type,
                        include_fenced_brokers,
                    )
                },
                timeout,
            )
            .await?;
        decode_describe_cluster_response(&mut body.clone(), version)
    }

    /// Delete ACL bindings (`DeleteAcls`) matching `resource_type`.
    ///
    /// Negotiates v0–v3 (v0–v1 classic; v2+ flexible). v1+ sends
    /// PatternTypeFilter ANY. Operation and Permission are ANY (Java
    /// `AccessControlEntryFilter.ANY`). Kafka 4.0 `validVersions` is
    /// `1-3`. v4+ is not spoken.
    ///
    /// `resource_type` is [`crate::AclResourceType`] or a protocol `i8`.
    /// Returns the first filter's error code. For principal / host / name
    /// filters or Filters of N, use [`Self::delete_acls_with`]. DeleteAcls
    /// has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::delete_acls_timeout`].
    pub async fn delete_acls(&mut self, resource_type: impl Into<i8>) -> Result<i16> {
        let results = self
            .delete_acls_with(&[AclBindingFilter::resource_type(resource_type)])
            .await?;
        Ok(results.first().map(|r| r.error_code).unwrap_or(0))
    }

    /// [`Self::delete_acls`] with a one-shot RPC deadline (Java
    /// `DeleteAclsOptions.timeoutMs`).
    pub async fn delete_acls_timeout(
        &mut self,
        resource_type: impl Into<i8>,
        timeout: Duration,
    ) -> Result<i16> {
        let results = self
            .delete_acls_with_timeout(&[AclBindingFilter::resource_type(resource_type)], timeout)
            .await?;
        Ok(results.first().map(|r| r.error_code).unwrap_or(0))
    }

    /// [`Self::delete_acls`] with Java `deleteAcls(Collection)` Filters of N.
    ///
    /// Empty `filters` is a no-op. DeleteAcls has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::delete_acls_with_timeout`].
    pub async fn delete_acls_with(
        &mut self,
        filters: &[AclBindingFilter],
    ) -> Result<Vec<DeletedAclsFilterResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_acls_with_timeout(filters, timeout).await
    }

    /// [`Self::delete_acls_with`] with Java `DeleteAclsOptions.timeoutMs`.
    ///
    /// Empty `filters` is a no-op. DeleteAcls has no TimeoutMs; `timeout`
    /// is the RPC deadline.
    pub async fn delete_acls_with_timeout(
        &mut self,
        filters: &[AclBindingFilter],
        timeout: Duration,
    ) -> Result<Vec<DeletedAclsFilterResult>> {
        if filters.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.delete_acls_version;
        let body = self
            .roundtrip_bootstrap(
                DELETE_ACLS,
                version,
                |buf| encode_delete_acls_request(buf, version, filters),
                timeout,
            )
            .await?;
        decode_delete_acls_filter_results(&mut body.clone(), version)
    }

    /// Delete committed offsets for `group_id` (OffsetDelete api 47).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// `COORDINATOR_LOAD_IN_PROGRESS` / `COORDINATOR_NOT_AVAILABLE` /
    /// `NOT_COORDINATOR` refresh the coordinator and retry.
    ///
    /// Each item is a [`crate::TopicPartition`] (or anything that converts
    /// to one). Java `deleteConsumerGroupOffsets` is
    /// [`Self::delete_consumer_group_offsets`]. Optional at [`Self::new`]
    /// (Kafka 2.4+ / KIP-496); a broker that omits api 47 returns
    /// [`Error::Unsupported`]. OffsetDelete has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::delete_offsets_timeout`].
    pub async fn delete_offsets(
        &mut self,
        group_id: &str,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
    ) -> Result<Vec<OffsetDeleteResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_offsets_timeout(group_id, partitions, timeout)
            .await
    }

    /// [`Self::delete_offsets`] with a one-shot RPC deadline (Java
    /// `DeleteConsumerGroupOffsetsOptions.timeoutMs`).
    ///
    /// OffsetDelete has no TimeoutMs; `timeout` is the RPC deadline and
    /// the coordinator retry budget.
    pub async fn delete_offsets_timeout(
        &mut self,
        group_id: &str,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<OffsetDeleteResult>> {
        let partitions: Vec<(String, i32)> = partitions
            .into_iter()
            .map(|p| {
                let tp = p.into();
                (tp.topic, tp.partition)
            })
            .collect();
        let topics = offset_delete_topics(&partitions);
        let version = self
            .offset_delete_version
            .ok_or_else(|| Error::Unsupported("broker does not support OffsetDelete".into()))?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let group_id = group_id.to_string();
        loop {
            let stale = self
                .group_coord
                .as_ref()
                .is_none_or(|(g, _)| g != &group_id);
            if stale {
                let node = self.discover_group_coord(&group_id).await?;
                self.group_coord = Some((group_id.clone(), node));
            }
            let node = self
                .group_coord
                .as_ref()
                .map(|(_, n)| *n)
                .ok_or_else(|| Error::protocol("missing group coordinator"))?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing delete_offsets conn"))?;
                conn.roundtrip(
                    OFFSET_DELETE,
                    version,
                    |buf| encode_offset_delete_request(buf, &group_id, &topics),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.group_coord = None;
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (top, results) = decode_offset_delete_response(&mut body.clone())?;
            if error::coordinator_retriable(top) {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            if top != 0 {
                return Err(Error::broker(top, "OffsetDelete"));
            }
            return Ok(results);
        }
    }

    /// Delete committed offsets (Java `Admin.deleteConsumerGroupOffsets`).
    ///
    /// Same wire as [`Self::delete_offsets`]: OffsetDelete api 47 on the
    /// group coordinator. OffsetDelete has no TimeoutMs; the RPC deadline
    /// is [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::delete_consumer_group_offsets_timeout`].
    pub async fn delete_consumer_group_offsets(
        &mut self,
        group_id: &str,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
    ) -> Result<Vec<OffsetDeleteResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_consumer_group_offsets_timeout(group_id, partitions, timeout)
            .await
    }

    /// [`Self::delete_consumer_group_offsets`] with a one-shot RPC deadline
    /// (Java `DeleteConsumerGroupOffsetsOptions.timeoutMs`).
    ///
    /// OffsetDelete has no TimeoutMs; `timeout` is the RPC deadline and
    /// the coordinator retry budget.
    pub async fn delete_consumer_group_offsets_timeout(
        &mut self,
        group_id: &str,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<OffsetDeleteResult>> {
        self.delete_offsets_timeout(group_id, partitions, timeout)
            .await
    }

    /// List committed offsets for `group_id` (Java `listConsumerGroupOffsets`).
    ///
    /// OffsetFetch v1–v9 on the group coordinator. Partitions with no committed
    /// offset return [`crate::OffsetAndMetadata`] offset `-1`. Empty
    /// `partitions` returns an empty list. For every committed partition,
    /// use [`Self::list_all_consumer_group_offsets`]. `COORDINATOR_LOAD_IN_PROGRESS` /
    /// `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` refresh the coordinator
    /// and retry. Waits up to [`AdminConfig::request_timeout`]. For a one-shot
    /// timeout, use [`Self::list_consumer_group_offsets_timeout`].
    pub async fn list_consumer_group_offsets(
        &mut self,
        group_id: &str,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>> {
        let timeout = self.cfg.request_timeout;
        self.list_consumer_group_offsets_timeout(group_id, partitions, timeout)
            .await
    }

    /// [`Self::list_consumer_group_offsets`] with a one-shot timeout (Java
    /// `ListConsumerGroupOffsetsOptions.timeoutMs`).
    pub async fn list_consumer_group_offsets_timeout(
        &mut self,
        group_id: &str,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
        timeout: Duration,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>> {
        self.list_consumer_group_offsets_with(group_id, partitions, false, timeout)
            .await
    }

    /// [`Self::list_consumer_group_offsets`] plus `requireStable` and timeout
    /// (Java `ListConsumerGroupOffsetsOptions.requireStable` and `timeoutMs`).
    ///
    /// `require_stable` is OffsetFetch v7+ RequireStable. `timeout` is the
    /// RPC deadline.
    pub async fn list_consumer_group_offsets_with(
        &mut self,
        group_id: &str,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
        require_stable: bool,
        timeout: Duration,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>> {
        let partitions: Vec<crate::TopicPartition> =
            partitions.into_iter().map(Into::into).collect();
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: Vec<(String, i32)> = partitions
            .iter()
            .map(|tp| (tp.topic.clone(), tp.partition))
            .collect();
        let topics = crate::group::group_offset_fetch_topics(&wanted);
        let fetched = self
            .fetch_consumer_group_offsets(group_id, Some(topics), require_stable, timeout)
            .await?;
        let map = crate::group::committed_offset_map(&fetched)?;
        Ok(partitions
            .iter()
            .map(|tp| {
                let md = map
                    .get(&(tp.topic.clone(), tp.partition))
                    .cloned()
                    .unwrap_or_else(|| crate::OffsetAndMetadata::new(-1));
                (tp.clone(), md)
            })
            .collect())
    }

    /// List every committed offset for `group_id` (Java
    /// `listConsumerGroupOffsets(groupId)` with no topic-partition filter).
    ///
    /// OffsetFetch null Topics (v2+). Waits up to
    /// [`AdminConfig::request_timeout`]. For a one-shot timeout, use
    /// [`Self::list_all_consumer_group_offsets_timeout`]. For
    /// `requireStable` and a one-shot timeout, use
    /// [`Self::list_all_consumer_group_offsets_with`].
    pub async fn list_all_consumer_group_offsets(
        &mut self,
        group_id: &str,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>> {
        let timeout = self.cfg.request_timeout;
        self.list_all_consumer_group_offsets_timeout(group_id, timeout)
            .await
    }

    /// [`Self::list_all_consumer_group_offsets`] with a one-shot timeout
    /// (Java `ListConsumerGroupOffsetsOptions.timeoutMs`).
    ///
    /// OffsetFetch has no TimeoutMs; `timeout` is the RPC deadline.
    /// RequireStable stays false. For `requireStable`, use
    /// [`Self::list_all_consumer_group_offsets_with`].
    pub async fn list_all_consumer_group_offsets_timeout(
        &mut self,
        group_id: &str,
        timeout: Duration,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>> {
        self.list_all_consumer_group_offsets_with(group_id, false, timeout)
            .await
    }

    /// [`Self::list_all_consumer_group_offsets`] plus `requireStable` and
    /// timeout (Java `ListConsumerGroupOffsetsOptions`).
    pub async fn list_all_consumer_group_offsets_with(
        &mut self,
        group_id: &str,
        require_stable: bool,
        timeout: Duration,
    ) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>> {
        let fetched = self
            .fetch_consumer_group_offsets(group_id, None, require_stable, timeout)
            .await?;
        let map = crate::group::committed_offset_map(&fetched)?;
        Ok(map
            .into_iter()
            .map(|((topic, partition), md)| (crate::TopicPartition::new(topic, partition), md))
            .collect())
    }

    /// List committed offsets for several groups (Java
    /// `listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>)`).
    ///
    /// OffsetFetch v8+ Groups array of N (KIP-709): groups that share a
    /// coordinator go in one RPC. FindCoordinator v4+ CoordinatorKeys
    /// array of N (KIP-699): one FindCoordinator per retry, not one per
    /// group. Brokers that only speak OffsetFetch v1–v7 get one
    /// OffsetFetch per group. Brokers that only speak FindCoordinator
    /// v1–v3 get one FindCoordinator per group. Empty input is a no-op.
    /// Empty [`ListConsumerGroupOffsetsSpec::topic_partitions`] for a
    /// group is a no-op for that group. Waits up to
    /// [`AdminConfig::request_timeout`]. For a one-shot timeout, use
    /// [`Self::list_consumer_group_offsets_for_groups_timeout`]. For
    /// `requireStable` and a one-shot timeout, use
    /// [`Self::list_consumer_group_offsets_for_groups_with`].
    pub async fn list_consumer_group_offsets_for_groups(
        &mut self,
        groups: impl IntoIterator<Item = (impl Into<String>, ListConsumerGroupOffsetsSpec)>,
    ) -> Result<
        Vec<(
            String,
            Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>,
        )>,
    > {
        let timeout = self.cfg.request_timeout;
        self.list_consumer_group_offsets_for_groups_timeout(groups, timeout)
            .await
    }

    /// [`Self::list_consumer_group_offsets_for_groups`] with a one-shot
    /// timeout (Java `ListConsumerGroupOffsetsOptions.timeoutMs`).
    ///
    /// OffsetFetch has no TimeoutMs; `timeout` is the RPC deadline.
    /// RequireStable stays false. For `requireStable`, use
    /// [`Self::list_consumer_group_offsets_for_groups_with`].
    pub async fn list_consumer_group_offsets_for_groups_timeout(
        &mut self,
        groups: impl IntoIterator<Item = (impl Into<String>, ListConsumerGroupOffsetsSpec)>,
        timeout: Duration,
    ) -> Result<
        Vec<(
            String,
            Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>,
        )>,
    > {
        self.list_consumer_group_offsets_for_groups_with(groups, false, timeout)
            .await
    }

    /// [`Self::list_consumer_group_offsets_for_groups`] plus `requireStable`
    /// and timeout (Java `ListConsumerGroupOffsetsOptions`).
    ///
    /// `require_stable` is OffsetFetch v7+ RequireStable. `timeout` is the
    /// RPC deadline.
    pub async fn list_consumer_group_offsets_for_groups_with(
        &mut self,
        groups: impl IntoIterator<Item = (impl Into<String>, ListConsumerGroupOffsetsSpec)>,
        require_stable: bool,
        timeout: Duration,
    ) -> Result<
        Vec<(
            String,
            Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>,
        )>,
    > {
        let jobs: Vec<(String, ListConsumerGroupOffsetsSpec)> = groups
            .into_iter()
            .map(|(g, spec)| (g.into(), spec))
            .collect();
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let mut out: Vec<(
            String,
            Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>,
        )> = jobs.iter().map(|(g, _)| (g.clone(), Vec::new())).collect();
        let mut remaining: Vec<usize> = Vec::new();
        for (i, (_, spec)) in jobs.iter().enumerate() {
            if spec.partitions.as_ref().is_some_and(Vec::is_empty) {
                continue;
            }
            remaining.push(i);
        }
        if remaining.is_empty() {
            return Ok(out);
        }
        if let Some(version) = self
            .versions
            .get(&OFFSET_FETCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 8, 9))
        {
            let deadline = Instant::now() + timeout;
            let mut attempt = 0u32;
            loop {
                let group_ids: Vec<String> = remaining
                    .iter()
                    .filter_map(|&i| jobs.get(i).map(|j| j.0.clone()))
                    .collect();
                let coords = self.discover_group_coords(&group_ids).await?;
                let mut by_node: HashMap<i32, Vec<usize>> = HashMap::new();
                for &i in &remaining {
                    let group_id = jobs
                        .get(i)
                        .ok_or_else(|| Error::protocol("missing group spec"))?
                        .0
                        .clone();
                    let node = *coords.get(&group_id).ok_or_else(|| {
                        Error::protocol(format!("missing coordinator for {group_id}"))
                    })?;
                    by_node.entry(node).or_default().push(i);
                }
                let mut nodes: Vec<i32> = by_node.keys().copied().collect();
                nodes.sort_unstable();
                let mut next_remaining = Vec::new();
                let mut retry = false;
                for node in nodes {
                    let idxs = by_node.get(&node).cloned().unwrap_or_default();
                    let mut groups = Vec::new();
                    for &i in &idxs {
                        let job = jobs
                            .get(i)
                            .ok_or_else(|| Error::protocol("missing group spec"))?;
                        groups.push(OffsetFetchGroup::new(
                            job.0.clone(),
                            offset_fetch_topics_for_spec(&job.1),
                        ));
                    }
                    self.connect_node(node).await?;
                    let body = {
                        let conn = self.conns.get_mut(&node).ok_or_else(|| {
                            Error::protocol("missing list_consumer_group_offsets conn")
                        })?;
                        conn.roundtrip(
                            OFFSET_FETCH,
                            version,
                            |buf| {
                                encode_offset_fetch_groups_request(
                                    buf,
                                    version,
                                    &groups,
                                    require_stable,
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
                            self.group_coord = None;
                            next_remaining.extend(idxs);
                            retry = true;
                            continue;
                        }
                        Err(e) => return Err(e),
                    };
                    let results =
                        match decode_offset_fetch_groups_response(&mut body.clone(), version) {
                            Ok(r) => r,
                            Err(e) if e.broker_code().is_some_and(error::coordinator_retriable) => {
                                self.group_coord = None;
                                let _ = self.conns.remove(&node);
                                next_remaining.extend(idxs);
                                retry = true;
                                continue;
                            }
                            Err(e) => return Err(e),
                        };
                    if results
                        .iter()
                        .any(|r| error::coordinator_retriable(r.error_code))
                    {
                        self.group_coord = None;
                        let _ = self.conns.remove(&node);
                        next_remaining.extend(idxs);
                        retry = true;
                        continue;
                    }
                    let mut by_id: HashMap<String, crate::protocol::group::OffsetFetchGroupResult> =
                        HashMap::new();
                    for g in results {
                        if g.error_code != 0 {
                            return Err(Error::broker(g.error_code, g.group_id));
                        }
                        let _ = by_id.insert(g.group_id.clone(), g);
                    }
                    for i in idxs {
                        let job = jobs
                            .get(i)
                            .ok_or_else(|| Error::protocol("missing group spec"))?;
                        let Some(got) = by_id.remove(&job.0) else {
                            return Err(Error::protocol(format!(
                                "OffsetFetch response missing group {}",
                                job.0
                            )));
                        };
                        let listed = listed_group_offsets(&job.1, &got.topics)?;
                        let slot = out
                            .get_mut(i)
                            .ok_or_else(|| Error::protocol("missing group result slot"))?;
                        slot.1 = listed;
                    }
                }
                if !retry {
                    break;
                }
                remaining = next_remaining;
                self.wait_retry(&mut attempt, deadline).await?;
            }
            return Ok(out);
        }
        for i in remaining {
            let (group_id, topics, spec) = {
                let job = jobs
                    .get(i)
                    .ok_or_else(|| Error::protocol("missing group spec"))?;
                (
                    job.0.clone(),
                    offset_fetch_topics_for_spec(&job.1),
                    job.1.clone(),
                )
            };
            let fetched = self
                .fetch_consumer_group_offsets(&group_id, topics, require_stable, timeout)
                .await?;
            let listed = listed_group_offsets(&spec, &fetched)?;
            let slot = out
                .get_mut(i)
                .ok_or_else(|| Error::protocol("missing group result slot"))?;
            slot.1 = listed;
        }
        Ok(out)
    }

    async fn fetch_consumer_group_offsets(
        &mut self,
        group_id: &str,
        topics: Option<Vec<crate::protocol::group::OffsetFetchTopic>>,
        require_stable: bool,
        timeout: Duration,
    ) -> Result<Vec<crate::protocol::group::FetchedOffsetTopic>> {
        let client_min = if topics.is_none() { 2 } else { 1 };
        let version = self
            .versions
            .get(&OFFSET_FETCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, client_min, 9))
            .ok_or_else(|| {
                Error::Unsupported(if topics.is_none() {
                    "broker does not support OffsetFetch v2-9 (null Topics)".into()
                } else {
                    "broker does not support OffsetFetch v1-9".into()
                })
            })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let group_id = group_id.to_string();
        loop {
            let stale = self
                .group_coord
                .as_ref()
                .is_none_or(|(g, _)| g != &group_id);
            if stale {
                let node = self.discover_group_coord(&group_id).await?;
                self.group_coord = Some((group_id.clone(), node));
            }
            let node = self
                .group_coord
                .as_ref()
                .map(|(_, n)| *n)
                .ok_or_else(|| Error::protocol("missing group coordinator"))?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing list_consumer_group_offsets conn"))?;
                conn.roundtrip(
                    OFFSET_FETCH,
                    version,
                    |buf| {
                        encode_offset_fetch_request(
                            buf,
                            version,
                            &group_id,
                            None,
                            -1,
                            require_stable,
                            topics.as_deref(),
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
                    self.group_coord = None;
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match decode_offset_fetch_response(&mut body.clone(), version) {
                Ok(t) => return Ok(t),
                Err(e) if e.broker_code().is_some_and(error::coordinator_retriable) => {
                    self.group_coord = None;
                    let _ = self.conns.remove(&node);
                    self.wait_retry(&mut attempt, deadline).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Write committed offsets for `group_id` (Java `alterConsumerGroupOffsets`).
    ///
    /// OffsetCommit v2–v9 on the group coordinator with generation `-1` and an
    /// empty member id (admin, not a group member). Empty `offsets` is a
    /// no-op. Coordinator load / move errors refresh and retry.
    /// OffsetCommit has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::alter_consumer_group_offsets_timeout`].
    pub async fn alter_consumer_group_offsets(
        &mut self,
        group_id: &str,
        offsets: impl IntoIterator<Item = (impl Into<crate::TopicPartition>, crate::OffsetAndMetadata)>,
    ) -> Result<()> {
        let timeout = self.cfg.request_timeout;
        self.alter_consumer_group_offsets_timeout(group_id, offsets, timeout)
            .await
    }

    /// [`Self::alter_consumer_group_offsets`] with a one-shot RPC deadline
    /// (Java `AlterConsumerGroupOffsetsOptions.timeoutMs`).
    ///
    /// OffsetCommit has no TimeoutMs; `timeout` is the RPC deadline and
    /// the coordinator retry budget. v2–v4 RetentionTimeMs stays `-1`.
    pub async fn alter_consumer_group_offsets_timeout(
        &mut self,
        group_id: &str,
        offsets: impl IntoIterator<Item = (impl Into<crate::TopicPartition>, crate::OffsetAndMetadata)>,
        timeout: Duration,
    ) -> Result<()> {
        let offsets: Vec<(crate::TopicPartition, crate::OffsetAndMetadata)> = offsets
            .into_iter()
            .map(|(tp, md)| (tp.into(), md))
            .collect();
        if offsets.is_empty() {
            return Ok(());
        }
        let topics = crate::group::group_offset_topics(&offsets);
        let version = self
            .versions
            .get(&OFFSET_COMMIT)
            .and_then(|v| pick_version(v.min_version, v.max_version, 2, 9))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support OffsetCommit v2-9".into())
            })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let group_id = group_id.to_string();
        loop {
            let stale = self
                .group_coord
                .as_ref()
                .is_none_or(|(g, _)| g != &group_id);
            if stale {
                let node = self.discover_group_coord(&group_id).await?;
                self.group_coord = Some((group_id.clone(), node));
            }
            let node = self
                .group_coord
                .as_ref()
                .map(|(_, n)| *n)
                .ok_or_else(|| Error::protocol("missing group coordinator"))?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing alter_consumer_group_offsets conn"))?;
                conn.roundtrip(
                    OFFSET_COMMIT,
                    version,
                    |buf| {
                        encode_offset_commit_request(buf, version, &group_id, -1, "", None, &topics)
                    },
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.group_coord = None;
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let err = decode_offset_commit_response(&mut body.clone(), version)?;
            if error::coordinator_retriable(err) {
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            if err != 0 {
                return Err(Error::broker(err, "OffsetCommit"));
            }
            return Ok(());
        }
    }

    /// Describe KIP-848 consumer groups (ConsumerGroupDescribe api 69).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Negotiates ConsumerGroupDescribe v0–v1 (Kafka 4.0 `validVersions`
    /// `0-1`; flexible from v0). Request layout is the same on both
    /// versions. v1 adds MemberType INT8 (KIP-1099; `-1` unknown, `0`
    /// classic, `1` consumer). v0 omits MemberType; decode fills `-1`.
    /// Official Apache JSON listeners are `broker` only; the official
    /// response lists `NOT_COORDINATOR` among supported errors. This is
    /// not a controller hop and not a partition-leader hop: there is no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41) retry,
    /// and no `NOT_LEADER_OR_FOLLOWER` (6) hop. `COORDINATOR_LOAD_IN_PROGRESS`
    /// / `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. ErrorCode is per-group (bytes 5–6 on
    /// leftover-empty fixture group `"g"`), not top-level after throttle.
    /// FindCoordinator v4+ CoordinatorKeys array of N (KIP-699): one
    /// FindCoordinator per retry for uncached groups. ConsumerGroupDescribe
    /// is one RPC per coordinator. Brokers that only speak FindCoordinator
    /// v1–v3 get one FindCoordinator per uncached group. Empty input is
    /// a no-op. ConsumerGroupDescribe has no TimeoutMs; the RPC deadline
    /// is [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::consumer_group_describe_timeout`].
    pub async fn consumer_group_describe(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<DescribedConsumerGroup>> {
        let timeout = self.cfg.request_timeout;
        self.consumer_group_describe_timeout(group_ids, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::consumer_group_describe`] with a one-shot RPC deadline.
    ///
    /// ConsumerGroupDescribe has no TimeoutMs; `timeout` is the RPC
    /// deadline and the coordinator retry budget.
    /// Java `Admin.describeConsumerGroups` is
    /// [`Self::describe_consumer_groups_timeout`] (tries this RPC, then
    /// DescribeGroups). This overload is the crate-first KIP-848 describe.
    pub async fn consumer_group_describe_timeout(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<DescribedConsumerGroup>> {
        let ids: Vec<String> = group_ids.iter().map(|s| (*s).to_string()).collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.consumer_group_describe_version.ok_or_else(|| {
            Error::Unsupported("broker does not support ConsumerGroupDescribe v0-1".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let mut out: Vec<Option<DescribedConsumerGroup>> = vec![None; ids.len()];
        let mut pending: Vec<usize> = (0..ids.len()).collect();
        loop {
            let by_node = self.group_coord_nodes(&ids, &pending).await?;
            let mut nodes: Vec<i32> = by_node.keys().copied().collect();
            nodes.sort_unstable();
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.get(&node).cloned().unwrap_or_default();
                match self
                    .consumer_group_describe_on_node(
                        node,
                        version,
                        &ids,
                        &idxs,
                        include_authorized_operations,
                        timeout,
                    )
                    .await
                {
                    Ok(done) => {
                        for (i, g) in done {
                            if error::coordinator_retriable(g.error_code) {
                                self.invalidate_group_coord_idxs(&ids, &[i], node);
                                still.push(i);
                            } else if let Some(slot) = out.get_mut(i) {
                                *slot = Some(g);
                            }
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        self.invalidate_group_coord_idxs(&ids, &idxs, node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
        }
        out.into_iter()
            .zip(ids)
            .map(|(g, id)| {
                g.ok_or_else(|| Error::protocol(format!("ConsumerGroupDescribe missing {id}")))
            })
            .collect()
    }

    /// Describe classic consumer groups (DescribeGroups api 15).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Official Apache JSON listeners are `broker` only. Official
    /// listed per-group errors include `NOT_COORDINATOR` (16). This is
    /// not a controller hop and not a partition-leader hop: there is no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41) retry,
    /// and no `NOT_LEADER_OR_FOLLOWER` (6) hop. `COORDINATOR_LOAD_IN_PROGRESS`
    /// / `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. ErrorCode is per-group (bytes 5–6 on
    /// leftover-empty fixture group `"g"` at v6), not top-level after throttle.
    /// Negotiates v0–v6 (v3 IncludeAuthorizedOperations; v4 GroupInstanceId;
    /// v5 flexible; v6 ErrorMessage / GROUP_ID_NOT_FOUND).
    /// Java `describeClassicGroups` is [`Self::describe_classic_groups`].
    /// Java `describeConsumerGroups` is [`Self::describe_consumer_groups`].
    /// FindCoordinator v4+ CoordinatorKeys array of N (KIP-699): one
    /// FindCoordinator per retry for uncached groups. DescribeGroups is
    /// one RPC per coordinator. Brokers that only speak FindCoordinator
    /// v1–v3 get one FindCoordinator per uncached group. Empty input is
    /// a no-op. DescribeGroups has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_groups_timeout`].
    pub async fn describe_groups(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<DescribedGroup>> {
        let timeout = self.cfg.request_timeout;
        self.describe_groups_timeout(group_ids, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::describe_groups`] with a one-shot RPC deadline (Java
    /// `DescribeClassicGroupsOptions` / `DescribeConsumerGroupsOptions.timeoutMs`).
    ///
    /// DescribeGroups has no TimeoutMs; `timeout` is the RPC deadline
    /// and the coordinator retry budget.
    pub async fn describe_groups_timeout(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<DescribedGroup>> {
        let ids: Vec<String> = group_ids.iter().map(|s| (*s).to_string()).collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.describe_groups_version;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let mut out: Vec<Option<DescribedGroup>> = vec![None; ids.len()];
        let mut pending: Vec<usize> = (0..ids.len()).collect();
        loop {
            let by_node = self.group_coord_nodes(&ids, &pending).await?;
            let mut nodes: Vec<i32> = by_node.keys().copied().collect();
            nodes.sort_unstable();
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.get(&node).cloned().unwrap_or_default();
                match self
                    .describe_groups_on_node(
                        node,
                        version,
                        &ids,
                        &idxs,
                        include_authorized_operations,
                        timeout,
                    )
                    .await
                {
                    Ok(done) => {
                        for (i, g) in done {
                            if error::coordinator_retriable(g.error_code) {
                                self.invalidate_group_coord_idxs(&ids, &[i], node);
                                still.push(i);
                            } else if let Some(slot) = out.get_mut(i) {
                                *slot = Some(g);
                            }
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        self.invalidate_group_coord_idxs(&ids, &idxs, node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
        }
        out.into_iter()
            .zip(ids)
            .map(|(g, id)| g.ok_or_else(|| Error::protocol(format!("DescribeGroups missing {id}"))))
            .collect()
    }

    /// Describe classic groups (Java `Admin.describeClassicGroups`).
    ///
    /// Same wire as [`Self::describe_groups`]: DescribeGroups api 15 on
    /// the group coordinator. Java's `DescribeClassicGroupsHandler`
    /// builds a DescribeGroups request. Empty input is a no-op.
    /// DescribeGroups has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_classic_groups_timeout`].
    pub async fn describe_classic_groups(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<DescribedGroup>> {
        let timeout = self.cfg.request_timeout;
        self.describe_classic_groups_timeout(group_ids, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::describe_classic_groups`] with a one-shot RPC deadline (Java
    /// `DescribeClassicGroupsOptions.timeoutMs`).
    ///
    /// DescribeGroups has no TimeoutMs; `timeout` is the RPC deadline
    /// and the coordinator retry budget.
    pub async fn describe_classic_groups_timeout(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<DescribedGroup>> {
        self.describe_groups_timeout(group_ids, include_authorized_operations, timeout)
            .await
    }

    /// Describe consumer groups (Java `Admin.describeConsumerGroups`).
    ///
    /// Kafka 4.0 `DescribeConsumerGroupsHandler` sends ConsumerGroupDescribe
    /// (api 69) on the group coordinator first. Per-group
    /// [`crate::error::UNSUPPORTED_VERSION`] (35) or
    /// [`crate::error::GROUP_ID_NOT_FOUND`] (69) retries that group with
    /// DescribeGroups (api 15). A broker that does not advertise api 69, or
    /// an RPC-level [`Error::Unsupported`] for that API, uses DescribeGroups
    /// for every group. Coordinator errors still refresh FindCoordinator
    /// and retry. Empty input is a no-op. Neither RPC has TimeoutMs; the
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::describe_consumer_groups_timeout`].
    pub async fn describe_consumer_groups(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<ConsumerGroupDescription>> {
        let timeout = self.cfg.request_timeout;
        self.describe_consumer_groups_timeout(group_ids, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::describe_consumer_groups`] with a one-shot RPC deadline (Java
    /// `DescribeConsumerGroupsOptions.timeoutMs`).
    ///
    /// Neither ConsumerGroupDescribe nor DescribeGroups has TimeoutMs;
    /// `timeout` is the RPC deadline and the coordinator retry budget
    /// across both hops.
    pub async fn describe_consumer_groups_timeout(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<ConsumerGroupDescription>> {
        if group_ids.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + timeout;
        let mut out: Vec<Option<ConsumerGroupDescription>> = vec![None; group_ids.len()];
        let mut classic_pending: Vec<usize> = Vec::new();
        if self.consumer_group_describe_version.is_some() {
            match self
                .consumer_group_describe_timeout(group_ids, include_authorized_operations, timeout)
                .await
            {
                Ok(groups) => {
                    for (i, g) in groups.into_iter().enumerate() {
                        if error::consumer_group_describe_classic_fallback(g.error_code) {
                            classic_pending.push(i);
                        } else if let Some(slot) = out.get_mut(i) {
                            *slot = Some(ConsumerGroupDescription::Consumer(g));
                        }
                    }
                }
                Err(Error::Unsupported(_)) => {
                    classic_pending.extend(0..group_ids.len());
                }
                Err(e) => return Err(e),
            }
        } else {
            classic_pending.extend(0..group_ids.len());
        }
        if !classic_pending.is_empty() {
            let classic_ids: Vec<&str> = classic_pending
                .iter()
                .filter_map(|&i| group_ids.get(i).copied())
                .collect();
            let remaining = deadline.saturating_duration_since(Instant::now());
            let classic = self
                .describe_groups_timeout(&classic_ids, include_authorized_operations, remaining)
                .await?;
            for (i, g) in classic_pending.iter().copied().zip(classic) {
                if let Some(slot) = out.get_mut(i) {
                    *slot = Some(ConsumerGroupDescription::Classic(g));
                }
            }
        }
        out.into_iter()
            .enumerate()
            .map(|(i, g)| {
                g.ok_or_else(|| {
                    let id = group_ids.get(i).copied().unwrap_or("");
                    Error::protocol(format!("describeConsumerGroups missing {id}"))
                })
            })
            .collect()
    }

    /// List consumer groups (ListGroups api 16).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official Apache
    /// JSON listeners are `broker` only. Official listed errors are
    /// `COORDINATOR_LOAD_IN_PROGRESS` (14), `COORDINATOR_NOT_AVAILABLE`
    /// (15), `AUTHORIZATION_FAILED` (29). `NOT_COORDINATOR` (16) is not
    /// listed. This is not a group-coordinator hop, not a controller hop,
    /// and not a partition-leader hop: there is no FindCoordinator,
    /// no Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41)
    /// retry, and no `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level
    /// `error_code` is the INT16 at bytes 4–5 on v1+ (after throttle) — not a
    /// first-group field. Negotiates v0–v5 (v3 flexible; v4 StatesFilter /
    /// GroupState; v5 TypesFilter / GroupType). Java `listConsumerGroups` is
    /// [`Self::list_consumer_groups`]. ListGroups has no TimeoutMs; the
    /// RPC deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::list_groups_timeout`].
    pub async fn list_groups(
        &mut self,
        states_filter: &[&str],
        types_filter: &[&str],
    ) -> Result<Vec<ListedGroup>> {
        let timeout = self.cfg.request_timeout;
        self.list_groups_timeout(states_filter, types_filter, timeout)
            .await
    }

    /// [`Self::list_groups`] with a one-shot RPC deadline (Java
    /// `ListGroupsOptions.timeoutMs`).
    ///
    /// ListGroups has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn list_groups_timeout(
        &mut self,
        states_filter: &[&str],
        types_filter: &[&str],
        timeout: Duration,
    ) -> Result<Vec<ListedGroup>> {
        let states: Vec<String> = states_filter.iter().map(|s| (*s).to_string()).collect();
        let types: Vec<String> = types_filter.iter().map(|s| (*s).to_string()).collect();
        self.list_groups_owned(states, types, timeout).await
    }

    async fn list_groups_owned(
        &mut self,
        states: Vec<String>,
        types: Vec<String>,
        timeout: Duration,
    ) -> Result<Vec<ListedGroup>> {
        let version = self.list_groups_version;
        let body = self
            .roundtrip_bootstrap(
                LIST_GROUPS,
                version,
                |buf| encode_list_groups_request(buf, version, &states, &types),
                timeout,
            )
            .await?;
        let resp = decode_list_groups_response(&mut body.clone(), version)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ListGroups"));
        }
        Ok(resp.groups)
    }

    /// [`Self::list_groups`] with Java `GroupState` / `GroupType`
    /// (`ListGroupsOptions.inGroupStates` / `withTypes`).
    ///
    /// TypesFilter strings are Java `GroupType.toString` (`Classic`, not
    /// `classic`). ListGroups has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::list_groups_with_timeout`].
    pub async fn list_groups_with(
        &mut self,
        states: impl IntoIterator<Item = GroupState>,
        types: impl IntoIterator<Item = GroupType>,
    ) -> Result<Vec<ListedGroup>> {
        let timeout = self.cfg.request_timeout;
        self.list_groups_with_timeout(states, types, timeout).await
    }

    /// [`Self::list_groups_with`] with a one-shot RPC deadline (Java
    /// `ListGroupsOptions.timeoutMs`).
    ///
    /// ListGroups has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn list_groups_with_timeout(
        &mut self,
        states: impl IntoIterator<Item = GroupState>,
        types: impl IntoIterator<Item = GroupType>,
        timeout: Duration,
    ) -> Result<Vec<ListedGroup>> {
        let states: Vec<String> = states.into_iter().map(String::from).collect();
        let types: Vec<String> = types.into_iter().map(String::from).collect();
        self.list_groups_owned(states, types, timeout).await
    }

    /// List every group (Java `Admin.listGroups()`).
    ///
    /// Same wire as [`Self::list_groups`] with empty StatesFilter and
    /// TypesFilter (Java `ListGroupsOptions` default).
    pub async fn list_groups_all(&mut self) -> Result<Vec<ListedGroup>> {
        self.list_groups(&[], &[]).await
    }

    /// [`Self::list_groups_all`] with a one-shot RPC deadline (Java
    /// `ListGroupsOptions.timeoutMs`).
    pub async fn list_groups_all_timeout(&mut self, timeout: Duration) -> Result<Vec<ListedGroup>> {
        self.list_groups_timeout(&[], &[], timeout).await
    }

    /// List consumer groups (Java `Admin.listConsumerGroups`).
    ///
    /// Same wire as [`Self::list_groups`]: ListGroups api 16 on the
    /// connected broker. Java's `ListConsumerGroupsHandler` builds a
    /// ListGroups request. ListGroups has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::list_consumer_groups_timeout`].
    pub async fn list_consumer_groups(
        &mut self,
        states_filter: &[&str],
        types_filter: &[&str],
    ) -> Result<Vec<ListedGroup>> {
        self.list_groups(states_filter, types_filter).await
    }

    /// [`Self::list_consumer_groups`] with a one-shot RPC deadline (Java
    /// `ListConsumerGroupsOptions.timeoutMs`).
    ///
    /// ListGroups has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn list_consumer_groups_timeout(
        &mut self,
        states_filter: &[&str],
        types_filter: &[&str],
        timeout: Duration,
    ) -> Result<Vec<ListedGroup>> {
        self.list_groups_timeout(states_filter, types_filter, timeout)
            .await
    }

    /// [`Self::list_consumer_groups`] with Java `GroupState` / `GroupType`
    /// (`ListConsumerGroupsOptions.inGroupStates` / `withTypes`).
    ///
    /// Same wire as [`Self::list_groups_with`].
    pub async fn list_consumer_groups_with(
        &mut self,
        states: impl IntoIterator<Item = GroupState>,
        types: impl IntoIterator<Item = GroupType>,
    ) -> Result<Vec<ListedGroup>> {
        self.list_groups_with(states, types).await
    }

    /// [`Self::list_consumer_groups_with`] with a one-shot RPC deadline
    /// (Java `ListConsumerGroupsOptions.timeoutMs`).
    pub async fn list_consumer_groups_with_timeout(
        &mut self,
        states: impl IntoIterator<Item = GroupState>,
        types: impl IntoIterator<Item = GroupType>,
        timeout: Duration,
    ) -> Result<Vec<ListedGroup>> {
        self.list_groups_with_timeout(states, types, timeout).await
    }

    /// List every consumer group (Java `Admin.listConsumerGroups()`).
    ///
    /// Same wire as [`Self::list_consumer_groups`] with empty StatesFilter
    /// and TypesFilter (Java `ListConsumerGroupsOptions` default).
    pub async fn list_consumer_groups_all(&mut self) -> Result<Vec<ListedGroup>> {
        self.list_groups_all().await
    }

    /// [`Self::list_consumer_groups_all`] with a one-shot RPC deadline
    /// (Java `ListConsumerGroupsOptions.timeoutMs`).
    pub async fn list_consumer_groups_all_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ListedGroup>> {
        self.list_groups_all_timeout(timeout).await
    }

    /// Delete consumer groups (DeleteGroups api 42).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Negotiates DeleteGroups v0–v2 (Kafka 4.0 `validVersions` `0-2`).
    /// v0–v1 are classic; v2 is the first flexible version. Response
    /// ThrottleTimeMs is present on every spoken version. Official Apache
    /// JSON listeners are `broker` only. Official listed per-group errors
    /// include `NOT_COORDINATOR` (16). This is not a controller hop and
    /// not a partition-leader hop: there is no Metadata `controller_id`
    /// lookup, no `NOT_CONTROLLER` (41) retry, and no
    /// `NOT_LEADER_OR_FOLLOWER` (6) hop. `COORDINATOR_LOAD_IN_PROGRESS` /
    /// `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. ErrorCode is per-group after GroupId
    /// (bytes 7–8 on leftover-empty fixture group `"g"` on v2; classic
    /// v0–v1 place that ErrorCode later). Java `deleteShareGroups` is
    /// [`Self::delete_share_groups`]. Java `deleteConsumerGroups` is
    /// [`Self::delete_consumer_groups`]. FindCoordinator v4+
    /// CoordinatorKeys array of N (KIP-699): one FindCoordinator per
    /// retry for uncached groups. DeleteGroups is one RPC per
    /// coordinator. Brokers that only speak FindCoordinator v1–v3 get
    /// one FindCoordinator per uncached group. Empty input is a no-op.
    /// DeleteGroups has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::delete_groups_timeout`].
    pub async fn delete_groups(&mut self, group_ids: &[&str]) -> Result<Vec<DeletableGroupResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_groups_timeout(group_ids, timeout).await
    }

    /// [`Self::delete_groups`] with a one-shot RPC deadline (Java
    /// `DeleteConsumerGroupsOptions` / `DeleteShareGroupsOptions.timeoutMs`).
    ///
    /// DeleteGroups has no TimeoutMs; `timeout` is the RPC deadline and
    /// the coordinator retry budget.
    pub async fn delete_groups_timeout(
        &mut self,
        group_ids: &[&str],
        timeout: Duration,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.delete_group_ids(
            group_ids.iter().map(|s| (*s).to_string()).collect(),
            timeout,
        )
        .await
    }

    async fn delete_group_ids(
        &mut self,
        ids: Vec<String>,
        timeout: Duration,
    ) -> Result<Vec<DeletableGroupResult>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.delete_groups_version;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let mut out: Vec<Option<DeletableGroupResult>> = vec![None; ids.len()];
        let mut pending: Vec<usize> = (0..ids.len()).collect();
        loop {
            let by_node = self.group_coord_nodes(&ids, &pending).await?;
            let mut nodes: Vec<i32> = by_node.keys().copied().collect();
            nodes.sort_unstable();
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.get(&node).cloned().unwrap_or_default();
                match self
                    .delete_groups_on_node(node, version, &ids, &idxs, timeout)
                    .await
                {
                    Ok(done) => {
                        for (i, g) in done {
                            if error::coordinator_retriable(g.error_code) {
                                self.invalidate_group_coord_idxs(&ids, &[i], node);
                                still.push(i);
                            } else if let Some(slot) = out.get_mut(i) {
                                *slot = Some(g);
                            }
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        self.invalidate_group_coord_idxs(&ids, &idxs, node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
        }
        out.into_iter()
            .zip(ids)
            .map(|(g, id)| g.ok_or_else(|| Error::protocol(format!("DeleteGroups missing {id}"))))
            .collect()
    }

    /// Delete share groups (Java `Admin.deleteShareGroups`).
    ///
    /// Same wire as [`Self::delete_groups`]: DeleteGroups api 42 on the
    /// group coordinator (`FindCoordinator` `key_type=0`). Java's
    /// `DeleteShareGroupsHandler` extends `DeleteGroupsHandler`. Empty
    /// input is a no-op. DeleteGroups has no TimeoutMs; the RPC deadline
    /// is [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::delete_share_groups_timeout`].
    pub async fn delete_share_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_share_groups_timeout(group_ids, timeout).await
    }

    /// [`Self::delete_share_groups`] with a one-shot RPC deadline (Java
    /// `DeleteShareGroupsOptions.timeoutMs`).
    ///
    /// DeleteGroups has no TimeoutMs; `timeout` is the RPC deadline and
    /// the coordinator retry budget.
    pub async fn delete_share_groups_timeout(
        &mut self,
        group_ids: impl IntoIterator<Item = impl AsRef<str>>,
        timeout: Duration,
    ) -> Result<Vec<DeletableGroupResult>> {
        let ids: Vec<String> = group_ids
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        self.delete_group_ids(ids, timeout).await
    }

    /// Delete consumer groups (Java `Admin.deleteConsumerGroups`).
    ///
    /// Same wire as [`Self::delete_groups`]: DeleteGroups api 42 on the
    /// group coordinator (`FindCoordinator` `key_type=0`). Java's
    /// `DeleteConsumerGroupsHandler` sends DeleteGroups. Empty
    /// input is a no-op. DeleteGroups has no TimeoutMs; the RPC deadline
    /// is [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::delete_consumer_groups_timeout`].
    pub async fn delete_consumer_groups(
        &mut self,
        group_ids: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let timeout = self.cfg.request_timeout;
        self.delete_consumer_groups_timeout(group_ids, timeout)
            .await
    }

    /// [`Self::delete_consumer_groups`] with a one-shot RPC deadline (Java
    /// `DeleteConsumerGroupsOptions.timeoutMs`).
    ///
    /// DeleteGroups has no TimeoutMs; `timeout` is the RPC deadline and
    /// the coordinator retry budget.
    pub async fn delete_consumer_groups_timeout(
        &mut self,
        group_ids: impl IntoIterator<Item = impl AsRef<str>>,
        timeout: Duration,
    ) -> Result<Vec<DeletableGroupResult>> {
        let ids: Vec<String> = group_ids
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        self.delete_group_ids(ids, timeout).await
    }

    /// Remove static members from a consumer group (Java
    /// `removeMembersFromConsumerGroup`).
    ///
    /// LeaveGroup v3 (classic), v4 (flexible), or v5 (KIP-800 Reason)
    /// on the group coordinator (`FindCoordinator` `key_type=0`) with a
    /// members array. Each [`MemberToRemove`] is a `group.instance.id`
    /// (KIP-345); member id on the wire is empty. v5 sends
    /// [`DEFAULT_LEAVE_GROUP_REASON`]. Empty `members` returns an empty
    /// list. Coordinator load / move errors refresh and retry. To remove
    /// every member, use [`Self::remove_all_members_from_consumer_group`].
    /// For a custom LeaveGroup v5 Reason, use
    /// [`Self::remove_members_from_consumer_group_with_reason`].
    /// LeaveGroup has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::remove_members_from_consumer_group_timeout`].
    pub async fn remove_members_from_consumer_group(
        &mut self,
        group_id: &str,
        members: impl IntoIterator<Item = impl Into<MemberToRemove>>,
    ) -> Result<Vec<RemovedMember>> {
        let timeout = self.cfg.request_timeout;
        self.remove_members_from_consumer_group_timeout(group_id, members, timeout)
            .await
    }

    /// [`Self::remove_members_from_consumer_group`] with a one-shot RPC
    /// deadline (Java `RemoveMembersFromConsumerGroupOptions.timeoutMs`).
    ///
    /// LeaveGroup has no TimeoutMs; `timeout` is the RPC deadline and the
    /// coordinator retry budget.
    pub async fn remove_members_from_consumer_group_timeout(
        &mut self,
        group_id: &str,
        members: impl IntoIterator<Item = impl Into<MemberToRemove>>,
        timeout: Duration,
    ) -> Result<Vec<RemovedMember>> {
        self.remove_members_from_consumer_group_timeout_with_reason(group_id, members, timeout, "")
            .await
    }

    /// [`Self::remove_members_from_consumer_group`] with Java
    /// `RemoveMembersFromConsumerGroupOptions.reason`.
    ///
    /// LeaveGroup v5 sends `reason` (KIP-800). Empty reason uses
    /// [`DEFAULT_LEAVE_GROUP_REASON`]. The string is truncated to 255
    /// characters. Kafka 4.0 `KafkaAdminClient` does not wire this
    /// Options field; later Java and this crate do. For a one-shot
    /// deadline, use
    /// [`Self::remove_members_from_consumer_group_timeout_with_reason`].
    pub async fn remove_members_from_consumer_group_with_reason(
        &mut self,
        group_id: &str,
        members: impl IntoIterator<Item = impl Into<MemberToRemove>>,
        reason: impl Into<String>,
    ) -> Result<Vec<RemovedMember>> {
        let timeout = self.cfg.request_timeout;
        self.remove_members_from_consumer_group_timeout_with_reason(
            group_id, members, timeout, reason,
        )
        .await
    }

    /// [`Self::remove_members_from_consumer_group_with_reason`] with a
    /// one-shot RPC deadline (Java
    /// `RemoveMembersFromConsumerGroupOptions.timeoutMs` plus `reason`).
    ///
    /// LeaveGroup has no TimeoutMs; `timeout` is the RPC deadline and the
    /// coordinator retry budget. Duration is before `reason`.
    pub async fn remove_members_from_consumer_group_timeout_with_reason(
        &mut self,
        group_id: &str,
        members: impl IntoIterator<Item = impl Into<MemberToRemove>>,
        timeout: Duration,
        reason: impl Into<String>,
    ) -> Result<Vec<RemovedMember>> {
        let reason = reason.into();
        let members: Vec<LeaveGroupMember> = members
            .into_iter()
            .map(|m| {
                let m = m.into();
                LeaveGroupMember {
                    member_id: String::new(),
                    group_instance_id: Some(m.group_instance_id),
                    reason: Some(reason.clone()),
                }
            })
            .collect();
        if members.is_empty() {
            return Ok(Vec::new());
        }
        self.leave_group_members(group_id, members, timeout).await
    }

    /// Remove every member of a consumer group (Java
    /// `RemoveMembersFromConsumerGroupOptions.removeAll`).
    ///
    /// DescribeGroups, then LeaveGroup v3–v5 with those members (member
    /// id plus `group.instance.id` when present). v5 sends
    /// [`DEFAULT_LEAVE_GROUP_REASON`]. A group with no members is a
    /// no-op (no LeaveGroup). This is not
    /// [`Self::remove_members_from_consumer_group`] with an empty list.
    /// For a custom LeaveGroup v5 Reason, use
    /// [`Self::remove_all_members_from_consumer_group_with_reason`].
    /// DescribeGroups and LeaveGroup have no TimeoutMs; the RPC deadline
    /// is [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::remove_all_members_from_consumer_group_timeout`].
    pub async fn remove_all_members_from_consumer_group(
        &mut self,
        group_id: &str,
    ) -> Result<Vec<RemovedMember>> {
        let timeout = self.cfg.request_timeout;
        self.remove_all_members_from_consumer_group_timeout(group_id, timeout)
            .await
    }

    /// [`Self::remove_all_members_from_consumer_group`] with a one-shot RPC
    /// deadline (Java `RemoveMembersFromConsumerGroupOptions.timeoutMs`
    /// plus `removeAll`).
    ///
    /// DescribeGroups and LeaveGroup have no TimeoutMs; `timeout` is the
    /// RPC deadline and the coordinator retry budget for both hops.
    pub async fn remove_all_members_from_consumer_group_timeout(
        &mut self,
        group_id: &str,
        timeout: Duration,
    ) -> Result<Vec<RemovedMember>> {
        self.remove_all_members_from_consumer_group_timeout_with_reason(group_id, timeout, "")
            .await
    }

    /// [`Self::remove_all_members_from_consumer_group`] with Java
    /// `RemoveMembersFromConsumerGroupOptions.reason` plus `removeAll`.
    ///
    /// LeaveGroup v5 sends `reason` (KIP-800). Empty reason uses
    /// [`DEFAULT_LEAVE_GROUP_REASON`]. The string is truncated to 255
    /// characters. Kafka 4.0 `KafkaAdminClient` does not wire this
    /// Options field; later Java and this crate do. For a one-shot
    /// deadline, use
    /// [`Self::remove_all_members_from_consumer_group_timeout_with_reason`].
    pub async fn remove_all_members_from_consumer_group_with_reason(
        &mut self,
        group_id: &str,
        reason: impl Into<String>,
    ) -> Result<Vec<RemovedMember>> {
        let timeout = self.cfg.request_timeout;
        self.remove_all_members_from_consumer_group_timeout_with_reason(group_id, timeout, reason)
            .await
    }

    /// [`Self::remove_all_members_from_consumer_group_with_reason`] with a
    /// one-shot RPC deadline (Java
    /// `RemoveMembersFromConsumerGroupOptions.timeoutMs` plus `removeAll`
    /// and `reason`).
    ///
    /// DescribeGroups and LeaveGroup have no TimeoutMs; `timeout` is the
    /// RPC deadline and the coordinator retry budget for both hops.
    /// Duration is before `reason`.
    pub async fn remove_all_members_from_consumer_group_timeout_with_reason(
        &mut self,
        group_id: &str,
        timeout: Duration,
        reason: impl Into<String>,
    ) -> Result<Vec<RemovedMember>> {
        let described = self
            .describe_groups_timeout(&[group_id], false, timeout)
            .await?;
        let Some(g) = described.first() else {
            return Ok(Vec::new());
        };
        if g.error_code != 0 {
            return Err(Error::broker(g.error_code, "DescribeGroups"));
        }
        let reason = reason.into();
        let members: Vec<LeaveGroupMember> = g
            .members
            .iter()
            .map(|m| LeaveGroupMember {
                member_id: m.member_id.clone(),
                group_instance_id: m.group_instance_id.clone(),
                reason: Some(reason.clone()),
            })
            .collect();
        if members.is_empty() {
            return Ok(Vec::new());
        }
        self.leave_group_members(group_id, members, timeout).await
    }

    async fn leave_group_members(
        &mut self,
        group_id: &str,
        members: Vec<LeaveGroupMember>,
        timeout: Duration,
    ) -> Result<Vec<RemovedMember>> {
        let version = self
            .versions
            .get(&LEAVE_GROUP)
            .and_then(|v| pick_version(v.min_version, v.max_version, 3, 5))
            .ok_or_else(|| Error::Unsupported("broker does not support LeaveGroup v3+".into()))?;
        let members: Vec<LeaveGroupMember> = members
            .into_iter()
            .map(|mut m| {
                m.reason = Some(admin_leave_reason(m.reason.as_deref()));
                m
            })
            .collect();
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let group_id = group_id.to_string();
        loop {
            let stale = self
                .group_coord
                .as_ref()
                .is_none_or(|(g, _)| g != &group_id);
            if stale {
                let node = self.discover_group_coord(&group_id).await?;
                self.group_coord = Some((group_id.clone(), node));
            }
            let node = self
                .group_coord
                .as_ref()
                .map(|(_, n)| *n)
                .ok_or_else(|| Error::protocol("missing group coordinator"))?;
            self.connect_node(node).await?;
            let body = {
                let conn = self.conns.get_mut(&node).ok_or_else(|| {
                    Error::protocol("missing remove_members_from_consumer_group conn")
                })?;
                conn.roundtrip(
                    LEAVE_GROUP,
                    version,
                    |buf| encode_leave_group_request_members(buf, version, &group_id, &members),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.group_coord = None;
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (err, results) = decode_leave_group_response_version(&mut body.clone(), version)?;
            if error::coordinator_retriable(err) {
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            if err != 0 {
                return Err(Error::broker(err, "LeaveGroup"));
            }
            return Ok(results
                .into_iter()
                .map(|m| RemovedMember {
                    member_id: m.member_id,
                    group_instance_id: m.group_instance_id,
                    error_code: m.error_code,
                })
                .collect());
        }
    }

    /// Describe KIP-932 share groups (ShareGroupDescribe api 77).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Negotiates ShareGroupDescribe v0–v1. Kafka 4.0 `validVersions` is
    /// `"0"` (`latestVersionUnstable`). Kafka 4.1 `validVersions` is
    /// `"1"` (v0 removed). Request and response fields are identical.
    /// This crate speaks 0–1. Official Apache JSON listeners are
    /// `broker` only. Official listed errors include `NOT_COORDINATOR`
    /// (16). Official Java `DescribeShareGroupsHandler` uses
    /// `CoordinatorType.GROUP`. This is not a controller hop and not a
    /// partition-leader hop: there is no Metadata `controller_id`
    /// lookup, no `NOT_CONTROLLER` (41) retry, and no
    /// `NOT_LEADER_OR_FOLLOWER` (6) hop. SHARE (`key_type=2`) is the
    /// FindCoordinator v6 share-state key
    /// (`groupId:topicId:partition`) and is not used here.
    /// `COORDINATOR_LOAD_IN_PROGRESS` / `COORDINATOR_NOT_AVAILABLE` /
    /// `NOT_COORDINATOR` (16) refresh the coordinator and retry.
    /// ErrorCode is per-group (bytes 5–6 on leftover-empty fixture
    /// group `"g"`), not top-level after throttle.
    /// Java `describeShareGroups` is [`Self::describe_share_groups`].
    /// FindCoordinator v4+ CoordinatorKeys array of N (KIP-699): one
    /// FindCoordinator per retry for uncached groups. ShareGroupDescribe
    /// is one RPC per coordinator. Brokers that only speak FindCoordinator
    /// v1–v3 get one FindCoordinator per uncached group. Empty input is
    /// a no-op. ShareGroupDescribe has no TimeoutMs; the RPC deadline
    /// is [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::share_group_describe_timeout`].
    pub async fn share_group_describe(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<DescribedShareGroup>> {
        let timeout = self.cfg.request_timeout;
        self.share_group_describe_timeout(group_ids, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::share_group_describe`] with a one-shot RPC deadline (Java
    /// `DescribeShareGroupsOptions.timeoutMs`).
    ///
    /// ShareGroupDescribe has no TimeoutMs; `timeout` is the RPC deadline
    /// and the coordinator retry budget.
    pub async fn share_group_describe_timeout(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<DescribedShareGroup>> {
        let ids: Vec<String> = group_ids.iter().map(|s| (*s).to_string()).collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let version = self.share_group_describe_version.ok_or_else(|| {
            Error::Unsupported("broker does not support ShareGroupDescribe v0-1".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let mut out: Vec<Option<DescribedShareGroup>> = vec![None; ids.len()];
        let mut pending: Vec<usize> = (0..ids.len()).collect();
        loop {
            let by_node = self.group_coord_nodes(&ids, &pending).await?;
            let mut nodes: Vec<i32> = by_node.keys().copied().collect();
            nodes.sort_unstable();
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.get(&node).cloned().unwrap_or_default();
                match self
                    .share_group_describe_on_node(
                        node,
                        version,
                        &ids,
                        &idxs,
                        include_authorized_operations,
                        timeout,
                    )
                    .await
                {
                    Ok(done) => {
                        for (i, g) in done {
                            if error::coordinator_retriable(g.error_code) {
                                self.invalidate_group_coord_idxs(&ids, &[i], node);
                                still.push(i);
                            } else if let Some(slot) = out.get_mut(i) {
                                *slot = Some(g);
                            }
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        self.invalidate_group_coord_idxs(&ids, &idxs, node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
        }
        out.into_iter()
            .zip(ids)
            .map(|(g, id)| {
                g.ok_or_else(|| Error::protocol(format!("ShareGroupDescribe missing {id}")))
            })
            .collect()
    }

    /// Describe share groups (Java `Admin.describeShareGroups`).
    ///
    /// Same wire as [`Self::share_group_describe`]: ShareGroupDescribe
    /// api 77 v0–v1 on the group coordinator. Java's
    /// `DescribeShareGroupsHandler` uses `CoordinatorType.GROUP`. Empty
    /// input is a no-op. ShareGroupDescribe has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::describe_share_groups_timeout`].
    pub async fn describe_share_groups(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<DescribedShareGroup>> {
        let timeout = self.cfg.request_timeout;
        self.describe_share_groups_timeout(group_ids, include_authorized_operations, timeout)
            .await
    }

    /// [`Self::describe_share_groups`] with a one-shot RPC deadline (Java
    /// `DescribeShareGroupsOptions.timeoutMs`).
    ///
    /// ShareGroupDescribe has no TimeoutMs; `timeout` is the RPC deadline
    /// and the coordinator retry budget.
    pub async fn describe_share_groups_timeout(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<DescribedShareGroup>> {
        self.share_group_describe_timeout(group_ids, include_authorized_operations, timeout)
            .await
    }

    /// Describe KIP-932 share-group offsets (DescribeShareGroupOffsets
    /// api 90).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Official Apache JSON listeners are `broker` only. Official listed
    /// errors include `NOT_COORDINATOR` (16). Official Java
    /// `ListShareGroupOffsetsHandler` (the AdminClient handler for this
    /// RPC) uses `CoordinatorType.GROUP`. This is not a controller hop
    /// and not a partition-leader hop: there is no Metadata
    /// `controller_id` lookup, no `NOT_CONTROLLER` (41) retry, and no
    /// `NOT_LEADER_OR_FOLLOWER` (6) hop. SHARE (`key_type=2`) is the
    /// FindCoordinator v6 share-state key
    /// (`groupId:topicId:partition`) and is not used here.
    /// `COORDINATOR_LOAD_IN_PROGRESS` / `COORDINATOR_NOT_AVAILABLE` /
    /// `NOT_COORDINATOR` (16) refresh the coordinator and retry.
    /// Group-level ErrorCode is per-group after GroupId and Topics
    /// (bytes 8–9 on leftover-empty fixture group `"g"`), not
    /// top-level after throttle and not first-partition.
    /// Java `listShareGroupOffsets` is [`Self::list_share_group_offsets`].
    /// FindCoordinator v4+ CoordinatorKeys array of N (KIP-699): one
    /// FindCoordinator per retry for uncached groups. DescribeShareGroupOffsets
    /// is one RPC per coordinator. Brokers that only speak FindCoordinator
    /// v1–v3 get one FindCoordinator per uncached group. Empty input is
    /// a no-op. DescribeShareGroupOffsets has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::describe_share_group_offsets_timeout`].
    pub async fn describe_share_group_offsets(
        &mut self,
        groups: &[DescribeShareGroupOffsetsGroup],
    ) -> Result<Vec<DescribedShareGroupOffsets>> {
        let timeout = self.cfg.request_timeout;
        self.describe_share_group_offsets_timeout(groups, timeout)
            .await
    }

    /// [`Self::describe_share_group_offsets`] with a one-shot RPC deadline
    /// (Java `ListShareGroupOffsetsOptions.timeoutMs`).
    ///
    /// DescribeShareGroupOffsets has no TimeoutMs; `timeout` is the RPC
    /// deadline and the coordinator retry budget.
    pub async fn describe_share_group_offsets_timeout(
        &mut self,
        groups: &[DescribeShareGroupOffsetsGroup],
        timeout: Duration,
    ) -> Result<Vec<DescribedShareGroupOffsets>> {
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = groups.iter().map(|g| g.group_id.clone()).collect();
        let version = self.describe_share_group_offsets_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DescribeShareGroupOffsets".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let mut out: Vec<Option<DescribedShareGroupOffsets>> = vec![None; groups.len()];
        let mut pending: Vec<usize> = (0..groups.len()).collect();
        loop {
            let by_node = self.group_coord_nodes(&ids, &pending).await?;
            let mut nodes: Vec<i32> = by_node.keys().copied().collect();
            nodes.sort_unstable();
            let mut still = Vec::new();
            for node in nodes {
                let idxs = by_node.get(&node).cloned().unwrap_or_default();
                match self
                    .describe_share_group_offsets_on_node(node, version, groups, &idxs, timeout)
                    .await
                {
                    Ok(done) => {
                        for (i, g) in done {
                            if error::coordinator_retriable(g.error_code) {
                                self.invalidate_group_coord_idxs(&ids, &[i], node);
                                still.push(i);
                            } else if let Some(slot) = out.get_mut(i) {
                                *slot = Some(g);
                            }
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        self.invalidate_group_coord_idxs(&ids, &idxs, node);
                        still.extend(idxs);
                    }
                    Err(e) => return Err(e),
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
            self.wait_retry(&mut attempt, deadline).await?;
        }
        out.into_iter()
            .zip(ids)
            .map(|(g, id)| {
                g.ok_or_else(|| Error::protocol(format!("DescribeShareGroupOffsets missing {id}")))
            })
            .collect()
    }

    /// List share-group offsets (Java `Admin.listShareGroupOffsets`).
    ///
    /// Same wire as [`Self::describe_share_group_offsets`]:
    /// DescribeShareGroupOffsets api 90 on the group coordinator.
    /// Java's `ListShareGroupOffsetsHandler` sends that RPC. Empty
    /// input is a no-op. DescribeShareGroupOffsets has no TimeoutMs;
    /// the RPC deadline is [`AdminConfig::request_timeout`]. For a
    /// one-shot deadline, use [`Self::list_share_group_offsets_timeout`].
    pub async fn list_share_group_offsets(
        &mut self,
        groups: &[DescribeShareGroupOffsetsGroup],
    ) -> Result<Vec<DescribedShareGroupOffsets>> {
        let timeout = self.cfg.request_timeout;
        self.list_share_group_offsets_timeout(groups, timeout).await
    }

    /// [`Self::list_share_group_offsets`] with a one-shot RPC deadline (Java
    /// `ListShareGroupOffsetsOptions.timeoutMs`).
    ///
    /// DescribeShareGroupOffsets has no TimeoutMs; `timeout` is the RPC
    /// deadline and the coordinator retry budget.
    pub async fn list_share_group_offsets_timeout(
        &mut self,
        groups: &[DescribeShareGroupOffsetsGroup],
        timeout: Duration,
    ) -> Result<Vec<DescribedShareGroupOffsets>> {
        self.describe_share_group_offsets_timeout(groups, timeout)
            .await
    }

    /// Alter KIP-932 share-group offsets (AlterShareGroupOffsets
    /// api 91).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Official Apache JSON listeners are `broker` only. Official listed
    /// errors include `NOT_COORDINATOR` (16). Official Java
    /// `AlterShareGroupOffsetsHandler` uses `CoordinatorType.GROUP`.
    /// This is not a controller hop and not a partition-leader hop:
    /// there is no Metadata `controller_id` lookup, no
    /// `NOT_CONTROLLER` (41) retry, and no `NOT_LEADER_OR_FOLLOWER`
    /// (6) hop. SHARE (`key_type=2`) is the FindCoordinator v6
    /// share-state key (`groupId:topicId:partition`) and is not used
    /// here. `COORDINATOR_LOAD_IN_PROGRESS` /
    /// `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh
    /// the coordinator and retry. ErrorCode is top-level after
    /// throttle (bytes 4–5 on leftover-empty fixture group `"g"`),
    /// not first-group and not first-partition (bytes 31–32 when
    /// leftover-empty topic `"t"` partition `0` is present).
    /// AlterShareGroupOffsets has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::alter_share_group_offsets_timeout`].
    pub async fn alter_share_group_offsets(
        &mut self,
        group_id: &str,
        topics: &[AlterShareGroupOffsetsTopic],
    ) -> Result<AlteredShareGroupOffsets> {
        let timeout = self.cfg.request_timeout;
        self.alter_share_group_offsets_timeout(group_id, topics, timeout)
            .await
    }

    /// [`Self::alter_share_group_offsets`] with a one-shot RPC deadline (Java
    /// `AlterShareGroupOffsetsOptions.timeoutMs`).
    ///
    /// AlterShareGroupOffsets has no TimeoutMs; `timeout` is the RPC
    /// deadline and the coordinator retry budget.
    pub async fn alter_share_group_offsets_timeout(
        &mut self,
        group_id: &str,
        topics: &[AlterShareGroupOffsetsTopic],
        timeout: Duration,
    ) -> Result<AlteredShareGroupOffsets> {
        let coord_key = group_id.to_string();
        let version = self.alter_share_group_offsets_version.ok_or_else(|| {
            Error::Unsupported("broker does not support AlterShareGroupOffsets".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let stale = self
                .group_coord
                .as_ref()
                .is_none_or(|(g, _)| g != &coord_key);
            if stale {
                let node = self.discover_group_coord(&coord_key).await?;
                self.group_coord = Some((coord_key.clone(), node));
            }
            let node = self
                .group_coord
                .as_ref()
                .map(|(_, n)| *n)
                .ok_or_else(|| Error::protocol("missing group coordinator"))?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing alter_share_group_offsets conn"))?;
                conn.roundtrip(
                    ALTER_SHARE_GROUP_OFFSETS,
                    version,
                    |buf| encode_alter_share_group_offsets_request(buf, group_id, topics),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.group_coord = None;
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let result = decode_alter_share_group_offsets_response(&mut body.clone())?;
            if error::coordinator_retriable(result.error_code) {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(result);
        }
    }

    /// Delete KIP-932 share-group offsets (DeleteShareGroupOffsets
    /// api 92).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Official Apache JSON listeners are `broker` only. Official listed
    /// errors include `NOT_COORDINATOR` (16). Official Java
    /// `DeleteShareGroupOffsetsHandler` uses `CoordinatorType.GROUP`.
    /// This is not a controller hop and not a partition-leader hop:
    /// there is no Metadata `controller_id` lookup, no
    /// `NOT_CONTROLLER` (41) retry, and no `NOT_LEADER_OR_FOLLOWER`
    /// (6) hop. SHARE (`key_type=2`) is the FindCoordinator v6
    /// share-state key (`groupId:topicId:partition`) and is not used
    /// here. `COORDINATOR_LOAD_IN_PROGRESS` /
    /// `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh
    /// the coordinator and retry. ErrorCode is top-level after
    /// throttle (bytes 4–5 on leftover-empty fixture group `"g"`),
    /// not first-group and not first-topic (bytes 26–27 when
    /// leftover-empty topic `"t"` is present). Official request topics
    /// are names only — no partitions. DeleteShareGroupOffsets has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use
    /// [`Self::delete_share_group_offsets_timeout`].
    pub async fn delete_share_group_offsets(
        &mut self,
        group_id: &str,
        topics: &[DeleteShareGroupOffsetsTopic],
    ) -> Result<DeletedShareGroupOffsets> {
        let timeout = self.cfg.request_timeout;
        self.delete_share_group_offsets_timeout(group_id, topics, timeout)
            .await
    }

    /// [`Self::delete_share_group_offsets`] with a one-shot RPC deadline
    /// (Java `DeleteShareGroupOffsetsOptions.timeoutMs`).
    ///
    /// DeleteShareGroupOffsets has no TimeoutMs; `timeout` is the RPC
    /// deadline and the coordinator retry budget.
    pub async fn delete_share_group_offsets_timeout(
        &mut self,
        group_id: &str,
        topics: &[DeleteShareGroupOffsetsTopic],
        timeout: Duration,
    ) -> Result<DeletedShareGroupOffsets> {
        let coord_key = group_id.to_string();
        let version = self.delete_share_group_offsets_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DeleteShareGroupOffsets".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let stale = self
                .group_coord
                .as_ref()
                .is_none_or(|(g, _)| g != &coord_key);
            if stale {
                let node = self.discover_group_coord(&coord_key).await?;
                self.group_coord = Some((coord_key.clone(), node));
            }
            let node = self
                .group_coord
                .as_ref()
                .map(|(_, n)| *n)
                .ok_or_else(|| Error::protocol("missing group coordinator"))?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing delete_share_group_offsets conn"))?;
                conn.roundtrip(
                    DELETE_SHARE_GROUP_OFFSETS,
                    version,
                    |buf| encode_delete_share_group_offsets_request(buf, group_id, topics),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.group_coord = None;
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let result = decode_delete_share_group_offsets_response(&mut body.clone())?;
            if error::coordinator_retriable(result.error_code) {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(result);
        }
    }

    /// Describe topic partitions (DescribeTopicPartitions api 75,
    /// KIP-966).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` only. Official JSON lists no
    /// `errorCodes`. Official Java
    /// `DescribeTopicPartitionsRequestHandler` answers from the broker
    /// `MetadataCache`. `NOT_COORDINATOR` (16) is not listed. This is
    /// not a group-coordinator hop, not a controller hop, and not a
    /// partition-leader hop: there is no FindCoordinator, no Metadata
    /// `controller_id` lookup, no `NOT_CONTROLLER` (41) retry, and no
    /// `NOT_LEADER_OR_FOLLOWER` (6) hop. ErrorCode is first-topic after
    /// throttle and the compact topics length (bytes 5–6 on leftover-
    /// empty fixture topic `"t"`), not top-level after throttle.
    /// First-partition ErrorCode is at bytes 27–28 when leftover-empty
    /// partition `0` is present and is not the first ErrorCode.
    /// DescribeTopicPartitions has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_topic_partitions_timeout`]. Java `describeTopics`
    /// is [`Self::describe_topics_timeout`].
    pub async fn describe_topic_partitions(
        &mut self,
        topics: &[&str],
        response_partition_limit: i32,
        cursor: Option<&TopicPartitionCursor>,
    ) -> Result<DescribeTopicPartitionsResponse> {
        let timeout = self.cfg.request_timeout;
        self.describe_topic_partitions_timeout(topics, response_partition_limit, cursor, timeout)
            .await
    }

    /// [`Self::describe_topic_partitions`] with a one-shot RPC deadline.
    ///
    /// DescribeTopicPartitions has no TimeoutMs; `timeout` is the RPC
    /// deadline. Java `Admin.describeTopics` uses
    /// [`Self::describe_topics_timeout`]. This overload is the crate-first
    /// raw DescribeTopicPartitions (api 75) path.
    pub async fn describe_topic_partitions_timeout(
        &mut self,
        topics: &[&str],
        response_partition_limit: i32,
        cursor: Option<&TopicPartitionCursor>,
        timeout: Duration,
    ) -> Result<DescribeTopicPartitionsResponse> {
        let names: Vec<String> = topics.iter().map(|s| (*s).to_string()).collect();
        self.describe_topic_partitions_once(&names, response_partition_limit, cursor, timeout)
            .await
    }

    async fn describe_topic_partitions_once(
        &mut self,
        names: &[String],
        response_partition_limit: i32,
        cursor: Option<&TopicPartitionCursor>,
        timeout: Duration,
    ) -> Result<DescribeTopicPartitionsResponse> {
        let version = self.describe_topic_partitions_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DescribeTopicPartitions".into())
        })?;
        let body = self
            .roundtrip_bootstrap(
                DESCRIBE_TOPIC_PARTITIONS,
                version,
                |buf| {
                    encode_describe_topic_partitions_request(
                        buf,
                        names,
                        response_partition_limit,
                        cursor,
                    )
                },
                timeout,
            )
            .await?;
        decode_describe_topic_partitions_response(&mut body.clone())
    }

    async fn describe_topics_dtp(
        &mut self,
        names: &[String],
        include_authorized_operations: bool,
        response_partition_limit: i32,
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        let deadline = Instant::now() + timeout;
        let mut cursor: Option<TopicPartitionCursor> = None;
        let mut out: Vec<TopicDescription> = Vec::new();
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let resp = self
                .describe_topic_partitions_once(
                    names,
                    response_partition_limit,
                    cursor.as_ref(),
                    timeout,
                )
                .await?;
            for t in &resp.topics {
                let desc = topic_description_from_dtp(t, include_authorized_operations);
                if let Some(existing) = out.iter_mut().find(|d| d.name == desc.name) {
                    existing.partitions.extend(desc.partitions);
                } else {
                    out.push(desc);
                }
            }
            match resp.next_cursor {
                Some(next) if cursor.as_ref() != Some(&next) => cursor = Some(next),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn describe_topics_metadata(
        &mut self,
        names: &[String],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<TopicDescription>> {
        let owned: Vec<MetadataRequestTopic> = names
            .iter()
            .map(|name| MetadataRequestTopic::by_name(name.clone()))
            .collect();
        let md = self
            .fetch_metadata_request_with(Some(&owned), include_authorized_operations, timeout)
            .await?;
        Ok(topic_descriptions_for_names(&md, names))
    }

    /// List configuration resources (ListConfigResources api 74,
    /// KIP-1142; formerly ListClientMetricsResources).
    ///
    /// Java `listClientMetricsResources` is
    /// [`Self::list_client_metrics_resources`] (CLIENT_METRICS only).
    ///
    /// Lands on the connected broker (bootstrap is fine). Negotiates
    /// ListConfigResources v0–v1 (flexible from v0). Kafka 4.0 api 74
    /// is ListClientMetricsResources v0 only (empty request; response
    /// names, ResourceType decode-fills `CLIENT_METRICS`). v1 adds
    /// ResourceTypes / ResourceType (KIP-1142). Official
    /// Apache JSON listeners are `broker` only. Official JSON lists no
    /// `errorCodes`. Official Java `KafkaApis.handleListConfigResources`
    /// answers from the connected broker. Official Java
    /// `ListConfigResourcesRequest.getErrorResponse` writes the
    /// exception onto the top-level ErrorCode.
    /// `CLUSTER_AUTHORIZATION_FAILED` (31) and `UNSUPPORTED_VERSION`
    /// (35) are handler-observed. `NOT_COORDINATOR` (16) is not listed.
    /// This is not a group-coordinator hop, not a controller hop, and
    /// not a partition-leader hop: there is no FindCoordinator, no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41) retry,
    /// and no `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level `error_code`
    /// is the INT16 at bytes 4–5, after throttle — not a first-resource
    /// field. Resources have no ErrorCode.
    ///
    /// `resource_types` is [`ConfigResourceType`] or a protocol `i8`
    /// (`CONFIG_RESOURCE_TOPIC`, …). Empty is Java `listConfigResources()`
    /// ([`Self::list_config_resources_all`]). ListConfigResources has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use [`Self::list_config_resources_timeout`].
    pub async fn list_config_resources(
        &mut self,
        resource_types: impl IntoIterator<Item = impl Into<i8>>,
    ) -> Result<Vec<ListedConfigResource>> {
        let timeout = self.cfg.request_timeout;
        self.list_config_resources_timeout(resource_types, timeout)
            .await
    }

    /// [`Self::list_config_resources`] with a one-shot RPC deadline (Java
    /// `ListConfigResourcesOptions.timeoutMs`).
    ///
    /// ListConfigResources has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn list_config_resources_timeout(
        &mut self,
        resource_types: impl IntoIterator<Item = impl Into<i8>>,
        timeout: Duration,
    ) -> Result<Vec<ListedConfigResource>> {
        let types: Vec<i8> = resource_types.into_iter().map(Into::into).collect();
        let version = self.list_config_resources_version.ok_or_else(|| {
            Error::Unsupported("broker does not support ListConfigResources v0-1".into())
        })?;
        let body = self
            .roundtrip_bootstrap(
                LIST_CONFIG_RESOURCES,
                version,
                |buf| encode_list_config_resources_request(buf, version, &types),
                timeout,
            )
            .await?;
        let resp = decode_list_config_resources_response(&mut body.clone(), version)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ListConfigResources"));
        }
        Ok(resp.config_resources)
    }

    /// List every configuration resource (Java `listConfigResources()`).
    ///
    /// Same wire as [`Self::list_config_resources`] with an empty
    /// ResourceTypes array (Java `Set.of()`). Kafka 4.1 Admin; Kafka 4.0
    /// has [`Self::list_client_metrics_resources`] only.
    pub async fn list_config_resources_all(&mut self) -> Result<Vec<ListedConfigResource>> {
        self.list_config_resources(std::iter::empty::<i8>()).await
    }

    /// [`Self::list_config_resources_all`] with a one-shot RPC deadline
    /// (Java `ListConfigResourcesOptions.timeoutMs`).
    ///
    /// ListConfigResources has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn list_config_resources_all_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ListedConfigResource>> {
        self.list_config_resources_timeout(std::iter::empty::<i8>(), timeout)
            .await
    }

    /// List client-metrics resources (Java
    /// `Admin.listClientMetricsResources`).
    ///
    /// Same wire as [`Self::list_config_resources`] with
    /// [`ConfigResourceType::ClientMetrics`]. Java 4.0 implements the
    /// deprecated `listClientMetricsResources` as ListConfigResources
    /// for that type. ListConfigResources has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::list_client_metrics_resources_timeout`].
    pub async fn list_client_metrics_resources(&mut self) -> Result<Vec<ListedConfigResource>> {
        self.list_config_resources([ConfigResourceType::ClientMetrics])
            .await
    }

    /// [`Self::list_client_metrics_resources`] with a one-shot RPC deadline
    /// (Java `ListClientMetricsResourcesOptions.timeoutMs`).
    ///
    /// ListConfigResources has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn list_client_metrics_resources_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ListedConfigResource>> {
        self.list_config_resources_timeout([ConfigResourceType::ClientMetrics], timeout)
            .await
    }

    /// Get client telemetry subscriptions (GetTelemetrySubscriptions
    /// api 71, KIP-714).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` only. Official JSON lists no
    /// `errorCodes`. Official Java
    /// `KafkaApis.handleGetTelemetrySubscriptionsRequest` answers from
    /// the connected broker (`clientMetricsManager`). Official Java
    /// `GetTelemetrySubscriptionsRequest.getErrorResponse` writes the
    /// exception onto the top-level ErrorCode. `INVALID_REQUEST` (42),
    /// `UNSUPPORTED_VERSION` (35), and `THROTTLING_QUOTA_EXCEEDED` (89)
    /// are handler-observed. `NOT_COORDINATOR` (16) is not listed.
    /// This is not a group-coordinator hop, not a controller hop, and
    /// not a partition-leader hop: there is no FindCoordinator, no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41) retry,
    /// and no `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level `error_code`
    /// is the INT16 at bytes 4–5, after throttle — not a first-
    /// subscription field and not a first-metric field.
    pub async fn get_telemetry_subscriptions(
        &mut self,
        client_instance_id: [u8; 16],
    ) -> Result<GetTelemetrySubscriptionsResponse> {
        let version = self.get_telemetry_subscriptions_version.ok_or_else(|| {
            Error::Unsupported("broker does not support GetTelemetrySubscriptions".into())
        })?;
        let timeout = self.cfg.request_timeout;
        let body = self
            .roundtrip_bootstrap(
                GET_TELEMETRY_SUBSCRIPTIONS,
                version,
                |buf| encode_get_telemetry_subscriptions_request(buf, &client_instance_id),
                timeout,
            )
            .await?;
        let resp = decode_get_telemetry_subscriptions_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "GetTelemetrySubscriptions"));
        }
        Ok(resp)
    }

    /// Push client telemetry metrics (PushTelemetry api 72, KIP-714).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` only. Official JSON lists no
    /// `errorCodes`. Official Java
    /// `KafkaApis.handlePushTelemetryRequest` answers from the
    /// connected broker (`clientMetricsManager`). Official Java
    /// `PushTelemetryRequest.getErrorResponse` writes the exception
    /// onto the top-level ErrorCode. `INVALID_REQUEST` (42),
    /// `UNKNOWN_SUBSCRIPTION_ID` (117), `THROTTLING_QUOTA_EXCEEDED`
    /// (89), `UNSUPPORTED_COMPRESSION_TYPE` (76),
    /// `TELEMETRY_TOO_LARGE` (118), and `INVALID_RECORD` (87) are
    /// handler-observed. `NOT_COORDINATOR` (16) is not listed.
    /// This is not a group-coordinator hop, not a controller hop, and
    /// not a partition-leader hop: there is no FindCoordinator, no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41) retry,
    /// and no `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level `error_code`
    /// is the INT16 at bytes 4–5, after throttle — not a first-metric
    /// field and not a first-payload field.
    pub async fn push_telemetry(
        &mut self,
        client_instance_id: [u8; 16],
        subscription_id: i32,
        terminating: bool,
        compression_type: i8,
        metrics: &[u8],
    ) -> Result<PushTelemetryResponse> {
        let version = self
            .push_telemetry_version
            .ok_or_else(|| Error::Unsupported("broker does not support PushTelemetry".into()))?;
        let timeout = self.cfg.request_timeout;
        let req = PushTelemetryRequest::new(
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics.to_vec(),
        );
        let body = self
            .roundtrip_bootstrap(
                PUSH_TELEMETRY,
                version,
                |buf| encode_push_telemetry_request(buf, &req),
                timeout,
            )
            .await?;
        let resp = decode_push_telemetry_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "PushTelemetry"));
        }
        Ok(resp)
    }

    /// Assign replicas to log directories (AssignReplicasToDirs api 73,
    /// KIP-858).
    ///
    /// Lands on the Metadata controller. Official Apache JSON listeners
    /// are `controller` only. Official JSON lists no `errorCodes`.
    /// Official Java `AssignReplicasToDirsRequest.getErrorResponse`
    /// writes `Errors.forException(e).code()` onto the top-level
    /// ErrorCode. Official Java `QuorumController.assignReplicasToDirs`
    /// is an `appendWriteEvent`; `ControllerWriteEvent.run` throws
    /// `NotControllerException` when the node is not the active
    /// controller, which `getErrorResponse` writes as
    /// `NOT_CONTROLLER` (41). `NOT_CONTROLLER` (41) refreshes Metadata
    /// and retries on the new controller. Official
    /// `ReplicationControlManager.handleAssignReplicasToDirs` does not
    /// write `NOT_COORDINATOR` (16); per-partition
    /// `NOT_LEADER_OR_FOLLOWER` (6) is a handler code when the broker
    /// is not a replica, not a partition-leader hop. This is not a
    /// FindCoordinator hop and has no `key_type`. Top-level
    /// `error_code` is the INT16 at bytes 4–5, after throttle — not a
    /// first-directory field and not a first-partition field. Fixture
    /// broker id/epoch and directory UUIDs only; this is not a replica
    /// directory store. AssignReplicasToDirs has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::assign_replicas_to_dirs_timeout`].
    pub async fn assign_replicas_to_dirs(
        &mut self,
        broker_id: i32,
        broker_epoch: i64,
        directories: Vec<AssignReplicasToDirsDirectory>,
    ) -> Result<AssignReplicasToDirsResponse> {
        let timeout = self.cfg.request_timeout;
        self.assign_replicas_to_dirs_timeout(broker_id, broker_epoch, directories, timeout)
            .await
    }

    /// [`Self::assign_replicas_to_dirs`] with a one-shot RPC deadline (Java
    /// `AssignReplicasToDirsOptions.timeoutMs`).
    ///
    /// AssignReplicasToDirs has no TimeoutMs; `timeout` is the RPC deadline
    /// and the `NOT_CONTROLLER` retry budget.
    pub async fn assign_replicas_to_dirs_timeout(
        &mut self,
        broker_id: i32,
        broker_epoch: i64,
        directories: Vec<AssignReplicasToDirsDirectory>,
        timeout: Duration,
    ) -> Result<AssignReplicasToDirsResponse> {
        let version = self.assign_replicas_to_dirs_version.ok_or_else(|| {
            Error::Unsupported("broker does not support AssignReplicasToDirs".into())
        })?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let req = AssignReplicasToDirsRequest::new(broker_id, broker_epoch, directories);
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing assign_replicas_to_dirs conn"))?;
                conn.roundtrip(
                    ASSIGN_REPLICAS_TO_DIRS,
                    version,
                    |buf| encode_assign_replicas_to_dirs_request(buf, &req),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_assign_replicas_to_dirs_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "AssignReplicasToDirs"));
            }
            return Ok(resp);
        }
    }

    /// Alter replica log directories (AlterReplicaLogDirs api 34,
    /// KIP-113; v1–v2, classic at v1, flexible from v2).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` only. Official JSON lists no
    /// `errorCodes`. Official Java
    /// `KafkaApis.handleAlterReplicaLogDirsRequest` answers from the
    /// connected broker (`replicaManager.alterReplicaLogDirs`).
    /// Official Java `AlterReplicaLogDirsRequest.getErrorResponse`
    /// writes the exception onto each partition ErrorCode (no
    /// top-level field). `CLUSTER_AUTHORIZATION_FAILED` (31),
    /// `INVALID_TOPIC_EXCEPTION` (17), `KAFKA_STORAGE_ERROR` (56),
    /// `INVALID_REPLICA_ASSIGNMENT` (39), `LOG_DIR_NOT_FOUND` (57),
    /// and `REPLICA_NOT_AVAILABLE` (9) (handler converts
    /// `NotLeaderOrFollowerException` to 9) are handler-observed.
    /// `NOT_COORDINATOR` (16) is not listed. `NOT_CONTROLLER` (41) is
    /// not listed. This is not a group-coordinator hop, not a
    /// controller hop, and not a partition-leader hop: there is no
    /// FindCoordinator, no Metadata `controller_id` lookup, no
    /// `NOT_CONTROLLER` (41) retry, and no `NOT_LEADER_OR_FOLLOWER`
    /// (6) hop. First-partition `error_code` is the INT16 at bytes
    /// 12–13 on leftover-empty **v2** fixture topic `"t"` partition `0`
    /// — not a top-level field after throttle. Classic **v1** places
    /// that ErrorCode later (bytes 19–20 on the same fixture). Fixture
    /// directory path and topic/partition indexes only; this is not a
    /// log-dir store. AlterReplicaLogDirs has no TimeoutMs; the RPC
    /// deadline is [`AdminConfig::request_timeout`]. For a one-shot
    /// deadline, use [`Self::alter_replica_log_dirs_timeout`]. Optional at
    /// [`Self::new`] (Kafka 1.1+ / KIP-113); a broker that omits api 34
    /// returns [`Error::Unsupported`].
    pub async fn alter_replica_log_dirs(
        &mut self,
        dirs: Vec<AlterReplicaLogDirsDirectory>,
    ) -> Result<AlterReplicaLogDirsResponse> {
        let timeout = self.cfg.request_timeout;
        self.alter_replica_log_dirs_timeout(dirs, timeout).await
    }

    /// [`Self::alter_replica_log_dirs`] with a one-shot RPC deadline (Java
    /// `AlterReplicaLogDirsOptions.timeoutMs`).
    ///
    /// AlterReplicaLogDirs has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn alter_replica_log_dirs_timeout(
        &mut self,
        dirs: Vec<AlterReplicaLogDirsDirectory>,
        timeout: Duration,
    ) -> Result<AlterReplicaLogDirsResponse> {
        let version = self.alter_replica_log_dirs_version.ok_or_else(|| {
            Error::Unsupported("broker does not support AlterReplicaLogDirs".into())
        })?;
        let req = AlterReplicaLogDirsRequest::new(dirs);
        let body = self
            .roundtrip_bootstrap(
                ALTER_REPLICA_LOG_DIRS,
                version,
                |buf| encode_alter_replica_log_dirs_request(buf, version, &req),
                timeout,
            )
            .await?;
        decode_alter_replica_log_dirs_response(&mut body.clone(), version)
    }

    /// Describe log directories (DescribeLogDirs api 35, KIP-113 /
    /// KIP-784 / KIP-827; v1–v4, classic at v1, flexible from v2,
    /// top-level ErrorCode v3+, TotalBytes / UsableBytes v4).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` only. Official JSON lists no
    /// `errorCodes`. Official Java
    /// `KafkaApis.handleDescribeLogDirsRequest` answers from the
    /// connected broker (`replicaManager.describeLogDirs`). Auth
    /// failure writes `CLUSTER_AUTHORIZATION_FAILED` (31) onto the
    /// top-level ErrorCode (v3+; v1–v2 omit that field and decode
    /// fills `0`). Official `ReplicaManager.describeLogDirs`
    /// writes `KAFKA_STORAGE_ERROR` (56) onto a first-directory
    /// ErrorCode when that dir is offline. `NOT_COORDINATOR` (16) is
    /// not listed. `NOT_CONTROLLER` (41) is not listed. This is not a
    /// group-coordinator hop, not a controller hop, and not a
    /// partition-leader hop: there is no FindCoordinator, no Metadata
    /// `controller_id` lookup, no `NOT_CONTROLLER` (41) retry, and no
    /// `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level `error_code` is the
    /// INT16 at bytes 4–5 on leftover-empty **v3+**, after throttle —
    /// not a first-directory field and not a first-partition field.
    /// Fixture directory path and topic/partition indexes only; this
    /// is not a log-dir store. v5 is a named STATUS hole and is not
    /// spoken. Java `describeLogDirs(Collection<Integer>)` is
    /// [`Self::describe_broker_log_dirs`]. DescribeLogDirs has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use [`Self::describe_log_dirs_timeout`].
    /// Optional at [`Self::new`] (Kafka 1.0+ / KIP-113); a broker that
    /// omits api 35 returns [`Error::Unsupported`].
    pub async fn describe_log_dirs(
        &mut self,
        topics: Option<Vec<DescribableLogDirTopic>>,
    ) -> Result<DescribeLogDirsResponse> {
        let timeout = self.cfg.request_timeout;
        self.describe_log_dirs_timeout(topics, timeout).await
    }

    /// [`Self::describe_log_dirs`] with a one-shot RPC deadline (Java
    /// `DescribeLogDirsOptions.timeoutMs`).
    ///
    /// DescribeLogDirs has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn describe_log_dirs_timeout(
        &mut self,
        topics: Option<Vec<DescribableLogDirTopic>>,
        timeout: Duration,
    ) -> Result<DescribeLogDirsResponse> {
        let version = self
            .describe_log_dirs_version
            .ok_or_else(|| Error::Unsupported("broker does not support DescribeLogDirs".into()))?;
        let req = DescribeLogDirsRequest::new(topics);
        let body = self
            .roundtrip_bootstrap(
                DESCRIBE_LOG_DIRS,
                version,
                |buf| encode_describe_log_dirs_request(buf, version, &req),
                timeout,
            )
            .await?;
        decode_describe_log_dirs_response(&mut body.clone(), version)
    }

    /// Replica log directories (Java `Admin.describeReplicaLogDirs`).
    ///
    /// Groups replicas by [`TopicPartitionReplica::broker_id`] and
    /// sends DescribeLogDirs (api 35) to each replica's broker. Empty
    /// input is a no-op. This is not [`Self::describe_log_dirs`]
    /// (bootstrap only) and not a controller or partition-leader hop.
    /// Replicas missing from the response get [`ReplicaLogDirInfo::unknown`].
    /// DescribeLogDirs has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_replica_log_dirs_timeout`].
    pub async fn describe_replica_log_dirs(
        &mut self,
        replicas: impl IntoIterator<Item = TopicPartitionReplica>,
    ) -> Result<Vec<(TopicPartitionReplica, ReplicaLogDirInfo)>> {
        let timeout = self.cfg.request_timeout;
        self.describe_replica_log_dirs_timeout(replicas, timeout)
            .await
    }

    /// [`Self::describe_replica_log_dirs`] with a one-shot RPC deadline
    /// (Java `describeReplicaLogDirs` plus `DescribeLogDirsOptions.timeoutMs`).
    ///
    /// DescribeLogDirs has no TimeoutMs; `timeout` is the RPC deadline
    /// for each replica-broker hop.
    pub async fn describe_replica_log_dirs_timeout(
        &mut self,
        replicas: impl IntoIterator<Item = TopicPartitionReplica>,
        timeout: Duration,
    ) -> Result<Vec<(TopicPartitionReplica, ReplicaLogDirInfo)>> {
        let replicas: Vec<TopicPartitionReplica> = replicas.into_iter().collect();
        if replicas.is_empty() {
            return Ok(Vec::new());
        }
        let mut infos: HashMap<(String, i32, i32), ReplicaLogDirInfo> = HashMap::new();
        for broker_id in replica_broker_ids(&replicas) {
            self.ensure_broker(broker_id).await?;
            let topics = describable_topics_for_broker(&replicas, broker_id);
            let resp = self
                .describe_log_dirs_on(broker_id, Some(topics), timeout)
                .await?;
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "DescribeLogDirs"));
            }
            for r in replicas.iter().filter(|r| r.broker_id == broker_id) {
                let info = replica_log_dir_info_from(r, &resp.results);
                let _ = infos.insert((r.topic.clone(), r.partition, r.broker_id), info);
            }
        }
        let mut out = Vec::with_capacity(replicas.len());
        for r in replicas {
            let info = infos
                .get(&(r.topic.clone(), r.partition, r.broker_id))
                .cloned()
                .unwrap_or_else(ReplicaLogDirInfo::unknown);
            out.push((r, info));
        }
        Ok(out)
    }

    /// Log directories on these brokers (Java
    /// `Admin.describeLogDirs(Collection<Integer>)`).
    ///
    /// Sends DescribeLogDirs with a null topic array (all dirs) to
    /// each broker. Duplicate ids are sent once, in first-seen order.
    /// Empty input is a no-op. Unknown broker ids refresh Metadata
    /// then fail. This is not [`Self::describe_log_dirs`] (bootstrap,
    /// optional topic filter) and not
    /// [`Self::describe_replica_log_dirs`]. DescribeLogDirs has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use
    /// [`Self::describe_broker_log_dirs_timeout`].
    pub async fn describe_broker_log_dirs(
        &mut self,
        brokers: impl IntoIterator<Item = i32>,
    ) -> Result<Vec<(i32, DescribeLogDirsResponse)>> {
        let timeout = self.cfg.request_timeout;
        self.describe_broker_log_dirs_timeout(brokers, timeout)
            .await
    }

    /// [`Self::describe_broker_log_dirs`] with a one-shot RPC deadline
    /// (Java `describeLogDirs` plus `DescribeLogDirsOptions.timeoutMs`).
    ///
    /// DescribeLogDirs has no TimeoutMs; `timeout` is the RPC deadline
    /// for each broker hop.
    pub async fn describe_broker_log_dirs_timeout(
        &mut self,
        brokers: impl IntoIterator<Item = i32>,
        timeout: Duration,
    ) -> Result<Vec<(i32, DescribeLogDirsResponse)>> {
        let mut ids = Vec::new();
        for id in brokers {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(ids.len());
        for broker_id in ids {
            self.ensure_broker(broker_id).await?;
            let resp = self.describe_log_dirs_on(broker_id, None, timeout).await?;
            out.push((broker_id, resp));
        }
        Ok(out)
    }

    async fn describe_log_dirs_on(
        &mut self,
        node: i32,
        topics: Option<Vec<DescribableLogDirTopic>>,
        timeout: Duration,
    ) -> Result<DescribeLogDirsResponse> {
        let version = self
            .describe_log_dirs_version
            .ok_or_else(|| Error::Unsupported("broker does not support DescribeLogDirs".into()))?;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let req = DescribeLogDirsRequest::new(topics);
        loop {
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing describe_log_dirs conn"))?;
                conn.roundtrip(
                    DESCRIBE_LOG_DIRS,
                    version,
                    |buf| encode_describe_log_dirs_request(buf, version, &req),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            return decode_describe_log_dirs_response(&mut body.clone(), version);
        }
    }

    async fn ensure_broker(&mut self, node: i32) -> Result<()> {
        if !self.cluster.brokers.contains_key(&node) {
            self.refresh_metadata(None).await?;
        }
        if self.cluster.brokers.contains_key(&node) {
            Ok(())
        } else {
            Err(Error::protocol(format!("unknown broker {node}")))
        }
    }

    /// Create a delegation token (CreateDelegationToken api 38, KIP-48 /
    /// KIP-373; v1–v3, classic at v1, flexible from v2, owner /
    /// requester principals v3).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` and `controller`. Official
    /// JSON lists no `errorCodes`. Official Java
    /// `KafkaApis.handleCreateTokenRequest` validates the connection
    /// then `forwardToController` (broker-side envelope, not a client
    /// hop). Official Java `KafkaAdminClient.createDelegationToken`
    /// uses `LeastLoadedNodeProvider`. Official handler writes
    /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) onto the top-level
    /// ErrorCode when the channel is not allowed. `NOT_COORDINATOR`
    /// (16) is not listed. `NOT_CONTROLLER` (41) is not listed. This
    /// is not a group-coordinator hop, not a controller hop, and not
    /// a partition-leader hop: there is no FindCoordinator, no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41)
    /// retry, and no `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level
    /// `error_code` is the INT16 at bytes 0–1, first field — not after
    /// throttle, not a first-renewer field, and not a first-token
    /// field. Fixture principal / lifetime only; this is not a token
    /// store. Kafka 4.0 `validVersions` is `1-3`. This crate speaks
    /// 1–3. v0 and v4+ are not spoken. v1–v2 omit owner on the wire
    /// (decode fills `None`) and omit requester (decode fills empty).
    /// CreateDelegationToken has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::create_delegation_token_timeout`]. Optional at [`Self::new`]
    /// (Kafka 1.1+ / KIP-48); a broker that omits api 38 returns
    /// [`Error::Unsupported`].
    pub async fn create_delegation_token(
        &mut self,
        req: CreateDelegationTokenRequest,
    ) -> Result<CreateDelegationTokenResponse> {
        let timeout = self.cfg.request_timeout;
        self.create_delegation_token_timeout(req, timeout).await
    }

    /// [`Self::create_delegation_token`] with a one-shot RPC deadline (Java
    /// `CreateDelegationTokenOptions.timeoutMs`).
    ///
    /// CreateDelegationToken has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn create_delegation_token_timeout(
        &mut self,
        req: CreateDelegationTokenRequest,
        timeout: Duration,
    ) -> Result<CreateDelegationTokenResponse> {
        let version = self.create_delegation_token_version.ok_or_else(|| {
            Error::Unsupported("broker does not support CreateDelegationToken".into())
        })?;
        let body = self
            .roundtrip_bootstrap(
                CREATE_DELEGATION_TOKEN,
                version,
                |buf| encode_create_delegation_token_request(buf, version, &req),
                timeout,
            )
            .await?;
        decode_create_delegation_token_response(&mut body.clone(), version)
    }

    /// Create a delegation token with default options (Java
    /// `Admin.createDelegationToken()`).
    ///
    /// Same wire as [`Self::create_delegation_token`] with
    /// [`CreateDelegationTokenRequest::default`] (request principal,
    /// empty renewers, `max_lifetime_ms = -1`).
    pub async fn create_delegation_token_default(
        &mut self,
    ) -> Result<CreateDelegationTokenResponse> {
        let timeout = self.cfg.request_timeout;
        self.create_delegation_token_default_timeout(timeout).await
    }

    /// [`Self::create_delegation_token_default`] with a one-shot RPC
    /// deadline (Java `CreateDelegationTokenOptions.timeoutMs`).
    pub async fn create_delegation_token_default_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<CreateDelegationTokenResponse> {
        self.create_delegation_token_timeout(CreateDelegationTokenRequest::default(), timeout)
            .await
    }

    /// Renew a delegation token (RenewDelegationToken api 39, KIP-48 /
    /// KIP-373; v1–v2, classic at v1, flexible from v2).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` and `controller`. Official
    /// JSON lists no `errorCodes`. Official Java
    /// `KafkaApis.handleRenewTokenRequest` validates the connection
    /// then `forwardToController` (broker-side envelope, not a client
    /// hop). Official Java `KafkaAdminClient.renewDelegationToken`
    /// uses `LeastLoadedNodeProvider`. Official handler writes
    /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) onto the top-level
    /// ErrorCode when the channel is not allowed. `NOT_COORDINATOR`
    /// (16) is not listed. `NOT_CONTROLLER` (41) is not listed. This
    /// is not a group-coordinator hop, not a controller hop, and not
    /// a partition-leader hop: there is no FindCoordinator, no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41)
    /// retry, and no `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level
    /// `error_code` is the INT16 at bytes 0–1, first field — not after
    /// throttle and not a first-token field. Fixture hmac / period
    /// only; this is not a token store. Kafka 4.0 `validVersions` is
    /// `1-2`. This crate speaks 1–2. v0 and v3+ are not spoken. Same
    /// fields on v1 and v2. Do not copy CreateDelegationToken just
    /// because it is the previous slice. RenewDelegationToken has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use [`Self::renew_delegation_token_timeout`].
    /// Optional at [`Self::new`] (Kafka 1.1+ / KIP-48); a broker that
    /// omits api 39 returns [`Error::Unsupported`].
    pub async fn renew_delegation_token(
        &mut self,
        req: RenewDelegationTokenRequest,
    ) -> Result<RenewDelegationTokenResponse> {
        let timeout = self.cfg.request_timeout;
        self.renew_delegation_token_timeout(req, timeout).await
    }

    /// [`Self::renew_delegation_token`] with a one-shot RPC deadline (Java
    /// `RenewDelegationTokenOptions.timeoutMs`).
    ///
    /// RenewDelegationToken has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn renew_delegation_token_timeout(
        &mut self,
        req: RenewDelegationTokenRequest,
        timeout: Duration,
    ) -> Result<RenewDelegationTokenResponse> {
        let version = self.renew_delegation_token_version.ok_or_else(|| {
            Error::Unsupported("broker does not support RenewDelegationToken".into())
        })?;
        let body = self
            .roundtrip_bootstrap(
                RENEW_DELEGATION_TOKEN,
                version,
                |buf| encode_renew_delegation_token_request(buf, version, &req),
                timeout,
            )
            .await?;
        decode_renew_delegation_token_response(&mut body.clone(), version)
    }

    /// Renew a delegation token by HMAC (Java
    /// `Admin.renewDelegationToken(byte[])`).
    ///
    /// `renew_period_ms` is `-1` (Java `RenewDelegationTokenOptions`
    /// default: broker default period).
    pub async fn renew_delegation_token_hmac(
        &mut self,
        hmac: impl AsRef<[u8]>,
    ) -> Result<RenewDelegationTokenResponse> {
        let timeout = self.cfg.request_timeout;
        self.renew_delegation_token_hmac_timeout(hmac, timeout)
            .await
    }

    /// [`Self::renew_delegation_token_hmac`] with a one-shot RPC deadline
    /// (Java `RenewDelegationTokenOptions.timeoutMs`).
    pub async fn renew_delegation_token_hmac_timeout(
        &mut self,
        hmac: impl AsRef<[u8]>,
        timeout: Duration,
    ) -> Result<RenewDelegationTokenResponse> {
        self.renew_delegation_token_timeout(
            RenewDelegationTokenRequest::new(hmac.as_ref().to_vec(), -1),
            timeout,
        )
        .await
    }

    /// Expire a delegation token (ExpireDelegationToken api 40, KIP-48 /
    /// KIP-373; v1–v2, classic at v1, flexible from v2).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` and `controller`. Official
    /// JSON lists no `errorCodes`. Official Java
    /// `KafkaApis.handleExpireTokenRequest` validates the connection
    /// then `forwardToController` (broker-side envelope, not a client
    /// hop). Official Java `KafkaAdminClient.expireDelegationToken`
    /// uses `LeastLoadedNodeProvider`. Official handler writes
    /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) onto the top-level
    /// ErrorCode when the channel is not allowed. `NOT_COORDINATOR`
    /// (16) is not listed. `NOT_CONTROLLER` (41) is not listed. This
    /// is not a group-coordinator hop, not a controller hop, and not
    /// a partition-leader hop: there is no FindCoordinator, no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41)
    /// retry, and no `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level
    /// `error_code` is the INT16 at bytes 0–1, first field — not after
    /// throttle and not a first-token field. Fixture hmac / period
    /// only; this is not a token store. Kafka 4.0 `validVersions` is
    /// `1-2`. This crate speaks 1–2. v0 and v3+ are not spoken. Same
    /// fields on v1 and v2. Do not copy RenewDelegationToken just
    /// because it is the previous slice. ExpireDelegationToken has no
    /// TimeoutMs; the RPC deadline is [`AdminConfig::request_timeout`].
    /// For a one-shot deadline, use [`Self::expire_delegation_token_timeout`].
    /// Optional at [`Self::new`] (Kafka 1.1+ / KIP-48); a broker that
    /// omits api 40 returns [`Error::Unsupported`].
    pub async fn expire_delegation_token(
        &mut self,
        req: ExpireDelegationTokenRequest,
    ) -> Result<ExpireDelegationTokenResponse> {
        let timeout = self.cfg.request_timeout;
        self.expire_delegation_token_timeout(req, timeout).await
    }

    /// [`Self::expire_delegation_token`] with a one-shot RPC deadline (Java
    /// `ExpireDelegationTokenOptions.timeoutMs`).
    ///
    /// ExpireDelegationToken has no TimeoutMs; `timeout` is the RPC deadline.
    pub async fn expire_delegation_token_timeout(
        &mut self,
        req: ExpireDelegationTokenRequest,
        timeout: Duration,
    ) -> Result<ExpireDelegationTokenResponse> {
        let version = self.expire_delegation_token_version.ok_or_else(|| {
            Error::Unsupported("broker does not support ExpireDelegationToken".into())
        })?;
        let body = self
            .roundtrip_bootstrap(
                EXPIRE_DELEGATION_TOKEN,
                version,
                |buf| encode_expire_delegation_token_request(buf, version, &req),
                timeout,
            )
            .await?;
        decode_expire_delegation_token_response(&mut body.clone(), version)
    }

    /// Expire a delegation token by HMAC (Java
    /// `Admin.expireDelegationToken(byte[])`).
    ///
    /// `expiry_time_period_ms` is `-1` (Java default: expire immediately).
    pub async fn expire_delegation_token_hmac(
        &mut self,
        hmac: impl AsRef<[u8]>,
    ) -> Result<ExpireDelegationTokenResponse> {
        let timeout = self.cfg.request_timeout;
        self.expire_delegation_token_hmac_timeout(hmac, timeout)
            .await
    }

    /// [`Self::expire_delegation_token_hmac`] with a one-shot RPC deadline
    /// (Java `ExpireDelegationTokenOptions.timeoutMs`).
    pub async fn expire_delegation_token_hmac_timeout(
        &mut self,
        hmac: impl AsRef<[u8]>,
        timeout: Duration,
    ) -> Result<ExpireDelegationTokenResponse> {
        self.expire_delegation_token_timeout(
            ExpireDelegationTokenRequest::new(hmac.as_ref().to_vec(), -1),
            timeout,
        )
        .await
    }

    /// Describe delegation tokens (DescribeDelegationToken api 41,
    /// KIP-48 / KIP-373; v1–v3, classic at v1, flexible from v2,
    /// TokenRequester v3).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` and `controller`. Official
    /// JSON lists no `errorCodes`. Official Java
    /// `KafkaApis.handleDescribeTokensRequest` answers locally
    /// (`allowTokenRequests` → `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`
    /// (64); it does not `forwardToController`). Official Java
    /// `KafkaAdminClient.describeDelegationToken` uses
    /// `LeastLoadedNodeProvider`. Official handler writes
    /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) onto the top-level
    /// ErrorCode when the channel is not allowed. `NOT_COORDINATOR`
    /// (16) is not listed. `NOT_CONTROLLER` (41) is not listed.
    /// apiKey 41 is not error code 41 and is not a hop. This is not a
    /// group-coordinator hop, not a controller hop, and not a
    /// partition-leader hop: there is no FindCoordinator, no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41)
    /// retry, and no `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level
    /// `error_code` is the INT16 at bytes 0–1, first field — not after
    /// throttle and not a first-token field. Fixture owners only;
    /// this is not a token store. Kafka 4.0 `validVersions` is `1-3`.
    /// This crate speaks 1–3. v0 and v4+ are not spoken. Request
    /// Owners is the same on v1–v3. v1–v2 omit TokenRequester on each
    /// token (decode fills empty). Do not copy ExpireDelegationToken
    /// just because it is the previous slice. DescribeDelegationToken
    /// has no TimeoutMs; the RPC deadline is
    /// [`AdminConfig::request_timeout`]. For a one-shot deadline, use
    /// [`Self::describe_delegation_token_timeout`]. Optional at
    /// [`Self::new`] (Kafka 1.1+ / KIP-48); a broker that omits api 41
    /// returns [`Error::Unsupported`].
    pub async fn describe_delegation_token(
        &mut self,
        req: DescribeDelegationTokenRequest,
    ) -> Result<DescribeDelegationTokenResponse> {
        let timeout = self.cfg.request_timeout;
        self.describe_delegation_token_timeout(req, timeout).await
    }

    /// [`Self::describe_delegation_token`] with a one-shot RPC deadline
    /// (Java `DescribeDelegationTokenOptions.timeoutMs`).
    ///
    /// DescribeDelegationToken has no TimeoutMs; `timeout` is the RPC
    /// deadline.
    pub async fn describe_delegation_token_timeout(
        &mut self,
        req: DescribeDelegationTokenRequest,
        timeout: Duration,
    ) -> Result<DescribeDelegationTokenResponse> {
        let version = self.describe_delegation_token_version.ok_or_else(|| {
            Error::Unsupported("broker does not support DescribeDelegationToken".into())
        })?;
        let body = self
            .roundtrip_bootstrap(
                DESCRIBE_DELEGATION_TOKEN,
                version,
                |buf| encode_describe_delegation_token_request(buf, version, &req),
                timeout,
            )
            .await?;
        decode_describe_delegation_token_response(&mut body.clone(), version)
    }

    /// Describe visible delegation tokens (Java
    /// `Admin.describeDelegationToken()`).
    ///
    /// Same wire as [`Self::describe_delegation_token`] with owners
    /// `None` (Java `DescribeDelegationTokenOptions` default).
    pub async fn describe_delegation_tokens(&mut self) -> Result<DescribeDelegationTokenResponse> {
        let timeout = self.cfg.request_timeout;
        self.describe_delegation_tokens_timeout(timeout).await
    }

    /// [`Self::describe_delegation_tokens`] with a one-shot RPC deadline
    /// (Java `DescribeDelegationTokenOptions.timeoutMs`).
    pub async fn describe_delegation_tokens_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<DescribeDelegationTokenResponse> {
        self.describe_delegation_token_timeout(DescribeDelegationTokenRequest::default(), timeout)
            .await
    }

    async fn discover_group_coord(&mut self, group_id: &str) -> Result<i32> {
        if self.cluster.brokers.is_empty() {
            self.refresh_metadata(None).await?;
        }
        let version = self.find_coord_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let body = self
                .roundtrip_bootstrap(
                    FIND_COORDINATOR,
                    version,
                    |buf| {
                        encode_find_coordinator_request_typed(
                            buf,
                            version,
                            group_id,
                            COORDINATOR_GROUP,
                        )
                    },
                    timeout,
                )
                .await;
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (err, node, _host, _port) =
                decode_find_coordinator_response(&mut body.clone(), version)?;
            if err == 0 {
                if !self.cluster.brokers.contains_key(&node) {
                    self.refresh_metadata(None).await?;
                }
                return Ok(node);
            }
            if error::coordinator_retriable(err) {
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Err(Error::broker(err, "FindCoordinator"));
        }
    }

    async fn discover_group_coords(
        &mut self,
        group_ids: &[String],
    ) -> Result<HashMap<String, i32>> {
        self.discover_coords(group_ids, COORDINATOR_GROUP).await
    }

    async fn discover_txn_coords(
        &mut self,
        transactional_ids: &[String],
    ) -> Result<HashMap<String, i32>> {
        self.discover_coords(transactional_ids, COORDINATOR_TRANSACTION)
            .await
    }

    async fn discover_coords(
        &mut self,
        keys: &[String],
        key_type: i8,
    ) -> Result<HashMap<String, i32>> {
        let mut uniq: Vec<String> = Vec::new();
        for k in keys {
            if !uniq.iter().any(|u| u == k) {
                uniq.push(k.clone());
            }
        }
        if uniq.is_empty() {
            return Ok(HashMap::new());
        }
        let version = self.find_coord_version;
        if version < 4 {
            let mut out = HashMap::new();
            for k in &uniq {
                let node = if key_type == COORDINATOR_TRANSACTION {
                    self.discover_txn_coord(k).await?
                } else {
                    self.discover_group_coord(k).await?
                };
                let _prev = out.insert(k.clone(), node);
            }
            return Ok(out);
        }
        if self.cluster.brokers.is_empty() {
            self.refresh_metadata(None).await?;
        }
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let key_refs: Vec<&str> = uniq.iter().map(String::as_str).collect();
            let body = self
                .roundtrip_bootstrap(
                    FIND_COORDINATOR,
                    version,
                    |buf| encode_find_coordinator_request_keys(buf, version, &key_refs, key_type),
                    timeout,
                )
                .await;
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let coords = decode_find_coordinator_response_coordinators(&mut body.clone(), version)?;
            let mut by_key: HashMap<String, (i16, i32)> = HashMap::new();
            for c in coords {
                let _prev = by_key.insert(c.key, (c.error_code, c.node_id));
            }
            let mut retry = false;
            let mut out = HashMap::new();
            for k in &uniq {
                let (err, node) = by_key
                    .get(k)
                    .copied()
                    .ok_or_else(|| Error::protocol(format!("FindCoordinator missing {k}")))?;
                if err == 0 {
                    if !self.cluster.brokers.contains_key(&node) {
                        self.refresh_metadata(None).await?;
                    }
                    let _prev = out.insert(k.clone(), node);
                    continue;
                }
                if error::coordinator_retriable(err) {
                    retry = true;
                    continue;
                }
                return Err(Error::broker(err, k.clone()));
            }
            if retry {
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(out);
        }
    }

    async fn group_coord_nodes(
        &mut self,
        ids: &[String],
        pending: &[usize],
    ) -> Result<HashMap<i32, Vec<usize>>> {
        let mut need: Vec<String> = Vec::new();
        for &i in pending {
            let Some(id) = ids.get(i) else {
                continue;
            };
            if !self.group_coords.contains_key(id) && !need.iter().any(|k| k == id) {
                need.push(id.clone());
            }
        }
        if !need.is_empty() {
            let found = self.discover_group_coords(&need).await?;
            self.group_coords.extend(found);
        }
        let mut by_node: HashMap<i32, Vec<usize>> = HashMap::new();
        for &i in pending {
            let id = ids
                .get(i)
                .ok_or_else(|| Error::protocol("missing group id"))?;
            let node = *self
                .group_coords
                .get(id)
                .ok_or_else(|| Error::protocol(format!("missing coordinator for {id}")))?;
            by_node.entry(node).or_default().push(i);
        }
        Ok(by_node)
    }

    fn invalidate_group_coord_idxs(&mut self, ids: &[String], idxs: &[usize], node: i32) {
        let _ = self.conns.remove(&node);
        for &i in idxs {
            if let Some(id) = ids.get(i) {
                let _ = self.group_coords.remove(id);
            }
        }
    }

    async fn txn_coord_nodes(
        &mut self,
        ids: &[String],
        pending: &[usize],
    ) -> Result<HashMap<i32, Vec<usize>>> {
        let mut need: Vec<String> = Vec::new();
        for &i in pending {
            let Some(id) = ids.get(i) else {
                continue;
            };
            if !self.txn_coords.contains_key(id) && !need.iter().any(|k| k == id) {
                need.push(id.clone());
            }
        }
        if !need.is_empty() {
            let found = self.discover_txn_coords(&need).await?;
            self.txn_coords.extend(found);
        }
        let mut by_node: HashMap<i32, Vec<usize>> = HashMap::new();
        for &i in pending {
            let id = ids
                .get(i)
                .ok_or_else(|| Error::protocol("missing transactional id"))?;
            let node = *self
                .txn_coords
                .get(id)
                .ok_or_else(|| Error::protocol(format!("missing coordinator for {id}")))?;
            by_node.entry(node).or_default().push(i);
        }
        Ok(by_node)
    }

    fn invalidate_txn_coord_idxs(&mut self, ids: &[String], idxs: &[usize], node: i32) {
        let _ = self.conns.remove(&node);
        for &i in idxs {
            if let Some(id) = ids.get(i) {
                let _ = self.txn_coords.remove(id);
            }
        }
    }

    async fn describe_groups_on_node(
        &mut self,
        node: i32,
        version: i16,
        ids: &[String],
        idxs: &[usize],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<(usize, DescribedGroup)>> {
        let subset: Vec<String> = idxs.iter().filter_map(|&i| ids.get(i).cloned()).collect();
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing describe_groups conn"))?;
            conn.roundtrip(
                DESCRIBE_GROUPS,
                version,
                |buf| {
                    encode_describe_groups_request(
                        buf,
                        version,
                        &subset,
                        include_authorized_operations,
                    )
                },
                timeout,
            )
            .await
        }?;
        let results = decode_describe_groups_response(&mut body.clone(), version)?;
        let mut by_id: HashMap<String, VecDeque<DescribedGroup>> = HashMap::new();
        for g in results {
            by_id.entry(g.group_id.clone()).or_default().push_back(g);
        }
        let mut out = Vec::new();
        for &i in idxs {
            let id = ids
                .get(i)
                .ok_or_else(|| Error::protocol("missing group id"))?;
            let g = by_id
                .get_mut(id)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| Error::protocol(format!("DescribeGroups missing {id}")))?;
            out.push((i, g));
        }
        Ok(out)
    }

    async fn delete_groups_on_node(
        &mut self,
        node: i32,
        version: i16,
        ids: &[String],
        idxs: &[usize],
        timeout: Duration,
    ) -> Result<Vec<(usize, DeletableGroupResult)>> {
        let subset: Vec<String> = idxs.iter().filter_map(|&i| ids.get(i).cloned()).collect();
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing delete_groups conn"))?;
            conn.roundtrip(
                DELETE_GROUPS,
                version,
                |buf| encode_delete_groups_request(buf, version, &subset),
                timeout,
            )
            .await
        }?;
        let results = decode_delete_groups_response(&mut body.clone(), version)?;
        let mut by_id: HashMap<String, VecDeque<DeletableGroupResult>> = HashMap::new();
        for g in results {
            by_id.entry(g.group_id.clone()).or_default().push_back(g);
        }
        let mut out = Vec::new();
        for &i in idxs {
            let id = ids
                .get(i)
                .ok_or_else(|| Error::protocol("missing group id"))?;
            let g = by_id
                .get_mut(id)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| Error::protocol(format!("DeleteGroups missing {id}")))?;
            out.push((i, g));
        }
        Ok(out)
    }

    async fn consumer_group_describe_on_node(
        &mut self,
        node: i32,
        version: i16,
        ids: &[String],
        idxs: &[usize],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<(usize, DescribedConsumerGroup)>> {
        let subset: Vec<String> = idxs.iter().filter_map(|&i| ids.get(i).cloned()).collect();
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing consumer_group_describe conn"))?;
            conn.roundtrip(
                CONSUMER_GROUP_DESCRIBE,
                version,
                |buf| {
                    encode_consumer_group_describe_request(
                        buf,
                        version,
                        &subset,
                        include_authorized_operations,
                    )
                },
                timeout,
            )
            .await
        }?;
        let results = decode_consumer_group_describe_response(&mut body.clone(), version)?;
        let mut by_id: HashMap<String, VecDeque<DescribedConsumerGroup>> = HashMap::new();
        for g in results {
            by_id.entry(g.group_id.clone()).or_default().push_back(g);
        }
        let mut out = Vec::new();
        for &i in idxs {
            let id = ids
                .get(i)
                .ok_or_else(|| Error::protocol("missing group id"))?;
            let g = by_id
                .get_mut(id)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| Error::protocol(format!("ConsumerGroupDescribe missing {id}")))?;
            out.push((i, g));
        }
        Ok(out)
    }

    async fn share_group_describe_on_node(
        &mut self,
        node: i32,
        version: i16,
        ids: &[String],
        idxs: &[usize],
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<Vec<(usize, DescribedShareGroup)>> {
        let subset: Vec<String> = idxs.iter().filter_map(|&i| ids.get(i).cloned()).collect();
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing share_group_describe conn"))?;
            conn.roundtrip(
                SHARE_GROUP_DESCRIBE,
                version,
                |buf| {
                    encode_share_group_describe_request(
                        buf,
                        version,
                        &subset,
                        include_authorized_operations,
                    )
                },
                timeout,
            )
            .await
        }?;
        let results = decode_share_group_describe_response(&mut body.clone(), version)?;
        let mut by_id: HashMap<String, VecDeque<DescribedShareGroup>> = HashMap::new();
        for g in results {
            by_id.entry(g.group_id.clone()).or_default().push_back(g);
        }
        let mut out = Vec::new();
        for &i in idxs {
            let id = ids
                .get(i)
                .ok_or_else(|| Error::protocol("missing group id"))?;
            let g = by_id
                .get_mut(id)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| Error::protocol(format!("ShareGroupDescribe missing {id}")))?;
            out.push((i, g));
        }
        Ok(out)
    }

    async fn describe_share_group_offsets_on_node(
        &mut self,
        node: i32,
        version: i16,
        groups: &[DescribeShareGroupOffsetsGroup],
        idxs: &[usize],
        timeout: Duration,
    ) -> Result<Vec<(usize, DescribedShareGroupOffsets)>> {
        let subset: Vec<DescribeShareGroupOffsetsGroup> = idxs
            .iter()
            .filter_map(|&i| groups.get(i).cloned())
            .collect();
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing describe_share_group_offsets conn"))?;
            conn.roundtrip(
                DESCRIBE_SHARE_GROUP_OFFSETS,
                version,
                |buf| encode_describe_share_group_offsets_request(buf, &subset),
                timeout,
            )
            .await
        }?;
        let results = decode_describe_share_group_offsets_response(&mut body.clone())?;
        let mut by_id: HashMap<String, VecDeque<DescribedShareGroupOffsets>> = HashMap::new();
        for g in results {
            by_id.entry(g.group_id.clone()).or_default().push_back(g);
        }
        let mut out = Vec::new();
        for &i in idxs {
            let id = groups
                .get(i)
                .map(|g| g.group_id.as_str())
                .ok_or_else(|| Error::protocol("missing group id"))?;
            let g = by_id
                .get_mut(id)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| {
                    Error::protocol(format!("DescribeShareGroupOffsets missing {id}"))
                })?;
            out.push((i, g));
        }
        Ok(out)
    }

    async fn describe_transactions_on_node(
        &mut self,
        node: i32,
        ids: &[String],
        idxs: &[usize],
        version: i16,
        timeout: Duration,
    ) -> Result<Vec<(usize, TransactionState)>> {
        let subset: Vec<String> = idxs.iter().filter_map(|&i| ids.get(i).cloned()).collect();
        self.connect_node(node).await?;
        let body = {
            let conn = self
                .conns
                .get_mut(&node)
                .ok_or_else(|| Error::protocol("missing describe_transactions conn"))?;
            conn.roundtrip(
                DESCRIBE_TRANSACTIONS,
                version,
                |buf| encode_describe_transactions_request(buf, &subset),
                timeout,
            )
            .await
        }?;
        let results = decode_describe_transactions_response(&mut body.clone())?;
        let mut by_id: HashMap<String, VecDeque<TransactionState>> = HashMap::new();
        for t in results {
            by_id
                .entry(t.transactional_id.clone())
                .or_default()
                .push_back(t);
        }
        let mut out = Vec::new();
        for &i in idxs {
            let id = ids
                .get(i)
                .ok_or_else(|| Error::protocol("missing transactional id"))?;
            let t = by_id
                .get_mut(id)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| Error::protocol(format!("DescribeTransactions missing {id}")))?;
            out.push((i, t));
        }
        Ok(out)
    }

    async fn discover_txn_coord(&mut self, transactional_id: &str) -> Result<i32> {
        if self.cluster.brokers.is_empty() {
            self.refresh_metadata(None).await?;
        }
        let version = self.find_coord_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let body = self
                .roundtrip_bootstrap(
                    FIND_COORDINATOR,
                    version,
                    |buf| {
                        encode_find_coordinator_request_typed(
                            buf,
                            version,
                            transactional_id,
                            COORDINATOR_TRANSACTION,
                        )
                    },
                    timeout,
                )
                .await;
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    self.wait_retry(&mut attempt, deadline).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (err, node, _host, _port) =
                decode_find_coordinator_response(&mut body.clone(), version)?;
            if err == 0 {
                if !self.cluster.brokers.contains_key(&node) {
                    self.refresh_metadata(None).await?;
                }
                return Ok(node);
            }
            if error::coordinator_retriable(err) {
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Err(Error::broker(err, "FindCoordinator"));
        }
    }
}

fn group_reassignments(assignments: &[PartitionReassignment]) -> Vec<ReassignableTopic> {
    let mut by_topic: HashMap<String, Vec<ReassignablePartition>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for a in assignments {
        match by_topic.entry(a.topic.clone()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                order.push(a.topic.clone());
                let _ = slot.insert(vec![ReassignablePartition {
                    partition_index: a.partition,
                    replicas: a.replicas.clone(),
                }]);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                slot.get_mut().push(ReassignablePartition {
                    partition_index: a.partition,
                    replicas: a.replicas.clone(),
                });
            }
        }
    }
    order
        .into_iter()
        .map(|name| ReassignableTopic {
            partitions: by_topic.remove(&name).unwrap_or_default(),
            name,
        })
        .collect()
}

fn group_list_reassignments(partitions: &[crate::TopicPartition]) -> Vec<ListReassignmentTopic> {
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for tp in partitions {
        match by_topic.entry(tp.topic.clone()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                order.push(tp.topic.clone());
                let _ = slot.insert(vec![tp.partition]);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                slot.get_mut().push(tp.partition);
            }
        }
    }
    order
        .into_iter()
        .map(|name| ListReassignmentTopic {
            partition_indexes: by_topic.remove(&name).unwrap_or_default(),
            name,
        })
        .collect()
}

fn flatten_list_reassignments(
    topics: &[crate::protocol::admin::OngoingTopicReassignment],
) -> Vec<OngoingReassignment> {
    let mut out = Vec::new();
    for t in topics {
        for p in &t.partitions {
            out.push(OngoingReassignment {
                topic: t.name.clone(),
                partition: p.partition_index,
                replicas: p.replicas.clone(),
                adding_replicas: p.adding_replicas.clone(),
                removing_replicas: p.removing_replicas.clone(),
            });
        }
    }
    out
}

fn flatten_reassignment_results(
    results: &[crate::protocol::admin::ReassignmentTopicResult],
) -> Vec<ReassignmentResult> {
    let mut out = Vec::new();
    for t in results {
        for p in &t.partitions {
            out.push(ReassignmentResult {
                topic: t.name.clone(),
                partition: p.partition_index,
                error_code: p.error_code,
                error_message: p.error_message.clone(),
            });
        }
    }
    out
}

fn offset_delete_topics(partitions: &[(String, i32)]) -> Vec<OffsetDeleteTopic> {
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for (topic, part) in partitions {
        match by_topic.entry(topic.clone()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                order.push(topic.clone());
                let _ = slot.insert(vec![*part]);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                slot.get_mut().push(*part);
            }
        }
    }
    order
        .into_iter()
        .map(|topic| OffsetDeleteTopic {
            partitions: by_topic.remove(&topic).unwrap_or_default(),
            topic,
        })
        .collect()
}

/// Java DescribeTopicPartitions default ResponsePartitionLimit.
const DESCRIBE_TOPIC_PARTITIONS_LIMIT: i32 = 2000;

fn topic_listings_from(md: &MetadataResponse, list_internal: bool) -> Vec<TopicListing> {
    md.topics
        .iter()
        .filter(|t| t.error_code == 0)
        .filter(|t| list_internal || !t.is_internal)
        .filter_map(|t| {
            t.name.as_ref().map(|name| TopicListing {
                name: name.clone(),
                topic_id: t.topic_id,
                is_internal: t.is_internal,
            })
        })
        .collect()
}

fn topic_descriptions_including_unnamed(md: &MetadataResponse) -> Vec<TopicDescription> {
    md.topics.iter().map(topic_description_from).collect()
}

fn topic_descriptions_for_names(md: &MetadataResponse, names: &[String]) -> Vec<TopicDescription> {
    names
        .iter()
        .map(|name| {
            md.topics
                .iter()
                .find(|t| t.name.as_deref() == Some(name.as_str()))
                .map(topic_description_from)
                .unwrap_or_else(|| {
                    TopicDescription::new(
                        name.clone(),
                        [0; 16],
                        false,
                        error::UNKNOWN_TOPIC_OR_PARTITION,
                        Vec::new(),
                    )
                })
        })
        .collect()
}

fn topic_description_from(t: &crate::protocol::api::TopicMetadata) -> TopicDescription {
    let name = t.name.clone().unwrap_or_default();
    let partitions = if t.error_code == 0 {
        t.partitions
            .iter()
            .map(|p| crate::PartitionInfo {
                topic: name.clone(),
                partition: p.partition_index,
                leader: p.leader_id,
                leader_epoch: p.leader_epoch,
                replicas: p.replica_nodes.clone(),
                isr: p.isr_nodes.clone(),
                offline_replicas: p.offline_replicas.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };
    TopicDescription {
        name,
        topic_id: t.topic_id,
        is_internal: t.is_internal,
        error_code: t.error_code,
        partitions,
        authorized_operations: t.topic_authorized_operations,
    }
}

fn topic_description_from_dtp(
    t: &DescribedTopicPartitions,
    include_authorized_operations: bool,
) -> TopicDescription {
    let name = t.name.clone().unwrap_or_default();
    let partitions = if t.error_code == 0 {
        t.partitions
            .iter()
            .map(|p| crate::PartitionInfo {
                topic: name.clone(),
                partition: p.partition_index,
                leader: p.leader_id,
                leader_epoch: p.leader_epoch,
                replicas: p.replica_nodes.clone(),
                isr: p.isr_nodes.clone(),
                offline_replicas: p.offline_replicas.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };
    TopicDescription {
        name,
        topic_id: t.topic_id,
        is_internal: t.is_internal,
        error_code: t.error_code,
        partitions,
        authorized_operations: if include_authorized_operations {
            t.topic_authorized_operations
        } else {
            AUTHORIZED_OPERATIONS_OMITTED
        },
    }
}

fn list_offset_topic_requests(
    queries: &[(crate::TopicPartition, i64)],
    idxs: &[usize],
    cluster: &Cluster,
) -> Vec<ListOffsetsTopicRequest> {
    let mut order: Vec<String> = Vec::new();
    let mut by_topic: HashMap<String, Vec<ListOffsetsPartitionRequest>> = HashMap::new();
    for &i in idxs {
        let Some((tp, ts)) = queries.get(i) else {
            continue;
        };
        let part = ListOffsetsPartitionRequest {
            partition: tp.partition,
            current_leader_epoch: cluster.leader_epoch(&tp.topic, tp.partition),
            timestamp: *ts,
        };
        match by_topic.entry(tp.topic.clone()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                order.push(tp.topic.clone());
                let _ = slot.insert(vec![part]);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                slot.get_mut().push(part);
            }
        }
    }
    order
        .into_iter()
        .map(|name| ListOffsetsTopicRequest {
            partitions: by_topic.remove(&name).unwrap_or_default(),
            name,
        })
        .collect()
}

fn replica_broker_ids(replicas: &[TopicPartitionReplica]) -> Vec<i32> {
    let mut ids = Vec::new();
    for r in replicas {
        if !ids.contains(&r.broker_id) {
            ids.push(r.broker_id);
        }
    }
    ids
}

fn describable_topics_for_broker(
    replicas: &[TopicPartitionReplica],
    broker_id: i32,
) -> Vec<DescribableLogDirTopic> {
    let mut order: Vec<String> = Vec::new();
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for r in replicas {
        if r.broker_id != broker_id {
            continue;
        }
        match by_topic.entry(r.topic.clone()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                order.push(r.topic.clone());
                let _ = slot.insert(vec![r.partition]);
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                if !slot.get().contains(&r.partition) {
                    slot.get_mut().push(r.partition);
                }
            }
        }
    }
    order
        .into_iter()
        .map(|name| DescribableLogDirTopic {
            partitions: by_topic.remove(&name).unwrap_or_default(),
            name,
        })
        .collect()
}

fn replica_log_dir_info_from(
    replica: &TopicPartitionReplica,
    results: &[DescribeLogDirsResult],
) -> ReplicaLogDirInfo {
    let mut info = ReplicaLogDirInfo::unknown();
    for dir in results {
        if dir.error_code != 0 {
            continue;
        }
        for topic in &dir.topics {
            if topic.name != replica.topic {
                continue;
            }
            for part in &topic.partitions {
                if part.partition_index != replica.partition {
                    continue;
                }
                if part.is_future_key {
                    info.future_log_dir = Some(dir.log_dir.clone());
                    info.future_offset_lag = part.offset_lag;
                } else {
                    info.current_log_dir = Some(dir.log_dir.clone());
                    info.current_offset_lag = part.offset_lag;
                }
            }
        }
    }
    info
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::admin::CreatedTopicConfig;

    #[test]
    fn records_to_delete_before_offset_converts_to_i64() {
        assert_eq!(i64::from(RecordsToDelete::before_offset(42)), 42);
        assert_eq!(RecordsToDelete::before_offset(7).offset(), 7);
    }

    #[test]
    fn deleted_records_matches_java() {
        let ok = DeletedRecords::new(42);
        assert_eq!(ok.low_watermark(), 42);
        assert_eq!(ok.error_code(), 0);
        let with_err = DeletedRecords::with_error_code(7, 6);
        assert_eq!(with_err.low_watermark(), 7);
        assert_eq!(with_err.error_code(), 6);
        let pair: (i64, i16) = with_err.into();
        assert_eq!(pair, (7, 6));
        assert_eq!(DeletedRecords::from((9, 0)).low_watermark(), 9);
    }

    #[test]
    fn topic_partition_replica_and_log_dir_getters() {
        let replica = TopicPartitionReplica::new("t", 2, 5);
        assert_eq!(replica.topic(), "t");
        assert_eq!(replica.partition(), 2);
        assert_eq!(replica.broker_id(), 5);
        let dirs = ReplicaLogDirInfo::new(Some("/data".into()), 3, Some("/next".into()), 4);
        assert_eq!(dirs.current_log_dir(), Some("/data"));
        assert_eq!(dirs.current_offset_lag(), 3);
        assert_eq!(dirs.future_log_dir(), Some("/next"));
        assert_eq!(dirs.future_offset_lag(), 4);
        let unknown = ReplicaLogDirInfo::unknown();
        assert!(unknown.current_log_dir().is_none());
        assert_eq!(unknown.current_offset_lag(), -1);
        let replica_info = DescribeLogDirsPartition::new(0, 10, 3, false);
        assert_eq!(replica_info.size(), 10);
        assert_eq!(replica_info.offset_lag(), 3);
        assert!(!replica_info.is_future());
        let log_dir = DescribeLogDirsResult::new(
            0,
            "/data",
            vec![DescribeLogDirsTopic::new("t", vec![replica_info])],
            4096,
            1024,
        );
        assert_eq!(log_dir.log_dir(), "/data");
        assert_eq!(log_dir.total_bytes(), Some(4096));
        assert_eq!(log_dir.usable_bytes(), Some(1024));
        assert_eq!(
            DescribeLogDirsResult::new(
                0,
                "/d",
                Vec::new(),
                UNKNOWN_VOLUME_BYTES,
                UNKNOWN_VOLUME_BYTES
            )
            .total_bytes(),
            None
        );
    }

    #[test]
    fn admin_java_spec_getters_match_fields() {
        let topic = NewTopic::new("orders", 3, 1);
        assert_eq!(topic.name(), "orders");
        assert_eq!(topic.num_partitions(), 3);
        assert_eq!(topic.replication_factor(), 1);
        assert!(topic.replicas_assignments().is_none());
        let assigned = NewTopic::with_assignments("t", [(0, vec![1, 2])]);
        assert_eq!(assigned.num_partitions(), -1);
        assert_eq!(
            assigned.replicas_assignments(),
            Some(&[(0, vec![1, 2])][..])
        );
        let parts = NewPartitions::increase_to("t", 5);
        assert_eq!(parts.name(), "t");
        assert_eq!(parts.total_count(), 5);
        assert!(parts.assignments().is_none());
        let with = parts.with_assignments([vec![1, 2]]);
        assert_eq!(with.assignments().map(<[Vec<i32>]>::len), Some(1));
        let abort = AbortTransactionSpec::new(("events", 2), 9, 1, 3);
        assert_eq!(
            abort.topic_partition(),
            crate::TopicPartition::new("events", 2)
        );
        assert_eq!(abort.producer_id(), 9);
        assert_eq!(abort.producer_epoch(), 1);
        assert_eq!(abort.coordinator_epoch(), 3);
        let member = MemberToRemove::new("i-1");
        assert_eq!(member.group_instance_id(), "i-1");
        let removed = RemovedMember {
            member_id: "m".into(),
            group_instance_id: Some("i-1".into()),
            error_code: 0,
        };
        assert_eq!(removed.member_id(), "m");
        assert_eq!(removed.group_instance_id(), Some("i-1"));
        assert_eq!(removed.error_code(), 0);
        let spec = ListConsumerGroupOffsetsSpec::topic_partitions([("t", 0)]);
        assert_eq!(spec.partitions().map(<[_]>::len), Some(1));
        assert!(ListConsumerGroupOffsetsSpec::all().partitions().is_none());
        let update = FeatureUpdate::new("metadata.version", 20);
        assert_eq!(update.name(), "metadata.version");
        assert_eq!(update.max_version_level(), 20);
        let range = SupportedVersionRange::new("metadata.version", 1, 20);
        assert_eq!(range.min_version(), 1);
        assert_eq!(range.max_version(), 20);
        let fin = FinalizedVersionRange::new("metadata.version", 1, 17);
        assert_eq!(fin.min_version_level(), 1);
        assert_eq!(fin.max_version_level(), 17);
        let md = FeatureMetadata {
            supported_features: vec![range],
            finalized_features: vec![fin],
            finalized_features_epoch: Some(8),
            zk_migration_ready: true,
        };
        assert_eq!(md.supported_features().len(), 1);
        assert_eq!(md.finalized_features().len(), 1);
        assert_eq!(md.finalized_features_epoch(), Some(8));
        assert!(md.zk_migration_ready());
        let cluster = ClusterDescription {
            error_code: 0,
            error_message: None,
            cluster_id: Some("mock".into()),
            controller_id: 1,
            endpoint_type: 1,
            cluster_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
            brokers: vec![DescribeClusterBroker::new(
                1,
                "127.0.0.1",
                9092,
                Some("r".into()),
                false,
            )],
        };
        assert_eq!(cluster.error_code(), 0);
        assert_eq!(cluster.cluster_id(), Some("mock"));
        assert_eq!(cluster.controller_id(), 1);
        assert_eq!(cluster.brokers()[0].id(), 1);
        assert_eq!(cluster.brokers()[0].host(), "127.0.0.1");
        assert_eq!(cluster.brokers()[0].port(), 9092);
        assert_eq!(cluster.brokers()[0].rack(), Some("r"));
        assert!(cluster.brokers()[0].has_rack());
        assert!(!cluster.brokers()[0].is_fenced());
    }

    #[test]
    fn config_get_and_replacement_match_java() {
        let entry = ConfigEntry::new("retention.ms", Some("1000".into()));
        let config = Config::new([entry.clone()]);
        assert_eq!(config.entries(), std::slice::from_ref(&entry));
        assert_eq!(config.get("retention.ms"), Some(&entry));
        assert_eq!(config.get("missing"), None);
        let described = DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: CONFIG_RESOURCE_TOPIC,
            name: "t".into(),
            entries: vec![entry.clone()],
        };
        assert_eq!(described.config().get("retention.ms"), Some(&entry));
        assert_eq!(described.name(), "t");
        assert_eq!(described.error_code(), 0);
        assert_eq!(described.entries().len(), 1);
        assert_eq!(entry.name(), "retention.ms");
        assert_eq!(entry.value(), Some("1000"));
        let replacement = ConfigReplacement::from_config(ConfigResource::topic("t"), &config);
        assert_eq!(replacement.resource.name, "t");
        assert_eq!(
            replacement.configs,
            vec![("retention.ms".into(), Some("1000".into()))]
        );
    }

    #[test]
    fn user_scram_credential_alteration_user_matches_java() {
        let d = UserScramCredentialDeletion::new("alice", SCRAM_SHA_256);
        assert_eq!(d.user(), "alice");
        assert_eq!(d.mechanism(), ScramMechanism::Sha256);
        assert_eq!(UserScramCredentialAlteration::from(d).user(), "alice");
        let u = UserScramCredentialUpsertion::new(
            "bob",
            SCRAM_SHA_256,
            4096,
            b"s".to_vec(),
            b"p".to_vec(),
        );
        assert_eq!(u.user(), "bob");
        assert_eq!(u.salt(), b"s");
        assert_eq!(u.credential_info().mechanism(), ScramMechanism::Sha256);
        assert_eq!(u.credential_info().iterations(), 4096);
        assert_eq!(UserScramCredentialAlteration::from(u).user(), "bob");
        let result = UserScramCredentialResult {
            user: "alice".into(),
            error_code: 0,
            error_message: None,
        };
        assert_eq!(result.user(), "alice");
        assert_eq!(result.error_code(), 0);
        assert!(result.error_message().is_none());
    }

    #[test]
    fn config_resource_type_matches_protocol_consts() {
        assert_eq!(i8::from(ConfigResourceType::Topic), CONFIG_RESOURCE_TOPIC);
        assert_eq!(i8::from(ConfigResourceType::Broker), CONFIG_RESOURCE_BROKER);
        assert_eq!(
            i8::from(ConfigResourceType::BrokerLogger),
            CONFIG_RESOURCE_BROKER_LOGGER
        );
        assert_eq!(
            i8::from(ConfigResourceType::ClientMetrics),
            CONFIG_RESOURCE_CLIENT_METRICS
        );
        assert_eq!(i8::from(ConfigResourceType::Group), CONFIG_RESOURCE_GROUP);
        assert_eq!(
            ConfigResource::topic("t").resource_type,
            i8::from(ConfigResourceType::Topic)
        );
        assert_eq!(ConfigResource::topic("t").name(), "t");
        assert_eq!(
            ConfigResource::topic("t").resource_type(),
            Some(ConfigResourceType::Topic)
        );
        assert_eq!(
            ConfigResourceType::from_id(CONFIG_RESOURCE_TOPIC),
            Some(ConfigResourceType::Topic)
        );
        assert_eq!(ConfigResourceType::from_id(99), None);
    }

    #[test]
    fn config_type_and_source_match_wire_ids() {
        assert_eq!(i8::from(ConfigType::Unknown), CONFIG_TYPE_UNKNOWN);
        assert_eq!(i8::from(ConfigType::String), CONFIG_TYPE_STRING);
        assert_eq!(i8::from(ConfigType::Password), CONFIG_TYPE_PASSWORD);
        assert_eq!(ConfigType::from_id(CONFIG_TYPE_STRING), ConfigType::String);
        assert_eq!(ConfigType::from_id(99), ConfigType::Unknown);
        assert_eq!(i8::from(ConfigSource::Unknown), CONFIG_SOURCE_UNKNOWN);
        assert_eq!(
            i8::from(ConfigSource::DynamicTopic),
            CONFIG_SOURCE_DYNAMIC_TOPIC
        );
        assert_eq!(i8::from(ConfigSource::Default), CONFIG_SOURCE_DEFAULT);
        assert_eq!(
            i8::from(ConfigSource::DynamicGroup),
            CONFIG_SOURCE_DYNAMIC_GROUP
        );
        assert_eq!(
            ConfigSource::from_id(CONFIG_SOURCE_DEFAULT),
            ConfigSource::Default
        );
        assert_eq!(ConfigSource::from_id(-1), ConfigSource::Unknown);
        assert_eq!(ConfigSource::from_id(99), ConfigSource::Unknown);
        let def = ConfigEntry {
            name: "k".into(),
            value: Some("v".into()),
            read_only: false,
            source: CONFIG_SOURCE_DEFAULT,
            is_sensitive: false,
            synonyms: Vec::new(),
            config_type: CONFIG_TYPE_INT,
            documentation: None,
        };
        assert!(def.is_default());
        assert_eq!(def.source(), ConfigSource::Default);
        assert_eq!(def.config_type(), ConfigType::Int);
        assert_eq!(def.name(), "k");
        assert_eq!(def.value(), Some("v"));
        assert!(!def.is_sensitive());
        assert!(!def.is_read_only());
        assert!(def.synonyms().is_empty());
        assert!(def.documentation().is_none());
        let syn = ConfigSynonym {
            name: "k".into(),
            value: Some("v".into()),
            source: CONFIG_SOURCE_DEFAULT,
        };
        assert_eq!(syn.name(), "k");
        assert_eq!(syn.value(), Some("v"));
        assert_eq!(syn.source(), ConfigSource::Default);
    }

    #[test]
    fn alter_config_op_type_matches_java() {
        assert_eq!(i8::from(AlterConfigOpType::Set), ALTER_CONFIG_SET);
        assert_eq!(i8::from(AlterConfigOpType::Delete), ALTER_CONFIG_DELETE);
        assert_eq!(i8::from(AlterConfigOpType::Append), ALTER_CONFIG_APPEND);
        assert_eq!(i8::from(AlterConfigOpType::Subtract), ALTER_CONFIG_SUBTRACT);
        assert_eq!(
            AlterConfigOpType::from_id(ALTER_CONFIG_SET),
            Some(AlterConfigOpType::Set)
        );
        assert_eq!(AlterConfigOpType::from_id(99), None);
        let entry = ConfigEntry::new("retention.ms", Some("1000".into()));
        let op = AlterConfig::from_entry(&entry, AlterConfigOpType::Set);
        assert_eq!(op.op_type(), Some(AlterConfigOpType::Set));
        assert_eq!(op.config_entry().name, "retention.ms");
        assert_eq!(op.config_entry().value.as_deref(), Some("1000"));
        assert_eq!(AlterConfigOp::set("k", "v").op, ALTER_CONFIG_SET);
    }

    #[test]
    fn group_type_and_state_match_java() {
        assert_eq!(GroupType::Classic.as_str(), "Classic");
        assert_eq!(GroupType::parse("classic"), GroupType::Classic);
        assert_eq!(GroupType::parse("CONSUMER"), GroupType::Consumer);
        assert_eq!(GroupType::parse("Unknown"), GroupType::Unknown);
        assert_eq!(GroupType::parse("Streams"), GroupType::Unknown);
        assert_eq!(GroupType::parse("nope"), GroupType::Unknown);
        assert_eq!(GroupState::Stable.as_str(), "Stable");
        assert_eq!(GroupState::parse("stable"), GroupState::Stable);
        assert_eq!(
            GroupState::parse("PreparingRebalance"),
            GroupState::PreparingRebalance
        );
        // Java keys `toString().toUpperCase()`, not the enum name: underscore
        // form is UNKNOWN, same as `GroupState.parse("PREPARING_REBALANCE")`.
        assert_eq!(
            GroupState::parse("PREPARING_REBALANCE"),
            GroupState::Unknown
        );
        assert_eq!(GroupState::parse("Unknown"), GroupState::Unknown);
        assert_eq!(GroupState::parse("nope"), GroupState::Unknown);
        assert_eq!(
            GroupState::group_states_for_type(GroupType::Share),
            &[GroupState::Stable, GroupState::Dead, GroupState::Empty]
        );
        assert!(GroupState::group_states_for_type(GroupType::Unknown).is_empty());
        let listed = ListedGroup {
            group_id: "g".into(),
            protocol_type: "consumer".into(),
            group_state: "Stable".into(),
            group_type: "classic".into(),
        };
        assert_eq!(listed.group_state(), GroupState::Stable);
        assert_eq!(listed.group_type(), GroupType::Classic);
        assert_eq!(listed.group_id(), "g");
        assert_eq!(listed.protocol(), "consumer");
        assert!(!listed.is_simple_consumer_group());
        let simple = ListedGroup {
            group_id: "s".into(),
            protocol_type: String::new(),
            group_state: "Empty".into(),
            group_type: "classic".into(),
        };
        assert!(simple.is_simple_consumer_group());
        assert_eq!(simple.protocol(), "");
        let consumer = ConsumerGroupDescription::Consumer({
            let mut g = DescribedConsumerGroup::new("g-cons", 0);
            g.group_state = "Stable".into();
            g.group_epoch = 4;
            g.assignment_epoch = 5;
            g.assignor_name = "uniform".into();
            g
        });
        assert_eq!(consumer.group_id(), "g-cons");
        assert_eq!(consumer.group_state(), "Stable");
        assert_eq!(consumer.partition_assignor(), "uniform");
        assert_eq!(consumer.group_type(), GroupType::Consumer);
        assert_eq!(consumer.group_epoch(), Some(4));
        assert_eq!(consumer.target_assignment_epoch(), Some(5));
        assert!(!consumer.is_simple_consumer_group());
        assert!(consumer.is_consumer_protocol());
        let classic = ConsumerGroupDescription::Classic({
            let mut g = DescribedGroup::new("g-classic", 0);
            g.group_state = "Stable".into();
            g.protocol_type = "consumer".into();
            g.protocol_data = "range".into();
            g
        });
        assert_eq!(classic.partition_assignor(), "range");
        assert_eq!(classic.group_type(), GroupType::Classic);
        assert!(classic.group_epoch().is_none());
        assert!(classic.target_assignment_epoch().is_none());
        assert!(!classic.is_simple_consumer_group());
        assert!(!classic.is_consumer_protocol());
        let simple_desc = ConsumerGroupDescription::Classic(DescribedGroup::new("s", 0));
        assert!(simple_desc.is_simple_consumer_group());
        assert_eq!(simple_desc.partition_assignor(), "");
    }

    #[test]
    fn remaining_admin_result_getters_match_java() {
        let pid = ProducerIdBlock {
            producer_id_start: 1000,
            producer_id_len: 1000,
        };
        assert_eq!(pid.producer_id_start(), 1000);
        assert_eq!(pid.producer_id_len(), 1000);
        let fenced = FencedProducer {
            transactional_id: "tid".into(),
            producer_id: 9,
            epoch: 1,
        };
        assert_eq!(fenced.transactional_id(), "tid");
        assert_eq!(fenced.producer_id(), 9);
        assert_eq!(fenced.epoch(), 1);
        let deleted = DeletedAclsFilterResult {
            error_code: 0,
            error_message: None,
            matching: vec![AclBinding::allow_topic("t", "User:alice")],
        };
        assert_eq!(deleted.error_code(), 0);
        assert!(deleted.error_message().is_none());
        assert_eq!(deleted.matching().len(), 1);
        let altered = AlterConfigsResourceResult {
            error_code: 0,
            error_message: None,
            resource_type: CONFIG_RESOURCE_TOPIC,
            name: "t".into(),
        };
        assert_eq!(altered.error_code(), 0);
        assert_eq!(altered.name(), "t");
        assert!(altered.error_message().is_none());
        let created = TopicResult {
            name: "orders".into(),
            error_code: 0,
            error_message: None,
            topic_id: Uuid::ONE.to_bytes(),
            num_partitions: 3,
            replication_factor: 1,
            configs: vec![CreatedTopicConfig {
                name: "cleanup.policy".into(),
                value: Some("compact".into()),
                read_only: false,
                config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
            }],
        };
        assert_eq!(created.name(), "orders");
        assert_eq!(created.error_code(), 0);
        assert!(created.error_message().is_none());
        assert_eq!(created.topic_id(), Uuid::ONE);
        assert_eq!(created.num_partitions(), 3);
        assert_eq!(created.replication_factor(), 1);
        assert_eq!(
            created
                .config()
                .get("cleanup.policy")
                .and_then(ConfigEntry::value),
            Some("compact")
        );
        let listed = ListedConfigResource::new("r", CONFIG_RESOURCE_CLIENT_METRICS);
        assert_eq!(listed.name(), "r");
        assert_eq!(
            listed.resource_type(),
            Some(ConfigResourceType::ClientMetrics)
        );
        let resource = listed.to_config_resource();
        assert_eq!(resource.name(), "r");
        assert_eq!(
            resource.resource_type(),
            Some(ConfigResourceType::ClientMetrics)
        );
        let owned = ConfigResource::from(listed);
        assert_eq!(owned.name(), "r");
        let deleted = DeletableGroupResult::new("g", 0);
        assert_eq!(deleted.group_id(), "g");
        assert_eq!(deleted.error_code(), 0);
        let telemetry = GetTelemetrySubscriptionsResponse::new(
            0,
            [0x11; 16],
            1,
            vec![1],
            1000,
            100,
            true,
            vec!["m".into()],
        );
        assert_eq!(telemetry.error_code(), 0);
        assert_eq!(telemetry.client_instance_id(), Uuid::from_bytes([0x11; 16]));
        assert_eq!(telemetry.subscription_id(), 1);
        assert_eq!(telemetry.accepted_compression_types(), &[1]);
        assert_eq!(telemetry.push_interval_ms(), 1000);
        assert_eq!(telemetry.telemetry_max_bytes(), 100);
        assert!(telemetry.delta_temporality());
        assert_eq!(telemetry.requested_metrics(), &["m".to_string()]);
        assert_eq!(PushTelemetryResponse::new(0).error_code(), 0);
    }

    #[test]
    fn delegation_token_getters_match_java() {
        let renewer = CreatableRenewer::new("User", "r");
        assert_eq!(renewer.principal_type(), "User");
        assert_eq!(renewer.principal_name(), "r");
        assert_eq!(renewer.to_string(), "User:r");
        let req = CreateDelegationTokenRequest::new(
            Some("User".into()),
            Some("alice".into()),
            vec![renewer],
            -1,
        );
        assert_eq!(req.owner_principal_type(), Some("User"));
        assert_eq!(req.owner_principal_name(), Some("alice"));
        assert_eq!(req.renewers().len(), 1);
        assert_eq!(req.max_lifetime_ms(), -1);
        let created = CreateDelegationTokenResponse::new(
            0,
            "User",
            "alice",
            "User",
            "bob",
            1,
            2,
            3,
            "tid",
            vec![0xaa],
        );
        assert_eq!(created.error_code(), 0);
        assert_eq!(created.principal_type(), "User");
        assert_eq!(created.principal_name(), "alice");
        assert_eq!(created.owner_as_string(), "User:alice");
        assert_eq!(created.token_requester_as_string(), "User:bob");
        assert_eq!(created.issue_timestamp(), 1);
        assert_eq!(created.expiry_timestamp(), 2);
        assert_eq!(created.max_timestamp(), 3);
        assert_eq!(created.token_id(), "tid");
        assert_eq!(created.hmac(), &[0xaa]);
        assert_eq!(created.hmac_as_base64_string(), "qg==");
        let created_debug = format!("{created:?}");
        assert!(
            created_debug.contains("[*******]"),
            "Java DelegationToken.toString redacts hmac: {created_debug}"
        );
        assert!(
            !created_debug.contains("aa") && !created_debug.contains("170"),
            "Debug must not leak hmac bytes: {created_debug}"
        );
        let owner = DescribeDelegationTokenOwner::new("User", "alice");
        assert_eq!(owner.to_string(), "User:alice");
        let described_req = DescribeDelegationTokenRequest::new(Some(vec![owner]));
        assert_eq!(
            described_req
                .owners()
                .map(<[DescribeDelegationTokenOwner]>::len),
            Some(1)
        );
        let token = DescribedDelegationToken::new(
            "User",
            "alice",
            "User",
            "bob",
            1,
            2,
            3,
            "tid",
            vec![0xaa],
            vec![DescribedDelegationTokenRenewer::new("User", "r")],
        );
        assert_eq!(token.owner_as_string(), "User:alice");
        assert_eq!(token.hmac_as_base64_string(), "qg==");
        assert_eq!(token.renewers()[0].to_string(), "User:r");
        let token_debug = format!("{token:?}");
        assert!(token_debug.contains("[*******]"));
        assert!(!token_debug.contains("170"));
        let listed = DescribeDelegationTokenResponse::new(0, vec![token]);
        assert_eq!(listed.error_code(), 0);
        assert_eq!(listed.tokens().len(), 1);
        let renewed = RenewDelegationTokenResponse::new(0, 9);
        assert_eq!(renewed.error_code(), 0);
        assert_eq!(renewed.expiry_timestamp(), 9);
        let expired = ExpireDelegationTokenResponse::new(0, 8);
        assert_eq!(expired.error_code(), 0);
        assert_eq!(expired.expiry_timestamp(), 8);
        let renew_req = RenewDelegationTokenRequest::new(vec![0xaa], -1);
        assert_eq!(renew_req.hmac(), &[0xaa]);
        assert_eq!(renew_req.renew_period_ms(), -1);
        let expire_req = ExpireDelegationTokenRequest::new(vec![0xaa], -1);
        assert_eq!(expire_req.hmac(), &[0xaa]);
        assert_eq!(expire_req.expiry_time_period_ms(), -1);
    }

    #[test]
    fn new_partition_reassignment_matches_java() {
        let neu = NewPartitionReassignment::new([2, 1]).unwrap();
        assert_eq!(neu.target_replicas(), &[2, 1]);
        let err = NewPartitionReassignment::new(Vec::<i32>::new()).unwrap_err();
        assert!(
            err.to_string().contains("without any replicas"),
            "Java NewPartitionReassignment rejects an empty replica list: {err}"
        );
        let assigned = PartitionReassignment::from_new(("t", 0), Some(neu));
        assert_eq!(assigned.topic(), "t");
        assert_eq!(assigned.partition(), 0);
        assert_eq!(assigned.replicas(), Some(&[2, 1][..]));
        let cancelled = PartitionReassignment::from_new(("t", 0), None);
        assert!(cancelled.replicas().is_none());
        let ongoing = OngoingReassignment {
            topic: "t".into(),
            partition: 0,
            replicas: vec![2, 1],
            adding_replicas: vec![2],
            removing_replicas: vec![3],
        };
        assert_eq!(ongoing.topic(), "t");
        assert_eq!(ongoing.partition(), 0);
        assert_eq!(ongoing.replicas(), &[2, 1]);
        assert_eq!(ongoing.adding_replicas(), &[2]);
        assert_eq!(ongoing.removing_replicas(), &[3]);
        let result = ReassignmentResult {
            topic: "t".into(),
            partition: 0,
            error_code: 0,
            error_message: None,
        };
        assert_eq!(result.topic(), "t");
        assert_eq!(result.partition(), 0);
        assert_eq!(result.error_code(), 0);
        assert!(result.error_message().is_none());
    }

    #[test]
    fn uuid_matches_java() {
        assert_eq!(Uuid::ZERO.to_string(), "AAAAAAAAAAAAAAAAAAAAAA");
        assert_eq!(Uuid::ONE.to_string(), "AAAAAAAAAAAAAAAAAAAAAQ");
        assert_eq!(Uuid::METADATA_TOPIC_ID, Uuid::ONE);
        assert_eq!(Uuid::ZERO.most_significant_bits(), 0);
        assert_eq!(Uuid::ZERO.least_significant_bits(), 0);
        assert_eq!(Uuid::ONE.most_significant_bits(), 0);
        assert_eq!(Uuid::ONE.least_significant_bits(), 1);
        assert_eq!(
            Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAA").unwrap(),
            Uuid::ZERO
        );
        assert_eq!(
            Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAQ").unwrap(),
            Uuid::ONE
        );
        assert_eq!(
            Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAQ==").unwrap(),
            Uuid::ONE,
            "Java fromString accepts URL-safe padding"
        );
        assert!(Uuid::from_string("not-a-uuid").is_err());
        assert!(Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAAAAA").is_err());
        let parsed: Uuid = "AAAAAAAAAAAAAAAAAAAAAA".parse().unwrap();
        assert_eq!(parsed, Uuid::ZERO);
        assert_eq!(<[u8; 16]>::from(Uuid::ONE), Uuid::ONE.to_bytes());
        let neg = Uuid::from_parts(i64::MIN, 0);
        assert!(neg < Uuid::ZERO, "Java compareTo uses signed longs");
        let listing = TopicListing::new("t", Uuid::ONE, false);
        assert_eq!(listing.topic_id(), Uuid::ONE);
        assert_eq!(listing.name(), "t");
        assert!(!listing.is_internal());
    }

    #[test]
    fn topic_collection_matches_java() {
        let names = TopicCollection::of_topic_names(["orders", "events"]);
        assert_eq!(names.topic_names().map(<[String]>::len), Some(2));
        assert!(names.topic_ids().is_none());
        let ids = TopicCollection::of_topic_ids([Uuid::ONE, Uuid::ZERO]);
        assert_eq!(ids.topic_ids(), Some(&[Uuid::ONE, Uuid::ZERO][..]));
        assert!(ids.topic_names().is_none());
        let from_bytes = TopicCollection::of_topic_ids([[1u8; 16]]);
        assert_eq!(
            from_bytes.topic_ids().and_then(|ids| ids.first().copied()),
            Some(Uuid::from_bytes([1u8; 16]))
        );
        let empty = TopicCollection::of_topic_ids(Vec::<Uuid>::new());
        assert_eq!(empty, TopicCollection::Ids(Vec::new()));
        assert!(empty.topic_ids().unwrap().is_empty());
    }

    #[test]
    fn scram_mechanism_matches_protocol_consts() {
        assert_eq!(i8::from(ScramMechanism::Unknown), SCRAM_UNKNOWN);
        assert_eq!(i8::from(ScramMechanism::Sha256), SCRAM_SHA_256);
        assert_eq!(i8::from(ScramMechanism::Sha512), SCRAM_SHA_512);
        assert_eq!(
            ScramMechanism::from_id(SCRAM_SHA_256),
            ScramMechanism::Sha256
        );
        assert_eq!(ScramMechanism::from_id(0), ScramMechanism::Unknown);
        assert_eq!(ScramMechanism::from_id(99), ScramMechanism::Unknown);
        assert_eq!(
            ScramMechanism::from_mechanism_name("SCRAM-SHA-256"),
            ScramMechanism::Sha256
        );
        assert_eq!(
            ScramMechanism::from_mechanism_name("SCRAM_SHA_256"),
            ScramMechanism::Unknown
        );
        assert_eq!(ScramMechanism::Sha256.mechanism_name(), "SCRAM-SHA-256");
        let info = ScramCredentialInfo::new(ScramMechanism::Sha512, 8192);
        assert_eq!(info.mechanism(), ScramMechanism::Sha512);
        assert_eq!(info.iterations(), 8192);
    }

    #[test]
    fn topic_listings_skip_errors_and_unnamed() {
        use crate::protocol::api::{PartitionMetadata, TopicMetadata};

        let md = MetadataResponse {
            throttle_time_ms: 0,
            brokers: Vec::new(),
            cluster_id: None,
            controller_id: 1,
            topics: vec![
                TopicMetadata {
                    error_code: 0,
                    name: Some("ok".into()),
                    topic_id: [1; 16],
                    is_internal: false,
                    partitions: vec![PartitionMetadata {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 3,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: Vec::new(),
                    }],
                    topic_authorized_operations: i32::MIN,
                },
                TopicMetadata {
                    error_code: error::UNKNOWN_TOPIC_OR_PARTITION,
                    name: Some("gone".into()),
                    topic_id: [0; 16],
                    is_internal: false,
                    partitions: vec![PartitionMetadata {
                        error_code: error::UNKNOWN_TOPIC_OR_PARTITION,
                        partition_index: 0,
                        leader_id: -1,
                        leader_epoch: -1,
                        replica_nodes: Vec::new(),
                        isr_nodes: Vec::new(),
                        offline_replicas: Vec::new(),
                    }],
                    topic_authorized_operations: i32::MIN,
                },
                TopicMetadata {
                    error_code: 0,
                    name: None,
                    topic_id: [2; 16],
                    is_internal: true,
                    partitions: Vec::new(),
                    topic_authorized_operations: i32::MIN,
                },
                TopicMetadata {
                    error_code: 0,
                    name: Some("__consumer_offsets".into()),
                    topic_id: [3; 16],
                    is_internal: true,
                    partitions: Vec::new(),
                    topic_authorized_operations: i32::MIN,
                },
            ],
            error_code: 0,
        };
        let listed = topic_listings_from(&md, true);
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].name, "ok");
        assert_eq!(listed[0].name(), "ok");
        assert_eq!(listed[0].topic_id, [1; 16]);
        assert_eq!(listed[0].topic_id(), Uuid::from_bytes([1; 16]));
        assert!(!listed[0].is_internal());
        assert_eq!(listed[1].name, "__consumer_offsets");
        assert!(listed[1].is_internal);
        assert!(listed[1].is_internal());
        let listed = topic_listings_from(&md, false);
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "ok");
        let described = topic_descriptions_including_unnamed(&md);
        assert_eq!(described.len(), 4);
        assert_eq!(described[0].name, "ok");
        assert_eq!(described[0].partitions.len(), 1);
        assert_eq!(described[0].partitions().len(), 1);
        assert_eq!(described[0].error_code(), 0);
        assert_eq!(
            described[0].authorized_operations(),
            AUTHORIZED_OPERATIONS_OMITTED
        );
        assert_eq!(described[0].partitions[0].leader_epoch, 3);
        assert_eq!(described[1].name, "gone");
        assert_eq!(described[1].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(described[1].error_code(), error::UNKNOWN_TOPIC_OR_PARTITION);
        assert!(described[1].partitions.is_empty());
        assert!(described[1].partitions().is_empty());
        assert!(described[2].name.is_empty());
        assert_eq!(described[2].topic_id, [2; 16]);
        assert_eq!(described[2].topic_id(), Uuid::from_bytes([2; 16]));
        assert_eq!(described[3].name, "__consumer_offsets");
        assert_eq!(described[3].name(), "__consumer_offsets");
        assert!(described[3].is_internal);
        assert!(described[3].is_internal());
        let named =
            topic_descriptions_for_names(&md, &["ok".into(), "gone".into(), "missing".into()]);
        assert_eq!(named.len(), 3);
        assert_eq!(named[0].name, "ok");
        assert_eq!(named[0].error_code, 0);
        assert_eq!(named[1].name, "gone");
        assert_eq!(named[1].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(named[2].name, "missing");
        assert_eq!(named[2].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn replica_log_dirs_group_and_map_current_future() {
        let replicas = [
            TopicPartitionReplica::new("t", 0, 2),
            TopicPartitionReplica::new("t", 1, 2),
            TopicPartitionReplica::new("u", 0, 1),
            TopicPartitionReplica::new("t", 0, 2),
        ];
        assert_eq!(replica_broker_ids(&replicas), vec![2, 1]);
        let on_two = describable_topics_for_broker(&replicas, 2);
        assert_eq!(on_two.len(), 1);
        assert_eq!(on_two[0].name, "t");
        assert_eq!(on_two[0].partitions, vec![0, 1]);
        let on_one = describable_topics_for_broker(&replicas, 1);
        assert_eq!(on_one.len(), 1);
        assert_eq!(on_one[0].name, "u");
        assert_eq!(on_one[0].partitions, vec![0]);

        let dirs = vec![
            DescribeLogDirsResult::new(
                0,
                "/current",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 10, 3, false)],
                )],
                -1,
                -1,
            ),
            DescribeLogDirsResult::new(
                0,
                "/future",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 4, 7, true)],
                )],
                -1,
                -1,
            ),
            DescribeLogDirsResult::new(56, "/offline", Vec::new(), -1, -1),
        ];
        let replica = TopicPartitionReplica::new("t", 0, 2);
        let info = replica_log_dir_info_from(&replica, &dirs);
        assert_eq!(info.current_log_dir.as_deref(), Some("/current"));
        assert_eq!(info.current_offset_lag, 3);
        assert_eq!(info.future_log_dir.as_deref(), Some("/future"));
        assert_eq!(info.future_offset_lag, 7);
        let missing = replica_log_dir_info_from(&TopicPartitionReplica::new("t", 9, 2), &dirs);
        assert_eq!(missing, ReplicaLogDirInfo::unknown());
    }

    #[test]
    fn list_offset_topic_requests_keeps_duplicate_partitions() {
        let cluster = Cluster::default();
        let queries = [
            (
                crate::TopicPartition::new("t", 0),
                crate::EARLIEST_TIMESTAMP,
            ),
            (crate::TopicPartition::new("t", 0), crate::LATEST_TIMESTAMP),
            (crate::TopicPartition::new("t", 1), crate::LATEST_TIMESTAMP),
        ];
        let topics = list_offset_topic_requests(&queries, &[0, 1, 2], &cluster);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "t");
        assert_eq!(topics[0].partitions.len(), 3);
        assert_eq!(topics[0].partitions[0].timestamp, crate::EARLIEST_TIMESTAMP);
        assert_eq!(topics[0].partitions[1].timestamp, crate::LATEST_TIMESTAMP);
        assert_eq!(topics[0].partitions[2].partition, 1);
    }
}
