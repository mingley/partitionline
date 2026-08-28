//! Kafka admin client: topics, configs, ACLs, groups, and cluster operations.
//!
//! [`Admin::connect`] / [`Admin::new`] negotiate ApiVersions. Methods that
//! must land on the controller retry on `NOT_CONTROLLER`. Group and
//! transaction methods retry on coordinator errors.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, TlsConfig};
use crate::protocol::acl::{
    decode_create_acls_response, decode_delete_acls_response, decode_describe_acls_response,
    encode_create_acls_request, encode_delete_acls_request, encode_describe_acls_request,
};
use crate::protocol::admin::{
    decode_allocate_producer_ids_response, decode_alter_client_quotas_response,
    decode_alter_configs_response, decode_alter_partition_reassignments_response,
    decode_alter_replica_log_dirs_response, decode_alter_share_group_offsets_response,
    decode_alter_user_scram_credentials_response, decode_assign_replicas_to_dirs_response,
    decode_consumer_group_describe_response, decode_create_delegation_token_response,
    decode_create_partitions_response, decode_create_topics_response,
    decode_delete_groups_response, decode_delete_records_response,
    decode_delete_share_group_offsets_response, decode_delete_topics_response,
    decode_describe_client_quotas_response, decode_describe_cluster_response,
    decode_describe_configs_response, decode_describe_delegation_token_response,
    decode_describe_groups_response, decode_describe_log_dirs_response,
    decode_describe_producers_response, decode_describe_share_group_offsets_response,
    decode_describe_topic_partitions_response, decode_describe_transactions_response,
    decode_describe_user_scram_credentials_response, decode_expire_delegation_token_response,
    decode_get_telemetry_subscriptions_response, decode_incremental_alter_configs_response,
    decode_list_config_resources_response, decode_list_groups_response,
    decode_list_partition_reassignments_response, decode_list_transactions_response,
    decode_push_telemetry_response, decode_renew_delegation_token_response,
    decode_share_group_describe_response, decode_unregister_broker_response,
    decode_update_features_response, encode_allocate_producer_ids_request,
    encode_alter_client_quotas_request, encode_alter_configs_request,
    encode_alter_partition_reassignments_request, encode_alter_replica_log_dirs_request,
    encode_alter_share_group_offsets_request, encode_alter_user_scram_credentials_request,
    encode_assign_replicas_to_dirs_request, encode_consumer_group_describe_request,
    encode_create_delegation_token_request, encode_create_partitions_request,
    encode_create_topics_request, encode_delete_groups_request, encode_delete_records_request,
    encode_delete_share_group_offsets_request, encode_delete_topics_request,
    encode_describe_client_quotas_request, encode_describe_cluster_request,
    encode_describe_configs_request, encode_describe_delegation_token_request,
    encode_describe_groups_request, encode_describe_log_dirs_request,
    encode_describe_producers_request, encode_describe_share_group_offsets_request,
    encode_describe_topic_partitions_request, encode_describe_transactions_request,
    encode_describe_user_scram_credentials_request, encode_expire_delegation_token_request,
    encode_get_telemetry_subscriptions_request, encode_incremental_alter_configs_request,
    encode_list_config_resources_request, encode_list_groups_request,
    encode_list_partition_reassignments_request, encode_list_transactions_request,
    encode_push_telemetry_request, encode_renew_delegation_token_request,
    encode_share_group_describe_request, encode_unregister_broker_request,
    encode_update_features_request, CreatableTopic, CreateTopicsRequest, DescribeConfigsResource,
    DescribeConfigsResult, FeatureUpdateKey, ListReassignmentTopic, ReassignablePartition,
    ReassignableTopic, ScramCredentialDeletion, ScramCredentialUpsertion, TopicConfig, TopicResult,
    RESOURCE_BROKER, RESOURCE_BROKER_LOGGER, RESOURCE_CLIENT_METRICS, RESOURCE_GROUP,
    RESOURCE_TOPIC,
};
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, encode_api_versions_request,
    encode_metadata_request, ApiVersion,
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
    INCREMENTAL_ALTER_CONFIGS, LIST_CONFIG_RESOURCES, LIST_GROUPS, LIST_PARTITION_REASSIGNMENTS,
    LIST_TRANSACTIONS, METADATA, OFFSET_DELETE, PUSH_TELEMETRY, RENEW_DELEGATION_TOKEN,
    SHARE_GROUP_DESCRIBE, UNREGISTER_BROKER, UPDATE_FEATURES,
};
use crate::protocol::group::{
    decode_find_coordinator_response, decode_offset_delete_response,
    encode_find_coordinator_request_typed, encode_offset_delete_request, OffsetDeleteTopic,
    COORDINATOR_GROUP, COORDINATOR_TRANSACTION,
};
use crate::protocol::sasl;

pub use crate::protocol::acl::{AclBinding, AclOperation, AclPermission, AclResourceType};
pub use crate::protocol::admin::{
    ActiveProducer, AlterConfig, AlterReplicaLogDirsDirectory, AlterReplicaLogDirsRequest,
    AlterReplicaLogDirsResponse, AlterReplicaLogDirsResponsePartition,
    AlterReplicaLogDirsResponseTopic, AlterReplicaLogDirsTopic, AlterShareGroupOffsetsPartition,
    AlterShareGroupOffsetsTopic, AlteredShareGroupOffsets, AlteredShareGroupOffsetsPartition,
    AlteredShareGroupOffsetsTopic, AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition,
    AssignReplicasToDirsRequest, AssignReplicasToDirsResponse,
    AssignReplicasToDirsResponseDirectory, AssignReplicasToDirsResponsePartition,
    AssignReplicasToDirsResponseTopic, AssignReplicasToDirsTopic, ClientQuotaAlteration,
    ClientQuotaAlterationResult, ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilterComponent,
    ClientQuotaOp, ClientQuotaValue, ClusterDescription, ConfigEntry, ConfigSynonym,
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
    GetTelemetrySubscriptionsResponse, ListedConfigResource, ListedGroup, PushTelemetryRequest,
    PushTelemetryResponse, RenewDelegationTokenRequest, RenewDelegationTokenResponse,
    ScramCredentialInfo, ScramMechanism, ShareGroupAssignment, ShareGroupMember,
    ShareGroupTopicPartitions, TopicPartitionCursor, TransactionListing, TransactionState,
    TransactionTopic, ALTER_CONFIG_DELETE, ALTER_CONFIG_SET, AUTHORIZED_OPERATIONS_OMITTED,
    QUOTA_MATCH_ANY, QUOTA_MATCH_DEFAULT, QUOTA_MATCH_EXACT,
    RESOURCE_BROKER as CONFIG_RESOURCE_BROKER,
    RESOURCE_BROKER_LOGGER as CONFIG_RESOURCE_BROKER_LOGGER,
    RESOURCE_CLIENT_METRICS as CONFIG_RESOURCE_CLIENT_METRICS,
    RESOURCE_GROUP as CONFIG_RESOURCE_GROUP, RESOURCE_TOPIC as CONFIG_RESOURCE_TOPIC,
    SCRAM_SHA_256, SCRAM_SHA_512,
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
    /// Partition count.
    pub num_partitions: i32,
    /// Replication factor.
    pub replication_factor: i16,
    /// Optional topic configs `(name, value)`.
    pub configs: Vec<(String, Option<String>)>,
}

impl NewTopic {
    /// `name` with partition count and replication factor.
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            configs: Vec::new(),
        }
    }

    /// Topic config `(name, value)`.
    #[must_use]
    pub fn config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((name.into(), Some(value.into())));
        self
    }
}

/// Increase a topic's partition count (`CreatePartitions`). Java `NewPartitions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPartitions {
    /// Topic name.
    pub name: String,
    /// Total partition count after the increase (not a delta).
    pub total_count: i32,
}

impl NewPartitions {
    /// Set `name` to `total_count` partitions (Java `NewPartitions.increaseTo`).
    #[must_use]
    pub fn increase_to(name: impl Into<String>, total_count: i32) -> Self {
        Self {
            name: name.into(),
            total_count,
        }
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

/// Flattened ongoing reassignment from ListPartitionReassignments.
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

/// One finalized-feature update for `Admin::update_features` (v0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdate {
    /// Feature name (for example `metadata.version`).
    pub name: String,
    /// Target finalized version.
    pub max_version_level: i16,
    /// When true, allow a downgrade (unsafe).
    pub allow_downgrade: bool,
}

impl FeatureUpdate {
    /// Feature `name` at `max_version_level` (upgrade only).
    #[must_use]
    pub fn new(name: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            name: name.into(),
            max_version_level,
            allow_downgrade: false,
        }
    }

    /// Allow a downgrade of this feature.
    #[must_use]
    pub fn allow_downgrade(mut self, allow: bool) -> Self {
        self.allow_downgrade = allow;
        self
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

/// Kafka admin client: topics, configs, ACLs, groups, and cluster operations.
pub struct Admin {
    cfg: AdminConfig,
    conn: BrokerConn,
    versions: HashMap<i16, ApiVersion>,
    create_version: i16,
    delete_version: i16,
    describe_version: i16,
    partitions_version: i16,
    alter_version: i16,
    legacy_alter_version: i16,
    delete_records_version: i16,
    describe_producers_version: i16,
    describe_cluster_version: i16,
    create_acls_version: i16,
    describe_acls_version: i16,
    delete_acls_version: i16,
    metadata_version: i16,
    find_coord_version: i16,
    offset_delete_version: i16,
    reassign_version: i16,
    list_reassign_version: i16,
    update_features_version: i16,
    alter_user_scram_version: i16,
    describe_user_scram_version: i16,
    unregister_broker_version: i16,
    describe_client_quotas_version: i16,
    alter_client_quotas_version: i16,
    allocate_producer_ids_version: i16,
    describe_transactions_version: i16,
    list_transactions_version: i16,
    consumer_group_describe_version: i16,
    describe_groups_version: i16,
    list_groups_version: i16,
    delete_groups_version: i16,
    share_group_describe_version: i16,
    describe_share_group_offsets_version: i16,
    alter_share_group_offsets_version: i16,
    delete_share_group_offsets_version: i16,
    describe_topic_partitions_version: i16,
    list_config_resources_version: i16,
    get_telemetry_subscriptions_version: i16,
    /// Cached KIP-714 client instance UUID (`None` until first fetch).
    cached_client_instance_id: Option<[u8; 16]>,
    push_telemetry_version: i16,
    assign_replicas_to_dirs_version: i16,
    alter_replica_log_dirs_version: i16,
    describe_log_dirs_version: i16,
    create_delegation_token_version: i16,
    renew_delegation_token_version: i16,
    expire_delegation_token_version: i16,
    describe_delegation_token_version: i16,
    cluster: Cluster,
    conns: HashMap<i32, BrokerConn>,
    reconnect_fails: HashMap<i32, u32>,
    group_coord: Option<(String, i32)>,
    txn_coord: Option<(String, i32)>,
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

impl Admin {
    /// Connect with default config to one bootstrap server.
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(AdminConfig::bootstrap([bootstrap.into()])).await
    }

    /// Connect using `cfg`. Negotiates ApiVersions and optional SASL/TLS.
    pub async fn new(cfg: AdminConfig) -> Result<Self> {
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
        let create_version = versions
            .get(&CREATE_TOPICS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 4))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support CreateTopics v0-4".into())
            })?;
        let delete_version = versions
            .get(&DELETE_TOPICS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 3))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DeleteTopics v0-3".into())
            })?;
        let describe_version = versions
            .get(&DESCRIBE_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeConfigs v0-1".into())
            })?;
        let partitions_version = versions
            .get(&CREATE_PARTITIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| Error::Unsupported("broker does not support CreatePartitions".into()))?;
        let alter_version = versions
            .get(&INCREMENTAL_ALTER_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support IncrementalAlterConfigs".into())
            })?;
        let legacy_alter_version = versions
            .get(&ALTER_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| Error::Unsupported("broker does not support AlterConfigs".into()))?;
        let delete_records_version = versions
            .get(&DELETE_RECORDS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| Error::Unsupported("broker does not support DeleteRecords".into()))?;
        let describe_producers_version = versions
            .get(&DESCRIBE_PRODUCERS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeProducers".into())
            })?;
        let describe_cluster_version = versions
            .get(&DESCRIBE_CLUSTER)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support DescribeCluster".into()))?;
        let create_acls_version = versions
            .get(&CREATE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support CreateAcls".into()))?;
        let describe_acls_version = versions
            .get(&DESCRIBE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support DescribeAcls".into()))?;
        let delete_acls_version = versions
            .get(&DELETE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support DeleteAcls".into()))?;
        let metadata_version = versions
            .get(&METADATA)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 12))
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        let find_coord_version = versions
            .get(&FIND_COORDINATOR)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support FindCoordinator v1-2".into())
            })?;
        let offset_delete_version = versions
            .get(&OFFSET_DELETE)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support OffsetDelete".into()))?;
        let reassign_version = versions
            .get(&ALTER_PARTITION_REASSIGNMENTS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support AlterPartitionReassignments".into())
            })?;
        let list_reassign_version = versions
            .get(&LIST_PARTITION_REASSIGNMENTS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support ListPartitionReassignments".into())
            })?;
        let update_features_version = versions
            .get(&UPDATE_FEATURES)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support UpdateFeatures".into()))?;
        let alter_user_scram_version = versions
            .get(&ALTER_USER_SCRAM_CREDENTIALS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support AlterUserScramCredentials".into())
            })?;
        let describe_user_scram_version = versions
            .get(&DESCRIBE_USER_SCRAM_CREDENTIALS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeUserScramCredentials".into())
            })?;
        let unregister_broker_version = versions
            .get(&UNREGISTER_BROKER)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support UnregisterBroker".into()))?;
        let describe_client_quotas_version = versions
            .get(&DESCRIBE_CLIENT_QUOTAS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 1))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeClientQuotas".into())
            })?;
        let alter_client_quotas_version = versions
            .get(&ALTER_CLIENT_QUOTAS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 1))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support AlterClientQuotas".into())
            })?;
        let allocate_producer_ids_version = versions
            .get(&ALLOCATE_PRODUCER_IDS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support AllocateProducerIds".into())
            })?;
        let describe_transactions_version = versions
            .get(&DESCRIBE_TRANSACTIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeTransactions".into())
            })?;
        let list_transactions_version = versions
            .get(&LIST_TRANSACTIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support ListTransactions".into()))?;
        let consumer_group_describe_version = versions
            .get(&CONSUMER_GROUP_DESCRIBE)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 1))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support ConsumerGroupDescribe".into())
            })?;
        let describe_groups_version = versions
            .get(&DESCRIBE_GROUPS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 6, 6))
            .ok_or_else(|| Error::Unsupported("broker does not support DescribeGroups".into()))?;
        let list_groups_version = versions
            .get(&LIST_GROUPS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 5, 5))
            .ok_or_else(|| Error::Unsupported("broker does not support ListGroups".into()))?;
        let delete_groups_version = versions
            .get(&DELETE_GROUPS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 2, 2))
            .ok_or_else(|| Error::Unsupported("broker does not support DeleteGroups".into()))?;
        let share_group_describe_version = versions
            .get(&SHARE_GROUP_DESCRIBE)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 1))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support ShareGroupDescribe".into())
            })?;
        let describe_share_group_offsets_version = versions
            .get(&DESCRIBE_SHARE_GROUP_OFFSETS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeShareGroupOffsets".into())
            })?;
        let alter_share_group_offsets_version = versions
            .get(&ALTER_SHARE_GROUP_OFFSETS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support AlterShareGroupOffsets".into())
            })?;
        let delete_share_group_offsets_version = versions
            .get(&DELETE_SHARE_GROUP_OFFSETS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DeleteShareGroupOffsets".into())
            })?;
        let describe_topic_partitions_version = versions
            .get(&DESCRIBE_TOPIC_PARTITIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeTopicPartitions".into())
            })?;
        let list_config_resources_version = versions
            .get(&LIST_CONFIG_RESOURCES)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 1))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support ListConfigResources".into())
            })?;
        let get_telemetry_subscriptions_version = versions
            .get(&GET_TELEMETRY_SUBSCRIPTIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support GetTelemetrySubscriptions".into())
            })?;
        let push_telemetry_version = versions
            .get(&PUSH_TELEMETRY)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support PushTelemetry".into()))?;
        let assign_replicas_to_dirs_version = versions
            .get(&ASSIGN_REPLICAS_TO_DIRS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support AssignReplicasToDirs".into())
            })?;
        let alter_replica_log_dirs_version = versions
            .get(&ALTER_REPLICA_LOG_DIRS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 2, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support AlterReplicaLogDirs".into())
            })?;
        let describe_log_dirs_version = versions
            .get(&DESCRIBE_LOG_DIRS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 4, 4))
            .ok_or_else(|| Error::Unsupported("broker does not support DescribeLogDirs".into()))?;
        let create_delegation_token_version = versions
            .get(&CREATE_DELEGATION_TOKEN)
            .and_then(|v| pick_version(v.min_version, v.max_version, 3, 3))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support CreateDelegationToken".into())
            })?;
        let renew_delegation_token_version = versions
            .get(&RENEW_DELEGATION_TOKEN)
            .and_then(|v| pick_version(v.min_version, v.max_version, 2, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support RenewDelegationToken".into())
            })?;
        let expire_delegation_token_version = versions
            .get(&EXPIRE_DELEGATION_TOKEN)
            .and_then(|v| pick_version(v.min_version, v.max_version, 2, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support ExpireDelegationToken".into())
            })?;
        let describe_delegation_token_version = versions
            .get(&DESCRIBE_DELEGATION_TOKEN)
            .and_then(|v| pick_version(v.min_version, v.max_version, 3, 3))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeDelegationToken".into())
            })?;
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
            txn_coord: None,
        })
    }

    /// Negotiated ApiVersions for this connection.
    #[must_use]
    pub fn versions(&self) -> &HashMap<i16, ApiVersion> {
        &self.versions
    }

    /// Drop the admin connection.
    pub async fn close(self) -> Result<()> {
        Ok(())
    }

    /// Java `clientInstanceId` (KIP-714 GetTelemetrySubscriptions).
    ///
    /// The first call sends a zero UUID; the broker assigns one. Later calls
    /// return the cached id without another round-trip.
    pub async fn client_instance_id(&mut self) -> Result<[u8; 16]> {
        if let Some(id) = self.cached_client_instance_id {
            return Ok(id);
        }
        let id = fetch_client_instance_id(
            &mut self.conn,
            self.get_telemetry_subscriptions_version,
            self.cfg.request_timeout,
            [0; 16],
        )
        .await?;
        self.cached_client_instance_id = Some(id);
        Ok(id)
    }

    /// Create topics (`CreateTopics`).
    pub async fn create_topics(
        &mut self,
        topics: &[NewTopic],
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let req = CreateTopicsRequest {
            topics: topics
                .iter()
                .map(|t| CreatableTopic {
                    name: t.name.clone(),
                    num_partitions: t.num_partitions,
                    replication_factor: t.replication_factor,
                    assignments: Vec::new(),
                    configs: t
                        .configs
                        .iter()
                        .map(|(n, v)| TopicConfig {
                            name: n.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                })
                .collect(),
            timeout_ms,
            validate_only,
        };
        let version = self.create_version;
        let timeout = self.cfg.request_timeout;
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
            return Ok(results);
        }
    }

    /// Delete topics (`DeleteTopics`).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    pub async fn delete_topics(
        &mut self,
        names: &[impl AsRef<str>],
        timeout_ms: i32,
    ) -> Result<Vec<TopicResult>> {
        let names: Vec<String> = names.iter().map(|n| n.as_ref().to_string()).collect();
        let version = self.delete_version;
        let timeout = self.cfg.request_timeout;
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
                    |buf| encode_delete_topics_request(buf, &names, timeout_ms),
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
            return Ok(results);
        }
    }

    /// Describe broker or topic configs (`DescribeConfigs`).
    pub async fn describe_configs(
        &mut self,
        resources: &[ConfigResource],
        include_synonyms: bool,
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
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_CONFIGS,
                version,
                |buf| encode_describe_configs_request(buf, version, &req, include_synonyms),
                timeout,
            )
            .await?;
        decode_describe_configs_response(&mut body.clone(), version)
    }

    /// Increase partition count (`CreatePartitions`).
    ///
    /// [`NewPartitions::total_count`] is the new total, not a delta. Lands on
    /// the Metadata controller. `NOT_CONTROLLER` (41) refreshes Metadata and
    /// retries.
    pub async fn create_partitions(
        &mut self,
        topics: &[NewPartitions],
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let topics: Vec<(String, i32)> = topics
            .iter()
            .map(|t| (t.name.clone(), t.total_count))
            .collect();
        let version = self.partitions_version;
        let timeout = self.cfg.request_timeout;
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
                    .ok_or_else(|| Error::protocol("missing create_partitions conn"))?;
                conn.roundtrip(
                    CREATE_PARTITIONS,
                    version,
                    |buf| encode_create_partitions_request(buf, &topics, timeout_ms, validate_only),
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
            let results = decode_create_partitions_response(&mut body.clone())?;
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

    /// Alter configs incrementally (`IncrementalAlterConfigs`).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    pub async fn incremental_alter_configs(
        &mut self,
        resource: &ConfigResource,
        configs: &[AlterConfig],
        validate_only: bool,
    ) -> Result<i16> {
        let version = self.alter_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        let resource_type = resource.resource_type;
        let name = resource.name.clone();
        let configs = configs.to_vec();
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
                        encode_incremental_alter_configs_request(
                            buf,
                            resource_type,
                            &name,
                            &configs,
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
            let err = decode_incremental_alter_configs_response(&mut body.clone())?;
            if err == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(err);
        }
    }

    /// Create ACL bindings (`CreateAcls`).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    pub async fn create_acls(&mut self, acls: &[AclBinding]) -> Result<Vec<i16>> {
        let version = self.create_acls_version;
        let timeout = self.cfg.request_timeout;
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
                    |buf| encode_create_acls_request(buf, &acls),
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
            let results = decode_create_acls_response(&mut body.clone())?;
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
    pub async fn alter_partition_reassignments(
        &mut self,
        assignments: &[PartitionReassignment],
        timeout_ms: i32,
    ) -> Result<Vec<ReassignmentResult>> {
        let topics = group_reassignments(assignments);
        let version = self.reassign_version;
        let timeout = self.cfg.request_timeout;
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
    pub async fn list_partition_reassignments(
        &mut self,
        partitions: Option<&[crate::TopicPartition]>,
        timeout_ms: i32,
    ) -> Result<Vec<OngoingReassignment>> {
        let topics = partitions.map(group_list_reassignments);
        let version = self.list_reassign_version;
        let timeout = self.cfg.request_timeout;
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
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    pub async fn update_features(
        &mut self,
        updates: &[FeatureUpdate],
        timeout_ms: i32,
    ) -> Result<Vec<FeatureUpdateResult>> {
        let keys: Vec<FeatureUpdateKey> = updates
            .iter()
            .map(|u| FeatureUpdateKey {
                name: u.name.clone(),
                max_version_level: u.max_version_level,
                allow_downgrade: u.allow_downgrade,
            })
            .collect();
        let version = self.update_features_version;
        let timeout = self.cfg.request_timeout;
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
                    |buf| encode_update_features_request(buf, timeout_ms, &keys),
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
            let resp = decode_update_features_response(&mut body.clone())?;
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

    /// Upsert or delete user SCRAM credentials (AlterUserScramCredentials
    /// api 51).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    pub async fn alter_user_scram_credentials(
        &mut self,
        deletions: &[UserScramCredentialDeletion],
        upsertions: &[UserScramCredentialUpsertion],
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
        let version = self.alter_user_scram_version;
        let timeout = self.cfg.request_timeout;
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
    /// (bytes 4–5), not a first-result field. Empty `users` describes all
    /// fixture users.
    pub async fn describe_user_scram_credentials(
        &mut self,
        users: &[&str],
    ) -> Result<Vec<DescribeUserScramCredentialsResult>> {
        let users: Vec<String> = users.iter().map(|s| (*s).to_string()).collect();
        let version = self.describe_user_scram_version;
        let timeout = self.cfg.request_timeout;
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
                    |buf| encode_describe_user_scram_credentials_request(buf, Some(&users)),
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

    /// Unregister a broker (UnregisterBroker api 64, KIP-500).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller. Top-level `error_code`
    /// (bytes 4–5), after throttle. Fixture broker id only; this is not
    /// a live KRaft unregistration.
    pub async fn unregister_broker(&mut self, broker_id: i32) -> Result<()> {
        let version = self.unregister_broker_version;
        let timeout = self.cfg.request_timeout;
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
    /// Lands on the connected broker (bootstrap is fine). Official Apache
    /// JSON listeners are `broker` only. This is not a controller hop:
    /// there is no Metadata `controller_id` lookup and no
    /// `NOT_CONTROLLER` (41) retry. Top-level `error_code` is the INT16
    /// at bytes 4–5, after throttle.
    pub async fn describe_client_quotas(
        &mut self,
        components: &[ClientQuotaFilterComponent],
        strict: bool,
    ) -> Result<Vec<ClientQuotaEntry>> {
        let components = components.to_vec();
        let version = self.describe_client_quotas_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_CLIENT_QUOTAS,
                version,
                |buf| encode_describe_client_quotas_request(buf, &components, strict),
                timeout,
            )
            .await?;
        let resp = decode_describe_client_quotas_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "DescribeClientQuotas"));
        }
        Ok(resp.entries.unwrap_or_default())
    }

    /// Upsert or delete client quotas (AlterClientQuotas api 49).
    ///
    /// Lands on the Metadata controller. `NOT_CONTROLLER` (41) refreshes
    /// Metadata and retries on the new controller.
    pub async fn alter_client_quotas(
        &mut self,
        entries: &[ClientQuotaAlteration],
        validate_only: bool,
    ) -> Result<Vec<ClientQuotaAlterationResult>> {
        let entries = entries.to_vec();
        let version = self.alter_client_quotas_version;
        let timeout = self.cfg.request_timeout;
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
                    |buf| encode_alter_client_quotas_request(buf, &entries, validate_only),
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
            let results = decode_alter_client_quotas_response(&mut body.clone())?;
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
    pub async fn allocate_producer_ids(
        &mut self,
        broker_id: i32,
        broker_epoch: i64,
    ) -> Result<ProducerIdBlock> {
        let version = self.allocate_producer_ids_version;
        let timeout = self.cfg.request_timeout;
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

    /// Describe transactional.id state (DescribeTransactions api 65).
    ///
    /// Lands on the transaction coordinator (`FindCoordinator`
    /// `key_type=1`). `COORDINATOR_LOAD_IN_PROGRESS` /
    /// `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. This is not `NOT_CONTROLLER` (41).
    pub async fn describe_transactions(
        &mut self,
        transactional_ids: &[&str],
    ) -> Result<Vec<TransactionState>> {
        let ids: Vec<String> = transactional_ids.iter().map(|s| (*s).to_string()).collect();
        let Some(coord_key) = ids.first().cloned() else {
            return Ok(Vec::new());
        };
        let version = self.describe_transactions_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            let stale = self.txn_coord.as_ref().is_none_or(|(k, _)| k != &coord_key);
            if stale {
                let node = self.discover_txn_coord(&coord_key).await?;
                self.txn_coord = Some((coord_key.clone(), node));
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
                    .ok_or_else(|| Error::protocol("missing describe_transactions conn"))?;
                conn.roundtrip(
                    DESCRIBE_TRANSACTIONS,
                    version,
                    |buf| encode_describe_transactions_request(buf, &ids),
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
            let results = decode_describe_transactions_response(&mut body.clone())?;
            if results
                .iter()
                .any(|r| error::coordinator_retriable(r.error_code))
            {
                // 14/15/16: FindCoordinator, then the new txn coordinator.
                self.txn_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(results);
        }
    }

    /// List transactional.id state (ListTransactions api 66).
    ///
    /// Lands on the transaction coordinator (`FindCoordinator`
    /// `key_type=1`). `COORDINATOR_LOAD_IN_PROGRESS` /
    /// `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. This is not `NOT_CONTROLLER` (41). Top-level
    /// `error_code` (bytes 4–5), not a first-result field.
    pub async fn list_transactions(
        &mut self,
        state_filters: &[&str],
        producer_id_filters: &[i64],
    ) -> Result<Vec<TransactionListing>> {
        let states: Vec<String> = state_filters.iter().map(|s| (*s).to_string()).collect();
        let pids = producer_id_filters.to_vec();
        // ListTransactions has no transactional.id; FindCoordinator still
        // needs a key. Empty string is the no-id lookup used here.
        const COORD_KEY: &str = "";
        let version = self.list_transactions_version;
        let timeout = self.cfg.request_timeout;
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
                    |buf| encode_list_transactions_request(buf, &states, &pids),
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
            let resp = decode_list_transactions_response(&mut body.clone())?;
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
    /// `resource_type` is [`crate::AclResourceType`] or a protocol `i8`
    /// (`ACL_RESOURCE_TOPIC`, …).
    pub async fn describe_acls(&mut self, resource_type: impl Into<i8>) -> Result<Vec<AclBinding>> {
        let resource_type = resource_type.into();
        let version = self.describe_acls_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_ACLS,
                version,
                |buf| encode_describe_acls_request(buf, resource_type),
                timeout,
            )
            .await?;
        decode_describe_acls_response(&mut body.clone())
    }

    /// Replace configs (`AlterConfigs`, legacy api 33).
    ///
    /// Prefer [`Self::incremental_alter_configs`] on modern brokers.
    pub async fn alter_configs(
        &mut self,
        resource: &ConfigResource,
        configs: &[(String, Option<String>)],
        validate_only: bool,
    ) -> Result<i16> {
        let version = self.legacy_alter_version;
        let timeout = self.cfg.request_timeout;
        let resource_type = resource.resource_type;
        let name = resource.name.clone();
        let configs: Vec<TopicConfig> = configs
            .iter()
            .map(|(n, v)| TopicConfig {
                name: n.clone(),
                value: v.clone(),
            })
            .collect();
        let body = self
            .conn
            .roundtrip(
                ALTER_CONFIGS,
                version,
                |buf| {
                    encode_alter_configs_request(
                        buf,
                        version,
                        resource_type,
                        &name,
                        &configs,
                        validate_only,
                    )
                },
                timeout,
            )
            .await?;
        decode_alter_configs_response(&mut body.clone(), version)
    }

    async fn refresh_metadata(&mut self, topics: Option<&[String]>) -> Result<()> {
        self.ensure_bootstrap().await?;
        let version = self.metadata_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                METADATA,
                version,
                |buf| encode_metadata_request(buf, version, topics, false),
                timeout,
            )
            .await?;
        let md = decode_metadata_response(&mut body.clone(), version)?;
        self.cluster.apply(&md);
        Ok(())
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
        Ok(conn)
    }

    /// Delete records before `offset` (`DeleteRecords`).
    ///
    /// Lands on the Metadata partition leader. `NOT_LEADER_OR_FOLLOWER` (6)
    /// and other retriable codes refresh Metadata and retry on the new
    /// leader. Returns `(low_watermark, error_code)`.
    pub async fn delete_records(
        &mut self,
        partition: impl Into<crate::TopicPartition>,
        offset: i64,
        timeout_ms: i32,
    ) -> Result<(i64, i16)> {
        let tp = partition.into();
        let topic = tp.topic;
        let partition = tp.partition;
        let version = self.delete_records_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.leader(&topic, partition).is_err() {
                let topics = [topic.clone()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(&topic, partition)?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing delete_records conn"))?;
                conn.roundtrip(
                    DELETE_RECORDS,
                    version,
                    |buf| encode_delete_records_request(buf, &topic, partition, offset, timeout_ms),
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
            let (_p, low, err) = decode_delete_records_response(&mut body.clone(), version)?;
            if err == 0 {
                return Ok((low, err));
            }
            let e = Error::broker(err, format!("{topic}-{partition}"));
            if e.is_retriable() {
                // NOT_LEADER_OR_FOLLOWER (6) and friends: Metadata, then the new leader.
                self.cluster.invalidate_topic(&topic);
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                let topics = [topic.clone()];
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Ok((low, err));
        }
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
    pub async fn describe_producers(
        &mut self,
        partition: impl Into<crate::TopicPartition>,
    ) -> Result<DescribeProducersPartition> {
        let tp = partition.into();
        let topic = tp.topic;
        let partition = tp.partition;
        let version = self.describe_producers_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if self.cluster.leader(&topic, partition).is_err() {
                let topics = [topic.clone()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(&topic, partition)?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing describe_producers conn"))?;
                conn.roundtrip(
                    DESCRIBE_PRODUCERS,
                    version,
                    |buf| encode_describe_producers_request(buf, &topic, &[partition]),
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
            let resp = decode_describe_producers_response(&mut body.clone())?;
            let part = resp
                .topics
                .into_iter()
                .next()
                .and_then(|t| t.partitions.into_iter().next())
                .ok_or_else(|| Error::protocol("empty DescribeProducers response"))?;
            if part.error_code == 0 {
                return Ok(part);
            }
            let e = Error::broker(part.error_code, format!("{topic}-{partition}"));
            if e.is_retriable() {
                // NOT_LEADER_OR_FOLLOWER (6) and friends: Metadata, then the new leader.
                self.cluster.invalidate_topic(&topic);
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                let topics = [topic.clone()];
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Ok(part);
        }
    }

    /// Brokers, controller, and cluster id (`DescribeCluster`).
    pub async fn describe_cluster(&mut self) -> Result<ClusterDescription> {
        self.ensure_bootstrap().await?;
        let version = self.describe_cluster_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_CLUSTER,
                version,
                |buf| encode_describe_cluster_request(buf, false),
                timeout,
            )
            .await?;
        decode_describe_cluster_response(&mut body.clone())
    }

    /// Delete ACL bindings (`DeleteAcls`) matching `resource_type`.
    ///
    /// `resource_type` is [`crate::AclResourceType`] or a protocol `i8`.
    pub async fn delete_acls(&mut self, resource_type: impl Into<i8>) -> Result<i16> {
        let resource_type = resource_type.into();
        let version = self.delete_acls_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DELETE_ACLS,
                version,
                |buf| encode_delete_acls_request(buf, resource_type),
                timeout,
            )
            .await?;
        decode_delete_acls_response(&mut body.clone())
    }

    /// Delete committed offsets for `group_id` (OffsetDelete api 47).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// `COORDINATOR_LOAD_IN_PROGRESS` / `COORDINATOR_NOT_AVAILABLE` /
    /// `NOT_COORDINATOR` refresh the coordinator and retry.
    ///
    /// Each item is a [`crate::TopicPartition`] (or anything that converts
    /// to one).
    pub async fn delete_offsets(
        &mut self,
        group_id: &str,
        partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
    ) -> Result<Vec<OffsetDeleteResult>> {
        let partitions: Vec<(String, i32)> = partitions
            .into_iter()
            .map(|p| {
                let tp = p.into();
                (tp.topic, tp.partition)
            })
            .collect();
        let topics = offset_delete_topics(&partitions);
        let version = self.offset_delete_version;
        let timeout = self.cfg.request_timeout;
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

    /// Describe KIP-848 consumer groups (ConsumerGroupDescribe api 69).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Official Apache JSON listeners are `broker` only; the official
    /// response lists `NOT_COORDINATOR` among supported errors. This is
    /// not a controller hop and not a partition-leader hop: there is no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41) retry,
    /// and no `NOT_LEADER_OR_FOLLOWER` (6) hop. `COORDINATOR_LOAD_IN_PROGRESS`
    /// / `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. ErrorCode is per-group (bytes 5–6 on
    /// leftover-empty fixture group `"g"`), not top-level after throttle.
    pub async fn consumer_group_describe(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<DescribedConsumerGroup>> {
        let ids: Vec<String> = group_ids.iter().map(|s| (*s).to_string()).collect();
        let Some(coord_key) = ids.first().cloned() else {
            return Ok(Vec::new());
        };
        let version = self.consumer_group_describe_version;
        let timeout = self.cfg.request_timeout;
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
                    .ok_or_else(|| Error::protocol("missing consumer_group_describe conn"))?;
                conn.roundtrip(
                    CONSUMER_GROUP_DESCRIBE,
                    version,
                    |buf| {
                        encode_consumer_group_describe_request(
                            buf,
                            &ids,
                            include_authorized_operations,
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
            let results = decode_consumer_group_describe_response(&mut body.clone())?;
            if results
                .iter()
                .any(|r| error::coordinator_retriable(r.error_code))
            {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(results);
        }
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
    /// leftover-empty fixture group `"g"`), not top-level after throttle.
    pub async fn describe_groups(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<DescribedGroup>> {
        let ids: Vec<String> = group_ids.iter().map(|s| (*s).to_string()).collect();
        let Some(coord_key) = ids.first().cloned() else {
            return Ok(Vec::new());
        };
        let version = self.describe_groups_version;
        let timeout = self.cfg.request_timeout;
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
                    .ok_or_else(|| Error::protocol("missing describe_groups conn"))?;
                conn.roundtrip(
                    DESCRIBE_GROUPS,
                    version,
                    |buf| encode_describe_groups_request(buf, &ids, include_authorized_operations),
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
            let results = decode_describe_groups_response(&mut body.clone())?;
            if results
                .iter()
                .any(|r| error::coordinator_retriable(r.error_code))
            {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(results);
        }
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
    /// `error_code` is the INT16 at bytes 4–5, after throttle — not a
    /// first-group field.
    pub async fn list_groups(
        &mut self,
        states_filter: &[&str],
        types_filter: &[&str],
    ) -> Result<Vec<ListedGroup>> {
        let states: Vec<String> = states_filter.iter().map(|s| (*s).to_string()).collect();
        let types: Vec<String> = types_filter.iter().map(|s| (*s).to_string()).collect();
        let version = self.list_groups_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                LIST_GROUPS,
                version,
                |buf| encode_list_groups_request(buf, &states, &types),
                timeout,
            )
            .await?;
        let resp = decode_list_groups_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ListGroups"));
        }
        Ok(resp.groups)
    }

    /// Delete consumer groups (DeleteGroups api 42).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Official Apache JSON listeners are `broker` only. Official
    /// listed per-group errors include `NOT_COORDINATOR` (16). This is
    /// not a controller hop and not a partition-leader hop: there is no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41) retry,
    /// and no `NOT_LEADER_OR_FOLLOWER` (6) hop. `COORDINATOR_LOAD_IN_PROGRESS`
    /// / `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR` (16) refresh the
    /// coordinator and retry. ErrorCode is per-group after GroupId
    /// (bytes 7–8 on leftover-empty fixture group `"g"`), not top-level
    /// after throttle.
    pub async fn delete_groups(&mut self, group_ids: &[&str]) -> Result<Vec<DeletableGroupResult>> {
        let ids: Vec<String> = group_ids.iter().map(|s| (*s).to_string()).collect();
        let Some(coord_key) = ids.first().cloned() else {
            return Ok(Vec::new());
        };
        let version = self.delete_groups_version;
        let timeout = self.cfg.request_timeout;
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
                    .ok_or_else(|| Error::protocol("missing delete_groups conn"))?;
                conn.roundtrip(
                    DELETE_GROUPS,
                    version,
                    |buf| encode_delete_groups_request(buf, &ids),
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
            let results = decode_delete_groups_response(&mut body.clone())?;
            if results
                .iter()
                .any(|r| error::coordinator_retriable(r.error_code))
            {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(results);
        }
    }

    /// Describe KIP-932 share groups (ShareGroupDescribe api 77).
    ///
    /// Lands on the group coordinator (`FindCoordinator` `key_type=0`).
    /// Official Apache JSON listeners are `broker` only. Official listed
    /// errors include `NOT_COORDINATOR` (16). Official Java
    /// `DescribeShareGroupsHandler` uses `CoordinatorType.GROUP`. This is
    /// not a controller hop and not a partition-leader hop: there is no
    /// Metadata `controller_id` lookup, no `NOT_CONTROLLER` (41) retry,
    /// and no `NOT_LEADER_OR_FOLLOWER` (6) hop. SHARE (`key_type=2`) is
    /// the FindCoordinator v6 share-state key
    /// (`groupId:topicId:partition`) and is not used here.
    /// `COORDINATOR_LOAD_IN_PROGRESS` / `COORDINATOR_NOT_AVAILABLE` /
    /// `NOT_COORDINATOR` (16) refresh the coordinator and retry.
    /// ErrorCode is per-group (bytes 5–6 on leftover-empty fixture
    /// group `"g"`), not top-level after throttle.
    pub async fn share_group_describe(
        &mut self,
        group_ids: &[&str],
        include_authorized_operations: bool,
    ) -> Result<Vec<DescribedShareGroup>> {
        let ids: Vec<String> = group_ids.iter().map(|s| (*s).to_string()).collect();
        let Some(coord_key) = ids.first().cloned() else {
            return Ok(Vec::new());
        };
        let version = self.share_group_describe_version;
        let timeout = self.cfg.request_timeout;
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
                    .ok_or_else(|| Error::protocol("missing share_group_describe conn"))?;
                conn.roundtrip(
                    SHARE_GROUP_DESCRIBE,
                    version,
                    |buf| {
                        encode_share_group_describe_request(
                            buf,
                            &ids,
                            include_authorized_operations,
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
            let results = decode_share_group_describe_response(&mut body.clone())?;
            if results
                .iter()
                .any(|r| error::coordinator_retriable(r.error_code))
            {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(results);
        }
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
    pub async fn describe_share_group_offsets(
        &mut self,
        groups: &[DescribeShareGroupOffsetsGroup],
    ) -> Result<Vec<DescribedShareGroupOffsets>> {
        let Some(coord_key) = groups.first().map(|g| g.group_id.clone()) else {
            return Ok(Vec::new());
        };
        let version = self.describe_share_group_offsets_version;
        let timeout = self.cfg.request_timeout;
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
                    .ok_or_else(|| Error::protocol("missing describe_share_group_offsets conn"))?;
                conn.roundtrip(
                    DESCRIBE_SHARE_GROUP_OFFSETS,
                    version,
                    |buf| encode_describe_share_group_offsets_request(buf, groups),
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
            let results = decode_describe_share_group_offsets_response(&mut body.clone())?;
            if results
                .iter()
                .any(|r| error::coordinator_retriable(r.error_code))
            {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                self.wait_retry(&mut attempt, deadline).await?;
                continue;
            }
            return Ok(results);
        }
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
    pub async fn alter_share_group_offsets(
        &mut self,
        group_id: &str,
        topics: &[AlterShareGroupOffsetsTopic],
    ) -> Result<AlteredShareGroupOffsets> {
        let coord_key = group_id.to_string();
        let version = self.alter_share_group_offsets_version;
        let timeout = self.cfg.request_timeout;
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
    /// are names only — no partitions.
    pub async fn delete_share_group_offsets(
        &mut self,
        group_id: &str,
        topics: &[DeleteShareGroupOffsetsTopic],
    ) -> Result<DeletedShareGroupOffsets> {
        let coord_key = group_id.to_string();
        let version = self.delete_share_group_offsets_version;
        let timeout = self.cfg.request_timeout;
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
    pub async fn describe_topic_partitions(
        &mut self,
        topics: &[&str],
        response_partition_limit: i32,
        cursor: Option<&TopicPartitionCursor>,
    ) -> Result<DescribeTopicPartitionsResponse> {
        let names: Vec<String> = topics.iter().map(|s| (*s).to_string()).collect();
        let version = self.describe_topic_partitions_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_TOPIC_PARTITIONS,
                version,
                |buf| {
                    encode_describe_topic_partitions_request(
                        buf,
                        &names,
                        response_partition_limit,
                        cursor,
                    )
                },
                timeout,
            )
            .await?;
        decode_describe_topic_partitions_response(&mut body.clone())
    }

    /// List configuration resources (ListConfigResources api 74,
    /// KIP-1142; formerly ListClientMetricsResources).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
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
    /// (`CONFIG_RESOURCE_TOPIC`, …).
    pub async fn list_config_resources(
        &mut self,
        resource_types: impl IntoIterator<Item = impl Into<i8>>,
    ) -> Result<Vec<ListedConfigResource>> {
        let types: Vec<i8> = resource_types.into_iter().map(Into::into).collect();
        let version = self.list_config_resources_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                LIST_CONFIG_RESOURCES,
                version,
                |buf| encode_list_config_resources_request(buf, &types),
                timeout,
            )
            .await?;
        let resp = decode_list_config_resources_response(&mut body.clone())?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ListConfigResources"));
        }
        Ok(resp.config_resources)
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
        let version = self.get_telemetry_subscriptions_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
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
        let version = self.push_telemetry_version;
        let timeout = self.cfg.request_timeout;
        let req = PushTelemetryRequest::new(
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics.to_vec(),
        );
        let body = self
            .conn
            .roundtrip(
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
    /// directory store.
    pub async fn assign_replicas_to_dirs(
        &mut self,
        broker_id: i32,
        broker_epoch: i64,
        directories: Vec<AssignReplicasToDirsDirectory>,
    ) -> Result<AssignReplicasToDirsResponse> {
        let version = self.assign_replicas_to_dirs_version;
        let timeout = self.cfg.request_timeout;
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
    /// KIP-113).
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
    /// 12–13 on leftover-empty fixture topic `"t"` partition `0` —
    /// not a top-level field after throttle. Fixture directory path
    /// and topic/partition indexes only; this is not a log-dir store.
    pub async fn alter_replica_log_dirs(
        &mut self,
        dirs: Vec<AlterReplicaLogDirsDirectory>,
    ) -> Result<AlterReplicaLogDirsResponse> {
        let version = self.alter_replica_log_dirs_version;
        let timeout = self.cfg.request_timeout;
        let req = AlterReplicaLogDirsRequest::new(dirs);
        let body = self
            .conn
            .roundtrip(
                ALTER_REPLICA_LOG_DIRS,
                version,
                |buf| encode_alter_replica_log_dirs_request(buf, &req),
                timeout,
            )
            .await?;
        decode_alter_replica_log_dirs_response(&mut body.clone())
    }

    /// Describe log directories (DescribeLogDirs api 35, KIP-113 /
    /// KIP-784 / KIP-827).
    ///
    /// Lands on the connected broker (bootstrap is fine). Official
    /// Apache JSON listeners are `broker` only. Official JSON lists no
    /// `errorCodes`. Official Java
    /// `KafkaApis.handleDescribeLogDirsRequest` answers from the
    /// connected broker (`replicaManager.describeLogDirs`). Auth
    /// failure writes `CLUSTER_AUTHORIZATION_FAILED` (31) onto the
    /// top-level ErrorCode. Official `ReplicaManager.describeLogDirs`
    /// writes `KAFKA_STORAGE_ERROR` (56) onto a first-directory
    /// ErrorCode when that dir is offline. `NOT_COORDINATOR` (16) is
    /// not listed. `NOT_CONTROLLER` (41) is not listed. This is not a
    /// group-coordinator hop, not a controller hop, and not a
    /// partition-leader hop: there is no FindCoordinator, no Metadata
    /// `controller_id` lookup, no `NOT_CONTROLLER` (41) retry, and no
    /// `NOT_LEADER_OR_FOLLOWER` (6) hop. Top-level `error_code` is the
    /// INT16 at bytes 4–5, after throttle — not a first-directory
    /// field and not a first-partition field. Fixture directory path
    /// and topic/partition indexes only; this is not a log-dir store.
    /// Speaks v4 only (`VERSIONS.max`).
    pub async fn describe_log_dirs(
        &mut self,
        topics: Option<Vec<DescribableLogDirTopic>>,
    ) -> Result<DescribeLogDirsResponse> {
        let version = self.describe_log_dirs_version;
        let timeout = self.cfg.request_timeout;
        let req = DescribeLogDirsRequest::new(topics);
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_LOG_DIRS,
                version,
                |buf| encode_describe_log_dirs_request(buf, &req),
                timeout,
            )
            .await?;
        decode_describe_log_dirs_response(&mut body.clone())
    }

    /// Create a delegation token (CreateDelegationToken api 38, KIP-48 /
    /// KIP-373).
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
    /// store. Speaks v3 only (`VERSIONS.max`).
    pub async fn create_delegation_token(
        &mut self,
        req: CreateDelegationTokenRequest,
    ) -> Result<CreateDelegationTokenResponse> {
        let version = self.create_delegation_token_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                CREATE_DELEGATION_TOKEN,
                version,
                |buf| encode_create_delegation_token_request(buf, &req),
                timeout,
            )
            .await?;
        decode_create_delegation_token_response(&mut body.clone())
    }

    /// Renew a delegation token (RenewDelegationToken api 39, KIP-48 /
    /// KIP-373).
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
    /// only; this is not a token store. Speaks v2 only
    /// (`VERSIONS.max`). Do not copy CreateDelegationToken just
    /// because it is the previous slice.
    pub async fn renew_delegation_token(
        &mut self,
        req: RenewDelegationTokenRequest,
    ) -> Result<RenewDelegationTokenResponse> {
        let version = self.renew_delegation_token_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                RENEW_DELEGATION_TOKEN,
                version,
                |buf| encode_renew_delegation_token_request(buf, &req),
                timeout,
            )
            .await?;
        decode_renew_delegation_token_response(&mut body.clone())
    }

    /// Expire a delegation token (ExpireDelegationToken api 40, KIP-48 /
    /// KIP-373).
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
    /// only; this is not a token store. Speaks v2 only
    /// (`VERSIONS.max`). Do not copy RenewDelegationToken just
    /// because it is the previous slice.
    pub async fn expire_delegation_token(
        &mut self,
        req: ExpireDelegationTokenRequest,
    ) -> Result<ExpireDelegationTokenResponse> {
        let version = self.expire_delegation_token_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                EXPIRE_DELEGATION_TOKEN,
                version,
                |buf| encode_expire_delegation_token_request(buf, &req),
                timeout,
            )
            .await?;
        decode_expire_delegation_token_response(&mut body.clone())
    }

    /// Describe delegation tokens (DescribeDelegationToken api 41,
    /// KIP-48 / KIP-373).
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
    /// this is not a token store. Speaks v3 only
    /// (`VERSIONS.max`). Do not copy ExpireDelegationToken just
    /// because it is the previous slice.
    pub async fn describe_delegation_token(
        &mut self,
        req: DescribeDelegationTokenRequest,
    ) -> Result<DescribeDelegationTokenResponse> {
        let version = self.describe_delegation_token_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_DELEGATION_TOKEN,
                version,
                |buf| encode_describe_delegation_token_request(buf, &req),
                timeout,
            )
            .await?;
        decode_describe_delegation_token_response(&mut body.clone())
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
                .conn
                .roundtrip(
                    FIND_COORDINATOR,
                    version,
                    |buf| encode_find_coordinator_request_typed(buf, group_id, COORDINATOR_GROUP),
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
            let (err, node, _host, _port) = decode_find_coordinator_response(&mut body.clone())?;
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
                .conn
                .roundtrip(
                    FIND_COORDINATOR,
                    version,
                    |buf| {
                        encode_find_coordinator_request_typed(
                            buf,
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
            let (err, node, _host, _port) = decode_find_coordinator_response(&mut body.clone())?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn scram_mechanism_matches_protocol_consts() {
        assert_eq!(i8::from(ScramMechanism::Sha256), SCRAM_SHA_256);
        assert_eq!(i8::from(ScramMechanism::Sha512), SCRAM_SHA_512);
    }
}
