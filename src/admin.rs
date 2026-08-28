#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

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
    decode_alter_share_group_offsets_response, decode_alter_user_scram_credentials_response,
    decode_assign_replicas_to_dirs_response, decode_consumer_group_describe_response,
    decode_create_partitions_response, decode_create_topics_response,
    decode_delete_groups_response, decode_delete_records_response,
    decode_delete_share_group_offsets_response, decode_delete_topics_response,
    decode_describe_client_quotas_response, decode_describe_cluster_response,
    decode_describe_configs_response, decode_describe_groups_response,
    decode_describe_producers_response, decode_describe_share_group_offsets_response,
    decode_describe_topic_partitions_response, decode_describe_transactions_response,
    decode_describe_user_scram_credentials_response, decode_get_telemetry_subscriptions_response,
    decode_incremental_alter_configs_response, decode_list_config_resources_response,
    decode_list_groups_response, decode_list_partition_reassignments_response,
    decode_list_transactions_response, decode_push_telemetry_response,
    decode_share_group_describe_response, decode_unregister_broker_response,
    decode_update_features_response, encode_allocate_producer_ids_request,
    encode_alter_client_quotas_request, encode_alter_configs_request,
    encode_alter_partition_reassignments_request, encode_alter_share_group_offsets_request,
    encode_alter_user_scram_credentials_request, encode_assign_replicas_to_dirs_request,
    encode_consumer_group_describe_request, encode_create_partitions_request,
    encode_create_topics_request, encode_delete_groups_request, encode_delete_records_request,
    encode_delete_share_group_offsets_request, encode_delete_topics_request,
    encode_describe_client_quotas_request, encode_describe_cluster_request,
    encode_describe_configs_request, encode_describe_groups_request,
    encode_describe_producers_request, encode_describe_share_group_offsets_request,
    encode_describe_topic_partitions_request, encode_describe_transactions_request,
    encode_describe_user_scram_credentials_request, encode_get_telemetry_subscriptions_request,
    encode_incremental_alter_configs_request, encode_list_config_resources_request,
    encode_list_groups_request, encode_list_partition_reassignments_request,
    encode_list_transactions_request, encode_push_telemetry_request,
    encode_share_group_describe_request, encode_unregister_broker_request,
    encode_update_features_request, CreatableTopic, CreateTopicsRequest, DescribeConfigsResource,
    DescribeConfigsResult, FeatureUpdateKey, ListReassignmentTopic, ReassignablePartition,
    ReassignableTopic, ScramCredentialDeletion, ScramCredentialUpsertion, TopicConfig, TopicResult,
    RESOURCE_BROKER, RESOURCE_TOPIC,
};
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, encode_api_versions_request,
    encode_metadata_request, ApiVersion,
};
use crate::protocol::api_keys::{
    pick_version, ALLOCATE_PRODUCER_IDS, ALTER_CLIENT_QUOTAS, ALTER_CONFIGS,
    ALTER_PARTITION_REASSIGNMENTS, ALTER_SHARE_GROUP_OFFSETS, ALTER_USER_SCRAM_CREDENTIALS,
    API_VERSIONS, ASSIGN_REPLICAS_TO_DIRS, CONSUMER_GROUP_DESCRIBE, CREATE_ACLS, CREATE_PARTITIONS,
    CREATE_TOPICS, DELETE_ACLS, DELETE_GROUPS, DELETE_RECORDS, DELETE_SHARE_GROUP_OFFSETS,
    DELETE_TOPICS, DESCRIBE_ACLS, DESCRIBE_CLIENT_QUOTAS, DESCRIBE_CLUSTER, DESCRIBE_CONFIGS,
    DESCRIBE_GROUPS, DESCRIBE_PRODUCERS, DESCRIBE_SHARE_GROUP_OFFSETS, DESCRIBE_TOPIC_PARTITIONS,
    DESCRIBE_TRANSACTIONS, DESCRIBE_USER_SCRAM_CREDENTIALS, FIND_COORDINATOR,
    GET_TELEMETRY_SUBSCRIPTIONS, INCREMENTAL_ALTER_CONFIGS, LIST_CONFIG_RESOURCES, LIST_GROUPS,
    LIST_PARTITION_REASSIGNMENTS, LIST_TRANSACTIONS, METADATA, OFFSET_DELETE, PUSH_TELEMETRY,
    SHARE_GROUP_DESCRIBE, UNREGISTER_BROKER, UPDATE_FEATURES,
};
use crate::protocol::group::{
    decode_find_coordinator_response, decode_offset_delete_response,
    encode_find_coordinator_request_typed, encode_offset_delete_request, OffsetDeleteTopic,
    COORDINATOR_GROUP, COORDINATOR_TRANSACTION,
};
use crate::protocol::sasl;

pub use crate::protocol::acl::AclBinding;
pub use crate::protocol::admin::{
    ActiveProducer, AlterConfig, AlterShareGroupOffsetsPartition, AlterShareGroupOffsetsTopic,
    AlteredShareGroupOffsets, AlteredShareGroupOffsetsPartition, AlteredShareGroupOffsetsTopic,
    AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition, AssignReplicasToDirsRequest,
    AssignReplicasToDirsResponse, AssignReplicasToDirsResponseDirectory,
    AssignReplicasToDirsResponsePartition, AssignReplicasToDirsResponseTopic,
    AssignReplicasToDirsTopic, ClientQuotaAlteration, ClientQuotaAlterationResult,
    ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilterComponent, ClientQuotaOp,
    ClientQuotaValue, ClusterDescription, ConfigEntry, ConfigSynonym, ConsumerGroupAssignment,
    ConsumerGroupMember, ConsumerGroupTopicPartitions, DeletableGroupResult,
    DeleteShareGroupOffsetsTopic, DeletedShareGroupOffsets, DeletedShareGroupOffsetsTopic,
    DescribeProducersPartition, DescribeShareGroupOffsetsGroup, DescribeShareGroupOffsetsTopic,
    DescribeTopicPartitionsResponse, DescribeUserScramCredentialsResult, DescribedConsumerGroup,
    DescribedGroup, DescribedGroupMember, DescribedShareGroup, DescribedShareGroupOffsets,
    DescribedShareGroupOffsetsPartition, DescribedShareGroupOffsetsTopic, DescribedTopicPartition,
    DescribedTopicPartitions, GetTelemetrySubscriptionsResponse, ListedConfigResource, ListedGroup,
    PushTelemetryRequest, PushTelemetryResponse, ScramCredentialInfo, ShareGroupAssignment,
    ShareGroupMember, ShareGroupTopicPartitions, TopicPartitionCursor, TransactionListing,
    TransactionState, TransactionTopic, ALTER_CONFIG_DELETE, ALTER_CONFIG_SET,
    AUTHORIZED_OPERATIONS_OMITTED, QUOTA_MATCH_ANY, QUOTA_MATCH_DEFAULT, QUOTA_MATCH_EXACT,
    RESOURCE_BROKER as CONFIG_RESOURCE_BROKER,
    RESOURCE_BROKER_LOGGER as CONFIG_RESOURCE_BROKER_LOGGER,
    RESOURCE_CLIENT_METRICS as CONFIG_RESOURCE_CLIENT_METRICS,
    RESOURCE_GROUP as CONFIG_RESOURCE_GROUP, RESOURCE_TOPIC as CONFIG_RESOURCE_TOPIC,
    SCRAM_SHA_256, SCRAM_SHA_512,
};
pub use crate::protocol::group::OffsetDeleteResult;

#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub bootstrap: Vec<String>,
    pub client_id: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub sasl_plain: Option<(String, String)>,
    pub sasl_scram: Option<(String, String)>,
    pub sasl_scram_sha512: Option<(String, String)>,
    pub sasl_oauthbearer: Option<String>,
    pub sasl_oauthbearer_oidc: Option<crate::OidcConfig>,
    pub tls: Option<TlsConfig>,
}

impl Default for AdminConfig {
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
        }
    }
}

impl AdminConfig {
    pub fn bootstrap<S: Into<String>>(servers: impl IntoIterator<Item = S>) -> Self {
        Self {
            bootstrap: servers.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: Vec<(String, Option<String>)>,
}

impl NewTopic {
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            configs: Vec::new(),
        }
    }

    pub fn config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((name.into(), Some(value.into())));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResource {
    pub resource_type: i8,
    pub name: String,
    pub keys: Option<Vec<String>>,
}

impl ConfigResource {
    pub fn topic(name: impl Into<String>) -> Self {
        Self {
            resource_type: RESOURCE_TOPIC,
            name: name.into(),
            keys: None,
        }
    }

    pub fn broker(id: i32) -> Self {
        Self {
            resource_type: RESOURCE_BROKER,
            name: id.to_string(),
            keys: None,
        }
    }

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
    pub topic: String,
    pub partition: i32,
    pub replicas: Option<Vec<i32>>,
}

impl PartitionReassignment {
    pub fn assign(topic: impl Into<String>, partition: i32, replicas: Vec<i32>) -> Self {
        Self {
            topic: topic.into(),
            partition,
            replicas: Some(replicas),
        }
    }

    pub fn cancel(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            replicas: None,
        }
    }
}

/// Flattened per-partition result of AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignmentResult {
    pub topic: String,
    pub partition: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// Flattened ongoing reassignment from ListPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingReassignment {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
}

/// One finalized-feature update for `Admin::update_features` (v0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdate {
    pub name: String,
    pub max_version_level: i16,
    pub allow_downgrade: bool,
}

impl FeatureUpdate {
    pub fn new(name: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            name: name.into(),
            max_version_level,
            allow_downgrade: false,
        }
    }

    pub fn allow_downgrade(mut self, allow: bool) -> Self {
        self.allow_downgrade = allow;
        self
    }
}

/// Per-feature result of UpdateFeatures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateResult {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// One SCRAM credential to remove for `Admin::alter_user_scram_credentials`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScramCredentialDeletion {
    pub name: String,
    pub mechanism: i8,
}

impl UserScramCredentialDeletion {
    pub fn new(name: impl Into<String>, mechanism: i8) -> Self {
        Self {
            name: name.into(),
            mechanism,
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
    pub name: String,
    pub mechanism: i8,
    pub iterations: i32,
    pub salt: Vec<u8>,
    pub salted_password: Vec<u8>,
}

impl UserScramCredentialUpsertion {
    pub fn new(
        name: impl Into<String>,
        mechanism: i8,
        iterations: i32,
        salt: impl Into<Vec<u8>>,
        salted_password: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            name: name.into(),
            mechanism,
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
    pub user: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// PID block from `Admin::allocate_producer_ids` (AllocateProducerIds api 67).
///
/// Fixture broker id/epoch only. This is not a live cluster PID allocator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerIdBlock {
    pub producer_id_start: i64,
    pub producer_id_len: i32,
}

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
    push_telemetry_version: i16,
    assign_replicas_to_dirs_version: i16,
    cluster: Cluster,
    conns: HashMap<i32, BrokerConn>,
    group_coord: Option<(String, i32)>,
    txn_coord: Option<(String, i32)>,
}

impl Admin {
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(AdminConfig::bootstrap([bootstrap.into()])).await
    }

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
            push_telemetry_version,
            assign_replicas_to_dirs_version,
            cluster: Cluster::default(),
            conns: HashMap::new(),
            group_coord: None,
            txn_coord: None,
        })
    }

    pub fn versions(&self) -> &HashMap<i16, ApiVersion> {
        &self.versions
    }

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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(results);
        }
    }

    pub async fn delete_topics(
        &mut self,
        names: &[impl AsRef<str>],
        timeout_ms: i32,
    ) -> Result<Vec<TopicResult>> {
        let names: Vec<String> = names.iter().map(|n| n.as_ref().to_string()).collect();
        let version = self.delete_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(results);
        }
    }

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

    pub async fn create_partitions(
        &mut self,
        topics: &[(String, i32)],
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let topics = topics.to_vec();
        let version = self.partitions_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(results);
        }
    }

    pub async fn incremental_alter_configs(
        &mut self,
        resource_type: i8,
        name: &str,
        configs: &[AlterConfig],
        validate_only: bool,
    ) -> Result<i16> {
        let version = self.alter_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        let name = name.to_string();
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let err = decode_incremental_alter_configs_response(&mut body.clone())?;
            if err == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(err);
        }
    }

    pub async fn create_acls(&mut self, acls: &[AclBinding]) -> Result<Vec<i16>> {
        let version = self.create_acls_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results = decode_create_acls_response(&mut body.clone())?;
            if results.contains(&error::NOT_CONTROLLER) {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
        partitions: Option<&[(String, i32)]>,
        timeout_ms: i32,
    ) -> Result<Vec<OngoingReassignment>> {
        let topics = partitions.map(group_list_reassignments);
        let version = self.list_reassign_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_list_partition_reassignments_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_describe_user_scram_credentials_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_unregister_broker_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_allocate_producer_ids_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_list_transactions_response(&mut body.clone())?;
            if error::coordinator_retriable(resp.error_code) {
                // 14/15/16: FindCoordinator, then the new txn coordinator.
                self.txn_coord = None;
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "ListTransactions"));
            }
            return Ok(resp.transaction_states);
        }
    }

    pub async fn describe_acls(&mut self, resource_type: i8) -> Result<Vec<AclBinding>> {
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

    pub async fn alter_configs(
        &mut self,
        resource_type: i8,
        name: &str,
        configs: &[(String, Option<String>)],
        validate_only: bool,
    ) -> Result<i16> {
        let version = self.legacy_alter_version;
        let timeout = self.cfg.request_timeout;
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
                        name,
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

    pub async fn delete_records(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        timeout_ms: i32,
    ) -> Result<(i64, i16)> {
        let version = self.delete_records_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        loop {
            if self.cluster.leader(topic, partition).is_err() {
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(topic, partition)?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing delete_records conn"))?;
                conn.roundtrip(
                    DELETE_RECORDS,
                    version,
                    |buf| encode_delete_records_request(buf, topic, partition, offset, timeout_ms),
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
            let (_p, low, err) = decode_delete_records_response(&mut body.clone(), version)?;
            if err == 0 {
                return Ok((low, err));
            }
            let e = Error::broker(err, format!("{topic}-{partition}"));
            if e.is_retriable() {
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
        topic: &str,
        partition: i32,
    ) -> Result<DescribeProducersPartition> {
        let version = self.describe_producers_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        loop {
            if self.cluster.leader(topic, partition).is_err() {
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(topic, partition)?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing describe_producers conn"))?;
                conn.roundtrip(
                    DESCRIBE_PRODUCERS,
                    version,
                    |buf| encode_describe_producers_request(buf, topic, &[partition]),
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
                self.cluster.invalidate_topic(topic);
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Ok(part);
        }
    }

    pub async fn describe_cluster(&mut self) -> Result<ClusterDescription> {
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

    pub async fn delete_acls(&mut self, resource_type: i8) -> Result<i16> {
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
    pub async fn delete_offsets(
        &mut self,
        group_id: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<OffsetDeleteResult>> {
        let topics = offset_delete_topics(partitions);
        let version = self.offset_delete_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (top, results) = decode_offset_delete_response(&mut body.clone())?;
            if error::coordinator_retriable(top) {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let result = decode_alter_share_group_offsets_response(&mut body.clone())?;
            if error::coordinator_retriable(result.error_code) {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let result = decode_delete_share_group_offsets_response(&mut body.clone())?;
            if error::coordinator_retriable(result.error_code) {
                // 14/15/16: FindCoordinator, then the new group coordinator.
                self.group_coord = None;
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
    pub async fn list_config_resources(
        &mut self,
        resource_types: &[i8],
    ) -> Result<Vec<ListedConfigResource>> {
        let types = resource_types.to_vec();
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp = decode_assign_replicas_to_dirs_response(&mut body.clone())?;
            if resp.error_code == error::NOT_CONTROLLER {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                self.refresh_metadata(None).await?;
                continue;
            }
            if resp.error_code != 0 {
                return Err(Error::broker(resp.error_code, "AssignReplicasToDirs"));
            }
            return Ok(resp);
        }
    }

    async fn discover_group_coord(&mut self, group_id: &str) -> Result<i32> {
        if self.cluster.brokers.is_empty() {
            self.refresh_metadata(None).await?;
        }
        let version = self.find_coord_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
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
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
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

fn group_list_reassignments(partitions: &[(String, i32)]) -> Vec<ListReassignmentTopic> {
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
