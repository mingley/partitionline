//! Request and response headers (classic header v0/v1 and flexible v1/v2).

use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::api_keys::{
    ADD_OFFSETS_TO_TXN, ADD_PARTITIONS_TO_TXN, ALLOCATE_PRODUCER_IDS, ALTER_CLIENT_QUOTAS,
    ALTER_CONFIGS, ALTER_PARTITION_REASSIGNMENTS, ALTER_REPLICA_LOG_DIRS,
    ALTER_SHARE_GROUP_OFFSETS, ALTER_USER_SCRAM_CREDENTIALS, API_VERSIONS, ASSIGN_REPLICAS_TO_DIRS,
    CONSUMER_GROUP_DESCRIBE, CONSUMER_GROUP_HEARTBEAT, CREATE_ACLS, CREATE_DELEGATION_TOKEN,
    CREATE_PARTITIONS, CREATE_TOPICS, DELETE_ACLS, DELETE_GROUPS, DELETE_RECORDS,
    DELETE_SHARE_GROUP_OFFSETS, DELETE_TOPICS, DESCRIBE_ACLS, DESCRIBE_CLIENT_QUOTAS,
    DESCRIBE_CLUSTER, DESCRIBE_CONFIGS, DESCRIBE_DELEGATION_TOKEN, DESCRIBE_GROUPS,
    DESCRIBE_LOG_DIRS, DESCRIBE_PRODUCERS, DESCRIBE_SHARE_GROUP_OFFSETS, DESCRIBE_TOPIC_PARTITIONS,
    DESCRIBE_TRANSACTIONS, DESCRIBE_USER_SCRAM_CREDENTIALS, END_TXN, EXPIRE_DELEGATION_TOKEN,
    FETCH, FIND_COORDINATOR, GET_TELEMETRY_SUBSCRIPTIONS, HEARTBEAT, INCREMENTAL_ALTER_CONFIGS,
    INIT_PRODUCER_ID, JOIN_GROUP, LEAVE_GROUP, LIST_CONFIG_RESOURCES, LIST_GROUPS, LIST_OFFSETS,
    LIST_PARTITION_REASSIGNMENTS, LIST_TRANSACTIONS, METADATA, OFFSET_COMMIT, OFFSET_FETCH,
    OFFSET_FOR_LEADER_EPOCH, PRODUCE, PUSH_TELEMETRY, RENEW_DELEGATION_TOKEN, SASL_AUTHENTICATE,
    SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_DESCRIBE, SHARE_GROUP_HEARTBEAT, SYNC_GROUP,
    TXN_OFFSET_COMMIT, UNREGISTER_BROKER, UPDATE_FEATURES, WRITE_TXN_MARKERS,
};
use super::buf;
use crate::error::{Error, Result};

/// Kafka request header (`api_key` through `client_id`, plus tagged fields when flexible).
///
/// [`Display`] is Java `RequestHeader.toString`:
/// `RequestHeader(apiKey=PRODUCE, apiVersion=9, clientId=client, correlationId=1, headerVersion=2)`.
/// `apiKey` is the Kafka 4.0 `ApiKeys` enum name (not `ApiMessageType.name`).
/// Null `clientId` prints `null`; an empty id prints empty.
/// [`Self::size`] is Java `RequestHeader.size`.
#[derive(Debug, Clone)]
pub struct RequestHeader {
    /// Api key.
    pub api_key: i16,
    /// Api version.
    pub api_version: i16,
    /// Correlation id echoed on the response.
    pub correlation_id: i32,
    /// Kafka `client.id`, or `None` for a null string.
    pub client_id: Option<String>,
}

impl RequestHeader {
    /// Java `RequestHeader.apiKey` id.
    #[must_use]
    pub const fn api_key(&self) -> i16 {
        self.api_key
    }

    /// Java `RequestHeader.apiVersion`.
    #[must_use]
    pub const fn api_version(&self) -> i16 {
        self.api_version
    }

    /// Java `RequestHeader.correlationId`.
    #[must_use]
    pub const fn correlation_id(&self) -> i32 {
        self.correlation_id
    }

    /// Java `RequestHeader.clientId` (`None` is Java `null`).
    #[must_use]
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Java `RequestHeader.headerVersion`.
    #[must_use]
    pub fn header_version(&self) -> i16 {
        request_header_version(self.api_key, self.api_version)
    }

    /// Java `RequestHeader.size`.
    ///
    /// Serialized bytes: INT16 api key, INT16 api version, INT32 correlation
    /// id, classic nullable STRING `clientId`, plus one empty tagged-fields
    /// unsigned varint when [`Self::header_version`] is 2. Matches
    /// [`encode_request_header`].
    #[must_use]
    pub fn size(&self) -> i32 {
        let client_bytes = self.client_id.as_deref().map_or(0, str::len);
        let mut n = 10i32; // 2+2+4 + INT16 length
        match i32::try_from(client_bytes) {
            Ok(len) => n = n.saturating_add(len),
            Err(_) => return i32::MAX,
        }
        if self.header_version() >= 2 {
            n = n.saturating_add(1);
        }
        n
    }

    /// Java `RequestHeader.toResponseHeader`.
    ///
    /// Copies [`Self::correlation_id`]. Java also stores
    /// `apiKey.responseHeaderVersion(apiVersion)` on the result; this crate's
    /// [`ResponseHeader`] only has `correlationId`.
    #[must_use]
    pub const fn to_response_header(&self) -> ResponseHeader {
        ResponseHeader {
            correlation_id: self.correlation_id,
        }
    }

    /// Java `AbstractResponse.parseResponse` correlation-id check
    /// (`CorrelationIdMismatchException`).
    pub fn check_correlation(&self, response: &ResponseHeader) -> Result<()> {
        if self.correlation_id() == response.correlation_id() {
            Ok(())
        } else {
            Err(Error::protocol(format!(
                "Correlation id for response ({}) does not match request ({}), request header: {self}",
                response.correlation_id(),
                self.correlation_id()
            )))
        }
    }
}

impl fmt::Display for RequestHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RequestHeader(apiKey=")?;
        match super::api_keys::name(self.api_key) {
            Some(n) => f.write_str(n)?,
            None => write!(f, "{}", self.api_key)?,
        }
        write!(f, ", apiVersion={}, clientId=", self.api_version)?;
        match self.client_id.as_deref() {
            Some(id) => f.write_str(id)?,
            None => f.write_str("null")?,
        }
        write!(
            f,
            ", correlationId={}, headerVersion={})",
            self.correlation_id,
            self.header_version()
        )
    }
}

/// Kafka response header (correlation id, plus tagged fields when flexible).
///
/// Java `ResponseHeader.toString` includes `headerVersion`; this type only
/// stores `correlationId`. [`response_header_size`] is Java
/// `ResponseHeader.size` for a known header version.
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    /// Correlation id from the request.
    pub correlation_id: i32,
}

impl ResponseHeader {
    /// Java `ResponseHeader.correlationId`.
    #[must_use]
    pub const fn correlation_id(&self) -> i32 {
        self.correlation_id
    }
}

/// Java `ResponseHeader.size` for `headerVersion`.
///
/// Classic (v0) is 4 bytes (INT32 correlation id). Flexible (v1+) is 5
/// (plus one empty tagged-fields unsigned varint). Matches
/// [`encode_response_header`].
#[must_use]
pub const fn response_header_size(header_version: i16) -> i32 {
    if header_version >= 1 {
        5
    } else {
        4
    }
}

/// Request header version: `1` classic, `2` flexible (KIP-482 tagged fields).
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        // ApiVersions is classic through v2; flexible from v3
        // (Apache JSON flexibleVersions: "3+"). Kafka 4.0 validVersions
        // is 0-4. This crate speaks 0–4. v4 is the same request as v3
        // (KAFKA-17011 MinVersion 0). Response header is never flexible.
        API_VERSIONS if api_version >= 3 => 2,
        PRODUCE if api_version >= 9 => 2,
        FETCH if api_version >= 12 => 2,
        // InitProducerId is classic through v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+"). This crate speaks 0–5.
        INIT_PRODUCER_ID if api_version >= 2 => 2,
        // FindCoordinator is classic through v2; flexible from v3
        // (Apache JSON flexibleVersions: "3+"). This crate speaks 1–6.
        // v4+ is KIP-699 CoordinatorKeys (same header as v3).
        FIND_COORDINATOR if api_version >= 3 => 2,
        // Metadata is classic through v8; flexible from v9
        // (Apache JSON flexibleVersions: "9+"). This crate speaks 1–13.
        // v13 adds top-level ErrorCode (same header as v9–v12).
        METADATA if api_version >= 9 => 2,
        // DescribeCluster is flexible from v0 (Apache JSON flexibleVersions: "0+").
        // Kafka 4.0 validVersions is 0-2. This crate speaks 0–2.
        // v1 EndpointType (KIP-919). v2 IncludeFencedBrokers / IsFenced
        // (KIP-1073). v3+ is not spoken.
        DESCRIBE_CLUSTER
        // UpdateFeatures is flexible from v0 (Apache JSON flexibleVersions: "0+").
        // Kafka 4.0 validVersions is 0-2. This crate speaks 0–2.
        // v1 UpgradeType / ValidateOnly. v2 omits Results. v3+ is not spoken.
        | UPDATE_FEATURES
        | ALTER_PARTITION_REASSIGNMENTS
        | LIST_PARTITION_REASSIGNMENTS
        | ALTER_USER_SCRAM_CREDENTIALS
        | DESCRIBE_USER_SCRAM_CREDENTIALS
        | ALLOCATE_PRODUCER_IDS
        | DESCRIBE_TRANSACTIONS
        | LIST_TRANSACTIONS
        | UNREGISTER_BROKER
        | DESCRIBE_PRODUCERS => 2,
        // DescribeClientQuotas / AlterClientQuotas are classic at v0;
        // flexible from v1 (Apache JSON flexibleVersions: "1+",
        // kafka-protocol 0.18.0). Kafka 4.0 validVersions is 0-1.
        // This crate speaks 0–1. v2+ is not spoken.
        DESCRIBE_CLIENT_QUOTAS | ALTER_CLIENT_QUOTAS if api_version >= 1 => 2,
        // DescribeGroups is classic through v4; flexible from v5
        // (Apache JSON flexibleVersions: "5+", kafka-protocol 0.18.0).
        // Kafka 4.0 validVersions is 0-6. This crate speaks 0–6.
        // v3 IncludeAuthorizedOperations. v4 GroupInstanceId. v6
        // ErrorMessage (KIP-1043). v7+ is not spoken.
        DESCRIBE_GROUPS if api_version >= 5 => 2,
        // ListGroups is classic through v2; flexible from v3
        // (Apache JSON flexibleVersions: "3+", kafka-protocol 0.18.0).
        // Kafka 4.0 validVersions is 0-5. This crate speaks 0–5.
        // v4 StatesFilter / GroupState. v5 TypesFilter / GroupType.
        // v6+ is not spoken.
        LIST_GROUPS if api_version >= 3 => 2,
        // DeleteGroups is classic through v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // Kafka 4.0 validVersions is 0-2. This crate speaks 0–2.
        // v3+ is not spoken.
        DELETE_GROUPS if api_version >= 2 => 2,
        // WriteTxnMarkers is classic at v0; flexible from v1
        // (Apache JSON flexibleVersions: "1+"). Kafka 4.0 removed v0;
        // this crate speaks 0–1.
        WRITE_TXN_MARKERS if api_version >= 1 => 2,
        // AddPartitionsToTxn is classic through v2; flexible from v3
        // (Apache JSON flexibleVersions: "3+"). This crate speaks 0–3.
        // v4+ (batched transactions) is not spoken.
        ADD_PARTITIONS_TO_TXN if api_version >= 3 => 2,
        // AddOffsetsToTxn is classic through v2; flexible from v3
        // (Apache JSON flexibleVersions: "3+"). This crate speaks 0–4.
        // v4 is TRANSACTION_ABORTABLE (KIP-890; same layout as v3).
        ADD_OFFSETS_TO_TXN if api_version >= 3 => 2,
        // EndTxn is classic through v2; flexible from v3
        // (Apache JSON flexibleVersions: "3+"). This crate speaks 0–5.
        // v4 is TRANSACTION_ABORTABLE (KIP-890; same request layout as v3).
        // v5 adds ProducerId / ProducerEpoch on the response.
        END_TXN if api_version >= 3 => 2,
        // TxnOffsetCommit is classic through v2; flexible from v3
        // (Apache JSON flexibleVersions: "3+"). This crate speaks 0–5.
        // v3 adds GenerationId / MemberId / GroupInstanceId. v4 is
        // TRANSACTION_ABORTABLE (KIP-890; same layout as v3). v5 is
        // transaction V2 (KIP-890 Part 2; same layout as v3–v4).
        TXN_OFFSET_COMMIT if api_version >= 3 => 2,
        // LeaveGroup is classic through v3; flexible from v4
        // (Apache JSON flexibleVersions: "4+"). This crate speaks 0–5
        // (v5 is KIP-800 Reason).
        LEAVE_GROUP if api_version >= 4 => 2,
        // ListOffsets is classic through v5; flexible from v6
        // (Apache JSON flexibleVersions: "6+"). Kafka 4.0 removed v0;
        // this crate speaks 1–10. v10 TimeoutMs (KIP-1075) follows Topics.
        LIST_OFFSETS if api_version >= 6 => 2,
        // OffsetCommit is classic through v7; flexible from v8
        // (Apache JSON flexibleVersions: "8+"). Kafka 4.0 validVersions
        // is 2-9 (v0–v1 removed). This crate speaks 2–9. v2–v4
        // RetentionTimeMs. v6 CommittedLeaderEpoch. v7 GroupInstanceId.
        // v9 is KIP-848 errors (same layout). v0–v1 and v10+ are not
        // spoken.
        OFFSET_COMMIT if api_version >= 8 => 2,
        // OffsetFetch is classic through v5; flexible from v6
        // (Apache JSON flexibleVersions: "6+"). Kafka 4.0 validVersions
        // is 1-9 (v0 removed). This crate speaks 1–9. v2 top-level
        // ErrorCode. v3 ThrottleTimeMs. v5 CommittedLeaderEpoch. v7
        // RequireStable. v8 Groups. v9 MemberId / MemberEpoch (same
        // header as v6). v0 and v10+ are not spoken.
        OFFSET_FETCH if api_version >= 6 => 2,
        // OffsetForLeaderEpoch is classic through v3; flexible from v4
        // (Apache JSON flexibleVersions: "4+"). Kafka 4.0 validVersions
        // is 2-4 (v0–v1 removed). This crate speaks 0–4. v2
        // CurrentLeaderEpoch. v3 ReplicaId. v5+ is not spoken.
        OFFSET_FOR_LEADER_EPOCH if api_version >= 4 => 2,
        // Heartbeat is classic through v3; flexible from v4
        // (Apache JSON flexibleVersions: "4+"). Kafka 4.0 validVersions
        // is 0-4. This crate speaks 0–4. v1 and v2 match v0. v3
        // GroupInstanceId. v5+ is not spoken.
        HEARTBEAT if api_version >= 4 => 2,
        // SyncGroup is classic through v3; flexible from v4
        // (Apache JSON flexibleVersions: "4+"). Kafka 4.0 validVersions
        // is 0-5. This crate speaks 0–5. v1 and v2 match v0. v3
        // GroupInstanceId. v5 ProtocolType / ProtocolName (KIP-559)
        // keep the v4 header. v6+ is not spoken.
        SYNC_GROUP if api_version >= 4 => 2,
        // JoinGroup is classic through v5; flexible from v6
        // (Apache JSON flexibleVersions: "6+"). Kafka 4.0 validVersions
        // is 2-9 (v0–v1 removed). This crate speaks 2–9. v5
        // GroupInstanceId. v8 Reason and v9 SkipAssignment keep the v6
        // header. v0–v1 and v10+ are not spoken.
        JOIN_GROUP if api_version >= 6 => 2,
        // CreateTopics is classic through v4; flexible from v5
        // (Apache JSON flexibleVersions: "5+"). Kafka 4.0 validVersions
        // is 2-7. This crate speaks 0–7. v5 returns configs (KIP-525);
        // v7 TopicId (KIP-516). v8+ is not spoken.
        CREATE_TOPICS if api_version >= 5 => 2,
        // DeleteTopics is classic through v3; flexible from v4
        // (Apache JSON flexibleVersions: "4+"). Kafka 4.0 validVersions
        // is 1-6 (v0 removed). This crate speaks 0–6. v5 ErrorMessage
        // (KIP-599); v6 Topics + TopicId (KIP-516). v7+ is not spoken.
        DELETE_TOPICS if api_version >= 4 => 2,
        // DescribeConfigs is classic through v3; flexible from v4
        // (Apache JSON flexibleVersions: "4+"). Kafka 4.0 validVersions
        // is 1-4 (v0 removed). This crate speaks 0–4. v3 IncludeDocumentation
        // / ConfigType (KIP-226). v5+ is not spoken.
        DESCRIBE_CONFIGS if api_version >= 4 => 2,
        // CreatePartitions is classic through v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+"). Kafka 4.0 validVersions
        // is 0-3. This crate speaks 0–3. v3 is the same layout as v2
        // (KIP-599 THROTTLING_QUOTA_EXCEEDED). v4+ is not spoken.
        CREATE_PARTITIONS if api_version >= 2 => 2,
        // IncrementalAlterConfigs is classic at v0; flexible from v1
        // (Apache JSON flexibleVersions: "1+"). Kafka 4.0 validVersions
        // is 0-1. This crate speaks 0–1. v2+ is not spoken.
        INCREMENTAL_ALTER_CONFIGS if api_version >= 1 => 2,
        // AlterConfigs is classic through v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+"). Kafka 4.0 validVersions
        // is 0-2. This crate speaks 0–2. v1 ThrottleTimeMs (KIP-219).
        // v3+ is not spoken.
        ALTER_CONFIGS if api_version >= 2 => 2,
        // DeleteRecords is classic through v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+"). Kafka 4.0 validVersions
        // is 0-2. This crate speaks 0–2. v1 ThrottleTimeMs (KIP-219).
        // v3+ is not spoken.
        DELETE_RECORDS if api_version >= 2 => 2,
        // CreateAcls / DescribeAcls / DeleteAcls are classic through v1;
        // flexible from v2 (Apache JSON flexibleVersions: "2+"). Kafka 4.0
        // validVersions is 1-3 (v0 removed). This crate speaks 0–3. v1
        // ResourcePatternType. v3 user resource type (same layout as v2).
        // v4+ is not spoken.
        CREATE_ACLS | DESCRIBE_ACLS | DELETE_ACLS if api_version >= 2 => 2,
        // ConsumerGroupHeartbeat is flexible from v0 (Apache JSON
        // flexibleVersions: "0+"). Kafka 4.0 validVersions is 0-1.
        // This crate speaks 0–1. v1 SubscribedTopicRegex (KIP-848) and
        // client-generated MemberId (KIP-1082). v2+ is not spoken.
        // ShareGroupHeartbeat is flexible from v0. Kafka 4.0
        // validVersions is "0" (unstable). Kafka 4.1 validVersions is
        // "1" (v0 removed). This crate speaks 0–1. Same fields. v2+
        // is not spoken. ShareGroupDescribe is the same: Kafka 4.0
        // validVersions is "0"; Kafka 4.1 validVersions is "1"
        // (v0 removed). This crate speaks 0–1. Same fields. v2+
        // is not spoken. ShareFetch is flexible from v0. Kafka 4.0
        // validVersions is "0"; Kafka 4.1 validVersions is "1"
        // (v0 removed). This crate speaks 0–1. v0 PartitionMaxBytes;
        // v1 MaxRecords / BatchSize / AcquisitionLockTimeoutMs. v2+
        // is not spoken. ShareAcknowledge is flexible from v0. Kafka
        // 4.0 validVersions is "0"; Kafka 4.1 validVersions is "1"
        // (v0 removed). This crate speaks 0–1. Same fields. v2+
        // is not spoken.
        CONSUMER_GROUP_DESCRIBE
        | CONSUMER_GROUP_HEARTBEAT
        | SHARE_GROUP_DESCRIBE
        | SHARE_GROUP_HEARTBEAT
        | SHARE_FETCH
        | SHARE_ACKNOWLEDGE
        | DESCRIBE_SHARE_GROUP_OFFSETS
        | ALTER_SHARE_GROUP_OFFSETS
        | DELETE_SHARE_GROUP_OFFSETS
        | DESCRIBE_TOPIC_PARTITIONS
        | LIST_CONFIG_RESOURCES
        | GET_TELEMETRY_SUBSCRIPTIONS
        | PUSH_TELEMETRY
        | ASSIGN_REPLICAS_TO_DIRS => 2,
        // AlterReplicaLogDirs is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks 1–2. v0 was removed in Kafka 4.0.
        ALTER_REPLICA_LOG_DIRS if api_version >= 2 => 2,
        // DescribeLogDirs is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks 1–4. v5 is a named STATUS hole.
        DESCRIBE_LOG_DIRS if api_version >= 2 => 2,
        // CreateDelegationToken is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks 1–3. v0 was removed in Kafka 4.0.
        CREATE_DELEGATION_TOKEN if api_version >= 2 => 2,
        // RenewDelegationToken is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks 1–2. v0 was removed in Kafka 4.0.
        RENEW_DELEGATION_TOKEN if api_version >= 2 => 2,
        // ExpireDelegationToken is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks 1–2. v0 was removed in Kafka 4.0.
        EXPIRE_DELEGATION_TOKEN if api_version >= 2 => 2,
        // DescribeDelegationToken is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks 1–3. v0 was removed in Kafka 4.0.
        DESCRIBE_DELEGATION_TOKEN if api_version >= 2 => 2,
        // SaslAuthenticate is classic through v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+"). Kafka 4.0 validVersions
        // is 0-2. This crate speaks 0–2. v0 omits SessionLifetimeMs.
        // SaslHandshake is classic at v0–v1 (flexibleVersions: "none")
        // and uses the default header. v3+ is not spoken.
        SASL_AUTHENTICATE if api_version >= 2 => 2,
        _ => 1,
    }
}

/// Response header version: `0` classic, `1` flexible. ApiVersions is always `0`.
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        // KIP-482: ApiVersions response header is never flexible so the
        // correlation id can be read before the body version is known.
        API_VERSIONS => 0,
        PRODUCE if api_version >= 9 => 1,
        FETCH if api_version >= 12 => 1,
        INIT_PRODUCER_ID if api_version >= 2 => 1,
        FIND_COORDINATOR if api_version >= 3 => 1,
        METADATA if api_version >= 9 => 1,
        DESCRIBE_CLUSTER
        | ALTER_PARTITION_REASSIGNMENTS
        | LIST_PARTITION_REASSIGNMENTS
        | UPDATE_FEATURES
        | ALTER_USER_SCRAM_CREDENTIALS
        | DESCRIBE_USER_SCRAM_CREDENTIALS
        | ALLOCATE_PRODUCER_IDS
        | DESCRIBE_TRANSACTIONS
        | LIST_TRANSACTIONS
        | UNREGISTER_BROKER
        | DESCRIBE_PRODUCERS => 1,
        DESCRIBE_CLIENT_QUOTAS | ALTER_CLIENT_QUOTAS if api_version >= 1 => 1,
        DESCRIBE_GROUPS if api_version >= 5 => 1,
        LIST_GROUPS if api_version >= 3 => 1,
        DELETE_GROUPS if api_version >= 2 => 1,
        ADD_PARTITIONS_TO_TXN if api_version >= 3 => 1,
        ADD_OFFSETS_TO_TXN if api_version >= 3 => 1,
        END_TXN if api_version >= 3 => 1,
        TXN_OFFSET_COMMIT if api_version >= 3 => 1,
        WRITE_TXN_MARKERS if api_version >= 1 => 1,
        LEAVE_GROUP if api_version >= 4 => 1,
        LIST_OFFSETS if api_version >= 6 => 1,
        OFFSET_COMMIT if api_version >= 8 => 1,
        OFFSET_FETCH if api_version >= 6 => 1,
        OFFSET_FOR_LEADER_EPOCH if api_version >= 4 => 1,
        HEARTBEAT if api_version >= 4 => 1,
        SYNC_GROUP if api_version >= 4 => 1,
        JOIN_GROUP if api_version >= 6 => 1,
        CREATE_TOPICS if api_version >= 5 => 1,
        DELETE_TOPICS if api_version >= 4 => 1,
        DESCRIBE_CONFIGS if api_version >= 4 => 1,
        CREATE_PARTITIONS if api_version >= 2 => 1,
        INCREMENTAL_ALTER_CONFIGS if api_version >= 1 => 1,
        ALTER_CONFIGS if api_version >= 2 => 1,
        DELETE_RECORDS if api_version >= 2 => 1,
        CREATE_ACLS | DESCRIBE_ACLS | DELETE_ACLS if api_version >= 2 => 1,
        CONSUMER_GROUP_DESCRIBE
        | CONSUMER_GROUP_HEARTBEAT
        | SHARE_GROUP_DESCRIBE
        | SHARE_GROUP_HEARTBEAT
        | SHARE_FETCH
        | SHARE_ACKNOWLEDGE
        | DESCRIBE_SHARE_GROUP_OFFSETS
        | ALTER_SHARE_GROUP_OFFSETS
        | DELETE_SHARE_GROUP_OFFSETS
        | DESCRIBE_TOPIC_PARTITIONS
        | LIST_CONFIG_RESOURCES
        | GET_TELEMETRY_SUBSCRIPTIONS
        | PUSH_TELEMETRY
        | ASSIGN_REPLICAS_TO_DIRS => 1,
        ALTER_REPLICA_LOG_DIRS if api_version >= 2 => 1,
        DESCRIBE_LOG_DIRS if api_version >= 2 => 1,
        CREATE_DELEGATION_TOKEN if api_version >= 2 => 1,
        RENEW_DELEGATION_TOKEN if api_version >= 2 => 1,
        EXPIRE_DELEGATION_TOKEN if api_version >= 2 => 1,
        DESCRIBE_DELEGATION_TOKEN if api_version >= 2 => 1,
        SASL_AUTHENTICATE if api_version >= 2 => 1,
        _ => 0,
    }
}

/// Write a request header from fields (used by the produce hot path).
pub fn encode_request_header_fields(
    buf: &mut BytesMut,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
) -> crate::error::Result<()> {
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);
    buf::put_classic_nullable_string(buf, client_id)?;
    if request_header_version(api_key, api_version) >= 2 {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Write [`RequestHeader`].
pub fn encode_request_header(
    buf: &mut BytesMut,
    header: &RequestHeader,
) -> crate::error::Result<()> {
    encode_request_header_fields(
        buf,
        header.api_key,
        header.api_version,
        header.correlation_id,
        header.client_id.as_deref(),
    )
}

/// Read [`RequestHeader`], including tagged fields when the header is flexible.
pub fn decode_request_header<B: Buf>(buf: &mut B) -> Result<RequestHeader> {
    let api_key = buf::get_i16(buf)?;
    let api_version = buf::get_i16(buf)?;
    let correlation_id = buf::get_i32(buf)?;
    let client_id = buf::get_classic_nullable_string(buf)?;
    if request_header_version(api_key, api_version) >= 2 {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

/// Write a response header for `api_key` / `api_version`.
pub fn encode_response_header(
    buf: &mut BytesMut,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
) -> crate::error::Result<()> {
    buf.put_i32(correlation_id);
    if response_header_version(api_key, api_version) >= 1 {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Read [`ResponseHeader`] for `api_key` / `api_version`.
pub fn decode_response_header<B: Buf>(
    buf: &mut B,
    api_key: i16,
    api_version: i16,
) -> Result<ResponseHeader> {
    let correlation_id = buf::get_i32(buf)?;
    if response_header_version(api_key, api_version) >= 1 {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ResponseHeader { correlation_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_versions_response_header_never_flexible() {
        // Official Kafka 4.0 JSON: validVersions 0-4, flexibleVersions 3+.
        // Request header is 1 at v0–2 and 2 at v3–v4. Response header is
        // always 0 so the correlation id can be read first (KIP-482).
        assert_eq!(request_header_version(API_VERSIONS, 2), 1);
        assert_eq!(response_header_version(API_VERSIONS, 0), 0);
        assert_eq!(request_header_version(API_VERSIONS, 3), 2);
        assert_eq!(response_header_version(API_VERSIONS, 3), 0);
        assert_eq!(request_header_version(API_VERSIONS, 4), 2);
        assert_eq!(response_header_version(API_VERSIONS, 4), 0);
    }

    #[test]
    fn alter_partition_reassignments_v0_is_flexible() {
        assert_eq!(request_header_version(ALTER_PARTITION_REASSIGNMENTS, 0), 2);
        assert_eq!(response_header_version(ALTER_PARTITION_REASSIGNMENTS, 0), 1);
    }

    #[test]
    fn list_partition_reassignments_v0_is_flexible() {
        assert_eq!(request_header_version(LIST_PARTITION_REASSIGNMENTS, 0), 2);
        assert_eq!(response_header_version(LIST_PARTITION_REASSIGNMENTS, 0), 1);
    }

    #[test]
    fn update_features_v0_to_v2_are_flexible() {
        // Official Kafka 4.0 JSON: validVersions 0-2, flexibleVersions 0+.
        // HeaderVersion is 2 / 1 at every spoken version. This crate speaks 0–2.
        for version in 0..=2 {
            assert_eq!(request_header_version(UPDATE_FEATURES, version), 2);
            assert_eq!(response_header_version(UPDATE_FEATURES, version), 1);
        }
    }

    #[test]
    fn alter_user_scram_credentials_v0_is_flexible() {
        assert_eq!(request_header_version(ALTER_USER_SCRAM_CREDENTIALS, 0), 2);
        assert_eq!(response_header_version(ALTER_USER_SCRAM_CREDENTIALS, 0), 1);
    }

    #[test]
    fn describe_user_scram_credentials_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v0.
        assert_eq!(
            request_header_version(DESCRIBE_USER_SCRAM_CREDENTIALS, 0),
            2
        );
        assert_eq!(
            response_header_version(DESCRIBE_USER_SCRAM_CREDENTIALS, 0),
            1
        );
    }

    #[test]
    fn unregister_broker_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v0.
        assert_eq!(request_header_version(UNREGISTER_BROKER, 0), 2);
        assert_eq!(response_header_version(UNREGISTER_BROKER, 0), 1);
    }

    #[test]
    fn describe_producers_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v0.
        assert_eq!(request_header_version(DESCRIBE_PRODUCERS, 0), 2);
        assert_eq!(response_header_version(DESCRIBE_PRODUCERS, 0), 1);
    }

    #[test]
    fn write_txn_markers_v1_is_flexible_v0_is_not() {
        // Kafka 3.9 JSON: validVersions 0-1, flexibleVersions 1+.
        // Kafka 4.0 removed v0 (trunk validVersions 1-2). HeaderVersion
        // is 1 / 0 at v0 and 2 / 1 at v1. This crate speaks 0–1.
        assert_eq!(request_header_version(WRITE_TXN_MARKERS, 0), 1);
        assert_eq!(response_header_version(WRITE_TXN_MARKERS, 0), 0);
        assert_eq!(request_header_version(WRITE_TXN_MARKERS, 1), 2);
        assert_eq!(response_header_version(WRITE_TXN_MARKERS, 1), 1);
    }

    #[test]
    fn add_partitions_to_txn_v3_is_flexible_v2_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 3+.
        // HeaderVersion is 1 / 0 at v0–2 and 2 / 1 at v3+. This crate
        // speaks 0–3. v4+ (batched transactions) is not spoken.
        assert_eq!(request_header_version(ADD_PARTITIONS_TO_TXN, 0), 1);
        assert_eq!(response_header_version(ADD_PARTITIONS_TO_TXN, 0), 0);
        assert_eq!(request_header_version(ADD_PARTITIONS_TO_TXN, 2), 1);
        assert_eq!(response_header_version(ADD_PARTITIONS_TO_TXN, 2), 0);
        assert_eq!(request_header_version(ADD_PARTITIONS_TO_TXN, 3), 2);
        assert_eq!(response_header_version(ADD_PARTITIONS_TO_TXN, 3), 1);
    }

    #[test]
    fn add_offsets_to_txn_v3_is_flexible_v2_is_not() {
        // Official JSON: validVersions 0-4, flexibleVersions 3+.
        // HeaderVersion is 1 / 0 at v0–2 and 2 / 1 at v3–4. This crate
        // speaks 0–4. v5+ is not spoken.
        assert_eq!(request_header_version(ADD_OFFSETS_TO_TXN, 0), 1);
        assert_eq!(response_header_version(ADD_OFFSETS_TO_TXN, 0), 0);
        assert_eq!(request_header_version(ADD_OFFSETS_TO_TXN, 2), 1);
        assert_eq!(response_header_version(ADD_OFFSETS_TO_TXN, 2), 0);
        assert_eq!(request_header_version(ADD_OFFSETS_TO_TXN, 3), 2);
        assert_eq!(response_header_version(ADD_OFFSETS_TO_TXN, 3), 1);
        assert_eq!(request_header_version(ADD_OFFSETS_TO_TXN, 4), 2);
        assert_eq!(response_header_version(ADD_OFFSETS_TO_TXN, 4), 1);
    }

    #[test]
    fn end_txn_v3_is_flexible_v2_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 3+.
        // HeaderVersion is 1 / 0 at v0–2 and 2 / 1 at v3+. This crate
        // speaks 0–5. v5 adds ProducerId / ProducerEpoch on the response.
        assert_eq!(request_header_version(END_TXN, 0), 1);
        assert_eq!(response_header_version(END_TXN, 0), 0);
        assert_eq!(request_header_version(END_TXN, 2), 1);
        assert_eq!(response_header_version(END_TXN, 2), 0);
        assert_eq!(request_header_version(END_TXN, 3), 2);
        assert_eq!(response_header_version(END_TXN, 3), 1);
        assert_eq!(request_header_version(END_TXN, 4), 2);
        assert_eq!(response_header_version(END_TXN, 4), 1);
        assert_eq!(request_header_version(END_TXN, 5), 2);
        assert_eq!(response_header_version(END_TXN, 5), 1);
    }

    #[test]
    fn txn_offset_commit_v3_is_flexible_v2_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 3+.
        // HeaderVersion is 1 / 0 at v0–2 and 2 / 1 at v3+. This crate
        // speaks 0–5. v5 is transaction V2 (same layout as v3–v4).
        assert_eq!(request_header_version(TXN_OFFSET_COMMIT, 0), 1);
        assert_eq!(response_header_version(TXN_OFFSET_COMMIT, 0), 0);
        assert_eq!(request_header_version(TXN_OFFSET_COMMIT, 2), 1);
        assert_eq!(response_header_version(TXN_OFFSET_COMMIT, 2), 0);
        assert_eq!(request_header_version(TXN_OFFSET_COMMIT, 3), 2);
        assert_eq!(response_header_version(TXN_OFFSET_COMMIT, 3), 1);
        assert_eq!(request_header_version(TXN_OFFSET_COMMIT, 4), 2);
        assert_eq!(response_header_version(TXN_OFFSET_COMMIT, 4), 1);
        assert_eq!(request_header_version(TXN_OFFSET_COMMIT, 5), 2);
        assert_eq!(response_header_version(TXN_OFFSET_COMMIT, 5), 1);
    }

    #[test]
    fn offset_commit_v8_is_flexible_v7_is_not() {
        // Official JSON: validVersions 2-9, flexibleVersions 8+.
        // HeaderVersion is 1 / 0 at v2–v7 and 2 / 1 at v8–v9. This crate
        // speaks 2–9. v9 is KIP-848 errors (same layout as v8).
        assert_eq!(request_header_version(OFFSET_COMMIT, 2), 1);
        assert_eq!(response_header_version(OFFSET_COMMIT, 2), 0);
        assert_eq!(request_header_version(OFFSET_COMMIT, 5), 1);
        assert_eq!(response_header_version(OFFSET_COMMIT, 5), 0);
        assert_eq!(request_header_version(OFFSET_COMMIT, 7), 1);
        assert_eq!(response_header_version(OFFSET_COMMIT, 7), 0);
        assert_eq!(request_header_version(OFFSET_COMMIT, 8), 2);
        assert_eq!(response_header_version(OFFSET_COMMIT, 8), 1);
        assert_eq!(request_header_version(OFFSET_COMMIT, 9), 2);
        assert_eq!(response_header_version(OFFSET_COMMIT, 9), 1);
    }

    #[test]
    fn offset_fetch_v6_is_flexible_v5_is_not() {
        // Official JSON: validVersions 1-9, flexibleVersions 6+.
        // HeaderVersion is 1 / 0 at v1–v5 and 2 / 1 at v6–v9. This crate
        // speaks 1–9. v7–v9 keep the v6 header (RequireStable / Groups).
        assert_eq!(request_header_version(OFFSET_FETCH, 1), 1);
        assert_eq!(response_header_version(OFFSET_FETCH, 1), 0);
        assert_eq!(request_header_version(OFFSET_FETCH, 5), 1);
        assert_eq!(response_header_version(OFFSET_FETCH, 5), 0);
        assert_eq!(request_header_version(OFFSET_FETCH, 6), 2);
        assert_eq!(response_header_version(OFFSET_FETCH, 6), 1);
        assert_eq!(request_header_version(OFFSET_FETCH, 9), 2);
        assert_eq!(response_header_version(OFFSET_FETCH, 9), 1);
    }

    #[test]
    fn heartbeat_v4_is_flexible_v3_is_not() {
        // Official JSON: validVersions 0-4, flexibleVersions 4+.
        // HeaderVersion is 1 / 0 at v0–v3 and 2 / 1 at v4. This crate
        // speaks 0–4. v1 and v2 match v0. v3 GroupInstanceId.
        assert_eq!(request_header_version(HEARTBEAT, 0), 1);
        assert_eq!(response_header_version(HEARTBEAT, 0), 0);
        assert_eq!(request_header_version(HEARTBEAT, 2), 1);
        assert_eq!(response_header_version(HEARTBEAT, 2), 0);
        assert_eq!(request_header_version(HEARTBEAT, 3), 1);
        assert_eq!(response_header_version(HEARTBEAT, 3), 0);
        assert_eq!(request_header_version(HEARTBEAT, 4), 2);
        assert_eq!(response_header_version(HEARTBEAT, 4), 1);
    }

    #[test]
    fn sync_group_v4_is_flexible_v3_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 4+.
        // HeaderVersion is 1 / 0 at v0–v3 and 2 / 1 at v4–v5. This crate
        // speaks 0–5. v1 and v2 match v0. v3 GroupInstanceId. v5
        // ProtocolType / ProtocolName keep the v4 header.
        assert_eq!(request_header_version(SYNC_GROUP, 0), 1);
        assert_eq!(response_header_version(SYNC_GROUP, 0), 0);
        assert_eq!(request_header_version(SYNC_GROUP, 2), 1);
        assert_eq!(response_header_version(SYNC_GROUP, 2), 0);
        assert_eq!(request_header_version(SYNC_GROUP, 3), 1);
        assert_eq!(response_header_version(SYNC_GROUP, 3), 0);
        assert_eq!(request_header_version(SYNC_GROUP, 4), 2);
        assert_eq!(response_header_version(SYNC_GROUP, 4), 1);
        assert_eq!(request_header_version(SYNC_GROUP, 5), 2);
        assert_eq!(response_header_version(SYNC_GROUP, 5), 1);
    }

    #[test]
    fn join_group_v6_is_flexible_v5_is_not() {
        // Official JSON: validVersions 2-9, flexibleVersions 6+.
        // HeaderVersion is 1 / 0 at v2–v5 and 2 / 1 at v6–v9. This crate
        // speaks 2–9. v8 Reason and v9 SkipAssignment keep the v6 header.
        assert_eq!(request_header_version(JOIN_GROUP, 2), 1);
        assert_eq!(response_header_version(JOIN_GROUP, 2), 0);
        assert_eq!(request_header_version(JOIN_GROUP, 4), 1);
        assert_eq!(response_header_version(JOIN_GROUP, 4), 0);
        assert_eq!(request_header_version(JOIN_GROUP, 5), 1);
        assert_eq!(response_header_version(JOIN_GROUP, 5), 0);
        assert_eq!(request_header_version(JOIN_GROUP, 6), 2);
        assert_eq!(response_header_version(JOIN_GROUP, 6), 1);
        assert_eq!(request_header_version(JOIN_GROUP, 9), 2);
        assert_eq!(response_header_version(JOIN_GROUP, 9), 1);
    }

    #[test]
    fn create_topics_v5_is_flexible_v4_is_not() {
        // Official Kafka 4.0 JSON: validVersions 2-7, flexibleVersions 5+.
        // HeaderVersion is 1 / 0 at v0–4 and 2 / 1 at v5–v7. This crate
        // speaks 0–7. v5 KIP-525 configs; v7 TopicId.
        assert_eq!(request_header_version(CREATE_TOPICS, 4), 1);
        assert_eq!(response_header_version(CREATE_TOPICS, 4), 0);
        assert_eq!(request_header_version(CREATE_TOPICS, 5), 2);
        assert_eq!(response_header_version(CREATE_TOPICS, 5), 1);
        assert_eq!(request_header_version(CREATE_TOPICS, 7), 2);
        assert_eq!(response_header_version(CREATE_TOPICS, 7), 1);
    }

    #[test]
    fn delete_topics_v4_is_flexible_v3_is_not() {
        // Official Kafka 4.0 JSON: validVersions 1-6, flexibleVersions 4+.
        // HeaderVersion is 1 / 0 at v0–3 and 2 / 1 at v4–v6. This crate
        // speaks 0–6. v5 ErrorMessage; v6 Topics + TopicId.
        assert_eq!(request_header_version(DELETE_TOPICS, 3), 1);
        assert_eq!(response_header_version(DELETE_TOPICS, 3), 0);
        assert_eq!(request_header_version(DELETE_TOPICS, 4), 2);
        assert_eq!(response_header_version(DELETE_TOPICS, 4), 1);
        assert_eq!(request_header_version(DELETE_TOPICS, 6), 2);
        assert_eq!(response_header_version(DELETE_TOPICS, 6), 1);
    }

    #[test]
    fn describe_configs_v4_is_flexible_v3_is_not() {
        // Official Kafka 4.0 JSON: validVersions 1-4, flexibleVersions 4+.
        // HeaderVersion is 1 / 0 at v0–3 and 2 / 1 at v4. This crate
        // speaks 0–4. v3 IncludeDocumentation / ConfigType.
        assert_eq!(request_header_version(DESCRIBE_CONFIGS, 3), 1);
        assert_eq!(response_header_version(DESCRIBE_CONFIGS, 3), 0);
        assert_eq!(request_header_version(DESCRIBE_CONFIGS, 4), 2);
        assert_eq!(response_header_version(DESCRIBE_CONFIGS, 4), 1);
    }

    #[test]
    fn create_partitions_v2_is_flexible_v1_is_not() {
        // Official Kafka 4.0 JSON: validVersions 0-3, flexibleVersions 2+.
        // HeaderVersion is 1 / 0 at v0–1 and 2 / 1 at v2–v3. This crate
        // speaks 0–3. v3 is the same layout as v2 (KIP-599).
        assert_eq!(request_header_version(CREATE_PARTITIONS, 1), 1);
        assert_eq!(response_header_version(CREATE_PARTITIONS, 1), 0);
        assert_eq!(request_header_version(CREATE_PARTITIONS, 2), 2);
        assert_eq!(response_header_version(CREATE_PARTITIONS, 2), 1);
        assert_eq!(request_header_version(CREATE_PARTITIONS, 3), 2);
        assert_eq!(response_header_version(CREATE_PARTITIONS, 3), 1);
    }

    #[test]
    fn incremental_alter_configs_v1_is_flexible_v0_is_not() {
        // Official Kafka 4.0 JSON: validVersions 0-1, flexibleVersions 1+.
        // HeaderVersion is 1 / 0 at v0 and 2 / 1 at v1. This crate speaks 0–1.
        assert_eq!(request_header_version(INCREMENTAL_ALTER_CONFIGS, 0), 1);
        assert_eq!(response_header_version(INCREMENTAL_ALTER_CONFIGS, 0), 0);
        assert_eq!(request_header_version(INCREMENTAL_ALTER_CONFIGS, 1), 2);
        assert_eq!(response_header_version(INCREMENTAL_ALTER_CONFIGS, 1), 1);
    }

    #[test]
    fn alter_configs_v2_is_flexible_v1_is_not() {
        // Official Kafka 4.0 JSON: validVersions 0-2, flexibleVersions 2+.
        // HeaderVersion is 1 / 0 at v0–1 and 2 / 1 at v2. This crate speaks 0–2.
        assert_eq!(request_header_version(ALTER_CONFIGS, 0), 1);
        assert_eq!(response_header_version(ALTER_CONFIGS, 0), 0);
        assert_eq!(request_header_version(ALTER_CONFIGS, 1), 1);
        assert_eq!(response_header_version(ALTER_CONFIGS, 1), 0);
        assert_eq!(request_header_version(ALTER_CONFIGS, 2), 2);
        assert_eq!(response_header_version(ALTER_CONFIGS, 2), 1);
    }

    #[test]
    fn describe_cluster_v0_to_v2_are_flexible() {
        // Official Kafka 4.0 JSON: validVersions 0-2, flexibleVersions 0+.
        // HeaderVersion is 2 / 1 at every spoken version. This crate speaks 0–2.
        for version in 0..=2 {
            assert_eq!(request_header_version(DESCRIBE_CLUSTER, version), 2);
            assert_eq!(response_header_version(DESCRIBE_CLUSTER, version), 1);
        }
    }

    #[test]
    fn delete_records_v2_is_flexible_v1_is_not() {
        // Official Kafka 4.0 JSON: validVersions 0-2, flexibleVersions 2+.
        // HeaderVersion is 1 / 0 at v0–1 and 2 / 1 at v2. This crate speaks 0–2.
        assert_eq!(request_header_version(DELETE_RECORDS, 0), 1);
        assert_eq!(response_header_version(DELETE_RECORDS, 0), 0);
        assert_eq!(request_header_version(DELETE_RECORDS, 1), 1);
        assert_eq!(response_header_version(DELETE_RECORDS, 1), 0);
        assert_eq!(request_header_version(DELETE_RECORDS, 2), 2);
        assert_eq!(response_header_version(DELETE_RECORDS, 2), 1);
    }

    #[test]
    fn acl_apis_v2_are_flexible_v1_is_not() {
        // Official Kafka 4.0 JSON: validVersions 1-3, flexibleVersions 2+.
        // HeaderVersion is 1 / 0 at v0–1 and 2 / 1 at v2–v3. This crate
        // speaks 0–3. v1 ResourcePatternType. v3 same layout as v2.
        for key in [CREATE_ACLS, DESCRIBE_ACLS, DELETE_ACLS] {
            assert_eq!(request_header_version(key, 0), 1);
            assert_eq!(response_header_version(key, 0), 0);
            assert_eq!(request_header_version(key, 1), 1);
            assert_eq!(response_header_version(key, 1), 0);
            assert_eq!(request_header_version(key, 2), 2);
            assert_eq!(response_header_version(key, 2), 1);
            assert_eq!(request_header_version(key, 3), 2);
            assert_eq!(response_header_version(key, 3), 1);
        }
    }

    #[test]
    fn leave_group_v4_is_flexible_v3_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 4+.
        // HeaderVersion is 1 / 0 at v0–3 and 2 / 1 at v4–5.
        // This crate speaks 0–5. Classic ConsumerGroup leave negotiates
        // the same range (prefer v5). Admin remove-members stays v3–v5.
        assert_eq!(request_header_version(LEAVE_GROUP, 0), 1);
        assert_eq!(response_header_version(LEAVE_GROUP, 0), 0);
        assert_eq!(request_header_version(LEAVE_GROUP, 3), 1);
        assert_eq!(response_header_version(LEAVE_GROUP, 3), 0);
        assert_eq!(request_header_version(LEAVE_GROUP, 4), 2);
        assert_eq!(response_header_version(LEAVE_GROUP, 4), 1);
        assert_eq!(request_header_version(LEAVE_GROUP, 5), 2);
        assert_eq!(response_header_version(LEAVE_GROUP, 5), 1);
    }

    #[test]
    fn produce_v9_is_flexible_v8_is_not() {
        // Official JSON: validVersions 3-12, flexibleVersions 9+.
        // Kafka 4.0 removed v0–v2. HeaderVersion is 1 / 0 at v3–8 and
        // 2 / 1 at v9+. This crate speaks 3–12. v13+ (topic IDs) is
        // not spoken.
        assert_eq!(request_header_version(PRODUCE, 3), 1);
        assert_eq!(response_header_version(PRODUCE, 3), 0);
        assert_eq!(request_header_version(PRODUCE, 8), 1);
        assert_eq!(response_header_version(PRODUCE, 8), 0);
        assert_eq!(request_header_version(PRODUCE, 9), 2);
        assert_eq!(response_header_version(PRODUCE, 9), 1);
        assert_eq!(request_header_version(PRODUCE, 12), 2);
        assert_eq!(response_header_version(PRODUCE, 12), 1);
    }

    #[test]
    fn fetch_v12_is_flexible_v11_is_not() {
        // Official JSON: validVersions 4-17, flexibleVersions 12+.
        // Kafka 4.0 removed v0–v3. HeaderVersion is 1 / 0 at v4–11 and
        // 2 / 1 at v12+. This crate speaks 4–17. v18+ (HighWatermark) is
        // not spoken.
        assert_eq!(request_header_version(FETCH, 4), 1);
        assert_eq!(response_header_version(FETCH, 4), 0);
        assert_eq!(request_header_version(FETCH, 11), 1);
        assert_eq!(response_header_version(FETCH, 11), 0);
        assert_eq!(request_header_version(FETCH, 12), 2);
        assert_eq!(response_header_version(FETCH, 12), 1);
        assert_eq!(request_header_version(FETCH, 14), 2);
        assert_eq!(response_header_version(FETCH, 14), 1);
        assert_eq!(request_header_version(FETCH, 15), 2);
        assert_eq!(response_header_version(FETCH, 15), 1);
        assert_eq!(request_header_version(FETCH, 16), 2);
        assert_eq!(response_header_version(FETCH, 16), 1);
        assert_eq!(request_header_version(FETCH, 17), 2);
        assert_eq!(response_header_version(FETCH, 17), 1);
    }

    #[test]
    fn init_producer_id_v2_is_flexible_v1_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 2+.
        // HeaderVersion is 1 / 0 at v0–1 and 2 / 1 at v2+. This crate
        // speaks 0–5. v6+ (KIP-939 2PC) is not spoken.
        assert_eq!(request_header_version(INIT_PRODUCER_ID, 0), 1);
        assert_eq!(response_header_version(INIT_PRODUCER_ID, 0), 0);
        assert_eq!(request_header_version(INIT_PRODUCER_ID, 1), 1);
        assert_eq!(response_header_version(INIT_PRODUCER_ID, 1), 0);
        assert_eq!(request_header_version(INIT_PRODUCER_ID, 2), 2);
        assert_eq!(response_header_version(INIT_PRODUCER_ID, 2), 1);
        assert_eq!(request_header_version(INIT_PRODUCER_ID, 5), 2);
        assert_eq!(response_header_version(INIT_PRODUCER_ID, 5), 1);
    }

    #[test]
    fn find_coordinator_v3_is_flexible_v2_is_not() {
        // Official JSON: validVersions 0-6, flexibleVersions 3+.
        // HeaderVersion is 1 / 0 at v1–2 and 2 / 1 at v3+. This crate
        // speaks 1–6. v4–v6 keep the v3 header (CoordinatorKeys body).
        assert_eq!(request_header_version(FIND_COORDINATOR, 1), 1);
        assert_eq!(response_header_version(FIND_COORDINATOR, 1), 0);
        assert_eq!(request_header_version(FIND_COORDINATOR, 2), 1);
        assert_eq!(response_header_version(FIND_COORDINATOR, 2), 0);
        assert_eq!(request_header_version(FIND_COORDINATOR, 3), 2);
        assert_eq!(response_header_version(FIND_COORDINATOR, 3), 1);
        assert_eq!(request_header_version(FIND_COORDINATOR, 4), 2);
        assert_eq!(response_header_version(FIND_COORDINATOR, 4), 1);
        assert_eq!(request_header_version(FIND_COORDINATOR, 6), 2);
        assert_eq!(response_header_version(FIND_COORDINATOR, 6), 1);
    }

    #[test]
    fn list_offsets_v6_is_flexible_v5_is_not() {
        // Official JSON: validVersions 1-10, flexibleVersions 6+.
        // Kafka 4.0 removed v0. HeaderVersion is 1 / 0 at v1–5 and
        // 2 / 1 at v6+. This crate speaks 1–10.
        assert_eq!(request_header_version(LIST_OFFSETS, 1), 1);
        assert_eq!(response_header_version(LIST_OFFSETS, 1), 0);
        assert_eq!(request_header_version(LIST_OFFSETS, 5), 1);
        assert_eq!(response_header_version(LIST_OFFSETS, 5), 0);
        assert_eq!(request_header_version(LIST_OFFSETS, 6), 2);
        assert_eq!(response_header_version(LIST_OFFSETS, 6), 1);
        assert_eq!(request_header_version(LIST_OFFSETS, 10), 2);
        assert_eq!(response_header_version(LIST_OFFSETS, 10), 1);
    }

    #[test]
    fn offset_for_leader_epoch_v4_is_flexible_v3_is_not() {
        // Official Kafka 4.0 JSON: validVersions 2-4, flexibleVersions 4+.
        // HeaderVersion is 1 / 0 at v0–3 and 2 / 1 at v4. This crate
        // speaks 0–4. v3 ReplicaId. v2 CurrentLeaderEpoch.
        assert_eq!(request_header_version(OFFSET_FOR_LEADER_EPOCH, 2), 1);
        assert_eq!(response_header_version(OFFSET_FOR_LEADER_EPOCH, 2), 0);
        assert_eq!(request_header_version(OFFSET_FOR_LEADER_EPOCH, 3), 1);
        assert_eq!(response_header_version(OFFSET_FOR_LEADER_EPOCH, 3), 0);
        assert_eq!(request_header_version(OFFSET_FOR_LEADER_EPOCH, 4), 2);
        assert_eq!(response_header_version(OFFSET_FOR_LEADER_EPOCH, 4), 1);
    }

    #[test]
    fn delete_groups_v2_is_flexible_v1_is_not() {
        // Official JSON: validVersions 0-2, flexibleVersions 2+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v2; 1 / 0
        // at v0–1. This crate speaks 0–2.
        assert_eq!(request_header_version(DELETE_GROUPS, 0), 1);
        assert_eq!(response_header_version(DELETE_GROUPS, 0), 0);
        assert_eq!(request_header_version(DELETE_GROUPS, 1), 1);
        assert_eq!(response_header_version(DELETE_GROUPS, 1), 0);
        assert_eq!(request_header_version(DELETE_GROUPS, 2), 2);
        assert_eq!(response_header_version(DELETE_GROUPS, 2), 1);
    }

    #[test]
    fn list_groups_v5_is_flexible_v2_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 3+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v3–5; 1 / 0
        // at v0–2. This crate speaks 0–5.
        assert_eq!(request_header_version(LIST_GROUPS, 0), 1);
        assert_eq!(response_header_version(LIST_GROUPS, 0), 0);
        assert_eq!(request_header_version(LIST_GROUPS, 2), 1);
        assert_eq!(response_header_version(LIST_GROUPS, 2), 0);
        assert_eq!(request_header_version(LIST_GROUPS, 3), 2);
        assert_eq!(response_header_version(LIST_GROUPS, 3), 1);
        assert_eq!(request_header_version(LIST_GROUPS, 5), 2);
        assert_eq!(response_header_version(LIST_GROUPS, 5), 1);
    }

    #[test]
    fn describe_groups_v6_is_flexible_v4_is_not() {
        // Official JSON: validVersions 0-6, flexibleVersions 5+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v5–6; 1 / 0
        // at v0–4. This crate speaks 0–6.
        assert_eq!(request_header_version(DESCRIBE_GROUPS, 0), 1);
        assert_eq!(response_header_version(DESCRIBE_GROUPS, 0), 0);
        assert_eq!(request_header_version(DESCRIBE_GROUPS, 4), 1);
        assert_eq!(response_header_version(DESCRIBE_GROUPS, 4), 0);
        assert_eq!(request_header_version(DESCRIBE_GROUPS, 5), 2);
        assert_eq!(response_header_version(DESCRIBE_GROUPS, 5), 1);
        assert_eq!(request_header_version(DESCRIBE_GROUPS, 6), 2);
        assert_eq!(response_header_version(DESCRIBE_GROUPS, 6), 1);
    }

    #[test]
    fn consumer_group_describe_v1_is_flexible() {
        // Official JSON: validVersions 0-1, flexibleVersions 0+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at every version.
        // This crate speaks 0–1.
        assert_eq!(request_header_version(CONSUMER_GROUP_DESCRIBE, 0), 2);
        assert_eq!(response_header_version(CONSUMER_GROUP_DESCRIBE, 0), 1);
        assert_eq!(request_header_version(CONSUMER_GROUP_DESCRIBE, 1), 2);
        assert_eq!(response_header_version(CONSUMER_GROUP_DESCRIBE, 1), 1);
    }

    #[test]
    fn consumer_group_heartbeat_v0_to_v1_are_flexible() {
        // Official Kafka 4.0 JSON: validVersions 0-1, flexibleVersions 0+.
        // HeaderVersion is 2 / 1 at every spoken version. This crate
        // speaks 0–1. v1 SubscribedTopicRegex (KIP-848) / KIP-1082.
        for version in 0..=1 {
            assert_eq!(request_header_version(CONSUMER_GROUP_HEARTBEAT, version), 2);
            assert_eq!(
                response_header_version(CONSUMER_GROUP_HEARTBEAT, version),
                1
            );
        }
    }

    #[test]
    fn share_group_heartbeat_v0_and_v1_are_flexible() {
        // Official Kafka 4.0 JSON: validVersions "0", flexibleVersions "0+".
        // Official Kafka 4.1 JSON: validVersions "1" (v0 removed).
        // HeaderVersion is 2 / 1 at every spoken version. This crate
        // speaks 0–1.
        for version in 0..=1 {
            assert_eq!(request_header_version(SHARE_GROUP_HEARTBEAT, version), 2);
            assert_eq!(response_header_version(SHARE_GROUP_HEARTBEAT, version), 1);
        }
    }

    #[test]
    fn share_group_describe_v0_and_v1_are_flexible() {
        // Official Kafka 4.0 JSON: validVersions "0", flexibleVersions "0+".
        // Official Kafka 4.1 JSON: validVersions "1" (v0 removed).
        // HeaderVersion is 2 / 1 at every spoken version. This crate
        // speaks 0–1.
        for version in 0..=1 {
            assert_eq!(request_header_version(SHARE_GROUP_DESCRIBE, version), 2);
            assert_eq!(response_header_version(SHARE_GROUP_DESCRIBE, version), 1);
        }
    }

    #[test]
    fn share_fetch_v0_and_v1_are_flexible() {
        // Official Kafka 4.0 JSON: validVersions "0", flexibleVersions "0+".
        // Official Kafka 4.1 JSON: validVersions "1" (v0 removed).
        // HeaderVersion is 2 / 1 at every spoken version. This crate
        // speaks 0–1. Request/response fields differ by version.
        for version in 0..=1 {
            assert_eq!(request_header_version(SHARE_FETCH, version), 2);
            assert_eq!(response_header_version(SHARE_FETCH, version), 1);
        }
    }

    #[test]
    fn share_acknowledge_v0_and_v1_are_flexible() {
        // Official Kafka 4.0 JSON: validVersions "0", flexibleVersions "0+".
        // Official Kafka 4.1 JSON: validVersions "1" (v0 removed).
        // HeaderVersion is 2 / 1 at every spoken version. This crate
        // speaks 0–1. Same fields.
        for version in 0..=1 {
            assert_eq!(request_header_version(SHARE_ACKNOWLEDGE, version), 2);
            assert_eq!(response_header_version(SHARE_ACKNOWLEDGE, version), 1);
        }
    }

    #[test]
    fn describe_share_group_offsets_v0_is_flexible() {
        // Official JSON: validVersions 0-1, flexibleVersions 0+.
        // kafka-protocol 0.18.0 VERSIONS min=max=0; HeaderVersion is 2 / 1
        // at v0. This crate speaks v0 (VERSIONS.max). Official trunk v1
        // (Lag, KIP-1226) is not in 0.18.0 and is not spoken here.
        assert_eq!(request_header_version(DESCRIBE_SHARE_GROUP_OFFSETS, 0), 2);
        assert_eq!(response_header_version(DESCRIBE_SHARE_GROUP_OFFSETS, 0), 1);
    }

    #[test]
    fn alter_share_group_offsets_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 VERSIONS min=max=0; HeaderVersion is 2 / 1
        // at v0. This crate speaks v0 (VERSIONS.max).
        assert_eq!(request_header_version(ALTER_SHARE_GROUP_OFFSETS, 0), 2);
        assert_eq!(response_header_version(ALTER_SHARE_GROUP_OFFSETS, 0), 1);
    }

    #[test]
    fn delete_share_group_offsets_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 VERSIONS min=max=0; HeaderVersion is 2 / 1
        // at v0. This crate speaks v0 (VERSIONS.max).
        assert_eq!(request_header_version(DELETE_SHARE_GROUP_OFFSETS, 0), 2);
        assert_eq!(response_header_version(DELETE_SHARE_GROUP_OFFSETS, 0), 1);
    }

    #[test]
    fn describe_topic_partitions_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 VERSIONS min=max=0; HeaderVersion is 2 / 1
        // at v0. This crate speaks v0 (VERSIONS.max).
        assert_eq!(request_header_version(DESCRIBE_TOPIC_PARTITIONS, 0), 2);
        assert_eq!(response_header_version(DESCRIBE_TOPIC_PARTITIONS, 0), 1);
    }

    #[test]
    fn list_config_resources_v1_is_flexible() {
        // Official JSON: validVersions 0-1, flexibleVersions 0+.
        // kafka-protocol 0.18.0 VERSIONS min=0 max=1; HeaderVersion is
        // 2 / 1 at v0 and v1. This crate speaks 0–1.
        assert_eq!(request_header_version(LIST_CONFIG_RESOURCES, 0), 2);
        assert_eq!(response_header_version(LIST_CONFIG_RESOURCES, 0), 1);
        assert_eq!(request_header_version(LIST_CONFIG_RESOURCES, 1), 2);
        assert_eq!(response_header_version(LIST_CONFIG_RESOURCES, 1), 1);
    }

    #[test]
    fn get_telemetry_subscriptions_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 VERSIONS min=max=0; HeaderVersion is
        // 2 / 1 at v0. This crate speaks v0 (VERSIONS.max).
        assert_eq!(request_header_version(GET_TELEMETRY_SUBSCRIPTIONS, 0), 2);
        assert_eq!(response_header_version(GET_TELEMETRY_SUBSCRIPTIONS, 0), 1);
    }

    #[test]
    fn push_telemetry_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 VERSIONS min=max=0; HeaderVersion is
        // 2 / 1 at v0. This crate speaks v0 (VERSIONS.max).
        assert_eq!(request_header_version(PUSH_TELEMETRY, 0), 2);
        assert_eq!(response_header_version(PUSH_TELEMETRY, 0), 1);
    }

    #[test]
    fn assign_replicas_to_dirs_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 VERSIONS min=max=0; HeaderVersion is
        // 2 / 1 at v0. This crate speaks v0 (VERSIONS.max).
        assert_eq!(request_header_version(ASSIGN_REPLICAS_TO_DIRS, 0), 2);
        assert_eq!(response_header_version(ASSIGN_REPLICAS_TO_DIRS, 0), 1);
    }

    #[test]
    fn alter_replica_log_dirs_v2_is_flexible_v1_is_not() {
        // Official JSON: validVersions 1-2, flexibleVersions 2+.
        // kafka-protocol 0.18.0 VERSIONS min=1 max=2; HeaderVersion is
        // 2 / 1 at v2; 1 / 0 at v1. This crate speaks 1–2.
        assert_eq!(request_header_version(ALTER_REPLICA_LOG_DIRS, 1), 1);
        assert_eq!(response_header_version(ALTER_REPLICA_LOG_DIRS, 1), 0);
        assert_eq!(request_header_version(ALTER_REPLICA_LOG_DIRS, 2), 2);
        assert_eq!(response_header_version(ALTER_REPLICA_LOG_DIRS, 2), 1);
    }

    #[test]
    fn describe_log_dirs_v4_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=4; HeaderVersion is 2 / 1 at v2–4; 1 / 0
        // at v1. This crate speaks 1–4. v5 is a named STATUS hole.
        assert_eq!(request_header_version(DESCRIBE_LOG_DIRS, 1), 1);
        assert_eq!(response_header_version(DESCRIBE_LOG_DIRS, 1), 0);
        assert_eq!(request_header_version(DESCRIBE_LOG_DIRS, 2), 2);
        assert_eq!(response_header_version(DESCRIBE_LOG_DIRS, 2), 1);
        assert_eq!(request_header_version(DESCRIBE_LOG_DIRS, 3), 2);
        assert_eq!(response_header_version(DESCRIBE_LOG_DIRS, 3), 1);
        assert_eq!(request_header_version(DESCRIBE_LOG_DIRS, 4), 2);
        assert_eq!(response_header_version(DESCRIBE_LOG_DIRS, 4), 1);
    }

    #[test]
    fn create_delegation_token_v3_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=3; HeaderVersion is 2 / 1 at v2–3; 1 / 0
        // at v1. This crate speaks 1–3.
        assert_eq!(request_header_version(CREATE_DELEGATION_TOKEN, 1), 1);
        assert_eq!(response_header_version(CREATE_DELEGATION_TOKEN, 1), 0);
        assert_eq!(request_header_version(CREATE_DELEGATION_TOKEN, 2), 2);
        assert_eq!(response_header_version(CREATE_DELEGATION_TOKEN, 2), 1);
        assert_eq!(request_header_version(CREATE_DELEGATION_TOKEN, 3), 2);
        assert_eq!(response_header_version(CREATE_DELEGATION_TOKEN, 3), 1);
    }

    #[test]
    fn renew_delegation_token_v2_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=2; HeaderVersion is 2 / 1 at v2; 1 / 0
        // at v1. This crate speaks 1–2.
        assert_eq!(request_header_version(RENEW_DELEGATION_TOKEN, 1), 1);
        assert_eq!(response_header_version(RENEW_DELEGATION_TOKEN, 1), 0);
        assert_eq!(request_header_version(RENEW_DELEGATION_TOKEN, 2), 2);
        assert_eq!(response_header_version(RENEW_DELEGATION_TOKEN, 2), 1);
    }

    #[test]
    fn expire_delegation_token_v2_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=2; HeaderVersion is 2 / 1 at v2; 1 / 0
        // at v1. This crate speaks 1–2.
        assert_eq!(request_header_version(EXPIRE_DELEGATION_TOKEN, 1), 1);
        assert_eq!(response_header_version(EXPIRE_DELEGATION_TOKEN, 1), 0);
        assert_eq!(request_header_version(EXPIRE_DELEGATION_TOKEN, 2), 2);
        assert_eq!(response_header_version(EXPIRE_DELEGATION_TOKEN, 2), 1);
    }

    #[test]
    fn describe_delegation_token_v3_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=3; HeaderVersion is 2 / 1 at v2–3; 1 / 0
        // at v1. This crate speaks 1–3.
        assert_eq!(request_header_version(DESCRIBE_DELEGATION_TOKEN, 1), 1);
        assert_eq!(response_header_version(DESCRIBE_DELEGATION_TOKEN, 1), 0);
        assert_eq!(request_header_version(DESCRIBE_DELEGATION_TOKEN, 2), 2);
        assert_eq!(response_header_version(DESCRIBE_DELEGATION_TOKEN, 2), 1);
        assert_eq!(request_header_version(DESCRIBE_DELEGATION_TOKEN, 3), 2);
        assert_eq!(response_header_version(DESCRIBE_DELEGATION_TOKEN, 3), 1);
    }

    #[test]
    fn describe_client_quotas_v1_is_flexible_v0_is_not() {
        // Official JSON: validVersions 0-1, flexibleVersions 1+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v1; 1 / 0 at v0.
        // This crate speaks 0–1.
        assert_eq!(request_header_version(DESCRIBE_CLIENT_QUOTAS, 0), 1);
        assert_eq!(response_header_version(DESCRIBE_CLIENT_QUOTAS, 0), 0);
        assert_eq!(request_header_version(DESCRIBE_CLIENT_QUOTAS, 1), 2);
        assert_eq!(response_header_version(DESCRIBE_CLIENT_QUOTAS, 1), 1);
    }

    #[test]
    fn alter_client_quotas_v1_is_flexible_v0_is_not() {
        // Official JSON: validVersions 0-1, flexibleVersions 1+.
        // v0 stays classic (header 1/0). This crate speaks 0–1.
        assert_eq!(request_header_version(ALTER_CLIENT_QUOTAS, 0), 1);
        assert_eq!(response_header_version(ALTER_CLIENT_QUOTAS, 0), 0);
        assert_eq!(request_header_version(ALTER_CLIENT_QUOTAS, 1), 2);
        assert_eq!(response_header_version(ALTER_CLIENT_QUOTAS, 1), 1);
    }

    #[test]
    fn allocate_producer_ids_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v0.
        assert_eq!(request_header_version(ALLOCATE_PRODUCER_IDS, 0), 2);
        assert_eq!(response_header_version(ALLOCATE_PRODUCER_IDS, 0), 1);
    }

    #[test]
    fn describe_transactions_v0_is_flexible() {
        // Official JSON: validVersions 0, flexibleVersions 0+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v0.
        assert_eq!(request_header_version(DESCRIBE_TRANSACTIONS, 0), 2);
        assert_eq!(response_header_version(DESCRIBE_TRANSACTIONS, 0), 1);
    }

    #[test]
    fn list_transactions_is_flexible() {
        // Official Kafka 4.0 JSON: validVersions 0-1, flexibleVersions 0+.
        // kafka-protocol 0.18.0 advertised 0-2 (TransactionalIdPattern);
        // this crate speaks 0–1. HeaderVersion is 2 / 1 at every spoken
        // version.
        assert_eq!(request_header_version(LIST_TRANSACTIONS, 0), 2);
        assert_eq!(response_header_version(LIST_TRANSACTIONS, 0), 1);
        assert_eq!(request_header_version(LIST_TRANSACTIONS, 1), 2);
        assert_eq!(response_header_version(LIST_TRANSACTIONS, 1), 1);
    }

    #[test]
    fn sasl_handshake_v0_and_v1_are_classic() {
        // Official Kafka 4.0 JSON: validVersions 0-1, flexibleVersions none.
        // HeaderVersion is 1 / 0 at v0 and v1. This crate speaks 0–1.
        // v1 enables SaslAuthenticate. v2+ is not spoken (KAFKA-9577).
        let key = crate::protocol::api_keys::SASL_HANDSHAKE;
        assert_eq!(request_header_version(key, 0), 1);
        assert_eq!(response_header_version(key, 0), 0);
        assert_eq!(request_header_version(key, 1), 1);
        assert_eq!(response_header_version(key, 1), 0);
    }

    #[test]
    fn sasl_authenticate_v2_is_flexible_v1_is_not() {
        // Official Kafka 4.0 JSON: validVersions 0-2, flexibleVersions 2+.
        // HeaderVersion is 1 / 0 at v0–v1 and 2 / 1 at v2.
        // This crate speaks 0–2. v3+ is not spoken.
        assert_eq!(request_header_version(SASL_AUTHENTICATE, 0), 1);
        assert_eq!(response_header_version(SASL_AUTHENTICATE, 0), 0);
        assert_eq!(request_header_version(SASL_AUTHENTICATE, 1), 1);
        assert_eq!(response_header_version(SASL_AUTHENTICATE, 1), 0);
        assert_eq!(request_header_version(SASL_AUTHENTICATE, 2), 2);
        assert_eq!(response_header_version(SASL_AUTHENTICATE, 2), 1);
    }

    #[test]
    fn request_header_client_id_is_classic_even_when_flexible() {
        let header = RequestHeader {
            api_key: API_VERSIONS,
            api_version: 3,
            correlation_id: 7,
            client_id: Some("pl".into()),
        };
        let mut buf = BytesMut::new();
        encode_request_header(&mut buf, &header).unwrap();
        // api_key, version, correlation, classic string "pl", tagged count 0
        let mut cur = &buf[..];
        let decoded = decode_request_header(&mut cur).unwrap();
        assert_eq!(decoded.correlation_id, 7);
        assert_eq!(decoded.client_id.as_deref(), Some("pl"));
        assert_eq!(cur.remaining(), 0);
    }

    #[test]
    fn request_header_display_matches_java() {
        // Java RequestHeader.toString:
        // RequestHeader(apiKey=PRODUCE, apiVersion=9, clientId=client,
        // correlationId=1, headerVersion=2)
        let header = RequestHeader {
            api_key: PRODUCE,
            api_version: 9,
            correlation_id: 1,
            client_id: Some("client".into()),
        };
        assert_eq!(header.api_key(), PRODUCE);
        assert_eq!(header.api_version(), 9);
        assert_eq!(header.correlation_id(), 1);
        assert_eq!(header.client_id(), Some("client"));
        assert_eq!(header.header_version(), 2);
        assert_eq!(
            header.to_string(),
            "RequestHeader(apiKey=PRODUCE, apiVersion=9, clientId=client, correlationId=1, headerVersion=2)"
        );

        let classic = RequestHeader {
            api_key: PRODUCE,
            api_version: 8,
            correlation_id: 1,
            client_id: Some("client".into()),
        };
        assert_eq!(classic.header_version(), 1);
        assert_eq!(
            classic.to_string(),
            "RequestHeader(apiKey=PRODUCE, apiVersion=8, clientId=client, correlationId=1, headerVersion=1)"
        );

        let null_client = RequestHeader {
            api_key: PRODUCE,
            api_version: 9,
            correlation_id: 1,
            client_id: None,
        };
        assert_eq!(null_client.client_id(), None);
        assert_eq!(
            null_client.to_string(),
            "RequestHeader(apiKey=PRODUCE, apiVersion=9, clientId=null, correlationId=1, headerVersion=2)"
        );

        let empty_client = RequestHeader {
            api_key: PRODUCE,
            api_version: 9,
            correlation_id: 1,
            client_id: Some(String::new()),
        };
        assert_eq!(empty_client.client_id(), Some(""));
        assert_eq!(
            empty_client.to_string(),
            "RequestHeader(apiKey=PRODUCE, apiVersion=9, clientId=, correlationId=1, headerVersion=2)"
        );

        // Java RequestHeaderTest.testRequestHeaderV1.
        let find = RequestHeader {
            api_key: FIND_COORDINATOR,
            api_version: 1,
            correlation_id: 10,
            client_id: Some(String::new()),
        };
        assert_eq!(find.header_version(), 1);
        assert_eq!(
            find.to_string(),
            "RequestHeader(apiKey=FIND_COORDINATOR, apiVersion=1, clientId=, correlationId=10, headerVersion=1)"
        );

        // Java 4.0 ApiKeys name for api 74 is LIST_CLIENT_METRICS_RESOURCES.
        let list_cfg = RequestHeader {
            api_key: LIST_CONFIG_RESOURCES,
            api_version: 0,
            correlation_id: 0,
            client_id: Some("c".into()),
        };
        assert_eq!(list_cfg.header_version(), 2);
        assert_eq!(
            list_cfg.to_string(),
            "RequestHeader(apiKey=LIST_CLIENT_METRICS_RESOURCES, apiVersion=0, clientId=c, correlationId=0, headerVersion=2)"
        );

        assert_eq!(crate::protocol::api_keys::name(43), Some("ELECT_LEADERS"));
        assert_eq!(crate::protocol::api_keys::name(999), None);
        assert!(crate::protocol::api_keys::has_id(43));
        assert!(!crate::protocol::api_keys::has_id(999));
        let unknown = RequestHeader {
            api_key: 999,
            api_version: 0,
            correlation_id: 0,
            client_id: None,
        };
        assert_eq!(unknown.header_version(), 1);
        assert_eq!(
            unknown.to_string(),
            "RequestHeader(apiKey=999, apiVersion=0, clientId=null, correlationId=0, headerVersion=1)"
        );
    }

    #[test]
    fn request_header_size_matches_java() {
        // Java RequestHeaderTest.testRequestHeaderV1: FIND_COORDINATOR v1,
        // empty clientId, serialized size 10 (header version 1).
        let v1 = RequestHeader {
            api_key: FIND_COORDINATOR,
            api_version: 1,
            correlation_id: 10,
            client_id: Some(String::new()),
        };
        assert_eq!(v1.header_version(), 1);
        assert_eq!(v1.size(), 10);
        let mut buf = BytesMut::new();
        encode_request_header(&mut buf, &v1).unwrap();
        assert_eq!(buf.len(), 10);

        // Java RequestHeaderTest.testRequestHeaderV2: CREATE_DELEGATION_TOKEN
        // v2, empty clientId, serialized size 11 (header version 2).
        let v2 = RequestHeader {
            api_key: CREATE_DELEGATION_TOKEN,
            api_version: 2,
            correlation_id: 10,
            client_id: Some(String::new()),
        };
        assert_eq!(v2.header_version(), 2);
        assert_eq!(v2.size(), 11);
        buf.clear();
        encode_request_header(&mut buf, &v2).unwrap();
        assert_eq!(buf.len(), 11);

        // Java RequestHeaderTest.verifySizeMethodsReturnSameValue clientId.
        let named = RequestHeader {
            api_key: FIND_COORDINATOR,
            api_version: 10,
            correlation_id: 123,
            client_id: Some("hakuna-matata".into()),
        };
        assert_eq!(named.header_version(), 2);
        assert_eq!(named.size(), 24);
        buf.clear();
        encode_request_header(&mut buf, &named).unwrap();
        assert_eq!(buf.len(), 24);

        let null_client = RequestHeader {
            api_key: PRODUCE,
            api_version: 9,
            correlation_id: 1,
            client_id: None,
        };
        assert_eq!(null_client.size(), 11);
        buf.clear();
        encode_request_header(&mut buf, &null_client).unwrap();
        assert_eq!(buf.len(), 11);

        let resp = v1.to_response_header();
        assert_eq!(resp.correlation_id(), 10);
        // ApiVersions response header is never flexible (size 4).
        assert_eq!(response_header_version(API_VERSIONS, 3), 0);
        assert_eq!(response_header_size(0), 4);
        buf.clear();
        encode_response_header(&mut buf, API_VERSIONS, 3, 10).unwrap();
        assert_eq!(buf.len(), 4);
        // Produce v9 response header is flexible (size 5).
        assert_eq!(response_header_version(PRODUCE, 9), 1);
        assert_eq!(response_header_size(1), 5);
        buf.clear();
        encode_response_header(&mut buf, PRODUCE, 9, 10).unwrap();
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn check_correlation_matches_java() {
        let req = RequestHeader {
            api_key: PRODUCE,
            api_version: 9,
            correlation_id: 1,
            client_id: Some("client".into()),
        };
        let ok = ResponseHeader { correlation_id: 1 };
        req.check_correlation(&ok).unwrap();
        let err = req
            .check_correlation(&ResponseHeader { correlation_id: 2 })
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "Correlation id for response (2) does not match request (1), request header: RequestHeader(apiKey=PRODUCE, apiVersion=9, clientId=client, correlationId=1, headerVersion=2)"
            ),
            "{err}"
        );
    }
}
