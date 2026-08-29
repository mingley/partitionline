//! Request and response headers (classic header v0/v1 and flexible v1/v2).

use bytes::{Buf, BufMut, BytesMut};

use super::api_keys::{
    ADD_OFFSETS_TO_TXN, ADD_PARTITIONS_TO_TXN, ALLOCATE_PRODUCER_IDS, ALTER_CLIENT_QUOTAS,
    ALTER_PARTITION_REASSIGNMENTS, ALTER_REPLICA_LOG_DIRS, ALTER_SHARE_GROUP_OFFSETS,
    ALTER_USER_SCRAM_CREDENTIALS, API_VERSIONS, ASSIGN_REPLICAS_TO_DIRS, CONSUMER_GROUP_DESCRIBE,
    CONSUMER_GROUP_HEARTBEAT, CREATE_DELEGATION_TOKEN, DELETE_GROUPS, DELETE_SHARE_GROUP_OFFSETS,
    DESCRIBE_CLIENT_QUOTAS, DESCRIBE_CLUSTER, DESCRIBE_DELEGATION_TOKEN, DESCRIBE_GROUPS,
    DESCRIBE_LOG_DIRS, DESCRIBE_PRODUCERS, DESCRIBE_SHARE_GROUP_OFFSETS, DESCRIBE_TOPIC_PARTITIONS,
    DESCRIBE_TRANSACTIONS, DESCRIBE_USER_SCRAM_CREDENTIALS, END_TXN, EXPIRE_DELEGATION_TOKEN,
    FETCH, FIND_COORDINATOR, GET_TELEMETRY_SUBSCRIPTIONS, HEARTBEAT, INIT_PRODUCER_ID, LEAVE_GROUP,
    LIST_CONFIG_RESOURCES, LIST_GROUPS, LIST_OFFSETS, LIST_PARTITION_REASSIGNMENTS,
    LIST_TRANSACTIONS, METADATA, OFFSET_COMMIT, OFFSET_FETCH, PRODUCE, PUSH_TELEMETRY,
    RENEW_DELEGATION_TOKEN, SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_DESCRIBE,
    SHARE_GROUP_HEARTBEAT, SYNC_GROUP, TXN_OFFSET_COMMIT, UNREGISTER_BROKER, UPDATE_FEATURES,
    WRITE_TXN_MARKERS,
};
use super::buf;
use crate::error::Result;

/// Kafka request header (`api_key` through `client_id`, plus tagged fields when flexible).
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

/// Kafka response header (correlation id, plus tagged fields when flexible).
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    /// Correlation id from the request.
    pub correlation_id: i32,
}

/// Request header version: `1` classic, `2` flexible (KIP-482 tagged fields).
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
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
        | DESCRIBE_PRODUCERS => 2,
        // DescribeClientQuotas / AlterClientQuotas are classic at v0;
        // flexible from v1 (Apache JSON flexibleVersions: "1+",
        // kafka-protocol 0.18.0).
        DESCRIBE_CLIENT_QUOTAS | ALTER_CLIENT_QUOTAS if api_version >= 1 => 2,
        // DescribeGroups is classic through v4; flexible from v5
        // (Apache JSON flexibleVersions: "5+", kafka-protocol 0.18.0).
        DESCRIBE_GROUPS if api_version >= 5 => 2,
        // ListGroups is classic through v2; flexible from v3
        // (Apache JSON flexibleVersions: "3+", kafka-protocol 0.18.0).
        LIST_GROUPS if api_version >= 3 => 2,
        // DeleteGroups is classic through v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
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
        // is 2-9. This crate speaks 7–9. v9 is KIP-848 errors (same layout).
        OFFSET_COMMIT if api_version >= 8 => 2,
        // OffsetFetch is classic through v5; flexible from v6
        // (Apache JSON flexibleVersions: "6+"). Kafka 4.0 validVersions
        // is 1-9. This crate speaks 5–9. v7 RequireStable; v8 Groups;
        // v9 MemberId / MemberEpoch (same header as v6).
        OFFSET_FETCH if api_version >= 6 => 2,
        // Heartbeat is classic through v3; flexible from v4
        // (Apache JSON flexibleVersions: "4+"). Kafka 4.0 validVersions
        // is 0-4. This crate speaks 3–4. v0–v2 (no instance id) are
        // not spoken.
        HEARTBEAT if api_version >= 4 => 2,
        // SyncGroup is classic through v3; flexible from v4
        // (Apache JSON flexibleVersions: "4+"). Kafka 4.0 validVersions
        // is 0-5. This crate speaks 3–5. v5 ProtocolType / ProtocolName
        // (KIP-559) keep the v4 header. v0–v2 are not spoken.
        SYNC_GROUP if api_version >= 4 => 2,
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
        ALTER_REPLICA_LOG_DIRS if api_version >= 2 => 2,
        // DescribeLogDirs is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks v4 only (VERSIONS.max).
        DESCRIBE_LOG_DIRS if api_version >= 2 => 2,
        // CreateDelegationToken is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks v3 only (VERSIONS.max).
        CREATE_DELEGATION_TOKEN if api_version >= 2 => 2,
        // RenewDelegationToken is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks v2 only (VERSIONS.max).
        RENEW_DELEGATION_TOKEN if api_version >= 2 => 2,
        // ExpireDelegationToken is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks v2 only (VERSIONS.max).
        EXPIRE_DELEGATION_TOKEN if api_version >= 2 => 2,
        // DescribeDelegationToken is classic at v1; flexible from v2
        // (Apache JSON flexibleVersions: "2+", kafka-protocol 0.18.0).
        // This crate speaks v3 only (VERSIONS.max).
        DESCRIBE_DELEGATION_TOKEN if api_version >= 2 => 2,
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
        HEARTBEAT if api_version >= 4 => 1,
        SYNC_GROUP if api_version >= 4 => 1,
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
        assert_eq!(response_header_version(API_VERSIONS, 0), 0);
        assert_eq!(response_header_version(API_VERSIONS, 3), 0);
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
    fn update_features_v0_is_flexible() {
        assert_eq!(request_header_version(UPDATE_FEATURES, 0), 2);
        assert_eq!(response_header_version(UPDATE_FEATURES, 0), 1);
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
        // HeaderVersion is 1 / 0 at v7 and 2 / 1 at v8–v9. This crate
        // speaks 7–9. v9 is KIP-848 errors (same layout as v8).
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
        // HeaderVersion is 1 / 0 at v5 and 2 / 1 at v6–v9. This crate
        // speaks 5–9. v7–v9 keep the v6 header (RequireStable / Groups).
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
        // HeaderVersion is 1 / 0 at v3 and 2 / 1 at v4. This crate
        // speaks 3–4. v0–v2 (no instance id) are not spoken.
        assert_eq!(request_header_version(HEARTBEAT, 3), 1);
        assert_eq!(response_header_version(HEARTBEAT, 3), 0);
        assert_eq!(request_header_version(HEARTBEAT, 4), 2);
        assert_eq!(response_header_version(HEARTBEAT, 4), 1);
    }

    #[test]
    fn sync_group_v4_is_flexible_v3_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 4+.
        // HeaderVersion is 1 / 0 at v3 and 2 / 1 at v4–v5. This crate
        // speaks 3–5. v5 ProtocolType / ProtocolName keep the v4 header.
        assert_eq!(request_header_version(SYNC_GROUP, 3), 1);
        assert_eq!(response_header_version(SYNC_GROUP, 3), 0);
        assert_eq!(request_header_version(SYNC_GROUP, 4), 2);
        assert_eq!(response_header_version(SYNC_GROUP, 4), 1);
        assert_eq!(request_header_version(SYNC_GROUP, 5), 2);
        assert_eq!(response_header_version(SYNC_GROUP, 5), 1);
    }

    #[test]
    fn leave_group_v4_is_flexible_v3_is_not() {
        // Official JSON: validVersions 0-5, flexibleVersions 4+.
        // HeaderVersion is 1 / 0 at v0–3 and 2 / 1 at v4–5.
        // This crate speaks 0–5 (classic group leave stays v0).
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
    fn delete_groups_v2_is_flexible_v1_is_not() {
        // Official JSON: validVersions 0-2, flexibleVersions 2+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v2; 1 / 0
        // at v0–1. This crate speaks v2 (VERSIONS.max).
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
        // at v0–2. This crate speaks v5 (VERSIONS.max).
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
        // at v0–4. This crate speaks v6 (VERSIONS.max).
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
        // This crate speaks v1.
        assert_eq!(request_header_version(CONSUMER_GROUP_DESCRIBE, 0), 2);
        assert_eq!(response_header_version(CONSUMER_GROUP_DESCRIBE, 0), 1);
        assert_eq!(request_header_version(CONSUMER_GROUP_DESCRIBE, 1), 2);
        assert_eq!(response_header_version(CONSUMER_GROUP_DESCRIBE, 1), 1);
    }

    #[test]
    fn share_group_describe_v1_is_flexible() {
        // Official JSON: validVersions 1, flexibleVersions 0+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at v1.
        // This crate speaks v1 (VERSIONS.max).
        assert_eq!(request_header_version(SHARE_GROUP_DESCRIBE, 1), 2);
        assert_eq!(response_header_version(SHARE_GROUP_DESCRIBE, 1), 1);
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
        // 2 / 1 at v0 and v1. This crate speaks v1 (VERSIONS.max).
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
        // 2 / 1 at v2; 1 / 0 at v1. This crate speaks v2 (VERSIONS.max).
        assert_eq!(request_header_version(ALTER_REPLICA_LOG_DIRS, 1), 1);
        assert_eq!(response_header_version(ALTER_REPLICA_LOG_DIRS, 1), 0);
        assert_eq!(request_header_version(ALTER_REPLICA_LOG_DIRS, 2), 2);
        assert_eq!(response_header_version(ALTER_REPLICA_LOG_DIRS, 2), 1);
    }

    #[test]
    fn describe_log_dirs_v4_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=4; HeaderVersion is 2 / 1 at v2–4; 1 / 0
        // at v1. This crate speaks v4 (VERSIONS.max).
        assert_eq!(request_header_version(DESCRIBE_LOG_DIRS, 1), 1);
        assert_eq!(response_header_version(DESCRIBE_LOG_DIRS, 1), 0);
        assert_eq!(request_header_version(DESCRIBE_LOG_DIRS, 4), 2);
        assert_eq!(response_header_version(DESCRIBE_LOG_DIRS, 4), 1);
    }

    #[test]
    fn create_delegation_token_v3_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=3; HeaderVersion is 2 / 1 at v2–3; 1 / 0
        // at v1. This crate speaks v3 (VERSIONS.max).
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
        // at v1. This crate speaks v2 (VERSIONS.max).
        assert_eq!(request_header_version(RENEW_DELEGATION_TOKEN, 1), 1);
        assert_eq!(response_header_version(RENEW_DELEGATION_TOKEN, 1), 0);
        assert_eq!(request_header_version(RENEW_DELEGATION_TOKEN, 2), 2);
        assert_eq!(response_header_version(RENEW_DELEGATION_TOKEN, 2), 1);
    }

    #[test]
    fn expire_delegation_token_v2_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=2; HeaderVersion is 2 / 1 at v2; 1 / 0
        // at v1. This crate speaks v2 (VERSIONS.max).
        assert_eq!(request_header_version(EXPIRE_DELEGATION_TOKEN, 1), 1);
        assert_eq!(response_header_version(EXPIRE_DELEGATION_TOKEN, 1), 0);
        assert_eq!(request_header_version(EXPIRE_DELEGATION_TOKEN, 2), 2);
        assert_eq!(response_header_version(EXPIRE_DELEGATION_TOKEN, 2), 1);
    }

    #[test]
    fn describe_delegation_token_v3_is_flexible() {
        // Official JSON: flexibleVersions 2+. kafka-protocol 0.18.0
        // VERSIONS min=1 max=3; HeaderVersion is 2 / 1 at v2–3; 1 / 0
        // at v1. This crate speaks v3 (VERSIONS.max).
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
        // This crate speaks v1.
        assert_eq!(request_header_version(DESCRIBE_CLIENT_QUOTAS, 0), 1);
        assert_eq!(response_header_version(DESCRIBE_CLIENT_QUOTAS, 0), 0);
        assert_eq!(request_header_version(DESCRIBE_CLIENT_QUOTAS, 1), 2);
        assert_eq!(response_header_version(DESCRIBE_CLIENT_QUOTAS, 1), 1);
    }

    #[test]
    fn alter_client_quotas_v1_is_flexible_v0_is_not() {
        // Official JSON: validVersions 0-1, flexibleVersions 1+.
        // v0 stays classic (header 1/0). This crate speaks v1.
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
    fn list_transactions_v0_is_flexible() {
        // Official JSON: validVersions 0-2, flexibleVersions 0+.
        // kafka-protocol 0.18.0 HeaderVersion is 2 / 1 at every version.
        // This crate targets v0 (KIP-664).
        assert_eq!(request_header_version(LIST_TRANSACTIONS, 0), 2);
        assert_eq!(response_header_version(LIST_TRANSACTIONS, 0), 1);
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
}
