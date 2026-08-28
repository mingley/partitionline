#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::api_keys::{
    ALLOCATE_PRODUCER_IDS, ALTER_CLIENT_QUOTAS, ALTER_PARTITION_REASSIGNMENTS,
    ALTER_SHARE_GROUP_OFFSETS, ALTER_USER_SCRAM_CREDENTIALS, API_VERSIONS, CONSUMER_GROUP_DESCRIBE,
    CONSUMER_GROUP_HEARTBEAT, DELETE_GROUPS, DELETE_SHARE_GROUP_OFFSETS, DESCRIBE_CLIENT_QUOTAS,
    DESCRIBE_CLUSTER, DESCRIBE_GROUPS, DESCRIBE_PRODUCERS, DESCRIBE_SHARE_GROUP_OFFSETS,
    DESCRIBE_TOPIC_PARTITIONS, DESCRIBE_TRANSACTIONS, DESCRIBE_USER_SCRAM_CREDENTIALS,
    GET_TELEMETRY_SUBSCRIPTIONS, LIST_CONFIG_RESOURCES, LIST_GROUPS, LIST_PARTITION_REASSIGNMENTS,
    LIST_TRANSACTIONS, METADATA, PRODUCE, PUSH_TELEMETRY, SHARE_ACKNOWLEDGE, SHARE_FETCH,
    SHARE_GROUP_DESCRIBE, SHARE_GROUP_HEARTBEAT, UNREGISTER_BROKER, UPDATE_FEATURES,
};
use super::buf;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        API_VERSIONS if api_version >= 3 => 2,
        PRODUCE if api_version >= 9 => 2,
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
        | PUSH_TELEMETRY => 2,
        _ => 1,
    }
}

pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        // KIP-482: ApiVersions response header is never flexible so the
        // correlation id can be read before the body version is known.
        API_VERSIONS => 0,
        PRODUCE if api_version >= 9 => 1,
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
        | PUSH_TELEMETRY => 1,
        _ => 0,
    }
}

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
