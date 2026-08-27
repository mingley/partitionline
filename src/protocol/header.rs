#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::api_keys::{
    ALLOCATE_PRODUCER_IDS, ALTER_CLIENT_QUOTAS, ALTER_PARTITION_REASSIGNMENTS,
    ALTER_USER_SCRAM_CREDENTIALS, API_VERSIONS, CONSUMER_GROUP_HEARTBEAT, DESCRIBE_CLUSTER,
    DESCRIBE_TRANSACTIONS, LIST_PARTITION_REASSIGNMENTS, METADATA, PRODUCE, SHARE_ACKNOWLEDGE,
    SHARE_FETCH, SHARE_GROUP_HEARTBEAT, UPDATE_FEATURES,
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
        | ALLOCATE_PRODUCER_IDS
        | DESCRIBE_TRANSACTIONS => 2,
        // AlterClientQuotas is classic at v0; flexible from v1
        // (Apache JSON flexibleVersions: "1+", kafka-protocol 0.18.0).
        ALTER_CLIENT_QUOTAS if api_version >= 1 => 2,
        CONSUMER_GROUP_HEARTBEAT | SHARE_GROUP_HEARTBEAT | SHARE_FETCH | SHARE_ACKNOWLEDGE => 2,
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
        | ALLOCATE_PRODUCER_IDS
        | DESCRIBE_TRANSACTIONS => 1,
        ALTER_CLIENT_QUOTAS if api_version >= 1 => 1,
        CONSUMER_GROUP_HEARTBEAT | SHARE_GROUP_HEARTBEAT | SHARE_FETCH | SHARE_ACKNOWLEDGE => 1,
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
