//! Partitionline: a pure-Rust Kafka client.
//!
//! Wire types are **not** defined here. They come from
//! [`kafka_protocol`] 0.18 (`messages::*`, `ApiKey`, `Encodable` / `Decodable`,
//! headers, tagged fields, flexible/compact). See `PROTOCOL.md`.

#![deny(missing_docs)]

pub use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, ListOffsetsRequest,
    ListOffsetsResponse, MetadataRequest, MetadataResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse,
    RequestHeader, ResponseHeader, SyncGroupRequest, SyncGroupResponse, TopicName,
};
pub use kafka_protocol::protocol::{
    decode_request_header_from_buffer, encode_request_header_into_buffer, Decodable, Encodable,
    HeaderVersion, Message, StrBytes,
};
pub use kafka_protocol::records::{
    Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, RecordSet, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, NO_TIMESTAMP,
};

/// Crate version sent as `client_software_version` on ApiVersions v3+.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Client name sent as `client_software_name` on ApiVersions v3+.
pub const CLIENT_NAME: &str = "partitionline";
