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

pub mod broker;
pub mod client;
pub mod compression;
pub mod consumer;
pub mod error;
pub mod frame;
pub mod partitioner;
pub mod producer;

pub use client::{topic_matches, Client, TopicMeta};
pub use compression::Compression;
pub use consumer::{
    consumer_fetch_request, consumer_fetch_request_parts, decode_records, ConsumerFetch, Fetched,
    Fetcher, CONSUMER_REPLICA_ID, FETCH_SESSION_EPOCH_INVALID, FETCH_SESSION_NONE,
    ISOLATION_READ_UNCOMMITTED,
};
pub use error::{Error, Result};
pub use partitioner::{hash_partition, murmur2};
pub use producer::{
    encode_record_batch, Acks, BatchIdentity, Delivery, ProduceResult, Producer, ProducerBuilder,
    RecordTo,
};

/// Crate version sent as `client_software_version` on ApiVersions v3+.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Client name sent as `client_software_name` on ApiVersions v3+.
pub const CLIENT_NAME: &str = "partitionline";
