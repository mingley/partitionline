//! A Kafka client written in Rust. No C, no librdkafka.
//!
//! Send and fetch records, join a consumer group, gzip, snappy, lz4, SASL PLAIN,
//! SASL SCRAM-SHA-256, SASL SCRAM-SHA-512, SASL OAUTHBEARER, TLS (rustls),
//! idempotent produce, and admin (CreateTopics / DeleteTopics / DescribeConfigs).
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

pub use admin::{
    Admin, AdminConfig, ConfigEntry, ConfigResource, NewTopic, CONFIG_RESOURCE_BROKER,
    CONFIG_RESOURCE_TOPIC,
};
pub use consumer::{Consumer, ConsumerConfig, FetchedRecord};
pub use error::{Error, Result};
pub use group::ConsumerGroup;
pub use net::TlsConfig;
pub use producer::{ProduceRecord, Producer, ProducerConfig, RecordMetadata};
pub use protocol::admin::{DescribeConfigsResult, TopicResult};
pub use protocol::records::{Compression, Header, Record, RecordBatch};

/// Software name sent in ApiVersions v3+.
pub const CLIENT_NAME: &str = "partitionline";
/// Crate version sent in ApiVersions v3+.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
