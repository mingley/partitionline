//! A Kafka client written in Rust. No C, no librdkafka.
//!
//! Send and fetch records, join a consumer group, gzip, snappy, lz4, SASL PLAIN,
//! SASL SCRAM-SHA-256, SASL SCRAM-SHA-512, SASL OAUTHBEARER, TLS (rustls),
//! idempotent produce, and admin (CreateTopics / DeleteTopics / DescribeConfigs).
//! See the crate README and `docs/gaps.md` for what is still missing.

#![forbid(unsafe_code)]

pub mod admin;
pub mod consumer;
pub mod error;
pub mod group;
pub mod net;
pub mod partitioner;
pub mod producer;
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

pub const CLIENT_NAME: &str = "partitionline";
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
