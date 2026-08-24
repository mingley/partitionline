//! A Kafka client written in Rust. No C, no librdkafka.
//!
//! Send and fetch records, join a consumer group, gzip, snappy, lz4, SASL PLAIN,
//! and idempotent produce.
//! See the crate README and `docs/gaps.md` for what is still missing.

#![forbid(unsafe_code)]

pub mod consumer;
pub mod error;
pub mod group;
pub mod net;
pub mod partitioner;
pub mod producer;
pub mod protocol;

pub use consumer::{Consumer, ConsumerConfig, FetchedRecord};
pub use error::{Error, Result};
pub use group::ConsumerGroup;
pub use producer::{ProduceRecord, Producer, ProducerConfig, RecordMetadata};
pub use protocol::records::{Compression, Header, Record, RecordBatch};

pub const CLIENT_NAME: &str = "partitionline";
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
