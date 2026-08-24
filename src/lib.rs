//! Pure-Rust Kafka client. No C.

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
