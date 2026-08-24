//! Pure-Rust Kafka client. No C. Produce path first.

pub mod error;
pub mod net;
pub mod partitioner;
pub mod producer;
pub mod protocol;

pub use error::{Error, Result};
pub use producer::{ProduceRecord, Producer, ProducerConfig, RecordMetadata};
pub use protocol::records::{Header, Record, RecordBatch};

pub const CLIENT_NAME: &str = "partitionline";
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
