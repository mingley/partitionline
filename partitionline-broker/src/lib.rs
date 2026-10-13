//! Experimental Rust Kafka broker under development.
//!
//! The implementation and qualification contracts live in the repository's
//! `docs/plan/broker-implementation.md` and KL11 task cards. This independent
//! crate does not change the published client's dependencies or defaults.
//! Implemented APIs will be advertised only after their handlers and versioned
//! semantics are verified. Production and comparative performance qualification
//! require the separate, evidence-backed completion gates.

pub mod catalog;
pub mod compaction;
pub mod configs;
pub mod controller;
pub mod fetch;
pub mod groups;
pub mod high_watermark;
pub mod idempotence;
pub mod journal;
pub mod metadata;
pub mod metadata_quorum;
pub mod metrics;
pub mod partition;
pub mod produce;
pub mod protocol;
pub mod quotas;
pub mod raft;
pub mod records;
pub mod recovery;
pub mod replication;
pub mod retention;
pub mod segments;
pub mod share_delivery;
pub mod share_state;
pub mod tiered;
pub mod transactions;
pub mod transport;

#[cfg(feature = "codecs")]
pub mod codecs;

pub mod security;
