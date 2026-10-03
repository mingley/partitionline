//! Experimental Rust Kafka broker under development.
//!
//! The implementation and qualification contracts live in the repository's
//! `docs/plan/broker-implementation.md` and KL11 task cards. This independent
//! crate does not change the published client's dependencies or defaults.
//! Implemented APIs will be advertised only after their handlers and versioned
//! semantics are verified. Production and comparative performance qualification
//! require the separate, evidence-backed completion gates.

pub mod catalog;
pub mod fetch;
pub mod journal;
pub mod metadata;
pub mod partition;
pub mod produce;
pub mod protocol;
pub mod raft;
pub mod records;
pub mod retention;
pub mod segments;
pub mod transport;

#[cfg(feature = "codecs")]
pub mod codecs;

#[cfg(any(feature = "tls", feature = "sasl"))]
pub mod security;
