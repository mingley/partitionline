//! Bounded metadata quorum primitives; wire RPCs and replication are separate.
//!
//! Election success is not a lease or permission to commit metadata. Fixed
//! membership and trusted peer messages are required; no Kafka API is advertised.

pub mod election;
