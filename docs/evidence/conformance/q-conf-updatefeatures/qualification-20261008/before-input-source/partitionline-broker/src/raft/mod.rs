//! Bounded metadata quorum election, replication and trusted peer transport.
//!
//! Election success is not a lease or permission to commit metadata. Durable
//! configuration and distinct voter authority govern replication. The runtime
//! uses private framing and configured numeric peer routes; it requires a trusted
//! network. The separate wire adapter advertises only its tested v0 controller
//! profile. Private peer transport does not advertise native Kafka KRaft APIs.

pub mod election;
pub mod membership;
mod peer_codec;
pub mod protocol;
pub mod replication;
pub mod runtime;
pub mod snapshot;
