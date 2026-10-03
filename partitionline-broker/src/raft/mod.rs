//! Bounded metadata quorum election and a separate fixed-profile wire adapter.
//!
//! Election success is not a lease or permission to commit metadata. Fixed
//! membership and trusted peer messages are required. The wire adapter advertises
//! only its tested v0 controller profile; replication and commit authority are
//! separate contracts.

pub mod election;
pub mod protocol;
pub mod replication;
