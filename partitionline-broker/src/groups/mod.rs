//! Consumer group coordination.
//!
//! Classic Join/Sync membership lives in [`classic`]. Session, leave, and
//! revocation live in [`lifecycle`].

pub mod classic;
pub mod lifecycle;
