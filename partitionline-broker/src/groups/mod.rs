//! Consumer group coordination.
//!
//! Classic Join/Sync membership lives in [`classic`]. Session, leave, and
//! revocation live in [`lifecycle`]. Modern server-side assignment lives in
//! [`consumer`].

pub mod classic;
pub mod consumer;
pub mod lifecycle;
pub mod offsets;
