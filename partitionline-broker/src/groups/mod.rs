//! Consumer group coordination.
//!
//! Classic Join/Sync membership lives in [`classic`]. Session, leave, and
//! revocation live in [`lifecycle`]. Modern server-side assignment lives in
//! [`consumer`]. Share groups live in [`share`]. Streams groups live in
//! [`streams`].

pub mod classic;
pub mod consumer;
pub mod lifecycle;
pub mod offsets;
pub mod share;
pub mod streams;
