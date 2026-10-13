//! Consumer group coordination.
//!
//! Classic Join/Sync membership lives in [`classic`]. Heartbeats and offset
//! commits are later cards.

pub mod classic;
