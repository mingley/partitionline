//! Null-broker runtime measurement harness for producer cells (KL09-09)
//! and consumer, fault and multi-node cells (KL09-10).
//!
//! Standalone crate: drives `partitionline` clients against an
//! `nb-serve` subprocess and emits one fail-closed result artifact per
//! (cell, repetition) run.

pub mod artifact;
pub mod cells;
pub mod drive;
pub mod fcells;
pub mod fdrive;
pub mod host;
pub mod measure;
pub mod stalled_dial;
