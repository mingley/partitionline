//! Null-broker runtime measurement harness for producer cells (KL09-09).
//!
//! Standalone crate: drives `partitionline::Producer` against an
//! `nb-serve` subprocess and emits one fail-closed result artifact per
//! (cell, repetition) run.

pub mod artifact;
pub mod cells;
pub mod drive;
pub mod host;
pub mod measure;
