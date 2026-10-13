//! Transaction coordinator state and marker visibility.
//!
//! [`coordinator`] stores the transaction lifecycle. [`markers`] records commit
//! and abort markers and the read-committed view.

pub mod coordinator;
pub mod markers;
