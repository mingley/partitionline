//! Transaction coordinator state, marker visibility, and transactional offsets.
//!
//! [`coordinator`] stores the transaction lifecycle. [`markers`] records commit
//! and abort markers and the read-committed view. [`offsets`] hides group
//! offsets until the same producer epoch commits.

pub mod coordinator;
pub mod markers;
pub mod offsets;
