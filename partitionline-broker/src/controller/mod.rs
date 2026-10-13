//! Controller-owned session state.
//!
//! Broker registration and epoch fencing live in [`registration`]. Partition
//! placement is a later card.

pub mod registration;
