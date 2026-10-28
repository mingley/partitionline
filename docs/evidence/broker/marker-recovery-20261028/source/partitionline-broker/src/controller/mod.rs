//! Controller-owned session and placement state.
//!
//! Broker registration and epoch fencing live in [`registration`]. Partition
//! placement, leader epochs, and ISR changes live in [`partitions`].

pub mod partitions;
pub mod registration;
