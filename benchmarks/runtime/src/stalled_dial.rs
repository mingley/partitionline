//! Safe loopback fixture for a pending TCP connect, shared with core tests.

#[path = "../../../tests/common/stalled_dial.rs"]
mod fixture;

pub use fixture::StalledDial;
