#![allow(clippy::panic)]
//! Runs the cooperative and static group histories stored with the broker
//! conformance notes.
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../tests/conformance/broker/groups/histories.rs"
));
