#![no_main]

//! Adversarial decode for ConsumerGroupHeartbeat (KIP-848) request/response.

use libfuzzer_sys::fuzz_target;
use partitionline::protocol::cgheartbeat::{
    decode_consumer_group_heartbeat_request, decode_consumer_group_heartbeat_response,
};

fuzz_target!(|data: &[u8]| {
    // Spoken ConsumerGroupHeartbeat versions in this crate (flexible v0–v1).
    for version in [0_i16, 1] {
        let mut cur = data;
        let _ = decode_consumer_group_heartbeat_request(&mut cur, version);
        let mut cur = data;
        let _ = decode_consumer_group_heartbeat_response(&mut cur, version);
    }
});
