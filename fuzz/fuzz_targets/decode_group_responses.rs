#![no_main]

//! Adversarial decode for classic group coordinator responses (WP-2.1 Group).
//! Join / Sync / Heartbeat / OffsetCommit — high blast radius for membership.

use libfuzzer_sys::fuzz_target;
use partitionline::protocol::group::{
    decode_heartbeat_response, decode_join_group_response, decode_offset_commit_response,
    decode_sync_group_response,
};

fuzz_target!(|data: &[u8]| {
    // Spoken JoinGroup response versions in this crate (v2–v9).
    for version in [2_i16, 3, 4, 5, 6, 7, 8, 9] {
        let mut cur = data;
        let _ = decode_join_group_response(&mut cur, version);
    }
    for version in [0_i16, 1, 2, 3, 4, 5] {
        let mut cur = data;
        let _ = decode_sync_group_response(&mut cur, version);
    }
    for version in [0_i16, 1, 2, 3, 4] {
        let mut cur = data;
        let _ = decode_heartbeat_response(&mut cur, version);
    }
    for version in [2_i16, 3, 4, 5, 6, 7, 8, 9] {
        let mut cur = data;
        let _ = decode_offset_commit_response(&mut cur, version);
    }
});
