#![no_main]

//! Adversarial decode for ShareFetch responses (KIP-932).

use libfuzzer_sys::fuzz_target;
use partitionline::protocol::share::decode_share_fetch_response;

fuzz_target!(|data: &[u8]| {
    for version in [0_i16, 1] {
        let mut cur = data;
        let _ = decode_share_fetch_response(&mut cur, version);
    }
});
