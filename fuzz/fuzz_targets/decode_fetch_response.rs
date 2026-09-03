#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline::protocol::fetch::decode_fetch_response;

fuzz_target!(|data: &[u8]| {
    // Spoken Fetch response versions in this crate (see docs/design.md).
    for version in [4_i16, 7, 11, 12, 13, 16, 17] {
        let mut cur = data;
        let _ = decode_fetch_response(&mut cur, version);
    }
});
