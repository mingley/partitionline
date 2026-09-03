#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline::protocol::api::decode_metadata_response;

fuzz_target!(|data: &[u8]| {
    for version in [1_i16, 8, 9, 10, 12, 13] {
        let mut cur = data;
        let _ = decode_metadata_response(&mut cur, version);
    }
});
