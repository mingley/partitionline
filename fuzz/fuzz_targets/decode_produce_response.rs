#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline::protocol::api::decode_produce_response;

fuzz_target!(|data: &[u8]| {
    for version in [3_i16, 8, 9, 10, 11, 12] {
        let mut cur = data;
        let _ = decode_produce_response(&mut cur, version);
    }
});
