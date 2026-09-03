#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline::protocol::records::decode_record_batches;

fuzz_target!(|data: &[u8]| {
    let mut cur = data;
    let _ = decode_record_batches(&mut cur);
});
