#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline_broker::protocol::{Limits, RequestHeader};
use partitionline_broker::raft::membership::Voters;

fuzz_target!(|data: &[u8]| {
    let limits = Limits::default();
    let _ = RequestHeader::parse(data, 1, limits);
    let _ = RequestHeader::parse(data, 2, limits);
    let _ = Voters::decode(data);
});
