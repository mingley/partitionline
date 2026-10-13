#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline_broker::raft::protocol;

fuzz_target!(|data: &[u8]| {
    let _ = protocol::decode_vote_body(data, 0);
    let _ = protocol::decode_vote_body(data, 1);
    let _ = protocol::decode_vote_body(data, 2);
    let _ = protocol::decode_vote_response(data, 0);
    let _ = protocol::decode_vote_response(data, 1);
    let _ = protocol::decode_vote_response(data, 2);
    let _ = protocol::decode_begin_body(data, 0);
    let _ = protocol::decode_begin_body(data, 1);
    let _ = protocol::decode_end_body(data, 0);
    let _ = protocol::decode_end_body(data, 1);
    let _ = protocol::decode_epoch_response(data, 0);
    let _ = protocol::decode_epoch_response(data, 1);
    let _ = protocol::decode_fetch_snapshot_body(data, 0);
    let _ = protocol::decode_fetch_snapshot_body(data, 1);
    let _ = protocol::decode_fetch_snapshot_response(data, 0);
    let _ = protocol::decode_fetch_snapshot_response(data, 1);
    let _ = protocol::decode_describe_quorum(data, 0);
    let _ = protocol::decode_describe_quorum(data, 1);
    let _ = protocol::decode_describe_quorum(data, 2);
    let _ = protocol::decode_remove_voter(data);
    let _ = protocol::decode_add_voter(data, 0);
    let _ = protocol::decode_add_voter(data, 1);
    let _ = protocol::decode_update_voter(data);
    let _ = protocol::decode_voter_change_response(data, 80, 0);
    let _ = protocol::decode_voter_change_response(data, 80, 1);
    let _ = protocol::decode_voter_change_response(data, 81, 0);
    let _ = protocol::decode_update_voter_response(data);
    let _ = protocol::decode_voters_record(data);
});
