#![allow(clippy::panic)]
//! Decoder check against bytes packed independently of this crate.

use partitionline_broker::raft::protocol::{self, FetchSnapshotBody, VoteBody};

fn fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/conformance/broker/kraft-layout")
        .join(name);
    let mut file =
        std::fs::File::open(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut bytes)
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    bytes
}

#[test]
fn vote_v0_matches_the_independent_layout() {
    let bytes = fixture("vote-v0.bin");
    let decoded = protocol::decode_vote_body(&bytes, 0).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(decoded.cluster_id.as_deref(), Some("cluster-1"));
    assert_eq!(decoded.topic, "__cluster_metadata");
    assert_eq!(decoded.replica_id, 2);
    assert!(decoded.replica_directory.is_none());
    let encoded = protocol::encode_vote_body(&decoded).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(encoded, bytes);
}

#[test]
fn vote_v2_matches_the_independent_layout() {
    let bytes = fixture("vote-v2.bin");
    let decoded = protocol::decode_vote_body(&bytes, 2).unwrap_or_else(|error| panic!("{error:?}"));
    assert!(decoded.pre_vote);
    assert_eq!(decoded.replica_directory, Some([7u8; 16]));
    assert_eq!(decoded.voter_directory, Some([8u8; 16]));
    assert_eq!(decoded.last_offset, 3);
    let encoded = protocol::encode_vote_body(&VoteBody {
        version: 2,
        ..decoded
    })
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(encoded, bytes);
}

#[test]
fn fetch_snapshot_v1_includes_the_snapshot_id_tag_suffix() {
    let bytes = fixture("fetch-snapshot-v1.bin");
    let decoded =
        protocol::decode_fetch_snapshot_body(&bytes, 1).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(decoded.cluster_id.as_deref(), Some("cluster-1"));
    assert_eq!(decoded.end_offset, 10);
    assert_eq!(decoded.position, 0);
    assert_eq!(decoded.replica_directory, Some([7u8; 16]));
    let encoded = protocol::encode_fetch_snapshot_body(&FetchSnapshotBody {
        version: 1,
        ..decoded
    })
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(encoded, bytes);
}
