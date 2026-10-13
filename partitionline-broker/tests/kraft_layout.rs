#![allow(clippy::panic)]
//! Decoder check against bytes packed independently of this crate.

use partitionline_broker::raft::protocol::{
    self, BeginEpochBody, EndEpochBody, FetchSnapshotBody, FetchSnapshotResponseBody, KafkaVoter,
    KafkaVotersRecord, LeaderEndpoint, PreferredCandidate, SnapshotLeader, UpdateVoterBody,
    VoteBody, VoteNodeEndpoint,
};

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

#[test]
fn begin_end_update_and_voters_record_match_the_independent_layout() {
    let listener = LeaderEndpoint {
        name: "CONTROLLER".to_owned(),
        host: "localhost".to_owned(),
        port: 9093,
    };
    let begin = protocol::encode_begin_body(&BeginEpochBody {
        version: 1,
        cluster_id: Some("cluster-1".to_owned()),
        voter_id: 1,
        topic: "__cluster_metadata".to_owned(),
        partition: 0,
        voter_directory: Some([6u8; 16]),
        leader_id: 2,
        leader_epoch: 3,
        leader_endpoints: vec![listener.clone()],
    })
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(begin, fixture("begin-v1.bin"));
    let end = protocol::encode_end_body(&EndEpochBody {
        version: 1,
        cluster_id: Some("cluster-1".to_owned()),
        topic: "__cluster_metadata".to_owned(),
        partition: 0,
        leader_id: 2,
        leader_epoch: 3,
        preferred: vec![PreferredCandidate {
            id: 1,
            directory: Some([6u8; 16]),
        }],
        leader_endpoints: vec![listener.clone()],
    })
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(end, fixture("end-v1.bin"));
    let update = protocol::encode_update_voter(&UpdateVoterBody {
        cluster_id: Some("cluster-1".to_owned()),
        current_leader_epoch: 3,
        voter_id: 2,
        voter_directory: [7u8; 16],
        listeners: vec![listener.clone()],
        kraft_min: 0,
        kraft_max: 1,
    })
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(update, fixture("update-voter.bin"));
    let record = protocol::encode_voters_record(&KafkaVotersRecord {
        version: 0,
        voters: vec![KafkaVoter {
            id: 2,
            directory: [7u8; 16],
            endpoints: vec![listener],
            kraft_min: 0,
            kraft_max: 1,
        }],
    })
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(record, fixture("voters-record.bin"));
    let response = protocol::encode_fetch_snapshot_response(&FetchSnapshotResponseBody {
        version: 1,
        throttle_ms: 0,
        error_code: 0,
        topic: "__cluster_metadata".to_owned(),
        partition: 0,
        partition_error: 0,
        end_offset: 4,
        epoch: 1,
        leader: Some(SnapshotLeader {
            leader_id: 2,
            leader_epoch: 1,
        }),
        size: 4,
        position: 0,
        records: Some(b"snap".to_vec()),
        endpoints: vec![VoteNodeEndpoint {
            node_id: 2,
            host: "localhost".to_owned(),
            port: 9093,
        }],
    })
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(response, fixture("fetch-snapshot-response-v1.bin"));
}
