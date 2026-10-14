#![allow(clippy::panic)]
//! Decoder check against bytes packed independently of this crate.

use partitionline_broker::protocol::ApiVersion;
use partitionline_broker::raft::protocol::{
    self, BeginEpochBody, DescribeQuorumBody, EndEpochBody, FetchSnapshotBody,
    FetchSnapshotResponseBody, KafkaVoter, KafkaVotersRecord, LeaderEndpoint, PreferredCandidate,
    QuorumNode, QuorumReplica, SnapshotLeader, UpdateVoterBody, VoteBody, VoteNodeEndpoint,
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
    let describe = protocol::encode_describe_quorum(&DescribeQuorumBody {
        version: 2,
        error_code: 0,
        error_message: None,
        topic: "__cluster_metadata".to_owned(),
        partition: 0,
        partition_error: 0,
        partition_error_message: None,
        leader_id: 2,
        leader_epoch: 3,
        high_watermark: 9,
        voters: vec![QuorumReplica {
            replica_id: 2,
            directory: Some([8u8; 16]),
            log_end_offset: 10,
            last_fetch_timestamp: 1,
            last_caught_up_timestamp: 2,
        }],
        observers: Vec::new(),
        nodes: vec![QuorumNode {
            node_id: 2,
            listeners: vec![LeaderEndpoint {
                name: "plaintext".to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 9093,
            }],
        }],
    })
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(describe, fixture("describe-quorum-v2.bin"));
}

fn apache(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/conformance/broker/kraft-layout/apache")
        .join(name);
    let mut file =
        std::fs::File::open(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut bytes)
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    bytes
}

#[test]
fn apache_431_bodies_round_trip_and_stay_unadvertised() {
    for name in [
        "vote-v0.bin",
        "vote-v2.bin",
        "fetch-snapshot-v1.bin",
        "begin-v1.bin",
        "end-v1.bin",
        "update-voter.bin",
        "voters-record.bin",
        "fetch-snapshot-response-v1.bin",
        "describe-quorum-v2.bin",
    ] {
        assert_eq!(apache(name), fixture(name), "{name}");
    }
    let vote0 = apache("vote-v0.bin");
    let decoded = protocol::decode_vote_body(&vote0, 0).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        protocol::encode_vote_body(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        vote0
    );
    let vote2 = apache("vote-v2.bin");
    let decoded = protocol::decode_vote_body(&vote2, 2).unwrap_or_else(|error| panic!("{error:?}"));
    assert!(decoded.pre_vote);
    assert_eq!(
        protocol::encode_vote_body(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        vote2
    );
    let fetch = apache("fetch-snapshot-v1.bin");
    let decoded =
        protocol::decode_fetch_snapshot_body(&fetch, 1).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        protocol::encode_fetch_snapshot_body(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        fetch
    );
    let begin = apache("begin-v1.bin");
    let decoded =
        protocol::decode_begin_body(&begin, 1).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        protocol::encode_begin_body(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        begin
    );
    let end = apache("end-v1.bin");
    let decoded = protocol::decode_end_body(&end, 1).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        protocol::encode_end_body(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        end
    );
    let update = apache("update-voter.bin");
    let decoded =
        protocol::decode_update_voter(&update).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        protocol::encode_update_voter(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        update
    );
    let record = apache("voters-record.bin");
    let decoded =
        protocol::decode_voters_record(&record).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        protocol::encode_voters_record(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        record
    );
    let response = apache("fetch-snapshot-response-v1.bin");
    let decoded = protocol::decode_fetch_snapshot_response(&response, 1)
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        protocol::encode_fetch_snapshot_response(&decoded)
            .unwrap_or_else(|error| panic!("{error:?}")),
        response
    );
    let describe = apache("describe-quorum-v2.bin");
    let decoded =
        protocol::decode_describe_quorum(&describe, 2).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(decoded.error_message, None);
    assert_eq!(
        protocol::encode_describe_quorum(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        describe
    );
    let empty = apache("describe-quorum-v2-empty.bin");
    let decoded =
        protocol::decode_describe_quorum(&empty, 2).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(decoded.error_message.as_deref(), Some(""));
    assert_eq!(decoded.partition_error_message.as_deref(), Some(""));
    assert_eq!(
        protocol::encode_describe_quorum(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        empty
    );
    let add0 = apache("add-voter-v0.bin");
    let decoded = protocol::decode_add_voter(&add0, 0).unwrap_or_else(|error| panic!("{error:?}"));
    assert!(decoded.ack_when_committed);
    assert_eq!(
        protocol::encode_add_voter(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        add0
    );
    let add1 = apache("add-voter-v1.bin");
    let decoded = protocol::decode_add_voter(&add1, 1).unwrap_or_else(|error| panic!("{error:?}"));
    assert!(!decoded.ack_when_committed);
    assert_eq!(decoded.voter_id, 4);
    assert_eq!(
        protocol::encode_add_voter(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        add1
    );
    let remove = apache("remove-voter.bin");
    let decoded =
        protocol::decode_remove_voter(&remove).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(decoded.voter_directory, [7u8; 16]);
    assert_eq!(
        protocol::encode_remove_voter(&decoded).unwrap_or_else(|error| panic!("{error:?}")),
        remove
    );
    let peer = [
        ApiVersion {
            api_key: 52,
            min_version: 0,
            max_version: 2,
        },
        ApiVersion {
            api_key: 53,
            min_version: 0,
            max_version: 1,
        },
        ApiVersion {
            api_key: 54,
            min_version: 0,
            max_version: 1,
        },
        ApiVersion {
            api_key: 55,
            min_version: 0,
            max_version: 2,
        },
        ApiVersion {
            api_key: 59,
            min_version: 0,
            max_version: 1,
        },
        ApiVersion {
            api_key: 80,
            min_version: 0,
            max_version: 1,
        },
        ApiVersion {
            api_key: 81,
            min_version: 0,
            max_version: 0,
        },
        ApiVersion {
            api_key: 82,
            min_version: 0,
            max_version: 0,
        },
    ];
    assert_eq!(protocol::negotiate(&peer, 52), Some(0));
    assert_eq!(protocol::negotiate(&peer, 53), Some(0));
    assert_eq!(protocol::negotiate(&peer, 54), Some(0));
    assert_eq!(protocol::negotiate(&peer, 55), None);
    assert_eq!(protocol::negotiate(&peer, 59), None);
    assert_eq!(protocol::negotiate(&peer, 80), None);
    assert_eq!(protocol::negotiate(&peer, 81), None);
    assert_eq!(protocol::negotiate(&peer, 82), None);
}
