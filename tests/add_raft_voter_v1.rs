//! AddRaftVoter v1 `AckWhenCommitted` wire and mock negotiation.
//!
//! A mock broker cannot prove that `false` returns after a local Raft append
//! while `true` waits for a majority commit. That remains an open KL05-40 gate.
#![expect(
    clippy::unwrap_used,
    reason = "finite wire and mock-broker regression assertions"
)]

mod common;

use bytes::BytesMut;
use partitionline::error;
use partitionline::protocol::admin::{
    decode_add_raft_voter_request, decode_add_raft_voter_response, encode_add_raft_voter_request,
    encode_add_raft_voter_response, AddRaftVoterRequest, AddRaftVoterResponse,
};
use partitionline::protocol::api_keys::ADD_RAFT_VOTER;
use partitionline::{
    AddRaftVoterOptions, Admin, Error, RaftVoterAcknowledgement, RaftVoterEndpoint,
};
use std::time::{Duration, Instant};

fn request(ack: bool) -> AddRaftVoterRequest {
    AddRaftVoterRequest {
        cluster_id: Some("cluster-410".into()),
        timeout_ms: 1500,
        voter_id: 7,
        voter_directory_id: [0x11; 16],
        listeners: vec![RaftVoterEndpoint::new("CONTROLLER", "127.0.0.1", 9093).unwrap()],
        ack_when_committed: ack,
    }
}

fn encode_request(version: i16, ack: bool) -> BytesMut {
    let mut bytes = BytesMut::new();
    encode_add_raft_voter_request(&mut bytes, version, &request(ack)).unwrap();
    bytes
}

fn options(ack: bool) -> AddRaftVoterOptions {
    AddRaftVoterOptions {
        cluster_id: Some("cluster-410".into()),
        timeout: Some(Duration::from_secs(2)),
        ack_when_committed: ack,
    }
}

fn success() -> AddRaftVoterResponse {
    AddRaftVoterResponse {
        throttle_time_ms: 57,
        error_code: 0,
        error_message: None,
        acknowledgement: RaftVoterAcknowledgement::Committed,
    }
}

#[test]
fn v1_serializes_true_and_false_and_v0_omits_the_field() {
    let v0 = encode_request(0, true);
    let v1_true = encode_request(1, true);
    let v1_false = encode_request(1, false);
    assert_eq!(v1_true.len(), v0.len() + 1);
    let (v0_body, v0_tags) = v0.split_at(v0.len() - 1);
    let (v1_body, v1_tail) = v1_true.split_at(v1_true.len() - 2);
    assert_eq!(v0_tags, &[0]);
    assert_eq!(v1_body, v0_body);
    assert_eq!(v1_tail, &[1, 0]);
    let (false_body, false_tail) = v1_false.split_at(v1_false.len() - 2);
    assert_eq!(false_body, v1_body);
    assert_eq!(false_tail, &[0, 0]);

    let mut fresh = BytesMut::new();
    let rejected = encode_add_raft_voter_request(&mut fresh, 0, &request(false)).unwrap_err();
    assert!(matches!(rejected, Error::Unsupported(_)));
    assert!(fresh.is_empty());

    let mut true_cursor = v1_true.as_ref();
    let mut false_cursor = v1_false.as_ref();
    let mut v0_cursor = v0.as_ref();
    let decoded_true = decode_add_raft_voter_request(&mut true_cursor, 1).unwrap();
    let decoded_false = decode_add_raft_voter_request(&mut false_cursor, 1).unwrap();
    assert!(decoded_true.ack_when_committed);
    assert!(!decoded_false.ack_when_committed);
    assert!(true_cursor.is_empty());
    assert!(false_cursor.is_empty());
    assert!(decode_add_raft_voter_request(&mut v0_cursor, 1).is_err());

    let mut invalid = v1_true.to_vec();
    let ack_at = invalid.len() - 2;
    *invalid.get_mut(ack_at).unwrap() = 2;
    let mut invalid_cursor = invalid.as_slice();
    assert!(decode_add_raft_voter_request(&mut invalid_cursor, 1).is_err());
    let (truncated, _) = v1_true.split_at(v1_true.len() - 1);
    let mut truncated_cursor = truncated;
    assert!(decode_add_raft_voter_request(&mut truncated_cursor, 1).is_err());

    let mut response = BytesMut::new();
    encode_add_raft_voter_response(&mut response, 0, &success()).unwrap();
    let mut response_cursor = response.as_ref();
    let as_v1 = decode_add_raft_voter_response(&mut response_cursor, 1).unwrap();
    assert_eq!(as_v1.acknowledgement, RaftVoterAcknowledgement::Committed);
    assert_eq!(as_v1.throttle_time_ms, 57);
}

#[tokio::test]
async fn explicit_false_on_v0_only_broker_sends_nothing() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(&mock.addr).await.unwrap();
    let directory = partitionline::Uuid::from_parts(7, 9);
    let error = admin
        .add_raft_voter(
            7,
            directory,
            request(false).listeners.clone(),
            options(false),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Unsupported(ref message) if message.contains("requires v1")),
        "{error}"
    );
    assert!(mock.add_raft_voter_requests().is_empty());

    mock.hide_api(ADD_RAFT_VOTER);
    let absent = admin
        .add_raft_voter(7, directory, request(true).listeners, options(true))
        .await
        .unwrap_err();
    assert!(matches!(absent, Error::Unsupported(_)));
    assert!(mock.add_raft_voter_requests().is_empty());
    admin.close().await.unwrap();
}

#[tokio::test]
async fn v1_true_is_committed_and_false_is_local_append_only() {
    let mock = common::Mock::start().await;
    mock.set_api_range(ADD_RAFT_VOTER, 0, 1);
    mock.queue_add_raft_voter_response(success());
    mock.queue_add_raft_voter_response(success());
    let mut admin = Admin::connect(&mock.addr).await.unwrap();
    let directory = partitionline::Uuid::from_parts(7, 9);
    let committed = admin
        .add_raft_voter(7, directory, request(true).listeners.clone(), options(true))
        .await
        .unwrap();
    assert_eq!(
        committed.acknowledgement,
        RaftVoterAcknowledgement::Committed
    );
    assert_eq!(committed.throttle_time_ms, 57);
    let local = admin
        .add_raft_voter(8, directory, request(false).listeners, options(false))
        .await
        .unwrap();
    assert_eq!(local.acknowledgement, RaftVoterAcknowledgement::LocalAppend);
    let seen = mock.add_raft_voter_requests();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].1, 1);
    assert!(seen[0].2.ack_when_committed);
    assert_eq!(seen[1].1, 1);
    assert!(!seen[1].2.ack_when_committed);
    assert_ne!(seen[0].2.voter_id, seen[1].2.voter_id);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn default_true_uses_v1_when_offered_and_v0_on_a_legacy_broker() {
    let modern = common::Mock::start().await;
    modern.set_api_range(ADD_RAFT_VOTER, 1, 1);
    modern.queue_add_raft_voter_response(success());
    let mut admin = Admin::connect(&modern.addr).await.unwrap();
    let directory = partitionline::Uuid::from_parts(7, 9);
    let result = admin
        .add_raft_voter(
            7,
            directory,
            request(true).listeners.clone(),
            AddRaftVoterOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.acknowledgement, RaftVoterAcknowledgement::Committed);
    assert_eq!(modern.add_raft_voter_requests()[0].1, 1);
    assert!(modern.add_raft_voter_requests()[0].2.ack_when_committed);
    admin.close().await.unwrap();

    let legacy = common::Mock::start().await;
    legacy.queue_add_raft_voter_response(success());
    let mut admin = Admin::connect(&legacy.addr).await.unwrap();
    let result = admin
        .add_raft_voter(
            7,
            directory,
            request(true).listeners,
            AddRaftVoterOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.acknowledgement, RaftVoterAcknowledgement::Committed);
    assert_eq!(legacy.add_raft_voter_requests()[0].1, 0);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn broker_error_and_lost_response_are_not_durable_success() {
    let mock = common::Mock::start().await;
    mock.set_api_range(ADD_RAFT_VOTER, 0, 1);
    mock.drop_add_raft_voter_responses(1);
    mock.queue_add_raft_voter_response(success());
    mock.queue_add_raft_voter_response(AddRaftVoterResponse {
        throttle_time_ms: 0,
        error_code: error::DUPLICATE_VOTER,
        error_message: Some("membership may already have changed".into()),
        acknowledgement: RaftVoterAcknowledgement::Committed,
    });
    let mut admin = Admin::connect(&mock.addr).await.unwrap();
    let directory = partitionline::Uuid::from_parts(7, 9);
    let lost = admin
        .add_raft_voter(
            7,
            directory,
            request(false).listeners.clone(),
            options(false),
        )
        .await
        .unwrap_err();
    assert_eq!(lost.broker_code(), Some(error::DUPLICATE_VOTER));
    assert!(mock
        .add_raft_voter_requests()
        .iter()
        .all(|row| row.1 == 1 && !row.2.ack_when_committed));

    mock.set_add_raft_voter_delay(Duration::from_secs(2));
    let started = Instant::now();
    let stalled = admin
        .add_raft_voter(
            7,
            directory,
            request(false).listeners,
            AddRaftVoterOptions {
                timeout: Some(Duration::from_millis(100)),
                ack_when_committed: false,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(stalled, Error::Timeout));
    assert!(started.elapsed() < Duration::from_secs(1));
    admin.close().await.unwrap();
}
