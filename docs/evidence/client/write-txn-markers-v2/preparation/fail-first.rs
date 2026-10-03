//! Old-compatible public regressions copied into an isolated baseline test target.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "bounded regression peers fail directly on unexpected outcomes"
)]
#[path = "fixtures/write-txn-markers-v2/socket_peer.rs"]
mod socket_peer;
use bytes::BytesMut;
use partitionline::protocol::txn::{
    encode_write_txn_markers_response, WritableTxnMarker, WritableTxnMarkerTopic,
};
use partitionline::{AbortTransactionSpec, Error};
use socket_peer::Peer;
fn marker() -> WritableTxnMarker {
    WritableTxnMarker {
        producer_id: 1000,
        producer_epoch: 2,
        transaction_result: false,
        topics: vec![WritableTxnMarkerTopic {
            name: "t".into(),
            partitions: vec![0],
        }],
        coordinator_epoch: 7,
    }
}
fn spec() -> AbortTransactionSpec {
    AbortTransactionSpec {
        topic: "t".into(),
        partition: 0,
        producer_id: 1000,
        producer_epoch: 2,
        coordinator_epoch: 7,
    }
}
fn response(code: i16) -> Vec<u8> {
    let mut wire = BytesMut::new();
    encode_write_txn_markers_response(&mut wire, 1, &[marker().result(code)]).unwrap();
    wire.to_vec()
}
#[tokio::test]
async fn missing_result_must_not_mean_success() {
    let peer = Peer::start(Some((1, 1)), None).await;
    peer.state
        .lock()
        .unwrap()
        .marker_responses
        .push_back(vec![1, 0]);
    let mut admin = peer.admin().await;
    assert!(matches!(
        admin.abort_transaction(spec()).await,
        Err(Error::Protocol(_))
    ));
    admin.close().await.unwrap();
}
#[tokio::test]
async fn broker_and_replica_unavailability_must_remap_and_retry() {
    for error in [8, 9] {
        let peer = Peer::start(Some((1, 1)), None).await;
        peer.state
            .lock()
            .unwrap()
            .marker_responses
            .extend([response(error), response(0)]);
        let mut admin = peer.admin().await;
        admin.abort_transaction(spec()).await.unwrap();
        admin.close().await.unwrap();
    }
}
#[tokio::test]
async fn version2_only_public_abort_must_negotiate_the_default_marker() {
    let peer = Peer::start(Some((2, 2)), None).await;
    peer.state
        .lock()
        .unwrap()
        .marker_responses
        .push_back(response(0));
    let mut admin = peer.admin().await;
    admin.abort_transaction(spec()).await.unwrap();
    admin.close().await.unwrap();
}
