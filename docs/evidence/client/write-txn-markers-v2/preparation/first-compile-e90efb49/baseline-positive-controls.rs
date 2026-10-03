//! Positive public-API controls for the declared old-source regression overlay.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "bounded regression peers fail directly on unexpected outcomes"
)]
#[path = "fixtures/write-txn-markers-v2/socket_peer.rs"]
mod socket_peer;
use bytes::BytesMut;
use partitionline::protocol::admin::{
    encode_describe_share_group_offsets_response_with_throttle, DescribedShareGroupOffsets,
    DescribedShareGroupOffsetsPartition, DescribedShareGroupOffsetsTopic,
};
use partitionline::protocol::txn::{
    encode_write_txn_markers_response, WritableTxnMarker, WritableTxnMarkerTopic,
};
use partitionline::{AbortTransactionSpec, DescribeShareGroupOffsetsGroup};
use socket_peer::Peer;

#[tokio::test]
async fn old_supported_v1_abort_positive_control() {
    let peer = Peer::start(Some((1, 1)), None).await;
    let marker = WritableTxnMarker {
        producer_id: 1000,
        producer_epoch: 2,
        transaction_result: false,
        topics: vec![WritableTxnMarkerTopic {
            name: "t".into(),
            partitions: vec![0],
        }],
        coordinator_epoch: 7,
    };
    let mut response = BytesMut::new();
    encode_write_txn_markers_response(&mut response, 1, &[marker.result(0)]).unwrap();
    peer.state.lock().unwrap().marker_responses.push_back(response.to_vec());
    let mut admin = peer.admin().await;
    admin.abort_transaction(AbortTransactionSpec {
        topic: "t".into(), partition: 0, producer_id: 1000,
        producer_epoch: 2, coordinator_epoch: 7,
    }).await.unwrap();
    admin.close().await.unwrap();
}

#[tokio::test]
async fn old_supported_v0_share_positive_control() {
    let peer = Peer::start(None, Some((0, 0))).await;
    let group = DescribedShareGroupOffsets {
        group_id: "g".into(), error_code: 0, error_message: None,
        topics: vec![DescribedShareGroupOffsetsTopic {
            topic_name: "t".into(), topic_id: [1; 16],
            partitions: vec![DescribedShareGroupOffsetsPartition {
                partition_index: 0, start_offset: 17, leader_epoch: 7,
                error_code: 0, error_message: None,
            }],
        }],
    };
    let mut response = BytesMut::new();
    encode_describe_share_group_offsets_response_with_throttle(&mut response, &[group], 13).unwrap();
    peer.state.lock().unwrap().share_responses.push_back(response.to_vec());
    let mut admin = peer.admin().await;
    let groups = admin.describe_share_group_offsets(&[DescribeShareGroupOffsetsGroup::all("g")]).await.unwrap();
    assert_eq!(groups[0].topics[0].partitions[0].start_offset, 17);
    admin.close().await.unwrap();
}
