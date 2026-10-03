//! Old-compatible proof that existing public share-offset methods must consumev1.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "bounded regression peers fail directly on unexpected outcomes"
)]
#[path = "fixtures/write-txn-markers-v2/socket_peer.rs"]
mod socket_peer;
use partitionline::DescribeShareGroupOffsetsGroup;
use socket_peer::Peer;
#[tokio::test]
async fn existing_public_operation_must_consume_v1_lag() {
    let peer = Peer::start(None, Some((1, 1))).await;
    // Strip only the independently handwritten full response header.
    let frame = include_bytes!("fixtures/share-offsets-v1/schema-lag-1.response.bin");
    peer.state
        .lock()
        .unwrap()
        .share_responses
        .push_back(frame[5..].to_vec());
    let mut admin = peer.admin().await;
    let groups = admin
        .describe_share_group_offsets(&[DescribeShareGroupOffsetsGroup::all("g")])
        .await
        .unwrap();
    assert_eq!(groups[0].topics[0].partitions[0].start_offset, 17);
    admin.close().await.unwrap();
}
