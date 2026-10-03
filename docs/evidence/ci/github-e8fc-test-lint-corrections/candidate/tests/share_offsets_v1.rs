//! Share-offset lag negotiation, coordinator routing and legacy result compatibility.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "bounded scripted peers and assertions fail on unexpected wire outcomes"
)]

#[path = "fixtures/write-txn-markers-v2/socket_peer.rs"]
mod socket_peer;

use bytes::BytesMut;
use partitionline::protocol::admin::{
    decode_describe_share_group_offsets_request, decode_describe_share_group_offsets_response,
    decode_describe_share_group_offsets_response_versioned,
    encode_describe_share_group_offsets_request,
    encode_describe_share_group_offsets_request_versioned,
    encode_describe_share_group_offsets_response_versioned, DescribedShareGroupOffsetsPartition,
    DescribedShareGroupOffsetsPartitionWithLag, DescribedShareGroupOffsetsTopicWithLag,
    DescribedShareGroupOffsetsWithLag,
};
use partitionline::protocol::api_keys::{DESCRIBE_SHARE_GROUP_OFFSETS, FIND_COORDINATOR, METADATA};
use partitionline::{DescribeShareGroupOffsetsGroup, DescribeShareGroupOffsetsTopic, Error};
use socket_peer::{Peer, BUDGET};
use std::time::{Duration, Instant};

fn group(id: &str, lag: i64, code: i16) -> DescribedShareGroupOffsetsWithLag {
    DescribedShareGroupOffsetsWithLag {
        group_id: id.into(),
        error_code: code,
        error_message: None,
        topics: if code == 0 {
            vec![DescribedShareGroupOffsetsTopicWithLag {
                topic_name: "t".into(),
                topic_id: [1; 16],
                partitions: vec![DescribedShareGroupOffsetsPartitionWithLag {
                    partition: DescribedShareGroupOffsetsPartition {
                        partition_index: 0,
                        start_offset: 17,
                        leader_epoch: 7,
                        error_code: 0,
                        error_message: None,
                    },
                    raw_lag: lag,
                }],
            }]
        } else {
            vec![]
        },
    }
}

fn response(version: i16, groups: &[DescribedShareGroupOffsetsWithLag]) -> Vec<u8> {
    let mut wire = BytesMut::new();
    encode_describe_share_group_offsets_response_versioned(&mut wire, version, groups, 13).unwrap();
    wire.to_vec()
}

#[tokio::test]
async fn typed_and_existing_operations_negotiate_and_consume_lag() {
    for (range, version) in [((0, 0), 0), ((1, 1), 1), ((0, 1), 1)] {
        let peer = Peer::start(None, Some(range)).await;
        peer.state
            .lock()
            .share_responses
            .push_back(response(version, &[group("g", 29, 0)]));
        let mut admin = peer.admin().await;
        let requests = [DescribeShareGroupOffsetsGroup::all("g")];
        let typed = admin
            .describe_share_group_offsets_with_lag(&requests)
            .await
            .unwrap();
        assert_eq!(
            typed[0].topics[0].partitions[0].lag(),
            if version == 1 { Some(29) } else { None }
        );
        assert_eq!(
            typed[0].topics[0].partitions[0].raw_lag,
            if version == 1 { 29 } else { -1 }
        );
        let expected = typed[0].clone().into_legacy();
        assert_eq!(
            admin.describe_share_group_offsets(&requests).await.unwrap(),
            vec![expected.clone()]
        );
        assert_eq!(
            admin.list_share_group_offsets(&requests).await.unwrap(),
            vec![expected]
        );
        let captured = peer.requests(DESCRIBE_SHARE_GROUP_OFFSETS);
        assert_eq!(captured.len(), 3);
        assert!(captured
            .iter()
            .all(|row| row.node == 2 && row.version == version));
        for row in captured {
            let mut cursor = row.body.as_slice();
            assert_eq!(
                decode_describe_share_group_offsets_request(&mut cursor).unwrap(),
                requests
            );
            assert!(cursor.is_empty());
        }
        assert!(!peer.requests(FIND_COORDINATOR).is_empty());
        admin.close().await.unwrap();
    }
}

#[tokio::test]
async fn unsupported_share_offsets_fail_before_coordinator_or_metadata_work() {
    for range in [None, Some((2, 2)), Some((-2, -1))] {
        let peer = Peer::start(None, range).await;
        let mut admin = peer.admin().await;
        let before = peer.state.lock().observed.len();
        let groups = [DescribeShareGroupOffsetsGroup::all("g")];
        assert!(matches!(
            admin.describe_share_group_offsets(&groups).await,
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            admin.describe_share_group_offsets_with_lag(&groups).await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(peer.state.lock().observed.len(), before);
        assert!(peer.requests(FIND_COORDINATOR).is_empty());
        assert!(peer.requests(METADATA).is_empty());
        assert!(admin
            .describe_share_group_offsets_with_lag(&[])
            .await
            .unwrap()
            .is_empty());
        admin.close().await.unwrap();
    }
}

#[tokio::test]
async fn group_hop_errors_refresh_coordinator_while_partition_errors_remain_results() {
    for code in [14, 15, 16] {
        let peer = Peer::start(None, Some((0, 1))).await;
        {
            let mut state = peer.state.lock();
            state.share_responses.extend([
                response(1, &[group("g", -1, code)]),
                response(1, &[group("g", 7, 0)]),
            ]);
        }
        let mut admin = peer.admin().await;
        let actual = admin
            .describe_share_group_offsets_with_lag(&[DescribeShareGroupOffsetsGroup::all("g")])
            .await
            .unwrap();
        assert_eq!(actual[0].topics[0].partitions[0].lag(), Some(7));
        assert_eq!(peer.requests(DESCRIBE_SHARE_GROUP_OFFSETS).len(), 2);
        assert!(peer.requests(FIND_COORDINATOR).len() >= 2);
        admin.close().await.unwrap();
    }
    let peer = Peer::start(None, Some((1, 1))).await;
    let mut result = group("g", -1, 0);
    result.topics[0].partitions[0].partition.error_code = 29;
    peer.state
        .lock()
        .share_responses
        .push_back(response(1, &[result.clone()]));
    let mut admin = peer.admin().await;
    assert_eq!(
        admin
            .describe_share_group_offsets_with_lag(&[DescribeShareGroupOffsetsGroup::all("g")])
            .await
            .unwrap(),
        vec![result]
    );
    assert_eq!(peer.requests(DESCRIBE_SHARE_GROUP_OFFSETS).len(), 1);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_request_groups_and_reordered_results_preserve_request_order() {
    let peer = Peer::start(None, Some((0, 1))).await;
    peer.state.lock().share_responses.push_back(response(
        1,
        &[group("b", 20, 0), group("a", 1, 0), group("a", 2, 0)],
    ));
    let mut admin = peer.admin().await;
    let groups = [
        DescribeShareGroupOffsetsGroup::all("a"),
        DescribeShareGroupOffsetsGroup::all("b"),
        DescribeShareGroupOffsetsGroup::all("a"),
    ];
    let actual = admin
        .describe_share_group_offsets_with_lag(&groups)
        .await
        .unwrap();
    let values: Vec<_> = actual
        .iter()
        .map(|g| (g.group_id.as_str(), g.topics[0].partitions[0].lag()))
        .collect();
    assert_eq!(
        values,
        vec![("a", Some(1)), ("b", Some(20)), ("a", Some(2))]
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn missing_group_truncated_and_trailing_responses_fail_without_fabricated_lag() {
    let valid = response(1, &[group("g", 7, 0)]);
    for wire in [
        response(1, &[group("unrequested", 7, 0)]),
        valid[..valid.len() - 1].to_vec(),
        [valid.as_slice(), &[0]].concat(),
        vec![0, 0, 0, 0, 255, 255, 255, 255, 7],
    ] {
        let peer = Peer::start(None, Some((1, 1))).await;
        peer.state.lock().share_responses.push_back(wire);
        let mut admin = peer.admin().await;
        assert!(matches!(
            admin
                .describe_share_group_offsets_with_lag(&[DescribeShareGroupOffsetsGroup::all("g")])
                .await,
            Err(Error::Protocol(_))
        ));
        admin.close().await.unwrap();
    }
}

#[tokio::test]
async fn share_retry_and_stalled_rpc_use_one_absolute_deadline() {
    let peer = Peer::start(None, Some((0, 1))).await;
    peer.state
        .lock()
        .share_responses
        .push_back(response(1, &[group("g", -1, 16)]));
    let mut admin = peer.admin().await;
    let started = Instant::now();
    assert!(matches!(
        admin
            .describe_share_group_offsets_with_lag_timeout(
                &[DescribeShareGroupOffsetsGroup::all("g")],
                Duration::from_millis(80)
            )
            .await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(peer.requests(DESCRIBE_SHARE_GROUP_OFFSETS).len() > 1);
    admin.close().await.unwrap();

    let peer = Peer::start(None, Some((1, 1))).await;
    {
        let mut state = peer.state.lock();
        state
            .share_responses
            .push_back(response(1, &[group("g", 1, 0)]));
        state.hold_share = true;
    }
    let mut admin = peer.admin().await;
    assert!(matches!(
        admin
            .describe_share_group_offsets_timeout(
                &[DescribeShareGroupOffsetsGroup::all("g")],
                Duration::from_millis(40)
            )
            .await,
        Err(Error::Timeout)
    ));
    peer.release.notify_one();
    assert_eq!(admin.describe_topics(["t"]).await.unwrap().len(), 1);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn canceled_share_request_leaves_unrelated_operation_usable() {
    let peer = Peer::start(None, Some((1, 1))).await;
    {
        let mut state = peer.state.lock();
        state
            .share_responses
            .push_back(response(1, &[group("g", 3, 0)]));
        state.hold_share = true;
    }
    let mut admin = peer.admin().await;
    let groups = [DescribeShareGroupOffsetsGroup::all("g")];
    let mut operation = Box::pin(admin.describe_share_group_offsets_with_lag(&groups));
    tokio::select! {
        ()=peer.seen.notified()=>{},
        result=&mut operation=>panic!("stalled share operation completed: {result:?}"),
        ()=tokio::time::sleep(BUDGET)=>panic!("share request was never sent"),
    }
    drop(operation);
    peer.release.notify_one();
    assert_eq!(admin.describe_topics(["t"]).await.unwrap().len(), 1);
    admin.close().await.unwrap();
}

#[test]
fn lag_signed_extremes_follow_official_negative_is_absent_interpretation() {
    for raw_lag in [i64::MIN, -2, -1, 0, 1, i64::MAX] {
        for version in [0, 1] {
            let expected = group("g", raw_lag, 0);
            let wire = response(version, std::slice::from_ref(&expected));
            let mut cursor = wire.as_slice();
            let (actual, throttle) =
                decode_describe_share_group_offsets_response_versioned(&mut cursor, version)
                    .unwrap();
            assert!(cursor.is_empty());
            assert_eq!(throttle, 13);
            let value = &actual[0].topics[0].partitions[0];
            assert_eq!(value.raw_lag, if version == 0 { -1 } else { raw_lag });
            assert_eq!(
                value.lag(),
                if version == 0 || raw_lag < 0 {
                    None
                } else {
                    Some(raw_lag)
                }
            );
            if version == 0 {
                let (legacy, legacy_throttle) =
                    decode_describe_share_group_offsets_response(&mut wire.as_slice()).unwrap();
                assert_eq!(legacy_throttle, throttle);
                assert_eq!(legacy, vec![actual[0].clone().into_legacy()]);
            }
        }
    }
}

#[test]
fn nullable_empty_named_requests_and_schema_counts_are_bounded() {
    let groups = [
        DescribeShareGroupOffsetsGroup::all("g"),
        DescribeShareGroupOffsetsGroup::new("empty"),
        DescribeShareGroupOffsetsGroup {
            group_id: "named".into(),
            topics: Some(vec![DescribeShareGroupOffsetsTopic::new("t", vec![0, 1])]),
        },
    ];
    let mut legacy = BytesMut::new();
    encode_describe_share_group_offsets_request(&mut legacy, &groups).unwrap();
    for version in [0, 1] {
        let mut wire = BytesMut::new();
        encode_describe_share_group_offsets_request_versioned(&mut wire, version, &groups).unwrap();
        assert_eq!(wire, legacy);
        let mut cursor = wire.as_ref();
        assert_eq!(
            decode_describe_share_group_offsets_request(&mut cursor).unwrap(),
            groups
        );
        assert!(cursor.is_empty());
    }
    let wire = response(1, &[group("g", 2, 0)]);
    for end in 0..wire.len() {
        assert!(
            decode_describe_share_group_offsets_response_versioned(&mut &wire[..end], 1).is_err()
        );
    }
    for version in [-1, 2, i16::MAX] {
        let mut wire = BytesMut::new();
        assert!(
            encode_describe_share_group_offsets_request_versioned(&mut wire, version, &groups)
                .is_err()
        );
        assert!(wire.is_empty());
    }
}

#[test]
fn pinned_schema_reference_frame_keeps_lag_before_partition_error() {
    use partitionline::protocol::header::{decode_request_header, decode_response_header};
    let mut cursor =
        include_bytes!("fixtures/share-offsets-v1/schema-lag-1.request.bin").as_slice();
    let header = decode_request_header(&mut cursor).unwrap();
    assert_eq!(
        (header.api_key, header.api_version, header.correlation_id),
        (90, 1, 7)
    );
    assert_eq!(
        decode_describe_share_group_offsets_request(&mut cursor).unwrap(),
        vec![DescribeShareGroupOffsetsGroup::all("g")]
    );
    assert!(cursor.is_empty());
    let mut cursor =
        include_bytes!("fixtures/share-offsets-v1/schema-lag-1.response.bin").as_slice();
    assert_eq!(
        decode_response_header(&mut cursor, 90, 1)
            .unwrap()
            .correlation_id,
        7
    );
    let (actual, throttle) =
        decode_describe_share_group_offsets_response_versioned(&mut cursor, 1).unwrap();
    assert_eq!(actual, vec![group("g", 1, 0)]);
    assert_eq!(throttle, 13);
    assert!(cursor.is_empty());
}
