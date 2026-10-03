//! WriteTxnMarkers version negotiation and authoritative public abort outcomes.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "bounded scripted peers and assertions fail on unexpected wire outcomes"
)]

#[path = "fixtures/write-txn-markers-v2/socket_peer.rs"]
mod socket_peer;

use bytes::BytesMut;
use partitionline::protocol::api_keys::{METADATA, WRITE_TXN_MARKERS};
use partitionline::protocol::txn::{
    encode_write_txn_markers_response, WritableTxnMarker, WritableTxnMarkerPartitionResult,
    WritableTxnMarkerResult, WritableTxnMarkerTopic, WritableTxnMarkerTopicResult,
};
use partitionline::{AbortTransactionSpec, Error};
use socket_peer::{Peer, BUDGET};
use std::time::{Duration, Instant};

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

// Response1 and2 have identical bodies, so this support routine also builds
// an executable counterexample against the old v1-only public implementation.
fn response(results: &[WritableTxnMarkerResult]) -> Vec<u8> {
    let mut wire = BytesMut::new();
    encode_write_txn_markers_response(&mut wire, 1, results).unwrap();
    wire.to_vec()
}

fn good(code: i16) -> WritableTxnMarkerResult {
    marker().result(code)
}

#[tokio::test]
async fn public_abort_negotiates_v2_and_default_transaction_version() {
    for (range, expected) in [((0, 0), 0), ((1, 1), 1), ((2, 2), 2), ((1, 2), 2)] {
        let peer = Peer::start(Some(range), None).await;
        peer.state
            .lock()
            .marker_responses
            .push_back(response(&[good(0)]));
        let mut admin = peer.admin().await;
        admin.abort_transaction(spec()).await.unwrap();
        let requests = peer.requests(WRITE_TXN_MARKERS);
        assert_eq!(requests.len(), 1);
        assert_eq!((requests[0].node, requests[0].version), (2, expected));
        let mut cursor = requests[0].body.as_slice();
        let values = partitionline::protocol::txn::decode_write_txn_markers_request_with_transaction_versions(
            &mut cursor, expected).unwrap();
        assert!(cursor.is_empty());
        assert_eq!(values[0].marker, marker());
        assert_eq!(values[0].transaction_version, 0);
        assert!(peer.requests(METADATA).iter().any(|row| row.node == 1));
        admin.close().await.unwrap();
    }
}

#[tokio::test]
async fn unsupported_abort_fails_before_metadata_or_marker_work() {
    for range in [None, Some((3, 3)), Some((-2, -1))] {
        let peer = Peer::start(range, None).await;
        let mut admin = peer.admin().await;
        let before = peer.state.lock().observed.len();
        assert!(matches!(
            admin.abort_transaction(spec()).await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(peer.state.lock().observed.len(), before);
        assert!(peer.requests(METADATA).is_empty());
        admin.close().await.unwrap();
    }
}

#[tokio::test]
async fn omitted_duplicate_or_unrelated_abort_results_cannot_succeed() {
    let target = good(0);
    let mut wrong_producer = target.clone();
    wrong_producer.producer_id += 1;
    let mut wrong_topic = target.clone();
    wrong_topic.topics[0].name = "other".into();
    let mut wrong_partition = target.clone();
    wrong_partition.topics[0].partitions[0].partition_index = 1;
    let mut no_topic = target.clone();
    no_topic.topics.clear();
    let mut no_partition = target.clone();
    no_partition.topics[0].partitions.clear();
    let mut duplicate_topic = target.clone();
    duplicate_topic.topics.push(target.topics[0].clone());
    let mut duplicate_partition = target.clone();
    duplicate_partition.topics[0]
        .partitions
        .push(target.topics[0].partitions[0].clone());
    let mut unrelated_nonzero = wrong_producer.clone();
    unrelated_nonzero.topics[0].partitions[0].error_code = 31;
    let cases = vec![
        vec![],
        vec![wrong_producer.clone()],
        vec![wrong_topic],
        vec![wrong_partition],
        vec![no_topic],
        vec![no_partition],
        vec![target.clone(), target.clone()],
        vec![duplicate_topic],
        vec![duplicate_partition],
        vec![target.clone(), wrong_producer],
        vec![target.clone(), unrelated_nonzero],
    ];
    for results in cases {
        let peer = Peer::start(Some((1, 1)), None).await;
        peer.state
            .lock()
            .marker_responses
            .push_back(response(&results));
        let mut admin = peer.admin().await;
        assert!(
            matches!(
                admin.abort_transaction(spec()).await,
                Err(Error::Protocol(_))
            ),
            "non-authoritative zero-code results cannot imply success: {results:?}"
        );
        assert_eq!(peer.requests(WRITE_TXN_MARKERS).len(), 1);
        assert_eq!(admin.describe_topics(["t"]).await.unwrap().len(), 1);
        admin.close().await.unwrap();
    }
}

#[tokio::test]
async fn broker_and_replica_unavailability_refresh_the_selected_partition_leader() {
    for code in [3, 6, 8, 9] {
        let peer = Peer::start(Some((1, 2)), None).await;
        {
            let mut state = peer.state.lock();
            state
                .marker_responses
                .extend([response(&[good(code)]), response(&[good(0)])]);
        }
        let mut admin = peer.admin().await;
        admin.abort_transaction(spec()).await.unwrap();
        assert_eq!(peer.requests(WRITE_TXN_MARKERS).len(), 2);
        assert!(peer.requests(METADATA).len() >= 2);
        assert!(peer
            .requests(WRITE_TXN_MARKERS)
            .iter()
            .all(|row| row.node == 2 && row.version == 2));
        admin.close().await.unwrap();
    }
}

#[tokio::test]
async fn authorization_and_producer_or_coordinator_fencing_are_terminal() {
    for code in [31, 47, 52] {
        let peer = Peer::start(Some((1, 2)), None).await;
        peer.state
            .lock()
            .marker_responses
            .push_back(response(&[good(code)]));
        let mut admin = peer.admin().await;
        assert!(
            matches!(admin.abort_transaction(spec()).await, Err(Error::Broker {code: got,..}) if got==code)
        );
        assert_eq!(peer.requests(WRITE_TXN_MARKERS).len(), 1);
        admin.close().await.unwrap();
    }
}

#[tokio::test]
async fn retry_deadline_is_one_absolute_budget() {
    let peer = Peer::start(Some((1, 2)), None).await;
    peer.state
        .lock()
        .marker_responses
        .push_back(response(&[good(8)]));
    let mut admin = peer.admin().await;
    let started = Instant::now();
    assert!(matches!(
        admin
            .abort_transaction_timeout(spec(), Duration::from_millis(80))
            .await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(peer.requests(WRITE_TXN_MARKERS).len() > 1);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn canceled_abort_and_stalled_deadline_leave_unrelated_metadata_usable() {
    let peer = Peer::start(Some((1, 2)), None).await;
    {
        let mut state = peer.state.lock();
        state.marker_responses.push_back(response(&[good(0)]));
        state.hold_markers = true;
    }
    let mut admin = peer.admin().await;
    let mut operation = Box::pin(admin.abort_transaction(spec()));
    tokio::select! {
        ()=peer.seen.notified()=>{},
        result=&mut operation=>panic!("stalled public operation completed: {result:?}"),
        ()=tokio::time::sleep(BUDGET)=>panic!("marker was never sent"),
    }
    drop(operation);
    peer.state.lock().hold_markers = false;
    peer.release.notify_one();
    assert_eq!(admin.describe_topics(["t"]).await.unwrap().len(), 1);
    admin.close().await.unwrap();

    let peer = Peer::start(Some((1, 2)), None).await;
    {
        let mut state = peer.state.lock();
        state.marker_responses.push_back(response(&[good(0)]));
        state.hold_markers = true;
    }
    let mut admin = peer.admin().await;
    assert!(matches!(
        admin
            .abort_transaction_timeout(spec(), Duration::from_millis(40))
            .await,
        Err(Error::Timeout)
    ));
    assert_eq!(peer.requests(WRITE_TXN_MARKERS).len(), 1);
    peer.release.notify_one();
    assert_eq!(admin.describe_topics(["t"]).await.unwrap().len(), 1);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn truncated_or_trailing_marker_response_is_a_protocol_error() {
    let valid = response(&[good(0)]);
    for wire in [
        valid[..valid.len() - 1].to_vec(),
        [valid.as_slice(), &[0]].concat(),
        vec![0xff, 0xff, 0xff, 0xff, 7],
    ] {
        let peer = Peer::start(Some((1, 2)), None).await;
        peer.state.lock().marker_responses.push_back(wire);
        let mut admin = peer.admin().await;
        assert!(matches!(
            admin.abort_transaction(spec()).await,
            Err(Error::Protocol(_))
        ));
        admin.close().await.unwrap();
    }
}

mod codecs {
    use super::*;
    use partitionline::protocol::txn::{
        decode_write_txn_markers_request,
        decode_write_txn_markers_request_with_transaction_versions,
        decode_write_txn_markers_response, encode_write_txn_markers_request,
        encode_write_txn_markers_request_with_transaction_versions, WritableTxnMarkerWithVersion,
    };

    #[test]
    fn signed_transaction_version_and_legacy_projection_are_explicit() {
        for transaction_version in [i8::MIN, -1, 0, 1, 2, i8::MAX] {
            let values = [WritableTxnMarkerWithVersion::new(
                marker(),
                transaction_version,
            )];
            for version in [0, 1, 2] {
                let mut wire = BytesMut::new();
                encode_write_txn_markers_request_with_transaction_versions(
                    &mut wire, version, &values,
                )
                .unwrap();
                let mut cursor = wire.as_ref();
                let actual = decode_write_txn_markers_request_with_transaction_versions(
                    &mut cursor,
                    version,
                )
                .unwrap();
                assert!(cursor.is_empty());
                assert_eq!(
                    actual[0].transaction_version,
                    if version == 2 { transaction_version } else { 0 }
                );
                if version == 2 {
                    assert!(decode_write_txn_markers_request(&mut wire.as_ref(), version).is_err());
                } else {
                    assert_eq!(
                        decode_write_txn_markers_request(&mut wire.as_ref(), version).unwrap(),
                        vec![marker()]
                    );
                }
            }
        }
        let mut legacy = BytesMut::new();
        encode_write_txn_markers_request(&mut legacy, 2, &[marker()]).unwrap();
        let mut explicit = BytesMut::new();
        encode_write_txn_markers_request_with_transaction_versions(
            &mut explicit,
            2,
            &[WritableTxnMarkerWithVersion::new(marker(), 0)],
        )
        .unwrap();
        assert_eq!(legacy, explicit);
    }

    #[test]
    fn nested_counts_and_every_truncated_prefix_fail_with_bounded_input() {
        let mut wire = BytesMut::new();
        encode_write_txn_markers_request_with_transaction_versions(
            &mut wire,
            2,
            &[WritableTxnMarkerWithVersion::new(marker(), 2)],
        )
        .unwrap();
        for end in 0..wire.len() {
            assert!(decode_write_txn_markers_request_with_transaction_versions(
                &mut &wire[..end],
                2
            )
            .is_err());
        }
        let hostile = [0xff, 0xff, 0xff, 0xff, 7];
        assert!(decode_write_txn_markers_request_with_transaction_versions(
            &mut hostile.as_slice(),
            2
        )
        .is_err());
        assert!(decode_write_txn_markers_response(&mut hostile.as_slice(), 2).is_err());
        for version in [-1, 3, i16::MAX] {
            let mut encoded = BytesMut::new();
            assert!(encode_write_txn_markers_request(&mut encoded, version, &[marker()]).is_err());
            assert!(encoded.is_empty());
        }
        let partition = WritableTxnMarkerPartitionResult {
            partition_index: 0,
            error_code: 0,
        };
        let topic = WritableTxnMarkerTopicResult {
            name: "t".into(),
            partitions: vec![partition],
        };
        let plural = [WritableTxnMarkerResult {
            producer_id: 1000,
            topics: vec![topic.clone(), topic],
        }];
        let wire = response(&plural);
        assert_eq!(
            decode_write_txn_markers_response(&mut wire.as_slice(), 2).unwrap(),
            plural
        );
    }
}

#[test]
fn pinned_schema_reference_frame_keeps_v2_field_after_coordinator_epoch() {
    use partitionline::protocol::header::{decode_request_header, decode_response_header};
    use partitionline::protocol::txn::{
        decode_write_txn_markers_request_with_transaction_versions,
        decode_write_txn_markers_response,
    };
    // Handwritten schema controls are labeled separately from future Apache
    // execution evidence. Neither these bytes nor their generator read Rust.
    let mut cursor =
        include_bytes!("fixtures/write-txn-markers-v2/schema-tv-0.request.bin").as_slice();
    let header = decode_request_header(&mut cursor).unwrap();
    assert_eq!(
        (header.api_key, header.api_version, header.correlation_id),
        (27, 2, 7)
    );
    let marker_data =
        decode_write_txn_markers_request_with_transaction_versions(&mut cursor, 2).unwrap();
    assert_eq!(marker_data[0].marker, marker());
    assert_eq!(marker_data[0].transaction_version, 0);
    assert!(cursor.is_empty());
    let mut cursor =
        include_bytes!("fixtures/write-txn-markers-v2/schema-tv-0.response.bin").as_slice();
    assert_eq!(
        decode_response_header(&mut cursor, 27, 2)
            .unwrap()
            .correlation_id,
        7
    );
    assert_eq!(
        decode_write_txn_markers_response(&mut cursor, 2).unwrap(),
        vec![good(0)]
    );
    assert!(cursor.is_empty());
}

/// Opt-in actual SDK socket lane. It is a bounded scripted peer, never a
/// production broker. Inputs and full requests are retained by the outer
/// independent runner; ordinary test runs return without opening a listener.
#[tokio::test]
async fn serve_public_admin_probe() {
    let Ok(directory) = std::env::var("PARTITIONLINE_CAPABILITY_PEER_DIR") else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let mode = std::env::var("PARTITIONLINE_CAPABILITY_PEER_MODE").unwrap();
    let version: i16 = std::env::var("PARTITIONLINE_CAPABILITY_PEER_VERSION")
        .unwrap()
        .parse()
        .unwrap();
    assert!(matches!(mode.as_str(), "abort" | "share"));
    assert!((0..=2).contains(&version));
    let responses = directory.join("responses.bin");
    let metadata = tokio::fs::metadata(&responses).await.unwrap();
    assert!(metadata.is_file() && metadata.len() <= 65_536);
    let bytes = tokio::fs::read(&responses).await.unwrap();
    let mut cursor = bytes.as_slice();
    let mut declared = Vec::new();
    while !cursor.is_empty() {
        assert!(declared.len() < 8 && cursor.len() >= 4);
        let length = usize::try_from(u32::from_be_bytes(cursor[..4].try_into().unwrap())).unwrap();
        cursor = &cursor[4..];
        assert!(length <= 65_536 && cursor.len() >= length);
        declared.push(cursor[..length].to_vec());
        cursor = &cursor[length..];
    }
    assert!(!declared.is_empty());
    let peer = if mode == "abort" {
        Peer::start(Some((version, version)), None).await
    } else {
        Peer::start(None, Some((version, version))).await
    };
    {
        let mut state = peer.state.lock();
        if mode == "abort" {
            state.marker_responses.extend(declared)
        } else {
            state.share_responses.extend(declared)
        }
    }
    tokio::fs::write(directory.join("ready"), &peer.bootstrap)
        .await
        .unwrap();
    let started = Instant::now();
    while !tokio::fs::try_exists(directory.join("stop")).await.unwrap() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "finite actual SDK peer deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let history = {
        let state = peer.state.lock();
        let hex = |data: &[u8]| {
            data.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let mut history = String::new();
        for row in &state.observed {
            use std::fmt::Write;
            writeln!(
                history,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                row.node,
                row.key,
                row.version,
                row.correlation,
                hex(&row.body),
                hex(&row.request_payload),
                row.response_payload
                    .as_ref()
                    .map_or_else(String::new, |data| hex(data)),
                row.response_written,
            )
            .unwrap();
        }
        history
    };
    tokio::fs::write(directory.join("actual-requests.tsv"), history)
        .await
        .unwrap();
}
