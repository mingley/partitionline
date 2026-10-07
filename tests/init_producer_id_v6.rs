//! Complete InitProducerId fields and ordinary producer behavior at v0–v6.
mod common;
use bytes::BytesMut;
use partitionline::protocol::{
    api_keys::{END_TXN, INIT_PRODUCER_ID},
    idem::*,
};
use partitionline::{error, Error, ProduceRecord, Producer, ProducerConfig};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/init-producer-id-v6")
}
#[expect(
    clippy::disallowed_methods,
    clippy::expect_used,
    reason = "finite synchronous fixture I/O outside async tests"
)]
fn read(p: &Path) -> Vec<u8> {
    std::fs::read(p).expect("read pinned Apache fixture")
}
#[expect(
    clippy::disallowed_methods,
    clippy::expect_used,
    reason = "finite synchronous conformance output outside async tests"
)]
fn emit(release: &str, name: &str, bytes: &[u8]) {
    if let Some(out) = std::env::var_os("PL_INIT_PRODUCER_ID_V6_OUTPUT") {
        let dir = PathBuf::from(out).join(release);
        std::fs::create_dir_all(&dir).expect("create conformance output");
        std::fs::write(dir.join(name), bytes).expect("write conformance output");
    }
}
#[test]
fn all_three_apache_serializers_preserve_flags_and_both_identities() {
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        for v in 0..=6 {
            let cells: &[&str] = if v < 6 {
                &["ordinary"]
            } else {
                &[
                    "ordinary", "flags00", "flags01", "flags10", "flags11", "error", "tagged",
                ]
            };
            for cell in cells {
                let prefix = format!("v{v}-{cell}");
                let dir = fixtures().join(release);
                let req = read(&dir.join(format!("{prefix}-request.bin")));
                let resp = read(&dir.join(format!("{prefix}-response.bin")));
                let request = decode_init_producer_id_request_data(&mut &req[..], v).unwrap();
                let response = decode_init_producer_id_response_data(&mut &resp[..], v).unwrap();
                let ordinary = *cell == "ordinary";
                assert_eq!(
                    request.transactional_id.as_deref(),
                    if ordinary { None } else { Some("tid") }
                );
                assert_eq!(request.transaction_timeout_ms, 45000);
                assert_eq!(
                    (request.producer_id, request.producer_epoch),
                    if ordinary { (-1, -1) } else { (1234, 7) }
                );
                assert_eq!(request.enable_2pc, cell.starts_with("flags1"));
                assert_eq!(
                    request.keep_prepared_txn,
                    cell.starts_with("flags") && cell.ends_with('1')
                );
                assert_eq!((response.producer_id, response.producer_epoch), (1234, 7));
                assert_eq!(
                    (
                        response.ongoing_txn_producer_id,
                        response.ongoing_txn_producer_epoch
                    ),
                    if ordinary { (-1, -1) } else { (9999, 11) }
                );
                assert_eq!(response.throttle_time_ms, 42);
                assert_eq!(response.error_code, if *cell == "error" { 90 } else { 0 });
                assert_eq!(
                    decode_init_producer_id_response(&mut &resp[..], v).unwrap(),
                    (response.error_code, 1234, 7, 42)
                );
                let mut encoded_req = BytesMut::new();
                let mut encoded_resp = BytesMut::new();
                encode_init_producer_id_request_data(&mut encoded_req, v, &request).unwrap();
                encode_init_producer_id_response_data(&mut encoded_resp, v, &response).unwrap();
                if *cell != "tagged" {
                    assert_eq!(&encoded_req[..], req);
                    assert_eq!(&encoded_resp[..], resp);
                }
                emit(release, &format!("{prefix}-request.bin"), &encoded_req);
                emit(release, &format!("{prefix}-response.bin"), &encoded_resp);
            }
        }
    }
}
#[test]
fn complete_input_truncation_and_tag_bounds_fail_closed() {
    for cell in ["ordinary", "flags11", "tagged"] {
        let dir = fixtures().join("4.3.1");
        let req = read(&dir.join(format!("v6-{cell}-request.bin")));
        let resp = read(&dir.join(format!("v6-{cell}-response.bin")));
        for n in 0..req.len() {
            assert!(decode_init_producer_id_request_data(&mut &req[..n], 6).is_err());
        }
        for n in 0..resp.len() {
            assert!(decode_init_producer_id_response_data(&mut &resp[..n], 6).is_err());
        }
        let mut trailing = req.clone();
        trailing.push(0);
        assert!(decode_init_producer_id_request_data(&mut &trailing[..], 6).is_err());
        let mut trailing = resp.clone();
        trailing.push(0);
        assert!(decode_init_producer_id_response_data(&mut &trailing[..], 6).is_err());
    }
    for tail in [&[0xff, 0xff, 0xff, 0xff, 0x07][..], &[1, 9, 127][..]] {
        let mut req = read(&fixtures().join("4.3.1/v6-ordinary-request.bin"));
        assert_eq!(req.pop(), Some(0));
        req.extend_from_slice(tail);
        let mut resp = read(&fixtures().join("4.3.1/v6-ordinary-response.bin"));
        assert_eq!(resp.pop(), Some(0));
        resp.extend_from_slice(tail);
        assert!(decode_init_producer_id_request_data(&mut &req[..], 6).is_err());
        assert!(decode_init_producer_id_response_data(&mut &resp[..], 6).is_err());
    }
}
#[test]
fn legacy_wrappers_supply_false_flags_and_ongoing_sentinels() {
    for v in 0..=6 {
        let mut req = BytesMut::new();
        encode_init_producer_id_request(&mut req, v, None, 45000, -1, -1).unwrap();
        let decoded = decode_init_producer_id_request_data(&mut &req[..], v).unwrap();
        assert!(!decoded.enable_2pc && !decoded.keep_prepared_txn);
        let mut resp = BytesMut::new();
        encode_init_producer_id_response(&mut resp, v, 90, 1234, 7).unwrap();
        let decoded = decode_init_producer_id_response_data(&mut &resp[..], v).unwrap();
        assert_eq!(
            (
                decoded.error_code,
                decoded.producer_id,
                decoded.producer_epoch
            ),
            (90, 1234, 7)
        );
        assert_eq!(
            (
                decoded.ongoing_txn_producer_id,
                decoded.ongoing_txn_producer_epoch
            ),
            (-1, -1)
        );
    }
}
fn config(mock: &common::Mock, transactional: bool) -> ProducerConfig {
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]).idempotent(true);
    if transactional {
        cfg.transactional_id = Some("init-v6".into());
    }
    cfg.linger = Duration::ZERO;
    cfg.request_timeout = Duration::from_secs(2);
    cfg.retry_backoff = Duration::from_millis(5);
    cfg.retry_backoff_max = cfg.retry_backoff;
    cfg
}
#[tokio::test]
async fn true_flags_on_old_peers_return_unsupported_before_any_application_frame() {
    let mock = common::Mock::start().await;
    for v in 0..6 {
        for flags in [(true, false), (false, true), (true, true)] {
            let mut conn = partitionline::net::BrokerConn::connect(
                &mock.addr,
                "flag-guard",
                Duration::from_secs(1),
            )
            .await
            .unwrap();
            let data = InitProducerIdRequestData {
                enable_2pc: flags.0,
                keep_prepared_txn: flags.1,
                ..Default::default()
            };
            let result = conn
                .roundtrip(
                    INIT_PRODUCER_ID,
                    v,
                    |buf| encode_init_producer_id_request_data(buf, v, &data),
                    Duration::from_secs(1),
                )
                .await;
            assert!(matches!(result, Err(Error::Unsupported(_))));
        }
    }
    assert!(mock.init_producer_id_nodes().is_empty());
}
#[tokio::test]
async fn ordinary_init_send_commit_abort_match_every_older_negotiated_version() {
    for version in 0..=6 {
        for transactional in [false, true] {
            let mock = common::Mock::start().await;
            mock.set_api_max(INIT_PRODUCER_ID, version);
            if version == 6 {
                mock.set_init_producer_id_ongoing(9999, 11);
            }
            let producer = Producer::new(config(&mock, transactional)).await.unwrap();
            assert_eq!(mock.last_init_producer_id_version(), Some(version));
            assert_eq!(mock.last_init_producer_id_flags(), Some((false, false)));
            assert_eq!(producer.__test_producer_id(), 1000);
            assert_eq!(producer.__test_producer_epoch(), 0);
            if transactional {
                producer.init_transactions().await.unwrap();
                producer.begin_transaction().await.unwrap();
            }
            let _metadata = producer
                .send(ProduceRecord::to("t").partition(0).value(&b"ordinary"[..]))
                .await
                .unwrap();
            if transactional {
                producer.commit_transaction().await.unwrap();
                producer.begin_transaction().await.unwrap();
                let _metadata = producer
                    .send(ProduceRecord::to("t").partition(0).value(&b"abort"[..]))
                    .await
                    .unwrap();
                producer.abort_transaction().await.unwrap();
            }
            assert_ne!(producer.__test_producer_id(), 9999);
            producer.close().await.unwrap();
        }
    }
}
#[tokio::test]
async fn ongoing_identity_never_masks_a_terminal_initialization_error() {
    for transactional in [false, true] {
        let mock = common::Mock::start().await;
        mock.set_api_max(INIT_PRODUCER_ID, 6);
        mock.set_init_producer_id_ongoing(9999, 11);
        mock.fail_init_producer_id_once(90);
        let result = Producer::new(config(&mock, transactional)).await;
        assert!(matches!(result, Err(Error::Broker { code: 90, .. })));
        assert_eq!(mock.init_producer_id_nodes().len(), 1);
    }
}
#[tokio::test]
async fn moved_coordinator_uses_its_own_lower_negotiation_on_initialization() {
    let mock = common::Mock::start_two_node().await;
    mock.set_api_max(INIT_PRODUCER_ID, 6);
    mock.set_node_api_max(2, INIT_PRODUCER_ID, 5);
    mock.move_txn_coordinator();
    mock.stale_txn_find_once();
    let producer = Producer::new(config(&mock, true)).await.unwrap();
    assert_eq!(mock.init_producer_id_nodes(), vec![1, 2]);
    assert_eq!(mock.last_init_producer_id_version(), Some(5));
    assert_eq!(mock.last_init_producer_id_flags(), Some((false, false)));
    producer.close().await.unwrap();
}
#[tokio::test]
async fn missing_or_nonoverlapping_initialization_api_sends_no_request() {
    for missing in [true, false] {
        let mock = common::Mock::start().await;
        if missing {
            mock.hide_api(INIT_PRODUCER_ID);
        } else {
            mock.set_api_range(INIT_PRODUCER_ID, 7, 7);
        }
        let result = Producer::new(config(&mock, false)).await;
        assert!(matches!(result, Err(Error::Unsupported(_))));
        assert!(mock.init_producer_id_nodes().is_empty());
    }
}
#[tokio::test]
async fn classic_epoch_recovery_renegotiates_a_moved_coordinator_without_using_ongoing_identity() {
    let mock = common::Mock::start_two_node().await;
    mock.set_api_max(INIT_PRODUCER_ID, 6);
    mock.set_api_max(END_TXN, 4);
    mock.set_init_producer_id_ongoing(9999, 11);
    let producer = Producer::new(config(&mock, true)).await.unwrap();
    producer.begin_transaction().await.unwrap();
    mock.set_produce_error_times(error::UNKNOWN_PRODUCER_ID, 1);
    assert_eq!(
        producer
            .send(ProduceRecord::to("t").partition(0).value(&b"recovery"[..]))
            .await
            .unwrap_err()
            .broker_code(),
        Some(error::UNKNOWN_PRODUCER_ID)
    );
    mock.set_node_api_max(2, INIT_PRODUCER_ID, 5);
    mock.move_txn_coordinator();
    producer.abort_transaction().await.unwrap();
    assert_eq!(mock.last_init_producer_id_version(), Some(5));
    assert_eq!(mock.last_init_producer_id_producer_id(), Some(1000));
    assert_eq!(producer.__test_producer_id(), 1000);
    assert_eq!(producer.__test_producer_epoch(), 1);
    producer.begin_transaction().await.unwrap();
    let _metadata = producer
        .send(
            ProduceRecord::to("t")
                .partition(0)
                .value(&b"after-recovery"[..]),
        )
        .await
        .unwrap();
    producer.commit_transaction().await.unwrap();
    producer.close().await.unwrap();
}
#[tokio::test]
async fn epoch_recovery_loading_and_rediscovery_do_not_renew_the_deadline() {
    let mock = common::Mock::start().await;
    mock.set_api_max(INIT_PRODUCER_ID, 6);
    mock.set_api_max(END_TXN, 4);
    let mut cfg = config(&mock, true);
    cfg.request_timeout = Duration::from_millis(100);
    let producer = Producer::new(cfg).await.unwrap();
    producer.begin_transaction().await.unwrap();
    mock.set_produce_error_times(error::UNKNOWN_PRODUCER_ID, 1);
    assert!(producer
        .send(ProduceRecord::to("t").partition(0).value(&b"recovery"[..]))
        .await
        .is_err());
    for _ in 0..64 {
        mock.fail_init_producer_id_once(14);
    }
    mock.set_init_producer_id_delay(Duration::from_millis(55));
    let result =
        tokio::time::timeout(Duration::from_millis(180), producer.abort_transaction()).await;
    assert!(matches!(result, Ok(Err(Error::Timeout))));
    producer.close().await.unwrap();
}
#[tokio::test]
async fn nontransactional_epoch_exhaustion_rotates_the_regular_identity_at_v6() {
    let mock = common::Mock::start().await;
    mock.set_api_max(INIT_PRODUCER_ID, 6);
    mock.set_init_producer_id_ongoing(9999, 11);
    let producer = Producer::new(config(&mock, false)).await.unwrap();
    let old = producer.__test_producer_id();
    producer.__test_set_producer_epoch(i16::MAX);
    mock.set_produce_error_times(error::UNKNOWN_PRODUCER_ID, 1);
    let _metadata = producer
        .send(ProduceRecord::to("t").partition(0).value(&b"renew"[..]))
        .await
        .unwrap();
    assert_ne!(producer.__test_producer_id(), old);
    assert_ne!(producer.__test_producer_id(), 9999);
    assert_eq!(mock.last_init_producer_id_version(), Some(6));
    assert_eq!(producer.__test_producer_epoch(), 0);
    producer.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires an explicitly isolated Kafka broker and independent Java readback"]
async fn native_ordinary_transaction_history() {
    let bootstrap = std::env::var("PL_INIT_V6_BOOTSTRAP").expect("isolated bootstrap");
    let topic = std::env::var("PL_INIT_V6_TOPIC").expect("fresh topic");
    let mut cfg =
        ProducerConfig::bootstrap([bootstrap]).transactional_id("partitionline-init-v6-native");
    cfg.linger = Duration::ZERO;
    cfg.request_timeout = Duration::from_secs(30);
    cfg.retry_backoff = Duration::from_millis(50);
    let producer = Producer::new(cfg).await.unwrap();
    producer.init_transactions().await.unwrap();
    producer.begin_transaction().await.unwrap();
    for value in ["R0", "R1"] {
        let _metadata = producer
            .send(
                ProduceRecord::to(topic.clone())
                    .partition(0)
                    .value(value.as_bytes()),
            )
            .await
            .unwrap();
    }
    producer.commit_transaction().await.unwrap();
    producer.begin_transaction().await.unwrap();
    let _metadata = producer
        .send(
            ProduceRecord::to(topic.clone())
                .partition(0)
                .value(&b"RA"[..]),
        )
        .await
        .unwrap();
    producer.abort_transaction().await.unwrap();
    producer.begin_transaction().await.unwrap();
    let _metadata = producer
        .send(ProduceRecord::to(topic).partition(0).value(&b"R2"[..]))
        .await
        .unwrap();
    producer.commit_transaction().await.unwrap();
    producer.close().await.unwrap();
}
