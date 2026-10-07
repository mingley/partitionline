//! API67 fixed-field codecs and actual controller raw dispatch.
use bytes::{Buf, Bytes, BytesMut};
use partitionline::protocol::admin::{
    decode_allocate_producer_ids_request, decode_allocate_producer_ids_response,
    encode_allocate_producer_ids_request, encode_allocate_producer_ids_response,
    AllocateProducerIdsRequest, AllocateProducerIdsResponse,
};

#[test]
fn fixed_fields_and_every_truncated_prefix_are_checked() {
    let mut request = BytesMut::new();
    encode_allocate_producer_ids_request(&mut request, i32::MIN, i64::MAX).unwrap();
    assert_eq!(request.len(), 13);
    for size in 0..request.len() {
        assert!(decode_allocate_producer_ids_request(&mut &request[..size]).is_err());
    }
    assert_eq!(
        decode_allocate_producer_ids_request(&mut request.freeze()).unwrap(),
        (i32::MIN, i64::MAX)
    );
    let value = AllocateProducerIdsResponse {
        throttle_time_ms: 123,
        error_code: 41,
        producer_id_start: i64::MAX,
        producer_id_len: i32::MIN,
    };
    let mut response = BytesMut::new();
    encode_allocate_producer_ids_response(&mut response, &value).unwrap();
    assert_eq!(response.len(), 19);
    for size in 0..response.len() {
        assert!(decode_allocate_producer_ids_response(&mut &response[..size]).is_err());
    }
    assert_eq!(
        decode_allocate_producer_ids_response(&mut response.freeze()).unwrap(),
        value
    );
}

#[test]
fn unknown_tag_payload_is_skipped_and_truncated_payload_fails() {
    let mut request = BytesMut::new();
    encode_allocate_producer_ids_request(&mut request, 2, 123).unwrap();
    request.truncate(12);
    request.extend_from_slice(&[1, 9, 3, 1, 2, 3]);
    let mut cursor = request.clone().freeze();
    assert_eq!(
        decode_allocate_producer_ids_request(&mut cursor).unwrap(),
        (2, 123)
    );
    assert!(!cursor.has_remaining());
    request.truncate(request.len() - 1);
    assert!(decode_allocate_producer_ids_request(&mut request.freeze()).is_err());
}

#[test]
fn error_factory_does_not_copy_request_identity_or_id_range() {
    for code in [0, 41, 77, 31, -1] {
        let response = AllocateProducerIdsRequest::error_response(code);
        assert_eq!(response, AllocateProducerIdsResponse::new(code, 0, 0));
        assert_eq!(
            response.error_counts(),
            std::collections::HashMap::from([(code, 1)])
        );
    }
}

#[tokio::test]
#[ignore = "requires guarded authentic Apache SDK fixtures and reverse parsing"]
async fn actual_sdk_allocate_producer_ids_bodies() {
    let fixtures = std::path::PathBuf::from(std::env::var_os("ALLOCATE_IDS_FIXTURES").unwrap());
    let reverse = std::path::PathBuf::from(std::env::var_os("ALLOCATE_IDS_REVERSE").unwrap());
    tokio::fs::create_dir(&reverse).await.unwrap();
    let mut count = 0;
    let opaque = tokio::fs::read(fixtures.join("opaque-request.tagged"))
        .await
        .unwrap();
    let mut cursor = Bytes::from(opaque);
    assert_eq!(
        decode_allocate_producer_ids_request(&mut cursor).unwrap(),
        (2, 123)
    );
    assert!(!cursor.has_remaining());
    let mut normalized = BytesMut::new();
    encode_allocate_producer_ids_request(&mut normalized, 2, 123).unwrap();
    tokio::fs::write(reverse.join("opaque-request.normalized"), normalized)
        .await
        .unwrap();
    let mut files = tokio::fs::read_dir(fixtures).await.unwrap();
    while let Some(item) = files.next_entry().await.unwrap() {
        let path = item.path();
        if path.extension().and_then(|v| v.to_str()) != Some("bin") {
            continue;
        }
        let name = path.file_name().unwrap().to_str().unwrap();
        let index: usize = name.split('-').nth(1).unwrap().parse().unwrap();
        let data = tokio::fs::read(&path).await.unwrap();
        assert!(data.len() <= 65536);
        for size in 0..data.len() {
            if name.ends_with("request.bin") {
                assert!(decode_allocate_producer_ids_request(&mut &data[..size]).is_err());
            } else {
                assert!(decode_allocate_producer_ids_response(&mut &data[..size]).is_err());
            }
        }
        let mut cursor = Bytes::from(data.clone());
        let mut encoded = BytesMut::new();
        if name.ends_with("request.bin") {
            let (id, epoch) = decode_allocate_producer_ids_request(&mut cursor).unwrap();
            assert_eq!(id, [0, 2, i32::MIN, i32::MAX][index / 4]);
            assert_eq!(epoch, [0, 123, i64::MIN, i64::MAX][index % 4]);
            encode_allocate_producer_ids_request(&mut encoded, id, epoch).unwrap();
        } else {
            let value = decode_allocate_producer_ids_response(&mut cursor).unwrap();
            let expected = if index == 16 {
                AllocateProducerIdsResponse {
                    throttle_time_ms: 123,
                    error_code: 0,
                    producer_id_start: 345,
                    producer_id_len: 234,
                }
            } else if index <= 20 {
                AllocateProducerIdsResponse {
                    throttle_time_ms: 17,
                    error_code: [41, 77, 31, -1][index - 17],
                    producer_id_start: 0,
                    producer_id_len: 0,
                }
            } else if index == 25 {
                AllocateProducerIdsResponse::new(0, 0, 0)
            } else {
                AllocateProducerIdsResponse {
                    throttle_time_ms: i32::MAX,
                    error_code: 0,
                    producer_id_start: [i64::MIN, i64::MAX][(index - 21) / 2],
                    producer_id_len: [i32::MIN, i32::MAX][(index - 21) % 2],
                }
            };
            assert_eq!(value, expected);
            encode_allocate_producer_ids_response(&mut encoded, &value).unwrap();
        }
        assert!(!cursor.has_remaining());
        assert_eq!(encoded.as_ref(), data.as_slice());
        tokio::fs::write(reverse.join(name), encoded).await.unwrap();
        count += 1;
    }
    assert_eq!(count, 26);
}

#[tokio::test]
#[ignore = "requires an owned current Kafka controller and independent SDK parsing"]
async fn native_allocate_producer_ids_raw_history() {
    use partitionline::net::{BrokerConn, Deadline};
    use partitionline::protocol::api::{decode_api_versions_response, encode_api_versions_request};
    use std::time::Duration;
    let address = std::env::var("ALLOCATE_IDS_CONTROLLER").unwrap();
    let epoch: i64 = std::env::var("ALLOCATE_IDS_BROKER_EPOCH")
        .unwrap()
        .parse()
        .unwrap();
    let directory = std::path::PathBuf::from(std::env::var_os("ALLOCATE_IDS_DIRECTORY").unwrap());
    tokio::fs::create_dir(&directory).await.unwrap();
    let deadline = Deadline::from_timeout(Duration::from_secs(5));
    let mut conn = deadline
        .run(BrokerConn::connect(
            &address,
            "allocate-ids-rust",
            Duration::from_secs(2),
        ))
        .await
        .unwrap();
    let body = conn
        .roundtrip_deadline(
            18,
            0,
            |buf| encode_api_versions_request(buf, 0, "partitionline", "0.1.0"),
            deadline,
        )
        .await
        .unwrap();
    assert!(body.len() <= 65536);
    let versions = decode_api_versions_response(&mut body.clone(), 0).unwrap();
    assert_eq!(versions.error_code, 0);
    assert!(versions
        .api_keys
        .iter()
        .any(|v| v.api_key == 67 && v.min_version <= 0 && v.max_version >= 0));
    let mut end = -1;
    for i in 0..5 {
        let id = if i == 3 { 99 } else { 1 };
        let requested_epoch = if i == 4 {
            -1
        } else if i == 3 {
            0
        } else {
            epoch
        };
        let mut request = BytesMut::new();
        encode_allocate_producer_ids_request(&mut request, id, requested_epoch).unwrap();
        tokio::fs::write(directory.join(format!("live-{i}-request.bin")), request)
            .await
            .unwrap();
        let response = conn
            .roundtrip_deadline(
                67,
                0,
                |buf| encode_allocate_producer_ids_request(buf, id, requested_epoch),
                deadline,
            )
            .await
            .unwrap();
        assert!(response.len() <= 65536);
        tokio::fs::write(directory.join(format!("live-{i}-response.bin")), &response)
            .await
            .unwrap();
        let mut cursor = response;
        let value = decode_allocate_producer_ids_response(&mut cursor).unwrap();
        assert!(!cursor.has_remaining());
        if i < 3 {
            assert_eq!(value.error_code, 0);
            assert_eq!(value.producer_id_len, 1000);
            assert!(value.producer_id_start >= end);
            end = value.producer_id_start + i64::from(value.producer_id_len);
        } else {
            assert_eq!(value.error_code, 77);
            assert_eq!((value.producer_id_start, value.producer_id_len), (0, 0));
        }
    }
    conn.close();
    drop(conn);
    let mut config =
        partitionline::AdminConfig::bootstrap(vec![std::env::var("ALLOCATE_IDS_BROKER").unwrap()]);
    config.request_timeout = Duration::from_secs(3);
    let mut admin = partitionline::Admin::new(config).await.unwrap();
    assert!(matches!(
        admin.allocate_producer_ids(1, epoch).await,
        Err(partitionline::Error::Unsupported(_))
    ));
    admin.close().await.unwrap();
    tokio::fs::write(directory.join("receipt.json"), b"{\"actual_controller_requests\":5,\"successful_blocks\":3,\"stale_epoch_errors\":2,\"ordinary_broker_Admin_policy\":\"Unsupported\",\"socket_closed\":true}\n").await.unwrap();
}
