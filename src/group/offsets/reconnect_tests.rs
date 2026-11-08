//! Offset retries must inspect each response and retain their wire identity.
use super::*;
use crate::protocol::api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse};
use crate::protocol::api_keys::{
    API_VERSIONS, FIND_COORDINATOR, OFFSET_COMMIT, OFFSET_FETCH, SASL_AUTHENTICATE, SASL_HANDSHAKE,
};
use crate::protocol::header::{decode_request_header, encode_response_header, RequestHeader};
use crate::protocol::sasl::{
    decode_sasl_authenticate_request, decode_sasl_handshake_request,
    encode_sasl_authenticate_response, encode_sasl_handshake_response, parse_plain_auth_bytes,
};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn read_request(socket: &mut TcpStream) -> (RequestHeader, Bytes) {
    let size = socket.read_i32().await.unwrap();
    assert!((8..=64 * 1024).contains(&size));
    let mut body = vec![0; usize::try_from(size).unwrap()];
    let _read = socket.read_exact(&mut body).await.unwrap();
    let mut body = Bytes::from(body);
    let header = decode_request_header(&mut body).unwrap();
    (header, body)
}

async fn respond(socket: &mut TcpStream, header: &RequestHeader, body: &[u8]) {
    let mut frame = BytesMut::new();
    encode_response_header(
        &mut frame,
        header.api_key,
        header.api_version,
        header.correlation_id,
    )
    .unwrap();
    frame.extend_from_slice(body);
    socket
        .write_i32(i32::try_from(frame.len()).unwrap())
        .await
        .unwrap();
    socket.write_all(&frame).await.unwrap();
}

async fn authenticate(listener: &TcpListener, api: i16, version: i16) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    let (header, _) = read_request(&mut socket).await;
    assert_eq!((header.api_key, header.api_version), (API_VERSIONS, 4));
    let mut body = BytesMut::new();
    encode_api_versions_response(
        &mut body,
        4,
        &ApiVersionsResponse {
            api_keys: [
                (FIND_COORDINATOR, 1),
                (api, version),
                (SASL_HANDSHAKE, 1),
                (SASL_AUTHENTICATE, 2),
            ]
            .into_iter()
            .map(|(api_key, version)| ApiVersion {
                api_key,
                min_version: version,
                max_version: version,
            })
            .collect(),
            ..ApiVersionsResponse::default()
        },
    )
    .unwrap();
    respond(&mut socket, &header, &body).await;

    let (header, mut request) = read_request(&mut socket).await;
    assert_eq!((header.api_key, header.api_version), (SASL_HANDSHAKE, 1));
    assert_eq!(
        decode_sasl_handshake_request(&mut request, 1).unwrap(),
        "PLAIN"
    );
    body.clear();
    encode_sasl_handshake_response(&mut body, 1, 0, &["PLAIN"]).unwrap();
    respond(&mut socket, &header, &body).await;

    let (header, mut request) = read_request(&mut socket).await;
    assert_eq!((header.api_key, header.api_version), (SASL_AUTHENTICATE, 2));
    let auth = decode_sasl_authenticate_request(&mut request, 2).unwrap();
    assert_eq!(
        parse_plain_auth_bytes(&auth),
        Some(("user".into(), "pass".into()))
    );
    body.clear();
    encode_sasl_authenticate_response(&mut body, 2, 0, None, &[], 0).unwrap();
    respond(&mut socket, &header, &body).await;
    socket
}

fn identity(version: i16) -> OffsetTopicIdentity {
    if version == 10 {
        OffsetTopicIdentity::Id([7; 16])
    } else {
        OffsetTopicIdentity::Name("t".into())
    }
}

fn request(api: i16, version: i16) -> Vec<u8> {
    if api == OFFSET_COMMIT {
        encode_offset_commit_request_data(
            &OffsetCommitRequestData {
                group_id: "retry".into(),
                generation_id_or_member_epoch: 3,
                member_id: "member".into(),
                group_instance_id: Some("static".into()),
                topics: vec![OffsetCommitTopicData {
                    identity: identity(version),
                    partitions: vec![OffsetCommitPartitionData {
                        partition_index: 0,
                        committed_offset: 42,
                        committed_leader_epoch: 6,
                        committed_metadata: Some("cursor".into()),
                    }],
                }],
            },
            version,
            OffsetLimits::default(),
        )
        .unwrap()
    } else {
        encode_offset_fetch_request_data(
            &OffsetFetchRequestData {
                require_stable: true,
                groups: vec![OffsetFetchGroupData {
                    group_id: "retry".into(),
                    member_id: Some("member".into()),
                    member_epoch: 3,
                    topics: Some(vec![OffsetFetchTopicData {
                        identity: identity(version),
                        partition_indexes: vec![0],
                    }]),
                }],
            },
            version,
            OffsetLimits::default(),
        )
        .unwrap()
    }
}

fn response(api: i16, version: i16, code: i16) -> Vec<u8> {
    if api == OFFSET_COMMIT {
        encode_offset_commit_response_data(
            &OffsetCommitResponseData {
                throttle_time_ms: 0,
                topics: vec![OffsetCommitTopicResponseData {
                    identity: identity(version),
                    partitions: vec![OffsetCommitPartitionResponseData {
                        partition_index: 0,
                        error_code: code,
                    }],
                }],
            },
            version,
            OffsetLimits::default(),
        )
        .unwrap()
    } else {
        encode_offset_fetch_response_data(
            &OffsetFetchResponseData {
                throttle_time_ms: 0,
                groups: vec![OffsetFetchGroupResponseData {
                    group_id: "retry".into(),
                    topics: Vec::new(),
                    error_code: code,
                }],
            },
            version,
            OffsetLimits::default(),
        )
        .unwrap()
    }
}

async fn moved_after_load(api: i16, version: i16, destination_version: i16) {
    let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let new = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_addr = old.local_addr().unwrap().to_string();
    let new_addr = new.local_addr().unwrap();
    let old_peer = tokio::spawn(async move {
        let mut socket = authenticate(&old, api, version).await;
        for code in [
            crate::error::COORDINATOR_LOAD_IN_PROGRESS,
            crate::error::NOT_COORDINATOR,
        ] {
            let (header, body) = read_request(&mut socket).await;
            assert_eq!((header.api_key, header.api_version), (api, version));
            assert_eq!(body.as_ref(), request(api, version));
            respond(&mut socket, &header, &response(api, version, code)).await;
        }
        let (header, mut body) = read_request(&mut socket).await;
        assert_eq!((header.api_key, header.api_version), (FIND_COORDINATOR, 1));
        assert_eq!(
            decode_find_coordinator_request(&mut body, 1).unwrap(),
            ("retry".into(), COORDINATOR_GROUP)
        );
        let mut body = BytesMut::new();
        encode_find_coordinator_response(
            &mut body,
            1,
            2,
            "127.0.0.1",
            i32::from(new_addr.port()),
            "retry",
        )
        .unwrap();
        respond(&mut socket, &header, &body).await;
    });
    let new_peer = tokio::spawn(async move {
        let mut socket = authenticate(&new, api, destination_version).await;
        if version == 10 && destination_version < 10 {
            let mut byte = [0; 1];
            assert_eq!(
                socket.read(&mut byte).await.unwrap(),
                0,
                "UUID request must not be sent after downgrade"
            );
            return;
        }
        let (header, body) = read_request(&mut socket).await;
        assert_eq!((header.api_key, header.api_version), (api, version));
        assert_eq!(body.as_ref(), request(api, version), "authenticated destination must receive the same identity, membership and offset fields");
        respond(&mut socket, &header, &response(api, version, 0)).await;
    });
    let mut cfg = crate::ConsumerConfig::bootstrap([old_addr.clone()]);
    cfg.sasl_plain = Some(("user".into(), "pass".into()));
    cfg.request_timeout = Duration::from_secs(2);
    let (mut coord, _, _) = super::super::open_coord_with_find_version(&cfg, &old_addr)
        .await
        .unwrap();
    let mut prior = HashMap::new();
    let result = if version == 10 {
        coord_offsets_v10(
            &mut coord,
            &cfg,
            "retry",
            COORDINATOR_GROUP,
            api,
            |buf| {
                buf.extend_from_slice(&request(api, 10));
                Ok(())
            },
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await
    } else {
        coord_offsets_legacy(
            &mut coord,
            &cfg,
            "retry",
            COORDINATOR_GROUP,
            api,
            version,
            |version, buf| {
                buf.extend_from_slice(&request(api, version));
                Ok(())
            },
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(2),
        )
        .await
        .map(|(_, body)| body)
    };
    if version == 10 && destination_version < 10 {
        assert!(
            matches!(result, Err(Error::Unsupported(_))),
            "unexpected result: {result:?}"
        );
    } else {
        assert_eq!(result.unwrap().as_ref(), response(api, version, 0));
        assert_eq!(coord.addr(), new_addr.to_string());
    }
    coord.close();
    coord.release_if_closed().await;
    tokio::time::timeout(Duration::from_secs(2), old_peer)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), new_peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn offset_load_retry_rediscovers_after_second_coordinator_error() {
    for version in [9, 10] {
        for api in [OFFSET_COMMIT, OFFSET_FETCH] {
            moved_after_load(api, version, version).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn v10_load_retry_never_downgrades_uuid_requests() {
    for api in [OFFSET_COMMIT, OFFSET_FETCH] {
        moved_after_load(api, 10, 9).await;
    }
}

async fn offset_replacement(api: i16, uuid_request: bool, fail_first_request: bool) {
    use tokio::io::AsyncReadExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut coord = crate::net::BrokerConn::connect(&addr, "idle-offset-replace", Duration::from_secs(3))
        .await
        .unwrap();
    let (mut first, _) = listener.accept().await.unwrap();
    if api == OFFSET_FETCH {
        coord.offset_fetch_version = if uuid_request { 10 } else { 9 };
        coord.offset_fetch_range = Some((1, if uuid_request { 10 } else { 9 }));
    } else {
        coord.offset_commit_version = if uuid_request { 10 } else { 9 };
        coord.offset_commit_range = Some((2, if uuid_request { 10 } else { 9 }));
    }
    let peer = tokio::spawn(async move {
        let mut byte = [0u8; 1];
        if fail_first_request {
            let (header, body) = read_request(&mut first).await;
            assert_eq!((header.api_key, header.api_version), (api, 9));
            assert_eq!(body.as_ref(), &9i16.to_be_bytes());
            drop(first);
        } else {
            let first_result = tokio::time::timeout(Duration::from_secs(3), first.read(&mut byte))
                .await
                .unwrap();
            match first_result {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                other => panic!("old offset peer did not see retirement: {other:?}"),
            }
        }

        let version = if uuid_request { 9 } else { 8 };
        let mut socket = authenticate(&listener, api, version).await;
        if uuid_request {
            let result = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut byte))
                .await
                .unwrap();
            match result {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                other => panic!("UUID request was sent to a v9-only socket: {other:?}"),
            }
        } else {
            let (header, body) = read_request(&mut socket).await;
            assert_eq!((header.api_key, header.api_version), (api, version));
            assert_eq!(body.as_ref(), &version.to_be_bytes());
            respond(&mut socket, &header, &response(api, version, 0)).await;
        }
    });
    let mut cfg = crate::ConsumerConfig::bootstrap([addr]);
    cfg.sasl_plain = Some(("user".into(), "pass".into()));
    cfg.connections_max_idle = if fail_first_request {
        Duration::from_secs(60)
    } else {
        Duration::from_nanos(1)
    };
    tokio::task::yield_now().await;
    assert_eq!(coord.idle_expired(cfg.connections_max_idle), !fail_first_request);
    let mut prior = HashMap::new();
    let mut encoded = Vec::new();
    if uuid_request {
        let result = coord_offsets_v10(
            &mut coord,
            &cfg,
            "retry",
            COORDINATOR_GROUP,
            api,
            |buf| {
                encoded.push(10);
                buf.extend_from_slice(b"UUID");
                Ok(())
            },
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(3),
        )
        .await;
        assert!(matches!(result, Err(Error::Unsupported(_))), "{result:?}");
        assert!(encoded.is_empty(), "a UUID request cannot be re-encoded as v9");
    } else {
        let (version, body) = coord_offsets_legacy(
            &mut coord,
            &cfg,
            "retry",
            COORDINATOR_GROUP,
            api,
            9,
            |version, buf| {
                encoded.push(version);
                buf.extend_from_slice(&version.to_be_bytes());
                Ok(())
            },
            &[],
            &HashMap::new(),
            &mut prior,
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        if fail_first_request {
            assert_eq!(encoded, [9, 8]);
        } else {
            assert_eq!(encoded, [8]);
        }
        assert_eq!(version, 8);
        assert_eq!(body.as_ref(), response(api, 8, 0));
    }
    coord
        .retire_physical_deadline(crate::net::Deadline::from_timeout(Duration::from_secs(3)))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn idle_offset_replacement_authenticates_and_reencodes_for_the_new_version() {
    for api in [OFFSET_FETCH, OFFSET_COMMIT] {
        offset_replacement(api, false, false).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn idle_offset_replacement_never_downgrades_uuid_requests() {
    for api in [OFFSET_FETCH, OFFSET_COMMIT] {
        offset_replacement(api, true, false).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn failed_offset_request_retires_socket_and_reencodes_for_new_version() {
    for api in [OFFSET_FETCH, OFFSET_COMMIT] {
        offset_replacement(api, false, true).await;
    }
}

async fn typed_idle_replacement(api: i16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut coord =
        crate::net::BrokerConn::connect(&addr, "typed-idle-offset", Duration::from_secs(3))
            .await
            .unwrap();
    let (mut first, _) = listener.accept().await.unwrap();
    coord.offset_fetch_version = 9;
    coord.offset_fetch_range = Some((1, 9));
    coord.offset_commit_version = 9;
    coord.offset_commit_range = Some((2, 9));
    let peer = tokio::spawn(async move {
        let mut byte = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_secs(3), first.read(&mut byte))
            .await
            .unwrap();
        match result {
            Ok(0) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            other => panic!("old typed offset peer did not see retirement: {other:?}"),
        }
        let mut socket = authenticate(&listener, api, 8).await;
        let (header, body) = read_request(&mut socket).await;
        assert_eq!((header.api_key, header.api_version), (api, 8));
        if api == OFFSET_FETCH {
            let request = decode_offset_fetch_request_data(&body, 8, OffsetLimits::default()).unwrap();
            assert_eq!(request.groups.len(), 1);
            assert_eq!(request.groups[0].group_id, "retry");
            assert!(request.require_stable);
        } else {
            let request = decode_offset_commit_request_data(&body, 8, OffsetLimits::default()).unwrap();
            assert_eq!(request.group_id, "retry");
            assert_eq!(request.generation_id_or_member_epoch, 3);
            assert_eq!(request.member_id, "member");
        }
        respond(&mut socket, &header, &response(api, 8, 0)).await;
    });
    let mut cfg = crate::ConsumerConfig::bootstrap([addr]);
    cfg.sasl_plain = Some(("user".into(), "pass".into()));
    cfg.connections_max_idle = Duration::from_nanos(1);
    tokio::task::yield_now().await;
    assert!(coord.idle_expired(cfg.connections_max_idle));
    let request = if api == OFFSET_FETCH {
        LegacyOffsetRequest::Fetch {
            group_id: "retry",
            member_id: None,
            member_epoch: -1,
            require_stable: true,
            topics: Some(&[]),
        }
    } else {
        LegacyOffsetRequest::Commit {
            group_id: "retry",
            generation: 3,
            member_id: "member",
            instance: None,
            topics: &[],
        }
    };
    let mut prior = HashMap::new();
    let (version, body) = coord_offsets_typed(
        &mut coord,
        &cfg,
        "retry",
        COORDINATOR_GROUP,
        &request,
        &[],
        &HashMap::new(),
        &mut prior,
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    assert_eq!(version, 8);
    assert_eq!(body.as_ref(), response(api, 8, 0));
    coord
        .retire_physical_deadline(crate::net::Deadline::from_timeout(Duration::from_secs(3)))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn typed_idle_offset_replacement_uses_the_new_authenticated_version() {
    for api in [OFFSET_FETCH, OFFSET_COMMIT] {
        typed_idle_replacement(api).await;
    }
}
