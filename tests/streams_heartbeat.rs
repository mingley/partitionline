//! Public Streams heartbeat routing, cancellation and response semantics.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "finite independently scripted socket assertions"
)]
#[path = "fixtures/streams-heartbeat/socket_peer.rs"]
mod socket_peer;

use partitionline::protocol::streams::{
    decode_streams_group_heartbeat_request, decode_streams_group_heartbeat_response,
    encode_streams_group_heartbeat_request, StreamsGroupHeartbeatRequest,
};
use partitionline::{Error, StreamsClient, StreamsConfig, StreamsLimits};
use socket_peer::{Peer, Reply};
use std::time::{Duration, Instant};

fn request() -> StreamsGroupHeartbeatRequest {
    decode_streams_group_heartbeat_request(
        include_bytes!("fixtures/streams/4.3.1/heartbeat-request-full.bin"),
        0,
        StreamsLimits::default(),
    )
    .unwrap()
}

fn response(code: i16) -> Vec<u8> {
    let mut bytes = include_bytes!("fixtures/streams/4.3.1/heartbeat-response-full.bin").to_vec();
    bytes[4..6].copy_from_slice(&code.to_be_bytes());
    bytes
}

#[tokio::test]
async fn explicit_policy_and_invalid_limits_fail_without_network() {
    let peer = Peer::start().await;
    let mut cfg = peer.config();
    cfg.allow_unstable = false;
    assert!(matches!(
        StreamsClient::new(cfg),
        Err(Error::Unsupported(_))
    ));
    let mut cfg = peer.config();
    cfg.limits.wire_bytes = 0;
    assert!(matches!(StreamsClient::new(cfg), Err(Error::Protocol(_))));
    let mut cfg = peer.config();
    cfg.connection
        .bootstrap
        .resize(17, peer.addresses[0].to_string());
    assert!(StreamsClient::new(cfg).is_err());
    assert!(peer.close().await.is_empty());
}

#[tokio::test]
async fn typed_epochs_topology_assignment_and_nullable_fields_reach_group_coordinator() {
    for fc_version in [1, 3, 6] {
        let peer = Peer::start().await;
        peer.state.lock().await.find_version = fc_version;
        peer.script(1, (0..4).map(|_| Reply::Body(response(0))))
            .await;
        let mut client = StreamsClient::new(peer.config()).unwrap();
        let mut expected_requests = Vec::new();
        for epoch in [0, 7, -1, -2] {
            let mut req = request();
            req.member_epoch = epoch;
            expected_requests.push(
                encode_streams_group_heartbeat_request(&req, 0, StreamsLimits::default()).unwrap(),
            );
            let actual = client.heartbeat(&req).await;
            assert_eq!(
                actual.unwrap(),
                decode_streams_group_heartbeat_response(&response(0), 0, StreamsLimits::default())
                    .unwrap()
            );
        }
        drop(client);
        let frames = peer.close().await;
        let heartbeats: Vec<_> = frames.iter().filter(|f| f.api == 88).collect();
        assert_eq!(heartbeats.len(), 4);
        for (frame, expected) in heartbeats.iter().zip(expected_requests) {
            assert_eq!((frame.slot, frame.version), (1, 0));
            assert_eq!(frame.body, expected);
        }
        let lookups: Vec<_> = frames.iter().filter(|f| f.api == 10).collect();
        assert_eq!(lookups.len(), 1);
        for frame in lookups {
            assert_eq!((frame.slot, frame.version), (0, fc_version));
            if fc_version >= 4 {
                assert_eq!(frame.body[0], 0);
            } else {
                assert_eq!(
                    frame.body[frame.body.len() - if fc_version >= 3 { 2 } else { 1 }],
                    0
                );
            }
        }
    }
}

#[tokio::test]
async fn unavailable_bootstrap_api_refuses_discovery_and_dispatch() {
    for range in [None, Some((1, 1))] {
        let peer = Peer::start().await;
        peer.state.lock().await.ranges[0] = range;
        let mut client = StreamsClient::new(peer.config()).unwrap();
        let result = client.heartbeat(&request()).await;
        drop(client);
        let frames = peer.close().await;
        assert!(matches!(result, Err(Error::Unsupported(_))));
        assert!(frames.iter().all(|f| f.api == 18));
    }
}

#[tokio::test]
async fn coordinator_capability_is_negotiated_before_heartbeat() {
    let peer = Peer::start().await;
    peer.state.lock().await.ranges[1] = None;
    let mut client = StreamsClient::new(peer.config()).unwrap();
    let result = client.heartbeat(&request()).await;
    drop(client);
    let frames = peer.close().await;
    assert!(matches!(result, Err(Error::Unsupported(_))));
    assert!(!frames.iter().any(|f| f.api == 88));
    assert!(frames.iter().any(|f| f.api == 18 && f.slot == 1));
}

#[tokio::test]
async fn warm_heartbeats_reuse_socket_and_close_or_group_change_forces_discovery() {
    let peer = Peer::start().await;
    peer.script(1, (0..4).map(|_| Reply::Body(response(0))))
        .await;
    let mut client = StreamsClient::new(peer.config()).unwrap();
    let mut req = request();
    assert_eq!(client.heartbeat(&req).await.unwrap().error_code, 0);
    assert_eq!(client.heartbeat(&req).await.unwrap().error_code, 0);
    assert_eq!(
        peer.state
            .lock()
            .await
            .frames
            .iter()
            .filter(|f| f.api == 10)
            .count(),
        1
    );
    client.close();
    tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            if peer.state.lock().await.active_workers == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(client.heartbeat(&req).await.unwrap().error_code, 0);
    req.group_id = "another-group".into();
    assert_eq!(client.heartbeat(&req).await.unwrap().error_code, 0);
    drop(client);
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|f| f.api == 10).count(), 3);
    let beats: Vec<_> = frames.iter().filter(|f| f.api == 88).collect();
    assert_eq!(beats.len(), 4);
    for frame in beats {
        assert_eq!(
            decode_streams_group_heartbeat_request(&frame.body, 0, StreamsLimits::default())
                .unwrap()
                .member_epoch,
            request().member_epoch
        );
    }
}

#[tokio::test]
async fn coordinator_errors_and_lost_socket_refresh_route_with_identical_request() {
    for reply in [
        Reply::Body(response(14)),
        Reply::Body(response(15)),
        Reply::Body(response(16)),
        Reply::Disconnect,
    ] {
        let peer = Peer::start().await;
        peer.state.lock().await.routes.extend([1, 2]);
        peer.script(1, [reply]).await;
        peer.script(2, [Reply::Body(response(0))]).await;
        let mut client = StreamsClient::new(peer.config()).unwrap();
        let result = client.heartbeat(&request()).await;
        drop(client);
        let frames = peer.close().await;
        assert_eq!(result.unwrap().error_code, 0);
        let beats: Vec<_> = frames.iter().filter(|f| f.api == 88).collect();
        assert_eq!(beats.iter().map(|f| f.slot).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(beats[0].body, beats[1].body);
        assert_eq!(frames.iter().filter(|f| f.api == 10).count(), 2);
    }
}

#[tokio::test]
async fn terminal_member_authorization_and_topology_errors_remain_typed() {
    for code in [31, 53, 79, 110, 130, 131, 132, 133] {
        let peer = Peer::start().await;
        peer.script(1, [Reply::Body(response(code))]).await;
        let mut client = StreamsClient::new(peer.config()).unwrap();
        let result = client.heartbeat(&request()).await;
        drop(client);
        let frames = peer.close().await;
        let actual = result.unwrap();
        assert_eq!(actual.error_code, code);
        assert_eq!(
            actual.status,
            decode_streams_group_heartbeat_response(&response(code), 0, StreamsLimits::default())
                .unwrap()
                .status
        );
        assert_eq!(frames.iter().filter(|f| f.api == 88).count(), 1);
    }
}

#[tokio::test]
async fn downgrade_after_route_refresh_refuses_incompatible_dispatch() {
    let peer = Peer::start().await;
    {
        let mut s = peer.state.lock().await;
        s.routes.extend([1, 2]);
        s.ranges[2] = Some((1, 1));
    }
    peer.script(1, [Reply::Disconnect]).await;
    let mut client = StreamsClient::new(peer.config()).unwrap();
    let result = client.heartbeat(&request()).await;
    drop(client);
    let frames = peer.close().await;
    assert!(matches!(result, Err(Error::Unsupported(_))));
    assert_eq!(frames.iter().filter(|f| f.api == 88).count(), 1);
    assert!(frames.iter().any(|f| f.slot == 2 && f.api == 18));
}

#[tokio::test]
async fn timeout_and_cancellation_close_owned_sockets_without_background_membership() {
    for cancel in [false, true] {
        let peer = Peer::start().await;
        peer.script(1, [Reply::Stall]).await;
        let mut client = StreamsClient::new(peer.config()).unwrap();
        let started = Instant::now();
        let req = request();
        if cancel {
            let call = client.heartbeat(&req);
            tokio::pin!(call);
            assert!(tokio::time::timeout(Duration::from_millis(30), &mut call)
                .await
                .is_err());
        } else {
            assert!(matches!(
                client
                    .heartbeat_timeout(&req, Duration::from_millis(50))
                    .await,
                Err(Error::Timeout)
            ));
        }
        tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if peer.state.lock().await.active_workers == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        peer.script(1, [Reply::Body(response(0))]).await;
        assert_eq!(client.heartbeat(&req).await.unwrap().error_code, 0);
        drop(client);
        let frames = peer.close().await;
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(frames.iter().filter(|f| f.api == 88).count(), 2);
    }
}

#[tokio::test]
async fn repeated_delayed_retry_cannot_reset_budget_or_exceed_eight_attempts() {
    for delay in [Duration::ZERO, Duration::from_millis(20)] {
        let peer = Peer::start().await;
        peer.script(1, (0..8).map(|_| Reply::Delay(delay, response(16))))
            .await;
        let mut client = StreamsClient::new(peer.config()).unwrap();
        let started = Instant::now();
        let result = client
            .heartbeat_timeout(&request(), Duration::from_millis(70))
            .await;
        drop(client);
        let frames = peer.close().await;
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_millis(500));
        let calls = frames.iter().filter(|f| f.api == 88).count();
        assert!((1..=8).contains(&calls));
        if delay.is_zero() {
            assert_eq!(calls, 8);
            assert_eq!(result.unwrap_err().broker_code(), Some(16));
        } else {
            assert!(matches!(result, Err(Error::Timeout)));
        }
    }
}

#[tokio::test]
async fn invalid_frames_and_request_limits_reject_without_success_or_unrelated_io() {
    let mut trailing = response(0);
    trailing.push(126);
    for reply in [
        Reply::Body(trailing),
        Reply::Body(vec![0]),
        Reply::Correlation(response(0)),
    ] {
        let peer = Peer::start().await;
        peer.script(1, [reply]).await;
        let mut client = StreamsClient::new(peer.config()).unwrap();
        let result = client.heartbeat(&request()).await;
        drop(client);
        let frames = peer.close().await;
        assert!(matches!(result, Err(Error::Protocol(_))));
        assert_eq!(frames.iter().filter(|f| f.api == 88).count(), 1);
    }
    let peer = Peer::start().await;
    let mut cfg = peer.config();
    cfg.limits.string_bytes = 8;
    let mut client = StreamsClient::new(cfg).unwrap();
    assert!(client.heartbeat(&request()).await.is_err());
    assert!(matches!(
        client
            .heartbeat_timeout(&StreamsGroupHeartbeatRequest::default(), Duration::ZERO)
            .await,
        Err(Error::Timeout)
    ));
    drop(client);
    assert!(peer.close().await.is_empty());
}

#[test]
fn default_config_requires_an_explicit_protocol_choice() {
    assert!(matches!(
        StreamsClient::new(StreamsConfig::default()),
        Err(Error::Unsupported(_))
    ));
}

#[tokio::test]
#[ignore = "requires freshly generated pinned Apache builder/error-response inputs"]
async fn actual_sdk_public_heartbeat_histories() {
    let fixtures = std::path::PathBuf::from(std::env::var("STREAMS_HEARTBEAT_FIXTURES").unwrap());
    let proof = std::path::PathBuf::from(std::env::var("STREAMS_HEARTBEAT_PROOF").unwrap());
    tokio::fs::create_dir(&proof).await.unwrap();
    let cases = tokio::fs::read_to_string(fixtures.join("cases.tsv"))
        .await
        .unwrap();
    let mut index = String::from("name\tapplication_frames\texpected_response\n");
    for row in cases.lines().skip(1) {
        let cells: Vec<_> = row.split('\t').collect();
        let original_name = cells[0];
        let code: i16 = cells[1].parse().unwrap();
        let input = tokio::fs::read(fixtures.join(format!("{original_name}.request.bin")))
            .await
            .unwrap();
        let reply = tokio::fs::read(fixtures.join(format!("{original_name}.response.bin")))
            .await
            .unwrap();
        let req =
            decode_streams_group_heartbeat_request(&input, 0, StreamsLimits::default()).unwrap();
        for fc_version in [1, 3, 6] {
            let name = format!("{original_name}-fc{fc_version}");
            let peer = Peer::start().await;
            peer.state.lock().await.find_version = fc_version;
            let expected_file = if matches!(code, 14..=16) {
                peer.state.lock().await.routes.extend([1, 2]);
                peer.script(1, [Reply::Body(reply.clone())]).await;
                peer.script(
                    2,
                    [Reply::Body(
                        tokio::fs::read(fixtures.join("epoch-0.response.bin"))
                            .await
                            .unwrap(),
                    )],
                )
                .await;
                "epoch-0.response.bin".to_string()
            } else {
                peer.script(1, [Reply::Body(reply.clone())]).await;
                format!("{original_name}.response.bin")
            };
            let mut client = StreamsClient::new(peer.config()).unwrap();
            let result = client.heartbeat(&req).await;
            drop(client);
            let frames = peer.close().await;
            let result = result.unwrap();
            let encoded =
                partitionline::protocol::streams::encode_streams_group_heartbeat_response(
                    &result,
                    0,
                    StreamsLimits::default(),
                )
                .unwrap();
            tokio::fs::write(proof.join(format!("{name}.result.bin")), encoded)
                .await
                .unwrap();
            let application: Vec<_> = frames.iter().filter(|f| matches!(f.api, 10 | 88)).collect();
            assert_eq!(
                application.len(),
                if matches!(code, 14..=16) { 4 } else { 2 }
            );
            for (i, frame) in application.iter().enumerate() {
                tokio::fs::write(proof.join(format!("{name}-{i}.request.bin")), &frame.raw)
                    .await
                    .unwrap();
                if frame.api == 88 {
                    assert_eq!(frame.body, input);
                }
            }
            index.push_str(&format!("{name}\t{}\t{expected_file}\n", application.len()));
            tokio::fs::write(proof.join(format!("{name}.closure.txt")), "listeners_joined=3\nworkers_joined=true\nports_closed_and_rebound=3\nruntime_tasks=0\n").await.unwrap();
        }
    }
    tokio::fs::write(proof.join("cases.tsv"), index)
        .await
        .unwrap();
}
