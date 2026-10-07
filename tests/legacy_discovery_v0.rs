//! Metadata and GROUP discovery legacy wire semantics.
use bytes::{Bytes, BytesMut};
use partitionline::protocol::api::*;
use partitionline::protocol::group::*;

#[path = "fixtures/legacy-discovery-v0/socket_peer.rs"]
mod socket_peer;

#[tokio::test]
async fn actual_socket_negotiation_chooses_highest_and_refuses_absent_ranges() {
    for (metadata, find, supported) in [
        (Some((0, 0)), Some((0, 0)), true),
        (Some((0, 13)), Some((0, 6)), true),
        (Some((1, 1)), Some((1, 1)), true),
        (None, Some((0, 0)), false),
        (Some((14, 14)), Some((0, 0)), false),
    ] {
        let peer = socket_peer::Peer::start().await;
        {
            let mut state = peer.state.lock().await;
            state.metadata = metadata;
            state.find = find;
        }
        let result = partitionline::Admin::new(peer.config()).await;
        if supported {
            let mut admin = result.unwrap();
            assert_eq!(admin.list_topics().await.unwrap().len(), 1);
            assert_eq!(
                admin
                    .list_consumer_group_offsets("group", [("topic", 0)])
                    .await
                    .unwrap()[0]
                    .1
                    .offset,
                123
            );
            admin.close().await.unwrap();
        } else {
            assert!(matches!(result, Err(partitionline::Error::Unsupported(_))));
        }
        let frames = peer.close().await;
        if supported {
            assert_eq!(
                frames.iter().find(|f| f.api == 3).unwrap().version,
                metadata.unwrap().1
            );
            assert_eq!(
                frames.iter().find(|f| f.api == 10).unwrap().version,
                find.unwrap().1
            );
        } else {
            assert!(frames.iter().all(|f| f.api == 18));
        }
    }
}

#[tokio::test]
async fn classic_group_discovery_v0_reaches_the_reported_ipv6_coordinator() {
    let peer = socket_peer::Peer::start().await;
    let mut cfg = partitionline::ConsumerConfig::bootstrap([peer.addresses[0].to_string()]);
    cfg.allow_auto_topic_creation = true;
    let result = partitionline::ConsumerGroup::join(cfg, "group", "topic").await;
    assert_eq!(result.err().unwrap().broker_code(), Some(30));
    let frames = peer.close().await;
    assert!(frames.iter().any(|f| f.api == 10 && f.version == 0));
    assert!(frames.iter().any(|f| f.api == 11 && f.slot == 1));
}

#[tokio::test]
async fn classic_group_discovery_v0_retries_a_disconnected_bootstrap() {
    let peer = socket_peer::Peer::start().await;
    peer.state
        .lock()
        .await
        .replies
        .push_back(socket_peer::Reply::Disconnect);
    let mut cfg = partitionline::ConsumerConfig::bootstrap([peer.addresses[0].to_string()]);
    cfg.allow_auto_topic_creation = true;
    cfg.retry_backoff = std::time::Duration::from_millis(1);
    cfg.retry_backoff_max = std::time::Duration::from_millis(1);
    assert_eq!(
        partitionline::ConsumerGroup::join(cfg, "group", "topic")
            .await
            .err()
            .unwrap()
            .broker_code(),
        Some(30)
    );
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|f| f.api == 10).count(), 2);
    assert!(frames.iter().any(|f| f.api == 11 && f.slot == 1));
}

#[tokio::test]
async fn legacy_coord_address_replaces_stale_metadata_for_same_node() {
    let peer = socket_peer::Peer::start().await;
    {
        let mut state = peer.state.lock().await;
        state.routes.extend([1, 2]);
        state
            .offset_replies
            .push_back(socket_peer::Reply::Body(socket_peer::offsets()));
        let mut moved = socket_peer::offsets();
        let at = moved.len() - 4;
        moved[at..at + 2].copy_from_slice(&16i16.to_be_bytes());
        state
            .offset_replies
            .push_back(socket_peer::Reply::Body(moved));
    }
    let mut admin = partitionline::Admin::new(peer.config()).await.unwrap();
    let _topics = admin.list_topics().await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            admin
                .list_consumer_group_offsets("group", [("topic", 0)])
                .await
                .unwrap()[0]
                .1
                .offset,
            123
        );
    }
    admin.close().await.unwrap();
    let frames = peer.close().await;
    assert_eq!(
        frames
            .iter()
            .filter(|f| f.api == 9)
            .map(|f| f.slot)
            .collect::<Vec<_>>(),
        vec![1, 1, 2]
    );
}

#[tokio::test]
async fn unsupported_legacy_public_options_dispatch_no_application_frame() {
    let peer = socket_peer::Peer::start().await;
    let mut consumer =
        partitionline::Consumer::new(partitionline::ConsumerConfig::bootstrap([peer.addresses
            [0]
        .to_string()]))
        .await
        .unwrap();
    assert!(matches!(
        consumer.assign("topic", 0, 0).await,
        Err(partitionline::Error::Unsupported(_))
    ));
    assert!(consumer.assignment().is_empty());
    consumer.close().await.unwrap();
    let mut cfg = partitionline::ProducerConfig::bootstrap([peer.addresses[0].to_string()]);
    cfg.transactional_id = Some("transaction".into());
    assert!(matches!(
        partitionline::Producer::new(cfg).await,
        Err(partitionline::Error::Unsupported(_))
    ));
    let frames = peer.close().await;
    assert!(frames.iter().all(|f| f.api == 18));
}

#[tokio::test]
#[ignore = "requires owned pinned SDK process runner"]
async fn serve_legacy_discovery_probe() {
    let directory =
        std::path::PathBuf::from(std::env::var_os("LEGACY_DISCOVERY_DIRECTORY").unwrap());
    let profile = std::env::var("LEGACY_DISCOVERY_PROFILE").unwrap();
    let driver = std::env::var("LEGACY_DISCOVERY_DRIVER").unwrap();
    let fault = !["legacy", "modern-group0", "mixed"].contains(&profile.as_str());
    let peer = socket_peer::Peer::start().await;
    {
        let mut state = peer.state.lock().await;
        match profile.as_str() {
            "legacy" => (),
            "modern-group0" => {
                state.metadata = Some((13, 13));
            }
            "mixed" => {
                state.metadata = Some((0, 13));
                state.find = Some((0, 6));
            }
            "loading" | "unavailable" | "wrong-coordinator" | "terminal" | "deadline"
            | "disconnect" | "moved" | "same-node-address" => {
                state.metadata = Some((13, 13));
                match profile.as_str() {
                    "disconnect" => state.replies.push_back(socket_peer::Reply::Disconnect),
                    "moved" | "same-node-address" => {
                        state.routes.extend([1, 2]);
                        if profile == "moved" {
                            state.route_ids[2] = 8;
                        }
                        let mut body = socket_peer::offsets();
                        let at = body.len() - 4;
                        body[at..at + 2].copy_from_slice(&16i16.to_be_bytes());
                        state
                            .offset_replies
                            .push_back(socket_peer::Reply::Body(body));
                    }
                    "deadline" => state.replies.extend((0..8).map(|_| {
                        socket_peer::Reply::Delay(
                            std::time::Duration::from_secs(3),
                            socket_peer::find_error(15),
                        )
                    })),
                    _ => {
                        state
                            .replies
                            .push_back(socket_peer::Reply::Body(socket_peer::find_error(
                                match profile.as_str() {
                                    "loading" => 14,
                                    "unavailable" => 15,
                                    "wrong-coordinator" => 16,
                                    "terminal" => 30,
                                    _ => panic!("unknown error"),
                                },
                            )))
                    }
                }
            }
            _ => panic!("unknown profile"),
        }
    }
    tokio::fs::write(directory.join("ready"), peer.addresses[0].to_string())
        .await
        .unwrap();
    if driver.starts_with("rust") {
        let address = peer.addresses[0].to_string();
        let mut calls = if driver == "rust-producer" { 1 } else { 2 };
        match driver.as_str() {
            "rust-admin" => {
                let mut admin = partitionline::Admin::new(peer.config()).await.unwrap();
                if !fault {
                    assert_eq!(admin.list_topics().await.unwrap().len(), 1);
                }
                let result = admin
                    .list_consumer_group_offsets("group", [("topic", 0)])
                    .await;
                if profile == "terminal" {
                    assert_eq!(result.unwrap_err().broker_code(), Some(30));
                    calls = 0;
                } else if profile == "deadline" {
                    assert!(matches!(result, Err(partitionline::Error::Timeout)));
                    calls = 0;
                } else {
                    assert_eq!(result.unwrap()[0].1.offset, 123);
                    if fault {
                        calls = 1;
                    }
                }
                admin.close().await.unwrap();
            }
            "rust-consumer" => {
                let mut cfg = partitionline::ConsumerConfig::bootstrap([address]);
                cfg.allow_auto_topic_creation = true;
                let mut consumer = partitionline::Consumer::new(cfg).await.unwrap();
                consumer.assign("topic", 0, 0).await.unwrap();
                assert!(consumer.fetch().await.unwrap().is_empty());
                consumer.close().await.unwrap();
            }
            "rust-producer" => {
                let mut cfg = partitionline::ProducerConfig::bootstrap([address]);
                cfg.allow_auto_topic_creation = true;
                cfg.enable_idempotence = false;
                let producer = partitionline::Producer::new(cfg).await.unwrap();
                assert_eq!(producer.partitions_for("topic").await.unwrap().len(), 1);
                producer.close().await.unwrap();
            }
            _ => panic!("unknown driver"),
        }
        tokio::fs::write(
            directory.join("rust-outcome.json"),
            &format!(
                "{{\"status\":\"pass\",\"successful_public_calls\":{}}}\n",
                calls
            ),
        )
        .await
        .unwrap();
    } else {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
        while !tokio::fs::try_exists(directory.join("stop")).await.unwrap() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    let responses = peer.state.lock().await.responses.clone();
    let requests = peer.close().await;
    let mut rows = String::from("kind\tindex\tslot\tapi\tversion\n");
    for (kind, frames) in [("request", requests), ("response", responses)] {
        for (index, frame) in frames.into_iter().enumerate() {
            assert!(frame.api == 18 && frame.version == 0 || !frame.body.is_empty());
            tokio::fs::write(
                directory.join(format!(
                    "{kind}-{}-{}-{index}.frame",
                    frame.api, frame.version
                )),
                frame.raw,
            )
            .await
            .unwrap();
            rows.push_str(&format!(
                "{kind}\t{index}\t{}\t{}\t{}\n",
                frame.slot, frame.api, frame.version
            ));
        }
    }
    tokio::fs::write(directory.join("frames.tsv"), rows)
        .await
        .unwrap();
    tokio::fs::write(
        directory.join("closure.txt"),
        "runtime_tasks=0\nports_closed_and_rebound=3\n",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn legacy_public_consumer_producer_and_admin_routes() {
    let peer = socket_peer::Peer::start().await;
    let address = peer.addresses[0].to_string();
    let mut cfg = partitionline::ConsumerConfig::bootstrap([address.clone()]);
    cfg.allow_auto_topic_creation = true;
    let mut consumer = partitionline::Consumer::new(cfg).await.unwrap();
    consumer.assign("topic", 0, 0).await.unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    consumer.close().await.unwrap();
    let mut cfg = partitionline::ProducerConfig::bootstrap([address]);
    cfg.enable_idempotence = false;
    cfg.allow_auto_topic_creation = true;
    let producer = partitionline::Producer::new(cfg).await.unwrap();
    assert_eq!(producer.partitions_for("topic").await.unwrap().len(), 1);
    producer.close().await.unwrap();
    let mut admin = partitionline::Admin::new(peer.config()).await.unwrap();
    assert_eq!(admin.list_topics().await.unwrap().len(), 1);
    let offsets = admin
        .list_consumer_group_offsets("group", [("topic", 0)])
        .await
        .unwrap();
    assert_eq!(offsets[0].1.offset, 123);
    admin.close().await.unwrap();
    let frames = peer.close().await;
    assert!(frames.iter().any(|f| f.api == 1 && f.slot == 1));
    assert!(frames.iter().any(|f| f.api == 10 && f.version == 0));
    assert!(frames.iter().any(|f| f.api == 9 && f.slot == 1));
}

#[tokio::test]
async fn bootstrap_budget_covers_version_probe_and_fallback() {
    use std::time::Duration;
    let peer = socket_peer::Peer::start().await;
    {
        let mut state = peer.state.lock().await;
        state.metadata = Some((1, 1));
        state.startup.push_back(socket_peer::Reply::Delay(
            Duration::from_millis(40),
            vec![0, 35, 0, 0, 0, 0],
        ));
        state.startup.push_back(socket_peer::Reply::Delay(
            Duration::from_millis(50),
            vec![0, 0, 0, 0, 0, 0],
        ));
    }
    let mut cfg = peer.config();
    cfg.request_timeout = Duration::from_millis(60);
    let started = std::time::Instant::now();
    assert!(matches!(
        partitionline::Admin::new(cfg).await,
        Err(partitionline::Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_millis(85));
    let _frames = peer.close().await;
}

#[tokio::test]
async fn group_discovery_retries_legacy_errors_and_disconnection_with_one_budget() {
    for code in [14i16, 15, 16, 30] {
        let peer = socket_peer::Peer::start().await;
        let mut body = code.to_be_bytes().to_vec();
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        peer.state
            .lock()
            .await
            .replies
            .push_back(socket_peer::Reply::Body(body));
        let mut admin = partitionline::Admin::new(peer.config()).await.unwrap();
        let result = admin
            .list_consumer_group_offsets("group", [("topic", 0)])
            .await;
        if code == 30 {
            assert_eq!(result.unwrap_err().broker_code(), Some(30));
        } else {
            assert_eq!(result.unwrap()[0].1.offset, 123);
        }
        admin.close().await.unwrap();
        let frames = peer.close().await;
        assert_eq!(
            frames.iter().filter(|f| f.api == 10).count(),
            if code == 30 { 1 } else { 2 }
        );
    }
    let peer = socket_peer::Peer::start().await;
    peer.state
        .lock()
        .await
        .replies
        .push_back(socket_peer::Reply::Disconnect);
    let mut admin = partitionline::Admin::new(peer.config()).await.unwrap();
    assert_eq!(
        admin
            .list_consumer_group_offsets("group", [("topic", 0)])
            .await
            .unwrap()[0]
            .1
            .offset,
        123
    );
    admin.close().await.unwrap();
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|f| f.api == 10).count(), 2);
}

#[tokio::test]
async fn group_discovery_caps_and_budget_refuse_before_offset_dispatch() {
    use std::time::Duration;
    for range in [None, Some((7, 7))] {
        let peer = socket_peer::Peer::start().await;
        peer.state.lock().await.find = range;
        let mut admin = partitionline::Admin::new(peer.config()).await.unwrap();
        assert!(matches!(
            admin
                .list_consumer_group_offsets("group", [("topic", 0)])
                .await,
            Err(partitionline::Error::Unsupported(_))
        ));
        admin.close().await.unwrap();
        let frames = peer.close().await;
        assert!(frames.iter().all(|f| f.api == 18));
    }
    let peer = socket_peer::Peer::start().await;
    let mut error = 15i16.to_be_bytes().to_vec();
    error.extend_from_slice(&(-1i32).to_be_bytes());
    error.extend_from_slice(&0i16.to_be_bytes());
    error.extend_from_slice(&(-1i32).to_be_bytes());
    peer.state.lock().await.replies.extend(
        (0..8).map(|_| socket_peer::Reply::Delay(Duration::from_millis(25), error.clone())),
    );
    let mut cfg = peer.config();
    cfg.request_timeout = Duration::from_millis(60);
    let mut admin = partitionline::Admin::new(cfg).await.unwrap();
    let started = std::time::Instant::now();
    assert!(matches!(
        admin
            .list_consumer_group_offsets("group", [("topic", 0)])
            .await,
        Err(partitionline::Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_millis(90));
    admin.close().await.unwrap();
    let frames = peer.close().await;
    assert!(frames.iter().all(|f| f.api != 9));
}

#[tokio::test]
async fn controller_operations_refuse_metadata_v0_without_inventing_a_controller() {
    let peer = socket_peer::Peer::start().await;
    let mut admin = partitionline::Admin::new(peer.config()).await.unwrap();
    let result = admin
        .create_topics(
            &[partitionline::NewTopic::new("new-topic", 1, 1)],
            800,
            false,
        )
        .await;
    assert!(
        matches!(result,Err(partitionline::Error::Protocol(message)) if message=="no controller")
    );
    admin.close().await.unwrap();
    let frames = peer.close().await;
    assert!(frames.iter().any(|f| f.api == 3 && f.version == 0));
    assert!(frames.iter().all(|f| f.api != 19));
}

#[tokio::test]
async fn producer_consumer_and_group_startup_keep_the_initial_deadline() {
    use std::time::Duration;
    for driver in ["producer", "consumer", "group"] {
        let peer = socket_peer::Peer::start().await;
        {
            let mut state = peer.state.lock().await;
            if driver == "group" {
                state.startup.push_back(socket_peer::Reply::Delay(
                    Duration::from_millis(20),
                    socket_peer::versions(Some((0, 0)), Some((0, 0)), 35),
                ));
                state
                    .startup
                    .push_back(socket_peer::Reply::Body(socket_peer::versions(
                        Some((0, 0)),
                        Some((0, 0)),
                        0,
                    )));
                state.startup.push_back(socket_peer::Reply::Delay(
                    Duration::from_millis(50),
                    socket_peer::versions(Some((0, 0)), Some((0, 0)), 35),
                ));
            } else {
                state.startup.push_back(socket_peer::Reply::Delay(
                    Duration::from_millis(40),
                    socket_peer::versions(Some((0, 0)), Some((0, 0)), 35),
                ));
                state.startup.push_back(socket_peer::Reply::Delay(
                    Duration::from_millis(50),
                    socket_peer::versions(Some((0, 0)), Some((0, 0)), 0),
                ));
            }
        }
        let address = peer.addresses[0].to_string();
        let started = std::time::Instant::now();
        let error = if driver == "producer" {
            let mut cfg = partitionline::ProducerConfig::bootstrap([address]);
            cfg.enable_idempotence = false;
            cfg.request_timeout = Duration::from_millis(60);
            partitionline::Producer::new(cfg).await.err()
        } else {
            let mut cfg = partitionline::ConsumerConfig::bootstrap([address]);
            cfg.allow_auto_topic_creation = true;
            cfg.request_timeout = Duration::from_millis(60);
            if driver == "group" {
                partitionline::ConsumerGroup::join(cfg, "group", "topic")
                    .await
                    .err()
            } else {
                partitionline::Consumer::new(cfg).await.err()
            }
        };
        assert!(matches!(error, Some(partitionline::Error::Timeout)));
        assert!(started.elapsed() < Duration::from_millis(90));
        let frames = peer.close().await;
        assert!(frames.iter().all(|f| f.api == 18));
    }
}

#[test]
fn legacy_metadata_sparse_route_indices_and_partial_writers_are_bounded() {
    let mut raw = socket_peer::metadata("[::1]:9092".parse().unwrap(), 0);
    // One topic's partition index follows its error code, after the two arrays.
    let index_at = 4 + 4 + 2 + 3 + 4 + 4 + 2 + 2 + 5 + 4 + 2;
    raw[index_at..index_at + 4].copy_from_slice(&i32::MAX.to_be_bytes());
    assert!(decode_metadata_response(&mut Bytes::from(raw), 0).is_err());
    let mut out = BytesMut::from(&b"sentinel"[..]);
    let before = out.clone();
    let c = CoordinatorResult {
        host: "x".repeat(32768),
        key: String::new(),
        node_id: 7,
        port: 9092,
        error_code: 0,
        error_message: None,
    };
    assert!(encode_find_coordinator_response_coordinators(&mut out, 0, &[c]).is_err());
    assert_eq!(out, before);
}

#[test]
fn metadata_v0_all_topics_uses_nonnullable_empty_array() {
    for names in [None, Some(Vec::<String>::new())] {
        let mut output = BytesMut::new();
        encode_metadata_request(&mut output, 0, names.as_deref(), true).unwrap();
        assert_eq!(output.as_ref(), &0i32.to_be_bytes());
        let (topics, allow, topic_auth, cluster_auth) =
            decode_metadata_request(&mut output.freeze(), 0).unwrap();
        assert_eq!(topics, Some(Vec::new()));
        assert!(allow);
        assert!(!topic_auth && !cluster_auth);
    }
}

#[test]
fn metadata_v0_names_and_absent_fields_match_legacy_schema() {
    let mut output = BytesMut::new();
    encode_metadata_request(&mut output, 0, Some(&["topic".into()]), true).unwrap();
    assert_eq!(output.as_ref(), b"\0\0\0\x01\0\x05topic");
    let mut response = Bytes::from_static(b"\0\0\0\0\0\0\0\0");
    let metadata = decode_metadata_response(&mut response, 0).unwrap();
    assert_eq!(metadata.controller_id, MetadataResponse::NO_CONTROLLER_ID);
    assert!(metadata.cluster_id.is_none());
    assert!(metadata.brokers.is_empty());
    assert!(metadata.topics.is_empty());
    assert_eq!(metadata.throttle_time_ms, 0);
    assert_eq!(metadata.cluster_authorized_operations, i32::MIN);
}

#[test]
fn metadata_v0_has_finite_body_admission_and_whole_input() {
    let impossible = i32::MAX.to_be_bytes();
    assert!(decode_metadata_request(&mut Bytes::copy_from_slice(&impossible), 0).is_err());
    assert!(decode_metadata_response(&mut Bytes::copy_from_slice(&impossible), 0).is_err());
    let mut oversized = Bytes::from(vec![0; 1024 * 1024 + 1]);
    assert!(decode_metadata_response(&mut oversized, 0).is_err());
    let mut trailing = Bytes::from_static(b"\0\0\0\0\0\0\0\0\0");
    assert!(decode_metadata_response(&mut trailing, 0).is_err());
    let mut null_topics = Bytes::from_static(b"\xff\xff\xff\xff");
    assert!(decode_metadata_request(&mut null_topics, 0).is_err());
}

#[test]
fn group_coordinator_v0_has_no_type_throttle_or_error_message() {
    let mut request = BytesMut::new();
    encode_find_coordinator_request(&mut request, 0, "group").unwrap();
    assert_eq!(request.as_ref(), b"\0\x05group");
    assert_eq!(
        decode_find_coordinator_request(&mut request.freeze(), 0).unwrap(),
        ("group".into(), 0)
    );
    let mut response = BytesMut::new();
    encode_find_coordinator_response(&mut response, 0, 7, "h", 9092, "group").unwrap();
    assert_eq!(response.as_ref(), b"\0\0\0\0\0\x07\0\x01h\0\0\x23\x84");
    assert_eq!(
        decode_find_coordinator_response(&mut response.freeze(), 0).unwrap(),
        (0, 7, "h".into(), 9092)
    );
}

#[test]
fn unsupported_legacy_options_and_java_builder_policy_are_preserved() {
    let mut output = BytesMut::new();
    assert!(
        encode_find_coordinator_request_typed(&mut output, 0, "tx", COORDINATOR_TRANSACTION)
            .is_err()
    );
    assert!(output.is_empty());
    assert!(encode_metadata_request(&mut output, 0, Some(&["topic".into()]), false).is_err());
    assert!(output.is_empty());
    assert!(MetadataRequest::build(0, None, true).is_err());
}

#[tokio::test]
#[ignore = "requires pinned Apache fixture generations and owned process runner"]
async fn actual_sdk_legacy_discovery_bodies() {
    let fixtures = std::path::PathBuf::from(std::env::var_os("LEGACY_DISCOVERY_FIXTURES").unwrap());
    let reverse = std::path::PathBuf::from(std::env::var_os("LEGACY_DISCOVERY_REVERSE").unwrap());
    tokio::fs::create_dir(&reverse).await.unwrap();
    let mut bodies = 0;
    for version in [0, 1, 13] {
        for mode in ["all", "empty", "named"] {
            let name = format!("metadata-v{version}-{mode}.request.bin");
            let raw = tokio::fs::read(fixtures.join(&name)).await.unwrap();
            let mut cursor = Bytes::copy_from_slice(&raw);
            let (topics, allow, topic_auth, cluster_auth) =
                decode_metadata_request_topics(&mut cursor, version).unwrap();
            assert!(cursor.is_empty());
            assert!(allow);
            assert!(!topic_auth && !cluster_auth);
            let expected = if mode == "all" && version >= 1 {
                None
            } else {
                Some(if mode == "named" {
                    vec![MetadataRequestTopic {
                        name: Some("topic-κ".into()),
                        topic_id: [0; 16],
                    }]
                } else {
                    Vec::new()
                })
            };
            assert_eq!(topics, expected);
            assert_eq!(
                MetadataRequest::is_all_topics(version, topics.as_deref()),
                mode == "all" || version == 0 && mode == "empty"
            );
            let mut output = BytesMut::new();
            encode_metadata_request_topics(
                &mut output,
                version,
                topics.as_deref(),
                allow,
                topic_auth,
            )
            .unwrap();
            assert_eq!(output.as_ref(), raw);
            tokio::fs::write(reverse.join(name), output).await.unwrap();
            bodies += 1;
        }
        for mode in ["full", "empty", "error"] {
            let name = format!("metadata-v{version}-{mode}.response.bin");
            let raw = tokio::fs::read(fixtures.join(&name)).await.unwrap();
            let mut cursor = Bytes::copy_from_slice(&raw);
            let response = decode_metadata_response(&mut cursor, version).unwrap();
            assert!(cursor.is_empty());
            let topic_id = if version >= 10 {
                let mut bytes = [0; 16];
                bytes
                    .get_mut(..8)
                    .unwrap()
                    .copy_from_slice(&1u64.to_be_bytes());
                bytes
                    .get_mut(8..)
                    .unwrap()
                    .copy_from_slice(&17u64.to_be_bytes());
                bytes
            } else {
                [0; 16]
            };
            let expected = MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![Broker::new(
                    7,
                    "::1",
                    9092,
                    if version >= 1 {
                        Some(String::new())
                    } else {
                        None
                    },
                )],
                cluster_id: if version >= 2 {
                    Some("cluster".into())
                } else {
                    None
                },
                controller_id: if version >= 1 { 7 } else { -1 },
                topics: if mode == "empty" {
                    Vec::new()
                } else {
                    vec![TopicMetadata {
                        error_code: if mode == "error" { 3 } else { 0 },
                        name: Some("topic-κ".into()),
                        topic_id,
                        is_internal: version >= 1,
                        partitions: if mode == "error" {
                            Vec::new()
                        } else {
                            vec![PartitionMetadata {
                                error_code: 0,
                                partition_index: 0,
                                leader_id: 7,
                                leader_epoch: if version >= 7 { 4 } else { -1 },
                                replica_nodes: vec![7, 9],
                                isr_nodes: vec![7],
                                offline_replicas: Vec::new(),
                            }]
                        },
                        topic_authorized_operations: i32::MIN,
                    }]
                },
                cluster_authorized_operations: i32::MIN,
                error_code: 0,
            };
            assert_eq!(response, expected);
            let mut output = BytesMut::new();
            encode_metadata_response(&mut output, version, &response).unwrap();
            assert_eq!(output.as_ref(), raw);
            tokio::fs::write(reverse.join(name), output).await.unwrap();
            bodies += 1;
        }
    }
    for version in [0, 1, 3, 6] {
        let name = format!("find-v{version}.request.bin");
        let raw = tokio::fs::read(fixtures.join(&name)).await.unwrap();
        let mut cursor = Bytes::copy_from_slice(&raw);
        let (keys, kind) = decode_find_coordinator_request_keys(&mut cursor, version).unwrap();
        assert!(cursor.is_empty());
        assert_eq!(keys, ["group-κ"]);
        assert_eq!(kind, 0);
        let mut output = BytesMut::new();
        encode_find_coordinator_request(&mut output, version, "group-κ").unwrap();
        assert_eq!(output.as_ref(), raw);
        tokio::fs::write(reverse.join(name), output).await.unwrap();
        bodies += 1;
        for code in [0, 14, 15, 16, 30] {
            let name = format!("find-v{version}-{code}.response.bin");
            let raw = tokio::fs::read(fixtures.join(&name)).await.unwrap();
            let mut cursor = Bytes::copy_from_slice(&raw);
            let (coordinators, throttle) =
                decode_find_coordinator_response_coordinators(&mut cursor, version).unwrap();
            assert!(cursor.is_empty());
            assert_eq!(throttle, 0);
            assert_eq!(
                coordinators,
                vec![CoordinatorResult {
                    key: if version >= 4 {
                        "group-κ".into()
                    } else {
                        String::new()
                    },
                    node_id: 7,
                    host: "::1".into(),
                    port: 9092,
                    error_code: code,
                    error_message: if version >= 1 {
                        Some(String::new())
                    } else {
                        None
                    }
                }]
            );
            let mut output = BytesMut::new();
            encode_find_coordinator_response_coordinators(&mut output, version, &coordinators)
                .unwrap();
            assert_eq!(output.as_ref(), raw);
            tokio::fs::write(reverse.join(name), output).await.unwrap();
            bodies += 1;
        }
    }
    assert_eq!(bodies, 42);
}
