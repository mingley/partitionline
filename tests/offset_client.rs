//! Public offset identity routing and owned socket lifetime.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "finite scripted socket assertions"
)]
#[path = "fixtures/offset-topic-ids-v10/socket_peer.rs"]
mod socket_peer;

use partitionline::protocol::group::*;
use partitionline::{Admin, Error, OffsetClient, OffsetOptions};
use partitionline::{ConsumerConfig, ConsumerGroup};
use socket_peer::{Peer, Reply};
use std::time::{Duration, Instant};

fn id(number: u8) -> OffsetTopicIdentity {
    OffsetTopicIdentity::Id([number; 16])
}
fn options() -> OffsetOptions {
    OffsetOptions {
        topic_ids: true,
        ..Default::default()
    }
}
fn commit() -> OffsetCommitRequestData {
    OffsetCommitRequestData {
        group_id: "group".into(),
        generation_id_or_member_epoch: 7,
        member_id: "member".into(),
        group_instance_id: Some(String::new()),
        topics: vec![OffsetCommitTopicData {
            identity: id(1),
            partitions: vec![OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 123,
                committed_leader_epoch: 4,
                committed_metadata: None,
            }],
        }],
    }
}
fn committed(code: i16) -> Vec<u8> {
    encode_offset_commit_response_data(
        &OffsetCommitResponseData {
            throttle_time_ms: 42,
            topics: vec![OffsetCommitTopicResponseData {
                identity: id(1),
                partitions: vec![OffsetCommitPartitionResponseData {
                    partition_index: 0,
                    error_code: code,
                }],
            }],
        },
        10,
        OffsetLimits::default(),
    )
    .unwrap()
}
fn fetch() -> OffsetFetchRequestData {
    OffsetFetchRequestData {
        groups: vec![OffsetFetchGroupData {
            group_id: "group".into(),
            member_id: Some("member".into()),
            member_epoch: 7,
            topics: Some(vec![OffsetFetchTopicData {
                identity: id(1),
                partition_indexes: vec![0],
            }]),
        }],
        require_stable: true,
    }
}
fn fetched(groups: &[&str], code: i16) -> Vec<u8> {
    encode_offset_fetch_response_data(
        &OffsetFetchResponseData {
            throttle_time_ms: 42,
            groups: groups
                .iter()
                .rev()
                .map(|group| OffsetFetchGroupResponseData {
                    group_id: (*group).into(),
                    topics: vec![OffsetFetchTopicResponseData {
                        identity: id(1),
                        partitions: vec![OffsetFetchPartitionData {
                            partition_index: 0,
                            committed_offset: 123,
                            committed_leader_epoch: 4,
                            metadata: None,
                            error_code: code,
                        }],
                    }],
                    error_code: 0,
                })
                .collect(),
        },
        10,
        OffsetLimits::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn validation_and_empty_operations_need_no_network() {
    let peer = Peer::start().await;
    let mut client = OffsetClient::new(peer.config()).unwrap();
    assert!(client
        .commit(&commit(), &OffsetOptions::default())
        .await
        .is_err());
    let mut opts = options();
    opts.limits.decoded_bytes = 1;
    assert!(client.commit(&commit(), &opts).await.is_err());
    assert_eq!(
        client
            .commit(&OffsetCommitRequestData::default(), &options())
            .await
            .unwrap(),
        OffsetCommitResponseData::default()
    );
    assert_eq!(
        client
            .fetch(&OffsetFetchRequestData::default(), &options())
            .await
            .unwrap(),
        OffsetFetchResponseData::default()
    );
    let mut duplicate = fetch();
    duplicate.groups.push(duplicate.groups[0].clone());
    assert!(client.fetch(&duplicate, &options()).await.is_err());
    assert!(peer.close().await.is_empty());
}

#[tokio::test]
async fn exact_uuid_commit_and_restart_fetch_preserve_epoch_metadata_and_errors() {
    for find_version in [1, 3, 6] {
        let peer = Peer::start().await;
        peer.state.lock().await.find_version = find_version;
        peer.script(
            1,
            [
                Reply::Body(committed(0)),
                Reply::Body(fetched(&["group"], 100)),
            ],
        )
        .await;
        let mut client = OffsetClient::new(peer.config()).unwrap();
        assert_eq!(
            client.commit(&commit(), &options()).await.unwrap().topics[0].partitions[0].error_code,
            0
        );
        drop(client);
        let mut restarted = OffsetClient::new(peer.config()).unwrap();
        let result = restarted.fetch(&fetch(), &options()).await.unwrap();
        assert_eq!(result.groups[0].topics[0].identity, id(1));
        assert_eq!(result.groups[0].topics[0].partitions[0].error_code, 100);
        assert_eq!(result.groups[0].topics[0].partitions[0].metadata, None);
        drop(restarted);
        let frames = peer.close().await;
        let c = frames.iter().find(|frame| frame.api == 8).unwrap();
        let f = frames.iter().find(|frame| frame.api == 9).unwrap();
        assert_eq!(c.version, 10);
        assert_eq!(f.version, 10);
        assert_eq!(
            usize::try_from(i32::from_be_bytes(c.raw[..4].try_into().unwrap())).unwrap() + 4,
            c.raw.len()
        );
        assert_eq!(
            decode_offset_commit_request_data(&c.body, 10, OffsetLimits::default()).unwrap(),
            commit()
        );
        assert_eq!(
            decode_offset_fetch_request_data(&f.body, 10, OffsetLimits::default()).unwrap(),
            fetch()
        );
        assert!(frames
            .iter()
            .filter(|frame| frame.api == 10)
            .all(|frame| frame.version == find_version));
    }
}

#[tokio::test]
async fn actual_coordinator_capability_is_required_before_mutation() {
    for missing_bootstrap in [false, true] {
        let peer = Peer::start().await;
        peer.state.lock().await.ranges[usize::from(!missing_bootstrap)] = Some((8, 9));
        let mut client = OffsetClient::new(peer.config()).unwrap();
        assert!(matches!(
            client.commit(&commit(), &options()).await,
            Err(Error::Unsupported(_))
        ));
        drop(client);
        let frames = peer.close().await;
        assert!(!frames.iter().any(|frame| frame.api == 8));
        assert_eq!(
            frames.iter().filter(|frame| frame.api == 10).count(),
            usize::from(!missing_bootstrap)
        );
    }
}

#[tokio::test]
async fn legacy_admin_checks_actual_socket_ranges_without_sending_a_name_as_v10() {
    for range in [None, Some((10, 10))] {
        let peer = Peer::start().await;
        {
            let mut state = peer.state.lock().await;
            state.ranges = [Some((8, 9)), range, Some((8, 9))];
        }
        let mut admin = Admin::new(peer.config()).await.unwrap();
        let error = admin
            .alter_consumer_group_offsets(
                "group",
                [(
                    partitionline::TopicPartition::new("topic", 0),
                    partitionline::OffsetAndMetadata::new(123),
                )],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Unsupported(_)));
        drop(admin);
        let frames = peer.close().await;
        assert!(!frames.iter().any(|frame| frame.api == 8));
    }
}

#[tokio::test]
async fn missing_uuid_metadata_and_oversized_metadata_fail_before_admin_commit() {
    let peer = Peer::start().await;
    peer.state.lock().await.metadata_version = 9;
    let mut admin = Admin::new(peer.config()).await.unwrap();
    assert!(matches!(
        admin
            .alter_consumer_group_offsets(
                "group",
                [(
                    partitionline::TopicPartition::new("topic", 0),
                    partitionline::OffsetAndMetadata::new(123)
                )]
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    drop(admin);
    let frames = peer.close().await;
    assert!(!frames.iter().any(|frame| frame.api == 3 || frame.api == 8));
    let peer = Peer::start().await;
    peer.state
        .lock()
        .await
        .metadata_replies
        .push_back(vec![0; 1024 * 1024]);
    let mut admin = Admin::new(peer.config()).await.unwrap();
    assert!(admin
        .alter_consumer_group_offsets(
            "group",
            [(
                partitionline::TopicPartition::new("topic", 0),
                partitionline::OffsetAndMetadata::new(123)
            )]
        )
        .await
        .is_err());
    drop(admin);
    let frames = peer.close().await;
    assert!(!frames.iter().any(|frame| frame.api == 8));
}

#[tokio::test]
async fn coordinator_moves_re_negotiate_and_keep_the_exact_uuid() {
    for first in [
        Reply::Body(committed(14)),
        Reply::Body(committed(15)),
        Reply::Body(committed(16)),
        Reply::Disconnect,
    ] {
        let peer = Peer::start().await;
        peer.state.lock().await.routes.extend([1, 2]);
        peer.script(1, [first]).await;
        peer.script(2, [Reply::Body(committed(0))]).await;
        let mut client = OffsetClient::new(peer.config()).unwrap();
        assert_eq!(
            client.commit(&commit(), &options()).await.unwrap().topics[0].identity,
            id(1)
        );
        drop(client);
        let frames = peer.close().await;
        let calls = frames
            .iter()
            .filter(|frame| frame.api == 8)
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].slot, 1);
        assert_eq!(calls[1].slot, 2);
        assert_eq!(calls[0].body, calls[1].body);
        assert!(frames
            .iter()
            .any(|frame| frame.api == 18 && frame.slot == 2));
    }
}

#[tokio::test]
async fn a_moved_older_coordinator_cannot_receive_a_uuid_as_a_name() {
    let peer = Peer::start().await;
    {
        let mut state = peer.state.lock().await;
        state.routes.extend([1, 2]);
        state.ranges[2] = Some((8, 9));
    }
    peer.script(1, [Reply::Body(committed(16))]).await;
    let mut client = OffsetClient::new(peer.config()).unwrap();
    assert!(matches!(
        client.commit(&commit(), &options()).await,
        Err(Error::Unsupported(_))
    ));
    drop(client);
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|frame| frame.api == 8).count(), 1);
    assert!(!frames.iter().any(|frame| frame.api == 8 && frame.slot == 2));
}

#[tokio::test]
async fn unknown_uuid_cannot_be_attached_to_another_requested_topic() {
    for fetch_response in [false, true] {
        let peer = Peer::start().await;
        let mut body = if fetch_response {
            fetched(&["group"], 0)
        } else {
            committed(0)
        };
        // Replace the independently located identity with a different UUID.
        let start = body.windows(16).position(|bytes| bytes == [1; 16]).unwrap();
        body[start..start + 16].fill(2);
        peer.script(1, [Reply::Body(body)]).await;
        let mut client = OffsetClient::new(peer.config()).unwrap();
        let result = if fetch_response {
            client.fetch(&fetch(), &options()).await.map(|_| ())
        } else {
            client.commit(&commit(), &options()).await.map(|_| ())
        };
        assert!(matches!(result, Err(Error::Protocol(_))));
        drop(client);
        let _frames = peer.close().await;
    }
}

#[tokio::test]
async fn same_coordinator_groups_batch_and_response_order_is_restored() {
    let peer = Peer::start().await;
    peer.script(1, [Reply::Body(fetched(&["group", "other"], 0))])
        .await;
    let mut req = fetch();
    let mut other = req.groups[0].clone();
    other.group_id = "other".into();
    req.groups.push(other);
    let mut client = OffsetClient::new(peer.config()).unwrap();
    let result = client.fetch(&req, &options()).await.unwrap();
    assert_eq!(
        result
            .groups
            .iter()
            .map(|group| group.group_id.as_str())
            .collect::<Vec<_>>(),
        ["group", "other"]
    );
    drop(client);
    let frames = peer.close().await;
    let calls = frames
        .iter()
        .filter(|frame| frame.api == 9)
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        decode_offset_fetch_request_data(&calls[0].body, 10, OffsetLimits::default())
            .unwrap()
            .groups
            .len(),
        2
    );
}

#[tokio::test]
async fn null_all_topics_preserves_unknown_uuid_and_empty_selection_stays_empty() {
    let peer = Peer::start().await;
    let empty = encode_offset_fetch_response_data(
        &OffsetFetchResponseData {
            groups: vec![OffsetFetchGroupResponseData {
                group_id: "group".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
        10,
        OffsetLimits::default(),
    )
    .unwrap();
    peer.script(1, [Reply::Body(fetched(&["group"], 0)), Reply::Body(empty)])
        .await;
    let mut req = fetch();
    req.groups[0].topics = None;
    let mut client = OffsetClient::new(peer.config()).unwrap();
    assert_eq!(
        client.fetch(&req, &options()).await.unwrap().groups[0].topics[0].identity,
        id(1)
    );
    req.groups[0].topics = Some(Vec::new());
    assert!(client.fetch(&req, &options()).await.unwrap().groups[0]
        .topics
        .is_empty());
    drop(client);
    let frames = peer.close().await;
    let calls = frames
        .iter()
        .filter(|frame| frame.api == 9)
        .collect::<Vec<_>>();
    assert_eq!(
        decode_offset_fetch_request_data(&calls[0].body, 10, OffsetLimits::default())
            .unwrap()
            .groups[0]
            .topics,
        None
    );
    assert_eq!(
        decode_offset_fetch_request_data(&calls[1].body, 10, OffsetLimits::default())
            .unwrap()
            .groups[0]
            .topics,
        Some(Vec::new())
    );
}

#[tokio::test]
async fn cancellation_closes_operation_sockets_while_client_remains_usable() {
    let peer = Peer::start().await;
    peer.script(1, [Reply::Stall, Reply::Body(committed(0))])
        .await;
    let mut client = OffsetClient::new(peer.config()).unwrap();
    let started = Instant::now();
    {
        let req = commit();
        let opts = options();
        let call = client.commit(&req, &opts);
        tokio::pin!(call);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut call)
            .await
            .is_err());
    }
    assert!(started.elapsed() < Duration::from_millis(500));
    for _ in 0..100 {
        if peer.state.lock().await.active_workers == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(peer.state.lock().await.active_workers, 0);
    assert!(client.commit(&commit(), &options()).await.is_ok());
    drop(client);
    let _frames = peer.close().await;
}

#[tokio::test]
async fn admin_typed_calls_use_real_coordinator_capabilities_and_raw_errors() {
    let peer = Peer::start().await;
    peer.script(
        1,
        [
            Reply::Body(committed(100)),
            Reply::Body(fetched(&["group"], 100)),
        ],
    )
    .await;
    let mut admin = Admin::new(peer.config()).await.unwrap();
    assert_eq!(
        admin
            .commit_group_offsets(&commit(), &options())
            .await
            .unwrap()
            .topics[0]
            .partitions[0]
            .error_code,
        100
    );
    assert_eq!(
        admin
            .fetch_group_offsets(&fetch(), &options())
            .await
            .unwrap()
            .groups[0]
            .topics[0]
            .partitions[0]
            .error_code,
        100
    );
    drop(admin);
    let frames = peer.close().await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.api == 8 || frame.api == 9)
            .count(),
        2
    );
}

#[tokio::test]
async fn one_deadline_and_eight_attempts_bound_ambiguous_commits() {
    let peer = Peer::start().await;
    peer.script(1, (0..8).map(|_| Reply::Body(committed(16))))
        .await;
    let mut client = OffsetClient::new(peer.config()).unwrap();
    assert!(client.commit(&commit(), &options()).await.is_err());
    drop(client);
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|frame| frame.api == 8).count(), 8);
    let peer = Peer::start().await;
    peer.script(1, [Reply::Delay(Duration::from_millis(200), committed(0))])
        .await;
    let mut client = OffsetClient::new(peer.config()).unwrap();
    let started = Instant::now();
    let mut opts = options();
    opts.timeout = Some(Duration::from_millis(60));
    assert!(matches!(
        client.commit(&commit(), &opts).await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_millis(180));
    drop(client);
    let _frames = peer.close().await;
}

#[tokio::test]
async fn malformed_identity_results_and_correlation_are_terminal() {
    for reply in [Reply::Body(vec![0; 5]), Reply::Correlation(committed(0))] {
        let peer = Peer::start().await;
        peer.script(1, [reply]).await;
        let mut client = OffsetClient::new(peer.config()).unwrap();
        assert!(client.commit(&commit(), &options()).await.is_err());
        drop(client);
        let frames = peer.close().await;
        assert_eq!(frames.iter().filter(|frame| frame.api == 8).count(), 1);
    }
}

#[tokio::test]
async fn ordinary_group_commit_and_restart_fetch_use_nonzero_assignment_identity() {
    let peer = Peer::start().await;
    peer.script(
        1,
        [
            Reply::Body(fetched(&["group"], 0)),
            Reply::Body(committed(0)),
            Reply::Body(fetched(&["group"], 0)),
        ],
    )
    .await;
    let mut cfg = ConsumerConfig::bootstrap([peer.addresses[0].to_string()]);
    cfg.request_timeout = Duration::from_secs(1);
    let mut group = ConsumerGroup::join_consumer(cfg.clone(), "group", "topic")
        .await
        .unwrap();
    assert_eq!(group.positions()[0].1, 123);
    group.commit().await.unwrap();
    group.close().await.unwrap();
    let restarted = ConsumerGroup::join_consumer(cfg, "group", "topic")
        .await
        .unwrap();
    assert_eq!(restarted.positions()[0].1, 123);
    restarted.close().await.unwrap();
    let frames = peer.close().await;
    let commits = frames
        .iter()
        .filter(|frame| frame.api == 8)
        .collect::<Vec<_>>();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].version, 10);
    let request =
        decode_offset_commit_request_data(&commits[0].body, 10, OffsetLimits::default()).unwrap();
    assert_eq!(request.topics[0].identity, id(1));
    assert_eq!(request.generation_id_or_member_epoch, 7);
    assert_eq!(request.member_id, "member");
    assert_eq!(request.topics[0].partitions[0].committed_offset, 123);
    assert!(frames
        .iter()
        .filter(|frame| frame.api == 9)
        .all(|frame| frame.version == 10));
}

#[tokio::test]
async fn group_explicit_identity_calls_check_membership_before_dispatch() {
    let peer = Peer::start().await;
    peer.script(
        1,
        [
            Reply::Body(fetched(&["group"], 0)),
            Reply::Body(committed(100)),
            Reply::Body(fetched(&["group"], 0)),
        ],
    )
    .await;
    let mut cfg = ConsumerConfig::bootstrap([peer.addresses[0].to_string()]);
    cfg.request_timeout = Duration::from_secs(1);
    let mut group = ConsumerGroup::join_consumer(cfg, "group", "topic")
        .await
        .unwrap();
    let mut request = commit();
    request.group_instance_id = None;
    request.member_id = "wrong".into();
    assert!(matches!(
        group.commit_offsets_by_identity(&request, &options()).await,
        Err(Error::Protocol(_))
    ));
    request.member_id = "member".into();
    assert_eq!(
        group
            .commit_offsets_by_identity(&request, &options())
            .await
            .unwrap()
            .topics[0]
            .partitions[0]
            .error_code,
        100
    );
    assert_eq!(
        group
            .committed_offsets_by_identity(&fetch(), &options())
            .await
            .unwrap()
            .groups[0]
            .topics[0]
            .identity,
        id(1)
    );
    group.close().await.unwrap();
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|frame| frame.api == 8).count(), 1);
}

#[tokio::test]
async fn metadata_refresh_cannot_rebind_delivered_group_offsets_to_a_recreated_name() {
    let peer = Peer::start().await;
    peer.script(
        1,
        [
            Reply::Body(fetched(&["group"], 0)),
            Reply::Body(committed(100)),
        ],
    )
    .await;
    let mut cfg = ConsumerConfig::bootstrap([peer.addresses[0].to_string()]);
    cfg.request_timeout = Duration::from_secs(1);
    let mut group = ConsumerGroup::join_consumer(cfg, "group", "topic")
        .await
        .unwrap();
    group.seek("topic", 0, 124).unwrap();
    peer.state.lock().await.metadata_id = [2; 16];
    assert_eq!(group.list_topics().await.unwrap().len(), 1);
    assert_eq!(group.commit().await.unwrap_err().broker_code(), Some(100));
    group.close().await.unwrap();
    let frames = peer.close().await;
    let frame = frames.iter().find(|frame| frame.api == 8).unwrap();
    let request =
        decode_offset_commit_request_data(&frame.body, 10, OffsetLimits::default()).unwrap();
    assert_eq!(request.topics[0].identity, id(1));
    assert_eq!(request.topics[0].partitions[0].committed_offset, 124);
}

#[tokio::test]
async fn old_async_commit_keeps_its_uuid_after_metadata_refresh() {
    let peer = Peer::start().await;
    peer.script(
        1,
        [
            Reply::Body(fetched(&["group"], 0)),
            Reply::Body(committed(100)),
        ],
    )
    .await;
    let mut cfg = ConsumerConfig::bootstrap([peer.addresses[0].to_string()]);
    cfg.request_timeout = Duration::from_secs(1);
    let mut group = ConsumerGroup::join_consumer(cfg, "group", "topic")
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    group.commit_async_with(move |result| {
        tx.send(result).unwrap();
    });
    peer.state.lock().await.metadata_id = [2; 16];
    let _topics = group.list_topics().await.unwrap();
    group.close().await.unwrap();
    assert_eq!(rx.await.unwrap().unwrap_err().broker_code(), Some(100));
    let frames = peer.close().await;
    let frame = frames.iter().find(|frame| frame.api == 8).unwrap();
    assert_eq!(
        decode_offset_commit_request_data(&frame.body, 10, OffsetLimits::default())
            .unwrap()
            .topics[0]
            .identity,
        id(1)
    );
}

#[tokio::test]
async fn recreation_during_initial_assignment_cannot_bind_old_committed_offset_to_new_uuid() {
    let peer = Peer::start().await;
    peer.state
        .lock()
        .await
        .metadata_ids
        .extend([[1; 16], [2; 16]]);
    peer.script(1, [Reply::Body(fetched(&["group"], 0))]).await;
    let mut cfg = ConsumerConfig::bootstrap([peer.addresses[0].to_string()]);
    cfg.request_timeout = Duration::from_secs(1);
    assert!(matches!(
        ConsumerGroup::join_consumer(cfg, "group", "topic").await,
        Err(Error::Protocol(_))
    ));
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|frame| frame.api == 9).count(), 1);
    assert!(!frames.iter().any(|frame| frame.api == 8));
}

#[tokio::test]
async fn existing_admin_name_methods_capture_uuid_metadata_before_commit_and_projection() {
    let peer = Peer::start().await;
    peer.script(
        1,
        [
            Reply::Body(committed(0)),
            Reply::Body(fetched(&["group"], 0)),
        ],
    )
    .await;
    let mut admin = Admin::new(peer.config()).await.unwrap();
    admin
        .alter_consumer_group_offsets(
            "group",
            [(
                partitionline::TopicPartition::new("topic", 0),
                partitionline::OffsetAndMetadata::new(123),
            )],
        )
        .await
        .unwrap();
    let offsets = admin
        .list_all_consumer_group_offsets("group")
        .await
        .unwrap();
    assert_eq!(offsets[0].0.topic, "topic");
    assert_eq!(offsets[0].1.offset, 123);
    drop(admin);
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|frame| frame.api == 3).count(), 2);
    let committed = frames.iter().find(|frame| frame.api == 8).unwrap();
    assert_eq!(
        decode_offset_commit_request_data(&committed.body, 10, OffsetLimits::default())
            .unwrap()
            .topics[0]
            .identity,
        id(1)
    );
}

#[tokio::test]
async fn named_admin_batches_use_one_uuid_snapshot_and_restore_group_order() {
    let peer = Peer::start().await;
    peer.script(1, [Reply::Body(fetched(&["group", "other"], 0))])
        .await;
    let mut admin = Admin::new(peer.config()).await.unwrap();
    let spec = partitionline::ListConsumerGroupOffsetsSpec::topic_partitions([
        partitionline::TopicPartition::new("topic", 0),
    ]);
    let result = admin
        .list_consumer_group_offsets_for_groups_with(
            [
                ("group", spec.clone()),
                (
                    "noop",
                    partitionline::ListConsumerGroupOffsetsSpec::topic_partitions(Vec::<
                        partitionline::TopicPartition,
                    >::new(
                    )),
                ),
                ("other", spec),
            ],
            true,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(
        result
            .iter()
            .map(|entry| entry.0.as_str())
            .collect::<Vec<_>>(),
        ["group", "noop", "other"]
    );
    assert!(result[1].1.is_empty());
    for index in [0, 2] {
        assert_eq!(result[index].1.len(), 1);
        assert_eq!(result[index].1[0].0.topic, "topic");
        assert_eq!(result[index].1[0].1.offset, 123);
    }
    drop(admin);
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|frame| frame.api == 3).count(), 1);
    let fetches = frames
        .iter()
        .filter(|frame| frame.api == 9)
        .collect::<Vec<_>>();
    assert_eq!(fetches.len(), 1);
    let request =
        decode_offset_fetch_request_data(&fetches[0].body, 10, OffsetLimits::default()).unwrap();
    assert!(request.require_stable);
    assert_eq!(request.groups.len(), 2);
    assert!(request
        .groups
        .iter()
        .all(|group| group.topics.as_ref().unwrap()[0].identity == id(1)));
}

#[tokio::test]
async fn recreated_assignment_revokes_old_identity_and_reads_new_committed_position() {
    let peer = Peer::start().await;
    let mut new_fetch =
        decode_offset_fetch_response_data(&fetched(&["group"], 0), 10, OffsetLimits::default())
            .unwrap();
    new_fetch.groups[0].topics[0].identity = id(2);
    new_fetch.groups[0].topics[0].partitions[0].committed_offset = 456;
    let mut new_commit =
        decode_offset_commit_response_data(&committed(0), 10, OffsetLimits::default()).unwrap();
    new_commit.topics[0].identity = id(2);
    peer.script(
        1,
        [
            Reply::Body(fetched(&["group"], 0)),
            Reply::Body(
                encode_offset_fetch_response_data(&new_fetch, 10, OffsetLimits::default()).unwrap(),
            ),
            Reply::Body(
                encode_offset_commit_response_data(&new_commit, 10, OffsetLimits::default())
                    .unwrap(),
            ),
        ],
    )
    .await;
    let changes = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let observed = std::sync::Arc::clone(&changes);
    let mut cfg = ConsumerConfig::bootstrap([peer.addresses[0].to_string()]).on_rebalance(
        move |revoked, assigned| {
            observed.lock().push((revoked.to_vec(), assigned.to_vec()));
        },
    );
    cfg.request_timeout = Duration::from_secs(1);
    let mut group = ConsumerGroup::join_consumer(cfg, "group", "topic")
        .await
        .unwrap();
    group.seek("topic", 0, 124).unwrap();
    peer.state.lock().await.metadata_id = [2; 16];
    group.enforce_rebalance();
    assert!(group
        .poll_timeout(Duration::from_millis(50))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(group.position("topic", 0).unwrap(), 456);
    group.commit().await.unwrap();
    group.close().await.unwrap();
    {
        let changes = changes.lock();
        assert_eq!(changes.len(), 2);
        let tp = partitionline::TopicPartition::new("topic", 0);
        assert_eq!(changes[1], (vec![tp.clone()], vec![tp]));
    }
    let frames = peer.close().await;
    let committed = frames.iter().find(|frame| frame.api == 8).unwrap();
    let request =
        decode_offset_commit_request_data(&committed.body, 10, OffsetLimits::default()).unwrap();
    assert_eq!(request.topics[0].identity, id(2));
    assert_eq!(request.topics[0].partitions[0].committed_offset, 456);
}

#[tokio::test]
async fn named_admin_batch_input_caps_reject_before_discovery() {
    let peer = Peer::start().await;
    let mut admin = Admin::new(peer.config()).await.unwrap();
    assert!(admin
        .list_consumer_group_offsets_for_groups_with(
            (0..257).map(|index| (
                format!("group-{index}"),
                partitionline::ListConsumerGroupOffsetsSpec::all(),
            )),
            true,
            Duration::from_secs(1)
        )
        .await
        .is_err());
    assert!(admin
        .list_consumer_group_offsets_for_groups_with(
            [(
                "group",
                partitionline::ListConsumerGroupOffsetsSpec::topic_partitions(
                    (0..8193).map(|index| partitionline::TopicPartition::new("topic", index)),
                )
            ),],
            true,
            Duration::from_secs(1)
        )
        .await
        .is_err());
    drop(admin);
    let frames = peer.close().await;
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.api, 3 | 8 | 9 | 10)));
}

#[tokio::test]
async fn legacy_named_admin_offsets_share_one_deadline_and_validate_group_before_io() {
    let peer = Peer::start().await;
    peer.state.lock().await.ranges = [Some((9, 9)); 3];
    let mut admin = Admin::new(peer.config()).await.unwrap();
    let large_group = "g".repeat(65537);
    assert!(admin
        .alter_consumer_group_offsets(
            &large_group,
            [(
                partitionline::TopicPartition::new("topic", 0),
                partitionline::OffsetAndMetadata::new(123),
            )]
        )
        .await
        .is_err());
    assert!(admin
        .list_all_consumer_group_offsets(&large_group)
        .await
        .is_err());
    assert!(!peer
        .state
        .lock()
        .await
        .frames
        .iter()
        .any(|frame| matches!(frame.api, 8..=10)));
    let mut response =
        decode_offset_commit_response_data(&committed(0), 10, OffsetLimits::default()).unwrap();
    response.topics[0].identity = OffsetTopicIdentity::Name("topic".into());
    peer.script(
        1,
        [Reply::Delay(
            Duration::from_millis(200),
            encode_offset_commit_response_data(&response, 9, OffsetLimits::default()).unwrap(),
        )],
    )
    .await;
    let started = Instant::now();
    assert!(matches!(
        admin
            .alter_consumer_group_offsets_timeout(
                "group",
                [(
                    partitionline::TopicPartition::new("topic", 0),
                    partitionline::OffsetAndMetadata::new(123),
                )],
                Duration::from_millis(60)
            )
            .await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_millis(180));
    drop(admin);
    let frames = peer.close().await;
    assert_eq!(frames.iter().filter(|frame| frame.api == 8).count(), 1);
}

#[tokio::test]
#[ignore = "requires pinned SDK seeds and owned process runner"]
async fn serve_offset_id_probe() {
    let directory = std::path::PathBuf::from(std::env::var_os("OFFSET_ID_DIRECTORY").unwrap());
    let fixtures = std::path::PathBuf::from(std::env::var_os("OFFSET_ID_FIXTURES").unwrap());
    let profile = std::env::var("OFFSET_ID_PROFILE").unwrap();
    let driver = std::env::var("OFFSET_ID_DRIVER").unwrap();
    let peer = Peer::start().await;
    {
        let mut state = peer.state.lock().await;
        let range = match profile.as_str() {
            "v8" => (8, 8),
            "v9" => (9, 9),
            "v10" => (10, 10),
            _ => (8, 10),
        };
        state.ranges = [Some(range); 3];
        state.require_bootstrap_find = !driver.starts_with("java");
        if let Some(version) = profile.strip_prefix("metadata-") {
            state.metadata_version = version.parse().unwrap();
            for _ in 0..2 {
                state.metadata_replies.push_back(
                    tokio::fs::read(
                        fixtures.join(format!("metadata-v{version}-full.response.bin")),
                    )
                    .await
                    .unwrap(),
                );
            }
        }
    }
    let committed = tokio::fs::read(fixtures.join("commit-0.bin"))
        .await
        .unwrap();
    let fetched = tokio::fs::read(fixtures.join(if driver.ends_with("batch") {
        "fetch-batch-0.bin"
    } else {
        "fetch-0.bin"
    }))
    .await
    .unwrap();
    let replies = if driver.ends_with("batch") {
        vec![Reply::Body(fetched)]
    } else if driver == "rust-group" {
        vec![
            Reply::Body(fetched.clone()),
            Reply::Body(committed),
            Reply::Body(fetched),
        ]
    } else {
        vec![Reply::Body(committed), Reply::Body(fetched)]
    };
    peer.script(1, replies).await;
    tokio::fs::write(directory.join("ready"), peer.addresses[0].to_string())
        .await
        .unwrap();
    if driver.starts_with("rust") {
        let mut success = 0;
        if driver == "rust-batch" {
            let mut admin = Admin::new(peer.config()).await.unwrap();
            let spec = partitionline::ListConsumerGroupOffsetsSpec::topic_partitions([
                partitionline::TopicPartition::new("topic", 0),
            ]);
            let result = admin
                .list_consumer_group_offsets_for_groups_with(
                    [("other", spec.clone()), ("group", spec)],
                    true,
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
            assert_eq!(result.len(), 2);
            for (entry, id) in result.iter().zip(["other", "group"]) {
                assert_eq!(entry.0, id);
                assert_eq!(entry.1.len(), 1);
                assert_eq!(entry.1[0].0.topic, "topic");
                assert_eq!(entry.1[0].1.offset, 123);
                assert_eq!(entry.1[0].1.leader_epoch, Some(4));
            }
            success += 1;
            drop(admin);
        } else if driver == "rust-admin" {
            let mut admin = Admin::new(peer.config()).await.unwrap();
            admin
                .alter_consumer_group_offsets(
                    "group",
                    [(
                        partitionline::TopicPartition::new("topic", 0),
                        partitionline::OffsetAndMetadata {
                            offset: 123,
                            leader_epoch: Some(4),
                            metadata: String::new(),
                        },
                    )],
                )
                .await
                .unwrap();
            success += 1;
            let result = admin
                .list_all_consumer_group_offsets_with("group", true, Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].0.topic, "topic");
            assert_eq!(result[0].1.offset, 123);
            assert_eq!(result[0].1.leader_epoch, Some(4));
            success += 1;
            drop(admin);
        } else if driver == "rust-group" {
            let mut cfg = ConsumerConfig::bootstrap([peer.addresses[0].to_string()]);
            cfg.request_timeout = Duration::from_secs(1);
            let mut group = ConsumerGroup::join_consumer(cfg, "group", "topic")
                .await
                .unwrap();
            group.commit().await.unwrap();
            success += 1;
            let result = group.committed().await.unwrap();
            assert_eq!(result[0].1.offset, 123);
            assert_eq!(result[0].1.leader_epoch, Some(4));
            success += 1;
            group.close().await.unwrap();
        } else if driver == "rust-typed" {
            let mut client = OffsetClient::new(peer.config()).unwrap();
            let result = client.commit(&commit(), &options()).await;
            if profile == "v8" || profile == "v9" {
                assert!(matches!(result, Err(Error::Unsupported(_))));
            } else {
                assert_eq!(result.unwrap().topics[0].partitions[0].error_code, 0);
                success += 1;
                assert_eq!(
                    client.fetch(&fetch(), &options()).await.unwrap().groups[0].topics[0]
                        .partitions[0]
                        .committed_offset,
                    123
                );
                success += 1;
            }
            drop(client);
        } else {
            panic!("unknown Rust driver");
        }
        tokio::fs::write(
            directory.join("rust-outcome.json"),
            format!("{{\"successful_public_calls\":{success}}}\n"),
        )
        .await
        .unwrap();
    } else {
        let deadline = Instant::now() + Duration::from_secs(12);
        while !directory.join("stop").exists() {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let responses = peer.state.lock().await.responses.clone();
    let requests = peer.close().await;
    let mut rows = String::from("kind\tindex\tslot\tapi\tversion\n");
    for (kind, frames) in [("request", requests), ("response", responses)] {
        for (index, frame) in frames.into_iter().enumerate() {
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
