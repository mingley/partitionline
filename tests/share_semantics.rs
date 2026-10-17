//! Share semantics over real sockets. The deterministic peer clock controls
//! broker locks independently from the client's advisory local deadline.
#![expect(
    unused_results,
    reason = "test producer metadata is asserted by the consumer"
)]
#![expect(
    clippy::unwrap_used,
    reason = "assertions inspect trusted socket fixtures"
)]

mod common;

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use partitionline::protocol::api_keys::{SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_HEARTBEAT};
use partitionline::protocol::records::{Header, Record, RecordBatch};
use partitionline::protocol::share::{
    decode_share_fetch_response, decode_share_fetch_response_with_limit,
    encode_share_fetch_response, encode_share_fetch_response_with_acquisition_lock_timeout,
    AcquiredRange, ShareFetchedPartition, ShareFetchedTopic,
};
use partitionline::share::ShareAcquireMode;
use partitionline::{
    error, Admin, AlterConfig, ConfigResource, ConfigResourceUpdate, ConsumerConfig, Error,
    NewTopic, ProduceRecord, Producer, ProducerConfig, ShareGroup, ShareRecord,
    ShareRequestMetadata,
};

fn config(mock: &common::Mock) -> ConsumerConfig {
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.max_wait_ms = 5;
    cfg.request_timeout = Duration::from_secs(2);
    cfg.retry_backoff = Duration::from_millis(2);
    cfg.retry_backoff_max = Duration::from_millis(5);
    cfg
}

fn v2(mock: &common::Mock) {
    mock.set_api_max(SHARE_FETCH, 2);
    mock.set_api_max(SHARE_ACKNOWLEDGE, 2);
}

async fn produce(mock: &common::Mock, entries: &[(i32, &[u8])]) {
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    let producer = Producer::new(cfg).await.unwrap();
    for (partition, value) in entries {
        producer
            .send(
                ProduceRecord::to("t")
                    .partition(*partition)
                    .value(value.to_vec()),
            )
            .await
            .unwrap();
    }
    producer.close().await.unwrap();
}

/// Leave waits until a share heartbeat stuck on the broker has stopped.
#[tokio::test(flavor = "current_thread")]
async fn leave_finishes_the_share_heartbeat_task() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    mock.set_share_heartbeat_interval_ms(40);
    produce(&mock, &[(0, b"share")]).await;
    let mut cfg = config(&mock);
    cfg.heartbeat_interval = Duration::from_millis(40);
    cfg.request_timeout = Duration::from_secs(2);
    let group = ShareGroup::join(cfg, "share-hb-stop", "t").await.unwrap();
    let before = mock.share_heartbeat_calls();
    mock.set_share_heartbeat_delay(Duration::from_secs(5));
    let started_wait = Instant::now();
    while mock.share_heartbeat_calls() == before {
        if started_wait.elapsed() > Duration::from_secs(2) {
            panic!("share heartbeat did not reach the broker");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let task = group.test_heartbeat_task().expect("heartbeat task");
    let started = Instant::now();
    group
        .close_timeout(Duration::from_millis(400))
        .await
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "leave waited for the delayed share heartbeat"
    );
    assert!(
        task.is_finished(),
        "leave returned while the share heartbeat task was still alive"
    );
}

/// A share-group heartbeat retry reuses the coordinator handshake.
#[tokio::test]
async fn share_heartbeat_retries_without_another_handshake() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    mock.move_coordinator();
    mock.set_share_heartbeat_interval_ms(40);
    produce(&mock, &[(0, b"share")]).await;
    let mut cfg = config(&mock);
    cfg.heartbeat_interval = Duration::from_millis(40);
    cfg.retry_backoff = Duration::from_millis(20);
    cfg.retry_backoff_max = Duration::from_millis(20);
    let group = ShareGroup::join(cfg, "share-hb-known", "t").await.unwrap();
    let started_first = Instant::now();
    while mock.share_heartbeat_calls() < 3 {
        if started_first.elapsed() > Duration::from_secs(2) {
            panic!("share heartbeat did not settle");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let versions_before = mock.api_versions_versions().len();
    let beats_before = mock.share_heartbeat_calls();
    mock.set_share_heartbeat_error_times(14, 1);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    while mock.share_heartbeat_calls() < beats_before + 2 {
        if started.elapsed() > Duration::from_secs(2) {
            panic!("share heartbeat did not retry");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(200),
        "share heartbeat retry repeated ApiVersions: {elapsed:?}"
    );
    assert_eq!(mock.api_versions_versions().len(), versions_before);
    group.leave().await.unwrap();
}

/// A share fetch to another bootstrap reuses the handshake from startup.
#[tokio::test]
async fn share_fetch_reuses_a_finished_bootstrap_handshake() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    mock.set_share_heartbeat_interval_ms(60_000);
    produce(&mock, &[(0, b"share")]).await;
    mock.set_api_versions_delay(Duration::from_millis(250));
    let addr2 = mock.broker_addr(2).unwrap();
    let mut cfg = config(&mock);
    cfg.bootstrap = vec![mock.addr.clone(), addr2];
    cfg.heartbeat_interval = Duration::from_secs(5);
    cfg.request_timeout = Duration::from_secs(3);
    let started = Instant::now();
    let mut group = ShareGroup::join(cfg, "share-boot", "t").await.unwrap();
    let startup = started.elapsed();
    let versions = mock.api_versions_versions().len();
    assert!(
        versions <= 8,
        "startup negotiated a bootstrap more than once: {versions}"
    );
    assert!(
        startup < Duration::from_millis(700),
        "startup repeated a bootstrap handshake: {startup:?}"
    );
    let started = Instant::now();
    let got = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(got.len(), 1);
    assert_eq!(
        mock.api_versions_versions().len(),
        versions,
        "share fetch renegotiated a bootstrap that finished at startup"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "share fetch repeated ApiVersions: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// Overlapped share leaders reuse bootstraps that finished at startup.
#[tokio::test]
async fn share_fetch_overlaps_leaders_without_repeating_bootstrap_handshakes() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    mock.set_share_heartbeat_interval_ms(60_000);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let addr2 = mock.broker_addr(2).unwrap();
    let mut cfg = config(&mock);
    cfg.bootstrap = vec![mock.addr.clone(), addr2];
    cfg.heartbeat_interval = Duration::from_secs(5);
    cfg.request_timeout = Duration::from_secs(3);
    let mut group = ShareGroup::join(cfg, "share-two-leaders", "t")
        .await
        .unwrap();
    let versions_before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let got = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(got.len(), 2, "one poll collects both leaders");
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "overlapped share dials renegotiated a bootstrap"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "overlapped share dials repeated ApiVersions: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// A share fetch from the group coordinator does not repeat ApiVersions.
#[tokio::test]
async fn share_fetch_reuses_the_coordinator_handshake() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    mock.move_coordinator();
    mock.set_share_heartbeat_interval_ms(60_000);
    produce(&mock, &[(0, b"share")]).await;
    let mut cfg = config(&mock);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ShareGroup::join(cfg, "share-coord", "t").await.unwrap();
    let versions_before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let got = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(got.len(), 1);
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "the share leader should reuse the coordinator handshake"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "share fetch repeated the coordinator handshake: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// A share fetch from the bootstrap broker does not repeat ApiVersions.
#[tokio::test]
async fn share_fetch_reuses_bootstrap_api_versions() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_share_heartbeat_interval_ms(60_000);
    produce(&mock, &[(0, b"share")]).await;
    let mut cfg = config(&mock);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ShareGroup::join(cfg, "share-reuse", "t").await.unwrap();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let versions_before = mock.api_versions_versions().len();
    let started = Instant::now();
    let got = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(got.len(), 1);
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "the share leader socket should reuse the member broker's versions"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "share fetch repeated ApiVersions: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn wakeup_interrupts_share_poll_and_accept_once() {
    let mock = common::Mock::start().await;
    v2(&mock);
    produce(&mock, &[(0, b"share")]).await;
    let mut group = ShareGroup::join(config(&mock), "wakeup-share", "t")
        .await
        .unwrap();
    group.wakeup();
    let interrupted = group.poll().await.unwrap_err();
    assert!(
        matches!(interrupted, Error::Wakeup),
        "share poll must observe a pending wakeup, got {interrupted}"
    );
    let acquired = group.poll().await.unwrap();
    assert_eq!(acquired.len(), 1);
    group.wakeup();
    let interrupted = group.accept(&acquired).await.unwrap_err();
    assert!(
        matches!(interrupted, Error::Wakeup),
        "share accept must observe a pending wakeup, got {interrupted}"
    );
    group.accept(&acquired).await.unwrap();
    group.leave().await.unwrap();
}

fn assert_broker(error: Error, expected: i16) {
    assert!(
        matches!(error, Error::Broker { code, .. } if code == expected),
        "{error:?}"
    );
}

#[tokio::test]
async fn share_fetch_log_append_time_uses_the_batch_max_timestamp() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    let topic_id = [9u8; 16];
    mock.recreate_topic_id("t", topic_id);
    let mut batch = RecordBatch::from_records(vec![
        Record {
            offset: 0,
            timestamp: 10,
            key: Some(Bytes::from_static(b"k0")),
            value: Some(Bytes::from_static(b"a")),
            headers: vec![Header::new("h", Bytes::from_static(b"v"))],
        },
        Record {
            offset: 0,
            timestamp: 40,
            key: Some(Bytes::from_static(b"k1")),
            value: Some(Bytes::from_static(b"b")),
            headers: Vec::new(),
        },
    ])
    .with_timestamp_type(partitionline::TimestampType::LogAppendTime);
    batch.partition_leader_epoch = 4;
    let mut part = ShareFetchedPartition::partition_response(0, 0);
    part.records = vec![batch];
    part.acquired = vec![AcquiredRange {
        first_offset: 0,
        last_offset: 1,
        delivery_count: 1,
    }];
    let mut body = BytesMut::new();
    encode_share_fetch_response(
        &mut body,
        2,
        &[ShareFetchedTopic {
            topic_id,
            partitions: vec![part],
        }],
    )
    .unwrap();
    mock.inject_share_fetch_response_once(&body);
    let mut group = ShareGroup::join(config(&mock), "log-append", "t")
        .await
        .unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(
        got.iter()
            .map(|record| (record.offset, record.timestamp, record.timestamp_type))
            .collect::<Vec<_>>(),
        vec![
            (0, 40, partitionline::TimestampType::LogAppendTime),
            (1, 40, partitionline::TimestampType::LogAppendTime),
        ]
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_fetch_request_uses_configured_record_and_byte_limits() {
    let mock = common::Mock::start().await;
    v2(&mock);
    produce(&mock, &[(0, b"wide")]).await;
    let mut cfg = config(&mock);
    cfg.max_poll_records = Some(64);
    cfg.max_bytes = 4096;
    cfg.max_partition_fetch_bytes = 2048;
    let mut group = ShareGroup::join(cfg, "limits", "t").await.unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(mock.last_share_fetch_max_records(), Some(64));
    assert_eq!(mock.last_share_fetch_batch_size(), Some(64));
    assert_eq!(mock.last_share_fetch_max_bytes(), Some(4096));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_join_loads_subscribed_topics_once() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    let before = mock.metadata_calls();
    let group = ShareGroup::join(config(&mock), "once", "t").await.unwrap();
    assert_eq!(
        mock.metadata_calls(),
        before + 1,
        "join should load the subscription once"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_metadata_refreshes_stale_topics_together() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    let mut cfg = config(&mock);
    cfg.metadata_max_age = Duration::ZERO;
    let mut group = ShareGroup::join_topics(cfg, "stale-meta", ["t", "u"])
        .await
        .unwrap();
    let before = mock.metadata_calls();
    let got = group.poll().await.unwrap();
    assert!(got.is_empty());
    assert_eq!(
        mock.metadata_calls(),
        before + 1,
        "stale topics should share one metadata request"
    );
    let mut names = mock.last_metadata_topics().unwrap().unwrap();
    names.sort();
    assert_eq!(names, ["t".to_string(), "u".to_string()]);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_fetch_overlaps_two_leaders() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let delay = Duration::from_millis(250);
    mock.delay_share_reply_once(SHARE_FETCH, delay);
    mock.delay_share_reply_once(SHARE_FETCH, delay);
    let mut group = ShareGroup::join(config(&mock), "overlap", "t")
        .await
        .unwrap();
    let started = Instant::now();
    let got = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(got.len(), 2, "one poll collects both leaders");
    let mut nodes: Vec<i32> = mock
        .share_fetch_history()
        .into_iter()
        .map(|(node, _)| node)
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    assert_eq!(nodes, vec![1, 2]);
    assert!(
        elapsed < Duration::from_millis(450),
        "overlapped leader fetches took {elapsed:?}"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_close_overlaps_two_leaders() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let mut group = ShareGroup::join(config(&mock), "close-overlap", "t")
        .await
        .unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 2, "both leaders must open a share session");
    let delay = Duration::from_millis(250);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    let started = Instant::now();
    group.leave().await.unwrap();
    let elapsed = started.elapsed();
    let mut nodes: Vec<i32> = mock
        .share_ack_history()
        .into_iter()
        .filter(|(_, epoch)| *epoch == ShareRequestMetadata::FINAL_EPOCH)
        .map(|(node, _)| node)
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    assert_eq!(nodes, vec![1, 2]);
    assert!(
        elapsed < Duration::from_millis(450),
        "share session close ran one leader after another: {elapsed:?}"
    );
}

#[tokio::test]
async fn share_ack_overlaps_two_leaders() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let mut group = ShareGroup::join(config(&mock), "ack-overlap", "t")
        .await
        .unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 2, "both leaders must deliver a record");
    let delay = Duration::from_millis(250);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    let started = Instant::now();
    group.accept(&got).await.unwrap();
    let elapsed = started.elapsed();
    let mut nodes: Vec<i32> = mock
        .share_ack_history()
        .into_iter()
        .filter(|(_, epoch)| *epoch > 0)
        .map(|(node, _)| node)
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    assert_eq!(nodes, vec![1, 2]);
    assert_eq!(group.acquired_record_count(), 0);
    assert!(
        elapsed < Duration::from_millis(450),
        "share acknowledgements ran one leader after another: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// The final share acknowledgement and the coordinator leave run together.
#[tokio::test]
async fn share_leave_closes_sessions_while_leaving() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let mut group = ShareGroup::join(config(&mock), "leave-overlap", "t")
        .await
        .unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 2);
    let delay = Duration::from_millis(250);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    mock.delay_share_reply_once(SHARE_GROUP_HEARTBEAT, delay);
    let started = Instant::now();
    group.leave().await.unwrap();
    assert!(
        mock.share_ack_history()
            .iter()
            .any(|(_, epoch)| *epoch == ShareRequestMetadata::FINAL_EPOCH),
        "leave should retire the share sessions"
    );
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "share leave closed sessions after the coordinator: {:?}",
        started.elapsed()
    );
}

/// Resubscribe loads the new topic while the old share sessions close.
#[tokio::test]
async fn share_resubscribe_refreshes_metadata_while_closing_sessions() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let mut group = ShareGroup::join(config(&mock), "resub-overlap", "t")
        .await
        .unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 2, "both leaders must open a share session");
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 1);
    let delay = Duration::from_millis(250);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    mock.set_metadata_delay(delay);
    let before = mock.metadata_calls();
    let started = Instant::now();
    group.subscribe(["t", "u"]).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        mock.metadata_calls() > before,
        "resubscribe should load metadata for the new topic"
    );
    assert!(
        mock.share_ack_history()
            .iter()
            .any(|(_, epoch)| *epoch == ShareRequestMetadata::FINAL_EPOCH),
        "resubscribe should retire the previous share sessions"
    );
    assert!(
        elapsed < Duration::from_millis(450),
        "share resubscribe closed sessions before loading metadata: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// An idle resubscribe dials the share leader while the new topic loads.
#[tokio::test]
async fn share_resubscribe_dials_idle_leader_while_loading_metadata() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_share_heartbeat_interval_ms(60_000);
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    produce(&mock, &[(0, b"held")]).await;
    let mut cfg = config(&mock);
    cfg.connections_max_idle = Duration::from_millis(40);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ShareGroup::join(cfg, "resub-idle", "t").await.unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 1, "the share session has to be open");
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _listed = group.list_topics().await.unwrap();
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 1);
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_api_versions_delay(delay);
    let before = mock.metadata_calls();
    let versions_before = mock.api_versions_versions().len();
    let started = Instant::now();
    group.subscribe(["t", "u"]).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        mock.metadata_calls() > before,
        "resubscribe should load metadata for the new topic"
    );
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "the idle share leader should reuse the versions already learned"
    );
    assert!(
        mock.share_ack_history()
            .iter()
            .any(|(_, epoch)| *epoch == ShareRequestMetadata::FINAL_EPOCH),
        "resubscribe should retire the previous share session"
    );
    assert!(
        elapsed < Duration::from_millis(450),
        "idle resubscribe loaded metadata before dialing the leader: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// An idle share resubscribe reconnects the bootstrap coordinator without ApiVersions.
#[tokio::test]
async fn share_resubscribe_reuses_coordinator_api_versions_after_idle() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_share_heartbeat_interval_ms(60_000);
    produce(&mock, &[(0, b"held")]).await;
    let mut cfg = config(&mock);
    cfg.connections_max_idle = Duration::from_millis(40);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ShareGroup::join(cfg, "resub-boot", "t").await.unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 1, "the share session has to be open");
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _listed = group.list_topics().await.unwrap();
    mock.set_topic_partitions("u", 1);
    let versions_before = mock.api_versions_versions().len();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    group.subscribe(["t", "u"]).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "the bootstrap coordinator should reuse the versions already learned"
    );
    assert!(
        mock.accepts() > accepts_before,
        "idle expiry replaces the coordinator socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "idle share resubscribe repeated ApiVersions: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// An idle share leader reconnects without ApiVersions.
#[tokio::test]
async fn idle_share_leader_reuses_learned_api_versions() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_share_heartbeat_interval_ms(60_000);
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    produce(&mock, &[(0, b"held")]).await;
    let mut cfg = config(&mock);
    cfg.connections_max_idle = Duration::from_millis(40);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ShareGroup::join(cfg, "share-idle", "t").await.unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(first.len(), 1, "the share session has to be open");
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _listed = group.list_topics().await.unwrap();
    let versions_before = mock.api_versions_versions().len();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let _again = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "an idle share leader should reuse the versions already learned"
    );
    assert!(
        mock.accepts() > accepts_before,
        "idle expiry replaces the share leader socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "idle share fetch repeated ApiVersions: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_join_heartbeat_loads_metadata_while_joining() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_share_heartbeat_interval_ms(60_000);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let mut cfg = config(&mock);
    cfg.metadata_max_age = Duration::ZERO;
    let mut group = ShareGroup::join(cfg, "hb-meta", "t").await.unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 2, "both leaders must open a share session");
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 1);
    let delay = Duration::from_millis(250);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, delay);
    mock.delay_share_reply_once(SHARE_GROUP_HEARTBEAT, delay);
    mock.set_metadata_delay(delay);
    let before = mock.metadata_calls();
    let started = Instant::now();
    group.subscribe(["t", "u"]).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        mock.metadata_calls() >= before + 2,
        "session close and the join heartbeat should each load metadata, got {} then {}",
        before,
        mock.metadata_calls()
    );
    assert!(
        elapsed < Duration::from_millis(650),
        "share join heartbeat waited for metadata: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// Idle share sockets are opened together before the acknowledgements.
#[tokio::test]
async fn share_ack_redials_leaders_together() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let mut cfg = config(&mock);
    cfg.connections_max_idle = Duration::from_millis(40);
    let mut group = ShareGroup::join(cfg, "ack-redial", "t").await.unwrap();
    let got = group.poll().await.unwrap();
    assert_eq!(got.len(), 2, "both leaders must deliver a record");
    tokio::time::sleep(Duration::from_millis(60)).await;
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    group.accept(&got).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "share ack redialed one leader after another: {:?}",
        started.elapsed()
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_leader_dials_overlap() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let mut group = ShareGroup::join(config(&mock), "dial-overlap", "t")
        .await
        .unwrap();
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let got = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(got.len(), 2, "one poll collects both leaders");
    assert!(
        mock.api_versions_versions().len() >= before + 1,
        "the non-bootstrap share leader should negotiate"
    );
    assert!(
        elapsed < Duration::from_millis(400),
        "share leader dials ran one after another: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// Stale share metadata reloads while idle leaders redial.
#[tokio::test]
async fn share_poll_dials_leaders_while_refreshing_metadata() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    produce(&mock, &[(0, b"left"), (1, b"right")]).await;
    let mut cfg = config(&mock);
    cfg.metadata_max_age = Duration::ZERO;
    cfg.connections_max_idle = Duration::from_millis(40);
    let mut group = ShareGroup::join(cfg, "meta-dial", "t").await.unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(first.len(), 2, "both leaders must open a share session");
    group.accept(&first).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    // Refresh the member socket. The share leaders stay idle.
    group.list_topics().await.unwrap();
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_api_versions_delay(delay);
    let started = Instant::now();
    let _second = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(450),
        "share poll refreshed metadata before redialing leaders: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// A not-leader ShareFetch retry reloads metadata while the leader reconnects.
#[tokio::test]
async fn share_fetch_redials_while_refreshing_after_not_leader() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    produce(&mock, &[(0, b"share")]).await;
    let mut group = ShareGroup::join(config(&mock), "share-not-leader", "t")
        .await
        .unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(first.len(), 1);
    group.accept(&first).await.unwrap();
    mock.fail_share_top_once(SHARE_FETCH, 2, error::NOT_LEADER_OR_FOLLOWER);
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_api_versions_delay(delay);
    let started = Instant::now();
    let _again = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(450),
        "share fetch refreshed metadata before redialing after not-leader: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

/// An empty share assignment loads topic metadata while the join heartbeat runs.
#[tokio::test]
async fn share_assignment_loads_metadata_while_heartbeating() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 1);
    mock.set_share_heartbeat_interval_ms(60_000);
    mock.set_share_hold_assignment(true);
    let mut cfg = config(&mock);
    cfg.metadata_max_age = Duration::ZERO;
    cfg.request_timeout = Duration::from_millis(800);
    let mut group = ShareGroup::join(cfg, "share-assign-meta", "t")
        .await
        .unwrap();
    assert!(group.assignment().is_empty());
    mock.set_share_hold_assignment(false);
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.delay_share_reply_once(SHARE_GROUP_HEARTBEAT, delay);
    let started = Instant::now();
    let got = group.poll().await.unwrap();
    let elapsed = started.elapsed();
    assert!(got.is_empty());
    assert_eq!(group.assignment().len(), 1);
    assert!(
        elapsed < Duration::from_millis(650),
        "share assignment refreshed metadata before the heartbeat: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn java_v2_batch_emits_only_acquired_offsets_and_buffers_poll_limit() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 4);
    mock.recreate_topic_id("t", std::array::from_fn(|i| u8::try_from(i).unwrap()));
    mock.inject_share_fetch_response_once(include_bytes!(
        "fixtures/protocol_oracles/share_v2/fetch_v2_acquired_subset_response.bin"
    ));
    let mut cfg = config(&mock);
    cfg.max_poll_records = Some(1);
    let mut group = ShareGroup::join(cfg, "java-subset", "t").await.unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        (first[0].partition, first[0].offset, first[0].delivery_count),
        (3, 1, 3)
    );
    assert_eq!(first[0].key.as_deref(), Some(&b"k1"[..]));
    assert_eq!(first[0].value.as_deref(), Some(&b"v1"[..]));
    assert_eq!(group.acquisition_lock_timeout_ms(), Some(1200));
    assert_eq!(group.acquired_record_count(), 2);
    let calls = mock.share_fetch_calls();
    let second = group.poll().await.unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!((second[0].offset, second[0].delivery_count), (2, 3));
    assert_eq!(
        mock.share_fetch_calls(),
        calls,
        "buffered delivery must not open another acquisition"
    );
    assert!(first
        .iter()
        .chain(second.iter())
        .all(|r| r.offset != 0 && r.offset != 3));
    group.unsubscribe().await.unwrap();
    assert_eq!(group.acquired_record_count(), 0);
    assert_eq!(mock.last_share_ack_epoch(), Some(-1));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_lock_expiry_reacquisition_stale_ack_release_reject_and_group_isolation() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_share_lock_timeout(1000);
    produce(&mock, &[(0, b"owned")]).await;
    let mut first = ShareGroup::join(config(&mock), "locks", "t").await.unwrap();
    let mut second = ShareGroup::join(config(&mock), "locks", "t").await.unwrap();
    let initial = first.poll().await.unwrap();
    assert_eq!(initial[0].delivery_count, 1);
    assert!(
        second.poll().await.unwrap().is_empty(),
        "another member cannot acquire an unexpired lock"
    );
    mock.advance_share_clock(1000);
    let redelivered = second.poll().await.unwrap();
    assert_eq!(
        (redelivered[0].offset, redelivered[0].delivery_count),
        (initial[0].offset, 2)
    );
    assert_broker(
        first.accept(&initial).await.unwrap_err(),
        error::INVALID_RECORD_STATE,
    );
    assert_eq!(first.metrics().records_acknowledged, 0);
    second.release(&redelivered).await.unwrap();
    let third = first.poll().await.unwrap();
    assert_eq!(third[0].delivery_count, 3);
    assert_broker(
        first.accept(&initial).await.unwrap_err(),
        error::INVALID_RECORD_STATE,
    );
    first.reject(&third).await.unwrap();
    assert!(second.poll().await.unwrap().is_empty());
    assert!(mock.share_accepted("locks", "t", 0, 0));
    let mut other_group = ShareGroup::join(config(&mock), "isolated", "t")
        .await
        .unwrap();
    let independent = other_group.poll().await.unwrap();
    assert_eq!(independent[0].delivery_count, 1);
    other_group.accept(&independent).await.unwrap();
    first.leave().await.unwrap();
    second.leave().await.unwrap();
    other_group.leave().await.unwrap();
}

#[tokio::test]
async fn renew_v2_extends_only_selected_record_and_preserves_delivery_count() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_share_lock_timeout(200);
    produce(&mock, &[(0, b"renew"), (0, b"expire")]).await;
    let mut owner = ShareGroup::join(config(&mock), "renew", "t").await.unwrap();
    owner.set_acquire_mode(ShareAcquireMode::RecordLimit);
    let acquired = owner.poll().await.unwrap();
    assert_eq!(acquired.len(), 2);
    assert_eq!(mock.last_share_acquire_mode(), 1);
    // Move both clocks, then renew one record. Only that local/server lock is extended.
    tokio::time::sleep(Duration::from_millis(120)).await;
    mock.advance_share_clock(120);
    owner.renew(&acquired[..1]).await.unwrap();
    assert_eq!(mock.last_share_ack_version(), Some(2));
    assert!(mock.last_share_renew_ack());
    assert_eq!(owner.metrics().records_acknowledged, 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    mock.advance_share_clock(100);
    assert_broker(
        owner.accept(&acquired[1..]).await.unwrap_err(),
        error::INVALID_RECORD_STATE,
    );
    owner.accept(&acquired[..1]).await.unwrap();
    let mut next = ShareGroup::join(config(&mock), "renew", "t").await.unwrap();
    let available = next.poll().await.unwrap();
    assert_eq!(available.len(), 1);
    assert_eq!((available[0].offset, available[0].delivery_count), (1, 2));
    next.accept(&available).await.unwrap();
    owner.leave().await.unwrap();
    next.leave().await.unwrap();
}

#[tokio::test]
async fn partial_terminal_ack_preserves_success_and_unacknowledged_same_partition_record() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    produce(&mock, &[(0, b"success"), (1, b"fail"), (1, b"keep")]).await;
    let mut group = ShareGroup::join(config(&mock), "partial-terminal", "t")
        .await
        .unwrap();
    let acquired = group.poll().await.unwrap();
    assert_eq!(acquired.len(), 3);
    mock.fail_share_ack_once("t", 1, error::INVALID_RECORD_STATE);
    assert_broker(
        group.accept(&acquired[..2]).await.unwrap_err(),
        error::INVALID_RECORD_STATE,
    );
    assert!(mock.share_accepted("partial-terminal", "t", 0, 0));
    assert!(!mock.share_accepted("partial-terminal", "t", 1, 0));
    assert_eq!(group.metrics().records_acknowledged, 1);
    assert_eq!(
        group.acquired_record_count(),
        1,
        "failure invalidates only attempted offsets"
    );
    group.accept(&acquired[2..]).await.unwrap();
    assert_eq!(group.metrics().records_acknowledged, 2);
    assert_eq!(
        mock.share_ack_attempts(),
        vec![
            ("t".into(), 0, 0, 0),
            ("t".into(), 1, 0, 0),
            ("t".into(), 1, 1, 1)
        ]
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn partial_retriable_ack_retries_only_failed_partition() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    produce(&mock, &[(0, b"once"), (1, b"retry")]).await;
    let mut group = ShareGroup::join(config(&mock), "partial-retry", "t")
        .await
        .unwrap();
    let acquired = group.poll().await.unwrap();
    mock.fail_share_ack_once("t", 1, error::REQUEST_TIMED_OUT);
    group.accept(&acquired).await.unwrap();
    assert_eq!(
        mock.share_ack_attempts(),
        vec![
            ("t".into(), 0, 0, 0),
            ("t".into(), 1, 0, 0),
            ("t".into(), 1, 0, 0)
        ]
    );
    assert_eq!(group.metrics().records_acknowledged, 2);
    assert_eq!(group.acquired_record_count(), 0);
    assert!(group.poll().await.unwrap().is_empty());
    group.leave().await.unwrap();
}

#[tokio::test]
async fn leader_change_ack_is_terminal_and_record_is_reacquired_on_new_leader() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 1);
    produce(&mock, &[(0, b"keep-success"), (1, b"move-lock")]).await;
    let mut group = ShareGroup::join(config(&mock), "leader-ack", "t")
        .await
        .unwrap();
    let acquired = group.poll().await.unwrap();
    mock.set_partition_leader("t", 1, 2);
    let calls = mock.share_ack_calls();
    assert_broker(
        group.accept(&acquired).await.unwrap_err(),
        error::NOT_LEADER_OR_FOLLOWER,
    );
    assert_eq!(
        mock.share_ack_calls(),
        calls + 1,
        "leader errors must not replay old locks"
    );
    assert_eq!(group.metrics().records_acknowledged, 1);
    let moved = group.poll().await.unwrap();
    assert_eq!(moved.len(), 1);
    assert_eq!(
        (moved[0].partition, moved[0].offset, moved[0].delivery_count),
        (1, 0, 2)
    );
    assert_eq!(mock.last_share_fetch_node(), Some(2));
    assert_broker(
        group.accept(&acquired[1..]).await.unwrap_err(),
        error::INVALID_RECORD_STATE,
    );
    group.accept(&moved).await.unwrap();
    assert_eq!(group.metrics().records_acknowledged, 2);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn session_reset_restarts_initial_epoch_and_repeated_failure_is_bounded() {
    let mock = common::Mock::start().await;
    v2(&mock);
    produce(&mock, &[(0, b"reset")]).await;
    mock.fail_share_session_once(1, error::SHARE_SESSION_NOT_FOUND);
    mock.fail_share_session_once(1, error::INVALID_SHARE_SESSION_EPOCH);
    mock.fail_share_session_once(1, error::SHARE_SESSION_LIMIT_REACHED);
    let mut group = ShareGroup::join(config(&mock), "session-reset", "t")
        .await
        .unwrap();
    let acquired = group.poll().await.unwrap();
    assert_eq!(acquired.len(), 1);
    assert_eq!(acquired[0].delivery_count, 1);
    assert_eq!(
        mock.share_fetch_history(),
        vec![(1, 0), (1, 0), (1, 0), (1, 0)]
    );
    group.accept(&acquired).await.unwrap();
    group.leave().await.unwrap();

    for _ in 0..100 {
        mock.fail_share_session_once(1, error::SHARE_SESSION_NOT_FOUND);
    }
    let mut cfg = config(&mock);
    cfg.request_timeout = Duration::from_millis(120);
    let mut bounded = ShareGroup::join(cfg, "bounded-reset", "t").await.unwrap();
    let start = Instant::now();
    assert!(matches!(bounded.poll().await.unwrap_err(), Error::Timeout));
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(mock.share_fetch_calls() < 100);
    bounded.leave().await.unwrap();
}

#[tokio::test]
async fn acknowledge_session_error_clears_old_acquisition_without_replaying_accept() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_share_lock_timeout(1000);
    produce(&mock, &[(0, b"ack-session")]).await;
    let mut group = ShareGroup::join(config(&mock), "ack-reset", "t")
        .await
        .unwrap();
    let mut acquired = group.poll().await.unwrap();
    for code in [
        error::SHARE_SESSION_NOT_FOUND,
        error::INVALID_SHARE_SESSION_EPOCH,
    ] {
        mock.fail_share_ack_session_once(1, code);
        let calls = mock.share_ack_calls();
        assert_broker(group.accept(&acquired).await.unwrap_err(), code);
        assert_eq!(mock.share_ack_calls(), calls + 1);
        assert_eq!(group.acquired_record_count(), 0);
        assert_eq!(group.metrics().records_acknowledged, 0);
        assert!(!mock.share_accepted("ack-reset", "t", 0, 0));
        mock.advance_share_clock(1000);
        let old_count = acquired[0].delivery_count;
        acquired = group.poll().await.unwrap();
        assert_eq!(acquired[0].delivery_count, old_count + 1);
        assert_eq!(mock.last_share_fetch_epoch(), Some(0));
    }
    group.accept(&acquired).await.unwrap();
    assert_eq!(group.metrics().records_acknowledged, 1);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_join_retries_only_typed_coordinator_startup_errors() {
    let mock = common::Mock::start().await;
    v2(&mock);
    produce(&mock, &[(0, b"startup")]).await;
    for code in [
        error::COORDINATOR_LOAD_IN_PROGRESS,
        error::COORDINATOR_NOT_AVAILABLE,
        error::NOT_COORDINATOR,
        error::COORDINATOR_LOAD_IN_PROGRESS,
        error::COORDINATOR_NOT_AVAILABLE,
        error::NOT_COORDINATOR,
    ] {
        mock.fail_find_coordinator_once(code);
    }
    let mut group = ShareGroup::join(config(&mock), "startup", "t")
        .await
        .unwrap();
    assert_eq!(mock.find_coordinator_calls(), 7);
    assert!(mock
        .find_coordinator_key_types()
        .iter()
        .all(|kind| *kind == 0));
    let records = group.poll().await.unwrap();
    assert_eq!(records.len(), 1);
    group.accept(&records).await.unwrap();
    group.leave().await.unwrap();

    for terminal in [error::GROUP_AUTHORIZATION_FAILED, error::REQUEST_TIMED_OUT] {
        let peer = common::Mock::start().await;
        v2(&peer);
        peer.fail_find_coordinator_once(terminal);
        let result = ShareGroup::join(config(&peer), "terminal-discovery", "t").await;
        assert!(result.is_err(), "terminal discovery unexpectedly succeeded");
        if let Err(failure) = result {
            assert_broker(failure, terminal);
        }
        assert_eq!(
            peer.find_coordinator_calls(),
            1,
            "only coordinator14/15/16 are retried"
        );
    }
}

#[tokio::test]
async fn share_join_repeated_coordinator_failure_obeys_original_deadline_and_backoff() {
    let mock = common::Mock::start().await;
    v2(&mock);
    for _ in 0..64 {
        mock.fail_find_coordinator_once(error::COORDINATOR_NOT_AVAILABLE);
    }
    let mut cfg = config(&mock);
    cfg.request_timeout = Duration::from_millis(80);
    cfg.retry_backoff = Duration::from_millis(25);
    cfg.retry_backoff_max = cfg.retry_backoff;
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        ShareGroup::join(cfg, "startup-bounded", "t"),
    )
    .await
    .unwrap();
    assert!(
        result.is_err(),
        "persistent coordinator failure unexpectedly succeeded"
    );
    if let Err(failure) = result {
        assert!(matches!(failure, Error::Timeout), "{failure:?}");
    }
    assert!(started.elapsed() >= Duration::from_millis(70));
    let attempts = mock.find_coordinator_calls();
    assert!(
        (1..=12).contains(&attempts),
        "one bootstrap,3passes per discovery,80ms/25ms fixed budget: {attempts}"
    );
    assert_eq!(mock.share_fetch_history().len(), 0);
}

#[tokio::test]
async fn acknowledge_top_level_retry_advances_epoch_without_duplicate_accept() {
    let mock = common::Mock::start().await;
    v2(&mock);
    produce(&mock, &[(0, b"top-retry")]).await;
    let mut group = ShareGroup::join(config(&mock), "top-retry", "t")
        .await
        .unwrap();
    let acquired = group.poll().await.unwrap();
    mock.fail_share_top_once(SHARE_ACKNOWLEDGE, 1, error::REQUEST_TIMED_OUT);
    group.accept(&acquired).await.unwrap();
    assert_eq!(mock.share_ack_history(), [(1, 1), (1, 2)]);
    assert_eq!(mock.share_ack_attempts(), [("t".into(), 0, 0, 0)]);
    assert_eq!(group.metrics().records_acknowledged, 1);
    assert!(group.poll().await.unwrap().is_empty());
    group.leave().await.unwrap();
}

#[tokio::test]
async fn fetch_top_level_error_advances_epoch_and_preserves_prior_acquisition() {
    let mock = common::Mock::start().await;
    v2(&mock);
    produce(&mock, &[(0, b"first"), (0, b"second")]).await;
    let mut cfg = config(&mock);
    cfg.max_poll_records = Some(1);
    let mut group = ShareGroup::join(cfg, "fetch-top", "t").await.unwrap();
    let first = group.poll().await.unwrap();
    mock.fail_share_top_once(SHARE_FETCH, 1, error::GROUP_AUTHORIZATION_FAILED);
    assert_broker(
        group.poll().await.unwrap_err(),
        error::GROUP_AUTHORIZATION_FAILED,
    );
    let second = group.poll().await.unwrap();
    assert_eq!(mock.share_fetch_history(), [(1, 0), (1, 1), (1, 2)]);
    assert_eq!(group.acquired_record_count(), 2);
    let all: Vec<_> = first.iter().chain(second.iter()).cloned().collect();
    group.accept(&all).await.unwrap();
    assert_eq!(group.metrics().records_acknowledged, 2);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn cancelled_share_poll_timeout_keeps_the_configured_wait() {
    let mock = common::Mock::start().await;
    v2(&mock);
    produce(&mock, &[(0, b"kept")]).await;
    let mut cfg = config(&mock);
    cfg.max_wait_ms = 500;
    let mut group = ShareGroup::join(cfg, "wait-kept", "t").await.unwrap();
    let acquired = group.poll().await.unwrap();
    assert_eq!(acquired.len(), 1);
    assert_eq!(mock.last_share_fetch_max_wait_ms(), Some(500));
    group.accept(&acquired).await.unwrap();
    mock.delay_share_reply_once(SHARE_FETCH, Duration::from_millis(400));
    let before = mock.share_fetch_calls();
    let request_sent = async {
        while mock.share_fetch_calls() == before {
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        biased;
        _ = request_sent => {}
        _ = tokio::time::sleep(Duration::from_secs(1)) => {
            panic!("cancelled poll did not send ShareFetch");
        }
        result = group.poll_timeout(Duration::from_millis(40)) => {
            panic!("one-shot poll finished before cancellation: {result:?}");
        }
    }
    assert_eq!(mock.last_share_fetch_max_wait_ms(), Some(40));
    let next = group.poll().await.unwrap();
    assert!(next.is_empty());
    assert_eq!(
        mock.last_share_fetch_max_wait_ms(),
        Some(500),
        "cancelling poll_timeout must not replace the configured wait"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn delayed_share_replies_after_outer_timeout_cannot_satisfy_new_requests() {
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_share_lock_timeout(1000);
    produce(&mock, &[(0, b"late-fetch"), (0, b"late-ack")]).await;
    let mut cfg = config(&mock);
    cfg.max_poll_records = Some(1);
    cfg.request_timeout = Duration::from_millis(60);
    let mut group = ShareGroup::join(cfg, "late-reply", "t").await.unwrap();
    mock.delay_share_reply_once(SHARE_FETCH, Duration::from_millis(180));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), group.poll())
            .await
            .is_err()
    );
    // The server acquired offset0, but the client never decoded it. A new peer
    // must create an initial session and receive offset1, not the stale reply0.
    let fresh = group.poll().await.unwrap();
    assert_eq!(fresh[0].offset, 1);
    assert_eq!(mock.share_fetch_history(), [(1, 0), (1, 0)]);
    mock.delay_share_reply_once(SHARE_ACKNOWLEDGE, Duration::from_millis(180));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), group.accept(&fresh))
            .await
            .is_err()
    );
    assert!(
        mock.share_accepted("late-reply", "t", 0, 1),
        "the delayed response is ambiguous, not proof of failure"
    );
    assert_eq!(group.metrics().records_acknowledged, 0);
    assert!(
        group.poll().await.unwrap().is_empty(),
        "old Ack bytes must not parse as a Fetch response"
    );
    assert_eq!(
        group.acquired_record_count(),
        0,
        "new session must discard ambiguous old metadata"
    );
    mock.advance_share_clock(1000);
    let reclaimed = group.poll().await.unwrap();
    assert_eq!((reclaimed[0].offset, reclaimed[0].delivery_count), (0, 2));
    group.accept(&reclaimed).await.unwrap();
    assert_eq!(group.metrics().records_acknowledged, 1);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn coordinator_move_preserves_delivery_and_partition_leader_session() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    produce(&mock, &[(0, b"before"), (0, b"after")]).await;
    let mut cfg = config(&mock);
    cfg.max_poll_records = Some(1);
    let mut group = ShareGroup::join(cfg, "coordinator-move", "t")
        .await
        .unwrap();
    let before = group.poll().await.unwrap();
    assert_eq!(before[0].value.as_deref(), Some(&b"before"[..]));
    mock.move_coordinator();
    common::wait_pred("share heartbeat moved coordinator", || {
        mock.membership_heartbeats_on(2) >= 1
    })
    .await;
    group.accept(&before).await.unwrap();
    let after = group.poll().await.unwrap();
    assert_eq!(after[0].value.as_deref(), Some(&b"after"[..]));
    assert_eq!(after[0].delivery_count, 1);
    assert_eq!(mock.last_share_fetch_node(), Some(2));
    assert_eq!(mock.last_share_fetch_epoch(), Some(2));
    group.accept(&after).await.unwrap();
    assert_eq!(group.metrics().records_acknowledged, 2);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn each_partition_leader_negotiates_its_own_share_version_and_v2_gate() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    mock.set_node_api_max(2, SHARE_FETCH, 0);
    mock.set_node_api_max(2, SHARE_ACKNOWLEDGE, 0);
    produce(&mock, &[(0, b"v2"), (1, b"v0")]).await;
    let mut group = ShareGroup::join(config(&mock), "mixed-versions", "t")
        .await
        .unwrap();
    let acquired = group.poll().await.unwrap();
    assert_eq!(acquired.len(), 2);
    assert_eq!(mock.share_fetch_version_on(1), Some(2));
    assert_eq!(mock.share_fetch_version_on(2), Some(0));
    let old: Vec<_> = acquired
        .iter()
        .filter(|r| r.partition == 1)
        .cloned()
        .collect();
    let calls = mock.share_ack_calls();
    assert!(matches!(
        group.renew(&old).await.unwrap_err(),
        Error::Unsupported(_)
    ));
    assert_eq!(mock.share_ack_calls(), calls);
    group.accept(&acquired).await.unwrap();
    group.set_acquire_mode(ShareAcquireMode::RecordLimit);
    assert!(matches!(
        group.poll().await.unwrap_err(),
        Error::Unsupported(_)
    ));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn older_bootstrap_does_not_cap_current_partition_leader_fields() {
    let mock = common::Mock::start_two_node().await;
    v2(&mock);
    mock.set_node_api_max(1, SHARE_FETCH, 1);
    mock.set_node_api_max(1, SHARE_ACKNOWLEDGE, 1);
    produce(&mock, &[(0, b"target-v2")]).await;
    let mut group = ShareGroup::join(config(&mock), "older-bootstrap", "t")
        .await
        .unwrap();
    group.set_acquire_mode(ShareAcquireMode::RecordLimit);
    let acquired = group.poll().await.unwrap();
    assert_eq!(mock.last_share_fetch_node(), Some(2));
    assert_eq!(mock.last_share_fetch_version(), Some(2));
    assert_eq!(mock.last_share_acquire_mode(), 1);
    group.renew(&acquired).await.unwrap();
    assert_eq!(mock.last_share_ack_version(), Some(2));
    assert!(mock.last_share_renew_ack());
    group.accept(&acquired).await.unwrap();
    group.leave().await.unwrap();

    let old = common::Mock::start().await;
    let mut group = ShareGroup::join(config(&old), "old-target", "t")
        .await
        .unwrap();
    group.set_acquire_mode(ShareAcquireMode::RecordLimit);
    assert!(matches!(
        group.poll().await.unwrap_err(),
        Error::Unsupported(_)
    ));
    assert_eq!(
        old.share_fetch_calls(),
        0,
        "old target rejects before emitting a v2-only acquisition"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn unsubscribe_releases_lock_and_resubscribe_restarts_delivery_heartbeats() {
    let mock = common::Mock::start().await;
    v2(&mock);
    produce(&mock, &[(0, b"leave")]).await;
    let mut group = ShareGroup::join(config(&mock), "unsubscribe", "t")
        .await
        .unwrap();
    let initial = group.poll().await.unwrap();
    group.unsubscribe().await.unwrap();
    assert_eq!(group.acquired_record_count(), 0);
    assert!(group.subscription().is_empty());
    assert_eq!(mock.last_share_ack_epoch(), Some(-1));
    group.subscribe(["t"]).await.unwrap();
    let again = group.poll().await.unwrap();
    assert_eq!(
        (again[0].offset, again[0].delivery_count),
        (initial[0].offset, 2)
    );
    group.accept(&again).await.unwrap();
    let calls = mock.share_heartbeat_calls();
    common::wait_pred("resubscribed share heartbeat", || {
        mock.share_heartbeat_calls() > calls
    })
    .await;
    group.leave().await.unwrap();
}

#[tokio::test]
async fn decoded_budget_and_overlapping_acquisitions_fail_before_delivery() {
    let fixture =
        include_bytes!("fixtures/protocol_oracles/share_v2/fetch_v2_acquired_subset_response.bin");
    assert!(
        decode_share_fetch_response_with_limit(&mut Bytes::copy_from_slice(fixture), 2, 1).is_err()
    );
    let mut malicious = BytesMut::from(&fixture[..]);
    // Compact Responses count after throttle/code/null message/lock: many entries
    // must fail the storage budget before allocating a topic vector.
    malicious[11] = 0xff;
    assert!(decode_share_fetch_response_with_limit(&mut malicious.freeze(), 2, 64).is_err());
    let (mut topics, _, _, _, _, _) =
        decode_share_fetch_response(&mut Bytes::copy_from_slice(fixture), 2).unwrap();
    topics[0].partitions[0].acquired.push(AcquiredRange {
        first_offset: 1,
        last_offset: 1,
        delivery_count: 3,
    });
    let mut overlap = BytesMut::new();
    encode_share_fetch_response_with_acquisition_lock_timeout(&mut overlap, 2, &topics, 1200)
        .unwrap();
    let mock = common::Mock::start().await;
    v2(&mock);
    mock.set_topic_partitions("t", 4);
    mock.recreate_topic_id("t", std::array::from_fn(|i| u8::try_from(i).unwrap()));
    mock.inject_share_fetch_response_once(&overlap);
    let mut group = ShareGroup::join(config(&mock), "overlap", "t")
        .await
        .unwrap();
    assert!(matches!(
        group.poll().await.unwrap_err(),
        Error::Protocol(_)
    ));
    assert_eq!(group.metrics().records_fetched, 0);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn outstanding_acquisitions_consume_budget_across_successive_polls() {
    let mock = common::Mock::start().await;
    v2(&mock);
    let entries: Vec<_> = (0..32).map(|_| (0, &b"bounded"[..])).collect();
    produce(&mock, &entries).await;
    let mut cfg = config(&mock);
    cfg.buffer_memory = 1400;
    cfg.max_poll_records = Some(1);
    let mut group = ShareGroup::join(cfg, "acquisition-budget", "t")
        .await
        .unwrap();
    let mut delivered = 0;
    loop {
        match group.poll().await {
            Ok(records) => {
                assert_eq!(records.len(), 1);
                delivered += 1;
                assert!(
                    delivered < 32,
                    "outstanding metadata must consume the allowance"
                );
            }
            Err(Error::Protocol(message)) => {
                assert!(message.contains("budget"), "{message}");
                break;
            }
            Err(error) => panic!("unexpected budget outcome: {error:?}"),
        }
    }
    assert!(delivered > 0);
    assert_eq!(group.metrics().records_fetched, delivered);
    assert_eq!(
        group.acquired_record_count(),
        0,
        "failed decoded response resets its ambiguous session"
    );
    group.unsubscribe().await.unwrap();
    assert_eq!(group.acquired_record_count(), 0);
    group.leave().await.unwrap();
}

async fn live_poll(group: &mut ShareGroup, stage: &str) -> partitionline::ShareRecords {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let records = group.poll().await.unwrap();
            if !records.is_empty() {
                return records;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|failure| format!("live Share stage {stage} timed out: {failure:?}"))
    .unwrap()
}

fn check_live_record(record: &ShareRecord, topic: &str) {
    assert_eq!(record.topic, topic);
    assert_eq!(record.partition, 0);
    assert!((0..4).contains(&record.offset));
    assert_eq!(record.timestamp, 1000 + record.offset);
    assert_eq!(
        record.key.as_deref(),
        Some(format!("key-{}", record.offset).as_bytes())
    );
    assert_eq!(
        record.value.as_deref(),
        Some(format!("value-{}", record.offset).as_bytes())
    );
    assert!(record.delivery_count > 0);
}

/// Separate from fault-controlled peer tests: this requires a fresh real image
/// and exact immutable source identity, never an unsupported disposition.
#[tokio::test]
#[ignore = "requires an explicitly selected fresh digest-pinned Apache Kafka 4.3.1 Docker cell"]
async fn live_current_v2_share_delivery_renew_release_expiry_accept() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap();
        let reference = std::env::var("PL_COMPAT_REFERENCE").unwrap();
        let source = std::env::var("PL_COMPAT_SOURCE_SHA").unwrap();
        let prefix = std::env::var("PL_SHARE_PREFIX").unwrap();
        assert_eq!(reference, "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837");
        assert_eq!(source.len(), 40);
        assert!(source.bytes().all(|b| b.is_ascii_hexdigit()));
        let lock_ms = std::env::var("PL_SHARE_LOCK_MS").map_or(15000, |value| value.parse::<u64>().unwrap());
        assert!((1000..=15000).contains(&lock_ms));
        let topic = format!("{prefix}-records");
        let group_id = format!("{prefix}-group");
        let mut admin = Admin::connect(bootstrap.clone()).await.unwrap();
        let fetch = admin.versions().get(&SHARE_FETCH).unwrap();
        let ack = admin.versions().get(&SHARE_ACKNOWLEDGE).unwrap();
        assert_eq!((fetch.min_version, fetch.max_version, ack.min_version, ack.max_version), (1, 2, 1, 2));
        let features = admin.describe_features().await.unwrap();
        let levels: std::collections::BTreeMap<_, _> = features.finalized_features.iter().map(|f| (f.name.clone(), [f.min_version_level, f.max_version_level])).collect();
        assert!(levels.get("share.version").is_some_and(|v| v[0] >= 1));
        for result in admin.create_topics(&[NewTopic::new(&topic, 1, 1).config("retention.ms", "-1")], 10000, false).await.unwrap() {
            assert_eq!(result.error_code, 0);
        }
        let changes = [ConfigResourceUpdate::new(ConfigResource::group(&group_id), [
            AlterConfig::set("share.auto.offset.reset", "earliest"),
            AlterConfig::set("share.record.lock.duration.ms", lock_ms.to_string()),
        ])];
        for result in admin.incremental_alter_configs_for(&changes, false).await.unwrap() { assert_eq!(result.error_code, 0); }
        let producer = Producer::new(ProducerConfig::bootstrap([bootstrap.clone()]).linger(Duration::from_secs(2))).await.unwrap();
        producer.partitions_for(&topic).await.unwrap();
        // Wait for connection readiness, queue all four, then await their batch
        // acknowledgement. RecordLimit must acquire one record from this batch.
        let produced = producer.send_all((0..4).map(|index| ProduceRecord::to(topic.clone()).partition(0).timestamp(1000 + index).key(format!("key-{index}").into_bytes()).value(format!("value-{index}").into_bytes()))).await.unwrap();
        assert_eq!(produced.iter().map(|record| (record.partition, record.offset)).collect::<Vec<_>>(), vec![(0, 0), (0, 1), (0, 2), (0, 3)]);
        producer.flush().await.unwrap();
        producer.close().await.unwrap();
        let cfg = ConsumerConfig::bootstrap([bootstrap.clone()]).max_wait_ms(100).max_poll_records(1).request_timeout(Duration::from_secs(3));
        let mut owner = ShareGroup::join(cfg.clone(), &group_id, &topic).await.unwrap();
        let mut next = ShareGroup::join(cfg, &group_id, &topic).await.unwrap();
        owner.set_acquire_mode(ShareAcquireMode::RecordLimit);
        next.set_acquire_mode(ShareAcquireMode::RecordLimit);
        let held = live_poll(&mut owner, "initial-acquisition").await;
        assert_eq!(held.len(), 1);
        check_live_record(&held[0], &topic);
        assert_eq!(owner.acquired_record_count(), 1, "v2 record limit must constrain acquisition across a full batch");
        assert_eq!(held[0].delivery_count, 1);
        assert_eq!(owner.acquisition_lock_timeout_ms(), Some(i32::try_from(lock_ms).unwrap()));
        owner.renew(&held).await.unwrap();
        assert_eq!(owner.acquired_record_count(), 1);
        assert_eq!(owner.metrics().records_acknowledged, 0);
        let mut accepted = std::collections::BTreeSet::new();
        // Drain available neighbours while proving another member cannot acquire
        // the held offset before its acquisition lock expires.
        loop {
            let records = next.poll().await.unwrap();
            if records.is_empty() { break; }
            assert_eq!(records.len(), 1);
            assert_eq!(next.acquired_record_count(), 1);
            check_live_record(&records[0], &topic);
            assert_ne!(records[0].offset, held[0].offset);
            assert_eq!(records[0].delivery_count, 1);
            assert!(accepted.insert(records[0].offset));
            next.accept(&records).await.unwrap();
        }
        assert_eq!(accepted.len(), 3);
        tokio::time::sleep(Duration::from_millis(lock_ms + 200)).await;
        let expired = live_poll(&mut next, "expired-lock-reacquisition").await;
        check_live_record(&expired[0], &topic);
        assert_eq!((expired[0].offset, expired[0].delivery_count), (held[0].offset, 2));
        next.release(&expired).await.unwrap();
        let released = live_poll(&mut owner, "released-record-reacquisition").await;
        check_live_record(&released[0], &topic);
        assert_eq!((released[0].offset, released[0].delivery_count), (held[0].offset, 3));
        assert_broker(owner.accept(&held).await.unwrap_err(), error::INVALID_RECORD_STATE);
        assert!(accepted.insert(released[0].offset));
        owner.accept(&released).await.unwrap();
        assert_eq!(accepted, [0, 1, 2, 3].into_iter().collect());
        assert!(next.poll().await.unwrap().is_empty(), "accepted records cannot be reacquired");
        owner.leave().await.unwrap();
        next.leave().await.unwrap();
        eprintln!("SHARE_V2_LIVE_REPORT {{\"schema_version\":1,\"source_sha\":\"{source}\",\"reference\":\"{reference}\",\"finalized_features\":{levels:?},\"runtime_version\":2,\"acquire_mode\":\"record_limit\",\"lock_ms\":{lock_ms},\"accepted_offsets\":[0,1,2,3],\"release_delivery_count\":3,\"expiry_delivery_count\":2,\"renew\":\"successful\",\"disposition\":\"supported\"}}");
    }).await.unwrap();
}
