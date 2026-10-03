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
use partitionline::protocol::api_keys::{SHARE_ACKNOWLEDGE, SHARE_FETCH};
use partitionline::protocol::share::{
    decode_share_fetch_response, decode_share_fetch_response_with_limit,
    encode_share_fetch_response_with_acquisition_lock_timeout, AcquiredRange,
};
use partitionline::share::ShareAcquireMode;
use partitionline::{
    error, Admin, AlterConfig, ConfigResource, ConfigResourceUpdate, ConsumerConfig, Error,
    NewTopic, ProduceRecord, Producer, ProducerConfig, ShareGroup, ShareRecord,
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

fn assert_broker(error: Error, expected: i16) {
    assert!(
        matches!(error, Error::Broker { code, .. } if code == expected),
        "{error:?}"
    );
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

async fn live_poll(group: &mut ShareGroup) -> partitionline::ShareRecords {
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
        for result in admin.create_topics(&[NewTopic::new(&topic, 1, 1)], 10000, false).await.unwrap() {
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
        let held = live_poll(&mut owner).await;
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
        let expired = live_poll(&mut next).await;
        check_live_record(&expired[0], &topic);
        assert_eq!((expired[0].offset, expired[0].delivery_count), (held[0].offset, 2));
        next.release(&expired).await.unwrap();
        let released = live_poll(&mut owner).await;
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
