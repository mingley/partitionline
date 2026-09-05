//! KL-02 slice: consumer leave/close must not auto-commit unprocessed offsets.
//!
//! Poll-interval auto-commit and explicit `commit*` remain the OffsetCommit
//! paths for positions. Leave/close/unsubscribe flush `commitAsync` only.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]

mod common;

use partitionline::{ConsumerConfig, ConsumerGroup, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn default_leave_does_not_offset_commit() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "default-leave",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 2);
    let before = mock.offset_commit_calls();
    group.leave().await.unwrap();
    assert_eq!(
        mock.offset_commit_calls(),
        before,
        "default leave must not OffsetCommit positions"
    );

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "default-leave",
        "t",
    )
    .await
    .unwrap();
    let again = group.poll().await.unwrap();
    assert_eq!(
        again.len(),
        2,
        "without commit, rejoin must still see polled records"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn auto_commit_on_leave_with_long_interval_does_not_commit() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send(ProduceRecord::to("t").value(&b"x"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_commit(true)
            .auto_commit_interval(Duration::from_secs(3600)),
        "leave-no-ac",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    let before = mock.offset_commit_calls();
    group.leave().await.unwrap();
    assert_eq!(
        mock.offset_commit_calls(),
        before,
        "leave must not auto-commit when poll interval has not elapsed"
    );

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "leave-no-ac",
        "t",
    )
    .await
    .unwrap();
    let again = group.poll().await.unwrap();
    assert_eq!(
        again.len(),
        1,
        "polled-but-unprocessed record must remain after leave without commit"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn auto_commit_on_poll_interval_still_commits() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send_all([
            ProduceRecord::to("t").value(&b"old"[..]),
            ProduceRecord::to("t").value(&b"new"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_commit(true)
            .auto_commit_interval(Duration::ZERO),
        "poll-ac",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 2);
    assert!(
        mock.offset_commit_calls() >= 1,
        "ZERO interval auto-commit must OffsetCommit on poll"
    );
    group.leave().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "poll-ac",
        "t",
    )
    .await
    .unwrap();
    let again = group.poll().await.unwrap();
    assert!(
        again.is_empty(),
        "poll-interval auto-commit must store the high watermark"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn unsubscribe_with_auto_commit_does_not_commit_positions() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send(ProduceRecord::to("t").value(&b"u"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_commit(true)
            .auto_commit_interval(Duration::from_secs(3600)),
        "unsub-no-ac",
        "t",
    )
    .await
    .unwrap();
    assert_eq!(group.poll().await.unwrap().len(), 1);
    let before = mock.offset_commit_calls();
    group.unsubscribe().await.unwrap();
    assert_eq!(
        mock.offset_commit_calls(),
        before,
        "unsubscribe must not auto-commit positions"
    );
}
