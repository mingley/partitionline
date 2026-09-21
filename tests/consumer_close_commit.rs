//! KL-02 slice: consumer leave/close must not auto-commit unprocessed offsets.
//!
//! Poll-interval auto-commit and explicit `commit*` remain the OffsetCommit
//! paths for positions. Leave/close/unsubscribe flush `commitAsync` only.

mod common;

use partitionline::{
    ConsumerConfig, ConsumerGroup, OffsetAndMetadata, ProduceRecord, Producer, ProducerConfig,
    TopicPartition,
};
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
async fn zero_interval_does_not_commit_before_first_application_delivery() {
    let mock = common::Mock::start().await;
    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_commit(true)
            .auto_commit_interval(Duration::ZERO),
        "zero-no-delivery",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll_timeout(Duration::from_millis(10)).await.unwrap();
    assert!(recs.is_empty());
    assert_eq!(
        mock.offset_commit_calls(),
        0,
        "zero interval must not commit records before the first application delivery"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn expired_interval_does_not_commit_before_first_application_delivery() {
    let mock = common::Mock::start().await;
    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_commit(true)
            .auto_commit_interval(Duration::from_millis(1)),
        "expired-no-delivery",
        "t",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let recs = group.poll_timeout(Duration::from_millis(10)).await.unwrap();
    assert!(recs.is_empty());
    assert_eq!(
        mock.offset_commit_calls(),
        0,
        "expired interval must not commit records before the first application delivery"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn interval_auto_commit_does_not_commit_batch_about_to_be_returned() {
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
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_commit(true)
            .auto_commit_interval(Duration::from_millis(1)),
        "interval-ac",
        "t",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;

    // First poll returns the records; interval auto-commit must NOT commit the batch about to be returned.
    let first = group.poll().await.unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(
        mock.offset_commit_calls(),
        0,
        "first poll must not commit the batch about to be returned"
    );

    // Sleep so the interval elapses again.
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Second poll: now commits the previously delivered positions from the first poll.
    let second = group.poll_timeout(Duration::from_millis(10)).await.unwrap();
    assert!(second.is_empty());
    assert!(
        mock.offset_commit_calls() >= 1,
        "second poll must auto-commit previously delivered work"
    );
    group.leave().await.unwrap();

    // Rejoin: all records committed, so next consumer sees empty poll.
    let mut replacement = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "interval-ac",
        "t",
    )
    .await
    .unwrap();
    let remaining = replacement.poll().await.unwrap();
    replacement.leave().await.unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn capped_poll_crash_with_interval_auto_commit_does_not_skip_prefetched_records() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
            ProduceRecord::to("t").value(&b"c"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()])
        .max_wait_ms(10)
        .auto_commit(true)
        .auto_commit_interval(Duration::from_millis(1));
    cfg.max_poll_records = Some(1);
    let mut group = ConsumerGroup::join(cfg, "crash-capped", "t").await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;

    // First capped poll delivers record 0 and prefetches records 1 and 2.
    // Interval auto-commit must NOT commit the batch about to be returned.
    let first = group.poll().await.unwrap();
    assert_eq!(first.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![0]);
    assert_eq!(
        mock.offset_commit_calls(),
        0,
        "capped poll must not auto-commit the returned batch"
    );

    // Simulate crash/stop without commit.
    group.leave().await.unwrap();

    // Rejoin: since nothing was committed, replacement consumer starts at offset 0.
    // Prefetched records 1 and 2 (and record 0) cannot be skipped on rejoin.
    let mut replacement = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "crash-capped",
        "t",
    )
    .await
    .unwrap();
    let remaining = replacement.poll().await.unwrap();
    replacement.leave().await.unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
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

#[tokio::test]
async fn commit_after_capped_poll_must_not_commit_buffered_records() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
            ProduceRecord::to("t").value(&b"c"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false);
    cfg.max_poll_records = Some(1);
    let mut group = ConsumerGroup::join(cfg, "audit-capped-poll", "t")
        .await
        .unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(
        first.iter().map(|record| record.offset).collect::<Vec<_>>(),
        vec![0]
    );
    group.commit().await.unwrap();
    group.leave().await.unwrap();

    let mut replacement = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false),
        "audit-capped-poll",
        "t",
    )
    .await
    .unwrap();
    let remaining = replacement.poll().await.unwrap();
    replacement.leave().await.unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test]
async fn commit_after_capped_poll_with_pause_and_resume() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
            ProduceRecord::to("t").value(&b"c"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false);
    cfg.max_poll_records = Some(1);
    let mut group = ConsumerGroup::join(cfg, "audit-pause-resume", "t")
        .await
        .unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(first.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![0]);
    assert_eq!(group.position("t", 0).unwrap(), 1);
    assert_eq!(group.fetch_cursor("t", 0).unwrap(), 3);

    group.pause([TopicPartition::new("t", 0)]);
    assert_eq!(group.paused(), vec![TopicPartition::new("t", 0)]);

    // Polling while paused returns no records
    let paused_poll = group.poll().await.unwrap();
    assert!(paused_poll.is_empty());
    // Delivered position remains at offset 1 while paused
    assert_eq!(group.position("t", 0).unwrap(), 1);

    // Commit while paused commits only the delivered position (1)
    group.commit().await.unwrap();

    group.resume([("t", 0)]);
    assert!(group.paused().is_empty());

    // Resume delivers next buffered record (offset 1)
    let second = group.poll().await.unwrap();
    assert_eq!(second.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![1]);
    assert_eq!(group.position("t", 0).unwrap(), 2);
    group.commit().await.unwrap();

    // Next poll delivers last buffered record (offset 2)
    let third = group.poll().await.unwrap();
    assert_eq!(third.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![2]);
    assert_eq!(group.position("t", 0).unwrap(), 3);
    group.commit().await.unwrap();
    group.leave().await.unwrap();

    // Rejoin: all records committed, poll is empty
    let mut replacement = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false),
        "audit-pause-resume",
        "t",
    )
    .await
    .unwrap();
    let remaining = replacement.poll().await.unwrap();
    replacement.leave().await.unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn commit_after_capped_poll_with_seek() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send_all([
            ProduceRecord::to("t").value(&b"0"[..]),
            ProduceRecord::to("t").value(&b"1"[..]),
            ProduceRecord::to("t").value(&b"2"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false);
    cfg.max_poll_records = Some(1);
    let mut group = ConsumerGroup::join(cfg, "audit-seek", "t").await.unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(first.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![0]);
    assert_eq!(group.position("t", 0).unwrap(), 1);

    // Seek drops pending buffered records (1, 2) and sets position to 2
    group.seek("t", 0, 2).unwrap();
    assert_eq!(group.position("t", 0).unwrap(), 2);
    assert_eq!(group.fetch_cursor("t", 0).unwrap(), 2);

    group.commit().await.unwrap();
    group.leave().await.unwrap();

    // Rejoin resumes from committed offset 2
    let mut replacement = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false),
        "audit-seek",
        "t",
    )
    .await
    .unwrap();
    let remaining = replacement.poll().await.unwrap();
    replacement.leave().await.unwrap();
    assert_eq!(
        remaining.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![2]
    );
}

#[tokio::test]
async fn commit_with_metadata_preserves_explicit_offsets() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send_all([
            ProduceRecord::to("t").value(&b"0"[..]),
            ProduceRecord::to("t").value(&b"1"[..]),
            ProduceRecord::to("t").value(&b"2"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false);
    cfg.max_poll_records = Some(1);
    let mut group = ConsumerGroup::join(cfg, "audit-explicit-md", "t")
        .await
        .unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(first.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![0]);

    // Explicitly commit offset 2 with metadata, ignoring delivered position 1
    group
        .commit_with_metadata([(
            TopicPartition::new("t", 0),
            OffsetAndMetadata::with_metadata(2, "custom-metadata").with_leader_epoch(0),
        )])
        .await
        .unwrap();
    group.leave().await.unwrap();

    // Rejoin: should start at committed offset 2
    let mut replacement = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false),
        "audit-explicit-md",
        "t",
    )
    .await
    .unwrap();
    let remaining = replacement.poll().await.unwrap();
    replacement.leave().await.unwrap();
    assert_eq!(
        remaining.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![2]
    );
}

#[tokio::test]
async fn rebalance_preserves_buffered_records_for_retained_partitions() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send_all([
            ProduceRecord::to("t").value(&b"0"[..]),
            ProduceRecord::to("t").value(&b"1"[..]),
            ProduceRecord::to("t").value(&b"2"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false);
    cfg.max_poll_records = Some(1);
    let mut group = ConsumerGroup::join(cfg, "audit-rebalance", "t")
        .await
        .unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(first.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![0]);
    assert_eq!(group.position("t", 0).unwrap(), 1);
    assert_eq!(group.fetch_cursor("t", 0).unwrap(), 3);

    // Trigger rebalance; partition "t"-0 is retained by the single consumer
    group.enforce_rebalance();

    // Poll re-joins group and retains buffered records 1 and 2
    let second = group.poll().await.unwrap();
    assert_eq!(second.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![1]);
    assert_eq!(group.position("t", 0).unwrap(), 2);

    let third = group.poll().await.unwrap();
    assert_eq!(third.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![2]);
    assert_eq!(group.position("t", 0).unwrap(), 3);

    group.commit().await.unwrap();
    group.leave().await.unwrap();

    // Rejoin: everything committed
    let mut replacement = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).auto_commit(false),
        "audit-rebalance",
        "t",
    )
    .await
    .unwrap();
    let remaining = replacement.poll().await.unwrap();
    replacement.leave().await.unwrap();
    assert!(remaining.is_empty());
}
