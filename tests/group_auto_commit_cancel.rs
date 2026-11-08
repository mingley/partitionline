//! Cancellation while an ordinary auto-commit and fetch overlap.

mod common;

use std::time::Duration;

use partitionline::{
    ConsumerConfig, ConsumerGroup, IsolationLevel, ProduceRecord, Producer, ProducerConfig,
};

#[tokio::test]
async fn cancelled_interval_commit_poll_redelivers_fetched_record() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"first"[..]))
        .await
        .unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .isolation(IsolationLevel::ReadUncommitted)
            .request_timeout(Duration::from_secs(8))
            .max_wait_ms(10)
            .auto_commit(true)
            .auto_commit_interval(Duration::from_millis(1)),
        "interval-cancel",
        "t",
    )
    .await
    .unwrap();

    let first = group.poll().await.unwrap();
    assert_eq!(first.iter().map(|r| r.offset).collect::<Vec<_>>(), [0]);
    assert_eq!(group.metrics().records_fetched, 1);

    producer
        .send(ProduceRecord::to("t").value(&b"second"[..]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    mock.set_offset_commit_delay(Duration::from_secs(3));

    let cancelled = tokio::time::timeout(Duration::from_millis(400), group.poll()).await;
    assert!(cancelled.is_err(), "the commit must still be in flight");
    assert_eq!(
        mock.offset_commit_calls(),
        0,
        "the delayed coordinator must not have completed the commit"
    );
    assert_eq!(
        group.metrics().records_fetched,
        2,
        "the second fetch must complete before the poll is cancelled"
    );
    let position_after_cancel = group.position("t", 0).unwrap();

    mock.set_offset_commit_delay(Duration::ZERO);
    let retry = tokio::time::timeout(
        Duration::from_secs(7),
        group.poll_timeout(Duration::from_millis(100)),
    )
    .await
    .expect("retry poll should finish")
    .unwrap();
    let offsets = retry.iter().map(|r| r.offset).collect::<Vec<_>>();
    eprintln!(
        "cancel witness: position after cancellation={position_after_cancel}, retry offsets={offsets:?}"
    );
    assert_eq!(
        offsets,
        [1],
        "a cancelled poll must redeliver the second record"
    );

    group.leave().await.unwrap();
    producer.close().await.unwrap();
}
