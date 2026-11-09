//! Controlled stale rollback and its effect on a public producer send.
#![cfg(stale_identity_witness)]
mod common;

use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn stale_rollback_must_not_reuse_a_new_generation_sequence() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("stale-rollback", 2);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .batch_records(1)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(3))
            .delivery_timeout(Duration::from_secs(5)),
    )
    .await
    .expect("connect");
    let warm = producer
        .send(ProduceRecord::to("stale-rollback").partition(1).value(&b"warm"[..]))
        .await
        .expect("warmup");
    assert_eq!(warm.offset, 0);
    let pid = producer.__test_producer_id();
    let epoch = producer.__test_producer_epoch();
    assert!(pid >= 0 && epoch >= 0);
    let after = producer.__test_stale_identity_rollback("stale-rollback", 0);
    mock.__test_seed_expected_sequence(pid, epoch, "stale-rollback", 0, 1);
    let sender = producer.clone();
    let task = tokio::spawn(async move {
        sender.send(ProduceRecord::to("stale-rollback").partition(0).value(&b"target"[..])).await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if mock.produce_sequences().len() >= 2 { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("target Produce was not observed");
    let wire = mock.produce_sequences();
    let first_base = wire[1].2;
    println!("STALE_IDENTITY after={after} first_base={first_base} identity={pid}/{epoch}");
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    let _ = producer.close_timeout(Duration::from_secs(2)).await;
    assert_eq!(after, 1, "stale rollback rewound the new generation");
    assert_eq!(first_base, 1, "public send reused the new generation sequence");
}
