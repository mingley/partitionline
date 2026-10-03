//! KL05-10: real framed mock-socket histories for the opt-in bounded policy.
//! Accounting is Rust conservative admitted record bytes plus one 61-byte
//! overhead per new cohort, not Java accumulator/compression byte identity.
mod common;

use partitionline::error;
use partitionline::partitioner::{StickyPartitioner, StickyPartitionerConfig};
use partitionline::producer::PreSendFault;
use partitionline::protocol::records::Record;
use partitionline::{partition_for_key, Error, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[expect(
    clippy::unwrap_used,
    reason = "bounded test record sizing must succeed and fit the accounting type"
)]
fn unkeyed_packed_bytes(value: &'static [u8]) -> u64 {
    let record = Record {
        offset: 0,
        timestamp: 0,
        key: None,
        value: Some(bytes::Bytes::from_static(value)),
        headers: Vec::new(),
    };
    u64::try_from(record.record_size_upper_bound().unwrap()).unwrap()
}

async fn admit(producer: &Producer, record: ProduceRecord) -> partitionline::Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            match producer.try_send(record.clone()) {
                Ok(()) => return Ok(()),
                Err(Error::QueueFull) => sleep(Duration::from_millis(2)).await,
                Err(err) => return Err(err),
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)?
}

async fn warm(producer: &Producer) -> partitionline::Result<()> {
    // Explicit partition warms the worker without consuming an unkeyed draw.
    admit(
        producer,
        ProduceRecord::to("t").partition(0).value(&b"warm"[..]),
    )
    .await?;
    producer.flush().await
}

#[tokio::test]
async fn sticky_burst_is_one_real_batch_and_partial_flush_preserves_partition() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 6);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(79443))
            .linger(Duration::from_secs(30))
            .batch_bytes(100_000)
            .batch_records(1000),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let first = mock.produce_batches().len();
    for _ in 0..9 {
        producer
            .try_send(ProduceRecord::to("t").value(&b"first"[..]))
            .unwrap();
    }
    producer.flush().await.unwrap();
    let burst: Vec<_> = mock.produce_batches().into_iter().skip(first).collect();
    assert_eq!(burst.len(), 1);
    assert_eq!(burst.first().map(|batch| batch.2), Some(9));
    let chosen = burst.first().map(|batch| batch.1).unwrap();
    let before = producer.__test_sticky_state().unwrap();
    assert_eq!(before.1, 0);
    producer
        .try_send(ProduceRecord::to("t").value(&b"second"[..]))
        .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(
        mock.produce_batches().last().map(|batch| batch.1),
        Some(chosen)
    );
    let first_bytes = unkeyed_packed_bytes(b"first");
    let second_bytes = unkeyed_packed_bytes(b"second");
    assert_eq!(before.4, first_bytes * 9 + 61);
    assert_eq!(
        producer.__test_sticky_state().unwrap().4,
        before.4 + second_bytes + 61
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn mixed_keyed_and_explicit_records_share_real_tail_without_unkeyed_charge() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 4);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(10))
            .linger(Duration::from_secs(30))
            .batch_bytes(100_000),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let keyed = producer.send_all([
        ProduceRecord::to("t").key(&b""[..]).value(&b"empty"[..]),
        ProduceRecord::to("t").key(&b"key"[..]).value(&b"keyed"[..]),
        ProduceRecord::to("t").partition(3).value(&b"explicit"[..]),
    ]);
    tokio::pin!(keyed);
    tokio::select! {
        result = &mut keyed => panic!("long linger completed before flush: {result:?}"),
        _ = common::wait_pred("all keyed admissions", || producer.metrics().records_queued >= 4) => {}
    }
    assert_eq!(producer.__test_sticky_state().unwrap().4, 0);
    producer.flush().await.unwrap();
    let acknowledgements = keyed.await.unwrap();
    assert_eq!(
        acknowledgements
            .iter()
            .map(|md| md.partition)
            .collect::<Vec<_>>(),
        vec![partition_for_key(b"", 4), partition_for_key(b"key", 4), 3]
    );
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn failed_buffer_admission_does_not_charge_or_consume_route() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 4);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(123))
            .linger(Duration::from_secs(30))
            .buffer_memory(160)
            .batch_bytes(10_000),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let payload = vec![b'x'; 80];
    producer
        .try_send(ProduceRecord::to("t").value(payload.clone()))
        .unwrap();
    producer
        .try_send(ProduceRecord::to("t").value(payload.clone()))
        .unwrap();
    let before = producer.__test_sticky_state().unwrap();
    for _ in 0..30 {
        assert!(matches!(
            producer.try_send(ProduceRecord::to("t").value(payload.clone())),
            Err(Error::QueueFull)
        ));
        assert_eq!(producer.__test_sticky_state().unwrap(), before);
    }
    assert_eq!(producer.metrics().bytes_buffered, 160);
    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn failed_leader_routing_does_not_create_history_or_advance_accounting() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 3);
    for partition in 0..3 {
        mock.set_partition_leader("t", partition, -1);
    }
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(123)),
    )
    .await
    .unwrap();
    let partitions = producer.partitions_for("t").await.unwrap();
    assert_eq!(partitions.len(), 3);
    assert!(partitions.iter().all(|partition| partition.leader == -1));
    let before = producer.__test_sticky_state().unwrap();
    for _ in 0..20 {
        assert!(matches!(
            producer.try_send(ProduceRecord::to("t").value(&b"no leader"[..])),
            Err(Error::QueueFull)
        ));
        assert_eq!(producer.__test_sticky_state().unwrap(), before);
    }
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn pressure_singletons_are_not_merged_and_ledger_is_bounded() {
    let mock = common::Mock::start().await;
    let policy = StickyPartitioner::with_config(StickyPartitionerConfig {
        max_topics: 1,
        max_cohorts: 1,
        max_topic_bytes: 1,
        max_pending_records: 100_000,
        seed: Some(9),
    });
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(policy)
            .linger(Duration::from_secs(30))
            .batch_records(2)
            .batch_bytes(10_000),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let first = mock.produce_batches().len();
    // No await: all four successful appends precede the actor's drain.
    for _ in 0..4 {
        producer
            .try_send(ProduceRecord::to("t").value(&b"value"[..]))
            .unwrap();
    }
    let state = producer.__test_sticky_state().unwrap();
    assert_eq!((state.0, state.1, state.2), (1, 1, 1));
    assert_eq!(state.3, 2);
    producer.flush().await.unwrap();
    assert_eq!(
        mock.produce_batches()
            .into_iter()
            .skip(first)
            .map(|batch| batch.2)
            .collect::<Vec<_>>(),
        vec![2, 1, 1]
    );
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    producer.clone().close().await.unwrap();
    let state = producer.__test_sticky_state().unwrap();
    assert_eq!((state.0, state.1, state.2), (0, 0, 0));
}

#[tokio::test]
async fn zero_caps_use_stateless_singletons_without_new_admission_errors() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::with_config(StickyPartitionerConfig {
                max_topics: 0,
                max_cohorts: 0,
                max_topic_bytes: 0,
                max_pending_records: 100_000,
                seed: Some(90),
            }))
            .linger(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let first = mock.produce_batches().len();
    for _ in 0..5 {
        producer
            .try_send(ProduceRecord::to("t").value(&b"v"[..]))
            .unwrap();
    }
    let state = producer.__test_sticky_state().unwrap();
    assert_eq!((state.0, state.1, state.2), (0, 0, 0));
    producer.flush().await.unwrap();
    assert_eq!(
        mock.produce_batches()
            .into_iter()
            .skip(first)
            .map(|batch| batch.2)
            .collect::<Vec<_>>(),
        vec![1; 5]
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn independent_producers_from_one_config_have_independent_histories() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 7);
    let cfg = ProducerConfig::bootstrap([mock.addr.clone()])
        .connections(1)
        .partitioner(StickyPartitioner::seeded(73))
        .linger(Duration::ZERO);
    let first = Producer::new(cfg.clone()).await.unwrap();
    let second = Producer::new(cfg).await.unwrap();
    let a = first
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    let b = second
        .send(ProduceRecord::to("t").value(&b"b"[..]))
        .await
        .unwrap();
    assert_eq!(a.partition, b.partition);
    assert_eq!(
        first.__test_sticky_state().unwrap().4,
        second.__test_sticky_state().unwrap().4
    );
    first.close().await.unwrap();
    assert_eq!(second.__test_sticky_state().unwrap().0, 1);
    second.close().await.unwrap();
}

#[tokio::test]
async fn retries_preserve_cohort_membership_and_never_charge_twice() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(3))
            .linger(Duration::from_secs(30))
            .idempotent(true)
            .retry_backoff(Duration::ZERO),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    producer.inject_pre_send_fault(PreSendFault::WriteRetriable);
    for _ in 0..5 {
        producer
            .try_send(ProduceRecord::to("t").value(&b"retry"[..]))
            .unwrap();
    }
    let bytes = producer.__test_sticky_state().unwrap().4;
    timeout(Duration::from_secs(5), producer.flush())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(producer.__test_sticky_state().unwrap().4, bytes);
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(mock.produce_batches().last().map(|batch| batch.2), Some(5));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn terminal_errors_retire_all_cohort_rows_and_payload_reservations() {
    for fault in [PreSendFault::Encode, PreSendFault::Write] {
        let mock = common::Mock::start().await;
        let producer = Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .connections(1)
                .partitioner(StickyPartitioner::seeded(2))
                .linger(Duration::from_secs(30)),
        )
        .await
        .unwrap();
        warm(&producer).await.unwrap();
        producer.inject_pre_send_fault(fault);
        for _ in 0..3 {
            producer
                .try_send(ProduceRecord::to("t").value(&b"fail"[..]))
                .unwrap();
        }
        assert!(producer.flush().await.is_err());
        assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
        assert_eq!(producer.metrics().bytes_buffered, 0);
        producer.close().await.unwrap();
    }
}

#[tokio::test]
async fn broker_terminal_error_and_acks_zero_both_retire_rows() {
    for acks in [0, 1] {
        let mock = common::Mock::start().await;
        let producer = Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .connections(1)
                .partitioner(StickyPartitioner::seeded(2))
                .linger(Duration::ZERO)
                .acks(if acks == 0 {
                    partitionline::Acks::None
                } else {
                    partitionline::Acks::Leader
                }),
        )
        .await
        .unwrap();
        if acks != 0 {
            mock.set_produce_error(error::INVALID_RECORD);
        }
        let result = producer
            .send(ProduceRecord::to("t").value(&b"terminal"[..]))
            .await;
        assert_eq!(result.is_ok(), acks == 0);
        assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
        assert_eq!(producer.metrics().bytes_buffered, 0);
        producer.close().await.unwrap();
    }
}

#[tokio::test]
async fn cancelled_queued_send_still_delivers_and_retires_its_cohort() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(2))
            .linger(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let mut future = Box::pin(producer.send(ProduceRecord::to("t").value(&b"ambiguous"[..])));
    tokio::select! {
        result = &mut future => panic!("send finished before cancellation point: {result:?}"),
        _ = common::wait_pred("actual admission", || producer.metrics().bytes_buffered > 0) => {}
    }
    let charged = producer.__test_sticky_state().unwrap().4;
    drop(future);
    producer.flush().await.unwrap();
    assert_eq!(producer.__test_sticky_state().unwrap().4, charged);
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(mock.produce_batches().last().map(|batch| batch.2), Some(1));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_before_buffer_admission_does_not_charge_or_leak() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(2))
            .linger(Duration::from_secs(30))
            .buffer_memory(160),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let payload = vec![b'x'; 80];
    for _ in 0..2 {
        producer
            .try_send(ProduceRecord::to("t").value(payload.clone()))
            .unwrap();
    }
    let before = producer.__test_sticky_state().unwrap();
    let mut blocked = Box::pin(producer.send(ProduceRecord::to("t").value(payload)));
    tokio::select! {
        result = &mut blocked => panic!("full buffer unexpectedly admitted: {result:?}"),
        _ = sleep(Duration::from_millis(20)) => {}
    }
    drop(blocked);
    assert_eq!(producer.__test_sticky_state().unwrap(), before);
    assert_eq!(producer.metrics().bytes_buffered, 160);
    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn absolute_max_block_with_unavailable_leaders_is_bounded_and_uncharged() {
    let mock = common::Mock::start().await;
    mock.set_partition_leader("t", 0, -1);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(2))
            .max_block(Duration::from_millis(80)),
    )
    .await
    .unwrap();
    let result = timeout(
        Duration::from_secs(2),
        producer.send(ProduceRecord::to("t").value(&b"blocked"[..])),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(Error::Timeout)));
    assert_eq!(producer.__test_sticky_state().unwrap().4, 0);
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn expiry_and_close_clear_state_without_late_history_recreation() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(2))
            .linger(Duration::from_secs(30))
            .delivery_timeout(Duration::from_millis(80)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    producer
        .try_send(ProduceRecord::to("t").value(&b"expire"[..]))
        .unwrap();
    common::wait_pred("delivery expiry", || producer.metrics().bytes_buffered == 0).await;
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    let clone = producer.clone();
    producer.close().await.unwrap();
    assert!(matches!(
        clone.try_send(ProduceRecord::to("t").value(&b"late"[..])),
        Err(Error::Closed)
    ));
    let after = clone.__test_sticky_state().unwrap();
    assert_eq!((after.0, after.1, after.2), (0, 0, 0));
}

#[tokio::test]
async fn retained_topic_text_and_idle_eviction_stay_within_hard_caps() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::with_config(StickyPartitionerConfig {
                max_topics: 2,
                max_cohorts: 2,
                max_topic_bytes: 8,
                max_pending_records: 100_000,
                seed: Some(2),
            }))
            .linger(Duration::ZERO),
    )
    .await
    .unwrap();
    for topic in ["a", "bb", "ccc", "dddd", "toolongfortextcap"] {
        let metadata = producer
            .send(ProduceRecord::to(topic).value(&b"payload"[..]))
            .await
            .unwrap();
        assert_eq!(metadata.topic, topic);
        assert_eq!(metadata.partition, 0);
        assert!(metadata.offset >= 0);
        let state = producer.__test_sticky_state().unwrap();
        assert!(state.0 <= 2 && state.1 == 0 && state.2 <= 8);
    }
    assert!(producer.__test_sticky_state().unwrap().3 > 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn transaction_partition_failure_retires_cohort_and_allows_abort() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(2))
            .transactional_id("sticky_txn")
            .linger(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    producer.begin_transaction().await.unwrap();
    producer.inject_pre_send_fault(PreSendFault::TxnPartition);
    for _ in 0..3 {
        producer
            .try_send(ProduceRecord::to("t").value(&b"txn_fail"[..]))
            .unwrap();
    }
    assert!(producer.flush().await.is_err());
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.abort_transaction().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn new_uniform_draws_exclude_unavailable_partitions() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 4);
    mock.set_partition_leader("t", 1, -1);
    mock.set_partition_leader("t", 3, -1);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(73))
            .linger(Duration::from_secs(30))
            .batch_bytes(100),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let first = mock.produce_batches().len();
    for _ in 0..40 {
        producer
            .try_send(ProduceRecord::to("t").value(vec![b'x'; 80]))
            .unwrap();
    }
    producer.flush().await.unwrap();
    let partitions: Vec<_> = mock
        .produce_batches()
        .into_iter()
        .skip(first)
        .map(|batch| batch.1)
        .collect();
    assert_eq!(partitions.len(), 40);
    assert!(partitions
        .iter()
        .all(|partition| *partition == 0 || *partition == 2));
    assert!(partitions.contains(&0) && partitions.contains(&2));
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn existing_sticky_selection_survives_leader_loss_without_consuming_draws() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 4);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(73))
            .linger(Duration::ZERO)
            .batch_bytes(100_000),
    )
    .await
    .unwrap();
    let first = producer
        .send(ProduceRecord::to("t").value(&b"first"[..]))
        .await
        .unwrap();
    mock.set_partition_leader("t", first.partition, -1);
    let partitions = producer.partitions_for("t").await.unwrap();
    assert_eq!(partitions.len(), 4);
    assert_eq!(
        partitions
            .iter()
            .find(|partition| partition.partition == first.partition)
            .map(|partition| partition.leader),
        Some(-1)
    );
    let before = producer.__test_sticky_state().unwrap();
    for _ in 0..20 {
        assert!(matches!(
            producer.try_send(ProduceRecord::to("t").value(&b"blocked"[..])),
            Err(Error::QueueFull)
        ));
        assert_eq!(producer.__test_sticky_state().unwrap(), before);
    }
    mock.set_partition_leader("t", first.partition, 1);
    let partitions = producer.partitions_for("t").await.unwrap();
    assert_eq!(partitions.len(), 4);
    assert_eq!(
        partitions
            .iter()
            .find(|partition| partition.partition == first.partition)
            .map(|partition| partition.leader),
        Some(1)
    );
    let second = producer
        .send(ProduceRecord::to("t").value(&b"second"[..]))
        .await
        .unwrap();
    assert_eq!(first.partition, second.partition);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn produce_v13_uses_admitted_uuid_after_metadata_recreation() {
    let mock = common::Mock::start().await;
    mock.set_api_max(partitionline::protocol::api_keys::PRODUCE, 13);
    mock.set_api_max(partitionline::protocol::api_keys::METADATA, 13);
    mock.set_topic_id("t", [1; 16]);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(73))
            .delivery_timeout(Duration::from_millis(500))
            .linger(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let initial_id_requests = mock.produce_sent_topic_ids().len();
    producer
        .try_send(ProduceRecord::to("t").value(&b"accepted_before_recreation"[..]))
        .unwrap();
    mock.set_topic_id("t", [2; 16]);
    let partitions = producer.partitions_for("t").await.unwrap();
    assert_eq!(partitions.len(), 1);
    assert!(partitions.iter().all(|partition| partition.topic == "t"));
    // Real broker rejects the old UUID after deletion. The framed mock may also
    // reject it; the required observation is the identity actually on the wire.
    let _flush_outcome = timeout(Duration::from_secs(2), producer.flush())
        .await
        .unwrap();
    assert_eq!(producer.metrics().produce_errors, 1);
    assert_eq!(producer.metrics().records_acked, 1);
    assert!(mock
        .produce_sent_topic_ids()
        .into_iter()
        .skip(initial_id_requests)
        .any(|(_topic, id)| id == [1; 16]));
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn leader_movement_retries_original_partition_and_complete_cohort() {
    let mock = common::Mock::start_two_node().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(9))
            .idempotent(true)
            .linger(Duration::from_secs(30))
            .retry_backoff(Duration::ZERO)
            .delivery_timeout(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    for _ in 0..5 {
        producer
            .try_send(ProduceRecord::to("t").value(&b"moved"[..]))
            .unwrap();
    }
    let charged = producer.__test_sticky_state().unwrap().4;
    mock.set_partition_leader("t", 0, 1);
    mock.set_produce_error_times(error::NOT_LEADER_OR_FOLLOWER, 1);
    timeout(Duration::from_secs(5), producer.flush())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mock.produce_request_nodes().last(), Some(&1));
    assert_eq!(
        mock.produce_batches()
            .last()
            .map(|batch| (batch.1, batch.2)),
        Some((0, 5))
    );
    assert_eq!(producer.__test_sticky_state().unwrap().4, charged);
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sequenced_retry_expiry_cancels_remaining_cohort_without_resending_short_batch() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::seeded(9))
            .idempotent(true)
            .linger(Duration::from_secs(30))
            .retry_backoff(Duration::from_millis(250))
            .delivery_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let before = mock.produce_batches().len();
    producer.inject_pre_send_fault(PreSendFault::WriteRetriable);
    for _ in 0..5 {
        producer
            .try_send(ProduceRecord::to("t").value(&b"expires_in_retry"[..]))
            .unwrap();
    }
    let charged = producer.__test_sticky_state().unwrap().4;
    let _result = timeout(Duration::from_secs(2), producer.flush())
        .await
        .unwrap();
    common::wait_pred("retry expiry drains all owners", || {
        producer.metrics().bytes_buffered == 0
    })
    .await;
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    assert_eq!(producer.__test_sticky_state().unwrap().4, charged);
    assert_eq!(mock.produce_batches().len(), before);
    assert_eq!(producer.metrics().produce_errors, 5);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn asynchronous_mixed_admissions_across_connection_slots_complete_and_release_ledger() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 6);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(3)
            .partitioner(StickyPartitioner::seeded(79443))
            .batch_records(5)
            .batch_bytes(1000)
            .linger(Duration::from_millis(5))
            .max_block(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let records: Vec<_> = (0..120)
        .map(|i| {
            let record = ProduceRecord::to("t").value(vec![b'x'; 20]);
            if i % 3 == 0 {
                record.partition(i % 6)
            } else if i % 3 == 1 {
                record.key(i.to_string())
            } else {
                record
            }
        })
        .collect();
    let result = timeout(Duration::from_secs(10), producer.send_all(records))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.len(), 120);
    assert_eq!(
        mock.produce_batches()
            .iter()
            .map(|batch| batch.2)
            .sum::<i32>(),
        120
    );
    assert!(mock
        .produce_batches()
        .iter()
        .all(|batch| batch.2 <= 5 && batch.3 <= 1000));
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn two_record_cap_bounds_null_and_empty_payloads_even_with_unlimited_byte_budget() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::with_config(StickyPartitionerConfig {
                max_pending_records: 2,
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            }))
            .buffer_memory(0)
            .linger(Duration::from_secs(30))
            .max_block(Duration::from_millis(80)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    producer.try_send(ProduceRecord::to("t")).unwrap();
    producer
        .try_send(ProduceRecord::to("t").key(&b""[..]).value(&b""[..]))
        .unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.__test_sticky_records(), Some((2, 2)));
    let before = producer.__test_sticky_state().unwrap();
    for _ in 0..100 {
        assert!(matches!(
            producer.try_send(ProduceRecord::to("t")),
            Err(Error::QueueFull)
        ));
        assert_eq!(producer.__test_sticky_records(), Some((2, 2)));
        assert_eq!(producer.__test_sticky_state().unwrap(), before);
    }
    let result = timeout(
        Duration::from_secs(1),
        producer.send(ProduceRecord::to("t")),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(Error::Timeout)));
    assert_eq!(producer.__test_sticky_records(), Some((2, 2)));
    assert_eq!(producer.__test_sticky_state().unwrap(), before);
    producer.flush().await.unwrap();
    assert_eq!(producer.__test_sticky_records(), Some((0, 2)));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_before_enqueue_releases_the_reserved_record_slot_without_policy_charge() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::with_config(StickyPartitionerConfig {
                max_pending_records: 3,
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            }))
            .buffer_memory(160)
            .linger(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let payload = vec![b'x'; 80];
    for _ in 0..2 {
        producer
            .try_send(ProduceRecord::to("t").value(payload.clone()))
            .unwrap();
    }
    let before = producer.__test_sticky_state().unwrap();
    let mut blocked = Box::pin(producer.send(ProduceRecord::to("t").value(payload)));
    tokio::select! {
        result = &mut blocked => panic!("byte-blocked send completed: {result:?}"),
        _ = common::wait_pred("reserved third admission slot", || producer.__test_sticky_records() == Some((3, 3))) => {}
    }
    assert_eq!(producer.__test_sticky_state().unwrap(), before);
    assert_eq!(producer.metrics().bytes_buffered, 160);
    drop(blocked);
    assert_eq!(producer.__test_sticky_records(), Some((2, 3)));
    assert_eq!(producer.__test_sticky_state().unwrap(), before);
    producer.flush().await.unwrap();
    assert_eq!(producer.__test_sticky_records(), Some((0, 3)));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_while_waiting_record_capacity_creates_no_reservation_or_admission() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::with_config(StickyPartitionerConfig {
                max_pending_records: 1,
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            }))
            .buffer_memory(0)
            .linger(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    producer.try_send(ProduceRecord::to("t")).unwrap();
    let before = producer.__test_sticky_state().unwrap();
    let mut blocked = Box::pin(producer.send(ProduceRecord::to("t")));
    tokio::select! { result = &mut blocked => panic!("record-capacity wait completed: {result:?}"), _ = sleep(Duration::from_millis(20)) => {} }
    drop(blocked);
    assert_eq!(producer.__test_sticky_records(), Some((1, 1)));
    assert_eq!(producer.__test_sticky_state().unwrap(), before);
    producer.flush().await.unwrap();
    assert_eq!(producer.__test_sticky_records(), Some((0, 1)));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_accepted_null_send_retains_slot_until_terminal_delivery() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::with_config(StickyPartitionerConfig {
                max_pending_records: 2,
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            }))
            .buffer_memory(0)
            .linger(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    let queued = producer.metrics().records_queued;
    let mut future = Box::pin(producer.send(ProduceRecord::to("t")));
    tokio::select! {
        result = &mut future => panic!("accepted send finished before cancellation: {result:?}"),
        _ = common::wait_pred("null record admitted", || producer.metrics().records_queued == queued + 1) => {}
    }
    let charged = producer.__test_sticky_state().unwrap().4;
    drop(future);
    assert_eq!(producer.__test_sticky_records(), Some((1, 2)));
    producer.flush().await.unwrap();
    assert_eq!(producer.__test_sticky_records(), Some((0, 2)));
    assert_eq!(producer.__test_sticky_state().unwrap().4, charged);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn retry_backoff_retains_record_slots_without_double_acquisition_or_charge() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::with_config(StickyPartitionerConfig {
                max_pending_records: 2,
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            }))
            .idempotent(true)
            .buffer_memory(0)
            .linger(Duration::from_secs(30))
            .retry_backoff(Duration::from_millis(150)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    producer.inject_pre_send_fault(PreSendFault::WriteRetriable);
    for _ in 0..2 {
        producer.try_send(ProduceRecord::to("t")).unwrap();
    }
    let charged = producer.__test_sticky_state().unwrap().4;
    let flush = producer.flush();
    tokio::pin!(flush);
    tokio::select! {
        result = &mut flush => panic!("flush finished before observed retry ownership: {result:?}"),
        _ = common::wait_pred("records in retry ownership", || producer.retries_in_flight() > 0) => {}
    }
    assert_eq!(producer.__test_sticky_records(), Some((2, 2)));
    assert!(matches!(
        producer.try_send(ProduceRecord::to("t")),
        Err(Error::QueueFull)
    ));
    assert_eq!(producer.__test_sticky_state().unwrap().4, charged);
    timeout(Duration::from_secs(5), &mut flush)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(producer.__test_sticky_records(), Some((0, 2)));
    assert_eq!(producer.__test_sticky_state().unwrap().1, 0);
    assert_eq!(producer.__test_sticky_state().unwrap().4, charged);
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn terminal_failure_and_failed_routing_release_record_slots_exactly_once() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .partitioner(StickyPartitioner::with_config(StickyPartitionerConfig {
                max_pending_records: 2,
                seed: Some(9),
                ..StickyPartitionerConfig::default()
            }))
            .buffer_memory(0)
            .linger(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    warm(&producer).await.unwrap();
    producer.inject_pre_send_fault(PreSendFault::Encode);
    for _ in 0..2 {
        producer.try_send(ProduceRecord::to("t")).unwrap();
    }
    assert!(producer.flush().await.is_err());
    assert_eq!(producer.__test_sticky_records(), Some((0, 2)));
    let before = producer.__test_sticky_state().unwrap();
    for _ in 0..20 {
        assert!(matches!(
            producer.try_send(ProduceRecord::to("t").partition(99)),
            Err(Error::QueueFull)
        ));
        assert_eq!(producer.__test_sticky_records(), Some((0, 2)));
        assert_eq!(producer.__test_sticky_state().unwrap(), before);
    }
    producer.try_send(ProduceRecord::to("t")).unwrap();
    assert_eq!(producer.__test_sticky_records(), Some((1, 2)));
    producer.flush().await.unwrap();
    assert_eq!(producer.__test_sticky_records(), Some((0, 2)));
    producer.close().await.unwrap();
}
