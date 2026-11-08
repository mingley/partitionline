//! Public send_all routing and admission regression tests.
mod common;

use partitionline::interceptor::ProducerInterceptor;
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerMetrics, RecordMetadata,
    StickyPartitioner,
};
use std::future::{poll_fn, Future};
use std::num::TryFromIntError;
use std::task::Poll;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn config(mock: &common::Mock) -> ProducerConfig {
    ProducerConfig::bootstrap([mock.addr.clone()])
        .connections(1)
        .idempotent(false)
        .linger(Duration::from_millis(1))
        .batch_records(256)
        .metadata_max_age(Duration::from_secs(3600))
        .request_timeout(Duration::from_secs(60))
        .delivery_timeout(Duration::from_secs(120))
        .max_block(Duration::from_secs(120))
}

fn value(id: usize) -> Result<Vec<u8>, TryFromIntError> {
    Ok(u64::try_from(id)?.to_le_bytes().to_vec())
}

fn records(
    start: usize,
    count: usize,
    partition: Option<i32>,
) -> Result<Vec<ProduceRecord>, TryFromIntError> {
    (start..start + count)
        .map(|id| {
            let mut record = ProduceRecord::to("t").value(value(id)?);
            record.partition = partition;
            Ok(record)
        })
        .collect()
}

async fn warm(producer: &Producer, partition: i32) -> Result<(), Error> {
    let result = producer
        .send(ProduceRecord::to("t").partition(partition).value("warm"))
        .await?;
    assert_eq!(result.partition, partition);
    assert_eq!(result.offset, 0);
    Ok(())
}

fn check_receipts(
    receipts: &[RecordMetadata],
    partition: i32,
    first_offset: i64,
) -> Result<(), TryFromIntError> {
    for (index, result) in receipts.iter().enumerate() {
        assert_eq!(result.topic.as_ref(), "t");
        assert_eq!(result.partition, partition);
        assert_eq!(result.offset, first_offset + i64::try_from(index)?);
    }
    Ok(())
}

fn check_log(
    mock: &common::Mock,
    partition: i32,
    expected_ids: &[usize],
) -> Result<(), TryFromIntError> {
    let actual = mock.log_values("t", partition);
    assert_eq!(actual.len(), expected_ids.len() + 1);
    let mut entries = actual.into_iter();
    assert_eq!(entries.next(), Some((0, Some(b"warm".to_vec()))));
    for (index, id) in expected_ids.iter().copied().enumerate() {
        assert_eq!(
            entries.next(),
            Some((i64::try_from(index + 1)?, Some(value(id)?)))
        );
    }
    assert_eq!(entries.next(), None);
    Ok(())
}

fn check_accounting(
    before: &ProducerMetrics,
    after: &ProducerMetrics,
    count: usize,
) -> Result<(), TryFromIntError> {
    let count = u64::try_from(count)?;
    assert_eq!(after.records_queued, before.records_queued + count);
    assert_eq!(after.records_acked, before.records_acked + count);
    assert_eq!(after.produce_errors, before.produce_errors);
    assert_eq!(after.bytes_queued, before.bytes_queued + count * 8);
    assert_eq!(after.bytes_buffered, 0);
    Ok(())
}

fn check_no_progress(before: &ProducerMetrics, after: &ProducerMetrics) {
    assert_eq!(after.records_queued, before.records_queued);
    assert_eq!(after.records_acked, before.records_acked);
    assert_eq!(after.produce_errors, before.produce_errors);
    assert_eq!(after.bytes_queued, before.bytes_queued);
    assert_eq!(after.bytes_buffered, before.bytes_buffered);
}

#[tokio::test(flavor = "current_thread")]
async fn same_explicit_route_preserves_order_and_accounting() -> TestResult {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock)).await?;
    warm(&producer, 0).await?;
    let mut start = 0usize;
    for count in [31, 32, 8191] {
        let before = producer.metrics();
        assert_eq!(before.bytes_buffered, 0);
        let output = producer.send_all(records(start, count, Some(0))?).await?;
        assert_eq!(output.len(), count);
        check_receipts(&output, 0, i64::try_from(start + 1)?)?;
        start += count;
        check_log(&mock, 0, &(0..start).collect::<Vec<_>>())?;
        check_accounting(&before, &producer.metrics(), count)?;
    }
    producer.close().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn full_scan_boundary_yields_before_admission() -> TestResult {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock)).await?;
    warm(&producer, 0).await?;
    let before = producer.metrics();
    assert_eq!(before.bytes_buffered, 0);
    let log_before = mock.log_values("t", 0);

    // With the route warmed and cooperative budgeting disabled, the first
    // poll stops at the 8,192-record collection yield. The second reaches
    // the nudge scan's own yield before the first record is admitted.
    let mut send = Box::pin(tokio::task::unconstrained(
        producer.send_all(records(0, 8192, Some(0))?),
    ));
    assert!(poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx)))
        .await
        .is_pending());
    check_no_progress(&before, &producer.metrics());
    assert_eq!(mock.log_values("t", 0), log_before);

    assert!(poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx)))
        .await
        .is_pending());
    check_no_progress(&before, &producer.metrics());
    assert_eq!(mock.log_values("t", 0), log_before);
    drop(send);
    check_no_progress(&before, &producer.metrics());
    assert_eq!(mock.log_values("t", 0), log_before);
    producer.close().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_explicit_partitions_preserve_input_order() -> TestResult {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 2);
    let producer = Producer::new(config(&mock)).await?;
    warm(&producer, 0).await?;
    warm(&producer, 1).await?;
    let before = producer.metrics();
    let input = (0..32)
        .map(|id| {
            let partition = if id % 2 == 0 { 0 } else { 1 };
            Ok(ProduceRecord::to("t").partition(partition).value(value(id)?))
        })
        .collect::<Result<Vec<_>, TryFromIntError>>()?;
    let output = producer.send_all(input).await?;
    assert_eq!(output.len(), 32);
    for (id, result) in output.iter().enumerate() {
        let partition = if id % 2 == 0 { 0 } else { 1 };
        assert_eq!(result.topic.as_ref(), "t");
        assert_eq!(result.partition, partition);
        assert_eq!(result.offset, 1 + i64::try_from(id / 2)?);
    }
    for partition in 0..2 {
        let ids = (0..32)
            .filter(|id| (id % 2 == 0) == (partition == 0))
            .collect::<Vec<_>>();
        check_log(&mock, partition, &ids)?;
    }
    check_accounting(&before, &producer.metrics(), 32)?;
    producer.close().await?;
    Ok(())
}

struct RewritePartition;

impl ProducerInterceptor for RewritePartition {
    fn on_send(&self, record: ProduceRecord) -> ProduceRecord {
        record.partition(1)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn interceptor_can_rewrite_an_explicit_partition() -> TestResult {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 2);
    let producer = Producer::new(config(&mock).interceptor(RewritePartition)).await?;
    warm(&producer, 1).await?;
    let before = producer.metrics();
    let output = producer.send_all(records(0, 32, Some(0))?).await?;
    assert_eq!(output.len(), 32);
    check_receipts(&output, 1, 1)?;
    check_log(&mock, 1, &(0..32).collect::<Vec<_>>())?;
    assert_eq!(mock.log_len("t", 0), 0);
    check_accounting(&before, &producer.metrics(), 32)?;
    producer.close().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sticky_implicit_partition_produces_all_records() -> TestResult {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock).partitioner(StickyPartitioner::new())).await?;
    warm(&producer, 0).await?;
    let before = producer.metrics();
    let output = producer.send_all(records(0, 32, None)?).await?;
    assert_eq!(output.len(), 32);
    check_receipts(&output, 0, 1)?;
    check_log(&mock, 0, &(0..32).collect::<Vec<_>>())?;
    check_accounting(&before, &producer.metrics(), 32)?;
    producer.close().await?;
    Ok(())
}
