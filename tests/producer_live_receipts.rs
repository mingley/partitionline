use partitionline::{Acks, Error, ProduceRecord, Producer, ProducerConfig};
use std::{
    error::Error as StdError,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

const TOPICS: [&str; 3] = ["pl-receipts-a", "pl-receipts-b", "pl-receipts-c"];
const PARTITIONS: i32 = 2;

fn positions(from: i64, to: i64) -> impl Iterator<Item = (&'static str, i32, i64)> {
    (from..to).flat_map(|sequence| {
        TOPICS.into_iter().flat_map(move |topic| {
            (0..PARTITIONS).map(move |partition| (topic, partition, sequence))
        })
    })
}

fn record((topic, partition, sequence): (&str, i32, i64)) -> ProduceRecord {
    ProduceRecord::to(topic)
        .partition(partition)
        .key(format!("{sequence:08}"))
        .value(format!("{topic}:{partition}:{sequence:08}"))
}

fn check_metrics(producer: &Producer, expected: u64) {
    let metrics = producer.metrics();
    assert_eq!(metrics.records_queued, expected);
    assert_eq!(metrics.records_acked, expected);
    assert_eq!(metrics.produce_errors, 0);
    assert_eq!(metrics.bytes_buffered, 0);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires the owned loopback Kafka fixture"]
async fn acknowledged_receipts_and_queued_delivery() -> Result<(), Box<dyn StdError>> {
    let address: SocketAddr = std::env::var("PL_RECEIPT_BOOTSTRAP")?.parse()?;
    assert!(matches!(address, SocketAddr::V4(v4) if *v4.ip() == Ipv4Addr::LOCALHOST));
    let acknowledged: i64 = std::env::var("PL_RECEIPT_ACKS_PER_PARTITION")?.parse()?;
    let queued: i64 = std::env::var("PL_RECEIPT_QUEUE_PER_PHASE")?.parse()?;
    assert!((1..=8192).contains(&acknowledged));
    assert!((1..=64).contains(&queued));
    let flushed = acknowledged + queued;
    let total = flushed + queued;
    let deadline = Duration::from_secs(30);
    let config = ProducerConfig::bootstrap([address.to_string()])
        .client_id("pl-receipt-delivery")
        .acks(Acks::All)
        .idempotent(false)
        .connections(1)
        .max_in_flight(1)
        .batch_records(8192)
        .linger(Duration::from_millis(20));
    let producer = Producer::new(config).await?;

    let receipts = producer
        .send_all(positions(0, acknowledged).map(record))
        .await?;
    assert_eq!(receipts.len(), positions(0, acknowledged).count());
    for (receipt, (topic, partition, sequence)) in receipts.iter().zip(positions(0, acknowledged)) {
        assert_eq!(receipt.topic.as_ref(), topic);
        assert_eq!(receipt.partition, partition);
        assert_eq!(receipt.offset, sequence);
    }

    for position in positions(acknowledged, flushed) {
        producer.try_send(record(position))?;
    }
    producer.flush_timeout(deadline).await?;
    check_metrics(&producer, u64::try_from(positions(0, flushed).count())?);

    for position in positions(flushed, total) {
        producer.try_send(record(position))?;
    }
    let observed = producer.clone();
    producer.close_timeout(deadline).await?;
    check_metrics(&observed, u64::try_from(positions(0, total).count())?);
    assert!(matches!(
        observed.try_send(record(("pl-receipts-a", 0, total))),
        Err(Error::Closed)
    ));
    println!(
        "receipt_delivery acknowledged={} after_flush={} after_close={}",
        positions(0, acknowledged).count(),
        positions(0, flushed).count(),
        positions(0, total).count()
    );
    Ok(())
}
