//! Print producer and consumer counter snapshots.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).

use partitionline::{Consumer, ProduceRecord, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());

    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap.clone()]).linger(std::time::Duration::ZERO),
    )
    .await?;
    let md = producer
        .send(ProduceRecord::to(topic.clone()).value(&b"hello"[..]))
        .await?;
    let pm = producer.metrics();
    println!(
        "produced {}-{}@{} queued={} acked={} bytes={}",
        md.topic, md.partition, md.offset, pm.records_queued, pm.records_acked, pm.bytes_queued
    );
    producer.close().await?;

    let mut consumer = Consumer::connect(bootstrap).await?;
    consumer.assign(&topic, 0, 0).await?;
    let recs = consumer.fetch().await?;
    let cm = consumer.metrics();
    println!(
        "fetched {} records rounds={} bytes={} errors={}",
        recs.len(),
        cm.fetch_rounds,
        cm.bytes_fetched,
        cm.fetch_errors
    );
    consumer.close().await?;
    Ok(())
}
