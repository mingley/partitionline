//! Print producer/consumer metrics as log lines or Prometheus text.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//! Set `FORMAT=prom` for a minimal Prometheus exposition (no prom crate).
//!
//! Formatting lives in [`prom_format`] so tests can prove the exporter without
//! a live broker (KL-07 optional-exporter Partial).

mod prom_format;

use partitionline::{Consumer, ProduceRecord, Producer, ProducerConfig};
use prom_format::{format_consumer_prometheus, format_producer_prometheus};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let format = std::env::var("FORMAT").unwrap_or_else(|_| "log".into());

    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap.clone()]).linger(std::time::Duration::ZERO),
    )
    .await?;
    let md = producer
        .send(ProduceRecord::to(topic.clone()).value(&b"hello"[..]))
        .await?;
    let pm = producer.metrics();

    if format == "prom" {
        print!("{}", format_producer_prometheus(&pm));
    } else {
        println!(
            "produced {}-{}@{} queued={} acked={} bytes={} ack_us={} p50_us={} p99_us={} topics={}",
            md.topic,
            md.partition,
            md.offset,
            pm.records_queued,
            pm.records_acked,
            pm.bytes_queued,
            pm.ack_latency.mean_nanos().unwrap_or(0) / 1000,
            pm.ack_latency.p50_nanos / 1000,
            pm.ack_latency.p99_nanos / 1000,
            pm.topics.len()
        );
    }
    producer.close().await?;

    let mut consumer = Consumer::connect(bootstrap).await?;
    consumer.assign(&topic, 0, 0).await?;
    let recs = consumer.fetch().await?;
    let cm = consumer.metrics();
    if format == "prom" {
        print!("{}", format_consumer_prometheus(&cm));
    } else {
        println!(
            "fetched {} records rounds={} bytes={} errors={} fetch_us={} p50_us={} p99_us={} topics={}",
            recs.len(),
            cm.fetch_rounds,
            cm.bytes_fetched,
            cm.fetch_errors,
            cm.fetch_latency.mean_nanos().unwrap_or(0) / 1000,
            cm.fetch_latency.p50_nanos / 1000,
            cm.fetch_latency.p99_nanos / 1000,
            cm.topics.len()
        );
    }
    consumer.close().await?;
    Ok(())
}
