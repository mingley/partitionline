//! Print producer/consumer metrics as log lines or Prometheus text.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//! Set `FORMAT=prom` for a minimal Prometheus exposition (no prom crate).

use partitionline::{Consumer, ProduceRecord, Producer, ProducerConfig};

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
        println!("# HELP partitionline_produce_records_acked Produce records acked");
        println!("# TYPE partitionline_produce_records_acked counter");
        println!("partitionline_produce_records_acked {}", pm.records_acked);
        println!("# HELP partitionline_produce_ack_p99_seconds Produce ack p99 latency");
        println!("# TYPE partitionline_produce_ack_p99_seconds gauge");
        println!(
            "partitionline_produce_ack_p99_seconds {}",
            (pm.ack_latency.p99_nanos as f64) / 1_000_000_000.0
        );
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
        println!("# HELP partitionline_fetch_rounds Fetch rounds completed");
        println!("# TYPE partitionline_fetch_rounds counter");
        println!("partitionline_fetch_rounds {}", cm.fetch_rounds);
        println!("# HELP partitionline_offsets_for_times_ok offsetsForTimes batch successes");
        println!("# TYPE partitionline_offsets_for_times_ok counter");
        println!("partitionline_offsets_for_times_ok {}", cm.offsets_for_times_ok);
        println!("# HELP partitionline_offsets_for_times_fail offsetsForTimes batch failures");
        println!("# TYPE partitionline_offsets_for_times_fail counter");
        println!("partitionline_offsets_for_times_fail {}", cm.offsets_for_times_fail);
        println!("# HELP partitionline_fetch_p99_seconds Fetch round p99 latency");
        println!("# TYPE partitionline_fetch_p99_seconds gauge");
        println!(
            "partitionline_fetch_p99_seconds {}",
            (cm.fetch_latency.p99_nanos as f64) / 1_000_000_000.0
        );
    } else {
        println!(
            "fetched {} records rounds={} bytes={} errors={} offsets_for_times_ok={} offsets_for_times_fail={} fetch_us={} p50_us={} p99_us={} topics={}",
            recs.len(),
            cm.fetch_rounds,
            cm.bytes_fetched,
            cm.fetch_errors,
            cm.offsets_for_times_ok,
            cm.offsets_for_times_fail,
            cm.fetch_latency.mean_nanos().unwrap_or(0) / 1000,
            cm.fetch_latency.p50_nanos / 1000,
            cm.fetch_latency.p99_nanos / 1000,
            cm.topics.len()
        );
    }
    consumer.close().await?;
    Ok(())
}
