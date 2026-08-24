use std::time::Duration;

use partitionline::{Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let payload = b"live-roundtrip";

    let mut pcfg = ProducerConfig::bootstrap([bootstrap.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await?;
    let md = producer
        .send(ProduceRecord::to(topic.clone()).value(&payload[..]))
        .await?;
    producer.close().await?;

    let mut ccfg = ConsumerConfig::bootstrap([bootstrap]);
    ccfg.max_wait_ms = 1000;
    let mut consumer = Consumer::new(ccfg).await?;
    consumer.assign(topic, md.partition, md.offset).await?;
    let recs = consumer.fetch().await?;
    let rec = recs
        .iter()
        .find(|r| r.offset == md.offset)
        .ok_or_else(|| partitionline::Error::protocol("record not fetched"))?;
    if rec.value.as_deref() != Some(&payload[..]) {
        return Err(partitionline::Error::protocol("payload mismatch"));
    }
    println!(
        "ok {}-{}@{} bytes={}",
        rec.topic,
        rec.partition,
        rec.offset,
        rec.value.as_ref().map(|v| v.len()).unwrap_or(0)
    );
    Ok(())
}
