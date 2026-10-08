//! Stamp a header on every produced record.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).

use partitionline::{ProduceRecord, Producer, ProducerConfig, ProducerInterceptor};

struct ClientHeader;

impl ProducerInterceptor for ClientHeader {
    fn on_send(&self, rec: ProduceRecord) -> ProduceRecord {
        rec.header("client", &b"partitionline"[..])
    }
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let producer =
        Producer::new(ProducerConfig::bootstrap([bootstrap]).interceptor(ClientHeader)).await?;
    let md = producer
        .send(ProduceRecord::to(topic).value(&b"hello with a header"[..]))
        .await?;
    println!("{}-{}@{}", md.topic, md.partition, md.offset);
    producer.close().await?;
    Ok(())
}
