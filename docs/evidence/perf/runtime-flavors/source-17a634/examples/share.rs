//! Join a KIP-932 share group, poll, and accept records.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).

use partitionline::{ConsumerConfig, ShareGroup};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let group_id = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "partitionline-share".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let mut group = ShareGroup::join(
        ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500),
        group_id,
        topic,
    )
    .await?;
    loop {
        let recs = group.poll().await?;
        if recs.is_empty() {
            continue;
        }
        for rec in &recs {
            println!("{}-{}@{}", rec.topic, rec.partition, rec.offset);
        }
        group.accept(&recs).await?;
    }
}
