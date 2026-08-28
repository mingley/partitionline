//! Join a classic consumer group and print records.

use partitionline::{ConsumerConfig, ConsumerGroup};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let group_id = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "partitionline".into());
    let mut group = ConsumerGroup::join(
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
        group.commit().await?;
    }
}
