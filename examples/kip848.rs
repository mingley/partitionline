//! Join a KIP-848 next-gen consumer group (`group.protocol=consumer`) and print
//! records. Requires a broker that speaks ConsumerGroupHeartbeat (Kafka 4.x).
//!
//! `KAFKA_TOPIC` may be a single name or a comma-separated list.

use partitionline::{ConsumerConfig, ConsumerGroup};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let group_id = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "partitionline-kip848".into());
    let topics: Vec<String> = std::env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "partitionline".into())
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    let mut group = ConsumerGroup::join_consumer_topics(
        ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500),
        group_id,
        topics,
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
        group.commit_with_metadata(recs.next_offsets()).await?;
    }
}
