//! Join a KIP-932 share group, poll, and accept records.
//!
//! Broker on `KAFKA_BOOTSTRAP`. `KAFKA_TOPIC` may be a single name or a
//! comma-separated list (default `partitionline`). Default
//! `share.auto.offset.reset` is latest — produce after join.

use partitionline::{ConsumerConfig, ShareGroup};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let group_id = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "partitionline-share".into());
    let topics: Vec<String> = std::env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "partitionline".into())
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    let mut group = ShareGroup::join_topics(
        ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500),
        group_id,
        topics,
    )
    .await?;
    let md = group.group_metadata();
    println!(
        "share group {} member {} epoch {} assignment {:?}",
        md.group_id(),
        md.member_id(),
        md.generation_id(),
        group.assignment()
    );
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
