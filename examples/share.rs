//! Join a KIP-932 share group, poll, and accept records.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//!
//! Join applies `ConsumerConfig::auto_offset_reset` as
//! `share.auto.offset.reset` when the broker accepts IncrementalAlterConfigs
//! (crate default earliest). If the broker keeps latest, produce while this
//! process is already polling — empty polls are waiting, not hung.

use partitionline::{ConsumerConfig, ShareGroup};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let group_id = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "partitionline-share".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let mut group = ShareGroup::join(
        ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500),
        group_id.clone(),
        topic.clone(),
    )
    .await?;
    println!("joined group={group_id} topic={topic}");
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
