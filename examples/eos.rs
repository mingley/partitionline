//! Consume, produce, and commit offsets in one transaction (exactly-once).
//!
//! Broker on `KAFKA_BOOTSTRAP`. Source `KAFKA_TOPIC` (default `partitionline`).
//! Destination `KAFKA_OUTPUT_TOPIC` (default `partitionline-out`). Group
//! `KAFKA_GROUP` (default `partitionline-eos`). `KAFKA_TRANSACTIONAL_ID`
//! defaults to `partitionline-eos`. Auto-commit stays off; offsets go through
//! [`partitionline::Producer::send_offsets_for_group`].

use std::time::Duration;

use partitionline::{
    ConsumerConfig, ConsumerGroup, ConsumerRecords, IsolationLevel, ProduceRecord, Producer,
    ProducerConfig,
};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let source = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let dest = std::env::var("KAFKA_OUTPUT_TOPIC").unwrap_or_else(|_| "partitionline-out".into());
    let group_id = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "partitionline-eos".into());
    let transactional_id =
        std::env::var("KAFKA_TRANSACTIONAL_ID").unwrap_or_else(|_| "partitionline-eos".into());

    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap.clone()])
            .transactional_id(transactional_id)
            .linger(Duration::ZERO),
    )
    .await?;
    producer.init_transactions().await?;

    let mut group = ConsumerGroup::join_topics(
        ConsumerConfig::bootstrap([bootstrap])
            .isolation(IsolationLevel::ReadCommitted)
            .max_wait_ms(500),
        group_id,
        [source],
    )
    .await?;

    loop {
        let recs = group.poll().await?;
        if recs.is_empty() {
            continue;
        }
        if let Err(e) = copy_batch(&producer, &group, &recs, &dest).await {
            drop(producer.abort_transaction().await);
            group.leave().await?;
            producer.close().await?;
            return Err(e);
        }
        for rec in &recs {
            println!("{}-{}@{} -> {dest}", rec.topic, rec.partition, rec.offset);
        }
    }
}

async fn copy_batch(
    producer: &Producer,
    group: &ConsumerGroup,
    recs: &ConsumerRecords,
    dest: &str,
) -> partitionline::Result<()> {
    producer.begin_transaction().await?;
    for rec in recs {
        let mut out = ProduceRecord::to(dest);
        if let Some(key) = rec.key.clone() {
            out = out.key(key);
        }
        if let Some(value) = rec.value.clone() {
            out = out.value(value);
        }
        drop(producer.send(out).await?);
    }
    producer
        .send_offsets_for_group(&group.group_metadata(), recs.next_offsets())
        .await?;
    producer.commit_transaction().await?;
    Ok(())
}
