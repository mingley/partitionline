//! Print beginning / end offsets, lag, and committed metadata.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).

use partitionline::{
    Consumer, ConsumerConfig, ConsumerGroup, OffsetAndMetadata, ProduceRecord, Producer,
    ProducerConfig, TopicPartition,
};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());

    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap.clone()]).linger(std::time::Duration::ZERO),
    )
    .await?;
    let md = producer
        .send(ProduceRecord::to(topic.clone()).value(&b"hello"[..]))
        .await?;
    println!("produced {}-{}@{}", md.topic, md.partition, md.offset);
    producer.close().await?;

    let mut consumer = Consumer::connect(bootstrap.clone()).await?;
    consumer.assign(&topic, 0, 0).await?;
    let begin = consumer.beginning_offsets([(topic.as_str(), 0)]).await?;
    let end = consumer.end_offsets([(topic.as_str(), 0)]).await?;
    let lag = consumer.current_lag((topic.as_str(), 0)).await?;
    println!("begin={begin:?} end={end:?} lag={lag:?}");
    consumer.close().await?;

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500),
        "partitionline-offsets",
        topic.clone(),
    )
    .await?;
    let recs = group.poll().await?;
    if let Some(rec) = recs.first() {
        let tp = TopicPartition::new(&rec.topic, rec.partition);
        group
            .commit_with_metadata([(
                tp,
                OffsetAndMetadata::with_metadata(rec.offset + 1, "example"),
            )])
            .await?;
    }
    for (tp, md) in group.committed().await? {
        println!("{tp} committed={} meta={}", md.offset, md.metadata);
    }
    group.leave().await?;
    Ok(())
}
