//! Produce one record inside a transaction.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//! `KAFKA_TRANSACTIONAL_ID` defaults to `partitionline-txn`.

use std::time::Duration;

use partitionline::{ProduceRecord, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let transactional_id =
        std::env::var("KAFKA_TRANSACTIONAL_ID").unwrap_or_else(|_| "partitionline-txn".into());

    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap])
            .transactional_id(transactional_id)
            .linger(Duration::ZERO),
    )
    .await?;
    producer.init_transactions().await?;
    producer.begin_transaction().await?;
    let md = producer
        .send(ProduceRecord::to(topic).value(&b"hello from a transaction"[..]))
        .await?;
    producer.commit_transaction().await?;
    println!("{}-{}@{}", md.topic, md.partition, md.offset);
    producer.close().await?;
    Ok(())
}
