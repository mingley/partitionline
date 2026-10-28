//! Pause and resume assigned partitions without dropping the assignment.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).

use partitionline::Consumer;

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let mut consumer = Consumer::connect(bootstrap).await?;
    consumer.assign_topic(topic, 0).await?;
    let assigned = consumer.assignment();
    println!("assigned {assigned:?}");
    if let Some(tp) = assigned.first().cloned() {
        consumer.pause([tp.clone()]);
        println!("paused {:?}", consumer.paused());
        let recs = consumer.fetch().await?;
        println!("while paused: {} records", recs.len());
        consumer.resume([tp]);
    }
    let recs = consumer.fetch().await?;
    println!("after resume: {} records", recs.len());
    consumer.close().await?;
    Ok(())
}
