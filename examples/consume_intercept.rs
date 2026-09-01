//! Drop heartbeat records before they reach the caller.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).

use partitionline::{Consumer, ConsumerConfig, ConsumerInterceptor, FetchedRecord};

struct SkipHeartbeats;

impl ConsumerInterceptor for SkipHeartbeats {
    fn on_consume(&self, recs: Vec<FetchedRecord>) -> Vec<FetchedRecord> {
        recs.into_iter()
            .filter(|rec| {
                rec.last_header("kind").and_then(|h| h.value.as_deref()) != Some(b"heartbeat")
            })
            .collect()
    }
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([bootstrap]).interceptor(SkipHeartbeats)).await?;
    consumer.assign_topic(topic, 0).await?;
    loop {
        for rec in consumer.fetch().await? {
            println!(
                "{}-{}@{} bytes={}",
                rec.topic,
                rec.partition,
                rec.offset,
                rec.value.as_ref().map(|v| v.len()).unwrap_or(0)
            );
        }
    }
}
