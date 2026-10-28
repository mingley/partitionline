//! Fetch until the process is stopped. Broker on `KAFKA_BOOTSTRAP`.

use partitionline::Consumer;

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let mut consumer = Consumer::connect(bootstrap).await?;
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
