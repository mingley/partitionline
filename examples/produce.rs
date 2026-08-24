use partitionline::{ProduceRecord, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let producer = Producer::new(ProducerConfig::bootstrap([bootstrap])).await?;
    let md = producer
        .send(ProduceRecord::to(topic).value(&b"hello from partitionline"[..]))
        .await?;
    println!("{}-{}@{}", md.topic, md.partition, md.offset);
    producer.close().await?;
    Ok(())
}
