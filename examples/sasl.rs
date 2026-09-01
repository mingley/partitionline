//! Produce one record over SASL (PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512).
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//! `SASL_MECHANISM` defaults to `SCRAM-SHA-256`. `KAFKA_USERNAME` /
//! `KAFKA_PASSWORD` default to `alice` / `secret`.

use partitionline::{ProduceRecord, Producer, ProducerConfig, Sasl};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let username = std::env::var("KAFKA_USERNAME").unwrap_or_else(|_| "alice".into());
    let password = std::env::var("KAFKA_PASSWORD").unwrap_or_else(|_| "secret".into());
    let mechanism = std::env::var("SASL_MECHANISM").unwrap_or_else(|_| "SCRAM-SHA-256".into());
    let sasl = match mechanism.as_str() {
        "PLAIN" => Sasl::plain(username, password),
        "SCRAM-SHA-256" => Sasl::scram_sha256(username, password),
        "SCRAM-SHA-512" => Sasl::scram_sha512(username, password),
        other => {
            return Err(partitionline::Error::protocol(format!(
                "unsupported SASL_MECHANISM {other}; use PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512"
            )));
        }
    };

    let producer = Producer::new(ProducerConfig::bootstrap([bootstrap]).sasl(sasl)).await?;
    let md = producer
        .send(ProduceRecord::to(topic).value(&b"hello over sasl"[..]))
        .await?;
    println!("{}-{}@{}", md.topic, md.partition, md.offset);
    producer.close().await?;
    Ok(())
}
