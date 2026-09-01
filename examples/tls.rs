//! Produce one record over TLS (rustls, no OpenSSL).
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//! `TLS_CA_PEM` is a CA bundle path for a private CA (local Kafka). Omit it
//! to trust Mozilla roots. `TLS_SERVER_NAME` overrides SNI / certificate
//! hostname.

use partitionline::{ProduceRecord, Producer, ProducerConfig, TlsConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let mut tls = TlsConfig::default();
    if let Ok(ca_path) = std::env::var("TLS_CA_PEM") {
        tls = tls.ca_pem(tokio::fs::read(&ca_path).await.map_err(|e| {
            partitionline::Error::protocol(format!("read TLS_CA_PEM {ca_path}: {e}"))
        })?);
    }
    if let Ok(name) = std::env::var("TLS_SERVER_NAME") {
        if !name.is_empty() {
            tls = tls.server_name(name);
        }
    }

    let producer = Producer::new(ProducerConfig::bootstrap([bootstrap]).tls(tls)).await?;
    let md = producer
        .send(ProduceRecord::to(topic).value(&b"hello over tls"[..]))
        .await?;
    println!("{}-{}@{}", md.topic, md.partition, md.offset);
    producer.close().await?;
    Ok(())
}
