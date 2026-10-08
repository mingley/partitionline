//! Produce one record over SASL (PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512).
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//! `SASL_MECHANISM` defaults to `SCRAM-SHA-256`. `KAFKA_USERNAME` /
//! `KAFKA_PASSWORD` default to `alice` / `secret`.
//!
//! Set `TLS_CA_PEM` (and optionally `TLS_SERVER_NAME`) to use SASL over TLS
//! (`SASL_SSL`) — the common production path. See `scripts/ci-auth-smoke.sh`.
//!
//! For OAUTHBEARER / OIDC, see `examples/oauth.rs`.

use partitionline::{ProduceRecord, Producer, ProducerConfig, Sasl, TlsConfig};

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

    let mut cfg = ProducerConfig::bootstrap([bootstrap]).sasl(sasl);
    if let Ok(ca_path) = std::env::var("TLS_CA_PEM") {
        let mut tls =
            TlsConfig::default().ca_pem(tokio::fs::read(&ca_path).await.map_err(|e| {
                partitionline::Error::protocol(format!("read TLS_CA_PEM {ca_path}: {e}"))
            })?);
        if let Ok(name) = std::env::var("TLS_SERVER_NAME") {
            if !name.is_empty() {
                tls = tls.server_name(name);
            }
        }
        cfg = cfg.tls(tls);
    }

    let producer = Producer::new(cfg).await?;
    let md = producer
        .send(ProduceRecord::to(topic).value(&b"hello over sasl"[..]))
        .await?;
    println!("{}-{}@{}", md.topic, md.partition, md.offset);
    producer.close().await?;
    Ok(())
}
