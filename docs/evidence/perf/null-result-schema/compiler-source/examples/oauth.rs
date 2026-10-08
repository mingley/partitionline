//! Produce one record with SASL OAUTHBEARER (unsecured JWT or OIDC token).
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//!
//! Unsecured JWT (local / test brokers that accept librdkafka-style tokens):
//!
//! ```text
//! SASL_OAUTH_PRINCIPAL=alice cargo run --example oauth
//! ```
//!
//! OIDC client-credentials (token URL must be reachable from this process):
//!
//! ```text
//! OIDC_TOKEN_URL=https://issuer.example/oauth/token \
//! OIDC_CLIENT_ID=… OIDC_CLIENT_SECRET=… \
//! cargo run --example oauth
//! ```
//!
//! Set `TLS_CA_PEM` (and optionally `TLS_SERVER_NAME`) for SASL_SSL — see
//! `scripts/ci-auth-smoke.sh`.

use partitionline::{OidcConfig, ProduceRecord, Producer, ProducerConfig, Sasl, TlsConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());

    let sasl = if let (Ok(url), Ok(id), Ok(secret)) = (
        std::env::var("OIDC_TOKEN_URL"),
        std::env::var("OIDC_CLIENT_ID"),
        std::env::var("OIDC_CLIENT_SECRET"),
    ) {
        Sasl::oidc(OidcConfig::new(url, id, secret))
    } else {
        let principal = std::env::var("SASL_OAUTH_PRINCIPAL").unwrap_or_else(|_| "alice".into());
        Sasl::oauthbearer(principal)
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
        .send(ProduceRecord::to(topic).value(&b"hello over oauthbearer"[..]))
        .await?;
    println!("{}-{}@{}", md.topic, md.partition, md.offset);
    producer.close().await?;
    Ok(())
}
