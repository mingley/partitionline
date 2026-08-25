//! Locked fetch throughput example.

use std::time::Instant;

use partitionline::{Consumer, ConsumerConfig, TlsConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "plbench".into());
    let count: u64 = std::env::var("COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8_000_000);
    let max_wait_ms = std::env::var("MAX_WAIT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100i32);
    let max_bytes = std::env::var("MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16_777_216i32);
    let min_bytes = std::env::var("MIN_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1i32);

    let mut cfg = ConsumerConfig::bootstrap([bootstrap]);
    cfg.max_wait_ms = max_wait_ms;
    cfg.max_bytes = max_bytes;
    cfg.min_bytes = min_bytes;
    if let Ok(ca_path) = std::env::var("TLS_CA_PEM") {
        let mut tls = TlsConfig {
            ca_pem: Some(tokio::fs::read(&ca_path).await.map_err(|e| {
                partitionline::Error::protocol(format!("read TLS_CA_PEM {ca_path}: {e}"))
            })?),
            ..TlsConfig::default()
        };
        if let Ok(name) = std::env::var("TLS_SERVER_NAME") {
            if !name.is_empty() {
                tls.server_name = Some(name);
            }
        }
        cfg.tls = Some(tls);
    }
    let mech = std::env::var("SASL_MECHANISM").unwrap_or_else(|_| "PLAIN".into());
    if mech == "OAUTHBEARER" {
        let principal = std::env::var("SASL_OAUTH_PRINCIPAL").unwrap_or_else(|_| "alice".into());
        cfg.sasl_oauthbearer = Some(principal);
    } else if let (Ok(user), Ok(pass)) = (
        std::env::var("SASL_USERNAME"),
        std::env::var("SASL_PASSWORD"),
    ) {
        match mech.as_str() {
            "SCRAM-SHA-256" => cfg.sasl_scram = Some((user, pass)),
            "SCRAM-SHA-512" => cfg.sasl_scram_sha512 = Some((user, pass)),
            "PLAIN" => cfg.sasl_plain = Some((user, pass)),
            other => {
                return Err(partitionline::Error::protocol(format!(
                    "unknown SASL_MECHANISM {other}"
                )));
            }
        }
    }

    let mut consumer = Consumer::new(cfg).await?;
    consumer.assign_topic(&topic, 0).await?;
    let assigned = consumer.assignment().len();
    let start = Instant::now();
    let mut got = 0u64;
    let mut empty = 0u32;
    while got < count {
        let recs = consumer.fetch().await?;
        if recs.is_empty() {
            empty += 1;
            if empty > 600 {
                return Err(partitionline::Error::Timeout);
            }
            continue;
        }
        empty = 0;
        got += recs.len() as u64;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let rec_s = got as f64 / elapsed.max(1e-9);
    println!(
        "{{\"consumed\":{got},\"elapsed_s\":{elapsed:.6},\"consumed_rec_s\":{rec_s:.3},\"partitions\":{assigned},\"max_wait_ms\":{max_wait_ms},\"max_bytes\":{max_bytes}}}"
    );
    Ok(())
}
