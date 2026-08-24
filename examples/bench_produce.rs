use std::time::{Duration, Instant};

use bytes::Bytes;
use partitionline::{Compression, ProduceRecord, Producer, ProducerConfig, TlsConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let payload = std::env::var("PAYLOAD_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100usize);
    let warmup = Duration::from_secs(
        std::env::var("WARMUP_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2),
    );
    let measure = Duration::from_secs(
        std::env::var("MEASURE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5),
    );
    let linger_ms = std::env::var("LINGER_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5u64);
    let acks = std::env::var("ACKS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1i16);

    let mut cfg = ProducerConfig::bootstrap([bootstrap]);
    cfg.linger = Duration::from_millis(linger_ms);
    cfg.batch_records = 32_768;
    cfg.batch_bytes = 1_000_000;
    cfg.acks = acks;
    cfg.connections = std::env::var("CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    cfg.max_in_flight = std::env::var("MAX_IN_FLIGHT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let compression =
        Compression::from_name(&std::env::var("COMPRESSION").unwrap_or_else(|_| "none".into()))?;
    cfg.compression = compression;
    let idempotent = std::env::var("IDEMPOTENT").ok().as_deref() == Some("1");
    if idempotent {
        cfg.enable_idempotence = true;
    }
    let tls_on = if let Ok(ca_path) = std::env::var("TLS_CA_PEM") {
        let mut tls = TlsConfig {
            ca_pem: Some(std::fs::read(&ca_path).map_err(|e| {
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
        true
    } else {
        false
    };
    let mut scram_on = false;
    if let (Ok(user), Ok(pass)) = (
        std::env::var("SASL_USERNAME"),
        std::env::var("SASL_PASSWORD"),
    ) {
        let mech = std::env::var("SASL_MECHANISM").unwrap_or_else(|_| "PLAIN".into());
        match mech.as_str() {
            "SCRAM-SHA-256" => {
                cfg.sasl_scram = Some((user, pass));
                scram_on = true;
            }
            "PLAIN" => cfg.sasl_plain = Some((user, pass)),
            other => {
                return Err(partitionline::Error::protocol(format!(
                    "unknown SASL_MECHANISM {other}"
                )));
            }
        }
    }
    let acks_out = if idempotent { -1 } else { acks };
    let producer = Producer::new(cfg).await?;
    let topic: std::sync::Arc<str> = topic.into();
    let value = Bytes::from(vec![b'x'; payload]);
    let count: Option<u64> = std::env::var("COUNT").ok().and_then(|s| s.parse().ok());

    async fn send_one(
        producer: &Producer,
        topic: &std::sync::Arc<str>,
        value: &Bytes,
        spins: &mut u32,
    ) -> partitionline::Result<bool> {
        match producer.try_send(ProduceRecord::to(topic.clone()).value(value.clone())) {
            Ok(()) => {
                *spins = 0;
                Ok(true)
            }
            Err(partitionline::Error::QueueFull) => {
                *spins += 1;
                if *spins > 32 {
                    tokio::task::yield_now().await;
                    *spins = 0;
                }
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    async fn drive(
        producer: &Producer,
        topic: &std::sync::Arc<str>,
        value: &Bytes,
        dur: Duration,
    ) -> partitionline::Result<u64> {
        if dur.is_zero() {
            producer.flush().await?;
            return Ok(0);
        }
        let deadline = Instant::now() + dur;
        let mut sent = 0u64;
        let mut spins = 0u32;
        loop {
            for _ in 0..1024 {
                if send_one(producer, topic, value, &mut spins).await? {
                    sent += 1;
                }
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        producer.flush().await?;
        Ok(sent)
    }

    async fn drive_count(
        producer: &Producer,
        topic: &std::sync::Arc<str>,
        value: &Bytes,
        n: u64,
    ) -> partitionline::Result<u64> {
        let mut sent = 0u64;
        let mut spins = 0u32;
        while sent < n {
            if send_one(producer, topic, value, &mut spins).await? {
                sent += 1;
            }
        }
        producer.flush().await?;
        Ok(sent)
    }

    if let Some(n) = count {
        let _ = drive(&producer, &topic, &value, warmup).await?;
        let start = Instant::now();
        let acked = drive_count(&producer, &topic, &value, n).await?;
        let elapsed = start.elapsed().as_secs_f64();
        let rec_s = acked as f64 / elapsed;
        println!(
            "{{\"acked\":{acked},\"elapsed_s\":{elapsed:.6},\"acked_rec_s\":{rec_s:.3},\"payload_bytes\":{payload},\"acks\":{acks_out},\"linger_ms\":{linger_ms},\"compression\":\"{}\",\"idempotent\":{},\"tls\":{},\"scram\":{}}}",
            compression.as_str(),
            idempotent,
            tls_on,
            scram_on
        );
    } else {
        let _ = drive(&producer, &topic, &value, warmup).await?;
        let start = Instant::now();
        let acked = drive(&producer, &topic, &value, measure).await?;
        let elapsed = start.elapsed().as_secs_f64();
        let rec_s = acked as f64 / elapsed;
        println!(
            "{{\"acked\":{acked},\"elapsed_s\":{elapsed:.6},\"acked_rec_s\":{rec_s:.3},\"payload_bytes\":{payload},\"acks\":{acks_out},\"linger_ms\":{linger_ms},\"compression\":\"{}\",\"idempotent\":{},\"tls\":{},\"scram\":{}}}",
            compression.as_str(),
            idempotent,
            tls_on,
            scram_on
        );
    }
    producer.close().await?;
    Ok(())
}
