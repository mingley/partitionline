use std::time::{Duration, Instant};

use bytes::Bytes;
use partitionline::{Compression, ProduceRecord, Producer, ProducerConfig};

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
            "{{\"acked\":{acked},\"elapsed_s\":{elapsed:.6},\"acked_rec_s\":{rec_s:.3},\"payload_bytes\":{payload},\"acks\":{acks},\"linger_ms\":{linger_ms},\"compression\":\"{}\"}}",
            compression.as_str()
        );
    } else {
        let _ = drive(&producer, &topic, &value, warmup).await?;
        let start = Instant::now();
        let acked = drive(&producer, &topic, &value, measure).await?;
        let elapsed = start.elapsed().as_secs_f64();
        let rec_s = acked as f64 / elapsed;
        println!(
            "{{\"acked\":{acked},\"elapsed_s\":{elapsed:.6},\"acked_rec_s\":{rec_s:.3},\"payload_bytes\":{payload},\"acks\":{acks},\"linger_ms\":{linger_ms},\"compression\":\"{}\"}}",
            compression.as_str()
        );
    }
    producer.close().await?;
    Ok(())
}
