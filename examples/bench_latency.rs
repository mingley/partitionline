//! Locked produce-ack and fetch-request latency example.
//!
//! Sequential `Producer::send` (enqueue to Produce ack) and non-empty
//! `Consumer::fetch` (already-on-log Fetch RPC). Prints one JSON object
//! per kind with p50/p99 in microseconds. Not a throughput bench.

use std::time::{Duration, Instant};

use bytes::Bytes;
use partitionline::{Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn percentile_us(sorted: &[u64], p: u32) -> partitionline::Result<u64> {
    if sorted.is_empty() {
        return Err(partitionline::Error::protocol("no latency samples"));
    }
    let n = sorted.len();
    let rank = n
        .saturating_mul(p as usize)
        .div_ceil(100)
        .saturating_sub(1)
        .min(n.saturating_sub(1));
    sorted
        .get(rank)
        .copied()
        .ok_or_else(|| partitionline::Error::protocol("percentile index"))
}

fn print_latency(kind: &str, mut samples: Vec<u64>, extra: &str) -> partitionline::Result<()> {
    if samples.is_empty() {
        return Err(partitionline::Error::protocol(format!(
            "{kind}: no latency samples"
        )));
    }
    samples.sort_unstable();
    let n = samples.len();
    let min_us = samples.first().copied().unwrap_or(0);
    let max_us = samples.last().copied().unwrap_or(0);
    let sum: u128 = samples.iter().map(|v| u128::from(*v)).sum();
    let mean_us = u64::try_from(sum / u128::from(n as u64))
        .map_err(|_| partitionline::Error::protocol("mean overflow"))?;
    let p50_us = percentile_us(&samples, 50)?;
    let p99_us = percentile_us(&samples, 99)?;
    println!(
        "{{\"kind\":\"{kind}\",\"samples\":{n},\"p50_us\":{p50_us},\"p99_us\":{p99_us},\"min_us\":{min_us},\"max_us\":{max_us},\"mean_us\":{mean_us}{extra}}}"
    );
    Ok(())
}

async fn produce_ack(
    producer: &Producer,
    topic: &str,
    value: &Bytes,
    n: u64,
) -> partitionline::Result<Vec<u64>> {
    let mut samples = Vec::with_capacity(usize::try_from(n).unwrap_or(0));
    for _ in 0..n {
        let start = Instant::now();
        let _md = producer
            .send(ProduceRecord::to(topic).value(value.clone()))
            .await?;
        samples.push(u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(samples)
}

async fn fetch_rpc(consumer: &mut Consumer, count: u64) -> partitionline::Result<(Vec<u64>, u64)> {
    let mut samples = Vec::new();
    let mut got = 0u64;
    let mut empty = 0u32;
    while got < count {
        let start = Instant::now();
        let recs = consumer.fetch().await?;
        let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        if recs.is_empty() {
            empty += 1;
            if empty > 600 {
                return Err(partitionline::Error::Timeout);
            }
            continue;
        }
        empty = 0;
        samples.push(elapsed);
        got += recs.len() as u64;
    }
    Ok((samples, got))
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = env_or("KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let topic = env_or("KAFKA_TOPIC", "pllat");
    let payload = env_parse("PAYLOAD_BYTES", 100usize);
    let warmup = env_parse("WARMUP", 1_000u64);
    let count = env_parse("COUNT", 10_000u64);
    let linger_ms = env_parse("LINGER_MS", 0u64);
    let acks = env_parse("ACKS", 1i16);
    let max_wait_ms = env_parse("MAX_WAIT_MS", 100i32);
    let max_bytes = env_parse("MAX_BYTES", 4_096i32);
    let min_bytes = env_parse("MIN_BYTES", 1i32);
    let mode = env_or("MODE", "both");

    let mut pcfg = ProducerConfig::bootstrap([bootstrap.clone()]);
    pcfg.linger = Duration::from_millis(linger_ms);
    pcfg.acks = acks;
    pcfg.batch_records = 1;
    pcfg.batch_bytes = payload.saturating_add(256).max(1);
    pcfg.connections = 1;
    pcfg.max_in_flight = 1;
    let producer = Producer::new(pcfg).await?;
    let value = Bytes::from(vec![b'x'; payload]);

    if warmup > 0 {
        let _ = produce_ack(&producer, &topic, &value, warmup).await?;
    }

    if mode == "produce" || mode == "both" {
        let samples = produce_ack(&producer, &topic, &value, count).await?;
        print_latency(
            "produce_ack",
            samples,
            &format!(
                ",\"payload_bytes\":{payload},\"acks\":{acks},\"linger_ms\":{linger_ms},\"client\":\"partitionline\""
            ),
        )?;
    }
    producer.close().await?;

    if mode == "fetch" || mode == "both" {
        if mode == "fetch" {
            let producer = Producer::new({
                let mut cfg = ProducerConfig::bootstrap([bootstrap.clone()]);
                cfg.linger = Duration::ZERO;
                cfg.acks = acks;
                cfg.batch_records = 1;
                cfg.connections = 1;
                cfg.max_in_flight = 1;
                cfg
            })
            .await?;
            let _ = produce_ack(&producer, &topic, &value, count).await?;
            producer.close().await?;
        }
        let mut ccfg = ConsumerConfig::bootstrap([bootstrap]);
        ccfg.max_wait_ms = max_wait_ms;
        ccfg.max_bytes = max_bytes;
        ccfg.min_bytes = min_bytes;
        let mut consumer = Consumer::new(ccfg).await?;
        consumer.assign(&topic, 0, 0).await?;
        let (samples, got) = fetch_rpc(&mut consumer, count).await?;
        print_latency(
            "fetch_rpc",
            samples,
            &format!(
                ",\"consumed\":{got},\"max_wait_ms\":{max_wait_ms},\"max_bytes\":{max_bytes},\"min_bytes\":{min_bytes},\"client\":\"partitionline\""
            ),
        )?;
    }
    Ok(())
}
