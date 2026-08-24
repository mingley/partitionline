use std::time::{Duration, Instant};

use partitionline::{ProduceRecord, Producer, ProducerConfig};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let payload = std::env::var("PAYLOAD_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100usize);
    let inflight = std::env::var("INFLIGHT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000usize);
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
    let repeats: u32 = std::env::var("REPEATS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let mut cfg = ProducerConfig::bootstrap([bootstrap]);
    cfg.linger = Duration::from_millis(5);
    cfg.batch_records = 10_000;
    cfg.batch_bytes = 1_000_000;
    cfg.acks = 1;
    let producer = Producer::new(cfg).await?;
    let value = vec![b'x'; payload];

    async fn drive(
        producer: Producer,
        topic: String,
        value: Vec<u8>,
        inflight: usize,
        dur: Duration,
    ) -> partitionline::Result<u64> {
        let deadline = Instant::now() + dur;
        let mut set = JoinSet::new();
        let mut sent = 0u64;
        let mut acked = 0u64;
        while Instant::now() < deadline || !set.is_empty() {
            while Instant::now() < deadline && set.len() < inflight {
                let p = producer.clone();
                let t = topic.clone();
                let v = value.clone();
                set.spawn(async move { p.send(ProduceRecord::to(t).value(v)).await });
                sent += 1;
            }
            if let Some(res) = set.join_next().await {
                res.map_err(|e| partitionline::Error::protocol(e.to_string()))??;
                acked += 1;
            } else if Instant::now() >= deadline {
                break;
            }
        }
        let _ = sent;
        Ok(acked)
    }

    println!("warmup {warmup:?}");
    let _ = drive(
        producer.clone(),
        topic.clone(),
        value.clone(),
        inflight,
        warmup,
    )
    .await?;

    for i in 0..repeats {
        let start = Instant::now();
        let acked = drive(
            producer.clone(),
            topic.clone(),
            value.clone(),
            inflight,
            measure,
        )
        .await?;
        let elapsed = start.elapsed().as_secs_f64();
        let recs = acked as f64 / elapsed;
        println!(
            "window {i}: {acked} rec in {elapsed:.3}s => {recs:.0} rec/s payload={payload}B acks=1"
        );
    }
    producer.close().await?;
    Ok(())
}
