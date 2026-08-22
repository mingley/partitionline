//! Lab A produce harness. See BENCH.md.
//!
//! Locked published knobs: acks=all, linger=50, compression=none,
//! idempotent=true. Default window 60s warmup + 180s measured.
//!
//! Measures produce-ack latency (enqueue → delivery), not time-to-queue.
//! Uses `Producer::enqueue` so linger/batch can fill. Refuses to print a
//! comparison table; it only prints this client's measured window.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use partitionline::{Acks, Compression, Producer, RecordTo};

fn usage() -> ! {
    eprintln!(
        "usage: bench --bootstrap HOST:PORT --topic NAME --size BYTES \
         --seconds N --warmup N --linger-ms N --inflight N [--csv PATH]"
    );
    std::process::exit(2);
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone())
}

fn req_arg(args: &[String], name: &str) -> String {
    arg(args, name).unwrap_or_else(|| usage())
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn incompressible(len: usize) -> Bytes {
    let mut buf = vec![0u8; len];
    let mut x = 0xC0FFEE_u64.wrapping_mul(len as u64 + 1);
    for b in &mut buf {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        *b = (x >> 33) as u8;
    }
    Bytes::from(buf)
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        usage();
    }
    let bootstrap = req_arg(&args, "--bootstrap");
    let topic = req_arg(&args, "--topic");
    let size: usize = req_arg(&args, "--size").parse().unwrap_or_else(|_| usage());
    let seconds: u64 = req_arg(&args, "--seconds")
        .parse()
        .unwrap_or_else(|_| usage());
    let warmup: u64 = req_arg(&args, "--warmup")
        .parse()
        .unwrap_or_else(|_| usage());
    let linger_ms: u64 = req_arg(&args, "--linger-ms")
        .parse()
        .unwrap_or_else(|_| usage());
    let inflight: usize = req_arg(&args, "--inflight")
        .parse()
        .unwrap_or_else(|_| usage());
    let csv = arg(&args, "--csv").map(PathBuf::from);

    if size == 0 || seconds == 0 || inflight == 0 {
        usage();
    }

    let payload = incompressible(size);
    // acks=all: librdkafka 2.15.0 refuses acks=1 when enable.idempotence=true.
    // Both clients use all so the knobs match. See results/lab-a.md.
    let producer = Producer::builder([&bootstrap])
        .acks(Acks::All)
        .linger(Duration::from_millis(linger_ms))
        .batch_size(1_000_000)
        .compression(Compression::None)
        .idempotent(true)
        .build()
        .await?;

    eprintln!(
        "partitionline produce: bootstrap={bootstrap} topic={topic} size={size} \
         linger_ms={linger_ms} acks=all compression=none idempotent=true \
         batch.size=1000000 inflight={inflight} warmup={warmup}s measured={seconds}s"
    );

    run_window(
        &producer,
        &topic,
        payload.clone(),
        Duration::from_secs(warmup),
        inflight,
        None,
    )
    .await?;
    eprintln!("warmup discarded");

    let stats = run_window(
        &producer,
        &topic,
        payload,
        Duration::from_secs(seconds),
        inflight,
        Some("measured"),
    )
    .await?;
    producer.flush().await?;

    println!(
        "client=partitionline recs={} bytes={} seconds={:.3} rec_s={:.1} mib_s={:.3} \
         p50_us={} p99_us={} p999_us={}",
        stats.recs,
        stats.bytes,
        stats.secs,
        stats.rec_s,
        stats.mib_s,
        stats.p50_us,
        stats.p99_us,
        stats.p999_us
    );

    if let Some(path) = csv {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let line = format!(
            "client,recs,bytes,seconds,rec_s,mib_s,p50_us,p99_us,p999_us\n\
             partitionline,{},{},{:.6},{:.6},{:.6},{},{},{}\n",
            stats.recs,
            stats.bytes,
            stats.secs,
            stats.rec_s,
            stats.mib_s,
            stats.p50_us,
            stats.p99_us,
            stats.p999_us
        );
        std::fs::write(&path, line)?;
        eprintln!("wrote {}", path.display());
    }
    Ok(())
}

struct Stats {
    recs: u64,
    bytes: u64,
    secs: f64,
    rec_s: f64,
    mib_s: f64,
    p50_us: u64,
    p99_us: u64,
    p999_us: u64,
}

async fn run_window(
    producer: &Producer,
    topic: &str,
    payload: Bytes,
    window: Duration,
    max_inflight: usize,
    label: Option<&str>,
) -> partitionline::Result<Stats> {
    let mut inflight: VecDeque<(Instant, partitionline::Delivery)> =
        VecDeque::with_capacity(max_inflight);
    let mut latencies = Vec::new();
    let mut recs = 0u64;
    let mut bytes = 0u64;
    let start = Instant::now();
    let end = start + window;

    while Instant::now() < end {
        while inflight.len() >= max_inflight {
            drain_one(
                &mut inflight,
                &mut latencies,
                &mut recs,
                &mut bytes,
                payload.len(),
            )
            .await?;
        }
        let rec = RecordTo::to(topic).payload(payload.clone());
        let t0 = Instant::now();
        let d = producer.enqueue(rec)?;
        inflight.push_back((t0, d));
    }
    while !inflight.is_empty() {
        drain_one(
            &mut inflight,
            &mut latencies,
            &mut recs,
            &mut bytes,
            payload.len(),
        )
        .await?;
    }

    let secs = start.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let rec_s = recs as f64 / secs;
    let mib_s = (bytes as f64 / secs) / (1024.0 * 1024.0);
    let stats = Stats {
        recs,
        bytes,
        secs,
        rec_s,
        mib_s,
        p50_us: percentile(&latencies, 0.50),
        p99_us: percentile(&latencies, 0.99),
        p999_us: percentile(&latencies, 0.999),
    };
    if let Some(l) = label {
        eprintln!(
            "{l}: recs={} rec_s={:.1} mib_s={:.3} p50_us={} p99_us={} p999_us={}",
            stats.recs, stats.rec_s, stats.mib_s, stats.p50_us, stats.p99_us, stats.p999_us
        );
    }
    Ok(stats)
}

async fn drain_one(
    inflight: &mut VecDeque<(Instant, partitionline::Delivery)>,
    latencies: &mut Vec<u64>,
    recs: &mut u64,
    bytes: &mut u64,
    payload_len: usize,
) -> partitionline::Result<()> {
    let (t0, d) = inflight.pop_front().expect("non-empty");
    d.await?;
    latencies.push(t0.elapsed().as_micros() as u64);
    *recs += 1;
    *bytes += payload_len as u64;
    Ok(())
}
