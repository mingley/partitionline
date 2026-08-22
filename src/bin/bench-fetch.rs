//! Lab A fetch harness. See BENCH.md.
//!
//! Produce first (completed), then consume from earliest. Measures fetch
//! rec/s and MiB/s only. Refuses to print a comparison table. Does not
//! invent C numbers. e2e send-timestamp latency is not this binary.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use partitionline::{Acks, Client, Compression, Fetcher, Producer, RecordTo};

fn usage() -> ! {
    eprintln!(
        "usage: bench-fetch --bootstrap HOST:PORT --topic NAME --size BYTES \
         --produce-seconds N --linger-ms N --inflight N [--csv PATH]"
    );
    std::process::exit(2);
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone())
}

fn req_arg(args: &[String], name: &str) -> String {
    arg(args, name).unwrap_or_else(|| usage())
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
    let produce_seconds: u64 = req_arg(&args, "--produce-seconds")
        .parse()
        .unwrap_or_else(|_| usage());
    let linger_ms: u64 = req_arg(&args, "--linger-ms")
        .parse()
        .unwrap_or_else(|_| usage());
    let inflight: usize = req_arg(&args, "--inflight")
        .parse()
        .unwrap_or_else(|_| usage());
    let csv = arg(&args, "--csv").map(PathBuf::from);

    if size == 0 || inflight == 0 {
        usage();
    }

    let payload = incompressible(size);
    if produce_seconds > 0 {
        let producer = Producer::builder([&bootstrap])
            .acks(Acks::All)
            .linger(Duration::from_millis(linger_ms))
            .batch_size(1_000_000)
            .compression(Compression::None)
            .idempotent(true)
            .build()
            .await?;
        let produced = produce_fill(
            &producer,
            &topic,
            payload,
            Duration::from_secs(produce_seconds),
            inflight,
        )
        .await?;
        producer.flush().await?;
        eprintln!(
            "produce completed: recs={produced} (not the fetch table)"
        );
    }

    let client = Client::connect([&bootstrap]).await?;
    let n = client
        .partition_count(&topic)
        .ok_or_else(|| partitionline::Error::protocol("topic missing from metadata"))?;
    let start: Vec<(i32, i64)> = (0..n).map(|p| (p, 0)).collect();
    let mut fetcher = Fetcher::new(client)
        .max_wait_ms(100)
        .min_bytes(1)
        .partition_max_bytes(1_048_576);

    eprintln!(
        "partitionline fetch: bootstrap={bootstrap} topic={topic} size={size} \
         partitions={n} from earliest (offset 0) max_wait_ms=100 \
         partition_max_bytes=1048576"
    );

    let t0 = Instant::now();
    let fetched = fetcher.consume_to_hw(&topic, &start).await?;
    let secs = t0.elapsed().as_secs_f64();
    let recs: u64 = fetched
        .iter()
        .map(|f| f.records.iter().filter(|r| !r.control).count() as u64)
        .sum();
    let bytes: u64 = fetched
        .iter()
        .flat_map(|f| f.records.iter())
        .filter(|r| !r.control)
        .map(|r| r.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64)
        .sum();
    let rec_s = if secs > 0.0 { recs as f64 / secs } else { 0.0 };
    let mib_s = if secs > 0.0 {
        (bytes as f64 / secs) / (1024.0 * 1024.0)
    } else {
        0.0
    };

    println!(
        "client=partitionline-fetch recs={recs} bytes={bytes} seconds={secs:.3} \
         rec_s={rec_s:.1} mib_s={mib_s:.3}"
    );

    if let Some(path) = csv {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let line = format!(
            "client,recs,bytes,seconds,rec_s,mib_s\n\
             partitionline-fetch,{recs},{bytes},{secs:.6},{rec_s:.6},{mib_s:.6}\n"
        );
        std::fs::write(&path, line)?;
        eprintln!("wrote {}", path.display());
    }
    Ok(())
}

async fn produce_fill(
    producer: &Producer,
    topic: &str,
    payload: Bytes,
    window: Duration,
    max_inflight: usize,
) -> partitionline::Result<u64> {
    let mut inflight: VecDeque<partitionline::Delivery> = VecDeque::with_capacity(max_inflight);
    let mut recs = 0u64;
    let end = Instant::now() + window;
    while Instant::now() < end {
        while inflight.len() >= max_inflight {
            inflight.pop_front().expect("non-empty").await?;
            recs += 1;
        }
        let d = producer.enqueue(RecordTo::to(topic).payload(payload.clone()))?;
        inflight.push_back(d);
    }
    while let Some(d) = inflight.pop_front() {
        d.await?;
        recs += 1;
    }
    Ok(recs)
}
