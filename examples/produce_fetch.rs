//! Produce then fetch using kafka-protocol types. Needs a broker.
//!
//! ```bash
//! docker compose up -d
//! cargo run --example produce_fetch -- localhost:9092 bench-pl
//! ```

use bytes::Bytes;
use partitionline::{Client, Fetcher, Producer};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().unwrap_or_else(|| "localhost:9092".into());
    let topic = args
        .next()
        .unwrap_or_else(|| "partitionline-example".into());

    let client = Client::connect([&bootstrap]).await?;
    let producer = Producer::new(client);
    let sent = producer
        .send_to(
            &topic,
            0,
            None,
            Some(Bytes::from_static(b"hello-from-partitionline")),
        )
        .await?;
    println!(
        "produced {}/{} offset={}",
        sent.topic, sent.partition, sent.base_offset
    );

    // New client so we do not fight Producer's ownership. Metadata is cheap.
    let client = Client::connect([&bootstrap]).await?;
    let mut fetcher = Fetcher::new(client);
    let fetched = fetcher.fetch(&topic, 0, sent.base_offset).await?;
    println!(
        "fetched {} records hw={}",
        fetched.records.len(),
        fetched.high_watermark
    );
    for r in fetched.records {
        println!("  offset={} value={:?}", r.offset, r.value);
    }
    Ok(())
}
