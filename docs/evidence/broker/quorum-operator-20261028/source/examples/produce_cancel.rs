//! Cancel one send wait, settle queued work, then close with deadlines.
//! KAFKA_BOOTSTRAP defaults to 127.0.0.1:9092; KAFKA_TOPIC to partitionline.
//! A cancelled wait has no per-record receipt and must not trigger blind resend.
use partitionline::{Acks, Error, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

async fn run() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap])
            .acks(Acks::All)
            .linger(Duration::from_millis(200))
            .connect_timeout(Duration::from_secs(2))
            .request_timeout(Duration::from_secs(2))
            .max_block(Duration::from_secs(1))
            .delivery_timeout(Duration::from_secs(5)),
    )
    .await?;
    let mut delivery =
        Box::pin(producer.send(ProduceRecord::to(topic).value(&b"cancel-recipe"[..])));
    let outcome = tokio::time::timeout(Duration::from_millis(20), &mut delivery).await;
    drop(delivery);
    match &outcome {
        Ok(Ok(md)) => println!(
            "acknowledged partition={} offset={}",
            md.partition, md.offset
        ),
        Ok(Err(error)) => eprintln!("send failed: {error}"),
        Err(_) => println!("send wait cancelled; delivery outcome ambiguous; no resend"),
    }
    // Capture flush separately: close reports a shutdown timeout but is not a
    // substitute for retaining each send result or the flush delivery error.
    let settled = producer.flush_timeout(Duration::from_secs(5)).await;
    let closed = producer.close_timeout(Duration::from_secs(2)).await;
    settled?;
    closed?;
    if let Ok(Err(error)) = outcome {
        return Err(error);
    }
    println!("queued work settled and producer closed; cancelled send has no per-record receipt");
    Ok(())
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), run())
        .await
        .map_err(|_| Error::Timeout)?
}
