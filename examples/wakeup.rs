//! Interrupt an in-flight fetch from another task.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).
//! Ctrl-C (or any second task holding [`partitionline::WakeupHandle`]) stops
//! `fetch` with [`partitionline::Error::Wakeup`].

use partitionline::Consumer;

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let mut consumer = Consumer::connect(bootstrap).await?;
    consumer.assign_topic(topic, 0).await?;
    let wakeup = consumer.wakeup_handle();
    drop(tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap_or(());
        wakeup.wakeup();
    }));
    loop {
        match consumer.fetch().await {
            Ok(recs) => {
                for rec in recs {
                    println!("{}-{}@{}", rec.topic, rec.partition, rec.offset);
                }
            }
            Err(partitionline::Error::Wakeup) => break,
            Err(e) => return Err(e),
        }
    }
    consumer.close().await?;
    Ok(())
}
