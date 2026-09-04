//! Create a topic (if needed) and list cluster topics.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).

use partitionline::{Admin, Error, NewTopic};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());

    let mut admin = Admin::connect(bootstrap).await?;
    let created = admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], 30_000, false)
        .await?;
    for result in &created {
        println!("{} error={}", result.name, result.error_code);
    }
    for listing in admin.list_topics_with(false).await? {
        println!("{}", listing.name());
    }
    match admin.describe_metadata_quorum().await {
        Ok(quorum) => println!(
            "quorum leader={} epoch={} hw={}",
            quorum.leader_id(),
            quorum.leader_epoch(),
            quorum.high_watermark()
        ),
        Err(Error::Unsupported(msg)) => {
            println!("describe_metadata_quorum skipped: {msg}");
        }
        Err(e) => return Err(e),
    }
    admin.close().await?;
    Ok(())
}
