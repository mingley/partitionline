//! Create a topic (if needed) and list cluster topics.
//!
//! Broker on `KAFKA_BOOTSTRAP`. Topic `KAFKA_TOPIC` (default `partitionline`).

use partitionline::{Admin, NewTopic};

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
    admin.close().await?;
    Ok(())
}
