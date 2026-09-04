//! Live KIP-848 join probe against a real broker.
//!
//! Ignored by default. Run with:
//! `RUN_LIVE=1 KAFKA_BOOTSTRAP=127.0.0.1:9092 cargo test --test kip848_live -- --ignored --nocapture`

use partitionline::{ConsumerConfig, ConsumerGroup};

#[tokio::test]
#[ignore = "needs a live Kafka 4.x broker with ConsumerGroupHeartbeat"]
async fn kip848_join_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "pl-kip848-smoke".into());
    let group = format!("pl-kip848-probe-{}", std::process::id());
    let cfg = ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500);
    let mut g = ConsumerGroup::join_consumer_topics(cfg, group, [topic])
        .await
        .expect("KIP-848 join");
    let recs = g.poll().await.expect("poll");
    // Seeded topic may be empty if another run consumed; join success is the bar.
    eprintln!("kip848 live: join+poll ok (n={})", recs.len());
}
