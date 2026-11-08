//! Real Kafka create-time receipt smoke through a response-delay proxy.
use partitionline::{Acks, ProduceRecord, Producer, ProducerConfig};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn producer_timestamp_real_kafka() {
    let endpoint = std::env::var("PL_LIVE_ENDPOINT").expect("PL_LIVE_ENDPOINT");
    let topic = std::env::var("PL_LIVE_TOPIC").expect("PL_LIVE_TOPIC");
    let marker = std::env::var("PL_LIVE_MARKER").expect("PL_LIVE_MARKER");
    let producer = Producer::new(
        ProducerConfig::bootstrap([endpoint])
            .idempotent(true)
            .acks(Acks::All)
            .connections(1)
            .batch_records(1)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(15))
            .delivery_timeout(Duration::from_secs(30)),
    )
    .await
    .expect("producer setup");
    let started = Instant::now();
    let metadata = tokio::time::timeout(
        Duration::from_secs(30),
        producer.send(ProduceRecord::to(topic.as_str()).partition(0).value(marker.clone())),
    )
    .await
    .expect("delivery deadline")
    .expect("delivery");
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(150));
    println!(
        "KAFKA_RECEIPT marker={marker} offset={} timestamp={} elapsed_ms={}",
        metadata.offset(),
        metadata.timestamp(),
        elapsed.as_millis()
    );
    let _close = producer.close_timeout(Duration::from_secs(5)).await;
}
