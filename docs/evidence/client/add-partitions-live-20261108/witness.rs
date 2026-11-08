//! Narrow live Kafka transaction witness for the AddPartitions candidate.
use std::time::Duration;
use partitionline::{ProduceRecord, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = Producer::new(
        ProducerConfig::bootstrap(["127.0.0.1:19092"])
            .transactional_id("pl-add-coverage-391")
            .linger(Duration::from_millis(500)),
    ).await?;
    producer.init_transactions().await?;
    producer.begin_transaction().await?;
    let records = vec![
        ProduceRecord::to("pl-add-coverage").partition(0).value(&b"pl-add-coverage-zero"[..]),
        ProduceRecord::to("pl-add-coverage").partition(1).value(&b"pl-add-coverage-one"[..]),
    ];
    let results = producer.send_all(records).await?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].partition, 0);
    assert_eq!(results[1].partition, 1);
    println!("acknowledged partitions={},{} offsets={},{}",
             results[0].partition, results[1].partition, results[0].offset, results[1].offset);
    producer.commit_transaction().await?;
    println!("commit returned success");
    producer.close().await?;
    Ok(())
}
