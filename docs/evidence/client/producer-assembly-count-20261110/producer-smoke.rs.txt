//! A bounded, acknowledged multi-topic producer smoke.
use std::time::Duration;
use partitionline::{Acks, ProduceRecord, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = std::env::var("ASSEMBLY_SMOKE_BOOTSTRAP")?;
    let prefix = std::env::var("ASSEMBLY_SMOKE_PREFIX")?;
    let config = ProducerConfig::bootstrap([bootstrap])
        .acks(Acks::All)
        .linger(Duration::from_millis(50))
        .batch_records(512)
        .request_timeout(Duration::from_secs(15))
        .delivery_timeout(Duration::from_secs(45));
    let producer = Producer::new(config).await?;
    let mut records = Vec::with_capacity(256);
    for index in 0..256 {
        let topic = format!("{prefix}-{}", index % 4);
        let value = format!("{prefix}:{index}");
        records.push(ProduceRecord::to(topic).partition(0).value(value));
    }
    let delivered = producer.send_all(records).await?;
    assert_eq!(delivered.len(), 256);
    for (index, metadata) in delivered.iter().enumerate() {
        assert_eq!(metadata.topic.as_ref(), format!("{prefix}-{}", index % 4));
        assert_eq!(metadata.partition, 0);
        assert_eq!(metadata.offset, i64::try_from(index / 4)?);
    }
    producer.close().await?;
    println!("ASSEMBLY_SMOKE acknowledged=256 topics=4");
    Ok(())
}
