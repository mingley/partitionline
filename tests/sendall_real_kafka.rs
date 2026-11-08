//! Public producer history against a separately started Apache Kafka broker.
use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn explicit_partition_send_all_real_kafka_history() {
    let bootstrap = std::env::var("PL_KAFKA_BOOTSTRAP").expect("explicit bootstrap");
    let topic = std::env::var("PL_KAFKA_TOPIC").expect("explicit fresh topic");
    let idempotent = match std::env::var("PL_KAFKA_IDEMPOTENT").as_deref() {
        Ok("0") => false,
        Ok("1") => true,
        other => panic!("invalid idempotence mode: {other:?}"),
    };
    let cfg = ProducerConfig::bootstrap([bootstrap])
        .idempotent(idempotent)
        .connections(1)
        .batch_records(8192)
        .batch_bytes(2 * 1024 * 1024)
        .max_request_size(2 * 1024 * 1024)
        .buffer_memory(64 * 1024 * 1024)
        .linger(Duration::ZERO)
        .request_timeout(Duration::from_secs(60))
        .delivery_timeout(Duration::from_secs(120))
        .max_block(Duration::from_secs(60));
    let producer = Producer::new(cfg).await.expect("producer");
    let warm = producer
        .send(ProduceRecord::to(topic.as_str()).partition(0).value(&b"warm"[..]))
        .await
        .expect("warm explicit route");
    assert_eq!((warm.partition, warm.offset), (0, 0));

    let mut expected_offset = 1_i64;
    for count in [32_usize, 512, 513, 1024, 4096, 8191, 8192] {
        let mut records = Vec::with_capacity(count);
        for index in 0..count {
            records.push(
                ProduceRecord::to(topic.as_str())
                    .partition(0)
                    .value(format!("{count}:{index}").into_bytes()),
            );
        }
        let receipts = producer.send_all(records).await.expect("delivery results");
        assert_eq!(receipts.len(), count);
        for (index, receipt) in receipts.iter().enumerate() {
            assert_eq!(receipt.partition, 0);
            assert_eq!(
                receipt.offset,
                expected_offset + i64::try_from(index).expect("index fits")
            );
        }
        expected_offset += i64::try_from(count).expect("count fits");
        println!("PUBLIC_RECEIPTS mode={idempotent} count={count} next_offset={expected_offset}");
    }
    assert_eq!(expected_offset, 22561);
    producer.flush().await.expect("flush");
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.metrics().produce_errors, 0);
    producer.close().await.expect("close");
}
