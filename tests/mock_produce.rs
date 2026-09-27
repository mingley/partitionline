//! Produce one record against the mock broker.

mod common;

use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn produce_one_record_against_mock() {
    let mock = common::Mock::start().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    cfg.client_id = "test".into();
    let producer = Producer::new(cfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"hello"[..]))
        .await
        .unwrap();
    assert_eq!(md.topic, "t");
    assert_eq!(md.partition, 0);
    assert_eq!(md.offset, 0);
    let md2 = producer
        .send(ProduceRecord::to("t").key(&b"k"[..]).value(&b"v"[..]))
        .await
        .unwrap();
    assert_eq!(md2.offset, 1);
    producer.close().await.unwrap();
}

/// KL09-14: header-heavy batches must respect the byte bound.
///
/// Each record carries a 16B value plus 10 headers x (5B key + 64B value),
/// ~800B on the wire, while the header-blind `estimate()` sees 16 + 64 =
/// 80B. With `batch_bytes = 2048` the old packing takes all 25 records
/// (~20KB encoded) into one batch; exact packing must split so every
/// observed batch fits the bound. The two callers put the 2048 bound in
/// `batch_bytes` vs `max_request_size` (`produce_batch_bytes` is the min).
async fn header_heavy_bound_case(
    batch_bytes: usize,
    max_request_size: usize,
    bound: i32,
) -> Result<(), String> {
    let mock = common::Mock::start().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.batch_bytes = batch_bytes;
    cfg.max_request_size = max_request_size;
    cfg.batch_records = 100_000;
    cfg.linger = Duration::from_millis(100);
    cfg.client_id = "kl09-14".into();
    let producer = Producer::new(cfg).await.map_err(|e| e.to_string())?;
    // One awaited send spawns the node worker; the try_send burst below
    // finds it immediately instead of racing worker startup (QueueFull).
    let warmup = producer
        .send(ProduceRecord::to("t").value(&b"warmup"[..]))
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(warmup.offset, 0);
    for i in 0..25u8 {
        let mut rec = ProduceRecord::to("t").value(vec![i; 16]);
        for h in 0..10 {
            rec = rec.header(format!("hk-{h}"), vec![i; 64]);
        }
        producer.try_send(rec).map_err(|e| e.to_string())?;
    }
    producer.close().await.map_err(|e| e.to_string())?;
    let batches = mock.produce_batches();
    assert!(!batches.is_empty(), "no produce batches observed");
    let total: i32 = batches.iter().map(|b| b.2).sum();
    assert_eq!(total, 26, "warmup + burst must be delivered");
    for (topic, partition, nrec, bytes) in &batches {
        assert_eq!(topic, "t");
        assert_eq!(*partition, 0);
        assert!(
            *bytes <= bound,
            "batch of {nrec} records is {bytes}B > bound {bound}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn header_heavy_batches_respect_batch_bytes() {
    header_heavy_bound_case(2048, 1_000_000, 2048)
        .await
        .unwrap();
}

#[tokio::test]
async fn header_heavy_batches_respect_max_request_size() {
    header_heavy_bound_case(1_000_000, 2048, 2048)
        .await
        .unwrap();
}

/// KL09-14: keyed records with empty values and headers respect the bound.
#[tokio::test]
async fn keyed_empty_value_batches_respect_bound() {
    let mock = common::Mock::start().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.batch_bytes = 1024;
    cfg.batch_records = 100_000;
    cfg.linger = Duration::from_millis(100);
    cfg.client_id = "kl09-14".into();
    let producer = Producer::new(cfg).await.unwrap();
    // One awaited send spawns the node worker; the try_send burst below
    // finds it immediately instead of racing worker startup (QueueFull).
    let warmup = producer
        .send(ProduceRecord::to("t").value(&b"warmup"[..]))
        .await
        .unwrap();
    assert_eq!(warmup.offset, 0);
    for i in 0..20u8 {
        let rec = ProduceRecord::to("t")
            .key(vec![i; 64])
            .value(Vec::new())
            .header("hk".to_string(), vec![i; 128])
            .null_header("hn".to_string());
        producer.try_send(rec).unwrap();
    }
    producer.close().await.unwrap();
    let batches = mock.produce_batches();
    let total: i32 = batches.iter().map(|b| b.2).sum();
    assert_eq!(total, 21, "warmup + burst must be delivered");
    for (_, _, nrec, bytes) in &batches {
        assert!(*bytes <= 1024, "batch of {nrec} records is {bytes}B > 1024");
    }
}

/// KL09-14: a single record over `batch_bytes` (but under
/// `max_request_size`) still sends alone — batches cannot split a record
/// (Java `RecordAccumulator` parity; only multi-record packing is bounded).
#[tokio::test]
async fn oversized_single_record_still_sends() {
    let mock = common::Mock::start().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.batch_bytes = 512;
    cfg.batch_records = 100_000;
    cfg.linger = Duration::from_millis(100);
    cfg.client_id = "kl09-14".into();
    let producer = Producer::new(cfg).await.unwrap();
    // One awaited send spawns the node worker (see above).
    let warmup = producer
        .send(ProduceRecord::to("t").value(&b"warmup"[..]))
        .await
        .unwrap();
    assert_eq!(warmup.offset, 0);
    let rec = ProduceRecord::to("t")
        .value(vec![7u8; 16])
        .header("big".to_string(), vec![7u8; 700]);
    producer.try_send(rec).unwrap();
    producer.close().await.unwrap();
    let batches = mock.produce_batches();
    assert_eq!(batches.len(), 2, "warmup + singleton expected");
    let big: Vec<_> = batches.iter().filter(|b| b.2 == 1 && b.3 > 512).collect();
    assert_eq!(
        big.len(),
        1,
        "singleton documents the unsplittable exception: {batches:?}"
    );
}

/// KL09-28: a retried batch refreshes metadata once, not once per record.
///
/// One 10-record batch fails with retriable NOT_LEADER and retries to
/// success. Metadata RPCs after the warmup baseline must be exactly 1
/// (the batch's shared refresh), and all 10 records must be delivered
/// exactly once (offsets 1..=10 after the warmup).
#[tokio::test]
async fn retry_refreshes_metadata_once_per_batch() {
    use partitionline::error::NOT_LEADER_OR_FOLLOWER;

    let mock = common::Mock::start().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.batch_records = 100_000;
    cfg.linger = Duration::from_millis(100);
    cfg.retry_backoff = Duration::from_millis(1);
    cfg.retry_backoff_max = Duration::from_millis(1);
    cfg.client_id = "kl09-28".into();
    let producer = Producer::new(cfg).await.unwrap();
    // One awaited send warms metadata and the node worker.
    let warmup = producer
        .send(ProduceRecord::to("t").value(&b"warmup"[..]))
        .await
        .unwrap();
    assert_eq!(warmup.offset, 0);
    let base_meta = mock.metadata_calls();

    // The next produce fails once; the 10-record batch must retry.
    mock.set_produce_error_times(NOT_LEADER_OR_FOLLOWER, 1);
    for i in 0..10u8 {
        producer
            .try_send(ProduceRecord::to("t").value(vec![i; 16]))
            .unwrap();
    }
    producer.flush().await.unwrap();

    let batches = mock.produce_batches();
    let total: i32 = batches.iter().map(|b| b.2).sum();
    // Warmup + failed burst + retried burst observed on the wire.
    assert_eq!(total, 21, "failed batch must be retried, not dropped");
    // ...but appended exactly once (no duplicates from the retry).
    assert_eq!(mock.log_len("t", 0), 11);
    assert_eq!(
        mock.metadata_calls() - base_meta,
        1,
        "one metadata refresh per retried batch, not per record"
    );
    producer.close().await.unwrap();
}
