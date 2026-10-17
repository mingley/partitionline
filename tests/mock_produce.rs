//! Produce one record against the mock broker.

mod common;

use partitionline::error::{NOT_COORDINATOR, NOT_LEADER_OR_FOLLOWER};
use partitionline::protocol::api_keys::TXN_OFFSET_COMMIT;
use partitionline::{
    Admin, AdminConfig, Error, NewTopic, ProduceRecord, Producer, ProducerConfig, TopicPartition,
};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "current_thread")]
async fn ack_completes_while_the_next_batch_lingers() {
    let mock = common::Mock::start().await;
    mock.set_produce_delay_times(Duration::from_millis(200), 1);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::from_millis(800);
    cfg.batch_records = 2;
    cfg.client_id = "linger-ack".into();
    let producer = Producer::new(cfg).await.unwrap();
    let first = ProduceRecord::to("t").value(&b"a"[..]);
    loop {
        match producer.try_send(first.clone()) {
            Ok(()) => break,
            Err(Error::QueueFull) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(error) => panic!("queue the first record: {error}"),
        }
    }
    producer
        .try_send(ProduceRecord::to("t").value(&b"b"[..]))
        .unwrap();
    producer
        .try_send(ProduceRecord::to("t").value(&b"c"[..]))
        .unwrap();
    let started = Instant::now();
    loop {
        if producer.metrics().records_acked >= 2 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "in-flight ack waited out linger: {:?}",
            started.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "in-flight ack waited out linger: {:?}",
        started.elapsed()
    );
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn producer_metadata_refreshes_waiting_topics_together() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    let producer = Producer::new(cfg).await.unwrap();
    let before = mock.metadata_calls();
    mock.set_metadata_delay(Duration::from_millis(150));
    let _ = producer.try_send(ProduceRecord::to("t").value(&b"a"[..]));
    let _ = producer.try_send(ProduceRecord::to("u").value(&b"b"[..]));
    let started = Instant::now();
    loop {
        if mock.metadata_calls() > before {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "metadata was not requested"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        mock.metadata_calls(),
        before + 1,
        "topics already waiting should share one metadata request"
    );
    let mut names = mock.last_metadata_topics().unwrap().unwrap();
    names.sort();
    assert_eq!(names, ["t".to_string(), "u".to_string()]);
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn send_all_refreshes_topics_together() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    let producer = Producer::new(cfg).await.unwrap();
    let before = mock.metadata_calls();
    mock.set_metadata_delay(Duration::from_millis(200));
    let started = Instant::now();
    let acks = producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("u").value(&b"b"[..]),
        ])
        .await
        .unwrap();
    assert_eq!(acks.len(), 2);
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "send_all waited for a metadata request per topic: {:?}",
        started.elapsed()
    );
    assert_eq!(
        mock.metadata_calls(),
        before + 1,
        "topics in one send_all should share one metadata request"
    );
    let mut names = mock.last_metadata_topics().unwrap().unwrap();
    names.sort();
    assert_eq!(names, ["t".to_string(), "u".to_string()]);
    producer.close().await.unwrap();
}

/// Records for two leaders start both handshakes before the first ack.
#[tokio::test(flavor = "current_thread")]
async fn send_all_dials_leaders_together() {
    let mock = common::Mock::start_three_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("t", 0, 2);
    mock.set_partition_leader("u", 0, 3);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    let producer = Producer::new(cfg).await.unwrap();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let acks = producer
        .send_all([
            ProduceRecord::to("t").partition(0).value(&b"a"[..]),
            ProduceRecord::to("u").partition(0).value(&b"b"[..]),
        ])
        .await
        .unwrap();
    assert_eq!(acks.len(), 2);
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "send_all waited for one leader handshake at a time: {:?}",
        started.elapsed()
    );
    let mut nodes = mock.produce_request_nodes();
    nodes.sort_unstable();
    nodes.dedup();
    assert_eq!(nodes, vec![2, 3]);
    producer.close().await.unwrap();
}

/// Stale topic metadata reloads while idle leaders redial.
#[tokio::test(flavor = "current_thread")]
async fn send_all_redials_leaders_while_refreshing_metadata() {
    let mock = common::Mock::start_three_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("t", 0, 2);
    mock.set_partition_leader("u", 0, 3);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    cfg.metadata_max_age = Duration::from_millis(30);
    cfg.connections_max_idle = Duration::from_millis(40);
    let producer = Producer::new(cfg).await.unwrap();
    let _warm = producer
        .send_all([
            ProduceRecord::to("t").partition(0).value(&b"a"[..]),
            ProduceRecord::to("u").partition(0).value(&b"b"[..]),
        ])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_api_versions_delay(delay);
    let started = Instant::now();
    let acks = producer
        .send_all([
            ProduceRecord::to("t").partition(0).value(&b"c"[..]),
            ProduceRecord::to("u").partition(0).value(&b"d"[..]),
        ])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(acks.len(), 2);
    assert!(
        elapsed < Duration::from_millis(450),
        "send_all refreshed metadata before redialing leaders: {elapsed:?}"
    );
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn send_offsets_refreshes_topics_together() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    cfg.transactional_id = Some("tx-offsets-meta".into());
    let producer = Producer::new(cfg).await.unwrap();
    producer.begin_transaction().await.unwrap();
    let before = mock.metadata_calls();
    mock.set_metadata_delay(Duration::from_millis(200));
    let started = Instant::now();
    producer
        .send_offsets_to_transaction(
            "g",
            [
                (TopicPartition::new("t", 0), 1),
                (TopicPartition::new("u", 0), 2),
            ],
        )
        .await
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "offset metadata waited once per topic: {:?}",
        started.elapsed()
    );
    assert_eq!(
        mock.metadata_calls(),
        before + 1,
        "offset topics should share one metadata request"
    );
    let mut names = mock.last_metadata_topics().unwrap().unwrap();
    names.sort();
    assert_eq!(names, ["t".to_string(), "u".to_string()]);
    producer.abort_transaction().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn send_offsets_loads_metadata_while_adding_offsets() {
    let mock = common::Mock::start().await;
    mock.set_api_max(TXN_OFFSET_COMMIT, 4);
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    cfg.transactional_id = Some("tx-offsets-overlap".into());
    let producer = Producer::new(cfg).await.unwrap();
    producer.begin_transaction().await.unwrap();
    let delay = Duration::from_millis(250);
    mock.set_add_offsets_delay(delay);
    mock.set_metadata_delay(delay);
    let before = mock.metadata_calls();
    let started = Instant::now();
    producer
        .send_offsets_to_transaction(
            "g",
            [
                (TopicPartition::new("t", 0), 1),
                (TopicPartition::new("u", 0), 2),
            ],
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        mock.metadata_calls() > before,
        "offset topics should load metadata"
    );
    assert!(
        mock.last_add_offsets_to_txn_version().is_some(),
        "classic TxnOffsetCommit must send AddOffsetsToTxn"
    );
    assert!(
        elapsed < Duration::from_millis(450),
        "AddOffsetsToTxn waited for topic metadata: {elapsed:?}"
    );
    producer.abort_transaction().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn multi_broker_produce_dials_overlap() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    let producer = Producer::new(cfg).await.unwrap();
    let _parts = producer.partitions_for("t").await.unwrap();
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let left = producer.clone();
    let right = producer.clone();
    let started = Instant::now();
    let (a, b) = tokio::join!(
        left.send(ProduceRecord::to("t").partition(0).value(&b"a"[..])),
        right.send(ProduceRecord::to("t").partition(1).value(&b"b"[..])),
    );
    let elapsed = started.elapsed();
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!((a.partition, b.partition), (0, 1));
    assert!(
        mock.api_versions_versions().len() >= before + 2,
        "both leaders should negotiate"
    );
    assert!(
        elapsed < Duration::from_millis(400),
        "produce leader dials ran one after another: {elapsed:?}"
    );
    producer.close().await.unwrap();
}

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
    assert_eq!(md.topic.as_ref(), "t");
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

#[tokio::test(flavor = "current_thread")]
async fn retry_refreshes_waiting_topics_together() {
    use partitionline::error::NOT_LEADER_OR_FOLLOWER;

    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.batch_records = 100_000;
    cfg.linger = Duration::from_millis(50);
    cfg.retry_backoff = Duration::from_millis(1);
    cfg.retry_backoff_max = Duration::from_millis(1);
    cfg.client_id = "retry-topics".into();
    let producer = Producer::new(cfg).await.unwrap();
    let _warm_t = producer
        .send(ProduceRecord::to("t").value(&b"warm-t"[..]))
        .await
        .unwrap();
    let _warm_u = producer
        .send(ProduceRecord::to("u").value(&b"warm-u"[..]))
        .await
        .unwrap();
    let base_meta = mock.metadata_calls();
    mock.set_produce_error_times(NOT_LEADER_OR_FOLLOWER, 1);
    producer
        .try_send(ProduceRecord::to("t").value(&b"again-t"[..]))
        .unwrap();
    producer
        .try_send(ProduceRecord::to("u").value(&b"again-u"[..]))
        .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(
        mock.metadata_calls() - base_meta,
        1,
        "retried topics already waiting should share one metadata request"
    );
    let mut names = mock.last_metadata_topics().unwrap().unwrap();
    names.sort();
    assert_eq!(names, ["t".to_string(), "u".to_string()]);
    producer.close().await.unwrap();
}

/// After a retriable produce error, the new leaders are dialed together.
#[tokio::test(flavor = "current_thread")]
async fn retry_dials_moved_leaders_together() {
    use partitionline::error::NOT_LEADER_OR_FOLLOWER;

    let mock = common::Mock::start_three_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("u", 0, 1);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::from_millis(40);
    cfg.batch_records = 100_000;
    cfg.retry_backoff = Duration::from_millis(1);
    cfg.retry_backoff_max = Duration::from_millis(1);
    let producer = Producer::new(cfg).await.unwrap();
    let _warm = producer
        .send_all([
            ProduceRecord::to("t").partition(0).value(&b"warm-t"[..]),
            ProduceRecord::to("u").partition(0).value(&b"warm-u"[..]),
        ])
        .await
        .unwrap();
    mock.set_partition_leader("t", 0, 2);
    mock.set_partition_leader("u", 0, 3);
    mock.set_produce_error_times(NOT_LEADER_OR_FOLLOWER, 1);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let acks = producer
        .send_all([
            ProduceRecord::to("t").partition(0).value(&b"a"[..]),
            ProduceRecord::to("u").partition(0).value(&b"b"[..]),
        ])
        .await
        .unwrap();
    assert_eq!(acks.len(), 2);
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "retry waited for one moved leader handshake at a time: {:?}",
        started.elapsed()
    );
    let mut nodes = mock.produce_request_nodes();
    nodes.sort_unstable();
    nodes.dedup();
    assert!(
        nodes.contains(&2) && nodes.contains(&3),
        "retries should reach both new leaders, got {nodes:?}"
    );
    producer.close().await.unwrap();
}

/// KL09-17: the route-cached tracker must produce metrics snapshots
/// identical to the old per-record-lookup path: exact global counters,
/// one sorted per-topic row per active topic with exact counts/bytes,
/// and per-topic ack-latency counts matching acked records. Exercises
/// both the fast path (`try_send`) and the slow path (`send_all`).
#[tokio::test]
async fn tracker_cache_mixed_topic_metrics_snapshot() {
    let mock = common::Mock::start().await;
    // The mock only knows "t"; create the three mixed-topic names first.
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let results = admin
        .create_topics(
            &[
                NewTopic::new("mta", 1, 1),
                NewTopic::new("mtb", 1, 1),
                NewTopic::new("mtc", 1, 1),
            ],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert!(
        results.iter().all(|r| r.error_code == 0),
        "create_topics: {results:?}"
    );
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    cfg.client_id = "kl09-17".into();
    let producer = Producer::new(cfg).await.unwrap();
    // Warmup spawns the node worker so the try_send burst below never
    // races worker startup (QueueFull).
    let warmup = producer
        .send(ProduceRecord::to("mta").value(&b"warmup"[..]))
        .await
        .unwrap();
    assert_eq!(warmup.offset, 0);
    // Fast path (try_send) across three topics, round-robin: 60 records.
    let topics = ["mta", "mtb", "mtc"];
    for i in 0..60u8 {
        let topic = topics[usize::from(i % 3)];
        let rec = ProduceRecord::to(topic).key(vec![i; 8]).value(vec![i; 32]);
        producer.try_send(rec).unwrap();
    }
    // Slow path (send_all) across the same topics: 9 records.
    let mut batch = Vec::new();
    for i in 0..9u8 {
        let topic = topics[usize::from(i % 3)];
        batch.push(ProduceRecord::to(topic).key(vec![i; 4]).value(vec![i; 16]));
    }
    let acks = producer.send_all(batch).await.unwrap();
    assert_eq!(acks.len(), 9);
    producer.flush().await.unwrap();
    let m = producer.metrics();
    // Global counters: 1 warmup + 60 fast + 9 slow.
    assert_eq!(m.records_queued, 70);
    assert_eq!(m.records_acked, 70);
    assert_eq!(m.produce_errors, 0);
    // Bytes are key + value only: warmup 6, fast 60 * 40, slow 9 * 20.
    assert_eq!(m.bytes_queued, 6 + 2400 + 180);
    assert_eq!(m.bytes_buffered, 0);
    assert_eq!(m.ack_latency.count, 70);
    // Per-topic rows: sorted, exactly one row per active topic.
    // mta: warmup + 20 fast + 3 slow = 24; mtb/mtc: 20 fast + 3 slow = 23.
    assert_eq!(m.topics.len(), 3);
    let names: Vec<&str> = m.topics.iter().map(|t| t.topic.as_str()).collect();
    assert_eq!(names, ["mta", "mtb", "mtc"]);
    let expected = [
        ("mta", 24u64, 6 + 800 + 60),
        ("mtb", 23u64, 860),
        ("mtc", 23u64, 860),
    ];
    let mut sum_queued = 0u64;
    for (row, (topic, n, bytes)) in m.topics.iter().zip(expected.iter()) {
        assert_eq!(row.topic, *topic);
        assert_eq!(row.records_queued, *n, "queued for {topic}");
        assert_eq!(row.records_acked, *n, "acked for {topic}");
        assert_eq!(row.produce_errors, 0, "errors for {topic}");
        assert_eq!(row.bytes_queued, *bytes, "bytes for {topic}");
        assert_eq!(row.ack_latency.count, *n, "latency count for {topic}");
        sum_queued += row.records_queued;
    }
    assert_eq!(sum_queued, m.records_queued);
    producer.close().await.unwrap();
}

/// A not-leader retry reloads metadata while the previous leader reconnects.
#[tokio::test]
async fn list_offsets_redials_while_refreshing_after_not_leader() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.retry_backoff = Duration::from_millis(1);
    cfg.retry_backoff_max = Duration::from_millis(1);
    let mut admin = Admin::new(cfg).await.unwrap();
    let _warm = admin
        .list_offsets([(TopicPartition::new("t", 0), -2i64)])
        .await
        .unwrap();
    mock.fail_list_offsets_once(NOT_LEADER_OR_FOLLOWER);
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_api_versions_delay(delay);
    let started = Instant::now();
    let listed = admin
        .list_offsets([(TopicPartition::new("t", 0), -2i64)])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 1);
    assert!(
        elapsed < Duration::from_millis(450),
        "list offsets refreshed metadata before redialing after not-leader: {elapsed:?}"
    );
}

/// Known leaders reconnect while metadata for an unknown topic loads.
#[tokio::test]
async fn list_offsets_dials_known_leaders_while_loading_metadata() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.connections_max_idle = Duration::from_millis(40);
    let mut admin = Admin::new(cfg).await.unwrap();
    let _warm = admin
        .list_offsets([(TopicPartition::new("t", 0), -2i64)])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _topics = admin.list_topics().await.unwrap();
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 2);
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_api_versions_delay(delay);
    let started = Instant::now();
    let listed = admin
        .list_offsets([
            (TopicPartition::new("t", 0), -2i64),
            (TopicPartition::new("u", 0), -2i64),
        ])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 2);
    assert!(
        elapsed < Duration::from_millis(450),
        "list offsets loaded metadata before redialing the known leader: {elapsed:?}"
    );
}

/// DeleteRecords redials a known leader while an unknown topic loads.
#[tokio::test]
async fn delete_records_dials_known_leaders_while_loading_metadata() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.connections_max_idle = Duration::from_millis(40);
    let mut admin = Admin::new(cfg).await.unwrap();
    let _warm = admin
        .delete_records_for([(TopicPartition::new("t", 0), 1i64)])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _topics = admin.list_topics().await.unwrap();
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 2);
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_api_versions_delay(delay);
    let started = Instant::now();
    let deleted = admin
        .delete_records_for([
            (TopicPartition::new("t", 0), 1i64),
            (TopicPartition::new("u", 0), 1i64),
        ])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(deleted.len(), 2);
    assert!(
        elapsed < Duration::from_millis(450),
        "delete records loaded metadata before redialing the known leader: {elapsed:?}"
    );
}

/// A known group coordinator reconnects while another group is being found.
#[tokio::test]
async fn describe_groups_dials_known_coordinators_while_finding_new_ones() {
    let mock = common::Mock::start_two_node().await;
    mock.set_group_coordinator("ga", 2);
    mock.set_group_coordinator("gb", 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.connections_max_idle = Duration::from_millis(40);
    let mut admin = Admin::new(cfg).await.unwrap();
    let _warm = admin.describe_groups(&["ga"], false).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _topics = admin.list_topics().await.unwrap();
    let delay = Duration::from_millis(250);
    mock.set_find_coordinator_delay(delay);
    mock.set_api_versions_delay(delay);
    let started = Instant::now();
    let described = admin.describe_groups(&["ga", "gb"], false).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(described.len(), 2);
    assert!(
        elapsed < Duration::from_millis(450),
        "describe groups found a coordinator before redialing the known one: {elapsed:?}"
    );
}

/// A not-coordinator retry finds the coordinator while that broker reconnects.
#[tokio::test]
async fn describe_groups_redials_while_finding_coordinator_after_not_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.set_group_coordinator("ga", 2);
    let mut admin = Admin::new(
        AdminConfig::bootstrap([mock.addr.clone()])
            .retry_backoff(Duration::from_millis(1))
            .retry_backoff_max(Duration::from_millis(1)),
    )
    .await
    .unwrap();
    let _warm = admin.describe_groups(&["ga"], false).await.unwrap();
    mock.fail_describe_groups_once(NOT_COORDINATOR);
    let delay = Duration::from_millis(250);
    mock.set_find_coordinator_delay(delay);
    mock.set_api_versions_delay(delay);
    let started = Instant::now();
    let described = admin.describe_groups(&["ga"], false).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].error_code, 0);
    assert!(
        elapsed < Duration::from_millis(450),
        "describe groups found the coordinator before redialing after not-coordinator: {elapsed:?}"
    );
}
