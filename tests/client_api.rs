//! Public client API: builders, send_all, headers, seek helpers.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]
#![expect(
    unused_results,
    reason = "tests discard RecordMetadata when only the fetch side matters"
)]

mod common;

use partitionline::{
    partition_for_key, Acks, Admin, AdminConfig, AutoOffsetReset, Compression, Consumer,
    ConsumerConfig, ConsumerGroup, ConsumerInterceptor, Error, FetchedRecord, IsolationLevel,
    NewTopic, Partitioner, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor,
    RecordMetadata, Sasl, ShareGroup,
};
use std::time::Duration;

#[tokio::test]
async fn send_all_queues_then_returns_offsets() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let mds = producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
            ProduceRecord::to("t").value(&b"c"[..]),
        ])
        .await
        .unwrap();
    assert_eq!(mds.len(), 3);
    assert_eq!(mds[0].offset, 0);
    assert_eq!(mds[1].offset, 1);
    assert_eq!(mds[2].offset, 2);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn produce_header_survives_fetch() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(
            ProduceRecord::to("t")
                .value(&b"with-header"[..])
                .header("k", &b"v"[..])
                .timestamp(1_700_000_000_000),
        )
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"with-header"[..]));
    assert_eq!(recs[0].timestamp, 1_700_000_000_000);
    assert_eq!(recs[0].headers.len(), 1);
    assert_eq!(recs[0].headers[0].key, "k");
    assert_eq!(recs[0].headers[0].value.as_deref(), Some(&b"v"[..]));
}

#[tokio::test]
async fn seek_to_end_skips_existing_then_reads_new() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"old"[..]))
        .await
        .unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    consumer.seek_to_end().await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert!(recs.is_empty(), "log end should have nothing new");

    producer
        .send(ProduceRecord::to("t").value(&b"new"[..]))
        .await
        .unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"new"[..]));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn seek_to_beginning_rereads_from_start() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"first"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    consumer.seek_to_beginning().await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"first"[..]));
}

#[test]
fn config_builders_set_typed_knobs() {
    let p = ProducerConfig::bootstrap(["127.0.0.1:9092"])
        .acks(Acks::All)
        .compression(Compression::Lz4)
        .idempotent(true)
        .sasl(Sasl::scram_sha256("alice", "secret"));
    assert_eq!(p.acks, -1);
    assert_eq!(p.compression, Compression::Lz4);
    assert!(p.enable_idempotence);
    assert_eq!(p.sasl_scram, Some(("alice".into(), "secret".into())));
    assert!(p.sasl_plain.is_none());

    let c = ConsumerConfig::bootstrap(["127.0.0.1:9092"])
        .isolation(IsolationLevel::ReadCommitted)
        .max_bytes(1024)
        .rack("az1")
        .group_instance_id("worker-1")
        .auto_offset_reset(AutoOffsetReset::Latest)
        .max_poll_records(50)
        .session_timeout(Duration::from_secs(20))
        .heartbeat_interval(Duration::from_millis(200))
        .auto_commit(true)
        .auto_commit_interval(Duration::ZERO)
        .max_poll_interval(Duration::from_secs(60));
    assert_eq!(c.isolation_level, 1);
    assert_eq!(c.max_bytes, 1024);
    assert_eq!(c.rack.as_deref(), Some("az1"));
    assert_eq!(c.group_instance_id.as_deref(), Some("worker-1"));
    assert_eq!(c.auto_offset_reset, AutoOffsetReset::Latest);
    assert_eq!(c.max_poll_records, Some(50));
    assert_eq!(c.session_timeout_ms, 20_000);
    assert_eq!(c.heartbeat_interval, Duration::from_millis(200));
    assert!(c.enable_auto_commit);
    assert_eq!(c.auto_commit_interval, Duration::ZERO);
    assert_eq!(c.max_poll_interval, Duration::from_secs(60));
}

#[test]
fn broker_error_display_and_code() {
    let e = Error::broker(6, "t-0");
    assert_eq!(e.broker_code(), Some(6));
    let s = e.to_string();
    assert!(s.contains("NOT_LEADER_OR_FOLLOWER"), "{s}");
    assert!(s.contains("t-0"), "{s}");
    let empty = Error::broker(3, "");
    assert_eq!(
        empty.to_string(),
        "broker error 3 (UNKNOWN_TOPIC_OR_PARTITION)"
    );
}

async fn create_two_topics(addr: &str, a: &str, b: &str) -> partitionline::Result<()> {
    let mut admin = Admin::new(AdminConfig::bootstrap([addr])).await?;
    let results = admin
        .create_topics(
            &[NewTopic::new(a, 1, 1), NewTopic::new(b, 1, 1)],
            10_000,
            false,
        )
        .await?;
    assert!(
        results.iter().all(|r| r.error_code == 0),
        "create_topics: {results:?}"
    );
    Ok(())
}

#[tokio::test]
async fn classic_group_join_topics_fetches_both() {
    let mock = common::Mock::start().await;
    create_two_topics(&mock.addr, "orders", "payments")
        .await
        .unwrap();
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("orders").value(&b"o"[..]),
            ProduceRecord::to("payments").value(&b"p"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join_topics(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "multi",
        ["orders", "payments"],
    )
    .await
    .unwrap();
    assert_eq!(
        group.topics(),
        &["orders".to_string(), "payments".to_string()]
    );
    let recs = group.poll().await.unwrap();
    let mut names: Vec<&str> = recs.iter().map(|r| r.topic.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["orders", "payments"]);
}

#[tokio::test]
async fn kip848_join_consumer_topics_fetches_both() {
    let mock = common::Mock::start().await;
    create_two_topics(&mock.addr, "alpha", "beta")
        .await
        .unwrap();
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("alpha").value(&b"a"[..]),
            ProduceRecord::to("beta").value(&b"b"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join_consumer_topics(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "kmulti",
        ["alpha", "beta"],
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    let mut names: Vec<&str> = recs.iter().map(|r| r.topic.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["alpha", "beta"]);
}

#[tokio::test]
async fn classic_group_mixed_subscriptions_stay_on_own_topics() {
    let mock = common::Mock::start().await;
    create_two_topics(&mock.addr, "orders", "payments")
        .await
        .unwrap();
    let cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10);
    let mut orders = ConsumerGroup::join(cfg.clone(), "mix", "orders")
        .await
        .unwrap();
    let payments_join = tokio::spawn({
        let cfg = cfg.clone();
        async move { ConsumerGroup::join(cfg, "mix", "payments").await }
    });
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(orders.poll().await);
    let payments = payments_join.await.unwrap().unwrap();
    let order_topics: Vec<String> = orders.assignment().into_iter().map(|(t, _)| t).collect();
    let pay_topics: Vec<String> = payments.assignment().into_iter().map(|(t, _)| t).collect();
    assert_eq!(order_topics, vec!["orders".to_string()]);
    assert_eq!(pay_topics, vec!["payments".to_string()]);
}

#[tokio::test]
async fn fetch_two_leaders_in_one_round() {
    let mock = common::Mock::start_two_node().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("split", 2, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    mock.set_partition_leader("split", 0, 1);
    mock.set_partition_leader("split", 1, 2);

    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("split").partition(0).value(&b"n1"[..]),
            ProduceRecord::to("split").partition(1).value(&b"n2"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("split", 0, 0).await.unwrap();
    consumer.assign("split", 1, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    let mut values: Vec<&[u8]> = recs.iter().filter_map(|r| r.value.as_deref()).collect();
    values.sort_unstable();
    assert_eq!(values, vec![&b"n1"[..], &b"n2"[..]]);
    let mut nodes = mock.fetch_nodes();
    nodes.sort_unstable();
    nodes.dedup();
    assert!(
        nodes.contains(&1) && nodes.contains(&2),
        "fetch must hit both leaders, got {nodes:?}"
    );
}

#[tokio::test]
async fn classic_join_sends_group_instance_id() {
    let mock = common::Mock::start().await;
    let group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .group_instance_id("worker-1"),
        "static",
        "t",
    )
    .await
    .unwrap();
    assert_eq!(mock.last_group_instance_id().as_deref(), Some("worker-1"));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn kip848_join_sends_instance_id_and_rack() {
    let mock = common::Mock::start().await;
    let group = ConsumerGroup::join_consumer(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .group_instance_id("worker-2")
            .rack("az1"),
        "kstatic",
        "t",
    )
    .await
    .unwrap();
    assert_eq!(mock.last_group_instance_id().as_deref(), Some("worker-2"));
    assert_eq!(mock.last_group_rack().as_deref(), Some("az1"));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_join_topics_fetches_both() {
    let mock = common::Mock::start().await;
    create_two_topics(&mock.addr, "orders", "payments")
        .await
        .unwrap();
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("orders").value(&b"o"[..]),
            ProduceRecord::to("payments").value(&b"p"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ShareGroup::join_topics(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "share-multi",
        ["orders", "payments"],
    )
    .await
    .unwrap();
    assert_eq!(
        group.topics(),
        &["orders".to_string(), "payments".to_string()]
    );
    let recs = group.poll().await.unwrap();
    let mut names: Vec<&str> = recs.iter().map(|r| r.topic.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["orders", "payments"]);
    let sm = group.metrics();
    assert_eq!(sm.fetch_rounds, 1);
    assert_eq!(sm.records_fetched, 2);
    assert_eq!(sm.records_acknowledged, 0);
    group.accept(&recs).await.unwrap();
    assert_eq!(group.metrics().records_acknowledged, 2);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn pause_skips_one_partition_until_resume() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("parts", 2, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("parts").partition(0).value(&b"p0"[..]),
            ProduceRecord::to("parts").partition(1).value(&b"p1"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("parts", 0, 0).await.unwrap();
    consumer.assign("parts", 1, 0).await.unwrap();
    consumer.pause(&[("parts".into(), 1)]);
    assert_eq!(consumer.paused(), vec![("parts".into(), 1)]);
    assert_eq!(consumer.position("parts", 0).unwrap(), 0);

    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].partition, 0);
    assert_eq!(recs[0].value.as_deref(), Some(&b"p0"[..]));
    assert_eq!(consumer.position("parts", 0).unwrap(), 1);
    assert_eq!(consumer.position("parts", 1).unwrap(), 0);

    consumer.resume(&[("parts".into(), 1)]);
    assert!(consumer.paused().is_empty());
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].partition, 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"p1"[..]));
}

#[tokio::test]
async fn max_poll_records_returns_rest_on_next_fetch() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
            ProduceRecord::to("t").value(&b"c"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .max_poll_records(1),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let a = consumer.fetch().await.unwrap();
    let b = consumer.fetch().await.unwrap();
    let c = consumer.fetch().await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_eq!(c.len(), 1);
    assert_eq!(a[0].value.as_deref(), Some(&b"a"[..]));
    assert_eq!(b[0].value.as_deref(), Some(&b"b"[..]));
    assert_eq!(c[0].value.as_deref(), Some(&b"c"[..]));
}

#[tokio::test]
async fn partitions_for_and_end_offsets() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"one"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    let info = consumer.partitions_for("t").await.unwrap();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].partition, 0);
    assert_eq!(info[0].leader, 1);
    let end = consumer.end_offsets(&[("t".into(), 0)]).await.unwrap();
    assert_eq!(end, vec![("t".into(), 0, 1)]);
    let begin = consumer
        .beginning_offsets(&[("t".into(), 0)])
        .await
        .unwrap();
    assert_eq!(begin, vec![("t".into(), 0, 0)]);
}

#[tokio::test]
async fn custom_partitioner_pins_keyed_records() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("keyed", 2, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );

    struct AlwaysZero;
    impl Partitioner for AlwaysZero {
        fn partition(&self, _topic: &str, _key: Option<&[u8]>, _n: i32) -> i32 {
            0
        }
    }

    let mut keys_on_one = Vec::new();
    for i in 0..32u8 {
        let k = [i];
        if partition_for_key(&k, 2) == 1 {
            keys_on_one.push(k);
        }
    }
    assert!(
        !keys_on_one.is_empty(),
        "need murmur2 keys that would land on partition 1"
    );

    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .partitioner(AlwaysZero),
    )
    .await
    .unwrap();
    let recs: Vec<ProduceRecord> = keys_on_one
        .iter()
        .map(|k| ProduceRecord::to("keyed").key(k.to_vec()).value(&b"v"[..]))
        .collect();
    let mds = producer.send_all(recs).await.unwrap();
    assert!(mds.iter().all(|m| m.partition == 0));
    producer
        .send(
            ProduceRecord::to("keyed")
                .partition(1)
                .value(&b"explicit"[..]),
        )
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("keyed", 1, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"explicit"[..]));
}

#[tokio::test]
async fn auto_offset_reset_latest_skips_existing() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"old"[..]))
        .await
        .unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_offset_reset(AutoOffsetReset::Latest),
        "reset-latest",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert!(recs.is_empty(), "latest must skip the existing record");
    producer
        .send(ProduceRecord::to("t").value(&b"new"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"new"[..]));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn group_committed_after_commit() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"x"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "cmt",
        "t",
    )
    .await
    .unwrap();
    let before = group.committed().await.unwrap();
    assert_eq!(before, vec![("t".into(), 0, -1)]);
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    group.commit().await.unwrap();
    let after = group.committed().await.unwrap();
    assert_eq!(after, vec![("t".into(), 0, 1)]);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn auto_commit_on_poll_then_rejoin() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("t").value(&b"old"[..]),
            ProduceRecord::to("t").value(&b"new"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_commit(true)
            .auto_commit_interval(Duration::ZERO),
        "auto",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 2);
    group.leave().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "auto",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert!(
        recs.is_empty(),
        "auto-commit must have stored the high watermark"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn rebalance_listener_sees_first_assignment() {
    let mock = common::Mock::start().await;
    let added = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cfg = ConsumerConfig::bootstrap([mock.addr.clone()])
        .max_wait_ms(10)
        .on_rebalance({
            let added = std::sync::Arc::clone(&added);
            move |_revoked, assigned| {
                added.store(assigned.len(), std::sync::atomic::Ordering::SeqCst);
            }
        });
    let group = ConsumerGroup::join(cfg, "rl", "t").await.unwrap();
    assert_eq!(added.load(std::sync::atomic::Ordering::SeqCst), 1);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn commit_offsets_skips_without_poll() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("t").value(&b"old"[..]),
            ProduceRecord::to("t").value(&b"new"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "skip",
        "t",
    )
    .await
    .unwrap();
    group.commit_offsets(&[("t".into(), 0, 1)]).await.unwrap();
    group.leave().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "skip",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"new"[..]));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn producer_and_consumer_metrics() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("t").value(&b"ab"[..]),
            ProduceRecord::to("t").value(&b"cd"[..]),
        ])
        .await
        .unwrap();
    let pm = producer.metrics();
    assert_eq!(pm.records_queued, 2);
    assert_eq!(pm.records_acked, 2);
    assert_eq!(pm.produce_errors, 0);
    assert_eq!(pm.bytes_queued, 4);
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 2);
    let cm = consumer.metrics();
    assert_eq!(cm.fetch_rounds, 1);
    assert_eq!(cm.records_fetched, 2);
    assert_eq!(cm.bytes_fetched, 4);
    assert_eq!(cm.fetch_errors, 0);
}

#[tokio::test]
async fn max_poll_interval_errors_on_second_poll() {
    let mock = common::Mock::start().await;
    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .max_poll_interval(Duration::from_millis(1)),
        "mpi",
        "t",
    )
    .await
    .unwrap();
    group.poll().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let err = group.poll().await.unwrap_err();
    assert!(
        matches!(err, Error::MaxPollInterval),
        "expected MaxPollInterval, got {err}"
    );
}

#[tokio::test]
async fn two_members_cooperative_sticky_partition_all() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("cs4", 4, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10);
    let mut a = ConsumerGroup::join_cooperative_sticky(ccfg.clone(), "csg", "cs4")
        .await
        .unwrap();
    let b_join = tokio::spawn({
        let ccfg = ccfg.clone();
        async move { ConsumerGroup::join_cooperative_sticky(ccfg, "csg", "cs4").await }
    });
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(a.poll().await);
    let mut b = b_join.await.unwrap().unwrap();
    let mut split = false;
    for _ in 0..12 {
        drop(a.poll().await);
        drop(b.poll().await);
        let a_n = a.assignment().len();
        let b_n = b.assignment().len();
        if a_n == 2 && b_n == 2 {
            split = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let a_parts: std::collections::HashSet<i32> = a.assignment().iter().map(|(_, p)| *p).collect();
    let b_parts: std::collections::HashSet<i32> = b.assignment().iter().map(|(_, p)| *p).collect();
    assert!(
        split && a_parts.is_disjoint(&b_parts) && a_parts.len() + b_parts.len() == 4,
        "cooperative-sticky should settle on a 2/2 split, got a={a_parts:?} b={b_parts:?}"
    );
    a.leave().await.unwrap();
    b.leave().await.unwrap();
}

#[tokio::test]
async fn max_poll_interval_heartbeat_leaves_group() {
    let mock = common::Mock::start().await;
    let mut a = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .heartbeat_interval(Duration::from_millis(20))
            .max_poll_interval(Duration::from_millis(30)),
        "mpi-leave",
        "t",
    )
    .await
    .unwrap();
    a.poll().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let b = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "mpi-leave",
        "t",
    )
    .await
    .unwrap();
    assert_eq!(
        b.assignment().len(),
        1,
        "first member must have left after max.poll.interval"
    );
    let err = a.poll().await.unwrap_err();
    assert!(
        matches!(err, Error::MaxPollInterval),
        "expected MaxPollInterval, got {err}"
    );
    b.leave().await.unwrap();
}

struct TagValue;

impl ProducerInterceptor for TagValue {
    fn on_send(&self, rec: ProduceRecord) -> ProduceRecord {
        rec.value(&b"tagged"[..])
    }
}

struct CountAck(std::sync::Arc<std::sync::atomic::AtomicU64>);

impl ProducerInterceptor for CountAck {
    fn on_ack(&self, _md: &RecordMetadata) {
        let _ = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

struct CountFetch(std::sync::Arc<std::sync::atomic::AtomicU64>);

impl ConsumerInterceptor for CountFetch {
    fn on_consume(&self, recs: Vec<FetchedRecord>) -> Vec<FetchedRecord> {
        let n = u64::try_from(recs.len()).unwrap_or(u64::MAX);
        let _ = self.0.fetch_add(n, std::sync::atomic::Ordering::SeqCst);
        recs
    }
}

#[tokio::test]
async fn wakeup_errors_then_fetch_succeeds() {
    let mock = common::Mock::start().await;
    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    consumer.wakeup();
    let err = consumer.fetch().await.unwrap_err();
    assert!(matches!(err, Error::Wakeup), "expected Wakeup, got {err}");
    let recs = consumer.fetch().await.unwrap();
    assert!(recs.is_empty());
}

#[tokio::test]
async fn interceptors_rewrite_produce_and_count_fetch() {
    let mock = common::Mock::start().await;
    let acks = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let fetched = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .interceptor(TagValue)
            .interceptor(CountAck(std::sync::Arc::clone(&acks))),
    )
    .await
    .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"orig"[..]))
        .await
        .unwrap();
    assert_eq!(acks.load(std::sync::atomic::Ordering::SeqCst), 1);
    producer.close().await.unwrap();

    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .interceptor(CountFetch(std::sync::Arc::clone(&fetched))),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"tagged"[..]));
    assert_eq!(fetched.load(std::sync::atomic::Ordering::SeqCst), 1);
}
