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
    Acks, Admin, AdminConfig, Compression, Consumer, ConsumerConfig, ConsumerGroup, Error,
    IsolationLevel, NewTopic, ProduceRecord, Producer, ProducerConfig, Sasl,
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
        .rack("az1");
    assert_eq!(c.isolation_level, 1);
    assert_eq!(c.max_bytes, 1024);
    assert_eq!(c.rack.as_deref(), Some("az1"));
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
