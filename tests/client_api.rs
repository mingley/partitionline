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
    partition_for_key, AbortTransactionSpec, Acks, Admin, AdminConfig, AutoOffsetReset,
    Compression, Consumer, ConsumerConfig, ConsumerGroup, ConsumerInterceptor,
    DescribeLogDirsRequest, DescribeShareGroupOffsetsGroup, Error, FetchedRecord, IsolationLevel,
    ListConsumerGroupOffsetsSpec, MemberToRemove, NewTopic, OffsetAndMetadata, OffsetAndTimestamp,
    Partitioner, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
    ReplicaLogDirInfo, Sasl, ShareGroup, TopicPartition, TopicPartitionReplica,
    CONFIG_RESOURCE_CLIENT_METRICS, DEFAULT_ENFORCE_REBALANCE_REASON, DEFAULT_LEAVE_GROUP_REASON,
    EARLIEST_LOCAL_TIMESTAMP, EARLIEST_TIMESTAMP, LATEST_TIERED_TIMESTAMP, LATEST_TIMESTAMP,
    LEAVE_GROUP_REASON_CLOSED, LEAVE_GROUP_REASON_POLL_TIMEOUT, LEAVE_GROUP_REASON_UNSUBSCRIBED,
    MAX_TIMESTAMP,
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
    assert_eq!(
        mock.last_produce_version(),
        Some(12),
        "Producer must prefer Produce v12 when the broker advertises it"
    );
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
                .null_header("empty")
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
    assert_eq!(
        mock.last_fetch_version(),
        Some(17),
        "Consumer must prefer Fetch v17 when the broker advertises it"
    );
    assert_eq!(recs.len(), 1);
    assert_eq!(recs.count(), 1);
    assert_eq!(recs.partitions(), vec![TopicPartition::new("t", 0)]);
    assert_eq!(recs.records(TopicPartition::new("t", 0)).count(), 1);
    assert_eq!(recs.records_for_topic("t").count(), 1);
    assert_eq!(
        recs.next_offsets(),
        vec![(
            TopicPartition::new("t", 0),
            OffsetAndMetadata {
                offset: recs[0].offset + 1,
                leader_epoch: recs[0].leader_epoch,
                metadata: String::new(),
            }
        )]
    );
    assert_eq!(recs[0].value.as_deref(), Some(&b"with-header"[..]));
    assert_eq!(recs[0].timestamp, 1_700_000_000_000);
    assert_eq!(recs[0].leader_epoch, Some(0));
    assert_eq!(recs[0].serialized_key_size(), -1);
    assert_eq!(recs[0].serialized_value_size(), 11);
    assert_eq!(recs[0].headers.len(), 2);
    assert_eq!(recs[0].headers[0].key, "k");
    assert_eq!(recs[0].headers[0].value.as_deref(), Some(&b"v"[..]));
    assert_eq!(recs[0].headers[1].key, "empty");
    assert!(recs[0].headers[1].value.is_none());
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
    assert_eq!(
        consumer
            .list_offset(("t", 0), EARLIEST_TIMESTAMP)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        consumer
            .list_offsets("t", 0, LATEST_TIMESTAMP)
            .await
            .unwrap(),
        1
    );
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    consumer.seek_to_beginning().await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"first"[..]));
    consumer
        .seek_to_end_of([TopicPartition::new("t", 0)])
        .await
        .unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert!(recs.is_empty(), "seek_to_end_of should skip existing");
    consumer.seek_to_beginning_of([("t", 0)]).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"first"[..]));
}

#[test]
fn config_builders_set_typed_knobs() {
    assert_eq!(ProducerConfig::default().buffer_memory, 32 * 1024 * 1024);
    assert_eq!(ProducerConfig::default().max_request_size, 1024 * 1024);
    assert!(!ConsumerConfig::default().allow_auto_topic_creation);
    assert_eq!(
        ProducerConfig::default().connections_max_idle,
        Duration::from_millis(9 * 60 * 1000)
    );
    let p = ProducerConfig::bootstrap(["127.0.0.1:9092"])
        .acks(Acks::All)
        .compression(Compression::Lz4)
        .idempotent(true)
        .allow_auto_create_topics(true)
        .connect_timeout(Duration::from_secs(3))
        .delivery_timeout(Duration::from_secs(45))
        .max_block(Duration::from_secs(15))
        .buffer_memory(4096)
        .max_request_size(2048)
        .retry_backoff(Duration::from_millis(80))
        .retry_backoff_max(Duration::from_millis(400))
        .transaction_timeout(Duration::from_secs(45))
        .metadata_max_age(Duration::from_secs(12))
        .reconnect_backoff(Duration::from_millis(40))
        .reconnect_backoff_max(Duration::from_millis(200))
        .connections_max_idle(Duration::from_secs(60))
        .sasl(Sasl::scram_sha256("alice", "secret"));
    assert_eq!(p.acks, -1);
    assert_eq!(p.compression, Compression::Lz4);
    assert!(p.enable_idempotence);
    assert!(p.allow_auto_topic_creation);
    assert_eq!(p.connect_timeout, Duration::from_secs(3));
    assert_eq!(p.delivery_timeout, Duration::from_secs(45));
    assert_eq!(p.max_block, Duration::from_secs(15));
    assert_eq!(p.buffer_memory, 4096);
    assert_eq!(p.max_request_size, 2048);
    assert_eq!(p.retry_backoff, Duration::from_millis(80));
    assert_eq!(p.retry_backoff_max, Duration::from_millis(400));
    assert_eq!(p.transaction_timeout, Duration::from_secs(45));
    assert_eq!(p.metadata_max_age, Duration::from_secs(12));
    assert_eq!(p.reconnect_backoff, Duration::from_millis(40));
    assert_eq!(p.reconnect_backoff_max, Duration::from_millis(200));
    assert_eq!(p.connections_max_idle, Duration::from_secs(60));
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
        .max_poll_interval(Duration::from_secs(60))
        .retry_backoff(Duration::from_millis(25))
        .retry_backoff_max(Duration::from_millis(250))
        .metadata_max_age(Duration::from_secs(9))
        .reconnect_backoff(Duration::from_millis(15))
        .reconnect_backoff_max(Duration::from_millis(120))
        .connections_max_idle(Duration::from_secs(90))
        .allow_auto_create_topics(true)
        .connect_timeout(Duration::from_secs(4));
    assert_eq!(c.isolation_level, IsolationLevel::ReadCommitted);
    assert_eq!(c.max_bytes, 1024);
    assert_eq!(c.max_partition_fetch_bytes, 1024);
    let split = ConsumerConfig::bootstrap(["127.0.0.1:9092"])
        .fetch_max_bytes(4096)
        .max_partition_fetch_bytes(512);
    assert_eq!(split.max_bytes, 4096);
    assert_eq!(split.max_partition_fetch_bytes, 512);
    assert_eq!(c.rack.as_deref(), Some("az1"));
    assert_eq!(c.group_instance_id.as_deref(), Some("worker-1"));
    assert_eq!(c.auto_offset_reset, AutoOffsetReset::Latest);
    assert_eq!(c.max_poll_records, Some(50));
    assert_eq!(c.session_timeout_ms, 20_000);
    assert_eq!(c.heartbeat_interval, Duration::from_millis(200));
    assert!(c.enable_auto_commit);
    assert_eq!(c.auto_commit_interval, Duration::ZERO);
    assert_eq!(c.max_poll_interval, Duration::from_secs(60));
    assert_eq!(c.retry_backoff, Duration::from_millis(25));
    assert_eq!(c.retry_backoff_max, Duration::from_millis(250));
    assert_eq!(c.metadata_max_age, Duration::from_secs(9));
    assert_eq!(c.reconnect_backoff, Duration::from_millis(15));
    assert_eq!(c.reconnect_backoff_max, Duration::from_millis(120));
    assert_eq!(c.connections_max_idle, Duration::from_secs(90));
    assert!(c.allow_auto_topic_creation);
    assert_eq!(c.connect_timeout, Duration::from_secs(4));

    let a = AdminConfig::bootstrap(["127.0.0.1:9092"])
        .reconnect_backoff(Duration::from_millis(30))
        .reconnect_backoff_max(Duration::from_millis(300))
        .connections_max_idle(Duration::from_secs(120))
        .retry_backoff(Duration::from_millis(45))
        .retry_backoff_max(Duration::from_millis(180))
        .connect_timeout(Duration::from_secs(2));
    assert_eq!(a.reconnect_backoff, Duration::from_millis(30));
    assert_eq!(a.reconnect_backoff_max, Duration::from_millis(300));
    assert_eq!(a.connections_max_idle, Duration::from_secs(120));
    assert_eq!(a.retry_backoff, Duration::from_millis(45));
    assert_eq!(a.retry_backoff_max, Duration::from_millis(180));
    assert_eq!(a.connect_timeout, Duration::from_secs(2));
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
    let order_topics: Vec<String> = orders.assignment().into_iter().map(|tp| tp.topic).collect();
    let pay_topics: Vec<String> = payments
        .assignment()
        .into_iter()
        .map(|tp| tp.topic)
        .collect();
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
    assert_eq!(
        mock.last_leave_group_version(),
        Some(5),
        "ConsumerGroup must prefer LeaveGroup v5 when the broker advertises it"
    );
    let members = mock.last_leave_group_members().expect("LeaveGroup members");
    assert_eq!(members[0].group_instance_id.as_deref(), Some("worker-1"));
    assert_eq!(
        members[0].reason.as_deref(),
        Some(LEAVE_GROUP_REASON_CLOSED)
    );
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
    assert_eq!(sm.bytes_fetched, 2);
    assert_eq!(sm.fetch_errors, 0);
    assert_eq!(sm.records_acknowledged, 0);
    assert_eq!(sm.fetch_latency.count, 1);
    assert!(sm.fetch_latency.max_nanos >= sm.fetch_latency.min_nanos);
    assert_eq!(sm.topics.len(), 2);
    assert_eq!(sm.topics[0].topic, "orders");
    assert_eq!(sm.topics[0].records_fetched, 1);
    assert_eq!(sm.topics[0].bytes_fetched, 1);
    assert_eq!(sm.topics[0].fetch_latency.count, 1);
    assert_eq!(sm.topics[1].topic, "payments");
    assert_eq!(sm.topics[1].records_fetched, 1);
    assert_eq!(sm.topics[1].bytes_fetched, 1);
    assert_eq!(sm.topics[1].fetch_latency.count, 1);
    assert!(recs.iter().all(|r| r.leader_epoch == Some(0)));
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
    consumer.pause([TopicPartition::new("parts", 1)]);
    assert_eq!(consumer.paused(), vec![TopicPartition::new("parts", 1)]);
    assert_eq!(consumer.position("parts", 0).unwrap(), 0);

    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].partition, 0);
    assert_eq!(recs[0].value.as_deref(), Some(&b"p0"[..]));
    assert_eq!(consumer.position("parts", 0).unwrap(), 1);
    assert_eq!(consumer.position("parts", 1).unwrap(), 0);

    consumer.resume([("parts", 1)]);
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
    let info = consumer
        .partitions_for_timeout("t", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].partition, 0);
    assert_eq!(info[0].leader, 1);
    assert_eq!(info[0].leader_epoch, 0);
    assert!(info[0].offline_replicas.is_empty());
    let end = consumer
        .end_offsets_timeout([("t", 0)], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(end, vec![(TopicPartition::new("t", 0), 1)]);
    let begin = consumer
        .beginning_offsets_timeout([("t", 0)], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(begin, vec![(TopicPartition::new("t", 0), 0)]);
    let listed = consumer
        .list_topics_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert!(listed.iter().any(|p| p.topic == "t" && p.partition == 0));
    consumer
        .assign_many([(TopicPartition::new("t", 0), 0)])
        .await
        .unwrap();
    assert_eq!(consumer.assignment().len(), 1);
    consumer.unassign();
    assert!(consumer.assignment().is_empty());
    consumer.assign_partitions([("t", 0)]).await.unwrap();
    assert_eq!(consumer.position_of(("t", 0)).unwrap(), 0);
    consumer
        .assign_partitions(Vec::<TopicPartition>::new())
        .await
        .unwrap();
    assert!(consumer.assignment().is_empty());
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn consumer_metadata_sends_allow_auto_create_topics() {
    let mock = common::Mock::start().await;
    let mut on = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()]).allow_auto_create_topics(true),
    )
    .await
    .unwrap();
    let _ = on.partitions_for("t").await.unwrap();
    assert_eq!(mock.last_metadata_allow_auto(), Some(true));
    on.close().await.unwrap();

    let mut off = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let _ = off.list_topics().await.unwrap();
    assert_eq!(mock.last_metadata_allow_auto(), Some(false));
    off.close().await.unwrap();
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
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].0, TopicPartition::new("t", 0));
    assert_eq!(before[0].1.offset, -1);
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    group.commit_timeout(Duration::from_secs(5)).await.unwrap();
    let after = group.committed().await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].1.offset, 1);
    assert_eq!(after[0].1.leader_epoch, Some(0));
    let timed = group
        .committed_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed[0].1.offset, 1);
    let one = group
        .committed_for_timeout([TopicPartition::new("t", 0)], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(one[0].1.offset, 1);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn commit_next_offsets_from_poll() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "nxt",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.count(), 2);
    let next = recs.next_offsets();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].0, TopicPartition::new("t", 0));
    assert_eq!(next[0].1.offset, recs[1].offset + 1);
    group
        .commit_with_metadata_timeout(next, Duration::from_secs(5))
        .await
        .unwrap();
    let after = group.committed().await.unwrap();
    assert_eq!(after[0].1.offset, recs[1].offset + 1);
    group.leave().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "nxt",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert!(
        recs.is_empty(),
        "committed nextOffsets should skip consumed records"
    );
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
    assert_eq!(
        mock.last_fetched_epoch(),
        Some(0),
        "rejoin must send OffsetFetch leader_epoch as LastFetchedEpoch"
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
            move |revoked, assigned| {
                added.store(assigned.len(), std::sync::atomic::Ordering::SeqCst);
                assert!(revoked.is_empty());
                let first = assigned.first().expect("first assignment");
                assert_eq!(first.topic, "t");
                assert_eq!(first.partition, 0);
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
    group
        .commit_offsets_timeout([(TopicPartition::new("t", 0), 1)], Duration::from_secs(5))
        .await
        .unwrap();
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
async fn commit_async_does_not_send_until_poll() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"async-poll"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "ca-poll",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    let before = mock.offset_commit_calls();
    group.commit_async();
    assert_eq!(
        mock.offset_commit_calls(),
        before,
        "commitAsync must not send OffsetCommit until poll"
    );
    let recs = group.poll().await.unwrap();
    assert!(recs.is_empty(), "async commit on poll must skip consumed");
    assert_eq!(
        mock.offset_commit_calls(),
        before + 1,
        "next poll must send the queued OffsetCommit"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn commit_async_with_callback_and_leave_flushes() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"async-leave"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "ca-leave",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    let got = std::sync::Arc::new(parking_lot::Mutex::new(None));
    let got_cb = std::sync::Arc::clone(&got);
    group.commit_async_with(move |result| {
        *got_cb.lock() = Some(result.map(|o| o.len()));
    });
    let before = mock.offset_commit_calls();
    group.leave().await.unwrap();
    assert_eq!(
        mock.offset_commit_calls(),
        before + 1,
        "leave must send a queued commitAsync OffsetCommit"
    );
    let n = got
        .lock()
        .clone()
        .expect("commitAsync callback")
        .expect("OffsetCommit ok");
    assert_eq!(n, 1);

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "ca-leave",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert!(
        recs.is_empty(),
        "leave-flushed commitAsync must store the high watermark"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn commit_with_metadata_async_sends_on_poll() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"async-md"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "ca-md",
        "t",
    )
    .await
    .unwrap();
    group.poll().await.unwrap();
    let got = std::sync::Arc::new(parking_lot::Mutex::new(None));
    let got_cb = std::sync::Arc::clone(&got);
    group.commit_with_metadata_async_with(
        [(
            TopicPartition::new("t", 0),
            OffsetAndMetadata::with_metadata(1, "async"),
        )],
        move |result| {
            *got_cb.lock() = Some(result.map(|o| o[0].1.metadata.clone()));
        },
    );
    group.poll().await.unwrap();
    let meta = got
        .lock()
        .clone()
        .expect("commitAsync callback")
        .expect("OffsetCommit ok");
    assert_eq!(meta, "async");
    group.leave().await.unwrap();
}

#[tokio::test]
async fn producer_and_consumer_metrics() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("u", 1, 1)], 10_000, false)
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
            ProduceRecord::to("t").value(&b"ab"[..]),
            ProduceRecord::to("t").value(&b"cd"[..]),
            ProduceRecord::to("u").value(&b"x"[..]),
        ])
        .await
        .unwrap();
    let pm = producer.metrics();
    assert_eq!(pm.records_queued, 3);
    assert_eq!(pm.records_acked, 3);
    assert_eq!(pm.produce_errors, 0);
    assert_eq!(pm.bytes_queued, 5);
    assert_eq!(pm.bytes_buffered, 0);
    assert_eq!(pm.ack_latency.count, 3);
    assert!(pm.ack_latency.max_nanos >= pm.ack_latency.min_nanos);
    assert!(pm.ack_latency.mean_nanos().is_some());
    assert!(pm.ack_latency.p50_nanos <= pm.ack_latency.p99_nanos);
    assert!(pm.ack_latency.p99_nanos <= pm.ack_latency.max_nanos);
    assert_eq!(pm.topics.len(), 2);
    assert_eq!(pm.topics[0].topic, "t");
    assert_eq!(pm.topics[0].records_queued, 2);
    assert_eq!(pm.topics[0].records_acked, 2);
    assert_eq!(pm.topics[0].produce_errors, 0);
    assert_eq!(pm.topics[0].bytes_queued, 4);
    assert_eq!(pm.topics[0].ack_latency.count, 2);
    assert_eq!(pm.topics[1].topic, "u");
    assert_eq!(pm.topics[1].records_queued, 1);
    assert_eq!(pm.topics[1].records_acked, 1);
    assert_eq!(pm.topics[1].bytes_queued, 1);
    assert_eq!(pm.topics[1].ack_latency.count, 1);
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
    assert_eq!(cm.fetch_latency.count, 1);
    assert!(cm.fetch_latency.max_nanos >= cm.fetch_latency.min_nanos);
    assert!(cm.fetch_latency.mean_nanos().is_some());
    assert_eq!(cm.topics.len(), 1);
    assert_eq!(cm.topics[0].topic, "t");
    assert_eq!(cm.topics[0].records_fetched, 2);
    assert_eq!(cm.topics[0].bytes_fetched, 4);
    assert_eq!(cm.topics[0].fetch_latency.count, 1);
}

#[tokio::test]
async fn try_send_queue_full_when_buffer_memory_is_full() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .buffer_memory(3),
    )
    .await
    .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b""[..]))
        .await
        .unwrap();
    producer
        .try_send(ProduceRecord::to("t").value(&b"ab"[..]))
        .unwrap();
    let err = producer
        .try_send(ProduceRecord::to("t").value(&b"cd"[..]))
        .unwrap_err();
    assert!(matches!(err, Error::QueueFull), "got {err}");
    assert_eq!(producer.metrics().bytes_buffered, 2);
    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer
        .try_send(ProduceRecord::to("t").value(&b"cd"[..]))
        .unwrap();
    producer.flush().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn send_times_out_when_record_exceeds_buffer_memory() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .buffer_memory(1)
            .max_block(Duration::from_millis(40)),
    )
    .await
    .unwrap();
    let err = producer
        .send(ProduceRecord::to("t").value(&b"ab"[..]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Timeout), "got {err}");
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn try_send_rejects_when_record_exceeds_max_request_size() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .max_request_size(3),
    )
    .await
    .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b""[..]))
        .await
        .unwrap();
    producer
        .try_send(ProduceRecord::to("t").value(&b"abc"[..]))
        .unwrap();
    producer.flush().await.unwrap();
    let err = producer
        .try_send(ProduceRecord::to("t").value(&b"abcd"[..]))
        .unwrap_err();
    assert!(
        matches!(err, Error::RecordTooLarge { size: 4, max: 3 }),
        "got {err}"
    );
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn send_rejects_when_record_exceeds_max_request_size() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .max_request_size(3)
            .max_block(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let err = producer
        .send(ProduceRecord::to("t").value(&b"abcd"[..]))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::RecordTooLarge { size: 4, max: 3 }),
        "got {err}"
    );
    assert!(!err.is_retriable());
    assert_eq!(
        err.to_string(),
        "record too large: 4 bytes (max.request.size 3)"
    );
    producer.close().await.unwrap();
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
    let a_parts: std::collections::HashSet<i32> =
        a.assignment().iter().map(|tp| tp.partition).collect();
    let b_parts: std::collections::HashSet<i32> =
        b.assignment().iter().map(|tp| tp.partition).collect();
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
    assert_eq!(
        mock.last_leave_group_members()
            .expect("max poll LeaveGroup")[0]
            .reason
            .as_deref(),
        Some(LEAVE_GROUP_REASON_POLL_TIMEOUT)
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

#[tokio::test]
async fn offsets_for_times_finds_record_and_misses() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"old"[..]).timestamp(1_000))
        .await
        .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"new"[..]).timestamp(2_000))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    let tp = TopicPartition::new("t", 0);
    let hit = consumer
        .offsets_for_times_timeout([(tp.clone(), 1_500)], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(hit.len(), 1);
    let found = hit[0].1.expect("should find the 2000ms record");
    assert_eq!(found.offset, 1);
    assert_eq!(found.timestamp, 2_000);
    assert_eq!(found.leader_epoch, Some(0));
    let miss = consumer.offsets_for_times([(tp, 9_999)]).await.unwrap();
    assert!(miss[0].1.is_none());
    consumer.close().await.unwrap();
}

#[test]
fn topic_partition_from_tuple() {
    let tp: TopicPartition = ("orders", 3).into();
    assert_eq!(tp, TopicPartition::new("orders", 3));
    let pair: (String, i32) = tp.clone().into();
    assert_eq!(pair, ("orders".into(), 3));
    assert_eq!(format!("{tp}"), "orders-3");
    let md = OffsetAndMetadata::with_metadata(9, "ckpt").with_leader_epoch(2);
    assert_eq!(md.offset, 9);
    assert_eq!(md.leader_epoch, Some(2));
    assert_eq!(md.metadata, "ckpt");
    let oat = OffsetAndTimestamp::new(1, 2).with_leader_epoch(0);
    assert_eq!(oat.offset, 1);
    assert_eq!(oat.timestamp, 2);
    assert_eq!(oat.leader_epoch, Some(0));
}

#[tokio::test]
async fn current_lag_is_hw_minus_position() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    assert_eq!(
        consumer
            .current_lag_timeout(("t", 0), Duration::from_secs(5))
            .await
            .unwrap(),
        Some(2)
    );
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(
        consumer
            .current_lag(TopicPartition::new("t", 0))
            .await
            .unwrap(),
        Some(0)
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn commit_with_metadata_roundtrip() {
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
        "meta",
        "t",
    )
    .await
    .unwrap();
    assert_eq!(group.subscription(), &["t".to_string()]);
    group
        .commit_with_metadata_timeout(
            [(
                TopicPartition::new("t", 0),
                OffsetAndMetadata::with_metadata(1, "ckpt").with_leader_epoch(0),
            )],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    group.leave().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "meta",
        "t",
    )
    .await
    .unwrap();
    let committed = group.committed().await.unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].1.offset, 1);
    assert_eq!(committed[0].1.leader_epoch, Some(0));
    assert_eq!(committed[0].1.metadata, "ckpt");
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"new"[..]));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn enforce_rebalance_rejoins_on_next_poll() {
    let mock = common::Mock::start().await;
    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "erb",
        "t",
    )
    .await
    .unwrap();
    let before = mock.join_group_calls();
    assert!(before >= 1);
    assert_eq!(
        mock.last_join_group_reason(),
        None,
        "first JoinGroup must send a null Reason"
    );
    group.enforce_rebalance();
    let _recs = group.poll().await.unwrap();
    let after = mock.join_group_calls();
    assert!(
        after > before,
        "enforce_rebalance must JoinGroup again (before {before}, after {after})"
    );
    assert_eq!(
        mock.last_join_group_reason().as_deref(),
        Some(DEFAULT_ENFORCE_REBALANCE_REASON),
        "enforce_rebalance must send JoinGroup v8 Reason"
    );
    group.enforce_rebalance_with("need new assignment");
    let _recs = group.poll().await.unwrap();
    assert_eq!(
        mock.last_join_group_reason().as_deref(),
        Some("need new assignment")
    );
    group.enforce_rebalance_with("");
    let _recs = group.poll().await.unwrap();
    assert_eq!(
        mock.last_join_group_reason().as_deref(),
        Some(DEFAULT_ENFORCE_REBALANCE_REASON),
        "empty enforceRebalance reason must use the Java default"
    );
    let long = "x".repeat(300);
    group.enforce_rebalance_with(long);
    let _recs = group.poll().await.unwrap();
    assert_eq!(
        mock.last_join_group_reason().as_deref().map(str::len),
        Some(255),
        "JoinGroup Reason must truncate to 255 characters"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn group_metadata_and_unsubscribe_resubscribe() {
    let mock = common::Mock::start().await;
    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "sub",
        "t",
    )
    .await
    .unwrap();
    let md = group.group_metadata();
    assert_eq!(md.group_id, "sub");
    assert_eq!(group.group_id(), "sub");
    assert!(!md.member_id.is_empty());
    assert!(md.generation_id >= 1);
    assert!(md.group_instance_id.is_none());
    assert!(!group.assignment().is_empty());
    group.unsubscribe().await.unwrap();
    assert!(group.assignment().is_empty());
    assert!(group.subscription().is_empty());
    assert_eq!(
        mock.last_leave_group_members()
            .expect("unsubscribe LeaveGroup")[0]
            .reason
            .as_deref(),
        Some(LEAVE_GROUP_REASON_UNSUBSCRIBED)
    );
    group.subscribe(["t"]).await.unwrap();
    assert_eq!(group.subscription(), &["t".to_string()]);
    assert!(!group.assignment().is_empty());
    group.leave().await.unwrap();
}

#[tokio::test]
async fn subscribe_switches_topics_without_leave() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(&[NewTopic::new("u", 1, 1)], 10_000, false)
        .await
        .unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "switch",
        "t",
    )
    .await
    .unwrap();
    assert!(group.assignment().iter().all(|tp| tp.topic == "t"));
    group.subscribe(["u"]).await.unwrap();
    assert_eq!(group.subscription(), &["u".to_string()]);
    assert!(
        group.assignment().iter().all(|tp| tp.topic == "u"),
        "subscribe must assign the new topic, got {:?}",
        group.assignment()
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn subscribe_matching_fetches_matching_topics() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(
            &[
                NewTopic::new("orders-a", 1, 1),
                NewTopic::new("orders-b", 1, 1),
                NewTopic::new("payments", 1, 1),
            ],
            10_000,
            false,
        )
        .await
        .unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "pat",
        "t",
    )
    .await
    .unwrap();
    group
        .subscribe_matching(|n: &str| n.starts_with("orders-"))
        .await
        .unwrap();
    assert_eq!(
        group.subscription(),
        &["orders-a".to_string(), "orders-b".to_string()]
    );
    assert!(group
        .assignment()
        .iter()
        .all(|tp| tp.topic.starts_with("orders-")));

    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("orders-a").value(&b"a"[..]),
            ProduceRecord::to("orders-b").value(&b"b"[..]),
            ProduceRecord::to("payments").value(&b"p"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut got = Vec::new();
    for _ in 0..8 {
        let recs = group.poll().await.unwrap();
        got.extend(recs.iter().cloned());
        if got.len() >= 2 {
            break;
        }
    }
    assert_eq!(got.len(), 2, "got {got:?}");
    assert!(got.iter().all(|r| r.topic.starts_with("orders-")));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn join_matching_picks_up_new_topic_on_poll() {
    let mock = common::Mock::start().await;
    let mut group = ConsumerGroup::join_matching(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .metadata_max_age(Duration::ZERO),
        "pat-new",
        |n: &str| n.starts_with("ord-"),
    )
    .await
    .unwrap();
    assert!(
        !group.subscription().iter().any(|t| t.starts_with("ord-")),
        "seeded topic t must not match, got {:?}",
        group.subscription()
    );

    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(&[NewTopic::new("ord-1", 1, 1)], 10_000, false)
        .await
        .unwrap();
    let _ = group.poll().await.unwrap();
    assert_eq!(group.subscription(), &["ord-1".to_string()]);

    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("ord-1").value(&b"x"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut got = Vec::new();
    for _ in 0..8 {
        let recs = group.poll().await.unwrap();
        got.extend(recs.iter().cloned());
        if !got.is_empty() {
            break;
        }
    }
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].topic, "ord-1");
    assert_eq!(got[0].value.as_deref(), Some(&b"x"[..]));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn join_sticky_matching_subscribes() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(
            &[
                NewTopic::new("sticky-a", 1, 1),
                NewTopic::new("other", 1, 1),
            ],
            10_000,
            false,
        )
        .await
        .unwrap();
    let group = ConsumerGroup::join_sticky_matching(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "sticky-pat",
        |n: &str| n.starts_with("sticky-"),
    )
    .await
    .unwrap();
    assert_eq!(group.subscription(), &["sticky-a".to_string()]);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn join_cooperative_sticky_matching_subscribes() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(
            &[NewTopic::new("coop-a", 1, 1), NewTopic::new("other", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    let group = ConsumerGroup::join_cooperative_sticky_matching(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "coop-pat",
        |n: &str| n.starts_with("coop-"),
    )
    .await
    .unwrap();
    assert_eq!(group.subscription(), &["coop-a".to_string()]);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn join_consumer_matching_subscribes() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(&[NewTopic::new("kmatch-a", 1, 1)], 10_000, false)
        .await
        .unwrap();
    let group = ConsumerGroup::join_consumer_matching(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "kpat",
        |n: &str| n.starts_with("kmatch-"),
    )
    .await
    .unwrap();
    assert_eq!(group.subscription(), &["kmatch-a".to_string()]);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_join_matching_picks_up_new_topic_on_poll() {
    let mock = common::Mock::start().await;
    let mut group = ShareGroup::join_matching(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .metadata_max_age(Duration::ZERO),
        "sg-pat",
        |n: &str| n.starts_with("sh-"),
    )
    .await
    .unwrap();
    assert!(
        !group.subscription().iter().any(|t| t.starts_with("sh-")),
        "seeded topic t must not match, got {:?}",
        group.subscription()
    );

    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(&[NewTopic::new("sh-1", 1, 1)], 10_000, false)
        .await
        .unwrap();
    let _ = group.poll().await.unwrap();
    assert_eq!(group.subscription(), &["sh-1".to_string()]);

    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("sh-1").value(&b"x"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut got = Vec::new();
    for _ in 0..8 {
        let recs = group.poll().await.unwrap();
        got.extend(recs.iter().cloned());
        if !got.is_empty() {
            break;
        }
    }
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].topic, "sh-1");
    group.accept(&got).await.unwrap();
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_subscribe_matching_replaces_subscription() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(&[NewTopic::new("pat-s", 1, 1)], 10_000, false)
        .await
        .unwrap();
    let mut group = ShareGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "sg-sub-pat",
        "t",
    )
    .await
    .unwrap();
    group
        .subscribe_matching(|n: &str| n.starts_with("pat-"))
        .await
        .unwrap();
    assert_eq!(group.subscription(), &["pat-s".to_string()]);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn kip848_unsubscribe_then_subscribe() {
    let mock = common::Mock::start().await;
    let mut group = ConsumerGroup::join_consumer(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "ksub",
        "t",
    )
    .await
    .unwrap();
    assert!(!group.assignment().is_empty());
    group.unsubscribe().await.unwrap();
    assert!(group.assignment().is_empty());
    group.subscribe(["t"]).await.unwrap();
    assert!(!group.assignment().is_empty());
    group.leave().await.unwrap();
}

#[tokio::test]
async fn fetch_timeout_returns_records() {
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

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer
        .fetch_timeout(Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(recs.len(), 1);
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn poll_timeout_returns_records() {
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
        "pto",
        "t",
    )
    .await
    .unwrap();
    let recs = group
        .poll_timeout(Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(recs.len(), 1);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn send_offsets_with_metadata_then_committed() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-meta".into());
    let producer = Producer::new(pcfg).await.unwrap();
    producer.begin_transaction().await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"x"[..]))
        .await
        .unwrap();
    let md = partitionline::ConsumerGroupMetadata {
        group_id: "txn-g".into(),
        generation_id: 1,
        member_id: "m".into(),
        group_instance_id: None,
    };
    producer
        .send_offsets_for_group(
            &md,
            [(
                TopicPartition::new("t", 0),
                OffsetAndMetadata::with_metadata(1, "eos").with_leader_epoch(0),
            )],
        )
        .await
        .unwrap();
    assert_eq!(
        mock.last_txn_offset_commit_version(),
        Some(5),
        "Producer must prefer TxnOffsetCommit v5 when the broker advertises it"
    );
    assert_eq!(
        mock.last_add_offsets_to_txn_version(),
        None,
        "TxnOffsetCommit v5 skips AddOffsetsToTxn (transaction V2)"
    );
    assert_eq!(mock.last_txn_offset_generation(), Some(1));
    assert_eq!(mock.last_txn_offset_member_id().as_deref(), Some("m"));
    producer.commit_transaction().await.unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "txn-g",
        "t",
    )
    .await
    .unwrap();
    let committed = group.committed().await.unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].1.offset, 1);
    assert_eq!(committed[0].1.metadata, "eos");
    group.leave().await.unwrap();
}

#[tokio::test]
async fn producer_partitions_for_returns_leader() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"x"[..]))
        .await
        .unwrap();
    let infos = producer.partitions_for("t").await.unwrap();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].topic, "t");
    assert_eq!(infos[0].partition, 0);
    assert!(infos[0].leader >= 0);
    let timed = producer
        .partitions_for_timeout("t", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].partition, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn admin_close_drops_handle() {
    let mock = common::Mock::start().await;
    let admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_close_timeout_drops_handle() {
    let mock = common::Mock::start().await;
    let admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin.close_timeout(Duration::from_secs(1)).await.unwrap();
}

struct CloseProd(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl ProducerInterceptor for CloseProd {
    fn close(&self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

struct CommitClose {
    commits: std::sync::Arc<std::sync::atomic::AtomicU64>,
    closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ConsumerInterceptor for CommitClose {
    fn on_commit(&self, offsets: &[(TopicPartition, OffsetAndMetadata)]) {
        let n = u64::try_from(offsets.len()).unwrap_or(u64::MAX);
        let _ = self
            .commits
            .fetch_add(n, std::sync::atomic::Ordering::SeqCst);
    }

    fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn interceptor_close_and_on_commit() {
    let mock = common::Mock::start().await;
    let prod_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .interceptor(CloseProd(std::sync::Arc::clone(&prod_closed))),
    )
    .await
    .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"x"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    assert!(prod_closed.load(std::sync::atomic::Ordering::SeqCst));

    let commits = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let cons_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .interceptor(CommitClose {
                commits: std::sync::Arc::clone(&commits),
                closed: std::sync::Arc::clone(&cons_closed),
            }),
        "int-c",
        "t",
    )
    .await
    .unwrap();
    drop(group.poll().await);
    group.commit().await.unwrap();
    assert!(commits.load(std::sync::atomic::Ordering::SeqCst) >= 1);
    group.leave().await.unwrap();
    assert!(cons_closed.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn share_unsubscribe_then_subscribe() {
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

    let mut group = ShareGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "sg-sub",
        "t",
    )
    .await
    .unwrap();
    assert_eq!(group.group_id(), "sg-sub");
    assert_eq!(group.subscription(), &["t".to_string()]);
    assert!(!group.assignment().is_empty());
    group.unsubscribe().await.unwrap();
    assert!(group.assignment().is_empty());
    assert!(group.subscription().is_empty());
    group.subscribe(["t"]).await.unwrap();
    assert_eq!(group.subscription(), &["t".to_string()]);
    assert!(!group.assignment().is_empty());
    let recs = group
        .poll_timeout(Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(recs.len(), 1);
    group.accept(&recs).await.unwrap();
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_subscribe_switches_topics() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .create_topics(&[NewTopic::new("u", 1, 1)], 10_000, false)
        .await
        .unwrap();
    admin.close().await.unwrap();

    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("u").value(&b"u"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ShareGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "sg-sw",
        "t",
    )
    .await
    .unwrap();
    assert!(group.assignment().iter().all(|tp| tp.topic == "t"));
    group.subscribe(["u"]).await.unwrap();
    assert_eq!(group.subscription(), &["u".to_string()]);
    assert!(
        group.assignment().iter().all(|tp| tp.topic == "u"),
        "subscribe must assign the new topic, got {:?}",
        group.assignment()
    );
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs.count(), 1);
    assert_eq!(recs.partitions(), vec![TopicPartition::new("u", 0)]);
    assert_eq!(recs.records(TopicPartition::new("u", 0)).count(), 1);
    assert_eq!(recs[0].topic, "u");
    group.accept(&recs).await.unwrap();
    group.leave().await.unwrap();
}

#[tokio::test]
async fn seek_to_and_assigned_partitions() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"b"[..]))
        .await
        .unwrap();
    producer
        .flush_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    assert_eq!(consumer.assignment(), vec![TopicPartition::new("t", 0)]);
    assert_eq!(consumer.assigned_partitions(), consumer.assignment());
    assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 0)]);
    consumer.seek_to(TopicPartition::new("t", 0), 1).unwrap();
    assert_eq!(consumer.position_of(("t", 0)).unwrap(), 1);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].topic_partition(), TopicPartition::new("t", 0));
    assert_eq!(recs[0].offset, 1);
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn seek_with_metadata_sets_position_and_last_fetched_epoch() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"b"[..]))
        .await
        .unwrap();
    producer
        .flush_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    consumer
        .seek_with_metadata(
            TopicPartition::new("t", 0),
            OffsetAndMetadata::with_metadata(1, "ignored").with_leader_epoch(3),
        )
        .unwrap();
    assert_eq!(consumer.position_of(("t", 0)).unwrap(), 1);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].offset, 1);
    assert_eq!(
        mock.last_fetched_epoch(),
        Some(3),
        "seek_with_metadata must send leader epoch as LastFetchedEpoch"
    );
    let err = consumer
        .seek_with_metadata(("u", 0), OffsetAndMetadata::new(0))
        .unwrap_err();
    assert!(
        err.to_string().contains("unassigned"),
        "seek of unassigned must fail, got {err}"
    );
    consumer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "swm",
        "t",
    )
    .await
    .unwrap();
    group
        .seek_with_metadata(("t", 0), OffsetAndMetadata::new(0).with_leader_epoch(5))
        .unwrap();
    assert_eq!(group.position_of(("t", 0)).unwrap(), 0);
    let recs = group.poll().await.unwrap();
    assert!(!recs.is_empty());
    assert_eq!(
        mock.last_fetched_epoch(),
        Some(5),
        "group seek_with_metadata must send LastFetchedEpoch"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn group_seek_to_beginning_rereads() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"g"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "skb",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(
        group.list_offset(("t", 0), LATEST_TIMESTAMP).await.unwrap(),
        1
    );
    group.seek_to_beginning().await.unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"g"[..]));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn init_transactions_requires_transactional_id() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let err = producer.init_transactions().await.unwrap_err();
    assert!(
        matches!(err, Error::Protocol(_)),
        "expected protocol error, got {err}"
    );
    producer.close().await.unwrap();

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-init".into());
    let txn = Producer::new(pcfg).await.unwrap();
    txn.init_transactions().await.unwrap();
    txn.begin_transaction().await.unwrap();
    txn.abort_transaction().await.unwrap();
    txn.close_timeout(Duration::from_secs(5)).await.unwrap();
}

#[tokio::test]
async fn client_instance_id_is_kip714() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let id = admin
        .client_instance_id_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(id, [0x11; 16]);
    assert_eq!(mock.last_get_telemetry_subscriptions(), Some([0; 16]));
    assert_eq!(admin.client_instance_id().await.unwrap(), id);
    assert_eq!(
        admin
            .client_instance_id_timeout(Duration::ZERO)
            .await
            .unwrap(),
        id
    );
    admin.close().await.unwrap();

    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    assert_eq!(
        producer
            .client_instance_id_timeout(Duration::from_secs(5))
            .await
            .unwrap(),
        [0x11; 16]
    );
    assert_eq!(
        producer
            .client_instance_id_timeout(Duration::ZERO)
            .await
            .unwrap(),
        [0x11; 16]
    );
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    assert_eq!(
        consumer
            .client_instance_id_timeout(Duration::from_secs(5))
            .await
            .unwrap(),
        [0x11; 16]
    );
    assert_eq!(
        consumer
            .client_instance_id_timeout(Duration::ZERO)
            .await
            .unwrap(),
        [0x11; 16]
    );
    consumer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "cid-group",
        "t",
    )
    .await
    .unwrap();
    assert_eq!(
        group
            .client_instance_id_timeout(Duration::from_secs(5))
            .await
            .unwrap(),
        [0x11; 16]
    );
    assert_eq!(
        group
            .client_instance_id_timeout(Duration::ZERO)
            .await
            .unwrap(),
        [0x11; 16]
    );
    group.close().await.unwrap();

    let mut share = ShareGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "cid-share",
        "t",
    )
    .await
    .unwrap();
    assert_eq!(
        share
            .client_instance_id_timeout(Duration::from_secs(5))
            .await
            .unwrap(),
        [0x11; 16]
    );
    assert_eq!(
        share
            .client_instance_id_timeout(Duration::ZERO)
            .await
            .unwrap(),
        [0x11; 16]
    );
    share.close().await.unwrap();
}

#[tokio::test]
async fn group_and_share_close_timeout_leaves() {
    let mock = common::Mock::start().await;
    let group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "close-to",
        "t",
    )
    .await
    .unwrap();
    group.close_timeout(Duration::from_secs(5)).await.unwrap();

    let share = ShareGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "sg-close-to",
        "t",
    )
    .await
    .unwrap();
    share.close_timeout(Duration::from_secs(5)).await.unwrap();
}

#[tokio::test]
async fn assign_partitions_uses_auto_offset_reset() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut earliest =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    earliest.assign_partitions([("t", 0)]).await.unwrap();
    assert_eq!(earliest.position_of(("t", 0)).unwrap(), 0);
    let recs = earliest.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    earliest
        .close_timeout(Duration::from_secs(5))
        .await
        .unwrap();

    let mut latest = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .auto_offset_reset(AutoOffsetReset::Latest),
    )
    .await
    .unwrap();
    latest
        .assign_partitions_timeout([("t", 0)], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(latest.position_of(("t", 0)).unwrap(), 1);
    let recs = latest.fetch().await.unwrap();
    assert!(recs.is_empty());
    latest.close().await.unwrap();

    let mut none = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()]).auto_offset_reset(AutoOffsetReset::None),
    )
    .await
    .unwrap();
    let err = none.assign_partitions([("t", 0)]).await.unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
    none.close().await.unwrap();
}

#[tokio::test]
async fn admin_list_and_alter_consumer_group_offsets() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .alter_consumer_group_offsets(
            "g-off",
            [(
                TopicPartition::new("t", 0),
                OffsetAndMetadata::with_metadata(3, "admin").with_leader_epoch(0),
            )],
        )
        .await
        .unwrap();
    assert_eq!(
        mock.last_offset_commit_version(),
        Some(9),
        "Admin must prefer OffsetCommit v9 when the broker advertises it"
    );
    let listed = admin
        .list_consumer_group_offsets("g-off", [TopicPartition::new("t", 0)])
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, TopicPartition::new("t", 0));
    assert_eq!(listed[0].1.offset, 3);
    assert_eq!(listed[0].1.metadata, "admin");
    assert_eq!(listed[0].1.leader_epoch, Some(0));
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(9),
        "Admin must prefer OffsetFetch v9 when the broker advertises it"
    );
    assert_eq!(mock.last_offset_fetch_null_topics(), Some(false));
    assert_eq!(mock.last_offset_fetch_require_stable(), Some(false));
    let mut all = admin
        .list_all_consumer_group_offsets("g-off")
        .await
        .unwrap();
    all.sort_by_key(|(tp, _)| (tp.topic.clone(), tp.partition));
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].0, TopicPartition::new("t", 0));
    assert_eq!(all[0].1.offset, 3);
    assert_eq!(mock.last_offset_fetch_null_topics(), Some(true));
    let stable = admin
        .list_consumer_group_offsets_with(
            "g-off",
            [TopicPartition::new("t", 0)],
            true,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(stable.len(), 1);
    assert_eq!(mock.last_offset_fetch_require_stable(), Some(true));
    assert_eq!(mock.last_offset_fetch_null_topics(), Some(false));
    let timed = admin
        .list_consumer_group_offsets_timeout(
            "g-off",
            [TopicPartition::new("t", 0)],
            Duration::from_secs(12),
        )
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(mock.last_offset_fetch_require_stable(), Some(false));
    let all_stable = admin
        .list_all_consumer_group_offsets_with("g-off", true, Duration::from_secs(8))
        .await
        .unwrap();
    assert_eq!(all_stable.len(), 1);
    assert_eq!(mock.last_offset_fetch_require_stable(), Some(true));
    assert_eq!(mock.last_offset_fetch_null_topics(), Some(true));
    admin
        .alter_consumer_group_offsets(
            "g-off-b",
            [(TopicPartition::new("t", 0), OffsetAndMetadata::new(9))],
        )
        .await
        .unwrap();
    let before_groups = mock.offset_fetch_calls();
    let before_find = mock.find_coordinator_calls();
    let mut listed_groups = admin
        .list_consumer_group_offsets_for_groups([
            (
                "g-off",
                ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
            ),
            (
                "g-off-b",
                ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
            ),
        ])
        .await
        .unwrap();
    listed_groups.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(listed_groups.len(), 2);
    assert_eq!(listed_groups[0].0, "g-off");
    assert_eq!(listed_groups[0].1[0].1.offset, 3);
    assert_eq!(listed_groups[1].0, "g-off-b");
    assert_eq!(listed_groups[1].1[0].1.offset, 9);
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(9),
        "batched listConsumerGroupOffsets must prefer OffsetFetch v9"
    );
    assert_eq!(
        mock.last_offset_fetch_group_count(),
        2,
        "KIP-709 OffsetFetch Groups array of N on one coordinator"
    );
    assert_eq!(
        mock.offset_fetch_calls().saturating_sub(before_groups),
        1,
        "groups that share a coordinator must be one OffsetFetch"
    );
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        2,
        "KIP-699 FindCoordinator CoordinatorKeys array of N"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        1,
        "groups that share a coordinator must be one FindCoordinator on v4+"
    );
    let mixed = admin
        .list_consumer_group_offsets_for_groups_with(
            [
                ("g-off", ListConsumerGroupOffsetsSpec::all()),
                (
                    "g-off-b",
                    ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
                ),
            ],
            true,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(mixed.len(), 2);
    assert_eq!(mock.last_offset_fetch_require_stable(), Some(true));
    assert_eq!(mock.last_offset_fetch_group_count(), 2);
    let empty_groups =
        admin
            .list_consumer_group_offsets_for_groups(
                Vec::<(String, ListConsumerGroupOffsetsSpec)>::new(),
            )
            .await
            .unwrap();
    assert!(empty_groups.is_empty());
    let empty = admin
        .list_consumer_group_offsets("g-off", Vec::<TopicPartition>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_list_offsets_earliest_and_latest() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let listed = admin
        .list_offsets([(("t", 0), EARLIEST_TIMESTAMP), (("t", 0), LATEST_TIMESTAMP)])
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].0, TopicPartition::new("t", 0));
    assert_eq!(listed[0].1.offset, 0);
    assert_eq!(listed[0].1.leader_epoch, Some(0));
    assert_eq!(listed[1].0, TopicPartition::new("t", 0));
    assert_eq!(listed[1].1.offset, 1);
    assert_eq!(listed[1].1.leader_epoch, Some(0));
    assert_eq!(
        mock.list_offsets_calls(),
        1,
        "same-leader queries must share one ListOffsets RPC"
    );
    assert_eq!(mock.last_list_offsets_n(), Some(2));
    assert_eq!(mock.last_list_offsets_isolation(), Some(0));
    assert_eq!(
        mock.last_list_offsets_timeout(),
        Some(30_000),
        "list_offsets must send request_timeout as ListOffsets v10 TimeoutMs"
    );
    assert_eq!(
        mock.last_list_offsets_version(),
        Some(10),
        "Admin must prefer ListOffsets v10 when the broker advertises it"
    );
    let sentinels = admin
        .list_offsets([
            (("t", 0), MAX_TIMESTAMP),
            (("t", 0), EARLIEST_LOCAL_TIMESTAMP),
            (("t", 0), LATEST_TIERED_TIMESTAMP),
        ])
        .await
        .unwrap();
    assert_eq!(sentinels.len(), 3);
    assert_eq!(sentinels[0].1.offset, 0, "MAX_TIMESTAMP on one record");
    assert_eq!(
        sentinels[1].1.offset, 0,
        "EARLIEST_LOCAL matches local log start"
    );
    assert_eq!(
        sentinels[2].1.offset, -1,
        "LATEST_TIERED is -1 when the mock has no remote log"
    );
    let committed = admin
        .list_offsets_with_isolation(
            [(("t", 0), LATEST_TIMESTAMP)],
            IsolationLevel::ReadCommitted,
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].1.offset, 1);
    assert_eq!(mock.last_list_offsets_isolation(), Some(1));
    let empty_iso = admin
        .list_offsets_with_isolation(
            Vec::<(TopicPartition, i64)>::new(),
            IsolationLevel::ReadCommitted,
        )
        .await
        .unwrap();
    assert!(empty_iso.is_empty());
    let empty = admin
        .list_offsets(Vec::<(TopicPartition, i64)>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    let timed = admin
        .list_offsets_timeout([(("t", 0), LATEST_TIMESTAMP)], Duration::from_secs(12))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(mock.last_list_offsets_timeout(), Some(12_000));
    let timed_iso = admin
        .list_offsets_with_isolation_timeout(
            [(("t", 0), LATEST_TIMESTAMP)],
            IsolationLevel::ReadCommitted,
            Duration::from_secs(8),
        )
        .await
        .unwrap();
    assert_eq!(timed_iso.len(), 1);
    assert_eq!(mock.last_list_offsets_isolation(), Some(1));
    assert_eq!(mock.last_list_offsets_timeout(), Some(8_000));
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_fence_producers_inits_on_txn_coordinator() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let fenced = admin.fence_producers(["tid-fence"]).await.unwrap();
    assert_eq!(fenced.len(), 1);
    assert_eq!(fenced[0].transactional_id, "tid-fence");
    assert_eq!(fenced[0].producer_id, 1000);
    assert_eq!(fenced[0].epoch, 0);
    assert_eq!(mock.last_init_producer_id_node(), Some(1));
    assert_eq!(mock.last_init_producer_id_timeout(), Some(30_000));
    assert_eq!(
        mock.last_find_coordinator_version(),
        Some(6),
        "Admin must prefer FindCoordinator v6 when the broker advertises it"
    );
    assert_eq!(
        mock.last_init_producer_id_version(),
        Some(5),
        "Admin must prefer InitProducerId v5 when the broker advertises it"
    );
    assert_eq!(
        mock.last_init_producer_id_producer_id(),
        Some(-1),
        "fenceProducers first InitProducerId must send ProducerId -1"
    );
    assert_eq!(
        mock.last_init_producer_id_producer_epoch(),
        Some(-1),
        "fenceProducers first InitProducerId must send ProducerEpoch -1"
    );
    let empty = admin.fence_producers(Vec::<String>::new()).await.unwrap();
    assert!(empty.is_empty());
    let fenced_timeout = admin
        .fence_producers_timeout(["tid-fence-to"], Duration::from_secs(12))
        .await
        .unwrap();
    assert_eq!(fenced_timeout[0].transactional_id, "tid-fence-to");
    assert_eq!(
        mock.last_init_producer_id_timeout(),
        Some(12_000),
        "fence_producers_timeout must send transaction.timeout.ms"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_force_terminate_transaction_fences_one_id() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let terminated = admin.force_terminate_transaction("tid-term").await.unwrap();
    assert_eq!(terminated.transactional_id, "tid-term");
    assert_eq!(terminated.producer_id, 1000);
    assert_eq!(terminated.epoch, 0);
    assert_eq!(mock.last_init_producer_id_node(), Some(1));
    let terminated_to = admin
        .force_terminate_transaction_timeout("tid-term-to", Duration::from_secs(8))
        .await
        .unwrap();
    assert_eq!(terminated_to.transactional_id, "tid-term-to");
    assert_eq!(
        mock.last_init_producer_id_timeout(),
        Some(8_000),
        "force_terminate_transaction_timeout must send transaction.timeout.ms"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_delete_share_groups_uses_delete_groups() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let empty = admin
        .delete_share_groups(Vec::<String>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(mock.last_delete_groups_node(), None);
    let deleted = admin.delete_share_groups(["g-share"]).await.unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].group_id, "g-share");
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(mock.last_delete_groups_node(), Some(1));
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_describe_classic_groups_uses_describe_groups() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let described = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].group_id, "g-classic");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(mock.last_describe_groups_node(), Some(1));
    let empty = admin.describe_classic_groups(&[], false).await.unwrap();
    assert!(empty.is_empty());
    let timed = admin
        .describe_classic_groups_timeout(&["g-classic"], false, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_describe_consumer_groups_uses_describe_groups() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let described = admin
        .describe_consumer_groups(&["g-cons"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].group_id, "g-cons");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(mock.last_describe_groups_node(), Some(1));
    let empty = admin.describe_consumer_groups(&[], false).await.unwrap();
    assert!(empty.is_empty());
    let timed = admin
        .describe_consumer_groups_timeout(&["g-cons"], false, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed[0].group_id, "g-cons");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_list_consumer_groups_uses_list_groups() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let listed = admin
        .list_consumer_groups(&["Stable"], &["classic"])
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].group_id, "g");
    assert_eq!(
        mock.last_list_groups(),
        Some((vec!["Stable".into()], vec!["classic".into()]))
    );
    assert_eq!(mock.last_list_groups_node(), Some(1));
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_delete_consumer_groups_uses_delete_groups() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let empty = admin
        .delete_consumer_groups(Vec::<String>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(mock.last_delete_groups_node(), None);
    let deleted = admin.delete_consumer_groups(["g-cons"]).await.unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].group_id, "g-cons");
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(mock.last_delete_groups_node(), Some(1));
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_describe_share_groups_uses_share_group_describe() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let described = admin
        .describe_share_groups(&["g-share"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].group_id, "g-share");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(mock.last_share_group_describe_node(), Some(1));
    let empty = admin.describe_share_groups(&[], false).await.unwrap();
    assert!(empty.is_empty());
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_list_client_metrics_resources() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let listed = admin.list_client_metrics_resources().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].resource_type, CONFIG_RESOURCE_CLIENT_METRICS);
    assert_eq!(
        mock.last_list_config_resources(),
        Some(vec![CONFIG_RESOURCE_CLIENT_METRICS])
    );
    assert_eq!(mock.last_list_config_resources_node(), Some(1));
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_list_share_group_offsets_uses_describe() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let listed = admin
        .list_share_group_offsets(&[DescribeShareGroupOffsetsGroup::new("sg-off")])
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].group_id, "sg-off");
    assert_eq!(listed[0].error_code, 0);
    assert_eq!(mock.last_describe_share_group_offsets_node(), Some(1));
    let empty = admin.list_share_group_offsets(&[]).await.unwrap();
    assert!(empty.is_empty());
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_delete_consumer_group_offsets_uses_offset_delete() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let deleted = admin
        .delete_consumer_group_offsets("g-off", [TopicPartition::new("t", 0)])
        .await
        .unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(mock.last_offset_delete_node(), Some(1));
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_remove_members_from_consumer_group() {
    let mock = common::Mock::start().await;
    let group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .group_instance_id("worker-rm"),
        "g-rm",
        "t",
    )
    .await
    .unwrap();
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let removed = admin
        .remove_members_from_consumer_group("g-rm", [MemberToRemove::new("worker-rm")])
        .await
        .unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].group_instance_id.as_deref(), Some("worker-rm"));
    assert_eq!(removed[0].error_code, 0);
    assert_eq!(
        mock.last_leave_group_version(),
        Some(5),
        "Admin must prefer LeaveGroup v5 when the broker advertises it"
    );
    let members = mock.last_leave_group_members().expect("LeaveGroup members");
    assert_eq!(
        members[0].reason.as_deref(),
        Some(DEFAULT_LEAVE_GROUP_REASON)
    );
    let empty = admin
        .remove_members_from_consumer_group("g-rm", Vec::<MemberToRemove>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    let timed_member = MemberToRemove::new("worker-timed");
    let timed = admin
        .remove_members_from_consumer_group_timeout("g-rm", [timed_member], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_instance_id.as_deref(), Some("worker-timed"));
    let removed_all = admin
        .remove_all_members_from_consumer_group("g-rm")
        .await
        .unwrap();
    assert!(
        removed_all.is_empty(),
        "member already removed; DescribeGroups has no members"
    );
    admin.close().await.unwrap();
    group.close().await.unwrap();
}

#[tokio::test]
async fn admin_remove_all_members_from_consumer_group() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let unknown = admin
        .remove_all_members_from_consumer_group("g-rm-all")
        .await
        .unwrap();
    assert!(unknown.is_empty(), "unknown group: no LeaveGroup");
    assert!(mock.last_leave_group_members().is_none());
    assert_eq!(mock.last_describe_groups_node(), Some(1));

    let group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .group_instance_id("i-all"),
        "g-rm-all",
        "t",
    )
    .await
    .unwrap();
    let removed = admin
        .remove_all_members_from_consumer_group_timeout("g-rm-all", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].error_code, 0);
    assert_eq!(removed[0].group_instance_id.as_deref(), Some("i-all"));
    assert!(!removed[0].member_id.is_empty());
    assert_eq!(mock.last_describe_groups_node(), Some(1));
    assert_eq!(mock.last_leave_group_node(), Some(1));
    let members = mock.last_leave_group_members().expect("LeaveGroup members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].group_instance_id.as_deref(), Some("i-all"));
    assert!(!members[0].member_id.is_empty());
    assert_eq!(
        mock.last_leave_group_version(),
        Some(5),
        "Admin must prefer LeaveGroup v5 when the broker advertises it"
    );
    assert_eq!(
        members[0].reason.as_deref(),
        Some(DEFAULT_LEAVE_GROUP_REASON)
    );
    admin.close().await.unwrap();
    group.close().await.unwrap();
}

#[tokio::test]
async fn admin_describe_features_from_api_versions() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let features = admin.describe_features().await.unwrap();
    let supported = features
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version supported");
    assert_eq!(supported.min_version, 1);
    assert_eq!(supported.max_version, 20);
    let kraft = features
        .supported_features
        .iter()
        .find(|f| f.name == "kraft.version")
        .expect("kraft.version supported on ApiVersions v4");
    assert_eq!(kraft.min_version, 0);
    assert_eq!(kraft.max_version, 1);
    let finalized = features
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized");
    assert_eq!(finalized.min_version_level, 1);
    assert_eq!(finalized.max_version_level, 20);
    assert_eq!(features.finalized_features_epoch, Some(1));
    assert!(!features.zk_migration_ready);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_abort_transaction_writes_abort_marker() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .abort_transaction(AbortTransactionSpec::new(("t", 0), 1000, 0, 1))
        .await
        .unwrap();
    assert_eq!(mock.last_write_txn_markers_node(), Some(1));
    assert_eq!(
        mock.last_write_txn_markers_version(),
        Some(1),
        "Admin must prefer WriteTxnMarkers v1 when the broker advertises it"
    );
    let marker = mock.last_write_txn_markers().expect("WriteTxnMarkers sent");
    assert_eq!(marker.producer_id, 1000);
    assert_eq!(marker.producer_epoch, 0);
    assert!(!marker.transaction_result);
    assert_eq!(marker.coordinator_epoch, 1);
    assert_eq!(marker.topics.len(), 1);
    assert_eq!(marker.topics[0].name, "t");
    assert_eq!(marker.topics[0].partitions, vec![0]);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_list_and_describe_topics() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let listed = admin.list_topics().await.unwrap();
    assert!(
        listed.iter().any(|t| t.name == "t" && !t.is_internal),
        "seeded topic t"
    );
    assert_eq!(mock.last_metadata_topics(), Some(None));
    assert_eq!(mock.last_metadata_allow_auto(), Some(false));
    let created_internal = admin
        .create_topics(&[NewTopic::new("lt-internal", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created_internal[0].error_code, 0);
    mock.set_topic_internal("lt-internal", true);
    assert_eq!(mock.topic_is_internal("lt-internal"), Some(true));
    let listed = admin.list_topics_with(false).await.unwrap();
    assert!(!listed.iter().any(|t| t.name == "lt-internal"));
    let listed = admin
        .list_topics_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert!(listed
        .iter()
        .any(|t| t.name == "lt-internal" && t.is_internal));
    let described = admin.describe_topics(["t"]).await.unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].name, "t");
    assert_eq!(described[0].error_code, 0);
    assert!(!described[0].is_internal);
    assert_eq!(described[0].partitions.len(), 1);
    assert_eq!(described[0].partitions[0].partition, 0);
    assert_eq!(described[0].topic_id[0], b't');
    assert_eq!(
        described[0].authorized_operations,
        partitionline::AUTHORIZED_OPERATIONS_OMITTED
    );
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 2000, None))
    );
    assert_eq!(mock.last_metadata_topics(), Some(None));
    assert_eq!(mock.last_metadata_allow_auto(), Some(false));
    let timed = admin
        .describe_topics_timeout(["t"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].name, "t");
    let calls = mock.metadata_calls();
    let empty = admin.describe_topics(Vec::<&str>::new()).await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.metadata_calls(),
        calls,
        "empty describe_topics is a no-op"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_describe_replica_log_dirs() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let empty = admin
        .describe_replica_log_dirs(Vec::<TopicPartitionReplica>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(mock.last_describe_log_dirs_node(), None);
    let described = admin
        .describe_replica_log_dirs([TopicPartitionReplica::new("t", 0, 1)])
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].0, TopicPartitionReplica::new("t", 0, 1));
    assert_eq!(
        described[0].1,
        ReplicaLogDirInfo::new(Some("/d".into()), 0, None, -1)
    );
    assert_eq!(mock.last_describe_log_dirs_node(), Some(1));
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_describe_broker_log_dirs() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let empty = admin
        .describe_broker_log_dirs(Vec::<i32>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(mock.last_describe_log_dirs_node(), None);
    let dirs = admin.describe_broker_log_dirs([1, 1]).await.unwrap();
    assert_eq!(dirs.len(), 1);
    assert_eq!(dirs[0].0, 1);
    assert_eq!(dirs[0].1.error_code, 0);
    assert_eq!(dirs[0].1.results[0].log_dir, "/d");
    assert_eq!(
        mock.last_describe_log_dirs(),
        Some(DescribeLogDirsRequest::new(None))
    );
    assert_eq!(mock.last_describe_log_dirs_node(), Some(1));
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_metrics() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let after_connect = admin.metrics();
    assert!(
        after_connect.requests >= 1,
        "Admin::new sends ApiVersions: {after_connect:?}"
    );
    assert_eq!(after_connect.errors, 0);
    assert_eq!(after_connect.connections, 1);
    assert_eq!(after_connect.request_latency.count, after_connect.requests);
    let before = after_connect.requests;
    admin.describe_cluster().await.unwrap();
    let after_rpc = admin.metrics();
    assert!(
        after_rpc.requests > before,
        "bootstrap RPC must increment requests: before={before} after={after_rpc:?}"
    );
    assert_eq!(after_rpc.errors, 0);
    assert_eq!(after_rpc.connections, 1);
    assert_eq!(after_rpc.request_latency.count, after_rpc.requests);
    admin.close().await.unwrap();
}
