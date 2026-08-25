//! Mock-broker coverage of produce, fetch, SASL, admin, and compression.
#![expect(
    unused_results,
    reason = "tests often discard RecordMetadata and admin delete results"
)]
#![expect(
    clippy::let_underscore_must_use,
    reason = "tests discard admin delete results"
)]

mod common;

use partitionline::{
    error, Admin, AdminConfig, Compression, ConfigResource, Consumer, ConsumerConfig,
    ConsumerGroup, Error, NewTopic, ProduceRecord, Producer, ProducerConfig,
};
use std::time::Duration;

#[tokio::test]
async fn idempotent_produce_gets_pid_and_offset() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.enable_idempotence = true;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"idem-hello"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
    let pid = mock.last_producer_id().expect("mock saw a produce batch");
    assert!(
        pid >= 0,
        "idempotent produce must set producer_id, got {pid}"
    );
    assert_ne!(pid, -1);
}

#[tokio::test]
async fn idempotent_unkeyed_multi_conn_stays_in_order() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.enable_idempotence = true;
    pcfg.connections = 8;
    let producer = Producer::new(pcfg).await.unwrap();
    const N: usize = 1024;
    let mut queued = 0usize;
    while queued < N {
        match producer.try_send(ProduceRecord::to("t").value(&b"seq"[..])) {
            Ok(()) => queued += 1,
            Err(Error::QueueFull) => tokio::task::yield_now().await,
            Err(e) => panic!("try_send: {e}"),
        }
    }
    producer.flush().await.unwrap();
    assert_eq!(
        mock.log_len("t", 0),
        N,
        "broker must append every record (error 45 means sequences arrived out of order)"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn flush_fails_on_broker_produce_error() {
    let mock = common::Mock::start().await;
    mock.set_produce_error(error::OUT_OF_ORDER_SEQUENCE_NUMBER);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.connections = 8;
    let producer = Producer::new(pcfg).await.unwrap();
    loop {
        match producer.try_send(ProduceRecord::to("t").value(&b"x"[..])) {
            Ok(()) => break,
            Err(Error::QueueFull) => tokio::task::yield_now().await,
            Err(e) => panic!("try_send: {e}"),
        }
    }
    let err = producer
        .flush()
        .await
        .expect_err("flush must surface broker error");
    match err {
        Error::Broker { code, .. } => assert_eq!(code, error::OUT_OF_ORDER_SEQUENCE_NUMBER),
        other => panic!("expected broker error 45, got {other}"),
    }
    assert_eq!(mock.log_len("t", 0), 0);
}

#[tokio::test]
async fn tls_produce_fetch() {
    let (mock, tls) = common::Mock::start_tls().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.tls = Some(tls.clone());
    let producer = Producer::new(pcfg).await.unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"tls-hello"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.tls = Some(tls);
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"tls-hello"[..]));
}

#[tokio::test]
async fn fetch_offset_out_of_range_jumps_to_log_start() {
    let mock = common::Mock::start().await;
    mock.set_log_start("t", 0, 10);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert!(recs.is_empty());
    assert_eq!(consumer.assignment(), &[("t".into(), 0, 10)]);
}

#[tokio::test]
async fn gzip_produce_fetch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.compression = Compression::Gzip;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"gzip-hello"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"gzip-hello"[..]));
}

#[tokio::test]
async fn snappy_produce_fetch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.compression = Compression::Snappy;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"snappy-hello"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"snappy-hello"[..]));
}

#[tokio::test]
async fn lz4_produce_fetch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.compression = Compression::Lz4;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"lz4-hello"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"lz4-hello"[..]));
}

#[tokio::test]
async fn sasl_scram_sha256_then_produce() {
    let mock = common::Mock::start_with_scram(("alice".into(), "secret".into())).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_scram = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"scram-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_scram_sha512_then_produce() {
    let mock = common::Mock::start_with_scram_sha512(("alice".into(), "secret".into())).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_scram_sha512 = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"scram512-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_oauthbearer_then_produce() {
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_oauthbearer = Some("alice".into());
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"oauth-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_oauthbearer_bad_principal_fails() {
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_oauthbearer = Some("eve".into());
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("bad oauth principal must fail"),
    };
    match err {
        Error::Broker { code, .. } => assert_eq!(code, error::SASL_AUTHENTICATION_FAILED),
        other => panic!("expected broker SASL_AUTHENTICATION_FAILED, got {other}"),
    }
}

#[tokio::test]
async fn sasl_scram_bad_password_fails() {
    let mock = common::Mock::start_with_scram(("alice".into(), "secret".into())).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_scram = Some(("alice".into(), "wrong".into()));
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("bad scram password must fail"),
    };
    match err {
        Error::Broker { code, .. } => assert_eq!(code, error::SASL_AUTHENTICATION_FAILED),
        other => panic!("expected broker SASL_AUTHENTICATION_FAILED, got {other}"),
    }
}

#[tokio::test]
async fn sasl_plain_then_produce() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"sasl-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn consumer_group_join_fetch_commit() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"grouped"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "g1", "t").await.unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"grouped"[..]));
    group.commit().await.unwrap();
}

#[tokio::test]
async fn admin_create_then_produce_fetch() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("orders", 3, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[0].name, "orders");

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(
            ProduceRecord::to("orders")
                .value(&b"admin-hello"[..])
                .partition(1),
        )
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign_topic("orders", 0).await.unwrap();
    assert_eq!(consumer.assignment().len(), 3);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].partition, 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"admin-hello"[..]));
}

#[tokio::test]
async fn admin_create_duplicate_is_already_exists() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let dup = admin
        .create_topics(&[NewTopic::new("t", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(dup[0].error_code, error::TOPIC_ALREADY_EXISTS);
}

#[tokio::test]
async fn admin_validate_only_does_not_create() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("ghost", 1, 1)], 10_000, true)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let err = producer
        .send(ProduceRecord::to("ghost").value(&b"x"[..]))
        .await
        .expect_err("validate_only must not create the topic");
    match err {
        Error::UnknownTopic(t) => assert_eq!(t, "ghost"),
        other => panic!("expected UnknownTopic, got {other}"),
    }
    producer.close().await.unwrap();
}

#[tokio::test]
async fn admin_delete_and_describe() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("orders", 2, 1).config("cleanup.policy", "compact")],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let described = admin
        .describe_configs(
            &[ConfigResource::topic("orders").keys(["cleanup.policy"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(described[0].error_code, 0);
    let entry = described[0]
        .entries
        .iter()
        .find(|e| e.name == "cleanup.policy")
        .expect("cleanup.policy");
    assert_eq!(entry.value.as_deref(), Some("compact"));

    let missing = admin
        .describe_configs(&[ConfigResource::topic("nope")], false)
        .await
        .unwrap();
    assert_eq!(missing[0].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);

    let deleted = admin.delete_topics(&["orders"], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    let gone = admin.delete_topics(&["orders"], 10_000).await.unwrap();
    assert_eq!(gone[0].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);
}

#[tokio::test]
async fn admin_against_kafka_if_present() {
    if tokio::net::TcpStream::connect("127.0.0.1:9092")
        .await
        .is_err()
    {
        return;
    }
    let name = format!("pl-admin-{}", std::process::id());
    let mut admin = Admin::connect("127.0.0.1:9092").await.unwrap();
    let broker = admin
        .describe_configs(&[ConfigResource::broker(1)], false)
        .await
        .unwrap();
    assert_eq!(broker[0].error_code, 0, "broker describe: {broker:?}");
    assert!(
        !broker[0].entries.is_empty(),
        "broker describe returned no entries: {broker:?}"
    );
    let _ = admin.delete_topics(&[&name], 10_000).await;
    let created = admin
        .create_topics(
            &[NewTopic::new(&name, 3, 1).config("cleanup.policy", "delete")],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut described = None;
    for _ in 0..20 {
        let got = admin
            .describe_configs(&[ConfigResource::topic(&name)], false)
            .await
            .unwrap();
        if got[0].error_code == 0 {
            described = Some(got);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let described = described.expect("DescribeConfigs did not see created topic");
    assert!(
        described[0]
            .entries
            .iter()
            .any(|e| e.name == "cleanup.policy"),
        "{described:?}"
    );
    let deleted = admin.delete_topics(&[&name], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0, "{deleted:?}");
}
