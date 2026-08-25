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
    error, AclBinding, Admin, AdminConfig, AlterConfig, Compression, ConfigResource, Consumer,
    ConsumerConfig, ConsumerGroup, Error, NewTopic, OidcConfig, ProduceRecord, Producer,
    ProducerConfig, ACL_OPERATION_ALL, ACL_PERMISSION_ALLOW, ACL_RESOURCE_TOPIC, ALTER_CONFIG_SET,
    CONFIG_RESOURCE_TOPIC, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP,
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
async fn produce_fetch_follow_metadata_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"leader"[..]))
        .await
        .unwrap();
    assert_eq!(md.partition, 0);
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
    let produced = mock.produce_nodes();
    assert!(
        produced.contains(&2),
        "successful produce must land on leader node 2, got {produced:?}"
    );
    assert_eq!(mock.log_len("t", 0), 1);

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"leader"[..]));
    let fetched = mock.fetch_nodes();
    assert!(
        fetched.contains(&2),
        "successful fetch must hit leader node 2, got {fetched:?}"
    );
}

#[tokio::test]
async fn produce_retries_retriable_then_succeeds() {
    let mock = common::Mock::start().await;
    mock.set_produce_error_times(error::LEADER_NOT_AVAILABLE, 1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"retry"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.flush().await.unwrap();
    assert_eq!(mock.log_len("t", 0), 1);
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
async fn transactional_commit_visible_abort_hidden() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-1".into());
    let producer = Producer::new(pcfg).await.unwrap();
    producer.begin_transaction().await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"committed"[..]))
        .await
        .unwrap();
    producer
        .send_offsets_to_transaction("g", &[("t".into(), 0, 1)])
        .await
        .unwrap();
    producer.commit_transaction().await.unwrap();
    producer.begin_transaction().await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"aborted"[..]))
        .await
        .unwrap();
    producer.abort_transaction().await.unwrap();
    producer.close().await.unwrap();
    assert_eq!(
        mock.last_produce_txn_id().as_deref(),
        Some("tx-1"),
        "Produce body must carry transactional_id, not null"
    );

    let mut ccfg0 = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg0.max_wait_ms = 10;
    ccfg0.isolation_level = 0;
    let mut uncommitted = Consumer::new(ccfg0).await.unwrap();
    uncommitted.assign("t", 0, 0).await.unwrap();
    let all = uncommitted.fetch().await.unwrap();
    let all_vals: Vec<&[u8]> = all.iter().filter_map(|r| r.value.as_deref()).collect();
    assert!(
        all_vals.iter().any(|v| *v == b"aborted"),
        "mock must return aborted records so the client, not the broker, filters them; got {all_vals:?}"
    );

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.isolation_level = 1;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    let vals: Vec<&[u8]> = recs.iter().filter_map(|r| r.value.as_deref()).collect();
    assert_eq!(vals, vec![&b"committed"[..]]);
    assert!(!vals.iter().any(|v| *v == b"aborted"));
}

#[tokio::test]
async fn list_offsets_seek_and_read_committed_isolation() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    for i in 0..3 {
        let v = format!("v{i}");
        producer
            .send(ProduceRecord::to("t").value(v.into_bytes()))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.isolation_level = 1;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    let earliest = consumer
        .list_offsets("t", 0, EARLIEST_TIMESTAMP)
        .await
        .unwrap();
    let latest = consumer
        .list_offsets("t", 0, LATEST_TIMESTAMP)
        .await
        .unwrap();
    assert_eq!(earliest, 0);
    assert_eq!(latest, 3);
    let by_ts = consumer.list_offsets("t", 0, 0).await.unwrap();
    assert_eq!(by_ts, 0);

    consumer.assign("t", 0, 0).await.unwrap();
    consumer.seek("t", 0, 1).unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].offset, 1);
    assert_eq!(mock.last_fetch_isolation(), 1);
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
async fn sasl_oidc_then_produce() {
    let token_url =
        common::start_oidc_token_endpoint("cid".into(), "csecret".into(), "alice".into()).await;
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url, "cid", "csecret"));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"oidc-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_oidc_then_fetch() {
    let token_url =
        common::start_oidc_token_endpoint("cid".into(), "csecret".into(), "alice".into()).await;
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url.clone(), "cid", "csecret"));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"oidc-fetch"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url, "cid", "csecret"));
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"oidc-fetch"[..]));
}

#[tokio::test]
async fn sasl_oidc_bad_secret_fails() {
    let token_url =
        common::start_oidc_token_endpoint("cid".into(), "csecret".into(), "alice".into()).await;
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url, "cid", "wrong"));
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("bad oidc secret must fail"),
    };
    match err {
        Error::Protocol(m) => assert!(m.contains("401") || m.contains("oidc"), "{m}"),
        other => panic!("expected oidc HTTP failure, got {other}"),
    }
}

#[tokio::test]
async fn sasl_oidc_bad_url_fails() {
    let bound = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    drop(bound);
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(
        format!("http://{addr}/oauth/token"),
        "cid",
        "csecret",
    ));
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("closed token URL must fail"),
    };
    match err {
        Error::Io(_) | Error::Timeout | Error::Protocol(_) => {}
        other => panic!("expected token URL failure, got {other}"),
    }
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
async fn two_members_range_partition_all() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("g4", 4, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut a = ConsumerGroup::join(ccfg.clone(), "rg", "g4").await.unwrap();
    assert_eq!(a.assignment().len(), 4, "solo member gets every partition");

    let b_join = tokio::spawn({
        let ccfg = ccfg.clone();
        async move { ConsumerGroup::join(ccfg, "rg", "g4").await }
    });
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(a.poll().await);
    let mut b = b_join.await.unwrap().unwrap();
    let a_parts: std::collections::HashSet<i32> = a.assignment().iter().map(|(_, p)| *p).collect();
    let b_parts: std::collections::HashSet<i32> = b.assignment().iter().map(|(_, p)| *p).collect();
    assert!(a_parts.is_disjoint(&b_parts), "range must not overlap");
    let union: std::collections::HashSet<i32> = a_parts.union(&b_parts).copied().collect();
    assert_eq!(union.len(), 4, "union of assignments is all partitions");
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        mock.heartbeat_total("rg") >= 2,
        "heartbeat loop must run after join, got {}",
        mock.heartbeat_total("rg")
    );

    a.leave().await.unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(b.poll().await);
    assert_eq!(
        b.assignment().len(),
        4,
        "remaining member covers all partitions after leave"
    );
}

#[tokio::test]
async fn two_members_sticky_partition_all() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("s4", 4, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut a = ConsumerGroup::join_sticky(ccfg.clone(), "sg", "s4")
        .await
        .unwrap();
    let b_join = tokio::spawn({
        let ccfg = ccfg.clone();
        async move { ConsumerGroup::join_sticky(ccfg, "sg", "s4").await }
    });
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(a.poll().await);
    let b = b_join.await.unwrap().unwrap();
    let a_parts: std::collections::HashSet<i32> = a.assignment().iter().map(|(_, p)| *p).collect();
    let b_parts: std::collections::HashSet<i32> = b.assignment().iter().map(|(_, p)| *p).collect();
    assert!(a_parts.is_disjoint(&b_parts));
    assert_eq!(a_parts.len() + b_parts.len(), 4);
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
async fn admin_partitions_alter_configs_and_acls() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("acl-t", 1, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let parts = admin
        .create_partitions(&[("acl-t".into(), 3)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    let err = admin
        .incremental_alter_configs(
            CONFIG_RESOURCE_TOPIC,
            "acl-t",
            &[AlterConfig {
                name: "retention.ms".into(),
                op: ALTER_CONFIG_SET,
                value: Some("1000".into()),
            }],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let described = admin
        .describe_configs(
            &[ConfigResource::topic("acl-t").keys(["retention.ms"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .entries
            .iter()
            .find(|e| e.name == "retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("1000")
    );
    let created = admin
        .create_acls(&[AclBinding {
            resource_type: ACL_RESOURCE_TOPIC,
            resource_name: "acl-t".into(),
            principal: "User:alice".into(),
            host: "*".into(),
            operation: ACL_OPERATION_ALL,
            permission: ACL_PERMISSION_ALLOW,
        }])
        .await
        .unwrap();
    assert_eq!(created, vec![0]);
    let listed = admin.describe_acls(ACL_RESOURCE_TOPIC).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].principal, "User:alice");
    assert_eq!(admin.delete_acls(ACL_RESOURCE_TOPIC).await.unwrap(), 0);
    assert!(admin
        .describe_acls(ACL_RESOURCE_TOPIC)
        .await
        .unwrap()
        .is_empty());
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
