//! Live KIP-932 share-group probe against a real broker.
//!
//! Default `share.auto.offset.reset` is latest: join first, then produce.
//! Ignored by default. Run with:
//! `RUN_LIVE=1 KAFKA_BOOTSTRAP=127.0.0.1:9092 cargo test --test share_live -- --ignored --nocapture`

use partitionline::{
    Admin, ConsumerConfig, NewTopic, ProduceRecord, Producer, ProducerConfig, ShareGroup,
};
use std::collections::HashSet;
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "needs a live Kafka 4.1+ broker with share.version=1"]
async fn share_join_then_produce_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let topic = format!("pl-share-live-{suffix}");
    let mut admin = Admin::connect(bootstrap.clone())
        .await
        .expect("admin connect");
    let created = admin
        .create_topics(&[NewTopic::new(&topic, 1, 1)], 30_000, false)
        .await
        .expect("create share topic");
    for result in &created {
        assert!(
            result.error_code == 0
                || result.error_code == partitionline::error::TOPIC_ALREADY_EXISTS,
            "{}: {} {:?}",
            result.name,
            result.error_code,
            result.error_message
        );
    }
    let group = format!("pl-share-live-{suffix}");
    let cfg = ConsumerConfig::bootstrap([bootstrap.clone()]).max_wait_ms(500);
    let mut g = ShareGroup::join(cfg, group, topic.clone())
        .await
        .expect("share join");
    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap]).linger(Duration::ZERO),
    )
    .await
    .expect("producer");
    let md = producer
        .send(ProduceRecord::to(&topic).value(&b"share-live"[..]))
        .await
        .expect("produce after join");
    producer.close().await.expect("producer close");
    let deadline = Instant::now() + Duration::from_secs(15);
    let recs = loop {
        let recs = g.poll().await.expect("share poll");
        if recs.iter().any(|r| r.topic == topic && r.offset == md.offset) {
            break recs;
        }
        assert!(
            Instant::now() < deadline,
            "share poll never saw {}-{}@{}",
            md.topic,
            md.partition,
            md.offset
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    g.accept(&recs).await.expect("accept");
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
    eprintln!(
        "share live: join-then-produce ok {}-{}@{}",
        md.topic, md.partition, md.offset
    );
}

#[tokio::test]
#[ignore = "needs a live Kafka 4.1+ broker with share.version=1"]
async fn share_join_topics_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let a = format!("pl-share-live-{suffix}-a");
    let b = format!("pl-share-live-{suffix}-b");
    let mut admin = Admin::connect(bootstrap.clone())
        .await
        .expect("admin connect");
    let created = admin
        .create_topics(
            &[NewTopic::new(&a, 1, 1), NewTopic::new(&b, 1, 1)],
            30_000,
            false,
        )
        .await
        .expect("create share topics");
    for result in &created {
        assert!(
            result.error_code == 0
                || result.error_code == partitionline::error::TOPIC_ALREADY_EXISTS,
            "{}: {} {:?}",
            result.name,
            result.error_code,
            result.error_message
        );
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let group = format!("pl-share-live-topics-{suffix}");
    let cfg = ConsumerConfig::bootstrap([bootstrap.clone()]).max_wait_ms(500);
    let mut g = ShareGroup::join_topics(cfg, group, [a.clone(), b.clone()])
        .await
        .expect("share join_topics");
    let names: HashSet<String> = g.assignment().iter().map(|tp| tp.topic.clone()).collect();
    let deadline = Instant::now() + Duration::from_secs(15);
    let names = if names.contains(&a) && names.contains(&b) {
        names
    } else {
        loop {
            let _recs = g.poll().await.expect("share poll");
            let names: HashSet<String> =
                g.assignment().iter().map(|tp| tp.topic.clone()).collect();
            if names.contains(&a) && names.contains(&b) {
                break names;
            }
            assert!(
                Instant::now() < deadline,
                "share assignment {names:?} wanted {a}+{b}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
    eprintln!("share live join_topics: assignment={names:?}");
}
