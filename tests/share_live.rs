//! Live KIP-932 share-group probe against a real broker.
//!
//! Default `share.auto.offset.reset` is latest: join first, then produce.
//! Ignored by default. Run with:
//! `RUN_LIVE=1 KAFKA_BOOTSTRAP=127.0.0.1:9092 cargo test --test share_live -- --ignored --nocapture`

use partitionline::{
    Admin, ConsumerConfig, DescribeShareGroupOffsetsGroup, NewTopic, ProduceRecord, Producer,
    ProducerConfig, ShareAcknowledgementMode, ShareGroup, ShareRecords,
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
    // Metadata + first ShareFetch must land before produce. Default
    // share.auto.offset.reset=latest initializes the share partition at
    // high watermark on the first fetch; producing first skips offset 0.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let group = format!("pl-share-live-{suffix}");
    let cfg = ConsumerConfig::bootstrap([bootstrap.clone()]).max_wait_ms(500);
    let mut g = ShareGroup::join(cfg, group.clone(), topic.clone())
        .await
        .expect("share join");
    let md = g.group_metadata();
    assert_eq!(md.group_id(), group);
    assert_eq!(md.member_id(), g.member_id());
    assert!(!md.member_id().is_empty());
    assert_eq!(md.generation_id(), g.member_epoch());
    assert_eq!(g.generation_id(), g.member_epoch());
    assert!(
        md.generation_id() >= 1,
        "share member epoch must be assigned, got {}",
        md.generation_id()
    );
    assert!(md.group_instance_id().is_none());
    let assigned_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let names: HashSet<String> = g.assignment().iter().map(|tp| tp.topic.clone()).collect();
        if names.contains(&topic) {
            let _warmup = g.poll().await.expect("share warmup poll");
            break;
        }
        let _recs = g.poll().await.expect("share poll for assignment");
        assert!(
            Instant::now() < assigned_deadline,
            "share assignment never included {topic}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let producer = Producer::new(ProducerConfig::bootstrap([bootstrap]).linger(Duration::ZERO))
        .await
        .expect("producer");
    let md = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"share-live"[..]))
        .await
        .expect("produce after join");
    producer.close().await.expect("producer close");
    let deadline = Instant::now() + Duration::from_secs(15);
    let recs = loop {
        let recs = g.poll().await.expect("share poll");
        if recs
            .iter()
            .any(|r| r.topic == topic && r.offset == md.offset)
        {
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
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fired_cb = fired.clone();
    g.set_acknowledgement_commit_callback(move |offsets, exception| {
        assert!(exception.is_none(), "live accept callback {exception:?}");
        assert!(
            !offsets.is_empty(),
            "live accept callback must include offsets"
        );
        fired_cb.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    g.accept(&recs).await.expect("accept");
    g.commit().await.expect("commit queued accept");
    assert!(
        fired.load(std::sync::atomic::Ordering::SeqCst),
        "live accept must fire AcknowledgementCommitCallback"
    );
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
            let names: HashSet<String> = g.assignment().iter().map(|tp| tp.topic.clone()).collect();
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

#[tokio::test]
#[ignore = "needs a live Kafka 4.1+ broker with share.version=1"]
async fn share_join_matching_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let keep = format!("pl-sjm-{suffix}-keep");
    let skip = format!("pl-sjm-{suffix}-skip");
    let mut admin = Admin::connect(bootstrap.clone())
        .await
        .expect("admin connect");
    let created = admin
        .create_topics(
            &[NewTopic::new(&keep, 1, 1), NewTopic::new(&skip, 1, 1)],
            30_000,
            false,
        )
        .await
        .expect("create matching topics");
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
    let group = format!("pl-share-live-match-{suffix}");
    let keep_name = keep.clone();
    let cfg = ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500);
    let mut g = ShareGroup::join_matching(cfg, group, move |name: &str| name == keep_name)
        .await
        .expect("share join_matching");
    let deadline = Instant::now() + Duration::from_secs(15);
    let names = loop {
        let names: HashSet<String> = g.assignment().iter().map(|tp| tp.topic.clone()).collect();
        if names.contains(&keep) {
            break names;
        }
        let _recs = g.poll().await.expect("share poll");
        assert!(
            Instant::now() < deadline,
            "share matching assignment {names:?} wanted {keep}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        !names.contains(&skip),
        "share matching assigned skip topic {names:?}"
    );
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
    eprintln!("share live join_matching: assignment={names:?}");
}

#[tokio::test]
#[ignore = "needs a live Kafka 4.1+ broker with share.version=1"]
async fn share_release_then_reject_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let topic = format!("pl-share-ack-{suffix}");
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
    tokio::time::sleep(Duration::from_secs(1)).await;
    let group = format!("pl-share-ack-{suffix}");
    let cfg = ConsumerConfig::bootstrap([bootstrap.clone()]).max_wait_ms(500);
    let mut g = ShareGroup::join(cfg, group, topic.clone())
        .await
        .expect("share join");
    let assigned_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let names: HashSet<String> = g.assignment().iter().map(|tp| tp.topic.clone()).collect();
        if names.contains(&topic) {
            let _warmup = g.poll().await.expect("share warmup poll");
            break;
        }
        let _recs = g.poll().await.expect("share poll for assignment");
        assert!(
            Instant::now() < assigned_deadline,
            "share assignment never included {topic}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let producer =
        Producer::new(ProducerConfig::bootstrap([bootstrap.clone()]).linger(Duration::ZERO))
            .await
            .expect("producer");
    let first_md = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"share-rel"[..]))
        .await
        .expect("produce released record");
    let recs = poll_until(&mut g, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == first_md.offset)
    })
    .await
    .expect("poll released record");
    let first_delivery = recs
        .iter()
        .find(|r| r.offset == first_md.offset)
        .expect("acquired first")
        .delivery_count;
    g.release(&recs).await.expect("release");
    let recs = poll_until(&mut g, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.offset == first_md.offset && r.delivery_count > first_delivery)
    })
    .await
    .expect("poll redelivery");
    let again_delivery = recs
        .iter()
        .find(|r| r.offset == first_md.offset)
        .expect("redelivered")
        .delivery_count;
    g.reject(&recs).await.expect("reject");
    let second_md = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"share-next"[..]))
        .await
        .expect("produce after reject");
    producer.close().await.expect("producer close");
    let recs = poll_until(&mut g, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == second_md.offset)
    })
    .await
    .expect("poll successor");
    assert!(
        recs.iter().all(|r| r.offset != first_md.offset),
        "rejected {}-{}@{} must not redeliver, got {:?}",
        first_md.topic,
        first_md.partition,
        first_md.offset,
        recs.iter()
            .map(|r| (r.offset, r.delivery_count))
            .collect::<Vec<_>>()
    );
    g.accept(&recs).await.expect("accept successor");
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
    eprintln!(
        "share live release/reject ok release={}@{}#{} redeliver#{} next={}@{}",
        first_md.topic,
        first_md.offset,
        first_delivery,
        again_delivery,
        second_md.topic,
        second_md.offset
    );
}

#[tokio::test]
#[ignore = "needs a live Kafka 4.1+ broker with share.version=1"]
async fn share_two_members_same_partition_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let topic = format!("pl-share-two-{suffix}");
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
    tokio::time::sleep(Duration::from_secs(1)).await;
    let group = format!("pl-share-two-{suffix}");
    let cfg = ConsumerConfig::bootstrap([bootstrap.clone()])
        .max_wait_ms(500)
        .max_poll_records(1);
    let mut a = ShareGroup::join(cfg.clone(), group.clone(), topic.clone())
        .await
        .expect("share join A");
    wait_assigned_and_warmup(&mut a, &topic)
        .await
        .expect("A assignment");
    let producer =
        Producer::new(ProducerConfig::bootstrap([bootstrap.clone()]).linger(Duration::ZERO))
            .await
            .expect("producer");
    let first = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"share-a"[..]))
        .await
        .expect("produce first");
    let recs_a = poll_until(&mut a, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == first.offset)
    })
    .await
    .expect("A acquire");
    let mut b = ShareGroup::join(cfg, group, topic.clone())
        .await
        .expect("share join B");
    wait_assigned_and_warmup(&mut b, &topic)
        .await
        .expect("B assignment");
    let second = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"share-b"[..]))
        .await
        .expect("produce second");
    producer.close().await.expect("producer close");
    let recs_b = poll_until(&mut b, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == second.offset)
    })
    .await
    .expect("B acquire");
    assert_eq!(recs_a[0].partition, recs_b[0].partition);
    assert_ne!(
        recs_a[0].offset, recs_b[0].offset,
        "members must not share an acquired offset"
    );
    assert!(
        recs_b.iter().all(|r| r.offset != first.offset),
        "B must not steal A's unacknowledged lock {}@{}",
        first.topic,
        first.offset
    );
    a.accept(&recs_a).await.expect("A accept");
    b.accept(&recs_b).await.expect("B accept");
    a.leave().await.expect("A leave");
    b.leave().await.expect("B leave");
    admin.close().await.expect("admin close");
    eprintln!(
        "share live two-member ok A={}@{} B={}@{}",
        first.topic, first.offset, second.topic, second.offset
    );
}

#[tokio::test]
#[ignore = "needs a live Kafka 4.1+ broker with share.version=1"]
async fn share_implicit_ack_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let topic = format!("pl-share-imp-{suffix}");
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
    tokio::time::sleep(Duration::from_secs(1)).await;
    let group = format!("pl-share-imp-{suffix}");
    let cfg = ConsumerConfig::bootstrap([bootstrap.clone()])
        .max_wait_ms(500)
        .max_poll_records(1)
        .share_acknowledgement_mode(ShareAcknowledgementMode::Implicit);
    let mut g = ShareGroup::join(cfg, group, topic.clone())
        .await
        .expect("share join");
    wait_assigned_and_warmup(&mut g, &topic)
        .await
        .expect("assignment");
    let producer = Producer::new(ProducerConfig::bootstrap([bootstrap]).linger(Duration::ZERO))
        .await
        .expect("producer");
    let first = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"imp-a"[..]))
        .await
        .expect("produce first");
    let recs = poll_until(&mut g, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == first.offset)
    })
    .await
    .expect("poll first");
    let first_off = recs[0].offset;
    let second = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"imp-b"[..]))
        .await
        .expect("produce second");
    producer.close().await.expect("producer close");
    let recs = poll_until(&mut g, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == second.offset)
    })
    .await
    .expect("poll second");
    assert!(
        recs.iter().all(|r| r.offset != first_off),
        "implicit next poll must accept {first_off}, got {:?}",
        recs.iter().map(|r| r.offset).collect::<Vec<_>>()
    );
    assert!(
        g.metrics().records_acknowledged >= 1,
        "implicit accept must increment records_acknowledged"
    );
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
    eprintln!(
        "share live implicit ack ok first={}@{} next={}@{}",
        first.topic, first.offset, second.topic, second.offset
    );
}

#[tokio::test]
#[ignore = "needs a live Kafka 4.1+ broker with share.version=1"]
async fn share_implicit_commit_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let topic = format!("pl-share-cmt-{suffix}");
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
    tokio::time::sleep(Duration::from_secs(1)).await;
    let group = format!("pl-share-cmt-{suffix}");
    let cfg = ConsumerConfig::bootstrap([bootstrap.clone()])
        .max_wait_ms(500)
        .max_poll_records(1)
        .share_acknowledgement_mode(ShareAcknowledgementMode::Implicit);
    let mut g = ShareGroup::join(cfg, group, topic.clone())
        .await
        .expect("share join");
    wait_assigned_and_warmup(&mut g, &topic)
        .await
        .expect("assignment");
    let producer = Producer::new(ProducerConfig::bootstrap([bootstrap]).linger(Duration::ZERO))
        .await
        .expect("producer");
    let first = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"cmt-a"[..]))
        .await
        .expect("produce first");
    let recs = poll_until(&mut g, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == first.offset)
    })
    .await
    .expect("poll first");
    let first_off = recs[0].offset;
    let committed = g.commit_sync().await.expect("commitSync map");
    assert_eq!(
        committed.len(),
        1,
        "commitSync must name the flushed partition"
    );
    assert_eq!(committed[0].0.topic(), topic);
    assert_eq!(committed[0].0.partition(), 0);
    assert!(
        committed[0].1.is_none(),
        "live commitSync Optional.empty(), got {:?}",
        committed[0].1
    );
    assert!(
        g.metrics().records_acknowledged >= 1,
        "commit must flush implicit accept"
    );
    let second = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"cmt-b"[..]))
        .await
        .expect("produce second");
    producer.close().await.expect("producer close");
    let recs = poll_until(&mut g, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == second.offset)
    })
    .await
    .expect("poll second");
    assert!(
        recs.iter().all(|r| r.offset != first_off),
        "committed {first_off} must not redeliver, got {:?}",
        recs.iter().map(|r| r.offset).collect::<Vec<_>>()
    );
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
    eprintln!(
        "share live commit ok first={}@{} next={}@{}",
        first.topic, first.offset, second.topic, second.offset
    );
}

#[tokio::test]
#[ignore = "needs a live Kafka 4.1+ broker with share.version=1"]
async fn share_admin_describe_and_list_offsets_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let topic = format!("pl-share-adm-{suffix}");
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
    tokio::time::sleep(Duration::from_secs(1)).await;
    let group = format!("pl-share-adm-{suffix}");
    let cfg = ConsumerConfig::bootstrap([bootstrap.clone()]).max_wait_ms(500);
    let mut g = ShareGroup::join(cfg, group.clone(), topic.clone())
        .await
        .expect("share join");
    wait_assigned_and_warmup(&mut g, &topic)
        .await
        .expect("assignment");
    let md = g.group_metadata();
    let described = wait_described_member(&mut admin, &group, md.member_id())
        .await
        .expect("describe_share_groups");
    assert_eq!(described.error_code(), 0);
    assert_eq!(described.group_id(), group);
    let member = described
        .members()
        .iter()
        .find(|m| m.member_id() == md.member_id())
        .expect("described member");
    assert!(
        member.subscribed_topic_names.iter().any(|n| n == &topic),
        "subscribed {:?} wanted {topic}",
        member.subscribed_topic_names
    );
    assert!(
        member.member_epoch >= 1,
        "described member epoch {}",
        member.member_epoch
    );
    let producer = Producer::new(ProducerConfig::bootstrap([bootstrap]).linger(Duration::ZERO))
        .await
        .expect("producer");
    let produced = producer
        .send(ProduceRecord::to(topic.as_str()).value(&b"adm"[..]))
        .await
        .expect("produce");
    let recs = poll_until(&mut g, Instant::now() + Duration::from_secs(15), |recs| {
        recs.iter()
            .any(|r| r.topic == topic && r.offset == produced.offset)
    })
    .await
    .expect("poll produced");
    g.accept(&recs).await.expect("accept");
    g.commit().await.expect("commit queued accept");
    let listed = admin
        .list_share_group_offsets(&[DescribeShareGroupOffsetsGroup::all(&group)])
        .await
        .expect("list_share_group_offsets");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].error_code(), 0);
    assert_eq!(listed[0].group_id(), group);
    let part = listed[0]
        .topics()
        .iter()
        .find(|t| t.topic_name() == topic)
        .expect("listed topic")
        .partitions()
        .iter()
        .find(|p| p.partition_index() == 0)
        .expect("listed partition 0");
    assert_eq!(part.error_code(), 0);
    assert!(
        part.start_offset() >= 0,
        "share start offset {}",
        part.start_offset()
    );
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
    eprintln!(
        "share live admin ok describe members={} list {}-0@{}",
        described.members().len(),
        topic,
        part.start_offset()
    );
}

async fn wait_described_member(
    admin: &mut Admin,
    group: &str,
    member_id: &str,
) -> partitionline::Result<partitionline::DescribedShareGroup> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let described = admin.describe_share_groups(&[group], false).await?;
        if let Some(found) = described.into_iter().find(|g| {
            g.group_id() == group
                && g.error_code() == 0
                && g.members().iter().any(|m| m.member_id() == member_id)
        }) {
            return Ok(found);
        }
        if Instant::now() >= deadline {
            return Err(partitionline::Error::protocol(format!(
                "describe_share_groups never listed member {member_id} in {group}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_assigned_and_warmup(g: &mut ShareGroup, topic: &str) -> partitionline::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let names: HashSet<String> = g.assignment().iter().map(|tp| tp.topic.clone()).collect();
        if names.iter().any(|n| n == topic) {
            let _warmup = g.poll().await?;
            return Ok(());
        }
        let _recs = g.poll().await?;
        if Instant::now() >= deadline {
            return Err(partitionline::Error::protocol(format!(
                "share assignment never included {topic}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn poll_until(
    g: &mut ShareGroup,
    deadline: Instant,
    ok: impl Fn(&ShareRecords) -> bool,
) -> partitionline::Result<ShareRecords> {
    loop {
        let recs = g.poll().await?;
        if ok(&recs) {
            return Ok(recs);
        }
        if Instant::now() >= deadline {
            return Err(partitionline::Error::protocol(format!(
                "share poll deadline, last n={}",
                recs.count()
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
