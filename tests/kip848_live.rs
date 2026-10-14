//! Live KIP-848 join probe against a real broker.
//!
//! Ignored by default. Run with:
//! `RUN_LIVE=1 KAFKA_BOOTSTRAP=127.0.0.1:9092 cargo test --test kip848_live -- --ignored --nocapture`

use partitionline::{Admin, ConsumerConfig, ConsumerGroup, NewTopic};
use std::collections::HashSet;

#[tokio::test]
#[ignore = "needs a live Kafka 4.x broker with ConsumerGroupHeartbeat"]
async fn kip848_join_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "pl-kip848-smoke".into());
    let group = format!("pl-kip848-probe-{}", std::process::id());
    let cfg = ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500);
    let mut g = ConsumerGroup::join_consumer_topics(cfg, group, [topic])
        .await
        .expect("KIP-848 join");
    let recs = g.poll().await.expect("poll");
    // Seeded topic may be empty if another run consumed; join success is the bar.
    eprintln!("kip848 live: join+poll ok (n={})", recs.len());
}

#[tokio::test]
#[ignore = "needs a live Kafka 4.x broker with ConsumerGroupHeartbeat v1+"]
async fn kip848_regex_against_local_broker() {
    if std::env::var("RUN_LIVE").ok().as_deref() != Some("1") {
        eprintln!("set RUN_LIVE=1 to enable");
        return;
    }
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let suffix = std::process::id();
    let match_a = format!("pl-rx-{suffix}-a");
    let match_b = format!("pl-rx-{suffix}-b");
    let other = format!("pl-rx-{suffix}-z");
    let mut admin = Admin::connect(bootstrap.clone())
        .await
        .expect("admin connect");
    let created = admin
        .create_topics(
            &[
                NewTopic::new(&match_a, 1, 1),
                NewTopic::new(&match_b, 1, 1),
                NewTopic::new(&other, 1, 1),
            ],
            30_000,
            false,
        )
        .await
        .expect("create regex topics");
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
    let listed = admin.list_topics_with(false).await.expect("list topics");
    let listed: HashSet<String> = listed.into_iter().map(|t| t.name().to_string()).collect();
    assert!(
        listed.contains(&match_a) && listed.contains(&match_b) && listed.contains(&other),
        "created topics missing from Metadata: {listed:?}"
    );
    // The coordinator's regex refresh uses its metadata image. Topics created
    // in the same millisecond as join are often missing from the first scan.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let cfg = ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500);
    let named_group = format!("pl-kip848-regex-{suffix}-named");
    let named = ConsumerGroup::join_consumer(cfg.clone(), named_group, match_a.clone())
        .await
        .expect("named KIP-848 join");
    assert!(
        named.assignment().iter().any(|tp| tp.topic == match_a),
        "named join assignment {:?}",
        named.assignment()
    );
    named.leave().await.expect("named leave");
    let group = format!("pl-kip848-regex-{suffix}");
    let regex = format!("pl-rx-{suffix}-[ab]");
    let mut g = ConsumerGroup::join_consumer_regex(cfg, group, regex)
        .await
        .expect("KIP-848 regex join");
    assert_eq!(g.topic_regex().map(str::is_empty), Some(false));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let names = loop {
        let names: HashSet<String> = g.assignment().iter().map(|tp| tp.topic.clone()).collect();
        if names.contains(&match_a) && names.contains(&match_b) && !names.contains(&other) {
            break names;
        }
        if std::time::Instant::now() >= deadline {
            panic!("regex assignment {names:?} wanted {match_a}+{match_b} not {other}");
        }
        let recs = g.poll().await.expect("poll");
        eprintln!(
            "kip848 live regex wait: assignment={names:?} poll_n={}",
            recs.len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    };
    eprintln!("kip848 live regex: assignment={names:?}");
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
}
