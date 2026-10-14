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
    let group = format!("pl-kip848-regex-{suffix}");
    let regex = format!("pl-rx-{suffix}-[ab]");
    let cfg = ConsumerConfig::bootstrap([bootstrap]).max_wait_ms(500);
    let mut g = ConsumerGroup::join_consumer_regex(cfg, group, regex)
        .await
        .expect("KIP-848 regex join");
    assert_eq!(g.topic_regex().map(str::is_empty), Some(false));
    let names: HashSet<String> = g.assignment().iter().map(|tp| tp.topic.clone()).collect();
    assert!(
        names.contains(&match_a) && names.contains(&match_b) && !names.contains(&other),
        "regex assignment {names:?} wanted {match_a}+{match_b} not {other}"
    );
    let recs = g.poll().await.expect("poll");
    eprintln!(
        "kip848 live regex: assignment={names:?} poll_n={}",
        recs.len()
    );
    g.leave().await.expect("leave");
    admin.close().await.expect("admin close");
}
