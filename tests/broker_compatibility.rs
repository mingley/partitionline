//! KL01-10 exact histories on an explicitly selected fresh, digest-pinned broker.
#![expect(
    clippy::expect_used,
    reason = "qualification helpers must fail the test on absent required fields or prerequisites"
)]
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use partitionline::admin::{AlterConfig, ConfigResource, ConfigResourceUpdate, NewTopic};
use partitionline::{
    Admin, AutoOffsetReset, Consumer, ConsumerConfig, ConsumerGroup, Error, FetchedRecord,
    IsolationLevel, OffsetSpec, ProduceRecord, Producer, ProducerConfig, ShareGroup,
};

const PER_PARTITION: i64 = 8;
const TOTAL: usize = 16;

struct Row {
    partition: i32,
    offset: i64,
    timestamp: i64,
    value: Vec<u8>,
}
struct Expected {
    topic: String,
    rows: BTreeMap<Vec<u8>, Row>,
}
impl Expected {
    fn check(&self, record: &FetchedRecord, seen: &mut BTreeSet<Vec<u8>>) {
        self.check_fields(
            &record.topic,
            record.partition,
            record.offset,
            record.timestamp,
            record.key.as_deref(),
            record.value.as_deref(),
            seen,
        );
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "compare complete fetched and share record fields"
    )]
    fn check_fields(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        seen: &mut BTreeSet<Vec<u8>>,
    ) {
        assert_eq!(topic, self.topic);
        let key = key.expect("record key absent");
        let expected = self.rows.get(key).expect("unexpected record identity");
        assert_eq!(
            (partition, offset, timestamp),
            (expected.partition, expected.offset, expected.timestamp)
        );
        assert_eq!(value, Some(expected.value.as_slice()), "corrupt payload");
        assert!(seen.insert(key.to_vec()), "duplicate record");
    }
    fn complete(&self, seen: &BTreeSet<Vec<u8>>) {
        assert_eq!(seen.len(), self.rows.len(), "missing records");
        assert!(self.rows.keys().all(|key| seen.contains(key)));
    }
}

fn consumer_config(bootstrap: &str) -> ConsumerConfig {
    ConsumerConfig::bootstrap([bootstrap])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .auto_commit(false)
        .max_wait_ms(100)
        .max_poll_records(64)
        .connect_timeout(Duration::from_secs(3))
        .request_timeout(Duration::from_secs(3))
}
fn producer_config(bootstrap: &str) -> ProducerConfig {
    ProducerConfig::bootstrap([bootstrap])
        .linger(Duration::ZERO)
        .connect_timeout(Duration::from_secs(3))
        .request_timeout(Duration::from_secs(3))
        .delivery_timeout(Duration::from_secs(10))
        .max_block(Duration::from_secs(5))
}
fn startup(error: &Error) -> bool {
    matches!(error.broker_code(), Some(14..=16))
}
async fn join_group(
    bootstrap: &str,
    id: &str,
    topic: &str,
    kind: &str,
    attempts: &mut Vec<(String, String)>,
) -> partitionline::Result<ConsumerGroup> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let cfg = if kind == "transaction" {
                consumer_config(bootstrap).isolation(IsolationLevel::ReadCommitted)
            } else {
                consumer_config(bootstrap)
            };
            let result = match kind {
                "kip848" => ConsumerGroup::join_consumer(cfg, id, topic).await,
                "cooperative" => ConsumerGroup::join_cooperative_sticky(cfg, id, topic).await,
                _ => ConsumerGroup::join(cfg, id, topic).await,
            };
            match result {
                Ok(group) => return Ok(group),
                Err(error) if startup(&error) => {
                    eprintln!("coordinator startup {kind}: {error}");
                    attempts.push((kind.to_owned(), error.to_string()));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)?
}
async fn new_transactional(
    bootstrap: &str,
    id: &str,
    attempts: &mut Vec<(String, String)>,
) -> partitionline::Result<Producer> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match Producer::new(producer_config(bootstrap).transactional_id(id)).await {
                Ok(producer) => return Ok(producer),
                Err(error) if startup(&error) => {
                    eprintln!("transaction coordinator startup: {error}");
                    attempts.push(("transaction".into(), error.to_string()));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)?
}
async fn verify_committed(
    group: &mut ConsumerGroup,
) -> partitionline::Result<BTreeMap<String, i64>> {
    let mut offsets = BTreeMap::new();
    for (partition, offset) in group.committed().await? {
        assert_eq!(offset.offset, PER_PARTITION);
        assert!(offsets
            .insert(partition.partition.to_string(), offset.offset)
            .is_none());
    }
    assert_eq!(offsets.len(), 2);
    Ok(offsets)
}

struct OffsetVisibility {
    elapsed_ms: u128,
    offsets: Option<BTreeMap<String, i64>>,
}

// EndTxn acknowledges the coordinator decision before every partition's marker
// is applied. Observe stable OffsetFetch results without replaying the transaction.
async fn transaction_committed(
    group: &mut ConsumerGroup,
) -> partitionline::Result<(BTreeMap<String, i64>, Vec<OffsetVisibility>)> {
    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut observations = Vec::new();
        loop {
            let offsets = match group.committed().await {
                Ok(partitions) => {
                    let mut offsets = BTreeMap::new();
                    for (partition, metadata) in partitions {
                        assert!((0..2).contains(&partition.partition));
                        assert!((-1..=PER_PARTITION).contains(&metadata.offset));
                        assert!(offsets
                            .insert(partition.partition.to_string(), metadata.offset)
                            .is_none());
                    }
                    assert_eq!(offsets.len(), 2);
                    Some(offsets)
                }
                Err(Error::Broker {
                    code: partitionline::error::UNSTABLE_OFFSET_COMMIT,
                    ..
                }) => None,
                Err(error) => return Err(error),
            };
            eprintln!(
                "transaction offset visibility: {}ms {offsets:?}",
                start.elapsed().as_millis()
            );
            let complete = offsets
                .as_ref()
                .is_some_and(|o| o.values().all(|v| *v == PER_PARTITION));
            observations.push(OffsetVisibility {
                elapsed_ms: start.elapsed().as_millis(),
                offsets: offsets.clone(),
            });
            if complete {
                return Ok((offsets.expect("complete stable offsets"), observations));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| Error::Timeout)?
}

async fn run() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").expect("explicit owned broker required");
    let requested = std::env::var("PL_COMPAT_REFERENCE").expect("digest-pinned image required");
    let source = std::env::var("PL_COMPAT_SOURCE_SHA").expect("exact source required");
    let prefix = std::env::var("PL_COMPAT_PREFIX").expect("fresh fixture namespace required");
    assert!(requested.contains("@sha256:"));
    let input = format!("{prefix}-input");
    let output = format!("{prefix}-output");
    eprintln!("compatibility phase: admin-connect");
    let mut admin = Admin::connect(bootstrap.clone()).await?;
    let api_ranges: BTreeMap<_, _> = admin
        .versions()
        .iter()
        .map(|(key, range)| (key.to_string(), [range.min_version, range.max_version]))
        .collect();
    for key in [0, 1, 2, 11, 68, 76, 78, 79] {
        assert!(
            admin.versions().contains_key(&key),
            "required API {key} absent"
        );
    }
    eprintln!("compatibility phase: admin-features");
    let features = admin.describe_features().await?;
    let finalized_features: BTreeMap<_, _> = features
        .finalized_features
        .iter()
        .map(|feature| {
            (
                feature.name.clone(),
                [feature.min_version_level, feature.max_version_level],
            )
        })
        .collect();
    for name in [
        "share.version",
        "group.version",
        "transaction.version",
        "metadata.version",
    ] {
        assert!(
            finalized_features
                .get(name)
                .is_some_and(|range| range[0] >= 1),
            "required feature {name} disabled"
        );
    }
    eprintln!("compatibility phase: admin-create-topics");
    for result in admin
        .create_topics(
            &[NewTopic::new(&input, 2, 1), NewTopic::new(&output, 1, 1)],
            10_000,
            false,
        )
        .await?
    {
        assert_eq!(
            result.error_code, 0,
            "create topic {}: {:?}",
            result.name, result.error_message
        );
    }
    eprintln!("compatibility phase: admin-offsets");
    let mut scenarios = BTreeSet::new();
    assert_eq!(
        admin
            .list_offsets([
                ((&input[..], 0), OffsetSpec::earliest()),
                ((&input[..], 1), OffsetSpec::latest())
            ])
            .await?
            .len(),
        2
    );
    assert!(scenarios.insert("admin"));
    eprintln!("compatibility phase: produce");
    let producer = Producer::new(producer_config(&bootstrap)).await?;
    let timestamp = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_millis(),
    )
    .expect("timestamp fits");
    let mut expected = Expected {
        topic: input.clone(),
        rows: BTreeMap::new(),
    };
    for partition in 0..2 {
        for index in 0..PER_PARTITION {
            let key = format!("{partition}:{index}").into_bytes();
            let value = format!(
                "KL01-10/{partition}/{index}/{}",
                "x".repeat(usize::try_from(index).expect("index") + 1)
            )
            .into_bytes();
            let metadata = producer
                .send(
                    ProduceRecord::to(input.as_str())
                        .partition(partition)
                        .key(key.clone())
                        .value(value.clone())
                        .timestamp(timestamp + index),
                )
                .await?;
            assert_eq!((metadata.partition, metadata.offset), (partition, index));
            assert!(expected
                .rows
                .insert(
                    key,
                    Row {
                        partition,
                        offset: index,
                        timestamp: timestamp + index,
                        value
                    }
                )
                .is_none());
        }
    }
    producer.flush().await?;
    producer.close().await?;
    assert!(scenarios.insert("produce"));
    eprintln!("compatibility phase: manual");
    let mut manual = Consumer::new(consumer_config(&bootstrap)).await?;
    manual
        .assign_many([((&input[..], 0), 0), ((&input[..], 1), 0)])
        .await?;
    let mut seen = BTreeSet::new();
    while seen.len() < TOTAL {
        for record in &manual.fetch().await? {
            expected.check(record, &mut seen);
        }
    }
    expected.complete(&seen);
    manual.close().await?;
    for (_, offset) in admin
        .list_offsets([
            ((&input[..], 0), OffsetSpec::latest()),
            ((&input[..], 1), OffsetSpec::latest()),
        ])
        .await?
    {
        assert_eq!(offset.offset, PER_PARTITION);
    }
    assert!(scenarios.insert("manual"));
    let mut attempts = Vec::new();
    let mut committed_offsets = BTreeMap::new();
    let mut group_ids = BTreeMap::new();
    eprintln!("compatibility phase: groups");
    for kind in ["classic", "cooperative", "kip848"] {
        let id = format!("{prefix}-{kind}");
        eprintln!("compatibility group phase: {kind} join");
        let mut group = join_group(&bootstrap, &id, &input, kind, &mut attempts).await?;
        eprintln!("compatibility group phase: {kind} poll");
        let mut seen = BTreeSet::new();
        while seen.len() < TOTAL {
            for record in &group.poll_timeout(Duration::from_secs(3)).await? {
                expected.check(record, &mut seen);
            }
        }
        expected.complete(&seen);
        eprintln!("compatibility group phase: {kind} commit");
        group.commit().await?;
        eprintln!("compatibility group phase: {kind} committed");
        assert!(committed_offsets
            .insert(kind, verify_committed(&mut group).await?)
            .is_none());
        eprintln!("compatibility group phase: {kind} close");
        group.close_timeout(Duration::from_secs(3)).await?;
        assert!(group_ids.insert(kind, id).is_none());
        assert!(scenarios.insert(kind));
    }
    eprintln!("compatibility phase: share");
    let share_id = format!("{prefix}-share");
    let changes = [ConfigResourceUpdate::new(
        ConfigResource::group(&share_id),
        [AlterConfig::set("share.auto.offset.reset", "earliest")],
    )];
    for result in admin.incremental_alter_configs_for(&changes, false).await? {
        assert_eq!(result.error_code, 0, "share group config");
    }
    let mut share = ShareGroup::join(consumer_config(&bootstrap), &share_id, &input).await?;
    let mut seen = BTreeSet::new();
    while seen.len() < TOTAL {
        let records = share.poll_timeout(Duration::from_secs(3)).await?;
        for record in &records {
            expected.check_fields(
                &record.topic,
                record.partition,
                record.offset,
                record.timestamp,
                record.key.as_deref(),
                record.value.as_deref(),
                &mut seen,
            );
        }
        share.accept(&records).await?;
    }
    expected.complete(&seen);
    share.close_timeout(Duration::from_secs(3)).await?;
    assert!(scenarios.insert("share"));
    eprintln!("compatibility phase: transaction");
    let id = format!("{prefix}-transaction");
    let txn = new_transactional(&bootstrap, &id, &mut attempts).await?;
    let mut group = join_group(&bootstrap, &id, &input, "transaction", &mut attempts).await?;
    let mut seen = BTreeSet::new();
    let mut output_expected = Expected {
        topic: output.clone(),
        rows: BTreeMap::new(),
    };
    while seen.len() < TOTAL {
        let records = group.poll_timeout(Duration::from_secs(3)).await?;
        if records.is_empty() {
            continue;
        }
        txn.begin_transaction().await?;
        for record in &records {
            expected.check(record, &mut seen);
            let key = record.key.as_ref().expect("key").to_vec();
            let value = record.value.as_ref().expect("value").to_vec();
            let metadata = txn
                .send(
                    ProduceRecord::to(output.as_str())
                        .partition(0)
                        .key(key.clone())
                        .value(value.clone())
                        .timestamp(record.timestamp),
                )
                .await?;
            assert!(output_expected
                .rows
                .insert(
                    key,
                    Row {
                        partition: 0,
                        offset: metadata.offset,
                        timestamp: record.timestamp,
                        value
                    }
                )
                .is_none());
        }
        txn.send_offsets_for_group(&group.group_metadata(), records.next_offsets())
            .await?;
        txn.commit_transaction().await?;
    }
    expected.complete(&seen);
    let (offsets, visibility) = transaction_committed(&mut group).await?;
    assert!(committed_offsets.insert("transaction", offsets).is_none());
    group.close_timeout(Duration::from_secs(3)).await?;
    assert!(group_ids.insert("transaction", id).is_none());
    txn.begin_transaction().await?;
    drop(
        txn.send(
            ProduceRecord::to(output.as_str())
                .partition(0)
                .key("aborted")
                .value("must stay hidden"),
        )
        .await?,
    );
    txn.abort_transaction().await?;
    txn.close().await?;
    let end = admin
        .list_offsets([((&output[..], 0), OffsetSpec::latest())])
        .await?
        .first()
        .expect("required output end offset")
        .1
        .offset;
    let mut committed =
        Consumer::new(consumer_config(&bootstrap).isolation(IsolationLevel::ReadCommitted)).await?;
    committed.assign(&output, 0, 0).await?;
    let mut seen = BTreeSet::new();
    while committed.position(&output, 0)? < end {
        for record in &committed.fetch().await? {
            output_expected.check(record, &mut seen);
        }
    }
    output_expected.complete(&seen);
    committed.close().await?;
    assert!(scenarios.insert("transaction"));
    println!("PL_COMPAT_SOURCE\t{source}");
    println!("PL_COMPAT_REFERENCE\t{requested}");
    println!("PL_COMPAT_INPUT_TOPIC\t{input}");
    println!("PL_COMPAT_OUTPUT_TOPIC\t{output}");
    println!("PL_COMPAT_SEED_TIMESTAMP\t{timestamp}");
    for scenario in scenarios {
        let records = if scenario == "admin" { 0 } else { TOTAL };
        println!("PL_COMPAT_SCENARIO\t{scenario}\tpassed\t{records}\t0\t0\t0");
    }
    for (key, range) in api_ranges {
        println!("PL_COMPAT_API\t{key}\t{}\t{}", range[0], range[1]);
    }
    for (name, range) in finalized_features {
        println!("PL_COMPAT_FEATURE\t{name}\t{}\t{}", range[0], range[1]);
    }
    for (group, offsets) in committed_offsets {
        for (partition, offset) in offsets {
            println!("PL_COMPAT_COMMITTED\t{group}\t{partition}\t{offset}");
        }
    }
    for (group, id) in group_ids {
        println!("PL_COMPAT_GROUP\t{group}\t{id}");
    }
    for (scenario, error) in attempts {
        println!("PL_COMPAT_STARTUP\t{scenario}\t{}", STANDARD.encode(error));
    }
    for (attempt, observation) in visibility.into_iter().enumerate() {
        match observation.offsets {
            Some(offsets) => println!(
                "PL_COMPAT_VISIBILITY\t{attempt}\t{}\t{}\t{}",
                observation.elapsed_ms,
                offsets.get("0").expect("partition 0 visibility"),
                offsets.get("1").expect("partition 1 visibility")
            ),
            None => println!(
                "PL_COMPAT_VISIBILITY_ERROR\t{attempt}\t{}\t88",
                observation.elapsed_ms
            ),
        }
    }
    println!("PL_COMPAT_ABORTED_VISIBLE\t0");
    println!("PL_COMPAT_SHARE_ACCEPTED\t{TOTAL}");
    println!("PL_COMPAT_COMPLETE");
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit fresh digest-pinned Kafka cell; runner selects this exact test"]
async fn live_compatibility_required() {
    tokio::time::timeout(Duration::from_secs(150), run())
        .await
        .expect("complete compatibility profile exceeded deadline")
        .expect("required broker scenario failed");
}
