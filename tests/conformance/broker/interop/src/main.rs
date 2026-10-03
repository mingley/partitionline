//! Bounded public Rust-client ordinary interoperability and restart peer.

use std::{
    collections::BTreeMap,
    error::Error as StdError,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use partitionline::{
    Acks, Admin, AdminConfig, Compression, ConfigResource, Consumer, ConsumerConfig, Error,
    FetchedRecord, Header, IsolationLevel, NewTopic, ProduceRecord, Producer, ProducerConfig,
    TimestampType,
};
use sha2::{Digest, Sha256};
use tokio::fs;

type Result<T> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;
const TIME: i64 = 1_700_000_000_000;
const COUNT: i64 = 12;
const DEADLINE: Duration = Duration::from_secs(5);
const RELEASES: [&str; 3] = ["4.1.2", "4.2.1", "4.3.1"];
const ACKS: [(&str, Acks); 3] = [("1", Acks::Leader), ("-1", Acks::All), ("0", Acks::None)];

#[derive(Default)]
struct Proof {
    assertions: usize,
    records: usize,
    history: Vec<String>,
}

impl Proof {
    fn check(&mut self, condition: bool, label: &str) -> Result<()> {
        self.assertions += 1;
        if !condition {
            return Err(format!("assertion failed: {label}").into());
        }
        Ok(())
    }

    fn event(&mut self, event: String) -> Result<()> {
        self.check(self.history.len() < 2048, "bounded receipt history")?;
        self.history.push(event);
        Ok(())
    }
}

fn quote(value: &str) -> String {
    let mut result = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            other => result.push(other),
        }
    }
    result.push('"');
    result
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(char::from(
            *DIGITS.get(usize::from(byte >> 4)).unwrap_or(&b'?'),
        ));
        result.push(char::from(
            *DIGITS.get(usize::from(byte & 15)).unwrap_or(&b'?'),
        ));
    }
    result
}

fn optional_hex(bytes: Option<&[u8]>) -> String {
    bytes.map_or_else(|| "null".into(), |value| quote(&hex(value)))
}

fn receipt(record: &FetchedRecord) -> String {
    let headers: Vec<String> = record
        .headers
        .iter()
        .map(|header| {
            format!(
                "{{\"key\":{},\"value_hex\":{}}}",
                quote(&header.key),
                optional_hex(header.value.as_deref())
            )
        })
        .collect();
    let content = format!(
        "{{\"topic\":{},\"partition\":{},\"offset\":{},\"timestamp\":{},\"key_hex\":{},\"value_hex\":{},\"headers\":[{}]}}",
        quote(&record.topic), record.partition, record.offset, record.timestamp,
        optional_hex(record.key.as_deref()), optional_hex(record.value.as_deref()), headers.join(",")
    );
    format!(
        "{{\"sha256\":{},\"record\":{content}}}",
        quote(&hex(&Sha256::digest(content.as_bytes())))
    )
}

fn record(topic: &str, partition: i32, index: i64) -> ProduceRecord {
    let identity = format!("{topic}:{partition}:{index}");
    let mut result = ProduceRecord::to(topic)
        .partition(partition)
        .timestamp(TIME + i64::from(partition) * 100 + index)
        .header("receipt", identity.clone())
        .header("dup", "a")
        .null_header("dup");
    if index % 3 != 0 {
        result = result.key(format!("key:{identity}"));
    }
    if index % 4 != 0 {
        result = result.value(if index % 4 == 1 {
            String::new()
        } else {
            format!("value:{identity}")
        });
    }
    result
}

fn producer_config(bootstrap: &str, acks: Acks) -> ProducerConfig {
    ProducerConfig::bootstrap([bootstrap])
        .acks(acks)
        .idempotent(false)
        .compression(Compression::None)
        .connections(1)
        .max_in_flight(1)
        .buffer_memory(1 << 20)
        .max_request_size(65_536)
        .batch_bytes(4096)
        .batch_records(16)
        .linger(Duration::ZERO)
        .request_timeout(DEADLINE)
        .connect_timeout(DEADLINE)
        .delivery_timeout(Duration::from_secs(10))
        .max_block(DEADLINE)
}

fn consumer_config(bootstrap: &str, isolation: IsolationLevel) -> ConsumerConfig {
    ConsumerConfig::bootstrap([bootstrap])
        .auto_commit(false)
        .allow_auto_create_topics(false)
        .isolation(isolation)
        .fetch_max_bytes(65_536)
        .max_partition_fetch_bytes(4096)
        .max_poll_records(64)
        .buffer_memory(1 << 20)
        .max_wait_ms(20)
        .request_timeout(DEADLINE)
        .connect_timeout(DEADLINE)
}

fn check_profile(
    proof: &mut Proof,
    versions: &std::collections::HashMap<i16, partitionline::protocol::api::ApiVersion>,
) -> Result<()> {
    let actual: BTreeMap<i16, (i16, i16)> = versions
        .iter()
        .map(|(&key, value)| (key, (value.min_version, value.max_version)))
        .collect();
    let expected = BTreeMap::from([
        (0, (3, 13)),
        (1, (4, 6)),
        (2, (1, 3)),
        (3, (0, 13)),
        (18, (0, 4)),
        (19, (2, 4)),
        (20, (1, 6)),
    ]);
    proof.check(
        actual == expected,
        "public client observed exact seven-API profile",
    )?;
    proof.event(format!(
        "{{\"label\":\"public-ApiVersions\",\"profile\":{}}}",
        quote(&format!("{actual:?}"))
    ))
}

async fn identity(
    admin: &mut Admin,
    proof: &mut Proof,
    state: &Path,
    topic: &str,
    partitions: usize,
    seed: bool,
) -> Result<[u8; 16]> {
    let description = admin.describe_topics([topic]).await?;
    proof.check(description.len() == 1, "one actual topic description")?;
    let row = description.first().ok_or("missing topic description")?;
    proof.check(
        row.error_code == 0 && row.partitions.len() == partitions && row.topic_id != [0; 16],
        "actual allocated UUID and partition count",
    )?;
    let path = state.join(format!("{topic}.rust-uuid"));
    if seed {
        proof.check(!path.exists(), "fresh topic UUID receipt")?;
        fs::write(&path, hex(&row.topic_id)).await?;
    } else {
        let size = fs::metadata(&path).await?.len();
        proof.check(size == 32, "bounded saved UUID")?;
        proof.check(
            fs::read_to_string(&path).await? == hex(&row.topic_id),
            "topic UUID retained through process restart",
        )?;
    }
    proof.event(format!(
        "{{\"label\":\"public-topic-identity\",\"topic\":{},\"uuid_hex\":{}}}",
        quote(topic),
        quote(&hex(&row.topic_id))
    ))?;
    Ok(row.topic_id)
}

async fn write_rust(
    bootstrap: &str,
    admin: &mut Admin,
    proof: &mut Proof,
    state: &Path,
    restart: bool,
) -> Result<()> {
    for (name, acks) in ACKS {
        let topic = format!("ordinary-rust-acks{name}");
        if !restart {
            let created = admin
                .create_topics(&[NewTopic::new(&topic, 2, 1)], 1000, false)
                .await?;
            proof.check(
                created.len() == 1 && created.first().is_some_and(|r| r.error_code == 0),
                "public Admin created real Rust topic",
            )?;
        }
        let _id = identity(admin, proof, state, &topic, 2, !restart).await?;
        let producer = Producer::new(producer_config(bootstrap, acks)).await?;
        for partition in 0..2 {
            let (start, end) = if restart {
                (COUNT, COUNT + 1)
            } else {
                (0, COUNT)
            };
            for index in start..end {
                let sent = producer.send(record(&topic, partition, index)).await?;
                proof.check(
                    sent.topic == topic && sent.partition == partition,
                    "public Producer topic/partition receipt",
                )?;
                proof.check(
                    sent.offset == if name == "0" { -1 } else { index },
                    "public Producer actual positive-acks offset or acks0 no-offset",
                )?;
                proof.event(format!("{{\"label\":\"public-Producer\",\"acks\":{},\"topic\":{},\"partition\":{},\"expected_log_offset\":{},\"acknowledged_offset\":{}}}", quote(name), quote(&topic), partition, index, sent.offset))?;
            }
        }
        producer.flush_timeout(DEADLINE).await?;
        producer.close_timeout(DEADLINE).await?;
    }
    if restart {
        fs::write(
            state.join("rust-checkpoint"),
            b"offset12 after actual process restart",
        )
        .await?;
    }
    Ok(())
}

async fn saved_id(path: &Path) -> Result<[u8; 16]> {
    if fs::metadata(path).await?.len() != 32 {
        return Err("bounded UUID receipt requires32hex bytes".into());
    }
    let text = fs::read(path).await?;
    let mut result = [0; 16];
    for (byte, pair) in result.iter_mut().zip(text.chunks_exact(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(result)
}

async fn admin_lifecycle(
    bootstrap: &str,
    admin: &mut Admin,
    proof: &mut Proof,
    state: &Path,
    phase: &str,
) -> Result<()> {
    let topic = "ordinary-rust-lifecycle";
    let old_path = state.join("lifecycle-deleted-uuid");
    if phase == "seed" {
        let validation = admin
            .create_topics(
                &[NewTopic::new("ordinary-rust-validation", 1, 1)],
                1000,
                true,
            )
            .await?;
        proof.check(
            validation.first().is_some_and(|row| row.error_code == 0),
            "actual validate-only CreateTopics success",
        )?;
        let absent = admin.describe_topics(["ordinary-rust-validation"]).await?;
        proof.check(
            absent.first().is_some_and(|row| row.error_code == 3),
            "validate-only did not create a topic",
        )?;
        let created = admin
            .create_topics(&[NewTopic::new(topic, 1, 1)], 1000, false)
            .await?;
        proof.check(
            created.first().is_some_and(|row| row.error_code == 0),
            "actual lifecycle topic created",
        )?;
        let duplicate = admin
            .create_topics(&[NewTopic::new(topic, 1, 1)], 1000, false)
            .await?;
        proof.check(
            duplicate.first().is_some_and(|row| row.error_code == 36),
            "duplicate topic creation retains actual error36",
        )?;
        let old = admin.describe_topics([topic]).await?;
        let old_id = old.first().ok_or("old identity missing")?.topic_id;
        proof.check(old_id != [0; 16], "old lifecycle identity allocated")?;
        fs::write(&old_path, hex(&old_id)).await?;
        let old_producer = Producer::new(producer_config(bootstrap, Acks::Leader)).await?;
        let sent = old_producer
            .send(record(topic, 0, 0).value("deleted-old-identity"))
            .await?;
        proof.check(sent.offset == 0, "old namespace record appended")?;
        old_producer.close_timeout(DEADLINE).await?;
        let deleted = admin.delete_topics_by_id(&[old_id], 1000).await?;
        proof.check(
            deleted.first().is_some_and(|row| row.error_code == 0),
            "public DeleteTopics by UUID succeeds",
        )?;
        let missing = admin.delete_topics(&[topic], 1000).await?;
        proof.check(
            missing.first().is_some_and(|row| row.error_code == 3),
            "deleting absent name preserves error3",
        )?;
        let recreated = admin
            .create_topics(&[NewTopic::new(topic, 1, 1)], 1000, false)
            .await?;
        proof.check(
            recreated.first().is_some_and(|row| row.error_code == 0),
            "actual same-name recreation succeeds",
        )?;
        let new_id = identity(admin, proof, state, topic, 1, true).await?;
        proof.check(
            new_id != old_id,
            "same-name recreation gets a distinct UUID",
        )?;
    } else {
        let _id = identity(admin, proof, state, topic, 1, false).await?;
    }
    let old_id = saved_id(&old_path).await?;
    let missing = admin.describe_topics_by_id(&[old_id]).await?;
    proof.check(
        missing.first().is_some_and(|row| row.error_code == 100),
        "deleted UUID remains unknown before/after restart",
    )?;
    proof.event(format!(
        "{{\"label\":\"public-admin-deleted-identity\",\"uuid_hex\":{},\"error_code\":100}}",
        quote(&hex(&old_id))
    ))?;
    if phase == "seed" || phase == "restart" {
        let producer = Producer::new(producer_config(bootstrap, Acks::Leader)).await?;
        let index = i64::from(phase == "restart");
        let sent = producer.send(record(topic, 0, index)).await?;
        proof.check(
            sent.offset == index,
            "recreated namespace starts0 and resumes1 without old-record leakage",
        )?;
        producer.close_timeout(DEADLINE).await?;
    }
    Ok(())
}

fn matches_rich(row: &FetchedRecord, index: i64) -> bool {
    let expected = record(&row.topic, row.partition, index);
    row.timestamp == expected.timestamp.unwrap_or(-1)
        && row.key == expected.key
        && row.value == expected.value
        && row.headers == expected.headers
}

fn matches_raw(record: &FetchedRecord, prefix: &str, index: i64) -> bool {
    record.timestamp == TIME + index
        && record.key.as_deref() == Some(format!("raw-key:{index}").as_bytes())
        && record.value.as_deref() == Some(format!("raw-value:{index}").as_bytes())
        && record.headers == vec![Header::new("receipt", format!("{prefix}:raw:{index}"))]
}

async fn read_topic(
    consumer: &mut Consumer,
    proof: &mut Proof,
    topic: &str,
    partitions: i32,
    count: i64,
    raw_prefix: Option<&str>,
) -> Result<()> {
    proof.check(
        (1..=2).contains(&partitions) && (1..=33).contains(&count),
        "bounded expected topic history",
    )?;
    consumer
        .assign_many((0..partitions).map(|partition| ((topic.to_string(), partition), 0)))
        .await?;
    let mut next: BTreeMap<i32, i64> = (0..partitions).map(|partition| (partition, 0)).collect();
    let expected_total = i64::from(partitions) * count;
    let mut received = 0;
    let deadline = Instant::now() + Duration::from_secs(15);
    while received < expected_total && Instant::now() < deadline {
        let rows = consumer.fetch_timeout(DEADLINE).await?;
        proof.check(rows.count() <= 64, "bounded public fetch output")?;
        for row in &rows {
            let index = next
                .get_mut(&row.partition)
                .ok_or("unassigned partition returned")?;
            proof.check(
                row.topic == topic && row.offset == *index && *index < count,
                "public Consumer exact partition offset/order/no duplicate",
            )?;
            proof.check(
                row.timestamp_type == TimestampType::CreateTime,
                "ordinary CreateTime timestamp type",
            )?;
            proof.check(
                if let Some(prefix) = raw_prefix {
                    matches_raw(row, prefix, *index)
                } else {
                    matches_rich(row, *index)
                },
                "exact ID/key/value/null/empty/timestamp/duplicate header bytes",
            )?;
            proof.event(format!(
                "{{\"label\":\"public-Consumer\",\"receipt\":{}}}",
                receipt(row)
            ))?;
            proof.records += 1;
            *index += 1;
            received += 1;
        }
    }
    proof.check(
        received == expected_total,
        "public Consumer received every seeded ID",
    )?;
    for partition in 0..partitions {
        proof.check(
            consumer.list_offsets(topic, partition, -2).await? == 0,
            "actual earliest offset0",
        )?;
        proof.check(
            consumer.list_offsets(topic, partition, -1).await? == count,
            "actual latest offset equals complete history",
        )?;
        let timestamp =
            TIME + if raw_prefix.is_some() {
                0
            } else {
                i64::from(partition) * 100
            } + 5;
        proof.check(
            consumer.list_offsets(topic, partition, timestamp).await?
                == if count > 5 { 5 } else { -1 },
            "actual timestamp boundary offset5 or absent-1",
        )?;
    }
    Ok(())
}

async fn read_all(
    bootstrap: &str,
    admin: &mut Admin,
    proof: &mut Proof,
    state: &Path,
    seed: bool,
) -> Result<()> {
    let mut consumer =
        Consumer::new(consumer_config(bootstrap, IsolationLevel::ReadUncommitted)).await?;
    check_profile(proof, consumer.versions())?;
    for release in RELEASES {
        let prefix = format!("ordinary-{}", release.replace('.', "-"));
        for (acks, _) in ACKS {
            let topic = format!("{prefix}-acks{acks}");
            let partitions = if acks == "1" { 2 } else { 1 };
            let _id = identity(
                admin,
                proof,
                state,
                &topic,
                usize::try_from(partitions)?,
                seed,
            )
            .await?;
            read_topic(&mut consumer, proof, &topic, partitions, COUNT, None).await?;
        }
        let topic = format!("{prefix}-raw");
        let _id = identity(admin, proof, state, &topic, 1, seed).await?;
        read_topic(&mut consumer, proof, &topic, 1, 33, Some(&prefix)).await?;
    }
    let _id = identity(admin, proof, state, "ordinary-native", 2, seed).await?;
    read_topic(&mut consumer, proof, "ordinary-native", 2, COUNT, None).await?;
    let count = COUNT + i64::from(state.join("rust-checkpoint").exists());
    for (acks, _) in ACKS {
        let topic = format!("ordinary-rust-acks{acks}");
        let _id = identity(admin, proof, state, &topic, 2, false).await?;
        read_topic(&mut consumer, proof, &topic, 2, count, None).await?;
    }
    read_topic(
        &mut consumer,
        proof,
        "ordinary-rust-lifecycle",
        1,
        1 + i64::from(state.join("rust-checkpoint").exists()),
        None,
    )
    .await?;
    consumer.close_timeout(DEADLINE).await?;
    let mut committed =
        Consumer::new(consumer_config(bootstrap, IsolationLevel::ReadCommitted)).await?;
    check_profile(proof, committed.versions())?;
    read_topic(&mut committed, proof, "ordinary-rust-acks1", 2, count, None).await?;
    committed.close_timeout(DEADLINE).await?;
    Ok(())
}

async fn run(bootstrap: &str, phase: &str, state: &Path, proof: &mut Proof) -> Result<()> {
    let mut admin = Admin::new(
        AdminConfig::bootstrap([bootstrap])
            .request_timeout(DEADLINE)
            .connect_timeout(DEADLINE),
    )
    .await?;
    check_profile(proof, admin.versions())?;
    match admin
        .describe_configs(&[ConfigResource::topic("ordinary-native")], false)
        .await
    {
        Err(Error::Unsupported(message)) => proof.check(
            message.contains("DescribeConfigs"),
            "operation-local unsupported Admin configuration",
        )?,
        other => {
            return Err(
                format!("expected Unsupported for unadvertised DescribeConfigs: {other:?}").into(),
            )
        }
    }
    for transactional in [false, true] {
        let config = if transactional {
            producer_config(bootstrap, Acks::Leader)
                .transactional_id("unsupported-local-transaction")
        } else {
            producer_config(bootstrap, Acks::Leader).idempotent(true)
        };
        let expected = if transactional {
            "FindCoordinator"
        } else {
            "InitProducerId"
        };
        match Producer::new(config).await {
            Err(Error::Unsupported(message)) => proof.check(
                message.contains(expected),
                "required identity/transaction capability remains unsupported on the real broker",
            )?,
            Err(error) => {
                return Err(format!("expected Unsupported({expected}), received {error}").into())
            }
            Ok(producer) => {
                producer.close_timeout(DEADLINE).await?;
                return Err(format!("unadvertised {expected} unexpectedly succeeded").into());
            }
        }
        proof.event(format!("{{\"label\":\"public-Producer-required-capability\",\"capability\":{},\"outcome\":\"Unsupported\"}}", quote(expected)))?;
    }
    admin_lifecycle(bootstrap, &mut admin, proof, state, phase).await?;
    if phase == "seed" || phase == "restart" {
        write_rust(bootstrap, &mut admin, proof, state, phase == "restart").await?;
    }
    read_all(bootstrap, &mut admin, proof, state, phase == "seed").await?;
    admin.close_timeout(DEADLINE).await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        return Err("bootstrap seed|restart|read state-directory report-path bounded-tag".into());
    }
    let bootstrap = args.get(1).ok_or("bootstrap")?;
    let phase = args.get(2).ok_or("phase")?;
    let state = PathBuf::from(args.get(3).ok_or("state")?);
    let output = PathBuf::from(args.get(4).ok_or("report")?);
    let tag = args.get(5).ok_or("tag")?;
    if !matches!(phase.as_str(), "seed" | "restart" | "read")
        || tag.len() > 32
        || !tag
            .bytes()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == b'-')
    {
        return Err("invalid bounded peer phase/tag".into());
    }
    fs::create_dir_all(&state).await?;
    if output.exists() {
        return Err("peer report already exists; use a fresh attempt path".into());
    }
    let mut proof = Proof::default();
    let result = tokio::time::timeout(
        Duration::from_secs(90),
        run(bootstrap, phase, &state, &mut proof),
    )
    .await;
    let failure = match &result {
        Ok(Ok(())) => "null".into(),
        Ok(Err(error)) => quote(&error.to_string()),
        Err(error) => quote(&error.to_string()),
    };
    let report = format!("{{\"peer\":\"public-partitionline-client\",\"tag\":{},\"phase\":{},\"passed\":{},\"assertions\":{},\"records\":{},\"failure\":{},\"history\":[{}]}}\n", quote(tag), quote(phase), matches!(&result, Ok(Ok(()))), proof.assertions, proof.records, failure, proof.history.join(","));
    if report.len() > 1_048_576 {
        return Err("bounded report exceeds1MiB".into());
    }
    fs::write(output, report).await?;
    result?
}
