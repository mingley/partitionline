//! Genuine public Rust Producer/Admin/manual sparse Consumer compaction peer.

use partitionline::{
    Acks, Admin, AdminConfig, Compression, Consumer, ConsumerConfig, FetchedRecord, IsolationLevel,
    NewTopic, ProduceRecord, Producer, ProducerConfig, TimestampType,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    error::Error as StdError,
    path::PathBuf,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);
const SCENARIOS: [&str; 3] = ["mixed", "removed", "nulls"];
const ENDS: [i64; 3] = [9, 4, 3];
const TIMES: [&[i64]; 3] = [
    &[1000, 1007, 1003, 1007, 1010, 1011, 1012, 1013, 1014, 1015],
    &[1000, 1001, 1002, 1003, 1004],
    &[1000, 1001, 1002, 1003],
];
const KEYS: [&[Option<&str>]; 3] = [
    &[
        Some("a"),
        None,
        Some("a"),
        Some("b"),
        Some("b"),
        Some(""),
        None,
        Some("c"),
        Some("a"),
        Some("a"),
    ],
    &[Some("a"), Some("a"), Some("a"), Some("a"), Some("a")],
    &[None, None, Some("z"), Some("z")],
];
const VALUES: [&[Option<&str>]; 3] = [
    &[
        Some("o"),
        Some("v"),
        Some("n"),
        Some("o"),
        None,
        Some(""),
        None,
        None,
        Some("p"),
        Some("q"),
    ],
    &[Some("o"), Some("n"), None, Some("p"), Some("q")],
    &[Some("v"), None, Some("p"), Some("q")],
];

#[derive(Default)]
struct Proof {
    assertions: usize,
    records: usize,
    history: Vec<String>,
    identities: BTreeMap<String, String>,
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
        self.check(
            self.history.len() < 1024 && event.len() <= 4096,
            "bounded public receipt history",
        )?;
        self.history.push(event);
        Ok(())
    }
}
fn quote(text: &str) -> String {
    format!(
        "\"{}\"",
        text.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    )
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(
            *DIGITS.get(usize::from(byte >> 4)).unwrap_or(&b'?'),
        ));
        output.push(char::from(
            *DIGITS.get(usize::from(byte & 15)).unwrap_or(&b'?'),
        ));
    }
    output
}
fn optional_hex(bytes: Option<&[u8]>) -> String {
    bytes.map_or_else(|| "null".into(), |value| quote(&hex(value)))
}
fn name(prefix: &str, scenario: usize) -> Result<String> {
    Ok(format!(
        "{prefix}-{}",
        SCENARIOS.get(scenario).ok_or("scenario")?
    ))
}
fn fields(
    scenario: usize,
    offset: i64,
) -> Result<(i64, Option<&'static str>, Option<&'static str>)> {
    let offset = usize::try_from(offset)?;
    Ok((
        *TIMES
            .get(scenario)
            .and_then(|row| row.get(offset))
            .ok_or("timestamp")?,
        *KEYS
            .get(scenario)
            .and_then(|row| row.get(offset))
            .ok_or("key")?,
        *VALUES
            .get(scenario)
            .and_then(|row| row.get(offset))
            .ok_or("value")?,
    ))
}
fn receipt(record: &FetchedRecord) -> String {
    let headers: Vec<_> = record
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
    format!("{{\"topic\":{},\"partition\":{},\"offset\":{},\"timestamp\":{},\"key_hex\":{},\"value_hex\":{},\"headers\":[{}]}}",quote(&record.topic),record.partition,record.offset,record.timestamp,optional_hex(record.key.as_deref()),optional_hex(record.value.as_deref()),headers.join(","))
}
fn expected_receipt(topic: &str, scenario: usize, offset: i64) -> Result<String> {
    let (time, key, value) = fields(scenario, offset)?;
    Ok(format!("{{\"topic\":{},\"partition\":0,\"offset\":{},\"timestamp\":{},\"key_hex\":{},\"value_hex\":{},\"headers\":[{{\"key\":\"d\",\"value_hex\":\"61\"}},{{\"key\":\"d\",\"value_hex\":null}},{{\"key\":\"e\",\"value_hex\":\"\"}}]}}",quote(topic),offset,time,optional_hex(key.map(str::as_bytes)),optional_hex(value.map(str::as_bytes))))
}
async fn identities(bootstrap: &str, prefix: &str, proof: &mut Proof) -> Result<()> {
    let mut admin = Admin::new(
        AdminConfig::bootstrap([bootstrap])
            .request_timeout(DEADLINE)
            .connect_timeout(DEADLINE),
    )
    .await?;
    let names: Vec<_> = (0..3)
        .map(|scenario| name(prefix, scenario))
        .collect::<Result<_>>()?;
    let descriptions = admin.describe_topics(&names).await?;
    proof.check(descriptions.len() == 3, "public actual topic descriptions")?;
    for row in descriptions {
        proof.check(
            row.error_code == 0 && row.partitions.len() == 1 && row.topic_id != [0; 16],
            "actual topic partition/UUID",
        )?;
        let _old = proof.identities.insert(row.name, hex(&row.topic_id));
    }
    Ok(())
}
fn producer_config(bootstrap: &str) -> ProducerConfig {
    ProducerConfig::bootstrap([bootstrap])
        .acks(Acks::Leader)
        .idempotent(false)
        .compression(Compression::None)
        .connections(1)
        .max_in_flight(1)
        .buffer_memory(1 << 20)
        .max_request_size(65536)
        .batch_bytes(128)
        .batch_records(1)
        .linger(Duration::ZERO)
        .request_timeout(DEADLINE)
        .connect_timeout(DEADLINE)
        .delivery_timeout(Duration::from_secs(15))
        .max_block(DEADLINE)
}
async fn produce(bootstrap: &str, prefix: &str, seed: bool, proof: &mut Proof) -> Result<()> {
    if seed {
        let mut admin = Admin::new(
            AdminConfig::bootstrap([bootstrap])
                .request_timeout(DEADLINE)
                .connect_timeout(DEADLINE),
        )
        .await?;
        let topics: Vec<_> = (0..3)
            .map(|scenario| Ok(NewTopic::new(name(prefix, scenario)?, 1, 1)))
            .collect::<Result<_>>()?;
        let created = admin.create_topics(&topics, 5000, false).await?;
        proof.check(
            created.len() == 3 && created.iter().all(|row| row.error_code == 0),
            "genuine public Admin topic creation",
        )?;
    }
    identities(bootstrap, prefix, proof).await?;
    let producer = Producer::new(producer_config(bootstrap)).await?;
    for scenario in 0..3 {
        let topic = name(prefix, scenario)?;
        let last = *ENDS.get(scenario).ok_or("end")?;
        let (start, end) = if seed { (0, last) } else { (last, last + 1) };
        for offset in start..end {
            let (time, key, value) = fields(scenario, offset)?;
            let mut record = ProduceRecord::to(topic.as_str())
                .partition(0)
                .timestamp(time)
                .header("d", "a")
                .null_header("d")
                .header("e", "");
            if let Some(key) = key {
                record = record.key(key);
            }
            if let Some(value) = value {
                record = record.value(value);
            }
            let ack = producer.send(record).await?;
            producer.flush_timeout(DEADLINE).await?;
            proof.check(
                ack.topic == topic && ack.partition == 0 && ack.offset == offset,
                "actual public Producer topic/partition/offset",
            )?;
            proof.event(format!(
                "{{\"label\":\"public-producer-delivery\",\"record\":{}}}",
                expected_receipt(&topic, scenario, offset)?
            ))?;
            proof.records += 1;
        }
    }
    producer.close_timeout(DEADLINE).await?;
    Ok(())
}
fn retained(scenario: usize, offset: i64, stage: &str) -> bool {
    if stage == "initial" {
        return ENDS.get(scenario).is_some_and(|end| offset < *end);
    }
    let first = matches!(stage, "first" | "before-expiry");
    let appended = stage == "appended";
    match scenario {
        0 => {
            matches!(offset, 2 | 5 | 8)
                || (first && matches!(offset, 4 | 7))
                || (appended && offset == 9)
        }
        1 => offset == 3 || (first && offset == 2) || (appended && offset == 4),
        2 => offset == 2 || (appended && offset == 3),
        _ => false,
    }
}
async fn consume(bootstrap: &str, prefix: &str, stage: &str, proof: &mut Proof) -> Result<()> {
    identities(bootstrap, prefix, proof).await?;
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([bootstrap])
            .auto_commit(false)
            .allow_auto_create_topics(false)
            .isolation(IsolationLevel::ReadUncommitted)
            .fetch_max_bytes(65536)
            .max_partition_fetch_bytes(4096)
            .max_poll_records(64)
            .buffer_memory(1 << 20)
            .max_wait_ms(20)
            .request_timeout(DEADLINE)
            .connect_timeout(DEADLINE),
    )
    .await?;
    let actual: BTreeMap<_, _> = consumer
        .versions()
        .iter()
        .map(|(&key, v)| (key, (v.min_version, v.max_version)))
        .collect();
    proof.check(
        actual
            == BTreeMap::from([
                (0, (3, 13)),
                (1, (4, 6)),
                (2, (1, 3)),
                (3, (0, 13)),
                (18, (0, 4)),
                (19, (2, 4)),
                (20, (1, 6)),
            ]),
        "actual compaction preserves selectable seven-entry wire profile",
    )?;
    for scenario in 0..3 {
        let topic = name(prefix, scenario)?;
        let end = *ENDS.get(scenario).ok_or("end")? + i64::from(stage == "appended");
        proof.check(
            consumer.list_offsets(&topic, 0, -2).await? == 0,
            "compaction preserves public logical floor0",
        )?;
        proof.check(
            consumer.list_offsets(&topic, 0, -1).await? == end,
            "compaction preserves public LEO",
        )?;
        consumer.assign_many([((topic.clone(), 0), 0)]).await?;
        for start in [0, if scenario == 0 { 3 } else { 1 }, end] {
            consumer.seek(&topic, 0, start)?;
            let wanted: Vec<_> = (start..end)
                .filter(|offset| retained(scenario, *offset, stage))
                .collect();
            let mut received = 0;
            let deadline = Instant::now() + Duration::from_secs(10);
            while (received < wanted.len() || consumer.position(&topic, 0)? < end)
                && Instant::now() < deadline
            {
                let fetched = consumer.fetch_timeout(DEADLINE).await?;
                proof.check(fetched.count() <= 64, "bounded genuine public fetch")?;
                for record in &fetched {
                    let offset = *wanted.get(received).ok_or("extra public consumer record")?;
                    received += 1;
                    proof.check(
                        record.offset == offset && record.topic == topic && record.partition == 0,
                        "public seek skips sparse holes",
                    )?;
                    proof.check(
                        record.timestamp_type == TimestampType::CreateTime,
                        "preserved CreateTime",
                    )?;
                    let actual = receipt(record);
                    proof.check(
                        actual == expected_receipt(&topic, scenario, offset)?,
                        "exact key/null/empty value/time/ordered headers",
                    )?;
                    proof.event(format!("{{\"label\":\"public-consumer-record\",\"seek\":{},\"stage\":{},\"record_sha256\":{},\"record\":{}}}",start,quote(stage),quote(&hex(&Sha256::digest(actual.as_bytes()))),actual))?;
                    proof.records += 1;
                }
            }
            proof.check(
                received == wanted.len() && consumer.position(&topic, 0)? == end,
                "public consumer reaches unchanged LEO across sparse/empty batches",
            )?;
            proof.event(format!("{{\"label\":\"public-consumer-position\",\"topic\":{},\"seek\":{},\"position\":{},\"beginning_offset\":0,\"end_offset\":{}}}",quote(&topic),start,end,end))?;
        }
    }
    consumer.close_timeout(DEADLINE).await?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 6 {
        return Err("bootstrap prefix seed|read|append stage output-json".into());
    }
    let bootstrap = args.get(1).ok_or("bootstrap")?;
    let prefix = args.get(2).ok_or("prefix")?;
    let operation = args.get(3).ok_or("operation")?;
    let stage = args.get(4).ok_or("stage")?;
    let output = PathBuf::from(args.get(5).ok_or("output")?);
    if bootstrap.len() > 128
        || prefix.is_empty()
        || prefix.len() > 40
        || !prefix
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || !matches!(
            stage.as_str(),
            "initial" | "first" | "before-expiry" | "expired" | "restart" | "appended"
        )
    {
        return Err("bounded trusted peer arguments".into());
    }
    let mut proof = Proof::default();
    let result = match operation.as_str() {
        "seed" => produce(bootstrap, prefix, true, &mut proof).await,
        "append" => produce(bootstrap, prefix, false, &mut proof).await,
        "read" => consume(bootstrap, prefix, stage, &mut proof).await,
        _ => Err("operation".into()),
    };
    let failure = result
        .as_ref()
        .err()
        .map_or_else(|| "null".into(), |error| quote(&error.to_string()));
    let ids: Vec<_> = proof
        .identities
        .iter()
        .map(|(name, id)| format!("{}:{}", quote(name), quote(id)))
        .collect();
    let json = format!("{{\"schema_version\":1,\"peer\":\"public-rust\",\"release\":\"partitionline\",\"operation\":{},\"stage\":{},\"prefix\":{},\"assertions\":{},\"records\":{},\"identities\":{{{}}},\"history\":[{}],\"failure\":{},\"passed\":{}}}\n",quote(operation),quote(stage),quote(prefix),proof.assertions,proof.records,ids.join(","),proof.history.join(","),failure,result.is_ok());
    if json.len() > 1024 * 1024 {
        return Err("report bound".into());
    }
    tokio::fs::write(output, json).await?;
    result
}
