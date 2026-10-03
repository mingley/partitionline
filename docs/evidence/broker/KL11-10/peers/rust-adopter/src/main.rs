//! Public Rust Admin and manual Consumer reads of Apache-written retained batches.

use partitionline::{
    Admin, AdminConfig, Consumer, ConsumerConfig, FetchedRecord, Header, IsolationLevel,
    TimestampType,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    error::Error as StdError,
    path::PathBuf,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;
const TIMES: [i64; 8] = [1000, 1007, 1003, 1007, 1010, 1011, 1012, 1013];
const DEADLINE: Duration = Duration::from_secs(5);
#[derive(Default)]
struct Proof {
    assertions: usize,
    records: usize,
    history: Vec<String>,
}
impl Proof {
    fn check(&mut self, condition: bool, label: &str) -> Result<()> {
        self.assertions += 1;
        if condition {
            Ok(())
        } else {
            Err(format!("assertion failed: {label}").into())
        }
    }
    fn event(&mut self, event: String) -> Result<()> {
        self.check(self.history.len() < 256, "bounded history")?;
        self.history.push(event);
        Ok(())
    }
}
fn quote(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(char::from(
            *DIGITS.get(usize::from(byte >> 4)).unwrap_or(&b'?'),
        ));
        out.push(char::from(
            *DIGITS.get(usize::from(byte & 15)).unwrap_or(&b'?'),
        ));
    }
    out
}
fn optional_hex(bytes: Option<&[u8]>) -> String {
    bytes.map_or_else(|| "null".into(), |b| quote(&hex(b)))
}
fn receipt(row: &FetchedRecord) -> String {
    let headers: Vec<_> = row
        .headers
        .iter()
        .map(|h| {
            format!(
                "{{\"key\":{},\"value_hex\":{}}}",
                quote(&h.key),
                optional_hex(h.value.as_deref())
            )
        })
        .collect();
    let json = format!("{{\"topic\":{},\"partition\":{},\"offset\":{},\"timestamp\":{},\"key_hex\":{},\"value_hex\":{},\"headers\":[{}]}}",
        quote(&row.topic),row.partition,row.offset,row.timestamp,optional_hex(row.key.as_deref()),optional_hex(row.value.as_deref()),headers.join(","));
    format!(
        "{{\"sha256\":{},\"record\":{json}}}",
        quote(&hex(&Sha256::digest(json.as_bytes())))
    )
}
fn expected(row: &FetchedRecord) -> Result<bool> {
    let index = usize::try_from(row.offset)?;
    let time = TIMES.get(index).ok_or("bounded record offset")?;
    let key = (index % 3 != 0).then(|| format!("key:{}:{index}", row.topic).into_bytes());
    let value = (index % 3 != 0).then(|| {
        if index % 3 == 1 {
            Vec::new()
        } else {
            format!("value:{}:{index}", row.topic).into_bytes()
        }
    });
    let headers = vec![
        Header::new("receipt", format!("{}:{index}", row.topic)),
        Header::new("dup", "a"),
        Header::null("dup"),
    ];
    Ok(row.timestamp == *time
        && row.timestamp_type == TimestampType::CreateTime
        && row.key.as_deref() == key.as_deref()
        && row.value.as_deref() == value.as_deref()
        && row.headers == headers)
}
async fn run(bootstrap: &str, topic: &str, phase: &str, end: i64, proof: &mut Proof) -> Result<()> {
    let mut admin = Admin::new(
        AdminConfig::bootstrap([bootstrap])
            .request_timeout(DEADLINE)
            .connect_timeout(DEADLINE),
    )
    .await?;
    for (offset, low, error) in [(-2, -1, 1), (end + 1, -1, 1), (3, 3, 0), (1, 3, 0)] {
        let result = admin.delete_records((topic, 0), offset, 5000).await?;
        proof.check(
            result.low_watermark() == low && result.error_code() == error,
            "actual public Admin DeleteRecords low watermark/error",
        )?;
        proof.event(format!("{{\"label\":\"public-delete\",\"offset\":{offset},\"low_watermark\":{},\"error_code\":{}}}",result.low_watermark(),result.error_code()))?;
    }
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
                (21, (0, 2)),
            ]),
        "actual eight-entry public-client profile",
    )?;
    proof.event(format!(
        "{{\"label\":\"public-profile\",\"ranges\":{}}}",
        quote(&format!("{actual:?}"))
    ))?;
    consumer.assign_many([((topic.to_owned(), 0), 3)]).await?;
    let mut next = 3;
    let deadline = Instant::now() + Duration::from_secs(15);
    while next < end && Instant::now() < deadline {
        let rows = consumer.fetch_timeout(DEADLINE).await?;
        proof.check(rows.count() <= 64, "bounded public fetch")?;
        for row in &rows {
            proof.check(
                row.topic == topic && row.partition == 0 && row.offset == next && next < end,
                "public Consumer filters containing-batch prefix with no duplicate/leak",
            )?;
            proof.check(
                expected(row)?,
                "cross-language exact bytes/null/empty/CreateTime/ordered duplicate headers",
            )?;
            proof.event(format!(
                "{{\"label\":\"retained-public-consumer\",\"receipt\":{}}}",
                receipt(row)
            ))?;
            proof.records += 1;
            next += 1;
        }
    }
    proof.check(next == end, "all retained records consumed")?;
    for (timestamp, offset) in [(-2, 3), (-1, end), (1004, 3), (1014, -1)] {
        proof.check(
            consumer.list_offsets(topic, 0, timestamp).await? == offset,
            "floor-aware public ListOffsets",
        )?;
    }
    consumer.close_timeout(DEADLINE).await?;
    proof.event(format!(
        "{{\"label\":\"phase-complete\",\"phase\":{},\"floor\":3,\"end\":{end}}}",
        quote(phase)
    ))?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 6 {
        return Err("bootstrap topic seed|restart end output-json".into());
    }
    let bootstrap = args.get(1).ok_or("bootstrap")?;
    let topic = args.get(2).ok_or("topic")?;
    let phase = args.get(3).ok_or("phase")?;
    let end: i64 = args.get(4).ok_or("end")?.parse()?;
    let output = PathBuf::from(args.get(5).ok_or("output")?);
    if topic.is_empty()
        || topic.len() > 64
        || !topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        || !matches!(phase.as_str(), "seed" | "restart")
        || !matches!(end, 7 | 8)
    {
        return Err("bounded peer arguments".into());
    }
    let mut proof = Proof::default();
    let result = run(bootstrap, topic, phase, end, &mut proof).await;
    let failure = result
        .as_ref()
        .err()
        .map(|e| quote(&e.to_string()))
        .unwrap_or_else(|| "null".into());
    let json = format!("{{\"passed\":{},\"phase\":{},\"topic\":{},\"assertions\":{},\"records\":{},\"failure\":{failure},\"history\":[{}]}}\n",
        result.is_ok(),quote(phase),quote(topic),proof.assertions,proof.records,proof.history.join(","));
    if json.len() > 1048576 {
        return Err("report bound".into());
    }
    tokio::fs::write(output, json).await?;
    result
}
