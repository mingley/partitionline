//! One consume-transform-produce crash or fencing case (KL03-20).
//!
//! `TXN_CRASH_PHASE` is `seed`, `run`, or `verify`. `TXN_CRASH_CASE` selects
//! the frozen case. Broker on `KAFKA_BOOTSTRAP`. Transaction timeout is
//! `TXN_TIMEOUT_MS` and is not a leader-recovery timer. External side
//! effects are out of scope.

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use partitionline::{
    error, Admin, AdminConfig, AutoOffsetReset, Consumer, ConsumerConfig, ConsumerGroup,
    ConsumerRecords, Error, FetchedRecord, IsolationLevel, NewTopic, ProduceRecord, Producer,
    ProducerConfig, TopicPartition,
};

const CRASH_EXIT: i32 = 77;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let phase = env::var("TXN_CRASH_PHASE").unwrap_or_else(|_| "run".into());
    match phase.as_str() {
        "seed" => seed().await?,
        "run" => run_case().await?,
        "verify" => verify().await?,
        other => return Err(Error::protocol(format!("unknown TXN_CRASH_PHASE {other}")).into()),
    }
    Ok(())
}

#[derive(Clone)]
struct Names {
    bootstrap: String,
    input: String,
    output: String,
    group: String,
    txn_id: String,
    case: String,
    crash: Option<String>,
}

impl Names {
    fn from_env() -> Self {
        Self {
            bootstrap: env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:19092".into()),
            input: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "pl-txn-in".into()),
            output: env::var("KAFKA_OUTPUT_TOPIC").unwrap_or_else(|_| "pl-txn-out".into()),
            group: env::var("KAFKA_GROUP").unwrap_or_else(|_| "pl-txn-eos".into()),
            txn_id: env::var("KAFKA_TRANSACTIONAL_ID").unwrap_or_else(|_| "pl-txn-eos".into()),
            case: env::var("TXN_CRASH_CASE").unwrap_or_else(|_| "happy_commit".into()),
            crash: env::var("TXN_CRASH_AT").ok().filter(|s| !s.is_empty()),
        }
    }

    fn servers(&self) -> impl Iterator<Item = &str> {
        self.bootstrap.split(',').map(str::trim)
    }
}

fn env_u64(name: &str, default: u64) -> partitionline::Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| Error::protocol(format!("invalid {name}"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(Error::protocol(format!("invalid {name}"))),
    }
}

fn coordinator_starting(error: &Error) -> bool {
    matches!(
        error.broker_code(),
        Some(
            error::COORDINATOR_NOT_AVAILABLE
                | error::COORDINATOR_LOAD_IN_PROGRESS
                | error::NOT_COORDINATOR
                | error::CONCURRENT_TRANSACTIONS
                | error::INVALID_REPLICATION_FACTOR
                | error::LEADER_NOT_AVAILABLE
                | error::NOT_LEADER_OR_FOLLOWER
                | error::NOT_ENOUGH_REPLICAS
                | error::NOT_ENOUGH_REPLICAS_AFTER_APPEND
        )
    )
}

async fn retry<T, F, Fut>(timeout: Duration, mut body: F) -> partitionline::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = partitionline::Result<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        match body().await {
            Ok(value) => return Ok(value),
            Err(error) if coordinator_starting(&error) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn ensure_topics(names: &Names) -> partitionline::Result<()> {
    retry(Duration::from_secs(30), || {
        let names = names.clone();
        async move {
            let mut admin = Admin::new(AdminConfig::bootstrap(names.servers())).await?;
            for topic in [&names.input, &names.output] {
                let created = admin
                    .create_topics(
                        &[NewTopic::new(topic.clone(), 1, 3).config("min.insync.replicas", "2")],
                        30_000,
                        false,
                    )
                    .await?;
                for result in created {
                    if result.error_code != 0 && result.error_code != 36 {
                        return Err(Error::broker(result.error_code, result.name));
                    }
                }
            }
            admin.close().await?;
            Ok(())
        }
    })
    .await
}

async fn connect_producer(names: &Names, transactional: bool) -> partitionline::Result<Producer> {
    let txn_ms = env_u64("TXN_TIMEOUT_MS", 15_000)?;
    let req_ms = env_u64("TXN_REQUEST_MS", 8_000)?;
    let mut cfg = ProducerConfig::bootstrap(names.servers())
        .acks(partitionline::Acks::All)
        .idempotent(true)
        .linger(Duration::ZERO)
        .request_timeout(Duration::from_millis(req_ms))
        .delivery_timeout(Duration::from_millis(req_ms.saturating_add(4_000)))
        .max_block(Duration::from_millis(req_ms));
    if transactional {
        cfg = cfg
            .transactional_id(names.txn_id.clone())
            .transaction_timeout(Duration::from_millis(txn_ms));
    }
    retry(Duration::from_secs(20), || {
        let cfg = cfg.clone();
        async move { Producer::new(cfg).await }
    })
    .await
}

async fn seed() -> partitionline::Result<()> {
    let names = Names::from_env();
    ensure_topics(&names).await?;
    let producer = connect_producer(&names, false).await?;
    retry(Duration::from_secs(20), || {
        let producer = &producer;
        let output = names.output.clone();
        async move {
            drop(
                producer
                    .send(
                        ProduceRecord::to(output)
                            .partition(0)
                            .key(b"anchor".to_vec())
                            .value(b"anchor".to_vec()),
                    )
                    .await?,
            );
            Ok(())
        }
    })
    .await?;
    let count = env_u64("TXN_INPUT_RECORDS", 2)?;
    for i in 0..count {
        let id = format!("in-{i}");
        retry(Duration::from_secs(20), || {
            let producer = &producer;
            let input = names.input.clone();
            let id = id.clone();
            async move {
                drop(
                    producer
                        .send(
                            ProduceRecord::to(input)
                                .partition(0)
                                .key(id.as_bytes().to_vec())
                                .value(id.as_bytes().to_vec()),
                        )
                        .await?,
                );
                Ok(())
            }
        })
        .await?;
    }
    producer.close().await?;
    Ok(())
}

fn maybe_crash(names: &Names, at: &str) {
    if names.crash.as_deref() == Some(at) {
        std::process::exit(CRASH_EXIT);
    }
}

async fn signal_ready() -> partitionline::Result<()> {
    if let Some(path) = env::var_os("TXN_CRASH_READY") {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::protocol(e.to_string()))?;
        }
        tokio::fs::write(path, b"ready\n")
            .await
            .map_err(|e| Error::protocol(e.to_string()))?;
    }
    Ok(())
}

fn rec_text(bytes: Option<&[u8]>) -> String {
    bytes
        .and_then(|v| std::str::from_utf8(v).ok())
        .unwrap_or("missing")
        .to_string()
}

async fn poll_input(
    group: &mut ConsumerGroup,
    need: usize,
) -> partitionline::Result<ConsumerRecords> {
    let mut collected: Vec<FetchedRecord> = Vec::new();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let recs = group.poll_timeout(Duration::from_secs(2)).await?;
            collected.extend(recs.iter().cloned());
            if collected.len() >= need {
                return Ok::<(), Error>(());
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)??;
    Ok(ConsumerRecords::from(collected))
}

async fn run_case() -> partitionline::Result<()> {
    let names = Names::from_env();
    if names.case == "fence_replacement" {
        return run_fence(&names).await;
    }
    let producer = connect_producer(&names, true).await?;
    producer.init_transactions().await?;
    let consumer_cfg = ConsumerConfig::bootstrap(names.servers())
        .isolation(IsolationLevel::ReadCommitted)
        .auto_commit(false)
        .max_poll_records(16)
        .max_wait_ms(250)
        .request_timeout(Duration::from_secs(8));
    let mut group = retry(Duration::from_secs(20), || {
        let cfg = consumer_cfg.clone();
        let group_id = names.group.clone();
        let topic = names.input.clone();
        async move { ConsumerGroup::join_topics(cfg, group_id, [topic]).await }
    })
    .await?;

    let need = usize::try_from(env_u64("TXN_INPUT_RECORDS", 2)?).unwrap_or(2);
    let recs = poll_input(&mut group, need).await?;

    producer.begin_transaction().await?;
    for rec in &recs {
        let id = rec_text(rec.value.as_deref());
        let payload = format!("out-{id}");
        let sent = ProduceRecord::to(names.output.clone())
            .partition(0)
            .key(id.as_bytes().to_vec())
            .value(payload.as_bytes().to_vec());
        if names.crash.as_deref() == Some("before_produce_ack") {
            drop(tokio::time::timeout(Duration::from_millis(1), producer.send(sent)).await);
            signal_ready().await?;
            std::process::exit(CRASH_EXIT);
        }
        drop(producer.send(sent).await?);
    }
    maybe_crash(&names, "after_produce_ack");
    producer
        .send_offsets_for_group(&group.group_metadata(), recs.next_offsets())
        .await?;
    maybe_crash(&names, "after_offset_stage");
    maybe_crash(&names, "before_endtxn");
    if names.case == "abort_hides" {
        producer.abort_transaction().await?;
    } else if names.crash.as_deref() == Some("during_endtxn") {
        signal_ready().await?;
        drop(tokio::time::timeout(Duration::from_millis(30), producer.commit_transaction()).await);
        std::process::exit(CRASH_EXIT);
    } else {
        producer.commit_transaction().await?;
    }
    group.leave().await?;
    producer.close().await?;
    Ok(())
}

async fn run_fence(names: &Names) -> partitionline::Result<()> {
    let first = connect_producer(names, true).await?;
    first.init_transactions().await?;
    first.begin_transaction().await?;
    drop(
        first
            .send(
                ProduceRecord::to(names.output.clone())
                    .partition(0)
                    .key(b"fenced".to_vec())
                    .value(b"out-fenced".to_vec()),
            )
            .await?,
    );
    let replacement = connect_producer(names, true).await?;
    replacement.init_transactions().await?;
    let fenced = first
        .send(
            ProduceRecord::to(names.output.clone())
                .partition(0)
                .key(b"fenced-2".to_vec())
                .value(b"out-fenced-2".to_vec()),
        )
        .await;
    match fenced {
        Err(error)
            if matches!(
                error.broker_code(),
                Some(error::PRODUCER_FENCED | error::INVALID_PRODUCER_EPOCH)
            ) =>
        {
            eprintln!(
                "stale producer fenced: {}",
                error.broker_code().unwrap_or(-1)
            );
        }
        Err(error) => return Err(error),
        Ok(_) => return Err(Error::protocol("stale producer was not fenced")),
    }
    replacement.begin_transaction().await?;
    drop(
        replacement
            .send(
                ProduceRecord::to(names.output.clone())
                    .partition(0)
                    .key(b"fence-out".to_vec())
                    .value(b"fence-out".to_vec()),
            )
            .await?,
    );
    replacement.commit_transaction().await?;
    drop(first.close().await);
    replacement.close().await?;
    Ok(())
}

async fn committed_input_offset(names: &Names) -> partitionline::Result<i64> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut admin = Admin::new(AdminConfig::bootstrap(names.servers())).await?;
        let result = admin
            .list_consumer_group_offsets(
                &names.group,
                [TopicPartition::new(names.input.clone(), 0)],
            )
            .await;
        drop(admin.close().await);
        match result {
            Ok(rows) => {
                return Ok(rows
                    .iter()
                    .find(|(tp, _)| tp.topic == names.input && tp.partition == 0)
                    .map(|(_, md)| md.offset)
                    .unwrap_or(-1));
            }
            Err(error) if coordinator_starting(&error) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(_) => return Ok(-1),
        }
    }
}

async fn verify() -> partitionline::Result<()> {
    let names = Names::from_env();
    let mut ccfg = ConsumerConfig::bootstrap(names.servers())
        .isolation(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .request_timeout(Duration::from_secs(8));
    ccfg.max_wait_ms = 500;
    let mut consumer = Consumer::new(ccfg).await?;
    consumer
        .assign_partitions([TopicPartition::new(names.output.clone(), 0)])
        .await?;
    let wait = Duration::from_millis(env_u64("TXN_VERIFY_MS", 4_000)?);
    let deadline = Instant::now() + wait;
    let mut consumed = Vec::new();
    while Instant::now() < deadline {
        match consumer.fetch_timeout(Duration::from_millis(400)).await {
            Ok(recs) => {
                for rec in recs.iter() {
                    consumed.push((
                        rec_text(rec.key.as_deref()),
                        rec_text(rec.value.as_deref()),
                        rec.offset,
                    ));
                }
            }
            Err(Error::Timeout) => {}
            Err(error) if error.is_retriable() => {}
            Err(error) => return Err(error),
        }
    }
    let committed_offset = committed_input_offset(&names).await?;
    let history_path = env::var_os("TXN_CRASH_HISTORY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("txn-crash-history.json"));
    if let Some(parent) = history_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Error::protocol(e.to_string()))?;
    }
    tokio::fs::write(
        &history_path,
        render_verify(&names, &consumed, committed_offset),
    )
    .await
    .map_err(|e| Error::protocol(e.to_string()))?;
    Ok(())
}

fn json_escape(s: &str) -> String {
    let mut out = String::new();
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn render_verify(
    names: &Names,
    consumed: &[(String, String, i64)],
    committed_offset: i64,
) -> String {
    let mut recs = String::new();
    for (i, (key, payload, offset)) in consumed.iter().enumerate() {
        if i > 0 {
            recs.push(',');
        }
        recs.push_str("{\"id\":");
        recs.push_str(&json_escape(payload));
        recs.push_str(",\"topic\":");
        recs.push_str(&json_escape(&names.output));
        recs.push_str(",\"partition\":0,\"offset\":");
        recs.push_str(&offset.to_string());
        recs.push_str(",\"key\":");
        recs.push_str(&json_escape(key));
        recs.push_str(",\"payload\":");
        recs.push_str(&json_escape(payload));
        recs.push_str(",\"status\":\"acked\"}");
    }
    format!(
        "{{\"id\":{},\"case\":{},\"input_topic\":{},\"committed_offset\":{},\"consumed\":[{}]}}\n",
        json_escape(&names.case),
        json_escape(&names.case),
        json_escape(&names.input),
        committed_offset,
        recs
    )
}
