use futures_executor::block_on;
use partitionline_rust_rdkafka_peer::{config::Config, native, records};
use rdkafka::{
    admin::{AdminClient, AdminOptions, ResourceSpecifier},
    client::ClientContext,
    consumer::{BaseConsumer, Consumer as KafkaConsumer},
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header, Headers, Message, OwnedHeaders},
    producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext, PurgeConfig},
    topic_partition_list::{Offset, TopicPartitionList},
};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

type Failure = Box<dyn std::error::Error>;

#[derive(Default, Serialize)]
struct Counts {
    offered: u64,
    accepted: u64,
    acknowledged: u64,
    rejected: u64,
    timed_out: u64,
    unknown: u64,
    callback_failures: u64,
    queue_full_retries: u64,
    elapsed_s: f64,
    errors: Vec<Value>,
}

struct Phase {
    counts: Counts,
    errors: BTreeMap<String, u64>,
    samples: Option<BufWriter<File>>,
    history: Option<BufWriter<File>>,
    io_error: bool,
    completed: u64,
    acks: i32,
    sample_limit: u64,
}

impl Phase {
    fn error(&mut self, error: &KafkaError) {
        *self.errors.entry(error.to_string()).or_default() += 1;
    }

    fn receipt(&mut self, opaque: &Delivery, status: &str, offset: Option<i64>) {
        if let Some(history) = self.history.as_mut() {
            let record = json!({"id":opaque.id.to_string(),"attempt_index":opaque.id,
                "topic":opaque.topic,"partition":opaque.partition,"key":opaque.key,
                "payload_hash":opaque.payload_hash,"status":status,"offset":offset});
            if serde_json::to_writer(&mut *history, &record).is_err() || writeln!(history).is_err()
            {
                self.io_error = true;
            }
        }
    }

    fn finish(&mut self) -> Result<(), Failure> {
        if let Some(history) = self.history.as_mut() {
            history.flush()?;
        }
        if let Some(samples) = self.samples.as_mut() {
            samples.flush()?;
        }
        self.counts.errors = self
            .errors
            .iter()
            .map(|(name, count)| json!({"code":name,"name":name,"count":count}))
            .collect();
        if self.io_error {
            return Err("delivery artifacts could not be written".into());
        }
        Ok(())
    }
}

struct Delivery {
    phase: Arc<Mutex<Phase>>,
    id: u64,
    partition: i32,
    topic: String,
    key: Option<String>,
    payload_hash: String,
    started: Instant,
}

struct Context;
impl ClientContext for Context {}
impl ProducerContext for Context {
    type DeliveryOpaque = Box<Delivery>;

    fn delivery(&self, result: &DeliveryResult<'_>, opaque: Self::DeliveryOpaque) {
        let mut phase = opaque.phase.lock().expect("phase lock poisoned");
        phase.completed += 1;
        let (status, offset) = match result {
            Ok(message) if phase.acks != 0 => {
                phase.counts.acknowledged += 1;
                if opaque.id < phase.sample_limit {
                    if let Some(samples) = phase.samples.as_mut() {
                        if writeln!(
                            samples,
                            "{},{:.3}",
                            opaque.id,
                            opaque.started.elapsed().as_secs_f64() * 1e6
                        )
                        .is_err()
                        {
                            phase.io_error = true;
                        }
                    }
                }
                ("acked", Some(message.offset()))
            }
            Ok(_) => {
                // Local successful delivery without broker acknowledgement stays unknown.
                phase.counts.unknown += 1;
                ("ambiguous", None)
            }
            Err((error, _)) => {
                phase.counts.callback_failures += 1;
                if error.rdkafka_error_code() == Some(RDKafkaErrorCode::MessageTimedOut) {
                    phase.counts.timed_out += 1;
                } else {
                    phase.counts.unknown += 1;
                }
                phase.error(error);
                ("ambiguous", None)
            }
        };
        phase.receipt(&opaque, status, offset);
    }
}

fn create(path: &str) -> Result<BufWriter<File>, Failure> {
    Ok(BufWriter::new(
        OpenOptions::new().write(true).create_new(true).open(path)?,
    ))
}

fn phase(config: &Config, timed: bool) -> Result<Arc<Mutex<Phase>>, Failure> {
    let samples = if timed {
        let mut file = create(&std::env::var("RUST_PEER_SAMPLES")?)?;
        writeln!(file, "record_id,latency_us")?;
        Some(file)
    } else {
        None
    };
    let history = if timed {
        Some(create(&std::env::var("RUST_PEER_HISTORY")?)?)
    } else {
        None
    };
    Ok(Arc::new(Mutex::new(Phase {
        counts: Counts::default(),
        errors: BTreeMap::new(),
        samples,
        history,
        io_error: false,
        completed: 0,
        acks: config.acks,
        sample_limit: config.latency_samples,
    })))
}

fn produce(
    producer: &BaseProducer<Context>,
    config: &Config,
    count: u64,
    phase: &Arc<Mutex<Phase>>,
) -> bool {
    let began = Instant::now();
    let deadline = began + Duration::from_millis(config.run_timeout_ms);
    for id in 0..count {
        if Instant::now() >= deadline {
            break;
        }
        if config.target_rate_per_s > 0 {
            let intended_ns =
                u128::from(id).saturating_mul(1_000_000_000) / u128::from(config.target_rate_per_s);
            let intended =
                began + Duration::from_nanos(u64::try_from(intended_ns).unwrap_or(u64::MAX));
            while Instant::now() < intended && Instant::now() < deadline {
                producer.poll(Duration::from_millis(1));
            }
        }
        phase.lock().expect("phase lock").counts.offered += 1;
        let key = records::key(config.record_seed, id);
        let value = if config.payload_mode == "json1k" {
            records::json1k_value(config.record_seed, id)
        } else {
            records::value(
                config.record_seed,
                id,
                config.payload_bytes,
                config.payload_mode == "seeded",
            )
        };
        let header = records::gw8_header(config.record_seed, id);
        let headers = (config.header_mode == "gw8").then(|| {
            OwnedHeaders::new().insert(Header {
                key: "gw",
                value: Some(header.as_slice()),
            })
        });
        let partition = (id % config.partitions as u64) as i32;
        let opaque = Box::new(Delivery {
            phase: phase.clone(),
            id,
            partition,
            topic: config.topic.clone(),
            key: (config.key_mode == "id").then(|| records::hex(&key)),
            payload_hash: format!("{:x}", Sha256::digest(&value)),
            started: Instant::now(),
        });
        let mut record = BaseRecord::with_opaque_to(&config.topic, opaque)
            .payload(&value)
            .partition(partition);
        if config.key_mode == "id" {
            record = record.key(&key);
        }
        if let Some(headers) = headers {
            record = record.headers(headers);
        }
        loop {
            match producer.send(record) {
                Ok(()) => {
                    phase.lock().expect("phase lock").counts.accepted += 1;
                    break;
                }
                Err((error, returned)) => {
                    if error.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull)
                        && Instant::now() < deadline
                    {
                        phase.lock().expect("phase lock").counts.queue_full_retries += 1;
                        producer.poll(Duration::from_millis(10));
                        record = returned;
                        continue;
                    }
                    let mut state = phase.lock().expect("phase lock");
                    state.counts.rejected += 1;
                    state.error(&error);
                    state.receipt(&returned.delivery_opaque, "failed", None);
                    break;
                }
            }
        }
        producer.poll(Duration::ZERO);
        if phase.lock().expect("phase lock").counts.rejected != 0 {
            break;
        }
    }
    let flush = producer.flush(Duration::from_millis(config.flush_timeout_ms));
    if let Err(error) = &flush {
        phase.lock().expect("phase lock").error(error);
        producer.purge(PurgeConfig::default().queue().inflight());
        let _ = producer.flush(Duration::from_secs(5));
    }
    let mut state = phase.lock().expect("phase lock");
    state.counts.elapsed_s = began.elapsed().as_secs_f64().max(f64::EPSILON);
    let missing = state.counts.accepted.saturating_sub(state.completed);
    state.counts.unknown += missing;
    state.counts.acknowledged == count
        && state.counts.callback_failures == 0
        && flush.is_ok()
        && !state.io_error
}

fn inspect(
    producer: &BaseProducer<Context>,
    config: &Config,
) -> Result<(Value, String, usize), Failure> {
    let metadata = producer
        .client()
        .fetch_metadata(Some(&config.topic), Duration::from_secs(10))?;
    let topic = metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == config.topic)
        .ok_or("topic missing")?;
    if topic.error().is_some() || topic.partitions().len() != config.partitions as usize {
        return Err("topic partition metadata differs from requested configuration".into());
    }
    let rf = topic.partitions()[0].replicas().len();
    if rf == 0
        || topic
            .partitions()
            .iter()
            .any(|p| p.error().is_some() || p.replicas().len() != rf)
    {
        return Err("topic metadata has invalid/nonuniform replication".into());
    }
    let admin: AdminClient<_> = config.client(true).create()?;
    let resources = block_on(admin.describe_configs(
        &[ResourceSpecifier::Topic(&config.topic)],
        &AdminOptions::new().request_timeout(Some(Duration::from_secs(10))),
    ))?;
    let isr: u64 = resources
        .into_iter()
        .next()
        .ok_or("no topic config")??
        .get("min.insync.replicas")
        .and_then(|entry| entry.value.as_deref())
        .ok_or("missing min ISR")?
        .parse()?;
    if isr == 0 {
        return Err("nonpositive min ISR".into());
    }
    let cluster_id = producer
        .client()
        .fetch_cluster_id(Duration::from_secs(10))
        .ok_or("missing cluster ID")?;
    Ok((
        json!({"verified":true,"replication_factor":rf,"min_insync_replicas":isr}),
        cluster_id,
        metadata.brokers().len(),
    ))
}

fn watermarks(producer: &BaseProducer<Context>, config: &Config) -> Result<Vec<i64>, Failure> {
    (0..config.partitions)
        .map(|p| {
            producer
                .client()
                .fetch_watermarks(&config.topic, p, Duration::from_secs(10))
                .map(|(_, high)| high)
                .map_err(Into::into)
        })
        .collect()
}

fn consume(config: &Config) -> Result<bool, Failure> {
    if config.assignment_offsets.len() != config.partitions as usize {
        return Err("consume requires assignment_offsets for every partition".into());
    }
    let consumer: BaseConsumer = config.client(true).create()?;
    let effective = native::consumer_effective(consumer.client())?;
    let mut assignment = TopicPartitionList::new();
    for (partition, offset) in config.assignment_offsets.iter().enumerate() {
        assignment.add_partition_offset(
            &config.topic,
            i32::try_from(partition).map_err(|_| "partition index")?,
            Offset::Offset(*offset),
        )?;
    }
    consumer.assign(&assignment)?;
    let history_path = std::env::var("RUST_PEER_HISTORY")?;
    let mut history = create(&history_path)?;
    let mut seen = std::collections::BTreeSet::new();
    let mut verified = 0u64;
    let mut duplicates = 0u64;
    let mut bad = 0u64;
    let mut received = 0u64;
    let deadline = Instant::now()
        + Duration::from_millis(config.consume_timeout_ms.max(config.run_timeout_ms));
    let began = Instant::now();
    while received < config.count && Instant::now() < deadline {
        let Some(result) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        let message = match result {
            Ok(message) => message,
            Err(error) => {
                if error.rdkafka_error_code() == Some(RDKafkaErrorCode::PartitionEOF) {
                    continue;
                }
                bad += 1;
                continue;
            }
        };
        received += 1;
        let Some(key) = message.key() else {
            bad += 1;
            continue;
        };
        if key.len() != 16 {
            bad += 1;
            continue;
        }
        let id = u64::from_be_bytes(key[..8].try_into().map_err(|_| "record key")?);
        let expected_key = records::key(config.record_seed, id);
        let expected_value = if config.payload_mode == "json1k" {
            records::json1k_value(config.record_seed, id)
        } else {
            records::value(
                config.record_seed,
                id,
                config.payload_bytes,
                config.payload_mode == "seeded",
            )
        };
        let payload = message.payload().unwrap_or(&[]);
        let expected_header = records::gw8_header(config.record_seed, id);
        let header_ok = message.headers().is_some_and(|headers| {
            (0..headers.count()).any(|index| {
                let header = headers.get(index);
                header.key == "gw" && header.value == Some(expected_header.as_slice())
            })
        });
        let partition_ok = id % u64::try_from(config.partitions).unwrap_or(1)
            == u64::try_from(message.partition()).unwrap_or(u64::MAX);
        if key != expected_key.as_slice()
            || payload != expected_value.as_slice()
            || !header_ok
            || !partition_ok
        {
            bad += 1;
            continue;
        }
        if !seen.insert(id) {
            duplicates += 1;
            continue;
        }
        verified += 1;
        let record = json!({
            "id": id.to_string(),
            "attempt_index": id,
            "topic": config.topic,
            "partition": message.partition(),
            "offset": message.offset(),
            "payload_hash": format!("{:x}", Sha256::digest(payload)),
            "status": "verified"
        });
        serde_json::to_writer(&mut history, &record)?;
        writeln!(history)?;
    }
    history.flush()?;
    let elapsed_s = began.elapsed().as_secs_f64().max(f64::EPSILON);
    let threads = fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("Threads:")
                    .and_then(|s| s.trim().parse::<u64>().ok())
            })
        })
        .unwrap_or(1);
    let raw = json!({
        "client": "rust-rdkafka-BaseConsumer",
        "version": native::WRAPPER_VERSION,
        "effective_config": effective,
        "timed": {
            "offered": received,
            "accepted": received,
            "acknowledged": 0,
            "consumed": verified,
            "rejected": bad,
            "timed_out": u64::from(verified < config.count),
            "unknown": 0,
            "elapsed_s": elapsed_s
        },
        "verification": {
            "verified_ids": verified,
            "duplicate_ids": duplicates,
            "bad_records": bad,
            "performed": true
        },
        "assignment": {
            "offsets": config.assignment_offsets,
            "partitions": config.partitions
        },
        "resources": {
            "user_cpu_seconds": 0,
            "system_cpu_seconds": 0,
            "peak_rss_bytes": 0,
            "threads_count": threads
        }
    });
    let mut file = create(&std::env::var("RUST_PEER_RAW")?)?;
    serde_json::to_writer(&mut file, &raw)?;
    writeln!(file)?;
    file.flush()?;
    Ok(verified == config.count && duplicates == 0 && bad == 0)
}

fn run() -> Result<bool, Failure> {
    let mode = std::env::args()
        .nth(1)
        .ok_or("usage: rust-peer emit-config|produce|emit-consumer-config|consume")?;
    if !["emit-config", "produce", "emit-consumer-config", "consume"].contains(&mode.as_str()) {
        return Err("unsupported mode".into());
    }
    let runtime = native::runtime(&std::env::var("RUST_PEER_NATIVE_SHA256")?)?;
    let config: Config = serde_json::from_slice(&fs::read(std::env::var("RUST_PEER_CONFIG")?)?)?;
    config.validate()?;
    if mode == "emit-consumer-config" || mode == "consume" {
        if mode == "emit-consumer-config" {
            let consumer: BaseConsumer = config.client(true).create()?;
            let effective = native::consumer_effective(consumer.client())?;
            println!(
                "{}",
                json!({"runtime":runtime,"effective_config":effective})
            );
            return Ok(true);
        }
        return consume(&config);
    }
    let producer: BaseProducer<_> = config.client(false).create_with_context(Context)?;
    let effective = native::effective(producer.client())?;
    if mode == "emit-config" {
        println!(
            "{}",
            json!({"runtime":runtime,"effective_config":effective})
        );
        return Ok(true);
    }
    let warmup = phase(&config, false)?;
    let timed = phase(&config, true)?;
    let inspection = inspect(&producer, &config);
    let (durability, cluster_id, broker_nodes) = inspection.unwrap_or_else(|error| {
        eprintln!("broker inspection failed: {error}");
        (
            json!({"verified":false,"replication_factor":1,"min_insync_replicas":1}),
            String::new(),
            0,
        )
    });
    let warm_ok = config.warmup == 0 || produce(&producer, &config, config.warmup, &warmup);
    let before = watermarks(&producer, &config);
    let ok = produce(&producer, &config, config.count, &timed) && warm_ok;
    let after = watermarks(&producer, &config);
    let queried = before.is_ok() && after.is_ok();
    let before = before.unwrap_or_else(|_| vec![0; config.partitions as usize]);
    let after = after.unwrap_or_else(|_| vec![0; config.partitions as usize]);
    let partitions: Vec<_> = before.iter().zip(&after).enumerate().map(|(p,(start,end))|
        json!({"partition":p,"start_offset":start,"end_offset":end,"offset_delta":(end-start).max(0)})).collect();
    let delta: i64 = before
        .iter()
        .zip(&after)
        .map(|(start, end)| (end - start).max(0))
        .sum();
    let threads = fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("Threads:")
                    .and_then(|s| s.trim().parse::<u64>().ok())
            })
        })
        .unwrap_or(1);
    let mut warm = warmup.lock().expect("phase lock");
    let mut measured = timed.lock().expect("phase lock");
    warm.finish()?;
    measured.finish()?;
    let raw = json!({"client":"rust-rdkafka-BaseProducer","version":native::WRAPPER_VERSION,"runtime":runtime,
        "effective_config":effective,"warmup":warm.counts,"timed":measured.counts,"cluster_id":cluster_id,
        "broker_nodes":broker_nodes,"durability":durability,
        "high_watermarks":{"queried":queried && after.iter().zip(&before).all(|(e,s)| e>=s),"partitions":partitions,"total_offset_delta":delta},
        "verification":{"verified_ids":0,"duplicate_ids":0,"bad_records":0,"performed":false},
        "resources":{"user_cpu_seconds":0,"system_cpu_seconds":0,"peak_rss_bytes":0,"threads_count":threads}});
    let mut file = create(&std::env::var("RUST_PEER_RAW")?)?;
    serde_json::to_writer(&mut file, &raw)?;
    writeln!(file)?;
    file.flush()?;
    Ok(ok)
}

fn main() {
    match run() {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("rust peer: {error}");
            std::process::exit(2);
        }
    }
}
