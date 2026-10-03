//! Prepared bounded public-API producer and independent delivery verifier.
//! No explicit partition is assigned by the driver. No measurements exist yet.
use bytes::Bytes;
use partitionline::{
    Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig, StickyPartitioner,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;

type Outcome<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const MAGIC: &[u8; 8] = b"PLSTK01\n";
const VALUE_MAGIC: &[u8; 8] = b"PLSTKVAL";
const ROW_BYTES: usize = 76;
const MAX_RECORDS: u64 = 10_000_000;
const QUALIFICATION_RECORDS: u64 = 24_576;
const QUALIFICATION_WARMUP: u64 = 8192;
const QUALIFICATION_EXERCISE: u64 = 16_384;
const MIN_MEASURE_RECORDS: u64 = 1_000_000;
const WINDOW: usize = 8192;
const FLOOR: u64 = 350 * 1024 * 1024; // The future host coordinator enforces this physical floor.

fn fail(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_owned()).into()
}
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
fn body(seed: u64, id: u64) -> Bytes {
    let mut value = Vec::with_capacity(100);
    value.extend_from_slice(VALUE_MAGIC);
    value.extend_from_slice(&seed.to_be_bytes());
    value.extend_from_slice(&id.to_be_bytes());
    let mut word = mix(seed ^ id);
    while value.len() < 100 {
        let take = (100 - value.len()).min(8);
        value.extend_from_slice(&word.to_be_bytes()[..take]);
        word = mix(word);
    }
    value.into()
}
fn key(seed: u64, id: u64, keyed: bool) -> Option<Bytes> {
    keyed.then(|| {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&seed.to_be_bytes());
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.into()
    })
}
fn hash(key: Option<&[u8]>, value: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update([u8::from(key.is_some())]);
    if let Some(key) = key {
        digest.update(key);
    }
    digest.update(value);
    digest.finalize().into()
}
fn ns(start: Instant) -> Outcome<u64> {
    Ok(u64::try_from(start.elapsed().as_nanos())?)
}
fn process_cpu_ticks() -> Outcome<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| fail("malformed self process stat"))?;
    let fields: Vec<_> = stat
        .get(end + 2..)
        .ok_or_else(|| fail("malformed process stat fields"))?
        .split_whitespace()
        .collect();
    let user: u64 = fields
        .get(11)
        .ok_or_else(|| fail("no user CPU ticks"))?
        .parse()?;
    let system: u64 = fields
        .get(12)
        .ok_or_else(|| fail("no system CPU ticks"))?
        .parse()?;
    user.checked_add(system)
        .ok_or_else(|| fail("CPU tick overflow"))
}
fn require_producer_cpu(qualification: bool) -> Outcome<()> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let expected = if qualification {
        "Cpus_allowed_list:\t0-1"
    } else {
        "Cpus_allowed_list:\t3"
    };
    if !status.lines().any(|line| line == expected) {
        return Err(fail(
            "producer CPU affinity differs from qualification0,1 or ranking3 lease",
        ));
    }
    Ok(())
}
fn create(path: &Path) -> Outcome<File> {
    Ok(OpenOptions::new().write(true).create_new(true).open(path)?)
}
fn receipt(out: &Path, label: &str, value: &Value) -> Outcome<()> {
    let mut file = create(&out.join(format!("{label}.json")))?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

struct Settings {
    profile: String,
    bootstrap: String,
    topic: String,
    out: PathBuf,
    seed: u64,
    qualification: bool,
}
impl Settings {
    fn read() -> Outcome<Self> {
        let args: Vec<String> = std::env::args().collect();
        if args.len() != 7
            || !["produce", "verify", "qualify-produce", "qualify-verify"]
                .contains(&args[1].as_str())
        {
            return Err(fail(
                "usage: sticky-benchmark produce|verify|qualify-produce|qualify-verify PROFILE BOOTSTRAP TOPIC OUT SEED",
            ));
        }
        let profile = args[2].clone();
        if ![
            "rust-rr-keyed",
            "rust-rr-null",
            "rust-uniform-keyed",
            "rust-uniform-null",
            "java-uniform-keyed",
            "java-uniform-null",
        ]
        .contains(&profile.as_str())
        {
            return Err(fail("unknown exact six-profile name"));
        }
        if args[4].is_empty()
            || args[4].len() > 120
            || !args[4]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(fail("bounded fresh topic name required"));
        }
        Ok(Self {
            profile,
            bootstrap: args[3].clone(),
            topic: args[4].clone(),
            out: args[5].clone().into(),
            seed: args[6].parse()?,
            qualification: args[1].starts_with("qualify-"),
        })
    }
    fn max_records(&self) -> u64 {
        if self.qualification {
            QUALIFICATION_RECORDS
        } else {
            MAX_RECORDS
        }
    }
    fn exercise_minimum(&self) -> u64 {
        if self.qualification {
            QUALIFICATION_EXERCISE
        } else {
            MIN_MEASURE_RECORDS
        }
    }
    fn keyed(&self) -> bool {
        self.profile.ends_with("-keyed")
    }
}
struct Ack {
    id: u64,
    phase: u32,
    partition: i32,
    offset: i64,
    started: u64,
    completed: u64,
    hash: [u8; 32],
    packed_record_upper: u32,
}
fn row(writer: &mut BufWriter<File>, ack: &Ack) -> Outcome<()> {
    writer.write_all(&ack.id.to_be_bytes())?;
    writer.write_all(&ack.phase.to_be_bytes())?;
    writer.write_all(&ack.partition.to_be_bytes())?;
    writer.write_all(&ack.offset.to_be_bytes())?;
    writer.write_all(&ack.started.to_be_bytes())?;
    writer.write_all(&ack.completed.to_be_bytes())?;
    writer.write_all(&ack.hash)?;
    writer.write_all(&ack.packed_record_upper.to_be_bytes())?;
    Ok(())
}
fn take_u64(bytes: &[u8]) -> Outcome<u64> {
    Ok(u64::from_be_bytes(bytes.try_into()?))
}
fn take_i64(bytes: &[u8]) -> Outcome<i64> {
    Ok(i64::from_be_bytes(bytes.try_into()?))
}
fn take_i32(bytes: &[u8]) -> Outcome<i32> {
    Ok(i32::from_be_bytes(bytes.try_into()?))
}
fn take_u32(bytes: &[u8]) -> Outcome<u32> {
    Ok(u32::from_be_bytes(bytes.try_into()?))
}

async fn phase(
    producer: &Producer,
    settings: &Settings,
    journal: &mut BufWriter<File>,
    epoch: Instant,
    next_id: &mut u64,
    phase: u32,
) -> Outcome<Value> {
    let duration = Duration::from_secs(if settings.qualification {
        0
    } else if phase == 0 {
        15
    } else {
        60
    });
    let minimum = if phase == 0 {
        if settings.qualification {
            QUALIFICATION_WARMUP
        } else {
            10_000
        }
    } else {
        settings.exercise_minimum()
    };
    let phase_start = Instant::now();
    let deadline = phase_start + Duration::from_secs(if settings.qualification { 90 } else { 300 });
    let phase_start_ns = ns(epoch)?;
    let cpu_ticks_before = process_cpu_ticks()?;
    let mut futures = JoinSet::new();
    let mut admitted = 0_u64;
    let mut acknowledged = 0_u64;
    let mut partition_counts = BTreeMap::<i32, u64>::new();
    let mut packed_record_upper_sum = 0_u64;
    loop {
        if Instant::now() >= deadline {
            return Err(fail(
                "bounded phase timeout; outstanding sends are ambiguous until close",
            ));
        }
        let complete_admission = admitted >= minimum && phase_start.elapsed() >= duration;
        if !complete_admission && futures.len() < WINDOW {
            if *next_id >= settings.max_records() {
                return Err(fail("bounded record cap reached before required count/time; unqualified, preserve history"));
            }
            let id = *next_id;
            *next_id += 1;
            let value = body(settings.seed, id);
            let key = key(settings.seed, id, settings.keyed());
            let payload_hash = hash(key.as_deref(), &value);
            let packed_record_upper = u32::try_from(
                partitionline::Record {
                    offset: 0,
                    timestamp: 0,
                    key: key.clone(),
                    value: Some(value.clone()),
                    headers: Vec::new(),
                }
                .record_size_upper_bound()?,
            )?;
            let mut record = ProduceRecord::to(settings.topic.clone()).value(value);
            if let Some(key) = key {
                record = record.key(key);
            }
            if record.partition.is_some() {
                return Err(fail("driver must never set explicit partition"));
            }
            let owned = producer.clone();
            let _abort_handle = futures.spawn(async move {
                let started = ns(epoch)?;
                let metadata = owned.send(record).await?;
                let completed = ns(epoch)?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Ack {
                    id,
                    phase,
                    partition: metadata.partition,
                    offset: metadata.offset,
                    started,
                    completed,
                    hash: payload_hash,
                    packed_record_upper,
                })
            });
            admitted += 1;
            // Drain already-ready acks to avoid making the fixed task window a batching rule.
            while let Some(completed) = futures.try_join_next() {
                let ack = completed??;
                if ack.partition < 0 || ack.offset < 0 {
                    return Err(fail("invalid public ack metadata"));
                }
                row(journal, &ack)?;
                *partition_counts.entry(ack.partition).or_default() += 1;
                packed_record_upper_sum += u64::from(ack.packed_record_upper);
                acknowledged += 1;
            }
            continue;
        }
        if let Some(completed) = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            futures.join_next(),
        )
        .await?
        {
            let ack = completed??;
            if ack.partition < 0 || ack.offset < 0 {
                return Err(fail("invalid public ack metadata"));
            }
            row(journal, &ack)?;
            *partition_counts.entry(ack.partition).or_default() += 1;
            packed_record_upper_sum += u64::from(ack.packed_record_upper);
            acknowledged += 1;
        } else if complete_admission {
            break;
        }
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(fail("phase absolute deadline before flush"));
    }
    producer
        .flush_timeout(remaining.min(Duration::from_secs(120)))
        .await?;
    journal.flush()?;
    let seconds = phase_start.elapsed().as_secs_f64();
    if admitted != acknowledged || acknowledged < minimum || seconds < duration.as_secs_f64() {
        return Err(fail(
            "phase minima or complete acknowledgement barrier failed",
        ));
    }
    Ok(
        json!({"phase":phase,"start_monotonic_ns":phase_start_ns,"end_monotonic_ns":ns(epoch)?,
        "acknowledged":acknowledged,"seconds_including_admission_and_drain_flush":seconds,
        "process_cpu_ticks_delta":process_cpu_ticks()?.checked_sub(cpu_ticks_before).ok_or_else(||fail("CPU ticks regressed"))?,
        "acknowledged_records_per_second":if settings.qualification { None } else { Some(acknowledged as f64/seconds) },
        "performance_qualified":false,"purpose":if settings.qualification {"qualification"} else {"ranking"},
        "partition_counts":partition_counts,"sum_record_size_upper_bound":packed_record_upper_sum,
        "sticky_61_byte_cohort_overheads":"unobserved by public API; not added to this sum",
        "latency_scope":"individual public send invocation to its ack; closed-loop window, not CO-corrected/open-loop claim"}),
    )
}
async fn produce(settings: &Settings) -> Outcome<()> {
    require_producer_cpu(settings.qualification)?;
    if settings.profile.starts_with("java-") {
        return Err(fail("Java profiles require genuine Java driver"));
    }
    std::fs::create_dir(&settings.out)?;
    receipt(
        &settings.out,
        "configuration",
        &json!({"classification":"executed only when separately authorized; no source preparation measurement",
        "profile":settings.profile,"purpose":if settings.qualification {"qualification"} else {"ranking"},
        "performance_qualified":false,"seed":settings.seed,"topic":settings.topic,"value_bytes":100,"key_bytes":if settings.keyed(){16}else{0},
        "key_presence":if settings.keyed(){"non-null"}else{"null"},"explicit_partition":false,
        "acks":-1,"idempotence":true,"max_in_flight":5,"linger_ms":5,"batch_bytes":1048576,
        "compression":"none","connections_per_leader":1,"record_window":WINDOW,"max_records":settings.max_records(),
        "record_accounting":"public Rust conservative per-record upper bound; real sticky policy adds61 per cohort",
        "producer_policy":if settings.profile.starts_with("rust-uniform"){"Java4.3 uniform/adaptive=false state machine; Rust packed accounting"}else{"existing global round-robin null/murmur2 keyed"},
        "host_physical_disk_floor_required":FLOOR,
        "request_timeout_ms":if settings.qualification {10000} else {30000},
        "delivery_timeout_ms":if settings.qualification {30000} else {120000},
        "max_block_ms":if settings.qualification {5000} else {60000},
        "cpu_lease_required":if settings.qualification {"CPU0,1 future qualification lease"} else {"exclusive CPU3 future ranking lease"}}),
    )?;
    let mut config = ProducerConfig::bootstrap([settings.bootstrap.clone()]);
    config.acks = -1;
    config.enable_idempotence = true;
    config.max_in_flight = 5;
    config.connections = 1;
    config.linger = Duration::from_millis(5);
    config.batch_records = 32768;
    config.batch_bytes = 1048576;
    config.max_request_size = 1048576;
    config.buffer_memory = 32 * 1024 * 1024;
    config.request_timeout = Duration::from_secs(if settings.qualification { 10 } else { 30 });
    config.delivery_timeout = Duration::from_secs(if settings.qualification { 30 } else { 120 });
    config.max_block = Duration::from_secs(if settings.qualification { 5 } else { 60 });
    config.allow_auto_topic_creation = false;
    if settings.profile.starts_with("rust-uniform") {
        config = config.partitioner(StickyPartitioner::seeded(settings.seed));
    }
    let producer = Producer::new(config).await?;
    let partitions = producer.partitions_for(settings.topic.clone()).await?;
    if partitions.len() != 6 || partitions.iter().any(|p| p.leader() < 0) {
        return Err(fail("exactly six leader-eligible partitions required"));
    }
    let mut empty = Consumer::new(ConsumerConfig::bootstrap([settings.bootstrap.clone()])).await?;
    empty.assign_topic(&settings.topic, 0).await?;
    let baseline = empty.end_offsets(empty.assignment()).await?;
    if baseline.len() != 6 || baseline.iter().any(|(_, offset)| *offset != 0) {
        return Err(fail("fresh empty topic required"));
    }
    empty.close().await?;
    receipt(
        &settings.out,
        "metadata-before",
        &json!({"partitions":partitions.iter().map(|p|json!({"partition":p.partition(),"leader":p.leader(),"epoch":p.leader_epoch()})).collect::<Vec<_>>(),"all_end_offsets_zero":true}),
    )?;
    let mut journal =
        BufWriter::with_capacity(256 * 1024, create(&settings.out.join("producer-acks.bin"))?);
    journal.write_all(MAGIC)?;
    let epoch = Instant::now();
    let mut next_id = 0_u64;
    let mut phases = Vec::new();
    for number in 0..=1 {
        match phase(
            &producer,
            settings,
            &mut journal,
            epoch,
            &mut next_id,
            number,
        )
        .await
        {
            Ok(value) => {
                receipt(&settings.out, &format!("phase-{number}"), &value)?;
                phases.push(value);
            }
            Err(error) => {
                journal.flush()?;
                let settled = producer
                    .close_timeout(Duration::from_secs(if settings.qualification {
                        30
                    } else {
                        120
                    }))
                    .await;
                receipt(
                    &settings.out,
                    "failure",
                    &json!({"message":error.to_string(),"next_id":next_id,"close_succeeded":settled.is_ok(),"qualification":false,"cancelled_sends_may_have_delivered":true}),
                )?;
                return Err(error);
            }
        }
    }
    let after = producer.partitions_for(settings.topic.clone()).await?;
    if after != partitions {
        return Err(fail(
            "metadata changed during measurement; preserve unqualified history",
        ));
    }
    producer
        .close_timeout(Duration::from_secs(if settings.qualification {
            30
        } else {
            120
        }))
        .await?;
    journal.flush()?;
    receipt(
        &settings.out,
        "producer-complete",
        &json!({"profile":settings.profile,"total_records":next_id,"phases":phases,
        "source_identity":"host must bind actual immutable client+driver SHA before execution",
        "independent_delivery_verification_required":true,"delivery_qualified":false,
        "utc_finished_ms":SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()}),
    )?;
    Ok(())
}

async fn verify(settings: &Settings) -> Outcome<()> {
    let path = settings.out.join("producer-acks.bin");
    let length = path.metadata()?.len();
    if length < 8 || (length - 8) % u64::try_from(ROW_BYTES)? != 0 {
        return Err(fail("incomplete bounded ack journal"));
    }
    let total = (length - 8) / u64::try_from(ROW_BYTES)?;
    if total > settings.max_records() {
        return Err(fail("record count exceeds verifier memory bound"));
    }
    let count = usize::try_from(total)?;
    let mut expected = vec![None::<(i32, i64, u32)>; count];
    let mut seen = vec![false; count];
    let mut reader = BufReader::new(File::open(path)?);
    let mut magic = [0_u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(fail("wrong ack journal magic"));
    }
    let mut phase_counts = [0_u64; 2];
    for _ in 0..total {
        let mut bytes = [0_u8; ROW_BYTES];
        reader.read_exact(&mut bytes)?;
        let id = take_u64(&bytes[0..8])?;
        let index = usize::try_from(id)?;
        let phase = take_u32(&bytes[8..12])?;
        if phase > 1 || index >= count || expected[index].is_some() {
            return Err(fail("duplicate/outside ID or phase"));
        }
        let value = body(settings.seed, id);
        let record_key = key(settings.seed, id, settings.keyed());
        if bytes[40..72] != hash(record_key.as_deref(), &value) {
            return Err(fail("ack ID/hash mismatch"));
        }
        expected[index] = Some((take_i32(&bytes[12..16])?, take_i64(&bytes[16..24])?, phase));
        phase_counts[usize::try_from(phase)?] += 1;
    }
    if phase_counts[1] < settings.exercise_minimum() || expected.iter().any(Option::is_none) {
        return Err(fail("missing dense IDs or measured floor"));
    }
    let mut config = ConsumerConfig::bootstrap([settings.bootstrap.clone()]);
    config.max_poll_records = Some(8192);
    config.buffer_memory = 32 * 1024 * 1024;
    config.enable_auto_commit = false;
    let mut consumer = Consumer::new(config).await?;
    consumer.assign_topic(&settings.topic, 0).await?;
    let fence = consumer.end_offsets(consumer.assignment()).await?;
    if fence.len() != 6
        || fence
            .iter()
            .map(|(_, offset)| u64::try_from(*offset))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .sum::<u64>()
            != total
    {
        return Err(fail("broker fence differs from total acknowledged IDs"));
    }
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut verified = 0_u64;
    let mut offsets = BTreeMap::<i32, i64>::new();
    let mut partition_counts = BTreeMap::<i32, [u64; 2]>::new();
    let mut capture = BufWriter::with_capacity(
        256 * 1024,
        create(&settings.out.join("consumer-delivery.bin"))?,
    );
    capture.write_all(MAGIC)?;
    while verified < total {
        if Instant::now() >= deadline {
            return Err(fail("delivery verification timeout"));
        }
        let records = consumer.fetch_timeout(Duration::from_secs(5)).await?;
        for record in records.as_ref() {
            let value = record.value().ok_or_else(|| fail("null/missing value"))?;
            if !record.headers().is_empty()
                || value.len() != 100
                || &value[..8] != VALUE_MAGIC
                || take_u64(&value[8..16])? != settings.seed
            {
                return Err(fail("delivery value seed/magic/length mismatch"));
            }
            let id = take_u64(&value[16..24])?;
            let index = usize::try_from(id)?;
            let (partition, offset, phase) = expected
                .get(index)
                .and_then(|x| *x)
                .ok_or_else(|| fail("unknown delivery ID"))?;
            if seen[index]
                || record.partition() != partition
                || record.offset() != offset
                || record.key() != key(settings.seed, id, settings.keyed()).as_deref()
                || value != body(settings.seed, id).as_ref()
            {
                return Err(fail(
                    "duplicate/corrupt record or public ack offset mismatch",
                ));
            }
            let next = offsets.entry(partition).or_insert(0);
            if record.offset() != *next {
                return Err(fail("broker offset hole/regression"));
            }
            *next += 1;
            seen[index] = true;
            partition_counts.entry(partition).or_insert([0, 0])[usize::try_from(phase)?] += 1;
            row(
                &mut capture,
                &Ack {
                    id,
                    phase,
                    partition,
                    offset,
                    started: 0,
                    completed: 0,
                    hash: hash(record.key(), value),
                    packed_record_upper: 0,
                },
            )?;
            verified += 1;
        }
    }
    capture.flush()?;
    let final_fence = consumer.end_offsets(consumer.assignment()).await?;
    if final_fence != fence {
        return Err(fail("broker fence moved during verification"));
    }
    consumer.close().await?;
    receipt(
        &settings.out,
        "delivery-complete",
        &json!({"profile":settings.profile,"verified_records":verified,"phase_counts":phase_counts,
        "public_ID_key_value_hash_and_ack_partition_offset_match":true,"duplicates":0,"holes":0,"corruptions":0,
        "partition_phase_counts":partition_counts,"fence":fence.iter().map(|(tp,o)|json!({"partition":tp.partition(),"offset":o})).collect::<Vec<_>>(),
        "qualification_scope":"delivery only; host still validates duration, paired count, CPU/image/config identities and raw history hashes"}),
    )?;
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Outcome<()> {
    let settings = Settings::read()?;
    if std::env::args()
        .nth(1)
        .is_some_and(|action| action.ends_with("produce"))
    {
        produce(&settings).await
    } else {
        verify(&settings).await
    }
}
