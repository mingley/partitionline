//! Partitionline produce driver for gateway cells.

use std::time::{Duration, Instant};

use bytes::Bytes;
use partitionline::{
    Admin, Compression, ProduceRecord, Producer, ProducerConfig, TlsConfig, TopicPartition,
    LATEST_TIMESTAMP,
};
use serde_json::json;
use tokio::task::JoinSet;

use crate::cells::{AckMode, Cell, Load};
use crate::json1k;
use crate::rusage::ProcessRusage;
use crate::schedule::FixedRateSchedule;
use crate::settings::{ComparableSettings, TlsMode};

#[path = "../../../examples/common/bench_history.rs"]
#[expect(
    dead_code,
    reason = "shared benchmark helper contains consumer-only envelope parsing"
)]
mod history;

/// Outcome of one partitionline drive.
pub struct DriveOutcome {
    /// Offered records in the timed phase.
    pub offered: u64,
    /// Accepted / enqueued records.
    pub accepted: u64,
    /// Broker acknowledgements (never an enqueue rate).
    pub acknowledged: u64,
    /// Local rejections.
    pub rejected: u64,
    /// Timeouts.
    pub timed_out: u64,
    /// Unresolved.
    pub unknown: u64,
    /// History-verified ids (accepted journal lines).
    pub verified_ids: u64,
    /// Timed-phase seconds.
    pub elapsed_s: f64,
    /// Per-record ack latency, microseconds.
    pub latencies_us: Vec<f64>,
    /// High-watermark rows.
    pub watermarks: Vec<serde_json::Value>,
    /// Sum of offset deltas.
    pub watermark_delta: i64,
    /// Watermarks were queried.
    pub watermarks_queried: bool,
    /// Effective settings.
    pub settings: ComparableSettings,
    /// Process rusage of this process.
    pub rusage: ProcessRusage,
    /// Queue-full retries.
    pub queue_full_retries: u64,
}

/// Drive request.
pub struct DriveRequest {
    /// Cell.
    pub cell: Cell,
    /// Load.
    pub load: Load,
    /// Bootstrap.
    pub bootstrap: String,
    /// Topic.
    pub topic: String,
    /// Timed record count.
    pub count: u64,
    /// Warmup records.
    pub warmup: u64,
    /// One overall deadline.
    pub run_timeout: Duration,
    /// History path.
    pub history_path: Option<String>,
    /// TLS mode.
    pub tls: TlsMode,
}

/// Run the partitionline side of a cell.
pub async fn drive_partitionline(req: &DriveRequest) -> Result<DriveOutcome, String> {
    let deadline = Instant::now()
        .checked_add(req.run_timeout)
        .ok_or_else(|| "run timeout overflow".to_string())?;
    let mut cfg = ProducerConfig::bootstrap([req.bootstrap.clone()]);
    cfg.acks = req.cell.acks;
    cfg.max_in_flight =
        usize::try_from(req.cell.max_in_flight).map_err(|_| "max_in_flight".to_string())?;
    cfg.linger = Duration::from_millis(req.cell.linger_ms);
    cfg.batch_bytes =
        usize::try_from(req.cell.batch_size_bytes).map_err(|_| "batch_size_bytes".to_string())?;
    cfg.compression = Compression::from_name(req.cell.codec).map_err(|e| e.to_string())?;
    cfg.enable_idempotence = false;
    cfg.client_id = "gateway-partitionline".into();
    let tls_mode = apply_tls(&mut cfg, req.tls)?;
    let settings = ComparableSettings {
        codec: req.cell.codec.to_string(),
        level: req.cell.level,
        acks: req.cell.acks,
        max_in_flight: req.cell.max_in_flight,
        linger_ms: req.cell.linger_ms,
        batch_size_bytes: req.cell.batch_size_bytes,
        tls: tls_mode,
    };
    let cpu_origin = ProcessRusage::current();
    let producer = Producer::new(cfg)
        .await
        .map_err(|e| format!("producer: {e}"))?;
    let partitions = producer
        .partitions_for(req.topic.clone())
        .await
        .map_err(|e| format!("partitions_for: {e}"))?;
    if partitions.len() != req.cell.partitions as usize {
        return Err(format!(
            "topic has {} partitions; cell requires {}",
            partitions.len(),
            req.cell.partitions
        ));
    }
    if req.warmup > 0 {
        run_phase(
            &producer, req, req.warmup, None, deadline, /*journal*/ &mut None,
        )
        .await?;
        producer
            .flush()
            .await
            .map_err(|e| format!("warmup flush: {e}"))?;
    }
    let before = watermarks(&req.bootstrap, &req.topic, req.cell.partitions).await;
    let mut journal = req
        .history_path
        .as_deref()
        .map(history::Journal::create)
        .transpose()
        .map_err(|e| e.to_string())?;
    if let Some(ref mut journal) = journal {
        journal
            .line(&format!(
                "{{\"kind\":\"config\",\"schema_version\":1,\"role\":\"producer\",\"topic\":{},\"acks\":{},\"idempotent\":false,\"transactional\":false,\"seed\":{},\"payload_bytes\":{},\"partitions\":{},\"count\":{},\"acknowledgment_mode\":{}}}",
                history::quote(&req.topic),
                req.cell.acks,
                json1k::SEED,
                json1k::VALUE_BYTES,
                req.cell.partitions,
                req.count,
                history::quote(req.cell.ack_mode.label()),
            ))
            .map_err(|e| e.to_string())?;
        journal.checkpoint().map_err(|e| e.to_string())?;
    }
    let timed = Instant::now();
    let phase = run_phase(
        &producer,
        req,
        req.count,
        req.load.rate_per_second(),
        deadline,
        &mut journal,
    )
    .await?;
    let flush = tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        producer.flush(),
    )
    .await;
    let flush_ok = matches!(flush, Ok(Ok(())));
    if !flush_ok {
        producer
            .flush()
            .await
            .map_err(|e| format!("final flush: {e}"))?;
    }
    let elapsed_s = timed.elapsed().as_secs_f64().max(1e-9);
    let after = watermarks(&req.bootstrap, &req.topic, req.cell.partitions).await;
    let metrics = producer.metrics();
    let acknowledged = metrics.records_acked.saturating_sub(req.warmup);
    let _ = producer.close().await;
    let accepted = phase.accepted;
    let unknown = accepted.saturating_sub(acknowledged);
    let (watermarks, watermark_delta, watermarks_queried) = match (before, after) {
        (Ok(start), Ok(end)) if start.len() == end.len() => {
            let rows: Vec<_> = start
                .iter()
                .zip(&end)
                .enumerate()
                .map(|(p, (a, b))| {
                    json!({
                        "partition": p,
                        "start_offset": a,
                        "end_offset": b,
                        "offset_delta": (*b - *a).max(0)
                    })
                })
                .collect();
            let delta: i64 = start.iter().zip(&end).map(|(a, b)| (*b - *a).max(0)).sum();
            (rows, delta, true)
        }
        _ => (Vec::new(), 0, false),
    };
    if let Some(ref mut journal) = journal {
        journal
            .line(&format!(
                "{{\"kind\":\"summary\",\"role\":\"producer\",\"accepted_total\":{accepted},\"acknowledged_total\":{acknowledged},\"run_disposition\":\"{}\"}}",
                if flush_ok { "executed" } else { "failed" }
            ))
            .map_err(|e| e.to_string())?;
        journal.checkpoint().map_err(|e| e.to_string())?;
    }
    Ok(DriveOutcome {
        offered: phase.offered,
        accepted,
        acknowledged,
        rejected: phase.rejected,
        timed_out: u64::from(!flush_ok),
        unknown,
        verified_ids: phase.accepted,
        elapsed_s,
        latencies_us: phase.latencies_us,
        watermarks,
        watermark_delta,
        watermarks_queried,
        settings,
        rusage: ProcessRusage::current().since(cpu_origin),
        queue_full_retries: phase.queue_full_retries,
    })
}

struct Phase {
    offered: u64,
    accepted: u64,
    rejected: u64,
    queue_full_retries: u64,
    latencies_us: Vec<f64>,
}

async fn run_phase(
    producer: &Producer,
    req: &DriveRequest,
    count: u64,
    rate: Option<u64>,
    deadline: Instant,
    journal: &mut Option<history::Journal>,
) -> Result<Phase, String> {
    match req.cell.ack_mode {
        AckMode::FireAndForgetTimedFlush => {
            fire_and_forget(producer, req, count, rate, deadline, journal).await
        }
        AckMode::PerRecordCompletion => {
            per_record(producer, req, count, rate, deadline, journal).await
        }
        AckMode::ManualAssignment => Err("manual assignment is a fetch cell".into()),
    }
}

async fn fire_and_forget(
    producer: &Producer,
    req: &DriveRequest,
    count: u64,
    rate: Option<u64>,
    deadline: Instant,
    journal: &mut Option<history::Journal>,
) -> Result<Phase, String> {
    let schedule = rate.map(FixedRateSchedule::new).transpose()?;
    let origin = Instant::now();
    let mut phase = Phase {
        offered: 0,
        accepted: 0,
        rejected: 0,
        queue_full_retries: 0,
        latencies_us: Vec::new(),
    };
    for id in 0..count {
        if Instant::now() >= deadline {
            phase.rejected += count - id;
            break;
        }
        if let Some(schedule) = schedule {
            let intended = origin + schedule.arrival(id)?;
            let now = Instant::now();
            if intended > now {
                tokio::time::sleep(intended - now).await;
            }
        }
        phase.offered += 1;
        let record = record(&req.topic, req.cell.partitions, id);
        if let Some(ref mut journal) = journal {
            journal
                .record(
                    &format!("{:016x}:{id}", json1k::SEED),
                    &req.topic,
                    (id % req.cell.partitions as u64) as i32,
                    None,
                    record.key.as_deref(),
                    record.value.as_deref().unwrap_or(&[]),
                    "measure",
                    "offered",
                )
                .map_err(|e| e.to_string())?;
        }
        loop {
            match producer.try_send(record.clone()) {
                Ok(()) => {
                    phase.accepted += 1;
                    if let Some(ref mut journal) = journal {
                        journal
                            .record(
                                &format!("{:016x}:{id}", json1k::SEED),
                                &req.topic,
                                (id % req.cell.partitions as u64) as i32,
                                None,
                                record.key.as_deref(),
                                record.value.as_deref().unwrap_or(&[]),
                                "measure",
                                "accepted",
                            )
                            .map_err(|e| e.to_string())?;
                    }
                    break;
                }
                Err(partitionline::Error::QueueFull) => {
                    phase.queue_full_retries += 1;
                    tokio::task::yield_now().await;
                    if Instant::now() >= deadline {
                        phase.rejected += 1;
                        break;
                    }
                }
                Err(_) => {
                    phase.rejected += 1;
                    break;
                }
            }
        }
    }
    Ok(phase)
}

async fn per_record(
    producer: &Producer,
    req: &DriveRequest,
    count: u64,
    rate: Option<u64>,
    deadline: Instant,
    journal: &mut Option<history::Journal>,
) -> Result<Phase, String> {
    let schedule = rate.map(FixedRateSchedule::new).transpose()?;
    let origin = Instant::now();
    let mut jobs: JoinSet<(u64, Duration, bool)> = JoinSet::new();
    let mut phase = Phase {
        offered: 0,
        accepted: 0,
        rejected: 0,
        queue_full_retries: 0,
        latencies_us: Vec::new(),
    };
    for id in 0..count {
        if Instant::now() >= deadline {
            phase.rejected += count - id;
            break;
        }
        if let Some(schedule) = schedule {
            let intended = tokio::time::Instant::from_std(origin + schedule.arrival(id)?);
            tokio::time::sleep_until(intended).await;
        }
        while jobs.len() >= 4096 {
            match jobs.join_next().await {
                Some(Ok((_, elapsed, true))) => {
                    phase.accepted += 1;
                    phase.latencies_us.push(elapsed.as_secs_f64() * 1e6);
                }
                Some(_) => phase.rejected += 1,
                None => break,
            }
        }
        phase.offered += 1;
        let rec = record(&req.topic, req.cell.partitions, id);
        if let Some(ref mut journal) = journal {
            journal
                .record(
                    &format!("{:016x}:{id}", json1k::SEED),
                    &req.topic,
                    (id % req.cell.partitions as u64) as i32,
                    None,
                    rec.key.as_deref(),
                    rec.value.as_deref().unwrap_or(&[]),
                    "measure",
                    "offered",
                )
                .map_err(|e| e.to_string())?;
        }
        let producer = producer.clone();
        let _ = jobs.spawn(async move {
            let start = Instant::now();
            let result = producer.send(rec).await;
            (id, start.elapsed(), result.is_ok())
        });
    }
    while let Some(done) = jobs.join_next().await {
        match done {
            Ok((_, elapsed, true)) => {
                phase.accepted += 1;
                phase.latencies_us.push(elapsed.as_secs_f64() * 1e6);
            }
            Ok((_, _, false)) => phase.rejected += 1,
            Err(_) => phase.rejected += 1,
        }
    }
    Ok(phase)
}

fn record(topic: &str, partitions: i32, id: u64) -> ProduceRecord {
    let partition = (id % partitions as u64) as i32;
    let value = Bytes::from(json1k::value(
        json1k::SEED,
        usize::try_from(id).unwrap_or(0),
    ));
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&id.to_be_bytes());
    let mut state = json1k::SEED ^ id;
    key[8..].copy_from_slice(&json1k::splitmix64(&mut state).to_be_bytes());
    ProduceRecord::to(topic)
        .partition(partition)
        .key(Bytes::copy_from_slice(&key))
        .value(value)
        .header(
            json1k::HEADER_KEY,
            Bytes::copy_from_slice(&json1k::header(json1k::SEED, id)),
        )
}

fn apply_tls(cfg: &mut ProducerConfig, requested: TlsMode) -> Result<TlsMode, String> {
    let ca = std::env::var("TLS_CA_PEM").ok();
    let cert = std::env::var("TLS_CLIENT_CERT_PEM").ok();
    let key = std::env::var("TLS_CLIENT_KEY_PEM").ok();
    if ca.is_none() && cert.is_none() {
        return Ok(TlsMode::Plain);
    }
    let mut tls = TlsConfig::default();
    if let Some(path) = ca {
        tls.ca_pem = Some(std::fs::read(&path).map_err(|e| format!("TLS_CA_PEM: {e}"))?);
    }
    if let (Some(cert), Some(key)) = (cert, key) {
        tls.client_cert_pem =
            Some(std::fs::read(&cert).map_err(|e| format!("TLS_CLIENT_CERT_PEM: {e}"))?);
        tls.client_key_pem =
            Some(std::fs::read(&key).map_err(|e| format!("TLS_CLIENT_KEY_PEM: {e}"))?);
    }
    if let Ok(name) = std::env::var("TLS_SERVER_NAME") {
        if !name.is_empty() {
            tls.server_name = Some(name);
        }
    }
    let mode = if tls.client_cert_pem.is_some() {
        TlsMode::Mtls
    } else {
        TlsMode::Tls
    };
    if requested != TlsMode::Plain && requested != mode {
        return Err(format!(
            "requested TLS {requested:?} but environment provides {mode:?}"
        ));
    }
    cfg.tls = Some(tls);
    Ok(mode)
}

async fn watermarks(bootstrap: &str, topic: &str, partitions: i32) -> Result<Vec<i64>, String> {
    let mut admin = Admin::connect(bootstrap)
        .await
        .map_err(|e| format!("admin: {e}"))?;
    let queries: Vec<_> = (0..partitions)
        .map(|partition| (TopicPartition::new(topic, partition), LATEST_TIMESTAMP))
        .collect();
    let listed = admin
        .list_offsets(queries)
        .await
        .map_err(|e| format!("list_offsets: {e}"))?;
    let mut highs = vec![0i64; partitions as usize];
    for (tp, offset) in listed {
        if let Ok(idx) = usize::try_from(tp.partition) {
            if let Some(slot) = highs.get_mut(idx) {
                *slot = offset.offset.max(0);
            }
        }
    }
    Ok(highs)
}
