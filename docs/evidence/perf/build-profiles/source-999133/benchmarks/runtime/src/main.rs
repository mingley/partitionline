//! Runtime harness binary (KL09-09): run producer cells against
//! `nb-serve` and write result artifacts.
//!
//! Usage: `runtime --cell <id|all> --out <dir> [--repetitions <n>]`
//!
//! One `nb-serve` subprocess per (cell, repetition); the broker
//! artifact reconciles server-side counts and validation failures.
//! Allocation counting reuses the KL09-04 tool (`codec::census` over
//! `codec::CountingAlloc`); CPU/RSS come from `libc` via
//! [`runtime::measure`]. The run must start from the repository root
//! (git provenance) with the `runtime` binary beside `nb-serve`
//! (same target dir), or `NB_SERVE` pointing at the broker binary.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use codec::{census, CountingAlloc};
use partitionline::producer::{Producer, ProducerConfig};
use partitionline::Acks;

use runtime::artifact::{build_result, parse_broker_artifact, BrokerCounts, RunContext};
use runtime::cells::{generate, producer_cells, CellDef};
use runtime::drive::drive_cell;
use runtime::executor::RuntimeConfig;
use runtime::host::probe as probe_host;
use runtime::measure::{cpu_now, peak_rss_bytes, rss_now, RssSampler};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn usage() -> ! {
    eprintln!(
        "usage: runtime --cell <id|all> --out <dir> [--repetitions <n>] [--runtime <current_thread|multi_thread>] [--workers <0|1..64>] [--connect-stalled-first]"
    );
    std::process::exit(2);
}

/// Child rusage harvested with `wait4` at reap time.
#[derive(Debug, Default, Clone, Copy)]
struct ChildRusage {
    user_s: f64,
    sys_s: f64,
    peak_rss_bytes: u64,
}

/// `nb-serve` options for one run: node topology, fault injection
/// and the synthetic fetch log (KL09-10). Defaults reproduce the
/// KL09-09 single-node, no-fault, default-synth broker.
#[derive(Debug, Clone)]
struct BrokerOpts {
    partitions: i32,
    nodes: u16,
    dead_nodes: u16,
    slow_node: Option<i32>,
    slow_delay_ms: u64,
    fault_seed: u64,
    fault_rate_per_million: u32,
    synth_seed: u64,
    synth_records_per_partition: u64,
    synth_records_per_batch: u32,
    synth_payload_bytes: usize,
    synth_header_count: u32,
    synth_codec: u8,
    synth_abort_every: u64,
}

impl Default for BrokerOpts {
    fn default() -> Self {
        let synth = nullbroker::synth::SynthConfig::default();
        Self {
            partitions: 6,
            nodes: 1,
            dead_nodes: 0,
            slow_node: None,
            slow_delay_ms: 0,
            fault_seed: 0x5EED_0001,
            fault_rate_per_million: 0,
            synth_seed: synth.seed,
            synth_records_per_partition: synth.records_per_partition,
            synth_records_per_batch: synth.records_per_batch,
            synth_payload_bytes: synth.payload_bytes,
            synth_header_count: synth.header_count,
            synth_codec: synth.codec,
            synth_abort_every: synth.abort_every,
        }
    }
}

impl BrokerOpts {
    fn args(&self, out_dir: &Path, tag: &str) -> Vec<String> {
        let mut args = vec![
            "--bind".to_owned(),
            "127.0.0.1".to_owned(),
            "--partitions".to_owned(),
            self.partitions.to_string(),
            "--serve-for-secs".to_owned(),
            "900".to_owned(),
            "--artifact".to_owned(),
            out_dir
                .join(format!("{tag}.broker.json"))
                .display()
                .to_string(),
            "--nodes".to_owned(),
            self.nodes.to_string(),
            "--dead-nodes".to_owned(),
            self.dead_nodes.to_string(),
            "--slow-delay-ms".to_owned(),
            self.slow_delay_ms.to_string(),
            "--fault-seed".to_owned(),
            self.fault_seed.to_string(),
            "--fault-rate-per-million".to_owned(),
            self.fault_rate_per_million.to_string(),
            "--synth-seed".to_owned(),
            self.synth_seed.to_string(),
            "--synth-records-per-partition".to_owned(),
            self.synth_records_per_partition.to_string(),
            "--synth-records-per-batch".to_owned(),
            self.synth_records_per_batch.to_string(),
            "--synth-payload-bytes".to_owned(),
            self.synth_payload_bytes.to_string(),
            "--synth-header-count".to_owned(),
            self.synth_header_count.to_string(),
            "--synth-codec".to_owned(),
            self.synth_codec.to_string(),
            "--synth-abort-every".to_owned(),
            self.synth_abort_every.to_string(),
        ];
        if let Some(id) = self.slow_node {
            args.push("--slow-node".to_owned());
            args.push(id.to_string());
        }
        args
    }
}

/// A running `nb-serve` broker: endpoints, artifact path, and the
/// pipes needed to stop it and harvest child rusage.
struct Broker {
    endpoints: Vec<String>,
    artifact_path: PathBuf,
    child: std::process::Child,
}

impl Broker {
    fn spawn(
        nb_serve: &Path,
        opts: &BrokerOpts,
        out_dir: &Path,
        tag: &str,
    ) -> Result<Self, String> {
        let artifact_path = out_dir.join(format!("{tag}.broker.json"));
        let _ = std::fs::remove_file(&artifact_path);
        let mut child = Command::new(nb_serve)
            .args(opts.args(out_dir, tag))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn nb-serve: {e}"))?;
        let stdout = child.stdout.take().ok_or("nb-serve stdout unavailable")?;
        let mut lines = std::io::BufReader::new(stdout).lines();
        // Blocking single-line read: nb-serve prints READY immediately
        // after bind, or exits (EOF) on failure.
        let mut ports: Option<Vec<String>> = None;
        match lines.next() {
            Some(Ok(text)) => {
                let text = text.trim().to_owned();
                if let Some(rest) = text.strip_prefix("READY ports=") {
                    let list: Vec<String> = rest
                        .split(',')
                        .filter_map(|p| p.trim().parse::<u16>().ok().map(|_| p.trim().to_owned()))
                        .collect();
                    if !list.is_empty() {
                        ports = Some(list);
                    }
                }
            }
            Some(Err(e)) => return Err(format!("read nb-serve READY: {e}")),
            None => {}
        }
        let ports = ports.ok_or_else(|| {
            let _ = child.kill();
            "nb-serve exited before READY".to_owned()
        })?;
        Ok(Self {
            endpoints: ports.iter().map(|p| format!("127.0.0.1:{p}")).collect(),
            artifact_path,
            child,
        })
    }

    /// Ask for shutdown, reap with `wait4` for child rusage, and parse
    /// the broker artifact.
    fn stop(mut self) -> Result<(BrokerCounts, ChildRusage), String> {
        if let Some(mut stdin) = self.child.stdin.take() {
            let _ = stdin.write_all(b"quit\n");
        }
        // Reap with wait4 for rusage: forget the Child (its wait
        // would double-reap) after extracting the pid.
        let pid = self.child.id() as libc::pid_t;
        std::mem::forget(self.child);
        let mut status: libc::c_int = 0;
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        loop {
            let rc = unsafe { libc::wait4(pid, &mut status, 0, &mut ru) };
            if rc >= 0 {
                break;
            }
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(format!("wait4 nb-serve: {err}"));
        }
        let to_s = |tv: libc::timeval| tv.tv_sec.max(0) as f64 + tv.tv_usec.max(0) as f64 / 1e6;
        let maxrss = ru.ru_maxrss.max(0) as u64;
        #[cfg(target_os = "linux")]
        let peak = maxrss.saturating_mul(1024);
        #[cfg(not(target_os = "linux"))]
        let peak = maxrss;
        let rusage = ChildRusage {
            user_s: to_s(ru.ru_utime),
            sys_s: to_s(ru.ru_stime),
            peak_rss_bytes: peak,
        };
        let counts = parse_broker_artifact(&self.artifact_path)?;
        Ok((counts, rusage))
    }
}

/// Mean loopback TCP-connect latency over 20 samples, milliseconds.
fn loopback_rtt_ms(endpoint: &str) -> f64 {
    let mut samples = Vec::with_capacity(20);
    for _ in 0..20 {
        let start = Instant::now();
        match std::net::TcpStream::connect(endpoint) {
            Ok(stream) => {
                samples.push(start.elapsed().as_secs_f64() * 1000.0);
                drop(stream);
            }
            Err(_) => break,
        }
    }
    if samples.is_empty() {
        0.0
    } else {
        samples.iter().sum::<f64>() / samples.len() as f64
    }
}

fn utc_now_iso() -> String {
    let now = time::OffsetDateTime::now_utc();
    now.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn thread_count() -> u64 {
    #[cfg(target_os = "linux")]
    {
        // 20th field of /proc/self/stat, parsed allocation-free-ish
        // (outside the census window, so Vec churn is fine).
        if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
            if let Some(tail) = stat.rsplit_once(')') {
                let fields: Vec<&str> = tail.1.split_whitespace().collect();
                // fields[0] is state; num_threads is the 20th field
                // overall = index 17 after ") ".
                if let Some(n) = fields.get(17).and_then(|s| s.parse::<u64>().ok()) {
                    return n;
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

fn locate_nb_serve() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var("NB_SERVE") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!("NB_SERVE={} is not a file", path.display()));
    }
    let exe = std::env::current_exe().map_err(|e| format!("current_exe unavailable: {e}"))?;
    let sibling = exe
        .parent()
        .map(|dir| dir.join("nb-serve"))
        .unwrap_or_else(|| PathBuf::from("nb-serve"));
    if sibling.is_file() {
        return Ok(sibling);
    }
    // Final fallback: PATH lookup.
    if let Ok(path_var) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let candidate = dir.join("nb-serve");
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    Err("nb-serve not found beside runtime and NB_SERVE unset".to_owned())
}

fn producer_config(
    cell: &CellDef,
    bootstrap: &[String],
    max_in_flight: usize,
    batch_records: Option<usize>,
) -> ProducerConfig {
    let acks = match cell.acks {
        -1 => Acks::All,
        0 => Acks::None,
        _ => Acks::Leader,
    };
    let cfg = ProducerConfig::bootstrap(bootstrap.iter().cloned())
        .client_id(format!("runtime-{}", cell.id))
        .acks(acks)
        .max_in_flight(max_in_flight)
        .idempotent(cell.idempotent);
    let cfg = match cell.linger_override_ms {
        Some(ms) => cfg.linger(Duration::from_millis(ms)),
        None => cfg,
    };
    match batch_records {
        Some(n) => cfg.batch_records(n),
        None => cfg,
    }
}

/// Shared measured-phase wrapper: RSS sampler, CPU delta, census,
/// wall clock. The drive runs inside `f`.
struct Phase {
    wall: Duration,
    start_iso: String,
    end_iso: String,
    cpu_us: (u64, u64),
    allocs: (u64, u64),
    rss: (u64, u64),
    baseline_rss: u64,
    process_peak_rss: u64,
    threads: u64,
}

fn measured_phase(f: impl FnOnce()) -> Phase {
    // Touch the sampler + RSS paths before measuring so setup
    // allocations stay outside the census.
    let _ = rss_now();
    let _ = peak_rss_bytes();
    let sampler = RssSampler::start(Duration::from_millis(10), 60_000);
    let baseline_rss = rss_now();
    let cpu_before = cpu_now();
    let wall_start = Instant::now();
    let start_iso = utc_now_iso();
    let ((), allocs, alloc_bytes) = census(f);
    let wall = wall_start.elapsed();
    let end_iso = utc_now_iso();
    let cpu_after = cpu_now();
    let (rss_peak, rss_mean) = sampler.stop();
    let delta = cpu_after.saturating_sub(cpu_before);
    Phase {
        wall,
        start_iso,
        end_iso,
        cpu_us: (delta.user_us, delta.sys_us),
        allocs: (allocs, alloc_bytes),
        rss: (rss_peak, rss_mean),
        baseline_rss,
        process_peak_rss: peak_rss_bytes(),
        threads: thread_count(),
    }
}

/// Write the raw-latency sidecar; the artifact hashes it.
fn write_latencies(out_dir: &Path, tag: &str, latencies: &[u64]) -> Result<PathBuf, String> {
    let latency_path = out_dir.join(format!("{tag}.latency-us.txt"));
    let mut body = String::with_capacity(latencies.len() * 4);
    for lat in latencies {
        body.push_str(&lat.to_string());
        body.push('\n');
    }
    std::fs::write(&latency_path, body).map_err(|e| format!("write latency sidecar: {e}"))?;
    Ok(latency_path)
}

/// Build the artifact, write it, and report the disposition.
fn finish(
    ctx: &RunContext,
    outcome: &runtime::drive::DriveOutcome,
    repo_root: &Path,
    harness_exe: &Path,
    result_path: &Path,
) -> Result<bool, String> {
    let doc = build_result(ctx, outcome, repo_root, harness_exe, result_path)?;
    let text = serde_json::to_string_pretty(&doc).map_err(|e| format!("serialize result: {e}"))?;
    std::fs::write(result_path, text).map_err(|e| format!("write result: {e}"))?;
    Ok(doc
        .get("scenario")
        .and_then(|s| s.get("cell_disposition"))
        .and_then(|d| d.as_str())
        == Some("failed"))
}

fn produce_effective(
    cell: &CellDef,
    max_in_flight: usize,
    linger_ms: u64,
    batch_size_bytes: u64,
    batch_records: Option<usize>,
    broker_opts: &BrokerOpts,
) -> serde_json::Value {
    serde_json::json!({
        "acks": cell.acks,
        "linger_ms": linger_ms,
        "batch_size_bytes": batch_size_bytes,
        "batch_records": batch_records,
        "max_in_flight": max_in_flight,
        "idempotence": cell.idempotent,
        "compression": "none",
        "drive_mode": format!("{:?}", cell.mode).to_lowercase(),
        "linger_override_ms": cell.linger_override_ms,
        "flush_every": cell.flush_every,
        "idle_seconds": cell.idle_seconds,
        "value_bytes": cell.value_bytes,
        "key_bytes": cell.key_bytes,
        "headers_each": cell.headers_each,
        "entropy": cell.entropy,
        "seed": cell.seed,
        "timeout_secs": cell.timeout.as_secs(),
        "fault_rate_per_million": broker_opts.fault_rate_per_million,
        "fault_seed": broker_opts.fault_seed,
    })
}

/// Extra produce-run knobs (KL09-10 retry cell).
struct ProduceRun {
    broker_opts: BrokerOpts,
    batch_records: Option<usize>,
    /// Fail when the broker injected no faults (the cell must
    /// exercise the retry path).
    require_faults: bool,
}

#[allow(clippy::too_many_arguments)]
fn run_produce(
    rt: &tokio::runtime::Runtime,
    runtime_config: RuntimeConfig,
    nb_serve: &Path,
    cell: &CellDef,
    run: &ProduceRun,
    repetition: u32,
    out_dir: &Path,
    repo_root: &Path,
    harness_exe: &Path,
) -> Result<(PathBuf, bool), String> {
    // Kafka protocol rule (also enforced by benchmark-report.py):
    // idempotence caps max.in.flight at 5. Non-idempotent cells keep
    // the crate default (16).
    let max_in_flight = if cell.idempotent { 5 } else { 16 };
    let tag = format!("{}-rep{repetition}", cell.id);
    let broker = Broker::spawn(nb_serve, &run.broker_opts, out_dir, &tag)?;
    let endpoints = broker.endpoints.clone();
    let endpoint = endpoints
        .first()
        .cloned()
        .ok_or("nb-serve reported no endpoints")?;
    let producer = Arc::new(
        rt.block_on(Producer::new(producer_config(
            cell,
            &endpoints,
            max_in_flight,
            run.batch_records,
        )))
        .map_err(|e| format!("producer connect: {e}"))?,
    );
    let mut records = generate(cell);
    // Pre-reserve drive bookkeeping outside the census.
    let mut outcome = runtime::drive::DriveOutcome::default();
    outcome.latencies_us.reserve(records.len());
    let mut stamps = Vec::with_capacity(records.len());
    let phase = measured_phase(|| {
        rt.block_on(drive_cell(
            &producer,
            cell,
            &mut records,
            &mut outcome,
            &mut stamps,
        ));
    });
    let producer = Arc::try_unwrap(producer)
        .map_err(|_| "harness producer still shared after drive".to_owned())?;
    rt.block_on(producer.close())
        .map_err(|e| format!("producer close: {e}"))?;
    let rtt_ms = loopback_rtt_ms(&endpoint);
    let latency_path = write_latencies(out_dir, &tag, &outcome.latencies_us)?;
    let (counts, child_ru) = broker.stop()?;
    let host = probe_host()?;
    let defaults = ProducerConfig::default();
    let linger_ms = cell
        .linger_override_ms
        .unwrap_or(defaults.linger.as_millis() as u64);
    let mut extra = serde_json::Map::new();
    let mut extra_failed = false;
    if run.require_faults {
        extra.insert(
            "injected_requests".to_owned(),
            serde_json::Value::from(counts.injected_requests),
        );
        extra.insert(
            "injected_errors".to_owned(),
            serde_json::Value::from(counts.injected_errors),
        );
        extra.insert(
            "metadata_requests".to_owned(),
            serde_json::Value::from(counts.metadata_requests),
        );
        extra.insert(
            "produce_requests".to_owned(),
            serde_json::Value::from(counts.produce_requests),
        );
        // One faulted partition = one retried batch.
        let ratio = if counts.injected_errors > 0 {
            counts.metadata_requests as f64 / counts.injected_errors as f64
        } else {
            f64::NAN
        };
        extra.insert(
            "metadata_rpcs_per_retried_batch".to_owned(),
            serde_json::json!(ratio),
        );
        if counts.injected_requests == 0 {
            extra_failed = true;
            outcome
                .errors
                .push("no faults injected; retry path unexercised".to_owned());
        }
    }
    let ctx = RunContext {
        cell_id: cell.id,
        offered: cell.total_records() as u64,
        seed: cell.seed,
        profile: "bulk",
        consumed: 0,
        extra_failed,
        runtime_config: runtime_config.quiesce(rt)?,
        effective_settings: produce_effective(
            cell,
            max_in_flight,
            linger_ms,
            defaults.batch_bytes as u64,
            run.batch_records,
            &run.broker_opts,
        ),
        equal_semantics: serde_json::json!({
            "acks": cell.acks,
            "idempotence": cell.idempotent,
            "max_in_flight": max_in_flight,
        }),
        drive_mode: format!("{:?}", cell.mode).to_lowercase(),
        extra_execution: extra,
        latency_note: "sequential: per-record send-call latency (send to metadata). try_send modes (pipelined/flush-heavy): offer-to-flush-complete bound per record (enqueue stamp to delivering flush end); flush time also in flush_us_total".to_owned(),
        repetition,
        broker: counts,
        host,
        wall_seconds: phase.wall.as_secs_f64(),
        cpu_us: phase.cpu_us,
        allocs: phase.allocs,
        rss: phase.rss,
        baseline_rss: phase.baseline_rss,
        process_peak_rss: phase.process_peak_rss,
        threads: phase.threads,
        broker_cpu_s: (child_ru.user_s, child_ru.sys_s),
        broker_peak_rss: child_ru.peak_rss_bytes,
        rtt_ms,
        endpoints,
        timestamps: (phase.start_iso, phase.end_iso),
        latency_path,
        extra_broker_artifacts: Vec::new(),
    };
    let result_path = out_dir.join(format!("{tag}.result.json"));
    let failed = finish(&ctx, &outcome, repo_root, harness_exe, &result_path)?;
    Ok((result_path, failed))
}

fn fetch_effective(cell: &runtime::fcells::FetchCellDef) -> serde_json::Value {
    serde_json::json!({
        "acks": null,
        "linger_ms": null,
        "batch_size_bytes": null,
        "max_in_flight": null,
        "idempotence": false,
        "isolation": if cell.read_committed { "read_committed" } else { "read_uncommitted" },
        "max_poll_records": cell.max_poll_records,
        "target_records": cell.target_records,
        "paused_partitions": cell.paused_partitions,
        "seek_offset": cell.seek_offset,
        "app_delay_per_batch_ms": cell.app_delay_per_batch.as_millis() as u64,
        "synth": {
            "seed": cell.synth_seed,
            "records_per_partition": cell.synth_records_per_partition,
            "records_per_batch": cell.synth_records_per_batch,
            "payload_bytes": cell.synth_payload_bytes,
            "abort_every": cell.synth_abort_every,
            "codec": cell.synth_codec,
        },
        "buffer_memory": cell.buffer_memory,
        "max_partition_fetch_bytes": cell.max_partition_fetch_bytes,
        "max_bytes": cell.max_bytes,
        "nodes": cell.nodes,
        "slow_node": cell.slow_node,
        "slow_delay_ms": cell.slow_delay.as_millis() as u64,
        "seed": cell.seed,
        "timeout_secs": cell.timeout.as_secs(),
    })
}

#[allow(clippy::too_many_arguments)]
fn run_fetch(
    rt: &tokio::runtime::Runtime,
    runtime_config: RuntimeConfig,
    nb_serve: &Path,
    cell: &runtime::fcells::FetchCellDef,
    repetition: u32,
    out_dir: &Path,
    repo_root: &Path,
    harness_exe: &Path,
) -> Result<(PathBuf, bool), String> {
    let tag = format!("{}-rep{repetition}", cell.id);
    let broker_opts = BrokerOpts {
        partitions: cell.partitions,
        nodes: cell.nodes,
        slow_node: cell.slow_node,
        slow_delay_ms: cell.slow_delay.as_millis() as u64,
        synth_seed: cell.synth_seed,
        synth_records_per_partition: cell.synth_records_per_partition,
        synth_records_per_batch: cell.synth_records_per_batch,
        synth_payload_bytes: cell.synth_payload_bytes,
        synth_abort_every: cell.synth_abort_every,
        synth_codec: cell.synth_codec,
        ..BrokerOpts::default()
    };
    let broker = Broker::spawn(nb_serve, &broker_opts, out_dir, &tag)?;
    let endpoints = broker.endpoints.clone();
    let endpoint = endpoints
        .first()
        .cloned()
        .ok_or("nb-serve reported no endpoints")?;
    let mut consumer = rt
        .block_on(partitionline::consumer::Consumer::new(
            runtime::fdrive::consumer_config(cell, &endpoints),
        ))
        .map_err(|e| format!("consumer connect: {e}"))?;
    let mut fout = runtime::fdrive::FetchOutcome::default();
    let verify_capacity = if cell.id == "nb-fetch-committed-aborts" {
        cell.synth_records_per_partition
            .saturating_mul(cell.partitions as u64)
    } else {
        cell.target_records
    };
    fout.latencies_us
        .reserve(verify_capacity.min(1_000_000) as usize);
    if cell.id == "nb-fetch-committed-aborts" {
        fout.committed_history.reserve(64);
        fout.committed_partition_cursors
            .reserve(cell.partitions as usize);
    }
    let phase = measured_phase(|| {
        rt.block_on(runtime::fdrive::drive_fetch(&mut consumer, cell, &mut fout));
    });
    rt.block_on(consumer.close())
        .map_err(|e| format!("consumer close: {e}"))?;
    let rtt_ms = loopback_rtt_ms(&endpoint);
    let latency_path = write_latencies(out_dir, &tag, &fout.latencies_us)?;
    let (counts, child_ru) = broker.stop()?;
    let host = probe_host()?;

    // Convert to the shared outcome shape; verification failures are
    // family failures (the generic count check cannot see them).
    let outcome = runtime::drive::DriveOutcome {
        latencies_us: std::mem::take(&mut fout.latencies_us),
        errors: std::mem::take(&mut fout.errors),
        bytes_offered: fout.bytes_delivered,
        timed_out: fout.timed_out,
        ..runtime::drive::DriveOutcome::default()
    };
    let extra_failed = fout.mismatched > 0
        || !fout.paused_delivered.is_empty()
        || (cell.id == "nb-fetch-committed-aborts"
            && (fout.verified != fout.returned_records
                || fout.verified < cell.target_records
                || fout
                    .committed_partition_cursors
                    .iter()
                    .map(|(_, cursor)| *cursor as u64)
                    .sum::<u64>()
                    != counts.fetched_records
                || !outcome.errors.is_empty()));

    let cpu_total_us = phase.cpu_us.0 + phase.cpu_us.1;
    let mut extra = serde_json::Map::new();
    extra.insert(
        "fetch_rounds".to_owned(),
        serde_json::Value::from(fout.rounds),
    );
    extra.insert(
        "empty_rounds".to_owned(),
        serde_json::Value::from(fout.empty_rounds),
    );
    extra.insert(
        "mismatched".to_owned(),
        serde_json::Value::from(fout.mismatched),
    );
    extra.insert(
        "cpu_ns_per_round".to_owned(),
        serde_json::json!(if fout.rounds > 0 {
            cpu_total_us as f64 * 1000.0 / fout.rounds as f64
        } else {
            0.0
        }),
    );
    extra.insert(
        "allocs_per_round".to_owned(),
        serde_json::json!(if fout.rounds > 0 {
            phase.allocs.0 as f64 / fout.rounds as f64
        } else {
            0.0
        }),
    );
    if cell.id == "nb-fetch-capped-paused" {
        extra.insert(
            "paused_backlog_records".to_owned(),
            serde_json::Value::from(fout.paused_backlog_records),
        );
        extra.insert(
            "prefill_buffered_bytes".to_owned(),
            serde_json::Value::from(fout.prefill_buffered_bytes),
        );
    }
    if cell.id == "nb-fetch-committed-aborts" {
        extra.insert(
            "target_records".to_owned(),
            serde_json::Value::from(cell.target_records),
        );
        extra.insert(
            "returned_records".to_owned(),
            serde_json::Value::from(fout.returned_records),
        );
        extra.insert(
            "committed_history".to_owned(),
            serde_json::json!(fout.committed_history),
        );
        extra.insert(
            "committed_partition_cursors".to_owned(),
            serde_json::json!(fout.committed_partition_cursors),
        );
        extra.insert(
            "committed_abort_gap_records".to_owned(),
            serde_json::Value::from(fout.committed_abort_gap_records),
        );
        extra.insert(
            "committed_aborted_deliveries".to_owned(),
            serde_json::Value::from(fout.committed_aborted_deliveries),
        );
        extra.insert(
            "filtered_records".to_owned(),
            serde_json::Value::from(counts.fetched_records.saturating_sub(fout.returned_records)),
        );
    }
    extra.insert(
        "fetch_requests".to_owned(),
        serde_json::Value::from(counts.fetch_requests),
    );
    extra.insert(
        "fetched_records".to_owned(),
        serde_json::Value::from(counts.fetched_records),
    );
    extra.insert(
        "per_partition_delivered".to_owned(),
        serde_json::json!(fout
            .per_partition
            .iter()
            .map(|(p, n)| serde_json::json!({"partition": p, "records": n}))
            .collect::<Vec<_>>()),
    );
    if cell.slow_node.is_some() {
        // Default leadership is partition % nodes; report the
        // fast-node share explicitly.
        let slow = cell.slow_node.unwrap_or(-1);
        let live = cell.nodes.max(1) as i32;
        let (slow_records, fast_records) =
            fout.per_partition
                .iter()
                .fold((0u64, 0u64), |(slow_acc, fast_acc), (p, n)| {
                    if p % live == slow % live {
                        (slow_acc + n, fast_acc)
                    } else {
                        (slow_acc, fast_acc + n)
                    }
                });
        let wall = phase.wall.as_secs_f64().max(f64::MIN_POSITIVE);
        extra.insert(
            "slow_node_records".to_owned(),
            serde_json::Value::from(slow_records),
        );
        extra.insert(
            "fast_node_records".to_owned(),
            serde_json::Value::from(fast_records),
        );
        extra.insert(
            "fast_node_rec_s".to_owned(),
            serde_json::json!(fast_records as f64 / wall),
        );
    }
    if cell.max_poll_records.is_some() {
        extra.insert(
            "cpu_ns_per_poll".to_owned(),
            serde_json::json!(if fout.rounds > 0 {
                cpu_total_us as f64 * 1000.0 / fout.rounds as f64
            } else {
                0.0
            }),
        );
    }

    let ctx = RunContext {
        cell_id: cell.id,
        runtime_config: runtime_config.quiesce(rt)?,
        offered: if cell.id == "nb-fetch-committed-aborts" {
            fout.returned_records
        } else {
            cell.target_records
        },
        seed: cell.seed,
        profile: "fetch",
        consumed: fout.verified,
        extra_failed,
        effective_settings: fetch_effective(cell),
        equal_semantics: serde_json::json!({
            "isolation": if cell.read_committed { "read_committed" } else { "read_uncommitted" },
        }),
        drive_mode: "fetch-verify".to_owned(),
        extra_execution: extra,
        latency_note: "per-record delivery-latency bound: the enclosing fetch() call's duration, shared by every record in the batch; empty rounds carry no latency".to_owned(),
        repetition,
        broker: counts,
        host,
        wall_seconds: phase.wall.as_secs_f64(),
        cpu_us: phase.cpu_us,
        allocs: phase.allocs,
        rss: phase.rss,
        baseline_rss: phase.baseline_rss,
        process_peak_rss: phase.process_peak_rss,
        threads: phase.threads,
        broker_cpu_s: (child_ru.user_s, child_ru.sys_s),
        broker_peak_rss: child_ru.peak_rss_bytes,
        rtt_ms,
        endpoints,
        timestamps: (phase.start_iso, phase.end_iso),
        latency_path,
        extra_broker_artifacts: Vec::new(),
    };
    let result_path = out_dir.join(format!("{tag}.result.json"));
    let failed = finish(&ctx, &outcome, repo_root, harness_exe, &result_path)?;
    Ok((result_path, failed))
}

/// Open socket count plus how it was measured. Linux counts
/// `socket:` fds; elsewhere the total fd count is reported with a
/// note (never presented as sockets).
fn open_sockets() -> (u64, &'static str) {
    #[cfg(target_os = "linux")]
    {
        let mut sockets = 0u64;
        if let Ok(dir) = std::fs::read_dir("/proc/self/fd") {
            for entry in dir.flatten() {
                if let Ok(link) = std::fs::read_link(entry.path()) {
                    if link.to_string_lossy().starts_with("socket:") {
                        sockets += 1;
                    }
                }
            }
        }
        (sockets, "socket fds via /proc/self/fd")
    }
    #[cfg(not(target_os = "linux"))]
    {
        let mut fds = 0u64;
        for fd in 0..1024 {
            if unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1 {
                fds += 1;
            }
        }
        (
            fds,
            "total fds via fcntl (non-Linux; sockets not distinguished)",
        )
    }
}

/// One `nb-connect` case: fresh broker, fresh producer, one record.
struct ConnectCase {
    partitions: i32,
    dead_first: bool,
    stalled_first: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "connect cell records its explicit executor alongside its existing owned broker/run parameters"
)]
fn run_connect(
    rt: &tokio::runtime::Runtime,
    runtime_config: RuntimeConfig,
    nb_serve: &Path,
    repetition: u32,
    out_dir: &Path,
    repo_root: &Path,
    harness_exe: &Path,
    include_stalled: bool,
) -> Result<(PathBuf, bool), String> {
    const CELL_ID: &str = "nb-connect";
    const SEED: u64 = 0xC0DE_0001;
    let tag = format!("{CELL_ID}-rep{repetition}");
    let mut cases = vec![
        ConnectCase {
            partitions: 1,
            dead_first: false,
            stalled_first: false,
        },
        ConnectCase {
            partitions: 1,
            dead_first: true,
            stalled_first: false,
        },
        ConnectCase {
            partitions: 6,
            dead_first: false,
            stalled_first: false,
        },
        ConnectCase {
            partitions: 6,
            dead_first: true,
            stalled_first: false,
        },
        ConnectCase {
            partitions: 64,
            dead_first: false,
            stalled_first: false,
        },
        ConnectCase {
            partitions: 64,
            dead_first: true,
            stalled_first: false,
        },
    ];
    const STALLED_CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
    if include_stalled {
        cases.extend([1, 6, 64].map(|partitions| ConnectCase {
            partitions,
            dead_first: true,
            stalled_first: true,
        }));
    }
    // Fixture construction and the actual pending-TCP verification are outside
    // the measured phase. Keep all fixtures alive across every matrix case so
    // their sockets contribute equally to each open-socket observation.
    let stalled = if include_stalled {
        let mut fixtures = Vec::with_capacity(3);
        for _ in 0..3 {
            fixtures.push(
                rt.block_on(runtime::stalled_dial::StalledDial::new())
                    .map_err(|e| format!("stalled TCP fixture unavailable: {e}"))?,
            );
        }
        fixtures
    } else {
        Vec::new()
    };
    // Spawn all brokers up front (outside the census); each case gets
    // a fresh partition layout.
    let mut brokers = Vec::with_capacity(cases.len());
    for (i, case) in cases.iter().enumerate() {
        let case_tag = format!("{tag}-c{i}");
        let opts = BrokerOpts {
            partitions: case.partitions,
            ..BrokerOpts::default()
        };
        brokers.push(Broker::spawn(nb_serve, &opts, out_dir, &case_tag)?);
    }
    let mut outcome = runtime::drive::DriveOutcome::default();
    outcome.latencies_us.reserve(cases.len());
    let mut case_rows = Vec::with_capacity(cases.len());
    let phase = measured_phase(|| {
        for (i, case) in cases.iter().enumerate() {
            let mut bootstrap = brokers[i].endpoints.clone();
            let fixture = if case.stalled_first {
                stalled.get(i - 6)
            } else {
                None
            };
            if case.stalled_first && fixture.is_none() {
                outcome
                    .errors
                    .push("missing verified stalled TCP fixture".to_owned());
                break;
            }
            if let Some(fixture) = fixture {
                bootstrap.insert(0, fixture.addr().to_string());
            } else if case.dead_first {
                // Refused-fast stand-in for an unreachable host.
                bootstrap.insert(0, "127.0.0.1:1".to_owned());
            }
            let start = Instant::now();
            let acked = (|| -> Result<i64, String> {
                let producer = rt
                    .block_on(Producer::new({
                        let cfg = ProducerConfig::bootstrap(bootstrap.iter().cloned())
                            .client_id(format!("runtime-{CELL_ID}-c{i}"));
                        if case.stalled_first {
                            cfg.connect_timeout(STALLED_CONNECT_TIMEOUT)
                        } else {
                            cfg
                        }
                    }))
                    .map_err(|e| format!("connect: {e}"))?;
                let mut rec = partitionline::producer::ProduceRecord::to("nb-connect");
                rec.partition = Some(0);
                rec.key = Some(bytes::Bytes::from_static(b"nb-connect-key!!"));
                rec.value = Some(bytes::Bytes::from_static(b"nb-connect-value"));
                let meta = rt
                    .block_on(producer.send(rec))
                    .map_err(|e| format!("send: {e}"))?;
                outcome.bytes_offered += 16 + 16;
                Ok(meta.offset)
            })();
            let elapsed_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            let ok = acked.is_ok();
            match acked {
                Ok(offset) => {
                    outcome.latencies_us.push(elapsed_us);
                    outcome.acked += 1;
                    if offset >= 0 {
                        outcome.offsets_valid += 1;
                    }
                }
                Err(e) => {
                    if outcome.errors.len() < 32 {
                        outcome.errors.push(format!("case {i}: {e}"));
                    }
                }
            }
            let (sockets, _) = open_sockets();
            case_rows.push(serde_json::json!({
                "partitions": case.partitions,
                "dead_bootstrap_first": case.dead_first,
                "bootstrap_kind": if case.stalled_first { "stalled_tcp" } else if case.dead_first { "refused" } else { "live" },
                "connect_timeout_ms": if case.stalled_first { STALLED_CONNECT_TIMEOUT.as_millis() } else { ProducerConfig::default().connect_timeout.as_millis() },
                "verified_tcp_stall_us": fixture.map(|f| f.verified_stall().as_micros()),
                "first_ack_us": elapsed_us,
                "open_sockets": sockets,
                "ok": ok,
            }));
        }
    });
    outcome.offsets_observed = true;
    // Stop brokers, merge counts and rusage.
    let mut merged: Option<BrokerCounts> = None;
    let mut extra_paths = Vec::new();
    let mut broker_cpu = (0.0, 0.0);
    let mut broker_peak = 0u64;
    let mut endpoints = Vec::new();
    for broker in brokers {
        endpoints.extend(broker.endpoints.clone());
        let (counts, ru) = broker.stop()?;
        if merged.is_none() {
            merged = Some(counts);
        } else if let Some(m) = merged.as_mut() {
            extra_paths.push(counts.artifact_path.clone());
            m.merge(&counts);
        }
        broker_cpu.0 += ru.user_s;
        broker_cpu.1 += ru.sys_s;
        broker_peak = broker_peak.max(ru.peak_rss_bytes);
    }
    let counts = merged.ok_or("nb-connect ran no cases")?;
    let rtt_endpoint = endpoints.first().cloned().unwrap_or_default();
    let rtt_ms = loopback_rtt_ms(&rtt_endpoint);
    let latency_path = write_latencies(out_dir, &tag, &outcome.latencies_us)?;
    let host = probe_host()?;
    let defaults = ProducerConfig::default();
    let mut extra = serde_json::Map::new();
    extra.insert(
        "connect_cases".to_owned(),
        serde_json::Value::Array(case_rows),
    );
    let (_, socket_note) = open_sockets();
    extra.insert(
        "socket_measure".to_owned(),
        serde_json::Value::String(socket_note.to_owned()),
    );
    let ctx = RunContext {
        cell_id: CELL_ID,
        runtime_config: runtime_config.quiesce(rt)?,
        offered: cases.len() as u64,
        seed: SEED,
        profile: "bulk",
        consumed: 0,
        extra_failed: false,
        effective_settings: serde_json::json!({
            "acks": 1,
            "linger_ms": defaults.linger.as_millis() as u64,
            "batch_size_bytes": defaults.batch_bytes,
            "max_in_flight": defaults.max_in_flight,
            "idempotence": false,
            "cases": "1/6/64 partitions x live-first/dead-first bootstrap",
            "dead_host": "127.0.0.1:1 (refused-fast unreachable stand-in)",
            "connect_stalled_first": include_stalled,
            "stalled_connect_timeout_ms": STALLED_CONNECT_TIMEOUT.as_millis(),
            "stalled_host": "loopback listen(1), two held unaccepted connections; verified pending TCP connect before census",
        }),
        equal_semantics: serde_json::json!({"acks": 1}),
        drive_mode: "connect-first-ack".to_owned(),
        extra_execution: extra,
        latency_note:
            "construction-to-first-ack per case: Producer::new through one send() ack, microseconds"
                .to_owned(),
        repetition,
        broker: counts,
        host,
        wall_seconds: phase.wall.as_secs_f64(),
        cpu_us: phase.cpu_us,
        allocs: phase.allocs,
        rss: phase.rss,
        baseline_rss: phase.baseline_rss,
        process_peak_rss: phase.process_peak_rss,
        threads: phase.threads,
        broker_cpu_s: broker_cpu,
        broker_peak_rss: broker_peak,
        rtt_ms,
        endpoints,
        timestamps: (phase.start_iso, phase.end_iso),
        latency_path,
        extra_broker_artifacts: extra_paths,
    };
    let result_path = out_dir.join(format!("{tag}.result.json"));
    let failed = finish(&ctx, &outcome, repo_root, harness_exe, &result_path)?;
    Ok((result_path, failed))
}

fn main() {
    let mut cell_filter: Option<String> = None;
    let mut out_dir: Option<PathBuf> = None;
    let mut repetitions: u32 = 1;
    let mut connect_stalled_first = false;
    let mut runtime_flavor = "current_thread".to_owned();
    let mut runtime_workers = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--cell" => cell_filter = Some(args.next().unwrap_or_else(|| usage())),
            "--out" => out_dir = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            "--repetitions" => {
                repetitions = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--connect-stalled-first" => connect_stalled_first = true,
            "--runtime" => runtime_flavor = args.next().unwrap_or_else(|| usage()),
            "--workers" => {
                runtime_workers = Some(
                    args.next()
                        .unwrap_or_else(|| usage())
                        .parse()
                        .unwrap_or_else(|_| usage()),
                );
            }
            _ => usage(),
        }
    }
    let cell_filter = cell_filter.unwrap_or_else(|| usage());
    let runtime_config =
        RuntimeConfig::parse(&runtime_flavor, runtime_workers).unwrap_or_else(|_| usage());
    if connect_stalled_first && cell_filter != "nb-connect" && cell_filter != "all" {
        usage();
    }
    let out_dir = out_dir.unwrap_or_else(|| usage());
    if repetitions == 0 {
        usage();
    }
    if let Err(e) = std::fs::create_dir_all(&out_dir) {
        eprintln!("runtime: create out dir: {e}");
        std::process::exit(1);
    }

    let nb_serve = locate_nb_serve().unwrap_or_else(|e| {
        eprintln!("runtime: {e}");
        std::process::exit(1);
    });
    let harness_exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("runtime"));
    let repo_root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    /// One runnable cell: produce (KL09-09/10), fetch (KL09-10) or
    /// the connect matrix (KL09-10).
    enum Job {
        Produce(CellDef, ProduceRun),
        Fetch(runtime::fcells::FetchCellDef),
        Connect,
    }

    impl Job {
        fn id(&self) -> &str {
            match self {
                Job::Produce(cell, _) => cell.id,
                Job::Fetch(cell) => cell.id,
                Job::Connect => "nb-connect",
            }
        }
    }

    let mut jobs: Vec<Job> = Vec::new();
    for cell in producer_cells() {
        let broker_opts = BrokerOpts {
            partitions: cell.broker_partitions(),
            ..BrokerOpts::default()
        };
        jobs.push(Job::Produce(
            cell,
            ProduceRun {
                broker_opts,
                batch_records: None,
                require_faults: false,
            },
        ));
    }
    for cell in runtime::fcells::fetch_cells() {
        jobs.push(Job::Fetch(cell));
    }
    {
        let cell = runtime::cells::retry_cell();
        let broker_opts = BrokerOpts {
            partitions: cell.broker_partitions(),
            fault_seed: 0x5EED_F001,
            fault_rate_per_million: 10_000,
            ..BrokerOpts::default()
        };
        jobs.push(Job::Produce(
            cell,
            ProduceRun {
                broker_opts,
                batch_records: Some(20),
                require_faults: true,
            },
        ));
    }
    jobs.push(Job::Connect);

    if cell_filter != "all" {
        jobs.retain(|j| j.id() == cell_filter);
        if jobs.is_empty() {
            eprintln!("runtime: unknown cell '{cell_filter}'");
            std::process::exit(2);
        }
    }

    // Global census and RUSAGE_SELF include all client runtime workers.
    // Broker subprocesses are separate; RSS sampling is allocation-free.
    let rt = runtime_config.build().unwrap_or_else(|e| {
        eprintln!("runtime: tokio build failed: {e}");
        std::process::exit(1);
    });
    let mut failures = 0u32;
    for job in &jobs {
        for rep in 0..repetitions {
            let outcome = match job {
                Job::Produce(cell, run) => run_produce(
                    &rt,
                    runtime_config,
                    &nb_serve,
                    cell,
                    run,
                    rep,
                    &out_dir,
                    &repo_root,
                    &harness_exe,
                ),
                Job::Fetch(cell) => run_fetch(
                    &rt,
                    runtime_config,
                    &nb_serve,
                    cell,
                    rep,
                    &out_dir,
                    &repo_root,
                    &harness_exe,
                ),
                Job::Connect => run_connect(
                    &rt,
                    runtime_config,
                    &nb_serve,
                    rep,
                    &out_dir,
                    &repo_root,
                    &harness_exe,
                    connect_stalled_first,
                ),
            };
            match outcome {
                Ok((path, failed)) => {
                    println!(
                        "cell={} rep={rep} failed={failed} artifact={}",
                        job.id(),
                        path.display()
                    );
                    failures += u32::from(failed);
                }
                Err(e) => {
                    eprintln!("cell={} rep={rep} ERROR: {e}", job.id());
                    failures += 1;
                }
            }
        }
    }
    if failures > 0 {
        std::process::exit(1);
    }
}
