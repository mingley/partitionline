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
use runtime::host::probe as probe_host;
use runtime::measure::{cpu_now, peak_rss_bytes, rss_now, RssSampler};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn usage() -> ! {
    eprintln!("usage: runtime --cell <id|all> --out <dir> [--repetitions <n>]");
    std::process::exit(2);
}

/// Child rusage harvested with `wait4` at reap time.
#[derive(Debug, Default, Clone, Copy)]
struct ChildRusage {
    user_s: f64,
    sys_s: f64,
    peak_rss_bytes: u64,
}

/// A running `nb-serve` broker: port, artifact path, and the pipes
/// needed to stop it and harvest child rusage.
struct Broker {
    endpoint: String,
    artifact_path: PathBuf,
    child: std::process::Child,
}

impl Broker {
    fn spawn(nb_serve: &Path, partitions: i32, out_dir: &Path, tag: &str) -> Result<Self, String> {
        let artifact_path = out_dir.join(format!("{tag}.broker.json"));
        let _ = std::fs::remove_file(&artifact_path);
        let mut child = Command::new(nb_serve)
            .args([
                "--bind",
                "127.0.0.1",
                "--partitions",
                &partitions.to_string(),
                "--serve-for-secs",
                "900",
                "--artifact",
            ])
            .arg(&artifact_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn nb-serve: {e}"))?;
        let stdout = child.stdout.take().ok_or("nb-serve stdout unavailable")?;
        let mut lines = std::io::BufReader::new(stdout).lines();
        // Blocking single-line read: nb-serve prints READY immediately
        // after bind, or exits (EOF) on failure.
        let mut port = None;
        match lines.next() {
            Some(Ok(text)) => {
                let text = text.trim().to_owned();
                if let Some(rest) = text.strip_prefix("READY port=") {
                    port = rest.parse::<u16>().ok();
                }
            }
            Some(Err(e)) => return Err(format!("read nb-serve READY: {e}")),
            None => {}
        }
        let port = port.ok_or_else(|| {
            let _ = child.kill();
            "nb-serve exited before READY".to_owned()
        })?;
        Ok(Self {
            endpoint: format!("127.0.0.1:{port}"),
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

fn producer_config(cell: &CellDef, endpoint: &str, max_in_flight: usize) -> ProducerConfig {
    let acks = match cell.acks {
        -1 => Acks::All,
        0 => Acks::None,
        _ => Acks::Leader,
    };
    let cfg = ProducerConfig::bootstrap([endpoint])
        .client_id(format!("runtime-{}", cell.id))
        .acks(acks)
        .max_in_flight(max_in_flight)
        .idempotent(cell.idempotent);
    match cell.linger_override_ms {
        Some(ms) => cfg.linger(Duration::from_millis(ms)),
        None => cfg,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_one(
    rt: &tokio::runtime::Runtime,
    nb_serve: &Path,
    cell: &CellDef,
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
    let broker = Broker::spawn(nb_serve, cell.broker_partitions(), out_dir, &tag)?;
    let endpoint = broker.endpoint.clone();
    let producer = Arc::new(
        rt.block_on(Producer::new(producer_config(
            cell,
            &endpoint,
            max_in_flight,
        )))
        .map_err(|e| format!("producer connect: {e}"))?,
    );
    let mut records = generate(cell);
    // Touch the sampler + RSS paths before measuring so setup
    // allocations stay outside the census.
    let _ = rss_now();
    let _ = peak_rss_bytes();
    // Pre-reserve drive bookkeeping outside the census.
    let mut outcome = runtime::drive::DriveOutcome::default();
    outcome.latencies_us.reserve(records.len());
    let mut stamps = Vec::with_capacity(records.len());
    let sampler = RssSampler::start(Duration::from_millis(10), 60_000);
    let baseline_rss = rss_now();
    let cpu_before = cpu_now();
    let wall_start = Instant::now();
    let wall_start_iso = utc_now_iso();
    let ((), allocs, alloc_bytes) = census(|| {
        rt.block_on(drive_cell(
            &producer,
            cell,
            &mut records,
            &mut outcome,
            &mut stamps,
        ));
    });
    let wall = wall_start.elapsed();
    let wall_end_iso = utc_now_iso();
    let cpu_after = cpu_now();
    let (rss_peak, rss_mean) = sampler.stop();
    let process_peak_rss = peak_rss_bytes();
    let threads = thread_count();
    drop(producer);
    let rtt_ms = loopback_rtt_ms(&endpoint);
    // Write the raw-latency sidecar before building the artifact
    // (the artifact hashes it).
    let latency_path = out_dir.join(format!("{tag}.latency-us.txt"));
    {
        let mut body = String::with_capacity(outcome.latencies_us.len() * 4);
        for lat in &outcome.latencies_us {
            body.push_str(&lat.to_string());
            body.push('\n');
        }
        std::fs::write(&latency_path, body).map_err(|e| format!("write latency sidecar: {e}"))?;
    }
    let (counts, child_ru) = broker.stop()?;
    let host = probe_host()?;
    let defaults = ProducerConfig::default();
    let ctx = RunContext {
        cell: cell.clone(),
        repetition,
        broker: counts,
        host,
        wall_seconds: wall.as_secs_f64(),
        cpu_us: {
            let delta = cpu_after.saturating_sub(cpu_before);
            (delta.user_us, delta.sys_us)
        },
        allocs: (allocs, alloc_bytes),
        rss: (rss_peak, rss_mean),
        baseline_rss,
        process_peak_rss,
        threads,
        broker_cpu_s: (child_ru.user_s, child_ru.sys_s),
        broker_peak_rss: child_ru.peak_rss_bytes,
        rtt_ms,
        endpoint,
        timestamps: (wall_start_iso, wall_end_iso),
        latency_path,
        max_in_flight,
        linger_ms: cell
            .linger_override_ms
            .unwrap_or(defaults.linger.as_millis() as u64),
        batch_size_bytes: defaults.batch_bytes as u64,
    };
    let result_path = out_dir.join(format!("{tag}.result.json"));
    let doc = build_result(&ctx, &outcome, repo_root, harness_exe, &result_path)?;
    let text = serde_json::to_string_pretty(&doc).map_err(|e| format!("serialize result: {e}"))?;
    std::fs::write(&result_path, text).map_err(|e| format!("write result: {e}"))?;
    let failed = doc
        .get("scenario")
        .and_then(|s| s.get("cell_disposition"))
        .and_then(|d| d.as_str())
        == Some("failed");
    Ok((result_path, failed))
}

fn main() {
    let mut cell_filter: Option<String> = None;
    let mut out_dir: Option<PathBuf> = None;
    let mut repetitions: u32 = 1;
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
            _ => usage(),
        }
    }
    let cell_filter = cell_filter.unwrap_or_else(|| usage());
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

    let mut cells = producer_cells();
    if cell_filter != "all" {
        cells.retain(|c| c.id == cell_filter);
        if cells.is_empty() {
            eprintln!("runtime: unknown cell '{cell_filter}'");
            std::process::exit(2);
        }
    }

    // Single-threaded runtime: census + RUSAGE_SELF then count only
    // client work (the sampler is allocation-free by construction).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| {
            eprintln!("runtime: tokio build failed: {e}");
            std::process::exit(1);
        });
    let mut failures = 0u32;
    for cell in &cells {
        for rep in 0..repetitions {
            let outcome = run_one(
                &rt,
                &nb_serve,
                cell,
                rep,
                &out_dir,
                &repo_root,
                &harness_exe,
            );
            match outcome {
                Ok((path, failed)) => {
                    println!(
                        "cell={} rep={rep} failed={failed} artifact={}",
                        cell.id,
                        path.display()
                    );
                    failures += u32::from(failed);
                }
                Err(e) => {
                    eprintln!("cell={} rep={rep} ERROR: {e}", cell.id);
                    failures += 1;
                }
            }
        }
    }
    if failures > 0 {
        std::process::exit(1);
    }
}
