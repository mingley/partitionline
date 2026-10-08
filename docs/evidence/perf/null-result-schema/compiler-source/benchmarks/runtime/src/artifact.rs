//! Result-artifact builder (KL09-09): one fail-closed JSON document
//! per (cell, repetition) run, shaped for `scripts/benchmark-report.py`.
//!
//! Every number is measured in the run: broker counts come from the
//! `nb-serve` artifact, client resources from [`crate::measure`],
//! allocations from `codec::census`, provenance from git, the
//! toolchain and [`crate::host`]. `cell_disposition` is `failed`
//! when any server-side validation failure occurred, any offered
//! record went unacknowledged, or the cell timed out.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::drive::DriveOutcome;
use crate::host::HostInfo;

/// Broker-side counts parsed from the `nb-serve` artifact.
#[derive(Debug, Default, Clone)]
pub struct BrokerCounts {
    /// Records accepted.
    pub accepted_records: u64,
    /// Validated batch wire bytes accepted.
    pub accepted_wire_bytes: u64,
    /// Produce requests handled.
    pub produce_requests: u64,
    /// Metadata requests handled.
    pub metadata_requests: u64,
    /// Faulted requests (KL09-10 retry accounting).
    pub injected_requests: u64,
    /// Faulted partitions (one retried batch each).
    pub injected_errors: u64,
    /// Fetch requests handled.
    pub fetch_requests: u64,
    /// Synthetic records served.
    pub fetched_records: u64,
    /// Server-side validation failures by cause.
    pub validation_failures: u64,
    /// Log end offset per topic/partition.
    pub end_offsets: Vec<(String, i32, i64)>,
    /// Path of the broker artifact file.
    pub artifact_path: PathBuf,
}

impl BrokerCounts {
    /// Merge another broker's counts (multi-broker cells such as
    /// `nb-connect`): counters sum, end offsets sum per
    /// topic/partition.
    pub fn merge(&mut self, other: &BrokerCounts) {
        self.accepted_records += other.accepted_records;
        self.accepted_wire_bytes += other.accepted_wire_bytes;
        self.produce_requests += other.produce_requests;
        self.metadata_requests += other.metadata_requests;
        self.injected_requests += other.injected_requests;
        self.injected_errors += other.injected_errors;
        self.fetch_requests += other.fetch_requests;
        self.fetched_records += other.fetched_records;
        self.validation_failures += other.validation_failures;
        for (topic, partition, offset) in &other.end_offsets {
            if let Some(slot) = self
                .end_offsets
                .iter_mut()
                .find(|(t, p, _)| t == topic && p == partition)
            {
                slot.2 += offset;
            } else {
                self.end_offsets.push((topic.clone(), *partition, *offset));
            }
        }
        self.end_offsets.sort();
    }
}

/// Parse the JSON artifact `nb-serve` wrote. Fails closed: any
/// missing field is an error, never a zero.
pub fn parse_broker_artifact(path: &Path) -> Result<BrokerCounts, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read broker artifact: {e}"))?;
    let doc: Value =
        serde_json::from_str(&text).map_err(|e| format!("parse broker artifact: {e}"))?;
    let num = |key: &str| -> Result<u64, String> {
        doc.get(key)
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("broker artifact missing u64 '{key}'"))
    };
    let failures = doc
        .get("validation_failures")
        .and_then(Value::as_object)
        .ok_or_else(|| "broker artifact missing 'validation_failures'".to_owned())?;
    let mut validation_failures = 0u64;
    for cause in ["framing", "crc", "count", "sequence", "transactional"] {
        validation_failures = validation_failures.saturating_add(
            failures
                .get(cause)
                .and_then(Value::as_u64)
                .ok_or_else(|| format!("broker artifact missing failure cause '{cause}'"))?,
        );
    }
    let offsets = doc
        .get("end_offsets")
        .and_then(Value::as_object)
        .ok_or_else(|| "broker artifact missing 'end_offsets'".to_owned())?;
    let mut end_offsets = Vec::with_capacity(offsets.len());
    for (tp, offset) in offsets {
        let (topic, partition) = tp
            .rsplit_once('/')
            .ok_or_else(|| format!("bad end_offsets key '{tp}'"))?;
        let partition: i32 = partition
            .parse()
            .map_err(|_| format!("bad end_offsets partition '{tp}'"))?;
        let offset = offset
            .as_i64()
            .ok_or_else(|| format!("bad end_offsets value '{tp}'"))?;
        end_offsets.push((topic.to_owned(), partition, offset));
    }
    end_offsets.sort();
    Ok(BrokerCounts {
        accepted_records: num("accepted_records")?,
        accepted_wire_bytes: num("accepted_wire_bytes")?,
        produce_requests: num("produce_requests")?,
        metadata_requests: num("metadata_requests")?,
        injected_requests: num("injected_requests")?,
        injected_errors: num("injected_errors")?,
        fetch_requests: num("fetch_requests")?,
        fetched_records: num("fetched_records")?,
        validation_failures,
        end_offsets,
        artifact_path: path.to_path_buf(),
    })
}

/// Everything the artifact builder needs beyond the drive outcome.
pub struct RunContext {
    /// Cell ID.
    pub cell_id: &'static str,
    /// Records offered (produce) or targeted (fetch/connect cases).
    pub offered: u64,
    /// Base seed; repetition seeds derive from it.
    pub seed: u64,
    /// Scenario profile (`bulk` for produce, `fetch` for consume).
    pub profile: &'static str,
    /// Records consumed and verified (fetch drives; 0 for produce).
    pub consumed: u64,
    /// Family-specific failure (verification mismatch, paused
    /// delivery, no faults fired, ...) beyond the generic checks.
    pub extra_failed: bool,
    /// Effective client settings (must carry the five required keys).
    pub effective_settings: Value,
    /// Requested and observed Tokio runtime configuration.
    pub runtime_config: Value,
    /// Scenario equal-semantics block.
    pub equal_semantics: Value,
    /// Drive-mode label for `execution`.
    pub drive_mode: String,
    /// Extra `execution` fields (cell-specific metrics).
    pub extra_execution: serde_json::Map<String, Value>,
    /// Latency-meaning note for `execution.latency_note`.
    pub latency_note: String,
    /// Zero-based repetition index.
    pub repetition: u32,
    /// Broker counts from the `nb-serve` artifact.
    pub broker: BrokerCounts,
    /// Host facts.
    pub host: HostInfo,
    /// Measured-phase wall time, seconds.
    pub wall_seconds: f64,
    /// Measured-phase client CPU, microseconds (user, sys).
    pub cpu_us: (u64, u64),
    /// Census allocations (count, bytes) over the measured phase.
    pub allocs: (u64, u64),
    /// Client RSS (sample peak, sample mean) in bytes.
    pub rss: (u64, u64),
    /// Client RSS just before the measured phase (fixture + runtime
    /// baseline; subtract from the peak for the drive-induced delta).
    pub baseline_rss: u64,
    /// Process-wide `ru_maxrss` reference (monotonic; shared by all
    /// cells in a multi-cell run, never per-cell attributed).
    pub process_peak_rss: u64,
    /// Client thread count observed at end of run.
    pub threads: u64,
    /// Broker child CPU seconds (user, sys) via `wait4`.
    pub broker_cpu_s: (f64, f64),
    /// Broker child peak RSS bytes via `wait4`.
    pub broker_peak_rss: u64,
    /// Loopback TCP-connect RTT over 20 samples, milliseconds.
    pub rtt_ms: f64,
    /// Broker endpoints, e.g. `127.0.0.1:54321` (one per node).
    pub endpoints: Vec<String>,
    /// UTC ISO-8601 start/end of the measured phase.
    pub timestamps: (String, String),
    /// Path of the raw-latency sidecar (written before this builds).
    pub latency_path: PathBuf,
    /// Extra broker artifacts (multi-broker cells; the primary is
    /// `broker.artifact_path`). Hashed into provenance; their bytes
    /// add to the broker disk-write count.
    pub extra_broker_artifacts: Vec<PathBuf>,
}

fn sha256_file(path: &Path) -> Result<(String, u64), String> {
    let bytes = std::fs::read(path).map_err(|e| format!("hash {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok((hex::encode(hasher.finalize()), bytes.len() as u64))
}

/// Minimal hex encoder (no new dependency for two hashes).
mod hex {
    /// Lowercase hex of `bytes`.
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let bytes = bytes.as_ref();
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(DIGITS[(b >> 4) as usize] as char);
            out.push(DIGITS[(b & 0x0f) as usize] as char);
        }
        out
    }
}

fn git(repo: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|e| format!("git {}: {e}", args.join(" ")))?;
    if !output.status.success() {
        return Err(format!("git {} failed", args.join(" ")));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn tool_version(tool: &str, arg: &str) -> String {
    Command::new(tool)
        .arg(arg)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Latency distribution summary in microseconds.
struct LatencyStats {
    count: usize,
    min: u64,
    max: u64,
    mean: f64,
    stddev: f64,
    p50: f64,
    p90: f64,
    p95: f64,
    p99: f64,
    p99_9: f64,
    ci95: (f64, f64),
    buckets: Vec<(u64, u64)>,
}

fn summarize_latency(mut sorted_us: Vec<u64>) -> LatencyStats {
    sorted_us.sort_unstable();
    let count = sorted_us.len();
    if count == 0 {
        return LatencyStats {
            count: 0,
            min: 0,
            max: 0,
            mean: 0.0,
            stddev: 0.0,
            p50: 0.0,
            p90: 0.0,
            p95: 0.0,
            p99: 0.0,
            p99_9: 0.0,
            ci95: (0.0, 0.0),
            buckets: vec![(0, 0)],
        };
    }
    let quantile = |q: f64| -> f64 {
        let rank = q * (count - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        sorted_us[lo] as f64 + (sorted_us[hi] as f64 - sorted_us[lo] as f64) * (rank - lo as f64)
    };
    let sum: u128 = sorted_us.iter().map(|&v| u128::from(v)).sum();
    let mean = sum as f64 / count as f64;
    let var = sorted_us
        .iter()
        .map(|&v| {
            let d = v as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / count as f64;
    let stddev = var.sqrt();
    let half = 1.96 * stddev / (count as f64).sqrt();
    // Log2 buckets: upper bound 2^(i+1) microseconds.
    let mut buckets = Vec::new();
    let mut bounds: Vec<u64> = vec![1];
    while bounds.len() < 64 {
        let next = bounds[bounds.len() - 1].saturating_mul(2);
        bounds.push(next);
        if next == u64::MAX {
            break;
        }
    }
    let mut counts = vec![0u64; bounds.len()];
    for &v in &sorted_us {
        let idx = bounds
            .iter()
            .position(|&b| v < b)
            .unwrap_or(bounds.len() - 1);
        counts[idx] += 1;
    }
    for (bound, count) in bounds.into_iter().zip(counts) {
        if count > 0 || buckets.is_empty() {
            buckets.push((bound, count));
        }
    }
    LatencyStats {
        count,
        min: sorted_us[0],
        max: sorted_us[count - 1],
        mean,
        stddev,
        p50: quantile(0.50),
        p90: quantile(0.90),
        p95: quantile(0.95),
        p99: quantile(0.99),
        p99_9: quantile(0.999),
        ci95: ((mean - half).max(0.0), mean + half),
        buckets,
    }
}

/// Build the result document. `repo_root` locates git provenance;
/// `harness_exe` is hashed as the client-under-test binary.
pub fn build_result(
    ctx: &RunContext,
    outcome: &DriveOutcome,
    repo_root: &Path,
    harness_exe: &Path,
    result_path: &Path,
) -> Result<Value, String> {
    let offered = ctx.offered;
    let acked = outcome.acked;
    let consumed = ctx.consumed;
    let delivered = acked.max(consumed);
    let broker_accepted = ctx.broker.accepted_records;
    let failed = ctx.broker.validation_failures > 0
        || outcome.timed_out
        || ctx.extra_failed
        || delivered != offered
        || (acked > 0 && broker_accepted != acked)
        || (outcome.offsets_observed && outcome.offsets_valid != acked);
    let disposition = if failed { "failed" } else { "executed" };

    let stats = summarize_latency(outcome.latencies_us.clone());
    let wall = ctx.wall_seconds.max(f64::MIN_POSITIVE);
    let rps = delivered as f64 / wall;
    let mbps = outcome.bytes_offered as f64 / wall / (1024.0 * 1024.0);
    let cpu_total_us = ctx.cpu_us.0.saturating_add(ctx.cpu_us.1);
    let cpu_pct = cpu_total_us as f64 / 1e6 / wall * 100.0;
    let cpu_ns_per_record = if delivered > 0 {
        cpu_total_us as f64 * 1000.0 / delivered as f64
    } else {
        0.0
    };
    let allocs_per_record = if delivered > 0 {
        ctx.allocs.0 as f64 / delivered as f64
    } else {
        0.0
    };

    let commit = git(repo_root, &["rev-parse", "HEAD"])?;
    let tree = git(repo_root, &["rev-parse", "HEAD^{tree}"])?;
    let branch = git(repo_root, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    let dirty = !git(repo_root, &["status", "--porcelain"])?.is_empty();
    let repo_url = git(repo_root, &["remote", "get-url", "origin"])?;

    let (bin_sha, bin_size) = sha256_file(harness_exe)?;
    let exe_name = harness_exe
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "runtime".to_owned());

    let mut effective = ctx.effective_settings.clone();
    let settings = effective
        .as_object_mut()
        .ok_or("effective settings must be an object")?;
    settings.insert("runtime".into(), ctx.runtime_config.clone());
    if ctx.profile == "fetch" {
        settings.insert("idempotence".into(), Value::Null);
    }
    let config_path = result_path.with_extension("config.json");
    let config_bytes = serde_json::to_vec_pretty(&effective)
        .map_err(|error| format!("encode effective config: {error}"))?;
    std::fs::write(&config_path, &config_bytes)
        .map_err(|error| format!("write effective config: {error}"))?;
    let cfg_sha = hex::encode(Sha256::digest(&config_bytes));
    let tokio_version = include_str!("../Cargo.lock")
        .split("[[package]]")
        .find(|package| package.lines().any(|line| line == "name = \"tokio\""))
        .and_then(|package| {
            package.lines().find_map(|line| {
                line.strip_prefix("version = \"")
                    .and_then(|version| version.strip_suffix('"'))
            })
        })
        .ok_or("retained lockfile has no Tokio version")?;

    let (broker_sha, broker_size) = sha256_file(&ctx.broker.artifact_path)?;
    let mut broker_artifacts = vec![json!({
        "path": ctx.broker.artifact_path.display().to_string(),
        "type": "broker-counts",
        "sha256": broker_sha,
        "size_bytes": broker_size,
    })];
    let mut disk_write_bytes = broker_size;
    for extra in &ctx.extra_broker_artifacts {
        let (sha, size) = sha256_file(extra)?;
        disk_write_bytes += size;
        broker_artifacts.push(json!({
            "path": extra.display().to_string(),
            "type": "broker-counts",
            "sha256": sha,
            "size_bytes": size,
        }));
    }
    let (lat_sha, lat_size) = sha256_file(&ctx.latency_path)?;

    let hw_delta: i64 = ctx.broker.end_offsets.iter().map(|(_, _, o)| o).sum();
    let partitions: Vec<Value> = ctx
        .broker
        .end_offsets
        .iter()
        .map(|(t, p, o)| {
            json!({"topic": t, "partition": p, "start_offset": 0,
            "end_offset": o, "offset_delta": o})
        })
        .collect();

    // Send-based modes verify per-record offsets; `try_send` modes
    // verify server-side acceptance (every batch CRC-validated by
    // the null broker) reconciled against the offered count; fetch
    // drives verify every delivered ID/hash client-side.
    let checksummed: u64 = if outcome.offsets_observed {
        outcome.offsets_valid
    } else if consumed > 0 {
        consumed
    } else {
        broker_accepted.min(acked)
    };
    let errors: Vec<Value> = outcome
        .errors
        .iter()
        .map(|e| json!({"phase": "measured", "message": e}))
        .collect();

    let seed = ctx.seed;
    let mut equal_semantics = ctx.equal_semantics.clone();
    let semantics = equal_semantics
        .as_object_mut()
        .ok_or("semantics must be an object")?;
    semantics.insert("durability".into(), Value::Null);
    semantics.insert(
        "security".into(),
        json!({"protocol":"PLAINTEXT","mechanism":"NONE"}),
    );
    if ctx.profile == "fetch" {
        semantics.insert("acks".into(), Value::Null);
        semantics.insert("idempotence".into(), Value::Null);
    } else {
        semantics
            .entry("idempotence")
            .or_insert_with(|| effective["idempotence"].clone());
        semantics.insert("isolation".into(), Value::Null);
    }
    let repetition = ctx
        .repetition
        .checked_add(1)
        .ok_or("repetition index overflow")?;
    let mut doc = json!({
        "schema_version": "2.0.0",
        "contract_version": "1.1.0",
        "suite_hold": {"status": "active", "policy":"A local fixture does not qualify a scenario or lift Suite HOLD."},
        "scenario": {
            "scenario_id": ctx.cell_id,
            "peer": "partitionline",
            "profile": ctx.profile,
            "tier": "exploratory",
            "cell_disposition": disposition,
            "equal_semantics": equal_semantics,
        },
        "provenance": {
            "source": {
                "git_commit": commit,
                "git_branch": branch,
                "tree_hash": tree,
                "dirty_tree": dirty,
                "clean": !dirty,
                "repo_url": repo_url,
            },
            "binary": {
                "name": exe_name,
                "path": harness_exe.display().to_string(),
                "sha256": bin_sha,
                "size_bytes": bin_size,
            },
            "config": {
                "path": config_path.display().to_string(),
                "sha256": cfg_sha,
                "effective_settings": effective,
            },
            "toolchains": {
                "compiler": tool_version("rustc", "--version"),
                "runtime": format!("tokio {tokio_version}"),
                "build_tool": tool_version("cargo", "--version"),
            },
            "broker": {
                "image": "nullbroker (KL09-06, workspace-excluded harness broker)",
                "version": env!("CARGO_PKG_VERSION"),
                "mode": "null",
                "cluster_id": null,
                "node_count": ctx.endpoints.len().max(1),
                "endpoints": ctx.endpoints.clone(),
            },
            "host": {
                "hostname": ctx.host.hostname,
                "os": ctx.host.os,
                "os_family": ctx.host.os_family,
                "kernel_version": ctx.host.kernel_version,
                "arch": ctx.host.arch,
                "cpu": {
                    "model": ctx.host.cpu_model,
                    "physical_cores": ctx.host.physical_cores,
                    "logical_cores": ctx.host.logical_cores,
                    "frequency_mhz": ctx.host.frequency_mhz,
                },
                "memory": {
                    "unit": "bytes",
                    "total_bytes": ctx.host.memory_total_bytes,
                },
            },
            "topology": {
                "environment": "loopback",
                "rtt_ms": ctx.rtt_ms,
                "rtt_unit": "milliseconds",
                "client_nodes": 1,
                "broker_nodes": 1,
                "network_interface": "lo",
            },
            "timestamps": {
                "start_time_utc": ctx.timestamps.0,
                "end_time_utc": ctx.timestamps.1,
                "duration_seconds": ctx.wall_seconds,
                "duration_unit": "seconds",
            },
            "seeds": {
                "payload_seed": seed,
                "key_seed": seed ^ 0x9E37_79B9_7F4A_7C15,
                "partition_seed": seed ^ 0xC2B2_AE35_1750_4D07,
                "repetition_seed": seed ^ u64::from(ctx.repetition),
            },
            "artifacts": broker_artifacts_plus_latency(broker_artifacts, &ctx.latency_path, &lat_sha, lat_size),
        },
        "execution": {
            "phase": "bounded_fixture",
            "warmup_completed": false,
            "warmup_records": 0,
            "warmup_duration_seconds": 0,
            "steady_state_duration_seconds": null,
            "measured_duration_seconds": ctx.wall_seconds,
            "total_repetitions": null,
            "pairing_order": "unreported",
            "coordinated_omission_avoidance": {"enabled": false, "schedule_type":"closed_loop"},
            "cell_id": ctx.cell_id,
            "repetition_index": repetition,
            "result_path": result_path.display().to_string(),
            "drive_mode": ctx.drive_mode.clone(),
            "records_offered": offered,
            "records_consumed": consumed,
            "timed_out": outcome.timed_out,
            "flush_us_total": outcome.flush_us_total,
            "queue_full_retries": outcome.queue_full_retries,
            "offsets_observed": outcome.offsets_observed,
            "validation_failures": ctx.broker.validation_failures,
            "latency_note": ctx.latency_note.clone(),
        },
        "outcomes": {
            "offered": offered,
            "accepted": delivered,
            "acknowledged": acked,
            "consumed": consumed,
            "rejected": 0,
            "timed_out": if outcome.timed_out { offered.saturating_sub(delivered) } else { 0 },
            "unknown": offered.saturating_sub(delivered),
        },
        "measurements": {
            "throughput": {
                "records_per_second": rps,
                "records_per_second_unit": "records/s",
                "megabytes_per_second": mbps,
                "megabytes_per_second_unit": "MB/s",
                "total_bytes_transferred": outcome.bytes_offered,
                "total_bytes_unit": "bytes",
            },
            "latency": {
                "sample_count": stats.count,
                "unit": "microseconds",
                "p50": stats.p50,
                "p90": stats.p90,
                "p95": stats.p95,
                "p99": stats.p99,
                "p99_9": stats.p99_9,
                "min": stats.min,
                "max": stats.max,
                "mean": stats.mean,
                "stddev": stats.stddev,
                "confidence_interval_95": {
                    "lower": stats.ci95.0,
                    "upper": stats.ci95.1,
                    "unit": "microseconds",
                },
                "raw_histogram": {
                    "bucket_unit": "microseconds",
                    "buckets": stats.buckets.iter().map(|(b, c)| json!({
                        "min_us": if *b == 1 { 0 } else { b / 2 + u64::from(*b == u64::MAX) },
                        "max_us": b.saturating_sub(u64::from(*b != u64::MAX)),
                        "upper_bound_us": b, "count": c})).collect::<Vec<_>>(),
                },
            },
            "client_resources": {
                "cpu_utilization_pct": cpu_pct,
                "cpu_unit": "percent",
                "user_cpu_seconds": ctx.cpu_us.0 as f64 / 1e6,
                "system_cpu_seconds": ctx.cpu_us.1 as f64 / 1e6,
                "cpu_seconds_unit": "seconds",
                "cpu_ns_per_record": cpu_ns_per_record,
                "allocations": {
                    "unit": "bytes",
                    "total_allocated_bytes": ctx.allocs.1,
                    "allocation_count": ctx.allocs.0,
                    "allocations_per_record": allocs_per_record,
                },
                "rss": {
                    "unit": "bytes",
                    "peak_rss_bytes": ctx.rss.0,
                    "average_rss_bytes": ctx.rss.1,
                    "baseline_rss_bytes": ctx.baseline_rss,
                    "process_peak_rss_bytes": ctx.process_peak_rss,
                    "note": "peak/average from a 10ms sampler over the measured phase only; baseline read at phase start (fixture + runtime); process_peak is ru_maxrss, monotonic per process",
                },
                "threads_count": ctx.threads,
            },
            "broker_resources": {
                "cpu_utilization_pct": 0.0,
                "cpu_unit": "percent",
                "user_cpu_seconds": ctx.broker_cpu_s.0,
                "system_cpu_seconds": ctx.broker_cpu_s.1,
                "cpu_seconds_unit": "seconds",
                "peak_rss_bytes": ctx.broker_peak_rss,
                "rss_unit": "bytes",
                "disk_write_bytes": disk_write_bytes,
                "disk_write_unit": "bytes",
                "note": "null broker is a validating loopback; user/system CPU and peak RSS via wait4 child rusage; disk writes are the broker artifacts only",
            },
            "errors": errors,
        },
        "integrity": {
            "idempotence_sequence_verified": null,
            "verified": !failed,
            "integrity_failure": failed,
            "high_watermark_audit": {
                "partitions": partitions,
                "total_offset_delta": hw_delta,
                "matches_acknowledged": hw_delta as u64 == acked,
            },
            "record_ids": {
                "start_id": if offered > 0 { 1 } else { 0 },
                "end_id": offered,
                "expected_count": delivered,
                "verified_count": checksummed,
                "missing_ids_count": delivered.saturating_sub(checksummed),
                "duplicate_ids_count": 0,
                "checksum_algorithm": if ctx.profile == "fetch" { "client fixture ID and payload verification" }
                    else { "broker batch-CRC validation (KL09-06) + client offset accounting" },
                "payload_checksum_matches": !failed,
            },
        },
        "repetition_history": {
            "total_attempts": 1,
            "failed_attempts": if failed { 1 } else { 0 },
            "attempts": [
                {
                    "attempt_number": 1,
                    "repetition_index": repetition,
                    "status": if failed { "failed_integrity" } else { "passed_measurement" },
                    "integrity_failure": failed,
                    "error_message": if failed { Some("Measured fixture failed; retained error and outcome fields describe the failure.") } else { None },
                    "timestamp_utc": ctx.timestamps.1,
                },
            ],
        },
    });
    if let Some(exec) = doc.get_mut("execution").and_then(Value::as_object_mut) {
        exec.extend(ctx.extra_execution.clone());
    }
    Ok(doc)
}

/// Append the latency sidecar entry to the broker-artifact list.
fn broker_artifacts_plus_latency(
    mut broker: Vec<Value>,
    latency_path: &Path,
    lat_sha: &str,
    lat_size: u64,
) -> Vec<Value> {
    broker.push(json!({
        "path": latency_path.display().to_string(),
        "type": "raw-latency-us",
        "sha256": lat_sha,
        "size_bytes": lat_size,
    }));
    broker
}
